use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Instant;

use anyhow::Context;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;

use bbr_client_chiavdf_fast::{
    ChiavdfBatchJob, prove_one_weso_fast, prove_one_weso_fast_streaming_getblock_opt,
    prove_one_weso_fast_streaming_getblock_opt_batch,
};

use crate::cli::WorkMode;
use crate::constants::default_classgroup_element;
use crate::format::{format_duration, format_number};

const BENCH_DISCRIMINANT_BITS: usize = 1024;
const BENCH_ITERS: u64 = 14_576_841;
const WARMUP_ITERS: u64 = 10_000;
const GROUP_PROOFS_PER_BATCH: usize = 8;
const PROOF_ROUNDS_PER_WORKER: usize = 1;
const GROUP_ROUNDS_PER_WORKER: usize = 3;
const BENCH_Y_REF_B64: &str = "AABi49IsOPkm3kNS+NW8BLw7jLR/QG2nKwsJ4VIRB+o+C5HAtC7XLoCvOHx/8CIA7fxD1esqHcB+RftlEwdKIMM692W2YUI7xwt4VJe3UoPc3zffkeZ5elOWDP/PO7DL00QBAA==";
const BENCH_CHALLENGE: [u8; 32] = [
    0x62, 0x62, 0x72, 0x2d, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x2d, 0x62, 0x65, 0x6e, 0x63,
    0x68, 0x2d, 0x76, 0x31, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
    0x0b, 0x0c,
];

pub fn run_benchmark(mode: WorkMode, parallel: usize) -> anyhow::Result<()> {
    let parallel = parallel.max(1);
    let rounds_per_worker = match mode {
        WorkMode::Proof => PROOF_ROUNDS_PER_WORKER,
        WorkMode::Group => GROUP_ROUNDS_PER_WORKER,
    };
    let proofs_per_task = match mode {
        WorkMode::Proof => 1usize,
        WorkMode::Group => GROUP_PROOFS_PER_BATCH,
    };
    let mode_label = match mode {
        WorkMode::Proof => "proof",
        WorkMode::Group => "group",
    };

    let task_count = parallel
        .checked_mul(rounds_per_worker)
        .ok_or_else(|| anyhow::anyhow!("benchmark task count overflow"))?;
    let total_proofs = task_count
        .checked_mul(proofs_per_task)
        .ok_or_else(|| anyhow::anyhow!("benchmark proof count overflow"))?;

    let x = default_classgroup_element();

    if BENCH_Y_REF_B64.starts_with("<fill-me") {
        anyhow::bail!("bench vector missing: set BENCH_Y_REF_B64 to a valid base64-encoded y_ref")
    }

    let y_ref = B64
        .decode(BENCH_Y_REF_B64.as_bytes())
        .context("decode BENCH_Y_REF_B64")?;

    let _ = prove_one_weso_fast(&BENCH_CHALLENGE, &x, BENCH_DISCRIMINANT_BITS, WARMUP_ITERS)
        .context("warmup prove_one_weso_fast")?;

    println!("Benchmark mode: {mode_label}");
    println!("Parallel workers: {}", format_number(parallel as u64));
    println!("Rounds per worker: {}", format_number(rounds_per_worker as u64));
    if matches!(mode, WorkMode::Group) {
        println!(
            "Group size: {} proofs",
            format_number(GROUP_PROOFS_PER_BATCH as u64)
        );
    }
    println!("Iterations per proof: {}", format_number(BENCH_ITERS));
    println!("Total proofs: {}", format_number(total_proofs as u64));

    let next_task = Arc::new(AtomicUsize::new(0));
    let y_ref = Arc::new(y_ref);

    let started_at = Instant::now();
    let mut handles = Vec::with_capacity(parallel);
    for _worker in 0..parallel {
        let next_task = next_task.clone();
        let y_ref = y_ref.clone();

        handles.push(thread::spawn(move || -> anyhow::Result<()> {
            loop {
                let task_idx = next_task.fetch_add(1, Ordering::Relaxed);
                if task_idx >= task_count {
                    break;
                }

                match mode {
                    WorkMode::Proof => run_proof_task(&x, y_ref.as_slice())?,
                    WorkMode::Group => run_group_task(&x, y_ref.as_slice())?,
                }
            }
            Ok(())
        }));
    }

    for handle in handles {
        match handle.join() {
            Ok(res) => res?,
            Err(_) => anyhow::bail!("benchmark worker thread panicked"),
        }
    }

    let duration = started_at.elapsed();
    let proofs_per_sec = (total_proofs as f64) / duration.as_secs_f64();

    println!("Duration: {}", format_duration(duration));
    println!("Throughput: {:.2} proofs/s", proofs_per_sec);
    Ok(())
}

fn run_proof_task(x: &[u8], y_ref: &[u8]) -> anyhow::Result<()> {
    let out = prove_one_weso_fast_streaming_getblock_opt(
        &BENCH_CHALLENGE,
        x,
        y_ref,
        BENCH_DISCRIMINANT_BITS,
        BENCH_ITERS,
    )
    .context("bench prove_one_weso_fast_streaming_getblock_opt")?;
    validate_output(&out, y_ref)?;
    Ok(())
}

fn run_group_task(x: &[u8], y_ref: &[u8]) -> anyhow::Result<()> {
    let jobs = vec![
        ChiavdfBatchJob {
            y_ref_s: y_ref,
            num_iterations: BENCH_ITERS,
        };
        GROUP_PROOFS_PER_BATCH
    ];
    let out = prove_one_weso_fast_streaming_getblock_opt_batch(
        &BENCH_CHALLENGE,
        x,
        BENCH_DISCRIMINANT_BITS,
        &jobs,
    )
    .context("bench prove_one_weso_fast_streaming_getblock_opt_batch")?;

    if out.len() != GROUP_PROOFS_PER_BATCH {
        anyhow::bail!(
            "unexpected batch output count (got {}, expected {})",
            out.len(),
            GROUP_PROOFS_PER_BATCH
        );
    }

    for item in &out {
        validate_output(item, y_ref)?;
    }
    Ok(())
}

fn validate_output(out: &[u8], y_ref: &[u8]) -> anyhow::Result<()> {
    if out.len() < 2 || out.len() % 2 != 0 {
        anyhow::bail!("unexpected output length {}", out.len());
    }
    let (y, witness) = out.split_at(out.len() / 2);
    if y != y_ref {
        anyhow::bail!("output mismatch against benchmark y_ref");
    }
    if witness.is_empty() {
        anyhow::bail!("empty witness");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_output;

    #[test]
    fn validate_output_checks_length_and_payload() {
        let y_ref = vec![1_u8, 2_u8];

        assert!(validate_output(&[1_u8, 2_u8, 9_u8, 9_u8], &y_ref).is_ok());
        assert!(validate_output(&[1_u8, 2_u8, 9_u8], &y_ref).is_err());
        assert!(validate_output(&[1_u8, 3_u8, 9_u8, 9_u8], &y_ref).is_err());
    }
}
