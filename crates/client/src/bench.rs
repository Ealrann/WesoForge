use std::time::Instant;

use anyhow::Context;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;

use bbr_client_chiavdf_fast::{
    last_streaming_parameters, last_streaming_stats, prove_one_weso_fast,
    prove_one_weso_fast_streaming, prove_one_weso_fast_streaming_getblock_opt,
};

use crate::constants::default_classgroup_element;
use crate::format::{format_duration, format_number};

pub fn run_benchmark(algo: u32) -> anyhow::Result<()> {
    const BENCH_DISCRIMINANT_BITS: usize = 1024;
    const BENCH_ITERS: u64 = 14_576_841;
    const WARMUP_ITERS: u64 = 10_000;
    const BENCH_Y_REF_B64: &str = "AABi49IsOPkm3kNS+NW8BLw7jLR/QG2nKwsJ4VIRB+o+C5HAtC7XLoCvOHx/8CIA7fxD1esqHcB+RftlEwdKIMM692W2YUI7xwt4VJe3UoPc3zffkeZ5elOWDP/PO7DL00QBAA==";
    const BENCH_CHALLENGE: [u8; 32] = [
        0x62, 0x62, 0x72, 0x2d, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x2d, 0x62, 0x65, 0x6e, 0x63,
        0x68, 0x2d, 0x76, 0x31, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
        0x0b, 0x0c,
    ];

    let x = default_classgroup_element();

    match algo {
        0 => {
            let _ =
                prove_one_weso_fast(&BENCH_CHALLENGE, &x, BENCH_DISCRIMINANT_BITS, WARMUP_ITERS)
                    .context("warmup prove_one_weso_fast")?;

            let started_at = Instant::now();
            let out =
                prove_one_weso_fast(&BENCH_CHALLENGE, &x, BENCH_DISCRIMINANT_BITS, BENCH_ITERS)
                    .context("bench prove_one_weso_fast")?;
            let duration = started_at.elapsed();

            let half = out.len() / 2;
            let y = &out[..half];
            let witness = &out[half..];

            println!("Benchmark algo: {algo}");
            println!("Discriminant bits: {BENCH_DISCRIMINANT_BITS}");
            println!("Challenge (b64): {}", B64.encode(BENCH_CHALLENGE));
            println!("Iterations: {}", format_number(BENCH_ITERS));
            println!("Y (b64): {}", B64.encode(y));
            println!("Witness (b64): {}", B64.encode(witness));
            println!("Duration: {}", format_duration(duration));
            Ok(())
        }
        1 => {
            if BENCH_Y_REF_B64.starts_with("<fill-me") {
                anyhow::bail!(
                    "bench vector missing: set BENCH_Y_REF_B64 from the `Y (b64)` output of `--bench 0`"
                );
            }

            let y_ref = B64
                .decode(BENCH_Y_REF_B64.as_bytes())
                .context("decode BENCH_Y_REF_B64")?;

            let _ =
                prove_one_weso_fast(&BENCH_CHALLENGE, &x, BENCH_DISCRIMINANT_BITS, WARMUP_ITERS)
                    .context("warmup prove_one_weso_fast")?;

            let started_at = Instant::now();
            let out = prove_one_weso_fast_streaming(
                &BENCH_CHALLENGE,
                &x,
                &y_ref,
                BENCH_DISCRIMINANT_BITS,
                BENCH_ITERS,
            )
            .context("bench prove_one_weso_fast_streaming")?;
            let duration = started_at.elapsed();

            let half = out.len() / 2;
            let y = &out[..half];
            let witness = &out[half..];

            println!("Benchmark algo: {algo}");
            println!("Discriminant bits: {BENCH_DISCRIMINANT_BITS}");
            println!("Challenge (b64): {}", B64.encode(BENCH_CHALLENGE));
            println!("Iterations: {}", format_number(BENCH_ITERS));
            if let Some(params) = last_streaming_parameters() {
                println!(
                    "Params: k={} l={} (tuned={})",
                    params.k, params.l, params.tuned
                );
            }
            if let Some(stats) = last_streaming_stats() {
                let checkpoint_overhead = stats
                    .checkpoint_event_time
                    .saturating_sub(stats.checkpoint_time);
                let accounted = stats.checkpoint_time + checkpoint_overhead + stats.finalize_time;
                let other = duration.checked_sub(accounted).unwrap_or_default();
                println!(
                    "Timing: checkpoint_time={} (calls={} updates={}), checkpoint_overhead={}, finalize_time={}, other_time={}",
                    format_duration(stats.checkpoint_time),
                    format_number(stats.checkpoint_calls),
                    format_number(stats.bucket_updates),
                    format_duration(checkpoint_overhead),
                    format_duration(stats.finalize_time),
                    format_duration(other),
                );
            }
            println!("Y (b64): {}", B64.encode(y));
            println!("Witness (b64): {}", B64.encode(witness));
            println!("Duration: {}", format_duration(duration));
            Ok(())
        }
        2 => {
            if BENCH_Y_REF_B64.starts_with("<fill-me") {
                anyhow::bail!(
                    "bench vector missing: set BENCH_Y_REF_B64 from the `Y (b64)` output of `--bench 0`"
                );
            }

            let y_ref = B64
                .decode(BENCH_Y_REF_B64.as_bytes())
                .context("decode BENCH_Y_REF_B64")?;

            let _ =
                prove_one_weso_fast(&BENCH_CHALLENGE, &x, BENCH_DISCRIMINANT_BITS, WARMUP_ITERS)
                    .context("warmup prove_one_weso_fast")?;

            let started_at = Instant::now();
            let out = prove_one_weso_fast_streaming_getblock_opt(
                &BENCH_CHALLENGE,
                &x,
                &y_ref,
                BENCH_DISCRIMINANT_BITS,
                BENCH_ITERS,
            )
            .context("bench prove_one_weso_fast_streaming_getblock_opt")?;
            let duration = started_at.elapsed();

            let half = out.len() / 2;
            let y = &out[..half];
            let witness = &out[half..];

            println!("Benchmark algo: {algo}");
            println!("Discriminant bits: {BENCH_DISCRIMINANT_BITS}");
            println!("Challenge (b64): {}", B64.encode(BENCH_CHALLENGE));
            println!("Iterations: {}", format_number(BENCH_ITERS));
            if let Some(params) = last_streaming_parameters() {
                println!(
                    "Params: k={} l={} (tuned={})",
                    params.k, params.l, params.tuned
                );
            }
            if let Some(stats) = last_streaming_stats() {
                let checkpoint_overhead = stats
                    .checkpoint_event_time
                    .saturating_sub(stats.checkpoint_time);
                let accounted = stats.checkpoint_time + checkpoint_overhead + stats.finalize_time;
                let other = duration.checked_sub(accounted).unwrap_or_default();
                println!(
                    "Timing: checkpoint_time={} (calls={} updates={}), checkpoint_overhead={}, finalize_time={}, other_time={}",
                    format_duration(stats.checkpoint_time),
                    format_number(stats.checkpoint_calls),
                    format_number(stats.bucket_updates),
                    format_duration(checkpoint_overhead),
                    format_duration(stats.finalize_time),
                    format_duration(other),
                );
            }
            println!("Y (b64): {}", B64.encode(y));
            println!("Witness (b64): {}", B64.encode(witness));
            println!("Duration: {}", format_duration(duration));
            Ok(())
        }
        _ => anyhow::bail!("unknown --bench algo {algo} (supported: 0, 1, 2)"),
    }
}
