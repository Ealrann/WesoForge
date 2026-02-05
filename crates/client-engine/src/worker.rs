use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Context;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use chrono::Utc;
use reqwest::Url;
use tokio::sync::mpsc;

use bbr_client_chiavdf_fast::{
    ChiavdfBatchJob, prove_one_weso_fast_streaming_getblock_opt_batch,
    prove_one_weso_fast_streaming_getblock_opt_batch_with_progress,
};
use bbr_client_core::submitter::SubmitterConfig;

use crate::api::{JobOutcome, JobSummary, WorkerStage};
use crate::backend::{BackendError, BackendJobDto, SubmitResponse, submit_job};
use crate::pinning::PinningPlan;

const DISCRIMINANT_BITS: usize = 1024;

fn default_classgroup_element() -> [u8; 100] {
    let mut el = [0u8; 100];
    el[0] = 0x08;
    el
}

#[derive(Debug)]
struct SubmitFailure {
    message: String,
    drop_inflight: bool,
}

pub(crate) enum WorkerCommand {
    Job {
        worker_idx: usize,
        backend_url: Url,
        lease_id: String,
        lease_expires_at: i64,
        progress_steps: u64,
        job: BackendJobDto,
    },
    Group {
        worker_idx: usize,
        backend_url: Url,
        lease_id: String,
        lease_expires_at: i64,
        progress_steps: u64,
        group_id: u64,
        jobs: Vec<BackendJobDto>,
    },
    Stop,
}

pub(crate) enum WorkerInternalEvent {
    StageChanged {
        worker_idx: usize,
        stage: WorkerStage,
    },
    WorkFinished {
        worker_idx: usize,
        outcomes: Vec<JobOutcome>,
    },
    Warning {
        message: String,
    },
    Error {
        message: String,
    },
}

pub(crate) async fn run_worker_task(
    _worker_idx: usize,
    mut rx: mpsc::Receiver<WorkerCommand>,
    internal_tx: mpsc::UnboundedSender<WorkerInternalEvent>,
    progress: Arc<AtomicU64>,
    http: reqwest::Client,
    submitter: Arc<tokio::sync::RwLock<SubmitterConfig>>,
    warned_invalid_reward_address: Arc<AtomicBool>,
    pinning: Arc<PinningPlan>,
) {
    let warned_pinning_failed = Arc::new(AtomicBool::new(false));
    while let Some(cmd) = rx.recv().await {
        match cmd {
            WorkerCommand::Stop => break,
            WorkerCommand::Job {
                worker_idx,
                backend_url,
                lease_id,
                lease_expires_at,
                progress_steps,
                job,
            } => {
                let outcome = run_job(
                    worker_idx,
                    &internal_tx,
                    progress.clone(),
                    &http,
                    &submitter,
                    warned_invalid_reward_address.clone(),
                    pinning.clone(),
                    warned_pinning_failed.clone(),
                    backend_url,
                    lease_id,
                    lease_expires_at,
                    progress_steps,
                    job,
                )
                .await;
                let _ = internal_tx.send(WorkerInternalEvent::WorkFinished {
                    worker_idx,
                    outcomes: vec![outcome],
                });
            }
            WorkerCommand::Group {
                worker_idx,
                backend_url,
                lease_id,
                lease_expires_at,
                progress_steps,
                group_id,
                jobs,
            } => {
                let outcomes = run_group(
                    worker_idx,
                    &internal_tx,
                    progress.clone(),
                    &http,
                    &submitter,
                    warned_invalid_reward_address.clone(),
                    pinning.clone(),
                    warned_pinning_failed.clone(),
                    backend_url,
                    lease_id,
                    lease_expires_at,
                    progress_steps,
                    group_id,
                    jobs,
                )
                .await;
                let _ = internal_tx.send(WorkerInternalEvent::WorkFinished {
                    worker_idx,
                    outcomes,
                });
            }
        }
    }
}

async fn run_job(
    worker_idx: usize,
    internal_tx: &mpsc::UnboundedSender<WorkerInternalEvent>,
    progress: Arc<AtomicU64>,
    http: &reqwest::Client,
    submitter: &tokio::sync::RwLock<SubmitterConfig>,
    warned_invalid_reward_address: Arc<AtomicBool>,
    pinning: Arc<PinningPlan>,
    warned_pinning_failed: Arc<AtomicBool>,
    backend_url: Url,
    lease_id: String,
    lease_expires_at: i64,
    progress_steps: u64,
    job: BackendJobDto,
) -> JobOutcome {
    let started_at = Instant::now();

    let job_summary = JobSummary {
        job_id: job.job_id,
        group_proofs: None,
        height: job.height,
        field_vdf: job.field_vdf,
        number_of_iterations: job.number_of_iterations,
    };

    let output = match B64.decode(job.output_b64.as_bytes()) {
        Ok(v) => v,
        Err(err) => {
            return JobOutcome {
                worker_idx,
                job: job_summary,
                output_mismatch: false,
                submit_reason: None,
                submit_detail: None,
                drop_inflight: false,
                error: Some(format!("Error (bad output_b64: {err:#})")),
                compute_ms: 0,
                submit_ms: 0,
                total_ms: started_at.elapsed().as_millis() as u64,
            };
        }
    };
    let challenge = match B64.decode(job.challenge_b64.as_bytes()) {
        Ok(v) => v,
        Err(err) => {
            return JobOutcome {
                worker_idx,
                job: job_summary,
                output_mismatch: false,
                submit_reason: None,
                submit_detail: None,
                drop_inflight: false,
                error: Some(format!("Error (bad challenge_b64: {err:#})")),
                compute_ms: 0,
                submit_ms: 0,
                total_ms: started_at.elapsed().as_millis() as u64,
            };
        }
    };

    let _ = internal_tx.send(WorkerInternalEvent::StageChanged {
        worker_idx,
        stage: WorkerStage::Computing,
    });

    let compute_started_at = Instant::now();
    let (witness, output_mismatch) = match compute_witness(
        worker_idx,
        internal_tx,
        progress.clone(),
        pinning.clone(),
        warned_pinning_failed.clone(),
        job.number_of_iterations,
        progress_steps,
        challenge,
        output.clone(),
    )
    .await
    {
        Ok(v) => v,
        Err(status) => {
            return JobOutcome {
                worker_idx,
                job: job_summary,
                output_mismatch: false,
                submit_reason: None,
                submit_detail: None,
                drop_inflight: false,
                error: Some(status),
                compute_ms: compute_started_at.elapsed().as_millis() as u64,
                submit_ms: 0,
                total_ms: started_at.elapsed().as_millis() as u64,
            };
        }
    };
    let compute_ms = compute_started_at.elapsed().as_millis() as u64;

    let _ = internal_tx.send(WorkerInternalEvent::StageChanged {
        worker_idx,
        stage: WorkerStage::Submitting,
    });

    let submit_started_at = Instant::now();
    let submit_res = submit_witness(
        http,
        submitter,
        warned_invalid_reward_address,
        internal_tx,
        &backend_url,
        job.job_id,
        &lease_id,
        lease_expires_at,
        &witness,
    )
    .await;
    let submit_ms = submit_started_at.elapsed().as_millis() as u64;

    match submit_res {
        Ok(res) => JobOutcome {
            worker_idx,
            job: job_summary,
            output_mismatch,
            submit_reason: Some(res.reason),
            submit_detail: Some(res.detail),
            drop_inflight: false,
            error: None,
            compute_ms,
            submit_ms,
            total_ms: started_at.elapsed().as_millis() as u64,
        },
        Err(err) => JobOutcome {
            worker_idx,
            job: job_summary,
            output_mismatch,
            submit_reason: None,
            submit_detail: None,
            drop_inflight: err.drop_inflight,
            error: Some(err.message),
            compute_ms,
            submit_ms,
            total_ms: started_at.elapsed().as_millis() as u64,
        },
    }
}

async fn run_group(
    worker_idx: usize,
    internal_tx: &mpsc::UnboundedSender<WorkerInternalEvent>,
    progress: Arc<AtomicU64>,
    http: &reqwest::Client,
    submitter: &tokio::sync::RwLock<SubmitterConfig>,
    warned_invalid_reward_address: Arc<AtomicBool>,
    pinning: Arc<PinningPlan>,
    warned_pinning_failed: Arc<AtomicBool>,
    backend_url: Url,
    lease_id: String,
    lease_expires_at: i64,
    progress_steps: u64,
    group_id: u64,
    jobs: Vec<BackendJobDto>,
) -> Vec<JobOutcome> {
    let started_at = Instant::now();
    if jobs.is_empty() {
        return Vec::new();
    }

    let now = Utc::now().timestamp();
    if now >= lease_expires_at {
        return jobs
            .into_iter()
            .map(|job| JobOutcome {
                worker_idx,
                job: JobSummary {
                    job_id: job.job_id,
                    group_proofs: None,
                    height: job.height,
                    field_vdf: job.field_vdf,
                    number_of_iterations: job.number_of_iterations,
                },
                output_mismatch: false,
                submit_reason: None,
                submit_detail: None,
                drop_inflight: true,
                error: Some("Error (lease expired)".to_string()),
                compute_ms: 0,
                submit_ms: 0,
                total_ms: started_at.elapsed().as_millis() as u64,
            })
            .collect();
    }

    let challenge_b64 = jobs[0].challenge_b64.clone();
    let challenge = match B64.decode(challenge_b64.as_bytes()) {
        Ok(v) => v,
        Err(err) => {
            let err = format!("Error (bad challenge_b64: {err:#})");
            return jobs
                .into_iter()
                .map(|job| JobOutcome {
                    worker_idx,
                    job: JobSummary {
                        job_id: job.job_id,
                        group_proofs: None,
                        height: job.height,
                        field_vdf: job.field_vdf,
                        number_of_iterations: job.number_of_iterations,
                    },
                    output_mismatch: false,
                    submit_reason: None,
                    submit_detail: None,
                    drop_inflight: false,
                    error: Some(err.clone()),
                    compute_ms: 0,
                    submit_ms: 0,
                    total_ms: started_at.elapsed().as_millis() as u64,
                })
                .collect();
        }
    };

    let mut outputs: Vec<Vec<u8>> = Vec::with_capacity(jobs.len());
    let mut iterations: Vec<u64> = Vec::with_capacity(jobs.len());
    let mut total_iters: u64 = 0;
    for job in &jobs {
        if job.challenge_b64 != challenge_b64 {
            let err = format!(
                "Error (group {group_id}: inconsistent challenge_b64 for job {})",
                job.job_id
            );
            return jobs
                .into_iter()
                .map(|job| JobOutcome {
                    worker_idx,
                    job: JobSummary {
                        job_id: job.job_id,
                        group_proofs: None,
                        height: job.height,
                        field_vdf: job.field_vdf,
                        number_of_iterations: job.number_of_iterations,
                    },
                    output_mismatch: false,
                    submit_reason: None,
                    submit_detail: None,
                    drop_inflight: false,
                    error: Some(err.clone()),
                    compute_ms: 0,
                    submit_ms: 0,
                    total_ms: started_at.elapsed().as_millis() as u64,
                })
                .collect();
        }

        match B64.decode(job.output_b64.as_bytes()) {
            Ok(v) => outputs.push(v),
            Err(err) => {
                let err = format!("Error (bad output_b64: {err:#})");
                return jobs
                    .into_iter()
                    .map(|job| JobOutcome {
                        worker_idx,
                        job: JobSummary {
                            job_id: job.job_id,
                            group_proofs: None,
                            height: job.height,
                            field_vdf: job.field_vdf,
                            number_of_iterations: job.number_of_iterations,
                        },
                        output_mismatch: false,
                        submit_reason: None,
                        submit_detail: None,
                        drop_inflight: false,
                        error: Some(err.clone()),
                        compute_ms: 0,
                        submit_ms: 0,
                        total_ms: started_at.elapsed().as_millis() as u64,
                    })
                    .collect();
            }
        }

        iterations.push(job.number_of_iterations);
        total_iters = total_iters.max(job.number_of_iterations);
    }

    let _ = internal_tx.send(WorkerInternalEvent::StageChanged {
        worker_idx,
        stage: WorkerStage::Computing,
    });

    let compute_started_at = Instant::now();
    let witnesses = match compute_witness_batch(
        worker_idx,
        internal_tx.clone(),
        progress.clone(),
        pinning.clone(),
        warned_pinning_failed.clone(),
        total_iters,
        lease_expires_at,
        progress_steps,
        challenge,
        outputs,
        iterations,
    )
    .await
    {
        Ok(v) => v,
        Err(err) => {
            let compute_ms = compute_started_at.elapsed().as_millis() as u64;
            return jobs
                .into_iter()
                .map(|job| JobOutcome {
                    worker_idx,
                    job: JobSummary {
                        job_id: job.job_id,
                        group_proofs: None,
                        height: job.height,
                        field_vdf: job.field_vdf,
                        number_of_iterations: job.number_of_iterations,
                    },
                    output_mismatch: false,
                    submit_reason: None,
                    submit_detail: None,
                    drop_inflight: true,
                    error: Some(err.clone()),
                    compute_ms,
                    submit_ms: 0,
                    total_ms: started_at.elapsed().as_millis() as u64,
                })
                .collect();
        }
    };
    let compute_ms = compute_started_at.elapsed().as_millis() as u64;

    let _ = internal_tx.send(WorkerInternalEvent::StageChanged {
        worker_idx,
        stage: WorkerStage::Submitting,
    });

    let mut out = Vec::with_capacity(jobs.len());
    for (job, (witness, output_mismatch)) in jobs.into_iter().zip(witnesses.into_iter()) {
        let job_summary = JobSummary {
            job_id: job.job_id,
            group_proofs: None,
            height: job.height,
            field_vdf: job.field_vdf,
            number_of_iterations: job.number_of_iterations,
        };

        let submit_started_at = Instant::now();
        let submit_res = submit_witness(
            http,
            submitter,
            warned_invalid_reward_address.clone(),
            internal_tx,
            &backend_url,
            job.job_id,
            &lease_id,
            lease_expires_at,
            &witness,
        )
        .await;
        let submit_ms = submit_started_at.elapsed().as_millis() as u64;

        match submit_res {
            Ok(res) => out.push(JobOutcome {
                worker_idx,
                job: job_summary,
                output_mismatch,
                submit_reason: Some(res.reason),
                submit_detail: Some(res.detail),
                drop_inflight: false,
                error: None,
                compute_ms,
                submit_ms,
                total_ms: started_at.elapsed().as_millis() as u64,
            }),
            Err(err) => out.push(JobOutcome {
                worker_idx,
                job: job_summary,
                output_mismatch,
                submit_reason: None,
                submit_detail: None,
                drop_inflight: err.drop_inflight,
                error: Some(err.message),
                compute_ms,
                submit_ms,
                total_ms: started_at.elapsed().as_millis() as u64,
            }),
        }
    }

    out
}

async fn compute_witness_batch(
    worker_idx: usize,
    internal_tx: mpsc::UnboundedSender<WorkerInternalEvent>,
    progress: Arc<AtomicU64>,
    pinning: Arc<PinningPlan>,
    warned_pinning_failed: Arc<AtomicBool>,
    total_iters: u64,
    lease_expires_at: i64,
    progress_steps: u64,
    challenge: Vec<u8>,
    outputs: Vec<Vec<u8>>,
    iterations: Vec<u64>,
) -> Result<Vec<(Vec<u8>, bool)>, String> {
    let mut last_compute_err: Option<String> = None;

    loop {
        let now = Utc::now().timestamp();
        if now >= lease_expires_at {
            return Err("Error (lease expired)".to_string());
        }

        let total_iters = total_iters.max(1);
        let progress_interval = progress_interval(total_iters, progress_steps);

        let challenge = challenge.clone();
        let outputs = outputs.clone();
        let iterations = iterations.clone();
        let progress_clone = progress.clone();
        let pinning = pinning.clone();
        let warned_pinning_failed = warned_pinning_failed.clone();
        let internal_tx = internal_tx.clone();

        let compute =
            tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<(Vec<u8>, bool)>> {
                if let Err(err) = pinning.pin_current_thread_for_worker(worker_idx) {
                    if !warned_pinning_failed.swap(true, Ordering::Relaxed) {
                        let _ = internal_tx.send(WorkerInternalEvent::Warning {
                            message: format!(
                                "warning: failed to pin worker {} to L3 CPU set: {}",
                                worker_idx + 1,
                                err
                            ),
                        });
                    }
                }
                let x = default_classgroup_element();

                let batch_jobs: Vec<ChiavdfBatchJob<'_>> = outputs
                    .iter()
                    .zip(iterations.iter())
                    .map(|(y_ref, num_iterations)| ChiavdfBatchJob {
                        y_ref_s: y_ref.as_slice(),
                        num_iterations: *num_iterations,
                    })
                    .collect();

                let results =
                    if progress_steps == 0 {
                        prove_one_weso_fast_streaming_getblock_opt_batch(
                            &challenge,
                            &x,
                            DISCRIMINANT_BITS,
                            &batch_jobs,
                        )
                        .context("chiavdf prove_one_weso_fast_streaming_getblock_opt_batch")?
                    } else {
                        let progress_for_cb = progress_clone.clone();
                        prove_one_weso_fast_streaming_getblock_opt_batch_with_progress(
                    &challenge,
                    &x,
                    DISCRIMINANT_BITS,
                    &batch_jobs,
                    progress_interval,
                    move |iters_done| {
                        progress_for_cb.store(iters_done, Ordering::Relaxed);
                    },
                )
                .context("chiavdf prove_one_weso_fast_streaming_getblock_opt_batch_with_progress")?
                    };

                progress_clone.store(total_iters, Ordering::Relaxed);

                if results.len() != batch_jobs.len() {
                    anyhow::bail!(
                        "unexpected batch result count (got {}, expected {})",
                        results.len(),
                        batch_jobs.len()
                    );
                }

                let mut out = Vec::with_capacity(batch_jobs.len());
                for (idx, blob) in results.into_iter().enumerate() {
                    let half = blob.len() / 2;
                    let y = &blob[..half];
                    let witness = blob[half..].to_vec();
                    let output_mismatch = y != batch_jobs[idx].y_ref_s;
                    out.push((witness, output_mismatch));
                }

                Ok(out)
            })
            .await;

        match compute {
            Ok(Ok(v)) => return Ok(v),
            Ok(Err(err)) => {
                let err_msg = format!("{err:#}");
                if last_compute_err.as_deref() != Some(&err_msg) {
                    last_compute_err = Some(err_msg.clone());
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
            Err(err) => {
                let err_msg = format!("{err:#}");
                if last_compute_err.as_deref() != Some(&err_msg) {
                    last_compute_err = Some(err_msg.clone());
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
    }
}

pub(crate) async fn compute_witness(
    worker_idx: usize,
    internal_tx: &mpsc::UnboundedSender<WorkerInternalEvent>,
    progress: Arc<AtomicU64>,
    pinning: Arc<PinningPlan>,
    warned_pinning_failed: Arc<AtomicBool>,
    total_iters: u64,
    progress_steps: u64,
    challenge: Vec<u8>,
    output: Vec<u8>,
) -> Result<(Vec<u8>, bool), String> {
    let mut last_compute_err: Option<String> = None;
    let mut last_log_at = Instant::now()
        .checked_sub(Duration::from_secs(3600))
        .unwrap_or_else(Instant::now);
    let mut attempts: u32 = 0;

    loop {
        let total_iters = total_iters.max(1);
        let progress_interval = progress_interval(total_iters, progress_steps);
        let challenge = challenge.clone();
        let output = output.clone();
        let progress_clone = progress.clone();
        let pinning = pinning.clone();
        let warned_pinning_failed = warned_pinning_failed.clone();
        let internal_tx_for_pin = internal_tx.clone();

        let compute = tokio::task::spawn_blocking(move || -> anyhow::Result<(Vec<u8>, bool)> {
            if let Err(err) = pinning.pin_current_thread_for_worker(worker_idx) {
                if !warned_pinning_failed.swap(true, Ordering::Relaxed) {
                    let _ = internal_tx_for_pin.send(WorkerInternalEvent::Warning {
                        message: format!(
                            "warning: failed to pin worker {} to L3 CPU set: {}",
                            worker_idx + 1,
                            err
                        ),
                    });
                }
            }
            let x = default_classgroup_element();
            let out = if progress_steps == 0 {
                bbr_client_chiavdf_fast::prove_one_weso_fast_streaming(
                    &challenge,
                    &x,
                    &output,
                    DISCRIMINANT_BITS,
                    total_iters,
                )
                .context("chiavdf prove_one_weso_fast_streaming")?
            } else {
                let progress_for_cb = progress_clone.clone();
                bbr_client_chiavdf_fast::prove_one_weso_fast_streaming_with_progress(
                    &challenge,
                    &x,
                    &output,
                    DISCRIMINANT_BITS,
                    total_iters,
                    progress_interval,
                    move |iters_done| {
                        progress_for_cb.store(iters_done, Ordering::Relaxed);
                    },
                )
                .context("chiavdf prove_one_weso_fast_streaming_with_progress")?
            };

            progress_clone.store(total_iters, Ordering::Relaxed);

            let half = out.len() / 2;
            let y = &out[..half];
            let witness = out[half..].to_vec();
            Ok((witness, y != output))
        })
        .await;

        match compute {
            Ok(Ok((witness, output_mismatch))) => return Ok((witness, output_mismatch)),
            Ok(Err(err)) => {
                attempts = attempts.saturating_add(1);
                let err_msg = format!("{err:#}");
                let should_log = last_compute_err.as_deref() != Some(&err_msg)
                    || last_log_at.elapsed() >= Duration::from_secs(30);
                if should_log {
                    last_compute_err = Some(err_msg.clone());
                    last_log_at = Instant::now();
                    let _ = internal_tx.send(WorkerInternalEvent::Error {
                        message: format!(
                            "error: worker {} compute failed (attempt {}): {}; retrying in 5s",
                            worker_idx + 1,
                            attempts,
                            err_msg
                        ),
                    });
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
            Err(err) => {
                attempts = attempts.saturating_add(1);
                let err_msg = format!("{err:#}");
                let should_log = last_compute_err.as_deref() != Some(&err_msg)
                    || last_log_at.elapsed() >= Duration::from_secs(30);
                if should_log {
                    last_compute_err = Some(err_msg.clone());
                    last_log_at = Instant::now();
                    let _ = internal_tx.send(WorkerInternalEvent::Error {
                        message: format!(
                            "error: worker {} compute join failed (attempt {}): {}; retrying in 5s",
                            worker_idx + 1,
                            attempts,
                            err_msg
                        ),
                    });
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };
    }
}

fn progress_interval(total_iters: u64, progress_steps: u64) -> u64 {
    if progress_steps == 0 {
        return 0;
    }
    if total_iters == 0 {
        return 1;
    }
    (total_iters.saturating_add(progress_steps - 1) / progress_steps).max(1)
}

async fn submit_witness(
    http: &reqwest::Client,
    submitter: &tokio::sync::RwLock<SubmitterConfig>,
    warned_invalid_reward_address: Arc<AtomicBool>,
    internal_tx: &mpsc::UnboundedSender<WorkerInternalEvent>,
    backend: &Url,
    job_id: u64,
    lease_id: &str,
    lease_expires_at: i64,
    witness: &[u8],
) -> Result<SubmitResponse, SubmitFailure> {
    let mut last_submit_err: Option<String> = None;
    let mut attempts: u32 = 0;
    let mut last_log_at = Instant::now()
        .checked_sub(Duration::from_secs(3600))
        .unwrap_or_else(Instant::now);

    loop {
        let now = Utc::now().timestamp();

        let (reward_address, name) = {
            let cfg = submitter.read().await;
            (cfg.reward_address.clone(), cfg.name.clone())
        };

        match submit_job(
            http,
            backend,
            job_id,
            lease_id,
            witness,
            reward_address.as_deref(),
            name.as_deref(),
        )
        .await
        {
            Ok(res) => return Ok(res),
            Err(err) => {
                attempts = attempts.saturating_add(1);
                if matches!(
                    err.downcast_ref::<BackendError>(),
                    Some(BackendError::LeaseInvalid)
                ) {
                    let _ = internal_tx.send(WorkerInternalEvent::Error {
                        message: format!(
                            "error: submit rejected for job {job_id}: lease invalid/expired"
                        ),
                    });
                    return Err(SubmitFailure {
                        message: "Error (lease invalid/expired)".to_string(),
                        drop_inflight: true,
                    });
                }
                if matches!(
                    err.downcast_ref::<BackendError>(),
                    Some(BackendError::LeaseConflict)
                ) {
                    let _ = internal_tx.send(WorkerInternalEvent::Error {
                        message: format!(
                            "error: submit rejected for job {job_id}: lease conflict (already leased by someone else)"
                        ),
                    });
                    return Err(SubmitFailure {
                        message: "Error (lease conflict)".to_string(),
                        drop_inflight: true,
                    });
                }
                if matches!(
                    err.downcast_ref::<BackendError>(),
                    Some(BackendError::JobNotFound)
                ) {
                    let _ = internal_tx.send(WorkerInternalEvent::Error {
                        message: format!("error: submit rejected for job {job_id}: job not found"),
                    });
                    return Err(SubmitFailure {
                        message: "Error (job not found)".to_string(),
                        drop_inflight: true,
                    });
                }
                if matches!(
                    err.downcast_ref::<BackendError>(),
                    Some(BackendError::InvalidRewardAddress)
                ) && reward_address.is_some()
                {
                    {
                        let mut cfg = submitter.write().await;
                        cfg.reward_address = None;
                    }

                    if !warned_invalid_reward_address.swap(true, Ordering::SeqCst) {
                        let _ = internal_tx.send(WorkerInternalEvent::Warning {
                            message: "warning: backend rejected configured reward address; submitting without reward metadata"
                                .to_string(),
                        });
                    }

                    continue;
                }

                let err_msg = format!("{err:#}");
                let should_log = last_submit_err.as_deref() != Some(&err_msg)
                    || last_log_at.elapsed() >= Duration::from_secs(30);
                if should_log {
                    last_submit_err = Some(err_msg.clone());
                    last_log_at = Instant::now();
                    let expires_in = (lease_expires_at - now).max(0);
                    let _ = internal_tx.send(WorkerInternalEvent::Error {
                        message: format!(
                            "error: submit failed for job {job_id} (attempt {attempts}, lease expires in {expires_in}s): {err_msg}; retrying in 5s"
                        ),
                    });
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        }
    }
}
