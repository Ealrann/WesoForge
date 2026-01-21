use std::time::{Duration, Instant};

use anyhow::Context;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use chrono::Utc;
use tokio::io::{AsyncBufReadExt, BufReader};

use bbr_client_chiavdf_fast::{
    prove_one_weso_fast_streaming, prove_one_weso_fast_streaming_with_progress,
};

use crate::backend::submit_job;
use crate::constants::{DISCRIMINANT_BITS, default_classgroup_element};
use crate::format::{
    field_vdf_label, format_duration, format_job_done_line, humanize_submit_reason,
};
use crate::protocol::{WorkerCommand, WorkerJobSpec, WorkerUpdate, worker_send_update};

#[derive(Debug)]
struct JobOutcome {
    height: u32,
    field_vdf: i32,
    status: String,
    number_of_iterations: u64,
    duration: Duration,
}

impl JobOutcome {
    fn new(
        height: u32,
        field_vdf: i32,
        status: String,
        number_of_iterations: u64,
        duration: Duration,
    ) -> Self {
        Self {
            height,
            field_vdf,
            status,
            number_of_iterations,
            duration,
        }
    }

    fn done_line(&self) -> String {
        format_job_done_line(
            self.height,
            self.field_vdf,
            &self.status,
            self.number_of_iterations,
            self.duration,
        )
    }
}

struct WorkerRunner {
    http: reqwest::Client,
}

impl WorkerRunner {
    async fn handle_command(&self, cmd: WorkerCommand) -> Option<WorkerUpdate> {
        match cmd {
            WorkerCommand::Stop => None,
            WorkerCommand::Job(spec) => {
                let outcome = self.run_job(spec).await;
                Some(WorkerUpdate::Done(outcome.done_line()))
            }
        }
    }

    async fn run_job(&self, spec: WorkerJobSpec) -> JobOutcome {
        let started_at = Instant::now();
        let height = spec.job.height;
        let field_vdf = spec.job.field_vdf;
        let number_of_iterations = spec.job.number_of_iterations;

        let output = match B64.decode(spec.job.output_b64.as_bytes()) {
            Ok(v) => v,
            Err(err) => {
                eprintln!("worker decode output_b64 error: {err:#}");
                return JobOutcome::new(
                    height,
                    field_vdf,
                    "Error (bad output_b64)".to_string(),
                    number_of_iterations,
                    started_at.elapsed(),
                );
            }
        };
        let challenge = match B64.decode(spec.job.challenge_b64.as_bytes()) {
            Ok(v) => v,
            Err(err) => {
                eprintln!("worker decode challenge_b64 error: {err:#}");
                return JobOutcome::new(
                    height,
                    field_vdf,
                    "Error (bad challenge_b64)".to_string(),
                    number_of_iterations,
                    started_at.elapsed(),
                );
            }
        };

        let (witness, output_mismatch) = match self
            .compute_witness(&spec, output, challenge, started_at)
            .await
        {
            Ok(v) => v,
            Err(status) => {
                return JobOutcome::new(
                    height,
                    field_vdf,
                    status,
                    number_of_iterations,
                    started_at.elapsed(),
                );
            }
        };

        let status = match self
            .submit_witness(&spec, &witness, output_mismatch, started_at)
            .await
        {
            Ok(status) => status,
            Err(status) => status,
        };

        JobOutcome::new(
            height,
            field_vdf,
            status,
            number_of_iterations,
            started_at.elapsed(),
        )
    }

    async fn compute_witness(
        &self,
        spec: &WorkerJobSpec,
        output: Vec<u8>,
        challenge: Vec<u8>,
        started_at: Instant,
    ) -> Result<(Vec<u8>, bool), String> {
        let mut last_compute_err: Option<String> = None;
        loop {
            let now = Utc::now().timestamp();
            if now >= spec.lease_expires_at {
                return Err("Error (lease expired)".to_string());
            }

            let progress_interval = spec.progress_interval;
            let total_iters = spec.job.number_of_iterations;
            let discriminant_bits = DISCRIMINANT_BITS;
            let challenge = challenge.clone();
            let output = output.clone();

            let compute =
                tokio::task::spawn_blocking(move || -> anyhow::Result<(Vec<u8>, bool)> {
                    let x = default_classgroup_element();
                    let out = if progress_interval == 0 {
                        prove_one_weso_fast_streaming(
                            &challenge,
                            &x,
                            &output,
                            discriminant_bits,
                            total_iters,
                        )
                        .context("chiavdf prove_one_weso_fast_streaming")?
                    } else {
                        prove_one_weso_fast_streaming_with_progress(
                            &challenge,
                            &x,
                            &output,
                            discriminant_bits,
                            total_iters,
                            progress_interval,
                            move |iters_done| {
                                worker_send_update(WorkerUpdate::Progress(iters_done))
                            },
                        )
                        .context("chiavdf prove_one_weso_fast_streaming_with_progress")?
                    };

                    if progress_interval != 0 {
                        worker_send_update(WorkerUpdate::Progress(total_iters));
                    }

                    let half = out.len() / 2;
                    let y = &out[..half];
                    let witness = out[half..].to_vec();
                    Ok((witness, y != output))
                })
                .await;

            let (witness, output_mismatch) = match compute {
                Ok(Ok(v)) => v,
                Ok(Err(err)) => {
                    let err_msg = format!("{err:#}");
                    if last_compute_err.as_deref() != Some(&err_msg) {
                        last_compute_err = Some(err_msg.clone());
                        eprintln!(
                            "Block {} ({}) compute error: {err_msg} (after {})",
                            spec.job.height,
                            field_vdf_label(spec.job.field_vdf),
                            format_duration(started_at.elapsed())
                        );
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
                Err(err) => {
                    let err_msg = format!("{err:#}");
                    if last_compute_err.as_deref() != Some(&err_msg) {
                        last_compute_err = Some(err_msg.clone());
                        eprintln!(
                            "Block {} ({}) compute task error: {err_msg} (after {})",
                            spec.job.height,
                            field_vdf_label(spec.job.field_vdf),
                            format_duration(started_at.elapsed())
                        );
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };

            return Ok((witness, output_mismatch));
        }
    }

    async fn submit_witness(
        &self,
        spec: &WorkerJobSpec,
        witness: &[u8],
        output_mismatch: bool,
        started_at: Instant,
    ) -> Result<String, String> {
        let mut last_submit_err: Option<String> = None;
        loop {
            let now = Utc::now().timestamp();
            if now >= spec.lease_expires_at {
                return Err("Error (lease expired)".to_string());
            }

            match submit_job(
                &self.http,
                &spec.backend_url,
                spec.job.job_id,
                &spec.lease_id,
                witness,
            )
            .await
            {
                Ok(res) => {
                    let mut status = humanize_submit_reason(&res.reason);
                    if output_mismatch {
                        status.push_str(" (output mismatch)");
                    }
                    if let Some(id) = res.accepted_event_id {
                        status.push_str(&format!(" (event {id})"));
                    }
                    if !res.detail.is_empty() && res.detail != res.reason {
                        status.push_str(&format!(" ({})", res.detail));
                    }
                    return Ok(status);
                }
                Err(err) => {
                    let err_msg = format!("{err:#}");
                    if last_submit_err.as_deref() != Some(&err_msg) {
                        last_submit_err = Some(err_msg.clone());
                        eprintln!(
                            "Block {} ({}) submit error: {err_msg} (after {})",
                            spec.job.height,
                            field_vdf_label(spec.job.field_vdf),
                            format_duration(started_at.elapsed())
                        );
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
            }
        }
    }
}

pub async fn run_worker() -> anyhow::Result<()> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .context("build http client")?;

    let runner = WorkerRunner { http };

    tokio::spawn(async { while tokio::signal::ctrl_c().await.is_ok() {} });

    let mut lines = BufReader::new(tokio::io::stdin()).lines();

    while let Some(line) = lines.next_line().await? {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let cmd = match WorkerCommand::parse_line(line) {
            Ok(v) => v,
            Err(err) => {
                eprintln!("worker: parse command error: {err:#}");
                continue;
            }
        };

        let Some(update) = runner.handle_command(cmd).await else {
            break;
        };
        worker_send_update(update);
    }

    Ok(())
}
