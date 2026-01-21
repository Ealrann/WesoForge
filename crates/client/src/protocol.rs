use std::io::Write;

use anyhow::Context;
use reqwest::Url;

use crate::backend::JobDto;

#[derive(Debug, Clone)]
pub struct WorkerJobSpec {
    pub backend_url: Url,
    pub lease_id: String,
    pub lease_expires_at: i64,
    pub progress_interval: u64,
    pub job: JobDto,
}

#[derive(Debug, Clone)]
pub enum WorkerCommand {
    Job(WorkerJobSpec),
    Stop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerUpdate {
    Progress(u64),
    Done(String),
}

impl WorkerCommand {
    pub fn to_line(&self) -> String {
        match self {
            WorkerCommand::Job(spec) => {
                format!(
                    "JOB {} {} {} {} {} {} {} {} {} {}\n",
                    spec.backend_url,
                    spec.lease_id,
                    spec.lease_expires_at,
                    spec.progress_interval,
                    spec.job.job_id,
                    spec.job.height,
                    spec.job.field_vdf,
                    spec.job.number_of_iterations,
                    spec.job.challenge_b64,
                    spec.job.output_b64
                )
            }
            WorkerCommand::Stop => "STOP\n".to_string(),
        }
    }

    pub fn parse_line(line: &str) -> anyhow::Result<Self> {
        let line = line.trim();
        if line == "STOP" {
            return Ok(WorkerCommand::Stop);
        }
        if let Some(rest) = line.strip_prefix("JOB ") {
            return Ok(WorkerCommand::Job(parse_job_line(rest)?));
        }
        anyhow::bail!("invalid command line: {line}");
    }
}

impl WorkerUpdate {
    pub fn to_line(&self) -> String {
        match self {
            WorkerUpdate::Progress(iters_done) => format!("PROGRESS {iters_done}\n"),
            WorkerUpdate::Done(line) => {
                if line.is_empty() {
                    "DONE\n".to_string()
                } else {
                    format!("DONE {line}\n")
                }
            }
        }
    }

    pub fn parse_line(line: &str) -> anyhow::Result<Self> {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("PROGRESS ") {
            let iters_done = rest.trim().parse::<u64>().context("parse PROGRESS")?;
            return Ok(WorkerUpdate::Progress(iters_done));
        }
        if line == "DONE" {
            return Ok(WorkerUpdate::Done(String::new()));
        }
        if let Some(rest) = line.strip_prefix("DONE ") {
            return Ok(WorkerUpdate::Done(rest.to_string()));
        }
        anyhow::bail!("invalid update line: {line}")
    }
}

pub fn worker_send_update(update: WorkerUpdate) {
    let mut out = std::io::stdout();
    let _ = out.write_all(update.to_line().as_bytes());
    let _ = out.flush();
}

fn parse_job_line(line: &str) -> anyhow::Result<WorkerJobSpec> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() != 10 {
        anyhow::bail!("invalid JOB line (expected 10 fields, got {})", parts.len());
    }

    let backend_url = Url::parse(parts[0]).context("parse backend_url")?;
    let lease_id = parts[1].to_string();
    let lease_expires_at = parts[2].parse::<i64>().context("parse lease_expires_at")?;
    let progress_interval = parts[3].parse::<u64>().context("parse progress_interval")?;
    let job_id = parts[4].parse::<u64>().context("parse job_id")?;
    let height = parts[5].parse::<u32>().context("parse height")?;
    let field_vdf = parts[6].parse::<i32>().context("parse field_vdf")?;
    let number_of_iterations = parts[7]
        .parse::<u64>()
        .context("parse number_of_iterations")?;
    let challenge_b64 = parts[8].to_string();
    let output_b64 = parts[9].to_string();

    Ok(WorkerJobSpec {
        backend_url,
        lease_id,
        lease_expires_at,
        progress_interval,
        job: JobDto {
            job_id,
            height,
            field_vdf,
            challenge_b64,
            number_of_iterations,
            output_b64,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_round_trip_preserves_padding() {
        let spec = WorkerJobSpec {
            backend_url: Url::parse("http://127.0.0.1:8080").unwrap(),
            lease_id: "lease-123".to_string(),
            lease_expires_at: 123456,
            progress_interval: 42,
            job: JobDto {
                job_id: 7,
                height: 99,
                field_vdf: 3,
                challenge_b64: "AAA=".to_string(),
                number_of_iterations: 1234,
                output_b64: "AA==".to_string(),
            },
        };

        let line = WorkerCommand::Job(spec.clone()).to_line();
        let parsed = WorkerCommand::parse_line(&line).unwrap();
        let WorkerCommand::Job(parsed_spec) = parsed else {
            panic!("expected job command");
        };

        assert_eq!(parsed_spec.backend_url, spec.backend_url);
        assert_eq!(parsed_spec.lease_id, spec.lease_id);
        assert_eq!(parsed_spec.lease_expires_at, spec.lease_expires_at);
        assert_eq!(parsed_spec.progress_interval, spec.progress_interval);
        assert_eq!(parsed_spec.job.job_id, spec.job.job_id);
        assert_eq!(parsed_spec.job.height, spec.job.height);
        assert_eq!(parsed_spec.job.field_vdf, spec.job.field_vdf);
        assert_eq!(
            parsed_spec.job.number_of_iterations,
            spec.job.number_of_iterations
        );
        assert_eq!(parsed_spec.job.challenge_b64, spec.job.challenge_b64);
        assert_eq!(parsed_spec.job.output_b64, spec.job.output_b64);
    }

    #[test]
    fn parse_rejects_wrong_field_count() {
        let err = WorkerCommand::parse_line("JOB a b c").unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("expected 10 fields"));
    }

    #[test]
    fn parse_rejects_bad_integers() {
        let line = "JOB http://127.0.0.1:8080 lease abc 1 1 1 1 1 AAA= AA==";
        let err = WorkerCommand::parse_line(line).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("lease_expires_at"));
    }

    #[test]
    fn parse_updates_and_stop() {
        assert_eq!(
            WorkerUpdate::parse_line("PROGRESS 123").unwrap(),
            WorkerUpdate::Progress(123)
        );
        assert_eq!(
            WorkerUpdate::parse_line("DONE ok").unwrap(),
            WorkerUpdate::Done("ok".to_string())
        );
        assert_eq!(
            WorkerUpdate::parse_line("DONE").unwrap(),
            WorkerUpdate::Done(String::new())
        );
        assert!(matches!(
            WorkerCommand::parse_line("STOP").unwrap(),
            WorkerCommand::Stop
        ));
    }
}
