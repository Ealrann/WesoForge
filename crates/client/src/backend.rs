use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use reqwest::Url;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
struct WorkRequest {
    count: u32,
    worker_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct WorkBatch {
    pub lease_id: String,
    pub lease_expires_at: i64,
    pub jobs: Vec<JobDto>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct JobDto {
    pub job_id: u64,
    pub height: u32,
    pub field_vdf: i32,
    pub challenge_b64: String,
    pub number_of_iterations: u64,
    pub output_b64: String,
}

#[derive(Debug, Serialize)]
struct SubmitRequest {
    lease_id: String,
    witness_b64: String,
}

#[derive(Debug, Deserialize)]
pub struct SubmitResponse {
    pub reason: String,
    pub detail: String,
    pub accepted_event_id: Option<u64>,
}

pub async fn fetch_work(
    http: &reqwest::Client,
    backend: &Url,
    worker_id: &str,
    count: u32,
) -> anyhow::Result<WorkBatch> {
    let url = backend.join("api/jobs/lease_proofs")?;
    let res = http
        .post(url)
        .json(&WorkRequest {
            count,
            worker_id: Some(worker_id.to_string()),
        })
        .send()
        .await?;

    if !res.status().is_success() {
        let status = res.status();
        let body = res.text().await.unwrap_or_default();
        anyhow::bail!("http {status}: {body}");
    }
    Ok(res.json().await?)
}

pub async fn submit_job(
    http: &reqwest::Client,
    backend: &Url,
    job_id: u64,
    lease_id: &str,
    witness: &[u8],
) -> anyhow::Result<SubmitResponse> {
    let url = backend.join(&format!("api/jobs/{job_id}/submit"))?;
    let res = http
        .post(url)
        .json(&SubmitRequest {
            lease_id: lease_id.to_string(),
            witness_b64: B64.encode(witness),
        })
        .send()
        .await?;

    if !res.status().is_success() {
        let status = res.status();
        let body = res.text().await.unwrap_or_default();
        anyhow::bail!("http {status}: {body}");
    }
    Ok(res.json().await?)
}
