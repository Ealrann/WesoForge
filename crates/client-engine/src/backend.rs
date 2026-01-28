use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use reqwest::header;
use reqwest::Url;
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub(crate) enum BackendError {
    #[error("invalid reward address")]
    InvalidRewardAddress,
    #[error("invalid or expired lease")]
    LeaseInvalid,
    #[error("lease conflict")]
    LeaseConflict,
    #[error("job not found")]
    JobNotFound,
}

#[derive(Debug, Deserialize)]
struct ApiErrorBody {
    code: String,
}

fn is_html_error(content_type: &str, body: &str) -> bool {
    if content_type.to_ascii_lowercase().contains("text/html") {
        return true;
    }
    let trimmed = body.trim_start();
    trimmed.starts_with("<!DOCTYPE")
        || trimmed.starts_with("<!doctype")
        || trimmed.starts_with("<html")
        || trimmed.starts_with("<HTML")
        || trimmed.starts_with('<')
}

fn truncate_one_line(body: &str, max_len: usize) -> String {
    let mut out = String::with_capacity(body.len().min(max_len));
    for ch in body.chars() {
        if out.len() >= max_len {
            out.push('…');
            break;
        }
        match ch {
            '\r' | '\n' | '\t' => out.push(' '),
            ch => out.push(ch),
        }
    }
    out.trim().to_string()
}

async fn error_from_response(res: reqwest::Response) -> anyhow::Error {
    let status = res.status();
    let url = res.url().clone();
    let content_type = res
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let body = res.text().await.unwrap_or_default();

    if let Ok(err) = serde_json::from_str::<ApiErrorBody>(&body) {
        if status == reqwest::StatusCode::BAD_REQUEST && err.code == "invalid_reward_address" {
            return BackendError::InvalidRewardAddress.into();
        }
        if status == reqwest::StatusCode::NOT_FOUND && err.code == "job_not_found" {
            return BackendError::JobNotFound.into();
        }
        if status == reqwest::StatusCode::CONFLICT {
            if err.code == "lease_invalid" {
                return BackendError::LeaseInvalid.into();
            }
            return BackendError::LeaseConflict.into();
        }
        return anyhow::anyhow!("backend error ({status}) for {url}: {}", err.code);
    }

    if status == reqwest::StatusCode::CONFLICT {
        return BackendError::LeaseConflict.into();
    }

    if status == reqwest::StatusCode::NOT_FOUND {
        return BackendError::JobNotFound.into();
    }

    if is_html_error(&content_type, &body) {
        return anyhow::anyhow!("backend error ({status}) for {url}");
    }

    let snippet = truncate_one_line(&body, 200);
    if snippet.is_empty() {
        anyhow::anyhow!("backend error ({status}) for {url}")
    } else {
        anyhow::anyhow!("backend error ({status}) for {url}: {snippet}")
    }
}

#[derive(Debug, Serialize)]
struct WorkRequest {
    count: u32,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BackendWorkBatch {
    pub(crate) lease_id: String,
    pub(crate) lease_expires_at: i64,
    pub(crate) jobs: Vec<BackendJobDto>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct BackendJobDto {
    pub(crate) job_id: u64,
    pub(crate) height: u32,
    pub(crate) field_vdf: i32,
    pub(crate) challenge_b64: String,
    pub(crate) number_of_iterations: u64,
    pub(crate) output_b64: String,
}

#[derive(Debug, Serialize)]
struct SubmitRequest {
    lease_id: String,
    witness_b64: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reward_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SubmitResponse {
    pub(crate) reason: String,
    pub(crate) detail: String,
}

pub(crate) async fn fetch_work(
    http: &reqwest::Client,
    backend: &Url,
    count: u32,
) -> anyhow::Result<BackendWorkBatch> {
    let url = backend.join("api/jobs/lease_proofs")?;
    let res = http
        .post(url)
        .json(&WorkRequest { count })
        .send()
        .await?;

    if !res.status().is_success() {
        return Err(error_from_response(res).await);
    }
    Ok(res.json().await?)
}

pub(crate) async fn submit_job(
    http: &reqwest::Client,
    backend: &Url,
    job_id: u64,
    lease_id: &str,
    witness: &[u8],
    reward_address: Option<&str>,
    name: Option<&str>,
) -> anyhow::Result<SubmitResponse> {
    let url = backend.join(&format!("api/jobs/{job_id}/submit"))?;
    let res = http
        .post(url)
        .json(&SubmitRequest {
            lease_id: lease_id.to_string(),
            witness_b64: B64.encode(witness),
            reward_address: reward_address.map(str::to_string),
            name: name.map(str::to_string),
        })
        .send()
        .await?;

    if !res.status().is_success() {
        return Err(error_from_response(res).await);
    }
    Ok(res.json().await?)
}
