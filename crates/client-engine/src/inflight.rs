use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::backend::BackendJobDto;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InflightJobEntry {
    pub(crate) lease_id: String,
    pub(crate) lease_expires_at: i64,
    pub(crate) job: BackendJobDto,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct InflightFile {
    #[serde(default)]
    version: u32,
    #[serde(default)]
    jobs: Vec<InflightJobEntry>,
}

pub(crate) struct InflightStore {
    path: PathBuf,
    jobs_by_id: BTreeMap<u64, InflightJobEntry>,
}

impl InflightStore {
    pub(crate) fn load() -> anyhow::Result<Option<Self>> {
        let path = inflight_path()?;
        if !path.exists() {
            return Ok(Some(Self {
                path,
                jobs_by_id: BTreeMap::new(),
            }));
        }

        let raw = std::fs::read_to_string(&path)?;
        let file: InflightFile = serde_json::from_str(&raw)?;
        let mut jobs_by_id = BTreeMap::new();
        for entry in file.jobs {
            jobs_by_id.insert(entry.job.job_id, entry);
        }

        Ok(Some(Self { path, jobs_by_id }))
    }

    pub(crate) fn entries(&self) -> impl Iterator<Item = &InflightJobEntry> {
        self.jobs_by_id.values()
    }

    pub(crate) fn insert_job(
        &mut self,
        lease_id: String,
        lease_expires_at: i64,
        job: BackendJobDto,
    ) -> bool {
        let job_id = job.job_id;
        let lease_id_for_cmp = lease_id.clone();
        let entry = InflightJobEntry {
            lease_id,
            lease_expires_at,
            job,
        };
        match self.jobs_by_id.insert(job_id, entry) {
            None => true,
            Some(prev) => prev.lease_id != lease_id_for_cmp || prev.lease_expires_at != lease_expires_at,
        }
    }

    pub(crate) fn remove_job(&mut self, job_id: u64) -> bool {
        self.jobs_by_id.remove(&job_id).is_some()
    }

    pub(crate) async fn persist(&self) -> anyhow::Result<()> {
        let path = self.path.clone();
        let file = InflightFile {
            version: 1,
            jobs: self.jobs_by_id.values().cloned().collect(),
        };

        tokio::task::spawn_blocking(move || persist_file(&path, &file))
            .await
            .map_err(|err| anyhow::anyhow!("persist inflight leases: {err:#}"))??;
        Ok(())
    }
}

fn persist_file(path: &Path, file: &InflightFile) -> anyhow::Result<()> {
    if file.jobs.is_empty() {
        if path.exists() {
            let _ = std::fs::remove_file(path);
        }
        return Ok(());
    }

    let dir = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("invalid inflight path: {}", path.display()))?;
    std::fs::create_dir_all(dir)?;

    let json = serde_json::to_string_pretty(file)?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, json)?;
    std::fs::rename(tmp, path)?;
    Ok(())
}

fn xdg_state_home() -> anyhow::Result<PathBuf> {
    if let Some(dir) = std::env::var_os("XDG_STATE_HOME") {
        let dir = PathBuf::from(dir);
        if dir.as_os_str().is_empty() {
            anyhow::bail!("XDG_STATE_HOME is set but empty");
        }
        return Ok(dir);
    }

    // On Windows we should not require HOME to be set: the conventional location
    // for per-user application state is %LOCALAPPDATA%.
    #[cfg(windows)]
    {
        if let Some(dir) = std::env::var_os("LOCALAPPDATA") {
            let dir = PathBuf::from(dir);
            if dir.as_os_str().is_empty() {
                anyhow::bail!("LOCALAPPDATA is set but empty");
            }
            return Ok(dir);
        }

        // Some restricted environments might not set LOCALAPPDATA; fall back to APPDATA.
        if let Some(dir) = std::env::var_os("APPDATA") {
            let dir = PathBuf::from(dir);
            if dir.as_os_str().is_empty() {
                anyhow::bail!("APPDATA is set but empty");
            }
            return Ok(dir);
        }

        // Fall back to USERPROFILE if available.
        if let Some(dir) = std::env::var_os("USERPROFILE") {
            let dir = PathBuf::from(dir);
            if dir.as_os_str().is_empty() {
                anyhow::bail!("USERPROFILE is set but empty");
            }
            return Ok(dir.join("AppData").join("Local"));
        }
    }

    let home = std::env::var_os("HOME").ok_or_else(|| anyhow::anyhow!("HOME is not set"))?;
    let home = PathBuf::from(home);
    if home.as_os_str().is_empty() {
        anyhow::bail!("HOME is set but empty");
    }
    Ok(home.join(".local").join("state"))
}

fn inflight_path() -> anyhow::Result<PathBuf> {
    Ok(xdg_state_home()?
        .join("bbr-client")
        .join("inflight-leases.json"))
}
