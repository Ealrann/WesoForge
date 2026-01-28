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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InflightGroupEntry {
    pub(crate) group_id: u64,
    pub(crate) lease_id: String,
    pub(crate) lease_expires_at: i64,
    pub(crate) jobs: Vec<BackendJobDto>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct InflightFile {
    #[serde(default)]
    version: u32,
    #[serde(default)]
    jobs: Vec<InflightJobEntry>,
    #[serde(default)]
    groups: Vec<InflightGroupEntry>,
}

pub(crate) struct InflightStore {
    path: PathBuf,
    jobs_by_id: BTreeMap<u64, InflightJobEntry>,
    groups_by_id: BTreeMap<u64, InflightGroupEntry>,
    job_to_group: BTreeMap<u64, u64>,
}

impl InflightStore {
    pub(crate) fn load() -> anyhow::Result<Option<Self>> {
        let path = inflight_path()?;
        if !path.exists() {
            return Ok(Some(Self {
                path,
                jobs_by_id: BTreeMap::new(),
                groups_by_id: BTreeMap::new(),
                job_to_group: BTreeMap::new(),
            }));
        }

        let raw = std::fs::read_to_string(&path)?;
        let file: InflightFile = serde_json::from_str(&raw)?;
        let mut jobs_by_id = BTreeMap::new();
        for entry in file.jobs {
            jobs_by_id.insert(entry.job.job_id, entry);
        }
        let mut groups_by_id = BTreeMap::new();
        let mut job_to_group = BTreeMap::new();
        for group in file.groups {
            for job in &group.jobs {
                job_to_group.insert(job.job_id, group.group_id);
            }
            groups_by_id.insert(group.group_id, group);
        }

        Ok(Some(Self {
            path,
            jobs_by_id,
            groups_by_id,
            job_to_group,
        }))
    }

    pub(crate) fn job_entries(&self) -> impl Iterator<Item = &InflightJobEntry> {
        self.jobs_by_id.values()
    }

    pub(crate) fn group_entries(&self) -> impl Iterator<Item = &InflightGroupEntry> {
        self.groups_by_id.values()
    }

    pub(crate) fn total_jobs(&self) -> usize {
        let group_jobs: usize = self.groups_by_id.values().map(|g| g.jobs.len()).sum();
        group_jobs + self.jobs_by_id.len()
    }

    pub(crate) fn promote_jobs_to_groups_by_challenge(&mut self, max_group_jobs: u32) -> bool {
        if !self.groups_by_id.is_empty() || self.jobs_by_id.is_empty() {
            return false;
        }

        let max_group_jobs = max_group_jobs.clamp(1, 200) as usize;
        let jobs_by_id = std::mem::take(&mut self.jobs_by_id);

        let mut buckets: BTreeMap<(String, i64, String), Vec<BackendJobDto>> = BTreeMap::new();
        for (_job_id, entry) in jobs_by_id {
            let key = (
                entry.lease_id,
                entry.lease_expires_at,
                entry.job.challenge_b64.clone(),
            );
            buckets.entry(key).or_default().push(entry.job);
        }

        for ((lease_id, lease_expires_at, _challenge_b64), mut jobs) in buckets {
            while !jobs.is_empty() {
                let chunk_len = jobs.len().min(max_group_jobs);
                let chunk: Vec<BackendJobDto> = jobs.drain(0..chunk_len).collect();
                if chunk.is_empty() {
                    break;
                }

                let group_id = chunk[0].job_id;
                for job in &chunk {
                    self.job_to_group.insert(job.job_id, group_id);
                }
                self.groups_by_id.insert(
                    group_id,
                    InflightGroupEntry {
                        group_id,
                        lease_id: lease_id.clone(),
                        lease_expires_at,
                        jobs: chunk,
                    },
                );
            }
        }

        !self.groups_by_id.is_empty()
    }

    pub(crate) fn insert_job(
        &mut self,
        lease_id: String,
        lease_expires_at: i64,
        job: BackendJobDto,
    ) -> bool {
        let job_id = job.job_id;
        if let Some(group_id) = self.job_to_group.remove(&job_id) {
            let mut remove_group = false;
            if let Some(group) = self.groups_by_id.get_mut(&group_id) {
                group.jobs.retain(|j| j.job_id != job_id);
                remove_group = group.jobs.is_empty();
            }
            if remove_group {
                self.groups_by_id.remove(&group_id);
            }
        }
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

    pub(crate) fn insert_group(
        &mut self,
        group_id: u64,
        lease_id: String,
        lease_expires_at: i64,
        jobs: Vec<BackendJobDto>,
    ) -> bool {
        let mut changed = false;

        if let Some(prev_group) = self.groups_by_id.remove(&group_id) {
            changed = true;
            for job in prev_group.jobs {
                self.job_to_group.remove(&job.job_id);
            }
        }

        let mut entry = InflightGroupEntry {
            group_id,
            lease_id,
            lease_expires_at,
            jobs,
        };

        for job in &entry.jobs {
            let job_id = job.job_id;
            if self.jobs_by_id.remove(&job_id).is_some() {
                changed = true;
            }

            if let Some(prev_group_id) = self.job_to_group.get(&job_id).copied() {
                if prev_group_id != group_id {
                    let mut remove_group = false;
                    if let Some(prev_group) = self.groups_by_id.get_mut(&prev_group_id) {
                        prev_group.jobs.retain(|j| j.job_id != job_id);
                        remove_group = prev_group.jobs.is_empty();
                    }
                    if remove_group {
                        self.groups_by_id.remove(&prev_group_id);
                    }
                    self.job_to_group.remove(&job_id);
                    changed = true;
                }
            }

            self.job_to_group.insert(job_id, group_id);
        }

        // Ensure group entry doesn't contain jobs that we may have just moved out of it above.
        entry.jobs.retain(|j| self.job_to_group.get(&j.job_id).copied() == Some(group_id));

        let prev = self.groups_by_id.insert(group_id, entry);
        if prev.is_none() {
            return true;
        }
        changed
    }

    pub(crate) fn remove_job(&mut self, job_id: u64) -> bool {
        if self.jobs_by_id.remove(&job_id).is_some() {
            return true;
        }

        let Some(group_id) = self.job_to_group.remove(&job_id) else {
            return false;
        };

        let mut removed = true;
        let mut remove_group = false;
        if let Some(group) = self.groups_by_id.get_mut(&group_id) {
            let before = group.jobs.len();
            group.jobs.retain(|j| j.job_id != job_id);
            removed = before != group.jobs.len();
            remove_group = group.jobs.is_empty();
        }
        if remove_group {
            self.groups_by_id.remove(&group_id);
        }
        removed
    }

    pub(crate) async fn persist(&self) -> anyhow::Result<()> {
        let path = self.path.clone();
        let file = InflightFile {
            version: 2,
            jobs: self.jobs_by_id.values().cloned().collect(),
            groups: self.groups_by_id.values().cloned().collect(),
        };

        tokio::task::spawn_blocking(move || persist_file(&path, &file))
            .await
            .map_err(|err| anyhow::anyhow!("persist inflight leases: {err:#}"))??;
        Ok(())
    }
}

fn persist_file(path: &Path, file: &InflightFile) -> anyhow::Result<()> {
    if file.jobs.is_empty() && file.groups.is_empty() {
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
