#[cfg(target_os = "linux")]
use std::collections::BTreeMap;

use crate::api::PinMode;

#[derive(Debug, Clone)]
pub(crate) struct PinningPlan {
    mode: PinMode,
    l3_domains: Vec<Vec<usize>>,
}

impl PinningPlan {
    pub(crate) fn build(mode: PinMode) -> Self {
        match mode {
            PinMode::Off => Self {
                mode,
                l3_domains: Vec::new(),
            },
            PinMode::L3 => {
                #[cfg(target_os = "linux")]
                {
                    let l3_domains = discover_l3_domains_linux();
                    return Self { mode, l3_domains };
                }
                #[cfg(not(target_os = "linux"))]
                {
                    Self {
                        mode: PinMode::Off,
                        l3_domains: Vec::new(),
                    }
                }
            }
        }
    }

    pub(crate) fn is_effective(&self) -> bool {
        match self.mode {
            PinMode::Off => false,
            PinMode::L3 => !self.l3_domains.is_empty(),
        }
    }

    pub(crate) fn domain_count(&self) -> usize {
        match self.mode {
            PinMode::Off => 0,
            PinMode::L3 => self.l3_domains.len(),
        }
    }

    pub(crate) fn pin_current_thread_for_worker(&self, worker_idx: usize) -> Result<(), String> {
        if self.mode == PinMode::Off {
            return Ok(());
        }

        #[cfg(target_os = "linux")]
        {
            let Some(cpus) = self.l3_cpus_for_worker(worker_idx) else {
                return Ok(());
            };
            bbr_client_affinity::set_current_thread_affinity(cpus).map_err(|e| format!("{e}"))?;
            Ok(())
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = worker_idx;
            Ok(())
        }
    }

    #[cfg(target_os = "linux")]
    fn l3_cpus_for_worker(&self, worker_idx: usize) -> Option<&[usize]> {
        let domains = &self.l3_domains;
        if domains.is_empty() {
            return None;
        }
        let domain_idx = worker_idx % domains.len();
        let cpus = domains.get(domain_idx)?;
        if cpus.is_empty() {
            None
        } else {
            Some(cpus.as_slice())
        }
    }
}

#[cfg(target_os = "linux")]
fn discover_l3_domains_linux() -> Vec<Vec<usize>> {
    let mut domains: BTreeMap<Vec<usize>, Vec<usize>> = BTreeMap::new();

    let Ok(entries) = std::fs::read_dir("/sys/devices/system/cpu") else {
        return Vec::new();
    };

    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("cpu") {
            continue;
        }
        if name[3..].chars().any(|c| !c.is_ascii_digit()) {
            continue;
        }

        let path = entry
            .path()
            .join("cache")
            .join("index3")
            .join("shared_cpu_list");
        let Ok(raw) = std::fs::read_to_string(&path) else {
            continue;
        };
        let Some(mut cpus) = parse_cpu_list(&raw) else {
            continue;
        };
        if cpus.is_empty() {
            continue;
        }
        cpus.sort_unstable();
        cpus.dedup();
        domains.entry(cpus.clone()).or_insert(cpus);
    }

    domains.into_values().collect()
}

#[cfg(target_os = "linux")]
fn parse_cpu_list(input: &str) -> Option<Vec<usize>> {
    let mut cpus = Vec::new();
    for part in input.trim().split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((a, b)) = part.split_once('-') {
            let start: usize = a.trim().parse().ok()?;
            let end: usize = b.trim().parse().ok()?;
            if end < start {
                return None;
            }
            cpus.extend(start..=end);
        } else {
            let cpu: usize = part.parse().ok()?;
            cpus.push(cpu);
        }
    }
    Some(cpus)
}
