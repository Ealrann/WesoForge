use std::io::Write as _;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SubmitterConfig {
    #[serde(default)]
    pub reward_address: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
}

impl SubmitterConfig {
    fn normalize(&mut self) {
        self.reward_address = self.reward_address.as_ref().map(|s| s.trim().to_string());
        if matches!(self.reward_address.as_deref(), Some(s) if s.is_empty()) {
            self.reward_address = None;
        }

        self.name = self.name.as_ref().map(|s| s.trim().to_string());
        if matches!(self.name.as_deref(), Some(s) if s.is_empty()) {
            self.name = None;
        }
    }
}

fn xdg_config_home() -> anyhow::Result<PathBuf> {
    if let Some(dir) = std::env::var_os("XDG_CONFIG_HOME") {
        let dir = PathBuf::from(dir);
        if dir.as_os_str().is_empty() {
            anyhow::bail!("XDG_CONFIG_HOME is set but empty");
        }
        return Ok(dir);
    }

    let home = std::env::var_os("HOME").ok_or_else(|| anyhow::anyhow!("HOME is not set"))?;
    let home = PathBuf::from(home);
    if home.as_os_str().is_empty() {
        anyhow::bail!("HOME is set but empty");
    }
    Ok(home.join(".config"))
}

pub fn submitter_config_path() -> anyhow::Result<PathBuf> {
    Ok(xdg_config_home()?.join("bbr-client").join("config.json"))
}

pub fn load_submitter_config() -> anyhow::Result<Option<SubmitterConfig>> {
    let path = submitter_config_path()?;
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(&path)?;
    let mut cfg: SubmitterConfig = serde_json::from_str(&raw)?;
    cfg.normalize();
    Ok(Some(cfg))
}

pub fn save_submitter_config(cfg: &SubmitterConfig) -> anyhow::Result<()> {
    let path = submitter_config_path()?;
    let dir = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("invalid config path: {}", path.display()))?;
    std::fs::create_dir_all(dir)?;

    let mut cfg = cfg.clone();
    cfg.normalize();

    let json = serde_json::to_string_pretty(&cfg)?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, json)?;
    std::fs::rename(tmp, path)?;
    Ok(())
}

pub fn ensure_submitter_config(interactive: bool) -> anyhow::Result<Option<SubmitterConfig>> {
    match load_submitter_config() {
        Ok(Some(cfg)) => return Ok(Some(cfg)),
        Ok(None) => {}
        Err(err) => {
            if !interactive {
                return Err(err);
            }
            eprintln!("warning: failed to read submitter config (will recreate): {err:#}");
        }
    }
    if !interactive {
        return Ok(None);
    }

    let cfg = prompt_submitter_config()?;
    save_submitter_config(&cfg)?;
    Ok(Some(cfg))
}

fn prompt_line(prompt: &str) -> anyhow::Result<String> {
    let mut out = std::io::stdout();
    out.write_all(prompt.as_bytes())?;
    out.flush()?;

    let mut buf = String::new();
    std::io::stdin().read_line(&mut buf)?;
    Ok(buf.trim().to_string())
}

fn prompt_submitter_config() -> anyhow::Result<SubmitterConfig> {
    let path = submitter_config_path()?;
    println!("First-run setup (saved to {}).", path.display());
    println!("Press ENTER to leave a field empty.");

    let reward_address = loop {
        let v = prompt_line("Reward address (xch…): ")?;
        if v.is_empty() || v.starts_with("xch") {
            break v;
        }
        println!("Invalid address: expected an xch… address (or leave empty).");
    };
    let name = prompt_line("Name: ")?;

    let mut cfg = SubmitterConfig {
        reward_address: Some(reward_address),
        name: Some(name),
    };
    cfg.normalize();
    Ok(cfg)
}
