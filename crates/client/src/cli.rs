use clap::{Parser, ValueEnum};
use reqwest::Url;

use bbr_client_engine::EngineConfig;

#[cfg(feature = "prod-backend")]
const DEFAULT_BACKEND_URL: &str = "https://weso.forgeros.fr/";

#[cfg(not(feature = "prod-backend"))]
const DEFAULT_BACKEND_URL: &str = "http://127.0.0.1:8080";

fn default_backend_url() -> Url {
    Url::parse(DEFAULT_BACKEND_URL).expect("DEFAULT_BACKEND_URL must be a valid URL")
}

pub fn default_parallel_proofs() -> u16 {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .min(512) as u16
}

fn parse_mem_budget_bytes(input: &str) -> Result<u64, String> {
    let s = input.trim();
    if s.is_empty() {
        return Err("mem budget must not be empty".to_string());
    }

    let lower = s.to_ascii_lowercase();
    let (num, scale) = if let Some(raw) = lower.strip_suffix("kib") {
        (raw, 1024u64)
    } else if let Some(raw) = lower.strip_suffix("mib") {
        (raw, 1024u64 * 1024)
    } else if let Some(raw) = lower.strip_suffix("gib") {
        (raw, 1024u64 * 1024 * 1024)
    } else if let Some(raw) = lower.strip_suffix("kb") {
        (raw, 1000u64)
    } else if let Some(raw) = lower.strip_suffix("mb") {
        (raw, 1000u64 * 1000)
    } else if let Some(raw) = lower.strip_suffix("gb") {
        (raw, 1000u64 * 1000 * 1000)
    } else if let Some(raw) = lower.strip_suffix('b') {
        (raw, 1u64)
    } else {
        // Default unit is MiB to match typical user expectations (e.g. "128").
        (lower.as_str(), 1024u64 * 1024)
    };

    let num = num.trim();
    if num.is_empty() {
        return Err(format!("invalid mem budget: {input:?}"));
    }

    let value: u64 = num
        .parse()
        .map_err(|_| format!("invalid mem budget number: {input:?}"))?;

    value
        .checked_mul(scale)
        .ok_or_else(|| format!("mem budget too large: {input:?}"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum WorkMode {
    /// Fetch and compute individual proofs (default).
    Proof,
    /// Fetch and compute 4-proof “groups” (shared squaring).
    Group,
}

#[derive(Debug, Clone, Parser)]
#[command(name = "wesoforge", version, about = "WesoForge compact proof worker")]
pub struct Cli {
    #[arg(long, env = "BBR_BACKEND_URL", default_value_t = default_backend_url())]
    pub backend_url: Url,

    /// Number of proof workers to run in parallel.
    #[arg(
        short = 'p',
        long,
        env = "BBR_PARALLEL_PROOFS",
        default_value_t = default_parallel_proofs(),
        value_parser = clap::value_parser!(u16).range(1..=512)
    )]
    pub parallel: u16,

    /// Work mode: individual proofs or grouped proofs.
    #[arg(long, env = "BBR_MODE", value_enum, default_value_t = WorkMode::Proof)]
    pub mode: WorkMode,

    /// Max number of proofs per group request (only used with `--mode group`).
    #[arg(
        long = "group-max-proofs",
        env = "BBR_GROUP_MAX_PROOFS_PER_GROUP",
        default_value_t = EngineConfig::DEFAULT_GROUP_MAX_PROOFS_PER_GROUP
    )]
    pub group_max_proofs_per_group: u32,

    #[arg(long, env = "BBR_NO_TUI", default_value_t = false)]
    pub no_tui: bool,

    /// Memory budget per worker for streaming proof generation (e.g. `128MB`).
    ///
    /// This is used by the `(k,l)` parameter tuner in the native prover.
    #[arg(
        short = 'm',
        long = "mem",
        env = "BBR_MEM_BUDGET",
        default_value = "128MB",
        value_parser = parse_mem_budget_bytes
    )]
    pub mem_budget_bytes: u64,

    /// Run a local benchmark (e.g. `--bench 0`) and exit.
    #[arg(long, value_name = "ALGO")]
    pub bench: Option<u32>,
}
