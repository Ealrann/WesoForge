use clap::{Parser, ValueEnum};
use reqwest::Url;

use bbr_client_engine::PinMode;

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
    /// Fetch and compute individual proofs.
    Proof,
    /// Fetch and compute grouped proofs (shared squaring, default).
    Group,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum PinArg {
    /// Do not pin worker compute threads (default).
    Off,
    /// Pin worker compute threads to shared L3 cache CPU sets (Linux best-effort).
    L3,
}

impl From<PinArg> for PinMode {
    fn from(value: PinArg) -> Self {
        match value {
            PinArg::Off => PinMode::Off,
            PinArg::L3 => PinMode::L3,
        }
    }
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
    #[arg(long, env = "BBR_MODE", value_enum, default_value_t = WorkMode::Group)]
    pub mode: WorkMode,

    #[arg(long, env = "BBR_NO_TUI", default_value_t = false)]
    pub no_tui: bool,

    /// CPU pinning strategy (Linux only; ignored on other platforms).
    #[arg(long, env = "BBR_PIN", value_enum, default_value_t = PinArg::Off)]
    pub pin: PinArg,

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

    /// Run a local benchmark and exit.
    ///
    /// Uses current `--mode` and `--parallel` settings.
    #[arg(long)]
    pub bench: bool,
}
