use clap::Parser;
use reqwest::Url;

pub fn default_parallel_proofs() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

pub fn default_worker_id() -> String {
    std::env::var("HOSTNAME").unwrap_or_else(|_| "bbr-client".to_string())
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

#[derive(Debug, Clone, Parser)]
#[command(name = "bbr-client", version, about = "BBR compact proof worker")]
pub struct Cli {
    #[arg(long, env = "BBR_BACKEND_URL", default_value = "http://127.0.0.1:8080")]
    pub backend_url: Url,

    #[arg(long, env = "BBR_WORKER_ID")]
    pub worker_id: Option<String>,

    /// Number of proof workers to run in parallel (controller mode).
    #[arg(
        short = 'p',
        long,
        env = "BBR_PARALLEL_PROOFS",
        default_value_t = default_parallel_proofs()
    )]
    pub parallel: usize,

    #[arg(long, env = "BBR_NO_TUI", default_value_t = false)]
    pub no_tui: bool,

    /// Memory budget per worker process for streaming proof generation (e.g. `128MB`).
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

    /// Hidden: run as a child worker process (controller spawns these).
    #[arg(long, hide = true)]
    pub worker: bool,
}
