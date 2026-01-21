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

    /// Run a local benchmark (e.g. `--bench 0`) and exit.
    #[arg(long, value_name = "ALGO")]
    pub bench: Option<u32>,

    /// Hidden: run as a child worker process (controller spawns these).
    #[arg(long, hide = true)]
    pub worker: bool,
}
