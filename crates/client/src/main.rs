mod backend;
mod bench;
mod cli;
mod constants;
mod controller;
mod format;
mod protocol;
mod shutdown;
mod submitter;
mod terminal;
mod worker;

use clap::Parser;
use std::io::IsTerminal;

use bbr_client_chiavdf_fast::{set_bucket_memory_budget_bytes, set_enable_streaming_stats};

use crate::bench::run_benchmark;
use crate::cli::Cli;
use crate::controller::run_controller;
use crate::submitter::ensure_submitter_config;
use crate::worker::run_worker;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    if let Some(algo) = cli.bench {
        set_bucket_memory_budget_bytes(cli.mem_budget_bytes);
        set_enable_streaming_stats(true);
        run_benchmark(algo)?;
        return Ok(());
    }

    if cli.worker {
        return run_worker(cli.mem_budget_bytes).await;
    }

    let interactive = std::io::stdin().is_terminal();
    if let Err(err) = ensure_submitter_config(interactive) {
        eprintln!("warning: failed to read/write submitter config: {err:#}");
    }

    let code = run_controller(cli).await?;
    if code != 0 {
        std::process::exit(code);
    }
    Ok(())
}
