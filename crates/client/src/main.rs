mod backend;
mod bench;
mod cli;
mod constants;
mod controller;
mod format;
mod protocol;
mod shutdown;
mod terminal;
mod worker;

use clap::Parser;

use crate::bench::run_benchmark;
use crate::cli::Cli;
use crate::controller::run_controller;
use crate::worker::run_worker;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    if let Some(algo) = cli.bench {
        run_benchmark(algo)?;
        return Ok(());
    }

    if cli.worker {
        return run_worker().await;
    }

    let code = run_controller(cli).await?;
    if code != 0 {
        std::process::exit(code);
    }
    Ok(())
}
