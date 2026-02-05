mod bench;
mod cli;
mod constants;
mod format;
mod shutdown;
mod terminal;
mod ui;

use clap::Parser;
use std::io::IsTerminal;
use std::time::Duration;

use bbr_client_chiavdf_fast::{set_bucket_memory_budget_bytes, set_enable_streaming_stats};
use bbr_client_core::submitter::{SubmitterConfig, ensure_submitter_config};
use bbr_client_engine::{EngineConfig, EngineEvent, start_engine};

use crate::bench::run_benchmark;
use crate::cli::{Cli, WorkMode};
use crate::constants::PROGRESS_BAR_STEPS;
use crate::format::{format_job_done_line, humanize_submit_reason};
use crate::shutdown::{ShutdownController, ShutdownEvent, spawn_ctrl_c_handler};
use crate::terminal::TuiTerminal;
use crate::ui::Ui;

fn format_outcome_status(outcome: &bbr_client_engine::JobOutcome) -> String {
    if let Some(err) = &outcome.error {
        return err.clone();
    }

    let reason = outcome.submit_reason.as_deref().unwrap_or("unknown").trim();
    let mut status = humanize_submit_reason(reason);

    if outcome.output_mismatch {
        status.push_str(" (output mismatch)");
    }
    if let Some(detail) = outcome.submit_detail.as_deref() {
        if !detail.is_empty() && detail != reason {
            status.push_str(&format!(" ({detail})"));
        }
    }
    status
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    if cli.bench {
        set_bucket_memory_budget_bytes(cli.mem_budget_bytes);
        set_enable_streaming_stats(true);
        run_benchmark()?;
        return Ok(());
    }

    let interactive = std::io::stdin().is_terminal();
    let submitter = match ensure_submitter_config(interactive) {
        Ok(Some(cfg)) => cfg,
        Ok(None) => SubmitterConfig::default(),
        Err(err) => {
            eprintln!("warning: failed to read/write submitter config: {err:#}");
            SubmitterConfig::default()
        }
    };

    if cli.parallel == 0 {
        anyhow::bail!("--parallel must be >= 1");
    }
    let parallel = cli.parallel as usize;

    let tui_enabled = !cli.no_tui && std::io::stdout().is_terminal();
    let warn_tui_too_many_workers = tui_enabled && parallel > 32;
    let progress_steps = if tui_enabled { PROGRESS_BAR_STEPS } else { 0 };

    let use_groups = cli.mode == WorkMode::Group;

    let engine = start_engine(EngineConfig {
        backend_url: cli.backend_url.clone(),
        parallel,
        use_groups,
        mem_budget_bytes: cli.mem_budget_bytes,
        submitter,
        idle_sleep: Duration::ZERO,
        progress_steps,
        progress_tick: Duration::ZERO,
        recent_jobs_max: 0,
        pin_mode: cli.pin.into(),
    });

    let mut events = engine.subscribe();

    let shutdown = std::sync::Arc::new(ShutdownController::new());
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::unbounded_channel::<ShutdownEvent>();
    let tui_terminal = if tui_enabled && std::io::stdin().is_terminal() {
        Some(TuiTerminal::enter(shutdown.clone(), shutdown_tx.clone())?)
    } else {
        None
    };
    if tui_terminal.is_none() {
        spawn_ctrl_c_handler(shutdown.clone(), shutdown_tx);
    }

    let startup = format!(
        "wesoforge {} parallel={}",
        env!("CARGO_PKG_VERSION"),
        parallel
    );

    let mut ui = if tui_enabled {
        Some(Ui::new(parallel))
    } else {
        None
    };
    if let Some(ui) = &ui {
        ui.println(&startup);
    } else {
        println!("{startup}");
    }
    if warn_tui_too_many_workers {
        let msg = format!(
            "warning: --parallel={} is high; TUI rendering is not optimized for this many progress bars. Consider running with --no-tui.",
            parallel
        );
        if let Some(ui) = &ui {
            ui.println(&msg);
        } else {
            eprintln!("{msg}");
        }
    }

    let mut worker_busy = vec![false; parallel];
    let mut worker_speed: Vec<u64> = vec![0; parallel];

    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut immediate_exit = false;

    loop {
        tokio::select! {
            ev_opt = shutdown_rx.recv() => {
                match ev_opt {
                    Some(ShutdownEvent::Graceful) => {
                        if let Some(ui) = &mut ui {
                            ui.set_stop_message("Stop requested — finishing current work before exiting (press CTRL+C again to exit immediately).");
                        } else {
                            eprintln!("Stop requested — finishing current work before exiting (press CTRL+C again to exit immediately).");
                        }
                        engine.request_stop();
                    }
                    Some(ShutdownEvent::Immediate) => {
                        if let Some(ui) = &mut ui {
                            ui.set_stop_message("Stop requested again — exiting immediately.");
                        } else {
                            eprintln!("Stop requested again — exiting immediately.");
                        }
                        immediate_exit = true;
                        break;
                    }
                    None => {}
                }
            }
            _ = ticker.tick(), if tui_enabled => {
                if let Some(ui) = &ui {
                    let busy = worker_busy.iter().filter(|v| **v).count();
                    let speed: u64 = worker_speed.iter().sum();
                    ui.tick_global(speed, busy, parallel);
                }
            }
            evt = events.recv() => {
                let evt = match evt {
                    Ok(v) => v,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                };

                match evt {
                    EngineEvent::Started | EngineEvent::StopRequested => {}
                    EngineEvent::WorkerJobStarted { worker_idx, job } => {
                        if let Some(slot) = worker_busy.get_mut(worker_idx) {
                            *slot = true;
                        }
                        if let Some(ui) = &mut ui {
                            ui.set_worker_job(worker_idx, &job);
                        }
                    }
                    EngineEvent::WorkerProgress { worker_idx, iters_done, iters_per_sec, .. } => {
                        if let Some(slot) = worker_speed.get_mut(worker_idx) {
                            *slot = iters_per_sec;
                        }
                        if let Some(ui) = &mut ui {
                            ui.set_worker_progress(worker_idx, iters_done);
                        }
                    }
                    EngineEvent::WorkerStage { .. } => {}
                    EngineEvent::JobFinished { outcome } => {
                        let worker_idx = outcome.worker_idx;
                        if let Some(slot) = worker_busy.get_mut(worker_idx) {
                            *slot = false;
                        }
                        if let Some(slot) = worker_speed.get_mut(worker_idx) {
                            *slot = 0;
                        }
                        if let Some(ui) = &mut ui {
                            ui.set_worker_idle(worker_idx);
                        }

                        let status = format_outcome_status(&outcome);
                        let duration = Duration::from_millis(outcome.total_ms);
                        let line = format_job_done_line(
                            outcome.job.height,
                            outcome.job.field_vdf,
                            &status,
                            outcome.job.number_of_iterations,
                            duration,
                        );

                        if let Some(ui) = &ui {
                            ui.println(&line);
                        } else {
                            println!("{line}");
                        }
                    }
                    EngineEvent::Warning { message } => {
                        if let Some(ui) = &ui {
                            ui.println(&message);
                        } else {
                            eprintln!("{message}");
                        }
                    }
                    EngineEvent::Error { message } => {
                        if let Some(ui) = &ui {
                            ui.println(&message);
                        } else {
                            eprintln!("{message}");
                        }
                    }
                    EngineEvent::Stopped => break,
                }
            }
        }
    }

    if let Some(ui) = &ui {
        ui.freeze();
    }

    if immediate_exit {
        drop(tui_terminal);
        std::process::exit(130);
    }

    engine.wait().await?;
    Ok(())
}
