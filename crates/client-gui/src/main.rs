use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use reqwest::Url;
use tauri::{AppHandle, Emitter, State};
use tokio::sync::Mutex;

use bbr_client_core::submitter::{SubmitterConfig, load_submitter_config, save_submitter_config};
use bbr_client_engine::{EngineConfig, EngineEvent, EngineHandle, StatusSnapshot, start_engine};

#[derive(Default)]
struct GuiState {
    engine: Mutex<Option<EngineHandle>>,
}

#[derive(Debug, Clone, Serialize)]
struct WorkerProgressUpdate {
    worker_idx: usize,
    iters_done: u64,
    iters_total: u64,
    iters_per_sec: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
enum GuiEngineEvent {
    ProgressBatch { updates: Vec<WorkerProgressUpdate> },
}

#[derive(Debug, Clone, Deserialize)]
struct StartOptions {
    parallel: Option<u32>,
}

#[cfg(feature = "prod-backend")]
const DEFAULT_BACKEND_URL: &str = "https://weso.forgeros.fr/";

#[cfg(not(feature = "prod-backend"))]
const DEFAULT_BACKEND_URL: &str = "http://127.0.0.1:8080";

fn default_backend_url() -> Url {
    if let Ok(v) = std::env::var("BBR_BACKEND_URL") {
        if let Ok(url) = Url::parse(v.trim()) {
            return url;
        }
    }
    Url::parse(DEFAULT_BACKEND_URL).expect("DEFAULT_BACKEND_URL must be a valid URL")
}

const GUI_PROGRESS_STEPS: u64 = 200;
const GUI_PROGRESS_TICK: Duration = Duration::from_millis(100);

#[tauri::command]
async fn get_submitter_config() -> Result<Option<SubmitterConfig>, String> {
    load_submitter_config().map_err(|e| format!("{e:#}"))
}

#[tauri::command]
async fn set_submitter_config(cfg: SubmitterConfig) -> Result<(), String> {
    save_submitter_config(&cfg).map_err(|e| format!("{e:#}"))
}

#[tauri::command]
async fn start_client(
    app: AppHandle,
    state: State<'_, Arc<GuiState>>,
    opts: StartOptions,
) -> Result<(), String> {
    let mut guard = state.engine.lock().await;
    if guard.is_some() {
        return Err("already running".to_string());
    }

    let state_for_task = state.inner().clone();
    let submitter = match load_submitter_config() {
        Ok(Some(cfg)) => cfg,
        Ok(None) => SubmitterConfig::default(),
        Err(err) => return Err(format!("{err:#}")),
    };

    let parallel = opts
        .parallel
        .filter(|v| *v > 0)
        .map(|v| v as usize)
        .unwrap_or(4);

    let engine = start_engine(EngineConfig {
        backend_url: default_backend_url(),
        parallel,
        mem_budget_bytes: 128 * 1024 * 1024,
        submitter,
        idle_sleep: Duration::ZERO,
        progress_steps: GUI_PROGRESS_STEPS,
        progress_tick: GUI_PROGRESS_TICK,
        recent_jobs_max: EngineConfig::DEFAULT_RECENT_JOBS_MAX,
    });

    let mut events = engine.subscribe();
    let app = app.clone();
    tokio::spawn(async move {
        let mut pending_progress: HashMap<usize, WorkerProgressUpdate> = HashMap::new();
        let mut flush_tick = tokio::time::interval(GUI_PROGRESS_TICK);
        flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                ev = events.recv() => {
                    match ev {
                        Ok(ev) => {
                            if let EngineEvent::WorkerProgress { worker_idx, iters_done, iters_total, iters_per_sec } = ev {
                                pending_progress.insert(worker_idx, WorkerProgressUpdate {
                                    worker_idx,
                                    iters_done,
                                    iters_total,
                                    iters_per_sec,
                                });
                            } else {
                                let is_stopped = matches!(ev, EngineEvent::Stopped);
                                let _ = app.emit("engine-event", ev);
                                if is_stopped {
                                    break;
                                }
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
                _ = flush_tick.tick() => {
                    if !pending_progress.is_empty() {
                        let mut updates: Vec<WorkerProgressUpdate> = pending_progress.drain().map(|(_, v)| v).collect();
                        updates.sort_by_key(|u| u.worker_idx);
                        let _ = app.emit("engine-event", GuiEngineEvent::ProgressBatch { updates });
                    }
                }
            }
        }

        let mut guard = state_for_task.engine.lock().await;
        *guard = None;
    });

    *guard = Some(engine);
    Ok(())
}

#[tauri::command]
async fn stop_client(state: State<'_, Arc<GuiState>>) -> Result<(), String> {
    let guard = state.engine.lock().await;
    let Some(engine) = guard.as_ref() else {
        return Ok(());
    };
    engine.request_stop();
    Ok(())
}

#[tauri::command]
async fn client_running(state: State<'_, Arc<GuiState>>) -> Result<bool, String> {
    let guard = state.engine.lock().await;
    Ok(guard.is_some())
}

#[tauri::command]
async fn engine_snapshot(state: State<'_, Arc<GuiState>>) -> Result<Option<StatusSnapshot>, String> {
    let guard = state.engine.lock().await;
    Ok(guard.as_ref().map(|engine| engine.snapshot()))
}

fn main() {
    #[cfg(target_os = "linux")]
    {
        // On some Linux/Wayland setups WebKitGTK's DMABUF renderer can fail with errors like:
        // "Failed to create GBM buffer ... Invalid argument" and render a blank window.
        // Default to disabling it unless the user explicitly opted in.
        if std::env::var_os("WEBKIT_DISABLE_DMABUF_RENDERER").is_none() {
            // SAFETY: this is executed at process startup before spawning any threads.
            unsafe {
                std::env::set_var("WEBKIT_DISABLE_DMABUF_RENDERER", "1");
            }
        }
    }

    let state = Arc::new(GuiState::default());
    tauri::Builder::default()
        .manage(state)
        .invoke_handler(tauri::generate_handler![
            get_submitter_config,
            set_submitter_config,
            start_client,
            stop_client,
            client_running,
            engine_snapshot
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
