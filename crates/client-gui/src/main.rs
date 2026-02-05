#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::sync::Arc;
use std::time::Duration;

use reqwest::Url;
use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Emitter, State};
use tokio::sync::Mutex;

#[cfg(feature = "support-devtools")]
use tauri::Manager;

use bbr_client_core::submitter::{SubmitterConfig, load_submitter_config, save_submitter_config};
use bbr_client_engine::{
    EngineConfig, EngineEvent, EngineHandle, PinMode, StatusSnapshot, start_engine,
};

struct GuiState {
    engine: Mutex<Option<EngineHandle>>,
    progress: Mutex<Vec<WorkerProgressUpdate>>,
}

#[derive(Debug, Clone, Serialize)]
struct WorkerProgressUpdate {
    worker_idx: usize,
    iters_done: u64,
    iters_total: u64,
    iters_per_sec: u64,
}

impl Default for GuiState {
    fn default() -> Self {
        Self {
            engine: Mutex::new(None),
            progress: Mutex::new(Vec::new()),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct StartOptions {
    parallel: Option<u32>,
    mode: Option<WorkMode>,
    max_proofs_per_group: Option<u32>,
    mem_budget_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum WorkMode {
    Proof,
    Group,
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

fn default_use_groups() -> bool {
    std::env::var("BBR_MODE")
        .ok()
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("group"))
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
async fn engine_progress(
    state: State<'_, Arc<GuiState>>,
) -> Result<Vec<WorkerProgressUpdate>, String> {
    let progress = state.progress.lock().await;
    Ok(progress.clone())
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

    let parallel = opts.parallel.unwrap_or(4);
    if !(1..=512).contains(&parallel) {
        return Err("Parallel workers must be between 1 and 512.".to_string());
    }
    let parallel = parallel as usize;

    let mode = opts.mode.unwrap_or_else(|| {
        if default_use_groups() {
            WorkMode::Group
        } else {
            WorkMode::Proof
        }
    });
    let use_groups = matches!(mode, WorkMode::Group);
    let group_max_proofs_per_group = opts
        .max_proofs_per_group
        .unwrap_or(EngineConfig::DEFAULT_GROUP_MAX_PROOFS_PER_GROUP)
        .clamp(1, 200);

    let mem_budget_bytes = opts
        .mem_budget_bytes
        .filter(|v| *v > 0)
        .unwrap_or(128 * 1024 * 1024);

    let engine = start_engine(EngineConfig {
        backend_url: default_backend_url(),
        parallel,
        use_groups,
        group_max_proofs_per_group,
        mem_budget_bytes,
        submitter,
        idle_sleep: Duration::ZERO,
        progress_steps: GUI_PROGRESS_STEPS,
        progress_tick: GUI_PROGRESS_TICK,
        recent_jobs_max: EngineConfig::DEFAULT_RECENT_JOBS_MAX,
        pin_mode: PinMode::Off,
    });

    let mut events = engine.subscribe();
    let app = app.clone();

    {
        let mut progress = state.progress.lock().await;
        progress.clear();
        progress.reserve(parallel);
        for worker_idx in 0..parallel {
            progress.push(WorkerProgressUpdate {
                worker_idx,
                iters_done: 0,
                iters_total: 0,
                iters_per_sec: 0,
            });
        }
    }
    tokio::spawn(async move {
        loop {
            let ev = match events.recv().await {
                Ok(ev) => ev,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            };

            match &ev {
                EngineEvent::WorkerProgress {
                    worker_idx,
                    iters_done,
                    iters_total,
                    iters_per_sec,
                } => {
                    let mut progress = state_for_task.progress.lock().await;
                    while progress.len() <= *worker_idx {
                        let idx = progress.len();
                        progress.push(WorkerProgressUpdate {
                            worker_idx: idx,
                            iters_done: 0,
                            iters_total: 0,
                            iters_per_sec: 0,
                        });
                    }
                    progress[*worker_idx] = WorkerProgressUpdate {
                        worker_idx: *worker_idx,
                        iters_done: *iters_done,
                        iters_total: *iters_total,
                        iters_per_sec: *iters_per_sec,
                    };
                }
                EngineEvent::WorkerJobStarted { worker_idx, job } => {
                    {
                        let mut progress = state_for_task.progress.lock().await;
                        while progress.len() <= *worker_idx {
                            let idx = progress.len();
                            progress.push(WorkerProgressUpdate {
                                worker_idx: idx,
                                iters_done: 0,
                                iters_total: 0,
                                iters_per_sec: 0,
                            });
                        }
                        progress[*worker_idx] = WorkerProgressUpdate {
                            worker_idx: *worker_idx,
                            iters_done: 0,
                            iters_total: job.number_of_iterations,
                            iters_per_sec: 0,
                        };
                    }
                    let _ = app.emit("engine-event", ev);
                }
                EngineEvent::JobFinished { outcome } => {
                    let worker_idx = outcome.worker_idx;
                    {
                        let mut progress = state_for_task.progress.lock().await;
                        while progress.len() <= worker_idx {
                            let idx = progress.len();
                            progress.push(WorkerProgressUpdate {
                                worker_idx: idx,
                                iters_done: 0,
                                iters_total: 0,
                                iters_per_sec: 0,
                            });
                        }
                        progress[worker_idx] = WorkerProgressUpdate {
                            worker_idx,
                            iters_done: 0,
                            iters_total: 0,
                            iters_per_sec: 0,
                        };
                    }
                    let _ = app.emit("engine-event", ev);
                }
                EngineEvent::Error { message } => {
                    eprintln!("{message}");
                    let _ = app.emit("engine-event", ev);
                }
                _ => {
                    let is_stopped = matches!(ev, EngineEvent::Stopped);
                    let _ = app.emit("engine-event", ev);
                    if is_stopped {
                        break;
                    }
                }
            }
        }

        let mut guard = state_for_task.engine.lock().await;
        *guard = None;

        let mut progress = state_for_task.progress.lock().await;
        progress.clear();
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
async fn engine_snapshot(
    state: State<'_, Arc<GuiState>>,
) -> Result<Option<StatusSnapshot>, String> {
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
        .setup(|app| {
            let _ = app;
            #[cfg(feature = "support-devtools")]
            {
                if let Some(win) = app.get_webview_window("main") {
                    win.open_devtools();
                }
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            get_submitter_config,
            set_submitter_config,
            engine_progress,
            start_client,
            stop_client,
            client_running,
            engine_snapshot
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
