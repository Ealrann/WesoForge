use std::collections::VecDeque;
use std::io::IsTerminal;
use std::io::Write;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc;

use crate::backend::{JobDto, WorkBatch, fetch_work};
use crate::cli::{Cli, default_worker_id};
use crate::constants::{IDLE_SLEEP, PROGRESS_BAR_STEPS};
use crate::format::{field_vdf_label, format_number};
use crate::protocol::{WorkerCommand, WorkerJobSpec, WorkerUpdate};
use crate::shutdown::{ShutdownController, ShutdownEvent, spawn_ctrl_c_handler};
use crate::terminal::TuiTerminal;

#[derive(Debug)]
enum WorkerEvent {
    Progress { worker_idx: usize, iters_done: u64 },
    Done { worker_idx: usize, line: String },
    Log { worker_idx: usize, line: String },
    Eof { worker_idx: usize },
}

#[derive(Debug)]
struct WorkerProcess {
    child: Child,
    stdin: ChildStdin,

    total_iters: Option<u64>,

    last_speed_sample_at: Option<Instant>,
    prev_speed_interval: Option<(u64, Duration)>,
    speed_its_per_sec: u64,

    last_reported_iters_done: u64,
}

impl WorkerProcess {
    async fn spawn(
        worker_idx: usize,
        events_tx: mpsc::UnboundedSender<WorkerEvent>,
        mem_budget_bytes: u64,
    ) -> anyhow::Result<Self> {
        let exe = std::env::current_exe().context("current_exe")?;
        let mut child = Command::new(exe)
            .arg("--worker")
            .arg("--mem")
            .arg(format!("{mem_budget_bytes}B"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .context("spawn worker process")?;

        let stdin = child.stdin.take().context("worker stdin is not piped")?;
        let stdout = child.stdout.take().context("worker stdout is not piped")?;

        tokio::spawn(async move {
            let mut lines = tokio::io::BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                match WorkerUpdate::parse_line(&line) {
                    Ok(WorkerUpdate::Progress(iters_done)) => {
                        let _ = events_tx.send(WorkerEvent::Progress {
                            worker_idx,
                            iters_done,
                        });
                    }
                    Ok(WorkerUpdate::Done(line)) => {
                        let _ = events_tx.send(WorkerEvent::Done { worker_idx, line });
                    }
                    Err(_) => {
                        let _ = events_tx.send(WorkerEvent::Log { worker_idx, line });
                    }
                };
            }

            let _ = events_tx.send(WorkerEvent::Eof { worker_idx });
        });

        Ok(Self {
            child,
            stdin,
            total_iters: None,
            last_speed_sample_at: None,
            prev_speed_interval: None,
            speed_its_per_sec: 0,
            last_reported_iters_done: 0,
        })
    }

    fn is_idle(&self) -> bool {
        self.total_iters.is_none()
    }

    fn is_busy(&self) -> bool {
        self.total_iters.is_some()
    }

    async fn send(&mut self, cmd: WorkerCommand) -> anyhow::Result<()> {
        let line = cmd.to_line();
        self.stdin
            .write_all(line.as_bytes())
            .await
            .context("write worker command")?;
        self.stdin.flush().await.ok();
        Ok(())
    }

    fn assign_job(&mut self, total_iters: u64) {
        self.total_iters = Some(total_iters);
        self.last_speed_sample_at = Some(Instant::now());
        self.prev_speed_interval = None;
        self.speed_its_per_sec = 0;
        self.last_reported_iters_done = 0;
    }

    fn apply_progress(&mut self, iters_done: u64) -> Option<u64> {
        let Some(total_iters) = self.total_iters else {
            return None;
        };

        let now = Instant::now();
        let iters_done = iters_done.min(total_iters);
        let delta = iters_done.saturating_sub(self.last_reported_iters_done);
        if delta > 0 {
            if let Some(last_at) = self.last_speed_sample_at {
                let dt = now.duration_since(last_at);
                let (total_iters, total_dt) =
                    if let Some((prev_iters, prev_dt)) = self.prev_speed_interval {
                        (prev_iters.saturating_add(delta), prev_dt + dt)
                    } else {
                        (delta, dt)
                    };

                if total_dt.as_secs_f64() > 0.0 {
                    self.speed_its_per_sec =
                        (total_iters as f64 / total_dt.as_secs_f64()).round() as u64;
                }
                self.prev_speed_interval = Some((delta, dt));
            }
            self.last_speed_sample_at = Some(now);
            self.last_reported_iters_done = iters_done;
            return Some(iters_done);
        }
        None
    }

    fn finish_work(&mut self) {
        self.total_iters = None;
        self.last_speed_sample_at = None;
        self.prev_speed_interval = None;
        self.speed_its_per_sec = 0;
        self.last_reported_iters_done = 0;
    }
}

struct WorkerUiState {
    bar: ProgressBar,
    total_iters: u64,
    last_step: u64,
}

struct Ui {
    mp: MultiProgress,
    global_pb: ProgressBar,
    stop_pb: ProgressBar,
    worker_states: Vec<WorkerUiState>,
}

impl Ui {
    fn new(worker_count: usize) -> Self {
        let mp = MultiProgress::new();
        mp.set_draw_target(ProgressDrawTarget::stdout());
        mp.set_move_cursor(true);

        let global_pb = mp.add(ProgressBar::new(0));
        let style = ProgressStyle::with_template("{msg}").unwrap();
        global_pb.set_style(style);
        global_pb.set_message("Global: 0 it/s");

        let worker_style = ProgressStyle::with_template("{prefix} {bar:20.cyan/blue} {msg}")
            .unwrap()
            .progress_chars("#--");

        let mut worker_states = Vec::with_capacity(worker_count);
        for idx in 0..worker_count {
            let pb = mp.add(ProgressBar::new(PROGRESS_BAR_STEPS));
            pb.set_style(worker_style.clone());
            pb.set_prefix(format!("W{}", idx + 1));
            pb.set_message("Idle");
            worker_states.push(WorkerUiState {
                bar: pb,
                total_iters: 0,
                last_step: 0,
            });
        }

        let stop_pb = mp.add(ProgressBar::new(0));
        let style = ProgressStyle::with_template("{msg}").unwrap();
        stop_pb.set_style(style);
        stop_pb.set_message(" ");

        Self {
            mp,
            global_pb,
            stop_pb,
            worker_states,
        }
    }

    fn println(&self, msg: &str) {
        let _ = self.mp.println(msg);
    }

    fn set_worker_work(&mut self, worker_idx: usize, msg: String, total_iters: u64) {
        let Some(state) = self.worker_states.get_mut(worker_idx) else {
            return;
        };
        state.bar.set_length(PROGRESS_BAR_STEPS);
        state.bar.set_position(0);
        state.bar.set_message(msg);
        state.total_iters = total_iters;
        state.last_step = 0;
    }

    fn set_worker_job(&mut self, worker_idx: usize, job: &JobDto) {
        let field = field_vdf_label(job.field_vdf);
        self.set_worker_work(
            worker_idx,
            format!("Block {} ({field})", job.height),
            job.number_of_iterations,
        );
    }

    fn set_worker_progress(&mut self, worker_idx: usize, iters_done: u64) {
        let Some(state) = self.worker_states.get_mut(worker_idx) else {
            return;
        };
        let step = calc_progress_step(state.total_iters, iters_done);
        if step != state.last_step {
            state.last_step = step;
            state.bar.set_position(step);
        }
    }

    fn set_worker_idle(&mut self, worker_idx: usize) {
        let Some(state) = self.worker_states.get_mut(worker_idx) else {
            return;
        };
        state.bar.set_position(0);
        state.bar.set_message("Idle");
        state.total_iters = 0;
        state.last_step = 0;
    }

    fn set_stop_message(&mut self, msg: &str) {
        self.stop_pb.set_message(msg.to_string());
    }

    fn tick_global(&self, speed: u64, busy: usize, total: usize) {
        self.global_pb.set_message(format!(
            "Global: {} it/s (running {busy}/{total})",
            format_number(speed)
        ));
    }

    fn freeze(&self) {
        self.mp.set_move_cursor(false);
        for worker in &self.worker_states {
            worker.bar.abandon();
        }
        self.global_pb.abandon();
        self.stop_pb.abandon();
        let _ = std::io::stdout().write_all(b"\n");
    }
}

#[derive(Debug)]
struct WorkJobItem {
    lease_id: String,
    lease_expires_at: i64,
    job: JobDto,
}

struct WorkQueue {
    pending: VecDeque<WorkJobItem>,
    fetch_task: Option<tokio::task::JoinHandle<anyhow::Result<Vec<WorkJobItem>>>>,
    fetch_backoff: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl WorkQueue {
    fn new() -> Self {
        Self {
            pending: VecDeque::new(),
            fetch_task: None,
            fetch_backoff: None,
        }
    }

    fn clear_pending(&mut self) {
        self.pending.clear();
    }

    fn pop_next(&mut self) -> Option<WorkJobItem> {
        self.pending.pop_front()
    }

    fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    fn maybe_start_fetch(
        &mut self,
        http: reqwest::Client,
        backend: reqwest::Url,
        worker_id: String,
        count: usize,
        shutdown_graceful: bool,
    ) {
        if shutdown_graceful || count == 0 {
            return;
        }
        if self.has_pending() || self.fetch_task.is_some() || self.fetch_backoff.is_some() {
            return;
        }

        self.fetch_task = Some(tokio::spawn(async move {
            let count = count.min(u32::MAX as usize) as u32;
            let batch: WorkBatch = fetch_work(&http, &backend, &worker_id, count).await?;
            let items = batch
                .jobs
                .into_iter()
                .map(|job| WorkJobItem {
                    lease_id: batch.lease_id.clone(),
                    lease_expires_at: batch.lease_expires_at,
                    job,
                })
                .collect();
            Ok(items)
        }));
    }

    fn clear_fetch_backoff(&mut self) {
        self.fetch_backoff = None;
    }

    fn handle_fetch_result(
        &mut self,
        res: Result<anyhow::Result<Vec<WorkJobItem>>, tokio::task::JoinError>,
        shutdown_graceful: bool,
    ) -> Option<String> {
        self.fetch_task = None;
        match res {
            Ok(Ok(items)) => {
                if !shutdown_graceful {
                    self.pending.extend(items);
                }
                if !self.has_pending() {
                    self.fetch_backoff = Some(Box::pin(tokio::time::sleep(IDLE_SLEEP)));
                }
                None
            }
            Ok(Err(err)) => {
                self.fetch_backoff = Some(Box::pin(tokio::time::sleep(IDLE_SLEEP)));
                Some(format!("work fetch error: {err:#}"))
            }
            Err(err) => {
                self.fetch_backoff = Some(Box::pin(tokio::time::sleep(IDLE_SLEEP)));
                Some(format!("work fetch task join error: {err:#}"))
            }
        }
    }
}

#[derive(Debug)]
enum ControllerAction {
    Shutdown(ShutdownEvent),
    ShutdownClosed,
    Worker(WorkerEvent),
    WorkerClosed,
    WorkFetchResult(Result<anyhow::Result<Vec<WorkJobItem>>, tokio::task::JoinError>),
    WorkFetchBackoffDone,
    Tick,
}

struct Controller {
    cli: Cli,
    worker_id: String,
    tui_enabled: bool,
    _tui_terminal: Option<TuiTerminal>,
    shutdown: Arc<ShutdownController>,
    shutdown_rx: mpsc::UnboundedReceiver<ShutdownEvent>,
    http: reqwest::Client,
    events_rx: mpsc::UnboundedReceiver<WorkerEvent>,
    workers: Vec<WorkerProcess>,
    work: WorkQueue,
    ui: Option<Ui>,
    ticker: tokio::time::Interval,
    immediate_exit: bool,
    stop_requested_printed: bool,
}

impl Controller {
    async fn new(cli: Cli) -> anyhow::Result<Self> {
        if cli.parallel == 0 {
            anyhow::bail!("--parallel must be >= 1");
        }

        let worker_id = cli.worker_id.clone().unwrap_or_else(default_worker_id);
        let tui_enabled = !cli.no_tui && std::io::stdout().is_terminal();

        let shutdown = Arc::new(ShutdownController::new());
        let (shutdown_tx, shutdown_rx) = mpsc::unbounded_channel::<ShutdownEvent>();
        let tui_terminal = if tui_enabled && std::io::stdin().is_terminal() {
            Some(TuiTerminal::enter(shutdown.clone(), shutdown_tx.clone())?)
        } else {
            None
        };
        if tui_terminal.is_none() {
            spawn_ctrl_c_handler(shutdown.clone(), shutdown_tx);
        }

        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .context("build http client")?;

        let (events_tx, events_rx) = mpsc::unbounded_channel::<WorkerEvent>();
        let ui = if tui_enabled {
            Some(Ui::new(cli.parallel))
        } else {
            None
        };

        let mut workers: Vec<WorkerProcess> = Vec::with_capacity(cli.parallel);
        for idx in 0..cli.parallel {
            workers.push(WorkerProcess::spawn(idx, events_tx.clone(), cli.mem_budget_bytes).await?);
        }

        let startup = format!(
            "bbr-client {} backend={} worker_id={} parallel={}",
            env!("CARGO_PKG_VERSION"),
            cli.backend_url,
            worker_id,
            cli.parallel
        );
        let controller = Self {
            cli,
            worker_id,
            tui_enabled,
            _tui_terminal: tui_terminal,
            shutdown,
            shutdown_rx,
            http,
            events_rx,
            workers,
            work: WorkQueue::new(),
            ui,
            ticker: tokio::time::interval(Duration::from_secs(1)),
            immediate_exit: false,
            stop_requested_printed: false,
        };
        controller.print_info(&startup);
        Ok(controller)
    }

    fn print_info(&self, msg: &str) {
        if let Some(ui) = &self.ui {
            ui.println(msg);
        } else {
            println!("{msg}");
        }
    }

    fn print_err(&self, msg: &str) {
        if let Some(ui) = &self.ui {
            ui.println(msg);
        } else {
            eprintln!("{msg}");
        }
    }

    fn print_worker_log(&self, worker_idx: usize, line: &str) {
        let msg = format!("W{}: {line}", worker_idx + 1);
        self.print_err(&msg);
    }

    fn apply_graceful_shutdown(&mut self) {
        if self.shutdown.should_exit_graceful() {
            self.work.clear_pending();
        }
    }

    fn request_graceful_shutdown(&mut self) {
        self.apply_graceful_shutdown();
        if !self.stop_requested_printed {
            self.stop_requested_printed = true;
            let msg = "Stop requested — finishing current work before exiting (press CTRL+C again to exit immediately).";
            self.set_stop_message(msg);
        }
    }

    fn request_immediate_shutdown(&mut self) {
        self.immediate_exit = true;
        let msg = "Stop requested again — exiting immediately.";
        self.set_stop_message(msg);
    }

    async fn run(mut self) -> anyhow::Result<i32> {
        loop {
            self.dispatch().await?;
            if self.shutdown.should_exit_graceful() && self.workers_idle() {
                break;
            }

            self.maybe_fetch_work();

            match self.next_action().await {
                ControllerAction::Shutdown(ev) => {
                    self.handle_shutdown_event(ev);
                    if self.immediate_exit {
                        break;
                    }
                }
                ControllerAction::ShutdownClosed => {}
                ControllerAction::Worker(ev) => {
                    self.handle_update(ev)?;
                }
                ControllerAction::WorkerClosed => {
                    anyhow::bail!("worker events channel closed unexpectedly");
                }
                ControllerAction::WorkFetchResult(res) => {
                    self.handle_work_fetch_result(res);
                }
                ControllerAction::WorkFetchBackoffDone => {
                    self.work.clear_fetch_backoff();
                }
                ControllerAction::Tick => {
                    self.tick_ui();
                }
            }
        }

        self.shutdown_workers().await;

        if let Some(ui) = &self.ui {
            ui.freeze();
        }
        Ok(if self.immediate_exit { 130 } else { 0 })
    }

    async fn dispatch(&mut self) -> anyhow::Result<()> {
        self.apply_graceful_shutdown();
        if self.shutdown.should_exit_graceful() {
            return Ok(());
        }

        for (worker_idx, worker) in self.workers.iter_mut().enumerate() {
            if !worker.is_idle() {
                continue;
            }
            let Some(work_item) = self.work.pop_next() else {
                break;
            };

            let total_iters = work_item.job.number_of_iterations;
            let progress_interval = if self.tui_enabled {
                default_job_progress_interval(total_iters)
            } else {
                0
            };

            if let Some(ui) = &mut self.ui {
                ui.set_worker_job(worker_idx, &work_item.job);
            }

            let spec = WorkerJobSpec {
                backend_url: self.cli.backend_url.clone(),
                lease_id: work_item.lease_id,
                lease_expires_at: work_item.lease_expires_at,
                progress_interval,
                job: work_item.job,
            };
            worker.send(WorkerCommand::Job(spec)).await?;
            worker.assign_job(total_iters);
        }

        Ok(())
    }

    fn maybe_fetch_work(&mut self) {
        let idle_count = self.workers.iter().filter(|w| w.is_idle()).count();
        if idle_count == 0 {
            return;
        }
        let count = idle_count;
        self.work.maybe_start_fetch(
            self.http.clone(),
            self.cli.backend_url.clone(),
            self.worker_id.clone(),
            count,
            self.shutdown.should_exit_graceful(),
        );
    }

    fn handle_update(&mut self, ev: WorkerEvent) -> anyhow::Result<()> {
        match ev {
            WorkerEvent::Progress {
                worker_idx,
                iters_done,
            } => {
                if let Some(worker) = self.workers.get_mut(worker_idx) {
                    if let Some(iters_done) = worker.apply_progress(iters_done) {
                        if let Some(ui) = &mut self.ui {
                            ui.set_worker_progress(worker_idx, iters_done);
                        }
                    }
                }
            }
            WorkerEvent::Done { worker_idx, line } => {
                if let Some(worker) = self.workers.get_mut(worker_idx) {
                    worker.finish_work();
                }
                if let Some(ui) = &mut self.ui {
                    ui.set_worker_idle(worker_idx);
                }
                if !line.is_empty() {
                    self.print_info(&line);
                }
            }
            WorkerEvent::Log { worker_idx, line } => {
                self.print_worker_log(worker_idx, &line);
            }
            WorkerEvent::Eof { worker_idx } => {
                anyhow::bail!("worker process {worker_idx} exited unexpectedly");
            }
        }
        Ok(())
    }

    fn handle_work_fetch_result(
        &mut self,
        res: Result<anyhow::Result<Vec<WorkJobItem>>, tokio::task::JoinError>,
    ) {
        if let Some(msg) = self
            .work
            .handle_fetch_result(res, self.shutdown.should_exit_graceful())
        {
            self.print_err(&msg);
        }
    }

    fn tick_ui(&mut self) {
        if let Some(ui) = &self.ui {
            let busy = self.workers.iter().filter(|w| w.is_busy()).count();
            let speed: u64 = self
                .workers
                .iter()
                .filter(|w| w.is_busy())
                .map(|w| w.speed_its_per_sec)
                .sum();
            ui.tick_global(speed, busy, self.cli.parallel);
        }
    }

    fn handle_shutdown_event(&mut self, ev: ShutdownEvent) {
        match ev {
            ShutdownEvent::Graceful => self.request_graceful_shutdown(),
            ShutdownEvent::Immediate => self.request_immediate_shutdown(),
        }
    }

    fn set_stop_message(&mut self, msg: &str) {
        if let Some(ui) = &mut self.ui {
            ui.set_stop_message(msg);
        } else {
            self.print_err(msg);
        }
    }

    fn workers_idle(&self) -> bool {
        !self.workers.iter().any(|w| w.is_busy())
    }

    async fn next_action(&mut self) -> ControllerAction {
        tokio::select! {
            ev_opt = self.shutdown_rx.recv() => match ev_opt {
                Some(ev) => ControllerAction::Shutdown(ev),
                None => ControllerAction::ShutdownClosed,
            },
            ev_opt = self.events_rx.recv() => match ev_opt {
                Some(ev) => ControllerAction::Worker(ev),
                None => ControllerAction::WorkerClosed,
            },
            res = async {
                match self.work.fetch_task.as_mut() {
                    Some(task) => task.await,
                    None => {
                        std::future::pending::<
                            Result<anyhow::Result<Vec<WorkJobItem>>, tokio::task::JoinError>,
                        >()
                        .await
                    }
                }
            } => ControllerAction::WorkFetchResult(res),
            _ = async {
                match self.work.fetch_backoff.as_mut() {
                    Some(backoff) => backoff.as_mut().await,
                    None => std::future::pending::<()>().await,
                }
            } => ControllerAction::WorkFetchBackoffDone,
            _ = self.ticker.tick(), if self.tui_enabled => ControllerAction::Tick,
        }
    }

    async fn shutdown_workers(&mut self) {
        for worker in self.workers.iter_mut() {
            let _ = worker.send(WorkerCommand::Stop).await;
            let _ = worker.child.kill().await;
        }
    }
}

fn default_job_progress_interval(total_iters: u64) -> u64 {
    if total_iters == 0 {
        return 1;
    }
    (total_iters.saturating_add(PROGRESS_BAR_STEPS - 1) / PROGRESS_BAR_STEPS).max(1)
}

fn calc_progress_step(total_iters: u64, iters_done: u64) -> u64 {
    let total = total_iters.max(1);
    let iters_done = iters_done.min(total_iters);
    ((iters_done.saturating_mul(PROGRESS_BAR_STEPS)) / total).min(PROGRESS_BAR_STEPS)
}

pub async fn run_controller(cli: Cli) -> anyhow::Result<i32> {
    Controller::new(cli).await?.run().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_job_progress_interval_rounds_up() {
        let steps = PROGRESS_BAR_STEPS;
        assert_eq!(default_job_progress_interval(0), 1);
        assert_eq!(default_job_progress_interval(steps), 1);
        assert_eq!(default_job_progress_interval(steps + 1), 2);
        assert_eq!(default_job_progress_interval(steps * 2), 2);
    }

    #[test]
    fn calc_progress_step_clamps_and_scales() {
        let steps = PROGRESS_BAR_STEPS;
        assert_eq!(calc_progress_step(100, 0), 0);
        assert_eq!(calc_progress_step(100, 50), steps / 2);
        assert_eq!(calc_progress_step(100, 100), steps);
        assert_eq!(calc_progress_step(100, 1000), steps);
    }
}
