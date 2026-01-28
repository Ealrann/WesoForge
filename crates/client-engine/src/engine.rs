use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use chrono::Utc;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinSet;

use crate::api::{
    EngineConfig, EngineEvent, EngineHandle, JobOutcome, JobSummary, StatusSnapshot,
    WorkerSnapshot, WorkerStage,
};
use crate::backend::{BackendJobDto, BackendWorkBatch, fetch_work};
use crate::inflight::InflightStore;
use crate::worker::{WorkerCommand, WorkerInternalEvent};

pub(crate) struct EngineInner {
    pub(crate) event_tx: broadcast::Sender<EngineEvent>,
    pub(crate) snapshot_rx: watch::Receiver<StatusSnapshot>,
    stop_requested: AtomicBool,
    notify: tokio::sync::Notify,
}

impl EngineInner {
    pub(crate) fn request_stop(&self) {
        if !self.stop_requested.swap(true, Ordering::SeqCst) {
            let _ = self.event_tx.send(EngineEvent::StopRequested);
            self.notify.notify_waiters();
        }
    }

    fn should_stop(&self) -> bool {
        self.stop_requested.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
struct WorkJobItem {
    lease_id: String,
    lease_expires_at: i64,
    job: BackendJobDto,
}

#[derive(Debug)]
struct WorkerRuntime {
    stage: WorkerStage,
    job: Option<JobSummary>,
    iters_total: u64,
    last_speed_sample_at: Option<Instant>,
    prev_speed_interval: Option<(u64, Duration)>,
    speed_its_per_sec: u64,
    last_reported_iters_done: u64,
    last_emitted_iters_done: u64,
}

impl WorkerRuntime {
    fn new() -> Self {
        Self {
            stage: WorkerStage::Idle,
            job: None,
            iters_total: 0,
            last_speed_sample_at: None,
            prev_speed_interval: None,
            speed_its_per_sec: 0,
            last_reported_iters_done: 0,
            last_emitted_iters_done: 0,
        }
    }

    fn is_idle(&self) -> bool {
        self.stage == WorkerStage::Idle
    }

    fn is_busy(&self) -> bool {
        self.stage != WorkerStage::Idle
    }

    fn start_job(&mut self, job: JobSummary) {
        self.stage = WorkerStage::Computing;
        self.job = Some(job.clone());
        self.iters_total = job.number_of_iterations;
        self.last_speed_sample_at = Some(Instant::now());
        self.prev_speed_interval = None;
        self.speed_its_per_sec = 0;
        self.last_reported_iters_done = 0;
        self.last_emitted_iters_done = 0;
    }

    fn set_stage(&mut self, stage: WorkerStage) {
        self.stage = stage;
    }

    fn finish_job(&mut self) {
        self.stage = WorkerStage::Idle;
        self.job = None;
        self.iters_total = 0;
        self.last_speed_sample_at = None;
        self.prev_speed_interval = None;
        self.speed_its_per_sec = 0;
        self.last_reported_iters_done = 0;
        self.last_emitted_iters_done = 0;
    }

    fn apply_progress(&mut self, iters_done: u64) -> Option<u64> {
        let total_iters = self.iters_total;
        if total_iters == 0 {
            return None;
        }

        let now = Instant::now();
        let iters_done = iters_done.min(total_iters);
        let delta = iters_done.saturating_sub(self.last_reported_iters_done);
        if delta == 0 {
            return None;
        }

        if let Some(last_at) = self.last_speed_sample_at {
            let dt = now.duration_since(last_at);
            let (total_iters, total_dt) = if let Some((prev_iters, prev_dt)) = self.prev_speed_interval
            {
                (prev_iters.saturating_add(delta), prev_dt + dt)
            } else {
                (delta, dt)
            };

            if total_dt.as_secs_f64() > 0.0 {
                self.speed_its_per_sec = (total_iters as f64 / total_dt.as_secs_f64()).round() as u64;
            }
            self.prev_speed_interval = Some((delta, dt));
        }

        self.last_speed_sample_at = Some(now);
        self.last_reported_iters_done = iters_done;
        Some(iters_done)
    }
}

struct EngineRuntime {
    http: reqwest::Client,
    cfg: EngineConfig,

    workers: Vec<WorkerRuntime>,
    worker_cmds: Vec<mpsc::Sender<WorkerCommand>>,
    worker_progress: Vec<Arc<std::sync::atomic::AtomicU64>>,
    internal_rx: mpsc::UnboundedReceiver<WorkerInternalEvent>,
    worker_join: JoinSet<()>,

    pending: VecDeque<WorkJobItem>,
    fetch_task: Option<tokio::task::JoinHandle<anyhow::Result<Vec<WorkJobItem>>>>,
    fetch_backoff: Option<Pin<Box<tokio::time::Sleep>>>,
    inflight: Option<InflightStore>,

    recent_jobs: VecDeque<JobOutcome>,
    snapshot_tx: watch::Sender<StatusSnapshot>,
    inner: Arc<EngineInner>,
}

impl EngineRuntime {
    fn build_snapshot(&self) -> StatusSnapshot {
        let workers = self
            .workers
            .iter()
            .enumerate()
            .map(|(idx, w)| WorkerSnapshot {
                worker_idx: idx,
                stage: w.stage,
                job: w.job.clone(),
                iters_done: self
                    .worker_progress
                    .get(idx)
                    .map(|a| a.load(std::sync::atomic::Ordering::Relaxed))
                    .unwrap_or(0),
                iters_total: w.iters_total,
                iters_per_sec: w.speed_its_per_sec,
            })
            .collect();

        StatusSnapshot {
            stop_requested: self.inner.should_stop(),
            workers,
            recent_jobs: self.recent_jobs.iter().cloned().collect(),
        }
    }

    fn push_snapshot(&self) {
        let snap = self.build_snapshot();
        let _ = self.snapshot_tx.send(snap);
    }

    fn emit(&self, event: EngineEvent) {
        let _ = self.inner.event_tx.send(event);
    }

    fn idle_count(&self) -> usize {
        self.workers.iter().filter(|w| w.is_idle()).count()
    }

    fn all_idle(&self) -> bool {
        !self.workers.iter().any(|w| w.is_busy())
    }

    fn maybe_start_fetch(&mut self) {
        if self.inner.should_stop() {
            return;
        }
        let count = self.idle_count();
        if count == 0 {
            return;
        }
        if !self.pending.is_empty() || self.fetch_task.is_some() || self.fetch_backoff.is_some() {
            return;
        }

        let http = self.http.clone();
        let backend = self.cfg.backend_url.clone();
        let count = count;
        self.fetch_task = Some(tokio::spawn(async move {
            let count = count.min(u32::MAX as usize) as u32;
            let batch: BackendWorkBatch = fetch_work(&http, &backend, count).await?;
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

    async fn assign_jobs(&mut self) -> anyhow::Result<()> {
        if self.inner.should_stop() {
            self.pending.clear();
            return Ok(());
        }

        let mut snapshot_dirty = false;
        for idx in 0..self.workers.len() {
            if !self.workers[idx].is_idle() {
                continue;
            }
            let Some(item) = self.pending.pop_front() else {
                break;
            };

            let job_summary = JobSummary {
                job_id: item.job.job_id,
                height: item.job.height,
                field_vdf: item.job.field_vdf,
                number_of_iterations: item.job.number_of_iterations,
            };

            {
                let worker = &mut self.workers[idx];
                worker.start_job(job_summary.clone());
            }
            if let Some(a) = self.worker_progress.get(idx) {
                a.store(0, std::sync::atomic::Ordering::Relaxed);
            }
            self.emit(EngineEvent::WorkerJobStarted {
                worker_idx: idx,
                job: job_summary,
            });
            self.emit(EngineEvent::WorkerStage {
                worker_idx: idx,
                stage: WorkerStage::Computing,
            });
            snapshot_dirty = true;

            let cmd = WorkerCommand::Job {
                worker_idx: idx,
                backend_url: self.cfg.backend_url.clone(),
                lease_id: item.lease_id,
                lease_expires_at: item.lease_expires_at,
                job: item.job,
                progress_steps: self.cfg.progress_steps,
            };

            self.worker_cmds
                .get(idx)
                .ok_or_else(|| anyhow::anyhow!("worker cmd sender missing for worker {idx}"))?
                .send(cmd)
                .await
                .map_err(|_| anyhow::anyhow!("worker {idx} command channel closed"))?;
        }

        if snapshot_dirty {
            self.push_snapshot();
        }

        Ok(())
    }

    async fn handle_fetch_result(
        &mut self,
        res: Result<anyhow::Result<Vec<WorkJobItem>>, tokio::task::JoinError>,
    ) {
        self.fetch_task = None;

        match res {
            Ok(Ok(items)) => {
                if !self.inner.should_stop() {
                    if let Some(store) = &mut self.inflight {
                        let mut changed = false;
                        for item in &items {
                            changed |= store.insert_job(
                                item.lease_id.clone(),
                                item.lease_expires_at,
                                item.job.clone(),
                            );
                        }
                        if changed {
                            if let Err(err) = store.persist().await {
                                self.emit(EngineEvent::Warning {
                                    message: format!("warning: failed to persist inflight leases: {err:#}"),
                                });
                            }
                        }
                    }
                    self.pending.extend(items);
                }
                if self.pending.is_empty() {
                    self.fetch_backoff = Some(Box::pin(tokio::time::sleep(self.cfg.idle_sleep)));
                }
            }
            Ok(Err(err)) => {
                self.fetch_backoff = Some(Box::pin(tokio::time::sleep(self.cfg.idle_sleep)));
                self.emit(EngineEvent::Error {
                    message: format!("work fetch error: {err:#}"),
                });
            }
            Err(err) => {
                self.fetch_backoff = Some(Box::pin(tokio::time::sleep(self.cfg.idle_sleep)));
                self.emit(EngineEvent::Error {
                    message: format!("work fetch task join error: {err:#}"),
                });
            }
        }
    }

    async fn handle_internal_event(&mut self, ev: WorkerInternalEvent) {
        match ev {
            WorkerInternalEvent::StageChanged { worker_idx, stage } => {
                if let Some(worker) = self.workers.get_mut(worker_idx) {
                    worker.set_stage(stage);
                }
                self.emit(EngineEvent::WorkerStage { worker_idx, stage });
                self.push_snapshot();
            }
            WorkerInternalEvent::Finished { outcome } => {
                let worker_idx = outcome.worker_idx;
                if let Some(worker) = self.workers.get_mut(worker_idx) {
                    worker.finish_job();
                }
                if let Some(a) = self.worker_progress.get(worker_idx) {
                    a.store(0, Ordering::Relaxed);
                }

                self.recent_jobs.push_back(outcome.clone());
                while self.recent_jobs.len() > self.cfg.recent_jobs_max.max(1) {
                    self.recent_jobs.pop_front();
                }

                if outcome.drop_inflight || (outcome.error.is_none() && outcome.submit_reason.is_some()) {
                    if let Some(store) = &mut self.inflight {
                        if store.remove_job(outcome.job.job_id) {
                            if let Err(err) = store.persist().await {
                                self.emit(EngineEvent::Warning {
                                    message: format!("warning: failed to persist inflight leases: {err:#}"),
                                });
                            }
                        }
                    }
                }

                self.emit(EngineEvent::JobFinished { outcome });
                self.push_snapshot();
            }
            WorkerInternalEvent::Warning { message } => {
                self.emit(EngineEvent::Warning { message });
            }
            WorkerInternalEvent::Error { message } => {
                self.emit(EngineEvent::Error { message });
            }
        }
    }

    fn sample_progress(&mut self) {
        let mut snapshot_dirty = false;
        for idx in 0..self.workers.len() {
            if self.workers[idx].is_idle() {
                continue;
            }
            let Some(progress) = self.worker_progress.get(idx) else {
                continue;
            };
            let iters_done = progress.load(std::sync::atomic::Ordering::Relaxed);

            let (iters_done, iters_total, iters_per_sec) = {
                let worker = &mut self.workers[idx];
                let Some(iters_done) = worker.apply_progress(iters_done) else {
                    continue;
                };
                if iters_done == worker.last_emitted_iters_done {
                    continue;
                }
                worker.last_emitted_iters_done = iters_done;
                (iters_done, worker.iters_total, worker.speed_its_per_sec)
            };

            self.emit(EngineEvent::WorkerProgress {
                worker_idx: idx,
                iters_done,
                iters_total,
                iters_per_sec,
            });
            snapshot_dirty = true;
        }

        if snapshot_dirty {
            self.push_snapshot();
        }
    }

    async fn shutdown_workers(&mut self) {
        for tx in &self.worker_cmds {
            let _ = tx.send(WorkerCommand::Stop).await;
        }
        while let Some(res) = self.worker_join.join_next().await {
            if res.is_err() {
                // Ignore.
            }
        }
    }

    async fn run(mut self) -> anyhow::Result<()> {
        self.emit(EngineEvent::Started);
        self.push_snapshot();

        let mut progress_tick = tokio::time::interval(self.cfg.progress_tick);
        progress_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut result: anyhow::Result<()> = Ok(());

        loop {
            if self.inner.should_stop() && self.all_idle() {
                if let Some(task) = self.fetch_task.take() {
                    task.abort();
                }
                self.fetch_backoff = None;
                self.pending.clear();
                break;
            }

            if let Err(err) = self.assign_jobs().await {
                result = Err(err);
                break;
            }
            self.maybe_start_fetch();

            let loop_result: anyhow::Result<()> = tokio::select! {
                _ = progress_tick.tick() => {
                    self.sample_progress();
                    Ok(())
                }
                _ = self.inner.notify.notified() => Ok(()),
                ev_opt = self.internal_rx.recv() => {
                    if let Some(ev) = ev_opt {
                        self.handle_internal_event(ev).await;
                    }
                    Ok(())
                }
                res = async {
                    match self.fetch_task.as_mut() {
                        Some(task) => task.await,
                        None => std::future::pending::<Result<anyhow::Result<Vec<WorkJobItem>>, tokio::task::JoinError>>().await,
                    }
                } => {
                    self.handle_fetch_result(res).await;
                    Ok(())
                }
                _ = async {
                    match self.fetch_backoff.as_mut() {
                        Some(sleep) => sleep.as_mut().await,
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    self.fetch_backoff = None;
                    Ok(())
                }
                res = self.worker_join.join_next() => {
                    match res {
                        Some(Ok(())) => Err(anyhow::anyhow!("worker task exited unexpectedly")),
                        Some(Err(err)) => Err(anyhow::anyhow!("worker task join error: {err:#}")),
                        None => Err(anyhow::anyhow!("worker join set empty unexpectedly")),
                    }
                }
            };

            if let Err(err) = loop_result {
                result = Err(err);
                break;
            }
        }

        if let Err(err) = &result {
            self.emit(EngineEvent::Error {
                message: format!("engine error: {err:#}"),
            });
        }

        if let Some(task) = self.fetch_task.take() {
            task.abort();
        }
        self.fetch_backoff = None;
        self.pending.clear();

        self.shutdown_workers().await;
        self.emit(EngineEvent::Stopped);
        self.push_snapshot();
        result
    }
}

pub(crate) fn start_engine(cfg: EngineConfig) -> EngineHandle {
    let (event_tx, _) = broadcast::channel::<EngineEvent>(1024);
    let (snapshot_tx, snapshot_rx) = watch::channel(StatusSnapshot {
        stop_requested: false,
        workers: Vec::new(),
        recent_jobs: Vec::new(),
    });

    let inner = Arc::new(EngineInner {
        event_tx,
        snapshot_rx,
        stop_requested: AtomicBool::new(false),
        notify: tokio::sync::Notify::new(),
    });

    let join = tokio::spawn(run_engine(inner.clone(), snapshot_tx, cfg));
    EngineHandle { inner, join }
}

async fn run_engine(
    inner: Arc<EngineInner>,
    snapshot_tx: watch::Sender<StatusSnapshot>,
    mut cfg: EngineConfig,
) -> anyhow::Result<()> {
    if cfg.parallel == 0 {
        cfg.parallel = 1;
    }
    if cfg.idle_sleep == Duration::ZERO {
        cfg.idle_sleep = EngineConfig::DEFAULT_IDLE_SLEEP;
    }
    if cfg.progress_steps == 0 {
        cfg.progress_steps = EngineConfig::DEFAULT_PROGRESS_STEPS;
    }
    if cfg.progress_tick == Duration::ZERO {
        cfg.progress_tick = EngineConfig::DEFAULT_PROGRESS_TICK;
    }
    if cfg.recent_jobs_max == 0 {
        cfg.recent_jobs_max = EngineConfig::DEFAULT_RECENT_JOBS_MAX;
    }

    bbr_client_chiavdf_fast::set_bucket_memory_budget_bytes(cfg.mem_budget_bytes);

    let http = match reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
    {
        Ok(http) => http,
        Err(err) => {
            let message = format!("build http client: {err:#}");
            let _ = inner
                .event_tx
                .send(EngineEvent::Error { message: message.clone() });
            let _ = inner.event_tx.send(EngineEvent::Stopped);
            let _ = snapshot_tx.send(StatusSnapshot {
                stop_requested: inner.should_stop(),
                workers: Vec::new(),
                recent_jobs: Vec::new(),
            });
            return Err(anyhow::anyhow!("{message}"));
        }
    };

    let submitter = Arc::new(tokio::sync::RwLock::new(cfg.submitter.clone()));
    let warned_invalid_reward_address = Arc::new(AtomicBool::new(false));

    let (internal_tx, internal_rx) = mpsc::unbounded_channel::<WorkerInternalEvent>();

    let mut worker_cmds = Vec::with_capacity(cfg.parallel);
    let mut worker_progress = Vec::with_capacity(cfg.parallel);
    let mut worker_join = JoinSet::new();

    for worker_idx in 0..cfg.parallel {
        let (tx, rx) = mpsc::channel::<WorkerCommand>(1);
        worker_cmds.push(tx);

        let progress = Arc::new(std::sync::atomic::AtomicU64::new(0));
        worker_progress.push(progress.clone());

        let http = http.clone();
        let submitter = submitter.clone();
        let warned = warned_invalid_reward_address.clone();
        let internal_tx = internal_tx.clone();
        let progress = progress.clone();

        worker_join.spawn(async move {
            crate::worker::run_worker_task(
                worker_idx,
                rx,
                internal_tx,
                progress,
                http,
                submitter,
                warned,
            )
            .await;
        });
    }

    let workers = (0..cfg.parallel).map(|_| WorkerRuntime::new()).collect();

    let mut inflight = match InflightStore::load() {
        Ok(Some(store)) => Some(store),
        Ok(None) => None,
        Err(err) => {
            let message = format!("warning: failed to load inflight leases (resume disabled): {err:#}");
            let _ = inner.event_tx.send(EngineEvent::Warning { message });
            None
        }
    };

    if let Some(store) = inflight.as_mut() {
        let now = Utc::now().timestamp();
        let expired_job_ids: Vec<u64> = store
            .entries()
            .filter(|entry| entry.lease_expires_at <= now)
            .map(|entry| entry.job.job_id)
            .collect();

        if !expired_job_ids.is_empty() {
            let expired_count = expired_job_ids.len();
            for job_id in expired_job_ids {
                store.remove_job(job_id);
            }

            if let Err(err) = store.persist().await {
                let _ = inner.event_tx.send(EngineEvent::Warning {
                    message: format!("warning: failed to persist expired inflight lease cleanup: {err:#}"),
                });
            } else {
                let _ = inner.event_tx.send(EngineEvent::Warning {
                    message: format!(
                        "Discarded {expired_count} expired inflight lease(s) from previous run."
                    ),
                });
            }
        }
    }

    let mut pending = VecDeque::new();
    if let Some(store) = inflight.as_ref() {
        for entry in store.entries() {
            pending.push_back(WorkJobItem {
                lease_id: entry.lease_id.clone(),
                lease_expires_at: entry.lease_expires_at,
                job: entry.job.clone(),
            });
        }
        if !pending.is_empty() {
            let message = format!(
                "Loaded {} inflight lease(s) from previous run; processing them before leasing new work.",
                pending.len()
            );
            let _ = inner.event_tx.send(EngineEvent::Warning { message });
        }
    }

    let runtime = EngineRuntime {
        http,
        cfg,
        workers,
        worker_cmds,
        worker_progress,
        internal_rx,
        worker_join,
        pending,
        fetch_task: None,
        fetch_backoff: None,
        inflight: inflight.take(),
        recent_jobs: VecDeque::new(),
        snapshot_tx,
        inner,
    };

    runtime.push_snapshot();
    runtime.run().await
}
