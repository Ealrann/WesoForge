//! Public API types for the in-process `bbr-client` engine.

use std::time::Duration;

use bbr_client_core::submitter::SubmitterConfig;
use reqwest::Url;
use serde::{Deserialize, Serialize};

/// CPU pinning strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinMode {
    /// Do not pin worker compute threads.
    Off,
    /// Pin worker compute threads to a shared-L3 (CCD/CCX) CPU set (Linux best-effort).
    L3,
}

/// Configuration for the in-process engine.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Backend base URL (e.g. `http://127.0.0.1:8080`).
    pub backend_url: Url,

    /// Number of proof workers to run concurrently.
    pub parallel: usize,

    /// Whether to fetch grouped work and compute batch proofs (Trick 2).
    ///
    /// When enabled, the engine leases work via `api/jobs/lease_groups` and uses
    /// the chiavdf batch API to reuse squaring across multiple targets sharing
    /// the same discriminant.
    pub use_groups: bool,

    /// Max number of proofs to request per leased group (Trick 2).
    ///
    /// When `use_groups` is enabled, the engine leases groups via `api/jobs/lease_groups`.
    /// The backend clamps this value to a small upper bound.
    pub group_max_proofs_per_group: u32,

    /// Memory budget (bytes) for the native streaming prover parameter tuner.
    ///
    /// Note: the chiavdf fast wrapper currently treats this as a *process-wide*
    /// setting, so all workers share the same configured budget.
    pub mem_budget_bytes: u64,

    /// Submitter metadata attached to job submissions.
    pub submitter: SubmitterConfig,

    /// How long to sleep after an empty work fetch / error.
    pub idle_sleep: Duration,

    /// Target number of progress updates per job.
    ///
    /// This is used to derive the chiavdf progress callback cadence
    /// (`progress_interval`).
    pub progress_steps: u64,

    /// How often the engine samples worker progress to emit progress events.
    pub progress_tick: Duration,

    /// Maximum number of completed jobs retained in the snapshot.
    pub recent_jobs_max: usize,

    /// CPU pinning strategy.
    pub pin_mode: PinMode,
}

impl EngineConfig {
    /// Default idle backoff used by the CLI worker.
    pub const DEFAULT_IDLE_SLEEP: Duration = Duration::from_secs(10);

    /// Default number of progress steps (matches the current CLI progress bars).
    pub const DEFAULT_PROGRESS_STEPS: u64 = 20;

    /// Default progress sampling tick.
    pub const DEFAULT_PROGRESS_TICK: Duration = Duration::from_millis(200);

    /// Default size of the recent-jobs ring buffer.
    pub const DEFAULT_RECENT_JOBS_MAX: usize = 100;

    /// Default max proofs per group when `use_groups` is enabled.
    pub const DEFAULT_GROUP_MAX_PROOFS_PER_GROUP: u32 = 100;
}

/// A lightweight summary of a leased proof job.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobSummary {
    /// Backend job identifier.
    pub job_id: u64,
    /// Number of proofs in the group, when this summary represents a grouped job (Trick 2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_proofs: Option<u32>,
    /// Block height.
    pub height: u32,
    /// Compressible VDF field identifier (1..=4).
    pub field_vdf: i32,
    /// VDF iteration count.
    pub number_of_iterations: u64,
}

/// Stage of a worker in the job lifecycle.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum WorkerStage {
    /// No job assigned (idle).
    Idle,
    /// Computing the proof witness.
    Computing,
    /// Submitting the witness to the backend.
    Submitting,
}

/// Snapshot of a single worker’s current state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerSnapshot {
    /// Worker index (0-based).
    pub worker_idx: usize,
    /// Current stage.
    pub stage: WorkerStage,
    /// Current job, if any.
    pub job: Option<JobSummary>,
    /// Iterations completed for the current job.
    pub iters_done: u64,
    /// Total iterations for the current job.
    pub iters_total: u64,
    /// Estimated speed in iterations/second.
    pub iters_per_sec: u64,
}

/// Result of a completed job (submitted or failed).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobOutcome {
    /// Worker index (0-based).
    pub worker_idx: usize,
    /// Job metadata.
    pub job: JobSummary,
    /// Whether the computed output mismatched the expected `y_ref`.
    pub output_mismatch: bool,
    /// Backend submission reason (e.g. `accepted`, `already_compact`), if submission happened.
    pub submit_reason: Option<String>,
    /// Backend submission detail string, if submission happened.
    pub submit_detail: Option<String>,
    /// Remove this job from the local in-flight store (resume file) even on failure.
    ///
    /// This is used for terminal submission rejections where retrying would be useless
    /// (e.g. `job_not_found`, lease conflicts).
    #[serde(default)]
    pub drop_inflight: bool,
    /// Human-readable failure message, for compute/submit errors.
    pub error: Option<String>,
    /// Total compute time (milliseconds).
    pub compute_ms: u64,
    /// Total submission time (milliseconds).
    pub submit_ms: u64,
    /// Total job time (milliseconds).
    pub total_ms: u64,
}

/// Engine event stream payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type")]
pub enum EngineEvent {
    /// Engine started.
    Started,
    /// Engine is stopping (graceful shutdown requested).
    StopRequested,
    /// Worker has been assigned a new job.
    WorkerJobStarted {
        /// Worker index (0-based).
        worker_idx: usize,
        /// Job summary.
        job: JobSummary,
    },
    /// Worker progress update.
    WorkerProgress {
        /// Worker index (0-based).
        worker_idx: usize,
        /// Iterations completed.
        iters_done: u64,
        /// Iterations total.
        iters_total: u64,
        /// Speed estimate in iterations/second.
        iters_per_sec: u64,
    },
    /// Worker stage transition.
    WorkerStage {
        /// Worker index (0-based).
        worker_idx: usize,
        /// New stage.
        stage: WorkerStage,
    },
    /// Worker completed a job (success or failure).
    JobFinished {
        /// Job outcome.
        outcome: JobOutcome,
    },
    /// A warning from the engine.
    Warning {
        /// Warning message.
        message: String,
    },
    /// A non-fatal error from the engine.
    Error {
        /// Error message.
        message: String,
    },
    /// Engine stopped (no more workers running).
    Stopped,
}

/// Current engine state snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StatusSnapshot {
    /// Whether the engine has been asked to stop.
    pub stop_requested: bool,
    /// Per-worker snapshots.
    pub workers: Vec<WorkerSnapshot>,
    /// Recently completed jobs (newest last).
    pub recent_jobs: Vec<JobOutcome>,
}

/// Handle to a running in-process engine instance.
pub struct EngineHandle {
    pub(crate) inner: std::sync::Arc<crate::engine::EngineInner>,
    pub(crate) join: tokio::task::JoinHandle<anyhow::Result<()>>,
}

/// Start a new in-process engine instance.
pub fn start_engine(config: EngineConfig) -> EngineHandle {
    crate::engine::start_engine(config)
}

impl EngineHandle {
    /// Subscribe to the engine event stream.
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<EngineEvent> {
        self.inner.event_tx.subscribe()
    }

    /// Get the latest engine snapshot.
    pub fn snapshot(&self) -> StatusSnapshot {
        self.inner.snapshot_rx.borrow().clone()
    }

    /// Request a graceful shutdown (finish in-flight work, stop leasing new jobs).
    pub fn request_stop(&self) {
        self.inner.request_stop();
    }

    /// Wait for the engine to stop, returning the engine task result.
    pub async fn wait(self) -> anyhow::Result<()> {
        match self.join.await {
            Ok(res) => res,
            Err(err) => Err(anyhow::anyhow!("engine task join error: {err}")),
        }
    }
}
