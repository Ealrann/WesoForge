<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import { invoke } from '@tauri-apps/api/core';
  import { listen } from '@tauri-apps/api/event';
  import PopupFrame from './components/PopupFrame.svelte';
  import pkg from '../package.json';

  type SubmitterConfig = { reward_address?: string | null; name?: string | null };
  type StartOptions = { parallel?: number | null };

  const appVersion = pkg.version;

  type WorkerStage = 'Idle' | 'Computing' | 'Submitting';

  type JobSummary = { job_id: number; height: number; field_vdf: number; number_of_iterations: number };

  type WorkerSnapshot = {
    worker_idx: number;
    stage: WorkerStage;
    job: JobSummary | null;
    iters_done: number;
    iters_total: number;
    iters_per_sec: number;
  };

  type WorkerProgressUpdate = {
    worker_idx: number;
    iters_done: number;
    iters_total: number;
    iters_per_sec: number;
  };

  type JobOutcome = {
    worker_idx: number;
    job: JobSummary;
    output_mismatch: boolean;
    submit_reason?: string | null;
    submit_detail?: string | null;
    error?: string | null;
    compute_ms: number;
    submit_ms: number;
    total_ms: number;
  };

  type StatusSnapshot = {
    stop_requested: boolean;
    workers: WorkerSnapshot[];
    recent_jobs: JobOutcome[];
  };

  type EngineEvent =
    | { type: 'Started' }
    | { type: 'StopRequested' }
    | { type: 'WorkerJobStarted'; worker_idx: number; job: JobSummary }
    | { type: 'WorkerStage'; worker_idx: number; stage: WorkerStage }
    | { type: 'JobFinished'; outcome: JobOutcome }
    | { type: 'Warning'; message: string }
    | { type: 'Error'; message: string }
    | { type: 'Stopped' };

  type LogEntry = { level: 'info' | 'warning' | 'error'; message: string; ts: number };

  let theme = $state<'dark' | 'light'>('dark');

  let cfg = $state<SubmitterConfig>({});
  let cfgLoaded = $state(false);
  let savingCfg = $state(false);
  let cfgError = $state<string | null>(null);
  let submitterOpen = $state(true);
  let draftCfg = $state<SubmitterConfig>({});
  let logsOpen = $state(false);

  let parallel = $state<number>(4);
  let running = $state(false);
  let stopRequested = $state(false);
  let runError = $state<string | null>(null);

  let workers = $state<WorkerSnapshot[]>([]);
  let recentJobs = $state<JobOutcome[]>([]);
  let logs = $state<LogEntry[]>([]);

  let globalItersPerSec = $state<number>(0);
  let busyWorkers = $state<number>(0);

  const fmtInt = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });

  function applyTheme(next: 'dark' | 'light') {
    theme = next;
    if (typeof document !== 'undefined') {
      document.documentElement.dataset.theme = next;
    }
    try {
      localStorage.setItem('bbr_theme', next);
    } catch {
      // ignore
    }
  }

  function toggleTheme() {
    applyTheme(theme === 'dark' ? 'light' : 'dark');
  }

  function openSubmitter() {
    cfgError = null;
    draftCfg = { ...cfg };
    submitterOpen = true;
  }

  function closeSubmitter() {
    submitterOpen = false;
  }

  function openLogs() {
    logsOpen = true;
  }

  function closeLogs() {
    logsOpen = false;
  }

  function pushLog(level: LogEntry['level'], message: string) {
    logs = [...logs, { level, message, ts: Date.now() }].slice(-200);
  }

  function formatCount(value: number) {
    if (!Number.isFinite(value)) return '—';
    return fmtInt.format(value);
  }

  function formatDuration(ms: number) {
    if (!Number.isFinite(ms)) return '—';
    if (ms < 1000) return `${ms}ms`;
    if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
    return `${(ms / 60_000).toFixed(1)}m`;
  }

  function workerBase(idx: number): WorkerSnapshot {
    return {
      worker_idx: idx,
      stage: 'Idle',
      job: null,
      iters_done: 0,
      iters_total: 0,
      iters_per_sec: 0
    };
  }

  function ensureWorker(idx: number) {
    while (workers.length <= idx) {
      workers.push(workerBase(workers.length));
    }
    return workers[idx];
  }

  function recomputeWorkerStats() {
    let busy = 0;
    let speed = 0;
    for (const w of workers) {
      if (w.stage === 'Idle') continue;
      busy += 1;
      speed += w.iters_per_sec;
    }
    busyWorkers = busy;
    globalItersPerSec = speed;
  }

  function patchWorker(idx: number, patch: Partial<WorkerSnapshot>) {
    ensureWorker(idx);
    const current = workers[idx];

    const prevStage = current.stage;
    const prevBusy = prevStage === 'Idle' ? 0 : 1;
    const prevSpeed = prevStage === 'Idle' ? 0 : current.iters_per_sec;

    const nextStage = patch.stage ?? current.stage;
    const nextBusy = nextStage === 'Idle' ? 0 : 1;
    const nextSpeedRaw = patch.iters_per_sec ?? current.iters_per_sec;
    const nextSpeed = nextStage === 'Idle' ? 0 : nextSpeedRaw;

    busyWorkers += nextBusy - prevBusy;
    globalItersPerSec += nextSpeed - prevSpeed;

    workers[idx] = { ...current, ...patch, worker_idx: idx };
  }

  function applySnapshot(snap: StatusSnapshot) {
    running = true;
    stopRequested = stopRequested || snap.stop_requested;
    workers = snap.workers;
    recomputeWorkerStats();
    recentJobs = snap.recent_jobs;
  }

  function clearSnapshot() {
    running = false;
    stopRequested = false;
    workers = [];
    recentJobs = [];
    busyWorkers = 0;
    globalItersPerSec = 0;
  }

  async function refreshSnapshot() {
    try {
      const snap = await invoke<StatusSnapshot | null>('engine_snapshot');
      if (snap) {
        applySnapshot(snap);
      } else {
        clearSnapshot();
      }
    } catch (e) {
      clearSnapshot();
      runError = String(e);
    }
  }

  function invokeWithTimeout<T>(cmd: string, timeoutMs: number): Promise<T> {
    let timeoutId: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<T>((_, reject) => {
      timeoutId = setTimeout(() => reject(new Error(`invoke(${cmd}) timed out after ${timeoutMs}ms`)), timeoutMs);
    });
    return Promise.race([
      invoke<T>(cmd).finally(() => {
        if (timeoutId) clearTimeout(timeoutId);
      }),
      timeout
    ]);
  }

  async function refreshSnapshotSoon(maxAttempts: number) {
    const attempts = Math.max(1, Math.floor(maxAttempts));
    for (let i = 0; i < attempts; i++) {
      await new Promise((resolve) => setTimeout(resolve, i === 0 ? 50 : 150));
      await refreshSnapshot();
      if (workers.length > 0) return;
      if (!running) return;
    }
  }

  function progressPollDelayMs() {
    if (typeof document !== 'undefined' && document.visibilityState === 'hidden') {
      return 5000;
    }
    if (typeof document !== 'undefined' && typeof document.hasFocus === 'function' && !document.hasFocus()) {
      return 1000;
    }
    return 500;
  }

  async function pollProgress(signal: AbortSignal) {
    while (!signal.aborted) {
      if (!running) {
        await new Promise((resolve) => setTimeout(resolve, 500));
        continue;
      }

      try {
        const updates = await invokeWithTimeout<WorkerProgressUpdate[]>('engine_progress', 2000);
        for (const u of updates) {
          patchWorker(u.worker_idx, {
            iters_done: u.iters_done,
            iters_total: u.iters_total,
            iters_per_sec: u.iters_per_sec
          });
        }
      } catch {
        // ignore
      }

      await new Promise((resolve) => setTimeout(resolve, progressPollDelayMs()));
    }
  }

  function handleEngineEvent(ev: EngineEvent) {
    switch (ev.type) {
      case 'Started':
        running = true;
        stopRequested = false;
        void refreshSnapshot();
        pushLog('info', 'Engine started');
        break;
      case 'StopRequested':
        stopRequested = true;
        pushLog('info', 'Stop requested');
        break;
      case 'WorkerJobStarted':
        patchWorker(ev.worker_idx, {
          stage: 'Computing',
          job: ev.job,
          iters_done: 0,
          iters_total: ev.job.number_of_iterations,
          iters_per_sec: 0
        });
        break;
      case 'WorkerStage':
        patchWorker(ev.worker_idx, { stage: ev.stage });
        break;
      case 'JobFinished': {
        const { outcome } = ev;
        patchWorker(outcome.worker_idx, {
          stage: 'Idle',
          job: null,
          iters_done: 0,
          iters_total: 0,
          iters_per_sec: 0
        });
        recentJobs = [...recentJobs, outcome].slice(-100);
        break;
      }
      case 'Warning':
        pushLog('warning', ev.message);
        break;
      case 'Error':
        pushLog('error', ev.message);
        break;
      case 'Stopped':
        running = false;
        stopRequested = false;
        workers = [];
        busyWorkers = 0;
        globalItersPerSec = 0;
        pushLog('info', 'Engine stopped');
        break;
    }
  }

  function stageBadgeClass(stage: WorkerStage) {
    switch (stage) {
      case 'Idle':
        return 'border-border bg-bg text-muted';
      case 'Computing':
        return 'border-info/50 bg-info/10 text-info';
      case 'Submitting':
        return 'border-warning/50 bg-warning/10 text-warning';
    }
  }

  function workerProgressPct(w: WorkerSnapshot) {
    const total = w.iters_total;
    if (!total) return 0;
    return Math.min(100, Math.max(0, (w.iters_done / total) * 100));
  }

  function outcomeBadge(outcome: JobOutcome) {
    if (outcome.error) {
      return { label: 'Error', cls: 'border-danger/50 bg-danger/10 text-danger' };
    }
    const reason = (outcome.submit_reason ?? '').trim().toLowerCase();
    if (reason === 'accepted') {
      return { label: 'Accepted', cls: 'border-success/50 bg-success/10 text-success' };
    }
    if (reason === 'queued') {
      return { label: 'Queued', cls: 'border-info/50 bg-info/10 text-info' };
    }
    if (outcome.output_mismatch) {
      return { label: 'Rejected', cls: 'border-danger/50 bg-danger/10 text-danger' };
    }
    return { label: 'Rejected', cls: 'border-muted/40 bg-bg text-muted' };
  }

  async function loadCfg() {
    cfgError = null;
    try {
      const res = await invoke<SubmitterConfig | null>('get_submitter_config');
      cfg = res ?? {};
      draftCfg = { ...cfg };
    } catch (e) {
      cfgError = String(e);
    } finally {
      cfgLoaded = true;
    }
  }

  async function saveSubmitter() {
    savingCfg = true;
    cfgError = null;
    try {
      await invoke<void>('set_submitter_config', { cfg: draftCfg });
      cfg = { ...draftCfg };
      submitterOpen = false;
    } catch (e) {
      cfgError = String(e);
    } finally {
      savingCfg = false;
    }
  }

  async function start() {
    runError = null;
    try {
      const opts: StartOptions = {
        parallel: parallel > 0 ? parallel : null
      };
      await invoke<void>('start_client', { opts });
      running = true;
      stopRequested = false;
      void refreshSnapshotSoon(10);
    } catch (e) {
      runError = String(e);
    }
  }

  async function stop() {
    runError = null;
    stopRequested = true;
    try {
      await invoke<void>('stop_client');
    } catch (e) {
      runError = String(e);
    }
  }

  onMount(async () => {
    let unlisten: (() => void) | null = null;
    onDestroy(() => {
      try {
        unlisten?.();
      } catch {
        // ignore
      }
    });
    void listen<EngineEvent>('engine-event', (event) => handleEngineEvent(event.payload))
      .then((fn) => {
        unlisten = fn;
      })
      .catch((err) => {
        pushLog('error', `Failed to subscribe to engine events: ${String(err)}`);
      });

    const progressPoll = new AbortController();
    onDestroy(() => progressPoll.abort());
    void pollProgress(progressPoll.signal);

    let saved: string | null = null;
    try {
      saved = localStorage.getItem('bbr_theme');
    } catch {
      // ignore
    }
    if (saved === 'light' || saved === 'dark') {
      applyTheme(saved);
    } else {
      applyTheme('dark');
    }

    await loadCfg();
    await refreshSnapshot();
  });
</script>

<div class="h-screen bg-bg text-fg flex flex-col overflow-hidden">
  <header class="border-b border-border bg-header text-on-header">
	    <div class="flex w-full items-center justify-between gap-4 px-6 py-4">
      <div class="flex items-center gap-2">
        <img
          src="/logo-64.avif"
          alt="WesoForge logo"
	          class="h-9 w-9"
	          width="36"
	          height="36"
	          decoding="async"
	        />
	        <h1 class="font-itc text-xl font-semibold relative top-0.5">WesoForge</h1>
      </div>

      <div class="flex items-center gap-3">
        <span class="text-xs font-mono text-on-header/70">v{appVersion}</span>
        <button
          type="button"
          class="rounded border border-border/60 px-2 py-2 text-on-header hover:text-accent hover:border-accent/60 transition-colors"
          aria-label={theme === 'dark' ? 'Switch to light theme' : 'Switch to dark theme'}
          title={theme === 'dark' ? 'Switch to light theme' : 'Switch to dark theme'}
	          onclick={toggleTheme}
	        >
	          {#if theme === 'dark'}
	            <svg class="h-5 w-5" viewBox="0 0 24 24" fill="none" aria-hidden="true">
	              <path
	                d="M12 3V4M12 20V21M4 12H3M6.31412 6.31412L5.5 5.5M17.6859 6.31412L18.5 5.5M6.31412 17.69L5.5 18.5001M17.6859 17.69L18.5 18.5001M21 12H20M16 12C16 14.2091 14.2091 16 12 16C9.79086 16 8 14.2091 8 12C8 9.79086 9.79086 8 12 8C14.2091 8 16 9.79086 16 12Z"
	                stroke="currentColor"
	                stroke-width="2"
	                stroke-linecap="round"
	                stroke-linejoin="round"
	              />
	            </svg>
	          {:else}
	            <svg class="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">
	              <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79Z" />
	            </svg>
          {/if}
	        </button>

        <button
          type="button"
          class="rounded border border-border/60 px-2 py-2 text-on-header hover:text-accent hover:border-accent/60 transition-colors"
          aria-label="View logs"
          title="View logs"
          onclick={openLogs}
        >
          <svg class="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">
            <path d="m4 17 6-6-6-6" />
            <path d="M12 19h8" />
          </svg>
        </button>
	
	        <button
	          type="button"
	          class="rounded border border-border/60 px-2 py-2 text-on-header hover:text-accent hover:border-accent/60 transition-colors"
	          aria-label="Submitter settings"
	          title="Submitter settings"
	          onclick={openSubmitter}
	        >
	          <svg
              class="h-5 w-5"
              viewBox="0 0 340.274 340.274"
              fill="currentColor"
              aria-hidden="true"
            >
              <path
                d="M293.629,127.806l-5.795-13.739c19.846-44.856,18.53-46.189,14.676-50.08l-25.353-24.77l-2.516-2.12h-2.937 c-1.549,0-6.173,0-44.712,17.48l-14.184-5.719c-18.332-45.444-20.212-45.444-25.58-45.444h-35.765 c-5.362,0-7.446-0.006-24.448,45.606l-14.123,5.734C86.848,43.757,71.574,38.19,67.452,38.19l-3.381,0.105L36.801,65.032 c-4.138,3.891-5.582,5.263,15.402,49.425l-5.774,13.691C0,146.097,0,147.838,0,153.33v35.068c0,5.501,0,7.44,46.585,24.127 l5.773,13.667c-19.843,44.832-18.51,46.178-14.655,50.032l25.353,24.8l2.522,2.168h2.951c1.525,0,6.092,0,44.685-17.516 l14.159,5.758c18.335,45.438,20.218,45.427,25.598,45.427h35.771c5.47,0,7.41,0,24.463-45.589l14.195-5.74 c26.014,11,41.253,16.585,45.349,16.585l3.404-0.096l27.479-26.901c3.909-3.945,5.278-5.309-15.589-49.288l5.734-13.702 c46.496-17.967,46.496-19.853,46.496-25.221v-35.029C340.268,146.361,340.268,144.434,293.629,127.806z M170.128,228.474 c-32.798,0-59.504-26.187-59.504-58.364c0-32.153,26.707-58.315,59.504-58.315c32.78,0,59.43,26.168,59.43,58.315 C229.552,202.287,202.902,228.474,170.128,228.474z"
              />
            </svg>
	        </button>
      </div>
    </div>
  </header>

	  <main class="flex w-full flex-1 flex-col gap-6 px-6 py-6 min-h-0 overflow-auto">
	    {#if runError}
	      <div class="rounded border border-danger bg-danger/10 px-4 py-3 text-sm text-danger">{runError}</div>
	    {/if}

		    <div class="grid flex-1 min-h-0 grid-cols-1 gap-6 lg:grid-cols-[minmax(0,1fr)_420px] lg:grid-rows-[auto_1fr]">
	    <section class="rounded border border-border bg-surface p-4 lg:col-start-1 lg:col-span-1 lg:row-start-1 flex flex-col">
		      <h2 class="text-sm font-semibold">Configuration</h2>

		      <div class="mt-4 grid grid-cols-1 gap-3">
		        <label class="flex items-center justify-between gap-4 text-sm">
		          <span class="text-muted">Parallel workers</span>
		          <input
		            class="w-28 rounded border border-border bg-bg px-3 py-2 text-sm text-fg focus:border-accent focus:outline-none"
		            type="number"
		            min="1"
		            step="1"
		            bind:value={parallel}
		          />
		        </label>
		      </div>

          <div class="mt-4 flex flex-wrap items-center justify-between gap-3 border-t border-border pt-4">
            <div class="grid gap-1">
              <div class="text-sm">
                <span class="text-muted">Status:</span>
                {#if running}
                  {#if stopRequested}
                    <span class="ml-2 font-medium text-warning">Stopping</span>
                  {:else}
                    <span class="ml-2 font-medium text-success">Running</span>
                  {/if}
                {:else}
                  <span class="ml-2 font-medium text-muted">Stopped</span>
                {/if}
              </div>
	              {#if running}
	                <div class="text-xs text-muted">
	                  Speed:{' '}
	                  {#if workers.length === 0}
	                    —
	                  {:else}
	                    <span class="font-semibold text-fg">{formatCount(globalItersPerSec)} it/s (running {busyWorkers}/{workers.length})</span>
	                  {/if}
	                </div>
	              {/if}
            </div>

            {#if running}
              <button
                class="rounded bg-danger px-3 py-2 text-sm font-medium text-white hover:bg-danger/90 disabled:opacity-60"
                onclick={stop}
                disabled={stopRequested}
              >
                {#if stopRequested}Stopping…{:else}Stop{/if}
              </button>
            {:else}
              <button class="rounded bg-accent px-3 py-2 text-sm font-medium text-on-accent hover:bg-accent/90" onclick={start}>
                Start
              </button>
            {/if}
          </div>

			    </section>

	    <section class="rounded border border-border bg-surface p-4 lg:col-start-1 lg:col-span-1 lg:row-start-2 flex min-h-0 flex-col">
      <div class="flex items-center justify-between gap-4">
        <h2 class="text-sm font-semibold">Workers</h2>
        <div class="text-xs text-muted">
          {#if workers.length === 0}
            —
          {:else}
            {Math.max(0, workers.length - busyWorkers)} idle / {workers.length} total
          {/if}
        </div>
      </div>

      <div class="mt-4 lg:min-h-0 lg:flex-1 lg:overflow-auto">
        {#if !running}
          <p class="text-sm text-muted">Start the engine to fetch work and compute proofs.</p>
        {:else if workers.length === 0}
          <p class="text-sm text-muted">Waiting for workers…</p>
        {:else}
          <div class="flex flex-wrap gap-4">
            {#each workers as w (w.worker_idx)}
              <div class="w-full rounded border border-border bg-bg p-4 sm:w-[275px]">
                <div class="flex items-center justify-between">
                  <div class="text-sm font-semibold">Worker {w.worker_idx + 1}</div>
                  <span class={`rounded border px-2 py-1 text-xs ${stageBadgeClass(w.stage)}`}>{w.stage}</span>
                </div>

                {#if w.job}
                  <div class="mt-2 text-xs text-muted">
                    Job #{w.job.job_id} • height {w.job.height} • field {w.job.field_vdf}
                  </div>

                  <div class="mt-3">
                    <div class="h-2 w-full rounded bg-border/40">
                      <div
                        class="h-2 rounded bg-accent transition-[width] duration-150"
                        style={`width: ${workerProgressPct(w).toFixed(2)}%`}
                      ></div>
                    </div>
                    <div class="mt-2 flex items-center justify-between text-xs text-muted">
                      <span>
                        {formatCount(w.iters_done)} / {formatCount(w.iters_total)} ({workerProgressPct(w).toFixed(1)}%)
                      </span>
                    </div>
                  </div>
                {:else}
                  <div class="mt-3 text-sm text-muted">Idle</div>
                {/if}
              </div>
            {/each}
          </div>
        {/if}
      </div>
    </section>

		    <section class="rounded border border-border bg-surface hidden min-h-0 flex-col lg:flex lg:col-span-1 lg:col-start-2 lg:row-start-1 lg:row-span-2">
	      <div class="flex items-center justify-between border-b border-border px-4 py-3">
	        <h2 class="text-sm font-semibold">Recent submissions</h2>
	      </div>
	      <div class="min-h-0 flex-1 overflow-auto">
	        {#if recentJobs.length === 0}
          <div class="p-4 text-sm text-muted">No submissions yet.</div>
        {:else}
          <table class="w-full border-collapse text-sm">
            <thead class="text-xs text-muted">
              <tr class="border-b border-border">
                <th class="px-4 py-2 text-left font-medium">Result</th>
                <th class="px-4 py-2 text-left font-medium">Job</th>
                <th class="px-4 py-2 text-right font-medium">Time</th>
              </tr>
            </thead>
            <tbody>
              {#each recentJobs.slice().reverse() as outcome (outcome.job.job_id)}
                {@const badge = outcomeBadge(outcome)}
                <tr class="border-b border-border/50">
                  <td class="px-4 py-2">
                    <span class={`inline-flex rounded border px-2 py-1 text-xs ${badge.cls}`}>{badge.label}</span>
                  </td>
                  <td class="px-4 py-2">
                    <div class="text-sm">
                      #{outcome.job.job_id} • h{outcome.job.height} • f{outcome.job.field_vdf}
                    </div>
                    {#if outcome.submit_reason || outcome.error || outcome.output_mismatch}
                      <div class="mt-1 text-xs text-muted">
                        {#if outcome.error}
                          {outcome.error}
                        {:else if outcome.output_mismatch}
                          Output mismatch
                        {:else}
                          {outcome.submit_reason}{#if outcome.submit_detail}: {outcome.submit_detail}{/if}
                        {/if}
                      </div>
                    {/if}
                  </td>
                  <td class="px-4 py-2 text-right font-mono text-xs text-muted">{formatDuration(outcome.total_ms)}</td>
                </tr>
              {/each}
            </tbody>
          </table>
        {/if}
      </div>
    </section>

		    </div>
		  </main>

		  {#if submitterOpen}
		    <PopupFrame title="Submitter" ariaCloseLabel="Close submitter settings" onClose={closeSubmitter}>
          {#if !cfgLoaded}
            <p class="text-sm text-muted">Loading…</p>
          {:else}
            <div class="grid grid-cols-1 gap-3">
              <label class="grid gap-1 text-sm">
                <span class="text-muted">Reward address (xch…)</span>
                <input
                  class="w-full rounded border border-border bg-bg px-3 py-2 text-sm text-fg placeholder:text-muted/70 focus:border-accent focus:outline-none"
                  placeholder="xch…"
                  bind:value={draftCfg.reward_address}
                />
              </label>
              <label class="grid gap-1 text-sm">
                <span class="text-muted">Name</span>
                <input
                  class="w-full rounded border border-border bg-bg px-3 py-2 text-sm text-fg placeholder:text-muted/70 focus:border-accent focus:outline-none"
                  placeholder="Optional"
                  bind:value={draftCfg.name}
                />
              </label>
            </div>
            <div class="mt-3 flex flex-wrap items-center justify-between gap-3">
              <p class="text-xs text-muted/80">
                Saved under <code class="rounded border border-border bg-bg px-1">~/.config/bbr-client/config.json</code> (or XDG config).
              </p>
              <button
                class="rounded bg-accent px-3 py-2 text-sm font-medium text-on-accent hover:bg-accent/90 disabled:opacity-60"
                onclick={saveSubmitter}
                disabled={savingCfg}
              >
                Save
              </button>
            </div>
            {#if cfgError}
              <p class="mt-2 text-sm text-danger">{cfgError}</p>
            {/if}
          {/if}
        </PopupFrame>
		  {/if}

      {#if logsOpen}
        <PopupFrame
          title="Logs"
          ariaCloseLabel="Close logs"
          maxWidthClass="max-w-3xl"
          bodyClass="overflow-auto"
          onClose={closeLogs}
        >
          {#snippet headerActions()}
            <button class="rounded bg-accent px-2 py-1 text-xs text-on-accent hover:bg-accent/90" onclick={() => (logs = [])}>
              Clear
            </button>
          {/snippet}

          <div class="font-mono text-xs leading-5 text-muted">
            {#if logs.length === 0}
              <div class="text-muted/60">No output yet.</div>
            {:else}
              {#each logs as entry}
                <div
                  class={entry.level === 'error' ? 'text-danger' : entry.level === 'warning' ? 'text-warning' : 'text-muted'}
                >
                  [{new Date(entry.ts).toLocaleTimeString()}] {entry.message}
                </div>
              {/each}
            {/if}
          </div>
        </PopupFrame>
      {/if}
	</div>
