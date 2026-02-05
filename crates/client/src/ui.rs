use std::io::Write;

use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};

use bbr_client_engine::JobSummary;

use crate::constants::PROGRESS_BAR_STEPS;
use crate::format::{field_vdf_label, format_number};

struct WorkerUiState {
    bar: ProgressBar,
    total_iters: u64,
    last_step: u64,
}

pub(crate) struct Ui {
    mp: MultiProgress,
    global_pb: ProgressBar,
    stop_pb: ProgressBar,
    worker_states: Vec<WorkerUiState>,
}

impl Ui {
    pub(crate) fn new(worker_count: usize) -> Self {
        let mp = MultiProgress::new();
        mp.set_draw_target(ProgressDrawTarget::stdout());
        mp.set_move_cursor(true);

        let global_pb = mp.add(ProgressBar::new(0));
        let style = ProgressStyle::with_template("{msg}\u{1b}[0K").unwrap();
        global_pb.set_style(style);
        global_pb.set_message("Global: 0 it/s");

        let worker_style =
            ProgressStyle::with_template("{prefix} {bar:20.cyan/blue} {msg}\u{1b}[0K")
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
        let style = ProgressStyle::with_template("{msg}\u{1b}[0K").unwrap();
        stop_pb.set_style(style);
        stop_pb.set_message(" ");

        Self {
            mp,
            global_pb,
            stop_pb,
            worker_states,
        }
    }

    pub(crate) fn println(&self, msg: &str) {
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

    pub(crate) fn set_worker_job(&mut self, worker_idx: usize, job: &JobSummary) {
        if let Some(count) = job.group_proofs.filter(|count| *count > 1) {
            self.set_worker_work(
                worker_idx,
                format!("Group {count} proofs"),
                job.number_of_iterations,
            );
            return;
        }

        let field = field_vdf_label(job.field_vdf);
        self.set_worker_work(
            worker_idx,
            format!("Block {} ({field})", job.height),
            job.number_of_iterations,
        );
    }

    pub(crate) fn set_worker_progress(&mut self, worker_idx: usize, iters_done: u64) {
        let Some(state) = self.worker_states.get_mut(worker_idx) else {
            return;
        };
        let step = calc_progress_step(state.total_iters, iters_done);
        if step != state.last_step {
            state.last_step = step;
            state.bar.set_position(step);
        }
    }

    pub(crate) fn set_worker_idle(&mut self, worker_idx: usize) {
        let Some(state) = self.worker_states.get_mut(worker_idx) else {
            return;
        };
        state.bar.set_position(0);
        state.bar.set_message("Idle".to_string());
        state.total_iters = 0;
        state.last_step = 0;
    }

    pub(crate) fn set_stop_message(&mut self, msg: &str) {
        self.stop_pb.set_message(msg.to_string());
    }

    pub(crate) fn tick_global(&self, speed: u64, busy: usize, total: usize) {
        self.global_pb.set_message(format!(
            "Global: {} it/s (running {busy}/{total})",
            format_number(speed)
        ));
    }

    pub(crate) fn freeze(&self) {
        self.mp.set_move_cursor(false);
        for worker in &self.worker_states {
            worker.bar.abandon();
        }
        self.global_pb.abandon();
        self.stop_pb.abandon();
        let _ = std::io::stdout().write_all(b"\n");
    }
}

fn calc_progress_step(total_iters: u64, iters_done: u64) -> u64 {
    let total = total_iters.max(1);
    let iters_done = iters_done.min(total_iters);
    ((iters_done.saturating_mul(PROGRESS_BAR_STEPS)) / total).min(PROGRESS_BAR_STEPS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calc_progress_step_clamps_and_scales() {
        let steps = PROGRESS_BAR_STEPS;
        assert_eq!(calc_progress_step(100, 0), 0);
        assert_eq!(calc_progress_step(100, 50), steps / 2);
        assert_eq!(calc_progress_step(100, 100), steps);
        assert_eq!(calc_progress_step(100, 1000), steps);
    }
}
