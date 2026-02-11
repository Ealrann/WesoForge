use std::collections::VecDeque;
use std::io::Stdout;
use std::time::Instant;

use bbr_client_engine::JobSummary;
use ratatui::Terminal;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::{Color, CrosstermBackend, Line, Modifier, Span, Style};
use ratatui::widgets::{Cell, Clear, List, ListItem, Paragraph, Row, Table};

use crate::format::{field_vdf_label, format_number};
use crate::terminal::TuiInputEvent;

const DETAILED_PROGRESS_BAR_WIDTH: usize = 20;
const MAX_LOG_LINES: usize = 200;
const LOG_SHORTCUTS: &str = "Logs: Up/Down PgUp/PgDn Home/End | h = hide logs";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TopViewMode {
    Detailed,
    Grid4,
}

impl TopViewMode {
    fn toggle(self) -> Self {
        match self {
            TopViewMode::Detailed => TopViewMode::Grid4,
            TopViewMode::Grid4 => TopViewMode::Detailed,
        }
    }
}

struct WorkerUiState {
    total_iters: u64,
    reported_iters_done: u64,
    display_iters_done: u64,
    squaring_iters_per_sec: u64,
    effective_iters_per_sec: u64,
    last_reported_at: Instant,
    label: String,
}

pub(crate) struct Ui {
    terminal: Terminal<CrosstermBackend<Stdout>>,
    worker_states: Vec<WorkerUiState>,
    worker_prefix_width: usize,
    top_mode: TopViewMode,
    global_message: String,
    status_prefix: String,
    stop_message: String,
    logs: VecDeque<String>,
    logs_visible: bool,
    log_scroll_from_bottom: usize,
    log_viewport_height: usize,
    needs_redraw_clear: bool,
    dirty: bool,
}

impl Ui {
    pub(crate) fn new(worker_count: usize) -> anyhow::Result<Self> {
        let backend = CrosstermBackend::new(std::io::stdout());
        let terminal = Terminal::new(backend)?;
        let mut worker_states = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            worker_states.push(WorkerUiState {
                total_iters: 0,
                reported_iters_done: 0,
                display_iters_done: 0,
                squaring_iters_per_sec: 0,
                effective_iters_per_sec: 0,
                last_reported_at: Instant::now(),
                label: "Idle".to_string(),
            });
        }

        let mut ui = Self {
            terminal,
            worker_states,
            worker_prefix_width: worker_count.max(1).ilog10() as usize + 1,
            top_mode: TopViewMode::Detailed,
            global_message: "Global: 0 it/s".to_string(),
            status_prefix: String::new(),
            stop_message: String::new(),
            logs: VecDeque::new(),
            logs_visible: true,
            log_scroll_from_bottom: 0,
            log_viewport_height: 1,
            needs_redraw_clear: true,
            dirty: true,
        };
        ui.redraw();
        Ok(ui)
    }

    pub(crate) fn println(&mut self, msg: &str) {
        if self.logs.len() >= MAX_LOG_LINES {
            self.logs.pop_front();
            if self.log_scroll_from_bottom > 0 {
                self.log_scroll_from_bottom = self.log_scroll_from_bottom.saturating_sub(1);
            }
        }
        if self.log_scroll_from_bottom > 0 {
            self.log_scroll_from_bottom = self.log_scroll_from_bottom.saturating_add(1);
        }
        self.logs.push_back(msg.to_string());
        self.clamp_log_scroll();
        self.dirty = true;
        self.redraw();
    }

    pub(crate) fn handle_input(&mut self, event: TuiInputEvent) {
        match event {
            TuiInputEvent::ToggleTopMode => {
                self.top_mode = self.top_mode.toggle();
                self.needs_redraw_clear = true;
            }
            TuiInputEvent::ToggleLogPane => {
                self.logs_visible = !self.logs_visible;
                self.needs_redraw_clear = true;
            }
            TuiInputEvent::TerminalResized => self.needs_redraw_clear = true,
            TuiInputEvent::LogUp => self.scroll_logs_up(1),
            TuiInputEvent::LogDown => self.scroll_logs_down(1),
            TuiInputEvent::LogPageUp => self.scroll_logs_up(self.log_viewport_height.max(1)),
            TuiInputEvent::LogPageDown => self.scroll_logs_down(self.log_viewport_height.max(1)),
            TuiInputEvent::LogHome => self.log_scroll_from_bottom = self.max_log_scroll(),
            TuiInputEvent::LogEnd => self.log_scroll_from_bottom = 0,
        }
        self.clamp_log_scroll();
        self.dirty = true;
        self.redraw();
    }

    fn set_worker_work(&mut self, worker_idx: usize, msg: String, total_iters: u64) {
        let Some(state) = self.worker_states.get_mut(worker_idx) else {
            return;
        };
        state.total_iters = total_iters.max(1);
        state.reported_iters_done = 0;
        state.display_iters_done = 0;
        state.squaring_iters_per_sec = 0;
        state.effective_iters_per_sec = 0;
        state.last_reported_at = Instant::now();
        state.label = msg;
        self.dirty = true;
        self.redraw();
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

    pub(crate) fn set_worker_progress(
        &mut self,
        worker_idx: usize,
        iters_done: u64,
        squaring_iters_per_sec: u64,
        effective_iters_per_sec: u64,
    ) {
        let Some(state) = self.worker_states.get_mut(worker_idx) else {
            return;
        };
        if state.total_iters == 0 {
            return;
        }
        state.reported_iters_done = iters_done.min(state.total_iters);
        state.display_iters_done = state.display_iters_done.max(state.reported_iters_done);
        state.squaring_iters_per_sec = squaring_iters_per_sec;
        state.effective_iters_per_sec = effective_iters_per_sec;
        state.last_reported_at = Instant::now();
        self.dirty = true;
    }

    pub(crate) fn set_worker_idle(&mut self, worker_idx: usize) {
        let Some(state) = self.worker_states.get_mut(worker_idx) else {
            return;
        };
        state.total_iters = 0;
        state.reported_iters_done = 0;
        state.display_iters_done = 0;
        state.squaring_iters_per_sec = 0;
        state.effective_iters_per_sec = 0;
        state.last_reported_at = Instant::now();
        state.label = "Idle".to_string();
        self.dirty = true;
        self.redraw();
    }

    pub(crate) fn set_stop_message(&mut self, msg: &str) {
        self.stop_message = msg.to_string();
        self.dirty = true;
        self.redraw();
    }

    pub(crate) fn set_status_prefix(&mut self, msg: &str) {
        self.status_prefix = msg.to_string();
        self.dirty = true;
        self.redraw();
    }

    pub(crate) fn tick_global(&mut self, speed: u64, busy: usize, total: usize) {
        self.global_message = format!(
            "Global: {} it/s (running {busy}/{total})",
            format_number(speed)
        );

        let now = Instant::now();
        for state in &mut self.worker_states {
            if state.total_iters == 0 || state.squaring_iters_per_sec == 0 {
                continue;
            }
            let elapsed = now.saturating_duration_since(state.last_reported_at);
            if elapsed.is_zero() {
                continue;
            }
            let predicted_iters = state
                .reported_iters_done
                .saturating_add(
                    (elapsed.as_secs_f64() * state.squaring_iters_per_sec as f64) as u64,
                )
                .min(state.total_iters);
            state.display_iters_done = state.display_iters_done.max(predicted_iters);
        }
        self.dirty = true;
        self.redraw();
    }

    pub(crate) fn freeze(&mut self) {
        self.dirty = true;
        self.redraw();
    }

    fn redraw(&mut self) {
        if !self.dirty && !self.needs_redraw_clear {
            return;
        }
        self.update_log_viewport_hint();
        self.clamp_log_scroll();

        let needs_redraw_clear = self.needs_redraw_clear;
        let global_message = self.global_message.clone();
        let status_message = self.status_line();
        let top_mode = self.top_mode;
        let logs_visible = self.logs_visible;
        let grid_column_widths = self
            .terminal
            .size()
            .ok()
            .map(|area| {
                let area = Rect::new(0, 0, area.width, area.height);
                let (_, top_area, _, _, _) = compute_layout(area, logs_visible);
                let (top_content_area, _) = split_pane_with_footer(top_area);
                compute_equal_column_widths(top_content_area.width as usize, 4, 1)
            })
            .unwrap_or_else(|| vec![0; 4]);
        let detailed_worker_lines: Vec<Line<'static>> = self
            .worker_states
            .iter()
            .enumerate()
            .map(|(idx, state)| self.worker_line_detailed(idx, state))
            .collect();
        let grid_worker_cells: Vec<Vec<Line<'static>>> = self
            .worker_states
            .chunks(4)
            .enumerate()
            .map(|(row_idx, chunk)| {
                let mut cells = Vec::with_capacity(4);
                let base_idx = row_idx * 4;
                for (col_idx, state) in chunk.iter().enumerate() {
                    let cell_width = grid_column_widths.get(col_idx).copied().unwrap_or(0);
                    cells.push(self.worker_cell_grid(base_idx + col_idx, state, cell_width));
                }
                while cells.len() < 4 {
                    cells.push(Line::from(""));
                }
                cells
            })
            .collect();
        let visible_logs = self.visible_logs();

        let draw_result = self.terminal.draw(|frame| {
            let (global_area, top_area, separator_area, log_area, stop_area) =
                compute_layout(frame.area(), logs_visible);
            let (top_content_area, top_footer_area) = split_pane_with_footer(top_area);
            let (log_content_area, log_footer_area) = split_pane_with_footer(log_area);
            if needs_redraw_clear {
                frame.render_widget(Clear, top_area);
                frame.render_widget(Clear, separator_area);
                frame.render_widget(Clear, log_area);
                frame.render_widget(Clear, stop_area);
            }

            let global = Paragraph::new(global_message.as_str())
                .style(Style::default().add_modifier(Modifier::BOLD));
            frame.render_widget(global, global_area);

            match top_mode {
                TopViewMode::Detailed => {
                    let workers: Vec<ListItem> = detailed_worker_lines
                        .into_iter()
                        .map(ListItem::new)
                        .collect();
                    frame.render_widget(List::new(workers), top_content_area);
                }
                TopViewMode::Grid4 => {
                    let rows: Vec<Row> = grid_worker_cells
                        .iter()
                        .map(|cells| {
                            let cols: Vec<Cell> = cells.iter().cloned().map(Cell::from).collect();
                            Row::new(cols)
                        })
                        .collect();
                    let table = Table::new(
                        rows,
                        [
                            Constraint::Ratio(1, 4),
                            Constraint::Ratio(1, 4),
                            Constraint::Ratio(1, 4),
                            Constraint::Ratio(1, 4),
                        ],
                    )
                    .column_spacing(1);
                    frame.render_widget(table, top_content_area);
                }
            }

            if logs_visible {
                let logs = Paragraph::new(visible_logs.join("\n"));
                frame.render_widget(logs, log_content_area);

                let separator = Paragraph::new("-".repeat(separator_area.width as usize))
                    .style(Style::default().fg(Color::DarkGray));
                frame.render_widget(separator, separator_area);
            }

            let top_shortcuts = match top_mode {
                TopViewMode::Detailed if logs_visible => "Top: Tab = grid view | h = hide logs",
                TopViewMode::Detailed => "Top: Tab = grid view | h = show logs",
                TopViewMode::Grid4 if logs_visible => "Top: Tab = detailed view | h = hide logs",
                TopViewMode::Grid4 => "Top: Tab = detailed view | h = show logs",
            };
            frame.render_widget(
                Paragraph::new(top_shortcuts).style(Style::default().fg(Color::DarkGray)),
                top_footer_area,
            );
            if logs_visible {
                frame.render_widget(
                    Paragraph::new(LOG_SHORTCUTS).style(Style::default().fg(Color::DarkGray)),
                    log_footer_area,
                );
            }

            let status = Paragraph::new(status_message.as_str());
            frame.render_widget(status, stop_area);
        });
        if draw_result.is_ok() && needs_redraw_clear {
            self.needs_redraw_clear = false;
        }
        if draw_result.is_ok() {
            self.dirty = false;
        }
    }

    fn worker_line_detailed(&self, worker_idx: usize, state: &WorkerUiState) -> Line<'static> {
        let prefix = format!(
            "W{:0width$}",
            worker_idx + 1,
            width = self.worker_prefix_width
        );
        if state.total_iters == 0 {
            return Line::from(format!("{prefix}  Idle"));
        }

        let total_iters = state.total_iters.max(1);
        let iters_done = state.display_iters_done.min(total_iters);
        let percent = (iters_done.saturating_mul(100) / total_iters).min(100);
        let filled = (iters_done.saturating_mul(DETAILED_PROGRESS_BAR_WIDTH as u64) / total_iters)
            .min(DETAILED_PROGRESS_BAR_WIDTH as u64) as usize;
        let eta = if state.squaring_iters_per_sec == 0 || iters_done >= total_iters {
            "--:--:--".to_string()
        } else {
            format_eta((total_iters - iters_done) / state.squaring_iters_per_sec)
        };

        let before_bar = format!("{prefix}  {percent:>3}%[");
        let bar_filled = "#".repeat(filled);
        let bar_remaining = "-".repeat(DETAILED_PROGRESS_BAR_WIDTH.saturating_sub(filled));
        let after_bar = format!(
            "] {eta} ETA  {:>12} it/s  {:>11} it   {}",
            format_number(state.effective_iters_per_sec),
            format_number(iters_done),
            state.label
        );

        Line::from(vec![
            Span::raw(before_bar),
            Span::styled(bar_filled, Style::default().fg(Color::Cyan)),
            Span::styled(bar_remaining, Style::default().fg(Color::Blue)),
            Span::raw(after_bar),
        ])
    }

    fn worker_cell_grid(
        &self,
        worker_idx: usize,
        state: &WorkerUiState,
        cell_width: usize,
    ) -> Line<'static> {
        let prefix = format!(
            "W{:0width$}",
            worker_idx + 1,
            width = self.worker_prefix_width
        );
        let left = format!("{prefix} [");
        let idle_or_eta = if state.total_iters == 0 {
            "--:--:--".to_string()
        } else {
            let total_iters = state.total_iters.max(1);
            let iters_done = state.display_iters_done.min(total_iters);
            if state.squaring_iters_per_sec == 0 || iters_done >= total_iters {
                "--:--:--".to_string()
            } else {
                format_eta((total_iters - iters_done) / state.squaring_iters_per_sec)
            }
        };
        let right = format!("] {idle_or_eta}");
        let bar_width = cell_width.saturating_sub(left.len() + right.len()).max(1);

        if state.total_iters == 0 {
            return Line::from(format!("{left}{}{}", "-".repeat(bar_width), right));
        }

        let total_iters = state.total_iters.max(1);
        let iters_done = state.display_iters_done.min(total_iters);
        let filled = (iters_done.saturating_mul(bar_width as u64) / total_iters)
            .min(bar_width as u64) as usize;

        Line::from(vec![
            Span::raw(left),
            Span::styled("#".repeat(filled), Style::default().fg(Color::Cyan)),
            Span::styled(
                "-".repeat(bar_width.saturating_sub(filled)),
                Style::default().fg(Color::Blue),
            ),
            Span::raw(right),
        ])
    }

    fn visible_logs(&self) -> Vec<String> {
        if self.logs.is_empty() || self.log_viewport_height == 0 {
            return Vec::new();
        }
        let viewport = self.log_viewport_height;
        let len = self.logs.len();
        let start = len.saturating_sub(viewport + self.log_scroll_from_bottom);
        let end = (start + viewport).min(len);
        self.logs
            .iter()
            .skip(start)
            .take(end.saturating_sub(start))
            .cloned()
            .collect()
    }

    fn scroll_logs_up(&mut self, lines: usize) {
        let max_scroll = self.max_log_scroll();
        self.log_scroll_from_bottom = self
            .log_scroll_from_bottom
            .saturating_add(lines)
            .min(max_scroll);
    }

    fn scroll_logs_down(&mut self, lines: usize) {
        self.log_scroll_from_bottom = self.log_scroll_from_bottom.saturating_sub(lines);
    }

    fn max_log_scroll(&self) -> usize {
        self.logs
            .len()
            .saturating_sub(self.log_viewport_height.max(1))
    }

    fn clamp_log_scroll(&mut self) {
        self.log_scroll_from_bottom = self.log_scroll_from_bottom.min(self.max_log_scroll());
    }

    fn update_log_viewport_hint(&mut self) {
        if let Ok(area) = self.terminal.size() {
            let area = Rect::new(0, 0, area.width, area.height);
            let (_, _, _, log_area, _) = compute_layout(area, self.logs_visible);
            let (log_content_area, _) = split_pane_with_footer(log_area);
            self.log_viewport_height = log_content_area.height as usize;
        }
    }

    fn status_line(&self) -> String {
        let prefix = self.status_prefix.trim();
        let stop = self.stop_message.trim();
        if prefix.is_empty() && stop.is_empty() {
            return " ".to_string();
        }
        if stop.is_empty() {
            return prefix.to_string();
        }
        if prefix.is_empty() {
            return stop.to_string();
        }
        format!("{prefix} | {stop}")
    }
}

fn split_pane_with_footer(area: Rect) -> (Rect, Rect) {
    if area.height == 0 {
        return (area, area);
    }
    let split = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(0), Constraint::Length(1)])
        .split(area);
    (split[0], split[1])
}

fn compute_layout(area: Rect, logs_visible: bool) -> (Rect, Rect, Rect, Rect, Rect) {
    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(1),
            Constraint::Length(1),
        ])
        .split(area);
    if !logs_visible {
        let empty = Rect::new(outer[1].x, outer[1].y, 0, 0);
        return (outer[0], outer[1], empty, empty, outer[2]);
    }
    let middle = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Ratio(2, 3),
            Constraint::Length(1),
            Constraint::Ratio(1, 3),
        ])
        .split(outer[1]);
    (outer[0], middle[0], middle[1], middle[2], outer[2])
}

fn compute_equal_column_widths(total_width: usize, columns: usize, spacing: usize) -> Vec<usize> {
    if columns == 0 {
        return Vec::new();
    }
    let total_spacing = spacing.saturating_mul(columns.saturating_sub(1));
    let content_width = total_width.saturating_sub(total_spacing);
    let base = content_width / columns;
    let mut remainder = content_width % columns;

    let mut widths = Vec::with_capacity(columns);
    for _ in 0..columns {
        let extra = usize::from(remainder > 0);
        if remainder > 0 {
            remainder -= 1;
        }
        widths.push(base + extra);
    }
    widths
}

fn format_eta(total_seconds: u64) -> String {
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}")
}
