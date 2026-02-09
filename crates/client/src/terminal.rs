use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crossterm::cursor::{Hide, Show};
use crossterm::execute;
use crossterm::terminal::{EnterAlternateScreen, LeaveAlternateScreen};
use tokio::sync::mpsc;

use crate::shutdown::{ShutdownController, ShutdownEvent};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TuiInputEvent {
    ToggleTopMode,
    ToggleLogPane,
    TerminalResized,
    LogUp,
    LogDown,
    LogPageUp,
    LogPageDown,
    LogHome,
    LogEnd,
}

#[cfg(unix)]
fn enable_onlcr() -> anyhow::Result<()> {
    use std::os::fd::AsRawFd as _;

    let fd = std::io::stdout().as_raw_fd();
    unsafe {
        let mut termios: libc::termios = std::mem::zeroed();
        if libc::tcgetattr(fd, &mut termios) != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        termios.c_oflag |= (libc::OPOST | libc::ONLCR) as libc::tcflag_t;
        if libc::tcsetattr(fd, libc::TCSANOW, &termios) != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
    }
    Ok(())
}

pub struct TuiTerminal {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl TuiTerminal {
    pub fn enter(
        shutdown: Arc<ShutdownController>,
        shutdown_tx: mpsc::UnboundedSender<ShutdownEvent>,
        input_tx: mpsc::UnboundedSender<TuiInputEvent>,
    ) -> anyhow::Result<Self> {
        crossterm::terminal::enable_raw_mode()?;
        execute!(std::io::stdout(), EnterAlternateScreen, Hide)?;
        #[cfg(unix)]
        if let Err(err) = enable_onlcr() {
            let _ = execute!(std::io::stdout(), Show, LeaveAlternateScreen);
            let _ = crossterm::terminal::disable_raw_mode();
            return Err(err);
        }

        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = stop.clone();
        let thread = std::thread::spawn(move || {
            use crossterm::event::{Event, KeyCode, KeyModifiers};

            while !stop_thread.load(Ordering::Relaxed) {
                if !crossterm::event::poll(Duration::from_millis(200)).unwrap_or(false) {
                    continue;
                }
                let Ok(ev) = crossterm::event::read() else {
                    continue;
                };
                match ev {
                    Event::Resize(_, _) => {
                        let _ = input_tx.send(TuiInputEvent::TerminalResized);
                    }
                    Event::Key(key) => {
                        if key.code == KeyCode::Char('c')
                            && key.modifiers.contains(KeyModifiers::CONTROL)
                        {
                            let n = shutdown.bump_forced();
                            if n == 1 {
                                let _ = shutdown_tx.send(ShutdownEvent::Graceful);
                            } else {
                                let _ = shutdown_tx.send(ShutdownEvent::Immediate);
                            }
                            continue;
                        }

                        let mapped = match key.code {
                            KeyCode::Up => Some(TuiInputEvent::LogUp),
                            KeyCode::Down => Some(TuiInputEvent::LogDown),
                            KeyCode::PageUp => Some(TuiInputEvent::LogPageUp),
                            KeyCode::PageDown => Some(TuiInputEvent::LogPageDown),
                            KeyCode::Home => Some(TuiInputEvent::LogHome),
                            KeyCode::End => Some(TuiInputEvent::LogEnd),
                            KeyCode::Tab => Some(TuiInputEvent::ToggleTopMode),
                            KeyCode::Char(ch) if ch.eq_ignore_ascii_case(&'h') => {
                                Some(TuiInputEvent::ToggleLogPane)
                            }
                            _ => None,
                        };
                        if let Some(ev) = mapped {
                            let _ = input_tx.send(ev);
                        }
                    }
                    _ => {}
                }
            }
        });

        Ok(Self {
            stop,
            thread: Some(thread),
        })
    }
}

impl Drop for TuiTerminal {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = execute!(std::io::stdout(), Show, LeaveAlternateScreen);
        let _ = crossterm::terminal::disable_raw_mode();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
