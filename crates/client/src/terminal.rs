use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::mpsc;

use crate::shutdown::{ShutdownController, ShutdownEvent};

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
    ) -> anyhow::Result<Self> {
        crossterm::terminal::enable_raw_mode()?;
        #[cfg(unix)]
        enable_onlcr()?;

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
                if let Event::Key(key) = ev {
                    if key.code == KeyCode::Char('c')
                        && key.modifiers.contains(KeyModifiers::CONTROL)
                    {
                        let n = shutdown.bump_forced();
                        if n == 1 {
                            shutdown.request_graceful();
                            let _ = shutdown_tx.send(ShutdownEvent::Graceful);
                        } else {
                            let _ = shutdown_tx.send(ShutdownEvent::Immediate);
                        }
                    }
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
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        let _ = crossterm::terminal::disable_raw_mode();
    }
}
