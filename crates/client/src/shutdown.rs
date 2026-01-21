use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use tokio::sync::mpsc;

#[derive(Debug)]
pub struct ShutdownController {
    graceful: AtomicBool,
    forced: AtomicU8,
}

#[derive(Debug, Clone, Copy)]
pub enum ShutdownEvent {
    Graceful,
    Immediate,
}

impl ShutdownController {
    pub fn new() -> Self {
        Self {
            graceful: AtomicBool::new(false),
            forced: AtomicU8::new(0),
        }
    }

    pub fn request_graceful(&self) {
        self.graceful.store(true, Ordering::SeqCst);
    }

    pub fn should_exit_graceful(&self) -> bool {
        self.graceful.load(Ordering::SeqCst)
    }

    pub fn bump_forced(&self) -> u8 {
        self.forced.fetch_add(1, Ordering::SeqCst) + 1
    }
}

pub fn spawn_ctrl_c_handler(
    shutdown: Arc<ShutdownController>,
    shutdown_tx: mpsc::UnboundedSender<ShutdownEvent>,
) {
    tokio::spawn(async move {
        loop {
            if tokio::signal::ctrl_c().await.is_err() {
                return;
            }
            let n = shutdown.bump_forced();
            if n == 1 {
                shutdown.request_graceful();
                let _ = shutdown_tx.send(ShutdownEvent::Graceful);
            } else {
                let _ = shutdown_tx.send(ShutdownEvent::Immediate);
                return;
            }
        }
    });
}
