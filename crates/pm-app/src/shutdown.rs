//! Shutdown: wait for a signal, then stop tasks in order.

use std::time::Duration;

/// Waits for every task after cancellation (they may hold store handles, so a store can only
/// finish once they are gone); a task that does not stop within 10 s is aborted.
pub async fn stop_tasks(tasks: Vec<(&str, tokio::task::JoinHandle<()>)>) {
    for (name, mut t) in tasks {
        if tokio::time::timeout(Duration::from_secs(10), &mut t).await.is_err() {
            tracing::warn!("{name} task did not stop in time; aborting");
            t.abort();
            let _ = t.await;
        }
    }
}

/// Resolves on Ctrl-C, SIGTERM or SIGHUP.
pub async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM");
        // tmux kill-session / closing the terminal sends SIGHUP.
        let mut hup = signal(SignalKind::hangup()).expect("SIGHUP");
        tokio::select! {
            _ = ctrl_c => {}
            _ = term.recv() => {}
            _ = hup.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = ctrl_c.await;
}
