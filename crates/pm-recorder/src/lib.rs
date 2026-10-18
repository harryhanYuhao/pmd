//! pm-recorder: records Polymarket order books, trades, market metadata and live scores for the
//! configured leagues. See the workspace README for the data layout. Run it with `pm record`.

mod clob;
pub mod config;
mod discovery;
mod feeds;
mod registry;
mod sessions;
mod status;

use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use anyhow::Result;
use pm_store::{Store, StoreConfig};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub use crate::config::Config;
use crate::registry::{Registry, SharedRegistry};

/// Records until Ctrl-C / SIGTERM / SIGHUP, or for `run_for` if given (smoke tests), then
/// finishes every file. Logging must already be set up.
pub async fn run(cfg: Config, run_for: Option<Duration>) -> Result<()> {
    let started = std::time::Instant::now();
    let cfg = Arc::new(cfg);
    log_startup(&cfg);

    let store = Store::start(StoreConfig {
        root: cfg.data_dir.clone(),
        zstd_level: cfg.store.zstd_level,
        flush_every: Duration::from_secs(cfg.store.flush_secs.max(1)),
        ..Default::default()
    })?;
    let cancel = CancellationToken::new();
    let reg: SharedRegistry = Arc::new(RwLock::new(Registry::new(&cfg)));
    let mut tasks = Vec::new();

    // Order books: the handler consumes every shard; discovery tells the manager what to subscribe.
    let clob_stats = clob::SharedClobStats::default();
    let clob_mgr = cfg.clob.enabled.then(|| {
        let (tx, rx) = mpsc::channel(50_000);
        let mgr = Arc::new(Mutex::new(clob::ClobManager::new(&cfg.clob, tx, cancel.clone())));
        let handler = clob::run_handler(
            rx,
            store.handle(),
            reg.clone(),
            mgr.clone(),
            cfg.clob.clone(),
            clob_stats.clone(),
            cancel.clone(),
        );
        tasks.push(("clob", tokio::spawn(handler)));
        mgr
    });

    let trade_stats = Arc::new(feeds::FeedStats::default());
    if cfg.activity.enabled {
        let feed = feeds::run_activity(
            cfg.activity.clone(),
            store.handle(),
            reg.clone(),
            trade_stats.clone(),
            cancel.clone(),
        );
        tasks.push(("trades feed", tokio::spawn(feed)));
    }
    let score_stats = Arc::new(feeds::FeedStats::default());
    if cfg.sports.enabled {
        let feed = feeds::run_sports(
            cfg.sports.clone(),
            store.handle(),
            reg.clone(),
            score_stats.clone(),
            cancel.clone(),
        );
        tasks.push(("scores feed", tokio::spawn(feed)));
    }

    let disc = discovery::Discovery::new(cfg.clone(), store.handle(), reg.clone(), clob_mgr.clone());
    tasks.push(("discovery", tokio::spawn(disc.run(cancel.clone()))));

    let sources = status::Sources {
        store: store.handle(),
        reg: reg.clone(),
        clob: clob_mgr.map(|m| (m, clob_stats)),
        trades: cfg.activity.enabled.then_some(trade_stats),
        scores: cfg.sports.enabled.then_some(score_stats),
    };
    let every = Duration::from_secs(cfg.log.status_secs.max(5));
    tasks.push(("status", tokio::spawn(status::run(sources, every, cancel.clone()))));

    match run_for {
        Some(d) => tokio::time::sleep(d).await,
        None => shutdown_signal().await,
    }
    tracing::info!("shutting down: finishing files...");
    cancel.cancel();
    stop_tasks(tasks).await;
    let totals = store.shutdown().await?;
    status::final_report(&totals, started.elapsed());
    tracing::info!("all files closed");
    Ok(())
}

fn log_startup(cfg: &Config) {
    let leagues: Vec<String> = cfg
        .leagues
        .iter()
        .map(|l| format!("{}/{}", l.category, l.key))
        .collect();
    tracing::info!(
        "pm-recorder starting: {} leagues ({})",
        leagues.len(),
        leagues.join(", ")
    );
    tracing::info!("data -> {}", cfg.data_dir.display());
    if let Some(f) = &cfg.log.file {
        tracing::info!("log  -> {} (and stdout)", f.display());
    }
    let feeds: Vec<&str> = [
        (cfg.clob.enabled, "order books"),
        (cfg.activity.enabled, "wallet trades"),
        (cfg.sports.enabled, "live scores"),
    ]
    .into_iter()
    .filter_map(|(on, name)| on.then_some(name))
    .collect();
    tracing::info!("feeds: {}", feeds.join(", "));
}

/// Waits for every task after cancellation (they hold store handles, so the store can only finish
/// once they are gone); a task that does not stop within 10 s is aborted.
async fn stop_tasks(tasks: Vec<(&str, tokio::task::JoinHandle<()>)>) {
    for (name, mut t) in tasks {
        if tokio::time::timeout(Duration::from_secs(10), &mut t).await.is_err() {
            tracing::warn!("{name} task did not stop in time; aborting");
            t.abort();
            let _ = t.await;
        }
    }
}

async fn shutdown_signal() {
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
