//! pm-recorder: records Polymarket order books, trades, market metadata and live scores for the
//! configured leagues. See the workspace README for the data layout.

mod clob;
mod config;
mod discovery;
mod feeds;
mod registry;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use pm_store::{Store, StoreConfig};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::Config;

#[derive(Parser)]
#[command(about = "Record Polymarket esports feeds to hour-partitioned zstd JSONL")]
struct Args {
    #[arg(short, long, default_value = "config/recorder.toml")]
    config: PathBuf,
    /// Override `data_dir` from the config.
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// Stop after this many seconds (for smoke tests).
    #[arg(long)]
    run_for: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let mut cfg = Config::load(&args.config)?;
    if let Some(d) = args.data_dir {
        cfg.data_dir = d;
    }
    let cfg = Arc::new(cfg);
    tracing::info!(data_dir = %cfg.data_dir.display(), leagues = cfg.leagues.len(), "starting recorder");

    let store = Store::start(StoreConfig {
        root: cfg.data_dir.clone(),
        zstd_level: cfg.store.zstd_level,
        flush_every: Duration::from_secs(cfg.store.flush_secs.max(1)),
        ..Default::default()
    })?;
    let cancel = CancellationToken::new();
    let reg: registry::SharedRegistry =
        Arc::new(std::sync::RwLock::new(registry::Registry::new(&cfg)));
    let mut tasks = Vec::new();

    let clob_stats = clob::SharedClobStats::default();
    let clob_mgr = if cfg.clob.enabled {
        let (tx, rx) = mpsc::channel(50_000);
        let mgr = Arc::new(std::sync::Mutex::new(clob::ClobManager::new(
            &cfg.clob,
            tx,
            cancel.clone(),
        )));
        let h = clob::run_handler(
            rx,
            store.handle(),
            reg.clone(),
            mgr.clone(),
            cfg.clob.clone(),
            clob_stats.clone(),
            cancel.clone(),
        );
        tasks.push(tokio::spawn(h));
        Some(mgr)
    } else {
        None
    };

    let act_stats = Arc::new(feeds::FeedStats::default());
    if cfg.activity.enabled {
        tasks.push(tokio::spawn(feeds::run_activity(
            cfg.activity.clone(),
            store.handle(),
            reg.clone(),
            act_stats.clone(),
            cancel.clone(),
        )));
    }
    let sp_stats = Arc::new(feeds::FeedStats::default());
    if cfg.sports.enabled {
        tasks.push(tokio::spawn(feeds::run_sports(
            cfg.sports.clone(),
            store.handle(),
            reg.clone(),
            sp_stats.clone(),
            cancel.clone(),
        )));
    }

    let disc = discovery::Discovery::new(cfg.clone(), store.handle(), reg.clone(), clob_mgr);
    tasks.push(tokio::spawn(disc.run(cancel.clone())));

    // Periodic status line.
    {
        let (store_h, reg, cancel) = (store.handle(), reg.clone(), cancel.clone());
        let (cs, a, s) = (clob_stats.clone(), act_stats.clone(), sp_stats.clone());
        tasks.push(tokio::spawn(async move {
            let mut iv = tokio::time::interval(Duration::from_secs(60));
            iv.tick().await;
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = iv.tick() => {}
                }
                let st = store_h.stats();
                let mb: f64 = st.values().map(|v| v.bytes as f64).sum::<f64>() / 1e6;
                let c = cs.lock().unwrap();
                tracing::info!(
                    markets = reg.read().unwrap().tracked.len(),
                    clob_events = c.events,
                    clob_unparsed = c.unparsed,
                    dropped = c.dropped,
                    pruned = c.pruned,
                    missing = c.missing,
                    resyncs = c.resyncs,
                    snapshots = c.snapshots,
                    trades = a.kept.load(Ordering::Relaxed),
                    trades_other = a.dropped.load(Ordering::Relaxed),
                    scores = s.kept.load(Ordering::Relaxed),
                    raw_mb = format!("{mb:.1}"),
                    "status"
                );
            }
        }));
    }

    let stop = async {
        match args.run_for {
            Some(s) => tokio::time::sleep(Duration::from_secs(s)).await,
            None => shutdown_signal().await,
        }
    };
    stop.await;
    tracing::info!("shutting down");
    cancel.cancel();
    // Tasks hold store handles; the store can only finish once they are gone.
    for mut t in tasks {
        if tokio::time::timeout(Duration::from_secs(10), &mut t)
            .await
            .is_err()
        {
            tracing::warn!("task did not stop in time; aborting");
            t.abort();
            let _ = t.await;
        }
    }
    store.shutdown().await?;
    tracing::info!("all files closed");
    Ok(())
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
