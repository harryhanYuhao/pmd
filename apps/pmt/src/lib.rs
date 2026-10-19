//! pmt: the Polymarket trading bot. See `docs/pmt/design.md`.
//!
//! [`run`] wires the tasks: the universe poller (Gamma), the CLOB market channel, the trade-history
//! loader (Data API) and the [`engine::Engine`] that runs the strategies, applies risk limits,
//! sends orders to the venue and journals everything.

pub mod config;
pub mod engine;
pub mod exec;
pub mod history;
pub mod journal;
pub mod market;
pub mod order;
pub mod portfolio;
pub mod report;
pub mod risk;
pub mod state;
pub mod strategy;
pub mod tape;
pub mod universe;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use pm_store::{Store, StoreConfig};
use pm_ws::clob::ClobManager;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub use crate::config::Config;
use crate::strategy::Strategy;

/// Builds the enabled strategies of the config, each with its `entry_cutoff`.
pub fn strategies(cfg: &Config) -> Result<Vec<(Box<dyn Strategy>, Duration)>> {
    cfg.strategies
        .iter()
        .filter(|s| s.enabled)
        .map(|e| Ok((strategy::build(e)?, e.entry_cutoff.map_or(Duration::ZERO, |d| d.0))))
        .collect()
}

/// Trades until the scheduled stop (`[run] run_for`) or a signal, then winds down: no new trades,
/// and the run ends once nothing is in progress (at most `drain_timeout`). A second signal ends it
/// at once. Working orders are cancelled and the journal closed either way. Logging must already
/// be set up.
pub async fn run(cfg: Config) -> Result<engine::Ending> {
    let strategies = strategies(&cfg)?;
    anyhow::ensure!(!strategies.is_empty(), "no strategy is enabled");
    let mode = cfg.mode.name();
    let ids: Vec<String> = strategies
        .iter()
        .map(|(s, cut)| match cut.is_zero() {
            true => s.id().to_string(),
            false => format!("{} (entry cutoff {})", s.id(), config::HumanDuration(*cut)),
        })
        .collect();
    tracing::info!("pmt starting in {mode} mode: strategies {}", ids.join(", "));
    let schedule = engine::Schedule {
        stop_at_ns: cfg.run.run_for.map(|d| pm_core::now_ns() + d.0.as_nanos() as i64),
        drain_timeout: cfg.run.drain_timeout.0,
    };
    match (cfg.run.run_for, schedule.stop_at_ns) {
        (Some(d), Some(t)) => {
            let at = chrono::DateTime::from_timestamp_nanos(t).with_timezone(&chrono::Local);
            tracing::info!(
                "scheduled stop: {} (in {d}); then wind down for at most {}",
                at.format("%Y-%m-%d %H:%M"),
                cfg.run.drain_timeout
            );
        }
        _ => tracing::info!("no scheduled stop: runs until Ctrl-C / SIGTERM / SIGHUP"),
    }
    if let Some(run_for) = cfg.run.run_for {
        for (st, cut) in &strategies {
            if *cut >= run_for.0 {
                tracing::warn!(
                    "{}: entry_cutoff {} is not shorter than run_for {run_for}: it will enter no market",
                    st.id(),
                    config::HumanDuration(*cut)
                );
            }
        }
    }
    let leagues: Vec<&str> = cfg.leagues.iter().map(|l| l.key.as_str()).collect();
    tracing::info!("leagues: {}", leagues.join(", "));
    tracing::info!("journal -> {}/YYYY/MM/DD/{mode}/<strategy>/", cfg.journal_dir.display());

    let store = Store::start(StoreConfig {
        root: cfg.journal_dir.clone(),
        flush_every: Duration::from_secs(1),
        channel_capacity: 10_000,
        ..Default::default()
    })?;
    let restored = {
        let root = cfg.journal_dir.clone();
        tokio::task::spawn_blocking(move || journal::restore(&root, mode)).await??
    };
    if restored.records > 0 {
        tracing::info!(
            "journal: replayed {} records: {} open positions (${}), {} markets already decided",
            restored.records,
            restored.portfolio.positions().count(),
            restored.portfolio.open_cost(),
            restored.decided.values().map(|t| t.len()).sum::<usize>()
        );
    }

    let cancel = CancellationToken::new();
    let mut tasks = Vec::new();
    let (ws_tx, ws_rx) = mpsc::channel(50_000);
    let clob = ClobManager::new(cfg.universe.max_assets_per_conn, true, ws_tx, cancel.clone());
    let (uni_tx, uni_rx) = mpsc::channel(4);
    let held = universe::Held::default();
    let uni = universe::run(
        cfg.universe.clone(),
        cfg.leagues.clone(),
        held.clone(),
        uni_tx,
        cancel.clone(),
    );
    tasks.push(("universe", tokio::spawn(uni)));
    let (hist_req_tx, hist_req_rx) = mpsc::unbounded_channel();
    let (hist_tx, hist_rx) = mpsc::channel(1_000);
    let hist = history::run(hist_req_rx, hist_tx, cfg.history.max_trades, cancel.clone());
    tasks.push(("history", tokio::spawn(hist)));

    let engine = engine::Engine::new(engine::Parts {
        strategies,
        schedule,
        restored,
        risk: risk::Risk::new(&cfg.risk),
        venue: Box::new(exec::paper::Paper::default()),
        journal: journal::Journal::new(store.handle(), mode),
        clob,
        history: hist_req_tx,
        held: Arc::clone(&held),
    });
    let inputs = engine::Inputs {
        universe: uni_rx,
        ws: ws_rx,
        history: hist_rx,
    };
    let status = Duration::from_secs(cfg.log.status_secs.max(10));
    let (ctl_tx, ctl_rx) = mpsc::channel(4);
    let signals = tokio::spawn(async move {
        pm_app::shutdown_signal().await;
        let _ = ctl_tx.send(engine::Control::WindDown("signal".into())).await;
        pm_app::shutdown_signal().await;
        let _ = ctl_tx.send(engine::Control::Stop("second signal".into())).await;
    });

    // The engine decides when the run is over; the feeds stop after it.
    let ending = engine.run(inputs, ctl_rx, status).await;
    signals.abort();
    cancel.cancel();
    pm_app::stop_tasks(tasks).await;
    store.shutdown().await?;
    tracing::info!("journal closed");
    Ok(ending)
}
