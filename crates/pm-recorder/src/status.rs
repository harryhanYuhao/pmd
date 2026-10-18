//! Human-readable progress reports: a summary line every interval, one line per league with
//! what was written, and a final total at shutdown.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use pm_store::{StatsKey, StoreHandle, StreamStats};
use tokio_util::sync::CancellationToken;

use crate::clob::{SharedClobManager, SharedClobStats};
use crate::feeds::FeedStats;
use crate::registry::SharedRegistry;

pub struct Sources {
    pub store: StoreHandle,
    pub reg: SharedRegistry,
    pub clob: Option<(SharedClobManager, SharedClobStats)>,
    pub trades: Option<Arc<FeedStats>>,
    pub scores: Option<Arc<FeedStats>>,
}

#[derive(Default, Clone, Copy)]
struct Counters {
    events: u64,
    pruned: u64,
    missing: u64,
    resyncs: u64,
    unparsed: u64,
    trades: u64,
    scores: u64,
}

pub async fn run(src: Sources, every: Duration, cancel: CancellationToken) {
    let mut iv = tokio::time::interval(every);
    iv.tick().await;
    let mut prev = Counters::default();
    let mut prev_store: HashMap<StatsKey, StreamStats> = HashMap::new();
    let mut prev_t = Instant::now();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = iv.tick() => {}
        }
        let secs = prev_t.elapsed().as_secs_f64().max(1.0);
        prev_t = Instant::now();
        let now = counters(&src);
        let store = src.store.stats();
        report(&src, &now, &prev, &store, &prev_store, secs);
        prev = now;
        prev_store = store;
    }
}

fn counters(src: &Sources) -> Counters {
    let mut c = Counters::default();
    if let Some((_, st)) = &src.clob {
        let s = st.lock().unwrap();
        (c.events, c.pruned, c.missing, c.resyncs, c.unparsed) = (s.events, s.pruned, s.missing, s.resyncs, s.unparsed);
    }
    c.trades = src.trades.as_ref().map_or(0, |f| f.kept.load(Ordering::Relaxed));
    c.scores = src.scores.as_ref().map_or(0, |f| f.kept.load(Ordering::Relaxed));
    c
}

fn report(
    src: &Sources,
    now: &Counters,
    prev: &Counters,
    store: &HashMap<StatsKey, StreamStats>,
    prev_store: &HashMap<StatsKey, StreamStats>,
    secs: f64,
) {
    let mins = secs / 60.0;
    // Markets per partition.
    let (n_markets, n_tokens, markets_by) = {
        let r = src.reg.read().unwrap();
        let mut by: BTreeMap<String, usize> = BTreeMap::new();
        for c in r.tracked.keys() {
            if let Some(p) = r.market_partition(c.as_str()) {
                *by.entry(p.to_string()).or_default() += 1;
            }
        }
        (
            r.tracked.len(),
            r.tracked.values().map(|m| m.token_ids.len()).sum::<usize>(),
            by,
        )
    };

    // Bytes per partition: (disk now, disk in window, raw in window). `_system` and `other/*`
    // are folded into one line each.
    let group = |k: &StatsKey| match &*k.0.category {
        "_system" | "other" => k.0.category.to_string(),
        _ => k.0.to_string(),
    };
    let mut by: BTreeMap<String, (u64, u64, u64)> = BTreeMap::new();
    for p in markets_by.keys() {
        by.entry(p.clone()).or_default();
    }
    for (k, s) in store {
        let old = prev_store.get(k).cloned().unwrap_or_default();
        let e = by.entry(group(k)).or_default();
        e.0 += s.disk_bytes;
        e.1 += s.disk_bytes - old.disk_bytes;
        e.2 += s.bytes - old.bytes;
    }
    let disk_total: u64 = by.values().map(|v| v.0).sum();
    let disk_win: u64 = by.values().map(|v| v.1).sum();
    let raw_win: u64 = by.values().map(|v| v.2).sum();

    let mut parts = vec![format!("{n_markets} markets ({n_tokens} tokens)")];
    if let Some((mgr, st)) = &src.clob {
        let up = st.lock().unwrap().connected.len();
        let total = mgr.lock().unwrap().shard_count();
        parts.push(format!("CLOB {up}/{total} connections up"));
    }
    let feed = |name: &str, f: &Option<Arc<FeedStats>>| {
        f.as_ref().map(|f| {
            format!(
                "{name} {}",
                if f.connected.load(Ordering::Relaxed) {
                    "up"
                } else {
                    "DOWN"
                }
            )
        })
    };
    parts.extend(feed("trades feed", &src.trades));
    parts.extend(feed("scores feed", &src.scores));
    tracing::info!("status: {}", parts.join(" | "));

    let ev = now.events - prev.events;
    let mut flow = vec![
        format!("{} book/trade events ({:.0}/s)", count(ev), ev as f64 / secs),
        format!("{} wallet trades", count(now.trades - prev.trades)),
        format!("{} score updates", count(now.scores - prev.scores)),
    ];
    let (pr, rs) = (now.pruned - prev.pruned, now.resyncs - prev.resyncs);
    if pr + rs > 0 {
        flow.push(format!("book repairs: {pr} pruned, {rs} resynced"));
    }
    if now.unparsed > prev.unparsed {
        flow.push(format!("{} unparsed CLOB events", now.unparsed - prev.unparsed));
    }
    tracing::info!("  last {:.0}s: {}", secs, flow.join(", "));
    tracing::info!(
        "  written: {}/min to disk ({}/min raw), {} total since start (~{}/day)",
        bytes(disk_win as f64 / mins),
        bytes(raw_win as f64 / mins),
        bytes(disk_total as f64),
        bytes(disk_win as f64 / mins * 1440.0),
    );
    for (p, (total, win, _)) in &by {
        let m = markets_by.get(p).map_or(String::new(), |n| format!("{n:>5} markets"));
        tracing::info!(
            "    {p:<16} {m:>13}  {:>10}/min  {:>10} total",
            bytes(*win as f64 / mins),
            bytes(*total as f64)
        );
    }
}

/// Final totals after the store closed every file.
pub fn final_report(stats: &HashMap<StatsKey, StreamStats>, ran: Duration) {
    let disk: u64 = stats.values().map(|s| s.disk_bytes).sum();
    let lines: u64 = stats.values().map(|s| s.lines).sum();
    tracing::info!(
        "recorded {} records, {} on disk in {:.1} min",
        count(lines),
        bytes(disk as f64),
        ran.as_secs_f64() / 60.0
    );
}

pub fn bytes(b: f64) -> String {
    match b {
        b if b >= 1e9 => format!("{:.2} GB", b / 1e9),
        b if b >= 1e6 => format!("{:.1} MB", b / 1e6),
        b if b >= 1e3 => format!("{:.0} KB", b / 1e3),
        b => format!("{b:.0} B"),
    }
}

pub fn count(n: u64) -> String {
    match n {
        n if n >= 1_000_000 => format!("{:.2}M", n as f64 / 1e6),
        n if n >= 10_000 => format!("{:.1}k", n as f64 / 1e3),
        n => n.to_string(),
    }
}
