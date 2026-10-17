//! The two single-connection feeds: RTDS trade activity (with wallets) and live game scores.
//!
//! Streams written:
//! - `trades` (under the event's `<category>/<league>`): RTDS `activity/trades` messages;
//!   untracked events go to `other/untracked` when `only_tracked = false`
//! - `scores`: sports live-state updates, under the configured league whose `score_league`
//!   (default: its key) matches `leagueAbbreviation`, else `other/<leagueAbbreviation>`
//! - `_system/rtds/sessions`, `_system/sports/sessions`: connect / disconnect markers

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use pm_core::now_ns;
use pm_core::wire::{clob_ws::split_frame, rtds, sports};
use pm_store::{Partition, StoreHandle};
use pm_ws::{WsEvent, WsSpec};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::{ActivitySection, SportsSection};
use crate::registry::{self, SharedRegistry};

#[derive(Default)]
pub struct FeedStats {
    pub kept: AtomicU64,
    pub dropped: AtomicU64,
}

async fn session_marker(store: &StoreHandle, feed_name: &str, ev: &WsEvent) -> bool {
    let (session, m) = match ev {
        WsEvent::Connected { feed, session } => (*session, json!({"feed": &**feed, "event": "connected"})),
        WsEvent::Disconnected { feed, session, reason } => {
            (*session, json!({"feed": &**feed, "event": "disconnected", "reason": reason}))
        }
        WsEvent::Text { .. } => return false,
    };
    store.write(&registry::system(feed_name), "sessions", now_ns(), session, &m.to_string()).await;
    true
}

pub async fn run_activity(
    cfg: ActivitySection,
    store: StoreHandle,
    reg: SharedRegistry,
    stats: Arc<FeedStats>,
    cancel: CancellationToken,
) {
    let mut spec = WsSpec::new("rtds", rtds::RTDS_URL);
    spec.keepalive = Some((Duration::from_secs(5), "PING".into()));
    spec.ignore = vec!["PONG".into(), "pong".into()];
    spec.idle_timeout = Duration::from_secs(60);
    let (tx, mut rx) = mpsc::channel(10_000);
    let (_h, _task) = pm_ws::spawn(spec, Arc::new(|| vec![rtds::subscribe_trades_msg()]), tx, cancel);
    let untracked = Partition::new(registry::OTHER, "untracked");

    while let Some(ev) = rx.recv().await {
        if session_marker(&store, "rtds", &ev).await {
            continue;
        }
        let WsEvent::Text { session, recv_ns, text } = ev else { continue };
        let Some(parts) = split_frame(&text) else { continue };
        for raw in parts {
            let probe: Option<rtds::RtdsProbe> = serde_json::from_str(raw.get()).ok();
            let Some(p) = probe.and_then(|p| p.payload) else { continue };
            let part = {
                let r = reg.read().unwrap();
                p.condition_id
                    .and_then(|c| r.market_partition(c))
                    .or_else(|| p.event_slug.and_then(|s| r.event_partition(s)))
            };
            let part = match (part, cfg.only_tracked) {
                (Some(p), _) => p,
                (None, true) => {
                    stats.dropped.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                (None, false) => untracked.clone(),
            };
            stats.kept.fetch_add(1, Ordering::Relaxed);
            store.write(&part, "trades", recv_ns, session, raw.get()).await;
        }
    }
}

#[derive(Deserialize)]
struct LeagueProbe<'a> {
    #[serde(rename = "leagueAbbreviation", borrow, default)]
    league: Option<&'a str>,
}

pub async fn run_sports(
    cfg: SportsSection,
    store: StoreHandle,
    reg: SharedRegistry,
    stats: Arc<FeedStats>,
    cancel: CancellationToken,
) {
    let mut spec = WsSpec::new("sports", sports::SPORTS_WS_URL);
    spec.auto_reply = vec![("ping".into(), "pong".into())];
    spec.idle_timeout = Duration::from_secs(120);
    let (tx, mut rx) = mpsc::channel(10_000);
    let (_h, _task) = pm_ws::spawn(spec, Arc::new(Vec::new), tx, cancel);
    let wanted: Vec<String> = cfg.leagues.iter().map(|l| l.to_ascii_lowercase()).collect();

    while let Some(ev) = rx.recv().await {
        if session_marker(&store, "sports", &ev).await {
            continue;
        }
        let WsEvent::Text { session, recv_ns, text } = ev else { continue };
        let Some(parts) = split_frame(&text) else { continue };
        for raw in parts {
            let lg = serde_json::from_str::<LeagueProbe>(raw.get())
                .ok()
                .and_then(|p| p.league)
                .unwrap_or("unknown")
                .to_ascii_lowercase();
            if !wanted.is_empty() && !wanted.contains(&lg) {
                stats.dropped.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            stats.kept.fetch_add(1, Ordering::Relaxed);
            let part = reg.read().unwrap().score_league(&lg);
            store.write(&part, "scores", recv_ns, session, raw.get()).await;
        }
    }
}
