//! Market discovery: polls Gamma for open events of each configured league, records metadata,
//! and tells the CLOB recorder which tokens to (un)subscribe.
//!
//! Streams written:
//! - `gamma_events`: the raw Gamma event JSON, on first sight, whenever its trading-
//!   relevant state changes (live/ended/score/market status/prices at resolution), and at least
//!   every `FULL_SNAPSHOT` regardless.
//! - `markets`: our normalised [`MarketInfo`] plus `tracked`, whenever it changes.

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use pm_api::{EventsQuery, Gamma, GammaEvent};
use pm_core::{ConditionId, MarketInfo, TokenId, now_ns};
use pm_store::{Partition, StoreHandle};
use serde_json::Value;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::clob::SharedClobManager;
use crate::config::{Config, League};
use crate::registry::SharedRegistry;

const FULL_SNAPSHOT: Duration = Duration::from_secs(6 * 3600);

/// A recorded event, for human-readable logging.
#[derive(Clone, PartialEq)]
struct EventSummary {
    part: Partition,
    title: String,
    start: Option<DateTime<Utc>>,
    markets: usize,
}

/// What one discovery poll found.
#[derive(Default)]
struct PollResult {
    /// Markets to record, by condition id.
    desired: HashMap<ConditionId, MarketInfo>,
    /// Events with at least one market to record, by event id.
    events: HashMap<String, EventSummary>,
    /// Leagues whose fetch failed; they keep their current markets.
    failed: HashSet<String>,
}

pub struct Discovery {
    cfg: Arc<Config>,
    gamma: Gamma,
    store: StoreHandle,
    reg: SharedRegistry,
    clob: Option<SharedClobManager>,
    /// event id → (state fingerprint, time of last raw snapshot)
    events: HashMap<String, (u64, Instant)>,
    last_info: HashMap<ConditionId, (MarketInfo, bool)>,
    ended_since: HashMap<String, Instant>,
    /// Events with at least one recorded market, as of the previous poll.
    recorded: HashMap<String, EventSummary>,
    polls: u64,
}

impl Discovery {
    pub fn new(cfg: Arc<Config>, store: StoreHandle, reg: SharedRegistry, clob: Option<SharedClobManager>) -> Self {
        Discovery {
            cfg,
            gamma: Gamma::default(),
            store,
            reg,
            clob,
            events: HashMap::new(),
            last_info: HashMap::new(),
            ended_since: HashMap::new(),
            recorded: HashMap::new(),
            polls: 0,
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) {
        let mut iv = tokio::time::interval(Duration::from_secs(self.cfg.discovery.interval_secs));
        iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = iv.tick() => {}
            }
            // A poll takes several seconds with many leagues; don't let it hold up shutdown.
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = self.poll_once() => {}
            }
        }
    }

    async fn poll_once(&mut self) {
        let t0 = Instant::now();
        let mut poll = PollResult::default();
        let cfg = self.cfg.clone();
        for (lg, fetched) in cfg.leagues.iter().zip(self.fetch_all().await) {
            match fetched {
                Ok(events) => {
                    for raw in events {
                        self.process_event(lg, raw, &mut poll).await;
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "discovery: fetching {} failed, keeping its current markets: {e:#}",
                        lg.key
                    );
                    poll.failed.insert(lg.key.clone());
                }
            }
        }

        let (add, remove) = self.update_tracked(&mut poll);
        for (id, e) in &self.recorded {
            if poll.failed.contains(&*e.part.league) && !poll.events.contains_key(id) {
                poll.events.insert(id.clone(), e.clone());
            }
        }
        self.log_changes(&poll.events, add.len(), remove.len(), t0.elapsed());
        self.recorded = poll.events;
        self.polls += 1;
        if let Some(clob) = &self.clob {
            clob.lock().unwrap().update(&add, &remove);
        }
        self.ended_since
            .retain(|_, t| t.elapsed() < Duration::from_secs(7 * 86400));
    }

    /// Records one event's metadata and adds the markets to record to `poll`.
    async fn process_event(&mut self, lg: &League, raw: Value, poll: &mut PollResult) {
        let Ok(ev) = serde_json::from_value::<GammaEvent>(raw.clone()) else {
            return;
        };
        if !self.in_scope(lg, &ev) {
            return;
        }
        let part = self.reg.read().unwrap().league(&lg.key);
        // An event can carry several tags; the first configured league claims it.
        if self
            .reg
            .read()
            .unwrap()
            .event_partition(&ev.slug)
            .is_some_and(|owner| owner != part)
        {
            return;
        }
        self.record_event(&part, &ev, &raw).await;
        self.reg.write().unwrap().note_event(&ev.slug, &part);

        let grace = Duration::from_secs(self.cfg.discovery.ended_grace_secs);
        let ended_long_ago = ev.ended
            && self
                .ended_since
                .entry(ev.id.clone())
                .or_insert_with(Instant::now)
                .elapsed()
                > grace;
        for m in ev.markets.iter().filter(|m| !m.condition_id.is_empty()) {
            let info = m.to_info(&lg.key, &ev);
            self.reg.write().unwrap().note_market(&info.condition_id, &part);
            let want = m.is_tradable() && lg.wants(m.sports_market_type.as_deref()) && !ended_long_ago;
            self.record_market(&part, &info, want).await;
            if want {
                let summary = poll.events.entry(ev.id.clone()).or_insert_with(|| EventSummary {
                    part: part.clone(),
                    title: ev.title.clone(),
                    start: ev.start(),
                    markets: 0,
                });
                summary.markets += 1;
                poll.desired.insert(info.condition_id.clone(), info);
            }
        }
    }

    /// `games_only` and `max_days_ahead` filters.
    fn in_scope(&self, lg: &League, ev: &GammaEvent) -> bool {
        if lg.games_only && !ev.is_game() {
            return false;
        }
        let days = self.cfg.discovery.max_days_ahead;
        match ev.start() {
            Some(t) if days > 0 => t <= Utc::now() + chrono::Duration::days(days as i64),
            _ => true,
        }
    }

    /// Replaces the tracked set with `poll.desired` (leagues whose fetch failed keep theirs) and
    /// returns the tokens to subscribe and unsubscribe.
    fn update_tracked(&self, poll: &mut PollResult) -> (Vec<TokenId>, Vec<TokenId>) {
        let mut reg = self.reg.write().unwrap();
        let mut remove = Vec::new();
        reg.tracked.retain(|c, m| {
            let keep = poll.desired.contains_key(c) || poll.failed.contains(&m.league);
            if !keep {
                remove.extend(m.token_ids.iter().cloned());
            }
            keep
        });
        let mut add = Vec::new();
        for (c, info) in poll.desired.drain() {
            if !reg.tracked.contains_key(&c) {
                add.extend(info.token_ids.iter().cloned());
            }
            reg.tracked.insert(c, info);
        }
        (add, remove)
    }

    /// Fetches every league's open events (tag and/or series, merged by event id), a few leagues
    /// at a time. Results are in config order.
    async fn fetch_all(&self) -> Vec<anyhow::Result<Vec<Value>>> {
        let lookback = chrono::Duration::hours(self.cfg.discovery.end_date_lookback_hours);
        let end_min = (Utc::now() - lookback).format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let sem = Arc::new(tokio::sync::Semaphore::new(4));
        let mut tasks = Vec::new();
        for lg in &self.cfg.leagues {
            let (gamma, sem, end_min) = (self.gamma.clone(), sem.clone(), end_min.clone());
            let queries: Vec<EventsQuery> = [
                lg.tag_id.map(|t| EventsQuery {
                    tag_id: Some(t),
                    ..Default::default()
                }),
                lg.series_id.map(|s| EventsQuery {
                    series_id: Some(s),
                    ..Default::default()
                }),
            ]
            .into_iter()
            .flatten()
            .map(|q| EventsQuery {
                closed: Some(false),
                end_date_min: Some(end_min.clone()),
                ..q
            })
            .collect();
            tasks.push(tokio::spawn(async move {
                let _permit = sem.acquire_owned().await?;
                let mut seen = HashSet::new();
                let mut out = Vec::new();
                for q in queries {
                    for v in gamma.events_raw(&q).await? {
                        let id = v.get("id").map(|i| i.to_string()).unwrap_or_default();
                        if seen.insert(id) {
                            out.push(v);
                        }
                    }
                }
                anyhow::Ok(out)
            }));
        }
        let mut out = Vec::new();
        for t in tasks {
            out.push(
                t.await
                    .unwrap_or_else(|e| Err(anyhow::anyhow!("fetch task failed: {e}"))),
            );
        }
        out
    }

    fn log_changes(
        &self,
        now: &HashMap<String, EventSummary>,
        added_tokens: usize,
        removed_tokens: usize,
        took: Duration,
    ) {
        let mut added: Vec<&EventSummary> = now
            .iter()
            .filter(|(id, _)| !self.recorded.contains_key(*id))
            .map(|(_, e)| e)
            .collect();
        let mut removed: Vec<&EventSummary> = self
            .recorded
            .iter()
            .filter(|(id, _)| !now.contains_key(*id))
            .map(|(_, e)| e)
            .collect();
        let mut resized: Vec<(&EventSummary, usize)> = now
            .iter()
            .filter_map(|(id, e)| {
                self.recorded
                    .get(id)
                    .filter(|o| o.markets != e.markets)
                    .map(|o| (e, o.markets))
            })
            .collect();
        let changed = !(added.is_empty() && removed.is_empty() && resized.is_empty());
        // By league, then scheduled matches by start time, then untimed events (futures, props).
        let key = |e: &EventSummary| (e.part.to_string(), e.start.is_none(), e.start, e.title.clone());
        added.sort_by_key(|e| key(e));
        removed.sort_by_key(|e| key(e));
        resized.sort_by_key(|(e, _)| key(e));

        for e in &added {
            tracing::info!(
                "  + {:<15} {}  ({}, {})",
                e.part.to_string(),
                e.title,
                when(e.start),
                markets(e.markets)
            );
        }
        for (e, before) in &resized {
            tracing::info!(
                "  ~ {:<15} {}  ({} -> {})",
                e.part.to_string(),
                e.title,
                before,
                markets(e.markets)
            );
        }
        for e in &removed {
            tracing::info!(
                "  - {:<15} {}  (finished, {} dropped)",
                e.part.to_string(),
                e.title,
                markets(e.markets)
            );
        }

        // Per-league totals.
        let mut by_league: std::collections::BTreeMap<String, (usize, usize)> = Default::default();
        for lg in &self.cfg.leagues {
            by_league
                .entry(self.reg.read().unwrap().league(&lg.key).to_string())
                .or_default();
        }
        for e in now.values() {
            let t = by_league.entry(e.part.to_string()).or_default();
            t.0 += e.markets;
            t.1 += 1;
        }
        let total: usize = by_league.values().map(|t| t.0).sum();
        let events: usize = by_league.values().map(|t| t.1).sum();
        let leagues: Vec<String> = by_league
            .iter()
            .map(|(p, (m, e))| format!("{p} {m} markets / {e} events"))
            .collect();
        let msg = format!(
            "discovery: recording {total} markets in {events} events [{}] | {} new events, {} finished, {} resized | tokens +{added_tokens} -{removed_tokens} | {:.1}s",
            leagues.join(", "),
            added.len(),
            removed.len(),
            resized.len(),
            took.as_secs_f64()
        );
        if changed || self.polls == 0 {
            tracing::info!("{msg}");
        } else {
            tracing::debug!("{msg}");
        }
    }

    async fn record_event(&mut self, part: &Partition, ev: &GammaEvent, raw: &Value) {
        let fp = fingerprint(ev);
        let now = Instant::now();
        let due = match self.events.get(&ev.id) {
            None => true,
            Some((old, at)) => *old != fp || now.duration_since(*at) > FULL_SNAPSHOT,
        };
        if due {
            self.events.insert(ev.id.clone(), (fp, now));
            self.store
                .write(part, "gamma_events", now_ns(), 0, &raw.to_string())
                .await;
        }
    }

    async fn record_market(&mut self, part: &Partition, info: &MarketInfo, tracked: bool) {
        let changed = match self.last_info.get(&info.condition_id) {
            None => true,
            Some((old, t)) => old != info || *t != tracked,
        };
        if changed {
            self.last_info
                .insert(info.condition_id.clone(), (info.clone(), tracked));
            let mut v = serde_json::to_value(info).unwrap_or_default();
            v["tracked"] = Value::Bool(tracked);
            self.store.write(part, "markets", now_ns(), 0, &v.to_string()).await;
        }
    }
}

/// Hash of the fields that matter for trading/research; volume and liquidity churn is ignored.
fn fingerprint(ev: &GammaEvent) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    (ev.live, ev.ended, ev.closed, &ev.score, &ev.period, &ev.start_time).hash(&mut h);
    for m in &ev.markets {
        (
            &m.condition_id,
            m.active,
            m.closed,
            m.accepting_orders,
            &m.game_start_time,
        )
            .hash(&mut h);
        // outcomePrices tracks live prices while open; it only matters once it is the resolution.
        if m.closed {
            m.outcome_prices.hash(&mut h);
        }
    }
    h.finish()
}

/// "starts Sat 14:00 (in 2h05m)" / "started Sat 14:00 (35m ago)", in local time.
fn when(start: Option<DateTime<Utc>>) -> String {
    let Some(t) = start else {
        return "no start time".into();
    };
    let at = t.with_timezone(&chrono::Local).format("%a %H:%M");
    let minutes = t.signed_duration_since(Utc::now()).num_minutes();
    if minutes >= 0 {
        format!("starts {at} (in {})", span(minutes))
    } else {
        format!("started {at} ({} ago)", span(-minutes))
    }
}

/// `2d3h`, `4h05m`, `12m`.
fn span(minutes: i64) -> String {
    match minutes {
        m if m >= 24 * 60 => format!("{}d{}h", m / 1440, m % 1440 / 60),
        m if m >= 60 => format!("{}h{:02}m", m / 60, m % 60),
        m => format!("{m}m"),
    }
}

fn markets(n: usize) -> String {
    if n == 1 {
        "1 market".into()
    } else {
        format!("{n} markets")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(json: serde_json::Value) -> GammaEvent {
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn fingerprint_tracks_state_not_prices_of_open_markets() {
        let base = serde_json::json!({"id": "1", "live": true, "score": "1-0", "volume": 5,
            "markets": [{"conditionId": "0xm", "active": true, "closed": false, "outcomePrices": "[\"0.4\",\"0.6\"]"}]});
        let fp = |f: &dyn Fn(&mut serde_json::Value)| {
            let mut v = base.clone();
            f(&mut v);
            fingerprint(&event(v))
        };
        let same = fp(&|_| {});
        assert_eq!(fp(&|v| v["volume"] = 999.into()), same, "volume churn is ignored");
        assert_eq!(
            fp(&|v| v["markets"][0]["outcomePrices"] = "[\"0.5\",\"0.5\"]".into()),
            same,
            "live prices too"
        );
        assert_ne!(fp(&|v| v["score"] = "2-0".into()), same);
        assert_ne!(fp(&|v| v["live"] = false.into()), same);
        assert_ne!(fp(&|v| v["markets"][0]["closed"] = true.into()), same);
        // Once closed, the final prices are the resolution and do count.
        let closed = |p: &str| {
            fp(&|v: &mut serde_json::Value| {
                v["markets"][0]["closed"] = true.into();
                v["markets"][0]["outcomePrices"] = p.into();
            })
        };
        assert_ne!(closed("[\"1\",\"0\"]"), closed("[\"0\",\"1\"]"));
    }

    #[test]
    fn human_spans() {
        assert_eq!(span(0), "0m");
        assert_eq!(span(59), "59m");
        assert_eq!(span(60), "1h00m");
        assert_eq!(span(125), "2h05m");
        assert_eq!(span(24 * 60 + 61), "1d1h");
        assert_eq!(markets(1), "1 market");
        assert_eq!(markets(0), "0 markets");
        assert_eq!(when(None), "no start time");
        assert!(when(Some(Utc::now() + chrono::Duration::minutes(90))).contains("(in 1h"));
        assert!(when(Some(Utc::now() - chrono::Duration::minutes(30))).ends_with("ago)"));
    }
}
