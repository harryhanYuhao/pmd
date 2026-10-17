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

use pm_api::{EventsQuery, Gamma, GammaEvent};
use pm_core::{ConditionId, MarketInfo, TokenId, now_ns};
use pm_store::{Partition, StoreHandle};
use serde_json::Value;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::clob::SharedClobManager;
use crate::config::Config;
use crate::registry::SharedRegistry;

const FULL_SNAPSHOT: Duration = Duration::from_secs(6 * 3600);

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
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) {
        let mut iv = tokio::time::interval(Duration::from_secs(self.cfg.discovery.interval_secs));
        iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = iv.tick() => self.poll_once().await,
            }
        }
    }

    async fn poll_once(&mut self) {
        let t0 = Instant::now();
        let end_min = chrono::Utc::now() - chrono::Duration::hours(self.cfg.discovery.end_date_lookback_hours);
        let grace = Duration::from_secs(self.cfg.discovery.ended_grace_secs);
        let mut desired: HashMap<ConditionId, MarketInfo> = HashMap::new();
        let mut failed: HashSet<String> = HashSet::new();
        let mut n_events = 0;

        let cfg = self.cfg.clone();
        for lg in &cfg.leagues {
            let q = EventsQuery {
                tag_id: Some(lg.tag_id),
                closed: Some(false),
                end_date_min: Some(end_min.format("%Y-%m-%dT%H:%M:%SZ").to_string()),
                slug: None,
            };
            let raw = match self.gamma.events_raw(&q).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(league = %lg.key, "discovery failed, keeping current subscriptions: {e:#}");
                    failed.insert(lg.key.clone());
                    continue;
                }
            };
            let part = self.reg.read().unwrap().league(&lg.key);
            for v in raw {
                let Ok(ev) = serde_json::from_value::<GammaEvent>(v.clone()) else { continue };
                // An event can carry several tags; the first configured league claims it.
                if let Some(owner) = self.reg.read().unwrap().event_partition(&ev.slug)
                    && owner != part
                {
                    continue;
                }
                n_events += 1;
                self.record_event(&part, &ev, &v).await;
                self.reg.write().unwrap().note_event(&ev.slug, &part);

                let ended_long_ago = if ev.ended {
                    self.ended_since.entry(ev.id.clone()).or_insert_with(Instant::now).elapsed() > grace
                } else {
                    false
                };
                for m in &ev.markets {
                    if m.condition_id.is_empty() {
                        continue;
                    }
                    let info = m.to_info(&lg.key, &ev);
                    self.reg.write().unwrap().note_market(&info.condition_id, &part);
                    let want = m.is_tradable() && lg.wants(m.sports_market_type.as_deref()) && !ended_long_ago;
                    self.record_market(&part, &info, want).await;
                    if want {
                        desired.insert(info.condition_id.clone(), info);
                    }
                }
            }
        }

        // Diff against what is tracked. Leagues whose fetch failed keep their markets.
        let (add, remove) = {
            let mut reg = self.reg.write().unwrap();
            let mut remove: Vec<TokenId> = Vec::new();
            reg.tracked.retain(|c, m| {
                let keep = desired.contains_key(c) || failed.contains(&m.league);
                if !keep {
                    remove.extend(m.token_ids.iter().cloned());
                }
                keep
            });
            let mut add: Vec<TokenId> = Vec::new();
            for (c, info) in desired {
                if !reg.tracked.contains_key(&c) {
                    add.extend(info.token_ids.iter().cloned());
                }
                reg.tracked.insert(c, info);
            }
            (add, remove)
        };
        let n_tracked = self.reg.read().unwrap().tracked.len();
        tracing::info!(
            events = n_events,
            markets = n_tracked,
            added_tokens = add.len(),
            removed_tokens = remove.len(),
            took_ms = t0.elapsed().as_millis() as u64,
            "discovery"
        );
        if let Some(clob) = &self.clob {
            clob.lock().unwrap().update(&add, &remove);
        }
        self.ended_since.retain(|_, t| t.elapsed() < Duration::from_secs(7 * 86400));
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
            self.store.write(part, "gamma_events", now_ns(), 0, &raw.to_string()).await;
        }
    }

    async fn record_market(&mut self, part: &Partition, info: &MarketInfo, tracked: bool) {
        let changed = match self.last_info.get(&info.condition_id) {
            None => true,
            Some((old, t)) => old != info || *t != tracked,
        };
        if changed {
            self.last_info.insert(info.condition_id.clone(), (info.clone(), tracked));
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
        (&m.condition_id, m.active, m.closed, m.accepting_orders, &m.game_start_time).hash(&mut h);
        // outcomePrices tracks live prices while open; it only matters once it is the resolution.
        if m.closed {
            m.outcome_prices.hash(&mut h);
        }
    }
    h.finish()
}
