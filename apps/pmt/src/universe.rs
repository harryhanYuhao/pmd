//! Polls Gamma for the configured leagues' upcoming and live events and sends the engine every
//! binary market found. Events of held positions that no longer appear in the listings (closed
//! events) are looked up by slug, so their resolution is still seen.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use pm_api::{EventsQuery, Gamma, GammaEvent};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::{League, UniverseSection};
use crate::market::Market;

/// One poll's result.
pub struct Update {
    pub markets: Vec<Market>,
    /// Leagues whose fetch failed; their markets are kept as they were.
    pub failed: HashSet<String>,
}

/// Event slug → league key of events with open positions, kept up to date by the engine.
pub type Held = Arc<Mutex<HashMap<String, String>>>;

pub async fn run(
    cfg: UniverseSection,
    leagues: Vec<League>,
    held: Held,
    tx: mpsc::Sender<Update>,
    cancel: CancellationToken,
) {
    let gamma = Gamma::default();
    let mut iv = tokio::time::interval(Duration::from_secs(cfg.interval_secs.max(5)));
    iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = iv.tick() => {}
        }
        let update = tokio::select! {
            _ = cancel.cancelled() => return,
            u = poll(&gamma, &cfg, &leagues, &held) => u,
        };
        if tx.send(update).await.is_err() {
            return;
        }
    }
}

async fn poll(gamma: &Gamma, cfg: &UniverseSection, leagues: &[League], held: &Held) -> Update {
    let end_min = (Utc::now() - chrono::Duration::hours(cfg.end_date_lookback_hours))
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();
    let horizon = Utc::now() + chrono::Duration::hours(cfg.max_hours_ahead as i64);
    let mut out = Update {
        markets: Vec::new(),
        failed: HashSet::new(),
    };
    let mut seen = Seen::default();
    for lg in leagues {
        match gamma.league_events(lg.tag_id, lg.series_id, &end_min).await {
            Ok(events) => {
                for raw in events {
                    let Ok(ev) = serde_json::from_value::<GammaEvent>(raw) else {
                        continue;
                    };
                    // Matches only, up to the horizon (futures and far fixtures are not traded).
                    if ev.start().is_some_and(|t| t <= horizon) {
                        seen.add(&lg.key, &ev, &mut out);
                    }
                }
            }
            Err(e) => {
                tracing::warn!("universe: fetching {} failed, keeping its markets: {e:#}", lg.key);
                out.failed.insert(lg.key.clone());
            }
        }
    }
    let missing: Vec<(String, String)> = held
        .lock()
        .unwrap()
        .iter()
        .filter(|(slug, _)| !seen.events.contains(*slug))
        .map(|(s, l)| (s.clone(), l.clone()))
        .collect();
    for (slug, league) in missing {
        let q = EventsQuery {
            slug: Some(slug.clone()),
            ..Default::default()
        };
        match gamma.events(&q).await {
            Ok(events) => {
                for ev in events {
                    seen.add(&league, &ev, &mut out);
                }
            }
            Err(e) => tracing::warn!("universe: looking up held event {slug} failed: {e:#}"),
        }
    }
    out
}

/// Events and markets already collected in one poll (an event can be in several leagues).
#[derive(Default)]
struct Seen {
    events: HashSet<String>,
    markets: HashSet<pm_core::ConditionId>,
}

impl Seen {
    fn add(&mut self, league: &str, ev: &GammaEvent, out: &mut Update) {
        self.events.insert(ev.slug.clone());
        for m in &ev.markets {
            if let Some(m) = Market::from_gamma(league, ev, m)
                && self.markets.insert(m.info.condition_id.clone())
            {
                out.markets.push(m);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markets_are_collected_once_per_poll() {
        let ev: GammaEvent = serde_json::from_value(serde_json::json!({
            "id": "1", "slug": "cs2-a-b", "title": "A vs B", "startTime": "2026-10-17T14:00:00Z",
            "markets": [
                {"conditionId": "0xm", "outcomes": "[\"A\", \"B\"]", "clobTokenIds": "[\"1\", \"2\"]",
                 "sportsMarketType": "moneyline", "active": true},
                {"conditionId": "0xn", "outcomes": "[\"A\", \"B\", \"C\"]", "clobTokenIds": "[\"3\", \"4\", \"5\"]"}
            ]
        }))
        .unwrap();
        let mut out = Update {
            markets: vec![],
            failed: HashSet::new(),
        };
        let mut seen = Seen::default();
        seen.add("cs2", &ev, &mut out);
        seen.add("other", &ev, &mut out); // the same event under a second league's tag
        assert_eq!(out.markets.len(), 1, "binary markets only, once");
        assert_eq!(out.markets[0].info.league, "cs2", "the first league keeps it");
        assert!(seen.events.contains("cs2-a-b"));
    }
}
