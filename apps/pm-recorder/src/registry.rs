//! Shared view of what is being recorded: tracked markets and id → partition routing.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use pm_core::{ConditionId, MarketInfo};
use pm_store::Partition;

use crate::config::Config;

/// Feed-level records (connect/disconnect markers, unroutable messages).
pub const SYSTEM: &str = "_system";
/// Data from leagues that are not configured (other sports' scores, untracked trades).
pub const OTHER: &str = "other";

pub struct Registry {
    /// Markets currently subscribed on the CLOB feed.
    pub tracked: HashMap<ConditionId, MarketInfo>,
    /// Configured league key → partition.
    leagues: HashMap<String, Partition>,
    /// Sports-feed `leagueAbbreviation` → partition.
    score_leagues: HashMap<String, Partition>,
    /// Every market / event ever discovered → partition (never pruned; small).
    markets: HashMap<ConditionId, Partition>,
    events: HashMap<String, Partition>,
}

pub type SharedRegistry = Arc<RwLock<Registry>>;

impl Registry {
    pub fn new(cfg: &Config) -> Self {
        let mut leagues = HashMap::new();
        let mut score_leagues = HashMap::new();
        for l in &cfg.leagues {
            let p = Partition::new(&l.category, &l.key);
            score_leagues.insert(l.score_league.as_deref().unwrap_or(&l.key).to_ascii_lowercase(), p.clone());
            leagues.insert(l.key.clone(), p);
        }
        Registry { tracked: HashMap::new(), leagues, score_leagues, markets: HashMap::new(), events: HashMap::new() }
    }

    pub fn league(&self, key: &str) -> Partition {
        self.leagues.get(key).cloned().unwrap_or_else(|| Partition::new(OTHER, key))
    }

    /// Partition for a sports-feed league; unconfigured leagues go under `other/`.
    pub fn score_league(&self, abbreviation: &str) -> Partition {
        let a = abbreviation.to_ascii_lowercase();
        self.score_leagues.get(&a).cloned().unwrap_or_else(|| Partition::new(OTHER, &a))
    }

    pub fn note_event(&mut self, event_slug: &str, part: &Partition) {
        if !self.events.contains_key(event_slug) {
            self.events.insert(event_slug.to_string(), part.clone());
        }
    }

    pub fn note_market(&mut self, cond: &ConditionId, part: &Partition) {
        self.markets.entry(cond.clone()).or_insert_with(|| part.clone());
    }

    pub fn market_partition(&self, cond: &str) -> Option<Partition> {
        self.markets.get(cond).cloned()
    }

    pub fn event_partition(&self, event_slug: &str) -> Option<Partition> {
        self.events.get(event_slug).cloned()
    }
}

pub fn system(feed: &str) -> Partition {
    Partition::new(SYSTEM, feed)
}
