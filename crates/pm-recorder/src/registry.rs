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
            score_leagues.insert(
                l.score_league.as_deref().unwrap_or(&l.key).to_ascii_lowercase(),
                p.clone(),
            );
            leagues.insert(l.key.clone(), p);
        }
        Registry {
            tracked: HashMap::new(),
            leagues,
            score_leagues,
            markets: HashMap::new(),
            events: HashMap::new(),
        }
    }

    pub fn league(&self, key: &str) -> Partition {
        self.leagues
            .get(key)
            .cloned()
            .unwrap_or_else(|| Partition::new(OTHER, key))
    }

    /// Partition for a sports-feed league; unconfigured leagues go under `other/`.
    pub fn score_league(&self, abbreviation: &str) -> Partition {
        let a = abbreviation.to_ascii_lowercase();
        self.score_leagues
            .get(&a)
            .cloned()
            .unwrap_or_else(|| Partition::new(OTHER, &a))
    }

    /// Remembers which partition an event belongs to (the first league to see it keeps it).
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

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> Registry {
        let cfg: Config = toml::from_str(
            r#"
            data_dir = "x"
            [[leagues]]
            category = "esports"
            key = "cs2"
            tag_id = 1
            [[leagues]]
            category = "esports"
            key = "r6seige"
            score_league = "R6Siege"
            tag_id = 2
            "#,
        )
        .unwrap();
        Registry::new(&cfg)
    }

    #[test]
    fn routes_leagues_and_score_leagues() {
        let r = registry();
        assert_eq!(r.league("cs2").to_string(), "esports/cs2");
        assert_eq!(r.league("nba").to_string(), "other/nba", "unconfigured league");
        // The sports feed's abbreviation, case-insensitively, through score_league aliases.
        assert_eq!(r.score_league("CS2").to_string(), "esports/cs2");
        assert_eq!(r.score_league("r6siege").to_string(), "esports/r6seige");
        assert_eq!(
            r.score_league("r6seige").to_string(),
            "other/r6seige",
            "the key is replaced by the alias"
        );
        assert_eq!(r.score_league("Wta Challenger").to_string(), "other/wta_challenger");
    }

    #[test]
    fn the_first_league_to_see_an_id_keeps_it() {
        let mut r = registry();
        let (cs2, r6) = (r.league("cs2"), r.league("r6seige"));
        r.note_event("ev", &cs2);
        r.note_event("ev", &r6);
        assert_eq!(r.event_partition("ev"), Some(cs2.clone()));
        let c = ConditionId::from("0xm");
        r.note_market(&c, &r6);
        r.note_market(&c, &cs2);
        assert_eq!(r.market_partition("0xm"), Some(r6));
        assert_eq!(r.market_partition("0xother"), None);
        assert_eq!(system("clob").to_string(), "_system/clob");
    }
}
