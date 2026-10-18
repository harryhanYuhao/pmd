//! Recorder configuration (TOML). See `config/recorder.toml` for a documented example.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Root of the recorded data. Relative paths are resolved against the config file's directory.
    pub data_dir: PathBuf,
    #[serde(default)]
    pub log: LogSection,
    #[serde(default)]
    pub store: StoreSection,
    #[serde(default)]
    pub discovery: DiscoverySection,
    pub leagues: Vec<League>,
    #[serde(default)]
    pub clob: ClobSection,
    #[serde(default)]
    pub activity: ActivitySection,
    #[serde(default)]
    pub sports: SportsSection,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LogSection {
    /// `error`, `warn`, `info`, `debug` (or any `RUST_LOG` filter); `RUST_LOG` overrides it.
    pub level: String,
    /// Also append the log to this file (relative to the config file). Empty = stdout only.
    pub file: Option<PathBuf>,
    /// Status report period.
    pub status_secs: u64,
}

impl Default for LogSection {
    fn default() -> Self {
        LogSection {
            level: "info".into(),
            file: None,
            status_secs: 60,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StoreSection {
    pub zstd_level: i32,
    pub flush_secs: u64,
}

impl Default for StoreSection {
    fn default() -> Self {
        StoreSection {
            zstd_level: 6,
            flush_secs: 5,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DiscoverySection {
    pub interval_secs: u64,
    /// Query events whose `endDate` is later than now minus this (Gamma's endDate is sometimes
    /// earlier than the actual game end).
    pub end_date_lookback_hours: i64,
    /// Keep recording an event's markets this long after Gamma first reports it `ended`.
    pub ended_grace_secs: u64,
    /// Only record events starting within this many days (0 = no limit). Events without a start
    /// time (futures) are not affected. Fixtures are often listed weeks ahead.
    pub max_days_ahead: u64,
}

impl Default for DiscoverySection {
    fn default() -> Self {
        DiscoverySection {
            interval_secs: 60,
            end_date_lookback_hours: 24,
            ended_grace_secs: 1800,
            max_days_ahead: 0,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct League {
    /// First directory level (`esports`, `sports`, …).
    pub category: String,
    /// Second directory level (`cs2`, `dota2`, `lol`, …).
    pub key: String,
    /// The sports feed's `leagueAbbreviation` for this league, if it differs from `key`.
    #[serde(default)]
    pub score_league: Option<String>,
    /// Gamma tag id (the `/sports` `primaryTagId`, not a tag slug): every event of the league,
    /// including futures and props.
    #[serde(default)]
    pub tag_id: Option<u64>,
    /// Gamma series id (the `/sports` `series`): the league's match events only. For some leagues
    /// (Serie A, Champions League, tennis) matches are only found this way. Give `tag_id`,
    /// `series_id` or both (results are merged).
    #[serde(default)]
    pub series_id: Option<u64>,
    /// Skip events without a start time or game id (futures, season props).
    #[serde(default)]
    pub games_only: bool,
    /// Only these `sportsMarketType`s; empty = all.
    #[serde(default)]
    pub include_types: Vec<String>,
    #[serde(default)]
    pub exclude_types: Vec<String>,
    /// Record markets without a `sportsMarketType` (outrights, props without a type).
    #[serde(default = "yes")]
    pub include_untyped: bool,
}

fn yes() -> bool {
    true
}

impl League {
    pub fn wants(&self, market_type: Option<&str>) -> bool {
        match market_type {
            None => self.include_untyped,
            Some(t) => {
                (self.include_types.is_empty() || self.include_types.iter().any(|x| x == t))
                    && !self.exclude_types.iter().any(|x| x == t)
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClobSection {
    pub enabled: bool,
    pub max_assets_per_conn: usize,
    /// Also receive `best_bid_ask`, `new_market` and `market_resolved` events.
    pub custom_features: bool,
    /// Write a top-of-book snapshot of every changed book this often (0 = off).
    pub book_snapshot_secs: u64,
    pub book_snapshot_levels: usize,
    /// Keep the per-update `hash` fields of `price_change` events (3x larger files).
    pub keep_hashes: bool,
}

impl Default for ClobSection {
    fn default() -> Self {
        ClobSection {
            enabled: true,
            max_assets_per_conn: 250,
            custom_features: true,
            book_snapshot_secs: 60,
            book_snapshot_levels: 50,
            keep_hashes: false,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ActivitySection {
    pub enabled: bool,
    /// Keep only trades in events discovered for the configured leagues; otherwise everything on
    /// Polymarket is recorded (unknown events under partition `other`).
    pub only_tracked: bool,
}

impl Default for ActivitySection {
    fn default() -> Self {
        ActivitySection {
            enabled: true,
            only_tracked: true,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SportsSection {
    pub enabled: bool,
    /// `leagueAbbreviation`s to keep; empty = all.
    pub leagues: Vec<String>,
}

impl Default for SportsSection {
    fn default() -> Self {
        SportsSection {
            enabled: true,
            leagues: vec![],
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Config> {
        let text = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let mut cfg: Config = toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        let base = path.parent().unwrap_or(Path::new("."));
        if cfg.data_dir.is_relative() {
            cfg.data_dir = base.join(&cfg.data_dir);
        }
        cfg.log.file = cfg
            .log
            .file
            .take()
            .filter(|f| !f.as_os_str().is_empty())
            .map(|f| base.join(f));
        anyhow::ensure!(!cfg.leagues.is_empty(), "no leagues configured");
        let mut keys = std::collections::HashSet::new();
        for l in &cfg.leagues {
            anyhow::ensure!(
                l.tag_id.is_some() || l.series_id.is_some(),
                "league {}: set tag_id and/or series_id",
                l.key
            );
            anyhow::ensure!(keys.insert(l.key.as_str()), "league {} is configured twice", l.key);
        }
        Ok(cfg)
    }
}
