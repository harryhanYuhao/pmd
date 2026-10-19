//! Trader configuration (TOML). See `config/pmt.toml` for a documented example.

use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;

use crate::risk::RiskConfig;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Root of the journal (decisions, orders, fills, settlements). Relative paths are resolved
    /// against the config file's directory.
    pub journal_dir: PathBuf,
    #[serde(default)]
    pub mode: Mode,
    #[serde(default)]
    pub run: RunSection,
    #[serde(default)]
    pub log: LogSection,
    #[serde(default)]
    pub universe: UniverseSection,
    #[serde(default)]
    pub history: HistorySection,
    #[serde(default)]
    pub risk: RiskConfig,
    pub leagues: Vec<League>,
    #[serde(default, rename = "strategy")]
    pub strategies: Vec<StrategyEntry>,
}

/// Where orders go.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// Simulated fills against the live order books; nothing is sent to Polymarket.
    #[default]
    Paper,
    /// Real orders (needs `pm-exec`: API keys and order signing; not implemented yet).
    Live,
}

impl Mode {
    pub fn name(self) -> &'static str {
        match self {
            Mode::Paper => "paper",
            Mode::Live => "live",
        }
    }
}

/// How a run ends.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RunSection {
    /// Stop automatically after this long (`"3d"`, `"12h"`, `"90m"`, seconds). Unset = run until
    /// stopped. At the stop time no new trades start; the run ends once nothing is in progress.
    pub run_for: Option<HumanDuration>,
    /// After the stop time (or the first Ctrl-C), wait at most this long for trades in progress
    /// before cancelling working orders and exiting anyway.
    pub drain_timeout: HumanDuration,
}

impl Default for RunSection {
    fn default() -> Self {
        RunSection {
            run_for: None,
            drain_timeout: HumanDuration(Duration::from_secs(4 * 3600)),
        }
    }
}

/// A duration written as `"3d"`, `"12h"`, `"90m"`, `"30s"`, combinations like `"1d12h"`, or a
/// number of seconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HumanDuration(pub Duration);

impl FromStr for HumanDuration {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let t = s.trim();
        if let Ok(secs) = t.parse::<u64>() {
            return Ok(HumanDuration(Duration::from_secs(secs)));
        }
        let mut total = 0u64;
        let mut num = String::new();
        for ch in t.chars() {
            if ch.is_ascii_digit() {
                num.push(ch);
                continue;
            }
            let unit = match ch {
                'd' => 86_400,
                'h' => 3_600,
                'm' => 60,
                's' => 1,
                _ => return Err(format!("invalid duration {s:?}: unknown unit {ch:?}")),
            };
            let n: u64 = num.parse().map_err(|_| format!("invalid duration {s:?}"))?;
            total = n
                .checked_mul(unit)
                .and_then(|x| total.checked_add(x))
                .ok_or_else(|| format!("duration {s:?} is too long"))?;
            num.clear();
        }
        if !num.is_empty() || t.is_empty() {
            return Err(format!("invalid duration {s:?}: use e.g. 3d, 12h, 90m, 1d12h"));
        }
        Ok(HumanDuration(Duration::from_secs(total)))
    }
}

impl fmt::Display for HumanDuration {
    /// `1d12h`, `2h30m`, `45s`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = self.0.as_secs();
        if s == 0 {
            return f.write_str("0s");
        }
        for (unit, n) in [
            ("d", s / 86_400),
            ("h", s / 3_600 % 24),
            ("m", s / 60 % 60),
            ("s", s % 60),
        ] {
            if n > 0 {
                write!(f, "{n}{unit}")?;
            }
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for HumanDuration {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Secs(u64),
            Text(String),
        }
        match Raw::deserialize(d)? {
            Raw::Secs(n) => Ok(HumanDuration(Duration::from_secs(n))),
            Raw::Text(t) => t.parse().map_err(serde::de::Error::custom),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LogSection {
    /// `error`, `warn`, `info`, `debug` (or any `RUST_LOG` filter); `RUST_LOG` overrides it.
    pub level: String,
    /// Also write the log under this directory (relative to the config file), one file per local
    /// day: `<dir>/YYYY/MM/DD/trade.log`. Empty = stdout only.
    pub dir: Option<PathBuf>,
    /// Status report period.
    pub status_secs: u64,
}

impl Default for LogSection {
    fn default() -> Self {
        LogSection {
            level: "info".into(),
            dir: None,
            status_secs: 300,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct UniverseSection {
    /// Gamma poll period.
    pub interval_secs: u64,
    /// Only events starting within this many hours. Events without a start time are skipped.
    pub max_hours_ahead: u64,
    /// Query events whose `endDate` is later than now minus this.
    pub end_date_lookback_hours: i64,
    /// Market-channel connections hold at most this many tokens each.
    pub max_assets_per_conn: usize,
}

impl Default for UniverseSection {
    fn default() -> Self {
        UniverseSection {
            interval_secs: 60,
            max_hours_ahead: 24,
            end_date_lookback_hours: 24,
            max_assets_per_conn: 250,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HistorySection {
    /// Most trades loaded per market from the Data API; a market with more is marked truncated.
    pub max_trades: usize,
}

impl Default for HistorySection {
    fn default() -> Self {
        HistorySection { max_trades: 3000 }
    }
}

/// A league to watch. Ids come from `https://gamma-api.polymarket.com/sports`.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct League {
    /// Our key (`cs2`, `dota2`, `lol`); strategies name leagues by it.
    pub key: String,
    #[serde(default)]
    pub tag_id: Option<u64>,
    #[serde(default)]
    pub series_id: Option<u64>,
}

/// One `[[strategy]]` table: `kind` picks the algorithm, the other keys are its parameters.
#[derive(Clone, Debug, Deserialize)]
pub struct StrategyEntry {
    pub kind: String,
    /// Instance name used in logs and the journal (default: `kind`). Lets one algorithm run
    /// several times with different parameters.
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default = "yes")]
    pub enabled: bool,
    /// Start no new trades (orders in markets the strategy is not in yet) when less than this
    /// remains before the scheduled stop. For strategies that trade in and out during a game.
    #[serde(default)]
    pub entry_cutoff: Option<HumanDuration>,
    #[serde(flatten)]
    pub params: toml::Table,
}

fn yes() -> bool {
    true
}

impl StrategyEntry {
    pub fn id(&self) -> &str {
        self.id.as_deref().unwrap_or(&self.kind)
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Config> {
        let text = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let mut cfg = Config::parse(&text).with_context(|| format!("parsing {}", path.display()))?;
        let base = path.parent().unwrap_or(Path::new("."));
        if cfg.journal_dir.is_relative() {
            cfg.journal_dir = base.join(&cfg.journal_dir);
        }
        cfg.log.dir = cfg
            .log
            .dir
            .take()
            .filter(|f| !f.as_os_str().is_empty())
            .map(|f| base.join(f));
        Ok(cfg)
    }

    /// Parses and validates; paths are left as written.
    pub fn parse(text: &str) -> Result<Config> {
        let cfg: Config = toml::from_str(text)?;
        if cfg.mode == Mode::Live {
            bail!("mode = \"live\" is not implemented yet (no order signing); use mode = \"paper\"");
        }
        ensure!(!cfg.leagues.is_empty(), "no leagues configured");
        let mut keys = HashSet::new();
        for l in &cfg.leagues {
            ensure!(
                l.tag_id.is_some() || l.series_id.is_some(),
                "league {}: set tag_id and/or series_id",
                l.key
            );
            ensure!(keys.insert(l.key.as_str()), "league {} is configured twice", l.key);
        }
        if let Some(d) = cfg.run.run_for {
            ensure!(
                !d.0.is_zero(),
                "[run] run_for must be positive (leave it out to run until stopped)"
            );
        }
        let mut ids = HashSet::new();
        for s in &cfg.strategies {
            ensure!(ids.insert(s.id()), "strategy id {} is used twice", s.id());
        }
        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = "journal_dir = \"j\"\n[[leagues]]\nkey = \"cs2\"\ntag_id = 1\n";

    #[test]
    fn strategy_tables_keep_their_parameters() {
        let cfg = Config::parse(&format!(
            "{BASE}[[strategy]]\nkind = \"a\"\nstake_usdc = 2.5\n[[strategy]]\nkind = \"a\"\nid = \"a2\"\nenabled = false\n"
        ))
        .unwrap();
        assert_eq!(cfg.mode, Mode::Paper);
        assert_eq!(cfg.strategies.len(), 2);
        assert_eq!(cfg.strategies[0].id(), "a");
        assert_eq!(cfg.strategies[0].params["stake_usdc"].as_float(), Some(2.5));
        assert!(!cfg.strategies[0].params.contains_key("kind"));
        assert_eq!(cfg.strategies[1].id(), "a2");
        assert!(!cfg.strategies[1].enabled);
    }

    #[test]
    fn mistakes_are_rejected_with_a_reason() {
        let err = |text: &str| format!("{:#}", Config::parse(text).unwrap_err());
        assert!(err(&format!("mode = \"live\"\n{BASE}")).contains("not implemented"));
        assert!(
            err(&format!(
                "{BASE}[[strategy]]\nkind = \"a\"\n[[strategy]]\nkind = \"a\"\n"
            ))
            .contains("used twice")
        );
        assert!(err(&format!("{BASE}[risk]\nmax_order = 1\n")).contains("unknown field"));
        assert!(err("journal_dir = \"j\"\nleagues = []\n").contains("no leagues"));
        assert!(err(&format!("{BASE}[run]\nrun_for = \"3x\"\n")).contains("unknown unit"));
        assert!(err(&format!("{BASE}[run]\nrun_for = 0\n")).contains("positive"));
    }

    #[test]
    fn durations() {
        let d = |s: &str| s.parse::<HumanDuration>().map(|d| d.0.as_secs());
        assert_eq!(d("3d"), Ok(3 * 86_400));
        assert_eq!(d("1d12h"), Ok(36 * 3_600));
        assert_eq!(d("90m"), Ok(5_400));
        assert_eq!(d("100"), Ok(100));
        assert!(
            d("99999999999999999999d").is_err() && d("999999999999999999d").is_err(),
            "overflow"
        );
        assert!(d("").is_err() && d("5").is_ok() && d("5x").is_err() && d("h").is_err() && d("3h5").is_err());
        assert_eq!(
            HumanDuration(std::time::Duration::from_secs(36 * 3_600 + 90)).to_string(),
            "1d12h1m30s"
        );
        let cfg = Config::parse(&format!(
            "{BASE}[run]\nrun_for = \"3d\"\n[[strategy]]\nkind = \"a\"\nentry_cutoff = \"2h\"\n"
        ))
        .unwrap();
        assert_eq!(cfg.run.run_for.unwrap().0.as_secs(), 3 * 86_400);
        assert_eq!(cfg.run.drain_timeout.0.as_secs(), 4 * 3_600, "default");
        assert_eq!(cfg.strategies[0].entry_cutoff.unwrap().0.as_secs(), 7_200);
        assert!(!cfg.strategies[0].params.contains_key("entry_cutoff"));
    }
}
