//! `pm audit`: one read-only pass over the recorded files (in parallel), producing a [`Report`]
//! of what was recorded, which runs and connections had gaps, and which files are damaged.

mod report;

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use anyhow::Result;
use pm_store::Partition;
use pm_store::read::{FileInfo, Filter, ReadReport, read_file, scan};
use rayon::prelude::*;
use serde::Deserialize;
use serde_json::value::RawValue;

pub use report::*;

/// Streams written on change or periodically rather than as a feed: they say nothing about
/// whether the feeds were up, so gap detection ignores them.
const META_STREAMS: [&str; 3] = ["markets", "gamma_events", "sessions"];

/// Records more than this far outside their file's hour are counted as misplaced.
const HOUR_SLACK_NS: i64 = 60_000_000_000;

pub struct Options {
    pub root: PathBuf,
    pub filter: Filter,
    /// Silences at least this long are reported as gaps.
    pub gap: Duration,
    /// Rows in the "longest …" lists.
    pub top: usize,
}

pub fn run(opts: &Options) -> Result<Report> {
    let mut found = scan(&opts.root, &opts.filter)?;
    found.files.retain(|f| f.stream != "sessions");
    // Connection markers are always read (for the selected dates), whatever else is filtered.
    let sessions = scan(
        &opts.root,
        &Filter {
            from: opts.filter.from,
            to: opts.filter.to,
            streams: vec!["sessions".into()],
            ..Default::default()
        },
    )?;
    let files: Vec<FileInfo> = found.files.into_iter().chain(sessions.files).collect();
    tracing::info!("audit: reading {} files under {}", files.len(), opts.root.display());

    let gap_ns = opts.gap.as_nanos() as i64;
    let acc = files
        .into_par_iter()
        .fold(Acc::default, |mut acc, f| {
            acc.add(scan_file(f, gap_ns));
            acc
        })
        .reduce(Acc::default, Acc::merge);
    Ok(report::build(acc, found.unrecognised, opts))
}

/// What one file contributed.
struct FileSummary {
    info: FileInfo,
    rep: ReadReport,
    /// Could not be opened at all.
    error: Option<String>,
    /// Modified in the last two minutes: probably still being written.
    open: bool,
    first: Option<i64>,
    last: Option<i64>,
    out_of_hour: u64,
    /// Silences of at least the gap threshold inside the file.
    gaps: Vec<(i64, i64)>,
}

#[derive(Default)]
struct Acc {
    files: Vec<FileSummary>,
    content: HashMap<Partition, Content>,
    sessions: Vec<SessionEvent>,
}

impl Acc {
    fn add(&mut self, (f, content, sessions): (FileSummary, Content, Vec<SessionEvent>)) {
        self.content.entry(f.info.part.clone()).or_default().merge(content);
        self.sessions.extend(sessions);
        self.files.push(f);
    }

    fn merge(mut self, other: Acc) -> Acc {
        self.files.extend(other.files);
        self.sessions.extend(other.sessions);
        for (k, c) in other.content {
            self.content.entry(k).or_default().merge(c);
        }
        self
    }
}

/// Per partition: what the records contain.
#[derive(Default)]
struct Content {
    clob_events: BTreeMap<String, u64>,
    markets_with_data: HashSet<String>,
    tokens_with_data: HashSet<String>,
    book_snapshots: u64,
    wallet_trades: u64,
    notional: f64,
    wallets: HashSet<String>,
    score_updates: u64,
    games: HashSet<String>,
    /// Markets the recorder subscribed (`markets` records with `tracked: true`).
    markets_tracked: HashSet<String>,
    events: HashSet<String>,
    unparsed: u64,
}

impl Content {
    fn merge(&mut self, o: Content) {
        for (k, n) in o.clob_events {
            *self.clob_events.entry(k).or_default() += n;
        }
        self.markets_with_data.extend(o.markets_with_data);
        self.tokens_with_data.extend(o.tokens_with_data);
        self.book_snapshots += o.book_snapshots;
        self.wallet_trades += o.wallet_trades;
        self.notional += o.notional;
        self.wallets.extend(o.wallets);
        self.score_updates += o.score_updates;
        self.games.extend(o.games);
        self.markets_tracked.extend(o.markets_tracked);
        self.events.extend(o.events);
        self.unparsed += o.unparsed;
    }

    /// Looks at one payload of `stream`. Payloads are probed with borrowed partial structs: only
    /// the fields the audit needs are decoded.
    fn add(&mut self, stream: &str, payload: &RawValue) -> Option<()> {
        let json = payload.get();
        match stream {
            "clob_book" | "clob_bbo" | "clob_trades" | "clob_other" => {
                let m: ClobProbe = serde_json::from_str(json).ok()?;
                let kind = m.event_type.as_deref().unwrap_or("?");
                *self.clob_events.entry(kind.to_string()).or_default() += 1;
                if let Some(c) = m.market {
                    self.markets_with_data.insert(c.into_owned());
                }
                if let Some(a) = m.asset_id {
                    self.tokens_with_data.insert(a.into_owned());
                }
                for pc in m.price_changes {
                    self.tokens_with_data.insert(pc.asset_id.into_owned());
                }
            }
            "book_snapshots" => self.book_snapshots += 1,
            "trades" => {
                let m: RtdsProbe = serde_json::from_str(json).ok()?;
                let p = m.payload;
                self.wallet_trades += 1;
                self.notional += p.price * p.size;
                self.wallets.insert(p.proxy_wallet.into_owned());
            }
            "scores" => {
                let m: ScoreProbe = serde_json::from_str(json).ok()?;
                let game = m
                    .game_id
                    .map(|g| g.to_string())
                    .or(m.metadata_game_id.map(Cow::into_owned))?;
                self.score_updates += 1;
                self.games.insert(game);
            }
            "markets" => {
                let m: MarketProbe = serde_json::from_str(json).ok()?;
                if m.tracked {
                    self.markets_tracked.insert(m.condition_id.into_owned());
                }
            }
            "gamma_events" => {
                let m: EventProbe = serde_json::from_str(json).ok()?;
                self.events.insert(m.id.into_owned());
            }
            _ => {}
        }
        Some(())
    }
}

#[derive(Deserialize)]
struct ClobProbe<'a> {
    #[serde(borrow, default)]
    event_type: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    market: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    asset_id: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    price_changes: Vec<AssetProbe<'a>>,
}

#[derive(Deserialize)]
struct AssetProbe<'a> {
    #[serde(borrow)]
    asset_id: Cow<'a, str>,
}

#[derive(Deserialize)]
struct RtdsProbe<'a> {
    #[serde(borrow)]
    payload: TradeProbe<'a>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TradeProbe<'a> {
    price: f64,
    size: f64,
    #[serde(borrow)]
    proxy_wallet: Cow<'a, str>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ScoreProbe<'a> {
    #[serde(default)]
    game_id: Option<i64>,
    /// Instead of `gameId` for some leagues (cricket).
    #[serde(borrow, default)]
    metadata_game_id: Option<Cow<'a, str>>,
}

#[derive(Deserialize)]
struct MarketProbe<'a> {
    #[serde(borrow)]
    condition_id: Cow<'a, str>,
    #[serde(default)]
    tracked: bool,
}

#[derive(Deserialize)]
struct EventProbe<'a> {
    #[serde(borrow)]
    id: Cow<'a, str>,
}

/// A connect / disconnect marker from `_system/<feed>/sessions`.
#[derive(Clone, Debug)]
struct SessionEvent {
    t_ns: i64,
    run: i64,
    feed: String,
    connected: bool,
    reason: String,
}

#[derive(Deserialize)]
struct SessionProbe {
    feed: String,
    event: String,
    #[serde(default)]
    reason: String,
}

fn scan_file(info: FileInfo, gap_ns: i64) -> (FileSummary, Content, Vec<SessionEvent>) {
    let open = std::fs::metadata(&info.path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|m| SystemTime::now().duration_since(m).ok())
        .is_some_and(|age| age < Duration::from_secs(120));
    let hour_start = info.hour_start_ns();
    let hour_range = hour_start - HOUR_SLACK_NS..hour_start + 3_600_000_000_000 + HOUR_SLACK_NS;
    let mut content = Content::default();
    let mut sessions = Vec::new();
    let (mut first, mut last) = (None::<i64>, None::<i64>);
    let mut out_of_hour = 0;
    let mut gaps = Vec::new();
    let result = read_file(&info.path, |r| {
        if !hour_range.contains(&r.t_ns) {
            out_of_hour += 1;
        }
        if let Some(l) = last
            && r.t_ns - l >= gap_ns
        {
            gaps.push((l, r.t_ns));
        }
        first = Some(first.map_or(r.t_ns, |f| f.min(r.t_ns)));
        last = Some(last.map_or(r.t_ns, |l| l.max(r.t_ns)));
        if info.stream == "sessions" {
            if let Ok(s) = serde_json::from_str::<SessionProbe>(r.payload.get()) {
                sessions.push(SessionEvent {
                    t_ns: r.t_ns,
                    run: info.run,
                    feed: s.feed,
                    connected: s.event == "connected",
                    reason: s.reason,
                });
            }
        } else if content.add(&info.stream, r.payload).is_none() {
            content.unparsed += 1;
        }
    });
    let (rep, error) = match result {
        Ok(rep) => (rep, None),
        Err(e) => (ReadReport::default(), Some(format!("{e:#}"))),
    };
    let summary = FileSummary {
        info,
        rep,
        error,
        open,
        first,
        last,
        out_of_hour,
        gaps,
    };
    (summary, content, sessions)
}
