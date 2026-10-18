//! The audit [`Report`]: built from the collected per-file data, rendered as text or JSON.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write;
use std::path::PathBuf;

use chrono::NaiveDate;
use serde::Serialize;

use super::{Acc, Content, FileSummary, META_STREAMS, Options, SessionEvent};
use crate::feed_family as family;
use crate::fmt::{bytes, count, secs, time_ns};

const NS: f64 = 1e9;
/// Shorter outages (a reconnect) are counted in the feed totals but not listed.
const MIN_OUTAGE_S: f64 = 5.0;

#[derive(Debug, Serialize)]
pub struct Report {
    pub root: String,
    pub totals: Totals,
    pub days: Vec<DayRow>,
    pub runs: Vec<RunRow>,
    pub feeds: Vec<FeedRow>,
    /// Longest disconnections of at least 5 s (or never recovered), longest first.
    pub outages: Vec<Outage>,
    /// Silences of every feed at once within a run: the recorder or the network stalled.
    pub recording_gaps: Vec<Gap>,
    /// Longest silences of single streams (often just a quiet league), longest first.
    pub stream_gaps: Vec<Gap>,
    pub partitions: Vec<SizeRow>,
    pub streams: Vec<SizeRow>,
    pub partition_streams: Vec<SizeRow>,
    pub content: Vec<ContentRow>,
    pub integrity: Integrity,
    /// Silence threshold used for the gap lists, seconds.
    pub gap_threshold_s: f64,
}

#[derive(Debug, Default, Serialize)]
pub struct Totals {
    pub files: u64,
    pub runs: u64,
    pub records: u64,
    pub disk_bytes: u64,
    pub raw_bytes: u64,
    pub first: Option<String>,
    pub last: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DayRow {
    /// YYYY-MM-DD (UTC).
    pub date: String,
    pub files: u64,
    pub runs: u64,
    pub records: u64,
    pub disk_bytes: u64,
    pub raw_bytes: u64,
}

#[derive(Debug, Serialize)]
pub struct RunRow {
    /// Run id = recorder start time (Unix seconds); the `r<run>` file suffix.
    pub run: i64,
    pub first: String,
    pub last: String,
    pub duration_s: f64,
    pub files: u64,
    pub records: u64,
    pub disk_bytes: u64,
    /// Time since the previous run's last record (negative: the runs overlapped).
    pub gap_before_s: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct FeedRow {
    /// Feed family: `clob` (all shards), `rtds`, `sports`.
    pub feed: String,
    pub connections: u64,
    /// Number of distinct connection names (CLOB shards).
    pub shards: u64,
    /// Disconnects other than a clean shutdown.
    pub drops: u64,
    pub downtime_s: f64,
    pub longest_s: f64,
    /// Drop reasons, most common first.
    pub reasons: Vec<(String, u64)>,
}

#[derive(Debug, Serialize)]
pub struct Outage {
    pub feed: String,
    pub run: i64,
    pub from: String,
    pub to: String,
    pub secs: f64,
    pub reason: String,
    /// False: never reconnected before the run's last record.
    pub recovered: bool,
}

#[derive(Debug, Serialize)]
pub struct Gap {
    /// `*` for recording gaps (every stream).
    pub partition: String,
    pub stream: String,
    pub run: i64,
    pub from: String,
    pub to: String,
    pub secs: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct SizeRow {
    pub partition: String,
    pub stream: String,
    pub files: u64,
    pub records: u64,
    pub disk_bytes: u64,
    pub raw_bytes: u64,
    pub first: Option<String>,
    pub last: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ContentRow {
    pub partition: String,
    pub markets_tracked: u64,
    pub markets_with_clob_data: u64,
    pub tokens_with_clob_data: u64,
    /// CLOB events by `event_type`.
    pub clob_events: BTreeMap<String, u64>,
    pub book_snapshots: u64,
    pub wallet_trades: u64,
    /// Sum of price × size of wallet trades (USDC).
    pub notional: f64,
    pub wallets: u64,
    pub score_updates: u64,
    pub games: u64,
    pub events: u64,
    /// Records whose payload did not have the expected shape.
    pub unparsed: u64,
}

#[derive(Debug, Default, Serialize)]
pub struct Integrity {
    pub damaged: Vec<Damaged>,
    pub bad_lines: u64,
    /// Records more than a minute outside their file's hour.
    pub out_of_hour: u64,
    pub unrecognised: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct Damaged {
    pub path: String,
    pub reason: String,
    /// Modified in the last two minutes: probably being written right now, not damaged.
    pub open: bool,
}

pub(super) fn build(acc: Acc, unrecognised: Vec<PathBuf>, opts: &Options) -> Report {
    let gap_ns = opts.gap.as_nanos() as i64;
    let Acc {
        files,
        content,
        sessions,
    } = acc;
    let span = |first: Option<i64>, last: Option<i64>| (first.map(time_ns), last.map(time_ns));

    // Sizes: totals, per day, per partition / stream.
    let mut totals = Totals::default();
    let mut days: BTreeMap<NaiveDate, (DayRow, BTreeSet<i64>)> = BTreeMap::new();
    let mut sizes: BTreeMap<(String, String), SizeAcc> = BTreeMap::new();
    let mut runs: BTreeMap<i64, RunAcc> = BTreeMap::new();
    let mut integrity = Integrity::default();
    let (mut t_first, mut t_last) = (None, None);
    for f in &files {
        let (records, raw) = (f.rep.records, f.rep.bytes);
        totals.files += 1;
        totals.records += records;
        totals.disk_bytes += f.info.size;
        totals.raw_bytes += raw;
        t_first = min_opt(t_first, f.first);
        t_last = max_opt(t_last, f.last);

        let (d, d_runs) = days.entry(f.info.date).or_insert_with(|| {
            (
                DayRow {
                    date: f.info.date.to_string(),
                    files: 0,
                    runs: 0,
                    records: 0,
                    disk_bytes: 0,
                    raw_bytes: 0,
                },
                BTreeSet::new(),
            )
        });
        d.files += 1;
        d.records += records;
        d.disk_bytes += f.info.size;
        d.raw_bytes += raw;
        d_runs.insert(f.info.run);

        sizes
            .entry((f.info.part.to_string(), f.info.stream.clone()))
            .or_default()
            .add(f);
        runs.entry(f.info.run).or_default().add(f);

        integrity.bad_lines += f.rep.bad_lines;
        integrity.out_of_hour += f.out_of_hour;
        if let Some(reason) = f.error.clone().or_else(|| f.rep.truncated.clone()) {
            integrity.damaged.push(Damaged {
                path: f.info.path.display().to_string(),
                reason,
                open: f.open,
            });
        }
    }
    totals.runs = runs.len() as u64;
    (totals.first, totals.last) = span(t_first, t_last);
    integrity.damaged.sort_by(|a, b| a.path.cmp(&b.path));
    integrity.unrecognised = unrecognised.iter().map(|p| p.display().to_string()).collect();

    let size_row = |(partition, stream): (String, String), a: &SizeAcc| {
        let (first, last) = span(a.first, a.last);
        SizeRow {
            partition,
            stream,
            files: a.files,
            records: a.records,
            disk_bytes: a.disk,
            raw_bytes: a.raw,
            first,
            last,
        }
    };
    let mut by_part: BTreeMap<String, SizeAcc> = BTreeMap::new();
    let mut by_stream: BTreeMap<String, SizeAcc> = BTreeMap::new();
    for ((p, s), a) in &sizes {
        by_part.entry(p.clone()).or_default().merge(a);
        by_stream.entry(s.clone()).or_default().merge(a);
    }
    let partitions = by_part
        .into_iter()
        .map(|(p, a)| size_row((p, "*".into()), &a))
        .collect();
    let streams = by_stream
        .into_iter()
        .map(|(s, a)| size_row(("*".into(), s), &a))
        .collect();
    let partition_streams = sizes.into_iter().map(|(k, a)| size_row(k, &a)).collect();

    // Runs, in start order, with the time since the previous one ended.
    let mut run_rows = Vec::new();
    let mut prev_last: Option<i64> = None;
    for (&run, a) in &runs {
        let (Some(first), Some(last)) = (a.first, a.last) else {
            continue;
        };
        run_rows.push(RunRow {
            run,
            first: time_ns(first),
            last: time_ns(last),
            duration_s: (last - first) as f64 / NS,
            files: a.files,
            records: a.records,
            disk_bytes: a.disk,
            gap_before_s: prev_last.map(|p| (first - p) as f64 / NS),
        });
        prev_last = max_opt(prev_last, Some(last));
    }

    let run_last: HashMap<i64, i64> = runs.iter().filter_map(|(r, a)| Some((*r, a.last?))).collect();
    let (feeds, mut outages) = connections(sessions, &run_last);
    outages.retain(|o| o.secs >= MIN_OUTAGE_S || !o.recovered);
    outages.sort_by(|a, b| b.secs.total_cmp(&a.secs));
    outages.truncate(opts.top);

    // Gaps: merge each file's covered intervals, per run (all feed streams) and per stream.
    let mut per_run: HashMap<i64, Vec<(i64, i64)>> = HashMap::new();
    let mut per_stream: HashMap<(String, String, i64), Vec<(i64, i64)>> = HashMap::new();
    for f in files.iter().filter(|f| !META_STREAMS.contains(&f.info.stream.as_str())) {
        let iv = covered(f);
        per_run.entry(f.info.run).or_default().extend(&iv);
        // Unconfigured leagues (`other/*`) only have scores, which pause between games.
        if &*f.info.part.category == "other" {
            continue;
        }
        per_stream
            .entry((f.info.part.to_string(), f.info.stream.clone(), f.info.run))
            .or_default()
            .extend(iv);
    }
    let gap_row = |partition: String, stream: String, run: i64, (a, b): (i64, i64)| Gap {
        partition,
        stream,
        run,
        from: time_ns(a),
        to: time_ns(b),
        secs: (b - a) as f64 / NS,
    };
    let mut recording_gaps: Vec<Gap> = per_run
        .into_iter()
        .flat_map(|(run, iv)| {
            silences(iv, gap_ns)
                .into_iter()
                .map(move |g| gap_row("*".into(), "*".into(), run, g))
        })
        .collect();
    recording_gaps.sort_by(|a, b| a.from.cmp(&b.from));
    let mut stream_gaps: Vec<Gap> = per_stream
        .into_iter()
        .flat_map(|((p, s, run), iv)| {
            silences(iv, gap_ns)
                .into_iter()
                .map(move |g| gap_row(p.clone(), s.clone(), run, g))
        })
        .collect();
    stream_gaps.sort_by(|a, b| b.secs.total_cmp(&a.secs));
    stream_gaps.truncate(opts.top);

    let mut content: Vec<ContentRow> = content
        .into_iter()
        .filter(|(p, _)| &*p.category != "_system")
        .map(|(p, c)| content_row(p.to_string(), c))
        .collect();
    content.sort_by(|a, b| a.partition.cmp(&b.partition));

    Report {
        root: opts.root.display().to_string(),
        totals,
        days: days
            .into_values()
            .map(|(mut d, r)| {
                d.runs = r.len() as u64;
                d
            })
            .collect(),
        runs: run_rows,
        feeds,
        outages,
        recording_gaps,
        stream_gaps,
        partitions,
        streams,
        partition_streams,
        content,
        integrity,
        gap_threshold_s: opts.gap.as_secs_f64(),
    }
}

#[derive(Default)]
struct SizeAcc {
    files: u64,
    records: u64,
    disk: u64,
    raw: u64,
    first: Option<i64>,
    last: Option<i64>,
}

impl SizeAcc {
    fn add(&mut self, f: &FileSummary) {
        self.files += 1;
        self.records += f.rep.records;
        self.disk += f.info.size;
        self.raw += f.rep.bytes;
        self.first = min_opt(self.first, f.first);
        self.last = max_opt(self.last, f.last);
    }

    fn merge(&mut self, o: &SizeAcc) {
        self.files += o.files;
        self.records += o.records;
        self.disk += o.disk;
        self.raw += o.raw;
        self.first = min_opt(self.first, o.first);
        self.last = max_opt(self.last, o.last);
    }
}

type RunAcc = SizeAcc;

fn min_opt(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

fn max_opt(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    a.max(b)
}

/// The time spans a file has records for: first to last, minus its internal silences.
fn covered(f: &FileSummary) -> Vec<(i64, i64)> {
    let (Some(first), Some(last)) = (f.first, f.last) else {
        return vec![];
    };
    let mut out = Vec::with_capacity(f.gaps.len() + 1);
    let mut start = first;
    for &(a, b) in &f.gaps {
        out.push((start, a));
        start = b;
    }
    out.push((start, last));
    out
}

/// Merges intervals and returns the holes between them of at least `min_ns`.
fn silences(mut iv: Vec<(i64, i64)>, min_ns: i64) -> Vec<(i64, i64)> {
    iv.sort_unstable();
    let mut out = Vec::new();
    let mut end: Option<i64> = None;
    for (a, b) in iv {
        if let Some(e) = end {
            if a - e >= min_ns {
                out.push((e, a));
            }
            end = Some(e.max(b));
        } else {
            end = Some(b);
        }
    }
    out
}

/// Replays connect / disconnect markers per run and connection. A disconnect starts an outage
/// that ends at the next connect; a clean `shutdown` ends tracking; an outage still open at the
/// end of the run lasts until the run's last record.
fn connections(mut evs: Vec<SessionEvent>, run_last: &HashMap<i64, i64>) -> (Vec<FeedRow>, Vec<Outage>) {
    evs.sort_by(|a, b| (a.run, &a.feed, a.t_ns).cmp(&(b.run, &b.feed, b.t_ns)));
    #[derive(Default)]
    struct Fam {
        connections: u64,
        shards: BTreeSet<String>,
        drops: u64,
        downtime: f64,
        longest: f64,
        reasons: HashMap<String, u64>,
    }
    let mut fams: BTreeMap<String, Fam> = BTreeMap::new();
    let mut outages = Vec::new();
    let mut i = 0;
    while i < evs.len() {
        let (run, feed) = (evs[i].run, evs[i].feed.clone());
        let fam = fams.entry(family(&feed).to_string()).or_default();
        fam.shards.insert(feed.clone());
        let mut down: Option<(i64, String)> = None;
        while i < evs.len() && evs[i].run == run && evs[i].feed == feed {
            let e = &evs[i];
            i += 1;
            if e.connected {
                fam.connections += 1;
                if let Some((t0, reason)) = down.take() {
                    outages.push(outage(&feed, run, t0, e.t_ns, reason, true));
                }
            } else if e.reason == "shutdown" {
                down = None;
            } else {
                fam.drops += 1;
                *fam.reasons.entry(short_reason(&e.reason)).or_default() += 1;
                down.get_or_insert((e.t_ns, e.reason.clone()));
            }
        }
        if let Some((t0, reason)) = down
            && let Some(&end) = run_last.get(&run)
        {
            outages.push(outage(&feed, run, t0, end.max(t0), reason, false));
        }
    }
    for o in &outages {
        let fam = fams.get_mut(family(&o.feed)).expect("family of a seen feed");
        fam.downtime += o.secs;
        fam.longest = fam.longest.max(o.secs);
    }
    let rows = fams
        .into_iter()
        .map(|(feed, f)| {
            let mut reasons: Vec<(String, u64)> = f.reasons.into_iter().collect();
            reasons.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
            FeedRow {
                feed,
                connections: f.connections,
                shards: f.shards.len() as u64,
                drops: f.drops,
                downtime_s: f.downtime,
                longest_s: f.longest,
                reasons,
            }
        })
        .collect();
    (rows, outages)
}

fn outage(feed: &str, run: i64, from: i64, to: i64, reason: String, recovered: bool) -> Outage {
    Outage {
        feed: feed.to_string(),
        run,
        from: time_ns(from),
        to: time_ns(to),
        secs: (to - from) as f64 / NS,
        reason,
        recovered,
    }
}

/// Groups similar disconnect reasons: `server close: Some(CloseFrame { code: Away, reason:
/// "server shutting down" })` → `server close: server shutting down`.
fn short_reason(r: &str) -> String {
    if let Some(i) = r.find("reason: Utf8Bytes(b\"") {
        let rest = &r[i + 20..];
        let msg = rest.split('"').next().unwrap_or("");
        let head = r.split(':').next().unwrap_or(r);
        return format!("{head}: {msg}");
    }
    r.chars().take(80).collect()
}

fn content_row(partition: String, c: Content) -> ContentRow {
    ContentRow {
        partition,
        markets_tracked: c.markets_tracked.len() as u64,
        markets_with_clob_data: c.markets_with_data.len() as u64,
        tokens_with_clob_data: c.tokens_with_data.len() as u64,
        clob_events: c.clob_events,
        book_snapshots: c.book_snapshots,
        wallet_trades: c.wallet_trades,
        notional: c.notional,
        wallets: c.wallets.len() as u64,
        score_updates: c.score_updates,
        games: c.games.len() as u64,
        events: c.events.len() as u64,
        unparsed: c.unparsed,
    }
}

impl Report {
    /// The report as plain text tables.
    pub fn render(&self) -> String {
        let mut o = String::new();
        let t = &self.totals;
        let ratio = if t.disk_bytes > 0 {
            t.raw_bytes as f64 / t.disk_bytes as f64
        } else {
            0.0
        };
        let _ = writeln!(o, "pm audit  {}", self.root);
        let _ = writeln!(
            o,
            "{} records in {} files, {} on disk ({} raw, {:.1}x), {} runs, {} .. {}",
            count(t.records),
            t.files,
            bytes(t.disk_bytes as f64),
            bytes(t.raw_bytes as f64),
            ratio,
            t.runs,
            t.first.as_deref().unwrap_or("-"),
            t.last.as_deref().unwrap_or("-"),
        );

        section(&mut o, "Days");
        let _ = writeln!(
            o,
            "{:<10}  {:>6} {:>5} {:>10} {:>10} {:>10}",
            "date", "files", "runs", "records", "disk", "raw"
        );
        for d in &self.days {
            let _ = writeln!(
                o,
                "{:<10}  {:>6} {:>5} {:>10} {:>10} {:>10}",
                d.date,
                d.files,
                d.runs,
                count(d.records),
                bytes(d.disk_bytes as f64),
                bytes(d.raw_bytes as f64)
            );
        }

        section(&mut o, "Runs (recorder process starts)");
        let _ = writeln!(
            o,
            "{:<11} {:<21} {:<21} {:>9} {:>10} {:>10}  gap before",
            "run", "first", "last", "length", "records", "disk"
        );
        for r in &self.runs {
            let gap = match r.gap_before_s {
                None => String::new(),
                Some(g) if g < 0.0 => format!("OVERLAPS previous by {}", secs(-g)),
                Some(g) => secs(g),
            };
            let _ = writeln!(
                o,
                "{:<11} {:<21} {:<21} {:>9} {:>10} {:>10}  {gap}",
                r.run,
                r.first,
                r.last,
                secs(r.duration_s),
                count(r.records),
                bytes(r.disk_bytes as f64)
            );
        }

        section(&mut o, "Feed connections");
        let _ = writeln!(
            o,
            "{:<8} {:>7} {:>6} {:>6} {:>9} {:>9}  drop reasons",
            "feed", "shards", "conns", "drops", "downtime", "longest"
        );
        for f in &self.feeds {
            let reasons: Vec<String> = f.reasons.iter().take(3).map(|(r, n)| format!("{n}x {r}")).collect();
            let _ = writeln!(
                o,
                "{:<8} {:>7} {:>6} {:>6} {:>9} {:>9}  {}",
                f.feed,
                f.shards,
                f.connections,
                f.drops,
                secs(f.downtime_s),
                secs(f.longest_s),
                reasons.join("; ")
            );
        }
        if self.outages.is_empty() {
            let _ = writeln!(o, "\nevery outage was shorter than {MIN_OUTAGE_S:.0}s");
        } else {
            let _ = writeln!(o, "\nlongest outages:");
            for x in &self.outages {
                let _ = writeln!(
                    o,
                    "  {:<10} {:>8}  {} .. {}{}  ({})",
                    x.feed,
                    secs(x.secs),
                    x.from,
                    x.to,
                    if x.recovered { "" } else { " (run ended)" },
                    short_reason(&x.reason)
                );
            }
        }

        let threshold = secs(self.gap_threshold_s);
        section(
            &mut o,
            &format!("Recording gaps (no record in any feed for >= {threshold})"),
        );
        if self.recording_gaps.is_empty() {
            let _ = writeln!(o, "none");
        }
        for g in &self.recording_gaps {
            let _ = writeln!(o, "  run {}  {} .. {}  {}", g.run, g.from, g.to, secs(g.secs));
        }
        section(
            &mut o,
            &format!("Longest silences of single streams (>= {threshold}; quiet leagues are normal)"),
        );
        if self.stream_gaps.is_empty() {
            let _ = writeln!(o, "none");
        }
        for g in &self.stream_gaps {
            let _ = writeln!(
                o,
                "  {:<16} {:<15} {:>8}  {} .. {}",
                g.partition,
                g.stream,
                secs(g.secs),
                g.from,
                g.to
            );
        }

        section(&mut o, "Size by stream");
        size_table(&mut o, &self.streams, |r| &r.stream);
        section(
            &mut o,
            "Size by partition (unconfigured leagues folded into other/*; see --json)",
        );
        size_table(&mut o, &fold_other_sizes(&self.partitions), |r| &r.partition);

        section(&mut o, "Content");
        let _ = writeln!(
            o,
            "{:<16} {:>13} {:>7} {:>8} {:>8} {:>7} {:>7} {:>9} {:>9} {:>7} {:>8} {:>6}",
            "partition",
            "mkts data/trk",
            "tokens",
            "book",
            "pchange",
            "trades",
            "bbo",
            "snapshots",
            "w.trades",
            "wallets",
            "notional",
            "games"
        );
        for c in &fold_other_content(&self.content) {
            let ev = |k: &str| count(c.clob_events.get(k).copied().unwrap_or(0));
            let _ = writeln!(
                o,
                "{:<16} {:>13} {:>7} {:>8} {:>8} {:>7} {:>7} {:>9} {:>9} {:>7} {:>8} {:>6}",
                c.partition,
                format!("{}/{}", c.markets_with_clob_data, c.markets_tracked),
                count(c.tokens_with_clob_data),
                ev("book"),
                ev("price_change"),
                ev("last_trade_price"),
                ev("best_bid_ask"),
                count(c.book_snapshots),
                count(c.wallet_trades),
                count(c.wallets),
                format!("${}", count(c.notional as u64)),
                c.games
            );
        }

        section(&mut o, "Integrity");
        let i = &self.integrity;
        let open = i.damaged.iter().filter(|d| d.open).count();
        let _ = writeln!(
            o,
            "truncated / unreadable files: {} ({} still being written), undecodable lines: {}, records outside their hour: {}, unrecognised files: {}",
            i.damaged.len() - open,
            open,
            i.bad_lines,
            i.out_of_hour,
            i.unrecognised.len()
        );
        for d in i.damaged.iter().filter(|d| !d.open).take(20) {
            let _ = writeln!(o, "  {}: {}", d.path, d.reason);
        }
        for (dir, (n, example)) in group_unrecognised(&self.root, &i.unrecognised) {
            let _ = writeln!(o, "  unrecognised: {n} files under {dir}/ (e.g. {example})");
        }
        let unparsed: u64 = self.content.iter().map(|c| c.unparsed).sum();
        if unparsed > 0 {
            let _ = writeln!(o, "records with an unexpected payload shape: {unparsed}");
        }
        o
    }
}

/// `other/*` partitions summed into one row (they only carry scores of unconfigured leagues).
fn fold_other_sizes(rows: &[SizeRow]) -> Vec<SizeRow> {
    let mut out: Vec<SizeRow> = Vec::new();
    let mut other: Option<SizeRow> = None;
    for r in rows {
        if !r.partition.starts_with("other/") {
            out.push(r.clone());
            continue;
        }
        let Some(o) = &mut other else {
            other = Some(SizeRow {
                partition: "other/*".into(),
                ..r.clone()
            });
            continue;
        };
        o.files += r.files;
        o.records += r.records;
        o.disk_bytes += r.disk_bytes;
        o.raw_bytes += r.raw_bytes;
        // Same-format UTC strings: lexical order is time order.
        if r.first.is_some() && (o.first.is_none() || r.first < o.first) {
            o.first = r.first.clone();
        }
        o.last = o.last.clone().max(r.last.clone());
    }
    out.extend(other);
    out
}

fn fold_other_content(rows: &[ContentRow]) -> Vec<ContentRow> {
    let mut out: Vec<ContentRow> = Vec::new();
    let mut other: Option<ContentRow> = None;
    for r in rows {
        if !r.partition.starts_with("other/") {
            out.push(r.clone());
            continue;
        }
        let Some(o) = &mut other else {
            other = Some(ContentRow {
                partition: "other/*".into(),
                ..r.clone()
            });
            continue;
        };
        o.markets_tracked += r.markets_tracked;
        o.markets_with_clob_data += r.markets_with_clob_data;
        o.tokens_with_clob_data += r.tokens_with_clob_data;
        for (k, n) in &r.clob_events {
            *o.clob_events.entry(k.clone()).or_default() += n;
        }
        o.book_snapshots += r.book_snapshots;
        o.wallet_trades += r.wallet_trades;
        o.notional += r.notional;
        o.wallets += r.wallets;
        o.score_updates += r.score_updates;
        o.games += r.games;
        o.events += r.events;
        o.unparsed += r.unparsed;
    }
    out.extend(other);
    out
}

/// Unrecognised files by their first directory under the root: (count, one example).
fn group_unrecognised(root: &str, paths: &[String]) -> BTreeMap<String, (usize, String)> {
    let mut out: BTreeMap<String, (usize, String)> = BTreeMap::new();
    for p in paths {
        let rel = p.strip_prefix(root).unwrap_or(p).trim_start_matches('/');
        let dir = rel.split('/').next().unwrap_or("").to_string();
        let e = out.entry(dir).or_insert_with(|| (0, rel.to_string()));
        e.0 += 1;
    }
    out
}

fn section(o: &mut String, title: &str) {
    let _ = writeln!(o, "\n== {title}");
}

fn size_table(o: &mut String, rows: &[SizeRow], name: impl Fn(&SizeRow) -> &str) {
    let _ = writeln!(
        o,
        "{:<16} {:>6} {:>10} {:>10} {:>10}  first .. last",
        "", "files", "records", "disk", "raw"
    );
    for r in rows {
        let _ = writeln!(
            o,
            "{:<16} {:>6} {:>10} {:>10} {:>10}  {} .. {}",
            name(r),
            r.files,
            count(r.records),
            bytes(r.disk_bytes as f64),
            bytes(r.raw_bytes as f64),
            r.first.as_deref().unwrap_or("-"),
            r.last.as_deref().unwrap_or("-")
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(t: i64, feed: &str, connected: bool, reason: &str) -> SessionEvent {
        SessionEvent {
            t_ns: t,
            run: 1,
            feed: feed.into(),
            connected,
            reason: reason.into(),
        }
    }

    #[test]
    fn merges_intervals_into_silences() {
        let iv = vec![(10, 20), (0, 5), (18, 30), (100, 110)];
        assert_eq!(silences(iv.clone(), 5), vec![(5, 10), (30, 100)]);
        assert_eq!(silences(iv, 50), vec![(30, 100)]);
    }

    #[test]
    fn replays_connections() {
        let s = 1_000_000_000;
        let evs = vec![
            ev(0, "clob-0", true, ""),
            ev(10 * s, "clob-0", false, "idle for 45s"),
            ev(13 * s, "clob-0", true, ""),
            ev(20 * s, "clob-1", true, ""),
            ev(30 * s, "clob-1", false, "stream ended"),
            ev(0, "rtds", true, ""),
            ev(50 * s, "rtds", false, "shutdown"),
        ];
        let run_last = HashMap::from([(1, 60 * s)]);
        let (feeds, outages) = connections(evs, &run_last);
        let clob = feeds.iter().find(|f| f.feed == "clob").unwrap();
        assert_eq!((clob.shards, clob.connections, clob.drops), (2, 3, 2));
        assert_eq!(clob.downtime_s, 3.0 + 30.0);
        assert_eq!(clob.longest_s, 30.0);
        let rtds = feeds.iter().find(|f| f.feed == "rtds").unwrap();
        assert_eq!((rtds.drops, rtds.downtime_s), (0, 0.0));
        assert!(outages.iter().any(|o| o.feed == "clob-1" && !o.recovered));
    }

    #[test]
    fn shortens_close_reasons() {
        let r = "server close: Some(CloseFrame { code: Away, reason: Utf8Bytes(b\"server shutting down\") })";
        assert_eq!(short_reason(r), "server close: server shutting down");
        assert_eq!(family("clob-12"), "clob");
        assert_eq!(family("rtds"), "rtds");
    }

    fn size(partition: &str, records: u64, first: Option<&str>, last: Option<&str>) -> SizeRow {
        SizeRow {
            partition: partition.into(),
            stream: "*".into(),
            files: 1,
            records,
            disk_bytes: 10,
            raw_bytes: 100,
            first: first.map(String::from),
            last: last.map(String::from),
        }
    }

    #[test]
    fn folds_unconfigured_leagues_into_one_row() {
        let rows = vec![
            size(
                "esports/cs2",
                5,
                Some("2026-10-17 10:00:00Z"),
                Some("2026-10-17 11:00:00Z"),
            ),
            size("other/a", 1, None, None),
            size("other/b", 2, Some("2026-10-17 09:00:00Z"), Some("2026-10-17 12:00:00Z")),
            size("other/c", 3, Some("2026-10-17 08:00:00Z"), Some("2026-10-17 10:00:00Z")),
        ];
        let f = fold_other_sizes(&rows);
        assert_eq!(f.len(), 2);
        assert_eq!(f[0].partition, "esports/cs2");
        let o = &f[1];
        assert_eq!(
            (o.partition.as_str(), o.files, o.records, o.disk_bytes),
            ("other/*", 3, 6, 30)
        );
        assert_eq!(
            o.first.as_deref(),
            Some("2026-10-17 08:00:00Z"),
            "earliest, skipping a row without records"
        );
        assert_eq!(o.last.as_deref(), Some("2026-10-17 12:00:00Z"));
        assert!(fold_other_sizes(&rows[..1]).iter().all(|r| r.partition != "other/*"));
    }

    #[test]
    fn covered_intervals_skip_internal_silences() {
        let info = pm_store::read::FileInfo {
            path: "x".into(),
            date: chrono::NaiveDate::from_ymd_opt(2026, 10, 17).unwrap(),
            hour: 0,
            part: pm_store::Partition::new("a", "b"),
            stream: "s".into(),
            run: 1,
            size: 0,
        };
        let f = |first, last, gaps: Vec<(i64, i64)>| FileSummary {
            info: info.clone(),
            rep: Default::default(),
            error: None,
            open: false,
            first,
            last,
            out_of_hour: 0,
            gaps,
        };
        assert_eq!(covered(&f(Some(0), Some(100), vec![])), [(0, 100)]);
        assert_eq!(
            covered(&f(Some(0), Some(100), vec![(10, 40), (50, 90)])),
            [(0, 10), (40, 50), (90, 100)]
        );
        assert!(covered(&f(None, None, vec![])).is_empty());
        // Touching and nested intervals merge; nothing is reported below the threshold.
        assert!(silences(vec![(0, 10), (10, 20), (2, 5)], 1).is_empty());
        assert!(silences(vec![], 1).is_empty());
    }

    #[test]
    fn short_reasons_never_panic() {
        for r in [
            "",
            "reason: Utf8Bytes(b\"",
            "reason: Utf8Bytes(b\"x",
            "é".repeat(200).as_str(),
        ] {
            let _ = short_reason(r);
        }
        assert_eq!(short_reason(&"é".repeat(200)).chars().count(), 80);
        assert_eq!(family("clob-"), "clob-", "no digits: not a shard number");
        assert_eq!(family("wta-challenger"), "wta-challenger");
    }
}
