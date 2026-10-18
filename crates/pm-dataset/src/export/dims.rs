//! Dimension tables, rebuilt from every recorded `markets`, `gamma_events` and `sessions` file:
//! `markets` and `events` (latest record per id, with first / last seen), `tokens` (one row per
//! outcome token: the join key for every time-series table) and `sessions` (one row per
//! websocket connection).

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
    TimestampNanosecondArray, UInt32Array, UInt64Array,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use pm_store::read::{FileInfo, read_file};
use rayon::prelude::*;
use serde::Deserialize;
use serde::de::IgnoredAny;

/// Dimension table names, in output order.
pub const DIMS: [&str; 4] = ["markets", "tokens", "events", "sessions"];

#[derive(Clone, Deserialize)]
struct MarketRec {
    condition_id: String,
    #[serde(default)]
    market_id: Option<String>,
    #[serde(default)]
    slug: Option<String>,
    #[serde(default)]
    question: Option<String>,
    #[serde(default)]
    event_id: Option<String>,
    #[serde(default)]
    event_slug: Option<String>,
    #[serde(default)]
    market_type: Option<String>,
    #[serde(default)]
    outcomes: Vec<String>,
    #[serde(default)]
    token_ids: Vec<String>,
    #[serde(default)]
    game_start_time: Option<String>,
    #[serde(default)]
    game_id: Option<i64>,
    #[serde(default)]
    tick_size: Option<serde_json::Value>,
    #[serde(default)]
    min_order_size: Option<f64>,
    #[serde(default)]
    neg_risk: Option<bool>,
    #[serde(default)]
    active: Option<bool>,
    #[serde(default)]
    closed: Option<bool>,
    #[serde(default)]
    accepting_orders: Option<bool>,
    #[serde(default)]
    tracked: Option<bool>,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EventRec {
    id: String,
    #[serde(default)]
    slug: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    series_slug: Option<String>,
    #[serde(default)]
    start_time: Option<String>,
    #[serde(default)]
    end_date: Option<String>,
    #[serde(default)]
    game_id: Option<i64>,
    #[serde(default)]
    active: Option<bool>,
    #[serde(default)]
    closed: Option<bool>,
    #[serde(default)]
    live: Option<bool>,
    #[serde(default)]
    ended: Option<bool>,
    #[serde(default)]
    score: Option<String>,
    #[serde(default)]
    period: Option<String>,
    #[serde(default)]
    volume: Option<f64>,
    #[serde(default)]
    volume24hr: Option<f64>,
    #[serde(default)]
    liquidity: Option<f64>,
    #[serde(default)]
    open_interest: Option<f64>,
    #[serde(default)]
    markets: Vec<IgnoredAny>,
    #[serde(default)]
    tags: Vec<Tag>,
}

#[derive(Clone, Deserialize)]
struct Tag {
    #[serde(default)]
    slug: Option<String>,
}

#[derive(Deserialize)]
struct SessionRec {
    feed: String,
    event: String,
    #[serde(default)]
    reason: Option<String>,
}

/// The latest record of a key, plus when the key was first and last seen.
#[derive(Clone)]
struct Latest<T> {
    rec: T,
    t: i64,
    first: i64,
    category: String,
    league: String,
}

fn keep_latest<T>(map: &mut HashMap<String, Latest<T>>, key: String, l: Latest<T>) {
    match map.get_mut(&key) {
        None => {
            map.insert(key, l);
        }
        Some(cur) => {
            let first = cur.first.min(l.first);
            if l.t >= cur.t {
                *cur = l;
            }
            cur.first = first;
        }
    }
}

#[derive(Default)]
struct Collected {
    markets: HashMap<String, Latest<MarketRec>>,
    events: HashMap<String, Latest<EventRec>>,
    /// (run, session id) → (feed, connected at, disconnected at, reason).
    sessions: HashMap<(i64, u64), Conn>,
    unparsed: u64,
}

#[derive(Default)]
struct Conn {
    feed: String,
    connected: Option<i64>,
    disconnected: Option<i64>,
    reason: Option<String>,
}

impl Collected {
    fn merge(mut self, o: Collected) -> Collected {
        for (k, v) in o.markets {
            keep_latest(&mut self.markets, k, v);
        }
        for (k, v) in o.events {
            keep_latest(&mut self.events, k, v);
        }
        for (k, c) in o.sessions {
            let e = self.sessions.entry(k).or_default();
            if e.feed.is_empty() {
                e.feed = c.feed;
            }
            e.connected = e.connected.or(c.connected);
            if c.disconnected.is_some() {
                e.disconnected = c.disconnected;
                e.reason = c.reason;
            }
        }
        self.unparsed += o.unparsed;
        self
    }

    fn add_file(mut self, f: &FileInfo) -> Collected {
        let (category, league) = (f.part.category.to_string(), f.part.league.to_string());
        let res = read_file(&f.path, |r| {
            let json = r.payload.get();
            fn latest<T>(rec: T, t: i64, category: &str, league: &str) -> Latest<T> {
                Latest {
                    rec,
                    t,
                    first: t,
                    category: category.to_string(),
                    league: league.to_string(),
                }
            }
            match f.stream.as_str() {
                "markets" => match serde_json::from_str::<MarketRec>(json) {
                    Ok(m) => keep_latest(
                        &mut self.markets,
                        m.condition_id.clone(),
                        latest(m, r.t_ns, &category, &league),
                    ),
                    Err(_) => self.unparsed += 1,
                },
                "gamma_events" => match serde_json::from_str::<EventRec>(json) {
                    Ok(e) => keep_latest(&mut self.events, e.id.clone(), latest(e, r.t_ns, &category, &league)),
                    Err(_) => self.unparsed += 1,
                },
                "sessions" => match serde_json::from_str::<SessionRec>(json) {
                    Ok(s) => {
                        let c = self.sessions.entry((f.run, r.session)).or_default();
                        c.feed = s.feed;
                        if s.event == "connected" {
                            c.connected = Some(r.t_ns);
                        } else {
                            c.disconnected = Some(r.t_ns);
                            c.reason = s.reason;
                        }
                    }
                    Err(_) => self.unparsed += 1,
                },
                _ => {}
            }
        });
        if let Err(e) = res {
            tracing::warn!("export: skipping {}: {e:#}", f.path.display());
        }
        self
    }
}

/// Builds the dimension tables from `files` (the `markets`, `gamma_events` and `sessions` files).
/// Returns (table name, batch) for every table, plus the number of unparsable records.
pub fn build(files: &[FileInfo]) -> (Vec<(&'static str, RecordBatch)>, u64) {
    let c = files
        .par_iter()
        .fold(Collected::default, |c, f| c.add_file(f))
        .reduce(Collected::default, Collected::merge);

    let mut markets: Vec<Latest<MarketRec>> = c.markets.into_values().collect();
    markets.sort_by(|a, b| (&a.league, &a.rec.condition_id).cmp(&(&b.league, &b.rec.condition_id)));
    let mut events: Vec<Latest<EventRec>> = c.events.into_values().collect();
    events.sort_by(|a, b| (&a.league, &a.rec.id).cmp(&(&b.league, &b.rec.id)));
    let mut sessions: Vec<((i64, u64), Conn)> = c.sessions.into_iter().collect();
    sessions.sort_by_key(|(k, c)| (k.0, c.connected, k.1));

    let out = vec![
        ("markets", markets_batch(&markets)),
        ("tokens", tokens_batch(&markets)),
        ("events", events_batch(&events)),
        ("sessions", sessions_batch(&sessions)),
    ];
    (out, c.unparsed)
}

/// Gamma time formats: RFC 3339, or `2026-10-17 14:00:00+00` / `+00:00`.
fn parse_time(s: &str) -> Option<i64> {
    let ms = |d: DateTime<Utc>| d.timestamp_millis();
    if let Ok(d) = DateTime::parse_from_rfc3339(s) {
        return Some(ms(d.to_utc()));
    }
    let s = s
        .trim_end_matches("+00:00")
        .trim_end_matches("+00")
        .trim_end_matches('Z');
    for fmt in [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
    ] {
        if let Ok(d) = NaiveDateTime::parse_from_str(s, fmt) {
            return Some(ms(d.and_utc()));
        }
    }
    None
}

fn strs<'a>(it: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(StringArray::from_iter(it))
}

fn bools(it: impl Iterator<Item = Option<bool>>) -> ArrayRef {
    Arc::new(BooleanArray::from_iter(it))
}

fn f64s(it: impl Iterator<Item = Option<f64>>) -> ArrayRef {
    Arc::new(Float64Array::from_iter(it))
}

fn i64s(it: impl Iterator<Item = Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from_iter(it))
}

fn ms(it: impl Iterator<Item = Option<i64>>) -> ArrayRef {
    Arc::new(TimestampMillisecondArray::from_iter(it).with_timezone("UTC"))
}

fn ns(it: impl Iterator<Item = Option<i64>>) -> ArrayRef {
    Arc::new(TimestampNanosecondArray::from_iter(it).with_timezone("UTC"))
}

/// Every column is declared nullable. `RecordBatch::try_from_iter` would infer nullability from
/// each batch's contents, and a Parquet file takes its schema from the first batch: a column
/// without nulls in the first hour would become required, and later nulls would be written as 0.
fn batch(cols: Vec<(&str, ArrayRef)>) -> RecordBatch {
    RecordBatch::try_from_iter_with_nullable(cols.into_iter().map(|(n, a)| (n, a, true)))
        .expect("columns of equal length")
}

fn tick(v: &Option<serde_json::Value>) -> Option<f64> {
    match v.as_ref()? {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn markets_batch(m: &[Latest<MarketRec>]) -> RecordBatch {
    let nth = |v: &Vec<String>, i: usize| v.get(i).cloned();
    let outcome = |i| m.iter().map(move |l| nth(&l.rec.outcomes, i)).collect::<Vec<_>>();
    let token = |i| m.iter().map(move |l| nth(&l.rec.token_ids, i)).collect::<Vec<_>>();
    let (o0, o1, t0, t1) = (outcome(0), outcome(1), token(0), token(1));
    batch(vec![
        ("market", strs(m.iter().map(|l| Some(l.rec.condition_id.as_str())))),
        ("market_id", strs(m.iter().map(|l| l.rec.market_id.as_deref()))),
        ("category", strs(m.iter().map(|l| Some(l.category.as_str())))),
        ("league", strs(m.iter().map(|l| Some(l.league.as_str())))),
        ("event_id", strs(m.iter().map(|l| l.rec.event_id.as_deref()))),
        ("event_slug", strs(m.iter().map(|l| l.rec.event_slug.as_deref()))),
        ("slug", strs(m.iter().map(|l| l.rec.slug.as_deref()))),
        ("question", strs(m.iter().map(|l| l.rec.question.as_deref()))),
        ("market_type", strs(m.iter().map(|l| l.rec.market_type.as_deref()))),
        (
            "n_outcomes",
            Arc::new(UInt32Array::from_iter_values(
                m.iter().map(|l| l.rec.outcomes.len() as u32),
            )),
        ),
        ("outcome_0", strs(o0.iter().map(|s| s.as_deref()))),
        ("outcome_1", strs(o1.iter().map(|s| s.as_deref()))),
        ("token_0", strs(t0.iter().map(|s| s.as_deref()))),
        ("token_1", strs(t1.iter().map(|s| s.as_deref()))),
        (
            "game_start_time",
            ms(m.iter().map(|l| l.rec.game_start_time.as_deref().and_then(parse_time))),
        ),
        ("game_id", i64s(m.iter().map(|l| l.rec.game_id))),
        ("tick_size", f64s(m.iter().map(|l| tick(&l.rec.tick_size)))),
        ("min_order_size", f64s(m.iter().map(|l| l.rec.min_order_size))),
        ("neg_risk", bools(m.iter().map(|l| l.rec.neg_risk))),
        ("active", bools(m.iter().map(|l| l.rec.active))),
        ("closed", bools(m.iter().map(|l| l.rec.closed))),
        ("accepting_orders", bools(m.iter().map(|l| l.rec.accepting_orders))),
        ("tracked", bools(m.iter().map(|l| l.rec.tracked))),
        ("first_seen", ns(m.iter().map(|l| Some(l.first)))),
        ("last_seen", ns(m.iter().map(|l| Some(l.t)))),
    ])
}

fn tokens_batch(m: &[Latest<MarketRec>]) -> RecordBatch {
    struct Tok<'a> {
        token: &'a str,
        idx: i32,
        outcome: Option<&'a str>,
        l: &'a Latest<MarketRec>,
    }
    let toks: Vec<Tok> = m
        .iter()
        .flat_map(|l| {
            l.rec.token_ids.iter().enumerate().map(move |(i, t)| Tok {
                token: t,
                idx: i as i32,
                outcome: l.rec.outcomes.get(i).map(|s| s.as_str()),
                l,
            })
        })
        .collect();
    batch(vec![
        ("asset_id", strs(toks.iter().map(|t| Some(t.token)))),
        ("market", strs(toks.iter().map(|t| Some(t.l.rec.condition_id.as_str())))),
        ("outcome", strs(toks.iter().map(|t| t.outcome))),
        (
            "outcome_index",
            Arc::new(Int32Array::from_iter_values(toks.iter().map(|t| t.idx))),
        ),
        ("category", strs(toks.iter().map(|t| Some(t.l.category.as_str())))),
        ("league", strs(toks.iter().map(|t| Some(t.l.league.as_str())))),
        ("event_slug", strs(toks.iter().map(|t| t.l.rec.event_slug.as_deref()))),
        ("market_type", strs(toks.iter().map(|t| t.l.rec.market_type.as_deref()))),
        ("question", strs(toks.iter().map(|t| t.l.rec.question.as_deref()))),
    ])
}

fn events_batch(e: &[Latest<EventRec>]) -> RecordBatch {
    let tags: Vec<String> = e
        .iter()
        .map(|l| {
            let t: Vec<&str> = l.rec.tags.iter().filter_map(|t| t.slug.as_deref()).collect();
            t.join(",")
        })
        .collect();
    batch(vec![
        ("event_id", strs(e.iter().map(|l| Some(l.rec.id.as_str())))),
        ("slug", strs(e.iter().map(|l| l.rec.slug.as_deref()))),
        ("title", strs(e.iter().map(|l| l.rec.title.as_deref()))),
        ("category", strs(e.iter().map(|l| Some(l.category.as_str())))),
        ("league", strs(e.iter().map(|l| Some(l.league.as_str())))),
        ("series_slug", strs(e.iter().map(|l| l.rec.series_slug.as_deref()))),
        ("tags", strs(tags.iter().map(|s| Some(s.as_str())))),
        (
            "start_time",
            ms(e.iter().map(|l| l.rec.start_time.as_deref().and_then(parse_time))),
        ),
        (
            "end_date",
            ms(e.iter().map(|l| l.rec.end_date.as_deref().and_then(parse_time))),
        ),
        ("game_id", i64s(e.iter().map(|l| l.rec.game_id))),
        (
            "n_markets",
            Arc::new(UInt32Array::from_iter_values(
                e.iter().map(|l| l.rec.markets.len() as u32),
            )),
        ),
        ("active", bools(e.iter().map(|l| l.rec.active))),
        ("closed", bools(e.iter().map(|l| l.rec.closed))),
        ("live", bools(e.iter().map(|l| l.rec.live))),
        ("ended", bools(e.iter().map(|l| l.rec.ended))),
        ("score", strs(e.iter().map(|l| l.rec.score.as_deref()))),
        ("period", strs(e.iter().map(|l| l.rec.period.as_deref()))),
        ("volume", f64s(e.iter().map(|l| l.rec.volume))),
        ("volume_24h", f64s(e.iter().map(|l| l.rec.volume24hr))),
        ("liquidity", f64s(e.iter().map(|l| l.rec.liquidity))),
        ("open_interest", f64s(e.iter().map(|l| l.rec.open_interest))),
        ("first_seen", ns(e.iter().map(|l| Some(l.first)))),
        ("last_seen", ns(e.iter().map(|l| Some(l.t)))),
    ])
}

fn sessions_batch(s: &[((i64, u64), Conn)]) -> RecordBatch {
    let fams: Vec<&str> = s.iter().map(|(_, c)| crate::feed_family(&c.feed)).collect();
    batch(vec![
        ("run", i64s(s.iter().map(|(k, _)| Some(k.0)))),
        (
            "session",
            Arc::new(UInt64Array::from_iter_values(s.iter().map(|(k, _)| k.1))),
        ),
        ("feed", strs(s.iter().map(|(_, c)| Some(c.feed.as_str())))),
        ("feed_family", strs(fams.iter().map(|f| Some(*f)))),
        ("connected", ns(s.iter().map(|(_, c)| c.connected))),
        ("disconnected", ns(s.iter().map(|(_, c)| c.disconnected))),
        ("reason", strs(s.iter().map(|(_, c)| c.reason.as_deref()))),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_gamma_times() {
        let want = Some(1_792_245_600_000); // 2026-10-17T14:00:00Z
        for s in [
            "2026-10-17T14:00:00Z",
            "2026-10-17 14:00:00+00",
            "2026-10-17 14:00:00+00:00",
        ] {
            assert_eq!(parse_time(s), want, "{s}");
        }
        assert_eq!(parse_time("soon"), None);
    }
}
