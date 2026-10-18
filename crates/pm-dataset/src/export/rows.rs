//! Time-series tables: parse recorded payloads into compact rows, then build Arrow batches.
//!
//! Rows hold strings as ids into a per-unit [`Strings`] table, so a whole hour of a busy league
//! fits in memory for sorting; strings are only materialised per batch.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
};
use serde::Deserialize;
use serde::de::{self, Deserializer, Visitor};

/// Rows per Arrow batch (and so the granularity of string materialisation).
const BATCH: usize = 128 * 1024;

/// The time-series tables, each fed by one recorded stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Table {
    BookUpdates,
    TickSizeChanges,
    Bbo,
    ClobTrades,
    WalletTrades,
    BookSnapshots,
    Scores,
}

impl Table {
    pub const ALL: [Table; 7] = [
        Table::BookUpdates,
        Table::TickSizeChanges,
        Table::Bbo,
        Table::ClobTrades,
        Table::WalletTrades,
        Table::BookSnapshots,
        Table::Scores,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Table::BookUpdates => "book_updates",
            Table::TickSizeChanges => "tick_size_changes",
            Table::Bbo => "bbo",
            Table::ClobTrades => "clob_trades",
            Table::WalletTrades => "wallet_trades",
            Table::BookSnapshots => "book_snapshots",
            Table::Scores => "scores",
        }
    }

    pub fn stream(self) -> &'static str {
        match self {
            Table::BookUpdates | Table::TickSizeChanges => "clob_book",
            Table::Bbo => "clob_bbo",
            Table::ClobTrades => "clob_trades",
            Table::WalletTrades => "trades",
            Table::BookSnapshots => "book_snapshots",
            Table::Scores => "scores",
        }
    }

    pub fn from_name(name: &str) -> Option<Table> {
        Table::ALL.into_iter().find(|t| t.name() == name)
    }
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Interned strings; id `NONE` stands for a missing value.
#[derive(Default)]
pub struct Strings {
    map: HashMap<Box<str>, u32>,
    list: Vec<Box<str>>,
}

const NONE: u32 = u32::MAX;

impl Strings {
    fn id(&mut self, s: &str) -> u32 {
        if let Some(&i) = self.map.get(s) {
            return i;
        }
        let i = self.list.len() as u32;
        self.list.push(s.into());
        self.map.insert(s.into(), i);
        i
    }

    fn opt(&mut self, s: Option<&str>) -> u32 {
        s.filter(|s| !s.is_empty()).map_or(NONE, |s| self.id(s))
    }

    fn get(&self, id: u32) -> Option<&str> {
        (id != NONE).then(|| &*self.list[id as usize])
    }
}

/// A number sent either as a JSON number or as a string (`"0.48"`, `"1792222399355"`); an empty
/// string or null is `None`.
#[derive(Clone, Copy, Debug, Default)]
struct Num(Option<f64>);

impl<'de> Deserialize<'de> for Num {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Num, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Num;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a number or numeric string")
            }
            fn visit_str<E: de::Error>(self, s: &str) -> Result<Num, E> {
                if s.is_empty() {
                    return Ok(Num(None));
                }
                s.parse().map(|x| Num(Some(x))).map_err(E::custom)
            }
            fn visit_f64<E>(self, x: f64) -> Result<Num, E> {
                Ok(Num(Some(x)))
            }
            fn visit_i64<E>(self, x: i64) -> Result<Num, E> {
                Ok(Num(Some(x as f64)))
            }
            fn visit_u64<E>(self, x: u64) -> Result<Num, E> {
                Ok(Num(Some(x as f64)))
            }
            fn visit_unit<E>(self) -> Result<Num, E> {
                Ok(Num(None))
            }
            fn visit_none<E>(self) -> Result<Num, E> {
                Ok(Num(None))
            }
            fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Num, D::Error> {
                Num::deserialize(d)
            }
        }
        d.deserialize_any(V)
    }
}

impl Num {
    fn ms(self) -> Option<i64> {
        self.0.map(|x| x as i64)
    }
}

// ---- payload probes (only the fields we export) ----

/// Every CLOB market-channel event type in one flat struct (one parse per record).
#[derive(Deserialize)]
struct Clob<'a> {
    #[serde(borrow)]
    event_type: Cow<'a, str>,
    #[serde(borrow, default)]
    market: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    asset_id: Option<Cow<'a, str>>,
    #[serde(default)]
    timestamp: Num,
    #[serde(default, alias = "buys")]
    bids: Vec<Level>,
    #[serde(default, alias = "sells")]
    asks: Vec<Level>,
    #[serde(borrow, default)]
    price_changes: Vec<Change<'a>>,
    #[serde(default)]
    price: Num,
    #[serde(default)]
    size: Num,
    #[serde(borrow, default)]
    side: Option<Cow<'a, str>>,
    #[serde(default)]
    fee_rate_bps: Num,
    #[serde(borrow, default)]
    transaction_hash: Option<Cow<'a, str>>,
    #[serde(default)]
    best_bid: Num,
    #[serde(default)]
    best_ask: Num,
    #[serde(default)]
    spread: Num,
    #[serde(default)]
    old_tick_size: Num,
    #[serde(default)]
    new_tick_size: Num,
}

#[derive(Deserialize)]
struct Level {
    price: Num,
    size: Num,
}

#[derive(Deserialize)]
struct Change<'a> {
    #[serde(borrow)]
    asset_id: Cow<'a, str>,
    price: Num,
    size: Num,
    #[serde(borrow)]
    side: Cow<'a, str>,
    #[serde(default)]
    best_bid: Num,
    #[serde(default)]
    best_ask: Num,
}

#[derive(Deserialize)]
struct Rtds<'a> {
    /// Server send time, ms.
    #[serde(default)]
    timestamp: Num,
    #[serde(borrow)]
    payload: RtdsTrade<'a>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RtdsTrade<'a> {
    #[serde(borrow)]
    asset: Cow<'a, str>,
    #[serde(borrow)]
    condition_id: Cow<'a, str>,
    #[serde(borrow, default)]
    event_slug: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    slug: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    title: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    outcome: Option<Cow<'a, str>>,
    #[serde(default)]
    outcome_index: Option<i32>,
    price: Num,
    size: Num,
    #[serde(borrow)]
    side: Cow<'a, str>,
    #[serde(borrow, default)]
    proxy_wallet: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    name: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    pseudonym: Option<Cow<'a, str>>,
    /// Chain time, seconds.
    #[serde(default)]
    timestamp: Num,
    #[serde(borrow, default)]
    transaction_hash: Option<Cow<'a, str>>,
}

#[derive(Deserialize)]
struct Snapshot<'a> {
    #[serde(borrow)]
    asset_id: Cow<'a, str>,
    #[serde(borrow)]
    market: Cow<'a, str>,
    #[serde(default)]
    ts: Num,
    #[serde(default)]
    bids: Vec<(Num, Num)>,
    #[serde(default)]
    asks: Vec<(Num, Num)>,
    #[serde(default)]
    n_bids: Option<u32>,
    #[serde(default)]
    n_asks: Option<u32>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Score<'a> {
    #[serde(default)]
    game_id: Option<i64>,
    /// Used instead of `gameId` by some leagues (cricket).
    #[serde(borrow, default)]
    metadata_game_id: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    league_abbreviation: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    home_team: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    away_team: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    status: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    score: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    period: Option<Cow<'a, str>>,
    #[serde(default)]
    live: Option<bool>,
    #[serde(default)]
    ended: Option<bool>,
}

// ---- rows ----

/// Book side: `bid` / `ask`. Taker side: `BUY` / `SELL`. Stored as a small code.
const BID: u8 = 1;
const ASK: u8 = 2;

fn book_side(s: &str) -> u8 {
    match s {
        "BUY" | "buy" => BID,
        "SELL" | "sell" => ASK,
        _ => 0,
    }
}

struct BookRow {
    t: i64,
    session: u64,
    ts: Option<i64>,
    market: u32,
    asset: u32,
    snapshot: bool,
    side: u8,
    price: Option<f64>,
    size: Option<f64>,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
}

struct TickRow {
    t: i64,
    session: u64,
    ts: Option<i64>,
    market: u32,
    asset: u32,
    old: Option<f64>,
    new: Option<f64>,
}

struct BboRow {
    t: i64,
    session: u64,
    ts: Option<i64>,
    market: u32,
    asset: u32,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    spread: Option<f64>,
}

struct ClobTradeRow {
    t: i64,
    session: u64,
    ts: Option<i64>,
    market: u32,
    asset: u32,
    price: Option<f64>,
    size: Option<f64>,
    side: u8,
    fee_rate_bps: Option<f64>,
    tx: u32,
}

struct WalletTradeRow {
    t: i64,
    session: u64,
    ts: Option<i64>,
    chain_ts: Option<i64>,
    market: u32,
    asset: u32,
    event_slug: u32,
    slug: u32,
    title: u32,
    outcome: u32,
    outcome_index: Option<i32>,
    price: Option<f64>,
    size: Option<f64>,
    side: u8,
    wallet: u32,
    name: u32,
    pseudonym: u32,
    tx: u32,
}

struct SnapRow {
    t: i64,
    ts: Option<i64>,
    market: u32,
    asset: u32,
    side: u8,
    level: Option<u16>,
    price: Option<f64>,
    size: Option<f64>,
    n_bids: Option<u32>,
    n_asks: Option<u32>,
}

struct ScoreRow {
    t: i64,
    session: u64,
    game_id: Option<i64>,
    metadata_game_id: u32,
    league: u32,
    home: u32,
    away: u32,
    status: u32,
    score: u32,
    period: u32,
    live: Option<bool>,
    ended: Option<bool>,
}

/// The rows of one unit (one stream, day and league) for the hour being processed.
pub struct Rows {
    wanted: Vec<Table>,
    pub strings: Strings,
    book: Vec<BookRow>,
    tick: Vec<TickRow>,
    bbo: Vec<BboRow>,
    ctrade: Vec<ClobTradeRow>,
    wtrade: Vec<WalletTradeRow>,
    snap: Vec<SnapRow>,
    score: Vec<ScoreRow>,
}

impl Rows {
    pub fn new(wanted: Vec<Table>) -> Self {
        Rows {
            wanted,
            strings: Strings::default(),
            book: vec![],
            tick: vec![],
            bbo: vec![],
            ctrade: vec![],
            wtrade: vec![],
            snap: vec![],
            score: vec![],
        }
    }

    fn wants(&self, t: Table) -> bool {
        self.wanted.contains(&t)
    }

    /// Parses one record of `stream`. Returns false if the payload did not have the expected
    /// shape (counted by the caller, never fatal).
    pub fn add(&mut self, stream: &str, t: i64, session: u64, json: &str) -> bool {
        match stream {
            "clob_book" | "clob_bbo" | "clob_trades" => match serde_json::from_str::<Clob>(json) {
                Ok(m) => {
                    self.add_clob(t, session, m);
                    true
                }
                Err(_) => false,
            },
            "trades" => match serde_json::from_str::<Rtds>(json) {
                Ok(m) => {
                    self.add_wallet_trade(t, session, m);
                    true
                }
                Err(_) => false,
            },
            "book_snapshots" => match serde_json::from_str::<Snapshot>(json) {
                Ok(m) => {
                    self.add_snapshot(t, m);
                    true
                }
                Err(_) => false,
            },
            "scores" => match serde_json::from_str::<Score>(json) {
                Ok(m) => {
                    self.add_score(t, session, m);
                    true
                }
                Err(_) => false,
            },
            _ => false,
        }
    }

    fn add_clob(&mut self, t: i64, session: u64, m: Clob) {
        let s = &mut self.strings;
        let market = s.opt(m.market.as_deref());
        let ts = m.timestamp.ms();
        match &*m.event_type {
            "book" if self.wanted.contains(&Table::BookUpdates) => {
                let asset = s.opt(m.asset_id.as_deref());
                let row = |side, price: Option<f64>, size: Option<f64>| BookRow {
                    t,
                    session,
                    ts,
                    market,
                    asset,
                    snapshot: true,
                    side,
                    price,
                    size,
                    best_bid: None,
                    best_ask: None,
                };
                if m.bids.is_empty() && m.asks.is_empty() {
                    // An empty snapshot still resets the book: keep it as one level-less row.
                    self.book.push(row(0, None, None));
                }
                for l in &m.bids {
                    self.book.push(row(BID, l.price.0, l.size.0));
                }
                for l in &m.asks {
                    self.book.push(row(ASK, l.price.0, l.size.0));
                }
            }
            "price_change" if self.wanted.contains(&Table::BookUpdates) => {
                for c in &m.price_changes {
                    let asset = s.id(&c.asset_id);
                    self.book.push(BookRow {
                        t,
                        session,
                        ts,
                        market,
                        asset,
                        snapshot: false,
                        side: book_side(&c.side),
                        price: c.price.0,
                        size: c.size.0,
                        best_bid: c.best_bid.0,
                        best_ask: c.best_ask.0,
                    });
                }
            }
            "tick_size_change" if self.wanted.contains(&Table::TickSizeChanges) => self.tick.push(TickRow {
                t,
                session,
                ts,
                market,
                asset: s.opt(m.asset_id.as_deref()),
                old: m.old_tick_size.0,
                new: m.new_tick_size.0,
            }),
            "best_bid_ask" if self.wanted.contains(&Table::Bbo) => self.bbo.push(BboRow {
                t,
                session,
                ts,
                market,
                asset: s.opt(m.asset_id.as_deref()),
                best_bid: m.best_bid.0,
                best_ask: m.best_ask.0,
                spread: m.spread.0,
            }),
            "last_trade_price" if self.wanted.contains(&Table::ClobTrades) => self.ctrade.push(ClobTradeRow {
                t,
                session,
                ts,
                market,
                asset: s.opt(m.asset_id.as_deref()),
                price: m.price.0,
                size: m.size.0,
                side: m.side.as_deref().map_or(0, book_side),
                fee_rate_bps: m.fee_rate_bps.0,
                tx: s.opt(m.transaction_hash.as_deref()),
            }),
            _ => {}
        }
    }

    fn add_wallet_trade(&mut self, t: i64, session: u64, m: Rtds) {
        if !self.wants(Table::WalletTrades) {
            return;
        }
        let s = &mut self.strings;
        let p = m.payload;
        self.wtrade.push(WalletTradeRow {
            t,
            session,
            ts: m.timestamp.ms(),
            chain_ts: p.timestamp.0.map(|x| x as i64),
            market: s.id(&p.condition_id),
            asset: s.id(&p.asset),
            event_slug: s.opt(p.event_slug.as_deref()),
            slug: s.opt(p.slug.as_deref()),
            title: s.opt(p.title.as_deref()),
            outcome: s.opt(p.outcome.as_deref()),
            outcome_index: p.outcome_index,
            price: p.price.0,
            size: p.size.0,
            side: book_side(&p.side),
            wallet: s.opt(p.proxy_wallet.as_deref()),
            name: s.opt(p.name.as_deref()),
            pseudonym: s.opt(p.pseudonym.as_deref()),
            tx: s.opt(p.transaction_hash.as_deref()),
        });
    }

    fn add_snapshot(&mut self, t: i64, m: Snapshot) {
        if !self.wants(Table::BookSnapshots) {
            return;
        }
        let s = &mut self.strings;
        let (market, asset, ts) = (s.id(&m.market), s.id(&m.asset_id), m.ts.ms());
        let row = |side, level: Option<u16>, (p, z): (Num, Num)| SnapRow {
            t,
            ts,
            market,
            asset,
            side,
            level,
            price: p.0,
            size: z.0,
            n_bids: m.n_bids,
            n_asks: m.n_asks,
        };
        if m.bids.is_empty() && m.asks.is_empty() {
            self.snap.push(row(0, None, (Num(None), Num(None))));
        }
        for (i, l) in m.bids.iter().enumerate() {
            self.snap.push(row(BID, Some(i as u16), *l));
        }
        for (i, l) in m.asks.iter().enumerate() {
            self.snap.push(row(ASK, Some(i as u16), *l));
        }
    }

    fn add_score(&mut self, t: i64, session: u64, m: Score) {
        if !self.wants(Table::Scores) {
            return;
        }
        let s = &mut self.strings;
        self.score.push(ScoreRow {
            t,
            session,
            game_id: m.game_id,
            metadata_game_id: s.opt(m.metadata_game_id.as_deref()),
            league: s.opt(m.league_abbreviation.as_deref()),
            home: s.opt(m.home_team.as_deref()),
            away: s.opt(m.away_team.as_deref()),
            status: s.opt(m.status.as_deref()),
            score: s.opt(m.score.as_deref()),
            period: s.opt(m.period.as_deref()),
            live: m.live,
            ended: m.ended,
        });
    }

    /// Sorts every table by receive time (stable: rows of one record keep their order).
    pub fn sort(&mut self) {
        fn by_t<R>(v: &mut [R], t: impl Fn(&R) -> i64) {
            if !v.is_sorted_by_key(&t) {
                v.sort_by_key(t);
            }
        }
        by_t(&mut self.book, |r| r.t);
        by_t(&mut self.tick, |r| r.t);
        by_t(&mut self.bbo, |r| r.t);
        by_t(&mut self.ctrade, |r| r.t);
        by_t(&mut self.wtrade, |r| r.t);
        by_t(&mut self.snap, |r| r.t);
        by_t(&mut self.score, |r| r.t);
    }

    /// Drains the rows into Arrow batches of at most [`BATCH`] rows, per table.
    pub fn drain_batches(
        &mut self,
        mut emit: impl FnMut(Table, RecordBatch) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let s = &self.strings;
        for c in self.book.chunks(BATCH) {
            emit(Table::BookUpdates, book_batch(c, s))?;
        }
        for c in self.tick.chunks(BATCH) {
            emit(Table::TickSizeChanges, tick_batch(c, s))?;
        }
        for c in self.bbo.chunks(BATCH) {
            emit(Table::Bbo, bbo_batch(c, s))?;
        }
        for c in self.ctrade.chunks(BATCH) {
            emit(Table::ClobTrades, clob_trade_batch(c, s))?;
        }
        for c in self.wtrade.chunks(BATCH) {
            emit(Table::WalletTrades, wallet_trade_batch(c, s))?;
        }
        for c in self.snap.chunks(BATCH) {
            emit(Table::BookSnapshots, snapshot_batch(c, s))?;
        }
        for c in self.score.chunks(BATCH) {
            emit(Table::Scores, score_batch(c, s))?;
        }
        self.book.clear();
        self.tick.clear();
        self.bbo.clear();
        self.ctrade.clear();
        self.wtrade.clear();
        self.snap.clear();
        self.score.clear();
        Ok(())
    }
}

// ---- Arrow column helpers ----

fn t_col<R>(rows: &[R], t: impl Fn(&R) -> i64) -> ArrayRef {
    Arc::new(TimestampNanosecondArray::from_iter_values(rows.iter().map(t)).with_timezone("UTC"))
}

fn ms_col<R>(rows: &[R], ts: impl Fn(&R) -> Option<i64>) -> ArrayRef {
    Arc::new(TimestampMillisecondArray::from_iter(rows.iter().map(ts)).with_timezone("UTC"))
}

fn u64_col<R>(rows: &[R], f: impl Fn(&R) -> u64) -> ArrayRef {
    Arc::new(UInt64Array::from_iter_values(rows.iter().map(f)))
}

fn f64_col<R>(rows: &[R], f: impl Fn(&R) -> Option<f64>) -> ArrayRef {
    Arc::new(Float64Array::from_iter(rows.iter().map(f)))
}

fn str_col<R>(rows: &[R], s: &Strings, f: impl Fn(&R) -> u32) -> ArrayRef {
    Arc::new(StringArray::from_iter(rows.iter().map(|r| s.get(f(r)))))
}

fn code_col<R>(rows: &[R], f: impl Fn(&R) -> u8, names: [&'static str; 2]) -> ArrayRef {
    Arc::new(StringArray::from_iter(rows.iter().map(|r| match f(r) {
        BID => Some(names[0]),
        ASK => Some(names[1]),
        _ => None,
    })))
}

/// Every column is declared nullable. `RecordBatch::try_from_iter` would infer nullability from
/// each batch's contents, and a Parquet file takes its schema from the first batch: a column
/// without nulls in the first hour would become required, and later nulls would be written as 0.
fn batch(cols: Vec<(&str, ArrayRef)>) -> RecordBatch {
    RecordBatch::try_from_iter_with_nullable(cols.into_iter().map(|(n, a)| (n, a, true)))
        .expect("columns of equal length")
}

const BOOK_SIDE: [&str; 2] = ["bid", "ask"];
const TAKER_SIDE: [&str; 2] = ["BUY", "SELL"];

fn book_batch(r: &[BookRow], s: &Strings) -> RecordBatch {
    batch(vec![
        ("t", t_col(r, |r| r.t)),
        ("session", u64_col(r, |r| r.session)),
        ("ts", ms_col(r, |r| r.ts)),
        ("market", str_col(r, s, |r| r.market)),
        ("asset_id", str_col(r, s, |r| r.asset)),
        (
            "kind",
            Arc::new(StringArray::from_iter_values(
                r.iter().map(|r| if r.snapshot { "snapshot" } else { "change" }),
            )),
        ),
        ("side", code_col(r, |r| r.side, BOOK_SIDE)),
        ("price", f64_col(r, |r| r.price)),
        ("size", f64_col(r, |r| r.size)),
        ("best_bid", f64_col(r, |r| r.best_bid)),
        ("best_ask", f64_col(r, |r| r.best_ask)),
    ])
}

fn tick_batch(r: &[TickRow], s: &Strings) -> RecordBatch {
    batch(vec![
        ("t", t_col(r, |r| r.t)),
        ("session", u64_col(r, |r| r.session)),
        ("ts", ms_col(r, |r| r.ts)),
        ("market", str_col(r, s, |r| r.market)),
        ("asset_id", str_col(r, s, |r| r.asset)),
        ("old_tick", f64_col(r, |r| r.old)),
        ("new_tick", f64_col(r, |r| r.new)),
    ])
}

fn bbo_batch(r: &[BboRow], s: &Strings) -> RecordBatch {
    batch(vec![
        ("t", t_col(r, |r| r.t)),
        ("session", u64_col(r, |r| r.session)),
        ("ts", ms_col(r, |r| r.ts)),
        ("market", str_col(r, s, |r| r.market)),
        ("asset_id", str_col(r, s, |r| r.asset)),
        ("best_bid", f64_col(r, |r| r.best_bid)),
        ("best_ask", f64_col(r, |r| r.best_ask)),
        ("spread", f64_col(r, |r| r.spread)),
    ])
}

fn clob_trade_batch(r: &[ClobTradeRow], s: &Strings) -> RecordBatch {
    batch(vec![
        ("t", t_col(r, |r| r.t)),
        ("session", u64_col(r, |r| r.session)),
        ("ts", ms_col(r, |r| r.ts)),
        ("market", str_col(r, s, |r| r.market)),
        ("asset_id", str_col(r, s, |r| r.asset)),
        ("price", f64_col(r, |r| r.price)),
        ("size", f64_col(r, |r| r.size)),
        ("side", code_col(r, |r| r.side, TAKER_SIDE)),
        ("fee_rate_bps", f64_col(r, |r| r.fee_rate_bps)),
        ("tx_hash", str_col(r, s, |r| r.tx)),
    ])
}

fn wallet_trade_batch(r: &[WalletTradeRow], s: &Strings) -> RecordBatch {
    batch(vec![
        ("t", t_col(r, |r| r.t)),
        ("session", u64_col(r, |r| r.session)),
        ("ts", ms_col(r, |r| r.ts)),
        (
            "chain_ts",
            Arc::new(TimestampSecondArray::from_iter(r.iter().map(|r| r.chain_ts)).with_timezone("UTC")),
        ),
        ("market", str_col(r, s, |r| r.market)),
        ("asset_id", str_col(r, s, |r| r.asset)),
        ("event_slug", str_col(r, s, |r| r.event_slug)),
        ("slug", str_col(r, s, |r| r.slug)),
        ("title", str_col(r, s, |r| r.title)),
        ("outcome", str_col(r, s, |r| r.outcome)),
        (
            "outcome_index",
            Arc::new(Int32Array::from_iter(r.iter().map(|r| r.outcome_index))),
        ),
        ("price", f64_col(r, |r| r.price)),
        ("size", f64_col(r, |r| r.size)),
        ("notional", f64_col(r, |r| Some(r.price? * r.size?))),
        ("side", code_col(r, |r| r.side, TAKER_SIDE)),
        ("wallet", str_col(r, s, |r| r.wallet)),
        ("name", str_col(r, s, |r| r.name)),
        ("pseudonym", str_col(r, s, |r| r.pseudonym)),
        ("tx_hash", str_col(r, s, |r| r.tx)),
    ])
}

fn snapshot_batch(r: &[SnapRow], s: &Strings) -> RecordBatch {
    batch(vec![
        ("t", t_col(r, |r| r.t)),
        ("ts", ms_col(r, |r| r.ts)),
        ("market", str_col(r, s, |r| r.market)),
        ("asset_id", str_col(r, s, |r| r.asset)),
        ("side", code_col(r, |r| r.side, BOOK_SIDE)),
        ("level", Arc::new(UInt16Array::from_iter(r.iter().map(|r| r.level)))),
        ("price", f64_col(r, |r| r.price)),
        ("size", f64_col(r, |r| r.size)),
        ("n_bids", Arc::new(UInt32Array::from_iter(r.iter().map(|r| r.n_bids)))),
        ("n_asks", Arc::new(UInt32Array::from_iter(r.iter().map(|r| r.n_asks)))),
    ])
}

fn score_batch(r: &[ScoreRow], s: &Strings) -> RecordBatch {
    batch(vec![
        ("t", t_col(r, |r| r.t)),
        ("session", u64_col(r, |r| r.session)),
        ("game_id", Arc::new(Int64Array::from_iter(r.iter().map(|r| r.game_id)))),
        ("metadata_game_id", str_col(r, s, |r| r.metadata_game_id)),
        ("league_abbr", str_col(r, s, |r| r.league)),
        ("home", str_col(r, s, |r| r.home)),
        ("away", str_col(r, s, |r| r.away)),
        ("status", str_col(r, s, |r| r.status)),
        ("score", str_col(r, s, |r| r.score)),
        ("period", str_col(r, s, |r| r.period)),
        ("live", Arc::new(BooleanArray::from_iter(r.iter().map(|r| r.live)))),
        ("ended", Arc::new(BooleanArray::from_iter(r.iter().map(|r| r.ended)))),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_clob_event_type() {
        let mut rows = Rows::new(Table::ALL.to_vec());
        let book = r#"{"market":"0xm","asset_id":"11","timestamp":"1000","bids":[{"price":"0.01","size":"5"}],"asks":[{"price":"0.99","size":"7"},{"price":"0.98","size":"1"}],"event_type":"book"}"#;
        let empty = r#"{"market":"0xm","asset_id":"11","timestamp":"1001","bids":[],"asks":[],"event_type":"book"}"#;
        let pc = r#"{"market":"0xm","timestamp":"1002","event_type":"price_change","price_changes":[{"asset_id":"11","price":"0.02","size":"3","side":"BUY","best_bid":"0.02","best_ask":"0.98"},{"asset_id":"22","price":"0.98","size":"0","side":"SELL"}]}"#;
        let bbo = r#"{"market":"0xm","asset_id":"11","best_bid":"0.06","best_ask":"","spread":"0.85","timestamp":"1003","event_type":"best_bid_ask"}"#;
        let tr = r#"{"market":"0xm","asset_id":"11","price":"0.48","size":"173","fee_rate_bps":"0","side":"BUY","timestamp":"1004","event_type":"last_trade_price","transaction_hash":"0xt"}"#;
        let tick = r#"{"market":"0xm","asset_id":"11","old_tick_size":"0.01","new_tick_size":"0.001","timestamp":"1005","event_type":"tick_size_change"}"#;
        for (i, j) in [book, empty, pc, tick].iter().enumerate() {
            assert!(rows.add("clob_book", 10 + i as i64, 1, j), "{j}");
        }
        assert!(rows.add("clob_bbo", 20, 1, bbo));
        assert!(rows.add("clob_trades", 30, 1, tr));
        assert!(!rows.add("clob_book", 40, 1, "{\"no\":1}"));

        assert_eq!(rows.book.len(), 3 + 1 + 2);
        assert_eq!(rows.book[3].side, 0); // empty snapshot marker
        assert_eq!((rows.book[4].side, rows.book[5].side), (BID, ASK));
        assert_eq!(rows.bbo[0].best_ask, None);
        assert_eq!(rows.ctrade[0].price, Some(0.48));
        assert_eq!(rows.tick[0].new, Some(0.001));

        let mut got = Vec::new();
        rows.drain_batches(|t, b| {
            got.push((t, b.num_rows(), b.num_columns()));
            Ok(())
        })
        .unwrap();
        assert_eq!(got[0], (Table::BookUpdates, 6, 11));
        assert!(got.contains(&(Table::Bbo, 1, 8)));
        assert!(rows.book.is_empty());
    }

    #[test]
    fn parses_trades_snapshots_scores_and_only_wanted_tables() {
        let mut rows = Rows::new(vec![Table::WalletTrades, Table::Scores, Table::BookSnapshots]);
        let rtds = r#"{"connection_id":"x","payload":{"asset":"11","conditionId":"0xm","eventSlug":"e","outcome":"Heroic","outcomeIndex":1,"price":0.52,"proxyWallet":"0xw","side":"BUY","size":40,"slug":"s","timestamp":1792222899,"title":"A \"B\"","transactionHash":"0xt"},"timestamp":1792222899110,"topic":"activity","type":"trades"}"#;
        assert!(rows.add("trades", 1, 2, rtds));
        assert_eq!(
            rows.wtrade[0].price.zip(rows.wtrade[0].size).map(|(p, s)| p * s),
            Some(20.8)
        );
        assert_eq!(rows.strings.get(rows.wtrade[0].title), Some("A \"B\""));

        let snap = r#"{"asks":[["0.96","15.16"]],"asset_id":"11","bids":[],"market":"0xm","n_asks":1,"n_bids":0,"ts":1792222867230}"#;
        assert!(rows.add("book_snapshots", 2, 0, snap));
        assert_eq!((rows.snap[0].side, rows.snap[0].level), (ASK, Some(0)));

        let cricket = r#"{"metadataGameId":"id27","leagueAbbreviation":"cricket","score":"65-1","period":"Live","live":true,"ended":false}"#;
        assert!(rows.add("scores", 3, 2, cricket));
        assert_eq!(rows.score[0].game_id, None);

        // Not wanted: parsed (shape is fine) but no rows.
        let bbo = r#"{"market":"0xm","asset_id":"11","best_bid":"0.06","best_ask":"0.9","timestamp":"1","event_type":"best_bid_ask"}"#;
        assert!(rows.add("clob_bbo", 4, 1, bbo));
        assert!(rows.bbo.is_empty());
    }
}
