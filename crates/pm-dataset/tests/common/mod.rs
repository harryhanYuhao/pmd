//! Builds recorded-data trees in the `pm-store` layout for integration tests.

#![allow(dead_code)]

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

/// 2026-10-17 00:00:00 UTC, ns.
pub const DAY0: i64 = 1_792_195_200 * 1_000_000_000;
pub const SEC: i64 = 1_000_000_000;

/// 2026-10-17 h:m:s UTC in ns.
pub fn at(h: i64, m: i64, s: i64) -> i64 {
    DAY0 + ((h * 60 + m) * 60 + s) * SEC
}

/// A temporary data root, removed on drop.
pub struct Fixture {
    pub base: PathBuf,
    pub root: PathBuf,
}

impl Fixture {
    pub fn new(name: &str) -> Fixture {
        // Tests run in parallel: every fixture gets its own directory.
        static N: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let base = std::env::temp_dir().join(format!("pm-dataset-it-{name}-{}-{n}", std::process::id()));
        let _ = fs::remove_dir_all(&base);
        let root = base.join("live");
        fs::create_dir_all(&root).unwrap();
        Fixture { base, root }
    }

    pub fn out(&self) -> PathBuf {
        self.base.join("parquet")
    }

    /// Path of `<root>/<date>/<part>/<stream>/<HH>.r<run>.jsonl.zst`; `date` like `2026/10/17`.
    pub fn path(&self, date: &str, part: &str, stream: &str, hour: u32, run: i64) -> PathBuf {
        self.root
            .join(date)
            .join(part)
            .join(stream)
            .join(format!("{hour:02}.r{run}.jsonl.zst"))
    }

    /// Writes one complete zstd frame of records (replacing the file).
    pub fn write(&self, date: &str, part: &str, stream: &str, hour: u32, run: i64, recs: &[String]) -> PathBuf {
        let p = self.path(date, part, stream, hour, run);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(&p, frame(recs)).unwrap();
        p
    }

    /// Records on 2026-10-17; the hour is taken from the first record.
    pub fn day(&self, part: &str, stream: &str, run: i64, recs: &[(i64, u64, &str)]) -> PathBuf {
        let hour = ((recs[0].0 - DAY0) / (3600 * SEC)) as u32;
        let lines: Vec<String> = recs.iter().map(|(t, s, m)| rec(*t, *s, m)).collect();
        self.write("2026/10/17", part, stream, hour, run, &lines)
    }

    pub fn raw(&self, rel: &str, bytes: &[u8]) -> PathBuf {
        let p = self.root.join(rel);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(&p, bytes).unwrap();
        p
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.base);
    }
}

pub fn rec(t: i64, s: u64, m: &str) -> String {
    format!("{{\"t\":{t},\"s\":{s},\"m\":{m}}}\n")
}

pub fn frame(lines: &[String]) -> Vec<u8> {
    let mut enc = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
    for l in lines {
        enc.write_all(l.as_bytes()).unwrap();
    }
    enc.finish().unwrap()
}

/// Backdates a file's mtime, so audit does not treat it as being written right now.
pub fn age(p: &Path) {
    let old = std::time::SystemTime::now() - std::time::Duration::from_secs(3600);
    fs::File::options()
        .write(true)
        .open(p)
        .unwrap()
        .set_modified(old)
        .unwrap();
}

/// Cuts the last `n` bytes off a file (a crash mid-write).
pub fn truncate(p: &Path, n: usize) {
    let b = fs::read(p).unwrap();
    fs::write(p, &b[..b.len() - n]).unwrap();
}

// ---- payloads, shaped like the live feeds ----

pub fn bbo(asset: &str, bid: &str, ask: &str) -> String {
    format!(
        r#"{{"market":"0xm1","asset_id":"{asset}","best_bid":"{bid}","best_ask":"{ask}","spread":"0.01","timestamp":"1","event_type":"best_bid_ask"}}"#
    )
}

pub fn book(asset: &str, bids: &[(&str, &str)], asks: &[(&str, &str)]) -> String {
    let lv = |v: &[(&str, &str)]| {
        v.iter()
            .map(|(p, s)| format!(r#"{{"price":"{p}","size":"{s}"}}"#))
            .collect::<Vec<_>>()
            .join(",")
    };
    format!(
        r#"{{"market":"0xm1","asset_id":"{asset}","timestamp":"1","bids":[{}],"asks":[{}],"event_type":"book"}}"#,
        lv(bids),
        lv(asks)
    )
}

/// One `price_change` entry with the server's top of book after it.
pub fn change(asset: &str, side: &str, price: &str, size: &str, best_bid: &str, best_ask: &str) -> String {
    format!(
        r#"{{"market":"0xm1","timestamp":"2","event_type":"price_change","price_changes":[{{"asset_id":"{asset}","price":"{price}","size":"{size}","side":"{side}","best_bid":"{best_bid}","best_ask":"{best_ask}"}}]}}"#
    )
}

pub fn clob_trade(asset: &str, price: &str, size: &str) -> String {
    format!(
        r#"{{"market":"0xm1","asset_id":"{asset}","price":"{price}","size":"{size}","fee_rate_bps":"0","side":"BUY","timestamp":"3","event_type":"last_trade_price","transaction_hash":"0xt"}}"#
    )
}

pub fn wallet_trade(asset: &str, price: f64, size: f64, wallet: &str, title: &str) -> String {
    format!(
        r#"{{"connection_id":"c","payload":{{"asset":"{asset}","conditionId":"0xm1","eventSlug":"cs2-a-b","outcome":"A","outcomeIndex":0,"price":{price},"proxyWallet":"{wallet}","side":"BUY","size":{size},"slug":"cs2-a-b","timestamp":1792260000,"title":{},"transactionHash":"0xt"}},"timestamp":1792260000123,"topic":"activity","type":"trades"}}"#,
        serde_json::to_string(title).unwrap()
    )
}

pub fn score_numeric(game: i64, score: &str) -> String {
    format!(
        r#"{{"gameId":{game},"leagueAbbreviation":"cs2","homeTeam":"A","awayTeam":"B","status":"running","score":"{score}","period":"1/3","live":true,"ended":false}}"#
    )
}

pub fn score_cricket(id: &str, score: &str) -> String {
    format!(
        r#"{{"metadataGameId":"{id}","leagueAbbreviation":"cricket","score":"{score}","period":"Live","live":true,"ended":false}}"#
    )
}

pub fn market(cond: &str, tokens: [&str; 2], question: &str, tracked: bool) -> String {
    format!(
        r#"{{"accepting_orders":true,"active":true,"closed":false,"condition_id":"{cond}","event_id":"7","event_slug":"cs2-a-b","game_id":5,"game_start_time":"2026-10-17 14:00:00+00","league":"cs2","market_id":"1","market_type":"moneyline","min_order_size":5.0,"neg_risk":false,"outcomes":["A","B"],"question":"{question}","slug":"cs2-a-b","tick_size":"0.01","token_ids":["{}","{}"],"tracked":{tracked}}}"#,
        tokens[0], tokens[1]
    )
}

pub fn gamma_event(id: &str, title: &str, live: bool, volume: f64) -> String {
    format!(
        r#"{{"id":"{id}","slug":"cs2-a-b","title":"{title}","startTime":"2026-10-17T14:00:00Z","endDate":"2026-10-18T14:00:00Z","gameId":5,"active":true,"closed":false,"live":{live},"ended":false,"score":"1-0","period":"2/3","volume":{volume},"volume24hr":10.5,"liquidity":99.0,"markets":[{{}},{{}}],"tags":[{{"slug":"esports"}},{{"slug":"cs2"}}],"description":"long text"}}"#
    )
}

pub fn session(feed: &str, connected: bool, reason: &str) -> String {
    if connected {
        format!(r#"{{"feed":"{feed}","event":"connected"}}"#)
    } else {
        format!(r#"{{"feed":"{feed}","event":"disconnected","reason":"{reason}"}}"#)
    }
}
