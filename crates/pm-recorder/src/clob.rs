//! CLOB market-channel recording.
//!
//! [`ClobManager`] spreads tokens over websocket shards of at most `max_assets_per_conn` and
//! subscribes/unsubscribes dynamically. [`run_handler`] consumes every shard's frames, stores
//! each event, keeps L2 books in memory and writes periodic book snapshots.
//!
//! Books are checked against the top of book the server attaches to every `price_change`. A
//! snapshot that raced a cancel can leave stale levels (pruned on the spot); a book missing a
//! level is resubscribed, which makes the server send a fresh `book`.
//!
//! Stored events are byte-for-byte what the server sent, except that `price_change` messages
//! have their `hash` fields removed unless `keep_hashes = true` (they triple the stream size).
//!
//! Streams written (under the market's `<category>/<league>`; `_system/unknown` if unroutable):
//! - `clob_book`: `book`, `price_change`, `tick_size_change` (everything needed to rebuild books)
//! - `clob_trades`: `last_trade_price`
//! - `clob_bbo`: `best_bid_ask`
//! - `clob_other`: anything else for tracked markets (`market_resolved`, unknown types);
//!   events for untracked markets (the global `new_market` broadcast) are dropped
//! - `book_snapshots`: top-N levels of every book that changed since the previous snapshot
//! - `_system/clob/sessions`: shard connect / disconnect markers

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pm_core::book::BookSet;
use pm_core::wire::clob_ws::{self, MarketMsg};
use pm_core::wire::split_frame;
use pm_core::{Side, TokenId, now_ns};
use pm_store::{Partition, StoreHandle};
use pm_ws::{WsEvent, WsHandle, WsSpec};
use serde_json::json;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::ClobSection;
use crate::registry::{self, SharedRegistry};
use crate::sessions;

struct Shard {
    assets: Arc<Mutex<BTreeSet<TokenId>>>,
    handle: WsHandle,
}

pub struct ClobManager {
    max_per_conn: usize,
    custom_features: bool,
    shards: Vec<Shard>,
    out: mpsc::Sender<WsEvent>,
    cancel: CancellationToken,
}

impl ClobManager {
    pub fn new(cfg: &ClobSection, out: mpsc::Sender<WsEvent>, cancel: CancellationToken) -> Self {
        ClobManager {
            max_per_conn: cfg.max_assets_per_conn.max(1),
            custom_features: cfg.custom_features,
            shards: Vec::new(),
            out,
            cancel,
        }
    }

    pub fn update(&mut self, add: &[TokenId], remove: &[TokenId]) {
        let remove: HashSet<&TokenId> = remove.iter().collect();
        if !remove.is_empty() {
            for s in &self.shards {
                let gone: Vec<TokenId> = {
                    let mut a = s.assets.lock().unwrap();
                    let gone: Vec<TokenId> = a.iter().filter(|t| remove.contains(t)).cloned().collect();
                    for t in &gone {
                        a.remove(t);
                    }
                    gone
                };
                if !gone.is_empty() {
                    let ids: Vec<&str> = gone.iter().map(|t| t.as_str()).collect();
                    s.handle.send(clob_ws::update_subscription_msg(&ids, false));
                }
            }
        }

        let mut pending: Vec<TokenId> = add.to_vec();
        // Fill existing shards first, then open new ones.
        for s in &self.shards {
            if pending.is_empty() {
                break;
            }
            let batch: Vec<TokenId> = {
                let mut a = s.assets.lock().unwrap();
                let room = self.max_per_conn.saturating_sub(a.len());
                let batch: Vec<TokenId> = pending.drain(..room.min(pending.len())).collect();
                a.extend(batch.iter().cloned());
                batch
            };
            if !batch.is_empty() {
                let ids: Vec<&str> = batch.iter().map(|t| t.as_str()).collect();
                s.handle.send(clob_ws::update_subscription_msg(&ids, true));
            }
        }
        while !pending.is_empty() {
            let batch: BTreeSet<TokenId> = pending.drain(..self.max_per_conn.min(pending.len())).collect();
            self.spawn_shard(batch);
        }
    }

    /// Unsubscribes and resubscribes tokens so the server sends fresh `book` snapshots.
    pub fn resync(&self, tokens: &[TokenId]) {
        for s in &self.shards {
            let mine: Vec<&str> = {
                let a = s.assets.lock().unwrap();
                tokens.iter().filter(|t| a.contains(*t)).map(|t| t.as_str()).collect()
            };
            if !mine.is_empty() {
                s.handle.send(clob_ws::update_subscription_msg(&mine, false));
                s.handle.send(clob_ws::update_subscription_msg(&mine, true));
            }
        }
    }

    fn spawn_shard(&mut self, assets: BTreeSet<TokenId>) {
        let assets = Arc::new(Mutex::new(assets));
        let a2 = assets.clone();
        let custom = self.custom_features;
        let on_connect: pm_ws::OnConnect = Arc::new(move || {
            let a = a2.lock().unwrap();
            if a.is_empty() {
                return vec![];
            }
            let ids: Vec<&str> = a.iter().map(|t| t.as_str()).collect();
            vec![clob_ws::subscribe_msg(&ids, custom)]
        });
        let mut spec = WsSpec::new(format!("clob-{}", self.shards.len()), clob_ws::MARKET_WS_URL);
        spec.keepalive = Some((Duration::from_secs(10), "PING".into()));
        spec.ignore = vec!["PONG".into()];
        spec.idle_timeout = Duration::from_secs(45);
        let (handle, _task) = pm_ws::spawn(spec, on_connect, self.out.clone(), self.cancel.clone());
        self.shards.push(Shard { assets, handle });
        tracing::debug!(
            "clob: opened connection {} ({} connections)",
            self.shards.len() - 1,
            self.shards.len()
        );
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }
}

pub type SharedClobManager = Arc<Mutex<ClobManager>>;

#[derive(Default)]
pub struct ClobStats {
    pub frames: u64,
    pub events: u64,
    pub unparsed: u64,
    /// Events not tied to a tracked league (`new_market` is broadcast for all of Polymarket).
    pub dropped: u64,
    /// Price changes after which stale levels were pruned.
    pub pruned: u64,
    /// Price changes revealing a missing level (book resynced afterwards).
    pub missing: u64,
    pub resyncs: u64,
    pub snapshots: u64,
    /// Shards currently connected.
    pub connected: HashSet<Arc<str>>,
}

pub type SharedClobStats = Arc<Mutex<ClobStats>>;

fn stream_of(msg: &MarketMsg) -> &'static str {
    match msg {
        MarketMsg::Book(_) | MarketMsg::PriceChange(_) | MarketMsg::TickSizeChange(_) => "clob_book",
        MarketMsg::LastTradePrice(_) => "clob_trades",
        MarketMsg::BestBidAsk(_) => "clob_bbo",
        _ => "clob_other",
    }
}

/// Resubscribe a token with a broken book at most this often.
const RESYNC_EVERY: Duration = Duration::from_secs(60);

/// Consumes every shard's events until cancelled: stores them, keeps the books and writes
/// snapshots.
pub async fn run_handler(
    mut rx: mpsc::Receiver<WsEvent>,
    store: StoreHandle,
    reg: SharedRegistry,
    mgr: SharedClobManager,
    cfg: ClobSection,
    stats: SharedClobStats,
    cancel: CancellationToken,
) {
    let mut snap = tokio::time::interval(Duration::from_secs(cfg.book_snapshot_secs.max(1)));
    snap.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut resync_tick = tokio::time::interval(Duration::from_secs(5));
    let mut h = Handler {
        store,
        reg,
        mgr,
        cfg,
        stats,
        books: BookSet::default(),
        dirty: HashSet::new(),
        want_resync: HashSet::new(),
        last_resync: HashMap::new(),
        unknown: registry::system("unknown"),
    };
    let mut closing = false;
    loop {
        tokio::select! {
            // `mgr` holds a sender of `rx`, so the channel never closes by itself: on shutdown,
            // close it and drain what is buffered.
            _ = cancel.cancelled(), if !closing => {
                rx.close();
                closing = true;
            }
            ev = rx.recv() => {
                let Some(ev) = ev else { break };
                h.on_event(ev).await;
            }
            _ = resync_tick.tick() => h.resync_due(),
            _ = snap.tick(), if h.cfg.book_snapshot_secs > 0 => h.write_snapshots().await,
        }
    }
}

struct Handler {
    store: StoreHandle,
    reg: SharedRegistry,
    mgr: SharedClobManager,
    cfg: ClobSection,
    stats: SharedClobStats,
    books: BookSet,
    /// Tokens whose book changed since the last snapshot.
    dirty: HashSet<TokenId>,
    /// Tokens whose book is missing a level and needs a fresh snapshot from the server.
    want_resync: HashSet<TokenId>,
    last_resync: HashMap<TokenId, Instant>,
    unknown: Partition,
}

impl Handler {
    async fn on_event(&mut self, ev: WsEvent) {
        if let Some((conn, up)) = sessions::record(&self.store, "clob", &ev).await {
            let connected = &mut self.stats.lock().unwrap().connected;
            if up {
                connected.insert(conn);
            } else {
                connected.remove(&conn);
            }
            return;
        }
        if let WsEvent::Text { session, recv_ns, text } = ev {
            self.on_frame(session, recv_ns, &text).await;
        }
    }

    /// Stores every event of a frame under its market's partition and applies it to the books.
    async fn on_frame(&mut self, session: u64, recv_ns: i64, text: &str) {
        let Some(parts) = split_frame(text) else { return };
        let mut st = ClobStats {
            frames: 1,
            ..Default::default()
        };
        for raw in parts {
            st.events += 1;
            let parsed = serde_json::from_str::<MarketMsg>(raw.get()).ok();
            let part = parsed
                .as_ref()
                .and_then(|m| m.market())
                .and_then(|c| self.reg.read().unwrap().market_partition(c.as_str()));
            let stream = parsed.as_ref().map_or("clob_other", stream_of);
            match (&part, stream) {
                // Events for markets we don't track (the global `new_market` broadcast).
                (None, "clob_other") => st.dropped += 1,
                _ => {
                    let body = match &parsed {
                        Some(MarketMsg::PriceChange(_)) if !self.cfg.keep_hashes => clob_ws::strip_hashes(raw.get()),
                        _ => raw.get().into(),
                    };
                    self.store
                        .write(part.as_ref().unwrap_or(&self.unknown), stream, recv_ns, session, &body)
                        .await;
                }
            }
            let Some(msg) = parsed else {
                st.unparsed += 1;
                continue;
            };
            let r = self.books.apply(&msg);
            st.pruned += r.pruned.len() as u64;
            st.missing += r.missing.len() as u64;
            self.dirty.extend(r.touched);
            self.want_resync.extend(r.missing);
        }
        let mut s = self.stats.lock().unwrap();
        s.frames += st.frames;
        s.events += st.events;
        s.unparsed += st.unparsed;
        s.dropped += st.dropped;
        s.pruned += st.pruned;
        s.missing += st.missing;
    }

    /// Resubscribes tokens whose book is still broken (rate-limited per token).
    fn resync_due(&mut self) {
        // A fresh snapshot may already have fixed the book.
        let books = &self.books;
        self.want_resync
            .retain(|t| books.get(t.as_str()).is_some_and(|b| b.needs_resync));
        let due: Vec<TokenId> = self
            .want_resync
            .iter()
            .filter(|t| self.last_resync.get(*t).is_none_or(|at| at.elapsed() > RESYNC_EVERY))
            .cloned()
            .collect();
        if !due.is_empty() {
            self.mgr.lock().unwrap().resync(&due);
            for t in &due {
                self.want_resync.remove(t);
                self.last_resync.insert(t.clone(), Instant::now());
            }
            self.stats.lock().unwrap().resyncs += due.len() as u64;
        }
        self.last_resync.retain(|_, at| at.elapsed() < RESYNC_EVERY);
    }

    /// Writes the top levels of every book that changed since the last call. Books of tokens no
    /// longer tracked are dropped.
    async fn write_snapshots(&mut self) {
        let t = now_ns();
        let mut tokens: Vec<TokenId> = self.dirty.drain().collect();
        tokens.sort();
        // Token -> (condition id, partition) for tokens still tracked.
        type Route = Option<(String, Partition)>;
        let routed: Vec<(TokenId, Route)> = {
            let r = self.reg.read().unwrap();
            let by_token: HashMap<&TokenId, &str> = r
                .tracked
                .values()
                .flat_map(|m| m.token_ids.iter().map(move |t| (t, m.condition_id.as_str())))
                .collect();
            self.books.books.retain(|k, _| by_token.contains_key(k));
            tokens
                .into_iter()
                .map(|tok| {
                    let m = by_token
                        .get(&tok)
                        .and_then(|c| Some((c.to_string(), r.market_partition(c)?)));
                    (tok, m)
                })
                .collect()
        };
        let levels = self.cfg.book_snapshot_levels;
        let mut n = 0;
        for (tok, m) in routed {
            let (Some(book), Some((market, part))) = (self.books.get(tok.as_str()), m) else {
                continue;
            };
            if !book.synced {
                continue;
            }
            let lv = |side| {
                book.top(side, levels)
                    .iter()
                    .map(|(p, s)| json!([p, s]))
                    .collect::<Vec<_>>()
            };
            let rec = json!({
                "asset_id": tok,
                "market": market,
                "ts": book.ts_ms,
                "bids": lv(Side::Buy),
                "asks": lv(Side::Sell),
                "n_bids": book.bids.len(),
                "n_asks": book.asks.len(),
            });
            self.store.write(&part, "book_snapshots", t, 0, &rec.to_string()).await;
            n += 1;
        }
        self.stats.lock().unwrap().snapshots += n;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokens(range: std::ops::Range<usize>) -> Vec<TokenId> {
        range.map(|i| TokenId::from(i.to_string())).collect()
    }

    fn sizes(m: &ClobManager) -> Vec<usize> {
        m.shards.iter().map(|s| s.assets.lock().unwrap().len()).collect()
    }

    /// A manager whose connections never open (cancelled before they start).
    fn manager(max: usize) -> ClobManager {
        let cfg = ClobSection {
            max_assets_per_conn: max,
            ..Default::default()
        };
        let cancel = CancellationToken::new();
        cancel.cancel();
        let (tx, _rx) = mpsc::channel(16);
        ClobManager::new(&cfg, tx, cancel)
    }

    #[tokio::test]
    async fn shards_fill_up_before_new_connections_open() {
        let mut m = manager(500);
        m.update(&tokens(0..1200), &[]);
        assert_eq!(sizes(&m), [500, 500, 200]);

        // Freed room is reused before the last shard grows or a new one opens.
        m.update(&[], &tokens(0..100));
        assert_eq!(sizes(&m), [400, 500, 200]);
        m.update(&tokens(2000..2150), &[]);
        assert_eq!(sizes(&m), [500, 500, 250]);
        assert_eq!(m.shard_count(), 3);

        // Removing unknown tokens is a no-op; shards are never closed, only emptied.
        m.update(&[], &tokens(9000..9010));
        m.update(&[], &tokens(100..1200));
        assert_eq!(sizes(&m), [100, 0, 50]);
        let all: usize = sizes(&m).iter().sum();
        assert_eq!(all, 150);
    }

    #[tokio::test]
    async fn a_token_is_in_exactly_one_shard() {
        let mut m = manager(3);
        m.update(&tokens(0..10), &[]);
        m.update(&[], &tokens(2..5));
        m.update(&tokens(10..13), &[]);
        let mut seen = std::collections::HashSet::new();
        for s in &m.shards {
            for t in s.assets.lock().unwrap().iter() {
                assert!(seen.insert(t.clone()), "{t} in two shards");
            }
        }
        assert_eq!(seen.len(), 10);
        assert!(sizes(&m).iter().all(|&n| n <= 3));
    }

    #[test]
    fn events_go_to_the_documented_streams() {
        let msg = |j: &str| serde_json::from_str::<MarketMsg>(j).unwrap();
        let m = r#""market":"0xm","asset_id":"1","timestamp":"1""#;
        assert_eq!(
            stream_of(&msg(&format!(r#"{{"event_type":"book",{m},"bids":[],"asks":[]}}"#))),
            "clob_book"
        );
        assert_eq!(
            stream_of(&msg(
                r#"{"event_type":"price_change","market":"0xm","timestamp":"1","price_changes":[]}"#
            )),
            "clob_book"
        );
        assert_eq!(
            stream_of(&msg(&format!(
                r#"{{"event_type":"tick_size_change",{m},"old_tick_size":"0.01","new_tick_size":"0.001"}}"#
            ))),
            "clob_book"
        );
        assert_eq!(
            stream_of(&msg(&format!(
                r#"{{"event_type":"last_trade_price",{m},"price":"0.5","size":"1","side":"BUY"}}"#
            ))),
            "clob_trades"
        );
        assert_eq!(
            stream_of(&msg(&format!(r#"{{"event_type":"best_bid_ask",{m}}}"#))),
            "clob_bbo"
        );
        assert_eq!(stream_of(&msg(r#"{"event_type":"market_resolved"}"#)), "clob_other");
        assert_eq!(stream_of(&msg(r#"{"event_type":"something_new"}"#)), "clob_other");
    }
}
