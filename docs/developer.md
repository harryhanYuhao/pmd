# pm-rs — developer guide

## Workspace

```
pm-rs/
├── Cargo.toml             workspace: shared dependency versions, release profile (debug=1)
├── crates/                libraries, reusable by the recorder, offline tools and a trader
│   ├── pm-core            pure domain logic, no I/O
│   ├── pm-api             REST clients
│   ├── pm-ws              resilient websocket sessions
│   └── pm-store           append-only zstd JSONL store
├── apps/
│   └── pm-recorder        binary that wires the crates together
├── config/recorder.toml
└── docs/
```

Dependency direction: `apps → pm-store, pm-ws, pm-api → pm-core`. `pm-core` never does I/O or
async, so the same book and wire code runs in live code, replays and tests.

Planned: `crates/pm-exec` (API-key auth, EIP-712 order signing, place/cancel, user channel) and
`apps/pm-trader`. A trader should reuse `pm-ws` + `pm-core::book` for market data, `pm-api` for
metadata, and `pm-store` to journal its own decisions and fills in the same format as recorded
data.

## Crates

### pm-core

| module | contents |
|---|---|
| `fixed` | `Fixed6(i64)`, an exact 6-decimal fixed-point type for prices and sizes (`Price`, `Size` aliases). Parses strings exactly; serde accepts a string or number; `mul_fixed`, `complement` (1 − p) |
| `ids` | `TokenId`, `ConditionId` (`Arc<str>` newtypes, cheap clone), `Side` (`BUY`/`SELL`) |
| `market` | `MarketInfo`: normalised market metadata (league, event, outcomes, token ids, start time, tick size, neg-risk, status) |
| `wire` | `split_frame`: splits any feed's frame (one object or an array) into raw JSON objects |
| `wire::clob_ws` | `MarketMsg` enum for the CLOB market channel. `strip_hashes`, `subscribe_msg`, `update_subscription_msg` |
| `wire::rtds` | RTDS `activity/trades` messages, `subscribe_trades_msg` |
| `wire::sports` | `GameUpdate` from the live-score feed |
| `book` | `OrderBook` (BTreeMap L2): `apply_snapshot`, `set_level`, `reconcile`, `best_bid/ask`, `mid`, `spread`, `top(n)`, `sweep`/`sweep_notional` (fill simulation with VWAP). `BookSet::apply(&MarketMsg)` keeps a book per token and returns an `ApplyReport` |

Wire structs borrow from the frame where possible. Deserialise with `serde_json::from_str` on
the `RawValue`s returned by `split_frame`.

### pm-api

`Http` (reqwest + retry on 429/5xx with backoff) and thin clients:
- `Gamma::events` / `events_raw` (`EventsQuery { tag_id, series_id, closed, end_date_min, … }`, paginates);
  `GammaMarket::to_info` builds a `MarketInfo`. `GammaEvent::start()` / `is_game()` and
  `gamma::parse_time` handle Gamma's start-time formats.
- `Clob::book`, `Clob::server_time`.
- `DataApi::trades` (offset ≤ 10k, ~1.4 req/s sustained).

### pm-ws

```rust
let mut spec = WsSpec::new("rtds", URL);
spec.keepalive = Some((Duration::from_secs(5), "PING".into())); // periodic text frame
spec.auto_reply = vec![("ping".into(), "pong".into())];         // answer server pings
spec.ignore = vec!["PONG".into()];                              // don't forward these
spec.idle_timeout = Duration::from_secs(60);                    // reconnect if silent
let (handle, task) = pm_ws::spawn(spec, Arc::new(|| vec![subscribe_msg()]), tx, cancel);
```

`spawn` connects, sends `on_connect()` messages on **every** (re)connect, and forwards
`WsEvent::{Connected, Text{session, recv_ns, text}, Disconnected}` to `tx`. It reconnects with
exponential backoff. Each connection gets a process-unique `session` id, which is used to detect
gaps. `handle.send(text)` sends a message on the live connection (dynamic subscribe).

### pm-store

```rust
let store = Store::start(StoreConfig { root, ..Default::default() })?;
let h = store.handle();                                  // Clone freely
h.write(&Partition::new("esports", "cs2"), "clob_book", t_ns, session, raw_json).await;
drop(h); store.shutdown().await?;                        // finishes every file
```

- One dedicated writer thread with a bounded channel (back-pressure, never unbounded memory).
- Path: `root/YYYY/MM/DD/<category>/<league>/<stream>/HH.r<run>.jsonl.zst`, using the UTC hour
  of `t_ns`. Files only roll forward, so a record a few ms late lands in the current hour.
- Flushes dirty encoders every `flush_every`, closes past-hour files on the next tick, and opens
  files in append mode.
- `stream` is `&'static str`: streams are a fixed set chosen in code, not data.
- `list_files(root, part, stream)` lists files across days.
- `StoreHandle::stats()` returns per `(Partition, stream)` lines, raw bytes and on-disk
  (compressed) bytes; disk bytes are counted at each flush. `shutdown()` returns the final totals.

## pm-recorder internals

```
main ─┬─ Discovery (Gamma poll every 60 s) ──► Registry (RwLock) ◄── routing lookups
      │        └─ ClobManager.update(add, remove)
      ├─ ClobManager: N shard websockets (≤250 tokens each) ──► mpsc ──► clob::run_handler
      ├─ feeds::run_activity (RTDS trades)
      ├─ feeds::run_sports   (live scores)
      └─ status logger
all writers ──► StoreHandle ──► pm-store thread
```

| file | role |
|---|---|
| `config.rs` | TOML schema (`deny_unknown_fields`, defaults per section) |
| `registry.rs` | `Registry`: tracked markets, plus market/event/score-league → `Partition` routing. Constants `SYSTEM = "_system"`, `OTHER = "other"` |
| `discovery.rs` | Polls each league's open events: `fetch_all` (tag + series, 4 leagues at a time) → `process_event` (scope filters, records `gamma_events` on fingerprint change or every 6 h and `markets` on change) → `update_tracked` (diff; leagues whose fetch failed keep their markets) → `log_changes` |
| `clob.rs` | `ClobManager` shards tokens and (un)subscribes dynamically. `run_handler` drives a `Handler`: `on_frame` stores events by stream and updates the `BookSet`, `resync_due` re-requests broken books, `write_snapshots` |
| `sessions.rs` | Connect/disconnect markers to `_system/<feed>/sessions`, shared by all feeds |
| `feeds.rs` | RTDS trades (routed by condition id, then event slug) and the sports feed (routed by `leagueAbbreviation`) |
| `status.rs` | Periodic human-readable status (per-league markets and MB from `StoreHandle::stats`) and the final report |
| `logging.rs` | tracing setup: stdout plus an optional file (non-blocking appender), local-time stamps |
| `main.rs` | CLI, task wiring, signals (Ctrl-C/SIGTERM/SIGHUP), ordered shutdown |

### Book integrity

Every `price_change` carries the server's best bid/ask (an empty side is reported as bid `"0"` /
ask `"1"`). After applying a change, `OrderBook::reconcile`:
- **prunes** levels better than the server's best. These are stale levels left behind when a
  snapshot raced a cancel (~0.25% of changes);
- flags **missing** when the server's best level is absent from our book (~0.02%). The handler
  then resubscribes that token (unsubscribe + subscribe, at most once per 60 s per token), and
  the server responds with a fresh `book`.

Offline replays of `clob_book` must apply the same rule.

### Shutdown order (important)

`cancel` → each task exits. The CLOB handler closes its receiver and drains it, because the
`ClobManager` it holds owns the shard senders. Wait up to 10 s per task, then abort. Finally
`store.shutdown()` runs once all `StoreHandle`s are dropped. If you add a task that holds a
`StoreHandle`, make sure it observes `cancel`; otherwise shutdown waits on it.

## Recipes

**Add a stream:** pick a name, call `store.write(&part, "my_stream", …)`, and document it in
`usage.md` and the readers (`research/live_data.py`).

**Add a feed:** build a `WsSpec`, `pm_ws::spawn` it, loop over `WsEvent`s (write session
markers to `registry::system("<feed>")`), route each message to a `Partition`, and spawn the
task in `main.rs` with `cancel`.

**Add a config option:** add the field to the section struct and its `Default`, and document it
in `config/recorder.toml`. `deny_unknown_fields` makes old binaries reject new keys, which is
intentional.

**Reuse in a trader:**
```rust
let mut books = BookSet::default();
// for each WsEvent::Text from a pm_ws CLOB connection:
for raw in split_frame(&text).unwrap_or_default() {
    if let Ok(msg) = serde_json::from_str::<MarketMsg>(raw.get()) {
        let rep = books.apply(&msg);     // resubscribe tokens in rep.missing
    }
}
let fill = books.get(token).unwrap().sweep(Side::Buy, size, Some(limit));
```

## Conventions

- Store server payloads verbatim; derive and normalise offline. Recording and organising are
  separate steps. The organiser (per event / market, Parquet) is planned and should only read
  finished hours.
- Log messages are sentences for a human reading `tail -f`: put the values in the message
  (`info!("discovery: recording {n} markets")`), not in key=value fields. Routine, high-rate
  events go to `debug!`.
- Use money as `Fixed6`, never `f64`, in live code.
- `cargo fmt` (120 columns, `rustfmt.toml`) and `cargo clippy --workspace --all-targets` must be
  clean; run `cargo test --workspace` before committing.
- Every long-running task must observe `cancel`, including inside long awaits (see
  `Discovery::run`): shutdown aborts tasks that take more than 10 s, and names them in the log.
- Verified feed quirks (empty-side markers, the global `new_market` broadcast, RTDS maker fills,
  ping formats) are documented in module docs. Keep them there when you change behaviour.

## Feed reference

| feed | URL | keepalive |
|---|---|---|
| CLOB market | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | send `PING` every ~10 s → `PONG` |
| RTDS | `wss://ws-live-data.polymarket.com` | send `PING` → `PONG` |
| Sports | `wss://sports-api.polymarket.com/ws` | server sends `ping`, reply `pong` |
| Gamma | `https://gamma-api.polymarket.com` (`/events`, `/sports`) | REST |
| CLOB REST | `https://clob.polymarket.com` (`/book`, `/time`) | REST |
| Data API | `https://data-api.polymarket.com` (`/trades`) | REST |
