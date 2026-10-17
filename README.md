# pm-rs — Polymarket recorder (and future trader)

A Cargo workspace. Shared library crates hold everything a live trader will also need; the
recorder is one binary on top of them.

Docs: [usage](docs/usage.md) (build, run, config, output) · [developer guide](docs/developer.md)
· [leagues worth recording](docs/leagues.md).

```
pm-rs/
├── crates/
│   ├── pm-core    domain types, no I/O: exact decimals (Fixed6), ids, feed wire formats,
│   │              MarketInfo, L2 OrderBook/BookSet (+ sweep/fill simulation, reconciliation)
│   ├── pm-api     REST clients: Gamma (metadata), CLOB public (books), Data API (trades)
│   ├── pm-ws      resilient websocket session: reconnect/backoff, keepalive, idle detection,
│   │              resubscribe on every connect, session ids for gap tracking
│   └── pm-store   hour-partitioned zstd JSONL writer on a dedicated thread
├── apps/
│   └── pm-recorder
└── config/recorder.toml
```

Planned for trading (same workspace): `crates/pm-exec` (API-key auth, EIP-712 order signing,
order placement/cancel, user websocket channel) and `apps/pm-trader`. A trader reuses `pm-ws` +
`pm-core::book` for market data, `pm-api` for metadata, and `pm-store` to journal its own
decisions and fills in the same format as the recorded data, so backtests and live runs read
identical files.

## Run

```bash
cargo build --release
./target/release/pm-recorder --config config/recorder.toml          # runs until Ctrl-C / SIGTERM / SIGHUP
./target/release/pm-recorder --data-dir /tmp/x --run-for 120         # smoke test
RUST_LOG=debug ./target/release/pm-recorder ...                      # file open/close detail
```

For unattended recording run it in tmux. The loop restarts it 10 s after a crash; each restart
starts new files, so nothing is overwritten:

```bash
tmux new -d -s recorder 'cd ~/trading/polymarket-explore-claude/pm-rs && \
  while true; do ./target/release/pm-recorder --config config/recorder.toml; sleep 10; done'
tmux attach -t recorder      # watch the status line (every minute); detach with Ctrl-b d
tmux kill-session -t recorder
```

`tmux kill-session` sends SIGHUP, which the recorder treats like Ctrl-C: it drains its queue and
finishes every file before exiting.

## What is recorded

Discovery polls Gamma every 60 s for open events of each configured league (`tag_id`) and
subscribes every tradable market (filterable by `sportsMarketType`). An event's markets are
dropped 30 min after Gamma marks it ended or when they close.

Layout:

```
data/live/<YYYY>/<MM>/<DD>/<category>/<league>/<stream>/<HH>.r<run-start>.jsonl.zst
e.g.  data/live/2026/10/16/esports/cs2/clob_book/18.r1792174620.jsonl.zst
```

Date first (UTC), so one day is one directory: move, back up or archive `data/live/2026/10/16`
as a unit (a day is finished once the clock is past it). `category` and `league` come from each
`[[leagues]]` entry in the config. Data not tied to a configured league goes to `_system/` (feed
bookkeeping) or `other/` (other leagues).

| stream | under | content |
|---|---|---|
| `clob_book` | `<category>/<league>` | CLOB `book` snapshots + `price_change` deltas + `tick_size_change`: full L2 history |
| `clob_trades` | `<category>/<league>` | CLOB `last_trade_price` (taker side, size, price, fee rate, tx hash) |
| `clob_bbo` | `<category>/<league>` | CLOB `best_bid_ask` |
| `clob_other` | `<category>/<league>` | `market_resolved` and unknown event types for tracked markets |
| `book_snapshots` | `<category>/<league>` | top 50 levels of each book that changed, every 60 s (no replay needed) |
| `trades` | `<category>/<league>` | RTDS `activity/trades`: trades **with wallets** (see caveat) |
| `scores` | `<category>/<league>`, else `other/<abbr>` | live game state (`score` = `in-map\|maps\|BoN`, `period`), all leagues |
| `gamma_events` | `<category>/<league>` | raw Gamma event JSON when its state changes (live/ended/score/status/resolution) and every 6 h |
| `markets` | `<category>/<league>` | normalised `MarketInfo` + `tracked` flag whenever it changes |
| `sessions` | `_system/<feed>` | websocket connect/disconnect markers (`clob`, `rtds`, `sports`) |
| `clob_*` | `_system/unknown` | CLOB events whose market could not be mapped to a league (should stay empty) |

One record per line: `{"t": receive time in ns, "s": session id, "m": message}`. `m` is the
server's message verbatim, except that `price_change` events have their opaque `hash` fields
removed (`keep_hashes = true` keeps them; they triple the size). Files are flushed every 5 s; a
crash loses at most that. Read with `zstdcat` or `research/live_data.py`.

The recorder only pulls and appends. Organising the data (per event / per market, Parquet) is
meant to be a separate offline step over finished hours, so it can be changed and re-run without
touching the raw recording.

Joins: `asset_id`/`asset` = token id, `market`/`conditionId` = condition id
(→ `markets` stream for league, event, outcomes, start time); `scores.gameId` = Gamma `gameId`
(in `markets.game_id`).

### Volume (measured 2026-10-16, ~1,900 markets / 3,800 tokens)

~2,000 CLOB events/s, ≈ 3 MB/min compressed (≈ 4–5 GB/day) of which `clob_book` is ~80%.
zstd level matters little (level 6 → 15 saves ~10%); dropping hashes saved 3x.

### Book integrity

Every `price_change` carries the server's best bid/ask (empty side = bid `0` / ask `1`). The
in-memory book is checked against it after each change: stale levels left by a snapshot that
raced a cancel are pruned (~0.25% of changes), and a book missing a level (~0.02%) is
resubscribed, which makes the server send a fresh `book`. Offline replays of `clob_book` should
apply the same rule (`OrderBook::reconcile`).

### Caveats

- **RTDS `trades` contains maker fills as well as taker trades** (3–4x as many rows as the Data
  API's `takerOnly=true` tape). For taker-flow features matching the historical research data,
  label takers by joining on `transactionHash` + asset + side + size with `clob_trades`. That
  catches ~98% of takers but also tags some makers. A clean taker flag is still open work.
- The global `new_market` broadcast (all of Polymarket) is dropped.
- Gamma tag ids must be the `/sports` `primaryTagId` (the `cs2` tag slug is a different tag).
