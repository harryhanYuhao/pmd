# pm-recorder — user guide

`pm-recorder` records Polymarket order books, trades (with wallets), market metadata and live
game scores for a configurable list of leagues. The data is written to compressed hourly files
for offline research and backtesting.

## 1. Build

Requires Rust ≥ 1.85 (edition 2024). No system libraries are needed (TLS is rustls).

```bash
cd pm-rs
cargo build --release              # binary: target/release/pm-recorder
cargo test --workspace             # optional
```

## 2. Run

```bash
./target/release/pm-recorder                                   # uses config/recorder.toml
./target/release/pm-recorder --config my.toml                  # other config
./target/release/pm-recorder --data-dir /tmp/x --run-for 120   # smoke test: 2 min into /tmp/x
RUST_LOG=debug ./target/release/pm-recorder                    # also log file open/close
```

| flag | default | meaning |
|---|---|---|
| `-c, --config` | `config/recorder.toml` | config file (relative to the current directory) |
| `--data-dir` | config `data_dir` | override the output root |
| `--run-for <secs>` | until stopped | stop after N seconds |

The recorder runs until Ctrl-C, SIGTERM or SIGHUP. On any of these it drains its queue and
finalises every file before exiting, which can take a few seconds.

### Unattended (tmux)

```bash
tmux new -d -s recorder 'cd ~/trading/polymarket-explore-claude/pm-rs && \
  while true; do ./target/release/pm-recorder; sleep 10; done'
tmux attach -t recorder        # detach: Ctrl-b d
tmux kill-session -t recorder  # clean stop (SIGHUP)
```

The loop restarts the recorder after a crash. Each run writes new files (the `r<run>` suffix),
so a restart never overwrites earlier data.

### Status line

The recorder logs one line per minute, e.g.:

```
status markets=1789 clob_events=118234 dropped=0 pruned=310 missing=12 resyncs=12 snapshots=2960 trades=402 scores=88 raw_mb=61.2
```

| field | meaning | healthy |
|---|---|---|
| `markets` | markets currently subscribed | roughly constant, moves with the schedule |
| `clob_events` | order-book / trade events received | ~1–3k/s for the default esports config |
| `dropped` | CLOB events with no known market | 0 |
| `pruned` / `missing` / `resyncs` | book repairs (see the developer guide) | `missing` ≪ 0.1% of events |
| `trades` / `trades_other` | RTDS trades kept / ignored (untracked events) | |
| `scores` | live-score updates | > 0 while games are live |
| `raw_mb` | uncompressed bytes written since start | ~10× the on-disk size |

## 3. Configure

The config is `config/recorder.toml`, and every field is commented there. Relative paths are
resolved against the config file's directory. Unknown keys are rejected, so typos fail at startup.

```toml
data_dir = "../../data/live"        # → <repo>/data/live (created if missing)

[store]
zstd_level = 6                      # 3 faster … 9+ smaller (~10% gain, more CPU)
flush_secs = 5                      # at most this much is lost on a crash

[discovery]
interval_secs = 60                  # Gamma poll; new markets appear within this
end_date_lookback_hours = 24
ended_grace_secs = 1800             # keep recording 30 min after an event ends

[[leagues]]                         # one block per league
category = "esports"                # 1st directory level under the date
key = "cs2"                         # 2nd directory level
tag_id = 100780                     # Gamma /sports primaryTagId
# score_league = "cs2"              # live-score leagueAbbreviation if it differs from key
# include_types = ["moneyline", "child_moneyline"]   # only these sportsMarketType (empty = all)
# exclude_types = ["totals"]
# include_untyped = true            # markets without a sportsMarketType

[clob]
enabled = true
max_assets_per_conn = 250           # tokens per websocket
custom_features = true              # best_bid_ask + market_resolved events
book_snapshot_secs = 60             # top-of-book snapshots (0 = off)
book_snapshot_levels = 50
keep_hashes = false                 # true triples clob_book size

[activity]                          # RTDS trades with wallets
enabled = true
only_tracked = true                 # false = record ALL Polymarket trades (other/untracked)

[sports]                            # live scores
enabled = true
leagues = []                        # empty = all leagues (small feed)
```

### Adding a league

1. Find its `primaryTagId`. It is listed in [`leagues.md`](leagues.md), or you can query it:
   ```bash
   curl -s https://gamma-api.polymarket.com/sports | jq -r '.[] | "\(.sport) \(.primaryTagId)"'
   ```
   Use `primaryTagId`. A tag *slug* (for example `/tags/slug/cs2`) is a different tag and
   returns the wrong events.
2. Add a `[[leagues]]` block with a `category`, a `key` and the `tag_id`.
3. Restart the recorder.

Cost scales with market count. The default 3 esports leagues (~1,800 markets / 3,600 tokens) use
about 4–5 GB/day. A major sports league can trade 10–100× more per market. For a high-volume
league, consider `include_types` (for example only moneylines) to keep the size manageable.

## 4. Output

```
<data_dir>/<YYYY>/<MM>/<DD>/<category>/<league>/<stream>/<HH>.r<run>.jsonl.zst
e.g. data/live/2026/10/17/esports/cs2/clob_book/06.r1792219759.jsonl.zst
```

- **Dates are UTC and come first.** A finished day is one self-contained directory that you can
  move, back up or archive as a unit, e.g. `rsync -a data/live/2026/10/16 backup:`. Don't move
  the current day: it is still being written.
- **Files are hourly.** The file for the current hour is still open; readers should treat its
  truncated tail as normal.
- **Special partitions:** `_system/<feed>/sessions` holds connect/disconnect markers (use them to
  find gaps). `_system/unknown` holds unroutable CLOB events and should stay empty.
  `other/<abbr>/scores` holds scores of leagues you have not configured. `other/untracked/trades`
  is only written when `only_tracked = false`.

| stream | content |
|---|---|
| `clob_book` | full L2 history: `book` snapshots, `price_change` deltas, `tick_size_change` |
| `clob_trades` | `last_trade_price` (one per taker trade: side, price, size, fee rate, tx hash) |
| `clob_bbo` | `best_bid_ask` updates |
| `clob_other` | `market_resolved` and unknown event types |
| `book_snapshots` | top 50 levels of each changed book every 60 s (no replay needed) |
| `trades` | RTDS trades with wallet, name, outcome (includes maker fills, see the caveat in the README) |
| `scores` | live game state (`score` = `in-map\|maps\|BoN`, `period`, `live`, `ended`) |
| `gamma_events` | raw Gamma event JSON on state change and every 6 h |
| `markets` | normalised market metadata (`MarketInfo`) + `tracked` flag on change |

Each line has the form `{"t": receive_ns, "s": session_id, "m": <message as received>}`. The one
change from the server's message is that the opaque `hash` fields are stripped from `price_change`.

### Reading

```bash
zstdcat data/live/2026/10/17/esports/cs2/clob_trades/*.zst | head
```

```python
# research/live_data.py (run inside research/ with `uv run`)
from live_data import trades, bbo, clob_trades, book_snapshots, markets, scores, sessions
tr  = trades("cs2", start="2026-10-17")     # polars DataFrame
mk  = markets()                             # latest metadata per market, all leagues
gap = sessions("clob")                      # connect/disconnect markers
```

Join keys: `asset_id`/`asset` = token id; `market`/`conditionId` = condition id; join on the
`markets` stream to get league, event, outcomes and start time. `scores.gameId` corresponds to
`markets.game_id`.

## 5. Troubleshooting

| symptom | cause / fix |
|---|---|
| `parsing config/recorder.toml: unknown field` | a typo, or a key in the wrong section |
| `no leagues configured` | at least one `[[leagues]]` block is required |
| a league records 0 markets | wrong `tag_id` (a slug tag rather than `primaryTagId`), or no open events right now |
| scores land in `other/<abbr>` instead of your league | set `score_league = "<abbr>"` on that league |
| repeated `disconnected` in sessions | network trouble; the recorder reconnects with backoff (0.5 s → 30 s) and resubscribes |
| `missing` grows quickly | books are resubscribed automatically; if it persists, report it with the log |
