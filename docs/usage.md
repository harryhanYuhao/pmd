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
./target/release/pm-recorder --log-file /tmp/rec.log            # log to stdout and this file
RUST_LOG=debug ./target/release/pm-recorder                    # also log file open/close, connects
```

| flag | default | meaning |
|---|---|---|
| `-c, --config` | `config/recorder.toml` | config file (relative to the current directory) |
| `--data-dir` | config `data_dir` | override the output root |
| `--run-for <secs>` | until stopped | stop after N seconds |
| `--log-file <path>` | config `[log] file` | also append the log to this file |
| `--no-log-file` | | stdout only, even if the config names a file |

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

### Log

The log always goes to stdout. With `[log] file` set (the default config uses
`data/logs/recorder.log`), the same lines are also appended to that file, without colours.
Timestamps are local time. Follow it with `tail -f data/logs/recorder.log`.

**Discovery** logs which events are being recorded. At startup it lists every event; after that
it logs only changes, sorted by league and start time:

```
INFO discovery: recording 2083 markets in 207 events [esports/cs2 565 markets / 87 events, esports/dota2 319 markets / 19 events, ...] | 207 new events, 0 finished, 0 resized | tokens +4166 -0 | 1.1s
INFO   + esports/cs2     Counter-Strike: HAVU vs Phantom (BO3) - Elisa BreakOut Series #2 Playoffs  (started Sat 17:00 (2h09m ago), 23 markets)
INFO   + esports/cs2     DraculaN Season 7: Winner  (no start time, 24 markets)
INFO   ~ esports/lol     LoL: T1 vs Gen.G (BO5) - Worlds  (12 -> 18 markets)
INFO   - esports/dota2   Dota 2: Tundra vs Falcons (BO3) - ...  (finished, 9 markets dropped)
```

`+` marks a new event, `~` an event whose market count changed (e.g. map markets added), and
`-` an event that is no longer recorded (closed, or 30 min after it ended). Polls with no change
are logged at debug level only.

**Status** is reported every `status_secs` (60 s):

```
INFO status: 2083 markets (4166 tokens) | CLOB 17/17 connections up | trades feed up | scores feed up
INFO   last 60s: 118.2k book/trade events (1970/s), 213 wallet trades, 45 score updates, book repairs: 98 pruned, 3 resynced
INFO   written: 2.2 MB/min to disk (50.8 MB/min raw), 412.0 MB total since start (~3.12 GB/day)
INFO     _system                                0 B/min       398 B total
INFO     esports/cs2        565 markets      1.1 MB/min      140.2 MB total
INFO     esports/lol        776 markets      212 KB/min       98.7 MB total
INFO     other                                 3 KB/min        2 KB total
```

| part | meaning | healthy |
|---|---|---|
| connections up | CLOB websockets connected / opened (250 tokens each); trades and scores feed state | all up; a `DOWN` that persists means network trouble |
| book/trade events | CLOB messages received in the window | ~1–3k/s for the esports config |
| book repairs | stale levels pruned / books re-requested (see the developer guide) | resynced ≪ 0.1% of events |
| written | compressed bytes on disk per minute, raw bytes, total since start, extrapolated per day | the first window is inflated by the initial book snapshots |
| per-league lines | markets recorded and bytes written for each `category/league` | |

Reconnects (`[clob-3] reconnected`) are logged at info and disconnects at warn. At shutdown the
recorder logs the total records and bytes written.

## 3. Configure

The config is `config/recorder.toml`, and every field is commented there. Relative paths are
resolved against the config file's directory. Unknown keys are rejected, so typos fail at startup.

```toml
data_dir = "../../data/live"        # → <repo>/data/live (created if missing)

[log]
level = "info"                      # error | warn | info | debug (RUST_LOG overrides)
file = "../../data/logs/recorder.log"   # also append here; "" = stdout only
status_secs = 60                    # status report period

[store]
zstd_level = 6                      # 3 faster … 9+ smaller (~10% gain, more CPU)
flush_secs = 5                      # at most this much is lost on a crash

[discovery]
interval_secs = 60                  # Gamma poll; new markets appear within this
end_date_lookback_hours = 24
ended_grace_secs = 1800             # keep recording 30 min after an event ends
max_days_ahead = 7                  # skip events starting later than this (0 = no limit)

[[leagues]]                         # one block per league
category = "esports"                # 1st directory level under the date
key = "cs2"                         # 2nd directory level
tag_id = 100780                     # Gamma /sports primaryTagId: all events incl. futures
# series_id = 10310                 # Gamma /sports series: match events only (tag and/or series)
# games_only = false                # true = skip events without start time / game id
# score_league = "cs2"              # live-score leagueAbbreviation if it differs from key
# include_types = ["moneyline", "child_moneyline"]   # only these sportsMarketType (empty = all)
# exclude_types = ["totals"]
# include_untyped = true            # markets without a sportsMarketType

[clob]
enabled = true
max_assets_per_conn = 500           # tokens per websocket (500 verified with 61 connections)
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

1. Look up the league in [`leagues.md`](leagues.md), or query the list:
   ```bash
   curl -s https://gamma-api.polymarket.com/sports | jq -r '.[] | "\(.sport) tag=\(.primaryTagId) series=\(.series)"'
   ```
2. Add a `[[leagues]]` block with a `category`, a `key`, and one of the following:
   - **`series_id`** (the `series` field): the league's **match events only**. Use this for
     traditional sports. For Serie A, the Champions League and tennis it is the only way to
     find matches, because the match events do not carry the league's primary tag.
   - **`tag_id`** (the `primaryTagId`): every event, including futures, outrights and season
     props. A tag *slug* (for example `/tags/slug/cs2`) is a different tag and returns the
     wrong events.
   - **Both**, if you want matches and futures. Results are merged.
3. For sports, add `include_types`. Each match has dozens of prop markets (player stats, exact
   score, corners) that are about 10× the markets for about 1% of the volume. To find a league's
   types and how much each trades:
   ```bash
   curl -s 'https://gamma-api.polymarket.com/events?series_id=12185&closed=false&limit=100' \
     | jq -r '.[].markets[] | "\(.sportsMarketType)\t\(.volume24hr // 0)"' \
     | awk '{n[$1]++; v[$1]+=$2} END {for (t in n) printf "%-40s %5d %12.0f\n", t, n[t], v[t]}' | sort -k3 -nr
   ```
4. Restart the recorder. The first discovery log line shows the markets per league.

Cost scales with activity rather than market count. Measured on Saturday evening 2026-10-17 with
the full config (26 leagues, ~15,200 markets, 61 connections): ~9–10k events/s and 17–27 MB/min
on disk (~25–40 GB/day at that rate). College football with live games was 60% of that, while
the five esports leagues were ~3 MB/min. Weekday and off-season rates are much lower. To cut
the volume, narrow `include_types` on the busiest league (`cfb` with only `moneyline` drops ~80%
of its book updates), or lower `max_days_ahead`.

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
| a league records 0 markets | wrong `tag_id` (a slug tag rather than `primaryTagId`); try `series_id`; no open events right now; or the season has not started (`cbb`) |
| scores land in `other/<abbr>` instead of your league | set `score_league = "<abbr>"` on that league |
| repeated `disconnected` in sessions | network trouble; the recorder reconnects with backoff (0.5 s → 30 s) and resubscribes |
| `missing` grows quickly | books are resubscribed automatically; if it persists, report it with the log |
