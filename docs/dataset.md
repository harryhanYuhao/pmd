# Parquet dataset (`pmd export`)

`pmd export` converts the recorded zstd JSONL (`data/live`) into typed Parquet tables
(`data/parquet`) that pandas, polars, DuckDB and Spark read directly. The raw files stay the
source of truth: the Parquet tree can be deleted and rebuilt at any time.

```bash
./target/release/pmd export                                  # every finished day → data/parquet
./target/release/pmd export --from 2026-10-17 --league cs2   # a subset
./target/release/pmd export --tables bbo,clob_trades,tokens  # only some tables
./target/release/pmd export --include-today                  # also today's growing files
./target/release/pmd export --force                          # rebuild even if unchanged
./target/release/pmd export --out /fast/disk/pq --threads 4
```

- **Incremental.** The unit of work is one stream of one league on one day. A unit is rebuilt
  only when its input files (paths and sizes) changed, so re-running after a sync only
  converts the new data. `_manifest.json` in the output root records the inputs of every
  output. Changing the export schema (a new version of `pmd`) rebuilds everything once.
- **Today is skipped** unless `--include-today`, because its files are still growing. Today's
  output is rebuilt on the next run once the day is complete.
- **Safe to interrupt.** Files are written under a temporary name and renamed when complete.
- **Cost.** A day of five esports leagues (1.5 GB zstd JSONL, 33 GB raw JSON, 60M records)
  exports in ~50 s on 8 threads with ~2.8 GB peak memory, into ~1 GB of Parquet. Memory is
  about one hour of the busiest league per worker; lower `--threads` (default: cores, at most 8)
  if memory is tight.

## Layout

```
data/parquet/
  <table>/date=YYYY-MM-DD/category=<c>/league=<l>/part.parquet   time series
  markets.parquet  tokens.parquet  events.parquet  sessions.parquet   dimensions
  _manifest.json
```

The directory names are Hive partitions. Readers that understand them add `date`, `category`
and `league` columns and skip directories a filter excludes:

```python
import polars as pl
bu = pl.scan_parquet("data/parquet/book_updates/**/*.parquet", hive_partitioning=True)
bu.filter(pl.col("league") == "cs2", pl.col("date") == pl.date(2026, 10, 17)).collect()
```

```sql
-- duckdb
SELECT league, count(*) AS trades, sum(notional) AS usdc
FROM read_parquet('data/parquet/wallet_trades/**/*.parquet', hive_partitioning = true)
GROUP BY ALL ORDER BY usdc DESC;
```

## Conventions

| column | type | meaning |
|---|---|---|
| `t` | timestamp[ns, UTC] | our receive time. Every time-series table is sorted by `t` |
| `ts` | timestamp[ms, UTC] | the server's timestamp in the message |
| `session` | uint64 | websocket connection id; joins `sessions.session` (with `run`) |
| `market` | string | condition id (`0x…`) |
| `asset_id` | string | outcome token id; joins `tokens.asset_id` |
| prices, sizes | float64 | as sent (prices in USDC per share, 0–1) |

Rows are sorted by `t` within each hour. Runs of the recorder that overlap in an hour are merged
in time order. A record a few ms late for an hour sits in the next hour's file, so at hour
boundaries `t` can step back by a few ms.

## Time-series tables

| table | from stream | one row per |
|---|---|---|
| `book_updates` | `clob_book` | price level of a `book` snapshot, or one entry of a `price_change` |
| `tick_size_changes` | `clob_book` | `tick_size_change` |
| `bbo` | `clob_bbo` | `best_bid_ask` update |
| `clob_trades` | `clob_trades` | `last_trade_price` (one per taker trade) |
| `wallet_trades` | `trades` | RTDS trade with wallet |
| `book_snapshots` | `book_snapshots` | level of a periodic top-50 snapshot |
| `scores` | `scores` | live-score update |

**`book_updates`**: `t, session, ts, market, asset_id, kind, side, price, size, best_bid,
best_ask`
- `kind` is `snapshot` or `change`. `side` is `bid` or `ask`. `size` is the new total size at
  `price`; 0 removes the level.
- The rows of one snapshot share `t` and `asset_id`. A snapshot with no levels at all is one
  row with null `side`, `price` and `size`, so that the reset is not lost.
- `best_bid` / `best_ask` (changes only) are the server's top of book after the change. An
  empty side is reported as bid 0 / ask 1.

To **rebuild a book**, take one `asset_id` in `t` order:
1. On the first row of a snapshot group (a new `t` with `kind = snapshot`), clear the book,
   then set every level of the group.
2. On a change, set `price` to `size`, or remove the level if `size` is 0.
3. Then prune bid levels above `best_bid` and ask levels below `best_ask`.

Step 3 matters. Without it ~0.1% of changes leave a stale level at the top, from a snapshot
that raced a cancel (measured: 0.12% on a busy Dota 2 token). The recorder's
`OrderBook::reconcile` applies the same rule.

**`tick_size_changes`**: `t, session, ts, market, asset_id, old_tick, new_tick`

**`bbo`**: `t, session, ts, market, asset_id, best_bid, best_ask, spread`. An empty side is
null.

**`clob_trades`**: `t, session, ts, market, asset_id, price, size, side, fee_rate_bps,
tx_hash`. `side` is the taker side (`BUY` / `SELL`).

**`wallet_trades`**: `t, session, ts, chain_ts, market, asset_id, event_slug, slug, title,
outcome, outcome_index, price, size, notional, side, wallet, name, pseudonym, tx_hash`
- `ts` is the RTDS send time and `chain_ts` the on-chain time (second resolution).
  `notional = price × size` (USDC).
- RTDS reports **maker fills as well as taker trades**. The table keeps every row as received.
  For taker flow, match `clob_trades` on `tx_hash` + `asset_id` + `size`.
- **One transaction holds several fills**, often of equal size at different prices (a taker
  sweeping levels). Do not de-duplicate on `(tx_hash, asset_id, wallet, size)`. On 2026-10-17
  that key dropped 6,111 of 180k rows, ~5,200 of them with distinct prices, i.e. real fills.
  Only ~0.5% of rows repeat every field, and even those cannot be told apart from genuine
  equal fills.
- A few trades are in markets the recorder did not subscribe to (same event, untracked market
  type), so they have no `tokens` row.

**`book_snapshots`**: `t, ts, market, asset_id, side, level, price, size, n_bids, n_asks`.
`level` 0 is the best price. `n_bids` / `n_asks` are the full book depth at the time. These are
our books (already repaired), written every 60 s for each book that changed, so no replay is
needed.

**`scores`**: `t, session, game_id, metadata_game_id, league_abbr, home, away, status, score,
period, live, ended`
- `game_id` joins `events.game_id` and `markets.game_id`. Some leagues (cricket) send
  `metadata_game_id` (a string) instead, with `game_id` null.
- Esports `score` is `"<in-map>|<maps won>|Bo<n>"`. Unconfigured leagues are under
  `category=other`.

## Dimension tables

Rebuilt from **all** recorded `markets`, `gamma_events` and `sessions` files on every export
where one of them changed, whatever `--from` / `--league` select.

| table | one row per | columns |
|---|---|---|
| `markets` | condition id (latest record) | `market, market_id, category, league, event_id, event_slug, slug, question, market_type, n_outcomes, outcome_0, outcome_1, token_0, token_1, game_start_time, game_id, tick_size, min_order_size, neg_risk, active, closed, accepting_orders, tracked, first_seen, last_seen` |
| `tokens` | outcome token | `asset_id, market, outcome, outcome_index, category, league, event_slug, market_type, question` |
| `events` | Gamma event (latest record) | `event_id, slug, title, category, league, series_slug, tags, start_time, end_date, game_id, n_markets, active, closed, live, ended, score, period, volume, volume_24h, liquidity, open_interest, first_seen, last_seen` |
| `sessions` | websocket connection | `run, session, feed, feed_family, connected, disconnected, reason` |

`tokens` is the usual join: it labels any time-series row with its league, market type and
outcome.

```python
tok = pl.read_parquet("data/parquet/tokens.parquet")
bbo = pl.scan_parquet("data/parquet/bbo/**/*.parquet", hive_partitioning=True)
moneyline = bbo.join(tok.lazy().filter(pl.col("market_type") == "moneyline").select("asset_id", "outcome"),
                     on="asset_id")
```

In `sessions`, `disconnected` is null when the recorder was stopped before the connection's
shutdown marker was written (common for CLOB shards). Treat the run's end as the disconnect
time.

## Not exported

`clob_other` (`market_resolved` and unknown event types) and `_system/unknown`. Recordings in
the layout used before 2026-10-17 (`<stream>/<league>/<date>/…`) are ignored; `pmd audit` lists
them.
