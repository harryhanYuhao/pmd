# Plan: one `pm` CLI — record, audit, export, markets

Status: done (2026-10-18). Kept as the design record. Phases are done in order; each ends with a working build,
passing tests and updated docs.

| phase | status |
|---|---|
| 1 restructure: `pm record` | done |
| 2 `pm-store::read` | done |
| 3 `pm audit` | done |
| 4 `pm export` (Parquet) | done |
| 5 `pm markets` | done |
| 6 finish docs | done |

Notes from implementation:
- Some leagues' score updates (cricket) carry a string `metadataGameId` instead of `gameId`.
  Audit handles both; the `scores` export table needs both columns.
- Data recorded before 2026-10-17 uses an older layout (`<stream>/<league>/<date>/…`). Audit
  reports it as unrecognised; export ignores it unless a migration is added.
- Export deviates from the draft schema below in naming: times are real timestamps (`t`
  ns, `ts` ms, UTC) instead of `t_ns` / `ts_ms` integers, and `sessions` is a dimension table
  (one row per connection). [dataset.md](dataset.md) is the reference.
- Parquet is only ~30% smaller than the zstd JSONL (best encodings tried gain another ~10%);
  the benefit is typed, columnar reads, not disk space.
- RTDS `wallet_trades`: one transaction often has several equal-size fills at different prices.
  `research/live_data.py` de-duplicates on `(tx_hash, asset, wallet, size)`, which drops real
  fills (6,111 of 180k rows on 2026-10-17).
- Gamma `/events` rejects offsets above 2000. `pm-api` now pages with `/events/keyset`, which
  also protects the recorder's discovery from failing on very large leagues.
- `pm markets` fetches events without markets by default (all ~22k open events in ~6 s;
  with markets it would be ~1 GB).

## Goals

1. **Record** — today's `pm-recorder`, unchanged in behaviour, becomes `pm record`.
2. **Audit** — summarise what has been recorded (volume, coverage, gaps, damage) without
   modifying anything.
3. **Export** — turn the raw zstd JSONL into typed, partitioned Parquet tables that pandas,
   polars and DuckDB read directly.
4. **Markets** — a quick live view of Polymarket: how many markets are open / live, volume and
   liquidity, broken down by category and league, top markets, and what our recorder config
   covers.

Non-goals: changing the recorded format, deleting or rewriting raw files, a database server.

## Principles

- **Raw files are the source of truth.** Parquet is derived, can be deleted and rebuilt with an
  improved schema at any time. Nothing ever writes into `data/live`.
- **Logic lives in library crates, the binary is thin.** `apps/pm` only parses arguments, sets up
  logging and calls into crates.
- **A crate per heavy concern, not per command.** The Arrow/Parquet dependencies are large, so
  they sit in `pm-dataset` and the recorder never links them. The markets command is a few
  Gamma calls and lives as a module in `apps/pm`.
- **Reruns are safe.** Export skips work whose inputs have not changed and never touches the
  hour that is still being written.

## Target layout

```
crates/
  pm-core        (unchanged) domain types, wire formats, books
  pm-api         (unchanged) REST clients; maybe a few Gamma fields for `pm markets`
  pm-ws          (unchanged) websocket sessions
  pm-store       + `read` module: find and read recorded files
  pm-recorder    NEW (moved from apps/pm-recorder): the recorder as a library, `run(cfg, opts)`
  pm-dataset     NEW: `audit` and `export` (Parquet)
apps/
  pm             NEW: the only binary — `pm record | audit | export | markets`
```

Dependency direction stays one-way: `apps/pm → pm-recorder, pm-dataset → pm-store, pm-ws,
pm-api → pm-core`.

## Phase 1 — restructure: `pm record`

- Move `apps/pm-recorder/src/*` to `crates/pm-recorder`. Its `main.rs` becomes `lib.rs` with
  `pub async fn run(cfg: Config, run_for: Option<Duration>) -> Result<()>`; `config` is public.
- `logging.rs` moves to `apps/pm` (every command logs, not just the recorder).
- `apps/pm/src/main.rs`: clap with subcommands. `pm record` keeps every current flag
  (`--config`, `--data-dir`, `--run-for`, `--log-file`, `--no-log-file`) and the same defaults.
- Delete `apps/pm-recorder`. Update README / usage.md / developer.md (tmux loop now runs
  `./target/release/pm record`).
- Check: build, clippy, tests, a 20 s `pm record --run-for` smoke test with output identical in
  shape to before.

## Phase 2 — `pm-store::read`

Shared by audit and export.

- `FileInfo { path, date, hour, partition, stream, run, size }` parsed from
  `<root>/YYYY/MM/DD/<category>/<league>/<stream>/HH.r<run>.jsonl.zst`; anything else is
  reported as unrecognised, not silently skipped.
- `scan(root, &Filter) -> ScanResult { files, unrecognised }` with `Filter { from, to (dates),
  categories, leagues, streams }`.
- `read_file(path, |Record| …) -> ReadReport { lines, bad_lines, truncated: Option<String> }`
  where `Record { t_ns, session, payload: &RawValue }`. A truncated zstd frame or partial last
  line (crash, or the file still being written) ends the read cleanly and is reported, never a
  panic or an abort of the whole run.
- Tests: round trip with the writer; truncated file; garbage line.

## Phase 3 — `pm audit`

`pm audit [--data-dir D] [--from DATE] [--to DATE] [--category C] [--league L] [--stream S]
[--json] [--threads N]` — read-only, files processed in parallel (rayon).

Report sections:

1. **Inventory** — per day: files, runs, records, compressed and raw size; per partition and
   stream: records, size, first/last record time. Totals and compression ratio.
2. **Runs** — each recorder run (`r<run>` = start time): first/last record, duration, size.
   Overlapping runs and long gaps between runs are flagged.
3. **Feed downtime** — from `_system/<feed>/sessions`: per feed (each CLOB shard, rtds, sports)
   disconnect count, total downtime, longest gap, most common reasons. Gaps across a run boundary
   (recorder not running) are listed separately.
4. **Coverage holes** — per partition and stream: hours with no records between the first and
   last record of a run (e.g. a dead shard while other feeds kept writing).
5. **Integrity** — truncated files, undecodable lines, records whose time is outside their
   file's hour (beyond a 1 min tolerance), unrecognised paths.
6. **Content** — CLOB events by type (`book`, `price_change`, `tick_size_change`,
   `last_trade_price`, `best_bid_ask`), distinct markets / tokens with data versus markets
   announced in `markets`, wallet trades and notional, score updates and distinct games.

`--json` emits the same report as one JSON document (for scripts, and to compare audits over
time).

## Phase 4 — `pm export` (Parquet)

`pm export [--data-dir D] [--out DIR] [--from] [--to] [--category] [--league] [--tables …]
[--include-today] [--force] [--threads N]`

Default output `../../data/parquet` next to `data/live`. Hive-style partitions, which DuckDB
(`hive_partitioning`) and polars pick up automatically:

```
<out>/<table>/date=YYYY-MM-DD/category=<c>/league=<l>/part.parquet   (time-series tables)
<out>/<table>.parquet                                                (dimension tables)
<out>/_manifest.json                                                 (inputs of every output)
```

One file per table, day and league — never per hour (small files make queries slow). Rows are
sorted by receive time; each source hour becomes a row group. zstd compression.

Common columns: `t_ns` (Int64, local receive time, ns since epoch, UTC), `session` (UInt64,
connection id, joins `sessions`). Prices and sizes are Float64. Ids (`market` = condition id,
`asset_id` = token id) are dictionary-encoded strings.

| table | source stream | one row per | columns (besides `t_ns`, `session`) |
|---|---|---|---|
| `book_updates` | `clob_book` | price level in a `book` snapshot, or one `price_change` entry | `ts_ms`, `market`, `asset_id`, `kind` (`snapshot`/`change`), `side`, `price`, `size`, `best_bid`, `best_ask` (changes only). A snapshot's rows share `t_ns` + `asset_id`; replay = reset the book on each snapshot group, then apply changes. A fully empty snapshot is one row with null side/price. |
| `tick_size_changes` | `clob_book` | change | `ts_ms`, `market`, `asset_id`, `old_tick`, `new_tick` |
| `bbo` | `clob_bbo` | `best_bid_ask` | `ts_ms`, `market`, `asset_id`, `best_bid`, `best_ask`, `spread` |
| `clob_trades` | `clob_trades` | `last_trade_price` | `ts_ms`, `market`, `asset_id`, `price`, `size`, `side` (taker), `fee_rate_bps`, `tx_hash` |
| `wallet_trades` | `trades` (RTDS) | trade | `ts_s` (chain), `market`, `asset_id`, `event_slug`, `slug`, `outcome`, `outcome_index`, `price`, `size`, `notional`, `side`, `wallet`, `name`, `pseudonym`, `tx_hash` |
| `book_snapshots` | `book_snapshots` | level | `ts_ms`, `market`, `asset_id`, `side`, `level` (0 = best), `price`, `size`, `n_bids`, `n_asks` |
| `scores` | `scores` | update | `game_id`, `league_abbr`, `home`, `away`, `status`, `score`, `period`, `live`, `ended` |
| `markets` (dim) | `markets` | condition id (latest record) | all `MarketInfo` fields, `category`, outcome/token 0 and 1, `first_seen`, `last_seen` |
| `tokens` (dim) | `markets` | token id | `asset_id`, `market`, `outcome`, `outcome_index`, `category`, `league`, `event_slug`, `market_type` — the join key for everything above |
| `events` (dim) | `gamma_events` | event id (latest record) | `event_id`, `slug`, `title`, `start_time`, `end_date`, `game_id`, `category`, `league`, `first_seen`, `last_seen` |
| `sessions` | `_system/*/sessions` | connection | `feed`, `session`, `connected_t_ns`, `disconnected_t_ns`, `reason` |

Incremental behaviour:
- Work unit = (table, date, partition). Its inputs (paths + sizes) are recorded in the manifest;
  a unit whose inputs are unchanged is skipped (`--force` rebuilds).
- Today (UTC) is skipped unless `--include-today`, because its files are still growing; even then
  the newest hour of each running file is read only up to its last complete frame.
- Outputs are written to a temp file and renamed, so an interrupted export never leaves a broken
  Parquet file.
- Dimension tables are rebuilt from all `markets` / `gamma_events` records on every run (small).

Docs: `docs/dataset.md` with the schema, the replay rule for `book_updates`, and DuckDB / polars
snippets, e.g.

```sql
-- duckdb
SELECT t.league, count(*), sum(w.notional)
FROM read_parquet('data/parquet/wallet_trades/**/*.parquet', hive_partitioning = true) w
JOIN 'data/parquet/tokens.parquet' t USING (asset_id)
GROUP BY ALL;
```

## Phase 5 — `pm markets`

`pm markets [--category esports|sports|…] [--tag SLUG] [--live] [--top N] [--config FILE]
[--json]`

From Gamma (`/events` with `active=true&closed=false`, paginated; plus `/sports` for league
names):
- Totals: open events, open markets, markets with an order book, live games right now, 24 h
  volume, total volume, liquidity.
- Breakdown by category (Gamma tags: sports, esports, politics, crypto, …) and, for sports and
  esports, by league.
- Top N events and markets by 24 h volume, with live score where there is one.
- With `--config`: which open markets / volume the recorder config covers and the largest
  leagues it does not.

Gamma fields for volume, liquidity and tags are added to `pm-api` as needed, verified against the
live API first.

## Phase 6 — finish

- README and `docs/usage.md`: one section per command; `docs/developer.md`: new crates.
- Remove the obsolete plan items from this file or mark the plan done.

## Open questions / risks

- Volume of `clob_book` for big leagues (CFB) per day may be several GB raw. Export streams
  hour by hour, so memory is bounded by one hour of one partition.
- Arrow/Parquet crate versions move fast; pin exact versions in the workspace.
- Gamma's category tagging is inconsistent; `pm markets` documents which tags it groups by.
