# pmt — design

`pmt` is the trading bot. It runs any number of strategies against live Polymarket data, puts
every order through shared risk limits, sends it to an execution venue (paper for now), and
journals decisions, orders, fills and settlements in the recorder's file format.

```bash
pmt strategies                         # available algorithms
pmt check -c config/pmt.toml           # validate the config, build every strategy
pmt run   -c config/pmt.toml           # trade until Ctrl-C / SIGTERM / SIGHUP
pmt run   --run-for 2m --no-log-file   # smoke test (overrides [run] run_for)
pmt report                             # summary of the journal (see below)
```

## Goals

1. **Adding an algorithm is one file.** A strategy is decision logic only: no sockets, no
   clocks, no order bookkeeping, no risk checks.
2. **Strategies can't break the account.** Risk limits, order tracking, positions and the
   journal belong to the engine, and every strategy goes through them.
3. **Replayable.** Strategies are synchronous and take time from the context, so the same code
   can later run on recorded data (`pm-store` files) for backtests.
4. **Restart-safe.** The journal is replayed at startup, so a restart never decides the same
   market twice and positions survive.
5. **Paper first, live later.** Execution sits behind a `Venue` trait; live signing plugs in
   without touching the strategies.

## Architecture

```text
universe (Gamma poll, 60 s) ──┐
CLOB market channel ──────────┼─► Engine ──► strategies ──► commands ──► risk ──► venue
history (Data API, on demand) ┤      ▲                                            │
timers, 1 s clock ────────────┘      └──────────── order reports ◄────────────────┘
                                      journal ◄── decisions, orders, fills, settlements
```

One task, the **engine**, owns all trading state: markets, books, tapes, orders, positions and
timers. There are no locks. It handles one input at a time:

1. It turns the input into events for the strategies that want the market.
2. It runs the strategies.
3. It carries out the commands they return before taking the next input.

Order reports go back into the same queue, so a fill is seen before the next market-data
message.

| module | role |
|---|---|
| `config.rs` | TOML schema; `[[strategy]]` tables: `kind`, `id`, `enabled`, plus free-form params |
| `universe.rs` | Gamma poll of the configured leagues, up to `max_hours_ahead`; also looks up the events of held positions after they leave the listings |
| `market.rs` | `Market` = `MarketInfo` + `MarketKind` (`Series`, `Map(n)`, `Other`) + event start + fee schedule + winner |
| `state.rs` | `MarketState`: markets, `BookSet`, tapes; `display_price` |
| `tape.rs` | per-market trades: live `last_trade_price` plus Data API history, merged without double counting |
| `history.rs` | Data API loader, one market at a time (~1.4 req/s limit) |
| `strategy/` | `Strategy` trait, `Event`, `Ctx`, registry (`KINDS`), one file per algorithm |
| `order.rs` | `OrderRequest`, `Order`, `Tif` (FAK / FOK / GTC / GTD), `Fill`, `shares_for` |
| `risk.rs` | per-order, per-day and open-exposure limits, tick / min-size checks, `halt` |
| `portfolio.rs` | positions per (strategy, market, token), settlement on resolution, P&L |
| `exec/` | `Venue` trait; `paper.rs` simulates fills |
| `journal.rs` | writes the journal streams; `restore` replays them at startup |
| `engine.rs` | the event loop described above |
| `report.rs` | `pmt report`: summary of the journal |

### Reused from the workspace

| need | from |
|---|---|
| order books, wire formats, exact decimals, fees, display price | `pm-core` (`BookSet`, `MarketMsg`, `Fixed6`, `FeeSchedule`, `OrderBook::display_price`) |
| Gamma events, Data API trades | `pm-api` (`Gamma::league_events`, `DataApi::trades`) |
| market-channel connections for a changing token set | `pm-ws::clob::ClobManager`, moved out of the recorder |
| journal files and their reader | `pm-store` |
| logging, signals, ordered task shutdown | `pm-app`, moved out of `pmd` and the recorder |

## Writing a strategy

```rust
pub trait Strategy: Send {
    fn id(&self) -> &str;
    fn wants(&self, m: &Market) -> bool;            // asked once per new market
    fn on_event(&mut self, ev: &Event, cx: &mut Ctx<'_>);
    fn busy(&self) -> bool { false }                // see "Ending a run"
}
```

**Events.**

- `Market(c)`: the market is new, or its metadata changed.
- `Removed(c)`: the market left the listings.
- `Book(token)`: a book changed.
- `Trade(c, trade)`: a trade printed.
- `History(c)`: history you asked for has loaded.
- `Timer(t)`: a timer you set has fired.
- `Order(report)`: one of your orders filled or finished.
- `Stopping`: the run is winding down (see "Ending a run").

Market events only reach strategies whose `wants` accepted the market. The engine subscribes
books for every wanted market.

**Ctx.**

- Reads: `now_ns`, `market`, `book` (synced only), `tape`, `display_price`, `order`,
  `position`, `has_decided`, `can_enter`, `stop_at`.
- Actions: `place(req) -> OrderId`, `cancel`, `at(t, market, tag)`, `load_history`,
  `note(market, what, msg, detail)`.
- `note` writes to both the log and the `decisions` stream. It also marks the market as
  decided, which survives restarts.

**Steps.**

1. Add `src/strategy/<name>.rs` with a `Params` struct (`#[serde(default, deny_unknown_fields)]`,
   so typos are errors), the strategy type, and `pub const KIND: Kind`.
2. Add `KIND` to `KINDS` in `strategy/mod.rs`.
3. Add a `[[strategy]]` table to `config/pmt.toml`.
4. Test with `strategy::testing::Harness`, which feeds events to the strategy and returns its
   commands (see the two strategies' tests).

**Conventions.**

- Keep one state machine per market, keyed by `ConditionId`.
- Timers carry a `tag`. Re-check the phase and the time when a timer fires, because a start
  time can move and leave stale timers behind.
- Call `note` on every decision, including skips, with the book at that moment (`top_of_book`).
  This is what lets paper trading be compared with the backtest.
- One-shot strategies check `cx.has_decided(c)` first.
- Before deciding on a new market, check `cx.can_enter()`. If it is false, do nothing and
  journal nothing, so the next run can still take the market.
- A strategy that must manage a position until it is closed (selling in-game, hedging) returns
  `true` from `busy()` while it has one, and gets `entry_cutoff` in its config.

## Ending a run

Runs are meant to stop on their own, for example every few days, so a new build can be deployed.
"Stopping" means **winding down**, never pulling the plug:

```toml
[run]
run_for = "3d"           # scheduled stop; leave out to run until stopped
drain_timeout = "4h"     # longest wait for trades in progress

[[strategy]]
kind = "some_in_game_algo"
entry_cutoff = "3h"      # no new trades in the last 3 h before the scheduled stop
```

1. **Before the stop.** A strategy with an `entry_cutoff` stops entering new markets that long
   before the scheduled stop. Everything else runs normally.
2. **At the stop** (or the first Ctrl-C / SIGTERM / SIGHUP): every strategy gets
   `Event::Stopping`, and the engine rejects orders that would start a new trade. A new trade
   is an order in a market where the strategy has no position and no working order. Orders in
   markets it is already in (exits, hedges, the rest of a partial fill) are still accepted.
   Market data, timers and the venue keep running.
3. **The end.** The run ends once no order is working and no strategy is `busy()`. Otherwise
   it ends when `drain_timeout` passes: working orders are cancelled, and `pmt run` exits
   non-zero so the cut-off shows.
4. **A second signal** ends the run at once: working orders are cancelled and the journal is
   closed. Positions stay as they are.

Positions held to resolution (c1, fav) do not keep the process up. They settle without it, and
the next run restores them from the journal and settles them when Gamma reports the result.
Markets left undecided while winding down are decided by the next run.

**Deploying.** Run it in a restart loop and rebuild whenever you like. Each scheduled stop
starts the latest binary:

```bash
tmux new -d -s pmt 'cd ~/trading/pm-rs && \
  while true; do ./target/release/pmt run; sleep 10; done'
git pull && cargo build --release      # takes effect at the next stop
tmux send-keys -t pmt C-c              # or wind down now; the loop restarts the new build
```

`C-c` asks for a wind-down. Press it twice to stop at once. To stop for good, kill the loop
with `tmux kill-session -t pmt`, which also winds the bot down: SIGHUP counts as the first signal.

## The two strategies

### `cs2_map_longshot` (c1)

Doc: [01_cs2_map_longshot.md](trading_algo/01_cs2_map_longshot.md).

Phases: `Waiting` → (series start) `Checking` → `Watching` → `Done`.

- **At the start:** load history. Any trade before the start excludes the market. A truncated
  history counts as not quiet.
- **Every 60 s:** if the last trade is ≤ 30 s old and implies 0.05–0.30 for either team, buy
  that team with a FAK at most 2¢ above (on the tick grid). Stake is $2, and at least the
  5-share minimum.
- **Once per market.** If the FAK finds no ask within the limit it is simply cancelled. The doc
  says not to rest a bid.

### `thin_favourite` (fav)

Doc: [02_thin_favourite.md](trading_algo/02_thin_favourite.md).

Phases: `Waiting` → (T = start − 5 min) `Checking` → `Armed` / `Taking` → `Done`.

- **At T:** load history, then wait until both books are synced.
- **Filter:** the favourite's displayed price (mid, or last trade if the spread > 10¢) must be
  0.80–0.90, and the USDC traded before T must be < $3,000.
- **Taker:** FAK at display + 1¢ whenever the ask allows, until T + 10 min, re-arming after
  partial fills while a minimum order is still affordable.
- **Maker:** a GTD bid at the displayed price that expires at the start.
- **No books by T + 10 min:** skip.

## Paper venue

- **Marketable orders:** they walk the opposite side of the live book up to the limit and pay
  the market's taker fee, `size × rate × (p(1−p))^exponent` from Gamma's `feeSchedule`. The book
  is not consumed.
- **Resting orders:** they fill when a taker trade on the same token prints at or through the
  price, up to that trade's size, with no fee. Queue position is ignored, so this is optimistic.
  It's fine for the thin markets these strategies trade, but don't trust it for busy books.

## Journal

```text
<journal_dir>/YYYY/MM/DD/<mode>/<strategy>/{decisions,orders,fills,settlements}/HH.r<run>.jsonl.zst
```

| stream | what |
|---|---|
| `decisions` | `{strategy, what, market, question, event, msg, detail}`; `detail` holds prices, volume and the book |
| `orders` | the whole `Order` on creation and on every status change |
| `fills` | order id, strategy, market, token, side, event slug, league, price, size, fee, maker |
| `settlements` | strategy, market, token, won, shares, cost, payout, P&L |

`paper` and `live` are separate categories, so their positions never mix. Read the journal with
`zstdcat`, `pm_store::read`, or the pmd tools.

**At startup:** the last 30 days are replayed to rebuild:

- positions and realised P&L;
- each strategy's decided markets (any note, or any non-rejected order);
- the order-id counter.

**At shutdown:** working orders are cancelled.

## Report

`pmt report` reads the journal (`journal_dir` and `mode` of the config, or `--journal-dir`
and `--mode`) and prints four sections:

- **activity** per strategy: signals, skips, orders (filled / unfilled / rejected / still
  open), USDC staked and fees.
- **results** per strategy, over settled bets:
  - bets and wins, win rate;
  - cost per share with fees, which is the win rate needed to break even;
  - P&L, ROI, and ROI per bet ± its standard error (the statistic the strategy docs use);
  - slippage: fill VWAP minus the signal price, averaged over markets.
- **by day / month**: orders, staked, bets settled and won, P&L, ROI, cumulative P&L. P&L
  counts on the day a bet settles.
- **open positions** as of now.

```bash
pmt report --from 2026-10-01 --strategy c1     # one strategy since a date (UTC days)
pmt report --by month                          # monthly P&L table
pmt report --json | jq .total                  # machine-readable
```

Windowing: decisions, fills and settlements count on their own day, orders on the day they were
created.

## Tests

```bash
cargo test -p pmt          # all offline, a few seconds
```

| where | covers |
|---|---|
| `engine.rs` tests | the whole loop with the paper venue and a real journal: order → fill → settlement → restore; resolution from the market channel kept over a stale Gamma poll; sell exits counted once (portfolio, journal, report); rejections reaching the strategy; shared history loads; timer order; cancel ownership; order ids across restarts; entry cutoff, wind-down, drain timeout, forced stop |
| `strategy/*` tests | each strategy through `testing::Harness`: signals and filters, one bet per market, restarts, paused markets, retries after rejections and empty fills, winding down |
| `risk.rs`, `exec/paper.rs`, `portfolio.rs`, `tape.rs`, `state.rs`, `journal.rs`, `report.rs` | the limits, simulated fills (both sides, resting, FIFO), P&L, history merging, the journal round trip, the report's numbers |
| `tests/cli.rs` | the binary: `strategies`, `check` (the shipped config and every kind of mistake), `run` refusing a bad config, `report` on a journal |

`Script` in the engine tests is a strategy built from a closure: use it to drive the engine
from a test without writing a strategy.

## Not done yet

The full, ordered list (including real trading) is in [todo.md](todo.md).


- **Live execution (`crates/pm-exec`):**
  - L1/L2 API-key auth and EIP-712 order signing for the proxy wallet;
  - `POST /order` and `DELETE /order`;
  - the user websocket channel for fills.

  It implements `Venue`: `submit` returns the acknowledgement, and fills arrive on a channel
  the engine also listens to (add it to `Inputs`). Turn `mode = "live"` on only after that
  works.
- **Backtest driver:** feed recorded `clob_book` / `clob_trades` / `markets` streams through the
  engine, with a clock taken from the records.
- **Live scores:** the sports feed into `Event` (e.g. for in-game strategies keyed on the map
  score).
- **Armed taker after a restart:** a market that was armed but unfilled is not re-armed after a
  restart (it is already decided). This is conservative, and fine at these sizes.
