# Strategy 1: CS2 map longshots in quiet markets ("C1")

## Idea
CS2 *map* winner markets (Map 1/2/3) that nobody traded before the game are thin.
- During the game, a team that falls behind on a map is often marked down to 5–30¢.
- At those prices it actually wins more often than the price implies, about 34% of the time at an average price of 24¢.
- This only holds in the quiet markets. In map markets that already had trading before the game, the same buys lose (−4.5% earlier; −9% on the holdout).
- It is a thin-market inefficiency (stale and over-shot quotes, few participants), not a CS2 game effect.

## Rule

**Universe.** Polymarket CS2 map-winner markets ("Map N Winner").
- Keep a market only if it had **no trades before the scheduled series start**.
- Live, that is checkable from the data-api `/trades?market=<condition_id>` call, or from the pm-rs recorder.

**Clock.** Every 60 s from the scheduled series start until the market resolves, read the last trade of the market. It must be at most 30 s old; otherwise skip that tick.

**Signal.** The last trade implies a token price `p` in **[0.05, 0.30]** for either team, where the token price is `price` for that outcome, or `1 − price` of the other outcome.

**One bet per market.** Take the first signal only, and ignore later ones in the same market.

**Execution.**
- **Buy immediately as a taker:** a marketable limit order at most 2¢ above `p`.
- Size **$1–5**; do not chase.
- **Do not rest a bid at `p`.** Resting bids get filled mostly when the team keeps collapsing, and on the holdout that version returned −20%.

**Exit.** Hold to resolution. There is no stop and no take-profit.

## Backtest (after fees; fill = VWAP of the next real taker buys of that token within 120 s)

| Order size | Train (<2026-04) | Test (04–09/2026) | Holdout (09/14–10/16) | Fill observed |
|---|---|---|---|---|
| $1  | +25.9% ± 14.0 (n 235) | +21.7% ± 6.5 (n 792) | +10.8% ± 10.6 (n 339) | 64% / 47% |
| $5  | +12.7% ± 12.1 (n 195) | +21.4% ± 6.8 (n 690) | +8.9% ± 11.4 (n 272) | 55% / 37% |
| $10 | +9.3% ± 12.5 (n 177)  | +19.8% ± 7.2 (n 622) | +4.0% ± 11.4 (n 233) | 50% / 32% |
| $25 | +19.8% ± 14.7         | +24.4% ± 8.1         | +13.0% ± 13.9        | 39% / 24% |
| maker bid at p, 10 min | +14.7% ± 10.5 | +20.1% ± 6.9 | **−20.5% ± 7.7** | 72% |

Notes on the table:
- **"Fill observed"** is the share of signals with enough real buying in the next 120 s to price the fill (main sample / holdout).
- **Signals with no observed buying** still win more often than their price implies. Assuming a fill at the last-trade price, all holdout signals together return +20.6% ± 8.7 before fees. So the taker result is not an artefact of only counting filled signals.
- **Fee drag is about 4%** of stake at 20¢ with the 0.05 fee rate.

## Strengths
- It is the largest per-bet edge found, and it survives taker fees, real tape prices and a holdout month never used for design.
- It is mechanical. The only inputs are the last trade price and "were there pre-game trades".
- It is uncorrelated with the favourite strategy, because it is in-game and on maps rather than pre-game on series.

## Tradeoffs and risks
- **It is noisy.** The holdout estimate (+9% ± 11 at $5) is not significant by itself; the evidence is the consistency across three periods. Expect long losing streaks, since roughly 2 of 3 bets lose.
- **Capacity is tiny.** Real buying near the signal is a median $34 and mean $185 per signal within 10 min. At $5 per bet that is about 125–270 bets per month and **≈ $600–1,350 staked per month**, so expected profit is roughly **$60–150 per month** at +10%.
- **It needs a bot that watches CS2 map markets live.** Signals happen in-game, and the edge is in reacting within a minute.
- **Edge decay.** If other bots start posting in these markets, they will no longer be "markets with no pre-game trades". The filter protects you, but the opportunity count will shrink.
- **Small orders run into the minimum order size.** Polymarket requires a minimum of 5 shares on limit orders; at 20¢ that is $1. That is fine for this strategy.

## Live checklist
1. Paper-trade it with the pm-rs recorder for 2–4 weeks. Log the actual best ask at signal time, which the tape cannot show us.
2. Go live at $1–2 per bet. Compare realised fill price vs `p` and the win rate vs the average price.
3. Stop if, after 300 or more live bets, ROI after fees is below 0, which is about 1 SE below the backtest.

Scripts: `small_orders.py` (this table), `c1_fees.py`, `c1_holdout.py`, `c1_validate.py`.
