# Strategy 2: Strong favourites in thin series markets

## Idea
In series (match-winner) markets with little pre-game money, clear favourites priced 80–90¢ win more often than their price says, about 88–91% of the time.
- In big markets the same favourites are fairly priced or slightly overpriced: takers buying them there lost −4.7%.
- The likely mechanism is that in a thin market the price is set by a few retail longshot buyers.

## Rule
**Universe.** Dota 2, LoL and CS2 series moneylines (match winner), excluding map markets.

**Decision time.** T = scheduled start − 5 min.

**Filter.**
- The favourite's displayed price at T is in **[0.80, 0.90]**.
- Pre-game volume is **< $3,000** of USDC traded before T, as known at T.
- Do **not** filter on final or event volume. That is look-ahead: upsets create in-game volume.

**Execution, either of two:**
- **Taker:** buy the favourite in the 10 min after T, $5–10, limit at most 1¢ above the displayed price.
- **Maker:** rest a bid at the displayed price from T to the start. It fills about 30–45% of the time, pays no fee, and returned a similar ROI.

**Exit.** Hold to resolution.

## Backtest (after fees)

| Execution | Train | Test | Holdout | Fill rate (train/test/holdout) |
|---|---|---|---|---|
| displayed price (reference) | +5.6% ± 2.1 (n 332) | +2.1% ± 2.0 (n 423) | +5.1% ± 2.7 (n 193) | 100% |
| taker, $1 | +7.1% ± 3.2 | +5.7% ± 2.4 | +5.5% ± 3.2 | 37 / 52 / 68% |
| taker, $5 | +5.9% ± 3.7 | +5.1% ± 2.6 | +4.6% ± 3.5 | 30 / 46 / 63% |
| taker, $10 | +5.5% ± 3.9 | +5.1% ± 2.7 | +3.0% ± 4.0 | 29 / 42 / 54% |
| maker bid at display, T→start | +4.9% ± 4.1 | +2.4% ± 3.1 | +7.5% ± 3.9 | 27 / 42 / 44% |

How the taker fill is measured: it is the VWAP of the first $1 / $5 / $10 of real taker buys of the favourite in [T, T+10 min]. Small orders get a slightly better price than the $25 fills used before (+1.1% in test then, +5.1% now). The "fill rate" is the share of games where that much buying actually happened. It is a lower bound on a live fill, because a resting ask may exist with nobody hitting it.

Robustness:
- Earlier (`fav_robust.py`), 33 of 36 variants of lead time and volume cut were positive.

## Strengths
- It is the most consistent effect found: positive in every period and in 33 of 36 variants, with a ~90% hit rate.
- It is easy to run: one decision per game, 5 min before the start, and no live in-game monitoring.
- Maker execution works here, because pre-game prices are stable and there is no in-game adverse selection.

## Tradeoffs and risks
- **The dollar upside is small.** Expect about 35–120 fills per month at $5–10 and ≈ +5%, which is **$10–60 per month**. The thin markets that create the edge also cap the size.
- **The per-bet edge is modest** (+3% to +6%), so a fee change or a rise in the fee rate could erase it. Re-check `fees.py` if Polymarket changes fees.
- **Tail risk:** each loss is −100% of the stake. With a 10% loss rate, runs of 3–4 losses happen.
- **The volume filter needs pre-game trade data** (data-api `/trades` or the recorder).

Scripts: `small_orders.py` (this table), `thin_fav2.py`, `fav_robust.py`, `fav_tape.py`.
