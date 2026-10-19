# pmt — todo

Ordered roughly by when it is needed. Real trading (section 2) is the big one; paper trading
(section 1) should run for 2–4 weeks first, as the strategy docs ask.

Endpoint names, contract addresses and field formats below are from memory of Polymarket's
CLOB. **Verify each one against the current docs and the official clients**
(`py-clob-client`, `clob-client`) before relying on it.

## 1. Before going live (paper)

- [ ] Run c1 and fav in paper mode for 2–4 weeks: `run_for = "3d"` in the tmux restart loop.
  Check `pmt report` weekly:
  - [ ] Fill price against the signal price (slippage column).
  - [ ] Win rate against cost per share.
  - [ ] ROI per bet ± SE against the backtest tables.
- [ ] Confirm the taker-fee formula (`FeeSchedule::taker_fee`) against a real fill or the docs.
  The live `feeSchedule` shows rate 0.03, while the strategy docs assumed 0.05.
- [ ] Make paper fills more honest:
  - [ ] Consume the book: two orders should not take the same ask.
  - [ ] Optionally add latency: fill against the book N ms after the decision.
  - [ ] For resting orders, model queue position (fill only after the size ahead of us has
    traded).
- [ ] Daily summary: log `pmt report --from <yesterday>` at UTC midnight, or send it as a
  notification.
- [ ] Alerts for:
  - [ ] the engine stopping on drain timeout or an error;
  - [ ] no market data for N minutes;
  - [ ] universe polls failing repeatedly.
- [ ] Back up the journal: it is the record of every decision and fill.

## 2. Real trading

### 2.1 Account and wallet (one-off, manual)

- [ ] Check that trading on Polymarket is allowed where the bot runs. Polymarket blocks some
  jurisdictions, so check its terms. Run the bot only where that is permitted.
- [ ] Decide the wallet type and record it:
  - an email/Magic proxy wallet;
  - a browser-wallet Gnosis Safe proxy;
  - a plain EOA.

  This sets the order `signatureType` (0 EOA, 1 POLY_PROXY, 2 POLY_GNOSIS_SAFE) and the `funder`
  (the proxy that holds the funds) versus the `signer` (the key that signs).
- [ ] Fund it with the collateral token on Polygon (USDC.e historically; verify the current one).
- [ ] Approvals, once per wallet. The web UI usually does this for proxy wallets; an EOA needs
  them sent on-chain.
  - [ ] Collateral `approve` to the CTF Exchange, the Neg Risk CTF Exchange and the Neg Risk
    Adapter.
  - [ ] Conditional Tokens `setApprovalForAll` to the same three, which is needed to sell.
- [ ] Key handling:
  - [ ] The private key comes from an environment variable or a file with `0600` permissions.
  - [ ] It is never stored in `config/pmt.toml`, the journal or the log.
  - [ ] Use a dedicated wallet holding only the bankroll.

### 2.2 `crates/pm-exec`: authenticated CLOB client

- [ ] EIP-712 signing (`alloy` or `ethers-signers` + `k256`).
- [ ] **L1 auth:** sign the `ClobAuth` message to create or derive the API key
  (`POST /auth/api-key`, `GET /auth/derive-api-key`), using the headers
  `POLY_ADDRESS / POLY_SIGNATURE / POLY_TIMESTAMP / POLY_NONCE`.
- [ ] **L2 auth:** HMAC-SHA256 request signing with the API secret, using the headers
  `POLY_ADDRESS / POLY_API_KEY / POLY_PASSPHRASE / POLY_TIMESTAMP / POLY_SIGNATURE`. Cache the
  API credentials (secret file) and re-derive them if they are missing.
- [ ] **Order building:** the EIP-712 `Order` struct.
  - [ ] Fields: salt, maker (funder), signer, taker (zero), tokenId, makerAmount, takerAmount,
    expiration, nonce, feeRateBps, side, signatureType.
  - [ ] Choose the verifying contract by `neg_risk` (CTF Exchange or Neg Risk CTF Exchange).
  - [ ] Get `feeRateBps` right; the server rejects a wrong one. Read it from the market or
    endpoint the clients use.
  - [ ] Get the amount rounding right: price on the market's tick, size and USDC amounts to the
    decimals the clients use per tick size. Wrong rounding is the usual cause of "invalid
    amount" rejections.
- [ ] **Endpoints:**
  - [ ] `POST /order` for one order, with `orderType` GTC / GTD / FOK / FAK.
  - [ ] Batch post, if needed.
  - [ ] `DELETE /order`, `DELETE /orders` and cancel-all.
  - [ ] `GET` open orders and trades, for reconciliation.
  - [ ] Collateral balance and allowance.
- [ ] **Errors:** map responses to `ExecReport::Done { Rejected, reason }`: insufficient
  balance or allowance, tick, minimum size, market closed, rate limited.
- [ ] Rate limits: per-endpoint token buckets, and back off on 429.
- [ ] Clock: compare `Clob::server_time` at startup and refuse to start if the skew exceeds a
  few seconds, because GTD expiries and auth timestamps depend on it.
- [ ] Unit tests: fixed test vectors (order hash and signature, L2 HMAC) generated with the
  official Python client and asserted byte for byte.

### 2.3 User channel (fills)

- [ ] Authenticated `wss://…/ws/user` connection through `pm-ws`, subscribed for the bot's
  markets or all of them.
- [ ] Map `order` events (placement, update, cancellation) and `trade` events (matched, mined,
  confirmed, failed) to `ExecReport`s.
  - [ ] Decide when a fill counts: at "matched" (fast) or at "confirmed" (safe). Handle
    "failed" by reversing the fill.
- [ ] Treat a disconnect as a gap: after reconnecting, reconcile open orders and recent trades
  over REST.

### 2.4 `LiveVenue` in pmt

- [ ] Implement `exec::Venue`:
  - [ ] `submit` signs and posts asynchronously and returns nothing at once. The
    acknowledgement and fills arrive later through a new `Inputs::exec` channel.
  - [ ] `cancel` sends the cancel request.
  - [ ] `on_trade` and `on_clock` do nothing; the exchange handles expiry.
- [ ] Engine: add `Inputs::exec` to the select loop, plus a pending state between "sent" and
  "acknowledged". Treat an order that is never acknowledged within N seconds as unknown and
  reconcile it.
- [ ] Map our `OrderId` to the exchange's order id. Journal both, plus the order hash.
- [ ] Idempotency: if a post times out, look the order up by hash before re-sending, so no
  order is ever sent twice.
- [ ] **Startup reconciliation** for live mode:
  - [ ] Open orders on the exchange that the journal doesn't know: cancel them, or adopt them.
  - [ ] Positions from the Data API `/positions` against the journal's positions: warn on any
    difference.
- [ ] **Shutdown:** a wind-down cancels on the exchange. On a forced stop, still send
  cancel-all for the bot's orders before exiting.
- [ ] Turn on `mode = "live"` in `Config::parse`, but require an explicit second switch
  (e.g. `--i-understand-this-is-real-money` or `[live] confirm = true`).
- [ ] Choose the venue in `lib::run`: `Paper`, or `LiveVenue` built from `pm-exec`.

### 2.5 Live risk

- [ ] Balance check before each buy (cached collateral balance minus resting buys).
- [ ] Hard caps that are lower for the first weeks: max $2 per order, $20 per day.
- [ ] Kill switches:
  - [ ] stop entering after N consecutive rejections;
  - [ ] stop if realised P&L for the day falls below −X;
  - [ ] a `halt` file in the journal directory that stops new trades without a restart.
- [ ] A per-market maximum position (shares and USDC), so a retry loop can never stack up a
  large position.
- [ ] Settlement: winning shares need redeeming (CTF `redeemPositions`, or through the UI) to
  turn back into collateral. Automate this or schedule it; until then, count unredeemed
  winnings separately from the balance.

### 2.6 Rollout

1. [ ] **Dry run:** build and sign real orders, log them, but don't post. Compare them with
   orders the official client builds for the same inputs.
2. [ ] Post one tiny GTC far from the market, check it on the website, then cancel it.
3. [ ] Live with c1 only at the 5-share minimum (about $1–1.50 at 20–30¢) for 1–2 weeks,
   running paper next to it to compare fills.
4. [ ] Then fav, at $5.
5. [ ] Apply the stop rule from the c1 doc: after 300+ live bets, stop if ROI after fees < 0.

## 3. Research and strategy infrastructure

- [ ] **Backtest driver:** replay recorded `clob_book` / `clob_trades` / `markets` /
  `gamma_events` (pmd's recordings) through the same engine and strategies, with the clock
  taken from the records, and write a journal that `pmt report` can read.
- [ ] Live scores (sports feed) as an `Event`, for in-game strategies such as map score or
  round count.
- [ ] Event-level grouping: strategies such as c1 see map markets independently, so add an
  "event" view (series plus its maps) to `Ctx` for strategies that need it.
- [ ] A per-strategy bankroll and allocation, instead of only global risk limits.
- [ ] `pmt run --only c1,fav` to pick strategies without editing the config.

## 4. Operations

- [ ] A systemd unit (or the tmux loop) with `Restart=always`, and log rotation for
  `trade.log`.
- [ ] Ship the release build with a version or git hash in the startup log, so the journal
  shows which build made each trade.
- [ ] Put the run id or version on every journal record for later attribution.
- [ ] A health line: last market-data age, connections up, queue depth, and lag between
  receiving and handling a message.
