# Recorded data format (`pmd record`)

What each field means in the raw files `pmd record` writes, with a short real example of
every message type. For where the files are and how to read them, see
[usage.md §4](usage.md#4-output). For the typed Parquet tables built from them, see
[dataset.md](dataset.md).

Examples are taken from the 2026-10-17 recording. They are pretty-printed and shortened: ids
are cut to `0x8e1d…cc1f`, and long lists or uninteresting fields are dropped (`…`). In the files,
each record is one line.

- [Files and the envelope](#files-and-the-envelope)
- [Number and time formats](#number-and-time-formats)
- CLOB market feed: [`clob_book`](#clob_book) · [`clob_bbo`](#clob_bbo) ·
  [`clob_trades`](#clob_trades) · [`clob_other`](#clob_other)
- Recorder book state: [`book_snapshots`](#book_snapshots)
- Activity feed (RTDS): [`trades`](#trades)
- Sports feed: [`scores`](#scores)
- Metadata: [`markets`](#markets) · [`gamma_events`](#gamma_events)
- Connections: [`sessions`](#sessions)
- [Joining streams](#joining-streams)

## Files and the envelope

```
<data_dir>/YYYY/MM/DD/<category>/<league>/<stream>/HH.r<run>.jsonl.zst
```

- `YYYY/MM/DD` and `HH` are the UTC date and hour of the receive time `t`.
- `<run>` is the start time of the recorder process, in Unix seconds. Every restart writes new
  files, so a restart never overwrites earlier data.
- `<category>/<league>` comes from the recorder config, e.g. `esports/cs2`.
- There are three special partitions:
  - `other/<abbr>`: scores of leagues that are not configured; `other/untracked` holds trades
    in unrecorded markets (only with `only_tracked = false`).
  - `_system/<feed>`: connection markers.
  - `_system/unknown`: CLOB events whose market could not be routed. It should stay empty.
- A file is a series of zstd frames of newline-separated JSON. The recorder flushes every 5 s,
  so the file of the current hour can end in an unfinished line.

Every line is an **envelope** around the message:

```json
{"t": 1792262528835279130, "s": 6, "m": { … }}
```

| field | meaning |
|---|---|
| `t` | when the recorder received the message (Unix nanoseconds, UTC). For records the recorder makes itself (`markets`, `gamma_events`, `book_snapshots`), when it wrote them |
| `s` | websocket session: one id per connection, counting from 1 in each run, so `(run, s)` is unique. It joins the `sessions` markers. `0` = not from a websocket (the records the recorder makes itself) |
| `m` | the message. Exactly as the server sent it, except that `price_change` loses its `hash` fields (unless `keep_hashes = true`) |

Lines are in the order they were written, which is close to `t` order but not exactly. Several
connections feed the same file, and a record received a few ms before the hour boundary may
land in the next hour's file. Sort by `t` when order matters.

## Number and time formats

The three Polymarket services do not agree on types, so check them before you compute:

| source | prices and sizes | timestamps |
|---|---|---|
| CLOB (`clob_*`) | strings: `"0.38"`, `"5"` | string, Unix **ms**: `"1792262528827"` |
| RTDS (`trades`) | numbers: `0.8`, `5` | outer `timestamp`: number, Unix **ms**. `payload.timestamp`: number, Unix **seconds** |
| sports (`scores`) | — | `finishedTimestamp`: RFC 3339 |
| Gamma (`gamma_events`) | mostly numbers, but some strings: `"volume": "31.57"`, and JSON-in-a-string for `outcomes`, `outcomePrices`, `clobTokenIds` | RFC 3339 (`2026-10-17T07:00:00Z`), or `2026-10-17 07:00:00+00` for `gameStartTime` |

Prices are in USDC per share, between 0 and 1. A share pays 1 USDC if its outcome wins. Sizes
are in shares.

## CLOB market feed

From the `wss://ws-subscriptions-clob.polymarket.com/ws/market` order-book feed. The recorder
spreads the tokens over several connections (`clob-0`, `clob-1`, …, at most 500 tokens each).
Every message names the market (`market`, the condition id) and the outcome token (`asset_id`).
A binary market has two tokens, whose prices mirror each other: a bid at 0.06 on one token is
an ask at 0.94 on the other.

### `clob_book`

Everything needed to rebuild the full order book (level 2), as three message types. The
`event_type` field tells them apart.

**`book`**: the full book of one token. The server sends one when the recorder subscribes. The
recorder also resubscribes a token when its book stops adding up, which produces a fresh one.

```json
{"t": 1792261692686801302, "s": 19, "m": {
  "event_type": "book",
  "market": "0x8e1d…cc1f",
  "asset_id": "9731…3717",
  "timestamp": "1792261673183",
  "hash": "23a4842b6ba97263a77569e76376c078b2aa13ee",
  "bids": [{"price": "0.001", "size": "320"}, …, {"price": "0.167", "size": "9.56"}],
  "asks": [{"price": "0.999", "size": "633.89"}, …, {"price": "0.511", "size": "39.25"}],
  "tick_size": "0.001",
  "last_trade_price": "0.750"
}}
```

| field | meaning |
|---|---|
| `timestamp` | server time of the book (ms) |
| `hash` | server's hash of the book state; not needed for replay |
| `bids` / `asks` | every level, as `{price, size}`. `size` is the total resting shares at that price. **Best price is last**: bids ascend, asks descend |
| `tick_size` | minimum price increment |
| `last_trade_price` | last traded price, `""` if none yet |

**`price_change`**: one or more levels changed. One message can cover both tokens of a market
(an order on one side shows on the other at the mirrored price).

```json
{"t": 1792245600000225335, "s": 20, "m": {
  "event_type": "price_change",
  "market": "0xa512…4b94",
  "timestamp": "1792245599991",
  "price_changes": [
    {"asset_id": "1200…7147", "price": "0.06", "size": "5.06", "side": "BUY",  "best_bid": "0.06", "best_ask": "0.95"},
    {"asset_id": "7000…9955", "price": "0.94", "size": "5.06", "side": "SELL", "best_bid": "0.05", "best_ask": "0.94"}
  ]
}}
```

| field | meaning |
|---|---|
| `price_changes[].side` | `BUY` = a bid level, `SELL` = an ask level |
| `price_changes[].size` | the **new total** at `price` (not a delta); `"0"` removes the level |
| `price_changes[].best_bid` / `best_ask` | the server's top of book for that token after the change |

When you replay, drop any bid above `best_bid` and any ask below `best_ask` after each change.
The full rule is in [dataset.md](dataset.md#time-series-tables).

**`tick_size_change`**: the token's minimum price increment changed. This usually happens as
the price nears 0 or 1.

```json
{"t": 1792224209762749037, "s": 15, "m": {
  "event_type": "tick_size_change",
  "market": "0x7c51…7353", "asset_id": "1127…2066",
  "old_tick_size": "0.01", "new_tick_size": "0.001",
  "timestamp": "1792224209752"
}}
```

### `clob_bbo`

`best_bid_ask`: the token's top of book changed.

```json
{"t": 1792262528303134070, "s": 6, "m": {
  "event_type": "best_bid_ask",
  "market": "0x3fbb…b9b4", "asset_id": "3444…2610",
  "best_bid": "0.05", "best_ask": "0.92", "spread": "0.87",
  "timestamp": "1792262528271"
}}
```

`spread` = `best_ask − best_bid`. This is the cheapest stream for top-of-book prices. It has
fewer messages than `clob_book`, but it sometimes goes quiet while `clob_book` keeps flowing
(`pmd audit` reports this).

### `clob_trades`

`last_trade_price`: one per taker order that traded, even if it filled against several makers.

```json
{"t": 1792262528835279130, "s": 6, "m": {
  "event_type": "last_trade_price",
  "market": "0xbfc4…b3bf", "asset_id": "7587…5153",
  "price": "0.38", "size": "5", "side": "BUY",
  "fee_rate_bps": "0",
  "timestamp": "1792262528827",
  "transaction_hash": "0xfd45…be10f"
}}
```

| field | meaning |
|---|---|
| `price`, `size` | trade price and shares |
| `side` | the **taker's** side: `BUY` lifted the ask, `SELL` hit the bid |
| `fee_rate_bps` | taker fee rate in basis points |
| `transaction_hash` | the on-chain settlement transaction; joins `trades.payload.transactionHash` |

### `clob_other`

Every other CLOB message type for a recorded market. Mostly `market_resolved`, plus any type the
recorder does not know yet. The global `new_market` broadcast is not kept. Fields vary by type;
all have `event_type` and `market`.

## `book_snapshots`

Made by the recorder, not received. Every 60 s, it writes the top 50 levels of each book that
changed since the last snapshot. The recorder keeps these books from `clob_book` and repairs
them by the rule above, so a snapshot needs no replay. `s` is `0`.

```json
{"t": 1792222956575147880, "s": 0, "m": {
  "market": "0x9aae…a320", "asset_id": "1000…1726",
  "ts": 1792222867230,
  "bids": [["0.04", "14.38"], ["0.03", "33.33"], ["0.02", "51.2"], ["0.01", "57.35"]],
  "asks": [["0.96", "15.16"], ["0.97", "33.33"], ["0.98", "78.28"], ["0.99", "45.35"]],
  "n_bids": 4, "n_asks": 4
}}
```

| field | meaning |
|---|---|
| `ts` | server timestamp (ms) of the last update applied to the book. It is older than `t` if the book was quiet |
| `bids` / `asks` | `[price, size]` pairs, **best price first** (the opposite of `book`) |
| `n_bids` / `n_asks` | the book's full depth; it can be more than the 50 levels written |

`book_snapshots` is configured by `book_snapshot_secs` (`0` turns it off) and
`book_snapshot_levels`.

## `trades`

From the real-time data service (RTDS, `wss://ws-live-data.polymarket.com`), topic `activity`.
Each record is one **fill of one wallet**, with the wallet's public profile. A taker order that
fills against three makers appears several times: once for the taker and once for each maker.
Several of those fills can have the same size at different prices. Do not de-duplicate on
`(transactionHash, asset, proxyWallet, size)`: that drops real fills.

```json
{"t": 1792261692363486089, "s": 1, "m": {
  "topic": "activity", "type": "trades",
  "timestamp": 1792261692061,
  "connection_id": "gd2Gik6luWeIKEjzKA==",
  "payload": {
    "asset": "8951…0961",
    "conditionId": "0x6306…00f4",
    "eventSlug": "cs2-nip-mibr-2026-10-17",
    "slug": "cs2-nip-mibr-2026-10-17-game2",
    "title": "Counter-Strike: NIP vs MIBR - Map 2 Winner",
    "outcome": "NIP", "outcomeIndex": 0,
    "side": "BUY", "price": 0.8, "size": 5, "fee": 0.04,
    "timestamp": 1792261692,
    "transactionHash": "0xc6ec…9b8c",
    "proxyWallet": "0xdDb6…b9B3",
    "name": "baseinv3", "pseudonym": "Physical-Enjoyment",
    "bio": "", "profileImage": "", "icon": "https://…/counter-strike-image.png"
  }
}}
```

| field | meaning |
|---|---|
| `timestamp` (outer) | RTDS send time (ms) |
| `connection_id` | RTDS's id for our connection; changes on reconnect |
| `payload.asset` | outcome token id (= CLOB `asset_id`) |
| `payload.conditionId` | market (= CLOB `market`) |
| `payload.eventSlug`, `slug`, `title` | the event, the market's slug, and the market's question |
| `payload.outcome`, `outcomeIndex` | the token's outcome name and its position in the market's outcomes |
| `payload.side`, `price`, `size` | this wallet's side of the fill, the price, and shares |
| `payload.fee` | fee charged on this fill, in USDC. Present on about a third of rows |
| `payload.timestamp` | on-chain time (**seconds**) |
| `payload.transactionHash` | settlement transaction; one transaction holds several fills |
| `payload.proxyWallet` | the trader's Polymarket wallet: the stable trader id |
| `payload.name`, `pseudonym`, `bio`, `profileImage` | the trader's public profile; `name` can be empty |
| `payload.icon` | the event's image |

Only trades in markets the recorder records are kept. With `only_tracked = false`, the rest go
to `other/untracked`. A few kept trades are in markets of a recorded event whose market type is
excluded, so they have no `markets` record with `tracked: true`.

## `scores`

From the sports feed (`wss://sports-api.polymarket.com/ws`): the live state of every game
Polymarket follows, in all leagues. A message is sent when something changes. Configured leagues
are filed under their own partition (the `score_league` setting maps the feed's abbreviation to
the league). Other leagues go to `other/<leagueAbbreviation>`.

```json
{"t": 1792262528304491591, "s": 2, "m": {
  "gameId": 1731167, "leagueAbbreviation": "cs2",
  "homeTeam": "NIP", "awayTeam": "MIBR",
  "status": "running", "score": "000-000|1-0|Bo3", "period": "2/3",
  "live": true, "ended": false
}}
```

| field | meaning |
|---|---|
| `gameId` | the game; joins `markets.game_id` and `gamma_events.gameId`. Missing for cricket, which sends `metadataGameId` |
| `metadataGameId` | string game id used instead of `gameId` by some leagues (cricket) |
| `leagueAbbreviation` | the feed's league code: `cs2`, `lol`, `nfl`, `epl`, … |
| `homeTeam`, `awayTeam` | team (or player) names; abbreviations in US sports |
| `status` | provider-specific, so the values vary: `running`, `inprogress`, `InProgress`, `Break`, `halftime`, `not_started`, `finished`, `Final` |
| `score` | home–away; the format depends on the sport (below) |
| `period` | current part of the game (below) |
| `elapsed` | game clock where there is one: soccer minutes `"43"`, US football clock `"15:00"` |
| `live`, `ended` | in play; finished. Use these, not `status` |
| `finishedTimestamp` | when the game ended (RFC 3339); only on the final messages |
| `sportradarGameId`, `turn`, `turnProviderId` | US sports only: Sportradar's id, and the team in possession |

Score and period formats seen:

| sport | `score` | `period` |
|---|---|---|
| esports | `"<in-map rounds>\|<maps won>\|Bo<n>"`, e.g. `000-000\|1-0\|Bo3` (map 1 won by home, map 2 at 0–0) | `"2/3"` = map 2 of 3 |
| esports, Bo5 | `"…\|2-1\|Bo5"` | `"4/5"` |
| soccer | `"0-0"` | `1H`, `HT`, `2H`, `FT`, `VFT` |
| tennis | sets, `"6-1, 7-5"` | `S1`…`S3`, tiebreaks `TB1`…, `LIVE`, `FT` |
| American football | `"0-0"` | `Q1`…`Q4`, `End Q1`, … |
| ice hockey | `"0-0"` | `P1`…`P3`, `End P1`, … |
| cricket | `"147-0"` | `Scheduled`, `Live`, `FT`, `CAN` (cancelled) |

Cricket example:

```json
{"t": 1792223075613210303, "s": 2, "m": {
  "metadataGameId": "id2703133775199214", "leagueAbbreviation": "cricket",
  "score": "147-0", "period": "Live", "live": true, "ended": false
}}
```

## `markets`

Made by the recorder from the Gamma event list, one record per market whenever it changes. The
first poll of a run writes every market. Discovery polls every 60 s. `s` is `0`.

```json
{"t": 1792222896849789959, "s": 0, "m": {
  "condition_id": "0x30b0…34e8", "market_id": "5308156",
  "slug": "cs2-ssp1-lei-2026-10-05",
  "question": "Counter-Strike: Sissi State Punks vs Team LEISURE (BO3) - DACH CS Masters Regular Season",
  "league": "cs2", "event_id": "1133922", "event_slug": "cs2-ssp1-lei-2026-10-05",
  "market_type": "moneyline",
  "outcomes": ["Sissi State Punks", "Team LEISURE"],
  "token_ids": ["1953…2447", "1634…1174"],
  "game_start_time": "2026-11-11 17:00:00+00", "game_id": 1661619,
  "tick_size": "0.01", "min_order_size": 5.0, "neg_risk": false,
  "active": true, "closed": false, "accepting_orders": true,
  "tracked": true
}}
```

| field | meaning |
|---|---|
| `condition_id` | the market (= CLOB `market`, RTDS `conditionId`) |
| `market_id` | Gamma's numeric market id |
| `slug`, `question` | URL slug and question text |
| `league` | the config's league key |
| `event_id`, `event_slug` | the Gamma event the market belongs to |
| `market_type` | Gamma `sportsMarketType`: `moneyline` (match winner), `child_moneyline` (map/game winner), `map_handicap`, `totals`, `round_handicap_game_1`, `round_over_under_game_2`, …; `null` for futures and props |
| `outcomes`, `token_ids` | outcome names and their CLOB token ids, in the same order (`outcomeIndex`) |
| `game_start_time` | scheduled start, as Gamma gives it; `null` for futures |
| `game_id` | joins `scores.gameId`; `null` for futures |
| `tick_size`, `min_order_size` | minimum price increment and order size (shares) at the time |
| `neg_risk` | part of a negative-risk group (mutually exclusive outcomes across markets) |
| `active`, `closed`, `accepting_orders` | Gamma's market status |
| `tracked` | whether the recorder records this market's order book and trades. `false` when the market is not tradable, its type is excluded by the config, or its game ended a while ago |

## `gamma_events`

The **raw** event JSON from Gamma's `/events` endpoint, nested markets included. It is written
the first time an event is seen, again whenever its trading state changes, and at least every
6 h. Changes that count: the event's live, ended, closed, score, period or start time; a
market's active, closed, accepting-orders or start time; and a closed market's final prices.
Price, volume and liquidity changes alone do not trigger a record. `s` is `0`.

Gamma has about 60 event fields and 80 per market, and they change over time. The useful ones:

```json
{"t": 1792222896865183339, "s": 0, "m": {
  "id": "1202871", "slug": "cs2-offcho-rstr-2026-10-17", "ticker": "cs2-offcho-rstr-2026-10-17",
  "title": "Counter-Strike: Off Chops vs Rooster (BO3) - Logitech G Pro Series Playoffs",
  "startTime": "2026-10-17T07:00:00Z", "endDate": "2026-10-17T13:00:00Z", "eventDate": "2026-10-17",
  "gameId": 1746370, "live": true, "ended": false, "score": "0-0|0-0|Bo3", "period": "1/3",
  "active": true, "closed": false, "seriesSlug": "counter-strike",
  "volume": 13070.70, "volume24hr": 13085.41, "liquidity": 1339.03, "openInterest": 11296.26,
  "sport": {"sport": "cs2", "name": "CS2", "resolution": "https://hltv.org", …},
  "teams": [{"name": "Off Chops", "abbreviation": "offcho", "ordering": "home", …},
            {"name": "Rooster",   "abbreviation": "rstr",   "ordering": "away", …}],
  "eventMetadata": {"league": "Logitech G Pro Series", "leagueTier": "5", "tournament": "Playoffs", …},
  "tags": [{"id": "1", "slug": "sports", "label": "Sports"}, {"id": "64", "slug": "esports"}, …],
  "series": [{"id": "10310", "slug": "counter-strike", "recurrence": "daily", …}],
  "markets": [{
    "id": "5792499", "conditionId": "0x7c51…7353",
    "question": "Counter-Strike: Off Chops vs Rooster - Map 1 Winner",
    "groupItemTitle": "Map 1 Winner", "sportsMarketType": "child_moneyline",
    "outcomes": "[\"Off Chops\", \"Rooster\"]",
    "outcomePrices": "[\"0.67\", \"0.33\"]",
    "clobTokenIds": "[\"1127…2066\", \"4242…7129\"]",
    "bestBid": 0.51, "bestAsk": 0.83, "lastTradePrice": 0.96,
    "volume": "31.578317", "orderPriceMinTickSize": 0.01, "orderMinSize": 5,
    "gameStartTime": "2026-10-17 07:00:00+00",
    "active": true, "closed": false, "acceptingOrders": true, "negRisk": false, …
  }, …],
  "description": "…", "resolutionSource": "https://kick.com/dfragtv", …
}}
```

| field | meaning |
|---|---|
| `id`, `slug`, `ticker`, `title` | the event |
| `startTime` | scheduled game start (game events only); `startDate` is when the event was listed |
| `endDate` | when Gamma expects it to resolve |
| `gameId`, `live`, `ended`, `score`, `period` | live state, same meaning as in `scores`; only on game events |
| `volume`, `volume24hr`, `volume1wk`, … | traded USDC, all time and over a window |
| `liquidity`, `openInterest` | USDC resting in the books; USDC in open positions |
| `sport.sport` | Gamma's league code (`cs2`, `nfl`, …) |
| `teams[]` | the two sides, `ordering` = `home` / `away` |
| `eventMetadata` | provider extras: tournament, tier, external match ids |
| `tags[]`, `series[]`, `seriesSlug` | Gamma's categorisation (`tags[].slug`: `sports`, `esports`, `cs2`, …) |
| `markets[]` | every market of the event. `outcomes`, `outcomePrices` and `clobTokenIds` are **JSON arrays inside strings**. `outcomePrices` is Gamma's current price per outcome; once the market resolves it becomes `["1", "0"]` |

Futures and props (e.g. "Will FaZe win a Tier 1 event in 2026?") have no `gameId`, `startTime`,
`live`, `score`, `teams` or `sport`.

## `sessions`

Connection markers under `_system/<feed>/sessions`, so that a gap in the data can be told apart
from a quiet market. `s` is the session the marker is about.

```json
{"t": 1792222897598944812, "s": 9,  "m": {"event": "connected", "feed": "clob-7"}}
{"t": 1792261723969923315, "s": 1,  "m": {"event": "disconnected", "feed": "rtds", "reason": "shutdown"}}
```

| field | meaning |
|---|---|
| `feed` | `clob-<n>` (CLOB connection `n`), `rtds` or `sports` |
| `event` | `connected` or `disconnected` |
| `reason` | why it disconnected: `shutdown` (recorder stopped), `idle for 60s` (no message, so the recorder reconnected), a network error such as `WebSocket protocol error: Connection reset …`, or `server close: …` |

The recorder reconnects with a new session id, after a backoff of 0.5 s that grows to at most
30 s while connecting keeps failing. Data between a `disconnected` and the
next `connected` of that feed is missing. The `disconnected` marker of the last session of a
run is often missing, because the recorder stops before writing it. Treat the end of the run's
files as the end of that session.

## Joining streams

| key | appears as |
|---|---|
| market (condition id, `0x…`) | `clob_*.market`, `book_snapshots.market`, `trades.payload.conditionId`, `markets.condition_id`, `gamma_events.markets[].conditionId` |
| outcome token | `clob_*.asset_id`, `book_snapshots.asset_id`, `trades.payload.asset`, `markets.token_ids[i]`, `gamma_events.markets[].clobTokenIds[i]` |
| event | `markets.event_id` / `event_slug`, `trades.payload.eventSlug`, `gamma_events.id` / `slug` |
| game | `scores.gameId`, `markets.game_id`, `gamma_events.gameId` |
| trade | `clob_trades.transaction_hash` = `trades.payload.transactionHash` (+ `asset_id`, `size` to pick the fill) |
| connection | `(run, s)` of any record = `(run, s)` of its `sessions` markers |

`markets` is the usual lookup: with a token id, it gives the league, event, market type and
outcome name.
