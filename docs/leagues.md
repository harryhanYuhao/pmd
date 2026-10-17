# Leagues worth recording

A snapshot taken on **2026-10-17** from Gamma (`/sports`, `/events`). The `tag_id` column is the
value for `[[leagues]] tag_id` in `config/recorder.toml`.

- **30-day volume:** the summed Gamma `volume` (in USDC) of events that closed between
  2026-09-17 and 2026-10-17. Gamma's lifetime figure includes pre-game trading and can include
  long-dated futures, so treat it as an order-of-magnitude ranking, not an exact flow number.
- **Per event:** 30-day volume ÷ closed events, which shows how deep a typical match is.
- **Open markets:** markets currently open, which is roughly what the recorder would subscribe
  to. It is the main driver of disk use and connection count. The current 3 esports leagues
  (~1,800 markets) use about 4–5 GB/day.
- Volumes are seasonal. NBA was in preseason, and NCAAB, IPL and the tennis majors were out of
  season in this window. Re-run the scan below before deciding.

## Esports (category `esports`)

| key | tag_id | 30-day vol | closed events | per event | open events / markets | note |
|---|---|---|---|---|---|---|
| cs2 | 100780 | $278M | 1,469 | $189k | 93 / 616 | **recording**. Most events; many thin tier-2/3 matches |
| lol | 65 | $256M | 314 | $816k | 50 / 957 | **recording**. Deepest books per match |
| dota2 | 102366 | $209M | 302 | $691k | 18 / 343 | **recording** |
| val | 101672 | $43M | 74 | $580k | 53 / 791 | **best next addition**: deep per match, cheap to record |
| r6siege | 102755 | $0.70M | 187 | $3.7k | 5 / 26 | thin; only longshot-type ideas |
| codmw | 100230 | $0.26M | 3 | — | 0 / 0 | event-driven (majors only) |
| hok | 102757 | $0.11M | 149 | $0.8k | 9 / 77 | very thin |
| mlbb | 102750 | $0.10M | 175 | $0.6k | 18 / 99 | very thin (its scores already land in `other/mlbb`) |
| ow | 102753 | $0.06M | 68 | $0.9k | 13 / 97 | very thin |
| rl | 102756 | $0.05M | 26 | $2k | 0 / 0 | very thin |
| sc2 | 103064 | $0.03M | 24 | $1k | 0 / 0 | very thin |
| lol-wild-rift, pubg, fifa, sc1 | 102752, 105560, 102751, 103065 | ~0 | — | — | — | inactive |

## Sports (category `sports`)

Sorted by 30-day volume. ⚠ means very many open markets, mostly props and season futures. For
these, use `include_types` (e.g. `["moneyline"]` plus the main spread/total types) or the
recorder will subscribe to tens of thousands of tokens.

| key | tag_id | sport | 30-day vol | closed events | per event | open events / markets | note |
|---|---|---|---|---|---|---|---|
| nfl | 450 | US football | $411M | 328 | $1.25M | 563 / 21,992 ⚠ | biggest single-game liquidity |
| mlb | 100381 | baseball | $230M | 2,100 | $110k | 46 / 1,843 | postseason in this window |
| unl | 100816 | soccer (UEFA Nations League) | $211M | 932 | $226k | 19 / 248 | international windows only |
| cfb | 100351 | US college football | $162M | 665 | $244k | 469 / 27,302 ⚠ | |
| nba | 745 | basketball | $84M | 102 | $823k | 85 / 2,845 | preseason; regular season much bigger |
| epl | 306 | soccer (Premier League) | $69M | 219 | $317k | 207 / 3,599 | |
| lal | 780 | soccer (La Liga) | $58M | 242 | $238k | 196 / 3,450 | |
| nhl | 899 | ice hockey | $35M | 182 | $191k | 221 / 2,701 | |
| uel | 101787 | soccer (Europa League) | $28M | 202 | $138k | 140 / 3,117 | |
| bun | 1494 | soccer (Bundesliga) | $27M | 146 | $187k | 141 / 2,805 | |
| ufc | 279 | MMA | $27M | 87 | $309k | 55 / 917 | event weekends |
| atp | 101232 | tennis | $27M | 3 | — | 7 / 166 | volume concentrated in a few tournament events; check how matches are tagged |
| mls | 100100 | soccer | $25M | 694 | $37k | 333 / 6,453 | |
| wnba | 100254 | basketball | $25M | 65 | $385k | 9 / 105 | season ending |
| bra | 102648 | soccer (Brasileirão) | $15M | 229 | $67k | 152 / 2,690 | |
| fl1 | 102070 | soccer (Ligue 1) | $13M | 205 | $63k | 175 / 3,145 | |
| mex | 102448 | soccer (Liga MX) | $13M | 270 | $48k | 151 / 2,844 | |
| arg | 102561 | soccer (Argentina) | $12M | 295 | $41k | 114 / 2,623 | |
| crint | 102803 | cricket (internationals) | $12M | 706 | $17k | 84 / 220 | |
| es2 | 102866 | soccer (Segunda) | $11M | 330 | $32k | 116 / 2,124 | |
| elc | 102643 | soccer (Championship) | $7.7M | 273 | $28k | 138 / 2,478 | |
| uwcl | 103885 | soccer (Women's CL) | $7.3M | 126 | $58k | 45 / 828 | |
| por | 101772 | soccer (Portugal) | $6.1M | 124 | $49k | 61 / 1,125 | |
| euroleague | 102669 | basketball | $5.1M | 60 | $85k | 8 / 40 | cheap to record |
| f1 | 435 | motorsport | $4.2M | 52 | $82k | 54 / 894 | |

About 460 leagues are listed in total; most are below $1M per 30 days.

## Suggestions

1. **Add `val` (Valorant).** It is the only other esports league with real depth ($580k per
   match). It has the same structure as cs2 (maps, BoN), so the esports research (crowd fade,
   thin map markets, in-game latency) transfers directly. It would add ~800 markets.
2. **Thin esports (r6siege, mlbb, hok, ow)** are only interesting for thin-market longshot
   ideas, and per-match volume of $1–4k caps capacity. They are cheap to add if wanted.
3. **Sports as a control group.** The recorder handles sports, but NFL/CFB/soccer would multiply
   the data volume. If you want a comparison (e.g. whether crowd fading also works in
   traditional sports), start with one league and a type filter:
   ```toml
   [[leagues]]
   category = "sports"
   key = "nba"
   tag_id = 745
   include_types = ["moneyline", "spreads", "totals"]
   include_untyped = false
   ```
   Check the exact `sportsMarketType` names first (the `market_type` column in
   `live_data.markets()`).

## Refreshing this table

```bash
curl -s https://gamma-api.polymarket.com/sports | jq -r '.[] | "\(.sport)\t\(.primaryTagId)\t\(.tags)"'
# tags: 64 = esports, 100350 = soccer, 517 = cricket, 864 = tennis, 678 = baseball,
#       28 = basketball, 100088 = hockey
curl -s 'https://gamma-api.polymarket.com/events?tag_id=101672&closed=true&end_date_min=2026-09-17T00:00:00Z&limit=100' \
  | jq '[.[].volume | tonumber] | add'
```
