//! Strategy 1, "C1": CS2 map longshots in quiet markets
//! (`docs/pmt/trading_algo/01_cs2_map_longshot.md`).
//!
//! - Universe: map-winner markets of the configured leagues that had **no trades before the
//!   scheduled series start** (checked from the Data API at the start).
//! - From the start, every `tick_secs`: if the market's last trade is at most `max_trade_age_secs`
//!   old and implies a price in `[min_price, max_price]` for either team, buy that team once as a
//!   taker (fill-and-kill, limit `slippage` above the price), `stake_usdc`. Held to resolution.

use std::collections::HashMap;

use anyhow::{Result, ensure};
use pm_core::{ConditionId, Fixed6};
use serde::Deserialize;
use serde_json::json;

use super::{Ctx, Event, Kind, S, Strategy, params, top_of_book};
use crate::market::{Market, MarketKind};
use crate::order::{OrderRequest, Tif, shares_for};
use crate::tape::History;

pub const KIND: Kind = Kind {
    name: "cs2_map_longshot",
    about: "buy a 5-30c team in-game in CS2 map markets that had no pre-game trades (taker, once per market)",
    build,
};

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Params {
    pub leagues: Vec<String>,
    pub min_price: f64,
    pub max_price: f64,
    /// The last trade must be at most this old.
    pub max_trade_age_secs: u64,
    pub tick_secs: u64,
    /// Limit price = signal price + this.
    pub slippage: f64,
    pub stake_usdc: f64,
    /// Stop watching a market this long after the series start.
    pub max_watch_hours: u64,
}

impl Default for Params {
    fn default() -> Self {
        Params {
            leagues: vec!["cs2".into()],
            min_price: 0.05,
            max_price: 0.30,
            max_trade_age_secs: 30,
            tick_secs: 60,
            slippage: 0.02,
            stake_usdc: 2.0,
            max_watch_hours: 6,
        }
    }
}

fn build(id: String, t: toml::Table) -> Result<Box<dyn Strategy>> {
    let p: Params = params(t)?;
    ensure!(
        0.0 < p.min_price && p.min_price <= p.max_price && p.max_price < 1.0,
        "need 0 < min_price <= max_price < 1"
    );
    ensure!(
        p.stake_usdc > 0.0 && p.tick_secs > 0,
        "stake_usdc and tick_secs must be positive"
    );
    Ok(Box::new(MapLongshot::new(id, p)))
}

#[derive(Clone, Debug, PartialEq)]
enum Phase {
    /// Before the series start.
    Waiting,
    /// History requested at the start: were there pre-game trades?
    Checking,
    /// Quiet market, in game: looking for a signal every tick.
    Watching,
    Done,
}

pub struct MapLongshot {
    id: String,
    p: Params,
    min: Fixed6,
    max: Fixed6,
    slippage: Fixed6,
    stake: Fixed6,
    markets: HashMap<ConditionId, Phase>,
}

const START: &str = "start";
const TICK: &str = "tick";

impl MapLongshot {
    pub fn new(id: String, p: Params) -> Self {
        MapLongshot {
            id,
            min: Fixed6::from_f64(p.min_price),
            max: Fixed6::from_f64(p.max_price),
            slippage: Fixed6::from_f64(p.slippage),
            stake: Fixed6::from_f64(p.stake_usdc),
            p,
            markets: HashMap::new(),
        }
    }

    fn on_market(&mut self, c: &ConditionId, cx: &mut Ctx<'_>) {
        let Some(m) = cx.market(c.as_str()) else { return };
        let (start, tradable) = (m.start_ns, m.tradable());
        let finished = m.winner.is_some() || m.info.closed;
        let phase = self.markets.entry(c.clone()).or_insert(Phase::Waiting);
        if *phase == Phase::Done {
            return;
        }
        // A market that is only paused (not accepting orders for a while) stays in play.
        if cx.has_decided(c) || finished {
            *phase = Phase::Done;
            return;
        }
        if !tradable {
            return;
        }
        // On first sight and whenever the start time moves; stale timers are ignored.
        if *phase == Phase::Waiting
            && let Some(start) = start
        {
            cx.at(start, Some(c), START);
        }
    }

    fn on_start(&mut self, c: &ConditionId, cx: &mut Ctx<'_>) {
        let Some(start) = cx.market(c.as_str()).and_then(|m| m.start_ns) else {
            return;
        };
        // Winding down: leave the market undecided for the next run.
        if self.markets.get(c) != Some(&Phase::Waiting) || cx.now_ns < start || !cx.can_enter() {
            return;
        }
        self.markets.insert(c.clone(), Phase::Checking);
        cx.load_history(c);
    }

    fn on_history(&mut self, c: &ConditionId, cx: &mut Ctx<'_>) {
        if self.markets.get(c) != Some(&Phase::Checking) || !cx.can_enter() {
            return;
        }
        let (Some(m), Some(tape)) = (cx.market(c.as_str()), cx.tape(c.as_str())) else {
            return;
        };
        let Some(start) = m.start_ns else { return };
        let label = m.label();
        let skip = match &tape.history {
            History::Failed(e) => {
                // Try again on the next tick.
                tracing::warn!("{}: trade history of {label} failed ({e}); retrying", self.id);
                self.markets.insert(c.clone(), Phase::Waiting);
                cx.at(cx.now_ns + self.p.tick_secs as i64 * S, Some(c), START);
                return;
            }
            History::Loaded { truncated: true, .. } => Some("too many trades to check".to_string()),
            History::Loaded { .. } => {
                let pre = tape.before(start);
                (!pre.is_empty()).then(|| format!("{} trades before the start", pre.len()))
            }
            History::NotLoaded | History::Loading => return,
        };
        match skip {
            Some(why) => {
                let detail = json!({"reason": why, "pregame_usdc": tape.notional_before(start)});
                cx.note(Some(c), "skip", format!("{label}: not a quiet market ({why})"), detail);
                self.markets.insert(c.clone(), Phase::Done);
            }
            None => {
                tracing::info!("{}: {label} had no pre-game trades; watching", self.id);
                self.markets.insert(c.clone(), Phase::Watching);
                cx.at(cx.now_ns, Some(c), TICK);
            }
        }
    }

    fn on_tick(&mut self, c: &ConditionId, cx: &mut Ctx<'_>) {
        if self.markets.get(c) != Some(&Phase::Watching) {
            return;
        }
        let Some(m) = cx.market(c.as_str()) else { return };
        let start = m.start_ns.unwrap_or(cx.now_ns);
        let finished = m.winner.is_some() || m.info.closed;
        if finished || cx.now_ns > start + self.p.max_watch_hours as i64 * 3600 * S {
            self.markets.insert(c.clone(), Phase::Done);
            return;
        }
        cx.at(cx.now_ns + self.p.tick_secs as i64 * S, Some(c), TICK);
        // Paused (not accepting orders for a while) or winding down: keep watching.
        if !m.tradable() || !cx.can_enter() {
            return;
        }
        let Some(last) = cx.tape(c.as_str()).and_then(|t| t.last()) else {
            return;
        };
        if cx.now_ns - last.t_ns > self.p.max_trade_age_secs as i64 * S {
            return;
        }
        let Some((token, p)) = m
            .info
            .token_ids
            .iter()
            .map(|t| (t, last.price_of(t)))
            .find(|(_, p)| self.min <= *p && *p <= self.max)
        else {
            return;
        };
        let limit = (p + self.slippage).floor_to(m.tick()).min(Fixed6::ONE - m.tick());
        let size = shares_for(self.stake, limit, m.min_size());
        let msg = format!(
            "{}: {} last traded at {p}, buying up to {size} at {limit}",
            m.label(),
            m.outcome(token)
        );
        let detail = json!({
            "outcome": m.outcome(token),
            "token": token,
            "price": p,
            "last_trade_age_ms": (cx.now_ns - last.t_ns) / 1_000_000,
            "limit": limit,
            "book": top_of_book(cx.book(token.as_str())),
        });
        let req = OrderRequest::buy(c, token, limit, size, Tif::Fak, format!("longshot at {p}"));
        cx.note(Some(c), "signal", msg, detail);
        cx.place(req);
        self.markets.insert(c.clone(), Phase::Done);
    }
}

impl Strategy for MapLongshot {
    fn id(&self) -> &str {
        &self.id
    }

    fn wants(&self, m: &Market) -> bool {
        matches!(m.kind, MarketKind::Map(_)) && self.p.leagues.contains(&m.info.league)
    }

    fn on_event(&mut self, ev: &Event, cx: &mut Ctx<'_>) {
        match ev {
            Event::Market(c) => self.on_market(c, cx),
            Event::Removed(c) => {
                self.markets.remove(c);
            }
            Event::History(c) => self.on_history(c, cx),
            Event::Timer(t) => match (&t.market, t.tag) {
                (Some(c), START) => self.on_start(c, cx),
                (Some(c), TICK) => self.on_tick(c, cx),
                _ => {}
            },
            Event::Book(_) | Event::Trade(..) | Event::Order(_) | Event::Stopping => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::testing::*;
    use super::*;
    use crate::tape::{Source, TapeTrade};
    use pm_core::Side;

    const START_NS: i64 = 1_000 * S;

    fn setup() -> (Harness, MapLongshot, ConditionId) {
        let mut h = Harness::default();
        h.md.upsert(market("m", MarketKind::Map(1), "cs2", START_NS));
        (h, MapLongshot::new("c1".into(), Params::default()), "m".into())
    }

    fn trade(t_ns: i64, token: &str, price: &str) -> TapeTrade {
        TapeTrade {
            t_ns,
            token: token.into(),
            price: px(price),
            size: px("50"),
            side: Side::Sell,
            tx: None,
            source: Source::Live,
        }
    }

    fn timer(tag: &'static str) -> Event {
        Event::Timer(super::super::Timer {
            market: Some("m".into()),
            tag,
        })
    }

    #[test]
    fn universe_is_map_markets_of_the_leagues() {
        let s = MapLongshot::new("c1".into(), Params::default());
        assert!(s.wants(&market("m", MarketKind::Map(2), "cs2", 0)));
        assert!(!s.wants(&market("m", MarketKind::Series, "cs2", 0)));
        assert!(!s.wants(&market("m", MarketKind::Map(1), "dota2", 0)));
    }

    #[test]
    fn buys_the_longshot_once_in_a_quiet_market() {
        let (mut h, mut s, c) = setup();
        assert_eq!(timers(&h.run(&mut s, Event::Market(c.clone()), 0)), [(START_NS, START)]);
        // A start timer that fires early (start time moved) does nothing.
        assert_eq!(loads(&h.run(&mut s, timer(START), START_NS - S)), 0);
        assert_eq!(loads(&h.run(&mut s, timer(START), START_NS)), 1);

        // Only in-game trades: a quiet market.
        let now = START_NS + 600 * S;
        h.md.tape_mut(&c)
            .set_history(vec![trade(START_NS + 10 * S, "m-a", "0.5")], false, now);
        let cmds = h.run(&mut s, Event::History(c.clone()), now);
        assert_eq!(timers(&cmds), [(now, TICK)]);

        // The last trade is too old: wait for the next tick.
        assert!(placed(&h.run(&mut s, timer(TICK), now)).is_empty());
        // Team A traded at 0.78: team B is at 0.22.
        h.md.tape_mut(&c).push_live(trade(now + 50 * S, "m-a", "0.78"));
        h.book("m-b", "0.20", "0.23");
        let cmds = h.run(&mut s, timer(TICK), now + 60 * S);
        let orders = placed(&cmds);
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].token.as_str(), "m-b");
        assert_eq!(orders[0].price.to_string(), "0.24");
        assert_eq!(orders[0].size.to_string(), "8.33"); // $2 / 0.24
        assert_eq!(orders[0].tif, Tif::Fak);
        assert_eq!(notes(&cmds), ["signal"]);

        // One bet per market.
        h.md.tape_mut(&c).push_live(trade(now + 110 * S, "m-a", "0.9"));
        assert!(h.run(&mut s, timer(TICK), now + 120 * S).is_empty());
    }

    #[test]
    fn pre_game_trades_exclude_the_market() {
        let (mut h, mut s, c) = setup();
        h.run(&mut s, Event::Market(c.clone()), 0);
        h.run(&mut s, timer(START), START_NS);
        h.md.tape_mut(&c)
            .set_history(vec![trade(START_NS - 3600 * S, "m-a", "0.5")], false, START_NS);
        let cmds = h.run(&mut s, Event::History(c.clone()), START_NS);
        assert_eq!(notes(&cmds), ["skip"]);
        assert!(timers(&cmds).is_empty());
        h.md.tape_mut(&c).push_live(trade(START_NS + 10 * S, "m-a", "0.9"));
        assert!(h.run(&mut s, timer(TICK), START_NS + 20 * S).is_empty());
    }

    #[test]
    fn no_new_bets_or_decisions_while_winding_down() {
        let (mut h, mut s, c) = setup();
        h.run(&mut s, Event::Market(c.clone()), 0);
        h.entries_closed = true;
        assert!(
            h.run(&mut s, timer(START), START_NS).is_empty(),
            "no history load, no decision"
        );
        // Entries open again (a new run would start fresh): it proceeds.
        h.entries_closed = false;
        assert_eq!(loads(&h.run(&mut s, timer(START), START_NS)), 1);
        let now = START_NS + 60 * S;
        h.md.tape_mut(&c).set_history(vec![], false, now);
        h.run(&mut s, Event::History(c.clone()), now);
        h.md.tape_mut(&c).push_live(trade(now, "m-a", "0.8"));
        h.entries_closed = true;
        let cmds = h.run(&mut s, timer(TICK), now + S);
        assert!(placed(&cmds).is_empty() && notes(&cmds).is_empty());
        assert_eq!(timers(&cmds).len(), 1, "keeps watching");
    }

    #[test]
    fn a_market_decided_in_an_earlier_run_is_left_alone() {
        let (mut h, mut s, c) = setup();
        h.decided.insert(c.clone());
        assert!(h.run(&mut s, Event::Market(c.clone()), 0).is_empty());
        assert!(h.run(&mut s, timer(START), START_NS).is_empty());
    }

    /// Runs to the Watching phase (no pre-game trades) and returns the time it started watching.
    fn watching(h: &mut Harness, s: &mut MapLongshot, c: &ConditionId) -> i64 {
        h.run(s, Event::Market(c.clone()), 0);
        h.run(s, timer(START), START_NS);
        h.md.tape_mut(c).set_history(vec![], false, START_NS);
        h.run(s, Event::History(c.clone()), START_NS);
        START_NS
    }

    #[test]
    fn a_paused_market_keeps_being_watched() {
        let (mut h, mut s, c) = setup();
        let now = watching(&mut h, &mut s, &c);
        let m = h.md.market("m").unwrap().clone();
        let mut paused = m.clone();
        paused.info.accepting_orders = false;
        h.md.upsert(paused);
        h.md.tape_mut(&c).push_live(trade(now + 50 * S, "m-a", "0.8"));
        let cmds = h.run(&mut s, timer(TICK), now + 60 * S);
        assert!(placed(&cmds).is_empty());
        assert_eq!(timers(&cmds).len(), 1, "still ticking");
        h.md.upsert(m);
        h.md.tape_mut(&c).push_live(trade(now + 110 * S, "m-a", "0.8"));
        assert_eq!(placed(&h.run(&mut s, timer(TICK), now + 120 * S)).len(), 1);
    }

    #[test]
    fn history_failures_retry_and_long_histories_are_not_quiet() {
        let (mut h, mut s, c) = setup();
        h.run(&mut s, Event::Market(c.clone()), 0);
        h.run(&mut s, timer(START), START_NS);
        h.md.tape_mut(&c).history = History::Failed("timeout".into());
        let cmds = h.run(&mut s, Event::History(c.clone()), START_NS);
        assert_eq!(timers(&cmds), [(START_NS + 60 * S, START)]);
        assert!(notes(&cmds).is_empty());
        assert_eq!(loads(&h.run(&mut s, timer(START), START_NS + 60 * S)), 1);
        h.md.tape_mut(&c).set_history(vec![], true, START_NS + 61 * S);
        assert_eq!(notes(&h.run(&mut s, Event::History(c), START_NS + 61 * S)), ["skip"]);
    }

    #[test]
    fn the_signal_band_and_the_trade_age() {
        let (mut h, mut s, c) = setup();
        let now = watching(&mut h, &mut s, &c);
        // 0.97 / 0.03: B is below the band. 0.5: neither. Both: nothing.
        for (i, price) in ["0.97", "0.5", "0.69"].iter().enumerate() {
            let t = now + (i as i64 + 1) * 60 * S;
            h.md.tape_mut(&c).push_live(trade(t - S, "m-a", price));
            assert!(placed(&h.run(&mut s, timer(TICK), t)).is_empty(), "{price}");
        }
        // 0.72 for A means 0.28 for B, inside: but the trade is 31 s old.
        let t = now + 300 * S;
        h.md.tape_mut(&c).push_live(trade(t - 31 * S, "m-a", "0.72"));
        assert!(placed(&h.run(&mut s, timer(TICK), t)).is_empty(), "stale");
        h.md.tape_mut(&c).push_live(trade(t + 30 * S, "m-a", "0.72"));
        let cmds = h.run(&mut s, timer(TICK), t + 60 * S);
        let o = placed(&cmds);
        assert_eq!(
            (o[0].token.as_str(), o[0].price.to_string()),
            ("m-b", "0.3".to_string())
        );
        assert_eq!(o[0].size.to_string(), "6.66");
    }
}
