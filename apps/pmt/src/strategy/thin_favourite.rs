//! Strategy 2: strong favourites in thin series markets
//! (`docs/pmt/trading_algo/02_thin_favourite.md`).
//!
//! - Universe: series-winner (match moneyline) markets of the configured leagues.
//! - At T = start − `lead_secs`: the favourite's displayed price must be in
//!   `[min_price, max_price]` and the USDC traded before T below `max_pregame_usdc`.
//! - Taker: buy the favourite with fill-and-kill orders, limit `slippage` above the displayed price,
//!   whenever the ask allows in [T, T + `taker_window_secs`], until `stake_usdc` is spent.
//! - Maker: rest a bid at the displayed price from T until the scheduled start.
//! - One decision per market; held to resolution.

use std::collections::HashMap;

use anyhow::{Result, ensure};
use pm_core::{ConditionId, Fixed6, Price, TokenId};
use serde::Deserialize;
use serde_json::json;

use super::{Ctx, Event, Kind, S, Strategy, params, top_of_book};
use crate::exec::ExecReport;
use crate::market::{Market, MarketKind};
use crate::order::{OrderId, OrderRequest, OrderStatus, Tif, shares_for};
use crate::tape::History;

pub const KIND: Kind = Kind {
    name: "thin_favourite",
    about: "buy an 80-90c favourite 5 min before thin series matches (< $3k pre-game volume)",
    build,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Execution {
    Taker,
    Maker,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Params {
    pub leagues: Vec<String>,
    /// T = scheduled start − this.
    pub lead_secs: u64,
    pub min_price: f64,
    pub max_price: f64,
    /// USDC traded before T must be below this.
    pub max_pregame_usdc: f64,
    pub execution: Execution,
    /// Taker: how long after T to keep trying.
    pub taker_window_secs: u64,
    /// Taker: limit = displayed price + this.
    pub slippage: f64,
    pub stake_usdc: f64,
}

impl Default for Params {
    fn default() -> Self {
        Params {
            leagues: vec!["cs2".into(), "dota2".into(), "lol".into()],
            lead_secs: 300,
            min_price: 0.80,
            max_price: 0.90,
            max_pregame_usdc: 3000.0,
            execution: Execution::Taker,
            taker_window_secs: 600,
            slippage: 0.01,
            stake_usdc: 5.0,
        }
    }
}

fn build(id: String, t: toml::Table) -> Result<Box<dyn Strategy>> {
    let p: Params = params(t)?;
    ensure!(
        0.0 < p.min_price && p.min_price <= p.max_price && p.max_price < 1.0,
        "need 0 < min_price <= max_price < 1"
    );
    ensure!(p.stake_usdc > 0.0, "stake_usdc must be positive");
    Ok(Box::new(ThinFavourite::new(id, p)))
}

#[derive(Clone, Debug, PartialEq)]
enum Phase {
    /// Before T.
    Waiting,
    /// History requested at T.
    Checking,
    /// Taker: waiting for an ask at or below `limit` until `until_ns`.
    Armed {
        token: TokenId,
        limit: Price,
        until_ns: i64,
        spent: Fixed6,
        /// No new attempt before this (after one that filled nothing).
        not_before_ns: i64,
    },
    /// Taker: an order is out.
    Taking {
        order: OrderId,
        token: TokenId,
        limit: Price,
        until_ns: i64,
        spent: Fixed6,
        /// The order filled at least partly.
        filled: bool,
    },
    Done,
}

pub struct ThinFavourite {
    id: String,
    p: Params,
    min: Fixed6,
    max: Fixed6,
    max_volume: Fixed6,
    slippage: Fixed6,
    stake: Fixed6,
    markets: HashMap<ConditionId, Phase>,
}

const DECIDE: &str = "decide";
const WINDOW_END: &str = "window_end";
/// After a taker attempt that filled nothing, wait this long before the next.
const RETRY_NS: i64 = 5 * S;

impl ThinFavourite {
    pub fn new(id: String, p: Params) -> Self {
        ThinFavourite {
            id,
            min: Fixed6::from_f64(p.min_price),
            max: Fixed6::from_f64(p.max_price),
            max_volume: Fixed6::from_f64(p.max_pregame_usdc),
            slippage: Fixed6::from_f64(p.slippage),
            stake: Fixed6::from_f64(p.stake_usdc),
            p,
            markets: HashMap::new(),
        }
    }

    fn decide_at(&self, m: &Market) -> Option<i64> {
        Some(m.start_ns? - self.p.lead_secs as i64 * S)
    }

    fn window_end(&self, decide_ns: i64) -> i64 {
        decide_ns + self.p.taker_window_secs as i64 * S
    }

    fn on_market(&mut self, c: &ConditionId, cx: &mut Ctx<'_>) {
        let Some(m) = cx.market(c.as_str()) else { return };
        let (decide, tradable) = (self.decide_at(m), m.tradable());
        let too_late = decide.is_some_and(|d| cx.now_ns > self.window_end(d));
        let phase = self.markets.entry(c.clone()).or_insert(Phase::Waiting);
        if *phase != Phase::Waiting {
            return;
        }
        let Some(decide) = decide else { return };
        // A market that is only paused (not accepting orders for a while) stays in play; the
        // decision checks again.
        let finished = m.winner.is_some() || m.info.closed;
        if cx.has_decided(c) || finished || too_late {
            *phase = Phase::Done;
            return;
        }
        if !tradable {
            return;
        }
        cx.at(decide, Some(c), DECIDE);
    }

    fn on_decide(&mut self, c: &ConditionId, cx: &mut Ctx<'_>) {
        let Some(decide) = cx.market(c.as_str()).and_then(|m| self.decide_at(m)) else {
            return;
        };
        // Winding down: leave the market undecided for the next run.
        if self.markets.get(c) != Some(&Phase::Waiting) || cx.now_ns < decide || !cx.can_enter() {
            return;
        }
        self.markets.insert(c.clone(), Phase::Checking);
        cx.load_history(c);
        cx.at(self.window_end(decide), Some(c), WINDOW_END);
    }

    fn on_history(&mut self, c: &ConditionId, cx: &mut Ctx<'_>) {
        if self.markets.get(c) != Some(&Phase::Checking) {
            return;
        }
        let (Some(m), Some(tape)) = (cx.market(c.as_str()), cx.tape(c.as_str())) else {
            return;
        };
        let Some(decide) = self.decide_at(m) else { return };
        let label = m.label();
        let until = self.window_end(decide);
        match &tape.history {
            History::Failed(e) if cx.now_ns + 15 * S < until => {
                tracing::warn!("{}: trade history of {label} failed ({e}); retrying", self.id);
                self.markets.insert(c.clone(), Phase::Waiting);
                cx.at(cx.now_ns + 15 * S, Some(c), DECIDE);
                return;
            }
            History::Failed(_) | History::Loaded { truncated: true, .. } => {
                let why = "trade history unavailable or too long".to_string();
                cx.note(Some(c), "skip", format!("{label}: {why}"), json!({"reason": why}));
                self.markets.insert(c.clone(), Phase::Done);
                return;
            }
            History::NotLoaded | History::Loading => return,
            History::Loaded { .. } => {}
        }
        self.evaluate(c, cx);
    }

    /// The decision, once the history is loaded and both books are synced (right after startup the
    /// books may lag behind the history, and a stale last trade is no price to decide on).
    fn evaluate(&mut self, c: &ConditionId, cx: &mut Ctx<'_>) {
        if self.markets.get(c) != Some(&Phase::Checking) {
            return;
        }
        let (Some(m), Some(tape)) = (cx.market(c.as_str()), cx.tape(c.as_str())) else {
            return;
        };
        let (Some(decide), History::Loaded { .. }) = (self.decide_at(m), &tape.history) else {
            return;
        };
        if m.info.token_ids.iter().any(|t| cx.book(t.as_str()).is_none()) {
            return;
        }
        if !cx.can_enter() {
            return;
        }
        let label = m.label();
        let until = self.window_end(decide);
        let volume = tape.notional_before(decide);
        let fav = m
            .info
            .token_ids
            .iter()
            .filter_map(|t| Some((t.clone(), cx.display_price(t)?)))
            .max_by_key(|(_, p)| *p);
        let Some((token, price)) = fav else {
            let why = "no price".to_string();
            cx.note(Some(c), "skip", format!("{label}: {why}"), json!({"reason": why}));
            self.markets.insert(c.clone(), Phase::Done);
            return;
        };
        let detail = json!({
            "outcome": m.outcome(&token),
            "token": token,
            "display_price": price,
            "pregame_usdc": volume,
            "book": top_of_book(cx.book(token.as_str())),
        });
        let skip = if price < self.min || price > self.max {
            Some(format!("favourite {} at {price}", m.outcome(&token)))
        } else if volume >= self.max_volume {
            Some(format!("${volume} traded before T"))
        } else if self.p.execution == Execution::Maker && m.start_ns.is_some_and(|t| cx.now_ns >= t) {
            Some("already started: too late for a bid".to_string())
        } else {
            None
        };
        if let Some(why) = skip {
            cx.note(Some(c), "skip", format!("{label}: {why}"), detail);
            self.markets.insert(c.clone(), Phase::Done);
            return;
        }
        let msg = format!(
            "{label}: {} at {price} with ${volume} traded; buying ({:?})",
            m.outcome(&token),
            self.p.execution
        );
        cx.note(Some(c), "signal", msg, detail);
        let tick = m.tick();
        let (start, min_size) = (m.start_ns.unwrap_or(decide), m.min_size());
        match self.p.execution {
            Execution::Maker => {
                let bid = price.floor_to(tick);
                let size = shares_for(self.stake, bid, min_size);
                let tif = Tif::Gtd { expires_ns: start };
                cx.place(OrderRequest::buy(
                    c,
                    &token,
                    bid,
                    size,
                    tif,
                    format!("favourite bid at {bid}"),
                ));
                self.markets.insert(c.clone(), Phase::Done);
            }
            Execution::Taker => {
                let limit = (price + self.slippage).floor_to(tick).min(Fixed6::ONE - tick);
                self.markets.insert(
                    c.clone(),
                    Phase::Armed {
                        token,
                        limit,
                        until_ns: until,
                        spent: Fixed6::ZERO,
                        not_before_ns: 0,
                    },
                );
                self.try_take(c, cx);
            }
        }
    }

    /// Taker: sends a fill-and-kill order if the ask is at or below the limit.
    fn try_take(&mut self, c: &ConditionId, cx: &mut Ctx<'_>) {
        let Some(Phase::Armed {
            token,
            limit,
            until_ns,
            spent,
            not_before_ns,
        }) = self.markets.get(c).cloned()
        else {
            return;
        };
        if cx.now_ns > until_ns || cx.now_ns < not_before_ns {
            return;
        }
        // The rest of a partly filled buy may complete; a market with nothing bought yet is new.
        if spent.is_zero() && !cx.can_enter() {
            return;
        }
        let Some(m) = cx.market(c.as_str()) else { return };
        let Some((ask, _)) = cx.book(token.as_str()).and_then(|b| b.best_ask()) else {
            return;
        };
        if ask > limit {
            return;
        }
        let size = shares_for(self.stake - spent, limit, m.min_size());
        let req = OrderRequest::buy(c, &token, limit, size, Tif::Fak, format!("favourite, ask {ask}"));
        let order = cx.place(req);
        self.markets.insert(
            c.clone(),
            Phase::Taking {
                order,
                token,
                limit,
                until_ns,
                spent,
                filled: false,
            },
        );
    }

    fn on_order(&mut self, r: &ExecReport, cx: &mut Ctx<'_>) {
        let Some((c, phase)) = self
            .markets
            .iter_mut()
            .find(|(_, p)| matches!(p, Phase::Taking { order, .. } if *order == r.id()))
        else {
            return;
        };
        let c = c.clone();
        let Phase::Taking {
            token,
            limit,
            until_ns,
            spent,
            filled,
            ..
        } = phase.clone()
        else {
            return;
        };
        match r {
            ExecReport::Fill { fill, .. } => {
                if let Phase::Taking { spent, filled, .. } = phase {
                    *spent += fill.price.mul_fixed(fill.size) + fill.fee;
                    *filled = true;
                }
            }
            // Risk limits said no: trying again on every book update would only repeat that.
            ExecReport::Done {
                status: OrderStatus::Rejected,
                ..
            } => *phase = Phase::Done,
            ExecReport::Done { .. } => {
                // Keep buying while the window is open and a minimum order is still affordable.
                let min_order = cx
                    .market(c.as_str())
                    .map_or(Fixed6::ZERO, |m| limit.mul_fixed(m.min_size()));
                *phase = if self.stake - spent >= min_order && cx.now_ns < until_ns {
                    Phase::Armed {
                        token,
                        limit,
                        until_ns,
                        spent,
                        not_before_ns: if filled { 0 } else { cx.now_ns + RETRY_NS },
                    }
                } else {
                    Phase::Done
                };
            }
        }
    }
}

impl Strategy for ThinFavourite {
    fn id(&self) -> &str {
        &self.id
    }

    fn wants(&self, m: &Market) -> bool {
        m.kind == MarketKind::Series && self.p.leagues.contains(&m.info.league)
    }

    fn on_event(&mut self, ev: &Event, cx: &mut Ctx<'_>) {
        match ev {
            Event::Market(c) => self.on_market(c, cx),
            Event::Removed(c) => {
                self.markets.remove(c);
            }
            Event::History(c) => self.on_history(c, cx),
            Event::Book(token) => {
                if let Some(c) = cx.md.market_of(token.as_str()).map(|m| m.info.condition_id.clone()) {
                    self.evaluate(&c, cx);
                    self.try_take(&c, cx);
                }
            }
            Event::Order(r) => self.on_order(r, cx),
            Event::Timer(t) => match (&t.market, t.tag) {
                (Some(c), DECIDE) => self.on_decide(c, cx),
                (Some(c), WINDOW_END) => {
                    let (what, why) = match self.markets.get(c) {
                        Some(Phase::Armed { .. }) => ("unfilled", "the ask never came down to the limit"),
                        Some(Phase::Checking) => ("skip", "no trade history or order book in the window"),
                        _ => return,
                    };
                    self.markets.insert(c.clone(), Phase::Done);
                    if cx.can_enter() {
                        let label = cx.market(c.as_str()).map(|m| m.label()).unwrap_or_default();
                        cx.note(Some(c), what, format!("{label}: {why}"), json!({"reason": why}));
                    }
                }
                _ => {}
            },
            Event::Trade(..) | Event::Stopping => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::testing::*;
    use super::*;
    use crate::order::Fill;
    use crate::tape::{Source, TapeTrade};
    use pm_core::Side;

    const START_NS: i64 = 10_000 * S;
    const T: i64 = START_NS - 300 * S;

    fn setup(execution: Execution) -> (Harness, ThinFavourite, ConditionId) {
        let mut h = Harness::default();
        h.md.upsert(market("m", MarketKind::Series, "dota2", START_NS));
        let p = Params {
            execution,
            ..Default::default()
        };
        (h, ThinFavourite::new("fav".into(), p), "m".into())
    }

    fn timer(tag: &'static str) -> Event {
        Event::Timer(super::super::Timer {
            market: Some("m".into()),
            tag,
        })
    }

    fn history(h: &mut Harness, usdc: &str) {
        let t = TapeTrade {
            t_ns: T - 3600 * S,
            token: "m-a".into(),
            price: px("1"),
            size: px(usdc),
            side: Side::Buy,
            tx: None,
            source: Source::History,
        };
        h.md.tape_mut(&"m".into()).set_history(vec![t], false, T);
    }

    /// Runs up to the decision with the favourite (`m-a`) quoted bid / ask.
    fn decide(
        h: &mut Harness,
        s: &mut ThinFavourite,
        bid: &str,
        ask: &str,
        volume: &str,
    ) -> Vec<super::super::Command> {
        let c: ConditionId = "m".into();
        assert_eq!(timers(&h.run(s, Event::Market(c.clone()), 0)), [(T, DECIDE)]);
        assert_eq!(loads(&h.run(s, timer(DECIDE), T)), 1);
        h.book("m-a", bid, ask);
        let (b, a) = (px(bid).complement().to_string(), px(ask).complement().to_string());
        h.book("m-b", &a, &b);
        history(h, volume);
        h.run(s, Event::History(c), T + S)
    }

    #[test]
    fn taker_buys_the_favourite_when_the_ask_allows() {
        let (mut h, mut s, _) = setup(Execution::Taker);
        // Displayed 0.85, ask 0.87 > limit 0.86: armed, no order yet.
        let cmds = decide(&mut h, &mut s, "0.83", "0.87", "500");
        assert_eq!(notes(&cmds), ["signal"]);
        assert!(placed(&cmds).is_empty());
        // The ask comes down.
        h.book("m-a", "0.84", "0.86");
        let cmds = h.run(&mut s, Event::Book("m-a".into()), T + 60 * S);
        let o = placed(&cmds);
        assert_eq!(
            (o[0].price.to_string(), o[0].size.to_string()),
            ("0.86".into(), "5.81".into())
        );
        assert_eq!(o[0].tif, Tif::Fak);

        // Partly filled: it tries again for the rest on the next book update.
        let id = OrderId(1);
        let fill = Fill {
            price: px("0.86"),
            size: px("0.5"),
            fee: Fixed6::ZERO,
            maker: false,
            t_ns: 0,
        };
        h.run(&mut s, Event::Order(ExecReport::Fill { id, fill }), T + 60 * S);
        let done = ExecReport::done(id, OrderStatus::Cancelled, "rest killed");
        h.run(&mut s, Event::Order(done), T + 60 * S);
        let again = h.run(&mut s, Event::Book("m-a".into()), T + 61 * S);
        assert_eq!(placed(&again)[0].size.to_string(), "5.31", "for the remaining $4.57");
        // Once the rest cannot buy the 5-share minimum ($4.30), it stops.
        h.run(
            &mut s,
            Event::Order(ExecReport::Fill { id: OrderId(2), fill }),
            T + 61 * S,
        );
        let done = ExecReport::done(OrderId(2), OrderStatus::Cancelled, "rest killed");
        h.run(&mut s, Event::Order(done), T + 61 * S);
        assert!(h.run(&mut s, Event::Book("m-a".into()), T + 62 * S).is_empty());
    }

    #[test]
    fn waits_for_both_books_before_deciding() {
        let (mut h, mut s, c) = setup(Execution::Taker);
        h.run(&mut s, Event::Market(c.clone()), 0);
        let cmds = h.run(&mut s, timer(DECIDE), T);
        assert_eq!(timers(&cmds), [(T + 600 * S, WINDOW_END)]);
        history(&mut h, "100");
        // A stale last trade says B is the favourite, but there is no book yet: no decision.
        let stale = TapeTrade {
            t_ns: T - S,
            token: "m-b".into(),
            price: px("0.6"),
            size: px("1"),
            side: Side::Buy,
            tx: None,
            source: Source::Live,
        };
        h.md.tape_mut(&c).push_live(stale);
        assert!(h.run(&mut s, Event::History(c.clone()), T + S).is_empty());
        h.book("m-a", "0.84", "0.86");
        assert!(
            h.run(&mut s, Event::Book("m-a".into()), T + 2 * S).is_empty(),
            "one book only"
        );
        h.book("m-b", "0.14", "0.16");
        let cmds = h.run(&mut s, Event::Book("m-b".into()), T + 3 * S);
        assert_eq!(notes(&cmds), ["signal"]);
        assert_eq!(placed(&cmds)[0].token.as_str(), "m-a");
    }

    #[test]
    fn gives_up_when_the_window_closes_without_books() {
        let (mut h, mut s, c) = setup(Execution::Taker);
        h.run(&mut s, Event::Market(c.clone()), 0);
        h.run(&mut s, timer(DECIDE), T);
        history(&mut h, "100");
        h.run(&mut s, Event::History(c), T + S);
        assert_eq!(notes(&h.run(&mut s, timer(WINDOW_END), T + 600 * S)), ["skip"]);
    }

    #[test]
    fn filters_on_price_and_pregame_volume() {
        let (mut h, mut s, _) = setup(Execution::Taker);
        assert_eq!(notes(&decide(&mut h, &mut s, "0.93", "0.95", "500")), ["skip"]);
        let (mut h, mut s, _) = setup(Execution::Taker);
        assert_eq!(notes(&decide(&mut h, &mut s, "0.84", "0.86", "3000")), ["skip"]);
        assert!(h.run(&mut s, Event::Book("m-a".into()), T + 60 * S).is_empty());
    }

    #[test]
    fn maker_rests_a_bid_at_the_displayed_price_until_the_start() {
        let (mut h, mut s, _) = setup(Execution::Maker);
        let cmds = decide(&mut h, &mut s, "0.83", "0.88", "100");
        let o = placed(&cmds);
        assert_eq!(o[0].price.to_string(), "0.85");
        assert_eq!(o[0].tif, Tif::Gtd { expires_ns: START_NS });
    }

    #[test]
    fn winding_down_stops_new_buys_but_not_a_partly_filled_one() {
        let (mut h, mut s, _) = setup(Execution::Taker);
        decide(&mut h, &mut s, "0.83", "0.87", "500");
        h.entries_closed = true;
        h.book("m-a", "0.84", "0.86");
        assert!(
            h.run(&mut s, Event::Book("m-a".into()), T + 60 * S).is_empty(),
            "nothing bought yet"
        );

        let (mut h, mut s, _) = setup(Execution::Taker);
        decide(&mut h, &mut s, "0.84", "0.86", "500");
        let fill = Fill {
            price: px("0.86"),
            size: px("0.5"),
            fee: Fixed6::ZERO,
            maker: false,
            t_ns: 0,
        };
        h.run(
            &mut s,
            Event::Order(ExecReport::Fill { id: OrderId(1), fill }),
            T + 2 * S,
        );
        let done = ExecReport::done(OrderId(1), OrderStatus::Cancelled, "rest killed");
        h.run(&mut s, Event::Order(done), T + 2 * S);
        h.entries_closed = true;
        assert_eq!(placed(&h.run(&mut s, Event::Book("m-a".into()), T + 3 * S)).len(), 1);
    }

    #[test]
    fn too_late_to_decide() {
        let (mut h, mut s, c) = setup(Execution::Taker);
        assert!(h.run(&mut s, Event::Market(c), T + 601 * S).is_empty());
        assert!(h.run(&mut s, timer(DECIDE), T + 602 * S).is_empty());
    }

    #[test]
    fn a_rejected_order_ends_the_market_instead_of_retrying_on_every_book() {
        let (mut h, mut s, _) = setup(Execution::Taker);
        let cmds = decide(&mut h, &mut s, "0.84", "0.86", "500");
        assert_eq!(placed(&cmds).len(), 1);
        let done = ExecReport::done(OrderId(1), OrderStatus::Rejected, "max_daily_usdc");
        h.run(&mut s, Event::Order(done), T + 2 * S);
        for i in 3..10 {
            assert!(h.run(&mut s, Event::Book("m-a".into()), T + i * S).is_empty());
        }
    }

    #[test]
    fn an_attempt_that_filled_nothing_waits_before_the_next() {
        let (mut h, mut s, _) = setup(Execution::Taker);
        decide(&mut h, &mut s, "0.84", "0.86", "500");
        let killed = ExecReport::done(OrderId(1), OrderStatus::Cancelled, "no liquidity");
        h.run(&mut s, Event::Order(killed), T + S);
        assert!(
            h.run(&mut s, Event::Book("m-a".into()), T + 2 * S).is_empty(),
            "throttled"
        );
        assert_eq!(placed(&h.run(&mut s, Event::Book("m-a".into()), T + 7 * S)).len(), 1);
    }

    #[test]
    fn no_maker_bid_once_the_match_has_started() {
        let (mut h, mut s, c) = setup(Execution::Maker);
        h.run(&mut s, Event::Market(c.clone()), 0);
        // The bot came up late: deciding 1 min after the start (still inside the window).
        let late = START_NS + 60 * S;
        h.run(&mut s, timer(DECIDE), late);
        h.book("m-a", "0.83", "0.88");
        h.book("m-b", "0.12", "0.17");
        history(&mut h, "100");
        let cmds = h.run(&mut s, Event::History(c), late);
        assert!(placed(&cmds).is_empty());
        assert_eq!(notes(&cmds), ["skip"]);
    }

    #[test]
    fn a_paused_market_stays_in_play() {
        let (mut h, mut s, c) = setup(Execution::Taker);
        let paused = Market {
            info: pm_core::MarketInfo {
                accepting_orders: false,
                ..h.md.market("m").unwrap().info.clone()
            },
            ..h.md.market("m").unwrap().clone()
        };
        let open = h.md.market("m").unwrap().clone();
        h.md.upsert(paused);
        assert!(h.run(&mut s, Event::Market(c.clone()), 0).is_empty());
        h.md.upsert(open);
        assert_eq!(timers(&h.run(&mut s, Event::Market(c.clone()), 1)), [(T, DECIDE)]);
        // Resolved: done for good.
        h.md.set_winner("m", 0);
        h.run(&mut s, Event::Market(c), 2);
        assert!(h.run(&mut s, timer(DECIDE), T).is_empty());
    }
}
