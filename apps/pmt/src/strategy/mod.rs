//! Strategies: the pluggable decision logic.
//!
//! A strategy is a synchronous state machine. The engine calls [`Strategy::on_event`] with one
//! [`Event`] at a time and a [`Ctx`] that gives read access to markets, books, tapes, orders and
//! positions, and collects the strategy's commands (orders, cancels, timers, history loads, journal
//! notes). Strategies never read the wall clock (`cx.now_ns`) or do I/O, so the same code can be
//! driven by a replay of recorded data.
//!
//! Adding an algorithm: write a module with a `Params` struct (serde, `deny_unknown_fields`), a
//! type implementing [`Strategy`], and a `pub const KIND: Kind`; then list it in [`KINDS`].

pub mod cs2_map_longshot;
pub mod thin_favourite;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use pm_core::book::OrderBook;
use pm_core::{ConditionId, Price, TokenId};
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::config::StrategyEntry;
use crate::exec::ExecReport;
use crate::market::Market;
use crate::order::{Order, OrderId, OrderRequest};
use crate::portfolio::{Portfolio, Position};
use crate::state::MarketState;
use crate::tape::{Tape, TapeTrade};

/// Something the strategy may react to. Market events only reach strategies whose
/// [`Strategy::wants`] accepted the market.
#[derive(Clone, Debug)]
pub enum Event {
    /// A market entered the strategy's universe, or its metadata changed (start time, status,
    /// resolution).
    Market(ConditionId),
    /// The market left the listings and nothing is held in it; forget its state.
    Removed(ConditionId),
    /// A token's order book changed.
    Book(TokenId),
    /// A trade printed in a market.
    Trade(ConditionId, TapeTrade),
    /// History requested with [`Ctx::load_history`] arrived (or failed: see `Tape::history`).
    History(ConditionId),
    /// A timer set with [`Ctx::at`] fired.
    Timer(Timer),
    /// One of this strategy's orders filled or finished.
    Order(ExecReport),
    /// The run is winding down (scheduled stop or a signal): no new markets can be entered. Finish
    /// or exit what is in progress; the run ends once no strategy is [`Strategy::busy`] and no
    /// order is working.
    Stopping,
}

/// One second in ns, for timer arithmetic.
pub const S: i64 = 1_000_000_000;

/// A wake-up the strategy asked for. `tag` tells its timers apart.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Timer {
    pub market: Option<ConditionId>,
    pub tag: &'static str,
}

pub trait Strategy: Send {
    /// Instance id (logs, journal partition).
    fn id(&self) -> &str;
    /// Whether a market is in this strategy's universe. Asked once, when the market first appears;
    /// wanted markets get order books and live trades.
    fn wants(&self, m: &Market) -> bool;
    fn on_event(&mut self, ev: &Event, cx: &mut Ctx<'_>);
    /// True while the strategy is in the middle of something the process must stay up for (say an
    /// in-game position it still has to sell). A stopping run waits for this to turn false.
    /// Positions held to resolution need no process and do not count.
    fn busy(&self) -> bool {
        false
    }
}

/// What a strategy asked for during one callback.
#[derive(Debug)]
pub enum Command {
    Place(OrderId, OrderRequest),
    Cancel(OrderId),
    At(i64, Timer),
    LoadHistory(ConditionId),
    Note {
        market: Option<ConditionId>,
        what: &'static str,
        msg: String,
        detail: Value,
    },
}

/// A strategy's view of the world during one callback.
pub struct Ctx<'a> {
    /// Current time, Unix ns.
    pub now_ns: i64,
    pub md: &'a MarketState,
    pub(crate) strategy: &'a Arc<str>,
    pub(crate) portfolio: &'a Portfolio,
    pub(crate) orders: &'a HashMap<OrderId, Order>,
    /// Markets this strategy has decided on (journaled a note or sent an order), in this run or
    /// (from the journal) earlier ones.
    pub(crate) decided: &'a HashSet<ConditionId>,
    pub(crate) next_order: &'a mut u64,
    pub(crate) out: &'a mut Vec<Command>,
    /// New markets may be entered (not winding down, not within `entry_cutoff` of the stop).
    pub(crate) entries_open: bool,
    pub(crate) stop_at_ns: Option<i64>,
}

/// Read accessors return references that outlive the borrow of `self`, so a strategy can keep a
/// market or book in hand while it places orders.
impl<'a> Ctx<'a> {
    pub fn market(&self, c: &str) -> Option<&'a Market> {
        self.md.market(c)
    }

    /// A synced order book.
    pub fn book(&self, token: &str) -> Option<&'a OrderBook> {
        self.md.book(token)
    }

    pub fn tape(&self, c: &str) -> Option<&'a Tape> {
        self.md.tape(c)
    }

    pub fn display_price(&self, token: &TokenId) -> Option<Price> {
        self.md.display_price(token)
    }

    pub fn order(&self, id: OrderId) -> Option<&'a Order> {
        self.orders.get(&id)
    }

    /// Whether the strategy may enter a market it is not in yet. When false the engine rejects
    /// such orders; strategies should not even decide (or journal decisions about) new markets,
    /// so that the next run can decide them.
    pub fn can_enter(&self) -> bool {
        self.entries_open
    }

    /// Scheduled end of the run, Unix ns (`None` = until stopped).
    pub fn stop_at(&self) -> Option<i64> {
        self.stop_at_ns
    }

    /// This strategy already decided on this market: it journaled a note (signal, skip, …) or sent
    /// an order (rejected orders do not count), in this run or an earlier one. One-shot strategies
    /// check this so that a restart never decides a market twice.
    pub fn has_decided(&self, c: &ConditionId) -> bool {
        self.decided.contains(c)
    }

    pub fn position(&self, market: &ConditionId, token: &TokenId) -> Option<&'a Position> {
        self.portfolio.position(self.strategy, market, token)
    }

    /// Queues an order; it is risk-checked and sent after the callback returns. Reports come back
    /// as [`Event::Order`].
    pub fn place(&mut self, req: OrderRequest) -> OrderId {
        *self.next_order += 1;
        let id = OrderId(*self.next_order);
        self.out.push(Command::Place(id, req));
        id
    }

    pub fn cancel(&mut self, id: OrderId) {
        self.out.push(Command::Cancel(id));
    }

    /// Fires [`Event::Timer`] at `t_ns` (at once if that is in the past).
    pub fn at(&mut self, t_ns: i64, market: Option<&ConditionId>, tag: &'static str) {
        let timer = Timer {
            market: market.cloned(),
            tag,
        };
        self.out.push(Command::At(t_ns, timer));
    }

    /// Loads the market's trades from the Data API into its tape, then sends [`Event::History`].
    pub fn load_history(&mut self, c: &ConditionId) {
        self.out.push(Command::LoadHistory(c.clone()));
    }

    /// Journals a decision (`what`: `signal`, `skip`, …) and logs `msg`. A note about a market
    /// marks it decided (see [`Ctx::has_decided`]).
    pub fn note(&mut self, market: Option<&ConditionId>, what: &'static str, msg: String, detail: Value) {
        self.out.push(Command::Note {
            market: market.cloned(),
            what,
            msg,
            detail,
        });
    }
}

/// An available algorithm.
pub struct Kind {
    pub name: &'static str,
    pub about: &'static str,
    pub build: fn(id: String, params: toml::Table) -> Result<Box<dyn Strategy>>,
}

/// Every algorithm `[[strategy]] kind = "…"` can name.
pub const KINDS: &[Kind] = &[cs2_map_longshot::KIND, thin_favourite::KIND];

pub fn build(entry: &StrategyEntry) -> Result<Box<dyn Strategy>> {
    let kind = KINDS.iter().find(|k| k.name == entry.kind).with_context(|| {
        let names: Vec<&str> = KINDS.iter().map(|k| k.name).collect();
        format!(
            "unknown strategy kind {:?}; choose from {}",
            entry.kind,
            names.join(", ")
        )
    })?;
    (kind.build)(entry.id().to_string(), entry.params.clone()).with_context(|| format!("strategy {}", entry.id()))
}

/// Parses a strategy's parameter table.
pub fn params<T: DeserializeOwned>(t: toml::Table) -> Result<T> {
    Ok(toml::Value::Table(t).try_into()?)
}

/// `[best bid, best ask]` with sizes, for journal records.
pub fn top_of_book(book: Option<&OrderBook>) -> Value {
    let Some(b) = book else { return Value::Null };
    serde_json::json!({
        "bid": b.best_bid().map(|(p, s)| [p, s]),
        "ask": b.best_ask().map(|(p, s)| [p, s]),
    })
}

#[cfg(test)]
pub(crate) mod testing {
    //! A harness that runs one strategy against hand-made market state, collecting its commands.

    use super::*;
    use crate::market::MarketKind;
    use pm_core::{Fixed6, MarketInfo};

    pub fn px(s: &str) -> Fixed6 {
        s.parse().unwrap()
    }

    pub fn market(c: &str, kind: MarketKind, league: &str, start_ns: i64) -> Market {
        Market {
            info: MarketInfo {
                condition_id: c.into(),
                market_id: "1".into(),
                slug: "s".into(),
                question: "A vs B".into(),
                league: league.into(),
                event_id: "e".into(),
                event_slug: "ev".into(),
                market_type: None,
                outcomes: vec!["A".into(), "B".into()],
                token_ids: vec![format!("{c}-a").into(), format!("{c}-b").into()],
                game_start_time: None,
                game_id: None,
                tick_size: Some(px("0.01")),
                min_order_size: Some(5.0),
                neg_risk: false,
                active: true,
                closed: false,
                accepting_orders: true,
            },
            kind,
            event_title: "A vs B".into(),
            start_ns: Some(start_ns),
            fees: None,
            winner: None,
        }
    }

    #[derive(Default)]
    pub struct Harness {
        pub md: MarketState,
        pub portfolio: Portfolio,
        pub orders: HashMap<OrderId, Order>,
        pub decided: HashSet<ConditionId>,
        pub next_order: u64,
        pub id: Arc<str>,
        /// Set to simulate a run that is winding down.
        pub entries_closed: bool,
    }

    impl Harness {
        pub fn run(&mut self, s: &mut dyn Strategy, ev: Event, now_ns: i64) -> Vec<Command> {
            let mut out = Vec::new();
            let mut cx = Ctx {
                now_ns,
                md: &self.md,
                strategy: &self.id,
                portfolio: &self.portfolio,
                orders: &self.orders,
                decided: &self.decided,
                next_order: &mut self.next_order,
                out: &mut out,
                entries_open: !self.entries_closed,
                stop_at_ns: None,
            };
            s.on_event(&ev, &mut cx);
            out
        }

        pub fn book(&mut self, token: &str, bid: &str, ask: &str) {
            let snap = format!(
                r#"{{"event_type":"book","market":"m","asset_id":"{token}","timestamp":"1","bids":[{{"price":"{bid}","size":"100"}}],"asks":[{{"price":"{ask}","size":"100"}}]}}"#
            );
            self.md.books.apply(&serde_json::from_str(&snap).unwrap());
        }
    }

    pub fn placed(cmds: &[Command]) -> Vec<&OrderRequest> {
        cmds.iter()
            .filter_map(|c| match c {
                Command::Place(_, r) => Some(r),
                _ => None,
            })
            .collect()
    }

    pub fn timers(cmds: &[Command]) -> Vec<(i64, &'static str)> {
        cmds.iter()
            .filter_map(|c| match c {
                Command::At(t, timer) => Some((*t, timer.tag)),
                _ => None,
            })
            .collect()
    }

    pub fn notes(cmds: &[Command]) -> Vec<&'static str> {
        cmds.iter()
            .filter_map(|c| match c {
                Command::Note { what, .. } => Some(*what),
                _ => None,
            })
            .collect()
    }

    pub fn loads(cmds: &[Command]) -> usize {
        cmds.iter().filter(|c| matches!(c, Command::LoadHistory(_))).count()
    }
}
