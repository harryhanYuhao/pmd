//! The engine: one task that owns all trading state and runs every strategy.
//!
//! ```text
//! universe (Gamma poll) ──┐
//! CLOB market channel ────┼─► Engine ──► strategies ──► commands ──► risk ──► venue
//! history (Data API) ─────┤      ▲                                            │
//! timers, clock ──────────┘      └──────────── order reports ◄────────────────┘
//!                                 journal ◄── decisions, orders, fills, settlements
//! ```
//!
//! Inputs are handled one at a time; each turns into events for the strategies that want the
//! market, and the commands they return are carried out before the next input (order reports go
//! back into the same queue). No locks, and the order of events is deterministic.
//!
//! Ending a run: at the scheduled stop (or on [`Control::WindDown`]) the engine winds down. It
//! rejects orders that would enter a market a strategy is not in yet (new trades), keeps running
//! everything else, and returns once no order is working and no strategy is busy, or after the
//! drain timeout. Strategies with an `entry_cutoff` stop entering that long before the scheduled
//! stop. [`Control::Stop`] ends the run at once; working orders are cancelled either way.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pm_core::wire::clob_ws::MarketMsg;
use pm_core::wire::split_frame;
use pm_core::{ConditionId, Fixed6, TokenId, now_ns};
use pm_ws::WsEvent;
use pm_ws::clob::ClobManager;
use serde_json::json;
use tokio::sync::mpsc;

use crate::config::HumanDuration;
use crate::exec::{ExecReport, Venue};
use crate::history;
use crate::journal::{FillRecord, Journal, Restored};
use crate::order::{Order, OrderId, OrderRequest, OrderStatus};
use crate::portfolio::{FillAt, Portfolio};
use crate::risk::Risk;
use crate::state::{Change, MarketState};
use crate::strategy::{Command, Ctx, Event, Strategy, Timer};
use crate::tape::{History, TapeTrade};
use crate::universe::{Held, Update};

/// Resubscribe a token with a broken book at most this often.
const RESYNC_EVERY: Duration = Duration::from_secs(60);

struct Slot {
    strategy: Box<dyn Strategy>,
    id: Arc<str>,
    /// No new markets this long before the scheduled stop.
    entry_cutoff: Duration,
    /// Markets in this strategy's universe.
    wanted: HashSet<ConditionId>,
    /// Markets this strategy decided on (see `Ctx::has_decided`).
    decided: HashSet<ConditionId>,
}

pub struct Inputs {
    pub universe: mpsc::Receiver<Update>,
    pub ws: mpsc::Receiver<WsEvent>,
    pub history: mpsc::Receiver<history::Loaded>,
}

/// When the run ends.
#[derive(Clone, Copy, Debug)]
pub struct Schedule {
    /// Scheduled stop, Unix ns (`None` = until stopped).
    pub stop_at_ns: Option<i64>,
    /// How long a winding-down run waits for trades in progress.
    pub drain_timeout: Duration,
}

/// Requests from outside (signals).
#[derive(Clone, Debug)]
pub enum Control {
    /// Start no new trades; end once nothing is in progress.
    WindDown(String),
    /// End now: cancel working orders and return.
    Stop(String),
}

/// How the run ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Ending {
    /// Wound down with nothing left in progress.
    Clean,
    /// The drain timeout passed with trades still in progress.
    TimedOut,
    /// Stopped on request before winding down finished.
    Forced(String),
}

struct WindDown {
    reason: String,
    since: Instant,
}

/// Everything the engine is built from.
pub struct Parts {
    /// Each strategy with its `entry_cutoff`.
    pub strategies: Vec<(Box<dyn Strategy>, Duration)>,
    pub schedule: Schedule,
    pub restored: Restored,
    pub risk: Risk,
    pub venue: Box<dyn Venue>,
    pub journal: Journal,
    pub clob: ClobManager,
    pub history: mpsc::UnboundedSender<ConditionId>,
    pub held: Held,
}

pub struct Engine {
    slots: Vec<Slot>,
    md: MarketState,
    portfolio: Portfolio,
    risk: Risk,
    venue: Box<dyn Venue>,
    journal: Journal,
    clob: ClobManager,
    orders: HashMap<OrderId, Order>,
    next_order: u64,
    timers: BinaryHeap<Reverse<(i64, u64)>>,
    timer_owner: HashMap<u64, (usize, Timer)>,
    timer_seq: u64,
    queue: VecDeque<(usize, Event)>,
    subscribed: HashSet<ConditionId>,
    history_tx: mpsc::UnboundedSender<ConditionId>,
    history_waiting: HashMap<ConditionId, HashSet<usize>>,
    held: Held,
    last_resync: HashMap<TokenId, Instant>,
    connected: HashSet<Arc<str>>,
    polls: u64,
    schedule: Schedule,
    winding: Option<WindDown>,
}

impl Engine {
    pub fn new(p: Parts) -> Engine {
        let Restored {
            portfolio,
            mut decided,
            last_order,
            ..
        } = p.restored;
        let slots = p
            .strategies
            .into_iter()
            .map(|(s, entry_cutoff)| {
                let id: Arc<str> = Arc::from(s.id());
                Slot {
                    decided: decided.remove(&id).unwrap_or_default(),
                    strategy: s,
                    id,
                    entry_cutoff,
                    wanted: HashSet::new(),
                }
            })
            .collect();
        let e = Engine {
            slots,
            md: MarketState::default(),
            portfolio,
            risk: p.risk,
            venue: p.venue,
            journal: p.journal,
            clob: p.clob,
            orders: HashMap::new(),
            next_order: last_order,
            timers: BinaryHeap::new(),
            timer_owner: HashMap::new(),
            timer_seq: 0,
            queue: VecDeque::new(),
            subscribed: HashSet::new(),
            history_tx: p.history,
            history_waiting: HashMap::new(),
            held: p.held,
            last_resync: HashMap::new(),
            connected: HashSet::new(),
            polls: 0,
            schedule: p.schedule,
            winding: None,
        };
        e.update_held();
        e
    }

    /// Runs until the run has wound down (scheduled stop or [`Control::WindDown`]) or is stopped.
    pub async fn run(
        mut self,
        mut inp: Inputs,
        mut control: mpsc::Receiver<Control>,
        status_every: Duration,
    ) -> Ending {
        let mut clock = tokio::time::interval(Duration::from_secs(1));
        let mut status = tokio::time::interval(status_every);
        status.tick().await;
        let ending = loop {
            let next = self.next_timer_in();
            let to_stop = self
                .schedule
                .stop_at_ns
                .map(|t| Duration::from_nanos((t - now_ns()).max(0) as u64));
            tokio::select! {
                Some(c) = control.recv() => match c {
                    Control::WindDown(why) => self.wind_down(why),
                    Control::Stop(why) => break Ending::Forced(why),
                },
                _ = tokio::time::sleep(to_stop.unwrap_or_default()), if self.winding.is_none() && to_stop.is_some() => {
                    self.wind_down("scheduled stop".into());
                }
                Some(u) = inp.universe.recv() => self.on_universe(u).await,
                Some(ev) = inp.ws.recv() => self.on_ws(ev).await,
                Some(h) = inp.history.recv() => self.on_history(h),
                _ = tokio::time::sleep(next) => self.fire_timers(),
                _ = clock.tick() => {
                    let r = self.venue.on_clock(now_ns());
                    self.on_reports(r).await;
                }
                _ = status.tick() => self.log_status(),
            }
            self.process_queue().await;
            if let Some(w) = &self.winding {
                if self.in_progress().is_empty() {
                    break Ending::Clean;
                }
                if w.since.elapsed() >= self.schedule.drain_timeout {
                    break Ending::TimedOut;
                }
            }
        };
        self.shutdown(&ending).await;
        ending
    }

    // ---- ending ----------------------------------------------------------------------------

    /// Stops new trades; the run ends once nothing is in progress.
    fn wind_down(&mut self, reason: String) {
        if self.winding.is_some() {
            return;
        }
        let busy = self.in_progress();
        let waiting = if busy.is_empty() {
            "nothing in progress".to_string()
        } else {
            format!("waiting for {}", busy.join(", "))
        };
        tracing::info!(
            "winding down ({reason}): no new trades; {waiting} (at most {}); a signal ends it now",
            HumanDuration(self.schedule.drain_timeout)
        );
        self.winding = Some(WindDown {
            reason,
            since: Instant::now(),
        });
        for i in 0..self.slots.len() {
            self.queue.push_back((i, Event::Stopping));
        }
    }

    /// What a winding-down run still waits for (empty = it can end).
    fn in_progress(&self) -> Vec<String> {
        let mut out = Vec::new();
        let open = self.orders.values().filter(|o| !o.status.is_done()).count();
        if open > 0 {
            out.push(format!("{open} working orders"));
        }
        for s in &self.slots {
            if s.strategy.busy() {
                out.push(format!("{} (busy)", s.id));
            }
        }
        out
    }

    /// Whether slot `i` may enter new markets now.
    fn entries_open(&self, i: usize, now: i64) -> bool {
        let cutoff = self.slots[i].entry_cutoff.as_nanos() as i64;
        self.winding.is_none() && self.schedule.stop_at_ns.is_none_or(|stop| now < stop - cutoff)
    }

    /// The strategy holds a position or has a working order in this market.
    fn engaged(&self, strategy: &Arc<str>, market: &ConditionId) -> bool {
        self.portfolio.strategy_holds(strategy, market.as_str())
            || self
                .orders
                .values()
                .any(|o| o.strategy == *strategy && o.req.market == *market && !o.status.is_done())
    }

    // ---- strategies ------------------------------------------------------------------------

    /// Queues `ev` for every strategy that wants market `c`.
    fn broadcast(&mut self, c: &ConditionId, ev: Event) {
        for (i, s) in self.slots.iter().enumerate() {
            if s.wanted.contains(c) {
                self.queue.push_back((i, ev.clone()));
            }
        }
    }

    fn slot_of(&self, strategy: &str) -> Option<usize> {
        self.slots.iter().position(|s| &*s.id == strategy)
    }

    /// Runs queued events through their strategies and carries out the commands, until the queue
    /// is empty (commands may queue more events, e.g. order reports).
    async fn process_queue(&mut self) {
        while let Some((i, ev)) = self.queue.pop_front() {
            let now = now_ns();
            let mut out = Vec::new();
            {
                let entries_open = self.entries_open(i, now);
                let Slot {
                    strategy, id, decided, ..
                } = &mut self.slots[i];
                let mut cx = Ctx {
                    now_ns: now,
                    md: &self.md,
                    strategy: id,
                    portfolio: &self.portfolio,
                    orders: &self.orders,
                    decided,
                    next_order: &mut self.next_order,
                    out: &mut out,
                    entries_open,
                    stop_at_ns: self.schedule.stop_at_ns,
                };
                strategy.on_event(&ev, &mut cx);
            }
            for cmd in out {
                self.command(i, cmd, now).await;
            }
        }
    }

    async fn command(&mut self, i: usize, cmd: Command, now: i64) {
        match cmd {
            Command::Place(id, req) => self.place(i, id, req, now).await,
            Command::Cancel(id) => {
                let mine = self
                    .orders
                    .get(&id)
                    .is_some_and(|o| o.strategy == self.slots[i].id && !o.status.is_done());
                if mine {
                    let r = self.venue.cancel(id, now);
                    self.on_reports(r).await;
                }
            }
            Command::At(t, timer) => {
                self.timer_seq += 1;
                self.timers.push(Reverse((t, self.timer_seq)));
                self.timer_owner.insert(self.timer_seq, (i, timer));
            }
            Command::LoadHistory(c) => {
                if self.md.market(c.as_str()).is_none() {
                    return;
                }
                let waiting = self.history_waiting.entry(c.clone()).or_default();
                let first = waiting.is_empty();
                waiting.insert(i);
                if first {
                    self.md.tape_mut(&c).history = History::Loading;
                    let _ = self.history_tx.send(c);
                }
            }
            Command::Note {
                market,
                what,
                msg,
                detail,
            } => {
                let id = self.slots[i].id.clone();
                tracing::info!("{id}: {msg}");
                if let Some(c) = &market {
                    self.slots[i].decided.insert(c.clone());
                }
                let m = market.as_ref().and_then(|c| self.md.market(c.as_str()));
                let rec = json!({
                    "strategy": &*id,
                    "what": what,
                    "market": market,
                    "question": m.map(|m| &m.info.question),
                    "event": m.map(|m| &m.event_title),
                    "msg": msg,
                    "detail": detail,
                });
                self.journal.decision(&id, now, &rec).await;
            }
        }
    }

    // ---- orders ----------------------------------------------------------------------------

    async fn place(&mut self, i: usize, id: OrderId, req: OrderRequest, now: i64) {
        let strategy = self.slots[i].id.clone();
        let mut order = Order::new(id, strategy.clone(), req, now);
        let open = self.orders.values().filter(|o| !o.status.is_done());
        let market = self.md.market(order.req.market.as_str());
        let label = market.map_or_else(|| order.req.market.to_string(), |m| m.label());
        let outcome = market.map_or("?", |m| m.outcome(&order.req.token)).to_string();
        // New trades (markets the strategy is not in yet) stop near the end of the run.
        let gate = if self.entries_open(i, now) || self.engaged(&strategy, &order.req.market) {
            Ok(())
        } else if self.winding.is_some() {
            Err("the run is winding down: no new trades".to_string())
        } else {
            Err(format!(
                "within entry_cutoff ({}) of the scheduled stop: no new trades",
                HumanDuration(self.slots[i].entry_cutoff)
            ))
        };
        let check = gate.and_then(|_| {
            self.risk
                .check(&order.req, market, &self.portfolio, open, &strategy, now)
        });
        if let Err(why) = check {
            tracing::warn!("{strategy}: order {id} in {label} rejected: {why}");
            order.status = OrderStatus::Rejected;
            order.note = Some(why.clone());
            self.journal.order(&order).await;
            self.orders.insert(id, order);
            self.queue
                .push_back((i, Event::Order(ExecReport::done(id, OrderStatus::Rejected, why))));
            return;
        }
        let r = &order.req;
        tracing::info!(
            "{strategy}: order {id}: {:?} {} {outcome} at {} or better ({:?}, ${}) in {label}: {}",
            r.side,
            r.size,
            r.price,
            r.tif,
            r.notional(),
            r.reason
        );
        self.slots[i].decided.insert(r.market.clone());
        self.journal.order(&order).await;
        let reports = self.venue.submit(&order, &self.md, now);
        self.orders.insert(id, order);
        self.on_reports(reports).await;
    }

    async fn on_reports(&mut self, reports: Vec<ExecReport>) {
        for r in reports {
            let Some(o) = self.orders.get_mut(&r.id()) else {
                continue;
            };
            match &r {
                ExecReport::Fill { fill, .. } => o.apply(fill),
                ExecReport::Done { status, reason, .. } => {
                    o.status = *status;
                    o.note = reason.clone();
                }
            }
            let o = o.clone();
            let m = self.md.market(o.req.market.as_str());
            let label = m.map_or_else(|| o.req.market.to_string(), |m| m.label());
            match &r {
                ExecReport::Fill { fill, .. } => {
                    let (slug, league) = m.map_or((String::new(), String::new()), |m| {
                        (m.info.event_slug.clone(), m.info.league.clone())
                    });
                    let at = FillAt {
                        strategy: &o.strategy,
                        market: &o.req.market,
                        token: &o.req.token,
                        side: o.req.side,
                        event_slug: &slug,
                        league: &league,
                    };
                    let exit = self.portfolio.apply_fill(&at, fill);
                    tracing::info!(
                        "{}: order {} filled {} at {} (fee ${}{}) in {label}",
                        o.strategy,
                        o.id,
                        fill.size,
                        fill.price,
                        fill.fee,
                        if fill.maker { ", maker" } else { "" }
                    );
                    let rec = FillRecord {
                        order: o.id,
                        strategy: o.strategy.clone(),
                        market: o.req.market.clone(),
                        token: o.req.token.clone(),
                        side: o.req.side,
                        event_slug: slug,
                        league,
                        fill: *fill,
                    };
                    self.journal.fill(&rec).await;
                    if let Some(s) = exit {
                        tracing::info!(
                            "{}: sold {} shares in {label}: cost ${}, proceeds ${}, P&L ${}",
                            s.strategy,
                            s.shares,
                            s.cost,
                            s.payout,
                            s.pnl
                        );
                        self.journal.settlement(&s).await;
                    }
                    self.update_held();
                }
                ExecReport::Done { status, reason, .. } => {
                    let avg = o.avg_price().map_or("-".to_string(), |p| p.to_string());
                    tracing::info!(
                        "{}: order {} {status:?}: {} of {} shares, avg {avg}{} in {label}",
                        o.strategy,
                        o.id,
                        o.filled,
                        o.req.size,
                        reason.as_ref().map_or(String::new(), |r| format!(" ({r})")),
                    );
                    self.journal.order(&o).await;
                }
            }
            if let Some(i) = self.slot_of(&o.strategy) {
                self.queue.push_back((i, Event::Order(r)));
            }
        }
    }

    /// Settles positions in a resolved market and cancels its open orders.
    async fn resolve(&mut self, c: &ConditionId, winner: usize, now: i64) {
        self.md.set_winner(c.as_str(), winner);
        let Some(m) = self.md.market(c.as_str()) else { return };
        let Some(token) = m.info.token_ids.get(winner).cloned() else {
            return;
        };
        let (label, outcome) = (m.label(), m.outcome(&token).to_string());
        for s in self.portfolio.settle(c, &token, now) {
            tracing::info!(
                "{}: {label} resolved, {outcome} won: {} shares cost ${}, paid ${}, P&L ${}",
                s.strategy,
                s.shares,
                s.cost,
                s.payout,
                s.pnl
            );
            self.journal.settlement(&s).await;
        }
        let open: Vec<OrderId> = self
            .orders
            .values()
            .filter(|o| o.req.market == *c && !o.status.is_done())
            .map(|o| o.id)
            .collect();
        for id in open {
            let r = self.venue.cancel(id, now);
            self.on_reports(r).await;
        }
        self.update_held();
    }

    /// Tells the universe which closed events to keep looking up (held positions).
    fn update_held(&self) {
        let held: HashMap<String, String> = self
            .portfolio
            .positions()
            .filter(|(_, p)| !p.event_slug.is_empty())
            .map(|(_, p)| (p.event_slug.clone(), p.league.clone()))
            .collect();
        *self.held.lock().unwrap() = held;
    }

    fn has_open_orders(&self, c: &str) -> bool {
        self.orders
            .values()
            .any(|o| o.req.market.as_str() == c && !o.status.is_done())
    }

    // ---- inputs ----------------------------------------------------------------------------

    async fn on_universe(&mut self, u: Update) {
        let now = now_ns();
        let mut seen = HashSet::new();
        let (mut subscribe, mut unsubscribe) = (Vec::new(), Vec::new());
        let mut new = 0;
        for m in u.markets {
            let c = m.info.condition_id.clone();
            seen.insert(c.clone());
            let winner = m.winner;
            let change = self.md.upsert(m);
            if change == Change::Same {
                continue;
            }
            let m = self.md.market(c.as_str()).expect("just added");
            if change == Change::New {
                new += 1;
                for s in &mut self.slots {
                    if s.strategy.wants(m) {
                        s.wanted.insert(c.clone());
                    }
                }
            }
            let wanted = self.slots.iter().any(|s| s.wanted.contains(&c));
            if wanted && m.tradable() && self.subscribed.insert(c.clone()) {
                subscribe.extend(m.info.token_ids.iter().cloned());
            }
            if let Some(w) = winner {
                self.resolve(&c, w, now).await;
            }
            self.broadcast(&c, Event::Market(c.clone()));
        }

        // Markets no longer listed are dropped, unless their league's fetch failed or they still
        // matter (positions, open orders).
        let gone: Vec<ConditionId> = self
            .md
            .markets()
            .filter(|m| !seen.contains(&m.info.condition_id) && !u.failed.contains(&m.info.league))
            .map(|m| m.info.condition_id.clone())
            .filter(|c| !self.portfolio.holds(c.as_str()) && !self.has_open_orders(c.as_str()))
            .collect();
        for c in &gone {
            self.broadcast(c, Event::Removed(c.clone()));
            for s in &mut self.slots {
                s.wanted.remove(c);
            }
            if let Some(m) = self.md.remove(c.as_str())
                && self.subscribed.remove(c)
            {
                unsubscribe.extend(m.info.token_ids);
            }
        }
        // Books of finished markets are no longer needed.
        let finished: Vec<ConditionId> = self
            .subscribed
            .iter()
            .filter(|c| self.md.market(c.as_str()).is_none_or(|m| !m.tradable()))
            .filter(|c| !self.has_open_orders(c.as_str()))
            .cloned()
            .collect();
        for c in finished {
            self.subscribed.remove(&c);
            if let Some(m) = self.md.market(c.as_str()) {
                unsubscribe.extend(m.info.token_ids.iter().cloned());
            }
            self.md.drop_books(c.as_str());
        }
        if !subscribe.is_empty() || !unsubscribe.is_empty() {
            self.clob.update(&subscribe, &unsubscribe);
        }
        let msg = format!(
            "universe: {} markets ({}), {} with books | +{new} -{} | {} leagues failed",
            self.md.markets().count(),
            self.wanted_summary(),
            self.subscribed.len(),
            gone.len(),
            u.failed.len()
        );
        if self.polls == 0 || new > 0 || !gone.is_empty() {
            tracing::info!("{msg}");
        } else {
            tracing::debug!("{msg}");
        }
        self.polls += 1;
    }

    fn wanted_summary(&self) -> String {
        let parts: Vec<String> = self
            .slots
            .iter()
            .map(|s| format!("{} wanted by {}", s.wanted.len(), s.id))
            .collect();
        parts.join(", ")
    }

    async fn on_ws(&mut self, ev: WsEvent) {
        let text = match ev {
            WsEvent::Connected { feed, .. } => {
                self.connected.insert(feed);
                return;
            }
            WsEvent::Disconnected { feed, .. } => {
                self.connected.remove(&feed);
                return;
            }
            WsEvent::Text { text, .. } => text,
        };
        let Some(parts) = split_frame(&text) else { return };
        for raw in parts {
            let Ok(msg) = serde_json::from_str::<MarketMsg>(raw.get()) else {
                continue;
            };
            match &msg {
                MarketMsg::LastTradePrice(lt) => {
                    if self.md.market(lt.market.as_str()).is_none() {
                        continue;
                    }
                    let t = TapeTrade::live(lt);
                    self.md.tape_mut(&lt.market).push_live(t.clone());
                    let r = self.venue.on_trade(&t, &self.md);
                    self.on_reports(r).await;
                    self.broadcast(&lt.market, Event::Trade(lt.market.clone(), t));
                }
                MarketMsg::MarketResolved(v) => {
                    let won = v.get("winning_asset_id").and_then(|w| w.as_str());
                    let Some(m) = won.and_then(|w| self.md.market_of(w)) else {
                        continue;
                    };
                    let c = m.info.condition_id.clone();
                    if let Some(i) = won.and_then(|w| m.info.outcome_index(w)) {
                        self.resolve(&c, i, now_ns()).await;
                        self.broadcast(&c, Event::Market(c.clone()));
                    }
                }
                _ => {
                    if msg.market().is_none_or(|c| self.md.market(c.as_str()).is_none()) {
                        continue;
                    }
                    let rep = self.md.books.apply(&msg);
                    for t in rep.touched {
                        if let Some(c) = self.md.market_of(t.as_str()).map(|m| m.info.condition_id.clone()) {
                            self.broadcast(&c, Event::Book(t));
                        }
                    }
                    let due: Vec<TokenId> = rep
                        .missing
                        .into_iter()
                        .filter(|t| self.last_resync.get(t).is_none_or(|at| at.elapsed() > RESYNC_EVERY))
                        .collect();
                    if !due.is_empty() {
                        self.clob.resync(&due);
                        self.last_resync.retain(|_, at| at.elapsed() < RESYNC_EVERY);
                        for t in due {
                            self.last_resync.insert(t, Instant::now());
                        }
                    }
                }
            }
        }
    }

    fn on_history(&mut self, h: history::Loaded) {
        let waiting = self.history_waiting.remove(&h.market).unwrap_or_default();
        if self.md.market(h.market.as_str()).is_none() {
            return;
        }
        let tape = self.md.tape_mut(&h.market);
        match h.result {
            Ok((trades, truncated)) => tape.set_history(trades, truncated, now_ns()),
            Err(e) => tape.history = History::Failed(e),
        }
        for i in waiting {
            self.queue.push_back((i, Event::History(h.market.clone())));
        }
    }

    fn next_timer_in(&self) -> Duration {
        match self.timers.peek() {
            Some(Reverse((t, _))) => Duration::from_nanos((t - now_ns()).max(0) as u64),
            None => Duration::from_secs(3600),
        }
    }

    fn fire_timers(&mut self) {
        let now = now_ns();
        while let Some(Reverse((t, seq))) = self.timers.peek().copied() {
            if t > now {
                break;
            }
            self.timers.pop();
            if let Some((i, timer)) = self.timer_owner.remove(&seq) {
                self.queue.push_back((i, Event::Timer(timer)));
            }
        }
    }

    // ---- reporting -------------------------------------------------------------------------

    fn log_status(&self) {
        let now = now_ns();
        let open_orders = self.orders.values().filter(|o| !o.status.is_done()).count();
        tracing::info!(
            "status: {} markets, {} with books on {}/{} connections | {open_orders} open orders | positions ${}, spent today ${}",
            self.md.markets().count(),
            self.subscribed.len(),
            self.connected.len(),
            self.clob.shard_count(),
            self.portfolio.open_cost(),
            self.portfolio.spent_today(now),
        );
        let busy = self.in_progress();
        if let Some(w) = self.winding.as_ref().filter(|_| !busy.is_empty()) {
            tracing::info!(
                "  winding down ({}) for {}: waiting for {}",
                w.reason,
                HumanDuration(Duration::from_secs(w.since.elapsed().as_secs())),
                busy.join(", ")
            );
        }
        for s in &self.slots {
            let t = self.portfolio.totals(&s.id);
            tracing::info!(
                "  {}: {} markets | {} open positions (${}) | {} settled, {} won, staked ${}, P&L ${}",
                s.id,
                s.wanted.len(),
                t.open,
                t.open_cost,
                t.settled,
                t.won,
                t.staked,
                t.pnl
            );
        }
    }

    async fn shutdown(&mut self, ending: &Ending) {
        match ending {
            Ending::Clean => tracing::info!("wound down: nothing left in progress"),
            Ending::TimedOut => tracing::warn!(
                "drain timeout ({}) passed with {} still in progress; stopping anyway",
                HumanDuration(self.schedule.drain_timeout),
                self.in_progress().join(", ")
            ),
            Ending::Forced(why) => {
                let busy = self.in_progress();
                if busy.is_empty() {
                    tracing::info!("stopping now ({why})");
                } else {
                    tracing::warn!("stopping now ({why}) with {} in progress", busy.join(", "));
                }
            }
        }
        let now = now_ns();
        let open: Vec<OrderId> = self
            .orders
            .values()
            .filter(|o| !o.status.is_done())
            .map(|o| o.id)
            .collect();
        if !open.is_empty() {
            tracing::info!("cancelling {} open orders", open.len());
        }
        for id in open {
            let r = self.venue.cancel(id, now);
            self.on_reports(r).await;
        }
        self.log_status();
        let pnl: Fixed6 = self
            .slots
            .iter()
            .map(|s| self.portfolio.totals(&s.id).pnl)
            .fold(Fixed6::ZERO, |a, b| a + b);
        tracing::info!("engine stopped; realised P&L ${pnl} over the journal window");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::paper::Paper;
    use crate::market::Market;
    use crate::order::Tif;
    use crate::risk::RiskConfig;
    use crate::strategy::testing::{market, px};
    use pm_store::{Store, StoreConfig};
    use tokio_util::sync::CancellationToken;

    /// Buys 10 shares of the first token at `price`, once, as soon as its book shows an ask.
    struct BuyOnce {
        price: &'static str,
        tif: Tif,
    }

    impl Strategy for BuyOnce {
        fn id(&self) -> &str {
            "buy_once"
        }

        fn wants(&self, _: &Market) -> bool {
            true
        }

        fn on_event(&mut self, ev: &Event, cx: &mut Ctx<'_>) {
            let Event::Book(t) = ev else { return };
            let Some(m) = cx.md.market_of(t.as_str()) else { return };
            let c = m.info.condition_id.clone();
            if *t != m.info.token_ids[0] || cx.has_decided(&c) {
                return;
            }
            cx.place(OrderRequest::buy(
                &c,
                t,
                px(self.price),
                px("10"),
                self.tif,
                "test".into(),
            ));
        }
    }

    fn book_frame(token: &str) -> WsEvent {
        let text = format!(
            r#"{{"event_type":"book","market":"m","asset_id":"{token}","timestamp":"1","bids":[],"asks":[{{"price":"0.2","size":"100"}}]}}"#
        );
        WsEvent::Text {
            session: 1,
            recv_ns: 0,
            text,
        }
    }

    fn update(m: &Market) -> Update {
        Update {
            markets: vec![m.clone()],
            failed: HashSet::new(),
        }
    }

    /// A temporary journal; remove `root` at the end.
    fn store(name: &str) -> (Store, std::path::PathBuf) {
        let root = std::env::temp_dir().join(format!("pmt-engine-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let store = Store::start(StoreConfig {
            root: root.clone(),
            ..Default::default()
        })
        .unwrap();
        (store, root)
    }

    fn engine(store: &Store, strategy: BuyOnce, schedule: Schedule, cutoff: Duration) -> Engine {
        let (e, _) = engine_with(
            store,
            vec![(Box::new(strategy), cutoff)],
            schedule,
            RiskConfig::default(),
            Restored::default(),
        );
        e
    }

    /// An engine whose market-channel connections never open; also returns the history request
    /// receiver.
    fn engine_with(
        store: &Store,
        strategies: Vec<(Box<dyn Strategy>, Duration)>,
        schedule: Schedule,
        risk: RiskConfig,
        restored: Restored,
    ) -> (Engine, mpsc::UnboundedReceiver<ConditionId>) {
        let cancel = CancellationToken::new();
        cancel.cancel();
        let (ws_tx, _ws_rx) = mpsc::channel(16);
        let (hist_tx, hist_rx) = mpsc::unbounded_channel();
        let e = Engine::new(Parts {
            strategies,
            schedule,
            restored,
            risk: Risk::new(&risk),
            venue: Box::new(Paper::default()),
            journal: Journal::new(store.handle(), "paper"),
            clob: ClobManager::new(10, true, ws_tx, cancel),
            history: hist_tx,
            held: Held::default(),
        });
        (e, hist_rx)
    }

    const UNTIL_STOPPED: Schedule = Schedule {
        stop_at_ns: None,
        drain_timeout: Duration::from_secs(3600),
    };

    fn taker() -> BuyOnce {
        BuyOnce {
            price: "0.25",
            tif: Tif::Fak,
        }
    }

    #[tokio::test]
    async fn an_order_flows_from_strategy_to_fill_to_settlement() {
        let (store, root) = store("flow");
        let mut e = engine(&store, taker(), UNTIL_STOPPED, Duration::ZERO);
        let m = market("m", crate::market::MarketKind::Map(1), "cs2", 0);
        e.on_universe(update(&m)).await;
        e.process_queue().await;
        assert!(e.subscribed.contains("m"));
        assert_eq!(e.clob.shard_count(), 1);

        e.on_ws(book_frame("m-a")).await;
        e.process_queue().await;
        let o = e.orders.values().next().unwrap();
        assert_eq!(o.status, OrderStatus::Filled);
        assert_eq!(o.avg_price().unwrap().to_string(), "0.2");
        assert!(e.portfolio.holds("m"));
        assert_eq!(e.held.lock().unwrap().get("ev").map(|s| s.as_str()), Some("cs2"));
        // A second book update does not buy again.
        e.on_ws(book_frame("m-a")).await;
        e.process_queue().await;
        assert_eq!(e.orders.len(), 1);

        // Gamma reports the market resolved for outcome A.
        let resolved = Market {
            winner: Some(0),
            ..m.clone()
        };
        e.on_universe(update(&resolved)).await;
        e.process_queue().await;
        let t = e.portfolio.totals("buy_once");
        assert_eq!((t.open, t.settled, t.won), (0, 1, 1));
        assert_eq!(t.pnl.to_string(), "8"); // 10 shares paid 2, no fees
        assert!(!e.subscribed.contains("m"), "books of finished markets are dropped");

        // The market leaves the listings: forgotten.
        e.on_universe(Update {
            markets: vec![],
            failed: HashSet::new(),
        })
        .await;
        assert!(e.md.market("m").is_none());

        drop(e);
        store.shutdown().await.unwrap();
        let r = crate::journal::restore(&root, "paper").unwrap();
        assert_eq!(r.portfolio.totals("buy_once").pnl.to_string(), "8");
        assert!(r.decided[&Arc::<str>::from("buy_once")].contains("m"));
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[tokio::test]
    async fn no_new_markets_within_the_entry_cutoff_but_engaged_ones_continue() {
        let (store, root) = store("cutoff");
        // Stops in 1 h; this strategy stops entering 2 h before.
        let schedule = Schedule {
            stop_at_ns: Some(now_ns() + 3600 * 1_000_000_000),
            drain_timeout: Duration::from_secs(60),
        };
        let mut e = engine(&store, taker(), schedule, Duration::from_secs(7200));
        e.on_universe(update(&market("m", crate::market::MarketKind::Map(1), "cs2", 0)))
            .await;
        e.on_ws(book_frame("m-a")).await;
        e.process_queue().await;
        let o = e.orders.values().next().unwrap();
        assert_eq!(o.status, OrderStatus::Rejected);
        assert!(o.note.as_deref().unwrap().contains("entry_cutoff"), "{:?}", o.note);
        assert!(
            !e.slots[0].decided.contains("m"),
            "a rejected order leaves the market undecided"
        );

        drop(e); // the store finishes only once every journal handle is gone

        // In a market it already holds, the strategy may still trade (exit, add, hedge).
        let mut e = engine(&store, taker(), UNTIL_STOPPED, Duration::ZERO);
        e.on_universe(update(&market("m", crate::market::MarketKind::Map(1), "cs2", 0)))
            .await;
        e.on_ws(book_frame("m-a")).await;
        e.process_queue().await;
        e.wind_down("test".into());
        let req = |c: &str| {
            OrderRequest::buy(
                &c.into(),
                &format!("{c}-a").into(),
                px("0.25"),
                px("10"),
                Tif::Fak,
                "x".into(),
            )
        };
        e.place(0, OrderId(50), req("m"), now_ns()).await;
        assert_eq!(e.orders[&OrderId(50)].status, OrderStatus::Filled);
        e.on_universe(update(&market("n", crate::market::MarketKind::Map(1), "cs2", 0)))
            .await;
        e.place(0, OrderId(51), req("n"), now_ns()).await;
        let o = &e.orders[&OrderId(51)];
        assert_eq!(o.status, OrderStatus::Rejected);
        assert!(o.note.as_deref().unwrap().contains("winding down"));
        drop(e);
        store.shutdown().await.unwrap();
        std::fs::remove_dir_all(&root).unwrap();
    }

    /// Runs the engine with live input channels; returns the control sender and the run's task.
    fn spawn(
        e: Engine,
    ) -> (
        mpsc::Sender<Control>,
        tokio::task::JoinHandle<Ending>,
        Vec<Box<dyn std::any::Any + Send>>,
    ) {
        let (uni_tx, uni_rx) = mpsc::channel(1);
        let (ws_tx, ws_rx) = mpsc::channel(1);
        let (hist_tx, hist_rx) = mpsc::channel(1);
        let (ctl_tx, ctl_rx) = mpsc::channel(4);
        let inputs = Inputs {
            universe: uni_rx,
            ws: ws_rx,
            history: hist_rx,
        };
        let task = tokio::spawn(e.run(inputs, ctl_rx, Duration::from_secs(3600)));
        (ctl_tx, task, vec![Box::new(uni_tx), Box::new(ws_tx), Box::new(hist_tx)])
    }

    #[tokio::test]
    async fn winding_down_waits_for_working_orders_up_to_the_timeout() {
        let (store, root) = store("wind");
        let schedule = Schedule {
            stop_at_ns: None,
            drain_timeout: Duration::from_secs(2),
        };
        // A bid below the ask rests (good till cancelled).
        let resting = BuyOnce {
            price: "0.15",
            tif: Tif::Gtc,
        };
        let mut e = engine(&store, resting, schedule, Duration::ZERO);
        e.on_universe(update(&market("m", crate::market::MarketKind::Map(1), "cs2", 0)))
            .await;
        e.on_ws(book_frame("m-a")).await;
        e.process_queue().await;
        assert_eq!(e.in_progress(), ["1 working orders"]);
        let (ctl, task, _keep) = spawn(e);
        ctl.send(Control::WindDown("test".into())).await.unwrap();
        let t0 = Instant::now();
        assert_eq!(task.await.unwrap(), Ending::TimedOut);
        assert!(t0.elapsed() >= Duration::from_secs(2));

        // With nothing in progress it ends at once; a stop request ends it at once too.
        let idle = engine(&store, taker(), schedule, Duration::ZERO);
        let (ctl, task, _keep) = spawn(idle);
        ctl.send(Control::WindDown("test".into())).await.unwrap();
        assert_eq!(task.await.unwrap(), Ending::Clean);
        let idle = engine(&store, taker(), UNTIL_STOPPED, Duration::ZERO);
        let (ctl, task, _keep) = spawn(idle);
        ctl.send(Control::Stop("now".into())).await.unwrap();
        assert_eq!(task.await.unwrap(), Ending::Forced("now".into()));
        store.shutdown().await.unwrap();
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[tokio::test]
    async fn the_scheduled_stop_winds_the_run_down() {
        let (store, root) = store("scheduled");
        let schedule = Schedule {
            stop_at_ns: Some(now_ns() + 300_000_000),
            drain_timeout: Duration::from_secs(60),
        };
        let e = engine(&store, taker(), schedule, Duration::ZERO);
        let (_ctl, task, _keep) = spawn(e);
        let ending = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ending, Ending::Clean);
        store.shutdown().await.unwrap();
        std::fs::remove_dir_all(&root).unwrap();
    }

    type Hook = Box<dyn FnMut(&Event, &mut Ctx<'_>) + Send>;
    type Seen = Arc<std::sync::Mutex<Vec<Event>>>;

    /// A strategy that wants every market, records every event and runs `hook` on it.
    struct Script {
        id: &'static str,
        hook: Hook,
        seen: Seen,
    }

    impl Strategy for Script {
        fn id(&self) -> &str {
            self.id
        }

        fn wants(&self, _: &Market) -> bool {
            true
        }

        fn on_event(&mut self, ev: &Event, cx: &mut Ctx<'_>) {
            self.seen.lock().unwrap().push(ev.clone());
            (self.hook)(ev, cx);
        }
    }

    fn script(
        id: &'static str,
        hook: impl FnMut(&Event, &mut Ctx<'_>) + Send + 'static,
    ) -> ((Box<dyn Strategy>, Duration), Seen) {
        let seen = Seen::default();
        let s = Script {
            id,
            hook: Box::new(hook),
            seen: seen.clone(),
        };
        ((Box::new(s), Duration::ZERO), seen)
    }

    fn text(t: &str) -> WsEvent {
        WsEvent::Text {
            session: 1,
            recv_ns: 0,
            text: t.to_string(),
        }
    }

    /// A book snapshot for `token` with one level per side (`None` = empty side).
    fn book(token: &str, bid: Option<&str>, ask: Option<&str>) -> WsEvent {
        let level = |p: Option<&str>| p.map_or(String::new(), |p| format!(r#"{{"price":"{p}","size":"100"}}"#));
        text(&format!(
            r#"{{"event_type":"book","market":"m","asset_id":"{token}","timestamp":"1","bids":[{}],"asks":[{}]}}"#,
            level(bid),
            level(ask)
        ))
    }

    fn map(c: &str) -> Market {
        market(c, crate::market::MarketKind::Map(1), "cs2", 0)
    }

    fn order(market: &str, token: &str, side: pm_core::Side, price: &str, tif: Tif) -> OrderRequest {
        OrderRequest {
            side,
            ..OrderRequest::buy(&market.into(), &token.into(), px(price), px("10"), tif, "test".into())
        }
    }

    fn rejected(seen: &Seen) -> Vec<OrderId> {
        seen.lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                Event::Order(ExecReport::Done {
                    id,
                    status: OrderStatus::Rejected,
                    ..
                }) => Some(*id),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn a_resolution_on_the_market_channel_settles_and_survives_a_stale_gamma_poll() {
        let (store, root) = store("resolved");
        let mut e = engine(&store, taker(), UNTIL_STOPPED, Duration::ZERO);
        e.on_universe(update(&map("m"))).await;
        e.on_ws(book_frame("m-a")).await;
        e.process_queue().await;
        assert!(e.portfolio.holds("m"));

        let resolved = r#"{"event_type":"market_resolved","market":"m","winning_asset_id":"m-b","winning_outcome":"B","timestamp":"5"}"#;
        e.on_ws(text(resolved)).await;
        e.process_queue().await;
        let t = e.portfolio.totals("buy_once");
        assert_eq!((t.settled, t.won, t.pnl.to_string()), (1, 0, "-2".to_string()));
        assert_eq!(e.md.market("m").unwrap().winner, Some(1));

        // Gamma still lists the market open and without a winner: the winner is kept, the market
        // is not tradable, and its books are dropped.
        e.on_universe(update(&map("m"))).await;
        e.process_queue().await;
        let m = e.md.market("m").unwrap();
        assert_eq!(m.winner, Some(1));
        assert!(!m.tradable());
        assert!(!e.subscribed.contains("m"));
        assert!(e.md.book("m-a").is_none(), "no stale book");
        assert_eq!(e.orders.len(), 1, "no new bet in a resolved market");
        drop(e);
        store.shutdown().await.unwrap();
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[tokio::test]
    async fn selling_realises_pnl_once_in_the_portfolio_the_journal_and_the_report() {
        use pm_core::Side;
        let (store, root) = store("sell");
        let (s, seen) = script("trader", |_, _| {});
        let (mut e, _h) = engine_with(
            &store,
            vec![s],
            UNTIL_STOPPED,
            RiskConfig::default(),
            Restored::default(),
        );
        e.on_universe(update(&map("m"))).await;
        e.on_ws(book("m-a", None, Some("0.2"))).await;
        e.place(0, OrderId(1), order("m", "m-a", Side::Buy, "0.25", Tif::Fak), now_ns())
            .await;
        // Selling more than held is refused.
        e.place(
            0,
            OrderId(2),
            OrderRequest {
                size: px("11"),
                ..order("m", "m-a", Side::Sell, "0.3", Tif::Fak)
            },
            now_ns(),
        )
        .await;
        assert_eq!(e.orders[&OrderId(2)].status, OrderStatus::Rejected);
        // The bid moved up: sell everything at 0.35.
        e.on_ws(book("m-a", Some("0.35"), None)).await;
        e.place(0, OrderId(3), order("m", "m-a", Side::Sell, "0.3", Tif::Fak), now_ns())
            .await;
        e.process_queue().await;
        assert_eq!(e.orders[&OrderId(3)].status, OrderStatus::Filled);
        assert!(!e.portfolio.holds("m"));
        let t = e.portfolio.totals("trader");
        assert_eq!(
            (t.settled, t.won, t.pnl.to_string(), t.staked.to_string()),
            (1, 1, "1.5".into(), "2".into())
        );
        assert_eq!(rejected(&seen), [OrderId(2)]);
        // Nothing is held any more: the market can go.
        e.on_universe(Update {
            markets: vec![],
            failed: HashSet::new(),
        })
        .await;
        assert!(e.md.market("m").is_none());
        drop(e);
        store.shutdown().await.unwrap();

        let r = crate::journal::restore(&root, "paper").unwrap();
        let t = r.portfolio.totals("trader");
        assert_eq!(
            (t.settled, t.pnl.to_string()),
            (1, "1.5".into()),
            "the exit is not counted twice"
        );
        assert_eq!(r.last_order, 3);
        let report = crate::report::run(&crate::report::Options {
            root: root.clone(),
            mode: "paper".into(),
            from: None,
            to: None,
            strategies: vec![],
            by: crate::report::Period::Day,
        })
        .unwrap();
        let st = &report.strategies[0];
        assert_eq!((st.settled, st.won, st.pnl.to_string()), (1, 1, "1.5".into()));
        assert_eq!((st.orders, st.filled, st.rejected), (2, 2, 1));
        assert!(report.open.is_empty());
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[tokio::test]
    async fn risk_rejections_reach_the_strategy_and_leave_the_market_undecided() {
        let (store, root) = store("halt");
        let (s, seen) = script("s", |ev, cx| {
            if let Event::Book(t) = ev
                && t.as_str() == "m-a"
            {
                cx.place(order("m", "m-a", pm_core::Side::Buy, "0.25", Tif::Fak));
            }
        });
        let halted = RiskConfig {
            halt: true,
            ..Default::default()
        };
        let (mut e, _h) = engine_with(&store, vec![s], UNTIL_STOPPED, halted, Restored::default());
        e.on_universe(update(&map("m"))).await;
        e.on_ws(book_frame("m-a")).await;
        e.process_queue().await;
        assert_eq!(rejected(&seen), [OrderId(1)]);
        assert!(e.orders[&OrderId(1)].note.as_deref().unwrap().contains("halted"));
        assert!(!e.slots[0].decided.contains("m"));
        drop(e);
        store.shutdown().await.unwrap();
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[tokio::test]
    async fn one_history_load_answers_every_strategy_that_asked() {
        let (store, root) = store("history");
        let ask = |ev: &Event, cx: &mut Ctx<'_>| {
            if let Event::Market(c) = ev {
                cx.load_history(c);
            }
        };
        let (a, seen_a) = script("a", ask);
        let (b, seen_b) = script("b", ask);
        let (mut e, mut requests) = engine_with(
            &store,
            vec![a, b],
            UNTIL_STOPPED,
            RiskConfig::default(),
            Restored::default(),
        );
        e.on_universe(update(&map("m"))).await;
        e.process_queue().await;
        assert_eq!(requests.try_recv().unwrap().as_str(), "m");
        assert!(requests.try_recv().is_err(), "requested once");
        assert_eq!(e.md.tape("m").unwrap().history, History::Loading);

        let trade = TapeTrade {
            t_ns: 5,
            token: "m-a".into(),
            price: px("0.4"),
            size: px("10"),
            side: pm_core::Side::Buy,
            tx: None,
            source: crate::tape::Source::History,
        };
        e.on_history(history::Loaded {
            market: "m".into(),
            result: Ok((vec![trade], false)),
        });
        e.process_queue().await;
        for seen in [&seen_a, &seen_b] {
            assert!(
                seen.lock()
                    .unwrap()
                    .iter()
                    .any(|e| matches!(e, Event::History(c) if c.as_str() == "m"))
            );
        }
        assert_eq!(e.md.tape("m").unwrap().notional_before(10).to_string(), "4");
        // A failure is reported the same way.
        e.command(0, Command::LoadHistory("m".into()), now_ns()).await;
        e.on_history(history::Loaded {
            market: "m".into(),
            result: Err("boom".into()),
        });
        assert_eq!(e.md.tape("m").unwrap().history, History::Failed("boom".into()));
        drop(e);
        store.shutdown().await.unwrap();
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[tokio::test]
    async fn timers_fire_when_due_in_time_order() {
        let (store, root) = store("timers");
        let (s, seen) = script("s", |ev, cx| {
            if let Event::Market(c) = ev {
                cx.at(cx.now_ns + 60_000_000, Some(c), "late");
                cx.at(cx.now_ns + 20_000_000, Some(c), "early");
            }
        });
        let (mut e, _h) = engine_with(
            &store,
            vec![s],
            UNTIL_STOPPED,
            RiskConfig::default(),
            Restored::default(),
        );
        e.on_universe(update(&map("m"))).await;
        e.process_queue().await;
        let tags = |seen: &Seen| -> Vec<&'static str> {
            seen.lock()
                .unwrap()
                .iter()
                .filter_map(|e| match e {
                    Event::Timer(t) => Some(t.tag),
                    _ => None,
                })
                .collect()
        };
        e.fire_timers();
        e.process_queue().await;
        assert!(tags(&seen).is_empty(), "not due yet");
        assert!(e.next_timer_in() <= Duration::from_millis(20));
        tokio::time::sleep(Duration::from_millis(30)).await;
        e.fire_timers();
        e.process_queue().await;
        assert_eq!(tags(&seen), ["early"]);
        tokio::time::sleep(Duration::from_millis(40)).await;
        e.fire_timers();
        e.process_queue().await;
        assert_eq!(tags(&seen), ["early", "late"]);
        drop(e);
        store.shutdown().await.unwrap();
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[tokio::test]
    async fn a_strategy_can_only_cancel_its_own_orders() {
        let (store, root) = store("cancel");
        let (a, seen_a) = script("a", |_, _| {});
        let (b, _) = script("b", |_, _| {});
        let (mut e, _h) = engine_with(
            &store,
            vec![a, b],
            UNTIL_STOPPED,
            RiskConfig::default(),
            Restored::default(),
        );
        e.on_universe(update(&map("m"))).await;
        e.on_ws(book("m-a", None, Some("0.2"))).await;
        e.place(
            0,
            OrderId(1),
            order("m", "m-a", pm_core::Side::Buy, "0.15", Tif::Gtc),
            now_ns(),
        )
        .await;
        assert_eq!(e.orders[&OrderId(1)].status, OrderStatus::Open);
        e.command(1, Command::Cancel(OrderId(1)), now_ns()).await;
        assert_eq!(e.orders[&OrderId(1)].status, OrderStatus::Open, "not b's order");
        e.command(0, Command::Cancel(OrderId(1)), now_ns()).await;
        e.process_queue().await;
        assert_eq!(e.orders[&OrderId(1)].status, OrderStatus::Cancelled);
        assert!(seen_a.lock().unwrap().iter().any(|e| matches!(
            e,
            Event::Order(ExecReport::Done {
                status: OrderStatus::Cancelled,
                ..
            })
        )));
        drop(e);
        store.shutdown().await.unwrap();
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[tokio::test]
    async fn order_ids_continue_from_the_journal() {
        let (store, root) = store("ids");
        let restored = Restored {
            last_order: 41,
            ..Default::default()
        };
        let (mut e, _h) = engine_with(
            &store,
            vec![(Box::new(taker()), Duration::ZERO)],
            UNTIL_STOPPED,
            RiskConfig::default(),
            restored,
        );
        e.on_universe(update(&map("m"))).await;
        e.on_ws(book_frame("m-a")).await;
        e.process_queue().await;
        assert_eq!(e.orders.keys().copied().collect::<Vec<_>>(), [OrderId(42)]);
        drop(e);
        store.shutdown().await.unwrap();
        std::fs::remove_dir_all(&root).unwrap();
    }
}
