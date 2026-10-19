//! Execution venues. The engine sends orders to one [`Venue`]; [`paper::Paper`] simulates
//! fills against the live books and tape. A live venue (API keys, EIP-712 order signing, the
//! user channel) will live in `crates/pm-exec` and implement the same trait, delivering its
//! asynchronous reports through a channel the engine also listens to.

pub mod paper;

use crate::order::{Fill, Order, OrderId, OrderStatus};
use crate::state::MarketState;
use crate::tape::TapeTrade;

/// What happened to an order.
#[derive(Clone, Debug, PartialEq)]
pub enum ExecReport {
    Fill {
        id: OrderId,
        fill: Fill,
    },
    /// Final state; `reason` for cancellations, expiries and rejections.
    Done {
        id: OrderId,
        status: OrderStatus,
        reason: Option<String>,
    },
}

impl ExecReport {
    pub fn id(&self) -> OrderId {
        match self {
            ExecReport::Fill { id, .. } | ExecReport::Done { id, .. } => *id,
        }
    }

    pub fn done(id: OrderId, status: OrderStatus, reason: impl Into<String>) -> ExecReport {
        ExecReport::Done {
            id,
            status,
            reason: Some(reason.into()),
        }
    }
}

pub trait Venue: Send {
    fn name(&self) -> &'static str;
    /// Sends a new order (already risk-checked). Returns the reports known at once.
    fn submit(&mut self, order: &Order, md: &MarketState, now_ns: i64) -> Vec<ExecReport>;
    fn cancel(&mut self, id: OrderId, now_ns: i64) -> Vec<ExecReport>;
    /// A trade printed on the tape (the paper venue fills resting orders from it).
    fn on_trade(&mut self, trade: &TapeTrade, md: &MarketState) -> Vec<ExecReport>;
    /// Called about once a second (expiries).
    fn on_clock(&mut self, now_ns: i64) -> Vec<ExecReport>;
}
