//! Orders: what strategies ask for, and what the engine tracks.

use std::fmt;
use std::sync::Arc;

use pm_core::{ConditionId, Fixed6, Price, Side, Size, TokenId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OrderId(pub u64);

impl fmt::Display for OrderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.0)
    }
}

/// Time in force (Polymarket's order types).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Tif {
    /// Fill and kill: take what is available at or better than the limit, cancel the rest.
    Fak,
    /// Fill or kill: everything at once, or nothing.
    Fok,
    /// Rest until cancelled.
    Gtc,
    /// Rest until cancelled or `expires_ns`.
    Gtd { expires_ns: i64 },
}

impl Tif {
    /// Whatever does not fill at once stays on the book.
    pub fn rests(self) -> bool {
        matches!(self, Tif::Gtc | Tif::Gtd { .. })
    }
}

/// A limit order a strategy wants placed.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrderRequest {
    pub market: ConditionId,
    pub token: TokenId,
    pub side: Side,
    pub price: Price,
    /// Shares.
    pub size: Size,
    pub tif: Tif,
    /// Why, for the journal and the log.
    pub reason: String,
}

impl OrderRequest {
    pub fn buy(market: &ConditionId, token: &TokenId, price: Price, size: Size, tif: Tif, reason: String) -> Self {
        OrderRequest {
            market: market.clone(),
            token: token.clone(),
            side: Side::Buy,
            price,
            size,
            tif,
            reason,
        }
    }

    /// USDC at the limit price.
    pub fn notional(&self) -> Fixed6 {
        self.price.mul_fixed(self.size)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    Open,
    Filled,
    /// Cancelled, or the unfilled rest of a FAK / FOK order.
    Cancelled,
    Expired,
    Rejected,
}

impl OrderStatus {
    pub fn is_done(self) -> bool {
        self != OrderStatus::Open
    }
}

/// One fill of an order.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Fill {
    pub price: Price,
    pub size: Size,
    pub fee: Fixed6,
    /// We provided the liquidity.
    pub maker: bool,
    pub t_ns: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Order {
    pub id: OrderId,
    pub strategy: Arc<str>,
    pub req: OrderRequest,
    pub created_ns: i64,
    pub status: OrderStatus,
    pub filled: Size,
    /// USDC paid (buy) or received (sell) for the filled shares, before fees.
    pub notional: Fixed6,
    pub fees: Fixed6,
    /// Why it was rejected, cancelled or expired.
    pub note: Option<String>,
}

impl Order {
    pub fn new(id: OrderId, strategy: Arc<str>, req: OrderRequest, now_ns: i64) -> Order {
        Order {
            id,
            strategy,
            req,
            created_ns: now_ns,
            status: OrderStatus::Open,
            filled: Fixed6::ZERO,
            notional: Fixed6::ZERO,
            fees: Fixed6::ZERO,
            note: None,
        }
    }

    pub fn remaining(&self) -> Size {
        self.req.size - self.filled
    }

    pub fn apply(&mut self, f: &Fill) {
        self.filled += f.size;
        self.notional += f.price.mul_fixed(f.size);
        self.fees += f.fee;
    }

    pub fn avg_price(&self) -> Option<Price> {
        self.notional.div_fixed(self.filled)
    }
}

/// Shares that `usdc` buys at `price`, rounded down to 0.01 and raised to `min_size` (Polymarket
/// rejects smaller limit orders).
pub fn shares_for(usdc: Fixed6, price: Price, min_size: Size) -> Size {
    let shares = usdc
        .div_fixed(price)
        .unwrap_or(Fixed6::ZERO)
        .floor_to(Fixed6::from_raw(10_000));
    shares.max(min_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn px(s: &str) -> Fixed6 {
        s.parse().unwrap()
    }

    #[test]
    fn share_sizing() {
        assert_eq!(shares_for(px("2"), px("0.22"), px("5")).to_string(), "9.09");
        assert_eq!(
            shares_for(px("1"), px("0.32"), px("5")).to_string(),
            "5",
            "minimum size"
        );
    }

    #[test]
    fn fills_accumulate() {
        let req = OrderRequest::buy(&"m".into(), &"t".into(), px("0.3"), px("10"), Tif::Fak, "x".into());
        let mut o = Order::new(OrderId(1), "s".into(), req, 0);
        for (p, s) in [("0.28", "4"), ("0.3", "2")] {
            o.apply(&Fill {
                price: px(p),
                size: px(s),
                fee: px("0.01"),
                maker: false,
                t_ns: 0,
            });
        }
        assert_eq!(o.remaining().to_string(), "4");
        assert_eq!(o.notional.to_string(), "1.72");
        assert_eq!(o.fees.to_string(), "0.02");
        assert_eq!(o.avg_price().unwrap().to_string(), "0.286666");
    }
}
