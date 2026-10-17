//! Polymarket core: exact decimals, identifiers, feed wire formats, market metadata and order
//! books. Pure data and logic, no I/O, so it can be shared by the recorder, offline tools and
//! the live trader.

pub mod book;
pub mod fixed;
pub mod ids;
pub mod market;
pub mod wire;

pub use book::{BookSet, Fill, OrderBook};
pub use fixed::{Fixed6, Price, Size};
pub use ids::{ConditionId, Side, TokenId};
pub use market::MarketInfo;

/// Wall-clock time in Unix nanoseconds.
pub fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}
