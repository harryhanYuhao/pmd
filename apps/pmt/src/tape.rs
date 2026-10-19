//! Per-market trade tape: live CLOB `last_trade_price` events, plus history loaded from the Data
//! API on request (a market's trades from before we subscribed).

use std::collections::HashSet;
use std::sync::Arc;

use pm_core::wire::clob_ws::LastTradeMsg;
use pm_core::{Fixed6, Price, Side, Size, TokenId};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    Live,
    History,
}

/// One taker trade.
#[derive(Clone, Debug, PartialEq)]
pub struct TapeTrade {
    /// Trade time, Unix ns (ms resolution live, s resolution from history).
    pub t_ns: i64,
    pub token: TokenId,
    pub price: Price,
    pub size: Size,
    /// Taker side.
    pub side: Side,
    pub tx: Option<Arc<str>>,
    pub source: Source,
}

impl TapeTrade {
    pub fn live(m: &LastTradeMsg) -> TapeTrade {
        TapeTrade {
            t_ns: m.timestamp as i64 * 1_000_000,
            token: m.asset_id.clone(),
            price: m.price,
            size: m.size,
            side: m.side,
            tx: m.transaction_hash.as_deref().filter(|h| !h.is_empty()).map(Arc::from),
            source: Source::Live,
        }
    }

    /// USDC traded.
    pub fn notional(&self) -> Fixed6 {
        self.price.mul_fixed(self.size)
    }

    /// The price this trade implies for `token` of the same binary market.
    pub fn price_of(&self, token: &TokenId) -> Price {
        if self.token == *token {
            self.price
        } else {
            self.price.complement()
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub enum History {
    #[default]
    NotLoaded,
    Loading,
    Loaded {
        at_ns: i64,
        /// More trades exist than were loaded; the oldest are missing.
        truncated: bool,
    },
    Failed(String),
}

#[derive(Clone, Debug, Default)]
pub struct Tape {
    /// Sorted by time.
    trades: Vec<TapeTrade>,
    pub history: History,
}

impl Tape {
    pub fn push_live(&mut self, t: TapeTrade) {
        let at = self.trades.partition_point(|x| x.t_ns <= t.t_ns);
        self.trades.insert(at, t);
    }

    /// Replaces the history part of the tape. History rows that are already on the tape from the
    /// live feed (same transaction and token) are dropped; when live trades carry no transaction
    /// hash, history newer than one second before the first live trade is dropped instead.
    pub fn set_history(&mut self, history: Vec<TapeTrade>, truncated: bool, now_ns: i64) {
        self.trades.retain(|t| t.source == Source::Live);
        let live: HashSet<(&str, &str)> = self
            .trades
            .iter()
            .filter_map(|t| Some((t.tx.as_deref()?, t.token.as_str())))
            .collect();
        let hashed = self.trades.iter().all(|t| t.tx.is_some());
        let cutoff = self.trades.first().map_or(i64::MAX, |t| t.t_ns - 1_000_000_000);
        let keep: Vec<TapeTrade> = history
            .into_iter()
            .filter(|h| {
                let dup = h.tx.as_deref().is_some_and(|tx| live.contains(&(tx, h.token.as_str())));
                !dup && (hashed || h.t_ns < cutoff)
            })
            .collect();
        self.trades.extend(keep);
        // History has 1 s resolution: at equal times the live trade is taken as the later one.
        self.trades.sort_by_key(|t| (t.t_ns, t.source == Source::Live));
        self.history = History::Loaded {
            at_ns: now_ns,
            truncated,
        };
    }

    pub fn trades(&self) -> &[TapeTrade] {
        &self.trades
    }

    pub fn last(&self) -> Option<&TapeTrade> {
        self.trades.last()
    }

    /// Trades strictly before `t_ns`.
    pub fn before(&self, t_ns: i64) -> &[TapeTrade] {
        &self.trades[..self.trades.partition_point(|t| t.t_ns < t_ns)]
    }

    /// USDC traded strictly before `t_ns`.
    pub fn notional_before(&self, t_ns: i64) -> Fixed6 {
        self.before(t_ns)
            .iter()
            .map(|t| t.notional())
            .fold(Fixed6::ZERO, |a, b| a + b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trade(t_s: i64, token: &str, price: &str, size: &str, tx: Option<&str>, source: Source) -> TapeTrade {
        TapeTrade {
            t_ns: t_s * 1_000_000_000,
            token: token.into(),
            price: price.parse().unwrap(),
            size: size.parse().unwrap(),
            side: Side::Buy,
            tx: tx.map(Arc::from),
            source,
        }
    }

    #[test]
    fn history_merges_without_double_counting() {
        let mut tape = Tape::default();
        tape.push_live(trade(100, "a", "0.5", "10", Some("0x2"), Source::Live));
        let hist = vec![
            trade(50, "a", "0.4", "10", Some("0x1"), Source::History),
            trade(100, "a", "0.5", "10", Some("0x2"), Source::History), // also seen live
            trade(100, "b", "0.5", "4", Some("0x2"), Source::History),  // same tx, other token
        ];
        tape.set_history(hist.clone(), false, 1);
        assert_eq!(tape.trades().len(), 3);
        assert_eq!(tape.notional_before(100 * 1_000_000_000).to_string(), "4");
        // Loading again replaces the history part.
        tape.set_history(hist, true, 2);
        assert_eq!(tape.trades().len(), 3);
        assert_eq!(
            tape.history,
            History::Loaded {
                at_ns: 2,
                truncated: true
            }
        );
        assert_eq!(tape.last().unwrap().source, Source::Live);
    }

    #[test]
    fn without_hashes_history_stops_before_the_live_feed() {
        let mut tape = Tape::default();
        tape.push_live(trade(100, "a", "0.5", "10", None, Source::Live));
        tape.set_history(
            vec![
                trade(98, "a", "0.4", "1", Some("0x1"), Source::History),
                trade(100, "a", "0.5", "10", Some("0x2"), Source::History),
            ],
            false,
            1,
        );
        assert_eq!(tape.trades().len(), 2);
        assert_eq!(tape.before(100 * 1_000_000_000).len(), 1);
    }

    #[test]
    fn implied_prices() {
        let t = trade(1, "a", "0.2", "1", None, Source::Live);
        assert_eq!(t.price_of(&"a".into()).to_string(), "0.2");
        assert_eq!(t.price_of(&"b".into()).to_string(), "0.8");
    }
}
