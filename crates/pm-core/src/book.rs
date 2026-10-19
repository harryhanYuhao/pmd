//! L2 order book for one outcome token, maintained from `book` snapshots and `price_change`
//! deltas. Shared by the recorder (integrity checks, periodic snapshots) and live trading.

use std::collections::{BTreeMap, HashMap};

use crate::fixed::{Fixed6, Price, Size};
use crate::ids::{Side, TokenId};
use crate::wire::clob_ws::{BookMsg, MarketMsg};

#[derive(Clone, Debug, Default)]
pub struct OrderBook {
    pub asks: BTreeMap<Price, Size>,
    pub bids: BTreeMap<Price, Size>,
    /// Server timestamp (ms) of the last applied event.
    pub ts_ms: u64,
    /// Set once a full snapshot has been applied; deltas before that are ignored.
    pub synced: bool,
    /// The server's top of book showed a level we do not have; only a new snapshot fixes that.
    pub needs_resync: bool,
    pub tick_size: Option<Price>,
}

/// Result of a simulated market order walking the book.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Fill {
    pub size: Size,
    pub notional: Fixed6,
    /// Worst price touched.
    pub last_price: Option<Price>,
}

impl Fill {
    fn add(&mut self, price: Price, size: Size) {
        self.size += size;
        self.notional += price.mul_fixed(size);
        self.last_price = Some(price);
    }

    pub fn vwap(&self) -> Option<f64> {
        (!self.size.is_zero()).then(|| self.notional.to_f64() / self.size.to_f64())
    }
}

impl OrderBook {
    pub fn apply_snapshot(&mut self, b: &BookMsg) {
        self.bids = b
            .bids
            .iter()
            .filter(|l| !l.size.is_zero())
            .map(|l| (l.price, l.size))
            .collect();
        self.asks = b
            .asks
            .iter()
            .filter(|l| !l.size.is_zero())
            .map(|l| (l.price, l.size))
            .collect();
        self.ts_ms = b.timestamp;
        self.synced = true;
        self.needs_resync = false;
    }

    /// Reconciles with the server-reported top of book (`None` = side empty). Levels better than
    /// the server's best cannot exist and are removed (a snapshot that raced a cancel leaves
    /// them behind). Returns `(pruned_any, consistent)`; inconsistent means a level is missing.
    pub fn reconcile(&mut self, best_bid: Option<Price>, best_ask: Option<Price>) -> (bool, bool) {
        let mut pruned = false;
        while let Some((p, _)) = self.best_bid() {
            if best_bid.is_none_or(|b| p > b) {
                self.bids.remove(&p);
                pruned = true;
            } else {
                break;
            }
        }
        while let Some((p, _)) = self.best_ask() {
            if best_ask.is_none_or(|a| p < a) {
                self.asks.remove(&p);
                pruned = true;
            } else {
                break;
            }
        }
        let ok = self.best_bid().map(|x| x.0) == best_bid && self.best_ask().map(|x| x.0) == best_ask;
        if !ok {
            self.needs_resync = true;
        }
        (pruned, ok)
    }

    /// Sets the absolute size at a level (`Buy` = bid side). Size 0 removes the level.
    pub fn set_level(&mut self, side: Side, price: Price, size: Size, ts_ms: u64) {
        let book = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        if size.is_zero() {
            book.remove(&price);
        } else {
            book.insert(price, size);
        }
        self.ts_ms = self.ts_ms.max(ts_ms);
    }

    pub fn best_bid(&self) -> Option<(Price, Size)> {
        self.bids.iter().next_back().map(|(p, s)| (*p, *s))
    }

    pub fn best_ask(&self) -> Option<(Price, Size)> {
        self.asks.iter().next().map(|(p, s)| (*p, *s))
    }

    pub fn mid(&self) -> Option<f64> {
        Some((self.best_bid()?.0.to_f64() + self.best_ask()?.0.to_f64()) / 2.0)
    }

    pub fn spread(&self) -> Option<Fixed6> {
        Some(self.best_ask()?.0 - self.best_bid()?.0)
    }

    /// The price Polymarket displays: the midpoint, unless the spread is wider than 10¢ (or a side
    /// is empty), in which case the last trade price.
    pub fn display_price(&self, last_trade: Option<Price>) -> Option<Price> {
        match (self.best_bid(), self.best_ask()) {
            (Some((b, _)), Some((a, _))) if a - b <= Fixed6::from_raw(100_000) => Some(Fixed6((a.0 + b.0) / 2)),
            _ => last_trade,
        }
    }

    /// Best `n` levels of one side, best first.
    pub fn top(&self, side: Side, n: usize) -> Vec<(Price, Size)> {
        match side {
            Side::Buy => self.bids.iter().rev().take(n).map(|(p, s)| (*p, *s)).collect(),
            Side::Sell => self.asks.iter().take(n).map(|(p, s)| (*p, *s)).collect(),
        }
    }

    /// Levels a taker `side` order can hit, best first, never past `limit` (a buy takes asks
    /// ≤ limit, a sell takes bids ≥ limit).
    fn takeable(&self, side: Side, limit: Option<Price>) -> impl Iterator<Item = (Price, Size)> + '_ {
        let levels: Box<dyn Iterator<Item = (&Price, &Size)>> = match side {
            Side::Buy => Box::new(self.asks.iter()),
            Side::Sell => Box::new(self.bids.iter().rev()),
        };
        levels
            .map(|(p, s)| (*p, *s))
            .take_while(move |(p, _)| match (side, limit) {
                (Side::Buy, Some(l)) => *p <= l,
                (Side::Sell, Some(l)) => *p >= l,
                (_, None) => true,
            })
    }

    /// Walks the opposite side as a taker `side` order would, up to `max_size` shares and never
    /// past `limit`.
    pub fn sweep(&self, side: Side, max_size: Size, limit: Option<Price>) -> Fill {
        let mut f = Fill::default();
        for (p, s) in self.takeable(side, limit) {
            if f.size >= max_size {
                break;
            }
            f.add(p, s.min(max_size - f.size));
        }
        f
    }

    /// Shares a taker `side` order could get for at most `budget` USDC, never past `limit`.
    pub fn sweep_notional(&self, side: Side, budget: Fixed6, limit: Option<Price>) -> Fill {
        let mut f = Fill::default();
        for (p, s) in self.takeable(side, limit) {
            let left = budget - f.notional;
            if left <= Fixed6::ZERO || p.is_zero() {
                break;
            }
            // Whole level if affordable, else the shares `left` buys at this price.
            let take = if p.mul_fixed(s) <= left {
                s
            } else {
                Fixed6((left.raw() as i128 * crate::fixed::SCALE as i128 / p.raw() as i128) as i64)
            };
            f.add(p, take);
        }
        f
    }
}

/// Outcome of applying one websocket message to a [`BookSet`].
#[derive(Debug, Default)]
pub struct ApplyReport {
    /// Tokens whose book changed.
    pub touched: Vec<TokenId>,
    /// Tokens where stale levels were removed to match the server's top of book.
    pub pruned: Vec<TokenId>,
    /// Tokens whose book is missing a level the server reports; they need a fresh snapshot.
    pub missing: Vec<TokenId>,
}

/// The server reports an empty bid side as best bid `0` and an empty ask side as best ask `1`.
fn hint(p: Option<Price>, empty: Price) -> Option<Option<Price>> {
    p.map(|p| if p == empty { None } else { Some(p) })
}

/// Books for many tokens, keyed by token id.
#[derive(Debug, Default)]
pub struct BookSet {
    pub books: HashMap<TokenId, OrderBook>,
}

impl BookSet {
    pub fn get(&self, t: &str) -> Option<&OrderBook> {
        self.books.get(t)
    }

    pub fn remove(&mut self, t: &str) {
        self.books.remove(t);
    }

    pub fn apply(&mut self, msg: &MarketMsg) -> ApplyReport {
        let mut r = ApplyReport::default();
        match msg {
            MarketMsg::Book(b) => {
                self.books.entry(b.asset_id.clone()).or_default().apply_snapshot(b);
                r.touched.push(b.asset_id.clone());
            }
            MarketMsg::PriceChange(pc) => {
                for c in &pc.price_changes {
                    let Some(book) = self.books.get_mut(c.asset_id.as_str()) else {
                        continue;
                    };
                    if !book.synced {
                        continue;
                    }
                    book.set_level(c.side, c.price, c.size, pc.timestamp);
                    if let (Some(bb), Some(ba)) = (hint(c.best_bid, Fixed6::ZERO), hint(c.best_ask, Fixed6::ONE))
                        && (book.best_bid().map(|x| x.0) != bb || book.best_ask().map(|x| x.0) != ba)
                    {
                        let (pruned, ok) = book.reconcile(bb, ba);
                        if pruned {
                            r.pruned.push(c.asset_id.clone());
                        }
                        if !ok {
                            r.missing.push(c.asset_id.clone());
                        }
                    }
                    if r.touched.last() != Some(&c.asset_id) {
                        r.touched.push(c.asset_id.clone());
                    }
                }
            }
            MarketMsg::TickSizeChange(t) => {
                if let Some(book) = self.books.get_mut(t.asset_id.as_str()) {
                    book.tick_size = Some(t.new_tick_size);
                }
            }
            _ => {}
        }
        r
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::clob_ws::Level;

    fn px(s: &str) -> Fixed6 {
        s.parse().unwrap()
    }

    fn book() -> OrderBook {
        let mut b = OrderBook::default();
        b.apply_snapshot(&BookMsg {
            market: "m".into(),
            asset_id: "a".into(),
            timestamp: 1,
            hash: None,
            bids: vec![
                Level {
                    price: px("0.40"),
                    size: px("100"),
                },
                Level {
                    price: px("0.41"),
                    size: px("10"),
                },
            ],
            asks: vec![
                Level {
                    price: px("0.43"),
                    size: px("50"),
                },
                Level {
                    price: px("0.45"),
                    size: px("200"),
                },
            ],
        });
        b
    }

    #[test]
    fn top_and_sweep() {
        let mut b = book();
        assert_eq!(b.best_bid().unwrap().0, px("0.41"));
        assert_eq!(b.best_ask().unwrap().0, px("0.43"));
        let f = b.sweep(Side::Buy, px("100"), None);
        assert_eq!(f.size, px("100"));
        assert_eq!(f.notional, px("44")); // 50*0.43 + 50*0.45
        assert_eq!(b.sweep(Side::Buy, px("100"), Some(px("0.44"))).size, px("50"));
        let g = b.sweep_notional(Side::Buy, px("30.1"), None);
        assert_eq!(g.size, px("69.111111")); // 50 for 21.5, then 8.6 / 0.45 = 19.111111
        assert_eq!(g.notional, px("30.1"));
        b.set_level(Side::Sell, px("0.43"), Fixed6::ZERO, 2);
        assert_eq!(b.best_ask().unwrap().0, px("0.45"));
    }

    #[test]
    fn display_price_is_the_mid_unless_the_spread_is_wide() {
        let mut b = book();
        assert_eq!(b.display_price(Some(px("0.9"))), Some(px("0.42")));
        b.set_level(Side::Sell, px("0.43"), Fixed6::ZERO, 2);
        b.set_level(Side::Sell, px("0.45"), Fixed6::ZERO, 2);
        b.set_level(Side::Sell, px("0.60"), px("1"), 2);
        assert_eq!(b.display_price(Some(px("0.5"))), Some(px("0.5")), "spread 19c");
        assert_eq!(OrderBook::default().display_price(None), None);
    }

    #[test]
    fn reconcile_prunes_stale_and_flags_missing() {
        let mut b = book();
        // Server says best bid 0.40: our 0.41 is stale.
        assert_eq!(b.reconcile(Some(px("0.40")), Some(px("0.43"))), (true, true));
        assert_eq!(b.best_bid().unwrap().0, px("0.40"));
        assert!(!b.needs_resync);
        // Server says best ask 0.42: we lack that level.
        assert_eq!(b.reconcile(Some(px("0.40")), Some(px("0.42"))), (false, false));
        assert!(b.needs_resync);
    }

    #[test]
    fn empty_side_sentinels_are_not_mismatches() {
        let mut set = BookSet::default();
        let snap = r#"{"event_type":"book","market":"m","asset_id":"a","timestamp":"1","bids":[{"price":"0.3","size":"5"}],"asks":[]}"#;
        set.apply(&serde_json::from_str(snap).unwrap());
        let pc = r#"{"event_type":"price_change","market":"m","timestamp":"2","price_changes":[{"asset_id":"a","price":"0.2","size":"1","side":"BUY","best_bid":"0.3","best_ask":"1"}]}"#;
        let r = set.apply(&serde_json::from_str(pc).unwrap());
        assert!(r.missing.is_empty() && r.pruned.is_empty());
    }
}
