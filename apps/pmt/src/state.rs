//! What the trader knows about the markets: metadata, order books and trade tapes. Owned by the
//! engine; strategies and the paper venue read it.

use std::collections::HashMap;

use pm_core::book::{BookSet, OrderBook};
use pm_core::{ConditionId, Price, TokenId};

use crate::market::Market;
use crate::tape::Tape;

#[derive(Debug, PartialEq, Eq)]
pub enum Change {
    New,
    Changed,
    Same,
}

#[derive(Default)]
pub struct MarketState {
    markets: HashMap<ConditionId, Market>,
    pub books: BookSet,
    tapes: HashMap<ConditionId, Tape>,
    token_market: HashMap<TokenId, ConditionId>,
}

impl MarketState {
    pub fn market(&self, c: &str) -> Option<&Market> {
        self.markets.get(c)
    }

    pub fn markets(&self) -> impl Iterator<Item = &Market> {
        self.markets.values()
    }

    pub fn market_of(&self, token: &str) -> Option<&Market> {
        self.markets.get(self.token_market.get(token)?)
    }

    pub fn book(&self, token: &str) -> Option<&OrderBook> {
        self.books.get(token).filter(|b| b.synced)
    }

    pub fn tape(&self, c: &str) -> Option<&Tape> {
        self.tapes.get(c)
    }

    pub fn tape_mut(&mut self, c: &ConditionId) -> &mut Tape {
        self.tapes.entry(c.clone()).or_default()
    }

    /// Price of the last trade in the token's market, as a price of this token.
    pub fn last_price(&self, token: &TokenId) -> Option<Price> {
        let c = self.token_market.get(token.as_str())?;
        Some(self.tapes.get(c)?.last()?.price_of(token))
    }

    /// The price Polymarket displays for a token (midpoint, or last trade if the spread is wide).
    pub fn display_price(&self, token: &TokenId) -> Option<Price> {
        let last = self.last_price(token);
        match self.book(token.as_str()) {
            Some(b) => b.display_price(last),
            None => last,
        }
    }

    /// Adds or updates a market. A known winner is kept when the update has none: the market
    /// channel's `market_resolved` is often ahead of Gamma.
    pub fn upsert(&mut self, mut m: Market) -> Change {
        if m.winner.is_none() {
            m.winner = self.markets.get(&m.info.condition_id).and_then(|old| old.winner);
        }
        let change = match self.markets.get(&m.info.condition_id) {
            None => Change::New,
            Some(old) if *old == m => return Change::Same,
            Some(_) => Change::Changed,
        };
        for t in &m.info.token_ids {
            self.token_market.insert(t.clone(), m.info.condition_id.clone());
        }
        self.markets.insert(m.info.condition_id.clone(), m);
        change
    }

    pub fn set_winner(&mut self, c: &str, winner: usize) -> bool {
        match self.markets.get_mut(c) {
            Some(m) if m.winner != Some(winner) => {
                m.winner = Some(winner);
                true
            }
            _ => false,
        }
    }

    pub fn remove(&mut self, c: &str) -> Option<Market> {
        let m = self.markets.remove(c)?;
        for t in &m.info.token_ids {
            self.token_market.remove(t.as_str());
            self.books.remove(t.as_str());
        }
        self.tapes.remove(c);
        Some(m)
    }

    /// Forgets a market's books (after unsubscribing: they would go stale).
    pub fn drop_books(&mut self, c: &str) {
        if let Some(m) = self.markets.get(c) {
            for t in &m.info.token_ids {
                self.books.remove(t.as_str());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::MarketKind;
    use crate::strategy::testing::{market, px};
    use crate::tape::{Source, TapeTrade};
    use pm_core::Side;

    #[test]
    fn upserts_report_changes_and_keep_a_known_winner() {
        let mut md = MarketState::default();
        let m = market("m", MarketKind::Map(1), "cs2", 0);
        assert_eq!(md.upsert(m.clone()), Change::New);
        assert_eq!(md.upsert(m.clone()), Change::Same);
        let later = Market {
            start_ns: Some(5),
            ..m.clone()
        };
        assert_eq!(md.upsert(later), Change::Changed);
        assert!(md.set_winner("m", 1));
        assert!(!md.set_winner("m", 1), "already known");
        md.upsert(m.clone());
        assert_eq!(md.market("m").unwrap().winner, Some(1), "kept when Gamma lags");
        assert_eq!(md.market_of("m-b").unwrap().info.condition_id.as_str(), "m");
        assert!(md.remove("m").is_some());
        assert!(md.market_of("m-b").is_none() && md.tape("m").is_none());
    }

    #[test]
    fn prices_from_books_and_trades() {
        let mut md = MarketState::default();
        md.upsert(market("m", MarketKind::Map(1), "cs2", 0));
        let a: TokenId = "m-a".into();
        let b: TokenId = "m-b".into();
        assert_eq!(md.display_price(&a), None);
        md.tape_mut(&"m".into()).push_live(TapeTrade {
            t_ns: 1,
            token: a.clone(),
            price: px("0.3"),
            size: px("1"),
            side: Side::Buy,
            tx: None,
            source: Source::Live,
        });
        assert_eq!(md.last_price(&b), Some(px("0.7")), "the other outcome");
        assert_eq!(md.display_price(&a), Some(px("0.3")), "no book: last trade");
        let snap = r#"{"event_type":"book","market":"m","asset_id":"m-a","timestamp":"1","bids":[{"price":"0.32","size":"5"}],"asks":[{"price":"0.36","size":"5"}]}"#;
        md.books.apply(&serde_json::from_str(snap).unwrap());
        assert_eq!(md.display_price(&a), Some(px("0.34")));
        md.drop_books("m");
        assert!(md.book("m-a").is_none());
        assert_eq!(md.display_price(&a), Some(px("0.3")));
    }
}
