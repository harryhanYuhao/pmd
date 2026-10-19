//! Positions per strategy. A position closes when its market resolves (the winning token pays 1)
//! or, partly or wholly, when it is sold: each sell realises its P&L against the average cost.

use std::collections::BTreeMap;
use std::sync::Arc;

use pm_core::{ConditionId, Fixed6, Side, Size, TokenId};
use serde::{Deserialize, Serialize};

use crate::order::Fill;

const DAY_NS: i64 = 86_400_000_000_000;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Position {
    pub shares: Size,
    /// USDC paid, fees included.
    pub cost: Fixed6,
    pub fees: Fixed6,
    /// Gamma event slug, to look the market up after its event leaves the open listings.
    pub event_slug: String,
    pub league: String,
}

/// A position (or part of one) closed: by its market's resolution, or by a sell (`exit`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Settlement {
    pub strategy: Arc<str>,
    pub market: ConditionId,
    pub token: TokenId,
    pub won: bool,
    pub shares: Size,
    pub cost: Fixed6,
    pub payout: Fixed6,
    pub pnl: Fixed6,
    pub t_ns: i64,
    /// Closed by selling (`payout` = proceeds after fees, `won` = sold above cost). Replaying the
    /// sell fill recreates it, so [`Portfolio::record_settlement`] ignores it.
    #[serde(default)]
    pub exit: bool,
}

/// Per-strategy totals.
#[derive(Clone, Debug, Default)]
pub struct Totals {
    pub open: usize,
    pub open_cost: Fixed6,
    pub settled: usize,
    pub won: usize,
    pub staked: Fixed6,
    pub pnl: Fixed6,
}

type Key = (Arc<str>, ConditionId, TokenId);

#[derive(Default)]
pub struct Portfolio {
    positions: BTreeMap<Key, Position>,
    settled: BTreeMap<Arc<str>, Totals>,
    /// (UTC day, USDC spent on buys that day).
    spent_today: (i64, Fixed6),
}

/// Where a fill happened.
pub struct FillAt<'a> {
    pub strategy: &'a Arc<str>,
    pub market: &'a ConditionId,
    pub token: &'a TokenId,
    pub side: Side,
    pub event_slug: &'a str,
    pub league: &'a str,
}

impl Portfolio {
    /// Applies a fill. A sell returns the part of the position it closed, with its P&L.
    pub fn apply_fill(&mut self, at: &FillAt<'_>, f: &Fill) -> Option<Settlement> {
        let key = (at.strategy.clone(), at.market.clone(), at.token.clone());
        let value = f.price.mul_fixed(f.size);
        if at.side == Side::Buy {
            let p = self.positions.entry(key).or_insert_with(|| Position {
                event_slug: at.event_slug.to_string(),
                league: at.league.to_string(),
                ..Default::default()
            });
            p.shares += f.size;
            p.cost += value + f.fee;
            p.fees += f.fee;
            let day = f.t_ns.div_euclid(DAY_NS);
            if self.spent_today.0 != day {
                self.spent_today = (day, Fixed6::ZERO);
            }
            self.spent_today.1 += value + f.fee;
            return None;
        }
        // Risk checks never let a strategy sell more than it holds; if it happens anyway, only
        // the shares held count.
        let p = self.positions.get_mut(&key)?;
        let size = f.size.min(p.shares);
        let released = p.cost.mul_fixed(size).div_fixed(p.shares).unwrap_or(p.cost);
        p.shares -= size;
        p.cost -= released;
        p.fees += f.fee;
        if p.shares.is_zero() {
            self.positions.remove(&key);
        }
        let proceeds = f.price.mul_fixed(size) - f.fee;
        let exit = Settlement {
            strategy: key.0,
            market: key.1,
            token: key.2,
            won: proceeds > released,
            shares: size,
            cost: released,
            payout: proceeds,
            pnl: proceeds - released,
            t_ns: f.t_ns,
            exit: true,
        };
        self.add_totals(&exit);
        Some(exit)
    }

    /// Settles every position in `market`: the winning token pays 1 per share.
    pub fn settle(&mut self, market: &ConditionId, winner: &TokenId, now_ns: i64) -> Vec<Settlement> {
        let keys: Vec<Key> = self.positions.keys().filter(|k| k.1 == *market).cloned().collect();
        let mut out = Vec::new();
        for key in keys {
            let p = self.positions.remove(&key).expect("listed");
            let won = key.2 == *winner;
            let payout = if won { p.shares } else { Fixed6::ZERO };
            let s = Settlement {
                strategy: key.0,
                market: key.1,
                token: key.2,
                won,
                shares: p.shares,
                cost: p.cost,
                payout,
                pnl: payout - p.cost,
                t_ns: now_ns,
                exit: false,
            };
            self.record_settlement(&s);
            out.push(s);
        }
        out
    }

    /// Applies a journaled resolution (restoring from the journal): removes the position and adds
    /// to the totals. Exits are ignored; replaying their sell fill already accounts for them.
    pub fn record_settlement(&mut self, s: &Settlement) {
        if s.exit {
            return;
        }
        self.positions
            .remove(&(s.strategy.clone(), s.market.clone(), s.token.clone()));
        self.add_totals(s);
    }

    fn add_totals(&mut self, s: &Settlement) {
        let t = self.settled.entry(s.strategy.clone()).or_default();
        t.settled += 1;
        t.won += s.won as usize;
        t.staked += s.cost;
        t.pnl += s.pnl;
    }

    pub fn position(&self, strategy: &Arc<str>, market: &ConditionId, token: &TokenId) -> Option<&Position> {
        self.positions.get(&(strategy.clone(), market.clone(), token.clone()))
    }

    pub fn positions(&self) -> impl Iterator<Item = (&Key, &Position)> {
        self.positions.iter()
    }

    /// `strategy` holds a position in `market` (any outcome).
    pub fn strategy_holds(&self, strategy: &str, market: &str) -> bool {
        self.positions
            .keys()
            .any(|k| &*k.0 == strategy && k.1.as_str() == market)
    }

    pub fn holds(&self, market: &str) -> bool {
        self.positions.keys().any(|k| k.1.as_str() == market)
    }

    /// USDC in unresolved positions.
    pub fn open_cost(&self) -> Fixed6 {
        self.positions.values().map(|p| p.cost).fold(Fixed6::ZERO, |a, b| a + b)
    }

    /// USDC spent on buys during the UTC day of `now_ns`.
    pub fn spent_today(&self, now_ns: i64) -> Fixed6 {
        if self.spent_today.0 == now_ns.div_euclid(DAY_NS) {
            self.spent_today.1
        } else {
            Fixed6::ZERO
        }
    }

    pub fn totals(&self, strategy: &str) -> Totals {
        let mut t = self.settled.get(strategy).cloned().unwrap_or_default();
        for (k, p) in &self.positions {
            if &*k.0 == strategy {
                t.open += 1;
                t.open_cost += p.cost;
            }
        }
        t
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn px(s: &str) -> Fixed6 {
        s.parse().unwrap()
    }

    fn fill(p: &str, s: &str, fee: &str, t_ns: i64) -> Fill {
        Fill {
            price: px(p),
            size: px(s),
            fee: px(fee),
            maker: false,
            t_ns,
        }
    }

    #[test]
    fn positions_settle_into_pnl() {
        let (s, m): (Arc<str>, ConditionId) = ("c1".into(), "0xm".into());
        let (a, b): (TokenId, TokenId) = ("a".into(), "b".into());
        let mut pf = Portfolio::default();
        let mut buy = |token: &TokenId, f: Fill| {
            let at = FillAt {
                strategy: &s,
                market: &m,
                token,
                side: Side::Buy,
                event_slug: "ev",
                league: "cs2",
            };
            pf.apply_fill(&at, &f);
        };
        buy(&a, fill("0.2", "10", "0.08", 1));
        buy(&b, fill("0.7", "5", "0", 2));
        assert_eq!(pf.open_cost().to_string(), "5.58");
        assert_eq!(pf.spent_today(3).to_string(), "5.58");
        assert_eq!(pf.spent_today(DAY_NS + 3), Fixed6::ZERO, "a new day");
        assert!(pf.holds("0xm"));

        let st = pf.settle(&m, &a, 10);
        assert_eq!(st.len(), 2);
        let won: Vec<_> = st.iter().filter(|x| x.won).collect();
        assert_eq!(won[0].pnl.to_string(), "7.92"); // 10 - 2.08
        let t = pf.totals("c1");
        assert_eq!((t.open, t.settled, t.won), (0, 2, 1));
        assert_eq!(t.pnl.to_string(), "4.42"); // 7.92 - 3.5
        assert!(!pf.holds("0xm"));
    }
}
