//! Paper trading: fills simulated against what the live feed shows.
//!
//! - The marketable part of an order takes the opposite side of the current book, level by level,
//!   never past the limit, paying the market's taker fee. The book is not consumed, so two orders
//!   in quick succession can both take the same liquidity.
//! - The resting part fills when a taker trade on the same token prints at or through our price
//!   (a seller at or below our bid), up to that trade's size, at our price, with no fee. This
//!   ignores queue position, so it is optimistic for busy price levels; the strategies here run in
//!   thin markets.

use std::collections::BTreeMap;

use pm_core::{FeeSchedule, Fixed6, Price, Side, Size, TokenId};

use super::{ExecReport, Venue};
use crate::order::{Fill, Order, OrderId, OrderStatus, Tif};
use crate::state::MarketState;
use crate::tape::TapeTrade;

struct Resting {
    token: TokenId,
    side: Side,
    price: Price,
    remaining: Size,
    expires_ns: Option<i64>,
    fees: Option<FeeSchedule>,
}

#[derive(Default)]
pub struct Paper {
    resting: BTreeMap<OrderId, Resting>,
}

impl Venue for Paper {
    fn name(&self) -> &'static str {
        "paper"
    }

    fn submit(&mut self, o: &Order, md: &MarketState, now_ns: i64) -> Vec<ExecReport> {
        let r = &o.req;
        let fees = md.market(r.market.as_str()).and_then(|m| m.fees);
        let mut out = Vec::new();
        let book = md.book(r.token.as_str());
        if book.is_none() && !r.tif.rests() {
            return vec![ExecReport::done(o.id, OrderStatus::Rejected, "no order book yet")];
        }
        // Opposite side, best first, within the limit.
        let levels: Vec<(Price, Size)> = book
            .map(|b| b.top(r.side.opposite(), usize::MAX))
            .unwrap_or_default()
            .into_iter()
            .take_while(|(p, _)| match r.side {
                Side::Buy => *p <= r.price,
                Side::Sell => *p >= r.price,
            })
            .collect();
        let available: Size = levels.iter().map(|l| l.1).fold(Fixed6::ZERO, |a, b| a + b);
        if r.tif == Tif::Fok && available < r.size {
            return vec![ExecReport::done(
                o.id,
                OrderStatus::Cancelled,
                format!("fill or kill: only {available} shares at or better than {}", r.price),
            )];
        }
        let mut left = r.size;
        for (p, s) in levels {
            if left.is_zero() {
                break;
            }
            let take = s.min(left);
            left -= take;
            let fee = fees.map_or(Fixed6::ZERO, |f| f.taker_fee(p, take));
            out.push(ExecReport::Fill {
                id: o.id,
                fill: Fill {
                    price: p,
                    size: take,
                    fee,
                    maker: false,
                    t_ns: now_ns,
                },
            });
        }
        if left.is_zero() {
            out.push(ExecReport::Done {
                id: o.id,
                status: OrderStatus::Filled,
                reason: None,
            });
        } else if r.tif.rests() {
            let expires_ns = match r.tif {
                Tif::Gtd { expires_ns } => Some(expires_ns),
                _ => None,
            };
            self.resting.insert(
                o.id,
                Resting {
                    token: r.token.clone(),
                    side: r.side,
                    price: r.price,
                    remaining: left,
                    expires_ns,
                    fees,
                },
            );
        } else {
            let why = format!("no more liquidity at or better than {}", r.price);
            out.push(ExecReport::done(o.id, OrderStatus::Cancelled, why));
        }
        out
    }

    fn cancel(&mut self, id: OrderId, _now_ns: i64) -> Vec<ExecReport> {
        match self.resting.remove(&id) {
            Some(_) => vec![ExecReport::done(id, OrderStatus::Cancelled, "cancelled")],
            None => vec![],
        }
    }

    fn on_trade(&mut self, t: &TapeTrade, _md: &MarketState) -> Vec<ExecReport> {
        let mut out = Vec::new();
        let mut left = t.size;
        let mut done = Vec::new();
        for (id, r) in self.resting.iter_mut() {
            if left.is_zero() {
                break;
            }
            let crosses = r.token == t.token
                && t.side == r.side.opposite()
                && match r.side {
                    Side::Buy => t.price <= r.price,
                    Side::Sell => t.price >= r.price,
                };
            if !crosses {
                continue;
            }
            let take = r.remaining.min(left);
            left -= take;
            r.remaining -= take;
            let fee = r.fees.map_or(Fixed6::ZERO, |f| f.maker_fee(r.price, take));
            out.push(ExecReport::Fill {
                id: *id,
                fill: Fill {
                    price: r.price,
                    size: take,
                    fee,
                    maker: true,
                    t_ns: t.t_ns,
                },
            });
            if r.remaining.is_zero() {
                done.push(*id);
            }
        }
        for id in done {
            self.resting.remove(&id);
            out.push(ExecReport::Done {
                id,
                status: OrderStatus::Filled,
                reason: None,
            });
        }
        out
    }

    fn on_clock(&mut self, now_ns: i64) -> Vec<ExecReport> {
        let expired: Vec<OrderId> = self
            .resting
            .iter()
            .filter(|(_, r)| r.expires_ns.is_some_and(|e| e <= now_ns))
            .map(|(id, _)| *id)
            .collect();
        expired
            .into_iter()
            .map(|id| {
                self.resting.remove(&id);
                ExecReport::done(id, OrderStatus::Expired, "expired")
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::{Market, MarketKind};
    use crate::order::OrderRequest;
    use crate::tape::Source;
    use pm_core::MarketInfo;

    fn px(s: &str) -> Fixed6 {
        s.parse().unwrap()
    }

    fn state() -> MarketState {
        let mut md = MarketState::default();
        md.upsert(Market {
            info: MarketInfo {
                condition_id: "m".into(),
                market_id: "1".into(),
                slug: "s".into(),
                question: "q".into(),
                league: "cs2".into(),
                event_id: "e".into(),
                event_slug: "ev".into(),
                market_type: Some("child_moneyline".into()),
                outcomes: vec!["A".into(), "B".into()],
                token_ids: vec!["a".into(), "b".into()],
                game_start_time: None,
                game_id: None,
                tick_size: Some(px("0.01")),
                min_order_size: Some(5.0),
                neg_risk: false,
                active: true,
                closed: false,
                accepting_orders: true,
            },
            kind: MarketKind::Map(1),
            event_title: "A vs B".into(),
            start_ns: None,
            fees: Some(FeeSchedule {
                rate: 0.05,
                exponent: 1.0,
                taker_only: true,
                rebate_rate: 0.0,
            }),
            winner: None,
        });
        let snap = r#"{"event_type":"book","market":"m","asset_id":"a","timestamp":"1",
            "bids":[{"price":"0.18","size":"100"}],
            "asks":[{"price":"0.20","size":"10"},{"price":"0.21","size":"10"},{"price":"0.30","size":"100"}]}"#;
        md.books.apply(&serde_json::from_str(snap).unwrap());
        md
    }

    fn order(id: u64, price: &str, size: &str, tif: Tif) -> Order {
        let req = OrderRequest::buy(&"m".into(), &"a".into(), px(price), px(size), tif, "test".into());
        Order::new(OrderId(id), "s".into(), req, 0)
    }

    fn fills(r: &[ExecReport]) -> Vec<(String, String, bool)> {
        r.iter()
            .filter_map(|r| match r {
                ExecReport::Fill { fill, .. } => Some((fill.price.to_string(), fill.size.to_string(), fill.maker)),
                _ => None,
            })
            .collect()
    }

    fn status(r: &[ExecReport]) -> Option<OrderStatus> {
        r.iter().find_map(|r| match r {
            ExecReport::Done { status, .. } => Some(*status),
            _ => None,
        })
    }

    #[test]
    fn fill_and_kill_walks_the_book_up_to_the_limit() {
        let md = state();
        let mut v = Paper::default();
        let r = v.submit(&order(1, "0.22", "25", Tif::Fak), &md, 5);
        assert_eq!(
            fills(&r),
            [("0.2".into(), "10".into(), false), ("0.21".into(), "10".into(), false)]
        );
        assert_eq!(status(&r), Some(OrderStatus::Cancelled), "5 shares killed");
        // Taker fee: 10 × 0.05 × 0.2 × 0.8.
        let ExecReport::Fill { fill, .. } = &r[0] else { panic!() };
        assert_eq!(fill.fee.to_string(), "0.08");

        let all = v.submit(&order(2, "0.22", "15", Tif::Fak), &md, 5);
        assert_eq!(status(&all), Some(OrderStatus::Filled));
        let fok = v.submit(&order(3, "0.22", "25", Tif::Fok), &md, 5);
        assert!(fills(&fok).is_empty());
        assert_eq!(status(&fok), Some(OrderStatus::Cancelled));
    }

    #[test]
    fn resting_orders_fill_from_sellers_and_expire() {
        let md = state();
        let mut v = Paper::default();
        assert!(
            v.submit(&order(1, "0.19", "30", Tif::Gtd { expires_ns: 100 }), &md, 5)
                .is_empty()
        );
        let sell = |price: &str, size: &str| TapeTrade {
            t_ns: 50,
            token: "a".into(),
            price: px(price),
            size: px(size),
            side: Side::Sell,
            tx: None,
            source: Source::Live,
        };
        assert!(v.on_trade(&sell("0.20", "50"), &md).is_empty(), "above our bid");
        let r = v.on_trade(&sell("0.19", "20"), &md);
        assert_eq!(fills(&r), [("0.19".into(), "20".into(), true)]);
        assert_eq!(status(&r), None);
        assert!(v.on_clock(99).is_empty());
        assert_eq!(status(&v.on_clock(100)), Some(OrderStatus::Expired));
        assert!(v.cancel(OrderId(1), 101).is_empty(), "already gone");
    }

    fn sell_order(id: u64, price: &str, size: &str, tif: Tif) -> Order {
        let mut o = order(id, price, size, tif);
        o.req.side = Side::Sell;
        o
    }

    fn print(token: &str, side: Side, price: &str, size: &str) -> TapeTrade {
        TapeTrade {
            t_ns: 50,
            token: token.into(),
            price: px(price),
            size: px(size),
            side,
            tx: None,
            source: Source::Live,
        }
    }

    #[test]
    fn sells_take_the_bids() {
        let md = state();
        let mut v = Paper::default();
        let r = v.submit(&sell_order(1, "0.15", "10", Tif::Fak), &md, 5);
        assert_eq!(fills(&r), [("0.18".into(), "10".into(), false)]);
        assert_eq!(status(&r), Some(OrderStatus::Filled));
        let r = v.submit(&sell_order(2, "0.19", "10", Tif::Fak), &md, 5);
        assert!(fills(&r).is_empty(), "no bid at 0.19 or better");
        assert_eq!(status(&r), Some(OrderStatus::Cancelled));
    }

    #[test]
    fn a_marketable_resting_order_takes_first_then_rests() {
        let md = state();
        let mut v = Paper::default();
        let r = v.submit(&order(1, "0.21", "30", Tif::Gtc), &md, 5);
        assert_eq!(fills(&r).len(), 2, "10 at 0.20 and 10 at 0.21");
        assert_eq!(status(&r), None, "10 shares rest");
        let r = v.on_trade(&print("a", Side::Sell, "0.21", "4"), &md);
        assert_eq!(fills(&r), [("0.21".into(), "4".into(), true)]);
        let r = v.on_trade(&print("a", Side::Sell, "0.1", "100"), &md);
        assert_eq!(
            fills(&r),
            [("0.21".into(), "6".into(), true)],
            "at our price, not the print's"
        );
        assert_eq!(status(&r), Some(OrderStatus::Filled));
    }

    #[test]
    fn resting_orders_share_a_print_oldest_first() {
        let md = state();
        let mut v = Paper::default();
        v.submit(&order(1, "0.19", "10", Tif::Gtc), &md, 5);
        v.submit(&order(2, "0.19", "10", Tif::Gtc), &md, 5);
        v.submit(&sell_order(3, "0.5", "10", Tif::Gtc), &md, 5);
        // A buyer on the other token, or a buyer of ours below our ask, fills nothing.
        assert!(v.on_trade(&print("b", Side::Sell, "0.1", "50"), &md).is_empty());
        assert!(v.on_trade(&print("a", Side::Buy, "0.45", "50"), &md).is_empty());
        let r = v.on_trade(&print("a", Side::Sell, "0.19", "15"), &md);
        let by_order: Vec<(OrderId, String)> = r
            .iter()
            .filter_map(|r| match r {
                ExecReport::Fill { id, fill } => Some((*id, fill.size.to_string())),
                _ => None,
            })
            .collect();
        assert_eq!(by_order, [(OrderId(1), "10".into()), (OrderId(2), "5".into())]);
        let r = v.on_trade(&print("a", Side::Buy, "0.55", "20"), &md);
        assert_eq!(fills(&r), [("0.5".into(), "10".into(), true)], "the resting sell");
    }

    #[test]
    fn without_a_book_takers_are_rejected_and_bids_rest() {
        let md = MarketState::default();
        let mut v = Paper::default();
        let r = v.submit(&order(1, "0.2", "10", Tif::Fak), &md, 5);
        assert_eq!(status(&r), Some(OrderStatus::Rejected));
        assert!(v.submit(&order(2, "0.2", "10", Tif::Gtc), &md, 5).is_empty(), "rests");
        assert_eq!(status(&v.cancel(OrderId(2), 6)), Some(OrderStatus::Cancelled));
    }
}
