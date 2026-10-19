//! Limits every order must pass before it reaches the venue, whatever the strategy.

use pm_core::{Fixed6, Side};
use serde::Deserialize;

use crate::market::Market;
use crate::order::{Order, OrderRequest};
use crate::portfolio::Portfolio;

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RiskConfig {
    /// Largest single order, USDC at the limit price.
    pub max_order_usdc: f64,
    /// New buys per UTC day across all strategies, filled and resting.
    pub max_daily_usdc: f64,
    /// Unresolved positions plus resting buys, across all strategies.
    pub max_open_usdc: f64,
    /// Stop sending orders (reject everything); for a quick halt without editing strategies.
    pub halt: bool,
}

impl Default for RiskConfig {
    fn default() -> Self {
        RiskConfig {
            max_order_usdc: 10.0,
            max_daily_usdc: 100.0,
            max_open_usdc: 300.0,
            halt: false,
        }
    }
}

pub struct Risk {
    max_order: Fixed6,
    max_daily: Fixed6,
    max_open: Fixed6,
    halt: bool,
}

impl Risk {
    pub fn new(cfg: &RiskConfig) -> Risk {
        Risk {
            max_order: Fixed6::from_f64(cfg.max_order_usdc),
            max_daily: Fixed6::from_f64(cfg.max_daily_usdc),
            max_open: Fixed6::from_f64(cfg.max_open_usdc),
            halt: cfg.halt,
        }
    }

    /// `Err(reason)` if the order must not be sent. `open` are the orders still working.
    pub fn check<'a>(
        &self,
        req: &OrderRequest,
        market: Option<&Market>,
        portfolio: &Portfolio,
        open: impl Iterator<Item = &'a Order>,
        strategy: &std::sync::Arc<str>,
        now_ns: i64,
    ) -> Result<(), String> {
        let open: Vec<&Order> = open.collect();
        if self.halt {
            return Err("trading is halted ([risk] halt = true)".into());
        }
        let Some(m) = market else {
            return Err("unknown market".into());
        };
        if !m.tradable() {
            return Err("market is not accepting orders".into());
        }
        if !m.info.token_ids.contains(&req.token) {
            return Err("token is not in this market".into());
        }
        if req.price <= Fixed6::ZERO || req.price >= Fixed6::ONE {
            return Err(format!("price {} is outside (0, 1)", req.price));
        }
        if req.price.floor_to(m.tick()) != req.price {
            return Err(format!("price {} is not on the {} tick", req.price, m.tick()));
        }
        if req.size < m.min_size() {
            return Err(format!("{} shares is below the minimum of {}", req.size, m.min_size()));
        }
        if req.side == Side::Sell {
            let held = portfolio
                .position(strategy, &req.market, &req.token)
                .map_or(Fixed6::ZERO, |p| p.shares);
            // Shares already offered by working sell orders are not available again.
            let offered: Fixed6 = open
                .iter()
                .filter(|o| o.strategy == *strategy && o.req.side == Side::Sell && o.req.token == req.token)
                .map(|o| o.remaining())
                .fold(Fixed6::ZERO, |a, b| a + b);
            return if req.size + offered <= held {
                Ok(())
            } else {
                Err(format!(
                    "selling {} shares but holding {held} ({offered} already offered)",
                    req.size
                ))
            };
        }
        let n = req.notional();
        if n > self.max_order {
            return Err(format!("order ${n} is above max_order_usdc ${}", self.max_order));
        }
        let resting: Fixed6 = open
            .iter()
            .filter(|o| o.req.side == Side::Buy)
            .map(|o| o.req.price.mul_fixed(o.remaining()))
            .fold(Fixed6::ZERO, |a, b| a + b);
        let day = portfolio.spent_today(now_ns) + resting + n;
        if day > self.max_daily {
            return Err(format!(
                "${day} today would be above max_daily_usdc ${}",
                self.max_daily
            ));
        }
        let exposure = portfolio.open_cost() + resting + n;
        if exposure > self.max_open {
            return Err(format!(
                "${exposure} open would be above max_open_usdc ${}",
                self.max_open
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::MarketKind;
    use crate::order::{Fill, OrderId, Tif};
    use crate::portfolio::FillAt;
    use crate::strategy::testing::{market, px};
    use std::sync::Arc;

    fn risk() -> Risk {
        Risk::new(&RiskConfig {
            max_order_usdc: 10.0,
            max_daily_usdc: 20.0,
            max_open_usdc: 30.0,
            halt: false,
        })
    }

    fn buy(price: &str, size: &str) -> OrderRequest {
        OrderRequest::buy(&"m".into(), &"m-a".into(), px(price), px(size), Tif::Fak, "t".into())
    }

    fn sell(size: &str) -> OrderRequest {
        OrderRequest {
            side: Side::Sell,
            ..buy("0.5", size)
        }
    }

    fn check(r: &Risk, req: &OrderRequest, pf: &Portfolio, open: &[Order]) -> Result<(), String> {
        let m = market("m", MarketKind::Map(1), "cs2", 0);
        r.check(req, Some(&m), pf, open.iter(), &Arc::from("s"), 0)
    }

    fn bought(pf: &mut Portfolio, strategy: &str, market: &str, usdc: &str) {
        let (s, m, t): (Arc<str>, pm_core::ConditionId, pm_core::TokenId) =
            (strategy.into(), market.into(), format!("{market}-a").into());
        let at = FillAt {
            strategy: &s,
            market: &m,
            token: &t,
            side: Side::Buy,
            event_slug: "ev",
            league: "cs2",
        };
        let fill = Fill {
            price: px("0.5"),
            size: px(usdc) + px(usdc),
            fee: Fixed6::ZERO,
            maker: false,
            t_ns: 0,
        };
        pf.apply_fill(&at, &fill);
    }

    #[test]
    fn order_shape_checks() {
        let (r, pf) = (risk(), Portfolio::default());
        assert!(check(&r, &buy("0.5", "10"), &pf, &[]).is_ok());
        let err = |req: OrderRequest| check(&r, &req, &pf, &[]).unwrap_err();
        assert!(err(buy("0", "10")).contains("outside"));
        assert!(err(buy("1", "10")).contains("outside"));
        assert!(err(buy("0.505", "10")).contains("tick"));
        assert!(err(buy("0.5", "4.99")).contains("minimum"));
        let wrong_token = OrderRequest {
            token: "x".into(),
            ..buy("0.5", "10")
        };
        assert!(err(wrong_token).contains("token"));
        let m = market("m", MarketKind::Map(1), "cs2", 0);
        let none = r.check(&buy("0.5", "10"), None, &pf, [].iter(), &Arc::from("s"), 0);
        assert!(none.unwrap_err().contains("unknown market"));
        let closed = crate::market::Market { winner: Some(0), ..m };
        let res = r.check(&buy("0.5", "10"), Some(&closed), &pf, [].iter(), &Arc::from("s"), 0);
        assert!(res.unwrap_err().contains("not accepting"));
        let halted = Risk::new(&RiskConfig {
            halt: true,
            ..Default::default()
        });
        assert!(
            check(&halted, &buy("0.5", "10"), &pf, &[])
                .unwrap_err()
                .contains("halted")
        );
    }

    #[test]
    fn money_limits_count_positions_and_resting_buys() {
        let r = risk();
        let mut pf = Portfolio::default();
        assert!(
            check(&r, &buy("0.5", "21"), &pf, &[])
                .unwrap_err()
                .contains("max_order")
        );
        assert!(check(&r, &buy("0.5", "20"), &pf, &[]).is_ok(), "exactly $10");

        // $12 spent today; a resting bid of $5 more: $17 used of the $20 daily limit.
        bought(&mut pf, "s", "m", "12");
        let mut resting = Order::new(OrderId(1), "s".into(), buy("0.5", "10"), 0);
        resting.status = crate::order::OrderStatus::Open;
        let open = [resting];
        assert!(check(&r, &buy("0.5", "6"), &pf, &open).is_ok(), "$3");
        assert!(
            check(&r, &buy("0.5", "8"), &pf, &open)
                .unwrap_err()
                .contains("max_daily")
        );

        // Open exposure counts every strategy's positions, also those bought on earlier days.
        let mut old = Portfolio::default();
        for (s, m) in [("a", "x"), ("b", "y"), ("c", "z")] {
            bought(&mut old, s, m, "9.5");
        }
        let m = market("m", MarketKind::Map(1), "cs2", 0);
        let two_days = 2 * 86_400_000_000_000;
        let later = r.check(&buy("0.5", "6"), Some(&m), &old, [].iter(), &Arc::from("s"), two_days);
        assert!(
            later.unwrap_err().contains("max_open"),
            "28.5 + 3 > 30 (the daily budget is fresh)"
        );
        let later = r.check(&buy("0.1", "10"), Some(&m), &old, [].iter(), &Arc::from("s"), two_days);
        assert!(later.is_ok(), "28.5 + 1");
    }

    #[test]
    fn sells_need_the_shares_net_of_working_sells() {
        let r = risk();
        let mut pf = Portfolio::default();
        assert!(check(&r, &sell("5"), &pf, &[]).unwrap_err().contains("holding 0"));
        bought(&mut pf, "s", "m", "5"); // 10 shares
        assert!(check(&r, &sell("10"), &pf, &[]).is_ok());
        let mut working = Order::new(OrderId(1), "s".into(), sell("6"), 0);
        working.status = crate::order::OrderStatus::Open;
        let open = [working];
        assert!(
            check(&r, &sell("5"), &pf, &open)
                .unwrap_err()
                .contains("6 already offered")
        );
        assert!(check(&r, &sell("5"), &pf, &[]).is_ok());
        // Sells are not limited by the buy budgets.
        let tight = Risk::new(&RiskConfig {
            max_order_usdc: 1.0,
            max_daily_usdc: 0.0,
            max_open_usdc: 0.0,
            halt: false,
        });
        assert!(check(&tight, &sell("10"), &pf, &[]).is_ok());
    }
}
