//! Normalised market metadata, independent of which API it came from.

use serde::{Deserialize, Serialize};

use crate::fixed::{Fixed6, Price, Size};
use crate::ids::{ConditionId, TokenId};

/// One binary market (two outcome tokens) as tracked by the recorder and the trader.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MarketInfo {
    pub condition_id: ConditionId,
    pub market_id: String,
    pub slug: String,
    pub question: String,
    /// Our league key (`cs2`, `dota2`, `lol`, …) from the recorder config.
    pub league: String,
    pub event_id: String,
    pub event_slug: String,
    /// Gamma `sportsMarketType` (`moneyline`, `child_moneyline`, `totals`, …), if any.
    pub market_type: Option<String>,
    /// Outcome names, aligned with `token_ids`.
    pub outcomes: Vec<String>,
    pub token_ids: Vec<TokenId>,
    /// Scheduled start, RFC 3339 / Gamma format as given.
    pub game_start_time: Option<String>,
    /// Gamma `gameId`, joins to the sports live-score feed.
    pub game_id: Option<i64>,
    pub tick_size: Option<Price>,
    pub min_order_size: Option<f64>,
    pub neg_risk: bool,
    pub active: bool,
    pub closed: bool,
    pub accepting_orders: bool,
}

impl MarketInfo {
    /// Index of a token among this market's outcomes.
    pub fn outcome_index(&self, token: &str) -> Option<usize> {
        self.token_ids.iter().position(|t| t.as_str() == token)
    }
}

/// Taker fee parameters of a market (Gamma `feeSchedule`, e.g.
/// `{"exponent": 1, "rate": 0.03, "takerOnly": true, "rebateRate": 0.25}`).
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeeSchedule {
    pub rate: f64,
    #[serde(default = "one")]
    pub exponent: f64,
    #[serde(default)]
    pub taker_only: bool,
    #[serde(default)]
    pub rebate_rate: f64,
}

fn one() -> f64 {
    1.0
}

impl FeeSchedule {
    /// USDC fee of a taker trade: `size × rate × (p (1 − p))^exponent`. With exponent 1 that is
    /// `rate × (1 − p)` of the stake (4% at 20¢ with rate 0.05).
    pub fn taker_fee(&self, price: Price, size: Size) -> Fixed6 {
        let p = price.to_f64();
        Fixed6::from_f64(size.to_f64() * self.rate * (p * (1.0 - p)).powf(self.exponent))
    }

    /// Fee of a maker fill (none when the schedule is taker-only).
    pub fn maker_fee(&self, price: Price, size: Size) -> Fixed6 {
        if self.taker_only {
            Fixed6::ZERO
        } else {
            self.taker_fee(price, size)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fees_scale_with_p_times_one_minus_p() {
        let f: FeeSchedule = serde_json::from_str(r#"{"exponent":1,"rate":0.05,"takerOnly":true}"#).unwrap();
        let (p, n): (Fixed6, Fixed6) = ("0.2".parse().unwrap(), "100".parse().unwrap());
        // $20 stake, 4% fee.
        assert_eq!(f.taker_fee(p, n).to_string(), "0.8");
        assert_eq!(f.maker_fee(p, n), Fixed6::ZERO);
    }
}
