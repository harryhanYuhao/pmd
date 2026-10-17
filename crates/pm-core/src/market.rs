//! Normalised market metadata, independent of which API it came from.

use serde::{Deserialize, Serialize};

use crate::fixed::Price;
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
