//! Data API (`https://data-api.polymarket.com`): historical trades with wallets.
//!
//! Server-side limits (measured 2026-09): offset ≤ 10,000 and ≤ 10,000 rows per request, so at
//! most 20,000 of the newest trades per market are reachable; throughput is throttled to
//! ~1.4 req/s regardless of client concurrency.

use anyhow::Result;
use pm_core::{Price, Side, Size};
use serde::Deserialize;

use crate::http::Http;

pub const DATA_API_URL: &str = "https://data-api.polymarket.com";

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Trade {
    pub proxy_wallet: String,
    pub side: Side,
    pub asset: String,
    pub condition_id: String,
    pub size: Size,
    pub price: Price,
    pub timestamp: i64,
    #[serde(default)]
    pub outcome: String,
    #[serde(default)]
    pub outcome_index: Option<u32>,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub transaction_hash: String,
}

#[derive(Clone, Default)]
pub struct DataApi {
    http: Http,
}

impl DataApi {
    pub fn new(http: Http) -> Self {
        DataApi { http }
    }

    /// One page of taker trades for a market, newest first.
    pub async fn trades(&self, condition_id: &str, limit: usize, offset: usize) -> Result<Vec<Trade>> {
        self.http
            .get_json(
                &format!("{DATA_API_URL}/trades"),
                &[
                    ("market", condition_id.to_string()),
                    ("limit", limit.to_string()),
                    ("offset", offset.to_string()),
                    ("takerOnly", "true".into()),
                ],
            )
            .await
    }
}
