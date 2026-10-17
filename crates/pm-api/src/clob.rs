//! Public (unauthenticated) CLOB REST endpoints (`https://clob.polymarket.com`).
//!
//! Authenticated order placement belongs in a separate execution crate that builds on this one.

use anyhow::Result;
use pm_core::wire::clob_ws::Level;
use pm_core::{ConditionId, Price, TokenId};
use serde::Deserialize;

use crate::http::Http;

pub const CLOB_URL: &str = "https://clob.polymarket.com";

/// `GET /book` response (same shape as the websocket `book` event, minus `event_type`).
#[derive(Clone, Debug, Deserialize)]
pub struct BookSummary {
    pub market: ConditionId,
    pub asset_id: TokenId,
    #[serde(default)]
    pub timestamp: Option<String>,
    #[serde(default)]
    pub hash: Option<String>,
    #[serde(default)]
    pub bids: Vec<Level>,
    #[serde(default)]
    pub asks: Vec<Level>,
    #[serde(default)]
    pub tick_size: Option<Price>,
    #[serde(default)]
    pub min_order_size: Option<Price>,
    #[serde(default)]
    pub neg_risk: Option<bool>,
}

#[derive(Clone, Default)]
pub struct Clob {
    http: Http,
}

impl Clob {
    pub fn new(http: Http) -> Self {
        Clob { http }
    }

    pub async fn book(&self, token: &str) -> Result<BookSummary> {
        self.http.get_json(&format!("{CLOB_URL}/book"), &[("token_id", token.to_string())]).await
    }

    /// Server time, Unix seconds.
    pub async fn server_time(&self) -> Result<i64> {
        self.http.get_json(&format!("{CLOB_URL}/time"), &[]).await
    }
}
