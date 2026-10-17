//! Gamma API (`https://gamma-api.polymarket.com`): events and markets metadata.
//!
//! Events are fetched as raw JSON (so they can be stored verbatim) and parsed into the typed
//! [`GammaEvent`] on demand. Gamma encodes `outcomes`, `clobTokenIds` and `outcomePrices` as
//! JSON strings inside JSON; the accessors decode them.

use anyhow::Result;
use pm_core::{Fixed6, MarketInfo, TokenId};
use serde::Deserialize;
use serde_json::Value;

use crate::http::Http;

pub const GAMMA_URL: &str = "https://gamma-api.polymarket.com";
/// Gamma's maximum page size for `/events`.
const PAGE: usize = 100;

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct GammaEvent {
    pub id: String,
    pub slug: String,
    pub title: String,
    pub start_time: Option<String>,
    pub end_date: Option<String>,
    pub active: bool,
    pub closed: bool,
    pub live: bool,
    pub ended: bool,
    pub game_id: Option<i64>,
    pub score: Option<String>,
    pub period: Option<String>,
    pub updated_at: Option<String>,
    pub markets: Vec<GammaMarket>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct GammaMarket {
    pub id: String,
    pub condition_id: String,
    pub slug: String,
    pub question: String,
    pub outcomes: Option<String>,
    pub clob_token_ids: Option<String>,
    pub outcome_prices: Option<String>,
    pub sports_market_type: Option<String>,
    pub game_start_time: Option<String>,
    pub active: bool,
    pub closed: bool,
    pub accepting_orders: Option<bool>,
    pub enable_order_book: Option<bool>,
    pub order_price_min_tick_size: Option<f64>,
    pub order_min_size: Option<f64>,
    pub neg_risk: Option<bool>,
    pub updated_at: Option<String>,
}

fn json_list(s: &Option<String>) -> Vec<String> {
    s.as_deref().and_then(|s| serde_json::from_str(s).ok()).unwrap_or_default()
}

impl GammaMarket {
    pub fn outcomes(&self) -> Vec<String> {
        json_list(&self.outcomes)
    }

    pub fn token_ids(&self) -> Vec<TokenId> {
        json_list(&self.clob_token_ids).into_iter().map(TokenId::from).collect()
    }

    pub fn outcome_prices(&self) -> Vec<f64> {
        json_list(&self.outcome_prices).iter().filter_map(|p| p.parse().ok()).collect()
    }

    /// Open for trading on the CLOB right now.
    pub fn is_tradable(&self) -> bool {
        self.active
            && !self.closed
            && self.enable_order_book.unwrap_or(false)
            && self.accepting_orders.unwrap_or(true)
            && self.token_ids().len() == 2
    }

    pub fn to_info(&self, league: &str, ev: &GammaEvent) -> MarketInfo {
        MarketInfo {
            condition_id: self.condition_id.as_str().into(),
            market_id: self.id.clone(),
            slug: self.slug.clone(),
            question: self.question.clone(),
            league: league.to_string(),
            event_id: ev.id.clone(),
            event_slug: ev.slug.clone(),
            market_type: self.sports_market_type.clone(),
            outcomes: self.outcomes(),
            token_ids: self.token_ids(),
            game_start_time: self.game_start_time.clone().or_else(|| ev.start_time.clone()),
            game_id: ev.game_id,
            tick_size: self.order_price_min_tick_size.map(Fixed6::from_f64),
            min_order_size: self.order_min_size,
            neg_risk: self.neg_risk.unwrap_or(false),
            active: self.active,
            closed: self.closed,
            accepting_orders: self.accepting_orders.unwrap_or(true),
        }
    }
}

/// Filters for `GET /events`.
#[derive(Clone, Debug, Default)]
pub struct EventsQuery {
    pub tag_id: Option<u64>,
    pub closed: Option<bool>,
    /// RFC 3339; events whose `endDate` is at or after this.
    pub end_date_min: Option<String>,
    pub slug: Option<String>,
}

impl EventsQuery {
    fn params(&self) -> Vec<(&'static str, String)> {
        let mut q = vec![];
        if let Some(t) = self.tag_id {
            q.push(("tag_id", t.to_string()));
        }
        if let Some(c) = self.closed {
            q.push(("closed", c.to_string()));
        }
        if let Some(d) = &self.end_date_min {
            q.push(("end_date_min", d.clone()));
        }
        if let Some(s) = &self.slug {
            q.push(("slug", s.clone()));
        }
        q
    }
}

#[derive(Clone, Default)]
pub struct Gamma {
    http: Http,
}

impl Gamma {
    pub fn new(http: Http) -> Self {
        Gamma { http }
    }

    /// All events matching the query (paginated), as raw JSON values.
    pub async fn events_raw(&self, q: &EventsQuery) -> Result<Vec<Value>> {
        let mut out = Vec::new();
        let mut offset = 0;
        loop {
            let mut params = q.params();
            params.push(("limit", PAGE.to_string()));
            params.push(("offset", offset.to_string()));
            let page: Vec<Value> = self.http.get_json(&format!("{GAMMA_URL}/events"), &params).await?;
            let n = page.len();
            out.extend(page);
            if n < PAGE || offset > 50_000 {
                break;
            }
            offset += n;
        }
        Ok(out)
    }

    pub async fn events(&self, q: &EventsQuery) -> Result<Vec<GammaEvent>> {
        Ok(self.events_raw(q).await?.into_iter().filter_map(|v| serde_json::from_value(v).ok()).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_nested_json_strings() {
        let v = serde_json::json!({
            "id": "1", "slug": "cs2-a-b", "live": true, "gameId": 7,
            "markets": [{
                "id": "9", "conditionId": "0xab", "slug": "cs2-a-b", "question": "A vs B",
                "outcomes": "[\"A\", \"B\"]", "clobTokenIds": "[\"11\", \"22\"]",
                "outcomePrices": "[\"0.4\", \"0.6\"]", "sportsMarketType": "moneyline",
                "active": true, "closed": false, "acceptingOrders": true, "enableOrderBook": true,
                "orderPriceMinTickSize": 0.001, "volume": "123.4"
            }]
        });
        let e: GammaEvent = serde_json::from_value(v).unwrap();
        let m = &e.markets[0];
        assert!(m.is_tradable());
        assert_eq!(m.outcome_prices(), vec![0.4, 0.6]);
        let info = m.to_info("cs2", &e);
        assert_eq!(info.token_ids[1].as_str(), "22");
        assert_eq!(info.tick_size.unwrap().to_string(), "0.001");
        assert_eq!(info.game_id, Some(7));
    }
}
