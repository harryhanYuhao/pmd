//! Gamma API (`https://gamma-api.polymarket.com`): events and markets metadata.
//!
//! Events are fetched as raw JSON (so they can be stored verbatim) and parsed into the typed
//! [`GammaEvent`] on demand. Gamma encodes `outcomes`, `clobTokenIds` and `outcomePrices` as
//! JSON strings inside JSON; the accessors decode them.

use anyhow::Result;
use chrono::{DateTime, Utc};
use pm_core::{FeeSchedule, Fixed6, MarketInfo, TokenId};
use serde::Deserialize;
use serde_json::Value;

use crate::http::Http;

pub const GAMMA_URL: &str = "https://gamma-api.polymarket.com";
/// Gamma's maximum page size for `/events`.
const PAGE: usize = 100;
/// Safety stop for pagination (100k events; all of Polymarket's open events are ~22k).
const MAX_PAGES: usize = 1000;

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
    /// Short label within the event: `Match Winner`, `Map 1 Winner`, `O/U 2.5 Games`, …
    #[serde(deserialize_with = "lenient")]
    pub group_item_title: Option<String>,
    pub game_start_time: Option<String>,
    pub active: bool,
    pub closed: bool,
    pub accepting_orders: Option<bool>,
    pub enable_order_book: Option<bool>,
    pub order_price_min_tick_size: Option<f64>,
    pub order_min_size: Option<f64>,
    pub neg_risk: Option<bool>,
    #[serde(deserialize_with = "lenient")]
    pub fees_enabled: Option<bool>,
    #[serde(deserialize_with = "lenient")]
    pub fee_schedule: Option<FeeSchedule>,
    pub updated_at: Option<String>,
}

impl GammaEvent {
    /// Scheduled start of the match, if Gamma gives one.
    pub fn start(&self) -> Option<DateTime<Utc>> {
        parse_time(self.start_time.as_deref()?)
    }

    /// A scheduled match (as opposed to a future, outright or season prop).
    pub fn is_game(&self) -> bool {
        self.game_id.is_some() || self.start().is_some()
    }
}

/// `None` instead of an error when a value has an unexpected shape, so that a change in a field
/// only the trader reads can never make a whole event unparseable.
fn lenient<'de, D: serde::Deserializer<'de>, T: serde::de::DeserializeOwned>(d: D) -> Result<Option<T>, D::Error> {
    Ok(Option::<Value>::deserialize(d)?.and_then(|v| serde_json::from_value(v).ok()))
}

/// Gamma times are RFC 3339, but sometimes with a space for `T` and an offset like `+00`.
pub fn parse_time(s: &str) -> Option<DateTime<Utc>> {
    let s = s.replace(' ', "T");
    DateTime::parse_from_rfc3339(&s)
        .or_else(|_| DateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%#z"))
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

fn json_list(s: &Option<String>) -> Vec<String> {
    s.as_deref()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default()
}

impl GammaMarket {
    pub fn outcomes(&self) -> Vec<String> {
        json_list(&self.outcomes)
    }

    pub fn token_ids(&self) -> Vec<TokenId> {
        json_list(&self.clob_token_ids).into_iter().map(TokenId::from).collect()
    }

    pub fn outcome_prices(&self) -> Vec<f64> {
        json_list(&self.outcome_prices)
            .iter()
            .filter_map(|p| p.parse().ok())
            .collect()
    }

    /// Open for trading on the CLOB right now.
    pub fn is_tradable(&self) -> bool {
        self.active
            && !self.closed
            && self.enable_order_book.unwrap_or(false)
            && self.accepting_orders.unwrap_or(true)
            && self.token_ids().len() == 2
    }

    /// Index of the winning outcome once the market is closed (its outcome price is 1).
    pub fn winner(&self) -> Option<usize> {
        if !self.closed {
            return None;
        }
        let p = self.outcome_prices();
        (p.len() == 2).then(|| p.iter().position(|&x| x > 0.99)).flatten()
    }

    /// Taker fees, if the market charges them.
    pub fn fees(&self) -> Option<FeeSchedule> {
        self.fee_schedule.filter(|_| self.fees_enabled.unwrap_or(true))
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
    /// Tag by slug (`esports`, `politics`, …), instead of `tag_id`.
    pub tag_slug: Option<String>,
    pub active: Option<bool>,
    /// Gamma series (one per league season; sports leagues' match events live here).
    pub series_id: Option<u64>,
    pub closed: Option<bool>,
    /// RFC 3339; events whose `endDate` is at or after this.
    pub end_date_min: Option<String>,
    pub slug: Option<String>,
    /// `false` leaves out the events' markets: ~14x less data.
    pub include_markets: Option<bool>,
}

impl EventsQuery {
    fn params(&self) -> Vec<(&'static str, String)> {
        let mut q = vec![];
        if let Some(t) = self.tag_id {
            q.push(("tag_id", t.to_string()));
        }
        if let Some(s) = self.series_id {
            q.push(("series_id", s.to_string()));
        }
        if let Some(t) = &self.tag_slug {
            q.push(("tag_slug", t.clone()));
        }
        if let Some(a) = self.active {
            q.push(("active", a.to_string()));
        }
        if let Some(m) = self.include_markets {
            q.push(("include_markets", m.to_string()));
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

    /// All events matching the query, as raw JSON values. Uses cursor pagination
    /// (`/events/keyset`): `/events` rejects offsets above 2000 ("offset too large").
    pub async fn events_raw(&self, q: &EventsQuery) -> Result<Vec<Value>> {
        #[derive(Deserialize)]
        struct Page {
            #[serde(default)]
            events: Vec<Value>,
            #[serde(default)]
            next_cursor: Option<String>,
        }
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..MAX_PAGES {
            let mut params = q.params();
            params.push(("limit", PAGE.to_string()));
            if let Some(c) = &cursor {
                params.push(("after_cursor", c.clone()));
            }
            let page: Page = self
                .http
                .get_json(&format!("{GAMMA_URL}/events/keyset"), &params)
                .await?;
            let n = page.events.len();
            out.extend(page.events);
            cursor = page.next_cursor.filter(|c| !c.is_empty());
            if n == 0 || cursor.is_none() {
                break;
            }
        }
        Ok(out)
    }

    /// Open events of one league (by tag and/or series, merged by event id) whose `endDate` is at
    /// or after `end_date_min` (RFC 3339).
    pub async fn league_events(
        &self,
        tag_id: Option<u64>,
        series_id: Option<u64>,
        end_date_min: &str,
    ) -> Result<Vec<Value>> {
        let queries = [
            tag_id.map(|t| EventsQuery {
                tag_id: Some(t),
                ..Default::default()
            }),
            series_id.map(|s| EventsQuery {
                series_id: Some(s),
                ..Default::default()
            }),
        ];
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::new();
        for q in queries.into_iter().flatten() {
            let q = EventsQuery {
                closed: Some(false),
                end_date_min: Some(end_date_min.to_string()),
                ..q
            };
            for v in self.events_raw(&q).await? {
                let id = v.get("id").map(|i| i.to_string()).unwrap_or_default();
                if seen.insert(id) {
                    out.push(v);
                }
            }
        }
        Ok(out)
    }

    pub async fn events(&self, q: &EventsQuery) -> Result<Vec<GammaEvent>> {
        Ok(self
            .events_raw(q)
            .await?
            .into_iter()
            .filter_map(|v| serde_json::from_value(v).ok())
            .collect())
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
                "orderPriceMinTickSize": 0.001, "volume": "123.4", "groupItemTitle": "Match Winner",
                "feesEnabled": true,
                "feeSchedule": {"exponent": 1, "rate": 0.03, "takerOnly": true, "rebateRate": 0.25}
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
        assert_eq!(m.group_item_title.as_deref(), Some("Match Winner"));
        assert_eq!(m.fees().unwrap().rate, 0.03);
        assert_eq!(m.winner(), None, "open");
    }

    #[test]
    fn odd_fee_fields_do_not_break_an_event() {
        let v = serde_json::json!({
            "id": "1", "markets": [{"conditionId": "0xab", "feeSchedule": {"rate": "0.03"},
                "feesEnabled": "yes", "groupItemTitle": 5}]
        });
        let e: GammaEvent = serde_json::from_value(v).unwrap();
        let m = &e.markets[0];
        assert_eq!(m.condition_id, "0xab");
        assert!(m.fee_schedule.is_none() && m.fees_enabled.is_none() && m.group_item_title.is_none());
    }

    #[test]
    fn winner_of_a_closed_market() {
        let m = |closed: bool, prices: &str| GammaMarket {
            closed,
            outcome_prices: Some(prices.into()),
            ..Default::default()
        };
        assert_eq!(m(true, "[\"0\", \"1\"]").winner(), Some(1));
        assert_eq!(m(true, "[\"1\", \"0\"]").winner(), Some(0));
        assert_eq!(m(true, "[\"0.5\", \"0.5\"]").winner(), None, "not resolved yet");
        assert_eq!(m(false, "[\"1\", \"0\"]").winner(), None);
    }

    #[test]
    fn query_parameters() {
        let q = EventsQuery {
            tag_id: Some(100780),
            tag_slug: Some("esports".into()),
            active: Some(true),
            closed: Some(false),
            include_markets: Some(false),
            ..Default::default()
        };
        let p: Vec<(&str, String)> = q.params();
        let get = |k: &str| p.iter().find(|(n, _)| *n == k).map(|(_, v)| v.as_str());
        assert_eq!(get("tag_id"), Some("100780"));
        assert_eq!(get("tag_slug"), Some("esports"));
        assert_eq!(get("active"), Some("true"));
        assert_eq!(get("closed"), Some("false"));
        assert_eq!(get("include_markets"), Some("false"));
        assert_eq!(get("series_id"), None);
        assert!(EventsQuery::default().params().is_empty(), "unset filters are not sent");
    }

    /// Walks well past the 2000-event offset cap of `/events` with the cursor endpoint.
    #[tokio::test]
    #[ignore = "network: pages through Gamma"]
    async fn keyset_pagination_goes_past_the_offset_cap() {
        let q = EventsQuery {
            active: Some(true),
            closed: Some(false),
            include_markets: Some(false),
            ..Default::default()
        };
        let events = Gamma::default().events_raw(&q).await.unwrap();
        assert!(events.len() > 2100, "only {} events", events.len());
        let ids: std::collections::HashSet<&str> = events.iter().filter_map(|e| e["id"].as_str()).collect();
        assert_eq!(ids.len(), events.len(), "no event twice");
    }

    #[test]
    fn parses_gamma_times() {
        for s in [
            "2026-10-17T14:00:00Z",
            "2026-10-17 14:00:00+00",
            "2026-10-17 14:00:00+00:00",
        ] {
            assert_eq!(parse_time(s).unwrap().to_rfc3339(), "2026-10-17T14:00:00+00:00", "{s}");
        }
        assert!(parse_time("").is_none());
    }
}
