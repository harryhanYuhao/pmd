//! CLOB `market` websocket channel (`wss://ws-subscriptions-clob.polymarket.com/ws/market`).
//!
//! Formats verified against the live feed (2026-10). A frame is either one JSON object or an
//! array of objects (the initial book dump after subscribing is an array).

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use super::de_u64;
use crate::fixed::{Price, Size};
use crate::ids::{ConditionId, Side, TokenId};

pub const MARKET_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum MarketMsg {
    /// Full snapshot of one token's book. Sent on subscribe and again after trades.
    Book(BookMsg),
    /// One or more level updates (new absolute size at a price; size 0 removes the level).
    PriceChange(PriceChangeMsg),
    LastTradePrice(LastTradeMsg),
    TickSizeChange(TickSizeChangeMsg),
    /// Only sent with `custom_feature_enabled: true`.
    BestBidAsk(BestBidAskMsg),
    /// Only sent with `custom_feature_enabled: true`.
    NewMarket(serde_json::Value),
    /// Only sent with `custom_feature_enabled: true`.
    MarketResolved(serde_json::Value),
    #[serde(other)]
    Unknown,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Level {
    pub price: Price,
    pub size: Size,
}

#[derive(Clone, Debug, Deserialize)]
pub struct BookMsg {
    pub market: ConditionId,
    pub asset_id: TokenId,
    #[serde(deserialize_with = "de_u64")]
    pub timestamp: u64,
    #[serde(default)]
    pub hash: Option<String>,
    #[serde(alias = "buys", default)]
    pub bids: Vec<Level>,
    #[serde(alias = "sells", default)]
    pub asks: Vec<Level>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PriceChangeMsg {
    pub market: ConditionId,
    #[serde(deserialize_with = "de_u64")]
    pub timestamp: u64,
    pub price_changes: Vec<PriceChange>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PriceChange {
    pub asset_id: TokenId,
    pub price: Price,
    /// New total size at this price level.
    pub size: Size,
    pub side: Side,
    #[serde(default)]
    pub hash: Option<String>,
    /// Top of book after this change, as reported by the server (used to verify our book).
    #[serde(default)]
    pub best_bid: Option<Price>,
    #[serde(default)]
    pub best_ask: Option<Price>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct LastTradeMsg {
    pub market: ConditionId,
    pub asset_id: TokenId,
    pub price: Price,
    pub size: Size,
    /// Taker side.
    pub side: Side,
    #[serde(default)]
    pub fee_rate_bps: Option<String>,
    #[serde(deserialize_with = "de_u64")]
    pub timestamp: u64,
    #[serde(default)]
    pub transaction_hash: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TickSizeChangeMsg {
    pub market: ConditionId,
    pub asset_id: TokenId,
    pub old_tick_size: Price,
    pub new_tick_size: Price,
    #[serde(deserialize_with = "de_u64")]
    pub timestamp: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct BestBidAskMsg {
    pub market: ConditionId,
    pub asset_id: TokenId,
    #[serde(default)]
    pub best_bid: Option<Price>,
    #[serde(default)]
    pub best_ask: Option<Price>,
    #[serde(deserialize_with = "de_u64")]
    pub timestamp: u64,
}

impl MarketMsg {
    pub fn market(&self) -> Option<&ConditionId> {
        match self {
            MarketMsg::Book(m) => Some(&m.market),
            MarketMsg::PriceChange(m) => Some(&m.market),
            MarketMsg::LastTradePrice(m) => Some(&m.market),
            MarketMsg::TickSizeChange(m) => Some(&m.market),
            MarketMsg::BestBidAsk(m) => Some(&m.market),
            _ => None,
        }
    }
}

/// Splits a frame into its JSON objects without re-encoding them, so the exact server bytes
/// can be stored. Returns `None` for non-JSON frames such as `PONG`.
pub fn split_frame(text: &str) -> Option<Vec<&RawValue>> {
    let t = text.trim_start();
    if t.starts_with('[') {
        serde_json::from_str::<Vec<&RawValue>>(text).ok()
    } else if t.starts_with('{') {
        serde_json::from_str::<&RawValue>(text).ok().map(|v| vec![v])
    } else {
        None
    }
}

/// Removes `"hash":"…"` fields. They are opaque per-update digests that cannot be compressed
/// (≈2/3 of the compressed size of the `price_change` stream) and are not needed to rebuild books.
pub fn strip_hashes(raw: &str) -> std::borrow::Cow<'_, str> {
    const KEY: &str = "\"hash\":\"";
    if !raw.contains(KEY) {
        return std::borrow::Cow::Borrowed(raw);
    }
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(i) = rest.find(KEY) {
        let Some(close) = rest[i + KEY.len()..].find('"') else { break };
        let mut end = i + KEY.len() + close + 1;
        let mut start = i;
        if rest[end..].starts_with(',') {
            end += 1;
        } else if rest[..start].ends_with(',') {
            // Last field of an object: drop the preceding comma instead.
            start -= 1;
        } else if start == 0 && out.ends_with(',') {
            out.pop();
        }
        out.push_str(&rest[..start]);
        rest = &rest[end..];
    }
    out.push_str(rest);
    std::borrow::Cow::Owned(out)
}

/// Initial subscription message for a fresh connection.
pub fn subscribe_msg(assets: &[&str], custom_features: bool) -> String {
    serde_json::json!({
        "assets_ids": assets,
        "type": "market",
        "custom_feature_enabled": custom_features,
    })
    .to_string()
}

/// Add or remove assets on an open connection.
pub fn update_subscription_msg(assets: &[&str], subscribe: bool) -> String {
    serde_json::json!({
        "assets_ids": assets,
        "operation": if subscribe { "subscribe" } else { "unsubscribe" },
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_live_samples() {
        let pc = r#"{"market":"0x10","price_changes":[{"asset_id":"327","price":"0.68","size":"55.03","side":"BUY","hash":"cd","best_bid":"0.68","best_ask":"0.69"}],"timestamp":"1792166227750","event_type":"price_change"}"#;
        let m: MarketMsg = serde_json::from_str(pc).unwrap();
        let MarketMsg::PriceChange(p) = m else { panic!() };
        assert_eq!(p.timestamp, 1792166227750);
        assert_eq!(p.price_changes[0].side, Side::Buy);

        let lt = r#"{"market":"0x10","asset_id":"327","price":"0.68","size":"452.04","fee_rate_bps":"0","side":"SELL","timestamp":"1792166227766","event_type":"last_trade_price","transaction_hash":"0x5f"}"#;
        assert!(matches!(serde_json::from_str::<MarketMsg>(lt).unwrap(), MarketMsg::LastTradePrice(_)));

        let bk = r#"[{"market":"0xb3","asset_id":"919","timestamp":"1792166227567","hash":"ed","bids":[{"price":"0.01","size":"22.35"}],"asks":[],"event_type":"book"}]"#;
        let parts = split_frame(bk).unwrap();
        assert_eq!(parts.len(), 1);
        let MarketMsg::Book(b) = serde_json::from_str(parts[0].get()).unwrap() else { panic!() };
        assert_eq!(b.bids.len(), 1);

        let other = r#"{"event_type":"something_new","market":"0x1"}"#;
        assert!(matches!(serde_json::from_str::<MarketMsg>(other).unwrap(), MarketMsg::Unknown));
        assert!(split_frame("PONG").is_none());
    }

    #[test]
    fn strips_hashes() {
        let a = r#"{"x":[{"asset_id":"1","side":"BUY","hash":"cd86","best_bid":"0.68"},{"p":"2","hash":"ab"}],"hash":"ff"}"#;
        let s = strip_hashes(a);
        assert_eq!(s, r#"{"x":[{"asset_id":"1","side":"BUY","best_bid":"0.68"},{"p":"2"}]}"#);
        serde_json::from_str::<serde_json::Value>(&s).unwrap();
        assert!(matches!(strip_hashes(r#"{"a":1}"#), std::borrow::Cow::Borrowed(_)));
    }
}
