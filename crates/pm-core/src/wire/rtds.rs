//! Real-time data socket (`wss://ws-live-data.polymarket.com`), `activity/trades` topic.
//!
//! One message per trade across all of Polymarket, including the taker's proxy wallet, which the
//! CLOB `last_trade_price` event does not carry.

use serde::Deserialize;

use crate::fixed::{Price, Size};
use crate::ids::{ConditionId, Side, TokenId};

pub const RTDS_URL: &str = "wss://ws-live-data.polymarket.com";

#[derive(Clone, Debug, Deserialize)]
pub struct RtdsMsg<T> {
    pub topic: String,
    #[serde(rename = "type")]
    pub kind: String,
    /// Server send time, ms.
    pub timestamp: u64,
    pub payload: T,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivityTrade {
    pub asset: TokenId,
    pub condition_id: ConditionId,
    #[serde(default)]
    pub event_slug: String,
    #[serde(default)]
    pub slug: String,
    #[serde(default)]
    pub outcome: String,
    #[serde(default)]
    pub outcome_index: Option<u32>,
    pub price: Price,
    pub size: Size,
    pub side: Side,
    pub proxy_wallet: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub pseudonym: String,
    /// Chain timestamp, seconds.
    pub timestamp: u64,
    #[serde(default)]
    pub transaction_hash: String,
}

/// Just enough of an RTDS message to route it.
#[derive(Debug, Deserialize)]
pub struct RtdsProbe<'a> {
    #[serde(borrow, default)]
    pub topic: Option<&'a str>,
    #[serde(borrow, default)]
    pub payload: Option<RtdsPayloadProbe<'a>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RtdsPayloadProbe<'a> {
    #[serde(borrow, default)]
    pub event_slug: Option<&'a str>,
    #[serde(borrow, default)]
    pub condition_id: Option<&'a str>,
}

pub fn subscribe_trades_msg() -> String {
    serde_json::json!({
        "action": "subscribe",
        "subscriptions": [{"topic": "activity", "type": "trades"}],
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_live_sample() {
        let s = r#"{"connection_id":"x","payload":{"asset":"813","bio":"","conditionId":"0xb3","eventSlug":"cs2-aur1-9z-2026-10-16","icon":"","name":"ferrari","outcome":"9z","outcomeIndex":1,"price":0.15,"profileImage":"","proxyWallet":"0xfE78","pseudonym":"Glaring-Salt","side":"BUY","size":169.04,"slug":"cs2-aur1-9z-2026-10-16-game2","timestamp":1792166308,"title":"t","transactionHash":"0x45"},"timestamp":1792166308856,"topic":"activity","type":"trades"}"#;
        let m: RtdsMsg<ActivityTrade> = serde_json::from_str(s).unwrap();
        assert_eq!(m.payload.price.to_string(), "0.15");
        assert_eq!(m.payload.side, Side::Buy);
        let p: RtdsProbe = serde_json::from_str(s).unwrap();
        assert_eq!(p.payload.unwrap().event_slug, Some("cs2-aur1-9z-2026-10-16"));
    }
}
