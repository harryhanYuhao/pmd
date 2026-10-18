//! Wire formats of the Polymarket streaming feeds.

pub mod clob_ws;
pub mod rtds;
pub mod sports;

use serde::{Deserialize, Deserializer, de};
use serde_json::value::RawValue;

/// Splits a websocket frame into its JSON objects without re-encoding them, so the exact server
/// bytes can be stored. All three feeds send either one object or an array of objects per frame.
/// Returns `None` for non-JSON frames such as `PONG`.
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

/// Millisecond timestamps arrive as strings on the CLOB feed and as numbers elsewhere.
pub(crate) fn de_u64<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum N<'a> {
        Num(u64),
        Str(&'a str),
        Float(f64),
    }
    match N::deserialize(d)? {
        N::Num(n) => Ok(n),
        N::Str(s) => s.parse().map_err(de::Error::custom),
        N::Float(f) => Ok(f as u64),
    }
}
