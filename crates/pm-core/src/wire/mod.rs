//! Wire formats of the Polymarket streaming feeds.

pub mod clob_ws;
pub mod rtds;
pub mod sports;

use serde::{Deserialize, Deserializer, de};

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
