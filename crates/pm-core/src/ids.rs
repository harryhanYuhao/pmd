//! Identifier newtypes and small shared enums.

use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

macro_rules! id_type {
    ($(#[$m:meta])* $name:ident) => {
        $(#[$m])*
        #[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub Arc<str>);

        impl $name {
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                $name(Arc::from(s))
            }
        }

        impl From<String> for $name {
            fn from(s: String) -> Self {
                $name(Arc::from(s))
            }
        }

        impl std::borrow::Borrow<str> for $name {
            fn borrow(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), &self.0)
            }
        }
    };
}

id_type!(
    /// CLOB token id of one outcome (a 77-digit decimal string); the websocket calls it `asset_id`.
    TokenId
);
id_type!(
    /// Market condition id (`0x…`); the websocket calls it `market`.
    ConditionId
);

/// Taker/order side. On the book, `Buy` levels are bids and `Sell` levels are asks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    #[serde(rename = "BUY", alias = "buy", alias = "Buy")]
    Buy,
    #[serde(rename = "SELL", alias = "sell", alias = "Sell")]
    Sell,
}

impl Side {
    pub fn opposite(self) -> Side {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}
