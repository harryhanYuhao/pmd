//! Offline tools over the recorder's raw files (`pm-store` layout): `audit` summarises what was
//! recorded and what is missing or damaged; `export` converts it to Parquet. Nothing here writes
//! into the recorded data.

pub mod audit;
pub mod export;
pub mod fmt;

/// A connection's feed family: `clob-17` → `clob` (CLOB shards); other names are unchanged.
pub(crate) fn feed_family(feed: &str) -> &str {
    match feed.rsplit_once('-') {
        Some((head, tail)) if !tail.is_empty() && tail.chars().all(|c| c.is_ascii_digit()) => head,
        _ => feed,
    }
}
