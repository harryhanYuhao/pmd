//! CLOB market channel (`wss://ws-subscriptions-clob.polymarket.com/ws/market`) for many tokens.
//!
//! [`ClobManager`] spreads tokens over connections of at most `max_per_conn` assets, opens new
//! connections as needed and (un)subscribes dynamically. Shards are never closed, only emptied.
//! Each connection restores its *current* token set on reconnect.

use std::collections::{BTreeSet, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pm_core::TokenId;
use pm_core::wire::clob_ws;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{WsEvent, WsHandle, WsSpec};

struct Shard {
    assets: Arc<Mutex<BTreeSet<TokenId>>>,
    handle: WsHandle,
}

/// Market-channel connections for a changing set of tokens. Every shard's events go to one
/// receiver.
pub struct ClobManager {
    max_per_conn: usize,
    custom_features: bool,
    shards: Vec<Shard>,
    out: mpsc::Sender<WsEvent>,
    cancel: CancellationToken,
}

impl ClobManager {
    /// `custom_features` also requests `best_bid_ask`, `new_market` and `market_resolved` events.
    pub fn new(
        max_per_conn: usize,
        custom_features: bool,
        out: mpsc::Sender<WsEvent>,
        cancel: CancellationToken,
    ) -> Self {
        ClobManager {
            max_per_conn: max_per_conn.max(1),
            custom_features,
            shards: Vec::new(),
            out,
            cancel,
        }
    }

    /// Subscribes `add` (filling existing shards first) and unsubscribes `remove`.
    pub fn update(&mut self, add: &[TokenId], remove: &[TokenId]) {
        let remove: HashSet<&TokenId> = remove.iter().collect();
        if !remove.is_empty() {
            for s in &self.shards {
                let gone: Vec<TokenId> = {
                    let mut a = s.assets.lock().unwrap();
                    let gone: Vec<TokenId> = a.iter().filter(|t| remove.contains(t)).cloned().collect();
                    for t in &gone {
                        a.remove(t);
                    }
                    gone
                };
                if !gone.is_empty() {
                    let ids: Vec<&str> = gone.iter().map(|t| t.as_str()).collect();
                    s.handle.send(clob_ws::update_subscription_msg(&ids, false));
                }
            }
        }

        let mut pending: Vec<TokenId> = add.to_vec();
        // Fill existing shards first, then open new ones.
        for s in &self.shards {
            if pending.is_empty() {
                break;
            }
            let batch: Vec<TokenId> = {
                let mut a = s.assets.lock().unwrap();
                let room = self.max_per_conn.saturating_sub(a.len());
                let batch: Vec<TokenId> = pending.drain(..room.min(pending.len())).collect();
                a.extend(batch.iter().cloned());
                batch
            };
            if !batch.is_empty() {
                let ids: Vec<&str> = batch.iter().map(|t| t.as_str()).collect();
                s.handle.send(clob_ws::update_subscription_msg(&ids, true));
            }
        }
        while !pending.is_empty() {
            let batch: BTreeSet<TokenId> = pending.drain(..self.max_per_conn.min(pending.len())).collect();
            self.spawn_shard(batch);
        }
    }

    /// Unsubscribes and resubscribes tokens so the server sends fresh `book` snapshots.
    pub fn resync(&self, tokens: &[TokenId]) {
        for s in &self.shards {
            let mine: Vec<&str> = {
                let a = s.assets.lock().unwrap();
                tokens.iter().filter(|t| a.contains(*t)).map(|t| t.as_str()).collect()
            };
            if !mine.is_empty() {
                s.handle.send(clob_ws::update_subscription_msg(&mine, false));
                s.handle.send(clob_ws::update_subscription_msg(&mine, true));
            }
        }
    }

    fn spawn_shard(&mut self, assets: BTreeSet<TokenId>) {
        let assets = Arc::new(Mutex::new(assets));
        let a2 = assets.clone();
        let custom = self.custom_features;
        let on_connect: crate::OnConnect = Arc::new(move || {
            let a = a2.lock().unwrap();
            if a.is_empty() {
                return vec![];
            }
            let ids: Vec<&str> = a.iter().map(|t| t.as_str()).collect();
            vec![clob_ws::subscribe_msg(&ids, custom)]
        });
        let mut spec = WsSpec::new(format!("clob-{}", self.shards.len()), clob_ws::MARKET_WS_URL);
        spec.keepalive = Some((Duration::from_secs(10), "PING".into()));
        spec.ignore = vec!["PONG".into()];
        spec.idle_timeout = Duration::from_secs(45);
        let (handle, _task) = crate::spawn(spec, on_connect, self.out.clone(), self.cancel.clone());
        self.shards.push(Shard { assets, handle });
        tracing::debug!(
            "clob: opened connection {} ({} connections)",
            self.shards.len() - 1,
            self.shards.len()
        );
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokens(range: std::ops::Range<usize>) -> Vec<TokenId> {
        range.map(|i| TokenId::from(i.to_string())).collect()
    }

    fn sizes(m: &ClobManager) -> Vec<usize> {
        m.shards.iter().map(|s| s.assets.lock().unwrap().len()).collect()
    }

    /// A manager whose connections never open (cancelled before they start).
    fn manager(max: usize) -> ClobManager {
        let cancel = CancellationToken::new();
        cancel.cancel();
        let (tx, _rx) = mpsc::channel(16);
        ClobManager::new(max, true, tx, cancel)
    }

    #[tokio::test]
    async fn shards_fill_up_before_new_connections_open() {
        let mut m = manager(500);
        m.update(&tokens(0..1200), &[]);
        assert_eq!(sizes(&m), [500, 500, 200]);

        // Freed room is reused before the last shard grows or a new one opens.
        m.update(&[], &tokens(0..100));
        assert_eq!(sizes(&m), [400, 500, 200]);
        m.update(&tokens(2000..2150), &[]);
        assert_eq!(sizes(&m), [500, 500, 250]);
        assert_eq!(m.shard_count(), 3);

        // Removing unknown tokens is a no-op; shards are never closed, only emptied.
        m.update(&[], &tokens(9000..9010));
        m.update(&[], &tokens(100..1200));
        assert_eq!(sizes(&m), [100, 0, 50]);
        let all: usize = sizes(&m).iter().sum();
        assert_eq!(all, 150);
    }

    #[tokio::test]
    async fn a_token_is_in_exactly_one_shard() {
        let mut m = manager(3);
        m.update(&tokens(0..10), &[]);
        m.update(&[], &tokens(2..5));
        m.update(&tokens(10..13), &[]);
        let mut seen = std::collections::HashSet::new();
        for s in &m.shards {
            for t in s.assets.lock().unwrap().iter() {
                assert!(seen.insert(t.clone()), "{t} in two shards");
            }
        }
        assert_eq!(seen.len(), 10);
        assert!(sizes(&m).iter().all(|&n| n <= 3));
    }
}
