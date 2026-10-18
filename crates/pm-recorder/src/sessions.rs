//! Websocket connect / disconnect markers, written to `_system/<feed>/sessions` by every feed so
//! gaps in the recorded data are explicit.

use std::sync::Arc;

use pm_core::now_ns;
use pm_store::StoreHandle;
use pm_ws::WsEvent;
use serde_json::json;

use crate::registry;

/// Records `ev` if it is a connect or disconnect and returns `(connection name, connected)`;
/// returns `None` for data frames.
pub async fn record(store: &StoreHandle, feed: &str, ev: &WsEvent) -> Option<(Arc<str>, bool)> {
    let (conn, session, connected, m) = match ev {
        WsEvent::Connected { feed, session } => (feed, *session, true, json!({"feed": &**feed, "event": "connected"})),
        WsEvent::Disconnected { feed, session, reason } => (
            feed,
            *session,
            false,
            json!({"feed": &**feed, "event": "disconnected", "reason": reason}),
        ),
        WsEvent::Text { .. } => return None,
    };
    store
        .write(&registry::system(feed), "sessions", now_ns(), session, &m.to_string())
        .await;
    Some((conn.clone(), connected))
}
