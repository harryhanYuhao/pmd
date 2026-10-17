//! Resilient websocket session.
//!
//! [`spawn`] keeps one logical connection alive forever: it reconnects with exponential backoff,
//! sends a keepalive, answers text pings, treats silence longer than `idle_timeout` as a dead
//! connection, and on every (re)connect sends the messages produced by `on_connect` (so the
//! caller's *current* subscription state is restored, not a stale copy).
//!
//! Each physical connection gets a process-unique session id. Consumers receive
//! [`WsEvent::Connected`] / [`WsEvent::Disconnected`] markers, which makes gaps in recorded data
//! explicit.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
pub struct WsSpec {
    /// Label used in logs.
    pub name: String,
    pub url: String,
    /// Text frame sent every interval (e.g. `"PING"` for the CLOB and RTDS feeds).
    pub keepalive: Option<(Duration, String)>,
    /// Exact text frames to answer, e.g. `("ping", "pong")` for the sports feed.
    pub auto_reply: Vec<(String, String)>,
    /// Text frames that are protocol noise and should not be forwarded (e.g. `"PONG"`).
    pub ignore: Vec<String>,
    pub idle_timeout: Duration,
    pub backoff_min: Duration,
    pub backoff_max: Duration,
}

impl WsSpec {
    pub fn new(name: impl Into<String>, url: impl Into<String>) -> Self {
        WsSpec {
            name: name.into(),
            url: url.into(),
            keepalive: None,
            auto_reply: vec![],
            ignore: vec![],
            idle_timeout: Duration::from_secs(60),
            backoff_min: Duration::from_millis(500),
            backoff_max: Duration::from_secs(30),
        }
    }
}

#[derive(Debug)]
pub enum WsEvent {
    Connected { feed: Arc<str>, session: u64 },
    /// A data frame. `recv_ns` is taken as soon as the frame is read.
    Text { session: u64, recv_ns: i64, text: String },
    Disconnected { feed: Arc<str>, session: u64, reason: String },
}

/// Produces the subscription messages to send right after each connect.
pub type OnConnect = Arc<dyn Fn() -> Vec<String> + Send + Sync>;

/// Sends text frames on the current connection. Frames sent while disconnected are delivered
/// after the next connect (after the `on_connect` messages); they should therefore be idempotent.
#[derive(Clone)]
pub struct WsHandle {
    tx: mpsc::UnboundedSender<String>,
}

impl WsHandle {
    pub fn send(&self, text: String) -> bool {
        self.tx.send(text).is_ok()
    }
}

pub fn spawn(
    spec: WsSpec,
    on_connect: OnConnect,
    out: mpsc::Sender<WsEvent>,
    cancel: CancellationToken,
) -> (WsHandle, tokio::task::JoinHandle<()>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let task = tokio::spawn(run(spec, on_connect, out, rx, cancel));
    (WsHandle { tx }, task)
}

fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

async fn run(
    spec: WsSpec,
    on_connect: OnConnect,
    out: mpsc::Sender<WsEvent>,
    mut ctl: mpsc::UnboundedReceiver<String>,
    cancel: CancellationToken,
) {
    let mut backoff = spec.backoff_min;
    while !cancel.is_cancelled() {
        let session = NEXT_SESSION.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let reason = tokio::select! {
            _ = cancel.cancelled() => "shutdown".to_string(),
            r = connection(&spec, session, &on_connect, &out, &mut ctl, &cancel) => match r {
                Ok(why) => why,
                Err(e) => format!("{e:#}"),
            },
        };
        let feed: Arc<str> = Arc::from(spec.name.as_str());
        if out.send(WsEvent::Disconnected { feed, session, reason: reason.clone() }).await.is_err() {
            return;
        }
        if cancel.is_cancelled() {
            return;
        }
        // A connection that lived a while resets the backoff.
        if started.elapsed() > Duration::from_secs(60) {
            backoff = spec.backoff_min;
        }
        tracing::warn!(feed = %spec.name, session, %reason, retry_in = ?backoff, "websocket disconnected");
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(spec.backoff_max);
    }
}

/// Runs one physical connection until it fails. Returns the reason it ended.
async fn connection(
    spec: &WsSpec,
    session: u64,
    on_connect: &OnConnect,
    out: &mpsc::Sender<WsEvent>,
    ctl: &mut mpsc::UnboundedReceiver<String>,
    cancel: &CancellationToken,
) -> anyhow::Result<String> {
    let (ws, _) = tokio::time::timeout(Duration::from_secs(20), tokio_tungstenite::connect_async(spec.url.as_str()))
        .await
        .map_err(|_| anyhow::anyhow!("connect timeout"))??;
    let (mut sink, mut stream) = ws.split();
    for m in on_connect() {
        sink.send(Message::Text(m.into())).await?;
    }
    tracing::info!(feed = %spec.name, session, "websocket connected");
    if out.send(WsEvent::Connected { feed: Arc::from(spec.name.as_str()), session }).await.is_err() {
        return Ok("consumer gone".into());
    }

    let ka_every = spec.keepalive.as_ref().map(|k| k.0).unwrap_or(Duration::from_secs(3600));
    let mut keepalive = tokio::time::interval_at(Instant::now() + ka_every, ka_every);
    let mut deadline = Instant::now() + spec.idle_timeout;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                let _ = sink.send(Message::Close(None)).await;
                return Ok("shutdown".into());
            }
            _ = tokio::time::sleep_until(deadline) => {
                return Ok(format!("idle for {:?}", spec.idle_timeout));
            }
            _ = keepalive.tick(), if spec.keepalive.is_some() => {
                let text = spec.keepalive.as_ref().map(|k| k.1.clone()).unwrap_or_default();
                sink.send(Message::Text(text.into())).await?;
            }
            m = ctl.recv() => {
                let Some(m) = m else { return Ok("handle dropped".into()) };
                sink.send(Message::Text(m.into())).await?;
            }
            msg = stream.next() => {
                let recv_ns = now_ns();
                let Some(msg) = msg else { return Ok("stream ended".into()) };
                deadline = Instant::now() + spec.idle_timeout;
                let text = match msg? {
                    Message::Text(t) => t.as_str().to_owned(),
                    Message::Binary(b) => match String::from_utf8(b.to_vec()) {
                        Ok(s) => s,
                        Err(_) => continue,
                    },
                    Message::Close(f) => return Ok(format!("server close: {f:?}")),
                    // Protocol pings are answered by tungstenite; pongs need nothing.
                    _ => continue,
                };
                if let Some((_, reply)) = spec.auto_reply.iter().find(|(k, _)| *k == text) {
                    sink.send(Message::Text(reply.clone().into())).await?;
                    continue;
                }
                if spec.ignore.contains(&text) {
                    continue;
                }
                if out.send(WsEvent::Text { session, recv_ns, text }).await.is_err() {
                    return Ok("consumer gone".into());
                }
            }
        }
    }
}
