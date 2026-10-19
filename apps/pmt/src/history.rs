//! Loads a market's past trades from the Data API, one market at a time (the API is throttled
//! to ~1.4 requests/s).

use std::sync::Arc;
use std::time::Duration;

use pm_api::DataApi;
use pm_core::ConditionId;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::tape::{Source, TapeTrade};

const PAGE: usize = 500;
const PAUSE: Duration = Duration::from_millis(700);

pub struct Loaded {
    pub market: ConditionId,
    /// Trades (any order) and whether more exist than were loaded.
    pub result: Result<(Vec<TapeTrade>, bool), String>,
}

pub async fn run(
    mut rx: mpsc::UnboundedReceiver<ConditionId>,
    tx: mpsc::Sender<Loaded>,
    max_trades: usize,
    cancel: CancellationToken,
) {
    let api = DataApi::default();
    loop {
        let market = tokio::select! {
            _ = cancel.cancelled() => return,
            m = rx.recv() => match m {
                Some(m) => m,
                None => return,
            },
        };
        let result = tokio::select! {
            _ = cancel.cancelled() => return,
            r = load(&api, &market, max_trades) => r.map_err(|e| format!("{e:#}")),
        };
        if tx.send(Loaded { market, result }).await.is_err() {
            return;
        }
    }
}

async fn load(api: &DataApi, market: &ConditionId, max_trades: usize) -> anyhow::Result<(Vec<TapeTrade>, bool)> {
    let mut out = Vec::new();
    loop {
        let page = api.trades(market.as_str(), PAGE, out.len()).await?;
        let full = page.len() == PAGE;
        out.extend(page.into_iter().map(|t| TapeTrade {
            t_ns: t.timestamp * 1_000_000_000,
            token: t.asset.into(),
            price: t.price,
            size: t.size,
            side: t.side,
            tx: (!t.transaction_hash.is_empty()).then(|| Arc::from(t.transaction_hash)),
            source: Source::History,
        }));
        tokio::time::sleep(PAUSE).await;
        if !full {
            return Ok((out, false));
        }
        if out.len() >= max_trades {
            return Ok((out, true));
        }
    }
}
