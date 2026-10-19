//! The trader's own records, in the recorder's format (`pm-store`):
//!
//! ```text
//! <journal_dir>/YYYY/MM/DD/<mode>/<strategy>/<stream>/HH.r<run>.jsonl.zst
//! ```
//!
//! Streams: `decisions` (signals and skips, with the book at the time), `orders` (the full order on
//! creation and on every status change), `fills`, `settlements`. At startup [`restore`] replays
//! the last days to rebuild positions and the markets each strategy already decided on, so a
//! restart never decides (or bets) the same market twice.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use chrono::NaiveDate;
use pm_core::{ConditionId, Side, TokenId, now_ns};
use pm_store::read::{self, Filter};
use pm_store::{Partition, StoreHandle};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::order::{Fill, Order, OrderId, OrderStatus};
use crate::portfolio::{FillAt, Portfolio, Settlement};

const ORDERS: &str = "orders";
const FILLS: &str = "fills";
const SETTLEMENTS: &str = "settlements";
const DECISIONS: &str = "decisions";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FillRecord {
    pub order: OrderId,
    pub strategy: Arc<str>,
    pub market: ConditionId,
    pub token: TokenId,
    pub side: Side,
    pub event_slug: String,
    pub league: String,
    #[serde(flatten)]
    pub fill: Fill,
}

#[derive(Clone)]
pub struct Journal {
    store: StoreHandle,
    mode: &'static str,
}

impl Journal {
    pub fn new(store: StoreHandle, mode: &'static str) -> Journal {
        Journal { store, mode }
    }

    async fn write(&self, strategy: &str, stream: &'static str, t_ns: i64, v: &impl Serialize) {
        let part = Partition::new(self.mode, strategy);
        match serde_json::to_string(v) {
            Ok(line) => self.store.write(&part, stream, t_ns, 0, &line).await,
            Err(e) => tracing::error!("journal: cannot encode a {stream} record: {e}"),
        }
    }

    pub async fn order(&self, o: &Order) {
        self.write(&o.strategy, ORDERS, now_ns(), o).await;
    }

    pub async fn fill(&self, f: &FillRecord) {
        self.write(&f.strategy, FILLS, f.fill.t_ns, f).await;
    }

    pub async fn settlement(&self, s: &Settlement) {
        self.write(&s.strategy, SETTLEMENTS, s.t_ns, s).await;
    }

    pub async fn decision(&self, strategy: &str, t_ns: i64, v: &Value) {
        self.write(strategy, DECISIONS, t_ns, v).await;
    }
}

/// A `decisions` record (written by the engine from a strategy's `Ctx::note`).
#[derive(Clone, Debug, Deserialize)]
pub struct Decision {
    pub strategy: Arc<str>,
    /// `signal`, `skip`, `unfilled`, …
    pub what: String,
    #[serde(default)]
    pub market: Option<ConditionId>,
    #[serde(default)]
    pub question: Option<String>,
    #[serde(default)]
    pub event: Option<String>,
    #[serde(default)]
    pub detail: Value,
}

/// One journal record.
#[derive(Clone, Debug)]
pub enum Entry {
    Order(Order),
    Fill(FillRecord),
    Settlement(Settlement),
    Decision(Decision),
}

/// Reads every journal record of `mode` from day `from` (UTC) on, as `(write time ns, record)`
/// sorted by time. Records that do not parse are skipped.
pub fn load(root: &Path, mode: &str, from: Option<NaiveDate>) -> Result<Vec<(i64, Entry)>> {
    if !root.exists() {
        return Ok(vec![]);
    }
    let filter = Filter {
        from,
        categories: vec![mode.to_string()],
        streams: [ORDERS, FILLS, SETTLEMENTS, DECISIONS].map(String::from).to_vec(),
        ..Default::default()
    };
    let mut out = Vec::new();
    for f in read::scan(root, &filter)?.files {
        read::read_file(&f.path, |r| {
            let p = r.payload.get();
            let entry = match f.stream.as_str() {
                ORDERS => serde_json::from_str(p).ok().map(Entry::Order),
                FILLS => serde_json::from_str(p).ok().map(Entry::Fill),
                SETTLEMENTS => serde_json::from_str(p).ok().map(Entry::Settlement),
                DECISIONS => serde_json::from_str(p).ok().map(Entry::Decision),
                _ => None,
            };
            if let Some(e) = entry {
                out.push((r.t_ns, e));
            }
        })?;
    }
    out.sort_by_key(|r| r.0);
    Ok(out)
}

/// Rebuilds positions from fills and settlements (in time order).
pub fn replay_positions<'a>(entries: impl Iterator<Item = &'a Entry>) -> Portfolio {
    let mut pf = Portfolio::default();
    for e in entries {
        match e {
            Entry::Fill(f) => {
                let at = FillAt {
                    strategy: &f.strategy,
                    market: &f.market,
                    token: &f.token,
                    side: f.side,
                    event_slug: &f.event_slug,
                    league: &f.league,
                };
                pf.apply_fill(&at, &f.fill);
            }
            Entry::Settlement(s) => pf.record_settlement(s),
            _ => {}
        }
    }
    pf
}

#[derive(Default)]
pub struct Restored {
    pub portfolio: Portfolio,
    /// Strategy → markets it journaled a decision about or sent (non-rejected) orders in.
    pub decided: HashMap<Arc<str>, HashSet<ConditionId>>,
    /// Highest order id used; new ids continue from it.
    pub last_order: u64,
    pub records: usize,
}

/// Replays the whole journal of `mode` (it is small: a few records per bet). All of it, so that
/// order ids stay unique across runs and no old unsettled position is forgotten.
pub fn restore(root: &Path, mode: &str) -> Result<Restored> {
    let entries = load(root, mode, None)?;
    let mut out = Restored {
        portfolio: replay_positions(entries.iter().map(|e| &e.1)),
        records: entries.len(),
        ..Default::default()
    };
    for (_, e) in entries {
        match e {
            Entry::Order(o) => {
                out.last_order = out.last_order.max(o.id.0);
                if o.status != OrderStatus::Rejected {
                    out.decided.entry(o.strategy.clone()).or_default().insert(o.req.market);
                }
            }
            Entry::Decision(d) => {
                if let Some(m) = d.market {
                    out.decided.entry(d.strategy).or_default().insert(m);
                }
            }
            Entry::Fill(_) | Entry::Settlement(_) => {}
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order::{OrderRequest, Tif};
    use pm_core::Fixed6;
    use pm_store::{Store, StoreConfig};

    #[tokio::test]
    async fn a_restart_restores_positions_and_decided_markets() {
        let root = std::env::temp_dir().join(format!("pmt-journal-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let store = Store::start(StoreConfig {
            root: root.clone(),
            ..Default::default()
        })
        .unwrap();
        let j = Journal::new(store.handle(), "paper");
        let px = |s: &str| -> Fixed6 { s.parse().unwrap() };
        let (s, now) = (Arc::<str>::from("c1"), now_ns());
        for (id, market, status) in [(7, "0xa", OrderStatus::Filled), (8, "0xb", OrderStatus::Rejected)] {
            let req = OrderRequest::buy(&market.into(), &"t".into(), px("0.2"), px("10"), Tif::Fak, "x".into());
            let mut o = Order::new(OrderId(id), s.clone(), req, now);
            o.status = status;
            j.order(&o).await;
        }
        let fill = |market: &str, token: &str| FillRecord {
            order: OrderId(7),
            strategy: s.clone(),
            market: market.into(),
            token: token.into(),
            side: Side::Buy,
            event_slug: "ev".into(),
            league: "cs2".into(),
            fill: Fill {
                price: px("0.2"),
                size: px("10"),
                fee: px("0.08"),
                maker: false,
                t_ns: now,
            },
        };
        j.fill(&fill("0xa", "t")).await;
        j.fill(&fill("0xc", "u")).await;
        let settled = Settlement {
            strategy: s.clone(),
            market: "0xc".into(),
            token: "u".into(),
            won: true,
            shares: px("10"),
            cost: px("2.08"),
            payout: px("10"),
            pnl: px("7.92"),
            t_ns: now + 1,
            exit: false,
        };
        j.settlement(&settled).await;
        let skip = serde_json::json!({"strategy": "c1", "what": "skip", "market": "0xd"});
        j.decision("c1", now, &skip).await;
        drop(j);
        store.shutdown().await.unwrap();

        let r = restore(&root, "paper").unwrap();
        assert_eq!(r.records, 6);
        assert_eq!(r.last_order, 8);
        assert_eq!(r.decided[&s].len(), 2, "rejected orders do not count");
        assert!(
            r.decided[&s].contains("0xa") && r.decided[&s].contains("0xd"),
            "an order and a skip"
        );
        assert!(r.portfolio.holds("0xa") && !r.portfolio.holds("0xc"));
        let t = r.portfolio.totals("c1");
        assert_eq!((t.open, t.settled, t.pnl.to_string()), (1, 1, "7.92".to_string()));
        assert_eq!(restore(&root, "live").unwrap().records, 0, "modes are separate");
        std::fs::remove_dir_all(&root).unwrap();
    }
}
