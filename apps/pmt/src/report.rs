//! `pmt report`: a summary of the trade journal. Per strategy: decisions, orders and how they
//! ended, money staked, settled bets, win rate against the price paid, P&L and ROI per bet with
//! its standard error, and how far fills landed from the signal price. Then realised P&L per day
//! or month, and the positions still open.
//!
//! Windowing (`from` / `to`, UTC days): decisions, fills and settlements count on the day they
//! happened, orders on the day they were created. Open positions are always as of now.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::path::PathBuf;

use anyhow::Result;
use chrono::{DateTime, NaiveDate};
use pm_core::{ConditionId, Fixed6, Price, Side};
use serde::Serialize;

use crate::journal::{self, Entry};
use crate::order::{Order, OrderId, OrderStatus};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum Period {
    Day,
    Month,
}

impl Period {
    fn key(self, t_ns: i64) -> String {
        let fmt = match self {
            Period::Day => "%Y-%m-%d",
            Period::Month => "%Y-%m",
        };
        DateTime::from_timestamp_nanos(t_ns).format(fmt).to_string()
    }
}

pub struct Options {
    pub root: PathBuf,
    pub mode: String,
    pub from: Option<NaiveDate>,
    pub to: Option<NaiveDate>,
    /// Only these strategy ids (empty = all).
    pub strategies: Vec<String>,
    pub by: Period,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct StrategyStats {
    pub strategy: String,
    pub signals: u64,
    pub skips: u64,
    /// Orders sent (not rejected).
    pub orders: u64,
    /// Orders with at least one fill.
    pub filled: u64,
    /// Orders that ended without any fill (killed, cancelled, expired).
    pub unfilled: u64,
    pub rejected: u64,
    /// Still working (or the run died before it ended).
    pub open_orders: u64,
    /// USDC spent on buys, fees included.
    pub staked: Fixed6,
    pub fees: Fixed6,
    /// Settled bets (one per position: strategy, market, token).
    pub settled: u64,
    pub won: u64,
    /// Cost of the settled bets.
    pub settled_cost: Fixed6,
    pub payout: Fixed6,
    pub pnl: Fixed6,
    /// `pnl / settled_cost`.
    pub roi: Option<f64>,
    /// Mean return per bet and its standard error.
    pub roi_per_bet: Option<f64>,
    pub roi_per_bet_se: Option<f64>,
    pub win_rate: Option<f64>,
    /// Cost per share of the settled bets, fees included: the win rate needed to break even.
    pub cost_per_share: Option<f64>,
    /// Mean of (fill VWAP − signal price) over markets with both, in cents.
    pub slippage_cents: Option<f64>,
    pub slippage_markets: u64,
    pub open_positions: u64,
    pub open_cost: Fixed6,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct PeriodRow {
    pub period: String,
    pub orders: u64,
    pub staked: Fixed6,
    pub settled: u64,
    pub won: u64,
    pub settled_cost: Fixed6,
    pub pnl: Fixed6,
    pub cumulative: Fixed6,
}

#[derive(Clone, Debug, Serialize)]
pub struct OpenPosition {
    pub strategy: String,
    pub market: ConditionId,
    pub label: String,
    pub shares: Fixed6,
    pub cost: Fixed6,
}

#[derive(Clone, Debug, Serialize)]
pub struct Report {
    pub mode: String,
    /// First and last day (UTC, YYYY-MM-DD), if limited.
    pub from: Option<String>,
    pub to: Option<String>,
    pub by: Period,
    pub records: usize,
    pub strategies: Vec<StrategyStats>,
    pub total: StrategyStats,
    pub periods: Vec<PeriodRow>,
    pub open: Vec<OpenPosition>,
}

/// Running sums for one strategy.
#[derive(Default)]
struct Acc {
    stats: StrategyStats,
    /// (cost, pnl) per settled bet.
    bets: Vec<(f64, f64)>,
    settled_shares: Fixed6,
    /// Market → signal price (first signal).
    signal: HashMap<ConditionId, Price>,
    /// Market → (shares bought, USDC before fees).
    bought: HashMap<ConditionId, (Fixed6, Fixed6)>,
}

pub fn run(o: &Options) -> Result<Report> {
    let entries = journal::load(&o.root, &o.mode, None)?;
    let in_window = |t_ns: i64| {
        let d = DateTime::from_timestamp_nanos(t_ns).date_naive();
        o.from.is_none_or(|f| d >= f) && o.to.is_none_or(|t| d <= t)
    };
    let wanted = |s: &str| o.strategies.is_empty() || o.strategies.iter().any(|x| x == s);

    let mut acc: BTreeMap<String, Acc> = BTreeMap::new();
    let mut periods: BTreeMap<String, PeriodRow> = BTreeMap::new();
    let mut labels: HashMap<ConditionId, String> = HashMap::new();
    let mut orders: HashMap<OrderId, &Order> = HashMap::new();
    for (_, e) in &entries {
        match e {
            Entry::Order(ord) => {
                orders.insert(ord.id, ord);
            }
            Entry::Decision(d) => {
                if let (Some(m), Some(q)) = (&d.market, &d.question) {
                    labels.insert(m.clone(), d.event.clone().unwrap_or_else(|| q.clone()));
                }
            }
            _ => {}
        }
    }
    for (t_ns, e) in &entries {
        match e {
            Entry::Decision(d) if wanted(&d.strategy) && in_window(*t_ns) => {
                let a = acc.entry(d.strategy.to_string()).or_default();
                match d.what.as_str() {
                    "signal" => {
                        a.stats.signals += 1;
                        let price = ["price", "display_price"]
                            .iter()
                            .find_map(|k| serde_json::from_value::<Price>(d.detail.get(*k)?.clone()).ok());
                        if let (Some(m), Some(p)) = (&d.market, price) {
                            a.signal.entry(m.clone()).or_insert(p);
                        }
                    }
                    "skip" => a.stats.skips += 1,
                    _ => {}
                }
            }
            Entry::Fill(f) if wanted(&f.strategy) && in_window(f.fill.t_ns) && f.side == Side::Buy => {
                let a = acc.entry(f.strategy.to_string()).or_default();
                let value = f.fill.price.mul_fixed(f.fill.size);
                a.stats.staked += value + f.fill.fee;
                a.stats.fees += f.fill.fee;
                let b = a.bought.entry(f.market.clone()).or_default();
                b.0 += f.fill.size;
                b.1 += value;
                periods.entry(o.by.key(f.fill.t_ns)).or_default().staked += value + f.fill.fee;
            }
            Entry::Settlement(s) if wanted(&s.strategy) && in_window(s.t_ns) => {
                let a = acc.entry(s.strategy.to_string()).or_default();
                a.stats.settled += 1;
                a.stats.won += s.won as u64;
                a.stats.settled_cost += s.cost;
                a.stats.payout += s.payout;
                a.stats.pnl += s.pnl;
                a.settled_shares += s.shares;
                a.bets.push((s.cost.to_f64(), s.pnl.to_f64()));
                let p = periods.entry(o.by.key(s.t_ns)).or_default();
                p.settled += 1;
                p.won += s.won as u64;
                p.settled_cost += s.cost;
                p.pnl += s.pnl;
            }
            _ => {}
        }
    }
    for ord in orders.values() {
        if !wanted(&ord.strategy) || !in_window(ord.created_ns) {
            continue;
        }
        let st = &mut acc.entry(ord.strategy.to_string()).or_default().stats;
        match ord.status {
            OrderStatus::Rejected => st.rejected += 1,
            OrderStatus::Open => st.open_orders += 1,
            _ if ord.filled.is_zero() => st.unfilled += 1,
            _ => st.filled += 1,
        }
        if ord.status != OrderStatus::Rejected {
            st.orders += 1;
            periods.entry(o.by.key(ord.created_ns)).or_default().orders += 1;
        }
    }

    // Open positions: as of now, over the whole journal.
    let pf = journal::replay_positions(entries.iter().map(|e| &e.1));
    let mut open = Vec::new();
    for ((strategy, market, _), p) in pf.positions() {
        if !wanted(strategy) {
            continue;
        }
        let st = &mut acc.entry(strategy.to_string()).or_default().stats;
        st.open_positions += 1;
        st.open_cost += p.cost;
        open.push(OpenPosition {
            strategy: strategy.to_string(),
            market: market.clone(),
            label: labels.get(market).cloned().unwrap_or_else(|| p.event_slug.clone()),
            shares: p.shares,
            cost: p.cost,
        });
    }

    let mut strategies = Vec::new();
    let mut all = Acc::default();
    for (name, a) in acc {
        all.bets.extend(a.bets.iter().copied());
        all.settled_shares += a.settled_shares;
        let stats = finish(name, a);
        add(&mut all.stats, &stats);
        strategies.push(stats);
    }
    let total = {
        let slip: Vec<(f64, u64)> = strategies
            .iter()
            .filter_map(|s| Some((s.slippage_cents?, s.slippage_markets)))
            .collect();
        let n: u64 = slip.iter().map(|x| x.1).sum();
        let mut t = finish("total".into(), all);
        t.slippage_markets = n;
        t.slippage_cents = (n > 0).then(|| slip.iter().map(|(c, k)| c * *k as f64).sum::<f64>() / n as f64);
        t
    };
    let mut cumulative = Fixed6::ZERO;
    let periods = periods
        .into_iter()
        .map(|(k, mut p)| {
            cumulative += p.pnl;
            p.period = k;
            p.cumulative = cumulative;
            p
        })
        .collect();
    Ok(Report {
        mode: o.mode.clone(),
        from: o.from.map(|d| d.to_string()),
        to: o.to.map(|d| d.to_string()),
        by: o.by,
        records: entries.len(),
        strategies,
        total,
        periods,
        open,
    })
}

/// Adds the countable fields of `s` to `t` (ratios are computed by [`finish`]).
fn add(t: &mut StrategyStats, s: &StrategyStats) {
    t.signals += s.signals;
    t.skips += s.skips;
    t.orders += s.orders;
    t.filled += s.filled;
    t.unfilled += s.unfilled;
    t.rejected += s.rejected;
    t.open_orders += s.open_orders;
    t.staked += s.staked;
    t.fees += s.fees;
    t.settled += s.settled;
    t.won += s.won;
    t.settled_cost += s.settled_cost;
    t.payout += s.payout;
    t.pnl += s.pnl;
    t.open_positions += s.open_positions;
    t.open_cost += s.open_cost;
}

fn finish(name: String, a: Acc) -> StrategyStats {
    let mut s = a.stats;
    s.strategy = name;
    let n = a.bets.len();
    if n > 0 {
        s.win_rate = Some(s.won as f64 / n as f64);
        s.roi = (!s.settled_cost.is_zero()).then(|| s.pnl.to_f64() / s.settled_cost.to_f64());
        s.cost_per_share = (!a.settled_shares.is_zero()).then(|| s.settled_cost.to_f64() / a.settled_shares.to_f64());
        let r: Vec<f64> = a.bets.iter().filter(|b| b.0 > 0.0).map(|(c, p)| p / c).collect();
        if !r.is_empty() {
            let mean = r.iter().sum::<f64>() / r.len() as f64;
            s.roi_per_bet = Some(mean);
            if r.len() > 1 {
                let var = r.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (r.len() - 1) as f64;
                s.roi_per_bet_se = Some((var / r.len() as f64).sqrt());
            }
        }
    }
    let slips: Vec<f64> = a
        .bought
        .iter()
        .filter_map(|(m, (shares, usdc))| {
            let vwap = usdc.to_f64() / shares.to_f64();
            Some((vwap - a.signal.get(m)?.to_f64()) * 100.0)
        })
        .collect();
    if !slips.is_empty() {
        s.slippage_markets = slips.len() as u64;
        s.slippage_cents = Some(slips.iter().sum::<f64>() / slips.len() as f64);
    }
    s
}

fn money(x: Fixed6) -> String {
    let v = x.to_f64();
    if v < 0.0 {
        format!("-${:.2}", -v)
    } else {
        format!("${v:.2}")
    }
}

fn pnl(x: Fixed6) -> String {
    if x.raw() > 0 {
        format!("+{}", money(x))
    } else {
        money(x)
    }
}

fn pct(x: Option<f64>) -> String {
    x.map_or("-".into(), |v| format!("{:.0}%", v * 100.0))
}

fn signed_pct(x: Option<f64>) -> String {
    x.map_or("-".into(), |v| format!("{:+.1}%", v * 100.0))
}

impl Report {
    pub fn render(&self) -> String {
        let mut out = String::new();
        let range = match (&self.from, &self.to) {
            (None, None) => "all days".to_string(),
            (f, t) => format!(
                "{} .. {}",
                f.as_deref().unwrap_or("start"),
                t.as_deref().unwrap_or("today")
            ),
        };
        let _ = writeln!(out, "pmt journal ({}), {range}: {} records", self.mode, self.records);
        if self.strategies.is_empty() {
            let _ = writeln!(out, "\nnothing journaled in this range");
            return out;
        }
        let mut rows = self.strategies.iter().collect::<Vec<_>>();
        if self.strategies.len() > 1 {
            rows.push(&self.total);
        }

        let _ = writeln!(out, "\nactivity");
        let _ = writeln!(
            out,
            "  {:<12} {:>7} {:>7} {:>7} {:>7} {:>8} {:>8} {:>5} {:>10} {:>8}",
            "strategy", "signals", "skips", "orders", "filled", "unfilled", "rejected", "open", "staked", "fees"
        );
        for s in &rows {
            let _ = writeln!(
                out,
                "  {:<12} {:>7} {:>7} {:>7} {:>7} {:>8} {:>8} {:>5} {:>10} {:>8}",
                s.strategy,
                s.signals,
                s.skips,
                s.orders,
                s.filled,
                s.unfilled,
                s.rejected,
                s.open_orders,
                money(s.staked),
                money(s.fees)
            );
        }

        let _ = writeln!(out, "\nresults (settled bets)");
        let _ = writeln!(
            out,
            "  {:<12} {:>5} {:>5} {:>5} {:>9} {:>10} {:>10} {:>8} {:>17} {:>9}",
            "strategy", "bets", "won", "win%", "cost/sh", "staked", "P&L", "ROI", "ROI/bet ± se", "slippage"
        );
        for s in &rows {
            let per_bet = match (s.roi_per_bet, s.roi_per_bet_se) {
                (Some(m), Some(se)) => format!("{:+.1}% ± {:.1}", m * 100.0, se * 100.0),
                (Some(m), None) => format!("{:+.1}%", m * 100.0),
                _ => "-".into(),
            };
            let slip = s.slippage_cents.map_or("-".into(), |c| format!("{c:+.2}c"));
            let _ = writeln!(
                out,
                "  {:<12} {:>5} {:>5} {:>5} {:>9} {:>10} {:>10} {:>8} {:>17} {:>9}",
                s.strategy,
                s.settled,
                s.won,
                pct(s.win_rate),
                s.cost_per_share.map_or("-".into(), |c| format!("{c:.3}")),
                money(s.settled_cost),
                pnl(s.pnl),
                signed_pct(s.roi),
                per_bet,
                slip
            );
        }
        let _ = writeln!(
            out,
            "  (cost/sh: price paid per share with fees = the win rate needed to break even; \
             slippage: fill VWAP minus the signal price)"
        );

        if !self.periods.is_empty() {
            let by = match self.by {
                Period::Day => "day",
                Period::Month => "month",
            };
            let _ = writeln!(out, "\nby {by} (UTC; P&L when settled)");
            let _ = writeln!(
                out,
                "  {:<10} {:>6} {:>10} {:>6} {:>5} {:>10} {:>8} {:>11}",
                by, "orders", "staked", "bets", "won", "P&L", "ROI", "cumulative"
            );
            for p in &self.periods {
                let roi = (!p.settled_cost.is_zero()).then(|| p.pnl.to_f64() / p.settled_cost.to_f64());
                let _ = writeln!(
                    out,
                    "  {:<10} {:>6} {:>10} {:>6} {:>5} {:>10} {:>8} {:>11}",
                    p.period,
                    p.orders,
                    money(p.staked),
                    p.settled,
                    p.won,
                    pnl(p.pnl),
                    signed_pct(roi),
                    pnl(p.cumulative)
                );
            }
        }

        let _ = writeln!(
            out,
            "\nopen positions: {} ({} at cost)",
            self.open.len(),
            money(self.total_open_cost())
        );
        for p in &self.open {
            let _ = writeln!(
                out,
                "  {:<12} {:>8} shares  {:>8}  {}",
                p.strategy,
                p.shares.to_string(),
                money(p.cost),
                p.label
            );
        }
        out
    }

    fn total_open_cost(&self) -> Fixed6 {
        self.open.iter().map(|p| p.cost).fold(Fixed6::ZERO, |a, b| a + b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::{FillRecord, Journal};
    use crate::order::{Fill, OrderRequest, Tif};
    use crate::portfolio::Settlement;
    use pm_store::{Store, StoreConfig};
    use serde_json::json;
    use std::sync::Arc;

    const DAY: i64 = 86_400_000_000_000;
    // 2026-10-17 00:00 UTC
    const D17: i64 = 1_792_195_200_000_000_000;

    fn px(s: &str) -> Fixed6 {
        s.parse().unwrap()
    }

    /// Writes a journal: c1 bets in markets a (won, day 17), b (lost, day 18), c (open), plus an
    /// unfilled order and a skip; fav one rejected order.
    async fn journal(root: &std::path::Path) {
        let store = Store::start(StoreConfig {
            root: root.to_path_buf(),
            ..Default::default()
        })
        .unwrap();
        let j = Journal::new(store.handle(), "paper");
        let c1: Arc<str> = "c1".into();
        for (i, (m, p, fill_p, filled, status)) in [
            ("a", "0.2", "0.21", true, OrderStatus::Filled),
            ("b", "0.25", "0.25", true, OrderStatus::Filled),
            ("c", "0.1", "0.12", true, OrderStatus::Filled),
            ("d", "0.3", "0.3", false, OrderStatus::Cancelled),
        ]
        .into_iter()
        .enumerate()
        {
            let t = D17 + i as i64 * 3600 * 1_000_000_000;
            let detail = json!({"price": p});
            let d = json!({"strategy": "c1", "what": "signal", "market": m, "question": "q", "event": format!("event {m}"), "detail": detail});
            j.decision("c1", t, &d).await;
            let req = OrderRequest::buy(
                &m.into(),
                &format!("{m}-t").into(),
                px("0.3"),
                px("10"),
                Tif::Fak,
                "x".into(),
            );
            let mut o = Order::new(OrderId(i as u64 + 1), c1.clone(), req, t);
            o.status = status;
            if filled {
                let fill = Fill {
                    price: px(fill_p),
                    size: px("10"),
                    fee: px("0.1"),
                    maker: false,
                    t_ns: t,
                };
                o.apply(&fill);
                let rec = FillRecord {
                    order: o.id,
                    strategy: c1.clone(),
                    market: m.into(),
                    token: format!("{m}-t").into(),
                    side: Side::Buy,
                    event_slug: format!("ev-{m}"),
                    league: "cs2".into(),
                    fill,
                };
                j.fill(&rec).await;
            }
            j.order(&o).await;
        }
        let skip = json!({"strategy": "c1", "what": "skip", "market": "e"});
        j.decision("c1", D17, &skip).await;
        for (m, won, cost, t) in [
            ("a", true, "2.2", D17 + 5 * 3600 * 1_000_000_000),
            ("b", false, "2.6", D17 + DAY),
        ] {
            let payout = if won { px("10") } else { Fixed6::ZERO };
            let s = Settlement {
                strategy: c1.clone(),
                market: m.into(),
                token: format!("{m}-t").into(),
                won,
                shares: px("10"),
                cost: px(cost),
                payout,
                pnl: payout - px(cost),
                t_ns: t,
                exit: false,
            };
            j.settlement(&s).await;
        }
        let req = OrderRequest::buy(&"f".into(), &"f-t".into(), px("0.85"), px("6"), Tif::Fak, "x".into());
        let mut o = Order::new(OrderId(9), "fav".into(), req, D17);
        o.status = OrderStatus::Rejected;
        j.order(&o).await;
        drop(j);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn summarises_the_journal() {
        let root = std::env::temp_dir().join(format!("pmt-report-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        journal(&root).await;
        let opts = |strategies: &[&str], from: Option<&str>| Options {
            root: root.clone(),
            mode: "paper".into(),
            from: from.map(|d| d.parse().unwrap()),
            to: None,
            strategies: strategies.iter().map(|s| s.to_string()).collect(),
            by: Period::Day,
        };
        let r = run(&opts(&[], None)).unwrap();
        let c1 = &r.strategies[0];
        assert_eq!(c1.strategy, "c1");
        assert_eq!((c1.signals, c1.skips), (4, 1));
        assert_eq!((c1.orders, c1.filled, c1.unfilled, c1.rejected), (4, 3, 1, 0));
        assert_eq!(c1.staked.to_string(), "6.1"); // 2.2 + 2.6 + 1.3
        assert_eq!((c1.settled, c1.won), (2, 1));
        assert_eq!(c1.pnl.to_string(), "5.2"); // 7.8 - 2.6
        assert_eq!(c1.win_rate, Some(0.5));
        let roi = c1.roi_per_bet.unwrap();
        assert!((roi - (7.8 / 2.2 - 1.0) / 2.0).abs() < 1e-9, "{roi}");
        assert!(c1.roi_per_bet_se.is_some());
        // Fills 1c, 0c, 2c above the signal.
        assert!((c1.slippage_cents.unwrap() - 1.0).abs() < 1e-9);
        assert_eq!((c1.open_positions, c1.open_cost.to_string()), (1, "1.3".to_string()));
        assert_eq!(r.open[0].label, "event c");
        let fav = &r.strategies[1];
        assert_eq!((fav.orders, fav.rejected), (0, 1));
        assert_eq!(r.total.rejected, 1);
        assert_eq!(r.total.settled, 2);

        let days: Vec<(&str, String, String)> = r
            .periods
            .iter()
            .map(|p| (p.period.as_str(), p.pnl.to_string(), p.cumulative.to_string()))
            .collect();
        assert_eq!(
            days,
            [
                ("2026-10-17", "7.8".to_string(), "7.8".to_string()),
                ("2026-10-18", "-2.6".to_string(), "5.2".to_string())
            ]
        );
        let text = r.render();
        assert!(text.contains("by day") && text.contains("open positions: 1"), "{text}");

        // Filters: one strategy, from the 18th (only b's settlement).
        let r = run(&opts(&["c1"], Some("2026-10-18"))).unwrap();
        assert_eq!(r.strategies.len(), 1);
        assert_eq!((r.strategies[0].settled, r.strategies[0].orders), (1, 0));
        assert_eq!(r.strategies[0].pnl.to_string(), "-2.6");
        let r = run(&Options {
            by: Period::Month,
            ..opts(&[], None)
        })
        .unwrap();
        assert_eq!(r.periods.len(), 1);
        assert_eq!(r.periods[0].period, "2026-10");
        std::fs::remove_dir_all(&root).unwrap();
    }
}
