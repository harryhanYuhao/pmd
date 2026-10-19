//! Markets as the trader sees them: Gamma metadata plus the market's role in its event.

use pm_api::{GammaEvent, GammaMarket};
use pm_core::{FeeSchedule, Fixed6, MarketInfo, Price, Size, TokenId};

/// What a market is about, for the markets strategies care about so far.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarketKind {
    /// Series (match) winner: Gamma `moneyline`.
    Series,
    /// Winner of map / game `n` of a series: Gamma `child_moneyline`, "Map 1 Winner".
    Map(u8),
    Other,
}

impl MarketKind {
    pub fn of(m: &GammaMarket) -> MarketKind {
        match m.sports_market_type.as_deref() {
            Some("moneyline") => MarketKind::Series,
            Some("child_moneyline") => {
                let label = m.group_item_title.as_deref().unwrap_or(&m.question);
                map_number(label)
                    .or_else(|| map_number(&m.question))
                    .map_or(MarketKind::Other, MarketKind::Map)
            }
            _ => MarketKind::Other,
        }
    }
}

/// `n` in "Map n Winner" / "Game n Winner".
fn map_number(label: &str) -> Option<u8> {
    if !label.contains("Winner") {
        return None;
    }
    ["Map ", "Game "].iter().find_map(|p| {
        let rest = &label[label.find(p)? + p.len()..];
        let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        digits.parse().ok()
    })
}

/// One binary market.
#[derive(Clone, Debug, PartialEq)]
pub struct Market {
    pub info: MarketInfo,
    pub kind: MarketKind,
    pub event_title: String,
    /// Scheduled start of the event (the series), Unix ns.
    pub start_ns: Option<i64>,
    pub fees: Option<FeeSchedule>,
    /// Winning outcome index, once resolved.
    pub winner: Option<usize>,
}

/// Polymarket's minimum size of a limit order, used when Gamma does not say.
const DEFAULT_MIN_SIZE: Fixed6 = Fixed6::from_raw(5_000_000);
const DEFAULT_TICK: Fixed6 = Fixed6::from_raw(10_000);

impl Market {
    /// `None` unless the market has exactly two outcome tokens.
    pub fn from_gamma(league: &str, ev: &GammaEvent, m: &GammaMarket) -> Option<Market> {
        let info = m.to_info(league, ev);
        if info.token_ids.len() != 2 || info.condition_id.as_str().is_empty() {
            return None;
        }
        Some(Market {
            info,
            kind: MarketKind::of(m),
            event_title: ev.title.clone(),
            start_ns: ev.start().and_then(|t| t.timestamp_nanos_opt()),
            fees: m.fees(),
            winner: m.winner(),
        })
    }

    /// Open on the CLOB right now.
    pub fn tradable(&self) -> bool {
        self.info.active && !self.info.closed && self.info.accepting_orders && self.winner.is_none()
    }

    pub fn tick(&self) -> Price {
        self.info.tick_size.filter(|t| !t.is_zero()).unwrap_or(DEFAULT_TICK)
    }

    pub fn min_size(&self) -> Size {
        self.info.min_order_size.map_or(DEFAULT_MIN_SIZE, Fixed6::from_f64)
    }

    /// The other outcome's token.
    pub fn other(&self, token: &TokenId) -> Option<&TokenId> {
        self.info.token_ids.iter().find(|t| *t != token)
    }

    /// Outcome name of a token (`?` if unknown).
    pub fn outcome(&self, token: &TokenId) -> &str {
        self.info
            .outcome_index(token.as_str())
            .and_then(|i| self.info.outcomes.get(i))
            .map_or("?", |s| s.as_str())
    }

    /// Short label for logs: `Map 2 Winner — Team A vs Team B`.
    pub fn label(&self) -> String {
        match self.kind {
            MarketKind::Series => format!("series — {}", self.event_title),
            MarketKind::Map(n) => format!("map {n} — {}", self.event_title),
            MarketKind::Other => self.info.question.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gamma(kind: &str, title: &str) -> GammaMarket {
        GammaMarket {
            sports_market_type: Some(kind.into()),
            group_item_title: Some(title.into()),
            question: format!("A vs B - {title}"),
            ..Default::default()
        }
    }

    #[test]
    fn kinds_from_gamma_labels() {
        assert_eq!(MarketKind::of(&gamma("moneyline", "Match Winner")), MarketKind::Series);
        assert_eq!(
            MarketKind::of(&gamma("child_moneyline", "Map 2 Winner")),
            MarketKind::Map(2)
        );
        assert_eq!(
            MarketKind::of(&gamma("child_moneyline", "Game 3 Winner")),
            MarketKind::Map(3)
        );
        assert_eq!(
            MarketKind::of(&gamma("child_moneyline", "First Blood")),
            MarketKind::Other
        );
        assert_eq!(MarketKind::of(&gamma("totals", "O/U 2.5 Games")), MarketKind::Other);
        let no_title = GammaMarket {
            group_item_title: None,
            ..gamma("child_moneyline", "Map 1 Winner")
        };
        assert_eq!(MarketKind::of(&no_title), MarketKind::Map(1));
    }

    #[test]
    fn builds_from_a_gamma_event() {
        let ev: GammaEvent = serde_json::from_value(serde_json::json!({
            "id": "1", "slug": "cs2-a-b", "title": "A vs B", "startTime": "2026-10-17T14:00:00Z",
            "markets": [{
                "conditionId": "0xm", "outcomes": "[\"A\", \"B\"]", "clobTokenIds": "[\"1\", \"2\"]",
                "sportsMarketType": "child_moneyline", "groupItemTitle": "Map 1 Winner",
                "active": true, "closed": false, "orderMinSize": 5, "orderPriceMinTickSize": 0.001
            }]
        }))
        .unwrap();
        let m = Market::from_gamma("cs2", &ev, &ev.markets[0]).unwrap();
        assert_eq!(m.kind, MarketKind::Map(1));
        assert_eq!(m.start_ns, Some(1_792_245_600_000_000_000));
        assert!(m.tradable());
        assert_eq!(m.tick().to_string(), "0.001");
        assert_eq!(m.min_size().to_string(), "5");
        assert_eq!(m.outcome(&"2".into()), "B");
        assert_eq!(m.other(&"2".into()).unwrap().as_str(), "1");
    }
}
