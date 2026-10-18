//! `pm markets`: a live overview of Polymarket's open events from Gamma: totals, a breakdown by
//! category and league, the biggest and the live events, and what the recorder config covers.
//!
//! Event-level data only by default (`include_markets=false`: all ~22k open events in ~20 s).
//! `--markets` also fetches every market (~14x the data), so pair it with `--category`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write;

use anyhow::Result;
use chrono::{DateTime, Utc};
use pm_api::{EventsQuery, Gamma};
use pm_dataset::fmt::{count, usd};
use serde::{Deserialize, Deserializer, Serialize};

/// Event category = the first of these tags the event carries (order matters: esports events
/// are also tagged `sports`), mapped to a display name.
const CATEGORIES: [(&str, &str); 15] = [
    ("esports", "esports"),
    ("sports", "sports"),
    ("crypto", "crypto"),
    ("politics", "politics"),
    ("elections", "politics"),
    ("geopolitics", "geopolitics"),
    ("finance", "finance"),
    ("economy", "finance"),
    ("business", "finance"),
    ("tech", "tech"),
    ("pop-culture", "culture"),
    ("culture", "culture"),
    ("science", "science"),
    ("weather", "weather"),
    ("world", "world"),
];

pub struct Options {
    /// Gamma tag slug to fetch (`esports`, `sports`, `politics`, …); all open events if None.
    pub category: Option<String>,
    /// League abbreviations (Gamma `sport`, e.g. `cs2`, `epl`); filtered after fetching.
    pub leagues: Vec<String>,
    pub live_only: bool,
    pub with_markets: bool,
    pub top: usize,
    /// Recorder config leagues as (category, key, score_league): for the coverage section.
    pub recorded: Option<Vec<(String, String, Option<String>)>>,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct Event {
    title: String,
    slug: String,
    tags: Vec<Tag>,
    sport: Option<Sport>,
    live: bool,
    score: Option<String>,
    period: Option<String>,
    start_time: Option<String>,
    #[serde(deserialize_with = "lenient_f64")]
    volume: f64,
    #[serde(deserialize_with = "lenient_f64")]
    volume24hr: f64,
    #[serde(deserialize_with = "lenient_f64")]
    liquidity: f64,
    #[serde(deserialize_with = "lenient_f64")]
    open_interest: f64,
    markets: Vec<Market>,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct Tag {
    slug: String,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct Sport {
    sport: String,
    name: String,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct Market {
    active: bool,
    closed: bool,
    enable_order_book: bool,
    accepting_orders: bool,
    sports_market_type: Option<String>,
    #[serde(deserialize_with = "lenient_f64")]
    volume24hr: f64,
}

/// Gamma sends some amounts as numbers and some as strings; missing or bad values are 0.
fn lenient_f64<'de, D: Deserializer<'de>>(d: D) -> Result<f64, D::Error> {
    Ok(match serde_json::Value::deserialize(d)? {
        serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0),
        serde_json::Value::String(s) => s.parse().unwrap_or(0.0),
        _ => 0.0,
    })
}

impl Event {
    fn category(&self) -> &'static str {
        CATEGORIES
            .iter()
            .find(|(slug, _)| self.tags.iter().any(|t| t.slug == *slug))
            .map_or("other", |(_, name)| name)
    }

    fn league(&self) -> Option<&Sport> {
        self.sport.as_ref().filter(|s| !s.sport.is_empty())
    }
}

#[derive(Debug, Default, Serialize)]
pub struct Overview {
    pub fetched_at: String,
    pub scope: String,
    pub events: u64,
    pub live: u64,
    pub volume_24h: f64,
    pub volume: f64,
    pub liquidity: f64,
    pub open_interest: f64,
    /// Present with `--markets`.
    pub markets: Option<MarketStats>,
    pub categories: Vec<Group>,
    /// Sports and esports leagues, by 24 h volume.
    pub leagues: Vec<Group>,
    pub top_events: Vec<EventRow>,
    pub live_events: Vec<EventRow>,
    pub coverage: Option<Coverage>,
    /// `--top`: the text report shows twice as many leagues.
    #[serde(skip)]
    pub top: usize,
}

#[derive(Debug, Default, Serialize)]
pub struct MarketStats {
    pub markets: u64,
    pub with_order_book: u64,
    pub accepting_orders: u64,
    /// (sportsMarketType or `-`, markets, 24 h volume), by volume.
    pub by_type: Vec<(String, u64, f64)>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Group {
    pub key: String,
    pub name: String,
    pub events: u64,
    pub live: u64,
    /// Present with `--markets`.
    pub markets: Option<u64>,
    pub volume_24h: f64,
    pub volume: f64,
    pub liquidity: f64,
    /// Whether the recorder config records this league (leagues only, with a config).
    pub recorded: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct EventRow {
    pub title: String,
    pub slug: String,
    pub category: String,
    pub league: Option<String>,
    pub volume_24h: f64,
    pub liquidity: f64,
    pub live: bool,
    pub score: Option<String>,
    pub period: Option<String>,
    pub start_time: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct Coverage {
    pub leagues_configured: u64,
    /// Configured leagues with open events right now.
    pub leagues_with_events: u64,
    /// 24 h volume of sports + esports events in configured leagues, and in all leagues.
    pub volume_24h_recorded: f64,
    pub volume_24h_sports: f64,
    /// Largest leagues by 24 h volume that the config does not record.
    pub biggest_unrecorded: Vec<Group>,
}

pub async fn run(opts: &Options) -> Result<Overview> {
    let q = EventsQuery {
        active: Some(true),
        closed: Some(false),
        tag_slug: opts.category.clone(),
        include_markets: Some(opts.with_markets),
        ..Default::default()
    };
    let started = std::time::Instant::now();
    let raw = Gamma::default().events_raw(&q).await?;
    tracing::info!(
        "markets: fetched {} open events in {:.1}s",
        raw.len(),
        started.elapsed().as_secs_f64()
    );
    let events: Vec<Event> = raw.into_iter().filter_map(|v| serde_json::from_value(v).ok()).collect();
    Ok(summarise(&select(events, opts), opts))
}

/// Applies the filters Gamma cannot: leagues (case-insensitive) and live-only.
fn select(mut events: Vec<Event>, opts: &Options) -> Vec<Event> {
    if !opts.leagues.is_empty() {
        events.retain(|e| {
            e.league()
                .is_some_and(|s| opts.leagues.iter().any(|l| l.eq_ignore_ascii_case(&s.sport)))
        });
    }
    if opts.live_only {
        events.retain(|e| e.live);
    }
    events
}

fn summarise(events: &[Event], opts: &Options) -> Overview {
    let mut o = Overview {
        fetched_at: Utc::now().format("%Y-%m-%d %H:%M:%SZ").to_string(),
        scope: scope(opts),
        top: opts.top.max(1),
        ..Default::default()
    };
    let mut cats: BTreeMap<&str, Group> = BTreeMap::new();
    let mut leagues: BTreeMap<String, Group> = BTreeMap::new();
    let mut ms = MarketStats::default();
    let mut types: HashMap<String, (u64, f64)> = HashMap::new();
    let add = |g: &mut Group, e: &Event, n_markets: u64| {
        g.events += 1;
        g.live += e.live as u64;
        g.volume_24h += e.volume24hr;
        g.volume += e.volume;
        g.liquidity += e.liquidity;
        if opts.with_markets {
            *g.markets.get_or_insert(0) += n_markets;
        }
    };
    for e in events {
        o.events += 1;
        o.live += e.live as u64;
        o.volume_24h += e.volume24hr;
        o.volume += e.volume;
        o.liquidity += e.liquidity;
        o.open_interest += e.open_interest;
        let open: Vec<&Market> = e.markets.iter().filter(|m| m.active && !m.closed).collect();
        for m in &open {
            ms.markets += 1;
            ms.with_order_book += m.enable_order_book as u64;
            ms.accepting_orders += m.accepting_orders as u64;
            let t = types
                .entry(m.sports_market_type.clone().unwrap_or_else(|| "-".into()))
                .or_default();
            t.0 += 1;
            t.1 += m.volume24hr;
        }
        let cat = e.category();
        let g = cats.entry(cat).or_insert_with(|| Group {
            key: cat.into(),
            name: cat.into(),
            ..Default::default()
        });
        add(g, e, open.len() as u64);
        if let Some(s) = e.league()
            && matches!(cat, "sports" | "esports")
        {
            let g = leagues.entry(s.sport.to_ascii_lowercase()).or_insert_with(|| Group {
                key: s.sport.to_ascii_lowercase(),
                name: s.name.clone(),
                ..Default::default()
            });
            add(g, e, open.len() as u64);
        }
    }
    if opts.with_markets {
        let mut by_type: Vec<(String, u64, f64)> = types.into_iter().map(|(k, (n, v))| (k, n, v)).collect();
        by_type.sort_by(|a, b| b.2.total_cmp(&a.2));
        ms.by_type = by_type;
        o.markets = Some(ms);
    }

    let by_volume = |a: &Group, b: &Group| b.volume_24h.total_cmp(&a.volume_24h);
    o.categories = cats.into_values().collect();
    o.categories.sort_by(by_volume);
    let mut leagues: Vec<Group> = leagues.into_values().collect();
    leagues.sort_by(by_volume);

    if let Some(rec) = &opts.recorded {
        // Gamma's `sport` abbreviation is the recorder's `score_league` if set, else its key.
        let keys: HashSet<String> = rec
            .iter()
            .map(|(_, k, s)| s.as_deref().unwrap_or(k).to_ascii_lowercase())
            .collect();
        for g in &mut leagues {
            g.recorded = Some(keys.contains(&g.key));
        }
        o.coverage = Some(Coverage {
            leagues_configured: keys.len() as u64,
            leagues_with_events: leagues.iter().filter(|g| g.recorded == Some(true)).count() as u64,
            volume_24h_recorded: leagues
                .iter()
                .filter(|g| g.recorded == Some(true))
                .map(|g| g.volume_24h)
                .sum(),
            volume_24h_sports: leagues.iter().map(|g| g.volume_24h).sum(),
            biggest_unrecorded: leagues
                .iter()
                .filter(|g| g.recorded == Some(false))
                .take(opts.top)
                .cloned()
                .collect(),
        });
    }
    o.leagues = leagues;

    let row = |e: &Event| EventRow {
        title: e.title.clone(),
        slug: e.slug.clone(),
        category: e.category().into(),
        league: e.league().map(|s| s.sport.clone()),
        volume_24h: e.volume24hr,
        liquidity: e.liquidity,
        live: e.live,
        score: e.score.clone().filter(|s| !s.is_empty()),
        period: e.period.clone().filter(|s| !s.is_empty()),
        start_time: e.start_time.clone(),
    };
    let mut by_vol: Vec<&Event> = events.iter().collect();
    by_vol.sort_by(|a, b| b.volume24hr.total_cmp(&a.volume24hr));
    o.top_events = by_vol.iter().take(opts.top).map(|e| row(e)).collect();
    o.live_events = by_vol
        .iter()
        .filter(|e| e.live)
        .take(opts.top)
        .map(|e| row(e))
        .collect();
    o
}

fn scope(opts: &Options) -> String {
    let mut s = vec![match &opts.category {
        Some(c) => format!("tag {c}"),
        None => "all open events".to_string(),
    }];
    if !opts.leagues.is_empty() {
        s.push(format!("leagues {}", opts.leagues.join(",")));
    }
    if opts.live_only {
        s.push("live only".into());
    }
    s.join(", ")
}

/// `in 2h05m` / `3h10m ago` from a Gamma start time (any of Gamma's formats).
fn when(start: Option<&str>, now: DateTime<Utc>) -> String {
    let Some(t) = start.and_then(pm_api::gamma::parse_time) else {
        return String::new();
    };
    let mins = (t - now).num_minutes();
    let span = |m: i64| match m {
        m if m >= 1440 => format!("{}d{}h", m / 1440, m % 1440 / 60),
        m if m >= 60 => format!("{}h{:02}m", m / 60, m % 60),
        m => format!("{m}m"),
    };
    if mins >= 0 {
        format!("in {}", span(mins))
    } else {
        format!("{} ago", span(-mins))
    }
}

fn trunc(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        let mut t: String = s.chars().take(n - 1).collect();
        t.push('…');
        t
    }
}

impl Overview {
    pub fn render(&self) -> String {
        let mut o = String::new();
        let _ = writeln!(o, "pm markets  {}  ({})", self.fetched_at, self.scope);
        let _ = writeln!(
            o,
            "{} open events, {} live now | 24h volume {} | total volume {} | liquidity {} | open interest {}",
            count(self.events),
            self.live,
            usd(self.volume_24h),
            usd(self.volume),
            usd(self.liquidity),
            usd(self.open_interest)
        );
        if let Some(m) = &self.markets {
            let _ = writeln!(
                o,
                "{} open markets, {} with an order book, {} accepting orders",
                count(m.markets),
                count(m.with_order_book),
                count(m.accepting_orders)
            );
        }

        let with_markets = self.markets.is_some();
        let group_table = |o: &mut String, rows: &[Group], recorded: bool| {
            let _ = write!(o, "{:<28} {:>7} {:>5}", "", "events", "live");
            if with_markets {
                let _ = write!(o, " {:>8}", "markets");
            }
            let _ = writeln!(
                o,
                " {:>10} {:>10} {:>10}{}",
                "24h vol",
                "volume",
                "liquidity",
                if recorded { "  rec" } else { "" }
            );
            for g in rows {
                let name = if g.key == g.name {
                    g.key.clone()
                } else {
                    format!("{} ({})", g.key, g.name)
                };
                let _ = write!(o, "{:<28} {:>7} {:>5}", trunc(&name, 28), g.events, g.live);
                if with_markets {
                    let _ = write!(o, " {:>8}", g.markets.unwrap_or(0));
                }
                let mark = match g.recorded {
                    Some(true) => "  yes",
                    Some(false) => "",
                    None => "",
                };
                let _ = writeln!(
                    o,
                    " {:>10} {:>10} {:>10}{mark}",
                    usd(g.volume_24h),
                    usd(g.volume),
                    usd(g.liquidity)
                );
            }
        };

        let _ = writeln!(o, "\n== By category (first matching tag)");
        group_table(&mut o, &self.categories, false);

        if !self.leagues.is_empty() {
            let shown = self.top.max(1) * 2;
            let _ = writeln!(
                o,
                "\n== Sports and esports leagues by 24h volume (top {} of {})",
                shown.min(self.leagues.len()),
                self.leagues.len()
            );
            group_table(
                &mut o,
                &self.leagues[..shown.min(self.leagues.len())],
                self.coverage.is_some(),
            );
        }

        if let Some(m) = &self.markets
            && m.by_type.iter().any(|(t, _, _)| t != "-")
        {
            let _ = writeln!(o, "\n== Markets by sportsMarketType (24h volume)");
            for (t, n, v) in m.by_type.iter().take(15) {
                let _ = writeln!(o, "{:<32} {:>7} markets {:>10}", t, n, usd(*v));
            }
        }

        let event_table = |o: &mut String, rows: &[EventRow]| {
            for e in rows {
                let place = e.league.clone().unwrap_or_else(|| e.category.clone());
                let state = match (&e.score, &e.period, e.live) {
                    (Some(s), Some(p), true) => format!("LIVE {s} ({p})"),
                    (Some(s), None, true) => format!("LIVE {s}"),
                    (_, _, true) => "LIVE".into(),
                    _ => when(e.start_time.as_deref(), Utc::now()),
                };
                let _ = writeln!(
                    o,
                    "{:>10} {:>10}  {:<10} {:<56} {}",
                    usd(e.volume_24h),
                    usd(e.liquidity),
                    trunc(&place, 10),
                    trunc(&e.title, 56),
                    state
                );
            }
        };
        let _ = writeln!(o, "\n== Top events by 24h volume");
        let _ = writeln!(
            o,
            "{:>10} {:>10}  {:<10} {:<56} state",
            "24h vol", "liquidity", "league", "event"
        );
        event_table(&mut o, &self.top_events);
        if !self.live_events.is_empty() {
            let _ = writeln!(o, "\n== Live now, by 24h volume");
            event_table(&mut o, &self.live_events);
        }

        if let Some(c) = &self.coverage {
            let share = if c.volume_24h_sports > 0.0 {
                100.0 * c.volume_24h_recorded / c.volume_24h_sports
            } else {
                0.0
            };
            let _ = writeln!(
                o,
                "\n== Recorder coverage (config leagues vs. sports + esports in scope)"
            );
            let _ = writeln!(
                o,
                "{} of {} configured leagues have open events in scope; they hold {} of {} 24h volume ({share:.0}%)",
                c.leagues_with_events,
                c.leagues_configured,
                usd(c.volume_24h_recorded),
                usd(c.volume_24h_sports)
            );
            if !c.biggest_unrecorded.is_empty() {
                let _ = writeln!(o, "largest leagues not recorded:");
                for g in &c.biggest_unrecorded {
                    let _ = writeln!(
                        o,
                        "  {:<8} {:<34} {:>10}  {} events",
                        g.key,
                        trunc(&g.name, 34),
                        usd(g.volume_24h),
                        g.events
                    );
                }
            }
        }
        o
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(json: serde_json::Value) -> Event {
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn categorises_and_summarises() {
        let events = vec![
            ev(
                serde_json::json!({"title": "A vs B", "slug": "a", "live": true, "volume24hr": 100.0, "volume": "1000",
                "tags": [{"slug": "sports"}, {"slug": "esports"}], "sport": {"sport": "cs2", "name": "Counter-Strike"},
                "markets": [{"active": true, "closed": false, "enableOrderBook": true, "sportsMarketType": "moneyline", "volume24hr": 90.0},
                            {"active": true, "closed": true}]}),
            ),
            ev(
                serde_json::json!({"title": "Election", "slug": "e", "volume24hr": 50, "tags": [{"slug": "elections"}]}),
            ),
            ev(serde_json::json!({"title": "Odd", "slug": "o", "volume24hr": null, "tags": []})),
        ];
        let opts = Options {
            category: None,
            leagues: vec![],
            live_only: false,
            with_markets: true,
            top: 5,
            recorded: Some(vec![("esports".into(), "cs2".into(), None)]),
        };
        let o = summarise(&events, &opts);
        assert_eq!((o.events, o.live, o.volume_24h, o.volume), (3, 1, 150.0, 1000.0));
        let names: Vec<&str> = o.categories.iter().map(|g| g.key.as_str()).collect();
        assert_eq!(names, ["esports", "politics", "other"]);
        assert_eq!(o.categories[0].markets, Some(1)); // the closed market is not counted
        assert_eq!(o.leagues[0].recorded, Some(true));
        let c = o.coverage.as_ref().unwrap();
        assert_eq!((c.volume_24h_recorded, c.volume_24h_sports), (100.0, 100.0));
        assert_eq!(o.markets.as_ref().unwrap().by_type[0], ("moneyline".into(), 1, 90.0));
        assert!(o.render().contains("LIVE"));
    }

    fn opts() -> Options {
        Options {
            category: None,
            leagues: vec![],
            live_only: false,
            with_markets: false,
            top: 15,
            recorded: None,
        }
    }

    fn sport_event(title: &str, league: &str, live: bool, vol: f64) -> Event {
        ev(
            serde_json::json!({"title": title, "slug": title, "live": live, "volume24hr": vol,
            "tags": [{"slug": "sports"}], "sport": {"sport": league, "name": league.to_uppercase()}}),
        )
    }

    #[test]
    fn categories_follow_tag_priority() {
        let cat = |tags: &[&str]| {
            let tags: Vec<serde_json::Value> = tags.iter().map(|t| serde_json::json!({"slug": t})).collect();
            ev(serde_json::json!({"tags": tags})).category()
        };
        assert_eq!(cat(&["games", "sports", "esports"]), "esports");
        assert_eq!(cat(&["soccer", "sports"]), "sports");
        assert_eq!(cat(&["elections", "trump"]), "politics");
        assert_eq!(cat(&["economy"]), "finance");
        assert_eq!(cat(&["pop-culture"]), "culture");
        assert_eq!(cat(&["recurring", "5M"]), "other");
        assert_eq!(cat(&[]), "other");
    }

    #[test]
    fn amounts_accept_numbers_strings_and_garbage() {
        let e = ev(serde_json::json!({"volume": "12.5", "volume24hr": 3, "liquidity": null, "openInterest": "n/a"}));
        assert_eq!(
            (e.volume, e.volume24hr, e.liquidity, e.open_interest),
            (12.5, 3.0, 0.0, 0.0)
        );
        // Unknown fields and missing ones never fail the event.
        let e = ev(serde_json::json!({"unexpected": {"deep": [1]}}));
        assert_eq!(e.title, "");
    }

    #[test]
    fn start_times_in_every_gamma_format() {
        let now = DateTime::parse_from_rfc3339("2026-10-17T12:00:00Z").unwrap().to_utc();
        assert_eq!(when(Some("2026-10-17T14:05:00Z"), now), "in 2h05m");
        assert_eq!(when(Some("2026-10-17 14:05:00+00"), now), "in 2h05m");
        assert_eq!(when(Some("2026-10-17 11:20:00+00:00"), now), "40m ago");
        assert_eq!(when(Some("2026-10-19T13:00:00Z"), now), "in 2d1h");
        assert_eq!(when(Some("tomorrow"), now), "");
        assert_eq!(when(None, now), "");
    }

    #[test]
    fn league_and_live_filters() {
        let events = || {
            vec![
                sport_event("a", "cs2", true, 1.0),
                sport_event("b", "CS2", false, 2.0),
                sport_event("c", "epl", true, 3.0),
                ev(serde_json::json!({"title": "no league", "live": true})),
            ]
        };
        let titles = |v: Vec<Event>| v.into_iter().map(|e| e.title).collect::<Vec<_>>();
        assert_eq!(titles(select(events(), &opts())).len(), 4);
        let cs2 = Options {
            leagues: vec!["cs2".into()],
            ..opts()
        };
        assert_eq!(titles(select(events(), &cs2)), ["a", "b"]);
        let live = Options {
            live_only: true,
            ..opts()
        };
        assert_eq!(titles(select(events(), &live)), ["a", "c", "no league"]);
        let both = Options {
            leagues: vec!["EPL".into(), "cs2".into()],
            live_only: true,
            ..opts()
        };
        assert_eq!(titles(select(events(), &both)), ["a", "c"]);
    }

    #[test]
    fn top_limits_lists_but_not_totals() {
        let events: Vec<Event> = (0..40)
            .map(|i| sport_event(&format!("e{i}"), &format!("l{i}"), i % 2 == 0, i as f64))
            .collect();
        let o = summarise(&events, &Options { top: 3, ..opts() });
        assert_eq!(o.events, 40);
        assert_eq!(o.live, 20);
        assert_eq!(
            o.top_events.iter().map(|e| e.title.as_str()).collect::<Vec<_>>(),
            ["e39", "e38", "e37"]
        );
        assert_eq!(
            o.live_events.iter().map(|e| e.title.as_str()).collect::<Vec<_>>(),
            ["e38", "e36", "e34"]
        );
        assert_eq!(o.leagues.len(), 40, "the JSON keeps every league");
        let text = o.render();
        assert!(text.contains("(top 6 of 40)"), "{text}");
        assert!(text.contains("l39 (L39)") && !text.contains("l30 (L30)"));
        assert!(o.coverage.is_none() && !text.contains("Recorder coverage"));
    }

    #[test]
    fn coverage_uses_score_league_aliases() {
        let events = vec![
            sport_event("r6", "r6siege", false, 10.0),
            sport_event("cs", "cs2", false, 30.0),
        ];
        let rec = |score: Option<&str>| Options {
            recorded: Some(vec![
                ("esports".into(), "cs2".into(), None),
                ("esports".into(), "r6seige".into(), score.map(String::from)),
            ]),
            ..opts()
        };
        let o = summarise(&events, &rec(None));
        let c = o.coverage.unwrap();
        assert_eq!((c.volume_24h_recorded, c.volume_24h_sports), (30.0, 40.0));
        assert_eq!(c.biggest_unrecorded[0].key, "r6siege");
        let o = summarise(&events, &rec(Some("r6siege")));
        assert_eq!(o.coverage.unwrap().volume_24h_recorded, 40.0);
    }

    #[test]
    fn json_has_no_render_only_fields() {
        let o = summarise(&[sport_event("a", "cs2", true, 1.0)], &opts());
        let v = serde_json::to_value(&o).unwrap();
        assert!(v.get("top").is_none());
        assert_eq!(v["events"], 1);
        assert_eq!(
            v["markets"],
            serde_json::Value::Null,
            "no market stats without --markets"
        );
    }
}
