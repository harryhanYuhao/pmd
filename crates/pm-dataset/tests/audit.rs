//! `pm audit` end to end over a synthetic recording that contains every kind of problem the
//! report is meant to surface.

mod common;

use std::time::Duration;

use common::*;
use pm_dataset::audit::{Options, Report, run};
use pm_store::read::Filter;

const CS2: &str = "esports/cs2";

/// Run 100: 10:00–12:20 with a 30 min clob_book silence, a 28 min silence of every feed, a
/// 10 s CLOB reconnect, a CLOB shard that never came back, a truncated and a misplaced record.
/// Run 200 starts 100 min after run 100 ended; run 300 overlaps run 200.
fn build() -> Fixture {
    let f = Fixture::new("audit");
    let m = |t| {
        (
            t,
            1u64,
            "{\"event_type\":\"price_change\",\"market\":\"0xm1\",\"price_changes\":[{\"asset_id\":\"11\"},{\"asset_id\":\"12\"}]}",
        )
    };
    f.day(
        CS2,
        "clob_book",
        100,
        &[
            m(at(10, 0, 0)),
            m(at(10, 5, 0)),
            m(at(10, 10, 0)),
            m(at(10, 40, 0)),
            m(at(10, 50, 0)),
            m(at(10, 59, 0)),
        ],
    );
    f.day(
        CS2,
        "clob_book",
        100,
        &[m(at(11, 0, 0)), m(at(11, 10, 0)), m(at(11, 20, 0)), m(at(11, 30, 0))],
    );
    // 11:58 sits in the 12:00 file: more than a minute outside its hour.
    f.write(
        "2026/10/17",
        CS2,
        "clob_book",
        12,
        100,
        &[
            rec(
                at(11, 58, 0),
                1,
                "{\"event_type\":\"book\",\"market\":\"0xm2\",\"asset_id\":\"21\"}",
            ),
            rec(
                at(12, 10, 0),
                1,
                "{\"event_type\":\"book\",\"market\":\"0xm2\",\"asset_id\":\"21\"}",
            ),
            rec(
                at(12, 20, 0),
                1,
                "{\"event_type\":\"book\",\"market\":\"0xm2\",\"asset_id\":\"21\"}",
            ),
        ],
    );

    let mut bbo_lines: Vec<String> = (0..12)
        .map(|i| rec(at(10, i * 5, 0), 2, &bbo("11", "0.4", "0.6")))
        .collect();
    bbo_lines.insert(3, "garbage\n".into());
    f.write("2026/10/17", CS2, "clob_bbo", 10, 100, &bbo_lines);

    // Crash mid-write: the first frame is intact, the second is cut short.
    let p = f.path("2026/10/17", CS2, "clob_trades", 11, 100);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    let mut bytes = frame(&[rec(at(11, 5, 0), 3, &clob_trade("11", "0.5", "10"))]);
    bytes.extend(frame(&[rec(at(11, 15, 0), 3, &clob_trade("11", "0.5", "10"))]));
    std::fs::write(&p, &bytes[..bytes.len() - 3]).unwrap();
    age(&p);

    f.day(
        CS2,
        "trades",
        100,
        &[
            (at(10, 1, 0), 4, &wallet_trade("11", 0.5, 10.0, "0xw1", "A vs B")),
            (at(10, 2, 0), 4, &wallet_trade("12", 0.25, 4.0, "0xw2", "A vs B")),
        ],
    );
    f.day(
        CS2,
        "scores",
        100,
        &[
            (at(10, 3, 0), 5, &score_numeric(5, "1-0")),
            (at(10, 4, 0), 5, &score_numeric(5, "2-0")),
        ],
    );
    f.day(
        "other/cricket",
        "scores",
        100,
        &[(at(10, 30, 0), 5, &score_cricket("idX", "65-1"))],
    );
    f.day(
        CS2,
        "markets",
        100,
        &[
            (at(10, 0, 0), 0, &market("0xm1", ["11", "12"], "A vs B", true)),
            (at(10, 0, 1), 0, &market("0xm9", ["91", "92"], "Prop", false)),
        ],
    );
    f.day(
        CS2,
        "gamma_events",
        100,
        &[(at(10, 0, 0), 0, &gamma_event("7", "A vs B", true, 1000.0))],
    );

    let sys = |feed: &str| format!("_system/{feed}");
    f.day(
        &sys("clob"),
        "sessions",
        100,
        &[
            (at(10, 0, 0), 1, &session("clob-0", true, "")),
            (at(10, 0, 0), 2, &session("clob-1", true, "")),
            (at(10, 20, 0), 1, &session("clob-0", false, "idle for 45s")),
            (at(10, 20, 10), 6, &session("clob-0", true, "")),
        ],
    );
    f.day(
        &sys("clob"),
        "sessions",
        100,
        &[(at(12, 0, 0), 2, &session("clob-1", false, "stream ended"))],
    );
    f.day(
        &sys("rtds"),
        "sessions",
        100,
        &[(at(10, 0, 0), 4, &session("rtds", true, ""))],
    );
    f.day(
        &sys("rtds"),
        "sessions",
        100,
        &[(at(12, 20, 0), 4, &session("rtds", false, "shutdown"))],
    );

    f.day(CS2, "clob_book", 200, &[m(at(14, 0, 0)), m(at(14, 1, 0))]);
    f.day("esports/lol", "clob_book", 300, &[m(at(14, 0, 30)), m(at(14, 0, 40))]);

    f.raw("clob_book/cs2/2026-10-16/18.r1.jsonl.zst", &frame(&[]));
    f.raw("clob_book/cs2/2026-10-16/19.r1.jsonl.zst", &frame(&[]));
    f
}

fn audit(f: &Fixture, filter: Filter) -> Report {
    run(&Options {
        root: f.root.clone(),
        filter,
        gap: Duration::from_secs(15 * 60),
        top: 20,
    })
    .unwrap()
}

#[test]
fn reports_runs_with_pauses_and_overlaps() {
    let f = build();
    let r = audit(&f, Filter::default());
    let runs: Vec<i64> = r.runs.iter().map(|x| x.run).collect();
    assert_eq!(runs, [100, 200, 300]);
    assert_eq!(r.runs[0].first, "2026-10-17 10:00:00Z");
    assert_eq!(r.runs[0].last, "2026-10-17 12:20:00Z");
    assert_eq!(r.runs[0].gap_before_s, None);
    assert_eq!(r.runs[1].gap_before_s, Some(100.0 * 60.0));
    assert_eq!(
        r.runs[2].gap_before_s,
        Some(-30.0),
        "run 300 starts 30 s before run 200 ends"
    );
    assert!(r.render().contains("OVERLAPS previous by 30s"));
}

#[test]
fn replays_connections_into_drops_and_outages() {
    let f = build();
    let r = audit(&f, Filter::default());
    let clob = r.feeds.iter().find(|x| x.feed == "clob").unwrap();
    assert_eq!((clob.shards, clob.connections, clob.drops), (2, 3, 2));
    assert_eq!(clob.downtime_s, 10.0 + 20.0 * 60.0);
    assert_eq!(clob.longest_s, 20.0 * 60.0);
    let rtds = r.feeds.iter().find(|x| x.feed == "rtds").unwrap();
    assert_eq!(
        (rtds.drops, rtds.downtime_s),
        (0, 0.0),
        "a clean shutdown is not a drop"
    );

    // Longest first; the shard that never reconnected lasts until the run's last record.
    assert_eq!(r.outages.len(), 2);
    assert_eq!((r.outages[0].feed.as_str(), r.outages[0].recovered), ("clob-1", false));
    assert_eq!(r.outages[0].to, "2026-10-17 12:20:00Z");
    assert_eq!((r.outages[1].feed.as_str(), r.outages[1].secs), ("clob-0", 10.0));
    assert!(r.render().contains("(run ended)"));
}

#[test]
fn finds_recording_gaps_and_stream_silences() {
    let f = build();
    let r = audit(&f, Filter::default());
    // Nothing at all was written between 11:30 and 11:58 in run 100.
    assert_eq!(r.recording_gaps.len(), 1, "{:#?}", r.recording_gaps);
    let g = &r.recording_gaps[0];
    assert_eq!(
        (g.run, g.from.as_str(), g.to.as_str(), g.secs),
        (100, "2026-10-17 11:30:00Z", "2026-10-17 11:58:00Z", 28.0 * 60.0)
    );

    // clob_book alone was also quiet 10:10–10:40 (clob_bbo kept writing). Longest first.
    let book: Vec<(f64, &str)> = r
        .stream_gaps
        .iter()
        .filter(|g| g.partition == CS2 && g.stream == "clob_book")
        .map(|g| (g.secs / 60.0, g.from.as_str()))
        .collect();
    assert_eq!(book, [(30.0, "2026-10-17 10:10:00Z"), (28.0, "2026-10-17 11:30:00Z")]);
    // Unconfigured leagues (scores only) and metadata streams never count as silent.
    assert!(
        r.stream_gaps
            .iter()
            .all(|g| !g.partition.starts_with("other/") && g.stream != "markets")
    );
}

#[test]
fn reports_damage_misplaced_records_and_foreign_files() {
    let f = build();
    let r = audit(&f, Filter::default());
    let i = &r.integrity;
    assert_eq!(i.damaged.len(), 1, "{:#?}", i.damaged);
    assert!(i.damaged[0].path.ends_with("clob_trades/11.r100.jsonl.zst"));
    assert!(!i.damaged[0].open);
    assert_eq!(i.bad_lines, 1);
    assert_eq!(i.out_of_hour, 1);
    assert_eq!(i.unrecognised.len(), 2);
    let text = r.render();
    assert!(text.contains("unrecognised: 2 files under clob_book/"), "{text}");
    // The intact frame of the damaged file still counts.
    let trades = r.partition_streams.iter().find(|s| s.stream == "clob_trades").unwrap();
    assert_eq!(trades.records, 1);
}

#[test]
fn summarises_content_per_league() {
    let f = build();
    let r = audit(&f, Filter::default());
    let cs2 = r.content.iter().find(|c| c.partition == CS2).unwrap();
    assert_eq!(
        (
            cs2.markets_tracked,
            cs2.markets_with_clob_data,
            cs2.tokens_with_clob_data
        ),
        (1, 2, 3)
    );
    assert_eq!(cs2.clob_events["price_change"], 12);
    assert_eq!(cs2.clob_events["book"], 3);
    assert_eq!(cs2.clob_events["best_bid_ask"], 12);
    assert_eq!((cs2.wallet_trades, cs2.wallets, cs2.notional), (2, 2, 6.0));
    assert_eq!((cs2.score_updates, cs2.games, cs2.events), (2, 1, 1));
    assert_eq!(cs2.unparsed, 0);
    let cricket = r.content.iter().find(|c| c.partition == "other/cricket").unwrap();
    assert_eq!((cricket.score_updates, cricket.games, cricket.unparsed), (1, 1, 0));
    assert!(r.content.iter().all(|c| !c.partition.starts_with("_system")));
    assert!(
        r.render().contains("other/*"),
        "unconfigured leagues are folded in the text report"
    );
}

#[test]
fn totals_add_up_and_json_round_trips() {
    let f = build();
    let r = audit(&f, Filter::default());
    let sum: u64 = r.partition_streams.iter().map(|s| s.records).sum();
    assert_eq!(r.totals.records, sum);
    assert_eq!(r.days.len(), 1);
    assert_eq!(r.days[0].records, r.totals.records);
    assert_eq!(r.totals.runs, 3);
    let v: serde_json::Value = serde_json::to_value(&r).unwrap();
    assert_eq!(v["totals"]["runs"], 3);
    assert_eq!(v["gap_threshold_s"], 900.0);
}

#[test]
fn filters_select_data_but_connections_are_always_read() {
    let f = build();
    let r = audit(
        &f,
        Filter {
            leagues: vec!["lol".into()],
            ..Default::default()
        },
    );
    let parts: Vec<&str> = r.partitions.iter().map(|p| p.partition.as_str()).collect();
    assert!(parts.contains(&"esports/lol"));
    assert!(!parts.contains(&CS2));
    assert!(
        r.feeds.iter().any(|x| x.feed == "clob" && x.drops == 2),
        "sessions are read whatever the filter"
    );

    let day = chrono::NaiveDate::from_ymd_opt(2026, 10, 18).unwrap();
    let empty = audit(
        &f,
        Filter {
            from: Some(day),
            ..Default::default()
        },
    );
    assert_eq!((empty.totals.files, empty.runs.len()), (0, 0));
    assert!(empty.render().contains("0 records in 0 files"));
}

#[test]
fn empty_and_missing_roots() {
    let f = Fixture::new("audit-empty");
    let r = audit(&f, Filter::default());
    assert_eq!(r.totals.records, 0);
    assert!(r.feeds.is_empty());
    let err = run(&Options {
        root: f.root.join("does-not-exist"),
        filter: Filter::default(),
        gap: Duration::from_secs(60),
        top: 5,
    });
    assert!(err.is_err());
}
