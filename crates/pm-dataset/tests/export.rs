//! `pm export` end to end: synthetic recordings in, Parquet read back and checked.

mod common;

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, TimestampNanosecondType};
use arrow_array::{Array, RecordBatch};
use common::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use pm_dataset::export::{Options, Summary, Table, run};
use pm_store::read::Filter;

const CS2: &str = "esports/cs2";

fn opts(f: &Fixture) -> Options {
    Options {
        root: f.root.clone(),
        out: f.out(),
        filter: Filter::default(),
        tables: Table::ALL.to_vec(),
        dims: true,
        include_today: false,
        force: false,
    }
}

fn part(f: &Fixture, table: &str, league: &str) -> PathBuf {
    f.out()
        .join(table)
        .join("date=2026-10-17")
        .join("category=esports")
        .join(format!("league={league}"))
        .join("part.parquet")
}

fn read(p: &Path) -> Vec<RecordBatch> {
    ParquetRecordBatchReaderBuilder::try_new(File::open(p).unwrap_or_else(|e| panic!("{}: {e}", p.display())))
        .unwrap()
        .build()
        .unwrap()
        .map(|b| b.unwrap())
        .collect()
}

fn rows(p: &Path) -> usize {
    read(p).iter().map(|b| b.num_rows()).sum()
}

fn f64s(p: &Path, col: &str) -> Vec<Option<f64>> {
    read(p)
        .iter()
        .flat_map(|b| {
            let a = b.column_by_name(col).unwrap().as_primitive::<Float64Type>();
            (0..a.len())
                .map(|i| a.is_valid(i).then(|| a.value(i)))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn strs(p: &Path, col: &str) -> Vec<Option<String>> {
    read(p)
        .iter()
        .flat_map(|b| {
            let a = b.column_by_name(col).unwrap().as_string::<i32>();
            (0..a.len())
                .map(|i| a.is_valid(i).then(|| a.value(i).to_string()))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn times(p: &Path) -> Vec<i64> {
    read(p)
        .iter()
        .flat_map(|b| {
            b.column_by_name("t")
                .unwrap()
                .as_primitive::<TimestampNanosecondType>()
                .values()
                .to_vec()
        })
        .collect()
}

fn ok(s: &Summary) {
    assert!(s.failed.is_empty(), "failed units: {:?}", s.failed);
}

#[test]
fn a_column_may_gain_nulls_in_a_later_hour() {
    // Hour 10 has every best_ask; hour 11 has an empty ask side (null). Both must land in one file.
    let f = Fixture::new("nulls");
    f.day(CS2, "clob_bbo", 1, &[(at(10, 0, 0), 1, &bbo("11", "0.4", "0.6"))]);
    f.day(CS2, "clob_bbo", 1, &[(at(11, 0, 0), 1, &bbo("11", "0.4", ""))]);
    let s = run(&opts(&f)).unwrap();
    ok(&s);
    assert_eq!(f64s(&part(&f, "bbo", "cs2"), "best_ask"), [Some(0.6), None]);
}

#[test]
fn rows_are_sorted_across_runs_and_files() {
    let f = Fixture::new("sorted");
    f.day(
        CS2,
        "clob_bbo",
        1,
        &[
            (at(10, 0, 3), 1, &bbo("11", "0.3", "0.9")),
            (at(10, 0, 1), 1, &bbo("11", "0.1", "0.9")),
        ],
    );
    f.day(CS2, "clob_bbo", 2, &[(at(10, 0, 2), 7, &bbo("11", "0.2", "0.9"))]);
    f.day(CS2, "clob_bbo", 2, &[(at(11, 0, 0), 7, &bbo("11", "0.4", "0.9"))]);
    ok(&run(&opts(&f)).unwrap());
    let p = part(&f, "bbo", "cs2");
    assert_eq!(f64s(&p, "best_bid"), [Some(0.1), Some(0.2), Some(0.3), Some(0.4)]);
    let t = times(&p);
    assert!(t.windows(2).all(|w| w[0] <= w[1]));
}

#[test]
fn book_updates_replay_to_the_servers_top_of_book() {
    let f = Fixture::new("replay");
    let recs = [
        (
            at(10, 0, 0),
            1,
            book("11", &[("0.40", "10"), ("0.39", "5")], &[("0.42", "7")]),
        ),
        (at(10, 0, 1), 1, change("11", "BUY", "0.41", "3", "0.41", "0.42")),
        (at(10, 0, 2), 1, change("11", "SELL", "0.43", "2", "0.41", "0.42")),
        (at(10, 0, 3), 1, change("11", "SELL", "0.42", "0", "0.41", "0.43")),
        // A new snapshot replaces everything, including the ask side, which is now empty.
        (at(10, 0, 4), 1, book("11", &[("0.30", "1")], &[])),
        (at(10, 0, 5), 1, book("11", &[], &[])),
    ];
    let recs: Vec<(i64, u64, &str)> = recs.iter().map(|(t, s, m)| (*t, *s, m.as_str())).collect();
    f.day(CS2, "clob_book", 1, &recs);
    ok(&run(&opts(&f)).unwrap());

    let p = part(&f, "book_updates", "cs2");
    let kind = strs(&p, "kind");
    let side = strs(&p, "side");
    let price = f64s(&p, "price");
    let size = f64s(&p, "size");
    let (bb, ba) = (f64s(&p, "best_bid"), f64s(&p, "best_ask"));
    let t = times(&p);
    assert_eq!(
        t.len(),
        3 + 3 + 1 + 1,
        "snapshot levels + changes + one empty-snapshot marker"
    );

    // The documented replay rule.
    let (mut bids, mut asks) = (BTreeMap::<i64, f64>::new(), BTreeMap::<i64, f64>::new());
    let key = |p: f64| (p * 1e6).round() as i64;
    let mut last_snap_t = None;
    for i in 0..t.len() {
        if kind[i].as_deref() == Some("snapshot") {
            if last_snap_t != Some(t[i]) {
                bids.clear();
                asks.clear();
                last_snap_t = Some(t[i]);
            }
            match side[i].as_deref() {
                Some("bid") => drop(bids.insert(key(price[i].unwrap()), size[i].unwrap())),
                Some("ask") => drop(asks.insert(key(price[i].unwrap()), size[i].unwrap())),
                _ => assert!(price[i].is_none() && size[i].is_none()),
            }
            continue;
        }
        last_snap_t = None;
        let book = if side[i].as_deref() == Some("bid") {
            &mut bids
        } else {
            &mut asks
        };
        if size[i] == Some(0.0) {
            book.remove(&key(price[i].unwrap()));
        } else {
            book.insert(key(price[i].unwrap()), size[i].unwrap());
        }
        assert_eq!(
            bids.keys().next_back().copied(),
            bb[i].map(key),
            "best bid after row {i}"
        );
        assert_eq!(asks.keys().next().copied(), ba[i].map(key), "best ask after row {i}");
    }
    assert!(
        bids.is_empty() && asks.is_empty(),
        "the empty snapshot cleared the book"
    );
}

#[test]
fn every_table_has_its_documented_columns() {
    let f = Fixture::new("columns");
    f.day(CS2, "clob_book", 1, &[(at(10, 0, 0), 1, &book("11", &[("0.4", "1")], &[])), (at(10, 0, 1), 1, r#"{"market":"0xm1","asset_id":"11","old_tick_size":"0.01","new_tick_size":"0.001","timestamp":"5","event_type":"tick_size_change"}"#)]);
    f.day(CS2, "clob_bbo", 1, &[(at(10, 0, 0), 1, &bbo("11", "0.4", "0.6"))]);
    f.day(
        CS2,
        "clob_trades",
        1,
        &[(at(10, 0, 0), 1, &clob_trade("11", "0.5", "10"))],
    );
    f.day(
        CS2,
        "trades",
        1,
        &[(at(10, 0, 0), 2, &wallet_trade("11", 0.5, 10.0, "0xw", "Say \"hi\" é"))],
    );
    f.day(CS2, "book_snapshots", 1, &[(at(10, 0, 0), 0, r#"{"asks":[["0.96","15.16"]],"asset_id":"11","bids":[["0.04","1"],["0.03","2"]],"market":"0xm1","n_asks":1,"n_bids":2,"ts":1792222867230}"#)]);
    f.day(CS2, "scores", 1, &[(at(10, 0, 0), 3, &score_numeric(5, "1-0|0-0|Bo3"))]);
    f.day(
        CS2,
        "markets",
        1,
        &[(at(10, 0, 0), 0, &market("0xm1", ["11", "12"], "A vs B", true))],
    );
    f.day(
        CS2,
        "gamma_events",
        1,
        &[(at(10, 0, 0), 0, &gamma_event("7", "A vs B", false, 5.0))],
    );
    f.day(
        "_system/clob",
        "sessions",
        1,
        &[(at(10, 0, 0), 9, &session("clob-0", true, ""))],
    );
    let s = run(&opts(&f)).unwrap();
    ok(&s);

    let cols = |p: &Path| -> Vec<String> { read(p)[0].schema().fields().iter().map(|f| f.name().clone()).collect() };
    let want: [(&str, &[&str]); 7] = [
        (
            "book_updates",
            &[
                "t", "session", "ts", "market", "asset_id", "kind", "side", "price", "size", "best_bid", "best_ask",
            ],
        ),
        (
            "tick_size_changes",
            &["t", "session", "ts", "market", "asset_id", "old_tick", "new_tick"],
        ),
        (
            "bbo",
            &[
                "t", "session", "ts", "market", "asset_id", "best_bid", "best_ask", "spread",
            ],
        ),
        (
            "clob_trades",
            &[
                "t",
                "session",
                "ts",
                "market",
                "asset_id",
                "price",
                "size",
                "side",
                "fee_rate_bps",
                "tx_hash",
            ],
        ),
        (
            "wallet_trades",
            &[
                "t",
                "session",
                "ts",
                "chain_ts",
                "market",
                "asset_id",
                "event_slug",
                "slug",
                "title",
                "outcome",
                "outcome_index",
                "price",
                "size",
                "notional",
                "side",
                "wallet",
                "name",
                "pseudonym",
                "tx_hash",
            ],
        ),
        (
            "book_snapshots",
            &[
                "t", "ts", "market", "asset_id", "side", "level", "price", "size", "n_bids", "n_asks",
            ],
        ),
        (
            "scores",
            &[
                "t",
                "session",
                "game_id",
                "metadata_game_id",
                "league_abbr",
                "home",
                "away",
                "status",
                "score",
                "period",
                "live",
                "ended",
            ],
        ),
    ];
    for (table, want) in want {
        assert_eq!(cols(&part(&f, table, "cs2")), want, "{table}");
    }
    let dims: [(&str, usize); 4] = [("markets", 25), ("tokens", 9), ("events", 23), ("sessions", 7)];
    for (d, n) in dims {
        assert_eq!(cols(&f.out().join(format!("{d}.parquet"))).len(), n, "{d}");
    }

    // Values survive: escaped strings, notional, snapshot levels best first.
    let wt = part(&f, "wallet_trades", "cs2");
    assert_eq!(strs(&wt, "title"), [Some("Say \"hi\" é".to_string())]);
    assert_eq!(f64s(&wt, "notional"), [Some(5.0)]);
    let snap = part(&f, "book_snapshots", "cs2");
    assert_eq!(f64s(&snap, "price"), [Some(0.04), Some(0.03), Some(0.96)]);
    assert_eq!(
        strs(&snap, "side"),
        [Some("bid".into()), Some("bid".into()), Some("ask".into())]
    );
    let tok = f.out().join("tokens.parquet");
    assert_eq!(strs(&tok, "asset_id"), [Some("11".into()), Some("12".into())]);
    assert_eq!(strs(&tok, "league"), [Some("cs2".into()), Some("cs2".into())]);
}

#[test]
fn dimensions_keep_the_latest_record_and_first_seen() {
    let f = Fixture::new("dims");
    f.day(
        CS2,
        "markets",
        1,
        &[(at(10, 0, 0), 0, &market("0xm1", ["11", "12"], "old question", true))],
    );
    // Another day and run: the latest record wins, first_seen stays.
    let later = rec(
        at(10, 0, 0) + 86_400 * SEC,
        0,
        &market("0xm1", ["11", "12"], "new question", false),
    );
    f.write("2026/10/18", CS2, "markets", 10, 2, &[later]);
    f.day(
        CS2,
        "gamma_events",
        1,
        &[
            (at(10, 0, 0), 0, &gamma_event("7", "A vs B", true, 5.0)),
            (at(11, 0, 0), 0, &gamma_event("7", "A vs B", false, 9.0)),
        ],
    );
    ok(&run(&opts(&f)).unwrap());
    let m = f.out().join("markets.parquet");
    assert_eq!(strs(&m, "question"), [Some("new question".into())]);
    let first = read(&m)[0]
        .column_by_name("first_seen")
        .unwrap()
        .as_primitive::<TimestampNanosecondType>()
        .value(0);
    assert_eq!(first, at(10, 0, 0));
    assert_eq!(f64s(&f.out().join("events.parquet"), "volume"), [Some(9.0)]);
}

#[test]
fn reruns_skip_unchanged_units_and_rebuild_changed_ones() {
    let f = Fixture::new("incremental");
    f.day(CS2, "clob_bbo", 1, &[(at(10, 0, 0), 1, &bbo("11", "0.4", "0.6"))]);
    f.day(
        "esports/lol",
        "clob_bbo",
        1,
        &[(at(10, 0, 0), 1, &bbo("11", "0.4", "0.6"))],
    );
    let s = run(&opts(&f)).unwrap();
    assert_eq!((s.units, s.skipped), (2, 0));
    let s = run(&opts(&f)).unwrap();
    assert_eq!((s.units, s.skipped), (0, 2));

    f.day(
        CS2,
        "clob_bbo",
        1,
        &[
            (at(10, 0, 0), 1, &bbo("11", "0.4", "0.6")),
            (at(10, 0, 1), 1, &bbo("11", "0.5", "0.6")),
        ],
    );
    let s = run(&opts(&f)).unwrap();
    assert_eq!((s.units, s.skipped, s.rows["bbo"]), (1, 1, 2));

    // A deleted output is rebuilt even though its inputs did not change.
    fs::remove_file(part(&f, "bbo", "lol")).unwrap();
    assert_eq!(run(&opts(&f)).unwrap().units, 1);
    // --force rebuilds everything.
    assert_eq!(
        run(&Options {
            force: true,
            ..opts(&f)
        })
        .unwrap()
        .units,
        2
    );
}

#[test]
fn a_new_schema_version_rebuilds_everything() {
    let f = Fixture::new("version");
    f.day(CS2, "clob_bbo", 1, &[(at(10, 0, 0), 1, &bbo("11", "0.4", "0.6"))]);
    ok(&run(&opts(&f)).unwrap());
    let man = f.out().join("_manifest.json");
    let text = fs::read_to_string(&man).unwrap();
    let mut v: serde_json::Value = serde_json::from_str(&text).unwrap();
    v["version"] = 0.into();
    fs::write(&man, v.to_string()).unwrap();
    assert_eq!(run(&opts(&f)).unwrap().units, 1);
    // A corrupt manifest is not fatal either: everything is rebuilt.
    fs::write(&man, "{not json").unwrap();
    assert_eq!(run(&opts(&f)).unwrap().units, 1);
}

#[test]
fn a_table_that_loses_its_rows_loses_its_file() {
    let f = Fixture::new("stale");
    f.day(CS2, "clob_book", 1, &[(at(10, 0, 0), 1, r#"{"market":"0xm1","asset_id":"11","old_tick_size":"0.01","new_tick_size":"0.001","timestamp":"5","event_type":"tick_size_change"}"#)]);
    ok(&run(&opts(&f)).unwrap());
    assert!(part(&f, "tick_size_changes", "cs2").is_file());
    f.day(
        CS2,
        "clob_book",
        1,
        &[(at(10, 0, 0), 1, &book("11", &[("0.4", "1")], &[]))],
    );
    ok(&run(&opts(&f)).unwrap());
    assert!(!part(&f, "tick_size_changes", "cs2").exists());
    assert_eq!(rows(&part(&f, "book_updates", "cs2")), 1);
}

#[test]
fn today_is_skipped_unless_asked_for() {
    let f = Fixture::new("today");
    let now = chrono::Utc::now();
    let date = now.format("%Y/%m/%d").to_string();
    let t = now.timestamp_nanos_opt().unwrap();
    let hour: u32 = now.format("%H").to_string().parse().unwrap();
    f.write(&date, CS2, "clob_bbo", hour, 1, &[rec(t, 1, &bbo("11", "0.4", "0.6"))]);
    let s = run(&opts(&f)).unwrap();
    assert_eq!((s.units, s.skipped_today), (0, 1));
    let s = run(&Options {
        include_today: true,
        ..opts(&f)
    })
    .unwrap();
    assert_eq!((s.units, s.rows["bbo"]), (1, 1));
}

#[test]
fn the_stream_filter_limits_what_is_read() {
    let f = Fixture::new("streams");
    f.day(CS2, "clob_bbo", 1, &[(at(10, 0, 0), 1, &bbo("11", "0.4", "0.6"))]);
    f.day(CS2, "scores", 1, &[(at(10, 0, 0), 1, &score_numeric(5, "1-0"))]);
    let s = run(&Options {
        filter: Filter {
            streams: vec!["scores".into()],
            ..Default::default()
        },
        ..opts(&f)
    })
    .unwrap();
    assert_eq!(s.units, 1, "only the scores unit was selected");
    assert!(!part(&f, "bbo", "cs2").exists());
    assert!(part(&f, "scores", "cs2").is_file());
}

#[test]
fn dimension_tables_alone_read_no_time_series() {
    let f = Fixture::new("dims-only");
    f.day(CS2, "clob_bbo", 1, &[(at(10, 0, 0), 1, &bbo("11", "0.4", "0.6"))]);
    f.day(
        CS2,
        "markets",
        1,
        &[(at(10, 0, 0), 0, &market("0xm1", ["11", "12"], "A vs B", true))],
    );
    let s = run(&Options {
        tables: vec![],
        ..opts(&f)
    })
    .unwrap();
    assert_eq!((s.units, s.input_bytes), (0, 0), "no time-series unit is read");
    assert_eq!(s.rows.get("tokens"), Some(&2));
    assert!(!f.out().join("bbo").exists());
    // And the other way round: no dimensions requested, none written.
    let g = Fixture::new("no-dims");
    g.day(
        CS2,
        "markets",
        1,
        &[(at(10, 0, 0), 0, &market("0xm1", ["11", "12"], "A vs B", true))],
    );
    run(&Options {
        dims: false,
        ..opts(&g)
    })
    .unwrap();
    assert!(!g.out().join("markets.parquet").exists());
}

#[test]
fn one_bad_unit_does_not_stop_the_others() {
    use std::os::unix::fs::PermissionsExt;
    let f = Fixture::new("failure");
    let bad = f.day(CS2, "clob_bbo", 1, &[(at(10, 0, 0), 1, &bbo("11", "0.4", "0.6"))]);
    f.day(
        "esports/lol",
        "clob_bbo",
        1,
        &[(at(10, 0, 0), 1, &bbo("11", "0.4", "0.6"))],
    );
    fs::set_permissions(&bad, fs::Permissions::from_mode(0o000)).unwrap();
    if File::open(&bad).is_ok() {
        return; // running as root: permissions are not enforced
    }
    let s = run(&opts(&f)).unwrap();
    assert_eq!(s.failed, ["clob_bbo/2026-10-17/esports/cs2"]);
    assert_eq!(s.units, 1);
    assert!(part(&f, "bbo", "lol").is_file());
    // The failed unit is retried on the next run, once readable.
    fs::set_permissions(&bad, fs::Permissions::from_mode(0o644)).unwrap();
    let s = run(&opts(&f)).unwrap();
    assert_eq!((s.units, s.skipped), (1, 1));
}

#[test]
fn damaged_input_exports_what_is_readable() {
    let f = Fixture::new("damaged");
    let p = f.path("2026/10/17", CS2, "clob_bbo", 10, 1);
    fs::create_dir_all(p.parent().unwrap()).unwrap();
    let mut bytes = frame(&[rec(at(10, 0, 0), 1, &bbo("11", "0.4", "0.6")), "garbage\n".into()]);
    bytes.extend(frame(&[rec(at(10, 0, 1), 1, &bbo("11", "0.5", "0.6"))]));
    fs::write(&p, &bytes[..bytes.len() - 3]).unwrap();
    // A payload of the right stream but the wrong shape.
    f.day(
        CS2,
        "clob_bbo",
        2,
        &[(
            at(10, 0, 2),
            1,
            r#"{"event_type":"best_bid_ask","best_bid":{"nested":1}}"#,
        )],
    );
    let s = run(&opts(&f)).unwrap();
    ok(&s);
    assert_eq!(s.rows["bbo"], 1);
    assert_eq!(s.truncated_files, 1);
    assert_eq!(s.unparsed, 2, "the garbage line and the bad payload");
}

#[test]
fn interrupted_exports_leave_no_partial_files() {
    let f = Fixture::new("tmp");
    f.day(CS2, "clob_bbo", 1, &[(at(10, 0, 0), 1, &bbo("11", "0.4", "0.6"))]);
    // A temp file left by an export that was killed.
    let p = part(&f, "bbo", "cs2");
    fs::create_dir_all(p.parent().unwrap()).unwrap();
    fs::write(p.with_extension("parquet.tmp"), b"half a file").unwrap();
    ok(&run(&opts(&f)).unwrap());
    assert_eq!(rows(&p), 1);
    assert!(!p.with_extension("parquet.tmp").exists());
}

/// The export reads what the real recorder writer produces.
#[tokio::test]
async fn reads_what_the_store_writer_wrote() {
    use pm_store::{Partition, Store, StoreConfig};
    let f = Fixture::new("writer");
    let store = Store::start(StoreConfig {
        root: f.root.clone(),
        ..Default::default()
    })
    .unwrap();
    let h = store.handle();
    let cs2 = Partition::new("esports", "cs2");
    for i in 0..1000 {
        let t = at(10, 59, 0) + i * 100_000_000; // crosses into hour 11
        h.write(&cs2, "clob_bbo", t, 1, &bbo("11", &format!("0.{}", i % 9 + 1), "0.99"))
            .await;
    }
    h.write(
        &cs2,
        "trades",
        at(10, 0, 0),
        2,
        &wallet_trade("11", 0.5, 2.0, "0xw", "x"),
    )
    .await;
    drop(h);
    store.shutdown().await.unwrap();

    let opts = opts(&f);
    let s = tokio::task::spawn_blocking(move || run(&opts)).await.unwrap().unwrap();
    ok(&s);
    assert_eq!((s.rows["bbo"], s.rows["wallet_trades"]), (1000, 1));
    let t = times(&part(&f, "bbo", "cs2"));
    assert_eq!((t[0], t[999]), (at(10, 59, 0), at(10, 59, 0) + 999 * 100_000_000));
}
