//! The `pm` binary end to end: arguments, exit codes, what goes to stdout vs stderr, defaults.
//! Tests that need the network are `#[ignore]`d: `cargo test -p pm -- --ignored` runs them.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const BIN: &str = env!("CARGO_BIN_EXE_pm");
/// 2026-10-17 10:00:00 UTC, ns.
const T10: i64 = 1_792_231_200 * 1_000_000_000;

struct Dir(PathBuf);

impl Dir {
    fn new(name: &str) -> Dir {
        let d = std::env::temp_dir().join(format!("pm-cli-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        Dir(d)
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn pm(args: &[&str], cwd: &Path) -> Output {
    Command::new(BIN).args(args).current_dir(cwd).output().unwrap()
}

fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

fn stderr(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

/// A tiny recording under `<dir>/data/live`.
fn record(dir: &Path) -> PathBuf {
    let root = dir.join("data/live");
    let write = |rel: &str, lines: &[String]| {
        let p = root.join(rel);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        let mut enc = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
        for l in lines {
            enc.write_all(l.as_bytes()).unwrap();
        }
        fs::write(p, enc.finish().unwrap()).unwrap();
    };
    let bbo = r#"{"market":"0xm1","asset_id":"11","best_bid":"0.4","best_ask":"0.6","timestamp":"1","event_type":"best_bid_ask"}"#;
    write(
        "2026/10/17/esports/cs2/clob_bbo/10.r1.jsonl.zst",
        &[format!("{{\"t\":{T10},\"s\":1,\"m\":{bbo}}}\n")],
    );
    let market = r#"{"condition_id":"0xm1","outcomes":["A","B"],"token_ids":["11","12"],"tracked":true}"#;
    write(
        "2026/10/17/esports/cs2/markets/10.r1.jsonl.zst",
        &[format!("{{\"t\":{T10},\"s\":0,\"m\":{market}}}\n")],
    );
    root
}

fn config(dir: &Path) -> PathBuf {
    let cfg = dir.join("config/recorder.toml");
    fs::create_dir_all(cfg.parent().unwrap()).unwrap();
    fs::write(
        &cfg,
        "data_dir = \"../data/live\"\n[[leagues]]\ncategory = \"esports\"\nkey = \"cs2\"\ntag_id = 100780\n",
    )
    .unwrap();
    cfg
}

#[test]
fn help_lists_every_command() {
    let d = Dir::new("help");
    let o = pm(&["--help"], &d.0);
    assert!(o.status.success());
    for c in ["record", "audit", "export", "markets"] {
        assert!(stdout(&o).contains(c), "{c} missing from help");
    }
    let o = pm(&["frobnicate"], &d.0);
    assert!(!o.status.success());
}

#[test]
fn audit_json_is_clean_on_stdout() {
    let d = Dir::new("audit-json");
    let root = record(&d.0);
    let o = pm(&["audit", "--data-dir", root.to_str().unwrap(), "--json"], &d.0);
    assert!(o.status.success(), "{}", stderr(&o));
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).expect("stdout is only the JSON report");
    assert_eq!(v["totals"]["records"], 2);
    assert!(stderr(&o).contains("audit: reading 2 files"), "logs go to stderr");
}

#[test]
fn audit_finds_the_data_root_through_the_config() {
    let d = Dir::new("audit-config");
    record(&d.0);
    config(&d.0);
    let o = pm(&["audit"], &d.0);
    assert!(o.status.success(), "{}", stderr(&o));
    assert!(stdout(&o).contains("2 records in 2 files"), "{}", stdout(&o));

    let o = pm(&["audit", "--config", "missing.toml"], &d.0);
    assert!(!o.status.success());
    assert!(stderr(&o).contains("missing.toml"), "{}", stderr(&o));
}

#[test]
fn bad_arguments_are_rejected() {
    let d = Dir::new("bad-args");
    let root = record(&d.0);
    let r = root.to_str().unwrap();
    let o = pm(&["audit", "--data-dir", r, "--from", "17/10/2026"], &d.0);
    assert!(!o.status.success());
    assert!(stderr(&o).contains("--from"));

    let o = pm(&["export", "--data-dir", r, "--tables", "bbo,nope"], &d.0);
    assert!(!o.status.success());
    let e = stderr(&o);
    assert!(
        e.contains("unknown table nope") && e.contains("book_updates") && e.contains("tokens"),
        "{e}"
    );

    let o = pm(&["record", "--log-file", "a.log", "--no-log-file"], &d.0);
    assert!(!o.status.success(), "conflicting flags");
}

#[test]
fn export_defaults_next_to_the_data_and_is_incremental() {
    let d = Dir::new("export");
    let root = record(&d.0);
    let r = root.to_str().unwrap();
    let o = pm(&["export", "--data-dir", r], &d.0);
    assert!(o.status.success(), "{}", stderr(&o));
    assert!(
        stdout(&o).starts_with("exported 1 units (0 unchanged)"),
        "{}",
        stdout(&o)
    );
    let out = d.0.join("data/parquet");
    assert!(
        out.join("bbo/date=2026-10-17/category=esports/league=cs2/part.parquet")
            .is_file()
    );
    assert!(out.join("tokens.parquet").is_file());

    let o = pm(&["export", "--data-dir", r], &d.0);
    assert!(
        stdout(&o).starts_with("exported 0 units (1 unchanged)"),
        "{}",
        stdout(&o)
    );
}

#[test]
fn export_only_dimension_tables() {
    let d = Dir::new("export-dims");
    let root = record(&d.0);
    let out = d.0.join("pq");
    let o = pm(
        &[
            "export",
            "--data-dir",
            root.to_str().unwrap(),
            "--out",
            out.to_str().unwrap(),
            "--tables",
            "tokens",
        ],
        &d.0,
    );
    assert!(o.status.success(), "{}", stderr(&o));
    assert!(out.join("tokens.parquet").is_file());
    assert!(!out.join("bbo").exists());
}

#[test]
fn record_reports_a_bad_config() {
    let d = Dir::new("record-config");
    fs::write(
        d.0.join("bad.toml"),
        "data_dir = \"x\"\n[[leagues]]\ncategory = \"esports\"\nkey = \"cs2\"\n",
    )
    .unwrap();
    let o = pm(&["record", "--config", "bad.toml"], &d.0);
    assert!(!o.status.success());
    assert!(stderr(&o).contains("set tag_id and/or series_id"), "{}", stderr(&o));
}

#[test]
#[ignore = "network: records live data for 15 s"]
fn record_smoke_test() {
    let d = Dir::new("record");
    let cfg = config(&d.0);
    let o = pm(
        &[
            "record",
            "--config",
            cfg.to_str().unwrap(),
            "--no-log-file",
            "--run-for",
            "15",
        ],
        &d.0,
    );
    assert!(o.status.success(), "{}", stderr(&o));
    let log = stdout(&o);
    assert!(
        log.contains("discovery: recording") && log.contains("all files closed"),
        "{log}"
    );
    let audit = pm(
        &["audit", "--data-dir", d.0.join("data/live").to_str().unwrap(), "--json"],
        &d.0,
    );
    let v: serde_json::Value = serde_json::from_slice(&audit.stdout).unwrap();
    assert!(v["totals"]["records"].as_u64().unwrap() > 0);
    assert_eq!(
        v["integrity"]["damaged"].as_array().unwrap().len(),
        0,
        "a clean shutdown leaves no damage"
    );
}

#[test]
#[ignore = "network: queries Gamma"]
fn markets_esports_overview() {
    let d = Dir::new("markets");
    let o = pm(&["markets", "--category", "esports", "--no-config", "--json"], &d.0);
    assert!(o.status.success(), "{}", stderr(&o));
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    assert!(v["events"].as_u64().unwrap() > 0);
    assert_eq!(v["categories"][0]["key"], "esports");
}
