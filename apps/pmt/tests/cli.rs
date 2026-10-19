//! The `pmt` binary end to end: arguments, exit codes, config checks and the journal report.
//! Nothing here touches the network (`pmt run` is only started with configs it must refuse).

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const BIN: &str = env!("CARGO_BIN_EXE_pmt");
/// 2026-10-17 10:00:00 UTC, ns.
const T10: i64 = 1_792_231_200 * 1_000_000_000;

struct Dir(PathBuf);

impl Dir {
    fn new(name: &str) -> Dir {
        let d = std::env::temp_dir().join(format!("pmt-cli-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        Dir(d)
    }

    fn config(&self, text: &str) -> PathBuf {
        let p = self.0.join("pmt.toml");
        fs::write(&p, text).unwrap();
        p
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn pmt(args: &[&str], cwd: &Path) -> Output {
    Command::new(BIN).args(args).current_dir(cwd).output().unwrap()
}

fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

fn stderr(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

const BASE: &str = "journal_dir = \"journal\"\n[[leagues]]\nkey = \"cs2\"\ntag_id = 100780\n";

fn workspace() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn lists_the_strategy_kinds() {
    let o = pmt(&["strategies"], &workspace());
    assert!(o.status.success());
    let out = stdout(&o);
    assert!(
        out.contains("cs2_map_longshot") && out.contains("thin_favourite"),
        "{out}"
    );
}

#[test]
fn the_shipped_config_is_valid() {
    let o = pmt(&["check"], &workspace());
    assert!(o.status.success(), "{}", stderr(&o));
    let out = stdout(&o);
    assert!(out.contains("config/pmt.toml: ok (paper mode)"), "{out}");
    assert!(out.contains("strategy c1 (cs2_map_longshot)"), "{out}");
}

#[test]
fn check_reports_config_mistakes() {
    let d = Dir::new("check");
    let check = |text: &str| {
        let cfg = d.config(text);
        pmt(&["check", "-c", cfg.to_str().unwrap()], &d.0)
    };
    let ok = check(&format!(
        "{BASE}[run]\nrun_for = \"1d12h\"\n[[strategy]]\nkind = \"cs2_map_longshot\"\nentry_cutoff = \"2h\"\n"
    ));
    assert!(ok.status.success(), "{}", stderr(&ok));
    let out = stdout(&ok);
    assert!(
        out.contains("stop after 1d12h") && out.contains("no new trades in the last 2h"),
        "{out}"
    );

    for (text, why) in [
        (
            format!("{BASE}[[strategy]]\nkind = \"nope\"\n"),
            "unknown strategy kind",
        ),
        (
            format!("{BASE}[[strategy]]\nkind = \"cs2_map_longshot\"\nstake = 2\n"),
            "unknown field",
        ),
        (
            format!("{BASE}[[strategy]]\nkind = \"cs2_map_longshot\"\nmin_price = 0.5\nmax_price = 0.2\n"),
            "min_price",
        ),
        (
            format!("{BASE}[[strategy]]\nkind = \"cs2_map_longshot\"\nenabled = false\n"),
            "no strategy is enabled",
        ),
        (format!("{BASE}[run]\nrun_for = \"3 days\"\n"), "unknown unit"),
        (format!("mode = \"live\"\n{BASE}"), "not implemented"),
    ] {
        let o = check(&text);
        assert!(!o.status.success(), "accepted: {text}");
        assert!(stderr(&o).contains(why), "{why}: {}", stderr(&o));
    }
}

#[test]
fn run_refuses_before_connecting_when_nothing_is_enabled() {
    let d = Dir::new("run");
    let cfg = d.config(&format!(
        "{BASE}[[strategy]]\nkind = \"thin_favourite\"\nenabled = false\n"
    ));
    let o = pmt(&["run", "-c", cfg.to_str().unwrap(), "--no-log-file"], &d.0);
    assert!(!o.status.success());
    assert!(stderr(&o).contains("no strategy is enabled"), "{}", stderr(&o));
    let bad = pmt(&["run", "-c", cfg.to_str().unwrap(), "--run-for", "soon"], &d.0);
    assert!(!bad.status.success());
    assert!(stderr(&bad).contains("--run-for"), "{}", stderr(&bad));
}

/// Writes one journal file: `<journal>/2026/10/17/paper/<strategy>/<stream>/10.r1.jsonl.zst`.
fn journal(root: &Path, strategy: &str, stream: &str, payloads: &[String]) {
    let p = root.join(format!("2026/10/17/paper/{strategy}/{stream}/10.r1.jsonl.zst"));
    fs::create_dir_all(p.parent().unwrap()).unwrap();
    let mut enc = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
    for m in payloads {
        writeln!(enc, "{{\"t\":{T10},\"s\":0,\"m\":{m}}}").unwrap();
    }
    fs::write(p, enc.finish().unwrap()).unwrap();
}

#[test]
fn report_summarises_a_journal() {
    let d = Dir::new("report");
    let cfg = d.config(BASE);
    let cfg = cfg.to_str().unwrap();
    let empty = pmt(&["report", "-c", cfg], &d.0);
    assert!(empty.status.success(), "{}", stderr(&empty));
    assert!(stdout(&empty).contains("nothing journaled"), "{}", stdout(&empty));

    let root = d.0.join("journal");
    let settled = |m: &str, won: bool, pnl: &str| {
        format!(
            r#"{{"strategy":"c1","market":"{m}","token":"t","won":{won},"shares":"10","cost":"2.2","payout":"{}","pnl":"{pnl}","t_ns":{T10}}}"#,
            if won { "10" } else { "0" }
        )
    };
    journal(
        &root,
        "c1",
        "settlements",
        &[settled("0xa", true, "7.8"), settled("0xb", false, "-2.2")],
    );
    let o = pmt(&["report", "-c", cfg], &d.0);
    assert!(o.status.success(), "{}", stderr(&o));
    let out = stdout(&o);
    assert!(out.contains("2026-10-17") && out.contains("+$5.60"), "{out}");
    assert!(out.contains("50%"), "win rate: {out}");

    let json = pmt(&["report", "-c", cfg, "--json", "--by", "month"], &d.0);
    let v: serde_json::Value = serde_json::from_slice(&json.stdout).unwrap();
    assert_eq!(v["strategies"][0]["settled"], 2);
    assert_eq!(v["periods"][0]["period"], "2026-10");
    // Outside the window, and another strategy: nothing.
    let o = pmt(&["report", "-c", cfg, "--from", "2026-10-18"], &d.0);
    assert!(stdout(&o).contains("nothing journaled"), "{}", stdout(&o));
    let o = pmt(&["report", "-c", cfg, "--strategy", "fav"], &d.0);
    assert!(stdout(&o).contains("nothing journaled"), "{}", stdout(&o));
    // The live journal is separate.
    let o = pmt(&["report", "-c", cfg, "--mode", "live"], &d.0);
    assert!(stdout(&o).contains("nothing journaled"), "{}", stdout(&o));
    let bad = pmt(&["report", "-c", cfg, "--mode", "real"], &d.0);
    assert!(!bad.status.success());
}
