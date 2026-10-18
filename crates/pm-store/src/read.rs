//! Reading recorded files: find them ([`scan`]) and stream their records ([`read_file`]).
//!
//! Readers never fail on damaged data. A file cut short by a crash (or still being written) ends
//! at its last complete line, and the damage is reported in [`ReadReport`].

use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::NaiveDate;
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::{HOUR_NS, Partition};

/// One recorded file, described by its path:
/// `<root>/YYYY/MM/DD/<category>/<league>/<stream>/HH.r<run>.jsonl.zst`.
#[derive(Clone, Debug)]
pub struct FileInfo {
    pub path: PathBuf,
    pub date: NaiveDate,
    /// UTC hour, 0–23.
    pub hour: u32,
    pub part: Partition,
    pub stream: String,
    /// Recorder run id (its start time, Unix seconds).
    pub run: i64,
    /// Compressed size on disk.
    pub size: u64,
}

impl FileInfo {
    /// Start of the file's hour, ns since the epoch.
    pub fn hour_start_ns(&self) -> i64 {
        let day = self.date.and_hms_opt(0, 0, 0).expect("midnight").and_utc().timestamp();
        day * 1_000_000_000 + self.hour as i64 * HOUR_NS
    }

    /// Parses `path` relative to `root`; `None` if it does not follow the layout.
    fn parse(root: &Path, path: &Path) -> Option<FileInfo> {
        let rel = path.strip_prefix(root).ok()?;
        let parts: Vec<&str> = rel.iter().map(|c| c.to_str()).collect::<Option<_>>()?;
        let [y, m, d, category, league, stream, name] = parts[..] else {
            return None;
        };
        let date = NaiveDate::from_ymd_opt(y.parse().ok()?, m.parse().ok()?, d.parse().ok()?)?;
        let (hour, run) = name.strip_suffix(".jsonl.zst")?.split_once(".r")?;
        let hour: u32 = hour.parse().ok().filter(|h| *h < 24)?;
        Some(FileInfo {
            path: path.to_path_buf(),
            date,
            hour,
            part: Partition::new(category, league),
            stream: stream.to_string(),
            run: run.parse().ok()?,
            size: fs::metadata(path).ok()?.len(),
        })
    }
}

/// Which files [`scan`] returns. Empty lists match everything; dates are inclusive.
#[derive(Clone, Debug, Default)]
pub struct Filter {
    pub from: Option<NaiveDate>,
    pub to: Option<NaiveDate>,
    pub categories: Vec<String>,
    pub leagues: Vec<String>,
    pub streams: Vec<String>,
}

impl Filter {
    fn date(&self, d: NaiveDate) -> bool {
        self.from.is_none_or(|f| d >= f) && self.to.is_none_or(|t| d <= t)
    }

    fn file(&self, f: &FileInfo) -> bool {
        let any = |list: &[String], v: &str| list.is_empty() || list.iter().any(|x| x == v);
        self.date(f.date)
            && any(&self.categories, &f.part.category)
            && any(&self.leagues, &f.part.league)
            && any(&self.streams, &f.stream)
    }
}

#[derive(Debug, Default)]
pub struct Scan {
    /// Sorted by date, hour, partition, stream, run.
    pub files: Vec<FileInfo>,
    /// Files under `root` that do not follow the layout (reported, never read).
    pub unrecognised: Vec<PathBuf>,
}

/// Finds the recorded files under `root` that match `filter`. Day directories outside the date
/// range are not descended into.
pub fn scan(root: &Path, filter: &Filter) -> Result<Scan> {
    let mut out = Scan::default();
    walk(root, root, 0, filter, &mut out).with_context(|| format!("scanning {}", root.display()))?;
    out.files.sort_by(|a, b| {
        (a.date, a.hour, &a.part.category, &a.part.league, &a.stream, a.run).cmp(&(
            b.date,
            b.hour,
            &b.part.category,
            &b.part.league,
            &b.stream,
            b.run,
        ))
    });
    out.unrecognised.sort();
    Ok(out)
}

fn walk(root: &Path, dir: &Path, depth: usize, filter: &Filter, out: &mut Scan) -> Result<()> {
    // At depth 3 `dir` is a day directory: skip it early if out of range.
    if depth == 3 {
        let day = dir.strip_prefix(root).ok().and_then(|rel| {
            let s: Vec<&str> = rel.iter().filter_map(|c| c.to_str()).collect();
            NaiveDate::from_ymd_opt(s[0].parse().ok()?, s[1].parse().ok()?, s[2].parse().ok()?)
        });
        if day.is_some_and(|d| !filter.date(d)) {
            return Ok(());
        }
    }
    for e in fs::read_dir(dir)? {
        let p = e?.path();
        if p.is_dir() {
            walk(root, &p, depth + 1, filter, out)?;
        } else {
            match FileInfo::parse(root, &p) {
                Some(f) if filter.file(&f) => out.files.push(f),
                Some(_) => {}
                None => out.unrecognised.push(p),
            }
        }
    }
    Ok(())
}

/// One stored record: the envelope fields and the payload exactly as written.
#[derive(Debug, Deserialize)]
pub struct Record<'a> {
    /// Receive time, ns since the epoch (UTC).
    #[serde(rename = "t")]
    pub t_ns: i64,
    /// Connection id (0 for records we produced, e.g. book snapshots and metadata).
    #[serde(rename = "s")]
    pub session: u64,
    #[serde(rename = "m", borrow)]
    pub payload: &'a RawValue,
}

#[derive(Clone, Debug, Default)]
pub struct ReadReport {
    /// Records passed to the callback.
    pub records: u64,
    /// Lines that were not a valid record.
    pub bad_lines: u64,
    /// Decompressed bytes.
    pub bytes: u64,
    /// Why reading stopped early (a crash-truncated frame, a partial last line, …).
    pub truncated: Option<String>,
}

/// Calls `f` for every record of a file, in file order. Only an unopenable file is an error.
pub fn read_file(path: &Path, mut f: impl FnMut(Record<'_>)) -> Result<ReadReport> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    // The writer creates a file before its first flush; empty is not damaged.
    if file.metadata().is_ok_and(|m| m.len() == 0) {
        return Ok(ReadReport::default());
    }
    let dec = zstd::stream::read::Decoder::new(file).with_context(|| format!("reading {}", path.display()))?;
    let mut rd = BufReader::with_capacity(1 << 16, dec);
    let mut rep = ReadReport::default();
    let mut line = Vec::with_capacity(4096);
    loop {
        line.clear();
        match rd.read_until(b'\n', &mut line) {
            Ok(0) => break,
            Ok(n) => {
                rep.bytes += n as u64;
                if line.last() != Some(&b'\n') {
                    rep.truncated = Some(format!("partial last line ({n} bytes)"));
                    break;
                }
                match serde_json::from_slice::<Record>(&line) {
                    Ok(r) => {
                        rep.records += 1;
                        f(r);
                    }
                    Err(_) => rep.bad_lines += 1,
                }
            }
            Err(e) => {
                rep.truncated = Some(e.to_string());
                break;
            }
        }
    }
    Ok(rep)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    fn tmp(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("pm-store-read-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        d
    }

    fn write_zst(path: &Path, lines: &[&str]) -> Vec<u8> {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut enc = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
        for l in lines {
            enc.write_all(l.as_bytes()).unwrap();
        }
        let buf = enc.finish().unwrap();
        fs::write(path, &buf).unwrap();
        buf
    }

    #[test]
    fn scans_layout_and_filters() {
        let root = tmp("scan");
        let a = root.join("2026/10/17/esports/cs2/clob_book/18.r100.jsonl.zst");
        let b = root.join("2026/10/18/esports/lol/trades/03.r200.jsonl.zst");
        let junk = root.join("2026/10/18/esports/lol/trades/notes.txt");
        write_zst(&a, &["{\"t\":1,\"s\":0,\"m\":{}}\n"]);
        write_zst(&b, &[]);
        fs::write(&junk, "x").unwrap();

        let all = scan(&root, &Filter::default()).unwrap();
        assert_eq!(all.files.len(), 2);
        assert_eq!(all.unrecognised, vec![junk]);
        let f = &all.files[0];
        assert_eq!((f.hour, f.run, f.stream.as_str()), (18, 100, "clob_book"));
        assert_eq!(f.part.to_string(), "esports/cs2");
        assert_eq!(f.hour_start_ns(), 1_792_260_000 * 1_000_000_000); // 2026-10-17T18:00Z

        let day = NaiveDate::from_ymd_opt(2026, 10, 18).unwrap();
        let only_18 = scan(
            &root,
            &Filter {
                from: Some(day),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(only_18.files.len(), 1);
        let lol = scan(
            &root,
            &Filter {
                leagues: vec!["lol".into()],
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(lol.files[0].part.league.as_ref(), "lol");
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn reads_records_and_reports_damage() {
        let root = tmp("read");
        let p = root.join("a.jsonl.zst");
        let lines = [
            "{\"t\":5,\"s\":2,\"m\":{\"x\":1}}\n",
            "not json\n",
            "{\"t\":6,\"s\":2,\"m\":[1,2]}\n",
        ];
        let full = write_zst(&p, &lines);
        let mut got = Vec::new();
        let rep = read_file(&p, |r| got.push((r.t_ns, r.session, r.payload.get().to_string()))).unwrap();
        assert_eq!(got, vec![(5, 2, "{\"x\":1}".into()), (6, 2, "[1,2]".into())]);
        assert_eq!((rep.records, rep.bad_lines, rep.truncated.is_none()), (2, 1, true));

        // A second frame appended by a later run is read too.
        let mut two = full.clone();
        two.extend(write_zst(&root.join("b"), &["{\"t\":7,\"s\":3,\"m\":0}\n"]));
        fs::write(&p, &two).unwrap();
        assert_eq!(read_file(&p, |_| {}).unwrap().records, 3);

        // Cut mid-frame: earlier data is still delivered, the damage is reported.
        fs::write(&p, &full[..full.len() - 3]).unwrap();
        let rep = read_file(&p, |_| {}).unwrap();
        assert!(rep.truncated.is_some(), "{rep:?}");
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn empty_and_flushed_but_unfinished_files() {
        let root = tmp("empty");
        fs::create_dir_all(&root).unwrap();

        // Opened by the writer, nothing flushed yet (a crash in the first seconds of an hour).
        let empty = root.join("empty.jsonl.zst");
        fs::write(&empty, b"").unwrap();
        let rep = read_file(&empty, |_| panic!("no records")).unwrap();
        assert_eq!((rep.records, rep.bad_lines, rep.bytes), (0, 0, 0));
        assert!(rep.truncated.is_none(), "an empty file is not damaged: {rep:?}");

        // Flushed but never finished (the normal state of the current hour's file): every
        // flushed line is readable.
        let open = root.join("open.jsonl.zst");
        let mut enc = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
        enc.write_all(b"{\"t\":1,\"s\":0,\"m\":1}\n{\"t\":2,\"s\":0,\"m\":2}\n")
            .unwrap();
        enc.flush().unwrap();
        fs::write(&open, enc.get_ref()).unwrap();
        let mut ts = vec![];
        let rep = read_file(&open, |r| ts.push(r.t_ns)).unwrap();
        assert_eq!(ts, vec![1, 2]);
        assert_eq!(rep.bad_lines, 0);

        assert!(read_file(&root.join("missing.jsonl.zst"), |_| {}).is_err());
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn partial_last_line_is_reported_not_parsed() {
        let root = tmp("partial");
        let p = root.join("p.jsonl.zst");
        write_zst(&p, &["{\"t\":1,\"s\":0,\"m\":1}\n", "{\"t\":2,\"s\":0,\"m\":2}"]);
        let mut n = 0;
        let rep = read_file(&p, |_| n += 1).unwrap();
        assert_eq!(n, 1);
        assert!(rep.truncated.unwrap().contains("partial last line"));
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn payload_is_passed_through_verbatim() {
        let root = tmp("verbatim");
        let p = root.join("v.jsonl.zst");
        let payload = r#"{"title":"A \"quoted\" \u00e9 title","n":[1,2.50,null]}"#;
        write_zst(
            &p,
            &[&format!("{{\"t\":-5,\"s\":18446744073709551615,\"m\":{payload}}}\n")],
        );
        let mut got = None;
        read_file(&p, |r| got = Some((r.t_ns, r.session, r.payload.get().to_string()))).unwrap();
        assert_eq!(got, Some((-5, u64::MAX, payload.to_string())));
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn rejects_paths_outside_the_layout() {
        let root = tmp("layout");
        let ok = "2026/10/17/esports/cs2/clob_book/18.r100.jsonl.zst";
        let bad = [
            "2026/10/17/esports/cs2/clob_book/24.r100.jsonl.zst", // hour 24
            "2026/10/17/esports/cs2/clob_book/18.rabc.jsonl.zst", // run not a number
            "2026/10/17/esports/cs2/clob_book/18.jsonl.zst",      // no run
            "2026/10/17/esports/cs2/clob_book/18.r100.jsonl",     // not compressed
            "2026/13/17/esports/cs2/clob_book/18.r100.jsonl.zst", // month 13
            "2026/10/17/esports/clob_book/18.r100.jsonl.zst",     // a level missing
            "clob_book/cs2/2026-10-16/18.r100.jsonl.zst",         // the old layout
        ];
        for rel in std::iter::once(ok).chain(bad) {
            write_zst(&root.join(rel), &[]);
        }
        let s = scan(&root, &Filter::default()).unwrap();
        assert_eq!(s.files.len(), 1);
        assert_eq!(s.unrecognised.len(), bad.len(), "{:?}", s.unrecognised);
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn scan_sorts_by_time_then_run_and_applies_every_filter() {
        let root = tmp("sort");
        for rel in [
            "2026/10/18/esports/cs2/trades/00.r9.jsonl.zst",
            "2026/10/17/esports/cs2/trades/23.r9.jsonl.zst",
            "2026/10/17/esports/cs2/trades/23.r3.jsonl.zst",
            "2026/10/17/sports/nfl/scores/05.r3.jsonl.zst",
            "2026/10/16/esports/cs2/trades/12.r1.jsonl.zst",
        ] {
            write_zst(&root.join(rel), &[]);
        }
        let s = scan(&root, &Filter::default()).unwrap();
        let order: Vec<(String, u32, i64)> = s.files.iter().map(|f| (f.date.to_string(), f.hour, f.run)).collect();
        assert_eq!(
            order,
            [
                ("2026-10-16".into(), 12, 1),
                ("2026-10-17".into(), 5, 3),
                ("2026-10-17".into(), 23, 3),
                ("2026-10-17".into(), 23, 9),
                ("2026-10-18".into(), 0, 9),
            ]
        );
        let d = |day| NaiveDate::from_ymd_opt(2026, 10, day).unwrap();
        let n = |f: Filter| scan(&root, &f).unwrap().files.len();
        assert_eq!(
            n(Filter {
                from: Some(d(17)),
                to: Some(d(17)),
                ..Default::default()
            }),
            3
        );
        assert_eq!(
            n(Filter {
                to: Some(d(16)),
                ..Default::default()
            }),
            1
        );
        assert_eq!(
            n(Filter {
                categories: vec!["sports".into()],
                ..Default::default()
            }),
            1
        );
        assert_eq!(
            n(Filter {
                streams: vec!["trades".into(), "scores".into()],
                ..Default::default()
            }),
            5
        );
        assert_eq!(
            n(Filter {
                leagues: vec!["cs2".into()],
                streams: vec!["scores".into()],
                ..Default::default()
            }),
            0
        );
        // A root that does not exist is an error, not an empty result.
        assert!(scan(&root.join("nope"), &Filter::default()).is_err());
        fs::remove_dir_all(&root).unwrap();
    }
}
