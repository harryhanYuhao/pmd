//! `pmd export`: recorded zstd JSONL → typed, Hive-partitioned Parquet.
//!
//! ```text
//! <out>/<table>/date=YYYY-MM-DD/category=<c>/league=<l>/part.parquet   time series
//! <out>/{markets,tokens,events,sessions}.parquet                       dimensions
//! <out>/_manifest.json                                                 inputs of every output
//! ```
//!
//! The work unit is one stream of one league on one day. A unit whose input files (paths and
//! sizes) are unchanged since the last export is skipped. Each source hour is read, sorted by
//! receive time and written as row groups, so memory is bounded by one hour of one league.
//! Files are written under a temporary name and renamed, so an interrupted export never leaves
//! a broken file.

mod dims;
mod rows;

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use chrono::{NaiveDate, Utc};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use pm_store::Partition;
use pm_store::read::{FileInfo, Filter, read_file, scan};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

pub use dims::DIMS;
use rows::Rows;
pub use rows::Table;

/// Bump when the output schema changes: every unit is then rebuilt.
/// v2: every column nullable (v1 could write a later hour's nulls as 0).
const SCHEMA_VERSION: u32 = 2;
const MANIFEST: &str = "_manifest.json";
const DIM_STREAMS: [&str; 3] = ["markets", "gamma_events", "sessions"];

pub struct Options {
    pub root: PathBuf,
    pub out: PathBuf,
    /// Selects the time-series units (dates, categories, leagues). Dimensions always use
    /// everything recorded.
    pub filter: Filter,
    /// Time-series tables to write.
    pub tables: Vec<Table>,
    /// Rebuild the dimension tables.
    pub dims: bool,
    /// Also export today (UTC), whose files are still growing. Off by default.
    pub include_today: bool,
    /// Rebuild units even if their inputs are unchanged.
    pub force: bool,
}

#[derive(Debug, Default)]
pub struct Summary {
    pub units: usize,
    pub skipped: usize,
    pub skipped_today: usize,
    pub failed: Vec<String>,
    pub rows: BTreeMap<String, u64>,
    pub unparsed: u64,
    pub truncated_files: u64,
    pub input_bytes: u64,
    pub output_bytes: u64,
}

#[derive(Default, Serialize, Deserialize)]
struct Manifest {
    version: u32,
    units: BTreeMap<String, UnitEntry>,
}

#[derive(Clone, Default, PartialEq, Serialize, Deserialize)]
struct UnitEntry {
    /// (path relative to the data root, size) of every input file.
    inputs: Vec<(String, u64)>,
    /// Table → rows written (0 = no file).
    tables: BTreeMap<String, u64>,
}

/// One stream of one league on one day.
struct Unit {
    key: String,
    stream: String,
    date: NaiveDate,
    part: Partition,
    files: Vec<FileInfo>,
    tables: Vec<Table>,
}

impl Unit {
    fn inputs(&self, root: &Path) -> Vec<(String, u64)> {
        inputs(root, &self.files)
    }

    fn out_path(&self, out: &Path, t: Table) -> PathBuf {
        out.join(t.name())
            .join(format!("date={}", self.date))
            .join(format!("category={}", self.part.category))
            .join(format!("league={}", self.part.league))
            .join("part.parquet")
    }
}

fn inputs(root: &Path, files: &[FileInfo]) -> Vec<(String, u64)> {
    files
        .iter()
        .map(|f| {
            let rel = f.path.strip_prefix(root).unwrap_or(&f.path);
            (rel.display().to_string(), f.size)
        })
        .collect()
}

pub fn run(opts: &Options) -> Result<Summary> {
    fs::create_dir_all(&opts.out).with_context(|| format!("creating {}", opts.out.display()))?;
    let mut manifest = load_manifest(&opts.out);
    if manifest.version != SCHEMA_VERSION {
        if !manifest.units.is_empty() {
            tracing::info!(
                "export: output schema changed (v{} -> v{SCHEMA_VERSION}); rebuilding everything",
                manifest.version
            );
        }
        manifest = Manifest {
            version: SCHEMA_VERSION,
            units: BTreeMap::new(),
        };
    }
    let mut summary = Summary::default();
    let today = Utc::now().date_naive();

    // Time-series units.
    // The streams feeding the selected tables, narrowed by the caller's stream filter. An empty
    // list would mean "every stream" to `scan`, so no streams means no time-series work.
    let mut streams: Vec<String> = opts
        .tables
        .iter()
        .map(|t| t.stream().to_string())
        .filter(|s| opts.filter.streams.is_empty() || opts.filter.streams.contains(s))
        .collect();
    streams.sort_unstable();
    streams.dedup();
    let found = if streams.is_empty() {
        Default::default()
    } else {
        scan(
            &opts.root,
            &Filter {
                streams,
                ..opts.filter.clone()
            },
        )?
    };
    let mut by_unit: BTreeMap<String, Unit> = BTreeMap::new();
    for f in found.files {
        if &*f.part.category == "_system" {
            continue;
        }
        if f.date >= today && !opts.include_today {
            summary.skipped_today += 1;
            continue;
        }
        let key = format!("{}/{}/{}/{}", f.stream, f.date, f.part.category, f.part.league);
        by_unit
            .entry(key.clone())
            .or_insert_with(|| Unit {
                key,
                stream: f.stream.clone(),
                date: f.date,
                part: f.part.clone(),
                files: vec![],
                tables: opts.tables.iter().copied().filter(|t| t.stream() == f.stream).collect(),
            })
            .files
            .push(f);
    }
    let mut todo: Vec<Unit> = Vec::new();
    for (_, u) in by_unit {
        let up_to_date = !opts.force
            && manifest.units.get(&u.key).is_some_and(|e| {
                e.inputs == u.inputs(&opts.root)
                    && u.tables.iter().all(|t| match e.tables.get(t.name()) {
                        Some(0) => true,
                        Some(_) => u.out_path(&opts.out, *t).is_file(),
                        None => false,
                    })
            });
        if up_to_date {
            summary.skipped += 1;
        } else {
            todo.push(u);
        }
    }
    // Biggest first, so the long units start early and the cores stay busy.
    todo.sort_by_key(|u| std::cmp::Reverse(u.files.iter().map(|f| f.size).sum::<u64>()));
    tracing::info!(
        "export: {} units to write, {} unchanged, {} files of today skipped{}",
        todo.len(),
        summary.skipped,
        summary.skipped_today,
        if summary.skipped_today > 0 {
            " (--include-today to export them)"
        } else {
            ""
        }
    );

    let done = AtomicUsize::new(0);
    let total = todo.len();
    let unparsed = AtomicU64::new(0);
    let truncated = AtomicU64::new(0);
    let results: Vec<(Unit, Result<BTreeMap<String, u64>>)> = todo
        .into_par_iter()
        .map(|u| {
            let r = export_unit(&u, &opts.out, &unparsed, &truncated);
            let n = done.fetch_add(1, Ordering::Relaxed) + 1;
            if n.is_multiple_of(50) || n == total {
                tracing::info!("export: {n}/{total} units done");
            }
            (u, r)
        })
        .collect();
    for (u, r) in results {
        match r {
            Ok(tables) => {
                summary.units += 1;
                summary.input_bytes += u.files.iter().map(|f| f.size).sum::<u64>();
                for (t, n) in &tables {
                    *summary.rows.entry(t.clone()).or_default() += n;
                    if *n > 0 {
                        summary.output_bytes +=
                            fs::metadata(u.out_path(&opts.out, Table::from_name(t).expect("our table")))
                                .map(|m| m.len())
                                .unwrap_or(0);
                    }
                }
                let entry = manifest.units.entry(u.key.clone()).or_default();
                let inputs = u.inputs(&opts.root);
                if entry.inputs != inputs {
                    // Other tables of this unit were built from different inputs.
                    entry.tables.clear();
                    entry.inputs = inputs;
                }
                entry.tables.extend(tables);
            }
            Err(e) => {
                tracing::error!("export: {} failed: {e:#}", u.key);
                summary.failed.push(u.key);
            }
        }
    }

    // Dimensions: small, rebuilt from everything recorded whenever an input changed.
    if opts.dims {
        let all = scan(
            &opts.root,
            &Filter {
                streams: DIM_STREAMS.iter().map(|s| s.to_string()).collect(),
                ..Default::default()
            },
        )?;
        let key = "_dims".to_string();
        let ins = inputs(&opts.root, &all.files);
        let unchanged = !opts.force
            && manifest.units.get(&key).is_some_and(|e| e.inputs == ins)
            && DIMS.iter().all(|d| opts.out.join(format!("{d}.parquet")).is_file());
        if unchanged {
            tracing::info!("export: dimension tables unchanged");
        } else {
            tracing::info!("export: building dimension tables from {} files", all.files.len());
            let (tables, bad) = dims::build(&all.files);
            summary.unparsed += bad;
            let mut entry = UnitEntry {
                inputs: ins,
                tables: BTreeMap::new(),
            };
            for (name, b) in tables {
                let path = opts.out.join(format!("{name}.parquet"));
                let n = b.num_rows() as u64;
                write_whole(&path, &b)?;
                summary.output_bytes += fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                *summary.rows.entry(name.to_string()).or_default() += n;
                entry.tables.insert(name.to_string(), n);
            }
            manifest.units.insert(key, entry);
        }
    }

    save_manifest(&opts.out, &manifest)?;
    summary.unparsed += unparsed.into_inner();
    summary.truncated_files = truncated.into_inner();
    Ok(summary)
}

/// Exports one unit: hour by hour, every run's file of that hour, sorted by receive time.
fn export_unit(u: &Unit, out: &Path, unparsed: &AtomicU64, truncated: &AtomicU64) -> Result<BTreeMap<String, u64>> {
    let mut rows = Rows::new(u.tables.clone());
    let mut sinks: HashMap<Table, Sink> = HashMap::new();
    let mut counts: BTreeMap<String, u64> = u.tables.iter().map(|t| (t.name().to_string(), 0)).collect();
    let mut i = 0;
    while i < u.files.len() {
        let hour = u.files[i].hour;
        while i < u.files.len() && u.files[i].hour == hour {
            let f = &u.files[i];
            i += 1;
            let mut bad = 0;
            let rep = read_file(&f.path, |r| {
                if !rows.add(&u.stream, r.t_ns, r.session, r.payload.get()) {
                    bad += 1;
                }
            })?;
            unparsed.fetch_add(bad + rep.bad_lines, Ordering::Relaxed);
            if let Some(why) = rep.truncated {
                truncated.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(
                    "export: {} is truncated ({why}); used what was readable",
                    f.path.display()
                );
            }
        }
        rows.sort();
        rows.drain_batches(|t, b| {
            *counts.entry(t.name().to_string()).or_default() += b.num_rows() as u64;
            let sink = match sinks.entry(t) {
                std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                std::collections::hash_map::Entry::Vacant(e) => e.insert(Sink::create(u.out_path(out, t), &b)?),
            };
            sink.w.write(&b).context("writing parquet")
        })?;
    }
    for (_, s) in sinks {
        s.finish()?;
    }
    // A table that got no rows this time must not keep a file from an earlier export.
    for t in &u.tables {
        if counts.get(t.name()) == Some(&0) {
            let _ = fs::remove_file(u.out_path(out, *t));
        }
    }
    Ok(counts)
}

/// zstd level 3; sorted time columns delta-encoded (~6% smaller than dictionary encoding,
/// measured on a day of CS2 book updates). Settings for columns a table lacks are ignored.
fn props() -> WriterProperties {
    let mut b = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).expect("valid level")))
        .set_max_row_group_row_count(Some(1 << 20));
    for c in ["t", "ts", "chain_ts", "session"] {
        b = b
            .set_column_dictionary_enabled(ColumnPath::from(c), false)
            .set_column_encoding(ColumnPath::from(c), Encoding::DELTA_BINARY_PACKED);
    }
    b.build()
}

/// A Parquet file being written under a temporary name.
struct Sink {
    path: PathBuf,
    tmp: PathBuf,
    w: ArrowWriter<File>,
}

impl Sink {
    fn create(path: PathBuf, first: &RecordBatch) -> Result<Sink> {
        let dir = path.parent().expect("output file has a directory");
        fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        let tmp = path.with_extension("parquet.tmp");
        let f = File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
        let w = ArrowWriter::try_new(f, first.schema(), Some(props()))?;
        Ok(Sink { path, tmp, w })
    }

    fn finish(self) -> Result<()> {
        self.w.close().context("finishing parquet")?;
        fs::rename(&self.tmp, &self.path).with_context(|| format!("renaming to {}", self.path.display()))?;
        Ok(())
    }
}

fn write_whole(path: &Path, b: &RecordBatch) -> Result<()> {
    let mut s = Sink::create(path.to_path_buf(), b)?;
    s.w.write(b)?;
    s.finish()
}

fn load_manifest(out: &Path) -> Manifest {
    fs::read_to_string(out.join(MANIFEST))
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

fn save_manifest(out: &Path, m: &Manifest) -> Result<()> {
    let path = out.join(MANIFEST);
    let tmp = out.join(format!("{MANIFEST}.tmp"));
    fs::write(&tmp, serde_json::to_vec_pretty(m)?)?;
    fs::rename(&tmp, &path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;

    fn write_zst(path: &Path, lines: &[String]) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut enc = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
        for l in lines {
            enc.write_all(l.as_bytes()).unwrap();
        }
        fs::write(path, enc.finish().unwrap()).unwrap();
    }

    fn rec(t: i64, s: u64, m: &str) -> String {
        format!("{{\"t\":{t},\"s\":{s},\"m\":{m}}}\n")
    }

    fn read_rows(path: &Path) -> (usize, Vec<String>) {
        let r = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = r.map(|b| b.unwrap()).collect();
        let cols = batches[0].schema().fields().iter().map(|f| f.name().clone()).collect();
        (batches.iter().map(|b| b.num_rows()).sum(), cols)
    }

    #[test]
    fn exports_units_dims_and_skips_unchanged() {
        let base = std::env::temp_dir().join(format!("pm-export-{}", std::process::id()));
        let _ = fs::remove_dir_all(&base);
        let (root, out) = (base.join("live"), base.join("parquet"));
        let day = root.join("2026/10/17");
        let h18 = 1_792_260_000_000_000_000i64;
        let bbo = |t: i64, b: &str| {
            rec(
                t,
                1,
                &format!(
                    r#"{{"market":"0xm","asset_id":"11","best_bid":"{b}","best_ask":"0.9","spread":"0.1","timestamp":"1","event_type":"best_bid_ask"}}"#
                ),
            )
        };
        // Two runs in the same hour, interleaved: output must be sorted by t.
        write_zst(
            &day.join("esports/cs2/clob_bbo/18.r1.jsonl.zst"),
            &[bbo(h18 + 30, "0.3"), bbo(h18 + 10, "0.1")],
        );
        write_zst(
            &day.join("esports/cs2/clob_bbo/18.r2.jsonl.zst"),
            &[bbo(h18 + 20, "0.2")],
        );
        write_zst(
            &day.join("esports/cs2/clob_bbo/19.r2.jsonl.zst"),
            &[bbo(h18 + 3_600_000_000_000, "0.4")],
        );
        let market = r#"{"condition_id":"0xm","market_id":"1","slug":"s","question":"A vs B","league":"cs2","event_id":"7","event_slug":"e","market_type":"moneyline","outcomes":["A","B"],"token_ids":["11","22"],"game_start_time":"2026-10-17 14:00:00+00","game_id":5,"tick_size":"0.01","min_order_size":5.0,"neg_risk":false,"active":true,"closed":false,"accepting_orders":true,"tracked":true}"#;
        write_zst(
            &day.join("esports/cs2/markets/18.r1.jsonl.zst"),
            &[rec(h18, 0, market), rec(h18 + 5, 0, market)],
        );
        write_zst(
            &day.join("_system/clob/sessions/18.r1.jsonl.zst"),
            &[
                rec(h18, 9, r#"{"feed":"clob-0","event":"connected"}"#),
                rec(
                    h18 + 99,
                    9,
                    r#"{"feed":"clob-0","event":"disconnected","reason":"shutdown"}"#,
                ),
            ],
        );

        let opts = Options {
            root: root.clone(),
            out: out.clone(),
            filter: Filter::default(),
            tables: Table::ALL.to_vec(),
            dims: true,
            include_today: true,
            force: false,
        };
        let s = run(&opts).unwrap();
        assert_eq!((s.units, s.skipped, s.failed.len()), (1, 0, 0));
        assert_eq!(s.rows["bbo"], 4);
        assert_eq!((s.rows["markets"], s.rows["tokens"], s.rows["sessions"]), (1, 2, 1));

        let part = out.join("bbo/date=2026-10-17/category=esports/league=cs2/part.parquet");
        let (n, cols) = read_rows(&part);
        assert_eq!(n, 4);
        assert_eq!(cols[..5], ["t", "session", "ts", "market", "asset_id"]);
        let b = ParquetRecordBatchReaderBuilder::try_new(File::open(&part).unwrap())
            .unwrap()
            .build()
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let bids = b
            .column_by_name("best_bid")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap();
        assert_eq!(bids.values(), &[0.1, 0.2, 0.3, 0.4]);
        assert!(!part.with_extension("parquet.tmp").exists());

        // Unchanged inputs: nothing to do. Changed input: that unit only.
        let s = run(&opts).unwrap();
        assert_eq!((s.units, s.skipped), (0, 1));
        write_zst(
            &day.join("esports/cs2/clob_bbo/19.r2.jsonl.zst"),
            &[bbo(h18 + 3_600_000_000_000, "0.4"), bbo(h18 + 3_600_000_000_001, "0.5")],
        );
        let s = run(&opts).unwrap();
        assert_eq!((s.units, s.rows["bbo"]), (1, 5));
        fs::remove_dir_all(&base).unwrap();
    }
}
