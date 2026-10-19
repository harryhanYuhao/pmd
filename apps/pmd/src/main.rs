//! `pmd`: Polymarket data tools. Each subcommand is a thin wrapper around a library crate.

mod logging;
mod markets;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use chrono::NaiveDate;
use clap::{Args, Parser, Subcommand};
use pm_store::read::Filter;

use crate::logging::Console;

#[derive(Parser)]
#[command(about = "Polymarket data tools: record live feeds, audit and export recordings")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Record order books, trades, metadata and live scores to hourly zstd JSONL files.
    Record(RecordArgs),
    /// Summarise recorded data: sizes, runs, connection drops, gaps, damaged files. Read-only.
    Audit(AuditArgs),
    /// Convert recorded data into Hive-partitioned Parquet tables (incremental).
    Export(ExportArgs),
    /// Live overview of Polymarket: open and live events, volume, by category and league.
    Markets(MarketsArgs),
}

#[derive(Args)]
struct RecordArgs {
    #[arg(short, long, default_value = "config/pmd.toml")]
    config: PathBuf,
    /// Override `data_dir` from the config.
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// Stop after this many seconds (for smoke tests).
    #[arg(long)]
    run_for: Option<u64>,
    /// Also write the log under this directory, as `YYYY/MM/DD/record.log` (overrides `[log] dir`).
    #[arg(long)]
    log_dir: Option<PathBuf>,
    /// Write the log to stdout only, even if the config names a log directory.
    #[arg(long, conflicts_with = "log_dir")]
    no_log_file: bool,
}

impl RecordArgs {
    /// The config file with command-line overrides applied.
    fn config(&self) -> Result<pm_recorder::Config> {
        let mut cfg = pm_recorder::Config::load(&self.config)?;
        if let Some(d) = &self.data_dir {
            cfg.data_dir = d.clone();
        }
        if self.no_log_file {
            cfg.log.dir = None;
        } else if let Some(d) = &self.log_dir {
            cfg.log.dir = Some(d.clone());
        }
        Ok(cfg)
    }
}

/// Where the recorded data is and which part of it to use (shared by the offline commands).
#[derive(Args)]
struct DataArgs {
    /// Recorded data root. Default: `data_dir` of the recorder config.
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// Recorder config, used only to find the data root.
    #[arg(short, long, default_value = "config/pmd.toml")]
    config: PathBuf,
    /// First day (UTC), YYYY-MM-DD.
    #[arg(long)]
    from: Option<NaiveDate>,
    /// Last day (UTC), inclusive.
    #[arg(long)]
    to: Option<NaiveDate>,
    /// Only these categories (esports, sports, soccer, other, …). Repeat or comma-separate.
    #[arg(long, value_delimiter = ',')]
    category: Vec<String>,
    /// Only these leagues (cs2, nfl, epl, …).
    #[arg(long, value_delimiter = ',')]
    league: Vec<String>,
    /// Only these streams (clob_book, trades, scores, …).
    #[arg(long, value_delimiter = ',')]
    stream: Vec<String>,
    /// Worker threads (default: all cores).
    #[arg(long)]
    threads: Option<usize>,
}

impl DataArgs {
    fn root(&self) -> Result<PathBuf> {
        match &self.data_dir {
            Some(d) => Ok(d.clone()),
            None => Ok(pm_recorder::Config::load(&self.config)?.data_dir),
        }
    }

    fn filter(&self) -> Filter {
        Filter {
            from: self.from,
            to: self.to,
            categories: self.category.clone(),
            leagues: self.league.clone(),
            streams: self.stream.clone(),
        }
    }

    /// Sizes the worker pool: `--threads`, else `default` (None = all cores).
    fn init_threads(&self, default: Option<usize>) -> Result<()> {
        if let Some(n) = self.threads.or(default) {
            rayon::ThreadPoolBuilder::new().num_threads(n.max(1)).build_global()?;
        }
        Ok(())
    }
}

#[derive(Args)]
struct AuditArgs {
    #[command(flatten)]
    data: DataArgs,
    /// Report silences at least this many minutes long.
    #[arg(long, default_value_t = 15)]
    gap_mins: u64,
    /// Rows in the "longest …" lists.
    #[arg(long, default_value_t = 20)]
    top: usize,
    /// Print the report as JSON instead of text.
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
struct ExportArgs {
    #[command(flatten)]
    data: DataArgs,
    /// Output root. Default: `parquet/` next to the data root (`data/live` → `data/parquet`).
    #[arg(long)]
    out: Option<PathBuf>,
    /// Only these tables (book_updates, tick_size_changes, bbo, clob_trades, wallet_trades,
    /// book_snapshots, scores, and the dimensions markets, tokens, events, sessions).
    #[arg(long, value_delimiter = ',')]
    tables: Vec<String>,
    /// Also export today (UTC), whose files are still being written.
    #[arg(long)]
    include_today: bool,
    /// Rebuild everything selected, even if its inputs are unchanged.
    #[arg(long)]
    force: bool,
}

impl ExportArgs {
    fn options(&self) -> Result<pm_dataset::export::Options> {
        use pm_dataset::export::{DIMS, Table};
        let root = self.data.root()?;
        let out = match &self.out {
            Some(o) => o.clone(),
            None => root.parent().unwrap_or(&root).join("parquet"),
        };
        let mut tables = Vec::new();
        let mut dims = self.tables.is_empty();
        for name in &self.tables {
            if let Some(t) = Table::from_name(name) {
                tables.push(t);
            } else if DIMS.contains(&name.as_str()) {
                dims = true;
            } else {
                let all: Vec<&str> = Table::ALL.iter().map(|t| t.name()).chain(DIMS).collect();
                anyhow::bail!("unknown table {name}; choose from {}", all.join(", "));
            }
        }
        if self.tables.is_empty() {
            tables = Table::ALL.to_vec();
        }
        Ok(pm_dataset::export::Options {
            root,
            out,
            filter: self.data.filter(),
            tables,
            dims,
            include_today: self.include_today,
            force: self.force,
        })
    }
}

#[derive(Args)]
struct MarketsArgs {
    /// Only events with this Gamma tag (esports, sports, soccer, politics, crypto, …). Fetched
    /// server-side, so much faster than the full overview.
    #[arg(long)]
    category: Option<String>,
    /// Only these leagues (Gamma sport abbreviations: cs2, lol, epl, nfl, …).
    #[arg(long, value_delimiter = ',')]
    league: Vec<String>,
    /// Only events that are live right now.
    #[arg(long)]
    live: bool,
    /// Also fetch every market (market counts, order books, market types). ~14x more data:
    /// combine with --category.
    #[arg(long)]
    markets: bool,
    /// Rows in the league and event lists.
    #[arg(long, default_value_t = 15)]
    top: usize,
    /// Recorder config, to show which leagues are recorded.
    #[arg(short, long, default_value = "config/pmd.toml")]
    config: PathBuf,
    /// Do not compare with the recorder config.
    #[arg(long)]
    no_config: bool,
    /// Print the overview as JSON instead of text.
    #[arg(long)]
    json: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Record(args) => {
            let cfg = args.config()?;
            let _log = logging::init(&cfg.log.level, Console::Stdout, cfg.log.dir.as_deref())?;
            pm_recorder::run(cfg, args.run_for.map(Duration::from_secs)).await
        }
        Cmd::Audit(args) => {
            let _log = logging::init("info", Console::Stderr, None)?;
            args.data.init_threads(None)?;
            let opts = pm_dataset::audit::Options {
                root: args.data.root()?,
                filter: args.data.filter(),
                gap: Duration::from_secs(args.gap_mins * 60),
                top: args.top,
            };
            let report = tokio::task::spawn_blocking(move || pm_dataset::audit::run(&opts)).await??;
            if args.json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                print!("{}", report.render());
            }
            Ok(())
        }
        Cmd::Markets(args) => {
            let _log = logging::init("info", Console::Stderr, None)?;
            let recorded = if args.no_config {
                None
            } else {
                match pm_recorder::Config::load(&args.config) {
                    Ok(cfg) => Some(
                        cfg.leagues
                            .into_iter()
                            .map(|l| (l.category, l.key, l.score_league))
                            .collect(),
                    ),
                    Err(e) => {
                        tracing::info!("markets: no recorder coverage ({e:#})");
                        None
                    }
                }
            };
            let opts = markets::Options {
                category: args.category,
                leagues: args.league,
                live_only: args.live,
                with_markets: args.markets,
                top: args.top,
                recorded,
            };
            let o = markets::run(&opts).await?;
            if args.json {
                println!("{}", serde_json::to_string_pretty(&o)?);
            } else {
                print!("{}", o.render());
            }
            Ok(())
        }
        Cmd::Export(args) => {
            let _log = logging::init("info", Console::Stderr, None)?;
            // Each worker holds one hour of one league in memory; cap the default.
            let cores = std::thread::available_parallelism().map_or(4, |n| n.get());
            args.data.init_threads(Some(cores.min(8)))?;
            let opts = args.options()?;
            let started = std::time::Instant::now();
            let s = tokio::task::spawn_blocking(move || pm_dataset::export::run(&opts)).await??;
            print_export_summary(&s, started.elapsed());
            anyhow::ensure!(
                s.failed.is_empty(),
                "{} units failed: {}",
                s.failed.len(),
                s.failed.join(", ")
            );
            Ok(())
        }
    }
}

fn print_export_summary(s: &pm_dataset::export::Summary, took: Duration) {
    use pm_dataset::fmt::{bytes, count};
    println!(
        "exported {} units ({} unchanged) in {:.1}s: {} read, {} written",
        s.units,
        s.skipped,
        took.as_secs_f64(),
        bytes(s.input_bytes as f64),
        bytes(s.output_bytes as f64)
    );
    for (t, n) in &s.rows {
        println!("  {t:<18} {:>10} rows", count(*n));
    }
    if s.skipped_today > 0 {
        println!("today's files skipped: {} (use --include-today)", s.skipped_today);
    }
    if s.unparsed > 0 || s.truncated_files > 0 {
        println!(
            "records with an unexpected shape: {}, truncated input files: {}",
            s.unparsed, s.truncated_files
        );
    }
}
