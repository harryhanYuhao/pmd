//! `pmt`: the Polymarket trading bot.

use std::path::PathBuf;

use anyhow::Result;
use chrono::NaiveDate;
use clap::{Args, Parser, Subcommand};
use pm_app::logging::{self, Console, LogFile};
use pmt::report::Period;

#[derive(Parser)]
#[command(about = "Polymarket trading bot: runs the configured strategies (paper trading for now)")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Trade until stopped (Ctrl-C / SIGTERM / SIGHUP).
    Run(RunArgs),
    /// Check the config and build every strategy, without connecting to anything.
    Check(ConfigArg),
    /// Summarise the trade journal: decisions, orders, settled bets, P&L per day or month, open
    /// positions.
    Report(ReportArgs),
    /// List the available strategy kinds.
    Strategies,
}

#[derive(Args)]
struct ConfigArg {
    #[arg(short, long, default_value = "config/pmt.toml")]
    config: PathBuf,
}

#[derive(Args)]
struct RunArgs {
    #[command(flatten)]
    config: ConfigArg,
    /// Scheduled stop after this long (`3d`, `12h`, `90m`, seconds); overrides `[run] run_for`.
    #[arg(long)]
    run_for: Option<pmt::config::HumanDuration>,
    /// Also write the log under this directory, as `YYYY/MM/DD/trade.log` (overrides `[log] dir`).
    #[arg(long)]
    log_dir: Option<PathBuf>,
    /// Write the log to stdout only, even if the config names a log directory.
    #[arg(long, conflicts_with = "log_dir")]
    no_log_file: bool,
}

#[derive(Args)]
struct ReportArgs {
    #[command(flatten)]
    config: ConfigArg,
    /// Journal root. Default: `journal_dir` of the config.
    #[arg(long)]
    journal_dir: Option<PathBuf>,
    /// Which journal: paper or live. Default: the config's mode.
    #[arg(long, value_parser = ["paper", "live"])]
    mode: Option<String>,
    /// First day (UTC), YYYY-MM-DD.
    #[arg(long)]
    from: Option<NaiveDate>,
    /// Last day (UTC), inclusive.
    #[arg(long)]
    to: Option<NaiveDate>,
    /// Only these strategy ids. Repeat or comma-separate.
    #[arg(long, value_delimiter = ',')]
    strategy: Vec<String>,
    /// Period of the P&L table.
    #[arg(long, value_enum, default_value_t = Period::Day)]
    by: Period,
    /// Print the report as JSON instead of text.
    #[arg(long)]
    json: bool,
}

impl ReportArgs {
    fn options(self) -> Result<pmt::report::Options> {
        let cfg = match (&self.journal_dir, &self.mode) {
            (Some(_), Some(_)) => None,
            _ => Some(pmt::Config::load(&self.config.config)?),
        };
        let root = match (self.journal_dir, &cfg) {
            (Some(d), _) => d,
            (None, Some(c)) => c.journal_dir.clone(),
            (None, None) => unreachable!("config loaded"),
        };
        let mode = self
            .mode
            .unwrap_or_else(|| cfg.as_ref().map_or("paper", |c| c.mode.name()).to_string());
        Ok(pmt::report::Options {
            root,
            mode,
            from: self.from,
            to: self.to,
            strategies: self.strategy,
            by: self.by,
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Run(args) => {
            let mut cfg = pmt::Config::load(&args.config.config)?;
            if args.run_for.is_some() {
                cfg.run.run_for = args.run_for;
            }
            if args.no_log_file {
                cfg.log.dir = None;
            } else if let Some(d) = &args.log_dir {
                cfg.log.dir = Some(d.clone());
            }
            let file = cfg.log.dir.clone();
            let file = file.as_deref().map(|dir| LogFile { dir, name: "trade.log" });
            let _log = logging::init(&cfg.log.level, Console::Stdout, file)?;
            let ending = pmt::run(cfg).await?;
            anyhow::ensure!(
                ending != pmt::engine::Ending::TimedOut,
                "stopped at the drain timeout with trades still in progress"
            );
            Ok(())
        }
        Cmd::Check(args) => {
            let cfg = pmt::Config::load(&args.config)?;
            let built = pmt::strategies(&cfg)?;
            println!("{}: ok ({} mode)", args.config.display(), cfg.mode.name());
            println!("journal: {}", cfg.journal_dir.display());
            let leagues: Vec<&str> = cfg.leagues.iter().map(|l| l.key.as_str()).collect();
            println!("leagues: {}", leagues.join(", "));
            match cfg.run.run_for {
                Some(d) => println!("run: stop after {d}, wind down for at most {}", cfg.run.drain_timeout),
                None => println!("run: until stopped, wind down for at most {}", cfg.run.drain_timeout),
            }
            for s in &cfg.strategies {
                let on = if s.enabled { "enabled" } else { "disabled" };
                let cut = s
                    .entry_cutoff
                    .map_or(String::new(), |d| format!(", no new trades in the last {d}"));
                println!("strategy {} ({}): {on}{cut}", s.id(), s.kind);
            }
            anyhow::ensure!(!built.is_empty(), "no strategy is enabled");
            Ok(())
        }
        Cmd::Report(args) => {
            let json = args.json;
            let opts = args.options()?;
            let report = tokio::task::spawn_blocking(move || pmt::report::run(&opts)).await??;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                print!("{}", report.render());
            }
            Ok(())
        }
        Cmd::Strategies => {
            for k in pmt::strategy::KINDS {
                println!("{:<18} {}", k.name, k.about);
            }
            Ok(())
        }
    }
}
