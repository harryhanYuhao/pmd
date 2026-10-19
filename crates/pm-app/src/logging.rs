//! Log output: always the console (stdout for `pmd record`, stderr for commands whose report goes
//! to stdout), optionally also appended to a file per local day, `<dir>/YYYY/MM/DD/record.log`
//! (same lines, no colours). Each sink has its own writer thread, so a stalled stdout (dead ssh
//! session, paused terminal) cannot hold up the file or the tasks that log; stdout lines are
//! dropped instead.

use std::fs::File;
use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};

use chrono::NaiveDate;

use anyhow::{Context, Result};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt::{format::Writer, time::FormatTime};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

/// Local wall-clock time, second resolution: `2026-10-17 09:14:03`.
struct LocalTime;

impl FormatTime for LocalTime {
    fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        write!(w, "{}", chrono::Local::now().format("%Y-%m-%d %H:%M:%S"))
    }
}

/// Where console log lines go.
#[derive(Clone, Copy)]
pub enum Console {
    Stdout,
    Stderr,
}

/// Installs the global subscriber. `RUST_LOG` overrides `level`. Keep the returned guards alive
/// until exit: dropping them flushes the console and the file.
pub fn init(level: &str, console: Console, dir: Option<&Path>) -> Result<Vec<WorkerGuard>> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));
    let (ansi, (out, stdout_guard)) = match console {
        Console::Stdout => (
            std::io::stdout().is_terminal(),
            tracing_appender::non_blocking(std::io::stdout()),
        ),
        Console::Stderr => (
            std::io::stderr().is_terminal(),
            tracing_appender::non_blocking(std::io::stderr()),
        ),
    };
    let stdout = fmt::layer()
        .with_timer(LocalTime)
        .with_target(false)
        .with_ansi(ansi)
        .with_writer(out);
    let (file_layer, guard) = match dir {
        Some(dir) => {
            let mut daily = DailyFile::new(dir, "record.log");
            // Open today's file now, so that an unwritable log directory fails at startup.
            daily.open(today())?;
            let (w, guard) = tracing_appender::non_blocking(daily);
            let layer = fmt::layer()
                .with_timer(LocalTime)
                .with_target(false)
                .with_ansi(false)
                .with_writer(w);
            (Some(layer), Some(guard))
        }
        None => (None, None),
    };
    tracing_subscriber::registry()
        .with(filter)
        .with(stdout)
        .with(file_layer)
        .init();
    Ok([Some(stdout_guard), guard].into_iter().flatten().collect())
}

fn today() -> NaiveDate {
    chrono::Local::now().date_naive()
}

/// Appends to `<dir>/YYYY/MM/DD/<name>` for the current local day, moving to a new file at
/// midnight. Local, not UTC, so that every line in a file carries the date of its directory.
struct DailyFile {
    dir: PathBuf,
    name: &'static str,
    current: Option<(NaiveDate, File)>,
}

impl DailyFile {
    fn new(dir: &Path, name: &'static str) -> DailyFile {
        DailyFile {
            dir: dir.to_path_buf(),
            name,
            current: None,
        }
    }

    fn path(&self, day: NaiveDate) -> PathBuf {
        self.dir.join(day.format("%Y/%m/%d").to_string()).join(self.name)
    }

    /// Makes `day`'s file the current one. On failure there is no current file and the next write
    /// tries again.
    fn open(&mut self, day: NaiveDate) -> Result<&mut File> {
        if self.current.as_ref().is_none_or(|(d, _)| *d != day) {
            self.current = None;
            let path = self.path(day);
            let parent = path.parent().expect("day directory");
            std::fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
            let f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .with_context(|| format!("opening log file {}", path.display()))?;
            self.current = Some((day, f));
        }
        Ok(&mut self.current.as_mut().expect("just opened").1)
    }

    fn write_on(&mut self, day: NaiveDate, buf: &[u8]) -> std::io::Result<usize> {
        self.open(day).map_err(std::io::Error::other)?.write_all(buf)?;
        Ok(buf.len())
    }
}

impl Write for DailyFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.write_on(today(), buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match &mut self.current {
            Some((_, f)) => f.flush(),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn daily_file_moves_to_a_new_directory_each_day() {
        let dir = std::env::temp_dir().join(format!("pm-daily-log-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let day = |d| NaiveDate::from_ymd_opt(2026, 10, d).unwrap();
        let mut f = DailyFile::new(&dir, "record.log");
        f.write_on(day(17), b"a\n").unwrap();
        f.write_on(day(17), b"b\n").unwrap();
        f.write_on(day(18), b"c\n").unwrap();
        // A restart on the same day appends.
        let mut again = DailyFile::new(&dir, "record.log");
        again.write_on(day(18), b"d\n").unwrap();
        drop((f, again));

        let read = |p: &str| std::fs::read_to_string(dir.join(p)).unwrap();
        assert_eq!(read("2026/10/17/record.log"), "a\nb\n");
        assert_eq!(read("2026/10/18/record.log"), "c\nd\n");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn unwritable_directory_is_an_error_and_retried() {
        let dir = std::env::temp_dir().join(format!("pm-daily-log-bad-{}", std::process::id()));
        std::fs::write(&dir, "a file where the directory should be").unwrap();
        let day = NaiveDate::from_ymd_opt(2026, 10, 17).unwrap();
        let mut f = DailyFile::new(&dir, "record.log");
        assert!(f.open(day).is_err());
        assert!(f.write_on(day, b"x\n").is_err());
        std::fs::remove_file(&dir).unwrap();
        f.write_on(day, b"y\n").unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.join("2026/10/17/record.log")).unwrap(),
            "y\n"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
