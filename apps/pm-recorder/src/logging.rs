//! Log output: always stdout, optionally also appended to a file (same lines, no colours).

use std::io::IsTerminal;
use std::path::Path;

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

/// Installs the global subscriber. `RUST_LOG` overrides `level`. Keep the returned guard alive
/// until exit: dropping it flushes the file.
pub fn init(level: &str, file: Option<&Path>) -> Result<Option<WorkerGuard>> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));
    let stdout = fmt::layer()
        .with_timer(LocalTime)
        .with_target(false)
        .with_ansi(std::io::stdout().is_terminal());
    let (file_layer, guard) = match file {
        Some(path) => {
            if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
                std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
            }
            let f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .with_context(|| format!("opening log file {}", path.display()))?;
            let (w, guard) = tracing_appender::non_blocking(f);
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
    Ok(guard)
}
