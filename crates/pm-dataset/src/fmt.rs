//! Human-readable numbers, sizes, durations and times for reports.

use chrono::{DateTime, Utc};

pub fn bytes(b: f64) -> String {
    match b {
        b if b >= 1e12 => format!("{:.2} TB", b / 1e12),
        b if b >= 1e9 => format!("{:.2} GB", b / 1e9),
        b if b >= 1e6 => format!("{:.1} MB", b / 1e6),
        b if b >= 1e3 => format!("{:.0} KB", b / 1e3),
        b => format!("{b:.0} B"),
    }
}

pub fn count(n: u64) -> String {
    match n {
        n if n >= 1_000_000_000 => format!("{:.2}B", n as f64 / 1e9),
        n if n >= 1_000_000 => format!("{:.2}M", n as f64 / 1e6),
        n if n >= 10_000 => format!("{:.1}k", n as f64 / 1e3),
        n => n.to_string(),
    }
}

/// `$44.2M`, `$310k`, `$950`.
pub fn usd(x: f64) -> String {
    match x.abs() {
        a if a >= 1e9 => format!("${:.2}B", x / 1e9),
        a if a >= 1e6 => format!("${:.1}M", x / 1e6),
        a if a >= 1e3 => format!("${:.0}k", x / 1e3),
        _ => format!("${x:.0}"),
    }
}

/// `3d4h`, `2h05m`, `7m12s`, `40s`.
pub fn secs(s: f64) -> String {
    let s = s.max(0.0).round() as u64;
    match s {
        s if s >= 86_400 => format!("{}d{}h", s / 86_400, s % 86_400 / 3600),
        s if s >= 3600 => format!("{}h{:02}m", s / 3600, s % 3600 / 60),
        s if s >= 60 => format!("{}m{:02}s", s / 60, s % 60),
        s => format!("{s}s"),
    }
}

/// UTC timestamp at second resolution, `2026-10-17 19:42:08Z`.
pub fn time_ns(t_ns: i64) -> String {
    DateTime::<Utc>::from_timestamp_nanos(t_ns)
        .format("%Y-%m-%d %H:%M:%SZ")
        .to_string()
}
