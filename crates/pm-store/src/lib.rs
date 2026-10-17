//! Append-only recording store.
//!
//! Every record is one JSON line `{"t":<recv ns>,"s":<session>,"m":<payload>}` where `m` is the
//! payload exactly as received (or a JSON object we produced). Lines are zstd-compressed into
//!
//! ```text
//! <root>/<YYYY>/<MM>/<DD>/<category>/<league>/<stream>/<HH>.r<run>.jsonl.zst
//! ```
//!
//! e.g. `2026/10/16/esports/cs2/clob_book/18.r1792174620.jsonl.zst`. The date comes first so a
//! day's data is one directory (easy to move, back up or archive). Files are partitioned by the
//! record's receive time (UTC day / hour), [`Partition`] (category + league) and stream, and are
//! suffixed by the process start time,
//! so restarts never touch a previous run's files. Encoders are flushed every few seconds, so a
//! crash loses at most that much and earlier data stays decodable (`zstdcat` reads truncated
//! frames up to the last flushed block).
//!
//! All file I/O happens on one dedicated thread; producers send lines over a bounded channel
//! (back-pressure instead of unbounded memory growth).

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use tokio::sync::mpsc;

const HOUR_NS: i64 = 3_600_000_000_000;

#[derive(Clone, Debug)]
pub struct StoreConfig {
    pub root: PathBuf,
    pub zstd_level: i32,
    pub flush_every: Duration,
    pub channel_capacity: usize,
}

impl Default for StoreConfig {
    fn default() -> Self {
        StoreConfig {
            root: PathBuf::from("data"),
            zstd_level: 6,
            flush_every: Duration::from_secs(5),
            channel_capacity: 200_000,
        }
    }
}

/// Lines and uncompressed bytes written, per stream.
#[derive(Clone, Debug, Default)]
pub struct StreamStats {
    pub lines: u64,
    pub bytes: u64,
}

type Stats = Arc<Mutex<HashMap<String, StreamStats>>>;

/// The two directory levels under the date: a category (`esports`, `sports`, `_system`, …) and a league
/// (`cs2`, `dota2`, …). Both are sanitised to `[a-z0-9_-]`. Cheap to clone.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Partition {
    pub category: Arc<str>,
    pub league: Arc<str>,
}

impl Partition {
    pub fn new(category: &str, league: &str) -> Self {
        Partition { category: sanitize(category), league: sanitize(league) }
    }
}

enum Cmd {
    Line { part: Partition, stream: &'static str, t_ns: i64, line: String },
    Tick,
}

/// Cheap, cloneable producer side of the store.
#[derive(Clone)]
pub struct StoreHandle {
    tx: mpsc::Sender<Cmd>,
    stats: Stats,
}

impl StoreHandle {
    /// Writes `payload` (must be valid JSON: an object, array, string, …) wrapped in the envelope.
    pub async fn write(&self, part: &Partition, stream: &'static str, t_ns: i64, session: u64, payload: &str) {
        let mut line = String::with_capacity(payload.len() + 48);
        line.push_str("{\"t\":");
        line.push_str(&t_ns.to_string());
        line.push_str(",\"s\":");
        line.push_str(&session.to_string());
        line.push_str(",\"m\":");
        line.push_str(payload.trim());
        line.push_str("}\n");
        let cmd = Cmd::Line { part: part.clone(), stream, t_ns, line };
        if self.tx.send(cmd).await.is_err() {
            tracing::error!(stream, "store writer is gone; dropping record");
        }
    }

    pub fn stats(&self) -> HashMap<String, StreamStats> {
        self.stats.lock().map(|s| s.clone()).unwrap_or_default()
    }
}

fn sanitize(p: &str) -> Arc<str> {
    let s: String = p
        .chars()
        .map(|c| c.to_ascii_lowercase())
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect();
    Arc::from(if s.is_empty() { "unknown".to_string() } else { s })
}

/// Owns the writer thread. Call [`Store::shutdown`] after all handles are dropped (or at least
/// after producers stopped) to finish every open file cleanly.
pub struct Store {
    handle: Option<StoreHandle>,
    thread: Option<std::thread::JoinHandle<Result<()>>>,
    ticker: tokio::task::JoinHandle<()>,
}

impl Store {
    /// Must be called inside a tokio runtime (spawns the flush ticker).
    pub fn start(cfg: StoreConfig) -> Result<Store> {
        fs::create_dir_all(&cfg.root).with_context(|| format!("creating {}", cfg.root.display()))?;
        let (tx, rx) = mpsc::channel(cfg.channel_capacity);
        let stats: Stats = Arc::default();
        let run = Utc::now().timestamp();
        let writer = Writer { cfg: cfg.clone(), run, files: HashMap::new(), stats: stats.clone() };
        let thread = std::thread::Builder::new()
            .name("pm-store".into())
            .spawn(move || writer.run(rx))?;
        let tick_tx = tx.clone();
        let ticker = tokio::spawn(async move {
            let mut iv = tokio::time::interval(cfg.flush_every);
            iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                iv.tick().await;
                if tick_tx.send(Cmd::Tick).await.is_err() {
                    break;
                }
            }
        });
        Ok(Store { handle: Some(StoreHandle { tx, stats }), thread: Some(thread), ticker })
    }

    pub fn handle(&self) -> StoreHandle {
        self.handle.clone().expect("store already shut down")
    }

    /// Stops accepting records once every handle is dropped, drains the queue and finishes all
    /// files.
    pub async fn shutdown(mut self) -> Result<()> {
        self.ticker.abort();
        let _ = (&mut self.ticker).await;
        self.handle = None;
        let thread = self.thread.take().expect("joined twice");
        tokio::task::spawn_blocking(move || thread.join())
            .await?
            .map_err(|_| anyhow::anyhow!("store writer panicked"))?
    }
}

struct OpenFile {
    hour: i64,
    path: PathBuf,
    enc: zstd::stream::write::Encoder<'static, BufWriter<File>>,
    dirty: bool,
}

struct Writer {
    cfg: StoreConfig,
    run: i64,
    files: HashMap<(Partition, &'static str), OpenFile>,
    stats: Stats,
}

impl Writer {
    fn run(mut self, mut rx: mpsc::Receiver<Cmd>) -> Result<()> {
        while let Some(cmd) = rx.blocking_recv() {
            match cmd {
                Cmd::Line { part, stream, t_ns, line } => {
                    if let Err(e) = self.write_line(part, stream, t_ns, &line) {
                        tracing::error!(stream, "write failed: {e:#}");
                    }
                }
                Cmd::Tick => self.tick(),
            }
        }
        // All senders gone: finish everything.
        for (_, f) in self.files.drain() {
            finish(f);
        }
        Ok(())
    }

    fn write_line(&mut self, part: Partition, stream: &'static str, t_ns: i64, line: &str) -> Result<()> {
        let hour = t_ns.div_euclid(HOUR_NS);
        let key = (part, stream);
        // Only roll forward: a record a few ms late for the previous hour goes in the current file.
        let rotate = self.files.get(&key).is_some_and(|f| hour > f.hour);
        if rotate
            && let Some(f) = self.files.remove(&key) {
                finish(f);
            }
        if !self.files.contains_key(&key) {
            let f = self.open(&key.0, stream, hour)?;
            self.files.insert(key.clone(), f);
        }
        let f = self.files.get_mut(&key).expect("just inserted");
        f.enc.write_all(line.as_bytes())?;
        f.dirty = true;
        if let Ok(mut s) = self.stats.lock() {
            let e = s.entry(stream.to_string()).or_default();
            e.lines += 1;
            e.bytes += line.len() as u64;
        }
        Ok(())
    }

    fn open(&self, part: &Partition, stream: &str, hour: i64) -> Result<OpenFile> {
        let dt: DateTime<Utc> = DateTime::from_timestamp(hour * 3600, 0).context("bad timestamp")?;
        let dir = self
            .cfg
            .root
            .join(dt.format("%Y/%m/%d").to_string())
            .join(&*part.category)
            .join(&*part.league)
            .join(stream);
        fs::create_dir_all(&dir)?;
        let path = dir.join(format!("{}.r{}.jsonl.zst", dt.format("%H"), self.run));
        // Append: if this run reopens an hour, the new zstd frame simply follows the old one.
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let enc = zstd::stream::write::Encoder::new(BufWriter::with_capacity(1 << 16, file), self.cfg.zstd_level)?;
        tracing::debug!(path = %path.display(), "opened");
        Ok(OpenFile { hour, path, enc, dirty: false })
    }

    fn tick(&mut self) {
        let now_hour = Utc::now().timestamp().div_euclid(3600);
        let stale: Vec<_> = self.files.iter().filter(|(_, f)| f.hour < now_hour).map(|(k, _)| k.clone()).collect();
        for k in stale {
            if let Some(f) = self.files.remove(&k) {
                finish(f);
            }
        }
        for f in self.files.values_mut() {
            if f.dirty {
                if let Err(e) = f.enc.flush() {
                    tracing::error!(path = %f.path.display(), "flush failed: {e}");
                }
                f.dirty = false;
            }
        }
    }
}

fn finish(f: OpenFile) {
    let path = f.path.clone();
    match f.enc.finish().and_then(|mut w| w.flush()) {
        Ok(()) => tracing::debug!(path = %path.display(), "closed"),
        Err(e) => tracing::error!(path = %path.display(), "finish failed: {e}"),
    }
}

/// Lists recorded files of one partition and stream across all days, sorted by time.
pub fn list_files(root: &Path, part: &Partition, stream: &str) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for day in subdirs(root, 3)? {
        let dir = day.join(&*part.category).join(&*part.league).join(stream);
        if !dir.is_dir() {
            continue;
        }
        for e in fs::read_dir(&dir)? {
            let p = e?.path();
            if p.is_file() && p.to_string_lossy().ends_with(".jsonl.zst") {
                out.push(p);
            }
        }
    }
    out.sort();
    Ok(out)
}

/// Directories exactly `depth` levels below `root` (`depth = 3` gives the `YYYY/MM/DD` days).
fn subdirs(root: &Path, depth: usize) -> Result<Vec<PathBuf>> {
    let mut level = vec![root.to_path_buf()];
    for _ in 0..depth {
        let mut next = Vec::new();
        for d in &level {
            if !d.is_dir() {
                continue;
            }
            for e in fs::read_dir(d)? {
                let p = e?.path();
                if p.is_dir() {
                    next.push(p);
                }
            }
        }
        level = next;
    }
    Ok(level)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[tokio::test]
    async fn roundtrip_and_partitioning() {
        let root = std::env::temp_dir().join(format!("pm-store-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let store = Store::start(StoreConfig { root: root.clone(), ..Default::default() }).unwrap();
        let h = store.handle();
        let t0 = 1_792_166_400 * 1_000_000_000i64; // 2026-10-16 16:00:00 UTC
        let cs2 = Partition::new("Esports", "CS2");
        let dota = Partition::new("esports", "dota 2");
        h.write(&cs2, "clob", t0, 1, r#"{"a":1}"#).await;
        h.write(&cs2, "clob", t0 + 10, 1, r#"{"a":2}"#).await;
        h.write(&cs2, "clob", t0 + HOUR_NS, 2, r#"{"a":3}"#).await;
        h.write(&dota, "clob", t0, 1, r#"[1,2]"#).await;
        drop(h);
        store.shutdown().await.unwrap();

        let files = list_files(&root, &cs2, "clob").unwrap();
        assert_eq!(files.len(), 2, "{files:?}");
        assert!(files[0].to_string_lossy().contains("/2026/10/16/esports/cs2/clob/16.r"), "{files:?}");
        assert!(files[1].to_string_lossy().contains("/2026/10/16/esports/cs2/clob/17.r"));
        let mut s = String::new();
        zstd::stream::read::Decoder::new(File::open(&files[0]).unwrap()).unwrap().read_to_string(&mut s).unwrap();
        assert_eq!(s, format!("{{\"t\":{t0},\"s\":1,\"m\":{{\"a\":1}}}}\n{{\"t\":{},\"s\":1,\"m\":{{\"a\":2}}}}\n", t0 + 10));
        let d = list_files(&root, &dota, "clob").unwrap();
        assert!(d[0].to_string_lossy().contains("/2026/10/16/esports/dota_2/clob/"));
        fs::remove_dir_all(&root).unwrap();
    }
}
