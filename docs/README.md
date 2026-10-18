# pm-rs docs

[Usage guide](usage.md) · [Parquet dataset](dataset.md) · [developer guide](developer.md) ·
[leagues](leagues.md) · [data tools plan](plan-data-tools.md)

## Quick start

Build once from `pm-rs/`; everything runs through the one binary, `target/release/pmd`.

```bash
cargo build --release

# Record (runs until Ctrl-C; config/recorder.toml sets the leagues and data_dir)
./target/release/pmd record --config config/recorder.toml
./target/release/pmd record --data-dir /tmp/x --run-for 60    # 1-minute smoke test

# Unattended on a server: restarts after a crash, `tmux kill-session -t recorder` stops it cleanly
tmux new -d -s recorder 'cd ~/trading/polymarket-explore-claude/pm-rs && \
  while true; do ./target/release/pmd record; sleep 10; done'

# Check what was recorded: sizes, runs, connection drops, gaps, damaged files (read-only)
./target/release/pmd audit
./target/release/pmd audit --from 2026-10-17 --league cs2      # narrow it down
./target/release/pmd audit --json > audit.json                 # machine-readable

# Convert finished days to Parquet (data/parquet); re-runs only convert what changed
./target/release/pmd export

# What is on Polymarket right now: events, live games, volume by category and league
./target/release/pmd markets                                   # everything (~6 s)
./target/release/pmd markets --category esports --markets      # one category, with market counts
```

- Raw data lands in `data/live/YYYY/MM/DD/<category>/<league>/<stream>/HH.r<run>.jsonl.zst`
  (UTC). Parquet goes to `data/parquet/<table>/date=…/category=…/league=…/part.parquet`; see
  [dataset.md](dataset.md).
- The recorder log goes to stdout and `data/logs/YYYY/MM/DD/record.log` (one file per local day).
- `pmd <command> --help` lists every flag.
