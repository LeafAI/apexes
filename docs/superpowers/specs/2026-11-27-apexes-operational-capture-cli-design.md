# F03.03 — Operational public depth capture

Expose the existing supervised raw-depth collector as an opt-in
`raw-depth-collect` binary in apexes-market-data. It is not a trading
session and resolves no exchange credentials. Require a canonical
uppercase symbol, explicit spot or usd_m_perpetual market, duration
between 1 and 3600 seconds, existing receipt directory, and explicitly
configured APEXES_CAPTURE_CLICKHOUSE_URL. Use a dedicated capture
database, never a live trading database.

Install signal handlers before initializing the store. Limit setup
to 30 seconds, request stop on duration expiry, SIGINT or SIGTERM
(on Unix), and await the existing collector's channel drain and synced
terminal receipt for up to 120 seconds. An early capture failure, write
failure or drain timeout fails closed with nonzero exit and no completion
JSON. Do not retry ambiguous writes or reuse a run. Database initialization
and insert errors omit connection details. Output diagnostic logging
on stderr and a JSON completion record on stdout only after acknowledged
inserts and the durable terminal receipt. Force termination, failed
drain and interrupted writes can leave incomplete journals; inspect
and preserve them, never declare them successful.

The duration limits the requested collection interval, not the
filesystem's ability to finish a write or the total process lifetime.
A successful intentional stop does not imply a usable synchronized
segment or continuous upstream coverage. The existing read-only
qualification command is the next gate; it refuses empty, invalidation-
only, incomplete or corrupt runs. Large runs may exceed the 256 MiB
qualification bound; use shorter capture runs. No changes to the default
market-data provider, engine, trading loop or account access.

## Usage

```sh
mkdir -p /path/to/capture-receipts
# Configure APEXES_CAPTURE_CLICKHOUSE_URL for an authorized isolated store.
cargo run --locked -p apexes-market-data --bin raw-depth-collect -- \\
  --symbol BTCUSDT --market spot --duration-seconds 60 \\
  --receipt-dir /path/to/capture-receipts > completion.json
cargo run --locked -p apexes-backtest --bin historical-manifest -- \\
  qualify-collector /path/to/capture-receipts/<run-id>.jsonl > qualified.json
```

Use the receipt_path from completion.json; its run_id is generated
uniquely by the collector. Use an independent receipt directory. Keep
the journal and the immutable database run. See the recorded-source
qualification design for export/replay requirements.

## Verification

Locked Rust 1.99.0 targeted tests and all-target Clippy with warnings
denied passed. 102 nonignored tests (60 market-data library, 5 CLI unit,
3 CLI subprocess, 34 storage), zero failures; 12 external tests ignored.
No real Binance or database access. Actual public-feed capture and
ClickHouse round-trip qualification, workspace CI and independent
review remain outstanding.
