# F03.03 / F04.02 — Interval-specific historical bar coverage

## Receipt contract

Receipt schema 2 applies to observation-order (schema 2) dataset
manifests. Retain the existing dataset ID, dataset version, source ID,
and replay SHA-256 binding. A bar receipt stream carries an explicit
`bar_interval` at the same level as `stream` and
`sequence_contract`, using the existing serialized `BarInterval`
names such as `OneSecond`, `OneMinute`, or `FiveMinutes`.
Stream identity consists of venue, symbol, event kind and, for bars,
interval. Non-bar streams omit `bar_interval`. Nulls, unknown intervals,
missing interval fields on bar streams and duplicate keys are invalid.

The manifest declares the base streams. The receipt declares which
intervals were covered for each bar stream. Every manifest stream needs
a receipt declaration, every observed record needs an exact matching
declaration, and every declared interval is reconciled even if its
windows all have zero counts. An interval absent from the receipts has
no coverage claim. Bar streams use `unavailable` and carry no sequence
range. Trade and depth sequence contracts retain their existing meaning.

Schema-1 receipts retain their serialized form and rules, including
schema-1 manifest bar coverage and schema-2 manifest coverage of non-bar
streams. Schema-1 receipts cannot cover a schema-2 manifest that
declares a bar stream. Neither manifest schema nor replay hashing
changes. Rust callers constructing `StreamCoverage` directly set
`bar_interval: None` for an existing receipt. The verifier never upgrades or derives collection claims from
an existing receipt or manifest.

## Reconciliation and bounds

Verify the manifest against the exact event sequence first. Windows
are contiguous, nonoverlapping, complete, half-open source-time ranges
covering the manifest's UTC-day range. A bar belongs to the window
containing its opening time, even when its scheduled closing time
falls in a later window or at midnight. Closing time is the earliest
historical observation time. Actual consumer arrival time may differ
and is outside this contract.

Within one venue, symbol and interval, a bar closes at its opening time
plus the interval's fixed duration. The verifier checks that addition
and rejects overlapping or duplicate openings for that stream. It
accepts sparse records and assumes no particular market session origin.
At a fixed duration, nondecreasing close time implies nondecreasing
open time within that stream. Each cursor can therefore reconcile its
windows in a single pass over the unchanged observation sequence,
even when opening times regress across different intervals and close
times tie. The verifier creates no synthetic bars.

Receipt IDs are globally unique. Counts and sequence arithmetic are
checked. Coverage limits remain at 100,000 windows across the receipt
with at most one cursor per declared stream and no per-bar identity
index. The verifier accepts caller-provided arrays, and the CLI reads
local JSON into memory as with the existing workflow. The separate
collector journal and native depth-range verification paths retain
their current contracts.

Reconciliation validates caller-supplied claims against the sealed
export. Both `independently_attested` and
`upstream_completeness_verified` remain false. Durable exchange capture,
independent production coverage, recorded live consumer arrival times
and storage/engine export integration remain separate work.

## CLI example

From the repository root, build the existing CLI and create a synthetic
export. The one-minute bar opens before the second record and closes at
the same time. Keep the closing-time tie in the supplied order.

```sh
cargo +1.99.0 build --locked -p apexes-backtest --bin historical-manifest
cli="$PWD/target/debug/historical-manifest"
example="$(mktemp -d)"
cd "$example"

python3 - <<'PY'
import json
from pathlib import Path

stream = {"symbol": "BTCUSDT", "venue": "Binance", "kind": "bar"}
definition = {
    "dataset_id": "synthetic-bars", "version": "v2", "source_id": "example",
    "start_day": "2026-01-01", "end_day_exclusive": "2026-01-02",
    "streams": [stream],
}

def bar(interval, opening, closing):
    return {"Bar": {
        "symbol": stream["symbol"], "venue": stream["venue"],
        "open": "100", "high": "101", "low": "99", "close": "100", "volume": "2",
        "interval": interval, "open_time": f"2026-01-01T{opening}Z",
        "close_time": f"2026-01-01T{closing}Z",
    }}

events = [
    bar("OneSecond", "00:00:00", "00:00:01"),
    bar("OneSecond", "00:00:59", "00:01:00"),
    bar("OneMinute", "00:00:00", "00:01:00"),
]
Path("definition.json").write_text(json.dumps(definition))
Path("events.json").write_text(json.dumps(events))
PY

"$cli" seal-observation definition.json events.json > manifest.json
jq '{
  schema_version: 2,
  dataset_id: .definition.dataset_id,
  dataset_version: .definition.version,
  source_id: .definition.source_id,
  replay_sha256,
  streams: [
    .definition.streams[0] as $stream |
    ["OneSecond", "OneMinute", "FiveMinutes"][] |
    {
      stream: $stream, bar_interval: ., sequence_contract: "unavailable",
      windows: [{
        receipt_id: ("synthetic-" + .),
        start: "2026-01-01T00:00:00Z", end_exclusive: "2026-01-02T00:00:00Z",
        complete: true, expected_count: ({"OneSecond": 2, "OneMinute": 1, "FiveMinutes": 0}[.]),
        sequence_range: null
      }]
    }
  ]
}' manifest.json > coverage.json

"$cli" verify-coverage manifest.json events.json coverage.json > verified.json
jq -e '(.event_count == 3) and (.source_coverage.empty_windows == 1)
  and (.upstream_completeness_verified == false)
  and (.source_coverage.independently_attested == false)' verified.json

jq '.streams[0].windows[0].expected_count = 1' coverage.json > wrong-receipt.json
! "$cli" verify-coverage manifest.json events.json wrong-receipt.json
jq '.[0].Bar.volume = "9"' events.json > wrong-events.json
! "$cli" verify-coverage manifest.json wrong-events.json coverage.json
```

The example counts are explicitly supplied synthetic collection claims.
Real counts must come from collection receipts. A manifest digest alone
cannot provide them.

## Verification

The integration suite seals and verifies a mixed event tape through the
public API and the CLI. It exercises overlapping intervals, tied closes,
midnight, sparse and explicitly empty windows, multiple instruments and
venues, receipt tampering, malformed bars and sequence overflow.

On Rust 1.99.0 with locked dependencies, 70 targeted regressions
passed, including 12 interval-coverage tests. The documented CLI
example passed and rejected both tampered inputs. Two separately
selected ClickHouse integration tests passed against the local test
database, including collector receipt reconciliation.

The locked workspace build and serial test run passed (1,595 passed,
111 ignored by the default run). All-target Clippy with warnings
denied, formatting and diff checks passed. Two of the ignored tests
were run separately as described above. The other ignored checks,
including exchange testnet checks, were left unrun.
