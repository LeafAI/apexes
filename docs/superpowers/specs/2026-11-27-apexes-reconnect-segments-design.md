# F03.03 — Reconnect-aware historical segmentation

Only apply this contract to receipt-bound raw Binance captures. Keep
the generic historical range verifier's uninterrupted-chain contract
unchanged. Scan in original receipt order across batch boundaries:
every synchronized segment begins with a sequenced snapshot and an
advancing native-range bridge. Spot requires inclusive next ID and no
previous-final field; USD-M requires the initial snapshot bridge and
exact previous-final links thereafter. Reject incremental data without
an anchor, a second snapshot without explicit invalidation, missing
native range endpoints, and incomplete snapshot-only segments. A
reconnect is allowed to reset IDs; never bridge across invalidation.

Record zero-based half-open event index ranges for synchronized
segments and contiguous invalidation marker groups, including leading,
repeated and trailing invalidations. Record first/last observation
timestamps and anchor/final IDs per segment. A gap's adjacent valid
publication timestamps are only bounds on observed availability,
not exchange disconnect/reconnect times. No invented time coverage
before first snapshot, after last event or between publications. An
invalidation-only run may be reconciled for diagnostics but may not
qualify for replay. No claim of upstream completeness or independently
attested source coverage is made.

Preserve invalidation events in replay. After full receipt and segment
validation, promote explicitly configured Contiguous depth policy to
VerifiedRanges; retain its existing age bound and leave SnapshotOnly
and absent policy unchanged. Existing strict book invalidation discards
the old chain and queue state; a fresh snapshot is required. Do not
relax the independent uninterrupted range-evidence verifier. Include
segment/gap report in reconciliation and verified backtest report.
Older serialized reports omit segment evidence rather than imply it.

## Qualification

Rust 1.99.0 locked offline all-target check, Clippy with warnings
denied, formatting and diff checks passed. All 365 nonignored
backtest tests passed serially (six new regressions), one external
storage test ignored. Tests cover leading/repeated/trailing gaps,
Spot and USD-M bridge/link rules, missing invalidation, snapshot-only
segments, cross-batch bridge recovery, diagnostic invalidation-only
runs and the strict replay policy. Full workspace CI and independent
review pending. No production data or accounts accessed. This change
depends on PR #56; do not merge ahead of it.
