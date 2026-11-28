# F04.05 — Independently selected opening collection receipt

## Contract

Seal the opening receipt only after successful strict read-only collection
against the exact unclaimed, execution-free checkpoint. Retain the
original predecessor, owner, complete wallet, independently selected
tracked amounts, both account-wide zero-open-order observation intervals
and original inspection timestamp. Domain-separated SHA-256 binds the
original selection (including initial parameters), full instrument
metadata and receipt body. Wallet rows are sorted; duplicate or
noncanonical rows are refused. Allocation must still match the explicit
independent selection, not be inferred from wallet amounts.

Use the existing create-new file and directory sync protocol with a
1 MiB canonical encoding limit for this receipt. Reopening requires a
digest independently retained at successful collection, not one obtained
from the same untrusted file. Check the original ordered intervals and
60-second observation bound without refreshing the recorded time.

Later inspection requires both the opening receipt and independently
selected durable closing collection receipt, exact selected coherent
boundary, instrument metadata and tracked amounts. Verify the original
predecessor and owner, validate the closing terminal and wallet evidence
against the durable journal, and check the entire opening collection
sequence precedes the first completed retained market observation and
all retained order creation and fill timestamps. Re-run exact closed
account cash-flow continuity including untracked assets and recheck the
durable journal. Return historical opening evidence only.

## Limits

Neither checksum nor independent selection authenticates the source
without a trusted collection and retention process. A first completed
observation does not establish the exact start of a callback; absence of
intervening activity and continuous quiescence are not proven. Wallet
equality is not evidence of strategy funding ownership or authoritative
transfer history. The protocol does not reconstruct a missing opening
observation, refresh historical evidence, restore live state, or grant
startup authority. No claim or watermark mutation.

## Verification

Extend the isolated PostgreSQL opening collection fixture to persist and
reopen the receipt before publishing a subsequent flat coherent boundary
with chronological market timestamps. Collect the closing receipt and
reinspect both against the exact preserved journal. Refuse overwrite,
incorrect digest, altered allocation, noncanonical encoding, changed
independent tracked amounts and changed instrument selection. Verify the
original time and untracked asset are retained and startup claim remains
unchanged.

Rust 1.99.0, locked dependencies: 236 OMS/EMS/live tests, 893
core/risk/research/engine tests and 27 Binance wiremock REST tests
passed (1,156 total, zero failures, four external ignored). Strict
workspace Clippy, formatting and diff checks passed; disposable
PostgreSQL stopped and removed. Full workspace build/test, independent
review, remote CI and real-service qualification remain outstanding.

## Next

Qualify the complete offline transition witness using independently
selected durable opening and closing collection receipts instead of
unbound wallet assertions, with exact accounting, risk and observation
checks. Keep funding provenance, actual runtime restoration and
post-execution startup authorization gated.
