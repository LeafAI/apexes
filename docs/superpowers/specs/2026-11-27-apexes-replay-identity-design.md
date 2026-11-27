# F03.04 — Opt-in deterministic simulated matching identity

Use a run-local namespace and sequential allocation for simulated order
and fill identity. Default matching engines (including paper/live)
continue using random UUIDs. Only the historical runner reads the
optional ReplayIdentityPolicy from its registry. The caller owns the
namespace; repeat it for the same sealed input and configuration, use
a different namespace for unrelated runs. Require a verified manifest,
as-fast-as-possible replay and no external control. Allocate before
checks as needed; rejected proposals may consume IDs deterministically.
Use domain-separated SHA-256 and UUID version 8, with no wall-clock or
global state. Use one allocator for direct, crossing, maker, stop and
simulated child order/fill IDs, retaining the original IDs across
trigger/arrival conversions.

Record the identity policy in the report. Provide a canonical report
digest (sorted JSON object keys, unchanged arrays) for comparison;
it is not an input/config/strategy attestation. Identity alone cannot
guarantee deterministic strategies, EMS parent identity/scheduling,
risk providers or portfolio processing. This is a first F03.04 slice,
not full feature completion. Keep real capture and maker-fill
qualification open until actual durable observations exist.

## Usage and qualification

Register `ReplayIdentityPolicy { namespace: Uuid }` alongside the
historical dataset manifest in the backtest registry. For comparison:

```sh
cargo run --locked -p apexes-backtest --bin historical-manifest -- \\
  fingerprint-replay backtest-report.json
```

The fingerprint covers the typed BacktestReport serialization, not
unknown JSON extension fields. It preserves array order, including
fills and equity points. It is not a digital signature or evidence
of identical strategy code. The policy is omitted for legacy reports
and does not change paper-mode default random identities.

Rust 1.99.0 locked offline all-target Clippy and 389 nonignored tests
(374 backtest, 15 paper) passed serially, three external tests ignored.
Three new regression tests cover run-local identity, ten repeated
sealed snapshot/trade runs with identical fill IDs/report digests,
missing manifest and non-fast replay refusal, legacy random identity,
and fingerprint CLI round-trip. Workspace CI and independent review
pending. The real capture database remains unavailable; no production
database, orders or accounts accessed. Empirical maker-fill evidence
remains unavailable. No F03.02/F03.03 completion claim.
