# F04.05 — Additional registered monitor evidence

## Scope

Expand the read-only registered monitor evidence carried by the existing
durable observation/risk attachment. No risk decision, risk reset,
execution logic or startup authorization changes. This slice covers
symbol/venue lists, self-trade prevention and registered margin
monitors; it does not complete all monitor coverage.

## Evidence contract

Capture actual SymbolVenueList configuration with versioned, sorted
unique symbol and venue values. Preserve disabled, blacklist and
whitelist distinctions including empty sets. Validate symbol size,
control characters, canonical ordering, role and bounded counts.
SelfTradePreventer has no mutable configuration or retained state;
capture its explicit versioned slot rather than treating it as an
absent or unsupported monitor.

Capture MarginMonitor warning threshold and sorted actual tripped
symbols from both shared adapters, refusing poisoned locks, invalid
thresholds and out-of-bounds or noncanonical states. Inspection does
not change trips. Equality between two adapter snapshots is not proof
of shared identity or external atomicity.

Extend the registered risk DTO with optional ordered position slots,
keeping the existing count. An absent position vector with nonzero
legacy count means unsupported coverage. A present vector must match
the count and only contain margin snapshots or explicit Unsupported
entries. Default custom position hooks return Unsupported. Empty
position coverage remains omitted in new captures; old serialized
receipts omit the new field unchanged. Validate all supported snapshots
and role placement before returning the composite evidence.

Market-condition registration is still count-only and separate
market-condition and margin-tier coverage flags are unchanged. The
durable attachment continues to bind exact captured bytes to the
existing checkpoint receipt. No restoration API is provided.

## Qualification

Two new risk tests cover sorted list configuration, exact disabled
versus empty distinction, stateless slot version, actual margin trips
from both roles, legacy count-only coverage, invalid role placement,
duplicate symbols/venues, invalid thresholds and count mismatch.
Extend the actual coherent-stop PostgreSQL integration with the three
additional registered check types and verify exact capture after
reopening using the existing independent receipt.

Rust 1.99.0, locked dependencies, serial tests: 207 OMS/EMS/live
tests (97/32/78) passed with disposable PostgreSQL; 815 core/risk tests
(including all 74 risk-engine tests) and 60 engine tests passed
(1,082 total), zero failures, six external tests ignored. Workspace
all-target check and Clippy with warnings denied, formatting and
diff checks passed. Disposable database stopped and removed. Full
workspace build/test, independent review, remote CI and real-service
qualification pending. No real orders or production DB operations.

## Remaining work

Next qualify market-condition monitor and margin-tier configuration
coverage. Custom monitor state, shared adapter identity and atomicity,
offline risk/ledger state restoration, source completeness and
authoritative evidence acquisition remain unqualified. Keep the
one-time startup restriction; do not enable repeatable restart or
callback replay.
