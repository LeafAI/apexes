# F03.04 — Deterministic EMS parent identity and causal scheduling

Use a run-local parent-ID allocator, domain separated from matching
order/fill IDs and seeded by the same ReplayIdentityPolicy namespace.
Keep legacy InMemoryEms random UUIDs and PostgresEms unchanged.
For deterministic algorithmic orders require explicit EventLatencyPolicy:
the existing causal scheduler and arrival-capital checks must be used,
never silently fall back to wall time. Direct-only deterministic
replays still work without latency. Reject an algorithmic request
before parent reservation when latency is absent. Retain causal
admission-order scheduling and equal-time child arrival order.

This slice tests repeated real-runner algorithmic executions, parent
IDs visible to the strategy, child fills and report fingerprints.
It does not qualify arbitrary custom strategies/providers or every
multi-instrument liquidation/portfolio ordering path.

## Verification

Register the existing ReplayIdentityPolicy and EventLatencyPolicy
together with a verified manifest for deterministic algorithmic replay.
The parent ID allocator is a separate UUIDv8 SHA-256 domain and counter;
the existing causal event-time scheduler retains admission order.
A deterministic direct-only replay can still omit latency. If such
a strategy emits an algorithmic intent, the EMS refuses it before
reserving a parent (the runner handles it as a rejected submission).

Rust 1.99.0 locked offline all-target Clippy, formatting and 413
nonignored tests passed serially (375 backtest, 23 EMS, 15 paper).
Twelve external tests ignored. Two new regressions cover distinct
namespaces and repeated sealed two-instrument TWAP execution with
identical strategy-visible parent IDs, four FIFO child fills and
canonical report digests across ten runs; no latency produces no
parents or fills. The fixture uses separate symbols because the
existing pending-capital policy limits concurrent same-symbol
admissions. No risk/capital policy was loosened. Full workspace CI
and independent review pending; no real orders or database accessed.
