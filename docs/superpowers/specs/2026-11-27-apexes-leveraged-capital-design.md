# F03.02 — Simulated leveraged capital reservations

Opt-in event-time linear perpetual intents reserve quote initial margin
for the full requested quantity plus modeled fees on either side.
Market buys use the highest required observed ask; sells use the best
observed bid. Limits and stop-limits use their limit, stops their trigger.
Actual fills can differ from these estimates (especially crossing sells
and gapped stops). Validate the entire proposed direct execution against
the projected ledger and other reservations before committing depth.
Keep only the proportional remainder hold for a working direct order.

Use the existing transit, parent-owned and resting reservation lifecycle.
Causal fixed-route leveraged EMS children are perpetual, not Spot;
require the submitted instrument and leverage contract to match.
Legacy routing and immediate/live execution remain unchanged.
Validate leverage >= 1, linear perpetual scope and explicit quote asset.
Do not use unrealized PnL or prospective close proceeds to fund admission.
This deliberately conservative estimate treats reducing and flipping
intents as opening the full quantity; actual execution uses the existing
ledger's close/flip settlement. No exchange margin-tier calibration or
emergency liquidation preemption is implied.

## Local qualification

Rust 1.99.0 locked workspace build, all-target workspace Clippy with
warnings denied, formatting and diff checks passed. All 80 test
binaries ran serially in groups: 1,481 passed (1,011 core, 144 database,
63 supervisor, 263 UI), zero failures, 44 external/manual tests ignored.
All 30 doc-test suites passed. Seven new deterministic regressions
cover margin-versus-notional valuation, shared Spot/short capital,
unfundable crossing sells, perpetual EMS child ownership, invalid
leverage and fee funding, depth preservation on refusal, and partial
resting short execution. Temporary isolated PostgreSQL was stopped.
No live orders or production databases used. Remote CI and independent
review remain pending.
