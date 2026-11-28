# F04.04 — OMS client identity evidence

The existing OMS allocates its client UUID before network submission,
persists the order and Submitting state, and retains the unknown-outcome
guard. Reuse that identity; do not create a parallel submission path.

OrderManager::inspect_client_identity(id) is read-only. It obtains the
Order and parsed SubmissionState from a single persisted row in the
manager's bound store. An absent local row returns None without a
gateway request; database errors propagate. For a known row, it uses
the manager's bound gateway and full stored order for the existing
client-ID lookup. No user-supplied symbol, account or exchange ID is
substituted. Callers must use the correct store/gateway account binding;
this API does not independently attest to account ownership.

The returned IdentityEvidence separates the local snapshot from the
subsequent remote observation. RemoteIdentity::Found carries returned
exchange ID and status consistent with the stored identity. NotFound
is a negative lookup, not proof of nonexecution. Unavailable covers
failed or unsupported queries. Conflict covers blank returned ID, a
contradictory persisted exchange ID or a remote record for a locally
rejected submission. Raw gateway errors are not exposed by this API.
Local and remote status can legitimately differ; observations are not
a single transaction or an attestation of ledger settlement.

Binance's existing client-ID REST lookup now checks the response's
echoed clientOrderId, symbol, positive exchange orderId, and consistency
with any stored exchange ID before accepting status. Missing or
mismatching response fields fail the lookup rather than producing
positive or definitive negative evidence. Spot and USD-M futures use
the existing asset-class routing. At the inspection boundary Binance
identity validation errors are unavailable observations; other gateways
returning conflicting IDs can produce explicit Conflict. No changes
to submission, reservation, health gate release, fill accounting,
automatic retries or reconciliation state transitions. This is an
inspection capability, not a live recovery authorization.

Verification: four new non-service tests cover the read-only snapshot
classification, unsupported lookups, Binance mismatched/missing client
identity and symbol, invalid exchange ID and stored ID mismatch, and
valid USD-M route. Existing interrupted-submission PostgreSQL test
now checks read-only inspection after manager restart, unchanged
Submitting state and missing local ID before normal reconciliation.
That ignored service test compiled but was not executed locally.

Rust 1.99.0 locked offline serial: 769 nonignored tests passed
(132 OMS and Binance gateway; 637 backtest, strategy, EMS, paper,
indicators and loader). 59 external-service tests ignored. All-target
Clippy passed with warnings denied for OMS, Binance gateway, live
trading, engine, EMS, backtest and paper. Formatting/diff checks
passed. No actual account, order or production database accessed.
Workspace CI, independent review and service qualification pending.

Next: durably bind strategy intent identity to the pre-existing OMS
client UUID at the submission transaction, without altering default
admission or allowing uncertain submissions to retry. Full live
recovery, durable child/fill evidence and delivery remain open.
