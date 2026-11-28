# F04.05 — Scoped Drift checkpoint codecs

## Scope and contract

The versioned envelope merged in PR #98 is the only checkpoint entry point.
Opt in explicitly using `DriftScriptStrategy::from_file_with_scoped_checkpoints`
with the original script, initial parameter overrides and a nonempty bounded
instrument universe. The caller must first qualify that all risk-relevant
mutable script state resides in the event-bound scoped APIs and parameter
store, and that top-level initialization is deterministic. This is a
caller-declared restriction, not a general interpreter heap serializer or a
static proof of script purity. Legacy constructors and Trend Guard V1 remain
unchanged. An event outside the opted-in venue/symbol universe yields no
intents or scoped state changes.

The schema-1 payload binds the actual script-source SHA-256, sorted exact
venue/symbol universe and initial parameter store, in addition to the outer
contract's independently established identity, configuration, ownership and
observation cursor. Restore reconstructs an isolated candidate with the
original load-time overrides; hot changes restore separately so top-level
cached parameters retain their original meaning. Explicit parameter values
and declared defaults are distinct and deterministically ordered.

## State codecs

- Persist committed scoped numbers (exact finite IEEE-754 bit patterns),
  booleans, null, bounded strings and finite numeric arrays. Never persist
  callback closures, expired bindings, or arbitrary objects.
- Persist each named native indicator's configured period/deviations and
  complete accumulator state: SMA and rolling standard deviation windows
  and running sums; EMA value and count; RSI previous price, gain/loss
  averages and warm-up count; ATR previous close, true-range average and
  count; MACD three constituent EMAs; Bollinger constituent windows and
  width. Retain exact Decimal accumulators instead of reconstructing from
  insufficient trailing history. DTOs do not deserialize directly into
  runnable indicators. Check period, price, shape, counter and accumulator
  invariants, nested period agreement and composite consistency. Partial
  window sums must agree exactly; full windows allow bounded Decimal
  rounding drift (absolute or relative 1e-12, whichever is greater).
  Excessive drift is refused on capture as well as restore. These safety
  bounds cannot prove that every admissible value arose from authentic
  historical input.
- Persist last observed time, named cooldown grants and closed-bar
  watermarks by interval. Reject future timestamps relative to the selected
  checkpoint cursor, duplicate names/intervals, invalid capacity or
  inconsistent observation timestamps.
- Reject duplicate scope/name keys, unknown fields, invalid value shapes,
  oversized state, scope mismatches, wrong original script/initial
  parameters and invalid native configuration. Order keys and encode
  floating-point values as bits for deterministic serialization.
- Decode and validate the complete scoped state before installing it.
  Restore parameter state under its locks only after validation, and
  publish the fresh candidate through the outer restore contract. A failed
  candidate never replaces the active instance. SHA-256 is corruption
  detection, not authentication.

## Qualification

Native scoped codec tests restore every supported indicator and value
type at seven boundaries (empty, partial warm-up, warm and steady state)
over three scopes including the same symbol on two venues. Compare exact
payloads over subsequent observations; verify cooldown and closed-bar
deduplication, future-boundary rejection, duplicate and invalid state
refusal. Native accumulator regressions reject inconsistent sums and
configuration. Real Drift interpreter tests restore through the actual
checkpoint envelope, compare post-restart orders and checkpoint bytes
over interleaved observations, preserve hot and load-time parameter
semantics, and reject wrong source, parameters or instrument universe.
Invalid callback writes remain rolled back, foreign events produce no
intents, and legacy constructors remain unsupported for checkpoints.

Rust 1.99.0, locked dependencies, warnings denied: 733 core tests passed,
including six new checkpoint-specific tests, zero failures and six
external ignored tests. Workspace-wide all-target Clippy, formatting
and diff checks passed. Full workspace build/test, database service
tests for this slice, remote CI and independent review remain pending.

## Remaining boundaries

No automatic enablement for arbitrary scripts. Arbitrary interpreter
globals/closures, legacy global indicator handles, lifecycle callback
state not confined to the scoped contract, nondeterministic script
initialization and external effects are not covered. Durable checkpoint
storage/publication and independent recovery selection, verified
runner integration, atomic callback/state recovery and causal no-order
history warm-up remain open. No production database or exchange activity.
