# F03.01 — Configurable execution fees

The roadmap acceptance is explicit fees changing P&L by the expected
amount and explicit labeling of zero fees. Existing book-walk prices
already include visible spread and depth; causal execution latency,
spread/impact fallback and IOC simulation parity belong to F03.02.

Expose a serializable, validated simulated fee schedule in engine launch
JSON: explicit zero, flat maker/taker basis points, or Binance Spot VIP
tier. Missing legacy fields preserve zero-rate behavior with an explicit
zero-fee label, not an assumption of realistic costs. Invalid rates or
VIP tiers fail before launch. VIP configuration is restricted to Binance
Spot instruments. Live configuration cannot inject simulated fees;
actual exchange fill commissions remain authoritative.

Backtest and paper runners capture the applied maker/taker rates per
configured venue in a report with explicit source/zero status; freeze
the schedule used by the matching engine to the recorded values for
configured venues. Unconfigured legacy routing retains the original
schedule and is outside report coverage. Old
reports with missing evidence are unknown, not zero. The engine session
identity includes the simulated fee classification, visible in the
operator banner. Keep legacy funding behavior separate. No synthetic
spread, slippage or causal latency changes in this increment.

Verify actual maker/taker charges and fee-inclusive equity on the
existing Drift baseline, config validation, backward compatibility,
report round-trip and paper application. No real exchange orders.
