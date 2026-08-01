//! Resolves an `AlgoOrderRequest`'s `OrderRouting` into concrete
//! per-venue quantity allocations, using `VenueScorer` for
//! `RouteToBest`/`Split`.

use apexes_core::AssetClass;
use apexes_strategy::{OrderRouting, VenueMarketState, VenueScorer};
use rust_decimal::Decimal;
use std::collections::HashMap;

use crate::ems::AlgoOrderRequest;
use crate::error::EmsError;
use crate::in_memory::state::VenueAllocation;

/// Builds the venue allocations for `request`, per its `routing` field:
/// - `Fixed`: a single allocation for the full quantity at the venue
///   implied by `request.venue`. **NOTE**: unlike `OrderIntent` (which
///   carries `asset_class` for `Fixed` routing), `AlgoOrderRequest` has
///   no `asset_class` field of its own -- `Fixed` routing for an algo
///   order means "use the single `AssetClass` implied by
///   `request.venue`'s configured default", which for this initial
///   scope is always `AssetClass::Spot` unless overridden via
///   `Split`/`RouteToBest`. This is a deliberate initial limitation:
///   `AlgoOrderRequest` does not currently expose a way to pin `Fixed`
///   routing to `Perpetual` -- flagged here as a known gap for a
///   future iteration, not silently worked around. For THIS plan's
///   scope, `Fixed` on an `AlgoOrderRequest` always resolves to
///   `AssetClass::Spot`.
/// - `RouteToBest`: scores `[Spot, Perpetual]` via `scorer`, picks the
///   single highest-scoring candidate for the FULL quantity.
/// - `Split(candidates)`: scores `candidates` via `scorer`, normalizes
///   scores into fractions summing to `1.0`, allocates
///   `request.quantity * fraction` to each.
pub(crate) fn resolve_allocations(
    request: &AlgoOrderRequest,
    scorer: &dyn VenueScorer,
    market_state: &VenueMarketState,
) -> Result<Vec<VenueAllocation>, EmsError> {
    match &request.routing {
        OrderRouting::Fixed => Ok(vec![VenueAllocation {
            asset_class: AssetClass::Spot,
            quantity: request.quantity,
            submitted_quantity: Decimal::ZERO,
        }]),
        OrderRouting::RouteToBest => {
            let candidates = [AssetClass::Spot, AssetClass::Perpetual];
            let scores = score_as_intent(request, scorer, &candidates, market_state);
            let best = scores
                .into_iter()
                .max_by(|a, b| a.1.cmp(&b.1))
                .ok_or_else(|| EmsError::ChildOrderSubmissionFailed {
                    parent_id: apexes_strategy::ParentOrderId(uuid::Uuid::nil()),
                    reason: "RouteToBest: no candidate venue has market data available".to_string(),
                })?;
            Ok(vec![VenueAllocation {
                asset_class: best.0,
                quantity: request.quantity,
                submitted_quantity: Decimal::ZERO,
            }])
        }
        OrderRouting::Split(candidates) => {
            let scores = score_as_intent(request, scorer, candidates, market_state);
            let total: Decimal = scores.values().copied().sum();
            if total <= Decimal::ZERO || scores.is_empty() {
                return Err(EmsError::ChildOrderSubmissionFailed {
                    parent_id: apexes_strategy::ParentOrderId(uuid::Uuid::nil()),
                    reason: "Split: no candidate venue has usable market data to derive weights"
                        .to_string(),
                });
            }
            Ok(scores
                .into_iter()
                .map(|(asset_class, score)| VenueAllocation {
                    asset_class,
                    quantity: request.quantity * (score / total),
                    submitted_quantity: Decimal::ZERO,
                })
                .collect())
        }
    }
}

/// `VenueScorer::score` takes an `&OrderIntent`, but `AlgoOrderRequest`
/// is a distinct, narrower type (no `execution_algo`/`routing`/`price`/
/// `stop_price`/`order_type` fields relevant to scoring) -- builds a
/// throwaway `OrderIntent` carrying only the fields `PriceAndFeeScorer`
/// (or any other `VenueScorer`) actually reads (`side`), to reuse the
/// existing scorer interface without adding a second, parallel
/// `VenueScorer`-for-algo-requests trait.
fn score_as_intent(
    request: &AlgoOrderRequest,
    scorer: &dyn VenueScorer,
    candidates: &[AssetClass],
    market_state: &VenueMarketState,
) -> HashMap<AssetClass, Decimal> {
    let intent = apexes_strategy::OrderIntent {
        symbol: request.symbol.clone(),
        venue: request.venue,
        asset_class: AssetClass::Spot,
        side: request.side,
        order_type: apexes_core::OrderType::Market,
        quantity: request.quantity,
        price: None,
        stop_price: None,
        execution_algo: None,
        routing: OrderRouting::Fixed,
    };
    scorer.score(&intent, candidates, market_state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{Side, Venue};
    use apexes_strategy::PriceAndFeeScorer;
    use rust_decimal_macros::dec;

    fn sample_request(routing: OrderRouting) -> AlgoOrderRequest {
        AlgoOrderRequest {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            quantity: dec!(10),
            execution_algo: apexes_strategy::ExecutionAlgoSpec::Twap {
                duration: std::time::Duration::from_secs(60),
                slices: 4,
            },
            routing,
        }
    }

    fn market_state() -> VenueMarketState {
        let mut best_bid_ask = HashMap::new();
        best_bid_ask.insert(AssetClass::Spot, (dec!(64990), dec!(65010)));
        best_bid_ask.insert(AssetClass::Perpetual, (dec!(64980), dec!(65000)));
        let mut estimated_fee_bps = HashMap::new();
        estimated_fee_bps.insert(AssetClass::Spot, dec!(10));
        estimated_fee_bps.insert(AssetClass::Perpetual, dec!(10));
        VenueMarketState {
            best_bid_ask,
            estimated_fee_bps,
        }
    }

    #[test]
    fn fixed_routing_allocates_full_quantity_to_spot() {
        let request = sample_request(OrderRouting::Fixed);
        let allocations =
            resolve_allocations(&request, &PriceAndFeeScorer, &market_state()).unwrap();
        assert_eq!(allocations.len(), 1);
        assert_eq!(allocations[0].asset_class, AssetClass::Spot);
        assert_eq!(allocations[0].quantity, dec!(10));
    }

    #[test]
    fn route_to_best_allocates_full_quantity_to_the_single_best_venue() {
        let request = sample_request(OrderRouting::RouteToBest);
        let allocations =
            resolve_allocations(&request, &PriceAndFeeScorer, &market_state()).unwrap();
        assert_eq!(allocations.len(), 1);
        // Perpetual has the lower ask in market_state() -- best for a buy.
        assert_eq!(allocations[0].asset_class, AssetClass::Perpetual);
        assert_eq!(allocations[0].quantity, dec!(10));
    }

    #[test]
    fn split_allocates_proportionally_to_normalized_scores_summing_to_total_quantity() {
        let request = sample_request(OrderRouting::Split(vec![
            AssetClass::Spot,
            AssetClass::Perpetual,
        ]));
        let allocations =
            resolve_allocations(&request, &PriceAndFeeScorer, &market_state()).unwrap();
        assert_eq!(allocations.len(), 2);
        let total: Decimal = allocations.iter().map(|a| a.quantity).sum();
        assert_eq!(total, dec!(10));
        // Both allocations are strictly positive (neither candidate is
        // starved to zero given how close the two venues' prices are).
        assert!(allocations.iter().all(|a| a.quantity > Decimal::ZERO));
    }

    #[test]
    fn split_with_no_market_data_at_all_is_an_error() {
        let request = sample_request(OrderRouting::Split(vec![
            AssetClass::Spot,
            AssetClass::Perpetual,
        ]));
        let empty_market_state = VenueMarketState {
            best_bid_ask: HashMap::new(),
            estimated_fee_bps: HashMap::new(),
        };
        let result = resolve_allocations(&request, &PriceAndFeeScorer, &empty_market_state);
        assert!(result.is_err());
    }
}
