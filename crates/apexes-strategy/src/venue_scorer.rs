//! [`VenueScorer`]: pluggable venue-scoring for smart order routing
//! (`OrderRouting::RouteToBest`) and cross-market splitting
//! (`OrderRouting::Split`). `PriceAndFeeScorer` is the one honestly-scoped
//! default implementation this sub-project ships -- see its own doc
//! comment for exactly what it does and does not consider.

use crate::strategy::OrderIntent;
use apexes_core::AssetClass;
use rust_decimal::Decimal;
use std::collections::HashMap;

/// A snapshot of per-venue market conditions passed to `VenueScorer::score`.
pub struct VenueMarketState {
    /// Best (bid, ask) price for each candidate `AssetClass`, sourced
    /// from the caller's most recent `MarketEvent::Depth` per venue.
    pub best_bid_ask: HashMap<AssetClass, (Decimal, Decimal)>,
    /// Estimated effective fee rate in basis points for each candidate
    /// `AssetClass`, pre-resolved by the caller (e.g. from its own
    /// `apexes_backtest::cost_model::FeeSchedule`) -- `apexes-strategy`
    /// does not depend on `apexes-backtest`, so this is plain data, not
    /// a `FeeSchedule` trait object.
    pub estimated_fee_bps: HashMap<AssetClass, Decimal>,
}

/// Scores candidate venues (`AssetClass`es of the same underlying
/// symbol, e.g. Spot vs. Perpetual) for a given `OrderIntent`. Higher is
/// better. Scores are only meaningful in relative terms: `RouteToBest`
/// picks the highest-scoring candidate; `Split` normalizes all
/// candidates' scores into fractions summing to `1.0` to determine
/// split ratios. No absolute scale is required.
pub trait VenueScorer: Send + Sync {
    fn score(
        &self,
        intent: &OrderIntent,
        candidates: &[AssetClass],
        market_state: &VenueMarketState,
    ) -> HashMap<AssetClass, Decimal>;
}

/// The only concrete `VenueScorer` this sub-project ships: scores each
/// candidate purely by "best available price net of estimated fee",
/// from the perspective of `intent.side` (buying prefers a lower
/// effective ask; selling prefers a higher effective bid).
///
/// **Known, deliberate limitations** (documented here so a future
/// reader/reviewer recognizes these as an intentional initial scope,
/// not an oversight): does NOT consider order-book depth beyond the
/// best price (so it cannot estimate market impact/fill probability
/// for large orders), does NOT consider regulatory/compliance
/// constraints (sub-project `#15`'s scope), and does NOT consider
/// cross-venue latency differences. A more sophisticated `VenueScorer`
/// can be substituted later via the registry (`apexes_core::registry`)
/// without any change to EMS's routing/splitting logic itself.
pub struct PriceAndFeeScorer;

impl VenueScorer for PriceAndFeeScorer {
    fn score(
        &self,
        intent: &OrderIntent,
        candidates: &[AssetClass],
        market_state: &VenueMarketState,
    ) -> HashMap<AssetClass, Decimal> {
        candidates
            .iter()
            .filter_map(|candidate| {
                let (bid, ask) = market_state.best_bid_ask.get(candidate)?;
                let fee_bps = market_state
                    .estimated_fee_bps
                    .get(candidate)
                    .copied()
                    .unwrap_or(Decimal::ZERO);
                let fee_fraction = fee_bps / Decimal::from(10_000);

                let effective_price = match intent.side {
                    apexes_core::Side::Buy => *ask * (Decimal::ONE + fee_fraction),
                    apexes_core::Side::Sell => *bid * (Decimal::ONE - fee_fraction),
                };
                if effective_price <= Decimal::ZERO {
                    return None;
                }

                // Lower effective price is better for a buy, higher is
                // better for a sell -- invert so "higher score = better"
                // holds uniformly for both sides.
                let score = match intent.side {
                    apexes_core::Side::Buy => Decimal::ONE / effective_price,
                    apexes_core::Side::Sell => effective_price,
                };
                Some((*candidate, score))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::OrderRouting;
    use apexes_core::{OrderType, Side, Venue};
    use rust_decimal_macros::dec;

    fn buy_intent() -> OrderIntent {
        OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: dec!(1),
            price: None,
            stop_price: None,
            execution_algo: None,
            routing: OrderRouting::Fixed,
        }
    }

    #[test]
    fn buy_prefers_the_venue_with_the_lower_effective_ask() {
        let scorer = PriceAndFeeScorer;
        let intent = buy_intent();
        let candidates = vec![AssetClass::Spot, AssetClass::Perpetual];
        let mut best_bid_ask = HashMap::new();
        best_bid_ask.insert(AssetClass::Spot, (dec!(64990), dec!(65010)));
        best_bid_ask.insert(AssetClass::Perpetual, (dec!(64980), dec!(65000)));
        let mut estimated_fee_bps = HashMap::new();
        estimated_fee_bps.insert(AssetClass::Spot, dec!(10));
        estimated_fee_bps.insert(AssetClass::Perpetual, dec!(10));
        let market_state = VenueMarketState {
            best_bid_ask,
            estimated_fee_bps,
        };

        let scores = scorer.score(&intent, &candidates, &market_state);

        assert!(scores[&AssetClass::Perpetual] > scores[&AssetClass::Spot]);
    }

    #[test]
    fn fee_can_flip_the_preferred_venue() {
        let scorer = PriceAndFeeScorer;
        let intent = buy_intent();
        let candidates = vec![AssetClass::Spot, AssetClass::Perpetual];
        let mut best_bid_ask = HashMap::new();
        // Perpetual has the better raw ask price...
        best_bid_ask.insert(AssetClass::Spot, (dec!(64990), dec!(65010)));
        best_bid_ask.insert(AssetClass::Perpetual, (dec!(64980), dec!(65000)));
        let mut estimated_fee_bps = HashMap::new();
        // ...but a much higher fee, more than eating the price advantage.
        estimated_fee_bps.insert(AssetClass::Spot, dec!(1));
        estimated_fee_bps.insert(AssetClass::Perpetual, dec!(50));
        let market_state = VenueMarketState {
            best_bid_ask,
            estimated_fee_bps,
        };

        let scores = scorer.score(&intent, &candidates, &market_state);

        assert!(scores[&AssetClass::Spot] > scores[&AssetClass::Perpetual]);
    }

    #[test]
    fn candidate_with_no_market_data_is_omitted_not_defaulted() {
        let scorer = PriceAndFeeScorer;
        let intent = buy_intent();
        let candidates = vec![AssetClass::Spot, AssetClass::Perpetual];
        let mut best_bid_ask = HashMap::new();
        best_bid_ask.insert(AssetClass::Spot, (dec!(64990), dec!(65010)));
        // No Perpetual entry at all.
        let market_state = VenueMarketState {
            best_bid_ask,
            estimated_fee_bps: HashMap::new(),
        };

        let scores = scorer.score(&intent, &candidates, &market_state);

        assert_eq!(scores.len(), 1);
        assert!(scores.contains_key(&AssetClass::Spot));
    }
}
