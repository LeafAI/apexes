//! [`MarginTierTable`]: a tiered maintenance-margin bracket table,
//! modeled on real exchanges' (e.g. Binance USDS-M Futures) tiered
//! margin schedules -- higher notional value means a higher
//! maintenance margin rate and a lower maximum allowed leverage.
//! Configured externally (not hardcoded) and passed by the caller to
//! both `apexes-risk::MarginMonitor` and
//! `apexes-backtest::PortfolioLedger`'s margin methods, keyed by
//! symbol, so both consumers always compute against the exact same
//! configuration.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// A single maintenance-margin bracket.
///
/// Derives `Serialize`/`Deserialize` so this type can be embedded
/// directly in `apexes_engine::launch_config::LaunchConfig` (the JSON
/// config `apexes-ui` hands to a spawned `apexes-engine` process) --
/// added purely additively, no existing behavior changes.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct MarginTier {
    /// This tier applies at/above this notional value (inclusive).
    pub notional_floor: Decimal,
    pub max_leverage: Decimal,
    pub maintenance_margin_rate: Decimal,
    /// Fixed deduction amount (the constant term real exchanges'
    /// tiered tables use to keep the maintenance-margin curve
    /// continuous across tier boundaries).
    pub maintenance_amount: Decimal,
}

/// One symbol's complete tiered margin table, sorted ascending by
/// `notional_floor`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarginTierTable {
    pub tiers: Vec<MarginTier>,
}

impl MarginTierTable {
    /// Returns the tier applicable to `notional` -- the LAST tier
    /// (highest `notional_floor`) whose `notional_floor <= notional`.
    /// If `notional` is below every tier's floor, returns the first
    /// (lowest) tier. Returns `None` only if `self.tiers` is empty
    /// (a misconfigured table -- callers should treat this as a
    /// construction-time error, not a runtime one to silently ignore).
    pub fn tier_for_notional(&self, notional: Decimal) -> Option<&MarginTier> {
        self.tiers
            .iter()
            .rev()
            .find(|tier| tier.notional_floor <= notional)
            .or_else(|| self.tiers.first())
    }
}

/// The real-time margin state of one leveraged position, computed by
/// `apexes_backtest::PortfolioLedger::margin_status` (the only place
/// with the actual ledger data) but defined here so both
/// `apexes-risk::MarginMonitor` and `apexes-strategy::StrategyContext`
/// (neither of which depends on `apexes-backtest`) can reference it.
/// Derives `Serialize`/`Deserialize` (added by `apexes-engine`'s Task
/// 8, design spec §6.2) so this type can be embedded in
/// `apexes_backtest::SessionSnapshot` and sent over `/session/ws` --
/// purely additive, no existing behavior changes.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct PositionMarginStatus {
    /// `None`: spot position, margin doesn't apply. `Some(ratio)`:
    /// current margin / maintenance margin requirement -- `<= 1.0`
    /// means liquidation has been triggered.
    pub margin_ratio: Option<Decimal>,
    /// `Some(price)` only for `MarginMode::Isolated` positions (a
    /// single, precise liquidation price is well-defined there).
    /// `None` for `MarginMode::Cross` positions -- since cross margin
    /// shares the whole account's equity as its pool, "this position's
    /// own" liquidation price isn't a precisely well-defined concept;
    /// only the shared account-level `margin_ratio` is meaningful.
    pub liquidation_price: Option<Decimal>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn sample_table() -> MarginTierTable {
        MarginTierTable {
            tiers: vec![
                MarginTier {
                    notional_floor: dec!(0),
                    max_leverage: dec!(20),
                    maintenance_margin_rate: dec!(0.005),
                    maintenance_amount: dec!(0),
                },
                MarginTier {
                    notional_floor: dec!(50000),
                    max_leverage: dec!(10),
                    maintenance_margin_rate: dec!(0.01),
                    maintenance_amount: dec!(250),
                },
                MarginTier {
                    notional_floor: dec!(250000),
                    max_leverage: dec!(5),
                    maintenance_margin_rate: dec!(0.025),
                    maintenance_amount: dec!(4000),
                },
            ],
        }
    }

    #[test]
    fn tier_for_notional_returns_lowest_tier_below_first_floor() {
        let table = sample_table();
        let tier = table.tier_for_notional(dec!(10000)).unwrap();
        assert_eq!(tier.max_leverage, dec!(20));
    }

    #[test]
    fn tier_for_notional_returns_exact_boundary_tier() {
        let table = sample_table();
        let tier = table.tier_for_notional(dec!(50000)).unwrap();
        assert_eq!(tier.max_leverage, dec!(10));
    }

    #[test]
    fn tier_for_notional_returns_highest_tier_above_last_floor() {
        let table = sample_table();
        let tier = table.tier_for_notional(dec!(1000000)).unwrap();
        assert_eq!(tier.max_leverage, dec!(5));
    }

    #[test]
    fn tier_for_notional_returns_none_for_empty_table() {
        let table = MarginTierTable { tiers: vec![] };
        assert!(table.tier_for_notional(dec!(100)).is_none());
    }
}
