//! [`MarketConditionConfig`]: tunable parameters for
//! [`crate::market_condition_monitor::MarketConditionMonitor`] --
//! thresholds, sensitivity, and which price source drives per-symbol
//! volatility/jump detection. See
//! `docs/superpowers/specs/2026-08-04-apexes-extreme-market-circuit-breaker-design.md`
//! for the full rationale.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Which `apexes_core::MarketEvent` variant's price field drives
/// per-symbol volatility/jump detection. Only events matching the
/// configured source are consumed by
/// `MarketConditionMonitor::on_market_event`; all other event types
/// are silently ignored (not an error -- e.g. a `Tick`/`Depth`/`Bar`
/// event arriving while `price_source: Trade` is configured is simply
/// a no-op for this monitor).
///
/// Derives `Serialize`/`Deserialize` (added additively) so
/// `MarketConditionConfig` can be embedded directly in
/// `apexes_engine::launch_config::LaunchConfig`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PriceSource {
    /// `MarketEvent::Trade.price` -- the default, appropriate for spot
    /// instruments.
    Trade,
    /// `MarketEvent::MarkPrice.mark_price` -- appropriate for perpetual
    /// futures, where the mark price (not the last trade print) is the
    /// index-and-funding-basis-adjusted price real exchanges use for
    /// this kind of risk calculation.
    MarkPrice,
}

/// Configuration for [`crate::market_condition_monitor::MarketConditionMonitor`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketConditionConfig {
    /// Absolute threshold for `apexes_indicators::RollingStdDev`'s
    /// output (same unit as price) -- a symbol's rolling volatility at
    /// or above this value is one of the two independent triggers for
    /// that symbol being flagged as "extreme".
    pub volatility_threshold: Decimal,
    /// Threshold for the percentage change between two consecutive
    /// price updates for the same symbol (e.g. `dec!(0.05)` = 5%) --
    /// the other of the two independent triggers.
    pub jump_threshold_pct: Decimal,
    /// Sensitivity coefficient in the linear de-risking-fraction
    /// formula: `reduce_fraction = min(1.0, sensitivity * (actual /
    /// threshold - 1.0))`, where `actual`/`threshold` is whichever of
    /// the volatility or jump ratio is more severe for that symbol.
    pub sensitivity: Decimal,
    /// Window size passed to `apexes_indicators::RollingStdDev::new`
    /// for each symbol's (and, if configured, the account's) rolling
    /// volatility state.
    pub volatility_window: usize,
    /// Which event type's price field drives volatility/jump
    /// detection.
    pub price_source: PriceSource,
    /// If `Some`, additionally enables account-level equity volatility
    /// detection using the SAME `sensitivity` formula (with this as
    /// the threshold), fed by `PortfolioLedger::equity()` via
    /// `MarketConditionMonitor::on_equity_update`. When it trips, the
    /// computed `reduce_fraction` is applied uniformly to every
    /// currently-open position's symbol (see
    /// `MarketConditionMonitor::reduce_fractions`'s doc comment).
    /// `None` (the default) disables account-level detection entirely.
    pub account_equity_volatility_threshold: Option<Decimal>,
    /// Whether de-risking partial closes apply to spot positions
    /// (`Position.leverage: None`) too, not just leveraged ones.
    /// Defaults to `false` (leveraged-only) at the call site, but this
    /// struct itself has no default -- every field is explicit, no
    /// implicit `Default` impl (matching this project's existing
    /// convention of no-implicit-defaults for risk-configuration
    /// structs, e.g. `apexes_risk::RiskLimits`).
    pub include_spot: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn sample_config() -> MarketConditionConfig {
        MarketConditionConfig {
            volatility_threshold: dec!(100),
            jump_threshold_pct: dec!(0.05),
            sensitivity: dec!(2),
            volatility_window: 20,
            price_source: PriceSource::Trade,
            account_equity_volatility_threshold: None,
            include_spot: false,
        }
    }

    #[test]
    fn config_is_constructible_and_comparable() {
        let a = sample_config();
        let b = sample_config();
        assert_eq!(a, b);
    }

    #[test]
    fn price_source_variants_are_distinct() {
        assert_ne!(PriceSource::Trade, PriceSource::MarkPrice);
    }
}
