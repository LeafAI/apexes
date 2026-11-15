//! [`LiveRiskLimits`]: the pre-trade limits a live session must carry.
//!
//! A live engine used to run with no risk checker at all (the runner treats
//! an unregistered `CompositeRiskChecker` as "no risk checking"). Limits are
//! now a required part of the live launch config: the four dimensions that
//! bound a single mistake (order size, order notional, position size, daily
//! trade count) have no defaults and must be positive.

use apexes_risk::{CompositeRiskChecker, LimitChecker, RiskLimits};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LiveRiskLimits {
    /// Largest single order, in quote currency (price * quantity).
    pub max_order_notional: Decimal,
    /// Largest single order, in base units.
    pub max_order_quantity: Decimal,
    /// Largest net position per symbol, in base units.
    pub max_position_per_symbol: Decimal,
    /// Orders accepted per UTC day.
    pub max_daily_trade_count: u32,
    /// Optional. `apexes-risk` compares the raw sum of balances across all
    /// assets (not mark-to-market), so this is only meaningful for an
    /// account holding a single quote asset; leave unset otherwise.
    #[serde(default)]
    pub min_account_equity: Option<Decimal>,
    #[serde(default)]
    pub max_daily_volume: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid live risk limit {field}: {reason}")]
pub struct RiskLimitsError {
    pub field: &'static str,
    pub reason: &'static str,
}

impl LiveRiskLimits {
    pub fn validate(&self) -> Result<(), RiskLimitsError> {
        let positive = |field: &'static str, value: Decimal| {
            if value > Decimal::ZERO {
                Ok(())
            } else {
                Err(RiskLimitsError {
                    field,
                    reason: "must be greater than zero",
                })
            }
        };
        positive("max_order_notional", self.max_order_notional)?;
        positive("max_order_quantity", self.max_order_quantity)?;
        positive("max_position_per_symbol", self.max_position_per_symbol)?;
        if self.max_daily_trade_count == 0 {
            return Err(RiskLimitsError {
                field: "max_daily_trade_count",
                reason: "must be greater than zero",
            });
        }
        if let Some(floor) = self.min_account_equity
            && floor < Decimal::ZERO
        {
            return Err(RiskLimitsError {
                field: "min_account_equity",
                reason: "must not be negative",
            });
        }
        if let Some(volume) = self.max_daily_volume {
            positive("max_daily_volume", volume)?;
        }
        Ok(())
    }

    pub fn to_risk_limits(&self) -> RiskLimits {
        RiskLimits {
            max_order_quantity: Some(self.max_order_quantity),
            max_order_notional: Some(self.max_order_notional),
            max_position_per_symbol: Some(self.max_position_per_symbol),
            min_account_equity: self.min_account_equity,
            max_daily_trade_count: Some(self.max_daily_trade_count),
            max_daily_volume: self.max_daily_volume,
        }
    }

    /// The checker `run_live_trading` looks up in the registry.
    pub fn build_checker(&self) -> CompositeRiskChecker {
        CompositeRiskChecker::new()
            .with_pre_trade_check(Box::new(LimitChecker::new(self.to_risk_limits())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    pub(crate) fn sample() -> LiveRiskLimits {
        LiveRiskLimits {
            max_order_notional: dec!(100),
            max_order_quantity: dec!(0.01),
            max_position_per_symbol: dec!(0.05),
            max_daily_trade_count: 50,
            min_account_equity: None,
            max_daily_volume: None,
        }
    }

    #[test]
    fn a_complete_positive_set_validates() {
        assert!(sample().validate().is_ok());
    }

    #[test]
    fn each_required_limit_rejects_zero_and_negative() {
        for (field, mutate) in [
            (
                "max_order_notional",
                (|l: &mut LiveRiskLimits| l.max_order_notional = dec!(0)) as fn(&mut _),
            ),
            ("max_order_quantity", |l| l.max_order_quantity = dec!(-1)),
            ("max_position_per_symbol", |l| {
                l.max_position_per_symbol = dec!(0)
            }),
            ("max_daily_trade_count", |l| l.max_daily_trade_count = 0),
        ] {
            let mut limits = sample();
            mutate(&mut limits);
            assert_eq!(limits.validate().unwrap_err().field, field);
        }
    }

    #[test]
    fn optional_limits_are_validated_when_present() {
        let mut limits = sample();
        limits.min_account_equity = Some(dec!(-1));
        assert_eq!(limits.validate().unwrap_err().field, "min_account_equity");
        let mut limits = sample();
        limits.max_daily_volume = Some(dec!(0));
        assert_eq!(limits.validate().unwrap_err().field, "max_daily_volume");
    }

    #[test]
    fn missing_required_field_is_a_parse_error_naming_it() {
        let json = r#"{"max_order_notional":"100","max_order_quantity":"0.01","max_position_per_symbol":"0.05"}"#;
        let error = serde_json::from_str::<LiveRiskLimits>(json)
            .unwrap_err()
            .to_string();
        assert!(error.contains("max_daily_trade_count"), "{error}");
    }

    #[test]
    fn conversion_maps_every_field() {
        let mut limits = sample();
        limits.min_account_equity = Some(dec!(10));
        limits.max_daily_volume = Some(dec!(1000));
        let converted = limits.to_risk_limits();
        assert_eq!(converted.max_order_notional, Some(dec!(100)));
        assert_eq!(converted.max_order_quantity, Some(dec!(0.01)));
        assert_eq!(converted.max_position_per_symbol, Some(dec!(0.05)));
        assert_eq!(converted.max_daily_trade_count, Some(50));
        assert_eq!(converted.min_account_equity, Some(dec!(10)));
        assert_eq!(converted.max_daily_volume, Some(dec!(1000)));
    }

    fn buy(quantity: Decimal, price: Decimal) -> apexes_strategy::OrderIntent {
        apexes_strategy::OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: apexes_core::Venue::Binance,
            asset_class: apexes_core::AssetClass::Spot,
            side: apexes_core::Side::Buy,
            order_type: apexes_core::OrderType::Limit,
            quantity,
            price: Some(price),
            stop_price: None,
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        }
    }

    fn check(
        checker: &CompositeRiskChecker,
        intent: &apexes_strategy::OrderIntent,
    ) -> Result<(), Vec<apexes_risk::RiskViolation>> {
        checker.check(
            intent,
            &std::collections::HashMap::new(),
            &std::collections::HashMap::new(),
            &std::collections::HashMap::new(),
            chrono::Utc::now(),
        )
    }

    /// The point of making limits mandatory: the checker built from them
    /// rejects what a runaway strategy would send, and passes a small order.
    #[test]
    fn the_built_checker_rejects_over_limit_orders_and_passes_small_ones() {
        let checker = sample().build_checker();

        assert!(
            check(&checker, &buy(dec!(0.001), dec!(50000))).is_ok(),
            "50 USDT is within limits"
        );

        let too_big_quantity = check(&checker, &buy(dec!(1), dec!(1))).unwrap_err();
        assert!(
            too_big_quantity
                .iter()
                .any(|v| matches!(v, apexes_risk::RiskViolation::OrderQuantityExceeded { .. }))
        );

        let too_big_notional = check(&checker, &buy(dec!(0.01), dec!(50000))).unwrap_err();
        assert!(
            too_big_notional
                .iter()
                .any(|v| matches!(v, apexes_risk::RiskViolation::OrderNotionalExceeded { .. }))
        );
    }

    #[test]
    fn the_daily_trade_cap_is_enforced() {
        let mut limits = sample();
        limits.max_daily_trade_count = 1;
        let checker = limits.build_checker();
        let intent = buy(dec!(0.001), dec!(50000));
        assert!(
            check(&checker, &intent).is_ok(),
            "first order of the day passes"
        );
        assert!(
            check(&checker, &intent).is_ok(),
            "asking again uses no allowance: only an accepted order does"
        );
        checker.on_order_accepted(&intent, chrono::Utc::now());
        let violations = check(&checker, &intent).unwrap_err();
        assert!(violations.iter().any(|v| matches!(
            v,
            apexes_risk::RiskViolation::DailyTradeCountExceeded { .. }
        )));
    }
}
