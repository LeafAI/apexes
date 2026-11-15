//! [`CompositeRiskChecker`]: runs multiple `PreTradeCheck`/
//! `InTradeMonitor` implementations in sequence, aggregating every
//! violation from every checker into one combined result -- so a caller
//! (e.g. `run_backtest`) only needs to hold and invoke one object
//! regardless of how many individual risk rules are configured.

use std::collections::HashMap;
use std::sync::Mutex;

use apexes_core::{Fill, Order, OrderId};
use apexes_strategy::{OrderIntent, Position, PositionMarginStatus};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::error::RiskViolation;
use crate::in_trade::InTradeMonitor;
use crate::market_condition_monitor::MarketConditionEventMonitor;
use crate::position_monitor::PositionMonitor;
use crate::pre_trade::PreTradeCheck;

/// All fields are `Mutex`-wrapped so `check`/`on_fill`/`on_tick` can take `&self`
/// (not `&mut self`) -- required for `CompositeRiskChecker` to be
/// storable as `Arc<CompositeRiskChecker>` in
/// `apexes_core::registry::Registry` (sub-project #9's registry
/// retrofit), since `Arc` only ever grants shared (`&T`) access.
/// `in_trade_monitors` genuinely needs this (`InTradeMonitor::on_fill`
/// requires `&mut self` on each individual monitor); `pre_trade_checks`
/// is Mutex-wrapped too for consistency even though `PreTradeCheck::
/// check` already takes `&self` and needs no mutation here.
#[derive(Default)]
pub struct CompositeRiskChecker {
    pre_trade_checks: Mutex<Vec<Box<dyn PreTradeCheck>>>,
    in_trade_monitors: Mutex<Vec<Box<dyn InTradeMonitor>>>,
    position_monitors: Mutex<Vec<Box<dyn PositionMonitor>>>,
    market_condition_monitors: Mutex<Vec<Box<dyn MarketConditionEventMonitor>>>,
}

impl CompositeRiskChecker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_pre_trade_check(self, check: Box<dyn PreTradeCheck>) -> Self {
        self.pre_trade_checks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(check);
        self
    }

    pub fn with_in_trade_monitor(self, monitor: Box<dyn InTradeMonitor>) -> Self {
        self.in_trade_monitors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(monitor);
        self
    }

    pub fn with_position_monitor(self, monitor: Box<dyn PositionMonitor>) -> Self {
        self.position_monitors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(monitor);
        self
    }

    pub fn with_market_condition_monitor(
        self,
        monitor: Box<dyn MarketConditionEventMonitor>,
    ) -> Self {
        self.market_condition_monitors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(monitor);
        self
    }

    /// Runs every registered `PreTradeCheck`, collecting every
    /// violation from every checker (not stopping at the first
    /// checker that fails) into one combined `Vec`.
    pub fn check(
        &self,
        intent: &OrderIntent,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
        open_orders: &HashMap<OrderId, Order>,
        now: DateTime<Utc>,
    ) -> Result<(), Vec<RiskViolation>> {
        let checks = self
            .pre_trade_checks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let violations: Vec<RiskViolation> = checks
            .iter()
            .filter_map(|checker| {
                checker
                    .check(intent, positions, balances, open_orders, now)
                    .err()
            })
            .flatten()
            .collect();

        if violations.is_empty() {
            Ok(())
        } else {
            Err(violations)
        }
    }

    /// Tells every pre-trade check that `intent` was accepted for execution,
    /// so stateful limits (orders and volume per day) can advance. Call once
    /// per accepted order, from the layer that owns the submission. See
    /// `PreTradeCheck::on_order_accepted`.
    pub fn on_order_accepted(&self, intent: &OrderIntent, now: DateTime<Utc>) {
        let checks = self
            .pre_trade_checks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for checker in checks.iter() {
            checker.on_order_accepted(intent, now);
        }
    }

    /// Runs every registered `InTradeMonitor`'s `on_fill`, collecting
    /// every violation from every monitor into one combined `Vec`.
    pub fn on_fill(
        &self,
        fill: &Fill,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
    ) -> Result<(), Vec<RiskViolation>> {
        let mut monitors = self
            .in_trade_monitors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let violations: Vec<RiskViolation> = monitors
            .iter_mut()
            .filter_map(|monitor| monitor.on_fill(fill, positions, balances).err())
            .flatten()
            .collect();

        if violations.is_empty() {
            Ok(())
        } else {
            Err(violations)
        }
    }

    /// Runs every registered `PositionMonitor::on_tick`, collecting
    /// every violation from every monitor into one combined result.
    /// Called once per event-loop tick, independent of fills (see
    /// `PositionMonitor`'s own doc comment for why).
    pub fn on_tick(
        &self,
        margin_status: &HashMap<String, PositionMarginStatus>,
    ) -> Result<(), Vec<RiskViolation>> {
        let mut monitors = self
            .position_monitors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let violations: Vec<RiskViolation> = monitors
            .iter_mut()
            .filter_map(|monitor| monitor.on_tick(margin_status).err())
            .flatten()
            .collect();

        if violations.is_empty() {
            Ok(())
        } else {
            Err(violations)
        }
    }

    /// Runs every registered `MarketConditionEventMonitor::on_market_event`,
    /// collecting every violation from every monitor into one combined
    /// result. Called once per event-loop event (not once per tick like
    /// `on_tick`, since market-condition detection needs the raw event
    /// stream, not a derived per-tick snapshot).
    pub fn on_market_event(
        &self,
        event: &apexes_core::MarketEvent,
    ) -> Result<(), Vec<RiskViolation>> {
        let mut monitors = self
            .market_condition_monitors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let violations: Vec<RiskViolation> = monitors
            .iter_mut()
            .filter_map(|monitor| monitor.on_market_event(event).err())
            .flatten()
            .collect();

        if violations.is_empty() {
            Ok(())
        } else {
            Err(violations)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, OrderType, Side, Venue};
    use chrono::TimeZone;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    struct AlwaysRejectPreTrade(RiskViolation);

    impl PreTradeCheck for AlwaysRejectPreTrade {
        fn check(
            &self,
            _intent: &OrderIntent,
            _positions: &HashMap<String, Position>,
            _balances: &HashMap<String, Decimal>,
            _open_orders: &HashMap<OrderId, Order>,
            _now: DateTime<Utc>,
        ) -> Result<(), Vec<RiskViolation>> {
            Err(vec![self.0.clone()])
        }
    }

    struct AlwaysPassPreTrade;

    impl PreTradeCheck for AlwaysPassPreTrade {
        fn check(
            &self,
            _intent: &OrderIntent,
            _positions: &HashMap<String, Position>,
            _balances: &HashMap<String, Decimal>,
            _open_orders: &HashMap<OrderId, Order>,
            _now: DateTime<Utc>,
        ) -> Result<(), Vec<RiskViolation>> {
            Ok(())
        }
    }

    struct AlwaysRejectInTrade(RiskViolation);

    impl InTradeMonitor for AlwaysRejectInTrade {
        fn on_fill(
            &mut self,
            _fill: &Fill,
            _positions: &HashMap<String, Position>,
            _balances: &HashMap<String, Decimal>,
        ) -> Result<(), Vec<RiskViolation>> {
            Err(vec![self.0.clone()])
        }
    }

    struct AlwaysRejectPositionMonitor(RiskViolation);

    impl PositionMonitor for AlwaysRejectPositionMonitor {
        fn on_tick(
            &mut self,
            _margin_status: &HashMap<String, PositionMarginStatus>,
        ) -> Result<(), Vec<RiskViolation>> {
            Err(vec![self.0.clone()])
        }
    }

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap()
    }

    fn sample_intent() -> OrderIntent {
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
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        }
    }

    fn sample_fill() -> Fill {
        Fill {
            id: apexes_core::FillId(Uuid::new_v4()),
            order_id: apexes_core::OrderId(Uuid::new_v4()),
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            price: dec!(100),
            quantity: dec!(1),
            fee: dec!(0),
            fee_currency: None,
            filled_at: now(),
            leverage: None,
            margin_mode: None,
        }
    }

    #[test]
    fn no_checkers_registered_always_passes() {
        let composite = CompositeRiskChecker::new();
        let result = composite.check(
            &sample_intent(),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            now(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn violations_from_multiple_checkers_are_all_aggregated() {
        let composite = CompositeRiskChecker::new()
            .with_pre_trade_check(Box::new(AlwaysRejectPreTrade(
                RiskViolation::SymbolBlacklisted {
                    symbol: "BTCUSDT".to_string(),
                },
            )))
            .with_pre_trade_check(Box::new(AlwaysRejectPreTrade(
                RiskViolation::SelfTradeRisk {
                    symbol: "BTCUSDT".to_string(),
                },
            )));
        let result = composite.check(
            &sample_intent(),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            now(),
        );
        let violations = result.expect_err("expected both checkers' violations");
        assert_eq!(violations.len(), 2);
    }

    #[test]
    fn one_failing_checker_among_passing_ones_still_rejects() {
        let composite = CompositeRiskChecker::new()
            .with_pre_trade_check(Box::new(AlwaysPassPreTrade))
            .with_pre_trade_check(Box::new(AlwaysRejectPreTrade(
                RiskViolation::SymbolBlacklisted {
                    symbol: "BTCUSDT".to_string(),
                },
            )))
            .with_pre_trade_check(Box::new(AlwaysPassPreTrade));
        let result = composite.check(
            &sample_intent(),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            now(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn all_checkers_passing_returns_ok() {
        let composite = CompositeRiskChecker::new()
            .with_pre_trade_check(Box::new(AlwaysPassPreTrade))
            .with_pre_trade_check(Box::new(AlwaysPassPreTrade));
        let result = composite.check(
            &sample_intent(),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            now(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn on_fill_aggregates_violations_from_multiple_monitors() {
        let composite = CompositeRiskChecker::new()
            .with_in_trade_monitor(Box::new(AlwaysRejectInTrade(
                RiskViolation::EquityFloorBreached {
                    actual: dec!(0),
                    floor: dec!(100),
                },
            )))
            .with_in_trade_monitor(Box::new(AlwaysRejectInTrade(
                RiskViolation::PositionLimitExceeded {
                    symbol: "BTCUSDT".to_string(),
                    projected: dec!(20),
                    limit: dec!(10),
                },
            )));
        let result = composite.on_fill(&sample_fill(), &HashMap::new(), &HashMap::new());
        let violations = result.expect_err("expected both monitors' violations");
        assert_eq!(violations.len(), 2);
    }

    #[test]
    fn on_tick_aggregates_violations_from_multiple_monitors() {
        let composite = CompositeRiskChecker::new()
            .with_position_monitor(Box::new(AlwaysRejectPositionMonitor(
                RiskViolation::MarginRatioBreached {
                    symbol: "BTCUSDT".to_string(),
                    ratio: dec!(1.2),
                    threshold: dec!(1.5),
                },
            )))
            .with_position_monitor(Box::new(AlwaysRejectPositionMonitor(
                RiskViolation::MarginRatioBreached {
                    symbol: "ETHUSDT".to_string(),
                    ratio: dec!(1.3),
                    threshold: dec!(1.5),
                },
            )));
        let result = composite.on_tick(&HashMap::new());
        let violations = result.expect_err("expected both monitors' violations");
        assert_eq!(violations.len(), 2);
    }

    #[test]
    fn circuit_breaker_split_adapters_share_state_through_composite() {
        use crate::circuit_breaker::CircuitBreaker;
        use crate::limits::RiskLimits;

        let breaker = CircuitBreaker::new(RiskLimits {
            max_position_per_symbol: Some(dec!(10)),
            ..Default::default()
        });
        let (pre_trade_adapter, in_trade_adapter) = breaker.split();

        let composite = CompositeRiskChecker::new()
            .with_pre_trade_check(Box::new(pre_trade_adapter))
            .with_in_trade_monitor(Box::new(in_trade_adapter));

        let intent = sample_intent();
        assert!(
            composite
                .check(
                    &intent,
                    &HashMap::new(),
                    &HashMap::new(),
                    &HashMap::new(),
                    now()
                )
                .is_ok()
        );

        let mut positions = HashMap::new();
        positions.insert(
            "BTCUSDT".to_string(),
            Position {
                symbol: "BTCUSDT".to_string(),
                quantity: dec!(15),
                average_entry_price: dec!(100),
                leverage: None,
                margin_mode: None,
                allocated_margin: Decimal::ZERO,
            },
        );
        composite
            .on_fill(&sample_fill(), &positions, &HashMap::new())
            .unwrap_err();

        assert!(
            composite
                .check(
                    &intent,
                    &HashMap::new(),
                    &HashMap::new(),
                    &HashMap::new(),
                    now()
                )
                .is_err()
        );
    }

    struct AlwaysRejectMarketCondition(RiskViolation);

    impl MarketConditionEventMonitor for AlwaysRejectMarketCondition {
        fn on_market_event(
            &mut self,
            _event: &apexes_core::MarketEvent,
        ) -> Result<(), Vec<RiskViolation>> {
            Err(vec![self.0.clone()])
        }
    }

    #[test]
    fn on_market_event_aggregates_violations_from_multiple_monitors() {
        let composite = CompositeRiskChecker::new()
            .with_market_condition_monitor(Box::new(AlwaysRejectMarketCondition(
                RiskViolation::MarketConditionBreached {
                    symbol: "BTCUSDT".to_string(),
                    reduce_fraction: dec!(0.5),
                },
            )))
            .with_market_condition_monitor(Box::new(AlwaysRejectMarketCondition(
                RiskViolation::AccountWideMarketConditionBreached {
                    reduce_fraction: dec!(0.3),
                },
            )));

        let event = apexes_core::MarketEvent::Trade(apexes_core::TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(100),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        });

        let result = composite.on_market_event(&event);
        assert_eq!(result.unwrap_err().len(), 2);
    }
}
