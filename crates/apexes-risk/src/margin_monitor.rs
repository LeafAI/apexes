//! [`MarginMonitor`]: trips (opens) for a symbol when its margin ratio
//! falls at or below `warning_threshold`, blocking new orders for that
//! symbol until it recovers. Architecturally mirrors
//! [`crate::circuit_breaker::CircuitBreaker`] (shared state behind
//! `Arc<Mutex<_>>`, `split()` into two thin trait adapters) -- but
//! updates its tripped state from `PositionMonitor::on_tick` (driven by
//! margin state, itself driven by mark price -- not fills), unlike
//! `CircuitBreaker`'s `InTradeMonitor::on_fill`.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use apexes_core::{Order, OrderId};
use apexes_strategy::{OrderIntent, Position, PositionMarginStatus};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::error::RiskViolation;
use crate::position_monitor::PositionMonitor;
use crate::pre_trade::PreTradeCheck;

pub struct MarginMonitor {
    /// A symbol trips when its `margin_ratio <= warning_threshold`
    /// (e.g. `1.5` -- trip BEFORE the `<= 1.0` auto-liquidation
    /// threshold `PortfolioLedger::check_liquidations` uses, giving a
    /// buffer for new-order blocking to actually help).
    warning_threshold: Decimal,
    tripped_symbols: HashSet<String>,
}

impl MarginMonitor {
    pub fn new(warning_threshold: Decimal) -> Self {
        Self {
            warning_threshold,
            tripped_symbols: HashSet::new(),
        }
    }

    pub fn split(self) -> (MarginMonitorPreTradeAdapter, MarginMonitorPositionAdapter) {
        let shared = Arc::new(Mutex::new(self));
        (
            MarginMonitorPreTradeAdapter(Arc::clone(&shared)),
            MarginMonitorPositionAdapter(shared),
        )
    }

    pub fn is_tripped(&self, symbol: &str) -> bool {
        self.tripped_symbols.contains(symbol)
    }

    pub fn reset(&mut self, symbol: &str) {
        self.tripped_symbols.remove(symbol);
    }

    fn on_tick_impl(
        &mut self,
        margin_status: &HashMap<String, PositionMarginStatus>,
    ) -> Result<(), Vec<RiskViolation>> {
        let mut violations = Vec::new();
        for (symbol, status) in margin_status {
            let Some(ratio) = status.margin_ratio else {
                continue;
            };
            if ratio <= self.warning_threshold {
                self.tripped_symbols.insert(symbol.clone());
                violations.push(RiskViolation::MarginRatioBreached {
                    symbol: symbol.clone(),
                    ratio,
                    threshold: self.warning_threshold,
                });
            } else {
                self.tripped_symbols.remove(symbol);
            }
        }
        if violations.is_empty() {
            Ok(())
        } else {
            Err(violations)
        }
    }

    fn check_impl(&self, intent: &OrderIntent) -> Result<(), Vec<RiskViolation>> {
        if self.tripped_symbols.contains(&intent.symbol) {
            return Err(vec![RiskViolation::MarginRatioBreached {
                symbol: intent.symbol.clone(),
                ratio: Decimal::ZERO,
                threshold: self.warning_threshold,
            }]);
        }
        Ok(())
    }
}

pub struct MarginMonitorPreTradeAdapter(Arc<Mutex<MarginMonitor>>);

impl PreTradeCheck for MarginMonitorPreTradeAdapter {
    fn check(
        &self,
        intent: &OrderIntent,
        _positions: &HashMap<String, Position>,
        _balances: &HashMap<String, Decimal>,
        _open_orders: &HashMap<OrderId, Order>,
        _now: DateTime<Utc>,
    ) -> Result<(), Vec<RiskViolation>> {
        let monitor = self.0.lock().unwrap_or_else(|p| p.into_inner());
        monitor.check_impl(intent)
    }
}

pub struct MarginMonitorPositionAdapter(Arc<Mutex<MarginMonitor>>);

impl PositionMonitor for MarginMonitorPositionAdapter {
    fn on_tick(
        &mut self,
        margin_status: &HashMap<String, PositionMarginStatus>,
    ) -> Result<(), Vec<RiskViolation>> {
        let mut monitor = self.0.lock().unwrap_or_else(|p| p.into_inner());
        monitor.on_tick_impl(margin_status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, OrderType, Side, Venue};
    use rust_decimal_macros::dec;

    fn sample_intent(symbol: &str) -> OrderIntent {
        OrderIntent {
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Perpetual,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: dec!(1),
            price: None,
            stop_price: None,
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: Some(dec!(10)),
            margin_mode: Some(apexes_strategy::MarginMode::Isolated),
        }
    }

    fn margin_status_with_ratio(
        symbol: &str,
        ratio: Decimal,
    ) -> HashMap<String, PositionMarginStatus> {
        let mut map = HashMap::new();
        map.insert(
            symbol.to_string(),
            PositionMarginStatus {
                margin_ratio: Some(ratio),
                liquidation_price: Some(dec!(50000)),
            },
        );
        map
    }

    #[test]
    fn trips_when_margin_ratio_breaches_warning_threshold() {
        let monitor = MarginMonitor::new(dec!(1.5));
        let (pre_trade, mut position) = monitor.split();

        let result = position.on_tick(&margin_status_with_ratio("BTCUSDT", dec!(1.2)));
        assert!(result.is_err());

        let check_result = pre_trade.check(
            &sample_intent("BTCUSDT"),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            Utc::now(),
        );
        assert!(check_result.is_err());
    }

    #[test]
    fn does_not_trip_for_a_healthy_margin_ratio() {
        let monitor = MarginMonitor::new(dec!(1.5));
        let (pre_trade, mut position) = monitor.split();

        let result = position.on_tick(&margin_status_with_ratio("BTCUSDT", dec!(3.0)));
        assert!(result.is_ok());

        let check_result = pre_trade.check(
            &sample_intent("BTCUSDT"),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            Utc::now(),
        );
        assert!(check_result.is_ok());
    }

    #[test]
    fn auto_resets_once_margin_ratio_recovers() {
        let monitor = MarginMonitor::new(dec!(1.5));
        let (pre_trade, mut position) = monitor.split();

        position
            .on_tick(&margin_status_with_ratio("BTCUSDT", dec!(1.2)))
            .unwrap_err();
        position
            .on_tick(&margin_status_with_ratio("BTCUSDT", dec!(2.0)))
            .unwrap();

        let check_result = pre_trade.check(
            &sample_intent("BTCUSDT"),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            Utc::now(),
        );
        assert!(check_result.is_ok());
    }
}
