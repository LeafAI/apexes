//! [`CircuitBreaker`]: trips (opens) when a fill causes a position-type
//! limit (per-symbol position or account equity) to be breached --
//! catching violations that accumulate across multiple partial fills,
//! which no single `PreTradeCheck` call could see coming since each
//! individual originating order may have looked fine in isolation.
//! While open, `PreTradeCheck::check` rejects every new order in that
//! scope. Auto-resets once the breaching condition clears (position
//! back within limits); `reset` allows early manual override.
//!
//! Because `CircuitBreaker` must be registered as BOTH a
//! `PreTradeCheck` (§`CircuitBreakerPreTradeAdapter`) and an
//! `InTradeMonitor` (§`CircuitBreakerInTradeAdapter`) sharing the exact
//! same underlying state, [`CircuitBreaker::split`] wraps one
//! `CircuitBreaker` in an `Arc<Mutex<_>>` and returns two thin adapter
//! structs, each implementing one trait by locking the shared mutex.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use apexes_core::{Fill, Order, OrderId};
use apexes_strategy::{OrderIntent, Position};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::error::{CircuitBreakerScope, RiskViolation};
use crate::in_trade::InTradeMonitor;
use crate::limits::RiskLimits;
use crate::pre_trade::PreTradeCheck;

pub struct CircuitBreaker {
    limits: RiskLimits,
    /// Symbols currently tripped (per-symbol position limit breached).
    tripped_symbols: HashSet<String>,
    /// Whether the account-wide equity floor is currently breached.
    account_tripped: bool,
}

impl CircuitBreaker {
    pub fn new(limits: RiskLimits) -> Self {
        Self {
            limits,
            tripped_symbols: HashSet::new(),
            account_tripped: false,
        }
    }

    /// Read-only capture of retained trips, including ones not
    /// inferable from the current ledger. No reset or restore operation.
    pub fn inspect_checkpoint(&self) -> Result<crate::CircuitStateSnapshot, crate::RiskStateError> {
        let mut tripped_symbols: Vec<_> = self.tripped_symbols.iter().cloned().collect();
        tripped_symbols.sort();
        let snapshot = crate::CircuitStateSnapshot {
            version: 1,
            limits: self.limits.clone(),
            tripped_symbols,
            account_tripped: self.account_tripped,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    /// Wraps `self` in `Arc<Mutex<_>>` and returns two adapters sharing
    /// that same instance: the first implements `PreTradeCheck`, the
    /// second implements `InTradeMonitor`. Register both with a
    /// `CompositeRiskChecker` (Task 7) to get a circuit breaker that
    /// both rejects new orders while tripped AND updates its own
    /// tripped state from fills.
    pub fn split(self) -> (CircuitBreakerPreTradeAdapter, CircuitBreakerInTradeAdapter) {
        let shared = Arc::new(Mutex::new(self));
        (
            CircuitBreakerPreTradeAdapter(Arc::clone(&shared)),
            CircuitBreakerInTradeAdapter(shared),
        )
    }

    /// Manually clears a trip before its auto-reset condition (position
    /// back within limits) would otherwise clear it -- e.g. a human
    /// operator confirming the position is intentional and safe to
    /// resume trading against.
    pub fn reset(&mut self, scope: &CircuitBreakerScope) {
        match scope {
            CircuitBreakerScope::Symbol(symbol) => {
                self.tripped_symbols.remove(symbol);
            }
            CircuitBreakerScope::Account => {
                self.account_tripped = false;
            }
        }
    }

    pub fn is_tripped(&self, scope: &CircuitBreakerScope) -> bool {
        match scope {
            CircuitBreakerScope::Symbol(symbol) => self.tripped_symbols.contains(symbol),
            CircuitBreakerScope::Account => self.account_tripped,
        }
    }

    fn on_fill_impl(
        &mut self,
        fill: &Fill,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
    ) -> Result<(), Vec<RiskViolation>> {
        let mut violations = Vec::new();

        if let Some(max_position) = self.limits.max_position_per_symbol {
            let current = positions
                .get(&fill.symbol)
                .map(|p| p.quantity.abs())
                .unwrap_or(Decimal::ZERO);
            if current > max_position {
                self.tripped_symbols.insert(fill.symbol.clone());
                violations.push(RiskViolation::PositionLimitExceeded {
                    symbol: fill.symbol.clone(),
                    projected: current,
                    limit: max_position,
                });
            } else {
                // Auto-reset: position back within limits.
                self.tripped_symbols.remove(&fill.symbol);
            }
        }

        if let Some(floor) = self.limits.min_account_equity {
            let total_balance: Decimal = balances.values().copied().sum();
            if total_balance < floor {
                self.account_tripped = true;
                violations.push(RiskViolation::EquityFloorBreached {
                    actual: total_balance,
                    floor,
                });
            } else {
                self.account_tripped = false;
            }
        }

        if violations.is_empty() {
            Ok(())
        } else {
            Err(violations)
        }
    }

    fn check_impl(&self, intent: &OrderIntent) -> Result<(), Vec<RiskViolation>> {
        let mut violations = Vec::new();

        if self.tripped_symbols.contains(&intent.symbol) {
            violations.push(RiskViolation::CircuitBreakerOpen {
                scope: CircuitBreakerScope::Symbol(intent.symbol.clone()),
            });
        }
        if self.account_tripped {
            violations.push(RiskViolation::CircuitBreakerOpen {
                scope: CircuitBreakerScope::Account,
            });
        }

        if violations.is_empty() {
            Ok(())
        } else {
            Err(violations)
        }
    }
}

impl InTradeMonitor for CircuitBreaker {
    fn inspect_registered_state(
        &self,
    ) -> Result<crate::RegisteredRiskState, crate::RiskStateError> {
        self.inspect_checkpoint()
            .map(crate::RegisteredRiskState::Circuit)
    }

    fn on_fill(
        &mut self,
        fill: &Fill,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
    ) -> Result<(), Vec<RiskViolation>> {
        self.on_fill_impl(fill, positions, balances)
    }
}

impl PreTradeCheck for CircuitBreaker {
    fn inspect_registered_state(
        &self,
    ) -> Result<crate::RegisteredRiskState, crate::RiskStateError> {
        self.inspect_checkpoint()
            .map(crate::RegisteredRiskState::Circuit)
    }

    fn check(
        &self,
        intent: &OrderIntent,
        _positions: &HashMap<String, Position>,
        _balances: &HashMap<String, Decimal>,
        _open_orders: &HashMap<OrderId, Order>,
        _now: DateTime<Utc>,
    ) -> Result<(), Vec<RiskViolation>> {
        self.check_impl(intent)
    }
}

/// Shares a [`CircuitBreaker`]'s state as a [`PreTradeCheck`]. See
/// [`CircuitBreaker::split`].
pub struct CircuitBreakerPreTradeAdapter(Arc<Mutex<CircuitBreaker>>);

impl CircuitBreakerPreTradeAdapter {
    pub fn inspect_checkpoint(&self) -> Result<crate::CircuitStateSnapshot, crate::RiskStateError> {
        self.0
            .lock()
            .map_err(|_| crate::RiskStateError::Poisoned)?
            .inspect_checkpoint()
    }
}

impl PreTradeCheck for CircuitBreakerPreTradeAdapter {
    fn shared_monitor(&self) -> Option<crate::SharedMonitorRef<'_>> {
        Some(crate::SharedMonitorRef(
            crate::shared_inspection::SharedMonitor::Circuit(&self.0),
        ))
    }

    fn inspect_registered_state(
        &self,
    ) -> Result<crate::RegisteredRiskState, crate::RiskStateError> {
        self.inspect_checkpoint()
            .map(crate::RegisteredRiskState::Circuit)
    }

    fn check(
        &self,
        intent: &OrderIntent,
        _positions: &HashMap<String, Position>,
        _balances: &HashMap<String, Decimal>,
        _open_orders: &HashMap<OrderId, Order>,
        _now: DateTime<Utc>,
    ) -> Result<(), Vec<RiskViolation>> {
        let guard = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.check_impl(intent)
    }
}

/// Shares a [`CircuitBreaker`]'s state as an [`InTradeMonitor`]. See
/// [`CircuitBreaker::split`].
pub struct CircuitBreakerInTradeAdapter(Arc<Mutex<CircuitBreaker>>);

impl InTradeMonitor for CircuitBreakerInTradeAdapter {
    fn shared_monitor(&self) -> Option<crate::SharedMonitorRef<'_>> {
        Some(crate::SharedMonitorRef(
            crate::shared_inspection::SharedMonitor::Circuit(&self.0),
        ))
    }

    fn inspect_registered_state(
        &self,
    ) -> Result<crate::RegisteredRiskState, crate::RiskStateError> {
        self.0
            .lock()
            .map_err(|_| crate::RiskStateError::Poisoned)?
            .inspect_checkpoint()
            .map(crate::RegisteredRiskState::Circuit)
    }

    fn on_fill(
        &mut self,
        fill: &Fill,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
    ) -> Result<(), Vec<RiskViolation>> {
        let mut guard = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.on_fill_impl(fill, positions, balances)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, OrderType, Side, Venue};
    use chrono::TimeZone;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap()
    }

    fn sample_intent(symbol: &str) -> OrderIntent {
        OrderIntent {
            symbol: symbol.to_string(),
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

    fn sample_fill(symbol: &str) -> Fill {
        Fill {
            id: apexes_core::FillId(Uuid::new_v4()),
            order_id: apexes_core::OrderId(Uuid::new_v4()),
            symbol: symbol.to_string(),
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

    fn positions_with(symbol: &str, quantity: Decimal) -> HashMap<String, Position> {
        let mut map = HashMap::new();
        map.insert(
            symbol.to_string(),
            Position {
                symbol: symbol.to_string(),
                quantity,
                average_entry_price: dec!(100),
                leverage: None,
                margin_mode: None,
                allocated_margin: Decimal::ZERO,
            },
        );
        map
    }

    fn empty_balances() -> HashMap<String, Decimal> {
        HashMap::new()
    }

    fn empty_open_orders() -> HashMap<OrderId, Order> {
        HashMap::new()
    }

    #[test]
    fn position_over_limit_trips_that_symbol_only() {
        let mut breaker = CircuitBreaker::new(RiskLimits {
            max_position_per_symbol: Some(dec!(10)),
            ..Default::default()
        });
        let fill = sample_fill("BTCUSDT");
        let result = breaker.on_fill(
            &fill,
            &positions_with("BTCUSDT", dec!(15)),
            &empty_balances(),
        );
        assert!(result.is_err());

        // BTCUSDT is tripped.
        let btc_intent = sample_intent("BTCUSDT");
        assert_eq!(
            breaker.check(
                &btc_intent,
                &HashMap::new(),
                &empty_balances(),
                &empty_open_orders(),
                now()
            ),
            Err(vec![RiskViolation::CircuitBreakerOpen {
                scope: CircuitBreakerScope::Symbol("BTCUSDT".to_string()),
            }])
        );

        // ETHUSDT is unaffected.
        let eth_intent = sample_intent("ETHUSDT");
        assert!(
            breaker
                .check(
                    &eth_intent,
                    &HashMap::new(),
                    &empty_balances(),
                    &empty_open_orders(),
                    now()
                )
                .is_ok()
        );
    }

    #[test]
    fn position_back_within_limit_auto_resets() {
        let mut breaker = CircuitBreaker::new(RiskLimits {
            max_position_per_symbol: Some(dec!(10)),
            ..Default::default()
        });
        let fill = sample_fill("BTCUSDT");
        breaker
            .on_fill(
                &fill,
                &positions_with("BTCUSDT", dec!(15)),
                &empty_balances(),
            )
            .unwrap_err();
        assert!(breaker.is_tripped(&CircuitBreakerScope::Symbol("BTCUSDT".to_string())));

        // A subsequent fill brings the position back within the limit.
        breaker
            .on_fill(
                &fill,
                &positions_with("BTCUSDT", dec!(5)),
                &empty_balances(),
            )
            .unwrap();
        assert!(!breaker.is_tripped(&CircuitBreakerScope::Symbol("BTCUSDT".to_string())));

        let intent = sample_intent("BTCUSDT");
        assert!(
            breaker
                .check(
                    &intent,
                    &HashMap::new(),
                    &empty_balances(),
                    &empty_open_orders(),
                    now()
                )
                .is_ok()
        );
    }

    #[test]
    fn manual_reset_clears_a_trip_early() {
        let mut breaker = CircuitBreaker::new(RiskLimits {
            max_position_per_symbol: Some(dec!(10)),
            ..Default::default()
        });
        let fill = sample_fill("BTCUSDT");
        breaker
            .on_fill(
                &fill,
                &positions_with("BTCUSDT", dec!(15)),
                &empty_balances(),
            )
            .unwrap_err();
        assert!(breaker.is_tripped(&CircuitBreakerScope::Symbol("BTCUSDT".to_string())));

        breaker.reset(&CircuitBreakerScope::Symbol("BTCUSDT".to_string()));
        assert!(!breaker.is_tripped(&CircuitBreakerScope::Symbol("BTCUSDT".to_string())));
    }

    #[test]
    fn account_equity_trip_rejects_every_symbol() {
        let mut breaker = CircuitBreaker::new(RiskLimits {
            min_account_equity: Some(dec!(1000)),
            ..Default::default()
        });
        let fill = sample_fill("BTCUSDT");
        let mut low_balance = HashMap::new();
        low_balance.insert("USDT".to_string(), dec!(500));
        breaker
            .on_fill(&fill, &HashMap::new(), &low_balance)
            .unwrap_err();
        assert!(breaker.is_tripped(&CircuitBreakerScope::Account));

        let btc_intent = sample_intent("BTCUSDT");
        let eth_intent = sample_intent("ETHUSDT");
        assert!(
            breaker
                .check(
                    &btc_intent,
                    &HashMap::new(),
                    &empty_balances(),
                    &empty_open_orders(),
                    now()
                )
                .is_err()
        );
        assert!(
            breaker
                .check(
                    &eth_intent,
                    &HashMap::new(),
                    &empty_balances(),
                    &empty_open_orders(),
                    now()
                )
                .is_err()
        );
    }

    #[test]
    fn checkpoint_inspects_shared_trip_state_without_clearing_it() {
        let breaker = CircuitBreaker::new(RiskLimits {
            max_position_per_symbol: Some(dec!(10)),
            min_account_equity: Some(dec!(1000)),
            ..Default::default()
        });
        let (pre, mut monitor) = breaker.split();
        let empty = pre.inspect_checkpoint().unwrap();
        for symbol in ["ETHUSDT", "BTCUSDT"] {
            monitor
                .on_fill(
                    &sample_fill(symbol),
                    &positions_with(symbol, dec!(15)),
                    &empty_balances(),
                )
                .unwrap_err();
        }
        let retained = pre.inspect_checkpoint().unwrap();
        assert_eq!(retained.tripped_symbols, vec!["BTCUSDT", "ETHUSDT"]);
        assert!(retained.account_tripped);
        retained
            .compare(&pre.inspect_checkpoint().unwrap())
            .unwrap();
        assert_eq!(
            retained.compare(&empty),
            Err(crate::RiskStateError::Mismatch)
        );
        assert!(
            pre.check(
                &sample_intent("BTCUSDT"),
                &HashMap::new(),
                &empty_balances(),
                &empty_open_orders(),
                now()
            )
            .is_err()
        );
    }

    #[test]
    fn split_adapters_share_the_same_underlying_state() {
        let breaker = CircuitBreaker::new(RiskLimits {
            max_position_per_symbol: Some(dec!(10)),
            ..Default::default()
        });
        let (pre_trade, mut in_trade) = breaker.split();

        let intent = sample_intent("BTCUSDT");
        assert!(
            pre_trade
                .check(
                    &intent,
                    &HashMap::new(),
                    &empty_balances(),
                    &empty_open_orders(),
                    now()
                )
                .is_ok()
        );

        let fill = sample_fill("BTCUSDT");
        in_trade
            .on_fill(
                &fill,
                &positions_with("BTCUSDT", dec!(15)),
                &empty_balances(),
            )
            .unwrap_err();

        // The trip recorded via the in-trade adapter must be visible
        // through the pre-trade adapter, since both wrap the same
        // underlying CircuitBreaker via Arc<Mutex<_>>.
        assert!(
            pre_trade
                .check(
                    &intent,
                    &HashMap::new(),
                    &empty_balances(),
                    &empty_open_orders(),
                    now()
                )
                .is_err()
        );
    }
}
