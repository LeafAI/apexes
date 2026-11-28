//! [`RiskLimits`]: the configurable threshold values [`LimitChecker`]
//! enforces. Constructed once, then optionally updated at runtime via
//! [`LimitChecker::update_limits`] -- e.g. a future live-trading
//! monitoring system tightening limits in response to detected market
//! stress, without requiring a process restart.
//!
//! [`LimitChecker`]: enforces [`RiskLimits`] as a [`crate::pre_trade::PreTradeCheck`].
//! Tracks its own per-day trade count/volume state internally (reset
//! when `now`'s UTC calendar day changes from the previously observed
//! day), since neither figure is derivable from the `positions`/
//! `balances` snapshot `check` receives.

use std::collections::HashMap;
use std::sync::Mutex;

use apexes_core::{AssetClass, Order, OrderId, OrderType, Side};
use apexes_strategy::{OrderIntent, OrderRouting, Position};
use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;

use crate::error::RiskViolation;
use crate::pre_trade::PreTradeCheck;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskLimits {
    /// `None` means "no limit configured for this dimension" -- every
    /// field is optional so callers can enable only the dimensions they
    /// care about.
    pub max_order_quantity: Option<Decimal>,
    pub max_order_notional: Option<Decimal>,
    pub max_position_per_symbol: Option<Decimal>,
    /// Minimum acceptable total account balance (summed across all
    /// assets in `balances`, NOT a mark-to-market equity calculation --
    /// see this crate's design spec §5.1 for why: `LimitChecker` has no
    /// access to `PortfolioLedger` or current mark prices without
    /// introducing a circular dependency on `apexes-backtest`). An
    /// order is rejected if the CURRENT balance sum (before the order
    /// executes) is already below this floor.
    pub min_account_equity: Option<Decimal>,
    pub max_daily_trade_count: Option<u32>,
    pub max_daily_volume: Option<Decimal>,
}

impl Default for RiskLimits {
    /// All limits unset (`None`) -- a `LimitChecker` built from
    /// `RiskLimits::default()` accepts every order (no-op checker),
    /// which is the correct "opt-in per dimension" default.
    fn default() -> Self {
        Self {
            max_order_quantity: None,
            max_order_notional: None,
            max_position_per_symbol: None,
            min_account_equity: None,
            max_daily_trade_count: None,
            max_daily_volume: None,
        }
    }
}

pub struct LimitChecker {
    limits: RiskLimits,
    /// Interior-mutable daily counters: `check` takes `&self` per the
    /// `PreTradeCheck` trait signature, but tracking "how many trades
    /// happened today" inherently requires mutation on every check
    /// call. `Mutex` (rather than `RefCell`) is required so
    /// `LimitChecker` is `Sync` -- needed to store `Arc<dyn
    /// PreTradeCheck>`/`Arc<CompositeRiskChecker>` in
    /// `apexes_core::registry::Registry`, which requires `Send + Sync`
    /// (sub-project #9's registry retrofit). Real callers still only
    /// ever call `check` sequentially (never concurrently), so lock
    /// contention is not a practical concern -- this is purely a
    /// `Sync`-trait-bound requirement, not a genuine concurrency need.
    daily_state: Mutex<DailyState>,
}

struct DailyState {
    day: Option<NaiveDate>,
    trade_count: u32,
    traded_volume: Decimal,
}

impl LimitChecker {
    pub fn new(limits: RiskLimits) -> Self {
        Self {
            limits,
            daily_state: Mutex::new(DailyState {
                day: None,
                trade_count: 0,
                traded_volume: Decimal::ZERO,
            }),
        }
    }

    /// Read-only capture of the actual accepted-order budget. Poisoned
    /// state is refused rather than treated as a fresh budget.
    pub fn inspect_checkpoint(&self) -> Result<crate::LimitStateSnapshot, crate::RiskStateError> {
        let state = self
            .daily_state
            .lock()
            .map_err(|_| crate::RiskStateError::Poisoned)?;
        let snapshot = crate::LimitStateSnapshot {
            version: 1,
            limits: self.limits.clone(),
            day: state.day,
            trade_count: state.trade_count,
            traded_volume: state.traded_volume,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    /// Replaces the currently enforced limits. Does not reset the
    /// internal daily counters -- a limit tightened mid-day applies
    /// against whatever count/volume has already accumulated that day.
    pub fn update_limits(&mut self, limits: RiskLimits) {
        self.limits = limits;
    }
}

/// Narrow exemption from entry budgets, not an authorization to trade.
/// The executor must still enforce ownership, available inventory and
/// health. Delayed/routed/leveraged orders are not protective exits.
pub fn is_protective_spot_exit(
    intent: &OrderIntent,
    positions: &HashMap<String, Position>,
    open_orders: &HashMap<OrderId, Order>,
) -> bool {
    intent.venue == apexes_core::Venue::Binance
        && intent.asset_class == AssetClass::Spot
        && intent.side == Side::Sell
        && intent.order_type == OrderType::Market
        && intent.execution_algo.is_none()
        && intent.routing == OrderRouting::Fixed
        && intent.leverage.is_none()
        && intent.margin_mode.is_none()
        && intent.quantity > Decimal::ZERO
        && positions.get(&intent.symbol).is_some_and(|position| {
            position.symbol == intent.symbol
                && position.leverage.is_none()
                && position.margin_mode.is_none()
                && position.quantity >= intent.quantity
        })
        && !open_orders
            .values()
            .any(|order| order.symbol == intent.symbol)
}

impl LimitChecker {
    fn check_with_scope(
        &self,
        intent: &OrderIntent,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
        open_orders: &HashMap<OrderId, Order>,
        now: DateTime<Utc>,
        child: bool,
    ) -> Result<(), Vec<RiskViolation>> {
        let mut violations = Vec::new();
        let today = now.date_naive();
        let protective = !child && is_protective_spot_exit(intent, positions, open_orders);

        if let Some(max_qty) = self.limits.max_order_quantity
            && intent.quantity > max_qty
        {
            violations.push(RiskViolation::OrderQuantityExceeded {
                actual: intent.quantity,
                limit: max_qty,
            });
        }

        if let Some(max_notional) = self.limits.max_order_notional
            && let Some(price) = intent.price
        {
            let notional = intent.quantity * price;
            if notional > max_notional {
                violations.push(RiskViolation::OrderNotionalExceeded {
                    actual: notional,
                    limit: max_notional,
                });
            }
        }
        // An intent with no price has no notional to check here. The unified
        // loop fills the price in with the last trade price before asking, so
        // market orders are covered in practice; a caller that does not (or
        // before any trade has printed) leaves this limit unenforced.

        if !protective && let Some(max_position) = self.limits.max_position_per_symbol {
            let current = positions
                .get(&intent.symbol)
                .map(|p| p.quantity)
                .unwrap_or(Decimal::ZERO);
            let signed_delta = match intent.side {
                Side::Buy => intent.quantity,
                Side::Sell => -intent.quantity,
            };
            // Pending spot buys may all execute before the new buy;
            // pending sales are not credited as capacity until filled.
            let pending: Decimal =
                if intent.asset_class == AssetClass::Spot && intent.side == Side::Buy {
                    open_orders
                        .values()
                        .filter(|o| {
                            o.symbol == intent.symbol
                                && o.venue == intent.venue
                                && o.asset_class == intent.asset_class
                                && o.side == Side::Buy
                                && matches!(
                                    o.status,
                                    apexes_core::OrderStatus::New
                                        | apexes_core::OrderStatus::PartiallyFilled
                                )
                        })
                        .map(|o| (o.quantity - o.filled_quantity).max(Decimal::ZERO))
                        .sum()
                } else {
                    Decimal::ZERO
                };
            let projected = (current + signed_delta + pending).abs();
            if projected > max_position {
                violations.push(RiskViolation::PositionLimitExceeded {
                    symbol: intent.symbol.clone(),
                    projected,
                    limit: max_position,
                });
            }
        }

        if let Some(floor) = self.limits.min_account_equity {
            let total_balance: Decimal = balances.values().copied().sum();
            if total_balance < floor {
                violations.push(RiskViolation::EquityFloorBreached {
                    actual: total_balance,
                    floor,
                });
            }
        }

        if !protective && !child {
            let state = self.daily_state.lock().unwrap_or_else(|p| p.into_inner());
            let (count, volume) = if state.day == Some(today) {
                (state.trade_count, state.traded_volume)
            } else {
                (0, Decimal::ZERO)
            };
            if let Some(max_count) = self.limits.max_daily_trade_count
                && count.saturating_add(1) > max_count
            {
                violations.push(RiskViolation::DailyTradeCountExceeded {
                    actual: count.saturating_add(1),
                    limit: max_count,
                    day: now,
                });
            }
            if let Some(max_volume) = self.limits.max_daily_volume
                && volume + intent.quantity > max_volume
            {
                violations.push(RiskViolation::DailyVolumeExceeded {
                    actual: volume + intent.quantity,
                    limit: max_volume,
                    day: now,
                });
            }
        }

        if violations.is_empty() {
            Ok(())
        } else {
            Err(violations)
        }
    }
}

impl PreTradeCheck for LimitChecker {
    fn check(
        &self,
        intent: &OrderIntent,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
        open_orders: &HashMap<OrderId, Order>,
        now: DateTime<Utc>,
    ) -> Result<(), Vec<RiskViolation>> {
        self.check_with_scope(intent, positions, balances, open_orders, now, false)
    }

    fn check_child(
        &self,
        intent: &OrderIntent,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
        open_orders: &HashMap<OrderId, Order>,
        now: DateTime<Utc>,
    ) -> Result<(), Vec<RiskViolation>> {
        self.check_with_scope(intent, positions, balances, open_orders, now, true)
    }

    fn on_order_accepted(&self, intent: &OrderIntent, now: DateTime<Utc>) {
        let today = now.date_naive();
        let mut state = self
            .daily_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.day != Some(today) {
            state.day = Some(today);
            state.trade_count = 0;
            state.traded_volume = Decimal::ZERO;
        }
        state.trade_count = state.trade_count.saturating_add(1);
        state.traded_volume += intent.quantity;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, Venue};
    use rust_decimal_macros::dec;

    fn sample_intent(
        symbol: &str,
        side: Side,
        quantity: Decimal,
        price: Option<Decimal>,
    ) -> OrderIntent {
        OrderIntent {
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side,
            order_type: if price.is_some() {
                apexes_core::OrderType::Limit
            } else {
                apexes_core::OrderType::Market
            },
            quantity,
            price,
            stop_price: None,
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        }
    }

    fn empty_positions() -> HashMap<String, Position> {
        HashMap::new()
    }

    fn empty_balances() -> HashMap<String, Decimal> {
        HashMap::new()
    }

    fn empty_open_orders() -> HashMap<OrderId, Order> {
        HashMap::new()
    }

    fn now() -> DateTime<Utc> {
        chrono::Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap()
    }

    use chrono::TimeZone;

    #[test]
    fn checkpoint_inspects_actual_accepted_budget_without_reset() {
        let mut checker = LimitChecker::new(RiskLimits {
            max_daily_trade_count: Some(2),
            max_daily_volume: Some(dec!(3)),
            ..Default::default()
        });
        let intent = sample_intent("BTCUSDT", Side::Buy, dec!(1.5), Some(dec!(100)));
        let before = checker.inspect_checkpoint().unwrap();
        assert_eq!(before.trade_count, 0);
        checker.on_order_accepted(&intent, now());
        checker.on_order_accepted(&intent, now());
        let retained = checker.inspect_checkpoint().unwrap();
        assert_eq!(retained.trade_count, 2);
        assert_eq!(retained.traded_volume, dec!(3));
        assert_eq!(retained.day, Some(now().date_naive()));
        retained
            .compare(&checker.inspect_checkpoint().unwrap())
            .unwrap();
        assert_eq!(
            retained.compare(&before),
            Err(crate::RiskStateError::Mismatch)
        );
        assert!(
            checker
                .check(
                    &intent,
                    &empty_positions(),
                    &empty_balances(),
                    &empty_open_orders(),
                    now()
                )
                .is_err()
        );
        // Inspection cannot reset the budget or trip at UTC midnight;
        // the existing check's day semantics remain unchanged.
        assert!(
            checker
                .check(
                    &intent,
                    &empty_positions(),
                    &empty_balances(),
                    &empty_open_orders(),
                    now() + chrono::TimeDelta::days(1)
                )
                .is_ok()
        );
        retained
            .compare(&checker.inspect_checkpoint().unwrap())
            .unwrap();
        checker.update_limits(RiskLimits {
            max_daily_trade_count: Some(1),
            ..Default::default()
        });
        let tightened = checker.inspect_checkpoint().unwrap();
        assert_eq!(tightened.trade_count, 2);
        assert_eq!(
            tightened.compare(&retained),
            Err(crate::RiskStateError::Mismatch)
        );
    }

    #[test]
    fn verified_spot_exit_bypasses_entry_budgets_but_not_hard_order_limits() {
        let checker = LimitChecker::new(RiskLimits {
            max_position_per_symbol: Some(dec!(1)),
            max_daily_trade_count: Some(0),
            max_daily_volume: Some(dec!(0)),
            max_order_quantity: Some(dec!(3)),
            ..Default::default()
        });
        let positions = HashMap::from([(
            "BTCUSDT".into(),
            Position {
                symbol: "BTCUSDT".into(),
                quantity: dec!(5),
                average_entry_price: dec!(100),
                leverage: None,
                margin_mode: None,
                allocated_margin: Decimal::ZERO,
            },
        )]);
        let mut exit = sample_intent("BTCUSDT", Side::Sell, dec!(2), None);
        let ask = |intent: &OrderIntent| {
            checker.check(
                intent,
                &positions,
                &empty_balances(),
                &empty_open_orders(),
                now(),
            )
        };
        assert!(ask(&exit).is_ok()); // residual 3 is still above entry position limit
        exit.quantity = dec!(4);
        assert!(matches!(
            ask(&exit).unwrap_err().as_slice(),
            [RiskViolation::OrderQuantityExceeded { .. }]
        ));
        exit.quantity = dec!(6);
        assert!(
            ask(&exit)
                .unwrap_err()
                .iter()
                .any(|v| matches!(v, RiskViolation::DailyTradeCountExceeded { .. }))
        );
        exit.quantity = dec!(2);
        exit.routing = OrderRouting::RouteToBest;
        assert!(ask(&exit).is_err());
        exit.routing = OrderRouting::Fixed;
        exit.side = Side::Buy;
        assert!(ask(&exit).is_err());
    }

    #[test]
    fn order_quantity_within_limit_passes() {
        let checker = LimitChecker::new(RiskLimits {
            max_order_quantity: Some(dec!(10)),
            ..Default::default()
        });
        let intent = sample_intent("BTCUSDT", Side::Buy, dec!(5), None);
        let result = checker.check(
            &intent,
            &empty_positions(),
            &empty_balances(),
            &empty_open_orders(),
            now(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn order_quantity_exceeding_limit_is_rejected() {
        let checker = LimitChecker::new(RiskLimits {
            max_order_quantity: Some(dec!(10)),
            ..Default::default()
        });
        let intent = sample_intent("BTCUSDT", Side::Buy, dec!(15), None);
        let result = checker.check(
            &intent,
            &empty_positions(),
            &empty_balances(),
            &empty_open_orders(),
            now(),
        );
        assert_eq!(
            result,
            Err(vec![RiskViolation::OrderQuantityExceeded {
                actual: dec!(15),
                limit: dec!(10),
            }])
        );
    }

    #[test]
    fn order_notional_exceeding_limit_is_rejected_for_limit_orders() {
        let checker = LimitChecker::new(RiskLimits {
            max_order_notional: Some(dec!(1000)),
            ..Default::default()
        });
        let intent = sample_intent("BTCUSDT", Side::Buy, dec!(1), Some(dec!(2000)));
        let result = checker.check(
            &intent,
            &empty_positions(),
            &empty_balances(),
            &empty_open_orders(),
            now(),
        );
        assert_eq!(
            result,
            Err(vec![RiskViolation::OrderNotionalExceeded {
                actual: dec!(2000),
                limit: dec!(1000),
            }])
        );
    }

    #[test]
    fn market_orders_are_not_checked_against_notional_limit() {
        let checker = LimitChecker::new(RiskLimits {
            max_order_notional: Some(dec!(1)),
            ..Default::default()
        });
        // Market order has no price -> notional cannot be computed ->
        // this dimension is skipped entirely, not treated as a failure.
        let intent = sample_intent("BTCUSDT", Side::Buy, dec!(1000), None);
        let result = checker.check(
            &intent,
            &empty_positions(),
            &empty_balances(),
            &empty_open_orders(),
            now(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn position_limit_exceeded_on_buy_increasing_long_position() {
        let checker = LimitChecker::new(RiskLimits {
            max_position_per_symbol: Some(dec!(10)),
            ..Default::default()
        });
        let mut positions = HashMap::new();
        positions.insert(
            "BTCUSDT".to_string(),
            Position {
                symbol: "BTCUSDT".to_string(),
                quantity: dec!(8),
                average_entry_price: dec!(100),
                leverage: None,
                margin_mode: None,
                allocated_margin: Decimal::ZERO,
            },
        );
        let intent = sample_intent("BTCUSDT", Side::Buy, dec!(5), None);
        let result = checker.check(
            &intent,
            &positions,
            &empty_balances(),
            &empty_open_orders(),
            now(),
        );
        assert_eq!(
            result,
            Err(vec![RiskViolation::PositionLimitExceeded {
                symbol: "BTCUSDT".to_string(),
                projected: dec!(13),
                limit: dec!(10),
            }])
        );
    }

    #[test]
    fn position_limit_exceeded_on_sell_increasing_short_position() {
        let checker = LimitChecker::new(RiskLimits {
            max_position_per_symbol: Some(dec!(10)),
            ..Default::default()
        });
        let mut positions = HashMap::new();
        positions.insert(
            "BTCUSDT".to_string(),
            Position {
                symbol: "BTCUSDT".to_string(),
                quantity: dec!(-8),
                average_entry_price: dec!(100),
                leverage: None,
                margin_mode: None,
                allocated_margin: Decimal::ZERO,
            },
        );
        let intent = sample_intent("BTCUSDT", Side::Sell, dec!(5), None);
        let result = checker.check(
            &intent,
            &positions,
            &empty_balances(),
            &empty_open_orders(),
            now(),
        );
        assert_eq!(
            result,
            Err(vec![RiskViolation::PositionLimitExceeded {
                symbol: "BTCUSDT".to_string(),
                projected: dec!(13),
                limit: dec!(10),
            }])
        );
    }

    #[test]
    fn account_equity_below_floor_is_rejected() {
        let checker = LimitChecker::new(RiskLimits {
            min_account_equity: Some(dec!(1000)),
            ..Default::default()
        });
        let mut balances = HashMap::new();
        balances.insert("USDT".to_string(), dec!(500));
        let intent = sample_intent("BTCUSDT", Side::Buy, dec!(1), None);
        let result = checker.check(
            &intent,
            &empty_positions(),
            &balances,
            &empty_open_orders(),
            now(),
        );
        assert_eq!(
            result,
            Err(vec![RiskViolation::EquityFloorBreached {
                actual: dec!(500),
                floor: dec!(1000),
            }])
        );
    }

    /// `check` is a pure question: asking again and again (the loop and the
    /// order manager both ask about the same order) must not use up allowance,
    /// and only an accepted order does.
    #[test]
    fn checking_does_not_count_only_accepted_orders_do() {
        let checker = LimitChecker::new(RiskLimits {
            max_daily_trade_count: Some(2),
            ..Default::default()
        });
        let now = chrono::Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        let intent = sample_intent("BTCUSDT", Side::Buy, dec!(1), None);
        let ask = || {
            checker.check(
                &intent,
                &empty_positions(),
                &empty_balances(),
                &empty_open_orders(),
                now,
            )
        };

        for _ in 0..100 {
            assert!(ask().is_ok(), "asking never uses allowance");
        }
        checker.on_order_accepted(&intent, now);
        assert!(ask().is_ok(), "1 of 2 used");
        checker.on_order_accepted(&intent, now);
        let error = ask().unwrap_err();
        assert!(
            error.iter().any(|v| matches!(
                v,
                RiskViolation::DailyTradeCountExceeded {
                    actual: 3,
                    limit: 2,
                    ..
                }
            )),
            "{error:?}"
        );
    }

    #[test]
    fn daily_volume_counts_only_accepted_quantity() {
        let checker = LimitChecker::new(RiskLimits {
            max_daily_volume: Some(dec!(10)),
            ..Default::default()
        });
        let now = chrono::Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        let six = sample_intent("BTCUSDT", Side::Buy, dec!(6), None);
        let args = (empty_positions(), empty_balances(), empty_open_orders());
        assert!(checker.check(&six, &args.0, &args.1, &args.2, now).is_ok());
        assert!(
            checker.check(&six, &args.0, &args.1, &args.2, now).is_ok(),
            "a rejected-by-exchange 6 never counted"
        );
        checker.on_order_accepted(&six, now);
        assert!(
            checker.check(&six, &args.0, &args.1, &args.2, now).is_err(),
            "6 + 6 exceeds 10 once the first was accepted"
        );
    }

    #[test]
    fn accepting_on_a_new_day_resets_the_counters_first() {
        let checker = LimitChecker::new(RiskLimits {
            max_daily_trade_count: Some(1),
            ..Default::default()
        });
        let day1 = chrono::Utc.with_ymd_and_hms(2026, 1, 1, 23, 59, 0).unwrap();
        let day2 = chrono::Utc.with_ymd_and_hms(2026, 1, 2, 0, 1, 0).unwrap();
        let intent = sample_intent("BTCUSDT", Side::Buy, dec!(1), None);
        checker.on_order_accepted(&intent, day1);
        let args = (empty_positions(), empty_balances(), empty_open_orders());
        assert!(
            checker
                .check(&intent, &args.0, &args.1, &args.2, day1)
                .is_err()
        );
        // Accepting first thing on the next day must start a fresh count.
        checker.on_order_accepted(&intent, day2);
        assert!(
            checker
                .check(&intent, &args.0, &args.1, &args.2, day2)
                .is_err(),
            "1 of 1 used on day 2"
        );
        let day3 = chrono::Utc.with_ymd_and_hms(2026, 1, 3, 9, 0, 0).unwrap();
        assert!(
            checker
                .check(&intent, &args.0, &args.1, &args.2, day3)
                .is_ok()
        );
    }

    #[test]
    fn daily_trade_count_resets_across_calendar_days() {
        let checker = LimitChecker::new(RiskLimits {
            max_daily_trade_count: Some(1),
            ..Default::default()
        });
        let day1 = chrono::Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        let day2 = chrono::Utc.with_ymd_and_hms(2026, 1, 2, 12, 0, 0).unwrap();
        let intent = sample_intent("BTCUSDT", Side::Buy, dec!(1), None);

        // First trade on day 1: passes (count 0 -> 1, within limit 1).
        assert!(
            checker
                .check(
                    &intent,
                    &empty_positions(),
                    &empty_balances(),
                    &empty_open_orders(),
                    day1
                )
                .is_ok()
        );
        checker.on_order_accepted(&intent, day1);
        // Second trade on day 1: rejected (would be count 2, exceeds limit 1).
        assert!(
            checker
                .check(
                    &intent,
                    &empty_positions(),
                    &empty_balances(),
                    &empty_open_orders(),
                    day1
                )
                .is_err()
        );
        // First trade on day 2: passes again (counter reset for new day).
        assert!(
            checker
                .check(
                    &intent,
                    &empty_positions(),
                    &empty_balances(),
                    &empty_open_orders(),
                    day2
                )
                .is_ok()
        );
    }

    #[test]
    fn daily_volume_limit_is_enforced_cumulatively() {
        let checker = LimitChecker::new(RiskLimits {
            max_daily_volume: Some(dec!(10)),
            ..Default::default()
        });
        let intent = sample_intent("BTCUSDT", Side::Buy, dec!(6), None);
        assert!(
            checker
                .check(
                    &intent,
                    &empty_positions(),
                    &empty_balances(),
                    &empty_open_orders(),
                    now()
                )
                .is_ok()
        );
        // The first order is accepted for execution, so it now counts.
        checker.on_order_accepted(&intent, now());
        // Second call: 6 (already traded) + 6 (this intent) = 12 > 10 -> rejected.
        let result = checker.check(
            &intent,
            &empty_positions(),
            &empty_balances(),
            &empty_open_orders(),
            now(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn multiple_violated_rules_are_all_returned() {
        let checker = LimitChecker::new(RiskLimits {
            max_order_quantity: Some(dec!(1)),
            max_order_notional: Some(dec!(1)),
            ..Default::default()
        });
        let intent = sample_intent("BTCUSDT", Side::Buy, dec!(100), Some(dec!(100)));
        let result = checker.check(
            &intent,
            &empty_positions(),
            &empty_balances(),
            &empty_open_orders(),
            now(),
        );
        let violations = result.expect_err("expected both rules to be violated");
        assert_eq!(violations.len(), 2);
        assert!(violations.contains(&RiskViolation::OrderQuantityExceeded {
            actual: dec!(100),
            limit: dec!(1),
        }));
        assert!(violations.contains(&RiskViolation::OrderNotionalExceeded {
            actual: dec!(10000),
            limit: dec!(1),
        }));
    }

    #[test]
    fn update_limits_takes_effect_on_next_check() {
        let mut checker = LimitChecker::new(RiskLimits {
            max_order_quantity: Some(dec!(100)),
            ..Default::default()
        });
        let intent = sample_intent("BTCUSDT", Side::Buy, dec!(50), None);
        assert!(
            checker
                .check(
                    &intent,
                    &empty_positions(),
                    &empty_balances(),
                    &empty_open_orders(),
                    now()
                )
                .is_ok()
        );

        checker.update_limits(RiskLimits {
            max_order_quantity: Some(dec!(10)),
            ..Default::default()
        });
        let result = checker.check(
            &intent,
            &empty_positions(),
            &empty_balances(),
            &empty_open_orders(),
            now(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn default_limits_accept_every_order() {
        let checker = LimitChecker::new(RiskLimits::default());
        let intent = sample_intent("BTCUSDT", Side::Buy, dec!(1_000_000), Some(dec!(1_000_000)));
        let result = checker.check(
            &intent,
            &empty_positions(),
            &empty_balances(),
            &empty_open_orders(),
            now(),
        );
        assert!(result.is_ok());
    }
}
