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

use std::cell::RefCell;
use std::collections::HashMap;

use apexes_core::{Order, OrderId, Side};
use apexes_strategy::{OrderIntent, Position};
use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;

use crate::error::RiskViolation;
use crate::pre_trade::PreTradeCheck;

#[derive(Debug, Clone)]
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
    /// call. `RefCell` keeps the public API `&self` (matching every
    /// other stateless-looking `PreTradeCheck` impl) while still
    /// allowing this one field to mutate internally -- single-threaded
    /// backtest/paper/live event loops call `check` sequentially, never
    /// concurrently, so this is sound without needing `Mutex`.
    daily_state: RefCell<DailyState>,
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
            daily_state: RefCell::new(DailyState {
                day: None,
                trade_count: 0,
                traded_volume: Decimal::ZERO,
            }),
        }
    }

    /// Replaces the currently enforced limits. Does not reset the
    /// internal daily counters -- a limit tightened mid-day applies
    /// against whatever count/volume has already accumulated that day.
    pub fn update_limits(&mut self, limits: RiskLimits) {
        self.limits = limits;
    }
}

impl PreTradeCheck for LimitChecker {
    fn check(
        &self,
        intent: &OrderIntent,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
        _open_orders: &HashMap<OrderId, Order>,
        now: DateTime<Utc>,
    ) -> Result<(), Vec<RiskViolation>> {
        let mut violations = Vec::new();
        let today = now.date_naive();

        {
            let mut state = self.daily_state.borrow_mut();
            if state.day != Some(today) {
                state.day = Some(today);
                state.trade_count = 0;
                state.traded_volume = Decimal::ZERO;
            }
        }

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
        // Market orders (intent.price == None) have no notional to
        // check against here -- their actual fill price is unknown
        // until the matching engine executes them. This is an accepted
        // limitation: max_order_notional only applies to Limit/
        // StopLimit orders where a price is known in advance.

        if let Some(max_position) = self.limits.max_position_per_symbol {
            let current = positions
                .get(&intent.symbol)
                .map(|p| p.quantity)
                .unwrap_or(Decimal::ZERO);
            let signed_delta = match intent.side {
                Side::Buy => intent.quantity,
                Side::Sell => -intent.quantity,
            };
            let projected = (current + signed_delta).abs();
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

        {
            let state = self.daily_state.borrow();
            if let Some(max_count) = self.limits.max_daily_trade_count
                && state.trade_count + 1 > max_count
            {
                violations.push(RiskViolation::DailyTradeCountExceeded {
                    actual: state.trade_count + 1,
                    limit: max_count,
                    day: now,
                });
            }
            if let Some(max_volume) = self.limits.max_daily_volume
                && state.traded_volume + intent.quantity > max_volume
            {
                violations.push(RiskViolation::DailyVolumeExceeded {
                    actual: state.traded_volume + intent.quantity,
                    limit: max_volume,
                    day: now,
                });
            }
        }

        if violations.is_empty() {
            let mut state = self.daily_state.borrow_mut();
            state.trade_count += 1;
            state.traded_volume += intent.quantity;
            Ok(())
        } else {
            Err(violations)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::Venue;
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
            side,
            order_type: if price.is_some() {
                apexes_core::OrderType::Limit
            } else {
                apexes_core::OrderType::Market
            },
            quantity,
            price,
            stop_price: None,
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
