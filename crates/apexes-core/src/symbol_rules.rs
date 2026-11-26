//! An exchange's trading rules for one symbol, and how an order is made to
//! satisfy them.
//!
//! Binance refuses an order whose quantity is not a multiple of the symbol's
//! step (`LOT_SIZE`, -1013), whose price is off the tick (`PRICE_FILTER`), or
//! whose value is under the minimum (`NOTIONAL`). A strategy cannot know every
//! symbol's step (the same script trades ETH at 0.0001 and BNB at 0.001), so
//! the engine conforms the order instead: quantity is rounded DOWN to the step
//! (never up: that would spend more than the strategy asked), a limit price is
//! moved to the tick on the passive side, and an order that cannot be made valid
//! is refused here with a reason an operator can read, not sent to be refused
//! by the exchange.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::order::Side;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SymbolRules {
    /// Quantity must be a multiple of this. Zero means no step is enforced.
    pub quantity_step: Decimal,
    pub min_quantity: Decimal,
    /// Zero means no maximum is enforced.
    pub max_quantity: Decimal,
    /// Price must be a multiple of this. Zero means no tick is enforced.
    pub price_tick: Decimal,
    /// Smallest order value (price x quantity) the exchange accepts.
    pub min_notional: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RuleViolation {
    #[error(
        "quantity {requested} is below the {step} step: it rounds to nothing (minimum {minimum})"
    )]
    RoundsToNothing {
        requested: Decimal,
        step: Decimal,
        minimum: Decimal,
    },
    #[error("quantity {quantity} is below the exchange minimum {minimum}")]
    BelowMinimumQuantity { quantity: Decimal, minimum: Decimal },
    #[error("quantity {quantity} is above the exchange maximum {maximum}")]
    AboveMaximumQuantity { quantity: Decimal, maximum: Decimal },
    #[error("order value {notional} is below the exchange minimum {minimum}")]
    BelowMinimumNotional { notional: Decimal, minimum: Decimal },
}

/// An order after the rules were applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Conformed {
    pub quantity: Decimal,
    pub price: Option<Decimal>,
}

/// `value` rounded down to a multiple of `step` (`step` of zero: unchanged).
fn floor_to(value: Decimal, step: Decimal) -> Decimal {
    if step <= Decimal::ZERO {
        return value;
    }
    (value / step).floor() * step
}

fn ceil_to(value: Decimal, step: Decimal) -> Decimal {
    if step <= Decimal::ZERO {
        return value;
    }
    (value / step).ceil() * step
}

impl SymbolRules {
    /// Makes `quantity` (and a limit `price`, if any) valid.
    ///
    /// `reference_price` values a market order for the minimum-notional check;
    /// pass the limit price when there is one. With no price at all the value
    /// cannot be checked and the exchange decides.
    pub fn conform(
        &self,
        side: Side,
        quantity: Decimal,
        price: Option<Decimal>,
        reference_price: Option<Decimal>,
    ) -> Result<Conformed, RuleViolation> {
        let quantity = floor_to(quantity, self.quantity_step).normalize();
        if quantity <= Decimal::ZERO {
            return Err(RuleViolation::RoundsToNothing {
                requested: quantity,
                step: self.quantity_step,
                minimum: self.min_quantity,
            });
        }
        if quantity < self.min_quantity {
            return Err(RuleViolation::BelowMinimumQuantity {
                quantity,
                minimum: self.min_quantity,
            });
        }
        if self.max_quantity > Decimal::ZERO && quantity > self.max_quantity {
            return Err(RuleViolation::AboveMaximumQuantity {
                quantity,
                maximum: self.max_quantity,
            });
        }
        let price = price.map(|p| {
            match side {
                // Passive side: a buy never pays more, a sell never asks less.
                Side::Buy => floor_to(p, self.price_tick),
                Side::Sell => ceil_to(p, self.price_tick),
            }
            .normalize()
        });
        if let Some(value_price) = price.or(reference_price) {
            let notional = quantity * value_price;
            if self.min_notional > Decimal::ZERO && notional < self.min_notional {
                return Err(RuleViolation::BelowMinimumNotional {
                    notional: notional.round_dp(8),
                    minimum: self.min_notional,
                });
            }
        }
        Ok(Conformed { quantity, price })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    /// BNBUSDT on the Spot Test Network, read from its exchangeInfo.
    fn bnb() -> SymbolRules {
        SymbolRules {
            quantity_step: dec!(0.001),
            min_quantity: dec!(0.001),
            max_quantity: dec!(900000),
            price_tick: dec!(0.01),
            min_notional: dec!(5),
        }
    }

    fn conform(q: Decimal) -> Result<Conformed, RuleViolation> {
        bnb().conform(Side::Buy, q, None, Some(dec!(590)))
    }

    /// The order from the log: BNBUSDT Buy 0.0846 against a 0.001 step.
    #[test]
    fn the_rejected_quantity_is_rounded_down_to_the_step() {
        let c = conform(dec!(0.0846)).unwrap();
        assert_eq!(c.quantity, dec!(0.084));
    }

    #[test]
    fn rounding_never_goes_up() {
        for q in [dec!(0.0199), dec!(0.0999999), dec!(1.2349999)] {
            let c = conform(q).unwrap();
            assert!(c.quantity <= q, "{q} -> {}", c.quantity);
            assert!(q - c.quantity < dec!(0.001), "{q} -> {}", c.quantity);
        }
    }

    #[test]
    fn a_quantity_already_on_the_step_is_unchanged() {
        assert_eq!(conform(dec!(0.085)).unwrap().quantity, dec!(0.085));
        assert_eq!(conform(dec!(3)).unwrap().quantity, dec!(3));
    }

    #[test]
    fn float_residue_just_under_a_step_is_not_lost_to_a_whole_step() {
        // 0.085 arriving as 0.08499999999999999 from a float strategy: the
        // decimal conversion keeps it below the step, so it rounds to 0.084.
        // Documented, not hidden: the strategy should add its own epsilon, as
        // trend_guard does.
        let c = conform(dec!(0.08499999999999999)).unwrap();
        assert_eq!(c.quantity, dec!(0.084));
    }

    #[test]
    fn a_quantity_that_rounds_to_nothing_is_refused_with_the_step_in_the_reason() {
        let error = conform(dec!(0.0004)).unwrap_err();
        assert!(matches!(error, RuleViolation::RoundsToNothing { .. }));
        let text = error.to_string();
        assert!(
            text.contains("0.001") && text.contains("rounds to nothing"),
            "{text}"
        );
    }

    #[test]
    fn an_order_under_the_minimum_value_is_refused() {
        // 0.008 BNB at 590 is 4.72, under the 5 minimum.
        let error = conform(dec!(0.008)).unwrap_err();
        assert!(
            matches!(error, RuleViolation::BelowMinimumNotional { .. }),
            "{error}"
        );
        assert!(error.to_string().contains('5'));
        // 0.009 is 5.31: fine.
        assert!(conform(dec!(0.009)).is_ok());
    }

    #[test]
    fn the_minimum_value_is_judged_after_rounding_not_before() {
        // 0.0089 BNB is 5.25 as asked, but rounds to 0.008 (4.72): refuse.
        assert!(conform(dec!(0.0089)).is_err());
    }

    #[test]
    fn a_limit_price_is_the_value_checked_not_the_reference() {
        let rules = bnb();
        // 0.009 at a limit of 100 is 0.9: refused though the market is 590.
        assert!(
            rules
                .conform(Side::Buy, dec!(0.009), Some(dec!(100)), Some(dec!(590)))
                .is_err()
        );
    }

    #[test]
    fn a_limit_price_moves_to_the_passive_side_of_the_tick() {
        let rules = bnb();
        let buy = rules
            .conform(Side::Buy, dec!(1), Some(dec!(590.019)), None)
            .unwrap();
        let sell = rules
            .conform(Side::Sell, dec!(1), Some(dec!(590.011)), None)
            .unwrap();
        assert_eq!(buy.price, Some(dec!(590.01)), "a buy never pays more");
        assert_eq!(sell.price, Some(dec!(590.02)), "a sell never asks for less");
    }

    #[test]
    fn a_market_order_keeps_no_price() {
        assert_eq!(conform(dec!(1)).unwrap().price, None);
    }

    #[test]
    fn a_quantity_over_the_maximum_is_refused_not_clipped() {
        let error = bnb()
            .conform(Side::Buy, dec!(900001), None, Some(dec!(590)))
            .unwrap_err();
        assert!(matches!(error, RuleViolation::AboveMaximumQuantity { .. }));
    }

    #[test]
    fn without_any_price_the_value_cannot_be_checked_so_the_exchange_decides() {
        assert!(bnb().conform(Side::Buy, dec!(0.002), None, None).is_ok());
    }

    #[test]
    fn zero_means_no_such_rule() {
        let none = SymbolRules {
            quantity_step: Decimal::ZERO,
            min_quantity: Decimal::ZERO,
            max_quantity: Decimal::ZERO,
            price_tick: Decimal::ZERO,
            min_notional: Decimal::ZERO,
        };
        let c = none
            .conform(Side::Buy, dec!(0.123456789), Some(dec!(1.2345678)), None)
            .unwrap();
        assert_eq!(c.quantity, dec!(0.123456789));
        assert_eq!(c.price, Some(dec!(1.2345678)));
    }

    #[test]
    fn zero_and_negative_quantities_are_refused() {
        assert!(conform(dec!(0)).is_err());
        assert!(conform(dec!(-1)).is_err());
    }

    #[test]
    fn the_result_is_a_whole_multiple_of_the_step_for_any_input() {
        let step = dec!(0.001);
        let mut q = dec!(0.0011);
        while q < dec!(2) {
            if let Ok(c) = conform(q) {
                assert_eq!(
                    (c.quantity / step).fract(),
                    Decimal::ZERO,
                    "{q} -> {}",
                    c.quantity
                );
            }
            q += dec!(0.00137);
        }
    }
}
