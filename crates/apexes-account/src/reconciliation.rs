//! [`ReconciliationTolerance`]/[`ReconciliationDiscrepancy`]: the
//! configurable-tolerance comparison result type used by
//! `Account::reconcile_against_exchange`. See
//! `docs/superpowers/specs/2026-08-02-apexes-account-design.md` §3.2.

use rust_decimal::Decimal;

/// Tolerance for `Account::reconcile_against_exchange`: a discrepancy
/// is only reported if it exceeds BOTH the absolute AND relative
/// thresholds (satisfying either is enough to be considered "within
/// tolerance") -- the more permissive combination, chosen so a small
/// asset's proportionally-large-looking relative error isn't
/// over-reported.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReconciliationTolerance {
    pub absolute: Decimal,
    /// e.g. `dec!(0.01)` for 1%.
    pub relative_fraction: Decimal,
}

impl ReconciliationTolerance {
    /// Returns `true` if `|local - exchange|` is within tolerance
    /// (either the absolute threshold OR the relative-to-`local`
    /// threshold is satisfied).
    pub fn within(&self, local: Decimal, exchange: Decimal) -> bool {
        let diff = (local - exchange).abs();
        if diff <= self.absolute {
            return true;
        }
        if local.is_zero() {
            return false;
        }
        let relative = diff / local.abs();
        relative <= self.relative_fraction
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReconciliationDiscrepancy {
    pub asset_or_symbol: String,
    pub local_value: Decimal,
    pub exchange_value: Decimal,
    pub difference: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn tolerance() -> ReconciliationTolerance {
        ReconciliationTolerance {
            absolute: dec!(1),
            relative_fraction: dec!(0.01),
        }
    }

    #[test]
    fn within_absolute_tolerance_passes() {
        assert!(tolerance().within(dec!(100), dec!(100.5)));
    }

    #[test]
    fn within_relative_tolerance_passes_even_when_absolute_diff_exceeds_threshold() {
        // diff = 5, exceeds absolute tolerance of 1, but 5/1000 = 0.5%
        // is within the 1% relative tolerance.
        assert!(tolerance().within(dec!(1000), dec!(1005)));
    }

    #[test]
    fn exceeding_both_thresholds_fails() {
        assert!(!tolerance().within(dec!(100), dec!(150)));
    }

    #[test]
    fn zero_local_value_only_uses_absolute_tolerance() {
        assert!(tolerance().within(Decimal::ZERO, dec!(0.5)));
        assert!(!tolerance().within(Decimal::ZERO, dec!(5)));
    }

    #[test]
    fn discrepancy_is_constructible_and_comparable() {
        let a = ReconciliationDiscrepancy {
            asset_or_symbol: "USDT".to_string(),
            local_value: dec!(100),
            exchange_value: dec!(95),
            difference: dec!(5),
        };
        let b = a.clone();
        assert_eq!(a, b);
    }
}
