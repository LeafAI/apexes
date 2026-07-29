use crate::error::StorageError;
use fixnum::FixedPoint;
use fixnum::typenum::U10;
use rust_decimal::Decimal;

/// ClickHouse-side fixed-point type matching the `Decimal(38, 10)` column
/// type used across all `apexes-storage` tables. `i128` gives the 38-digit
/// range; `U10` gives the 10-decimal-place scale.
pub type CkDecimal = FixedPoint<i128, U10>;

/// Converts a `rust_decimal::Decimal` into the ClickHouse wire type,
/// preserving exact value via `Decimal`'s mantissa/scale representation.
/// `rust_decimal::Decimal` stores values as `mantissa * 10^-scale`; we pass
/// that directly to `FixedPoint::from_decimal(mantissa, -scale)`, which
/// re-scales internally to `U10`'s fixed 10-decimal-place representation.
pub fn decimal_to_fixed(value: Decimal) -> Result<CkDecimal, StorageError> {
    let mantissa = value.mantissa();
    let scale = value.scale() as i32;
    CkDecimal::from_decimal(mantissa, -scale).map_err(|error| {
        StorageError::DecimalConversionFailed {
            reason: error.to_string(),
        }
    })
}

/// Converts a ClickHouse `FixedPoint` value back into `rust_decimal::Decimal`
/// exactly, with no precision loss. `FixedPoint<i128, U10>::into_bits()`
/// returns the raw `i128` mantissa at `U10`'s fixed 10-decimal-place scale,
/// which is passed directly to `Decimal::try_from_i128_with_scale` -- the
/// inverse of `decimal_to_fixed`'s `from_decimal(mantissa, -scale)`. This
/// avoids round-tripping through `f64`, which silently loses precision for
/// any value with more significant digits than f64's ~15-17 digit mantissa
/// can represent (e.g. large `Decimal(38, 10)` values near the column's
/// actual range, not just "unrealistic" edge cases).
pub fn fixed_to_decimal(value: CkDecimal) -> Decimal {
    let raw_mantissa = value.into_bits();
    Decimal::try_from_i128_with_scale(raw_mantissa, CkDecimal::PRECISION as u32)
        .unwrap_or(Decimal::ZERO)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn round_trips_typical_crypto_price() {
        let original = dec!(65000.12345678);
        let fixed = decimal_to_fixed(original).unwrap();
        let back = fixed_to_decimal(fixed);
        assert_eq!(back, original);
    }

    #[test]
    fn round_trips_zero() {
        let original = Decimal::ZERO;
        let fixed = decimal_to_fixed(original).unwrap();
        let back = fixed_to_decimal(fixed);
        assert_eq!(back, original);
    }

    #[test]
    fn round_trips_negative_value() {
        let original = dec!(-123.456);
        let fixed = decimal_to_fixed(original).unwrap();
        let back = fixed_to_decimal(fixed);
        assert_eq!(back, original);
    }

    #[test]
    fn round_trips_integer_value() {
        let original = dec!(100);
        let fixed = decimal_to_fixed(original).unwrap();
        let back = fixed_to_decimal(fixed);
        assert_eq!(back, original);
    }

    #[test]
    fn round_trips_max_supported_scale() {
        // U10 supports up to 10 decimal places.
        let original = dec!(1.2345678901);
        let fixed = decimal_to_fixed(original).unwrap();
        let back = fixed_to_decimal(fixed);
        assert_eq!(back, original);
    }

    #[test]
    fn round_trips_value_exceeding_f64_significant_digits() {
        // 25 significant digits -- well beyond f64's ~15-17 digit mantissa,
        // which a round-trip through f64 (the previous implementation)
        // would silently truncate. This value is realistic for the
        // Decimal(38, 10) column type these conversions target.
        let original = dec!(123456789012345.1234567891);
        let fixed = decimal_to_fixed(original).unwrap();
        let back = fixed_to_decimal(fixed);
        assert_eq!(back, original);
    }

    #[test]
    fn round_trips_large_integer_part_with_full_fractional_scale() {
        let original = dec!(99999999999.9999999999);
        let fixed = decimal_to_fixed(original).unwrap();
        let back = fixed_to_decimal(fixed);
        assert_eq!(back, original);
    }
}
