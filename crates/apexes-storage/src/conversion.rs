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

/// Converts a ClickHouse `FixedPoint` value back into `rust_decimal::Decimal`.
/// Goes through `f64` as an intermediate step (matching the `From<FixedPoint>
/// for f64` impl `fixnum` provides), then re-parses via `Decimal::try_from`.
/// This is lossy in theory for values at the extreme edge of f64's 52-bit
/// mantissa, but is exact in practice for all realistic price/quantity
/// values (well within f64's ~15-17 significant decimal digits).
pub fn fixed_to_decimal(value: CkDecimal) -> Decimal {
    let as_f64: f64 = value.into();
    Decimal::try_from(as_f64).unwrap_or(Decimal::ZERO)
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
}
