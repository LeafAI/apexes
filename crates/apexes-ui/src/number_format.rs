//! How numbers are shown to an operator.
//!
//! The engine's decimals carry whatever scale the arithmetic left them with:
//! `9940.9058680000000000` for a balance, `0.00000000` for a closed position.
//! That scale is an accident of the calculation, so it is hidden here, while
//! the digits that mean something are kept.

use rust_decimal::Decimal;

/// Most decimal places any asset amount is shown with (Binance's own limit).
const MAX_AMOUNT_DP: u32 = 8;

/// An amount of an asset: trailing zeros dropped, at most 8 places, never in
/// scientific notation, and `0` for zero. `9940.905868000000` -> `9940.905868`,
/// `1.00060000` -> `1.0006`, `0.00000000` -> `0`.
pub fn amount(value: Decimal) -> String {
    plain(value.round_dp(MAX_AMOUNT_DP))
}

/// A price: like [`amount`], but at least two decimals so `64657.62` and
/// `64650` line up as prices (`64650.00`), as exchanges show them.
pub fn price(value: Decimal) -> String {
    let rounded = value.round_dp(MAX_AMOUNT_DP).normalize();
    if rounded.scale() < 2 {
        format!("{rounded:.2}")
    } else {
        plain(rounded)
    }
}

/// Quote-currency money (equity, PnL, balances of the quote asset): two
/// places, which is what the figure is good to.
pub fn money(value: Decimal) -> String {
    format!("{:.2}", value.round_dp(2))
}

/// `normalize()` renders zero as `0` and drops trailing zeros without
/// switching to exponent form, which `Decimal` never uses for display.
fn plain(value: Decimal) -> String {
    let normalized = value.normalize();
    if normalized.is_zero() {
        "0".to_string()
    } else {
        normalized.to_string()
    }
}

/// The fee text for the evaluation panel: only currencies that actually
/// charged something, or a plain `0` when none did (a testnet charges
/// nothing, and `0.00000000 BTC, 0.00000000 USDT` reads like missing data).
pub fn fees(paid: &std::collections::HashMap<String, Decimal>) -> String {
    let mut charged: Vec<_> = paid.iter().filter(|(_, v)| !v.is_zero()).collect();
    if charged.is_empty() {
        return "0".to_string();
    }
    charged.sort_by(|a, b| a.0.cmp(b.0));
    charged
        .iter()
        .map(|(currency, value)| {
            let label = if currency.is_empty() {
                "?"
            } else {
                currency.as_str()
            };
            format!("{} {label}", amount(**value))
        })
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;
    use std::str::FromStr;

    #[test]
    fn the_screenshots_balances_read_cleanly() {
        let usdt = Decimal::from_str("9940.9058680000000000").unwrap();
        assert_eq!(amount(usdt), "9940.905868");
        assert_eq!(amount(Decimal::from_str("1.00060000").unwrap()), "1.0006");
    }

    #[test]
    fn zero_is_a_plain_zero_however_it_was_computed() {
        for zero in ["0", "0.00000000", "-0", "0.0000000000000000"] {
            assert_eq!(amount(Decimal::from_str(zero).unwrap()), "0", "{zero}");
        }
        assert_eq!(amount(dec!(-0.00000000001)), "0", "rounds away, no `-0`");
    }

    #[test]
    fn amounts_keep_up_to_eight_places_and_no_more() {
        assert_eq!(amount(dec!(0.12345678)), "0.12345678");
        assert_eq!(amount(dec!(0.123456789)), "0.12345679");
        assert_eq!(amount(dec!(1234567890.5)), "1234567890.5");
        assert_eq!(amount(dec!(-12.50)), "-12.5");
    }

    #[test]
    fn amounts_never_use_scientific_notation() {
        assert_eq!(amount(dec!(0.00000001)), "0.00000001");
        assert!(!amount(dec!(1e-8)).contains('e'));
    }

    #[test]
    fn prices_always_show_two_decimals_at_least() {
        assert_eq!(price(dec!(64657.62000000)), "64657.62");
        assert_eq!(price(dec!(64650)), "64650.00");
        assert_eq!(price(dec!(64650.5)), "64650.50");
        assert_eq!(price(dec!(0.00001234)), "0.00001234");
    }

    #[test]
    fn money_is_two_places() {
        assert_eq!(money(dec!(74078.28)), "74078.28");
        assert_eq!(money(dec!(-271.9)), "-271.90");
        assert_eq!(money(dec!(0)), "0.00");
    }

    #[test]
    fn no_fee_charged_reads_as_zero_not_as_a_list_of_zeros() {
        let none: HashMap<String, Decimal> = HashMap::new();
        assert_eq!(fees(&none), "0");
        let zeros = HashMap::from([
            ("BTC".to_string(), dec!(0.00000000)),
            ("USDT".to_string(), dec!(0.00000000)),
        ]);
        assert_eq!(fees(&zeros), "0", "the testnet's all-zero fees");
    }

    #[test]
    fn only_currencies_that_charged_are_listed_in_a_stable_order() {
        let paid = HashMap::from([
            ("USDT".to_string(), dec!(0.40)),
            ("BNB".to_string(), dec!(0.0004)),
            ("BTC".to_string(), dec!(0)),
            (String::new(), dec!(0.1)),
        ]);
        assert_eq!(fees(&paid), "0.1 ?, 0.0004 BNB, 0.4 USDT");
    }
}
