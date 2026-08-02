//! [`FxRateProvider`]: pluggable cross-currency rate lookup for
//! `Account::equity`. See
//! `docs/superpowers/specs/2026-08-02-apexes-account-design.md` §3.3
//! for why this is synchronous, not `async`.

use apexes_core::ApexesError;
use rust_decimal::Decimal;

pub trait FxRateProvider: Send + Sync {
    fn fx_rate(&self, from_currency: &str, to_currency: &str) -> Result<Decimal, ApexesError>;
}

use std::collections::HashMap;

/// A configuration-driven, in-memory FX rate table. `from_currency ==
/// to_currency` always returns `Decimal::ONE` without needing an entry
/// in `rates`. Real (asynchronous, network-backed) FX data sources are
/// explicitly out of scope for this crate -- see the design spec's
/// "not included" section.
pub struct StaticFxRateProvider {
    rates: HashMap<(String, String), Decimal>,
}

impl StaticFxRateProvider {
    pub fn new(rates: HashMap<(String, String), Decimal>) -> Self {
        Self { rates }
    }
}

impl FxRateProvider for StaticFxRateProvider {
    fn fx_rate(&self, from_currency: &str, to_currency: &str) -> Result<Decimal, ApexesError> {
        if from_currency == to_currency {
            return Ok(Decimal::ONE);
        }
        self.rates
            .get(&(from_currency.to_string(), to_currency.to_string()))
            .copied()
            .ok_or_else(|| ApexesError::InvalidBacktestConfig {
                reason: format!("no FX rate configured for {from_currency} -> {to_currency}"),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    #[test]
    fn same_currency_is_always_rate_one() {
        let provider = StaticFxRateProvider::new(HashMap::new());
        assert_eq!(provider.fx_rate("USDT", "USDT").unwrap(), dec!(1));
    }

    #[test]
    fn looks_up_configured_cross_currency_rate() {
        let mut rates = HashMap::new();
        rates.insert(("USDT".to_string(), "CNY".to_string()), dec!(7.2));
        let provider = StaticFxRateProvider::new(rates);

        assert_eq!(provider.fx_rate("USDT", "CNY").unwrap(), dec!(7.2));
    }

    #[test]
    fn missing_rate_is_an_error() {
        let provider = StaticFxRateProvider::new(HashMap::new());
        let result = provider.fx_rate("USDT", "CNY");
        assert!(result.is_err());
    }
}
