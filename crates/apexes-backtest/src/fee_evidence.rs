//! F04.05: read-only retained Spot attribution state, not a restore API.
use crate::observation_receipt::ObservationError;
use apexes_core::Venue;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub(crate) const MAX_PRICE_ROWS: usize = 16384;
pub(crate) const MAX_FEE_ASSETS: usize = 128;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeeAccountEvidence {
    pub symbol: String,
    pub venue: Venue,
    pub base: String,
    pub quote: String,
    pub gross_quantity: Decimal,
    pub entry: Decimal,
    pub gross_realized: Decimal,
    pub fees: Vec<(String, Decimal)>,
    pub external_fees: Vec<(String, Decimal)>,
    /// Known subtotal even if some executions lack a valuation.
    pub historical_fee: Decimal,
    pub missing_execution_valuations: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversionPriceEvidence {
    pub venue: Venue,
    pub base: String,
    pub quote: String,
    pub observations: Vec<(DateTime<Utc>, Decimal)>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeeValuationEvidence {
    pub version: u32,
    pub accounts: Vec<FeeAccountEvidence>,
    pub prices: Vec<ConversionPriceEvidence>,
}

fn valid_key(s: &str) -> bool {
    !s.is_empty() && s.len() <= 128 && !s.chars().any(char::is_control)
}
fn valid_fees(fees: &[(String, Decimal)]) -> bool {
    fees.len() <= MAX_FEE_ASSETS
        && fees.windows(2).all(|w| w[0].0 < w[1].0)
        && fees
            .iter()
            .all(|(asset, value)| valid_key(asset) && *value >= Decimal::ZERO)
}
pub(crate) fn pair_key<'a>(
    venue: Venue,
    base: &'a str,
    quote: &'a str,
) -> (String, &'a str, &'a str) {
    (format!("{venue:?}"), base, quote)
}
impl FeeAccountEvidence {
    fn validate(&self, scope: &[(Venue, String)]) -> Result<(), ObservationError> {
        if !scope
            .iter()
            .any(|(v, s)| *v == self.venue && s == &self.symbol)
            || !valid_key(&self.base)
            || !valid_key(&self.quote)
            || self.base == self.quote
            || self.entry < Decimal::ZERO
            || (!self.gross_quantity.is_zero() && self.entry.is_zero())
            || self.historical_fee < Decimal::ZERO
            || self.missing_execution_valuations > 8192
            || !valid_fees(&self.fees)
            || !valid_fees(&self.external_fees)
            || self.external_fees.iter().any(|(asset, amount)| {
                self.fees
                    .binary_search_by(|(a, _)| a.cmp(asset))
                    .ok()
                    .is_none_or(|i| self.fees[i].1 < *amount)
            })
        {
            return Err(ObservationError::Encoding);
        }
        Ok(())
    }
}
impl FeeValuationEvidence {
    pub fn validate(&self, scope: &[(Venue, String)]) -> Result<(), ObservationError> {
        use ObservationError as E;
        if self.version != 1
            || self.accounts.len() > 128
            || self.prices.len() > 128
            || self
                .prices
                .iter()
                .map(|p| p.observations.len())
                .try_fold(0usize, usize::checked_add)
                .is_none_or(|n| n > MAX_PRICE_ROWS)
        {
            return Err(E::Capacity);
        }
        if !self.accounts.windows(2).all(|w| w[0].symbol < w[1].symbol)
            || !self.prices.windows(2).all(|w| {
                pair_key(w[0].venue, &w[0].base, &w[0].quote)
                    < pair_key(w[1].venue, &w[1].base, &w[1].quote)
            })
        {
            return Err(E::Encoding);
        }
        for account in &self.accounts {
            account.validate(scope)?;
        }
        for p in &self.prices {
            if !scope.iter().any(|(v, _)| *v == p.venue)
                || !valid_key(&p.base)
                || !valid_key(&p.quote)
                || p.base == p.quote
                || p.observations.is_empty()
                || p.observations.len() > 2048
                || !p.observations.windows(2).all(|w| w[0].0 < w[1].0)
                || p.observations
                    .iter()
                    .any(|(_, price)| *price <= Decimal::ZERO)
            {
                return Err(E::Encoding);
            }
        }
        Ok(())
    }
    pub(crate) fn validate_through(&self, through: DateTime<Utc>) -> Result<(), ObservationError> {
        if self
            .prices
            .iter()
            .any(|p| p.observations.last().is_some_and(|(at, _)| *at > through))
        {
            return Err(ObservationError::Time);
        }
        Ok(())
    }
}
