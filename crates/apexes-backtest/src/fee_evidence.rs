//! F04.05: read-only retained Spot attribution state, not a restore API.
use crate::observation_receipt::ObservationError;
use apexes_core::{FillId, Venue};
use chrono::{DateTime, TimeDelta, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

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
pub(crate) const MAX_DECISIONS: usize = 8192;

/// Actual valuation path, not a reconstructed historical observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum FeeDecisionSource {
    Zero,
    Base,
    Quote,
    Observed {
        observed_at: DateTime<Utc>,
        rate: Decimal,
    },
    Missing,
    /// A caller supplied a valuation; the underlying rate is not known here.
    Provided,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeeDecisionEvidence {
    pub fill_id: FillId,
    pub venue: Venue,
    pub symbol: String,
    pub asset: String,
    pub filled_at: DateTime<Utc>,
    pub price: Decimal,
    pub fee: Decimal,
    pub external_fee: bool,
    pub value: Option<Decimal>,
    pub source: FeeDecisionSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeeValuationEvidence {
    pub version: u32,
    pub accounts: Vec<FeeAccountEvidence>,
    pub prices: Vec<ConversionPriceEvidence>,
    /// Actual attribution order. Absent on older captures.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decisions: Option<Vec<FeeDecisionEvidence>>,
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
#[derive(Default)]
struct DecisionTotals {
    fees: BTreeMap<String, Decimal>,
    external: BTreeMap<String, Decimal>,
    historical: Decimal,
    missing: u64,
}

impl FeeDecisionEvidence {
    fn validate(&self, account: &FeeAccountEvidence) -> Result<(), ObservationError> {
        use FeeDecisionSource::*;
        use ObservationError as E;
        if self.venue != account.venue
            || self.symbol != account.symbol
            || !valid_key(&self.asset)
            || self.price <= Decimal::ZERO
            || self.fee < Decimal::ZERO
            || self.value.is_some_and(|v| v < Decimal::ZERO)
        {
            return Err(E::Encoding);
        }
        let valid = match &self.source {
            Zero => self.fee.is_zero() && self.value == Some(Decimal::ZERO),
            Base => {
                self.fee > Decimal::ZERO
                    && self.asset == account.base
                    && self
                        .fee
                        .checked_mul(self.price)
                        .is_some_and(|v| self.value == Some(v))
            }
            Quote => {
                self.fee > Decimal::ZERO
                    && self.asset == account.quote
                    && self.value == Some(self.fee)
            }
            Observed { observed_at, rate } => {
                self.fee > Decimal::ZERO
                    && self.asset != account.base
                    && self.asset != account.quote
                    && *rate > Decimal::ZERO
                    && *observed_at <= self.filled_at
                    && self.filled_at.signed_duration_since(*observed_at) <= TimeDelta::minutes(5)
                    && self
                        .fee
                        .checked_mul(*rate)
                        .is_some_and(|v| self.value == Some(v))
            }
            Missing => {
                self.fee > Decimal::ZERO
                    && self.asset != account.base
                    && self.asset != account.quote
                    && self.value.is_none()
            }
            Provided => true,
        };
        if !valid {
            return Err(E::Encoding);
        }
        Ok(())
    }
}

fn add_value(target: &mut Decimal, value: Decimal) -> Result<(), ObservationError> {
    *target = target
        .checked_add(value)
        .ok_or(ObservationError::Encoding)?;
    Ok(())
}

impl FeeValuationEvidence {
    fn validate_decisions(
        &self,
        decisions: &[FeeDecisionEvidence],
    ) -> Result<(), ObservationError> {
        use ObservationError as E;
        if decisions.len() > MAX_DECISIONS {
            return Err(E::Capacity);
        }
        let mut ids = HashSet::new();
        let mut totals = BTreeMap::<&str, DecisionTotals>::new();
        for d in decisions {
            if !ids.insert(d.fill_id) {
                return Err(E::Encoding);
            }
            let i = self
                .accounts
                .binary_search_by(|a| a.symbol.cmp(&d.symbol))
                .map_err(|_| E::Encoding)?;
            d.validate(&self.accounts[i])?;
            let total = totals.entry(&d.symbol).or_default();
            if !d.fee.is_zero() {
                add_value(total.fees.entry(d.asset.clone()).or_default(), d.fee)?;
                if d.external_fee {
                    add_value(total.external.entry(d.asset.clone()).or_default(), d.fee)?;
                }
            }
            if let Some(value) = d.value {
                add_value(&mut total.historical, value)?;
            } else {
                total.missing += 1;
            }
        }
        if totals.len() != self.accounts.len() {
            return Err(E::Encoding);
        }
        for a in &self.accounts {
            let total = totals.get(a.symbol.as_str()).ok_or(E::Encoding)?;
            if a.fees.iter().map(|(k, v)| (k, v)).ne(total.fees.iter())
                || a.external_fees
                    .iter()
                    .map(|(k, v)| (k, v))
                    .ne(total.external.iter())
                || a.historical_fee != total.historical
                || a.missing_execution_valuations != total.missing
            {
                return Err(E::Encoding);
            }
        }
        Ok(())
    }

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
        if let Some(decisions) = &self.decisions {
            self.validate_decisions(decisions)?;
        }
        Ok(())
    }
    // The market cursor bounds price observations, not supplemental fill
    // callbacks. Fill time is checked against its own selected conversion.
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
