//! F03.02: explicit, reproducible simulated execution assumptions.
use apexes_core::{ApexesError, AssetClass, Instrument, Side, SymbolRules, Venue};
use apexes_strategy::{OrderIntent, OrderRouting};
use chrono::{DateTime, TimeDelta, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SimulatedExecution {
    #[default]
    UnrestrictedMarket,
    BoundedSpotIoc {
        rules: BTreeMap<String, SymbolRules>,
        max_reference_age_ms: u32,
    },
}

fn reject(reason: impl Into<String>) -> ApexesError {
    ApexesError::OrderRejected {
        reason: reason.into(),
    }
}

impl SimulatedExecution {
    pub fn validate(&self, instruments: &[Instrument]) -> Result<(), String> {
        if let Self::BoundedSpotIoc {
            rules,
            max_reference_age_ms,
        } = self
        {
            if *max_reference_age_ms == 0 || instruments.is_empty() {
                return Err("bounded IOC requires instruments and positive reference age".into());
            }
            if instruments.iter().any(|i| {
                i.venue != Venue::Binance
                    || i.asset_class != AssetClass::Spot
                    || !rules.contains_key(&i.symbol)
            }) || rules.len() != instruments.len()
            {
                return Err("bounded IOC requires exact Binance Spot symbol rule coverage".into());
            }
            for r in rules.values() {
                if r.price_tick <= Decimal::ZERO
                    || r.quantity_step <= Decimal::ZERO
                    || r.min_quantity <= Decimal::ZERO
                    || r.max_quantity < r.min_quantity
                    || r.min_notional < Decimal::ZERO
                {
                    return Err("invalid historical Spot rules".into());
                }
            }
        }
        Ok(())
    }

    pub fn from_registry(
        registry: &apexes_core::registry::Registry,
        instruments: &[Instrument],
    ) -> Result<Self, ApexesError> {
        let policy = registry
            .try_one::<Self>()
            .map_err(|e| ApexesError::InvalidBacktestConfig {
                reason: e.to_string(),
            })?
            .unwrap_or_default();
        policy
            .validate(instruments)
            .map_err(|reason| ApexesError::InvalidBacktestConfig { reason })?;
        Ok(policy)
    }

    pub fn bounded(&self) -> bool {
        matches!(self, Self::BoundedSpotIoc { .. })
    }

    /// Validate scope for all orders, including those passed directly to
    /// the shared matching engine by the EMS adapter.
    pub(crate) fn validate_intent(&self, intent: &OrderIntent) -> Result<(), ApexesError> {
        if let Self::BoundedSpotIoc { rules, .. } = self
            && (intent.venue != Venue::Binance
                || intent.asset_class != AssetClass::Spot
                || !rules.contains_key(&intent.symbol)
                || intent.routing != OrderRouting::Fixed
                || intent.leverage.is_some()
                || intent.margin_mode.is_some()
                || matches!(
                    intent.order_type,
                    apexes_core::OrderType::Stop | apexes_core::OrderType::StopLimit
                ))
        {
            return Err(reject("unsupported bounded Spot simulation intent"));
        }
        Ok(())
    }

    pub(crate) fn market_buy(
        &self,
        intent: &OrderIntent,
        now: DateTime<Utc>,
        reference: Option<(DateTime<Utc>, Decimal)>,
        exact_quantity: bool,
    ) -> Result<(Decimal, Option<Decimal>), ApexesError> {
        self.validate_intent(intent)?;
        let Self::BoundedSpotIoc {
            rules,
            max_reference_age_ms,
        } = self
        else {
            return Ok((intent.quantity, None));
        };
        if intent.side != Side::Buy {
            return Ok((intent.quantity, None));
        }
        let (at, price) = reference.ok_or_else(|| reject("missing causal IOC trade reference"))?;
        if price <= Decimal::ZERO
            || at > now
            || now - at > TimeDelta::milliseconds(i64::from(*max_reference_age_ms))
        {
            return Err(reject("stale or invalid IOC trade reference"));
        }
        let r = rules
            .get(&intent.symbol)
            .ok_or_else(|| reject("missing IOC rules"))?;
        // Checked arithmetic for caller-supplied historical rules.
        let floor = |value: Decimal, step: Decimal| {
            if step <= Decimal::ZERO {
                return None;
            }
            value.checked_div(step)?.floor().checked_mul(step)
        };
        let quantity = floor(intent.quantity, r.quantity_step)
            .filter(|q| {
                *q > Decimal::ZERO
                    && *q <= intent.quantity
                    && *q >= r.min_quantity
                    && *q <= r.max_quantity
            })
            .ok_or_else(|| reject("invalid IOC quantity"))?;
        let cap = floor(price, r.price_tick)
            .filter(|p| *p > Decimal::ZERO && *p <= price)
            .ok_or_else(|| reject("invalid IOC price cap"))?;
        if quantity.checked_mul(cap).is_none_or(|n| n < r.min_notional) {
            return Err(reject("IOC notional below minimum or overflow"));
        }
        if exact_quantity && quantity != intent.quantity {
            return Err(reject("IOC child quantity must conform exactly"));
        }
        Ok((quantity, Some(cap)))
    }
}
