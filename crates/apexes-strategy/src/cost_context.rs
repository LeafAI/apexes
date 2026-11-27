//! F04.03 — Read-only frozen simulated execution fee assumptions.
//! This is not an exchange fee quote or a total execution cost estimate.
use apexes_core::{AssetClass, Instrument, MarketEvent, Venue};
use rust_decimal::Decimal;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeeRole {
    Maker,
    Taker,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CostObservation {
    symbol: String,
    venue: Venue,
    asset_class: AssetClass,
    base_currency: String,
    quote_currency: String,
    maker_fee_bps: Decimal,
    taker_fee_bps: Decimal,
}

impl CostObservation {
    /// Restrict to the known quote-notional convention of unit-multiplier
    /// Spot and perpetual instruments. Rates must be frozen session rates.
    pub fn from_frozen_rates(
        instrument: &Instrument,
        maker_fee_bps: Decimal,
        taker_fee_bps: Decimal,
    ) -> Option<Self> {
        let quote = instrument
            .quote_currency
            .as_ref()
            .filter(|q| !q.is_empty())?;
        let base = instrument
            .base_currency
            .as_ref()
            .filter(|b| !b.is_empty() && *b != quote)?;
        if !matches!(
            instrument.asset_class,
            AssetClass::Spot | AssetClass::Perpetual
        ) || instrument.multiplier != Decimal::ONE
            || [maker_fee_bps, taker_fee_bps]
                .iter()
                .any(|r| *r < Decimal::ZERO || *r >= Decimal::from(10000))
        {
            return None;
        }
        Some(Self {
            symbol: instrument.symbol.clone(),
            venue: instrument.venue,
            asset_class: instrument.asset_class,
            base_currency: base.clone(),
            quote_currency: quote.clone(),
            maker_fee_bps,
            taker_fee_bps,
        })
    }

    pub fn matches(&self, event: &MarketEvent, instrument: &Instrument) -> bool {
        self.symbol == event.symbol()
            && self.venue == event.venue()
            && self.symbol == instrument.symbol
            && self.venue == instrument.venue
            && instrument.quote_currency.as_deref() == Some(self.quote_currency.as_str())
            && instrument.asset_class == self.asset_class
            && instrument.base_currency.as_deref() == Some(self.base_currency.as_str())
            && instrument.multiplier == Decimal::ONE
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }
    pub fn venue(&self) -> Venue {
        self.venue
    }
    pub fn quote_currency(&self) -> &str {
        &self.quote_currency
    }
    pub fn maker_fee_bps(&self) -> Decimal {
        self.maker_fee_bps
    }
    pub fn taker_fee_bps(&self) -> Decimal {
        self.taker_fee_bps
    }

    /// One hypothetical fill's fee, in quote currency, at a supplied
    /// positive quote notional. Match the simulator's multiplication order;
    /// return unavailable on arithmetic overflow instead of fabricating zero.
    pub fn estimate_fee(&self, quote_notional: Decimal, role: FeeRole) -> Option<Decimal> {
        if quote_notional <= Decimal::ZERO {
            return None;
        }
        let rate = match role {
            FeeRole::Maker => self.maker_fee_bps,
            FeeRole::Taker => self.taker_fee_bps,
        };
        quote_notional
            .checked_mul(rate)?
            .checked_div(Decimal::from(10000))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::MarketType;
    use rust_decimal_macros::dec;

    fn instrument() -> Instrument {
        Instrument::new(
            "BTCUSDT",
            MarketType::Crypto,
            Venue::Binance,
            AssetClass::Spot,
            Some("BTC".into()),
            Some("USDT".into()),
            dec!(0.01),
            dec!(0.001),
            Decimal::ONE,
        )
        .unwrap()
    }

    #[test]
    fn fee_estimate_checks_notional_rates_scope_and_overflow() {
        let mut i = instrument();
        let c = CostObservation::from_frozen_rates(&i, dec!(5), dec!(10)).unwrap();
        assert_eq!(c.estimate_fee(dec!(99), FeeRole::Maker), Some(dec!(0.0495)));
        assert_eq!(c.estimate_fee(dec!(100), FeeRole::Taker), Some(dec!(0.1)));
        assert_eq!(c.estimate_fee(Decimal::ZERO, FeeRole::Maker), None);
        assert_eq!(c.estimate_fee(dec!(-1), FeeRole::Maker), None);
        assert_eq!(c.estimate_fee(Decimal::MAX, FeeRole::Taker), None);
        let zero = CostObservation::from_frozen_rates(&i, Decimal::ZERO, Decimal::ZERO).unwrap();
        assert_eq!(
            zero.estimate_fee(Decimal::MAX, FeeRole::Maker),
            Some(Decimal::ZERO)
        );
        assert!(CostObservation::from_frozen_rates(&i, dec!(-1), dec!(10)).is_none());
        assert!(CostObservation::from_frozen_rates(&i, dec!(5), dec!(10000)).is_none());
        i.multiplier = dec!(2);
        assert!(CostObservation::from_frozen_rates(&i, dec!(5), dec!(10)).is_none());
        i.multiplier = Decimal::ONE;
        i.quote_currency = None;
        assert!(CostObservation::from_frozen_rates(&i, dec!(5), dec!(10)).is_none());
    }
}
