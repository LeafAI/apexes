use crate::error::ApexesError;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MarketType {
    AShare,
    HShare,
    UsEquity,
    Crypto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Venue {
    Sse,
    Szse,
    Hkex,
    Nasdaq,
    Nyse,
    Binance,
    Okx,
    Coinbase,
}

impl Venue {
    pub fn market_type(&self) -> MarketType {
        match self {
            Venue::Sse | Venue::Szse => MarketType::AShare,
            Venue::Hkex => MarketType::HShare,
            Venue::Nasdaq | Venue::Nyse => MarketType::UsEquity,
            Venue::Binance | Venue::Okx | Venue::Coinbase => MarketType::Crypto,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AssetClass {
    Spot,
    Perpetual,
    Future,
    Option,
    Equity,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Instrument {
    pub symbol: String,
    pub market_type: MarketType,
    pub venue: Venue,
    pub asset_class: AssetClass,
    pub base_currency: Option<String>,
    pub quote_currency: Option<String>,
    pub tick_size: Decimal,
    pub lot_size: Decimal,
    pub multiplier: Decimal,
}

impl Instrument {
    /// Validates that `venue` belongs to `market_type` before constructing.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        symbol: impl Into<String>,
        market_type: MarketType,
        venue: Venue,
        asset_class: AssetClass,
        base_currency: Option<String>,
        quote_currency: Option<String>,
        tick_size: Decimal,
        lot_size: Decimal,
        multiplier: Decimal,
    ) -> Result<Self, ApexesError> {
        if venue.market_type() != market_type {
            return Err(ApexesError::InvalidInstrument {
                reason: format!("venue {venue:?} does not belong to market_type {market_type:?}"),
            });
        }
        Ok(Self {
            symbol: symbol.into(),
            market_type,
            venue,
            asset_class,
            base_currency,
            quote_currency,
            tick_size,
            lot_size,
            multiplier,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn venue_market_type_mapping() {
        assert_eq!(Venue::Sse.market_type(), MarketType::AShare);
        assert_eq!(Venue::Szse.market_type(), MarketType::AShare);
        assert_eq!(Venue::Hkex.market_type(), MarketType::HShare);
        assert_eq!(Venue::Nasdaq.market_type(), MarketType::UsEquity);
        assert_eq!(Venue::Nyse.market_type(), MarketType::UsEquity);
        assert_eq!(Venue::Binance.market_type(), MarketType::Crypto);
        assert_eq!(Venue::Okx.market_type(), MarketType::Crypto);
        assert_eq!(Venue::Coinbase.market_type(), MarketType::Crypto);
    }

    #[test]
    fn new_succeeds_when_venue_matches_market_type() {
        let instrument = Instrument::new(
            "BTCUSDT",
            MarketType::Crypto,
            Venue::Binance,
            AssetClass::Spot,
            Some("BTC".to_string()),
            Some("USDT".to_string()),
            dec!(0.01),
            dec!(0.0001),
            dec!(1),
        );
        assert!(instrument.is_ok());
        let instrument = instrument.unwrap();
        assert_eq!(instrument.symbol, "BTCUSDT");
        assert_eq!(instrument.venue, Venue::Binance);
    }

    #[test]
    fn new_fails_when_venue_does_not_match_market_type() {
        let result = Instrument::new(
            "600519",
            MarketType::AShare,
            Venue::Binance,
            AssetClass::Equity,
            None,
            Some("CNY".to_string()),
            dec!(0.01),
            dec!(100),
            dec!(1),
        );
        assert!(matches!(result, Err(ApexesError::InvalidInstrument { .. })));
    }

    #[test]
    fn instrument_serde_round_trip() {
        let instrument = Instrument::new(
            "0700",
            MarketType::HShare,
            Venue::Hkex,
            AssetClass::Equity,
            None,
            Some("HKD".to_string()),
            dec!(0.2),
            dec!(100),
            dec!(1),
        )
        .unwrap();
        let json = serde_json::to_string(&instrument).unwrap();
        let round_tripped: Instrument = serde_json::from_str(&json).unwrap();
        assert_eq!(instrument, round_tripped);
    }
}
