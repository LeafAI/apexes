use crate::conversion::{CkDecimal, decimal_to_fixed, fixed_to_decimal};
use crate::error::StorageError;
use apexes_core::{Side, Tick, TradeEvent, Venue};
use chrono::{DateTime, Utc};
use clickhouse::Row;
use serde::{Deserialize, Serialize};

pub const TRADES_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS trades
(
    symbol      String,
    venue       String,
    price       Decimal(38, 10),
    quantity    Decimal(38, 10),
    side        Nullable(String),
    trade_id    Nullable(String),
    timestamp   DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (venue, symbol, timestamp)
";

pub const TICKS_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS ticks
(
    symbol      String,
    venue       String,
    bid_price   Decimal(38, 10),
    bid_size    Decimal(38, 10),
    ask_price   Decimal(38, 10),
    ask_size    Decimal(38, 10),
    timestamp   DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (venue, symbol, timestamp)
";

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct TradeRow {
    pub symbol: String,
    pub venue: String,
    pub price: CkDecimal,
    pub quantity: CkDecimal,
    pub side: Option<String>,
    pub trade_id: Option<String>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
}

impl TryFrom<&TradeEvent> for TradeRow {
    type Error = StorageError;

    fn try_from(event: &TradeEvent) -> Result<Self, StorageError> {
        Ok(TradeRow {
            symbol: event.symbol.clone(),
            venue: format!("{:?}", event.venue),
            price: decimal_to_fixed(event.price)?,
            quantity: decimal_to_fixed(event.quantity)?,
            side: event.side.map(|s| format!("{s:?}")),
            trade_id: event.trade_id.clone(),
            timestamp: event.timestamp,
        })
    }
}

impl TryFrom<TradeRow> for TradeEvent {
    type Error = StorageError;

    fn try_from(row: TradeRow) -> Result<Self, StorageError> {
        Ok(TradeEvent {
            symbol: row.symbol,
            venue: parse_venue(&row.venue)?,
            price: fixed_to_decimal(row.price),
            quantity: fixed_to_decimal(row.quantity),
            side: row.side.map(|s| parse_side(&s)).transpose()?,
            trade_id: row.trade_id,
            timestamp: row.timestamp,
        })
    }
}

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct TickRow {
    pub symbol: String,
    pub venue: String,
    pub bid_price: CkDecimal,
    pub bid_size: CkDecimal,
    pub ask_price: CkDecimal,
    pub ask_size: CkDecimal,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
}

impl TryFrom<&Tick> for TickRow {
    type Error = StorageError;

    fn try_from(tick: &Tick) -> Result<Self, StorageError> {
        Ok(TickRow {
            symbol: tick.symbol.clone(),
            venue: format!("{:?}", tick.venue),
            bid_price: decimal_to_fixed(tick.bid_price)?,
            bid_size: decimal_to_fixed(tick.bid_size)?,
            ask_price: decimal_to_fixed(tick.ask_price)?,
            ask_size: decimal_to_fixed(tick.ask_size)?,
            timestamp: tick.timestamp,
        })
    }
}

impl TryFrom<TickRow> for Tick {
    type Error = StorageError;

    fn try_from(row: TickRow) -> Result<Self, StorageError> {
        Ok(Tick {
            symbol: row.symbol,
            venue: parse_venue(&row.venue)?,
            bid_price: fixed_to_decimal(row.bid_price),
            bid_size: fixed_to_decimal(row.bid_size),
            ask_price: fixed_to_decimal(row.ask_price),
            ask_size: fixed_to_decimal(row.ask_size),
            timestamp: row.timestamp,
        })
    }
}

/// Parses a `Venue`'s `{:?}` (Debug) representation back into the enum.
/// `Venue` derives `Debug`, which for a unit-variant enum prints exactly
/// the variant name (e.g. "Binance"), so this round-trips `format!("{:?}",
/// venue)` used when building rows.
pub(crate) fn parse_venue(value: &str) -> Result<Venue, StorageError> {
    match value {
        "Sse" => Ok(Venue::Sse),
        "Szse" => Ok(Venue::Szse),
        "Hkex" => Ok(Venue::Hkex),
        "Nasdaq" => Ok(Venue::Nasdaq),
        "Nyse" => Ok(Venue::Nyse),
        "Binance" => Ok(Venue::Binance),
        "Okx" => Ok(Venue::Okx),
        "Coinbase" => Ok(Venue::Coinbase),
        other => Err(StorageError::QueryFailed {
            reason: format!("unrecognized venue string in ClickHouse row: {other}"),
        }),
    }
}

pub(crate) fn parse_side(value: &str) -> Result<Side, StorageError> {
    match value {
        "Buy" => Ok(Side::Buy),
        "Sell" => Ok(Side::Sell),
        other => Err(StorageError::QueryFailed {
            reason: format!("unrecognized side string in ClickHouse row: {other}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::Venue;
    use rust_decimal_macros::dec;

    #[test]
    fn trade_event_round_trips_through_trade_row() {
        let event = TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(65000.5),
            quantity: dec!(0.5),
            side: Some(Side::Buy),
            trade_id: Some("T123".to_string()),
            timestamp: Utc::now(),
        };

        let row = TradeRow::try_from(&event).unwrap();
        let back = TradeEvent::try_from(row).unwrap();

        assert_eq!(back.symbol, event.symbol);
        assert_eq!(back.venue, event.venue);
        assert_eq!(back.price, event.price);
        assert_eq!(back.quantity, event.quantity);
        assert_eq!(back.side, event.side);
        assert_eq!(back.trade_id, event.trade_id);
    }

    #[test]
    fn trade_event_with_none_side_and_trade_id_round_trips() {
        let event = TradeEvent {
            symbol: "ETHUSDT".to_string(),
            venue: Venue::Okx,
            price: dec!(3200),
            quantity: dec!(2),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        };

        let row = TradeRow::try_from(&event).unwrap();
        let back = TradeEvent::try_from(row).unwrap();

        assert_eq!(back.side, None);
        assert_eq!(back.trade_id, None);
    }

    #[test]
    fn tick_round_trips_through_tick_row() {
        let tick = Tick {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bid_price: dec!(64999.5),
            bid_size: dec!(1.5),
            ask_price: dec!(65000.5),
            ask_size: dec!(2.0),
            timestamp: Utc::now(),
        };

        let row = TickRow::try_from(&tick).unwrap();
        let back = Tick::try_from(row).unwrap();

        assert_eq!(back.symbol, tick.symbol);
        assert_eq!(back.venue, tick.venue);
        assert_eq!(back.bid_price, tick.bid_price);
        assert_eq!(back.bid_size, tick.bid_size);
        assert_eq!(back.ask_price, tick.ask_price);
        assert_eq!(back.ask_size, tick.ask_size);
    }

    #[test]
    fn parse_venue_rejects_unknown_string() {
        let result = parse_venue("NotARealVenue");
        assert!(result.is_err());
    }
}
