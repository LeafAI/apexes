use crate::conversion::{CkDecimal, decimal_to_fixed, fixed_to_decimal};
use crate::error::StorageError;
use apexes_core::{
    Bar, BarInterval, DepthUpdate, MarkPriceUpdate, PriceLevel, Side, Tick, TradeEvent, Venue,
};
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

pub const DEPTH_UPDATES_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS depth_updates
(
    symbol      String,
    venue       String,
    bids        Array(Tuple(Decimal(38, 10), Decimal(38, 10))),
    asks        Array(Tuple(Decimal(38, 10), Decimal(38, 10))),
    sequence    Nullable(UInt64),
    source_first Nullable(UInt64),
    source_last Nullable(UInt64),
    source_previous_final Nullable(UInt64),
    is_snapshot Bool,
    timestamp   DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (venue, symbol, timestamp)
";

pub const BARS_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS bars
(
    symbol      String,
    venue       String,
    open        Decimal(38, 10),
    high        Decimal(38, 10),
    low         Decimal(38, 10),
    close       Decimal(38, 10),
    volume      Decimal(38, 10),
    interval    String,
    open_time   DateTime64(3, 'UTC'),
    close_time  DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(open_time)
ORDER BY (venue, symbol, interval, open_time)
";

pub const MARK_PRICES_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS mark_prices
(
    symbol      String,
    venue       String,
    mark_price  Decimal(38, 10),
    timestamp   DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (venue, symbol, timestamp)
";

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct MarkPriceRow {
    pub symbol: String,
    pub venue: String,
    pub mark_price: CkDecimal,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
}

impl TryFrom<&MarkPriceUpdate> for MarkPriceRow {
    type Error = StorageError;

    fn try_from(update: &MarkPriceUpdate) -> Result<Self, StorageError> {
        Ok(MarkPriceRow {
            symbol: update.symbol.clone(),
            venue: format!("{:?}", update.venue),
            mark_price: decimal_to_fixed(update.mark_price)?,
            timestamp: update.timestamp,
        })
    }
}

impl TryFrom<MarkPriceRow> for MarkPriceUpdate {
    type Error = StorageError;

    fn try_from(row: MarkPriceRow) -> Result<Self, StorageError> {
        Ok(MarkPriceUpdate {
            symbol: row.symbol,
            venue: parse_venue(&row.venue)?,
            mark_price: fixed_to_decimal(row.mark_price),
            timestamp: row.timestamp,
        })
    }
}

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

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct DepthRow {
    pub symbol: String,
    pub venue: String,
    pub bids: Vec<(CkDecimal, CkDecimal)>,
    pub asks: Vec<(CkDecimal, CkDecimal)>,
    pub sequence: Option<u64>,
    pub source_first: Option<u64>,
    pub source_last: Option<u64>,
    pub source_previous_final: Option<u64>,
    pub is_snapshot: bool,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
}

impl TryFrom<&DepthUpdate> for DepthRow {
    type Error = StorageError;

    fn try_from(update: &DepthUpdate) -> Result<Self, StorageError> {
        update
            .validate_source_range()
            .map_err(|reason| StorageError::InvalidDepthRange { reason })?;
        Ok(DepthRow {
            symbol: update.symbol.clone(),
            venue: format!("{:?}", update.venue),
            bids: levels_to_pairs(&update.bids)?,
            asks: levels_to_pairs(&update.asks)?,
            sequence: update.sequence,
            source_first: update.source_range.map(|r| r.first),
            source_last: update.source_range.map(|r| r.last),
            source_previous_final: update.source_range.and_then(|r| r.previous_final),
            is_snapshot: update.is_snapshot,
            timestamp: update.timestamp,
        })
    }
}

impl TryFrom<DepthRow> for DepthUpdate {
    type Error = StorageError;

    fn try_from(row: DepthRow) -> Result<Self, StorageError> {
        let source_range = match (row.source_first, row.source_last, row.source_previous_final) {
            (None, None, None) => None,
            (Some(first), Some(last), previous_final) => Some(apexes_core::DepthSourceRange {
                first,
                last,
                previous_final,
            }),
            _ => {
                return Err(StorageError::InvalidDepthRange {
                    reason: "partial stored range metadata".into(),
                });
            }
        };
        let update = DepthUpdate {
            symbol: row.symbol,
            venue: parse_venue(&row.venue)?,
            bids: pairs_to_levels(row.bids),
            asks: pairs_to_levels(row.asks),
            sequence: row.sequence,
            source_range,
            is_snapshot: row.is_snapshot,
            timestamp: row.timestamp,
        };
        update
            .validate_source_range()
            .map_err(|reason| StorageError::InvalidDepthRange { reason })?;
        Ok(update)
    }
}

fn levels_to_pairs(levels: &[PriceLevel]) -> Result<Vec<(CkDecimal, CkDecimal)>, StorageError> {
    levels
        .iter()
        .map(|level| {
            Ok((
                decimal_to_fixed(level.price)?,
                decimal_to_fixed(level.quantity)?,
            ))
        })
        .collect()
}

fn pairs_to_levels(pairs: Vec<(CkDecimal, CkDecimal)>) -> Vec<PriceLevel> {
    pairs
        .into_iter()
        .map(|(price, quantity)| PriceLevel {
            price: fixed_to_decimal(price),
            quantity: fixed_to_decimal(quantity),
        })
        .collect()
}

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct BarRow {
    pub symbol: String,
    pub venue: String,
    pub open: CkDecimal,
    pub high: CkDecimal,
    pub low: CkDecimal,
    pub close: CkDecimal,
    pub volume: CkDecimal,
    pub interval: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub open_time: DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub close_time: DateTime<Utc>,
}

impl TryFrom<&Bar> for BarRow {
    type Error = StorageError;

    fn try_from(bar: &Bar) -> Result<Self, StorageError> {
        Ok(BarRow {
            symbol: bar.symbol.clone(),
            venue: format!("{:?}", bar.venue),
            open: decimal_to_fixed(bar.open)?,
            high: decimal_to_fixed(bar.high)?,
            low: decimal_to_fixed(bar.low)?,
            close: decimal_to_fixed(bar.close)?,
            volume: decimal_to_fixed(bar.volume)?,
            interval: format!("{:?}", bar.interval),
            open_time: bar.open_time,
            close_time: bar.close_time,
        })
    }
}

impl TryFrom<BarRow> for Bar {
    type Error = StorageError;

    fn try_from(row: BarRow) -> Result<Self, StorageError> {
        Ok(Bar {
            symbol: row.symbol,
            venue: parse_venue(&row.venue)?,
            open: fixed_to_decimal(row.open),
            high: fixed_to_decimal(row.high),
            low: fixed_to_decimal(row.low),
            close: fixed_to_decimal(row.close),
            volume: fixed_to_decimal(row.volume),
            interval: parse_bar_interval(&row.interval)?,
            open_time: row.open_time,
            close_time: row.close_time,
        })
    }
}

pub(crate) fn parse_bar_interval(value: &str) -> Result<BarInterval, StorageError> {
    match value {
        "OneSecond" => Ok(BarInterval::OneSecond),
        "OneMinute" => Ok(BarInterval::OneMinute),
        "FiveMinutes" => Ok(BarInterval::FiveMinutes),
        "FifteenMinutes" => Ok(BarInterval::FifteenMinutes),
        "OneHour" => Ok(BarInterval::OneHour),
        "OneDay" => Ok(BarInterval::OneDay),
        other => Err(StorageError::QueryFailed {
            reason: format!("unrecognized bar interval string in ClickHouse row: {other}"),
        }),
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
    fn original_range_round_trips_through_depth_storage_row() {
        let mut d = DepthUpdate {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            bids: vec![],
            asks: vec![],
            sequence: Some(103),
            source_range: None,
            is_snapshot: false,
            timestamp: Utc::now(),
        };
        let row = DepthRow::try_from(&d).unwrap();
        assert_eq!(
            (row.source_first, row.source_last, row.source_previous_final),
            (None, None, None)
        );
        assert_eq!(DepthUpdate::try_from(row).unwrap(), d);
        for previous_final in [None, Some(98)] {
            d.source_range = Some(apexes_core::DepthSourceRange {
                first: 99,
                last: 103,
                previous_final,
            });
            let row = DepthRow::try_from(&d).unwrap();
            assert_eq!(
                (row.source_first, row.source_last, row.source_previous_final),
                (Some(99), Some(103), previous_final)
            );
            assert_eq!(DepthUpdate::try_from(row).unwrap(), d);
        }
    }

    #[test]
    fn incomplete_and_inconsistent_stored_ranges_are_rejected() {
        let mut d = DepthUpdate {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            bids: vec![],
            asks: vec![],
            sequence: Some(103),
            source_range: Some(apexes_core::DepthSourceRange {
                first: 99,
                last: 103,
                previous_final: Some(98),
            }),
            is_snapshot: false,
            timestamp: Utc::now(),
        };
        let mut row = DepthRow::try_from(&d).unwrap();
        row.source_last = None;
        assert!(matches!(
            DepthUpdate::try_from(row),
            Err(StorageError::InvalidDepthRange { .. })
        ));
        let mut row = DepthRow::try_from(&d).unwrap();
        row.sequence = Some(104);
        assert!(matches!(
            DepthUpdate::try_from(row),
            Err(StorageError::InvalidDepthRange { .. })
        ));
        d.is_snapshot = true;
        assert!(matches!(
            DepthRow::try_from(&d),
            Err(StorageError::InvalidDepthRange { .. })
        ));
    }

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

    #[test]
    fn mark_price_row_round_trips_through_try_from() {
        let update = MarkPriceUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            mark_price: dec!(65012.34),
            timestamp: Utc::now(),
        };
        let row = MarkPriceRow::try_from(&update).unwrap();
        let round_tripped = MarkPriceUpdate::try_from(row).unwrap();
        assert_eq!(update, round_tripped);
    }

    #[test]
    fn depth_update_round_trips_through_depth_row() {
        let update = DepthUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bids: vec![
                PriceLevel {
                    price: dec!(64999.5),
                    quantity: dec!(1.5),
                },
                PriceLevel {
                    price: dec!(64998.0),
                    quantity: dec!(2.0),
                },
            ],
            asks: vec![PriceLevel {
                price: dec!(65000.5),
                quantity: dec!(1.0),
            }],
            sequence: Some(12345),
            source_range: None,
            is_snapshot: true,
            timestamp: Utc::now(),
        };

        let row = DepthRow::try_from(&update).unwrap();
        let back = DepthUpdate::try_from(row).unwrap();

        assert_eq!(back.symbol, update.symbol);
        assert_eq!(back.venue, update.venue);
        assert_eq!(back.bids.len(), 2);
        assert_eq!(back.bids[0].price, dec!(64999.5));
        assert_eq!(back.bids[0].quantity, dec!(1.5));
        assert_eq!(back.bids[1].price, dec!(64998.0));
        assert_eq!(back.asks.len(), 1);
        assert_eq!(back.asks[0].price, dec!(65000.5));
        assert_eq!(back.sequence, Some(12345));
        assert!(back.is_snapshot);
    }

    #[test]
    fn depth_update_with_empty_sides_round_trips() {
        let update = DepthUpdate {
            symbol: "ETHUSDT".to_string(),
            venue: Venue::Okx,
            bids: vec![],
            asks: vec![],
            sequence: None,
            source_range: None,
            is_snapshot: false,
            timestamp: Utc::now(),
        };

        let row = DepthRow::try_from(&update).unwrap();
        let back = DepthUpdate::try_from(row).unwrap();

        assert!(back.bids.is_empty());
        assert!(back.asks.is_empty());
        assert_eq!(back.sequence, None);
        assert!(!back.is_snapshot);
    }

    #[test]
    fn bar_round_trips_through_bar_row() {
        let now = Utc::now();
        let bar = Bar {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            open: dec!(65000),
            high: dec!(65100),
            low: dec!(64900),
            close: dec!(65050),
            volume: dec!(123.456),
            interval: BarInterval::OneMinute,
            open_time: now,
            close_time: now,
        };

        let row = BarRow::try_from(&bar).unwrap();
        let back = Bar::try_from(row).unwrap();

        assert_eq!(back.symbol, bar.symbol);
        assert_eq!(back.venue, bar.venue);
        assert_eq!(back.open, bar.open);
        assert_eq!(back.high, bar.high);
        assert_eq!(back.low, bar.low);
        assert_eq!(back.close, bar.close);
        assert_eq!(back.volume, bar.volume);
        assert_eq!(back.interval, bar.interval);
    }

    #[test]
    fn parse_bar_interval_rejects_unknown_string() {
        let result = parse_bar_interval("NotARealInterval");
        assert!(result.is_err());
    }

    #[test]
    fn parse_bar_interval_handles_all_six_variants() {
        assert_eq!(
            parse_bar_interval("OneSecond").unwrap(),
            BarInterval::OneSecond
        );
        assert_eq!(
            parse_bar_interval("OneMinute").unwrap(),
            BarInterval::OneMinute
        );
        assert_eq!(
            parse_bar_interval("FiveMinutes").unwrap(),
            BarInterval::FiveMinutes
        );
        assert_eq!(
            parse_bar_interval("FifteenMinutes").unwrap(),
            BarInterval::FifteenMinutes
        );
        assert_eq!(parse_bar_interval("OneHour").unwrap(), BarInterval::OneHour);
        assert_eq!(parse_bar_interval("OneDay").unwrap(), BarInterval::OneDay);
    }
}
