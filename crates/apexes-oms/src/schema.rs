//! PostgreSQL table schemas for `apexes-oms`. Mirrors
//! `apexes_storage::schema`'s pattern (DDL constants + Row structs +
//! bidirectional `TryFrom` conversions), adapted for `sqlx`/PostgreSQL
//! instead of the `clickhouse` client.

use apexes_core::{AssetClass, Fill, FillId, Order, OrderId, OrderStatus, OrderType, Side, Venue};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::error::OmsError;

pub const ORDERS_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS orders
(
    id                  UUID PRIMARY KEY,
    exchange_order_id   TEXT,
    symbol              TEXT NOT NULL,
    venue               TEXT NOT NULL,
    asset_class         TEXT NOT NULL,
    side                TEXT NOT NULL,
    order_type          TEXT NOT NULL,
    quantity            NUMERIC(38, 10) NOT NULL,
    price               NUMERIC(38, 10),
    stop_price          NUMERIC(38, 10),
    filled_quantity     NUMERIC(38, 10) NOT NULL,
    status              TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL
)
";

pub const ORDERS_EXCHANGE_ID_INDEX_DDL: &str = "
CREATE INDEX IF NOT EXISTS idx_orders_exchange_order_id
    ON orders (exchange_order_id)
    WHERE exchange_order_id IS NOT NULL
";

pub const FILLS_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS fills
(
    id                  UUID PRIMARY KEY,
    order_id            UUID NOT NULL REFERENCES orders(id),
    exchange_trade_id   TEXT NOT NULL,
    symbol              TEXT NOT NULL,
    venue               TEXT NOT NULL,
    side                TEXT NOT NULL,
    price               NUMERIC(38, 10) NOT NULL,
    quantity            NUMERIC(38, 10) NOT NULL,
    fee                 NUMERIC(38, 10) NOT NULL,
    fee_currency        TEXT,
    filled_at           TIMESTAMPTZ NOT NULL,
    UNIQUE (order_id, exchange_trade_id)
)
";

pub const FILLS_ORDER_ID_INDEX_DDL: &str = "
CREATE INDEX IF NOT EXISTS idx_fills_order_id ON fills (order_id)
";

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct OrderRow {
    pub id: Uuid,
    pub exchange_order_id: Option<String>,
    pub symbol: String,
    pub venue: String,
    pub asset_class: String,
    pub side: String,
    pub order_type: String,
    pub quantity: Decimal,
    pub price: Option<Decimal>,
    pub stop_price: Option<Decimal>,
    pub filled_quantity: Decimal,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<&Order> for OrderRow {
    fn from(order: &Order) -> Self {
        OrderRow {
            id: order.id.0,
            exchange_order_id: order.exchange_order_id.clone(),
            symbol: order.symbol.clone(),
            venue: format!("{:?}", order.venue),
            asset_class: format!("{:?}", order.asset_class),
            side: format!("{:?}", order.side),
            order_type: format!("{:?}", order.order_type),
            quantity: order.quantity,
            price: order.price,
            stop_price: order.stop_price,
            filled_quantity: order.filled_quantity,
            status: format!("{:?}", order.status),
            created_at: order.created_at,
            updated_at: order.updated_at,
        }
    }
}

impl TryFrom<OrderRow> for Order {
    type Error = OmsError;

    fn try_from(row: OrderRow) -> Result<Self, OmsError> {
        Ok(Order {
            id: OrderId(row.id),
            exchange_order_id: row.exchange_order_id,
            symbol: row.symbol,
            venue: parse_venue(&row.venue)?,
            asset_class: parse_asset_class(&row.asset_class)?,
            side: parse_side(&row.side)?,
            order_type: parse_order_type(&row.order_type)?,
            quantity: row.quantity,
            price: row.price,
            stop_price: row.stop_price,
            // The `orders` table has no leverage/margin_mode columns yet
            // (no task in the margin-monitoring plan adds them), so
            // orders rehydrated from Postgres always read back as spot
            // (`None`). Persisting these would require a schema
            // migration; flagged for a future task if leveraged orders
            // must survive a live-trading restart via OMS recovery.
            leverage: None,
            margin_mode: None,
            filled_quantity: row.filled_quantity,
            status: parse_order_status(&row.status)?,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct FillRow {
    pub id: Uuid,
    pub order_id: Uuid,
    pub exchange_trade_id: String,
    pub symbol: String,
    pub venue: String,
    pub side: String,
    pub price: Decimal,
    pub quantity: Decimal,
    pub fee: Decimal,
    pub fee_currency: Option<String>,
    pub filled_at: DateTime<Utc>,
}

impl FillRow {
    pub fn from_fill(fill: &Fill, exchange_trade_id: &str) -> Self {
        FillRow {
            id: fill.id.0,
            order_id: fill.order_id.0,
            exchange_trade_id: exchange_trade_id.to_string(),
            symbol: fill.symbol.clone(),
            venue: format!("{:?}", fill.venue),
            side: format!("{:?}", fill.side),
            price: fill.price,
            quantity: fill.quantity,
            fee: fill.fee,
            fee_currency: fill.fee_currency.clone(),
            filled_at: fill.filled_at,
        }
    }
}

impl TryFrom<FillRow> for Fill {
    type Error = OmsError;

    fn try_from(row: FillRow) -> Result<Self, OmsError> {
        Ok(Fill {
            id: FillId(row.id),
            order_id: OrderId(row.order_id),
            symbol: row.symbol,
            venue: parse_venue(&row.venue)?,
            side: parse_side(&row.side)?,
            price: row.price,
            quantity: row.quantity,
            fee: row.fee,
            fee_currency: row.fee_currency,
            filled_at: row.filled_at,
            // The `fills` table has no leverage/margin_mode columns (Task
            // 8b adds them to `orders` but NOT to `fills`: a historical
            // fill's leverage tag is only needed for audit/reporting, out
            // of this plan's scope), so fills rehydrated from Postgres
            // always read back as spot (`None`).
            leverage: None,
            margin_mode: None,
        })
    }
}

fn parse_venue(value: &str) -> Result<Venue, OmsError> {
    match value {
        "Sse" => Ok(Venue::Sse),
        "Szse" => Ok(Venue::Szse),
        "Hkex" => Ok(Venue::Hkex),
        "Nasdaq" => Ok(Venue::Nasdaq),
        "Nyse" => Ok(Venue::Nyse),
        "Binance" => Ok(Venue::Binance),
        "Okx" => Ok(Venue::Okx),
        "Coinbase" => Ok(Venue::Coinbase),
        other => Err(OmsError::InvalidRow {
            reason: format!("unknown venue: {other}"),
        }),
    }
}

fn parse_asset_class(value: &str) -> Result<AssetClass, OmsError> {
    match value {
        "Spot" => Ok(AssetClass::Spot),
        "Perpetual" => Ok(AssetClass::Perpetual),
        "Future" => Ok(AssetClass::Future),
        "Option" => Ok(AssetClass::Option),
        "Equity" => Ok(AssetClass::Equity),
        other => Err(OmsError::InvalidRow {
            reason: format!("unknown asset_class: {other}"),
        }),
    }
}

fn parse_side(value: &str) -> Result<Side, OmsError> {
    match value {
        "Buy" => Ok(Side::Buy),
        "Sell" => Ok(Side::Sell),
        other => Err(OmsError::InvalidRow {
            reason: format!("unknown side: {other}"),
        }),
    }
}

fn parse_order_type(value: &str) -> Result<OrderType, OmsError> {
    match value {
        "Market" => Ok(OrderType::Market),
        "Limit" => Ok(OrderType::Limit),
        "Stop" => Ok(OrderType::Stop),
        "StopLimit" => Ok(OrderType::StopLimit),
        other => Err(OmsError::InvalidRow {
            reason: format!("unknown order_type: {other}"),
        }),
    }
}

fn parse_order_status(value: &str) -> Result<OrderStatus, OmsError> {
    match value {
        "New" => Ok(OrderStatus::New),
        "PartiallyFilled" => Ok(OrderStatus::PartiallyFilled),
        "Filled" => Ok(OrderStatus::Filled),
        "Cancelled" => Ok(OrderStatus::Cancelled),
        "Rejected" => Ok(OrderStatus::Rejected),
        other => Err(OmsError::InvalidRow {
            reason: format!("unknown order_status: {other}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn sample_order() -> Order {
        Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: Some("EX123".to_string()),
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: dec!(1.5),
            price: Some(dec!(65000)),
            stop_price: None,
            leverage: None,
            margin_mode: None,
            filled_quantity: dec!(0.5),
            status: OrderStatus::PartiallyFilled,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn order_round_trips_through_order_row() {
        let order = sample_order();
        let row = OrderRow::from(&order);
        let back = Order::try_from(row).unwrap();
        assert_eq!(back, order);
    }

    #[test]
    fn order_with_none_exchange_order_id_round_trips() {
        let mut order = sample_order();
        order.exchange_order_id = None;
        let row = OrderRow::from(&order);
        let back = Order::try_from(row).unwrap();
        assert_eq!(back.exchange_order_id, None);
    }

    #[test]
    fn order_row_with_invalid_status_fails_to_convert() {
        let mut row = OrderRow::from(&sample_order());
        row.status = "NotARealStatus".to_string();
        let result = Order::try_from(row);
        assert!(matches!(result, Err(OmsError::InvalidRow { .. })));
    }

    #[test]
    fn order_row_with_invalid_venue_fails_to_convert() {
        let mut row = OrderRow::from(&sample_order());
        row.venue = "NotARealVenue".to_string();
        let result = Order::try_from(row);
        assert!(matches!(result, Err(OmsError::InvalidRow { .. })));
    }

    fn sample_fill() -> Fill {
        Fill {
            id: FillId(Uuid::new_v4()),
            order_id: OrderId(Uuid::new_v4()),
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            price: dec!(65000),
            quantity: dec!(0.5),
            fee: dec!(1.2),
            fee_currency: Some("USDT".to_string()),
            filled_at: Utc::now(),
            leverage: None,
            margin_mode: None,
        }
    }

    #[test]
    fn fill_round_trips_through_fill_row() {
        let fill = sample_fill();
        let row = FillRow::from_fill(&fill, "T1");
        let back = Fill::try_from(row).unwrap();
        assert_eq!(back, fill);
    }

    #[test]
    fn fill_with_none_fee_currency_round_trips() {
        let mut fill = sample_fill();
        fill.fee_currency = None;
        let row = FillRow::from_fill(&fill, "T1");
        let back = Fill::try_from(row).unwrap();
        assert_eq!(back.fee_currency, None);
    }

    #[test]
    fn from_fill_stores_the_given_exchange_trade_id() {
        let fill = sample_fill();
        let row = FillRow::from_fill(&fill, "T-UNIQUE-42");
        assert_eq!(row.exchange_trade_id, "T-UNIQUE-42");
    }
}
