use crate::instrument::Venue;
use crate::order::{MarginMode, OrderId, Side};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FillId(pub Uuid);

/// An execution (fill) of all or part of an order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fill {
    pub id: FillId,
    pub order_id: OrderId,
    pub symbol: String,
    pub venue: Venue,
    pub side: Side,
    pub price: Decimal,
    pub quantity: Decimal,
    pub fee: Decimal,
    pub fee_currency: Option<String>,
    pub filled_at: DateTime<Utc>,
    /// `None`: spot fill (today's behavior, unchanged). `Some(leverage)`:
    /// this fill opens/adds to or reduces/closes a leveraged perpetual
    /// position at this leverage -- copied from the originating
    /// `Order.leverage` by whatever constructs this `Fill`
    /// (`MatchingEngine::submit_market_order` et al., in backtest/paper;
    /// `apexes-oms`'s exchange-fill ingestion, in live trading).
    pub leverage: Option<Decimal>,
    /// Only meaningful when `leverage.is_some()`.
    pub margin_mode: Option<MarginMode>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn fill_serde_round_trip() {
        let fill = Fill {
            id: FillId(Uuid::new_v4()),
            order_id: OrderId(Uuid::new_v4()),
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            price: dec!(65000),
            quantity: dec!(0.5),
            fee: dec!(3.25),
            fee_currency: Some("USDT".to_string()),
            filled_at: Utc::now(),
            leverage: None,
            margin_mode: None,
        };
        let json = serde_json::to_string(&fill).unwrap();
        let round_tripped: Fill = serde_json::from_str(&json).unwrap();
        assert_eq!(fill, round_tripped);
    }
}
