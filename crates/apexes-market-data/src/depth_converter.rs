use apexes_core::{DepthUpdate, PriceLevel, Venue};
use barter_data::books::{Level as BarterLevel, OrderBook};
use barter_data::subscription::book::OrderBookEvent;
use chrono::Utc;

fn convert_level(level: &BarterLevel) -> PriceLevel {
    PriceLevel {
        price: level.price,
        quantity: level.amount,
    }
}

fn convert_book(symbol: &str, venue: Venue, book: &OrderBook, is_snapshot: bool) -> DepthUpdate {
    DepthUpdate {
        symbol: symbol.to_string(),
        venue,
        bids: book.bids().levels().iter().map(convert_level).collect(),
        asks: book.asks().levels().iter().map(convert_level).collect(),
        sequence: Some(book.sequence()),
        is_snapshot,
        timestamp: book.time_engine().unwrap_or_else(Utc::now),
    }
}

/// Converts a `barter-data` `OrderBookEvent` (already synchronized and
/// gap-checked by `barter-data` itself) into an Apexes `DepthUpdate`.
pub fn convert(symbol: &str, venue: Venue, event: &OrderBookEvent) -> DepthUpdate {
    match event {
        OrderBookEvent::Snapshot(book) => convert_book(symbol, venue, book, true),
        OrderBookEvent::Update(book) => convert_book(symbol, venue, book, false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn sample_book(sequence: u64) -> OrderBook {
        OrderBook::new(
            sequence,
            Some(Utc::now()),
            vec![BarterLevel { price: dec!(64999.5), amount: dec!(1.5) }],
            vec![BarterLevel { price: dec!(65000.5), amount: dec!(2.0) }],
        )
    }

    #[test]
    fn snapshot_event_converts_with_is_snapshot_true() {
        let book = sample_book(42);
        let event = OrderBookEvent::Snapshot(book);

        let update = convert("BTCUSDT", Venue::Binance, &event);

        assert_eq!(update.symbol, "BTCUSDT");
        assert_eq!(update.venue, Venue::Binance);
        assert!(update.is_snapshot);
        assert_eq!(update.sequence, Some(42));
        assert_eq!(update.bids.len(), 1);
        assert_eq!(update.bids[0].price, dec!(64999.5));
        assert_eq!(update.bids[0].quantity, dec!(1.5));
        assert_eq!(update.asks.len(), 1);
        assert_eq!(update.asks[0].price, dec!(65000.5));
        assert_eq!(update.asks[0].quantity, dec!(2.0));
    }

    #[test]
    fn update_event_converts_with_is_snapshot_false() {
        let book = sample_book(43);
        let event = OrderBookEvent::Update(book);

        let update = convert("BTCUSDT", Venue::Binance, &event);

        assert!(!update.is_snapshot);
        assert_eq!(update.sequence, Some(43));
    }

    #[test]
    fn empty_book_sides_convert_to_empty_vecs() {
        let book = OrderBook::new(
            1,
            Some(Utc::now()),
            Vec::<BarterLevel>::new(),
            Vec::<BarterLevel>::new(),
        );
        let event = OrderBookEvent::Snapshot(book);

        let update = convert("ETHUSDT", Venue::Binance, &event);

        assert!(update.bids.is_empty());
        assert!(update.asks.is_empty());
    }
}
