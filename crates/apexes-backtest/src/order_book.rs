use apexes_core::{DepthUpdate, PriceLevel};
use barter_data::books::{Level, OrderBook};
use barter_data::subscription::book::OrderBookEvent;

/// A locally-reconstructed order book for backtest replay, built by
/// feeding it a chronological sequence of `apexes_core::DepthUpdate`s.
///
/// Wraps `barter_data::books::OrderBook` (the same type used live by
/// `apexes-market-data`) to reuse its proven snapshot/diff merge
/// semantics rather than reimplementing them. `DepthUpdate` events with
/// `is_snapshot: false` carry delta-only levels (a level with `quantity:
/// Decimal::ZERO` means "remove this price level"), matching
/// `OrderBookSide::upsert`'s exact semantics.
#[derive(Debug, Default)]
pub struct ReplayOrderBook {
    inner: OrderBook,
}

impl ReplayOrderBook {
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies a `DepthUpdate` to the local book: a full replace if
    /// `update.is_snapshot`, or an upsert-merge of delta levels otherwise.
    pub fn apply(&mut self, update: &DepthUpdate) {
        let sequence = update.sequence.unwrap_or(0);
        let event = if update.is_snapshot {
            OrderBookEvent::Snapshot(OrderBook::new(
                sequence,
                Some(update.timestamp),
                update.bids.iter().map(price_level_to_barter_level),
                update.asks.iter().map(price_level_to_barter_level),
            ))
        } else {
            OrderBookEvent::Update(OrderBook::new(
                sequence,
                Some(update.timestamp),
                update.bids.iter().map(price_level_to_barter_level),
                update.asks.iter().map(price_level_to_barter_level),
            ))
        };
        self.inner.update(&event);
    }

    /// Current bids, sorted best (highest price) first.
    pub fn bids(&self) -> Vec<PriceLevel> {
        self.inner
            .bids()
            .levels()
            .iter()
            .map(barter_level_to_price_level)
            .collect()
    }

    /// Current asks, sorted best (lowest price) first.
    pub fn asks(&self) -> Vec<PriceLevel> {
        self.inner
            .asks()
            .levels()
            .iter()
            .map(barter_level_to_price_level)
            .collect()
    }
}

fn price_level_to_barter_level(level: &PriceLevel) -> Level {
    Level::new(level.price, level.quantity)
}

fn barter_level_to_price_level(level: &Level) -> PriceLevel {
    PriceLevel {
        price: level.price,
        quantity: level.amount,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::Venue;
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn snapshot(bids: Vec<PriceLevel>, asks: Vec<PriceLevel>) -> DepthUpdate {
        DepthUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bids,
            asks,
            sequence: Some(1),
            is_snapshot: true,
            timestamp: Utc::now(),
        }
    }

    fn diff(sequence: u64, bids: Vec<PriceLevel>, asks: Vec<PriceLevel>) -> DepthUpdate {
        DepthUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bids,
            asks,
            sequence: Some(sequence),
            is_snapshot: false,
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn snapshot_replaces_book_state() {
        let mut book = ReplayOrderBook::new();
        book.apply(&snapshot(
            vec![PriceLevel {
                price: dec!(100),
                quantity: dec!(1),
            }],
            vec![PriceLevel {
                price: dec!(101),
                quantity: dec!(2),
            }],
        ));

        assert_eq!(book.bids().len(), 1);
        assert_eq!(book.bids()[0].price, dec!(100));
        assert_eq!(book.asks()[0].price, dec!(101));
    }

    #[test]
    fn diff_upserts_a_new_level() {
        let mut book = ReplayOrderBook::new();
        book.apply(&snapshot(
            vec![PriceLevel {
                price: dec!(100),
                quantity: dec!(1),
            }],
            vec![PriceLevel {
                price: dec!(101),
                quantity: dec!(2),
            }],
        ));
        book.apply(&diff(
            2,
            vec![PriceLevel {
                price: dec!(99),
                quantity: dec!(3),
            }],
            vec![],
        ));

        let bids = book.bids();
        assert_eq!(bids.len(), 2);
        // Best bid (highest price) first.
        assert_eq!(bids[0].price, dec!(100));
        assert_eq!(bids[1].price, dec!(99));
    }

    #[test]
    fn diff_with_zero_quantity_removes_the_level() {
        let mut book = ReplayOrderBook::new();
        book.apply(&snapshot(
            vec![PriceLevel {
                price: dec!(100),
                quantity: dec!(1),
            }],
            vec![PriceLevel {
                price: dec!(101),
                quantity: dec!(2),
            }],
        ));
        book.apply(&diff(
            2,
            vec![PriceLevel {
                price: dec!(100),
                quantity: dec!(0),
            }],
            vec![],
        ));

        assert!(book.bids().is_empty());
    }

    #[test]
    fn diff_replaces_existing_level_quantity() {
        let mut book = ReplayOrderBook::new();
        book.apply(&snapshot(
            vec![PriceLevel {
                price: dec!(100),
                quantity: dec!(1),
            }],
            vec![],
        ));
        book.apply(&diff(
            2,
            vec![PriceLevel {
                price: dec!(100),
                quantity: dec!(5),
            }],
            vec![],
        ));

        let bids = book.bids();
        assert_eq!(bids.len(), 1);
        assert_eq!(bids[0].quantity, dec!(5));
    }

    #[test]
    fn bids_and_asks_are_sorted_best_first() {
        let mut book = ReplayOrderBook::new();
        book.apply(&snapshot(
            vec![
                PriceLevel {
                    price: dec!(99),
                    quantity: dec!(1),
                },
                PriceLevel {
                    price: dec!(100),
                    quantity: dec!(1),
                },
            ],
            vec![
                PriceLevel {
                    price: dec!(102),
                    quantity: dec!(1),
                },
                PriceLevel {
                    price: dec!(101),
                    quantity: dec!(1),
                },
            ],
        ));

        let bids = book.bids();
        let asks = book.asks();
        assert_eq!(bids[0].price, dec!(100)); // highest bid first
        assert_eq!(bids[1].price, dec!(99));
        assert_eq!(asks[0].price, dec!(101)); // lowest ask first
        assert_eq!(asks[1].price, dec!(102));
    }
}
