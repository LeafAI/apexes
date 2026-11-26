use apexes_core::{DepthUpdate, PriceLevel, Side};
use barter_data::books::{Level, OrderBook};
use barter_data::subscription::book::OrderBookEvent;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;

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
    observed_at: Option<DateTime<Utc>>,
    sequence: Option<u64>,
    consumed_bids: HashMap<Decimal, Decimal>,
    consumed_asks: HashMap<Decimal, Decimal>,
}

impl ReplayOrderBook {
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies a `DepthUpdate` to the local book: a full replace if
    /// `update.is_snapshot`, or an upsert-merge of delta levels otherwise.
    pub fn apply(&mut self, update: &DepthUpdate) {
        if self.observed_at.is_some_and(|at| {
            update.timestamp < at
                || (update.timestamp == at
                    && self.sequence.is_some()
                    && self.sequence == update.sequence)
        }) || (!update.is_snapshot
            && self
                .sequence
                .zip(update.sequence)
                .is_some_and(|(old, new)| new <= old))
        {
            return;
        }
        if update.is_snapshot {
            self.consumed_bids.clear();
            self.consumed_asks.clear();
        } else {
            for level in &update.bids {
                self.consumed_bids.remove(&level.price);
            }
            for level in &update.asks {
                self.consumed_asks.remove(&level.price);
            }
        }
        self.observed_at = Some(update.timestamp);
        self.sequence = update.sequence;
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
            .filter_map(|l| available(l, &self.consumed_bids))
            .collect()
    }

    /// Current asks, sorted best (lowest price) first.
    pub fn asks(&self) -> Vec<PriceLevel> {
        self.inner
            .asks()
            .levels()
            .iter()
            .filter_map(|l| available(l, &self.consumed_asks))
            .collect()
    }

    pub fn is_causal_at(&self, now: DateTime<Utc>) -> bool {
        self.observed_at.is_some_and(|at| at <= now)
    }

    /// Consume from the opposite side without altering historical depth.
    pub fn consume(&mut self, side: Side, price: Decimal, quantity: Decimal) {
        let consumed = match side {
            Side::Buy => &mut self.consumed_asks,
            Side::Sell => &mut self.consumed_bids,
        };
        *consumed.entry(price).or_default() += quantity;
    }
}

fn price_level_to_barter_level(level: &PriceLevel) -> Level {
    Level::new(level.price, level.quantity)
}

fn available(level: &Level, consumed: &HashMap<Decimal, Decimal>) -> Option<PriceLevel> {
    let quantity = level.amount - consumed.get(&level.price).copied().unwrap_or_default();
    (quantity > Decimal::ZERO && level.price > Decimal::ZERO).then_some(PriceLevel {
        price: level.price,
        quantity,
    })
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
