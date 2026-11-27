//! F01.03: reconstruct the latest validated book for new relay clients.
//! Upstream adapters validate exchange sequence ranges; a final update ID
//! alone is not enough to infer a missing Binance delta.
use apexes_core::{DepthUpdate, MarketEvent, PriceLevel, Venue};
use rust_decimal::Decimal;
use std::collections::{BTreeMap, HashMap};

#[derive(Default)]
pub(crate) struct BookCache {
    books: HashMap<(Venue, String), Book>,
}

struct Book {
    latest: DepthUpdate,
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
}

fn apply_levels(side: &mut BTreeMap<Decimal, Decimal>, levels: &[PriceLevel]) {
    for level in levels {
        if level.quantity.is_zero() {
            side.remove(&level.price);
        } else {
            side.insert(level.price, level.quantity);
        }
    }
}

impl BookCache {
    /// False means the delta cannot be applied safely, so it must not
    /// be broadcast as if it belonged to a valid book.
    pub(crate) fn apply(&mut self, update: &DepthUpdate) -> bool {
        let key = (update.venue, update.symbol.clone());
        if update.is_snapshot {
            if update.is_invalidation() {
                self.books.remove(&key);
                return true;
            }
            self.books.insert(
                key.clone(),
                Book {
                    latest: update.clone(),
                    bids: BTreeMap::new(),
                    asks: BTreeMap::new(),
                },
            );
        }
        let Some(book) = self.books.get_mut(&key) else {
            return false;
        };
        if !update.is_snapshot
            && let (Some(previous), Some(next)) = (book.latest.sequence, update.sequence)
            && next <= previous
        {
            return false;
        }
        apply_levels(&mut book.bids, &update.bids);
        apply_levels(&mut book.asks, &update.asks);
        book.latest = update.clone();
        true
    }

    pub(crate) fn snapshots(&self) -> Vec<MarketEvent> {
        self.books
            .values()
            .map(|book| {
                let mut snapshot = book.latest.clone();
                snapshot.is_snapshot = true;
                // A synthesized snapshot is not the original delta.
                snapshot.source_range = None;
                snapshot.bids = book
                    .bids
                    .iter()
                    .rev()
                    .map(|(&price, &quantity)| PriceLevel { price, quantity })
                    .collect();
                snapshot.asks = book
                    .asks
                    .iter()
                    .map(|(&price, &quantity)| PriceLevel { price, quantity })
                    .collect();
                MarketEvent::Depth(snapshot)
            })
            .collect()
    }

    pub(crate) fn clear(&mut self) {
        self.books.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    fn depth(seq: u64, snapshot: bool, bids: Vec<PriceLevel>) -> DepthUpdate {
        DepthUpdate {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            sequence: Some(seq),
            source_range: None,
            timestamp: chrono::Utc::now(),
            is_snapshot: snapshot,
            bids,
            asks: vec![PriceLevel {
                price: dec!(102),
                quantity: dec!(1),
            }],
        }
    }
    fn level(price: Decimal, quantity: Decimal) -> PriceLevel {
        PriceLevel { price, quantity }
    }

    #[test]
    fn snapshot_plus_delta_recovers_complete_book_and_discards_old_updates() {
        let mut cache = BookCache::default();
        assert!(!cache.apply(&depth(5, false, vec![level(dec!(100), dec!(1))])));
        assert!(cache.apply(&depth(
            10,
            true,
            vec![level(dec!(100), dec!(1)), level(dec!(99), dec!(3))]
        )));
        let mut update = depth(
            15,
            false,
            vec![level(dec!(100), dec!(0)), level(dec!(101), dec!(2))],
        );
        update.source_range = Some(apexes_core::DepthSourceRange {
            first: 11,
            last: 15,
            previous_final: None,
        });
        assert!(cache.apply(&update));
        assert!(!cache.apply(&depth(12, false, vec![level(dec!(100), dec!(9))])));
        let snapshots = cache.snapshots();
        let MarketEvent::Depth(snapshot) = &snapshots[0] else {
            panic!("depth expected")
        };
        assert!(snapshot.is_snapshot);
        assert_eq!(snapshot.sequence, Some(15));
        assert_eq!(snapshot.source_range, None);
        assert_eq!(snapshot.timestamp, update.timestamp);
        assert_eq!(
            snapshot.bids,
            vec![level(dec!(101), dec!(2)), level(dec!(99), dec!(3))]
        );
        let mut invalidation = depth(16, true, vec![]);
        invalidation.asks.clear();
        invalidation.sequence = None;
        assert!(cache.apply(&invalidation));
        assert!(cache.snapshots().is_empty());
        assert!(!cache.apply(&update));
        assert!(cache.apply(&depth(20, true, vec![level(dec!(98), dec!(4))])));
        assert_eq!(cache.snapshots().len(), 1);
    }
}
