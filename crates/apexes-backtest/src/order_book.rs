use crate::{DepthSequence, SimulatedDepthPolicy};
use apexes_core::{DepthUpdate, PriceLevel, Side};
use barter_data::books::{Level, OrderBook};
use barter_data::subscription::book::OrderBookEvent;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};

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
    strict_watermark: Option<DateTime<Utc>>,
    strict_ready: bool,
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

    /// Strict replay source validation. Returns true when prior queue
    /// state must be discarded; only snapshots establish a new chain.
    pub(crate) fn apply_strict(
        &mut self,
        update: &DepthUpdate,
        policy: SimulatedDepthPolicy,
    ) -> bool {
        if self
            .strict_watermark
            .is_some_and(|at| update.timestamp < at)
        {
            return false;
        }
        let stale = self.strict_ready && !self.fresh_at(update.timestamp, policy);
        if stale {
            self.invalidate(update.timestamp);
        }
        let explicit_invalidation = update.is_invalidation();
        if explicit_invalidation {
            self.invalidate(update.timestamp);
            return true;
        }
        if self.strict_ready
            && ((self.sequence.is_some() && self.sequence == update.sequence)
                || (update.is_snapshot
                    && self.sequence.is_none()
                    && update.sequence.is_none()
                    && self.observed_at == Some(update.timestamp)))
        {
            return stale;
        }
        if !update.is_snapshot {
            if !self.strict_ready || policy.sequence == DepthSequence::SnapshotOnly {
                self.invalidate(update.timestamp);
                return true;
            }
            match (self.sequence, update.sequence) {
                (Some(old), Some(new)) if new < old => return false,
                (Some(old), Some(new))
                    if old.checked_add(1) == Some(new)
                        || (policy.sequence == DepthSequence::VerifiedRanges && new > old) => {}
                _ => {
                    self.invalidate(update.timestamp);
                    return true;
                }
            }
        }
        if !valid_levels(&update.bids)
            || !valid_levels(&update.asks)
            || (policy.sequence != DepthSequence::SnapshotOnly && update.sequence.is_none())
        {
            self.invalidate(update.timestamp);
            return true;
        }
        self.apply(update);
        self.strict_watermark = Some(update.timestamp);
        self.strict_ready = true;
        // Inspect raw depth, not the consumption overlay.
        let bid = self
            .inner
            .bids()
            .levels()
            .iter()
            .filter(|l| l.amount > Decimal::ZERO)
            .map(|l| l.price)
            .max();
        let ask = self
            .inner
            .asks()
            .levels()
            .iter()
            .filter(|l| l.amount > Decimal::ZERO)
            .map(|l| l.price)
            .min();
        if bid.zip(ask).is_some_and(|(b, a)| b >= a) {
            self.invalidate(update.timestamp);
            return true;
        }
        stale
    }

    /// Once the simulation clock passes the freshness deadline, late
    /// deltas cannot revive the old chain. A new snapshot is required.
    pub(crate) fn expire_at(&mut self, now: DateTime<Utc>, policy: SimulatedDepthPolicy) {
        if self.strict_ready
            && self
                .observed_at
                .is_some_and(|at| now - at > policy.max_age())
        {
            self.invalidate(now);
        }
    }

    fn invalidate(&mut self, at: DateTime<Utc>) {
        self.inner = OrderBook::default();
        self.observed_at = None;
        self.sequence = None;
        self.consumed_bids.clear();
        self.consumed_asks.clear();
        self.strict_ready = false;
        self.strict_watermark = Some(at);
    }

    pub(crate) fn fresh_at(&self, now: DateTime<Utc>, policy: SimulatedDepthPolicy) -> bool {
        self.strict_ready
            && self
                .observed_at
                .is_some_and(|at| at <= now && now - at <= policy.max_age())
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

fn valid_levels(levels: &[PriceLevel]) -> bool {
    let mut prices = HashSet::new();
    levels
        .iter()
        .all(|l| l.price > Decimal::ZERO && l.quantity >= Decimal::ZERO && prices.insert(l.price))
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
            source_range: None,
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
            source_range: None,
            is_snapshot: false,
            timestamp: Utc::now(),
        }
    }

    fn strict_policy() -> SimulatedDepthPolicy {
        SimulatedDepthPolicy {
            max_age_ms: 1000,
            sequence: DepthSequence::Contiguous,
        }
    }

    #[test]
    fn strict_gap_requires_snapshot_and_delayed_updates_cannot_restore_depth() {
        let mut book = ReplayOrderBook::new();
        let policy = strict_policy();
        let mut snap = snapshot(
            vec![],
            vec![PriceLevel {
                price: dec!(101),
                quantity: dec!(3),
            }],
        );
        let at = snap.timestamp;
        let mut delta = diff(2, vec![], snap.asks.clone());
        delta.timestamp = at;
        assert!(book.apply_strict(&delta, policy));
        assert!(!book.fresh_at(at, policy));
        assert!(!book.apply_strict(&snap, policy));
        assert!(book.fresh_at(at, policy));
        book.consume(Side::Buy, dec!(101), dec!(2));
        delta.timestamp = at + chrono::Duration::milliseconds(100);
        delta.sequence = Some(1);
        assert!(!book.apply_strict(&delta, policy));
        assert_eq!(book.asks()[0].quantity, dec!(1));
        delta.sequence = Some(3);
        assert!(book.apply_strict(&delta, policy));
        assert!(!book.fresh_at(delta.timestamp, policy));
        assert!(book.asks().is_empty());
        delta.sequence = Some(2);
        assert!(book.apply_strict(&delta, policy));
        assert!(!book.fresh_at(delta.timestamp, policy));
        assert!(!book.apply_strict(&snap, policy));
        assert!(!book.fresh_at(delta.timestamp, policy));
        snap.timestamp = delta.timestamp + chrono::Duration::milliseconds(1);
        snap.sequence = Some(20);
        assert!(!book.apply_strict(&snap, policy));
        assert!(book.fresh_at(snap.timestamp, policy));
        assert_eq!(book.asks()[0].quantity, dec!(3));
        delta.sequence = Some(21);
        delta.timestamp = snap.timestamp + chrono::Duration::milliseconds(1001);
        assert!(book.apply_strict(&delta, policy));
        assert!(!book.fresh_at(delta.timestamp, policy));
    }

    #[test]
    fn strict_snapshot_only_rejects_deltas_and_explicit_invalidation() {
        let mut book = ReplayOrderBook::new();
        let policy = SimulatedDepthPolicy {
            sequence: DepthSequence::SnapshotOnly,
            ..strict_policy()
        };
        let mut snap = snapshot(
            vec![],
            vec![PriceLevel {
                price: dec!(101),
                quantity: dec!(3),
            }],
        );
        snap.sequence = None;
        let at = snap.timestamp;
        book.apply_strict(&snap, policy);
        book.consume(Side::Buy, dec!(101), dec!(1));
        book.apply_strict(&snap, policy);
        assert_eq!(book.asks()[0].quantity, dec!(2));
        let mut delta = diff(2, vec![], snap.asks.clone());
        delta.timestamp = at + chrono::Duration::milliseconds(1);
        assert!(book.apply_strict(&delta, policy));
        assert!(!book.fresh_at(delta.timestamp, policy));
        snap.timestamp = delta.timestamp;
        book.apply_strict(&snap, policy);
        assert!(book.fresh_at(snap.timestamp, policy));
        snap.asks.clear();
        assert!(book.apply_strict(&snap, policy));
        assert!(!book.fresh_at(snap.timestamp, policy));
    }

    #[test]
    fn strict_book_rejects_crossed_and_malformed_depth() {
        let mut book = ReplayOrderBook::new();
        let policy = strict_policy();
        let mut snap = snapshot(
            vec![PriceLevel {
                price: dec!(100),
                quantity: dec!(1),
            }],
            vec![PriceLevel {
                price: dec!(101),
                quantity: dec!(2),
            }],
        );
        book.apply_strict(&snap, policy);
        let mut delta = diff(
            2,
            vec![PriceLevel {
                price: dec!(102),
                quantity: dec!(1),
            }],
            vec![],
        );
        delta.timestamp = snap.timestamp + chrono::Duration::milliseconds(1);
        assert!(book.apply_strict(&delta, policy));
        assert!(!book.fresh_at(delta.timestamp, policy));
        snap.timestamp = delta.timestamp;
        snap.bids[0].quantity = dec!(-1);
        assert!(book.apply_strict(&snap, policy));
        assert!(!book.fresh_at(snap.timestamp, policy));
        snap.bids[0].quantity = dec!(1);
        snap.bids.push(snap.bids[0]);
        assert!(book.apply_strict(&snap, policy));
        snap.bids.pop();
        book.apply_strict(&snap, policy);
        assert!(book.fresh_at(snap.timestamp, policy));
        book.expire_at(
            snap.timestamp + chrono::Duration::milliseconds(1001),
            policy,
        );
        assert!(!book.fresh_at(
            snap.timestamp + chrono::Duration::milliseconds(1001),
            policy
        ));
        assert!(!book.apply_strict(&snap, policy));
        assert!(book.asks().is_empty());
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
