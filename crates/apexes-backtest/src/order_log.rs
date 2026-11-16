//! A bounded history of the orders a session has sent, for the operator console.
//!
//! The order manager only holds orders that are still working, and a market
//! order is working for well under a second, so the console's "Open orders"
//! list is almost always empty and nothing shows what the strategy has done.
//! The trading loop sees every submission, rejection and fill; this keeps the
//! most recent of them in a fixed amount of memory.
//!
//! Pure data and rules: no I/O, so it is tested without a loop.

use std::collections::{HashMap, HashSet, VecDeque};

use apexes_core::{Fill, Order};
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Orders kept (and sent in each snapshot). The default daily limit is 50
/// orders, so this covers a day of a strategy hitting its cap.
pub const ORDER_LOG_CAP: usize = 100;

/// An order that has left the working set without filling must have been
/// absent from the open list this long before it is called closed. The live
/// open-orders view can lag by an exchange round trip, so a brand-new order
/// must not be declared gone because the view has not caught up.
const CLOSE_GRACE_SECS: i64 = 10;

pub const WORKING: &str = "Working";
pub const PARTIAL: &str = "Partially filled";
pub const FILLED: &str = "Filled";
pub const REJECTED: &str = "Rejected";
pub const CLOSED: &str = "Closed";

/// One order as the operator sees it. Plain strings and decimals, like
/// [`crate::OpenOrderView`], so a console needs no knowledge of core types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderRecord {
    pub id: String,
    /// When it was sent (or refused).
    pub at: DateTime<Utc>,
    pub symbol: String,
    /// `"Buy"` / `"Sell"`.
    pub side: String,
    /// `"Market"`, `"Limit"`, ...
    pub order_type: String,
    pub quantity: Decimal,
    /// The limit price, when it has one.
    pub limit_price: Option<Decimal>,
    pub filled_quantity: Decimal,
    /// Volume-weighted price of what has filled so far.
    pub avg_fill_price: Option<Decimal>,
    /// One of [`WORKING`], [`PARTIAL`], [`FILLED`], [`REJECTED`], [`CLOSED`].
    pub status: String,
    /// Why it was rejected or closed, when that is known.
    pub detail: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OrderLog {
    cap: usize,
    /// Oldest first.
    records: VecDeque<OrderRecord>,
    /// Sum of `price x quantity` filled per order id, for the average price.
    notional: HashMap<String, Decimal>,
}

impl Default for OrderLog {
    fn default() -> Self {
        Self::new(ORDER_LOG_CAP)
    }
}

impl OrderLog {
    pub fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            records: VecDeque::new(),
            notional: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// An order the exchange (or simulator) accepted. Recording the same id
    /// twice keeps the first.
    ///
    /// The order's own `filled_quantity` is deliberately ignored: the
    /// simulator returns an order that already carries its immediate fills,
    /// and the caller then passes those same fills to [`Self::filled`], so
    /// reading both would count them twice. Fills are the only source.
    pub fn submitted(&mut self, order: &Order) {
        let id = order.id.0.to_string();
        if self.records.iter().any(|r| r.id == id) {
            return;
        }
        self.push(OrderRecord {
            id,
            at: order.created_at,
            symbol: order.symbol.clone(),
            side: format!("{:?}", order.side),
            order_type: format!("{:?}", order.order_type),
            quantity: order.quantity,
            limit_price: order.price,
            filled_quantity: Decimal::ZERO,
            avg_fill_price: None,
            status: WORKING.to_string(),
            detail: None,
        });
    }

    /// An order that was refused before it reached the exchange (a risk limit)
    /// or by it. It never worked, so it is final at once.
    #[allow(clippy::too_many_arguments)]
    pub fn rejected(
        &mut self,
        id: String,
        at: DateTime<Utc>,
        symbol: &str,
        side: &str,
        order_type: &str,
        quantity: Decimal,
        limit_price: Option<Decimal>,
        reason: String,
    ) {
        self.push(OrderRecord {
            id,
            at,
            symbol: symbol.to_string(),
            side: side.to_string(),
            order_type: order_type.to_string(),
            quantity,
            limit_price,
            filled_quantity: Decimal::ZERO,
            avg_fill_price: None,
            status: REJECTED.to_string(),
            detail: Some(reason),
        });
    }

    /// Folds executions into their orders. A fill for an order this log does
    /// not know (a child of an execution algo, or one that predates the
    /// session) is ignored. A fill after an order was called closed reopens
    /// the truth: the exchange's report wins.
    pub fn filled(&mut self, fills: &[Fill]) {
        for fill in fills {
            let id = fill.order_id.0.to_string();
            let Some(record) = self.records.iter_mut().find(|r| r.id == id) else {
                continue;
            };
            let notional = self.notional.entry(id).or_insert(Decimal::ZERO);
            *notional += fill.price * fill.quantity;
            record.filled_quantity += fill.quantity;
            if record.filled_quantity > Decimal::ZERO {
                record.avg_fill_price = Some(*notional / record.filled_quantity);
            }
            record.detail = None;
            record.status = if record.filled_quantity >= record.quantity {
                FILLED.to_string()
            } else {
                PARTIAL.to_string()
            };
        }
    }

    /// Marks orders that are neither filled nor in `open_ids` as closed (they
    /// were cancelled, expired or refused after being accepted), once they
    /// have been absent long enough to be sure.
    pub fn sync_open(&mut self, now: DateTime<Utc>, open_ids: &HashSet<String>) {
        for record in &mut self.records {
            let still_working = record.status == WORKING || record.status == PARTIAL;
            if !still_working
                || open_ids.contains(&record.id)
                || now - record.at < Duration::seconds(CLOSE_GRACE_SECS)
            {
                continue;
            }
            record.detail = Some(if record.filled_quantity > Decimal::ZERO {
                format!(
                    "filled {} of {}, then no longer working",
                    record.filled_quantity, record.quantity
                )
            } else {
                "no longer working: cancelled or refused by the exchange".to_string()
            });
            record.status = CLOSED.to_string();
        }
    }

    /// At most `max` orders, newest first.
    pub fn view(&self, max: usize) -> Vec<OrderRecord> {
        self.records.iter().rev().take(max).cloned().collect()
    }

    fn push(&mut self, record: OrderRecord) {
        self.records.push_back(record);
        while self.records.len() > self.cap {
            if let Some(dropped) = self.records.pop_front() {
                self.notional.remove(&dropped.id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, FillId, OrderId, OrderStatus, OrderType, Side, Venue};
    use chrono::TimeZone;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn t(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_800_000_000 + secs, 0).unwrap()
    }

    fn order(side: Side, quantity: Decimal, secs: i64) -> Order {
        Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: None,
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side,
            order_type: OrderType::Market,
            quantity,
            price: None,
            stop_price: None,
            leverage: None,
            margin_mode: None,
            filled_quantity: Decimal::ZERO,
            status: OrderStatus::New,
            created_at: t(secs),
            updated_at: t(secs),
        }
    }

    fn fill_of(order: &Order, price: Decimal, quantity: Decimal) -> Fill {
        Fill {
            id: FillId(Uuid::new_v4()),
            order_id: order.id,
            symbol: order.symbol.clone(),
            venue: order.venue,
            side: order.side,
            price,
            quantity,
            fee: Decimal::ZERO,
            fee_currency: None,
            filled_at: order.created_at,
            leverage: None,
            margin_mode: None,
        }
    }

    #[test]
    fn a_new_order_is_working_and_a_full_fill_completes_it_with_its_price() {
        let mut log = OrderLog::default();
        let o = order(Side::Buy, dec!(0.0007), 0);
        log.submitted(&o);
        assert_eq!(log.view(10)[0].status, WORKING);
        log.filled(&[fill_of(&o, dec!(64657.62), dec!(0.0007))]);
        let r = &log.view(10)[0];
        assert_eq!(r.status, FILLED);
        assert_eq!(r.filled_quantity, dec!(0.0007));
        assert_eq!(r.avg_fill_price, Some(dec!(64657.62)));
        assert_eq!((r.side.as_str(), r.order_type.as_str()), ("Buy", "Market"));
    }

    /// The simulator hands back an order that already has its fills in
    /// `filled_quantity` and then the same fills separately: counting both
    /// showed 0.02 filled on a 0.01 order.
    #[test]
    fn an_order_that_arrives_already_filled_is_not_counted_twice() {
        let mut log = OrderLog::default();
        let mut o = order(Side::Buy, dec!(0.01), 0);
        o.filled_quantity = dec!(0.01);
        o.status = OrderStatus::Filled;
        let fills = [fill_of(&o, dec!(100), dec!(0.01))];
        log.submitted(&o);
        log.filled(&fills);
        let r = &log.view(1)[0];
        assert_eq!(r.filled_quantity, dec!(0.01));
        assert_eq!(r.status, FILLED);
        assert_eq!(r.avg_fill_price, Some(dec!(100)));
    }

    #[test]
    fn partial_fills_accumulate_into_a_volume_weighted_price() {
        let mut log = OrderLog::default();
        let o = order(Side::Sell, dec!(1), 0);
        log.submitted(&o);
        log.filled(&[fill_of(&o, dec!(100), dec!(0.25))]);
        assert_eq!(log.view(1)[0].status, PARTIAL);
        log.filled(&[fill_of(&o, dec!(200), dec!(0.75))]);
        let r = &log.view(1)[0];
        assert_eq!(r.status, FILLED);
        assert_eq!(r.avg_fill_price, Some(dec!(175)), "(25 + 150) / 1");
    }

    /// The screenshot's pairs: two market orders sent a second apart.
    #[test]
    fn the_view_is_newest_first_and_bounded() {
        let mut log = OrderLog::default();
        for i in 0..5 {
            log.submitted(&order(Side::Buy, dec!(1), i));
        }
        let view = log.view(3);
        assert_eq!(view.len(), 3);
        assert!(view.windows(2).all(|w| w[0].at > w[1].at), "newest first");
        assert_eq!(view[0].at, t(4));
    }

    #[test]
    fn the_log_never_grows_past_its_cap_and_forgets_the_oldest_first() {
        let mut log = OrderLog::new(3);
        let orders: Vec<Order> = (0..5).map(|i| order(Side::Buy, dec!(1), i)).collect();
        for o in &orders {
            log.submitted(o);
        }
        assert_eq!(log.len(), 3);
        let ids: Vec<String> = log.view(10).into_iter().map(|r| r.id).collect();
        assert_eq!(ids[0], orders[4].id.0.to_string());
        assert_eq!(ids[2], orders[2].id.0.to_string());
        // The forgotten order's running notional is dropped with it.
        assert!(!log.notional.contains_key(&orders[0].id.0.to_string()));
    }

    #[test]
    fn a_refused_order_is_final_at_once_and_says_why() {
        let mut log = OrderLog::default();
        log.rejected(
            "r-1".to_string(),
            t(5),
            "BTCUSDT",
            "Buy",
            "Market",
            dec!(0.5),
            None,
            "order notional 32000 exceeds per-order notional limit 100".to_string(),
        );
        let r = &log.view(1)[0];
        assert_eq!(r.status, REJECTED);
        assert!(r.detail.as_deref().unwrap().contains("notional limit"));
        // It never worked, so the open-orders sync must leave it alone.
        log.sync_open(t(500), &HashSet::new());
        assert_eq!(log.view(1)[0].status, REJECTED);
    }

    #[test]
    fn a_working_order_that_vanishes_is_closed_only_after_the_grace_period() {
        let mut log = OrderLog::default();
        let o = order(Side::Buy, dec!(1), 0);
        log.submitted(&o);
        // The live open-orders view can lag: not yet in it, 3 s old: still working.
        log.sync_open(t(3), &HashSet::new());
        assert_eq!(log.view(1)[0].status, WORKING);
        log.sync_open(t(12), &HashSet::new());
        let r = &log.view(1)[0];
        assert_eq!(r.status, CLOSED);
        assert!(r.detail.as_deref().unwrap().contains("no longer working"));
    }

    #[test]
    fn a_working_order_that_is_still_open_stays_working() {
        let mut log = OrderLog::default();
        let o = order(Side::Buy, dec!(1), 0);
        log.submitted(&o);
        let open = HashSet::from([o.id.0.to_string()]);
        log.sync_open(t(600), &open);
        assert_eq!(log.view(1)[0].status, WORKING);
    }

    #[test]
    fn a_partly_filled_order_that_vanishes_says_how_much_filled() {
        let mut log = OrderLog::default();
        let o = order(Side::Buy, dec!(1), 0);
        log.submitted(&o);
        log.filled(&[fill_of(&o, dec!(100), dec!(0.4))]);
        log.sync_open(t(60), &HashSet::new());
        let r = &log.view(1)[0];
        assert_eq!(r.status, CLOSED);
        assert!(r.detail.as_deref().unwrap().contains("filled 0.4 of 1"));
    }

    /// The exchange's fill report is the truth: a late fill on an order we had
    /// already called closed corrects it.
    #[test]
    fn a_late_fill_corrects_an_order_called_closed_too_early() {
        let mut log = OrderLog::default();
        let o = order(Side::Buy, dec!(1), 0);
        log.submitted(&o);
        log.sync_open(t(30), &HashSet::new());
        assert_eq!(log.view(1)[0].status, CLOSED);
        log.filled(&[fill_of(&o, dec!(100), dec!(1))]);
        let r = &log.view(1)[0];
        assert_eq!(r.status, FILLED);
        assert!(r.detail.is_none(), "the old explanation is gone");
    }

    #[test]
    fn fills_for_unknown_orders_and_duplicate_submissions_are_ignored() {
        let mut log = OrderLog::default();
        let known = order(Side::Buy, dec!(1), 0);
        let stranger = order(Side::Buy, dec!(1), 1);
        log.submitted(&known);
        log.submitted(&known);
        assert_eq!(log.len(), 1, "same id twice keeps one");
        log.filled(&[fill_of(&stranger, dec!(100), dec!(1))]);
        assert_eq!(log.view(5)[0].status, WORKING, "nothing was attributed");
        assert_eq!(log.len(), 1);
    }

    #[test]
    fn a_record_survives_a_serde_round_trip() {
        let mut log = OrderLog::default();
        let o = order(Side::Sell, dec!(0.0007), 0);
        log.submitted(&o);
        log.filled(&[fill_of(&o, dec!(64553.82), dec!(0.0007))]);
        let record = log.view(1).remove(0);
        let json = serde_json::to_string(&record).unwrap();
        let back: OrderRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(back, record);
    }
}
