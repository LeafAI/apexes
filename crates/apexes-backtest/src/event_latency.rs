//! F03.02: deterministic event-time latency for direct strategy orders.
use apexes_core::{ApexesError, Venue, registry::Registry};
use apexes_strategy::OrderIntent;
use chrono::{DateTime, TimeDelta, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventLatencyPolicy {
    pub delay_ms: u32,
}

impl EventLatencyPolicy {
    pub fn validate(&self) -> Result<(), String> {
        if self.delay_ms == 0 {
            return Err("event-time delay must be positive".into());
        }
        Ok(())
    }
    pub fn from_registry(registry: &Registry) -> Result<Option<Self>, ApexesError> {
        let p = registry
            .try_one::<Self>()
            .map_err(|e| ApexesError::InvalidBacktestConfig {
                reason: e.to_string(),
            })?;
        if let Some(p) = p {
            p.validate()
                .map_err(|reason| ApexesError::InvalidBacktestConfig { reason })?;
        }
        Ok(p)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventLatencyReport {
    pub policy: EventLatencyPolicy,
    pub scheduled: u64,
    pub arrived: u64,
    pub expired_at_end: u64,
    pub refused: u64,
    pub regressing_events: u64,
    /// Spot market-intent capital reservation evidence (before admission).
    #[serde(default)]
    pub capital_refused: u64,
    #[serde(default)]
    pub capital_released_at_end: u64,

    /// Separate EMS child transit accounting; None for legacy reports.
    #[serde(default)]
    pub child_orders: Option<ChildLatencyReport>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildLatencyReport {
    pub scheduled: u64,
    pub arrived: u64,
    pub refused: u64,
    pub rejected_at_arrival: u64,
    pub cancelled_before_arrival: u64,
    pub expired_at_end: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CapitalHold {
    pub asset: String,
    pub amount: Decimal,
}

pub(crate) struct ArrivalQueue {
    pending: VecDeque<(DateTime<Utc>, OrderIntent)>,
    holds: HashMap<(Venue, String), CapitalHold>,
    pub report: EventLatencyReport,
    watermark: Option<DateTime<Utc>>,
}

impl ArrivalQueue {
    pub fn new(policy: EventLatencyPolicy) -> Self {
        Self {
            pending: VecDeque::new(),
            holds: HashMap::new(),
            report: EventLatencyReport {
                policy,
                scheduled: 0,
                arrived: 0,
                expired_at_end: 0,
                refused: 0,
                regressing_events: 0,
                capital_refused: 0,
                capital_released_at_end: 0,
                child_orders: None,
            },
            watermark: None,
        }
    }
    pub fn observe(&mut self, at: DateTime<Utc>) -> bool {
        if self.watermark.is_some_and(|old| at < old) {
            self.report.regressing_events += 1;
            return false;
        }
        self.watermark = Some(at);
        true
    }
    pub fn check_capital(&self, hold: &CapitalHold, balances: &HashMap<String, Decimal>) -> bool {
        let total = self
            .holds
            .values()
            .filter(|h| h.asset == hold.asset)
            .try_fold(hold.amount, |sum, h| sum.checked_add(h.amount));
        hold.amount > Decimal::ZERO
            && total.is_some_and(|amount| {
                balances.get(&hold.asset).copied().unwrap_or_default() >= amount
            })
    }

    pub fn capital_refused(&mut self, before_admission: bool) {
        self.report.capital_refused += 1;
        if before_admission {
            self.report.refused += 1;
        }
    }

    /// Keep due holds until each intent is individually processed; extracting
    /// the whole due batch must not give the first arrival another's funds.
    pub fn release(&mut self, intent: &OrderIntent) {
        self.holds.remove(&(intent.venue, intent.symbol.clone()));
    }

    pub fn preserves_capital(
        &self,
        before: &HashMap<String, Decimal>,
        after: &HashMap<String, Decimal>,
    ) -> bool {
        self.holds.values().all(|hold| {
            let old = before.get(&hold.asset).copied().unwrap_or_default();
            let new = after.get(&hold.asset).copied().unwrap_or_default();
            // Do not reject a fill that improves a pre-existing shortfall.
            new >= old
                || self
                    .holds
                    .values()
                    .filter(|h| h.asset == hold.asset)
                    .try_fold(Decimal::ZERO, |sum, h| sum.checked_add(h.amount))
                    .is_some_and(|reserved| new >= reserved)
        })
    }

    pub fn schedule(
        &mut self,
        at: DateTime<Utc>,
        intent: OrderIntent,
        hold: Option<CapitalHold>,
    ) -> Result<(), &'static str> {
        let deadline = at.checked_add_signed(TimeDelta::milliseconds(i64::from(
            self.report.policy.delay_ms,
        )));
        let reason = if self
            .pending
            .iter()
            .any(|(_, p)| p.venue == intent.venue && p.symbol == intent.symbol)
        {
            Some("event_latency_symbol_pending")
        } else if self.pending.len() >= 1024 {
            Some("event_latency_queue_full")
        } else if deadline.is_none() {
            Some("event_latency_timestamp_overflow")
        } else {
            None
        };
        if let Some(reason) = reason {
            self.report.refused += 1;
            return Err(reason);
        }
        let deadline = deadline.ok_or("event_latency_timestamp_overflow")?;
        if let Some(hold) = hold {
            self.holds
                .insert((intent.venue, intent.symbol.clone()), hold);
        }
        self.pending.push_back((deadline, intent));
        self.report.scheduled += 1;
        Ok(())
    }
    pub fn due(&mut self, at: DateTime<Utc>) -> Vec<OrderIntent> {
        let mut due = Vec::new();
        while self
            .pending
            .front()
            .is_some_and(|(deadline, _)| *deadline <= at)
        {
            if let Some((_, intent)) = self.pending.pop_front() {
                due.push(intent);
                self.report.arrived += 1;
            }
        }
        due
    }
    pub fn finish(mut self) -> EventLatencyReport {
        self.report.expired_at_end = self.pending.len() as u64;
        self.report.capital_released_at_end = self.holds.len() as u64;
        self.report
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, OrderType, Side, Venue};
    use apexes_strategy::OrderRouting;
    use rust_decimal::Decimal;

    fn intent(symbol: &str) -> OrderIntent {
        OrderIntent {
            symbol: symbol.into(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: Decimal::ONE,
            price: None,
            stop_price: None,
            execution_algo: None,
            routing: OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        }
    }

    #[test]
    fn due_batch_preserves_other_intents_holds_until_individual_release() {
        let at = Utc::now();
        let mut queue = ArrivalQueue::new(EventLatencyPolicy { delay_ms: 500 });
        let hold = CapitalHold {
            asset: "USDT".into(),
            amount: Decimal::from(6),
        };
        let balances = HashMap::from([("USDT".into(), Decimal::from(12))]);
        let first = intent("BTCUSDT");
        let second = intent("ETHUSDT");
        assert!(queue.check_capital(&hold, &balances));
        queue
            .schedule(at, first.clone(), Some(hold.clone()))
            .unwrap();
        assert!(queue.check_capital(&hold, &balances));
        queue
            .schedule(at, second.clone(), Some(hold.clone()))
            .unwrap();
        assert!(!queue.check_capital(&hold, &balances));
        let due = queue.due(at + TimeDelta::milliseconds(500));
        assert_eq!(due.len(), 2);
        assert!(!queue.check_capital(&hold, &balances));
        queue.release(&first);
        let expensive = CapitalHold {
            asset: "USDT".into(),
            amount: Decimal::from(7),
        };
        assert!(!queue.check_capital(&expensive, &balances));
        assert!(!queue.preserves_capital(
            &balances,
            &HashMap::from([("USDT".into(), Decimal::from(5))])
        ));
        queue.release(&second);
        assert!(queue.check_capital(&expensive, &balances));
        queue
            .schedule(at + TimeDelta::milliseconds(500), second, Some(hold))
            .unwrap();
        let report = queue.finish();
        assert_eq!(report.capital_released_at_end, 1);
        assert_eq!(report.expired_at_end, 1);
    }

    #[test]
    fn fifo_duplicate_regression_and_expiry_are_deterministic() {
        let at = Utc::now();
        let mut queue = ArrivalQueue::new(EventLatencyPolicy { delay_ms: 500 });
        assert!(queue.observe(at));
        queue.schedule(at, intent("BTCUSDT"), None).unwrap();
        queue.schedule(at, intent("ETHUSDT"), None).unwrap();
        assert_eq!(
            queue.schedule(at, intent("BTCUSDT"), None),
            Err("event_latency_symbol_pending")
        );
        assert!(!queue.observe(at - TimeDelta::milliseconds(1)));
        assert!(queue.due(at + TimeDelta::milliseconds(499)).is_empty());
        let arrived = queue.due(at + TimeDelta::milliseconds(500));
        assert_eq!(
            arrived
                .iter()
                .map(|i| i.symbol.as_str())
                .collect::<Vec<_>>(),
            vec!["BTCUSDT", "ETHUSDT"]
        );
        queue
            .schedule(at + TimeDelta::milliseconds(500), intent("BTCUSDT"), None)
            .unwrap();
        let report = queue.finish();
        assert_eq!(
            (
                report.scheduled,
                report.arrived,
                report.expired_at_end,
                report.refused,
                report.regressing_events
            ),
            (3, 2, 1, 1, 1)
        );
    }
}
