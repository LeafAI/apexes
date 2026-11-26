//! F03.02: deterministic event-time latency for direct strategy orders.
use apexes_core::{ApexesError, registry::Registry};
use apexes_strategy::OrderIntent;
use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

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
}

pub(crate) struct ArrivalQueue {
    pending: VecDeque<(DateTime<Utc>, OrderIntent)>,
    pub report: EventLatencyReport,
    watermark: Option<DateTime<Utc>>,
}

impl ArrivalQueue {
    pub fn new(policy: EventLatencyPolicy) -> Self {
        Self {
            pending: VecDeque::new(),
            report: EventLatencyReport {
                policy,
                scheduled: 0,
                arrived: 0,
                expired_at_end: 0,
                refused: 0,
                regressing_events: 0,
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
    pub fn schedule(&mut self, at: DateTime<Utc>, intent: OrderIntent) -> Result<(), &'static str> {
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
    fn fifo_duplicate_regression_and_expiry_are_deterministic() {
        let at = Utc::now();
        let mut queue = ArrivalQueue::new(EventLatencyPolicy { delay_ms: 500 });
        assert!(queue.observe(at));
        queue.schedule(at, intent("BTCUSDT")).unwrap();
        queue.schedule(at, intent("ETHUSDT")).unwrap();
        assert_eq!(
            queue.schedule(at, intent("BTCUSDT")),
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
            .schedule(at + TimeDelta::milliseconds(500), intent("BTCUSDT"))
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
