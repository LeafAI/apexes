//! Offline ordered observation receipt. A digest proves equality with
//! independently retained records, not source completeness or capture.
use apexes_core::{MarketEvent, Venue};
use apexes_strategy::checkpoint::StateCursor;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;

const MAX_RECORDS: usize = 8192;
const MAX_RECORD_BYTES: usize = 1024 * 1024;
const MAX_TOTAL_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObservationClock {
    SourceAvailability,
    ConsumerUtc,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletedObservation {
    pub cursor: StateCursor,
    pub event: MarketEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationReceipt {
    pub version: u32,
    pub predecessor: StateCursor,
    pub through: StateCursor,
    pub clock: ObservationClock,
    pub count: usize,
    pub sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservationError {
    Capacity,
    Scope,
    Cursor,
    Time,
    Encoding,
    Mismatch,
}

impl std::fmt::Display for ObservationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Capacity => "observation history exceeds inspection limits",
            Self::Scope => "observation history has invalid scope or event shape",
            Self::Cursor => "observation cursor is missing, duplicated, or nonmonotonic",
            Self::Time => "observation timestamp violates availability or clock policy",
            Self::Encoding => "observation encoding failed",
            Self::Mismatch => "observation history differs from independently retained receipt",
        })
    }
}
impl std::error::Error for ObservationError {}

impl ObservationReceipt {
    pub fn validate(&self) -> Result<(), ObservationError> {
        if self.version != 1
            || self.count == 0
            || self.count > MAX_RECORDS
            || self.predecessor.ordinal == 0
            || self.through.ordinal.checked_sub(self.predecessor.ordinal) != Some(self.count as u64)
            || self.through.observed_at < self.predecessor.observed_at
            || self.sha256.len() != 64
            || !self
                .sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(ObservationError::Cursor);
        }
        Ok(())
    }

    /// Capture a receipt over caller-supplied completed iterations.
    /// The scope and observation clock must be independently selected.
    /// This does not capture a live loop or prove gap-free exchange data.
    pub fn build(
        predecessor: StateCursor,
        through: StateCursor,
        clock: ObservationClock,
        scope: &[(Venue, String)],
        records: &[CompletedObservation],
    ) -> Result<Self, ObservationError> {
        use ObservationError as E;
        if records.is_empty() || records.len() > MAX_RECORDS {
            return Err(E::Capacity);
        }
        if predecessor.ordinal == 0
            || through.ordinal.checked_sub(predecessor.ordinal) != Some(records.len() as u64)
        {
            return Err(E::Cursor);
        }
        let valid_symbol =
            |s: &str| !s.is_empty() && s.len() <= 128 && !s.chars().any(char::is_control);
        let allowed: HashSet<_> = scope.iter().map(|(v, s)| (*v, s.as_str())).collect();
        if scope.is_empty()
            || scope.len() > 128
            || allowed.len() != scope.len()
            || scope.iter().any(|(_, s)| !valid_symbol(s))
        {
            return Err(E::Scope);
        }
        let mut sorted_scope = scope.to_vec();
        sorted_scope.sort_by(|a, b| {
            format!("{:?}", a.0)
                .cmp(&format!("{:?}", b.0))
                .then_with(|| a.1.cmp(&b.1))
        });
        let mut hash = Sha256::new();
        hash.update(b"apexes-completed-observations-v1\0");
        let header = serde_json::to_vec(&(predecessor, through, clock, sorted_scope))
            .map_err(|_| E::Encoding)?;
        hash.update((header.len() as u64).to_be_bytes());
        hash.update(header);
        let mut total = 0usize;
        let mut prior = predecessor;
        for r in records {
            if prior.ordinal.checked_add(1) != Some(r.cursor.ordinal)
                || r.cursor.observed_at < prior.observed_at
            {
                return Err(E::Cursor);
            }
            let e = &r.event;
            if !allowed.contains(&(e.venue(), e.symbol())) {
                return Err(E::Scope);
            }
            if matches!(e,MarketEvent::Bar(b) if b.close_time <= b.open_time)
                || matches!(e,MarketEvent::Depth(d) if d.bids.len()>4096 || d.asks.len()>4096 || d.validate_source_range().is_err())
                || matches!(e,MarketEvent::Trade(t) if t.trade_id.as_ref().is_some_and(|id|id.len()>256))
            {
                return Err(E::Scope);
            }
            let available = crate::market_data_source::observation_timestamp(e);
            if available <= predecessor.observed_at
                || r.cursor.observed_at < available
                || (clock == ObservationClock::SourceAvailability
                    && r.cursor.observed_at != available)
            {
                return Err(E::Time);
            }
            let bytes = serde_json::to_vec(r).map_err(|_| E::Encoding)?;
            total = total.checked_add(bytes.len()).ok_or(E::Capacity)?;
            if bytes.len() > MAX_RECORD_BYTES || total > MAX_TOTAL_BYTES {
                return Err(E::Capacity);
            }
            hash.update((bytes.len() as u64).to_be_bytes());
            hash.update(bytes);
            prior = r.cursor;
        }
        if prior != through {
            return Err(E::Cursor);
        }
        let receipt = Self {
            version: 1,
            predecessor,
            through,
            clock,
            count: records.len(),
            sha256: format!("{:x}", hash.finalize()),
        };
        receipt.validate()?;
        Ok(receipt)
    }

    pub fn verify(
        &self,
        scope: &[(Venue, String)],
        records: &[CompletedObservation],
    ) -> Result<(), ObservationError> {
        self.validate()?;
        if &Self::build(self.predecessor, self.through, self.clock, scope, records)? != self {
            return Err(ObservationError::Mismatch);
        }
        Ok(())
    }
}

/// Actual loop-local observations returned only after the existing
/// checkpoint publication has acknowledged success. Not a durable
/// atomic DB witness or proof of upstream completeness.
#[derive(Debug, Clone)]
pub struct CapturedObservations {
    pub receipt: ObservationReceipt,
    pub scope: Vec<(Venue, String)>,
    pub records: Vec<CompletedObservation>,
}

pub(crate) struct ObservationCapture {
    predecessor: StateCursor,
    clock: ObservationClock,
    scope: Vec<(Venue, String)>,
    records: Vec<CompletedObservation>,
    bytes: usize,
}

impl ObservationCapture {
    pub(crate) fn new(
        predecessor: StateCursor,
        clock: ObservationClock,
        scope: Vec<(Venue, String)>,
    ) -> Self {
        Self {
            predecessor,
            clock,
            scope,
            records: Vec::new(),
            bytes: 0,
        }
    }

    pub(crate) fn record(
        &mut self,
        cursor: StateCursor,
        event: &MarketEvent,
    ) -> Result<(), ObservationError> {
        use ObservationError as E;
        if self.records.len() >= MAX_RECORDS {
            return Err(E::Capacity);
        }
        // Bound variable-size payloads before cloning or serializing.
        if event.symbol().len() > 128
            || matches!(event,MarketEvent::Depth(d) if d.bids.len()>4096 || d.asks.len()>4096)
            || matches!(event,MarketEvent::Trade(t) if t.trade_id.as_ref().is_some_and(|id|id.len()>256))
        {
            return Err(E::Scope);
        }
        let record = CompletedObservation {
            cursor,
            event: event.clone(),
        };
        let size = serde_json::to_vec(&record).map_err(|_| E::Encoding)?.len();
        let total = self.bytes.checked_add(size).ok_or(E::Capacity)?;
        if size > MAX_RECORD_BYTES || total > MAX_TOTAL_BYTES {
            return Err(E::Capacity);
        }
        self.bytes = total;
        self.records.push(record);
        Ok(())
    }

    pub(crate) fn finish(
        self,
        through: StateCursor,
    ) -> Result<CapturedObservations, ObservationError> {
        let receipt = ObservationReceipt::build(
            self.predecessor,
            through,
            self.clock,
            &self.scope,
            &self.records,
        )?;
        Ok(CapturedObservations {
            receipt,
            scope: self.scope,
            records: self.records,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{Bar, BarInterval, TradeEvent};
    use chrono::{DateTime, Utc};
    use rust_decimal_macros::dec;

    fn at(s: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + s, 0).unwrap()
    }
    fn cursor(ordinal: u64, s: i64) -> StateCursor {
        StateCursor {
            ordinal,
            observed_at: at(s),
        }
    }
    fn trade(ordinal: u64, s: i64, observed: i64) -> CompletedObservation {
        CompletedObservation {
            cursor: cursor(ordinal, observed),
            event: MarketEvent::Trade(TradeEvent {
                symbol: "BTCUSDT".into(),
                venue: Venue::Binance,
                price: dec!(100),
                quantity: dec!(1),
                side: None,
                trade_id: Some(ordinal.to_string()),
                timestamp: at(s),
            }),
        }
    }
    fn scope() -> Vec<(Venue, String)> {
        vec![(Venue::Binance, "BTCUSDT".into())]
    }

    #[test]
    fn capture_uses_actual_records_and_exact_completed_cursor() {
        let mut capture =
            ObservationCapture::new(cursor(10, 0), ObservationClock::ConsumerUtc, scope());
        let first = trade(11, 1, 2);
        let second = trade(12, 2, 3);
        capture.record(first.cursor, &first.event).unwrap();
        capture.record(second.cursor, &second.event).unwrap();
        let captured = capture.finish(second.cursor).unwrap();
        assert_eq!(captured.records, vec![first, second]);
        captured
            .receipt
            .verify(&captured.scope, &captured.records)
            .unwrap();
        assert_eq!(captured.receipt.clock, ObservationClock::ConsumerUtc);
    }

    #[test]
    fn capture_refuses_capacity_and_invalid_completion() {
        let mut capture =
            ObservationCapture::new(cursor(10, 0), ObservationClock::SourceAvailability, scope());
        let item = trade(11, 1, 1);
        capture.bytes = MAX_TOTAL_BYTES;
        assert_eq!(
            capture.record(item.cursor, &item.event),
            Err(ObservationError::Capacity)
        );
        assert!(capture.records.is_empty());
        capture.bytes = 0;
        capture.record(item.cursor, &item.event).unwrap();
        assert_eq!(
            capture.finish(cursor(12, 2)).unwrap_err(),
            ObservationError::Cursor
        );
    }

    #[test]
    fn exact_history_detects_payload_scope_and_clock_changes() {
        let records = vec![trade(11, 1, 1), trade(12, 1, 1)];
        let scope = scope();
        let receipt = ObservationReceipt::build(
            cursor(10, 0),
            cursor(12, 1),
            ObservationClock::SourceAvailability,
            &scope,
            &records,
        )
        .unwrap();
        receipt.verify(&scope, &records).unwrap();
        let mut changed = records.clone();
        if let MarketEvent::Trade(t) = &mut changed[0].event {
            t.price = dec!(101);
        }
        assert_eq!(
            receipt.verify(&scope, &changed),
            Err(ObservationError::Mismatch)
        );
        let mut expanded = scope.clone();
        expanded.push((Venue::Binance, "ETHUSDT".into()));
        assert_eq!(
            receipt.verify(&expanded, &records),
            Err(ObservationError::Mismatch)
        );
        let mut wrong = receipt.clone();
        wrong.clock = ObservationClock::ConsumerUtc;
        assert_eq!(
            wrong.verify(&scope, &records),
            Err(ObservationError::Mismatch)
        );
    }

    #[test]
    fn cursor_gaps_reordering_and_wrong_endpoint_refused() {
        let scope = scope();
        for records in [
            vec![trade(11, 1, 1)],
            vec![trade(12, 1, 1), trade(11, 2, 2)],
            vec![trade(11, 1, 1), trade(11, 2, 2)],
            vec![trade(11, 2, 2), trade(12, 1, 1)],
        ] {
            assert_eq!(
                ObservationReceipt::build(
                    cursor(10, 0),
                    cursor(12, 2),
                    ObservationClock::SourceAvailability,
                    &scope,
                    &records
                ),
                Err(ObservationError::Cursor)
            );
        }
    }

    #[test]
    fn consumer_clock_accepts_delayed_arrival_but_not_preavailability_or_old_source() {
        let scope = scope();
        let records = vec![trade(11, 2, 4), trade(12, 1, 5)];
        ObservationReceipt::build(
            cursor(10, 0),
            cursor(12, 5),
            ObservationClock::ConsumerUtc,
            &scope,
            &records,
        )
        .unwrap();
        assert_eq!(
            ObservationReceipt::build(
                cursor(10, 0),
                cursor(12, 5),
                ObservationClock::SourceAvailability,
                &scope,
                &records
            ),
            Err(ObservationError::Time)
        );
        for record in [trade(11, 2, 1), trade(11, 0, 1)] {
            assert_eq!(
                ObservationReceipt::build(
                    cursor(10, 0),
                    record.cursor,
                    ObservationClock::ConsumerUtc,
                    &scope,
                    &[record]
                ),
                Err(ObservationError::Time)
            );
        }
    }

    #[test]
    fn bars_use_close_not_open_and_require_valid_interval() {
        let scope = scope();
        let mut record = CompletedObservation {
            cursor: cursor(11, 60),
            event: MarketEvent::Bar(Bar {
                symbol: "BTCUSDT".into(),
                venue: Venue::Binance,
                open: dec!(100),
                high: dec!(102),
                low: dec!(99),
                close: dec!(101),
                volume: dec!(10),
                interval: BarInterval::OneMinute,
                open_time: at(0),
                close_time: at(60),
            }),
        };
        ObservationReceipt::build(
            cursor(10, 0),
            record.cursor,
            ObservationClock::SourceAvailability,
            &scope,
            std::slice::from_ref(&record),
        )
        .unwrap();
        record.cursor.observed_at = at(1);
        assert_eq!(
            ObservationReceipt::build(
                cursor(10, 0),
                record.cursor,
                ObservationClock::ConsumerUtc,
                &scope,
                std::slice::from_ref(&record)
            ),
            Err(ObservationError::Time)
        );
        if let MarketEvent::Bar(b) = &mut record.event {
            b.close_time = b.open_time;
        }
        assert_eq!(
            ObservationReceipt::build(
                cursor(10, 0),
                record.cursor,
                ObservationClock::ConsumerUtc,
                &scope,
                &[record]
            ),
            Err(ObservationError::Scope)
        );
    }

    #[test]
    fn bounded_history_and_independently_selected_scope() {
        let scope = scope();
        let records = vec![trade(11, 1, 1); MAX_RECORDS + 1];
        assert_eq!(
            ObservationReceipt::build(
                cursor(10, 0),
                cursor(11, 1),
                ObservationClock::SourceAvailability,
                &scope,
                &records
            ),
            Err(ObservationError::Capacity)
        );
        let record = trade(11, 1, 1);
        let wrong = vec![(Venue::Binance, "ETHUSDT".into())];
        assert_eq!(
            ObservationReceipt::build(
                cursor(10, 0),
                record.cursor,
                ObservationClock::SourceAvailability,
                &wrong,
                std::slice::from_ref(&record)
            ),
            Err(ObservationError::Scope)
        );
        let duplicate = vec![scope[0].clone(), scope[0].clone()];
        assert_eq!(
            ObservationReceipt::build(
                cursor(10, 0),
                record.cursor,
                ObservationClock::SourceAvailability,
                &duplicate,
                &[record]
            ),
            Err(ObservationError::Scope)
        );
    }
}
