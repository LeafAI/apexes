//! F03.03: partition receipt-bound raw capture at explicit invalidations.
//! These are observed event spans, not exchange time coverage claims.
use crate::collector_receipts::CaptureMarket;
use apexes_core::DepthUpdate;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaptureSegment {
    pub start_index: usize,
    pub end_index_exclusive: usize,
    pub first_observed_at: DateTime<Utc>,
    pub last_observed_at: DateTime<Utc>,
    pub snapshot_sequence: u64,
    pub final_sequence: u64,
    pub increments: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaptureGap {
    pub first_invalidation_index: usize,
    pub end_index_exclusive: usize,
    pub first_invalidation_at: DateTime<Utc>,
    pub last_invalidation_at: DateTime<Utc>,
    /// Last valid publication before the gap, if observed in this run.
    pub previous_valid_at: Option<DateTime<Utc>>,
    /// First valid snapshot after the gap, if observed in this run.
    pub next_snapshot_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CaptureSegmentation {
    pub segments: Vec<CaptureSegment>,
    pub gaps: Vec<CaptureGap>,
}

pub(crate) struct Segmenter {
    market: CaptureMarket,
    active: Option<CaptureSegment>,
    gap: Option<CaptureGap>,
    report: CaptureSegmentation,
}

impl Segmenter {
    pub(crate) fn new(market: CaptureMarket) -> Self {
        Self {
            market,
            active: None,
            gap: None,
            report: CaptureSegmentation::default(),
        }
    }

    fn close_segment(&mut self) -> Result<(), String> {
        if let Some(segment) = self.active.take() {
            if segment.increments == 0 {
                return Err("capture segment lacks advancing snapshot bridge".into());
            }
            self.report.segments.push(segment);
        }
        Ok(())
    }

    fn invalidate(&mut self, index: usize, d: &DepthUpdate) -> Result<(), String> {
        self.close_segment()?;
        let gap = self.gap.get_or_insert_with(|| CaptureGap {
            first_invalidation_index: index,
            end_index_exclusive: index,
            first_invalidation_at: d.timestamp,
            last_invalidation_at: d.timestamp,
            previous_valid_at: self.report.segments.last().map(|s| s.last_observed_at),
            next_snapshot_at: None,
        });
        gap.end_index_exclusive = index + 1;
        gap.last_invalidation_at = d.timestamp;
        Ok(())
    }

    fn snapshot(&mut self, index: usize, d: &DepthUpdate, sequence: u64) -> Result<(), String> {
        if self.active.is_some() {
            return Err("unexpected snapshot without capture invalidation".into());
        }
        if let Some(mut gap) = self.gap.take() {
            gap.next_snapshot_at = Some(d.timestamp);
            self.report.gaps.push(gap);
        }
        self.active = Some(CaptureSegment {
            start_index: index,
            end_index_exclusive: index + 1,
            first_observed_at: d.timestamp,
            last_observed_at: d.timestamp,
            snapshot_sequence: sequence,
            final_sequence: sequence,
            increments: 0,
        });
        Ok(())
    }

    pub(crate) fn observe(&mut self, index: usize, d: &DepthUpdate) -> Result<(), String> {
        d.validate_source_range()?;
        if d.is_invalidation() {
            return self.invalidate(index, d);
        }
        let sequence = d.sequence.ok_or("capture event missing sequence")?;
        if d.is_snapshot {
            return self.snapshot(index, d, sequence);
        }
        let active = self
            .active
            .as_mut()
            .ok_or("capture increment without synchronized snapshot")?;
        let r = d
            .source_range
            .ok_or("capture increment missing native range")?;
        let prior = active.final_sequence;
        let linked = match self.market {
            CaptureMarket::Spot => {
                r.previous_final.is_none()
                    && prior
                        .checked_add(1)
                        .is_some_and(|next| r.first <= next && next <= r.last)
            }
            CaptureMarket::UsdMPerpetual if active.increments == 0 => {
                r.first <= prior && r.previous_final.is_some_and(|previous| previous <= prior)
            }
            CaptureMarket::UsdMPerpetual => r.previous_final == Some(prior),
        };
        if r.last <= prior || !linked {
            return Err(format!(
                "capture depth link or advancing range invalid at event {index}"
            ));
        }
        active.final_sequence = sequence;
        active.increments += 1;
        active.last_observed_at = d.timestamp;
        active.end_index_exclusive = index + 1;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<CaptureSegmentation, String> {
        self.close_segment()?;
        if let Some(gap) = self.gap.take() {
            self.report.gaps.push(gap);
        }
        Ok(self.report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{DepthSourceRange, Venue};
    use chrono::TimeZone;

    fn at(n: usize) -> DateTime<Utc> {
        Utc.timestamp_opt(n as i64, 0).unwrap()
    }

    fn snapshot(index: usize, sequence: u64) -> DepthUpdate {
        DepthUpdate {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            bids: vec![],
            asks: vec![],
            sequence: Some(sequence),
            source_range: None,
            is_snapshot: true,
            timestamp: at(index),
        }
    }

    fn delta(index: usize, first: u64, last: u64, previous_final: Option<u64>) -> DepthUpdate {
        DepthUpdate {
            is_snapshot: false,
            source_range: Some(DepthSourceRange {
                first,
                last,
                previous_final,
            }),
            ..snapshot(index, last)
        }
    }

    fn invalidation(index: usize) -> DepthUpdate {
        DepthUpdate {
            sequence: None,
            ..snapshot(index, 0)
        }
    }

    fn scan(market: CaptureMarket, updates: &[DepthUpdate]) -> Result<CaptureSegmentation, String> {
        let mut state = Segmenter::new(market);
        for (i, d) in updates.iter().enumerate() {
            state.observe(i, d)?;
        }
        state.finish()
    }

    #[test]
    fn leading_repeated_and_trailing_invalidations_are_explicit_gaps() {
        let report = scan(
            CaptureMarket::Spot,
            &[
                invalidation(0),
                invalidation(1),
                snapshot(2, 100),
                delta(3, 101, 105, None),
                invalidation(4),
                invalidation(5),
                snapshot(6, 10),
                delta(7, 10, 12, None),
                invalidation(8),
            ],
        )
        .unwrap();
        assert_eq!(report.segments.len(), 2);
        assert_eq!(report.gaps.len(), 3);
        assert_eq!(
            (
                report.segments[0].start_index,
                report.segments[0].end_index_exclusive
            ),
            (2, 4)
        );
        assert_eq!(
            (
                report.segments[1].snapshot_sequence,
                report.segments[1].final_sequence
            ),
            (10, 12)
        );
        let leading = &report.gaps[0];
        assert_eq!(
            (
                leading.first_invalidation_index,
                leading.end_index_exclusive
            ),
            (0, 2)
        );
        assert_eq!(leading.previous_valid_at, None);
        assert_eq!(leading.next_snapshot_at, Some(at(2)));
        let middle = &report.gaps[1];
        assert_eq!(
            (middle.first_invalidation_index, middle.end_index_exclusive),
            (4, 6)
        );
        assert_eq!(middle.previous_valid_at, Some(at(3)));
        assert_eq!(middle.next_snapshot_at, Some(at(6)));
        assert_eq!(report.gaps[2].previous_valid_at, Some(at(7)));
        assert_eq!(report.gaps[2].next_snapshot_at, None);
    }

    #[test]
    fn spot_rejects_unmarked_gap_reset_and_snapshot_without_bridge() {
        for events in [
            vec![delta(0, 101, 105, None)],
            vec![snapshot(0, 100)],
            vec![snapshot(0, 100), invalidation(1)],
            vec![snapshot(0, 100), delta(1, 101, 105, None), snapshot(2, 105)],
            vec![snapshot(0, 100), delta(1, 103, 105, None)],
            vec![snapshot(0, 100), delta(1, 101, 105, Some(100))],
            vec![
                snapshot(0, 100),
                delta(1, 101, 105, None),
                delta(2, 102, 105, None),
            ],
            vec![
                snapshot(0, 100),
                delta(1, 101, 105, None),
                invalidation(2),
                delta(3, 106, 109, None),
            ],
        ] {
            assert!(scan(CaptureMarket::Spot, &events).is_err(), "{events:?}");
        }
        let mut missing = delta(1, 101, 105, None);
        missing.source_range = None;
        assert!(scan(CaptureMarket::Spot, &[snapshot(0, 100), missing]).is_err());
    }

    #[test]
    fn futures_bridge_and_previous_final_are_restarted_per_segment() {
        let events = [
            snapshot(0, 100),
            delta(1, 98, 103, Some(97)),
            delta(2, 110, 112, Some(103)),
            invalidation(3),
            snapshot(4, 50),
            delta(5, 48, 52, Some(45)),
            delta(6, 60, 62, Some(52)),
        ];
        let report = scan(CaptureMarket::UsdMPerpetual, &events).unwrap();
        assert_eq!(report.segments[0].final_sequence, 112);
        assert_eq!(report.segments[1].final_sequence, 62);
        assert_eq!(report.segments[1].increments, 2);
        for bad in [
            delta(5, 51, 52, Some(49)),
            delta(5, 48, 52, Some(51)),
            delta(5, 48, 52, None),
        ] {
            let mut altered = events.clone();
            altered[5] = bad;
            assert!(scan(CaptureMarket::UsdMPerpetual, &altered).is_err());
        }
        let mut altered = events;
        altered[6] = delta(6, 60, 62, Some(51));
        assert!(scan(CaptureMarket::UsdMPerpetual, &altered).is_err());
    }

    #[test]
    fn empty_and_invalidation_only_runs_have_no_synchronized_segments() {
        assert_eq!(
            scan(CaptureMarket::Spot, &[]).unwrap(),
            CaptureSegmentation::default()
        );
        let report = scan(CaptureMarket::Spot, &[invalidation(0), invalidation(1)]).unwrap();
        assert!(report.segments.is_empty());
        assert_eq!(report.gaps.len(), 1);
        assert_eq!(report.gaps[0].previous_valid_at, None);
        assert_eq!(report.gaps[0].next_snapshot_at, None);
    }
}
