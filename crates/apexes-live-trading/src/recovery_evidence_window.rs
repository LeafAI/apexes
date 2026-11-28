//! Offline qualification of supplied recovery evidence intervals and coverage.
//! These checks do not fetch evidence, prove provenance, or authorize startup.
use apexes_core::{AssetClass, Venue};
use apexes_oms::{CoherentRecoverySnapshot, TerminalOrderEvidence};
use chrono::{DateTime, TimeDelta, Utc};

/// Conservative fixed ceiling for the entire supplied inspection window.
/// Older evidence requires a fresh, independently collected set.
pub const MAX_EVIDENCE_AGE: TimeDelta = TimeDelta::seconds(60);

#[derive(Debug, Clone, Copy)]
pub struct EvidenceWindow {
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
}

/// The evidence supplier must establish account-wide coverage, including
/// foreign/manual orders. A symbol-scoped empty response is insufficient.
#[derive(Debug, Clone)]
pub struct OpenOrderCoverage {
    pub window: EvidenceWindow,
    pub venue: Venue,
    pub asset_class: AssetClass,
    pub owner_fingerprint: String,
    pub account_wide_complete: bool,
    pub open_order_count: u64,
    pub foreign_order_count: u64,
}

/// Independently supplied observations. The owner fingerprint and time
/// intervals are assertions from the evidence supplier, not authentication
/// or a proof of an atomic exchange snapshot.
#[derive(Debug, Clone)]
pub struct RecoveryEvidenceWindow {
    pub terminal_window: EvidenceWindow,
    pub terminal_owner_fingerprint: String,
    pub terminal_evidence_complete: bool,
    pub terminal_orders: Vec<TerminalOrderEvidence>,
    pub open_orders: OpenOrderCoverage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EvidenceWindowError {
    #[error("supplied evidence has incomplete account coverage")]
    Incomplete,
    #[error("supplied evidence belongs to a different owner or market scope")]
    Scope,
    #[error("supplied evidence interval is reversed, future-dated, or out of order")]
    Interval,
    #[error("supplied evidence predates the selected journal or exceeds the freshness window")]
    Stale,
    #[error("supplied account evidence reports open or foreign orders")]
    OpenOrders,
}

/// Compare supplied intervals against an independently selected inspection
/// time and the actual durable journal. Success is only an offline check of
/// the supplied assertions, not independently authenticated freshness.
pub(crate) fn validate_evidence_window(
    evidence: &RecoveryEvidenceWindow,
    snapshot: &CoherentRecoverySnapshot,
    checkpoint_at: DateTime<Utc>,
    expected_owner: &str,
    inspected_at: DateTime<Utc>,
) -> Result<(), EvidenceWindowError> {
    use EvidenceWindowError as E;
    let terminal = evidence.terminal_window;
    let open = evidence.open_orders.window;
    if !evidence.terminal_evidence_complete || !evidence.open_orders.account_wide_complete {
        return Err(E::Incomplete);
    }
    if evidence.terminal_owner_fingerprint != expected_owner
        || evidence.open_orders.owner_fingerprint != expected_owner
        || evidence.open_orders.venue != Venue::Binance
        || evidence.open_orders.asset_class != AssetClass::Spot
    {
        return Err(E::Scope);
    }
    if terminal.finished_at < terminal.started_at
        || open.finished_at < open.started_at
        || open.started_at < terminal.finished_at
        || open.finished_at > inspected_at
    {
        return Err(E::Interval);
    }
    // The lower bound includes late durable execution timestamps, not
    // just the market observation cursor (which may be much older).
    let latest = snapshot.orders.iter().fold(checkpoint_at, |at, order| {
        at.max(order.updated_at).max(order.created_at)
    });
    let latest = snapshot
        .fills
        .iter()
        .fold(latest, |at, fill| at.max(fill.filled_at));
    if terminal.started_at < latest
        || inspected_at.signed_duration_since(terminal.started_at) > MAX_EVIDENCE_AGE
    {
        return Err(E::Stale);
    }
    if evidence.open_orders.open_order_count != 0 || evidence.open_orders.foreign_order_count != 0 {
        return Err(E::OpenOrders);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{Order, OrderId, OrderStatus, OrderType, Side};
    use rust_decimal::Decimal;

    fn at(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + seconds, 0).unwrap()
    }

    fn fixture() -> (CoherentRecoverySnapshot, RecoveryEvidenceWindow) {
        let order = Order {
            id: OrderId(uuid::Uuid::new_v4()),
            exchange_order_id: Some("remote".into()),
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: Decimal::ONE,
            price: None,
            stop_price: None,
            leverage: None,
            margin_mode: None,
            filled_quantity: Decimal::ZERO,
            status: OrderStatus::Cancelled,
            created_at: at(1),
            updated_at: at(5),
        };
        let snapshot = CoherentRecoverySnapshot {
            envelope: vec![],
            orders: vec![order],
            fills: vec![],
        };
        let evidence = RecoveryEvidenceWindow {
            terminal_window: EvidenceWindow {
                started_at: at(6),
                finished_at: at(10),
            },
            terminal_owner_fingerprint: "selected-owner".into(),
            terminal_evidence_complete: true,
            terminal_orders: vec![],
            open_orders: OpenOrderCoverage {
                window: EvidenceWindow {
                    started_at: at(10),
                    finished_at: at(12),
                },
                venue: Venue::Binance,
                asset_class: AssetClass::Spot,
                owner_fingerprint: "selected-owner".into(),
                account_wide_complete: true,
                open_order_count: 0,
                foreign_order_count: 0,
            },
        };
        (snapshot, evidence)
    }

    fn check(
        snapshot: &CoherentRecoverySnapshot,
        evidence: &RecoveryEvidenceWindow,
        now: i64,
    ) -> Result<(), EvidenceWindowError> {
        validate_evidence_window(evidence, snapshot, at(0), "selected-owner", at(now))
    }

    #[test]
    fn ordered_complete_evidence_within_sixty_seconds() {
        let (snapshot, evidence) = fixture();
        check(&snapshot, &evidence, 12).unwrap();
        check(&snapshot, &evidence, 66).unwrap();
        assert_eq!(
            check(&snapshot, &evidence, 67),
            Err(EvidenceWindowError::Stale)
        );
    }

    #[test]
    fn evidence_must_follow_latest_durable_timestamp_not_only_market_cursor() {
        let (mut snapshot, mut evidence) = fixture();
        evidence.terminal_window.started_at = at(4);
        assert_eq!(
            check(&snapshot, &evidence, 12),
            Err(EvidenceWindowError::Stale)
        );
        evidence.terminal_window.started_at = at(6);
        snapshot.fills.push(apexes_oms::schema::FillRow {
            id: uuid::Uuid::new_v4(),
            order_id: snapshot.orders[0].id.0,
            exchange_trade_id: "late".into(),
            symbol: "BTCUSDT".into(),
            venue: "Binance".into(),
            side: "Buy".into(),
            price: Decimal::ONE,
            quantity: Decimal::ONE,
            fee: Decimal::ZERO,
            fee_currency: None,
            filled_at: at(7),
        });
        assert_eq!(
            check(&snapshot, &evidence, 12),
            Err(EvidenceWindowError::Stale)
        );
    }

    #[test]
    fn reversed_overlapping_and_future_intervals_refused() {
        let (snapshot, evidence) = fixture();
        for i in 0..4 {
            let mut bad = evidence.clone();
            match i {
                0 => bad.terminal_window.finished_at = at(5),
                1 => bad.open_orders.window.started_at = at(9),
                2 => bad.open_orders.window.finished_at = at(9),
                _ => bad.open_orders.window.finished_at = at(13),
            }
            assert_eq!(
                check(&snapshot, &bad, 12),
                Err(EvidenceWindowError::Interval)
            );
        }
    }

    #[test]
    fn incomplete_account_coverage_and_wrong_owner_rejected() {
        let (snapshot, evidence) = fixture();
        let mut bad = evidence.clone();
        bad.terminal_evidence_complete = false;
        assert_eq!(
            check(&snapshot, &bad, 12),
            Err(EvidenceWindowError::Incomplete)
        );
        let mut bad = evidence.clone();
        bad.open_orders.account_wide_complete = false;
        assert_eq!(
            check(&snapshot, &bad, 12),
            Err(EvidenceWindowError::Incomplete)
        );
        let mut bad = evidence.clone();
        bad.terminal_owner_fingerprint = "other".into();
        assert_eq!(check(&snapshot, &bad, 12), Err(EvidenceWindowError::Scope));
        let mut bad = evidence.clone();
        bad.open_orders.owner_fingerprint = "other".into();
        assert_eq!(check(&snapshot, &bad, 12), Err(EvidenceWindowError::Scope));
        let mut bad = evidence;
        bad.open_orders.venue = Venue::Okx;
        assert_eq!(check(&snapshot, &bad, 12), Err(EvidenceWindowError::Scope));
    }

    #[test]
    fn nonzero_open_or_foreign_order_counts_refused() {
        let (snapshot, evidence) = fixture();
        let mut bad = evidence.clone();
        bad.open_orders.open_order_count = 1;
        assert_eq!(
            check(&snapshot, &bad, 12),
            Err(EvidenceWindowError::OpenOrders)
        );
        let mut bad = evidence;
        bad.open_orders.foreign_order_count = 1;
        assert_eq!(
            check(&snapshot, &bad, 12),
            Err(EvidenceWindowError::OpenOrders)
        );
    }
}
