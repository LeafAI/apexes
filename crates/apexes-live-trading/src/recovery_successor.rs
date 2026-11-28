//! Offline consecutive-run qualification. Existing singleton startup
//! claims remain unchanged; two independently preserved stores are needed.
use crate::{
    CoherentInspectionError, CoherentRecoverySelection, OfflineTransitionWitness,
    TransitionEvidence, WalletEvidence,
};
use apexes_oms::{OrderStore, RemoteTerminalEvidence, TerminalOrderEvidence};
use apexes_strategy::Strategy;
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SuccessorContinuityError {
    #[error(
        "successor does not select the exact prior checkpoint, owner, configuration and a distinct run"
    )]
    Identity,
    #[error("successor opening wallet does not follow the prior inspection interval")]
    Interval,
    #[error("successor wallet differs from the prior closing wallet")]
    Wallet,
    #[error("cold successor cannot carry nonflat strategy positions or change tracked allocation")]
    Inventory,
    #[error("execution identities overlap between runs")]
    ExecutionOverlap,
}

#[derive(Debug, thiserror::Error)]
pub enum SuccessorInspectionError {
    #[error("offline boundary inspection failed: {0}")]
    Inspection(#[from] CoherentInspectionError),
    #[error("independently selected historical witness differs: {0}")]
    Witness(#[from] crate::StrategyRecoveryError),
    #[error("successor continuity refused: {0}")]
    Link(#[from] SuccessorContinuityError),
}

/// One independently preserved historical boundary and its selected evidence.
pub struct SuccessorRun<'a, S> {
    pub selection: &'a CoherentRecoverySelection,
    pub store: &'a mut OrderStore,
    pub ems_postgres_url: &'a str,
    pub strategies: (S, S),
    pub evidence: &'a TransitionEvidence<'a>,
    pub witness: &'a OfflineTransitionWitness,
}

fn compare_selection(
    prior: &CoherentRecoverySelection,
    next: &CoherentRecoverySelection,
) -> Result<(), SuccessorContinuityError> {
    use SuccessorContinuityError as E;
    if prior.receipt().checkpoint != *next.predecessor.receipt()
        || prior.predecessor.owner_fingerprint() != next.predecessor.owner_fingerprint()
        || prior.initial_params() != next.initial_params()
        || prior.receipt().run_scope == next.receipt().run_scope
        || prior.receipt().checkpoint.cursor.ordinal >= next.receipt().checkpoint.cursor.ordinal
        || prior.receipt().checkpoint.cursor.observed_at
            > next.receipt().checkpoint.cursor.observed_at
    {
        return Err(E::Identity);
    }
    Ok(())
}

fn compare_wallets(
    prior: &WalletEvidence,
    next: &WalletEvidence,
    inspected_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), SuccessorContinuityError> {
    use SuccessorContinuityError as E;
    if prior.window.finished_at > inspected_at
        || inspected_at > next.window.started_at
        || next.window.finished_at < next.window.started_at
    {
        return Err(E::Interval);
    }
    if prior.owner_fingerprint != next.owner_fingerprint
        || prior.venue != next.venue
        || prior.asset_class != next.asset_class
        || !prior.account_wide_complete
        || !next.account_wide_complete
        || prior.balances != next.balances
    {
        return Err(E::Wallet);
    }
    Ok(())
}

fn compare_execution(
    prior: &[TerminalOrderEvidence],
    next: &[TerminalOrderEvidence],
) -> Result<(), SuccessorContinuityError> {
    let mut clients = HashSet::new();
    let mut orders = HashSet::new();
    let mut trades = HashSet::new();
    for order in prior.iter().chain(next) {
        if !clients.insert(order.client_order_id) {
            return Err(SuccessorContinuityError::ExecutionOverlap);
        }
        if let RemoteTerminalEvidence::Found {
            exchange_order_id,
            trades: fills,
            ..
        } = &order.remote
        {
            if !orders.insert((
                order.venue,
                order.symbol.as_str(),
                exchange_order_id.as_str(),
            )) {
                return Err(SuccessorContinuityError::ExecutionOverlap);
            }
            for fill in fills {
                if !trades.insert((
                    order.venue,
                    order.symbol.as_str(),
                    fill.exchange_trade_id.as_str(),
                )) {
                    return Err(SuccessorContinuityError::ExecutionOverlap);
                }
            }
        }
    }
    Ok(())
}

fn compare_inventory(
    ledger: &apexes_backtest::ledger_evidence::LedgerEvidence,
    positions: &crate::ClosingPositionEvidence,
    allocation: &crate::OpeningTrackedAssetEvidence,
) -> Result<(), SuccessorContinuityError> {
    if !positions.positions.is_empty()
        || ledger.positions.iter().any(|p| !p.quantity.is_zero())
        || ledger.balances != allocation.balances
    {
        return Err(SuccessorContinuityError::Inventory);
    }
    Ok(())
}

/// Requalify both independent historical witnesses against their exact
/// durable stores, then check cold flat-boundary continuity. No state or
/// authority is returned. This does not prove absence of unseen activity
/// between wallet observations, carry risk state, or permit a new claim.
pub async fn inspect_offline_successor<S: Strategy, T: Strategy>(
    prior: SuccessorRun<'_, S>,
    next: SuccessorRun<'_, T>,
) -> Result<(), SuccessorInspectionError> {
    use SuccessorContinuityError as E;
    compare_selection(prior.selection, next.selection)?;
    let a = prior.evidence;
    let b = next.evidence;
    let qualified_prior = prior
        .selection
        .inspect_offline_transition(prior.store, prior.ems_postgres_url, prior.strategies, a)
        .await?;
    prior.witness.compare(&qualified_prior)?;
    let qualified_next = next
        .selection
        .inspect_offline_transition(next.store, next.ems_postgres_url, next.strategies, b)
        .await?;
    next.witness.compare(&qualified_next)?;
    if a.instruments != b.instruments
        || a.conversion.instruments != b.conversion.instruments
        || a.registered_risk != b.registered_risk
        || a.market_monitor != b.market_monitor
        || a.margin_tiers != b.margin_tiers
    {
        return Err(E::Identity.into());
    }
    compare_wallets(a.closing, &b.opening.wallet, a.inspected_at)?;
    compare_execution(&a.window.terminal_orders, &b.window.terminal_orders)?;
    let capture = prior
        .selection
        .inspect_durable_capture(prior.store, prior.ems_postgres_url)
        .await?;
    let ledger = capture.ledger.as_ref().ok_or(E::Inventory)?;
    compare_inventory(ledger, a.positions, b.allocation)?;
    let next_capture = next
        .selection
        .inspect_durable_capture(next.store, next.ems_postgres_url)
        .await?;
    if next_capture
        .records
        .first()
        .is_none_or(|record| record.cursor.observed_at <= b.opening.wallet.window.finished_at)
    {
        return Err(E::Interval.into());
    }
    Ok(())
}

/// One historical run with independently selected opening and closing
/// collection receipts, and a separately retained receipt-bound witness.
pub struct ReceiptBoundSuccessorRun<'a, S> {
    pub selection: &'a CoherentRecoverySelection,
    pub store: &'a mut OrderStore,
    pub ems_postgres_url: &'a str,
    pub strategies: (S, S),
    pub evidence: &'a crate::ReceiptTransitionEvidence<'a>,
    pub witness: &'a crate::ReceiptBoundTransitionWitness,
    pub witness_sha256: &'a str,
}

/// Requalify both receipt-bound runs and compare the exact historical
/// linkage. In addition to wallet equality, require the entire next
/// opening collection to follow the prior closing inspection. No live
/// state, risk carryover, funding proof or startup authority is issued.
pub async fn inspect_receipt_bound_successor<S: Strategy, T: Strategy>(
    prior: ReceiptBoundSuccessorRun<'_, S>,
    next: ReceiptBoundSuccessorRun<'_, T>,
) -> Result<(), SuccessorInspectionError> {
    use SuccessorContinuityError as E;
    compare_selection(prior.selection, next.selection)?;
    if prior.witness.sha256() != prior.witness_sha256
        || next.witness.sha256() != next.witness_sha256
        || prior.witness_sha256 == next.witness_sha256
    {
        return Err(crate::StrategyRecoveryError::InvalidReceipt.into());
    }
    let a = prior.evidence;
    let b = next.evidence;
    if a.instruments != b.instruments
        || a.conversion.instruments != b.conversion.instruments
        || a.registered_risk != b.registered_risk
        || a.market_monitor != b.market_monitor
        || a.margin_tiers != b.margin_tiers
    {
        return Err(E::Identity.into());
    }
    let (qualified_prior, _, prior_closing) = prior
        .selection
        .qualify_receipt_bound_transition(prior.store, prior.ems_postgres_url, prior.strategies, a)
        .await?;
    prior.witness.compare(&qualified_prior)?;
    let (qualified_next, next_opening, next_closing) = next
        .selection
        .qualify_receipt_bound_transition(next.store, next.ems_postgres_url, next.strategies, b)
        .await?;
    next.witness.compare(&qualified_next)?;
    if prior_closing.inspected_at > next_opening.open_before.started_at {
        return Err(E::Interval.into());
    }
    compare_wallets(
        &prior_closing.wallet,
        &next_opening.opening.wallet,
        prior_closing.inspected_at,
    )?;
    compare_execution(
        &prior_closing.window.terminal_orders,
        &next_closing.window.terminal_orders,
    )?;
    let capture = prior
        .selection
        .inspect_durable_capture(prior.store, prior.ems_postgres_url)
        .await?;
    let ledger = capture.ledger.as_ref().ok_or(E::Inventory)?;
    compare_inventory(ledger, a.positions, &next_opening.allocation)?;
    let next_capture = next
        .selection
        .inspect_durable_capture(next.store, next.ems_postgres_url)
        .await?;
    if next_capture
        .records
        .first()
        .is_none_or(|record| record.cursor.observed_at <= next_opening.inspected_at)
    {
        return Err(E::Interval.into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EvidenceWindow, RecoverySelection, SpotWalletBalance};
    use apexes_core::{AssetClass, OrderId, OrderStatus, Side, Venue};
    use apexes_oms::{CoherentCheckpointReceipt, ExchangeFill, OrderExecutionSnapshot};
    use apexes_strategy::checkpoint::{CheckpointReceipt, StateCursor, StateIdentity};
    use chrono::{DateTime, Utc};
    use rust_decimal_macros::dec;
    use std::collections::HashMap;
    use uuid::Uuid;

    fn at(n: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + n, 0).unwrap()
    }

    fn checkpoint(n: u64) -> CheckpointReceipt {
        CheckpointReceipt {
            identity: StateIdentity {
                strategy: "test".into(),
                revision: "1".into(),
                config_sha256: "a".repeat(64),
                scope: "scope".into(),
                payload_version: 1,
            },
            cursor: StateCursor {
                ordinal: n,
                observed_at: at(n as i64),
            },
            envelope_sha256: format!("{n:064x}"),
        }
    }

    fn selection(
        previous: CheckpointReceipt,
        next: CheckpointReceipt,
        owner: &str,
        parameter: i64,
        run: Uuid,
    ) -> CoherentRecoverySelection {
        CoherentRecoverySelection::new(
            RecoverySelection::new(
                owner.repeat(64),
                previous,
                HashMap::from([("factor".into(), parameter.into())]),
            )
            .unwrap(),
            CoherentCheckpointReceipt {
                version: 1,
                checkpoint: next,
                run_scope: run,
                last_intent: 0,
                fill_sequence: 0,
                terminal_sequence: 0,
                order_count: 0,
                fill_count: 0,
                journal_sha256: "b".repeat(64),
                capture_sha256: Some("c".repeat(64)),
            },
        )
        .unwrap()
    }

    #[test]
    fn exact_link_requires_predecessor_owner_config_and_distinct_scope() {
        let run = Uuid::new_v4();
        let a = selection(checkpoint(1), checkpoint(2), "a", 1, run);
        let next_run = Uuid::new_v4();
        let b = selection(checkpoint(2), checkpoint(3), "a", 1, next_run);
        compare_selection(&a, &b).unwrap();
        let mut different = checkpoint(2);
        different.envelope_sha256 = "f".repeat(64);
        for wrong in [
            selection(different, checkpoint(3), "a", 1, next_run),
            selection(checkpoint(2), checkpoint(3), "d", 1, next_run),
            selection(checkpoint(2), checkpoint(3), "a", 2, next_run),
            selection(checkpoint(2), checkpoint(3), "a", 1, run),
        ] {
            assert_eq!(
                compare_selection(&a, &wrong),
                Err(SuccessorContinuityError::Identity)
            );
        }
        assert_eq!(
            compare_selection(&b, &a),
            Err(SuccessorContinuityError::Identity)
        );
    }

    fn wallet(n: i64) -> WalletEvidence {
        WalletEvidence {
            window: EvidenceWindow {
                started_at: at(n),
                finished_at: at(n),
            },
            owner_fingerprint: "a".repeat(64),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            account_wide_complete: true,
            balances: HashMap::from([
                (
                    "BTC".into(),
                    SpotWalletBalance {
                        free: dec!(0),
                        locked: dec!(0),
                    },
                ),
                (
                    "USDT".into(),
                    SpotWalletBalance {
                        free: dec!(100),
                        locked: dec!(0),
                    },
                ),
            ]),
        }
    }

    #[test]
    fn wallet_link_requires_complete_equal_assets_and_ordered_intervals() {
        let a = wallet(3);
        let b = wallet(5);
        compare_wallets(&a, &b, at(4)).unwrap();
        assert_eq!(
            compare_wallets(&a, &b, at(6)),
            Err(SuccessorContinuityError::Interval)
        );
        let mut changed = b.clone();
        changed.balances.remove("BTC");
        assert_eq!(
            compare_wallets(&a, &changed, at(4)),
            Err(SuccessorContinuityError::Wallet)
        );
        let mut changed = b;
        changed.balances.get_mut("USDT").unwrap().free = dec!(99);
        assert_eq!(
            compare_wallets(&a, &changed, at(4)),
            Err(SuccessorContinuityError::Wallet)
        );
    }

    fn terminal(id: &str, trade: &str) -> TerminalOrderEvidence {
        TerminalOrderEvidence {
            client_order_id: OrderId(Uuid::new_v4()),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            symbol: "BTCUSDT".into(),
            side: Side::Buy,
            remote: RemoteTerminalEvidence::Found {
                exchange_order_id: id.into(),
                execution: OrderExecutionSnapshot {
                    status: OrderStatus::Filled,
                    executed_quantity: dec!(1),
                },
                trades: vec![ExchangeFill {
                    exchange_trade_id: trade.into(),
                    price: dec!(100),
                    quantity: dec!(1),
                    fee: dec!(0),
                    fee_currency: None,
                    filled_at: at(1),
                }],
            },
        }
    }

    #[test]
    fn duplicate_client_exchange_order_and_trade_identities_refused() {
        let a = terminal("1", "1");
        let b = terminal("2", "2");
        compare_execution(std::slice::from_ref(&a), std::slice::from_ref(&b)).unwrap();
        let mut duplicate_client = b;
        duplicate_client.client_order_id = a.client_order_id;
        for duplicate in [duplicate_client, terminal("1", "3"), terminal("3", "1")] {
            assert_eq!(
                compare_execution(std::slice::from_ref(&a), &[duplicate]),
                Err(SuccessorContinuityError::ExecutionOverlap)
            );
        }
    }

    #[test]
    fn inventory_requires_flat_and_exact_tracked_balances() {
        let a = selection(checkpoint(1), checkpoint(2), "a", 1, Uuid::new_v4());
        let mut positions = crate::ClosingPositionEvidence {
            version: 1,
            owner_fingerprint: "a".repeat(64),
            boundary: a.receipt().clone(),
            captured_at: at(3),
            symbols: vec!["BTCUSDT".into()],
            positions: vec![],
        };
        let balances = vec![("BTC".into(), dec!(0)), ("USDT".into(), dec!(100))];
        let ledger = apexes_backtest::ledger_evidence::LedgerEvidence {
            version: 1,
            scope: vec![(Venue::Binance, "BTCUSDT".into())],
            balances: balances.clone(),
            positions: vec![],
            realized_pnl: vec![],
            fee_valuation: None,
        };
        let mut allocation = crate::OpeningTrackedAssetEvidence {
            version: 1,
            predecessor: a.receipt().checkpoint.clone(),
            owner_fingerprint: "a".repeat(64),
            captured_at: at(5),
            balances,
        };
        compare_inventory(&ledger, &positions, &allocation).unwrap();
        allocation.balances.remove(0);
        assert_eq!(
            compare_inventory(&ledger, &positions, &allocation),
            Err(SuccessorContinuityError::Inventory)
        );
        allocation.balances = ledger.balances.clone();
        positions.positions.push(crate::ClosingSpotPosition {
            symbol: "BTCUSDT".into(),
            quantity: dec!(1),
            average_entry_price: dec!(100),
        });
        assert_eq!(
            compare_inventory(&ledger, &positions, &allocation),
            Err(SuccessorContinuityError::Inventory)
        );
    }
}
