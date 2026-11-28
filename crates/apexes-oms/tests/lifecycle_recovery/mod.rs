//! F04.04: interrupt the real managed journal at each process-local receipt
//! boundary. Dropping/reopening an OMS models loss of volatile state, not
//! an OS crash or durable strategy-state recovery.
use super::*;
use apexes_core::{FillId, OrderId};
use apexes_oms::IntentLink;
use apexes_strategy::IntentId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Cut {
    Committed,
    FillStaged,
    LedgerAcknowledged,
    FillNotified,
    TerminalStaged,
    TerminalNotified,
}

async fn finish(
    manager: &mut OrderManager,
    fixture: &Fixture,
    id: OrderId,
    trade_id: &str,
    offset: i64,
) -> FillId {
    fixture.gateway.set_exchange_state(id, OrderStatus::Filled);
    fixture
        .gateway
        .set_fills_for_order(id, vec![fill(trade_id, dec!(3), offset)]);
    manager.reconcile().await.unwrap();
    let store = OrderStore::new(&fixture.url).await.unwrap();
    let fills = store.get_fills_for_order(id).await.unwrap();
    assert_eq!(fills.len(), 1);
    fills[0].id
}

async fn interrupt(cut: Cut) {
    let fixture = Fixture::new().await;
    let mut manager = fixture.manager().await;
    let link = IntentLink {
        run_scope: Uuid::new_v4(),
        intent_id: IntentId(1),
    };
    let old = manager
        .submit_correlated_order(intent(), &HashMap::new(), &HashMap::new(), link)
        .await
        .unwrap();
    let old_fill = finish(&mut manager, &fixture, old, "1", 0).await;
    let terminal = manager.terminal_evidence(old).await.unwrap().unwrap();
    let sequence = terminal.required_fill_sequence;
    assert_eq!(terminal.intent_link, Some(link));
    assert!(sequence > 0);
    assert!(manager.ready_terminal_batch().await.is_empty());
    if cut >= Cut::FillStaged {
        let batch = manager.durable_fill_batch().await.unwrap();
        assert_eq!(
            batch.iter().map(|f| f.id).collect::<Vec<_>>(),
            vec![old_fill]
        );
    }
    if cut >= Cut::LedgerAcknowledged {
        manager.acknowledge_fill_batch(&[old_fill], true).await;
    }
    if cut >= Cut::FillNotified {
        manager.acknowledge_fill_notifications(&[old_fill], true);
    }
    if cut >= Cut::TerminalStaged {
        let batch = manager.ready_terminal_batch().await;
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].order.id, old);
        assert_eq!(
            manager.terminal_delivery_progress().unwrap().staged_count,
            1
        );
    }
    if cut >= Cut::TerminalNotified {
        manager.acknowledge_terminal_batch(&[old]);
    }
    let fill_progress = manager.fill_delivery_progress().unwrap();
    let terminal_progress = manager.terminal_delivery_progress().unwrap();
    assert_eq!(fill_progress.recovered_through, 0);
    assert_eq!(
        fill_progress.notifications_returned_through,
        (cut >= Cut::FillNotified).then_some(sequence)
    );
    assert_eq!(terminal_progress.recovered_through, 0);
    assert_eq!(
        terminal_progress.notifications_returned_through,
        (cut >= Cut::TerminalNotified).then_some(terminal.sequence)
    );
    assert!(!terminal_progress.failed && !terminal_progress.blocked_by_fill_delivery);
    drop(manager);
    // New manager, new health state, same durable owner. No old receipt is
    // restored or implied by the recovered inventory watermark.
    let mut store = OrderStore::new(&fixture.url).await.unwrap();
    store.bind_spot_inventory("fixture-owner").await.unwrap();
    let health = Arc::new(KillSwitch::managed());
    let mut manager = OrderManager::new(store, &fixture.registry)
        .await
        .unwrap()
        .with_kill_switch(health.clone());
    health.begin_reconciliation();
    manager.reconcile().await.unwrap();
    let recovered = manager.spot_inventory_fills().await.unwrap();
    assert_eq!(
        recovered.iter().map(|f| f.id).collect::<Vec<_>>(),
        vec![old_fill],
        "{cut:?}"
    );
    assert_eq!(recovered[0].quantity, dec!(3));
    health.startup_ready();
    let progress = manager.fill_delivery_progress().unwrap();
    assert_eq!(progress.recovered_through, sequence);
    assert_eq!(progress.ledger_acknowledged_through, sequence);
    assert_eq!(progress.notifications_returned_through, None);
    let progress = manager.terminal_delivery_progress().unwrap();
    assert_eq!(progress.recovered_through, terminal.sequence);
    assert_eq!(progress.notifications_returned_through, None);
    assert_eq!(progress.staged_count, 0);
    assert!(manager.durable_fill_batch().await.unwrap().is_empty());
    assert!(manager.ready_terminal_batch().await.is_empty());
    // Delayed duplicate push is not a new fill or terminal.
    manager
        .apply_exchange_update(
            old,
            terminal.order.exchange_order_id.clone(),
            OrderStatus::Filled,
        )
        .await
        .unwrap();
    assert!(manager.durable_fill_batch().await.unwrap().is_empty());
    assert!(manager.ready_terminal_batch().await.is_empty());
    let persisted = manager.terminal_evidence(old).await.unwrap().unwrap();
    assert_eq!(persisted.sequence, terminal.sequence);
    assert_eq!(persisted.intent_link, Some(link));
    // Same ordinal in a new scope is a different decision; the prior
    // marker and fill cannot be mistaken for it.
    let next_link = IntentLink {
        run_scope: Uuid::new_v4(),
        intent_id: IntentId(1),
    };
    let next = manager
        .submit_correlated_order(intent(), &HashMap::new(), &HashMap::new(), next_link)
        .await
        .unwrap();
    let next_fill = finish(&mut manager, &fixture, next, "2", 1).await;
    let batch = manager.durable_fill_batch().await.unwrap();
    assert_eq!(
        batch.iter().map(|f| f.id).collect::<Vec<_>>(),
        vec![next_fill]
    );
    manager.acknowledge_fill_batch(&[next_fill], true).await;
    assert!(manager.ready_terminal_batch().await.is_empty());
    manager.acknowledge_fill_notifications(&[next_fill], true);
    let batch = manager.ready_terminal_batch().await;
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].order.id, next);
    assert_eq!(batch[0].intent_link, Some(next_link));
    assert!(batch[0].sequence > terminal.sequence);
    manager.acknowledge_terminal_batch(&[next]);
    assert_eq!(
        manager
            .terminal_delivery_progress()
            .unwrap()
            .notifications_returned_through,
        Some(batch[0].sequence)
    );
    assert!(manager.ready_terminal_batch().await.is_empty());
    assert!(!health.is_engaged());
    drop(manager);
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore]
async fn restart_at_every_lifecycle_receipt_boundary_preserves_evidence_without_replay() {
    for cut in [
        Cut::Committed,
        Cut::FillStaged,
        Cut::LedgerAcknowledged,
        Cut::FillNotified,
        Cut::TerminalStaged,
        Cut::TerminalNotified,
    ] {
        interrupt(cut).await;
    }
}

#[tokio::test]
#[ignore]
async fn running_manager_cannot_rebaseline_over_pending_or_failed_receipts() {
    let fixture = Fixture::new().await;
    let mut manager = fixture.manager().await;
    let id = submit(&mut manager).await;
    let fill_id = finish(&mut manager, &fixture, id, "1", 0).await;
    let batch = manager.durable_fill_batch().await.unwrap();
    assert_eq!(batch[0].id, fill_id);
    manager.acknowledge_fill_batch(&[fill_id], true).await;
    manager.acknowledge_fill_notifications(&[fill_id], true);
    let terminals = manager.ready_terminal_batch().await;
    assert_eq!(terminals.len(), 1);
    let before = manager.terminal_delivery_progress().unwrap();
    assert!(manager.spot_inventory_fills().await.is_err());
    assert_eq!(manager.terminal_delivery_progress().unwrap(), before);
    assert!(fixture.health.is_engaged());
    manager.acknowledge_terminal_batch(&[OrderId(Uuid::new_v4())]);
    let failed = manager.terminal_delivery_progress().unwrap();
    assert!(failed.failed);
    assert!(manager.spot_inventory_fills().await.is_err());
    assert_eq!(manager.terminal_delivery_progress().unwrap(), failed);
    assert!(manager.ready_terminal_batch().await.is_empty());
    drop(manager);
    fixture.cleanup().await;
}
