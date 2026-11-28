//! Real unified-loop capture and atomic publication of current-run callback
//! progress. Exchange and market transport are mocked; PostgreSQL is real.
use super::*;
use apexes_live_trading::run_live_trading_with_checkpoint_on_stop;
use apexes_strategy::{
    FillOutcome, IntentId, OrderOutcome, OrderOutcomeKind, TerminalKind, TerminalOutcome,
};

struct ExecutionProbe {
    state: Probe,
    submitted: bool,
    stop_on_attempt: bool,
    stop: Option<oneshot::Sender<()>>,
}

impl ExecutionProbe {
    fn new(stop: oneshot::Sender<()>, stop_on_attempt: bool) -> Self {
        Self {
            state: Probe::new(0),
            submitted: false,
            stop_on_attempt,
            stop: Some(stop),
        }
    }

    fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

impl Strategy for ExecutionProbe {
    fn on_market_event(&mut self, event: &MarketEvent, ctx: &StrategyContext) -> Vec<OrderIntent> {
        self.state.on_market_event(event, ctx);
        if matches!(event, MarketEvent::Trade(_)) && !self.submitted {
            self.submitted = true;
            vec![callback_probe_intent(false)]
        } else {
            vec![]
        }
    }

    fn on_order_outcome(&mut self, outcome: &OrderOutcome<'_>) {
        assert_eq!(outcome.intent_id, Some(IntentId(1)));
        assert!(
            matches!(outcome.kind, OrderOutcomeKind::Returned { .. }),
            "{:?}",
            outcome.kind
        );
        self.state.value += 10;
        if self.stop_on_attempt {
            self.stop();
        }
    }

    fn on_fill(&mut self, outcome: &FillOutcome<'_>) {
        assert_eq!(outcome.intent_id, Some(IntentId(1)));
        self.state.value += 100;
    }

    fn on_order_terminal(&mut self, outcome: &TerminalOutcome<'_>) {
        assert_eq!(outcome.intent_id, Some(IntentId(1)));
        assert!(matches!(outcome.kind, TerminalKind::Live { .. }));
        self.state.value += 1000;
        self.stop();
    }

    fn checkpoint_schema_version(&self) -> Option<u32> {
        self.state.checkpoint_schema_version()
    }

    fn checkpoint_payload(&self) -> Result<String, CheckpointError> {
        self.state.checkpoint_payload()
    }

    fn restore_checkpoint(&mut self, state: &ValidatedState<'_>) -> Result<(), CheckpointError> {
        self.state.restore_checkpoint(state)
    }
}

fn execution_startup(
    selection: RecoverySelection,
    stop: oneshot::Sender<()>,
    stop_on_attempt: bool,
) -> StrategyStartup<ExecutionProbe> {
    StrategyStartup::Restore {
        active: ExecutionProbe {
            state: Probe::new(999),
            submitted: false,
            stop_on_attempt: false,
            stop: None,
        },
        candidate: ExecutionProbe::new(stop, stop_on_attempt),
        selection: Box::new(selection),
    }
}

async fn assert_old_head(fixture: &Fixture, selection: &RecoverySelection) {
    let mut store = fixture.store().await;
    let bytes = store
        .load_strategy_checkpoint(Some(selection.receipt()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        bytes,
        capture(&Probe::new(42), identity(), cursor()).unwrap()
    );
    drop(store);
    let pool = sqlx::PgPool::connect(&fixture.url).await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM strategy_checkpoint_boundary")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn completed_direct_callbacks_and_checkpoint_commit_as_one_exact_boundary() {
    let fixture = Fixture::new().await;
    let selection = fixture.seed().await;
    let gateway = Arc::new(MockExecutionGateway::new());
    let registry = registry_with(
        gateway.clone(),
        Arc::new(MockMarketDataProvider {
            events: sample_events(),
            keep_open: true,
        }),
    );
    let (manager, config, health) = fixture.manager(&registry).await;
    let pool = sqlx::PgPool::connect(&fixture.url).await.unwrap();
    let driver_pool = pool.clone();
    let (updates, stream) = futures::channel::mpsc::unbounded();
    let driver = tokio::spawn(async move {
        let (id, exchange_id, _) =
            wait_for_callback_order(&driver_pool, uuid::Uuid::nil(), false).await;
        // Preserve an independent copy of the synthetic exchange fixture;
        // do not reconstruct the comparison evidence from the database.
        let trade = ExchangeFill {
            exchange_trade_id: "coherent-fill".into(),
            price: dec!(100),
            quantity: dec!(1),
            fee: dec!(0),
            fee_currency: None,
            filled_at: Utc::now(),
        };
        gateway.set_fills_for_order(id, vec![trade.clone()]);
        gateway.set_exchange_state(id, OrderStatus::Filled);
        updates
            .unbounded_send(ExchangeUpdate {
                client_order_id: id,
                exchange_order_id: exchange_id.clone(),
                status: OrderStatus::Filled,
            })
            .unwrap();
        (
            id,
            apexes_oms::TerminalOrderEvidence {
                client_order_id: id,
                venue: Venue::Binance,
                asset_class: AssetClass::Spot,
                symbol: "BTCUSDT".into(),
                side: Side::Buy,
                remote: apexes_oms::RemoteTerminalEvidence::Found {
                    exchange_order_id: exchange_id.unwrap(),
                    execution: apexes_oms::OrderExecutionSnapshot {
                        status: OrderStatus::Filled,
                        executed_quantity: dec!(1),
                    },
                    trades: vec![trade],
                },
            },
        )
    });
    let (stop, receiver) = oneshot::channel();
    let (report, receipt) = tokio::time::timeout(
        Duration::from_secs(10),
        run_live_trading_with_checkpoint_on_stop(
            config,
            &registry,
            execution_startup(selection.clone(), stop, false),
            manager,
            stream,
            receiver,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let (id, evidence) = driver.await.unwrap();
    assert_eq!(report.trades.len(), 1);
    assert_eq!(report.trades[0].order_id, id);
    assert!(!health.is_engaged(), "{:?}", health.reason());
    assert_eq!(receipt.last_intent, 1);
    assert_eq!((receipt.order_count, receipt.fill_count), (1, 1));
    assert!(receipt.fill_sequence > 0 && receipt.terminal_sequence > 0);
    assert_eq!(receipt.checkpoint.cursor.ordinal, cursor().ordinal + 1);
    assert_eq!(
        receipt.checkpoint.cursor.observed_at,
        cursor().observed_at + chrono::TimeDelta::seconds(1)
    );
    assert_eq!(receipt.checkpoint.identity, identity());
    let (stored_scope, stored_terminal): (uuid::Uuid, i64) =
        sqlx::query_as("SELECT intent_scope,terminal_delivery_seq FROM orders WHERE id=$1")
            .bind(id.0)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(stored_scope, receipt.run_scope);
    assert_eq!(stored_terminal, receipt.terminal_sequence);
    let stored_fill: i64 = sqlx::query_scalar("SELECT delivery_seq FROM fills WHERE order_id=$1")
        .bind(id.0)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(stored_fill, receipt.fill_sequence);

    let mut store = fixture.store().await;
    store.verify_coherent_boundary(&receipt).await.unwrap();
    let bytes = store
        .load_strategy_checkpoint(Some(&receipt.checkpoint))
        .await
        .unwrap()
        .unwrap();
    let mut restored = Probe::new(0);
    apexes_strategy::checkpoint::restore(
        &mut restored,
        Probe::new(0),
        &bytes,
        &identity(),
        receipt.checkpoint.cursor,
    )
    .unwrap();
    // market decision + direct attempt + fill + terminal callback.
    assert_eq!(restored.value, 42 + 1 + 10 + 100 + 1000);
    assert!(
        store
            .load_strategy_checkpoint(Some(selection.receipt()))
            .await
            .is_err()
    );
    // An exact plain retry cannot overwrite/detach the coherent evidence.
    assert!(
        store
            .publish_strategy_checkpoint(
                Some(selection.receipt()),
                &identity(),
                receipt.checkpoint.cursor,
                &bytes
            )
            .await
            .is_err()
    );
    let mut false_receipt = receipt.clone();
    false_receipt.last_intent += 1;
    assert!(
        store
            .verify_coherent_boundary(&false_receipt)
            .await
            .is_err()
    );
    // Select the actual acknowledged coherent receipt independently
    // of the database and verify a post-execution preview after reopening.
    let coherent =
        apexes_live_trading::CoherentRecoverySelection::new(selection.clone(), receipt.clone())
            .unwrap();
    let dir = std::env::temp_dir().join(format!(
        "apexes-coherent-selection-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("coherent.json");
    coherent.write_new(&path).unwrap();
    let coherent = apexes_live_trading::CoherentRecoverySelection::read(&path).unwrap();
    // Independently supplied synthetic coverage for the offline preview.
    // No live exchange lookup is performed by the inspection API.
    let started = Utc::now();
    let end = started + chrono::TimeDelta::seconds(1);
    let inspected = end + chrono::TimeDelta::seconds(1);
    let window = apexes_live_trading::RecoveryEvidenceWindow {
        terminal_window: apexes_live_trading::EvidenceWindow {
            started_at: started,
            finished_at: end,
        },
        terminal_owner_fingerprint: OWNER_SHA256.into(),
        terminal_evidence_complete: true,
        terminal_orders: vec![evidence.clone()],
        open_orders: apexes_live_trading::OpenOrderCoverage {
            window: apexes_live_trading::EvidenceWindow {
                started_at: end,
                finished_at: inspected,
            },
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            owner_fingerprint: OWNER_SHA256.into(),
            account_wide_complete: true,
            open_order_count: 0,
            foreign_order_count: 0,
        },
    };
    coherent
        .inspect_evidence_window(&mut store, &fixture.url, &window, inspected)
        .await
        .unwrap();
    assert!(matches!(
        coherent
            .inspect_evidence_window(
                &mut store,
                &fixture.url,
                &window,
                inspected + chrono::TimeDelta::seconds(61)
            )
            .await,
        Err(
            apexes_live_trading::CoherentInspectionError::EvidenceWindow(
                apexes_live_trading::EvidenceWindowError::Stale
            )
        )
    ));
    let mut incomplete = window.clone();
    incomplete.open_orders.account_wide_complete = false;
    assert!(
        coherent
            .inspect_evidence_window(&mut store, &fixture.url, &incomplete, inspected)
            .await
            .is_err()
    );
    let mut foreign = window.clone();
    foreign.open_orders.foreign_order_count = 1;
    assert!(
        coherent
            .inspect_evidence_window(&mut store, &fixture.url, &foreign, inspected)
            .await
            .is_err()
    );

    coherent
        .inspect_terminal_evidence(&mut store, &fixture.url, std::slice::from_ref(&evidence))
        .await
        .unwrap();
    let mut revised = evidence.clone();
    if let apexes_oms::RemoteTerminalEvidence::Found { trades, .. } = &mut revised.remote {
        trades[0].fee = dec!(0.01);
    }
    assert!(matches!(
        coherent
            .inspect_terminal_evidence(&mut store, &fixture.url, &[revised])
            .await,
        Err(apexes_live_trading::CoherentInspectionError::Terminal(
            apexes_oms::TerminalAuditError::Trade
        ))
    ));
    assert!(matches!(
        coherent
            .inspect_terminal_evidence(&mut store, &fixture.url, &[])
            .await,
        Err(apexes_live_trading::CoherentInspectionError::Terminal(
            apexes_oms::TerminalAuditError::MissingOrder
        ))
    ));

    let wallet = HashMap::from([
        (
            "BTC".into(),
            apexes_live_trading::spot_recovery::SpotWalletBalance {
                free: dec!(5),
                locked: dec!(0),
            },
        ),
        (
            "USDT".into(),
            apexes_live_trading::spot_recovery::SpotWalletBalance {
                free: dec!(99900),
                locked: dec!(0),
            },
        ),
    ]);
    let instruments = base_config().instruments;
    let wallet_evidence = apexes_live_trading::WalletEvidence {
        window: apexes_live_trading::EvidenceWindow {
            started_at: inspected,
            finished_at: inspected + chrono::TimeDelta::seconds(1),
        },
        owner_fingerprint: OWNER_SHA256.into(),
        venue: Venue::Binance,
        asset_class: AssetClass::Spot,
        account_wide_complete: true,
        balances: wallet.clone(),
    };
    let wallet_at = wallet_evidence.window.finished_at;
    let qualified = coherent
        .inspect_wallet_continuity(
            &mut store,
            &fixture.url,
            (Probe::new(999), Probe::new(0)),
            &instruments,
            (&window, &wallet_evidence, wallet_at),
        )
        .await
        .unwrap();
    assert_eq!(qualified.owned_positions["BTCUSDT"].quantity, dec!(1));
    assert_eq!(qualified.wallet_balances["BTC"].free, dec!(5));
    let mut missing = wallet_evidence.clone();
    missing.balances.remove("USDT");
    assert!(matches!(
        coherent
            .inspect_wallet_continuity(
                &mut store,
                &fixture.url,
                (Probe::new(0), Probe::new(0)),
                &instruments,
                (&window, &missing, wallet_at)
            )
            .await,
        Err(apexes_live_trading::CoherentInspectionError::Wallet(
            apexes_live_trading::WalletEvidenceError::MissingAsset
        ))
    ));
    let mut short = wallet_evidence.clone();
    short.balances.get_mut("BTC").unwrap().free = dec!(0.5);
    assert!(matches!(
        coherent
            .inspect_wallet_continuity(
                &mut store,
                &fixture.url,
                (Probe::new(0), Probe::new(0)),
                &instruments,
                (&window, &short, wallet_at)
            )
            .await,
        Err(apexes_live_trading::CoherentInspectionError::Inventory(_))
    ));
    let candidate = Probe::new(0);
    let observed = candidate.observed.clone();
    let preview = coherent
        .inspect(
            &mut store,
            &fixture.url,
            Probe::new(999),
            candidate,
            &instruments,
            &wallet,
        )
        .await
        .unwrap();
    assert_eq!(preview.cursor, receipt.checkpoint.cursor);
    assert_eq!(
        (
            preview.historical_order_count,
            preview.historical_fill_count
        ),
        (1, 1)
    );
    assert_eq!(preview.owned_positions["BTCUSDT"].quantity, dec!(1));
    assert_eq!(
        preview.owned_positions["BTCUSDT"].average_entry_price,
        dec!(100)
    );
    assert_eq!(preview.wallet_balances["BTC"].free, dec!(5));
    assert!(observed.lock().unwrap().is_empty());
    assert_eq!(fixture.claim_count().await, 1);

    let mut short = wallet.clone();
    short.get_mut("BTC").unwrap().free = dec!(0.5);
    assert!(matches!(
        coherent
            .inspect(
                &mut store,
                &fixture.url,
                Probe::new(0),
                Probe::new(0),
                &instruments,
                &short
            )
            .await,
        Err(apexes_live_trading::CoherentInspectionError::Inventory(_))
    ));
    let mut invalid_candidate = Probe::new(0);
    invalid_candidate.fail_restore = true;
    assert!(matches!(
        coherent
            .inspect(
                &mut store,
                &fixture.url,
                Probe::new(0),
                invalid_candidate,
                &instruments,
                &wallet
            )
            .await,
        Err(apexes_live_trading::CoherentInspectionError::Restore(_))
    ));
    assert!(
        coherent
            .inspect(
                &mut store,
                &fixture.url,
                Probe::new(0),
                Probe::new(0),
                &[],
                &wallet
            )
            .await
            .is_err()
    );
    assert!(
        coherent
            .inspect(
                &mut store,
                "different database selection",
                Probe::new(0),
                Probe::new(0),
                &instruments,
                &wallet
            )
            .await
            .is_err()
    );
    let wrong_owner =
        RecoverySelection::new("0".repeat(64), selection.receipt().clone(), HashMap::new())
            .unwrap();
    let wrong =
        apexes_live_trading::CoherentRecoverySelection::new(wrong_owner, receipt.clone()).unwrap();
    assert!(matches!(
        wrong
            .inspect(
                &mut store,
                &fixture.url,
                Probe::new(0),
                Probe::new(0),
                &instruments,
                &wallet
            )
            .await,
        Err(apexes_live_trading::CoherentInspectionError::Store(
            apexes_oms::CheckpointStoreError::Owner
        ))
    ));
    let mut other_receipt = selection.receipt().clone();
    other_receipt.envelope_sha256 = "0".repeat(64);
    let wrong_predecessor =
        RecoverySelection::new(OWNER_SHA256.into(), other_receipt, HashMap::new()).unwrap();
    let wrong =
        apexes_live_trading::CoherentRecoverySelection::new(wrong_predecessor, receipt.clone())
            .unwrap();
    assert!(matches!(
        wrong
            .inspect(
                &mut store,
                &fixture.url,
                Probe::new(0),
                Probe::new(0),
                &instruments,
                &wallet
            )
            .await,
        Err(apexes_live_trading::CoherentInspectionError::Store(
            apexes_oms::CheckpointStoreError::Conflict
        ))
    ));
    // A preview never enables a repeatable live startup.
    std::fs::remove_dir_all(dir).unwrap();
    // Modification with unchanged row counts and marker watermarks must
    // still invalidate the exact witness.
    sqlx::query("UPDATE orders SET updated_at=updated_at+interval '1 second' WHERE id=$1")
        .bind(id.0)
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store.verify_coherent_boundary(&receipt).await,
        Err(apexes_oms::CheckpointStoreError::JournalChanged)
    ));
    assert!(
        coherent
            .inspect(
                &mut store,
                &fixture.url,
                Probe::new(0),
                Probe::new(0),
                &instruments,
                &wallet
            )
            .await
            .is_err()
    );
    assert!(
        coherent
            .inspect_terminal_evidence(&mut store, &fixture.url, &[evidence])
            .await
            .is_err()
    );
    drop(store);
    pool.close().await;
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn working_order_refuses_coherent_publication_and_preserves_predecessor() {
    let fixture = Fixture::new().await;
    let selection = fixture.seed().await;
    let reg = registry();
    let (manager, config, health) = fixture.manager(&reg).await;
    let (stop, receiver) = oneshot::channel();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        run_live_trading_with_checkpoint_on_stop(
            config,
            &reg,
            execution_startup(selection.clone(), stop, true),
            manager,
            futures::stream::empty(),
            receiver,
        ),
    )
    .await
    .unwrap();
    assert!(result.is_err());
    assert!(health.is_engaged());
    let pool = sqlx::PgPool::connect(&fixture.url).await.unwrap();
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM orders WHERE status='New' AND submission_state='Accepted'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 1);
    pool.close().await;
    assert_old_head(&fixture, &selection).await;
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn commit_failure_rolls_back_both_head_and_boundary_receipt() {
    let fixture = Fixture::new().await;
    let selection = fixture.seed().await;
    let pool = sqlx::PgPool::connect(&fixture.url).await.unwrap();
    sqlx::query("CREATE FUNCTION refuse_boundary_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected boundary commit failure'; END; $$")
        .execute(&pool).await.unwrap();
    sqlx::query("CREATE CONSTRAINT TRIGGER boundary_commit_failure AFTER INSERT ON strategy_checkpoint_boundary DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION refuse_boundary_commit()")
        .execute(&pool).await.unwrap();
    let reg = registry();
    let (manager, config, health) = fixture.manager(&reg).await;
    let candidate = Probe::new(0);
    let observed = candidate.observed.clone();
    let (stop, receiver) = oneshot::channel();
    let driver = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let _ = stop.send(());
    });
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        run_live_trading_with_checkpoint_on_stop(
            config,
            &reg,
            restore_startup(selection.clone(), candidate),
            manager,
            futures::stream::empty(),
            receiver,
        ),
    )
    .await
    .unwrap();
    driver.abort();
    assert!(
        matches!(result, Err(ApexesError::InvalidBacktestConfig { ref reason })
        if reason.contains("checkpoint database operation failed"))
    );
    assert!(health.is_engaged());
    assert_eq!(*observed.lock().unwrap(), vec![43]);
    assert_old_head(&fixture, &selection).await;
    pool.close().await;
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn no_completed_market_observation_does_not_publish_a_checkpoint() {
    let fixture = Fixture::new().await;
    let selection = fixture.seed().await;
    let reg = registry_with(
        Arc::new(MockExecutionGateway::new()),
        Arc::new(MockMarketDataProvider {
            events: vec![sample_events().remove(0)],
            keep_open: true,
        }),
    );
    let (manager, config, health) = fixture.manager(&reg).await;
    let (stop, receiver) = oneshot::channel();
    let driver = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let _ = stop.send(());
    });
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        run_live_trading_with_checkpoint_on_stop(
            config,
            &reg,
            restore_startup(selection.clone(), Probe::new(0)),
            manager,
            futures::stream::empty(),
            receiver,
        ),
    )
    .await
    .unwrap();
    driver.abort();
    assert!(result.is_err());
    assert!(health.is_engaged());
    assert_old_head(&fixture, &selection).await;
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn incomplete_fill_and_terminal_callback_prefixes_never_publish() {
    // Each interruption uses a separate owner and one-time claim. An
    // incomplete prefix may not be promoted merely by inventing an envelope.
    for interruption in 0..4 {
        let fixture = Fixture::new().await;
        let selection = fixture.seed().await;
        let gateway = Arc::new(MockExecutionGateway::new());
        let registry = registry_with(
            gateway.clone(),
            Arc::new(MockMarketDataProvider {
                events: vec![],
                keep_open: true,
            }),
        );
        let (mut manager, config, health) = fixture.manager(&registry).await;
        manager
            .claim_strategy_startup(selection.receipt(), OWNER_SHA256, &config.ems_postgres_url)
            .await
            .unwrap();
        health.startup_ready();
        let scope = uuid::Uuid::new_v4();
        let id = manager
            .submit_correlated_order(
                callback_probe_intent(false),
                &HashMap::new(),
                &config.initial_balances,
                apexes_oms::IntentLink {
                    run_scope: scope,
                    intent_id: IntentId(1),
                },
            )
            .await
            .unwrap();
        let exchange_id = manager.open_orders()[&id].exchange_order_id.clone();
        let (updates, _stream) = futures::channel::mpsc::unbounded();
        publish_mock_fill(
            &gateway,
            &updates,
            id,
            exchange_id.clone(),
            "interrupted-fill",
        );
        manager
            .apply_exchange_update(id, exchange_id, OrderStatus::Filled)
            .await
            .unwrap();
        assert!(manager.open_orders().is_empty());
        let staged = manager.durable_fill_batch().await.unwrap();
        assert_eq!(staged.len(), 1);
        let ids = [staged[0].id];
        if interruption >= 1 {
            manager.acknowledge_fill_batch(&ids, true).await;
        }
        if interruption >= 2 {
            manager.acknowledge_fill_notifications(&ids, true);
        }
        if interruption >= 3 {
            let batch = manager.ready_terminal_batch().await;
            assert_eq!(batch.len(), 1);
        }
        assert!(!health.is_engaged());
        let next = StateCursor {
            ordinal: cursor().ordinal + 1,
            observed_at: cursor().observed_at + chrono::TimeDelta::seconds(1),
        };
        let envelope = capture(&Probe::new(43), identity(), next).unwrap();
        assert!(
            matches!(
                manager
                    .publish_coherent_strategy_checkpoint(
                        selection.receipt(),
                        next,
                        scope,
                        1,
                        &envelope
                    )
                    .await,
                Err(apexes_oms::CheckpointStoreError::Boundary)
            ),
            "interruption {interruption}"
        );
        assert!(health.is_engaged());
        drop(manager);
        assert_old_head(&fixture, &selection).await;
        fixture.cleanup().await;
    }
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn zero_fill_terminal_callback_is_included_in_the_coherent_boundary() {
    let fixture = Fixture::new().await;
    let selection = fixture.seed().await;
    let gateway = Arc::new(MockExecutionGateway::new());
    let reg = registry_with(
        gateway.clone(),
        Arc::new(MockMarketDataProvider {
            events: sample_events(),
            keep_open: true,
        }),
    );
    let (manager, config, health) = fixture.manager(&reg).await;
    let pool = sqlx::PgPool::connect(&fixture.url).await.unwrap();
    let (updates, stream) = futures::channel::mpsc::unbounded();
    let driver = tokio::spawn(async move {
        let (id, exchange_id, _) = wait_for_callback_order(&pool, uuid::Uuid::nil(), false).await;
        gateway.set_exchange_state(id, OrderStatus::Cancelled);
        updates
            .unbounded_send(ExchangeUpdate {
                client_order_id: id,
                exchange_order_id: exchange_id,
                status: OrderStatus::Cancelled,
            })
            .unwrap();
    });
    let (stop, receiver) = oneshot::channel();
    let (report, receipt) = tokio::time::timeout(
        Duration::from_secs(10),
        run_live_trading_with_checkpoint_on_stop(
            config,
            &reg,
            execution_startup(selection, stop, false),
            manager,
            stream,
            receiver,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    driver.await.unwrap();
    assert!(report.trades.is_empty());
    assert!(!health.is_engaged());
    assert_eq!(
        (
            receipt.order_count,
            receipt.fill_count,
            receipt.fill_sequence
        ),
        (1, 0, 0)
    );
    assert!(receipt.terminal_sequence > 0);
    let mut store = fixture.store().await;
    store.verify_coherent_boundary(&receipt).await.unwrap();
    let bytes = store
        .load_strategy_checkpoint(Some(&receipt.checkpoint))
        .await
        .unwrap()
        .unwrap();
    let mut restored = Probe::new(0);
    apexes_strategy::checkpoint::restore(
        &mut restored,
        Probe::new(0),
        &bytes,
        &identity(),
        receipt.checkpoint.cursor,
    )
    .unwrap();
    assert_eq!(restored.value, 42 + 1 + 10 + 1000);
    drop(store);
    fixture.cleanup().await;
}
