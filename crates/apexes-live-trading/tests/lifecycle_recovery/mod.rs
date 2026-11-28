//! F04.04 recovery qualification through the real startup recovery and
//! unified live loop; only the exchange and market transport are mocked.
use super::*;
use apexes_live_trading::{
    ExchangeSnapshot, SpotWalletBalance, recover_live_state, recover_spot_positions,
};
use apexes_strategy::{FillOutcome, IntentId, LiveTerminalStatus, TerminalKind, TerminalOutcome};

struct RecoveredProbe {
    submitted: bool,
    observed: Arc<std::sync::Mutex<Vec<(OrderId, &'static str)>>>,
    stop: Option<oneshot::Sender<()>>,
}

impl Strategy for RecoveredProbe {
    fn on_market_event(&mut self, event: &MarketEvent, ctx: &StrategyContext) -> Vec<OrderIntent> {
        if self.submitted || !matches!(event, MarketEvent::Trade(_)) {
            return vec![];
        }
        assert_eq!(ctx.positions["BTCUSDT"].quantity, dec!(0.99));
        assert_eq!(
            ctx.positions["BTCUSDT"].average_entry_price,
            dec!(100) / dec!(0.99)
        );
        assert_eq!(ctx.balances["BTC"], dec!(5.99));
        assert_eq!(ctx.balances["USDT"], dec!(99900));
        self.submitted = true;
        vec![callback_probe_intent(false)]
    }

    fn on_fill(&mut self, outcome: &FillOutcome<'_>) {
        assert_eq!(outcome.intent_id, Some(IntentId(1)));
        self.observed
            .lock()
            .unwrap()
            .push((outcome.fill.order_id, "fill"));
    }

    fn on_order_terminal(&mut self, outcome: &TerminalOutcome<'_>) {
        let TerminalKind::Live {
            order,
            status,
            parent_id,
            dispatch_id,
        } = outcome.kind
        else {
            panic!("unexpected terminal kind");
        };
        assert_eq!(outcome.intent_id, Some(IntentId(1)));
        assert_eq!(status, LiveTerminalStatus::Filled);
        assert_eq!((parent_id, dispatch_id), (None, None));
        let mut observed = self.observed.lock().unwrap();
        assert_eq!(*observed, vec![(order.id, "fill")]);
        observed.push((order.id, "terminal"));
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn restarted_live_loop_seeds_owned_inventory_without_replaying_old_callbacks() {
    let admin = sqlx::PgPool::connect(POSTGRES_URL).await.unwrap();
    let schema = format!("live_recovery_test_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let url = format!("{POSTGRES_URL}?options[search_path]={schema}");
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let gateway = Arc::new(MockExecutionGateway::new());
    let registry = registry_with(gateway.clone(), Arc::new(SteadyFeed));
    let mut store = OrderStore::new(&url).await.unwrap();
    store
        .bind_spot_inventory("managed-lifecycle-recovery")
        .await
        .unwrap();
    let health = Arc::new(apexes_oms::KillSwitch::managed());
    let mut manager = OrderManager::new(store, &registry)
        .await
        .unwrap()
        .with_kill_switch(health.clone());
    manager.spot_inventory_fills().await.unwrap();
    health.begin_reconciliation();
    health.startup_ready();
    let old_scope = uuid::Uuid::new_v4();
    let old_order = manager
        .submit_correlated_order(
            callback_probe_intent(false),
            &HashMap::new(),
            &HashMap::new(),
            apexes_oms::IntentLink {
                run_scope: old_scope,
                intent_id: IntentId(1),
            },
        )
        .await
        .unwrap();
    let old_exchange = manager.open_orders()[&old_order].exchange_order_id.clone();
    let old_execution = ExchangeFill {
        exchange_trade_id: "old-recovery".into(),
        price: dec!(100),
        quantity: dec!(1),
        fee: dec!(0.01),
        fee_currency: Some("BTC".into()),
        filled_at: Utc::now() - chrono::Duration::seconds(1),
    };
    gateway.set_fills_for_order(old_order, vec![old_execution]);
    gateway.set_exchange_state(old_order, OrderStatus::Filled);
    // Lose the process after exchange execution but before local reconciliation.
    drop(manager);
    let mut store = OrderStore::new(&url).await.unwrap();
    store
        .bind_spot_inventory("managed-lifecycle-recovery")
        .await
        .unwrap();
    let health = Arc::new(apexes_oms::KillSwitch::managed());
    let mut manager = OrderManager::new(store, &registry)
        .await
        .unwrap()
        .with_kill_switch(health.clone());
    health.begin_reconciliation();
    let wallet = HashMap::from([
        (
            "BTC".into(),
            SpotWalletBalance {
                free: dec!(5.99),
                locked: dec!(0),
            },
        ),
        (
            "USDT".into(),
            SpotWalletBalance {
                free: dec!(99900),
                locked: dec!(0),
            },
        ),
    ]);
    let recovered = recover_live_state(&mut manager, &[AssetClass::Spot], async {
        Ok(ExchangeSnapshot {
            balances: wallet
                .iter()
                .map(|(asset, amount): (&String, &SpotWalletBalance)| {
                    (asset.clone(), amount.free + amount.locked)
                })
                .collect(),
            positions: HashMap::new(),
        })
    })
    .await
    .unwrap();
    assert_eq!(recovered.reconciled_orders, 1);
    let historical = manager.spot_inventory_fills().await.unwrap();
    assert_eq!(historical.len(), 1);
    assert_eq!(historical[0].order_id, old_order);
    let marker = manager.terminal_evidence(old_order).await.unwrap().unwrap();
    assert_eq!(
        manager
            .terminal_delivery_progress()
            .unwrap()
            .recovered_through,
        marker.sequence
    );
    assert_eq!(
        manager
            .terminal_delivery_progress()
            .unwrap()
            .notifications_returned_through,
        None
    );
    assert_eq!(
        manager
            .fill_delivery_progress()
            .unwrap()
            .notifications_returned_through,
        None
    );
    let mut config = base_config();
    config.initial_positions = recover_spot_positions(
        &config.instruments,
        &historical,
        &wallet,
        !manager.open_orders().is_empty(),
    )
    .unwrap();
    config.initial_balances = recovered.snapshot.balances;
    config.ems_postgres_url = url.clone();
    config.kill_switch = Some(health.clone());
    config.initial_prices.insert("BTCUSDT".into(), dec!(100));
    let (updates, stream) = futures::channel::mpsc::unbounded();
    updates
        .unbounded_send(ExchangeUpdate {
            client_order_id: old_order,
            exchange_order_id: old_exchange,
            status: OrderStatus::Filled,
        })
        .unwrap();
    let driver_pool = pool.clone();
    let driver = tokio::spawn(async move {
        let next = wait_for_callback_order(&driver_pool, old_scope, false).await;
        publish_mock_fill(&gateway, &updates, next.0, next.1, "new-recovery");
        next.0
    });
    let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (stop, receiver) = oneshot::channel();
    let report = tokio::time::timeout(
        Duration::from_secs(12),
        run_live_trading(
            config,
            &registry,
            RecoveredProbe {
                submitted: false,
                observed: observed.clone(),
                stop: Some(stop),
            },
            manager,
            stream,
            receiver,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let next = driver.await.unwrap();
    assert_ne!(next, old_order);
    assert_eq!(
        *observed.lock().unwrap(),
        vec![(next, "fill"), (next, "terminal")]
    );
    assert_eq!(report.trades.len(), 1);
    assert_eq!(report.trades[0].order_id, next);
    assert!(!health.is_engaged(), "{:?}", health.reason());
    pool.close().await;
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
}
