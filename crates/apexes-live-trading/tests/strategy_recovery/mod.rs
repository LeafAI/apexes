//! F04.05: exercise the actual live runner with a real PostgreSQL head
//! and independently persisted receipt. All exchange/market I/O is mocked.
use super::*;
use apexes_live_trading::{
    ExchangeSnapshot, RecoverySelection, StrategyStartup, recover_live_state,
    run_live_trading_with_startup,
};
use apexes_strategy::checkpoint::{
    CheckpointError, StateCursor, StateIdentity, ValidatedState, capture,
};

const OWNER: &str = "strategy-restore-owner";
const OWNER_SHA256: &str = "36683112f9f1a6b031c09636c206edda34a57d3c6ac8534ea7b018c0e33900fb";

struct Probe {
    value: u64,
    observed: Arc<std::sync::Mutex<Vec<u64>>>,
    fail_restore: bool,
}

impl Probe {
    fn new(value: u64) -> Self {
        Self {
            value,
            observed: Default::default(),
            fail_restore: false,
        }
    }
}

impl Strategy for Probe {
    fn on_market_event(&mut self, _: &MarketEvent, _: &StrategyContext) -> Vec<OrderIntent> {
        self.value += 1;
        self.observed.lock().unwrap().push(self.value);
        vec![]
    }

    fn checkpoint_schema_version(&self) -> Option<u32> {
        Some(1)
    }

    fn checkpoint_payload(&self) -> Result<String, CheckpointError> {
        Ok(self.value.to_string())
    }

    fn restore_checkpoint(&mut self, state: &ValidatedState<'_>) -> Result<(), CheckpointError> {
        self.value = state
            .payload()
            .parse()
            .map_err(|_| CheckpointError::Payload)?;
        if self.fail_restore {
            return Err(CheckpointError::Payload);
        }
        Ok(())
    }
}

fn identity() -> StateIdentity {
    StateIdentity {
        strategy: "counter".into(),
        revision: "1".into(),
        config_sha256: "f".repeat(64),
        scope: "live:strategy-restore-owner:BTCUSDT".into(),
        payload_version: 1,
    }
}

fn cursor() -> StateCursor {
    StateCursor {
        ordinal: 10,
        observed_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
    }
}

struct Fixture {
    admin: sqlx::PgPool,
    schema: String,
    url: String,
}

impl Fixture {
    async fn new() -> Self {
        let admin = sqlx::PgPool::connect(POSTGRES_URL).await.unwrap();
        let schema = format!("strategy_restore_test_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .unwrap();
        let url = format!("{POSTGRES_URL}?options[search_path]={schema}");
        Self { admin, schema, url }
    }

    async fn store(&self) -> OrderStore {
        let mut store = OrderStore::new(&self.url).await.unwrap();
        store.bind_spot_inventory(OWNER).await.unwrap();
        store
    }

    async fn seed(&self) -> RecoverySelection {
        let mut store = self.store().await;
        let envelope = capture(&Probe::new(42), identity(), cursor()).unwrap();
        let receipt = store
            .publish_strategy_checkpoint(None, &identity(), cursor(), &envelope)
            .await
            .unwrap();
        RecoverySelection::new(OWNER_SHA256.into(), receipt, HashMap::new()).unwrap()
    }

    async fn manager(
        &self,
        registry: &Registry,
    ) -> (OrderManager, LiveTradingConfig, Arc<apexes_oms::KillSwitch>) {
        let health = Arc::new(apexes_oms::KillSwitch::managed());
        let mut config = base_config();
        config.ems_postgres_url = self.url.clone();
        config.kill_switch = Some(health.clone());
        let mut manager = OrderManager::new(self.store().await, registry)
            .await
            .unwrap()
            .with_kill_switch(health.clone())
            .with_spot_scope(&config.instruments);
        health.begin_reconciliation();
        recover_live_state(&mut manager, &[AssetClass::Spot], async {
            Ok(ExchangeSnapshot::default())
        })
        .await
        .unwrap();
        assert!(manager.spot_inventory_fills().await.unwrap().is_empty());
        (manager, config, health)
    }

    async fn cleanup(self) {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP SCHEMA {} CASCADE",
            self.schema
        )))
        .execute(&self.admin)
        .await
        .unwrap();
    }

    async fn claim_count(&self) -> i64 {
        let pool = sqlx::PgPool::connect(&self.url).await.unwrap();
        let count = sqlx::query_scalar("SELECT count(*) FROM strategy_checkpoint_startup_claim")
            .fetch_one(&pool)
            .await
            .unwrap();
        pool.close().await;
        count
    }
}

fn registry() -> Registry {
    registry_with(
        Arc::new(MockExecutionGateway::new()),
        Arc::new(MockMarketDataProvider {
            events: sample_events(),
            keep_open: true,
        }),
    )
}

async fn run_startup(
    config: LiveTradingConfig,
    registry: &Registry,
    startup: StrategyStartup<Probe>,
    manager: OrderManager,
) -> Result<apexes_live_trading::LiveTradingReport, ApexesError> {
    let (stop, receiver) = oneshot::channel();
    let driver = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let _ = stop.send(());
    });
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        run_live_trading_with_startup(
            config,
            registry,
            startup,
            manager,
            futures::stream::empty(),
            receiver,
        ),
    )
    .await
    .unwrap();
    driver.abort();
    result
}

fn restore_startup(selection: RecoverySelection, candidate: Probe) -> StrategyStartup<Probe> {
    StrategyStartup::Restore {
        active: Probe::new(999),
        candidate,
        selection: Box::new(selection),
    }
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn runner_restores_independently_selected_state_before_first_fresh_event_only_once() {
    let fixture = Fixture::new().await;
    let selection = fixture.seed().await;
    let dir = std::env::temp_dir().join(format!("apexes-restore-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("receipt.json");
    selection.write_new(&path).unwrap();
    let selection = RecoverySelection::read(&path).unwrap();

    let reg = registry();
    let (manager, config, health) = fixture.manager(&reg).await;
    let candidate = Probe::new(0);
    let observed = candidate.observed.clone();
    let report = run_startup(
        config,
        &reg,
        restore_startup(selection.clone(), candidate),
        manager,
    )
    .await
    .unwrap();
    // Depth at the checkpoint timestamp is skipped; subsequent trade
    // sees the restored counter rather than either fresh strategy.
    assert_eq!(*observed.lock().unwrap(), vec![43]);
    assert!(report.trades.is_empty());
    assert!(!health.is_engaged());
    assert_eq!(fixture.claim_count().await, 1);

    // Even though no order was submitted, further observations may have
    // changed strategy state. Do not allow replaying the old checkpoint.
    let reg = registry();
    let (manager, config, health) = fixture.manager(&reg).await;
    let candidate = Probe::new(0);
    let observed = candidate.observed.clone();
    assert!(
        run_startup(config, &reg, restore_startup(selection, candidate), manager)
            .await
            .is_err()
    );
    assert!(observed.lock().unwrap().is_empty());
    assert!(health.is_engaged());
    // A legacy restart cannot bypass the durable claim even when the
    // checkpoint head has been removed by an inconsistent external write.
    let pool = sqlx::PgPool::connect(&fixture.url).await.unwrap();
    sqlx::query("DELETE FROM strategy_checkpoint_head")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    let reg = registry();
    let (manager, config, health) = fixture.manager(&reg).await;
    assert!(
        run_startup(
            config,
            &reg,
            StrategyStartup::Legacy(Probe::new(0)),
            manager
        )
        .await
        .is_err()
    );
    assert!(health.is_engaged());
    std::fs::remove_dir_all(dir).unwrap();
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn runner_refuses_missing_stale_or_wrong_owner_receipt_before_callbacks() {
    let fixture = Fixture::new().await;
    let envelope = capture(&Probe::new(42), identity(), cursor()).unwrap();
    let receipt =
        apexes_strategy::checkpoint::verify_checkpoint(&envelope, &identity(), cursor()).unwrap();
    let expected = RecoverySelection::new(OWNER_SHA256.into(), receipt, HashMap::new()).unwrap();
    let reg = registry();
    let (manager, config, health) = fixture.manager(&reg).await;
    let candidate = Probe::new(0);
    let observed = candidate.observed.clone();
    assert!(
        run_startup(
            config,
            &reg,
            restore_startup(expected.clone(), candidate),
            manager
        )
        .await
        .is_err()
    );
    assert!(health.is_engaged());
    assert!(observed.lock().unwrap().is_empty());
    assert_eq!(fixture.claim_count().await, 0);

    let selected = fixture.seed().await;
    let reg = registry();
    let (manager, config, health) = fixture.manager(&reg).await;
    let legacy = Probe::new(0);
    let observed = legacy.observed.clone();
    assert!(
        run_startup(config, &reg, StrategyStartup::Legacy(legacy), manager)
            .await
            .is_err()
    );
    assert!(observed.lock().unwrap().is_empty());
    assert!(health.is_engaged());

    let mut stale = selected.receipt().clone();
    stale.envelope_sha256 = "0".repeat(64);
    let stale = RecoverySelection::new(OWNER_SHA256.into(), stale, HashMap::new()).unwrap();
    let wrong_owner =
        RecoverySelection::new("0".repeat(64), selected.receipt().clone(), HashMap::new()).unwrap();
    for selection in [stale, wrong_owner] {
        let reg = registry();
        let (manager, config, health) = fixture.manager(&reg).await;
        let candidate = Probe::new(0);
        let observed = candidate.observed.clone();
        assert!(
            run_startup(config, &reg, restore_startup(selection, candidate), manager)
                .await
                .is_err()
        );
        assert!(health.is_engaged());
        assert!(observed.lock().unwrap().is_empty());
        assert_eq!(fixture.claim_count().await, 0);
    }
    let reg = registry();
    let (manager, mut config, health) = fixture.manager(&reg).await;
    config.ems_postgres_url = "postgres://unused.invalid/apexes".into();
    assert!(
        run_startup(
            config,
            &reg,
            restore_startup(selected, Probe::new(0)),
            manager
        )
        .await
        .is_err()
    );
    assert!(health.is_engaged());
    assert_eq!(fixture.claim_count().await, 0);
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn runner_refuses_zero_fill_execution_history_and_burns_failed_semantic_claim() {
    let fixture = Fixture::new().await;
    let selection = fixture.seed().await;
    let store = fixture.store().await;
    let now = cursor().observed_at;
    store
        .upsert_order(&Order {
            id: OrderId(uuid::Uuid::new_v4()),
            exchange_order_id: None,
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: dec!(1),
            price: None,
            stop_price: None,
            leverage: None,
            margin_mode: None,
            filled_quantity: dec!(0),
            status: OrderStatus::Rejected,
            created_at: now,
            updated_at: now,
        })
        .await
        .unwrap();
    drop(store);
    let reg = registry();
    let (manager, config, health) = fixture.manager(&reg).await;
    let candidate = Probe::new(0);
    let observed = candidate.observed.clone();
    assert!(
        run_startup(config, &reg, restore_startup(selection, candidate), manager)
            .await
            .is_err()
    );
    assert!(health.is_engaged());
    assert!(observed.lock().unwrap().is_empty());
    assert_eq!(fixture.claim_count().await, 0);
    fixture.cleanup().await;

    let fixture = Fixture::new().await;
    let selection = fixture.seed().await;
    let reg = registry();
    let (manager, config, health) = fixture.manager(&reg).await;
    let mut candidate = Probe::new(0);
    candidate.fail_restore = true;
    let observed = candidate.observed.clone();
    assert!(
        run_startup(
            config,
            &reg,
            restore_startup(selection.clone(), candidate),
            manager
        )
        .await
        .is_err()
    );
    assert!(health.is_engaged());
    assert!(observed.lock().unwrap().is_empty());
    assert_eq!(fixture.claim_count().await, 1);
    let reg = registry();
    let (manager, config, health) = fixture.manager(&reg).await;
    assert!(
        run_startup(
            config,
            &reg,
            restore_startup(selection, Probe::new(0)),
            manager
        )
        .await
        .is_err()
    );
    assert!(health.is_engaged());
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn runner_refuses_durable_parent_history_without_orders_or_fills() {
    let fixture = Fixture::new().await;
    let selection = fixture.seed().await;
    let pool = sqlx::PgPool::connect(&fixture.url).await.unwrap();
    sqlx::query(
        "INSERT INTO spot_parent_allocations (id, symbol, side, total_quantity, budget,
          spend_asset, receipt_asset, commission_rate, commission_at, reference_price, release_requested)
         VALUES ($1, 'BTCUSDT', 'Buy', 1, 100, 'USDT', 'BTC', 0, NOW(), 100, TRUE)"
    ).bind(uuid::Uuid::new_v4()).execute(&pool).await.unwrap();
    pool.close().await;
    let reg = registry();
    let (manager, config, health) = fixture.manager(&reg).await;
    let candidate = Probe::new(0);
    let observed = candidate.observed.clone();
    assert!(
        run_startup(config, &reg, restore_startup(selection, candidate), manager)
            .await
            .is_err()
    );
    assert!(health.is_engaged());
    assert!(observed.lock().unwrap().is_empty());
    assert_eq!(fixture.claim_count().await, 0);
    fixture.cleanup().await;
}

mod coherent;
