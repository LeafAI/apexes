//! F02.02: managed fill journal, atomic reconciliation and delivery gate.
//! Uses independent PostgreSQL schemas on the test database only.
use apexes_core::registry::Registry;
use apexes_core::{AssetClass, OrderStatus, OrderType, Side, Venue};
use apexes_oms::{
    ExchangeFill, ExecutionGateway, KillSwitch, MockExecutionGateway, OmsError, OrderManager,
    OrderStore,
};
use apexes_strategy::{OrderIntent, OrderRouting};
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

const URL: &str = "postgres://apexes:apexes@localhost:5432/apexes";

struct Fixture {
    admin: PgPool,
    schema: String,
    url: String,
    gateway: Arc<MockExecutionGateway>,
    registry: Registry,
    health: Arc<KillSwitch>,
}

impl Fixture {
    async fn new() -> Self {
        let admin = PgPool::connect(URL).await.unwrap();
        // Generated exclusively from a fixed prefix and UUID hex.
        let schema = format!("fill_test_{}", Uuid::new_v4().simple());
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .unwrap();
        let url = format!("{URL}?options[search_path]={schema}");
        let registry = Registry::new();
        let gateway = Arc::new(MockExecutionGateway::new());
        registry
            .register::<Arc<dyn ExecutionGateway>, ()>((), gateway.clone())
            .unwrap();
        let health = Arc::new(KillSwitch::managed());
        Self {
            admin,
            schema,
            url,
            gateway,
            registry,
            health,
        }
    }

    async fn manager(&self) -> OrderManager {
        let mut store = OrderStore::new(&self.url).await.unwrap();
        store.bind_spot_inventory("fixture-owner").await.unwrap();
        let mut manager = OrderManager::new(store, &self.registry)
            .await
            .unwrap()
            .with_kill_switch(self.health.clone());
        manager.spot_inventory_fills().await.unwrap();
        self.health.begin_reconciliation();
        self.health.startup_ready();
        manager
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
}

fn intent() -> OrderIntent {
    OrderIntent {
        symbol: "BTCUSDT".into(),
        venue: Venue::Binance,
        asset_class: AssetClass::Spot,
        side: Side::Buy,
        order_type: OrderType::Market,
        quantity: dec!(3),
        price: None,
        stop_price: None,
        execution_algo: None,
        routing: OrderRouting::Fixed,
        leverage: None,
        margin_mode: None,
    }
}

fn fill(id: &str, quantity: Decimal, offset: i64) -> ExchangeFill {
    ExchangeFill {
        exchange_trade_id: id.into(),
        price: dec!(100),
        quantity,
        fee: dec!(0.01),
        fee_currency: Some("BTC".into()),
        filled_at: Utc::now() + chrono::Duration::seconds(offset),
    }
}

async fn submit(manager: &mut OrderManager) -> apexes_core::OrderId {
    manager
        .submit_order(intent(), &HashMap::new(), &HashMap::new())
        .await
        .unwrap()
}

async fn acknowledge(manager: &mut OrderManager, expected: usize) {
    let batch = manager.durable_fill_batch().await.unwrap();
    assert_eq!(batch.len(), expected);
    manager
        .acknowledge_fill_batch(&batch.iter().map(|f| f.id).collect::<Vec<_>>(), true)
        .await;
}

#[tokio::test]
#[ignore]
async fn managed_spot_sale_requires_durable_owned_free_inventory_and_no_working_order() {
    let fixture = Fixture::new().await;
    let mut manager = fixture.manager().await;
    fixture.gateway.set_spot_sell_inventory("BTC", dec!(10));
    let mut exit = intent();
    exit.side = Side::Sell;
    exit.quantity = dec!(1);
    // Caller-supplied position and a large account wallet do not prove ownership.
    let positions = HashMap::from([(
        "BTCUSDT".into(),
        apexes_strategy::Position {
            symbol: "BTCUSDT".into(),
            quantity: dec!(10),
            average_entry_price: dec!(100),
            leverage: None,
            margin_mode: None,
            allocated_margin: Decimal::ZERO,
        },
    )]);
    assert!(
        manager
            .submit_order(exit.clone(), &positions, &HashMap::new())
            .await
            .is_err()
    );
    let id = submit(&mut manager).await;
    fixture.gateway.set_exchange_state(id, OrderStatus::Filled);
    fixture
        .gateway
        .set_fills_for_order(id, vec![fill("1", dec!(3), 0)]);
    manager.reconcile().await.unwrap();
    assert!(
        manager
            .submit_order(exit.clone(), &positions, &HashMap::new())
            .await
            .is_err()
    );
    acknowledge(&mut manager, 1).await;
    exit.quantity = dec!(3);
    assert!(
        manager
            .submit_order(exit.clone(), &positions, &HashMap::new())
            .await
            .is_err()
    ); // base fee
    exit.quantity = dec!(2);
    let accepted = manager
        .submit_order(exit.clone(), &positions, &HashMap::new())
        .await
        .unwrap();
    assert!(
        manager
            .submit_order(exit.clone(), &positions, &HashMap::new())
            .await
            .is_err()
    ); // pending sell
    manager.cancel_order(accepted).await.unwrap();
    fixture.gateway.set_spot_sell_inventory("BTC", dec!(1));
    assert!(
        manager
            .submit_order(exit.clone(), &positions, &HashMap::new())
            .await
            .is_err()
    );
    assert!(fixture.health.is_engaged());
    fixture.gateway.set_spot_sell_inventory("BTC", dec!(10));
    assert!(
        manager
            .submit_order(exit.clone(), &positions, &HashMap::new())
            .await
            .is_err()
    );
    assert!(manager.submit_flatten_order(exit).await.is_err());
    drop(manager);
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore]
async fn durable_reservation_restores_after_manager_restart_and_releases_on_terminal() {
    let fixture = Fixture::new().await;
    let mut manager = fixture.manager().await;
    let id = submit(&mut manager).await;
    drop(manager);
    let mut restarted = fixture.manager().await;
    let mut other = intent();
    other.symbol = "ETHUSDT".into();
    assert!(
        restarted
            .submit_child_order(other.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("reserved or awaiting receipt")
    );
    restarted.cancel_order(id).await.unwrap();
    assert!(restarted.submit_child_order(other).await.is_ok());
    let pool = PgPool::connect(&fixture.url).await.unwrap();
    let evidence: (String,String,Decimal,bool,Option<Decimal>) = sqlx::query_as("SELECT reserved_spend_asset,reserved_receipt_asset,reserved_amount,reservation_breached,reserved_market_buy_cap FROM orders WHERE id=$1")
        .bind(id.0).fetch_one(&pool).await.unwrap();
    assert_eq!(
        evidence,
        (
            "USDT".into(),
            "BTC".into(),
            dec!(303),
            false,
            Some(dec!(100))
        )
    );
    drop(restarted);
    pool.close().await;
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore]
async fn actual_spend_including_quote_fee_breaches_budget_without_losing_fill() {
    let fixture = Fixture::new().await;
    let mut manager = fixture.manager().await;
    let id = submit(&mut manager).await;
    let mut execution = fill("1", dec!(3), 0);
    // A fee-only breach at the capped execution price, independent of a price breach.
    execution.price = dec!(100);
    execution.fee = dec!(3.01);
    execution.fee_currency = Some("USDT".into());
    fixture.gateway.set_exchange_state(id, OrderStatus::Filled);
    fixture.gateway.set_fills_for_order(id, vec![execution]);
    manager.reconcile().await.unwrap();
    assert!(fixture.health.is_engaged());
    assert!(!manager.open_orders().contains_key(&id));
    acknowledge(&mut manager, 1).await;
    let pool = PgPool::connect(&fixture.url).await.unwrap();
    let breached: bool = sqlx::query_scalar("SELECT reservation_breached FROM orders WHERE id=$1")
        .bind(id.0)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(breached);
    let store = OrderStore::new(&fixture.url).await.unwrap();
    assert_eq!(store.get_fills_for_order(id).await.unwrap().len(), 1);
    drop(store);
    drop(manager);
    let mut store = OrderStore::new(&fixture.url).await.unwrap();
    store.bind_spot_inventory("fixture-owner").await.unwrap();
    let mut restarted = OrderManager::new(store, &fixture.registry).await.unwrap();
    assert!(restarted.spot_inventory_fills().await.is_err());
    assert!(matches!(
        restarted.submit_child_order(intent()).await,
        Err(OmsError::TradingHalted { .. })
    ));
    drop(restarted);
    pool.close().await;
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore]
async fn bounded_ioc_partial_expiry_releases_capital_after_delivery() {
    let fixture = Fixture::new().await;
    let mut manager = fixture.manager().await;
    let id = submit(&mut manager).await;
    let mut execution = fill("1", dec!(1), 0);
    execution.price = dec!(99);
    fixture
        .gateway
        .set_exchange_state(id, OrderStatus::Cancelled);
    fixture.gateway.set_fills_for_order(id, vec![execution]);
    manager.reconcile().await.unwrap();
    assert!(!manager.open_orders().contains_key(&id));
    assert!(!fixture.health.is_engaged());
    let mut other = intent();
    other.symbol = "ETHUSDT".into();
    assert!(manager.submit_child_order(other.clone()).await.is_err());
    acknowledge(&mut manager, 1).await;
    assert!(manager.submit_child_order(other).await.is_ok());
    drop(manager);
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore]
async fn fill_above_cap_latches_breach_even_when_total_spend_is_under_budget() {
    let fixture = Fixture::new().await;
    let mut manager = fixture.manager().await;
    let id = submit(&mut manager).await;
    let mut execution = fill("1", dec!(1), 0);
    execution.price = dec!(100.01);
    fixture
        .gateway
        .set_exchange_state(id, OrderStatus::Cancelled);
    fixture.gateway.set_fills_for_order(id, vec![execution]);
    manager.reconcile().await.unwrap();
    assert!(fixture.health.is_engaged());
    acknowledge(&mut manager, 1).await;
    let pool = PgPool::connect(&fixture.url).await.unwrap();
    let evidence: (Decimal, bool) = sqlx::query_as(
        "SELECT reserved_market_buy_cap,reservation_breached FROM orders WHERE id=$1",
    )
    .bind(id.0)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(evidence, (dec!(100), true));
    let store = OrderStore::new(&fixture.url).await.unwrap();
    assert_eq!(store.get_fills_for_order(id).await.unwrap().len(), 1);
    drop(store);
    drop(manager);
    pool.close().await;
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore]
async fn shared_quote_reservation_survives_partial_fill_until_terminal_reconciliation() {
    let fixture = Fixture::new().await;
    let mut manager = fixture.manager().await;
    fixture.gateway.set_spot_free_quote(dec!(302));
    assert!(
        manager
            .submit_order(intent(), &HashMap::new(), &HashMap::new())
            .await
            .is_err()
    );
    assert!(manager.open_orders().is_empty());
    fixture.gateway.set_spot_free_quote(dec!(1000));
    let first = submit(&mut manager).await;
    let mut other = intent();
    other.symbol = "ETHUSDT".into();
    let error = manager.submit_child_order(other.clone()).await.unwrap_err();
    assert!(error.to_string().contains("reserved"));
    fixture
        .gateway
        .set_exchange_state(first, OrderStatus::PartiallyFilled);
    fixture
        .gateway
        .set_fills_for_order(first, vec![fill("1", dec!(1), 0)]);
    manager.reconcile().await.unwrap();
    acknowledge(&mut manager, 1).await;
    assert!(
        manager
            .submit_child_order(other.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("reserved")
    );
    manager.cancel_order(first).await.unwrap();
    assert!(!manager.open_orders().contains_key(&first));
    let second = manager.submit_child_order(other).await.unwrap();
    assert!(manager.open_orders().contains_key(&second));
    drop(manager);
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore]
async fn managed_buy_risk_uses_durable_position_and_fresh_notional_without_caller_snapshot() {
    let fixture = Fixture::new().await;
    let checker = apexes_risk::CompositeRiskChecker::new().with_pre_trade_check(Box::new(
        apexes_risk::LimitChecker::new(apexes_risk::RiskLimits {
            max_position_per_symbol: Some(dec!(4)),
            max_order_notional: Some(dec!(350)),
            ..Default::default()
        }),
    ));
    fixture
        .registry
        .register::<Arc<apexes_risk::CompositeRiskChecker>, ()>((), Arc::new(checker))
        .unwrap();
    let mut manager = fixture.manager().await;
    let mut too_large = intent();
    too_large.quantity = dec!(3.6);
    assert!(matches!(
        manager.submit_child_order(too_large).await,
        Err(OmsError::RiskCheckFailed { .. })
    ));
    let id = submit(&mut manager).await;
    fixture.gateway.set_exchange_state(id, OrderStatus::Filled);
    fixture
        .gateway
        .set_fills_for_order(id, vec![fill("1", dec!(3), 0)]);
    manager.reconcile().await.unwrap();
    acknowledge(&mut manager, 1).await;
    let mut next = intent();
    next.quantity = dec!(2);
    assert!(matches!(
        manager.submit_child_order(next).await,
        Err(OmsError::RiskCheckFailed { .. })
    ));
    assert!(manager.open_orders().is_empty());
    drop(manager);
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore]
async fn repeated_partial_and_cancel_deliver_each_execution_once() {
    let fixture = Fixture::new().await;
    let mut manager = fixture.manager().await;
    let id = submit(&mut manager).await;
    let first = fill("1", dec!(1), 0);
    let second = fill("2", dec!(1), 1);
    let third = fill("3", dec!(0.5), 2);
    fixture
        .gateway
        .set_exchange_state(id, OrderStatus::PartiallyFilled);
    fixture.gateway.set_fills_for_order(id, vec![first.clone()]);
    manager.reconcile().await.unwrap();
    assert!(matches!(
        manager
            .submit_order(intent(), &HashMap::new(), &HashMap::new())
            .await,
        Err(OmsError::TradingHalted { .. })
    ));
    acknowledge(&mut manager, 1).await;
    assert!(manager.durable_fill_batch().await.unwrap().is_empty());

    fixture
        .gateway
        .set_fills_for_order(id, vec![second.clone(), first.clone()]);
    manager.reconcile().await.unwrap();
    assert_eq!(manager.open_orders()[&id].filled_quantity, dec!(2));
    acknowledge(&mut manager, 1).await;
    // An old NEW push must not regress the authoritative partial state.
    manager
        .apply_exchange_update(id, None, OrderStatus::New)
        .await
        .unwrap();
    assert_eq!(
        manager.open_orders()[&id].status,
        OrderStatus::PartiallyFilled
    );
    assert!(manager.durable_fill_batch().await.unwrap().is_empty());

    fixture
        .gateway
        .set_fills_for_order(id, vec![first, second, third]);
    // Cancel returns no fill payload to its caller; the durable journal
    // still delivers the execution that raced with cancellation.
    manager.cancel_order(id).await.unwrap();
    assert!(!manager.open_orders().contains_key(&id));
    acknowledge(&mut manager, 1).await;
    assert!(manager.durable_fill_batch().await.unwrap().is_empty());
    let store = OrderStore::new(&fixture.url).await.unwrap();
    let order = store.get_order(id).await.unwrap().unwrap();
    assert_eq!(order.status, OrderStatus::Cancelled);
    assert_eq!(order.filled_quantity, dec!(2.5));
    assert_eq!(store.get_fills_for_order(id).await.unwrap().len(), 3);
    assert!(!fixture.health.is_engaged());
    drop(manager);
    drop(store);
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore]
async fn contradictory_trade_rolls_back_entire_update_and_latches_fault() {
    let fixture = Fixture::new().await;
    let mut manager = fixture.manager().await;
    let id = submit(&mut manager).await;
    let first = fill("1", dec!(1), 0);
    fixture
        .gateway
        .set_exchange_state(id, OrderStatus::PartiallyFilled);
    fixture.gateway.set_fills_for_order(id, vec![first.clone()]);
    manager.reconcile().await.unwrap();
    acknowledge(&mut manager, 1).await;
    let mut conflicting = first;
    conflicting.price = dec!(200);
    fixture
        .gateway
        .set_fills_for_order(id, vec![fill("2", dec!(1), 1), conflicting]);
    assert!(manager.reconcile().await.is_err());
    assert!(fixture.health.is_engaged());
    let store = OrderStore::new(&fixture.url).await.unwrap();
    assert_eq!(store.get_fills_for_order(id).await.unwrap().len(), 1);
    assert_eq!(
        store.get_order(id).await.unwrap().unwrap().filled_quantity,
        dec!(1)
    );
    drop(manager);
    drop(store);
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore]
async fn incomplete_terminal_execution_cannot_retire_order() {
    let fixture = Fixture::new().await;
    let mut manager = fixture.manager().await;
    let id = submit(&mut manager).await;
    fixture.gateway.set_exchange_state(id, OrderStatus::Filled);
    fixture
        .gateway
        .set_fills_for_order(id, vec![fill("1", dec!(1), 0)]);
    assert!(manager.reconcile().await.is_err());
    assert!(fixture.health.is_engaged());
    assert!(manager.open_orders().contains_key(&id));
    let store = OrderStore::new(&fixture.url).await.unwrap();
    assert!(store.get_fills_for_order(id).await.unwrap().is_empty());
    assert_eq!(
        store.get_order(id).await.unwrap().unwrap().status,
        OrderStatus::New
    );
    drop(manager);
    drop(store);
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore]
async fn late_execution_is_durable_but_cannot_change_applied_basis() {
    let fixture = Fixture::new().await;
    let mut manager = fixture.manager().await;
    let id = submit(&mut manager).await;
    let later = fill("2", dec!(1), 10);
    let earlier = fill("1", dec!(1), 0);
    fixture
        .gateway
        .set_exchange_state(id, OrderStatus::PartiallyFilled);
    fixture.gateway.set_fills_for_order(id, vec![later.clone()]);
    manager.reconcile().await.unwrap();
    acknowledge(&mut manager, 1).await;
    fixture
        .gateway
        .set_fills_for_order(id, vec![later, earlier]);
    manager.reconcile().await.unwrap();
    assert!(manager.durable_fill_batch().await.unwrap().is_empty());
    assert!(fixture.health.is_engaged());
    let store = OrderStore::new(&fixture.url).await.unwrap();
    assert_eq!(store.get_fills_for_order(id).await.unwrap().len(), 2);
    assert!(matches!(
        manager
            .submit_order(intent(), &HashMap::new(), &HashMap::new())
            .await,
        Err(OmsError::TradingHalted { .. })
    ));
    drop(manager);
    drop(store);
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore]
async fn failed_ledger_acknowledgement_blocks_replay_and_new_exposure() {
    let fixture = Fixture::new().await;
    let mut manager = fixture.manager().await;
    let id = submit(&mut manager).await;
    fixture
        .gateway
        .set_exchange_state(id, OrderStatus::PartiallyFilled);
    fixture
        .gateway
        .set_fills_for_order(id, vec![fill("1", dec!(1), 0)]);
    manager.reconcile().await.unwrap();
    let batch = manager.durable_fill_batch().await.unwrap();
    assert_eq!(batch.len(), 1);
    manager.acknowledge_fill_batch(&[batch[0].id], false).await;
    assert!(fixture.health.is_engaged());
    assert!(manager.durable_fill_batch().await.unwrap().is_empty());
    let store = OrderStore::new(&fixture.url).await.unwrap();
    assert_eq!(store.get_fills_for_order(id).await.unwrap().len(), 1);
    drop(manager);
    drop(store);
    fixture.cleanup().await;
}
