//! F02.01 qualification with isolated PostgreSQL schemas and mock fills.
//! Run only against the repository's test database, never a live database.
use apexes_core::{
    AssetClass, Fill, FillId, Instrument, MarketType, Order, OrderId, OrderStatus, OrderType, Side,
    Venue,
};
use apexes_live_trading::{SpotWalletBalance, recover_spot_positions};
use apexes_oms::{OmsError, OrderStore};
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sqlx::PgPool;
use std::collections::HashMap;
use uuid::Uuid;

const URL: &str = "postgres://apexes:apexes@localhost:5432/apexes";

struct Fixture {
    admin: PgPool,
    schema: String,
    url: String,
}

impl Fixture {
    async fn new() -> Self {
        let admin = PgPool::connect(URL).await.unwrap();
        // The identifier consists only of a fixed prefix and UUID hex.
        let schema = format!("spot_recovery_test_{}", Uuid::new_v4().simple());
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .unwrap();
        let url = format!("{URL}?options[search_path]={schema}");
        Self { admin, schema, url }
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

fn instrument() -> Instrument {
    Instrument::new(
        "BTCUSDT",
        MarketType::Crypto,
        Venue::Binance,
        AssetClass::Spot,
        Some("BTC".into()),
        Some("USDT".into()),
        dec!(0.01),
        dec!(0.001),
        dec!(1),
    )
    .unwrap()
}

fn execution(side: Side, qty: Decimal, price: Decimal, fee: Decimal, offset: i64) -> (Order, Fill) {
    let at = Utc::now() + chrono::Duration::seconds(offset);
    let order = Order {
        id: OrderId(Uuid::new_v4()),
        exchange_order_id: Some(Uuid::new_v4().to_string()),
        symbol: "BTCUSDT".into(),
        venue: Venue::Binance,
        asset_class: AssetClass::Spot,
        side,
        order_type: OrderType::Market,
        quantity: qty,
        price: None,
        stop_price: None,
        leverage: None,
        margin_mode: None,
        filled_quantity: qty,
        status: OrderStatus::Filled,
        created_at: at,
        updated_at: at,
    };
    let fill = Fill {
        id: FillId(Uuid::new_v4()),
        order_id: order.id,
        symbol: order.symbol.clone(),
        venue: order.venue,
        side,
        price,
        quantity: qty,
        fee,
        fee_currency: Some("BTC".into()),
        filled_at: at,
        leverage: None,
        margin_mode: None,
    };
    (order, fill)
}

#[tokio::test]
#[ignore]
async fn durable_fee_conversion_is_causal_immutable_and_recovered_after_restart() {
    let fixture = Fixture::new().await;
    let store = bound_store(&fixture.url, "fee-pricing-owner").await;
    let (order, mut fill) = execution(Side::Buy, dec!(1), dec!(100), dec!(0.01), 0);
    fill.fee_currency = Some("BNB".into());
    store.upsert_order(&order).await.unwrap();
    assert!(store.insert_fill(&fill, "1").await.unwrap());
    let journal = store.fee_journal().unwrap();
    assert!(journal.history().await.unwrap()[0].1.is_none());
    let observed_at =
        chrono::DateTime::from_timestamp_millis(fill.filled_at.timestamp_millis() - 1000).unwrap();
    let valid = apexes_oms::FeeConversion {
        asset: "BNB".into(),
        quote: "USDT".into(),
        observed_at,
        rate: dec!(300),
    };
    let future = apexes_oms::FeeConversion {
        observed_at: fill.filled_at + chrono::TimeDelta::seconds(1),
        ..valid.clone()
    };
    assert!(journal.persist(fill.id, &future).await.is_err());
    let stale = apexes_oms::FeeConversion {
        observed_at: fill.filled_at - chrono::TimeDelta::minutes(6),
        ..valid.clone()
    };
    assert!(journal.persist(fill.id, &stale).await.is_err());
    let wrong_asset = apexes_oms::FeeConversion {
        asset: "ETH".into(),
        ..valid.clone()
    };
    assert!(journal.persist(fill.id, &wrong_asset).await.is_err());
    journal.persist(fill.id, &valid).await.unwrap();
    journal.persist(fill.id, &valid).await.unwrap();
    let conflicting = apexes_oms::FeeConversion {
        rate: dec!(301),
        ..valid.clone()
    };
    assert!(journal.persist(fill.id, &conflicting).await.is_err());
    drop(journal);
    drop(store);
    let restarted = bound_store(&fixture.url, "fee-pricing-owner").await;
    let recovered = restarted.fee_journal().unwrap().history().await.unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].0.id, fill.id);
    assert_eq!(recovered[0].1, Some(valid));
    drop(restarted);
    fixture.cleanup().await;
}

async fn bound_store(url: &str, owner: &str) -> OrderStore {
    let mut store = OrderStore::new(url).await.unwrap();
    store.bind_spot_inventory(owner).await.unwrap();
    store
}

#[tokio::test]
#[ignore]
async fn durable_fills_rebuild_inventory_and_cost_basis_after_restart() {
    let fixture = Fixture::new().await;
    let mut store = bound_store(&fixture.url, "testnet-strategy-btc").await;
    // Insert in reverse order; recovery must order by execution time, not
    // insertion order. A replayed exchange trade must not double count.
    let (sell, sell_fill) = execution(Side::Sell, dec!(1), dec!(150), dec!(0.1), 10);
    let (buy, buy_fill) = execution(Side::Buy, dec!(2), dec!(100), dec!(0.1), 0);
    for (order, fill, trade_id) in [(&sell, &sell_fill, "12"), (&buy, &buy_fill, "11")] {
        store.upsert_order(order).await.unwrap();
        assert!(store.insert_fill(fill, trade_id).await.unwrap());
        assert!(!store.insert_fill(fill, trade_id).await.unwrap());
    }
    let before = store.spot_inventory_fills().await.unwrap();
    drop(store);
    let mut restarted = bound_store(&fixture.url, "testnet-strategy-btc").await;
    let after = restarted.spot_inventory_fills().await.unwrap();
    assert_eq!(
        after.iter().map(|f| f.id).collect::<Vec<_>>(),
        vec![buy_fill.id, sell_fill.id]
    );
    assert_eq!(
        before.iter().map(|f| f.id).collect::<Vec<_>>(),
        after.iter().map(|f| f.id).collect::<Vec<_>>()
    );
    let wallet = HashMap::from([
        (
            "BTC".into(),
            SpotWalletBalance {
                free: dec!(10.8),
                locked: dec!(0),
            },
        ),
        (
            "USDT".into(),
            SpotWalletBalance {
                free: dec!(1000),
                locked: dec!(0),
            },
        ),
    ]);
    let positions = recover_spot_positions(&[instrument()], &after, &wallet, false).unwrap();
    assert_eq!(positions["BTCUSDT"].quantity, dec!(0.8));
    assert_eq!(
        positions["BTCUSDT"].average_entry_price,
        dec!(200) / dec!(1.9)
    );
    drop(restarted);
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore]
async fn exclusive_owner_binding_rejects_concurrent_and_different_owners() {
    let fixture = Fixture::new().await;
    let store = bound_store(&fixture.url, "owner-a").await;
    let mut competing = OrderStore::new(&fixture.url).await.unwrap();
    assert!(matches!(
        competing.bind_spot_inventory("owner-a").await,
        Err(OmsError::InvalidRow { .. })
    ));
    drop(store);
    assert!(matches!(
        competing.bind_spot_inventory("owner-b").await,
        Err(OmsError::InvalidRow { .. })
    ));
    competing.bind_spot_inventory("owner-a").await.unwrap();
    drop(competing);
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore]
async fn legacy_history_is_not_silently_adopted() {
    let fixture = Fixture::new().await;
    let mut store = OrderStore::new(&fixture.url).await.unwrap();
    let (order, fill) = execution(Side::Buy, dec!(1), dec!(100), dec!(0), 0);
    store.upsert_order(&order).await.unwrap();
    store.insert_fill(&fill, "1").await.unwrap();
    let error = store.bind_spot_inventory("new-owner").await.unwrap_err();
    assert!(error.to_string().contains("audited migration"));
    assert_eq!(store.get_fills_for_order(order.id).await.unwrap().len(), 1);
    drop(store);
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore]
async fn incomplete_fill_history_refuses_recovery() {
    let fixture = Fixture::new().await;
    let mut store = bound_store(&fixture.url, "owner").await;
    let (order, _) = execution(Side::Buy, dec!(1), dec!(100), dec!(0), 0);
    store.upsert_order(&order).await.unwrap();
    assert!(matches!(
        store.spot_inventory_fills().await,
        Err(OmsError::InvalidRow { .. })
    ));
    drop(store);
    fixture.cleanup().await;
}
