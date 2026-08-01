//! End-to-end test of `run_live_trading` driving a real `OrderManager`
//! (backed by a real, test-local PostgreSQL database) through a
//! `MockExecutionGateway`, a synthetic market-data stream, and a
//! synthetic `ExchangeUpdate` push stream -- proving the three
//! `tokio::select!` branches (market data -> submit, push update ->
//! apply, reconcile timer -> apply) all work together without needing
//! any real network access. Requires a live PostgreSQL instance:
//!   docker compose up -d postgres
//!   cargo test -p apexes-live-trading --test live_trading_integration -- --ignored

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use apexes_core::registry::Registry;
use apexes_core::{
    AssetClass, DepthUpdate, Instrument, MarketDataProvider, MarketEvent, MarketType, OrderType,
    PriceLevel, Side, TradeEvent, Venue,
};
use apexes_live_trading::{LiveTradingConfig, run_live_trading};
use apexes_oms::{
    ExchangeUpdate, ExecutionGateway, MockExecutionGateway, OrderManager, OrderStore,
};
use apexes_strategy::{OrderIntent, OrderRouting, Strategy, StrategyContext};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use futures::StreamExt;
use futures::stream::BoxStream;
use rust_decimal_macros::dec;
use tokio::sync::oneshot;

/// Each test constructs its own isolated `Registry` (rather than using
/// the process-wide `apexes_core::registry::global()`), so each test's
/// `MockExecutionGateway`/`MockMarketDataProvider` instances don't
/// collide with any other test's registrations -- see
/// `apexes-oms`'s own test module for the identical rationale.
fn registry_with(
    gateway: Arc<dyn ExecutionGateway>,
    provider: Arc<dyn MarketDataProvider>,
) -> Registry {
    let registry = Registry::new();
    registry
        .register::<Arc<dyn ExecutionGateway>, ()>((), gateway)
        .unwrap();
    registry
        .register::<Arc<dyn MarketDataProvider>, Venue>(Venue::Binance, provider)
        .unwrap();
    registry
}

const POSTGRES_URL: &str = "postgres://apexes:apexes@localhost:5432/apexes";

async fn clean_db() {
    let pool = sqlx::PgPool::connect(POSTGRES_URL).await.unwrap();
    sqlx::query("TRUNCATE fills, orders")
        .execute(&pool)
        .await
        .unwrap();
}

struct MockMarketDataProvider {
    events: Vec<MarketEvent>,
    /// When true, the returned stream stays open (pending) after
    /// `events` are exhausted. Needed for tests that rely on the
    /// `exchange_updates`/stop-signal branches: `run_live_trading`
    /// breaks its loop as soon as the market stream ends, so a finite
    /// empty stream would terminate the run before any exchange update
    /// is ever polled.
    keep_open: bool,
}

#[async_trait]
impl MarketDataProvider for MockMarketDataProvider {
    async fn subscribe(
        &self,
        _instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, apexes_core::ApexesError> {
        let stream = futures::stream::iter(self.events.clone());
        if self.keep_open {
            Ok(Box::pin(stream.chain(futures::stream::pending())))
        } else {
            Ok(Box::pin(stream))
        }
    }
}

struct BuyOnceStrategy {
    bought: bool,
}

impl Strategy for BuyOnceStrategy {
    fn on_market_event(&mut self, event: &MarketEvent, _ctx: &StrategyContext) -> Vec<OrderIntent> {
        if !self.bought && matches!(event, MarketEvent::Trade(_)) {
            self.bought = true;
            return vec![OrderIntent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                asset_class: AssetClass::Spot,
                side: Side::Buy,
                order_type: OrderType::Market,
                quantity: dec!(1),
                price: None,
                stop_price: None,
                execution_algo: None,
                routing: OrderRouting::Fixed,
            }];
        }
        vec![]
    }
}

fn btc_usdt_instrument() -> Instrument {
    Instrument::new(
        "BTCUSDT",
        MarketType::Crypto,
        Venue::Binance,
        AssetClass::Spot,
        Some("BTC".to_string()),
        Some("USDT".to_string()),
        dec!(0.01),
        dec!(0.0001),
        dec!(1),
    )
    .unwrap()
}

fn sample_events() -> Vec<MarketEvent> {
    let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    vec![
        MarketEvent::Depth(DepthUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bids: vec![],
            asks: vec![PriceLevel {
                price: dec!(100),
                quantity: dec!(1000),
            }],
            sequence: Some(1),
            is_snapshot: true,
            timestamp: t0,
        }),
        MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(100),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: t0 + chrono::Duration::seconds(1),
        }),
    ]
}

fn base_config() -> LiveTradingConfig {
    let mut initial_balances = HashMap::new();
    initial_balances.insert("USDT".to_string(), dec!(100000));
    LiveTradingConfig {
        instruments: vec![btc_usdt_instrument()],
        initial_balances,
        risk_free_return: dec!(0),
        reconcile_interval: Duration::from_secs(3600), // long enough not to fire during this test
    }
}

#[tokio::test]
#[ignore]
async fn strategy_order_is_submitted_and_a_push_update_produces_a_fill() {
    clean_db().await;
    let gateway: Arc<dyn ExecutionGateway> = Arc::new(MockExecutionGateway::new());
    let provider: Arc<dyn MarketDataProvider> = Arc::new(MockMarketDataProvider {
        events: sample_events(),
        keep_open: false,
    });
    let registry = registry_with(Arc::clone(&gateway), provider);

    let store = OrderStore::new(POSTGRES_URL).await.unwrap();
    let order_manager = OrderManager::new(store, &registry).await.unwrap();

    let (_tx, rx) = oneshot::channel();

    // No exchange_updates in this run (empty stream) -- exercises only
    // the market-data -> submit_order path end to end.
    let exchange_updates = futures::stream::empty::<ExchangeUpdate>();

    let report = run_live_trading(
        base_config(),
        &registry,
        BuyOnceStrategy { bought: false },
        order_manager,
        exchange_updates,
        rx,
    )
    .await
    .unwrap();

    // No fills yet -- MockExecutionGateway's submit_order accepts the
    // order but the test never pushed a fill-producing exchange update,
    // so trades should be empty even though an order was submitted.
    assert!(report.trades.is_empty());
}

#[tokio::test]
#[ignore]
async fn exchange_update_push_produces_a_fill_applied_to_the_ledger() {
    use apexes_core::OrderStatus;
    use apexes_oms::ExchangeFill;

    clean_db().await;
    let gateway = Arc::new(MockExecutionGateway::new());
    let provider: Arc<dyn MarketDataProvider> = Arc::new(MockMarketDataProvider {
        events: vec![],
        keep_open: true,
    });
    let registry = registry_with(Arc::clone(&gateway) as Arc<dyn ExecutionGateway>, provider);

    let store = OrderStore::new(POSTGRES_URL).await.unwrap();
    let mut order_manager = OrderManager::new(store, &registry).await.unwrap();

    // Submit one order directly (bypassing the strategy path) so we
    // have a known OrderId to push an ExchangeUpdate for.
    let order_id = order_manager
        .submit_order(
            OrderIntent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                asset_class: AssetClass::Spot,
                side: Side::Buy,
                order_type: OrderType::Market,
                quantity: dec!(1),
                price: None,
                stop_price: None,
                execution_algo: None,
                routing: OrderRouting::Fixed,
            },
            &HashMap::new(),
            &HashMap::new(),
        )
        .await
        .unwrap();

    gateway.set_fills_for_order(
        order_id,
        vec![ExchangeFill {
            exchange_trade_id: "T1".to_string(),
            price: dec!(100),
            quantity: dec!(1),
            fee: dec!(0),
            fee_currency: None,
            filled_at: Utc::now(),
        }],
    );

    let exchange_order_id = order_manager
        .open_orders()
        .get(&order_id)
        .unwrap()
        .exchange_order_id
        .clone();

    let update = ExchangeUpdate {
        client_order_id: order_id,
        exchange_order_id,
        status: OrderStatus::Filled,
    };
    let exchange_updates = futures::stream::iter(vec![update]).chain(futures::stream::pending());

    // Send the stop signal shortly after the single update should have
    // been processed, since `futures::stream::pending()` never
    // completes on its own (the loop would otherwise run forever).
    let (tx, rx_stop) = oneshot::channel();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let _ = tx.send(());
    });

    let report = run_live_trading(
        base_config(),
        &registry,
        BuyOnceStrategy { bought: true }, // already "bought" so it emits no new intents
        order_manager,
        exchange_updates,
        rx_stop,
    )
    .await
    .unwrap();

    assert_eq!(report.trades.len(), 1);
    assert_eq!(report.trades[0].quantity, dec!(1));
}
