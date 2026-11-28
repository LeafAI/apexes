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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use apexes_backtest::CostModel;
use apexes_backtest::cost_model::FlatFeeSchedule;
use apexes_core::registry::Registry;
use apexes_core::{
    ApexesError, AssetClass, DepthUpdate, Instrument, MarketDataProvider, MarketEvent, MarketType,
    Order, OrderId, OrderStatus, OrderType, PriceLevel, Side, TradeEvent, Venue,
};
use apexes_live_trading::{LiveTradingConfig, run_live_trading};
use apexes_message_bus::MarketEventBus;
use apexes_oms::{
    ExchangeFill, ExchangeUpdate, ExecutionGateway, GatewayOutcome, MockExecutionGateway,
    OrderManager, OrderStore,
};
use apexes_strategy::{
    MarginMode, MarginTier, MarginTierTable, OrderIntent, OrderRouting, Strategy, StrategyContext,
};
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
    let bus = Arc::new(MarketEventBus::new(Box::new(ArcProviderAdapter(provider))));
    registry
        .register::<Arc<MarketEventBus>, Venue>(Venue::Binance, bus)
        .unwrap();
    registry
}

/// `Box<dyn MarketDataProvider>` is required by `MarketEventBus::new`,
/// but every call site in this test file already holds an
/// `Arc<dyn MarketDataProvider>` (constructed once and potentially
/// shared across multiple assertions within one test). This thin
/// wrapper lets an `Arc<dyn MarketDataProvider>` satisfy that `Box`
/// requirement by delegating `subscribe` to the wrapped `Arc`,
/// avoiding a forced clone-and-reconstruct at every call site.
struct ArcProviderAdapter(Arc<dyn MarketDataProvider>);

#[async_trait]
impl MarketDataProvider for ArcProviderAdapter {
    async fn subscribe(
        &self,
        instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, ApexesError> {
        self.0.subscribe(instruments).await
    }
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
                leverage: None,
                margin_mode: None,
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
            source_range: None,
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
        initial_positions: HashMap::new(),
        // Live trading never simulates fees itself; `cost_model` exists
        // only for `funding_rate_bps` (disabled here).
        cost_model: CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: dec!(0),
                taker_bps: dec!(0),
            }),
            funding_rate_bps: None,
        },
        margin_tiers: HashMap::new(),
        market_condition_config: None,
        auto_derisk_live: false,
        initial_prices: HashMap::new(),
        symbol_rules: HashMap::new(),
        risk_free_return: dec!(0),
        reconcile_interval: Duration::from_secs(3600), // long enough not to fire during this test
        ems_postgres_url: POSTGRES_URL.to_string(),
        control: None,
        kill_switch: None,
        stale_data_timeout: None,
        resync: None,
        position_source: None,
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

    // MarketEventBus's broadcast stream does not end on its own even
    // after the mock provider's underlying events are exhausted (the
    // bus holds its sender open for its whole lifetime), so unlike
    // before this test's market-data source was wired through the
    // bus, `market_events.next()` no longer returns `None` once the
    // mock's finite events are drained -- the stop signal must be
    // sent explicitly instead of relying on a dropped `_tx`.
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let _ = tx.send(());
    });

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
                leverage: None,
                margin_mode: None,
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

/// Wraps `MockExecutionGateway`, counting every `submit_order` call so
/// the monitor-only liquidation test below can prove `run_live_trading`
/// never turns a liquidation-threshold breach into an order submission
/// (it must only log loudly, leaving real liquidation to the exchange).
struct CountingGateway {
    inner: MockExecutionGateway,
    submit_count: AtomicUsize,
}

#[async_trait]
impl ExecutionGateway for CountingGateway {
    async fn submit_order(&self, order: &Order) -> GatewayOutcome {
        self.submit_count.fetch_add(1, Ordering::SeqCst);
        self.inner.submit_order(order).await
    }

    async fn cancel_order(&self, order: &Order) -> GatewayOutcome {
        self.inner.cancel_order(order).await
    }

    async fn query_order_status(&self, order: &Order) -> Result<Option<OrderStatus>, String> {
        self.inner.query_order_status(order).await
    }

    async fn query_all_open_orders(
        &self,
        asset_class: AssetClass,
    ) -> Result<Vec<(OrderId, Option<String>, OrderStatus)>, String> {
        self.inner.query_all_open_orders(asset_class).await
    }

    async fn query_fills_for_order(&self, order: &Order) -> Result<Vec<ExchangeFill>, String> {
        self.inner.query_fills_for_order(order).await
    }
}

/// MONITOR-ONLY proof: seeds a 10x leveraged isolated long, then sends
/// a mark-price update far below its liquidation threshold. Even with
/// `check_liquidations` firing on that tick, the gateway must see ZERO
/// additional order submissions -- live trading only logs the breach.
#[tokio::test]
#[ignore]
async fn liquidation_breach_is_logged_but_never_submits_an_order() {
    use apexes_core::MarkPriceUpdate;

    clean_db().await;

    let gateway = Arc::new(CountingGateway {
        inner: MockExecutionGateway::new(),
        submit_count: AtomicUsize::new(0),
    });

    // A provider whose single mark-price update arrives 100ms in, so
    // the exchange-update branch is guaranteed to have applied the
    // leveraged fill (and thus created the position) first; the stream
    // then stays open so the loop survives until the stop signal.
    struct DelayedMarkPriceProvider;

    #[async_trait]
    impl MarketDataProvider for DelayedMarkPriceProvider {
        async fn subscribe(
            &self,
            _instruments: &[Instrument],
        ) -> Result<BoxStream<'static, MarketEvent>, apexes_core::ApexesError> {
            let event = MarketEvent::MarkPrice(MarkPriceUpdate {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                mark_price: dec!(50),
                timestamp: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            });
            Ok(Box::pin(
                futures::stream::once(async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    event
                })
                .chain(futures::stream::pending()),
            ))
        }
    }

    let provider: Arc<dyn MarketDataProvider> = Arc::new(DelayedMarkPriceProvider);
    let registry = registry_with(gateway.clone() as Arc<dyn ExecutionGateway>, provider);

    let store = OrderStore::new(POSTGRES_URL).await.unwrap();
    let mut order_manager = OrderManager::new(store, &registry).await.unwrap();

    // Seed a 10x leveraged isolated long (1 BTC @ 100) directly through
    // the OrderManager; its fill arrives via the exchange-updates
    // stream below, so the ledger holds a real leveraged position by
    // the time the mark-price event is processed.
    let order_id = order_manager
        .submit_order(
            OrderIntent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                asset_class: AssetClass::Perpetual,
                side: Side::Buy,
                order_type: OrderType::Market,
                quantity: dec!(1),
                price: None,
                stop_price: None,
                execution_algo: None,
                routing: OrderRouting::Fixed,
                leverage: Some(dec!(10)),
                margin_mode: Some(MarginMode::Isolated),
            },
            &HashMap::new(),
            &HashMap::new(),
        )
        .await
        .unwrap();
    gateway.inner.set_fills_for_order(
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

    let exchange_updates = futures::stream::iter(vec![ExchangeUpdate {
        client_order_id: order_id,
        exchange_order_id,
        status: OrderStatus::Filled,
    }])
    .chain(futures::stream::pending());

    let (tx, rx_stop) = oneshot::channel();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let _ = tx.send(());
    });

    // At mark 50 the position's margin ratio is (10 - 50) / (50 *
    // 0.005) = -160 -- deep past the 1.0 liquidation threshold.
    let mut margin_tiers = HashMap::new();
    margin_tiers.insert(
        "BTCUSDT".to_string(),
        MarginTierTable {
            tiers: vec![MarginTier {
                notional_floor: dec!(0),
                max_leverage: dec!(10),
                maintenance_margin_rate: dec!(0.005),
                maintenance_amount: dec!(0),
            }],
        },
    );
    let config = LiveTradingConfig {
        instruments: vec![
            Instrument::new(
                "BTCUSDT",
                MarketType::Crypto,
                Venue::Binance,
                AssetClass::Perpetual,
                Some("BTC".to_string()),
                Some("USDT".to_string()),
                dec!(0.01),
                dec!(0.0001),
                dec!(1),
            )
            .unwrap(),
        ],
        margin_tiers,
        ..base_config()
    };

    let report = run_live_trading(
        config,
        &registry,
        BuyOnceStrategy { bought: true }, // emits no intents
        order_manager,
        exchange_updates,
        rx_stop,
    )
    .await
    .unwrap();

    // The seeded order's fill was applied to the ledger...
    assert_eq!(report.trades.len(), 1);
    assert_eq!(report.trades[0].leverage, Some(dec!(10)));
    // ...and the seeded order remains the ONLY submission the gateway
    // ever saw: the liquidation breach on the mark-price event was
    // logged (tracing::error!), never turned into an order.
    assert_eq!(gateway.submit_count.load(Ordering::SeqCst), 1);
}

/// A `MarketDataProvider` whose stream delivers one seed-position-
/// enabling delay, then a sequence of `MarketEvent::Trade` prices for
/// the same symbol (to drive `MarketConditionMonitor`'s jump
/// detection), then stays open via `futures::stream::pending()`.
struct DelayedPriceJumpProvider {
    prices: Vec<rust_decimal::Decimal>,
}

#[async_trait]
impl MarketDataProvider for DelayedPriceJumpProvider {
    async fn subscribe(
        &self,
        _instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, apexes_core::ApexesError> {
        let prices = self.prices.clone();
        let events =
            futures::stream::iter(prices.into_iter().enumerate().map(|(i, price)| TradeEvent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                price,
                quantity: dec!(1),
                side: None,
                trade_id: None,
                timestamp: Utc.with_ymd_and_hms(2026, 1, 1, 0, i as u32, 0).unwrap(),
            }))
            .then(|trade| async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                MarketEvent::Trade(trade)
            });
        Ok(Box::pin(events.chain(futures::stream::pending())))
    }
}

fn seed_leveraged_long_config(auto_derisk_live: bool) -> LiveTradingConfig {
    LiveTradingConfig {
        instruments: vec![btc_usdt_instrument()],
        market_condition_config: Some(apexes_risk::MarketConditionConfig {
            volatility_threshold: dec!(1000000), // effectively disabled, isolate the jump signal
            jump_threshold_pct: dec!(0.10),
            sensitivity: dec!(1),
            volatility_window: 20,
            price_source: apexes_risk::PriceSource::Trade,
            account_equity_volatility_threshold: None,
            include_spot: false,
        }),
        auto_derisk_live,
        ..base_config()
    }
}

async fn seed_leveraged_long(
    order_manager: &mut OrderManager,
    gateway: &CountingGateway,
) -> OrderId {
    let order_id = order_manager
        .submit_order(
            OrderIntent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                asset_class: AssetClass::Perpetual,
                side: Side::Buy,
                order_type: OrderType::Market,
                quantity: dec!(1),
                price: None,
                stop_price: None,
                execution_algo: None,
                routing: OrderRouting::Fixed,
                leverage: Some(dec!(10)),
                margin_mode: Some(MarginMode::Isolated),
            },
            &HashMap::new(),
            &HashMap::new(),
        )
        .await
        .unwrap();
    gateway.inner.set_fills_for_order(
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
    order_id
}

/// MONITOR-ONLY-BY-DEFAULT proof: with `auto_derisk_live: false` (the
/// default), a 35% trade-price jump past the configured 10% threshold
/// must be logged (tracing::error!) and never turned into a de-risking
/// order submission -- matching row 6.4's live-trading-never-auto-
/// liquidates precedent. New orders for the affected symbol are
/// blocked via MarketConditionMonitor's PreTradeCheck instead.
#[tokio::test]
#[ignore]
async fn extreme_price_jump_is_logged_but_never_submits_an_order_by_default() {
    clean_db().await;

    let gateway = Arc::new(CountingGateway {
        inner: MockExecutionGateway::new(),
        submit_count: AtomicUsize::new(0),
    });
    // 100 -> 135 is a 35% jump, well past the 10% threshold configured
    // in seed_leveraged_long_config.
    let provider: Arc<dyn MarketDataProvider> = Arc::new(DelayedPriceJumpProvider {
        prices: vec![dec!(100), dec!(135)],
    });
    let registry = registry_with(gateway.clone() as Arc<dyn ExecutionGateway>, provider);

    let store = OrderStore::new(POSTGRES_URL).await.unwrap();
    let mut order_manager = OrderManager::new(store, &registry).await.unwrap();
    let order_id = seed_leveraged_long(&mut order_manager, &gateway).await;
    let exchange_order_id = order_manager
        .open_orders()
        .get(&order_id)
        .unwrap()
        .exchange_order_id
        .clone();
    let exchange_updates = futures::stream::iter(vec![ExchangeUpdate {
        client_order_id: order_id,
        exchange_order_id,
        status: OrderStatus::Filled,
    }])
    .chain(futures::stream::pending());

    let (tx, rx_stop) = oneshot::channel();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(600)).await;
        let _ = tx.send(());
    });

    let report = run_live_trading(
        seed_leveraged_long_config(false), // auto_derisk_live: false (default)
        &registry,
        BuyOnceStrategy { bought: true }, // emits no intents
        order_manager,
        exchange_updates,
        rx_stop,
    )
    .await
    .unwrap();

    // The seeded order's fill was applied to the ledger...
    assert_eq!(report.trades.len(), 1);
    // ...and the seeded order remains the ONLY submission the gateway
    // ever saw: the 35% price jump was logged (tracing::error!), never
    // turned into a de-risking order submission.
    assert_eq!(gateway.submit_count.load(Ordering::SeqCst), 1);
}

/// AUTO-DERISK opt-in proof: with `auto_derisk_live: true`, the same
/// 35% jump IS acted on -- a de-risking partial-close (Sell 1 BTC at
/// reduce_fraction 1.0) is submitted through the exchange gateway on
/// top of the seeded order.
#[tokio::test]
#[ignore]
async fn extreme_price_jump_submits_a_partial_close_when_auto_derisk_live_is_true() {
    clean_db().await;

    let gateway = Arc::new(CountingGateway {
        inner: MockExecutionGateway::new(),
        submit_count: AtomicUsize::new(0),
    });
    let provider: Arc<dyn MarketDataProvider> = Arc::new(DelayedPriceJumpProvider {
        prices: vec![dec!(100), dec!(135)],
    });
    let registry = registry_with(gateway.clone() as Arc<dyn ExecutionGateway>, provider);

    let store = OrderStore::new(POSTGRES_URL).await.unwrap();
    let mut order_manager = OrderManager::new(store, &registry).await.unwrap();
    let order_id = seed_leveraged_long(&mut order_manager, &gateway).await;
    let exchange_order_id = order_manager
        .open_orders()
        .get(&order_id)
        .unwrap()
        .exchange_order_id
        .clone();
    let exchange_updates = futures::stream::iter(vec![ExchangeUpdate {
        client_order_id: order_id,
        exchange_order_id,
        status: OrderStatus::Filled,
    }])
    .chain(futures::stream::pending());

    let (tx, rx_stop) = oneshot::channel();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(600)).await;
        let _ = tx.send(());
    });

    let report = run_live_trading(
        seed_leveraged_long_config(true), // auto_derisk_live: true
        &registry,
        BuyOnceStrategy { bought: true },
        order_manager,
        exchange_updates,
        rx_stop,
    )
    .await
    .unwrap();

    assert_eq!(report.trades.len(), 1);
    // The seed order PLUS a de-risking partial-close submission this
    // time -- auto_derisk_live: true means the 35% jump is actually
    // acted on.
    assert_eq!(gateway.submit_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn registering_a_market_event_bus_shares_one_upstream_connection() {
    // Uses apexes_message_bus's own reusable mock directly (not this
    // file's ArcProviderAdapter/registry_with helper), since this
    // test's purpose is proving the connection-sharing property of
    // MarketEventBus::subscribe itself when reached through this
    // crate's own Registry-based lookup pattern -- constructing the
    // Registry entry by hand here, matching subscribe_all_venues's
    // own lookup key exactly (Arc<MarketEventBus>, Venue).
    let mock = apexes_message_bus::test_support::MockMarketDataProvider::new();
    let handle = mock.handle();
    let bus = Arc::new(MarketEventBus::new(Box::new(mock)));

    let registry = Registry::new();
    registry
        .register::<Arc<MarketEventBus>, Venue>(Venue::Binance, Arc::clone(&bus))
        .unwrap();

    let looked_up = registry.get::<Arc<MarketEventBus>, Venue>();
    let looked_up_bus = looked_up.get(&Venue::Binance).unwrap();

    let instrument = Instrument::new(
        "BTCUSDT",
        apexes_core::MarketType::Crypto,
        Venue::Binance,
        apexes_core::AssetClass::Spot,
        Some("BTC".to_string()),
        Some("USDT".to_string()),
        dec!(0.01),
        dec!(0.0001),
        dec!(1),
    )
    .unwrap();
    let instruments = vec![instrument];

    let _receiver_1 = looked_up_bus.subscribe(&instruments).await.unwrap();
    let _receiver_2 = looked_up_bus.subscribe(&instruments).await.unwrap();

    assert_eq!(
        handle.subscribe_call_count(),
        1,
        "two subscribe() calls through the same registered MarketEventBus must share one upstream connection"
    );
}

/// A gateway whose order queries hang (a slow exchange, or a dead address
/// waiting out its connect timeout). Everything else is the mock.
struct HangingQueryGateway {
    inner: MockExecutionGateway,
    hang_for: Duration,
}

#[async_trait]
impl ExecutionGateway for HangingQueryGateway {
    async fn submit_order(&self, order: &Order) -> GatewayOutcome {
        self.inner.submit_order(order).await
    }
    async fn cancel_order(&self, order: &Order) -> GatewayOutcome {
        self.inner.cancel_order(order).await
    }
    async fn query_order_status(&self, order: &Order) -> Result<Option<OrderStatus>, String> {
        tokio::time::sleep(self.hang_for).await;
        self.inner.query_order_status(order).await
    }
    async fn query_all_open_orders(
        &self,
        asset_class: AssetClass,
    ) -> Result<Vec<(OrderId, Option<String>, OrderStatus)>, String> {
        tokio::time::sleep(self.hang_for).await;
        self.inner.query_all_open_orders(asset_class).await
    }
    async fn query_fills_for_order(&self, order: &Order) -> Result<Vec<ExchangeFill>, String> {
        self.inner.query_fills_for_order(order).await
    }
}

/// A feed that ticks every 100 ms forever.
struct SteadyFeed;

#[async_trait]
impl MarketDataProvider for SteadyFeed {
    async fn subscribe(
        &self,
        _instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, apexes_core::ApexesError> {
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        Ok(Box::pin(futures::stream::unfold(
            0i64,
            move |n| async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let event = MarketEvent::Trade(TradeEvent {
                    symbol: "BTCUSDT".to_string(),
                    venue: Venue::Binance,
                    price: dec!(100),
                    quantity: dec!(1),
                    side: None,
                    trade_id: None,
                    timestamp: t0 + chrono::Duration::milliseconds(n * 100),
                });
                Some((event, n + 1))
            },
        )))
    }
}

/// Reproduces the real-testnet halt: "market data stale: no events for 37.4s"
/// while the feed was healthy. With an open order, every reconcile tick queries
/// the exchange while HOLDING the order-manager lock, and the trading loop
/// needs that same lock on every market event (it asks for the open orders).
/// A slow or hanging exchange query therefore stalls event processing, the
/// activity clock stops being touched, and the staleness watchdog halts a
/// session whose feed is fine.
#[tokio::test]
#[ignore]
async fn a_slow_exchange_query_must_not_stall_market_events_or_trip_the_stale_data_halt() {
    clean_db().await;
    let gateway = Arc::new(HangingQueryGateway {
        inner: MockExecutionGateway::new(),
        hang_for: Duration::from_secs(4),
    });
    let provider: Arc<dyn MarketDataProvider> = Arc::new(SteadyFeed);
    let registry = registry_with(Arc::clone(&gateway) as Arc<dyn ExecutionGateway>, provider);
    let store = OrderStore::new(POSTGRES_URL).await.unwrap();
    let mut order_manager = OrderManager::new(store, &registry).await.unwrap();
    // One open order, so reconcile has something to query.
    order_manager
        .submit_order(
            OrderIntent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                asset_class: AssetClass::Spot,
                side: Side::Buy,
                order_type: OrderType::Limit,
                quantity: dec!(1),
                price: Some(dec!(1)),
                stop_price: None,
                execution_algo: None,
                routing: OrderRouting::Fixed,
                leverage: None,
                margin_mode: None,
            },
            &HashMap::new(),
            &HashMap::new(),
        )
        .await
        .unwrap();

    let kill_switch = Arc::new(apexes_oms::KillSwitch::new());
    let mut config = base_config();
    config.reconcile_interval = Duration::from_millis(500);
    config.kill_switch = Some(Arc::clone(&kill_switch));
    // The feed ticks every 0.1 s, so 2 s without an event is unmistakably a stall.
    config.stale_data_timeout = Some(Duration::from_secs(2));

    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(12)).await;
        let _ = tx.send(());
    });
    let seen = Arc::new(AtomicUsize::new(0));
    let outcome = tokio::time::timeout(
        Duration::from_secs(40),
        run_live_trading(
            config,
            &registry,
            CountingStrategy {
                seen: Arc::clone(&seen),
            },
            order_manager,
            futures::stream::empty::<ExchangeUpdate>(),
            rx,
        ),
    )
    .await;

    // The feed ticks every 0.1 s for 12 s: ~120 events if nothing stalls.
    let processed = seen.load(Ordering::SeqCst);
    println!("events processed by the strategy: {processed} (feed produced ~120)");
    assert!(
        outcome.is_ok(),
        "the run did not stop within 40 s of the stop signal (only {processed} events processed): \
         the trading loop is blocked behind exchange queries"
    );
    assert!(
        !kill_switch.is_engaged(),
        "the feed never went quiet, yet trading was halted: {:?} (strategy saw {processed} events)",
        kill_switch.reason()
    );
}

struct CountingStrategy {
    seen: Arc<AtomicUsize>,
}

impl Strategy for CountingStrategy {
    fn on_market_event(
        &mut self,
        _event: &MarketEvent,
        _ctx: &StrategyContext,
    ) -> Vec<OrderIntent> {
        self.seen.fetch_add(1, Ordering::SeqCst);
        Vec::new()
    }
}

/// Exercise current-run direct and EMS callback attribution, and an older
/// run's identical ordinal, through the real managed journal and live loop.
#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn managed_live_callbacks_use_persisted_scope_and_dispatch() {
    let admin = sqlx::PgPool::connect(POSTGRES_URL).await.unwrap();
    let schema = format!("live_fill_test_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let url = format!("{POSTGRES_URL}?options[search_path]={schema}");
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let gateway = Arc::new(MockExecutionGateway::new());
    let registry = registry_with(gateway.clone(), Arc::new(SteadyFeed));
    let health = Arc::new(apexes_oms::KillSwitch::managed());
    let mut store = OrderStore::new(&url).await.unwrap();
    store
        .bind_spot_inventory("managed-live-fill-test")
        .await
        .unwrap();
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
                intent_id: apexes_strategy::IntentId(1),
            },
        )
        .await
        .unwrap();
    let old_exchange = manager
        .open_orders()
        .get(&old_order)
        .unwrap()
        .exchange_order_id
        .clone();
    let (updates, stream) = futures::channel::mpsc::unbounded();
    publish_mock_fill(&gateway, &updates, old_order, old_exchange, "old-run");
    let driver_pool = pool.clone();
    let driver = tokio::spawn(async move {
        let direct = wait_for_callback_order(&driver_pool, old_scope, false).await;
        publish_mock_fill(&gateway, &updates, direct.0, direct.1, "current-direct");
        let child = wait_for_callback_order(&driver_pool, old_scope, true).await;
        publish_mock_fill(&gateway, &updates, child.0, child.1, "current-child");
        (direct.0, child.0, child.2.unwrap())
    });
    let mut config = base_config();
    config.ems_postgres_url = url.clone();
    config.kill_switch = Some(health.clone());
    let (stop, receiver) = oneshot::channel();
    let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let terminals = Arc::new(std::sync::Mutex::new(Vec::new()));
    let report = tokio::time::timeout(
        Duration::from_secs(45),
        run_live_trading(
            config,
            &registry,
            ManagedCallbackProbe {
                stage: 0,
                observed: observed.clone(),
                terminals: terminals.clone(),
                stop: Some(stop),
            },
            manager,
            stream,
            receiver,
        ),
    )
    .await
    .unwrap_or_else(|err| {
        panic!(
            "managed callback runner timed out: {err}; health={:?}; fills={:?}; terminals={:?}",
            health.reason(),
            observed.lock().unwrap(),
            terminals.lock().unwrap()
        )
    })
    .unwrap();
    let (direct, child, parent) = driver.await.unwrap();
    let observed = observed.lock().unwrap().clone();
    assert_eq!(
        observed,
        vec![
            (old_order, None, None, None),
            (direct, Some(apexes_strategy::IntentId(1)), None, None),
            (
                child,
                Some(apexes_strategy::IntentId(2)),
                Some(apexes_strategy::ParentOrderId(parent)),
                Some(dec!(1))
            ),
        ]
    );
    assert_eq!(report.trades.len(), 3);
    let terminals = terminals.lock().unwrap().clone();
    assert_eq!(terminals.len(), 3);
    assert_eq!(
        terminals
            .iter()
            .map(|t| (t.0, t.1, t.2, t.4))
            .collect::<Vec<_>>(),
        observed
            .iter()
            .map(|f| (f.0, f.1, f.2, dec!(1)))
            .collect::<Vec<_>>()
    );
    let dispatch: uuid::Uuid = sqlx::query_scalar("SELECT ems_dispatch_id FROM orders WHERE id=$1")
        .bind(child.0)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(terminals[0].3, None);
    assert_eq!(terminals[1].3, None);
    assert_eq!(terminals[2].3, Some(dispatch));
    assert!(!health.is_engaged(), "{:?}", health.reason());
    pool.close().await;
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
}

fn callback_probe_intent(algo: bool) -> OrderIntent {
    OrderIntent {
        symbol: "BTCUSDT".into(),
        venue: Venue::Binance,
        asset_class: AssetClass::Spot,
        side: Side::Buy,
        order_type: OrderType::Market,
        quantity: dec!(1),
        price: None,
        stop_price: None,
        execution_algo: algo.then_some(apexes_strategy::ExecutionAlgoSpec::Pov {
            participation_rate: dec!(1),
        }),
        routing: OrderRouting::Fixed,
        leverage: None,
        margin_mode: None,
    }
}

// CI can run this alongside Docker builds on a contended runner. Keep
// a bounded, generous harness deadline, distinct from production OMS/EMS
// health and submission deadlines. Report durable state on failure.
async fn wait_for_callback_order(
    pool: &sqlx::PgPool,
    old_scope: uuid::Uuid,
    child: bool,
) -> (OrderId, Option<String>, Option<uuid::Uuid>) {
    let result = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let row: Option<(uuid::Uuid, Option<String>, Option<uuid::Uuid>)> = sqlx::query_as(
                "SELECT id,exchange_order_id,ems_parent_id FROM orders WHERE submission_state='Accepted'
                 AND (($2 AND ems_parent_id IS NOT NULL) OR (NOT $2 AND ems_parent_id IS NULL AND intent_scope<>$1))",
            )
            .bind(old_scope)
            .bind(child)
            .fetch_optional(pool)
            .await
            .unwrap();
            if let Some((id, exchange_id, parent)) = row {
                break (OrderId(id), exchange_id, parent);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await;
    match result {
        Ok(row) => row,
        Err(err) => {
            let rows = sqlx::query_as::<_, (uuid::Uuid, String, String, Option<uuid::Uuid>)>(
                "SELECT id, status, submission_state, ems_parent_id FROM orders ORDER BY created_at"
            ).fetch_all(pool).await;
            panic!(
                "timed out waiting for accepted {} order: {err}; journal={rows:?}",
                if child { "EMS child" } else { "direct" }
            );
        }
    }
}

fn publish_mock_fill(
    gateway: &MockExecutionGateway,
    updates: &futures::channel::mpsc::UnboundedSender<ExchangeUpdate>,
    order: OrderId,
    exchange_id: Option<String>,
    trade_id: &str,
) {
    gateway.set_fills_for_order(
        order,
        vec![ExchangeFill {
            exchange_trade_id: trade_id.into(),
            price: dec!(100),
            quantity: dec!(1),
            fee: dec!(0),
            fee_currency: None,
            filled_at: Utc::now(),
        }],
    );
    gateway.set_exchange_state(order, OrderStatus::Filled);
    updates
        .unbounded_send(ExchangeUpdate {
            client_order_id: order,
            exchange_order_id: exchange_id,
            status: OrderStatus::Filled,
        })
        .unwrap();
}

type CallbackObservation = (
    OrderId,
    Option<apexes_strategy::IntentId>,
    Option<apexes_strategy::ParentOrderId>,
    Option<rust_decimal::Decimal>,
);

type TerminalObservation = (
    OrderId,
    Option<apexes_strategy::IntentId>,
    Option<apexes_strategy::ParentOrderId>,
    Option<uuid::Uuid>,
    rust_decimal::Decimal,
);

struct ManagedCallbackProbe {
    stage: u8,
    observed: Arc<std::sync::Mutex<Vec<CallbackObservation>>>,
    terminals: Arc<std::sync::Mutex<Vec<TerminalObservation>>>,
    stop: Option<oneshot::Sender<()>>,
}

impl Strategy for ManagedCallbackProbe {
    fn on_market_event(&mut self, event: &MarketEvent, _: &StrategyContext) -> Vec<OrderIntent> {
        if matches!(event, MarketEvent::Trade(_)) && matches!(self.stage, 0 | 2) {
            let algo = self.stage == 2;
            self.stage += 1;
            vec![callback_probe_intent(algo)]
        } else {
            Vec::new()
        }
    }

    fn on_fill(&mut self, outcome: &apexes_strategy::FillOutcome<'_>) {
        let mut observed = self.observed.lock().unwrap();
        observed.push((
            outcome.fill.order_id,
            outcome.intent_id,
            outcome.parent_id,
            outcome.parent_executed_quantity,
        ));
        if outcome.intent_id == Some(apexes_strategy::IntentId(1)) && self.stage == 1 {
            self.stage = 2;
        }
    }

    fn on_order_terminal(&mut self, outcome: &apexes_strategy::TerminalOutcome<'_>) {
        use apexes_strategy::{LiveTerminalStatus, TerminalKind};
        let TerminalKind::Live {
            order,
            status,
            parent_id,
            dispatch_id,
        } = outcome.kind
        else {
            panic!("unexpected simulated terminal in live loop");
        };
        assert_eq!(status, LiveTerminalStatus::Filled);
        assert!(
            self.observed
                .lock()
                .unwrap()
                .iter()
                .any(|fill| fill.0 == order.id),
            "terminal callback must follow the fill callback"
        );
        let mut terminals = self.terminals.lock().unwrap();
        terminals.push((
            order.id,
            outcome.intent_id,
            parent_id,
            dispatch_id,
            order.filled_quantity,
        ));
        if terminals.len() == 3
            && let Some(stop) = self.stop.take()
        {
            let _ = stop.send(());
        }
    }
}

/// Terminal-only work must wake an idle loop without a market event or
/// artificial fill/equity observation.
#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn managed_zero_fill_terminal_wakes_idle_loop() {
    let admin = sqlx::PgPool::connect(POSTGRES_URL).await.unwrap();
    let schema = format!("live_terminal_test_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let url = format!("{POSTGRES_URL}?options[search_path]={schema}");
    let gateway = Arc::new(MockExecutionGateway::new());
    let provider = Arc::new(MockMarketDataProvider {
        events: vec![],
        keep_open: true,
    });
    let registry = registry_with(gateway.clone(), provider);
    let health = Arc::new(apexes_oms::KillSwitch::managed());
    let mut store = OrderStore::new(&url).await.unwrap();
    store
        .bind_spot_inventory("managed-live-terminal-test")
        .await
        .unwrap();
    let mut manager = OrderManager::new(store, &registry)
        .await
        .unwrap()
        .with_kill_switch(health.clone());
    manager.spot_inventory_fills().await.unwrap();
    health.begin_reconciliation();
    health.startup_ready();
    let order_id = manager
        .submit_correlated_order(
            callback_probe_intent(false),
            &HashMap::new(),
            &HashMap::new(),
            apexes_oms::IntentLink {
                run_scope: uuid::Uuid::new_v4(),
                intent_id: apexes_strategy::IntentId(1),
            },
        )
        .await
        .unwrap();
    let exchange_id = manager
        .open_orders()
        .get(&order_id)
        .unwrap()
        .exchange_order_id
        .clone();
    let (updates, stream) = futures::channel::mpsc::unbounded();
    let (stop, receiver) = oneshot::channel();
    let driver = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(250)).await;
        gateway.set_exchange_state(order_id, OrderStatus::Cancelled);
        updates
            .unbounded_send(ExchangeUpdate {
                client_order_id: order_id,
                exchange_order_id: exchange_id,
                status: OrderStatus::Cancelled,
            })
            .unwrap();
    });
    let mut config = base_config();
    config.ems_postgres_url = url.clone();
    config.kill_switch = Some(health.clone());
    let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let report = tokio::time::timeout(
        Duration::from_secs(5),
        run_live_trading(
            config,
            &registry,
            IdleTerminalProbe {
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
    driver.await.unwrap();
    assert!(report.trades.is_empty());
    assert_eq!(*observed.lock().unwrap(), vec![order_id]);
    assert!(!health.is_engaged(), "{:?}", health.reason());
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
}

struct IdleTerminalProbe {
    observed: Arc<std::sync::Mutex<Vec<OrderId>>>,
    stop: Option<oneshot::Sender<()>>,
}

impl Strategy for IdleTerminalProbe {
    fn on_market_event(&mut self, _: &MarketEvent, _: &StrategyContext) -> Vec<OrderIntent> {
        panic!("no market event expected");
    }
    fn on_fill(&mut self, _: &apexes_strategy::FillOutcome<'_>) {
        panic!("zero-fill cancellation must not manufacture a fill");
    }
    fn on_order_terminal(&mut self, outcome: &apexes_strategy::TerminalOutcome<'_>) {
        use apexes_strategy::{LiveTerminalStatus, TerminalKind};
        let TerminalKind::Live {
            order,
            status,
            parent_id,
            dispatch_id,
        } = outcome.kind
        else {
            panic!("unexpected simulated terminal");
        };
        assert_eq!(status, LiveTerminalStatus::Cancelled);
        assert_eq!(order.filled_quantity, dec!(0));
        assert_eq!(
            (outcome.intent_id, parent_id, dispatch_id),
            (None, None, None)
        );
        self.observed.lock().unwrap().push(order.id);
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

#[path = "lifecycle_recovery/mod.rs"]
mod lifecycle_recovery;

#[path = "strategy_recovery/mod.rs"]
mod strategy_recovery;
