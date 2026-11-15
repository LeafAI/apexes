//! Position flattening for the kill switch's optional "flatten" step.
//!
//! Runs after the cancel-all sweep (so resting orders cannot change the
//! position underneath us). Positions come from the EXCHANGE, not the local
//! ledger: a halt is usually triggered because something is wrong, and the
//! exchange is the only state that cannot be wrong about what is held.
//!
//! Only derivative positions are flattened. Spot balances are holdings, not
//! exposure to be unwound; selling them is a policy decision for an
//! operator, not something a halt should do on its own.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use apexes_core::{Instrument, OrderId, OrderType, Side};
use apexes_oms::OrderManager;
use apexes_strategy::{OrderIntent, OrderRouting, Position};
use async_trait::async_trait;
use rust_decimal::Decimal;

/// Where open exchange positions are read from.
#[async_trait]
pub trait PositionSource: Send + Sync {
    /// Currently open (non-zero) derivative positions, from the exchange.
    async fn open_positions(&self) -> Result<Vec<Position>, String>;
}

/// Everything the halt task needs to flatten.
#[derive(Clone)]
pub struct FlattenSetup {
    pub source: Arc<dyn PositionSource>,
    /// symbol -> instrument, for venue/asset class of the closing order.
    pub instruments: Arc<HashMap<String, Instrument>>,
    pub config: FlattenConfig,
}

#[derive(Debug, Clone, Copy)]
pub struct FlattenConfig {
    /// Poll/submit rounds before giving up and reporting what is left.
    pub max_rounds: u32,
    /// Pause between rounds, letting closing orders fill.
    pub poll_interval: Duration,
}

impl Default for FlattenConfig {
    fn default() -> Self {
        Self {
            max_rounds: 10,
            poll_interval: Duration::from_secs(2),
        }
    }
}

#[derive(Debug, Default)]
pub struct FlattenReport {
    pub orders_submitted: usize,
    /// Symbols still holding a position when the rounds ran out.
    pub remaining: Vec<String>,
}

impl FlattenReport {
    pub fn is_flat(&self) -> bool {
        self.remaining.is_empty()
    }
}

/// Closes every open position with market orders, re-polling the exchange
/// until it reports flat or `max_rounds` is exhausted. Never double-submits:
/// a symbol with a closing order still open in the order manager is waited
/// on, not re-ordered.
pub async fn flatten_positions(
    setup: &FlattenSetup,
    order_manager: &tokio::sync::Mutex<OrderManager>,
) -> FlattenReport {
    let telemetry = apexes_telemetry::metrics();
    let mut report = FlattenReport::default();
    // Closing order currently working, per symbol.
    let mut in_flight: HashMap<String, OrderId> = HashMap::new();

    for round in 0..setup.config.max_rounds {
        let positions = match setup.source.open_positions().await {
            Ok(positions) => positions
                .into_iter()
                .filter(|p| !p.quantity.is_zero())
                .collect::<Vec<_>>(),
            Err(reason) => {
                tracing::error!(
                    reason,
                    round,
                    "flatten: cannot read exchange positions; retrying"
                );
                tokio::time::sleep(setup.config.poll_interval).await;
                continue;
            }
        };
        telemetry.unflattened_positions.set(positions.len() as i64);
        if positions.is_empty() {
            tracing::warn!(
                orders = report.orders_submitted,
                "flatten: exchange reports flat"
            );
            report.remaining.clear();
            return report;
        }
        report.remaining = positions.iter().map(|p| p.symbol.clone()).collect();

        for position in &positions {
            {
                let manager = order_manager.lock().await;
                if let Some(id) = in_flight.get(&position.symbol)
                    && manager.open_orders().contains_key(id)
                {
                    continue; // previous closing order still working
                }
            }
            let Some(instrument) = setup.instruments.get(&position.symbol) else {
                tracing::error!(symbol = %position.symbol, "flatten: no instrument configured for this open position; CANNOT close it");
                continue;
            };
            let intent = closing_intent(position, instrument);
            match order_manager
                .lock()
                .await
                .submit_flatten_order(intent)
                .await
            {
                Ok(id) => {
                    in_flight.insert(position.symbol.clone(), id);
                    report.orders_submitted += 1;
                    telemetry.flatten_orders.inc();
                }
                Err(error) => {
                    tracing::error!(?error, symbol = %position.symbol, "flatten: closing order failed");
                }
            }
        }
        tokio::time::sleep(setup.config.poll_interval).await;
    }

    tracing::error!(remaining = ?report.remaining, "flatten: gave up with positions still open");
    report
}

fn closing_intent(position: &Position, instrument: &Instrument) -> OrderIntent {
    OrderIntent {
        symbol: position.symbol.clone(),
        venue: instrument.venue,
        asset_class: instrument.asset_class,
        // Long closes with a sell, short with a buy.
        side: if position.quantity > Decimal::ZERO {
            Side::Sell
        } else {
            Side::Buy
        },
        order_type: OrderType::Market,
        quantity: position.quantity.abs(),
        price: None,
        stop_price: None,
        execution_algo: None,
        routing: OrderRouting::Fixed,
        leverage: position.leverage,
        margin_mode: position.margin_mode,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::registry::Registry;
    use apexes_core::{AssetClass, MarketType, OrderStatus, Venue};
    use apexes_oms::{ExecutionGateway, KillSwitch, MockExecutionGateway, OrderStore};
    use rust_decimal_macros::dec;
    use std::sync::Mutex;

    const POSTGRES_URL: &str = "postgres://apexes:apexes@localhost:5432/apexes";

    /// Serialises these tests: they share and TRUNCATE one database.
    static DB_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    struct FakeSource(Mutex<Vec<Position>>);

    #[async_trait]
    impl PositionSource for FakeSource {
        async fn open_positions(&self) -> Result<Vec<Position>, String> {
            Ok(self.0.lock().unwrap().clone())
        }
    }

    fn position(symbol: &str, quantity: Decimal) -> Position {
        Position {
            symbol: symbol.to_string(),
            quantity,
            average_entry_price: dec!(100),
            leverage: Some(dec!(3)),
            margin_mode: Some(apexes_core::MarginMode::Cross),
            allocated_margin: Decimal::ZERO,
        }
    }

    fn instrument(symbol: &str) -> Instrument {
        Instrument::new(
            symbol,
            MarketType::Crypto,
            Venue::Binance,
            AssetClass::Perpetual,
            Some("BTC".to_string()),
            Some("USDT".to_string()),
            dec!(0.01),
            dec!(0.001),
            dec!(1),
        )
        .unwrap()
    }

    fn fast() -> FlattenConfig {
        FlattenConfig {
            max_rounds: 6,
            poll_interval: Duration::from_millis(40),
        }
    }

    struct Fixture {
        manager: Arc<tokio::sync::Mutex<OrderManager>>,
        store: OrderStore,
        _guard: tokio::sync::MutexGuard<'static, ()>,
    }

    /// Order manager whose kill switch is ALREADY engaged, as in a real halt.
    async fn fixture() -> Fixture {
        let guard = DB_LOCK.lock().await;
        let store = OrderStore::new(POSTGRES_URL).await.unwrap();
        sqlx::query("TRUNCATE fills, orders")
            .execute(&sqlx::PgPool::connect(POSTGRES_URL).await.unwrap())
            .await
            .unwrap();
        let registry = Registry::new();
        registry
            .register::<Arc<dyn ExecutionGateway>, ()>(
                (),
                Arc::new(MockExecutionGateway::new()) as Arc<dyn ExecutionGateway>,
            )
            .unwrap();
        let kill_switch = Arc::new(KillSwitch::new());
        kill_switch.engage("test halt");
        let manager = OrderManager::new(OrderStore::new(POSTGRES_URL).await.unwrap(), &registry)
            .await
            .unwrap()
            .with_kill_switch(kill_switch);
        Fixture {
            manager: Arc::new(tokio::sync::Mutex::new(manager)),
            store,
            _guard: guard,
        }
    }

    fn setup(source: Arc<FakeSource>, symbols: &[&str]) -> FlattenSetup {
        FlattenSetup {
            source,
            instruments: Arc::new(
                symbols
                    .iter()
                    .map(|s| (s.to_string(), instrument(s)))
                    .collect(),
            ),
            config: fast(),
        }
    }

    async fn open_orders(f: &Fixture) -> Vec<apexes_core::Order> {
        f.store.get_open_orders().await.unwrap()
    }

    #[tokio::test]
    async fn closes_long_and_short_positions_despite_the_engaged_kill_switch() {
        let f = fixture().await;
        let source = Arc::new(FakeSource(Mutex::new(vec![
            position("BTCUSDT", dec!(0.5)),
            position("ETHUSDT", dec!(-2)),
        ])));
        let s = setup(Arc::clone(&source), &["BTCUSDT", "ETHUSDT"]);

        let manager = Arc::clone(&f.manager);
        let task = tokio::spawn(async move { flatten_positions(&s, &manager).await });
        tokio::time::sleep(Duration::from_millis(120)).await;
        // Exchange now reports flat (the closing orders "filled").
        source.0.lock().unwrap().clear();
        let report = task.await.unwrap();

        assert!(report.is_flat());
        assert_eq!(report.orders_submitted, 2);
        let orders = open_orders(&f).await;
        let btc = orders.iter().find(|o| o.symbol == "BTCUSDT").unwrap();
        let eth = orders.iter().find(|o| o.symbol == "ETHUSDT").unwrap();
        assert_eq!((btc.side, btc.quantity), (Side::Sell, dec!(0.5)));
        assert_eq!((eth.side, eth.quantity), (Side::Buy, dec!(2)));
        assert!(orders.iter().all(|o| o.order_type == OrderType::Market));

        // And the normal path is still blocked.
        let blocked = f
            .manager
            .lock()
            .await
            .submit_order(
                closing_intent(&position("BTCUSDT", dec!(1)), &instrument("BTCUSDT")),
                &HashMap::new(),
                &HashMap::new(),
            )
            .await;
        assert!(matches!(
            blocked,
            Err(apexes_oms::OmsError::TradingHalted { .. })
        ));
    }

    #[tokio::test]
    async fn does_not_double_submit_while_the_closing_order_is_still_working() {
        let f = fixture().await;
        // Position never changes: the closing order stays open the whole time.
        let source = Arc::new(FakeSource(Mutex::new(vec![position("BTCUSDT", dec!(1))])));
        let s = setup(source, &["BTCUSDT"]);

        let report = flatten_positions(&s, &f.manager).await;

        assert_eq!(report.orders_submitted, 1, "one order, however many polls");
        assert_eq!(report.remaining, vec!["BTCUSDT".to_string()]);
        assert!(!report.is_flat());
        assert_eq!(open_orders(&f).await.len(), 1);
    }

    #[tokio::test]
    async fn resubmits_when_the_closing_order_ended_without_flattening() {
        let f = fixture().await;
        let source = Arc::new(FakeSource(Mutex::new(vec![position("BTCUSDT", dec!(1))])));
        let mut s = setup(Arc::clone(&source), &["BTCUSDT"]);
        // Driven by the position changes below, not by running out of rounds.
        s.config.max_rounds = 500;

        let manager = Arc::clone(&f.manager);
        let task = tokio::spawn(async move { flatten_positions(&s, &manager).await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        // First closing order got cancelled by the exchange; position remains.
        let first = open_orders(&f).await.remove(0).id;
        f.manager
            .lock()
            .await
            .apply_exchange_update(first, None, OrderStatus::Cancelled)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        source.0.lock().unwrap().clear();
        let report = task.await.unwrap();

        assert!(report.is_flat());
        assert_eq!(report.orders_submitted, 2, "a replacement was submitted");
    }

    #[tokio::test]
    async fn open_position_without_an_instrument_is_reported_not_silently_dropped() {
        let f = fixture().await;
        let source = Arc::new(FakeSource(Mutex::new(vec![position("XYZUSDT", dec!(1))])));
        let s = setup(source, &["BTCUSDT"]); // XYZUSDT unknown

        let report = flatten_positions(&s, &f.manager).await;

        assert_eq!(report.orders_submitted, 0);
        assert_eq!(report.remaining, vec!["XYZUSDT".to_string()]);
    }

    /// The whole halt path: resting orders are cancelled FIRST, then the
    /// position is closed -- and only when the engagement asked for it.
    async fn halt_with_resting_order(flatten_requested: bool) -> (Fixture, Arc<FakeSource>) {
        let guard = DB_LOCK.lock().await;
        let store = OrderStore::new(POSTGRES_URL).await.unwrap();
        sqlx::query("TRUNCATE fills, orders")
            .execute(&sqlx::PgPool::connect(POSTGRES_URL).await.unwrap())
            .await
            .unwrap();
        let registry = Registry::new();
        registry
            .register::<Arc<dyn ExecutionGateway>, ()>(
                (),
                Arc::new(MockExecutionGateway::new()) as Arc<dyn ExecutionGateway>,
            )
            .unwrap();
        let kill_switch = Arc::new(KillSwitch::new());
        let mut om = OrderManager::new(OrderStore::new(POSTGRES_URL).await.unwrap(), &registry)
            .await
            .unwrap()
            .with_kill_switch(Arc::clone(&kill_switch));
        // A resting limit order placed while trading is still allowed.
        let resting = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Perpetual,
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: dec!(1),
            price: Some(dec!(50)),
            stop_price: None,
            execution_algo: None,
            routing: OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        };
        om.submit_order(resting, &HashMap::new(), &HashMap::new())
            .await
            .unwrap();
        let manager = Arc::new(tokio::sync::Mutex::new(om));

        let source = Arc::new(FakeSource(Mutex::new(vec![position("BTCUSDT", dec!(0.5))])));
        let _task = crate::halt::spawn_cancel_all_task(
            Arc::clone(&kill_switch),
            Arc::clone(&manager),
            Some(setup(Arc::clone(&source), &["BTCUSDT"])),
        );
        if flatten_requested {
            kill_switch.request_flatten();
        }
        kill_switch.engage("operator halt");
        (
            Fixture {
                manager,
                store,
                _guard: guard,
            },
            source,
        )
    }

    #[tokio::test]
    async fn halt_with_flatten_cancels_resting_orders_then_closes_the_position() {
        let (f, source) = halt_with_resting_order(true).await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        let open = open_orders(&f).await;
        // The resting limit is gone (cancelled); what is open is the closing market order.
        assert!(
            open.iter().all(|o| o.order_type == OrderType::Market),
            "{open:?}"
        );
        let closing: Vec<_> = open.iter().filter(|o| o.side == Side::Sell).collect();
        assert_eq!(closing.len(), 1, "{open:?}");
        assert_eq!(closing[0].quantity, dec!(0.5));
        source.0.lock().unwrap().clear();
    }

    #[tokio::test]
    async fn halt_without_flatten_cancels_orders_but_leaves_the_position() {
        let (f, _source) = halt_with_resting_order(false).await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        assert!(
            open_orders(&f).await.is_empty(),
            "resting order must be cancelled, nothing placed"
        );
    }
}
