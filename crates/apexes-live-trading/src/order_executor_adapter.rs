//! [`OrderManagerExecutor`]/[`LiveLiquidationPolicy`]/
//! [`LiveSupplementalFillSource`]: adapt `apexes_oms::OrderManager` and
//! today's live-trading auto-liquidation behavior to the three
//! `apexes_strategy` traits (`OrderExecutor`/`LiquidationPolicy`/
//! `SupplementalFillSource`) a future unified event-loop core drives,
//! abstracting over live trading (this module) vs. backtest/paper (the
//! sibling adapter in `apexes-backtest::order_executor_adapter`).
//!
//! NOT wired into `run_live_trading` yet -- a later plan task builds the
//! unified loop and switches `run_live_trading` over to it. This module
//! has zero behavioral impact on any existing code path today.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use apexes_core::{ApexesError, Fill, MarketEvent, Order, OrderId};
use apexes_oms::{ExchangeUpdate, OrderManager};
use apexes_strategy::{
    LiquidationPolicy, LiquidationReason, OrderExecutor, OrderIntent, Position,
    SupplementalFillSource,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt};
use rust_decimal::Decimal;

/// Adapts `OrderManager` (live trading's real, asynchronous order
/// submission/tracking backend) to `OrderExecutor`. Wraps the SAME
/// `Arc<tokio::sync::Mutex<OrderManager>>` `run_live_trading` already
/// constructs and shares with `apexes-ems`'s `OrderManagerChildOrderSink`
/// -- exactly the shared-instance precedent
/// `apexes-backtest::MatchingEngineOrderExecutor` follows for its own
/// shared `MatchingEngine`.
pub struct OrderManagerExecutor {
    order_manager: Arc<tokio::sync::Mutex<OrderManager>>,
    /// The open orders as of the last time the manager's lock was obtained.
    /// Served when the lock is busy, so the trading loop never stalls behind a
    /// slow exchange query (see [`OrderManagerExecutor::open_orders`]).
    last_open_orders: std::sync::Mutex<HashMap<OrderId, Order>>,
}

/// How long the loop waits for the order manager before using its last known
/// view of the open orders. Uncontended acquisition is immediate; this only
/// bites while a reconcile or submission is talking to the exchange.
const OPEN_ORDERS_LOCK_WAIT: Duration = Duration::from_millis(50);

impl OrderManagerExecutor {
    pub fn new(order_manager: Arc<tokio::sync::Mutex<OrderManager>>) -> Self {
        Self {
            order_manager,
            last_open_orders: std::sync::Mutex::new(HashMap::new()),
        }
    }
}

/// Runs `future` as its own task and waits for its result.
///
/// For work that holds the order manager's lock across network calls. Inside
/// a stream polled only when the trading loop takes a turn, such a future is
/// suspended mid-call while STILL HOLDING the lock whenever the loop does
/// something else, and the loop's next need for the lock deadlocks against it
/// (found on the real testnet: the engine froze after a reconcile, the
/// stale-data halt fired, and it never logged again). As a task it keeps
/// running to completion whether or not anyone is polling, so the lock is
/// always released. It also finishes if the waiting side is dropped, rather
/// than being cancelled half way through a reconcile.
async fn run_detached<T, F>(future: F) -> Option<T>
where
    T: Send + 'static,
    F: std::future::Future<Output = T> + Send + 'static,
{
    match tokio::spawn(future).await {
        Ok(value) => Some(value),
        Err(error) => {
            tracing::error!(?error, "order-manager task failed");
            None
        }
    }
}

#[async_trait]
impl OrderExecutor for OrderManagerExecutor {
    async fn submit(
        &self,
        intent: &OrderIntent,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
        _now: DateTime<Utc>,
    ) -> Result<(Order, Vec<Fill>), ApexesError> {
        // `now` is unused: `OrderManager::submit_order` stamps its own
        // `Order::created_at`/`updated_at` via `Utc::now()` internally
        // -- unlike `MatchingEngine`, which needs a caller-supplied
        // simulation clock, live trading always means "right now".
        let mut manager = self.order_manager.lock().await;
        let order_id = manager
            .submit_order(intent.clone(), positions, balances)
            .await
            .map_err(|error| ApexesError::OrderRejected {
                reason: error.to_string(),
            })?;

        // `OrderManager::submit_order` returns only `OrderId`, but the
        // trait needs the full `Order` -- look up the just-inserted
        // order while still holding the same lock guard (avoids a
        // second lock acquisition and any TOCTOU window against a
        // concurrent `cancel_order`/`apply_exchange_update` call).
        let order = manager
            .open_orders()
            .get(&order_id)
            .cloned()
            .ok_or_else(|| ApexesError::OrderRejected {
                reason: format!(
                    "order {order_id:?} not found in open_orders immediately after submission"
                ),
            })?;

        // Real fills never arrive synchronously in live trading -- they
        // surface later via `LiveSupplementalFillSource`'s merged
        // `exchange_updates`/`reconcile()` stream.
        Ok((order, Vec::new()))
    }

    fn on_market_event(&self, _event: &MarketEvent) -> Vec<(OrderId, Vec<Fill>)> {
        // Live trading holds no local resting-order simulation to react
        // to market data with -- all fills come from the exchange via
        // `LiveSupplementalFillSource`.
        Vec::new()
    }

    /// Waits at most [`OPEN_ORDERS_LOCK_WAIT`] for the order manager. While
    /// it is busy (a reconcile or a submission waiting on the exchange) the
    /// last known open orders are returned instead.
    ///
    /// Blocking here would stall market-event processing for as long as the
    /// exchange takes to answer, and the stale-data watchdog would then halt a
    /// session whose feed is fine. The cost of the shortcut is a view that can
    /// lag by one exchange round trip: the strategy might not yet see an order
    /// it just had accepted, which `submit` itself still guards (it takes the
    /// real lock).
    async fn open_orders(&self) -> HashMap<OrderId, Order> {
        match tokio::time::timeout(OPEN_ORDERS_LOCK_WAIT, self.order_manager.lock()).await {
            Ok(manager) => {
                let fresh = manager.open_orders().clone();
                if let Ok(mut cache) = self.last_open_orders.lock() {
                    *cache = fresh.clone();
                }
                fresh
            }
            Err(_) => self
                .last_open_orders
                .lock()
                .map(|cache| cache.clone())
                .unwrap_or_default(),
        }
    }
}

/// Matches today's live-trading behavior (see the margin/de-risking
/// blocks in `runner.rs`'s `run_live_trading`): margin-breach
/// liquidations are NEVER auto-executed regardless of configuration --
/// real liquidation is the exchange's own responsibility, and this
/// system only logs loudly and relies on `MarginMonitor`'s
/// `PreTradeCheck` to block new orders for the affected symbol.
/// Extreme-market de-risking orders follow `auto_derisk_live`: `false`
/// (the default) means monitor-only, matching the margin-breach
/// precedent; `true` means actually submit the partial close through
/// the exchange, same as backtest/paper.
#[derive(Debug, Clone, Copy)]
pub struct LiveLiquidationPolicy {
    auto_derisk_live: bool,
}

impl LiveLiquidationPolicy {
    /// `auto_derisk_live`: copied from `LiveTradingConfig::auto_derisk_live`
    /// at construction time.
    pub fn new(auto_derisk_live: bool) -> Self {
        Self { auto_derisk_live }
    }
}

impl LiquidationPolicy for LiveLiquidationPolicy {
    fn should_auto_execute(&self, reason: LiquidationReason) -> bool {
        match reason {
            // Unconditional -- deliberately does NOT read
            // `self.auto_derisk_live` for this arm. This is a real-money
            // safety property: real liquidation is the exchange's own
            // responsibility, and no configuration is allowed to make
            // this system auto-submit a margin-breach liquidation order.
            LiquidationReason::MarginBreach => false,
            LiquidationReason::ExtremeMarketCondition => self.auto_derisk_live,
        }
    }
}

/// One `reconcile()` as its own task (see [`run_detached`]); the fills it
/// found, or none (with a warning) if it failed.
async fn reconcile_detached(
    order_manager: &Arc<tokio::sync::Mutex<OrderManager>>,
    why: &'static str,
) -> Vec<Fill> {
    let order_manager = Arc::clone(order_manager);
    run_detached(async move { order_manager.lock().await.reconcile().await })
        .await
        .and_then(|result| {
            result
                .map(|changes| changes.into_iter().flat_map(|(_, fills)| fills).collect())
                .map_err(|error| {
                    tracing::warn!(
                        ?error,
                        why,
                        "LiveSupplementalFillSource: reconcile() failed"
                    );
                })
                .ok()
        })
        .unwrap_or_default()
}

/// Adapts live trading's real fill sources -- the exchange's push-based
/// `exchange_updates` stream plus a periodic `OrderManager::reconcile()`
/// poll -- to `SupplementalFillSource`, merging both into one
/// `Stream<Item = Vec<Fill>>` via a `tokio::select!`-driven
/// `futures::stream::unfold` wrapper (`tokio-stream` is not a workspace
/// dependency, so no merge combinator from that crate is used).
///
/// Callers MUST pass an `exchange_updates` stream that is already
/// `.chain(futures::stream::pending())`-guarded (as `run_live_trading`'s
/// existing construction site already does) -- this adapter does not
/// re-apply that guard itself, to avoid double-guarding. If the
/// supplied stream unexpectedly ends anyway, this adapter's merged
/// stream ends too (rather than risk busy-looping), which is a
/// defensive fallback, not the expected steady-state path.
///
/// Only genuinely non-empty fill batches are yielded -- an
/// `exchange_updates` item or `reconcile()` tick that produced no new
/// fills is silently skipped (the merge loop continues to the next
/// event) rather than yielding an empty `Vec`, matching today's
/// `run_live_trading`'s existing `Ok(new_fills) if !new_fills.is_empty()`
/// handling of both sources.
pub struct LiveSupplementalFillSource<S> {
    order_manager: Arc<tokio::sync::Mutex<OrderManager>>,
    exchange_updates: S,
    reconcile_interval: tokio::time::Interval,
    /// Optional "the push stream just (re)connected" signal: triggers an
    /// immediate `reconcile()` instead of waiting for the next tick.
    resync: Option<Arc<tokio::sync::Notify>>,
}

impl<S> LiveSupplementalFillSource<S>
where
    S: Stream<Item = ExchangeUpdate> + Unpin + Send,
{
    pub fn new(
        order_manager: Arc<tokio::sync::Mutex<OrderManager>>,
        exchange_updates: S,
        reconcile_interval: Duration,
    ) -> Self {
        Self {
            order_manager,
            exchange_updates,
            reconcile_interval: tokio::time::interval(reconcile_interval),
            resync: None,
        }
    }

    /// Reconcile immediately whenever `resync` is notified (see
    /// `UserDataStreamHandle::resync_notifier`).
    pub fn with_resync(mut self, resync: Arc<tokio::sync::Notify>) -> Self {
        self.resync = Some(resync);
        self
    }
}

impl<S> SupplementalFillSource for LiveSupplementalFillSource<S>
where
    S: Stream<Item = ExchangeUpdate> + Unpin + Send,
{
    fn stream(&mut self) -> impl Stream<Item = Vec<Fill>> + Unpin + '_ {
        let state = (
            Arc::clone(&self.order_manager),
            &mut self.exchange_updates,
            &mut self.reconcile_interval,
            self.resync.clone(),
        );

        // Boxed so the resulting stream is unconditionally `Unpin`
        // (an `async` block driving `tokio::select!` internally is
        // generally `!Unpin`; `Pin<Box<_>>` is `Unpin` regardless of
        // the boxed value, satisfying this trait method's signature).
        Box::pin(futures::stream::unfold(
            state,
            |(order_manager, exchange_updates, reconcile_interval, resync)| async move {
                loop {
                    tokio::select! {
                        update = exchange_updates.next() => {
                            let Some(update) = update else {
                                // The caller's guard (chaining with
                                // `futures::stream::pending()`) should
                                // make this unreachable in steady-state
                                // operation; ending the merged stream
                                // here is a safe fallback rather than
                                // risking a busy loop.
                                return None;
                            };
                            let fills = run_detached({
                                let order_manager = Arc::clone(&order_manager);
                                async move {
                                    order_manager
                                        .lock()
                                        .await
                                        .apply_exchange_update(
                                            update.client_order_id,
                                            update.exchange_order_id,
                                            update.status,
                                        )
                                        .await
                                }
                            })
                            .await
                            .and_then(|result| {
                                result
                                    .map_err(|error| {
                                        tracing::warn!(
                                            ?error,
                                            "LiveSupplementalFillSource: apply_exchange_update failed"
                                        );
                                    })
                                    .ok()
                            })
                            .unwrap_or_default();
                            if fills.is_empty() {
                                continue;
                            }
                            return Some((fills, (order_manager, exchange_updates, reconcile_interval, resync)));
                        }
                        _ = async {
                            match &resync {
                                Some(notify) => notify.notified().await,
                                None => futures::future::pending::<()>().await,
                            }
                        } => {
                            tracing::info!("LiveSupplementalFillSource: push stream (re)connected; reconciling now");
                            let fills = reconcile_detached(&order_manager, "resync").await;
                            if fills.is_empty() {
                                continue;
                            }
                            return Some((fills, (order_manager, exchange_updates, reconcile_interval, resync)));
                        }
                        _ = reconcile_interval.tick() => {
                            let fills = reconcile_detached(&order_manager, "periodic").await;
                            if fills.is_empty() {
                                continue;
                            }
                            return Some((fills, (order_manager, exchange_updates, reconcile_interval, resync)));
                        }
                    }
                }
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::registry::Registry;
    use apexes_core::{AssetClass, OrderStatus, OrderType, Side, TradeEvent, Venue};
    use apexes_oms::{ExchangeFill, ExecutionGateway, MockExecutionGateway, OrderStore};
    use apexes_strategy::OrderRouting;
    use rust_decimal_macros::dec;

    const POSTGRES_URL: &str = "postgres://apexes:apexes@localhost:5432/apexes";

    /// Mirrors `apexes-oms::manager`'s and
    /// `live_trading_integration.rs`'s existing test-isolation pattern:
    /// each test constructs its OWN `Registry::new()` (never the
    /// process-wide global one) so its `MockExecutionGateway`
    /// registration can't collide with another test's.
    fn registry_with_gateway(gateway: Arc<dyn ExecutionGateway>) -> Registry {
        let registry = Registry::new();
        registry
            .register::<Arc<dyn ExecutionGateway>, ()>((), gateway)
            .unwrap();
        registry
    }

    /// Truncates both tables before a test runs so tests sharing one
    /// live PostgreSQL database don't see leftover state from earlier
    /// tests -- same rationale as `apexes-oms::manager`'s own
    /// `clean_db` helper.
    async fn clean_db() {
        let pool = sqlx::PgPool::connect(POSTGRES_URL).await.unwrap();
        sqlx::query("TRUNCATE fills, orders")
            .execute(&pool)
            .await
            .unwrap();
    }

    async fn fresh_order_manager(gateway: Arc<dyn ExecutionGateway>) -> OrderManager {
        clean_db().await;
        let store = OrderStore::new(POSTGRES_URL).await.unwrap();
        let registry = registry_with_gateway(gateway);
        OrderManager::new(store, &registry).await.unwrap()
    }

    fn sample_intent() -> OrderIntent {
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
        }
    }

    #[tokio::test]
    async fn order_manager_executor_submit_always_returns_empty_fills() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let order_manager = fresh_order_manager(gateway).await;
        let executor = OrderManagerExecutor::new(Arc::new(tokio::sync::Mutex::new(order_manager)));

        let (order, fills) = executor
            .submit(
                &sample_intent(),
                &HashMap::new(),
                &HashMap::new(),
                Utc::now(),
            )
            .await
            .unwrap();

        assert!(
            fills.is_empty(),
            "live submission must never return synchronous fills"
        );
        assert_eq!(order.status, OrderStatus::New);
    }

    #[tokio::test]
    async fn order_manager_executor_open_orders_reflects_submitted_orders() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let order_manager = fresh_order_manager(gateway).await;
        let executor = OrderManagerExecutor::new(Arc::new(tokio::sync::Mutex::new(order_manager)));

        let (order_one, _) = executor
            .submit(
                &sample_intent(),
                &HashMap::new(),
                &HashMap::new(),
                Utc::now(),
            )
            .await
            .unwrap();
        let (order_two, _) = executor
            .submit(
                &sample_intent(),
                &HashMap::new(),
                &HashMap::new(),
                Utc::now(),
            )
            .await
            .unwrap();

        let open_orders = executor.open_orders().await;
        assert!(open_orders.contains_key(&order_one.id));
        assert!(open_orders.contains_key(&order_two.id));
        assert_eq!(open_orders.len(), 2);
    }

    #[tokio::test]
    async fn order_manager_executor_on_market_event_always_returns_empty() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let order_manager = fresh_order_manager(gateway).await;
        let executor = OrderManagerExecutor::new(Arc::new(tokio::sync::Mutex::new(order_manager)));

        let event = MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(100),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        });

        assert!(executor.on_market_event(&event).is_empty());
    }

    #[test]
    fn live_liquidation_policy_margin_breach_always_false_regardless_of_auto_derisk_live() {
        let policy_auto_derisk_true = LiveLiquidationPolicy::new(true);
        let policy_auto_derisk_false = LiveLiquidationPolicy::new(false);

        assert!(
            !policy_auto_derisk_true.should_auto_execute(LiquidationReason::MarginBreach),
            "MarginBreach must never auto-execute, even with auto_derisk_live=true"
        );
        assert!(
            !policy_auto_derisk_false.should_auto_execute(LiquidationReason::MarginBreach),
            "MarginBreach must never auto-execute, even with auto_derisk_live=false"
        );
    }

    #[test]
    fn live_liquidation_policy_extreme_market_condition_follows_auto_derisk_live() {
        let policy_auto_derisk_true = LiveLiquidationPolicy::new(true);
        let policy_auto_derisk_false = LiveLiquidationPolicy::new(false);

        assert!(
            policy_auto_derisk_true.should_auto_execute(LiquidationReason::ExtremeMarketCondition)
        );
        assert!(
            !policy_auto_derisk_false
                .should_auto_execute(LiquidationReason::ExtremeMarketCondition)
        );
    }

    #[tokio::test]
    async fn live_supplemental_fill_source_merges_exchange_updates_and_reconcile() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut order_manager =
            fresh_order_manager(Arc::clone(&gateway) as Arc<dyn ExecutionGateway>).await;

        // Submit one order directly (bypassing the executor) so we have
        // a known `OrderId` to push an `ExchangeUpdate` for.
        let order_id = order_manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
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

        let order_manager = Arc::new(tokio::sync::Mutex::new(order_manager));

        let update = ExchangeUpdate {
            client_order_id: order_id,
            exchange_order_id,
            status: OrderStatus::Filled,
        };
        // A long reconcile interval so this test's assertion is about
        // the push-update path; the merge loop's empty-yield filtering
        // (see `stream`'s doc comment) means the immediate first
        // `reconcile_interval.tick()` -- `tokio::time::interval`'s first
        // tick always resolves right away -- cannot smuggle a spurious
        // empty result into this test's single awaited item, since
        // empty results are skipped internally rather than yielded.
        let exchange_updates =
            futures::stream::iter(vec![update]).chain(futures::stream::pending());

        let mut source = LiveSupplementalFillSource::new(
            Arc::clone(&order_manager),
            exchange_updates,
            Duration::from_secs(3600),
        );

        let fills = tokio::time::timeout(Duration::from_millis(2000), source.stream().next())
            .await
            .expect("merged stream must yield within the timeout")
            .expect("merged stream must not end");

        assert_eq!(
            fills.len(),
            1,
            "OrderId-preservation requires exactly one fill"
        );
        assert_eq!(
            fills[0].order_id, order_id,
            "OrderId must be preserved through the reconcile/exchange-update flattening"
        );
    }

    #[tokio::test]
    async fn resync_notification_triggers_an_immediate_reconcile() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut order_manager =
            fresh_order_manager(Arc::clone(&gateway) as Arc<dyn ExecutionGateway>).await;
        let order_id = order_manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();
        let order_manager = Arc::new(tokio::sync::Mutex::new(order_manager));

        let resync = Arc::new(tokio::sync::Notify::new());
        // No push updates and a one-hour reconcile interval: only the
        // resync signal can cause a reconcile after the first tick.
        let mut source = LiveSupplementalFillSource::new(
            Arc::clone(&order_manager),
            futures::stream::pending::<ExchangeUpdate>(),
            Duration::from_secs(3600),
        )
        .with_resync(Arc::clone(&resync));

        // Consume interval's immediate first tick (reconciles nothing).
        assert!(
            tokio::time::timeout(Duration::from_millis(200), source.stream().next())
                .await
                .is_err()
        );

        // While "disconnected" the order fills on the exchange.
        gateway.set_exchange_state(order_id, OrderStatus::Filled);
        gateway.set_fills_for_order(
            order_id,
            vec![ExchangeFill {
                exchange_trade_id: "T-resync".to_string(),
                price: dec!(100),
                quantity: dec!(1),
                fee: dec!(0),
                fee_currency: None,
                filled_at: Utc::now(),
            }],
        );
        resync.notify_one();

        let fills = tokio::time::timeout(Duration::from_millis(2000), source.stream().next())
            .await
            .expect("resync must trigger a reconcile promptly")
            .expect("stream must not end");
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].order_id, order_id);
    }
}
