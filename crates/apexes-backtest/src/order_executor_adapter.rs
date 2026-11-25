//! [`MatchingEngineOrderExecutor`]/[`BacktestLiquidationPolicy`]/
//! [`NeverFillSupplementalFillSource`]: adapt `crate::matching_engine::MatchingEngine`
//! and today's backtest/paper auto-liquidation behavior to the three
//! `apexes_strategy` traits (`OrderExecutor`/`LiquidationPolicy`/
//! `SupplementalFillSource`) that `crate::unified_loop::run_unified_event_loop`
//! drives, abstracting over backtest/paper (this module) vs. live
//! trading (a sibling adapter in `apexes-live-trading`).
//!
//! Both `run_backtest` and `apexes-paper`'s `run_paper_trading`
//! construct this module's three types and pass them into the unified
//! loop -- this is the execution backend for every simulated
//! (backtest/paper) run.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use apexes_core::{ApexesError, Fill, MarketEvent, Order, OrderId, OrderStatus, OrderType};
use apexes_strategy::{
    LiquidationPolicy, LiquidationReason, OrderExecutor, OrderIntent, Position,
    SupplementalFillSource,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;
use rust_decimal::Decimal;

use crate::matching_engine::MatchingEngine;

/// Adapts `MatchingEngine` (backtest/paper's synchronous simulated
/// matching kernel) to `OrderExecutor`. Wraps the SAME
/// `Arc<Mutex<MatchingEngine>>` a unified loop shares with
/// `crate::ems_adapter::MatchingEngineChildOrderSink` -- `run_backtest`
/// and `run_paper_trading` each construct ONE engine instance and give
/// it to both this executor (direct strategy dispatch) and the EMS
/// child-order sink, so EMS fills and direct-strategy fills interact
/// through one single order book, not two independent simulations.
pub struct MatchingEngineOrderExecutor {
    engine: Arc<Mutex<MatchingEngine>>,
    /// Working orders only, matching the live OMS contract. Terminal
    /// orders are reported through submissions/fills and the OrderLog,
    /// not retained in the strategy's open-order view.
    open_orders: Mutex<HashMap<OrderId, Order>>,
}

impl MatchingEngineOrderExecutor {
    pub fn new(engine: Arc<Mutex<MatchingEngine>>) -> Self {
        Self {
            engine,
            open_orders: Mutex::new(HashMap::new()),
        }
    }

    /// Locks the shared `MatchingEngine`, recovering from poisoning by
    /// taking the inner value -- matching `event_loop.rs`'s own
    /// `lock_engine` helper's poison-recovery pattern.
    fn lock_engine(&self) -> std::sync::MutexGuard<'_, MatchingEngine> {
        self.engine.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn lock_open_orders(&self) -> std::sync::MutexGuard<'_, HashMap<OrderId, Order>> {
        self.open_orders.lock().unwrap_or_else(|p| p.into_inner())
    }
}

#[async_trait]
impl OrderExecutor for MatchingEngineOrderExecutor {
    async fn submit(
        &self,
        intent: &OrderIntent,
        _positions: &HashMap<String, Position>,
        _balances: &HashMap<String, Decimal>,
        now: DateTime<Utc>,
    ) -> Result<(Order, Vec<Fill>), ApexesError> {
        // `MatchingEngine` performs no risk checking of its own -- the
        // `positions`/`balances` parameters exist only so this trait
        // has one uniform signature shared with `OrderManager`'s live
        // implementation, which does use them for a second, independent
        // pre-trade check (see `OrderExecutor::submit`'s doc comment).
        let submit_result = match intent.order_type {
            OrderType::Market => self.lock_engine().submit_market_order(intent, now),
            OrderType::Limit => self
                .lock_engine()
                .submit_limit_order(intent, now)
                .map(|order| (order, Vec::new())),
            OrderType::Stop | OrderType::StopLimit => self
                .lock_engine()
                .submit_stop_order(intent, now)
                .map(|order| (order, Vec::new())),
        };

        let (order, fills) = submit_result?;
        if matches!(
            order.status,
            OrderStatus::New | OrderStatus::PartiallyFilled
        ) {
            self.lock_open_orders().insert(order.id, order.clone());
        }
        Ok((order, fills))
    }

    fn on_market_event(&self, event: &MarketEvent) -> Vec<(OrderId, Vec<Fill>)> {
        match event {
            MarketEvent::Trade(trade) => {
                let fill_results = self.lock_engine().on_trade_event(trade, trade.timestamp);
                let mut open_orders = self.lock_open_orders();
                for (order_id, fills) in &fill_results {
                    // Mirror `event_loop.rs`'s `MarketEvent::Trade` arm
                    // verbatim: update `filled_quantity`/`status` from
                    // the fills just produced, do not re-derive this
                    // logic differently.
                    if let Some(order) = open_orders.get_mut(order_id)
                        && let Some(last_fill) = fills.last()
                    {
                        order.filled_quantity += fills.iter().map(|f| f.quantity).sum::<Decimal>();
                        order.updated_at = last_fill.filled_at;
                        if order.filled_quantity >= order.quantity {
                            order.status = OrderStatus::Filled;
                        } else {
                            order.status = OrderStatus::PartiallyFilled;
                        }
                    }
                }
                open_orders.retain(|_, order| {
                    matches!(
                        order.status,
                        OrderStatus::New | OrderStatus::PartiallyFilled
                    )
                });
                fill_results
            }
            MarketEvent::Depth(update) => {
                self.lock_engine().on_depth_update(update);
                Vec::new()
            }
            MarketEvent::Tick(_) | MarketEvent::Bar(_) | MarketEvent::MarkPrice(_) => Vec::new(),
        }
    }

    async fn open_orders(&self) -> HashMap<OrderId, Order> {
        self.lock_open_orders().clone()
    }
}

/// Matches today's backtest/paper behavior: margin-breach liquidations
/// and extreme-market-condition de-risking orders are always
/// auto-executed (never merely logged for an operator), for both
/// `LiquidationReason` variants.
#[derive(Debug, Default, Clone, Copy)]
pub struct BacktestLiquidationPolicy;

impl LiquidationPolicy for BacktestLiquidationPolicy {
    fn should_auto_execute(&self, _reason: LiquidationReason) -> bool {
        true
    }
}

/// Backtest/paper have no fill source independent of the primary
/// market-event stream `OrderExecutor::submit`/`on_market_event`
/// already cover (see `SupplementalFillSource`'s doc comment) -- this
/// is a stream that never yields.
#[derive(Debug, Default, Clone, Copy)]
pub struct NeverFillSupplementalFillSource;

impl SupplementalFillSource for NeverFillSupplementalFillSource {
    fn stream(&mut self) -> impl Stream<Item = Vec<Fill>> + Unpin + '_ {
        futures::stream::pending::<Vec<Fill>>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cost_model::{CostModel, FlatFeeSchedule};
    use apexes_core::{AssetClass, DepthUpdate, PriceLevel, Side, TradeEvent, Venue};
    use apexes_strategy::OrderRouting;
    use rust_decimal_macros::dec;

    fn engine_with_book() -> Arc<Mutex<MatchingEngine>> {
        let cost_model = CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: dec!(0),
                taker_bps: dec!(0),
            }),
            funding_rate_bps: None,
        };
        let mut engine = MatchingEngine::new(cost_model, dec!(0));
        engine.on_depth_update(&DepthUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bids: vec![PriceLevel {
                price: dec!(64990),
                quantity: dec!(10),
            }],
            asks: vec![PriceLevel {
                price: dec!(65010),
                quantity: dec!(10),
            }],
            sequence: None,
            is_snapshot: true,
            timestamp: Utc::now(),
        });
        Arc::new(Mutex::new(engine))
    }

    fn market_intent(order_type: OrderType, price: Option<Decimal>) -> OrderIntent {
        OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type,
            quantity: dec!(1),
            price,
            stop_price: None,
            execution_algo: None,
            routing: OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        }
    }

    #[tokio::test]
    async fn matching_engine_order_executor_submit_market_order_returns_immediate_fills() {
        let executor = MatchingEngineOrderExecutor::new(engine_with_book());
        let (order, fills) = executor
            .submit(
                &market_intent(OrderType::Market, None),
                &HashMap::new(),
                &HashMap::new(),
                Utc::now(),
            )
            .await
            .unwrap();

        assert!(
            !fills.is_empty(),
            "crossing market order must fill immediately"
        );
        assert_eq!(order.status, OrderStatus::Filled);
        assert!(!executor.open_orders().await.contains_key(&order.id));
    }

    #[tokio::test]
    async fn matching_engine_order_executor_submit_limit_order_returns_no_immediate_fills() {
        let executor = MatchingEngineOrderExecutor::new(engine_with_book());
        // A non-crossing Buy limit far below the best ask (65010).
        let (order, fills) = executor
            .submit(
                &market_intent(OrderType::Limit, Some(dec!(60000))),
                &HashMap::new(),
                &HashMap::new(),
                Utc::now(),
            )
            .await
            .unwrap();

        assert!(
            fills.is_empty(),
            "non-crossing limit order must not fill immediately"
        );
        assert_eq!(order.status, OrderStatus::New);
    }

    #[tokio::test]
    async fn matching_engine_order_executor_open_orders_reflects_submitted_orders() {
        let executor = MatchingEngineOrderExecutor::new(engine_with_book());
        let (order_one, _) = executor
            .submit(
                &market_intent(OrderType::Limit, Some(dec!(60000))),
                &HashMap::new(),
                &HashMap::new(),
                Utc::now(),
            )
            .await
            .unwrap();
        let (order_two, _) = executor
            .submit(
                &market_intent(OrderType::Limit, Some(dec!(59000))),
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
    async fn matching_engine_order_executor_on_market_event_fills_resting_order() {
        let executor = MatchingEngineOrderExecutor::new(engine_with_book());
        // A resting Buy limit at 65010 -- crossed by a trade at or below
        // that price.
        let (order, immediate_fills) = executor
            .submit(
                &market_intent(OrderType::Limit, Some(dec!(65010))),
                &HashMap::new(),
                &HashMap::new(),
                Utc::now(),
            )
            .await
            .unwrap();
        assert!(immediate_fills.is_empty());

        let trade_event = MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(65010),
            quantity: dec!(5),
            side: Some(Side::Sell),
            trade_id: None,
            timestamp: Utc::now(),
        });
        let results = executor.on_market_event(&trade_event);

        let (filled_order_id, fills) = results
            .into_iter()
            .find(|(id, _)| *id == order.id)
            .expect("resting order must receive a fill from the crossing trade");
        assert_eq!(filled_order_id, order.id);
        assert!(!fills.is_empty());

        let open_orders = executor.open_orders().await;
        assert!(!open_orders.contains_key(&order.id));
        assert_eq!(
            fills.iter().map(|fill| fill.quantity).sum::<Decimal>(),
            dec!(1)
        );
    }

    #[test]
    fn backtest_liquidation_policy_always_auto_executes() {
        let policy = BacktestLiquidationPolicy;
        assert!(policy.should_auto_execute(LiquidationReason::MarginBreach));
        assert!(policy.should_auto_execute(LiquidationReason::ExtremeMarketCondition));
    }

    #[tokio::test]
    async fn never_fill_supplemental_fill_source_never_yields() {
        use futures::StreamExt;

        let mut source = NeverFillSupplementalFillSource;
        let result = tokio::time::timeout(std::time::Duration::from_millis(50), async {
            source.stream().next().await
        })
        .await;

        assert!(
            result.is_err(),
            "a stream that never yields must time out, not resolve to None/a value"
        );
    }
}
