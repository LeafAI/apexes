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
    completed: Mutex<Vec<Order>>,
}

impl MatchingEngineOrderExecutor {
    pub fn new(engine: Arc<Mutex<MatchingEngine>>) -> Self {
        Self {
            engine,
            open_orders: Mutex::new(HashMap::new()),
            completed: Mutex::new(Vec::new()),
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
                .submit_executable_limit_order(intent, now),
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

    async fn submit_simulated_checked(
        &self,
        intent: &OrderIntent,
        _positions: &HashMap<String, Position>,
        _balances: &HashMap<String, Decimal>,
        now: DateTime<Utc>,
        check: &mut apexes_strategy::order_executor::SimulatedDirectCheck<'_>,
    ) -> Result<(Order, Vec<Fill>), ApexesError> {
        let (order, fills) = self
            .lock_engine()
            .submit_direct_checked(intent, now, check)?;
        if matches!(
            order.status,
            OrderStatus::New | OrderStatus::PartiallyFilled
        ) {
            self.lock_open_orders().insert(order.id, order.clone());
        }
        Ok((order, fills))
    }

    fn simulated_margin_rates(
        &self,
        intent: &OrderIntent,
        now: DateTime<Utc>,
    ) -> Result<(Decimal, Decimal), ApexesError> {
        self.lock_engine().simulated_margin_rates(intent, now)
    }

    fn simulated_buy_budget(
        &self,
        intent: &OrderIntent,
        now: DateTime<Utc>,
    ) -> Result<Decimal, ApexesError> {
        self.lock_engine().simulated_buy_budget(intent, now)
    }

    fn on_market_event(&self, event: &MarketEvent) -> Vec<(OrderId, Vec<Fill>)> {
        self.process_event(event, None)
    }

    fn book_observation(
        &self,
        event: &MarketEvent,
        now: DateTime<Utc>,
    ) -> Option<apexes_strategy::BookObservation> {
        self.lock_engine().book_observation(event, now)
    }

    fn cost_observation(
        &self,
        instrument: &apexes_core::Instrument,
    ) -> Option<apexes_strategy::CostObservation> {
        self.lock_engine().cost_observation(instrument)
    }

    fn cancel_simulated_working(&self) -> Result<Vec<OrderId>, ApexesError> {
        let mut engine = self.lock_engine();
        let ids = engine.cancel_all_working();
        let mut open = self.lock_open_orders();
        open.clear();
        Ok(ids)
    }

    fn on_market_event_checked(
        &self,
        event: &MarketEvent,
        check: &mut apexes_strategy::order_executor::SimulatedRestingCheck<'_>,
    ) -> Vec<(OrderId, Vec<Fill>)> {
        self.process_event(event, Some(check))
    }

    fn take_simulated_completions(&self) -> Vec<Order> {
        std::mem::take(&mut *self.completed.lock().unwrap_or_else(|p| p.into_inner()))
    }

    async fn open_orders(&self) -> HashMap<OrderId, Order> {
        let engine = self.lock_engine();
        let mut open = self.lock_open_orders();
        open.retain(|id, _| engine.is_working(*id));
        open.clone()
    }
}

impl MatchingEngineOrderExecutor {
    fn process_event(
        &self,
        event: &MarketEvent,
        check: Option<&mut apexes_strategy::order_executor::SimulatedRestingCheck<'_>>,
    ) -> Vec<(OrderId, Vec<Fill>)> {
        let mut completed = self.completed.lock().unwrap_or_else(|p| p.into_inner());
        completed.clear();
        match event {
            MarketEvent::Trade(trade) => {
                let mut engine = self.lock_engine();
                let fill_results = engine.on_trade_event_checked(trade, trade.timestamp, check);
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
                            if order.filled_quantity == order.quantity {
                                completed.push(order.clone());
                            }
                        } else {
                            order.status = OrderStatus::PartiallyFilled;
                        }
                    }
                }
                open_orders.retain(|id, _| engine.is_working(*id));
                fill_results
            }
            MarketEvent::Depth(update) => {
                let mut engine = self.lock_engine();
                engine.on_depth_update(update);
                self.lock_open_orders()
                    .retain(|id, _| engine.is_working(*id));
                Vec::new()
            }
            MarketEvent::Tick(tick) => {
                let mut engine = self.lock_engine();
                engine.advance_time(tick.timestamp);
                self.lock_open_orders()
                    .retain(|id, _| engine.is_working(*id));
                Vec::new()
            }
            MarketEvent::MarkPrice(mark) => {
                let mut engine = self.lock_engine();
                engine.advance_time(mark.timestamp);
                self.lock_open_orders()
                    .retain(|id, _| engine.is_working(*id));
                Vec::new()
            }
            MarketEvent::Bar(bar) => {
                let mut engine = self.lock_engine();
                engine.advance_depth_time(bar.close_time);
                self.lock_open_orders()
                    .retain(|id, _| engine.is_working(*id));
                Vec::new()
            }
        }
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
            source_range: None,
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
    async fn strict_depth_expiry_and_gap_cancel_working_orders_until_snapshot() {
        let policy = crate::SimulatedDepthPolicy {
            max_age_ms: 1000,
            sequence: crate::DepthSequence::Contiguous,
        };
        let engine = MatchingEngine::new(
            CostModel {
                fee_schedule: Box::new(FlatFeeSchedule {
                    maker_bps: dec!(0),
                    taker_bps: dec!(0),
                }),
                funding_rate_bps: None,
            },
            dec!(0),
        )
        .with_depth_policy(Some(policy));
        let executor = MatchingEngineOrderExecutor::new(Arc::new(Mutex::new(engine)));
        let at = Utc::now();
        let mut depth = DepthUpdate {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            bids: vec![PriceLevel {
                price: dec!(99),
                quantity: dec!(10),
            }],
            asks: vec![PriceLevel {
                price: dec!(101),
                quantity: dec!(10),
            }],
            sequence: Some(1),
            source_range: None,
            is_snapshot: true,
            timestamp: at,
        };
        let market = market_intent(OrderType::Market, None);
        let limit = market_intent(OrderType::Limit, Some(dec!(99)));
        assert!(
            executor
                .submit(&market, &HashMap::new(), &HashMap::new(), at)
                .await
                .is_err()
        );
        executor.on_market_event(&MarketEvent::Depth(depth.clone()));
        let (order, _) = executor
            .submit(&limit, &HashMap::new(), &HashMap::new(), at)
            .await
            .unwrap();
        assert!(executor.open_orders().await.contains_key(&order.id));
        depth.is_snapshot = false;
        depth.sequence = Some(3);
        depth.timestamp = at + chrono::Duration::milliseconds(1);
        executor.on_market_event(&MarketEvent::Depth(depth.clone()));
        assert!(executor.open_orders().await.is_empty());
        assert!(
            executor
                .submit(&market, &HashMap::new(), &HashMap::new(), depth.timestamp)
                .await
                .is_err()
        );
        depth.is_snapshot = true;
        depth.sequence = Some(10);
        executor.on_market_event(&MarketEvent::Depth(depth.clone()));
        let (order, _) = executor
            .submit(&limit, &HashMap::new(), &HashMap::new(), depth.timestamp)
            .await
            .unwrap();
        assert!(executor.open_orders().await.contains_key(&order.id));
        let later = at + chrono::Duration::milliseconds(1002);
        let trade = MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            price: dec!(99),
            quantity: dec!(2),
            side: Some(Side::Sell),
            trade_id: None,
            timestamp: later,
        });
        assert!(executor.on_market_event(&trade).is_empty());
        assert!(executor.open_orders().await.is_empty());
        depth.is_snapshot = false;
        depth.sequence = Some(11);
        depth.timestamp = later;
        executor.on_market_event(&MarketEvent::Depth(depth.clone()));
        assert!(
            executor
                .submit(&market, &HashMap::new(), &HashMap::new(), later)
                .await
                .is_err()
        );
        depth.is_snapshot = true;
        executor.on_market_event(&MarketEvent::Depth(depth));
        assert_eq!(
            executor
                .submit(&market, &HashMap::new(), &HashMap::new(), later)
                .await
                .unwrap()
                .0
                .status,
            OrderStatus::Filled
        );
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
    async fn partial_market_order_never_enters_working_view() {
        let executor = MatchingEngineOrderExecutor::new(engine_with_book());
        let mut intent = market_intent(OrderType::Market, None);
        intent.quantity = dec!(15);
        let (order, fills) = executor
            .submit(&intent, &HashMap::new(), &HashMap::new(), Utc::now())
            .await
            .unwrap();
        assert_eq!(order.status, OrderStatus::Cancelled);
        assert_eq!(order.filled_quantity, dec!(10));
        assert_eq!(fills.iter().map(|f| f.quantity).sum::<Decimal>(), dec!(10));
        assert!(executor.open_orders().await.is_empty());
        let (empty, fills) = executor
            .submit(&intent, &HashMap::new(), &HashMap::new(), Utc::now())
            .await
            .unwrap();
        assert_eq!(empty.status, OrderStatus::Cancelled);
        assert!(fills.is_empty());
        assert!(executor.open_orders().await.is_empty());
    }

    #[tokio::test]
    async fn triggered_stop_market_expires_and_preserves_original_fill_id() {
        let executor = MatchingEngineOrderExecutor::new(engine_with_book());
        let mut intent = market_intent(OrderType::Stop, None);
        intent.quantity = dec!(15);
        intent.stop_price = Some(dec!(65000));
        let (order, _) = executor
            .submit(&intent, &HashMap::new(), &HashMap::new(), Utc::now())
            .await
            .unwrap();
        let event = MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            price: dec!(65010),
            quantity: dec!(1),
            side: Some(Side::Buy),
            trade_id: None,
            timestamp: Utc::now(),
        });
        let result = executor.on_market_event(&event);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, order.id);
        assert_eq!(result[0].1[0].order_id, order.id);
        assert_eq!(result[0].1[0].quantity, dec!(10));
        assert!(executor.open_orders().await.is_empty());
        let (empty, _) = executor
            .submit(&intent, &HashMap::new(), &HashMap::new(), Utc::now())
            .await
            .unwrap();
        assert!(executor.open_orders().await.contains_key(&empty.id));
        let event = MarketEvent::Trade(TradeEvent {
            timestamp: Utc::now(),
            ..match event {
                MarketEvent::Trade(t) => t,
                _ => unreachable!(),
            }
        });
        assert!(executor.on_market_event(&event).is_empty());
        assert!(executor.open_orders().await.is_empty());
    }

    #[tokio::test]
    async fn stop_limit_keeps_original_identity_across_conversion() {
        let executor = MatchingEngineOrderExecutor::new(engine_with_book());
        let mut intent = market_intent(OrderType::StopLimit, Some(dec!(65000)));
        intent.stop_price = Some(dec!(65000));
        let (order, _) = executor
            .submit(&intent, &HashMap::new(), &HashMap::new(), Utc::now())
            .await
            .unwrap();
        let trade = TradeEvent {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            price: dec!(65000),
            quantity: dec!(5),
            side: Some(Side::Sell),
            trade_id: None,
            timestamp: Utc::now(),
        };
        assert!(
            executor
                .on_market_event(&MarketEvent::Trade(trade.clone()))
                .is_empty()
        );
        assert!(executor.open_orders().await.contains_key(&order.id));
        let fills = executor.on_market_event(&MarketEvent::Trade(TradeEvent {
            timestamp: trade.timestamp + chrono::Duration::milliseconds(1),
            ..trade
        }));
        assert_eq!(fills[0].0, order.id);
        assert_eq!(fills[0].1[0].order_id, order.id);
        assert!(executor.open_orders().await.is_empty());
    }

    #[tokio::test]
    async fn marketable_stop_limit_takes_depth_and_rests_only_remainder() {
        let executor = MatchingEngineOrderExecutor::new(engine_with_book());
        let mut intent = market_intent(OrderType::StopLimit, Some(dec!(65010)));
        intent.stop_price = Some(dec!(65000));
        intent.quantity = dec!(12);
        let (order, fills) = executor
            .submit(&intent, &HashMap::new(), &HashMap::new(), Utc::now())
            .await
            .unwrap();
        assert!(fills.is_empty());
        let trade = TradeEvent {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            price: dec!(65010),
            quantity: dec!(5),
            side: Some(Side::Sell),
            trade_id: None,
            timestamp: Utc::now(),
        };
        let fills = executor.on_market_event(&MarketEvent::Trade(trade.clone()));
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].0, order.id);
        assert_eq!(fills[0].1[0].order_id, order.id);
        assert_eq!(fills[0].1[0].price, dec!(65010));
        assert_eq!(fills[0].1[0].quantity, dec!(10));
        assert_eq!(
            executor.open_orders().await[&order.id].filled_quantity,
            dec!(10)
        );

        let (other, fills) = executor
            .submit(
                &market_intent(OrderType::Limit, Some(dec!(65010))),
                &HashMap::new(),
                &HashMap::new(),
                trade.timestamp,
            )
            .await
            .unwrap();
        assert!(fills.is_empty(), "triggered order consumed shared depth");
        let fills = executor.on_market_event(&MarketEvent::Trade(TradeEvent {
            timestamp: trade.timestamp + chrono::Duration::milliseconds(1),
            quantity: dec!(2),
            ..trade
        }));
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].0, order.id);
        assert_eq!(fills[0].1[0].quantity, dec!(2));
        assert!(!executor.open_orders().await.contains_key(&order.id));
        assert!(executor.open_orders().await.contains_key(&other.id));
    }

    #[tokio::test]
    async fn marketable_stop_limit_full_fill_is_terminal() {
        let executor = MatchingEngineOrderExecutor::new(engine_with_book());
        let mut intent = market_intent(OrderType::StopLimit, Some(dec!(64990)));
        intent.side = Side::Sell;
        intent.stop_price = Some(dec!(65000));
        let (order, _) = executor
            .submit(&intent, &HashMap::new(), &HashMap::new(), Utc::now())
            .await
            .unwrap();
        let fills = executor.on_market_event(&MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            price: dec!(64995),
            quantity: dec!(1),
            side: Some(Side::Buy),
            trade_id: None,
            timestamp: Utc::now(),
        }));
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].0, order.id);
        assert_eq!(fills[0].1[0].order_id, order.id);
        assert_eq!(fills[0].1[0].price, dec!(64990));
        assert!(!executor.open_orders().await.contains_key(&order.id));
    }

    #[tokio::test]
    async fn stop_limit_rejects_invalid_prices() {
        let executor = MatchingEngineOrderExecutor::new(engine_with_book());
        for (limit, stop) in [(dec!(0), dec!(65000)), (dec!(65000), dec!(-1))] {
            let mut intent = market_intent(OrderType::StopLimit, Some(limit));
            intent.stop_price = Some(stop);
            assert!(
                executor
                    .submit(&intent, &HashMap::new(), &HashMap::new(), Utc::now())
                    .await
                    .is_err()
            );
        }
        assert!(executor.open_orders().await.is_empty());
    }

    #[tokio::test]
    async fn crossing_limit_consumes_bounded_depth_and_rests_only_remainder() {
        let at = Utc::now();
        let mut engine = MatchingEngine::new(
            CostModel {
                fee_schedule: Box::new(FlatFeeSchedule {
                    maker_bps: dec!(5),
                    taker_bps: dec!(20),
                }),
                funding_rate_bps: None,
            },
            dec!(0),
        );
        engine.on_depth_update(&DepthUpdate {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            bids: vec![PriceLevel {
                price: dec!(99),
                quantity: dec!(10),
            }],
            asks: vec![
                PriceLevel {
                    price: dec!(101),
                    quantity: dec!(2),
                },
                PriceLevel {
                    price: dec!(102),
                    quantity: dec!(3),
                },
                PriceLevel {
                    price: dec!(103),
                    quantity: dec!(10),
                },
            ],
            sequence: None,
            source_range: None,
            is_snapshot: true,
            timestamp: at,
        });
        let executor = MatchingEngineOrderExecutor::new(Arc::new(Mutex::new(engine)));
        let mut intent = market_intent(OrderType::Limit, Some(dec!(102)));
        intent.quantity = dec!(6);
        let (order, fills) = executor
            .submit(&intent, &HashMap::new(), &HashMap::new(), at)
            .await
            .unwrap();
        assert_eq!(order.status, OrderStatus::PartiallyFilled);
        assert_eq!(order.filled_quantity, dec!(5));
        assert_eq!(
            fills
                .iter()
                .map(|f| (f.price, f.quantity, f.fee))
                .collect::<Vec<_>>(),
            vec![
                (dec!(101), dec!(2), dec!(0.404)),
                (dec!(102), dec!(3), dec!(0.612))
            ]
        );
        assert!(
            fills
                .iter()
                .all(|f| f.order_id == order.id && f.filled_at == at)
        );
        assert_eq!(
            executor.open_orders().await[&order.id].filled_quantity,
            dec!(5)
        );

        let (second, fills) = executor
            .submit(&intent, &HashMap::new(), &HashMap::new(), at)
            .await
            .unwrap();
        assert_eq!(second.status, OrderStatus::New);
        assert!(fills.is_empty(), "consumed depth must not be reused");
        let later = at + chrono::Duration::milliseconds(1);
        let fills = executor.on_market_event(&MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            price: dec!(102),
            quantity: dec!(1),
            side: Some(Side::Sell),
            trade_id: None,
            timestamp: later,
        }));
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].0, order.id);
        assert_eq!(fills[0].1[0].quantity, dec!(1));
        assert_eq!(fills[0].1[0].fee, dec!(0.051));
        assert!(!executor.open_orders().await.contains_key(&order.id));
        assert!(executor.open_orders().await.contains_key(&second.id));
    }

    #[tokio::test]
    async fn crossing_sell_limit_fills_at_bid_and_is_terminal() {
        let executor = MatchingEngineOrderExecutor::new(engine_with_book());
        let mut intent = market_intent(OrderType::Limit, Some(dec!(64980)));
        intent.side = Side::Sell;
        let (order, fills) = executor
            .submit(&intent, &HashMap::new(), &HashMap::new(), Utc::now())
            .await
            .unwrap();
        assert_eq!(order.status, OrderStatus::Filled);
        assert_eq!(order.filled_quantity, dec!(1));
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].order_id, order.id);
        assert_eq!(fills[0].price, dec!(64990));
        assert!(executor.open_orders().await.is_empty());
    }

    #[tokio::test]
    async fn crossing_limit_does_not_execute_against_future_depth() {
        let executor = MatchingEngineOrderExecutor::new(engine_with_book());
        let (order, fills) = executor
            .submit(
                &market_intent(OrderType::Limit, Some(dec!(65010))),
                &HashMap::new(),
                &HashMap::new(),
                Utc::now() - chrono::Duration::seconds(10),
            )
            .await
            .unwrap();
        assert_eq!(order.status, OrderStatus::New);
        assert!(fills.is_empty());
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
        // A resting Buy limit at 65000 -- crossed by a trade at or below
        // that price.
        let (order, immediate_fills) = executor
            .submit(
                &market_intent(OrderType::Limit, Some(dec!(65000))),
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
            price: dec!(65000),
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
