use crate::clock::{BacktestClock, ReplaySpeed};
use crate::cost_model::CostModel;
use crate::ledger::PortfolioLedger;
use crate::market_data_source::HistoricalMarketDataSource;
use crate::matching_engine::MatchingEngine;
use crate::performance::PerformanceTracker;
use crate::report::BacktestReport;
use apexes_core::{ApexesError, Fill, Instrument, MarketEvent, Order, OrderId, OrderType};
use apexes_risk::CompositeRiskChecker;
use apexes_strategy::{Strategy, StrategyContext};
use chrono::NaiveDate;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::ops::Range;

/// Configuration for a single backtest run (design spec §8).
pub struct BacktestConfig {
    pub instruments: Vec<Instrument>,
    /// Informational / for-caller-use only: `run_backtest` itself never
    /// reads this field -- event selection is entirely the
    /// `HistoricalMarketDataSource`'s responsibility. Kept on the config
    /// so a caller can, for example, construct a
    /// `StorageMarketDataSource` using the same range, and for future
    /// use (e.g. runner-side event filtering).
    pub date_range: Range<NaiveDate>,
    /// Required -- no implicit default (design spec §8).
    pub initial_balances: HashMap<String, Decimal>,
    pub cost_model: CostModel,
    pub speed: ReplaySpeed,
    /// 0.0 (optimistic) .. 1.0 (pessimistic), see design spec §7.3.
    pub queue_position_factor: Decimal,
    /// Risk-free return rate used for Sharpe/Sortino/Calmar calculations
    /// (design spec §8, matching `barter::statistic::metric`'s
    /// `calculate` signatures).
    pub risk_free_return: Decimal,
    /// Optional risk checks run before submitting each `OrderIntent`
    /// (rejected intents are logged via `tracing::warn!` and never
    /// reach the matching engine) and after each `Fill` (violations are
    /// logged, not propagated -- an in-trade violation does not undo a
    /// fill that already happened, per `apexes-risk`'s design spec §一
    /// "no automatic liquidation" decision). `None` means no risk
    /// checking is performed, preserving this crate's pre-existing
    /// behavior exactly.
    pub risk_checker: Option<CompositeRiskChecker>,
}

/// Runs a full backtest: replays `market_data` in chronological order
/// through `strategy`, simulating fills via a `MatchingEngine`, tracking
/// portfolio state via a `PortfolioLedger`, and producing a
/// `BacktestReport` with overall + per-symbol + per-venue performance
/// summaries (design spec §8).
pub async fn run_backtest(
    mut config: BacktestConfig,
    market_data: impl HistoricalMarketDataSource,
    mut strategy: impl Strategy,
) -> Result<BacktestReport, ApexesError> {
    let events = market_data.events().await?;

    let instruments_by_symbol: HashMap<String, Instrument> = config
        .instruments
        .iter()
        .map(|instrument| (instrument.symbol.clone(), instrument.clone()))
        .collect();

    let mut ledger = PortfolioLedger::new(
        instruments_by_symbol.clone(),
        config.initial_balances.clone(),
    );
    let mut matching_engine = MatchingEngine::new(config.cost_model, config.queue_position_factor);
    let mut clock = BacktestClock::new(config.speed);
    let mut overall_tracker = PerformanceTracker::new(config.risk_free_return);
    let mut per_symbol_trackers: HashMap<String, PerformanceTracker> = HashMap::new();
    let mut per_venue_trackers: HashMap<String, PerformanceTracker> = HashMap::new();
    let mut open_orders: HashMap<OrderId, Order> = HashMap::new();
    let mut last_prices: HashMap<String, Decimal> = HashMap::new();
    let mut all_fills: Vec<Fill> = Vec::new();

    // Record the run's TRUE initial equity point (before any event's
    // fills are applied) so `PerformanceSummary.total_pnl` -- computed
    // as last-minus-first of the curve -- includes whatever P&L (e.g.
    // fees) occurred during the very first processed event.
    if let Some(first_event) = events.first() {
        let initial_equity = ledger.equity(&last_prices)?;
        overall_tracker.record_equity_point(event_timestamp(first_event), initial_equity);
    }

    for event in &events {
        let timestamp = event_timestamp(event);
        clock.wait_for_next_event(timestamp).await;

        match event {
            MarketEvent::Depth(update) => {
                matching_engine.on_depth_update(update);
            }
            MarketEvent::Trade(trade) => {
                if let Some(instrument) = instruments_by_symbol.get(&trade.symbol)
                    && let Some(base) = &instrument.base_currency
                {
                    last_prices.insert(base.clone(), trade.price);
                }
                let fill_results = matching_engine.on_trade_event(trade, timestamp);
                for (order_id, fills) in fill_results {
                    apply_fills(
                        &mut ledger,
                        &mut overall_tracker,
                        &mut per_symbol_trackers,
                        &mut per_venue_trackers,
                        &mut all_fills,
                        &fills,
                        config.risk_free_return,
                    )?;
                    if let Some(risk_checker) = &mut config.risk_checker {
                        for fill in &fills {
                            if let Err(violations) =
                                risk_checker.on_fill(fill, ledger.positions(), ledger.balances())
                            {
                                tracing::warn!(
                                    ?violations,
                                    symbol = %fill.symbol,
                                    "in-trade risk monitor flagged a violation"
                                );
                            }
                        }
                    }
                    if let Some(order) = open_orders.get_mut(&order_id)
                        && let Some(last_fill) = fills.last()
                    {
                        order.filled_quantity += fills.iter().map(|f| f.quantity).sum::<Decimal>();
                        order.updated_at = last_fill.filled_at;
                        if order.filled_quantity >= order.quantity {
                            order.status = apexes_core::OrderStatus::Filled;
                        } else {
                            order.status = apexes_core::OrderStatus::PartiallyFilled;
                        }
                    }
                }
            }
            _ => {} // Tick/Bar events currently only advance the clock/context; no direct matching-engine action.
        }

        let ctx = StrategyContext {
            positions: ledger.positions(),
            balances: ledger.balances(),
            open_orders: &open_orders,
            now: timestamp,
        };
        let intents = strategy.on_market_event(event, &ctx);

        for intent in intents {
            if let Some(risk_checker) = &config.risk_checker
                && let Err(violations) = risk_checker.check(
                    &intent,
                    ledger.positions(),
                    ledger.balances(),
                    &open_orders,
                    timestamp,
                )
            {
                tracing::warn!(?violations, symbol = %intent.symbol, "order rejected by risk checker");
                continue;
            }

            let submit_result = match intent.order_type {
                OrderType::Market => matching_engine
                    .submit_market_order(&intent, timestamp)
                    .map(|(order, fills)| (order, Some(fills))),
                OrderType::Limit => matching_engine
                    .submit_limit_order(&intent, timestamp)
                    .map(|order| (order, None)),
                OrderType::Stop | OrderType::StopLimit => matching_engine
                    .submit_stop_order(&intent, timestamp)
                    .map(|order| (order, None)),
            };

            match submit_result {
                Ok((order, immediate_fills)) => {
                    open_orders.insert(order.id, order);
                    if let Some(fills) = immediate_fills {
                        apply_fills(
                            &mut ledger,
                            &mut overall_tracker,
                            &mut per_symbol_trackers,
                            &mut per_venue_trackers,
                            &mut all_fills,
                            &fills,
                            config.risk_free_return,
                        )?;
                        if let Some(risk_checker) = &mut config.risk_checker {
                            for fill in &fills {
                                if let Err(violations) = risk_checker.on_fill(
                                    fill,
                                    ledger.positions(),
                                    ledger.balances(),
                                ) {
                                    tracing::warn!(
                                        ?violations,
                                        symbol = %fill.symbol,
                                        "in-trade risk monitor flagged a violation"
                                    );
                                }
                            }
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(?error, symbol = %intent.symbol, "order submission rejected");
                }
            }
        }

        // Mark-to-market equity point for the overall tracker after
        // processing this event.
        let equity = ledger.equity(&last_prices)?;
        overall_tracker.record_equity_point(timestamp, equity);
    }

    Ok(BacktestReport {
        overall: overall_tracker.summary().into(),
        per_symbol: per_symbol_trackers
            .into_iter()
            .map(|(k, v)| (k, v.summary().into()))
            .collect(),
        per_venue: per_venue_trackers
            .into_iter()
            .map(|(k, v)| (k, v.summary().into()))
            .collect(),
        equity_curve: overall_tracker.equity_curve().to_vec(),
        trades: all_fills,
    })
}

/// Applies a batch of `Fill`s produced by the matching engine: updates
/// the ledger, records each fill into the overall/per-symbol/per-venue
/// `PerformanceTracker`s, and accumulates every fill into `all_fills`
/// (the `BacktestReport.trades` source) in occurrence order.
/// `risk_free_return` is threaded through from `BacktestConfig` so the
/// per-symbol/per-venue trackers' Sharpe/Sortino/Calmar are computed
/// with the same risk-free rate as the overall summary.
fn apply_fills(
    ledger: &mut PortfolioLedger,
    overall_tracker: &mut PerformanceTracker,
    per_symbol_trackers: &mut HashMap<String, PerformanceTracker>,
    per_venue_trackers: &mut HashMap<String, PerformanceTracker>,
    all_fills: &mut Vec<Fill>,
    fills: &[Fill],
    risk_free_return: Decimal,
) -> Result<(), ApexesError> {
    for fill in fills {
        ledger.apply_fill(fill)?;
        overall_tracker.record_fill(fill);
        per_symbol_trackers
            .entry(fill.symbol.clone())
            .or_insert_with(|| PerformanceTracker::new(risk_free_return))
            .record_fill(fill);
        per_venue_trackers
            .entry(format!("{:?}", fill.venue))
            .or_insert_with(|| PerformanceTracker::new(risk_free_return))
            .record_fill(fill);
    }
    all_fills.extend_from_slice(fills);
    Ok(())
}

fn event_timestamp(event: &MarketEvent) -> chrono::DateTime<chrono::Utc> {
    match event {
        MarketEvent::Tick(tick) => tick.timestamp,
        MarketEvent::Depth(depth) => depth.timestamp,
        MarketEvent::Bar(bar) => bar.open_time,
        MarketEvent::Trade(trade) => trade.timestamp,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::ReplaySpeed;
    use crate::market_data_source::InMemoryMarketDataSource;
    use apexes_core::{AssetClass, DepthUpdate, MarketType, PriceLevel, Side, TradeEvent, Venue};
    use apexes_strategy::OrderIntent;
    use chrono::{Duration, TimeZone, Utc};
    use rust_decimal_macros::dec;

    struct BuyOnceStrategy {
        bought: bool,
    }

    impl Strategy for BuyOnceStrategy {
        fn on_market_event(
            &mut self,
            event: &MarketEvent,
            _ctx: &StrategyContext,
        ) -> Vec<OrderIntent> {
            if !self.bought && matches!(event, MarketEvent::Trade(_)) {
                self.bought = true;
                return vec![OrderIntent {
                    symbol: "BTCUSDT".to_string(),
                    venue: Venue::Binance,
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    quantity: dec!(1),
                    price: None,
                    stop_price: None,
                }];
            }
            Vec::new()
        }
    }

    fn btcusdt_instrument() -> Instrument {
        Instrument {
            symbol: "BTCUSDT".to_string(),
            market_type: MarketType::Crypto,
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            base_currency: Some("BTC".to_string()),
            quote_currency: Some("USDT".to_string()),
            tick_size: dec!(0.01),
            lot_size: dec!(0.00001),
            multiplier: dec!(1),
        }
    }

    fn zero_cost_model() -> CostModel {
        CostModel {
            fee_schedule: Box::new(crate::cost_model::FlatFeeSchedule {
                maker_bps: Decimal::ZERO,
                taker_bps: Decimal::ZERO,
            }),
            funding_rate_bps: None,
        }
    }

    /// Regression test for the Task 14 fix: `run_backtest` must key
    /// `last_prices` by BASE ASSET (e.g. "BTC"), not by trading-pair symbol
    /// (e.g. "BTCUSDT"), because `PortfolioLedger::equity()` looks up mark
    /// prices by asset symbol. With the old (buggy) keying, a held BTC
    /// balance silently fell back to a price of 1.0, so buying 1 BTC at
    /// 100 USDT from 10,000 USDT would report final equity of 9,901
    /// (9,900 USDT + 1 BTC * 1.0) instead of the correct 10,010
    /// (9,900 USDT + 1 BTC * 110 mark).
    #[tokio::test]
    async fn equity_curve_marks_held_base_asset_at_real_trade_price() {
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let events = vec![
            MarketEvent::Depth(DepthUpdate {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                bids: vec![PriceLevel {
                    price: dec!(99),
                    quantity: dec!(10),
                }],
                asks: vec![PriceLevel {
                    price: dec!(100),
                    quantity: dec!(10),
                }],
                sequence: None,
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
                timestamp: t0 + Duration::seconds(1),
            }),
            MarketEvent::Trade(TradeEvent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                price: dec!(110),
                quantity: dec!(1),
                side: None,
                trade_id: None,
                timestamp: t0 + Duration::seconds(2),
            }),
        ];

        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(10000));

        let config = BacktestConfig {
            instruments: vec![btcusdt_instrument()],
            date_range: chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
                ..chrono::NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(),
            initial_balances,
            cost_model: zero_cost_model(),
            speed: ReplaySpeed::AsFastAsPossible,
            queue_position_factor: dec!(0.5),
            risk_free_return: Decimal::ZERO,
            risk_checker: None,
        };

        let report = run_backtest(
            config,
            InMemoryMarketDataSource::new(events),
            BuyOnceStrategy { bought: false },
        )
        .await
        .unwrap();

        assert_eq!(report.trades.len(), 1);
        let final_equity = report
            .equity_curve
            .last()
            .map(|(_, equity)| *equity)
            .unwrap();
        // Correct mark-to-market: 9,900 USDT + 1 BTC * 110 = 10,010.
        assert_eq!(final_equity, dec!(10010));
        // Guard against the regression's exact wrong value: with
        // symbol-keyed mark prices the held BTC fell back to price 1.0,
        // giving 9,900 + 1 = 9,901.
        assert_ne!(final_equity, dec!(9901));
    }
}
