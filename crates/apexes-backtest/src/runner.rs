use crate::clock::{BacktestClock, ReplaySpeed};
use crate::cost_model::CostModel;
use crate::ledger::PortfolioLedger;
use crate::market_data_source::HistoricalMarketDataSource;
use crate::matching_engine::MatchingEngine;
use crate::performance::PerformanceTracker;
use crate::report::BacktestReport;
use crate::strategy::{Strategy, StrategyContext};
use apexes_core::{ApexesError, Fill, Instrument, MarketEvent, Order, OrderId, OrderType};
use chrono::NaiveDate;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::ops::Range;

/// Configuration for a single backtest run (design spec §8).
pub struct BacktestConfig {
    pub instruments: Vec<Instrument>,
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
}

/// Runs a full backtest: replays `market_data` in chronological order
/// through `strategy`, simulating fills via a `MatchingEngine`, tracking
/// portfolio state via a `PortfolioLedger`, and producing a
/// `BacktestReport` with overall + per-symbol + per-venue performance
/// summaries (design spec §8).
pub async fn run_backtest(
    config: BacktestConfig,
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

    for event in &events {
        let timestamp = event_timestamp(event);
        clock.wait_for_next_event(timestamp).await;

        match event {
            MarketEvent::Depth(update) => {
                matching_engine.on_depth_update(update);
            }
            MarketEvent::Trade(trade) => {
                last_prices.insert(trade.symbol.clone(), trade.price);
                let fill_results = matching_engine.on_trade_event(trade, timestamp);
                for (order_id, fills) in fill_results {
                    apply_fills(
                        &mut ledger,
                        &mut overall_tracker,
                        &mut per_symbol_trackers,
                        &mut per_venue_trackers,
                        &mut all_fills,
                        &fills,
                    )?;
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
                        )?;
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
fn apply_fills(
    ledger: &mut PortfolioLedger,
    overall_tracker: &mut PerformanceTracker,
    per_symbol_trackers: &mut HashMap<String, PerformanceTracker>,
    per_venue_trackers: &mut HashMap<String, PerformanceTracker>,
    all_fills: &mut Vec<Fill>,
    fills: &[Fill],
) -> Result<(), ApexesError> {
    for fill in fills {
        ledger.apply_fill(fill)?;
        overall_tracker.record_fill(fill);
        per_symbol_trackers
            .entry(fill.symbol.clone())
            .or_insert_with(|| PerformanceTracker::new(Decimal::ZERO))
            .record_fill(fill);
        per_venue_trackers
            .entry(format!("{:?}", fill.venue))
            .or_insert_with(|| PerformanceTracker::new(Decimal::ZERO))
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
