//! [`run_paper_trading`]: the `apexes-paper` counterpart to
//! `apexes_backtest::run_backtest` -- drives the same shared event-loop
//! core (`apexes_backtest::event_loop::run_event_loop`) against a live
//! `MarketDataProvider::subscribe()` stream instead of pre-loaded
//! historical data.

use std::collections::HashMap;

use apexes_backtest::clock::BacktestClock;
use apexes_backtest::event_loop::{EventLoopConfig, run_event_loop};
use apexes_backtest::{BacktestReport, CostModel, LatencyModel, ReplaySpeed};
use apexes_core::{ApexesError, Instrument, MarketDataProvider};
use apexes_risk::CompositeRiskChecker;
use apexes_strategy::Strategy;
use rust_decimal::Decimal;
use tokio::sync::oneshot;

pub struct PaperTradingConfig {
    pub instruments: Vec<Instrument>,
    pub initial_balances: HashMap<String, Decimal>,
    pub cost_model: CostModel,
    pub queue_position_factor: Decimal,
    pub risk_free_return: Decimal,
    pub risk_checker: Option<CompositeRiskChecker>,
    pub latency_model: LatencyModel,
}

/// Runs paper trading: subscribes to `provider` for `config.instruments`,
/// drives the shared event-loop core against the resulting live stream,
/// applying `config.latency_model`'s delay before each `OrderIntent`
/// that passes its `PreTradeCheck` reaches the matching engine. Exits
/// gracefully (returning the `BacktestReport` accumulated so far, not an
/// error) once `stop_signal` fires or its sender is dropped.
pub async fn run_paper_trading(
    config: PaperTradingConfig,
    provider: &dyn MarketDataProvider,
    strategy: impl Strategy,
    stop_signal: oneshot::Receiver<()>,
) -> Result<BacktestReport, ApexesError> {
    let event_stream = provider.subscribe(&config.instruments).await?;
    let clock = BacktestClock::new(ReplaySpeed::AsFastAsPossible);

    let event_loop_config = EventLoopConfig {
        instruments: config.instruments,
        initial_balances: config.initial_balances,
        cost_model: config.cost_model,
        queue_position_factor: config.queue_position_factor,
        risk_free_return: config.risk_free_return,
        risk_checker: config.risk_checker,
    };

    run_event_loop(
        event_loop_config,
        event_stream,
        clock,
        strategy,
        Some(stop_signal),
        Some(config.latency_model),
    )
    .await
}
