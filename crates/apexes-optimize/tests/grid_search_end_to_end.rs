//! End-to-end test wiring a real `.drift` script through
//! `DriftScriptStrategy::from_file_with_params`, `run_backtest`, and
//! `GridSearchOptimizer`, proving the full parameter-optimization
//! pipeline works together (not just each piece in isolation).

use apexes_backtest::{
    BacktestConfig, CostModel, FlatFeeSchedule, HistoricalMarketDataSource,
    InMemoryMarketDataSource, ReplaySpeed, run_backtest,
};
use apexes_core::{
    AssetClass, DepthUpdate, Instrument, MarketEvent, MarketType, PriceLevel, TradeEvent, Venue,
};
use apexes_optimize::{Evaluator, GridSearchOptimizer, ParameterOptimizer, SearchSpace};
use apexes_strategy::drift_dsl::DriftScriptStrategy;
use async_trait::async_trait;
use chrono::{NaiveDate, TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::path::PathBuf;

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
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

/// A depth snapshot with abundant ask-side liquidity (so every Market
/// buy order this test's strategy submits fills completely, regardless
/// of how many orders are submitted -- `MatchingEngine::submit_market_order`
/// walks the order book's ask levels and produces zero fills if the book
/// is empty, so a Depth snapshot MUST precede any Trade event a strategy
/// is expected to react to with a Market order; see
/// `crates/apexes-backtest/src/matching_engine.rs`'s `submit_market_order`),
/// followed by six trade prices (ascending then descending) so different
/// `threshold` values produce a predictably different number of trades
/// that exceed them.
fn sample_events() -> Vec<MarketEvent> {
    let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();

    let depth_snapshot = MarketEvent::Depth(DepthUpdate {
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        bids: vec![],
        asks: vec![PriceLevel {
            price: dec!(1),
            quantity: dec!(1000),
        }],
        sequence: Some(1),
        is_snapshot: true,
        timestamp: t0,
    });

    let trades = [
        dec!(90000),
        dec!(95000),
        dec!(100000),
        dec!(105000),
        dec!(110000),
        dec!(100000),
    ]
    .into_iter()
    .enumerate()
    .map(|(i, price)| {
        MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price,
            quantity: dec!(1),
            side: None,
            trade_id: None,
            // +1 so every trade timestamp is strictly after the depth
            // snapshot (chronological ordering matters for the replay).
            timestamp: t0 + chrono::Duration::seconds(i as i64 + 1),
        })
    });

    std::iter::once(depth_snapshot).chain(trades).collect()
}

struct DriftScriptEvaluator {
    script_path: PathBuf,
}

#[async_trait]
impl Evaluator for DriftScriptEvaluator {
    async fn evaluate(
        &mut self,
        params: &HashMap<String, Decimal>,
        market_data: &InMemoryMarketDataSource,
    ) -> Result<Decimal, String> {
        // `DriftScriptStrategy` bridges its construction
        // (`from_file_with_params`) and its `Strategy::on_market_event`
        // implementation (called synchronously by `run_backtest`'s
        // replay loop) to the async `Interpreter` via
        // `tokio::runtime::Handle::current().block_on(..)` internally.
        // That panics with "Cannot start a runtime from within a
        // runtime" if the calling thread is already inside a tokio
        // `block_on`/task-polling frame for that same runtime --
        // driving `run_backtest`'s future via `rt.block_on(..)` (tokio's
        // own block_on) counts as such a frame, since `on_market_event`
        // is called synchronously from inside `run_backtest`'s replay
        // loop, not offloaded to a separate task. The fix used
        // throughout this crate's own `NativeFn` bindings (see
        // `crates/apexes-strategy/src/drift_dsl/{indicators,params,
        // signals,bindings}.rs`, all of which call
        // `futures::executor::block_on(..)` rather than a tokio
        // `Handle::block_on(..)` to bridge sync->async) is to drive the
        // outer future with a non-tokio executor instead:
        // `futures::executor::block_on` has no such reentrancy guard, so
        // the inner `Handle::current().block_on(..)` calls (which still
        // need a `Handle` to resolve, hence `rt.enter()` below) succeed
        // as the *first* tokio block_on frame on this thread's stack.
        // Run on a dedicated plain `std::thread::spawn` OS thread (not
        // one of this test's own tokio runtime's worker/blocking-pool
        // threads) so no ambient tokio context leaks in, matching the
        // same nested-runtime constraint
        // `crates/apexes-strategy/tests/drift_script_strategy.rs`
        // documents and works around with its own `with_runtime` helper.
        let script_path = self.script_path.clone();
        let owned_params = params.clone();
        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(100000));

        let config = BacktestConfig {
            instruments: vec![btc_usdt_instrument()],
            date_range: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
                ..NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(),
            initial_balances,
            cost_model: CostModel {
                fee_schedule: Box::new(FlatFeeSchedule {
                    maker_bps: dec!(0),
                    taker_bps: dec!(0),
                }),
                funding_rate_bps: None,
            },
            speed: ReplaySpeed::AsFastAsPossible,
            queue_position_factor: dec!(0.0),
            risk_free_return: dec!(0),
            margin_tiers: HashMap::new(),
            market_condition_config: None,
        };
        let market_data_events = market_data.events().await.map_err(|e| e.to_string())?;

        let (tx, rx) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            let outcome: Result<Decimal, String> = {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let _guard = rt.enter();
                let market_data = InMemoryMarketDataSource::new(market_data_events);
                futures::executor::block_on(async {
                    let strategy =
                        DriftScriptStrategy::from_file_with_params(&script_path, owned_params)
                            .map_err(|e| e.to_string())?;
                    let registry = apexes_core::registry::Registry::new();
                    let report = run_backtest(config, &registry, market_data, strategy)
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok(Decimal::from(report.overall.trade_count))
                })
            };
            // Ignore send failure: it only happens if the receiver
            // (this evaluator's `.await` below) was dropped, e.g. due
            // to the calling task being cancelled -- nothing to report
            // to in that case.
            let _ = tx.send(outcome);
        });

        rx.await.map_err(|e| e.to_string())?
    }
}

#[tokio::test]
async fn grid_search_finds_the_threshold_that_maximizes_trade_count() {
    let events = sample_events();
    let optimizer = GridSearchOptimizer::new(events);

    // threshold=80000 -> all 6 prices exceed it -> 6 trades.
    // threshold=97000 -> only 100000/105000/110000/100000 exceed it -> 4 trades.
    // threshold=120000 -> no price exceeds it -> 0 trades.
    let search_space = SearchSpace::new()
        .with_parameter("threshold", vec![dec!(80000), dec!(97000), dec!(120000)]);

    let mut evaluator = DriftScriptEvaluator {
        script_path: fixture_path("threshold_strategy.drift"),
    };

    let result = optimizer.optimize(&search_space, &mut evaluator).await;

    assert_eq!(result.error, None);
    let best = result.best.expect("expected a best combination");
    assert_eq!(best.params["threshold"], dec!(80000));
    assert_eq!(best.score, Some(dec!(6)));
    assert_eq!(result.all_results.len(), 3);
}
