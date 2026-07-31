//! Live integration test proving `StorageMarketDataSource` (backed by a
//! real ClickHouse instance via `apexes-storage`) feeds correctly into
//! `run_backtest`. Excluded from default `cargo test` -- run explicitly
//! with:
//!   docker compose up -d
//!   cargo test -p apexes-backtest -- --ignored

use apexes_backtest::{
    BacktestConfig, CostModel, FlatFeeSchedule, OrderIntent, ReplaySpeed, StorageMarketDataSource,
    Strategy, StrategyContext, run_backtest,
};
use apexes_core::{AssetClass, Instrument, MarketEvent, MarketType, Side, TradeEvent, Venue};
use apexes_storage::{StorageReader, StorageWriter};
use chrono::Utc;
use rust_decimal_macros::dec;
use std::collections::HashMap;

const CLICKHOUSE_URL: &str = "http://localhost:8123";

struct NoOpStrategy;

impl Strategy for NoOpStrategy {
    fn on_market_event(
        &mut self,
        _event: &MarketEvent,
        _ctx: &StrategyContext,
    ) -> Vec<OrderIntent> {
        vec![]
    }
}

#[tokio::test]
#[ignore]
async fn backtest_reads_real_data_from_clickhouse_via_storage_market_data_source() {
    let writer = StorageWriter::new(CLICKHOUSE_URL)
        .await
        .expect("StorageWriter::new should succeed with ClickHouse running locally");
    let reader = StorageReader::new(CLICKHOUSE_URL)
        .await
        .expect("StorageReader::new should succeed");

    let now = Utc::now();
    let symbol = "BACKTESTLIVE";
    let events = vec![MarketEvent::Trade(TradeEvent {
        symbol: symbol.to_string(),
        venue: Venue::Binance,
        price: dec!(65000),
        quantity: dec!(0.5),
        side: Some(Side::Buy),
        trade_id: Some("T1".to_string()),
        timestamp: now,
    })];
    writer
        .write_batch(&events)
        .await
        .expect("write_batch should succeed");

    let date = now.date_naive();
    let market_data = StorageMarketDataSource::new(
        reader,
        symbol,
        Venue::Binance,
        date..(date.succ_opt().unwrap()),
    );

    let instrument = Instrument::new(
        symbol,
        MarketType::Crypto,
        Venue::Binance,
        AssetClass::Spot,
        Some("BACKTEST".to_string()),
        Some("LIVE".to_string()),
        dec!(0.01),
        dec!(0.0001),
        dec!(1),
    )
    .unwrap();

    let mut initial_balances = HashMap::new();
    initial_balances.insert("LIVE".to_string(), dec!(10000));

    let config = BacktestConfig {
        instruments: vec![instrument],
        date_range: date..(date.succ_opt().unwrap()),
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
        risk_checker: None,
    };

    let report = run_backtest(config, market_data, NoOpStrategy)
        .await
        .unwrap();

    // NoOpStrategy places no orders, so no trades -- but the run must
    // complete without error, and the equity curve must be non-empty,
    // proving the written Trade event was actually read from ClickHouse
    // and flowed through StorageMarketDataSource -> run_backtest (an
    // empty equity curve would mean zero events were processed).
    assert!(report.trades.is_empty());
    assert!(
        !report.equity_curve.is_empty(),
        "equity curve must be non-empty, proving at least one event flowed through the pipeline"
    );
    // NoOpStrategy never trades, so equity stays flat at the initial
    // 10,000 LIVE balance for every recorded point.
    assert!(
        report
            .equity_curve
            .iter()
            .all(|(_, equity)| *equity == dec!(10000)),
        "with no orders, equity must stay flat at the initial balance"
    );
}
