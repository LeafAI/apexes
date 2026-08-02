//! End-to-end test wiring `VersionStore` + `CanaryComparator` through a
//! real `DriftScriptStrategy` + `run_backtest`, proving the full
//! publish -> compare -> promote -> rollback pipeline works together.
//! Requires a live ClickHouse instance -- all tests here are `#[ignore]`,
//! matching `apexes-storage`'s established pattern (see
//! `crates/apexes-storage/tests/live_clickhouse.rs`).

use apexes_backtest::{
    BacktestConfig, CostModel, FlatFeeSchedule, HistoricalMarketDataSource,
    InMemoryMarketDataSource, ReplaySpeed, run_backtest,
};
use apexes_core::{
    AssetClass, DepthUpdate, Instrument, MarketEvent, MarketType, PriceLevel, TradeEvent, Venue,
};
use apexes_optimize::Evaluator;
use apexes_strategy::drift_dsl::DriftScriptStrategy;
use apexes_strategy_registry::{CanaryComparator, VersionStore};
use async_trait::async_trait;
use chrono::{NaiveDate, TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::path::PathBuf;

const CLICKHOUSE_URL: &str = "http://localhost:8123";

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

fn unique_strategy_name(prefix: &str) -> String {
    format!("{prefix}{}", Utc::now().timestamp_nanos_opt().unwrap_or(0))
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

/// Same fixture-data shape as `apexes-optimize`'s end-to-end test: a
/// liquidity-abundant depth snapshot followed by six ascending-then-
/// descending trade prices, so different `threshold` values produce a
/// predictably different number of trades.
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
            timestamp: t0 + chrono::Duration::seconds(i as i64 + 1),
        })
    });

    std::iter::once(depth_snapshot).chain(trades).collect()
}

mod tempfile_holder {
    use std::path::PathBuf;

    /// Minimal RAII temp-file holder: writes `contents` to a uniquely
    /// named file under `std::env::temp_dir()` at construction, deletes
    /// it on drop. Avoids adding a `tempfile` crate dependency for this
    /// one narrow use.
    pub struct TempFile {
        path: PathBuf,
    }

    impl TempFile {
        pub fn write(contents: &str) -> (Self, PathBuf) {
            let path = std::env::temp_dir().join(format!(
                "apexes-strategy-registry-test-{}.drift",
                uuid::Uuid::new_v4()
            ));
            std::fs::write(&path, contents).expect("failed to write temp .drift file");
            (Self { path: path.clone() }, path)
        }
    }

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Runs a `.drift` script (given as source text, written to a temp file
/// since `DriftScriptStrategy::from_file_with_params` takes a path) as a
/// backtest and scores it by `report.overall.trade_count`. Uses the same
/// dedicated-OS-thread + `futures::executor::block_on` bridge as
/// `apexes-optimize`'s end-to-end test
/// (`crates/apexes-optimize/tests/grid_search_end_to_end.rs`) to avoid
/// nested-tokio-runtime panics.
struct SourceBackedEvaluator {
    script_path: PathBuf,
    _temp_dir: tempfile_holder::TempFile,
}

impl SourceBackedEvaluator {
    fn new(script_source: &str) -> Self {
        let (temp_dir, script_path) = tempfile_holder::TempFile::write(script_source);
        Self {
            script_path,
            _temp_dir: temp_dir,
        }
    }
}

#[async_trait]
impl Evaluator for SourceBackedEvaluator {
    async fn evaluate(
        &mut self,
        params: &HashMap<String, Decimal>,
        market_data: &InMemoryMarketDataSource,
    ) -> Result<Decimal, String> {
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
            let _ = tx.send(outcome);
        });

        rx.await.map_err(|e| e.to_string())?
    }
}

#[tokio::test]
#[ignore]
async fn publish_compare_and_promote_selects_the_threshold_with_more_trades() {
    let store = VersionStore::new(CLICKHOUSE_URL).await.unwrap();
    let strategy_name = unique_strategy_name("registry_e2e_");

    let v1_source = std::fs::read_to_string(fixture_path("threshold_strategy_v1.drift")).unwrap();
    let v2_source = std::fs::read_to_string(fixture_path("threshold_strategy_v2.drift")).unwrap();

    // v1: threshold=97000 -> 4 of the 6 sample trade prices exceed it.
    let mut v1_params = HashMap::new();
    v1_params.insert("threshold".to_string(), dec!(97000));
    let v1 = store
        .publish(
            &strategy_name,
            &v1_source,
            Some(v1_params),
            Some("initial version"),
        )
        .await
        .unwrap();
    assert_eq!(v1, 1);
    assert_eq!(
        store.current_version(&strategy_name).await.unwrap(),
        Some(1)
    );

    // v2: threshold=80000 -> all 6 sample trade prices exceed it -- more
    // trades than v1, so v2 should win the comparison.
    let mut v2_params = HashMap::new();
    v2_params.insert("threshold".to_string(), dec!(80000));
    let v2 = store
        .publish(
            &strategy_name,
            &v2_source,
            Some(v2_params),
            Some("lower threshold"),
        )
        .await
        .unwrap();
    assert_eq!(v2, 2);
    // Publishing v2 must NOT move the pointer -- only an explicit
    // `set_current`/`compare_and_promote` call does that.
    assert_eq!(
        store.current_version(&strategy_name).await.unwrap(),
        Some(1)
    );

    let versions = store.list_versions(&strategy_name).await.unwrap();
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0].version, 1);
    assert_eq!(versions[0].description.as_deref(), Some("initial version"));
    assert_eq!(versions[1].version, 2);

    let comparator = CanaryComparator::new(&store);
    let market_data = InMemoryMarketDataSource::new(sample_events());

    let (results, winner) = comparator
        .compare_and_promote(&strategy_name, &[1, 2], &market_data, |source| {
            Box::new(SourceBackedEvaluator::new(source))
        })
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], (1, Ok(dec!(4))));
    assert_eq!(results[1], (2, Ok(dec!(6))));
    assert_eq!(winner, 2);
    assert_eq!(
        store.current_version(&strategy_name).await.unwrap(),
        Some(2)
    );

    // Roll back to v1 and confirm the pointer moves accordingly.
    store.set_current(&strategy_name, 1).await.unwrap();
    assert_eq!(
        store.current_version(&strategy_name).await.unwrap(),
        Some(1)
    );
}
