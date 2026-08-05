//! End-to-end integration test: spawns the real `apexes-engine` binary
//! as a subprocess with a `Backtest`-mode `LaunchConfig` written to a
//! temp file (mirroring exactly what `apexes-ui`, Task 9, is expected
//! to do), pointed at a real ClickHouse instance seeded with a small
//! synthetic history, and asserts the process runs to completion and
//! exits 0. This is the cheapest mode to test fully end-to-end without
//! external infrastructure beyond ClickHouse itself (`Paper`/`Live`
//! modes additionally need a running `apexes-market-data-gateway`
//! and/or PostgreSQL).
//!
//! Requires: `docker compose up -d clickhouse` and a release/debug
//! build of the `apexes-engine` binary already produced by `cargo
//! build`. Run explicitly with:
//!   docker compose up -d clickhouse
//!   cargo build -p apexes-engine
//!   cargo test -p apexes-engine --test backtest_subprocess -- --ignored

use apexes_core::{AssetClass, Instrument, MarketType, TradeEvent, Venue};
use apexes_engine::StrategyRef;
use apexes_engine::launch_config::{LaunchConfig, SessionMode};
use apexes_storage::StorageWriter;
use chrono::Utc;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::process::Command;

const CLICKHOUSE_URL: &str = "http://localhost:8123";

#[tokio::test]
#[ignore]
async fn apexes_engine_binary_runs_a_backtest_session_to_completion() {
    let symbol = "ENGINESUBPROCESS";
    let writer = StorageWriter::new(CLICKHOUSE_URL)
        .await
        .expect("StorageWriter::new should succeed with ClickHouse running locally");

    let now = Utc::now();
    let events = vec![apexes_core::MarketEvent::Trade(TradeEvent {
        symbol: symbol.to_string(),
        venue: Venue::Binance,
        price: dec!(100),
        quantity: dec!(1),
        side: None,
        trade_id: None,
        timestamp: now,
    })];
    writer
        .write_batch(&events)
        .await
        .expect("write_batch should succeed");

    let date = now.date_naive();
    let instrument = Instrument::new(
        symbol,
        MarketType::Crypto,
        Venue::Binance,
        AssetClass::Spot,
        Some("BASE".to_string()),
        Some("QUOTE".to_string()),
        dec!(0.01),
        dec!(0.0001),
        dec!(1),
    )
    .unwrap();

    let strategy_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../apexes-strategy-loader/tests/fixtures/always_buy.drift");

    let config = LaunchConfig {
        mode: SessionMode::Backtest {
            date_range: date..(date.succ_opt().unwrap()),
            clickhouse_url: CLICKHOUSE_URL.to_string(),
        },
        strategy_ref: StrategyRef::DriftScript {
            path: strategy_path,
        },
        instruments: vec![instrument],
        initial_balances: HashMap::from([("QUOTE".to_string(), dec!(10000))]),
        cpu_cores: None,
        snapshot_min_interval: LaunchConfig::default_snapshot_min_interval(),
        margin_tiers: HashMap::new(),
        market_condition_config: None,
        risk_free_return: dec!(0),
        gateway_url: "ws://localhost:9001/market".to_string(),
    };

    let dir = std::env::temp_dir().join(format!(
        "apexes-engine-subprocess-test-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let config_path = dir.join("launch_config.json");
    std::fs::write(&config_path, serde_json::to_string(&config).unwrap()).unwrap();

    let binary = env!("CARGO_BIN_EXE_apexes-engine");
    let output = Command::new(binary)
        .arg("--launch-config")
        .arg(&config_path)
        .output()
        .expect("failed to spawn apexes-engine binary");

    let _ = std::fs::remove_dir_all(&dir);

    assert!(
        output.status.success(),
        "apexes-engine exited with {:?}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
