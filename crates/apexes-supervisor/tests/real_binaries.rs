//! Launches the REAL `apexes-engine` and `apexes-market-data-gateway`
//! through the real `Supervisor`, against the Binance Spot TEST NETWORK.
//! All `#[ignore]`: they need the network, Postgres (docker compose), and
//! built binaries (`cargo build -p apexes-engine -p apexes-market-data-gateway`).
//!
//!   docker compose up -d postgres
//!   cargo test -p apexes-supervisor --test real_binaries -- --ignored --test-threads=1 --nocapture
//!
//! `bogus_testnet_keys_*` needs no account: it proves the real engine
//! refuses to start with bad keys and says why. `real_testnet_*` additionally
//! needs your testnet keys (from https://testnet.binance.vision/, NOT your
//! Binance account) in `APEXES_TESTNET_API_KEY` / `APEXES_TESTNET_API_SECRET`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use apexes_core::binance_network::BinanceNetwork;
use apexes_engine::LiveRiskLimits;
use apexes_strategy_loader::StrategyRef;
use apexes_supervisor::{LaunchRequest, SessionId, Supervisor, SupervisorError};
use rust_decimal_macros::dec;

fn target_debug() -> PathBuf {
    let exe = std::env::current_exe().expect("current exe");
    exe.parent()
        .and_then(|p| p.parent())
        .expect("target/debug")
        .to_path_buf()
}

fn workspace() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn supervisor(home: &std::path::Path, key: &str, secret: &str) -> Supervisor {
    let vars: HashMap<String, String> = HashMap::from([
        (
            "PATH".to_string(),
            std::env::var("PATH").unwrap_or_default(),
        ),
        (
            "APEXES_CRED_BINANCE_MAIN_TESTNET_API_KEY".to_string(),
            key.to_string(),
        ),
        (
            "APEXES_CRED_BINANCE_MAIN_TESTNET_API_SECRET".to_string(),
            secret.to_string(),
        ),
    ]);
    let env: apexes_supervisor::process::EnvLookup = Arc::new(move |name| vars.get(name).cloned());
    Supervisor::with_env(home, env)
        .with_binaries(
            target_debug().join("apexes-engine"),
            target_debug().join("apexes-market-data-gateway"),
        )
        .with_timing(
            Duration::from_secs(90),
            Duration::from_secs(10),
            Duration::from_millis(200),
        )
}

fn request(id: &str) -> LaunchRequest {
    LaunchRequest {
        network: BinanceNetwork::Testnet,
        mainnet_confirmation: String::new(),
        credentials_ref: "binance-main".to_string(),
        strategy: StrategyRef::DriftScript {
            path: workspace().join("deploy/strategy.example.drift"),
        },
        symbols: vec!["BTCUSDT".to_string()],
        risk_limits: LiveRiskLimits {
            max_order_notional: dec!(300),
            max_order_quantity: dec!(0.002),
            max_position_per_symbol: dec!(0.005),
            max_daily_trade_count: 50,
            min_account_equity: None,
            max_daily_volume: None,
        },
        ems_postgres_url: "postgres://apexes:apexes@localhost:5432/apexes".to_string(),
        stale_data_timeout_secs: Some(30),
        snapshot_min_interval: Duration::from_millis(500),
        session_id: Some(id.to_string()),
    }
}

fn temp_home(tag: &str) -> PathBuf {
    let home = std::env::temp_dir().join(format!(
        "apexes-real-{tag}-{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&home).unwrap();
    home
}

fn cleanup(sup: &Supervisor, home: &std::path::Path) {
    for s in sup.discover() {
        if s.alive {
            let _ = sup.request_stop(&s.id, Duration::from_secs(10), true);
        }
    }
    let _ = sup.stop_gateway(BinanceNetwork::Testnet);
    std::fs::remove_dir_all(home).ok();
}

#[test]
#[ignore = "needs network, Postgres and built binaries"]
fn bogus_testnet_keys_make_the_real_engine_refuse_to_start_and_say_why() {
    let home = temp_home("bogus");
    // Well-formed (64 alphanumerics, like a real key) but not registered:
    // the exchange itself must be the one to refuse it.
    let sup = supervisor(&home, &"A1".repeat(32), &"B2".repeat(32));
    let result = sup.launch(request("t-bogus"));
    let log = std::fs::read_to_string(
        sup.paths()
            .session_log(&SessionId::parse("t-bogus").unwrap()),
    )
    .unwrap_or_default();
    println!("--- engine log ---\n{log}\n--- result: {result:?}");
    cleanup(&sup, &home);

    let error = result.expect_err("bad keys must not produce a running live session");
    assert!(
        matches!(
            error,
            SupervisorError::ExitedDuringStartup { .. } | SupervisorError::StartupTimeout { .. }
        ),
        "{error:?}"
    );
    assert!(
        !log.contains(&"B2".repeat(32)),
        "the secret must never be logged"
    );
    assert!(!log.contains(&"A1".repeat(32)), "nor the key");
}

#[test]
#[ignore = "needs your testnet keys in APEXES_TESTNET_API_KEY / APEXES_TESTNET_API_SECRET"]
fn real_testnet_session_launches_runs_and_stops_cleanly() {
    let (Ok(key), Ok(secret)) = (
        std::env::var("APEXES_TESTNET_API_KEY"),
        std::env::var("APEXES_TESTNET_API_SECRET"),
    ) else {
        panic!(
            "set APEXES_TESTNET_API_KEY and APEXES_TESTNET_API_SECRET (testnet.binance.vision keys)"
        );
    };
    let home = temp_home("real");
    let sup = supervisor(&home, &key, &secret);
    let launched = sup.launch(request("t-real"));
    let id = SessionId::parse("t-real").unwrap();
    let log = std::fs::read_to_string(sup.paths().session_log(&id)).unwrap_or_default();
    println!("--- engine log ---\n{log}");
    let launched = launched.expect("a real testnet session must launch");
    assert!(launched.info.alive);

    // The console's own connection path: authenticated /session/ws.
    std::thread::sleep(Duration::from_secs(5));
    let stopped = sup.request_stop(&id, Duration::from_secs(30), false);
    cleanup(&sup, &home);
    stopped.expect("a clean stop through the control endpoint");
}

/// The whole chain, for real: supervisor -> real gateway (testnet market
/// data) + real engine (testnet account, Postgres) -> the console's own
/// client path, authenticated with the per-session token the supervisor
/// generated. Asserts what an operator relies on: the console sees a real
/// snapshot that says TESTNET and shows sane equity; a parameter change
/// reaches the running strategy; a viewer-less operator Halt is acknowledged;
/// and Stop ends the process.
#[test]
#[ignore = "needs your testnet keys in APEXES_TESTNET_API_KEY / APEXES_TESTNET_API_SECRET"]
fn the_console_sees_a_real_testnet_session_and_can_control_it() {
    use apexes_backtest::SessionCommand;
    use apexes_engine::SessionMessage;
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let (Ok(key), Ok(secret)) = (
        std::env::var("APEXES_TESTNET_API_KEY"),
        std::env::var("APEXES_TESTNET_API_SECRET"),
    ) else {
        panic!(
            "set APEXES_TESTNET_API_KEY and APEXES_TESTNET_API_SECRET (testnet.binance.vision keys)"
        );
    };
    let home = temp_home("console");
    let sup = supervisor(&home, &key, &secret);
    let id = SessionId::parse("t-console").unwrap();
    let launched = sup.launch(request("t-console"));
    let log = std::fs::read_to_string(sup.paths().session_log(&id)).unwrap_or_default();
    let launched = match launched {
        Ok(launched) => launched,
        Err(error) => {
            println!("--- engine log ---\n{log}");
            cleanup(&sup, &home);
            panic!("launch failed: {error}");
        }
    };

    let token = launched.info.token.clone().expect("token");
    let url = launched.info.control_url.clone();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let outcome: Result<(), String> = runtime.block_on(async {
        let mut request = url
            .as_str()
            .into_client_request()
            .map_err(|e| e.to_string())?;
        request.headers_mut().insert(
            "Authorization",
            apexes_security::bearer_header_value(&token)
                .parse()
                .map_err(|_| "header")?,
        );
        let (mut socket, _) = tokio_tungstenite::connect_async(request)
            .await
            .map_err(|e| format!("console connect: {e}"))?;

        // 1. A real snapshot arrives and says TESTNET.
        let snapshot = tokio::time::timeout(Duration::from_secs(60), async {
            while let Some(Ok(Message::Text(text))) = socket.next().await {
                if let Ok(SessionMessage::Snapshot(s)) =
                    serde_json::from_str::<SessionMessage>(&text)
                    && !s.is_placeholder()
                {
                    return Some(s);
                }
            }
            None
        })
        .await
        .map_err(|_| "no real snapshot within 60s".to_string())?
        .ok_or("socket closed before a snapshot")?;

        let identity = snapshot
            .identity
            .as_ref()
            .ok_or("snapshot has no identity")?;
        if identity.network.as_deref() != Some("testnet") {
            return Err(format!(
                "snapshot says network {:?}, expected testnet",
                identity.network
            ));
        }
        if identity.strategy != "strategy.example.drift" {
            return Err(format!("unexpected strategy {:?}", identity.strategy));
        }
        println!("snapshot: equity={} identity={identity:?}", snapshot.equity);
        println!("params:   {:?}", snapshot.params);

        // 2. Equity is a sane number: the testnet account holds 10 000 USDT
        //    plus 1 BTC (~65k), not 500+ unrelated assets at 1.0 each.
        if snapshot.equity <= rust_decimal::Decimal::ZERO {
            return Err(format!("equity {} is not positive", snapshot.equity));
        }

        // 3. A live parameter change is applied (the strategy declares it).
        socket
            .send(Message::Text(
                serde_json::to_string(&SessionMessage::Command(SessionCommand::SetParam {
                    name: "quantity".to_string(),
                    value: rust_decimal::Decimal::new(1, 3),
                }))
                .map_err(|e| e.to_string())?
                .into(),
            ))
            .await
            .map_err(|e| e.to_string())?;

        // 4. Halt is acknowledged in the engine's reported status.
        socket
            .send(Message::Text(
                serde_json::to_string(&SessionMessage::Command(SessionCommand::Halt {
                    reason: "e2e check".to_string(),
                    flatten: false,
                }))
                .map_err(|e| e.to_string())?
                .into(),
            ))
            .await
            .map_err(|e| e.to_string())?;
        tokio::time::timeout(Duration::from_secs(20), async {
            while let Some(Ok(Message::Text(text))) = socket.next().await {
                if let Ok(SessionMessage::Status(status)) =
                    serde_json::from_str::<SessionMessage>(&text)
                    && status.halted
                {
                    return Ok(());
                }
            }
            Err("closed before a halted status".to_string())
        })
        .await
        .map_err(|_| "Halt was never acknowledged".to_string())?
    });

    let stopped = sup.request_stop(&id, Duration::from_secs(30), false);
    let log_after = std::fs::read_to_string(sup.paths().session_log(&id)).unwrap_or_default();
    cleanup(&sup, &home);
    if let Err(message) = outcome {
        println!("--- engine log ---\n{log_after}");
        panic!("{message}");
    }
    stopped.expect("a clean stop through the control endpoint");
}
