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

/// Reads `/market` of a real gateway for a few seconds and returns the set of
/// symbols seen in the events and how many arrived.
fn symbols_seen(
    port: u16,
    token: &str,
    seconds: u64,
) -> (std::collections::BTreeSet<String>, usize) {
    use futures::StreamExt;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async move {
        let mut request = format!("ws://127.0.0.1:{port}/market")
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert("Authorization", format!("Bearer {token}").parse().unwrap());
        let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let mut symbols = std::collections::BTreeSet::new();
        let mut count = 0;
        let end = tokio::time::Instant::now() + Duration::from_secs(seconds);
        while let Ok(Some(Ok(message))) = tokio::time::timeout_at(end, socket.next()).await {
            if let Ok(text) = message.into_text()
                && let Ok(value) = serde_json::from_str::<serde_json::Value>(&text)
                && let Some(inner) = value.as_object().and_then(|o| o.values().next())
                && let Some(symbol) = inner.get("symbol").and_then(|s| s.as_str())
            {
                symbols.insert(symbol.to_string());
                count += 1;
            }
        }
        (symbols, count)
    })
}

/// The reported bug, with the real gateway: BTCUSDT is already streaming and
/// ETHUSDT is asked for. Needs only the network (no keys, no Postgres).
#[test]
#[ignore = "needs network and the built gateway binary"]
fn a_second_symbol_gets_its_own_real_gateway_and_each_streams_only_its_own_data() {
    let home = temp_home("twogw");
    let sup = supervisor(&home, "unused", "unused");
    let btc = sup
        .ensure_gateway(BinanceNetwork::Testnet, &["BTCUSDT".to_string()])
        .expect("BTC gateway");
    let eth = sup
        .ensure_gateway(BinanceNetwork::Testnet, &["ETHUSDT".to_string()])
        .expect("an ETH request must start its own gateway, not be refused");
    let again = sup
        .ensure_gateway(BinanceNetwork::Testnet, &["BTCUSDT".to_string()])
        .expect("BTC again");

    let (btc_symbols, btc_events) = symbols_seen(btc.port, btc.token.as_deref().unwrap(), 12);
    let (eth_symbols, eth_events) = symbols_seen(eth.port, eth.token.as_deref().unwrap(), 12);
    println!("BTC gateway: {btc_events} events {btc_symbols:?}");
    println!("ETH gateway: {eth_events} events {eth_symbols:?}");

    let _ = sup.stop_gateway(BinanceNetwork::Testnet);
    std::thread::sleep(Duration::from_millis(500));
    let alive = (
        apexes_supervisor::pidfile::port_open(btc.port),
        apexes_supervisor::pidfile::port_open(eth.port),
    );
    std::fs::remove_dir_all(&home).ok();

    assert!(btc.started && eth.started);
    assert_ne!(btc.port, eth.port);
    assert!(!again.started && again.port == btc.port, "BTC is reused");
    assert!(
        btc_events > 0 && eth_events > 0,
        "both feeds delivered live data"
    );
    assert_eq!(
        btc_symbols.iter().map(String::as_str).collect::<Vec<_>>(),
        ["BTCUSDT"]
    );
    assert_eq!(
        eth_symbols.iter().map(String::as_str).collect::<Vec<_>>(),
        ["ETHUSDT"]
    );
    assert_eq!(alive, (false, false), "stop_gateway stopped both");
}

/// Your exact report, with real processes and your testnet keys: a BTCUSDT
/// session is running, then an ETHUSDT session is launched. Both must run,
/// each on its own gateway, and each must see only its own symbol's market.
#[test]
#[ignore = "needs your testnet keys in APEXES_TESTNET_API_KEY / APEXES_TESTNET_API_SECRET and Postgres"]
fn a_btc_session_and_an_eth_session_run_side_by_side_on_the_real_testnet() {
    use apexes_engine::SessionMessage;
    use futures::StreamExt;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let (Ok(key), Ok(secret)) = (
        std::env::var("APEXES_TESTNET_API_KEY"),
        std::env::var("APEXES_TESTNET_API_SECRET"),
    ) else {
        panic!("set APEXES_TESTNET_API_KEY and APEXES_TESTNET_API_SECRET");
    };
    let home = temp_home("btceth");
    let sup = supervisor(&home, &key, &secret);
    let mut eth_request = request("t-eth");
    eth_request.symbols = vec!["ETHUSDT".to_string()];

    let btc = sup.launch(request("t-btc"));
    let eth = btc.as_ref().ok().map(|_| sup.launch(eth_request));
    let logs = ["t-btc", "t-eth"].map(|id| {
        std::fs::read_to_string(sup.paths().session_log(&SessionId::parse(id).unwrap()))
            .unwrap_or_default()
    });

    let result = (|| -> Result<(), String> {
        let btc = btc.as_ref().map_err(|e| format!("BTC launch: {e}"))?;
        let eth = eth
            .as_ref()
            .ok_or("no ETH launch")?
            .as_ref()
            .map_err(|e| format!("ETH launch failed (the reported bug): {e}"))?;
        if btc.gateway.port == eth.gateway.port {
            return Err("both sessions share one gateway".to_string());
        }
        // Each session's own snapshot: its equity is priced from ITS symbol
        // only, so a BTC price leaking into the ETH session would show here.
        let runtime = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
        for (label, session, expect) in [("BTC", btc, "BTCUSDT"), ("ETH", eth, "ETHUSDT")] {
            let token = session.info.token.clone().ok_or("token")?;
            let url = session.info.control_url.clone();
            let identity: Result<String, String> = runtime.block_on(async {
                let mut req = url
                    .as_str()
                    .into_client_request()
                    .map_err(|e| e.to_string())?;
                req.headers_mut().insert(
                    "Authorization",
                    apexes_security::bearer_header_value(&token)
                        .parse()
                        .map_err(|_| "header")?,
                );
                let (mut socket, _) = tokio_tungstenite::connect_async(req)
                    .await
                    .map_err(|e| e.to_string())?;
                tokio::time::timeout(Duration::from_secs(60), async {
                    while let Some(Ok(Message::Text(text))) = socket.next().await {
                        if let Ok(SessionMessage::Snapshot(s)) =
                            serde_json::from_str::<SessionMessage>(&text)
                            && !s.is_placeholder()
                            && let Some(id) = s.identity
                        {
                            return Some(format!(
                                "{} {} equity {}",
                                id.session_id, id.strategy, s.equity
                            ));
                        }
                    }
                    None
                })
                .await
                .map_err(|_| format!("{label}: no real snapshot within 60s"))?
                .ok_or_else(|| format!("{label}: snapshot stream ended"))
            });
            println!("{label} ({expect}): {}", identity?);
        }
        Ok(())
    })();

    let gateways = sup.paths().gateway_keys(BinanceNetwork::Testnet).len();
    println!("gateway records: {gateways}");
    println!("--- ETH engine log tail ---");
    for line in logs[1]
        .lines()
        .rev()
        .take(4)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
    {
        println!("{line}");
    }
    for id in ["t-btc", "t-eth"] {
        let _ = sup.request_stop(
            &SessionId::parse(id).unwrap(),
            Duration::from_secs(30),
            true,
        );
    }
    let _ = sup.stop_gateway(BinanceNetwork::Testnet);
    std::fs::remove_dir_all(&home).ok();
    result.expect("both sessions run side by side");
}

/// The screenshot's situation with the real engine: the testnet account
/// already holds BTC, the demo strategy is not trading, and the console's
/// evaluation must (a) price the holding, (b) report the market's effect on it
/// as `holdings_pnl` and not as the strategy's, and (c) count it as exposure.
#[test]
#[ignore = "needs your testnet keys in APEXES_TESTNET_API_KEY / APEXES_TESTNET_API_SECRET and Postgres"]
fn a_preexisting_holding_is_separated_from_the_strategy_in_the_real_evaluation() {
    use apexes_engine::SessionMessage;
    use futures::StreamExt;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let (Ok(key), Ok(secret)) = (
        std::env::var("APEXES_TESTNET_API_KEY"),
        std::env::var("APEXES_TESTNET_API_SECRET"),
    ) else {
        panic!("set APEXES_TESTNET_API_KEY and APEXES_TESTNET_API_SECRET");
    };
    let home = temp_home("holdings");
    let sup = supervisor(&home, &key, &secret);
    // A strategy that never trades, so any strategy PnL would be a bug.
    let idle = home.join("idle.drift");
    std::fs::write(
        &idle,
        "function on_market_event(event, ctx) {\n  return [];\n}\n",
    )
    .unwrap();
    let mut req = request("t-holdings");
    req.strategy = StrategyRef::DriftScript { path: idle };
    let launched = sup.launch(req);
    let log = std::fs::read_to_string(
        sup.paths()
            .session_log(&SessionId::parse("t-holdings").unwrap()),
    )
    .unwrap_or_default();
    let launched = match launched {
        Ok(l) => l,
        Err(e) => {
            println!("{log}");
            cleanup(&sup, &home);
            panic!("launch failed: {e}");
        }
    };

    let token = launched.info.token.clone().expect("token");
    let url = launched.info.control_url.clone();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let outcome: Result<String, String> = runtime.block_on(async {
        let mut req = url
            .as_str()
            .into_client_request()
            .map_err(|e| e.to_string())?;
        req.headers_mut().insert(
            "Authorization",
            apexes_security::bearer_header_value(&token)
                .parse()
                .map_err(|_| "header")?,
        );
        let (mut socket, _) = tokio_tungstenite::connect_async(req)
            .await
            .map_err(|e| e.to_string())?;
        // Wait for a snapshot whose evaluation has a priced baseline, then a
        // few more so the price has moved at least once.
        let mut last = None;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(75);
        while let Ok(Some(Ok(Message::Text(text)))) =
            tokio::time::timeout_at(deadline, socket.next()).await
        {
            if let Ok(SessionMessage::Snapshot(s)) = serde_json::from_str::<SessionMessage>(&text)
                && !s.is_placeholder()
                && s.evaluation
                    .as_ref()
                    .is_some_and(|e| e.holdings_value_at_start > dec!(0))
            {
                last = Some(s);
            }
        }
        let s = last.ok_or("no snapshot with a priced holding")?;
        let e = s.evaluation.ok_or("no evaluation")?;
        let btc = s.balances.get("BTC").copied().unwrap_or_default();
        let line = format!(
            "BTC held {btc}; start equity {}; total {} = holdings {} + strategy {}; \
             exposure {}; drawdown strat/account {}/{}; positions open: {}",
            e.start_equity,
            e.total_pnl,
            e.holdings_pnl,
            e.strategy_pnl,
            e.gross_exposure_fraction,
            e.strategy_max_drawdown,
            e.max_drawdown,
            s.positions
                .values()
                .filter(|p| !p.quantity.is_zero())
                .count()
        );
        if e.holdings_value_at_start <= dec!(0) {
            return Err(format!("holding not seen as a baseline: {line}"));
        }
        if e.strategy_pnl != dec!(0) || e.strategy_max_drawdown != dec!(0) {
            return Err(format!("an idle strategy reported its own PnL: {line}"));
        }
        // The split is rounded to 8 places, `total_pnl` is not: equal to within that.
        if (e.strategy_pnl + e.holdings_pnl - e.total_pnl).abs() > dec!(0.000001) {
            return Err(format!("the parts do not add up: {line}"));
        }
        if e.gross_exposure_fraction < dec!(0.5) {
            return Err(format!(
                "a large holding is not counted as exposure: {line}"
            ));
        }
        Ok(line)
    });
    println!("--- {outcome:?}");
    let _ = sup.request_stop(
        &SessionId::parse("t-holdings").unwrap(),
        Duration::from_secs(30),
        true,
    );
    let _ = sup.stop_gateway(BinanceNetwork::Testnet);
    std::fs::remove_dir_all(&home).ok();
    outcome.expect("the real evaluation separates the holding from the strategy");
}
