//! Launches REAL processes (test doubles for the engine and gateway that
//! follow the real binaries' contracts) through the real `Supervisor`, in a
//! throwaway HOME, and checks the properties that matter: detached spawn,
//! secrets only by environment and `0600` files, credential isolation
//! between networks, discovery, clean stop, and every failure path.

use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use apexes_core::binance_network::BinanceNetwork;
use apexes_engine::LiveRiskLimits;
use apexes_strategy_loader::StrategyRef;
use apexes_supervisor::{
    LaunchRequest, SessionId, Supervisor, SupervisorError, process::EnvLookup,
};
use rust_decimal_macros::dec;

const TESTNET_KEY: &str = "APEXES_CRED_BINANCE_MAIN_TESTNET_API_KEY";
const TESTNET_SECRET: &str = "APEXES_CRED_BINANCE_MAIN_TESTNET_API_SECRET";
const MAINNET_KEY: &str = "APEXES_CRED_BINANCE_MAIN_MAINNET_API_KEY";
const MAINNET_SECRET: &str = "APEXES_CRED_BINANCE_MAIN_MAINNET_API_SECRET";

struct Fixture {
    home: PathBuf,
    supervisor: Supervisor,
}

fn bin(name: &str) -> PathBuf {
    // `cargo test` builds this package's binaries and exposes them.
    match name {
        "engine" => PathBuf::from(env!("CARGO_BIN_EXE_fake-engine")),
        _ => PathBuf::from(env!("CARGO_BIN_EXE_fake-gateway")),
    }
}

fn fixture(vars: &[(&str, &str)]) -> Fixture {
    let home =
        std::env::temp_dir().join(format!("apexes-launch-{}", uuid::Uuid::new_v4().simple()));
    std::fs::create_dir_all(&home).unwrap();
    let map: HashMap<String, String> = vars
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let env: EnvLookup = Arc::new(move |name| map.get(name).cloned());
    let supervisor = Supervisor::with_env(&home, env)
        .with_binaries(bin("engine"), bin("gateway"))
        .with_timing(
            Duration::from_secs(20),
            Duration::from_millis(900),
            Duration::from_millis(50),
        );
    Fixture { home, supervisor }
}

fn testnet_vars() -> Vec<(&'static str, &'static str)> {
    vec![
        ("PATH", "/usr/bin:/bin"),
        (TESTNET_KEY, "testnet-key-value"),
        (TESTNET_SECRET, "testnet-secret-value"),
        // Present in the GUI's environment; must never reach a testnet session.
        (MAINNET_KEY, "REAL-MAINNET-KEY"),
        (MAINNET_SECRET, "REAL-MAINNET-SECRET"),
        ("UNRELATED_SECRET", "do-not-leak"),
    ]
}

fn request(network: BinanceNetwork, id: &str, symbols: &[&str]) -> LaunchRequest {
    LaunchRequest {
        network,
        mainnet_confirmation: "MAINNET".to_string(),
        credentials_ref: "binance-main".to_string(),
        strategy: StrategyRef::DriftScript {
            path: PathBuf::from("/s/strategy.drift"),
        },
        symbols: symbols.iter().map(|s| s.to_string()).collect(),
        risk_limits: LiveRiskLimits {
            max_order_notional: dec!(200),
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

fn seen(home: &std::path::Path, id: &str) -> serde_json::Value {
    let path = home
        .join(".apexes/sessions")
        .join(format!("{id}.seen.json"));
    serde_json::from_str(
        &std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path:?}: {e}")),
    )
    .unwrap()
}

/// Files of every gateway a test has started, by extension.
fn gateway_files(f: &Fixture, extension: &str) -> Vec<PathBuf> {
    let mut found: Vec<PathBuf> = std::fs::read_dir(f.home.join(".apexes/gateways"))
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == extension))
        .collect();
    found.sort();
    found
}

/// The files of the gateway serving `port`.
fn files_of_gateway(f: &Fixture, port: u16) -> apexes_supervisor::GatewayFiles {
    [BinanceNetwork::Testnet, BinanceNetwork::Mainnet]
        .into_iter()
        .flat_map(|n| f.supervisor.paths().gateway_keys(n))
        .map(|key| f.supervisor.paths().gateway_files(&key))
        .find(|files| {
            std::fs::read_to_string(&files.pid)
                .is_ok_and(|text| text.lines().nth(1) == Some(&port.to_string()))
        })
        .unwrap_or_else(|| panic!("no gateway files for port {port}"))
}

/// The value a gateway was started with for `flag`, found by name.
fn gateway_flag(f: &Fixture, port: u16, flag: &str) -> String {
    let path = f.home.join(format!(
        ".apexes/market-data-gateway-testnet-{port}.seen.json"
    ));
    let json: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path:?}: {e}")),
    )
    .unwrap();
    let args: Vec<&str> = json["args"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a.as_str().unwrap())
        .collect();
    let at = args
        .iter()
        .position(|a| *a == flag)
        .unwrap_or_else(|| panic!("{flag} not in {args:?}"));
    args[at + 1].to_string()
}

fn mode(path: &std::path::Path) -> u32 {
    std::fs::metadata(path).unwrap().permissions().mode() & 0o777
}

/// Stops anything a test started, whatever the test outcome.
struct Cleanup<'a>(&'a Fixture);
impl Drop for Cleanup<'_> {
    fn drop(&mut self) {
        for s in self.0.supervisor.discover() {
            if s.alive {
                let _ = self
                    .0
                    .supervisor
                    .request_stop(&s.id, Duration::from_secs(3), true);
            }
        }
        for network in [BinanceNetwork::Testnet, BinanceNetwork::Mainnet] {
            let _ = self.0.supervisor.stop_gateway(network);
        }
        std::fs::remove_dir_all(&self.0.home).ok();
    }
}

#[test]
fn a_testnet_launch_starts_a_gateway_and_an_engine_that_outlive_the_caller() {
    let f = fixture(&testnet_vars());
    let _cleanup = Cleanup(&f);
    let launched = f
        .supervisor
        .launch(request(BinanceNetwork::Testnet, "t-detach", &["BTCUSDT"]))
        .expect("launch");

    assert!(launched.gateway.started);
    assert_eq!(launched.info.network, Some(BinanceNetwork::Testnet));
    assert!(launched.info.alive);
    assert_eq!(
        launched.info.control_url,
        format!("ws://127.0.0.1:{}/session/ws", launched.info.port)
    );

    // Detached: the engine leads its own process group, so a signal to (or
    // exit of) this process's group cannot reach it.
    assert_eq!(seen(&f.home, "t-detach")["own_process_group"], true);
    assert!(apexes_supervisor::pidfile::pid_exists(launched.info.pid));
}

#[test]
fn secrets_travel_by_environment_and_private_files_never_by_command_line_or_launch_config() {
    let f = fixture(&testnet_vars());
    let _cleanup = Cleanup(&f);
    let launched = f
        .supervisor
        .launch(request(BinanceNetwork::Testnet, "t-secrets", &["BTCUSDT"]))
        .unwrap();
    let token = launched
        .info
        .token
        .clone()
        .expect("token returned to the caller");
    let s = seen(&f.home, "t-secrets");

    // The control token reaches the engine as a named operator token.
    assert_eq!(s["control_tokens"], format!("gui:operator:{token}"));
    // Gateway token reaches the engine too.
    assert_eq!(
        Some(s["gateway_token"].as_str().unwrap().to_string()),
        launched.gateway.token
    );

    // Nothing secret on the command line or in the launch config.
    let args = s["args"].to_string();
    let config = std::fs::read_to_string(
        f.supervisor
            .paths()
            .session_config(&SessionId::parse("t-secrets").unwrap()),
    )
    .unwrap();
    for secret in [
        token.as_str(),
        "testnet-key-value",
        "testnet-secret-value",
        launched.gateway.token.as_deref().unwrap(),
    ] {
        assert!(!args.contains(secret), "secret on command line: {args}");
        assert!(!config.contains(secret), "secret in launch config");
    }

    // Files holding secrets are owner-only.
    let id = SessionId::parse("t-secrets").unwrap();
    for path in [
        f.supervisor.paths().session_token(&id),
        f.supervisor.paths().session_config(&id),
        f.supervisor.paths().session_log(&id),
        files_of_gateway(&f, launched.gateway.port).token,
    ] {
        assert_eq!(mode(&path), 0o600, "{path:?}");
    }
}

#[test]
fn a_testnet_session_never_sees_mainnet_keys_or_unrelated_secrets() {
    let f = fixture(&testnet_vars());
    let _cleanup = Cleanup(&f);
    f.supervisor
        .launch(request(
            BinanceNetwork::Testnet,
            "t-isolation",
            &["BTCUSDT"],
        ))
        .unwrap();
    let names: Vec<String> =
        serde_json::from_value(seen(&f.home, "t-isolation")["env_names"].clone()).unwrap();
    assert!(names.contains(&TESTNET_KEY.to_string()));
    assert!(names.contains(&TESTNET_SECRET.to_string()));
    assert!(
        !names.contains(&MAINNET_KEY.to_string()),
        "mainnet key leaked: {names:?}"
    );
    assert!(
        !names.contains(&MAINNET_SECRET.to_string()),
        "mainnet secret leaked: {names:?}"
    );
    assert!(
        !names.contains(&"UNRELATED_SECRET".to_string()),
        "unrelated secret leaked: {names:?}"
    );
    assert_eq!(
        seen(&f.home, "t-isolation")["home"],
        f.home.to_string_lossy().as_ref()
    );
}

#[test]
fn missing_credentials_are_reported_by_variable_name_before_anything_starts() {
    let f = fixture(&[("PATH", "/usr/bin")]);
    let _cleanup = Cleanup(&f);
    let error = f
        .supervisor
        .launch(request(BinanceNetwork::Testnet, "t-nocreds", &["BTCUSDT"]))
        .unwrap_err();
    match &error {
        SupervisorError::MissingCredentials { missing_var, .. } => {
            assert_eq!(missing_var, TESTNET_KEY)
        }
        other => panic!("{other:?}"),
    }
    assert!(error.to_string().contains(TESTNET_KEY));
    // Nothing was started: no PID files, no gateway.
    assert!(f.supervisor.discover().is_empty());
    assert!(gateway_files(&f, "pid").is_empty());
}

#[test]
fn only_the_matching_networks_credentials_count() {
    // Mainnet keys are present, testnet keys are not: a testnet launch fails.
    let f = fixture(&[
        ("PATH", "/usr/bin"),
        (MAINNET_KEY, "k"),
        (MAINNET_SECRET, "s"),
    ]);
    let _cleanup = Cleanup(&f);
    assert!(matches!(
        f.supervisor
            .launch(request(BinanceNetwork::Testnet, "t-wrongnet", &["BTCUSDT"]))
            .unwrap_err(),
        SupervisorError::MissingCredentials { .. }
    ));
}

#[test]
fn mainnet_needs_the_typed_confirmation_and_nothing_starts_without_it() {
    let f = fixture(&testnet_vars());
    let _cleanup = Cleanup(&f);
    let mut r = request(BinanceNetwork::Mainnet, "m-unconfirmed", &["BTCUSDT"]);
    r.mainnet_confirmation = "yes".to_string();
    assert!(matches!(
        f.supervisor.launch(r).unwrap_err(),
        SupervisorError::InvalidRequest(_)
    ));
    assert!(f.supervisor.discover().is_empty());
    assert!(gateway_files(&f, "pid").is_empty());
}

fn alive_sessions(f: &Fixture) -> usize {
    f.supervisor.discover().iter().filter(|s| s.alive).count()
}

#[test]
fn a_second_session_on_covered_symbols_reuses_the_gateway() {
    let f = fixture(&testnet_vars());
    let _cleanup = Cleanup(&f);
    let first = f
        .supervisor
        .launch(request(
            BinanceNetwork::Testnet,
            "t-one",
            &["BTCUSDT", "ETHUSDT"],
        ))
        .unwrap();
    let second = f
        .supervisor
        .launch(request(BinanceNetwork::Testnet, "t-two", &["BTCUSDT"]))
        .unwrap();
    assert!(first.gateway.started);
    assert!(!second.gateway.started, "must reuse, not start another");
    assert_eq!(first.gateway.port, second.gateway.port);
    assert_eq!(first.gateway.token, second.gateway.token);
    assert_eq!(gateway_files(&f, "pid").len(), 1);
}

/// The reported bug: with a BTCUSDT session running, launching ETHUSDT failed
/// with "a gateway is already running with instruments [BTCUSDT]".
#[test]
fn a_symbol_no_gateway_carries_gets_its_own_gateway_and_nothing_is_disturbed() {
    let f = fixture(&testnet_vars());
    let _cleanup = Cleanup(&f);
    let btc = f
        .supervisor
        .launch(request(BinanceNetwork::Testnet, "t-btc", &["BTCUSDT"]))
        .unwrap();
    let eth = f
        .supervisor
        .launch(request(BinanceNetwork::Testnet, "t-eth", &["ETHUSDT"]))
        .expect("another symbol must launch, not be refused");

    assert!(btc.gateway.started && eth.gateway.started);
    assert_ne!(
        btc.gateway.port, eth.gateway.port,
        "a gateway per symbol set"
    );
    assert_ne!(
        btc.gateway.token, eth.gateway.token,
        "each gateway has its own secret"
    );
    assert_eq!(
        gateway_flag(&f, btc.gateway.port, "--instruments"),
        "BTCUSDT"
    );
    assert_eq!(
        gateway_flag(&f, eth.gateway.port, "--instruments"),
        "ETHUSDT"
    );
    assert_eq!(gateway_files(&f, "pid").len(), 2);

    // The BTC gateway and session were not restarted or stopped.
    assert!(apexes_supervisor::pidfile::port_open(btc.gateway.port));
    assert!(apexes_supervisor::pidfile::port_open(eth.gateway.port));
    assert_eq!(alive_sessions(&f), 2);

    // Each engine was pointed at its own gateway.
    let gateway_url = |id: &str| -> String {
        let text = std::fs::read_to_string(
            f.supervisor
                .paths()
                .session_config(&SessionId::parse(id).unwrap()),
        )
        .unwrap();
        let config: serde_json::Value = serde_json::from_str(&text).unwrap();
        config["gateway_url"].as_str().unwrap().to_string()
    };
    assert_eq!(
        gateway_url("t-btc"),
        format!("ws://127.0.0.1:{}/market", btc.gateway.port)
    );
    assert_eq!(
        gateway_url("t-eth"),
        format!("ws://127.0.0.1:{}/market", eth.gateway.port)
    );

    // Later launches pick the matching gateway, not a new one.
    let again = f
        .supervisor
        .launch(request(BinanceNetwork::Testnet, "t-btc2", &["BTCUSDT"]))
        .unwrap();
    assert!(!again.gateway.started);
    assert_eq!(again.gateway.port, btc.gateway.port);
    assert_eq!(gateway_files(&f, "pid").len(), 2);
}

#[test]
fn a_request_no_single_gateway_covers_starts_one_that_does() {
    let f = fixture(&testnet_vars());
    let _cleanup = Cleanup(&f);
    let btc = f
        .supervisor
        .launch(request(BinanceNetwork::Testnet, "t-b", &["BTCUSDT"]))
        .unwrap();
    let eth = f
        .supervisor
        .launch(request(BinanceNetwork::Testnet, "t-e", &["ETHUSDT"]))
        .unwrap();
    let both = f
        .supervisor
        .launch(request(
            BinanceNetwork::Testnet,
            "t-both",
            &["BTCUSDT", "ETHUSDT"],
        ))
        .expect("two feeds do not make one that carries both");
    assert!(both.gateway.started);
    assert!(![btc.gateway.port, eth.gateway.port].contains(&both.gateway.port));
    assert_eq!(gateway_files(&f, "pid").len(), 3);
    assert_eq!(alive_sessions(&f), 3);
}

#[test]
fn networks_never_share_a_gateway() {
    let mut vars = testnet_vars();
    vars.push((MAINNET_KEY, "k"));
    vars.push((MAINNET_SECRET, "s"));
    let f = fixture(&vars);
    let _cleanup = Cleanup(&f);
    let testnet = f
        .supervisor
        .launch(request(BinanceNetwork::Testnet, "t-n", &["BTCUSDT"]))
        .unwrap();
    let mainnet = f
        .supervisor
        .launch(request(BinanceNetwork::Mainnet, "t-m", &["BTCUSDT"]))
        .unwrap();
    assert!(testnet.gateway.started && mainnet.gateway.started);
    assert_ne!(testnet.gateway.port, mainnet.gateway.port);
    assert_eq!(testnet.gateway.network, BinanceNetwork::Testnet);
    assert_eq!(mainnet.gateway.network, BinanceNetwork::Mainnet);
}

#[test]
fn stopping_gateways_waits_for_every_session_and_then_stops_all_of_them() {
    let f = fixture(&testnet_vars());
    let _cleanup = Cleanup(&f);
    let btc = f
        .supervisor
        .launch(request(BinanceNetwork::Testnet, "t-sb", &["BTCUSDT"]))
        .unwrap();
    let eth = f
        .supervisor
        .launch(request(BinanceNetwork::Testnet, "t-se", &["ETHUSDT"]))
        .unwrap();
    assert!(matches!(
        f.supervisor
            .stop_gateway(BinanceNetwork::Testnet)
            .unwrap_err(),
        SupervisorError::GatewayInUse { .. }
    ));
    for id in ["t-sb", "t-se"] {
        f.supervisor
            .request_stop(
                &SessionId::parse(id).unwrap(),
                Duration::from_secs(10),
                false,
            )
            .unwrap();
    }
    f.supervisor.stop_gateway(BinanceNetwork::Testnet).unwrap();
    std::thread::sleep(Duration::from_millis(600));
    assert!(!apexes_supervisor::pidfile::port_open(btc.gateway.port));
    assert!(!apexes_supervisor::pidfile::port_open(eth.gateway.port));
}

/// An older build ran one gateway in flat files, with no record of what it
/// streams. It must not block a launch, and must still be stoppable.
#[test]
fn a_gateway_from_an_older_build_neither_blocks_a_launch_nor_gets_lost() {
    let f = fixture(&testnet_vars());
    let _cleanup = Cleanup(&f);
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let mut legacy = std::process::Command::new(bin("gateway"))
        .args(["--network", "testnet", "--instruments", "BTCUSDT", "--port"])
        .arg(port.to_string())
        .env("HOME", &f.home)
        .spawn()
        .unwrap();
    let legacy_pid = f.home.join(".apexes/market-data-gateway-testnet.pid");
    for _ in 0..100 {
        if legacy_pid.exists() && apexes_supervisor::pidfile::port_open(port) {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(legacy_pid.exists(), "the stand-in legacy gateway is up");

    let launched = f
        .supervisor
        .launch(request(
            BinanceNetwork::Testnet,
            "t-after-legacy",
            &["ETHUSDT"],
        ))
        .expect("an unknown gateway must not block a launch");
    assert!(launched.gateway.started);
    assert_ne!(launched.gateway.port, port);

    f.supervisor
        .request_stop(
            &SessionId::parse("t-after-legacy").unwrap(),
            Duration::from_secs(10),
            false,
        )
        .unwrap();
    f.supervisor.stop_gateway(BinanceNetwork::Testnet).unwrap();
    let exited = (0..60).any(|_| {
        std::thread::sleep(Duration::from_millis(50));
        legacy.try_wait().ok().flatten().is_some()
    });
    if !exited {
        legacy.kill().ok();
    }
    assert!(exited, "the legacy gateway was stopped too");
    assert!(!apexes_supervisor::pidfile::port_open(
        launched.gateway.port
    ));
}

#[test]
fn files_of_long_dead_gateways_are_tidied_but_recent_ones_are_kept() {
    let f = fixture(&testnet_vars());
    let _cleanup = Cleanup(&f);
    let dir = f.home.join(".apexes/gateways");
    std::fs::create_dir_all(&dir).unwrap();
    let make = |slot: &str, age: Duration| {
        for ext in ["json", "token", "pid", "log"] {
            let path = dir.join(format!("testnet-{slot}.{ext}"));
            let body = if ext == "pid" {
                "999999\n1\n"
            } else {
                "{\"instruments\":[\"SOLUSDT\"],\"port\":1}"
            };
            std::fs::write(&path, body).unwrap();
            let file = std::fs::File::options().write(true).open(&path).unwrap();
            file.set_modified(std::time::SystemTime::now() - age)
                .unwrap();
        }
    };
    make("olddead", Duration::from_secs(3600));
    make("newdead", Duration::from_secs(30));

    f.supervisor
        .launch(request(BinanceNetwork::Testnet, "t-tidy", &["BTCUSDT"]))
        .unwrap();
    assert!(
        !dir.join("testnet-olddead.token").exists(),
        "old secret removed"
    );
    assert!(!dir.join("testnet-olddead.json").exists());
    assert!(
        dir.join("testnet-newdead.json").exists(),
        "recent record kept"
    );
}

#[test]
fn discovery_finds_running_sessions_with_their_network_and_token() {
    let f = fixture(&testnet_vars());
    let _cleanup = Cleanup(&f);
    let launched = f
        .supervisor
        .launch(request(BinanceNetwork::Testnet, "t-discover", &["BTCUSDT"]))
        .unwrap();

    // A fresh supervisor (as after the GUI restarts) finds it again.
    let restarted = Supervisor::with_env(&f.home, Arc::new(|_| None));
    let found = restarted.discover();
    assert_eq!(found.len(), 1);
    let info = &found[0];
    assert_eq!(info.id.as_str(), "t-discover");
    assert_eq!(info.pid, launched.info.pid);
    assert!(info.alive);
    assert_eq!(info.network, Some(BinanceNetwork::Testnet));
    assert_eq!(
        info.token, launched.info.token,
        "token survives for reconnecting"
    );
}

#[test]
fn stop_goes_through_the_control_endpoint_and_the_process_exits() {
    let f = fixture(&testnet_vars());
    let _cleanup = Cleanup(&f);
    let launched = f
        .supervisor
        .launch(request(BinanceNetwork::Testnet, "t-stop", &["BTCUSDT"]))
        .unwrap();
    let id = SessionId::parse("t-stop").unwrap();

    f.supervisor
        .request_stop(&id, Duration::from_secs(10), false)
        .expect("stop");
    assert!(!apexes_supervisor::pidfile::pid_exists(launched.info.pid));
    let after = f.supervisor.discover();
    assert!(after.iter().all(|s| !s.alive), "{after:?}");
}

#[test]
fn an_engine_that_ignores_stop_times_out_and_is_not_killed_unless_forced() {
    let f = fixture(&testnet_vars());
    let _cleanup = Cleanup(&f);
    let launched = f
        .supervisor
        .launch(request(BinanceNetwork::Testnet, "t-deaf", &["BTCUSDT"]))
        .unwrap();
    let id = SessionId::parse("t-deaf").unwrap();

    let error = f
        .supervisor
        .request_stop(&id, Duration::from_millis(600), false)
        .unwrap_err();
    assert!(
        matches!(error, SupervisorError::StopTimedOut { .. }),
        "{error:?}"
    );
    assert!(
        apexes_supervisor::pidfile::pid_exists(launched.info.pid),
        "not killed without force"
    );

    // Forced: SIGTERM, only to a PID verified to be this session's engine.
    f.supervisor
        .request_stop(&id, Duration::from_millis(300), true)
        .expect("forced stop");
    std::thread::sleep(Duration::from_millis(500));
    assert!(!apexes_supervisor::pidfile::pid_exists(launched.info.pid));
}

#[test]
fn an_engine_that_refuses_to_start_is_reported_with_its_log_and_leaves_no_secret_behind() {
    let f = fixture(&testnet_vars());
    let _cleanup = Cleanup(&f);
    let error = f
        .supervisor
        .launch(request(BinanceNetwork::Testnet, "t-badstart", &["BTCUSDT"]))
        .unwrap_err();
    match &error {
        SupervisorError::ExitedDuringStartup { log_tail, .. } => {
            assert!(log_tail.contains("startup recovery failed"), "{log_tail}");
        }
        other => panic!("{other:?}"),
    }
    let id = SessionId::parse("t-badstart").unwrap();
    assert!(
        !f.supervisor.paths().session_token(&id).exists(),
        "token file removed"
    );
    assert!(f.supervisor.discover().iter().all(|s| !s.alive));
}

#[test]
fn an_engine_that_serves_then_dies_during_the_settle_window_is_a_failed_launch() {
    // Live engines open their port before startup recovery, and recovery
    // can still refuse to start: "serving" alone is not success.
    let f = fixture(&testnet_vars());
    let _cleanup = Cleanup(&f);
    let error = f
        .supervisor
        .launch(request(BinanceNetwork::Testnet, "t-crash", &["BTCUSDT"]))
        .unwrap_err();
    assert!(
        matches!(error, SupervisorError::ExitedDuringStartup { .. }),
        "{error:?}"
    );
}

#[test]
fn a_gateway_that_cannot_start_is_reported_and_cleaned_up() {
    let f = fixture(&testnet_vars());
    let _cleanup = Cleanup(&f);
    let error = f
        .supervisor
        .launch(request(
            BinanceNetwork::Testnet,
            "t-gwfail",
            &["BADSTARTUSDT"],
        ))
        .unwrap_err();
    match &error {
        SupervisorError::ExitedDuringStartup { what, log_tail, .. } => {
            assert!(what.contains("gateway"), "{what}");
            assert!(
                log_tail.contains("cannot connect to upstream"),
                "{log_tail}"
            );
        }
        other => panic!("{other:?}"),
    }
    assert!(
        gateway_files(&f, "token").is_empty(),
        "the secret of a gateway that never came up is removed"
    );
    assert!(gateway_files(&f, "json").is_empty());
    assert!(f.supervisor.discover().is_empty(), "no engine was started");
}

#[test]
fn a_missing_binary_is_a_clear_error_and_starts_nothing() {
    let f = fixture(&testnet_vars());
    let _cleanup = Cleanup(&f);
    let broken = f
        .supervisor
        .clone()
        .with_binaries(PathBuf::from("/nonexistent/apexes-engine"), bin("gateway"));
    let error = broken
        .launch(request(BinanceNetwork::Testnet, "t-nobin", &["BTCUSDT"]))
        .unwrap_err();
    assert!(
        matches!(
            error,
            SupervisorError::BinaryNotFound {
                name: "apexes-engine",
                ..
            }
        ),
        "{error:?}"
    );
    assert!(
        gateway_files(&f, "pid").is_empty(),
        "no gateway started either"
    );
}

#[test]
fn launching_an_id_that_is_already_running_is_refused() {
    let f = fixture(&testnet_vars());
    let _cleanup = Cleanup(&f);
    f.supervisor
        .launch(request(BinanceNetwork::Testnet, "t-dup", &["BTCUSDT"]))
        .unwrap();
    assert!(matches!(
        f.supervisor
            .launch(request(BinanceNetwork::Testnet, "t-dup", &["BTCUSDT"]))
            .unwrap_err(),
        SupervisorError::SessionAlreadyRunning { .. }
    ));
}

#[test]
fn the_gateway_is_not_stopped_while_a_session_uses_it_and_is_afterwards() {
    let f = fixture(&testnet_vars());
    let _cleanup = Cleanup(&f);
    let launched = f
        .supervisor
        .launch(request(BinanceNetwork::Testnet, "t-gwuse", &["BTCUSDT"]))
        .unwrap();
    assert!(matches!(
        f.supervisor
            .stop_gateway(BinanceNetwork::Testnet)
            .unwrap_err(),
        SupervisorError::GatewayInUse { .. }
    ));
    let id = SessionId::parse("t-gwuse").unwrap();
    f.supervisor
        .request_stop(&id, Duration::from_secs(10), false)
        .unwrap();
    f.supervisor
        .stop_gateway(BinanceNetwork::Testnet)
        .expect("stop gateway once unused");
    std::thread::sleep(Duration::from_millis(500));
    assert!(!apexes_supervisor::pidfile::port_open(
        launched.gateway.port
    ));
}

#[test]
fn a_stale_pid_file_for_an_unrelated_process_is_not_treated_as_a_running_session() {
    let f = fixture(&testnet_vars());
    let _cleanup = Cleanup(&f);
    let dir = f.home.join(".apexes/sessions");
    std::fs::create_dir_all(&dir).unwrap();
    // This test's own PID is alive, but nothing listens on the recorded port.
    std::fs::write(
        dir.join("t-stale.pid"),
        format!("{}\n1\n", std::process::id()),
    )
    .unwrap();
    let found = f.supervisor.discover();
    assert_eq!(found.len(), 1);
    assert!(!found[0].alive);
    // And it never blocks a fresh launch of the same id.
    f.supervisor
        .launch(request(BinanceNetwork::Testnet, "t-stale", &["BTCUSDT"]))
        .unwrap();
}

#[test]
fn forget_removes_a_stopped_sessions_files_and_refuses_a_running_one() {
    let f = fixture(&testnet_vars());
    let _cleanup = Cleanup(&f);
    let launched = f
        .supervisor
        .launch(request(BinanceNetwork::Testnet, "t-forget", &["BTCUSDT"]))
        .unwrap();
    let id = SessionId::parse("t-forget").unwrap();
    assert!(matches!(
        f.supervisor.forget(&id).unwrap_err(),
        SupervisorError::SessionStillRunning { .. }
    ));
    f.supervisor
        .request_stop(&id, Duration::from_secs(10), false)
        .unwrap();
    std::thread::sleep(Duration::from_millis(300));
    f.supervisor.forget(&id).expect("forget");
    assert!(!f.supervisor.paths().session_token(&id).exists());
    assert!(!f.supervisor.paths().session_config(&id).exists());
    assert!(f.supervisor.discover().is_empty());
    let _ = launched;
}

#[test]
fn stopping_a_session_that_does_not_exist_is_an_error() {
    let f = fixture(&testnet_vars());
    let _cleanup = Cleanup(&f);
    let id = SessionId::parse("ghost").unwrap();
    assert!(matches!(
        f.supervisor
            .request_stop(&id, Duration::from_millis(100), false)
            .unwrap_err(),
        SupervisorError::NoSuchSession { .. }
    ));
}

#[test]
fn a_gateway_that_loses_the_race_for_its_port_is_retried_on_a_fresh_port() {
    let f = fixture(&testnet_vars());
    let _cleanup = Cleanup(&f);
    let launched = f
        .supervisor
        .launch(request(
            BinanceNetwork::Testnet,
            "t-race",
            &["BINDFAILONCEUSDT"],
        ))
        .expect("the retry must recover from a lost port race");
    assert!(launched.gateway.started);
    assert!(launched.info.alive);
    let marker = f.home.join(".apexes/bindfailonce.marker");
    assert!(marker.exists(), "the first attempt really did fail");
}
