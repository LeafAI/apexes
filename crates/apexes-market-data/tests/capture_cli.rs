//! Offline subprocess coverage of the opt-in collector entry point.
use std::process::Command;

fn command() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_raw-depth-collect"));
    cmd.env_remove("APEXES_CAPTURE_CLICKHOUSE_URL");
    cmd
}

#[test]
fn help_describes_explicit_capture_controls() {
    let output = command().arg("--help").output().unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    for option in [
        "--symbol",
        "--market",
        "--duration-seconds",
        "--receipt-dir",
    ] {
        assert!(help.contains(option));
    }
}

#[test]
fn missing_storage_configuration_does_not_create_a_run_or_print_success() {
    let dir = std::env::temp_dir().join(format!("apexes-capture-cli-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let output = command()
        .args([
            "--symbol",
            "BTCUSDT",
            "--market",
            "spot",
            "--duration-seconds",
            "1",
            "--receipt-dir",
        ])
        .arg(&dir)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("APEXES_CAPTURE_CLICKHOUSE_URL"));
    assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
    std::fs::remove_dir(&dir).unwrap();
}

#[test]
fn invalid_duration_is_rejected_before_initializing_storage() {
    let output = command()
        .args([
            "--symbol",
            "BTCUSDT",
            "--market",
            "spot",
            "--duration-seconds",
            "0",
            "--receipt-dir",
            ".",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--duration-seconds"));
}
