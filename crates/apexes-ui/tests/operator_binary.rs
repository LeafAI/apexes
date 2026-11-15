//! The `apexes-operator` binary's own entry point.
//!
//! Nothing else exercises its `main`, and an earlier commit shipped the
//! console-only version without the launcher while every other test passed.
//! This runs the real binary and checks what it advertises.

use std::process::Command;

fn help() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_apexes-operator"))
        .arg("--help")
        .output()
        .expect("run apexes-operator --help");
    assert!(output.status.success(), "{output:?}");
    String::from_utf8_lossy(&output.stdout).to_string()
}

#[test]
fn the_binary_is_the_launcher_and_console_not_just_a_console() {
    let text = help();
    assert!(
        text.to_lowercase().contains("launcher"),
        "the binary must advertise the launcher: {text}"
    );
}

#[test]
fn engine_only_mode_is_optional_so_no_arguments_means_the_launcher() {
    let text = help();
    // `--engine` is optional: running with no arguments must start the
    // launcher, not try to connect to a default engine address.
    assert!(text.contains("--engine"), "{text}");
    assert!(
        !text.contains("[default: ws://127.0.0.1:9100/session/ws]"),
        "there must be no default engine address: {text}"
    );
}
