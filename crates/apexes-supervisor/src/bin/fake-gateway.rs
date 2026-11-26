//! TEST DOUBLE for `apexes-market-data-gateway`, used only by this crate's
//! integration tests. Follows the real gateway's contract: `--port`,
//! `--instruments`, `--network` (required), `--pid-file` (optional) and a
//! `<pid>\n<port>\n` file at that path, or by default at
//! `$HOME/.apexes/market-data-gateway-<network>.pid`. Default SIGTERM
//! handling, like the real one.
//!
//! Hooks: `--instruments` containing `BADSTART` exits with status 5 before
//! serving; containing `BINDFAILONCE` fails the first start with the real
//! gateway's bind-error message (the port was taken between the supervisor
//! picking it and the child binding it), and starts normally afterwards. It records how it was launched into
//! `$HOME/.apexes/market-data-gateway-<network>.seen.json`.

use std::net::TcpListener;
use std::path::PathBuf;

fn fail(message: &str, code: i32) -> ! {
    eprintln!("fake-gateway: {message}");
    std::process::exit(code)
}

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let network = flag(&args, "--network").unwrap_or_else(|| fail("--network is required", 2));
    if network != "mainnet" && network != "testnet" {
        fail("--network must be mainnet or testnet", 2);
    }
    let instruments = flag(&args, "--instruments").unwrap_or_default();
    if instruments.contains("BADSTART") {
        fail("cannot connect to upstream market data (test hook)", 5);
    }
    if instruments.contains("BINDFAILONCE") {
        let home = std::env::var("HOME").unwrap_or_default();
        let marker = PathBuf::from(home)
            .join(".apexes")
            .join("bindfailonce.marker");
        if !marker.exists() {
            std::fs::create_dir_all(marker.parent().expect("parent")).ok();
            std::fs::write(&marker, "x").ok();
            fail(
                "failed to bind 127.0.0.1:0: Address already in use (os error 48)",
                2,
            );
        }
    }
    let port: u16 = flag(&args, "--port")
        .and_then(|p| p.parse().ok())
        .unwrap_or_else(|| fail("--port required", 2));

    let home = PathBuf::from(std::env::var("HOME").unwrap_or_else(|_| fail("HOME not set", 2)));
    let dir = home.join(".apexes");
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| fail(&format!("mkdir: {e}"), 2));
    let listener = TcpListener::bind(("127.0.0.1", port))
        .unwrap_or_else(|e| fail(&format!("failed to bind 127.0.0.1:{port}: {e}"), 2));

    let mut names: Vec<String> = std::env::vars().map(|(k, _)| k).collect();
    names.sort();
    let seen = serde_json::json!({
        "args": args,
        "env_names": names,
        "gateway_token": std::env::var("APEXES_GATEWAY_TOKEN").ok(),
    });
    std::fs::write(
        dir.join(format!("market-data-gateway-{network}-{port}.seen.json")),
        seen.to_string(),
    )
    .unwrap_or_else(|e| fail(&format!("write seen: {e}"), 2));
    let pid_path = flag(&args, "--pid-file")
        .map(PathBuf::from)
        .unwrap_or_else(|| dir.join(format!("market-data-gateway-{network}.pid")));
    if let Some(parent) = pid_path.parent() {
        std::fs::create_dir_all(parent).unwrap_or_else(|e| fail(&format!("mkdir: {e}"), 2));
    }
    std::fs::write(&pid_path, format!("{}\n{}\n", std::process::id(), port))
        .unwrap_or_else(|e| fail(&format!("write pid: {e}"), 2));

    for stream in listener.incoming() {
        drop(stream);
    }
}
