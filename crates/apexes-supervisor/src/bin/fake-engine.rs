//! TEST DOUBLE for `apexes-engine`, used only by this crate's integration
//! tests. Behaves like the real engine where the supervisor can tell:
//! `--launch-config <path>`, a `<pid>\n<port>\n` file under
//! `$HOME/.apexes/sessions/<session_id>.pid`, a real WebSocket server, and
//! exit on a `Stop` command frame.
//!
//! Test hooks, keyed on the launch config's `session_id`:
//! - contains `badstart`: exits with status 4 before writing a PID file
//!   (like live startup recovery refusing to start);
//! - contains `crash`: writes the PID file, then exits with status 3 shortly
//!   after (a process that serves briefly and dies);
//! - contains `deaf`: never reacts to `Stop` (a hung engine).
//!
//! It records how it was launched into `<session>.seen.json`: its argument
//! list, the *names* of its environment variables, and the few values the
//! tests assert on.

use std::path::PathBuf;
use std::time::Duration;

use futures::StreamExt;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

fn fail(message: &str, code: i32) -> ! {
    eprintln!("fake-engine: {message}");
    std::process::exit(code)
}

fn own_process_group() -> bool {
    let pid = std::process::id().to_string();
    std::process::Command::new("ps")
        .args(["-o", "pgid=,pid=", "-p", &pid])
        .output()
        .map(|out| {
            let text = String::from_utf8_lossy(&out.stdout).to_string();
            let mut parts = text.split_whitespace();
            matches!((parts.next(), parts.next()), (Some(g), Some(p)) if g == p)
        })
        .unwrap_or(false)
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let config_path = args
        .iter()
        .position(|a| a == "--launch-config")
        .and_then(|i| args.get(i + 1))
        .cloned()
        .unwrap_or_else(|| fail("--launch-config required", 2));
    let text = std::fs::read_to_string(&config_path)
        .unwrap_or_else(|e| fail(&format!("cannot read {config_path}: {e}"), 2));
    let config: serde_json::Value =
        serde_json::from_str(&text).unwrap_or_else(|e| fail(&format!("bad config: {e}"), 2));
    let session_id = config["session_id"]
        .as_str()
        .unwrap_or("unknown")
        .to_string();

    if session_id.contains("badstart") {
        fail("live startup recovery failed (test hook)", 4);
    }

    let home = PathBuf::from(std::env::var("HOME").unwrap_or_else(|_| fail("HOME not set", 2)));
    let dir = home.join(".apexes").join("sessions");
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| fail(&format!("mkdir: {e}"), 2));

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|e| fail(&format!("bind: {e}"), 2));
    let port = listener
        .local_addr()
        .unwrap_or_else(|e| fail(&format!("addr: {e}"), 2))
        .port();

    let mut names: Vec<String> = std::env::vars().map(|(k, _)| k).collect();
    names.sort();
    let seen = serde_json::json!({
        "args": args,
        "env_names": names,
        "control_tokens": std::env::var("APEXES_CONTROL_TOKENS").ok(),
        "gateway_token": std::env::var("APEXES_GATEWAY_TOKEN").ok(),
        "audit_log": std::env::var("APEXES_AUDIT_LOG").ok(),
        "home": std::env::var("HOME").ok(),
        "own_process_group": own_process_group(),
    });
    std::fs::write(
        dir.join(format!("{session_id}.seen.json")),
        seen.to_string(),
    )
    .unwrap_or_else(|e| fail(&format!("write seen: {e}"), 2));
    std::fs::write(
        dir.join(format!("{session_id}.pid")),
        format!("{}\n{}\n", std::process::id(), port),
    )
    .unwrap_or_else(|e| fail(&format!("write pid: {e}"), 2));

    if session_id.contains("crash") {
        tokio::time::sleep(Duration::from_millis(400)).await;
        fail("crashing (test hook)", 3);
    }
    let deaf = session_id.contains("deaf");

    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        tokio::spawn(async move {
            let Ok(mut socket) = tokio_tungstenite::accept_async(stream).await else {
                return;
            };
            while let Some(Ok(message)) = socket.next().await {
                if let Message::Text(text) = message
                    && text.contains("Stop")
                    && !deaf
                {
                    std::process::exit(0);
                }
            }
        });
    }
}
