//! Spawning detached children, the environment they are given, and the
//! checks made before signalling a process.

use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use crate::error::SupervisorError;
use crate::secrets::open_private_log;

/// Environment variables passed through to children. Everything else is
/// dropped (`env_clear`), which is a safety property: a testnet engine can
/// never inherit mainnet API keys that happen to be set for the GUI, and
/// unrelated secrets in the operator's shell never reach a trading process.
const PASSTHROUGH: &[&str] = &[
    "PATH",
    "USER",
    "LANG",
    "LC_ALL",
    "TMPDIR",
    "RUST_LOG",
    "APEXES_LOG_FORMAT",
    "APEXES_BINANCE_RECV_WINDOW_MS",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "HTTPS_PROXY",
    "HTTP_PROXY",
    "NO_PROXY",
    "ALL_PROXY",
    "https_proxy",
    "http_proxy",
    "no_proxy",
    "all_proxy",
];

pub type EnvLookup = std::sync::Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;

/// The environment for one child: the allowlisted variables the supervisor
/// itself has, `HOME`, and the explicitly named extras.
pub fn child_env(
    lookup: &EnvLookup,
    home: &Path,
    extras: &[(String, String)],
) -> Vec<(String, String)> {
    let mut env: Vec<(String, String)> = PASSTHROUGH
        .iter()
        .filter_map(|name| lookup(name).map(|value| ((*name).to_string(), value)))
        .collect();
    env.push(("HOME".to_string(), home.to_string_lossy().into_owned()));
    env.extend(extras.iter().cloned());
    env
}

/// Spawns `binary` detached: its own process group (so a signal or exit of
/// the GUI never reaches it), no stdin, output appended to a private log.
/// The returned `Child` is for startup monitoring; hand it to [`reap`]
/// afterwards.
pub fn spawn_detached(
    binary: &Path,
    args: &[String],
    env: &[(String, String)],
    log_path: &Path,
) -> Result<Child, SupervisorError> {
    let log = open_private_log(log_path)?;
    let log_err = log.try_clone().map_err(|source| SupervisorError::Io {
        action: "duplicate log handle for",
        path: log_path.to_path_buf(),
        source,
    })?;
    Command::new(binary)
        .args(args)
        .env_clear()
        .envs(env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err))
        .process_group(0)
        .spawn()
        .map_err(|source| SupervisorError::Io {
            action: "start",
            path: binary.to_path_buf(),
            source,
        })
}

/// Waits for a detached child on a background thread so it never lingers as
/// a zombie while the GUI keeps running. Does not stop the child.
pub fn reap(mut child: Child) {
    std::thread::spawn(move || {
        let _ = child.wait();
    });
}

/// The last `max_lines` lines of a log, trimmed to a sane size, for error
/// messages. Never returns more than 4 KiB.
pub fn log_tail(path: &Path, max_lines: usize) -> String {
    let Ok(text) = std::fs::read_to_string(path) else {
        return "(no log output)".to_string();
    };
    let lines: Vec<&str> = text.lines().collect();
    let start = lines.len().saturating_sub(max_lines);
    let mut tail = lines[start..].join("\n");
    if tail.len() > 4096 {
        let mut cut = tail.len() - 4096;
        while !tail.is_char_boundary(cut) {
            cut += 1;
        }
        tail = format!("...{}", &tail[cut..]);
    }
    if tail.is_empty() {
        "(no log output)".to_string()
    } else {
        tail
    }
}

/// Sends SIGTERM to `pid`. Callers must have verified what the PID is
/// first (see `Supervisor::signal_if_ours`).
pub fn terminate(pid: u32) -> Result<(), SupervisorError> {
    use rustix::process::{Pid, Signal, kill_process};
    let target = i32::try_from(pid)
        .ok()
        .and_then(Pid::from_raw)
        .ok_or_else(|| SupervisorError::UnsafeToSignal {
            pid,
            reason: "not a valid process id".to_string(),
        })?;
    match kill_process(target, Signal::TERM) {
        Ok(()) => Ok(()),
        // Already gone: the goal is met.
        Err(error) if error == rustix::io::Errno::SRCH => Ok(()),
        Err(error) => Err(SupervisorError::Io {
            action: "signal",
            path: PathBuf::from(format!("pid {pid}")),
            source: std::io::Error::from(error),
        }),
    }
}

/// The command line of `pid`, via `ps`. `None` if it cannot be read.
pub fn command_line(pid: u32) -> Option<String> {
    let output = Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "command="])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (!text.is_empty()).then_some(text)
}

/// Finds an executable named `name`: next to the current executable first
/// (a `cargo build` or installed bundle), then on `PATH`.
pub fn locate_binary(
    name: &'static str,
    current_exe: Option<&Path>,
    path_var: Option<&str>,
) -> Result<PathBuf, SupervisorError> {
    let mut tried = Vec::new();
    if let Some(dir) = current_exe.and_then(Path::parent) {
        tried.push(dir.join(name));
    }
    if let Some(path_var) = path_var {
        tried.extend(std::env::split_paths(path_var).map(|dir| dir.join(name)));
    }
    tried
        .iter()
        .find(|candidate| candidate.is_file())
        .cloned()
        .ok_or(SupervisorError::BinaryNotFound {
            name,
            looked_for: tried,
        })
}

fn reserve_loopback_listener() -> std::io::Result<std::net::TcpListener> {
    std::net::TcpListener::bind("127.0.0.1:0")
}

/// A candidate loopback port, free at the instant it was reserved.
/// Releasing the listener does not guarantee that the port stays free:
/// the supervisor retries when the child loses the subsequent bind race.
pub fn free_loopback_port() -> std::io::Result<u16> {
    Ok(reserve_loopback_listener()?.local_addr()?.port())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn lookup(vars: &[(&str, &str)]) -> EnvLookup {
        let map: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Arc::new(move |name| map.get(name).cloned())
    }

    #[test]
    fn only_allowlisted_variables_reach_a_child_and_secrets_do_not() {
        let env = child_env(
            &lookup(&[
                ("PATH", "/usr/bin"),
                ("APEXES_CRED_BINANCE_MAIN_MAINNET_API_KEY", "REAL-KEY"),
                ("APEXES_CONTROL_TOKENS", "x:operator:y"),
                ("AWS_SECRET_ACCESS_KEY", "nope"),
                ("HOME", "/should/be/overridden"),
            ]),
            Path::new("/h"),
            &[("APEXES_GATEWAY_TOKEN".to_string(), "g".to_string())],
        );
        let names: Vec<&str> = env.iter().map(|(k, _)| k.as_str()).collect();
        assert!(names.contains(&"PATH"));
        assert!(names.contains(&"APEXES_GATEWAY_TOKEN"));
        assert!(
            !names.contains(&"APEXES_CRED_BINANCE_MAIN_MAINNET_API_KEY"),
            "{names:?}"
        );
        assert!(!names.contains(&"AWS_SECRET_ACCESS_KEY"), "{names:?}");
        assert!(!names.contains(&"APEXES_CONTROL_TOKENS"), "{names:?}");
        let homes: Vec<&str> = env
            .iter()
            .filter(|(k, _)| k == "HOME")
            .map(|(_, v)| v.as_str())
            .collect();
        assert_eq!(
            homes,
            ["/h"],
            "HOME comes from the supervisor, not the ambient environment"
        );
    }

    #[test]
    fn log_tail_keeps_the_last_lines_and_handles_missing_and_empty_logs() {
        let dir =
            std::env::temp_dir().join(format!("apexes-sup-tail-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let log = dir.join("a.log");
        std::fs::write(
            &log,
            (1..=30).map(|i| format!("line {i}\n")).collect::<String>(),
        )
        .unwrap();
        let tail = log_tail(&log, 3);
        assert_eq!(tail, "line 28\nline 29\nline 30");
        assert_eq!(log_tail(&dir.join("missing.log"), 3), "(no log output)");
        std::fs::write(&log, "").unwrap();
        assert_eq!(log_tail(&log, 3), "(no log output)");
        std::fs::write(&log, "é".repeat(5000)).unwrap();
        assert!(
            log_tail(&log, 3).len() <= 4096 + 3 + 4,
            "bounded, and cut on a char boundary"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn a_missing_binary_lists_everywhere_it_looked() {
        let error = locate_binary(
            "apexes-engine",
            Some(Path::new("/nowhere/bin/ui")),
            Some("/a:/b"),
        )
        .unwrap_err();
        match error {
            SupervisorError::BinaryNotFound { name, looked_for } => {
                assert_eq!(name, "apexes-engine");
                assert_eq!(looked_for.len(), 3);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_binary_next_to_the_current_executable_is_found_before_path() {
        let dir =
            std::env::temp_dir().join(format!("apexes-sup-bin-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("apexes-engine"), "x").unwrap();
        let found =
            locate_binary("apexes-engine", Some(&dir.join("ui")), Some("/elsewhere")).unwrap();
        assert_eq!(found, dir.join("apexes-engine"));
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn the_command_line_of_this_process_is_readable_and_a_dead_pid_is_not() {
        let line = command_line(std::process::id()).expect("ps works for our own pid");
        assert!(!line.is_empty());
        assert_eq!(command_line(u32::MAX - 1), None);
    }

    #[test]
    fn reserved_loopback_ports_are_usable_and_distinct_while_held() {
        let first = reserve_loopback_listener().unwrap();
        let second = reserve_loopback_listener().unwrap();
        let a = first.local_addr().unwrap();
        let b = second.local_addr().unwrap();
        assert!(a.ip().is_loopback() && b.ip().is_loopback());
        assert!(a.port() > 0 && b.port() > 0);
        assert_ne!(a.port(), b.port());
        // Verify usability without releasing the reservations.
        for (listener, address) in [(first, a), (second, b)] {
            let client =
                std::net::TcpStream::connect_timeout(&address, std::time::Duration::from_secs(2))
                    .unwrap();
            let (_, peer) = listener.accept().unwrap();
            assert_eq!(peer, client.local_addr().unwrap());
        }
    }

    #[test]
    fn candidate_port_does_not_conflict_with_held_listener() {
        let held = reserve_loopback_listener().unwrap();
        let candidate = free_loopback_port().unwrap();
        assert!(candidate > 0);
        assert_ne!(candidate, held.local_addr().unwrap().port());
        // No assertion about re-binding the candidate after release:
        // another process can legally take it. The gateway startup
        // integration test exercises recovery from that race.
    }
}
