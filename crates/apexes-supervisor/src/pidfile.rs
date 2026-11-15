//! The `<pid>\n<port>\n` PID-file format shared by the gateway and engine,
//! and the liveness checks built on it.
//!
//! A PID file alone is not proof a process is running: after a reboot the
//! recorded PID may belong to something unrelated. "Alive" here therefore
//! means the PID exists *and* the recorded port accepts connections.

use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::time::Duration;

use rustix::process::{Pid, test_kill_process};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PidFile {
    pub pid: u32,
    pub port: u16,
}

pub fn parse(text: &str) -> Option<PidFile> {
    let mut lines = text.lines();
    let pid = lines.next()?.trim().parse::<u32>().ok()?;
    let port = lines.next()?.trim().parse::<u16>().ok()?;
    if lines.any(|extra| !extra.trim().is_empty()) || pid == 0 {
        return None;
    }
    Some(PidFile { pid, port })
}

/// `None` when the file is missing or malformed.
pub fn read(path: &Path) -> Option<PidFile> {
    parse(&std::fs::read_to_string(path).ok()?)
}

/// True if a process with this PID exists (a process we may not signal still
/// counts: it is there).
pub fn pid_exists(pid: u32) -> bool {
    let Some(pid) = i32::try_from(pid).ok().and_then(Pid::from_raw) else {
        return false;
    };
    match test_kill_process(pid) {
        Ok(()) => true,
        Err(error) => error == rustix::io::Errno::PERM,
    }
}

/// True if something accepts TCP connections on loopback at `port`.
pub fn port_open(port: u16) -> bool {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    TcpStream::connect_timeout(&addr, Duration::from_millis(300)).is_ok()
}

/// PID exists and its port answers.
pub fn is_serving(file: PidFile) -> bool {
    pid_exists(file.pid) && port_open(file.port)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    #[test]
    fn parses_the_two_line_format() {
        assert_eq!(
            parse("123\n9741\n"),
            Some(PidFile {
                pid: 123,
                port: 9741
            })
        );
        assert_eq!(
            parse("123\n9741"),
            Some(PidFile {
                pid: 123,
                port: 9741
            })
        );
    }

    #[test]
    fn rejects_anything_else() {
        for bad in [
            "",
            "123\n",
            "abc\n9741\n",
            "123\nabc\n",
            "0\n9741\n",
            "123\n9741\nextra\n",
            "123\n70000\n",
            "-1\n80\n",
        ] {
            assert_eq!(parse(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn this_process_exists_and_an_absurd_pid_does_not() {
        assert!(pid_exists(std::process::id()));
        assert!(!pid_exists(u32::MAX), "out of range for pid_t");
        assert!(!pid_exists(0));
    }

    /// A port nothing is listening on, without the race of "bind, drop, hope
    /// nobody else takes it": while this guard is alive the port is bound but
    /// never `listen`ed on... which still refuses connections, and, unlike a
    /// released port, cannot be taken by a parallel test or another process.
    fn reserved_closed_port() -> (std::net::TcpStream, u16) {
        use std::net::TcpStream;
        // Connect a client to a short-lived listener, then close the LISTENER:
        // the client's ephemeral port is held by this process and nothing is
        // accepting on it.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).unwrap();
        let held = client.local_addr().unwrap().port();
        drop(listener);
        (client, held)
    }

    #[test]
    fn port_open_reflects_a_real_listener() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        assert!(port_open(port));
        drop(listener);
        let (_guard, closed) = reserved_closed_port();
        assert!(!port_open(closed), "nothing accepts on a reserved port");
    }

    #[test]
    fn serving_needs_both_a_live_pid_and_an_open_port() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let live = PidFile {
            pid: std::process::id(),
            port,
        };
        assert!(is_serving(live));
        assert!(
            !is_serving(PidFile {
                pid: u32::MAX,
                port
            }),
            "dead pid, open port"
        );
        drop(listener);
        let (_guard, closed) = reserved_closed_port();
        assert!(
            !is_serving(PidFile {
                pid: std::process::id(),
                port: closed
            }),
            "live pid, closed port"
        );
    }
}
