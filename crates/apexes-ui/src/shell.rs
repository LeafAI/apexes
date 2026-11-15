//! The operator application: a session launcher and the operator console in
//! one window.
//!
//! Supervisor calls (launch, stop) can take many seconds, so they run on a
//! worker thread and report back over a channel; the GUI thread never
//! blocks. The state machine in [`ShellState`] decides what each click and
//! each result means and is tested without a window.

use std::sync::mpsc as std_mpsc;
use std::time::{Duration, Instant};

use apexes_supervisor::{LaunchRequest, LaunchedSession, SessionId, SessionInfo, Supervisor};

use crate::launcher::{LaunchStatus, LauncherState};
use crate::launcher_view::LauncherAction;

/// How often the session list is re-read from disk.
const REFRESH_EVERY: Duration = Duration::from_secs(3);
/// How long a stop waits for the engine to exit on its own.
pub const STOP_WAIT: Duration = Duration::from_secs(20);

#[derive(Debug)]
pub enum WorkerJob {
    Launch(Box<LaunchRequest>),
    Stop { session_id: String },
    Forget { session_id: String },
}

#[derive(Debug)]
pub enum WorkerResult {
    Launched(Box<LaunchedSession>),
    LaunchFailed(String),
    Stopped { session_id: String },
    StopFailed { session_id: String, message: String },
    Forgotten { session_id: String },
    ForgetFailed { session_id: String, message: String },
}

/// What the shell asks the outside world to do after a click.
#[derive(Debug, PartialEq)]
pub enum Effect {
    None,
    Job(JobKind),
    Connect(SessionInfo),
    RefreshSessions,
}

#[derive(Debug, PartialEq)]
pub enum JobKind {
    Launch(Box<LaunchRequestSummary>),
    Stop(String),
    Forget(String),
}

/// A comparable stand-in for a `LaunchRequest` in tests (the request itself
/// carries non-comparable parts).
#[derive(Debug, PartialEq, Eq)]
pub struct LaunchRequestSummary {
    pub network: apexes_core::binance_network::BinanceNetwork,
    pub symbols: Vec<String>,
}

#[derive(Default)]
pub struct ShellState {
    pub launcher: LauncherState,
    /// A Stop awaiting the operator's second click.
    pub pending_stop: Option<String>,
    /// The session whose console is open.
    pub connected: Option<String>,
}

impl ShellState {
    /// Applies the operator's click; returns what to do about it. A launch
    /// request is only produced when the form is valid and not already busy.
    pub fn handle(&mut self, action: LauncherAction) -> (Effect, Option<LaunchRequest>) {
        match action {
            LauncherAction::Launch => {
                if !self.launcher.can_launch() {
                    return (Effect::None, None);
                }
                match self.launcher.form.to_request() {
                    Ok(request) => {
                        self.launcher.status = LaunchStatus::Launching;
                        self.launcher.notice = None;
                        let summary = LaunchRequestSummary {
                            network: request.network,
                            symbols: request.symbols.clone(),
                        };
                        (
                            Effect::Job(JobKind::Launch(Box::new(summary))),
                            Some(request),
                        )
                    }
                    Err(_) => (Effect::None, None),
                }
            }
            LauncherAction::Refresh => (Effect::RefreshSessions, None),
            LauncherAction::Connect { session_id } => {
                let found = self
                    .launcher
                    .sessions
                    .iter()
                    .find(|s| s.id.as_str() == session_id && s.alive && s.token.is_some())
                    .cloned();
                match found {
                    Some(info) => {
                        self.connected = Some(session_id);
                        (Effect::Connect(info), None)
                    }
                    None => {
                        self.launcher.notice =
                            Some(format!("{session_id} is not running or has no saved token"));
                        (Effect::None, None)
                    }
                }
            }
            LauncherAction::RequestStop { session_id } => {
                self.pending_stop = Some(session_id);
                (Effect::None, None)
            }
            LauncherAction::CancelStop => {
                self.pending_stop = None;
                (Effect::None, None)
            }
            LauncherAction::ConfirmStop { session_id } => {
                // Only the session actually awaiting confirmation can be
                // stopped: a stale or forged action must not stop another.
                if self.pending_stop.as_deref() != Some(session_id.as_str()) {
                    return (Effect::None, None);
                }
                self.pending_stop = None;
                (Effect::Job(JobKind::Stop(session_id)), None)
            }
            LauncherAction::Forget { session_id } => {
                (Effect::Job(JobKind::Forget(session_id)), None)
            }
        }
    }

    pub fn apply_result(&mut self, result: WorkerResult) {
        match result {
            WorkerResult::Launched(session) => {
                self.launcher.status = LaunchStatus::Launched {
                    session_id: session.info.id.to_string(),
                };
                self.launcher.notice = None;
            }
            WorkerResult::LaunchFailed(message) => {
                self.launcher.status = LaunchStatus::Failed(message);
            }
            WorkerResult::Stopped { session_id } => {
                self.launcher.notice = Some(format!("Stopped {session_id}"));
                if self.connected.as_deref() == Some(session_id.as_str()) {
                    // The console stays up to show the final state; it will
                    // report the lost connection itself.
                }
            }
            WorkerResult::StopFailed {
                session_id,
                message,
            } => {
                self.launcher.notice = Some(format!("Could not stop {session_id}: {message}"));
            }
            WorkerResult::Forgotten { session_id } => {
                self.launcher.notice = Some(format!("Removed files of {session_id}"));
            }
            WorkerResult::ForgetFailed {
                session_id,
                message,
            } => {
                self.launcher.notice = Some(format!("Could not remove {session_id}: {message}"));
            }
        }
    }

    /// Replaces the session list and re-checks credentials for the selected
    /// network. A stale pending-stop for a session that is gone is dropped.
    pub fn apply_sessions(
        &mut self,
        sessions: Vec<SessionInfo>,
        missing_credential: Option<String>,
    ) {
        if let Some(pending) = &self.pending_stop
            && !sessions.iter().any(|s| s.id.as_str() == pending && s.alive)
        {
            self.pending_stop = None;
        }
        self.launcher.sessions = sessions;
        self.launcher.missing_credential = missing_credential;
    }
}

/// Runs supervisor jobs off the GUI thread.
pub struct Worker {
    jobs: std_mpsc::Sender<WorkerJob>,
    results: std_mpsc::Receiver<WorkerResult>,
}

impl Worker {
    pub fn spawn(supervisor: Supervisor) -> Self {
        let (job_tx, job_rx) = std_mpsc::channel::<WorkerJob>();
        let (result_tx, result_rx) = std_mpsc::channel::<WorkerResult>();
        std::thread::Builder::new()
            .name("apexes-supervisor-worker".to_string())
            .spawn(move || {
                while let Ok(job) = job_rx.recv() {
                    let result = run_job(&supervisor, job);
                    if result_tx.send(result).is_err() {
                        return;
                    }
                }
            })
            .ok();
        Self {
            jobs: job_tx,
            results: result_rx,
        }
    }

    pub fn submit(&self, job: WorkerJob) -> bool {
        self.jobs.send(job).is_ok()
    }

    pub fn try_results(&self) -> Vec<WorkerResult> {
        self.results.try_iter().collect()
    }
}

pub fn run_job(supervisor: &Supervisor, job: WorkerJob) -> WorkerResult {
    match job {
        WorkerJob::Launch(request) => match supervisor.launch(*request) {
            Ok(session) => WorkerResult::Launched(Box::new(session)),
            Err(error) => WorkerResult::LaunchFailed(error.to_string()),
        },
        WorkerJob::Stop { session_id } => {
            let outcome = SessionId::parse(&session_id)
                .map_err(|e| e.to_string())
                .and_then(|id| {
                    supervisor
                        .request_stop(&id, STOP_WAIT, false)
                        .map_err(|e| e.to_string())
                });
            match outcome {
                Ok(()) => WorkerResult::Stopped { session_id },
                Err(message) => WorkerResult::StopFailed {
                    session_id,
                    message,
                },
            }
        }
        WorkerJob::Forget { session_id } => {
            let outcome = SessionId::parse(&session_id)
                .map_err(|e| e.to_string())
                .and_then(|id| supervisor.forget(&id).map_err(|e| e.to_string()));
            match outcome {
                Ok(()) => WorkerResult::Forgotten { session_id },
                Err(message) => WorkerResult::ForgetFailed {
                    session_id,
                    message,
                },
            }
        }
    }
}

/// Reads the session list and the credential check the launcher needs.
pub fn read_sessions(
    supervisor: &Supervisor,
    state: &LauncherState,
) -> (Vec<SessionInfo>, Option<String>) {
    let missing =
        supervisor.missing_credential(state.form.credentials_ref.trim(), state.form.network);
    (supervisor.discover(), missing)
}

pub fn refresh_due(last: Option<Instant>) -> bool {
    last.is_none_or(|at| at.elapsed() >= REFRESH_EVERY)
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::binance_network::BinanceNetwork;

    fn info(id: &str, alive: bool, token: bool) -> SessionInfo {
        SessionInfo {
            id: SessionId::parse(id).unwrap(),
            pid: 1,
            port: 2,
            network: Some(BinanceNetwork::Testnet),
            alive,
            control_url: "ws://127.0.0.1:2/session/ws".to_string(),
            token: token.then(|| "t".repeat(32)),
            log_path: "/tmp/x.log".into(),
        }
    }

    #[test]
    fn launch_marks_busy_returns_the_request_and_cannot_be_double_clicked() {
        let mut shell = ShellState::default();
        let (effect, request) = shell.handle(LauncherAction::Launch);
        assert!(matches!(effect, Effect::Job(JobKind::Launch(_))));
        assert_eq!(request.unwrap().network, BinanceNetwork::Testnet);
        assert_eq!(shell.launcher.status, LaunchStatus::Launching);

        let (effect, request) = shell.handle(LauncherAction::Launch);
        assert_eq!(
            effect,
            Effect::None,
            "no second launch while one is running"
        );
        assert!(request.is_none());
    }

    #[test]
    fn launch_is_ignored_when_credentials_are_missing_or_the_form_is_invalid() {
        let mut shell = ShellState::default();
        shell.launcher.missing_credential = Some("X".to_string());
        assert_eq!(shell.handle(LauncherAction::Launch).0, Effect::None);
        assert_eq!(shell.launcher.status, LaunchStatus::Idle);

        let mut shell = ShellState::default();
        shell.launcher.form.max_order_quantity = "bad".to_string();
        assert_eq!(shell.handle(LauncherAction::Launch).0, Effect::None);
    }

    #[test]
    fn a_mainnet_launch_without_the_typed_word_never_reaches_the_worker() {
        let mut shell = ShellState::default();
        shell.launcher.form.set_network(BinanceNetwork::Mainnet);
        assert_eq!(shell.handle(LauncherAction::Launch).0, Effect::None);
        shell.launcher.form.mainnet_confirmation = "MAINNET".to_string();
        assert!(matches!(
            shell.handle(LauncherAction::Launch).0,
            Effect::Job(_)
        ));
    }

    #[test]
    fn results_update_the_status() {
        let mut shell = ShellState::default();
        shell.launcher.status = LaunchStatus::Launching;
        shell.apply_result(WorkerResult::LaunchFailed("boom".to_string()));
        assert_eq!(
            shell.launcher.status,
            LaunchStatus::Failed("boom".to_string())
        );
        assert!(
            !shell.launcher.busy(),
            "a failure frees the button for a retry"
        );
    }

    #[test]
    fn stop_needs_a_confirmation_for_exactly_that_session() {
        let mut shell = ShellState::default();
        shell.apply_sessions(vec![info("a", true, true), info("b", true, true)], None);

        assert_eq!(
            shell
                .handle(LauncherAction::ConfirmStop {
                    session_id: "a".into()
                })
                .0,
            Effect::None,
            "nothing was requested"
        );
        shell.handle(LauncherAction::RequestStop {
            session_id: "a".into(),
        });
        assert_eq!(shell.pending_stop.as_deref(), Some("a"));
        assert_eq!(
            shell
                .handle(LauncherAction::ConfirmStop {
                    session_id: "b".into()
                })
                .0,
            Effect::None,
            "confirming a different session must not stop it"
        );
        assert_eq!(
            shell
                .handle(LauncherAction::ConfirmStop {
                    session_id: "a".into()
                })
                .0,
            Effect::Job(JobKind::Stop("a".into()))
        );
        assert!(shell.pending_stop.is_none());
    }

    #[test]
    fn cancelling_a_stop_sends_nothing() {
        let mut shell = ShellState::default();
        shell.handle(LauncherAction::RequestStop {
            session_id: "a".into(),
        });
        shell.handle(LauncherAction::CancelStop);
        assert!(shell.pending_stop.is_none());
    }

    #[test]
    fn a_pending_stop_for_a_session_that_has_gone_is_dropped_on_refresh() {
        let mut shell = ShellState::default();
        shell.apply_sessions(vec![info("a", true, true)], None);
        shell.handle(LauncherAction::RequestStop {
            session_id: "a".into(),
        });
        shell.apply_sessions(vec![info("a", false, true)], None);
        assert!(shell.pending_stop.is_none());
    }

    #[test]
    fn connect_requires_a_running_session_with_a_saved_token() {
        let mut shell = ShellState::default();
        shell.apply_sessions(
            vec![
                info("ok", true, true),
                info("notoken", true, false),
                info("dead", false, true),
            ],
            None,
        );
        assert!(matches!(
            shell
                .handle(LauncherAction::Connect {
                    session_id: "ok".into()
                })
                .0,
            Effect::Connect(_)
        ));
        assert_eq!(shell.connected.as_deref(), Some("ok"));
        for id in ["notoken", "dead", "missing"] {
            let mut shell2 = ShellState::default();
            shell2.apply_sessions(
                vec![info("notoken", true, false), info("dead", false, true)],
                None,
            );
            assert_eq!(
                shell2
                    .handle(LauncherAction::Connect {
                        session_id: id.into()
                    })
                    .0,
                Effect::None,
                "{id}"
            );
            assert!(shell2.launcher.notice.is_some());
            assert!(shell2.connected.is_none());
        }
    }

    #[test]
    fn refresh_cadence() {
        assert!(refresh_due(None));
        assert!(!refresh_due(Some(Instant::now())));
        assert!(refresh_due(Some(
            Instant::now() - REFRESH_EVERY - Duration::from_millis(1)
        )));
    }

    #[test]
    fn failed_jobs_become_notices_not_silence() {
        let mut shell = ShellState::default();
        shell.apply_result(WorkerResult::StopFailed {
            session_id: "a".into(),
            message: "still running".into(),
        });
        assert!(
            shell
                .launcher
                .notice
                .as_deref()
                .unwrap()
                .contains("still running")
        );
        shell.apply_result(WorkerResult::ForgetFailed {
            session_id: "a".into(),
            message: "nope".into(),
        });
        assert!(shell.launcher.notice.as_deref().unwrap().contains("nope"));
    }
}

/// The `eframe` application: the launcher on one tab and the console of the
/// connected session on the other.
pub mod app {
    use super::*;
    use crate::client::ConnectionStatus;
    use crate::operator_client::{OperatorClientConfig, run};
    use crate::operator_view::OperatorApp;
    use tokio::sync::{mpsc, watch};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Tab {
        Launcher,
        Console,
    }

    pub struct ShellApp {
        supervisor: Supervisor,
        worker: Worker,
        runtime: tokio::runtime::Handle,
        shell: ShellState,
        console: Option<OperatorApp>,
        client_task: Option<tokio::task::JoinHandle<()>>,
        tab: Tab,
        last_refresh: Option<Instant>,
    }

    impl ShellApp {
        pub fn new(supervisor: Supervisor, runtime: tokio::runtime::Handle) -> Self {
            let worker = Worker::spawn(supervisor.clone());
            let mut shell = ShellState::default();
            shell.launcher.refresh_strategies();
            Self {
                supervisor,
                worker,
                runtime,
                shell,
                console: None,
                client_task: None,
                tab: Tab::Launcher,
                last_refresh: None,
            }
        }

        /// Opens a console for `info`, replacing any open one. The old
        /// client task is aborted so only one connection exists.
        fn connect(&mut self, info: &SessionInfo) {
            if let Some(task) = self.client_task.take() {
                task.abort();
            }
            let (event_tx, event_rx) = mpsc::unbounded_channel();
            let (status_tx, status_rx) = watch::channel(ConnectionStatus::default());
            let (command_tx, command_rx) = mpsc::unbounded_channel();
            let task = self.runtime.spawn(run(
                OperatorClientConfig {
                    url: info.control_url.clone(),
                    token: info.token.clone(),
                },
                event_tx,
                status_tx,
                command_rx,
            ));
            self.client_task = Some(task);
            self.console = Some(OperatorApp::new(
                info.control_url.clone(),
                event_rx,
                status_rx,
                command_tx,
            ));
            self.tab = Tab::Console;
        }

        fn refresh(&mut self) {
            let (sessions, missing) = read_sessions(&self.supervisor, &self.shell.launcher);
            self.shell.apply_sessions(sessions, missing);
            self.last_refresh = Some(Instant::now());
        }

        fn drain_worker(&mut self) {
            let results = self.worker.try_results();
            let changed = !results.is_empty();
            for result in results {
                self.shell.apply_result(result);
            }
            if changed {
                self.refresh();
            }
        }

        fn perform(&mut self, effect: Effect, request: Option<LaunchRequest>) {
            match effect {
                Effect::None => {}
                Effect::RefreshSessions => self.refresh(),
                Effect::Connect(info) => self.connect(&info),
                Effect::Job(JobKind::Launch(_)) => {
                    if let Some(request) = request
                        && !self.worker.submit(WorkerJob::Launch(Box::new(request)))
                    {
                        self.shell.apply_result(WorkerResult::LaunchFailed(
                            "the supervisor worker has stopped; restart the app".to_string(),
                        ));
                    }
                }
                Effect::Job(JobKind::Stop(session_id)) => {
                    self.worker.submit(WorkerJob::Stop { session_id });
                }
                Effect::Job(JobKind::Forget(session_id)) => {
                    self.worker.submit(WorkerJob::Forget { session_id });
                }
            }
        }
    }

    impl eframe::App for ShellApp {
        fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
            ui.ctx().request_repaint_after(Duration::from_millis(250));
            self.drain_worker();
            if refresh_due(self.last_refresh) {
                self.refresh();
            }

            ui.horizontal(|ui| {
                ui.selectable_value(&mut self.tab, Tab::Launcher, "Launcher");
                let label = match &self.shell.connected {
                    Some(id) => format!("Console: {id}"),
                    None => "Console".to_string(),
                };
                ui.add_enabled_ui(self.console.is_some(), |ui| {
                    ui.selectable_value(&mut self.tab, Tab::Console, label);
                });
            });
            ui.separator();

            match self.tab {
                Tab::Launcher => {
                    let pending = self.shell.pending_stop.clone();
                    let actions = crate::launcher_view::show(
                        ui,
                        &mut self.shell.launcher,
                        pending.as_deref(),
                    );
                    for action in actions {
                        let (effect, request) = self.shell.handle(action);
                        self.perform(effect, request);
                    }
                }
                Tab::Console => match &mut self.console {
                    Some(console) => console.render(ui),
                    None => {
                        ui.weak("Open a session from the launcher.");
                    }
                },
            }
        }
    }
}
