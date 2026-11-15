//! Operator console state: everything the console decides, with no
//! rendering and no networking, so the safety rules are unit-testable.
//!
//! The rules (the point of this module):
//! - **Halt is one click.** Stopping trading must never sit behind a
//!   confirmation dialog.
//! - **Anything that adds risk or destroys state needs a second click:**
//!   resuming trading, halt-with-flatten (fires market orders), stopping the
//!   session.
//! - **A viewer cannot command**, and the console says so instead of
//!   pretending: no command is produced.
//! - **Nothing is sent while disconnected**, so a stale click can never be
//!   replayed against a reconnected engine.

use std::collections::BTreeMap;

use apexes_backtest::{SessionCommand, SessionSnapshot};
use apexes_engine::{EngineStatus, SessionMessage};
use rust_decimal::Decimal;

use crate::client::ConnectionStatus;

/// Something the operator asked for.
#[derive(Debug, Clone, PartialEq)]
pub enum OperatorAction {
    /// Stop new orders and cancel resting ones.
    Halt,
    /// Halt, then close open futures positions with market orders.
    HaltAndFlatten,
    /// Release the halt.
    ResumeTrading,
    /// Pause/resume strategy dispatch (does not touch orders).
    PauseStrategy,
    ResumeStrategy,
    /// Terminate the session.
    Stop,
    SetParam {
        name: String,
        value: Decimal,
    },
}

impl OperatorAction {
    /// Whether this needs an explicit confirmation before being sent.
    pub fn needs_confirmation(&self) -> bool {
        matches!(
            self,
            OperatorAction::HaltAndFlatten | OperatorAction::ResumeTrading | OperatorAction::Stop
        )
    }

    /// Operator-facing explanation shown while awaiting confirmation.
    pub fn warning(&self) -> &'static str {
        match self {
            OperatorAction::HaltAndFlatten => {
                "Halt trading, cancel all orders and CLOSE all open futures positions with market orders?"
            }
            OperatorAction::ResumeTrading => "Resume trading? New orders will be accepted again.",
            OperatorAction::Stop => "Stop this session?",
            _ => "",
        }
    }

    fn command(&self) -> SessionCommand {
        match self {
            OperatorAction::Halt => SessionCommand::Halt {
                reason: "operator console".to_string(),
                flatten: false,
            },
            OperatorAction::HaltAndFlatten => SessionCommand::Halt {
                reason: "operator console (flatten)".to_string(),
                flatten: true,
            },
            OperatorAction::ResumeTrading => SessionCommand::ResumeTrading,
            OperatorAction::PauseStrategy => SessionCommand::Pause,
            OperatorAction::ResumeStrategy => SessionCommand::Resume,
            OperatorAction::Stop => SessionCommand::Stop,
            OperatorAction::SetParam { name, value } => SessionCommand::SetParam {
                name: name.clone(),
                value: *value,
            },
        }
    }
}

/// Result of asking the state machine to perform an action.
#[derive(Debug, Clone, PartialEq)]
pub enum Requested {
    /// Send this command now.
    Send(SessionCommand),
    /// Waiting for the operator to confirm (see `pending`).
    NeedsConfirmation,
    /// Refused; the reason is also in `notice`.
    Refused(String),
}

#[derive(Debug, Default)]
pub struct OperatorState {
    pub connection: ConnectionStatus,
    pub snapshot: Option<SessionSnapshot>,
    pub status: Option<EngineStatus>,
    /// Latest refusal/engine error, shown until the next successful action.
    pub notice: Option<String>,
    /// An action awaiting the operator's confirmation.
    pub pending: Option<OperatorAction>,
    /// Text being typed into each parameter's edit box.
    pub param_edits: BTreeMap<String, String>,
}

impl OperatorState {
    pub fn set_connection(&mut self, connection: ConnectionStatus) {
        self.connection = connection;
        if connection != ConnectionStatus::Connected {
            // Whatever was awaiting confirmation belongs to a session we
            // can no longer vouch for; a stale status must not linger.
            self.pending = None;
            self.status = None;
        }
    }

    /// Folds one engine message into the state.
    pub fn apply(&mut self, message: SessionMessage) {
        match message {
            // The engine's "nothing pushed yet" value is not account state.
            SessionMessage::Snapshot(snapshot) if snapshot.is_placeholder() => {}
            SessionMessage::Snapshot(snapshot) => {
                // Keep half-typed edits for params that still exist.
                self.param_edits
                    .retain(|name, _| snapshot.params.contains_key(name));
                self.snapshot = Some(snapshot);
            }
            SessionMessage::Status(status) => self.status = Some(status),
            SessionMessage::Error { message } => self.notice = Some(message),
            SessionMessage::Command(_) => {} // never sent to clients
        }
    }

    pub fn connected(&self) -> bool {
        self.connection == ConnectionStatus::Connected
    }

    /// True only once the engine has said this connection is an operator.
    pub fn can_command(&self) -> bool {
        self.connected() && self.status.as_ref().is_some_and(|s| s.role == "operator")
    }

    pub fn halted(&self) -> bool {
        self.status.as_ref().is_some_and(|s| s.halted)
    }

    pub fn paused(&self) -> bool {
        self.snapshot.as_ref().is_some_and(|s| s.paused)
    }

    /// The operator asked for `action`.
    pub fn request(&mut self, action: OperatorAction) -> Requested {
        if !self.connected() {
            return self.refuse("not connected to the engine");
        }
        if !self.can_command() {
            return self.refuse("read-only: your token is a viewer");
        }
        if action.needs_confirmation() {
            self.pending = Some(action);
            return Requested::NeedsConfirmation;
        }
        self.pending = None;
        self.notice = None;
        Requested::Send(action.command())
    }

    /// The operator confirmed the pending action.
    pub fn confirm(&mut self) -> Option<SessionCommand> {
        let action = self.pending.take()?;
        // Re-check at the moment of confirmation: the connection or role may
        // have changed while the dialog was open.
        if !self.can_command() {
            self.notice = Some("not connected as an operator; nothing sent".to_string());
            return None;
        }
        self.notice = None;
        Some(action.command())
    }

    pub fn cancel(&mut self) {
        self.pending = None;
    }

    fn refuse(&mut self, reason: &str) -> Requested {
        self.pending = None;
        self.notice = Some(reason.to_string());
        Requested::Refused(reason.to_string())
    }

    /// Parses a parameter edit box into a `SetParam` request.
    pub fn request_set_param(&mut self, name: &str) -> Requested {
        let Some(text) = self.param_edits.get(name) else {
            return self.refuse("nothing typed for that parameter");
        };
        match text.trim().parse::<Decimal>() {
            Ok(value) => {
                let result = self.request(OperatorAction::SetParam {
                    name: name.to_string(),
                    value,
                });
                if matches!(result, Requested::Send(_)) {
                    self.param_edits.remove(name);
                }
                result
            }
            Err(_) => self.refuse(&format!("{text:?} is not a number")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    fn status(role: &str, halted: bool) -> EngineStatus {
        EngineStatus {
            actor: "alice".to_string(),
            role: role.to_string(),
            halted,
            halt_reason: halted.then(|| "why".to_string()),
            flatten_requested: false,
        }
    }

    fn snapshot(params: &[(&str, Decimal)], paused: bool) -> SessionSnapshot {
        SessionSnapshot {
            identity: None,
            evaluation: None,
            equity_history: Vec::new(),
            positions: HashMap::new(),
            balances: HashMap::new(),
            equity: dec!(100),
            realized_pnl: HashMap::new(),
            margin_status: HashMap::new(),
            params: params.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
            paused,
            open_orders: Vec::new(),
            timestamp: Utc::now(),
        }
    }

    fn operator() -> OperatorState {
        let mut s = OperatorState::default();
        s.set_connection(ConnectionStatus::Connected);
        s.apply(SessionMessage::Status(status("operator", false)));
        s
    }

    #[test]
    fn halt_is_one_click_but_risk_adding_actions_need_confirmation() {
        let mut s = operator();
        assert!(matches!(
            s.request(OperatorAction::Halt),
            Requested::Send(SessionCommand::Halt { flatten: false, .. })
        ));
        assert!(s.pending.is_none());
        assert!(matches!(
            s.request(OperatorAction::PauseStrategy),
            Requested::Send(SessionCommand::Pause)
        ));

        for action in [
            OperatorAction::HaltAndFlatten,
            OperatorAction::ResumeTrading,
            OperatorAction::Stop,
        ] {
            assert_eq!(s.request(action.clone()), Requested::NeedsConfirmation);
            assert_eq!(s.pending, Some(action.clone()));
            assert!(!action.warning().is_empty());
        }
    }

    #[test]
    fn confirming_sends_the_pending_command_once_and_cancel_discards_it() {
        let mut s = operator();
        s.request(OperatorAction::HaltAndFlatten);
        assert!(matches!(
            s.confirm(),
            Some(SessionCommand::Halt { flatten: true, .. })
        ));
        assert_eq!(s.confirm(), None, "a confirmation is single-use");

        s.request(OperatorAction::Stop);
        s.cancel();
        assert_eq!(s.confirm(), None);
        assert!(s.pending.is_none());
    }

    #[test]
    fn a_viewer_gets_no_commands_not_even_halt() {
        let mut s = OperatorState::default();
        s.set_connection(ConnectionStatus::Connected);
        s.apply(SessionMessage::Status(status("viewer", false)));
        assert!(!s.can_command());
        for action in [
            OperatorAction::Halt,
            OperatorAction::HaltAndFlatten,
            OperatorAction::Stop,
            OperatorAction::SetParam {
                name: "x".into(),
                value: dec!(1),
            },
        ] {
            assert!(matches!(s.request(action), Requested::Refused(_)));
            assert!(s.pending.is_none());
        }
        assert!(s.notice.as_deref().unwrap().contains("read-only"));
    }

    #[test]
    fn nothing_is_sent_before_the_engine_has_identified_the_connection_or_while_disconnected() {
        let mut s = OperatorState::default();
        assert!(matches!(
            s.request(OperatorAction::Halt),
            Requested::Refused(_)
        ));

        // Connected but no Status frame yet: role unknown => no commands.
        s.set_connection(ConnectionStatus::Connected);
        assert!(matches!(
            s.request(OperatorAction::Halt),
            Requested::Refused(_)
        ));

        s.apply(SessionMessage::Status(status("operator", false)));
        assert!(matches!(
            s.request(OperatorAction::Halt),
            Requested::Send(_)
        ));

        s.set_connection(ConnectionStatus::Reconnecting);
        assert!(
            s.status.is_none(),
            "stale status must not outlive the connection"
        );
        assert!(matches!(
            s.request(OperatorAction::Halt),
            Requested::Refused(_)
        ));
    }

    #[test]
    fn dropping_the_connection_cancels_a_pending_confirmation() {
        let mut s = operator();
        s.request(OperatorAction::Stop);
        s.set_connection(ConnectionStatus::Disconnected);
        assert!(s.pending.is_none());
        s.set_connection(ConnectionStatus::Connected);
        s.apply(SessionMessage::Status(status("operator", false)));
        assert_eq!(
            s.confirm(),
            None,
            "a dialog from before the drop must not fire"
        );
    }

    #[test]
    fn confirmation_rechecks_that_we_can_still_command() {
        let mut s = operator();
        s.request(OperatorAction::Stop);
        // Role downgraded while the dialog was open.
        s.apply(SessionMessage::Status(status("viewer", false)));
        assert_eq!(s.confirm(), None);
        assert!(s.notice.is_some());
    }

    #[test]
    fn engine_status_and_errors_are_surfaced() {
        let mut s = operator();
        assert!(!s.halted());
        s.apply(SessionMessage::Status(status("operator", true)));
        assert!(s.halted());
        s.apply(SessionMessage::Error {
            message: "refused".into(),
        });
        assert_eq!(s.notice.as_deref(), Some("refused"));
        // A successful action clears the stale notice.
        s.request(OperatorAction::PauseStrategy);
        assert_eq!(s.notice, None);
    }

    #[test]
    fn param_edits_parse_validate_and_clear_on_send() {
        let mut s = operator();
        s.apply(SessionMessage::Snapshot(snapshot(
            &[("threshold", dec!(5))],
            false,
        )));

        s.param_edits.insert("threshold".into(), "  7.5 ".into());
        assert_eq!(
            s.request_set_param("threshold"),
            Requested::Send(SessionCommand::SetParam {
                name: "threshold".into(),
                value: dec!(7.5)
            })
        );
        assert!(s.param_edits.is_empty(), "sent edit is cleared");

        s.param_edits.insert("threshold".into(), "abc".into());
        assert!(matches!(
            s.request_set_param("threshold"),
            Requested::Refused(_)
        ));
        assert!(
            s.param_edits.contains_key("threshold"),
            "bad input is kept for fixing"
        );
        assert!(matches!(
            s.request_set_param("missing"),
            Requested::Refused(_)
        ));
    }

    #[test]
    fn the_engines_placeholder_snapshot_is_never_shown_as_real_state() {
        let mut s = operator();
        let mut placeholder = snapshot(&[], false);
        placeholder.timestamp = chrono::DateTime::<Utc>::UNIX_EPOCH;
        s.apply(SessionMessage::Snapshot(placeholder));
        assert!(
            s.snapshot.is_none(),
            "an all-zero placeholder must not look like a flat account"
        );

        s.apply(SessionMessage::Snapshot(snapshot(&[], false)));
        assert!(s.snapshot.is_some());
    }

    #[test]
    fn snapshots_drop_edits_for_parameters_that_no_longer_exist() {
        let mut s = operator();
        s.param_edits.insert("gone".into(), "1".into());
        s.param_edits.insert("kept".into(), "2".into());
        s.apply(SessionMessage::Snapshot(snapshot(
            &[("kept", dec!(2))],
            false,
        )));
        assert!(s.param_edits.contains_key("kept") && !s.param_edits.contains_key("gone"));
        assert!(!s.paused());
        s.apply(SessionMessage::Snapshot(snapshot(&[], true)));
        assert!(s.paused());
    }
}
