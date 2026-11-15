//! [`SessionMessage`]: the message envelope carried over
//! `GET /session/ws` (design spec
//! `2026-08-05-apexes-13c-strategy-monitoring-tuning-design.md` §6.2)
//! -- distinguishes the two directions a bidirectional session
//! connection needs, unlike `apexes-market-data-gateway`'s read-only
//! `/market` endpoint (which sends bare, un-enveloped
//! `apexes_core::MarketEvent` JSON frames):
//!
//! - Engine -> UI: [`SessionMessage::Snapshot`], one per
//!   [`apexes_backtest::SessionSnapshot`] push from the trading loop's
//!   `ControlHandle` (throttled to `snapshot_min_interval`).
//! - UI -> Engine: [`SessionMessage::Command`], one per
//!   [`apexes_backtest::SessionCommand`] a connected client sends
//!   (`SetParam`/`Pause`/`Resume`/`Stop`).
//!
//! The `#[serde(tag = "type")]` representation means each JSON frame
//! looks like `{"type": "Snapshot", ...fields...}` or `{"type":
//! "Command", ...fields...}` -- a UI client's deserializer can
//! dispatch on `"type"` without needing to try both variants.

use apexes_backtest::{SessionCommand, SessionSnapshot};
use serde::{Deserialize, Serialize};

// `SessionSnapshot` is meaningfully larger than `SessionCommand` (it
// holds several `HashMap`s), so clippy's `large_enum_variant` flags
// this enum. Not boxed deliberately: this is the exact wire-protocol
// shape from the design spec (§6.2) that `apexes-ui`'s future client
// code will match against, and a `SessionSnapshot` is only ever
// constructed/cloned at most once per `snapshot_min_interval` tick
// (already throttled by `ControlHandle::maybe_push_snapshot`) -- not
// a hot path where the extra enum size matters.
#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum SessionMessage {
    Snapshot(SessionSnapshot),
    Command(SessionCommand),
    /// Engine -> client: who you are, what you may do, and the engine's
    /// halt state. Sent on connect and whenever it changes.
    Status(EngineStatus),
    /// Engine -> client: a command was refused (e.g. a viewer sent one).
    Error {
        message: String,
    },
}

/// Connection-level state the trading loop's `SessionSnapshot` does not
/// carry (the kill switch lives outside the event loop on purpose).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct EngineStatus {
    /// Authenticated name of THIS connection (`unauthenticated` when the
    /// engine runs without tokens).
    pub actor: String,
    /// `"operator"` may send commands; `"viewer"` is read-only.
    pub role: String,
    pub halted: bool,
    pub halt_reason: Option<String>,
    /// A halt asked for positions to be closed.
    pub flatten_requested: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    fn empty_snapshot() -> SessionSnapshot {
        SessionSnapshot {
            identity: None,
            evaluation: None,
            equity_history: Vec::new(),
            positions: HashMap::new(),
            balances: HashMap::new(),
            equity: Decimal::ZERO,
            realized_pnl: HashMap::new(),
            margin_status: HashMap::new(),
            params: HashMap::new(),
            paused: false,
            open_orders: Vec::new(),
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn snapshot_round_trips_through_json_with_type_tag() {
        let message = SessionMessage::Snapshot(empty_snapshot());
        let json = serde_json::to_string(&message).unwrap();
        assert!(json.contains("\"type\":\"Snapshot\""));

        let deserialized: SessionMessage = serde_json::from_str(&json).unwrap();
        match deserialized {
            SessionMessage::Snapshot(snapshot) => assert_eq!(snapshot.equity, Decimal::ZERO),
            other => panic!("expected Snapshot, got {other:?}"),
        }
    }

    #[test]
    fn command_round_trips_through_json_with_type_tag() {
        let message = SessionMessage::Command(SessionCommand::SetParam {
            name: "threshold".to_string(),
            value: dec!(42),
        });
        let json = serde_json::to_string(&message).unwrap();
        assert!(json.contains("\"type\":\"Command\""));

        let deserialized: SessionMessage = serde_json::from_str(&json).unwrap();
        match deserialized {
            SessionMessage::Command(SessionCommand::SetParam { name, value }) => {
                assert_eq!(name, "threshold");
                assert_eq!(value, dec!(42));
            }
            _ => panic!("expected Command(SetParam)"),
        }
    }

    #[test]
    fn status_and_error_frames_have_stable_wire_shapes() {
        let status = SessionMessage::Status(EngineStatus {
            actor: "alice".to_string(),
            role: "operator".to_string(),
            halted: true,
            halt_reason: Some("why".to_string()),
            flatten_requested: false,
        });
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("\"type\":\"Status\""), "{json}");
        assert!(json.contains("\"actor\":\"alice\"") && json.contains("\"halted\":true"));
        let back: SessionMessage = serde_json::from_str(&json).unwrap();
        assert!(
            matches!(back, SessionMessage::Status(s) if s.halt_reason.as_deref() == Some("why"))
        );

        let error = r#"{"type":"Error","message":"nope"}"#;
        assert!(matches!(
            serde_json::from_str::<SessionMessage>(error).unwrap(),
            SessionMessage::Error { message } if message == "nope"
        ));
    }
}
