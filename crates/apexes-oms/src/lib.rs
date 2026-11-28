//! Apexes order management system (OMS): gateway-agnostic order
//! lifecycle management (submission with pre-trade risk checks,
//! cancellation, persistence, and reconnect reconciliation). Covers
//! `docs/FEATURES.md` rows 5.1.1 (order generation & pre-trade
//! validation) and 5.1.3 (persistence & reconnect state
//! recovery/reconciliation). Row 5.1.2 (order state machine) is
//! unchanged from sub-project #0's `apexes_core::OrderStatus`.
//!
//! [`ExecutionGateway`] decouples order submission/cancellation/query
//! from any specific exchange -- sub-project #8 (first live gateway)
//! implements it against a real exchange without needing to modify this
//! crate. This crate ships only [`mock_gateway::MockExecutionGateway`]
//! for testing.

pub mod error;
pub mod exchange_update;
pub mod fee_journal;
pub mod gateway;
pub use fee_journal::{FeeConversion, FeeJournal};
pub mod kill_switch;
pub mod manager;
pub mod mock_gateway;
pub mod schema;
pub mod store;

pub use error::OmsError;
pub use exchange_update::ExchangeUpdate;
pub use gateway::{
    ExchangeFill, ExecutionGateway, GatewayOutcome, OrderExecutionSnapshot, SpotCommission,
    SpotPreflight,
};
pub use kill_switch::{KillSwitch, SessionPhase};
pub use manager::{CancelAllReport, FillDeliveryProgress, OrderManager, TerminalDeliveryProgress};
pub use mock_gateway::MockExecutionGateway;
pub use store::{CheckpointStoreError, OrderStore, SubmissionState, TerminalEvidence};

pub mod identity_evidence;
pub use identity_evidence::{
    ChildLink, FillIdentity, IdentityEvidence, IntentLink, RemoteIdentity,
};
