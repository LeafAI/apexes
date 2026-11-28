//! Apexes live-trading event loop (sub-project #8): exchange-agnostic
//! orchestration driving `Strategy` -> `OrderManager` against live
//! market data and gateway push/poll updates. Structurally parallel to
//! `apexes-paper::run_paper_trading`, but sources order state from a
//! real `apexes_oms::ExecutionGateway`/`OrderManager` instead of a
//! local `apexes_backtest::MatchingEngine` -- fills are real exchange
//! executions, not simulated ones.
//!
//! This crate depends on `apexes-backtest` only for
//! `apexes_backtest::PortfolioLedger` (a generic "rebuild positions/
//! balances from a stream of `Fill`s" component with no
//! backtest-specific logic) -- reused here as `run_live_trading`'s
//! temporary position/balance tracking, pending sub-project #11
//! (account & fund management), which will supersede it.

pub mod coherent_recovery;
pub mod flatten;
pub mod halt;
pub mod order_executor_adapter;
pub mod recovery_evidence_window;
mod recovery_fee;
pub mod recovery_ledger;
pub mod recovery_observation;
pub mod recovery_risk;
pub mod recovery_wallet;
pub mod runner;
pub mod spot_recovery;
pub mod startup_recovery;
pub mod strategy_recovery;

pub use flatten::{FlattenConfig, FlattenReport, FlattenSetup, PositionSource, flatten_positions};
pub use order_executor_adapter::{
    LiveLiquidationPolicy, LiveSupplementalFillSource, OrderManagerExecutor,
};
pub use runner::{
    LiveTradingConfig, LiveTradingReport, run_live_trading,
    run_live_trading_with_checkpoint_on_stop, run_live_trading_with_observation_capture_on_stop,
    run_live_trading_with_startup,
};
pub use spot_recovery::{SpotRecoveryError, SpotWalletBalance, recover_spot_positions};
pub use startup_recovery::{
    ExchangeSnapshot, RecoveredState, RecoveryError, RecoveryOrders, recover_live_state,
};
pub use strategy_recovery::{RecoverySelection, StrategyRecoveryError, StrategyStartup};

pub use coherent_recovery::{
    CoherentInspectionError, CoherentRecoveryPreview, CoherentRecoverySelection,
};

pub use recovery_evidence_window::{
    EvidenceWindow, EvidenceWindowError, OpenOrderCoverage, RecoveryEvidenceWindow,
};

pub use recovery_wallet::{WalletEvidence, WalletEvidenceError};

pub use recovery_ledger::{
    ClosingPositionEvidence, ClosingSpotPosition, LedgerContinuityError, OpeningWalletEvidence,
};

pub use recovery_risk::{RiskBoundaryEvidence, RiskEvidenceError};

pub use recovery_observation::{ObservationBoundaryEvidence, ObservationEvidenceError};
