//! Apexes account & fund management: a unified `Account` trait across
//! backtest, paper, and live-trading environments, real Binance
//! balance/position/transfer/deposit-withdrawal-history integration,
//! cross-currency valuation (`FxRateProvider`), multi-account
//! aggregation (`AccountRegistry`), and alert-only reconciliation
//! against the real exchange. Covers `docs/FEATURES.md` rows 7.1/7.2/7.3.

pub mod account;
pub mod fx_rate;
pub mod reconciliation;

pub use account::Account;
pub use apexes_core::{FundMovement, FundMovementStatus, WalletId};
pub use fx_rate::FxRateProvider;
pub use reconciliation::{ReconciliationDiscrepancy, ReconciliationTolerance};
