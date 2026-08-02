//! [`Account`]: unified read/write interface over balances, positions,
//! realized PnL, equity, fund-movement history, internal transfers,
//! and reconciliation -- implemented identically across backtest
//! (`apexes_backtest::PortfolioLedger`), paper (`apexes_paper::PaperAccount`),
//! and live (`apexes_account::LiveAccount`) environments. See
//! `docs/superpowers/specs/2026-08-02-apexes-account-design.md` §3.1.

use std::collections::HashMap;

use apexes_core::{ApexesError, FundMovement, WalletId};
use apexes_strategy::Position;
use async_trait::async_trait;
use rust_decimal::Decimal;

use crate::fx_rate::FxRateProvider;
use crate::reconciliation::{ReconciliationDiscrepancy, ReconciliationTolerance};

#[async_trait]
pub trait Account: Send + Sync {
    /// Per-asset balance snapshot.
    fn balances(&self) -> HashMap<String, Decimal>;

    /// Per-symbol current net position snapshot.
    fn positions(&self) -> HashMap<String, Position>;

    /// Per-symbol accumulated realized profit/loss.
    fn realized_pnl(&self) -> HashMap<String, Decimal>;

    /// Total equity, converted to `valuation_currency` using
    /// `mark_prices` (same convention as
    /// `apexes_backtest::PortfolioLedger::equity`) with `fx_provider`
    /// as a fallback for assets not present in `mark_prices`.
    fn equity(
        &self,
        mark_prices: &HashMap<String, Decimal>,
        fx_provider: &dyn FxRateProvider,
        valuation_currency: &str,
    ) -> Result<Decimal, ApexesError>;

    /// Read-only deposit history. Never initiates a deposit.
    async fn deposit_history(&self) -> Result<Vec<FundMovement>, ApexesError>;

    /// Read-only withdrawal history. Never initiates a withdrawal --
    /// this crate has no withdrawal-initiation code path anywhere.
    async fn withdrawal_history(&self) -> Result<Vec<FundMovement>, ApexesError>;

    /// Real wallet-to-wallet transfer within the same account (e.g.
    /// Binance Spot <-> USDS-M Futures). backtest/paper environments
    /// return `Err(ApexesError::UnsupportedForEnvironment)`.
    async fn internal_transfer(
        &self,
        from_wallet: WalletId,
        to_wallet: WalletId,
        asset: &str,
        amount: Decimal,
    ) -> Result<(), ApexesError>;

    /// Compares real exchange balances/positions against local state,
    /// returning discrepancies exceeding `tolerance`. NEVER mutates
    /// local state -- alert-only. backtest/paper environments return
    /// an empty `Vec` (local state IS the source of truth there, so
    /// reconciliation is trivially satisfied, not an error).
    async fn reconcile_against_exchange(
        &self,
        tolerance: ReconciliationTolerance,
    ) -> Result<Vec<ReconciliationDiscrepancy>, ApexesError>;
}
