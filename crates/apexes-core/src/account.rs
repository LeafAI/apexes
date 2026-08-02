//! Shared account/fund-movement types used by `apexes-gateway-binance`
//! (real exchange API method signatures) and `apexes-account` (the
//! `Account` trait and its implementations). Defined here, not in
//! either of those two crates, to avoid a circular dependency:
//! `apexes-gateway-binance`'s new real-API methods need these types in
//! their signatures, and `apexes-account::LiveAccount` depends on
//! `apexes-gateway-binance` -- see
//! `docs/superpowers/specs/2026-08-02-apexes-account-design.md` §四
//! for the full analysis.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// A wallet/sub-account category on an exchange that supports internal
/// transfers between them (e.g. Binance Spot vs. USDS-M Futures).
/// backtest/paper environments have no concept of separate wallets --
/// `apexes_account::Account::internal_transfer` returns
/// `ApexesError::UnsupportedForEnvironment` there instead of using this
/// type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WalletId {
    Spot,
    UsdsFutures,
}

/// A single deposit or withdrawal record as reported by an exchange.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundMovement {
    pub asset: String,
    pub amount: Decimal,
    pub status: FundMovementStatus,
    pub timestamp: DateTime<Utc>,
    /// The exchange's own transaction identifier -- used by callers to
    /// deduplicate across repeated history queries, the same pattern
    /// `apexes_oms::ExchangeFill::exchange_trade_id` uses for fills.
    pub exchange_tx_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FundMovementStatus {
    Pending,
    Completed,
    Failed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn wallet_id_variants_are_distinct() {
        assert_ne!(WalletId::Spot, WalletId::UsdsFutures);
    }

    #[test]
    fn fund_movement_is_constructible_and_comparable() {
        let a = FundMovement {
            asset: "USDT".to_string(),
            amount: dec!(100),
            status: FundMovementStatus::Completed,
            timestamp: Utc::now(),
            exchange_tx_id: "tx-1".to_string(),
        };
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn fund_movement_status_variants_are_distinct() {
        assert_ne!(FundMovementStatus::Pending, FundMovementStatus::Completed);
        assert_ne!(FundMovementStatus::Completed, FundMovementStatus::Failed);
    }
}
