//! `impl Account for apexes_backtest::PortfolioLedger` -- the backtest
//! environment's implementation. Deposit/withdrawal history and
//! internal transfers are meaningless for a backtest ledger (no real
//! wallets), and reconciliation is trivially satisfied (local state IS
//! the source of truth) -- see the behavior-differences table in
//! `docs/superpowers/specs/2026-08-02-apexes-account-design.md` §六.

use std::collections::HashMap;

use apexes_backtest::PortfolioLedger;
use apexes_core::{ApexesError, FundMovement, WalletId};
use apexes_strategy::Position;
use async_trait::async_trait;
use rust_decimal::Decimal;

use crate::account::Account;
use crate::fx_rate::FxRateProvider;
use crate::reconciliation::{ReconciliationDiscrepancy, ReconciliationTolerance};

#[async_trait]
impl Account for PortfolioLedger {
    fn balances(&self) -> HashMap<String, Decimal> {
        self.balances().clone()
    }

    fn positions(&self) -> HashMap<String, Position> {
        self.positions().clone()
    }

    fn realized_pnl(&self) -> HashMap<String, Decimal> {
        self.realized_pnl().clone()
    }

    fn equity(
        &self,
        mark_prices: &HashMap<String, Decimal>,
        _fx_provider: &dyn FxRateProvider,
        _valuation_currency: &str,
    ) -> Result<Decimal, ApexesError> {
        // A backtest ledger's balances are already assumed
        // single-market/single-quote-currency (its existing `equity`
        // contract) -- no FX fallback is exercised in practice today,
        // but the parameter is threaded through for interface
        // uniformity with `LiveAccount`, which DOES need it once a
        // second real market exists.
        PortfolioLedger::equity(self, mark_prices)
    }

    async fn deposit_history(&self) -> Result<Vec<FundMovement>, ApexesError> {
        Ok(Vec::new())
    }

    async fn withdrawal_history(&self) -> Result<Vec<FundMovement>, ApexesError> {
        Ok(Vec::new())
    }

    async fn internal_transfer(
        &self,
        _from_wallet: WalletId,
        _to_wallet: WalletId,
        _asset: &str,
        _amount: Decimal,
    ) -> Result<(), ApexesError> {
        Err(ApexesError::UnsupportedForEnvironment {
            reason: "backtest ledgers have no real wallets to transfer between".to_string(),
        })
    }

    async fn reconcile_against_exchange(
        &self,
        _tolerance: ReconciliationTolerance,
    ) -> Result<Vec<ReconciliationDiscrepancy>, ApexesError> {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use crate::account::Account;
    use crate::fx_rate::StaticFxRateProvider;
    use crate::reconciliation::ReconciliationTolerance;
    use apexes_backtest::PortfolioLedger;
    use apexes_core::{AssetClass, Instrument, MarketType, Venue};
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    fn btc_usdt() -> Instrument {
        Instrument::new(
            "BTCUSDT",
            MarketType::Crypto,
            Venue::Binance,
            AssetClass::Spot,
            Some("BTC".to_string()),
            Some("USDT".to_string()),
            dec!(0.01),
            dec!(0.0001),
            dec!(1),
        )
        .unwrap()
    }

    fn ledger_with_10k_usdt() -> PortfolioLedger {
        let mut instruments = HashMap::new();
        instruments.insert("BTCUSDT".to_string(), btc_usdt());
        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(10000));
        PortfolioLedger::new(instruments, initial_balances)
    }

    #[test]
    fn account_balances_delegates_to_ledger() {
        let ledger = ledger_with_10k_usdt();
        let balances = Account::balances(&ledger);
        assert_eq!(*balances.get("USDT").unwrap(), dec!(10000));
    }

    #[test]
    fn account_positions_delegates_to_ledger() {
        let ledger = ledger_with_10k_usdt();
        assert!(Account::positions(&ledger).is_empty());
    }

    #[test]
    fn account_realized_pnl_delegates_to_ledger() {
        let ledger = ledger_with_10k_usdt();
        assert!(Account::realized_pnl(&ledger).is_empty());
    }

    #[test]
    fn account_equity_matches_ledger_equity_with_no_fx_needed() {
        let ledger = ledger_with_10k_usdt();
        let fx = StaticFxRateProvider::new(HashMap::new());
        let mark_prices = HashMap::new();

        let equity = Account::equity(&ledger, &mark_prices, &fx, "USDT").unwrap();
        assert_eq!(equity, dec!(10000));
    }

    #[tokio::test]
    async fn deposit_and_withdrawal_history_are_empty_for_backtest() {
        let ledger = ledger_with_10k_usdt();
        assert!(Account::deposit_history(&ledger).await.unwrap().is_empty());
        assert!(
            Account::withdrawal_history(&ledger)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn internal_transfer_is_unsupported_for_backtest() {
        let ledger = ledger_with_10k_usdt();
        let result = Account::internal_transfer(
            &ledger,
            apexes_core::WalletId::Spot,
            apexes_core::WalletId::UsdsFutures,
            "USDT",
            dec!(100),
        )
        .await;
        assert!(matches!(
            result,
            Err(apexes_core::ApexesError::UnsupportedForEnvironment { .. })
        ));
    }

    #[tokio::test]
    async fn reconcile_against_exchange_is_trivially_empty_for_backtest() {
        let ledger = ledger_with_10k_usdt();
        let tolerance = ReconciliationTolerance {
            absolute: dec!(1),
            relative_fraction: dec!(0.01),
        };
        let discrepancies = Account::reconcile_against_exchange(&ledger, tolerance)
            .await
            .unwrap();
        assert!(discrepancies.is_empty());
    }
}
