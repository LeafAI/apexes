//! `impl Account for apexes_paper::PaperAccount` -- delegates to the
//! `&PortfolioLedger` it wraps, exactly like `PaperAccount`'s own
//! existing `balances_snapshot`/`positions_snapshot`/`equity_snapshot`
//! methods already do.

use std::collections::HashMap;

use apexes_core::{ApexesError, FundMovement, WalletId};
use apexes_paper::PaperAccount;
use apexes_strategy::Position;
use async_trait::async_trait;
use rust_decimal::Decimal;

use crate::account::Account;
use crate::fx_rate::FxRateProvider;
use crate::reconciliation::{ReconciliationDiscrepancy, ReconciliationTolerance};

#[async_trait]
impl Account for PaperAccount<'_> {
    fn balances(&self) -> HashMap<String, Decimal> {
        self.balances_snapshot()
    }

    fn positions(&self) -> HashMap<String, Position> {
        self.positions_snapshot()
    }

    fn realized_pnl(&self) -> HashMap<String, Decimal> {
        // PaperAccount has no existing realized_pnl_snapshot method --
        // add one now, mirroring balances_snapshot/positions_snapshot's
        // existing delegation pattern (see crates/apexes-paper/src/account.rs).
        self.realized_pnl_snapshot()
    }

    fn equity(
        &self,
        mark_prices: &HashMap<String, Decimal>,
        _fx_provider: &dyn FxRateProvider,
        _valuation_currency: &str,
    ) -> Result<Decimal, ApexesError> {
        self.equity_snapshot(mark_prices)
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
            reason: "paper trading accounts have no real wallets to transfer between".to_string(),
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
    use apexes_backtest::PortfolioLedger;
    use apexes_core::{AssetClass, Instrument, MarketType, Venue};
    use apexes_paper::PaperAccount;
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
    fn paper_account_balances_matches_underlying_ledger() {
        let ledger = ledger_with_10k_usdt();
        let paper_account = PaperAccount::new(&ledger);

        let balances = Account::balances(&paper_account);
        assert_eq!(*balances.get("USDT").unwrap(), dec!(10000));
    }

    #[test]
    fn paper_account_equity_matches_underlying_ledger() {
        let ledger = ledger_with_10k_usdt();
        let paper_account = PaperAccount::new(&ledger);
        let fx = StaticFxRateProvider::new(HashMap::new());
        let mark_prices = HashMap::new();

        let equity = Account::equity(&paper_account, &mark_prices, &fx, "USDT").unwrap();
        assert_eq!(equity, dec!(10000));
    }

    #[tokio::test]
    async fn paper_account_internal_transfer_is_unsupported() {
        let ledger = ledger_with_10k_usdt();
        let paper_account = PaperAccount::new(&ledger);

        let result = Account::internal_transfer(
            &paper_account,
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
}
