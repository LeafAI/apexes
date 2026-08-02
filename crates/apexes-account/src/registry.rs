//! [`AccountRegistry`]: holds multiple named `Arc<dyn Account>`
//! instances, supporting lookup by name and cross-account aggregate
//! queries (e.g. total equity). Deliberately NOT built on
//! `apexes_core::registry::Registry` (that locator is keyed by Rust
//! type + a business key, for "the one gateway"/"one provider per
//! venue" use cases; here the natural key is a human-readable account
//! name, a different enough shape that reusing it would be awkward --
//! see `docs/superpowers/specs/2026-08-02-apexes-account-design.md`
//! §3.4).

use std::collections::HashMap;
use std::sync::Arc;

use apexes_core::ApexesError;
use rust_decimal::Decimal;

use crate::account::Account;
use crate::fx_rate::FxRateProvider;

pub struct AccountRegistry {
    accounts: HashMap<String, Arc<dyn Account>>,
}

impl Default for AccountRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl AccountRegistry {
    pub fn new() -> Self {
        Self {
            accounts: HashMap::new(),
        }
    }

    pub fn register(&mut self, name: impl Into<String>, account: Arc<dyn Account>) {
        self.accounts.insert(name.into(), account);
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn Account>> {
        self.accounts.get(name).cloned()
    }

    /// Sums `equity()` across every registered account using the same
    /// `mark_prices`/`fx_provider`/`valuation_currency`. Any single
    /// account's `equity()` failing fails the WHOLE aggregate (never
    /// silently skips a failed account) -- a partial total-equity
    /// figure that silently omits one account is worse than no figure
    /// at all for a number this safety-relevant.
    pub fn total_equity_across_all(
        &self,
        mark_prices: &HashMap<String, Decimal>,
        fx_provider: &dyn FxRateProvider,
        valuation_currency: &str,
    ) -> Result<Decimal, ApexesError> {
        let mut total = Decimal::ZERO;
        for account in self.accounts.values() {
            total += account.equity(mark_prices, fx_provider, valuation_currency)?;
        }
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use crate::account::Account;
    use crate::fx_rate::StaticFxRateProvider;
    use crate::reconciliation::{ReconciliationDiscrepancy, ReconciliationTolerance};
    use crate::registry::AccountRegistry;
    use apexes_core::{ApexesError, FundMovement, WalletId};
    use apexes_strategy::Position;
    use async_trait::async_trait;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;
    use std::sync::Arc;

    struct FixedEquityAccount {
        equity: Decimal,
        fails: bool,
    }

    #[async_trait]
    impl Account for FixedEquityAccount {
        fn balances(&self) -> HashMap<String, Decimal> {
            HashMap::new()
        }
        fn positions(&self) -> HashMap<String, Position> {
            HashMap::new()
        }
        fn realized_pnl(&self) -> HashMap<String, Decimal> {
            HashMap::new()
        }
        fn equity(
            &self,
            _mark_prices: &HashMap<String, Decimal>,
            _fx_provider: &dyn crate::fx_rate::FxRateProvider,
            _valuation_currency: &str,
        ) -> Result<Decimal, ApexesError> {
            if self.fails {
                Err(ApexesError::InvalidBacktestConfig {
                    reason: "simulated failure".to_string(),
                })
            } else {
                Ok(self.equity)
            }
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
            Ok(())
        }
        async fn reconcile_against_exchange(
            &self,
            _tolerance: ReconciliationTolerance,
        ) -> Result<Vec<ReconciliationDiscrepancy>, ApexesError> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn register_and_get_by_name() {
        let mut registry = AccountRegistry::new();
        let account: Arc<dyn Account> = Arc::new(FixedEquityAccount {
            equity: dec!(100),
            fails: false,
        });
        registry.register("main", account);

        assert!(registry.get("main").is_some());
        assert!(registry.get("missing").is_none());
    }

    #[test]
    fn total_equity_across_all_sums_every_account() {
        let mut registry = AccountRegistry::new();
        registry.register(
            "a",
            Arc::new(FixedEquityAccount {
                equity: dec!(100),
                fails: false,
            }) as Arc<dyn Account>,
        );
        registry.register(
            "b",
            Arc::new(FixedEquityAccount {
                equity: dec!(250),
                fails: false,
            }) as Arc<dyn Account>,
        );

        let fx = StaticFxRateProvider::new(HashMap::new());
        let total = registry
            .total_equity_across_all(&HashMap::new(), &fx, "USDT")
            .unwrap();
        assert_eq!(total, dec!(350));
    }

    #[test]
    fn total_equity_across_all_fails_if_any_account_fails() {
        let mut registry = AccountRegistry::new();
        registry.register(
            "a",
            Arc::new(FixedEquityAccount {
                equity: dec!(100),
                fails: false,
            }) as Arc<dyn Account>,
        );
        registry.register(
            "b",
            Arc::new(FixedEquityAccount {
                equity: Decimal::ZERO,
                fails: true,
            }) as Arc<dyn Account>,
        );

        let fx = StaticFxRateProvider::new(HashMap::new());
        let result = registry.total_equity_across_all(&HashMap::new(), &fx, "USDT");
        assert!(result.is_err());
    }
}
