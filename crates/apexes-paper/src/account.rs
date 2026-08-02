//! [`PaperAccount`]: a thin read-only query layer over `PortfolioLedger`,
//! for future monitoring/UI (sub-project #13) consumers that need to
//! inspect current equity/positions without holding a mutable reference
//! to the ledger the event loop itself owns.

use apexes_backtest::PortfolioLedger;
use apexes_core::ApexesError;
use apexes_strategy::Position;
use rust_decimal::Decimal;
use std::collections::HashMap;

/// A point-in-time snapshot of account state.
#[derive(Debug, Clone, PartialEq)]
pub struct AccountSnapshot {
    pub balances: HashMap<String, Decimal>,
    pub positions: HashMap<String, Position>,
    pub equity: Decimal,
}

pub struct PaperAccount<'a> {
    ledger: &'a PortfolioLedger,
}

impl<'a> PaperAccount<'a> {
    pub fn new(ledger: &'a PortfolioLedger) -> Self {
        Self { ledger }
    }

    pub fn balances_snapshot(&self) -> HashMap<String, Decimal> {
        self.ledger.balances().clone()
    }

    pub fn positions_snapshot(&self) -> HashMap<String, Position> {
        self.ledger.positions().clone()
    }

    pub fn realized_pnl_snapshot(&self) -> HashMap<String, Decimal> {
        self.ledger.realized_pnl().clone()
    }

    /// Computes current equity using `mark_prices` (asset symbol -> price
    /// in quote terms), the same convention as
    /// `PortfolioLedger::equity`.
    pub fn equity_snapshot(
        &self,
        mark_prices: &HashMap<String, Decimal>,
    ) -> Result<Decimal, ApexesError> {
        self.ledger.equity(mark_prices)
    }

    /// Convenience: all three snapshots in one call, for callers that
    /// want a single consistent point-in-time view (all three fields
    /// read from the same `&PortfolioLedger` borrow, so no other
    /// mutation can happen between them).
    pub fn full_snapshot(
        &self,
        mark_prices: &HashMap<String, Decimal>,
    ) -> Result<AccountSnapshot, ApexesError> {
        Ok(AccountSnapshot {
            balances: self.balances_snapshot(),
            positions: self.positions_snapshot(),
            equity: self.equity_snapshot(mark_prices)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::Instrument;
    use rust_decimal_macros::dec;

    fn ledger_with_usdt_balance(amount: Decimal) -> PortfolioLedger {
        let instruments: HashMap<String, Instrument> = HashMap::new();
        let mut balances = HashMap::new();
        balances.insert("USDT".to_string(), amount);
        PortfolioLedger::new(instruments, balances)
    }

    #[test]
    fn balances_snapshot_matches_ledger_balances() {
        let ledger = ledger_with_usdt_balance(dec!(1000));
        let account = PaperAccount::new(&ledger);
        let snapshot = account.balances_snapshot();
        assert_eq!(snapshot.get("USDT"), Some(&dec!(1000)));
    }

    #[test]
    fn positions_snapshot_is_empty_for_a_fresh_ledger() {
        let ledger = ledger_with_usdt_balance(dec!(1000));
        let account = PaperAccount::new(&ledger);
        assert!(account.positions_snapshot().is_empty());
    }

    #[test]
    fn realized_pnl_snapshot_matches_ledger_realized_pnl() {
        let ledger = ledger_with_usdt_balance(dec!(1000));
        let account = PaperAccount::new(&ledger);
        assert_eq!(
            account.realized_pnl_snapshot(),
            ledger.realized_pnl().clone()
        );
    }

    #[test]
    fn equity_snapshot_matches_ledger_equity_for_a_simple_balance() {
        let ledger = ledger_with_usdt_balance(dec!(1000));
        let account = PaperAccount::new(&ledger);
        let mark_prices = HashMap::new();
        let equity = account.equity_snapshot(&mark_prices).unwrap();
        assert_eq!(equity, dec!(1000));
    }

    #[test]
    fn full_snapshot_combines_all_three_fields_consistently() {
        let ledger = ledger_with_usdt_balance(dec!(500));
        let account = PaperAccount::new(&ledger);
        let mark_prices = HashMap::new();
        let snapshot = account.full_snapshot(&mark_prices).unwrap();
        assert_eq!(snapshot.balances.get("USDT"), Some(&dec!(500)));
        assert!(snapshot.positions.is_empty());
        assert_eq!(snapshot.equity, dec!(500));
    }
}
