//! F04.03 — Local simulated Spot capital after tracked order reservations.
//! This is neither an exchange free-balance query nor an admission decision.
use rust_decimal::Decimal;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapitalObservation {
    pub ledger_balance: Decimal,
    pub reserved: Decimal,
    pub unreserved: Decimal,
}

impl CapitalObservation {
    /// A shortfall is visible in ledger_balance versus reserved; never
    /// publish negative spendable capital or hide the outstanding hold.
    pub fn from_balances(ledger_balance: Decimal, reserved: Decimal) -> Option<Self> {
        if ledger_balance < Decimal::ZERO || reserved < Decimal::ZERO {
            return None;
        }
        Some(Self {
            ledger_balance,
            reserved,
            unreserved: ledger_balance.checked_sub(reserved)?.max(Decimal::ZERO),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn capital_keeps_shortfalls_visible_and_rejects_invalid_amounts() {
        let d = Decimal::from;
        let c = CapitalObservation::from_balances(d(100), d(40)).unwrap();
        assert_eq!(c.unreserved, d(60));
        let c = CapitalObservation::from_balances(d(10), d(20)).unwrap();
        assert_eq!(c.unreserved, Decimal::ZERO);
        assert_eq!(c.reserved, d(20));
        assert!(CapitalObservation::from_balances(d(-1), d(0)).is_none());
        assert!(CapitalObservation::from_balances(d(1), d(-1)).is_none());
    }
}
