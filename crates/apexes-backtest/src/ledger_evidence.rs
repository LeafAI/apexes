//! F04.05: bounded read-only evidence of actual loop-local ledger state.
//! Partial retained attribution evidence, not a restore format.
use apexes_core::Venue;
use apexes_strategy::Position;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::observation_receipt::ObservationError;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LedgerEvidence {
    pub version: u32,
    pub scope: Vec<(Venue, String)>,
    pub balances: Vec<(String, Decimal)>,
    /// Includes actual flat entries retained by the ledger.
    pub positions: Vec<Position>,
    pub realized_pnl: Vec<(String, Decimal)>,
    /// Absent on legacy captures. Retained windows are not complete history.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fee_valuation: Option<crate::fee_evidence::FeeValuationEvidence>,
}

fn valid_key(s: &str, limit: usize) -> bool {
    !s.is_empty() && s.len() <= limit && !s.chars().any(char::is_control)
}

impl LedgerEvidence {
    pub fn validate(&self, scope: &[(Venue, String)]) -> Result<(), ObservationError> {
        use ObservationError as E;
        if self.version != 1
            || self.scope.is_empty()
            || self.scope.len() > 128
            || self.balances.len() > 1024
            || self.positions.len() > 128
            || self.realized_pnl.len() > 128
        {
            return Err(E::Capacity);
        }
        let mut expected = scope.to_vec();
        expected.sort_by(|a, b| a.1.cmp(&b.1));
        if self.scope != expected
            || !self.scope.windows(2).all(|w| w[0].1 < w[1].1)
            || self.scope.iter().any(|(_, s)| !valid_key(s, 128))
        {
            return Err(E::Scope);
        }
        let known = |s: &str| {
            self.scope
                .binary_search_by(|(_, symbol)| symbol.as_str().cmp(s))
                .is_ok()
        };
        if !self.balances.windows(2).all(|w| w[0].0 < w[1].0)
            || self
                .balances
                .iter()
                .any(|(a, amount)| !valid_key(a, 128) || *amount < Decimal::ZERO)
            || !self.positions.windows(2).all(|w| w[0].symbol < w[1].symbol)
            || self.positions.iter().any(|p| {
                !known(&p.symbol)
                    || p.quantity < Decimal::ZERO
                    || p.average_entry_price < Decimal::ZERO
                    || (!p.quantity.is_zero() && p.average_entry_price.is_zero())
                    || p.leverage.is_some()
                    || p.margin_mode.is_some()
                    || !p.allocated_margin.is_zero()
            })
            || !self.realized_pnl.windows(2).all(|w| w[0].0 < w[1].0)
            || self.realized_pnl.iter().any(|(s, _)| !known(s))
        {
            return Err(E::Encoding);
        }
        if let Some(fees) = &self.fee_valuation {
            fees.validate(scope)?;
        }
        Ok(())
    }
}
