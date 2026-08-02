//! [`FxRateProvider`]: pluggable cross-currency rate lookup for
//! `Account::equity`. See
//! `docs/superpowers/specs/2026-08-02-apexes-account-design.md` §3.3
//! for why this is synchronous, not `async`.

use apexes_core::ApexesError;
use rust_decimal::Decimal;

pub trait FxRateProvider: Send + Sync {
    fn fx_rate(&self, from_currency: &str, to_currency: &str) -> Result<Decimal, ApexesError>;
}
