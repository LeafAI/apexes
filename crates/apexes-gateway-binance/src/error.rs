//! Internal error/response shapes for parsing Binance REST responses.
//! Not `apexes_oms::OmsError` -- this crate's job is to translate these
//! into `apexes_oms::GatewayOutcome`/`Result<_, String>` at the
//! `ExecutionGateway` trait boundary (see `gateway.rs`), matching every
//! other `ExecutionGateway` method's `String`-typed error channel.

use serde::Deserialize;

/// Binance's documented error-response body shape:
/// `{"code": -2010, "msg": "Account has insufficient balance..."}`.
/// A negative `code` with a `msg` is Binance's standard way of
/// reporting an explicit business-logic rejection (as opposed to a
/// transport-level failure, which never reaches this parsed form at
/// all -- see `gateway.rs`'s `GatewayOutcome` mapping).
#[derive(Debug, Clone, Deserialize)]
pub struct BinanceApiError {
    pub code: i64,
    pub msg: String,
}
