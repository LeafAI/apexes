//! Detached fill payload. No submission capability or inferred order status.
use crate::FillOutcome;
use apexes_core::{MarginMode, Side};
use drift_script::DriftValue;
use rust_decimal::Decimal;
use std::sync::Arc;

fn string(s: &str) -> DriftValue {
    DriftValue::String(Arc::new(s.to_owned()))
}
fn number(n: Decimal) -> DriftValue {
    f64::try_from(n)
        .ok()
        .map_or(DriftValue::Null, DriftValue::Number)
}
pub(super) fn to_value(outcome: &FillOutcome<'_>) -> DriftValue {
    let f = outcome.fill;
    DriftValue::Object(
        [
            ("source", string(outcome.source.as_str())),
            ("at", string(&outcome.at.to_rfc3339())),
            (
                "intent_id",
                outcome
                    .intent_id
                    .map_or(DriftValue::Null, |id| string(&id.0.to_string())),
            ),
            ("filled_at", string(&f.filled_at.to_rfc3339())),
            ("fill_id", string(&f.id.0.to_string())),
            ("order_id", string(&f.order_id.0.to_string())),
            ("symbol", string(&f.symbol)),
            ("venue", string(super::convert::venue_to_str(f.venue))),
            (
                "side",
                string(match f.side {
                    Side::Buy => "buy",
                    Side::Sell => "sell",
                }),
            ),
            ("price", number(f.price)),
            ("quantity", number(f.quantity)),
            ("fee", number(f.fee)),
            (
                "fee_currency",
                f.fee_currency.as_deref().map_or(DriftValue::Null, string),
            ),
            ("leverage", f.leverage.map_or(DriftValue::Null, number)),
            (
                "margin_mode",
                f.margin_mode.map_or(DriftValue::Null, |m| {
                    string(match m {
                        MarginMode::Isolated => "isolated",
                        MarginMode::Cross => "cross",
                    })
                }),
            ),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v))
        .collect(),
    )
}
