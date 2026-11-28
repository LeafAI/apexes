//! Detached notification payload: no context capabilities, exchange IDs,
//! or untrusted error strings. Callback return values are ignored.
use crate::{AttemptStage, OrderOutcome, OrderOutcomeKind};
use apexes_core::{OrderStatus, Side};
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

pub(super) fn to_value(outcome: &OrderOutcome<'_>) -> DriftValue {
    let (kind, code, order) = match outcome.kind {
        OrderOutcomeKind::Rejected { code } => ("rejected", Some(code), None),
        OrderOutcomeKind::Scheduled => ("scheduled", None, None),
        OrderOutcomeKind::Returned { order } => ("returned", None, Some(order)),
        OrderOutcomeKind::SubmissionFailed { code } => ("submission_failed", Some(code), None),
    };
    let stage = match outcome.stage {
        AttemptStage::Decision => "decision",
        AttemptStage::Arrival => "arrival",
    };
    let status = order.map(|order| match order.status {
        OrderStatus::New => "new",
        OrderStatus::PartiallyFilled => "partially_filled",
        OrderStatus::Filled => "filled",
        OrderStatus::Cancelled => "cancelled",
        OrderStatus::Rejected => "rejected",
    });
    let intent = outcome.intent;
    DriftValue::Object(
        [
            ("kind", string(kind)),
            ("code", code.map_or(DriftValue::Null, string)),
            ("stage", string(stage)),
            ("at", string(&outcome.at.to_rfc3339())),
            (
                "intent_id",
                outcome
                    .intent_id
                    .map_or(DriftValue::Null, |id| string(&id.0.to_string())),
            ),
            ("symbol", string(&intent.symbol)),
            ("venue", string(super::convert::venue_to_str(intent.venue))),
            (
                "side",
                string(match intent.side {
                    Side::Buy => "buy",
                    Side::Sell => "sell",
                }),
            ),
            ("quantity", number(intent.quantity)),
            ("price", intent.price.map_or(DriftValue::Null, number)),
            (
                "order_id",
                order.map_or(DriftValue::Null, |order| string(&order.id.0.to_string())),
            ),
            ("order_status", status.map_or(DriftValue::Null, string)),
            (
                "order_quantity",
                order.map_or(DriftValue::Null, |order| number(order.quantity)),
            ),
            (
                "filled_quantity",
                order.map_or(DriftValue::Null, |order| number(order.filled_quantity)),
            ),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect(),
    )
}
