//! Detached explicit terminal outcome; transit intents have no order ID.
use crate::{TerminalKind, TerminalOutcome};
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

pub(super) fn to_value(outcome: &TerminalOutcome<'_>) -> DriftValue {
    let (kind, id, symbol, venue, quantity, filled) = match outcome.kind {
        TerminalKind::Filled { order } => (
            "filled",
            Some(order.id),
            Some(order.symbol.as_str()),
            Some(order.venue),
            Some(order.quantity),
            Some(order.filled_quantity),
        ),
        TerminalKind::Cancelled { order_id } => {
            ("cancelled", Some(order_id), None, None, None, None)
        }
        TerminalKind::TransitCancelled { intent } => (
            "transit_cancelled",
            None,
            Some(intent.symbol.as_str()),
            Some(intent.venue),
            Some(intent.quantity),
            None,
        ),
        TerminalKind::TransitExpired { intent } => (
            "transit_expired",
            None,
            Some(intent.symbol.as_str()),
            Some(intent.venue),
            Some(intent.quantity),
            None,
        ),
    };
    DriftValue::Object(
        [
            ("kind", string(kind)),
            ("at", string(&outcome.at.to_rfc3339())),
            (
                "intent_id",
                outcome
                    .intent_id
                    .map_or(DriftValue::Null, |id| string(&id.0.to_string())),
            ),
            (
                "order_id",
                id.map_or(DriftValue::Null, |id| string(&id.0.to_string())),
            ),
            ("symbol", symbol.map_or(DriftValue::Null, string)),
            (
                "venue",
                venue.map_or(DriftValue::Null, |v| {
                    string(super::convert::venue_to_str(v))
                }),
            ),
            ("quantity", quantity.map_or(DriftValue::Null, number)),
            ("filled_quantity", filled.map_or(DriftValue::Null, number)),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v))
        .collect(),
    )
}
