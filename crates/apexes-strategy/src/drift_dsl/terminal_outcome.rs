//! Detached explicit terminal outcome; transit intents have no order ID.
use crate::{LiveTerminalStatus, TerminalKind, TerminalOutcome, TriggerTerminalReason};
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
        TerminalKind::Triggered { evidence } => (
            match evidence.reason {
                TriggerTerminalReason::IocRemainderExpired => "trigger_expired",
                TriggerTerminalReason::ConversionRejected => "trigger_rejected",
            },
            Some(evidence.order.id),
            Some(evidence.order.symbol.as_str()),
            Some(evidence.order.venue),
            Some(evidence.order.quantity),
            Some(evidence.order.filled_quantity),
        ),
        TerminalKind::Live { order, status, .. } => (
            match status {
                LiveTerminalStatus::Filled => "filled",
                LiveTerminalStatus::Cancelled => "cancelled",
                LiveTerminalStatus::Rejected => "rejected",
            },
            Some(order.id),
            Some(order.symbol.as_str()),
            Some(order.venue),
            Some(order.quantity),
            Some(order.filled_quantity),
        ),
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
    let mut fields: std::collections::HashMap<String, DriftValue> = [
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
    .collect();
    if let TerminalKind::Live {
        parent_id,
        dispatch_id,
        ..
    } = outcome.kind
    {
        fields.insert("source".into(), string("live"));
        fields.insert(
            "parent_id".into(),
            parent_id.map_or(DriftValue::Null, |id| string(&id.0.to_string())),
        );
        fields.insert(
            "dispatch_id".into(),
            dispatch_id.map_or(DriftValue::Null, |id| string(&id.to_string())),
        );
    }
    DriftValue::Object(fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, Order, OrderId, OrderStatus, OrderType, Side, Venue};
    use chrono::Utc;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    #[test]
    fn live_terminal_payload_keeps_exact_child_ids_and_actual_quantity() {
        let now = Utc::now();
        let order = Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: Some("exchange-id".into()),
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: dec!(3),
            price: Some(dec!(100)),
            stop_price: None,
            leverage: None,
            margin_mode: None,
            filled_quantity: dec!(1.5),
            status: OrderStatus::Cancelled,
            created_at: now,
            updated_at: now,
        };
        let parent_id = crate::ParentOrderId(Uuid::new_v4());
        let dispatch_id = Uuid::new_v4();
        let outcome = TerminalOutcome {
            intent_id: Some(crate::IntentId(u64::MAX)),
            at: now,
            kind: TerminalKind::Live {
                order: &order,
                status: LiveTerminalStatus::Cancelled,
                parent_id: Some(parent_id),
                dispatch_id: Some(dispatch_id),
            },
        };
        let DriftValue::Object(fields) = to_value(&outcome) else {
            panic!("expected object");
        };
        for (key, expected) in [
            ("kind", "cancelled".into()),
            ("source", "live".into()),
            ("order_id", order.id.0.to_string()),
            ("parent_id", parent_id.0.to_string()),
            ("dispatch_id", dispatch_id.to_string()),
            ("intent_id", u64::MAX.to_string()),
        ] {
            assert!(
                matches!(fields.get(key), Some(DriftValue::String(s)) if s.as_str() == expected)
            );
        }
        assert!(matches!(fields.get("filled_quantity"), Some(DriftValue::Number(n)) if *n == 1.5));
        let old = TerminalOutcome {
            kind: TerminalKind::Cancelled { order_id: order.id },
            ..outcome
        };
        let DriftValue::Object(fields) = to_value(&old) else {
            panic!("expected object");
        };
        assert!(!fields.contains_key("source"));
        assert!(matches!(
            fields.get("filled_quantity"),
            Some(DriftValue::Null)
        ));
    }
}
