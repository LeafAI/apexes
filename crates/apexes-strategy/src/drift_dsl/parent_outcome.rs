//! Detached parent evidence. Never exposes arbitrary EMS error strings.
use crate::{ParentOutcome, ParentOutcomeKind};
use drift_script::DriftValue;
use std::sync::Arc;

fn string(s: &str) -> DriftValue {
    DriftValue::String(Arc::new(s.to_owned()))
}

pub(super) fn to_value(outcome: &ParentOutcome<'_>) -> DriftValue {
    let (kind, id, intent, previous, current) = match outcome.kind {
        ParentOutcomeKind::Submitted { id, intent } => {
            ("submitted", Some(id), Some(intent), None, None)
        }
        ParentOutcomeKind::SubmissionFailed { intent } => {
            ("submission_failed", None, Some(intent), None, None)
        }
        ParentOutcomeKind::StatusChanged {
            id,
            previous,
            current,
        } => (
            "status_changed",
            Some(id),
            None,
            Some(previous),
            Some(current),
        ),
        ParentOutcomeKind::Unavailable { id } => ("unavailable", Some(id), None, None, None),
    };
    DriftValue::Object(
        [
            ("kind", string(kind)),
            ("at", string(&outcome.at.to_rfc3339())),
            (
                "parent_id",
                id.map_or(DriftValue::Null, |id| string(&id.0.to_string())),
            ),
            (
                "symbol",
                intent.map_or(DriftValue::Null, |i| string(&i.symbol)),
            ),
            (
                "venue",
                intent.map_or(DriftValue::Null, |i| {
                    string(super::convert::venue_to_str(i.venue))
                }),
            ),
            (
                "quantity",
                intent
                    .and_then(|i| f64::try_from(i.quantity).ok())
                    .map_or(DriftValue::Null, DriftValue::Number),
            ),
            (
                "previous",
                previous.map_or(DriftValue::Null, |p| string(p.as_str())),
            ),
            (
                "status",
                current.map_or(DriftValue::Null, |p| string(p.as_str())),
            ),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v))
        .collect(),
    )
}
