//! F04.01: event-bound, transactional data-only state bindings.
use crate::symbol_state::SymbolState;
use apexes_core::MarketEvent;
use drift_script::{DriftValue, NativeFn};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

const MAX_SYMBOLS: usize = 128;
const MAX_KEYS: usize = 32;
const MAX_NAME_BYTES: usize = 64;
const MAX_STRING_BYTES: usize = 1024;
const MAX_ARRAY_LEN: usize = 512;

type Values = HashMap<String, DriftValue>;

pub(super) struct ScriptState(SymbolState<Values>);

impl Default for ScriptState {
    fn default() -> Self {
        Self(SymbolState::new(MAX_SYMBOLS))
    }
}

#[derive(Default)]
struct Stage {
    values: Values,
    dirty: bool,
    invalid: bool,
    closed: bool,
    writable: bool,
}

pub(super) struct CallbackState(Arc<Mutex<Stage>>);

fn valid_value(value: &DriftValue) -> bool {
    match value {
        DriftValue::Number(n) => n.is_finite(),
        DriftValue::Bool(_) | DriftValue::Null => true,
        DriftValue::String(s) => s.len() <= MAX_STRING_BYTES,
        DriftValue::Array(a) => {
            a.len() <= MAX_ARRAY_LEN
                && a.iter()
                    .all(|v| matches!(v, DriftValue::Number(n) if n.is_finite()))
        }
        _ => false,
    }
}

fn name(value: Option<&DriftValue>) -> Option<&str> {
    match value {
        Some(DriftValue::String(s)) if !s.is_empty() && s.len() <= MAX_NAME_BYTES => {
            Some(s.as_str())
        }
        _ => None,
    }
}

impl ScriptState {
    pub(super) fn begin(&self, event: &MarketEvent) -> CallbackState {
        CallbackState(Arc::new(Mutex::new(Stage {
            values: self.0.get(event).cloned().unwrap_or_default(),
            writable: event.symbol().len() <= MAX_NAME_BYTES
                && !event.symbol().is_empty()
                && self.0.can_insert(event),
            ..Default::default()
        })))
    }

    pub(super) fn finish(
        &mut self,
        event: &MarketEvent,
        stage: CallbackState,
        valid_return: bool,
    ) -> bool {
        let Ok(mut stage) = stage.0.lock() else {
            return false;
        };
        stage.closed = true;
        let values = std::mem::take(&mut stage.values);
        if !valid_return || stage.invalid {
            return false;
        }
        if stage.dirty && self.0.insert(event, values).is_err() {
            return false;
        }
        true
    }
}

impl CallbackState {
    pub(super) fn attach(&self, ctx: &mut DriftValue) {
        if let DriftValue::Object(obj) = ctx {
            obj.insert(
                "state_get".into(),
                DriftValue::NativeFunction(self.get_fn()),
            );
            obj.insert(
                "state_set".into(),
                DriftValue::NativeFunction(self.set_fn()),
            );
        }
    }

    fn get_fn(&self) -> NativeFn {
        let stage = Arc::clone(&self.0);
        Arc::new(move |args| {
            let stage = Arc::clone(&stage);
            Box::pin(async move {
                let Ok(mut s) = stage.lock() else {
                    return DriftValue::Undefined;
                };
                if s.closed {
                    return DriftValue::Undefined;
                }
                match (name(args.first()), args.get(1)) {
                    (Some(key), Some(default)) if args.len() == 2 && valid_value(default) => s
                        .values
                        .get(key)
                        .cloned()
                        .unwrap_or_else(|| default.clone()),
                    _ => {
                        s.invalid = true;
                        DriftValue::Undefined
                    }
                }
            })
        })
    }

    fn set_fn(&self) -> NativeFn {
        let stage = Arc::clone(&self.0);
        Arc::new(move |args| {
            let stage = Arc::clone(&stage);
            Box::pin(async move {
                let Ok(mut s) = stage.lock() else {
                    return DriftValue::Bool(false);
                };
                if s.closed {
                    return DriftValue::Bool(false);
                }
                match (name(args.first()), args.get(1)) {
                    (Some(key), Some(value))
                        if args.len() == 2
                            && s.writable
                            && valid_value(value)
                            && (s.values.contains_key(key) || s.values.len() < MAX_KEYS) =>
                    {
                        s.values.insert(key.to_owned(), value.clone());
                        s.dirty = true;
                        DriftValue::Bool(true)
                    }
                    _ => {
                        s.invalid = true;
                        DriftValue::Bool(false)
                    }
                }
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{TradeEvent, Venue};
    use chrono::Utc;
    use rust_decimal::Decimal;

    fn event(symbol: &str) -> MarketEvent {
        MarketEvent::Trade(TradeEvent {
            symbol: symbol.into(),
            venue: Venue::Binance,
            price: Decimal::ONE,
            quantity: Decimal::ONE,
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        })
    }
    fn string(s: &str) -> DriftValue {
        DriftValue::String(Arc::new(s.into()))
    }
    fn set(stage: &CallbackState, key: &str, value: DriftValue) -> DriftValue {
        futures::executor::block_on(stage.set_fn()(vec![string(key), value]))
    }
    fn get(stage: &CallbackState, key: &str) -> DriftValue {
        futures::executor::block_on(stage.get_fn()(vec![string(key), DriftValue::Number(0.0)]))
    }

    #[test]
    fn stale_callbacks_are_closed_and_invalid_operations_roll_back() {
        let mut state = ScriptState::default();
        let btc = event("BTCUSDT");
        let stage = state.begin(&btc);
        let stale = stage.set_fn();
        assert!(matches!(
            set(&stage, "count", DriftValue::Number(1.0)),
            DriftValue::Bool(true)
        ));
        assert!(state.finish(&btc, stage, true));
        assert!(matches!(
            futures::executor::block_on(stale(vec![string("count"), DriftValue::Number(99.0)])),
            DriftValue::Bool(false)
        ));
        let stage = state.begin(&btc);
        assert!(matches!(get(&stage, "count"), DriftValue::Number(1.0)));
        assert!(matches!(
            set(&stage, "count", DriftValue::Number(2.0)),
            DriftValue::Bool(true)
        ));
        assert!(matches!(
            set(&stage, "bad", DriftValue::Number(f64::NAN)),
            DriftValue::Bool(false)
        ));
        assert!(!state.finish(&btc, stage, true));
        let stage = state.begin(&btc);
        assert!(matches!(get(&stage, "count"), DriftValue::Number(1.0)));
        assert!(state.finish(&btc, stage, true));
    }

    #[test]
    fn capacity_and_value_limits_refuse_without_eviction() {
        let mut state = ScriptState::default();
        for i in 0..MAX_SYMBOLS {
            let event = event(&format!("S{i}"));
            let stage = state.begin(&event);
            assert!(matches!(
                set(&stage, "count", DriftValue::Number(i as f64)),
                DriftValue::Bool(true)
            ));
            assert!(state.finish(&event, stage, true));
        }
        let extra = event("extra");
        let stage = state.begin(&extra);
        assert!(matches!(
            set(&stage, "count", DriftValue::Number(1.0)),
            DriftValue::Bool(false)
        ));
        assert!(!state.finish(&extra, stage, true));
        let existing = event("S1");
        let stage = state.begin(&existing);
        assert!(matches!(get(&stage, "count"), DriftValue::Number(1.0)));
        for i in 1..MAX_KEYS {
            assert!(matches!(
                set(&stage, &format!("key{i}"), DriftValue::Number(1.0)),
                DriftValue::Bool(true)
            ));
        }
        assert!(state.finish(&existing, stage, true));
        let stage = state.begin(&existing);
        assert!(matches!(
            set(&stage, "extra", DriftValue::Number(1.0)),
            DriftValue::Bool(false)
        ));
        assert!(!state.finish(&existing, stage, true));
        let stage = state.begin(&existing);
        assert!(matches!(
            set(
                &stage,
                "count",
                DriftValue::Array(vec![DriftValue::Number(1.0); MAX_ARRAY_LEN + 1])
            ),
            DriftValue::Bool(false)
        ));
        assert!(!state.finish(&existing, stage, true));
    }
}
