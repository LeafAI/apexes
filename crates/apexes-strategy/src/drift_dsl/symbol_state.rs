//! F04.01: event-bound, transactional data and native indicator state bindings.
use super::indicators::error_object;
use super::scoped_indicators::{Instance, Kind, MAX_INDICATORS};
use crate::event_time::{MAX_COOLDOWN_MS, TimeGateError, TimeObservation, TimeState};
use crate::symbol_state::SymbolState;
use apexes_core::MarketEvent;
use chrono::{DateTime, Utc};
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

#[derive(Clone, Default)]
struct InstrumentState {
    values: Values,
    indicators: HashMap<String, Instance>,
    time: TimeState,
}

pub(super) struct ScriptState(SymbolState<InstrumentState>);

impl Default for ScriptState {
    fn default() -> Self {
        Self(SymbolState::new(MAX_SYMBOLS))
    }
}

#[derive(Default)]
struct Stage {
    state: InstrumentState,
    dirty: bool,
    invalid: bool,
    closed: bool,
    writable: bool,
}

pub(super) struct CallbackState(Arc<Mutex<Stage>>);

#[derive(Clone, Copy)]
enum TimeOperation {
    Cooldown,
    Bar,
}

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
            state: self.0.get(event).cloned().unwrap_or_default(),
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
        let values = std::mem::take(&mut stage.state);
        if !valid_return || stage.invalid {
            return false;
        }
        if stage.dirty && self.0.insert(event, values).is_err() {
            return false;
        }
        true
    }
}

impl Stage {
    fn apply_time(
        &mut self,
        observation: Result<TimeObservation, TimeGateError>,
        operation: TimeOperation,
        args: &[DriftValue],
    ) -> Result<bool, TimeGateError> {
        let obs = observation?;
        if matches!(operation, TimeOperation::Bar) && !args.is_empty() {
            return Err(TimeGateError::InvalidObservation);
        }
        if matches!(operation, TimeOperation::Bar) && !obs.is_bar() {
            return Ok(false);
        }
        if !self.writable {
            return Err(TimeGateError::Capacity);
        }
        let granted = match operation {
            TimeOperation::Cooldown => {
                let key = name(args.first()).ok_or(TimeGateError::InvalidCooldown)?;
                let Some(DriftValue::Number(ms)) = args.get(1) else {
                    return Err(TimeGateError::InvalidCooldown);
                };
                if args.len() != 2
                    || !ms.is_finite()
                    || ms.fract() != 0.0
                    || *ms < 1.0
                    || *ms > MAX_COOLDOWN_MS as f64
                {
                    return Err(TimeGateError::InvalidCooldown);
                }
                self.state.time.try_cooldown(obs, key, *ms as u64)?
            }
            TimeOperation::Bar => self.state.time.accept_closed_bar(obs)?,
        };
        self.dirty = true;
        Ok(granted)
    }

    fn update_indicator(
        &mut self,
        kind: Kind,
        args: &[DriftValue],
    ) -> Result<DriftValue, DriftValue> {
        let key =
            name(args.first()).ok_or_else(|| error_object("invalid indicator name".into()))?;
        if !self.writable {
            return Err(error_object("instrument state capacity exceeded".into()));
        }
        let (spec, input) = kind.parse(args)?;
        let indicators = &mut self.state.indicators;
        if !indicators.contains_key(key) {
            if indicators.len() >= MAX_INDICATORS {
                return Err(error_object("scoped indicator capacity exceeded".into()));
            }
            indicators.insert(key.to_owned(), Instance::new(spec)?);
        }
        let instance = indicators
            .get_mut(key)
            .ok_or_else(|| error_object("missing indicator".into()))?;
        instance.update(spec, input)
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
            obj.insert(
                "indicators".into(),
                DriftValue::Object(
                    Kind::ALL
                        .into_iter()
                        .map(|kind| {
                            (
                                kind.name().to_owned(),
                                DriftValue::NativeFunction(self.indicator_fn(kind)),
                            )
                        })
                        .collect(),
                ),
            );
        }
    }

    pub(super) fn attach_time(
        &self,
        ctx: &mut DriftValue,
        event: &MarketEvent,
        now: DateTime<Utc>,
    ) {
        let observation = TimeObservation::new(event, now);
        if let DriftValue::Object(obj) = ctx {
            obj.insert(
                "time".into(),
                DriftValue::Object(HashMap::from([
                    (
                        "try_cooldown".into(),
                        DriftValue::NativeFunction(
                            self.time_fn(observation, TimeOperation::Cooldown),
                        ),
                    ),
                    (
                        "accept_closed_bar".into(),
                        DriftValue::NativeFunction(self.time_fn(observation, TimeOperation::Bar)),
                    ),
                ])),
            );
        }
    }

    fn time_fn(
        &self,
        observation: Result<TimeObservation, TimeGateError>,
        operation: TimeOperation,
    ) -> NativeFn {
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
                let result = s.apply_time(observation, operation, &args);
                match result {
                    Ok(granted) => DriftValue::Bool(granted),
                    Err(error) => {
                        tracing::warn!(%error,"strategy time gate rejected");
                        s.invalid = true;
                        DriftValue::Bool(false)
                    }
                }
            })
        })
    }

    fn indicator_fn(&self, kind: Kind) -> NativeFn {
        let stage = Arc::clone(&self.0);
        Arc::new(move |args| {
            let stage = Arc::clone(&stage);
            Box::pin(async move {
                let Ok(mut s) = stage.lock() else {
                    return error_object("state lock poisoned".into());
                };
                if s.closed {
                    return error_object("expired indicator callback".into());
                }
                let result = s.update_indicator(kind, &args);
                match result {
                    Ok(value) => {
                        s.dirty = true;
                        value
                    }
                    Err(error) => {
                        s.invalid = true;
                        error
                    }
                }
            })
        })
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
                        .state
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
                            && (s.state.values.contains_key(key)
                                || s.state.values.len() < MAX_KEYS) =>
                    {
                        s.state.values.insert(key.to_owned(), value.clone());
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
    #[test]
    fn all_scoped_native_indicators_match_independent_reference_instances() {
        use super::super::scoped_indicators::Kind;
        fn equal(a: &DriftValue, b: &DriftValue) {
            match (a, b) {
                (DriftValue::Null, DriftValue::Null) => (),
                (DriftValue::Number(x), DriftValue::Number(y)) => assert_eq!(x, y),
                (DriftValue::Object(x), DriftValue::Object(y)) => {
                    assert_eq!(x.len(), y.len());
                    for (key, v) in x {
                        equal(v, &y[key]);
                    }
                }
                _ => panic!("indicator output mismatch: {a:?} != {b:?}"),
            }
        }
        fn args(kind: Kind, price: f64) -> Vec<DriftValue> {
            let mut args = vec![string("instance"), DriftValue::Number(2.0)];
            if kind == Kind::Macd {
                args.extend([DriftValue::Number(3.0), DriftValue::Number(2.0)]);
            } else if kind == Kind::Bollinger {
                args.push(DriftValue::Number(2.0));
            }
            if kind == Kind::Atr {
                args.push(DriftValue::Object(HashMap::from([
                    ("high".into(), DriftValue::Number(price + 1.0)),
                    ("low".into(), DriftValue::Number(price - 1.0)),
                    ("close".into(), DriftValue::Number(price)),
                ])));
            } else {
                args.push(DriftValue::Number(price));
            }
            args
        }
        for kind in Kind::ALL {
            let mut state = ScriptState::default();
            let a = event("BTCUSDT");
            let b = event("ETHUSDT");
            let (spec, _) = kind.parse(&args(kind, 10.0)).unwrap();
            let mut expected_a = Instance::new(spec).unwrap();
            let mut expected_b = Instance::new(spec).unwrap();
            let mut warmed = false;
            for p in [10.0, 12.0, 11.0, 14.0, 13.0, 16.0] {
                for (event, price, expected) in
                    [(&a, p, &mut expected_a), (&b, 100.0 - p, &mut expected_b)]
                {
                    let args = args(kind, price);
                    let (spec, input) = kind.parse(&args).unwrap();
                    let reference = expected.update(spec, input).unwrap();
                    warmed |= !matches!(reference, DriftValue::Null);
                    let stage = state.begin(event);
                    let actual = futures::executor::block_on(stage.indicator_fn(kind)(args));
                    equal(&actual, &reference);
                    assert!(state.finish(event, stage, true));
                }
            }
            assert!(warmed, "{}", kind.name());
        }
    }

    #[test]
    fn indicator_capacity_configuration_and_expired_callback_refuse() {
        let mut state = ScriptState::default();
        let event = event("BTCUSDT");
        let stage = state.begin(&event);
        let update = stage.indicator_fn(Kind::Sma);
        let call = |key: &str, period: f64, price: f64| {
            futures::executor::block_on(update(vec![
                string(key),
                DriftValue::Number(period),
                DriftValue::Number(price),
            ]))
        };
        for i in 0..MAX_INDICATORS {
            assert!(matches!(
                call(&format!("s{i}"), 2.0, 10.0),
                DriftValue::Null
            ));
        }
        assert!(state.finish(&event, stage, true));
        assert!(matches!(call("s0", 2.0, 999.0), DriftValue::Object(_)));
        let stage = state.begin(&event);
        let update = stage.indicator_fn(Kind::Sma);
        let call = |key: &str, period: f64, price: f64| {
            futures::executor::block_on(update(vec![
                string(key),
                DriftValue::Number(period),
                DriftValue::Number(price),
            ]))
        };
        assert!(matches!(call("s0", 2.0, 20.0), DriftValue::Number(15.0)));
        assert!(matches!(call("overflow", 2.0, 10.0), DriftValue::Object(_)));
        assert!(!state.finish(&event, stage, true));
        let stage = state.begin(&event);
        let update = stage.indicator_fn(Kind::Sma);
        assert!(matches!(
            futures::executor::block_on(update(vec![
                string("s0"),
                DriftValue::Number(2.0),
                DriftValue::Number(30.0)
            ])),
            DriftValue::Number(20.0)
        ));
        assert!(matches!(
            futures::executor::block_on(update(vec![
                string("s0"),
                DriftValue::Number(513.0),
                DriftValue::Number(30.0)
            ])),
            DriftValue::Object(_)
        ));
        assert!(!state.finish(&event, stage, true));
    }
    #[test]
    fn retained_time_gate_cannot_advance_committed_cooldown() {
        let mut state = ScriptState::default();
        let event = event("BTCUSDT");
        let MarketEvent::Trade(t) = &event else {
            panic!("trade fixture")
        };
        let obs = TimeObservation::new(&event, t.timestamp);
        let stage = state.begin(&event);
        let gate = stage.time_fn(obs, TimeOperation::Cooldown);
        let args = || vec![string("entry"), DriftValue::Number(1000.0)];
        assert!(matches!(
            futures::executor::block_on(gate(args())),
            DriftValue::Bool(true)
        ));
        assert!(state.finish(&event, stage, true));
        assert!(matches!(
            futures::executor::block_on(gate(args())),
            DriftValue::Bool(false)
        ));
        let stage = state.begin(&event);
        let gate = stage.time_fn(obs, TimeOperation::Cooldown);
        assert!(matches!(
            futures::executor::block_on(gate(args())),
            DriftValue::Bool(false)
        ));
        assert!(state.finish(&event, stage, true));
    }
}

mod checkpoint;
pub(super) use checkpoint::{InstrumentKey, ScriptCheckpoint};
