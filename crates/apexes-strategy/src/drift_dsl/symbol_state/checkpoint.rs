//! F04.05: deterministic scoped state serialization. All floats are encoded
//! as their finite IEEE-754 bits, not via a potentially lossy JSON number.
use super::super::scoped_indicators::InstanceCheckpoint;
use super::*;
use crate::checkpoint::CheckpointError;
use crate::event_time::TimeCheckpoint;
use apexes_core::{Instrument, Venue};
use serde::{Deserialize, Serialize};

type Result<T> = std::result::Result<T, CheckpointError>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::drift_dsl) struct InstrumentKey {
    venue: Venue,
    symbol: String,
}

impl InstrumentKey {
    pub(in crate::drift_dsl) fn from_instruments(instruments: &[Instrument]) -> Result<Vec<Self>> {
        if instruments.is_empty() || instruments.len() > MAX_SYMBOLS {
            return Err(CheckpointError::Identity);
        }
        let mut keys = instruments
            .iter()
            .map(|i| Self {
                venue: i.venue,
                symbol: i.symbol.clone(),
            })
            .collect::<Vec<_>>();
        keys.sort_by_key(Self::sort_key);
        if keys.iter().any(|k| !k.valid()) || keys.windows(2).any(|w| w[0] == w[1]) {
            return Err(CheckpointError::Identity);
        }
        Ok(keys)
    }
    pub(in crate::drift_dsl) fn matches(&self, event: &MarketEvent) -> bool {
        self.venue == event.venue() && self.symbol == event.symbol()
    }
    fn sort_key(&self) -> (String, String) {
        (format!("{:?}", self.venue), self.symbol.clone())
    }
    fn valid(&self) -> bool {
        !self.symbol.is_empty() && self.symbol.len() <= MAX_NAME_BYTES
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", deny_unknown_fields)]
enum Value {
    Number(u64),
    Bool(bool),
    Null,
    String(String),
    Array(Vec<u64>),
}

impl Value {
    fn capture(value: &DriftValue) -> Result<Self> {
        if !valid_value(value) {
            return Err(CheckpointError::Payload);
        }
        Ok(match value {
            DriftValue::Number(n) => Self::Number(n.to_bits()),
            DriftValue::Bool(b) => Self::Bool(*b),
            DriftValue::Null => Self::Null,
            DriftValue::String(s) => Self::String(s.as_str().to_owned()),
            DriftValue::Array(values) => Self::Array(
                values
                    .iter()
                    .map(|v| match v {
                        DriftValue::Number(n) => Ok(n.to_bits()),
                        _ => Err(CheckpointError::Payload),
                    })
                    .collect::<Result<_>>()?,
            ),
            _ => return Err(CheckpointError::Payload),
        })
    }
    fn restore(self) -> Result<DriftValue> {
        let value = match self {
            Self::Number(bits) => DriftValue::Number(f64::from_bits(bits)),
            Self::Bool(b) => DriftValue::Bool(b),
            Self::Null => DriftValue::Null,
            Self::String(s) => DriftValue::String(Arc::new(s)),
            Self::Array(bits) => DriftValue::Array(
                bits.into_iter()
                    .map(|n| DriftValue::Number(f64::from_bits(n)))
                    .collect(),
            ),
        };
        if !valid_value(&value) {
            return Err(CheckpointError::Payload);
        }
        Ok(value)
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    instrument: InstrumentKey,
    values: Vec<(String, Value)>,
    indicators: Vec<(String, InstanceCheckpoint)>,
    time: TimeCheckpoint,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::drift_dsl) struct ScriptCheckpoint {
    entries: Vec<Entry>,
}

fn capture_entry(key: &(Venue, String), state: &InstrumentState) -> Result<Entry> {
    let mut values = state
        .values
        .iter()
        .map(|(k, v)| Ok((k.clone(), Value::capture(v)?)))
        .collect::<Result<Vec<_>>>()?;
    values.sort_by(|a, b| a.0.cmp(&b.0));
    let mut indicators = state
        .indicators
        .iter()
        .map(|(k, v)| Ok((k.clone(), v.checkpoint()?)))
        .collect::<Result<Vec<_>>>()?;
    indicators.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(Entry {
        instrument: InstrumentKey {
            venue: key.0,
            symbol: key.1.clone(),
        },
        values,
        indicators,
        time: state.time.checkpoint(),
    })
}

impl ScriptState {
    pub(in crate::drift_dsl) fn checkpoint(&self) -> Result<ScriptCheckpoint> {
        let mut entries = self
            .0
            .entries()
            .map(|(key, value)| capture_entry(key, value))
            .collect::<Result<Vec<_>>>()?;
        entries.sort_by_key(|entry| entry.instrument.sort_key());
        Ok(ScriptCheckpoint { entries })
    }
}

fn restore_map<V, T>(
    entries: Vec<(String, V)>,
    limit: usize,
    restore: impl Fn(V) -> Result<T>,
) -> Result<HashMap<String, T>> {
    if entries.len() > limit || !entries.windows(2).all(|w| w[0].0 < w[1].0) {
        return Err(CheckpointError::Payload);
    }
    let mut map = HashMap::new();
    for (key, value) in entries {
        if key.is_empty() || key.len() > MAX_NAME_BYTES || map.contains_key(&key) {
            return Err(CheckpointError::Payload);
        }
        map.insert(key, restore(value)?);
    }
    Ok(map)
}

impl ScriptCheckpoint {
    pub(in crate::drift_dsl) fn restore(
        self,
        scope: &[InstrumentKey],
        through: DateTime<Utc>,
    ) -> Result<ScriptState> {
        if self.entries.len() > MAX_SYMBOLS
            || !self
                .entries
                .windows(2)
                .all(|w| w[0].instrument.sort_key() < w[1].instrument.sort_key())
        {
            return Err(CheckpointError::Payload);
        }
        let mut state = ScriptState::default();
        for entry in self.entries {
            if !entry.instrument.valid() || !scope.contains(&entry.instrument) {
                return Err(CheckpointError::Identity);
            }
            let instrument = (entry.instrument.venue, entry.instrument.symbol);
            let restored = InstrumentState {
                values: restore_map(entry.values, MAX_KEYS, Value::restore)?,
                indicators: restore_map(
                    entry.indicators,
                    MAX_INDICATORS,
                    InstanceCheckpoint::restore,
                )?,
                time: entry.time.restore(through)?,
            };
            state
                .0
                .insert_checkpoint(instrument, restored)
                .map_err(|_| CheckpointError::Payload)?;
        }
        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{Bar, BarInterval, TradeEvent};
    use rust_decimal::Decimal;

    fn time(ms: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(ms).unwrap()
    }

    fn key(venue: Venue, symbol: &str) -> InstrumentKey {
        InstrumentKey {
            venue,
            symbol: symbol.into(),
        }
    }

    fn event(key: &InstrumentKey, ms: i64) -> MarketEvent {
        MarketEvent::Trade(TradeEvent {
            venue: key.venue,
            symbol: key.symbol.clone(),
            price: Decimal::ONE,
            quantity: Decimal::ONE,
            side: None,
            trade_id: None,
            timestamp: time(ms),
        })
    }

    fn advance(state: &mut ScriptState, key: &InstrumentKey, ms: i64, price: f64) {
        let event = event(key, ms);
        let stage = state.begin(&event);
        let mut lock = stage.0.lock().unwrap();
        lock.state
            .values
            .insert("number".into(), DriftValue::Number(-0.0));
        lock.state.values.insert(
            "string".into(),
            DriftValue::String(Arc::new("state".into())),
        );
        lock.state
            .values
            .insert("bool".into(), DriftValue::Bool(true));
        lock.state.values.insert("null".into(), DriftValue::Null);
        lock.state.values.insert(
            "array".into(),
            DriftValue::Array(vec![
                DriftValue::Number(f64::from_bits(1)),
                DriftValue::Number(price),
            ]),
        );
        lock.state
            .time
            .try_cooldown(
                TimeObservation::new(&event, time(ms)).unwrap(),
                "entry",
                1000,
            )
            .unwrap();
        for kind in Kind::ALL {
            let mut args = vec![
                DriftValue::String(Arc::new(kind.name().into())),
                DriftValue::Number(3.0),
            ];
            if kind == Kind::Macd {
                args.extend([DriftValue::Number(5.0), DriftValue::Number(2.0)]);
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
            lock.update_indicator(kind, &args).unwrap();
        }
        lock.dirty = true;
        drop(lock);
        assert!(state.finish(&event, stage, true));
    }

    fn payload(state: &ScriptState) -> String {
        serde_json::to_string(&state.checkpoint().unwrap()).unwrap()
    }

    #[test]
    fn every_indicator_and_value_round_trips_before_and_after_warmup() {
        let keys = [
            key(Venue::Binance, "BTCUSDT"),
            key(Venue::Binance, "ETHUSDT"),
            key(Venue::Okx, "BTCUSDT"),
        ];
        for cut in [0, 1, 2, 3, 5, 9, 50] {
            let mut original = ScriptState::default();
            for i in 0..cut {
                for (j, key) in keys.iter().enumerate() {
                    advance(
                        &mut original,
                        key,
                        i * 100,
                        10.0 + j as f64 * 100.0 + ((i * 13) % 23) as f64 / 7.0,
                    );
                }
            }
            let before = payload(&original);
            let mut restored = serde_json::from_str::<ScriptCheckpoint>(&before)
                .unwrap()
                .restore(&keys, time(cut * 100))
                .unwrap();
            assert_eq!(payload(&restored), before);
            for i in cut..cut + 12 {
                for (j, key) in keys.iter().enumerate() {
                    let price = 10.0 + j as f64 * 100.0 + ((i * 13) % 23) as f64 / 7.0;
                    advance(&mut original, key, i * 100, price);
                    advance(&mut restored, key, i * 100, price);
                }
                assert_eq!(
                    payload(&original),
                    payload(&restored),
                    "cut={cut}, step={i}"
                );
            }
        }
    }

    #[test]
    fn closed_bar_and_elapsed_gate_survive_and_reject_future_boundary() {
        let key = key(Venue::Binance, "BTCUSDT");
        let bar = MarketEvent::Bar(Bar {
            venue: key.venue,
            symbol: key.symbol.clone(),
            open: Decimal::ONE,
            high: Decimal::ONE,
            low: Decimal::ONE,
            close: Decimal::ONE,
            volume: Decimal::ONE,
            interval: BarInterval::OneSecond,
            open_time: time(0),
            close_time: time(1000),
        });
        let mut original = ScriptState::default();
        let stage = original.begin(&bar);
        let obs = TimeObservation::new(&bar, time(1000));
        {
            let mut stage = stage.0.lock().unwrap();
            assert!(stage.apply_time(obs, TimeOperation::Bar, &[]).unwrap());
            assert!(
                stage
                    .state
                    .time
                    .try_cooldown(obs.unwrap(), "entry", 1000)
                    .unwrap()
            );
        }
        assert!(original.finish(&bar, stage, true));
        let checkpoint = original.checkpoint().unwrap();
        assert!(
            checkpoint
                .clone()
                .restore(std::slice::from_ref(&key), time(999))
                .is_err()
        );
        let mut restored = checkpoint
            .restore(std::slice::from_ref(&key), time(1000))
            .unwrap();
        let stage = restored.begin(&bar);
        {
            let mut stage = stage.0.lock().unwrap();
            assert!(!stage.apply_time(obs, TimeOperation::Bar, &[]).unwrap());
            assert!(
                !stage
                    .state
                    .time
                    .try_cooldown(obs.unwrap(), "entry", 1000)
                    .unwrap()
            );
            let next = event(&key, 2000);
            assert!(
                stage
                    .state
                    .time
                    .try_cooldown(
                        TimeObservation::new(&next, time(2000)).unwrap(),
                        "entry",
                        1000
                    )
                    .unwrap()
            );
        }
        assert!(restored.finish(&bar, stage, true));
    }

    #[test]
    fn invalid_scope_duplicate_keys_invalid_values_and_indicators_are_refused() {
        let key = key(Venue::Binance, "BTCUSDT");
        let mut original = ScriptState::default();
        advance(&mut original, &key, 1000, 20.0);
        let checkpoint = original.checkpoint().unwrap();
        let scope = std::slice::from_ref(&key);
        assert!(checkpoint.clone().restore(&[], time(1000)).is_err());
        let mut duplicate = checkpoint.clone();
        duplicate.entries.push(duplicate.entries[0].clone());
        assert!(duplicate.restore(scope, time(1000)).is_err());
        let mut invalid = checkpoint.clone();
        invalid.entries[0].values[0].1 = Value::Number(f64::NAN.to_bits());
        assert!(invalid.restore(scope, time(1000)).is_err());
        let mut duplicate = checkpoint.clone();
        let value = duplicate.entries[0].values[0].clone();
        duplicate.entries[0].values.push(value);
        assert!(duplicate.restore(scope, time(1000)).is_err());
        let mut invalid = serde_json::to_value(checkpoint).unwrap();
        // ATR is alphabetically first. Its configured period must agree
        // with the validated native state and cannot be zero.
        invalid["entries"][0]["indicators"][0][1]["period"] = 0.into();
        assert!(
            serde_json::from_value::<ScriptCheckpoint>(invalid)
                .unwrap()
                .restore(scope, time(1000))
                .is_err()
        );
    }
}
