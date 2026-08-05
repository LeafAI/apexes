//! `param(name, default)` binding exposed to Drift Script strategy
//! scripts, letting a script declare a tunable parameter and read either
//! a caller-supplied override or its own literal default in one call:
//! `let sma_period = param("sma_period", 20);`. Unlike `indicators.*`/
//! `signals.*` (which are the same for every construction of a
//! `DriftScriptStrategy`), the [`ParamStore`] this function consults is
//! supplied per-construction via
//! [`crate::drift_dsl::DriftScriptStrategy::from_file_with_params`], so
//! the exact same script can be re-run with different parameter values
//! across many backtests (the `apexes-optimize` crate's primary use
//! case), AND is mutable/shared so a caller can hot-update a value via
//! [`ParamStore::set`] between ticks (see this module's doc on
//! `register_to` for the top-level-vs-inside-`on_market_event` caveat
//! that determines whether a given `param(...)` call site actually
//! observes such an update).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use drift_script::{DriftValue, Interpreter, NativeFn};
use rust_decimal::Decimal;

/// Shared, mutable store backing both the `param(name, default)` Drift
/// Script binding AND `DriftScriptStrategy::current_params`/`set_param`.
/// `Mutex` (not `RwLock`) -- per-tick lock hold times are microsecond-scale
/// map lookups/inserts, never held across an `.await`, and `set_param`
/// writes are rare (operator-driven) relative to `param()`'s own per-tick
/// reads -- a plain `Mutex` avoids `RwLock`'s extra complexity for a
/// workload that doesn't need reader/writer differentiation at this
/// scale.
pub struct ParamStore {
    values: Mutex<HashMap<String, Decimal>>,
}

impl ParamStore {
    pub fn new(initial_overrides: HashMap<String, Decimal>) -> Self {
        Self {
            values: Mutex::new(initial_overrides),
        }
    }

    /// Returns the stored value for `name` if present, otherwise
    /// `default` (without inserting `default` into the store -- a
    /// subsequent `set` for `name` starts from a clean slate, not from
    /// whatever various call sites' literal defaults happened to be).
    pub fn get_or_default(&self, name: &str, default: Decimal) -> Decimal {
        let values = match self.values.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        values.get(name).copied().unwrap_or(default)
    }

    /// Hot-updates (or inserts) a single parameter value.
    pub fn set(&self, name: String, value: Decimal) {
        let mut values = match self.values.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        values.insert(name, value);
    }

    /// A point-in-time copy of every currently-stored parameter value.
    pub fn snapshot(&self) -> HashMap<String, Decimal> {
        let values = match self.values.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        values.clone()
    }
}

/// Installs the `param` global function into `interp`'s environment,
/// reading from `store` on every call. This is unchanged from before in
/// one crucial respect: a script's TOP-LEVEL `param(...)` call reads the
/// store's value AT THAT MOMENT ONLY, since top-level statements execute
/// exactly once at script-load time (see
/// `DriftScriptStrategy::from_file_inner`) -- this is what makes the
/// top-level pattern NOT hot-reloadable, a real and deliberate
/// authoring-convention distinction from calling `param(...)` inside
/// `on_market_event`, which re-invokes this native function (and
/// therefore re-reads `store`) on every tick.
pub fn register_to(interp: &Interpreter, store: Arc<ParamStore>) {
    let param_fn: NativeFn = Arc::new(move |args| {
        let store = Arc::clone(&store);
        Box::pin(async move {
            const FN: &str = "param";
            let name = match args.first() {
                Some(DriftValue::String(s)) => s.as_str().to_string(),
                Some(other) => {
                    return error_object(format!(
                        "{FN}: first argument (name) must be a string, got {}",
                        other.type_name()
                    ));
                }
                None => return error_object(format!("{FN}: missing argument \"name\"")),
            };
            let default = match args.get(1) {
                Some(DriftValue::Number(n)) => *n,
                Some(other) => {
                    return error_object(format!(
                        "{FN}: second argument (default) must be a number, got {}",
                        other.type_name()
                    ));
                }
                None => return error_object(format!("{FN}: missing argument \"default\"")),
            };
            let default_decimal: Decimal = default.try_into().unwrap_or(Decimal::ZERO);

            let value = store.get_or_default(&name, default_decimal);
            DriftValue::Number(value.try_into().unwrap_or(default))
        })
    });
    interp
        .env()
        .define_const("param".to_string(), DriftValue::NativeFunction(param_fn));
}

fn error_object(message: String) -> DriftValue {
    let mut map = HashMap::new();
    map.insert("error".to_string(), DriftValue::String(Arc::new(message)));
    DriftValue::Object(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use drift_script::ScriptParser;

    fn call(f: &NativeFn, args: Vec<DriftValue>) -> DriftValue {
        futures::executor::block_on(f(args))
    }

    /// Extracts the registered `param` `NativeFn` back out of the
    /// interpreter's environment, for direct unit testing without
    /// parsing a whole script.
    fn get_param_fn(interp: &Interpreter) -> NativeFn {
        match interp.env().get("param") {
            Some(DriftValue::NativeFunction(f)) => f,
            other => panic!("expected param to be registered as a NativeFunction, got {other:?}"),
        }
    }

    #[test]
    fn returns_override_value_when_present() {
        let interp = Interpreter::new();
        let mut overrides = HashMap::new();
        overrides.insert("sma_period".to_string(), rust_decimal_macros::dec!(30));
        register_to(&interp, Arc::new(ParamStore::new(overrides)));

        let f = get_param_fn(&interp);
        let result = call(
            &f,
            vec![
                DriftValue::String(Arc::new("sma_period".to_string())),
                DriftValue::Number(20.0),
            ],
        );
        assert!(matches!(result, DriftValue::Number(n) if n == 30.0));
    }

    #[test]
    fn returns_default_when_override_absent() {
        let interp = Interpreter::new();
        register_to(&interp, Arc::new(ParamStore::new(HashMap::new())));

        let f = get_param_fn(&interp);
        let result = call(
            &f,
            vec![
                DriftValue::String(Arc::new("sma_period".to_string())),
                DriftValue::Number(20.0),
            ],
        );
        assert!(matches!(result, DriftValue::Number(n) if n == 20.0));
    }

    #[test]
    fn rejects_non_string_name() {
        let interp = Interpreter::new();
        register_to(&interp, Arc::new(ParamStore::new(HashMap::new())));

        let f = get_param_fn(&interp);
        let result = call(&f, vec![DriftValue::Number(1.0), DriftValue::Number(20.0)]);
        assert!(matches!(result, DriftValue::Object(obj) if obj.contains_key("error")));
    }

    #[test]
    fn rejects_non_number_default() {
        let interp = Interpreter::new();
        register_to(&interp, Arc::new(ParamStore::new(HashMap::new())));

        let f = get_param_fn(&interp);
        let result = call(
            &f,
            vec![
                DriftValue::String(Arc::new("sma_period".to_string())),
                DriftValue::String(Arc::new("20".to_string())),
            ],
        );
        assert!(matches!(result, DriftValue::Object(obj) if obj.contains_key("error")));
    }

    #[tokio::test]
    async fn works_end_to_end_via_a_real_script() {
        let mut interp = Interpreter::new();
        let mut overrides = HashMap::new();
        overrides.insert("threshold".to_string(), rust_decimal_macros::dec!(0.5));
        register_to(&interp, Arc::new(ParamStore::new(overrides)));

        let stmts = ScriptParser::new()
            .parse("let threshold = param(\"threshold\", 0.2);")
            .unwrap();
        interp.evaluate(&stmts).await.unwrap();

        let value = interp.env().get("threshold").unwrap();
        assert!(matches!(value, DriftValue::Number(n) if n == 0.5));
    }

    #[tokio::test]
    async fn set_after_top_level_call_is_not_observed_by_the_already_bound_let() {
        // Proves the ParamStore itself has no opinion about "top-level
        // vs. inside a function" -- the non-hot-reload behavior of the
        // top-level pattern comes entirely from the fact that a `let`
        // binding captures the DriftValue returned by that one call,
        // and nothing re-invokes `param(...)` afterwards. This is a
        // narrower, store-level companion to the full end-to-end proof
        // in `drift_script_strategy.rs`.
        let mut interp = Interpreter::new();
        let store = Arc::new(ParamStore::new(HashMap::new()));
        register_to(&interp, Arc::clone(&store));

        let stmts = ScriptParser::new()
            .parse("let x = param(\"x\", 1);")
            .unwrap();
        interp.evaluate(&stmts).await.unwrap();

        store.set("x".to_string(), rust_decimal_macros::dec!(99));

        let value = interp.env().get("x").unwrap();
        assert!(matches!(value, DriftValue::Number(n) if n == 1.0));
    }
}
