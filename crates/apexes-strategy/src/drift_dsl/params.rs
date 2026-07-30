//! `param(name, default)` binding exposed to Drift Script strategy
//! scripts, letting a script declare a tunable parameter and read either
//! a caller-supplied override or its own literal default in one call:
//! `let sma_period = param("sma_period", 20);`. Unlike `indicators.*`/
//! `signals.*` (which are the same for every construction of a
//! `DriftScriptStrategy`), the `overrides` map this function consults is
//! supplied per-construction via
//! [`crate::drift_dsl::DriftScriptStrategy::from_file_with_params`], so
//! the exact same script can be re-run with different parameter values
//! across many backtests (the `apexes-optimize` crate's primary use
//! case).

use std::collections::HashMap;
use std::sync::Arc;

use drift_script::{DriftValue, Interpreter, NativeFn};
use rust_decimal::Decimal;

/// Installs the `param` global function into `interp`'s environment.
/// `overrides` is consulted once per `param(name, default)` call (not
/// cached) -- in practice a script calls `param(...)` once per
/// parameter, at the top level, and stores the result in a `let`
/// binding, so re-consulting `overrides` on every call has no observable
/// cost.
pub fn register_to(interp: &Interpreter, overrides: HashMap<String, Decimal>) {
    let overrides = Arc::new(overrides);
    let param_fn: NativeFn = Arc::new(move |args| {
        let overrides = Arc::clone(&overrides);
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

            match overrides.get(&name) {
                Some(value) => DriftValue::Number((*value).try_into().unwrap_or(default)),
                None => DriftValue::Number(default),
            }
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
        register_to(&interp, overrides);

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
        register_to(&interp, HashMap::new());

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
        register_to(&interp, HashMap::new());

        let f = get_param_fn(&interp);
        let result = call(&f, vec![DriftValue::Number(1.0), DriftValue::Number(20.0)]);
        assert!(matches!(result, DriftValue::Object(obj) if obj.contains_key("error")));
    }

    #[test]
    fn rejects_non_number_default() {
        let interp = Interpreter::new();
        register_to(&interp, HashMap::new());

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
        register_to(&interp, overrides);

        let stmts = ScriptParser::new()
            .parse("let threshold = param(\"threshold\", 0.2);")
            .unwrap();
        interp.evaluate(&stmts).await.unwrap();

        let value = interp.env().get("threshold").unwrap();
        assert!(matches!(value, DriftValue::Number(n) if n == 0.5));
    }
}
