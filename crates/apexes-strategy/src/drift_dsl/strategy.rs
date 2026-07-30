//! [`DriftScriptStrategy`]: a [`crate::Strategy`] implementation that runs
//! a user-authored Drift Script (`.drift`) file as the strategy's decision
//! logic, so researchers can iterate on strategy behavior without a Rust
//! compile cycle.

use drift_script::{DriftValue, Interpreter, ScriptParser};
use std::path::{Path, PathBuf};

use crate::drift_dsl::bindings::build_ctx_drift_value;
use crate::drift_dsl::convert::{market_event_to_drift_value, order_intents_from_drift_value};
use crate::error::StrategyError;
use crate::strategy::{OrderIntent, Strategy, StrategyContext};

/// Runs a `.drift` script's top-level `on_market_event(event, ctx)`
/// function as a [`Strategy`] implementation. The script is parsed and its
/// top-level statements executed exactly once, at construction
/// ([`DriftScriptStrategy::from_file`]); `drift-script-math`'s `math.*`
/// bindings are registered automatically, so scripts can use
/// `math.mean()`/`math.stddev()`/etc. out of the box.
pub struct DriftScriptStrategy {
    interpreter: Interpreter,
    on_market_event_fn: DriftValue,
}

impl DriftScriptStrategy {
    /// Reads and parses `path`, executes its top-level statements (so a
    /// `function on_market_event(event, ctx) { ... }` declaration enters
    /// the interpreter's environment), then resolves that function's
    /// value. Returns `Err` if the file cannot be read, fails to parse, or
    /// does not define a callable `on_market_event` function at the top
    /// level -- construction fails closed, never producing a
    /// partially-usable instance.
    pub fn from_file(path: &Path) -> Result<Self, StrategyError> {
        let source = std::fs::read_to_string(path).map_err(|source| StrategyError::Io {
            path: PathBuf::from(path),
            source,
        })?;

        let stmts = ScriptParser::new()
            .parse(&source)
            .map_err(|e| StrategyError::Parse(e.to_string()))?;

        let mut interpreter = Interpreter::new();
        drift_script_math::register_to(&interpreter);

        let handle = tokio::runtime::Handle::current();
        handle
            .block_on(interpreter.evaluate(&stmts))
            .map_err(|e| StrategyError::Parse(e.to_string()))?;

        let on_market_event_fn = interpreter
            .env()
            .get("on_market_event")
            .ok_or(StrategyError::MissingEntryPoint)?;

        if !matches!(on_market_event_fn, DriftValue::Function(_)) {
            return Err(StrategyError::EntryPointNotCallable);
        }

        Ok(Self {
            interpreter,
            on_market_event_fn,
        })
    }
}

impl Strategy for DriftScriptStrategy {
    fn on_market_event(
        &mut self,
        event: &apexes_core::MarketEvent,
        ctx: &StrategyContext,
    ) -> Vec<OrderIntent> {
        let ctx_value = build_ctx_drift_value(
            ctx.positions.clone(),
            ctx.balances.clone(),
            ctx.open_orders.clone(),
            ctx.now,
        );
        let event_value = market_event_to_drift_value(event);

        let handle = tokio::runtime::Handle::current();
        let call_result = handle.block_on(self.interpreter.call(
            self.on_market_event_fn.clone(),
            vec![event_value, ctx_value],
        ));

        match call_result {
            Ok(value) => match order_intents_from_drift_value(value) {
                Ok(intents) => intents,
                Err(msg) => {
                    tracing::error!("on_market_event returned an invalid value: {msg}");
                    vec![]
                }
            },
            Err(e) => {
                tracing::error!("on_market_event script error: {e}");
                vec![]
            }
        }
    }
}
