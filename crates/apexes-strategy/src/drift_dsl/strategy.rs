//! [`DriftScriptStrategy`]: a [`crate::Strategy`] implementation that runs
//! a user-authored Drift Script (`.drift`) file as the strategy's decision
//! logic, so researchers can iterate on strategy behavior without a Rust
//! compile cycle.

use drift_script::{DriftValue, Interpreter, ScriptParser};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::drift_dsl::bindings::build_ctx_drift_value;
use crate::drift_dsl::convert::{market_event_to_drift_value, order_intents_from_drift_value};
use crate::drift_dsl::params::ParamStore;
use crate::error::StrategyError;
use crate::strategy::{OrderIntent, Strategy, StrategyContext};

/// Runs a `.drift` script's top-level `on_market_event(event, ctx)`
/// function as a [`Strategy`] implementation. The script is parsed and its
/// top-level statements executed exactly once, at construction
/// ([`DriftScriptStrategy::from_file`]); `drift-script-math`'s `math.*`
/// bindings are registered automatically, so scripts can use
/// `math.mean()`/`math.stddev()`/etc. out of the box.
///
/// A script's `param(name, default)` calls read/write a shared
/// [`ParamStore`] (`self.params`) -- see `crate::drift_dsl::params` for
/// the full top-level-vs-inside-`on_market_event` hot-reload semantics.
pub struct DriftScriptStrategy {
    interpreter: Interpreter,
    on_market_event_fn: DriftValue,
    on_order_outcome_fn: Option<DriftValue>,
    on_fill_fn: Option<DriftValue>,
    on_order_terminal_fn: Option<DriftValue>,
    on_parent_outcome_fn: Option<DriftValue>,
    params: Arc<ParamStore>,
    symbol_state: super::symbol_state::ScriptState,
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
        Self::from_file_inner(path, std::collections::HashMap::new())
    }

    /// Like `from_file`, but registers a `param(name, default)` native
    /// function (see `crate::drift_dsl::params::register_to`) that
    /// returns `overrides.get(name)` when present, otherwise `default`.
    /// Intended for parameter-optimization callers (`apexes-optimize`)
    /// that need to re-construct the same script with different
    /// parameter values across many backtest runs.
    pub fn from_file_with_params(
        path: &Path,
        overrides: std::collections::HashMap<String, rust_decimal::Decimal>,
    ) -> Result<Self, StrategyError> {
        Self::from_file_inner(path, overrides)
    }

    fn from_file_inner(
        path: &Path,
        overrides: std::collections::HashMap<String, rust_decimal::Decimal>,
    ) -> Result<Self, StrategyError> {
        let source = std::fs::read_to_string(path).map_err(|source| StrategyError::Io {
            path: PathBuf::from(path),
            source,
        })?;

        let stmts = ScriptParser::new()
            .parse(&source)
            .map_err(|e| StrategyError::Parse(e.to_string()))?;

        let mut interpreter = Interpreter::new();
        drift_script_math::register_to(&interpreter);
        crate::drift_dsl::indicators::register_to(&interpreter);
        crate::drift_dsl::signals::register_to(&interpreter);
        let params = Arc::new(ParamStore::new(overrides));
        crate::drift_dsl::params::register_to(&interpreter, Arc::clone(&params));

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

        let on_order_outcome_fn = interpreter.env().get("on_order_outcome");
        if on_order_outcome_fn
            .as_ref()
            .is_some_and(|f| !matches!(f, DriftValue::Function(_)))
        {
            return Err(StrategyError::OrderOutcomeNotCallable);
        }

        let on_fill_fn = interpreter.env().get("on_fill");
        if on_fill_fn
            .as_ref()
            .is_some_and(|f| !matches!(f, DriftValue::Function(_)))
        {
            return Err(StrategyError::FillCallbackNotCallable);
        }

        let on_order_terminal_fn = interpreter.env().get("on_order_terminal");
        if on_order_terminal_fn
            .as_ref()
            .is_some_and(|f| !matches!(f, DriftValue::Function(_)))
        {
            return Err(StrategyError::TerminalCallbackNotCallable);
        }

        let on_parent_outcome_fn = interpreter.env().get("on_parent_outcome");
        if on_parent_outcome_fn
            .as_ref()
            .is_some_and(|f| !matches!(f, DriftValue::Function(_)))
        {
            return Err(StrategyError::ParentCallbackNotCallable);
        }

        Ok(Self {
            interpreter,
            on_parent_outcome_fn,
            on_order_terminal_fn,
            on_fill_fn,
            on_order_outcome_fn,
            on_market_event_fn,
            params,
            symbol_state: Default::default(),
        })
    }
}

impl Strategy for DriftScriptStrategy {
    fn on_market_event(
        &mut self,
        event: &apexes_core::MarketEvent,
        ctx: &StrategyContext,
    ) -> Vec<OrderIntent> {
        let mut ctx_value = build_ctx_drift_value(
            ctx.positions.clone(),
            ctx.balances.clone(),
            ctx.open_orders.clone(),
            ctx.now,
        );
        super::context_market::attach(&mut ctx_value, event, ctx);
        let state = self.symbol_state.begin(event);
        state.attach(&mut ctx_value);
        state.attach_time(&mut ctx_value, event, ctx.now);
        let event_value = market_event_to_drift_value(event);

        let handle = tokio::runtime::Handle::current();
        let call_result = handle.block_on(self.interpreter.call(
            self.on_market_event_fn.clone(),
            vec![event_value, ctx_value],
        ));

        let result = match call_result {
            Ok(value) => match order_intents_from_drift_value(value) {
                Ok(intents) => Some(intents),
                Err(msg) => {
                    tracing::error!("on_market_event returned an invalid value: {msg}");
                    None
                }
            },
            Err(e) => {
                tracing::error!("on_market_event script error: {e}");
                None
            }
        };
        if self.symbol_state.finish(event, state, result.is_some()) {
            result.unwrap_or_default()
        } else {
            if result.is_some() {
                tracing::error!(symbol = event.symbol(), venue = ?event.venue(), "symbol state operation rejected; discarding callback writes and intents");
            }
            vec![]
        }
    }

    fn on_order_outcome(&mut self, outcome: &crate::OrderOutcome<'_>) {
        if let Some(callback) = &self.on_order_outcome_fn {
            let handle = tokio::runtime::Handle::current();
            if handle
                .block_on(self.interpreter.call(
                    callback.clone(),
                    vec![super::order_outcome::to_value(outcome)],
                ))
                .is_err()
            {
                tracing::error!("on_order_outcome script error");
            }
        }
    }

    fn on_fill(&mut self, outcome: &crate::FillOutcome<'_>) {
        if let Some(callback) = &self.on_fill_fn {
            let handle = tokio::runtime::Handle::current();
            if handle
                .block_on(self.interpreter.call(
                    callback.clone(),
                    vec![super::fill_outcome::to_value(outcome)],
                ))
                .is_err()
            {
                tracing::error!("on_fill script error");
            }
        }
    }

    fn on_order_terminal(&mut self, outcome: &crate::TerminalOutcome<'_>) {
        if let Some(callback) = &self.on_order_terminal_fn {
            let handle = tokio::runtime::Handle::current();
            if handle
                .block_on(self.interpreter.call(
                    callback.clone(),
                    vec![super::terminal_outcome::to_value(outcome)],
                ))
                .is_err()
            {
                tracing::error!("on_order_terminal script error");
            }
        }
    }

    fn on_parent_outcome(&mut self, outcome: &crate::ParentOutcome<'_>) {
        if let Some(callback) = &self.on_parent_outcome_fn {
            let handle = tokio::runtime::Handle::current();
            if handle
                .block_on(self.interpreter.call(
                    callback.clone(),
                    vec![super::parent_outcome::to_value(outcome)],
                ))
                .is_err()
            {
                tracing::error!("on_parent_outcome script error");
            }
        }
    }

    fn current_params(&self) -> HashMap<String, Decimal> {
        self.params.snapshot()
    }

    fn set_param(&mut self, name: &str, value: Decimal) -> Result<(), StrategyError> {
        // A script that never calls `param(name, ...)` for this exact
        // name has no way to know this name exists -- accept the write
        // anyway (store it), since a FUTURE param(name, ...) call (e.g.
        // the script gets edited and relaunched, or the name is simply
        // not yet reached in execution order) would then see it.
        // Returning UnknownParam here would require tracking "names
        // this script has ever declared", which `ParamStore`
        // intentionally does not do.
        self.params.set(name.to_string(), value);
        Ok(())
    }
}
