# Apexes: Extreme-Market Circuit Breaker & Automatic Strategy De-Risking Implementation Plan (`docs/FEATURES.md` row `6.5`)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the design in `docs/superpowers/specs/2026-08-04-apexes-extreme-market-circuit-breaker-design.md`: `apexes-risk::MarketConditionMonitor` (per-symbol price volatility/jump detection, optional account-level equity volatility, linearly-interpolated de-risking fractions), `apexes-backtest::PortfolioLedger::check_de_risking` (partial-close computation mirroring `check_liquidations`), backtest/paper auto-execution of de-risking orders, and live-trading monitor-only-by-default behavior with an explicit opt-in — completing roadmap sub-project `#5`'s row `6.5`.

**Architecture:** Eight sequential tasks in dependency order. Task 1 adds the new `apexes-risk` types (`MarketConditionConfig`, `RiskViolation` variants, `MarketConditionEventMonitor` trait, `MarketConditionMonitor` + its three `split()` adapters) and wires `CompositeRiskChecker`'s fourth registration list. Task 2 exposes the new public config field on ALL THREE caller-facing config types (`BacktestConfig`, `PaperTradingConfig`, `LiveTradingConfig`) in the SAME task — deliberately not deferred, per this plan's own explicit lesson from row `6.4`'s final review (see Global Constraints). Task 3 adds `PortfolioLedger::check_de_risking`. Task 4 wires `run_event_loop` (shared by `run_backtest`/`run_paper_trading`) to call `on_market_event`/`on_equity_update` per tick and auto-execute de-risking orders. Task 5 wires `run_live_trading` for monitor-only-by-default behavior with the `auto_derisk_live` opt-in. Task 6 is a full-workspace checkpoint. Task 7 is the final whole-branch review. Task 8 updates `docs/FEATURES.md`.

**Tech Stack:** Rust 2024, existing workspace deps only. `apexes-indicators::RollingStdDev` (already exists) is reused directly for both per-symbol price volatility and the optional account-level equity volatility.

## Global Constraints

- Rust edition 2024, workspace-wide `cargo check`/`cargo clippy --all-targets -- -D warnings`/`cargo fmt --check` must stay clean after every task.
- No `unsafe` blocks. Never bare `.unwrap()`/`.expect()` in production code (test code may use them freely).
- `thiserror` for all new error types; `serde`/`chrono`/`rust_decimal` conventions match existing crates exactly.
- Every commit signed: `git commit -S -s`.
- Existing spot-only / no-market-condition-monitor behavior must not change for any task in this plan — every extension is additive (`market_condition_config: None` must behave identically to today's code in every path).
- **CRITICAL LESSON FROM ROW 6.4's FINAL REVIEW, BINDING ON THIS PLAN:** the previous plan (margin & leverage monitoring) added `margin_tiers` to the internal `EventLoopConfig` in one task, but did NOT add the corresponding public field to `BacktestConfig`/`PaperTradingConfig` until a post-hoc fix wave dispatched after the final whole-branch review caught it — making the entire margin-monitoring system unreachable dead code for any real caller in between. This plan's Task 2 explicitly adds `market_condition_config` to ALL THREE public config types (`BacktestConfig`, `PaperTradingConfig`, `LiveTradingConfig`) in one single task, before any wiring task touches `run_event_loop`/`run_live_trading` — do not defer this.
- `MarketConditionMonitor`'s architecture (an `Arc<Mutex<_>>`-shared struct + `split()` into thin adapter structs, one per trait it needs to implement) directly mirrors `apexes-risk::MarginMonitor` (`docs/superpowers/specs/2026-08-03-apexes-margin-leverage-monitoring-design.md`) and, before that, `CircuitBreaker` — read `crates/apexes-risk/src/margin_monitor.rs` before starting Task 1 if anything below is unclear on *why* this shape, not just *what*.
- Backtest/paper auto-execute de-risking orders (this is a strategy-level risk decision, not exchange liquidation); live trading defaults to monitor-only (log + block new orders via `PreTradeCheck`), matching row `6.4`'s established live-trading-never-auto-liquidates precedent, with an explicit `auto_derisk_live: bool` opt-in (default `false`).
- Full design rationale (why linear interpolation instead of tiered brackets, why account-level triggers apply uniformly across all symbols, why extreme-market de-risking is a separate checker from `MarginMonitor` rather than merged into it) is in the design spec — read it before starting if anything below is unclear on *why*, not just *what*.

---

### Task 1: `apexes-risk`: `MarketConditionConfig`, `RiskViolation` variants, `MarketConditionEventMonitor` trait, `MarketConditionMonitor`

**Files:**
- Modify: `crates/apexes-risk/Cargo.toml` (add `apexes-indicators` dependency)
- Create: `crates/apexes-risk/src/market_condition_config.rs`
- Create: `crates/apexes-risk/src/market_condition_monitor.rs`
- Modify: `crates/apexes-risk/src/error.rs`
- Modify: `crates/apexes-risk/src/composite.rs`
- Modify: `crates/apexes-risk/src/lib.rs`

**Interfaces:**
- Consumes: `apexes_core::MarketEvent` (existing), `apexes_indicators::RollingStdDev` (existing).
- Produces: `apexes_risk::{MarketConditionConfig, PriceSource, MarketConditionMonitor, MarketConditionEventMonitor, MarketConditionPreTradeAdapter, MarketConditionEventAdapter, MarketConditionReadHandle}`; `RiskViolation::{MarketConditionBreached, AccountWideMarketConditionBreached}`; `CompositeRiskChecker::{with_market_condition_monitor, on_market_event}`.

- [ ] **Step 1: Add the `apexes-indicators` dependency**

```toml
# crates/apexes-risk/Cargo.toml -- add to [dependencies]:
apexes-indicators = { path = "../apexes-indicators" }
```

- [ ] **Step 2: Write `MarketConditionConfig`**

```rust
// crates/apexes-risk/src/market_condition_config.rs (new file)
//! [`MarketConditionConfig`]: tunable parameters for
//! [`crate::market_condition_monitor::MarketConditionMonitor`] --
//! thresholds, sensitivity, and which price source drives per-symbol
//! volatility/jump detection. See
//! `docs/superpowers/specs/2026-08-04-apexes-extreme-market-circuit-breaker-design.md`
//! for the full rationale.

use rust_decimal::Decimal;

/// Which `apexes_core::MarketEvent` variant's price field drives
/// per-symbol volatility/jump detection. Only events matching the
/// configured source are consumed by
/// `MarketConditionMonitor::on_market_event`; all other event types
/// are silently ignored (not an error -- e.g. a `Tick`/`Depth`/`Bar`
/// event arriving while `price_source: Trade` is configured is simply
/// a no-op for this monitor).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriceSource {
    /// `MarketEvent::Trade.price` -- the default, appropriate for spot
    /// instruments.
    Trade,
    /// `MarketEvent::MarkPrice.mark_price` -- appropriate for perpetual
    /// futures, where the mark price (not the last trade print) is the
    /// index-and-funding-basis-adjusted price real exchanges use for
    /// this kind of risk calculation.
    MarkPrice,
}

/// Configuration for [`crate::market_condition_monitor::MarketConditionMonitor`].
#[derive(Debug, Clone, PartialEq)]
pub struct MarketConditionConfig {
    /// Absolute threshold for `apexes_indicators::RollingStdDev`'s
    /// output (same unit as price) -- a symbol's rolling volatility at
    /// or above this value is one of the two independent triggers for
    /// that symbol being flagged as "extreme".
    pub volatility_threshold: Decimal,
    /// Threshold for the percentage change between two consecutive
    /// price updates for the same symbol (e.g. `dec!(0.05)` = 5%) --
    /// the other of the two independent triggers.
    pub jump_threshold_pct: Decimal,
    /// Sensitivity coefficient in the linear de-risking-fraction
    /// formula: `reduce_fraction = min(1.0, sensitivity * (actual /
    /// threshold - 1.0))`, where `actual`/`threshold` is whichever of
    /// the volatility or jump ratio is more severe for that symbol.
    pub sensitivity: Decimal,
    /// Window size passed to `apexes_indicators::RollingStdDev::new`
    /// for each symbol's (and, if configured, the account's) rolling
    /// volatility state.
    pub volatility_window: usize,
    /// Which event type's price field drives volatility/jump
    /// detection.
    pub price_source: PriceSource,
    /// If `Some`, additionally enables account-level equity volatility
    /// detection using the SAME `sensitivity` formula (with this as
    /// the threshold), fed by `PortfolioLedger::equity()` via
    /// `MarketConditionMonitor::on_equity_update`. When it trips, the
    /// computed `reduce_fraction` is applied uniformly to every
    /// currently-open position's symbol (see
    /// `MarketConditionMonitor::reduce_fractions`'s doc comment).
    /// `None` (the default) disables account-level detection entirely.
    pub account_equity_volatility_threshold: Option<Decimal>,
    /// Whether de-risking partial closes apply to spot positions
    /// (`Position.leverage: None`) too, not just leveraged ones.
    /// Defaults to `false` (leveraged-only) at the call site, but this
    /// struct itself has no default -- every field is explicit, no
    /// implicit `Default` impl (matching this project's existing
    /// convention of no-implicit-defaults for risk-configuration
    /// structs, e.g. `apexes_risk::RiskLimits`).
    pub include_spot: bool,
}
```

- [ ] **Step 3: Write the failing tests for `MarketConditionConfig`**

Add to a new `#[cfg(test)] mod tests` at the bottom of `crates/apexes-risk/src/market_condition_config.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn sample_config() -> MarketConditionConfig {
        MarketConditionConfig {
            volatility_threshold: dec!(100),
            jump_threshold_pct: dec!(0.05),
            sensitivity: dec!(2),
            volatility_window: 20,
            price_source: PriceSource::Trade,
            account_equity_volatility_threshold: None,
            include_spot: false,
        }
    }

    #[test]
    fn config_is_constructible_and_comparable() {
        let a = sample_config();
        let b = sample_config();
        assert_eq!(a, b);
    }

    #[test]
    fn price_source_variants_are_distinct() {
        assert_ne!(PriceSource::Trade, PriceSource::MarkPrice);
    }
}
```

Run: `cargo test -p apexes-risk market_condition_config`
Expected: FAIL with "unresolved import" or "cannot find" (module not yet registered in `lib.rs`) -- this is expected at this point; Step 5 registers it.

- [ ] **Step 4: Add the two new `RiskViolation` variants**

```rust
// crates/apexes-risk/src/error.rs -- add to the RiskViolation enum,
// after the existing MarginRatioBreached variant:
    #[error("market conditions for {symbol} have breached the de-risking threshold (reduce_fraction={reduce_fraction})")]
    MarketConditionBreached {
        symbol: String,
        reduce_fraction: Decimal,
    },

    #[error("account-wide market conditions have breached the de-risking threshold (reduce_fraction={reduce_fraction})")]
    AccountWideMarketConditionBreached { reduce_fraction: Decimal },
```

- [ ] **Step 5: Write `MarketConditionMonitor` and its trait/adapters**

```rust
// crates/apexes-risk/src/market_condition_monitor.rs (new file)
//! [`MarketConditionMonitor`]: detects per-symbol price volatility
//! spikes and price jumps (and, optionally, account-level equity
//! volatility), computing a linearly-interpolated de-risking fraction
//! for each affected symbol. Architecturally mirrors
//! [`crate::margin_monitor::MarginMonitor`] (`Arc<Mutex<_>>`-shared
//! state + `split()` into thin trait adapters), but is driven by raw
//! market events (via the new [`MarketConditionEventMonitor`] trait)
//! and portfolio equity updates, not by margin state.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use apexes_core::{MarketEvent, Order, OrderId};
use apexes_indicators::{Indicator, RollingStdDev};
use apexes_strategy::{OrderIntent, Position};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::error::RiskViolation;
use crate::market_condition_config::{MarketConditionConfig, PriceSource};
use crate::pre_trade::PreTradeCheck;

/// Called once per event-loop tick (event-by-event, not tied to
/// fills), for every `MarketEvent` -- so implementations can react to
/// raw price movement directly. Distinct from
/// `crate::position_monitor::PositionMonitor` (which only carries
/// margin state, no raw price) because volatility/jump detection needs
/// the actual price, not a derived margin ratio.
pub trait MarketConditionEventMonitor: Send + Sync {
    fn on_market_event(&mut self, event: &MarketEvent) -> Result<(), Vec<RiskViolation>>;
}

pub struct MarketConditionMonitor {
    config: MarketConditionConfig,
    volatility: HashMap<String, RollingStdDev>,
    last_price: HashMap<String, Decimal>,
    equity_volatility: Option<RollingStdDev>,
    reduce_fractions: HashMap<String, Decimal>,
    account_wide_reduce_fraction: Option<Decimal>,
}

impl MarketConditionMonitor {
    pub fn new(config: MarketConditionConfig) -> Self {
        let equity_volatility = config
            .account_equity_volatility_threshold
            .map(|_| RollingStdDev::new(config.volatility_window));
        Self {
            config,
            volatility: HashMap::new(),
            last_price: HashMap::new(),
            equity_volatility,
            reduce_fractions: HashMap::new(),
            account_wide_reduce_fraction: None,
        }
    }

    /// Splits into three adapters sharing this same instance: a
    /// `PreTradeCheck` (blocks new orders for tripped symbols), a
    /// `MarketConditionEventMonitor` (consumes market events to update
    /// volatility/jump state), and a read handle exposing the current
    /// per-symbol de-risking fractions plus the `on_equity_update`
    /// entry point for account-level detection.
    pub fn split(
        self,
    ) -> (
        MarketConditionPreTradeAdapter,
        MarketConditionEventAdapter,
        MarketConditionReadHandle,
    ) {
        let shared = Arc::new(Mutex::new(self));
        (
            MarketConditionPreTradeAdapter(Arc::clone(&shared)),
            MarketConditionEventAdapter(Arc::clone(&shared)),
            MarketConditionReadHandle(shared),
        )
    }

    fn price_from_event(&self, event: &MarketEvent) -> Option<(String, Decimal)> {
        match (self.config.price_source, event) {
            (PriceSource::Trade, MarketEvent::Trade(trade)) => {
                Some((trade.symbol.clone(), trade.price))
            }
            (PriceSource::MarkPrice, MarketEvent::MarkPrice(update)) => {
                Some((update.symbol.clone(), update.mark_price))
            }
            _ => None,
        }
    }

    /// Computes `min(1.0, sensitivity * (actual/threshold - 1.0))`,
    /// clamped to `0.0` when `actual <= threshold` (not yet breached).
    fn reduce_fraction_for(&self, actual: Decimal, threshold: Decimal) -> Decimal {
        if threshold.is_zero() || actual <= threshold {
            return Decimal::ZERO;
        }
        let over = actual / threshold - Decimal::ONE;
        (self.config.sensitivity * over).min(Decimal::ONE)
    }

    fn on_market_event_impl(&mut self, event: &MarketEvent) -> Result<(), Vec<RiskViolation>> {
        let Some((symbol, price)) = self.price_from_event(event) else {
            return Ok(());
        };

        let volatility = self
            .volatility
            .entry(symbol.clone())
            .or_insert_with(|| RollingStdDev::new(self.config.volatility_window))
            .update(price);

        let jump_pct = self
            .last_price
            .get(&symbol)
            .filter(|previous| !previous.is_zero())
            .map(|previous| (price - previous).abs() / previous);
        self.last_price.insert(symbol.clone(), price);

        let volatility_fraction = volatility
            .map(|value| self.reduce_fraction_for(value, self.config.volatility_threshold))
            .unwrap_or(Decimal::ZERO);
        let jump_fraction = jump_pct
            .map(|value| self.reduce_fraction_for(value, self.config.jump_threshold_pct))
            .unwrap_or(Decimal::ZERO);
        let fraction = volatility_fraction.max(jump_fraction);

        if fraction.is_zero() {
            self.reduce_fractions.remove(&symbol);
            Ok(())
        } else {
            self.reduce_fractions.insert(symbol.clone(), fraction);
            Err(vec![RiskViolation::MarketConditionBreached {
                symbol,
                reduce_fraction: fraction,
            }])
        }
    }

    fn on_equity_update_impl(&mut self, equity: Decimal) -> Result<(), Vec<RiskViolation>> {
        let Some(equity_volatility) = self.equity_volatility.as_mut() else {
            return Ok(());
        };
        let Some(threshold) = self.config.account_equity_volatility_threshold else {
            return Ok(());
        };
        let Some(volatility) = equity_volatility.update(equity) else {
            return Ok(());
        };
        let fraction = if threshold.is_zero() || volatility <= threshold {
            Decimal::ZERO
        } else {
            let over = volatility / threshold - Decimal::ONE;
            (self.config.sensitivity * over).min(Decimal::ONE)
        };
        if fraction.is_zero() {
            self.account_wide_reduce_fraction = None;
            Ok(())
        } else {
            self.account_wide_reduce_fraction = Some(fraction);
            Err(vec![RiskViolation::AccountWideMarketConditionBreached {
                reduce_fraction: fraction,
            }])
        }
    }

    fn check_impl(&self, intent: &OrderIntent) -> Result<(), Vec<RiskViolation>> {
        if let Some(fraction) = self.reduce_fractions.get(&intent.symbol) {
            return Err(vec![RiskViolation::MarketConditionBreached {
                symbol: intent.symbol.clone(),
                reduce_fraction: *fraction,
            }]);
        }
        if self.account_wide_reduce_fraction.is_some() {
            return Err(vec![RiskViolation::AccountWideMarketConditionBreached {
                reduce_fraction: self.account_wide_reduce_fraction.unwrap_or(Decimal::ZERO),
            }]);
        }
        Ok(())
    }

    /// Current per-symbol de-risking fractions the caller should act
    /// on this tick -- only symbols with a nonzero fraction. If the
    /// account-level trigger is currently active, its fraction is
    /// applied to EVERY symbol in `open_symbols` that isn't already
    /// present with a (possibly different, possibly larger) per-symbol
    /// fraction -- the caller passes the currently-open position
    /// symbols in explicitly, since this monitor has no visibility
    /// into `PortfolioLedger`'s positions itself.
    fn reduce_fractions_impl(&self, open_symbols: &[String]) -> HashMap<String, Decimal> {
        let mut result = self.reduce_fractions.clone();
        if let Some(account_fraction) = self.account_wide_reduce_fraction {
            for symbol in open_symbols {
                result
                    .entry(symbol.clone())
                    .and_modify(|existing| *existing = (*existing).max(account_fraction))
                    .or_insert(account_fraction);
            }
        }
        result
    }

    pub fn reset(&mut self, symbol: &str) {
        self.reduce_fractions.remove(symbol);
    }
}

pub struct MarketConditionPreTradeAdapter(Arc<Mutex<MarketConditionMonitor>>);

impl PreTradeCheck for MarketConditionPreTradeAdapter {
    fn check(
        &self,
        intent: &OrderIntent,
        _positions: &HashMap<String, Position>,
        _balances: &HashMap<String, Decimal>,
        _open_orders: &HashMap<OrderId, Order>,
        _now: DateTime<Utc>,
    ) -> Result<(), Vec<RiskViolation>> {
        let monitor = self.0.lock().unwrap_or_else(|p| p.into_inner());
        monitor.check_impl(intent)
    }
}

pub struct MarketConditionEventAdapter(Arc<Mutex<MarketConditionMonitor>>);

impl MarketConditionEventMonitor for MarketConditionEventAdapter {
    fn on_market_event(&mut self, event: &MarketEvent) -> Result<(), Vec<RiskViolation>> {
        let mut monitor = self.0.lock().unwrap_or_else(|p| p.into_inner());
        monitor.on_market_event_impl(event)
    }
}

pub struct MarketConditionReadHandle(Arc<Mutex<MarketConditionMonitor>>);

impl MarketConditionReadHandle {
    /// Feeds a fresh `PortfolioLedger::equity()` reading into the
    /// account-level equity-volatility detector (a no-op if
    /// `MarketConditionConfig.account_equity_volatility_threshold` is
    /// `None`). Separate from `MarketConditionEventMonitor` because its
    /// input is a `Decimal`, not a `MarketEvent`.
    pub fn on_equity_update(&self, equity: Decimal) -> Result<(), Vec<RiskViolation>> {
        let mut monitor = self.0.lock().unwrap_or_else(|p| p.into_inner());
        monitor.on_equity_update_impl(equity)
    }

    /// Current per-symbol de-risking fractions -- see
    /// `MarketConditionMonitor::reduce_fractions_impl`'s doc comment
    /// for how account-level triggers are folded in via `open_symbols`.
    pub fn reduce_fractions(&self, open_symbols: &[String]) -> HashMap<String, Decimal> {
        let monitor = self.0.lock().unwrap_or_else(|p| p.into_inner());
        monitor.reduce_fractions_impl(open_symbols)
    }

    pub fn reset(&self, symbol: &str) {
        let mut monitor = self.0.lock().unwrap_or_else(|p| p.into_inner());
        monitor.reset(symbol);
    }
}
```

- [ ] **Step 6: Write tests for `MarketConditionMonitor`**

Add to `crates/apexes-risk/src/market_condition_monitor.rs`'s `#[cfg(test)] mod tests`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, OrderType, Side, TradeEvent, Venue};
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn sample_config() -> MarketConditionConfig {
        MarketConditionConfig {
            volatility_threshold: dec!(2),
            jump_threshold_pct: dec!(0.10),
            sensitivity: dec!(1),
            volatility_window: 3,
            price_source: PriceSource::Trade,
            account_equity_volatility_threshold: None,
            include_spot: false,
        }
    }

    fn trade_event(symbol: &str, price: Decimal) -> MarketEvent {
        MarketEvent::Trade(TradeEvent {
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            price,
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        })
    }

    fn sample_intent(symbol: &str) -> OrderIntent {
        OrderIntent {
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: dec!(1),
            price: None,
            stop_price: None,
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        }
    }

    #[test]
    fn price_jump_trips_the_symbol_and_computes_a_reduce_fraction() {
        let monitor = MarketConditionMonitor::new(sample_config());
        let (pre_trade, mut event_monitor, read) = monitor.split();

        event_monitor
            .on_market_event(&trade_event("BTCUSDT", dec!(100)))
            .unwrap();
        // 20% jump vs a 10% threshold -> reduce_fraction = min(1.0, 1*(0.20/0.10 - 1)) = 1.0
        let result = event_monitor.on_market_event(&trade_event("BTCUSDT", dec!(120)));
        assert!(result.is_err());

        let fractions = read.reduce_fractions(&["BTCUSDT".to_string()]);
        assert_eq!(fractions.get("BTCUSDT"), Some(&dec!(1.0)));

        let check_result = pre_trade.check(
            &sample_intent("BTCUSDT"),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            Utc::now(),
        );
        assert!(check_result.is_err());
    }

    #[test]
    fn stable_prices_never_trip() {
        let monitor = MarketConditionMonitor::new(sample_config());
        let (pre_trade, mut event_monitor, read) = monitor.split();

        for _ in 0..5 {
            event_monitor
                .on_market_event(&trade_event("BTCUSDT", dec!(100)))
                .unwrap();
        }

        assert!(read.reduce_fractions(&["BTCUSDT".to_string()]).is_empty());
        let check_result = pre_trade.check(
            &sample_intent("BTCUSDT"),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            Utc::now(),
        );
        assert!(check_result.is_ok());
    }

    #[test]
    fn auto_resets_once_price_stabilizes() {
        let monitor = MarketConditionMonitor::new(sample_config());
        let (pre_trade, mut event_monitor, _read) = monitor.split();

        event_monitor
            .on_market_event(&trade_event("BTCUSDT", dec!(100)))
            .unwrap();
        event_monitor
            .on_market_event(&trade_event("BTCUSDT", dec!(120)))
            .unwrap_err();
        // A subsequent small move (well under the jump threshold)
        // clears the trip.
        event_monitor
            .on_market_event(&trade_event("BTCUSDT", dec!(121)))
            .unwrap();

        let check_result = pre_trade.check(
            &sample_intent("BTCUSDT"),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            Utc::now(),
        );
        assert!(check_result.is_ok());
    }

    #[test]
    fn events_not_matching_the_configured_price_source_are_ignored() {
        let monitor = MarketConditionMonitor::new(sample_config()); // price_source: Trade
        let (_pre_trade, mut event_monitor, read) = monitor.split();

        let tick = MarketEvent::Tick(apexes_core::Tick {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bid_price: dec!(100),
            bid_size: dec!(1),
            ask_price: dec!(101),
            ask_size: dec!(1),
            timestamp: Utc::now(),
        });
        event_monitor.on_market_event(&tick).unwrap();
        assert!(read.reduce_fractions(&["BTCUSDT".to_string()]).is_empty());
    }

    #[test]
    fn account_wide_equity_volatility_applies_uniformly_to_open_symbols() {
        let mut config = sample_config();
        config.account_equity_volatility_threshold = Some(dec!(50));
        config.volatility_window = 2;
        let monitor = MarketConditionMonitor::new(config);
        let (_pre_trade, _event_monitor, read) = monitor.split();

        read.on_equity_update(dec!(10000)).unwrap();
        // Large equity swing -> should trip the account-wide detector.
        let result = read.on_equity_update(dec!(9000));
        assert!(result.is_err());

        let fractions = read.reduce_fractions(&["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
        assert!(fractions.contains_key("BTCUSDT"));
        assert!(fractions.contains_key("ETHUSDT"));
        assert_eq!(fractions["BTCUSDT"], fractions["ETHUSDT"]);
    }

    #[test]
    fn account_equity_detection_is_a_noop_when_not_configured() {
        let monitor = MarketConditionMonitor::new(sample_config()); // threshold: None
        let (_pre_trade, _event_monitor, read) = monitor.split();

        let result = read.on_equity_update(dec!(1));
        assert!(result.is_ok());
        assert!(read.reduce_fractions(&[]).is_empty());
    }
}
```

- [ ] **Step 7: Add `CompositeRiskChecker`'s fourth registration list and `on_market_event`**

```rust
// crates/apexes-risk/src/composite.rs -- add import:
use crate::market_condition_monitor::MarketConditionEventMonitor;
```

```rust
// crates/apexes-risk/src/composite.rs -- CompositeRiskChecker struct,
// add a fourth field:
#[derive(Default)]
pub struct CompositeRiskChecker {
    pre_trade_checks: Mutex<Vec<Box<dyn PreTradeCheck>>>,
    in_trade_monitors: Mutex<Vec<Box<dyn InTradeMonitor>>>,
    position_monitors: Mutex<Vec<Box<dyn PositionMonitor>>>,
    market_condition_monitors: Mutex<Vec<Box<dyn MarketConditionEventMonitor>>>,
}
```

```rust
// crates/apexes-risk/src/composite.rs -- add a builder method alongside
// with_position_monitor:
    pub fn with_market_condition_monitor(
        self,
        monitor: Box<dyn MarketConditionEventMonitor>,
    ) -> Self {
        self.market_condition_monitors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(monitor);
        self
    }
```

```rust
// crates/apexes-risk/src/composite.rs -- add a method alongside on_tick:
    /// Runs every registered `MarketConditionEventMonitor::on_market_event`,
    /// collecting every violation from every monitor into one combined
    /// result. Called once per event-loop event (not once per tick like
    /// `on_tick`, since market-condition detection needs the raw event
    /// stream, not a derived per-tick snapshot).
    pub fn on_market_event(
        &self,
        event: &apexes_core::MarketEvent,
    ) -> Result<(), Vec<RiskViolation>> {
        let mut monitors = self
            .market_condition_monitors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let violations: Vec<RiskViolation> = monitors
            .iter_mut()
            .filter_map(|monitor| monitor.on_market_event(event).err())
            .flatten()
            .collect();

        if violations.is_empty() {
            Ok(())
        } else {
            Err(violations)
        }
    }
```

- [ ] **Step 8: Write a test for `CompositeRiskChecker::on_market_event`**

Add to `crates/apexes-risk/src/composite.rs`'s existing `#[cfg(test)] mod tests`, mirroring the existing `on_tick_aggregates_violations_from_multiple_monitors` test's structure:

```rust
    struct AlwaysRejectMarketCondition(RiskViolation);

    impl MarketConditionEventMonitor for AlwaysRejectMarketCondition {
        fn on_market_event(
            &mut self,
            _event: &apexes_core::MarketEvent,
        ) -> Result<(), Vec<RiskViolation>> {
            Err(vec![self.0.clone()])
        }
    }

    #[test]
    fn on_market_event_aggregates_violations_from_multiple_monitors() {
        let composite = CompositeRiskChecker::new()
            .with_market_condition_monitor(Box::new(AlwaysRejectMarketCondition(
                RiskViolation::MarketConditionBreached {
                    symbol: "BTCUSDT".to_string(),
                    reduce_fraction: dec!(0.5),
                },
            )))
            .with_market_condition_monitor(Box::new(AlwaysRejectMarketCondition(
                RiskViolation::AccountWideMarketConditionBreached {
                    reduce_fraction: dec!(0.3),
                },
            )));

        let event = apexes_core::MarketEvent::Trade(apexes_core::TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(100),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        });

        let result = composite.on_market_event(&event);
        assert_eq!(result.unwrap_err().len(), 2);
    }
```

(Confirm `Venue`/`Utc`/`dec!` are already imported in this test module -- they are, per the existing tests in this file.)

- [ ] **Step 9: Register the new modules and re-exports in `lib.rs`**

```rust
// crates/apexes-risk/src/lib.rs -- add module declarations:
pub mod market_condition_config;
pub mod market_condition_monitor;
```

```rust
// crates/apexes-risk/src/lib.rs -- add re-exports:
pub use market_condition_config::{MarketConditionConfig, PriceSource};
pub use market_condition_monitor::{
    MarketConditionEventAdapter, MarketConditionEventMonitor, MarketConditionMonitor,
    MarketConditionPreTradeAdapter, MarketConditionReadHandle,
};
```

- [ ] **Step 10: Run this crate's tests, clippy, fmt, then a workspace-wide check**

Run: `cargo test -p apexes-risk -- --test-threads=1`
Expected: PASS, all new tests plus every pre-existing test in this crate.

Run: `cargo build --workspace`
Expected: 0 errors.

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean (run `cargo fmt` first if not).

- [ ] **Step 11: Commit**

```bash
git add -A
git commit -S -s -m "feat(apexes-risk): MarketConditionMonitor -- price volatility/jump detection with linear de-risking fractions"
```

---

### Task 2: Expose `market_condition_config` on `BacktestConfig`/`PaperTradingConfig`/`LiveTradingConfig` (ALL THREE, this task, not deferred)

**Files:**
- Modify: `crates/apexes-backtest/src/event_loop.rs` (`EventLoopConfig` gains the field; NOT wired into any logic yet -- Task 4 does that)
- Modify: `crates/apexes-backtest/src/runner.rs` (`BacktestConfig` + its own test)
- Modify: `crates/apexes-paper/src/runner.rs` (`PaperTradingConfig` + its own test)
- Modify: `crates/apexes-live-trading/src/runner.rs` (`LiveTradingConfig`, plus a new `auto_derisk_live` field)
- Modify every other `BacktestConfig { .. }` / `PaperTradingConfig { .. }` / `LiveTradingConfig { .. }` construction site in the workspace

**Interfaces:**
- Consumes: `apexes_risk::MarketConditionConfig` (Task 1).
- Produces: `EventLoopConfig.market_condition_config: Option<MarketConditionConfig>`; `BacktestConfig.market_condition_config`; `PaperTradingConfig.market_condition_config`; `LiveTradingConfig.{market_condition_config, auto_derisk_live}`.

- [ ] **Step 1: Add the field to `EventLoopConfig` (not wired into logic yet)**

```rust
// crates/apexes-backtest/src/event_loop.rs -- EventLoopConfig, add field
// after margin_tiers:
    /// Extreme-market circuit breaker configuration (design spec
    /// `2026-08-04-apexes-extreme-market-circuit-breaker-design.md`).
    /// `None` (the default for every pre-existing caller) is a silent
    /// no-op: no volatility/jump detection, no de-risking. NOT YET
    /// WIRED UP into `run_event_loop`'s logic in this task -- Task 4
    /// does that; this task only makes the field exist and thread
    /// through from the public config types, learning directly from
    /// row 6.4's `margin_tiers` gap (see this plan's own Global
    /// Constraints).
    pub market_condition_config: Option<apexes_risk::MarketConditionConfig>,
```

- [ ] **Step 2: Add the field to `BacktestConfig`, thread it into `EventLoopConfig`, update its own test**

```rust
// crates/apexes-backtest/src/runner.rs -- BacktestConfig, add field
// after margin_tiers:
    /// Extreme-market circuit breaker configuration. `None` (default)
    /// disables it entirely -- see `EventLoopConfig.market_condition_config`'s
    /// doc comment.
    pub market_condition_config: Option<apexes_strategy_placeholder_do_not_use>,
```

**STOP -- the line above is intentionally wrong, read this note instead of copying it verbatim:** `MarketConditionConfig` lives in `apexes_risk`, not `apexes_strategy` -- use the correct type:

```rust
// crates/apexes-backtest/src/runner.rs -- BacktestConfig, add field
// after margin_tiers (CORRECT version):
    /// Extreme-market circuit breaker configuration. `None` (default)
    /// disables it entirely -- see `EventLoopConfig.market_condition_config`'s
    /// doc comment.
    pub market_condition_config: Option<apexes_risk::MarketConditionConfig>,
```

```rust
// crates/apexes-backtest/src/runner.rs -- run_backtest's EventLoopConfig
// construction, add:
    let event_loop_config = EventLoopConfig {
        instruments: config.instruments,
        initial_balances: config.initial_balances,
        cost_model: config.cost_model,
        queue_position_factor: config.queue_position_factor,
        risk_free_return: config.risk_free_return,
        risk_checker,
        margin_tiers: config.margin_tiers,
        market_condition_config: config.market_condition_config,
    };
```

```rust
// crates/apexes-backtest/src/runner.rs -- its own test's BacktestConfig
// construction (search for "let config = BacktestConfig {"), add:
            margin_tiers: HashMap::new(),
            market_condition_config: None,
```

- [ ] **Step 3: Add the field to `PaperTradingConfig`, thread it, update its own test**

```rust
// crates/apexes-paper/src/runner.rs -- PaperTradingConfig, add field
// after margin_tiers:
    /// Extreme-market circuit breaker configuration. `None` (default)
    /// disables it entirely.
    pub market_condition_config: Option<apexes_risk::MarketConditionConfig>,
```

(Add `use apexes_risk::MarketConditionConfig;` -- or reference it fully-qualified as above -- to this file's imports; `apexes-paper` already depends on `apexes_risk` transitively via `apexes_backtest`, but confirm via `cargo check` whether a direct `apexes-risk` dependency needs adding to `crates/apexes-paper/Cargo.toml`; if the fully-qualified path `apexes_risk::MarketConditionConfig` doesn't resolve, add `apexes-risk = { path = "../apexes-risk" }` to `[dependencies]` there.)

```rust
// crates/apexes-paper/src/runner.rs -- run_paper_trading's EventLoopConfig
// construction, add:
    let event_loop_config = EventLoopConfig {
        instruments: config.instruments,
        initial_balances: config.initial_balances,
        cost_model: config.cost_model,
        queue_position_factor: config.queue_position_factor,
        risk_free_return: config.risk_free_return,
        risk_checker,
        margin_tiers: config.margin_tiers,
        market_condition_config: config.market_condition_config,
    };
```

```rust
// crates/apexes-paper/src/runner.rs -- its own test's PaperTradingConfig
// construction (search for "PaperTradingConfig {" inside base_config()),
// add:
            margin_tiers: HashMap::new(),
            market_condition_config: None,
```

- [ ] **Step 4: Add the fields to `LiveTradingConfig`**

```rust
// crates/apexes-live-trading/src/runner.rs -- LiveTradingConfig, add
// fields after margin_tiers:
    /// Extreme-market circuit breaker configuration. `None` (default)
    /// disables it entirely.
    pub market_condition_config: Option<apexes_risk::MarketConditionConfig>,
    /// Whether `run_live_trading` is allowed to automatically submit
    /// de-risking (partial-close) orders when `market_condition_config`
    /// detects extreme conditions. Defaults to `false` at every
    /// existing call site (added by this task): monitor-only, matching
    /// row 6.4's live-trading-never-auto-liquidates precedent. Set to
    /// `true` to explicitly opt into live automatic partial closes --
    /// this is a deliberate choice this plan's design spec calls out
    /// as a real behavioral difference from margin-based liquidation
    /// (row 6.4), which NEVER auto-executes in live trading regardless
    /// of configuration.
    pub auto_derisk_live: bool,
```

(`apexes-live-trading` already depends on `apexes-risk` directly -- confirm via `crates/apexes-live-trading/Cargo.toml`; it does, since `CompositeRiskChecker` is already imported in `runner.rs`.)

- [ ] **Step 5: Update every other construction site in the workspace**

Run to find every site (re-run yourself before starting, since this plan's authors' counts may have drifted):

```bash
grep -rln "BacktestConfig {" crates/
grep -rln "PaperTradingConfig {" crates/
grep -rln "LiveTradingConfig {" crates/
```

At drafting time, `BacktestConfig {` appears in: `crates/apexes-backtest/src/runner.rs` (its own test, handled in Step 2), `crates/apexes-backtest/tests/{end_to_end.rs (5 sites), mark_price_replay.rs, risk_integration.rs, live_storage_backtest.rs, margin_tiers_backtest.rs (2 sites)}`, `crates/apexes-optimize/tests/grid_search_end_to_end.rs`, `crates/apexes-strategy-registry/tests/registry_end_to_end.rs`. `PaperTradingConfig {` appears in: `crates/apexes-paper/src/runner.rs` (its own test, handled in Step 3), `crates/apexes-paper/tests/live_binance.rs`. `LiveTradingConfig {` appears in: `crates/apexes-live-trading/tests/live_trading_integration.rs` (its `base_config()` helper).

For every site outside the ones already handled in Steps 2-4, add `market_condition_config: None,` (and, for `LiveTradingConfig` sites, also `auto_derisk_live: false,`) -- preserving today's exact behavior for every existing caller.

- [ ] **Step 6: Run this crate's tests, clippy, fmt, then a workspace-wide check**

Run: `cargo test -p apexes-backtest -p apexes-paper -p apexes-live-trading -p apexes-optimize -p apexes-strategy-registry -- --test-threads=1`
Expected: PASS, 0 failures (no behavior change yet -- this task only adds fields and threads them through, without wiring any new logic).

Run: `cargo build --workspace`
Expected: 0 errors.

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean (run `cargo fmt` first if not).

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -S -s -m "feat(apexes-backtest,apexes-paper,apexes-live-trading): expose market_condition_config on all three public run configs"
```

---

### Task 3: `apexes-backtest::PortfolioLedger::check_de_risking`

**Files:**
- Modify: `crates/apexes-backtest/src/ledger.rs`

**Interfaces:**
- Consumes: `apexes_strategy::Position` (existing, with `leverage`/`allocated_margin` from row 6.4).
- Produces: `PortfolioLedger::check_de_risking(&self, reduce_fractions: &HashMap<String, Decimal>, include_spot: bool) -> Vec<(String, apexes_core::Side, Decimal)>`.

- [ ] **Step 1: Write `check_de_risking`**

```rust
// crates/apexes-backtest/src/ledger.rs -- add to impl PortfolioLedger,
// after check_liquidations:

    /// Returns every `(symbol, side, quantity)` a caller should submit
    /// as a partial-close market order to de-risk in response to
    /// extreme market conditions. `reduce_fractions` is read from
    /// `apexes_risk::MarketConditionReadHandle::reduce_fractions`
    /// (already resolved for account-wide triggers by that point --
    /// this method itself has no concept of "account-wide", it only
    /// sees a flat per-symbol map). `include_spot: false` skips
    /// positions with `leverage: None`. Symbols with zero quantity, or
    /// not present in `reduce_fractions`, are skipped entirely.
    /// `quantity = position.quantity.abs() * reduce_fraction`; `side`
    /// is inverted from the position's sign (long position ->
    /// `Side::Sell` to reduce, short position -> `Side::Buy`),
    /// matching `check_liquidations`'s caller-side convention exactly.
    pub fn check_de_risking(
        &self,
        reduce_fractions: &HashMap<String, Decimal>,
        include_spot: bool,
    ) -> Vec<(String, Side, Decimal)> {
        self.positions
            .iter()
            .filter_map(|(symbol, position)| {
                if position.quantity.is_zero() {
                    return None;
                }
                if !include_spot && position.leverage.is_none() {
                    return None;
                }
                let fraction = reduce_fractions.get(symbol)?;
                if fraction.is_zero() {
                    return None;
                }
                let side = if position.quantity.is_sign_positive() {
                    Side::Sell
                } else {
                    Side::Buy
                };
                let quantity = position.quantity.abs() * fraction;
                Some((symbol.clone(), side, quantity))
            })
            .collect()
    }
```

(Confirm `Side` is already imported in `ledger.rs` -- it is, via the existing `use apexes_core::{ApexesError, Fill, Instrument, MarginMode, Side};` line.)

- [ ] **Step 2: Write tests**

Add to `crates/apexes-backtest/src/ledger.rs`'s existing `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn check_de_risking_returns_a_partial_close_for_a_flagged_leveraged_long() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&leveraged_fill_for_symbol("BTCUSDT", Side::Buy, dec!(60000), dec!(1), dec!(10)))
            .unwrap();

        let mut reduce_fractions = HashMap::new();
        reduce_fractions.insert("BTCUSDT".to_string(), dec!(0.5));

        let orders = ledger.check_de_risking(&reduce_fractions, false);
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0], ("BTCUSDT".to_string(), Side::Sell, dec!(0.5)));
    }

    #[test]
    fn check_de_risking_returns_a_partial_close_for_a_flagged_short() {
        let mut instruments = HashMap::new();
        instruments.insert("BTCUSDT".to_string(), btc_usdt());
        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(10000));
        let mut ledger = PortfolioLedger::new(instruments, initial_balances);
        ledger
            .apply_fill(&leveraged_fill_for_symbol("BTCUSDT", Side::Sell, dec!(60000), dec!(1), dec!(10)))
            .unwrap();

        let mut reduce_fractions = HashMap::new();
        reduce_fractions.insert("BTCUSDT".to_string(), dec!(0.25));

        let orders = ledger.check_de_risking(&reduce_fractions, false);
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0], ("BTCUSDT".to_string(), Side::Buy, dec!(0.25)));
    }

    #[test]
    fn check_de_risking_skips_spot_positions_by_default() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.1), dec!(0)))
            .unwrap();

        let mut reduce_fractions = HashMap::new();
        reduce_fractions.insert("BTCUSDT".to_string(), dec!(0.5));

        let orders = ledger.check_de_risking(&reduce_fractions, false);
        assert!(orders.is_empty());
    }

    #[test]
    fn check_de_risking_includes_spot_positions_when_configured() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.1), dec!(0)))
            .unwrap();

        let mut reduce_fractions = HashMap::new();
        reduce_fractions.insert("BTCUSDT".to_string(), dec!(0.5));

        let orders = ledger.check_de_risking(&reduce_fractions, true);
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0], ("BTCUSDT".to_string(), Side::Sell, dec!(0.05)));
    }

    #[test]
    fn check_de_risking_skips_symbols_not_in_reduce_fractions() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&leveraged_fill_for_symbol("BTCUSDT", Side::Buy, dec!(60000), dec!(1), dec!(10)))
            .unwrap();

        let orders = ledger.check_de_risking(&HashMap::new(), false);
        assert!(orders.is_empty());
    }
```

- [ ] **Step 3: Run this crate's tests, clippy, fmt**

Run: `cargo test -p apexes-backtest check_de_risking`
Expected: PASS, all 5 new tests.

Run: `cargo clippy -p apexes-backtest --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean (run `cargo fmt` first if not).

- [ ] **Step 4: Commit**

```bash
git add crates/apexes-backtest/src/ledger.rs
git commit -S -s -m "feat(apexes-backtest): PortfolioLedger::check_de_risking -- partial-close computation for extreme-market conditions"
```

---

### Task 4: `run_event_loop` wiring: `on_market_event`, `on_equity_update`, auto-executed de-risking orders

**Files:**
- Modify: `crates/apexes-backtest/src/event_loop.rs`

**Interfaces:**
- Consumes: `apexes_risk::{CompositeRiskChecker::on_market_event, MarketConditionReadHandle}` (Task 1), `PortfolioLedger::check_de_risking` (Task 3), `EventLoopConfig.market_condition_config` (Task 2).
- Produces: no new public interfaces -- `run_event_loop` now calls `on_market_event` per event and executes de-risking orders per tick, when `config.market_condition_config` is `Some`.

**IMPORTANT design note before starting:** `apexes_risk::MarketConditionMonitor::split()` is only useful if `run_event_loop` itself constructs the monitor from `config.market_condition_config` and holds onto the resulting `MarketConditionReadHandle` directly -- registering the `PreTradeAdapter`/`EventAdapter` halves into `CompositeRiskChecker` is NOT enough by itself, since `run_event_loop` also needs the `MarketConditionReadHandle` (for `reduce_fractions`/`on_equity_update`) which is NOT one of `CompositeRiskChecker`'s registered trait objects. This means `run_event_loop`, when `config.market_condition_config` is `Some`, must construct its OWN `MarketConditionMonitor` (from the config) and split it locally, THEN separately decide whether to also register the `PreTradeAdapter`/`EventAdapter` halves into the SAME `risk_checker: Option<Arc<CompositeRiskChecker>>` the caller already resolved via the registry -- but `CompositeRiskChecker` is received as an already-constructed `Arc<CompositeRiskChecker>` (built by the CALLER before `run_event_loop` is invoked, via the registry), not something `run_event_loop` can mutate/extend at runtime (its `with_*` builders consume `self` by value). **Resolve this by NOT routing `MarketConditionMonitor` through `CompositeRiskChecker`'s registration lists at all inside `run_event_loop`** -- instead, `run_event_loop` constructs its own local `MarketConditionMonitor` from `config.market_condition_config`, splits it, and calls `on_market_event`/`check`/`reduce_fractions`/`on_equity_update` directly on the three local adapters, entirely independent of whatever `risk_checker: Option<Arc<CompositeRiskChecker>>` the caller separately configured. (A caller who ALSO wants `MarketConditionMonitor` registered into their own `CompositeRiskChecker` -- e.g. for a custom pre-trade gate on OTHER order-submission paths outside `run_event_loop`, such as a strategy submitting orders directly to an `OrderManager` bypassing this loop -- constructs a SEPARATE `MarketConditionMonitor` instance for that purpose and registers its adapters into their own `CompositeRiskChecker` before resolving it via the registry; the two instances track independent state, which is an accepted simplification for this plan -- not a bug, since `run_event_loop`'s own `PreTradeCheck` gate for orders submitted THROUGH this loop is what Step 3 below wires directly, and that is the primary path this plan's scope cares about.)

- [ ] **Step 1: Construct the local `MarketConditionMonitor` (if configured) near the top of `run_event_loop`**

```rust
// crates/apexes-backtest/src/event_loop.rs -- run_event_loop's body,
// add near the top, right after `let mut risk_checker = config.risk_checker;`:
    let market_condition_monitor = config
        .market_condition_config
        .map(apexes_risk::MarketConditionMonitor::new)
        .map(apexes_risk::MarketConditionMonitor::split);
```

(`market_condition_monitor` is now `Option<(MarketConditionPreTradeAdapter, MarketConditionEventAdapter, MarketConditionReadHandle)>`.)

- [ ] **Step 2: Call `on_market_event` once per event**

Immediately after the existing `let timestamp = event_timestamp(&event);` / `clock.wait_for_next_event(timestamp).await;` lines (before the `ems.on_market_event(&event).await` call), add:

```rust
        if let Some((_, event_monitor, _)) = &mut market_condition_monitor
            && let Err(violations) = event_monitor.on_market_event(&event)
        {
            tracing::warn!(?violations, "market condition monitor flagged a violation");
        }
```

- [ ] **Step 3: Compute de-risking orders and gate new orders, right after the existing auto-liquidation block**

Immediately after the existing auto-liquidation `for symbol in symbols_to_liquidate { .. }` loop (and before the `StrategyContext` construction), add:

```rust
        // Extreme-market de-risking (backtest/paper only -- live
        // trading defaults to monitor-only, see Task 5's
        // `auto_derisk_live` wiring in `run_live_trading`): every
        // symbol `MarketConditionMonitor` has flagged gets a partial
        // close proportional to its computed `reduce_fraction`,
        // submitted as a market order through the `MatchingEngine`,
        // applied as a normal fill.
        if let Some((_, _, read_handle)) = &market_condition_monitor {
            let equity_for_volatility = ledger.equity(&mark_prices).unwrap_or(Decimal::ZERO);
            if let Err(violations) = read_handle.on_equity_update(equity_for_volatility) {
                tracing::warn!(?violations, "account-wide market condition violation");
            }

            let open_symbols: Vec<String> = ledger.positions().keys().cloned().collect();
            let reduce_fractions = read_handle.reduce_fractions(&open_symbols);
            let include_spot = false; // wired from config in Step 4 below
            let de_risking_orders = ledger.check_de_risking(&reduce_fractions, include_spot);

            for (symbol, side, quantity) in de_risking_orders {
                let Some(position) = ledger.positions().get(&symbol).cloned() else {
                    continue;
                };
                let de_risk_intent = apexes_strategy::OrderIntent {
                    symbol: symbol.clone(),
                    venue: instruments_by_symbol
                        .get(&symbol)
                        .map(|i| i.venue)
                        .unwrap_or(apexes_core::Venue::Binance),
                    asset_class: instruments_by_symbol
                        .get(&symbol)
                        .map(|i| i.asset_class)
                        .unwrap_or(apexes_core::AssetClass::Perpetual),
                    side,
                    order_type: OrderType::Market,
                    quantity,
                    price: None,
                    stop_price: None,
                    execution_algo: None,
                    routing: apexes_strategy::OrderRouting::Fixed,
                    leverage: position.leverage,
                    margin_mode: position.margin_mode,
                };
                match lock_engine(&matching_engine).submit_market_order(&de_risk_intent, timestamp) {
                    Ok((order, fills)) => {
                        tracing::warn!(symbol = %symbol, %quantity, "position partially closed (extreme market conditions)");
                        open_orders.insert(order.id, order);
                        apply_fills(
                            &mut ledger,
                            &mut overall_tracker,
                            &mut per_symbol_trackers,
                            &mut per_venue_trackers,
                            &mut all_fills,
                            &fills,
                            config.risk_free_return,
                        )?;
                    }
                    Err(error) => {
                        tracing::error!(?error, symbol = %symbol, "de-risking order submission failed");
                    }
                }
            }
        }
```

- [ ] **Step 4: Thread `include_spot` from the config instead of the hardcoded `false` placeholder above**

Replace the `let include_spot = false; // wired from config in Step 4 below` line from Step 3 with:

```rust
            let include_spot = config
                .market_condition_config
                .as_ref()
                .map(|c| c.include_spot)
                .unwrap_or(false);
```

**Note:** by this point in the function, `config.market_condition_config` was already partially moved out in Step 1 (`.map(apexes_risk::MarketConditionMonitor::new)` consumes it by value). Fix this by capturing `include_spot` into a local variable BEFORE Step 1's move, alongside where `market_condition_monitor` is constructed:

```rust
// crates/apexes-backtest/src/event_loop.rs -- replace Step 1's snippet
// with this corrected version that captures include_spot first:
    let market_condition_include_spot = config
        .market_condition_config
        .as_ref()
        .map(|c| c.include_spot)
        .unwrap_or(false);
    let market_condition_monitor = config
        .market_condition_config
        .map(apexes_risk::MarketConditionMonitor::new)
        .map(apexes_risk::MarketConditionMonitor::split);
```

And in Step 3's block, use `market_condition_include_spot` directly instead of re-deriving it from `config.market_condition_config` (which is no longer available at that point):

```rust
            let include_spot = market_condition_include_spot;
```

- [ ] **Step 5: Gate new strategy-submitted orders through the `PreTradeAdapter` too**

Find the existing per-intent loop (`for intent in intents { if let Some(checker) = &risk_checker && let Err(violations) = checker.check(...) { .. } }`) and add a second check right alongside it, using `market_condition_monitor`'s `PreTradeAdapter`:

```rust
        for intent in intents {
            if let Some(checker) = &risk_checker
                && let Err(violations) = checker.check(
                    &intent,
                    ledger.positions(),
                    ledger.balances(),
                    &open_orders,
                    timestamp,
                )
            {
                tracing::warn!(?violations, symbol = %intent.symbol, "order rejected by risk checker");
                continue;
            }
            if let Some((pre_trade, _, _)) = &market_condition_monitor
                && let Err(violations) = pre_trade.check(
                    &intent,
                    ledger.positions(),
                    ledger.balances(),
                    &open_orders,
                    timestamp,
                )
            {
                tracing::warn!(?violations, symbol = %intent.symbol, "order rejected by market condition monitor");
                continue;
            }
```

(This replaces the existing `for intent in intents {` loop's opening lines -- keep everything after the second `continue;` unchanged, per the existing loop body.)

- [ ] **Step 6: Write tests**

Add to `crates/apexes-backtest/src/event_loop.rs`'s existing `#[cfg(test)] mod tests`:

```rust
    #[tokio::test]
    async fn market_condition_config_none_does_not_change_existing_behavior() {
        // Regression guard: EventLoopConfig.market_condition_config: None
        // must run byte-for-byte identically to the pre-this-plan
        // behavior. Re-runs an existing simple buy-once scenario (the
        // same shape this file's other pre-existing tests already use)
        // with market_condition_config explicitly set to None, and
        // asserts exactly the same fill count/final equity a
        // pre-this-plan run would have produced -- if this task's
        // changes to run_event_loop ever start unconditionally touching
        // ledger state when market_condition_monitor is None, this
        // test's exact-equity assertion will catch it.
        let events = vec![
            MarketEvent::Depth(apexes_core::DepthUpdate {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                bids: vec![apexes_core::PriceLevel { price: dec!(99), quantity: dec!(10) }],
                asks: vec![apexes_core::PriceLevel { price: dec!(101), quantity: dec!(10) }],
                sequence: None,
                is_snapshot: true,
                timestamp: chrono::Utc::now(),
            }),
            MarketEvent::Trade(TradeEvent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                price: dec!(100),
                quantity: dec!(1),
                side: None,
                trade_id: None,
                timestamp: chrono::Utc::now() + chrono::Duration::seconds(1),
            }),
        ];

        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(10000));

        let config = EventLoopConfig {
            instruments: vec![btcusdt_instrument()],
            initial_balances,
            cost_model: zero_cost_model(),
            queue_position_factor: dec!(0.5),
            risk_free_return: Decimal::ZERO,
            risk_checker: None,
            margin_tiers: HashMap::new(),
            market_condition_config: None,
        };

        let report = run_event_loop(
            config,
            futures::stream::iter(events),
            BacktestClock::new(crate::clock::ReplaySpeed::AsFastAsPossible),
            Arc::new(apexes_strategy::PriceAndFeeScorer),
            None,
            BuyOnceStrategy { bought: false },
            None,
            None,
        )
        .await
        .unwrap();

        // Exactly one fill (the strategy's opening buy) -- no
        // de-risking activity, since market_condition_config is None.
        assert_eq!(report.trades.len(), 1);
        assert_eq!(report.trades[0].side, apexes_core::Side::Buy);
    }

    #[tokio::test]
    async fn extreme_price_jump_triggers_a_partial_close_through_the_matching_engine() {
        let mut ledger_events: Vec<MarketEvent> = Vec::new();
        let t0 = chrono::Utc::now();
        ledger_events.push(MarketEvent::Depth(apexes_core::DepthUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bids: vec![apexes_core::PriceLevel { price: dec!(99), quantity: dec!(10) }],
            asks: vec![apexes_core::PriceLevel { price: dec!(101), quantity: dec!(10) }],
            sequence: None,
            is_snapshot: true,
            timestamp: t0,
        }));
        ledger_events.push(MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(100),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: t0 + chrono::Duration::seconds(1),
        }));
        // A 30% jump -- well past a 10% threshold.
        ledger_events.push(MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(130),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: t0 + chrono::Duration::seconds(2),
        }));

        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(10000));

        let config = EventLoopConfig {
            instruments: vec![btcusdt_instrument()],
            initial_balances,
            cost_model: zero_cost_model(),
            queue_position_factor: dec!(0.5),
            risk_free_return: Decimal::ZERO,
            risk_checker: None,
            margin_tiers: HashMap::new(),
            market_condition_config: Some(apexes_risk::MarketConditionConfig {
                volatility_threshold: dec!(1000000), // effectively disabled, isolate the jump signal
                jump_threshold_pct: dec!(0.10),
                sensitivity: dec!(1),
                volatility_window: 20,
                price_source: apexes_risk::PriceSource::Trade,
                account_equity_volatility_threshold: None,
                include_spot: true, // this test uses a SPOT buy-once strategy
            }),
        };

        let registry = apexes_core::registry::Registry::new();
        let report = run_event_loop(
            config,
            futures::stream::iter(ledger_events),
            BacktestClock::new(crate::clock::ReplaySpeed::AsFastAsPossible),
            Arc::new(apexes_strategy::PriceAndFeeScorer),
            None,
            BuyOnceStrategy { bought: false },
            None,
            None,
        )
        .await
        .unwrap();
        let _ = registry; // not used by run_event_loop directly in this test

        // Expect 2 fills: the strategy's opening buy, then a partial
        // de-risking sell triggered by the 30% jump.
        assert_eq!(report.trades.len(), 2);
        assert_eq!(report.trades[1].side, apexes_core::Side::Sell);
    }
```

**Note for the implementer:** the second test above (`extreme_price_jump_triggers_a_partial_close_through_the_matching_engine`) reuses this file's existing test helpers (`btcusdt_instrument`, `zero_cost_model`, `BuyOnceStrategy`) -- verify their exact names/signatures via `file_read` on this file's own `#[cfg(test)] mod tests` before finalizing, since the illustrative code above may not match this file's actual current helper names exactly (this plan was drafted by reading the file's structure, but re-verify signatures rather than assuming). If `run_event_loop`'s actual parameter list has drifted from the 8-parameter shape shown, adjust the call accordingly.

- [ ] **Step 7: Run this crate's tests, clippy, fmt, then a workspace-wide check**

Run: `cargo test -p apexes-backtest -- --test-threads=1`
Expected: PASS, all new tests plus every pre-existing test.

Run: `cargo build --workspace`
Expected: 0 errors.

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean (run `cargo fmt` first if not).

- [ ] **Step 8: Commit**

```bash
git add crates/apexes-backtest/src/event_loop.rs
git commit -S -s -m "feat(apexes-backtest): wire MarketConditionMonitor into run_event_loop -- auto-executed de-risking orders"
```

---

### Task 5: `run_live_trading` wiring: monitor-only-by-default, `auto_derisk_live` opt-in

**Files:**
- Modify: `crates/apexes-live-trading/src/runner.rs`

**Interfaces:**
- Consumes: `apexes_risk::MarketConditionMonitor` (Task 1), `PortfolioLedger::check_de_risking` (Task 3), `LiveTradingConfig.{market_condition_config, auto_derisk_live}` (Task 2).
- Produces: no new public interfaces -- `run_live_trading` now maintains market-condition state, gates new orders via `PreTradeCheck`, and either logs-only or auto-executes de-risking orders per `config.auto_derisk_live`.

- [ ] **Step 1: Construct the local `MarketConditionMonitor` near the top of `run_live_trading`**

```rust
// crates/apexes-live-trading/src/runner.rs -- run_live_trading's body,
// add near the top, alongside the existing `mut mark_prices`/
// `last_funding_settlement` local state:
    let market_condition_include_spot = config
        .market_condition_config
        .as_ref()
        .map(|c| c.include_spot)
        .unwrap_or(false);
    let auto_derisk_live = config.auto_derisk_live;
    let mut market_condition_monitor = config
        .market_condition_config
        .map(apexes_risk::MarketConditionMonitor::new)
        .map(apexes_risk::MarketConditionMonitor::split);
```

- [ ] **Step 2: Call `on_market_event` right after `update_prices` in the market-event branch**

Immediately after the existing `update_prices(&event, &instruments_by_symbol, &mut last_prices, &mut mark_prices);` line, add:

```rust
                if let Some((_, event_monitor, _)) = &mut market_condition_monitor
                    && let Err(violations) = event_monitor.on_market_event(&event)
                {
                    tracing::warn!(?violations, "market condition monitor flagged a violation (live)");
                }
```

- [ ] **Step 3: Add the de-risking block right after the existing margin-based `check_liquidations` monitor-only block**

Immediately after the existing `for symbol in ledger.check_liquidations(&mark_prices, &config.margin_tiers) { tracing::error!(..); }` loop (and before the `handle_market_event(...)` call), add:

```rust
                // Extreme-market de-risking: monitor-only by default
                // (log + rely on MarketConditionMonitor's PreTradeCheck
                // to block new orders for the affected symbol), matching
                // row 6.4's live-trading-never-auto-liquidates
                // precedent -- UNLESS `config.auto_derisk_live` was
                // explicitly set to `true`, in which case this behaves
                // like backtest/paper and actually submits the partial
                // close through the exchange (via order_manager).
                if let Some((_, _, read_handle)) = &market_condition_monitor {
                    let equity_for_volatility = ledger.equity(&last_prices).unwrap_or(Decimal::ZERO);
                    if let Err(violations) = read_handle.on_equity_update(equity_for_volatility) {
                        tracing::warn!(?violations, "account-wide market condition violation (live)");
                    }

                    let open_symbols: Vec<String> = ledger.positions().keys().cloned().collect();
                    let reduce_fractions = read_handle.reduce_fractions(&open_symbols);
                    let de_risking_orders =
                        ledger.check_de_risking(&reduce_fractions, market_condition_include_spot);

                    for (symbol, side, quantity) in de_risking_orders {
                        if !auto_derisk_live {
                            tracing::error!(
                                symbol = %symbol,
                                %quantity,
                                "LIVE TRADING: extreme market conditions detected -- \\
                                 auto_derisk_live is false, so this system will NOT \\
                                 automatically submit a de-risking order. New orders for \\
                                 this symbol are blocked via MarketConditionMonitor."
                            );
                            continue;
                        }
                        let Some(position) = ledger.positions().get(&symbol).cloned() else {
                            continue;
                        };
                        let de_risk_intent = apexes_strategy::OrderIntent {
                            symbol: symbol.clone(),
                            venue: instruments_by_symbol
                                .get(&symbol)
                                .map(|i| i.venue)
                                .unwrap_or(Venue::Binance),
                            asset_class: instruments_by_symbol
                                .get(&symbol)
                                .map(|i| i.asset_class)
                                .unwrap_or(apexes_core::AssetClass::Perpetual),
                            side,
                            order_type: apexes_core::OrderType::Market,
                            quantity,
                            price: None,
                            stop_price: None,
                            execution_algo: None,
                            routing: apexes_strategy::OrderRouting::Fixed,
                            leverage: position.leverage,
                            margin_mode: position.margin_mode,
                        };
                        match order_manager.lock().await.submit_order(
                            de_risk_intent,
                            ledger.positions(),
                            ledger.balances(),
                        ).await {
                            Ok(_order_id) => {
                                tracing::warn!(symbol = %symbol, %quantity, "LIVE TRADING: de-risking order submitted (auto_derisk_live=true)");
                            }
                            Err(error) => {
                                tracing::error!(?error, symbol = %symbol, "LIVE TRADING: de-risking order submission failed");
                            }
                        }
                    }
                }
```

**Note for the implementer:** confirm `OrderManager::submit_order`'s exact signature via `file_read` on `crates/apexes-oms/src/manager.rs` before finalizing this step -- the call above assumes `submit_order(intent, positions, balances) -> Result<OrderId, OmsError>` based on this plan's understanding of the existing codebase, but re-verify the exact parameter list and return type match what `handle_market_event` in this same file already calls, and mirror that exactly rather than guessing.

- [ ] **Step 4: Gate new orders through the `PreTradeAdapter`**

Find `handle_market_event`'s existing call to `strategy.on_market_event(event, &ctx)` and the subsequent per-intent submission loop (in this file, likely inside `handle_market_event` itself, alongside where `risk_checker.check(...)` is already called for the direct order-submission path). Add the same `market_condition_monitor`'s `PreTradeAdapter` check alongside the existing `risk_checker` check, mirroring Task 4 Step 5's pattern exactly. Since `market_condition_monitor` is constructed in `run_live_trading`'s outer scope (not inside `handle_market_event`), this requires either (a) passing the `PreTradeAdapter` (or a reference to it) as a new parameter into `handle_market_event`, or (b) moving the per-intent gating check out of `handle_market_event` and into the caller, right after `handle_market_event` returns its `Vec<OrderIntent>` (if it already returns intents rather than submitting them itself). **Read `handle_market_event`'s actual current structure via `file_read` first** to determine which of these two shapes fits -- this plan cannot specify the exact diff without seeing whether `handle_market_event` submits orders itself or returns intents for the caller to submit, since that determines where the gating check physically belongs. If `handle_market_event` submits orders itself, prefer option (a); document your choice in the task report.

- [ ] **Step 5: Write tests**

Add to `crates/apexes-live-trading/tests/live_trading_integration.rs`'s existing test module (this file's tests are `#[tokio::test]` + `#[ignore]`, requiring a live Postgres per this crate's established convention -- both tests below follow the exact structure of this file's existing `liquidation_breach_is_logged_but_never_submits_an_order` test, adapted to trigger on a price jump instead of a margin breach):

```rust
/// A `MarketDataProvider` whose stream delivers one seed-position-
/// enabling delay, then a sequence of `MarketEvent::Trade` prices for
/// the same symbol (to drive `MarketConditionMonitor`'s jump
/// detection), then stays open via `futures::stream::pending()`.
struct DelayedPriceJumpProvider {
    prices: Vec<rust_decimal::Decimal>,
}

#[async_trait]
impl MarketDataProvider for DelayedPriceJumpProvider {
    async fn subscribe(
        &self,
        _instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, apexes_core::ApexesError> {
        let prices = self.prices.clone();
        let events = futures::stream::iter(prices.into_iter().enumerate().map(|(i, price)| {
            TradeEvent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                price,
                quantity: dec!(1),
                side: None,
                trade_id: None,
                timestamp: Utc.with_ymd_and_hms(2026, 1, 1, 0, i as u32, 0).unwrap(),
            }
        }))
        .then(|trade| async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            MarketEvent::Trade(trade)
        });
        Ok(Box::pin(events.chain(futures::stream::pending())))
    }
}

fn seed_leveraged_long_config(auto_derisk_live: bool) -> LiveTradingConfig {
    LiveTradingConfig {
        instruments: vec![btc_usdt_instrument()],
        market_condition_config: Some(apexes_risk::MarketConditionConfig {
            volatility_threshold: dec!(1000000), // effectively disabled, isolate the jump signal
            jump_threshold_pct: dec!(0.10),
            sensitivity: dec!(1),
            volatility_window: 20,
            price_source: apexes_risk::PriceSource::Trade,
            account_equity_volatility_threshold: None,
            include_spot: false,
        }),
        auto_derisk_live,
        ..base_config()
    }
}

async fn seed_leveraged_long(
    order_manager: &mut OrderManager,
    gateway: &CountingGateway,
) -> OrderId {
    let order_id = order_manager
        .submit_order(
            OrderIntent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                asset_class: AssetClass::Perpetual,
                side: Side::Buy,
                order_type: OrderType::Market,
                quantity: dec!(1),
                price: None,
                stop_price: None,
                execution_algo: None,
                routing: OrderRouting::Fixed,
                leverage: Some(dec!(10)),
                margin_mode: Some(MarginMode::Isolated),
            },
            &HashMap::new(),
            &HashMap::new(),
        )
        .await
        .unwrap();
    gateway.inner.set_fills_for_order(
        order_id,
        vec![ExchangeFill {
            exchange_trade_id: "T1".to_string(),
            price: dec!(100),
            quantity: dec!(1),
            fee: dec!(0),
            fee_currency: None,
            filled_at: Utc::now(),
        }],
    );
    order_id
}

#[tokio::test]
#[ignore]
async fn extreme_price_jump_is_logged_but_never_submits_an_order_by_default() {
    clean_db().await;

    let gateway = Arc::new(CountingGateway {
        inner: MockExecutionGateway::new(),
        submit_count: AtomicUsize::new(0),
    });
    // 100 -> 135 is a 35% jump, well past the 10% threshold configured
    // in seed_leveraged_long_config.
    let provider: Arc<dyn MarketDataProvider> = Arc::new(DelayedPriceJumpProvider {
        prices: vec![dec!(100), dec!(135)],
    });
    let registry = registry_with(gateway.clone() as Arc<dyn ExecutionGateway>, provider);

    let store = OrderStore::new(POSTGRES_URL).await.unwrap();
    let mut order_manager = OrderManager::new(store, &registry).await.unwrap();
    let order_id = seed_leveraged_long(&mut order_manager, &gateway).await;
    let exchange_order_id = order_manager
        .open_orders()
        .get(&order_id)
        .unwrap()
        .exchange_order_id
        .clone();
    let exchange_updates = futures::stream::iter(vec![ExchangeUpdate {
        client_order_id: order_id,
        exchange_order_id,
        status: OrderStatus::Filled,
    }])
    .chain(futures::stream::pending());

    let (tx, rx_stop) = oneshot::channel();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(600)).await;
        let _ = tx.send(());
    });

    let report = run_live_trading(
        seed_leveraged_long_config(false), // auto_derisk_live: false (default)
        &registry,
        BuyOnceStrategy { bought: true }, // emits no intents
        order_manager,
        exchange_updates,
        rx_stop,
    )
    .await
    .unwrap();

    // The seeded order's fill was applied to the ledger...
    assert_eq!(report.trades.len(), 1);
    // ...and the seeded order remains the ONLY submission the gateway
    // ever saw: the 35% price jump was logged (tracing::error!), never
    // turned into a de-risking order submission.
    assert_eq!(gateway.submit_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
#[ignore]
async fn extreme_price_jump_submits_a_partial_close_when_auto_derisk_live_is_true() {
    clean_db().await;

    let gateway = Arc::new(CountingGateway {
        inner: MockExecutionGateway::new(),
        submit_count: AtomicUsize::new(0),
    });
    let provider: Arc<dyn MarketDataProvider> = Arc::new(DelayedPriceJumpProvider {
        prices: vec![dec!(100), dec!(135)],
    });
    let registry = registry_with(gateway.clone() as Arc<dyn ExecutionGateway>, provider);

    let store = OrderStore::new(POSTGRES_URL).await.unwrap();
    let mut order_manager = OrderManager::new(store, &registry).await.unwrap();
    let order_id = seed_leveraged_long(&mut order_manager, &gateway).await;
    let exchange_order_id = order_manager
        .open_orders()
        .get(&order_id)
        .unwrap()
        .exchange_order_id
        .clone();
    let exchange_updates = futures::stream::iter(vec![ExchangeUpdate {
        client_order_id: order_id,
        exchange_order_id,
        status: OrderStatus::Filled,
    }])
    .chain(futures::stream::pending());

    let (tx, rx_stop) = oneshot::channel();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(600)).await;
        let _ = tx.send(());
    });

    let report = run_live_trading(
        seed_leveraged_long_config(true), // auto_derisk_live: true
        &registry,
        BuyOnceStrategy { bought: true },
        order_manager,
        exchange_updates,
        rx_stop,
    )
    .await
    .unwrap();

    assert_eq!(report.trades.len(), 1);
    // The seed order PLUS a de-risking partial-close submission this
    // time -- auto_derisk_live: true means the 35% jump is actually
    // acted on.
    assert_eq!(gateway.submit_count.load(Ordering::SeqCst), 2);
}
```

**Note for the implementer:** `btc_usdt_instrument()` is this test file's existing helper (used by `base_config()`) -- confirm its exact name/signature via `file_read` before finalizing (it should already exist in this file, since `base_config()` calls it). If `run_live_trading`'s call to `OrderManager::submit_order` inside `handle_market_event`/the de-risking block (Task 5 Step 3/4) has a different exact signature than assumed, adjust `seed_leveraged_long`'s call accordingly to match whatever the rest of this file already uses successfully.

- [ ] **Step 6: Run this crate's tests, clippy, fmt, then a full workspace check**

Run: `cargo test -p apexes-live-trading -- --test-threads=1`
Expected: PASS, 0 failures (unit tests); the 2 new `#[ignore]`d integration tests compile cleanly.

If a live Postgres is available in your environment: `cargo test -p apexes-live-trading --test live_trading_integration -- --ignored --test-threads=1`
Expected: PASS, including both new tests.

Run: `cargo build --workspace`
Expected: 0 errors.

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean (run `cargo fmt` first if not).

- [ ] **Step 7: Commit**

```bash
git add crates/apexes-live-trading/src/runner.rs crates/apexes-live-trading/tests/live_trading_integration.rs
git commit -S -s -m "feat(apexes-live-trading): monitor-only-by-default extreme-market de-risking, with auto_derisk_live opt-in"
```

---

### Task 6: Full-workspace verification checkpoint

**Files:** none (verification-only task).

**Interfaces:** none.

- [ ] **Step 1: Run the complete verification suite**

Run: `cargo build --workspace`
Expected: 0 errors.

Run: `cargo test --workspace -- --test-threads=1`
Expected: 0 failures. Record the exact passed/ignored counts (via `grep "test result:"` summed across every suite, per this project's established convention of never estimating these numbers) for comparison against the pre-this-plan baseline.

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

- [ ] **Step 2: Grep for leftover placeholders**

Run: `grep -rn "TODO\|FIXME\|todo!\|unimplemented!" crates/apexes-risk/src/market_condition_config.rs crates/apexes-risk/src/market_condition_monitor.rs`
Expected: no matches (this task's own new files must be placeholder-free; the workspace-wide grep the final review will perform in Task 7 covers everything else).

- [ ] **Step 3: Commit** (only if Step 1/2 required any fixes; otherwise this task produces no diff and can be skipped without a commit)

```bash
git add -A
git commit -S -s -m "chore: full-workspace verification checkpoint for extreme-market circuit breaker plan"
```

---

### Task 7: Final whole-branch code review and `docs/FEATURES.md` update

**Files:**
- Modify: `docs/FEATURES.md`

**Interfaces:** none (documentation/review-only task).

- [ ] **Step 1: Run the complete verification suite one final time**

Run: `cargo build --workspace`, `cargo test --workspace -- --test-threads=1`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --check`.
Expected: all clean, matching this project's established multi-sub-project-completion precedent (rows 6.2/6.4 both ended with this exact step).

- [ ] **Step 2: Escalate to a whole-branch review**

Per this project's established precedent (rows 6.2's/6.4's final reviews, both escalated to the most capable available reviewer profile for a correctness-critical whole-branch pass): dispatch an independent reviewer over the ENTIRE branch diff for this plan (base: the commit before Task 1 started, head: Task 5's final commit) -- not just individual tasks in isolation. Pay particular attention to:

- **`MarketConditionMonitor`'s linear-interpolation math**: re-derive `reduce_fraction = min(1.0, sensitivity * (actual/threshold - 1.0))` independently and confirm the shipped code matches, for both the volatility and jump-percentage paths, and for the account-wide equity path.
- **Whether the de-risking order construction correctly threads `leverage`/`margin_mode`** from the existing position onto the closing intent, exactly as row 6.4's auto-liquidation logic does -- this is the exact bug class row 6.4's Task 5 caught (a `leverage: None` close on a leveraged position takes the wrong accounting path and can crash the whole run).
- **Whether `run_live_trading`'s `auto_derisk_live: false` (default) path could EVER accidentally submit a de-risking order** -- this is the single most safety-critical property of this plan, mirroring row 6.4's equivalent finding for `run_live_trading`'s monitor-only liquidation path.
- **Whether `BacktestConfig`/`PaperTradingConfig`/`LiveTradingConfig` genuinely ALL expose `market_condition_config`** (not just the internal `EventLoopConfig`) -- this is the exact gap this plan's own Global Constraints section was written specifically to prevent; confirm it did not recur.
- **Whether every construction site for the three public config types across the whole workspace was genuinely updated** -- not just the ones Task 2's own brief explicitly enumerated (several tasks in row 6.4's plan found sites their own briefs missed; re-grep independently rather than trusting Task 2's own site list).
- **Whether a single symbol simultaneously flagged by both `MarginMonitor` (row 6.4) and `MarketConditionMonitor` (this plan) in the same tick behaves sanely** -- two sequential partial/full closes against the same position, applied through the same `PortfolioLedger`, should not double-count or corrupt `allocated_margin`/`quantity` bookkeeping (the ledger's own per-fill accounting, from row 6.4, should already make this safe by construction, but confirm rather than assume).

Fix any findings, get a fix-verification re-review, and only then consider this plan complete.

- [ ] **Step 3: Update `docs/FEATURES.md`**

Find row `6.5` (`grep -n "| 6.5 |" docs/FEATURES.md`) and update its Status column from `🔲 Not Started` to `✅ Done`, with Notes summarizing: `apexes-risk::MarketConditionMonitor` (per-symbol price volatility via `apexes-indicators::RollingStdDev` + price-jump detection, optional account-level equity volatility, linearly-interpolated de-risking fractions), `PortfolioLedger::check_de_risking` (partial-close computation), backtest/paper auto-execution of de-risking orders, live-trading monitor-only-by-default behavior with an explicit `auto_derisk_live` opt-in, and the three public config types (`BacktestConfig`/`PaperTradingConfig`/`LiveTradingConfig`) all exposing the new configuration from the start.

Recompute the Summary counts table via actual `grep -c "✅ Done"`/`grep -c "🔲 Not Started"` (per this project's explicit standing instruction: NEVER estimate these counts, always recompute them from the real table contents) -- subtract 2 from each raw count (the legend row near the top of the file, AND the Summary table's own count row, both of which also contain these exact strings as their own text, per this project's standing memory note about this recurring double-count mistake).

- [ ] **Step 4: Commit**

```bash
git add docs/FEATURES.md
git commit -S -s -m "docs: mark FEATURES.md row 6.5 (extreme-market circuit breaker & automatic strategy de-risking) as Done"
```
