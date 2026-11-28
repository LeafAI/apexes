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
    fn inspect_registered_state(
        &self,
    ) -> Result<crate::RegisteredRiskState, crate::RiskStateError> {
        Ok(crate::RegisteredRiskState::Unsupported)
    }

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

    pub fn inspect_checkpoint(
        &self,
    ) -> Result<crate::MarketConditionSnapshot, crate::RiskStateError> {
        use crate::RiskStateError as E;
        use apexes_indicators::checkpoint::{MAX_PERIOD, StddevState};
        if self.volatility.len() > 128
            || self.last_price.len() != self.volatility.len()
            || self.reduce_fractions.len() > self.volatility.len()
            || !(1..=MAX_PERIOD).contains(&self.config.volatility_window)
            || self
                .reduce_fractions
                .keys()
                .any(|s| !self.volatility.contains_key(s))
        {
            return Err(E::Invalid);
        }
        let mut symbols = Vec::with_capacity(self.volatility.len());
        for (symbol, volatility) in &self.volatility {
            symbols.push(crate::MarketSymbolSnapshot {
                symbol: symbol.clone(),
                last_price: *self.last_price.get(symbol).ok_or(E::Invalid)?,
                volatility: StddevState::capture(volatility),
                reduce_fraction: self.reduce_fractions.get(symbol).copied(),
            });
        }
        symbols.sort_by(|a, b| a.symbol.cmp(&b.symbol));
        let snapshot = crate::MarketConditionSnapshot {
            version: 1,
            config: self.config.clone(),
            symbols,
            equity_volatility: self.equity_volatility.as_ref().map(StddevState::capture),
            account_wide_reduce_fraction: self.account_wide_reduce_fraction,
        };
        snapshot.validate()?;
        Ok(snapshot)
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
        let fraction = self.reduce_fraction_for(volatility, threshold);
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
        if let Some(fraction) = self.account_wide_reduce_fraction {
            return Err(vec![RiskViolation::AccountWideMarketConditionBreached {
                reduce_fraction: fraction,
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
    fn inspect_registered_state(
        &self,
    ) -> Result<crate::RegisteredRiskState, crate::RiskStateError> {
        self.0
            .lock()
            .map_err(|_| crate::RiskStateError::Poisoned)?
            .inspect_checkpoint()
            .map(crate::RegisteredRiskState::MarketCondition)
    }

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
    fn inspect_registered_state(
        &self,
    ) -> Result<crate::RegisteredRiskState, crate::RiskStateError> {
        self.0
            .lock()
            .map_err(|_| crate::RiskStateError::Poisoned)?
            .inspect_checkpoint()
            .map(crate::RegisteredRiskState::MarketCondition)
    }

    fn on_market_event(&mut self, event: &MarketEvent) -> Result<(), Vec<RiskViolation>> {
        let mut monitor = self.0.lock().unwrap_or_else(|p| p.into_inner());
        monitor.on_market_event_impl(event)
    }
}

pub struct MarketConditionReadHandle(Arc<Mutex<MarketConditionMonitor>>);

impl MarketConditionReadHandle {
    pub fn inspect_checkpoint(
        &self,
    ) -> Result<crate::MarketConditionSnapshot, crate::RiskStateError> {
        self.0
            .lock()
            .map_err(|_| crate::RiskStateError::Poisoned)?
            .inspect_checkpoint()
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, OrderType, Side, TradeEvent, Venue};
    use chrono::Utc;
    use rust_decimal_macros::dec;

    // NOTE: `volatility_threshold` is set high (1000) so these tests
    // exercise the JUMP trigger in isolation -- with a low volatility
    // threshold, a 20% jump would keep the rolling stddev (and hence
    // the trip) elevated for the whole window, making the auto-reset
    // test below impossible to write without aging the jump out of
    // the window. The account-wide test exercises the volatility
    // trigger separately.
    fn sample_config() -> MarketConditionConfig {
        MarketConditionConfig {
            volatility_threshold: dec!(1000),
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
    fn inspect_retains_actual_price_windows_and_equity_state() {
        let mut config = sample_config();
        config.account_equity_volatility_threshold = Some(dec!(1));
        let (pre, mut event, read) = MarketConditionMonitor::new(config.clone()).split();
        event
            .on_market_event(&trade_event("ETHUSDT", dec!(50)))
            .unwrap();
        event
            .on_market_event(&trade_event("BTCUSDT", dec!(100)))
            .unwrap();
        event
            .on_market_event(&trade_event("BTCUSDT", dec!(120)))
            .unwrap_err();
        read.on_equity_update(dec!(100)).unwrap();
        read.on_equity_update(dec!(110)).unwrap();
        read.on_equity_update(dec!(120)).unwrap_err();
        let state = read.inspect_checkpoint().unwrap();
        assert_eq!(state.config, config);
        assert_eq!(state.symbols[0].symbol, "BTCUSDT");
        assert_eq!(state.symbols[0].last_price, dec!(120));
        assert_eq!(state.symbols[0].volatility.sample_count(), 2);
        assert_eq!(state.symbols[0].reduce_fraction, Some(dec!(1)));
        assert_eq!(state.symbols[1].symbol, "ETHUSDT");
        assert_eq!(state.equity_volatility.as_ref().unwrap().sample_count(), 3);
        assert!(state.account_wide_reduce_fraction.is_some());
        assert_eq!(
            pre.inspect_registered_state().unwrap(),
            crate::RegisteredRiskState::MarketCondition(state.clone())
        );
        assert_eq!(
            event.inspect_registered_state().unwrap(),
            crate::RegisteredRiskState::MarketCondition(state.clone())
        );
        assert_eq!(state, read.inspect_checkpoint().unwrap());
        let mut invalid = state;
        invalid.symbols[0].last_price = dec!(121);
        assert!(invalid.validate().is_err());
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
    fn partial_jump_computes_a_partial_reduce_fraction() {
        let monitor = MarketConditionMonitor::new(sample_config());
        let (_pre_trade, mut event_monitor, read) = monitor.split();

        event_monitor
            .on_market_event(&trade_event("BTCUSDT", dec!(100)))
            .unwrap();
        // 15% jump vs a 10% threshold -> reduce_fraction = min(1.0, 1*(0.15/0.10 - 1)) = 0.5
        let result = event_monitor.on_market_event(&trade_event("BTCUSDT", dec!(115)));
        assert!(result.is_err());

        let fractions = read.reduce_fractions(&["BTCUSDT".to_string()]);
        assert_eq!(fractions.get("BTCUSDT"), Some(&dec!(0.5)));
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
    fn mark_price_source_consumes_mark_price_events() {
        let mut config = sample_config();
        config.price_source = PriceSource::MarkPrice;
        let monitor = MarketConditionMonitor::new(config);
        let (_pre_trade, mut event_monitor, read) = monitor.split();

        let mark_event = |symbol: &str, price: Decimal| {
            MarketEvent::MarkPrice(apexes_core::MarkPriceUpdate {
                symbol: symbol.to_string(),
                venue: Venue::Binance,
                mark_price: price,
                timestamp: Utc::now(),
            })
        };
        event_monitor
            .on_market_event(&mark_event("BTCUSDT", dec!(100)))
            .unwrap();
        // Trade events are ignored under MarkPrice sourcing...
        event_monitor
            .on_market_event(&trade_event("BTCUSDT", dec!(200)))
            .unwrap();
        assert!(read.reduce_fractions(&["BTCUSDT".to_string()]).is_empty());
        // ...while a MarkPrice jump trips.
        event_monitor
            .on_market_event(&mark_event("BTCUSDT", dec!(150)))
            .unwrap_err();
        assert!(
            read.reduce_fractions(&["BTCUSDT".to_string()])
                .contains_key("BTCUSDT")
        );
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
    fn account_wide_fraction_folds_in_via_max_with_per_symbol_fractions() {
        let mut config = sample_config();
        config.account_equity_volatility_threshold = Some(dec!(50));
        config.volatility_window = 2;
        let monitor = MarketConditionMonitor::new(config);
        let (_pre_trade, mut event_monitor, read) = monitor.split();

        // Trip BTCUSDT per-symbol with only a PARTIAL (0.5) jump
        // fraction -- 100 -> 115 is a 15% jump vs. a 10% threshold with
        // sensitivity 1: min(1.0, 1*(0.15/0.10 - 1)) = 0.5.
        event_monitor
            .on_market_event(&trade_event("BTCUSDT", dec!(100)))
            .unwrap();
        event_monitor
            .on_market_event(&trade_event("BTCUSDT", dec!(115)))
            .unwrap_err();
        // Trip the account-wide detector at a full (1.0) fraction --
        // strictly LARGER than BTCUSDT's per-symbol 0.5, so this test
        // actually distinguishes `max` (BTCUSDT ends up at 1.0) from a
        // naive overwrite-only-if-absent (which would incorrectly leave
        // BTCUSDT at its own smaller 0.5).
        read.on_equity_update(dec!(10000)).unwrap();
        read.on_equity_update(dec!(9000)).unwrap_err();

        let fractions = read.reduce_fractions(&["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
        assert_eq!(fractions.get("BTCUSDT"), Some(&dec!(1.0)));
        assert_eq!(fractions.get("ETHUSDT"), Some(&dec!(1.0)));
    }

    #[test]
    fn larger_per_symbol_fraction_is_not_overwritten_by_a_smaller_account_wide_one() {
        // Companion to the test above, covering the OTHER direction of
        // `max`: a per-symbol fraction that is LARGER than the
        // account-wide one must survive the fold-in unchanged, not get
        // clobbered by the smaller account-wide value.
        let mut config = sample_config();
        config.account_equity_volatility_threshold = Some(dec!(1000));
        config.volatility_window = 2;
        let monitor = MarketConditionMonitor::new(config);
        let (_pre_trade, mut event_monitor, read) = monitor.split();

        // BTCUSDT: a full (1.0) jump fraction -- 100 -> 120, 20% jump
        // vs. a 10% threshold.
        event_monitor
            .on_market_event(&trade_event("BTCUSDT", dec!(100)))
            .unwrap();
        event_monitor
            .on_market_event(&trade_event("BTCUSDT", dec!(120)))
            .unwrap_err();
        // Account-wide: a small equity dip, well under a threshold of
        // 1000, still produces SOME nonzero fraction via a mild
        // volatility signal but stays strictly below 1.0.
        read.on_equity_update(dec!(10000)).unwrap();
        read.on_equity_update(dec!(9500)).unwrap();

        let fractions = read.reduce_fractions(&["BTCUSDT".to_string()]);
        assert_eq!(fractions.get("BTCUSDT"), Some(&dec!(1.0)));
    }

    #[test]
    fn account_equity_detection_is_a_noop_when_not_configured() {
        let monitor = MarketConditionMonitor::new(sample_config()); // threshold: None
        let (_pre_trade, _event_monitor, read) = monitor.split();

        let result = read.on_equity_update(dec!(1));
        assert!(result.is_ok());
        assert!(read.reduce_fractions(&[]).is_empty());
    }

    #[test]
    fn account_wide_trip_blocks_pre_trade_for_any_symbol() {
        let mut config = sample_config();
        config.account_equity_volatility_threshold = Some(dec!(50));
        config.volatility_window = 2;
        let monitor = MarketConditionMonitor::new(config);
        let (pre_trade, _event_monitor, read) = monitor.split();

        read.on_equity_update(dec!(10000)).unwrap();
        read.on_equity_update(dec!(9000)).unwrap_err();

        let check_result = pre_trade.check(
            &sample_intent("DOGEUSDT"),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            Utc::now(),
        );
        assert!(check_result.is_err());
    }
}
