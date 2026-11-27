//! F03.05: frictionless, cash-funded static-weight Spot portfolio comparisons.

use crate::{BacktestConfig, BacktestReport, market_data_source::observation_timestamp};
use apexes_core::{
    ApexesError, AssetClass, Fill, Instrument, MarketEvent, Side, TradeEvent, Venue,
    registry::Registry,
};
use chrono::{DateTime, TimeDelta, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortfolioBenchmarkPolicy {
    pub allocations: Vec<PortfolioAllocation>,
    pub quote_currency: String,
    pub max_end_age_ms: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortfolioAllocation {
    pub symbol: String,
    pub weight: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum PortfolioBenchmarkEvidence {
    Available {
        policy: PortfolioBenchmarkPolicy,
        comparison: Box<PortfolioComparison>,
    },
    Unavailable {
        policy: PortfolioBenchmarkPolicy,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortfolioComparison {
    pub method: PortfolioBenchmarkMethod,
    pub start_at: DateTime<Utc>,
    pub end_at: DateTime<Utc>,
    pub quote_currency: String,
    pub initial_strategy_equity: Decimal,
    pub final_strategy_equity: Decimal,
    pub initial_benchmark_equity: Decimal,
    pub final_benchmark_equity: Decimal,
    pub cash_weight: Decimal,
    pub cash_capital: Decimal,
    pub strategy_return: Decimal,
    pub portfolio_return: Decimal,
    pub cash_return: Decimal,
    pub excess_vs_cash: Decimal,
    pub excess_vs_portfolio: Decimal,
    pub instruments: Vec<PortfolioInstrumentComparison>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PortfolioBenchmarkMethod {
    FrictionlessBuyAndHold,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortfolioInstrumentComparison {
    pub symbol: String,
    pub venue: Venue,
    pub weight: Decimal,
    pub initial_capital: Decimal,
    pub start_price: Decimal,
    pub start_reference_at: DateTime<Utc>,
    pub end_price: Decimal,
    pub end_reference_at: DateTime<Utc>,
    pub instrument_return: Decimal,
    pub contribution: Decimal,
    pub final_holding_value: Decimal,
}

pub(crate) struct PortfolioReference {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    capital: Decimal,
    cash_weight: Decimal,
    expected_points: usize,
    universe: BTreeMap<String, Venue>,
    marked_symbols: HashSet<String>,
    instruments: Vec<PortfolioInstrumentComparison>,
}

#[derive(Clone, Copy, Default)]
struct TradeReferences<'a> {
    first: Option<&'a TradeEvent>,
    last: Option<&'a TradeEvent>,
}

const OVERFLOW: &str = "portfolio benchmark Decimal arithmetic overflow";

impl PortfolioBenchmarkPolicy {
    pub(crate) fn from_registry(registry: &Registry) -> Result<Option<Self>, ApexesError> {
        registry
            .try_one::<Self>()
            .map_err(|error| ApexesError::InvalidBacktestConfig {
                reason: error.to_string(),
            })
    }

    fn universe<'a>(
        &self,
        instruments: &'a [Instrument],
    ) -> Result<BTreeMap<&'a str, Venue>, String> {
        let mut symbols = BTreeMap::new();
        let mut bases = HashSet::new();
        for instrument in instruments {
            let base = instrument
                .base_currency
                .as_deref()
                .filter(|base| !base.trim().is_empty() && *base != self.quote_currency);
            if instrument.asset_class != AssetClass::Spot
                || instrument.multiplier != Decimal::ONE
                || instrument.quote_currency.as_deref() != Some(self.quote_currency.as_str())
                || base.is_none()
            {
                return Err(format!(
                    "{} requires unit-multiplier Spot with a distinct base and quote {}",
                    instrument.symbol, self.quote_currency
                ));
            }
            if instrument.symbol.trim().is_empty()
                || symbols
                    .insert(instrument.symbol.as_str(), instrument.venue)
                    .is_some()
                || !bases.insert(base)
            {
                return Err(format!(
                    "ambiguous symbol or base-asset valuation for {}",
                    instrument.symbol
                ));
            }
        }
        Ok(symbols)
    }

    fn weights<'a>(
        &'a self,
        universe: &BTreeMap<&str, Venue>,
    ) -> Result<(BTreeMap<&'a str, Decimal>, Decimal), String> {
        if self.allocations.is_empty() {
            return Err("configure at least one portfolio allocation".into());
        }
        let mut weights = BTreeMap::new();
        let mut remaining = 10_i128.pow(Decimal::MAX_SCALE);
        for allocation in &self.allocations {
            if !universe.contains_key(allocation.symbol.as_str()) {
                return Err(format!("unknown portfolio symbol {}", allocation.symbol));
            }
            if weights
                .insert(allocation.symbol.as_str(), allocation.weight)
                .is_some()
            {
                return Err(format!("duplicate portfolio symbol {}", allocation.symbol));
            }
            if allocation.weight <= Decimal::ZERO || allocation.weight > Decimal::ONE {
                return Err(format!(
                    "weight for {} must be positive and at most one",
                    allocation.symbol
                ));
            }
            // Each validated weight fits in 10^28 units. Decimal addition
            // can round at large magnitudes and hide a small overallocation.
            remaining -= allocation.weight.mantissa()
                * 10_i128.pow(Decimal::MAX_SCALE - allocation.weight.scale());
            if remaining < 0 {
                return Err("total portfolio weight exceeds one".into());
            }
        }
        Ok((
            weights,
            Decimal::from_i128_with_scale(remaining, Decimal::MAX_SCALE).normalize(),
        ))
    }

    pub(crate) fn prepare(
        &self,
        config: &BacktestConfig,
        events: &[MarketEvent],
        verified: bool,
        event_time: bool,
    ) -> Result<PortfolioReference, String> {
        if !verified || config.control.is_some() {
            return Err(
                "portfolio benchmark requires verified replay without external control".into(),
            );
        }
        if self.quote_currency.trim().is_empty() || self.max_end_age_ms == 0 {
            return Err(
                "configure a quote currency and positive final reference freshness limit".into(),
            );
        }
        let universe = self.universe(&config.instruments)?;
        let (weights, cash_weight) = self.weights(&universe)?;
        let capital = config
            .initial_balances
            .get(&self.quote_currency)
            .copied()
            .filter(|amount| *amount > Decimal::ZERO)
            .ok_or("portfolio benchmark requires positive starting quote cash")?;
        if config
            .initial_balances
            .iter()
            .any(|(asset, amount)| asset != &self.quote_currency && !amount.is_zero())
        {
            return Err("portfolio benchmark requires cash-only initial balances".into());
        }
        crate::replay_identity::validate_observation_order(events, event_time)?;
        let start = events
            .first()
            .map(observation_timestamp)
            .ok_or("empty replay horizon")?;
        let end = events
            .last()
            .map(observation_timestamp)
            .ok_or("empty replay horizon")?;
        let declared_end = config
            .date_range
            .end
            .and_hms_opt(0, 0, 0)
            .ok_or("invalid replay end date")?
            .and_utc();
        if end > declared_end {
            return Err("observation extends beyond the verified replay date range".into());
        }
        let references = trade_references(events, &universe, start)?;
        let max_age = TimeDelta::milliseconds(i64::from(self.max_end_age_ms));
        let instruments = weights
            .into_iter()
            .map(|(symbol, weight)| {
                let reference = references.get(symbol).copied().unwrap_or_default();
                reference.compare(symbol, weight, capital, end, max_age)
            })
            .collect::<Result<_, _>>()?;
        Ok(PortfolioReference {
            start,
            end,
            capital,
            cash_weight,
            expected_points: events.len() + 1,
            universe: universe
                .into_iter()
                .map(|(symbol, venue)| (symbol.to_owned(), venue))
                .collect(),
            marked_symbols: references
                .keys()
                .map(|symbol| (*symbol).to_owned())
                .collect(),
            instruments,
        })
    }

    pub(crate) fn finish(
        self,
        reference: Result<PortfolioReference, String>,
        report: &BacktestReport,
    ) -> PortfolioBenchmarkEvidence {
        match reference.and_then(|reference| reference.compare(&self.quote_currency, report)) {
            Ok(comparison) => PortfolioBenchmarkEvidence::Available {
                policy: self,
                comparison: Box::new(comparison),
            },
            Err(reason) => PortfolioBenchmarkEvidence::Unavailable {
                policy: self,
                reason,
            },
        }
    }
}

fn trade_references<'a>(
    events: &'a [MarketEvent],
    universe: &BTreeMap<&str, Venue>,
    start: DateTime<Utc>,
) -> Result<BTreeMap<&'a str, TradeReferences<'a>>, String> {
    let mut references = BTreeMap::<_, TradeReferences<'_>>::new();
    for event in events {
        let MarketEvent::Trade(trade) = event else {
            continue;
        };
        if universe.get(trade.symbol.as_str()) != Some(&trade.venue) || trade.price <= Decimal::ZERO
        {
            return Err(format!("invalid trade reference for {}", trade.symbol));
        }
        let reference = references.entry(trade.symbol.as_str()).or_default();
        if trade.timestamp == start && reference.first.is_none() {
            reference.first = Some(trade);
        }
        reference.last = Some(trade);
    }
    Ok(references)
}

fn simple_return(end: Decimal, start: Decimal) -> Result<Decimal, String> {
    end.checked_sub(start)
        .and_then(|change| change.checked_div(start))
        .ok_or_else(|| OVERFLOW.into())
}

fn holding_value(capital: Decimal, rate: Decimal) -> Result<Decimal, String> {
    Decimal::ONE
        .checked_add(rate)
        .and_then(|ratio| capital.checked_mul(ratio))
        .ok_or_else(|| OVERFLOW.into())
}

impl TradeReferences<'_> {
    fn compare(
        self,
        symbol: &str,
        weight: Decimal,
        capital: Decimal,
        end: DateTime<Utc>,
        max_age: TimeDelta,
    ) -> Result<PortfolioInstrumentComparison, String> {
        let first = self
            .first
            .ok_or_else(|| format!("missing starting trade for {symbol} at replay start"))?;
        let last = self
            .last
            .ok_or_else(|| format!("missing final trade for {symbol}"))?;
        if end - last.timestamp > max_age {
            return Err(format!("final trade for {symbol} is stale at replay end"));
        }
        let instrument_return = simple_return(last.price, first.price)?;
        let initial_capital = capital.checked_mul(weight).ok_or(OVERFLOW)?;
        if initial_capital.is_zero() {
            return Err(format!(
                "allocation capital for {symbol} rounds to zero at Decimal precision"
            ));
        }
        let contribution = weight.checked_mul(instrument_return).ok_or(OVERFLOW)?;
        let gain = capital.checked_mul(contribution).ok_or(OVERFLOW)?;
        Ok(PortfolioInstrumentComparison {
            symbol: symbol.into(),
            venue: first.venue,
            weight,
            initial_capital,
            start_price: first.price,
            start_reference_at: first.timestamp,
            end_price: last.price,
            end_reference_at: last.timestamp,
            instrument_return,
            contribution,
            final_holding_value: initial_capital.checked_add(gain).ok_or(OVERFLOW)?,
        })
    }
}

impl PortfolioReference {
    fn validate_fills(&self, quote: &str, fills: &[Fill]) -> Result<(), String> {
        let mut unmarked_quantities = BTreeMap::<&str, Decimal>::new();
        for fill in fills {
            if self.universe.get(&fill.symbol) != Some(&fill.venue)
                || fill.quantity <= Decimal::ZERO
                || fill.price <= Decimal::ZERO
                || fill.leverage.is_some()
                || fill.margin_mode.is_some()
                || (!fill.fee.is_zero()
                    && fill
                        .fee_currency
                        .as_deref()
                        .is_some_and(|asset| asset != quote))
                || !(self.start..=self.end).contains(&fill.filled_at)
            {
                return Err(format!(
                    "incompatible execution economics or fill time for {}",
                    fill.symbol
                ));
            }
            if !self.marked_symbols.contains(&fill.symbol) {
                let held = unmarked_quantities.entry(&fill.symbol).or_default();
                *held = match fill.side {
                    Side::Buy => held.checked_add(fill.quantity),
                    Side::Sell => held.checked_sub(fill.quantity),
                }
                .ok_or(OVERFLOW)?;
            }
        }
        if let Some((symbol, _)) = unmarked_quantities
            .iter()
            .find(|(_, quantity)| !quantity.is_zero())
        {
            return Err(format!(
                "strategy holds {symbol} without an observed trade valuation"
            ));
        }
        Ok(())
    }

    fn compare(self, quote: &str, report: &BacktestReport) -> Result<PortfolioComparison, String> {
        let &(end, final_equity) = report.equity_curve.last().ok_or("empty equity curve")?;
        if report.equity_curve.len() != self.expected_points
            || report.equity_curve.first() != Some(&(self.start, self.capital))
            || end != self.end
            || report
                .event_latency
                .as_ref()
                .is_some_and(|r| r.regressing_events != 0)
        {
            return Err("strategy equity curve does not cover the verified replay horizon".into());
        }
        self.validate_fills(quote, &report.trades)?;

        let strategy_return = simple_return(final_equity, self.capital)?;
        let portfolio_return = self
            .instruments
            .iter()
            .try_fold(Decimal::ZERO, |total, instrument| {
                total.checked_add(instrument.contribution)
            })
            .ok_or(OVERFLOW)?;
        Ok(PortfolioComparison {
            method: PortfolioBenchmarkMethod::FrictionlessBuyAndHold,
            start_at: self.start,
            end_at: self.end,
            quote_currency: quote.into(),
            initial_strategy_equity: self.capital,
            final_strategy_equity: final_equity,
            initial_benchmark_equity: self.capital,
            final_benchmark_equity: holding_value(self.capital, portfolio_return)?,
            cash_weight: self.cash_weight,
            cash_capital: self.capital.checked_mul(self.cash_weight).ok_or(OVERFLOW)?,
            strategy_return,
            portfolio_return,
            cash_return: Decimal::ZERO,
            excess_vs_cash: strategy_return,
            excess_vs_portfolio: strategy_return
                .checked_sub(portfolio_return)
                .ok_or(OVERFLOW)?,
            instruments: self.instruments,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::performance::PerformanceTracker;
    use apexes_core::{FillId, OrderId};
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn reference() -> PortfolioReference {
        PortfolioReference {
            start: DateTime::UNIX_EPOCH,
            end: DateTime::UNIX_EPOCH + TimeDelta::seconds(1),
            capital: dec!(100),
            cash_weight: Decimal::ONE,
            expected_points: 3,
            universe: BTreeMap::from([("BTCUSDT".into(), Venue::Binance)]),
            marked_symbols: HashSet::new(),
            instruments: Vec::new(),
        }
    }

    fn report(reference: &PortfolioReference) -> BacktestReport {
        BacktestReport {
            overall: PerformanceTracker::new(Decimal::ZERO).summary().into(),
            per_symbol: Default::default(),
            per_venue: Default::default(),
            equity_curve: vec![
                (reference.start, reference.capital),
                (reference.start, reference.capital),
                (reference.end, reference.capital),
            ],
            trades: Vec::new(),
            spot_pnl: Default::default(),
            simulated_fees: None,
            simulated_execution: None,
            simulated_depth: None,
            event_latency: None,
            historical_dataset: None,
            replay_identity: None,
            execution_performance: None,
            drawdown_duration: None,
            benchmark: None,
            portfolio_benchmark: None,
            funding: None,
        }
    }

    fn fill() -> Fill {
        Fill {
            id: FillId(Uuid::nil()),
            order_id: OrderId(Uuid::nil()),
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            side: Side::Buy,
            price: dec!(10),
            quantity: Decimal::ONE,
            fee: dec!(0.1),
            fee_currency: Some("USDT".into()),
            filled_at: DateTime::UNIX_EPOCH,
            leverage: None,
            margin_mode: None,
        }
    }

    #[test]
    fn rejects_truncated_horizon_even_with_correct_endpoints() {
        let reference = reference();
        let mut report = report(&reference);
        report.equity_curve.remove(1);
        assert!(
            reference
                .compare("USDT", &report)
                .unwrap_err()
                .contains("horizon")
        );
    }

    #[test]
    fn rejects_external_fee_and_future_execution() {
        let reference = reference();
        let mut report = report(&reference);
        let mut fill = fill();
        fill.fee_currency = Some("BNB".into());
        report.trades.push(fill.clone());
        assert!(
            reference
                .validate_fills("USDT", &report.trades)
                .unwrap_err()
                .contains("economics")
        );
        fill.fee_currency = Some("USDT".into());
        fill.filled_at = reference.end + TimeDelta::milliseconds(1);
        assert!(
            reference
                .validate_fills("USDT", &[fill])
                .unwrap_err()
                .contains("fill time")
        );
    }

    #[test]
    fn refuses_unmarked_open_inventory_and_accepts_unmarked_round_trip() {
        let reference = reference();
        let buy = fill();
        let sell = Fill {
            side: Side::Sell,
            ..buy.clone()
        };
        assert!(
            reference
                .validate_fills("USDT", std::slice::from_ref(&buy))
                .unwrap_err()
                .contains("without an observed trade")
        );
        assert!(reference.validate_fills("USDT", &[buy, sell]).is_ok());
    }

    #[test]
    fn rejects_allocation_that_rounds_below_decimal_precision() {
        let trade = TradeEvent {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            price: Decimal::ONE,
            quantity: Decimal::ONE,
            side: None,
            trade_id: None,
            timestamp: DateTime::UNIX_EPOCH,
        };
        let reference = TradeReferences {
            first: Some(&trade),
            last: Some(&trade),
        };
        let error = reference
            .compare(
                &trade.symbol,
                dec!(0.4),
                dec!(0.0000000000000000000000000001),
                trade.timestamp,
                TimeDelta::milliseconds(1),
            )
            .unwrap_err();
        assert!(error.contains("rounds to zero at Decimal precision"));
    }

    #[test]
    fn decimal_overflow_becomes_unavailable_evidence() {
        let mut reference = reference();
        reference.capital = dec!(0.0000000000000000000000000001);
        let mut report = report(&reference);
        report.equity_curve.last_mut().unwrap().1 = Decimal::MAX;
        let evidence = PortfolioBenchmarkPolicy {
            allocations: vec![],
            quote_currency: "USDT".into(),
            max_end_age_ms: 1,
        }
        .finish(Ok(reference), &report);
        assert!(
            matches!(evidence, PortfolioBenchmarkEvidence::Unavailable { reason, .. } if reason == OVERFLOW)
        );
    }
}
