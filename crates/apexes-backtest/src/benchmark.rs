//! F03.05: explicit single-Spot, cash-funded reference performance.
use crate::{BacktestConfig, BacktestReport};
use apexes_core::{ApexesError, AssetClass, MarketEvent, registry::Registry};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BenchmarkPolicy {
    pub symbol: String,
    pub max_end_age_ms: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BenchmarkComparison {
    pub quote_currency: String,
    pub start_at: DateTime<Utc>,
    pub end_at: DateTime<Utc>,
    pub last_reference_at: DateTime<Utc>,
    pub start_price: Decimal,
    pub end_price: Decimal,
    pub initial_equity: Decimal,
    pub final_equity: Decimal,
    pub strategy_return: Decimal,
    pub cash_return: Decimal,
    pub buy_hold_return: Decimal,
    /// Arithmetic excess return, not a ratio or annualized alpha.
    pub excess_vs_cash: Decimal,
    pub excess_vs_buy_hold: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum BenchmarkEvidence {
    Available {
        policy: BenchmarkPolicy,
        comparison: Box<BenchmarkComparison>,
    },
    Unavailable {
        policy: BenchmarkPolicy,
        reason: String,
    },
}

pub(crate) struct Reference {
    quote: String,
    symbol: String,
    venue: apexes_core::Venue,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    last_at: DateTime<Utc>,
    first_price: Decimal,
    last_price: Decimal,
    capital: Decimal,
    expected_points: usize,
}

impl BenchmarkPolicy {
    pub(crate) fn from_registry(registry: &Registry) -> Result<Option<Self>, ApexesError> {
        registry
            .try_one::<Self>()
            .map_err(|error| ApexesError::InvalidBacktestConfig {
                reason: error.to_string(),
            })
    }

    pub(crate) fn prepare(
        &self,
        config: &BacktestConfig,
        events: &[MarketEvent],
        verified: bool,
        event_time: bool,
    ) -> Result<Reference, String> {
        let [instrument] = config.instruments.as_slice() else {
            return Err("benchmark requires one instrument".into());
        };
        let quote = instrument
            .quote_currency
            .as_ref()
            .filter(|q| !q.is_empty())
            .ok_or("missing quote currency")?;
        if !verified
            || config.control.is_some()
            || self.max_end_age_ms == 0
            || instrument.symbol != self.symbol
            || instrument.asset_class != AssetClass::Spot
            || instrument.multiplier != Decimal::ONE
            || instrument
                .base_currency
                .as_ref()
                .is_none_or(|b| b.is_empty() || b == quote)
        {
            return Err("benchmark requires verified, uncontrolled, unit-multiplier Spot replay and positive freshness limit".into());
        }
        let capital = config
            .initial_balances
            .get(quote)
            .copied()
            .filter(|c| *c > Decimal::ZERO)
            .ok_or("benchmark requires positive starting quote cash")?;
        if config
            .initial_balances
            .iter()
            .any(|(asset, amount)| asset != quote && !amount.is_zero())
        {
            return Err("benchmark requires cash-only initial balances".into());
        }
        crate::replay_identity::validate_observation_order(events, event_time)?;
        let Some(MarketEvent::Trade(first)) = events.first() else {
            return Err("benchmark requires a first-event trade reference".into());
        };
        let matches = |trade: &apexes_core::TradeEvent| {
            trade.symbol == instrument.symbol
                && trade.venue == instrument.venue
                && trade.price > Decimal::ZERO
        };
        if !matches(first) {
            return Err("invalid first trade reference".into());
        }
        let mut last = first;
        for event in events {
            if let MarketEvent::Trade(trade) = event {
                if !matches(trade) {
                    return Err("invalid trade reference in benchmark scope".into());
                }
                last = trade;
            }
        }
        let end = match events.last().ok_or("empty input")? {
            MarketEvent::Bar(bar) => bar.close_time,
            event => crate::market_data_source::event_timestamp(event),
        };
        if end - last.timestamp > chrono::TimeDelta::milliseconds(i64::from(self.max_end_age_ms)) {
            return Err("final benchmark reference is stale".into());
        }
        Ok(Reference {
            quote: quote.clone(),
            symbol: instrument.symbol.clone(),
            venue: instrument.venue,
            start: first.timestamp,
            end,
            last_at: last.timestamp,
            first_price: first.price,
            last_price: last.price,
            capital,
            expected_points: events.len() + 1,
        })
    }

    pub(crate) fn finish(
        self,
        reference: Result<Reference, String>,
        report: &BacktestReport,
    ) -> BenchmarkEvidence {
        match reference.and_then(|reference| reference.compare(report)) {
            Ok(comparison) => BenchmarkEvidence::Available {
                policy: self,
                comparison: Box::new(comparison),
            },
            Err(reason) => BenchmarkEvidence::Unavailable {
                policy: self,
                reason,
            },
        }
    }
}

impl Reference {
    fn compare(self, report: &BacktestReport) -> Result<BenchmarkComparison, String> {
        if report.equity_curve.len() != self.expected_points
            || report.equity_curve.first() != Some(&(self.start, self.capital))
            || report
                .equity_curve
                .last()
                .is_none_or(|(at, _)| *at != self.end)
            || report
                .event_latency
                .as_ref()
                .is_some_and(|r| r.regressing_events != 0)
            || report.trades.iter().any(|f| {
                f.symbol != self.symbol
                    || f.venue != self.venue
                    || f.quantity <= Decimal::ZERO
                    || f.price <= Decimal::ZERO
                    || f.leverage.is_some()
                    || (!f.fee.is_zero()
                        && f.fee_currency.as_ref().is_some_and(|a| a != &self.quote))
            })
        {
            return Err(
                "report horizon, execution economics or starting equity is incompatible".into(),
            );
        }
        let final_equity = report.equity_curve.last().ok_or("empty equity curve")?.1;
        let result = |end: Decimal, start: Decimal| end.checked_sub(start)?.checked_div(start);
        let strategy_return =
            result(final_equity, self.capital).ok_or("benchmark arithmetic overflow")?;
        let buy_hold_return =
            result(self.last_price, self.first_price).ok_or("benchmark arithmetic overflow")?;
        let excess_vs_buy_hold = strategy_return
            .checked_sub(buy_hold_return)
            .ok_or("benchmark arithmetic overflow")?;
        Ok(BenchmarkComparison {
            quote_currency: self.quote,
            start_at: self.start,
            end_at: self.end,
            last_reference_at: self.last_at,
            start_price: self.first_price,
            end_price: self.last_price,
            initial_equity: self.capital,
            final_equity,
            strategy_return,
            cash_return: Decimal::ZERO,
            buy_hold_return,
            excess_vs_cash: strategy_return,
            excess_vs_buy_hold,
        })
    }
}
