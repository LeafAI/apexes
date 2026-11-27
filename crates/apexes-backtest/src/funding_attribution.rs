//! F03.05: exact simulated funding movements and quote-denominated attribution.
use crate::{
    cost_model::CostModel,
    execution_performance::{ExecutionPerformance, SymbolExecutionPerformance},
    ledger::PortfolioLedger,
};
use apexes_core::{ApexesError, Venue};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FundingPriceSource {
    Mark,
    EntryFallback,
}

/// The actual simulated ledger credit (negative for a debit).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FundingMovement {
    pub symbol: String,
    pub venue: Venue,
    pub quote_currency: String,
    pub position_quantity: Decimal,
    pub valuation_price: Decimal,
    pub price_source: FundingPriceSource,
    pub notional: Decimal,
    pub rate_bps: Decimal,
    pub balance_delta: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FundingSettlement {
    pub boundary: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
    #[serde(flatten)]
    pub movement: FundingMovement,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FundingTotals {
    pub quote_currency: String,
    pub income: Decimal,
    pub expenses: Decimal,
    pub net_funding: Decimal,
    pub settlement_count: u64,
    /// Component sum, excluding unrealized value and unallocated entry fees.
    pub net_realized_plus_funding: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum SymbolFunding {
    Available(Box<FundingTotals>),
    Unavailable { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FundingReport {
    /// None explicitly means disabled; this is a simulated fixed rate.
    pub rate_bps: Option<Decimal>,
    pub observed_settlement_passes: u64,
    pub settlements: Vec<FundingSettlement>,
    pub per_symbol: BTreeMap<String, SymbolFunding>,
}

impl FundingReport {
    pub(crate) fn new(rate_bps: Option<Decimal>) -> Self {
        Self {
            rate_bps,
            observed_settlement_passes: 0,
            settlements: Vec::new(),
            per_symbol: BTreeMap::new(),
        }
    }

    pub(crate) fn reconcile(&mut self, execution: &ExecutionPerformance) {
        let mut grouped: BTreeMap<String, Vec<&FundingMovement>> = BTreeMap::new();
        for settlement in &self.settlements {
            grouped
                .entry(settlement.movement.symbol.clone())
                .or_default()
                .push(&settlement.movement);
        }
        self.per_symbol = grouped
            .into_iter()
            .map(|(symbol, movements)| {
                let result = summarize(&movements, execution.per_symbol.get(&symbol));
                let summary = match result {
                    Some(totals) => SymbolFunding::Available(Box::new(totals)),
                    None => SymbolFunding::Unavailable {
                        reason: "funding totals overflow or inconsistent currency".into(),
                    },
                };
                (symbol, summary)
            })
            .collect();
    }
}

fn summarize(
    movements: &[&FundingMovement],
    execution: Option<&SymbolExecutionPerformance>,
) -> Option<FundingTotals> {
    let quote = &movements.first()?.quote_currency;
    let mut income = Decimal::ZERO;
    let mut expenses = Decimal::ZERO;
    let mut net_funding = Decimal::ZERO;
    for m in movements {
        if &m.quote_currency != quote {
            return None;
        }
        net_funding = net_funding.checked_add(m.balance_delta)?;
        if m.balance_delta >= Decimal::ZERO {
            income = income.checked_add(m.balance_delta)?;
        } else {
            expenses = expenses.checked_add(m.balance_delta.abs())?;
        }
    }
    let net_realized_plus_funding = match execution {
        Some(SymbolExecutionPerformance::Available(s)) if &s.quote_currency == quote => {
            s.net_realized.checked_add(net_funding)
        }
        _ => None,
    };
    Some(FundingTotals {
        quote_currency: quote.clone(),
        income,
        expenses,
        net_funding,
        settlement_count: movements.len() as u64,
        net_realized_plus_funding,
    })
}

/// Both causal and legacy paths call this once at their existing settlement point.
pub(crate) fn settle(
    ledger: &mut PortfolioLedger,
    mark_prices: &HashMap<String, Decimal>,
    cost_model: &CostModel,
    boundary: DateTime<Utc>,
    observed_at: DateTime<Utc>,
    report: Option<&mut FundingReport>,
) -> Result<(), ApexesError> {
    let movements = ledger.settle_funding(mark_prices, cost_model)?;
    if let Some(report) = report
        && report.rate_bps.is_some()
    {
        report.observed_settlement_passes += 1;
        report
            .settlements
            .extend(movements.into_iter().map(|movement| FundingSettlement {
                boundary,
                observed_at,
                movement,
            }));
    }
    Ok(())
}
