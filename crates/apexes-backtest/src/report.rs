use crate::performance::PerformanceSummary;
use apexes_core::Fill;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Structured, serializable backtest results (design spec §8, §10). No
/// rendering/charting code is part of this crate -- visualization is
/// sub-project #13's responsibility, consuming this data directly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestReport {
    pub overall: SerializablePerformanceSummary,
    pub per_symbol: HashMap<String, SerializablePerformanceSummary>,
    /// Keyed by venue via `format!("{venue:?}")`, matching the same
    /// Debug-based string convention used throughout `apexes-storage`'s
    /// schema.rs for venue serialization -- `String` keys keep JSON
    /// serialization simple (JSON object keys must be strings; a
    /// `HashMap<Venue, _>` would need a custom serde key serializer).
    pub per_venue: HashMap<String, SerializablePerformanceSummary>,
    pub equity_curve: Vec<(DateTime<Utc>, rust_decimal::Decimal)>,
    /// Every `Fill` that occurred during the run, in chronological order.
    pub trades: Vec<Fill>,
    /// Spot execution contribution by symbol, each in its own quote unit.
    #[serde(default)]
    pub spot_pnl: HashMap<String, crate::spot_accounting::SpotPnlSummary>,
    /// None means fee assumptions are unrecorded, not zero.
    #[serde(default)]
    pub simulated_fees: Option<crate::SimulatedFeeReport>,
    #[serde(default)]
    pub simulated_execution: Option<crate::SimulatedExecution>,
    #[serde(default)]
    pub simulated_depth: Option<crate::SimulatedDepthPolicy>,
    #[serde(default)]
    pub event_latency: Option<crate::EventLatencyReport>,
    /// Verified identity of the loaded replay input, not upstream completeness.
    #[serde(default)]
    pub historical_dataset: Option<crate::historical_dataset::VerifiedDataset>,
    /// Opt-in matching ID policy, not a full reproducibility attestation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_identity: Option<crate::replay_identity::ReplayIdentityPolicy>,
    /// Historical closing-fill metrics, not legacy equity-point statistics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_performance: Option<crate::execution_performance::ExecutionPerformance>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drawdown_duration: Option<crate::drawdown_duration::DrawdownEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub benchmark: Option<crate::benchmark::BenchmarkEvidence>,
    /// Frictionless, static-weight Spot reference for the verified replay horizon.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub portfolio_benchmark: Option<crate::portfolio_benchmark::PortfolioBenchmarkEvidence>,
    /// Historical simulation only; absent for legacy reports and paper/live.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub funding: Option<crate::funding_attribution::FundingReport>,
}

/// `PerformanceSummary` (Task 13) does not itself derive `Serialize`/
/// `Deserialize` (it lives in `performance.rs` and has no reason to
/// depend on `serde` directly); this wrapper type in `report.rs` mirrors
/// its fields with serde derives, since `BacktestReport` -- not
/// `PerformanceSummary` itself -- is this crate's serialization boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializablePerformanceSummary {
    pub total_pnl: rust_decimal::Decimal,
    pub sharpe: rust_decimal::Decimal,
    pub sortino: rust_decimal::Decimal,
    pub calmar: rust_decimal::Decimal,
    pub max_drawdown: rust_decimal::Decimal,
    pub win_rate: rust_decimal::Decimal,
    pub profit_factor: rust_decimal::Decimal,
    pub turnover: rust_decimal::Decimal,
    pub trade_count: u64,
}

impl From<PerformanceSummary> for SerializablePerformanceSummary {
    fn from(summary: PerformanceSummary) -> Self {
        Self {
            total_pnl: summary.total_pnl,
            sharpe: summary.sharpe,
            sortino: summary.sortino,
            calmar: summary.calmar,
            max_drawdown: summary.max_drawdown,
            win_rate: summary.win_rate,
            profit_factor: summary.profit_factor,
            turnover: summary.turnover,
            trade_count: summary.trade_count,
        }
    }
}
