//! F03.03: offline no-order smoke replay of a qualified capture bundle.
//! Checks the original receipts again, not the provenance claim in JSON.
use crate::{
    BacktestConfig, CostModel, DepthSequence, FlatFeeSchedule, InMemoryMarketDataSource,
    OrderIntent, ReplaySpeed, SimulatedDepthPolicy, Strategy, StrategyContext,
    collector_receipts::{CollectorEvidence, QualificationMethod, QualifiedCapture},
    historical_dataset::VerifiedDataset,
    run_backtest,
};
use apexes_core::{Instrument, MarketEvent, registry::Registry};
use rust_decimal::Decimal;
use serde::Serialize;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Debug, Serialize)]
pub struct CollectorReplayReport {
    pub dataset: VerifiedDataset,
    /// This is a label from the input bundle, not independently verified.
    pub declared_qualification_method: QualificationMethod,
    pub events_observed: usize,
    pub simulated_depth: SimulatedDepthPolicy,
    pub fills: usize,
    pub independently_attested: bool,
}

struct NoOrders(Arc<AtomicUsize>);

impl Strategy for NoOrders {
    fn on_market_event(&mut self, _: &MarketEvent, _: &StrategyContext) -> Vec<OrderIntent> {
        self.0.fetch_add(1, Ordering::Relaxed);
        vec![]
    }
}

fn config(instrument: Instrument, dataset: &VerifiedDataset) -> Result<BacktestConfig, String> {
    if instrument.market_type != instrument.venue.market_type()
        || instrument.tick_size <= Decimal::ZERO
        || instrument.lot_size <= Decimal::ZERO
        || instrument.multiplier <= Decimal::ZERO
    {
        return Err("invalid replay instrument".into());
    }
    let quote = instrument
        .quote_currency
        .clone()
        .filter(|s| !s.is_empty())
        .ok_or("replay instrument requires quote currency")?;
    let def = &dataset.manifest.definition;
    Ok(BacktestConfig {
        instruments: vec![instrument],
        date_range: def.start_day..def.end_day_exclusive,
        initial_balances: HashMap::from([(quote, Decimal::ONE)]),
        cost_model: CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: Decimal::ZERO,
                taker_bps: Decimal::ZERO,
            }),
            funding_rate_bps: None,
        },
        speed: ReplaySpeed::AsFastAsPossible,
        queue_position_factor: Decimal::ZERO,
        risk_free_return: Decimal::ZERO,
        margin_tiers: HashMap::new(),
        market_condition_config: None,
        control: None,
    })
}

/// Recheck the original receipt evidence and require every input event
/// to reach a no-order strategy through the actual historical runner.
/// No trading result, fill quality or independent source attestation.
pub async fn verify_capture_replay(
    bundle: QualifiedCapture,
    instrument: Instrument,
) -> Result<CollectorReplayReport, String> {
    // The in-memory source sorts by timestamp. Refuse an altered order
    // before passing the input to that adapter.
    bundle.dataset.manifest.verify(&bundle.events)?;
    let cfg = config(instrument, &bundle.dataset)?;
    let registry = Registry::new();
    registry
        .register::<_, ()>((), bundle.dataset.manifest.clone())
        .map_err(|e| e.to_string())?;
    registry
        .register::<_, ()>(
            (),
            CollectorEvidence {
                journal: bundle.journal,
            },
        )
        .map_err(|e| e.to_string())?;
    registry
        .register::<_, ()>(
            (),
            SimulatedDepthPolicy {
                sequence: DepthSequence::Contiguous,
                max_age_ms: 30_000,
            },
        )
        .map_err(|e| e.to_string())?;
    let expected = bundle.events.len();
    let observed = Arc::new(AtomicUsize::new(0));
    let report = run_backtest(
        cfg,
        &registry,
        InMemoryMarketDataSource::new(bundle.events),
        NoOrders(observed.clone()),
    )
    .await
    .map_err(|e| e.to_string())?;
    let dataset = report
        .historical_dataset
        .ok_or("replay omitted dataset verification")?;
    let simulated_depth = report
        .simulated_depth
        .ok_or("replay omitted strict depth policy")?;
    let events_observed = observed.load(Ordering::Relaxed);
    if dataset != bundle.dataset
        || events_observed != expected
        || simulated_depth.sequence != DepthSequence::VerifiedRanges
        || !report.trades.is_empty()
    {
        return Err(
            "replay evidence, event count, depth policy or no-order invariant mismatch".into(),
        );
    }
    Ok(CollectorReplayReport {
        dataset,
        declared_qualification_method: bundle.qualification_method,
        events_observed,
        simulated_depth,
        fills: report.trades.len(),
        independently_attested: false,
    })
}
