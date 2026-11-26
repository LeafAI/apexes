//! F00.02 — structured trading decisions in the shared event loop.
//!
//! Each run has a UUID; event sequence and intent ordinal identify a
//! decision. Accepted order IDs join subsequent fills, including delayed
//! exchange fills. No raw exchange errors, credentials or strategy source
//! are emitted by this module. The tracing subscriber owns persistence.

use apexes_core::{ApexesError, Fill, MarketEvent, Order, RuleViolation};
use apexes_risk::RiskViolation;
use apexes_strategy::{OrderIntent, ParentOrderId};
use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Clone, Copy)]
pub(crate) struct Diagnostics {
    run_id: Uuid,
    event_seq: u64,
    next_ordinal: usize,
}

impl Diagnostics {
    pub(crate) fn new() -> Self {
        Self {
            run_id: Uuid::new_v4(),
            event_seq: 0,
            next_ordinal: 0,
        }
    }

    pub(crate) fn next_event(&mut self) {
        self.event_seq += 1;
        self.next_ordinal = 0;
    }

    pub(crate) fn decision(
        self,
        event: &MarketEvent,
        at: DateTime<Utc>,
        paused: bool,
        count: usize,
    ) {
        let kind = match event {
            MarketEvent::Trade(_) => "trade",
            MarketEvent::Tick(_) => "tick",
            MarketEvent::Depth(_) => "depth",
            MarketEvent::Bar(_) => "bar",
            MarketEvent::MarkPrice(_) => "mark_price",
        };
        if count > 0 {
            tracing::info!(target: "apexes::decision", stage = "decision", run_id = %self.run_id,
                event_seq = self.event_seq, event_at = %at, symbol = %event.symbol(),
                venue = ?event.venue(), event_kind = kind, paused, intent_count = count);
        } else {
            tracing::debug!(target: "apexes::decision", stage = "decision", run_id = %self.run_id,
                event_seq = self.event_seq, event_at = %at, symbol = %event.symbol(),
                venue = ?event.venue(), event_kind = kind, paused, intent_count = count);
        }
    }

    pub(crate) fn intent(
        &mut self,
        intent: &OrderIntent,
        source: &'static str,
        at: DateTime<Utc>,
    ) -> IntentTrace {
        let ordinal = self.next_ordinal;
        self.next_ordinal += 1;
        let trace = IntentTrace {
            diagnostics: *self,
            ordinal,
        };
        tracing::info!(target: "apexes::decision", stage = "intent", run_id = %self.run_id,
            event_seq = self.event_seq, intent_ordinal = ordinal, event_at = %at, source, symbol = %intent.symbol,
            venue = ?intent.venue, side = ?intent.side, order_type = ?intent.order_type,
            quantity = %intent.quantity, price = ?intent.price);
        trace
    }

    pub(crate) fn fill(self, fill: &Fill, source: &'static str, error: Option<&ApexesError>) {
        let ledger_result = if error.is_some() { "failed" } else { "applied" };
        let reason = error.map(error_code).unwrap_or("none");
        tracing::info!(target: "apexes::decision", stage = "fill", run_id = %self.run_id,
            // Supplemental fills have no causal market-event association.
            event_seq = if source == "supplemental" { None } else { Some(self.event_seq) },
            source, order_id = %fill.order_id.0, fill_id = %fill.id.0,
            filled_at = %fill.filled_at, symbol = %fill.symbol, venue = ?fill.venue,
            side = ?fill.side, quantity = %fill.quantity, price = %fill.price,
            fee = %fill.fee, fee_currency = ?fill.fee_currency, ledger_result, reason);
    }
}

#[derive(Clone, Copy)]
pub(crate) struct IntentTrace {
    diagnostics: Diagnostics,
    ordinal: usize,
}

impl IntentTrace {
    pub(crate) fn outcome(self, stage: &'static str, reason: &'static str) {
        tracing::info!(target: "apexes::decision", stage, reason,
            run_id = %self.diagnostics.run_id, event_seq = self.diagnostics.event_seq,
            intent_ordinal = self.ordinal);
    }

    pub(crate) fn conformed(self, intent: &OrderIntent) {
        tracing::info!(target: "apexes::decision", stage = "conformed",
            run_id = %self.diagnostics.run_id, event_seq = self.diagnostics.event_seq,
            intent_ordinal = self.ordinal, quantity = %intent.quantity, price = ?intent.price);
    }

    pub(crate) fn risk_rejected(self, stage: &'static str, violations: &[RiskViolation]) {
        for violation in violations {
            self.outcome(stage, risk_code(violation));
        }
    }

    pub(crate) fn accepted(self, order: &Order) {
        tracing::info!(target: "apexes::decision", stage = "order_result",
            run_id = %self.diagnostics.run_id, event_seq = self.diagnostics.event_seq,
            intent_ordinal = self.ordinal, order_id = %order.id.0, status = ?order.status);
    }

    pub(crate) fn parent_accepted(self, parent: ParentOrderId) {
        tracing::info!(target: "apexes::decision", stage = "parent_accepted",
            run_id = %self.diagnostics.run_id, event_seq = self.diagnostics.event_seq,
            intent_ordinal = self.ordinal, parent_id = %parent.0);
    }
}

pub(crate) fn error_code(error: &ApexesError) -> &'static str {
    match error {
        ApexesError::InvalidInstrument { .. } => "invalid_instrument",
        ApexesError::InvalidStateTransition { .. } => "invalid_state_transition",
        ApexesError::ConnectionFailed { .. } => "connection_failed",
        ApexesError::SubscriptionFailed { .. } => "subscription_failed",
        ApexesError::InsufficientBalance { .. } => "insufficient_balance",
        ApexesError::InvalidBacktestConfig { .. } => "invalid_config",
        ApexesError::InconsistentLeverageForIsolatedPosition { .. } => "inconsistent_leverage",
        ApexesError::OrderRejected { .. } => "order_rejected",
        ApexesError::UnsupportedForEnvironment { .. } => "unsupported",
    }
}

fn risk_code(violation: &RiskViolation) -> &'static str {
    match violation {
        RiskViolation::OrderQuantityExceeded { .. } => "order_quantity_exceeded",
        RiskViolation::OrderNotionalExceeded { .. } => "order_notional_exceeded",
        RiskViolation::PositionLimitExceeded { .. } => "position_limit_exceeded",
        RiskViolation::EquityFloorBreached { .. } => "equity_floor_breached",
        RiskViolation::DailyTradeCountExceeded { .. } => "daily_trade_count_exceeded",
        RiskViolation::DailyVolumeExceeded { .. } => "daily_volume_exceeded",
        RiskViolation::SymbolBlacklisted { .. } => "symbol_blacklisted",
        RiskViolation::SymbolNotWhitelisted { .. } => "symbol_not_whitelisted",
        RiskViolation::VenueBlacklisted { .. } => "venue_blacklisted",
        RiskViolation::VenueNotWhitelisted { .. } => "venue_not_whitelisted",
        RiskViolation::SelfTradeRisk { .. } => "self_trade_risk",
        RiskViolation::CircuitBreakerOpen { .. } => "circuit_breaker_open",
        RiskViolation::MarginRatioBreached { .. } => "margin_ratio_breached",
        RiskViolation::MarketConditionBreached { .. } => "market_condition_breached",
        RiskViolation::AccountWideMarketConditionBreached { .. } => {
            "account_market_condition_breached"
        }
    }
}

pub(crate) fn rule_code(violation: &RuleViolation) -> &'static str {
    match violation {
        RuleViolation::RoundsToNothing { .. } => "rounds_to_nothing",
        RuleViolation::BelowMinimumQuantity { .. } => "below_minimum_quantity",
        RuleViolation::AboveMaximumQuantity { .. } => "above_maximum_quantity",
        RuleViolation::BelowMinimumNotional { .. } => "below_minimum_notional",
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    struct Writer(Arc<Mutex<Vec<u8>>>);

    impl Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    pub(crate) fn capture(f: impl FnOnce()) -> Vec<serde_json::Value> {
        let output = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::clone(&output);
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(move || Writer(Arc::clone(&writer)))
            .finish();
        tracing::subscriber::with_default(subscriber, f);
        let buffer = output.lock().unwrap();
        std::str::from_utf8(&buffer)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[test]
    fn submission_failure_records_only_the_allowlisted_error_code() {
        use super::*;
        let secret = "DO_NOT_LOG_TEST_SECRET";
        let error = ApexesError::ConnectionFailed {
            reason: format!("https://example.invalid?signature={secret}"),
            source_detail: Some(secret.to_string()),
        };
        let mut diagnostics = Diagnostics::new();
        diagnostics.next_event();
        let trace = IntentTrace {
            diagnostics,
            ordinal: 0,
        };
        let records = capture(|| trace.outcome("submission_failed", error_code(&error)));
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["fields"]["reason"], "connection_failed");
        assert!(!records[0].to_string().contains(secret));
    }
}
