//! F03.02: deterministic event-time latency for direct strategy orders.
use apexes_core::{ApexesError, Fill, Order, OrderId, Venue, registry::Registry};
use apexes_strategy::{OrderIntent, ParentOrderId, ParentOrderStatus};
use chrono::{DateTime, TimeDelta, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventLatencyPolicy {
    pub delay_ms: u32,
}

impl EventLatencyPolicy {
    pub fn validate(&self) -> Result<(), String> {
        if self.delay_ms == 0 {
            return Err("event-time delay must be positive".into());
        }
        Ok(())
    }
    pub fn from_registry(registry: &Registry) -> Result<Option<Self>, ApexesError> {
        let p = registry
            .try_one::<Self>()
            .map_err(|e| ApexesError::InvalidBacktestConfig {
                reason: e.to_string(),
            })?;
        if let Some(p) = p {
            p.validate()
                .map_err(|reason| ApexesError::InvalidBacktestConfig { reason })?;
        }
        Ok(p)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventLatencyReport {
    pub policy: EventLatencyPolicy,
    pub scheduled: u64,
    pub arrived: u64,
    pub expired_at_end: u64,
    pub refused: u64,
    pub regressing_events: u64,
    /// Spot market-intent capital reservation evidence (before admission).
    #[serde(default)]
    pub capital_refused: u64,
    #[serde(default)]
    pub capital_released_at_end: u64,
    #[serde(default)]
    pub parent_capital_reserved: u64,
    #[serde(default)]
    pub parent_capital_released: u64,
    #[serde(default)]
    pub parent_capital_released_at_end: u64,
    #[serde(default)]
    pub resting_capital_reserved: u64,
    #[serde(default)]
    pub resting_capital_released: u64,
    #[serde(default)]
    pub resting_capital_released_at_end: u64,
    #[serde(default)]
    pub resting_capital_refused: u64,

    /// Emergency cancellation and admission suppression evidence.
    #[serde(default)]
    pub emergency_events: u64,
    #[serde(default)]
    pub emergency_transit_cancelled: u64,
    #[serde(default)]
    pub emergency_resting_cancelled: u64,
    #[serde(default)]
    pub emergency_parents_cancelled: u64,
    #[serde(default)]
    pub emergency_cancel_failures: u64,
    #[serde(default)]
    pub emergency_intents_refused: u64,

    /// Separate EMS child transit accounting; None for legacy reports.
    #[serde(default)]
    pub child_orders: Option<ChildLatencyReport>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildLatencyReport {
    pub scheduled: u64,
    pub arrived: u64,
    pub refused: u64,
    pub rejected_at_arrival: u64,
    pub cancelled_before_arrival: u64,
    pub expired_at_end: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CapitalHold {
    pub asset: String,
    pub amount: Decimal,
    pub margin: Option<MarginAllocation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MarginAllocation {
    pub closing: Decimal,
    pub unit_margin: Decimal,
    pub unit_fee: Decimal,
}

impl CapitalHold {
    pub fn plain(asset: String, amount: Decimal) -> Self {
        Self {
            asset,
            amount,
            margin: None,
        }
    }

    pub fn leveraged(
        asset: String,
        quantity: Decimal,
        allocation: MarginAllocation,
    ) -> Result<Self, String> {
        if quantity < Decimal::ZERO
            || allocation.closing < Decimal::ZERO
            || allocation.closing > quantity
            || allocation.unit_margin <= Decimal::ZERO
            || allocation.unit_fee < Decimal::ZERO
        {
            return Err("invalid leveraged allocation".into());
        }
        let amount = (quantity - allocation.closing)
            .checked_mul(allocation.unit_margin)
            .and_then(|margin| {
                quantity
                    .checked_mul(allocation.unit_fee)
                    .and_then(|fee| margin.checked_add(fee))
            })
            .ok_or("margin allocation overflow")?;
        Ok(Self {
            asset,
            amount,
            margin: Some(allocation),
        })
    }

    fn closing(&self) -> Decimal {
        self.margin.as_ref().map_or(Decimal::ZERO, |m| m.closing)
    }

    /// A fill consumes closing quantity before opening quantity. For a
    /// terminal child IOC, relinquish its requested allocation even if
    /// only a fraction executed; this cannot underfund future slices.
    fn remainder(&self, previous: Decimal, remaining: Decimal) -> Result<Self, String> {
        if previous <= Decimal::ZERO || remaining < Decimal::ZERO || remaining > previous {
            return Err("invalid reservation remainder".into());
        }
        if let Some(mut allocation) = self.margin.clone() {
            allocation.closing = (allocation.closing - (previous - remaining)).max(Decimal::ZERO);
            return Self::leveraged(self.asset.clone(), remaining, allocation);
        }
        let amount = self
            .amount
            .checked_mul(remaining)
            .and_then(|n| n.checked_div(previous))
            .ok_or("reservation arithmetic overflow")?;
        Ok(Self::plain(self.asset.clone(), amount))
    }
}

pub(crate) fn closing_quantity(
    intent: &OrderIntent,
    positions: &HashMap<String, apexes_strategy::Position>,
) -> Decimal {
    positions
        .get(&intent.symbol)
        .filter(|p| {
            p.leverage.is_some()
                && match intent.side {
                    apexes_core::Side::Buy => p.quantity < Decimal::ZERO,
                    apexes_core::Side::Sell => p.quantity > Decimal::ZERO,
                }
        })
        .map_or(Decimal::ZERO, |p| p.quantity.abs().min(intent.quantity))
}

#[derive(Debug, Clone)]
struct ParentCapital {
    hold: CapitalHold,
    symbol: String,
    venue: Venue,
    side: apexes_core::Side,
    asset_class: apexes_core::AssetClass,
    leverage: Option<Decimal>,
    margin_mode: Option<apexes_core::MarginMode>,
    remaining: Decimal,
}

#[derive(Debug, Clone)]
struct RestingCapital {
    symbol: String,
    hold: CapitalHold,
    remaining: Decimal,
}

fn validate_closing(
    symbol: &str,
    side: apexes_core::Side,
    required: Decimal,
    positions: &HashMap<String, apexes_strategy::Position>,
) -> Result<(), String> {
    if required > Decimal::ZERO
        && !positions.get(symbol).is_some_and(|p| {
            p.leverage.is_some()
                && p.quantity.abs() >= required
                && match side {
                    apexes_core::Side::Buy => p.quantity < Decimal::ZERO,
                    apexes_core::Side::Sell => p.quantity > Decimal::ZERO,
                }
        })
    {
        return Err("reserved closing quantity no longer available".into());
    }
    Ok(())
}

pub(crate) struct ArrivalQueue {
    pending: VecDeque<(DateTime<Utc>, OrderIntent)>,
    holds: HashMap<(Venue, String), CapitalHold>,
    parents: HashMap<ParentOrderId, ParentCapital>,
    resting: HashMap<OrderId, RestingCapital>,
    pub report: EventLatencyReport,
    watermark: Option<DateTime<Utc>>,
}

impl ArrivalQueue {
    pub fn new(policy: EventLatencyPolicy) -> Self {
        Self {
            pending: VecDeque::new(),
            holds: HashMap::new(),
            parents: HashMap::new(),
            resting: HashMap::new(),
            report: EventLatencyReport {
                policy,
                scheduled: 0,
                arrived: 0,
                expired_at_end: 0,
                refused: 0,
                regressing_events: 0,
                capital_refused: 0,
                capital_released_at_end: 0,
                parent_capital_reserved: 0,
                parent_capital_released: 0,
                parent_capital_released_at_end: 0,
                resting_capital_reserved: 0,
                resting_capital_released: 0,
                resting_capital_released_at_end: 0,
                resting_capital_refused: 0,
                emergency_events: 0,
                emergency_transit_cancelled: 0,
                emergency_resting_cancelled: 0,
                emergency_parents_cancelled: 0,
                emergency_cancel_failures: 0,
                emergency_intents_refused: 0,
                child_orders: None,
            },
            watermark: None,
        }
    }
    pub fn observe(&mut self, at: DateTime<Utc>) -> bool {
        if self.watermark.is_some_and(|old| at < old) {
            self.report.regressing_events += 1;
            return false;
        }
        self.watermark = Some(at);
        true
    }
    fn reserved(&self, asset: &str, excluding: Option<ParentOrderId>) -> Option<Decimal> {
        self.holds
            .values()
            .chain(
                self.parents
                    .iter()
                    .filter(|(id, _)| Some(**id) != excluding)
                    .map(|(_, p)| &p.hold),
            )
            .chain(self.resting.values().map(|r| &r.hold))
            .filter(|h| h.asset == asset)
            .try_fold(Decimal::ZERO, |sum, h| sum.checked_add(h.amount))
    }

    /// Read the same reservation totals used for admission. Scope this to
    /// configured unit-multiplier Spot assets; leveraged/mixed sessions
    /// need a distinct margin-aware availability contract.
    pub(crate) fn capital_snapshot(
        &self,
        instruments: &[apexes_core::Instrument],
        positions: &HashMap<String, apexes_strategy::Position>,
        balances: &HashMap<String, Decimal>,
    ) -> Option<HashMap<String, apexes_strategy::CapitalObservation>> {
        use std::collections::HashSet;
        let mut symbols = HashSet::new();
        let mut assets = HashSet::new();
        for instrument in instruments {
            let base = instrument
                .base_currency
                .as_deref()
                .filter(|a| !a.is_empty())?;
            let quote = instrument
                .quote_currency
                .as_deref()
                .filter(|a| !a.is_empty())?;
            if instrument.asset_class != apexes_core::AssetClass::Spot
                || instrument.multiplier != Decimal::ONE
                || base == quote
                || !symbols.insert(instrument.symbol.as_str())
            {
                return None;
            }
            assets.extend([base, quote]);
        }
        if symbols.is_empty()
            || positions.values().any(|p| {
                p.leverage.is_some()
                    || p.quantity < Decimal::ZERO
                    || !symbols.contains(p.symbol.as_str())
            })
        {
            return None;
        }
        let mut result = HashMap::new();
        for asset in assets {
            if let Some(balance) = balances.get(asset) {
                // Overflow or invalid ledger values are unavailable, never zero.
                if let Some(observation) = self.reserved(asset, None).and_then(|reserved| {
                    apexes_strategy::CapitalObservation::from_balances(*balance, reserved)
                }) {
                    result.insert(asset.to_owned(), observation);
                }
            }
        }
        Some(result)
    }

    /// The ledger is keyed by symbol. Closing quantity is granted only
    /// when no other order on that symbol owns execution capacity.
    pub fn has_symbol_owner(&self, symbol: &str) -> bool {
        self.holds.keys().any(|(_, s)| s == symbol)
            || self.parents.values().any(|p| p.symbol == symbol)
            || self.resting.values().any(|r| r.symbol == symbol)
    }

    pub fn has_closing_owner(&self, symbol: &str) -> bool {
        self.holds
            .iter()
            .any(|((_, s), h)| s == symbol && h.closing() > Decimal::ZERO)
            || self
                .parents
                .values()
                .any(|p| p.symbol == symbol && p.hold.closing() > Decimal::ZERO)
            || self
                .resting
                .values()
                .any(|r| r.symbol == symbol && r.hold.closing() > Decimal::ZERO)
    }

    pub fn validate_resting_claim(
        &self,
        id: OrderId,
        fills: &[Fill],
        positions: &HashMap<String, apexes_strategy::Position>,
    ) -> Result<(), String> {
        if let Some(r) = self.resting.get(&id)
            && r.hold.closing() > Decimal::ZERO
            && let Some(fill) = fills.first()
        {
            let required = r.hold.closing().min(fills.iter().map(|f| f.quantity).sum());
            validate_closing(&r.symbol, fill.side, required, positions)?;
        }
        Ok(())
    }

    pub fn validate_child_claim(
        &self,
        parent: Option<ParentOrderId>,
        intent: &OrderIntent,
        positions: &HashMap<String, apexes_strategy::Position>,
    ) -> Result<(), String> {
        if let Some(p) = parent.and_then(|id| self.parents.get(&id)) {
            validate_closing(
                &p.symbol,
                p.side,
                p.hold.closing().min(intent.quantity),
                positions,
            )?;
        }
        Ok(())
    }

    pub fn check_capital(&self, hold: &CapitalHold, balances: &HashMap<String, Decimal>) -> bool {
        hold.amount >= Decimal::ZERO
            && (hold.amount > Decimal::ZERO || hold.margin.is_some())
            && self
                .reserved(&hold.asset, None)
                .and_then(|amount| amount.checked_add(hold.amount))
                .is_some_and(|amount| {
                    balances.get(&hold.asset).copied().unwrap_or_default() >= amount
                })
    }

    pub fn reserve_parent(&mut self, id: ParentOrderId, intent: &OrderIntent, hold: CapitalHold) {
        self.parents.insert(
            id,
            ParentCapital {
                hold,
                symbol: intent.symbol.clone(),
                venue: intent.venue,
                side: intent.side,
                asset_class: intent.asset_class,
                leverage: intent.leverage,
                margin_mode: intent.margin_mode,
                remaining: intent.quantity,
            },
        );
        self.report.parent_capital_reserved += 1;
    }

    /// Retain holds on unknown status (including status lookup failures).
    /// Only an explicitly terminal parent releases the residual budget.
    pub fn reconcile_parents(&mut self, statuses: &HashMap<ParentOrderId, ParentOrderStatus>) {
        let before = self.parents.len();
        self.parents.retain(|id, _| {
            !matches!(
                statuses.get(id),
                Some(
                    ParentOrderStatus::Completed
                        | ParentOrderStatus::Cancelled
                        | ParentOrderStatus::Failed { .. }
                )
            )
        });
        self.report.parent_capital_released += (before - self.parents.len()) as u64;
    }

    pub fn check_direct(
        &self,
        hold: Option<&CapitalHold>,
        quantity: Decimal,
        remaining: Decimal,
        before: &HashMap<String, Decimal>,
        after: &HashMap<String, Decimal>,
    ) -> Result<(), String> {
        if quantity <= Decimal::ZERO || remaining < Decimal::ZERO || remaining > quantity {
            return Err("invalid direct reservation quantity".into());
        }
        if let Some(hold) = hold
            && remaining > Decimal::ZERO
            && !self.check_capital(&hold.remainder(quantity, remaining)?, after)
        {
            return Err("insufficient capital for working remainder".into());
        }
        for asset in self
            .holds
            .values()
            .map(|h| &h.asset)
            .chain(self.parents.values().map(|p| &p.hold.asset))
            .chain(self.resting.values().map(|r| &r.hold.asset))
        {
            let old = before.get(asset).copied().unwrap_or_default();
            let new = after.get(asset).copied().unwrap_or_default();
            let reserved = self
                .reserved(asset, None)
                .ok_or("capital reservation arithmetic overflow")?;
            if new < old && new < reserved {
                return Err("direct fill would consume another order's reservation".into());
            }
        }
        Ok(())
    }

    pub fn reserve_resting(&mut self, order: &Order, hold: CapitalHold) -> Result<(), String> {
        let remaining = order.quantity - order.filled_quantity;
        if remaining <= Decimal::ZERO {
            return Ok(());
        }
        let hold = hold.remainder(order.quantity, remaining)?;
        self.resting.insert(
            order.id,
            RestingCapital {
                symbol: order.symbol.clone(),
                hold,
                remaining,
            },
        );
        self.report.resting_capital_reserved += 1;
        Ok(())
    }

    pub fn reconcile_resting(&mut self, orders: &HashMap<OrderId, Order>) {
        let before = self.resting.len();
        self.resting.retain(|id, _| orders.contains_key(id));
        self.report.resting_capital_released += (before - self.resting.len()) as u64;
    }

    /// Match only against actual balances net of all other reservations.
    /// Reduce the current order's hold proportionally to the proposed fill;
    /// an IOC trigger releases its own entire hold at terminal execution.
    pub fn accept_resting(
        &mut self,
        id: OrderId,
        fills: &[Fill],
        terminal: bool,
        before: &HashMap<String, Decimal>,
        after: &HashMap<String, Decimal>,
    ) -> Result<(), String> {
        let old_hold = self.resting.get(&id);
        let remaining = if let Some(r) = old_hold {
            let executed = fills
                .iter()
                .try_fold(Decimal::ZERO, |n, f| {
                    if f.order_id != id || f.quantity <= Decimal::ZERO {
                        return None;
                    }
                    n.checked_add(f.quantity)
                })
                .ok_or("invalid resting fill allocation")?;
            if executed > r.remaining {
                return Err("resting fill exceeds reserved quantity".into());
            }
            if terminal {
                Decimal::ZERO
            } else {
                r.remaining - executed
            }
        } else {
            Decimal::ZERO
        };
        let replacement = old_hold
            .map(|r| r.hold.remainder(r.remaining, remaining))
            .transpose()?;
        let new_amount = replacement.as_ref().map_or(Decimal::ZERO, |h| h.amount);
        for asset in self
            .holds
            .values()
            .map(|h| &h.asset)
            .chain(self.parents.values().map(|p| &p.hold.asset))
            .chain(self.resting.values().map(|r| &r.hold.asset))
        {
            let mut total = self
                .reserved(asset, None)
                .ok_or("capital reservation arithmetic overflow")?;
            if let Some(r) = old_hold
                && &r.hold.asset == asset
            {
                total = total
                    .checked_sub(r.hold.amount)
                    .and_then(|n| n.checked_add(new_amount))
                    .ok_or("capital reservation arithmetic overflow")?;
            }
            let old = before.get(asset).copied().unwrap_or_default();
            let new = after.get(asset).copied().unwrap_or_default();
            if new < old && new < total {
                return Err("resting fill would consume another order's reservation".into());
            }
        }
        if let Some(r) = self.resting.get_mut(&id)
            && let Some(hold) = replacement
        {
            r.remaining = remaining;
            r.hold = hold;
        }
        Ok(())
    }

    pub fn capital_refused(&mut self, before_admission: bool) {
        self.report.capital_refused += 1;
        if before_admission {
            self.report.refused += 1;
        }
    }

    /// Keep due holds until each intent is individually processed; extracting
    /// the whole due batch must not give the first arrival another's funds.
    pub fn release(&mut self, intent: &OrderIntent) {
        self.holds.remove(&(intent.venue, intent.symbol.clone()));
    }

    /// Reallocate only the arriving child's proportional share. Price
    /// increases may use genuinely free funds, never a sibling's hold.
    /// Commit the new hold only after the entire execution proposal passes.
    pub fn accept_child(
        &mut self,
        parent: Option<ParentOrderId>,
        intent: &OrderIntent,
        before: &HashMap<String, Decimal>,
        after: &HashMap<String, Decimal>,
    ) -> Result<(), String> {
        let replacement = if let Some(id) = parent
            && let Some(p) = self.parents.get(&id)
        {
            if p.symbol != intent.symbol
                || p.venue != intent.venue
                || p.side != intent.side
                || p.asset_class != intent.asset_class
                || p.leverage != intent.leverage
                || p.margin_mode != intent.margin_mode
                || intent.quantity <= Decimal::ZERO
                || intent.quantity > p.remaining
            {
                return Err("child does not match parent capital allocation".into());
            }
            let remaining = p.remaining - intent.quantity;
            Some((id, remaining, p.hold.remainder(p.remaining, remaining)?))
        } else {
            if parent.is_some()
                && matches!(
                    intent.asset_class,
                    apexes_core::AssetClass::Spot | apexes_core::AssetClass::Perpetual
                )
            {
                return Err("missing parent capital reservation".into());
            }
            None
        };
        let excluding = replacement.as_ref().map(|(id, _, _)| *id);
        for asset in self
            .holds
            .values()
            .map(|h| &h.asset)
            .chain(self.parents.values().map(|p| &p.hold.asset))
            .chain(self.resting.values().map(|r| &r.hold.asset))
        {
            let old = before.get(asset).copied().unwrap_or_default();
            let new = after.get(asset).copied().unwrap_or_default();
            let additional = replacement
                .as_ref()
                .filter(|(_, _, h)| &h.asset == asset)
                .map_or(Decimal::ZERO, |(_, _, h)| h.amount);
            let total = self
                .reserved(asset, excluding)
                .and_then(|n| n.checked_add(additional))
                .ok_or("capital reservation arithmetic overflow")?;
            if new < old && new < total {
                return Err(
                    "child would spend capital reserved for another order or sibling slice".into(),
                );
            }
        }
        if let Some((id, remaining, hold)) = replacement
            && let Some(p) = self.parents.get_mut(&id)
        {
            p.remaining = remaining;
            p.hold = hold;
        }
        Ok(())
    }

    pub fn schedule(
        &mut self,
        at: DateTime<Utc>,
        intent: OrderIntent,
        hold: Option<CapitalHold>,
    ) -> Result<(), &'static str> {
        let deadline = at.checked_add_signed(TimeDelta::milliseconds(i64::from(
            self.report.policy.delay_ms,
        )));
        let reason = if self
            .pending
            .iter()
            .any(|(_, p)| p.venue == intent.venue && p.symbol == intent.symbol)
        {
            Some("event_latency_symbol_pending")
        } else if self.pending.len() >= 1024 {
            Some("event_latency_queue_full")
        } else if deadline.is_none() {
            Some("event_latency_timestamp_overflow")
        } else {
            None
        };
        if let Some(reason) = reason {
            self.report.refused += 1;
            return Err(reason);
        }
        let deadline = deadline.ok_or("event_latency_timestamp_overflow")?;
        if let Some(hold) = hold {
            self.holds
                .insert((intent.venue, intent.symbol.clone()), hold);
        }
        self.pending.push_back((deadline, intent));
        self.report.scheduled += 1;
        Ok(())
    }
    pub fn cancel_transit_for_emergency(&mut self) -> Vec<OrderIntent> {
        let intents: Vec<_> = self.pending.drain(..).map(|(_, intent)| intent).collect();
        for intent in &intents {
            self.release(intent);
        }
        self.report.emergency_transit_cancelled += intents.len() as u64;
        intents
    }

    pub fn due(&mut self, at: DateTime<Utc>) -> Vec<OrderIntent> {
        let mut due = Vec::new();
        while self
            .pending
            .front()
            .is_some_and(|(deadline, _)| *deadline <= at)
        {
            if let Some((_, intent)) = self.pending.pop_front() {
                due.push(intent);
                self.report.arrived += 1;
            }
        }
        due
    }
    pub fn finish(mut self) -> EventLatencyReport {
        self.report.expired_at_end = self.pending.len() as u64;
        self.report.capital_released_at_end = self.holds.len() as u64;
        self.report.parent_capital_released_at_end = self.parents.len() as u64;
        self.report.resting_capital_released_at_end = self.resting.len() as u64;
        self.report
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, OrderType, Side, Venue};
    use apexes_strategy::OrderRouting;
    use rust_decimal::Decimal;

    fn intent(symbol: &str) -> OrderIntent {
        OrderIntent {
            symbol: symbol.into(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: Decimal::ONE,
            price: None,
            stop_price: None,
            execution_algo: None,
            routing: OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        }
    }

    #[test]
    fn capital_snapshot_sums_shared_wallet_holds_and_retains_unknown_parent() {
        use apexes_core::{Instrument, MarketType};
        let d = Decimal::from;
        let instruments: Vec<_> = [("BTCUSDT", "BTC"), ("ETHUSDT", "ETH")]
            .into_iter()
            .map(|(symbol, base)| {
                Instrument::new(
                    symbol,
                    MarketType::Crypto,
                    Venue::Binance,
                    AssetClass::Spot,
                    Some(base.into()),
                    Some("USDT".into()),
                    Decimal::ONE,
                    Decimal::ONE,
                    Decimal::ONE,
                )
                .unwrap()
            })
            .collect();
        let balances = HashMap::from([("USDT".into(), d(100)), ("BTC".into(), Decimal::ZERO)]);
        let positions = HashMap::new();
        let mut queue = ArrivalQueue::new(EventLatencyPolicy { delay_ms: 500 });
        let parent = ParentOrderId(uuid::Uuid::new_v4());
        queue.reserve_parent(
            parent,
            &intent("BTCUSDT"),
            CapitalHold::plain("USDT".into(), d(30)),
        );
        let pending = intent("ETHUSDT");
        queue
            .schedule(
                Utc::now(),
                pending.clone(),
                Some(CapitalHold::plain("USDT".into(), d(20))),
            )
            .unwrap();
        let snapshot = |q: &ArrivalQueue| {
            q.capital_snapshot(&instruments, &positions, &balances)
                .unwrap()
        };
        assert_eq!(snapshot(&queue)["USDT"].unreserved, d(50));
        assert_eq!(snapshot(&queue)["BTC"].unreserved, Decimal::ZERO);
        assert!(!snapshot(&queue).contains_key("ETH"));
        queue.reconcile_parents(&HashMap::new());
        assert_eq!(snapshot(&queue)["USDT"].reserved, d(50));
        queue.reconcile_parents(&HashMap::from([(parent, ParentOrderStatus::Cancelled)]));
        assert_eq!(snapshot(&queue)["USDT"].reserved, d(20));
        queue.release(&pending);
        assert_eq!(snapshot(&queue)["USDT"].unreserved, d(100));
        let mut mixed = instruments.clone();
        mixed[0].asset_class = AssetClass::Perpetual;
        assert!(
            queue
                .capital_snapshot(&mixed, &positions, &balances)
                .is_none()
        );
        let mut ambiguous = instruments.clone();
        ambiguous.push(instruments[0].clone());
        assert!(
            queue
                .capital_snapshot(&ambiguous, &positions, &balances)
                .is_none()
        );
    }

    #[test]
    fn leveraged_remainder_consumes_closing_quantity_before_opening_margin() {
        let hold = CapitalHold::leveraged(
            "USDT".into(),
            Decimal::from(4),
            MarginAllocation {
                closing: Decimal::from(2),
                unit_margin: Decimal::from(5),
                unit_fee: Decimal::ONE,
            },
        )
        .unwrap();
        assert_eq!(hold.amount, Decimal::from(14));
        let reduced = hold.remainder(Decimal::from(4), Decimal::from(3)).unwrap();
        assert_eq!(reduced.amount, Decimal::from(13));
        assert_eq!(reduced.closing(), Decimal::ONE);
        let reduced = reduced
            .remainder(Decimal::from(3), Decimal::from(2))
            .unwrap();
        assert_eq!(reduced.amount, Decimal::from(12));
        assert_eq!(reduced.closing(), Decimal::ZERO);
        let reduced = reduced.remainder(Decimal::from(2), Decimal::ONE).unwrap();
        assert_eq!(reduced.amount, Decimal::from(6));
    }

    #[test]
    fn closing_claim_rejects_missing_or_reversed_position() {
        let mut positions = HashMap::new();
        assert!(validate_closing("BTCUSDT", Side::Sell, Decimal::ONE, &positions).is_err());
        positions.insert(
            "BTCUSDT".into(),
            apexes_strategy::Position {
                symbol: "BTCUSDT".into(),
                quantity: -Decimal::ONE,
                average_entry_price: Decimal::from(10),
                leverage: Some(Decimal::from(2)),
                margin_mode: Some(apexes_core::MarginMode::Isolated),
                allocated_margin: Decimal::from(5),
            },
        );
        assert!(validate_closing("BTCUSDT", Side::Sell, Decimal::ONE, &positions).is_err());
        assert!(validate_closing("BTCUSDT", Side::Buy, Decimal::ONE, &positions).is_ok());
        assert!(validate_closing("BTCUSDT", Side::Buy, Decimal::from(2), &positions).is_err());
    }

    #[test]
    fn parent_child_spends_only_its_share_and_terminal_parent_releases_residual() {
        let mut queue = ArrivalQueue::new(EventLatencyPolicy { delay_ms: 500 });
        let parent = ParentOrderId(uuid::Uuid::new_v4());
        let mut request = intent("BTCUSDT");
        request.quantity = Decimal::from(2);
        let hold = CapitalHold::plain("USDT".into(), Decimal::from(12));
        let balances = HashMap::from([("USDT".into(), Decimal::from(15))]);
        queue.reserve_parent(parent, &request, hold);
        let other = CapitalHold::plain("USDT".into(), Decimal::from(4));
        assert!(!queue.check_capital(&other, &balances));
        let child = intent("BTCUSDT");
        // First child cannot spend 10: the other slice still owns 6.
        assert!(
            queue
                .accept_child(
                    Some(parent),
                    &child,
                    &balances,
                    &HashMap::from([("USDT".into(), Decimal::from(5))])
                )
                .is_err()
        );
        // Partial execution consuming 3 releases only this child's 6 hold.
        let after = HashMap::from([("USDT".into(), Decimal::from(12))]);
        queue
            .accept_child(Some(parent), &child, &balances, &after)
            .unwrap();
        assert!(queue.check_capital(&other, &after));
        assert_eq!(queue.reserved("USDT", None), Some(Decimal::from(6)));
        // Unknown status or Scheduling must not release the sibling hold.
        queue.reconcile_parents(&HashMap::new());
        queue.reconcile_parents(&HashMap::from([(parent, ParentOrderStatus::Scheduling)]));
        assert_eq!(queue.reserved("USDT", None), Some(Decimal::from(6)));
        queue.reconcile_parents(&HashMap::from([(parent, ParentOrderStatus::Cancelled)]));
        queue.reconcile_parents(&HashMap::from([(parent, ParentOrderStatus::Cancelled)]));
        assert_eq!(queue.reserved("USDT", None), Some(Decimal::ZERO));
        let report = queue.finish();
        assert_eq!(
            (
                report.parent_capital_reserved,
                report.parent_capital_released,
                report.parent_capital_released_at_end
            ),
            (1, 1, 0)
        );
    }

    #[test]
    fn parent_budget_cannot_be_claimed_by_unrelated_child_and_releases_at_end() {
        let mut queue = ArrivalQueue::new(EventLatencyPolicy { delay_ms: 500 });
        let parent = ParentOrderId(uuid::Uuid::new_v4());
        let request = intent("BTCUSDT");
        queue.reserve_parent(
            parent,
            &request,
            CapitalHold::plain("USDT".into(), Decimal::from(6)),
        );
        let balance = HashMap::from([("USDT".into(), Decimal::from(6))]);
        let after = HashMap::from([("USDT".into(), Decimal::ZERO)]);
        assert!(
            queue
                .accept_child(Some(parent), &intent("ETHUSDT"), &balance, &after)
                .is_err()
        );
        assert!(
            queue
                .accept_child(
                    Some(ParentOrderId(uuid::Uuid::new_v4())),
                    &request,
                    &balance,
                    &after
                )
                .is_err()
        );
        assert!(
            queue
                .accept_child(None, &request, &balance, &after)
                .is_err()
        );
        assert_eq!(queue.finish().parent_capital_released_at_end, 1);
    }

    #[test]
    fn due_batch_preserves_other_intents_holds_until_individual_release() {
        let at = Utc::now();
        let mut queue = ArrivalQueue::new(EventLatencyPolicy { delay_ms: 500 });
        let hold = CapitalHold::plain("USDT".into(), Decimal::from(6));
        let balances = HashMap::from([("USDT".into(), Decimal::from(12))]);
        let first = intent("BTCUSDT");
        let second = intent("ETHUSDT");
        assert!(queue.check_capital(&hold, &balances));
        queue
            .schedule(at, first.clone(), Some(hold.clone()))
            .unwrap();
        assert!(queue.check_capital(&hold, &balances));
        queue
            .schedule(at, second.clone(), Some(hold.clone()))
            .unwrap();
        assert!(!queue.check_capital(&hold, &balances));
        let due = queue.due(at + TimeDelta::milliseconds(500));
        assert_eq!(due.len(), 2);
        assert!(!queue.check_capital(&hold, &balances));
        queue.release(&first);
        let expensive = CapitalHold::plain("USDT".into(), Decimal::from(7));
        assert!(!queue.check_capital(&expensive, &balances));
        assert!(
            queue
                .accept_child(
                    None,
                    &first,
                    &balances,
                    &HashMap::from([("USDT".into(), Decimal::from(5))])
                )
                .is_err()
        );
        queue.release(&second);
        assert!(queue.check_capital(&expensive, &balances));
        queue
            .schedule(at + TimeDelta::milliseconds(500), second, Some(hold))
            .unwrap();
        let report = queue.finish();
        assert_eq!(report.capital_released_at_end, 1);
        assert_eq!(report.expired_at_end, 1);
    }

    #[test]
    fn fifo_duplicate_regression_and_expiry_are_deterministic() {
        let at = Utc::now();
        let mut queue = ArrivalQueue::new(EventLatencyPolicy { delay_ms: 500 });
        assert!(queue.observe(at));
        queue.schedule(at, intent("BTCUSDT"), None).unwrap();
        queue.schedule(at, intent("ETHUSDT"), None).unwrap();
        assert_eq!(
            queue.schedule(at, intent("BTCUSDT"), None),
            Err("event_latency_symbol_pending")
        );
        assert!(!queue.observe(at - TimeDelta::milliseconds(1)));
        assert!(queue.due(at + TimeDelta::milliseconds(499)).is_empty());
        let arrived = queue.due(at + TimeDelta::milliseconds(500));
        assert_eq!(
            arrived
                .iter()
                .map(|i| i.symbol.as_str())
                .collect::<Vec<_>>(),
            vec!["BTCUSDT", "ETHUSDT"]
        );
        queue
            .schedule(at + TimeDelta::milliseconds(500), intent("BTCUSDT"), None)
            .unwrap();
        let report = queue.finish();
        assert_eq!(
            (
                report.scheduled,
                report.arrived,
                report.expired_at_end,
                report.refused,
                report.regressing_events
            ),
            (3, 2, 1, 1, 1)
        );
    }
}
