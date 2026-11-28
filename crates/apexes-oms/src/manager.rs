//! [`OrderManager`]: the business-logic layer -- risk checks, gateway
//! submission, status tracking, and reconnect reconciliation. Holds an
//! in-memory `HashMap<OrderId, Order>` snapshot of open orders (mirroring
//! `apexes_backtest::PortfolioLedger`'s in-memory-plus-persistence
//! pattern), rebuilt from `OrderStore::get_open_orders` on construction
//! so a process restart recovers its view of in-flight orders without
//! waiting for the next `reconcile()` call.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use apexes_core::registry::Registry;
use apexes_core::{AssetClass, Fill, FillId, Order, OrderId, OrderStatus, Side};
use apexes_risk::CompositeRiskChecker;
use apexes_strategy::{OrderIntent, Position};
use chrono::Utc;
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::error::OmsError;
use crate::gateway::{ExecutionGateway, GatewayOutcome};
use crate::kill_switch::KillSwitch;
use crate::store::OrderStore;

/// Outcome of [`OrderManager::cancel_all_open`].
#[derive(Debug, Default)]
pub struct CancelAllReport {
    /// Orders whose cancellation the exchange accepted.
    pub cancelled: Vec<OrderId>,
    /// Orders whose cancellation failed (rejected / query error); still
    /// believed open, worth retrying.
    pub failed: Vec<(OrderId, String)>,
    /// Orders with no known exchange id even after a reconcile attempt
    /// (submission outcome still unknown); cannot be cancelled yet.
    pub unresolved: Vec<OrderId>,
}

impl CancelAllReport {
    pub fn is_complete(&self) -> bool {
        self.failed.is_empty() && self.unresolved.is_empty()
    }
}

/// Statuses whose transition may have produced fills that must be
/// fetched: partial/full fills, and a cancel (which can follow partial
/// fills).
fn status_carries_fills(status: OrderStatus) -> bool {
    matches!(
        status,
        OrderStatus::PartiallyFilled | OrderStatus::Filled | OrderStatus::Cancelled
    )
}

/// Default for [`OrderManager::with_unsent_order_grace`].
const DEFAULT_UNSENT_ORDER_GRACE: std::time::Duration = std::time::Duration::from_secs(60);

struct FillDelivery {
    checkpoint: i64,
    pending: Option<(i64, Vec<FillId>)>,
    dirty: bool,
    failed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SpotReservation {
    pub(crate) spend_asset: String,
    pub(crate) receipt_asset: String,
    pub(crate) amount: Decimal,
    pub(crate) owned_quantity: Decimal,
    pub(crate) reference_price: Decimal,
    pub(crate) market_buy_cap: Option<Decimal>,
    pub(crate) commission: Option<crate::SpotCommission>,
}

pub struct OrderManager {
    store: OrderStore,
    gateway: Arc<dyn ExecutionGateway>,
    risk_checker: Option<Arc<CompositeRiskChecker>>,
    open_orders: HashMap<OrderId, Order>,
    kill_switch: Option<Arc<KillSwitch>>,
    // Process-local acknowledgement watermark; startup rebuilds from all
    // durable history before establishing a new watermark.
    fill_delivery: Option<FillDelivery>,
    spot_scope: Option<HashMap<String, (String, String)>>,
    // Retain the full spend reservation until authoritative terminal state.
    // Recovery remains quiescent; unknown recovered reservations refuse orders.
    spot_reservations: HashMap<OrderId, SpotReservation>,
    reservation_breached: bool,
    /// Age threshold for an unresolved-submission warning, not an expiry.
    unsent_order_grace: std::time::Duration,
}

impl OrderManager {
    /// Constructs a new manager, rebuilding `open_orders` from
    /// `store.get_open_orders()` -- the "recover in-flight order state
    /// after a process restart" half of reconnect reconciliation;
    /// `reconcile()` is the other half (comparing that recovered state
    /// against the exchange's current truth). `gateway` (exactly one
    /// `Arc<dyn ExecutionGateway>`) is looked up via `registry.one()` --
    /// a missing or ambiguous (2+) registration is a hard error, since
    /// an `OrderManager` cannot function without exactly one gateway.
    /// `risk_checker` (`Arc<CompositeRiskChecker>`) is looked up via
    /// `registry.try_one()` -- absent is valid and means "no risk
    /// checking configured", matching this method's previous `None`
    /// parameter semantics exactly.
    pub async fn new(store: OrderStore, registry: &Registry) -> Result<Self, OmsError> {
        let gateway = registry.one::<Arc<dyn ExecutionGateway>>()?;
        let risk_checker = registry.try_one::<Arc<CompositeRiskChecker>>()?;

        let open_orders_vec = store.get_open_orders().await?;
        let reservation_breached =
            store.inventory_bound() && store.has_reservation_breach().await?;
        let spot_reservations = if store.inventory_bound() {
            store.load_spot_reservations().await?
        } else {
            HashMap::new()
        };
        let open_orders = open_orders_vec
            .into_iter()
            .map(|order| (order.id, order))
            .collect();
        Ok(Self {
            store,
            gateway,
            risk_checker,
            open_orders,
            kill_switch: None,
            fill_delivery: None,
            spot_scope: None,
            spot_reservations,
            reservation_breached,
            unsent_order_grace: DEFAULT_UNSENT_ORDER_GRACE,
        })
    }

    /// Restrict managed order identity to the configured live instruments.
    pub fn with_spot_scope(mut self, instruments: &[apexes_core::Instrument]) -> Self {
        self.spot_scope = Some(
            instruments
                .iter()
                .filter_map(|i| {
                    if i.asset_class != AssetClass::Spot || i.venue != apexes_core::Venue::Binance {
                        return None;
                    }
                    Some((
                        i.symbol.clone(),
                        (i.base_currency.clone()?, i.quote_currency.clone()?),
                    ))
                })
                .collect(),
        );
        self
    }

    /// The bound store's persisted spot fills after startup reconciliation.
    pub async fn spot_inventory_fills(&mut self) -> Result<Vec<Fill>, OmsError> {
        let fills = self.store.spot_inventory_fills().await?;
        // Before any background task or new submission is allowed, all
        // persisted fills are represented by the recovered ledger.
        self.fill_delivery = Some(FillDelivery {
            checkpoint: self.store.latest_fill_sequence().await?,
            pending: None,
            dirty: false,
            failed: false,
        });
        Ok(fills)
    }

    /// Managed sessions deliver from the journal, not from return values of
    /// reconciliation/cancellation tasks. Legacy adapters use None.
    pub async fn durable_fill_batch(&mut self) -> Option<Vec<Fill>> {
        let delivery = self.fill_delivery.as_ref()?;
        if delivery.failed {
            return Some(Vec::new());
        }
        if delivery.pending.is_some() {
            self.fail_fill_delivery();
            return Some(Vec::new());
        }
        let checkpoint = delivery.checkpoint;
        match self.store.fills_after(checkpoint).await {
            Ok((high, fills)) => {
                if let Some(delivery) = &mut self.fill_delivery {
                    delivery.dirty = !fills.is_empty();
                    if !fills.is_empty() {
                        delivery.pending = Some((high, fills.iter().map(|f| f.id).collect()));
                    }
                }
                Some(fills)
            }
            Err(_) => {
                self.fail_fill_delivery();
                Some(Vec::new())
            }
        }
    }

    pub async fn acknowledge_fill_batch(&mut self, ids: &[FillId], applied: bool) {
        let Some(delivery) = &mut self.fill_delivery else {
            if !applied {
                self.report_health_fault("live ledger rejected exchange fill");
            }
            return;
        };
        let Some((high, pending)) = delivery.pending.take() else {
            self.fail_fill_delivery();
            return;
        };
        if !applied || pending != ids {
            self.fail_fill_delivery();
            return;
        }
        delivery.checkpoint = high;
        // Another task may have recorded fills while the loop applied
        // this batch. Keep the submission gate closed until they arrive.
        match self.store.latest_fill_sequence().await {
            Ok(latest) => delivery.dirty = latest > high,
            Err(_) => self.fail_fill_delivery(),
        }
    }

    fn fail_fill_delivery(&mut self) {
        if let Some(delivery) = &mut self.fill_delivery {
            delivery.failed = true;
            delivery.dirty = true;
        }
        tracing::error!("durable fill delivery inconsistent; restart and reconcile required");
        self.report_health_fault("live fill delivery or ledger discrepancy");
    }

    /// Sets the age threshold for warning about an unresolved submission.
    /// No age or negative lookup is proof of non-execution.
    pub fn with_unsent_order_grace(mut self, grace: std::time::Duration) -> Self {
        self.unsent_order_grace = grace;
        self
    }

    /// Attaches a [`KillSwitch`]: while engaged, `submit_order` fails
    /// with `OmsError::TradingHalted` (cancels remain allowed).
    pub fn with_kill_switch(mut self, kill_switch: Arc<KillSwitch>) -> Self {
        self.kill_switch = Some(kill_switch);
        self
    }

    /// Clone the exact submission gate before sharing the manager with
    /// async exchange tasks. Reading it never requires the manager lock.
    pub fn submission_gate(&self) -> Option<Arc<KillSwitch>> {
        self.kill_switch.clone()
    }

    /// Submits a new order:
    /// 1. Runs `risk_checker` (if configured) against `intent`; returns
    ///    `Err(OmsError::RiskCheckFailed)` without creating any order if
    ///    it fails. `positions`/`balances` are caller-supplied snapshots
    ///    passed straight through to `risk_checker.check(..)` --
    ///    `OrderManager` does not own position/balance tracking itself
    ///    (that is sub-project #11's scope).
    /// 2. Constructs an `Order` (status `New`, `exchange_order_id:
    ///    None`), persists it via `store.upsert_order` BEFORE calling
    ///    the gateway (a local record the gateway never accepted is
    ///    recoverable via reconciliation or manual review; a
    ///    gateway-accepted order with no local record is not).
    /// 3. Calls `gateway.submit_order`:
    ///    - `Accepted { exchange_order_id }`: updates the order's
    ///      `exchange_order_id` field, re-persists, inserts into
    ///      `open_orders`, returns `Ok(order_id)`.
    ///    - `Rejected { reason }`: transitions the order to `Rejected`,
    ///      re-persists, does NOT insert into `open_orders`, returns
    ///      `Err(OmsError::OrderRejected { reason })`.
    ///    - `Uncertain { reason }`: leaves the order's status as `New`
    ///      (already persisted), inserts into `open_orders` anyway (it
    ///      is genuinely uncertain whether it's live), logs a
    ///      `tracing::warn!`, and returns `Ok(order_id)` -- only
    ///      `reconcile()` will later resolve the true status.
    pub async fn submit_order(
        &mut self,
        intent: OrderIntent,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
    ) -> Result<OrderId, OmsError> {
        self.submit_order_inner(intent, positions, balances, false, None, (None, None))
            .await
    }

    /// Persist the scoped strategy identity in the same transaction as
    /// the client order and any reservations before gateway submission.
    pub async fn submit_correlated_order(
        &mut self,
        intent: OrderIntent,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
        link: crate::IntentLink,
    ) -> Result<OrderId, OmsError> {
        self.submit_order_inner(
            intent,
            positions,
            balances,
            false,
            None,
            (Some(link.validate()?), None),
        )
        .await
    }

    /// EMS already persisted its dispatch quantity. Refuse rather than
    /// silently changing the quantity recorded by the parent scheduler.
    pub async fn submit_child_order(&mut self, intent: OrderIntent) -> Result<OrderId, OmsError> {
        self.submit_order_inner(
            intent,
            &HashMap::new(),
            &HashMap::new(),
            true,
            None,
            (None, None),
        )
        .await
    }

    /// Reserve a complete parent budget before EMS persists/schedules it.
    /// Full quantity must conform at admission; each child revalidates.
    pub async fn reserve_ems_parent(
        &mut self,
        id: Uuid,
        mut intent: OrderIntent,
    ) -> Result<(), OmsError> {
        if !self.store.inventory_bound() {
            return Ok(());
        }
        if self
            .kill_switch
            .as_ref()
            .is_some_and(|k| k.blocks_submissions())
            || self
                .fill_delivery
                .as_ref()
                .is_none_or(|d| d.dirty || d.failed)
            || self.reservation_breached
        {
            return Err(OmsError::TradingHalted {
                reason: "managed parent admission requires reconciled healthy inventory".into(),
            });
        }
        self.store.verify_inventory_lease().await?;
        if intent.routing != apexes_strategy::OrderRouting::Fixed {
            return Err(OmsError::OrderRejected {
                reason: "managed EMS requires fixed routing".into(),
            });
        }
        let requested = intent.quantity;
        let reservation = self
            .validate_managed_spot_order(&mut intent)
            .await?
            .ok_or_else(|| OmsError::InvalidRow {
                reason: "missing parent reservation".into(),
            })?;
        if requested != intent.quantity {
            return Err(OmsError::OrderRejected {
                reason: "parent quantity must conform to current trading rules".into(),
            });
        }
        // A parent also locks its receipt asset, even against an existing
        // direct order spending it. Recheck every potentially affected asset.
        let assets = [
            Some(reservation.spend_asset.as_str()),
            Some(reservation.receipt_asset.as_str()),
            reservation
                .commission
                .as_ref()
                .and_then(|c| c.discount_asset.as_deref()),
        ];
        if self.spot_reservations.values().any(|r| {
            [
                Some(r.spend_asset.as_str()),
                Some(r.receipt_asset.as_str()),
                r.commission
                    .as_ref()
                    .and_then(|c| c.discount_asset.as_deref()),
            ]
            .into_iter()
            .flatten()
            .any(|a| assets.contains(&Some(a)))
        }) {
            return Err(OmsError::OrderRejected {
                reason: "working order conflicts with EMS parent assets".into(),
            });
        }
        self.store
            .check_parent_capital(None, &intent, &reservation)
            .await?;
        self.store
            .reserve_parent_capital(id, &intent, &reservation)
            .await
    }

    pub async fn verify_ems_parent(
        &mut self,
        id: Uuid,
        symbol: &str,
        side: Side,
        total: Decimal,
        submitted: Decimal,
    ) -> Result<(), OmsError> {
        if !self.store.inventory_bound() {
            return Ok(());
        }
        self.store.verify_inventory_lease().await?;
        self.store
            .verify_parent_capital(id, symbol, side, total, submitted)
            .await
    }

    /// A terminal scheduler may request release, but working child orders
    /// retain the lock; the fill-delivery gate protects final proceeds.
    pub async fn release_ems_parent(&mut self, id: Uuid) -> Result<(), OmsError> {
        if !self.store.inventory_bound() {
            return Ok(());
        }
        self.store.verify_inventory_lease().await?;
        self.store.release_parent_capital(id).await
    }

    pub async fn submit_allocated_child(
        &mut self,
        parent: Uuid,
        intent: OrderIntent,
    ) -> Result<OrderId, OmsError> {
        let parent = self.store.inventory_bound().then_some(parent);
        self.submit_order_inner(
            intent,
            &HashMap::new(),
            &HashMap::new(),
            true,
            parent,
            (None, None),
        )
        .await
    }

    /// Preserve dispatch ownership before the actual gateway call,
    /// even if managed capital allocation is disabled.
    pub async fn submit_dispatched_child(
        &mut self,
        link: crate::ChildLink,
        intent: OrderIntent,
    ) -> Result<OrderId, OmsError> {
        let link = link.validate()?;
        let allocation = self.store.inventory_bound().then_some(link.parent_id.0);
        self.submit_order_inner(
            intent,
            &HashMap::new(),
            &HashMap::new(),
            true,
            allocation,
            (None, Some(link)),
        )
        .await
    }

    async fn submit_order_inner(
        &mut self,
        mut intent: OrderIntent,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
        exact_quantity: bool,
        parent: Option<Uuid>,
        correlation: (Option<crate::IntentLink>, Option<crate::ChildLink>),
    ) -> Result<OrderId, OmsError> {
        let (link, child) = correlation;
        if let Some(kill_switch) = &self.kill_switch
            && kill_switch.blocks_submissions()
        {
            apexes_telemetry::metrics()
                .orders_submitted
                .with_label_values(&[apexes_telemetry::order_result::HALTED])
                .inc();
            return Err(OmsError::TradingHalted {
                reason: kill_switch
                    .reason()
                    .or_else(|| kill_switch.health_reason())
                    .unwrap_or_default(),
            });
        }
        if self
            .fill_delivery
            .as_ref()
            .is_some_and(|delivery| delivery.dirty || delivery.failed)
        {
            return Err(OmsError::TradingHalted {
                reason: "durable executions await ledger acknowledgement".into(),
            });
        }
        if self.reservation_breached {
            self.report_health_fault(
                "managed spot reservation exceeded; audited reconciliation required",
            );
            return Err(OmsError::TradingHalted {
                reason: "persisted reservation breach requires reconciliation".into(),
            });
        }
        let requested_quantity = intent.quantity;
        let reservation = match self.validate_managed_spot_order(&mut intent).await {
            Ok(reservation) => reservation,
            Err(error) => {
                if matches!(error, OmsError::QueryFailed { .. }) {
                    self.report_health_fault("managed spot inventory persistence unavailable");
                }
                return Err(error);
            }
        };
        if exact_quantity && requested_quantity != intent.quantity {
            return Err(OmsError::OrderRejected {
                reason: "EMS child quantity must already conform to current exchange rules".into(),
            });
        }
        if let Some(r) = &reservation {
            self.store.check_parent_capital(parent, &intent, r).await?;
        }
        let mut risk_intent = intent.clone();
        let mut risk_positions = positions.clone();
        if let Some(evidence) = &reservation {
            if risk_intent.price.is_none() {
                risk_intent.price = Some(evidence.reference_price);
            }
            // The position cap must not depend on a caller supplying a
            // snapshot (EMS children supply none). This quantity-only
            // risk input is never published as a recovered cost basis.
            if intent.side == Side::Buy {
                let position = risk_positions
                    .entry(intent.symbol.clone())
                    .or_insert(Position {
                        symbol: intent.symbol.clone(),
                        quantity: Decimal::ZERO,
                        average_entry_price: Decimal::ZERO,
                        leverage: None,
                        margin_mode: None,
                        allocated_margin: Decimal::ZERO,
                    });
                position.quantity = evidence.owned_quantity;
            }
        }
        if let Some(checker) = &self.risk_checker
            && let Err(violations) = checker.check(
                &risk_intent,
                &risk_positions,
                balances,
                &self.open_orders,
                Utc::now(),
            )
        {
            apexes_telemetry::metrics()
                .orders_submitted
                .with_label_values(&[apexes_telemetry::order_result::RISK_REJECTED])
                .inc();
            return Err(OmsError::RiskCheckFailed { violations });
        }

        let result = self
            .place_order(intent, false, reservation, parent, link, child)
            .await;
        if matches!(
            &result,
            Err(OmsError::QueryFailed { .. } | OmsError::SubmissionUnresolved { .. })
        ) {
            self.report_health_fault("OMS submission persistence or reconciliation failed");
        }
        result
    }

    pub fn report_health_fault(&self, reason: &'static str) {
        if let Some(ks) = &self.kill_switch {
            ks.fault(reason);
        }
    }

    /// Submits a position-closing order for the kill switch's flatten
    /// step. Deliberately bypasses the kill switch (it is engaged by
    /// definition) AND the pre-trade risk checks (they exist to stop risk
    /// being added; a closing order must not be blocked by, say, a position
    /// limit). Callers must only pass orders that reduce exposure.
    pub async fn submit_flatten_order(&mut self, intent: OrderIntent) -> Result<OrderId, OmsError> {
        tracing::warn!(symbol = %intent.symbol, side = ?intent.side, quantity = %intent.quantity, "submitting flatten order (bypasses halt and pre-trade risk)");
        self.place_order(intent, true, None, None, None, None).await
    }

    /// Validate fresh rule/availability evidence and reserve the spend
    /// asset exclusively while a working order could still consume it.
    /// Do not credit pending receipts, or infer safety from exchange
    /// free balance alone when an earlier request may still be in flight.
    async fn validate_managed_spot_order(
        &mut self,
        intent: &mut OrderIntent,
    ) -> Result<Option<SpotReservation>, OmsError> {
        if !self.store.inventory_bound() {
            return Ok(None);
        }
        let reject = |reason: &str| OmsError::OrderRejected {
            reason: reason.into(),
        };
        if intent.asset_class != AssetClass::Spot
            || intent.venue != apexes_core::Venue::Binance
            || intent.quantity <= Decimal::ZERO
            || intent.leverage.is_some()
            || intent.margin_mode.is_some()
        {
            return Err(reject("invalid managed spot order"));
        }
        if self
            .spot_scope
            .as_ref()
            .is_some_and(|scope| !scope.contains_key(&intent.symbol))
        {
            return Err(reject("symbol is outside managed inventory scope"));
        }
        self.spot_reservations
            .retain(|id, _| self.open_orders.contains_key(id));
        if self.open_orders.keys().any(|id| {
            self.spot_reservations
                .get(id)
                .is_none_or(|r| r.commission.is_none())
        }) {
            return Err(reject(
                "working order lacks a verified reservation; reconcile before trading",
            ));
        }
        if self
            .open_orders
            .values()
            .any(|o| o.exchange_order_id.is_none())
        {
            return Err(reject("unresolved submission prevents managed trading"));
        }
        let evidence = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.gateway.spot_preflight(intent),
        )
        .await
        .map_err(|_| reject("spot preflight timed out"))?
        .map_err(|reason| OmsError::OrderRejected { reason })?;
        if evidence.reference_price <= Decimal::ZERO
            || evidence.free_base < Decimal::ZERO
            || evidence.free_quote < Decimal::ZERO
            || evidence.base.is_empty()
            || evidence.quote.is_empty()
            || evidence.base == evidence.quote
            || self.spot_scope.as_ref().is_some_and(|scope| {
                scope.get(&intent.symbol) != Some(&(evidence.base.clone(), evidence.quote.clone()))
            })
        {
            return Err(reject(
                "invalid spot preflight evidence or instrument identity",
            ));
        }
        if evidence.quantity <= Decimal::ZERO
            || evidence.quantity > intent.quantity
            || !match (intent.price, evidence.price) {
                (None, None) => true,
                (Some(old), Some(new)) => {
                    new > Decimal::ZERO
                        && match intent.side {
                            Side::Buy => new <= old,
                            Side::Sell => new >= old,
                        }
                }
                _ => false,
            }
        {
            return Err(reject(
                "exchange preflight attempted non-passive conformance",
            ));
        }
        let requires_cap =
            intent.side == Side::Buy && intent.order_type == apexes_core::OrderType::Market;
        if requires_cap != evidence.market_buy_cap.is_some()
            || evidence
                .market_buy_cap
                .is_some_and(|cap| cap <= Decimal::ZERO || cap != evidence.reference_price)
        {
            return Err(reject(
                "missing or invalid exchange-enforceable market-buy cap",
            ));
        }
        let commission = evidence.commission;
        let age = Utc::now() - commission.observed_at;
        if !commission.valid()
            || age < chrono::TimeDelta::zero()
            || age > chrono::TimeDelta::seconds(5)
        {
            return Err(reject("missing, invalid or stale Spot commission evidence"));
        }
        // Fees paid in another managed base asset cannot be attributed
        // to this symbol's inventory. Require discount disabled there.
        if let Some(fee_asset) = &commission.discount_asset
            && self.spot_scope.as_ref().is_some_and(|scope| {
                scope
                    .iter()
                    .any(|(symbol, (base, _))| symbol != &intent.symbol && base == fee_asset)
            })
        {
            return Err(reject(
                "discount fee asset belongs to another managed inventory; disable discount or reconcile scope",
            ));
        }
        intent.quantity = evidence.quantity;
        intent.price = evidence.price;
        let owned = self
            .store
            .owned_spot_quantity(&intent.symbol, intent.venue, &evidence.base)
            .await?;
        if owned < Decimal::ZERO {
            self.report_health_fault("negative durable strategy spot inventory");
            return Err(reject(
                "negative durable spot inventory; reconcile before trading",
            ));
        }
        let (asset, receipt_asset, required, free) = match intent.side {
            Side::Sell => {
                if self.open_orders.values().any(|o| o.symbol == intent.symbol) {
                    return Err(reject("working orders prevent verified spot reduction"));
                }
                if owned > evidence.free_base {
                    self.report_health_fault(
                        "strategy spot inventory exceeds free exchange inventory",
                    );
                    return Err(reject(
                        "spot inventory discrepancy; reconcile before trading",
                    ));
                }
                // Normal Spot sale fees come from quote receipts, not
                // base principal. If the discount asset is the base,
                // reserve the undiscounted base-equivalent amount too.
                let required =
                    if commission.discount_asset.as_deref() == Some(evidence.base.as_str()) {
                        intent
                            .quantity
                            .checked_mul(Decimal::ONE + commission.rate)
                            .ok_or_else(|| reject("spot fee reservation overflow"))?
                    } else {
                        intent.quantity
                    };
                if required > owned {
                    return Err(reject(
                        "sale and possible base fee exceed strategy-owned inventory",
                    ));
                }
                (evidence.base, evidence.quote, required, evidence.free_base)
            }
            Side::Buy => {
                let cost = intent
                    .quantity
                    .checked_mul(evidence.reference_price)
                    .and_then(|v| v.checked_mul(Decimal::ONE + commission.rate))
                    .ok_or_else(|| reject("spot reservation amount overflow"))?;
                (evidence.quote, evidence.base, cost, evidence.free_quote)
            }
        };
        if self.spot_reservations.values().any(|reserved| {
            let existing_fee = reserved
                .commission
                .as_ref()
                .and_then(|c| c.discount_asset.as_deref());
            let new_fee = commission.discount_asset.as_deref();
            reserved.spend_asset == asset
                || reserved.receipt_asset == asset
                || existing_fee.is_some_and(|fee| fee == asset || fee == receipt_asset)
                || new_fee.is_some_and(|fee| {
                    fee == reserved.spend_asset
                        || fee == reserved.receipt_asset
                        || Some(fee) == existing_fee
                })
        }) {
            return Err(reject(
                "spend asset reserved or awaiting receipt from a working order",
            ));
        }
        if required > free {
            return Err(reject(
                "insufficient exchange free balance for spot reservation",
            ));
        }
        Ok(Some(SpotReservation {
            spend_asset: asset,
            receipt_asset,
            amount: required,
            owned_quantity: owned,
            reference_price: evidence.reference_price,
            market_buy_cap: evidence.market_buy_cap,
            commission: Some(commission),
        }))
    }

    /// Persists, then sends, one order. Shared tail of `submit_order` and
    /// `submit_flatten_order`; both paths enforce unresolved-submission safety.
    async fn place_order(
        &mut self,
        intent: OrderIntent,
        flatten: bool,
        reservation: Option<SpotReservation>,
        parent: Option<Uuid>,
        link: Option<crate::IntentLink>,
        child: Option<crate::ChildLink>,
    ) -> Result<OrderId, OmsError> {
        // Do not rely on strategies checking has_open_order: the OMS is
        // the final guard, including when a caller attempts a flatten.
        if let Some(pending) = self.open_orders.values().find(|o| {
            o.exchange_order_id.is_none()
                && o.symbol == intent.symbol
                && o.venue == intent.venue
                && o.asset_class == intent.asset_class
        }) {
            return Err(OmsError::SubmissionUnresolved {
                order_id: pending.id,
            });
        }
        if let Err(error) = self.store.verify_inventory_lease().await {
            self.report_health_fault("live inventory lease lost");
            return Err(error);
        }
        if flatten && self.store.inventory_bound() {
            return Err(OmsError::OrderRejected {
                reason: "managed Spot cannot bypass health through flatten".into(),
            });
        }
        let now = Utc::now();
        let mut order = Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: None,
            symbol: intent.symbol,
            venue: intent.venue,
            asset_class: intent.asset_class,
            side: intent.side,
            order_type: intent.order_type,
            quantity: intent.quantity,
            price: intent.price,
            stop_price: intent.stop_price,
            leverage: intent.leverage,
            margin_mode: intent.margin_mode,
            filled_quantity: Decimal::ZERO,
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
        };
        self.store
            .begin_submission(&order, reservation.as_ref(), parent, link, child)
            .await?;
        let order_id = order.id;
        // Retain the guard before awaiting network I/O. Cancellation or a
        // failed acknowledgement write must not erase an in-flight order.
        self.open_orders.insert(order_id, order.clone());
        let market_buy_cap = reservation.as_ref().and_then(|r| r.market_buy_cap);
        if let Some(reservation) = reservation {
            tracing::debug!(asset = %reservation.spend_asset, amount = %reservation.amount, "managed spot spend reserved");
            self.spot_reservations.insert(order_id, reservation);
        }
        if !flatten
            && let Some(ks) = &self.kill_switch
            && ks.blocks_submissions()
        {
            // No gateway call occurred: persist a definite local refusal.
            order.status = OrderStatus::Rejected;
            order.updated_at = Utc::now();
            self.store.upsert_order(&order).await?;
            self.open_orders.remove(&order_id);
            return Err(OmsError::TradingHalted {
                reason: ks
                    .reason()
                    .or_else(|| ks.health_reason())
                    .unwrap_or_default(),
            });
        }

        let submit_started = std::time::Instant::now();
        let outcome = match market_buy_cap {
            Some(cap) => self.gateway.submit_bounded_spot_buy(&order, cap).await,
            None => self.gateway.submit_order(&order).await,
        };
        let telemetry = apexes_telemetry::metrics();
        telemetry.observe_submit_latency(submit_started.elapsed());
        telemetry
            .orders_submitted
            .with_label_values(&[match &outcome {
                GatewayOutcome::Accepted { .. } => apexes_telemetry::order_result::ACCEPTED,
                GatewayOutcome::Rejected { .. } => apexes_telemetry::order_result::REJECTED,
                GatewayOutcome::Uncertain { .. } => apexes_telemetry::order_result::UNCERTAIN,
            }])
            .inc();
        let result = match outcome {
            GatewayOutcome::Accepted { exchange_order_id } => {
                order.exchange_order_id = Some(exchange_order_id);
                order.updated_at = Utc::now();
                self.store.upsert_order(&order).await?;
                let order_id = order.id;
                self.open_orders.insert(order_id, order);
                Ok(order_id)
            }
            GatewayOutcome::Rejected { reason } => {
                order.status = OrderStatus::Rejected;
                order.updated_at = Utc::now();
                self.store.upsert_order(&order).await?;
                self.open_orders.remove(&order_id);
                Err(OmsError::OrderRejected { reason })
            }
            GatewayOutcome::Uncertain { .. } => {
                self.report_health_fault("OMS order submission outcome uncertain");
                tracing::warn!(
                    order_id = ?order.id,
                    "order submission outcome uncertain; deferring to reconcile()"
                );
                order.updated_at = Utc::now();
                // If this write fails, the durable Submitting marker and
                // the in-memory unknown-ID guard remain in place.
                self.store.upsert_order(&order).await?;
                self.open_orders.insert(order_id, order);
                Ok(order_id)
            }
        };
        telemetry.open_orders.set(self.open_orders.len() as i64);
        result
    }

    /// Cancels an existing order:
    /// 1. Looks up `order_id` in `open_orders`; returns
    ///    `Err(OmsError::OrderNotFound)` if absent, or
    ///    `Err(OmsError::OrderNotCancellable { current_status })` if its
    ///    status isn't `New`/`PartiallyFilled`.
    /// 2. Checks `order.exchange_order_id`: if `None` (this order's
    ///    original `submit_order` call returned `Uncertain` and has not
    ///    yet been resolved by `reconcile()`), returns
    ///    `Err(OmsError::ExchangeOrderIdUnknown)` rather than calling
    ///    the gateway.
    /// 3. Otherwise calls `gateway.cancel_order`. Same three-way
    ///    `GatewayOutcome` handling as `submit_order`: `Accepted` ->
    ///    mark `Cancelled`, remove from `open_orders`; `Rejected` ->
    ///    return `Err` with the reason, order status unchanged;
    ///    `Uncertain` -> log and leave status unchanged, defer to
    ///    `reconcile()`.
    pub async fn cancel_order(&mut self, order_id: OrderId) -> Result<(), OmsError> {
        let order = self
            .open_orders
            .get(&order_id)
            .ok_or(OmsError::OrderNotFound { order_id })?;

        if !matches!(
            order.status,
            OrderStatus::New | OrderStatus::PartiallyFilled
        ) {
            return Err(OmsError::OrderNotCancellable {
                order_id,
                current_status: order.status,
            });
        }

        if order.exchange_order_id.is_none() {
            return Err(OmsError::ExchangeOrderIdUnknown { order_id });
        }
        let order_snapshot = order.clone();

        match self.gateway.cancel_order(&order_snapshot).await {
            GatewayOutcome::Accepted { .. } => {
                // Cancellation may race with executions; do not retire
                // the order before reconciling its trades.
                self.apply_exchange_update(
                    order_id,
                    order_snapshot.exchange_order_id,
                    OrderStatus::Cancelled,
                )
                .await?;
                Ok(())
            }
            GatewayOutcome::Rejected { reason } => Err(OmsError::OrderRejected { reason }),
            GatewayOutcome::Uncertain { reason } => {
                tracing::warn!(
                    order_id = ?order_id,
                    reason,
                    "order cancellation outcome uncertain; deferring to reconcile()"
                );
                Ok(())
            }
        }
    }

    /// Reconcile authoritative executions, not merely status transitions.
    /// Repeated PartiallyFilled updates can contain new executions.
    pub async fn apply_exchange_update(
        &mut self,
        client_order_id: OrderId,
        exchange_order_id: Option<String>,
        status: OrderStatus,
    ) -> Result<Vec<Fill>, OmsError> {
        let result = self
            .apply_exchange_update_inner(client_order_id, exchange_order_id, status)
            .await;
        match &result {
            Ok(fills) if !fills.is_empty() => {
                if let Some(delivery) = &mut self.fill_delivery {
                    delivery.dirty = true;
                }
            }
            Err(_) => self.report_health_fault("OMS execution reconciliation failed"),
            _ => {}
        }
        result
    }

    async fn apply_exchange_update_inner(
        &mut self,
        client_order_id: OrderId,
        exchange_order_id: Option<String>,
        status: OrderStatus,
    ) -> Result<Vec<Fill>, OmsError> {
        let managed = self.store.inventory_bound();
        let mut candidate = if let Some(order) = self.open_orders.get(&client_order_id) {
            order.clone()
        } else if let Some(order) = self.store.get_order(client_order_id).await? {
            if !managed || !status_carries_fills(status) {
                return Ok(Vec::new());
            }
            order
        } else {
            self.report_health_fault("OMS untracked exchange update");
            return Ok(Vec::new());
        };
        let previous_status = candidate.status;
        let was_terminal = matches!(
            previous_status,
            OrderStatus::Filled | OrderStatus::Cancelled | OrderStatus::Rejected
        );
        let id_changed =
            exchange_order_id.is_some() && candidate.exchange_order_id != exchange_order_id;
        if managed && id_changed && candidate.exchange_order_id.is_some() {
            return Err(OmsError::InvalidRow {
                reason: "exchange order identity changed".into(),
            });
        }
        if id_changed {
            candidate.exchange_order_id = exchange_order_id;
        }
        if previous_status == status && !id_changed && !status_carries_fills(status) {
            return Ok(Vec::new());
        }

        // A push is a wake-up, not proof of current cumulative execution.
        let expected = if managed {
            let snapshot = self
                .gateway
                .query_order_execution(&candidate)
                .await
                .map_err(|_| OmsError::QueryFailed {
                    reason: "authoritative execution snapshot unavailable".into(),
                })?;
            candidate.status = snapshot.status;
            if was_terminal && candidate.status != previous_status {
                return Err(OmsError::InvalidRow {
                    reason: "terminal order status contradicted by exchange".into(),
                });
            }
            Some(snapshot.executed_quantity)
        } else {
            if previous_status == OrderStatus::PartiallyFilled && status == OrderStatus::New {
                return Ok(Vec::new());
            }
            candidate.status = status;
            None
        };

        let executions = if status_carries_fills(candidate.status) {
            if candidate.exchange_order_id.is_none() {
                self.report_health_fault("OMS fill has no exchange order ID");
                return Ok(Vec::new());
            }
            match self.gateway.query_fills_for_order(&candidate).await {
                Ok(executions) => executions,
                Err(_) => {
                    self.report_health_fault("OMS fill query failed");
                    if id_changed {
                        candidate.status = previous_status;
                        self.store.upsert_order(&candidate).await?;
                        self.open_orders.insert(client_order_id, candidate);
                    }
                    return Ok(Vec::new());
                }
            }
        } else {
            Vec::new()
        };
        candidate.updated_at = Utc::now();
        let (fills, breached) = self
            .store
            .commit_exchange_update(&mut candidate, executions, expected)
            .await?;
        if managed && breached {
            self.reservation_breached = true;
            tracing::error!(order_id = %client_order_id.0, "actual execution spend exceeded durable reservation");
            self.report_health_fault(
                "managed spot reservation exceeded; audited reconciliation required",
            );
        }
        if matches!(
            candidate.status,
            OrderStatus::Filled | OrderStatus::Cancelled | OrderStatus::Rejected
        ) {
            self.open_orders.remove(&client_order_id);
        } else {
            self.open_orders.insert(client_order_id, candidate);
        }
        let telemetry = apexes_telemetry::metrics();
        telemetry.fills.inc_by(fills.len() as u64);
        telemetry.open_orders.set(self.open_orders.len() as i64);
        Ok(fills)
    }

    /// Reconnect reconciliation, in two passes, both delegating the
    /// actual per-order state application to `apply_exchange_update`
    /// (above):
    ///
    /// **Pass 1** -- for each distinct `asset_class` among locally-open
    /// orders, calls `gateway.query_all_open_orders(asset_class)` (real
    /// exchange APIs scope "list open orders" to one API family, e.g.
    /// Binance's spot vs. futures open-orders endpoints are entirely
    /// separate -- this is why the call happens once per asset class
    /// rather than once for the whole account). For every
    /// `(client_order_id, exchange_order_id, exchange_status)` triple
    /// returned, applies it via `apply_exchange_update`.
    ///
    /// **Pass 2** -- `query_all_open_orders` is documented to report
    /// ONLY non-terminal orders, so it can never surface a `Filled`/
    /// `Cancelled`/`Rejected` transition on its own. For every local
    /// order still `New`/`PartiallyFilled` that pass 1 did NOT report
    /// (and that has a known `exchange_order_id`), this pass calls
    /// `gateway.query_order_status` individually (which DOES report
    /// terminal statuses) and applies the result the same way. A local
    /// order with no `exchange_order_id` yet, not reported by pass 1
    /// either, cannot be resolved via the current `ExecutionGateway`
    /// interface -- logged via `tracing::warn!`, left unchanged.
    ///
    /// Returns one `(OrderId, Vec<Fill>)` entry for every order whose
    /// status actually changed OR that produced new fills, so a caller
    /// (e.g. `apexes-live-trading`'s event loop) can both log/alert on
    /// what changed and apply any new `Fill`s to its own portfolio
    /// tracking.
    pub async fn reconcile(&mut self) -> Result<Vec<(OrderId, Vec<Fill>)>, OmsError> {
        let telemetry = apexes_telemetry::metrics();
        if self.store.verify_inventory_lease().await.is_err() {
            // Still reconcile/cancel existing orders; losing the ownership
            // lease is not permission to leave them unattended.
            self.report_health_fault("live inventory lease lost");
        }
        let result = self.reconcile_inner().await;
        match &result {
            Ok(changes) => {
                telemetry.reconcile_runs.with_label_values(&["ok"]).inc();
                telemetry.reconcile_changes.inc_by(changes.len() as u64);
            }
            Err(_) => {
                telemetry.reconcile_runs.with_label_values(&["error"]).inc();
                self.report_health_fault("OMS reconciliation failed");
            }
        }
        if self
            .open_orders
            .values()
            .any(|o| o.exchange_order_id.is_none())
        {
            self.report_health_fault("OMS unresolved submission");
        }
        telemetry.open_orders.set(self.open_orders.len() as i64);
        result
    }

    async fn reconcile_inner(&mut self) -> Result<Vec<(OrderId, Vec<Fill>)>, OmsError> {
        let asset_classes: HashSet<AssetClass> = self
            .open_orders
            .values()
            .map(|order| order.asset_class)
            .collect();

        let mut all_exchange_orders: Vec<(OrderId, Option<String>, OrderStatus)> = Vec::new();
        for asset_class in &asset_classes {
            let exchange_orders = self
                .gateway
                .query_all_open_orders(*asset_class)
                .await
                .map_err(|reason| OmsError::QueryFailed { reason })?;
            all_exchange_orders.extend(exchange_orders);
        }

        let reported_client_ids: HashSet<OrderId> =
            all_exchange_orders.iter().map(|(id, _, _)| *id).collect();

        let mut results: Vec<(OrderId, Vec<Fill>)> = Vec::new();

        // Pass 1: orders the exchange still considers open (or has just
        // transitioned between non-terminal states).
        for (client_order_id, exchange_order_id, exchange_status) in all_exchange_orders {
            let Some(previous_status) = self
                .open_orders
                .get(&client_order_id)
                .map(|order| order.status)
            else {
                self.report_health_fault("OMS untracked exchange order");
                tracing::error!(
                    order_id = ?client_order_id,
                    "reconcile: exchange reports an open order we have no local record of"
                );
                continue;
            };

            let new_fills = self
                .apply_exchange_update(client_order_id, exchange_order_id, exchange_status)
                .await?;
            if previous_status != exchange_status || !new_fills.is_empty() {
                results.push((client_order_id, new_fills));
            }
        }

        // Pass 2: local orders still New/PartiallyFilled that the
        // exchange did NOT report as open in pass 1 -- the primary way
        // a Filled/Cancelled/Rejected transition is ever discovered.
        let still_locally_open: Vec<OrderId> = self
            .open_orders
            .iter()
            .filter(|(id, order)| {
                !reported_client_ids.contains(*id)
                    && matches!(
                        order.status,
                        OrderStatus::New | OrderStatus::PartiallyFilled
                    )
            })
            .map(|(id, _)| *id)
            .collect();

        for client_order_id in still_locally_open {
            let Some(order) = self.open_orders.get(&client_order_id) else {
                continue;
            };
            let previous_status = order.status;
            let Some(exchange_order_id) = order.exchange_order_id.clone() else {
                // No exchange id: submission outcome was Uncertain, or the
                // acknowledgement was lost in a crash. Resolve by OUR id.
                let order_snapshot = order.clone();
                match self.gateway.query_order_by_client_id(&order_snapshot).await {
                    Ok(Some((exchange_order_id, status))) => {
                        let new_fills = self
                            .apply_exchange_update(client_order_id, Some(exchange_order_id), status)
                            .await?;
                        results.push((client_order_id, new_fills));
                    }
                    Ok(None) => {
                        self.store.upsert_order(&order_snapshot).await?;
                        let age = Utc::now() - order_snapshot.created_at;
                        let grace = chrono::Duration::from_std(self.unsent_order_grace)
                            .unwrap_or(chrono::Duration::MAX);
                        if age >= grace {
                            tracing::warn!(
                                order_id = ?client_order_id,
                                ?age,
                                "reconcile: client ID not found; submission remains unresolved, manual review may be required"
                            );
                        }
                    }
                    Err(_) => {
                        tracing::warn!(
                            order_id = ?client_order_id,
                            "reconcile: cannot resolve order with no exchange_order_id (client-id lookup failed/unsupported); will retry"
                        );
                    }
                }
                continue;
            };
            let order_snapshot = order.clone();

            match self.gateway.query_order_status(&order_snapshot).await {
                Ok(Some(status)) => {
                    let new_fills = self
                        .apply_exchange_update(client_order_id, Some(exchange_order_id), status)
                        .await?;
                    if previous_status != status || !new_fills.is_empty() {
                        results.push((client_order_id, new_fills));
                    }
                }
                Ok(None) => {
                    self.report_health_fault("OMS open order missing at exchange");
                    tracing::error!(
                        order_id = ?client_order_id,
                        exchange_order_id,
                        "reconcile: exchange has no record at all of an order we believe is open"
                    );
                }
                Err(reason) => {
                    self.report_health_fault("OMS order status query failed");
                    tracing::warn!(
                        order_id = ?client_order_id,
                        reason,
                        "reconcile: query_order_status failed for this order; will retry on next reconcile() call"
                    );
                }
            }
        }

        Ok(results)
    }

    /// Open orders on the exchange (across `asset_classes`) that this system
    /// did not place and therefore cannot track. See
    /// `ExecutionGateway::count_foreign_open_orders`.
    pub async fn foreign_open_order_count(
        &self,
        asset_classes: &[AssetClass],
    ) -> Result<usize, OmsError> {
        let mut total = 0;
        for asset_class in asset_classes {
            total += self
                .gateway
                .count_foreign_open_orders(*asset_class)
                .await
                .map_err(|reason| OmsError::QueryFailed { reason })?;
        }
        Ok(total)
    }

    /// Best-effort cancel of every open order. Runs `reconcile()` first
    /// so orders whose submission outcome was `Uncertain` learn their
    /// exchange id (and orders that already filled/cancelled drop out).
    /// Never returns early on a per-order failure: every order is
    /// attempted and the report says what is left, so the caller (the
    /// kill-switch task) can retry until `is_complete()`.
    pub async fn cancel_all_open(&mut self) -> CancelAllReport {
        if let Err(error) = self.reconcile().await {
            tracing::warn!(
                ?error,
                "cancel_all_open: pre-cancel reconcile failed; continuing"
            );
        }
        let mut report = CancelAllReport::default();
        let ids: Vec<OrderId> = self.open_orders.keys().copied().collect();
        for id in ids {
            match self.cancel_order(id).await {
                Ok(()) => {
                    // `Uncertain` cancels return Ok but leave the order open.
                    if self.open_orders.contains_key(&id) {
                        report
                            .failed
                            .push((id, "cancel outcome uncertain".to_string()));
                    } else {
                        report.cancelled.push(id);
                    }
                }
                Err(OmsError::ExchangeOrderIdUnknown { .. }) => report.unresolved.push(id),
                Err(OmsError::OrderNotCancellable { .. }) => {}
                Err(error) => report.failed.push((id, error.to_string())),
            }
        }
        report
    }

    /// Inspect a persisted client ID using this manager's bound gateway.
    /// Unknown local IDs never cause a gateway call. No mutation, release
    /// of reservations, or retry authorization follows from this observation.
    pub async fn inspect_client_identity(
        &self,
        id: OrderId,
    ) -> Result<Option<crate::IdentityEvidence>, OmsError> {
        let Some((order, state, link, child)) = self.store.identity_snapshot(id).await? else {
            return Ok(None);
        };
        Ok(Some(
            crate::identity_evidence::observe(self.gateway.as_ref(), order, state, link, child)
                .await,
        ))
    }

    /// Find a persisted client ID by its scoped intent identity.
    /// This does not query the exchange or grant permission to retry.
    pub async fn client_order_for_intent(
        &self,
        link: crate::IntentLink,
    ) -> Result<Option<OrderId>, OmsError> {
        self.store.client_order_for_intent(link).await
    }

    pub async fn client_order_for_dispatch(
        &self,
        link: crate::ChildLink,
    ) -> Result<Option<OrderId>, OmsError> {
        self.store.client_order_for_dispatch(link).await
    }

    /// Durable lookup for background EMS reconciliation, including terminal children.
    pub async fn persisted_order(&self, id: OrderId) -> Result<Option<Order>, OmsError> {
        self.store.get_order(id).await
    }

    pub fn open_orders(&self) -> &HashMap<OrderId, Order> {
        &self.open_orders
    }

    /// Queries the exchange's open orders for each of `asset_classes`
    /// and returns the client order IDs we have NO local record of
    /// (orphans: e.g. placed manually or by another process).
    /// `reconcile()` only logs these and only looks at asset classes
    /// with local open orders, so startup recovery calls this
    /// explicitly to refuse to start on an account it cannot account for.
    pub async fn unknown_exchange_orders(
        &self,
        asset_classes: &[AssetClass],
    ) -> Result<Vec<OrderId>, OmsError> {
        let mut unknown = Vec::new();
        for asset_class in asset_classes {
            let exchange_orders = self
                .gateway
                .query_all_open_orders(*asset_class)
                .await
                .map_err(|reason| OmsError::QueryFailed { reason })?;
            unknown.extend(
                exchange_orders
                    .into_iter()
                    .map(|(id, _, _)| id)
                    .filter(|id| !self.open_orders.contains_key(id)),
            );
        }
        Ok(unknown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock_gateway::MockExecutionGateway;
    use apexes_core::{AssetClass, MarginMode, OrderType, Side, Venue};
    use apexes_risk::{PreTradeCheck, RiskViolation};
    use chrono::DateTime;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    const POSTGRES_URL: &str = "postgres://apexes:apexes@localhost:5432/apexes";

    /// Production code uses the process-wide global `Registry`
    /// (`apexes_core::registry::global()`); this crate's tests each
    /// construct their OWN isolated `Registry::new()` instead. This is
    /// required, not just a style preference: each test needs its own
    /// fresh `MockExecutionGateway` registered so it can inspect that
    /// specific instance's exchange state afterward -- a shared global
    /// registry would mean only the first test to register ever "wins"
    /// (later `register_or_skip` calls silently no-op, so later tests
    /// would observe the FIRST test's stale gateway instead of their
    /// own), breaking test isolation regardless of `--test-threads`.
    fn registry_with_gateway(gateway: Arc<dyn ExecutionGateway>) -> Registry {
        let registry = Registry::new();
        registry
            .register::<Arc<dyn ExecutionGateway>, ()>((), gateway)
            .unwrap();
        registry
    }

    /// Wraps a `PreTradeCheck` that always rejects, for testing
    /// `submit_order`'s risk-check-failure path without depending on
    /// `apexes-risk`'s concrete checkers.
    struct AlwaysRejectCheck;

    impl PreTradeCheck for AlwaysRejectCheck {
        fn check(
            &self,
            intent: &OrderIntent,
            _positions: &HashMap<String, Position>,
            _balances: &HashMap<String, Decimal>,
            _open_orders: &HashMap<OrderId, Order>,
            _now: DateTime<Utc>,
        ) -> Result<(), Vec<RiskViolation>> {
            Err(vec![RiskViolation::SymbolBlacklisted {
                symbol: intent.symbol.clone(),
            }])
        }
    }

    fn sample_intent() -> OrderIntent {
        OrderIntent {
            symbol: "BTCUSDT".to_string(),
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

    /// Deletes all rows from both tables before a test runs, so tests
    /// sharing one live PostgreSQL database don't see leftover state
    /// from earlier tests (e.g. `OrderManager::new`'s
    /// `get_open_orders()` recovery would otherwise pick up unrelated
    /// New-status orders from a previous test run).
    async fn clean_db(store: &OrderStore) {
        // OrderStore has no public "delete everything" method (by
        // design -- production code should never truncate order
        // history). Test-only cleanup goes directly through sqlx.
        // OrderStore does not expose its pool, so this helper
        // reconnects independently for the sole purpose of clearing
        // state; production code never does this.
        let pool = sqlx::PgPool::connect(POSTGRES_URL).await.unwrap();
        sqlx::query("TRUNCATE fills, orders")
            .execute(&pool)
            .await
            .unwrap();
        let _ = store; // parameter kept for call-site symmetry/clarity
    }

    async fn fresh_manager(gateway: Arc<dyn ExecutionGateway>) -> OrderManager {
        let store = OrderStore::new(POSTGRES_URL).await.unwrap();
        clean_db(&store).await;
        let registry = registry_with_gateway(gateway);
        OrderManager::new(store, &registry).await.unwrap()
    }

    #[tokio::test]
    #[ignore]
    async fn engaged_kill_switch_blocks_submit_but_not_after_release() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let kill_switch = Arc::new(KillSwitch::new());
        let mut manager = fresh_manager(gateway)
            .await
            .with_kill_switch(Arc::clone(&kill_switch));

        let halted_before = apexes_telemetry::metrics()
            .orders_submitted
            .with_label_values(&[apexes_telemetry::order_result::HALTED])
            .get();
        kill_switch.engage("test halt");
        let err = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap_err();
        assert!(matches!(err, OmsError::TradingHalted { ref reason } if reason == "test halt"));
        assert!(manager.open_orders().is_empty());
        assert!(
            apexes_telemetry::metrics()
                .orders_submitted
                .with_label_values(&[apexes_telemetry::order_result::HALTED])
                .get()
                > halted_before
        );

        kill_switch.release();
        manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn cancel_all_open_cancels_every_open_order() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = fresh_manager(gateway).await;
        for _ in 0..3 {
            manager
                .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
                .await
                .unwrap();
        }
        assert_eq!(manager.open_orders().len(), 3);

        let report = manager.cancel_all_open().await;
        assert_eq!(report.cancelled.len(), 3);
        assert!(report.is_complete());
        assert!(manager.open_orders().is_empty());
    }

    #[tokio::test]
    #[ignore]
    async fn submit_order_with_failing_risk_check_creates_no_order() {
        let store = OrderStore::new(POSTGRES_URL).await.unwrap();
        clean_db(&store).await;
        let gateway = Arc::new(MockExecutionGateway::new());
        let registry = registry_with_gateway(gateway);
        registry
            .register::<Arc<CompositeRiskChecker>, ()>(
                (),
                Arc::new(
                    CompositeRiskChecker::new().with_pre_trade_check(Box::new(AlwaysRejectCheck)),
                ),
            )
            .unwrap();
        let mut manager = OrderManager::new(store, &registry).await.unwrap();

        let result = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await;

        assert!(matches!(result, Err(OmsError::RiskCheckFailed { .. })));
        assert!(manager.open_orders().is_empty());
    }

    #[tokio::test]
    #[ignore]
    async fn submit_order_accepted_by_gateway_appears_in_open_orders_with_exchange_id() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = fresh_manager(gateway).await;

        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();

        let order = manager.open_orders().get(&order_id).unwrap();
        assert!(order.exchange_order_id.is_some());
        assert_eq!(order.status, OrderStatus::New);
    }

    #[tokio::test]
    #[ignore]
    async fn submit_order_threads_leverage_and_margin_mode_onto_constructed_order() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = fresh_manager(gateway).await;
        let mut intent = sample_intent();
        intent.leverage = Some(dec!(10));
        intent.margin_mode = Some(MarginMode::Isolated);

        let order_id = manager
            .submit_order(intent, &HashMap::new(), &HashMap::new())
            .await
            .unwrap();

        let order = manager.open_orders().get(&order_id).unwrap();
        assert_eq!(order.leverage, Some(dec!(10)));
        assert_eq!(order.margin_mode, Some(MarginMode::Isolated));
    }

    #[tokio::test]
    #[ignore]
    async fn submit_order_rejected_by_gateway_is_not_in_open_orders() {
        let gateway = Arc::new(MockExecutionGateway::new());
        gateway.program_next_submit_outcome(GatewayOutcome::Rejected {
            reason: "insufficient balance".to_string(),
        });
        let mut manager = fresh_manager(gateway).await;

        let result = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await;

        assert!(matches!(result, Err(OmsError::OrderRejected { .. })));
        assert!(manager.open_orders().is_empty());
    }

    #[tokio::test]
    #[ignore]
    async fn submit_order_uncertain_outcome_stays_open_with_no_exchange_id() {
        let gateway = Arc::new(MockExecutionGateway::new());
        gateway.program_next_submit_outcome(GatewayOutcome::Uncertain {
            reason: "connection reset".to_string(),
        });
        let mut manager = fresh_manager(gateway).await;

        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();

        let order = manager.open_orders().get(&order_id).unwrap();
        assert_eq!(order.exchange_order_id, None);
        assert_eq!(order.status, OrderStatus::New);
    }

    #[tokio::test]
    #[ignore]
    async fn cancel_order_not_found_returns_error() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = fresh_manager(gateway).await;

        let result = manager.cancel_order(OrderId(Uuid::new_v4())).await;
        assert!(matches!(result, Err(OmsError::OrderNotFound { .. })));
    }

    #[tokio::test]
    #[ignore]
    async fn cancel_order_with_unknown_exchange_id_is_rejected_without_calling_gateway() {
        let gateway = Arc::new(MockExecutionGateway::new());
        gateway.program_next_submit_outcome(GatewayOutcome::Uncertain {
            reason: "timeout".to_string(),
        });
        let mut manager = fresh_manager(gateway).await;
        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();

        let result = manager.cancel_order(order_id).await;
        assert!(matches!(
            result,
            Err(OmsError::ExchangeOrderIdUnknown { .. })
        ));
    }

    #[tokio::test]
    #[ignore]
    async fn cancel_order_accepted_removes_it_from_open_orders() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = fresh_manager(gateway).await;
        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();

        manager.cancel_order(order_id).await.unwrap();

        assert!(manager.open_orders().get(&order_id).is_none());
    }

    #[tokio::test]
    #[ignore]
    async fn reconcile_pass_one_overwrites_local_status_for_a_reported_open_order() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = fresh_manager(Arc::clone(&gateway) as Arc<dyn ExecutionGateway>).await;
        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();
        assert_eq!(
            manager.open_orders().get(&order_id).unwrap().status,
            OrderStatus::New
        );

        // PartiallyFilled is NON-terminal, so it still appears in
        // query_all_open_orders -- this exercises pass 1.
        gateway.set_exchange_state(order_id, OrderStatus::PartiallyFilled);

        let changed = manager.reconcile().await.unwrap();

        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].0, order_id);
        assert!(changed[0].1.is_empty());
        assert_eq!(
            manager.open_orders().get(&order_id).unwrap().status,
            OrderStatus::PartiallyFilled
        );
    }

    #[tokio::test]
    #[ignore]
    async fn reconcile_pass_two_discovers_a_terminal_status_via_query_order_status() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = fresh_manager(Arc::clone(&gateway) as Arc<dyn ExecutionGateway>).await;
        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();

        // Filled is TERMINAL -- query_all_open_orders will no longer
        // report this order (by MockExecutionGateway's own filtering,
        // matching the real ExecutionGateway contract), so pass 1 alone
        // cannot see this. Only pass 2's query_order_status call can.
        gateway.set_exchange_state(order_id, OrderStatus::Filled);

        let changed = manager.reconcile().await.unwrap();

        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].0, order_id);
        assert!(manager.open_orders().get(&order_id).is_none());

        let persisted = manager.store.get_order(order_id).await.unwrap().unwrap();
        assert_eq!(persisted.status, OrderStatus::Filled);
    }

    #[tokio::test]
    #[ignore]
    async fn reconcile_resolves_a_previously_uncertain_order_via_pass_one() {
        let gateway = Arc::new(MockExecutionGateway::new());
        gateway.program_next_submit_outcome(GatewayOutcome::Uncertain {
            reason: "timeout".to_string(),
        });
        let mut manager = fresh_manager(Arc::clone(&gateway) as Arc<dyn ExecutionGateway>).await;
        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();
        assert_eq!(
            manager
                .open_orders()
                .get(&order_id)
                .unwrap()
                .exchange_order_id,
            None
        );

        // The exchange actually accepted the order despite our local
        // Uncertain result, and it is still open (non-terminal) --
        // this is reported via query_all_open_orders (pass 1), which
        // is why force_register_exchange_order (not set_exchange_state,
        // which requires a prior submit_order through this mock) is
        // used here.
        gateway.force_register_exchange_order(
            order_id,
            "EX-999",
            OrderStatus::PartiallyFilled,
            AssetClass::Spot,
        );

        let changed = manager.reconcile().await.unwrap();

        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].0, order_id);
        let order = manager.open_orders().get(&order_id).unwrap();
        assert_eq!(order.status, OrderStatus::PartiallyFilled);
        assert_eq!(order.exchange_order_id, Some("EX-999".to_string()));
    }

    #[tokio::test]
    #[ignore]
    async fn reconcile_leaves_terminal_orders_unaffected_when_not_reported_by_exchange() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = fresh_manager(Arc::clone(&gateway) as Arc<dyn ExecutionGateway>).await;
        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();
        manager.cancel_order(order_id).await.unwrap();

        // Cancelled order is terminal and no longer in open_orders;
        // reconcile should not error or resurrect it.
        let changed = manager.reconcile().await.unwrap();
        assert!(!changed.iter().any(|(id, _)| *id == order_id));
    }

    #[tokio::test]
    #[ignore]
    async fn reconcile_pass_two_fetches_and_persists_fills_on_fill_transition() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = fresh_manager(Arc::clone(&gateway) as Arc<dyn ExecutionGateway>).await;
        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();

        gateway.set_exchange_state(order_id, OrderStatus::Filled);
        gateway.set_fills_for_order(
            order_id,
            vec![crate::gateway::ExchangeFill {
                exchange_trade_id: "T1".to_string(),
                price: dec!(65000),
                quantity: dec!(1),
                fee: dec!(1.5),
                fee_currency: Some("USDT".to_string()),
                filled_at: Utc::now(),
            }],
        );

        let changed = manager.reconcile().await.unwrap();

        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].0, order_id);
        assert_eq!(changed[0].1.len(), 1);
        assert_eq!(changed[0].1[0].quantity, dec!(1));

        let persisted_fills = manager.store.get_fills_for_order(order_id).await.unwrap();
        assert_eq!(persisted_fills.len(), 1);

        // Regression check for the final whole-branch review's Important
        // finding: filled_quantity must reflect the fill that just
        // transitioned this order to Filled, both in the persisted row
        // and in the in-memory snapshot (the order is removed from
        // open_orders once Filled, so check the persisted row).
        let persisted_order = manager.store.get_order(order_id).await.unwrap().unwrap();
        assert_eq!(persisted_order.filled_quantity, dec!(1));

        gateway.set_exchange_state(order_id, OrderStatus::Filled);
        let changed_again = manager.reconcile().await.unwrap();
        assert!(changed_again.is_empty());
        let persisted_fills_again = manager.store.get_fills_for_order(order_id).await.unwrap();
        assert_eq!(persisted_fills_again.len(), 1);
    }

    #[tokio::test]
    #[ignore]
    async fn reconcile_threads_leverage_and_margin_mode_from_order_onto_constructed_fills() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = fresh_manager(Arc::clone(&gateway) as Arc<dyn ExecutionGateway>).await;
        let mut intent = sample_intent();
        intent.leverage = Some(dec!(10));
        intent.margin_mode = Some(MarginMode::Isolated);
        let order_id = manager
            .submit_order(intent, &HashMap::new(), &HashMap::new())
            .await
            .unwrap();

        gateway.set_exchange_state(order_id, OrderStatus::Filled);
        gateway.set_fills_for_order(
            order_id,
            vec![crate::gateway::ExchangeFill {
                exchange_trade_id: "T1".to_string(),
                price: dec!(65000),
                quantity: dec!(1),
                fee: dec!(1.5),
                fee_currency: Some("USDT".to_string()),
                filled_at: Utc::now(),
            }],
        );

        let changed = manager.reconcile().await.unwrap();

        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].1.len(), 1);
        assert_eq!(changed[0].1[0].leverage, Some(dec!(10)));
        assert_eq!(changed[0].1[0].margin_mode, Some(MarginMode::Isolated));
    }

    /// Simulates acceptance at the exchange followed by a lost acknowledgement
    /// and a cancelled caller future. No new exchange request is needed to recover.
    struct InterruptedGateway {
        exchange: MockExecutionGateway,
        dispatched: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl ExecutionGateway for InterruptedGateway {
        async fn submit_order(&self, order: &Order) -> GatewayOutcome {
            self.exchange.force_register_exchange_order(
                order.id,
                "EX-INTERRUPTED",
                OrderStatus::New,
                order.asset_class,
            );
            self.dispatched.notify_one();
            std::future::pending().await
        }

        async fn cancel_order(&self, order: &Order) -> GatewayOutcome {
            self.exchange.cancel_order(order).await
        }

        async fn query_order_status(&self, order: &Order) -> Result<Option<OrderStatus>, String> {
            self.exchange.query_order_status(order).await
        }

        async fn query_order_by_client_id(
            &self,
            order: &Order,
        ) -> Result<Option<(String, OrderStatus)>, String> {
            self.exchange.query_order_by_client_id(order).await
        }

        async fn query_all_open_orders(
            &self,
            asset_class: AssetClass,
        ) -> Result<Vec<(OrderId, Option<String>, OrderStatus)>, String> {
            self.exchange.query_all_open_orders(asset_class).await
        }

        async fn query_fills_for_order(
            &self,
            order: &Order,
        ) -> Result<Vec<crate::ExchangeFill>, String> {
            self.exchange.query_fills_for_order(order).await
        }
    }

    #[tokio::test]
    #[ignore]
    async fn interrupted_submission_retains_durable_client_id_and_in_memory_guard() {
        use crate::SubmissionState;
        let gateway = Arc::new(InterruptedGateway {
            exchange: MockExecutionGateway::new(),
            dispatched: tokio::sync::Notify::new(),
        });
        let url = std::env::var("APEXES_INTENT_TEST_PG_URL").ok();
        let mut manager = if let Some(url) = &url {
            let registry = registry_with_gateway(gateway.clone());
            OrderManager::new(OrderStore::new(url).await.unwrap(), &registry)
                .await
                .unwrap()
        } else {
            fresh_manager(gateway.clone()).await
        };
        let url = url.as_deref().unwrap_or(POSTGRES_URL);
        let positions = HashMap::new();
        let balances = HashMap::new();
        let link = crate::IntentLink {
            run_scope: Uuid::new_v4(),
            intent_id: apexes_strategy::IntentId(1),
        };
        {
            let submit =
                manager.submit_correlated_order(sample_intent(), &positions, &balances, link);
            tokio::pin!(submit);
            tokio::select! {
                _ = gateway.dispatched.notified() => {}
                result = &mut submit => panic!("unexpected completion: {result:?}"),
            }
        }
        let order_id = *manager.open_orders().keys().next().unwrap();
        let store = OrderStore::new(url).await.unwrap();
        assert_eq!(
            store.client_order_for_intent(link).await.unwrap(),
            Some(order_id)
        );
        assert_eq!(
            store.get_submission_state(order_id).await.unwrap(),
            Some(SubmissionState::Submitting)
        );
        assert!(matches!(
            manager.submit_order(sample_intent(), &positions, &balances).await,
            Err(OmsError::SubmissionUnresolved { order_id: blocked }) if blocked == order_id
        ));
        drop(manager);
        let registry = registry_with_gateway(gateway);
        let mut recovered = OrderManager::new(OrderStore::new(url).await.unwrap(), &registry)
            .await
            .unwrap();
        let evidence = recovered
            .inspect_client_identity(order_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(evidence.local.id, order_id);
        assert_eq!(evidence.intent_link, Some(link));
        assert_eq!(
            recovered.client_order_for_intent(link).await.unwrap(),
            Some(order_id)
        );
        assert_eq!(evidence.submission_state, SubmissionState::Submitting);
        assert_eq!(evidence.local.exchange_order_id, None);
        assert_eq!(
            evidence.remote,
            crate::RemoteIdentity::Found {
                exchange_order_id: "EX-INTERRUPTED".into(),
                status: OrderStatus::New
            }
        );
        assert_eq!(
            store.get_submission_state(order_id).await.unwrap(),
            Some(SubmissionState::Submitting)
        );
        assert!(
            recovered
                .inspect_client_identity(OrderId(Uuid::new_v4()))
                .await
                .unwrap()
                .is_none()
        );
        recovered.reconcile().await.unwrap();
        assert_eq!(
            store.get_submission_state(order_id).await.unwrap(),
            Some(SubmissionState::Accepted)
        );
        assert_eq!(
            recovered.open_orders()[&order_id]
                .exchange_order_id
                .as_deref(),
            Some("EX-INTERRUPTED")
        );
    }
    #[tokio::test]
    #[ignore]
    async fn correlated_submission_preserves_link_and_rejects_duplicate_across_restart() {
        let url = std::env::var("APEXES_INTENT_TEST_PG_URL").ok();
        let gateway = Arc::new(MockExecutionGateway::new());
        let registry = registry_with_gateway(gateway.clone());
        // Explicit URL is a dedicated disposable database. Default CI
        // shares its test database and must clear preceding test orders.
        let mut manager = if let Some(url) = &url {
            OrderManager::new(OrderStore::new(url).await.unwrap(), &registry)
                .await
                .unwrap()
        } else {
            fresh_manager(gateway.clone()).await
        };
        let url = url.as_deref().unwrap_or(POSTGRES_URL);
        let link = crate::IntentLink {
            run_scope: Uuid::new_v4(),
            intent_id: apexes_strategy::IntentId(u64::MAX),
        };
        let positions = HashMap::new();
        let balances = HashMap::new();
        let id = manager
            .submit_correlated_order(sample_intent(), &positions, &balances, link)
            .await
            .unwrap();
        let evidence = manager.inspect_client_identity(id).await.unwrap().unwrap();
        assert_eq!(evidence.intent_link, Some(link));
        assert_eq!(evidence.submission_state, crate::SubmissionState::Accepted);
        assert_eq!(
            manager.client_order_for_intent(link).await.unwrap(),
            Some(id)
        );
        let other = crate::IntentLink {
            run_scope: Uuid::new_v4(),
            ..link
        };
        let other_id = manager
            .submit_correlated_order(sample_intent(), &positions, &balances, other)
            .await
            .unwrap();
        assert_ne!(id, other_id);
        let legacy = manager
            .submit_order(sample_intent(), &positions, &balances)
            .await
            .unwrap();
        assert_eq!(
            manager
                .inspect_client_identity(legacy)
                .await
                .unwrap()
                .unwrap()
                .intent_link,
            None
        );
        let count = gateway
            .query_all_open_orders(AssetClass::Spot)
            .await
            .unwrap()
            .len();
        drop(manager);
        let mut recovered = OrderManager::new(OrderStore::new(url).await.unwrap(), &registry)
            .await
            .unwrap();
        assert_eq!(
            recovered.client_order_for_intent(link).await.unwrap(),
            Some(id)
        );
        assert!(
            recovered
                .submit_correlated_order(sample_intent(), &positions, &balances, link)
                .await
                .is_err()
        );
        assert_eq!(
            gateway
                .query_all_open_orders(AssetClass::Spot)
                .await
                .unwrap()
                .len(),
            count
        );
        assert_eq!(
            recovered.client_order_for_intent(link).await.unwrap(),
            Some(id)
        );
        assert_eq!(
            recovered
                .inspect_client_identity(id)
                .await
                .unwrap()
                .unwrap()
                .intent_link,
            Some(link)
        );
    }

    #[tokio::test]
    #[ignore]
    async fn managed_oms_blocks_startup_and_latches_uncertain_submission() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let ks = Arc::new(KillSwitch::managed());
        let mut manager = fresh_manager(gateway.clone())
            .await
            .with_kill_switch(ks.clone());
        let positions = HashMap::new();
        let balances = HashMap::new();
        assert!(matches!(
            manager
                .submit_order(sample_intent(), &positions, &balances)
                .await,
            Err(OmsError::TradingHalted { .. })
        ));
        assert!(manager.open_orders().is_empty());
        ks.begin_reconciliation();
        ks.startup_ready();
        gateway.program_next_submit_outcome(GatewayOutcome::Uncertain {
            reason: "lost ack".into(),
        });
        let id = manager
            .submit_order(sample_intent(), &positions, &balances)
            .await
            .unwrap();
        assert!(manager.open_orders().contains_key(&id));
        assert_eq!(ks.phase(), Some(crate::SessionPhase::Degraded));
        assert!(ks.try_release().is_err());
        let mut other = sample_intent();
        other.symbol = "ETHUSDT".into();
        assert!(matches!(
            manager.submit_order(other, &positions, &balances).await,
            Err(OmsError::TradingHalted { .. })
        ));
    }
}
