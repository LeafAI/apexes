//! [`ExecutionGateway`]: the abstraction `OrderManager` submits/cancels/
//! queries orders through, decoupling order-lifecycle management from
//! any specific exchange's API. Sub-project #8 (first live gateway)
//! implements it against a real exchange; this crate ships only
//! [`crate::mock_gateway::MockExecutionGateway`] for testing.

use apexes_core::{AssetClass, Order, OrderId, OrderStatus};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

/// The outcome of a `submit_order`/`cancel_order` call, distinguishing
/// three cases `OrderManager` must handle differently:
/// - `Accepted`: the exchange confirmed the action.
/// - `Rejected`: the exchange explicitly refused (e.g. insufficient
///   balance, invalid parameters) -- authoritative, no ambiguity.
/// - `Uncertain`: the call failed in a way that does NOT confirm
///   whether the exchange actually processed it (e.g. network timeout,
///   connection dropped mid-request) -- the order may have gone through
///   despite the local failure. `OrderManager` must NOT assume failure
///   in this case; it leaves the order's status unchanged and defers to
///   a later `reconcile()` call to discover the true state.
#[derive(Debug, Clone, PartialEq)]
pub enum GatewayOutcome {
    Accepted { exchange_order_id: String },
    Rejected { reason: String },
    Uncertain { reason: String },
}

/// A single trade execution as reported by the exchange, keyed by the
/// exchange's own per-trade identifier (`exchange_trade_id`) so
/// `OrderManager` can deduplicate against fills it has already recorded.
/// Real exchanges' "my trades"-style endpoints return the FULL trade
/// history for an order every call, not just new trades since the last
/// call -- naively inserting every returned entry as a fresh `Fill`
/// would create duplicates across repeated calls. Not
/// `apexes_core::Fill` directly: the gateway has no knowledge of our
/// locally-assigned `FillId`, which `OrderManager` assigns only for
/// genuinely-new trades (see `OrderStore::insert_fill`'s dedup logic,
/// Task 10).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ExchangeFill {
    pub exchange_trade_id: String,
    pub price: Decimal,
    pub quantity: Decimal,
    pub fee: Decimal,
    pub fee_currency: Option<String>,
    pub filled_at: DateTime<Utc>,
}

/// Authoritative order state and cumulative executed base quantity from
/// one exchange response (not inferred from the returned trade page).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OrderExecutionSnapshot {
    pub status: OrderStatus,
    pub executed_quantity: Decimal,
}

/// Undiscounted commission evidence for the requested side. The rate
/// covers standard, tax and special fees at the more expensive liquidity
/// role. A discount asset is a possible fee debit, not guaranteed savings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpotCommission {
    pub rate: Decimal,
    pub observed_at: DateTime<Utc>,
    pub discount_asset: Option<String>,
}

impl SpotCommission {
    pub(crate) fn valid(&self) -> bool {
        self.rate >= Decimal::ZERO
            && self.rate < Decimal::ONE
            && self.discount_asset.as_ref().is_none_or(|asset| {
                !asset.is_empty()
                    && asset.len() <= 24
                    && asset
                        .bytes()
                        .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
            })
    }
}

/// Fresh exchange evidence for an exact managed Spot intent. All prices
/// are in the instrument quote asset; free balances exclude exchange locks.
#[derive(Debug, Clone)]
pub struct SpotPreflight {
    pub base: String,
    pub quote: String,
    pub free_base: Decimal,
    pub free_quote: Decimal,
    pub reference_price: Decimal,
    /// Exchange-enforceable limit price for a managed market-buy intent.
    pub market_buy_cap: Option<Decimal>,
    pub commission: SpotCommission,
    pub quantity: Decimal,
    pub price: Option<Decimal>,
}

#[async_trait]
pub trait ExecutionGateway: Send + Sync {
    /// Submits `order` (already persisted locally with status `New` by
    /// the caller before this is invoked -- see
    /// `OrderManager::submit_order`) to the exchange. Implementations
    /// MUST pass `order.id` to the exchange as a client order ID
    /// (alongside whatever exchange-native order ID the exchange
    /// generates), so `query_all_open_orders` can later report it back
    /// -- this is what lets `reconcile()` match exchange reports to
    /// local orders even when `exchange_order_id` was never learned
    /// (an `Uncertain` outcome from this call).
    async fn submit_order(&self, order: &Order) -> GatewayOutcome;

    /// Submit a market-buy intent with an exchange-side price and quantity
    /// bound. An unsupported gateway must never fall back to MARKET.
    async fn submit_bounded_spot_buy(&self, _order: &Order, _cap: Decimal) -> GatewayOutcome {
        GatewayOutcome::Rejected {
            reason: "bounded Spot buy unsupported".into(),
        }
    }

    /// Cancels an already-submitted order (the caller -- `OrderManager`
    /// -- guarantees `order.exchange_order_id` is `Some` before
    /// calling). Takes the full `Order` (not just its
    /// `exchange_order_id`) because real exchanges' cancel-by-ID
    /// endpoints commonly also require the order's `symbol` (Binance:
    /// order IDs are unique per-symbol, not globally) -- discovered
    /// during `apexes-gateway-binance`'s implementation (sub-project
    /// #8) and folded back into this trait rather than worked around.
    async fn cancel_order(&self, order: &Order) -> GatewayOutcome;

    /// Queries the exchange's current authoritative status for a
    /// single order. Takes the full `Order` for the same
    /// symbol-required-for-lookup reason as `cancel_order`.
    /// `order.asset_class` tells implementations which API family to
    /// query (e.g. `BinanceExecutionGateway` routes `Spot` to
    /// `api.binance.com` and `Perpetual` to `fapi.binance.com` -- see
    /// `apexes-gateway-binance`'s design). Returns `Ok(None)` if the
    /// exchange has no record of this order ID at all (distinct from
    /// "query failed" -- that's `Err`).
    async fn query_order_status(&self, order: &Order) -> Result<Option<OrderStatus>, String>;

    /// Authoritative base identity and spendable Spot inventory. The
    /// managed OMS uses this only for sell safety; unsupported gateways
    /// must refuse rather than guess availability.
    async fn spot_sell_inventory(&self, _symbol: &str) -> Result<(String, Decimal), String> {
        Err("spot sell inventory is not supported".into())
    }

    /// Validate current exchange rules for the exact intent and retrieve
    /// spendable inventory. Unsupported gateways must refuse managed orders.
    async fn spot_preflight(
        &self,
        _intent: &apexes_strategy::OrderIntent,
    ) -> Result<SpotPreflight, String> {
        Err("managed spot preflight unsupported".into())
    }

    /// Required for managed fill reconciliation. Legacy integrations
    /// may keep the status-only interface.
    async fn query_order_execution(
        &self,
        _order: &Order,
    ) -> Result<OrderExecutionSnapshot, String> {
        Err("authoritative executed quantity is not supported".into())
    }

    /// Looks an order up by OUR client order id (`order.id`) -- the only
    /// handle available for an order whose submission outcome was
    /// `Uncertain` or whose acknowledgement was lost in a crash, so its
    /// exchange id was never recorded. Returns `(exchange_order_id,
    /// status)` if the exchange knows the order, `Ok(None)` ONLY if the
    /// exchange reports no such order, `Err` if that cannot be
    /// determined. A negative lookup is not proof that an ambiguous
    /// submission never executed; the OMS must retain its guard.
    /// The default is `Err` for gateways without this lookup.
    async fn query_order_by_client_id(
        &self,
        _order: &Order,
    ) -> Result<Option<(String, OrderStatus)>, String> {
        Err("query_order_by_client_id is not supported by this gateway".to_string())
    }

    /// Queries the exchange for every currently open (not Filled/
    /// Cancelled/Rejected) order within ONE asset class -- used by
    /// `OrderManager::reconcile` after a reconnect. Callers needing
    /// full-account reconciliation across multiple asset classes call
    /// this once per asset class they have local open orders in (see
    /// `OrderManager::reconcile`, Task 10) -- this method itself never
    /// mixes asset classes in one call, since real exchanges' "list
    /// open orders" endpoints are themselves scoped to one API family
    /// (e.g. Binance's spot open-orders endpoint cannot return futures
    /// orders and vice versa).
    ///
    /// Returns `(client_order_id, exchange_order_id, status)` triples.
    /// `client_order_id` is OUR locally-assigned `OrderId` (`Order.id`),
    /// not the exchange's own identifier -- see `submit_order`'s docs
    /// for why implementations must echo it back here.
    async fn query_all_open_orders(
        &self,
        asset_class: AssetClass,
    ) -> Result<Vec<(OrderId, Option<String>, OrderStatus)>, String>;

    /// Number of open orders in `asset_class` that were NOT placed through
    /// this system (e.g. entered by hand in the exchange UI), which
    /// `query_all_open_orders` cannot report because it has no `OrderId` for
    /// them. Startup recovery refuses to run beside such orders. The default
    /// `Ok(0)` is for gateways with no way to tell.
    async fn count_foreign_open_orders(&self, _asset_class: AssetClass) -> Result<usize, String> {
        Ok(0)
    }

    /// Returns every trade execution the exchange has recorded for
    /// `order` (identified by `order.exchange_order_id`, which the
    /// caller guarantees is `Some`, and `order.asset_class`, used the
    /// same way as `query_order_status`/`query_all_open_orders` to pick
    /// an API family). Returns the FULL trade history for this order
    /// every call (matching real exchanges' typical "my trades"
    /// endpoint shape) -- deduplication against previously recorded
    /// fills is `OrderManager`'s responsibility (via
    /// `exchange_trade_id`), not this method's.
    async fn query_fills_for_order(&self, order: &Order) -> Result<Vec<ExchangeFill>, String>;
}
