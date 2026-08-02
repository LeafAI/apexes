use crate::margin_tier::PositionMarginStatus;
use apexes_core::{AssetClass, MarginMode, Order, OrderId, OrderType, Side, Venue};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

/// The shared strategy interface implemented by both the backtest engine
/// (`apexes-backtest`) and all future live-trading sub-projects (#6 Paper
/// Trading, #7 OMS, #8 live crypto gateway), per the project's
/// non-negotiable backtest/live logic-parity requirement. A `Strategy`
/// implementation never needs to know whether it is running in backtest
/// or live mode.
pub trait Strategy: Send {
    /// Called once per incoming `MarketEvent`, in timestamp order.
    fn on_market_event(
        &mut self,
        event: &apexes_core::MarketEvent,
        ctx: &StrategyContext,
    ) -> Vec<OrderIntent>;
}

/// Read-only view of current backtest/live state, passed to
/// `Strategy::on_market_event` so it can make informed order decisions.
pub struct StrategyContext<'a> {
    /// Current net positions, keyed by symbol.
    pub positions: &'a HashMap<String, Position>,
    /// Current balances, keyed by asset symbol (e.g. "USDT", "BTC").
    pub balances: &'a HashMap<String, Decimal>,
    /// Currently-open (unfilled or partially-filled) orders, keyed by
    /// `OrderId`.
    ///
    /// Triggered Stop orders: a Stop order that triggers converts
    /// internally to a Market order, and its fills are reported under the
    /// ORIGINAL `OrderId` this map already holds -- the runner's
    /// `open_orders.get_mut(&order_id)` lookup finds the same entry, so
    /// the strategy observes the fill on the order it placed.
    ///
    /// Known limitation (StopLimit only): a triggered StopLimit converts
    /// to a NEW resting Limit order tracked under a NEW `OrderId`; the
    /// original OrderId's entry in this map will not reflect fills of
    /// the converted order. See
    /// docs/superpowers/plans/2026-07-28-apexes-backtest.md Task 14 for
    /// full rationale.
    pub open_orders: &'a HashMap<OrderId, Order>,
    /// Aggregate status of every currently-tracked EMS parent order
    /// (algorithmically-scheduled order), keyed by `ParentOrderId` --
    /// a snapshot from the caller's most recent
    /// `Ems::parent_order_status` batch query. A strategy that never
    /// sets `OrderIntent.execution_algo` will simply see this map stay
    /// empty. See `docs/superpowers/specs/2026-08-01-apexes-ems-design.md`
    /// §五's integration-points section.
    pub open_parent_orders: &'a HashMap<ParentOrderId, ParentOrderStatus>,
    /// Real-time margin state of every currently-open leveraged
    /// position (spot positions never appear here), computed
    /// identically across backtest/paper/live via
    /// `PortfolioLedger::margin_status` -- a strategy can use this to
    /// proactively de-risk BEFORE an actual liquidation is triggered
    /// (backtest/paper auto-liquidate at `margin_ratio <= 1.0`; live
    /// trading never auto-liquidates but blocks new orders for the
    /// affected symbol via `apexes-risk::MarginMonitor`).
    pub margin_status: &'a HashMap<String, PositionMarginStatus>,
    /// Current simulated (backtest) or real (live) time.
    pub now: DateTime<Utc>,
}

/// How an `OrderIntent` should be routed across venues of the same
/// underlying symbol (e.g. Spot vs. Perpetual on the same exchange).
/// See `docs/superpowers/specs/2026-08-01-apexes-ems-design.md` §五.
#[derive(Debug, Clone, PartialEq)]
pub enum OrderRouting {
    /// Existing behavior: submit strictly to `OrderIntent.asset_class`,
    /// no routing decision made.
    Fixed,
    /// Smart order routing: a `VenueScorer` picks the single
    /// highest-scoring candidate venue for the WHOLE order quantity.
    RouteToBest,
    /// Cross-market splitting: a `VenueScorer`'s normalized scores
    /// determine what fraction of the order quantity goes to each
    /// listed candidate venue. Weights are never caller-specified --
    /// they always come from the scorer.
    Split(Vec<AssetClass>),
}

/// How an EMS-managed parent order should be paced. `None` on
/// `OrderIntent.execution_algo` means immediate submission (today's
/// behavior, unchanged); `Some(spec)` means the EMS slices/paces
/// submission per `spec`. See design spec §五.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionAlgoSpec {
    /// Splits `quantity` into `slices` equal child orders, one
    /// submitted every `duration / slices`.
    Twap { duration: Duration, slices: u32 },
    /// Paces child-order submission against a historical intraday
    /// volume curve (`apexes_storage::intraday_volume_curve`,
    /// `lookback_days` calendar days). Fails the whole parent order
    /// (`ParentOrderStatus::Failed`) if fewer than `lookback_days` of
    /// history exist -- never silently degrades to a uniform-time
    /// assumption.
    Vwap {
        duration: Duration,
        lookback_days: u32,
    },
    /// Keeps at most `visible_qty` resting on the book at any time,
    /// refilling as each visible slice is exhausted, until the full
    /// quantity is submitted.
    Iceberg { visible_qty: Decimal },
    /// Submits `trade.quantity * participation_rate` (capped at the
    /// remaining unsent quantity) on every observed
    /// `MarketEvent::Trade` for the symbol.
    Pov { participation_rate: Decimal },
}

/// Identifies an EMS-managed parent (algorithmic execution) order,
/// distinct from any individual child `OrderId`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ParentOrderId(pub Uuid);

/// Aggregate lifecycle state of a parent order, computed by an `Ems`
/// implementation across all of its child orders.
#[derive(Debug, Clone, PartialEq)]
pub enum ParentOrderStatus {
    /// Still actively submitting child orders per its `ExecutionAlgoSpec`.
    Scheduling,
    /// The full quantity has been submitted and every child order has
    /// reached a terminal state.
    Completed,
    /// Cancelled before completion; no further child orders will be
    /// submitted for it.
    Cancelled,
    /// Could not proceed (e.g. `Vwap` with insufficient historical
    /// data) -- no child orders were submitted at all.
    Failed { reason: String },
}

/// An order a `Strategy` wants to place, returned from
/// `on_market_event`. Distinct from `apexes_core::Order` (which represents
/// an order already accepted into the system with an assigned `OrderId`
/// and lifecycle state) -- `OrderIntent` is the strategy's *request*,
/// before the matching engine (backtest) or OMS (live) assigns it an
/// identity and begins tracking its status.
#[derive(Debug, Clone)]
pub struct OrderIntent {
    pub symbol: String,
    pub venue: Venue,
    pub asset_class: AssetClass,
    pub side: Side,
    pub order_type: OrderType,
    pub quantity: Decimal,
    /// Limit price (`Limit`, `StopLimit` only).
    pub price: Option<Decimal>,
    /// Trigger price (`Stop`, `StopLimit` only).
    pub stop_price: Option<Decimal>,
    /// `None`: submit immediately (today's behavior). `Some(spec)`: an
    /// EMS slices/paces submission per `spec` instead of submitting
    /// this intent directly.
    pub execution_algo: Option<ExecutionAlgoSpec>,
    /// How this intent should be routed across venues.
    /// `OrderRouting::Fixed` (the default a strategy should use unless
    /// it explicitly wants SOR/splitting) preserves today's behavior
    /// exactly: submit to `asset_class` as specified, no routing
    /// decision made.
    pub routing: OrderRouting,
    /// `None`: spot order (today's behavior). `Some(leverage)`: open/add
    /// to a leveraged perpetual position at this leverage.
    pub leverage: Option<Decimal>,
    /// Only read when `leverage.is_some()`; `None` defaults to
    /// `MarginMode::Isolated` (the safer default -- an isolated
    /// position's liquidation cannot cascade into other positions).
    pub margin_mode: Option<MarginMode>,
}

/// A net position in a single symbol.
#[derive(Debug, Clone, PartialEq)]
pub struct Position {
    pub symbol: String,
    /// Signed: positive = long, negative = short.
    pub quantity: Decimal,
    pub average_entry_price: Decimal,
    /// `None`: spot net position (today's behavior, unchanged). `Some`:
    /// this is a leveraged perpetual position opened at this leverage.
    pub leverage: Option<Decimal>,
    /// Only meaningful when `leverage.is_some()`.
    pub margin_mode: Option<MarginMode>,
    /// Only meaningful when `leverage.is_some()`. The margin actually
    /// allocated/reserved for this position, maintained INCREMENTALLY
    /// by `PortfolioLedger::apply_fill` (see the Plan Amendment task
    /// "apexes-core/apexes-backtest: leveraged Fill accounting" for the
    /// full accounting rules) -- NOT recomputed from `quantity *
    /// average_entry_price / leverage` on every read, because real
    /// exchanges (including Binance) accumulate margin per-fill rather
    /// than recompute the whole position's margin against its current
    /// average entry price on every trade. `Decimal::ZERO` for spot
    /// positions (`leverage: None`) and for any Position before its
    /// first leveraged fill.
    pub allocated_margin: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::MarketEvent;
    use rust_decimal_macros::dec;

    /// A trivial test `Strategy` that emits a fixed `OrderIntent` on every
    /// event, used to verify the trait's call shape compiles and works as
    /// expected. Real strategy logic will be added by future sub-projects
    /// consuming this trait, not by this plan.
    struct AlwaysBuyOneUnit {
        symbol: String,
        venue: Venue,
    }

    impl Strategy for AlwaysBuyOneUnit {
        fn on_market_event(
            &mut self,
            _event: &MarketEvent,
            _ctx: &StrategyContext,
        ) -> Vec<OrderIntent> {
            vec![OrderIntent {
                symbol: self.symbol.clone(),
                venue: self.venue,
                asset_class: AssetClass::Spot,
                side: Side::Buy,
                order_type: OrderType::Market,
                quantity: dec!(1),
                price: None,
                stop_price: None,
                execution_algo: None,
                routing: OrderRouting::Fixed,
                leverage: None,
                margin_mode: None,
            }]
        }
    }

    #[test]
    fn strategy_trait_is_callable_and_returns_intents() {
        let mut strategy = AlwaysBuyOneUnit {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
        };
        let positions = HashMap::new();
        let balances = HashMap::new();
        let open_orders = HashMap::new();
        let open_parent_orders = HashMap::new();
        let ctx = StrategyContext {
            positions: &positions,
            balances: &balances,
            open_orders: &open_orders,
            open_parent_orders: &open_parent_orders,
            margin_status: &HashMap::new(),
            now: Utc::now(),
        };
        let event = MarketEvent::Trade(apexes_core::TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(65000),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        });

        let intents = strategy.on_market_event(&event, &ctx);

        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].symbol, "BTCUSDT");
        assert_eq!(intents[0].side, Side::Buy);
        assert_eq!(intents[0].quantity, dec!(1));
    }

    #[test]
    fn strategy_context_exposes_provided_state() {
        let mut positions = HashMap::new();
        positions.insert(
            "BTCUSDT".to_string(),
            Position {
                symbol: "BTCUSDT".to_string(),
                quantity: dec!(2),
                average_entry_price: dec!(64000),
                leverage: None,
                margin_mode: None,
                allocated_margin: Decimal::ZERO,
            },
        );
        let balances = HashMap::new();
        let open_orders = HashMap::new();
        let open_parent_orders = HashMap::new();
        let now = Utc::now();

        let ctx = StrategyContext {
            positions: &positions,
            balances: &balances,
            open_orders: &open_orders,
            open_parent_orders: &open_parent_orders,
            margin_status: &HashMap::new(),
            now,
        };

        assert_eq!(ctx.positions.get("BTCUSDT").unwrap().quantity, dec!(2));
        assert_eq!(ctx.now, now);
    }
}
