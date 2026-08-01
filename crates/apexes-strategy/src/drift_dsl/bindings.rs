//! `ctx.*` native-function bindings exposed to Drift Script strategy
//! scripts: `get_position`, `get_balance`, `now`, `has_open_order`. Built
//! fresh on every `DriftScriptStrategy::on_market_event` call from owned
//! clones of the current `StrategyContext`'s borrowed maps, since
//! `DriftValue::NativeFunction` requires `'static` closures while
//! `StrategyContext<'a>`'s maps are only borrowed for `'a` (see design
//! spec §6).

use apexes_core::{Order, OrderId};
use chrono::{DateTime, Utc};
use drift_script::{DriftValue, NativeFn};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;

use crate::drift_dsl::convert::{default_position_drift_value, position_to_drift_value};
use crate::strategy::Position;

/// Builds the `ctx` object passed as the second argument to a strategy
/// script's `on_market_event(event, ctx)` function, from owned clones of
/// the current tick's state. `now` is baked in directly as a string field;
/// `get_position`/`get_balance`/`has_open_order` are `NativeFunction`
/// closures capturing owned clones of `positions`/`balances`/`open_orders`.
pub fn build_ctx_drift_value(
    positions: HashMap<String, Position>,
    balances: HashMap<String, Decimal>,
    open_orders: HashMap<OrderId, Order>,
    now: DateTime<Utc>,
) -> DriftValue {
    let mut obj: HashMap<String, DriftValue> = HashMap::new();
    obj.insert(
        "now".to_string(),
        DriftValue::String(Arc::new(now.to_rfc3339())),
    );
    obj.insert(
        "get_position".to_string(),
        DriftValue::NativeFunction(get_position_fn(positions)),
    );
    obj.insert(
        "get_balance".to_string(),
        DriftValue::NativeFunction(get_balance_fn(balances)),
    );
    obj.insert(
        "has_open_order".to_string(),
        DriftValue::NativeFunction(has_open_order_fn(open_orders)),
    );
    DriftValue::Object(obj)
}

fn first_string_arg(args: &[DriftValue]) -> Option<String> {
    match args.first() {
        Some(DriftValue::String(s)) => Some(s.as_str().to_string()),
        _ => None,
    }
}

fn get_position_fn(positions: HashMap<String, Position>) -> NativeFn {
    Arc::new(move |args| {
        let positions = positions.clone();
        Box::pin(async move {
            let Some(symbol) = first_string_arg(&args) else {
                return DriftValue::Undefined;
            };
            match positions.get(&symbol) {
                Some(position) => position_to_drift_value(position),
                None => default_position_drift_value(&symbol),
            }
        })
    })
}

fn get_balance_fn(balances: HashMap<String, Decimal>) -> NativeFn {
    Arc::new(move |args| {
        let balances = balances.clone();
        Box::pin(async move {
            let Some(asset) = first_string_arg(&args) else {
                return DriftValue::Undefined;
            };
            let value = balances.get(&asset).copied().unwrap_or(Decimal::ZERO);
            DriftValue::Number(value.try_into().unwrap_or(0.0))
        })
    })
}

fn has_open_order_fn(open_orders: HashMap<OrderId, Order>) -> NativeFn {
    Arc::new(move |args| {
        let open_orders = open_orders.clone();
        Box::pin(async move {
            let Some(symbol) = first_string_arg(&args) else {
                return DriftValue::Undefined;
            };
            let has_any = open_orders.values().any(|order| order.symbol == symbol);
            DriftValue::Bool(has_any)
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, OrderStatus, OrderType, Side, Venue};
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn call(f: &NativeFn, args: Vec<DriftValue>) -> DriftValue {
        futures::executor::block_on(f(args))
    }

    #[test]
    fn get_position_returns_default_when_absent() {
        let f = get_position_fn(HashMap::new());
        let DriftValue::Object(obj) = call(
            &f,
            vec![DriftValue::String(Arc::new("BTCUSDT".to_string()))],
        ) else {
            panic!("expected Object");
        };
        assert!(matches!(obj.get("quantity"), Some(DriftValue::Number(n)) if *n == 0.0));
    }

    #[test]
    fn get_position_returns_existing_position() {
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
        let f = get_position_fn(positions);
        let DriftValue::Object(obj) = call(
            &f,
            vec![DriftValue::String(Arc::new("BTCUSDT".to_string()))],
        ) else {
            panic!("expected Object");
        };
        assert!(matches!(obj.get("quantity"), Some(DriftValue::Number(n)) if *n == 2.0));
    }

    #[test]
    fn get_balance_returns_zero_when_absent() {
        let f = get_balance_fn(HashMap::new());
        let result = call(&f, vec![DriftValue::String(Arc::new("USDT".to_string()))]);
        assert!(matches!(result, DriftValue::Number(n) if n == 0.0));
    }

    #[test]
    fn get_balance_returns_existing_balance() {
        let mut balances = HashMap::new();
        balances.insert("USDT".to_string(), dec!(10000));
        let f = get_balance_fn(balances);
        let result = call(&f, vec![DriftValue::String(Arc::new("USDT".to_string()))]);
        assert!(matches!(result, DriftValue::Number(n) if n == 10000.0));
    }

    #[test]
    fn has_open_order_false_when_none_match() {
        let f = has_open_order_fn(HashMap::new());
        let result = call(
            &f,
            vec![DriftValue::String(Arc::new("BTCUSDT".to_string()))],
        );
        assert!(matches!(result, DriftValue::Bool(false)));
    }

    #[test]
    fn has_open_order_true_when_any_side_matches() {
        let mut open_orders = HashMap::new();
        let id = OrderId(Uuid::new_v4());
        open_orders.insert(
            id,
            Order {
                id,
                exchange_order_id: None,
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                asset_class: AssetClass::Spot,
                side: Side::Sell,
                order_type: OrderType::Limit,
                quantity: dec!(1),
                price: Some(dec!(65000)),
                stop_price: None,
                leverage: None,
                margin_mode: None,
                filled_quantity: dec!(0),
                status: OrderStatus::New,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            },
        );
        let f = has_open_order_fn(open_orders);
        let result = call(
            &f,
            vec![DriftValue::String(Arc::new("BTCUSDT".to_string()))],
        );
        assert!(matches!(result, DriftValue::Bool(true)));
    }
}
