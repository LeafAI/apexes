//! Conversions between `apexes_core`/`apexes_strategy` types and
//! `drift_script::DriftValue`, hand-written (not serde-based) so field
//! names and script-facing shapes are precisely controlled.

use apexes_core::{
    AssetClass, Bar, BarInterval, DepthUpdate, MarkPriceUpdate, MarketEvent, OrderType, PriceLevel,
    Side, Tick, TradeEvent, Venue,
};
use drift_script::DriftValue;
use std::collections::HashMap;
use std::sync::Arc;

use crate::strategy::{OrderIntent, Position};

fn decimal_to_number(value: rust_decimal::Decimal) -> DriftValue {
    DriftValue::Number(value.try_into().unwrap_or(f64::NAN))
}

fn string_value(s: impl Into<String>) -> DriftValue {
    DriftValue::String(Arc::new(s.into()))
}

pub(super) fn venue_to_str(venue: Venue) -> &'static str {
    match venue {
        Venue::Sse => "sse",
        Venue::Szse => "szse",
        Venue::Hkex => "hkex",
        Venue::Nasdaq => "nasdaq",
        Venue::Nyse => "nyse",
        Venue::Binance => "binance",
        Venue::Okx => "okx",
        Venue::Coinbase => "coinbase",
        Venue::EthereumMainnet => "ethereum_mainnet",
    }
}

fn side_to_str(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

fn bar_interval_to_str(interval: BarInterval) -> &'static str {
    match interval {
        BarInterval::OneSecond => "1s",
        BarInterval::OneMinute => "1m",
        BarInterval::FiveMinutes => "5m",
        BarInterval::FifteenMinutes => "15m",
        BarInterval::OneHour => "1h",
        BarInterval::OneDay => "1d",
    }
}

fn price_level_to_drift_value(level: &PriceLevel) -> DriftValue {
    let mut obj = HashMap::new();
    obj.insert("price".to_string(), decimal_to_number(level.price));
    obj.insert("quantity".to_string(), decimal_to_number(level.quantity));
    DriftValue::Object(obj)
}

/// Converts an `apexes_core::MarketEvent` to the `DriftValue::Object` shape
/// passed as `event` into a Drift Script strategy's `on_market_event(event,
/// ctx)` function. Every variant's object carries a `"type"` tag field
/// (`"tick"` / `"depth"` / `"bar"` / `"trade"`) plus variant-specific
/// fields. `Decimal` fields become `Number`; `DateTime<Utc>` fields become
/// ISO 8601 strings.
pub fn market_event_to_drift_value(event: &MarketEvent) -> DriftValue {
    let mut obj = HashMap::new();
    match event {
        MarketEvent::Tick(Tick {
            symbol,
            venue,
            bid_price,
            bid_size,
            ask_price,
            ask_size,
            timestamp,
        }) => {
            obj.insert("type".to_string(), string_value("tick"));
            obj.insert("symbol".to_string(), string_value(symbol.clone()));
            obj.insert("venue".to_string(), string_value(venue_to_str(*venue)));
            obj.insert("bid_price".to_string(), decimal_to_number(*bid_price));
            obj.insert("bid_size".to_string(), decimal_to_number(*bid_size));
            obj.insert("ask_price".to_string(), decimal_to_number(*ask_price));
            obj.insert("ask_size".to_string(), decimal_to_number(*ask_size));
            obj.insert(
                "timestamp".to_string(),
                string_value(timestamp.to_rfc3339()),
            );
        }
        MarketEvent::Depth(DepthUpdate {
            symbol,
            venue,
            bids,
            asks,
            sequence,
            source_range: _,
            is_snapshot,
            timestamp,
        }) => {
            obj.insert("type".to_string(), string_value("depth"));
            obj.insert("symbol".to_string(), string_value(symbol.clone()));
            obj.insert("venue".to_string(), string_value(venue_to_str(*venue)));
            obj.insert(
                "bids".to_string(),
                DriftValue::Array(bids.iter().map(price_level_to_drift_value).collect()),
            );
            obj.insert(
                "asks".to_string(),
                DriftValue::Array(asks.iter().map(price_level_to_drift_value).collect()),
            );
            obj.insert(
                "sequence".to_string(),
                sequence
                    .map(|s| DriftValue::Number(s as f64))
                    .unwrap_or(DriftValue::Null),
            );
            obj.insert("is_snapshot".to_string(), DriftValue::Bool(*is_snapshot));
            obj.insert(
                "timestamp".to_string(),
                string_value(timestamp.to_rfc3339()),
            );
        }
        MarketEvent::Bar(Bar {
            symbol,
            venue,
            open,
            high,
            low,
            close,
            volume,
            interval,
            open_time,
            close_time,
        }) => {
            obj.insert("type".to_string(), string_value("bar"));
            obj.insert("symbol".to_string(), string_value(symbol.clone()));
            obj.insert("venue".to_string(), string_value(venue_to_str(*venue)));
            obj.insert("open".to_string(), decimal_to_number(*open));
            obj.insert("high".to_string(), decimal_to_number(*high));
            obj.insert("low".to_string(), decimal_to_number(*low));
            obj.insert("close".to_string(), decimal_to_number(*close));
            obj.insert("volume".to_string(), decimal_to_number(*volume));
            obj.insert(
                "interval".to_string(),
                string_value(bar_interval_to_str(*interval)),
            );
            obj.insert(
                "open_time".to_string(),
                string_value(open_time.to_rfc3339()),
            );
            obj.insert(
                "close_time".to_string(),
                string_value(close_time.to_rfc3339()),
            );
        }
        MarketEvent::Trade(TradeEvent {
            symbol,
            venue,
            price,
            quantity,
            side,
            trade_id,
            timestamp,
        }) => {
            obj.insert("type".to_string(), string_value("trade"));
            obj.insert("symbol".to_string(), string_value(symbol.clone()));
            obj.insert("venue".to_string(), string_value(venue_to_str(*venue)));
            obj.insert("price".to_string(), decimal_to_number(*price));
            obj.insert("quantity".to_string(), decimal_to_number(*quantity));
            obj.insert(
                "side".to_string(),
                side.map(|s| string_value(side_to_str(s)))
                    .unwrap_or(DriftValue::Null),
            );
            obj.insert(
                "trade_id".to_string(),
                trade_id
                    .clone()
                    .map(string_value)
                    .unwrap_or(DriftValue::Null),
            );
            obj.insert(
                "timestamp".to_string(),
                string_value(timestamp.to_rfc3339()),
            );
        }
        MarketEvent::MarkPrice(MarkPriceUpdate {
            symbol,
            venue,
            mark_price,
            timestamp,
        }) => {
            obj.insert("type".to_string(), string_value("mark_price"));
            obj.insert("symbol".to_string(), string_value(symbol.clone()));
            obj.insert("venue".to_string(), string_value(venue_to_str(*venue)));
            obj.insert("mark_price".to_string(), decimal_to_number(*mark_price));
            obj.insert(
                "timestamp".to_string(),
                string_value(timestamp.to_rfc3339()),
            );
        }
    }
    DriftValue::Object(obj)
}

/// Converts a `Position` to the `DriftValue::Object` shape returned by
/// `ctx.get_position(symbol)`.
pub fn position_to_drift_value(position: &Position) -> DriftValue {
    let mut obj = HashMap::new();
    obj.insert("symbol".to_string(), string_value(position.symbol.clone()));
    obj.insert("quantity".to_string(), decimal_to_number(position.quantity));
    obj.insert(
        "average_entry_price".to_string(),
        decimal_to_number(position.average_entry_price),
    );
    DriftValue::Object(obj)
}

/// The default value `ctx.get_position(symbol)` returns when there is no
/// existing position for `symbol`: `{ symbol, quantity: 0,
/// average_entry_price: 0 }` (not `null` -- scripts can always read
/// `.quantity` without a null check).
pub fn default_position_drift_value(symbol: &str) -> DriftValue {
    let mut obj = HashMap::new();
    obj.insert("symbol".to_string(), string_value(symbol.to_string()));
    obj.insert("quantity".to_string(), DriftValue::Number(0.0));
    obj.insert("average_entry_price".to_string(), DriftValue::Number(0.0));
    DriftValue::Object(obj)
}

fn str_field<'a>(
    obj: &'a HashMap<String, DriftValue>,
    key: &str,
    fn_name: &str,
) -> Result<&'a str, String> {
    match obj.get(key) {
        Some(DriftValue::String(s)) => Ok(s.as_str()),
        Some(other) => Err(format!(
            "{fn_name}: field {key:?} must be a string, got {}",
            drift_value_type_name(other)
        )),
        None => Err(format!("{fn_name}: missing required field {key:?}")),
    }
}

fn num_field(obj: &HashMap<String, DriftValue>, key: &str, fn_name: &str) -> Result<f64, String> {
    match obj.get(key) {
        Some(DriftValue::Number(n)) => Ok(*n),
        Some(other) => Err(format!(
            "{fn_name}: field {key:?} must be a number, got {}",
            drift_value_type_name(other)
        )),
        None => Err(format!("{fn_name}: missing required field {key:?}")),
    }
}

fn optional_num_field(
    obj: &HashMap<String, DriftValue>,
    key: &str,
    fn_name: &str,
) -> Result<Option<f64>, String> {
    match obj.get(key) {
        None | Some(DriftValue::Null) | Some(DriftValue::Undefined) => Ok(None),
        Some(DriftValue::Number(n)) => Ok(Some(*n)),
        Some(other) => Err(format!(
            "{fn_name}: field {key:?} must be a number if present, got {}",
            drift_value_type_name(other)
        )),
    }
}

fn drift_value_type_name(value: &DriftValue) -> &'static str {
    match value {
        DriftValue::Null => "null",
        DriftValue::Undefined => "undefined",
        DriftValue::Bool(_) => "bool",
        DriftValue::Number(_) => "number",
        DriftValue::String(_) => "string",
        DriftValue::Array(_) => "array",
        DriftValue::Object(_) => "object",
        DriftValue::Function(_) => "function",
        DriftValue::NativeFunction(_) => "native function",
        DriftValue::Promise(_) => "promise",
        DriftValue::Regex { .. } => "regex",
    }
}

fn parse_side(s: &str, fn_name: &str) -> Result<Side, String> {
    match s.to_ascii_lowercase().as_str() {
        "buy" => Ok(Side::Buy),
        "sell" => Ok(Side::Sell),
        other => Err(format!(
            "{fn_name}: unknown side {other:?} (expected \"buy\" or \"sell\")"
        )),
    }
}

fn parse_order_type(s: &str, fn_name: &str) -> Result<OrderType, String> {
    match s.to_ascii_lowercase().as_str() {
        "market" => Ok(OrderType::Market),
        "limit" => Ok(OrderType::Limit),
        "stop" => Ok(OrderType::Stop),
        "stop_limit" => Ok(OrderType::StopLimit),
        other => Err(format!(
            "{fn_name}: unknown order_type {other:?} (expected \"market\", \"limit\", \"stop\", or \"stop_limit\")"
        )),
    }
}

fn parse_venue(s: &str, fn_name: &str) -> Result<Venue, String> {
    match s.to_ascii_lowercase().as_str() {
        "sse" => Ok(Venue::Sse),
        "szse" => Ok(Venue::Szse),
        "hkex" => Ok(Venue::Hkex),
        "nasdaq" => Ok(Venue::Nasdaq),
        "nyse" => Ok(Venue::Nyse),
        "binance" => Ok(Venue::Binance),
        "okx" => Ok(Venue::Okx),
        "coinbase" => Ok(Venue::Coinbase),
        other => Err(format!("{fn_name}: unknown venue {other:?}")),
    }
}

fn parse_asset_class(s: &str, fn_name: &str) -> Result<AssetClass, String> {
    match s.to_ascii_lowercase().as_str() {
        "spot" => Ok(AssetClass::Spot),
        "perpetual" => Ok(AssetClass::Perpetual),
        "future" => Ok(AssetClass::Future),
        "option" => Ok(AssetClass::Option),
        "equity" => Ok(AssetClass::Equity),
        other => Err(format!("{fn_name}: unknown asset_class {other:?}")),
    }
}

fn f64_to_decimal(value: f64, field: &str, fn_name: &str) -> Result<rust_decimal::Decimal, String> {
    rust_decimal::Decimal::try_from(value)
        .map_err(|_| format!("{fn_name}: field {field:?} is not a finite number ({value})"))
}

/// Parses a Drift Script strategy's `on_market_event` return value (expected
/// to be a `DriftValue::Array` of order-intent objects) into
/// `Vec<OrderIntent>`. Returns `Err(message)` on any structural or
/// validation failure -- the caller ([`crate::drift_dsl::DriftScriptStrategy`])
/// logs the message and returns an empty `Vec` rather than propagating it,
/// per the project's "single tick's strategy failure must not halt the
/// loop" requirement.
pub fn order_intents_from_drift_value(value: DriftValue) -> Result<Vec<OrderIntent>, String> {
    const FN: &str = "on_market_event return value";
    let DriftValue::Array(items) = value else {
        return Err(format!(
            "{FN}: expected an array, got {}",
            drift_value_type_name(&value)
        ));
    };

    items
        .into_iter()
        .enumerate()
        .map(|(i, item)| {
            let DriftValue::Object(obj) = item else {
                return Err(format!(
                    "{FN}[{i}]: expected an object, got {}",
                    drift_value_type_name(&item)
                ));
            };

            let symbol = str_field(&obj, "symbol", FN)?.to_string();
            let venue = parse_venue(str_field(&obj, "venue", FN)?, FN)?;
            let asset_class = parse_asset_class(str_field(&obj, "asset_class", FN)?, FN)?;
            let side = parse_side(str_field(&obj, "side", FN)?, FN)?;
            let order_type = parse_order_type(str_field(&obj, "order_type", FN)?, FN)?;
            let quantity = f64_to_decimal(num_field(&obj, "quantity", FN)?, "quantity", FN)?;

            let price = optional_num_field(&obj, "price", FN)?
                .map(|n| f64_to_decimal(n, "price", FN))
                .transpose()?;
            let stop_price = optional_num_field(&obj, "stop_price", FN)?
                .map(|n| f64_to_decimal(n, "stop_price", FN))
                .transpose()?;

            match order_type {
                OrderType::Limit | OrderType::StopLimit if price.is_none() => {
                    return Err(format!(
                        "{FN}[{i}]: order_type {order_type:?} requires a \"price\" field"
                    ));
                }
                OrderType::Stop | OrderType::StopLimit if stop_price.is_none() => {
                    return Err(format!(
                        "{FN}[{i}]: order_type {order_type:?} requires a \"stop_price\" field"
                    ));
                }
                _ => {}
            }

            Ok(OrderIntent {
                symbol,
                venue,
                asset_class,
                side,
                order_type,
                quantity,
                price,
                stop_price,
                execution_algo: None,
                routing: crate::strategy::OrderRouting::Fixed,
                leverage: None,
                margin_mode: None,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::TradeEvent;
    use chrono::Utc;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    fn get_str<'a>(obj: &'a HashMap<String, DriftValue>, key: &str) -> &'a str {
        match obj.get(key) {
            Some(DriftValue::String(s)) => s.as_str(),
            other => panic!("expected string field {key:?}, got {other:?}"),
        }
    }

    fn get_num(obj: &HashMap<String, DriftValue>, key: &str) -> f64 {
        match obj.get(key) {
            Some(DriftValue::Number(n)) => *n,
            other => panic!("expected number field {key:?}, got {other:?}"),
        }
    }

    #[test]
    fn tick_converts_with_type_tag_and_all_fields() {
        let event = MarketEvent::Tick(Tick {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bid_price: dec!(64999),
            bid_size: dec!(1.5),
            ask_price: dec!(65001),
            ask_size: dec!(2.0),
            timestamp: Utc::now(),
        });
        let DriftValue::Object(obj) = market_event_to_drift_value(&event) else {
            panic!("expected Object");
        };
        assert_eq!(get_str(&obj, "type"), "tick");
        assert_eq!(get_str(&obj, "symbol"), "BTCUSDT");
        assert_eq!(get_str(&obj, "venue"), "binance");
        assert_eq!(get_num(&obj, "bid_price"), 64999.0);
        assert_eq!(get_num(&obj, "ask_size"), 2.0);
    }

    #[test]
    fn trade_with_none_side_and_trade_id_uses_null() {
        let event = MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(65000),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        });
        let DriftValue::Object(obj) = market_event_to_drift_value(&event) else {
            panic!("expected Object");
        };
        assert_eq!(get_str(&obj, "type"), "trade");
        assert!(matches!(obj.get("side"), Some(DriftValue::Null)));
        assert!(matches!(obj.get("trade_id"), Some(DriftValue::Null)));
    }

    #[test]
    fn depth_converts_bids_and_asks_as_arrays_of_price_levels() {
        let event = MarketEvent::Depth(DepthUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bids: vec![PriceLevel {
                price: dec!(64999),
                quantity: dec!(1),
            }],
            asks: vec![PriceLevel {
                price: dec!(65001),
                quantity: dec!(2),
            }],
            sequence: Some(42),
            source_range: None,
            is_snapshot: true,
            timestamp: Utc::now(),
        });
        let DriftValue::Object(obj) = market_event_to_drift_value(&event) else {
            panic!("expected Object");
        };
        assert_eq!(get_str(&obj, "type"), "depth");
        let DriftValue::Array(bids) = obj.get("bids").unwrap() else {
            panic!("expected array")
        };
        assert_eq!(bids.len(), 1);
        assert_eq!(get_num(&obj, "sequence"), 42.0);
        assert!(matches!(
            obj.get("is_snapshot"),
            Some(DriftValue::Bool(true))
        ));
    }

    #[test]
    fn bar_converts_all_ohlcv_fields() {
        let event = MarketEvent::Bar(Bar {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            open: dec!(64000),
            high: dec!(65500),
            low: dec!(63800),
            close: dec!(65000),
            volume: dec!(1234.5),
            interval: BarInterval::OneHour,
            open_time: Utc::now(),
            close_time: Utc::now(),
        });
        let DriftValue::Object(obj) = market_event_to_drift_value(&event) else {
            panic!("expected Object");
        };
        assert_eq!(get_str(&obj, "type"), "bar");
        assert_eq!(get_str(&obj, "interval"), "1h");
        assert_eq!(get_num(&obj, "close"), 65000.0);
    }

    #[test]
    fn position_to_drift_value_converts_all_fields() {
        let position = Position {
            symbol: "BTCUSDT".to_string(),
            quantity: dec!(2.5),
            average_entry_price: dec!(64500),
            leverage: None,
            margin_mode: None,
            allocated_margin: Decimal::ZERO,
        };
        let DriftValue::Object(obj) = position_to_drift_value(&position) else {
            panic!("expected Object");
        };
        assert_eq!(get_str(&obj, "symbol"), "BTCUSDT");
        assert_eq!(get_num(&obj, "quantity"), 2.5);
        assert_eq!(get_num(&obj, "average_entry_price"), 64500.0);
    }

    #[test]
    fn default_position_drift_value_has_zero_quantity_not_null() {
        let DriftValue::Object(obj) = default_position_drift_value("ETHUSDT") else {
            panic!("expected Object");
        };
        assert_eq!(get_str(&obj, "symbol"), "ETHUSDT");
        assert_eq!(get_num(&obj, "quantity"), 0.0);
        assert_eq!(get_num(&obj, "average_entry_price"), 0.0);
    }

    fn drift_object(fields: &[(&str, DriftValue)]) -> DriftValue {
        let mut obj = HashMap::new();
        for (k, v) in fields {
            obj.insert(k.to_string(), v.clone());
        }
        DriftValue::Object(obj)
    }

    #[test]
    fn valid_market_order_intent_parses() {
        let value = DriftValue::Array(vec![drift_object(&[
            ("symbol", string_value("BTCUSDT")),
            ("venue", string_value("Binance")),
            ("asset_class", string_value("spot")),
            ("side", string_value("BUY")),
            ("order_type", string_value("market")),
            ("quantity", DriftValue::Number(1.5)),
        ])]);
        let intents = order_intents_from_drift_value(value).unwrap();
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].symbol, "BTCUSDT");
        assert_eq!(intents[0].venue, Venue::Binance);
        assert_eq!(intents[0].asset_class, AssetClass::Spot);
        assert_eq!(intents[0].side, Side::Buy);
        assert_eq!(intents[0].order_type, OrderType::Market);
        assert_eq!(intents[0].quantity, dec!(1.5));
        assert_eq!(intents[0].price, None);
    }

    #[test]
    fn valid_limit_order_intent_with_price_parses() {
        let value = DriftValue::Array(vec![drift_object(&[
            ("symbol", string_value("BTCUSDT")),
            ("venue", string_value("binance")),
            ("asset_class", string_value("spot")),
            ("side", string_value("sell")),
            ("order_type", string_value("limit")),
            ("quantity", DriftValue::Number(1.0)),
            ("price", DriftValue::Number(65000.0)),
        ])]);
        let intents = order_intents_from_drift_value(value).unwrap();
        assert_eq!(intents[0].order_type, OrderType::Limit);
        assert_eq!(intents[0].price, Some(dec!(65000)));
    }

    #[test]
    fn non_array_top_level_is_rejected() {
        let err = order_intents_from_drift_value(DriftValue::Number(1.0)).unwrap_err();
        assert!(err.contains("expected an array"));
    }

    #[test]
    fn non_object_element_is_rejected() {
        let value = DriftValue::Array(vec![DriftValue::Number(1.0)]);
        let err = order_intents_from_drift_value(value).unwrap_err();
        assert!(err.contains("expected an object"));
    }

    #[test]
    fn missing_required_field_is_rejected() {
        let value = DriftValue::Array(vec![drift_object(&[
            ("symbol", string_value("BTCUSDT")),
            ("venue", string_value("binance")),
            ("asset_class", string_value("spot")),
            ("side", string_value("buy")),
            ("order_type", string_value("market")),
            // missing "quantity"
        ])]);
        let err = order_intents_from_drift_value(value).unwrap_err();
        assert!(err.contains("quantity"));
    }

    #[test]
    fn invalid_side_string_is_rejected() {
        let value = DriftValue::Array(vec![drift_object(&[
            ("symbol", string_value("BTCUSDT")),
            ("venue", string_value("binance")),
            ("asset_class", string_value("spot")),
            ("side", string_value("sideways")),
            ("order_type", string_value("market")),
            ("quantity", DriftValue::Number(1.0)),
        ])]);
        let err = order_intents_from_drift_value(value).unwrap_err();
        assert!(err.contains("unknown side"));
    }

    #[test]
    fn invalid_order_type_string_is_rejected() {
        let value = DriftValue::Array(vec![drift_object(&[
            ("symbol", string_value("BTCUSDT")),
            ("venue", string_value("binance")),
            ("asset_class", string_value("spot")),
            ("side", string_value("buy")),
            ("order_type", string_value("trailing_stop")),
            ("quantity", DriftValue::Number(1.0)),
        ])]);
        let err = order_intents_from_drift_value(value).unwrap_err();
        assert!(err.contains("unknown order_type"));
    }

    #[test]
    fn invalid_venue_string_is_rejected() {
        let value = DriftValue::Array(vec![drift_object(&[
            ("symbol", string_value("BTCUSDT")),
            ("venue", string_value("kraken")),
            ("side", string_value("buy")),
            ("order_type", string_value("market")),
            ("quantity", DriftValue::Number(1.0)),
        ])]);
        let err = order_intents_from_drift_value(value).unwrap_err();
        assert!(err.contains("unknown venue"));
    }

    #[test]
    fn invalid_asset_class_string_is_rejected() {
        let value = DriftValue::Array(vec![drift_object(&[
            ("symbol", string_value("BTCUSDT")),
            ("venue", string_value("binance")),
            ("asset_class", string_value("dogecoin_futures")),
            ("side", string_value("buy")),
            ("order_type", string_value("market")),
            ("quantity", DriftValue::Number(1.0)),
        ])]);
        let err = order_intents_from_drift_value(value).unwrap_err();
        assert!(err.contains("unknown asset_class"));
    }

    #[test]
    fn limit_order_missing_price_is_rejected() {
        let value = DriftValue::Array(vec![drift_object(&[
            ("symbol", string_value("BTCUSDT")),
            ("venue", string_value("binance")),
            ("asset_class", string_value("spot")),
            ("side", string_value("buy")),
            ("order_type", string_value("limit")),
            ("quantity", DriftValue::Number(1.0)),
        ])]);
        let err = order_intents_from_drift_value(value).unwrap_err();
        assert!(err.contains("requires a \"price\" field"));
    }

    #[test]
    fn stop_order_missing_stop_price_is_rejected() {
        let value = DriftValue::Array(vec![drift_object(&[
            ("symbol", string_value("BTCUSDT")),
            ("venue", string_value("binance")),
            ("asset_class", string_value("spot")),
            ("side", string_value("buy")),
            ("order_type", string_value("stop")),
            ("quantity", DriftValue::Number(1.0)),
        ])]);
        let err = order_intents_from_drift_value(value).unwrap_err();
        assert!(err.contains("requires a \"stop_price\" field"));
    }

    #[test]
    fn non_finite_quantity_is_rejected() {
        let value = DriftValue::Array(vec![drift_object(&[
            ("symbol", string_value("BTCUSDT")),
            ("venue", string_value("binance")),
            ("asset_class", string_value("spot")),
            ("side", string_value("buy")),
            ("order_type", string_value("market")),
            ("quantity", DriftValue::Number(f64::NAN)),
        ])]);
        let err = order_intents_from_drift_value(value).unwrap_err();
        assert!(err.contains("not a finite number"));
    }
}
