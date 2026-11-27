//! F04.03 — Detached data-only Drift market metadata, no exchange actions.
use crate::InstrumentContext;
use apexes_core::{AssetClass, MarketEvent, SymbolRules};
use drift_script::DriftValue;
use rust_decimal::Decimal;
use std::{collections::HashMap, sync::Arc};

fn string(value: &str) -> DriftValue {
    DriftValue::String(Arc::new(value.to_owned()))
}

fn number(value: Decimal) -> DriftValue {
    f64::try_from(value)
        .ok()
        .map_or(DriftValue::Null, DriftValue::Number)
}

fn object<const N: usize>(fields: [(&str, DriftValue); N]) -> DriftValue {
    DriftValue::Object(
        fields
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v))
            .collect::<HashMap<_, _>>(),
    )
}

fn rules_value(rules: &SymbolRules) -> DriftValue {
    object([
        ("quantity_step", number(rules.quantity_step)),
        ("min_quantity", number(rules.min_quantity)),
        ("max_quantity", number(rules.max_quantity)),
        ("price_tick", number(rules.price_tick)),
        ("min_notional", number(rules.min_notional)),
    ])
}

fn market_value(market: InstrumentContext<'_>) -> DriftValue {
    let instrument = market.instrument();
    let asset_class = match instrument.asset_class {
        AssetClass::Spot => "spot",
        AssetClass::Perpetual => "perpetual",
        AssetClass::Future => "future",
        AssetClass::Option => "option",
        AssetClass::Equity => "equity",
    };
    object([
        ("symbol", string(&instrument.symbol)),
        (
            "venue",
            string(super::convert::venue_to_str(instrument.venue)),
        ),
        ("asset_class", string(asset_class)),
        (
            "base_currency",
            instrument
                .base_currency
                .as_deref()
                .map_or(DriftValue::Null, string),
        ),
        (
            "quote_currency",
            instrument
                .quote_currency
                .as_deref()
                .map_or(DriftValue::Null, string),
        ),
        ("tick_size", number(instrument.tick_size)),
        ("lot_size", number(instrument.lot_size)),
        ("multiplier", number(instrument.multiplier)),
        (
            "rules",
            market.rules().map_or(DriftValue::Null, rules_value),
        ),
    ])
}

pub(super) fn attach(
    ctx: &mut DriftValue,
    event: &MarketEvent,
    market: Option<InstrumentContext<'_>>,
) {
    if let DriftValue::Object(fields) = ctx {
        fields.insert(
            "market".into(),
            market
                .filter(|m| m.matches(event))
                .map_or(DriftValue::Null, market_value),
        );
    }
}
