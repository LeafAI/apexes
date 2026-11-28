//! F04.03 — Detached data-only Drift market metadata, no exchange actions.
use crate::{BookIntegrity, BookObservation, InstrumentContext, StrategyContext};
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

fn level_value(level: apexes_core::PriceLevel) -> DriftValue {
    object([
        ("price", number(level.price)),
        ("quantity", number(level.quantity)),
    ])
}

fn book_value(book: &BookObservation) -> DriftValue {
    let integrity = match book.integrity {
        BookIntegrity::SnapshotOnly => "snapshot_only",
        BookIntegrity::Contiguous => "contiguous",
        BookIntegrity::VerifiedRanges => "verified_ranges",
    };
    object([
        ("symbol", string(&book.symbol)),
        ("venue", string(super::convert::venue_to_str(book.venue))),
        ("source", string("simulated_remaining_depth")),
        ("observed_at", string(&book.observed_at.to_rfc3339())),
        ("decision_at", string(&book.decision_at.to_rfc3339())),
        ("age_ms", DriftValue::Number(book.age_ms() as f64)),
        ("max_age_ms", DriftValue::Number(f64::from(book.max_age_ms))),
        ("integrity", string(integrity)),
        // u64 exchange sequence IDs are not necessarily exact f64 values.
        (
            "sequence",
            book.sequence
                .map_or(DriftValue::Null, |seq| string(&seq.to_string())),
        ),
        (
            "best_bid",
            book.best_bid.map_or(DriftValue::Null, level_value),
        ),
        (
            "best_ask",
            book.best_ask.map_or(DriftValue::Null, level_value),
        ),
        ("spread", book.spread().map_or(DriftValue::Null, number)),
    ])
}

fn cost_value(costs: &crate::CostObservation) -> DriftValue {
    let snapshot = costs.clone();
    let estimate: drift_script::NativeFn = Arc::new(move |args| {
        let snapshot = snapshot.clone();
        Box::pin(async move {
            let [DriftValue::Number(notional), DriftValue::String(role)] = args.as_slice() else {
                return DriftValue::Undefined;
            };
            let role = match role.as_str() {
                "maker" => crate::FeeRole::Maker,
                "taker" => crate::FeeRole::Taker,
                _ => return DriftValue::Undefined,
            };
            Decimal::try_from(*notional)
                .ok()
                .and_then(|n| snapshot.estimate_fee(n, role))
                .map_or(DriftValue::Null, number)
        })
    });
    object([
        ("symbol", string(costs.symbol())),
        ("venue", string(super::convert::venue_to_str(costs.venue()))),
        ("source", string("frozen_simulated_fees")),
        ("quote_currency", string(costs.quote_currency())),
        ("maker_fee_bps", number(costs.maker_fee_bps())),
        ("taker_fee_bps", number(costs.taker_fee_bps())),
        ("estimate_fee", DriftValue::NativeFunction(estimate)),
    ])
}

fn capital_value(capital: &HashMap<String, crate::CapitalObservation>) -> DriftValue {
    let snapshot = Arc::new(capital.clone());
    let lookup: drift_script::NativeFn = Arc::new(move |args| {
        let snapshot = Arc::clone(&snapshot);
        Box::pin(async move {
            let [DriftValue::String(asset)] = args.as_slice() else {
                return DriftValue::Undefined;
            };
            snapshot
                .get(asset.as_str())
                .map_or(DriftValue::Null, |entry| {
                    object([
                        ("ledger_balance", number(entry.ledger_balance)),
                        ("reserved", number(entry.reserved)),
                        ("unreserved", number(entry.unreserved)),
                    ])
                })
        })
    });
    object([
        ("source", string("simulated_spot_reservations")),
        ("get", DriftValue::NativeFunction(lookup)),
    ])
}

pub(super) fn attach(ctx: &mut DriftValue, event: &MarketEvent, context: &StrategyContext<'_>) {
    if let DriftValue::Object(fields) = ctx {
        let market = context.market.filter(|m| m.matches(event));
        fields.insert(
            "market".into(),
            market.map_or(DriftValue::Null, market_value),
        );
        let book = context
            .book
            .as_ref()
            .filter(|book| market.is_some() && book.matches_at(event, context.now));
        fields.insert("book".into(), book.map_or(DriftValue::Null, book_value));
        let costs = context
            .costs
            .as_ref()
            .filter(|c| market.is_some_and(|m| c.matches(event, m.instrument())));
        fields.insert("costs".into(), costs.map_or(DriftValue::Null, cost_value));
        fields.insert(
            "capital".into(),
            context
                .capital
                .as_ref()
                .map_or(DriftValue::Null, capital_value),
        );
    }
}
