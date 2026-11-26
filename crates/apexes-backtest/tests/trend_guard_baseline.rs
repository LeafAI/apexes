//! F00.01: deterministic, offline replay of the original deployed Drift
//! strategy through the real matching engine and shared trading loop.
//! Synthetic data tests execution correctness, not trading profitability.

use apexes_backtest::{
    BacktestConfig, BacktestReport, CostModel, FlatFeeSchedule, InMemoryMarketDataSource,
    ReplaySpeed, run_backtest,
};
use apexes_core::registry::Registry;
use apexes_core::{
    AssetClass, DepthUpdate, Instrument, MarketEvent, MarketType, PriceLevel, Side, TradeEvent,
    Venue,
};
use apexes_strategy::drift_dsl::DriftScriptStrategy;
use chrono::{Duration, NaiveDate, TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::{
    collections::HashMap,
    io::Write,
    path::Path,
    sync::{Arc, Mutex},
};

const SYMBOL: &str = "ETHUSDT";

fn fixture(prices: &[Decimal]) -> Vec<MarketEvent> {
    let start = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let mut events = Vec::with_capacity(prices.len() * 2);
    for (i, &price) in prices.iter().enumerate() {
        let timestamp = start + Duration::seconds(i as i64 * 2);
        events.push(MarketEvent::Depth(DepthUpdate {
            symbol: SYMBOL.into(),
            venue: Venue::Binance,
            bids: vec![PriceLevel {
                price: price - dec!(0.01),
                quantity: dec!(10),
            }],
            asks: vec![PriceLevel {
                price: price + dec!(0.01),
                quantity: dec!(10),
            }],
            sequence: Some(i as u64 + 1),
            is_snapshot: true,
            timestamp,
        }));
        events.push(MarketEvent::Trade(TradeEvent {
            symbol: SYMBOL.into(),
            venue: Venue::Binance,
            price,
            quantity: dec!(1),
            side: Some(Side::Buy),
            trade_id: Some(format!("fixture-{i}")),
            timestamp: timestamp + Duration::seconds(1),
        }));
    }
    events
}

fn replay(prices: &[Decimal], taker_bps: Decimal) -> BacktestReport {
    replay_with_registry(prices, taker_bps, &Registry::new())
}

fn replay_with_registry(
    prices: &[Decimal],
    taker_bps: Decimal,
    registry: &Registry,
) -> BacktestReport {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();
    let script =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/strategy.trend_guard.drift");
    let strategy = DriftScriptStrategy::from_file(&script).unwrap();
    let config = BacktestConfig {
        instruments: vec![
            Instrument::new(
                SYMBOL,
                MarketType::Crypto,
                Venue::Binance,
                AssetClass::Spot,
                Some("ETH".into()),
                Some("USDT".into()),
                dec!(0.01),
                dec!(0.0001),
                dec!(1),
            )
            .unwrap(),
        ],
        date_range: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
            ..NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(),
        initial_balances: HashMap::from([("USDT".into(), dec!(1000))]),
        cost_model: CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: Decimal::ZERO,
                taker_bps,
            }),
            funding_rate_bps: None,
        },
        speed: ReplaySpeed::AsFastAsPossible,
        queue_position_factor: Decimal::ZERO,
        risk_free_return: Decimal::ZERO,
        margin_tiers: HashMap::new(),
        market_condition_config: None,
        control: None,
    };
    // Drift internally uses Handle::block_on: enter the runtime but drive
    // the loop outside Tokio's task context, just as apexes-engine does.
    futures::executor::block_on(run_backtest(
        config,
        registry,
        InMemoryMarketDataSource::new(fixture(prices)),
        strategy,
    ))
    .unwrap()
}

fn trending_prices() -> Vec<Decimal> {
    let mut prices = vec![dec!(100); 120];
    prices.extend((1..=60).map(|i| dec!(100) + Decimal::from(i) * dec!(0.05)));
    prices.extend([dec!(98); 40]);
    prices
}

fn economic_report(report: &BacktestReport) -> serde_json::Value {
    // Execution IDs are deliberately random UUIDs. Keep all other report
    // fields, including timestamps, quantity, fees and equity, comparable.
    let mut value = serde_json::to_value(report).unwrap();
    for fill in value["trades"].as_array_mut().unwrap() {
        let fields = fill.as_object_mut().unwrap();
        fields.remove("id");
        fields.remove("order_id");
    }
    value
}

#[test]
fn flat_market_does_not_create_orders() {
    let report = replay(&[dec!(100); 250], dec!(10));
    assert!(report.trades.is_empty());
    assert!(!report.equity_curve.is_empty());
    assert!(
        report
            .equity_curve
            .iter()
            .all(|(_, value)| *value == dec!(1000))
    );
}

#[test]
fn trend_guard_round_trip_is_reproducible_and_cost_sensitive() {
    let prices = trending_prices();
    let paid = replay(&prices, dec!(10));
    let repeated = replay(&prices, dec!(10));
    let free = replay(&prices, Decimal::ZERO);
    assert_eq!(economic_report(&paid), economic_report(&repeated));
    assert_eq!(paid.trades.len(), 2, "{:?}", paid.trades);
    let (buy, sell) = (&paid.trades[0], &paid.trades[1]);
    assert_eq!(buy.side, Side::Buy);
    assert_eq!(sell.side, Side::Sell);
    assert_eq!(buy.quantity, sell.quantity);
    assert_eq!(buy.quantity, dec!(0.4977));
    assert_eq!(buy.price, dec!(100.46));
    assert_eq!(sell.price, dec!(101.99));
    assert!(buy.filled_at < sell.filled_at);
    for fill in &paid.trades {
        assert_eq!(fill.fee, fill.price * fill.quantity * dec!(0.001));
    }
    assert_eq!(free.trades.len(), 2);
    for (paid_fill, free_fill) in paid.trades.iter().zip(&free.trades) {
        assert_eq!(paid_fill.price, free_fill.price);
        assert_eq!(paid_fill.quantity, free_fill.quantity);
        assert_eq!(paid_fill.filled_at, free_fill.filled_at);
        assert_eq!(free_fill.fee, Decimal::ZERO);
    }
    let fees = buy.fee + sell.fee;
    let expected = dec!(1000) + (sell.price - buy.price) * buy.quantity - fees;
    assert_eq!(paid.equity_curve.last().unwrap().1, expected);
    assert_eq!(free.equity_curve.last().unwrap().1 - expected, fees);
    let accounting = &paid.spot_pnl[&buy.symbol];
    assert_eq!(
        accounting.gross_pnl,
        Some((sell.price - buy.price) * buy.quantity)
    );
    assert_eq!(accounting.fee_at_execution, Some(fees));
    assert_eq!(accounting.fee_at_mark, Some(fees));
    assert_eq!(accounting.net_pnl, Some(expected - dec!(1000)));
    assert_eq!(accounting.net_realized, accounting.net_pnl);
    assert_eq!(accounting.net_unrealized, Some(Decimal::ZERO));
}

#[test]
fn trend_guard_can_stop_out_after_a_filled_entry() {
    let mut prices = vec![dec!(100); 120];
    prices.extend((1..=9).map(|i| dec!(100) + Decimal::from(i) * dec!(0.05)));
    prices.extend([dec!(100.45); 25]);
    prices.extend([dec!(98); 40]);

    let report = replay(&prices, dec!(10));
    assert_eq!(report.trades.len(), 2, "{:?}", report.trades);
    let (buy, sell) = (&report.trades[0], &report.trades[1]);
    assert_eq!(buy.side, Side::Buy);
    assert_eq!(sell.side, Side::Sell);
    assert_eq!(buy.quantity, sell.quantity);
    assert_eq!(sell.price, dec!(97.99));
    assert!(sell.price < buy.price * dec!(0.994));
    assert!(report.equity_curve.last().unwrap().1 < dec!(1000));
}

#[test]
fn trend_guard_exits_even_when_daily_entry_budget_is_exhausted() {
    let registry = Registry::new();
    let checker = apexes_risk::CompositeRiskChecker::new().with_pre_trade_check(Box::new(
        apexes_risk::LimitChecker::new(apexes_risk::RiskLimits {
            max_daily_trade_count: Some(1),
            ..Default::default()
        }),
    ));
    registry
        .register::<Arc<apexes_risk::CompositeRiskChecker>, ()>((), Arc::new(checker))
        .unwrap();
    let report = replay_with_registry(&trending_prices(), dec!(10), &registry);
    assert_eq!(report.trades.len(), 2);
    assert_eq!(report.trades[0].side, Side::Buy);
    assert_eq!(report.trades[1].side, Side::Sell);
}

#[test]
fn trend_guard_protects_recovered_position_on_first_trade_before_warmup() {
    use apexes_strategy::{Position, Strategy, StrategyContext};
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();
    let script =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/strategy.trend_guard.drift");
    let mut strategy = DriftScriptStrategy::from_file(&script).unwrap();
    let positions = HashMap::from([(
        SYMBOL.into(),
        Position {
            symbol: SYMBOL.into(),
            quantity: dec!(0.5),
            average_entry_price: dec!(100),
            leverage: None,
            margin_mode: None,
            allocated_margin: Decimal::ZERO,
        },
    )]);
    let balances = HashMap::from([("ETH".into(), dec!(0.5))]);
    let event = fixture(&[dec!(98)]).pop().unwrap();
    let ctx = StrategyContext {
        positions: &positions,
        balances: &balances,
        open_orders: &HashMap::new(),
        open_parent_orders: &HashMap::new(),
        margin_status: &HashMap::new(),
        now: Utc::now(),
    };
    let orders = strategy.on_market_event(&event, &ctx);
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].side, Side::Sell);
    assert_eq!(orders[0].quantity, dec!(0.5));
    // A failed attempt is paced independently of the entry gap.
    assert!(strategy.on_market_event(&event, &ctx).is_empty());
}

// F00.02: capture the same JSON tracing records the engine can persist.
struct TraceWriter(Arc<Mutex<Vec<u8>>>);

impl Write for TraceWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn trace_replay(
    prices: &[Decimal],
    registry: &Registry,
) -> (BacktestReport, Vec<serde_json::Value>) {
    let output = Arc::new(Mutex::new(Vec::new()));
    let writer = Arc::clone(&output);
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(move || TraceWriter(Arc::clone(&writer)))
        .finish();
    let report = tracing::subscriber::with_default(subscriber, || {
        replay_with_registry(prices, dec!(10), registry)
    });
    let buffer = output.lock().unwrap();
    let records = std::str::from_utf8(&buffer)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .filter(|record| record["target"] == "apexes::decision")
        .map(|record| record["fields"].clone())
        .collect();
    (report, records)
}

#[test]
fn decision_trace_links_real_drift_intents_to_orders_fills_and_ledger() {
    let prices = trending_prices();
    let (report, records) = trace_replay(&prices, &Registry::new());
    assert_eq!(
        economic_report(&report),
        economic_report(&replay(&prices, dec!(10)))
    );
    let intents: Vec<_> = records.iter().filter(|r| r["stage"] == "intent").collect();
    assert_eq!(intents.len(), 2);
    assert!(
        records
            .iter()
            .any(|r| r["stage"] == "decision" && r["intent_count"] == 0)
    );
    assert_eq!(records.iter().filter(|r| r["stage"] == "fill").count(), 2);
    for (intent, fill) in intents.iter().zip(&report.trades) {
        let same_decision = |record: &&serde_json::Value| {
            record["run_id"] == intent["run_id"] && record["event_seq"] == intent["event_seq"]
        };
        assert!(
            records
                .iter()
                .filter(same_decision)
                .any(|r| r["stage"] == "decision" && r["intent_count"] == 1)
        );
        let order = records
            .iter()
            .filter(same_decision)
            .find(|r| {
                r["stage"] == "order_result" && r["intent_ordinal"] == intent["intent_ordinal"]
            })
            .unwrap();
        assert_eq!(order["order_id"], fill.order_id.0.to_string());
        assert!(records.iter().filter(same_decision).any(|r| {
            r["stage"] == "fill"
                && r["order_id"] == order["order_id"]
                && r["fill_id"] == fill.id.0.to_string()
                && r["ledger_result"] == "applied"
        }));
    }
}

#[test]
fn decision_trace_correlates_risk_refusal_without_a_submission() {
    let registry = Registry::new();
    let checker = apexes_risk::CompositeRiskChecker::new().with_pre_trade_check(Box::new(
        apexes_risk::LimitChecker::new(apexes_risk::RiskLimits {
            max_order_quantity: Some(dec!(0.1)),
            ..Default::default()
        }),
    ));
    registry
        .register::<Arc<apexes_risk::CompositeRiskChecker>, ()>((), Arc::new(checker))
        .unwrap();
    let (report, records) = trace_replay(&trending_prices(), &registry);
    assert!(report.trades.is_empty());
    let refused: Vec<_> = records
        .iter()
        .filter(|r| r["stage"] == "risk_rejected")
        .collect();
    assert!(!refused.is_empty());
    for rejection in refused {
        assert_eq!(rejection["reason"], "order_quantity_exceeded");
        assert!(records.iter().any(|r| r["stage"] == "intent"
            && r["run_id"] == rejection["run_id"]
            && r["event_seq"] == rejection["event_seq"]
            && r["intent_ordinal"] == rejection["intent_ordinal"]));
    }
    assert!(
        !records
            .iter()
            .any(|r| r["stage"] == "order_result" || r["stage"] == "fill")
    );
}
