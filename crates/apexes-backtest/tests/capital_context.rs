//! F04.03 — Cross-symbol reservation observations through actual replay.
use apexes_backtest::{
    BacktestConfig, CostModel, EventLatencyPolicy, FlatFeeSchedule, InMemoryMarketDataSource,
    ReplaySpeed, run_backtest,
};
use apexes_core::{
    AssetClass, DepthUpdate, Instrument, MarketEvent, MarketType, PriceLevel, Side, Tick,
    TradeEvent, Venue, registry::Registry,
};
use apexes_strategy::{OrderIntent, Strategy, StrategyContext, drift_dsl::DriftScriptStrategy};
use chrono::{TimeDelta, TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;

struct Probe {
    inner: DriftScriptStrategy,
    event_time: bool,
    start: chrono::DateTime<Utc>,
}
impl Strategy for Probe {
    fn on_market_event(&mut self, event: &MarketEvent, ctx: &StrategyContext) -> Vec<OrderIntent> {
        if !self.event_time {
            assert!(ctx.capital.is_none());
        } else {
            let capital = ctx.capital.as_ref().unwrap();
            let cash = capital.get("USDT").unwrap();
            let ms = (ctx.now - self.start).num_milliseconds();
            let (balance, reserved, unreserved) = match ms {
                0 => (dec!(1000), dec!(0), dec!(1000)),
                100 | 500 => (dec!(1000), dec!(99.099), dec!(900.901)),
                600 => (dec!(900.9505), dec!(0), dec!(900.9505)),
                _ => panic!("unexpected observation {ms}"),
            };
            assert_eq!(
                (cash.ledger_balance, cash.reserved, cash.unreserved),
                (balance, reserved, unreserved)
            );
            assert!(!capital.contains_key("UNKNOWN"));
            assert!(!capital.contains_key("ETH"));
            if ms == 600 {
                assert_eq!(capital["BTC"].unreserved, Decimal::ONE);
            } else {
                assert!(!capital.contains_key("BTC"));
            }
        }
        self.inner.on_market_event(event, ctx)
    }
}

#[test]
fn capital_context_tracks_transit_resting_and_filled_holds_across_symbols() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();
    let at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let tick = |ms| {
        MarketEvent::Tick(Tick {
            symbol: "ETHUSDT".into(),
            venue: Venue::Binance,
            bid_price: dec!(49),
            bid_size: dec!(10),
            ask_price: dec!(50),
            ask_size: dec!(10),
            timestamp: at + TimeDelta::milliseconds(ms),
        })
    };
    let events = vec![
        MarketEvent::Depth(DepthUpdate {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            bids: vec![PriceLevel {
                price: dec!(99),
                quantity: dec!(10),
            }],
            asks: vec![PriceLevel {
                price: dec!(100),
                quantity: dec!(10),
            }],
            sequence: Some(1),
            source_range: None,
            is_snapshot: true,
            timestamp: at,
        }),
        tick(100),
        tick(500),
        MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            price: dec!(99),
            quantity: dec!(2),
            side: Some(Side::Sell),
            trade_id: Some("fill".into()),
            timestamp: at + TimeDelta::milliseconds(600),
        }),
    ];
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/capital_context.drift");
    for event_time in [true, false] {
        let registry = Registry::new();
        if event_time {
            registry
                .register::<_, ()>((), EventLatencyPolicy { delay_ms: 500 })
                .unwrap();
        }
        let config = BacktestConfig {
            instruments: [("BTCUSDT", "BTC"), ("ETHUSDT", "ETH")]
                .into_iter()
                .map(|(symbol, base)| {
                    Instrument::new(
                        symbol,
                        MarketType::Crypto,
                        Venue::Binance,
                        AssetClass::Spot,
                        Some(base.into()),
                        Some("USDT".into()),
                        dec!(0.01),
                        dec!(0.001),
                        Decimal::ONE,
                    )
                    .unwrap()
                })
                .collect(),
            date_range: at.date_naive()..at.date_naive().succ_opt().unwrap(),
            initial_balances: HashMap::from([("USDT".into(), dec!(1000))]),
            cost_model: CostModel {
                fee_schedule: Box::new(FlatFeeSchedule {
                    maker_bps: dec!(5),
                    taker_bps: dec!(10),
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
        let strategy = Probe {
            inner: DriftScriptStrategy::from_file(&path).unwrap(),
            event_time,
            start: at,
        };
        let report = futures::executor::block_on(run_backtest(
            config,
            &registry,
            InMemoryMarketDataSource::new(events.clone()),
            strategy,
        ))
        .unwrap();
        if event_time {
            assert_eq!(report.trades.len(), 1);
            assert_eq!(
                (report.trades[0].quantity, report.trades[0].fee),
                (Decimal::ONE, dec!(0.0495))
            );
            let evidence = report.event_latency.unwrap();
            assert_eq!(
                (
                    evidence.scheduled,
                    evidence.arrived,
                    evidence.resting_capital_reserved,
                    evidence.resting_capital_released
                ),
                (1, 1, 1, 1)
            );
        } else {
            assert!(report.trades.is_empty());
        }
    }
}
