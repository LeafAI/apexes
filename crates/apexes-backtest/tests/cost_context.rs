//! F04.03 — Frozen modeled fee context through real Drift and replay fills.
use apexes_backtest::{
    BacktestConfig, CostModel, InMemoryMarketDataSource, ReplaySpeed, cost_model::FeeSchedule,
    run_backtest,
};
use apexes_core::{
    AssetClass, DepthUpdate, Instrument, MarketEvent, MarketType, PriceLevel, Side, Tick,
    TradeEvent, Venue, registry::Registry,
};
use apexes_strategy::{
    FeeRole, OrderIntent, Strategy, StrategyContext, drift_dsl::DriftScriptStrategy,
};
use chrono::{TimeDelta, TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

struct SingleReadSchedule {
    maker: Decimal,
    taker: Decimal,
    calls: Arc<AtomicUsize>,
}
impl FeeSchedule for SingleReadSchedule {
    fn maker_fee_bps(&self, _: Venue) -> Decimal {
        assert_eq!(self.calls.fetch_add(1, Ordering::SeqCst), 0);
        self.maker
    }
    fn taker_fee_bps(&self, _: Venue) -> Decimal {
        assert_eq!(self.calls.fetch_add(1, Ordering::SeqCst), 1);
        self.taker
    }
}

struct Probe {
    inner: DriftScriptStrategy,
    maker: Decimal,
    taker: Decimal,
}
impl Strategy for Probe {
    fn on_market_event(&mut self, event: &MarketEvent, ctx: &StrategyContext) -> Vec<OrderIntent> {
        let cost = ctx.costs.as_ref().expect("frozen cost observation");
        assert!(cost.matches(event, ctx.market.unwrap().instrument()));
        assert_eq!(cost.maker_fee_bps(), self.maker);
        assert_eq!(cost.taker_fee_bps(), self.taker);
        assert_eq!(
            cost.estimate_fee(dec!(100), FeeRole::Taker),
            Some(self.taker / dec!(100))
        );
        self.inner.on_market_event(event, ctx)
    }
}

#[test]
fn frozen_cost_context_matches_actual_maker_and_taker_fills_including_zero_fees() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();
    let at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let event_trade = |ms, price| {
        MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            price,
            quantity: dec!(2),
            side: Some(Side::Sell),
            trade_id: Some(format!("trade-{ms}")),
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
        event_trade(1, dec!(100)),
        MarketEvent::Tick(Tick {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            bid_price: dec!(99),
            bid_size: dec!(10),
            ask_price: dec!(100),
            ask_size: dec!(10),
            timestamp: at + TimeDelta::milliseconds(500),
        }),
        event_trade(1000, dec!(99)),
    ];
    for (maker, taker) in [(dec!(5), dec!(10)), (Decimal::ZERO, Decimal::ZERO)] {
        let calls = Arc::new(AtomicUsize::new(0));
        let config = BacktestConfig {
            instruments: vec![
                Instrument::new(
                    "BTCUSDT",
                    MarketType::Crypto,
                    Venue::Binance,
                    AssetClass::Spot,
                    Some("BTC".into()),
                    Some("USDT".into()),
                    dec!(0.01),
                    dec!(0.001),
                    Decimal::ONE,
                )
                .unwrap(),
            ],
            date_range: at.date_naive()..at.date_naive().succ_opt().unwrap(),
            initial_balances: HashMap::from([("USDT".into(), dec!(1000))]),
            cost_model: CostModel {
                fee_schedule: Box::new(SingleReadSchedule {
                    maker,
                    taker,
                    calls: calls.clone(),
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
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/cost_context.drift");
        let strategy = Probe {
            inner: DriftScriptStrategy::from_file(&path).unwrap(),
            maker,
            taker,
        };
        let report = futures::executor::block_on(run_backtest(
            config,
            &Registry::new(),
            InMemoryMarketDataSource::new(events.clone()),
            strategy,
        ))
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            report
                .trades
                .iter()
                .map(|f| (f.quantity, f.price, f.fee))
                .collect::<Vec<_>>(),
            vec![
                (dec!(1), dec!(100), taker / dec!(100)),
                (dec!(1), dec!(99), dec!(99) * maker / dec!(10000))
            ]
        );
        let rates = &report.simulated_fees.as_ref().unwrap().rates_by_venue["Binance"];
        assert_eq!((rates.maker_bps, rates.taker_bps), (maker, taker));
    }
}
