use crate::clock::{BacktestClock, ReplaySpeed};
use crate::cost_model::CostModel;
use crate::event_loop::{EventLoopConfig, run_event_loop};
use crate::market_data_source::HistoricalMarketDataSource;
use crate::report::BacktestReport;
use apexes_core::registry::Registry;
use apexes_core::{ApexesError, Instrument};
use apexes_risk::CompositeRiskChecker;
use apexes_strategy::{MarginTierTable, Strategy, VenueScorer};
use chrono::NaiveDate;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

/// Configuration for a single backtest run (design spec §8).
pub struct BacktestConfig {
    pub instruments: Vec<Instrument>,
    /// Informational / for-caller-use only: `run_backtest` itself never
    /// reads this field -- event selection is entirely the
    /// `HistoricalMarketDataSource`'s responsibility. Kept on the config
    /// so a caller can, for example, construct a
    /// `StorageMarketDataSource` using the same range, and for future
    /// use (e.g. runner-side event filtering).
    pub date_range: Range<NaiveDate>,
    /// Required -- no implicit default (design spec §8).
    pub initial_balances: HashMap<String, Decimal>,
    pub cost_model: CostModel,
    pub speed: ReplaySpeed,
    /// 0.0 (optimistic) .. 1.0 (pessimistic), see design spec §7.3.
    pub queue_position_factor: Decimal,
    /// Risk-free return rate used for Sharpe/Sortino/Calmar calculations
    /// (design spec §8, matching `barter::statistic::metric`'s
    /// `calculate` signatures).
    pub risk_free_return: Decimal,
    /// Per-symbol tiered maintenance-margin tables driving margin
    /// monitoring and auto-liquidation (design spec §margin/leverage
    /// monitoring). Empty map (the default for every pre-existing
    /// spot-only caller) is a silent no-op: no margin status, no
    /// liquidation.
    pub margin_tiers: HashMap<String, MarginTierTable>,
}

/// Runs a full backtest: replays `market_data` in chronological order
/// through `strategy`, simulating fills via a `MatchingEngine`, tracking
/// portfolio state via a `PortfolioLedger`, and producing a
/// `BacktestReport` with overall + per-symbol + per-venue performance
/// summaries (design spec §8). Internally delegates to the shared
/// `crate::event_loop::run_event_loop` core (see that module's docs) --
/// this function's only job is to adapt `HistoricalMarketDataSource`'s
/// pre-loaded `Vec<MarketEvent>` into the `Stream` the shared core
/// expects, and to pass `stop_signal: None`/`latency_model: None` since
/// neither concept applies to historical replay.
/// `registry` (sub-project #9's registry retrofit) is used to resolve
/// an optional `CompositeRiskChecker` via
/// `registry.try_one::<Arc<CompositeRiskChecker>>()` -- absent means no
/// risk checking is performed, preserving this crate's pre-existing
/// `None`-means-no-risk-checking behavior exactly.
pub async fn run_backtest(
    config: BacktestConfig,
    registry: &Registry,
    market_data: impl HistoricalMarketDataSource,
    strategy: impl Strategy,
) -> Result<BacktestReport, ApexesError> {
    let risk_checker = registry
        .try_one::<Arc<CompositeRiskChecker>>()
        .map_err(|error| ApexesError::InvalidBacktestConfig {
            reason: format!("registry lookup for CompositeRiskChecker failed: {error}"),
        })?;
    let venue_scorer = registry
        .try_one::<Arc<dyn VenueScorer>>()
        .map_err(|error| ApexesError::InvalidBacktestConfig {
            reason: format!("registry lookup for VenueScorer failed: {error}"),
        })?
        .unwrap_or_else(|| Arc::new(apexes_strategy::PriceAndFeeScorer));
    let volume_curve_provider = registry
        .try_one::<Arc<dyn apexes_ems::VolumeCurveProvider>>()
        .map_err(|error| ApexesError::InvalidBacktestConfig {
            reason: format!("registry lookup for VolumeCurveProvider failed: {error}"),
        })?;

    let events = market_data.events().await?;
    let event_stream = futures::stream::iter(events);
    let clock = BacktestClock::new(config.speed);

    let event_loop_config = EventLoopConfig {
        instruments: config.instruments,
        initial_balances: config.initial_balances,
        cost_model: config.cost_model,
        queue_position_factor: config.queue_position_factor,
        risk_free_return: config.risk_free_return,
        risk_checker,
        margin_tiers: config.margin_tiers,
    };

    run_event_loop(
        event_loop_config,
        event_stream,
        clock,
        venue_scorer,
        volume_curve_provider,
        strategy,
        None,
        None,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::ReplaySpeed;
    use crate::market_data_source::InMemoryMarketDataSource;
    use apexes_core::{
        AssetClass, DepthUpdate, MarketType, OrderType, PriceLevel, Side, TradeEvent, Venue,
    };
    use apexes_strategy::{OrderIntent, OrderRouting, StrategyContext};
    use chrono::{Duration, TimeZone, Utc};
    use rust_decimal_macros::dec;

    struct BuyOnceStrategy {
        bought: bool,
    }

    impl Strategy for BuyOnceStrategy {
        fn on_market_event(
            &mut self,
            event: &apexes_core::MarketEvent,
            _ctx: &StrategyContext,
        ) -> Vec<OrderIntent> {
            if !self.bought && matches!(event, apexes_core::MarketEvent::Trade(_)) {
                self.bought = true;
                return vec![OrderIntent {
                    symbol: "BTCUSDT".to_string(),
                    venue: Venue::Binance,
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
                }];
            }
            Vec::new()
        }
    }

    fn btcusdt_instrument() -> Instrument {
        Instrument {
            symbol: "BTCUSDT".to_string(),
            market_type: MarketType::Crypto,
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            base_currency: Some("BTC".to_string()),
            quote_currency: Some("USDT".to_string()),
            tick_size: dec!(0.01),
            lot_size: dec!(0.00001),
            multiplier: dec!(1),
        }
    }

    fn zero_cost_model() -> CostModel {
        CostModel {
            fee_schedule: Box::new(crate::cost_model::FlatFeeSchedule {
                maker_bps: Decimal::ZERO,
                taker_bps: Decimal::ZERO,
            }),
            funding_rate_bps: None,
        }
    }

    /// Regression test for the Task 14 fix: `run_backtest` must key
    /// `last_prices` by BASE ASSET (e.g. "BTC"), not by trading-pair symbol
    /// (e.g. "BTCUSDT"), because `PortfolioLedger::equity()` looks up mark
    /// prices by asset symbol. With the old (buggy) keying, a held BTC
    /// balance silently fell back to a price of 1.0, so buying 1 BTC at
    /// 100 USDT from 10,000 USDT would report final equity of 9,901
    /// (9,900 USDT + 1 BTC * 1.0) instead of the correct 10,010
    /// (9,900 USDT + 1 BTC * 110 mark).
    #[tokio::test]
    async fn equity_curve_marks_held_base_asset_at_real_trade_price() {
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let events = vec![
            apexes_core::MarketEvent::Depth(DepthUpdate {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                bids: vec![PriceLevel {
                    price: dec!(99),
                    quantity: dec!(10),
                }],
                asks: vec![PriceLevel {
                    price: dec!(100),
                    quantity: dec!(10),
                }],
                sequence: None,
                is_snapshot: true,
                timestamp: t0,
            }),
            apexes_core::MarketEvent::Trade(TradeEvent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                price: dec!(100),
                quantity: dec!(1),
                side: None,
                trade_id: None,
                timestamp: t0 + Duration::seconds(1),
            }),
            apexes_core::MarketEvent::Trade(TradeEvent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                price: dec!(110),
                quantity: dec!(1),
                side: None,
                trade_id: None,
                timestamp: t0 + Duration::seconds(2),
            }),
        ];

        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(10000));

        let config = BacktestConfig {
            instruments: vec![btcusdt_instrument()],
            date_range: chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
                ..chrono::NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(),
            initial_balances,
            cost_model: zero_cost_model(),
            speed: ReplaySpeed::AsFastAsPossible,
            queue_position_factor: dec!(0.5),
            risk_free_return: Decimal::ZERO,
            margin_tiers: HashMap::new(),
        };

        let registry = Registry::new();
        let report = run_backtest(
            config,
            &registry,
            InMemoryMarketDataSource::new(events),
            BuyOnceStrategy { bought: false },
        )
        .await
        .unwrap();

        assert_eq!(report.trades.len(), 1);
        let final_equity = report
            .equity_curve
            .last()
            .map(|(_, equity)| *equity)
            .unwrap();
        // Correct mark-to-market: 9,900 USDT + 1 BTC * 110 = 10,010.
        assert_eq!(final_equity, dec!(10010));
        // Guard against the regression's exact wrong value: with
        // symbol-keyed mark prices the held BTC fell back to price 1.0,
        // giving 9,900 + 1 = 9,901.
        assert_ne!(final_equity, dec!(9901));
    }
}
