use apexes_core::Venue;
use apexes_core::{ApexesError, AssetClass, Instrument};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Whether a fill was the resting (maker) or aggressing (taker) side of a
/// trade — determines which fee rate applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillLiquidity {
    Maker,
    Taker,
}

/// Provides maker/taker fee rates (in basis points) for a given `Venue`.
pub trait FeeSchedule: Send + Sync {
    fn maker_fee_bps(&self, venue: Venue) -> Decimal;
    fn taker_fee_bps(&self, venue: Venue) -> Decimal;
    /// Only a session-frozen schedule may expose a deterministic quote.
    fn frozen_rates(&self, _venue: Venue) -> Option<FeeRates> {
        None
    }
}

/// Binance Spot's real VIP tier fee table (VIP0/Regular..VIP9), standard
/// (non-BNB-discounted) rates, confirmed by the user directly from
/// binance.com/en/fee/schedule on 2026-07-28. Only applies its rates when
/// queried for `Venue::Binance`; any other venue falls back to VIP0/Regular
/// rates (0.10%/0.10%) as a conservative default, logged at `tracing::warn`
/// level since this fee schedule is not meant to be used for non-Binance
/// venues (use `FlatFeeSchedule` for those instead).
pub struct BinanceSpotVipFeeSchedule {
    pub tier: u8, // 0..=9
}

impl BinanceSpotVipFeeSchedule {
    /// (maker_bps, taker_bps) for each VIP tier, standard spot rates.
    const TIERS: [(Decimal, Decimal); 10] = [
        (dec!(10.0), dec!(10.0)), // 0 (Regular)
        (dec!(9.0), dec!(10.0)),  // 1
        (dec!(8.0), dec!(10.0)),  // 2
        (dec!(4.0), dec!(6.0)),   // 3
        (dec!(4.0), dec!(5.2)),   // 4
        (dec!(2.5), dec!(3.1)),   // 5
        (dec!(2.0), dec!(2.9)),   // 6
        (dec!(1.9), dec!(2.8)),   // 7
        (dec!(1.6), dec!(2.5)),   // 8
        (dec!(1.1), dec!(2.3)),   // 9
    ];

    fn tier_rates(&self) -> (Decimal, Decimal) {
        Self::TIERS
            .get(self.tier as usize)
            .copied()
            .unwrap_or(Self::TIERS[0])
    }

    /// Rates for `venue`: the configured tier's rates for Binance, or the
    /// VIP0/Regular rates (with a warning) for any other venue.
    fn rates_for_venue(&self, venue: Venue) -> (Decimal, Decimal) {
        if venue != Venue::Binance {
            tracing::warn!(
                ?venue,
                "BinanceSpotVipFeeSchedule queried for a non-Binance venue; using VIP0/Regular fallback rate"
            );
            return Self::TIERS[0];
        }
        self.tier_rates()
    }
}

impl FeeSchedule for BinanceSpotVipFeeSchedule {
    fn maker_fee_bps(&self, venue: Venue) -> Decimal {
        self.rates_for_venue(venue).0
    }

    fn taker_fee_bps(&self, venue: Venue) -> Decimal {
        self.rates_for_venue(venue).1
    }
}

/// A simple flat maker/taker rate applied uniformly to every venue.
/// Intended as the fallback `FeeSchedule` for venues without a dedicated
/// tiered-fee implementation yet (see design spec §7.5).
pub struct FlatFeeSchedule {
    pub maker_bps: Decimal,
    pub taker_bps: Decimal,
}

impl FeeSchedule for FlatFeeSchedule {
    fn maker_fee_bps(&self, _venue: Venue) -> Decimal {
        self.maker_bps
    }

    fn taker_fee_bps(&self, _venue: Venue) -> Decimal {
        self.taker_bps
    }
}

/// Launch-time simulated fee policy. Legacy launch JSON defaults to
/// explicit zero fees; live fills never use this schedule.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SimulatedFees {
    #[default]
    Zero,
    Flat {
        maker_bps: Decimal,
        taker_bps: Decimal,
    },
    BinanceSpotVip {
        tier: u8,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeeRates {
    pub maker_bps: Decimal,
    pub taker_bps: Decimal,
}

impl FeeRates {
    fn valid(&self) -> bool {
        (Decimal::ZERO..dec!(10000)).contains(&self.maker_bps)
            && (Decimal::ZERO..dec!(10000)).contains(&self.taker_bps)
    }
}

/// Frozen per-venue assumptions, rather than inferred from fills (a
/// run with no fills may still have nonzero configured fees).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimulatedFeeReport {
    pub rates_by_venue: BTreeMap<String, FeeRates>,
}

impl SimulatedFeeReport {
    pub fn label(&self) -> &'static str {
        if self.rates_by_venue.is_empty() {
            "Fees unknown"
        } else if self
            .rates_by_venue
            .values()
            .all(|r| r.maker_bps.is_zero() && r.taker_bps.is_zero())
        {
            "ZERO FEES"
        } else {
            "Modeled fees"
        }
    }
}

impl SimulatedFees {
    pub fn validate(&self, instruments: &[Instrument]) -> Result<(), String> {
        match self {
            Self::Flat { maker_bps, taker_bps } if !(FeeRates { maker_bps:*maker_bps,taker_bps:*taker_bps }).valid() =>
                Err("maker/taker fees must be in [0, 10000) bps".into()),
            Self::BinanceSpotVip { tier } if *tier > 9 || instruments.iter().any(|i| i.venue != Venue::Binance || i.asset_class != AssetClass::Spot) =>
                Err("Binance Spot VIP fee policy requires tier 0–9 and only Binance Spot instruments".into()),
            _ => Ok(()),
        }
    }

    pub fn cost_model(&self) -> CostModel {
        let fee_schedule: Box<dyn FeeSchedule> = match *self {
            Self::Zero => Box::new(FlatFeeSchedule {
                maker_bps: Decimal::ZERO,
                taker_bps: Decimal::ZERO,
            }),
            Self::Flat {
                maker_bps,
                taker_bps,
            } => Box::new(FlatFeeSchedule {
                maker_bps,
                taker_bps,
            }),
            Self::BinanceSpotVip { tier } => Box::new(BinanceSpotVipFeeSchedule { tier }),
        };
        CostModel {
            fee_schedule,
            funding_rate_bps: None,
        }
    }
}

struct FrozenFeeSchedule(BTreeMap<String, FeeRates>);
impl FeeSchedule for FrozenFeeSchedule {
    fn maker_fee_bps(&self, venue: Venue) -> Decimal {
        self.0
            .get(&format!("{venue:?}"))
            .map_or(Decimal::ZERO, |r| r.maker_bps)
    }
    fn taker_fee_bps(&self, venue: Venue) -> Decimal {
        self.0
            .get(&format!("{venue:?}"))
            .map_or(Decimal::ZERO, |r| r.taker_bps)
    }
}

struct FrozenWithFallback {
    frozen: FrozenFeeSchedule,
    fallback: Box<dyn FeeSchedule>,
}
impl FeeSchedule for FrozenWithFallback {
    fn frozen_rates(&self, venue: Venue) -> Option<FeeRates> {
        self.frozen.0.get(&format!("{venue:?}")).cloned()
    }

    fn maker_fee_bps(&self, venue: Venue) -> Decimal {
        if self.frozen.0.contains_key(&format!("{venue:?}")) {
            self.frozen.maker_fee_bps(venue)
        } else {
            self.fallback.maker_fee_bps(venue)
        }
    }
    fn taker_fee_bps(&self, venue: Venue) -> Decimal {
        if self.frozen.0.contains_key(&format!("{venue:?}")) {
            self.frozen.taker_fee_bps(venue)
        } else {
            self.fallback.taker_fee_bps(venue)
        }
    }
}

/// The backtest's cost model: fees per fill, plus a funding-rate field
/// intended for open perpetual positions (NOT YET WIRED UP -- see
/// `funding_rate_bps`). Market-impact/slippage is produced naturally by
/// the matching engine's book-walk fill logic (Tasks 8-11), not modeled
/// here.
pub struct CostModel {
    pub fee_schedule: Box<dyn FeeSchedule>,
    /// Funding rate (in basis points) for perpetual positions; `None`
    /// disables funding entirely (e.g. for spot-only backtests).
    ///
    /// NOT YET WIRED UP: `funding_payment` is currently dead code; no
    /// periodic funding pass exists in `run_backtest` or `MatchingEngine`.
    /// Set `funding_rate_bps: None` for all current (spot-only)
    /// backtests. Implementing periodic funding application to open
    /// `AssetClass::Perpetual` positions is deferred to a future
    /// iteration (see final-review.md finding I2).
    pub funding_rate_bps: Option<Decimal>,
}

impl CostModel {
    /// Freeze rates for configured venues so report assumptions match
    /// the matching engine, even for custom schedules. Unconfigured
    /// venues retain the original policy (see simulation routing).
    pub fn freeze_fees(
        &mut self,
        instruments: &[Instrument],
    ) -> Result<SimulatedFeeReport, ApexesError> {
        let mut rates_by_venue = BTreeMap::new();
        for instrument in instruments {
            let venue = instrument.venue;
            let key = format!("{venue:?}");
            if rates_by_venue.contains_key(&key) {
                continue;
            }
            let rates = FeeRates {
                maker_bps: self.fee_schedule.maker_fee_bps(venue),
                taker_bps: self.fee_schedule.taker_fee_bps(venue),
            };
            if !rates.valid() {
                return Err(ApexesError::InvalidBacktestConfig {
                    reason: format!("invalid fee rates for {key}"),
                });
            }
            rates_by_venue.insert(key, rates);
        }
        // Only configured venues are modeled; retain the original
        // policy for legacy SOR routes outside the instrument set.
        let report = SimulatedFeeReport { rates_by_venue };
        self.fee_schedule = Box::new(FrozenWithFallback {
            frozen: FrozenFeeSchedule(report.rates_by_venue.clone()),
            fallback: std::mem::replace(
                &mut self.fee_schedule,
                Box::new(FlatFeeSchedule {
                    maker_bps: Decimal::ZERO,
                    taker_bps: Decimal::ZERO,
                }),
            ),
        });
        Ok(report)
    }

    /// Computes the fee owed for a fill of `notional` value (price *
    /// quantity, already computed by the caller) at `venue`, given whether
    /// it was a maker or taker fill.
    pub fn fee_for_fill(
        &self,
        venue: Venue,
        liquidity: FillLiquidity,
        notional: Decimal,
    ) -> Decimal {
        let bps = match liquidity {
            FillLiquidity::Maker => self.fee_schedule.maker_fee_bps(venue),
            FillLiquidity::Taker => self.fee_schedule.taker_fee_bps(venue),
        };
        notional * bps / dec!(10000)
    }

    /// Computes the funding payment owed (positive = paid by the position
    /// holder, negative = received) for a perpetual position with the
    /// given notional value, or `None` if funding is disabled
    /// (`funding_rate_bps: None`).
    pub fn funding_payment(&self, position_notional: Decimal) -> Option<Decimal> {
        self.funding_rate_bps
            .map(|bps| position_notional * bps / dec!(10000))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binance_vip0_rates_match_confirmed_table() {
        let schedule = BinanceSpotVipFeeSchedule { tier: 0 };
        assert_eq!(schedule.maker_fee_bps(Venue::Binance), dec!(10.0));
        assert_eq!(schedule.taker_fee_bps(Venue::Binance), dec!(10.0));
    }

    #[test]
    fn binance_vip9_rates_match_confirmed_table() {
        let schedule = BinanceSpotVipFeeSchedule { tier: 9 };
        assert_eq!(schedule.maker_fee_bps(Venue::Binance), dec!(1.1));
        assert_eq!(schedule.taker_fee_bps(Venue::Binance), dec!(2.3));
    }

    #[test]
    fn binance_vip4_rates_match_confirmed_table() {
        let schedule = BinanceSpotVipFeeSchedule { tier: 4 };
        assert_eq!(schedule.maker_fee_bps(Venue::Binance), dec!(4.0));
        assert_eq!(schedule.taker_fee_bps(Venue::Binance), dec!(5.2));
    }

    #[test]
    fn binance_schedule_falls_back_to_tier0_for_out_of_range_tier() {
        let schedule = BinanceSpotVipFeeSchedule { tier: 200 };
        assert_eq!(schedule.maker_fee_bps(Venue::Binance), dec!(10.0));
    }

    #[test]
    fn binance_schedule_falls_back_for_non_binance_venue() {
        let schedule = BinanceSpotVipFeeSchedule { tier: 9 };
        // Non-Binance venue: falls back to VIP0/Regular rate, not VIP9.
        assert_eq!(schedule.maker_fee_bps(Venue::Okx), dec!(10.0));
    }

    #[test]
    fn flat_fee_schedule_applies_uniformly() {
        let schedule = FlatFeeSchedule {
            maker_bps: dec!(5),
            taker_bps: dec!(7),
        };
        assert_eq!(schedule.maker_fee_bps(Venue::Binance), dec!(5));
        assert_eq!(schedule.taker_fee_bps(Venue::Nasdaq), dec!(7));
    }

    #[test]
    fn fee_for_fill_computes_correct_amount() {
        let cost_model = CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: dec!(10), // 0.10%
                taker_bps: dec!(20), // 0.20%
            }),
            funding_rate_bps: None,
        };

        // 10,000 notional * 0.10% = 10
        let maker_fee = cost_model.fee_for_fill(Venue::Binance, FillLiquidity::Maker, dec!(10000));
        assert_eq!(maker_fee, dec!(10));

        // 10,000 notional * 0.20% = 20
        let taker_fee = cost_model.fee_for_fill(Venue::Binance, FillLiquidity::Taker, dec!(10000));
        assert_eq!(taker_fee, dec!(20));
    }

    #[test]
    fn funding_payment_returns_none_when_disabled() {
        let cost_model = CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: dec!(0),
                taker_bps: dec!(0),
            }),
            funding_rate_bps: None,
        };
        assert_eq!(cost_model.funding_payment(dec!(10000)), None);
    }

    #[test]
    fn funding_payment_computes_correct_amount_when_enabled() {
        let cost_model = CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: dec!(0),
                taker_bps: dec!(0),
            }),
            funding_rate_bps: Some(dec!(1)), // 0.01%
        };
        // 100,000 notional * 0.01% = 10
        assert_eq!(cost_model.funding_payment(dec!(100000)), Some(dec!(10)));
    }
}

#[cfg(test)]
mod frozen_context_tests {
    use super::*;
    use apexes_core::MarketType;

    #[test]
    fn frozen_context_has_no_unconfigured_venue_or_unfrozen_fallback_quote() {
        let mut model = SimulatedFees::Flat {
            maker_bps: dec!(5),
            taker_bps: dec!(10),
        }
        .cost_model();
        assert!(model.fee_schedule.frozen_rates(Venue::Binance).is_none());
        let instrument = Instrument::new(
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
        .unwrap();
        let report = model.freeze_fees(&[instrument]).unwrap();
        assert_eq!(
            model.fee_schedule.frozen_rates(Venue::Binance),
            report.rates_by_venue.get("Binance").cloned()
        );
        assert!(model.fee_schedule.frozen_rates(Venue::Okx).is_none());
        assert_eq!(
            model.fee_for_fill(Venue::Okx, FillLiquidity::Taker, dec!(100)),
            dec!(0.1)
        );
    }
}
