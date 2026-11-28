//! Offline closing fee mark comparison, not a price feed or restore format.
use crate::recovery_ledger::{LedgerContinuityError as E, exact_add, exact_mul};
use apexes_backtest::ledger_evidence::LedgerEvidence;
use apexes_core::{AssetClass, Instrument, Venue};
use apexes_oms::CoherentCheckpointReceipt;
use chrono::{DateTime, TimeDelta, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClosingFeeMarkRate {
    pub venue: Venue,
    pub asset: String,
    pub quote: String,
    pub observed_at: DateTime<Utc>,
    pub rate: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClosingFeeRevaluation {
    pub symbol: String,
    pub fee_at_execution: Decimal,
    pub fee_at_mark: Decimal,
    pub fee_revaluation: Decimal,
}

/// Independent closing mark observations and valuation totals, bound to
/// the selected coherent market-observation boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClosingFeeMarkEvidence {
    pub version: u32,
    pub boundary: CoherentCheckpointReceipt,
    pub owner_fingerprint: String,
    /// Sorted by (asset, quote), exactly the needed non-quote fee pairs.
    pub rates: Vec<ClosingFeeMarkRate>,
    /// Sorted by symbol, including traded symbols without fees.
    pub symbols: Vec<ClosingFeeRevaluation>,
}

impl ClosingFeeMarkEvidence {
    pub(crate) fn compare(
        &self,
        boundary: &CoherentCheckpointReceipt,
        owner: &str,
        captured: &LedgerEvidence,
        instruments: &[Instrument],
    ) -> Result<(), E> {
        if self.version != 1 || &self.boundary != boundary || self.owner_fingerprint != owner {
            return Err(E::Identity);
        }
        let scope = instruments
            .iter()
            .map(|i| (i.venue, i.symbol.clone()))
            .collect::<Vec<_>>();
        captured
            .validate(&scope)
            .map_err(|_| E::CapturedLedgerMismatch)?;
        if instruments
            .iter()
            .any(|i| i.venue != Venue::Binance || i.asset_class != AssetClass::Spot)
        {
            return Err(E::Execution);
        }
        let attribution = captured.fee_valuation.as_ref().ok_or(E::FeeMismatch)?;
        let through = boundary.checkpoint.cursor.observed_at;
        let rates = self.check_rates(through, attribution)?;
        let mut used = BTreeSet::new();
        if self.symbols.len() != attribution.accounts.len() || self.symbols.len() > 128 {
            return Err(E::FeeMismatch);
        }
        for (expected, account) in self.symbols.iter().zip(&attribution.accounts) {
            if expected.symbol != account.symbol || account.missing_execution_valuations != 0 {
                return Err(E::FeeMismatch);
            }
            let mut marked = Decimal::ZERO;
            for (asset, quantity) in &account.fees {
                if quantity.is_zero() {
                    continue;
                }
                let rate = if asset == &account.quote {
                    Decimal::ONE
                } else {
                    let key = (asset.as_str(), account.quote.as_str());
                    used.insert(key);
                    *rates.get(&key).ok_or(E::FeeMismatch)?
                };
                marked = exact_add(marked, exact_mul(*quantity, rate)?)?;
            }
            if expected.fee_at_execution != account.historical_fee
                || expected.fee_at_mark != marked
                || expected.fee_revaluation != exact_add(marked, -account.historical_fee)?
            {
                return Err(E::FeeMismatch);
            }
        }
        if used.len() != rates.len() {
            return Err(E::FeeMismatch);
        }
        Ok(())
    }

    fn check_rates(
        &self,
        through: DateTime<Utc>,
        attribution: &apexes_backtest::fee_evidence::FeeValuationEvidence,
    ) -> Result<BTreeMap<(&str, &str), Decimal>, E> {
        if self.rates.len() > 128
            || !self
                .rates
                .windows(2)
                .all(|w| (&w[0].asset, &w[0].quote) < (&w[1].asset, &w[1].quote))
        {
            return Err(E::FeeMismatch);
        }
        let mut rates = BTreeMap::new();
        for r in &self.rates {
            if r.venue != Venue::Binance
                || r.asset == r.quote
                || r.rate <= Decimal::ZERO
                || r.observed_at > through
                || through.signed_duration_since(r.observed_at) > TimeDelta::minutes(5)
            {
                return Err(E::FeeMismatch);
            }
            let latest = attribution
                .prices
                .iter()
                .find(|p| p.venue == r.venue && p.base == r.asset && p.quote == r.quote)
                .and_then(|p| p.observations.last())
                .ok_or(E::FeeMismatch)?;
            if *latest != (r.observed_at, r.rate) {
                return Err(E::FeeMismatch);
            }
            rates.insert((r.asset.as_str(), r.quote.as_str()), r.rate);
        }
        Ok(rates)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_backtest::ledger::PortfolioLedger;
    use apexes_core::{Fill, FillId, MarketType, OrderId, Side};
    use apexes_strategy::checkpoint::{CheckpointReceipt, StateCursor, StateIdentity};
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn at(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + seconds, 0).unwrap()
    }

    fn fixture() -> (ClosingFeeMarkEvidence, LedgerEvidence, Vec<Instrument>) {
        let instruments = ["BNB", "BTC", "ETH"]
            .into_iter()
            .map(|base| {
                Instrument::new(
                    format!("{base}USDT"),
                    MarketType::Crypto,
                    Venue::Binance,
                    AssetClass::Spot,
                    Some(base.into()),
                    Some("USDT".into()),
                    dec!(0.01),
                    dec!(0.001),
                    dec!(1),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let mut ledger = PortfolioLedger::new(
            instruments
                .iter()
                .map(|i| (i.symbol.clone(), i.clone()))
                .collect(),
            [("USDT".into(), dec!(1000)), ("BNB".into(), dec!(1))]
                .into_iter()
                .collect(),
        );
        ledger.observe_spot_price("BNBUSDT", at(1), dec!(300));
        for (asset, fee, t) in [
            ("BTC", dec!(0.01), 2),
            ("USDT", dec!(1), 3),
            ("BNB", dec!(0.01), 4),
        ] {
            ledger
                .apply_fill(&Fill {
                    id: FillId(Uuid::new_v4()),
                    order_id: OrderId(Uuid::new_v4()),
                    symbol: "BTCUSDT".into(),
                    venue: Venue::Binance,
                    side: Side::Buy,
                    price: dec!(100),
                    quantity: dec!(1),
                    fee,
                    fee_currency: Some(asset.into()),
                    filled_at: at(t),
                    leverage: None,
                    margin_mode: None,
                })
                .unwrap();
        }
        ledger.observe_spot_price("BTCUSDT", at(13), dec!(150));
        ledger.observe_spot_price("BNBUSDT", at(14), dec!(400));
        ledger.observe_spot_price("ETHUSDT", at(14), dec!(2000));
        let report = ledger.spot_pnl();
        assert_eq!(report["BTCUSDT"].fee_at_execution, Some(dec!(5)));
        assert_eq!(report["BTCUSDT"].fee_at_mark, Some(dec!(6.5)));
        assert_eq!(report["BTCUSDT"].fee_revaluation, Some(dec!(1.5)));
        let boundary = CoherentCheckpointReceipt {
            version: 1,
            checkpoint: CheckpointReceipt {
                identity: StateIdentity {
                    strategy: "test".into(),
                    revision: "1".into(),
                    config_sha256: "a".repeat(64),
                    scope: "scope".into(),
                    payload_version: 1,
                },
                cursor: StateCursor {
                    ordinal: 5,
                    observed_at: at(15),
                },
                envelope_sha256: "b".repeat(64),
            },
            run_scope: Uuid::new_v4(),
            last_intent: 3,
            fill_sequence: 3,
            terminal_sequence: 3,
            order_count: 3,
            fill_count: 3,
            journal_sha256: "c".repeat(64),
            capture_sha256: Some("d".repeat(64)),
        };
        let evidence = ClosingFeeMarkEvidence {
            version: 1,
            boundary,
            owner_fingerprint: "owner".into(),
            rates: vec![
                ClosingFeeMarkRate {
                    venue: Venue::Binance,
                    asset: "BNB".into(),
                    quote: "USDT".into(),
                    observed_at: at(14),
                    rate: dec!(400),
                },
                ClosingFeeMarkRate {
                    venue: Venue::Binance,
                    asset: "BTC".into(),
                    quote: "USDT".into(),
                    observed_at: at(13),
                    rate: dec!(150),
                },
            ],
            symbols: vec![ClosingFeeRevaluation {
                symbol: "BTCUSDT".into(),
                fee_at_execution: dec!(5),
                fee_at_mark: dec!(6.5),
                fee_revaluation: dec!(1.5),
            }],
        };
        (evidence, ledger.inspect_evidence().unwrap(), instruments)
    }

    #[test]
    fn mixed_asset_mark_evidence_matches_actual_retained_conversion_windows() {
        let (evidence, captured, instruments) = fixture();
        let check = |e: &ClosingFeeMarkEvidence| {
            e.compare(&evidence.boundary, "owner", &captured, &instruments)
        };
        check(&evidence).unwrap();
        let mut changed = evidence.clone();
        changed.rates[0].rate = dec!(300);
        changed.rates[0].observed_at = at(1);
        assert_eq!(check(&changed), Err(E::FeeMismatch));
        changed = evidence.clone();
        changed.rates[0].rate = dec!(450);
        changed.rates[1].rate = dec!(100); // preserves aggregate fee-at-mark
        assert_eq!(check(&changed), Err(E::FeeMismatch));
        changed = evidence.clone();
        changed.rates.pop();
        assert_eq!(check(&changed), Err(E::FeeMismatch));
        changed = evidence.clone();
        changed.rates.push(ClosingFeeMarkRate {
            venue: Venue::Binance,
            asset: "ETH".into(),
            quote: "USDT".into(),
            observed_at: at(14),
            rate: dec!(2000),
        });
        assert_eq!(check(&changed), Err(E::FeeMismatch));
        changed = evidence.clone();
        changed.rates.push(changed.rates[1].clone());
        assert_eq!(check(&changed), Err(E::FeeMismatch));
        changed = evidence.clone();
        changed.symbols[0].fee_revaluation = dec!(2);
        assert_eq!(check(&changed), Err(E::FeeMismatch));
        changed = evidence.clone();
        changed.owner_fingerprint = "wrong".into();
        assert_eq!(check(&changed), Err(E::Identity));
        changed = evidence.clone();
        changed.boundary.journal_sha256 = "e".repeat(64);
        assert_eq!(check(&changed), Err(E::Identity));
    }

    #[test]
    fn mark_evidence_refuses_stale_future_and_missing_historical_valuation() {
        let (mut evidence, mut captured, instruments) = fixture();
        evidence.boundary.checkpoint.cursor.observed_at = at(314);
        assert_eq!(
            evidence.compare(&evidence.boundary, "owner", &captured, &instruments),
            Err(E::FeeMismatch)
        );
        evidence.boundary.checkpoint.cursor.observed_at = at(13);
        assert_eq!(
            evidence.compare(&evidence.boundary, "owner", &captured, &instruments),
            Err(E::FeeMismatch)
        );
        evidence.boundary.checkpoint.cursor.observed_at = at(15);
        let fees = captured.fee_valuation.as_mut().unwrap();
        fees.accounts[0].historical_fee = dec!(2);
        fees.accounts[0].missing_execution_valuations = 1;
        let row = &mut fees.decisions.as_mut().unwrap()[2];
        row.value = None;
        row.source = apexes_backtest::fee_evidence::FeeDecisionSource::Missing;
        assert_eq!(
            evidence.compare(&evidence.boundary, "owner", &captured, &instruments),
            Err(E::FeeMismatch)
        );
    }
}
