//! Offline classification of evidence scope, never an execution allowlist.
use crate::LedgerContinuityError as E;
use apexes_backtest::ledger_evidence::LedgerEvidence;
use apexes_core::{AssetClass, Fill, Instrument, Order, Venue};
use apexes_oms::CoherentCheckpointReceipt;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EvidenceInstrumentRole {
    StrategyInventory,
    ConversionOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceInstrumentScope {
    pub venue: Venue,
    pub symbol: String,
    pub base: String,
    pub quote: String,
    pub role: EvidenceInstrumentRole,
}

/// Independently retained classification for the complete selected
/// instrument scope. A match describes only the retained boundary,
/// not permission to exclude any instrument from recovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversionScopeEvidence {
    pub version: u32,
    pub boundary: CoherentCheckpointReceipt,
    pub owner_fingerprint: String,
    /// Canonically sorted by symbol; includes every scoped instrument.
    pub instruments: Vec<EvidenceInstrumentScope>,
}

impl ConversionScopeEvidence {
    pub(crate) fn compare(
        &self,
        boundary: &CoherentCheckpointReceipt,
        owner: &str,
        captured: &LedgerEvidence,
        instruments: &[Instrument],
        journal: (&[Order], &[Fill]),
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
        let roles = self.match_instruments(instruments)?;
        let fees = captured.fee_valuation.as_ref().ok_or(E::FeeMismatch)?;
        let decisions = fees.decisions.as_ref().ok_or(E::FeeMismatch)?;
        let inventory = |venue: Venue, symbol: &str| {
            roles.get(symbol).is_some_and(|r| {
                r.venue == venue && r.role == EvidenceInstrumentRole::StrategyInventory
            })
        };
        if journal.0.len() > 2048
            || journal.1.len() > 8192
            || journal
                .0
                .iter()
                .any(|o| o.asset_class != AssetClass::Spot || !inventory(o.venue, &o.symbol))
            || journal.1.iter().any(|f| !inventory(f.venue, &f.symbol))
            || decisions.iter().any(|d| !inventory(d.venue, &d.symbol))
            || fees.accounts.iter().any(|a| {
                !inventory(a.venue, &a.symbol)
                    || roles
                        .get(a.symbol.as_str())
                        .is_none_or(|r| r.base != a.base || r.quote != a.quote)
            })
            || captured
                .positions
                .iter()
                .any(|p| !inventory(Venue::Binance, &p.symbol))
            || captured
                .realized_pnl
                .iter()
                .any(|(s, _)| !inventory(Venue::Binance, s))
        {
            return Err(E::Execution);
        }
        self.compare_price_scope(fees)?;
        Ok(())
    }

    /// Compare final retained conversion windows to the verified completed
    /// observations, without reconstructing fill-time availability.
    pub(crate) fn compare_prices(
        &self,
        captured: &LedgerEvidence,
        records: &[apexes_backtest::observation_receipt::CompletedObservation],
    ) -> Result<(), E> {
        use apexes_core::MarketEvent;
        use rust_decimal::Decimal;
        if records.len() > 8192 {
            return Err(E::Execution);
        }
        let fees = captured.fee_valuation.as_ref().ok_or(E::FeeMismatch)?;
        let roles = self
            .instruments
            .iter()
            .map(|r| (r.symbol.as_str(), r))
            .collect::<BTreeMap<_, _>>();
        let mut expected =
            BTreeMap::<(&str, &str), BTreeMap<chrono::DateTime<chrono::Utc>, Decimal>>::new();
        for record in records {
            let MarketEvent::Trade(trade) = &record.event else {
                continue;
            };
            let role = roles.get(trade.symbol.as_str()).ok_or(E::Execution)?;
            if role.venue != trade.venue {
                return Err(E::Execution);
            }
            if trade.price <= Decimal::ZERO {
                continue;
            }
            let history = expected.entry((&role.base, &role.quote)).or_default();
            history.insert(trade.timestamp, trade.price);
            // The accounting window retains the newest 2,048 source times.
            while history.len() > 2048 {
                history.pop_first();
            }
        }
        if expected.len() != fees.prices.len() {
            return Err(E::FeeMismatch);
        }
        for p in &fees.prices {
            let history = expected
                .remove(&(p.base.as_str(), p.quote.as_str()))
                .ok_or(E::FeeMismatch)?;
            if p.venue != Venue::Binance
                || p.observations
                    .iter()
                    .map(|(t, v)| (t, v))
                    .ne(history.iter())
            {
                return Err(E::FeeMismatch);
            }
        }
        Ok(())
    }

    fn match_instruments<'a>(
        &'a self,
        instruments: &[Instrument],
    ) -> Result<BTreeMap<&'a str, &'a EvidenceInstrumentScope>, E> {
        if self.instruments.len() != instruments.len()
            || self.instruments.is_empty()
            || self.instruments.len() > 128
            || !self
                .instruments
                .windows(2)
                .all(|w| w[0].symbol < w[1].symbol)
            || !self
                .instruments
                .iter()
                .any(|r| r.role == EvidenceInstrumentRole::StrategyInventory)
        {
            return Err(E::Execution);
        }
        let roles = self
            .instruments
            .iter()
            .map(|r| (r.symbol.as_str(), r))
            .collect::<BTreeMap<_, _>>();
        let mut pairs = BTreeSet::new();
        for i in instruments {
            let r = roles.get(i.symbol.as_str()).ok_or(E::Execution)?;
            if i.venue != Venue::Binance
                || i.asset_class != AssetClass::Spot
                || r.venue != i.venue
                || Some(r.base.as_str()) != i.base_currency.as_deref()
                || Some(r.quote.as_str()) != i.quote_currency.as_deref()
                || [&r.base, &r.quote].iter().any(|a| {
                    a.is_empty()
                        || a.len() > 24
                        || !a
                            .bytes()
                            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
                })
                || r.base == r.quote
                || !pairs.insert((r.base.as_str(), r.quote.as_str()))
            {
                return Err(E::Execution);
            }
        }
        Ok(roles)
    }

    fn compare_price_scope(
        &self,
        fees: &apexes_backtest::fee_evidence::FeeValuationEvidence,
    ) -> Result<(), E> {
        let pairs = self
            .instruments
            .iter()
            .map(|r| ((r.base.as_str(), r.quote.as_str()), r))
            .collect::<BTreeMap<_, _>>();
        let mut observed = BTreeSet::new();
        for p in &fees.prices {
            let key = (p.base.as_str(), p.quote.as_str());
            let r = pairs.get(&key).ok_or(E::FeeMismatch)?;
            if r.venue != p.venue || p.observations.is_empty() {
                return Err(E::FeeMismatch);
            }
            observed.insert(key);
        }
        if self.instruments.iter().any(|r| {
            r.role == EvidenceInstrumentRole::ConversionOnly
                && !observed.contains(&(r.base.as_str(), r.quote.as_str()))
        }) {
            return Err(E::FeeMismatch);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_backtest::ledger::PortfolioLedger;
    use apexes_core::{FillId, MarketType, OrderId, OrderStatus, OrderType, Side};
    use apexes_strategy::{
        Position,
        checkpoint::{CheckpointReceipt, StateCursor, StateIdentity},
    };
    use chrono::{DateTime, Utc};
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn at(t: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + t, 0).unwrap()
    }
    fn fixture() -> (
        ConversionScopeEvidence,
        LedgerEvidence,
        Vec<Instrument>,
        Order,
        Fill,
    ) {
        let instruments = ["BNB", "BTC"]
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
            [("USDT".into(), dec!(1000))].into_iter().collect(),
        );
        ledger.observe_spot_price("BNBUSDT", at(1), dec!(300));
        let order = Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: Some("test".into()),
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: dec!(1),
            price: None,
            stop_price: None,
            leverage: None,
            margin_mode: None,
            filled_quantity: dec!(1),
            status: OrderStatus::Filled,
            created_at: at(1),
            updated_at: at(2),
        };
        let fill = Fill {
            id: FillId(Uuid::new_v4()),
            order_id: order.id,
            symbol: order.symbol.clone(),
            venue: order.venue,
            side: order.side,
            quantity: dec!(1),
            price: dec!(100),
            fee: dec!(0.01),
            fee_currency: Some("BNB".into()),
            filled_at: at(2),
            leverage: None,
            margin_mode: None,
        };
        ledger.apply_fill(&fill).unwrap();
        ledger.observe_spot_price("BNBUSDT", at(3), dec!(400));
        let captured = ledger.inspect_evidence().unwrap();
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
                    ordinal: 4,
                    observed_at: at(4),
                },
                envelope_sha256: "b".repeat(64),
            },
            run_scope: Uuid::new_v4(),
            last_intent: 1,
            fill_sequence: 1,
            terminal_sequence: 1,
            order_count: 1,
            fill_count: 1,
            journal_sha256: "c".repeat(64),
            capture_sha256: Some("d".repeat(64)),
        };
        let evidence = ConversionScopeEvidence {
            version: 1,
            boundary,
            owner_fingerprint: "owner".into(),
            instruments: instruments
                .iter()
                .map(|i| EvidenceInstrumentScope {
                    venue: i.venue,
                    symbol: i.symbol.clone(),
                    base: i.base_currency.clone().unwrap(),
                    quote: i.quote_currency.clone().unwrap(),
                    role: if i.symbol == "BNBUSDT" {
                        EvidenceInstrumentRole::ConversionOnly
                    } else {
                        EvidenceInstrumentRole::StrategyInventory
                    },
                })
                .collect(),
        };
        (evidence, captured, instruments, order, fill)
    }

    fn trade(
        symbol: &str,
        source: i64,
        ordinal: u64,
        price: rust_decimal::Decimal,
    ) -> apexes_backtest::observation_receipt::CompletedObservation {
        apexes_backtest::observation_receipt::CompletedObservation {
            cursor: StateCursor {
                ordinal,
                observed_at: at(5000 + ordinal as i64),
            },
            event: apexes_core::MarketEvent::Trade(apexes_core::TradeEvent {
                symbol: symbol.into(),
                venue: Venue::Binance,
                price,
                quantity: dec!(1),
                side: None,
                trade_id: None,
                timestamp: at(source),
            }),
        }
    }

    #[test]
    fn fill_observation_prefix_distinguishes_late_replacement_and_future_source() {
        use crate::{FillObservationAvailability, FillObservationPrefix};
        let (scope, captured, _, _, fill) = fixture();
        let records = vec![
            trade("BNBUSDT", 1, 2, dec!(300)),
            trade("BNBUSDT", 3, 3, dec!(400)),
            trade("BNBUSDT", 1, 4, dec!(350)),
        ];
        let evidence = FillObservationAvailability {
            version: 1,
            boundary: scope.boundary.clone(),
            owner_fingerprint: "owner".into(),
            prefixes: vec![FillObservationPrefix {
                fill_id: fill.id,
                observations: 1,
            }],
        };
        let check = |e: &FillObservationAvailability, c: &LedgerEvidence| {
            e.compare(
                &scope.boundary,
                "owner",
                &scope,
                c,
                &records,
                std::slice::from_ref(&fill),
            )
        };
        check(&evidence, &captured).unwrap();
        let mut changed = evidence.clone();
        changed.prefixes[0].observations = 2;
        // The later source timestamp cannot value an earlier execution.
        check(&changed, &captured).unwrap();
        changed.prefixes[0].observations = 3;
        assert_eq!(check(&changed, &captured), Err(E::FeeMismatch));
        changed.prefixes[0].observations = 0;
        assert_eq!(check(&changed, &captured), Err(E::FeeMismatch));
        let mut missing = captured.clone();
        let fees = missing.fee_valuation.as_mut().unwrap();
        fees.accounts[0].historical_fee = dec!(0);
        fees.accounts[0].missing_execution_valuations = 1;
        let decision = &mut fees.decisions.as_mut().unwrap()[0];
        decision.source = apexes_backtest::fee_evidence::FeeDecisionSource::Missing;
        decision.value = None;
        check(&changed, &missing).unwrap();
        assert_eq!(check(&evidence, &missing), Err(E::FeeMismatch));
        changed.prefixes[0].observations = 4;
        assert_eq!(check(&changed, &captured), Err(E::FeeMismatch));
        changed = evidence.clone();
        changed.prefixes[0].fill_id = FillId(Uuid::new_v4());
        assert_eq!(check(&changed, &captured), Err(E::FeeMismatch));
        changed = evidence.clone();
        changed.prefixes.clear();
        assert_eq!(check(&changed, &captured), Err(E::FeeMismatch));
        changed = evidence.clone();
        changed.owner_fingerprint = "wrong".into();
        assert_eq!(check(&changed, &captured), Err(E::Identity));
        let mut supplied = captured.clone();
        supplied
            .fee_valuation
            .as_mut()
            .unwrap()
            .decisions
            .as_mut()
            .unwrap()[0]
            .source = apexes_backtest::fee_evidence::FeeDecisionSource::Provided;
        assert_eq!(check(&evidence, &supplied), Err(E::FeeMismatch));
    }

    #[test]
    fn fill_observation_prefix_rejects_regressing_order_and_duplicate_ids() {
        use crate::{FillObservationAvailability, FillObservationPrefix};
        let (scope, mut captured, _, _, fill) = fixture();
        let mut zero = fill.clone();
        zero.id = FillId(Uuid::new_v4());
        zero.fee = dec!(0);
        zero.fee_currency = None;
        let rows = captured
            .fee_valuation
            .as_mut()
            .unwrap()
            .decisions
            .as_mut()
            .unwrap();
        let mut row = rows[0].clone();
        row.fill_id = zero.id;
        row.fee = dec!(0);
        row.asset = "USDT".into();
        row.external_fee = false;
        row.source = apexes_backtest::fee_evidence::FeeDecisionSource::Zero;
        row.value = Some(dec!(0));
        rows.push(row);
        let fills = vec![fill.clone(), zero.clone()];
        let records = vec![
            trade("BNBUSDT", 1, 2, dec!(300)),
            trade("BNBUSDT", 3, 3, dec!(400)),
        ];
        let mut evidence = FillObservationAvailability {
            version: 1,
            boundary: scope.boundary.clone(),
            owner_fingerprint: "owner".into(),
            prefixes: vec![
                FillObservationPrefix {
                    fill_id: fill.id,
                    observations: 1,
                },
                FillObservationPrefix {
                    fill_id: zero.id,
                    observations: 2,
                },
            ],
        };
        let check = |e: &FillObservationAvailability, f: &[Fill]| {
            e.compare(&scope.boundary, "owner", &scope, &captured, &records, f)
        };
        check(&evidence, &fills).unwrap();
        assert_eq!(check(&evidence, &[fill.clone(), fill]), Err(E::FeeMismatch));
        evidence.prefixes[1].observations = 0;
        assert_eq!(check(&evidence, &fills), Err(E::FeeMismatch));
    }

    #[test]
    fn conversion_price_provenance_requires_exact_final_windows() {
        let (e, c, _, _, _) = fixture();
        let records = vec![
            trade("BNBUSDT", 1, 2, dec!(250)),
            trade("BNBUSDT", 3, 3, dec!(400)),
            trade("BNBUSDT", 1, 4, dec!(300)),
            trade("BNBUSDT", 2, 5, dec!(0)),
        ];
        e.compare_prices(&c, &records).unwrap();
        let mut changed = records.clone();
        changed[2] = trade("BNBUSDT", 1, 4, dec!(301));
        assert_eq!(e.compare_prices(&c, &changed), Err(E::FeeMismatch));
        let mut changed = records.clone();
        changed.push(trade("BTCUSDT", 3, 6, dec!(100)));
        assert_eq!(e.compare_prices(&c, &changed), Err(E::FeeMismatch));
        assert_eq!(e.compare_prices(&c, &records[..1]), Err(E::FeeMismatch));
        let mut changed = records.clone();
        changed[0] = trade("UNKNOWN", 1, 2, dec!(300));
        assert_eq!(e.compare_prices(&c, &changed), Err(E::Execution));
    }

    #[test]
    fn conversion_price_provenance_reproduces_bounded_source_time_eviction() {
        let (e, _, instruments, _, _) = fixture();
        let mut ledger = PortfolioLedger::new(
            instruments
                .iter()
                .map(|i| (i.symbol.clone(), i.clone()))
                .collect(),
            [("USDT".into(), dec!(1000))].into_iter().collect(),
        );
        let mut records = Vec::new();
        for t in 1..=2050 {
            ledger.observe_spot_price("BNBUSDT", at(t), dec!(300));
            records.push(trade("BNBUSDT", t, t as u64 + 1, dec!(300)));
        }
        ledger.observe_spot_price("BNBUSDT", at(1), dec!(999));
        records.push(trade("BNBUSDT", 1, 2052, dec!(999)));
        let captured = ledger.inspect_evidence().unwrap();
        let observations = &captured.fee_valuation.as_ref().unwrap().prices[0].observations;
        assert_eq!(observations.len(), 2048);
        assert_eq!(observations[0], (at(3), dec!(300)));
        e.compare_prices(&captured, &records).unwrap();
        let mut altered = captured.clone();
        altered.fee_valuation.as_mut().unwrap().prices[0]
            .observations
            .remove(0);
        assert_eq!(e.compare_prices(&altered, &records), Err(E::FeeMismatch));
    }

    #[test]
    fn qualifies_conversion_only_price_scope_without_changing_inventory_gate() {
        let (e, c, instruments, order, fill) = fixture();
        let check = |evidence: &ConversionScopeEvidence, captured: &LedgerEvidence| {
            evidence.compare(
                &e.boundary,
                "owner",
                captured,
                &instruments,
                (std::slice::from_ref(&order), std::slice::from_ref(&fill)),
            )
        };
        check(&e, &c).unwrap();
        let encoded = serde_json::to_vec(&e).unwrap();
        let decoded: ConversionScopeEvidence = serde_json::from_slice(&encoded).unwrap();
        check(&decoded, &c).unwrap();
        let mut altered = e.clone();
        altered.instruments[0].base = "ETH".into();
        assert_eq!(check(&altered, &c), Err(E::Execution));
        altered = e.clone();
        altered.instruments.swap(0, 1);
        assert_eq!(check(&altered, &c), Err(E::Execution));
        altered = e.clone();
        altered.instruments.remove(0);
        assert_eq!(check(&altered, &c), Err(E::Execution));
        altered = e.clone();
        altered.owner_fingerprint = "other".into();
        assert_eq!(check(&altered, &c), Err(E::Identity));
        altered = e.clone();
        altered.boundary.journal_sha256 = "e".repeat(64);
        assert_eq!(check(&altered, &c), Err(E::Identity));
        let mut altered = c.clone();
        altered.fee_valuation.as_mut().unwrap().prices.clear();
        assert_eq!(check(&e, &altered), Err(E::FeeMismatch));
        let mut altered = c.clone();
        altered.fee_valuation.as_mut().unwrap().prices[0].base = "ETH".into();
        assert_eq!(check(&e, &altered), Err(E::FeeMismatch));
        let wallet = [(
            "BTC".into(),
            crate::SpotWalletBalance {
                free: dec!(1),
                locked: dec!(0),
            },
        )]
        .into_iter()
        .collect();
        assert!(matches!(
            crate::recover_spot_positions(&instruments, &[fill], &wallet, false),
            Err(crate::SpotRecoveryError::InvalidFill(_))
        ));
    }

    #[test]
    fn refuses_conversion_only_execution_or_retained_inventory_footprint() {
        let (e, c, instruments, order, fill) = fixture();
        let check = |captured: &LedgerEvidence, o: &Order, f: &Fill| {
            e.compare(
                &e.boundary,
                "owner",
                captured,
                &instruments,
                (std::slice::from_ref(o), std::slice::from_ref(f)),
            )
        };
        let mut wrong_order = order.clone();
        wrong_order.symbol = "BNBUSDT".into();
        wrong_order.status = OrderStatus::Rejected;
        wrong_order.filled_quantity = dec!(0);
        assert_eq!(check(&c, &wrong_order, &fill), Err(E::Execution));
        let mut wrong_fill = fill.clone();
        wrong_fill.symbol = "BNBUSDT".into();
        assert_eq!(check(&c, &order, &wrong_fill), Err(E::Execution));
        let mut altered = c.clone();
        altered.positions.insert(
            0,
            Position {
                symbol: "BNBUSDT".into(),
                quantity: dec!(0),
                average_entry_price: dec!(0),
                leverage: None,
                margin_mode: None,
                allocated_margin: dec!(0),
            },
        );
        assert_eq!(check(&altered, &order, &fill), Err(E::Execution));
        let mut altered = c.clone();
        altered.realized_pnl.insert(0, ("BNBUSDT".into(), dec!(0)));
        assert_eq!(check(&altered, &order, &fill), Err(E::Execution));
        let mut altered = c.clone();
        altered.fee_valuation.as_mut().unwrap().decisions = None;
        assert_eq!(check(&altered, &order, &fill), Err(E::FeeMismatch));
    }
}
