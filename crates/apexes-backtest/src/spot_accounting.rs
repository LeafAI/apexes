//! F02.03: session execution attribution in each instrument's quote unit.
//! Gross execution economics and actual fee asset movements are separate,
//! so neither fees nor execution slippage are counted twice.
use apexes_core::{Fill, Side, Venue};
use chrono::{DateTime, TimeDelta, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

const MAX_OBSERVATIONS: usize = 2048;
const MAX_CONVERSION_AGE: TimeDelta = TimeDelta::minutes(5);

/// Session-only contribution relative to holding the starting wallet.
/// Gross inventory is a fee-free shadow of the actual executed quantities;
/// it may be negative when a session sells pre-existing holdings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpotPnlSummary {
    pub quote_currency: String,
    pub gross_realized: Decimal,
    pub gross_unrealized: Option<Decimal>,
    pub gross_pnl: Option<Decimal>,
    pub fees_by_asset: HashMap<String, Decimal>,
    /// Cost valued at each execution, not today's conversion rate.
    pub fee_at_execution: Option<Decimal>,
    /// Cost of the same fee quantities valued at the latest observed mark.
    pub fee_at_mark: Option<Decimal>,
    /// Change in the value of assets consumed by fees since execution.
    pub fee_revaluation: Option<Decimal>,
    /// Expense fees at execution (not capitalized a second time).
    pub net_realized: Option<Decimal>,
    pub net_unrealized: Option<Decimal>,
    pub net_pnl: Option<Decimal>,
    /// Fees paid outside the seeded wallet, already included in net P&L
    /// when their conversion is known; do not subtract them again.
    pub external_fees_by_asset: HashMap<String, Decimal>,
    pub missing_execution_valuations: u64,
}

/// Lifetime attribution from the bound durable Spot journal. This is
/// separate from session wallet equity; historical external funding and
/// wallet classification are not reconstructed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LifetimeSpotPnl {
    pub as_of: DateTime<Utc>,
    pub fill_count: usize,
    pub per_symbol: HashMap<String, SpotPnlSummary>,
}

#[derive(Default)]
struct SymbolAccount {
    quote: String,
    base: String,
    venue: Option<Venue>,
    gross_quantity: Decimal,
    entry: Decimal,
    gross_realized: Decimal,
    fees: HashMap<String, Decimal>,
    external_fees: HashMap<String, Decimal>,
    historical_fee: Decimal,
    missing: u64,
}

#[derive(Default)]
pub struct SpotAccounting {
    accounts: HashMap<String, SymbolAccount>,
    prices: HashMap<(Venue, String, String), BTreeMap<DateTime<Utc>, Decimal>>,
    decisions: Vec<crate::fee_evidence::FeeDecisionEvidence>,
    decisions_incomplete: bool,
}

impl SpotAccounting {
    /// Inspect actual retained attribution aggregates and price windows.
    /// Does not claim that evicted observations can be recovered.
    pub(crate) fn inspect_evidence(
        &self,
        scope: &[(Venue, String)],
    ) -> Result<
        crate::fee_evidence::FeeValuationEvidence,
        crate::observation_receipt::ObservationError,
    > {
        use crate::fee_evidence::{
            ConversionPriceEvidence, FeeAccountEvidence, FeeValuationEvidence, MAX_FEE_ASSETS,
            MAX_PRICE_ROWS, pair_key,
        };
        use crate::observation_receipt::ObservationError as E;
        if self.decisions_incomplete
            || self.accounts.len() > 128
            || self.prices.len() > 128
            || self
                .prices
                .values()
                .map(|p| p.len())
                .try_fold(0usize, usize::checked_add)
                .is_none_or(|n| n > MAX_PRICE_ROWS)
            || self
                .accounts
                .values()
                .any(|a| a.fees.len() > MAX_FEE_ASSETS || a.external_fees.len() > MAX_FEE_ASSETS)
        {
            return Err(E::Capacity);
        }
        let sorted = |m: &HashMap<String, Decimal>| {
            let mut values = m.iter().map(|(a, v)| (a.clone(), *v)).collect::<Vec<_>>();
            values.sort_by(|a, b| a.0.cmp(&b.0));
            values
        };
        let mut accounts = self
            .accounts
            .iter()
            .map(|(symbol, a)| {
                Ok(FeeAccountEvidence {
                    symbol: symbol.clone(),
                    venue: a.venue.ok_or(E::Encoding)?,
                    base: a.base.clone(),
                    quote: a.quote.clone(),
                    gross_quantity: a.gross_quantity,
                    entry: a.entry,
                    gross_realized: a.gross_realized,
                    fees: sorted(&a.fees),
                    external_fees: sorted(&a.external_fees),
                    historical_fee: a.historical_fee,
                    missing_execution_valuations: a.missing,
                })
            })
            .collect::<Result<Vec<_>, E>>()?;
        accounts.sort_by(|a, b| a.symbol.cmp(&b.symbol));
        let mut prices = self
            .prices
            .iter()
            .map(|((venue, base, quote), p)| ConversionPriceEvidence {
                venue: *venue,
                base: base.clone(),
                quote: quote.clone(),
                observations: p.iter().map(|(t, v)| (*t, *v)).collect(),
            })
            .collect::<Vec<_>>();
        prices.sort_by(|a, b| {
            pair_key(a.venue, &a.base, &a.quote).cmp(&pair_key(b.venue, &b.base, &b.quote))
        });
        let evidence = FeeValuationEvidence {
            version: 1,
            accounts,
            prices,
            decisions: Some(self.decisions.clone()),
        };
        evidence.validate(scope)?;
        Ok(evidence)
    }

    pub fn observe(
        &mut self,
        venue: Venue,
        base: &str,
        quote: &str,
        at: DateTime<Utc>,
        price: Decimal,
    ) {
        if base == quote || price <= Decimal::ZERO {
            return;
        }
        let observations = self
            .prices
            .entry((venue, base.into(), quote.into()))
            .or_default();
        observations.insert(at, price);
        while observations.len() > MAX_OBSERVATIONS {
            observations.pop_first();
        }
    }

    fn historical_rate(
        &self,
        venue: Venue,
        asset: &str,
        quote: &str,
        at: DateTime<Utc>,
    ) -> Option<(DateTime<Utc>, Decimal)> {
        let history = self.prices.get(&(venue, asset.into(), quote.into()))?;
        let (time, price) = history.range(..=at).next_back()?;
        (at.signed_duration_since(*time) <= MAX_CONVERSION_AGE).then_some((*time, *price))
    }

    fn mark(&self, venue: Venue, asset: &str, quote: &str) -> Option<Decimal> {
        if asset == quote {
            return Some(Decimal::ONE);
        }
        self.prices
            .get(&(venue, asset.into(), quote.into()))?
            .last_key_value()
            .map(|(_, value)| *value)
    }

    pub(crate) fn record(&mut self, fill: &Fill, base: &str, quote: &str, external_fee: bool) {
        let asset = fill.fee_currency.as_deref().unwrap_or(quote);
        use crate::fee_evidence::FeeDecisionSource as Source;
        let (fee_value, source) = if fill.fee.is_zero() {
            (Some(Decimal::ZERO), Source::Zero)
        } else if asset == base {
            (Some(fill.fee * fill.price), Source::Base)
        } else if asset == quote {
            (Some(fill.fee * Decimal::ONE), Source::Quote)
        } else if let Some((observed_at, rate)) =
            self.historical_rate(fill.venue, asset, quote, fill.filled_at)
        {
            (
                Some(rate * fill.fee),
                Source::Observed { observed_at, rate },
            )
        } else {
            (None, Source::Missing)
        };
        self.record_with_source(fill, base, quote, external_fee, fee_value, source);
    }

    /// Replay a fill using previously validated durable conversion evidence.
    /// No wallet balances or positions are changed by this attribution.
    pub fn record_valued(
        &mut self,
        fill: &Fill,
        base: &str,
        quote: &str,
        external_fee: bool,
        fee_value: Option<Decimal>,
    ) {
        self.record_with_source(
            fill,
            base,
            quote,
            external_fee,
            fee_value,
            crate::fee_evidence::FeeDecisionSource::Provided,
        );
    }

    fn record_with_source(
        &mut self,
        fill: &Fill,
        base: &str,
        quote: &str,
        external_fee: bool,
        fee_value: Option<Decimal>,
        source: crate::fee_evidence::FeeDecisionSource,
    ) {
        let asset = fill.fee_currency.as_deref().unwrap_or(quote);
        // Capture is bounded; overflow marks evidence unavailable, not a
        // change to normal attribution behavior or execution policy.
        if !self.decisions_incomplete {
            if self.decisions.len() >= crate::fee_evidence::MAX_DECISIONS
                || [fill.symbol.as_str(), asset]
                    .iter()
                    .any(|s| s.is_empty() || s.len() > 128 || s.chars().any(char::is_control))
            {
                self.decisions_incomplete = true;
            } else {
                self.decisions
                    .push(crate::fee_evidence::FeeDecisionEvidence {
                        fill_id: fill.id,
                        venue: fill.venue,
                        symbol: fill.symbol.clone(),
                        asset: asset.to_string(),
                        filled_at: fill.filled_at,
                        price: fill.price,
                        fee: fill.fee,
                        external_fee,
                        value: fee_value,
                        source,
                    });
            }
        }
        let account = self.accounts.entry(fill.symbol.clone()).or_default();
        account.base = base.into();
        account.quote = quote.into();
        account.venue = Some(fill.venue);
        account.record_execution(fill);
        if !fill.fee.is_zero() {
            *account.fees.entry(asset.into()).or_default() += fill.fee;
            if external_fee {
                *account.external_fees.entry(asset.into()).or_default() += fill.fee;
            }
        }
        match fee_value {
            Some(value) => account.historical_fee += value,
            None => account.missing += 1,
        }
    }

    pub fn summaries(&self) -> HashMap<String, SpotPnlSummary> {
        self.accounts
            .iter()
            .filter_map(|(symbol, account)| {
                let venue = account.venue?;
                let unrealized = if account.gross_quantity.is_zero() {
                    Some(Decimal::ZERO)
                } else {
                    self.mark(venue, &account.base, &account.quote)
                        .map(|mark| (mark - account.entry) * account.gross_quantity)
                };
                let gross = unrealized.map(|pnl| account.gross_realized + pnl);
                let historical = (account.missing == 0).then_some(account.historical_fee);
                let marked =
                    account
                        .fees
                        .iter()
                        .try_fold(Decimal::ZERO, |sum, (asset, quantity)| {
                            if quantity.is_zero() {
                                return Some(sum);
                            }
                            self.mark(venue, asset, &account.quote)
                                .map(|rate| sum + quantity * rate)
                        });
                let revaluation = marked.zip(historical).map(|(now, then)| now - then);
                Some((
                    symbol.clone(),
                    SpotPnlSummary {
                        quote_currency: account.quote.clone(),
                        gross_realized: account.gross_realized,
                        gross_unrealized: unrealized,
                        gross_pnl: gross,
                        fees_by_asset: account.fees.clone(),
                        fee_at_execution: historical,
                        fee_at_mark: marked,
                        fee_revaluation: revaluation,
                        net_realized: historical.map(|fee| account.gross_realized - fee),
                        net_unrealized: unrealized
                            .zip(revaluation)
                            .map(|(pnl, change)| pnl - change),
                        net_pnl: gross.zip(marked).map(|(pnl, fee)| pnl - fee),
                        external_fees_by_asset: account.external_fees.clone(),
                        missing_execution_valuations: account.missing,
                    },
                ))
            })
            .collect()
    }
}

impl SymbolAccount {
    fn record_execution(&mut self, fill: &Fill) {
        let quantity = match fill.side {
            Side::Buy => fill.quantity,
            Side::Sell => -fill.quantity,
        };
        let before = self.gross_quantity;
        let after = before + quantity;
        if before.is_zero() || before.is_sign_positive() == quantity.is_sign_positive() {
            if !after.is_zero() {
                self.entry =
                    (before.abs() * self.entry + quantity.abs() * fill.price) / after.abs();
            }
        } else {
            let closed = before.abs().min(quantity.abs());
            let direction = if before.is_sign_positive() {
                Decimal::ONE
            } else {
                -Decimal::ONE
            };
            self.gross_realized += (fill.price - self.entry) * closed * direction;
            if after.is_zero() || after.is_sign_positive() != before.is_sign_positive() {
                self.entry = fill.price;
            }
        }
        self.gross_quantity = after;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{FillId, OrderId};
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn fill(
        side: Side,
        qty: Decimal,
        price: Decimal,
        fee: Decimal,
        asset: &str,
        at: DateTime<Utc>,
    ) -> Fill {
        Fill {
            id: FillId(Uuid::new_v4()),
            order_id: OrderId(Uuid::new_v4()),
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            side,
            quantity: qty,
            price,
            fee,
            fee_currency: Some(asset.into()),
            filled_at: at,
            leverage: None,
            margin_mode: None,
        }
    }

    #[test]
    fn retains_original_per_fill_decisions_after_price_replacement() {
        use crate::fee_evidence::FeeDecisionSource as Source;
        let at = DateTime::from_timestamp(1_800_000_000, 0).unwrap();
        let mut accounting = SpotAccounting::default();
        let scope = vec![(Venue::Binance, "BTCUSDT".into())];
        let f = fill(Side::Buy, dec!(1), dec!(100), dec!(0.01), "BNB", at);
        accounting.observe(Venue::Binance, "BNB", "USDT", at, dec!(300));
        accounting.record(&f, "BTC", "USDT", true);
        accounting.observe(Venue::Binance, "BNB", "USDT", at, dec!(400));
        accounting.record(
            &fill(Side::Buy, dec!(1), dec!(100), dec!(0.01), "OTHER", at),
            "BTC",
            "USDT",
            true,
        );
        accounting.record(
            &fill(Side::Sell, dec!(1), dec!(110), dec!(1), "USDT", at),
            "BTC",
            "USDT",
            false,
        );
        accounting.record(
            &fill(Side::Buy, dec!(1), dec!(100), dec!(0.01), "BTC", at),
            "BTC",
            "USDT",
            false,
        );
        accounting.record(
            &fill(Side::Buy, dec!(1), dec!(100), dec!(0), "USDT", at),
            "BTC",
            "USDT",
            false,
        );
        accounting.record_valued(
            &fill(Side::Buy, dec!(1), dec!(100), dec!(0.01), "BNB", at),
            "BTC",
            "USDT",
            true,
            Some(dec!(3.5)),
        );
        let state = accounting.inspect_evidence(&scope).unwrap();
        let decisions = state.decisions.as_ref().unwrap();
        assert_eq!(decisions.len(), 6);
        assert_eq!(decisions[0].fill_id, f.id);
        assert_eq!(
            decisions[0].source,
            Source::Observed {
                observed_at: at,
                rate: dec!(300)
            }
        );
        assert_eq!(decisions[0].value, Some(dec!(3)));
        assert_eq!(state.prices[0].observations[0].1, dec!(400));
        assert_eq!(decisions[1].source, Source::Missing);
        assert_eq!(decisions[1].value, None);
        assert_eq!(decisions[2].source, Source::Quote);
        assert_eq!(decisions[3].source, Source::Base);
        assert_eq!(decisions[4].source, Source::Zero);
        assert_eq!(decisions[5].source, Source::Provided);
        assert_eq!(state.accounts[0].historical_fee, dec!(8.5));
        assert_eq!(state.accounts[0].missing_execution_valuations, 1);
        let mut invalid = state.clone();
        invalid.decisions.as_mut().unwrap()[0].value = Some(dec!(4));
        assert!(invalid.validate(&scope).is_err());
        let mut invalid = state.clone();
        invalid.decisions.as_mut().unwrap().remove(1);
        assert!(invalid.validate(&scope).is_err());
        let mut invalid = state.clone();
        let rows = invalid.decisions.as_mut().unwrap();
        rows[1].fill_id = rows[0].fill_id;
        assert!(invalid.validate(&scope).is_err());
        let mut legacy = state.clone();
        legacy.decisions = None;
        legacy.validate(&scope).unwrap();
        assert!(
            !serde_json::to_string(&legacy)
                .unwrap()
                .contains("\"decisions\"")
        );
        assert_eq!(
            state,
            serde_json::from_str(&serde_json::to_string(&state).unwrap()).unwrap()
        );
    }

    #[test]
    fn decision_capacity_refuses_evidence_without_changing_attribution() {
        let at = DateTime::from_timestamp(1_800_000_000, 0).unwrap();
        let mut accounting = SpotAccounting::default();
        for _ in 0..=crate::fee_evidence::MAX_DECISIONS {
            accounting.record(
                &fill(Side::Buy, dec!(1), dec!(100), dec!(1), "USDT", at),
                "BTC",
                "USDT",
                false,
            );
        }
        assert_eq!(
            accounting.decisions.len(),
            crate::fee_evidence::MAX_DECISIONS
        );
        assert_eq!(
            accounting.summaries()["BTCUSDT"].fee_at_execution,
            Some(Decimal::from(crate::fee_evidence::MAX_DECISIONS + 1))
        );
        assert!(
            accounting
                .inspect_evidence(&[(Venue::Binance, "BTCUSDT".into())])
                .is_err()
        );
    }

    #[test]
    fn inspect_retains_known_subtotal_missing_valuations_and_price_windows() {
        let at = DateTime::from_timestamp(1_800_000_000, 0).unwrap();
        let mut accounting = SpotAccounting::default();
        accounting.observe(Venue::Binance, "BNB", "USDT", at, dec!(300));
        accounting.record(
            &fill(Side::Buy, dec!(1), dec!(100), dec!(0.01), "BNB", at),
            "BTC",
            "USDT",
            true,
        );
        accounting.record(
            &fill(Side::Buy, dec!(1), dec!(100), dec!(0.02), "BTC", at),
            "BTC",
            "USDT",
            false,
        );
        accounting.record(
            &fill(Side::Sell, dec!(1), dec!(110), dec!(0.01), "OTHER", at),
            "BTC",
            "USDT",
            true,
        );
        accounting.observe(
            Venue::Binance,
            "BNB",
            "USDT",
            at + TimeDelta::seconds(1),
            dec!(310),
        );
        let scope = vec![(Venue::Binance, "BTCUSDT".into())];
        let state = accounting.inspect_evidence(&scope).unwrap();
        let account = &state.accounts[0];
        assert_eq!(account.gross_quantity, dec!(1));
        assert_eq!(account.entry, dec!(100));
        assert_eq!(account.gross_realized, dec!(10));
        assert_eq!(account.historical_fee, dec!(5));
        assert_eq!(account.missing_execution_valuations, 1);
        assert_eq!(
            account.fees,
            vec![
                ("BNB".into(), dec!(0.01)),
                ("BTC".into(), dec!(0.02)),
                ("OTHER".into(), dec!(0.01))
            ]
        );
        assert_eq!(
            account.external_fees,
            vec![("BNB".into(), dec!(0.01)), ("OTHER".into(), dec!(0.01))]
        );
        assert_eq!(
            state.prices[0].observations,
            vec![(at, dec!(300)), (at + TimeDelta::seconds(1), dec!(310))]
        );
        assert!(state.validate_through(at).is_err());
        state.validate_through(at + TimeDelta::seconds(1)).unwrap();
        let encoded = serde_json::to_vec(&state).unwrap();
        let decoded: crate::fee_evidence::FeeValuationEvidence =
            serde_json::from_slice(&encoded).unwrap();
        assert_eq!(state, decoded);
        assert_eq!(state, accounting.inspect_evidence(&scope).unwrap());
        let mut invalid = state.clone();
        invalid.accounts[0].external_fees[0].1 = dec!(1);
        assert!(invalid.validate(&scope).is_err());
        let mut invalid = state;
        invalid.prices[0].observations.reverse();
        assert!(invalid.validate(&scope).is_err());
    }

    #[test]
    fn base_fee_revaluation_reconciles_net_inventory_and_cash() {
        let at = Utc::now();
        let mut accounting = SpotAccounting::default();
        accounting.record(
            &fill(Side::Buy, dec!(1), dec!(100), dec!(0.01), "BTC", at),
            "BTC",
            "USDT",
            false,
        );
        accounting.record(
            &fill(Side::Sell, dec!(0.99), dec!(110), dec!(0), "USDT", at),
            "BTC",
            "USDT",
            false,
        );
        accounting.observe(Venue::Binance, "BTC", "USDT", at, dec!(110));
        let summary = &accounting.summaries()["BTCUSDT"];
        assert_eq!(summary.gross_pnl, Some(dec!(10)));
        assert_eq!(summary.fee_at_execution, Some(dec!(1)));
        assert_eq!(summary.fee_at_mark, Some(dec!(1.1)));
        assert_eq!(summary.net_realized, Some(dec!(8.9)));
        assert_eq!(summary.net_unrealized, Some(dec!(0)));
        assert_eq!(summary.net_pnl, Some(dec!(8.9)));
    }

    #[test]
    fn quote_and_third_asset_fees_use_causal_rates_and_are_not_double_counted() {
        let at = Utc::now();
        let mut accounting = SpotAccounting::default();
        accounting.observe(
            Venue::Binance,
            "BNB",
            "USDT",
            at - TimeDelta::seconds(1),
            dec!(300),
        );
        accounting.observe(
            Venue::Binance,
            "BNB",
            "USDT",
            at + TimeDelta::seconds(1),
            dec!(400),
        );
        accounting.record(
            &fill(Side::Buy, dec!(1), dec!(100), dec!(0.01), "BNB", at),
            "BTC",
            "USDT",
            true,
        );
        accounting.record(
            &fill(Side::Sell, dec!(1), dec!(110), dec!(1), "USDT", at),
            "BTC",
            "USDT",
            false,
        );
        let summary = &accounting.summaries()["BTCUSDT"];
        assert_eq!(summary.gross_pnl, Some(dec!(10)));
        assert_eq!(summary.fee_at_execution, Some(dec!(4)));
        assert_eq!(summary.fee_at_mark, Some(dec!(5)));
        assert_eq!(summary.net_realized, Some(dec!(6)));
        assert_eq!(summary.net_unrealized, Some(dec!(-1)));
        assert_eq!(summary.net_pnl, Some(dec!(5)));
        assert_eq!(summary.external_fees_by_asset["BNB"], dec!(0.01));
    }

    #[test]
    fn future_stale_and_wrong_quote_rates_do_not_price_historical_fees() {
        let at = Utc::now();
        let mut accounting = SpotAccounting::default();
        accounting.observe(
            Venue::Binance,
            "BNB",
            "USDT",
            at - TimeDelta::minutes(6),
            dec!(200),
        );
        accounting.observe(
            Venue::Binance,
            "BNB",
            "USDT",
            at + TimeDelta::seconds(1),
            dec!(400),
        );
        accounting.observe(Venue::Binance, "BNB", "BTC", at, dec!(0.01));
        accounting.record(
            &fill(Side::Buy, dec!(1), dec!(100), dec!(0.01), "BNB", at),
            "BTC",
            "USDT",
            true,
        );
        let summary = &accounting.summaries()["BTCUSDT"];
        assert_eq!(summary.fee_at_execution, None);
        assert_eq!(summary.net_realized, None);
        assert_eq!(summary.missing_execution_valuations, 1);
        assert_eq!(summary.fees_by_asset["BNB"], dec!(0.01));
        assert_eq!(summary.net_pnl, None); // BTC itself has no mark yet
    }
}
