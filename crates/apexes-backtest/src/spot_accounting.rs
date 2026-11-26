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
}

impl SpotAccounting {
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
    ) -> Option<Decimal> {
        if asset == quote {
            return Some(Decimal::ONE);
        }
        let history = self.prices.get(&(venue, asset.into(), quote.into()))?;
        let (time, price) = history.range(..=at).next_back()?;
        (at.signed_duration_since(*time) <= MAX_CONVERSION_AGE).then_some(*price)
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
        let fee_value = if fill.fee.is_zero() {
            Some(Decimal::ZERO)
        } else if asset == base {
            Some(fill.fee * fill.price)
        } else {
            self.historical_rate(fill.venue, asset, quote, fill.filled_at)
                .map(|rate| rate * fill.fee)
        };
        self.record_valued(fill, base, quote, external_fee, fee_value);
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
        let asset = fill.fee_currency.as_deref().unwrap_or(quote);
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
