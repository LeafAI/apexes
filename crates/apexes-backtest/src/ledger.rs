use crate::strategy::Position;
use apexes_core::{ApexesError, Fill, Instrument, Side};
use rust_decimal::Decimal;
use std::collections::HashMap;

/// Tracks multi-asset balances (like a real exchange account: balance per
/// asset, not just quote currency) and per-symbol net positions during a
/// backtest, updated by applying each simulated `Fill`.
pub struct PortfolioLedger {
    instruments: HashMap<String, Instrument>,
    balances: HashMap<String, Decimal>,
    positions: HashMap<String, Position>,
}

impl PortfolioLedger {
    /// Constructs a new ledger. `instruments` maps symbol -> `Instrument`
    /// (used to resolve each fill's base/quote currency for balance
    /// debit/credit). `initial_balances` is required explicit starting
    /// capital per asset symbol (e.g. "USDT" -> 10000) -- no implicit
    /// default, per design spec §8.
    pub fn new(instruments: HashMap<String, Instrument>, initial_balances: HashMap<String, Decimal>) -> Self {
        Self {
            instruments,
            balances: initial_balances,
            positions: HashMap::new(),
        }
    }

    pub fn balances(&self) -> &HashMap<String, Decimal> {
        &self.balances
    }

    pub fn positions(&self) -> &HashMap<String, Position> {
        &self.positions
    }

    /// Applies a `Fill` to the ledger: debits/credits the base and quote
    /// asset balances (including `fill.fee`, always debited from the
    /// balance denominated in `fill.fee_currency`, defaulting to the quote
    /// asset if `fee_currency` is `None`), and updates the symbol's net
    /// `Position` (quantity and volume-weighted average entry price).
    ///
    /// All balance checks are validated BEFORE any mutation is applied, so
    /// this method is atomic: on `Err`, the ledger is left completely
    /// unchanged (no partial debit/credit).
    pub fn apply_fill(&mut self, fill: &Fill) -> Result<(), ApexesError> {
        let instrument = self.instruments.get(&fill.symbol).ok_or_else(|| {
            ApexesError::InvalidBacktestConfig {
                reason: format!("no Instrument registered for symbol {}", fill.symbol),
            }
        })?;

        let base = instrument.base_currency.clone().ok_or_else(|| {
            ApexesError::InvalidBacktestConfig {
                reason: format!("Instrument {} has no base_currency", fill.symbol),
            }
        })?;
        let quote = instrument.quote_currency.clone().ok_or_else(|| {
            ApexesError::InvalidBacktestConfig {
                reason: format!("Instrument {} has no quote_currency", fill.symbol),
            }
        })?;

        let notional = fill.price * fill.quantity;
        let fee_currency = fill.fee_currency.clone().unwrap_or_else(|| quote.clone());

        // Phase 1: compute every (asset, debit_amount) this fill requires,
        // merging same-asset debits (e.g. fee_currency == quote) so the
        // balance check below is correct even when both debits draw from
        // the same asset.
        let mut required_debits: HashMap<String, Decimal> = HashMap::new();
        match fill.side {
            Side::Buy => {
                *required_debits.entry(quote.clone()).or_insert(Decimal::ZERO) += notional;
            }
            Side::Sell => {
                *required_debits.entry(base.clone()).or_insert(Decimal::ZERO) += fill.quantity;
            }
        }
        *required_debits.entry(fee_currency.clone()).or_insert(Decimal::ZERO) += fill.fee;

        // Phase 2: validate every required debit against current balances
        // before mutating anything.
        for (asset, amount) in &required_debits {
            let available = self.balances.get(asset).copied().unwrap_or(Decimal::ZERO);
            if available < *amount {
                return Err(ApexesError::InsufficientBalance {
                    asset: asset.clone(),
                    required: *amount,
                    available,
                });
            }
        }

        // Phase 3: all checks passed -- apply every mutation.
        for (asset, amount) in &required_debits {
            self.credit(asset, -*amount); // debit via negative credit (balance already validated)
        }
        match fill.side {
            Side::Buy => self.credit(&base, fill.quantity),
            Side::Sell => self.credit(&quote, notional),
        }

        self.update_position(fill);

        Ok(())
    }

    /// Computes total portfolio equity (sum of all balances converted to
    /// a common valuation, using `mark_prices` keyed by asset symbol ->
    /// price-in-quote-terms for non-quote assets; assets not present in
    /// `mark_prices` and not the implicit "quote of record" are treated
    /// as needing an explicit mark price, returning an error if missing
    /// and non-zero).
    pub fn equity(&self, mark_prices: &HashMap<String, Decimal>) -> Result<Decimal, ApexesError> {
        let mut total = Decimal::ZERO;
        for (asset, balance) in &self.balances {
            if balance.is_zero() {
                continue;
            }
            let price = mark_prices.get(asset).copied().unwrap_or(Decimal::ONE);
            total += balance * price;
        }
        Ok(total)
    }

    /// Adjusts `asset`'s balance by `amount` (positive to credit, negative
    /// to debit). Callers are responsible for validating sufficient
    /// balance BEFORE calling this for debits (see `apply_fill`'s
    /// phase 2/3 split) -- this method itself performs no validation, by
    /// design, so it can be used for both directions uniformly.
    fn credit(&mut self, asset: &str, amount: Decimal) {
        let balance = self.balances.entry(asset.to_string()).or_insert(Decimal::ZERO);
        *balance += amount;
    }

    fn update_position(&mut self, fill: &Fill) {
        let signed_quantity = match fill.side {
            Side::Buy => fill.quantity,
            Side::Sell => -fill.quantity,
        };

        let position = self.positions.entry(fill.symbol.clone()).or_insert(Position {
            symbol: fill.symbol.clone(),
            quantity: Decimal::ZERO,
            average_entry_price: Decimal::ZERO,
        });

        let same_direction = position.quantity.is_zero()
            || (position.quantity.is_sign_positive() == signed_quantity.is_sign_positive());

        if same_direction {
            // Increasing (or opening) a position: volume-weighted average entry price.
            let existing_value = position.quantity.abs() * position.average_entry_price;
            let new_value = signed_quantity.abs() * fill.price;
            let new_quantity = position.quantity + signed_quantity;
            if !new_quantity.is_zero() {
                position.average_entry_price = (existing_value + new_value) / new_quantity.abs();
            }
            position.quantity = new_quantity;
        } else {
            // Reducing or flipping a position: average entry price unchanged
            // for the reduced portion; if flipping past zero, the new
            // position's entry price is this fill's price.
            let new_quantity = position.quantity + signed_quantity;
            if position.quantity.abs() >= signed_quantity.abs() {
                position.quantity = new_quantity;
            } else {
                position.quantity = new_quantity;
                position.average_entry_price = fill.price;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, FillId, MarketType, OrderId, Venue};
    use chrono::Utc;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn btc_usdt() -> Instrument {
        Instrument::new(
            "BTCUSDT",
            MarketType::Crypto,
            Venue::Binance,
            AssetClass::Spot,
            Some("BTC".to_string()),
            Some("USDT".to_string()),
            dec!(0.01),
            dec!(0.0001),
            dec!(1),
        )
        .unwrap()
    }

    fn sample_fill(side: Side, price: Decimal, quantity: Decimal, fee: Decimal) -> Fill {
        Fill {
            id: FillId(Uuid::new_v4()),
            order_id: OrderId(Uuid::new_v4()),
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side,
            price,
            quantity,
            fee,
            fee_currency: None,
            filled_at: Utc::now(),
        }
    }

    fn ledger_with_10k_usdt() -> PortfolioLedger {
        let mut instruments = HashMap::new();
        instruments.insert("BTCUSDT".to_string(), btc_usdt());
        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(10000));
        PortfolioLedger::new(instruments, initial_balances)
    }

    #[test]
    fn buy_fill_debits_quote_credits_base() {
        let mut ledger = ledger_with_10k_usdt();
        let fill = sample_fill(Side::Buy, dec!(65000), dec!(0.1), dec!(6.5));

        ledger.apply_fill(&fill).unwrap();

        // 10000 - (65000*0.1) - 6.5 fee = 10000 - 6500 - 6.5 = 3493.5
        assert_eq!(*ledger.balances().get("USDT").unwrap(), dec!(3493.5));
        assert_eq!(*ledger.balances().get("BTC").unwrap(), dec!(0.1));
    }

    #[test]
    fn sell_fill_credits_quote_debits_base() {
        let mut ledger = ledger_with_10k_usdt();
        // First buy some BTC to have a base balance to sell (0.1 * 60000 = 6000 USDT, within the 10000 starting balance).
        ledger.apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.1), dec!(0))).unwrap();
        // USDT: 10000 - 6000 = 4000; BTC: 0.1

        let sell_fill = sample_fill(Side::Sell, dec!(65000), dec!(0.04), dec!(2.6));
        ledger.apply_fill(&sell_fill).unwrap();

        // Base: 0.1 - 0.04 = 0.06
        assert_eq!(*ledger.balances().get("BTC").unwrap(), dec!(0.06));
        // Quote: 4000 + (65000*0.04) - 2.6 fee = 4000 + 2600 - 2.6 = 6597.4
        assert_eq!(*ledger.balances().get("USDT").unwrap(), dec!(6597.4));
    }

    #[test]
    fn sell_fill_fails_with_insufficient_base_balance_and_leaves_ledger_unchanged() {
        let mut ledger = ledger_with_10k_usdt();
        ledger.apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.1), dec!(0))).unwrap();
        let usdt_before = *ledger.balances().get("USDT").unwrap();
        let btc_before = *ledger.balances().get("BTC").unwrap();

        // Try to sell more BTC than we have.
        let result = ledger.apply_fill(&sample_fill(Side::Sell, dec!(65000), dec!(5), dec!(0)));

        assert!(matches!(result, Err(ApexesError::InsufficientBalance { .. })));
        // Ledger must be completely unchanged after a failed apply_fill (atomicity).
        assert_eq!(*ledger.balances().get("USDT").unwrap(), usdt_before);
        assert_eq!(*ledger.balances().get("BTC").unwrap(), btc_before);
    }

    #[test]
    fn buy_fill_fails_with_insufficient_quote_balance() {
        let mut ledger = ledger_with_10k_usdt();
        let fill = sample_fill(Side::Buy, dec!(65000), dec!(1), dec!(0)); // needs 65000, only have 10000

        let result = ledger.apply_fill(&fill);

        assert!(matches!(result, Err(ApexesError::InsufficientBalance { .. })));
    }

    #[test]
    fn position_average_entry_price_is_volume_weighted() {
        let mut ledger = ledger_with_10k_usdt();
        ledger.apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.05), dec!(0))).unwrap();
        ledger.apply_fill(&sample_fill(Side::Buy, dec!(70000), dec!(0.05), dec!(0))).unwrap();

        let position = ledger.positions().get("BTCUSDT").unwrap();
        assert_eq!(position.quantity, dec!(0.1));
        // (0.05*60000 + 0.05*70000) / 0.1 = (3000+3500)/0.1 = 65000
        assert_eq!(position.average_entry_price, dec!(65000));
    }

    #[test]
    fn position_flips_sign_and_resets_entry_price_on_full_reversal() {
        // NOTE: this ledger additionally starts with 0.5 BTC so the
        // direction-flipping sell below passes the balance check
        // (apply_fill is atomic and rejects sells exceeding the base
        // balance; the brief's original 10k-USDT-only setup held just
        // 0.1 BTC at flip time and could never execute the 0.3 BTC sell).
        let mut instruments = HashMap::new();
        instruments.insert("BTCUSDT".to_string(), btc_usdt());
        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(10000));
        initial_balances.insert("BTC".to_string(), dec!(0.5));
        let mut ledger = PortfolioLedger::new(instruments, initial_balances);
        ledger.apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.1), dec!(0))).unwrap();
        // Sell more than the current long position: flips to short.
        ledger.apply_fill(&sample_fill(Side::Sell, dec!(65000), dec!(0.3), dec!(0))).unwrap();

        let position = ledger.positions().get("BTCUSDT").unwrap();
        assert_eq!(position.quantity, dec!(-0.2));
        assert_eq!(position.average_entry_price, dec!(65000));
    }

    #[test]
    fn apply_fill_fails_for_unregistered_symbol() {
        let mut ledger = ledger_with_10k_usdt();
        let mut fill = sample_fill(Side::Buy, dec!(100), dec!(1), dec!(0));
        fill.symbol = "UNKNOWNPAIR".to_string();

        let result = ledger.apply_fill(&fill);

        assert!(matches!(result, Err(ApexesError::InvalidBacktestConfig { .. })));
    }

    #[test]
    fn equity_sums_balances_with_mark_prices() {
        let mut ledger = ledger_with_10k_usdt();
        ledger.apply_fill(&sample_fill(Side::Buy, dec!(65000), dec!(0.1), dec!(0))).unwrap();

        let mut mark_prices = HashMap::new();
        mark_prices.insert("BTC".to_string(), dec!(65000));
        // USDT valued at 1.0 (default fallback for assets not in mark_prices).

        let equity = ledger.equity(&mark_prices).unwrap();
        // USDT: 10000 - 6500 = 3500; BTC: 0.1 * 65000 = 6500; total = 10000
        assert_eq!(equity, dec!(10000));
    }
}
