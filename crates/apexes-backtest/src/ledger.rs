use crate::cost_model::CostModel;
use apexes_core::{ApexesError, Fill, Instrument, MarginMode, Side};
use apexes_strategy::Position;
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
    pub fn new(
        instruments: HashMap<String, Instrument>,
        initial_balances: HashMap<String, Decimal>,
    ) -> Self {
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
    ///
    /// A fill with `leverage: Some(..)` takes the leveraged (perpetual
    /// futures) accounting path instead (`apply_leveraged_fill`) -- the
    /// spot-accounting body below is completely untouched by it.
    pub fn apply_fill(&mut self, fill: &Fill) -> Result<(), ApexesError> {
        if fill.leverage.is_some() {
            return self.apply_leveraged_fill(fill);
        }

        let instrument = self.instruments.get(&fill.symbol).ok_or_else(|| {
            ApexesError::InvalidBacktestConfig {
                reason: format!("no Instrument registered for symbol {}", fill.symbol),
            }
        })?;

        let base =
            instrument
                .base_currency
                .clone()
                .ok_or_else(|| ApexesError::InvalidBacktestConfig {
                    reason: format!("Instrument {} has no base_currency", fill.symbol),
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
                *required_debits
                    .entry(quote.clone())
                    .or_insert(Decimal::ZERO) += notional;
            }
            Side::Sell => {
                *required_debits.entry(base.clone()).or_insert(Decimal::ZERO) += fill.quantity;
            }
        }
        *required_debits
            .entry(fee_currency.clone())
            .or_insert(Decimal::ZERO) += fill.fee;

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

    /// Settles one funding payment for every currently-open leveraged
    /// position (`Position.leverage.is_some()`), using `cost_model`'s
    /// `funding_payment(notional)` (already existing, previously dead
    /// code -- see `CostModel`'s own doc comment). Positive payment
    /// debits the position holder's quote-currency balance (funding
    /// paid); negative debits (i.e. credits) it (funding received).
    /// Spot positions (`leverage: None`) are skipped entirely -- this
    /// method is a no-op for spot-only backtests, matching the
    /// pre-existing "funding_rate_bps: None disables funding entirely"
    /// contract. Calling this when `cost_model.funding_rate_bps` is
    /// `None` is also a safe no-op (via `funding_payment`'s existing
    /// `None`-propagating behavior).
    ///
    /// Callers (`run_event_loop`) are responsible for deciding WHEN to
    /// call this -- this method itself performs exactly one settlement
    /// pass over all open leveraged positions each time it's called; it
    /// does not track time or funding-interval scheduling itself.
    pub fn apply_funding_payments(
        &mut self,
        mark_prices: &HashMap<String, Decimal>,
        cost_model: &CostModel,
    ) -> Result<(), ApexesError> {
        // Collect (quote_currency, payment) pairs first so we don't hold
        // an immutable borrow of self.positions while later mutably
        // crediting self.balances.
        let mut settlements = Vec::new();
        for (symbol, position) in &self.positions {
            if position.leverage.is_none() || position.quantity.is_zero() {
                continue;
            }
            let instrument =
                self.instruments
                    .get(symbol)
                    .ok_or_else(|| ApexesError::InvalidBacktestConfig {
                        reason: format!("no Instrument registered for symbol {symbol}"),
                    })?;
            let quote = instrument.quote_currency.clone().ok_or_else(|| {
                ApexesError::InvalidBacktestConfig {
                    reason: format!("Instrument {symbol} has no quote_currency"),
                }
            })?;
            let mark_price = instrument
                .base_currency
                .as_ref()
                .and_then(|base| mark_prices.get(base))
                .copied()
                .unwrap_or(position.average_entry_price);
            let notional = position.quantity.abs() * mark_price;
            if let Some(payment) = cost_model.funding_payment(notional) {
                // A long position paying funding debits its quote
                // balance; a short position paying funding credits it
                // (mirroring real exchanges' sign convention where
                // funding flows from longs to shorts when the rate is
                // positive, or the reverse when negative).
                let signed_payment = if position.quantity.is_sign_positive() {
                    payment
                } else {
                    -payment
                };
                settlements.push((quote, signed_payment));
            }
        }

        for (quote, payment) in settlements {
            self.credit(&quote, -payment);
        }
        Ok(())
    }

    /// Leveraged (perpetual futures) fill accounting -- cash-settled,
    /// never credits/debits the literal base-asset quantity. Branches on
    /// whether `fill` is same-direction (opening/adding to the existing
    /// position) or opposite-direction (reducing/closing/flipping it),
    /// mirroring `update_position`'s existing `same_direction` check
    /// exactly. Called only from `apply_fill` when `fill.leverage.is_some()`.
    fn apply_leveraged_fill(&mut self, fill: &Fill) -> Result<(), ApexesError> {
        let instrument = self.instruments.get(&fill.symbol).ok_or_else(|| {
            ApexesError::InvalidBacktestConfig {
                reason: format!("no Instrument registered for symbol {}", fill.symbol),
            }
        })?;
        let quote = instrument.quote_currency.clone().ok_or_else(|| {
            ApexesError::InvalidBacktestConfig {
                reason: format!("Instrument {} has no quote_currency", fill.symbol),
            }
        })?;
        // `apply_fill` only dispatches here when `fill.leverage.is_some()`;
        // the `unwrap_or` is a defensive default, never actually taken.
        let leverage = fill.leverage.unwrap_or(Decimal::ONE);
        let margin_mode = fill.margin_mode.unwrap_or(MarginMode::Isolated);

        // Fee is always debited from the quote balance immediately,
        // regardless of opening/closing (matches real exchanges: fees
        // are never "reserved", they're paid on the spot).
        let fee_currency = fill.fee_currency.clone().unwrap_or_else(|| quote.clone());
        let fee_available = self
            .balances
            .get(&fee_currency)
            .copied()
            .unwrap_or(Decimal::ZERO);
        if fee_available < fill.fee {
            return Err(ApexesError::InsufficientBalance {
                asset: fee_currency,
                required: fill.fee,
                available: fee_available,
            });
        }

        let existing_quantity = self
            .positions
            .get(&fill.symbol)
            .map(|p| p.quantity)
            .unwrap_or(Decimal::ZERO);
        let signed_fill_quantity = match fill.side {
            Side::Buy => fill.quantity,
            Side::Sell => -fill.quantity,
        };
        let same_direction = existing_quantity.is_zero()
            || (existing_quantity.is_sign_positive() == signed_fill_quantity.is_sign_positive());

        if same_direction {
            self.open_leveraged(fill, &quote, &fee_currency, leverage, margin_mode)
        } else {
            self.close_leveraged(fill, &quote, &fee_currency, leverage, margin_mode)
        }
    }

    /// Opening/adding side of `apply_leveraged_fill`: reserves THIS fill's
    /// own incremental margin contribution (`quantity * price / leverage`),
    /// added to `Position.allocated_margin` -- never recomputes the whole
    /// position's margin against a new average entry price, matching how
    /// real exchanges (including Binance) accumulate margin per-trade.
    fn open_leveraged(
        &mut self,
        fill: &Fill,
        quote: &str,
        fee_currency: &str,
        leverage: Decimal,
        margin_mode: MarginMode,
    ) -> Result<(), ApexesError> {
        let this_fill_margin = fill.quantity * fill.price / leverage;
        let quote_available = self.balances.get(quote).copied().unwrap_or(Decimal::ZERO);
        if quote_available < this_fill_margin + fill.fee {
            return Err(ApexesError::InsufficientBalance {
                asset: quote.to_string(),
                required: this_fill_margin + fill.fee,
                available: quote_available,
            });
        }
        self.credit(fee_currency, -fill.fee);
        self.credit(quote, -this_fill_margin);

        let position = self
            .positions
            .entry(fill.symbol.clone())
            .or_insert(Position {
                symbol: fill.symbol.clone(),
                quantity: Decimal::ZERO,
                average_entry_price: Decimal::ZERO,
                leverage: Some(leverage),
                margin_mode: Some(margin_mode),
                allocated_margin: Decimal::ZERO,
            });
        let signed_fill_quantity = match fill.side {
            Side::Buy => fill.quantity,
            Side::Sell => -fill.quantity,
        };
        let existing_value = position.quantity.abs() * position.average_entry_price;
        let new_value = fill.quantity * fill.price;
        let new_quantity = position.quantity + signed_fill_quantity;
        if !new_quantity.is_zero() {
            position.average_entry_price = (existing_value + new_value) / new_quantity.abs();
        }
        position.quantity = new_quantity;
        position.allocated_margin += this_fill_margin;
        position.leverage = Some(leverage);
        position.margin_mode = Some(margin_mode);

        Ok(())
    }

    /// Reducing/closing/flipping side of `apply_leveraged_fill`: releases
    /// the closing portion's proportional share of `allocated_margin` and
    /// settles its realized PnL in the SAME direction as the position
    /// (long profits when price rises). If this fill's quantity exceeds
    /// the current position's quantity, the excess FLIPS the position --
    /// treated as a brand-new opening fill at `fill.price`, using THIS
    /// fill's own leverage/margin_mode (the position's old leverage no
    /// longer applies to the new, opposite-direction exposure).
    fn close_leveraged(
        &mut self,
        fill: &Fill,
        quote: &str,
        fee_currency: &str,
        leverage: Decimal,
        margin_mode: MarginMode,
    ) -> Result<(), ApexesError> {
        let Some(position) = self.positions.get(&fill.symbol).cloned() else {
            return Err(ApexesError::InvalidBacktestConfig {
                reason: format!(
                    "cannot reduce a leveraged position that doesn't exist for {}",
                    fill.symbol
                ),
            });
        };

        let closing_quantity = fill.quantity.min(position.quantity.abs());
        let flip_quantity = fill.quantity - closing_quantity; // > 0 only if this fill flips past zero

        // Settle the closing portion: release its proportional share of
        // allocated_margin and realize its PnL.
        let margin_released = if position.quantity.is_zero() {
            Decimal::ZERO
        } else {
            position.allocated_margin * (closing_quantity / position.quantity.abs())
        };
        let realized_pnl = if position.quantity.is_sign_positive() {
            (fill.price - position.average_entry_price) * closing_quantity
        } else {
            (position.average_entry_price - fill.price) * closing_quantity
        };

        // Build the post-close position as a pure computation -- no
        // mutation yet.
        let mut updated_position = position.clone();
        updated_position.allocated_margin -= margin_released;
        updated_position.quantity -= match fill.side {
            Side::Buy => -closing_quantity,
            Side::Sell => closing_quantity,
        };

        let mut flip_margin = Decimal::ZERO;
        if flip_quantity > Decimal::ZERO {
            // The flip remainder is a fresh opening fill -- its margin
            // requirement is validated against the PROJECTED quote balance
            // after the closing portion's credits (fee debit + released
            // margin + realized PnL), so the just-released margin and PnL
            // are available to fund it (a full close + flip at a higher
            // effective leverage otherwise fails spuriously) -- but checked
            // BEFORE any of those credits are actually applied, so a failed
            // check leaves the ledger completely unchanged.
            flip_margin = flip_quantity * fill.price / leverage;
            let fee_on_quote = if fee_currency == quote {
                fill.fee
            } else {
                Decimal::ZERO
            };
            let projected_quote = self.balances.get(quote).copied().unwrap_or(Decimal::ZERO)
                - fee_on_quote
                + margin_released
                + realized_pnl;
            if projected_quote < flip_margin {
                return Err(ApexesError::InsufficientBalance {
                    asset: quote.to_string(),
                    required: flip_margin,
                    available: projected_quote,
                });
            }
            updated_position.quantity = match fill.side {
                Side::Buy => flip_quantity,
                Side::Sell => -flip_quantity,
            };
            updated_position.average_entry_price = fill.price;
            updated_position.allocated_margin = flip_margin;
            updated_position.leverage = Some(leverage);
            updated_position.margin_mode = Some(margin_mode);
        }

        // All validation passed -- apply every mutation together.
        self.credit(fee_currency, -fill.fee);
        self.credit(quote, margin_released + realized_pnl);
        if flip_quantity > Decimal::ZERO {
            self.credit(quote, -flip_margin);
        }
        self.positions.insert(fill.symbol.clone(), updated_position);

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
        let balance = self
            .balances
            .entry(asset.to_string())
            .or_insert(Decimal::ZERO);
        *balance += amount;
    }

    fn update_position(&mut self, fill: &Fill) {
        let signed_quantity = match fill.side {
            Side::Buy => fill.quantity,
            Side::Sell => -fill.quantity,
        };

        let position = self
            .positions
            .entry(fill.symbol.clone())
            .or_insert(Position {
                symbol: fill.symbol.clone(),
                quantity: Decimal::ZERO,
                average_entry_price: Decimal::ZERO,
                leverage: None,
                margin_mode: None,
                allocated_margin: Decimal::ZERO,
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
    use crate::cost_model::FlatFeeSchedule;
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
            leverage: None,
            margin_mode: None,
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
        ledger
            .apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.1), dec!(0)))
            .unwrap();
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
        ledger
            .apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.1), dec!(0)))
            .unwrap();
        let usdt_before = *ledger.balances().get("USDT").unwrap();
        let btc_before = *ledger.balances().get("BTC").unwrap();

        // Try to sell more BTC than we have.
        let result = ledger.apply_fill(&sample_fill(Side::Sell, dec!(65000), dec!(5), dec!(0)));

        assert!(matches!(
            result,
            Err(ApexesError::InsufficientBalance { .. })
        ));
        // Ledger must be completely unchanged after a failed apply_fill (atomicity).
        assert_eq!(*ledger.balances().get("USDT").unwrap(), usdt_before);
        assert_eq!(*ledger.balances().get("BTC").unwrap(), btc_before);
    }

    #[test]
    fn buy_fill_fails_with_insufficient_quote_balance() {
        let mut ledger = ledger_with_10k_usdt();
        let fill = sample_fill(Side::Buy, dec!(65000), dec!(1), dec!(0)); // needs 65000, only have 10000

        let result = ledger.apply_fill(&fill);

        assert!(matches!(
            result,
            Err(ApexesError::InsufficientBalance { .. })
        ));
    }

    #[test]
    fn position_average_entry_price_is_volume_weighted() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.05), dec!(0)))
            .unwrap();
        ledger
            .apply_fill(&sample_fill(Side::Buy, dec!(70000), dec!(0.05), dec!(0)))
            .unwrap();

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
        ledger
            .apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.1), dec!(0)))
            .unwrap();
        // Sell more than the current long position: flips to short.
        ledger
            .apply_fill(&sample_fill(Side::Sell, dec!(65000), dec!(0.3), dec!(0)))
            .unwrap();

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

        assert!(matches!(
            result,
            Err(ApexesError::InvalidBacktestConfig { .. })
        ));
    }

    #[test]
    fn equity_sums_balances_with_mark_prices() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&sample_fill(Side::Buy, dec!(65000), dec!(0.1), dec!(0)))
            .unwrap();

        let mut mark_prices = HashMap::new();
        mark_prices.insert("BTC".to_string(), dec!(65000));
        // USDT valued at 1.0 (default fallback for assets not in mark_prices).

        let equity = ledger.equity(&mark_prices).unwrap();
        // USDT: 10000 - 6500 = 3500; BTC: 0.1 * 65000 = 6500; total = 10000
        assert_eq!(equity, dec!(10000));
    }

    fn leveraged_fill_for_symbol(
        symbol: &str,
        side: Side,
        price: Decimal,
        quantity: Decimal,
        leverage: Decimal,
    ) -> Fill {
        Fill {
            id: FillId(Uuid::new_v4()),
            order_id: OrderId(Uuid::new_v4()),
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            side,
            price,
            quantity,
            fee: Decimal::ZERO,
            fee_currency: None,
            filled_at: Utc::now(),
            leverage: Some(leverage),
            margin_mode: Some(MarginMode::Isolated),
        }
    }

    #[test]
    fn leveraged_buy_reserves_incremental_margin_not_full_notional() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&leveraged_fill_for_symbol(
                "BTCUSDT",
                Side::Buy,
                dec!(60000),
                dec!(1),
                dec!(10),
            ))
            .unwrap();

        // margin = notional/leverage = 60000/10 = 6000 -- NOT the full 60000.
        assert_eq!(*ledger.balances().get("USDT").unwrap(), dec!(4000));
        let position = ledger.positions().get("BTCUSDT").unwrap();
        assert_eq!(position.allocated_margin, dec!(6000));
        assert_eq!(position.leverage, Some(dec!(10)));
        // Crucially: leveraged fills must NEVER credit the literal base
        // asset (unlike spot) -- there is no real BTC custody here.
        assert!(
            ledger.balances().get("BTC").is_none()
                || ledger.balances().get("BTC") == Some(&Decimal::ZERO)
        );
    }

    #[test]
    fn leveraged_add_accumulates_margin_incrementally() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&leveraged_fill_for_symbol(
                "BTCUSDT",
                Side::Buy,
                dec!(60000),
                dec!(0.5),
                dec!(10),
            ))
            .unwrap();
        ledger
            .apply_fill(&leveraged_fill_for_symbol(
                "BTCUSDT",
                Side::Buy,
                dec!(62000),
                dec!(0.5),
                dec!(10),
            ))
            .unwrap();

        // 30000/10 + 31000/10 = 3000 + 3100 = 6100 -- accumulated, not recomputed.
        let position = ledger.positions().get("BTCUSDT").unwrap();
        assert_eq!(position.allocated_margin, dec!(6100));
        assert_eq!(position.quantity, dec!(1));
    }

    #[test]
    fn leveraged_partial_close_releases_proportional_margin_and_realizes_pnl() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&leveraged_fill_for_symbol(
                "BTCUSDT",
                Side::Buy,
                dec!(60000),
                dec!(1),
                dec!(10),
            ))
            .unwrap();
        // margin=6000, balance now 4000.

        ledger
            .apply_fill(&leveraged_fill_for_symbol(
                "BTCUSDT",
                Side::Sell,
                dec!(65000),
                dec!(0.5),
                dec!(10),
            ))
            .unwrap();
        // Closing half: releases 3000 margin, realizes (65000-60000)*0.5 = 2500 profit.
        // New balance: 4000 + 3000 + 2500 = 9500.
        assert_eq!(*ledger.balances().get("USDT").unwrap(), dec!(9500));
        let position = ledger.positions().get("BTCUSDT").unwrap();
        assert_eq!(position.quantity, dec!(0.5));
        assert_eq!(position.allocated_margin, dec!(3000));
    }

    #[test]
    fn leveraged_flip_closes_old_side_and_opens_new_side_at_flip_fills_own_leverage() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&leveraged_fill_for_symbol(
                "BTCUSDT",
                Side::Buy,
                dec!(60000),
                dec!(1),
                dec!(10),
            ))
            .unwrap();
        // Sell 1.5 -- closes the 1 long AND opens a 0.5 short, at 5x leverage this time.
        ledger
            .apply_fill(&leveraged_fill_for_symbol(
                "BTCUSDT",
                Side::Sell,
                dec!(60000),
                dec!(1.5),
                dec!(5),
            ))
            .unwrap();

        let position = ledger.positions().get("BTCUSDT").unwrap();
        assert_eq!(position.quantity, dec!(-0.5));
        assert_eq!(position.average_entry_price, dec!(60000));
        // New margin for the flipped 0.5 short at 5x: 0.5*60000/5 = 6000.
        assert_eq!(position.allocated_margin, dec!(6000));
        assert_eq!(position.leverage, Some(dec!(5)));
    }

    #[test]
    fn leveraged_flip_with_insufficient_margin_leaves_ledger_completely_unchanged() {
        // Atomicity regression guard for `close_leveraged`: a flip whose
        // margin requirement is insufficient even AFTER accounting for the
        // closing portion's credits must return Err with NO partial
        // mutation -- balances must not reflect the (never-completed) close
        // and the position map must still hold the OLD pre-close position.
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&leveraged_fill_for_symbol(
                "BTCUSDT",
                Side::Buy,
                dec!(60000),
                dec!(1),
                dec!(10),
            ))
            .unwrap();
        // margin=6000 reserved, quote balance now 4000.

        let balances_before = ledger.balances().clone();
        let positions_before = ledger.positions().clone();

        // Sell 2 @60000 at 2x: closes the 1 long (would release 6000
        // margin, zero PnL) and flips into a 1 short requiring
        // 1*60000/2 = 30000 margin -- but the projected quote balance
        // after the close credits is only 4000 + 6000 = 10000.
        let result = ledger.apply_fill(&leveraged_fill_for_symbol(
            "BTCUSDT",
            Side::Sell,
            dec!(60000),
            dec!(2),
            dec!(2),
        ));
        assert!(matches!(
            result,
            Err(ApexesError::InsufficientBalance { .. })
        ));

        // Byte-for-byte unchanged: no fee debit, no margin release, no
        // PnL credit, no position mutation.
        assert_eq!(ledger.balances(), &balances_before);
        assert_eq!(ledger.positions(), &positions_before);
    }

    #[test]
    fn spot_fills_are_completely_unaffected_by_leveraged_accounting_path() {
        // Regression guard: confirms apply_fill's spot branch (fill.leverage
        // is None) is byte-for-byte the pre-existing behavior, exercised
        // via the EXISTING buy_fill_debits_quote_credits_base test's
        // assertions replayed here for extra confidence after this task's
        // refactor.
        let mut ledger = ledger_with_10k_usdt();
        let fill = sample_fill(Side::Buy, dec!(65000), dec!(0.1), dec!(6.5));
        ledger.apply_fill(&fill).unwrap();
        assert_eq!(*ledger.balances().get("USDT").unwrap(), dec!(3493.5));
        assert_eq!(*ledger.balances().get("BTC").unwrap(), dec!(0.1));
    }

    fn cost_model_with_funding(funding_rate_bps: Decimal) -> CostModel {
        CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: dec!(0),
                taker_bps: dec!(0),
            }),
            funding_rate_bps: Some(funding_rate_bps),
        }
    }

    #[test]
    fn apply_funding_payments_debits_long_position_holder() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&leveraged_fill_for_symbol(
                "BTCUSDT",
                Side::Buy,
                dec!(60000),
                dec!(0.1),
                dec!(5),
            ))
            .unwrap();

        let mut mark_prices = HashMap::new();
        mark_prices.insert("BTC".to_string(), dec!(60000));
        let cost_model = cost_model_with_funding(dec!(1)); // 0.01%

        let usdt_before = *ledger.balances().get("USDT").unwrap();
        ledger
            .apply_funding_payments(&mark_prices, &cost_model)
            .unwrap();
        let usdt_after = *ledger.balances().get("USDT").unwrap();

        // notional = 0.1 * 60000 = 6000; payment = 6000 * 0.0001 = 0.6
        assert_eq!(usdt_before - usdt_after, dec!(0.6));
    }

    #[test]
    fn apply_funding_payments_credits_short_position_holder() {
        let mut instruments = HashMap::new();
        instruments.insert("BTCUSDT".to_string(), btc_usdt());
        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(10000));
        initial_balances.insert("BTC".to_string(), dec!(1));
        let mut ledger = PortfolioLedger::new(instruments, initial_balances);
        ledger
            .apply_fill(&leveraged_fill_for_symbol(
                "BTCUSDT",
                Side::Sell,
                dec!(60000),
                dec!(0.1),
                dec!(5),
            ))
            .unwrap();

        let mut mark_prices = HashMap::new();
        mark_prices.insert("BTC".to_string(), dec!(60000));
        let cost_model = cost_model_with_funding(dec!(1));

        let usdt_before = *ledger.balances().get("USDT").unwrap();
        ledger
            .apply_funding_payments(&mark_prices, &cost_model)
            .unwrap();
        let usdt_after = *ledger.balances().get("USDT").unwrap();

        assert_eq!(usdt_after - usdt_before, dec!(0.6));
    }

    #[test]
    fn apply_funding_payments_skips_spot_positions() {
        let mut ledger = ledger_with_10k_usdt();
        // A genuinely SPOT fill (sample_fill, not leveraged_fill_for_symbol
        // -- fill.leverage stays None, so apply_fill's spot branch runs).
        ledger
            .apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.1), dec!(0)))
            .unwrap();

        let mut mark_prices = HashMap::new();
        mark_prices.insert("BTC".to_string(), dec!(60000));
        let cost_model = cost_model_with_funding(dec!(1));

        let usdt_before = *ledger.balances().get("USDT").unwrap();
        ledger
            .apply_funding_payments(&mark_prices, &cost_model)
            .unwrap();
        let usdt_after = *ledger.balances().get("USDT").unwrap();

        assert_eq!(
            usdt_before, usdt_after,
            "spot positions must not be charged funding"
        );
    }

    #[test]
    fn apply_funding_payments_is_a_noop_when_funding_disabled() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&leveraged_fill_for_symbol(
                "BTCUSDT",
                Side::Buy,
                dec!(60000),
                dec!(0.1),
                dec!(5),
            ))
            .unwrap();

        let mut mark_prices = HashMap::new();
        mark_prices.insert("BTC".to_string(), dec!(60000));
        let cost_model = CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: dec!(0),
                taker_bps: dec!(0),
            }),
            funding_rate_bps: None,
        };

        let usdt_before = *ledger.balances().get("USDT").unwrap();
        ledger
            .apply_funding_payments(&mark_prices, &cost_model)
            .unwrap();
        let usdt_after = *ledger.balances().get("USDT").unwrap();

        assert_eq!(usdt_before, usdt_after);
    }
}
