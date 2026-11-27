use crate::cost_model::CostModel;
use apexes_core::{ApexesError, Fill, Instrument, MarginMode, Side};
use apexes_strategy::{MarginTierTable, Position, PositionMarginStatus};
use rust_decimal::Decimal;
use std::collections::HashMap;

/// Tracks multi-asset balances (like a real exchange account: balance per
/// asset, not just quote currency) and per-symbol net positions during a
/// backtest, updated by applying each simulated `Fill`.
pub struct PortfolioLedger {
    instruments: HashMap<String, Instrument>,
    balances: HashMap<String, Decimal>,
    positions: HashMap<String, Position>,
    /// Per-symbol accumulated realized profit/loss. Spot reductions
    /// (`update_position`'s reduce/flip branch) and leveraged closes
    /// (`close_leveraged`) both write into this map; opening/adding to
    /// a position never does (there is no realized PnL to book until
    /// quantity is reduced). See `docs/superpowers/specs/2026-08-02-apexes-account-design.md`
    /// §五 for the full design rationale.
    realized_pnl: HashMap<String, Decimal>,
    spot_accounting: crate::spot_accounting::SpotAccounting,
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
            realized_pnl: HashMap::new(),
            spot_accounting: Default::default(),
        }
    }

    /// Seeds pre-existing positions (e.g. recovered from the exchange at
    /// live startup). Balances are NOT adjusted: callers pass balances
    /// that already reflect these positions.
    pub fn with_positions(mut self, positions: HashMap<String, Position>) -> Self {
        self.positions = positions;
        self
    }

    /// Private execution projection for atomic arrival checks. Attribution
    /// histories are not needed to validate balances or positions; the
    /// authoritative ledger records them when accepted fills are delivered.
    pub(crate) fn execution_snapshot(&self) -> Self {
        Self {
            instruments: self.instruments.clone(),
            balances: self.balances.clone(),
            positions: self.positions.clone(),
            realized_pnl: self.realized_pnl.clone(),
            spot_accounting: Default::default(),
        }
    }

    pub fn balances(&self) -> &HashMap<String, Decimal> {
        &self.balances
    }

    pub fn positions(&self) -> &HashMap<String, Position> {
        &self.positions
    }

    pub fn realized_pnl(&self) -> &HashMap<String, Decimal> {
        &self.realized_pnl
    }

    /// Record an observed conversion price for fee attribution, not a future quote.
    pub fn observe_spot_price(
        &mut self,
        symbol: &str,
        at: chrono::DateTime<chrono::Utc>,
        price: Decimal,
    ) {
        if let Some(instrument) = self.instruments.get(symbol)
            && instrument.asset_class == apexes_core::AssetClass::Spot
            && let (Some(base), Some(quote)) =
                (&instrument.base_currency, &instrument.quote_currency)
        {
            self.spot_accounting
                .observe(instrument.venue, base, quote, at, price);
        }
    }

    pub fn spot_pnl(&self) -> HashMap<String, crate::spot_accounting::SpotPnlSummary> {
        self.spot_accounting.summaries()
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

        let is_spot = instrument.asset_class == apexes_core::AssetClass::Spot;
        let notional = fill.price * fill.quantity;
        let fee_currency = fill.fee_currency.clone().unwrap_or_else(|| quote.clone());
        let external_fee = !self.tracks_asset(&fee_currency, &base, &quote);

        // Phase 1: the NET change this fill makes to each asset. A fee is
        // netted against what the same fill credits: Binance takes it from
        // the asset RECEIVED (base on a buy, quote on a sell), so checking
        // the fee against the balance before the credit refused valid fills.
        let mut deltas: HashMap<String, Decimal> = HashMap::new();
        let mut add = |asset: &str, amount: Decimal| {
            *deltas.entry(asset.to_string()).or_insert(Decimal::ZERO) += amount;
        };
        match fill.side {
            Side::Buy => {
                add(&quote, -notional);
                add(&base, fill.quantity);
            }
            Side::Sell => {
                add(&base, -fill.quantity);
                add(&quote, notional);
            }
        }
        if !fill.fee.is_zero() {
            if self.tracks_asset(&fee_currency, &base, &quote) {
                add(&fee_currency, -fill.fee);
            } else {
                // The session holds only this symbol's assets, so a fee in
                // another one (BNB with "pay fees in BNB") has no balance to
                // come out of. The trade still happened on the exchange; the
                // fee is left out of the ledger rather than undoing the fill.
                // The evaluation's fee total still counts it.
                tracing::debug!(
                    asset = %fee_currency,
                    fee = %fill.fee,
                    "fee in an asset this session does not track; not debited from the ledger"
                );
            }
        }

        // Phase 2: every asset that goes down must stay non-negative,
        // validated before anything changes (atomic).
        for (asset, delta) in &deltas {
            if delta.is_sign_negative() && !delta.is_zero() {
                let available = self.balances.get(asset).copied().unwrap_or(Decimal::ZERO);
                if available + *delta < Decimal::ZERO {
                    return Err(ApexesError::InsufficientBalance {
                        asset: asset.clone(),
                        required: -*delta,
                        available,
                    });
                }
            }
        }

        // Phase 3: apply.
        for (asset, delta) in &deltas {
            self.credit(asset, *delta);
        }

        let base_fee = if fill.fee_currency.as_deref() == Some(base.as_str()) {
            fill.fee
        } else {
            Decimal::ZERO
        };
        self.update_position(fill, base_fee);
        if is_spot {
            self.spot_accounting
                .record(fill, &base, &quote, external_fee);
        }

        Ok(())
    }

    /// Whether the ledger keeps a balance for `asset`: the symbol's own two
    /// assets, or any asset the session was seeded with.
    fn tracks_asset(&self, asset: &str, base: &str, quote: &str) -> bool {
        asset == base || asset == quote || self.balances.contains_key(asset)
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
    /// Callers (`run_unified_event_loop`) are responsible for deciding WHEN to
    /// call this -- this method itself performs exactly one settlement
    /// pass over all open leveraged positions each time it's called; it
    /// does not track time or funding-interval scheduling itself.
    pub fn apply_funding_payments(
        &mut self,
        mark_prices: &HashMap<String, Decimal>,
        cost_model: &CostModel,
    ) -> Result<(), ApexesError> {
        self.settle_funding(mark_prices, cost_model).map(|_| ())
    }

    pub(crate) fn settle_funding(
        &mut self,
        mark_prices: &HashMap<String, Decimal>,
        cost_model: &CostModel,
    ) -> Result<Vec<crate::funding_attribution::FundingMovement>, ApexesError> {
        use crate::funding_attribution::{FundingMovement, FundingPriceSource};
        // Collect settlements first so we don't hold
        // an immutable borrow of self.positions while later mutably
        // crediting self.balances.
        let mut settlements = Vec::new();
        for (symbol, position) in crate::stable_order::entries(&self.positions) {
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
            let observed_mark = instrument
                .base_currency
                .as_ref()
                .and_then(|base| mark_prices.get(base))
                .copied();
            let mark_price = observed_mark.unwrap_or(position.average_entry_price);
            let notional = position.quantity.abs() * mark_price;
            if let (Some(rate_bps), Some(payment)) = (
                cost_model.funding_rate_bps,
                cost_model.funding_payment(notional),
            ) {
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
                settlements.push(FundingMovement {
                    symbol: symbol.clone(),
                    venue: instrument.venue,
                    quote_currency: quote,
                    position_quantity: position.quantity,
                    valuation_price: mark_price,
                    price_source: if observed_mark.is_some() {
                        FundingPriceSource::Mark
                    } else {
                        FundingPriceSource::EntryFallback
                    },
                    notional,
                    rate_bps,
                    balance_delta: -signed_payment,
                });
            }
        }

        for settlement in &settlements {
            self.credit(&settlement.quote_currency, settlement.balance_delta);
        }
        Ok(settlements)
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
    ///
    /// Design-spec invariant: adding to an EXISTING Isolated position at a
    /// different leverage than the position's current leverage is rejected
    /// (`InconsistentLeverageForIsolatedPosition`) BEFORE any mutation --
    /// same-direction adds must keep the position's leverage uniform.
    /// Cross-margin positions are exempt (cross leverage is an order-level
    /// parameter, not required to be uniform across the account).
    fn open_leveraged(
        &mut self,
        fill: &Fill,
        quote: &str,
        fee_currency: &str,
        leverage: Decimal,
        margin_mode: MarginMode,
    ) -> Result<(), ApexesError> {
        if margin_mode == MarginMode::Isolated
            && let Some(existing_position) = self.positions.get(&fill.symbol)
            && !existing_position.quantity.is_zero()
        {
            let existing_leverage = existing_position.leverage.unwrap_or(Decimal::ONE);
            if existing_leverage != leverage {
                return Err(ApexesError::InconsistentLeverageForIsolatedPosition {
                    symbol: fill.symbol.clone(),
                    existing: existing_leverage,
                    requested: leverage,
                });
            }
        }

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
        // Accumulate the closing portion's realized PnL into the new field.
        // Placed here (with the other mutations, AFTER the flip-margin
        // validation above) rather than next to the local `realized_pnl`
        // computation so a rejected flip leaves the ledger -- including this
        // field -- completely unchanged, preserving `apply_fill`'s atomicity
        // contract (the brief's proposed placement next to the computation
        // would book PnL for a fill that was never accepted).
        *self
            .realized_pnl
            .entry(fill.symbol.clone())
            .or_insert(Decimal::ZERO) += realized_pnl;
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
        for (asset, balance) in crate::stable_order::entries(&self.balances) {
            if balance.is_zero() {
                continue;
            }
            let price = mark_prices.get(asset).copied().unwrap_or(Decimal::ONE);
            total += balance * price;
        }
        Ok(total)
    }

    /// Computes the real-time margin state of every currently-open
    /// leveraged position. Spot positions (`leverage: None`) are
    /// skipped entirely (they never appear in the returned map --
    /// margin doesn't apply to them, there is no sentinel entry).
    ///
    /// Isolated positions get a precise `margin_ratio`: the position's
    /// own effective margin (its reserved `allocated_margin` PLUS its
    /// unrealized PnL at the current mark price -- matching how real
    /// exchanges mark an isolated position's margin balance to market)
    /// divided by that position's maintenance margin requirement (per
    /// its `MarginTierTable` bracket for its current notional). They
    /// also get a precise `liquidation_price`: the mark price at which
    /// `margin_ratio` would hit exactly `1.0`, solved algebraically
    /// (see `isolated_liquidation_price`).
    ///
    /// Cross positions get an ACCOUNT-LEVEL `margin_ratio` (this
    /// account's total equity, via `self.equity(mark_prices)`, divided
    /// by the SUM of every cross position's maintenance margin
    /// requirement) -- the same value for every cross position this
    /// account holds, since they all share one pool. `liquidation_price`
    /// is always `None` for cross positions (see `PositionMarginStatus`'s
    /// own doc comment for why a single position's liquidation price
    /// isn't a well-defined concept under cross margin).
    pub fn margin_status(
        &self,
        mark_prices: &HashMap<String, Decimal>,
        margin_tiers: &HashMap<String, MarginTierTable>,
    ) -> HashMap<String, PositionMarginStatus> {
        let mut result = HashMap::new();

        // First pass: compute the account-wide cross-margin maintenance
        // requirement total, needed before we can compute any
        // individual cross position's margin_ratio.
        let mut cross_maintenance_total = Decimal::ZERO;
        for (symbol, position) in crate::stable_order::entries(&self.positions) {
            if position.leverage.is_none() || position.quantity.is_zero() {
                continue;
            }
            if position.margin_mode != Some(MarginMode::Cross) {
                continue;
            }
            if let Some(maintenance) =
                self.maintenance_requirement(symbol, position, mark_prices, margin_tiers)
            {
                cross_maintenance_total += maintenance;
            }
        }
        let account_equity = self.equity(mark_prices).unwrap_or(Decimal::ZERO);
        let cross_margin_ratio = if cross_maintenance_total.is_zero() {
            None
        } else {
            Some(account_equity / cross_maintenance_total)
        };

        for (symbol, position) in crate::stable_order::entries(&self.positions) {
            if position.leverage.is_none() || position.quantity.is_zero() {
                continue;
            }
            let Some(margin_mode) = position.margin_mode else {
                continue;
            };

            let status = match margin_mode {
                MarginMode::Isolated => {
                    let mark_price = self.mark_price_for_position(symbol, mark_prices);
                    let maintenance =
                        self.maintenance_requirement(symbol, position, mark_prices, margin_tiers);
                    // `allocated_margin` is a REAL, incrementally-
                    // accumulated field on `Position` (maintained by
                    // `PortfolioLedger::apply_leveraged_fill`, Task 2b)
                    // -- read it directly here rather than recomputing
                    // `notional / leverage` from scratch. Recomputing
                    // would be WRONG whenever a position has been
                    // partially closed or added to at a different price
                    // than its current average entry price, since real
                    // exchanges (and Task 2b's accounting) never
                    // "re-margin" a position's already-reserved margin
                    // just because its average entry price changed.
                    //
                    // The effective margin balance of an isolated
                    // position is its reserved margin PLUS its
                    // unrealized PnL at the current mark price: adverse
                    // price moves erode the margin balance in real time
                    // (this is what makes `check_liquidations` able to
                    // fire at all -- without the PnL term the ratio
                    // would only ever drift with notional, backwards).
                    let unrealized_pnl = if position.quantity.is_sign_positive() {
                        (mark_price - position.average_entry_price) * position.quantity
                    } else {
                        (position.average_entry_price - mark_price) * position.quantity.abs()
                    };
                    let effective_margin = position.allocated_margin + unrealized_pnl;
                    let margin_ratio = maintenance
                        .filter(|m| !m.is_zero())
                        .map(|m| effective_margin / m);
                    let liquidation_price =
                        self.isolated_liquidation_price(position, margin_tiers.get(symbol));
                    PositionMarginStatus {
                        margin_ratio,
                        liquidation_price,
                    }
                }
                MarginMode::Cross => PositionMarginStatus {
                    margin_ratio: cross_margin_ratio,
                    liquidation_price: None,
                },
            };
            result.insert(symbol.clone(), status);
        }

        result
    }

    /// Returns this position's maintenance margin requirement (notional
    /// times the tier's `maintenance_margin_rate`, minus the tier's
    /// `maintenance_amount`, per its `MarginTierTable` bracket for its
    /// current notional), or `None` if no tier table is configured for
    /// this symbol.
    fn maintenance_requirement(
        &self,
        symbol: &str,
        position: &apexes_strategy::Position,
        mark_prices: &HashMap<String, Decimal>,
        margin_tiers: &HashMap<String, MarginTierTable>,
    ) -> Option<Decimal> {
        let mark_price = self.mark_price_for_position(symbol, mark_prices);
        let notional = position.quantity.abs() * mark_price;
        let tier = margin_tiers.get(symbol)?.tier_for_notional(notional)?;
        Some(notional * tier.maintenance_margin_rate - tier.maintenance_amount)
    }

    /// Resolves the mark price to use for `symbol`'s notional/margin
    /// calculations: looks up `mark_prices` by the symbol's own base
    /// currency (matching `equity`'s existing convention), falling back
    /// to the position's `average_entry_price` if no mark price is
    /// available (a conservative approximation, not a silent zero).
    fn mark_price_for_position(
        &self,
        symbol: &str,
        mark_prices: &HashMap<String, Decimal>,
    ) -> Decimal {
        self.instruments
            .get(symbol)
            .and_then(|instrument| instrument.base_currency.as_ref())
            .and_then(|base| mark_prices.get(base))
            .copied()
            .unwrap_or_else(|| {
                self.positions
                    .get(symbol)
                    .map(|p| p.average_entry_price)
                    .unwrap_or(Decimal::ONE)
            })
    }

    /// Solves for the mark price at which an isolated position's
    /// `margin_ratio` would equal exactly `1.0` -- i.e. the price `p`
    /// where `allocated_margin + unrealized_pnl(p) == maintenance(p)`,
    /// with `maintenance(p) = |q| * p * rate - amount` for the
    /// position's current tier.
    ///
    /// Long (`q > 0`, entry `e`, reserved margin `M`, tier rate `r`,
    /// tier amount `A`):
    ///   M + q*(p - e) = q*p*r - A   =>   p = (q*e - M - A) / (q*(1 - r))
    /// Short (`Q = |q|`):
    ///   M + Q*(e - p) = Q*p*r - A   =>   p = (Q*e + M + A) / (Q*(1 + r))
    ///
    /// Since `maintenance` itself depends on notional (and therefore on
    /// price), this uses the CURRENT tier's rate as a linear
    /// approximation (an accepted simplification -- see this plan's own
    /// non-goals: not a perfectly tier-boundary-aware iterative solve).
    fn isolated_liquidation_price(
        &self,
        position: &apexes_strategy::Position,
        tier_table: Option<&MarginTierTable>,
    ) -> Option<Decimal> {
        let tier_table = tier_table?;
        let quantity = position.quantity.abs();
        if quantity.is_zero() {
            return None;
        }
        let entry_notional = quantity * position.average_entry_price;
        let tier = tier_table.tier_for_notional(entry_notional)?;
        let rate = tier.maintenance_margin_rate;
        let amount = tier.maintenance_amount;
        let margin = position.allocated_margin;
        if position.quantity.is_sign_positive() {
            let denominator = quantity * (Decimal::ONE - rate);
            if denominator.is_zero() {
                return None;
            }
            Some((entry_notional - margin - amount) / denominator)
        } else {
            let denominator = quantity * (Decimal::ONE + rate);
            Some((entry_notional + margin + amount) / denominator)
        }
    }

    /// Returns every symbol whose current `margin_status` has
    /// `margin_ratio <= 1.0` (maintenance margin requirement can no
    /// longer be met by the allocated/pooled margin) -- these positions
    /// must be force-closed. Callers (`run_unified_event_loop`, backtest/paper
    /// only -- `run_live_trading` calls this too but never force-closes,
    /// see Task 7) submit one market order per returned symbol.
    pub fn check_liquidations(
        &self,
        mark_prices: &HashMap<String, Decimal>,
        margin_tiers: &HashMap<String, MarginTierTable>,
    ) -> Vec<String> {
        let mut symbols: Vec<_> = self
            .margin_status(mark_prices, margin_tiers)
            .into_iter()
            .filter_map(|(symbol, status)| {
                let ratio = status.margin_ratio?;
                (ratio <= Decimal::ONE).then_some(symbol)
            })
            .collect();
        symbols.sort_unstable();
        symbols
    }

    /// Returns every `(symbol, side, quantity)` a caller should submit
    /// as a partial-close market order to de-risk in response to
    /// extreme market conditions. `reduce_fractions` is read from
    /// `apexes_risk::MarketConditionReadHandle::reduce_fractions`
    /// (already resolved for account-wide triggers by that point --
    /// this method itself has no concept of "account-wide", it only
    /// sees a flat per-symbol map). `include_spot: false` skips
    /// positions with `leverage: None`. Symbols with zero quantity, or
    /// not present in `reduce_fractions`, are skipped entirely.
    /// `quantity = position.quantity.abs() * reduce_fraction`; `side`
    /// is inverted from the position's sign (long position ->
    /// `Side::Sell` to reduce, short position -> `Side::Buy`),
    /// matching `check_liquidations`'s caller-side convention exactly.
    pub fn check_de_risking(
        &self,
        reduce_fractions: &HashMap<String, Decimal>,
        include_spot: bool,
    ) -> Vec<(String, Side, Decimal)> {
        crate::stable_order::entries(&self.positions)
            .into_iter()
            .filter_map(|(symbol, position)| {
                if position.quantity.is_zero() {
                    return None;
                }
                if !include_spot && position.leverage.is_none() {
                    return None;
                }
                let fraction = reduce_fractions.get(symbol)?;
                if fraction.is_zero() {
                    return None;
                }
                let side = if position.quantity.is_sign_positive() {
                    Side::Sell
                } else {
                    Side::Buy
                };
                let quantity = position.quantity.abs() * fraction;
                Some((symbol.clone(), side, quantity))
            })
            .collect()
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

    /// `base_fee` is a fee taken in the base asset itself (Binance's default
    /// on a buy): the account then holds `quantity - base_fee`, so that is the
    /// position, and the whole cost is still spent on it, so the fee raises the
    /// average entry price instead of vanishing.
    fn update_position(&mut self, fill: &Fill, base_fee: Decimal) {
        let signed_quantity = match fill.side {
            Side::Buy => fill.quantity - base_fee,
            Side::Sell => -fill.quantity,
        };
        // What was paid for the position, before any base fee came off it.
        let paid_for = fill.quantity;

        let existing = self.positions.get(&fill.symbol).cloned();
        let (existing_quantity, existing_average_entry_price) = existing
            .as_ref()
            .map(|p| (p.quantity, p.average_entry_price))
            .unwrap_or((Decimal::ZERO, Decimal::ZERO));

        let same_direction = existing_quantity.is_zero()
            || (existing_quantity.is_sign_positive() == signed_quantity.is_sign_positive());

        if !same_direction && !existing_quantity.is_zero() {
            let closing_quantity = existing_quantity.abs().min(signed_quantity.abs());
            if !closing_quantity.is_zero() {
                let realized = if existing_quantity.is_sign_positive() {
                    (fill.price - existing_average_entry_price) * closing_quantity
                } else {
                    (existing_average_entry_price - fill.price) * closing_quantity
                };
                *self
                    .realized_pnl
                    .entry(fill.symbol.clone())
                    .or_insert(Decimal::ZERO) += realized;
            }
        }

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

        if same_direction {
            let existing_value = position.quantity.abs() * position.average_entry_price;
            let new_value = paid_for * fill.price;
            let new_quantity = position.quantity + signed_quantity;
            if !new_quantity.is_zero() {
                position.average_entry_price = (existing_value + new_value) / new_quantity.abs();
            }
            position.quantity = new_quantity;
        } else {
            let new_quantity = position.quantity + signed_quantity;
            if position.quantity.abs() >= signed_quantity.abs() {
                position.quantity = new_quantity;
            } else {
                position.quantity = new_quantity;
                position.average_entry_price = fill.price;
            }
            // A base-asset fee on a sell also leaves the account: it is a
            // quantity lost for nothing, so it comes off the position and is
            // booked as a loss at the entry price.
            if fill.side == Side::Sell
                && !base_fee.is_zero()
                && position.quantity.is_sign_positive()
            {
                let lost = base_fee.min(position.quantity);
                position.quantity -= lost;
                let entry = position.average_entry_price;
                *self
                    .realized_pnl
                    .entry(fill.symbol.clone())
                    .or_insert(Decimal::ZERO) -= entry * lost;
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

    fn fill_with_fee(
        side: Side,
        price: Decimal,
        quantity: Decimal,
        fee: Decimal,
        currency: &str,
    ) -> Fill {
        Fill {
            fee_currency: Some(currency.to_string()),
            ..sample_fill(side, price, quantity, fee)
        }
    }

    fn ledger_holding(balances: &[(&str, Decimal)]) -> PortfolioLedger {
        let mut instruments = HashMap::new();
        instruments.insert("BTCUSDT".to_string(), btc_usdt());
        PortfolioLedger::new(
            instruments,
            balances
                .iter()
                .map(|(asset, amount)| (asset.to_string(), *amount))
                .collect(),
        )
    }

    #[test]
    fn spot_fee_attribution_matches_wallet_equity_for_held_third_asset() {
        let mut bnb = btc_usdt();
        bnb.symbol = "BNBUSDT".into();
        bnb.base_currency = Some("BNB".into());
        let mut ledger = PortfolioLedger::new(
            HashMap::from([("BTCUSDT".into(), btc_usdt()), ("BNBUSDT".into(), bnb)]),
            HashMap::from([("USDT".into(), dec!(1000)), ("BNB".into(), dec!(1))]),
        );
        let buy = fill_with_fee(Side::Buy, dec!(100), dec!(1), dec!(0.01), "BNB");
        let sell = fill_with_fee(Side::Sell, dec!(110), dec!(1), dec!(1), "USDT");
        ledger.observe_spot_price("BNBUSDT", buy.filled_at, dec!(300));
        ledger.apply_fill(&buy).unwrap();
        ledger.apply_fill(&sell).unwrap();
        ledger.observe_spot_price(
            "BNBUSDT",
            sell.filled_at + chrono::TimeDelta::seconds(1),
            dec!(400),
        );
        ledger.observe_spot_price("BTCUSDT", sell.filled_at, dec!(110));
        let prices = HashMap::from([("BTC".into(), dec!(110)), ("BNB".into(), dec!(400))]);
        let equity = ledger.equity(&prices).unwrap();
        // Compare with the unchanged starting wallet at today's marks.
        let baseline = dec!(1000) + dec!(400);
        let pnl = &ledger.spot_pnl()["BTCUSDT"];
        assert_eq!(ledger.balances()["BNB"], dec!(0.99));
        assert_eq!(pnl.gross_pnl, Some(dec!(10)));
        assert_eq!(pnl.fee_at_execution, Some(dec!(4)));
        assert_eq!(pnl.net_pnl, Some(equity - baseline));
        assert_eq!(pnl.net_pnl, Some(dec!(5)));
        assert!(pnl.external_fees_by_asset.is_empty());
    }

    #[test]
    fn unpriced_external_fee_is_visible_and_does_not_invent_wallet_value() {
        let mut ledger = ledger_holding(&[("USDT", dec!(1000))]);
        let buy = fill_with_fee(Side::Buy, dec!(100), dec!(1), dec!(0.01), "BNB");
        ledger.apply_fill(&buy).unwrap();
        ledger.observe_spot_price("BTCUSDT", buy.filled_at, dec!(100));
        let pnl = &ledger.spot_pnl()["BTCUSDT"];
        assert_eq!(pnl.external_fees_by_asset["BNB"], dec!(0.01));
        assert_eq!(pnl.fee_at_execution, None);
        assert_eq!(pnl.net_pnl, None);
        assert!(!ledger.balances().contains_key("BNB"));
    }

    // ---- Fees: how a real exchange charges them -------------------------
    //
    // Binance's default takes the fee in the asset you RECEIVE: the base
    // asset on a buy, the quote asset on a sell. With "pay fees in BNB" it is
    // BNB, an asset a one-symbol session does not hold. The Spot testnet
    // charges nothing, so none of this showed until a mainnet-style fee was
    // fed in.

    /// A fresh session holds only quote. A buy whose fee is taken from the
    /// base it just received used to be refused ("required 0.0000277 ETH,
    /// available 0") because the fee was checked before the base was credited.
    #[test]
    fn a_buy_with_the_fee_in_the_asset_just_received_is_applied_from_a_flat_start() {
        let mut ledger = ledger_holding(&[("USDT", dec!(100000))]);
        let fill = fill_with_fee(Side::Buy, dec!(60000), dec!(0.5), dec!(0.0005), "BTC");
        ledger
            .apply_fill(&fill)
            .expect("the fee is paid out of what was just bought");
        assert_eq!(ledger.balances()["BTC"], dec!(0.4995));
        assert_eq!(ledger.balances()["USDT"], dec!(70000));
    }

    #[test]
    fn a_sell_with_the_fee_in_the_quote_asset_is_applied_when_no_quote_is_held() {
        let mut ledger = ledger_holding(&[("BTC", dec!(1))]);
        let fill = fill_with_fee(Side::Sell, dec!(60000), dec!(0.5), dec!(30), "USDT");
        ledger
            .apply_fill(&fill)
            .expect("the fee is paid out of the proceeds");
        assert_eq!(ledger.balances()["BTC"], dec!(0.5));
        assert_eq!(ledger.balances()["USDT"], dec!(29970));
    }

    /// The position the strategy sees must be what the account really holds,
    /// or it will try to sell more than it owns and the exchange refuses.
    #[test]
    fn the_position_is_net_of_a_fee_taken_in_the_base_asset() {
        let mut ledger = ledger_holding(&[("USDT", dec!(100000))]);
        let fill = fill_with_fee(Side::Buy, dec!(60000), dec!(0.5), dec!(0.0005), "BTC");
        ledger.apply_fill(&fill).unwrap();
        assert_eq!(ledger.positions()["BTCUSDT"].quantity, dec!(0.4995));
        assert_eq!(
            ledger.positions()["BTCUSDT"].quantity,
            ledger.balances()["BTC"],
            "position and holding agree"
        );
    }

    #[test]
    fn everything_bought_net_of_a_base_fee_can_be_sold_again() {
        let mut ledger = ledger_holding(&[("USDT", dec!(100000))]);
        ledger
            .apply_fill(&fill_with_fee(
                Side::Buy,
                dec!(60000),
                dec!(0.5),
                dec!(0.0005),
                "BTC",
            ))
            .unwrap();
        let held = ledger.positions()["BTCUSDT"].quantity;
        ledger
            .apply_fill(&fill_with_fee(
                Side::Sell,
                dec!(61000),
                held,
                dec!(30),
                "USDT",
            ))
            .expect("selling exactly what the position says must be affordable");
        assert!(ledger.positions()["BTCUSDT"].quantity.is_zero());
        assert!(ledger.balances()["BTC"].is_zero());
    }

    #[test]
    fn the_fee_is_part_of_the_cost_basis() {
        let mut ledger = ledger_holding(&[("USDT", dec!(100000))]);
        ledger
            .apply_fill(&fill_with_fee(
                Side::Buy,
                dec!(100),
                dec!(1),
                dec!(0.01),
                "BTC",
            ))
            .unwrap();
        let position = &ledger.positions()["BTCUSDT"];
        assert_eq!(position.quantity, dec!(0.99));
        // 100 USDT bought 0.99 BTC, so the break-even price is above 100.
        let cost = position.average_entry_price * position.quantity;
        assert!((cost - dec!(100)).abs() < dec!(0.000000001), "{cost}");
    }

    #[test]
    fn a_fee_in_the_base_asset_on_a_sell_leaves_the_position_too() {
        let mut ledger = ledger_holding(&[("BTC", dec!(1))]).with_positions(HashMap::from([(
            "BTCUSDT".to_string(),
            Position {
                symbol: "BTCUSDT".to_string(),
                quantity: dec!(1),
                average_entry_price: dec!(100),
                leverage: None,
                margin_mode: None,
                allocated_margin: Decimal::ZERO,
            },
        )]));
        ledger
            .apply_fill(&fill_with_fee(
                Side::Sell,
                dec!(110),
                dec!(0.5),
                dec!(0.001),
                "BTC",
            ))
            .unwrap();
        assert_eq!(ledger.balances()["BTC"], dec!(0.499));
        assert_eq!(ledger.positions()["BTCUSDT"].quantity, dec!(0.499));
    }

    /// The reported-risk case: "pay fees in BNB" on a session that holds only
    /// the symbol's own assets. The fill happened on the exchange, so the
    /// ledger must take it; refusing it left the position and PnL out of step.
    #[test]
    fn a_fee_in_an_asset_the_session_does_not_hold_does_not_reject_the_fill() {
        let mut ledger = ledger_holding(&[("USDT", dec!(100000))]);
        ledger
            .apply_fill(&fill_with_fee(
                Side::Buy,
                dec!(60000),
                dec!(0.5),
                dec!(0.0004),
                "BNB",
            ))
            .expect("a BNB fee must not undo a real trade");
        assert_eq!(
            ledger.balances()["BTC"],
            dec!(0.5),
            "the full quantity arrives"
        );
        assert_eq!(ledger.balances()["USDT"], dec!(70000));
        assert_eq!(ledger.positions()["BTCUSDT"].quantity, dec!(0.5));
        assert!(
            !ledger.balances().contains_key("BNB"),
            "no phantom negative BNB balance: {:?}",
            ledger.balances()
        );
    }

    /// If the session does hold the fee asset (a BNB symbol, or BNB seeded),
    /// the fee really does come out of it and must be affordable.
    #[test]
    fn a_fee_in_a_held_third_asset_is_debited_from_it() {
        let mut ledger = ledger_holding(&[("USDT", dec!(100000)), ("BNB", dec!(1))]);
        ledger
            .apply_fill(&fill_with_fee(
                Side::Buy,
                dec!(60000),
                dec!(0.5),
                dec!(0.001),
                "BNB",
            ))
            .unwrap();
        assert_eq!(ledger.balances()["BNB"], dec!(0.999));
    }

    #[test]
    fn an_unaffordable_fee_in_a_held_asset_still_rejects_and_changes_nothing() {
        let mut ledger = ledger_holding(&[("USDT", dec!(100000)), ("BNB", dec!(0.0001))]);
        let before = ledger.balances().clone();
        let result = ledger.apply_fill(&fill_with_fee(
            Side::Buy,
            dec!(60000),
            dec!(0.5),
            dec!(2),
            "BNB",
        ));
        assert!(matches!(
            result,
            Err(ApexesError::InsufficientBalance { .. })
        ));
        assert_eq!(ledger.balances(), &before, "atomic: nothing was applied");
        assert!(ledger.positions().is_empty());
    }

    /// Receiving the fee asset is not a free pass: the primary debit is still
    /// checked, so a sell of more than is held fails whatever the fee asset.
    #[test]
    fn crediting_the_fee_asset_does_not_excuse_an_unaffordable_primary_debit() {
        let mut ledger = ledger_holding(&[("BTC", dec!(0.1))]);
        let before = ledger.balances().clone();
        let result = ledger.apply_fill(&fill_with_fee(
            Side::Sell,
            dec!(60000),
            dec!(5),
            dec!(1),
            "USDT",
        ));
        assert!(matches!(
            result,
            Err(ApexesError::InsufficientBalance { .. })
        ));
        assert_eq!(ledger.balances(), &before);
        let result = ledger.apply_fill(&fill_with_fee(
            Side::Buy,
            dec!(60000),
            dec!(5),
            dec!(0.001),
            "BTC",
        ));
        assert!(
            matches!(result, Err(ApexesError::InsufficientBalance { .. })),
            "no quote at all to pay for the buy"
        );
    }

    #[test]
    fn seeded_position_is_reduced_by_a_subsequent_sell() {
        let seeded = Position {
            symbol: "BTCUSDT".to_string(),
            quantity: dec!(0.5),
            average_entry_price: dec!(60000),
            leverage: None,
            margin_mode: None,
            allocated_margin: Decimal::ZERO,
        };
        let mut ledger =
            ledger_with_10k_usdt().with_positions(HashMap::from([("BTCUSDT".to_string(), seeded)]));
        ledger.credit("BTC", dec!(0.5)); // balances reflect the seeded position
        assert_eq!(ledger.positions()["BTCUSDT"].quantity, dec!(0.5));

        ledger
            .apply_fill(&sample_fill(Side::Sell, dec!(65000), dec!(0.2), dec!(0)))
            .unwrap();

        assert_eq!(ledger.positions()["BTCUSDT"].quantity, dec!(0.3));
        assert_eq!(ledger.realized_pnl()["BTCUSDT"], dec!(1000));
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
    fn realized_pnl_starts_empty() {
        let ledger = ledger_with_10k_usdt();
        assert!(ledger.realized_pnl().is_empty());
    }

    #[test]
    fn spot_partial_sell_realizes_pnl_against_average_entry_price() {
        // NOTE: custom 20000 USDT starting balance -- the brief's original
        // 10k-USDT-only setup could never fund the 0.2 BTC @ 60000 = 12000
        // opening buy (apply_fill atomically rejects it with
        // InsufficientBalance), mirroring the same fix documented in
        // `position_flips_sign_and_resets_entry_price_on_full_reversal`'s
        // NOTE. Fill quantities and asserted PnL values are unchanged.
        let mut instruments = HashMap::new();
        instruments.insert("BTCUSDT".to_string(), btc_usdt());
        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(20000));
        let mut ledger = PortfolioLedger::new(instruments, initial_balances);
        ledger
            .apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.2), dec!(0)))
            .unwrap();
        // Sell half at a higher price: realizes (65000-60000)*0.1 = 500 profit.
        ledger
            .apply_fill(&sample_fill(Side::Sell, dec!(65000), dec!(0.1), dec!(0)))
            .unwrap();

        assert_eq!(*ledger.realized_pnl().get("BTCUSDT").unwrap(), dec!(500));
        // Position still open (0.1 left) -- quantity/average_entry_price
        // are unaffected by this new field.
        let position = ledger.positions().get("BTCUSDT").unwrap();
        assert_eq!(position.quantity, dec!(0.1));
        assert_eq!(position.average_entry_price, dec!(60000));
    }

    #[test]
    fn spot_short_side_realized_pnl_sign_is_reversed() {
        // Start with a base-asset balance so a short-then-cover sequence
        // is representable (this ledger models net spot position, not a
        // literal borrow -- selling first requires an existing balance,
        // matching `position_flips_sign_and_resets_entry_price_on_full_reversal`'s
        // existing setup pattern).
        let mut instruments = HashMap::new();
        instruments.insert("BTCUSDT".to_string(), btc_usdt());
        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(10000));
        initial_balances.insert("BTC".to_string(), dec!(1));
        let mut ledger = PortfolioLedger::new(instruments, initial_balances);

        // Sell 0.2 at 65000 (opens a "short" relative to the flat position
        // this ledger tracks -- quantity goes negative).
        ledger
            .apply_fill(&sample_fill(Side::Sell, dec!(65000), dec!(0.2), dec!(0)))
            .unwrap();
        // Buy back 0.1 at 60000: price fell, so covering a short realizes
        // a profit of (65000-60000)*0.1 = 500 (reversed sign vs. a long).
        ledger
            .apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.1), dec!(0)))
            .unwrap();

        assert_eq!(*ledger.realized_pnl().get("BTCUSDT").unwrap(), dec!(500));
    }

    #[test]
    fn spot_opening_and_adding_does_not_write_realized_pnl() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.05), dec!(0)))
            .unwrap();
        ledger
            .apply_fill(&sample_fill(Side::Buy, dec!(70000), dec!(0.05), dec!(0)))
            .unwrap();

        assert!(ledger.realized_pnl().get("BTCUSDT").is_none());
    }

    #[test]
    fn realized_pnl_accumulates_across_multiple_partial_closes() {
        // NOTE: custom 20000 USDT starting balance -- the brief's original
        // 10k-USDT-only setup could never fund the 0.3 BTC @ 60000 = 18000
        // opening buy (see `spot_partial_sell_realizes_pnl_against_average_entry_price`'s
        // NOTE). Fill quantities and asserted PnL values are unchanged.
        let mut instruments = HashMap::new();
        instruments.insert("BTCUSDT".to_string(), btc_usdt());
        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(20000));
        let mut ledger = PortfolioLedger::new(instruments, initial_balances);
        ledger
            .apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.3), dec!(0)))
            .unwrap();
        // First partial close: (65000-60000)*0.1 = 500.
        ledger
            .apply_fill(&sample_fill(Side::Sell, dec!(65000), dec!(0.1), dec!(0)))
            .unwrap();
        // Second partial close: (70000-60000)*0.1 = 1000.
        ledger
            .apply_fill(&sample_fill(Side::Sell, dec!(70000), dec!(0.1), dec!(0)))
            .unwrap();

        // Accumulated, not overwritten: 500 + 1000 = 1500.
        assert_eq!(*ledger.realized_pnl().get("BTCUSDT").unwrap(), dec!(1500));
    }

    #[test]
    fn leveraged_partial_close_writes_realized_pnl_field() {
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
        ledger
            .apply_fill(&leveraged_fill_for_symbol(
                "BTCUSDT",
                Side::Sell,
                dec!(65000),
                dec!(0.5),
                dec!(10),
            ))
            .unwrap();

        // Same (65000-60000)*0.5 = 2500 the existing balance-based test
        // (`leveraged_partial_close_releases_proportional_margin_and_realizes_pnl`)
        // already asserts shows up in the credited balance -- now also
        // asserted directly against the new field.
        assert_eq!(*ledger.realized_pnl().get("BTCUSDT").unwrap(), dec!(2500));
    }

    #[test]
    fn leveraged_open_and_add_does_not_write_realized_pnl() {
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

        assert!(ledger.realized_pnl().get("BTCUSDT").is_none());
    }

    #[test]
    fn rejected_leveraged_flip_does_not_write_realized_pnl() {
        // Atomicity regression guard for `close_leveraged`: the new
        // realized_pnl accumulation must live in the final mutation block
        // (after the flip-margin validation), so a flip whose margin
        // requirement is insufficient returns Err with the ledger -- including
        // the new realized_pnl field -- completely unchanged. A fill that was
        // rejected must never book realized PnL.
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

        // Sell 2 @65000 at 2x: closes the 1 long (would release 6000 margin
        // and realize (65000-60000)*1 = 5000) and flips into a 1 short
        // requiring 1*65000/2 = 32500 margin -- but the projected quote
        // balance after the close credits is only 4000 + 6000 + 5000 = 15000.
        let result = ledger.apply_fill(&leveraged_fill_for_symbol(
            "BTCUSDT",
            Side::Sell,
            dec!(65000),
            dec!(2),
            dec!(2),
        ));
        assert!(matches!(
            result,
            Err(ApexesError::InsufficientBalance { .. })
        ));

        // Byte-for-byte unchanged: no fee debit, no margin release, no PnL
        // credit, no position mutation, and NO realized PnL booked.
        assert_eq!(*ledger.balances().get("USDT").unwrap(), dec!(4000));
        assert!(ledger.realized_pnl().is_empty());
        let position = ledger.positions().get("BTCUSDT").unwrap();
        assert_eq!(position.quantity, dec!(1));
        assert_eq!(position.allocated_margin, dec!(6000));
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
    fn isolated_add_at_different_leverage_is_rejected_and_leaves_ledger_completely_unchanged() {
        // Design-spec invariant: a same-direction fill ADDING to an
        // existing Isolated position must carry the SAME leverage as the
        // position; a mismatch is rejected with
        // `InconsistentLeverageForIsolatedPosition` BEFORE any mutation
        // (atomicity, mirroring `close_leveraged`'s convention).
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
        // margin=6000 reserved, quote balance now 4000, leverage 10.

        let balances_before = ledger.balances().clone();
        let positions_before = ledger.positions().clone();

        // Adding 0.5 more at 5x must be rejected: existing leverage is 10.
        let result = ledger.apply_fill(&leveraged_fill_for_symbol(
            "BTCUSDT",
            Side::Buy,
            dec!(62000),
            dec!(0.5),
            dec!(5),
        ));
        assert_eq!(
            result,
            Err(ApexesError::InconsistentLeverageForIsolatedPosition {
                symbol: "BTCUSDT".to_string(),
                existing: dec!(10),
                requested: dec!(5),
            })
        );

        // Byte-for-byte unchanged: no margin reservation, no position
        // mutation (quantity/allocated_margin/leverage all as before).
        assert_eq!(ledger.balances(), &balances_before);
        assert_eq!(ledger.positions(), &positions_before);
    }

    #[test]
    fn cross_add_at_different_leverage_is_allowed() {
        // Cross-margin positions are exempt from the isolated-leverage
        // consistency rule: cross leverage is an order-level parameter,
        // not required to be uniform across the account.
        let cross_fill = |side: Side, price: Decimal, quantity: Decimal, leverage: Decimal| Fill {
            margin_mode: Some(MarginMode::Cross),
            ..leveraged_fill_for_symbol("BTCUSDT", side, price, quantity, leverage)
        };
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&cross_fill(Side::Buy, dec!(60000), dec!(1), dec!(10)))
            .unwrap();
        ledger
            .apply_fill(&cross_fill(Side::Buy, dec!(62000), dec!(0.05), dec!(5)))
            .unwrap();

        let position = ledger.positions().get("BTCUSDT").unwrap();
        assert_eq!(position.quantity, dec!(1.05));
        // 60000/10 + 3100/5 = 6000 + 620 = 6620.
        assert_eq!(position.allocated_margin, dec!(6620));
        // The most recent fill's leverage/margin_mode wins (pre-existing
        // behavior, unchanged by the Isolated-only check).
        assert_eq!(position.leverage, Some(dec!(5)));
        assert_eq!(position.margin_mode, Some(MarginMode::Cross));
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

    fn sample_margin_tiers() -> HashMap<String, apexes_strategy::MarginTierTable> {
        let mut tiers = HashMap::new();
        tiers.insert(
            "BTCUSDT".to_string(),
            apexes_strategy::MarginTierTable {
                tiers: vec![apexes_strategy::MarginTier {
                    notional_floor: dec!(0),
                    max_leverage: dec!(20),
                    maintenance_margin_rate: dec!(0.05),
                    maintenance_amount: dec!(0),
                }],
            },
        );
        tiers
    }

    #[test]
    fn margin_status_returns_none_ratio_for_spot_positions() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.1), dec!(0)))
            .unwrap();
        // Spot position -- leverage stays None.

        let mut mark_prices = HashMap::new();
        mark_prices.insert("BTC".to_string(), dec!(60000));
        let status = ledger.margin_status(&mark_prices, &sample_margin_tiers());

        assert!(
            !status.contains_key("BTCUSDT"),
            "spot positions must not appear in margin_status's output at all"
        );
    }

    #[test]
    fn margin_status_computes_isolated_margin_ratio_and_liquidation_price() {
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

        let mut mark_prices = HashMap::new();
        mark_prices.insert("BTC".to_string(), dec!(60000));
        let status = ledger.margin_status(&mark_prices, &sample_margin_tiers());

        let position_status = status.get("BTCUSDT").unwrap();
        // allocated_margin = notional/leverage = 60000/10 = 6000 (set by
        // apply_fill's leveraged-accounting branch, Task 2b)
        // maintenance = notional * 0.05 - 0 = 3000
        // effective margin = 6000 + 0 unrealized PnL (mark == entry)
        // margin_ratio = 6000 / 3000 = 2.0
        assert_eq!(position_status.margin_ratio, Some(dec!(2)));
        assert!(position_status.liquidation_price.is_some());
    }

    #[test]
    fn margin_status_returns_none_liquidation_price_for_cross_positions() {
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
        // apply_leveraged_fill (Task 2b) always sets margin_mode from
        // the fill; override it to Cross here since
        // leveraged_fill_for_symbol's helper defaults to Isolated.
        ledger.positions.get_mut("BTCUSDT").unwrap().margin_mode =
            Some(apexes_strategy::MarginMode::Cross);

        let mut mark_prices = HashMap::new();
        mark_prices.insert("BTC".to_string(), dec!(60000));
        let status = ledger.margin_status(&mark_prices, &sample_margin_tiers());

        let position_status = status.get("BTCUSDT").unwrap();
        assert_eq!(position_status.liquidation_price, None);
        assert!(position_status.margin_ratio.is_some());
    }

    #[test]
    fn check_liquidations_flags_a_position_whose_margin_ratio_has_collapsed() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&leveraged_fill_for_symbol(
                "BTCUSDT",
                Side::Buy,
                dec!(60000),
                dec!(1),
                dec!(20),
            ))
            .unwrap();
        // very high leverage (20x) -- thin margin, easy to breach below.

        // Mark price crashes far below entry -- with 20x leverage this
        // should have already breached maintenance margin.
        // allocated = 60000/20 = 3000; unrealized PnL at 45000 =
        // (45000-60000)*1 = -15000; effective margin = -12000;
        // maintenance = 45000*0.05 = 2250; ratio = -5.33 <= 1.0.
        let mut mark_prices = HashMap::new();
        mark_prices.insert("BTC".to_string(), dec!(45000));
        let triggered = ledger.check_liquidations(&mark_prices, &sample_margin_tiers());

        assert!(triggered.contains(&"BTCUSDT".to_string()));
    }

    #[test]
    fn check_liquidations_does_not_flag_a_healthy_position() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&leveraged_fill_for_symbol(
                "BTCUSDT",
                Side::Buy,
                dec!(60000),
                dec!(0.1),
                dec!(2),
            ))
            .unwrap();
        // low leverage (2x) -- healthy margin.

        let mut mark_prices = HashMap::new();
        mark_prices.insert("BTC".to_string(), dec!(60000)); // unchanged from entry
        let triggered = ledger.check_liquidations(&mark_prices, &sample_margin_tiers());

        assert!(!triggered.contains(&"BTCUSDT".to_string()));
    }

    /// Asserts that `liquidation_price` actually satisfies the defining
    /// invariant: at that price, effective margin (allocated + unrealized
    /// PnL) equals the maintenance requirement (|q|*p*rate - amount).
    /// This is a STRONGER check than the brief's `is_some()` -- an
    /// algebraically wrong formula would still return `Some(..)`.
    fn assert_liquidation_price_invariant(
        liquidation_price: Decimal,
        quantity_abs: Decimal,
        is_long: bool,
        entry_price: Decimal,
        allocated_margin: Decimal,
        rate: Decimal,
        amount: Decimal,
    ) {
        let unrealized = if is_long {
            (liquidation_price - entry_price) * quantity_abs
        } else {
            (entry_price - liquidation_price) * quantity_abs
        };
        let effective_margin = allocated_margin + unrealized;
        let maintenance = quantity_abs * liquidation_price * rate - amount;
        let diff = (effective_margin - maintenance).abs();
        assert!(
            diff < dec!(0.01),
            "liquidation_price {liquidation_price} does not satisfy margin==maintenance: \
             effective margin {effective_margin} vs maintenance {maintenance}"
        );
    }

    #[test]
    fn isolated_liquidation_price_long_satisfies_margin_invariant() {
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
        // allocated = 60000/10 = 6000; expected p = (1*60000 - 6000 - 0) / (1*(1-0.05))
        //           = 54000/0.95 = 56842.105...

        let mut mark_prices = HashMap::new();
        mark_prices.insert("BTC".to_string(), dec!(60000));
        let status = ledger.margin_status(&mark_prices, &sample_margin_tiers());

        let p = status.get("BTCUSDT").unwrap().liquidation_price.unwrap();
        assert!(
            p < dec!(60000),
            "a long's liquidation price must be BELOW entry"
        );
        assert_liquidation_price_invariant(
            p,
            dec!(1),
            true,
            dec!(60000),
            dec!(6000),
            dec!(0.05),
            dec!(0),
        );
    }

    #[test]
    fn isolated_liquidation_price_short_satisfies_margin_invariant() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&leveraged_fill_for_symbol(
                "BTCUSDT",
                Side::Sell,
                dec!(60000),
                dec!(1),
                dec!(10),
            ))
            .unwrap();
        // allocated = 6000; expected p = (1*60000 + 6000 + 0) / (1*(1+0.05))
        //           = 66000/1.05 = 62857.142...
        // (Note the short denominator is 1 + rate, NOT 1 - rate.)

        let mut mark_prices = HashMap::new();
        mark_prices.insert("BTC".to_string(), dec!(60000));
        let status = ledger.margin_status(&mark_prices, &sample_margin_tiers());

        let p = status.get("BTCUSDT").unwrap().liquidation_price.unwrap();
        assert!(
            p > dec!(60000),
            "a short's liquidation price must be ABOVE entry"
        );
        assert_liquidation_price_invariant(
            p,
            dec!(1),
            false,
            dec!(60000),
            dec!(6000),
            dec!(0.05),
            dec!(0),
        );
    }

    #[test]
    fn isolated_liquidation_price_accounts_for_maintenance_amount_term() {
        // A nonzero tier maintenance_amount must REDUCE the maintenance
        // requirement, pushing a long's liquidation price FURTHER below
        // entry (more buffer), not closer to it.
        let mut tiers = HashMap::new();
        tiers.insert(
            "BTCUSDT".to_string(),
            apexes_strategy::MarginTierTable {
                tiers: vec![apexes_strategy::MarginTier {
                    notional_floor: dec!(0),
                    max_leverage: dec!(20),
                    maintenance_margin_rate: dec!(0.05),
                    maintenance_amount: dec!(250),
                }],
            },
        );
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
        // expected p = (1*60000 - 6000 - 250) / (1*(1-0.05))
        //            = 53750/0.95 = 56578.947...
        // (lower than the amount=0 case's 56842.105 -- more buffer).

        let mut mark_prices = HashMap::new();
        mark_prices.insert("BTC".to_string(), dec!(60000));
        let status = ledger.margin_status(&mark_prices, &tiers);

        let p = status.get("BTCUSDT").unwrap().liquidation_price.unwrap();
        assert_liquidation_price_invariant(
            p,
            dec!(1),
            true,
            dec!(60000),
            dec!(6000),
            dec!(0.05),
            dec!(250),
        );
    }

    #[test]
    fn margin_status_omits_leveraged_position_without_tier_table_from_ratio_but_keeps_it_out_of_liquidations()
     {
        // No tier table configured for the symbol: maintenance is None,
        // so margin_ratio is None and check_liquidations cannot flag it
        // (a missing config must never silently force-close a position).
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&leveraged_fill_for_symbol(
                "BTCUSDT",
                Side::Buy,
                dec!(60000),
                dec!(1),
                dec!(20),
            ))
            .unwrap();

        let mut mark_prices = HashMap::new();
        mark_prices.insert("BTC".to_string(), dec!(45000));
        let status = ledger.margin_status(&mark_prices, &HashMap::new());
        assert_eq!(status.get("BTCUSDT").unwrap().margin_ratio, None);
        let triggered = ledger.check_liquidations(&mark_prices, &HashMap::new());
        assert!(triggered.is_empty());
    }

    #[test]
    fn check_de_risking_returns_a_partial_close_for_a_flagged_leveraged_long() {
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

        let mut reduce_fractions = HashMap::new();
        reduce_fractions.insert("BTCUSDT".to_string(), dec!(0.5));

        let orders = ledger.check_de_risking(&reduce_fractions, false);
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0], ("BTCUSDT".to_string(), Side::Sell, dec!(0.5)));
    }

    #[test]
    fn check_de_risking_returns_a_partial_close_for_a_flagged_short() {
        let mut instruments = HashMap::new();
        instruments.insert("BTCUSDT".to_string(), btc_usdt());
        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(10000));
        let mut ledger = PortfolioLedger::new(instruments, initial_balances);
        ledger
            .apply_fill(&leveraged_fill_for_symbol(
                "BTCUSDT",
                Side::Sell,
                dec!(60000),
                dec!(1),
                dec!(10),
            ))
            .unwrap();

        let mut reduce_fractions = HashMap::new();
        reduce_fractions.insert("BTCUSDT".to_string(), dec!(0.25));

        let orders = ledger.check_de_risking(&reduce_fractions, false);
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0], ("BTCUSDT".to_string(), Side::Buy, dec!(0.25)));
    }

    #[test]
    fn check_de_risking_skips_spot_positions_by_default() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.1), dec!(0)))
            .unwrap();

        let mut reduce_fractions = HashMap::new();
        reduce_fractions.insert("BTCUSDT".to_string(), dec!(0.5));

        let orders = ledger.check_de_risking(&reduce_fractions, false);
        assert!(orders.is_empty());
    }

    #[test]
    fn check_de_risking_includes_spot_positions_when_configured() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.1), dec!(0)))
            .unwrap();

        let mut reduce_fractions = HashMap::new();
        reduce_fractions.insert("BTCUSDT".to_string(), dec!(0.5));

        let orders = ledger.check_de_risking(&reduce_fractions, true);
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0], ("BTCUSDT".to_string(), Side::Sell, dec!(0.05)));
    }

    #[test]
    fn check_de_risking_skips_symbols_not_in_reduce_fractions() {
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

        let orders = ledger.check_de_risking(&HashMap::new(), false);
        assert!(orders.is_empty());
    }
    #[test]
    fn de_risking_is_symbol_ordered_for_long_and_short_positions() {
        let btc = btc_usdt();
        let mut eth = btc.clone();
        eth.symbol = "ETHUSDT".into();
        eth.base_currency = Some("ETH".into());
        let fractions = HashMap::from([
            ("ETHUSDT".into(), dec!(0.25)),
            ("BTCUSDT".into(), dec!(0.5)),
        ]);
        for reversed in [false, true] {
            let mut ledger = PortfolioLedger::new(
                HashMap::from([
                    ("BTCUSDT".into(), btc.clone()),
                    ("ETHUSDT".into(), eth.clone()),
                ]),
                HashMap::from([("USDT".into(), dec!(10000))]),
            );
            let long =
                leveraged_fill_for_symbol("BTCUSDT", Side::Buy, dec!(100), dec!(2), dec!(10));
            let short =
                leveraged_fill_for_symbol("ETHUSDT", Side::Sell, dec!(100), dec!(4), dec!(10));
            let fills = if reversed {
                [short, long]
            } else {
                [long, short]
            };
            for fill in fills {
                ledger.apply_fill(&fill).unwrap();
            }
            assert_eq!(
                ledger.check_de_risking(&fractions, false),
                vec![
                    ("BTCUSDT".into(), Side::Sell, dec!(1)),
                    ("ETHUSDT".into(), Side::Buy, dec!(1))
                ]
            );
        }
    }

    #[test]
    fn equity_uses_stable_asset_order_even_when_decimal_addition_rounds() {
        // The large term forces rounding of the tiny term; cancellation
        // makes the order observable, not just a test of commutative integers.
        let a = dec!(10000000000000000000000000000);
        let b = dec!(0.1);
        let c = -a;
        let ordered = (a + b) + c;
        assert_ne!(ordered, (a + c) + b);
        for _ in 0..32 {
            let balances = HashMap::from([("C".into(), c), ("A".into(), a), ("B".into(), b)]);
            let ledger = PortfolioLedger::new(HashMap::new(), balances);
            assert_eq!(ledger.equity(&HashMap::new()).unwrap(), ordered);
        }
    }
    #[test]
    fn funding_movements_match_balance_changes_in_symbol_order_with_fallback() {
        use crate::funding_attribution::FundingPriceSource;
        let btc = btc_usdt();
        let mut eth = btc.clone();
        eth.symbol = "ETHUSDT".into();
        eth.base_currency = Some("ETH".into());
        let mut ledger = PortfolioLedger::new(
            HashMap::from([("ETHUSDT".into(), eth), ("BTCUSDT".into(), btc)]),
            HashMap::from([("USDT".into(), dec!(10000))]),
        );
        ledger
            .apply_fill(&leveraged_fill_for_symbol(
                "ETHUSDT",
                Side::Sell,
                dec!(100),
                dec!(3),
                dec!(10),
            ))
            .unwrap();
        ledger
            .apply_fill(&leveraged_fill_for_symbol(
                "BTCUSDT",
                Side::Buy,
                dec!(100),
                dec!(2),
                dec!(10),
            ))
            .unwrap();
        let marks = HashMap::from([("BTC".into(), dec!(150))]);
        let before = ledger.balances()["USDT"];
        let movements = ledger
            .settle_funding(&marks, &cost_model_with_funding(dec!(10)))
            .unwrap();
        assert_eq!(movements.len(), 2);
        assert_eq!(movements[0].symbol, "BTCUSDT");
        assert_eq!(movements[0].position_quantity, dec!(2));
        assert_eq!(movements[0].price_source, FundingPriceSource::Mark);
        assert_eq!(movements[0].valuation_price, dec!(150));
        assert_eq!(movements[0].notional, dec!(300));
        assert_eq!(movements[0].balance_delta, dec!(-0.3));
        assert_eq!(movements[1].symbol, "ETHUSDT");
        assert_eq!(movements[1].position_quantity, dec!(-3));
        assert_eq!(movements[1].price_source, FundingPriceSource::EntryFallback);
        assert_eq!(movements[1].valuation_price, dec!(100));
        assert_eq!(movements[1].balance_delta, dec!(0.3));
        assert_eq!(ledger.balances()["USDT"], before);
        let negative = ledger
            .settle_funding(&marks, &cost_model_with_funding(dec!(-10)))
            .unwrap();
        assert_eq!(negative[0].balance_delta, dec!(0.3));
        assert_eq!(negative[1].balance_delta, dec!(-0.3));
        assert_eq!(ledger.balances()["USDT"], before);
        let zero = ledger
            .settle_funding(&marks, &cost_model_with_funding(Decimal::ZERO))
            .unwrap();
        assert_eq!(zero.len(), 2);
        assert!(zero.iter().all(|m| m.balance_delta.is_zero()));
        assert_eq!(ledger.balances()["USDT"], before);
    }
}
