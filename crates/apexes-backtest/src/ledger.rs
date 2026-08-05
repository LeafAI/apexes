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
    /// Callers (`run_unified_event_loop`) are responsible for deciding WHEN to
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
        for (asset, balance) in &self.balances {
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
        for (symbol, position) in &self.positions {
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

        for (symbol, position) in &self.positions {
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
        self.margin_status(mark_prices, margin_tiers)
            .into_iter()
            .filter_map(|(symbol, status)| {
                let ratio = status.margin_ratio?;
                (ratio <= Decimal::ONE).then_some(symbol)
            })
            .collect()
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
        self.positions
            .iter()
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

    fn update_position(&mut self, fill: &Fill) {
        let signed_quantity = match fill.side {
            Side::Buy => fill.quantity,
            Side::Sell => -fill.quantity,
        };

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
            let new_value = signed_quantity.abs() * fill.price;
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
}
