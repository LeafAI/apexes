//! F03.05: realized closing-fill economics in explicit quote units.
use apexes_core::{AssetClass, Fill, Instrument, Side};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum SymbolExecutionPerformance {
    Available(Box<ClosingPerformance>),
    Unavailable { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClosingPerformance {
    pub quote_currency: String,
    pub turnover: Decimal,
    pub fees_paid: Decimal,
    pub gross_realized: Decimal,
    /// Net of allocated entry and exit fees; excludes funding.
    pub net_realized: Decimal,
    pub closing_count: u64,
    pub wins: u64,
    pub losses: u64,
    pub breakevens: u64,
    pub net_winning_pnl: Decimal,
    /// Positive magnitude.
    pub net_losing_pnl: Decimal,
    pub win_rate: Option<Decimal>,
    pub expectancy_per_close: Option<Decimal>,
    pub profit_factor: Option<Decimal>,
    pub open_quantity: Decimal,
    pub unallocated_entry_fees: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionPerformance {
    pub per_symbol: BTreeMap<String, SymbolExecutionPerformance>,
}

#[derive(Default)]
struct Accumulator {
    qty: Decimal,
    entry: Decimal,
    entry_fees: Decimal,
    turnover: Decimal,
    fees: Decimal,
    gross: Decimal,
    net: Decimal,
    wins: u64,
    losses: u64,
    breakevens: u64,
    profit: Decimal,
    loss: Decimal,
}

impl Accumulator {
    fn apply(&mut self, fill: &Fill) -> Option<()> {
        let signed = match fill.side {
            Side::Buy => fill.quantity,
            Side::Sell => -fill.quantity,
        };
        self.turnover = self
            .turnover
            .checked_add(fill.price.checked_mul(fill.quantity)?)?;
        self.fees = self.fees.checked_add(fill.fee)?;
        let opposite =
            !self.qty.is_zero() && self.qty.is_sign_positive() != signed.is_sign_positive();
        if !opposite {
            let next = self.qty.checked_add(signed)?;
            self.entry = self
                .qty
                .abs()
                .checked_mul(self.entry)?
                .checked_add(fill.quantity.checked_mul(fill.price)?)?
                .checked_div(next.abs())?;
            self.entry_fees = self.entry_fees.checked_add(fill.fee)?;
            self.qty = next;
            return Some(());
        }
        self.close(fill, signed)
    }

    fn close(&mut self, fill: &Fill, signed: Decimal) -> Option<()> {
        let closed = self.qty.abs().min(fill.quantity);
        let opening_fee = if closed == self.qty.abs() {
            self.entry_fees
        } else {
            self.entry_fees
                .checked_mul(closed.checked_div(self.qty.abs())?)?
        };
        let exit_fee = if closed == fill.quantity {
            fill.fee
        } else {
            fill.fee.checked_mul(closed.checked_div(fill.quantity)?)?
        };
        let change = if self.qty.is_sign_positive() {
            fill.price.checked_sub(self.entry)?
        } else {
            self.entry.checked_sub(fill.price)?
        };
        let gross = change.checked_mul(closed)?;
        let net = gross.checked_sub(opening_fee)?.checked_sub(exit_fee)?;
        self.gross = self.gross.checked_add(gross)?;
        self.net = self.net.checked_add(net)?;
        if net > Decimal::ZERO {
            self.wins += 1;
            self.profit = self.profit.checked_add(net)?;
        } else if net < Decimal::ZERO {
            self.losses += 1;
            self.loss = self.loss.checked_add(net.abs())?;
        } else {
            self.breakevens += 1;
        }

        let next = self.qty.checked_add(signed)?;
        self.entry_fees = self.entry_fees.checked_sub(opening_fee)?;
        if fill.quantity > closed {
            self.entry = fill.price;
            self.entry_fees = fill.fee.checked_sub(exit_fee)?;
        } else if next.is_zero() {
            self.entry = Decimal::ZERO;
        }
        self.qty = next;
        Some(())
    }

    fn finish(self, quote_currency: String) -> Option<ClosingPerformance> {
        let closing_count = self.wins + self.losses + self.breakevens;
        let (win_rate, expectancy_per_close) = if closing_count == 0 {
            (None, None)
        } else {
            let count = Decimal::from(closing_count);
            (
                Some(Decimal::from(self.wins).checked_div(count)?),
                Some(self.net.checked_div(count)?),
            )
        };
        let profit_factor = if self.loss.is_zero() {
            None
        } else {
            Some(self.profit.checked_div(self.loss)?)
        };
        Some(ClosingPerformance {
            quote_currency,
            turnover: self.turnover,
            fees_paid: self.fees,
            gross_realized: self.gross,
            net_realized: self.net,
            closing_count,
            wins: self.wins,
            losses: self.losses,
            breakevens: self.breakevens,
            net_winning_pnl: self.profit,
            net_losing_pnl: self.loss,
            win_rate,
            expectancy_per_close,
            profit_factor,
            open_quantity: self.qty,
            unallocated_entry_fees: self.entry_fees,
        })
    }
}

fn summarize(instrument: &Instrument, fills: &[&Fill]) -> Result<ClosingPerformance, String> {
    let quote = instrument
        .quote_currency
        .as_ref()
        .filter(|q| !q.is_empty())
        .ok_or("missing quote currency")?;
    if instrument.multiplier != Decimal::ONE
        || !matches!(
            instrument.asset_class,
            AssetClass::Spot | AssetClass::Perpetual
        )
    {
        return Err("unsupported instrument economics".into());
    }
    let mut acc = Accumulator::default();
    for fill in fills {
        if fill.venue != instrument.venue
            || fill.quantity <= Decimal::ZERO
            || fill.price <= Decimal::ZERO
            || (instrument.asset_class == AssetClass::Perpetual) != fill.leverage.is_some()
            || (!fill.fee.is_zero()
                && fill
                    .fee_currency
                    .as_ref()
                    .is_some_and(|asset| asset != quote))
        {
            return Err("unsupported fill identity, economics or fee currency".into());
        }
        acc.apply(fill)
            .ok_or("execution performance arithmetic overflow")?;
    }
    acc.finish(quote.clone())
        .ok_or_else(|| "execution performance arithmetic overflow".into())
}

/// Only symbols with accepted fills are included. No cross-currency totals.
pub(crate) fn evaluate(instruments: &[Instrument], fills: &[Fill]) -> ExecutionPerformance {
    let mut by_symbol: BTreeMap<String, Vec<&Fill>> = BTreeMap::new();
    let mut lookup: HashMap<&str, Option<&Instrument>> = HashMap::new();
    for instrument in instruments {
        lookup
            .entry(&instrument.symbol)
            .and_modify(|entry| *entry = None)
            .or_insert(Some(instrument));
    }
    for fill in fills {
        by_symbol.entry(fill.symbol.clone()).or_default().push(fill);
    }
    let per_symbol = by_symbol
        .into_iter()
        .map(|(symbol, fills)| {
            let result = lookup
                .get(symbol.as_str())
                .copied()
                .flatten()
                .ok_or_else(|| "missing or ambiguous instrument".to_string())
                .and_then(|instrument| summarize(instrument, &fills));
            let value = match result {
                Ok(summary) => SymbolExecutionPerformance::Available(Box::new(summary)),
                Err(reason) => SymbolExecutionPerformance::Unavailable { reason },
            };
            (symbol, value)
        })
        .collect();
    ExecutionPerformance { per_symbol }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{FillId, MarketType, OrderId, Venue};
    use rust_decimal_macros::dec;

    fn instrument(asset: AssetClass) -> Instrument {
        Instrument::new(
            "BTCUSDT",
            MarketType::Crypto,
            Venue::Binance,
            asset,
            Some("BTC".into()),
            Some("USDT".into()),
            dec!(0.01),
            dec!(0.001),
            Decimal::ONE,
        )
        .unwrap()
    }

    fn fill(side: Side, qty: Decimal, price: Decimal, fee: Decimal) -> Fill {
        Fill {
            id: FillId(uuid::Uuid::new_v4()),
            order_id: OrderId(uuid::Uuid::new_v4()),
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            side,
            quantity: qty,
            price,
            fee,
            fee_currency: None,
            filled_at: chrono::DateTime::UNIX_EPOCH,
            leverage: None,
            margin_mode: None,
        }
    }

    fn report(fills: &[Fill]) -> ClosingPerformance {
        let result = evaluate(&[instrument(AssetClass::Spot)], fills);
        match &result.per_symbol["BTCUSDT"] {
            SymbolExecutionPerformance::Available(summary) => (**summary).clone(),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn average_entry_partial_closes_and_fee_allocation() {
        let fills = vec![
            fill(Side::Buy, dec!(1), dec!(100), dec!(1)),
            fill(Side::Buy, dec!(1), dec!(120), dec!(2)),
            fill(Side::Sell, dec!(1), dec!(130), dec!(1)),
            fill(Side::Sell, dec!(1), dec!(90), dec!(1)),
        ];
        let partial = report(&fills[..3]);
        assert_eq!(partial.gross_realized, dec!(20));
        assert_eq!(partial.net_realized, dec!(17.5));
        assert_eq!(partial.unallocated_entry_fees, dec!(1.5));
        assert_eq!(partial.open_quantity, dec!(1));
        assert_eq!(partial.profit_factor, None);
        let result = report(&fills);
        assert_eq!(result.turnover, dec!(440));
        assert_eq!(result.fees_paid, dec!(5));
        assert_eq!(result.gross_realized, Decimal::ZERO);
        assert_eq!(result.net_realized, dec!(-5));
        assert_eq!(result.closing_count, 2);
        assert_eq!((result.wins, result.losses, result.breakevens), (1, 1, 0));
        assert_eq!(result.net_winning_pnl, dec!(17.5));
        assert_eq!(result.net_losing_pnl, dec!(22.5));
        assert_eq!(result.win_rate, Some(dec!(0.5)));
        assert_eq!(result.expectancy_per_close, Some(dec!(-2.5)));
        assert_eq!(result.profit_factor, Some(dec!(17.5) / dec!(22.5)));
        assert_eq!(result.open_quantity, Decimal::ZERO);
        assert_eq!(result.unallocated_entry_fees, Decimal::ZERO);
    }

    #[test]
    fn flip_allocates_fees_to_close_and_new_short() {
        let mut fills = vec![
            fill(Side::Buy, dec!(1), dec!(100), dec!(1)),
            fill(Side::Sell, dec!(2), dec!(110), dec!(4)),
            fill(Side::Buy, dec!(1), dec!(105), dec!(1)),
        ];
        for f in &mut fills {
            f.leverage = Some(dec!(10));
        }
        let i = instrument(AssetClass::Perpetual);
        let partial = summarize(&i, &fills[..2].iter().collect::<Vec<_>>()).unwrap();
        assert_eq!(partial.net_realized, dec!(7));
        assert_eq!(partial.open_quantity, dec!(-1));
        assert_eq!(partial.unallocated_entry_fees, dec!(2));
        let result = summarize(&i, &fills.iter().collect::<Vec<_>>()).unwrap();
        assert_eq!(result.gross_realized, dec!(15));
        assert_eq!(result.net_realized, dec!(9));
        assert_eq!(result.fees_paid, dec!(6));
        assert_eq!(result.closing_count, 2);
        assert_eq!(result.wins, 2);
        assert_eq!(result.profit_factor, None);
        assert_eq!(result.expectancy_per_close, Some(dec!(4.5)));
        assert_eq!(result.unallocated_entry_fees, Decimal::ZERO);
    }

    #[test]
    fn opening_only_and_breakeven_do_not_invent_ratios() {
        let fills = [
            fill(Side::Buy, dec!(1), dec!(100), dec!(1)),
            fill(Side::Sell, dec!(1), dec!(102), dec!(1)),
        ];
        let open = report(&fills[..1]);
        assert_eq!(open.win_rate, None);
        assert_eq!(open.expectancy_per_close, None);
        assert_eq!(open.profit_factor, None);
        assert_eq!(open.unallocated_entry_fees, dec!(1));
        let closed = report(&fills);
        assert_eq!(closed.breakevens, 1);
        assert_eq!(closed.win_rate, Some(Decimal::ZERO));
        assert_eq!(closed.expectancy_per_close, Some(Decimal::ZERO));
    }

    #[test]
    fn unsupported_currency_and_overflow_are_not_reported_as_zero_cost() {
        let mut f = fill(Side::Buy, dec!(1), dec!(100), dec!(1));
        f.fee_currency = Some("BNB".into());
        let i = instrument(AssetClass::Spot);
        let result = evaluate(std::slice::from_ref(&i), &[f.clone()]);
        assert!(matches!(
            result.per_symbol["BTCUSDT"],
            SymbolExecutionPerformance::Unavailable { .. }
        ));
        f.fee_currency = None;
        f.price = Decimal::MAX;
        f.quantity = dec!(2);
        let result = evaluate(&[i], &[f]);
        assert!(matches!(
            result.per_symbol["BTCUSDT"],
            SymbolExecutionPerformance::Unavailable { .. }
        ));
    }
}
