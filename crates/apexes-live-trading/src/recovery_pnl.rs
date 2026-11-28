//! Independent offline realized attribution for qualified Spot fills.
use crate::recovery_ledger::{LedgerContinuityError as E, exact_add, exact_mul};
use apexes_backtest::ledger_evidence::LedgerEvidence;
use apexes_core::{Fill, Instrument, Side};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::{BTreeMap, HashMap};

#[derive(Default)]
struct Inventory {
    quantity: Decimal,
    entry: Decimal,
    realized: Option<Decimal>,
}
impl Inventory {
    fn buy(&mut self, received: Decimal, cost: Decimal) -> Result<(), E> {
        if received <= Decimal::ZERO {
            return Err(E::Execution);
        }
        let total = exact_add(exact_mul(self.quantity, self.entry)?, cost)?;
        self.quantity = exact_add(self.quantity, received)?;
        let average = total.checked_div(self.quantity).ok_or(E::Arithmetic)?;
        if exact_mul(average, self.quantity)? != total {
            return Err(E::Arithmetic);
        }
        self.entry = average;
        Ok(())
    }

    fn sell(&mut self, quantity: Decimal, price: Decimal, base_fee: Decimal) -> Result<(), E> {
        let spent = exact_add(quantity, base_fee)?;
        if spent > self.quantity {
            return Err(E::Execution);
        }
        let gain = exact_mul(exact_add(price, -self.entry)?, quantity)?;
        let loss = exact_mul(self.entry, base_fee)?;
        self.realized = Some(exact_add(
            self.realized.unwrap_or_default(),
            exact_add(gain, -loss)?,
        )?);
        self.quantity = exact_add(self.quantity, -spent)?;
        Ok(())
    }
}

#[derive(Default)]
struct SymbolState {
    owned: Inventory,
    gross: Inventory,
    last: Option<DateTime<Utc>>,
    historical_fee: Decimal,
}

impl SymbolState {
    fn record(&mut self, fill: &Fill, base_fee: Decimal, value: Decimal) -> Result<(), E> {
        if self.last.is_some_and(|last| last >= fill.filled_at) {
            return Err(E::Execution);
        }
        self.last = Some(fill.filled_at);
        self.historical_fee = exact_add(self.historical_fee, value)?;
        match fill.side {
            Side::Buy => {
                let cost = exact_mul(fill.quantity, fill.price)?;
                self.owned.buy(exact_add(fill.quantity, -base_fee)?, cost)?;
                self.gross.buy(fill.quantity, cost)?;
            }
            Side::Sell => {
                self.owned.sell(fill.quantity, fill.price, base_fee)?;
                self.gross.sell(fill.quantity, fill.price, Decimal::ZERO)?;
                // Shadow attribution resets its entry on a complete close.
                if self.gross.quantity.is_zero() {
                    self.gross.entry = fill.price;
                }
            }
        }
        Ok(())
    }
}

/// Only called after exact fee-decision and independent wallet/journal
/// qualification. Check actual attribution order per symbol rather than
/// silently sorting it. No restored state is returned or applied.
pub(crate) fn compare_fee_bearing_pnl(
    captured: &LedgerEvidence,
    instruments: &[Instrument],
    fills: &[Fill],
) -> Result<(), E> {
    let fees = captured.fee_valuation.as_ref().ok_or(E::FeeMismatch)?;
    let decisions = fees.decisions.as_ref().ok_or(E::FeeMismatch)?;
    let by_id = fills.iter().map(|f| (f.id, f)).collect::<HashMap<_, _>>();
    let by_symbol = instruments
        .iter()
        .map(|i| (i.symbol.as_str(), i))
        .collect::<HashMap<_, _>>();
    if decisions.len() != fills.len() || by_id.len() != fills.len() {
        return Err(E::FeeMismatch);
    }
    let mut states = BTreeMap::<&str, SymbolState>::new();
    for d in decisions {
        let f = by_id.get(&d.fill_id).ok_or(E::FeeMismatch)?;
        let instrument = by_symbol.get(f.symbol.as_str()).ok_or(E::Execution)?;
        let base = instrument.base_currency.as_deref().ok_or(E::Execution)?;
        let base_fee = if f.fee_currency.as_deref() == Some(base) {
            f.fee
        } else {
            Decimal::ZERO
        };
        states
            .entry(&f.symbol)
            .or_default()
            .record(f, base_fee, d.value.ok_or(E::FeeMismatch)?)?;
    }
    if states.len() != fees.accounts.len() || states.len() != captured.positions.len() {
        return Err(E::RealizedPnlMismatch);
    }
    let mut realized = Vec::new();
    for (symbol, s) in states {
        let a = fees
            .accounts
            .iter()
            .find(|a| a.symbol == symbol)
            .ok_or(E::RealizedPnlMismatch)?;
        let p = captured
            .positions
            .iter()
            .find(|p| p.symbol == symbol)
            .ok_or(E::RealizedPnlMismatch)?;
        if p.quantity != s.owned.quantity
            || p.average_entry_price != s.owned.entry
            || a.gross_quantity != s.gross.quantity
            || a.entry != s.gross.entry
            || a.gross_realized != s.gross.realized.unwrap_or_default()
            || a.historical_fee != s.historical_fee
        {
            return Err(E::RealizedPnlMismatch);
        }
        // Validate the derived net-realized value without using a mark rate.
        let expected_net = exact_add(s.gross.realized.unwrap_or_default(), -s.historical_fee)?;
        let retained_net = exact_add(a.gross_realized, -a.historical_fee)?;
        if expected_net != retained_net {
            return Err(E::RealizedPnlMismatch);
        }
        if let Some(pnl) = s.owned.realized {
            realized.push((symbol.to_string(), pnl));
        }
    }
    if captured.realized_pnl != realized {
        return Err(E::RealizedPnlMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_backtest::fee_evidence::{
        FeeAccountEvidence, FeeDecisionEvidence, FeeDecisionSource, FeeValuationEvidence,
    };
    use apexes_core::{AssetClass, FillId, MarketType, OrderId, Venue};
    use apexes_strategy::Position;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn fixture() -> (LedgerEvidence, Vec<Instrument>, Vec<Fill>) {
        let instrument = Instrument::new(
            "BTCUSDT",
            MarketType::Crypto,
            Venue::Binance,
            AssetClass::Spot,
            Some("BTC".into()),
            Some("USDT".into()),
            dec!(0.01),
            dec!(0.001),
            dec!(1),
        )
        .unwrap();
        let fill = |side, quantity, price, fee, asset: &str, t: i64| Fill {
            id: FillId(Uuid::new_v4()),
            order_id: OrderId(Uuid::new_v4()),
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            side,
            quantity,
            price,
            fee,
            fee_currency: Some(asset.into()),
            filled_at: DateTime::from_timestamp(1_800_000_000 + t, 0).unwrap(),
            leverage: None,
            margin_mode: None,
        };
        // Owned buy cost 100 / .8 = 125; sell .4 and pay .1 BTC.
        // Basic realized: (.4 * 25) - (.1 * 125) = -2.5.
        // Gross shadow: .4 * 50 = 20; historical fees 20+15+1+3=39.
        // Quote and third-asset fees do not change owned position cost.
        let fills = vec![
            fill(Side::Buy, dec!(1), dec!(100), dec!(0.2), "BTC", 1),
            fill(Side::Sell, dec!(0.4), dec!(150), dec!(0.1), "BTC", 2),
            fill(Side::Buy, dec!(0.1), dec!(100), dec!(1), "USDT", 3),
            fill(Side::Buy, dec!(0.1), dec!(100), dec!(0.01), "BNB", 4),
        ];
        // Gross shadow average remains 100; owned average becomes 115.
        let sources = [
            FeeDecisionSource::Base,
            FeeDecisionSource::Base,
            FeeDecisionSource::Quote,
            FeeDecisionSource::Observed {
                observed_at: fills[2].filled_at,
                rate: dec!(300),
            },
        ];
        let decisions = fills
            .iter()
            .zip(sources)
            .zip([dec!(20), dec!(15), dec!(1), dec!(3)])
            .map(|((f, source), value)| FeeDecisionEvidence {
                fill_id: f.id,
                venue: f.venue,
                symbol: f.symbol.clone(),
                asset: f.fee_currency.clone().unwrap(),
                filled_at: f.filled_at,
                price: f.price,
                fee: f.fee,
                external_fee: false,
                value: Some(value),
                source,
            })
            .collect();
        let evidence = LedgerEvidence {
            version: 1,
            scope: vec![(Venue::Binance, "BTCUSDT".into())],
            balances: vec![],
            positions: vec![Position {
                symbol: "BTCUSDT".into(),
                quantity: dec!(0.5),
                average_entry_price: dec!(115),
                leverage: None,
                margin_mode: None,
                allocated_margin: Decimal::ZERO,
            }],
            realized_pnl: vec![("BTCUSDT".into(), dec!(-2.5))],
            fee_valuation: Some(FeeValuationEvidence {
                version: 1,
                accounts: vec![FeeAccountEvidence {
                    symbol: "BTCUSDT".into(),
                    venue: Venue::Binance,
                    base: "BTC".into(),
                    quote: "USDT".into(),
                    gross_quantity: dec!(0.8),
                    entry: dec!(100),
                    gross_realized: dec!(20),
                    fees: vec![
                        ("BNB".into(), dec!(0.01)),
                        ("BTC".into(), dec!(0.3)),
                        ("USDT".into(), dec!(1)),
                    ],
                    external_fees: vec![],
                    historical_fee: dec!(39),
                    missing_execution_valuations: 0,
                }],
                prices: vec![],
                decisions: Some(decisions),
            }),
        };
        (evidence, vec![instrument], fills)
    }

    #[test]
    fn mixed_fee_realized_pnl_matches_owned_and_shadow_cost_basis() {
        let (captured, instruments, fills) = fixture();
        compare_fee_bearing_pnl(&captured, &instruments, &fills).unwrap();
        let mut changed = captured.clone();
        changed.realized_pnl[0].1 = dec!(10);
        assert_eq!(
            compare_fee_bearing_pnl(&changed, &instruments, &fills),
            Err(E::RealizedPnlMismatch)
        );
        let mut changed = captured.clone();
        changed.fee_valuation.as_mut().unwrap().accounts[0].gross_realized = dec!(19);
        assert_eq!(
            compare_fee_bearing_pnl(&changed, &instruments, &fills),
            Err(E::RealizedPnlMismatch)
        );
        let mut changed = captured.clone();
        changed.positions[0].average_entry_price = dec!(100);
        assert_eq!(
            compare_fee_bearing_pnl(&changed, &instruments, &fills),
            Err(E::RealizedPnlMismatch)
        );
        let mut changed = captured.clone();
        changed
            .fee_valuation
            .as_mut()
            .unwrap()
            .decisions
            .as_mut()
            .unwrap()
            .swap(2, 3);
        assert_eq!(
            compare_fee_bearing_pnl(&changed, &instruments, &fills),
            Err(E::Execution)
        );
    }

    #[test]
    fn refuses_inexact_cost_and_unowned_base_fee() {
        let mut inventory = Inventory::default();
        assert_eq!(inventory.buy(dec!(3), dec!(1)), Err(E::Arithmetic));
        let mut inventory = Inventory::default();
        inventory.buy(dec!(1), dec!(100)).unwrap();
        assert_eq!(
            inventory.sell(dec!(1), dec!(100), dec!(0.01)),
            Err(E::Execution)
        );
    }
}
