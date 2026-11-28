//! F02.01: rebuild strategy-owned Spot inventory from durable executions.
//! Never infer a position or cost basis from the account's wallet alone.
use apexes_core::{AssetClass, Fill, Instrument, Side};
use apexes_strategy::Position;
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};

/// Account-level amounts; neither of these implies strategy ownership.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SpotWalletBalance {
    pub free: Decimal,
    pub locked: Decimal,
}

#[derive(Debug, thiserror::Error)]
pub enum SpotRecoveryError {
    #[error(
        "spot inventory recovery requires a quiescent account; reconcile/cancel working orders first"
    )]
    WorkingOrders,
    #[error("unsupported or ambiguous spot instrument configuration: {0}")]
    Instrument(String),
    #[error("invalid or unattributable persisted spot fill for {0}")]
    InvalidFill(String),
    #[error("persisted sells exceed strategy-owned inventory for {0}")]
    UnownedSale(String),
    #[error("account has less inventory than the strategy owns for {0}")]
    InventoryShortfall(String),
    #[error("relevant account asset {0} has locked balance; reconcile reservations before startup")]
    LockedBalance(String),
    #[error("invalid spot wallet amount for {0}")]
    InvalidBalance(String),
}

/// Return only positions attributable to this strategy's durable fills.
/// Account balances must be supplied independently and must not be
/// credited/debited again when these positions seed the live ledger.
pub fn recover_spot_positions(
    instruments: &[Instrument],
    fills: &[Fill],
    wallet: &HashMap<String, SpotWalletBalance>,
    has_working_orders: bool,
) -> Result<HashMap<String, Position>, SpotRecoveryError> {
    if has_working_orders {
        return Err(SpotRecoveryError::WorkingOrders);
    }
    let mut by_symbol = HashMap::new();
    let mut bases = HashSet::new();
    let mut quotes = HashSet::new();
    for i in instruments {
        let (Some(base), Some(quote)) = (&i.base_currency, &i.quote_currency) else {
            return Err(SpotRecoveryError::Instrument(i.symbol.clone()));
        };
        // One position per base and independent quote capital. Mixed
        // base/quote attribution needs the asset ledger in F02.03/F02.05.
        if i.asset_class != AssetClass::Spot
            || base == quote
            || !bases.insert(base.as_str())
            || by_symbol.insert(i.symbol.as_str(), i).is_some()
        {
            return Err(SpotRecoveryError::Instrument(i.symbol.clone()));
        }
        quotes.insert(quote.as_str());
    }
    if let Some(asset) = bases.intersection(&quotes).next() {
        return Err(SpotRecoveryError::Instrument((*asset).to_string()));
    }

    for asset in bases.union(&quotes) {
        let balance = wallet.get(*asset).copied().unwrap_or_default();
        if balance.free < Decimal::ZERO || balance.locked < Decimal::ZERO {
            return Err(SpotRecoveryError::InvalidBalance((*asset).to_string()));
        }
        // Until F02.05 tracks reservations continuously, do not seed
        // spendable balances with exchange-locked funds.
        if !balance.locked.is_zero() {
            return Err(SpotRecoveryError::LockedBalance((*asset).to_string()));
        }
    }

    let mut positions: HashMap<String, Position> = HashMap::new();
    for fill in fills {
        let i = by_symbol
            .get(fill.symbol.as_str())
            .ok_or_else(|| SpotRecoveryError::InvalidFill(fill.symbol.clone()))?;
        let base = i
            .base_currency
            .as_deref()
            .ok_or_else(|| SpotRecoveryError::Instrument(i.symbol.clone()))?;
        if fill.venue != i.venue
            || fill.leverage.is_some()
            || fill.margin_mode.is_some()
            || fill.quantity <= Decimal::ZERO
            || fill.price <= Decimal::ZERO
            || fill.fee < Decimal::ZERO
        {
            return Err(SpotRecoveryError::InvalidFill(fill.symbol.clone()));
        }
        // A fee charged in another strategy-owned base cannot be
        // attributed by the current per-symbol ledger. Fail closed.
        if !fill.fee.is_zero()
            && fill
                .fee_currency
                .as_deref()
                .is_some_and(|asset| asset != base && bases.contains(asset))
        {
            return Err(SpotRecoveryError::InvalidFill(fill.symbol.clone()));
        }
        let base_fee = if fill.fee_currency.as_deref() == Some(base) {
            fill.fee
        } else {
            Decimal::ZERO
        };
        let position = positions.entry(fill.symbol.clone()).or_insert(Position {
            symbol: fill.symbol.clone(),
            quantity: Decimal::ZERO,
            average_entry_price: Decimal::ZERO,
            leverage: None,
            margin_mode: None,
            allocated_margin: Decimal::ZERO,
        });
        match fill.side {
            Side::Buy => {
                let received = fill.quantity - base_fee;
                if received <= Decimal::ZERO {
                    return Err(SpotRecoveryError::InvalidFill(fill.symbol.clone()));
                }
                // Match PortfolioLedger's current base-fee weighted cost
                // basis. Quote and third-asset fees remain F02.03 scope.
                let cost =
                    position.quantity * position.average_entry_price + fill.quantity * fill.price;
                position.quantity += received;
                position.average_entry_price = cost / position.quantity;
            }
            Side::Sell => {
                let spent = fill.quantity + base_fee;
                if spent > position.quantity {
                    return Err(SpotRecoveryError::UnownedSale(fill.symbol.clone()));
                }
                position.quantity -= spent;
            }
        }
    }

    positions.retain(|_, p| !p.quantity.is_zero());
    for (symbol, position) in &positions {
        let base = by_symbol
            .get(symbol.as_str())
            .and_then(|i| i.base_currency.as_deref())
            .ok_or_else(|| SpotRecoveryError::Instrument(symbol.clone()))?;
        if wallet.get(base).copied().unwrap_or_default().free < position.quantity {
            return Err(SpotRecoveryError::InventoryShortfall(base.to_string()));
        }
    }
    Ok(positions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{FillId, MarketType, OrderId, Venue};
    use chrono::Utc;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn instrument() -> Instrument {
        Instrument::new(
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
        .unwrap()
    }

    fn fill(side: Side, quantity: Decimal, price: Decimal, fee: Decimal) -> Fill {
        Fill {
            id: FillId(Uuid::new_v4()),
            order_id: OrderId(Uuid::new_v4()),
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            side,
            price,
            quantity,
            fee,
            fee_currency: Some("BTC".into()),
            filled_at: Utc::now(),
            leverage: None,
            margin_mode: None,
        }
    }

    fn wallet(base: Decimal) -> HashMap<String, SpotWalletBalance> {
        HashMap::from([
            (
                "BTC".into(),
                SpotWalletBalance {
                    free: base,
                    locked: Decimal::ZERO,
                },
            ),
            (
                "USDT".into(),
                SpotWalletBalance {
                    free: dec!(10000),
                    locked: Decimal::ZERO,
                },
            ),
        ])
    }

    #[test]
    fn wallet_holdings_are_not_strategy_positions() {
        assert!(
            recover_spot_positions(&[instrument()], &[], &wallet(dec!(20)), false)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn recovered_quantity_and_basis_match_live_ledger_without_double_crediting_wallet() {
        let fills = vec![
            fill(Side::Buy, dec!(2), dec!(100), dec!(0.1)),
            fill(Side::Buy, dec!(1), dec!(150), dec!(0)),
            fill(Side::Sell, dec!(1), dec!(180), dec!(0.1)),
        ];
        let instrument = instrument();
        let mut ledger = apexes_backtest::PortfolioLedger::new(
            HashMap::from([(instrument.symbol.clone(), instrument.clone())]),
            HashMap::from([("BTC".into(), dec!(10)), ("USDT".into(), dec!(10000))]),
        );
        for f in &fills {
            ledger.apply_fill(f).unwrap();
        }
        let recovered = recover_spot_positions(
            std::slice::from_ref(&instrument),
            &fills,
            &wallet(dec!(11.8)),
            false,
        )
        .unwrap();
        assert_eq!(recovered["BTCUSDT"].quantity, dec!(1.8));
        assert_eq!(
            recovered["BTCUSDT"].quantity,
            ledger.positions()["BTCUSDT"].quantity
        );
        assert_eq!(
            recovered["BTCUSDT"].average_entry_price,
            ledger.positions()["BTCUSDT"].average_entry_price
        );
        let seeded = apexes_backtest::PortfolioLedger::new(
            HashMap::from([(instrument.symbol.clone(), instrument)]),
            ledger.balances().clone(),
        )
        .with_positions(recovered);
        assert_eq!(seeded.balances(), ledger.balances());
    }

    #[test]
    fn unowned_sells_and_missing_inventory_fail_closed() {
        assert!(matches!(
            recover_spot_positions(
                &[instrument()],
                &[fill(Side::Sell, dec!(1), dec!(100), dec!(0))],
                &wallet(dec!(20)),
                false
            ),
            Err(SpotRecoveryError::UnownedSale(_))
        ));
        assert!(matches!(
            recover_spot_positions(
                &[instrument()],
                &[fill(Side::Buy, dec!(2), dec!(100), dec!(0))],
                &wallet(dec!(1)),
                false
            ),
            Err(SpotRecoveryError::InventoryShortfall(_))
        ));
    }

    #[test]
    fn working_orders_or_locked_funds_prevent_ambiguous_snapshot() {
        assert!(matches!(
            recover_spot_positions(&[instrument()], &[], &wallet(dec!(1)), true),
            Err(SpotRecoveryError::WorkingOrders)
        ));
        let mut balances = wallet(dec!(1));
        balances.get_mut("BTC").unwrap().locked = dec!(0.5);
        assert!(matches!(
            recover_spot_positions(&[instrument()], &[], &balances, false),
            Err(SpotRecoveryError::LockedBalance(_))
        ));
    }
}
