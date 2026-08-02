//! [`AccountPnl`]/[`compute_pnl`]: a free function computing a
//! cross-currency-valued P&L snapshot from any `&dyn Account`, rather
//! than a method every `Account` implementation would need to
//! duplicate -- pure composition over the trait's already-exposed
//! data. See `docs/superpowers/specs/2026-08-02-apexes-account-design.md`
//! §3.6.

use std::collections::HashMap;

use apexes_core::ApexesError;
use rust_decimal::Decimal;

use crate::account::Account;
use crate::fx_rate::FxRateProvider;

#[derive(Debug, Clone, PartialEq)]
pub struct AccountPnl {
    pub realized: HashMap<String, Decimal>,
    pub unrealized: HashMap<String, Decimal>,
    pub total_equity: Decimal,
}

/// Computes a `AccountPnl` snapshot from any `&dyn Account`.
/// `mark_prices`: current price for each open position's underlying
/// asset (same convention as `Account::equity`). A symbol with a
/// non-zero position but no entry in `mark_prices` is a configuration
/// error -- returns `Err`, matching
/// `PortfolioLedger::margin_status`'s established "never silently
/// skip a missing mark price" precedent.
pub fn compute_pnl(
    account: &dyn Account,
    mark_prices: &HashMap<String, Decimal>,
    fx_provider: &dyn FxRateProvider,
    valuation_currency: &str,
) -> Result<AccountPnl, ApexesError> {
    let realized = account.realized_pnl();
    let positions = account.positions();

    let mut unrealized = HashMap::new();
    for (symbol, position) in &positions {
        if position.quantity.is_zero() {
            continue;
        }
        let mark_price =
            mark_prices
                .get(symbol)
                .ok_or_else(|| ApexesError::InvalidBacktestConfig {
                    reason: format!("no mark price provided for open position {symbol}"),
                })?;
        let pnl = (*mark_price - position.average_entry_price) * position.quantity;
        unrealized.insert(symbol.clone(), pnl);
    }

    let total_equity = account.equity(mark_prices, fx_provider, valuation_currency)?;

    Ok(AccountPnl {
        realized,
        unrealized,
        total_equity,
    })
}

#[cfg(test)]
mod tests {
    use crate::account::Account;
    use crate::fx_rate::StaticFxRateProvider;
    use crate::pnl::compute_pnl;
    use apexes_backtest::PortfolioLedger;
    use apexes_core::{AssetClass, Instrument, MarketType, Side, Venue};
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

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

    fn ledger_with_20k_usdt() -> PortfolioLedger {
        let mut instruments = HashMap::new();
        instruments.insert("BTCUSDT".to_string(), btc_usdt());
        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(20000));
        PortfolioLedger::new(instruments, initial_balances)
    }

    #[test]
    fn compute_pnl_reports_realized_unrealized_and_total_equity() {
        let mut ledger = ledger_with_20k_usdt();
        // Open 0.2 BTC at 60000, then partially close 0.1 at 65000
        // (realizes 500), leaving 0.1 open.
        ledger
            .apply_fill(&apexes_core::Fill {
                id: apexes_core::FillId(uuid::Uuid::new_v4()),
                order_id: apexes_core::OrderId(uuid::Uuid::new_v4()),
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                side: Side::Buy,
                price: dec!(60000),
                quantity: dec!(0.2),
                fee: dec!(0),
                fee_currency: None,
                filled_at: chrono::Utc::now(),
                leverage: None,
                margin_mode: None,
            })
            .unwrap();
        ledger
            .apply_fill(&apexes_core::Fill {
                id: apexes_core::FillId(uuid::Uuid::new_v4()),
                order_id: apexes_core::OrderId(uuid::Uuid::new_v4()),
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                side: Side::Sell,
                price: dec!(65000),
                quantity: dec!(0.1),
                fee: dec!(0),
                fee_currency: None,
                filled_at: chrono::Utc::now(),
                leverage: None,
                margin_mode: None,
            })
            .unwrap();

        let mut mark_prices = HashMap::new();
        // Both conventions coexist: `Account::equity` prices BALANCES
        // (asset-keyed "BTC"), while `compute_pnl`'s unrealized loop
        // prices POSITIONS (symbol-keyed "BTCUSDT") -- per the design
        // note, populate both keys in the same map.
        mark_prices.insert("BTC".to_string(), dec!(70000));
        mark_prices.insert("BTCUSDT".to_string(), dec!(70000));
        let fx = StaticFxRateProvider::new(HashMap::new());

        let pnl = compute_pnl(&ledger, &mark_prices, &fx, "USDT").unwrap();

        assert_eq!(*pnl.realized.get("BTCUSDT").unwrap(), dec!(500));
        // Unrealized: (70000 - 60000) * 0.1 remaining = 1000.
        assert_eq!(*pnl.unrealized.get("BTCUSDT").unwrap(), dec!(1000));
        assert_eq!(
            pnl.total_equity,
            Account::equity(&ledger, &mark_prices, &fx, "USDT").unwrap()
        );
    }
}
