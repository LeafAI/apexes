//! [`SymbolVenueList`]: symbol-level and venue-level allow/deny lists.
//! Each dimension (symbol, venue) is independently either a blacklist
//! (reject only listed values, allow everything else) or a whitelist
//! (allow only listed values, reject everything else) -- configured via
//! [`ListMode`]. A dimension with `ListMode::Disabled` performs no
//! check on that dimension at all.

use std::collections::HashMap;
use std::collections::HashSet;

use apexes_core::{Order, OrderId, Venue};
use apexes_strategy::{OrderIntent, Position};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::error::RiskViolation;
use crate::pre_trade::PreTradeCheck;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListMode<T: std::hash::Hash + Eq> {
    Disabled,
    Blacklist(HashSet<T>),
    Whitelist(HashSet<T>),
}

pub struct SymbolVenueList {
    symbols: ListMode<String>,
    venues: ListMode<Venue>,
}

impl SymbolVenueList {
    pub fn new(symbols: ListMode<String>, venues: ListMode<Venue>) -> Self {
        Self { symbols, venues }
    }
}

impl PreTradeCheck for SymbolVenueList {
    fn check(
        &self,
        intent: &OrderIntent,
        _positions: &HashMap<String, Position>,
        _balances: &HashMap<String, Decimal>,
        _open_orders: &HashMap<OrderId, Order>,
        _now: DateTime<Utc>,
    ) -> Result<(), Vec<RiskViolation>> {
        let mut violations = Vec::new();

        match &self.symbols {
            ListMode::Disabled => {}
            ListMode::Blacklist(set) if set.contains(&intent.symbol) => {
                violations.push(RiskViolation::SymbolBlacklisted {
                    symbol: intent.symbol.clone(),
                });
            }
            ListMode::Whitelist(set) if !set.contains(&intent.symbol) => {
                violations.push(RiskViolation::SymbolNotWhitelisted {
                    symbol: intent.symbol.clone(),
                });
            }
            _ => {}
        }

        match &self.venues {
            ListMode::Disabled => {}
            ListMode::Blacklist(set) if set.contains(&intent.venue) => {
                violations.push(RiskViolation::VenueBlacklisted {
                    venue: intent.venue,
                });
            }
            ListMode::Whitelist(set) if !set.contains(&intent.venue) => {
                violations.push(RiskViolation::VenueNotWhitelisted {
                    venue: intent.venue,
                });
            }
            _ => {}
        }

        if violations.is_empty() {
            Ok(())
        } else {
            Err(violations)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::AssetClass;
    use apexes_core::{OrderType, Side};
    use chrono::TimeZone;

    fn sample_intent(symbol: &str, venue: Venue) -> OrderIntent {
        OrderIntent {
            symbol: symbol.to_string(),
            venue,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: rust_decimal_macros::dec!(1),
            price: None,
            stop_price: None,
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        }
    }

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap()
    }

    fn empty_positions() -> HashMap<String, Position> {
        HashMap::new()
    }

    fn empty_balances() -> HashMap<String, Decimal> {
        HashMap::new()
    }

    fn empty_open_orders() -> HashMap<OrderId, Order> {
        HashMap::new()
    }

    #[test]
    fn symbol_blacklist_rejects_listed_symbol() {
        let mut blacklist = HashSet::new();
        blacklist.insert("BTCUSDT".to_string());
        let list = SymbolVenueList::new(ListMode::Blacklist(blacklist), ListMode::Disabled);
        let intent = sample_intent("BTCUSDT", Venue::Binance);
        let result = list.check(
            &intent,
            &empty_positions(),
            &empty_balances(),
            &empty_open_orders(),
            now(),
        );
        assert_eq!(
            result,
            Err(vec![RiskViolation::SymbolBlacklisted {
                symbol: "BTCUSDT".to_string(),
            }])
        );
    }

    #[test]
    fn symbol_blacklist_allows_unlisted_symbol() {
        let mut blacklist = HashSet::new();
        blacklist.insert("BTCUSDT".to_string());
        let list = SymbolVenueList::new(ListMode::Blacklist(blacklist), ListMode::Disabled);
        let intent = sample_intent("ETHUSDT", Venue::Binance);
        let result = list.check(
            &intent,
            &empty_positions(),
            &empty_balances(),
            &empty_open_orders(),
            now(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn symbol_whitelist_rejects_unlisted_symbol() {
        let mut whitelist = HashSet::new();
        whitelist.insert("BTCUSDT".to_string());
        let list = SymbolVenueList::new(ListMode::Whitelist(whitelist), ListMode::Disabled);
        let intent = sample_intent("ETHUSDT", Venue::Binance);
        let result = list.check(
            &intent,
            &empty_positions(),
            &empty_balances(),
            &empty_open_orders(),
            now(),
        );
        assert_eq!(
            result,
            Err(vec![RiskViolation::SymbolNotWhitelisted {
                symbol: "ETHUSDT".to_string(),
            }])
        );
    }

    #[test]
    fn symbol_whitelist_allows_listed_symbol() {
        let mut whitelist = HashSet::new();
        whitelist.insert("BTCUSDT".to_string());
        let list = SymbolVenueList::new(ListMode::Whitelist(whitelist), ListMode::Disabled);
        let intent = sample_intent("BTCUSDT", Venue::Binance);
        let result = list.check(
            &intent,
            &empty_positions(),
            &empty_balances(),
            &empty_open_orders(),
            now(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn disabled_symbol_dimension_never_rejects() {
        let list = SymbolVenueList::new(ListMode::Disabled, ListMode::Disabled);
        let intent = sample_intent("ANYTHING", Venue::Binance);
        let result = list.check(
            &intent,
            &empty_positions(),
            &empty_balances(),
            &empty_open_orders(),
            now(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn venue_blacklist_rejects_listed_venue() {
        let mut blacklist = HashSet::new();
        blacklist.insert(Venue::Okx);
        let list = SymbolVenueList::new(ListMode::Disabled, ListMode::Blacklist(blacklist));
        let intent = sample_intent("BTCUSDT", Venue::Okx);
        let result = list.check(
            &intent,
            &empty_positions(),
            &empty_balances(),
            &empty_open_orders(),
            now(),
        );
        assert_eq!(
            result,
            Err(vec![RiskViolation::VenueBlacklisted { venue: Venue::Okx }])
        );
    }

    #[test]
    fn venue_whitelist_rejects_unlisted_venue() {
        let mut whitelist = HashSet::new();
        whitelist.insert(Venue::Binance);
        let list = SymbolVenueList::new(ListMode::Disabled, ListMode::Whitelist(whitelist));
        let intent = sample_intent("BTCUSDT", Venue::Okx);
        let result = list.check(
            &intent,
            &empty_positions(),
            &empty_balances(),
            &empty_open_orders(),
            now(),
        );
        assert_eq!(
            result,
            Err(vec![RiskViolation::VenueNotWhitelisted {
                venue: Venue::Okx
            }])
        );
    }

    #[test]
    fn symbol_and_venue_violations_both_returned_when_both_hit() {
        let mut symbol_blacklist = HashSet::new();
        symbol_blacklist.insert("BTCUSDT".to_string());
        let mut venue_blacklist = HashSet::new();
        venue_blacklist.insert(Venue::Okx);
        let list = SymbolVenueList::new(
            ListMode::Blacklist(symbol_blacklist),
            ListMode::Blacklist(venue_blacklist),
        );
        let intent = sample_intent("BTCUSDT", Venue::Okx);
        let result = list.check(
            &intent,
            &empty_positions(),
            &empty_balances(),
            &empty_open_orders(),
            now(),
        );
        let violations = result.expect_err("expected both symbol and venue violations");
        assert_eq!(violations.len(), 2);
        assert!(violations.contains(&RiskViolation::SymbolBlacklisted {
            symbol: "BTCUSDT".to_string(),
        }));
        assert!(violations.contains(&RiskViolation::VenueBlacklisted { venue: Venue::Okx }));
    }
}
