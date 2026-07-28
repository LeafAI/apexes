/// Splits a Binance spot symbol like "BTCUSDT" into (base, quote) pairs
/// for barter-data's subscription tuple syntax. This is a heuristic split
/// for the fixed set of quote currencies this sub-project targets.
pub fn split_symbol(symbol: &str) -> Option<(String, String)> {
    const KNOWN_QUOTES: [&str; 3] = ["USDT", "USDC", "BUSD"];
    for quote in KNOWN_QUOTES {
        if let Some(base) = symbol.strip_suffix(quote)
            && !base.is_empty()
        {
            return Some((base.to_lowercase(), quote.to_lowercase()));
        }
    }
    None
}

/// Reconstructs the original exchange symbol (e.g. "BTCUSDT") from a
/// `barter-data` market-data instrument. `MarketDataInstrument`'s `Display`
/// impl renders "{base}_{quote}_{kind}" (e.g. "btc_usdt_spot"), which is not
/// the original symbol, so we rebuild it from the (internally lowercased)
/// base/quote asset names instead.
pub fn symbol_of(
    instrument: &barter_instrument::instrument::market_data::MarketDataInstrument,
) -> String {
    format!(
        "{}{}",
        instrument.base.name().to_uppercase(),
        instrument.quote.name().to_uppercase()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_symbol_handles_usdt_pairs() {
        assert_eq!(
            split_symbol("BTCUSDT"),
            Some(("btc".to_string(), "usdt".to_string()))
        );
    }

    #[test]
    fn split_symbol_handles_usdc_pairs() {
        assert_eq!(
            split_symbol("ETHUSDC"),
            Some(("eth".to_string(), "usdc".to_string()))
        );
    }

    #[test]
    fn split_symbol_returns_none_for_unknown_quote() {
        assert_eq!(split_symbol("BTCXYZ"), None);
    }
}
