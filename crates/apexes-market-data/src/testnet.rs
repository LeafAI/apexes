//! Binance Spot **Test Network** market-data server for `barter-data`.
//!
//! `barter-data` 0.11 makes `Binance<Server>` generic over a public
//! `ExchangeServer`, with public trades and top-of-book (L1) implemented
//! for every server. Defining a server that points at the testnet
//! WebSocket reuses all of that. Order-book depth (L2) is implemented only
//! for the concrete mainnet `BinanceSpot` type, so a testnet session has
//! trades, ticks and bars but **no depth**.
//!
//! The URL comes from `apexes_core::binance_network`, the only place a
//! Binance hostname is written down.

use apexes_core::binance_network::BinanceNetwork;
use barter_data::exchange::{ExchangeServer, binance::Binance};
use barter_instrument::exchange::ExchangeId;

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct BinanceServerSpotTestnet;

impl ExchangeServer for BinanceServerSpotTestnet {
    // The testnet speaks the same protocol and instrument naming as
    // mainnet Spot; `barter-data` has no separate id for it, and each
    // stream builder in this crate only ever holds one exchange type.
    const ID: ExchangeId = ExchangeId::BinanceSpot;

    fn websocket_url() -> &'static str {
        BinanceNetwork::Testnet.endpoints().spot_market_ws
    }
}

pub type BinanceSpotTestnet = Binance<BinanceServerSpotTestnet>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn websocket_url_is_the_testnet_stream_host() {
        let url = BinanceServerSpotTestnet::websocket_url();
        assert!(url.starts_with("wss://"), "{url}");
        assert!(url.contains("testnet.binance.vision"), "{url}");
    }
}
