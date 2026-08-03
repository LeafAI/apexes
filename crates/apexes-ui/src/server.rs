//! `axum`-based WebSocket server: subscribes to a caller-supplied
//! `MarketEventBus` for a caller-supplied instrument list, and for
//! every connecting WebSocket client, forwards each `Trade`/`Tick`
//! `MarketEvent` as a JSON text frame (`Depth`/`Bar`/`MarkPrice` are
//! filtered out -- this phase's UI only shows bid/ask/last-trade).
//! Parameterized over an already-constructed `Arc<MarketEventBus>`
//! (not a function that builds its own `BinanceMarketDataProvider`
//! internally) specifically so tests can inject
//! `apexes_message_bus::test_support::MockMarketDataProvider` instead
//! of a real Binance connection -- see this crate's own tests below
//! and Task 4's `tests/server_client_roundtrip.rs`.

use apexes_core::{Instrument, MarketEvent};
use apexes_message_bus::MarketEventBus;
use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::any;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;

#[derive(Clone)]
struct AppState {
    bus: Arc<MarketEventBus>,
    instruments: Arc<Vec<Instrument>>,
}

/// Builds the `axum::Router` this crate's server task serves. Callers
/// (production: `main.rs`; tests: this file and
/// `tests/server_client_roundtrip.rs`) are responsible for binding a
/// listener and calling `axum::serve` themselves -- this function's
/// only job is producing the router, so it stays testable without a
/// real network bind.
pub fn build_app(bus: Arc<MarketEventBus>, instruments: Vec<Instrument>) -> Router {
    let state = AppState {
        bus,
        instruments: Arc::new(instruments),
    };
    Router::new()
        .route("/ws", any(ws_handler))
        .with_state(state)
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let receiver = match state.bus.subscribe(&state.instruments).await {
        Ok(receiver) => receiver,
        Err(error) => {
            tracing::warn!(
                ?error,
                "apexes-ui server: bus.subscribe failed, closing socket"
            );
            return;
        }
    };
    let mut event_stream = apexes_message_bus::receiver_into_stream(receiver);
    let (mut sender, _receiver_half) = socket.split();

    while let Some(event) = event_stream.next().await {
        if !matches!(event, MarketEvent::Trade(_) | MarketEvent::Tick(_)) {
            continue;
        }
        let json = match serde_json::to_string(&event) {
            Ok(json) => json,
            Err(error) => {
                tracing::warn!(?error, "apexes-ui server: failed to serialize MarketEvent");
                continue;
            }
        };
        if sender.send(Message::Text(json.into())).await.is_err() {
            tracing::info!("apexes-ui server: client disconnected, stopping forward loop");
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, MarketType, Venue};
    use apexes_message_bus::test_support::MockMarketDataProvider;
    use rust_decimal_macros::dec;

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

    #[test]
    fn build_app_does_not_panic_and_returns_a_router() {
        let mock = MockMarketDataProvider::new();
        let bus = Arc::new(MarketEventBus::new(Box::new(mock)));
        let _router = build_app(bus, vec![btc_usdt()]);
    }
}
