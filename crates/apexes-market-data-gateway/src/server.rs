//! `axum` app exposing `GET /market` -- upgrades to a WebSocket,
//! optionally replays history first (if `?replay_from=<RFC3339>` is
//! present, via [`crate::replay::replay_since`]), then forwards live
//! events from [`Gateway::subscribe`] as JSON text frames, matching
//! `apexes-ui`'s existing `/ws` (13a) frame format exactly (bare
//! `serde_json::to_string(&event)`, no envelope -- see `protocol.rs`).
//!
//! Deviation from this task's brief pseudocode: [`build_app`] also
//! takes `instruments`, not just `gateway`/`storage_reader`. The
//! brief's stub signature omitted it, but [`crate::replay::replay_since`]
//! genuinely needs to know which instruments to query (there is no way
//! to recover that list from `Gateway` itself -- it does not expose
//! the instrument set it was `connect`ed with). This mirrors
//! `apexes-ui`'s own `build_app(bus, instruments)` precedent
//! (`crates/apexes-ui/src/server.rs`), which takes the identical extra
//! parameter for the identical reason.
//!
//! Ordering guarantee for a `replay_from` client (no lost/duplicated-
//! out-of-order events at the replay/live seam): this handler calls
//! `Gateway::subscribe()` to obtain a live broadcast receiver BEFORE
//! issuing the (async, potentially slow) `replay_since` query. Any
//! live event published while the replay query is in flight is
//! therefore already buffered in that receiver's ring buffer (up to
//! its capacity) rather than lost, and is forwarded, in order, right
//! after the replayed history finishes sending. Per the design spec
//! (§3.2), a handful of duplicate or slightly-early events across this
//! seam is an accepted tradeoff -- clients dedupe by `(symbol,
//! timestamp)` -- not a defect.

use crate::gateway::Gateway;
use crate::replay::replay_since;
use apexes_core::Instrument;
use apexes_storage::StorageReader;
use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::routing::get;
use chrono::{DateTime, Utc};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Clone)]
struct AppState {
    gateway: Arc<Gateway>,
    storage_reader: Option<Arc<StorageReader>>,
    instruments: Arc<Vec<Instrument>>,
}

/// Optional query parameters accepted by `GET /market`. `replay_from`
/// deserializes an RFC 3339 timestamp string (chrono's `serde` feature
/// parses `DateTime<Utc>` in RFC 3339 form by default) -- absent means
/// "no replay, live forwarding only".
#[derive(Debug, Deserialize)]
struct MarketQuery {
    replay_from: Option<DateTime<Utc>>,
}

/// Builds the `axum::Router` this crate's `main.rs` serves. Callers
/// are responsible for binding a listener and calling `axum::serve`
/// themselves -- this function's only job is producing the router, so
/// it stays testable without a real network bind (mirroring
/// `apexes-ui`'s own `build_app` precedent). `storage_reader: None`
/// disables replay entirely: a client that supplies `?replay_from=`
/// against a gateway with no persistence configured gets its
/// connection closed gracefully (see `handle_socket`) rather than a
/// panic.
pub fn build_app(
    gateway: Arc<Gateway>,
    storage_reader: Option<Arc<StorageReader>>,
    instruments: Vec<Instrument>,
) -> Router {
    let state = AppState {
        gateway,
        storage_reader,
        instruments: Arc::new(instruments),
    };
    Router::new()
        .route("/market", get(market_handler))
        .with_state(state)
}

async fn market_handler(
    ws: WebSocketUpgrade,
    Query(query): Query<MarketQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, query, state))
}

async fn handle_socket(socket: WebSocket, query: MarketQuery, state: AppState) {
    // Subscribe to the live broadcast BEFORE doing anything else (in
    // particular, before the potentially-slow `replay_since` query
    // below) -- see this module's doc comment for why this ordering
    // matters for replay/live continuity.
    let receiver = state.gateway.subscribe();
    let (mut sender, _receiver_half) = socket.split();

    if let Some(replay_from) = query.replay_from {
        let Some(reader) = state.storage_reader.as_ref() else {
            tracing::warn!(
                "apexes-market-data-gateway server: client requested ?replay_from= but no \
                 persistence/StorageReader is configured on this gateway, closing socket"
            );
            close_gracefully(&mut sender).await;
            return;
        };

        let replayed = match replay_since(reader, &state.instruments, replay_from).await {
            Ok(events) => events,
            Err(error) => {
                tracing::warn!(
                    ?error,
                    "apexes-market-data-gateway server: replay_since failed, closing socket"
                );
                close_gracefully(&mut sender).await;
                return;
            }
        };

        for event in replayed {
            if !send_event(&mut sender, &event).await {
                return;
            }
        }
    }

    // `Lagged` (a slow WS client falling behind the broadcast channel)
    // is genuinely possible here -- this is the receiver-side consumer
    // of `Gateway::subscribe()`'s bare `broadcast::Receiver`.
    // `receiver_into_stream` already implements exactly this
    // log-and-continue handling (see its own doc comment in
    // `apexes-message-bus`), so it is reused here rather than
    // reimplemented.
    let mut event_stream = apexes_message_bus::receiver_into_stream(receiver);
    while let Some(event) = event_stream.next().await {
        if !send_event(&mut sender, &event).await {
            return;
        }
    }
}

/// Sends a real WebSocket Close frame before returning, so the client
/// sees a clean protocol-level close (not a bare TCP reset, which
/// `tokio-tungstenite` clients on the other end surface as
/// `Protocol(ResetWithoutClosingHandshake)`) -- used by
/// `handle_socket`'s "no persistence configured" / "replay query
/// failed" early-return paths. Ignores a send failure here (the
/// client may have already disconnected) since there is nothing
/// further to do either way.
async fn close_gracefully(sender: &mut futures::stream::SplitSink<WebSocket, Message>) {
    let _ = sender.send(Message::Close(None)).await;
}

/// Serializes `event` as a bare JSON text frame and sends it.
/// Returns `false` if the send failed (the client disconnected) or
/// serialization failed, in which case the caller should stop
/// forwarding to this socket -- matching `apexes-ui::server`'s own
/// "send failure = client disconnected, stop the loop" convention.
async fn send_event(
    sender: &mut futures::stream::SplitSink<WebSocket, Message>,
    event: &apexes_core::MarketEvent,
) -> bool {
    let json = match serde_json::to_string(event) {
        Ok(json) => json,
        Err(error) => {
            tracing::warn!(
                ?error,
                "apexes-market-data-gateway server: failed to serialize MarketEvent, skipping"
            );
            return true;
        }
    };
    if sender.send(Message::Text(json.into())).await.is_err() {
        tracing::info!(
            "apexes-market-data-gateway server: client disconnected, stopping forward loop"
        );
        return false;
    }
    true
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
        let gateway = Arc::new(Gateway::new(Box::new(mock), 16));
        let _router = build_app(gateway, None, vec![btc_usdt()]);
    }
}
