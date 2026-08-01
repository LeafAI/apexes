//! [`run`]: subscribes to Binance's public `<symbol>@markPrice@1s`
//! combined WebSocket stream for `symbols` (USDS-M Perpetual only),
//! converting each message into `MarketEvent::MarkPrice`. Hand-rolled
//! raw WebSocket (via `tokio-tungstenite`, mirroring
//! `apexes_gateway_binance::user_data_stream`'s existing pattern) --
//! `barter-data` 0.11 has no mark-price subscription kind at all. See
//! `docs/superpowers/specs/2026-08-02-apexes-mark-price-pipeline-design.md`
//! §3.2 for the raw message shape.

use apexes_core::{MarkPriceUpdate, MarketEvent, Venue};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use rust_decimal::Decimal;
use serde_json::Value;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::tungstenite::Message;

const WS_BASE_URL: &str = "wss://fstream.binance.com/stream";
/// Delay before attempting to reconnect after the WebSocket connection
/// drops -- avoids a tight reconnect loop hammering Binance during an
/// outage. `barter-data`'s tasks get equivalent backoff for free from
/// the library; this hand-rolled task must implement its own.
const RECONNECT_DELAY: Duration = Duration::from_secs(2);

/// Runs until `symbols` is empty (returns immediately, matching the
/// other tasks' "empty subscription list -> warn and return" behavior)
/// or forever otherwise, reconnecting automatically on any WebSocket
/// error or unexpected close.
pub async fn run(symbols: Vec<String>, tx: UnboundedSender<MarketEvent>) {
    if symbols.is_empty() {
        tracing::warn!("mark_price_task: no symbols provided, nothing to subscribe to");
        return;
    }

    let stream_names: Vec<String> = symbols
        .iter()
        .map(|s| format!("{}@markPrice@1s", s.to_lowercase()))
        .collect();
    let url = format!("{WS_BASE_URL}?streams={}", stream_names.join("/"));

    loop {
        match tokio_tungstenite::connect_async(&url).await {
            Ok((socket, _response)) => {
                tracing::info!(?symbols, "mark_price_task: connected");
                let (_write, mut read) = socket.split();
                loop {
                    match read.next().await {
                        Some(Ok(Message::Text(text))) => {
                            if let Some(event) = parse_mark_price_message(&text)
                                && tx.send(event).is_err()
                            {
                                tracing::warn!("mark_price_task: output channel closed, stopping");
                                return;
                            }
                        }
                        Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                        Some(Ok(_)) => {}
                        Some(Err(error)) => {
                            tracing::warn!(
                                ?error,
                                "mark_price_task: WebSocket error, reconnecting"
                            );
                            break;
                        }
                        None => {
                            tracing::warn!("mark_price_task: WebSocket stream ended, reconnecting");
                            break;
                        }
                    }
                }
            }
            Err(error) => {
                tracing::warn!(?error, "mark_price_task: failed to connect, retrying");
            }
        }
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}

/// Parses one raw combined-stream text message
/// (`{"stream": "<symbol>@markPrice@1s", "data": {"e": "markPriceUpdate",
/// "E": ..., "s": "BTCUSDT", "p": "...", ...}}`) into a
/// `MarketEvent::MarkPrice`, or `None` if the message isn't a
/// `markPriceUpdate` event or is malformed. Errors are logged, not
/// panicked on -- a single malformed message must not crash ingestion.
fn parse_mark_price_message(raw: &str) -> Option<MarketEvent> {
    let outer: Value = serde_json::from_str(raw).ok()?;
    let data = outer.get("data")?;

    if data.get("e")?.as_str()? != "markPriceUpdate" {
        return None;
    }

    let symbol = data.get("s")?.as_str()?.to_string();
    let mark_price_str = data.get("p")?.as_str()?;
    let mark_price = Decimal::from_str(mark_price_str).ok()?;
    let event_time_ms = data.get("E")?.as_i64()?;
    let timestamp = DateTime::<Utc>::from_timestamp_millis(event_time_ms)?;

    Some(MarketEvent::MarkPrice(MarkPriceUpdate {
        symbol,
        venue: Venue::Binance,
        mark_price,
        timestamp,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_message() -> String {
        r#"{"stream":"btcusdt@markPrice@1s","data":{"e":"markPriceUpdate","E":1234567890000,"s":"BTCUSDT","p":"65012.34000000","P":"65010.00000000","r":"0.00010000","T":1234598400000}}"#.to_string()
    }

    #[test]
    fn parses_a_valid_mark_price_update() {
        let event = parse_mark_price_message(&sample_message()).expect("should parse");
        let MarketEvent::MarkPrice(update) = event else {
            panic!("expected MarketEvent::MarkPrice");
        };
        assert_eq!(update.symbol, "BTCUSDT");
        assert_eq!(update.venue, Venue::Binance);
        assert_eq!(
            update.mark_price,
            Decimal::from_str("65012.34000000").unwrap()
        );
        assert_eq!(
            update.timestamp,
            DateTime::<Utc>::from_timestamp_millis(1234567890000).unwrap()
        );
    }

    #[test]
    fn returns_none_for_a_non_mark_price_event_type() {
        let raw = r#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1234567890000,"s":"BTCUSDT"}}"#;
        assert!(parse_mark_price_message(raw).is_none());
    }

    #[test]
    fn returns_none_for_malformed_json() {
        assert!(parse_mark_price_message("not json").is_none());
    }

    #[test]
    fn returns_none_when_price_field_is_not_a_valid_decimal() {
        let raw = r#"{"stream":"btcusdt@markPrice@1s","data":{"e":"markPriceUpdate","E":1234567890000,"s":"BTCUSDT","p":"not-a-number"}}"#;
        assert!(parse_mark_price_message(raw).is_none());
    }
}
