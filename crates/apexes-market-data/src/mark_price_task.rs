//! [`run`]: subscribes to Binance's public `<symbol>@markPrice@1s`
//! combined WebSocket stream for `symbols` (USDS-M Perpetual only),
//! converting each message into `MarketEvent::MarkPrice`. Hand-rolled
//! raw WebSocket (via `tokio-tungstenite`, mirroring
//! `apexes_gateway_binance::user_data_stream`'s existing pattern) --
//! `barter-data` 0.11 has no mark-price subscription kind at all. See
//! `docs/superpowers/specs/2026-08-02-apexes-mark-price-pipeline-design.md`
//! §3.2 for the raw message shape.

use crate::supervised::{self, Flow, Timings};
use apexes_core::{MarkPriceUpdate, MarketEvent, Venue};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use futures::stream::LocalBoxStream;
use rust_decimal::Decimal;
use serde_json::Value;
use std::str::FromStr;
use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::tungstenite::Message;

const WS_BASE_URL: &str = "wss://fstream.binance.com/stream";

/// Runs until `symbols` is empty (returns immediately, matching the
/// other tasks' "empty subscription list -> warn and return" behavior)
/// or until the output channel closes. The connection is supervised
/// (`crate::supervised`): an error, a close, a failed connect, or a socket
/// that goes silent all lead to a backed-off reconnect. Blocks the calling
/// thread.
pub fn run(symbols: Vec<String>, tx: UnboundedSender<MarketEvent>) {
    if symbols.is_empty() {
        tracing::warn!("mark_price_task: no symbols provided, nothing to subscribe to");
        return;
    }
    let stream_names: Vec<String> = symbols
        .iter()
        .map(|s| format!("{}@markPrice@1s", s.to_lowercase()))
        .collect();
    let url = format!("{WS_BASE_URL}?streams={}", stream_names.join("/"));
    run_at(url, Timings::default(), tx);
}

/// [`run`] against an explicit URL and timings (tests point it at a local
/// server and shorten the idle window).
fn run_at(url: String, timings: Timings, tx: UnboundedSender<MarketEvent>) {
    supervised::run_isolated(
        "mark_price_task",
        timings,
        move || connect(url.clone()),
        move |event: MarketEvent| {
            if tx.send(event).is_err() {
                tracing::warn!("mark_price_task: output channel closed, stopping");
                Flow::Stop
            } else {
                Flow::Continue
            }
        },
    );
}

/// Opens the WebSocket and yields parsed mark-price events. Ping/pong and
/// other control frames are answered by the library and filtered out here,
/// so only real data counts as activity; a close or error ends the stream.
async fn connect(url: String) -> Option<LocalBoxStream<'static, MarketEvent>> {
    match tokio_tungstenite::connect_async(&url).await {
        Ok((socket, _response)) => {
            tracing::info!("mark_price_task: connected");
            let (_write, read) = socket.split();
            Some(
                read.take_while(|message| std::future::ready(message.is_ok()))
                    .filter_map(|message| {
                        std::future::ready(match message {
                            Ok(Message::Text(text)) => parse_mark_price_message(&text),
                            _ => None,
                        })
                    })
                    .boxed_local(),
            )
        }
        Err(error) => {
            tracing::warn!(?error, "mark_price_task: failed to connect");
            None
        }
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

    /// The failure from the field, against a real socket: the server delivers
    /// one update and then goes silent with the connection left open (no
    /// close, no reset). The task must notice, reconnect, and get data again.
    #[test]
    fn a_connection_that_goes_silent_but_stays_open_is_replaced() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        let accepted = Arc::new(AtomicUsize::new(0));
        let server_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let listener = server_runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        let counter = accepted.clone();
        server_runtime.spawn(async move {
            use futures::SinkExt;
            loop {
                let Ok((tcp, _)) = listener.accept().await else {
                    return;
                };
                let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
                tokio::spawn(async move {
                    let Ok(mut ws) = tokio_tungstenite::accept_async(tcp).await else {
                        return;
                    };
                    let message = sample_message().replace("65012.34", &format!("{}", 65000 + n));
                    let _ = ws.send(Message::Text(message.into())).await;
                    // Silent from here on, connection held open.
                    std::future::pending::<()>().await;
                });
            }
        });

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let timings = Timings {
            idle: Duration::from_millis(400),
            connect: Duration::from_secs(5),
            backoff_initial: Duration::from_millis(50),
            backoff_max: Duration::from_millis(200),
        };
        std::thread::spawn(move || {
            run_at(format!("ws://127.0.0.1:{port}/stream"), timings, tx);
        });

        let mut prices = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while prices.len() < 3 && std::time::Instant::now() < deadline {
            if let Ok(MarketEvent::MarkPrice(m)) = rx.try_recv() {
                prices.push(m.mark_price);
            } else {
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        assert_eq!(
            prices.len(),
            3,
            "data kept arriving across silent connections (got {prices:?})"
        );
        assert!(
            accepted.load(Ordering::SeqCst) >= 3,
            "each silent connection was dropped and replaced"
        );
        drop(rx);
    }
}
