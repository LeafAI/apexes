//! Explicitly opt-in, single-market public raw depth capture. Not wired
//! into the default trading provider. Publish only synchronized books;
//! preserve source range IDs and causal publication time.
use crate::{
    provider::BinanceExchangeKind,
    raw_depth,
    supervised::{Backoff, Timings},
};
use apexes_core::{DepthUpdate, MarketEvent, Venue, binance_network::BinanceNetwork};
use chrono::{DateTime, Utc};
use futures::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use std::{
    collections::{BTreeMap, VecDeque},
    time::Duration,
};
use tokio::{
    sync::mpsc,
    time::{Instant, timeout},
};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

const MAX_BUFFER: usize = 4096;
const SNAPSHOT_LIMIT: u32 = 1000;
const MAX_BUFFERED_LEVELS: usize = 100_000;
const MAX_BOOK_LEVELS: usize = 20_000;
const MAX_FRAME_BYTES: usize = 1024 * 1024;

#[derive(Debug, Default)]
struct Book {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
}

impl Book {
    fn apply(&mut self, update: &DepthUpdate) -> Result<(), String> {
        if update.is_snapshot {
            self.bids.clear();
            self.asks.clear();
        }
        for (side, levels) in [
            (&mut self.bids, &update.bids),
            (&mut self.asks, &update.asks),
        ] {
            for level in levels {
                if level.quantity == Decimal::ZERO {
                    side.remove(&level.price);
                } else {
                    side.insert(level.price, level.quantity);
                }
            }
            if side.len() > MAX_BOOK_LEVELS {
                return Err("raw depth book level limit exceeded".into());
            }
        }
        match (self.bids.last_key_value(), self.asks.first_key_value()) {
            (Some((bid, _)), Some((ask, _))) if bid < ask => Ok(()),
            _ => Err("raw depth book is empty on one side or crossed".into()),
        }
    }
}

#[derive(Debug)]
struct Synchronizer {
    symbol: String,
    exchange: BinanceExchangeKind,
    snapshot: Option<DepthUpdate>,
    buffered: VecDeque<DepthUpdate>,
    last: Option<u64>,
    max_buffer: usize,
    buffered_levels: usize,
    book: Book,
    published_at: Option<DateTime<Utc>>,
}

impl Synchronizer {
    fn new(symbol: String, exchange: BinanceExchangeKind, max_buffer: usize) -> Self {
        Self {
            symbol,
            exchange,
            snapshot: None,
            buffered: VecDeque::new(),
            last: None,
            max_buffer,
            buffered_levels: 0,
            book: Book::default(),
            published_at: None,
        }
    }

    fn delta(&mut self, raw: &str, at: DateTime<Utc>) -> Result<Vec<DepthUpdate>, String> {
        let update = raw_depth::decode_delta_at(raw, &self.symbol, self.exchange, at)?;
        if let Some(last) = self.last {
            // Ignore stale retransmissions without refreshing freshness.
            if update.sequence.is_some_and(|id| id <= last) {
                return Ok(vec![]);
            }
            self.check_link(&update, last, false)?;
            self.book.apply(&update)?;
            self.last = update.sequence;
            return Ok(vec![self.stamp(update, at)]);
        }
        if self.buffered.len() >= self.max_buffer {
            return Err("raw depth initialization buffer exhausted".into());
        }
        self.buffered_levels = self
            .buffered_levels
            .checked_add(update.bids.len())
            .and_then(|n| n.checked_add(update.asks.len()))
            .filter(|n| *n <= MAX_BUFFERED_LEVELS)
            .ok_or("raw depth initialization level buffer exhausted")?;
        self.buffered.push_back(update);
        self.try_bridge(at)
    }

    fn snapshot(&mut self, raw: &str, at: DateTime<Utc>) -> Result<Vec<DepthUpdate>, String> {
        if self.snapshot.is_some() || self.last.is_some() {
            return Err("unexpected second snapshot in connection attempt".into());
        }
        self.snapshot = Some(raw_depth::decode_snapshot(raw, &self.symbol, at)?);
        self.try_bridge(at)
    }

    fn check_link(&self, update: &DepthUpdate, last: u64, first: bool) -> Result<(), String> {
        let range = update.source_range.ok_or("missing original range")?;
        if range.last <= last {
            return Err("nonadvancing range".into());
        }
        let valid = match self.exchange {
            BinanceExchangeKind::Spot => last
                .checked_add(1)
                .is_some_and(|next| range.first <= next && next <= range.last),
            BinanceExchangeKind::Perpetual if first => {
                range.first <= last && range.previous_final.is_some_and(|p| p <= last)
            }
            BinanceExchangeKind::Perpetual => range.previous_final == Some(last),
        };
        if !valid {
            return Err("depth range gap or invalid previous-final link".into());
        }
        Ok(())
    }

    fn stamp(&mut self, mut update: DepthUpdate, at: DateTime<Utc>) -> DepthUpdate {
        // Buffered deltas become observable only when the complete bridge
        // is validated. Never move the stream clock backwards.
        let at = at
            .max(update.timestamp)
            .max(self.published_at.unwrap_or(at));
        update.timestamp = at;
        self.published_at = Some(at);
        update
    }

    fn try_bridge(&mut self, at: DateTime<Utc>) -> Result<Vec<DepthUpdate>, String> {
        let Some(snapshot) = self.snapshot.as_ref() else {
            return Ok(vec![]);
        };
        let anchor = snapshot.sequence.ok_or("snapshot missing sequence")?;
        while self
            .buffered
            .front()
            .is_some_and(|d| d.sequence.is_some_and(|id| id <= anchor))
        {
            if let Some(stale) = self.buffered.pop_front() {
                self.buffered_levels -= stale.bids.len() + stale.asks.len();
            }
        }
        if self.buffered.is_empty() {
            return Ok(vec![]);
        }
        // Validate the whole currently buffered advancing chain before
        // emitting even the snapshot. No partially qualified startup.
        let mut book = Book::default();
        book.apply(snapshot)?;
        let mut last = anchor;
        let mut first = true;
        for update in &self.buffered {
            if update.sequence.is_some_and(|id| id <= last) {
                continue;
            }
            self.check_link(update, last, first)?;
            book.apply(update)?;
            last = update.sequence.ok_or("missing delta sequence")?;
            first = false;
        }
        if first {
            return Ok(vec![]);
        }
        let snapshot = self.snapshot.take().ok_or("missing snapshot")?;
        let mut result = vec![self.stamp(snapshot, at)];
        let mut prior = anchor;
        while let Some(update) = self.buffered.pop_front() {
            let id = update.sequence.ok_or("missing delta sequence")?;
            if id <= prior {
                continue;
            }
            prior = id;
            result.push(self.stamp(update, at));
        }
        self.last = Some(last);
        self.book = book;
        self.buffered_levels = 0;
        Ok(result)
    }
}

#[derive(Clone)]
struct Endpoints {
    websocket: String,
    snapshot: String,
}

fn endpoints(symbol: &str, exchange: BinanceExchangeKind) -> Result<Endpoints, String> {
    if symbol.is_empty()
        || !symbol
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
    {
        return Err("expected canonical uppercase Binance symbol".into());
    }
    let spot = BinanceNetwork::Mainnet.endpoints();
    let (ws, rest, path) = match exchange {
        BinanceExchangeKind::Spot => (spot.spot_market_ws, spot.spot_rest, "/api/v3/depth"),
        BinanceExchangeKind::Perpetual => (
            "wss://fstream.binance.com/ws",
            "https://fapi.binance.com",
            "/fapi/v1/depth",
        ),
    };
    Ok(Endpoints {
        websocket: format!("{ws}/{}@depth@100ms", symbol.to_ascii_lowercase()),
        snapshot: format!("{rest}{path}?symbol={symbol}&limit={SNAPSHOT_LIMIT}"),
    })
}

fn invalidation(symbol: &str, at: DateTime<Utc>) -> MarketEvent {
    MarketEvent::Depth(DepthUpdate {
        symbol: symbol.into(),
        venue: Venue::Binance,
        bids: vec![],
        asks: vec![],
        sequence: None,
        source_range: None,
        is_snapshot: true,
        timestamp: at,
    })
}

#[derive(Debug)]
enum End {
    Closed,
    ConsumerStalled,
    Retry { healthy: bool, reason: String },
}

// Reserve the whole batch before publishing. Never wait behind a slow
// consumer or publish a partially synchronized startup batch.
fn publish(
    tx: &mpsc::Sender<MarketEvent>,
    updates: Vec<DepthUpdate>,
    published: &mut Option<DateTime<Utc>>,
) -> Result<(), End> {
    if updates.is_empty() {
        return Ok(());
    }
    let permits = tx
        .try_reserve_many(updates.len())
        .map_err(|error| match error {
            mpsc::error::TrySendError::Closed(_) => End::Closed,
            mpsc::error::TrySendError::Full(_) => End::ConsumerStalled,
        })?;
    for (permit, mut update) in permits.zip(updates) {
        let at = Utc::now()
            .max(update.timestamp)
            .max(published.unwrap_or(DateTime::<Utc>::MIN_UTC));
        update.timestamp = at;
        permit.send(MarketEvent::Depth(update));
        *published = Some(at);
    }
    Ok(())
}

struct CaptureSession<'a> {
    symbol: &'a str,
    exchange: BinanceExchangeKind,
    endpoints: &'a Endpoints,
    client: &'a reqwest::Client,
    timings: Timings,
    tx: &'a mpsc::Sender<MarketEvent>,
}

async fn attempt(session: &CaptureSession<'_>, published: &mut Option<DateTime<Utc>>) -> End {
    let CaptureSession {
        symbol,
        exchange,
        endpoints,
        client,
        timings,
        tx,
    } = *session;
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_FRAME_BYTES))
        .max_frame_size(Some(MAX_FRAME_BYTES));
    let result = tokio::select! {
        _ = tx.closed() => return End::Closed,
        result = timeout(timings.connect, connect_async_with_config(&endpoints.websocket, Some(config), false)) => result,
    };
    let mut socket = match result {
        Ok(Ok((socket, _))) => socket,
        _ => {
            return End::Retry {
                healthy: false,
                reason: "raw depth websocket connect failed or timed out".into(),
            };
        }
    };
    let mut sync = Synchronizer::new(symbol.into(), exchange, MAX_BUFFER);
    // Start REST only after websocket handshake; continue reading WS
    // while the snapshot request is outstanding.
    let request = async {
        let response = client
            .get(&endpoints.snapshot)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .error_for_status()
            .map_err(|e| e.to_string())?;
        // Bound the response before JSON allocation; no unbounded body.
        let mut response = response;
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|e| e.to_string())? {
            if body.len().saturating_add(chunk.len()) > 4 * 1024 * 1024 {
                return Err("depth snapshot exceeds size limit".into());
            }
            body.extend_from_slice(&chunk);
        }
        String::from_utf8(body).map_err(|e| e.to_string())
    };
    tokio::pin!(request);
    let init_deadline = Instant::now() + timings.connect;
    let mut snapshot_received = false;
    let mut healthy = false;
    let mut last_data = Instant::now();
    loop {
        let deadline = if healthy {
            last_data + timings.idle
        } else {
            init_deadline
        };
        let updates = tokio::select! {
            _ = tx.closed() => return End::Closed,
            _ = tokio::time::sleep_until(deadline) => return End::Retry { healthy, reason: "depth initialization or data idle timeout".into() },
            result = &mut request, if !snapshot_received => {
                snapshot_received = true;
                result.and_then(|raw| sync.snapshot(&raw, Utc::now().max(published.unwrap_or(DateTime::<Utc>::MIN_UTC))))
            },
            message = socket.next() => match message {
                Some(Ok(Message::Text(raw))) => {
                    sync.delta(&raw, Utc::now().max(published.unwrap_or(DateTime::<Utc>::MIN_UTC)))
                }
                Some(Ok(Message::Ping(payload))) => {
                    if !matches!(timeout(deadline.saturating_duration_since(Instant::now()), socket.send(Message::Pong(payload))).await, Ok(Ok(()))) {
                        return End::Retry { healthy, reason: "depth websocket pong failed".into() };
                    }
                    continue;
                }
                Some(Ok(Message::Pong(_))) => continue,
                _ => return End::Retry { healthy, reason: "depth websocket closed or unexpected frame".into() },
            },
        };
        match updates {
            Ok(updates) => {
                if !updates.is_empty() {
                    if let Err(end) = publish(tx, updates, published) {
                        return end;
                    }
                    healthy = true;
                    last_data = Instant::now();
                }
            }
            Err(reason) => return End::Retry { healthy, reason },
        }
    }
}

/// Capture one public Binance market on an explicitly requested stream.
/// Use a dedicated bounded receiver, never merge Spot and USD-M streams
/// for the same symbol into one historical source namespace. The default
/// trading provider remains unchanged. Stops when the receiver closes.
pub async fn capture(
    symbol: &str,
    exchange: BinanceExchangeKind,
    tx: mpsc::Sender<MarketEvent>,
) -> Result<(), String> {
    let endpoints = endpoints(symbol, exchange)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .map_err(|e| e.to_string())?;
    capture_at(symbol, exchange, endpoints, client, Timings::default(), tx).await
}

async fn capture_at(
    symbol: &str,
    exchange: BinanceExchangeKind,
    endpoints: Endpoints,
    client: reqwest::Client,
    timings: Timings,
    tx: mpsc::Sender<MarketEvent>,
) -> Result<(), String> {
    let mut backoff = Backoff::new(timings);
    let mut published = None;
    while !tx.is_closed() {
        let session = CaptureSession {
            symbol,
            exchange,
            endpoints: &endpoints,
            client: &client,
            timings,
            tx: &tx,
        };
        match attempt(&session, &mut published).await {
            End::Closed => return Ok(()),
            End::ConsumerStalled => {
                return Err(
                    "raw depth output buffer full; capture terminated, consumer must discard book"
                        .into(),
                );
            }
            End::Retry { healthy, reason } => {
                tracing::warn!(%symbol, %reason, "raw depth capture attempt ended");
                // Invalidate before backoff. If the consumer cannot
                // accept the invalidation, terminate with an error.
                let at = Utc::now().max(published.unwrap_or(DateTime::<Utc>::MIN_UTC));
                match tx.try_send(invalidation(symbol, at)) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Closed(_)) => return Ok(()),
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        return Err(
                            "raw depth output buffer full during invalidation; capture terminated"
                                .into(),
                        );
                    }
                }
                published = Some(at);
                tokio::select! {
                    _ = tx.closed() => return Ok(()),
                    _ = tokio::time::sleep(backoff.after(healthy)) => {}
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    fn at(ms: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(1_700_000_000_000 + ms).unwrap()
    }

    fn snapshot(last: u64) -> String {
        format!(r#"{{"lastUpdateId":{last},"bids":[["99","1"]],"asks":[["101","1"]]}}"#)
    }

    fn delta(first: u64, last: u64, pu: Option<u64>) -> String {
        let link = pu.map(|p| format!(r#","pu":{p}"#)).unwrap_or_default();
        format!(
            r#"{{"e":"depthUpdate","E":1700000000000,"s":"BTCUSDT","U":{first},"u":{last}{link},"b":[["99","2"]],"a":[]}}"#
        )
    }

    fn sync(exchange: BinanceExchangeKind) -> Synchronizer {
        Synchronizer::new("BTCUSDT".into(), exchange, 8)
    }

    #[test]
    fn spot_buffers_until_snapshot_bridge_and_keeps_original_ranges() {
        let mut s = sync(BinanceExchangeKind::Spot);
        assert!(s.delta(&delta(90, 99, None), at(1)).unwrap().is_empty());
        assert!(s.delta(&delta(99, 103, None), at(2)).unwrap().is_empty());
        let out = s.snapshot(&snapshot(100), at(3)).unwrap();
        assert_eq!(out.len(), 2);
        assert!(out[0].is_snapshot);
        assert_eq!(out[0].sequence, Some(100));
        assert_eq!(out[1].source_range.unwrap().first, 99);
        assert_eq!(out[1].sequence, Some(103));
        assert_eq!(out[0].timestamp, at(3));
        assert_eq!(out[1].timestamp, at(3));
        assert!(s.delta(&delta(99, 103, None), at(4)).unwrap().is_empty());
        let out = s.delta(&delta(104, 110, None), at(5)).unwrap();
        assert_eq!(out[0].sequence, Some(110));
        assert_eq!(s.book.bids[&dec!(99)], dec!(2));
        assert!(s.delta(&delta(112, 113, None), at(6)).is_err());
    }

    #[test]
    fn snapshot_waits_for_advancing_update_and_rejects_initial_gap() {
        let mut s = sync(BinanceExchangeKind::Spot);
        assert!(s.snapshot(&snapshot(100), at(1)).unwrap().is_empty());
        assert!(s.delta(&delta(95, 100, None), at(2)).unwrap().is_empty());
        assert!(s.delta(&delta(102, 103, None), at(3)).is_err());
        let mut s = sync(BinanceExchangeKind::Spot);
        s.delta(&delta(99, 103, None), at(1)).unwrap();
        s.delta(&delta(105, 110, None), at(2)).unwrap();
        assert!(s.snapshot(&snapshot(100), at(3)).is_err());
        assert!(s.last.is_none());
    }

    #[test]
    fn futures_bridges_snapshot_then_requires_previous_final() {
        let mut s = sync(BinanceExchangeKind::Perpetual);
        s.delta(&delta(99, 103, Some(98)), at(1)).unwrap();
        let out = s.snapshot(&snapshot(100), at(2)).unwrap();
        assert_eq!(out[1].source_range.unwrap().previous_final, Some(98));
        assert_eq!(
            s.delta(&delta(108, 110, Some(103)), at(3)).unwrap().len(),
            1
        );
        assert!(s.delta(&delta(111, 114, Some(109)), at(4)).is_err());
        let mut s = sync(BinanceExchangeKind::Perpetual);
        s.snapshot(&snapshot(100), at(1)).unwrap();
        assert!(s.delta(&delta(101, 103, Some(100)), at(2)).is_err());
    }

    #[test]
    fn buffer_and_book_integrity_fail_closed() {
        let mut s = Synchronizer::new("BTCUSDT".into(), BinanceExchangeKind::Spot, 1);
        s.delta(&delta(99, 103, None), at(1)).unwrap();
        assert!(
            s.delta(&delta(104, 105, None), at(2))
                .unwrap_err()
                .contains("buffer")
        );
        let mut s = sync(BinanceExchangeKind::Spot);
        s.snapshot(&snapshot(100), at(1)).unwrap();
        let crossed = delta(101, 103, None).replace(r#"["99","2"]"#, r#"["102","2"]"#);
        assert!(s.delta(&crossed, at(2)).unwrap_err().contains("crossed"));
        let mut s = sync(BinanceExchangeKind::Spot);
        s.snapshot(&snapshot(100), at(1)).unwrap();
        s.delta(&delta(101, 103, None), at(2)).unwrap();
        let deleted = delta(104, 105, None).replace(r#"["99","2"]"#, r#"["99","0"]"#);
        assert!(s.delta(&deleted, at(3)).unwrap_err().contains("empty"));
    }

    #[test]
    fn publication_is_atomic_on_insufficient_channel_capacity() {
        let mut s = sync(BinanceExchangeKind::Spot);
        s.delta(&delta(99, 103, None), at(1)).unwrap();
        let out = s.snapshot(&snapshot(100), at(2)).unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        let mut published = None;
        assert!(matches!(
            publish(&tx, out, &mut published),
            Err(End::ConsumerStalled)
        ));
        assert!(published.is_none());
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn production_endpoints_are_market_specific_and_symbols_are_validated() {
        let spot = endpoints("BTCUSDT", BinanceExchangeKind::Spot).unwrap();
        let futures = endpoints("BTCUSDT", BinanceExchangeKind::Perpetual).unwrap();
        assert!(
            spot.websocket
                .contains("stream.binance.com:9443/ws/btcusdt@depth@100ms")
        );
        assert!(spot.snapshot.contains("/api/v3/depth?symbol=BTCUSDT"));
        assert!(
            futures
                .websocket
                .contains("fstream.binance.com/ws/btcusdt@depth@100ms")
        );
        assert!(futures.snapshot.contains("/fapi/v1/depth?symbol=BTCUSDT"));
        assert!(endpoints("BTC/USDT", BinanceExchangeKind::Spot).is_err());
    }

    #[tokio::test]
    async fn supervised_capture_reconnects_from_fresh_snapshot_after_gap() {
        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rest = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoints = Endpoints {
            websocket: format!("ws://{}", ws.local_addr().unwrap()),
            snapshot: format!("http://{}", rest.local_addr().unwrap()),
        };
        let (ready_tx, mut ready_rx) = mpsc::channel(2);
        let ws_task = tokio::spawn(async move {
            for (anchor, last) in [(100, 103), (200, 203)] {
                let (socket, _) = ws.accept().await.unwrap();
                let mut socket = tokio_tungstenite::accept_async(socket).await.unwrap();
                socket
                    .send(Message::Text(delta(anchor - 1, last, None).into()))
                    .await
                    .unwrap();
                ready_tx.send(anchor).await.unwrap();
                // Wait until the REST snapshot was sent and the caller
                // actually observed the synchronized startup.
                if anchor == 100 {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    socket
                        .send(Message::Text(delta(105, 106, None).into()))
                        .await
                        .unwrap();
                } else {
                    // Keep the second connection alive until capture is stopped.
                    let _ = socket.next().await;
                }
            }
        });
        let rest_task = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut socket, _) = rest.accept().await.unwrap();
                let mut request = Vec::new();
                loop {
                    let mut b = [0; 512];
                    let n = socket.read(&mut b).await.unwrap();
                    assert!(n > 0);
                    request.extend_from_slice(&b[..n]);
                    if request.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                let anchor = ready_rx.recv().await.unwrap();
                let body = snapshot(anchor);
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            }
        });
        let (tx, mut rx) = mpsc::channel(16);
        let timings = Timings {
            idle: Duration::from_secs(2),
            connect: Duration::from_secs(2),
            backoff_initial: Duration::from_millis(10),
            backoff_max: Duration::from_millis(20),
        };
        let capture = tokio::spawn(async move {
            capture_at(
                "BTCUSDT",
                BinanceExchangeKind::Spot,
                endpoints,
                reqwest::Client::builder().no_proxy().build().unwrap(),
                timings,
                tx,
            )
            .await
        });
        let mut received = vec![];
        for _ in 0..5 {
            let event = timeout(Duration::from_secs(5), rx.recv())
                .await
                .unwrap()
                .unwrap();
            let MarketEvent::Depth(d) = event else {
                panic!("not depth")
            };
            received.push(d);
        }
        assert_eq!(
            received.iter().map(|d| d.sequence).collect::<Vec<_>>(),
            vec![Some(100), Some(103), None, Some(200), Some(203)]
        );
        assert!(received[2].is_invalidation());
        assert!(
            received
                .windows(2)
                .all(|pair| pair[0].timestamp <= pair[1].timestamp)
        );
        drop(rx);
        assert!(
            timeout(Duration::from_secs(3), capture)
                .await
                .unwrap()
                .unwrap()
                .is_ok()
        );
        rest_task.await.unwrap();
        ws_task.abort();
    }
}
