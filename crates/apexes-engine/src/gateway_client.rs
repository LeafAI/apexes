//! [`GatewayMarketDataProvider`]: adapts
//! `apexes-market-data-gateway`'s `GET /market` WebSocket endpoint
//! (row 8.3's minimum-viable message-bus gateway) into an
//! `apexes_core::MarketDataProvider`, so `main.rs` can wrap it in a
//! real `apexes_message_bus::MarketEventBus` for both `SessionMode::Paper`
//! (`run_paper_trading` takes `&MarketEventBus` directly) and
//! `SessionMode::Live` (`run_live_trading` looks up
//! `Arc<MarketEventBus>` instances from its `Registry`, keyed by
//! `Venue` -- see `crate::main`'s Live-mode wiring for how this
//! adapter's single instance gets registered under every venue
//! present in `LaunchConfig.instruments`).
//!
//! Deliberately NOT `apexes_message_bus::MarketEventBus`'s own
//! internal upstream-connecting logic (that connects to a real
//! exchange provider, e.g. `BinanceMarketDataProvider`) -- this
//! adapter instead connects to ANOTHER WebSocket server (the gateway
//! process) acting as a relay, mirroring `apexes-ui`'s existing
//! `client.rs` (13a) connect-and-deserialize-`MarketEvent`-frames
//! logic almost exactly (same reconnect-with-backoff shape), since the
//! gateway's `/market` wire protocol is identical to `apexes-ui`'s own
//! `/ws` protocol (bare `serde_json`-serialized `MarketEvent` frames,
//! no envelope -- see `apexes-market-data-gateway`'s `protocol.rs`).

use apexes_core::binance_network::BinanceNetwork;
use apexes_core::{ApexesError, Instrument, MarketDataProvider, MarketEvent, Venue};
use apexes_oms::KillSwitch;
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(30);
const IO_TIMEOUT: Duration = Duration::from_secs(5);
const EVENT_QUEUE_CAPACITY: usize = 1024;

#[derive(Clone)]
struct LiveGuard {
    kill_switch: Arc<KillSwitch>,
    max_event_age: Duration,
}

impl LiveGuard {
    fn halt(&self, reason: &'static str) {
        self.kill_switch.engage(reason);
    }

    fn fresh(&self, event: &MarketEvent) -> bool {
        let timestamp = match event {
            MarketEvent::Trade(e) => e.timestamp,
            MarketEvent::Tick(e) => e.timestamp,
            MarketEvent::Depth(e) => e.timestamp,
            MarketEvent::Bar(e) => e.close_time,
            MarketEvent::MarkPrice(e) => e.timestamp,
        };
        let age = chrono::Utc::now().signed_duration_since(timestamp);
        age >= -chrono::Duration::seconds(5)
            && age.to_std().map_or(true, |age| age <= self.max_event_age)
    }
}

/// Tracks the exponential-backoff delay across reconnect attempts,
/// identical in shape to `apexes-ui`'s own `BackoffState`
/// (`crates/apexes-ui/src/client.rs`) -- not reused directly since
/// `apexes-engine` does not depend on `apexes-ui` (and should not:
/// `apexes-ui` spawns/supervises `apexes-engine`, never the reverse).
struct BackoffState {
    current: Duration,
}

impl BackoffState {
    fn new() -> Self {
        Self {
            current: INITIAL_BACKOFF,
        }
    }

    fn next_delay(&mut self) -> Duration {
        let delay = self.current;
        self.current = (self.current * 2).min(MAX_BACKOFF);
        delay
    }

    fn reset(&mut self) {
        self.current = INITIAL_BACKOFF;
    }
}

/// Adapts one gateway `/market` WebSocket connection into an
/// `apexes_core::MarketDataProvider`.
///
/// A gateway process streams a fixed instrument set (its own
/// `--instruments` flag) and may be shared by several sessions, so its
/// connection can carry symbols this session never asked for. `subscribe`
/// therefore keeps only events for the instruments it is given: a session
/// must never be fed another symbol's prices (the ledger would value its
/// position at the wrong price). An empty instrument list means "no
/// filter", which keeps the paper-mode and test callers that pass none
/// working. [`MarketEventBus::subscribe`] calls this exactly once.
pub struct GatewayMarketDataProvider {
    gateway_url: String,
    token: Option<String>,
    /// When set, the gateway must announce this network in its
    /// `x-apexes-network` response header on every connect.
    expected_network: Option<BinanceNetwork>,
    live_guard: Option<LiveGuard>,
}

/// Keeps only events for a set of symbols. `None`: keep everything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SymbolFilter(Option<std::collections::HashSet<String>>);

impl SymbolFilter {
    pub fn for_instruments(instruments: &[Instrument]) -> Self {
        if instruments.is_empty() {
            return Self(None);
        }
        Self(Some(instruments.iter().map(|i| i.symbol.clone()).collect()))
    }

    pub fn allows(&self, event: &MarketEvent) -> bool {
        self.0
            .as_ref()
            .is_none_or(|symbols| symbols.contains(event.symbol()))
    }
}

/// Response header a gateway uses to announce its Binance network. Kept in
/// step with `apexes_market_data_gateway::NETWORK_HEADER` (the engine does
/// not depend on the gateway crate); a test pins the two together.
pub const GATEWAY_NETWORK_HEADER: &str = "x-apexes-network";

/// What a live session concludes from a gateway's announced network.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkCheck {
    Matches,
    Mismatch {
        expected: BinanceNetwork,
        announced: String,
    },
    Unannounced {
        expected: BinanceNetwork,
    },
}

impl NetworkCheck {
    pub fn evaluate(expected: BinanceNetwork, announced: Option<&str>) -> Self {
        match announced {
            Some(value) if value == expected.as_str() => Self::Matches,
            Some(value) => Self::Mismatch {
                expected,
                announced: value.to_string(),
            },
            None => Self::Unannounced { expected },
        }
    }

    pub fn describe(&self) -> Option<String> {
        match self {
            Self::Matches => None,
            Self::Mismatch {
                expected,
                announced,
            } => Some(format!(
                "market-data gateway is on {announced} but this session is on {expected}; refusing to trade on the wrong network's prices"
            )),
            Self::Unannounced { expected } => Some(format!(
                "market-data gateway did not announce its network (older build?) but this session requires {expected}"
            )),
        }
    }
}

impl GatewayMarketDataProvider {
    pub fn new(gateway_url: impl Into<String>) -> Self {
        Self {
            gateway_url: gateway_url.into(),
            token: None,
            expected_network: None,
            live_guard: None,
        }
    }

    /// Requires the gateway to be on `network`: `subscribe` fails on a
    /// mismatch, and a gateway that later reconnects on another network ends
    /// the stream instead of feeding it wrong prices.
    pub fn with_expected_network(mut self, network: BinanceNetwork) -> Self {
        self.expected_network = Some(network);
        self
    }

    /// Fail closed on a transport gap or stale source event in a live session.
    pub fn with_live_guard(
        mut self,
        kill_switch: Arc<KillSwitch>,
        max_event_age: Duration,
    ) -> Self {
        self.live_guard = Some(LiveGuard {
            kill_switch,
            max_event_age,
        });
        self
    }

    /// Sends `Authorization: Bearer <token>` when connecting (for
    /// gateways started with `APEXES_GATEWAY_TOKEN`). `None`/empty: no header.
    pub fn with_token(mut self, token: Option<String>) -> Self {
        self.token = token.filter(|t| !t.trim().is_empty());
        self
    }
}

#[async_trait]
impl MarketDataProvider for GatewayMarketDataProvider {
    async fn subscribe(
        &self,
        instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, ApexesError> {
        let filter = SymbolFilter::for_instruments(instruments);
        let (tx, rx) = mpsc::channel(EVENT_QUEUE_CAPACITY);
        // With an expected network, connect once up front so a mismatch is
        // a `subscribe` error rather than something found in a log later.
        let first_socket = match self.expected_network {
            Some(expected) => Some(
                connect_checked(&self.gateway_url, self.token.as_deref(), expected)
                    .await
                    .map_err(|reason| ApexesError::SubscriptionFailed {
                        symbol: String::new(),
                        venue: Venue::Binance,
                        reason,
                        source_detail: None,
                    })?,
            ),
            None => None,
        };
        tokio::spawn(run_connection_loop(
            self.gateway_url.clone(),
            self.token.clone(),
            self.expected_network,
            first_socket,
            filter,
            self.live_guard.clone(),
            tx,
        ));

        // Adapts the `mpsc::Receiver` into a `Stream` via
        // `futures::stream::unfold` rather than pulling in
        // `tokio-stream`'s `ReceiverStream` wrapper as a new
        // dependency purely for this one call site.
        let stream = stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|event| (event, rx))
        });
        Ok(Box::pin(stream))
    }
}

/// Connects, attaching the bearer token header when configured. The error is
/// a plain string: callers only log it, and `tungstenite::Error` is large
/// enough to trip `clippy::result_large_err`.
type GatewaySocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Connects, then (when `expected` is set) verifies the gateway's announced
/// network, returning an error message that names both networks.
async fn connect_checked(
    url: &str,
    token: Option<&str>,
    expected: BinanceNetwork,
) -> Result<GatewaySocket, String> {
    let (socket, announced) = connect_with_headers(url, token).await?;
    match NetworkCheck::evaluate(expected, announced.as_deref()).describe() {
        None => Ok(socket),
        Some(reason) => Err(reason),
    }
}

async fn connect(url: &str, token: Option<&str>) -> Result<GatewaySocket, String> {
    connect_with_headers(url, token)
        .await
        .map(|(socket, _)| socket)
}

async fn connect_with_headers(
    url: &str,
    token: Option<&str>,
) -> Result<(GatewaySocket, Option<String>), String> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut request = url.into_client_request().map_err(|e| e.to_string())?;
    if let Some(token) = token {
        request.headers_mut().insert(
            "Authorization",
            apexes_security::bearer_header_value(token)
                .parse()
                .map_err(|_| "invalid characters in gateway token".to_string())?,
        );
    }
    tokio::time::timeout(IO_TIMEOUT, tokio_tungstenite::connect_async(request))
        .await
        .map_err(|_| "gateway handshake timed out".to_string())?
        .map(|(socket, response)| {
            let announced = response
                .headers()
                .get(GATEWAY_NETWORK_HEADER)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            (socket, announced)
        })
        .map_err(|e| e.to_string())
}

/// Runs forever: connects to `url`, forwards parsed `MarketEvent`s
/// into `tx`, and reconnects with backoff on any error or unexpected
/// close. Returns only if `tx` is closed (every receiver -- ultimately
/// `MarketEventBus`'s own internal forwarding task -- was dropped).
async fn run_connection_loop(
    url: String,
    token: Option<String>,
    expected_network: Option<BinanceNetwork>,
    mut first_socket: Option<GatewaySocket>,
    filter: SymbolFilter,
    live_guard: Option<LiveGuard>,
    tx: mpsc::Sender<MarketEvent>,
) {
    let mut backoff = BackoffState::new();
    loop {
        if tx.is_closed() {
            return;
        }
        let attempt = match first_socket.take() {
            Some(socket) => Ok(socket),
            None => match expected_network {
                Some(expected) => {
                    match connect_with_headers(&url, token.as_deref()).await {
                        Ok((socket, announced)) => {
                            match NetworkCheck::evaluate(expected, announced.as_deref()).describe()
                            {
                                None => Ok(socket),
                                Some(reason) => {
                                    // Wrong network after a reconnect is not
                                    // retried: ending the stream stops the
                                    // session rather than feeding it bad prices.
                                    tracing::error!(%reason, "apexes-engine gateway_client: stopping");
                                    if let Some(guard) = &live_guard {
                                        guard.halt("market data network mismatch");
                                    }
                                    return;
                                }
                            }
                        }
                        Err(error) => Err(error),
                    }
                }
                None => connect(&url, token.as_deref()).await,
            },
        };
        match attempt {
            Ok(mut socket) => {
                tracing::info!("apexes-engine gateway_client: connected");
                let reason = receive_connection(
                    &mut socket,
                    &filter,
                    live_guard.as_ref(),
                    &tx,
                    &mut backoff,
                )
                .await;
                if tx.is_closed() {
                    return;
                }
                tracing::warn!(reason, "market data gateway connection lost; reconnecting");
                if let Some(guard) = &live_guard {
                    guard.halt(reason);
                }
            }
            Err(error) => {
                tracing::warn!(
                    ?error,
                    "apexes-engine gateway_client: failed to connect, retrying"
                );
                if let Some(guard) = &live_guard {
                    guard.halt("market data gateway unavailable");
                }
            }
        }
        tokio::select! {
            _ = tx.closed() => return,
            _ = tokio::time::sleep(backoff.next_delay()) => {}
        }
    }
}

async fn receive_connection(
    socket: &mut GatewaySocket,
    filter: &SymbolFilter,
    guard: Option<&LiveGuard>,
    tx: &mpsc::Sender<MarketEvent>,
    backoff: &mut BackoffState,
) -> &'static str {
    let mut heartbeat = tokio::time::interval_at(
        tokio::time::Instant::now() + HEARTBEAT_INTERVAL,
        HEARTBEAT_INTERVAL,
    );
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut deadline = tokio::time::Instant::now() + HEARTBEAT_TIMEOUT;
    loop {
        tokio::select! {
            _ = tx.closed() => return "market data consumer closed",
            _ = tokio::time::sleep_until(deadline) => return "market data gateway heartbeat timed out",
            _ = heartbeat.tick() => {
                if !matches!(tokio::time::timeout(IO_TIMEOUT, socket.send(Message::Ping(Vec::new().into()))).await, Ok(Ok(()))) {
                    return "market data gateway heartbeat send failed";
                }
            }
            frame = socket.next() => {
                match frame {
                    Some(Ok(Message::Text(text))) => {
                        deadline = tokio::time::Instant::now() + HEARTBEAT_TIMEOUT;
                        let Ok(event) = serde_json::from_str::<MarketEvent>(&text) else {
                            return "market data gateway sent malformed event";
                        };
                        backoff.reset();
                        if !filter.allows(&event) { continue; }
                        if let Some(guard) = guard {
                            if !guard.fresh(&event) { return "market data gateway sent stale event"; }
                            if matches!(&event, MarketEvent::Depth(depth) if depth.is_invalidation()) {
                                guard.halt("market data order book invalidated");
                            }
                        }
                        if tx.try_send(event).is_err() {
                            return "market data gateway consumer queue overflow";
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {
                        deadline = tokio::time::Instant::now() + HEARTBEAT_TIMEOUT;
                        backoff.reset();
                    }
                    Some(Ok(Message::Ping(_))) => {
                        deadline = tokio::time::Instant::now() + HEARTBEAT_TIMEOUT;
                        if !matches!(tokio::time::timeout(IO_TIMEOUT, socket.flush()).await, Ok(Ok(()))) {
                            return "market data gateway pong failed";
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => return "market data gateway disconnected",
                    Some(Err(_)) => return "market data gateway WebSocket error",
                    Some(Ok(_)) => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instrument(symbol: &str) -> Instrument {
        use apexes_core::{AssetClass, MarketType};
        use rust_decimal_macros::dec;
        Instrument::new(
            symbol,
            MarketType::Crypto,
            Venue::Binance,
            AssetClass::Spot,
            Some(symbol.trim_end_matches("USDT").to_string()),
            Some("USDT".to_string()),
            dec!(0.01),
            dec!(0.0001),
            dec!(1),
        )
        .unwrap()
    }

    fn trade(symbol: &str) -> MarketEvent {
        use rust_decimal_macros::dec;
        MarketEvent::Trade(apexes_core::TradeEvent {
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            price: dec!(1),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: chrono::Utc::now(),
        })
    }

    #[test]
    fn a_session_only_sees_the_symbols_it_asked_for() {
        let filter = SymbolFilter::for_instruments(&[instrument("ETHUSDT")]);
        assert!(filter.allows(&trade("ETHUSDT")));
        assert!(
            !filter.allows(&trade("BTCUSDT")),
            "another session's symbol on a shared gateway must be dropped"
        );
    }

    #[test]
    fn several_symbols_are_all_allowed_and_no_instruments_means_no_filter() {
        let two = SymbolFilter::for_instruments(&[instrument("ETHUSDT"), instrument("BTCUSDT")]);
        assert!(two.allows(&trade("ETHUSDT")) && two.allows(&trade("BTCUSDT")));
        assert!(!two.allows(&trade("SOLUSDT")));
        let none = SymbolFilter::for_instruments(&[]);
        assert!(none.allows(&trade("ANYTHING")));
    }

    #[test]
    fn backoff_state_doubles_each_call_and_caps_at_max_backoff() {
        let mut backoff = BackoffState::new();
        assert_eq!(backoff.next_delay(), Duration::from_millis(500));
        assert_eq!(backoff.next_delay(), Duration::from_millis(1000));
        assert_eq!(backoff.next_delay(), Duration::from_millis(2000));
        assert_eq!(backoff.next_delay(), Duration::from_millis(4000));
        assert_eq!(backoff.next_delay(), Duration::from_millis(8000));
        assert_eq!(backoff.next_delay(), Duration::from_millis(16000));
        assert_eq!(backoff.next_delay(), Duration::from_secs(30));
        assert_eq!(backoff.next_delay(), Duration::from_secs(30));
    }

    #[test]
    fn backoff_state_reset_returns_to_the_initial_delay() {
        let mut backoff = BackoffState::new();
        let _ = backoff.next_delay();
        let _ = backoff.next_delay();
        backoff.reset();
        assert_eq!(backoff.next_delay(), Duration::from_millis(500));
    }

    #[test]
    fn matching_network_passes_the_check() {
        let check = NetworkCheck::evaluate(BinanceNetwork::Testnet, Some("testnet"));
        assert_eq!(check, NetworkCheck::Matches);
        assert!(check.describe().is_none());
    }

    #[test]
    fn a_different_network_is_refused_and_both_are_named() {
        let reason = NetworkCheck::evaluate(BinanceNetwork::Testnet, Some("mainnet"))
            .describe()
            .unwrap();
        assert!(
            reason.contains("mainnet") && reason.contains("testnet"),
            "{reason}"
        );
    }

    #[test]
    fn a_gateway_that_announces_nothing_is_refused() {
        let reason = NetworkCheck::evaluate(BinanceNetwork::Mainnet, None)
            .describe()
            .unwrap();
        assert!(reason.contains("did not announce"), "{reason}");
    }

    #[test]
    fn the_header_name_matches_the_gateways() {
        assert_eq!(GATEWAY_NETWORK_HEADER, "x-apexes-network");
    }
    #[test]
    fn live_guard_rejects_old_and_implausibly_future_exchange_events() {
        let guard = LiveGuard {
            kill_switch: Arc::new(KillSwitch::new()),
            max_event_age: Duration::from_secs(10),
        };
        let MarketEvent::Trade(mut event) = trade("ETHUSDT") else {
            unreachable!()
        };
        assert!(guard.fresh(&MarketEvent::Trade(event.clone())));
        event.timestamp -= chrono::Duration::seconds(20);
        assert!(!guard.fresh(&MarketEvent::Trade(event.clone())));
        event.timestamp = chrono::Utc::now() + chrono::Duration::seconds(60);
        assert!(!guard.fresh(&MarketEvent::Trade(event)));
    }

    async fn socket_pair() -> (
        GatewaySocket,
        tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let url = format!("ws://{address}");
        let (client, server) = tokio::join!(connect(&url, None), async {
            let (stream, _) = listener.accept().await.unwrap();
            tokio_tungstenite::accept_async(stream).await.unwrap()
        });
        (client.unwrap(), server)
    }

    #[tokio::test]
    async fn malformed_frame_halts_live_session_before_reconnecting() {
        let (client, mut server) = socket_pair().await;
        let (tx, _rx) = mpsc::channel(4);
        let kill_switch = Arc::new(KillSwitch::new());
        let guard = LiveGuard {
            kill_switch: kill_switch.clone(),
            max_event_age: Duration::from_secs(30),
        };
        let task = tokio::spawn(run_connection_loop(
            "ws://127.0.0.1:1".into(),
            None,
            None,
            Some(client),
            SymbolFilter::for_instruments(&[]),
            Some(guard),
            tx,
        ));
        server
            .send(Message::Text("invalid JSON".into()))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !kill_switch.is_engaged() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(kill_switch.reason().unwrap().contains("malformed"));
        task.abort();
    }

    #[tokio::test]
    async fn full_event_queue_is_a_gap_not_a_silent_drop() {
        let (mut client, mut server) = socket_pair().await;
        let (tx, _rx) = mpsc::channel(1);
        let payload = serde_json::to_string(&trade("ETHUSDT")).unwrap();
        server
            .send(Message::Text(payload.clone().into()))
            .await
            .unwrap();
        server.send(Message::Text(payload.into())).await.unwrap();
        let reason = tokio::time::timeout(
            Duration::from_secs(2),
            receive_connection(
                &mut client,
                &SymbolFilter::for_instruments(&[]),
                None,
                &tx,
                &mut BackoffState::new(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(reason, "market data gateway consumer queue overflow");
    }

    #[tokio::test]
    async fn silent_socket_times_out_even_without_a_tcp_close() {
        let (mut client, _server) = socket_pair().await;
        tokio::time::pause();
        let (tx, _rx) = mpsc::channel(4);
        let filter = SymbolFilter::for_instruments(&[]);
        let mut backoff = BackoffState::new();
        let receive = receive_connection(&mut client, &filter, None, &tx, &mut backoff);
        tokio::pin!(receive);
        // Poll once to establish the deadline, then explicitly advance
        // virtual time: the real loopback socket can keep Tokio's I/O
        // driver from automatically advancing paused time.
        assert!(futures::poll!(&mut receive).is_pending());
        tokio::time::advance(HEARTBEAT_TIMEOUT + Duration::from_secs(1)).await;
        assert_eq!(receive.await, "market data gateway heartbeat timed out");
    }
}
