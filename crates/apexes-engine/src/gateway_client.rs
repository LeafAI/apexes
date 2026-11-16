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
use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

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
        }
    }

    /// Requires the gateway to be on `network`: `subscribe` fails on a
    /// mismatch, and a gateway that later reconnects on another network ends
    /// the stream instead of feeding it wrong prices.
    pub fn with_expected_network(mut self, network: BinanceNetwork) -> Self {
        self.expected_network = Some(network);
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
        let (tx, rx) = mpsc::unbounded_channel();
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
            tx,
        ));

        // Adapts the `mpsc::UnboundedReceiver` into a `Stream` via
        // `futures::stream::unfold` rather than pulling in
        // `tokio-stream`'s `UnboundedReceiverStream` wrapper as a new
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
    tokio_tungstenite::connect_async(request)
        .await
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
    tx: mpsc::UnboundedSender<MarketEvent>,
) {
    let mut backoff = BackoffState::new();
    loop {
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
            Ok(socket) => {
                tracing::info!(%url, "apexes-engine gateway_client: connected");
                backoff.reset();
                let (_write, mut read) = socket.split();
                loop {
                    match read.next().await {
                        Some(Ok(Message::Text(text))) => {
                            match serde_json::from_str::<MarketEvent>(&text) {
                                Ok(event) if !filter.allows(&event) => {}
                                Ok(event) => {
                                    if tx.send(event).is_err() {
                                        tracing::info!(
                                            "apexes-engine gateway_client: output channel closed, stopping"
                                        );
                                        return;
                                    }
                                }
                                Err(error) => {
                                    tracing::warn!(
                                        ?error,
                                        "apexes-engine gateway_client: failed to parse MarketEvent JSON, skipping frame"
                                    );
                                }
                            }
                        }
                        Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                        Some(Ok(_)) => {}
                        Some(Err(error)) => {
                            tracing::warn!(
                                ?error,
                                "apexes-engine gateway_client: WebSocket error, reconnecting"
                            );
                            break;
                        }
                        None => {
                            tracing::warn!(
                                "apexes-engine gateway_client: WebSocket stream ended, reconnecting"
                            );
                            break;
                        }
                    }
                }
            }
            Err(error) => {
                tracing::warn!(
                    ?error,
                    "apexes-engine gateway_client: failed to connect, retrying"
                );
            }
        }
        tokio::time::sleep(backoff.next_delay()).await;
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
}
