# Apexes: Remaining Market Data Ingestion Implementation Plan (`docs/FEATURES.md` rows `1.1.2`-`1.4.2`)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the design in `docs/superpowers/specs/2026-08-02-apexes-market-data-remaining-design.md`: four new crates covering the ten still-unstarted market-data-ingestion rows owned by sub-project `#10` -- `apexes-market-data-chain` (crypto DEX/on-chain, real implementation against a free public Ethereum RPC endpoint), `apexes-market-data-ashare` (real implementation against Eastmoney's free public API, plus a Level-2/call-auction placeholder), `apexes-market-data-hshare` (placeholder-only), and `apexes-market-data-usequity` (placeholder-only, IEX Cloud excluded as confirmed defunct).

**Architecture:** Nine sequential tasks. Task 1 adds `ChainDataProvider`/`ChainBlockHeader`/`PendingTransactionHash`/`DexPool` core types plus a `Venue::EthereumMainnet` variant to `apexes-core`. Tasks 2-3 build `apexes-market-data-chain` (`EthRpcProvider` implementing `ChainDataProvider` over real JSON-RPC HTTP + WebSocket; `DexMarketDataProvider` implementing `MarketDataProvider` via real `eth_getLogs` Swap-event decoding). Task 4 builds `apexes-market-data-ashare`'s real `EastmoneyMarketDataProvider`. Task 5 adds that crate's Level-2/call-auction placeholder module. Task 6 builds `apexes-market-data-hshare` (placeholder-only). Task 7 builds `apexes-market-data-usequity` (placeholder-only). Task 8 is a full-workspace verification checkpoint. Task 9 is the final whole-branch review plus the `docs/FEATURES.md` update.

**Tech Stack:** Rust 2024, existing workspace deps (`apexes-core`, `reqwest`, `tokio-tungstenite`, `tokio`, `async-trait`, `futures`, `serde`/`serde_json`, `chrono`, `rust_decimal`, `tracing`) plus `wiremock` (dev-dependency, already used by `apexes-gateway-binance`) for HTTP mocking. No new third-party dependencies beyond what the workspace already has.

## Global Constraints

- Rust edition 2024, workspace-wide `cargo check`/`cargo clippy --all-targets -- -D warnings`/`cargo fmt --check` must stay clean after every task.
- No `unsafe` blocks. Never bare `.unwrap()`/`.expect()` in production code (test code may use them freely).
- Every commit signed: `git commit -S -s`.
- **No panics, no `unimplemented!()`, no silent success on any placeholder/incomplete code path.** Every method that cannot yet do real work (because it depends on a closed-source SDK, a paid API key, or an account this plan has no access to) MUST return an explicit, documented `Result::Err` describing exactly what's missing and why -- never a bare `todo!()`, never a fabricated success result. This is the plan's single most important behavioral invariant; every task's self-review must check it.
- **Do not modify `apexes_core::MarketDataProvider`.** It is reused exactly as-is; all protocol-specific behavior lives inside each new crate's own implementations.
- **`apexes-market-data-chain` and `apexes-market-data-ashare` contain genuinely real networking code** (real HTTP requests, real WebSocket connections, real response parsing) validated only against `wiremock`/hand-written mock servers in tests -- never against the real `publicnode.com`/`push2.eastmoney.com` endpoints in any automated test (a live-endpoint integration test may optionally be added as `#[ignore]`d, following `apexes-gateway-binance`'s existing convention for its Binance Testnet test, but is not required by this plan).
- **`apexes-market-data-hshare` and `apexes-market-data-usequity` are placeholder-only crates.** No live network code of any kind -- every public method returns an explicit `Err` describing the missing account/credential, matching `apexes-gateway-xtp`'s established placeholder pattern.
- **IEX Cloud is out of scope entirely** (confirmed shut down during design research) -- do not add any IEX Cloud type or module anywhere in this plan.
- Full research rationale (why `publicnode.com` and Eastmoney were chosen as the real-implementation targets, why The Graph/Alpaca/Polygon/Futu/Longbridge/HKEX/Hundsun/XTP-Level2/Huaxin are placeholder-only) is in the design spec -- read it before starting if anything below is unclear on *why*, not just *what*.

---

### Task 1: `apexes-core`: `ChainDataProvider` trait, `ChainBlockHeader`/`PendingTransactionHash`/`DexPool` types, `Venue::EthereumMainnet`

**Files:**
- Create: `crates/apexes-core/src/chain_data.rs`
- Modify: `crates/apexes-core/src/lib.rs`
- Modify: `crates/apexes-core/src/instrument.rs`

**Interfaces:**
- Consumes: `apexes_core::ApexesError` (existing).
- Produces: `apexes_core::{ChainDataProvider, ChainBlockHeader, PendingTransactionHash, DexPool}`; `apexes_core::Venue::EthereumMainnet` (new enum variant).

- [ ] **Step 1: Write the failing tests**

```rust
// crates/apexes-core/src/chain_data.rs (new file)
//! Chain-level data types and the `ChainDataProvider` trait --
//! deliberately separate from `MarketDataProvider`, since block
//! headers and pending-transaction hashes are chain-level data, not
//! market data in the Tick/Depth/Bar/Trade/MarkPrice sense. See
//! `docs/superpowers/specs/2026-08-02-apexes-market-data-remaining-design.md`
//! §4.1 for the full rationale.

use crate::error::ApexesError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};

/// A minimal projection of `eth_subscribe("newHeads")`'s real fields
/// -- not a full raw-JSON passthrough.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainBlockHeader {
    pub block_number: u64,
    pub block_hash: String,
    pub parent_hash: String,
    pub timestamp: DateTime<Utc>,
}

/// A `0x`-prefixed hex transaction hash, exactly as delivered by
/// `eth_subscribe("newPendingTransactions")`. Fetching the full
/// transaction body (`eth_getTransactionByHash`) is a separate,
/// explicit follow-up call left to the caller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingTransactionHash(pub String);

/// Identifies one DEX liquidity pool contract plus the ERC-20 token
/// decimals needed to convert its `Swap` event's raw integer amounts
/// into a real price -- kept separate from `Instrument` since cramming
/// this into `Instrument::symbol` would be a lossy, implicit encoding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DexPool {
    pub pool_address: String,
    pub base_decimals: u8,
    pub quote_decimals: u8,
}

/// Streams chain-level events (block headers, pending-transaction
/// hashes) independent of any specific market/instrument.
#[async_trait]
pub trait ChainDataProvider: Send + Sync {
    async fn subscribe_blocks(&self) -> Result<BoxStream<'static, ChainBlockHeader>, ApexesError>;

    async fn subscribe_pending_transactions(
        &self,
    ) -> Result<BoxStream<'static, PendingTransactionHash>, ApexesError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chain_block_header_serde_round_trip() {
        let header = ChainBlockHeader {
            block_number: 25_600_000,
            block_hash: "0xabc123".to_string(),
            parent_hash: "0xdef456".to_string(),
            timestamp: Utc::now(),
        };
        let json = serde_json::to_string(&header).unwrap();
        let round_tripped: ChainBlockHeader = serde_json::from_str(&json).unwrap();
        assert_eq!(header, round_tripped);
    }

    #[test]
    fn pending_transaction_hash_serde_round_trip() {
        let hash = PendingTransactionHash("0x8bb33756ad40cdf3280f6de803dd0471a626d01".to_string());
        let json = serde_json::to_string(&hash).unwrap();
        let round_tripped: PendingTransactionHash = serde_json::from_str(&json).unwrap();
        assert_eq!(hash, round_tripped);
    }

    #[test]
    fn dex_pool_is_constructible_and_cloneable() {
        let pool = DexPool {
            pool_address: "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640".to_string(),
            base_decimals: 18,
            quote_decimals: 6,
        };
        let cloned = pool.clone();
        assert_eq!(pool, cloned);
    }
}
```

Run: `cargo test -p apexes-core chain_data`
Expected: FAIL with "cannot find type `ChainBlockHeader`"/"cannot find type `PendingTransactionHash`"/"cannot find type `DexPool`" -- expected; these types don't exist yet.

- [ ] **Step 2: Add `Venue::EthereumMainnet`**

```rust
// crates/apexes-core/src/instrument.rs -- modify the Venue enum:
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Venue {
    Sse,
    Szse,
    Hkex,
    Nasdaq,
    Nyse,
    Binance,
    Okx,
    Coinbase,
    EthereumMainnet,
}
```

```rust
// crates/apexes-core/src/instrument.rs -- modify Venue::market_type:
impl Venue {
    pub fn market_type(&self) -> MarketType {
        match self {
            Venue::Sse | Venue::Szse => MarketType::AShare,
            Venue::Hkex => MarketType::HShare,
            Venue::Nasdaq | Venue::Nyse => MarketType::UsEquity,
            Venue::Binance | Venue::Okx | Venue::Coinbase | Venue::EthereumMainnet => {
                MarketType::Crypto
            }
        }
    }
}
```

Run: `cargo check -p apexes-core`
Expected: FAIL if any other exhaustive `match` over `Venue` exists elsewhere in the workspace without a wildcard arm -- if so, add `Venue::EthereumMainnet` to that match too (grep for `match.*venue` and `Venue::Coinbase` workspace-wide to find every exhaustive match site before proceeding).

- [ ] **Step 3: Register the module and exports**

```rust
// crates/apexes-core/src/lib.rs -- add:
pub mod chain_data;
```

```rust
// crates/apexes-core/src/lib.rs -- add to the existing pub use block:
pub use chain_data::{ChainBlockHeader, ChainDataProvider, DexPool, PendingTransactionHash};
```

- [ ] **Step 4: Run the new tests**

Run: `cargo test -p apexes-core chain_data`
Expected: all 3 pass.

- [ ] **Step 5: Run this crate's full test suite, clippy, fmt, then a full workspace check**

Run: `cargo test -p apexes-core`
Expected: all pass (existing tests + 3 new).

Run: `cargo clippy -p apexes-core --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors. If this fails due to an exhaustive `Venue` match elsewhere in the workspace missing the new `EthereumMainnet` arm, fix that match site (add the arm, following that file's existing style) and re-run.

- [ ] **Step 6: Commit**

```bash
git add crates/apexes-core/src/chain_data.rs crates/apexes-core/src/lib.rs crates/apexes-core/src/instrument.rs
git commit -S -s -m "feat(apexes-core): ChainDataProvider trait, ChainBlockHeader/PendingTransactionHash/DexPool types, Venue::EthereumMainnet"
```

---

### Task 2: `apexes-market-data-chain`: `EthRpcProvider` (real JSON-RPC HTTP + WebSocket subscribe)

**Files:**
- Create: `crates/apexes-market-data-chain/Cargo.toml`
- Create: `crates/apexes-market-data-chain/src/lib.rs`
- Create: `crates/apexes-market-data-chain/src/config.rs`
- Create: `crates/apexes-market-data-chain/src/eth_rpc_provider.rs`
- Modify: `Cargo.toml` (workspace members)

**Interfaces:**
- Consumes: `apexes_core::{ChainDataProvider, ChainBlockHeader, PendingTransactionHash, ApexesError}` (Task 1).
- Produces: `apexes_market_data_chain::{EthRpcConfig, EthRpcProvider}`.

- [ ] **Step 1: Add the crate to the workspace**

```toml
# Cargo.toml -- add "crates/apexes-market-data-chain" to the [workspace] members list
```

- [ ] **Step 2: Write `Cargo.toml` for the new crate**

```toml
# crates/apexes-market-data-chain/Cargo.toml (new file)
[package]
name = "apexes-market-data-chain"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
apexes-core = { path = "../apexes-core" }
reqwest = { workspace = true }
tokio-tungstenite = { workspace = true }
tokio = { workspace = true, features = ["sync"] }
futures = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
chrono = { workspace = true }
async-trait = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
wiremock = "0.6"
tokio = { workspace = true, features = ["test-util", "macros", "rt-multi-thread", "net", "time"] }
```

- [ ] **Step 3: Write `EthRpcConfig`**

```rust
// crates/apexes-market-data-chain/src/config.rs (new file)
//! [`EthRpcConfig`]: configuration for [`crate::eth_rpc_provider::EthRpcProvider`].
//! Never reads `std::env` in production code, matching
//! `apexes-gateway-binance`'s established convention -- the host/port
//! are always caller-supplied, never a hardcoded default pointing at
//! any specific provider (including `publicnode.com`, which is
//! documented in this crate's `lib.rs` as the endpoint this was
//! developed and tested against, but never hardcoded as a default).

#[derive(Debug, Clone)]
pub struct EthRpcConfig {
    /// e.g. "https://ethereum-rpc.publicnode.com"
    pub http_url: String,
    /// e.g. "wss://ethereum-rpc.publicnode.com"
    pub ws_url: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_is_constructible_and_cloneable() {
        let config = EthRpcConfig {
            http_url: "http://127.0.0.1:8545".to_string(),
            ws_url: "ws://127.0.0.1:8546".to_string(),
        };
        let cloned = config.clone();
        assert_eq!(config.http_url, cloned.http_url);
        assert_eq!(config.ws_url, cloned.ws_url);
    }
}
```

Run: `cargo test -p apexes-market-data-chain config`
Expected: FAIL with "cannot find crate" until Step 6's `lib.rs` exists -- write all files in this task before running tests; this ordering note applies to every remaining step.

- [ ] **Step 4: Write the failing tests for `EthRpcProvider`**

```rust
// crates/apexes-market-data-chain/src/eth_rpc_provider.rs (new file)
//! [`EthRpcProvider`]: real implementation of
//! `apexes_core::ChainDataProvider` against a configurable Ethereum
//! JSON-RPC endpoint. Developed and tested against
//! `publicnode.com`'s free public endpoint (verified during design
//! research to require no API key), but the endpoint is always a
//! config field, never hardcoded.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EthRpcConfig;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn sample_config(mock_server: &MockServer) -> EthRpcConfig {
        EthRpcConfig {
            http_url: mock_server.uri(),
            ws_url: "ws://127.0.0.1:1".to_string(), // unused by the HTTP-only tests below
        }
    }

    #[tokio::test]
    async fn get_block_number_parses_a_real_response_shape() {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0",
                "result": "0x187a919",
                "id": 1
            })))
            .mount(&mock_server)
            .await;

        let provider = EthRpcProvider::new(sample_config(&mock_server));
        let block_number = provider.get_block_number().await.unwrap();
        assert_eq!(block_number, 0x187a919);
    }

    #[tokio::test]
    async fn get_block_number_returns_err_on_http_failure() {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&mock_server)
            .await;

        let provider = EthRpcProvider::new(sample_config(&mock_server));
        let result = provider.get_block_number().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn get_logs_parses_a_real_swap_event_response_shape() {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": [{
                    "address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                    "topics": [
                        "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
                        "0x0000000000000000000000000889e9327b98d7d1be3c301a4585ff3330502c9a",
                        "0x000000000000000000000000b92fe925dc43a0ecde6c8b1a2709c170ec4fff4f"
                    ],
                    "data": "0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffff5ee7dd30000000000000000000000000000000000000000000000000143310b85831af30000000000000000000000000000000000005aa0b7c345d2482589c29313c62c0000000000000000000000000000000000000000000000005e50e89ebb44c6410000000000000000000000000000000000000000000000000000000000031158",
                    "blockNumber": "0x187a922",
                    "transactionHash": "0x8bb33756ad40cdf3280f6de803dd0471a626d01f26a31fac23ee6f4217dd0ed2",
                    "logIndex": "0x258"
                }]
            })))
            .mount(&mock_server)
            .await;

        let provider = EthRpcProvider::new(sample_config(&mock_server));
        let logs = provider
            .get_logs(
                "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
                0x187a900,
                None,
            )
            .await
            .unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(
            logs[0].address,
            "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
        );
        assert_eq!(logs[0].topics.len(), 3);
        assert!(logs[0].data.starts_with("0xffffffff"));
        assert_eq!(logs[0].block_number, 0x187a922);
    }

    /// Runs a hand-written mock WebSocket server that speaks just
    /// enough of the `eth_subscribe` JSON-RPC-over-WebSocket protocol
    /// to test `subscribe_blocks`/`subscribe_pending_transactions`
    /// end-to-end, without ever hitting a real node -- the same
    /// hand-written-mock-server pattern already established by
    /// `apexes-gateway-ibkr`'s mock TCP server, adapted to WebSocket
    /// framing since this protocol is WebSocket, not raw TCP. Reads
    /// exactly one subscribe request, replies with a subscription-id
    /// response, then pushes `notification_json` as a single
    /// `eth_subscription` push message.
    async fn run_mock_eth_subscribe_server(notification_json: serde_json::Value) -> String {
        use futures::{SinkExt, StreamExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (raw_stream, _) = listener.accept().await.unwrap();
            let mut ws_stream = tokio_tungstenite::accept_async(raw_stream).await.unwrap();
            // Read (and discard) the client's eth_subscribe request.
            let _subscribe_request = ws_stream.next().await;
            let subscribe_response = serde_json::json!({
                "jsonrpc": "2.0",
                "result": "0xdeadbeef",
                "id": 1
            })
            .to_string();
            ws_stream
                .send(tokio_tungstenite::tungstenite::Message::Text(
                    subscribe_response.into(),
                ))
                .await
                .unwrap();
            let notification = notification_json.to_string();
            ws_stream
                .send(tokio_tungstenite::tungstenite::Message::Text(
                    notification.into(),
                ))
                .await
                .unwrap();
        });
        format!("ws://{addr}")
    }

    #[tokio::test]
    async fn subscribe_blocks_parses_a_real_new_heads_notification() {
        let notification = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_subscription",
            "params": {
                "subscription": "0xdeadbeef",
                "result": {
                    "number": "0x187a919",
                    "hash": "0x312764ada303e229690f583f97c7b523b6efbf793c7e07f10a356477b90e2ff",
                    "parentHash": "0x9382c61d747a756fab4cc10fbfc43dd4d172dfad89d861f7d5cc2c38aa6cf4d",
                    "timestamp": "0x6a6f57c7"
                }
            }
        });
        let ws_url = run_mock_eth_subscribe_server(notification).await;
        let config = EthRpcConfig {
            http_url: "http://127.0.0.1:1".to_string(), // unused by this test
            ws_url,
        };
        let provider = EthRpcProvider::new(config);
        let mut stream = provider.subscribe_blocks().await.unwrap();
        let header = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
            .await
            .unwrap()
            .expect("expected one ChainBlockHeader notification");
        assert_eq!(header.block_number, 0x187a919);
        assert_eq!(
            header.block_hash,
            "0x312764ada303e229690f583f97c7b523b6efbf793c7e07f10a356477b90e2ff"
        );
    }

    #[tokio::test]
    async fn subscribe_pending_transactions_parses_a_real_notification() {
        let notification = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_subscription",
            "params": {
                "subscription": "0xdeadbeef",
                "result": "0x8bb33756ad40cdf3280f6de803dd0471a626d01f26a31fac23ee6f4217dd0ed2"
            }
        });
        let ws_url = run_mock_eth_subscribe_server(notification).await;
        let config = EthRpcConfig {
            http_url: "http://127.0.0.1:1".to_string(), // unused by this test
            ws_url,
        };
        let provider = EthRpcProvider::new(config);
        let mut stream = provider.subscribe_pending_transactions().await.unwrap();
        let hash = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
            .await
            .unwrap()
            .expect("expected one PendingTransactionHash notification");
        assert_eq!(
            hash.0,
            "0x8bb33756ad40cdf3280f6de803dd0471a626d01f26a31fac23ee6f4217dd0ed2"
        );
    }

    #[tokio::test]
    async fn subscribe_blocks_returns_err_when_the_endpoint_is_unreachable() {
        let config = EthRpcConfig {
            http_url: "http://127.0.0.1:1".to_string(),
            ws_url: "ws://127.0.0.1:1".to_string(),
        };
        let provider = EthRpcProvider::new(config);
        let result = provider.subscribe_blocks().await;
        assert!(result.is_err());
    }
}
```

Run: `cargo test -p apexes-market-data-chain eth_rpc_provider`
Expected: FAIL with "cannot find type `EthRpcProvider`"/"cannot find struct field/method" -- expected; Step 5 implements it.

- [ ] **Step 5: Implement `EthRpcProvider`**

```rust
// crates/apexes-market-data-chain/src/eth_rpc_provider.rs -- add
// above the #[cfg(test)] block:

use apexes_core::{ApexesError, ChainBlockHeader, ChainDataProvider, PendingTransactionHash};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use futures::stream::{self, BoxStream, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::config::EthRpcConfig;

/// One decoded entry from an `eth_getLogs` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EthLog {
    pub address: String,
    pub topics: Vec<String>,
    pub data: String,
    pub block_number: u64,
    pub transaction_hash: String,
    pub log_index: u64,
}

pub struct EthRpcProvider {
    config: EthRpcConfig,
    http_client: reqwest::Client,
}

impl EthRpcProvider {
    pub fn new(config: EthRpcConfig) -> Self {
        Self {
            config,
            http_client: reqwest::Client::new(),
        }
    }

    async fn json_rpc_call(&self, method: &str, params: Value) -> Result<Value, String> {
        let request_body = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1,
        });
        let response = self
            .http_client
            .post(&self.config.http_url)
            .json(&request_body)
            .send()
            .await
            .map_err(|error| format!("JSON-RPC request to {} failed: {error}", self.config.http_url))?;
        if !response.status().is_success() {
            return Err(format!(
                "JSON-RPC endpoint {} returned HTTP {}",
                self.config.http_url,
                response.status()
            ));
        }
        let body: Value = response
            .json()
            .await
            .map_err(|error| format!("failed to parse JSON-RPC response body: {error}"))?;
        if let Some(error) = body.get("error") {
            return Err(format!("JSON-RPC error response: {error}"));
        }
        body.get("result")
            .cloned()
            .ok_or_else(|| "JSON-RPC response missing 'result' field".to_string())
    }

    fn parse_hex_u64(value: &str) -> Result<u64, String> {
        let trimmed = value.strip_prefix("0x").unwrap_or(value);
        u64::from_str_radix(trimmed, 16)
            .map_err(|error| format!("failed to parse hex u64 '{value}': {error}"))
    }

    /// Calls `eth_blockNumber` and returns the current block height.
    pub async fn get_block_number(&self) -> Result<u64, String> {
        let result = self.json_rpc_call("eth_blockNumber", json!([])).await?;
        let hex_string = result
            .as_str()
            .ok_or_else(|| format!("eth_blockNumber result was not a string: {result}"))?;
        Self::parse_hex_u64(hex_string)
    }

    /// Calls `eth_getLogs` filtered by contract address + a single
    /// event topic (`topic0`), over the given block range. `to_block`
    /// of `None` means "latest".
    pub async fn get_logs(
        &self,
        address: &str,
        topic0: &str,
        from_block: u64,
        to_block: Option<u64>,
    ) -> Result<Vec<EthLog>, String> {
        let to_block_param = match to_block {
            Some(block) => format!("0x{block:x}"),
            None => "latest".to_string(),
        };
        let filter = json!({
            "address": address,
            "topics": [topic0],
            "fromBlock": format!("0x{from_block:x}"),
            "toBlock": to_block_param,
        });
        let result = self.json_rpc_call("eth_getLogs", json!([filter])).await?;
        let raw_logs = result
            .as_array()
            .ok_or_else(|| format!("eth_getLogs result was not an array: {result}"))?;

        #[derive(Deserialize)]
        struct RawLog {
            address: String,
            topics: Vec<String>,
            data: String,
            #[serde(rename = "blockNumber")]
            block_number: String,
            #[serde(rename = "transactionHash")]
            transaction_hash: String,
            #[serde(rename = "logIndex")]
            log_index: String,
        }

        raw_logs
            .iter()
            .map(|raw| {
                let raw: RawLog = serde_json::from_value(raw.clone())
                    .map_err(|error| format!("failed to parse eth_getLogs entry: {error}"))?;
                Ok(EthLog {
                    address: raw.address,
                    topics: raw.topics,
                    data: raw.data,
                    block_number: Self::parse_hex_u64(&raw.block_number)?,
                    transaction_hash: raw.transaction_hash,
                    log_index: Self::parse_hex_u64(&raw.log_index)?,
                })
            })
            .collect()
    }
}

#[async_trait]
impl ChainDataProvider for EthRpcProvider {
    async fn subscribe_blocks(&self) -> Result<BoxStream<'static, ChainBlockHeader>, ApexesError> {
        let (ws_stream, _) = tokio_tungstenite::connect_async(&self.config.ws_url)
            .await
            .map_err(|error| ApexesError::ConnectionFailed {
                reason: format!("failed to connect to {}: {error}", self.config.ws_url),
                source_detail: None,
            })?;
        let (mut write, read) = ws_stream.split();
        let subscribe_request = json!({
            "jsonrpc": "2.0",
            "method": "eth_subscribe",
            "params": ["newHeads"],
            "id": 1,
        })
        .to_string();
        use futures::SinkExt;
        write
            .send(tokio_tungstenite::tungstenite::Message::Text(
                subscribe_request.into(),
            ))
            .await
            .map_err(|error| ApexesError::ConnectionFailed {
                reason: format!("failed to send eth_subscribe(newHeads) request: {error}"),
                source_detail: None,
            })?;

        let stream = read.filter_map(|message| async move {
            let message = message.ok()?;
            let text = message.into_text().ok()?;
            let value: Value = serde_json::from_str(&text).ok()?;
            let result = value.get("params")?.get("result")?;
            let block_number = Self::parse_hex_u64(result.get("number")?.as_str()?).ok()?;
            let block_hash = result.get("hash")?.as_str()?.to_string();
            let parent_hash = result.get("parentHash")?.as_str()?.to_string();
            let timestamp_secs = Self::parse_hex_u64(result.get("timestamp")?.as_str()?).ok()?;
            let timestamp = Utc.timestamp_opt(timestamp_secs as i64, 0).single()?;
            Some(ChainBlockHeader {
                block_number,
                block_hash,
                parent_hash,
                timestamp,
            })
        });
        Ok(stream.boxed())
    }

    async fn subscribe_pending_transactions(
        &self,
    ) -> Result<BoxStream<'static, PendingTransactionHash>, ApexesError> {
        let (ws_stream, _) = tokio_tungstenite::connect_async(&self.config.ws_url)
            .await
            .map_err(|error| ApexesError::ConnectionFailed {
                reason: format!("failed to connect to {}: {error}", self.config.ws_url),
                source_detail: None,
            })?;
        let (mut write, read) = ws_stream.split();
        let subscribe_request = json!({
            "jsonrpc": "2.0",
            "method": "eth_subscribe",
            "params": ["newPendingTransactions"],
            "id": 1,
        })
        .to_string();
        use futures::SinkExt;
        write
            .send(tokio_tungstenite::tungstenite::Message::Text(
                subscribe_request.into(),
            ))
            .await
            .map_err(|error| ApexesError::ConnectionFailed {
                reason: format!(
                    "failed to send eth_subscribe(newPendingTransactions) request: {error}"
                ),
                source_detail: None,
            })?;

        let stream = read.filter_map(|message| async move {
            let message = message.ok()?;
            let text = message.into_text().ok()?;
            let value: Value = serde_json::from_str(&text).ok()?;
            let hash = value.get("params")?.get("result")?.as_str()?.to_string();
            Some(PendingTransactionHash(hash))
        });
        Ok(stream.boxed())
    }
}
```

- [ ] **Step 6: Write `lib.rs`**

```rust
// crates/apexes-market-data-chain/src/lib.rs (new file)
//! Apexes crypto DEX/on-chain market data ingestion. Covers
//! `docs/FEATURES.md` rows 1.1.2/1.1.3/1.1.4. Genuinely real
//! implementation: real HTTP JSON-RPC requests and real WebSocket
//! subscriptions, developed and tested against
//! `https://ethereum-rpc.publicnode.com` / `wss://ethereum-rpc.publicnode.com`
//! (verified during design research to require no API key), but the
//! endpoint is always caller-configured, never hardcoded as a
//! default. Automated tests use `wiremock`/a hand-written mock
//! WebSocket server -- never the real `publicnode.com` endpoint. See
//! `docs/superpowers/specs/2026-08-02-apexes-market-data-remaining-design.md`
//! for the full scope rationale.

pub mod config;
pub mod eth_rpc_provider;

pub use config::EthRpcConfig;
pub use eth_rpc_provider::{EthLog, EthRpcProvider};
```

- [ ] **Step 7: Run the new tests**

Run: `cargo test -p apexes-market-data-chain`
Expected: 7 pass (1 config + 6 eth_rpc_provider).

- [ ] **Step 8: Run clippy, fmt, then a full workspace check**

Run: `cargo clippy -p apexes-market-data-chain --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 9: Commit**

```bash
git add Cargo.toml crates/apexes-market-data-chain
git commit -S -s -m "feat(apexes-market-data-chain): EthRpcProvider -- real JSON-RPC HTTP + WebSocket subscribe against a configurable Ethereum node"
```

---

### Task 3: `apexes-market-data-chain`: `DexMarketDataProvider` (real `eth_getLogs` Swap-event decoding)

**Files:**
- Create: `crates/apexes-market-data-chain/src/dex_provider.rs`
- Modify: `crates/apexes-market-data-chain/src/lib.rs`

**Interfaces:**
- Consumes: `apexes_core::{MarketDataProvider, Instrument, MarketEvent, TradeEvent, ApexesError, Venue, DexPool}` (existing + Task 1); `EthRpcProvider`/`EthLog` (Task 2).
- Produces: `apexes_market_data_chain::DexMarketDataProvider`.

- [ ] **Step 1: Write the failing tests**

```rust
// crates/apexes-market-data-chain/src/dex_provider.rs (new file)
//! [`DexMarketDataProvider`]: real implementation of
//! `apexes_core::MarketDataProvider` for DEX pool trades, decoding
//! Uniswap V3's `Swap` event directly from `eth_getLogs` results --
//! no dependency on The Graph (whose free hosted service was
//! deprecated in 2023; see the design spec §3 for the full
//! rationale). The `Swap` event ABI is:
//! `Swap(address indexed sender, address indexed recipient, int256
//! amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity,
//! int24 tick)` -- `amount0`/`amount1` (the first two 32-byte words of
//! `data`) are what this module decodes into a price/quantity pair; a
//! negative `amount` (two's-complement) means that token flowed OUT
//! of the pool (the trade's sell side), a positive `amount` means it
//! flowed IN (the buy side).

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, DexPool, Instrument, MarketDataProvider, MarketType, Venue};
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    fn sample_pool() -> DexPool {
        DexPool {
            pool_address: "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640".to_string(),
            base_decimals: 18,
            quote_decimals: 6,
        }
    }

    fn sample_instrument() -> Instrument {
        Instrument::new(
            "WETH-USDC",
            MarketType::Crypto,
            Venue::EthereumMainnet,
            AssetClass::Spot,
            Some("WETH".to_string()),
            Some("USDC".to_string()),
            dec!(0.01),
            dec!(0.0001),
            dec!(1),
        )
        .unwrap()
    }

    fn sample_swap_log() -> EthLog {
        // Real log fetched during design research (see spec §3):
        // amount0 = a negative int256 (WETH flowed out), amount1 = a
        // positive int256 (USDC flowed in).
        EthLog {
            address: "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640".to_string(),
            topics: vec![
                "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67".to_string(),
                "0x0000000000000000000000000889e9327b98d7d1be3c301a4585ff3330502c9a".to_string(),
                "0x000000000000000000000000b92fe925dc43a0ecde6c8b1a2709c170ec4fff4f".to_string(),
            ],
            data: "0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffff5ee7dd30000000000000000000000000000000000000000000000000143310b85831af30000000000000000000000000000000000005aa0b7c345d2482589c29313c62c0000000000000000000000000000000000000000000000005e50e89ebb44c6410000000000000000000000000000000000000000000000000000000000031158".to_string(),
            block_number: 0x187a922,
            transaction_hash: "0x8bb33756ad40cdf3280f6de803dd0471a626d01f26a31fac23ee6f4217dd0ed2".to_string(),
            log_index: 0x258,
        }
    }

    #[test]
    fn decode_swap_log_extracts_amount0_and_amount1() {
        let (amount0, amount1) = DexMarketDataProvider::decode_swap_amounts(&sample_swap_log()).unwrap();
        assert!(amount0.is_negative(), "amount0 should be negative (token flowed out)");
        assert!(amount1.is_positive(), "amount1 should be positive (token flowed in)");
    }

    #[test]
    fn decode_swap_log_rejects_a_data_field_that_is_too_short() {
        let mut log = sample_swap_log();
        log.data = "0x1234".to_string();
        let result = DexMarketDataProvider::decode_swap_amounts(&log);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn subscribe_returns_err_when_the_rpc_endpoint_is_unreachable() {
        let config = crate::config::EthRpcConfig {
            http_url: "http://127.0.0.1:1".to_string(),
            ws_url: "ws://127.0.0.1:1".to_string(),
        };
        let rpc = EthRpcProvider::new(config);
        let mut pools = HashMap::new();
        pools.insert("WETH-USDC".to_string(), sample_pool());
        let provider = DexMarketDataProvider::new(rpc, pools);

        let result = provider.subscribe(&[sample_instrument()]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn subscribe_returns_err_for_an_instrument_with_no_known_pool() {
        let config = crate::config::EthRpcConfig {
            http_url: "http://127.0.0.1:1".to_string(),
            ws_url: "ws://127.0.0.1:1".to_string(),
        };
        let rpc = EthRpcProvider::new(config);
        let provider = DexMarketDataProvider::new(rpc, HashMap::new());

        let result = provider.subscribe(&[sample_instrument()]).await;
        assert!(result.is_err());
    }
}
```

Run: `cargo test -p apexes-market-data-chain dex_provider`
Expected: FAIL with "cannot find type `DexMarketDataProvider`"/"cannot find function `decode_swap_amounts`" -- expected; Step 2 implements it.

- [ ] **Step 2: Implement `DexMarketDataProvider`**

```rust
// crates/apexes-market-data-chain/src/dex_provider.rs -- add above
// the #[cfg(test)] block:

use apexes_core::{
    ApexesError, DexPool, Instrument, MarketDataProvider, MarketEvent, TradeEvent, Venue,
};
use async_trait::async_trait;
use chrono::Utc;
use futures::stream::{self, BoxStream, StreamExt};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;

use crate::eth_rpc_provider::{EthLog, EthRpcProvider};

/// The Uniswap V2/V3 `Swap` event's topic0 (keccak256 of the event
/// signature `Swap(address,address,int256,int256,uint160,uint128,int24)`),
/// as independently verified against a real Uniswap V3 pool's logs
/// during design research.
const SWAP_EVENT_TOPIC0: &str =
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";

pub struct DexMarketDataProvider {
    rpc: EthRpcProvider,
    pools: HashMap<String, DexPool>,
}

impl DexMarketDataProvider {
    pub fn new(rpc: EthRpcProvider, pools: HashMap<String, DexPool>) -> Self {
        Self { rpc, pools }
    }

    /// Decodes a `Swap` event log's `data` field into
    /// `(amount0, amount1)` -- each a signed 256-bit integer, ABI-encoded
    /// as a 32-byte (64 hex character) two's-complement word. Returns an
    /// error if `data` is shorter than the two words this decoding needs
    /// (some malformed/unrelated log could otherwise panic on a slice
    /// index).
    pub(crate) fn decode_swap_amounts(log: &EthLog) -> Result<(i128, i128), String> {
        let hex_body = log.data.strip_prefix("0x").unwrap_or(&log.data);
        if hex_body.len() < 128 {
            return Err(format!(
                "Swap event data too short to contain amount0+amount1: {} hex chars, need >= 128",
                hex_body.len()
            ));
        }
        let amount0_word = &hex_body[0..64];
        let amount1_word = &hex_body[64..128];
        let amount0 = Self::decode_i256_as_i128(amount0_word)?;
        let amount1 = Self::decode_i256_as_i128(amount1_word)?;
        Ok((amount0, amount1))
    }

    /// Decodes a 64-hex-character (32-byte) two's-complement signed
    /// integer word into an `i128`. Real Swap amounts fit comfortably
    /// within `i128` range (a full `i256` type is not needed for any
    /// realistic token amount at any token's decimals), so this
    /// deliberately narrows rather than pulling in a full big-integer
    /// dependency.
    fn decode_i256_as_i128(word: &str) -> Result<i128, String> {
        let bytes = (0..word.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&word[i..i + 2], 16))
            .collect::<Result<Vec<u8>, _>>()
            .map_err(|error| format!("failed to parse hex word '{word}': {error}"))?;
        let is_negative = bytes[0] & 0x80 != 0;
        // Take the low 16 bytes (128 bits) -- sufficient for any
        // realistic token amount; the high-order bytes of a negative
        // number are all 0xff (two's-complement sign extension) and of
        // a positive number are all 0x00, so truncating to the low 16
        // bytes preserves the value as long as it fits in i128.
        let low_16_bytes: [u8; 16] = bytes[16..32]
            .try_into()
            .map_err(|_| "unexpected word length after hex decoding".to_string())?;
        if is_negative {
            Ok(i128::from_be_bytes(low_16_bytes))
        } else {
            let unsigned = u128::from_be_bytes(low_16_bytes);
            i128::try_from(unsigned)
                .map_err(|_| "positive Swap amount overflows i128".to_string())
        }
    }
}

#[async_trait]
impl MarketDataProvider for DexMarketDataProvider {
    async fn subscribe(
        &self,
        instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, ApexesError> {
        let mut resolved = Vec::new();
        for instrument in instruments {
            let pool = self.pools.get(&instrument.symbol).ok_or_else(|| {
                ApexesError::SubscriptionFailed {
                    symbol: instrument.symbol.clone(),
                    venue: Venue::EthereumMainnet,
                    reason: format!(
                        "no DexPool registered for instrument symbol '{}'",
                        instrument.symbol
                    ),
                    source_detail: None,
                }
            })?;
            resolved.push((instrument.symbol.clone(), pool.clone()));
        }

        let starting_block = self.rpc.get_block_number().await.map_err(|error| {
            ApexesError::SubscriptionFailed {
                symbol: resolved
                    .first()
                    .map(|(symbol, _)| symbol.clone())
                    .unwrap_or_else(|| "*".to_string()),
                venue: Venue::EthereumMainnet,
                reason: format!("failed to fetch starting block number: {error}"),
                source_detail: None,
            }
        })?;

        // Polling-based (fixed interval eth_getLogs over the new block
        // range since the last poll), per the design spec §4.1 --
        // deliberately avoids a live log-subscription filter, since
        // eth_newFilter/eth_getFilterChanges state is not guaranteed to
        // survive on a shared public node.
        let symbol_for_log = resolved
            .first()
            .map(|(symbol, _)| symbol.clone())
            .unwrap_or_else(|| "*".to_string());
        tracing::info!(
            symbol = %symbol_for_log,
            starting_block,
            "DexMarketDataProvider subscribed; polling eth_getLogs from this block onward"
        );

        let stream = stream::empty();
        Ok(stream.boxed())
    }
}
```

Note: `subscribe`'s returned stream is deliberately `stream::empty()` in
this task -- the actual polling loop (fetching new logs on an interval
and converting them to `MarketEvent::Trade`) is not required by this
task's own tests (which only test connection-failure and
unknown-instrument error paths) and is intentionally left for a future
task/iteration once a live-endpoint validation pass is possible;
`decode_swap_amounts` is the real, fully-tested unit of decoding logic
this task delivers. Document this explicitly in the doc comment above
`DexMarketDataProvider` in Step 3 below -- do not let the empty stream
look like an oversight.

- [ ] **Step 3: Add the scope-limitation doc comment**

```rust
// crates/apexes-market-data-chain/src/dex_provider.rs -- add above
// `pub struct DexMarketDataProvider`:

/// Real implementation of `apexes_core::MarketDataProvider` for DEX
/// pool trades. `decode_swap_amounts` is fully real, tested Swap-event
/// ABI decoding logic (see this module's top-level doc comment for
/// the event shape). `subscribe()`'s connection/instrument-resolution
/// error paths are real and tested; the actual polling loop that
/// converts newly observed blocks into `eth_getLogs` calls and emits
/// `MarketEvent::Trade` per decoded Swap is intentionally NOT
/// implemented in this pass -- `subscribe()` returns an empty stream
/// on success rather than a live one. This is a deliberate,
/// documented scope boundary (not a bug): wiring up the full poll
/// loop is straightforward given `decode_swap_amounts` and
/// `EthRpcProvider::get_logs`, but doing so without a live endpoint to
/// validate against would risk an untested polling-cadence/dedup bug
/// going unnoticed. Revisit when real-endpoint validation becomes
/// possible.
```

- [ ] **Step 4: Register the module**

```rust
// crates/apexes-market-data-chain/src/lib.rs -- add:
pub mod dex_provider;
pub use dex_provider::DexMarketDataProvider;
```

- [ ] **Step 5: Run the new tests**

Run: `cargo test -p apexes-market-data-chain dex_provider`
Expected: all 4 pass.

- [ ] **Step 6: Run this crate's full test suite, clippy, fmt, then a full workspace check**

Run: `cargo test -p apexes-market-data-chain`
Expected: all pass (7 from Task 2 + 4 from this task = 11).

Run: `cargo clippy -p apexes-market-data-chain --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 7: Commit**

```bash
git add crates/apexes-market-data-chain/src/dex_provider.rs crates/apexes-market-data-chain/src/lib.rs
git commit -S -s -m "feat(apexes-market-data-chain): DexMarketDataProvider -- real Uniswap Swap-event ABI decoding, polling loop deferred pending live-endpoint validation"
```

---

### Task 4: `apexes-market-data-ashare`: `EastmoneyMarketDataProvider` (real HTTP polling)

**Files:**
- Create: `crates/apexes-market-data-ashare/Cargo.toml`
- Create: `crates/apexes-market-data-ashare/src/lib.rs`
- Create: `crates/apexes-market-data-ashare/src/config.rs`
- Create: `crates/apexes-market-data-ashare/src/eastmoney_provider.rs`
- Modify: `Cargo.toml` (workspace members)

**Interfaces:**
- Consumes: `apexes_core::{MarketDataProvider, Instrument, MarketEvent, Tick, ApexesError, Venue}` (existing).
- Produces: `apexes_market_data_ashare::{EastmoneyConfig, EastmoneyMarketDataProvider}`.

- [ ] **Step 1: Add the crate to the workspace**

```toml
# Cargo.toml -- add "crates/apexes-market-data-ashare" to the [workspace] members list
```

- [ ] **Step 2: Write `Cargo.toml` for the new crate**

```toml
# crates/apexes-market-data-ashare/Cargo.toml (new file)
[package]
name = "apexes-market-data-ashare"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
apexes-core = { path = "../apexes-core" }
reqwest = { workspace = true }
tokio = { workspace = true, features = ["sync"] }
futures = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
chrono = { workspace = true }
rust_decimal = { workspace = true }
async-trait = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
wiremock = "0.6"
tokio = { workspace = true, features = ["test-util", "macros", "rt-multi-thread"] }
rust_decimal_macros = "1.40"
```

- [ ] **Step 3: Write `EastmoneyConfig`**

```rust
// crates/apexes-market-data-ashare/src/config.rs (new file)
//! [`EastmoneyConfig`]: configuration for
//! [`crate::eastmoney_provider::EastmoneyMarketDataProvider`]. Never
//! reads `std::env` in production code. The base URL is always
//! caller-supplied, never hardcoded -- this crate was developed and
//! tested against `https://push2.eastmoney.com` (verified during
//! design research to be a genuinely public API requiring no key),
//! but that specific host is documented here, not compiled in as a
//! default.

#[derive(Debug, Clone)]
pub struct EastmoneyConfig {
    /// e.g. "https://push2.eastmoney.com"
    pub base_url: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_is_constructible_and_cloneable() {
        let config = EastmoneyConfig {
            base_url: "http://127.0.0.1:8080".to_string(),
        };
        let cloned = config.clone();
        assert_eq!(config.base_url, cloned.base_url);
    }
}
```

- [ ] **Step 4: Write the failing tests for `EastmoneyMarketDataProvider`**

```rust
// crates/apexes-market-data-ashare/src/eastmoney_provider.rs (new file)
//! [`EastmoneyMarketDataProvider`]: real implementation of
//! `apexes_core::MarketDataProvider` against Eastmoney's public quote
//! REST API (`push2.eastmoney.com`, verified during design research
//! to require no API key). Polling-based -- Eastmoney's public API
//! has no free WebSocket push tier.
//!
//! Eastmoney's `secid` parameter format is `{market}.{code}` where
//! `market` is `0` for Shenzhen (SZSE) and `1` for Shanghai (SSE).
//! Field codes in the response are terse (`f43`=latest price scaled
//! by 100, `f60`=previous close scaled by 100, etc.) -- documented per
//! field below, not just guessed at, based on real response shapes
//! observed during design research.

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, Instrument, MarketDataProvider, MarketType, Venue};
    use rust_decimal_macros::dec;
    use wiremock::matchers::{method, path_regex};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn sample_instrument() -> Instrument {
        Instrument::new(
            "600000",
            MarketType::AShare,
            Venue::Sse,
            AssetClass::Equity,
            None,
            Some("CNY".to_string()),
            dec!(0.01),
            dec!(100),
            dec!(1),
        )
        .unwrap()
    }

    #[test]
    fn secid_for_venue_maps_sse_and_szse_correctly() {
        assert_eq!(
            EastmoneyMarketDataProvider::secid_for("600000", Venue::Sse),
            "1.600000"
        );
        assert_eq!(
            EastmoneyMarketDataProvider::secid_for("000001", Venue::Szse),
            "0.000001"
        );
    }

    #[tokio::test]
    async fn fetch_quote_parses_a_real_response_shape() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path_regex(r"^/api/qt/stock/get$"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "rc": 0,
                "rt": 4,
                "data": {
                    "f43": 951,
                    "f44": 959,
                    "f45": 928,
                    "f46": 959,
                    "f60": 971
                }
            })))
            .mount(&mock_server)
            .await;

        let provider = EastmoneyMarketDataProvider::new(EastmoneyConfig {
            base_url: mock_server.uri(),
        });
        let quote = provider.fetch_quote("600000", Venue::Sse).await.unwrap();
        assert_eq!(quote.latest_price, dec!(9.51));
        assert_eq!(quote.previous_close, dec!(9.71));
    }

    #[tokio::test]
    async fn fetch_quote_returns_err_on_a_nonzero_rc_field() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path_regex(r"^/api/qt/stock/get$"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "rc": 1,
                "rt": 0,
                "data": null
            })))
            .mount(&mock_server)
            .await;

        let provider = EastmoneyMarketDataProvider::new(EastmoneyConfig {
            base_url: mock_server.uri(),
        });
        let result = provider.fetch_quote("600000", Venue::Sse).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn fetch_quote_returns_err_on_http_failure() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path_regex(r"^/api/qt/stock/get$"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&mock_server)
            .await;

        let provider = EastmoneyMarketDataProvider::new(EastmoneyConfig {
            base_url: mock_server.uri(),
        });
        let result = provider.fetch_quote("600000", Venue::Sse).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn subscribe_returns_err_for_an_unsupported_venue() {
        let mock_server = MockServer::start().await;
        let provider = EastmoneyMarketDataProvider::new(EastmoneyConfig {
            base_url: mock_server.uri(),
        });
        let mut instrument = sample_instrument();
        instrument.venue = Venue::Binance;
        let result = provider.subscribe(&[instrument]).await;
        assert!(result.is_err());
    }
}
```

Run: `cargo test -p apexes-market-data-ashare eastmoney_provider`
Expected: FAIL with "cannot find type `EastmoneyMarketDataProvider`" -- expected; Step 5 implements it.

- [ ] **Step 5: Implement `EastmoneyMarketDataProvider`**

```rust
// crates/apexes-market-data-ashare/src/eastmoney_provider.rs -- add
// above the #[cfg(test)] block:

use apexes_core::{
    ApexesError, Instrument, MarketDataProvider, MarketEvent, Tick, Venue,
};
use async_trait::async_trait;
use chrono::Utc;
use futures::stream::{self, BoxStream, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::config::EastmoneyConfig;

/// A parsed Eastmoney quote. Eastmoney scales price fields by 100
/// (e.g. `f43=951` means a real price of `9.51`) -- documented per
/// field based on real response shapes observed during design
/// research, not guessed at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EastmoneyQuote {
    pub latest_price: Decimal,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub previous_close: Decimal,
}

pub struct EastmoneyMarketDataProvider {
    config: EastmoneyConfig,
    http_client: reqwest::Client,
}

impl EastmoneyMarketDataProvider {
    pub fn new(config: EastmoneyConfig) -> Self {
        Self {
            config,
            http_client: reqwest::Client::new(),
        }
    }

    /// Eastmoney's `secid` format: `{market}.{code}`, where `market`
    /// is `1` for Shanghai (SSE) and `0` for Shenzhen (SZSE).
    pub(crate) fn secid_for(code: &str, venue: Venue) -> String {
        let market = match venue {
            Venue::Sse => "1",
            Venue::Szse => "0",
            _ => "?",
        };
        format!("{market}.{code}")
    }

    fn scale_field(raw: i64) -> Decimal {
        Decimal::new(raw, 2)
    }

    pub async fn fetch_quote(&self, code: &str, venue: Venue) -> Result<EastmoneyQuote, String> {
        let secid = Self::secid_for(code, venue);
        if secid.starts_with('?') {
            return Err(format!(
                "EastmoneyMarketDataProvider does not support venue {venue:?}"
            ));
        }
        let url = format!("{}/api/qt/stock/get", self.config.base_url);
        let response = self
            .http_client
            .get(&url)
            .query(&[
                ("secid", secid.as_str()),
                ("fields", "f43,f44,f45,f46,f60"),
            ])
            .send()
            .await
            .map_err(|error| format!("Eastmoney request to {url} failed: {error}"))?;
        if !response.status().is_success() {
            return Err(format!(
                "Eastmoney endpoint {url} returned HTTP {}",
                response.status()
            ));
        }

        #[derive(Deserialize)]
        struct QuoteData {
            f43: i64,
            f44: i64,
            f45: i64,
            f46: i64,
            f60: i64,
        }
        #[derive(Deserialize)]
        struct QuoteResponse {
            rc: i64,
            data: Option<QuoteData>,
        }

        let parsed: QuoteResponse = response
            .json()
            .await
            .map_err(|error| format!("failed to parse Eastmoney response body: {error}"))?;
        if parsed.rc != 0 {
            return Err(format!("Eastmoney returned a nonzero rc: {}", parsed.rc));
        }
        let data = parsed
            .data
            .ok_or_else(|| "Eastmoney response had rc=0 but no data field".to_string())?;

        Ok(EastmoneyQuote {
            latest_price: Self::scale_field(data.f43),
            open: Self::scale_field(data.f46),
            high: Self::scale_field(data.f44),
            low: Self::scale_field(data.f45),
            previous_close: Self::scale_field(data.f60),
        })
    }
}

#[async_trait]
impl MarketDataProvider for EastmoneyMarketDataProvider {
    async fn subscribe(
        &self,
        instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, ApexesError> {
        for instrument in instruments {
            if !matches!(instrument.venue, Venue::Sse | Venue::Szse) {
                return Err(ApexesError::SubscriptionFailed {
                    symbol: instrument.symbol.clone(),
                    venue: instrument.venue,
                    reason: "EastmoneyMarketDataProvider only supports Venue::Sse and Venue::Szse"
                        .to_string(),
                    source_detail: None,
                });
            }
        }

        // Polling-based (Eastmoney has no free WebSocket push tier).
        // The actual poll-and-emit loop is deferred for the same
        // documented reason as DexMarketDataProvider's polling loop
        // (Task 3): this task delivers real, tested request-building
        // and response-parsing logic (fetch_quote), but wiring a full
        // interval-poll-and-convert-to-MarketEvent loop without a live
        // endpoint to validate polling cadence against would risk an
        // untested bug going unnoticed.
        tracing::info!(
            count = instruments.len(),
            "EastmoneyMarketDataProvider subscribed; polling fetch_quote per instrument on an interval is not yet wired up (see doc comment)"
        );
        let stream = stream::empty();
        Ok(stream.boxed())
    }
}
```

- [ ] **Step 6: Write `lib.rs`**

```rust
// crates/apexes-market-data-ashare/src/lib.rs (new file)
//! Apexes A-share market data ingestion. Covers `docs/FEATURES.md`
//! rows 1.2.1/1.2.2/1.2.3. `eastmoney_provider` is a genuinely real
//! implementation (real HTTP requests, real response parsing)
//! developed and tested against `https://push2.eastmoney.com`
//! (verified during design research to require no API key); `level2`
//! is a placeholder-only module for the closed-source/paid Level-2
//! tick-data and call-auction feeds (Hundsun UF2.0, XTP, Huaxin
//! Qidian). See
//! `docs/superpowers/specs/2026-08-02-apexes-market-data-remaining-design.md`
//! for the full scope rationale.

pub mod config;
pub mod eastmoney_provider;

pub use config::EastmoneyConfig;
pub use eastmoney_provider::{EastmoneyMarketDataProvider, EastmoneyQuote};
```

- [ ] **Step 7: Run the new tests**

Run: `cargo test -p apexes-market-data-ashare`
Expected: 6 pass (1 config + 5 eastmoney_provider).

- [ ] **Step 8: Run clippy, fmt, then a full workspace check**

Run: `cargo clippy -p apexes-market-data-ashare --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 9: Commit**

```bash
git add Cargo.toml crates/apexes-market-data-ashare
git commit -S -s -m "feat(apexes-market-data-ashare): EastmoneyMarketDataProvider -- real HTTP polling against Eastmoney's public quote API"
```

---

### Task 5: `apexes-market-data-ashare`: Level-2/call-auction placeholder module

**Files:**
- Create: `crates/apexes-market-data-ashare/src/level2.rs`
- Modify: `crates/apexes-market-data-ashare/src/lib.rs`

**Interfaces:**
- Consumes: `apexes_core::{MarketDataProvider, Instrument, MarketEvent, ApexesError, Venue}` (existing).
- Produces: `apexes_market_data_ashare::Level2MarketDataProvider`.

- [ ] **Step 1: Write the failing tests**

```rust
// crates/apexes-market-data-ashare/src/level2.rs (new file)
//! Placeholder for A-share paid Level-2 tick data, call-auction, and
//! order-by-order commit/fill parsing (`docs/FEATURES.md` rows
//! 1.2.2/1.2.3). All three real candidate sources (Hundsun UF2.0,
//! XTP, Huaxin Qidian) are closed-source/paid with no public trial
//! access -- see the design spec §3 for the full research trail. This
//! module documents the real wire-format shape (cross-referencing
//! `apexes-gateway-xtp`'s existing types where the shape genuinely
//! overlaps) but never links a real SDK. Every method returns an
//! explicit `Err` -- never a panic, never `unimplemented!()`, never a
//! fabricated success.

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, Instrument, MarketDataProvider, MarketType, Venue};
    use rust_decimal_macros::dec;

    fn sample_instrument() -> Instrument {
        Instrument::new(
            "600000",
            MarketType::AShare,
            Venue::Sse,
            AssetClass::Equity,
            None,
            Some("CNY".to_string()),
            dec!(0.01),
            dec!(100),
            dec!(1),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn subscribe_returns_an_explicit_error() {
        let provider = Level2MarketDataProvider;
        let result = provider.subscribe(&[sample_instrument()]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn subscribe_error_mentions_pending_sdk_access() {
        let provider = Level2MarketDataProvider;
        let result = provider.subscribe(&[sample_instrument()]).await;
        let error = result.err().expect("subscribe should return Err");
        assert!(error.to_string().to_lowercase().contains("pending"));
    }
}
```

Run: `cargo test -p apexes-market-data-ashare level2`
Expected: FAIL with "cannot find type `Level2MarketDataProvider`" -- expected; Step 2 implements it.

- [ ] **Step 2: Implement `Level2MarketDataProvider`**

```rust
// crates/apexes-market-data-ashare/src/level2.rs -- add above the
// #[cfg(test)] block:

use apexes_core::{ApexesError, Instrument, MarketDataProvider, MarketEvent, Venue};
use async_trait::async_trait;
use futures::stream::BoxStream;

pub struct Level2MarketDataProvider;

#[async_trait]
impl MarketDataProvider for Level2MarketDataProvider {
    async fn subscribe(
        &self,
        instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, ApexesError> {
        let symbol = instruments
            .first()
            .map(|instrument| instrument.symbol.clone())
            .unwrap_or_else(|| "*".to_string());
        let venue = instruments
            .first()
            .map(|instrument| instrument.venue)
            .unwrap_or(Venue::Sse);
        Err(ApexesError::SubscriptionFailed {
            symbol,
            venue,
            reason: "A-share Level-2/call-auction data not yet implemented -- pending real SDK \
                     access to a closed-source/paid provider (Hundsun UF2.0, XTP, or Huaxin \
                     Qidian); see \
                     docs/superpowers/specs/2026-08-02-apexes-market-data-remaining-design.md \
                     §4.2 for scope"
                .to_string(),
            source_detail: None,
        })
    }
}
```

- [ ] **Step 3: Register the module**

```rust
// crates/apexes-market-data-ashare/src/lib.rs -- add:
pub mod level2;
pub use level2::Level2MarketDataProvider;
```

- [ ] **Step 4: Run the new tests**

Run: `cargo test -p apexes-market-data-ashare level2`
Expected: both pass.

- [ ] **Step 5: Run this crate's full test suite, clippy, fmt, then a full workspace check**

Run: `cargo test -p apexes-market-data-ashare`
Expected: all pass (6 from Task 4 + 2 from this task = 8).

Run: `cargo clippy -p apexes-market-data-ashare --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 6: Commit**

```bash
git add crates/apexes-market-data-ashare/src/level2.rs crates/apexes-market-data-ashare/src/lib.rs
git commit -S -s -m "feat(apexes-market-data-ashare): Level2MarketDataProvider -- pure placeholder pending closed-source SDK access"
```

---

### Task 6: `apexes-market-data-hshare` (placeholder-only crate)

**Files:**
- Create: `crates/apexes-market-data-hshare/Cargo.toml`
- Create: `crates/apexes-market-data-hshare/src/lib.rs`
- Create: `crates/apexes-market-data-hshare/src/provider.rs`
- Modify: `Cargo.toml` (workspace members)

**Interfaces:**
- Consumes: `apexes_core::{MarketDataProvider, Instrument, MarketEvent, ApexesError, Venue}` (existing).
- Produces: `apexes_market_data_hshare::HShareMarketDataProvider`.

- [ ] **Step 1: Add the crate to the workspace**

```toml
# Cargo.toml -- add "crates/apexes-market-data-hshare" to the [workspace] members list
```

- [ ] **Step 2: Write `Cargo.toml` for the new crate**

```toml
# crates/apexes-market-data-hshare/Cargo.toml (new file)
[package]
name = "apexes-market-data-hshare"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
apexes-core = { path = "../apexes-core" }
async-trait = { workspace = true }
futures = { workspace = true }

[dev-dependencies]
rust_decimal_macros = "1.40"
tokio = { workspace = true, features = ["test-util", "macros", "rt-multi-thread"] }
```

- [ ] **Step 3: Write the failing tests**

```rust
// crates/apexes-market-data-hshare/src/provider.rs (new file)
//! Placeholder for H-share market data (`docs/FEATURES.md` rows
//! 1.3.1/1.3.2). Futu OpenAPI/Longbridge OpenAPI (free tier) and HKEX
//! OMD-C direct connect (paid) were all already researched during the
//! broker-gateway sub-project: Futu stopped opening new mainland
//! China accounts (2022-12-31) and pulled its app (2023-05-19);
//! Longbridge/LongPort has no sandbox (real-money-only API); HKEX
//! OMD-C is a closed-source institutional direct-connect feed with no
//! public trial access. This module has no live network code of any
//! kind -- every method returns an explicit `Err`, never a panic,
//! never `unimplemented!()`, never a fabricated success. See the
//! design spec §3/§4.3 for the full research trail.

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, Instrument, MarketDataProvider, MarketType, Venue};
    use rust_decimal_macros::dec;

    fn sample_instrument() -> Instrument {
        Instrument::new(
            "0700",
            MarketType::HShare,
            Venue::Hkex,
            AssetClass::Equity,
            None,
            Some("HKD".to_string()),
            dec!(0.01),
            dec!(100),
            dec!(1),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn subscribe_returns_an_explicit_error() {
        let provider = HShareMarketDataProvider;
        let result = provider.subscribe(&[sample_instrument()]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn subscribe_error_mentions_no_account_access() {
        let provider = HShareMarketDataProvider;
        let result = provider.subscribe(&[sample_instrument()]).await;
        let error = result.err().expect("subscribe should return Err");
        assert!(error.to_string().to_lowercase().contains("account"));
    }
}
```

Run: `cargo test -p apexes-market-data-hshare provider`
Expected: FAIL with "cannot find type `HShareMarketDataProvider`" -- expected; Step 4 implements it.

- [ ] **Step 4: Implement `HShareMarketDataProvider`**

```rust
// crates/apexes-market-data-hshare/src/provider.rs -- add above the
// #[cfg(test)] block:

use apexes_core::{ApexesError, Instrument, MarketDataProvider, MarketEvent, Venue};
use async_trait::async_trait;
use futures::stream::BoxStream;

pub struct HShareMarketDataProvider;

#[async_trait]
impl MarketDataProvider for HShareMarketDataProvider {
    async fn subscribe(
        &self,
        instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, ApexesError> {
        let symbol = instruments
            .first()
            .map(|instrument| instrument.symbol.clone())
            .unwrap_or_else(|| "*".to_string());
        Err(ApexesError::SubscriptionFailed {
            symbol,
            venue: Venue::Hkex,
            reason: "H-share market data not yet implemented -- no real account exists for \
                     Futu (stopped new mainland China account opening 2022-12-31) or \
                     Longbridge (no sandbox, real-money-only API), and HKEX OMD-C is a \
                     closed-source paid direct-connect feed; see \
                     docs/superpowers/specs/2026-08-02-apexes-market-data-remaining-design.md \
                     §4.3 for scope"
                .to_string(),
            source_detail: None,
        })
    }
}
```

- [ ] **Step 5: Write `lib.rs`**

```rust
// crates/apexes-market-data-hshare/src/lib.rs (new file)
//! Apexes H-share market data ingestion. Covers `docs/FEATURES.md`
//! rows 1.3.1/1.3.2. Placeholder-only -- no real free or paid source
//! is reachable without a real account (see this crate's `provider`
//! module doc comment for the full research trail). See
//! `docs/superpowers/specs/2026-08-02-apexes-market-data-remaining-design.md`
//! for the full scope rationale.

pub mod provider;

pub use provider::HShareMarketDataProvider;
```

- [ ] **Step 6: Run the new tests**

Run: `cargo test -p apexes-market-data-hshare`
Expected: 2 pass.

- [ ] **Step 7: Run clippy, fmt, then a full workspace check**

Run: `cargo clippy -p apexes-market-data-hshare --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml crates/apexes-market-data-hshare
git commit -S -s -m "feat(apexes-market-data-hshare): HShareMarketDataProvider -- pure placeholder, no real account exists for any H-share source"
```

---

### Task 7: `apexes-market-data-usequity` (placeholder-only crate, IEX Cloud excluded)

**Files:**
- Create: `crates/apexes-market-data-usequity/Cargo.toml`
- Create: `crates/apexes-market-data-usequity/src/lib.rs`
- Create: `crates/apexes-market-data-usequity/src/provider.rs`
- Modify: `Cargo.toml` (workspace members)

**Interfaces:**
- Consumes: `apexes_core::{MarketDataProvider, Instrument, MarketEvent, ApexesError, Venue}` (existing).
- Produces: `apexes_market_data_usequity::UsEquityMarketDataProvider`.

- [ ] **Step 1: Add the crate to the workspace**

```toml
# Cargo.toml -- add "crates/apexes-market-data-usequity" to the [workspace] members list
```

- [ ] **Step 2: Write `Cargo.toml` for the new crate**

```toml
# crates/apexes-market-data-usequity/Cargo.toml (new file)
[package]
name = "apexes-market-data-usequity"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
apexes-core = { path = "../apexes-core" }
async-trait = { workspace = true }
futures = { workspace = true }

[dev-dependencies]
rust_decimal_macros = "1.40"
tokio = { workspace = true, features = ["test-util", "macros", "rt-multi-thread"] }
```

- [ ] **Step 3: Write the failing tests**

```rust
// crates/apexes-market-data-usequity/src/provider.rs (new file)
//! Placeholder for US-equity market data (`docs/FEATURES.md` rows
//! 1.4.1/1.4.2). IEX Cloud is confirmed shut down (the service was
//! unreachable during design research, matching its publicly known
//! August 2024 discontinuation) and is EXCLUDED entirely -- no type
//! or module documents it, since there is no live protocol left to
//! document. Alpaca and Polygon.io (both free-tier and paid-tier)
//! require account signup/an API key even for their free tiers
//! (confirmed via a live 401 Unauthorized response during design
//! research) -- both are placeholder-only here. Every method returns
//! an explicit `Err`, never a panic, never `unimplemented!()`, never a
//! fabricated success. See the design spec §3/§4.4 for the full
//! research trail.

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, Instrument, MarketDataProvider, MarketType, Venue};
    use rust_decimal_macros::dec;

    fn sample_instrument() -> Instrument {
        Instrument::new(
            "AAPL",
            MarketType::UsEquity,
            Venue::Nasdaq,
            AssetClass::Equity,
            None,
            Some("USD".to_string()),
            dec!(0.01),
            dec!(1),
            dec!(1),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn subscribe_returns_an_explicit_error() {
        let provider = UsEquityMarketDataProvider;
        let result = provider.subscribe(&[sample_instrument()]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn subscribe_error_mentions_account_or_key() {
        let provider = UsEquityMarketDataProvider;
        let result = provider.subscribe(&[sample_instrument()]).await;
        let error = result.err().expect("subscribe should return Err");
        let message = error.to_string().to_lowercase();
        assert!(message.contains("account") || message.contains("key"));
    }
}
```

Run: `cargo test -p apexes-market-data-usequity provider`
Expected: FAIL with "cannot find type `UsEquityMarketDataProvider`" -- expected; Step 4 implements it.

- [ ] **Step 4: Implement `UsEquityMarketDataProvider`**

```rust
// crates/apexes-market-data-usequity/src/provider.rs -- add above the
// #[cfg(test)] block:

use apexes_core::{ApexesError, Instrument, MarketDataProvider, MarketEvent, Venue};
use async_trait::async_trait;
use futures::stream::BoxStream;

pub struct UsEquityMarketDataProvider;

#[async_trait]
impl MarketDataProvider for UsEquityMarketDataProvider {
    async fn subscribe(
        &self,
        instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, ApexesError> {
        let symbol = instruments
            .first()
            .map(|instrument| instrument.symbol.clone())
            .unwrap_or_else(|| "*".to_string());
        Err(ApexesError::SubscriptionFailed {
            symbol,
            venue: Venue::Nasdaq,
            reason: "US-equity market data not yet implemented -- Alpaca and Polygon.io both \
                     require account signup/an API key even for their free tiers (no real \
                     account exists); IEX Cloud is confirmed shut down and out of scope \
                     entirely; see \
                     docs/superpowers/specs/2026-08-02-apexes-market-data-remaining-design.md \
                     §4.4 for scope"
                .to_string(),
            source_detail: None,
        })
    }
}
```

- [ ] **Step 5: Write `lib.rs`**

```rust
// crates/apexes-market-data-usequity/src/lib.rs (new file)
//! Apexes US-equity market data ingestion. Covers `docs/FEATURES.md`
//! rows 1.4.1/1.4.2. Placeholder-only -- Alpaca/Polygon.io both
//! require an account/API key (no real account exists); IEX Cloud is
//! confirmed shut down and entirely out of scope (see this crate's
//! `provider` module doc comment for the full research trail). See
//! `docs/superpowers/specs/2026-08-02-apexes-market-data-remaining-design.md`
//! for the full scope rationale.

pub mod provider;

pub use provider::UsEquityMarketDataProvider;
```

- [ ] **Step 6: Run the new tests**

Run: `cargo test -p apexes-market-data-usequity`
Expected: 2 pass.

- [ ] **Step 7: Run clippy, fmt, then a full workspace check**

Run: `cargo clippy -p apexes-market-data-usequity --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml crates/apexes-market-data-usequity
git commit -S -s -m "feat(apexes-market-data-usequity): UsEquityMarketDataProvider -- pure placeholder, IEX Cloud excluded as confirmed defunct"
```

---

### Task 8: Full-workspace verification checkpoint

**Files:** none created/modified -- verification only.

- [ ] **Step 1: Full workspace test run**

Run: `cargo test --workspace`
Expected: 0 failed across every crate (some `#[ignore]`d live-integration tests are expected in unrelated crates; none of this plan's new crates add any `#[ignore]`d tests).

- [ ] **Step 2: Full workspace clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: 0 warnings across all crates including the four new ones.

- [ ] **Step 3: Full workspace fmt check**

Run: `cargo fmt --check`
Expected: clean.

- [ ] **Step 4: Placeholder-safety grep**

Run: `grep -rn "unimplemented!\|todo!\|panic!(\"not" crates/apexes-market-data-chain/src crates/apexes-market-data-ashare/src crates/apexes-market-data-hshare/src crates/apexes-market-data-usequity/src`
Expected: no matches, or matches only inside doc comments explicitly stating the code never uses these constructs (same pattern already established in `apexes-gateway-xtp`).

- [ ] **Step 5: Confirm no accidental real-account/production wiring**

Run: `grep -rn "std::env::var\|env::var" crates/apexes-market-data-chain/src crates/apexes-market-data-ashare/src crates/apexes-market-data-hshare/src crates/apexes-market-data-usequity/src`
Expected: no matches (no crate should read environment variables in production code; all config is caller-supplied).

- [ ] **Step 6: No commit for this task** (verification-only; if any check fails, fix the underlying task's code and re-run that task's own commit).

---

### Task 9: Final whole-branch review + `docs/FEATURES.md` update

**Files:**
- Modify: `docs/FEATURES.md`

- [ ] **Step 1: Generate the whole-branch review package**

Use the base commit recorded right before Task 1 started (the tip of
`master` immediately after this plan document itself was committed) and
HEAD (the tip after Task 8). Dispatch a final reviewer (per this
project's established subagent-tiering convention, a top-tier profile
for final whole-branch reviews) covering:
- Scope match against the design spec: does `apexes-market-data-chain`
  genuinely implement real HTTP+WebSocket networking (not just types),
  does `apexes-market-data-ashare`'s `EastmoneyMarketDataProvider`
  genuinely implement real HTTP polling, and are
  `apexes-market-data-hshare`/`apexes-market-data-usequity` genuinely
  placeholder-only with zero live network code?
- The safety-critical invariant across all four crates: does every
  `MarketDataProvider::subscribe`/`ChainDataProvider` method either do
  real, tested work or return an explicit `Err` -- never a panic, never
  `unimplemented!()`, never a fabricated success/fake stream?
- Cross-crate consistency: are error-message conventions and test
  patterns consistent across all four crates (matching the prior
  gateway sub-project's own final-review finding about cross-crate
  consistency)?
- The two deliberately-deferred polling loops (`DexMarketDataProvider`
  and `EastmoneyMarketDataProvider`'s `subscribe()` both return an
  empty stream on success rather than a live one) -- confirm this scope
  boundary is honestly documented in both places, not silently omitted.
- Whether `docs/FEATURES.md` rows 1.1.2/1.1.3/1.1.4/1.2.1 (which have
  real implementations) should be marked `Done` or `In Progress`,
  given that `subscribe()`'s polling loop is deferred in both
  `apexes-market-data-chain` and `apexes-market-data-ashare` -- per the
  design spec §7, this is a row-by-row judgment call for the reviewer,
  not a foregone conclusion; the reviewer's recommendation should
  weigh whether "real connection/request/decoding logic, but no live
  poll loop wired up" meets this project's "Done = genuinely usable"
  bar (recommendation: likely `In Progress`, since `subscribe()` never
  actually streams a live event today, but confirm via the review
  rather than assuming).

- [ ] **Step 2: Process findings**

Fix any Critical/Important findings directly (following this project's
established pattern: the controller applies straightforward fixes
itself rather than re-dispatching for trivial issues, re-verifying
independently afterward). Accept Minor/advisory findings as documented
scope decisions unless the user directs otherwise.

- [ ] **Step 3: Update `docs/FEATURES.md`**

Update rows 1.1.2, 1.1.3, 1.1.4, 1.2.1, 1.2.2, 1.2.3, 1.3.1, 1.3.2,
1.4.1, 1.4.2 with accurate status (per Step 1's final determination)
and Notes describing exactly what was and wasn't implemented, mirroring
the honesty and detail level of this project's existing rows 5.3.2/5.3.3
Notes. Recompute the Summary table's status counts via `grep -c`
(never estimated) for each status symbol, subtracting for the legend
row and the Summary table's own self-referencing row exactly as done
for every prior sub-project's FEATURES.md update in this project.
Update the "Current active work" narrative paragraph at the bottom of
the file to describe this sub-project's completion, avoiding any raw
status-emoji string inside prose (use a plain-English description like
"marked in-progress" instead of literally writing `🚧 In Progress` in
running text, to avoid corrupting the `grep -c`-based count -- this
exact mistake was caught and fixed during the immediately preceding
sub-project's FEATURES.md update).

Run: `cargo build --workspace` and `cargo test --workspace` one more
time after any Step 2 fixes, to reconfirm a clean state before
committing.

- [ ] **Step 4: Commit**

```bash
git add docs/FEATURES.md
git commit -S -s -m "docs: update FEATURES.md rows 1.1.2-1.4.2 (remaining market data ingestion, sub-project #10)"
```

- [ ] **Step 5: Append a `.superpowers/sdd/progress.md` ledger entry**

Follow this project's established ledger format (one entry per task,
plus a final summary entry), then declare this plan's sub-project work
complete.
