//! [`EthRpcProvider`]: real implementation of
//! `apexes_core::ChainDataProvider` against a configurable Ethereum
//! JSON-RPC endpoint. Developed and tested against
//! `publicnode.com`'s free public endpoint (verified during design
//! research to require no API key), but the endpoint is always a
//! config field, never hardcoded.

use apexes_core::{ApexesError, ChainBlockHeader, ChainDataProvider, PendingTransactionHash};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use futures::stream::{BoxStream, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};

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
            .map_err(|error| {
                format!(
                    "JSON-RPC request to {} failed: {error}",
                    self.config.http_url
                )
            })?;
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
