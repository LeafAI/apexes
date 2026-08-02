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
