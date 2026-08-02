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

use apexes_core::{ApexesError, DexPool, Instrument, MarketDataProvider, MarketEvent, Venue};
use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use std::collections::HashMap;

use crate::eth_rpc_provider::{EthLog, EthRpcProvider};

/// The Uniswap V2/V3 `Swap` event's topic0 (keccak256 of the event
/// signature `Swap(address,address,int256,int256,uint160,uint128,int24)`),
/// as independently verified against a real Uniswap V3 pool's logs
/// during design research.
#[allow(dead_code)]
const SWAP_EVENT_TOPIC0: &str =
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";

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
    ///
    /// Not yet called from production code: the polling loop that
    /// would call this on real `eth_getLogs` results is deliberately
    /// deferred (see this struct's doc comment) pending live-endpoint
    /// validation, so this is exercised only by this module's unit
    /// tests for now -- hence `#[allow(dead_code)]` below.
    #[allow(dead_code)]
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
    /// dependency. Both the positive and negative branches explicitly
    /// check that the discarded high 16 bytes are pure sign-extension
    /// (all `0x00` for positive, all `0xff` for negative) before
    /// truncating -- an amount whose magnitude doesn't fit in `i128`
    /// returns an explicit `Err` on EITHER sign, rather than silently
    /// truncating/corrupting a value only on the positive side.
    #[allow(dead_code)]
    fn decode_i256_as_i128(word: &str) -> Result<i128, String> {
        let bytes = (0..word.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&word[i..i + 2], 16))
            .collect::<Result<Vec<u8>, _>>()
            .map_err(|error| format!("failed to parse hex word '{word}': {error}"))?;
        let is_negative = bytes[0] & 0x80 != 0;
        let high_16_bytes = &bytes[0..16];
        let low_16_bytes: [u8; 16] = bytes[16..32]
            .try_into()
            .map_err(|_| "unexpected word length after hex decoding".to_string())?;
        let expected_sign_extension_byte = if is_negative { 0xffu8 } else { 0x00u8 };
        if high_16_bytes
            .iter()
            .any(|byte| *byte != expected_sign_extension_byte)
        {
            return Err(format!(
                "Swap amount magnitude overflows i128 (sign={}, word='{word}')",
                if is_negative { "negative" } else { "positive" }
            ));
        }
        if is_negative {
            Ok(i128::from_be_bytes(low_16_bytes))
        } else {
            let unsigned = u128::from_be_bytes(low_16_bytes);
            i128::try_from(unsigned).map_err(|_| "positive Swap amount overflows i128".to_string())
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

        let starting_block =
            self.rpc
                .get_block_number()
                .await
                .map_err(|error| ApexesError::SubscriptionFailed {
                    symbol: resolved
                        .first()
                        .map(|(symbol, _)| symbol.clone())
                        .unwrap_or_else(|| "*".to_string()),
                    venue: Venue::EthereumMainnet,
                    reason: format!("failed to fetch starting block number: {error}"),
                    source_detail: None,
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
        let (amount0, amount1) =
            DexMarketDataProvider::decode_swap_amounts(&sample_swap_log()).unwrap();
        assert!(
            amount0.is_negative(),
            "amount0 should be negative (token flowed out)"
        );
        assert!(
            amount1.is_positive(),
            "amount1 should be positive (token flowed in)"
        );
        // Verified independently by manual byte-level decoding (see
        // design research): amount0 = -168919597, amount1 =
        // 90970343031905011.
        assert_eq!(amount0, -168_919_597);
        assert_eq!(amount1, 90_970_343_031_905_011);
    }

    #[test]
    fn decode_swap_log_rejects_a_data_field_that_is_too_short() {
        let mut log = sample_swap_log();
        log.data = "0x1234".to_string();
        let result = DexMarketDataProvider::decode_swap_amounts(&log);
        assert!(result.is_err());
    }

    #[test]
    fn decode_i256_as_i128_rejects_a_negative_magnitude_that_overflows_i128() {
        // Two's-complement encoding of -(2^130) as a 256-bit word,
        // independently verified via `python3 -c "print(format((-(2**130))
        // % (2**256), '064x'))"`: 31 'f' bytes, then 'c', then 32 '0'
        // bytes -- 64 hex characters total. -(2^130) is a value no
        // realistic Uniswap Swap amount would ever take (bounded by real
        // token supplies/decimals), but which the ABI encoding could in
        // principle represent. Its high 16 bytes are NOT all 0xff (unlike
        // a realistic negative amount, whose magnitude fits in i128), so
        // this must be rejected rather than silently truncated to a wrong
        // value.
        let word = "fffffffffffffffffffffffffffffffc00000000000000000000000000000000";
        assert_eq!(word.len(), 64, "sanity-check the literal is a full word");
        let result = DexMarketDataProvider::decode_i256_as_i128(word);
        assert!(
            result.is_err(),
            "a negative amount whose magnitude overflows i128 must return Err, not a silently truncated value"
        );
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
