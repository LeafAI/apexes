# Sub-project #10 (remainder): Market Data Ingestion — DEX/On-Chain, A-Share, H-Share, US-Equity

**Date:** 2026-08-02
**Sub-project:** #10 (Additional market gateways), rows 1.1.2–1.4.2
**Status:** Draft for review

## 1. Scope

This spec covers the ten still-unstarted `docs/FEATURES.md` rows owned by sub-project #10 under
"1. Market Data & Data Ingestion":

| Row | Feature | Market |
|---|---|---|
| 1.1.2 | DEX on-chain data (The Graph, DEX pool event parsing) | Crypto |
| 1.1.3 | Public chain RPC access & node management | Crypto |
| 1.1.4 | Mempool monitoring & block event subscription | Crypto |
| 1.2.1 | A-share free data sources (AKShare, Tushare, Sina/Eastmoney) | A-share |
| 1.2.2 | A-share paid Level-2 tick data (Hundsun UF2.0, XTP, Huaxin Qidian) | A-share |
| 1.2.3 | Call auction, order-by-order commit/fill parsing | A-share |
| 1.3.1 | H-share free data (Futu OpenAPI, Longbridge OpenAPI) | H-share |
| 1.3.2 | H-share paid HKEX direct connect (OMD-C) | H-share |
| 1.4.1 | US equity free/low-cost data (IEX Cloud, Alpaca, Polygon free tier) | US-equity |
| 1.4.2 | US equity paid full-depth data (Polygon.io paid, Databento, Nasdaq TotalView) | US-equity |

This spec does **not** cover any broker/execution gateway work (already complete, see
`2026-08-02-apexes-gateway-ashare-hshare-usequity-design.md`) or the market data normalization
model itself (`MarketEvent`, already `Done` per row 1.5).

## 2. Constraint carried over from the gateway sub-project

No real brokerage account, paid data subscription, or API key exists for any credentialed data
source across any of these four markets. This spec follows the same principle established and
user-approved for the broker-gateway work: **build real, working code wherever a source is
genuinely usable without payment or account signup; degrade every other source to a
protocol/schema-reference-only placeholder** that documents the real wire format but is never
exercised against a live paid/credentialed endpoint.

## 3. Real-world research findings (verified live during this design phase)

All of the following were tested directly (not assumed) before scoping this spec:

- **`publicnode.com`'s free Ethereum RPC endpoint** (`https://ethereum-rpc.publicnode.com` /
  `wss://ethereum-rpc.publicnode.com`) requires no API key and genuinely works:
  `eth_blockNumber`, `eth_getBlockByNumber`, `eth_getLogs` (verified against a real Uniswap V3
  pool's `Swap` event topic, returning real recent swap logs), and WebSocket `eth_subscribe`
  for both `newHeads` (new block headers) and `newPendingTransactions` (mempool tx hashes) all
  returned genuine live data during testing.
- **The Graph's free hosted service was deprecated in 2023**; its replacement (The Graph
  Network) is a paid, GRT-token-metered decentralized query market with no free tier suitable
  for this project. DEX pool *querying via The Graph* is therefore out of reach without a real
  account — but since a DEX pool's `Swap` event is public on-chain data, we can independently
  reconstruct the same information (price, quantity) by decoding `Swap` event logs pulled
  directly via `eth_getLogs`, with zero dependency on The Graph. This was independently
  verified as described above.
- **Eastmoney's public quote/list REST API** (`push2.eastmoney.com`) requires no API key and
  genuinely works: both individual quote lookups (`/api/qt/stock/get`) and market-wide listings
  (`/api/qt/clist/get`) returned real, current A-share market data during testing.
- **AKShare/Tushare** are Python-only wrapper libraries around endpoints similar to Eastmoney's;
  they are not directly reusable from Rust and are treated as prior-art references in code
  comments only, not as a dependency or an implementation target.
- **IEX Cloud is confirmed shut down** (the service is unreachable; this matches its publicly
  known August 2024 discontinuation). It is marked **not viable** and dropped entirely from
  this project's real-implementation scope — no placeholder needed since there's no live
  protocol to document.
- **Alpaca and Polygon.io** both require account signup / API key even for their free tiers;
  confirmed by a live `401 Unauthorized` response when probed without credentials. Both are
  scoped as protocol-reference placeholders.
- **Futu and Longbridge** (H-share free tiers) were already fully researched during the
  broker-gateway sub-project: Futu stopped opening new mainland China accounts (2022-12-31) and
  pulled its app (2023-05-19); Longbridge/LongPort has no sandbox (real-money-only API). Both
  are placeholders.
- **HKEX OMD-C**, Hundsun UF2.0, XTP Level-2, and Huaxin Qidian are all closed-source/paid
  institutional data feeds with no public trial access — all placeholders, matching the existing
  XTP gateway's placeholder pattern.

## 4. Architecture

Four new crates, one per market, each independently buildable and independently testable — no
crate depends on another's market-specific code (matching the existing gateway crates'
independence). Where a market has a genuinely working free source, that crate contains **real**
networking code (HTTP polling and/or WebSocket streaming) validated with `wiremock` (HTTP) or a
hand-written mock WebSocket/TCP server (streaming), following this project's established
testing conventions. Where a market has no viable free source, the crate (or the relevant
module within it) is placeholder-only: real request/schema type definitions, but every method
that would require a live response returns an explicit error — never a panic, never
`unimplemented!()`, never a fabricated success — exactly the same safety invariant established
for the XTP/IBKR gateways.

### 4.1 `apexes-market-data-chain` (rows 1.1.2, 1.1.3, 1.1.4) — real implementation

- **`ChainDataProvider` trait** (new, in `apexes-core`, alongside the existing
  `MarketDataProvider`): a deliberately separate trait from `MarketDataProvider`, since block
  headers and pending-transaction hashes are chain-level data, not market data in the
  Tick/Depth/Bar/Trade/MarkPrice sense. Two methods: `subscribe_blocks(&self) ->
  Result<BoxStream<'static, ChainBlockHeader>, ApexesError>` and `subscribe_pending_transactions(&self)
  -> Result<BoxStream<'static, PendingTransactionHash>, ApexesError>`.
  - `ChainBlockHeader { block_number: u64, block_hash: String, parent_hash: String, timestamp: DateTime<Utc> }`
    -- a minimal projection of `eth_subscribe("newHeads")`'s real fields, not a full raw-JSON passthrough.
  - `PendingTransactionHash(String)` -- a newtype wrapping the `0x`-prefixed hex transaction
    hash exactly as delivered by `eth_subscribe("newPendingTransactions")`. Fetching the full
    transaction body (`eth_getTransactionByHash`) is a separate, explicit follow-up call left to
    the caller -- `ChainDataProvider` deliberately does not auto-fetch full tx bodies for every
    pending-tx notification, since that would multiply RPC calls unboundedly under mempool load
    on a shared public node.
- **`EthRpcProvider`**: real implementation of `ChainDataProvider` against a configurable
  JSON-RPC endpoint (defaults documented for `publicnode.com`, but never hardcoded — the host is
  a config field, matching `apexes-gateway-binance`'s no-`std::env`-reads convention). Uses
  `tokio-tungstenite` for the `eth_subscribe` WebSocket calls (same dependency already used by
  `apexes-gateway-binance`).
- **`DexMarketDataProvider`**: real implementation of `apexes_core::MarketDataProvider`. Takes a
  list of `DexPool { pool_address: String, base_decimals: u8, quote_decimals: u8 }` (a new type,
  not an overload of `Instrument::symbol`, since a pool needs its ERC-20 token decimals to
  convert the Swap event's raw integer amounts into a real price -- cramming that into a bare
  `Instrument` would be a lossy, implicit encoding). `subscribe()` still takes `&[Instrument]`
  to satisfy the `MarketDataProvider` trait signature, resolving each `Instrument.symbol` to a
  `DexPool` via a caller-supplied `HashMap<String, DexPool>` passed to the provider's
  constructor. Calls `eth_getLogs` filtered by the pool's address + the Uniswap V2/V3 `Swap`
  event topic, decodes the ABI-encoded log fields (adjusted by each token's decimals) into
  price/quantity, and emits `MarketEvent::Trade`. Polling-based (fixed interval `eth_getLogs`
  over the new block range since the last poll -- the design deliberately avoids a live
  log-subscription filter, since `eth_newFilter`/`eth_getFilterChanges` state is not guaranteed
  to survive on a shared public node and must be treated as ephemeral).
- Real integration tests use `wiremock` for the HTTP JSON-RPC calls and a hand-written mock
  WebSocket server for the subscription calls (same pattern as `apexes-gateway-ibkr`'s
  hand-written mock TCP server, adapted to WebSocket framing) — never hitting the real
  `publicnode.com` endpoint in CI.

### 4.2 `apexes-market-data-ashare` (rows 1.2.1, 1.2.2, 1.2.3)

- **`EastmoneyMarketDataProvider`**: real implementation of `apexes_core::MarketDataProvider`
  against `push2.eastmoney.com`'s public REST API — polling-based (no free WebSocket push
  available), producing `MarketEvent::Tick` and `MarketEvent::Bar`. Real HTTP request-building
  and response-parsing, tested with `wiremock`.
- **Level-2 tick data / call-auction / order-by-order parsing (rows 1.2.2, 1.2.3)**:
  placeholder-only, in a separate module (`level2.rs`) — real request/response type definitions
  documenting Hundsun UF2.0's and XTP's public wire formats (reusing/cross-referencing
  `apexes-gateway-xtp`'s existing `XtpMarket`/`XtpSide` types where the shape genuinely
  overlaps), but no SDK linkage, matching the XTP gateway's placeholder pattern. Every method
  returns an explicit `Err`.

### 4.3 `apexes-market-data-hshare` (rows 1.3.1, 1.3.2) — placeholder-only

No real free or paid source is reachable without an account. Type definitions only (documenting
Futu OpenAPI's and HKEX OMD-C's public wire/schema shapes), every method returns explicit `Err`.

### 4.4 `apexes-market-data-usequity` (rows 1.4.1, 1.4.2) — placeholder-only, with one exclusion

IEX Cloud is dropped entirely (confirmed shut down, no protocol worth documenting). Alpaca and
Polygon.io (both free-tier and paid-tier) are placeholder-only: real request-shape type
definitions matching their documented REST schemas, every method returns explicit `Err`.

## 5. Error handling

`ChainDataProvider`'s methods return `Result<_, ApexesError>`, reusing the existing
`ApexesError::ConnectionFailed`/`ApexesError::SubscriptionFailed` variants (no new error variant
needed — both are already generic enough to cover "couldn't reach the chain RPC endpoint" and
"couldn't establish a block/mempool subscription").

## 6. Testing strategy

- `apexes-market-data-chain`: `wiremock` for HTTP JSON-RPC; a hand-written mock WebSocket server
  (new, since no existing crate in this workspace has one) for `eth_subscribe` streaming tests.
- `apexes-market-data-ashare`: `wiremock` for the real Eastmoney HTTP polling path; ordinary unit
  tests for the Level-2 placeholder's request-mapping logic.
- `apexes-market-data-hshare` / `apexes-market-data-usequity`: unit tests only (type
  construction, error-path assertions) — no network layer exists to integration-test.

## 7. Documentation honesty

Per this project's established principle, `docs/FEATURES.md` rows for markets with only
placeholder coverage (1.2.2, 1.2.3, 1.3.1, 1.3.2, 1.4.1, 1.4.2) will be marked `🚧 In Progress`,
never `Done`. Rows with genuine working implementations (1.1.2, 1.1.3, 1.1.4, 1.2.1) may be
marked `Done` if their own scope is fully met by a real, tested implementation — this will be
assessed row-by-row during the final whole-branch review, not assumed upfront.
