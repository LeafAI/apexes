# Apexes — Mark-Price Data Pipeline & General Ingestion Loop Design

> This is a **prerequisite** design for roadmap sub-project `#5`'s remaining scope, `docs/FEATURES.md` row `6.4` (margin & leverage monitoring, crypto derivatives focus). Row `6.4` itself requires a leveraged-`Position` model with real margin/liquidation math, which in turn requires a genuine mark price (not last-trade price, which is exploitable/noisy for margin purposes on real exchanges) as historical replay data in backtest and a real-time feed in paper/live trading. Neither exists anywhere in the codebase today. This spec builds ONLY the mark-price data pipeline (ingestion, storage, replay) plus the general-purpose live-ingestion loop it exposed as a pre-existing gap — the margin/leverage/liquidation math itself (row `6.4`'s actual scope) is a separate, subsequent spec that consumes what this one produces.
>
> Front-loaded scope note (found and confirmed during brainstorming, not silently discovered later): there is currently **no production ingestion path anywhere in the codebase** — `apexes_storage::StorageWriter::write_batch` is exercised only by tests; nothing subscribes to `MarketDataProvider::subscribe()` and persists the result. This gap predates and is broader than mark price specifically, but the user explicitly asked for a general-purpose ingestion loop (not a mark-price-only one) to be built as part of this same spec, since building a mark-price-only ingestion path would otherwise mean solving the exact same problem twice.

---

## 一、Scope

### Included

1. **`apexes-core`**: a new `MarketEvent::MarkPrice(MarkPriceUpdate)` variant — `{symbol, venue, mark_price, timestamp}`.
2. **`apexes-market-data`**:
   - Parameterize the existing `trade_task`/`depth_task`/`ticker_task` over exchange (`BinanceSpot` vs `BinanceFuturesUsd`, both already supported by the `barter-data` dependency already in use) so `AssetClass::Perpetual` instruments get the same Trade/Tick/Depth ingestion Spot instruments already have.
   - A new `mark_price_task.rs`: a hand-rolled raw WebSocket client (mirroring `apexes-gateway-binance::user_data_stream`'s existing pattern, since `barter-data` 0.11 has no mark-price subscription kind at all — confirmed via inspecting its `subscription::mod.rs`'s `SubKind` enum, which only has `PublicTrades`/`OrderBooksL1`/`OrderBooksL2`/`Liquidations`), subscribing to Binance's public `<symbol>@markPrice` combined stream for `AssetClass::Perpetual` instruments only (mark price is not a meaningful concept for Spot).
   - `BinanceMarketDataProvider::subscribe` extended to accept `Venue::Binance` + `AssetClass::Perpetual` instruments (spawning all four tasks: trade/depth/ticker/mark-price) in addition to its existing Spot support.
   - A new `apexes_market_data::ingestion` module: `run_ingestion_loop(provider: &dyn MarketDataProvider, writer: &apexes_storage::StorageWriter, instruments: &[Instrument], stop_signal: oneshot::Receiver<()>) -> Result<(), ApexesError>` — the project's first general-purpose live-market-data-to-ClickHouse ingestion path, usable for ANY `MarketEvent` variant this or any future `MarketDataProvider` produces, not specific to mark price. `stop_signal` follows the same graceful-shutdown convention already used by `run_live_trading`/`run_paper_trading`. New workspace dependency: `apexes-market-data` → `apexes-storage`.
3. **`apexes-storage`**: a new `MarkPriceRow` + `mark_prices` ClickHouse table DDL, `StorageWriter`/`StorageReader` extended to read/write it, following the exact existing Trade/Tick/Depth/Bar pattern (range-based `read_batch`, no new reader shape).
4. **`apexes-backtest`**: `StorageMarketDataSource` (already generic over whatever `StorageReader::read_batch` returns) requires no structural change — verifying `MarketEvent::MarkPrice` replays through it correctly is a test-only addition.

### Excluded (explicitly out of scope, deferred)

- **The margin/leverage/liquidation math itself** (row `6.4`'s actual deliverable: leveraged `Position` fields, tiered maintenance-margin brackets, funding-rate application, `MarginMonitor` risk checker, backtest auto-liquidation). This spec only produces the mark-price data this future work will consume.
- **Row `6.5`** (extreme-market circuit breaker, automatic strategy de-risking) — an entirely separate follow-up sub-project per explicit user direction.
- **DEX/on-chain mark price, or any non-Binance venue's mark price** — Binance USDS-M Perpetual only, matching the project's existing single-venue-first pattern (`BinanceMarketDataProvider`, `apexes-gateway-binance`).
- **A general operational `apexes-cli` ingestion subcommand** — this spec produces the *library* function (`run_ingestion_loop`), not a wired-up CLI entry point; invoking it from `apexes-cli` (or anywhere else) is left for whoever first needs a running ingestion process.
- **Historical backfill of mark price for dates before this pipeline exists** — no retroactive backfill from Binance's REST history; only live-forward ingestion from the point this ships.

---

## 二、`apexes-core`: `MarketEvent::MarkPrice`

```rust
// crates/apexes-core/src/market_data.rs -- add to the MarketEvent enum:
pub enum MarketEvent {
    Tick(Tick),
    Depth(DepthUpdate),
    Bar(Bar),
    Trade(TradeEvent),
    MarkPrice(MarkPriceUpdate),
}

/// A perpetual/futures instrument's current mark price -- the
/// index-and-funding-basis-adjusted price real exchanges use for
/// margin ratio / liquidation / unrealized PnL calculations, DELIBERATELY
/// separate from `TradeEvent.price` (last trade) or order-book mid,
/// both of which are more easily manipulated/noisy for that purpose.
/// Meaningless for `AssetClass::Spot` instruments -- never emitted for
/// them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarkPriceUpdate {
    pub symbol: String,
    pub venue: Venue,
    pub mark_price: Decimal,
    pub timestamp: DateTime<Utc>,
}
```

Every existing exhaustive `match` over `MarketEvent` in the workspace must be updated to handle the new variant (`event_timestamp` helpers in `apexes-backtest::event_loop`, `apexes-live-trading::runner`, `apexes-paper`, `apexes-ems::in_memory`, and any `MatchingEngine`/`update_last_price`-style dispatch) — a full `file_grep` for `match.*MarketEvent|match event {` across the workspace at implementation time is required to find every site; a reasonable default for all of them is "no-op for direct order-book/ledger effects, contributes `timestamp` to whatever `event_timestamp` helper exists, like `Tick` already does".

---

## 三、`apexes-market-data`

### 3.1 Parameterizing `trade_task`/`depth_task`/`ticker_task` over exchange

Today each task hardcodes `BinanceSpot::default()` directly in its subscription tuple. `barter-data` already defines `BinanceFuturesUsd = Binance<BinanceServerFuturesUsd>` implementing the same `StreamSelector` traits (`PublicTrades`/`OrderBooksL1`/`OrderBooksL2`) as `BinanceSpot`. Each task becomes generic over the exchange type (or takes an enum parameter selecting which `barter-data` exchange constant to use per-symbol, since a single `subscribe()` call may mix Spot and Perpetual instruments) — implementation detail to be settled in the plan, not this spec, but the key behavioral requirement: **Spot instruments' existing behavior must not change** (same `MarketDataInstrumentKind::Spot`, same output), and Perpetual instruments get the equivalent treatment via `MarketDataInstrumentKind::Perpetual` + `BinanceFuturesUsd`.

### 3.2 `mark_price_task.rs` (new)

```rust
//! [`run`]: subscribes to Binance's public `<symbol>@markPrice` combined
//! WebSocket stream for `symbols` (USDS-M Perpetual only), converting
//! each message into `MarketEvent::MarkPrice`. Hand-rolled raw
//! WebSocket (via `tokio-tungstenite`, mirroring
//! `apexes_gateway_binance::user_data_stream`'s existing pattern) --
//! `barter-data` 0.11 has no mark-price subscription kind at all.

pub async fn run(symbols: Vec<String>, tx: UnboundedSender<MarketEvent>) {
    // wss://fstream.binance.com/stream?streams=<sym1>@markPrice/<sym2>@markPrice/...
    // Reconnect-on-disconnect loop (unlike barter-data's tasks, which
    // get reconnection for free from the library) -- mirrors
    // user_data_stream.rs's own reconnect handling for its raw socket.
}
```

Binance's `<symbol>@markPrice` message shape (1s update speed, the non-`@1s` variant is 3s — this design uses `@1s` for responsiveness):

```json
{
  "e": "markPriceUpdate",
  "E": 1234567890000,
  "s": "BTCUSDT",
  "p": "11794.15000000",
  "P": "11784.62659091",
  "r": "0.00038",
  "T": 1234567890000
}
```

Only `s` (symbol), `p` (mark price), and `E` (event time, used as `timestamp`) are consumed for this spec; `P` (index price), `r` (funding rate), and `T` (next funding time) are present in the raw message but NOT carried into `MarkPriceUpdate` (see §二's YAGNI decision — add them when the margin/funding design actually needs them, not speculatively now).

### 3.3 `BinanceMarketDataProvider::subscribe` extended

`filter_binance_spot_instruments` (currently named for its Spot-only behavior) becomes `filter_binance_instruments`, accepting both `AssetClass::Spot` and `AssetClass::Perpetual` (still `Venue::Binance` only, still warning-and-skipping anything else). Perpetual instruments spawn all four tasks (`trade_task`/`ticker_task`/`depth_task`/`mark_price_task`); Spot instruments spawn the same three as today plus nothing new (no mark price for Spot).

### 3.4 `ingestion.rs` (new): general-purpose ingestion loop

```rust
//! [`run_ingestion_loop`]: the project's first general-purpose
//! live-market-data-to-ClickHouse ingestion path. Subscribes to
//! `provider` for `instruments`, buffers incoming `MarketEvent`s by
//! variant (StorageWriter::write_batch requires a homogeneous batch),
//! and flushes each variant's buffer to `writer` whenever it reaches
//! `MAX_BATCH_SIZE` events OR `FLUSH_INTERVAL` has elapsed since that
//! variant's last flush, whichever comes first -- balances write
//! latency (a low-volume symbol's data shouldn't wait indefinitely)
//! against ClickHouse insert efficiency (one-row-at-a-time inserts are
//! wasteful). Runs until `provider`'s stream ends or `stop_signal`
//! fires (graceful-shutdown convention matching `run_live_trading`/
//! `run_paper_trading`'s existing `oneshot::Receiver<()>` pattern).

const MAX_BATCH_SIZE: usize = 500;
const FLUSH_INTERVAL: Duration = Duration::from_secs(5);

pub async fn run_ingestion_loop(
    provider: &dyn MarketDataProvider,
    writer: &StorageWriter,
    instruments: &[Instrument],
    stop_signal: oneshot::Receiver<()>,
) -> Result<(), ApexesError> {
    // Five buffers (Tick/Depth/Bar/Trade/MarkPrice), each with its own
    // last-flush Instant; a tokio::time::interval tick (checked
    // alongside the event stream via tokio::select!) triggers
    // time-based flushes independent of new events arriving.
}
```

Errors from `writer.write_batch` are logged (`tracing::error!`) and the buffer is dropped (not retried indefinitely, not propagated as a fatal error) — one bad batch must not silently wedge ingestion for every other event type forever; this mirrors the project's existing "a violation from an in-trade monitor is logged, not propagated" convention (`apexes_backtest::event_loop::apply_fills`'s risk-checker handling) applied to a new context.

Adds `apexes-storage` as a new dependency of `apexes-market-data`.

---

## 四、`apexes-storage`: `MarkPriceRow` + `mark_prices` table

```rust
// crates/apexes-storage/src/schema.rs -- new DDL constant, following
// the exact existing Trade/Tick/Depth/Bar pattern:
pub const MARK_PRICES_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS mark_prices
(
    symbol      String,
    venue       String,
    mark_price  Decimal(38, 10),
    timestamp   DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (venue, symbol, timestamp)
";

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct MarkPriceRow {
    pub symbol: String,
    pub venue: String,
    pub mark_price: CkDecimal,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
}

// TryFrom<&MarkPriceUpdate> for MarkPriceRow / TryFrom<MarkPriceRow> for
// MarkPriceUpdate, mirroring TradeRow's exact conversion pattern
// (decimal_to_fixed/fixed_to_decimal, parse_venue).
```

`StorageWriter::new` gains a fifth `CREATE TABLE IF NOT EXISTS` call; `write_batch`'s `classify_batch`/`BatchVariant` gains a `MarkPrice` arm and a `write_mark_prices` private helper, mirroring `write_trades` exactly. `StorageReader::read_batch` gains the equivalent read-side arm.

---

## 五、`apexes-backtest`: replay verification

No structural change required — `StorageMarketDataSource::events()` already returns whatever `StorageReader::read_batch` yields as a flat `Vec<MarketEvent>`, chronologically merged with every other variant already (confirmed by reading `market_data_source.rs`). This spec's only `apexes-backtest`-side work is:
- Updating `run_event_loop`'s (and any other exhaustive-`match`-over-`MarketEvent` site's) handling to not panic/miscompile on the new variant (see §二's note).
- A test proving a `MarketEvent::MarkPrice` event replays through `run_backtest` without error (contributes to `event_timestamp`/clock-pacing exactly like `Tick` does, produces no direct ledger/matching-engine effect in this spec — that's the follow-up margin design's job).

---

## 六、Testing

- **`apexes-market-data`**: unit tests for `mark_price_task`'s WS-message-parsing logic (given a raw JSON message, produces the correct `MarketEvent::MarkPrice`); unit tests confirming `filter_binance_instruments` now accepts Perpetual instruments and routes them to all four tasks (mirroring the existing `subscribe_returns_subscription_failed_when_all_instruments_filtered_out`-style tests); a live-network `#[ignore]`d integration test connecting to the real Binance mark-price stream (matching this crate's and `apexes-gateway-binance`'s existing `#[ignore]`d-live-test convention), plus a similar one exercising `run_ingestion_loop` end-to-end against a live ClickHouse + live Binance feed for a short duration.
- **`apexes-storage`**: unit tests for `MarkPriceRow`'s `TryFrom` conversions (round-trip); a live-ClickHouse `#[ignore]`d integration test proving `write_batch`/`read_batch` round-trip `MarkPrice` events, matching the existing `live_clickhouse.rs` test file's pattern exactly.
- **`apexes-backtest`**: a test (using `InMemoryMarketDataSource`, no live dependencies) proving a `MarketEvent::MarkPrice` event flows through `run_backtest` without error.
- Full workspace `cargo build`/`cargo test --workspace -- --test-threads=1`/`cargo clippy --workspace --all-targets -- -D warnings`/`cargo fmt --check` must stay clean throughout, per this project's standing global constraint.

---

## 七、Non-goals recap (for the follow-up margin/leverage spec to pick up)

This spec deliberately stops at "mark price is now real, historical-replayable, and live-streamable data, and the project has its first general ingestion loop." It does NOT touch `apexes_strategy::Position`, `apexes_backtest::PortfolioLedger`, `apexes-risk`, `OrderIntent.leverage`/`.margin_mode`, tiered maintenance-margin brackets, funding-rate payment application, or any liquidation mechanic — all of that is `docs/FEATURES.md` row `6.4`'s actual scope, to be brainstormed as its own spec once this one is implemented and merged.
