# Binance Spot testnet MVP — design

Date: 2026-11-15. Execution tracking: [`docs/PLAN.md`](../../PLAN.md) ("Now"). Supersedes #13c Tasks 9–11 (see PLAN).

## 1. Goal and scope

An operator starts a **Binance Spot** live session from a GUI, on the **Spot Test Network** by default, watches it, tunes it, halts it, and judges the strategy and its risk from a live timeline.

In scope: Spot only; one network switch; mandatory pre-trade risk limits; a supervisor that launches the gateway and engine; a launcher view; an evaluation view; a trivial sample strategy.
Out of scope: futures (margin, flatten), paper mode, backtest launching, order entry from the UI, multi-session view, mainnet acceptance testing.

"Binance" means the Binance exchange's Spot market, not BNB Smart Chain or the BNB token. The Spot Test Network (`testnet.binance.vision`) is a separate system with its own accounts, free API keys (GitHub login), virtual funds, and periodic resets to a blank state.

## 2. Network switch (task A)

```rust
// apexes-gateway-binance
pub enum BinanceNetwork { Mainnet, Testnet }   // serde, Copy, Display

pub struct BinanceEndpoints {
    pub spot_rest: &'static str,    // https://api.binance.com | https://testnet.binance.vision
    pub spot_ws_api: &'static str,  // wss://ws-api.binance.com:443/ws-api/v3 | wss://ws-api.testnet.binance.vision/ws-api/v3
    pub spot_market_ws: &'static str, // wss://stream.binance.com:9443/ws | wss://stream.testnet.binance.vision/ws
}
impl BinanceNetwork { pub fn endpoints(self) -> BinanceEndpoints }
```

Rules:
1. `endpoints()` is the **only** place a Binance Spot hostname appears. A test greps the workspace sources and fails if one appears elsewhere (futures hosts stay where they are; futures is out of scope and untouched).
2. `LaunchConfig::Live` gains a **required** `network: BinanceNetwork` (no serde default). Missing ⇒ parse error naming the field. The gateway gets `--network <mainnet|testnet>`, also required.
3. **Mismatch refusal.** The gateway's `/market` upgrade response carries `X-Apexes-Network: <mainnet|testnet>` (an HTTP header, so the bare-`MarketEvent` frame protocol and its existing tests are unchanged). `GatewayMarketDataProvider`, when given an expected network, checks it on every connect: `subscribe` fails with a clear error on a mismatch, and a mismatch after a reconnect (gateway restarted on another network) ends the stream so the session stops rather than trading on the wrong prices. A gateway that sends no header (older build) counts as a mismatch for a live session.
4. **Per-network credentials.** `resolve_binance_credentials(ref, network, lookup)` reads `APEXES_CRED_<REF>_<NETWORK>_API_KEY` / `_API_SECRET` (e.g. `APEXES_CRED_BINANCE_MAIN_TESTNET_API_KEY`). The unsuffixed legacy names are no longer read, so mainnet keys can never be loaded for a testnet session. Error names the exact variable.
5. `BinanceGatewayConfig` is built from `network.endpoints()`. **Correction (found by running the real engine on 2026-11-15):** Binance removed the listenKey user-data stream (`POST /api/v3/userDataStream` returns HTTP 410 since 2026-02-20), so the Spot user-data stream now subscribes on the WebSocket API (`spot_ws_api`) with `userDataStream.subscribe.signature` (HMAC), via the `ParamSigner` trait; see `ws_user_stream.rs`. Futures keeps its listenKey stream and is out of scope.
6. The engine logs `network=<..>` at startup and puts it in `SessionSnapshot.network` (task B).

## 3. Testnet market data (task A2)

`BinanceMarketDataProvider` is built on `barter-data` 0.11, whose `Binance<Server>` is generic over a public `ExchangeServer` trait; trades and L1 are generic over the server, L2 is implemented only for mainnet `BinanceSpot`.

- Add `BinanceServerSpotTestnet` implementing `ExchangeServer` (`websocket_url() = spot_market_ws` of testnet) and a `BinanceSpotTestnet = Binance<BinanceServerSpotTestnet>` alias. Needs a distinct `ExchangeId`; if `barter-data` cannot accept one for this (it is an enum), fall back to `BinanceSpot` type with a custom connect URL, or, failing both, a small hand-rolled trade/ticker WebSocket client (the crate already does this for mark price). Decide in a spike that compiles and connects before building on it.
- `BinanceMarketDataProvider::new()` stays mainnet; add `BinanceMarketDataProvider::for_network(network)`. Testnet provides Trade and Tick (and self-aggregated Bar); **no Depth**. The gateway logs and the UI shows "depth unavailable on testnet".
- The gateway constructs its provider from `--network`.

## 4. Mandatory live risk limits (task A3)

Today `apexes-engine` registers no `CompositeRiskChecker`, and `run_live_trading` treats absence as "no risk checking". Change:

- `SessionMode::Live` gains required `risk_limits: LiveRiskLimits` (serde mirror of `apexes_risk::RiskLimits` with `max_order_notional`, `max_position_per_symbol`, `max_order_quantity`, `max_daily_trade_count` **required**; `min_account_equity` and `max_daily_volume` optional).
- `run_live_mode` builds `CompositeRiskChecker::new().with_pre_trade_check(Box::new(LimitChecker::new(limits)))` and registers `Arc<CompositeRiskChecker>` before `run_live_trading`. Validation rejects zero/negative limits.
- Known weakness, documented not hidden: `LimitChecker`'s `min_account_equity` compares the **raw sum of balances across assets** (not mark-to-market), so it is only meaningful for single-quote-asset accounts. The launcher therefore leaves it unset by default and the evaluation view shows true equity separately. Fixing the checker is a separate item (PLAN carry-forward).
- Test: a strategy emitting an over-limit order in a live-mode loop is rejected and the rejection is visible in the snapshot/log.

## 5. Snapshot additions (tasks B, C)

`SessionSnapshot` (in `apexes-backtest::control`) gains `#[serde(default)]` fields so old and new peers interoperate:
- `network: Option<String>`, `session_id`, `strategy_name`.
- `evaluation: Option<EvaluationSummary>`: realised/unrealised PnL, current and max drawdown, gross exposure fraction, fees paid, trade count, turnover, time-in-market fraction, per-limit utilisation `Vec<{name, used, limit}>`.
- The **equity history is not in every snapshot**. A new `SessionMessage::History { points: Vec<(DateTime<Utc>, Decimal)> }` is sent once when a console connects and then incremental points ride in snapshots (`equity_tail`, at most N points since the last send). Engine keeps a bounded ring (default 5 000 points, halve-by-pairs downsampling when full).

Metrics are computed from the existing `PerformanceTracker` (equity curve, fills) plus the ledger; no second source of truth.

## 6. Supervisor (task D)

New library crate `apexes-supervisor` (no GUI dependency):

- `SessionLauncher::launch(request) -> SessionHandle`: validates the request, writes `LaunchConfig` to `~/.apexes/sessions/<id>.json` (`0600`), ensures a gateway for the network (reuses a live one by PID file, else spawns it), spawns `apexes-engine --launch-config <path>` **detached** (own session via `pre_exec`/`setsid`-equivalent using `std::os::unix::process::CommandExt::process_group(0)`), waits for the engine's PID file (`<pid>\n<port>\n`) with a timeout, returns its control URL.
- Secrets: the control token is `rand`-generated (32 bytes, hex), passed as `APEXES_CONTROL_TOKENS=gui:operator:<token>` in the child's **environment**, and written `0600` to `~/.apexes/sessions/<id>.token` so a restarted GUI can reconnect. Credentials: the supervisor copies only `APEXES_CRED_<REF>_<NETWORK>_API_KEY/_SECRET` from its own environment and refuses to launch, naming the variable, if absent. No secret on any command line or in the launch JSON.
- `discover() -> Vec<SessionInfo>`: scans PID files, checks liveness (`kill(pid, 0)` via `libc`/`nix` — Unix only, documented), prunes stale files.
- `stop(id)`: sends the engine `Stop` over `/session/ws` first, then SIGTERM after a grace period; never SIGKILL a live session without an explicit force flag.
- Respawn after N consecutive connection failures is a method the GUI calls; it is **off by default for live** (an engine that died needs a human to look at why; restart policy is the operator's decision), documented in the runbook.
- Binary lookup: next to the current executable, else `PATH`; a missing binary is a clear error.

## 7. GUI (tasks B, C, D, in `apexes-ui`)

All in the `apexes-operator` binary (keeps working when market data is down):
- **Launcher view:** network (default Testnet; choosing Mainnet requires typing `MAINNET`), strategy (list from `apexes-strategy-loader` + a path picker for a `.drift`), instruments (validated against the network's exchange info is out of scope; free text, validated for shape), risk limits pre-filled conservatively, snapshot interval. Lists discovered sessions with Connect / Stop. Shows which credential variable is missing instead of launching.
- **Banner:** persistent `TESTNET` (neutral) or `MAINNET — REAL FUNDS` (red) from `SessionSnapshot.network`, never from launcher state.
- **Evaluation view:** `egui_plot` equity line with drawdown shading, and a metrics table with limit-utilisation bars (amber ≥ 70 %, red ≥ 90 %).
- `egui_plot` is added to the workspace dependency table at the version that resolves with `egui` 0.35.

## 8. Sample strategy (task E)

`deploy/strategy.example.drift`: a deliberately trivial Spot strategy that holds a fixed small base quantity when the last price is above a simple moving average of the last `n` trades and is flat otherwise, with `param()` calls inside `on_market_event` for `n` and `quantity`. It exists to exercise the pipeline and hot reload, not to make money.

## 9. Verification (task F, owner's instruction)

Per the owner, tasks A–E are built first with focused unit tests only; one comprehensive verification follows:
1. Workspace build, test (`--test-threads=1`), clippy `-D warnings`, fmt.
2. Subprocess e2e without the network: supervisor launches a real engine + gateway against local fakes for the exchange REST/user-stream (wiremock/local WS), proving launch, discovery, token reconnect, stop, mismatch refusal, and risk rejection.
3. Testnet acceptance run with the owner's testnet keys: startup recovery, strategy trading, halt/resume, stale-feed halt, stop, restart-and-reconcile, unclean kill, and behaviour across a testnet reset.
4. Whole-branch review, then `FEATURES.md` / `README.md` / `PLAN.md` / `RUNBOOK.md` updates.

Items needing the owner: testnet API keys (before step 3).

## 10. Risks

- `barter-data` may not accept a testnet server type cleanly (§3 has fallbacks; spike first).
- `egui_plot` version resolution against `egui` 0.35 is unverified.
- Detached spawn and PID handling are Unix-only; documented, not hidden.
- GUI behaviour is hard to unit-test; use `egui_kittest` for state/render smoke tests and keep logic in non-GUI modules (`operator.rs` pattern).
- Testnet resets can desynchronise local order state; covered in step 3.
