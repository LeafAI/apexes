# Apexes operations runbook

Covers running the live engine and market-data gateway under Docker Compose,
what the safety machinery does, and how to respond when it trips.

## Quick start: a Binance Spot **testnet** session from the GUI

The Spot Test Network is a separate system from your Binance account: its own
keys, virtual funds, periodic resets. Use it first.

1. **Keys.** Log in at <https://testnet.binance.vision/> with GitHub and
   generate an HMAC key. Put it where the app can read it as environment
   variables, never in the launcher or a config file:
   `APEXES_CRED_BINANCE_MAIN_TESTNET_API_KEY` and
   `APEXES_CRED_BINANCE_MAIN_TESTNET_API_SECRET` (`binance-main` is the
   default credentials name; the network is part of the variable name, so
   testnet and mainnet keys can never be confused).
2. **Postgres** (order store): `docker compose up -d postgres`.
3. **Build:** `cargo build --release -p apexes-engine -p apexes-market-data-gateway -p apexes-ui`.
4. **Run:** `target/release/apexes-operator`. The launcher defaults to Testnet and
   conservative limits. Pick the symbols and strategy (`deploy/strategy.example.drift`
   is a trivial demo, not a recommendation), press **Launch on TESTNET**, then
   **Open console**.

The launcher starts a market-data gateway and an engine as **detached
processes**: closing the window never stops a session, and reopening the app
finds running sessions again. A launch only counts as started once the engine
has passed startup recovery; if it refuses, the launcher shows its log.
Everything the app starts is logged under `~/.apexes/` (`*.log`, mode 0600),
alongside a per-session control token it generates itself.

**Mainnet** is the same flow with the Network switched and the word `MAINNET`
typed to confirm. It trades real funds; do not use it until a testnet run has
behaved the way you expect.

### Binance changed its user-data API (2026-02-20)

The old listenKey user-data stream is gone (`POST /api/v3/userDataStream`
answers **HTTP 410**). Spot order updates now come from the WebSocket API's
`userDataStream.subscribe.signature`, which this system uses (HMAC keys work;
no Ed25519 is needed). If a session dies at startup with
`user-data subscription rejected`, the message carries Binance's own error
code (`-2015`: key, IP restriction or permissions; `-1021`: your clock is off).

### Network gotcha: a dead address in the testnet's DNS

`testnet.binance.vision` resolves to several addresses, and at the time of
writing one of them accepts no connections. A client that waits for it stalls
about 30 s per request, which turns signed requests into `-1021` (outside
`recvWindow`) rejections. The Binance client here connects with a 4 s
timeout per address so it moves on to a working one. If testnet requests are
slow or failing with `-1021`, test `curl -w '%{time_connect}' https://testnet.binance.vision/api/v3/ping`
a few times first.

## Deploy

```sh
cp deploy/launch-config.example.json deploy/launch-config.json   # edit instruments, mode params
cp my-strategy.drift deploy/strategy.drift

# Named, role-scoped control tokens (>= 16 chars each); at least one of these
# two is REQUIRED for live. Operators may send commands; viewers are read-only.
export APEXES_CONTROL_TOKENS="alice:operator:$(openssl rand -hex 24),bob:viewer:$(openssl rand -hex 24)"
# export APEXES_CONTROL_TOKEN=$(openssl rand -hex 32)             # legacy single shared operator token
export APEXES_GATEWAY_TOKEN=$(openssl rand -hex 32)               # recommended
export APEXES_NETWORK=testnet                                     # testnet | mainnet; gateway AND engine must agree
export APEXES_CRED_BINANCE_MAIN_TESTNET_API_KEY=...               # credentials_ref "binance-main", per network
export APEXES_CRED_BINANCE_MAIN_TESTNET_API_SECRET=...

docker compose up -d                       # postgres, clickhouse, market-data-gateway
docker compose --profile live up -d engine # the live engine
```

Ports are published on `127.0.0.1` only: engine `9100`, gateway `9001`. Put a
TLS-terminating proxy or VPN in front before exposing either (TLS/mTLS is not
built in yet).

### Environment

| Variable | Used by | Meaning |
|---|---|---|
| `APEXES_CONTROL_TOKENS` | engine | `name:role:token[,...]` bearer tokens for `/session/ws` and `/metrics`; `role` is `operator` (may send commands) or `viewer` (read-only). Tokens must be >= 16 characters; names are unique and appear in the audit log. |
| `APEXES_CONTROL_TOKEN` | engine | Legacy single shared operator token (audited as `shared`). Combine with, or use instead of, the above. **Live sessions refuse to start with neither.** |
| `APEXES_AUDIT_LOG` | engine | Append-only JSONL audit trail (default `~/.apexes/audit-<session>.jsonl`). **Live sessions refuse to start if it cannot be opened.** |
| `APEXES_ENGINE_BIND` | engine | Listen address (default `127.0.0.1:0`; the image sets `0.0.0.0:9100`). |
| `APEXES_GATEWAY_TOKEN` | gateway, engine | Bearer token for the gateway's `/market`; the engine sends it. |
| `APEXES_NETWORK` | compose | `testnet` or `mainnet`: passed to the gateway as `--network`. Must equal the launch config's `network`. Required (no default). |
| `APEXES_CRED_<REF>_<NETWORK>_API_KEY/_SECRET` | engine | Exchange credentials for `credentials_ref` on that network, e.g. `APEXES_CRED_BINANCE_MAIN_TESTNET_API_KEY`. The network is part of the name; the old un-suffixed names are no longer read. Testnet keys come from `https://testnet.binance.vision/` (GitHub login), not from your Binance account. |
| `RUST_LOG`, `APEXES_LOG_FORMAT=json` | all | Log filter / JSON lines. |

## What happens at startup (live)

1. Tokens and the audit log are validated (refuse otherwise).
2. Local open orders are reconciled with the exchange. Orders the exchange
   never received are marked `Rejected` after 60 s; fills missed while down are
   recorded.
3. The engine **refuses to start** (non-zero exit) if the exchange has open
   orders it has no record of, open orders that were **not placed through this
   system** (e.g. entered by hand — cancel them first), or a local order that
   cannot be resolved.
4. Balances and positions are then read from the exchange and seed the ledger;
   `initial_balances` in the launch config is ignored in live mode.

A refusal under `restart: unless-stopped` retries with Docker's backoff. If
`docker compose ps` shows the engine restarting, read its logs
(`docker compose logs engine`) — do not just wait. Typical causes: an order
placed manually on the account (cancel it), credentials/API outage, Postgres
down.

## Kill switch

Engages automatically when no market data arrives for
`stale_data_timeout_secs` (launch config), or manually with a `Halt` command on
`/session/ws`. While engaged: all new orders are rejected, every open order is
cancelled (retried until none remain), strategy dispatch is paused.
`/healthz` stays 200 (do **not** restart a halted engine — it loses nothing, but
it is not a fault); `/readyz` returns 503.

```
# message frames are JSON; authenticate with the Authorization header
{"type":"Command","Halt":{"reason":"operator: investigating"}}
{"type":"Command","Halt":{"reason":"exchange incident","flatten":true}}
{"type":"Command","ResumeTrading":null}
```

Resuming never happens automatically: confirm the cause, then send
`ResumeTrading`.

### Flatten

`"flatten":true` additionally **closes open futures positions** once the
cancel-all sweep has finished: opposite-side market orders sized from the
exchange's own position report (not the local ledger), re-polled every 2 s
for up to 10 rounds, never re-ordering a symbol whose closing order is still
working. These orders deliberately bypass the halt and the pre-trade risk
checks — reducing exposure must not be blockable.

- Spot balances are **not** sold: they are holdings, not exposure, and
  liquidating them is a decision for you.
- If it cannot finish (exchange errors, a position in a symbol missing from
  the launch config, rounds exhausted) the engine logs
  `flatten INCOMPLETE -- positions still open` and
  `apexes_unflattened_positions` stays above zero: **close them manually**.
- The automatic stale-data halt does not flatten (a dead feed is a poor moment
  to fire market orders unattended); send a `Halt` with `flatten` if you want it.

## Operator console

`apexes-operator` is a desktop console for a running engine: connection and
halt state, equity/PnL, positions (with margin ratio), open orders, balances,
editable strategy parameters, and the controls.

```sh
APEXES_CONTROL_TOKEN=<an operator or viewer token> \
  apexes-operator --engine ws://127.0.0.1:9100/session/ws
```

(Use the environment variable, not a `--token` argument: command lines are
visible to other users of the machine. The engine port is published on
`127.0.0.1` only; reach a remote engine through an SSH tunnel or VPN.)

What it does and does not let you do:

- **HALT TRADING** is one click, always available to operators: stopping must
  never be behind a dialog. **Resume trading**, **Halt + flatten** and **Stop
  session** ask for a second, explicit confirmation, and say what they will do.
- A **viewer** token sees everything but every control is disabled; the engine
  enforces this too (a modified client is refused and the attempt audited).
- Nothing is sent while disconnected, and anything clicked against a dead link
  is discarded on reconnect, so an old click can never fire later.
- A red **TRADING HALTED** banner shows the reason (and when a flatten is in
  progress); an amber **NO CONNECTION** banner means everything below it may
  be out of date; **snapshot Ns old** turns amber after 5 s. The engine only
  pushes snapshots as market data arrives, so an old snapshot usually means a
  stalled feed, which is exactly when the halt state above still updates.
- A console that connects late shows the engine's last known state
  immediately. "Waiting for the first snapshot" means the engine has not
  pushed a real one yet (it never shows a fake all-zero account).

The window has a **Launcher** tab (start, find, stop sessions) and a **Console**
tab (this section). The console always opens with a banner from the *engine's
own* report: `TESTNET (virtual funds)`, `MAINNET - REAL FUNDS`, or
`ENVIRONMENT UNKNOWN` if the engine says nothing (never assume testnet). Below
the controls is an **Evaluation** view: an equity timeline with its running
peak, PnL, drawdown, exposure, fees, trades, time in market, and how close each
risk limit is to being hit (amber from 70 %, red from 90 %). Live sessions
carry pre-trade risk limits (order size/notional, position size, trades per
day) that are required in the launch config; an engine will not start without
them.

`apexes-operator --engine ws://...` still opens just the console, for an engine
started elsewhere.

Not in the console yet: placing or cancelling individual orders, editing risk
limits, and watching several sessions at once.

## Monitoring

Prometheus scrapes `http://<engine>:9100/metrics` (send
`Authorization: Bearer <any valid token, viewer included>`). Ready-to-load rules are in
`deploy/prometheus/alerts.yml` (same thresholds as below). An importable Grafana dashboard is in
`deploy/grafana/apexes-engine.json` (Dashboards → Import; pick your Prometheus
data source).

| Condition | Metric |
|---|---|
| Halted | `apexes_kill_switch_engaged == 1` |
| Positions left open after a flatten | `apexes_unflattened_positions > 0` for > 1 min while halted |
| Stream down | `apexes_stream_connected == 0` for > 1 min |
| Reconnect storm | `rate(apexes_stream_reconnects_total[10m]) > 3` |
| Feed stale | `apexes_market_data_idle_seconds > 15` |
| Missed state | `increase(apexes_reconcile_changes_total[1h]) > 0` (push stream dropped events) |
| Reconcile failing | `increase(apexes_reconcile_runs_total{result="error"}[10m]) > 0` |
| Rejects / uncertain | `rate(apexes_orders_submitted_total{result=~"rejected|uncertain"}[10m])` |
| Rate limited | `increase(apexes_rest_rate_limited_total[10m]) > 0` |
| Order latency | `histogram_quantile(0.99, apexes_order_submit_latency_seconds_bucket)` |

## Audit log

`docker compose exec engine cat /var/lib/apexes/audit.jsonl` — one JSON object
per line: `ts`, `actor` (the token's name, e.g. `alice`; `shared` for the
legacy token; `anonymous` for rejected requests), `action` (`ws_connect`,
`command`, `command_denied` for a viewer's attempt, `unauthorized_request`),
`detail`.
It lives on the `apexes-engine-data` volume; back it up.

## Not covered yet

High availability / failover, TLS/mTLS, KMS-held keys, and
post-trade compliance reporting are tracked in the production-readiness epic.
