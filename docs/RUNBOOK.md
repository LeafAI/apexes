# Apexes operations runbook

Covers running the live engine and market-data gateway under Docker Compose,
what the safety machinery does, and how to respond when it trips.

## Deploy

```sh
cp deploy/launch-config.example.json deploy/launch-config.json   # edit instruments, mode params
cp my-strategy.drift deploy/strategy.drift

export APEXES_CONTROL_TOKEN=$(openssl rand -hex 32)               # required for live
export APEXES_GATEWAY_TOKEN=$(openssl rand -hex 32)               # recommended
export APEXES_CRED_BINANCE_MAIN_API_KEY=...                       # credentials_ref "binance-main"
export APEXES_CRED_BINANCE_MAIN_API_SECRET=...

docker compose up -d                       # postgres, clickhouse, market-data-gateway
docker compose --profile live up -d engine # the live engine
```

Ports are published on `127.0.0.1` only: engine `9100`, gateway `9001`. Put a
TLS-terminating proxy or VPN in front before exposing either (TLS/mTLS is not
built in yet).

### Environment

| Variable | Used by | Meaning |
|---|---|---|
| `APEXES_CONTROL_TOKEN` | engine | Bearer token for `/session/ws` and `/metrics`. **Live sessions refuse to start without it.** |
| `APEXES_AUDIT_LOG` | engine | Append-only JSONL audit trail (default `~/.apexes/audit-<session>.jsonl`). **Live sessions refuse to start if it cannot be opened.** |
| `APEXES_ENGINE_BIND` | engine | Listen address (default `127.0.0.1:0`; the image sets `0.0.0.0:9100`). |
| `APEXES_GATEWAY_TOKEN` | gateway, engine | Bearer token for the gateway's `/market`; the engine sends it. |
| `APEXES_CRED_<REF>_API_KEY/_SECRET` | engine | Exchange credentials for `credentials_ref`. |
| `RUST_LOG`, `APEXES_LOG_FORMAT=json` | all | Log filter / JSON lines. |

## What happens at startup (live)

1. Token and audit log are validated (refuse otherwise).
2. Local open orders are reconciled with the exchange. Orders the exchange
   never received are marked `Rejected` after 60 s; fills missed while down are
   recorded.
3. The engine **refuses to start** (non-zero exit) if the exchange has open
   orders it has no record of, or a local order cannot be resolved.
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
{"type":"Command","ResumeTrading":null}
```

Resuming never happens automatically: confirm the cause, then send
`ResumeTrading`. Positions are *not* flattened by a halt (tracked in the
roadmap); close them manually if required.

## Monitoring

Prometheus scrapes `http://<engine>:9100/metrics` (send
`Authorization: Bearer $APEXES_CONTROL_TOKEN`). Suggested alerts:

| Condition | Metric |
|---|---|
| Halted | `apexes_kill_switch_engaged == 1` |
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
per line: `ts`, `actor` (`token:<fingerprint>`; `anonymous` for rejected
requests), `action` (`ws_connect`, `command`, `unauthorized_request`), `detail`.
It lives on the `apexes-engine-data` volume; back it up.

## Not covered yet

High availability / failover, TLS/mTLS, KMS-held keys, flatten-on-halt, and
post-trade compliance reporting are tracked in the production-readiness epic.
