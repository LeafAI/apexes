# Running Apexes on the Binance Spot testnet

A short guide to getting one real session running and testing it by hand. Testnet only: virtual funds, separate keys from your Binance account. Mainnet is out of scope here.

## 1. One-time setup

1. **Keys.** `~/.apexes/binance.yaml` (mode `600`) needs your testnet HMAC key:
   ```yaml
   testnet:
     hmac-sha256:
       api-key: <64 chars>
       secret-key: <64 chars>
   ```
   Generate it at <https://testnet.binance.vision/> (log in with GitHub). The launcher script reads it; you never type keys into the app.
2. **Docker** (for Postgres, the order store). On macOS with colima: `colima start`.
3. **Python 3 with `websockets` and `pyyaml`**, only for the script's key reading (`pip3 install pyyaml websockets`).

## 2. Start

```sh
scripts/run-testnet.sh --check   # verifies keys, Postgres, testnet reachability, clock; starts nothing
scripts/run-testnet.sh           # starts Postgres if needed, builds if needed, opens the app
```

The app has two tabs. In **Launcher**: leave **Testnet** selected, keep `BTCUSDT` and `deploy/strategy.example.drift`, press **Launch on TESTNET**. It takes 10-20 s (the engine reconciles with the exchange first). When the status turns green, press **Open console**.

You should see a blue banner `TESTNET (virtual funds)`, equity around 75 000 (10 000 USDT + 1 BTC at testnet prices), and the strategy's two parameters `quantity` and `enabled`.

## 3. What to test

| Check | How | Expect |
|---|---|---|
| Banner | Look at the top of the console | Blue `TESTNET (virtual funds)`, never red or `UNKNOWN` |
| Evaluation | Wait a minute | Equity line draws; PnL near 0; fees `-` until a trade |
| Strategy trades | Wait ~2 min (it needs 20 trades to warm up) | A BUY of 0.001 BTC appears under positions / open orders, then fills (testnet is liquid) |
| Live tuning | Change `quantity` to `0.002`, press **Set** | The table shows the new value; the next order uses it |
| Switch off | Set `enabled` to `0` | It sells down what it holds and stops buying |
| Pause / Resume | Use the buttons | Banner says `STRATEGY PAUSED`; equity keeps updating |
| Halt | **HALT TRADING** (one click) | Red `TRADING HALTED` banner; open orders cancelled; no new orders |
| Resume | **Resume trading** | Needs a second confirming click |
| Survives the window | Close the app, run the script again | The session is still in the Launcher list as `running`; **Open console** reconnects |
| Stop | **Stop** then **Yes, stop** | Session goes `stopped`; **Remove files** cleans it up |
| Risk limits | In the launcher set `Max order quantity` below `quantity`, relaunch | Orders are rejected (look for `order rejected by risk checker` in the log) |

The risk limits default to small values (order ≤ 300 USDT / 0.002 BTC, position ≤ 0.005 BTC, 50 trades a day). The demo strategy buys 0.001 BTC.

## 4. Where things are

| What | Where |
|---|---|
| Session log | `~/.apexes/sessions/<id>.log` (the first place to look when something fails) |
| Gateway log | `~/.apexes/market-data-gateway-testnet.log` |
| Audit log (commands, logins) | `~/.apexes/sessions/<id>.audit.jsonl` |
| Per-session control token | `~/.apexes/sessions/<id>.token` (mode 600; generated for you) |
| Orders in Postgres | `docker exec apexes-postgres psql -U apexes -d apexes -c "select id,symbol,status from orders order by created_at desc limit 10;"` |

Metrics and health are on the engine's port (shown in the launcher): `/healthz`, `/readyz`, `/metrics` (the latter needs the token).

## 5. Stopping everything

- A session: **Stop** in the Launcher (clean). Closing the window does **not** stop it.
- The gateway, once no session uses it: `pkill -f apexes-market-data-gateway`.
- Postgres: `docker compose stop postgres`.

## 6. If something goes wrong

| Symptom | Likely cause |
|---|---|
| `Missing credential variable` in the launcher | Start via `scripts/run-testnet.sh`, not by double-clicking the binary (it sets the variables) |
| `user-data subscription rejected ... -2015` | Wrong/expired testnet key, or a key from your real Binance account |
| `... -1021` / requests very slow | Clock off by more than ~5 s, or the testnet's dead DNS address (see `docs/RUNBOOK.md`); the script prints your offset |
| Launch fails with `live startup recovery failed` | Open orders on the account that this system didn't place (cancel them), or Postgres down |
| Reconcile warnings `-1100` in the log | A stale order row in your local Postgres from an earlier test: harmless noise |
| Everything vanished | The testnet resets periodically: balances and orders are wiped; relaunch |

## 7. What has and has not been verified

Verified against the real testnet by automated tests: launch through the supervisor, authenticated console connection, TESTNET identity, sane equity, Halt acknowledged, clean stop, and real order updates. **Not yet verified, so these are what your testing is for:** clicking through the actual window, a strategy-driven trade, `SetParam` changing a running strategy's orders, the stale-feed halt, restart-and-reconcile, a testnet reset. Please report anything odd with the relevant log lines. Details: `docs/PLAN.md`.
