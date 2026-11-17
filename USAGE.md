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
scripts/run-testnet.sh --check   # verifies keys, Postgres, testnet reachability, clock and rebuilds; starts nothing
scripts/run-testnet.sh           # starts Postgres if needed, rebuilds the latest code, opens the app
```

To start with a different strategy, pass it: `scripts/run-testnet.sh --strategy ~/Downloads/strategy.trend_guard.drift` (any readable file; a relative path is taken from where you run the script). It only pre-fills the Launcher's **Strategy file** box. You still press Launch, and can change the file there. Typos are caught before anything starts.

The window has a **Launcher** tab and one tab per open session. In **Launcher**: leave **Testnet** selected, keep `BTCUSDT` and `deploy/strategy.example.drift`, press **Launch on TESTNET**. It takes 10-20 s (the engine reconciles with the exchange first); the new session's console then opens by itself.

**Windows and tabs.** Open as many sessions as you like (**Open console** in the Launcher list); each keeps its own connection, and switching tabs loses nothing. A coloured dot on each tab shows its health, and a tab says `offline` or `HALTED` without you opening it; the active tab is the filled, underlined one. Closing a tab (the x, middle-click, or Cmd/Ctrl+W) only disconnects the console: the session keeps running.

| Keys (Ctrl instead of Cmd off macOS) | Does |
|---|---|
| Cmd+1 ... Cmd+9 | Go to tab N (1 is the Launcher) |
| Cmd+[ / Cmd+] | Previous / next tab, wrapping |
| Cmd+W | Close the console in front (never the Launcher) |
| Cmd+Shift+L | Cycle the theme: System, Light, Dark |

**The console's layout.** Top to bottom: the environment banner; one status line (connection, a TRADING / PAUSED / HALTED pill, how old the data is, and the controls, with **HALT TRADING** the one red button); the key figures as tiles (Equity, PnL, Drawdown, Exposure, Trades; with holdings from before the session, *Strategy PnL* leads and *From holdings* gets its own tile); the chart card, whose selector switches between **Equity**, each symbol's price, and **All** (every symbol as percent change since its first price, so BTC and BNB can share one axis); then two columns of cards, **details and positions/open orders** on the left and **risk limits and balances** on the right, and the strategy parameters in up to three columns. The columns sit side by side when the window is wide and stack when it is narrow; while an asset you hold has no price yet, Equity reads **pricing…** instead of a number; the page scrolls, and the top (banner, status line, HALT) never does.

**Recent orders.** Every order the strategy sends is listed, newest first, with its time, side, quantity, the price it filled at and its outcome: *Filled*, *Partially filled*, *Working*, *Rejected* (a risk limit or the exchange refused it; the reason is printed under the row) or *Closed* (it stopped working without filling). It sits in the empty space to the right of the tables when the window is wide, and under them when it is narrow; drag its left edge to resize. The last 100 are kept; `Show N older` reveals the ones beyond the newest 30.

**Theme.** The **Theme** menu at the top right (every window) picks System (follows your OS), Light or Dark; the choice is remembered in `~/.apexes/ui.json`. The environment banner stays loud in both: MAINNET red, TESTNET blue. The window remembers its size and position, and will not shrink below a readable size.

You should see a blue banner `TESTNET (virtual funds)`, equity of roughly 10 000 USDT plus the value of the base asset you trade (about 12 000 for ETHUSDT, about 75 000 for BTCUSDT), and the strategy's three parameters `notional`, `step` and `enabled`.

## 3. What to test

| Check | How | Expect |
|---|---|---|
| Banner | Look at the top of the console | Blue `TESTNET (virtual funds)`, never red or `UNKNOWN` |
| Evaluation | Wait a minute | Equity line draws; PnL near 0; fees `-` until a trade |
| Strategy trades | Wait 1-3 min (it needs 20 trades to warm up) | A BUY worth about 20 USDT (for example 0.011 ETH) appears and fills; `Trades` and `Turnover` in the evaluation move |
| Live tuning | Change `notional` to `40`, press **Set** | The table shows the new value; the position grows to about 40 USDT |
| Switch off | Set `enabled` to `0` | It sells down what it holds and stops buying |
| Pause / Resume | Use the buttons | Banner says `STRATEGY PAUSED`; equity keeps updating |
| Halt | **HALT TRADING** (one click) | Red `TRADING HALTED` banner; open orders cancelled; no new orders |
| Resume | **Resume trading** | Needs a second confirming click |
| Survives the window | Close the app, run the script again | The session is still in the Launcher list as `running`; **Open console** reconnects |
| Stop | **Stop** then **Yes, stop** | Session goes `stopped`; **Remove files** cleans it up |
| Risk limits | In the launcher set `Max position per symbol` to a very small value (for example `0.001`), relaunch | Orders are blocked (look for `order rejected by risk checker` in the log) |

The risk limits default to: order value ≤ 100 USDT, order and position size ≤ 1 base unit, 50 orders a day. The demo strategy holds about 20 USDT worth of the symbol (sized by value, so one setting works for BTC and ETH; Binance refuses orders worth less than 5 USDT). Only orders the exchange accepts count against the daily cap.

## 4. Where things are

| What | Where |
|---|---|
| Session log | `~/.apexes/sessions/<id>.log` (the first place to look when something fails) |
| Gateway logs | `~/.apexes/gateways/testnet-<id>.log`, one per gateway (a session on a symbol no running gateway carries starts its own; `ls ~/.apexes/gateways`). Gateways from an older build are in `~/.apexes/market-data-gateway-testnet.log` |
| Audit log (commands, logins) | `~/.apexes/sessions/<id>.audit.jsonl` |
| Per-session control token | `~/.apexes/sessions/<id>.token` (mode 600; generated for you) |
| Orders in Postgres | `docker exec apexes-postgres psql -U apexes -d apexes_live -c "select id,symbol,status from orders order by created_at desc limit 10;"` (live sessions use `apexes_live`; the test suites use and wipe `apexes`) |

Metrics and health are on the engine's port (shown in the launcher): `/healthz`, `/readyz`, `/metrics` (the latter needs the token).

## 5. Stopping everything

- A session: **Stop** in the Launcher (clean). Closing the window does **not** stop it.
- The gateways, once no session uses them: `pkill -f apexes-market-data-gateway`.
- Postgres: `docker compose stop postgres`.

## 6. If something goes wrong

| Symptom | Likely cause |
|---|---|
| `Missing credential variable` in the launcher | Start via `scripts/run-testnet.sh`, not by double-clicking the binary (it sets the variables) |
| `user-data subscription rejected ... -2015` | Wrong/expired testnet key, or a key from your real Binance account |
| `... -1021` / requests very slow | Clock off by more than ~5 s, or the testnet's dead DNS address (see `docs/RUNBOOK.md`); the script prints your offset |
| Launch fails with `live startup recovery failed` | Open orders on the account that this system didn't place (cancel them), or Postgres down |
| Reconcile warnings `-1100` in the log | An order row in the database with a non-Binance id. Live sessions use their own database (`apexes_live`) so test runs cannot cause this; if you see it, remove the row |
| `market data stale ... halted` | No market data for 90 s on testnet (30 s on mainnet). Check the gateway log; resume with **Resume trading** once data flows |
| Everything vanished | The testnet resets periodically: balances and orders are wiped; relaunch |

## 7. What has and has not been verified

Verified against the real testnet by automated tests: launch through the supervisor, authenticated console connection, TESTNET identity, sane equity, Halt acknowledged, clean stop, and real order updates. **Not yet verified, so these are what your testing is for:** clicking through the actual window, a strategy-driven trade, `SetParam` changing a running strategy's orders, the stale-feed halt, restart-and-reconcile, a testnet reset. Please report anything odd with the relevant log lines. Details: `docs/PLAN.md`.
