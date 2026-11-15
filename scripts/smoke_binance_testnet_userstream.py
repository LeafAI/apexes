#!/usr/bin/env python3
"""Smoke test: does Binance's NEW user-data-stream API work for us?

Binance removed the listenKey endpoints (HTTP 410 since 2026-02-20). The
replacement is `userDataStream.subscribe.signature` on the WebSocket API.
This script checks, against the Binance Spot TEST NETWORK only, the exact
things the engine will depend on:

  1. the WebSocket API is reachable                      (public `time`)
  2. your testnet keys are valid                         (signed GET /account)
  3. a signature-based subscription is accepted          (the thing to prove)
  4. order events arrive on that subscription, in what
     envelope and with which fields: a tiny limit order is placed far below
     the market (it cannot fill), then cancelled         (NEW, then CANCELED)

It only ever talks to testnet hosts (hard-coded; there is no flag to change
that), spends only virtual funds, never prints your key or secret, and
cancels the order it placed even if something fails.

Usage (zsh; `read -s` keeps the secret out of your shell history):

    read -s "APEXES_TESTNET_API_KEY?testnet API key: "; export APEXES_TESTNET_API_KEY
    read -s "APEXES_TESTNET_API_SECRET?testnet API secret: "; export APEXES_TESTNET_API_SECRET
    python3 scripts/smoke_binance_testnet_userstream.py

Keys come from https://testnet.binance.vision/ (log in with GitHub, then
"Generate HMAC_SHA256 Key"). They are NOT your Binance account's keys.
"""

import asyncio
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

try:
    import websockets
except ImportError:
    sys.exit("This script needs the `websockets` package: pip3 install websockets")

# Testnet only. Deliberately not configurable.
REST = "https://testnet.binance.vision"
WS_API = "wss://ws-api.testnet.binance.vision/ws-api/v3"
SYMBOL = "BTCUSDT"
RECV_WINDOW = 5000
EVENT_TIMEOUT = 20.0

results: list[tuple[str, bool, str]] = []


def record(name: str, ok: bool, detail: str = "") -> bool:
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f": {detail}" if detail else ""))
    return ok


def sign(secret: str, payload: str) -> str:
    return hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()


def rest(method: str, path: str, key: str, secret: str | None, params: dict | None = None):
    """One REST call. Signed when `secret` is given. Returns (status, json|text)."""
    params = dict(params or {})
    if secret is not None:
        params["timestamp"] = int(time.time() * 1000)
        params["recvWindow"] = RECV_WINDOW
    query = urllib.parse.urlencode(params)
    if secret is not None:
        query += "&signature=" + sign(secret, query)
    url = f"{REST}{path}" + (f"?{query}" if query else "")
    request = urllib.request.Request(url, method=method, headers={"X-MBX-APIKEY": key})
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            body = response.read().decode()
            status = response.status
    except urllib.error.HTTPError as error:
        body = error.read().decode()
        status = error.code
    try:
        return status, json.loads(body)
    except json.JSONDecodeError:
        return status, body[:200]


async def call(ws, method: str, params: dict | None = None) -> dict:
    """Send one WebSocket-API request and wait for its reply (events that
    arrive meanwhile are not lost: they are handed to `pending_events`)."""
    request_id = uuid.uuid4().hex
    await ws.send(json.dumps({"id": request_id, "method": method, "params": params or {}}))
    while True:
        message = json.loads(await asyncio.wait_for(ws.recv(), 20))
        if message.get("id") == request_id:
            return message
        pending_events.append(message)


pending_events: list[dict] = []


async def next_event(ws, wanted) -> dict | None:
    """The next pushed event for which `wanted(event_dict)` is true."""
    deadline = time.monotonic() + EVENT_TIMEOUT
    while True:
        for index, message in enumerate(pending_events):
            if wanted(message):
                return pending_events.pop(index)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        try:
            message = json.loads(await asyncio.wait_for(ws.recv(), remaining))
        except asyncio.TimeoutError:
            return None
        pending_events.append(message)


def event_of(message: dict) -> dict:
    """Pushed events arrive wrapped: {"subscriptionId": N, "event": {...}}."""
    return message.get("event", {}) if isinstance(message, dict) else {}


async def main() -> int:
    key = os.environ.get("APEXES_TESTNET_API_KEY", "").strip()
    secret = os.environ.get("APEXES_TESTNET_API_SECRET", "").strip()
    if not key or not secret:
        print(__doc__)
        print("\nAPEXES_TESTNET_API_KEY / APEXES_TESTNET_API_SECRET are not set.")
        return 2
    print(f"Testnet only: {REST} and {WS_API}")
    print(f"Key: ...{key[-4:]} (length {len(key)}); secret never printed\n")

    order_id = None
    try:
        print("1. WebSocket API reachable")
        async with websockets.connect(WS_API, open_timeout=20) as ws:
            reply = await call(ws, "time")
            record("public `time` request", reply.get("status") == 200,
                   f"status {reply.get('status')}")

            print("2. Keys valid (signed REST GET /api/v3/account)")
            status, account = rest("GET", "/api/v3/account", key, secret)
            if not record("account endpoint accepts the key", status == 200,
                          f"HTTP {status}" + ("" if status == 200 else f" {account}")):
                print("\nStopping: fix the keys first (testnet keys, HMAC type).")
                return 1
            balances = {b["asset"]: b["free"] for b in account.get("balances", [])
                        if float(b["free"]) > 0}
            print(f"     canTrade={account.get('canTrade')}  funded assets: "
                  f"{ {a: balances[a] for a in list(balances)[:6]} }")

            print("3. Signature-based user-data subscription")
            ts = int(time.time() * 1000)
            payload = f"apiKey={key}&recvWindow={RECV_WINDOW}&timestamp={ts}"
            reply = await call(ws, "userDataStream.subscribe.signature", {
                "apiKey": key, "recvWindow": RECV_WINDOW, "timestamp": ts,
                "signature": sign(secret, payload),
            })
            subscribed = reply.get("status") == 200
            record("subscribe.signature accepted", subscribed,
                   f"status {reply.get('status')} result={reply.get('result')} "
                   f"error={reply.get('error')}")
            if not subscribed:
                print("\nThe new API did not accept the subscription; see the error above.")
                return 1

            print("4. Order events arrive on the subscription")
            status, ticker = rest("GET", "/api/v3/ticker/price", key, None, {"symbol": SYMBOL})
            if status != 200:
                record("fetch current price", False, f"HTTP {status} {ticker}")
                return 1
            last = float(ticker["price"])
            price = f"{last * 0.8:.2f}"  # far below market: rests, never fills
            quantity = "0.001"
            print(f"     last price {last}; resting BUY {quantity} {SYMBOL} @ {price} (virtual funds)")
            status, order = rest("POST", "/api/v3/order", key, secret, {
                "symbol": SYMBOL, "side": "BUY", "type": "LIMIT", "timeInForce": "GTC",
                "quantity": quantity, "price": price, "newOrderRespType": "ACK",
            })
            if not record("order accepted by testnet", status == 200,
                          f"HTTP {status}" + ("" if status == 200 else f" {order}")):
                return 1
            order_id = order["orderId"]

            new_event = await next_event(
                ws, lambda m: event_of(m).get("e") == "executionReport"
                and event_of(m).get("i") == order_id and event_of(m).get("X") == "NEW")
            record("executionReport NEW received", new_event is not None,
                   "none within %ds" % EVENT_TIMEOUT if new_event is None else "")
            if new_event:
                e = event_of(new_event)
                print(f"     envelope keys: {sorted(new_event.keys())}")
                print(f"     event fields:  e={e.get('e')} s={e.get('s')} c={e.get('c')} "
                      f"i={e.get('i')} X={e.get('X')} x={e.get('x')} S={e.get('S')} "
                      f"q={e.get('q')} p={e.get('p')}")
                print(f"     subscriptionId={new_event.get('subscriptionId')}")

            status, cancelled = rest("DELETE", "/api/v3/order", key, secret,
                                     {"symbol": SYMBOL, "orderId": order_id})
            record("order cancelled", status == 200, f"HTTP {status}")
            if status == 200:
                order_id = None

            cancel_event = await next_event(
                ws, lambda m: event_of(m).get("e") == "executionReport"
                and event_of(m).get("X") == "CANCELED")
            record("executionReport CANCELED received", cancel_event is not None,
                   "none within %ds" % EVENT_TIMEOUT if cancel_event is None else "")
            if cancel_event:
                e = event_of(cancel_event)
                print(f"     event fields:  X={e.get('X')} x={e.get('x')} c={e.get('c')} "
                      f"C(orig client id)={e.get('C')}")

            other = sorted({event_of(m).get("e") for m in pending_events if event_of(m)})
            if other:
                print(f"     other event types seen: {other}")
    except Exception as error:  # report, then fall through to cleanup
        record("unexpected error", False, f"{type(error).__name__}: {error}")
    finally:
        if order_id is not None:
            status, _ = rest("DELETE", "/api/v3/order", key, secret,
                             {"symbol": SYMBOL, "orderId": order_id})
            print(f"  cleanup: cancelled leftover order {order_id} (HTTP {status})")

    print("\nSummary")
    for name, ok, _ in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    return 0 if results and all(ok for _, ok, _ in results) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
