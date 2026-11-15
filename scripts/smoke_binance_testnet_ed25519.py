#!/usr/bin/env python3
"""Smoke test: Binance Spot TESTNET with an Ed25519 key held in ssh-agent.

The private key never leaves the agent. This script reads, from
~/.apexes/binance.yaml:

    testnet:
      ed25519:
        api-key: <Binance API key id registered for your public key>
        socket:  <path of the ssh-agent socket that holds the private key>

and checks what a live engine would rely on, with exactly ONE signature
requested from the agent (the WebSocket `session.logon`; every later request
on that authenticated session is unsigned):

  1. the agent is reachable and offers the key (default comment "apexes")
  2. the agent signs, and Binance accepts it            (session.logon)
  3. the session reports the account                    (account.status)
  4. user-data events arrive on this session            (userDataStream.subscribe)
  5. order events: a tiny resting BUY far below market, then cancelled

Testnet only (hard-coded), virtual funds only, never prints key material,
and cancels the order it placed even if something fails.

NOTE: the agent may ask you to approve the signature. This script waits up to
120 s for that and never kills or restarts the agent.

    python3 scripts/smoke_binance_testnet_ed25519.py
"""

import asyncio
import base64
import json
import os
import socket
import struct
import sys
import time
import uuid

try:
    import websockets
    import yaml
except ImportError as error:
    sys.exit(f"missing package ({error.name}): pip3 install websockets pyyaml")

WS_API = "wss://ws-api.testnet.binance.vision/ws-api/v3"  # testnet only
SYMBOL = "BTCUSDT"
EVENT_TIMEOUT = 20.0
SIGN_TIMEOUT = 120.0
CONFIG = os.path.expanduser("~/.apexes/binance.yaml")
KEY_COMMENT = os.environ.get("APEXES_AGENT_KEY_COMMENT", "apexes")

SSH_AGENTC_REQUEST_IDENTITIES = 11
SSH_AGENT_IDENTITIES_ANSWER = 12
SSH_AGENTC_SIGN_REQUEST = 13
SSH_AGENT_SIGN_RESPONSE = 14

results: list[tuple[str, bool]] = []
pending_events: list[dict] = []


def record(name: str, ok: bool, detail: str = "") -> bool:
    results.append((name, ok))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f": {detail}" if detail else ""))
    return ok


# ---- minimal ssh-agent client (RFC draft-miller-ssh-agent) -----------------

def _string(data: bytes) -> bytes:
    return struct.pack(">I", len(data)) + data


def _read_string(buf: bytes, offset: int) -> tuple[bytes, int]:
    (length,) = struct.unpack_from(">I", buf, offset)
    return buf[offset + 4: offset + 4 + length], offset + 4 + length


def agent_call(path: str, message_type: int, payload: bytes, timeout: float) -> tuple[int, bytes]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        sock.connect(path)
        body = bytes([message_type]) + payload
        sock.sendall(struct.pack(">I", len(body)) + body)

        def read_exact(n: int) -> bytes:
            data = b""
            while len(data) < n:
                chunk = sock.recv(n - len(data))
                if not chunk:
                    raise ConnectionError("agent closed the connection")
                data += chunk
            return data

        (length,) = struct.unpack(">I", read_exact(4))
        reply = read_exact(length)
        return reply[0], reply[1:]


def agent_identities(path: str) -> list[tuple[bytes, str]]:
    kind, body = agent_call(path, SSH_AGENTC_REQUEST_IDENTITIES, b"", 10)
    if kind != SSH_AGENT_IDENTITIES_ANSWER:
        raise RuntimeError(f"unexpected agent reply type {kind}")
    (count,) = struct.unpack_from(">I", body, 0)
    offset, found = 4, []
    for _ in range(count):
        blob, offset = _read_string(body, offset)
        comment, offset = _read_string(body, offset)
        found.append((blob, comment.decode(errors="replace")))
    return found


def agent_sign_ed25519(path: str, key_blob: bytes, data: bytes) -> bytes:
    """Returns the raw 64-byte Ed25519 signature of `data`."""
    payload = _string(key_blob) + _string(data) + struct.pack(">I", 0)
    kind, body = agent_call(path, SSH_AGENTC_SIGN_REQUEST, payload, SIGN_TIMEOUT)
    if kind != SSH_AGENT_SIGN_RESPONSE:
        raise RuntimeError(f"the agent refused to sign (reply type {kind})")
    signature_blob, _ = _read_string(body, 0)
    algorithm, offset = _read_string(signature_blob, 0)
    raw, _ = _read_string(signature_blob, offset)
    if algorithm != b"ssh-ed25519" or len(raw) != 64:
        raise RuntimeError(f"unexpected signature format {algorithm!r}/{len(raw)}")
    return raw


# ---- Binance WebSocket API ------------------------------------------------

async def call(ws, method: str, params: dict | None = None) -> dict:
    request_id = uuid.uuid4().hex
    await ws.send(json.dumps({"id": request_id, "method": method, "params": params or {}}))
    while True:
        message = json.loads(await asyncio.wait_for(ws.recv(), 20))
        if message.get("id") == request_id:
            return message
        pending_events.append(message)


def event_of(message: dict) -> dict:
    return message.get("event", {}) if isinstance(message, dict) else {}


async def next_event(ws, wanted) -> dict | None:
    deadline = time.monotonic() + EVENT_TIMEOUT
    while True:
        for index, message in enumerate(pending_events):
            if wanted(message):
                return pending_events.pop(index)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        try:
            pending_events.append(json.loads(await asyncio.wait_for(ws.recv(), remaining)))
        except asyncio.TimeoutError:
            return None


def now_ms() -> int:
    return int(time.time() * 1000)


def load_config() -> tuple[str, str]:
    with open(CONFIG) as handle:
        section = yaml.safe_load(handle)["testnet"]["ed25519"]
    return str(section["api-key"]).strip(), os.path.expanduser(str(section["socket"]).strip())


async def main() -> int:
    try:
        api_key, socket_path = load_config()
    except Exception as error:
        print(f"Cannot read {CONFIG}: {type(error).__name__}. Expected testnet.ed25519.api-key and .socket")
        return 2
    print(f"Testnet only: {WS_API}")
    print(f"API key ...{api_key[-4:]} (length {len(api_key)}); agent socket {socket_path}\n")

    print("1. ssh-agent reachable and holds the key")
    try:
        identities = agent_identities(socket_path)
    except Exception as error:
        record("agent reachable", False, f"{type(error).__name__}: {error}")
        return 1
    ed = [(blob, comment) for blob, comment in identities if blob[4:15] == b"ssh-ed25519"]
    chosen = [blob for blob, comment in ed if comment == KEY_COMMENT]
    record("agent lists the key", len(chosen) == 1,
           f"{len(ed)} ed25519 key(s) offered; wanted comment {KEY_COMMENT!r}"
           + ("" if len(chosen) == 1 else f" (comments: {[c for _, c in ed]})"))
    if len(chosen) != 1:
        print("\nSet APEXES_AGENT_KEY_COMMENT to the comment of the key registered with Binance.")
        return 1
    key_blob = chosen[0]

    order_id = None
    try:
        async with websockets.connect(WS_API, open_timeout=20) as ws:
            print("2. Agent signs, Binance accepts (session.logon): ONE signature")
            ts = now_ms()
            payload = f"apiKey={api_key}&timestamp={ts}"
            print("     asking the agent to sign (approve it if it prompts; waiting up to 120s)...")
            try:
                raw = await asyncio.get_running_loop().run_in_executor(
                    None, agent_sign_ed25519, socket_path, key_blob, payload.encode())
            except Exception as error:
                record("agent produced a signature", False, f"{type(error).__name__}: {error}")
                return 1
            record("agent produced a signature", True, "64 bytes")
            reply = await call(ws, "session.logon", {
                "apiKey": api_key, "timestamp": ts,
                "signature": base64.b64encode(raw).decode()})
            ok = reply.get("status") == 200
            record("session.logon accepted", ok,
                   f"status {reply.get('status')}" + ("" if ok else f" error={reply.get('error')}"))
            if not ok:
                print("\nBinance rejected the logon. Likely: the public key registered on testnet is not "
                      "the agent's key, or the api-key id is for a different key.")
                return 1

            print("3. Authenticated session works with no further signatures")
            reply = await call(ws, "account.status", {"timestamp": now_ms()})
            ok = reply.get("status") == 200
            record("account.status (unsigned, on the session)", ok,
                   f"status {reply.get('status')}" + ("" if ok else f" error={reply.get('error')}"))
            if ok:
                r = reply["result"]
                print(f"     canTrade={r.get('canTrade')} accountType={r.get('accountType')}")

            print("4. User-data subscription on the authenticated session")
            reply = await call(ws, "userDataStream.subscribe")
            if reply.get("status") != 200 and "apiKey" in json.dumps(reply.get("error", "")):
                reply = await call(ws, "userDataStream.subscribe", {"apiKey": api_key})
            subscribed = reply.get("status") == 200
            record("userDataStream.subscribe", subscribed,
                   f"status {reply.get('status')} result={reply.get('result')}"
                   + ("" if subscribed else f" error={reply.get('error')}"))
            if not subscribed:
                return 1

            print("5. Order events (resting BUY far below market, then cancel)")
            reply = await call(ws, "ticker.price", {"symbol": SYMBOL})
            last = float(reply["result"]["price"])
            price = f"{last * 0.8:.2f}"
            print(f"     last price {last}; BUY 0.001 {SYMBOL} @ {price} (virtual funds)")
            reply = await call(ws, "order.place", {
                "symbol": SYMBOL, "side": "BUY", "type": "LIMIT", "timeInForce": "GTC",
                "quantity": "0.001", "price": price, "newOrderRespType": "ACK",
                "timestamp": now_ms()})
            placed = reply.get("status") == 200
            record("order.place (unsigned, on the session)", placed,
                   f"status {reply.get('status')}" + ("" if placed else f" error={reply.get('error')}"))
            if not placed:
                return 1
            order_id = reply["result"]["orderId"]

            new_event = await next_event(
                ws, lambda m: event_of(m).get("e") == "executionReport"
                and event_of(m).get("i") == order_id and event_of(m).get("X") == "NEW")
            record("executionReport NEW received", new_event is not None)

            reply = await call(ws, "order.cancel", {
                "symbol": SYMBOL, "orderId": order_id, "timestamp": now_ms()})
            cancelled = reply.get("status") == 200
            record("order.cancel", cancelled, f"status {reply.get('status')}")
            if cancelled:
                order_id = None
            cancel_event = await next_event(
                ws, lambda m: event_of(m).get("e") == "executionReport"
                and event_of(m).get("X") == "CANCELED")
            record("executionReport CANCELED received", cancel_event is not None)

            if order_id is not None:  # cleanup on the same authenticated session
                reply = await call(ws, "order.cancel", {
                    "symbol": SYMBOL, "orderId": order_id, "timestamp": now_ms()})
                print(f"  cleanup: cancelled leftover order {order_id} (status {reply.get('status')})")
                order_id = None
    except Exception as error:
        record("unexpected error", False, f"{type(error).__name__}: {error}")

    print("\nSummary")
    for name, ok in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    return 0 if results and all(ok for _, ok in results) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
