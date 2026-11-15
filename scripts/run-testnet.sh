#!/usr/bin/env bash
# Starts everything needed for a Binance Spot TESTNET session and opens the
# Apexes operator app (launcher + console). Safe to run repeatedly.
#
#   scripts/run-testnet.sh            # check, build if needed, open the app
#   scripts/run-testnet.sh --check    # only check prerequisites, start nothing
#
# Reads your testnet HMAC key from ~/.apexes/binance.yaml:
#
#   testnet:
#     hmac-sha256:
#       api-key: ...
#       secret-key: ...
#
# The values are exported only into this script's own environment (for the app
# and the processes it starts) and are never printed.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG="${APEXES_BINANCE_CONFIG:-$HOME/.apexes/binance.yaml}"
CHECK_ONLY=0
[ "${1:-}" = "--check" ] && CHECK_ONLY=1

fail() { echo "ERROR: $*" >&2; exit 1; }
ok()   { echo "  ok  $*"; }

echo "Apexes testnet launcher"

# 1. Credentials ------------------------------------------------------------
[ -f "$CONFIG" ] || fail "$CONFIG not found (see USAGE.md, step 1)"
read_key() {
  python3 - "$CONFIG" "$1" <<'PY'
import sys, yaml
section = yaml.safe_load(open(sys.argv[1]))["testnet"]["hmac-sha256"]
print(str(section[sys.argv[2]]).strip())
PY
}
API_KEY="$(read_key api-key 2>/dev/null)"   || fail "no testnet.hmac-sha256.api-key in $CONFIG"
API_SECRET="$(read_key secret-key 2>/dev/null)" || fail "no testnet.hmac-sha256.secret-key in $CONFIG"
[ -n "$API_KEY" ] && [ -n "$API_SECRET" ] || fail "empty key or secret in $CONFIG"
ok "testnet HMAC key found (api-key ...${API_KEY: -4})"
MODE="$(stat -f '%Lp' "$CONFIG" 2>/dev/null || stat -c '%a' "$CONFIG")"
[ "$MODE" = "600" ] || echo "  WARN $CONFIG is mode $MODE; run: chmod 600 $CONFIG"

# 2. Postgres (the order store) ----------------------------------------------
if ! nc -z localhost 5432 2>/dev/null; then
  echo "  ..  Postgres not running; starting it with docker compose"
  command -v docker >/dev/null || fail "Postgres is not running and docker is not installed (USAGE.md, step 2)"
  docker info >/dev/null 2>&1 || fail "docker is not running (macOS with colima: 'colima start')"
  (cd "$ROOT" && APEXES_NETWORK=testnet docker compose up -d postgres) >/dev/null
  for _ in $(seq 1 30); do nc -z localhost 5432 2>/dev/null && break; sleep 1; done
  nc -z localhost 5432 2>/dev/null || fail "Postgres did not come up on localhost:5432"
fi
ok "Postgres reachable on localhost:5432"

# Live sessions keep their orders in their own database. The test suites
# TRUNCATE the shared `apexes` database and leave mock orders behind, which a
# live engine would adopt as its own open orders.
LIVE_DB="apexes_live"
if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx apexes-postgres; then
  if ! docker exec apexes-postgres psql -U apexes -d postgres -Atc "select 1 from pg_database where datname='$LIVE_DB'" | grep -q 1; then
    docker exec apexes-postgres createdb -U apexes "$LIVE_DB" || fail "could not create database $LIVE_DB"
    ok "created database $LIVE_DB"
  else
    ok "database $LIVE_DB present"
  fi
else
  echo "  WARN Postgres is not the apexes-postgres container; make sure database '$LIVE_DB' exists (createdb -U apexes $LIVE_DB)"
fi

# 3. Binance testnet reachable, and the clock is close enough ----------------
# The testnet's hostname resolves to several addresses and one of them can
# accept no connections. A plain HTTP client waits ~30 s on it, so try each
# address with a short timeout instead (the Rust client does the same).
SKEW="$(python3 - <<'PY'
import http.client, json, socket, ssl, time

HOST = "testnet.binance.vision"
ctx = ssl.create_default_context()
last = "no address answered"
try:
    addrs = sorted({a[4][0] for a in socket.getaddrinfo(HOST, 443, socket.AF_INET)})
except OSError as e:
    addrs = []
    last = f"cannot resolve {HOST}: {e}"
for ip in addrs:
    try:
        sock = socket.create_connection((ip, 443), timeout=4)
        tls = ctx.wrap_socket(sock, server_hostname=HOST)
        tls.settimeout(8)
        t0 = time.time()
        conn = http.client.HTTPSConnection(HOST, 443, timeout=8)
        conn.sock = tls
        conn.request("GET", "/api/v3/time")
        server = json.load(conn.getresponse())["serverTime"]
        t1 = time.time()
        print(int((t0 + t1) / 2 * 1000 - server))
        break
    except Exception as e:  # try the next address
        last = f"{ip}: {type(e).__name__}"
else:
    print("ERR", last)
PY
)"
case "$SKEW" in
  ERR*) fail "cannot reach https://testnet.binance.vision (${SKEW#ERR })" ;;
esac
ABS=${SKEW#-}
if [ "$ABS" -gt 4000 ]; then
  echo "  WARN your clock differs from Binance's by ${SKEW} ms; signed requests are rejected above ~5000 ms (-1021). Sync your clock."
else
  ok "Binance testnet reachable; clock offset ${SKEW} ms"
fi

# 4. Binaries ------------------------------------------------------------------
BIN="$ROOT/target/release"
need_build=0
for b in apexes-operator apexes-engine apexes-market-data-gateway; do
  [ -x "$BIN/$b" ] || need_build=1
done
if [ "$need_build" = 1 ]; then
  if [ "$CHECK_ONLY" = 1 ]; then
    echo "  ..  binaries not built yet (the real run builds them: a few minutes the first time)"
  else
    echo "  ..  building release binaries (first time takes a few minutes)"
    (cd "$ROOT" && cargo build --release -p apexes-engine -p apexes-market-data-gateway -p apexes-ui)
  fi
else
  ok "release binaries present"
fi
[ -f "$ROOT/deploy/strategy.example.drift" ] || fail "deploy/strategy.example.drift is missing"
ok "demo strategy present"

if [ "$CHECK_ONLY" = 1 ]; then
  echo "All checks done (nothing was started)."
  exit 0
fi

# 5. Run -------------------------------------------------------------------------
# The app resolves the engine and gateway next to itself, so it works from any
# directory; the strategy path in the launcher is relative to this directory.
cd "$ROOT"
export APEXES_CRED_BINANCE_MAIN_TESTNET_API_KEY="$API_KEY"
export APEXES_CRED_BINANCE_MAIN_TESTNET_API_SECRET="$API_SECRET"
echo "Starting the app. In the Launcher tab: leave Testnet selected, press 'Launch on TESTNET'."
exec "$BIN/apexes-operator"
