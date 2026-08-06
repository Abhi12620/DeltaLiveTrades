import os
import time
import hmac
import hashlib
import json
import threading
from datetime import datetime, timezone

from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
try:
    import websocket  # pip install websocket-client
    _WS_AVAILABLE = True
except ImportError:
    _WS_AVAILABLE = False

# ---- shared HTTP session: reuses TCP+TLS connections to Delta across calls
# instead of paying a fresh handshake (~50-150ms) on every single request.
# With ~10-15 Delta API calls per order (mostly sequential by design), this
# is pure upside -- no behavior change, just less time spent connecting.
SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json"})

WS_URL = "wss://socket.india.delta.exchange"


class LiveOrderbookCache:
    """Keeps a small set of option symbols' L2 books live in memory via
    Delta's public orderbook WebSocket, so check_depth() at ORDER TIME can
    read a value that's already sitting in RAM instead of paying a fresh
    REST round-trip right when it matters most (the 3-4s the user is
    trying to cut down).

    Usage: call ensure_subscribed([symbols]) as EARLY as possible -- e.g.
    the moment the user opens the qty modal on the frontend, not when they
    click "place order" -- so by the time the order actually fires, the
    book has had time to arrive and update at least once. If the socket
    hasn't delivered anything fresh yet (cold subscribe, disconnect, etc.)
    callers must fall back to the REST snapshot -- this cache is a
    latency optimization, never a hard dependency."""

    MAX_SYMBOLS = 40          # small bounded set -- this is a pre-trade cache, not a market-data platform
    STALE_UNSUBSCRIBE_S = 600  # drop symbols nobody has asked about in 10 min

    def __init__(self):
        self._lock = threading.Lock()
        self._books = {}       # symbol -> {"buy": [...], "sell": [...], "ts": float}
        self._last_touched = {}  # symbol -> float (last time someone asked for it)
        self._subscribed = set()
        self._pending = set()   # requested but not yet sent (socket not open yet)
        self._ws = None
        self._ws_open = False
        self._started = False

    def start(self):
        if self._started or not _WS_AVAILABLE:
            return
        self._started = True
        threading.Thread(target=self._run_forever, daemon=True).start()
        threading.Thread(target=self._cleanup_loop, daemon=True).start()

    def ensure_subscribed(self, symbols):
        """Fire-and-forget: add symbols to the live feed. Safe to call
        repeatedly (e.g. every time a leg is selected in the UI) -- it's a
        no-op for symbols already subscribed."""
        if not _WS_AVAILABLE:
            return
        self.start()
        now = time.time()
        new_syms = []
        with self._lock:
            for s in symbols:
                if not s:
                    continue
                self._last_touched[s] = now
                if s not in self._subscribed and s not in self._pending:
                    self._pending.add(s)
                    new_syms.append(s)
        if new_syms and self._ws_open:
            self._send_subscribe(new_syms)

    def get(self, symbol, max_age_s=1.2):
        """Returns {'buy':[...], 'sell':[...]} if we have a fresh-enough
        live book for this symbol, else None (caller should fall back to
        REST)."""
        with self._lock:
            self._last_touched[symbol] = time.time()
            book = self._books.get(symbol)
        if not book:
            return None
        if time.time() - book["ts"] > max_age_s:
            return None
        return book

    def _send_subscribe(self, symbols):
        try:
            payload = {"type": "subscribe", "payload": {"channels": [
                {"name": "l2_orderbook", "symbols": symbols}
            ]}}
            self._ws.send(json.dumps(payload))
            with self._lock:
                self._pending -= set(symbols)
                self._subscribed |= set(symbols)
        except Exception:
            pass  # next reconnect will re-subscribe from _pending/_subscribed

    def _on_open(self, ws):
        self._ws_open = True
        with self._lock:
            all_syms = list(self._subscribed | self._pending)
        if all_syms:
            self._send_subscribe(all_syms)

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
        except Exception:
            return
        # be defensive about the exact envelope shape -- different Delta
        # feed versions/clients nest this slightly differently
        body = data.get("orderbook", data)
        if body.get("type") != "l2_orderbook" and data.get("type") != "l2_orderbook":
            return
        symbol = body.get("symbol") or data.get("symbol")
        buy = body.get("buy")
        sell = body.get("sell")
        if not symbol or buy is None or sell is None:
            return
        with self._lock:
            self._books[symbol] = {"buy": buy, "sell": sell, "ts": time.time()}

    def _on_error(self, ws, error):
        self._ws_open = False

    def _on_close(self, ws, *a):
        self._ws_open = False

    def _run_forever(self):
        backoff = 1
        while True:
            try:
                self._ws = websocket.WebSocketApp(
                    WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception:
                pass
            self._ws_open = False
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)

    def _cleanup_loop(self):
        """Drops symbols nobody has touched in a while so the subscription
        list (and memory) doesn't grow unbounded over a long-running
        process. Doesn't bother sending an unsubscribe -- just stops
        caring about the data -- simpler and Delta doesn't mind extra
        server-side subscriptions for a handful of symbols."""
        while True:
            time.sleep(60)
            now = time.time()
            with self._lock:
                stale = [s for s, t in self._last_touched.items()
                         if now - t > self.STALE_UNSUBSCRIBE_S]
                for s in stale:
                    self._last_touched.pop(s, None)
                    self._books.pop(s, None)
                    self._subscribed.discard(s)


ORDERBOOK_CACHE = LiveOrderbookCache()

app = Flask(__name__)
CORS(app)

BASE_URL = "https://api.india.delta.exchange"

# ---- set these as ENVIRONMENT VARIABLES in Render, never hardcode them here ----
# Two accounts supported so you can switch between them from the dashboard
# without a redeploy. Old single-account env vars (DELTA_API_KEY/SECRET) still
# work and are treated as "Account A" if the _A versions aren't set.
APP_SECRET = os.environ.get("APP_SECRET", "")  # shared secret between the dashboard and this backend

ACCOUNT_LABELS = {
    "A": os.environ.get("DELTA_ACCOUNT_A_LABEL", "Account A"),
    "B": os.environ.get("DELTA_ACCOUNT_B_LABEL", "Account B"),
}


def get_credentials(which):
    if which == "B":
        return os.environ.get("DELTA_API_KEY_B", ""), os.environ.get("DELTA_API_SECRET_B", "")
    return (
        os.environ.get("DELTA_API_KEY_A") or os.environ.get("DELTA_API_KEY", ""),
        os.environ.get("DELTA_API_SECRET_A") or os.environ.get("DELTA_API_SECRET", ""),
    )


def get_active_credentials():
    """Which account (A/B) is active is a SETTING (persisted, switchable
    from the dashboard, no redeploy) -- the actual key/secret values
    themselves always stay in Render env vars, never touch the dashboard."""
    return get_credentials(load_settings().get("active_account", "A"))

# ---- tunables: runtime-editable via /api/settings, persisted to a local
# JSON file so they survive without needing a Render redeploy. Env vars are
# only the FIRST-TIME defaults (used if the settings file doesn't exist yet).
FILL_POLL_ATTEMPTS = 6
FILL_POLL_DELAY = 0.15   # seconds between order-status polls (market orders resolve almost instantly)

SETTINGS_PATH = os.path.join(os.path.dirname(__file__), "settings.json")
_settings_lock = threading.Lock()
_DEFAULT_SETTINGS = {
    "max_lot_size": int(os.environ.get("MAX_LOT_SIZE", "500")),
    "depth_band_pct": float(os.environ.get("DEPTH_BAND_PCT", "1.0")),  # stored as a PERCENT (e.g. 1.0 = 1%)
    "inter_leg_delay_ms": int(os.environ.get("INTER_LEG_DELAY_MS", "0")),  # deliberate pause AFTER the buy leg confirms, BEFORE the sell leg fires (0 = fire as soon as ready)
    "active_account": "A",       # which Delta API key/secret pair is currently in use ("A" or "B")
    "dry_run": False,            # if true, every check (margin/depth) still runs for real, but NO real order is sent to Delta -- fully simulated response instead
    "kill_switch": False,        # if true, /api/place-spread refuses everything immediately, no exceptions
    "sell_leg_retries": 2,       # how many extra attempts for the sell leg before giving up
    "unwind_enabled": False,     # if true, a failed sell leg triggers an automatic opposite-side order to flatten the leftover buy leg. Off by default (per instruction) -- a naked long is a known, bounded-risk state; auto-unwinding trades that certainty for automatically closing the position at whatever price is available.
    "circuit_breaker_threshold": 3,   # consecutive FAILED trades before kill_switch auto-flips on
    "stale_data_max_seconds": 3.0,    # if an orderbook fetch takes longer than this, treat it as too stale/slow to trade on and refuse
    "notify_on_success": True,
    "notify_on_failure": True,
    "notify_on_naked_position": True,
    "notify_on_circuit_breaker": True,
    "consecutive_failures": 0,   # internal state, not meant to be edited directly -- tracked by the circuit breaker
}


def load_settings():
    with _settings_lock:
        try:
            with open(SETTINGS_PATH) as f:
                s = json.load(f)
            return {**_DEFAULT_SETTINGS, **s}
        except Exception:
            return dict(_DEFAULT_SETTINGS)


def save_settings(new_settings):
    with _settings_lock:
        with open(SETTINGS_PATH, "w") as f:
            json.dump(new_settings, f)


_product_cache = {}      # symbol -> {id, tick_size}
_cache_lock = threading.Lock()

AUDIT_LOG_PATH = os.path.join(os.path.dirname(__file__), "order_audit.log")
_audit_lock = threading.Lock()


def audit(event, **fields):
    """Append-only audit trail of every order attempt/result. Lives on
    Render's local (ephemeral) disk -- it survives normal operation but is
    wiped on a redeploy/instance replace. Good enough for same-session
    debugging; for permanent history, ship these lines to Supabase later."""
    entry = {"ts": datetime.now(timezone.utc).isoformat(), "event": event, **fields}
    line = json.dumps(entry, default=str)
    try:
        with _audit_lock, open(AUDIT_LOG_PATH, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass  # never let logging failure block a trade
    return entry


# ==================== Telegram notifications (backend-side) ====================
# Separate from the dashboard's own price-alert Telegram bot -- this one
# fires from the SERVER, so it works even if the dashboard tab is closed.
# Set these as Render env vars (create a bot via @BotFather, same pattern as
# the dashboard's alert bot -- can reuse the same bot/chat or use a new one).
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")


def send_telegram(text):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return False
    try:
        SESSION.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": text},
            timeout=8,
        )
        return True
    except Exception:
        return False


_NOTIFY_SETTING_KEY = {
    "success": "notify_on_success",
    "failure": "notify_on_failure",
    "naked_position": "notify_on_naked_position",
    "circuit_breaker": "notify_on_circuit_breaker",
}


def notify_trade_outcome(kind, message, settings=None):
    settings = settings or load_settings()
    key = _NOTIFY_SETTING_KEY.get(kind)
    if key and settings.get(key, True):
        send_telegram(message)


# ==================== Delta auth / request helpers ====================
def _sign(method, path, query, body, api_secret):
    ts = str(int(time.time()))
    payload = method + ts + path + query + body
    sig = hmac.new(api_secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return ts, sig


def _delta_get(path, params=None, signed=True):
    query = ""
    if params:
        query = "?" + "&".join(f"{k}={v}" for k, v in params.items())
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if signed:
        api_key, api_secret = get_active_credentials()
        ts, sig = _sign("GET", path, query, "", api_secret)
        headers.update({"api-key": api_key, "timestamp": ts, "signature": sig})
    r = SESSION.get(BASE_URL + path + query, headers=headers, timeout=10)
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, {"success": False, "error": r.text}


def _delta_post(path, body_dict):
    api_key, api_secret = get_active_credentials()
    body = json.dumps(body_dict)
    ts, sig = _sign("POST", path, "", body, api_secret)
    headers = {
        "api-key": api_key, "timestamp": ts, "signature": sig,
        "Content-Type": "application/json", "Accept": "application/json",
    }
    r = SESSION.post(BASE_URL + path, data=body, headers=headers, timeout=10)
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, {"success": False, "error": r.text}


def _delta_delete(path, body_dict):
    api_key, api_secret = get_active_credentials()
    body = json.dumps(body_dict)
    ts, sig = _sign("DELETE", path, "", body, api_secret)
    headers = {
        "api-key": api_key, "timestamp": ts, "signature": sig,
        "Content-Type": "application/json", "Accept": "application/json",
    }
    r = SESSION.delete(BASE_URL + path, data=body, headers=headers, timeout=10)
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, {"success": False, "error": r.text}


def get_usd_inr_rate():
    return 85.0


# ==================== product / ticker helpers ====================
def get_product(symbol):
    """Resolve a Delta option symbol to {id, tick_size}. Cached in memory."""
    with _cache_lock:
        if symbol in _product_cache:
            return _product_cache[symbol]
    res = SESSION.get(f"{BASE_URL}/v2/products/{symbol}",
                       headers={"Accept": "application/json"}, timeout=10)
    data = res.json()
    if not data.get("success"):
        raise ValueError(f"Could not resolve product for symbol {symbol}: {data}")
    r = data["result"]
    info = {"id": r["id"], "tick_size": float(r.get("tick_size") or 0.5)}
    with _cache_lock:
        _product_cache[symbol] = info
    return info


def get_orderbook(symbol, max_staleness_s=None):
    """Delta's L2 orderbook: {'buy': [{price, size}, ...], 'sell': [{price, size}, ...]}
    each already sorted best-price-first. Public endpoint, no auth needed.
    Also acts as a staleness guard: Delta doesn't expose a per-response
    timestamp, so as a practical proxy we measure how long the request
    itself took -- an unusually slow response (network hiccup, Delta-side
    lag) is a sign the data we'd be trading on can't be trusted as "now"."""
    if max_staleness_s is None:
        max_staleness_s = load_settings().get("stale_data_max_seconds", 3.0)
    t0 = time.time()
    res = SESSION.get(f"{BASE_URL}/v2/l2orderbook/{symbol}",
                       headers={"Accept": "application/json"}, timeout=10)
    elapsed = time.time() - t0
    if elapsed > max_staleness_s:
        raise ValueError(f"orderbook response took {elapsed:.1f}s (> {max_staleness_s}s limit) — too slow/stale to trade on safely")
    data = res.json()
    if not data.get("success"):
        raise ValueError(f"Could not fetch orderbook for {symbol}: {data}")
    return data["result"]


def check_depth(symbol, side, size, band_pct):
    """For a SELL order, liquidity comes from the book's BUY side (bids);
    for a BUY order, liquidity comes from the book's SELL side (asks).
    Sums size across price levels within band_pct of the best price on that
    side -- this band exists ONLY to decide "is there real depth nearby",
    it is never sent to Delta as a limit price. The order placed afterwards
    is a plain market order with no price restriction.

    PERFORMANCE: tries the live WebSocket orderbook cache FIRST (near-zero
    latency, already sitting in memory) and only falls back to a REST
    fetch if we don't have a fresh-enough live book yet. Either way it also
    makes sure the symbol is subscribed going forward, so a second call on
    the same symbol a moment later (e.g. the re-check right before firing
    the actual order) is much more likely to hit the cache."""
    book = ORDERBOOK_CACHE.get(symbol)
    source = "ws_cache"
    if book is None:
        ORDERBOOK_CACHE.ensure_subscribed([symbol])
        book = get_orderbook(symbol)
        source = "rest"
    levels = book.get("buy" if side == "sell" else "sell") or []
    if not levels:
        return {"available": 0, "best_price": None, "levels_checked": 0, "source": source}
    best_price = float(levels[0]["price"])
    if side == "sell":
        cutoff = best_price * (1 - band_pct)
        in_band = [l for l in levels if float(l["price"]) >= cutoff]
    else:
        cutoff = best_price * (1 + band_pct)
        in_band = [l for l in levels if float(l["price"]) <= cutoff]
    available = sum(float(l.get("size", 0)) for l in in_band)
    return {"available": available, "best_price": best_price, "levels_checked": len(in_band), "source": source}


def round_to_tick(price, tick_size):
    if not tick_size:
        return round(price, 2)
    return round(round(price / tick_size) * tick_size, 8)


# ==================== depth-checked market order placement ====================
def place_market_leg(symbol, side, size, band_pct=None, dry_run=False):
    """1) Checks L2 orderbook depth near the best price BEFORE placing anything
          -- if the book can't realistically absorb `size`, the order is never
          sent at all.
       2) If depth is sufficient, places a plain MARKET order (no limit price) --
          unless dry_run is True, in which case everything up to and including
          the depth check is real, but no order is actually sent to Delta; a
          simulated fill is returned instead (using the best available price).
       3) Polls the order status afterwards so the returned fill size/price are
          the REAL executed values, not an assumption (skipped in dry_run)."""
    if band_pct is None:
        band_pct = load_settings()["depth_band_pct"] / 100.0
    try:
        product = get_product(symbol)
    except Exception as e:
        return {"symbol": symbol, "ok": False, "filled_size": 0, "error": f"product lookup failed: {e}"}

    try:
        depth = check_depth(symbol, side, size, band_pct)
    except Exception as e:
        return {"symbol": symbol, "ok": False, "filled_size": 0, "error": f"orderbook lookup failed: {e}"}

    if depth["available"] < size:
        return {
            "symbol": symbol, "ok": False, "filled_size": 0,
            "error": (
                f"Insufficient orderbook depth: only {depth['available']:.0f} lots available "
                f"within {band_pct*100:.1f}% of best price ({depth['best_price']}), "
                f"but {size} lots requested. Order NOT placed."
            ),
            "depth_available": depth["available"], "depth_checked_pct": band_pct * 100,
        }

    if dry_run:
        leg_result = {
            "symbol": symbol, "ok": True, "order_id": "DRY_RUN", "dry_run": True,
            "requested_size": int(size), "filled_size": int(size), "unfilled_size": 0,
            "avg_price": depth["best_price"], "state": "simulated",
            "depth_available_precheck": depth["available"],
        }
        audit("dry_run_order", **leg_result)
        return leg_result

    body = {
        "product_id": product["id"],
        "size": int(size),
        "side": side,
        "order_type": "market_order",
    }
    audit("order_attempt", symbol=symbol, side=side, size=size, depth_available=depth["available"])
    status, data = _delta_post("/v2/orders", body)
    if not data.get("success"):
        audit("order_reject", symbol=symbol, side=side, response=data)
        return {"symbol": symbol, "ok": False, "filled_size": 0, "error": data.get("error"), "response": data}

    result = data.get("result", {})
    order_id = result.get("id")

    # reconcile: poll actual order state instead of trusting the initial ack
    # -- BUT check the ack itself first. Delta's POST /v2/orders response
    # for a market order often already comes back "closed"/"filled" with
    # size/unfilled_size populated (market orders resolve near-instantly),
    # so if that's already true we skip the poll loop's REST round-trips
    # entirely instead of unconditionally sleeping/polling at least once.
    state = result.get("state")
    filled_size = int(result.get("size", 0)) - int(result.get("unfilled_size", result.get("size", 0)))
    avg_price = result.get("average_fill_price")
    if state not in ("closed", "cancelled", "filled") or avg_price is None:
        for _ in range(FILL_POLL_ATTEMPTS):
            st, odata = _delta_get(f"/v2/orders/{order_id}")
            if odata.get("success"):
                r = odata.get("result", {})
                state = r.get("state")
                filled_size = int(r.get("size", 0)) - int(r.get("unfilled_size", r.get("size", 0)))
                avg_price = r.get("average_fill_price")
                if state in ("closed", "cancelled", "filled"):
                    break
            time.sleep(FILL_POLL_DELAY)

    ok = filled_size > 0
    leg_result = {
        "symbol": symbol, "ok": ok, "order_id": order_id,
        "requested_size": int(size), "filled_size": filled_size,
        "unfilled_size": int(size) - filled_size,
        "avg_price": avg_price, "state": state,
        "depth_available_precheck": depth["available"],
    }
    audit("order_result", **leg_result)
    return leg_result


def unwind_leg(symbol, opposite_side, size, band_pct, dry_run):
    """Optional (off by default -- toggle in Trade Params): fires an
    immediate opposite-side protected order to flatten a leg left dangling
    because its partner leg failed to fill. Uses a wider depth band since
    getting OUT matters more than getting a great price here."""
    audit("unwind_attempt", symbol=symbol, side=opposite_side, size=size)
    result = place_market_leg(symbol, opposite_side, size, band_pct=max(band_pct * 3, 0.03), dry_run=dry_run)
    audit("unwind_result", **result)
    return result


def get_position_size(symbol):
    """Current live position size for a symbol (0 if flat, None if lookup failed).
    Used for post-trade reconciliation -- confirms what we THINK happened
    (based on order fill responses) matches what Delta's position ledger
    actually shows."""
    underlying = None
    for u in ("BTC", "ETH", "XAUT"):
        if u in (symbol or ""):
            underlying = u
            break
    if not underlying:
        return None
    st, data = _delta_get("/v2/positions/margined", {"underlying_asset_symbol": underlying})
    if not data.get("success"):
        return None
    for p in data.get("result", []) or []:
        if (p.get("product_symbol") or p.get("symbol")) == symbol:
            try:
                return float(p.get("size") or 0)
            except (TypeError, ValueError):
                return None
    return 0.0  # no entry for this symbol = flat


def record_outcome(success):
    """Circuit breaker: tracks consecutive FAILED trades (real ones only --
    dry-run doesn't count, nothing actually happened). After N in a row
    (configurable), auto-flips the kill switch on and sends an alert, so a
    systemic problem (bad API key, Delta outage, etc.) can't silently keep
    failing trade after trade unattended."""
    s = load_settings()
    if success:
        if s.get("consecutive_failures", 0) != 0:
            s["consecutive_failures"] = 0
            save_settings(s)
        return
    s["consecutive_failures"] = s.get("consecutive_failures", 0) + 1
    threshold = s.get("circuit_breaker_threshold", 3)
    if s["consecutive_failures"] >= threshold and not s.get("kill_switch"):
        s["kill_switch"] = True
        save_settings(s)
        audit("circuit_breaker_tripped", consecutive_failures=s["consecutive_failures"], threshold=threshold)
        notify_trade_outcome(
            "circuit_breaker",
            f"🛑 Circuit breaker tripped: {s['consecutive_failures']} consecutive failed trades. "
            f"Kill switch auto-enabled — all live orders blocked until you turn it off in Trade Params.",
            settings=s,
        )
    else:
        save_settings(s)


# ==================== margin precheck ====================
def get_available_balance():
    st, data = _delta_get("/v2/wallet/balances")
    if not data.get("success"):
        return None, data.get("error")
    total_avail = 0.0
    for a in data.get("result", []):
        try:
            total_avail += float(a.get("available_balance") or 0)
        except (TypeError, ValueError):
            continue
    return total_avail, None


# ==================== routes ====================
@app.route("/api/health")
def health():
    settings = load_settings()
    active = settings.get("active_account", "A")
    key, secret = get_active_credentials()
    return jsonify({
        "ok": True,
        "delta_key_configured": bool(key and secret),
        "app_secret_configured": bool(APP_SECRET),
        "active_account": active,
        "active_account_label": ACCOUNT_LABELS.get(active, active),
        "kill_switch": settings.get("kill_switch", False),
        "dry_run": settings.get("dry_run", False),
        "telegram_configured": bool(TG_BOT_TOKEN and TG_CHAT_ID),
        "consecutive_failures": settings.get("consecutive_failures", 0),
    })


@app.route("/api/settings", methods=["GET", "POST"])
def settings_endpoint():
    if not APP_SECRET or request.headers.get("X-App-Secret") != APP_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    def accounts_info():
        info = {}
        for which in ("A", "B"):
            key, secret = get_credentials(which)
            info[which] = {"label": ACCOUNT_LABELS[which], "configured": bool(key and secret)}
        return info

    if request.method == "GET":
        return jsonify({"ok": True, "settings": load_settings(), "accounts": accounts_info()})

    data = request.get_json(force=True, silent=True) or {}
    current = load_settings()
    if "max_lot_size" in data:
        try:
            v = int(data["max_lot_size"])
            if v <= 0:
                raise ValueError()
            current["max_lot_size"] = v
        except (ValueError, TypeError):
            return jsonify({"ok": False, "error": "max_lot_size must be a positive integer"}), 400
    if "depth_band_pct" in data:
        try:
            v = float(data["depth_band_pct"])
            if v <= 0:
                raise ValueError()
            current["depth_band_pct"] = v
        except (ValueError, TypeError):
            return jsonify({"ok": False, "error": "depth_band_pct must be a positive number"}), 400
    if "inter_leg_delay_ms" in data:
        try:
            v = int(data["inter_leg_delay_ms"])
            if v < 0:
                raise ValueError()
            current["inter_leg_delay_ms"] = v
        except (ValueError, TypeError):
            return jsonify({"ok": False, "error": "inter_leg_delay_ms must be a non-negative integer"}), 400
    if "sell_leg_retries" in data:
        try:
            v = int(data["sell_leg_retries"])
            if v < 0:
                raise ValueError()
            current["sell_leg_retries"] = v
        except (ValueError, TypeError):
            return jsonify({"ok": False, "error": "sell_leg_retries must be a non-negative integer"}), 400
    if "dry_run" in data:
        current["dry_run"] = bool(data["dry_run"])
    if "kill_switch" in data:
        current["kill_switch"] = bool(data["kill_switch"])
        if not current["kill_switch"]:
            # manually turning the kill switch back off also resets the
            # circuit-breaker counter, so it doesn't instantly re-trip
            current["consecutive_failures"] = 0
    if "unwind_enabled" in data:
        current["unwind_enabled"] = bool(data["unwind_enabled"])
    if "circuit_breaker_threshold" in data:
        try:
            v = int(data["circuit_breaker_threshold"])
            if v <= 0:
                raise ValueError()
            current["circuit_breaker_threshold"] = v
        except (ValueError, TypeError):
            return jsonify({"ok": False, "error": "circuit_breaker_threshold must be a positive integer"}), 400
    if "stale_data_max_seconds" in data:
        try:
            v = float(data["stale_data_max_seconds"])
            if v <= 0:
                raise ValueError()
            current["stale_data_max_seconds"] = v
        except (ValueError, TypeError):
            return jsonify({"ok": False, "error": "stale_data_max_seconds must be a positive number"}), 400
    for k in ("notify_on_success", "notify_on_failure", "notify_on_naked_position", "notify_on_circuit_breaker"):
        if k in data:
            current[k] = bool(data[k])
    if "active_account" in data:
        which = data["active_account"]
        if which not in ("A", "B"):
            return jsonify({"ok": False, "error": "active_account must be 'A' or 'B'"}), 400
        key, secret = get_credentials(which)
        if not key or not secret:
            return jsonify({"ok": False, "error": f"Account {which} has no credentials configured on the server (DELTA_API_KEY_{which}/DELTA_API_SECRET_{which}) — refusing to switch to it."}), 400
        current["active_account"] = which

    save_settings(current)
    audit("settings_updated", settings=current)
    return jsonify({"ok": True, "settings": current, "accounts": accounts_info()})


@app.route("/api/order-log")
def order_log():
    if not APP_SECRET or request.headers.get("X-App-Secret") != APP_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    n = int(request.args.get("n", 100))
    try:
        with open(AUDIT_LOG_PATH) as f:
            lines = f.readlines()[-n:]
        entries = [json.loads(l) for l in lines]
    except FileNotFoundError:
        entries = []
    return jsonify({"ok": True, "entries": entries})


@app.route("/api/account-info", methods=["GET"])
def account_info():
    if not APP_SECRET or request.headers.get("X-App-Secret") != APP_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    active_key, active_secret = get_active_credentials()
    if not active_key or not active_secret:
        return jsonify({"ok": False, "error": "Delta API credentials not configured for the active account on the server"}), 500

    bal_status, bal_data = _delta_get("/v2/wallet/balances")
    balances = []
    net_equity_usd = None
    if bal_data.get("success"):
        for a in bal_data.get("result", []):
            try:
                bal = float(a.get("balance") or 0)
                avail = float(a.get("available_balance") or 0)
            except (TypeError, ValueError):
                bal, avail = 0, 0
            if bal == 0 and avail == 0:
                continue
            balances.append({
                "asset": a.get("asset_symbol"), "balance": bal, "available_balance": avail,
            })
        meta = bal_data.get("meta") or {}
        try:
            net_equity_usd = float(meta.get("net_equity")) if meta.get("net_equity") is not None else None
        except (TypeError, ValueError):
            net_equity_usd = None

    usd_inr = get_usd_inr_rate()
    net_equity_inr = (net_equity_usd * usd_inr) if (net_equity_usd is not None and usd_inr) else None

    margin_mode = None
    margin_mode_error = None
    for candidate_path in ("/v2/users/margin_mode", "/v2/profile"):
        st, data = _delta_get(candidate_path)
        if data.get("success"):
            result = data.get("result", {})
            mm = result.get("margin_mode") if isinstance(result, dict) else None
            if mm:
                margin_mode = mm
                break
        else:
            margin_mode_error = data.get("error")
    if not margin_mode:
        margin_mode = "unknown"

    # positions: Delta's underlying_asset_symbol filter was returning the
    # full position list regardless of value, causing duplicates when we
    # looped per-underlying -- so fetch once and dedupe by symbol to be safe
    # either way.
    positions_by_symbol = {}
    positions_error = None
    risk_by_underlying = {}  # e.g. "BTC" -> {margin, initial_margin, maintenance_margin, delta, theta, vega, gamma}

    def underlying_of(symbol):
        # option/future symbols look like "C-BTC-78000-280826" or "BTCUSD"
        for u in ("BTC", "ETH", "XAUT"):
            if u in (symbol or ""):
                return u
        return "OTHER"

    for underlying in ("BTC", "ETH", "XAUT"):
        st, data = _delta_get("/v2/positions/margined", {"underlying_asset_symbol": underlying})
        if data.get("success"):
            for p in data.get("result", []) or []:
                try:
                    size = float(p.get("size") or 0)
                except (TypeError, ValueError):
                    size = 0
                if size == 0:
                    continue
                sym = p.get("product_symbol") or p.get("symbol")
                margin_val = p.get("margin")
                positions_by_symbol[sym] = {
                    "symbol": sym, "size": size,
                    "entry_price": p.get("entry_price"), "mark_price": p.get("mark_price"),
                    "liquidation_price": p.get("liquidation_price"),
                    "unrealized_pnl": p.get("unrealized_pnl") or p.get("unrealized_cashflow"),
                    "margin": margin_val,
                }

                coin = underlying_of(sym)
                bucket = risk_by_underlying.setdefault(coin, {
                    "margin": 0.0, "initial_margin": 0.0, "maintenance_margin": 0.0,
                    "delta": 0.0, "theta": 0.0, "vega": 0.0, "gamma": 0.0,
                    "fields_available": {"initial_margin": False, "maintenance_margin": False, "greeks": False},
                })
                try:
                    bucket["margin"] += float(margin_val or 0)
                except (TypeError, ValueError):
                    pass
                # opportunistic: these exact field names aren't publicly confirmed
                # in Delta's docs, so only used if actually present
                if p.get("initial_margin") is not None:
                    try:
                        bucket["initial_margin"] += float(p["initial_margin"])
                        bucket["fields_available"]["initial_margin"] = True
                    except (TypeError, ValueError):
                        pass
                if p.get("maintenance_margin") is not None:
                    try:
                        bucket["maintenance_margin"] += float(p["maintenance_margin"])
                        bucket["fields_available"]["maintenance_margin"] = True
                    except (TypeError, ValueError):
                        pass

                # portfolio greeks: per-contract greeks come from the ticker,
                # scaled by this position's signed size
                try:
                    tst, tdata = _delta_get(f"/v2/tickers/{sym}", signed=False)
                    if tdata.get("success"):
                        g = (tdata.get("result") or {}).get("greeks") or {}
                        if g:
                            bucket["fields_available"]["greeks"] = True
                            for greek in ("delta", "theta", "vega", "gamma"):
                                try:
                                    bucket[greek] += float(g.get(greek) or 0) * size
                                except (TypeError, ValueError):
                                    pass
                except Exception:
                    pass
        elif positions_error is None:
            positions_error = data.get("error")

    return jsonify({
        "ok": True,
        "balances": balances,
        "net_equity_usd": net_equity_usd,
        "net_equity_inr": net_equity_inr,
        "usd_inr_rate": usd_inr,
        "margin_mode": margin_mode,
        "margin_mode_note": None if margin_mode != "unknown" else f"Could not confirm via API ({margin_mode_error}) — check Delta app: Portfolio tab.",
        "positions": list(positions_by_symbol.values()),
        "positions_note": positions_error,
        "risk_by_underlying": risk_by_underlying,
        "risk_note": "Best-effort reconstruction from position + ticker data — Delta's exact Initial/Maintenance Margin split isn't in the public API docs, so those two may show '—' if the position object doesn't expose them directly. Position Margin and Greeks are computed live.",
    })


@app.route("/api/warm-legs", methods=["POST"])
def warm_legs():
    """Call this the MOMENT leg symbols are known on the frontend (e.g. as
    soon as the qty modal opens) -- well before the user has even typed a
    size or clicked confirm. It starts the live orderbook WebSocket
    subscription for those symbols and warms the product-id cache in the
    background, so by the time the actual order fires, check_depth() can
    read an already-live book from memory instead of paying a fresh REST
    round-trip at the worst possible moment. Fire-and-forget: does not
    wait for the socket to actually deliver a snapshot, returns instantly
    either way, and is never on the critical path of placing an order --
    place-spread still works fine (just slightly slower, via REST
    fallback) even if this was never called."""
    if not APP_SECRET or request.headers.get("X-App-Secret") != APP_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    data = request.get_json(force=True, silent=True) or {}
    symbols = [s for s in (data.get("symbols") or []) if s]
    if not symbols:
        return jsonify({"ok": False, "error": "symbols required"}), 400
    ORDERBOOK_CACHE.ensure_subscribed(symbols)

    def _warm_products():
        for s in symbols:
            try:
                get_product(s)
            except Exception:
                pass
    threading.Thread(target=_warm_products, daemon=True).start()
    return jsonify({"ok": True, "warming": symbols, "ws_available": _WS_AVAILABLE})


def precheck_leg(symbol, side, size, band_pct):
    """Warms the product-id cache AND checks orderbook depth for one leg.
    Meant to be run in parallel (one thread per leg) so both legs' prechecks
    finish in max(t_buy, t_sell) instead of t_buy + t_sell, and so BOTH
    sides are known-fillable (or not) before either order is committed --
    catching a doomed trade upfront instead of discovering it only after
    the buy leg has already gone through. Also grabs the pre-trade position
    size (piggybacking on this same parallel window) for post-trade
    reconciliation later."""
    try:
        get_product(symbol)  # warms cache, used again during actual placement
        depth = check_depth(symbol, side, size, band_pct)
    except Exception as e:
        return {"symbol": symbol, "ok": False, "available": 0, "error": str(e), "position_before": None}
    ok = depth["available"] >= size
    position_before = get_position_size(symbol)
    return {"symbol": symbol, "ok": ok, "available": depth["available"], "best_price": depth["best_price"], "position_before": position_before}


@app.route("/api/place-spread", methods=["POST"])
def place_spread():
    if not APP_SECRET or request.headers.get("X-App-Secret") != APP_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    settings = load_settings()

    if settings.get("kill_switch"):
        audit("kill_switch_block")
        return jsonify({"ok": False, "error": "🛑 Kill switch is ON — all live orders are blocked. Turn it off in the dashboard's Trade Params panel to resume."}), 403

    active_key, active_secret = get_active_credentials()
    if not active_key or not active_secret:
        return jsonify({"ok": False, "error": f"Delta API credentials not configured for the active account ({ACCOUNT_LABELS.get(settings.get('active_account','A'))}) on the server"}), 500

    dry_run = bool(settings.get("dry_run"))

    data = request.get_json(force=True, silent=True) or {}
    leg1 = data.get("leg1")
    leg2 = data.get("leg2")
    ratio = float(data.get("ratio", 1))
    depth_band_pct = float(data.get("depth_band_pct", settings["depth_band_pct"] / 100.0))

    if not leg1 or not leg2:
        return jsonify({"ok": False, "error": "leg1 and leg2 are required"}), 400
    for leg in (leg1, leg2):
        if not leg.get("symbol") or leg.get("side") not in ("buy", "sell") or not leg.get("size"):
            return jsonify({"ok": False, "error": f"bad leg payload: {leg}"}), 400

    # normalise so buy_leg/sell_leg are clear regardless of which one the
    # frontend labelled "leg1"/"leg2"
    buy_leg = leg1 if leg1["side"] == "buy" else leg2
    sell_leg = leg2 if leg1["side"] == "buy" else leg1

    # fat-finger ceiling (server-side, independent of the frontend's own check)
    if buy_leg["size"] > settings["max_lot_size"] or sell_leg["size"] > settings["max_lot_size"]:
        return jsonify({"ok": False, "error": f"size exceeds server safety ceiling of {settings['max_lot_size']} lots — refusing to place. Change this in the dashboard's Trade Params panel."}), 400

    audit("spread_request", buy_leg=buy_leg, sell_leg=sell_leg, ratio=ratio)

    # ---- margin precheck (soft) ----
    # NOTE: this is NOT a precise margin calculator -- Delta's real margin
    # formula for multi-leg options portfolios is complex and not something
    # we recompute here. This only catches the obvious case of an empty/near
    # -empty wallet before risking a partial-fill situation.
    avail, bal_err = get_available_balance()
    if avail is not None and avail <= 0:
        audit("margin_precheck_block", available_balance=avail)
        if not dry_run:
            record_outcome(False)
            notify_trade_outcome("failure", "❌ Order blocked: available balance is ₹0/$0 — nothing was placed.", settings=settings)
        return jsonify({"ok": False, "error": "Available balance is ₹0/$0 (or could not be confirmed positive) — refusing to place any leg. Check your Delta wallet."}), 400

    # ---- parallel precheck (both legs at once) ----
    # Resolves product IDs (cache warm-up) and checks orderbook depth for
    # BOTH legs simultaneously, using each leg's requested/nominal size.
    # If either side clearly can't be filled, we abort here -- before firing
    # even the buy leg -- instead of discovering the sell side is doomed
    # only after already taking the buy position.
    precheck_results = [None, None]

    def run_precheck(i, leg):
        precheck_results[i] = precheck_leg(leg["symbol"], leg["side"], leg["size"], depth_band_pct)

    t1 = threading.Thread(target=run_precheck, args=(0, buy_leg))
    t2 = threading.Thread(target=run_precheck, args=(1, sell_leg))
    t1.start(); t2.start()
    t1.join(timeout=15); t2.join(timeout=15)
    buy_precheck, sell_precheck = precheck_results[0], precheck_results[1]

    if not (buy_precheck and buy_precheck["ok"]) or not (sell_precheck and sell_precheck["ok"]):
        audit("precheck_fail", buy_precheck=buy_precheck, sell_precheck=sell_precheck)
        problems = []
        if not (buy_precheck and buy_precheck["ok"]):
            problems.append(f"BUY {buy_leg['symbol']}: only {buy_precheck['available']:.0f} lots depth available (need {buy_leg['size']})" if buy_precheck else f"BUY precheck failed ({buy_precheck.get('error') if buy_precheck else 'unknown'})")
        if not (sell_precheck and sell_precheck["ok"]):
            problems.append(f"SELL {sell_leg['symbol']}: only {sell_precheck['available']:.0f} lots depth available (need {sell_leg['size']})" if sell_precheck else f"SELL precheck failed ({sell_precheck.get('error') if sell_precheck else 'unknown'})")
        msg = "Insufficient orderbook depth (or stale data) on at least one leg — nothing was placed. " + " · ".join(problems)
        if not dry_run:
            record_outcome(False)
            notify_trade_outcome("failure", f"❌ Order precheck failed: {msg}", settings=settings)
        return jsonify({"ok": False, "leg1": None, "leg2": None, "dry_run": dry_run, "error": msg})

    # ---- leg 1: BUY fires FIRST (uses less/no margin) ----
    # If this fails, nothing has happened yet -- clean abort, nothing to
    # unwind. If leg 2 (sell) later fails, the leftover position is a
    # bounded-risk naked LONG (worst case: lose the premium already paid),
    # not an uncovered short -- the safer failure mode to be left holding.
    buy_result = place_market_leg(buy_leg["symbol"], "buy", buy_leg["size"], depth_band_pct, dry_run=dry_run)
    if not buy_result["ok"]:
        msg = "Buy leg did not fill (or insufficient orderbook depth) — no position taken, sell leg was not attempted."
        if not dry_run:
            record_outcome(False)
            notify_trade_outcome("failure", f"❌ Buy leg failed, nothing placed: {buy_leg['symbol']} — {buy_result.get('error','')}", settings=settings)
        return jsonify({"ok": False, "leg1": buy_result, "leg2": None, "dry_run": dry_run, "error": msg})

    # size the sell leg proportionally to whatever fraction of the buy
    # actually filled (handles partial fills cleanly, keeps the ratio intact)
    fill_fraction = buy_result["filled_size"] / buy_leg["size"]
    sell_size = max(1, round(sell_leg["size"] * fill_fraction))

    # optional deliberate pause between legs, entirely user-controlled via
    # the Trade Params panel (0 = fire the sell leg immediately, default)
    inter_leg_delay_ms = settings.get("inter_leg_delay_ms", 0)
    if inter_leg_delay_ms > 0:
        audit("inter_leg_delay", delay_ms=inter_leg_delay_ms)
        time.sleep(inter_leg_delay_ms / 1000.0)

    # ---- leg 2: SELL fires SECOND, with retries (margin-gated side) ----
    sell_result = None
    for attempt in range(settings.get("sell_leg_retries", 2) + 1):
        sell_result = place_market_leg(sell_leg["symbol"], "sell", sell_size, depth_band_pct, dry_run=dry_run)
        if sell_result["ok"]:
            break

    if not sell_result or sell_result["filled_size"] == 0:
        # Sell leg is dead -- buy leg (bounded-risk naked long) is left
        # standing. Auto-unwind is OPT-IN (Trade Params toggle, off by
        # default): if enabled, fire an immediate opposite-side order to
        # flatten it; either way, this gets its own urgent notification
        # channel distinct from generic failures.
        unwind_result = None
        if settings.get("unwind_enabled") and not dry_run:
            unwind_result = unwind_leg(buy_leg["symbol"], "sell", buy_result["filled_size"], depth_band_pct, dry_run)

        if unwind_result and unwind_result.get("ok") and unwind_result.get("filled_size", 0) >= buy_result["filled_size"]:
            msg = f"Sell leg failed after retries, but auto-unwind flattened the buy leg ({buy_result['filled_size']} lots on {buy_leg['symbol']}). No net position taken."
            notify_kind, notify_msg = "naked_position", f"⚠️ Sell leg failed but auto-unwound successfully — {buy_leg['symbol']} flattened, no leftover position."
        else:
            msg = (
                f"Sell leg failed after retries (insufficient margin, thin book, or rejected). "
                f"You have a naked LONG position of {buy_result['filled_size']} lots on {buy_leg['symbol']} left over from the buy leg. "
                + ("Auto-unwind was attempted but did not fully succeed — check Delta NOW." if settings.get("unwind_enabled") else "Auto-unwind is OFF (enable in Trade Params if you want this handled automatically) — review manually.")
            ) if not dry_run else "Sell leg simulation failed (insufficient depth) after retries. (dry run — no real position was taken on either leg.)"
            notify_kind, notify_msg = "naked_position", f"🚨 NAKED POSITION: sell leg failed, {buy_result['filled_size']} lots of {buy_leg['symbol']} left uncovered. {'Unwind attempted, check Delta.' if settings.get('unwind_enabled') else 'Auto-unwind is OFF — action needed.'}"

        if not dry_run:
            record_outcome(False)
            notify_trade_outcome(notify_kind, notify_msg, settings=settings)
        return jsonify({"ok": False, "leg1": buy_result, "leg2": sell_result, "unwind": unwind_result, "dry_run": dry_run, "error": msg})

    # ---- post-trade reconciliation ----
    # Confirms Delta's own position ledger agrees with what the fill
    # responses told us -- catches any silent mismatch (race condition,
    # partial data, etc.) instead of just trusting the order responses blindly.
    reconciliation_notes = []
    if not dry_run:
        buy_pos_after = get_position_size(buy_leg["symbol"])
        sell_pos_after = get_position_size(sell_leg["symbol"])
        buy_pos_before = (buy_precheck or {}).get("position_before")
        sell_pos_before = (sell_precheck or {}).get("position_before")
        if buy_pos_before is not None and buy_pos_after is not None:
            expected = buy_pos_before + buy_result["filled_size"]
            if abs(buy_pos_after - expected) > 0.01:
                reconciliation_notes.append(f"{buy_leg['symbol']}: expected position {expected}, Delta shows {buy_pos_after}")
        if sell_pos_before is not None and sell_pos_after is not None:
            expected = sell_pos_before - sell_result["filled_size"]
            if abs(sell_pos_after - expected) > 0.01:
                reconciliation_notes.append(f"{sell_leg['symbol']}: expected position {expected}, Delta shows {sell_pos_after}")
        if reconciliation_notes:
            audit("reconciliation_mismatch", notes=reconciliation_notes)

    reconciliation_warning = ("Reconciliation mismatch — " + " · ".join(reconciliation_notes)) if reconciliation_notes else None

    if sell_result["filled_size"] < sell_size:
        warning = f"Sell leg partially filled ({sell_result['filled_size']}/{sell_size}); ratio vs the buy leg is now mismatched — review positions manually."
        if reconciliation_warning:
            warning += " " + reconciliation_warning
        if not dry_run:
            record_outcome(True)  # order executed, just not at full size -- not a "failure" for circuit-breaker purposes
            notify_trade_outcome("success", f"⚠️ Spread executed with partial sell fill: {buy_leg['symbol']} x{buy_result['filled_size']} / {sell_leg['symbol']} x{sell_result['filled_size']}. {warning}", settings=settings)
        return jsonify({"ok": True, "leg1": buy_result, "leg2": sell_result, "dry_run": dry_run, "warning": warning})

    if not dry_run:
        record_outcome(True)
        notify_trade_outcome("success", f"✅ Spread executed: BUY {buy_leg['symbol']} x{buy_result['filled_size']} @ {buy_result['avg_price']} / SELL {sell_leg['symbol']} x{sell_result['filled_size']} @ {sell_result['avg_price']}", settings=settings)
    return jsonify({"ok": True, "leg1": buy_result, "leg2": sell_result, "warning": reconciliation_warning, "dry_run": dry_run})


# ==================== keep-alive (best-effort, use UptimeRobot as primary) ====================
def _self_ping_loop():
    url = os.environ.get("RENDER_EXTERNAL_URL")
    if not url:
        return
    while True:
        time.sleep(240)  # ~4 min, under Render's 15 min inactivity sleep threshold
        try:
            requests.get(url.rstrip("/") + "/api/health", timeout=8)
        except Exception:
            pass


threading.Thread(target=_self_ping_loop, daemon=True).start()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
