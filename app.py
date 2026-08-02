import os
import time
import hmac
import hashlib
import json
import threading

from flask import Flask, request, jsonify
from flask_cors import CORS
import requests

app = Flask(__name__)
CORS(app)  # tighten to your GitHub Pages origin once live, if you want (see note at bottom)

BASE_URL = "https://api.india.delta.exchange"

# ---- set these as ENVIRONMENT VARIABLES in Render, never hardcode them here ----
API_KEY = os.environ.get("DELTA_API_KEY", "")
API_SECRET = os.environ.get("DELTA_API_SECRET", "")
APP_SECRET = os.environ.get("APP_SECRET", "")  # shared secret between the dashboard and this backend

_product_cache = {}
_cache_lock = threading.Lock()


def _sign(method, path, query, body):
    ts = str(int(time.time()))
    payload = method + ts + path + query + body
    sig = hmac.new(API_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return ts, sig


def _delta_get(path, params=None):
    query = ""
    if params:
        query = "?" + "&".join(f"{k}={v}" for k, v in params.items())
    ts, sig = _sign("GET", path, query, "")
    headers = {
        "api-key": API_KEY,
        "timestamp": ts,
        "signature": sig,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    r = requests.get(BASE_URL + path + query, headers=headers, timeout=10)
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, {"success": False, "error": r.text}


_fx_cache = {"rate": None, "ts": 0}


def get_usd_inr_rate():
    """Cached ~10min: USD->INR rate from a free, keyless FX API. This is an
    approximate ECB-based reference rate, not Delta's own crypto-pair rate,
    so treat the INR figure as indicative rather than exact."""
    now = time.time()
    if _fx_cache["rate"] and (now - _fx_cache["ts"]) < 600:
        return _fx_cache["rate"]
    try:
        r = requests.get("https://api.frankfurter.app/latest?from=USD&to=INR", timeout=8)
        rate = r.json()["rates"]["INR"]
        _fx_cache["rate"] = rate
        _fx_cache["ts"] = now
        return rate
    except Exception:
        return _fx_cache["rate"]  # may be None if never fetched successfully


def _delta_post(path, body_dict):
    body = json.dumps(body_dict)
    ts, sig = _sign("POST", path, "", body)
    headers = {
        "api-key": API_KEY,
        "timestamp": ts,
        "signature": sig,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    r = requests.post(BASE_URL + path, data=body, headers=headers, timeout=10)
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, {"success": False, "error": r.text}


def get_product_id(symbol):
    """Resolve a Delta option symbol (e.g. 'C-BTC-70000-280826') to its numeric
    product_id, which the order endpoint requires. Cached in memory so repeat
    trades on the same strike don't re-fetch every time."""
    with _cache_lock:
        if symbol in _product_cache:
            return _product_cache[symbol]
    res = requests.get(
        f"{BASE_URL}/v2/products/{symbol}",
        headers={"Accept": "application/json"},
        timeout=10,
    )
    data = res.json()
    if not data.get("success"):
        raise ValueError(f"Could not resolve product for symbol {symbol}: {data}")
    pid = data["result"]["id"]
    with _cache_lock:
        _product_cache[symbol] = pid
    return pid


def place_market_order(symbol, side, size):
    try:
        product_id = get_product_id(symbol)
    except Exception as e:
        return {"symbol": symbol, "ok": False, "error": str(e)}
    body = {
        "product_id": product_id,
        "size": int(size),
        "side": side,          # "buy" or "sell"
        "order_type": "market_order",
    }
    status, data = _delta_post("/v2/orders", body)
    ok = bool(data.get("success"))
    return {"symbol": symbol, "ok": ok, "status": status, "response": data}


@app.route("/api/health")
def health():
    return jsonify({
        "ok": True,
        "delta_key_configured": bool(API_KEY and API_SECRET),
        "app_secret_configured": bool(APP_SECRET),
    })


@app.route("/api/account-info", methods=["GET"])
def account_info():
    if not APP_SECRET or request.headers.get("X-App-Secret") != APP_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    if not API_KEY or not API_SECRET:
        return jsonify({"ok": False, "error": "Delta API credentials not configured on server"}), 500

    # ---- balances ----
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
                "asset": a.get("asset_symbol"),
                "balance": bal,
                "available_balance": avail,
                "blocked_margin": a.get("blocked_margin"),
                "position_margin": a.get("position_margin"),
                "order_margin": a.get("order_margin"),
            })
        meta = bal_data.get("meta") or {}
        try:
            net_equity_usd = float(meta.get("net_equity")) if meta.get("net_equity") is not None else None
        except (TypeError, ValueError):
            net_equity_usd = None

    usd_inr = get_usd_inr_rate()
    net_equity_inr = (net_equity_usd * usd_inr) if (net_equity_usd is not None and usd_inr) else None

    # ---- margin mode ----
    # Best-effort: exact path isn't fully confirmed from public docs. If this
    # 404s / errors, we report "unknown" instead of guessing, and surface the
    # raw error so it can be corrected quickly.
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

    # ---- positions (across the underlyings this dashboard trades) ----
    positions = []
    positions_error = None
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
                positions.append({
                    "symbol": p.get("product_symbol") or p.get("symbol"),
                    "size": size,
                    "entry_price": p.get("entry_price"),
                    "mark_price": p.get("mark_price"),
                    "liquidation_price": p.get("liquidation_price"),
                    "unrealized_pnl": p.get("unrealized_pnl") or p.get("unrealized_cashflow"),
                    "margin": p.get("margin"),
                })
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
        "positions": positions,
        "positions_note": positions_error,
    })


@app.route("/api/place-spread", methods=["POST"])
def place_spread():
    # simple shared-secret gate so random people who find this URL can't fire
    # orders on your account -- this is NOT the Delta key, it's your own passphrase
    if not APP_SECRET or request.headers.get("X-App-Secret") != APP_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    if not API_KEY or not API_SECRET:
        return jsonify({"ok": False, "error": "Delta API credentials not configured on server"}), 500

    data = request.get_json(force=True, silent=True) or {}
    leg1 = data.get("leg1")
    leg2 = data.get("leg2")
    if not leg1 or not leg2:
        return jsonify({"ok": False, "error": "leg1 and leg2 are required"}), 400
    for leg in (leg1, leg2):
        if not leg.get("symbol") or leg.get("side") not in ("buy", "sell") or not leg.get("size"):
            return jsonify({"ok": False, "error": f"bad leg payload: {leg}"}), 400

    results = [None, None]

    def run(i, leg):
        results[i] = place_market_order(leg["symbol"], leg["side"], leg["size"])

    t1 = threading.Thread(target=run, args=(0, leg1))
    t2 = threading.Thread(target=run, args=(1, leg2))
    t1.start()
    t2.start()
    t1.join(timeout=20)
    t2.join(timeout=20)

    leg1_ok = bool(results[0] and results[0].get("ok"))
    leg2_ok = bool(results[1] and results[1].get("ok"))

    return jsonify({
        "ok": leg1_ok and leg2_ok,
        "leg1": results[0],
        "leg2": results[1],
        "warning": None if (leg1_ok and leg2_ok) else "One leg may have failed while the other filled — check your Delta positions immediately.",
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
