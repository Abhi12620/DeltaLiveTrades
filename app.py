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
