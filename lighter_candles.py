"""
lighter_candles.py - baut eigene 1s-Kerzen aus dem Lighter-Trade-Stream (trade/{market}).
Lighter selbst liefert keine Sekunden-Kerzen. Nur fuer Coins in LIGHTER_1S_COINS
(Env, Komma-getrennt, Standard "HYPE") - alle anderen Coins bleiben bei Binance.
Eigene WS-Verbindung (beeinflusst trading_loop nicht), Snapshot in Redis (ueberlebt Deploys,
die Luecke waehrend des Neustarts bleibt aber leer - sie laesst sich nicht nachholen).
"""
import asyncio, base64, json, os, time, zlib
import aiohttp
import websockets
from bot_core import debug_log, BASE_URL, WS_URL, MARKET_INDICES, get_redis

COINS = [c.strip().upper() for c in os.environ.get("LIGHTER_1S_COINS", "HYPE,XAU,XAG,LIT,WTI").split(",") if c.strip() in MARKET_INDICES or c.strip().upper() in MARKET_INDICES]
MAX_CANDLES = 4000
SNAP_EVERY = 60
SNAP_TTL = 3 * 86400
STALE_AFTER = 120  # s ohne Nachricht -> reconnect

_candles = {}   # sym -> {ts_ms: dict}
_last_msg = {}  # sym -> time
_seen = {}      # sym -> set(trade_id) (Dedupe, Snapshot beim Subscribe)
_info = {}      # sym -> {"trades": n, "since": ts}


def _key(sym):
    return f"lightercandles:{sym}:1s"


def _add_trade(sym, price, size, ts_ms, taker_buy):
    sec = ts_ms // 1000 * 1000
    d = _candles.setdefault(sym, {})
    c = d.get(sec)
    if c is None:
        d[sec] = {"ts": sec, "o": price, "h": price, "l": price, "c": price, "v": size,
                  "tb": size if taker_buy else 0.0, "n": 1}
        if len(d) > MAX_CANDLES + 100:
            for k in sorted(d)[:len(d) - MAX_CANDLES]:
                del d[k]
    else:
        c["h"] = max(c["h"], price); c["l"] = min(c["l"], price); c["c"] = price
        c["v"] += size; c["n"] += 1
        if taker_buy:
            c["tb"] += size


def handle_trades(sym, trades):
    seen = _seen.setdefault(sym, set())
    n = 0
    for t in sorted(trades, key=lambda x: x.get("timestamp", 0)):
        try:
            tid = t.get("trade_id", t.get("tx_hash"))
            if tid is not None:
                if tid in seen:
                    continue
                seen.add(tid)
                if len(seen) > 5000:
                    seen.clear()
            ts = int(t["timestamp"])
            if ts < 10**12:  # Sekunden statt ms
                ts *= 1000
            # is_maker_ask=True -> Maker war Verkaeufer -> Taker kauft
            taker_buy = bool(t.get("is_maker_ask")) if "is_maker_ask" in t else (t.get("type") != "sell")
            _add_trade(sym, float(t["price"]), float(t["size"]), ts, taker_buy)
            n += 1
        except Exception:
            continue
    if n:
        _last_msg[sym] = time.time()
        i = _info.setdefault(sym, {"trades": 0, "since": time.time()})
        i["trades"] += n


def get(sym, back_seconds):
    """Liste von Kerzen-Dicts (wie binance_ws.get_cached_candles_ext) oder None, wenn (noch) nichts da ist."""
    d = _candles.get(sym)
    if not d:
        return None
    cutoff = (time.time() - back_seconds) * 1000
    out = [dict(d[k]) for k in sorted(d) if k >= cutoff]
    return out or None


def status(sym):
    d = _candles.get(sym) or {}
    return {"candles": len(d), "last_msg_age": round(time.time() - _last_msg[sym], 1) if sym in _last_msg else None,
            **(_info.get(sym) or {})}


async def _save():
    r = await get_redis()
    if not r:
        return
    for sym in COINS:
        d = _candles.get(sym)
        if not d or len(d) < 3:
            continue
        rows = [[c["ts"], c["o"], c["h"], c["l"], c["c"], c["v"], c["tb"], c["n"]] for c in (d[k] for k in sorted(d)[-MAX_CANDLES:])]
        blob = base64.b64encode(zlib.compress(json.dumps(rows).encode())).decode()
        try:
            await r.set(_key(sym), blob, ex=SNAP_TTL)
        except Exception as e:
            debug_log(f"⚠️ [{sym}] Lighter-1s-Snapshot speichern fehlgeschlagen", {"error": str(e)})


async def _load():
    r = await get_redis()
    if not r:
        return
    for sym in COINS:
        try:
            blob = await r.get(_key(sym))
            if not blob:
                continue
            rows = json.loads(zlib.decompress(base64.b64decode(blob)))
            d = _candles.setdefault(sym, {})
            for ts, o, h, l, c, v, tb, n in rows:
                d.setdefault(int(ts), {"ts": int(ts), "o": o, "h": h, "l": l, "c": c, "v": v, "tb": tb, "n": n})
            debug_log(f"📦 [{sym}] {len(rows)} Lighter-1s-Kerzen aus Redis geladen")
        except Exception as e:
            debug_log(f"⚠️ [{sym}] Lighter-1s-Snapshot laden fehlgeschlagen", {"error": str(e)})


async def _snap_loop():
    while True:
        await asyncio.sleep(SNAP_EVERY)
        await _save()


async def _stream_loop():
    idx_to_sym = {MARKET_INDICES[s]: s for s in COINS}
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20) as ws:
                for s in COINS:
                    await ws.send(json.dumps({"type": "subscribe", "channel": f"trade/{MARKET_INDICES[s]}"}))
                debug_log(f"✅ Lighter-Trades für 1s-Kerzen verbunden: {', '.join(COINS)}")
                while True:
                    raw = await asyncio.wait_for(ws.recv(), timeout=STALE_AFTER)
                    msg = json.loads(raw)
                    ch = msg.get("channel", "")
                    try:
                        mi = int(ch.split(":")[1].split("/")[0]) if ":" in ch else int(ch.split("/")[1])
                    except Exception:
                        continue
                    sym = idx_to_sym.get(mi)
                    if sym and ch.startswith("trade") and msg.get("trades"):
                        handle_trades(sym, msg["trades"])
        except Exception as e:
            debug_log("⚠️ Lighter-1s-Stream getrennt, reconnect in 5s", {"error": str(e)})
            await asyncio.sleep(5)


async def lighter_candles_loop():
    if not COINS:
        return
    await _load()
    await asyncio.gather(_stream_loop(), _snap_loop(), _rest_loop())


# ---------------------------------------------------------------------------
# 1m / 5m / 15m: native Lighter-REST-Kerzen (max. 500 pro Abruf), das letzte Stueck aus dem Live-Stream
# ---------------------------------------------------------------------------
REST_TFS = {"1m": 60, "5m": 300, "15m": 900}
REST_EVERY = {"1m": 20, "5m": 60, "15m": 60}
_rest = {}  # (sym, tf) -> [candle dicts]


def _f(x, default=None):
    try:
        return float(x)
    except Exception:
        return default


def _parse_rest(rows, tf_s):
    out = []
    prev = None
    for r in rows or []:
        t = r.get("t", r.get("timestamp"))
        if t is None:
            continue
        t = int(t)
        if t < 10**12:
            t *= 1000
        c = _f(r.get("c", r.get("close")))
        o = _f(r.get("o", r.get("open")), c)
        h = _f(r.get("h", r.get("high")))
        l = _f(r.get("l", r.get("low")))
        if c is None:
            c = o if o is not None else prev
        if c is None:
            continue
        o = c if o is None else o
        h = max(o, c) if h is None else h
        l = min(o, c) if l is None else l
        v = _f(r.get("v", r.get("V", r.get("volume"))), 0.0) or 0.0
        out.append({"ts": t // (tf_s * 1000) * (tf_s * 1000), "o": o, "h": h, "l": l, "c": c, "v": v, "tb": v * 0.5, "n": 0})
        prev = c
    out.sort(key=lambda x: x["ts"])
    return out


async def _fetch_rest(session, sym, tf):
    tf_s = REST_TFS[tf]
    now = int(time.time())
    params = {"market_id": MARKET_INDICES[sym], "resolution": tf, "start_timestamp": (now - tf_s * 500) * 1000,
              "end_timestamp": now * 1000, "count_back": 500}
    async with session.get(f"{BASE_URL}/api/v1/candles", params=params, timeout=aiohttp.ClientTimeout(total=15)) as r:
        data = await r.json(content_type=None)
    rows = data.get("c") or data.get("candlesticks") or data.get("candles") or []
    cs = _parse_rest(rows, tf_s)
    if cs:
        _rest[(sym, tf)] = cs
    else:
        debug_log(f"⚠️ [{sym}] Lighter-REST {tf}: keine Kerzen erkannt", {"antwort": str(data)[:300]})


async def _rest_loop():
    last = {}
    async with aiohttp.ClientSession() as session:
        while True:
            for sym in COINS:
                for tf in REST_TFS:
                    if time.time() - last.get((sym, tf), 0) < REST_EVERY[tf]:
                        continue
                    last[(sym, tf)] = time.time()
                    try:
                        await _fetch_rest(session, sym, tf)
                    except Exception as e:
                        debug_log(f"⚠️ [{sym}] Lighter-REST {tf} fehlgeschlagen", {"error": str(e)})
                    await asyncio.sleep(1)
            await asyncio.sleep(5)


def get_tf(sym, tf, count):
    """1m/5m/15m: REST-Kerzen + aktuelle Kerze aus dem Live-Stream. None, solange noch nichts da ist."""
    base = _rest.get((sym, tf))
    if not base:
        return None
    tf_ms = REST_TFS[tf] * 1000
    out = [dict(c) for c in base]
    last_ts = out[-1]["ts"]
    own = _candles.get(sym) or {}
    for k in sorted(own):
        if k < last_ts:
            continue
        b = k // tf_ms * tf_ms
        c = own[k]
        if b == out[-1]["ts"]:
            x = out[-1]
            x["h"] = max(x["h"], c["h"]); x["l"] = min(x["l"], c["l"]); x["c"] = c["c"]
        elif b > out[-1]["ts"]:
            out.append({"ts": b, "o": c["o"], "h": c["h"], "l": c["l"], "c": c["c"], "v": c["v"], "tb": c["tb"], "n": c["n"]})
    return out[-count:]
