"""
lighter_candles.py - baut eigene 1s-Kerzen aus dem Lighter-Trade-Stream (trade/{market}).
Lighter selbst liefert keine Sekunden-Kerzen. Nur fuer Coins in LIGHTER_1S_COINS
(Env, Komma-getrennt, Standard "HYPE") - alle anderen Coins bleiben bei Binance.
Eigene WS-Verbindung (beeinflusst trading_loop nicht), Snapshot in Redis (ueberlebt Deploys,
die Luecke waehrend des Neustarts bleibt aber leer - sie laesst sich nicht nachholen).
"""
import asyncio, base64, json, os, time, zlib
import websockets
from bot_core import debug_log, WS_URL, MARKET_INDICES, get_redis

COINS = [c.strip().upper() for c in os.environ.get("LIGHTER_1S_COINS", "HYPE").split(",") if c.strip() in MARKET_INDICES or c.strip().upper() in MARKET_INDICES]
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
    await asyncio.gather(_stream_loop(), _snap_loop())
