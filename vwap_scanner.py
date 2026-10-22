"""
vwap_scanner.py - VWAP-Scanner: Unterseite /vwap-scanner.

Pro Coin eine Zeile, drei Spalten (Day-, Week-, Month-VWAP). Jede Zelle zeigt, wie weit die EMA 9
vom jeweiligen VWAP entfernt ist (in %, = Abstand bis zum naechsten Cross), ob sie darueber oder
darunter liegt und wann der letzte Cross (Buy = EMA 9 kreuzt den VWAP nach oben, Sell = nach unten)
war. Gleiche Logik wie das Pine-Skript "Wellenanker Overlay" (VWAP aus hlc3 x Volumen, Reset je
Tag/Woche/Monat in UTC, Woche beginnt Montag).

Eigene, getrennte Strategie/Seite - aendert nichts an den Handels-Strategien.

Daten: je Coin die erste Quelle, die Kerzen liefert: Binance SPOT -> Binance FUTURES -> Lighter (REST). Die
gefundene Quelle wird 6 Stunden gemerkt (steht als Kuerzel in der Tabelle). Alle REFRESH_SECONDS (Standard 30 s)
und NUR solange die Seite offen ist (nach IDLE_AFTER Sekunden ohne Abruf pausiert die Berechnung -> keine Last).
Lighter liefert max. 500 Kerzen je Abruf und kein 3m: 3m wird dort aus 1m-Kerzen zusammengesetzt (Fenster ca. 8 h).
 - Zeitrahmen der EMA 9: 1m oder 3m (TF_CANDLES = 1000 Kerzen, eine Anfrage je Coin und Runde).
 - VWAP: bereits abgeschlossene 15m-Kerzen seit Wochen-/Monatsbeginn (laufend zwischengespeichert,
   je Runde nur die neuen Kerzen nachladen) + die Kerzen des gewaehlten Zeitrahmens fuer die letzten Stunden.
   Dadurch ist der VWAP fast identisch mit dem aus 1m-Kerzen, braucht aber nur wenige Anfragen.
Die Anfragen laufen ueber binance_ws._rest_klines_range (gemeinsame Drossel und Bann-Schutz).
"""

import asyncio
import datetime
import json
import os
import re
import time
import traceback

import aiohttp
from aiohttp import web

from bot_core import debug_log, get_redis, BASE_URL, MARKET_INDICES
import binance_ws
from strategies import BINANCE_SYMBOL_MAP, BINANCE_FUTURES_ONLY_SYMBOLS

REFRESH_SECONDS = float(os.getenv("VWAP_SCANNER_INTERVAL", "30"))
IDLE_AFTER = 120            # Sekunden ohne Seitenabruf, danach pausiert die Berechnung
TFS = {"1m": 60_000, "3m": 180_000}
BASE_MS = 900_000           # 15m-Basiskerzen fuer den VWAP seit Perioden-Beginn
TF_CANDLES = 1000           # Kerzen des gewaehlten Zeitrahmens je Anfrage (Binance-Maximum)
MAX_COINS = 150
REDIS_KEY = "gridbot:vwap_scanner_cfg"

DEFAULT_COINS = ["BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "ADA", "AVAX", "LINK", "SUI", "TRX", "LTC"]
CFG_DEFAULT = {"coins": DEFAULT_COINS, "tf": "1m", "ema": 9, "near": 0.3}
CFG = dict(CFG_DEFAULT)

STATE = {"rows": {}, "updated": 0.0, "last_view": 0.0, "force": False, "cycle_sec": 0.0}
_BASE = {}      # Binance-Paar -> {"bars": [15m-Kerzen], "start": Beginn des Abrufbereichs}
_PS_CACHE = {}  # (Tag, Art) -> Perioden-Beginn
_NODATA = {}    # Coin -> (Fehlversuche, naechster Versuch ab) - Coins ohne Daten in allen Quellen nicht jede Runde neu anfragen
_SRC = {}       # Coin -> (Quelle, gueltig bis) - zuletzt erfolgreiche Quelle
_LIGHTER_IDS = {}   # Symbol -> market_id (aus /api/v1/orderBooks)
_LIGHTER_IDS_AT = 0.0
SRC_TAGS = {"spot": "Spot", "futures": "Fut", "lighter": "Lighter"}


# ---------------------------------------------------------------------------
# Rechnung
# ---------------------------------------------------------------------------
def _period_start(ts_ms, kind):
    """Beginn der Periode (UTC, ms): 'd' Tag, 'w' Woche (Montag), 'm' Monat."""
    day = int(ts_ms // 86_400_000)
    key = (day, kind)
    v = _PS_CACHE.get(key)
    if v is not None:
        return v
    if kind == "d":
        v = day * 86_400_000
    elif kind == "w":
        v = (day - (day + 3) % 7) * 86_400_000   # 01.01.1970 war ein Donnerstag
    else:
        dt = datetime.datetime.fromtimestamp(day * 86400, datetime.timezone.utc)
        v = int(datetime.datetime(dt.year, dt.month, 1, tzinfo=datetime.timezone.utc).timestamp() * 1000)
    if len(_PS_CACHE) > 5000:
        _PS_CACHE.clear()
    _PS_CACHE[key] = v
    return v


def _ema(values, n):
    k = 2.0 / (n + 1)
    out, prev = [], None
    for x in values:
        prev = x if prev is None else prev + k * (x - prev)
        out.append(prev)
    return out


def _clean_cfg(body, base=None):
    cfg = dict(base or CFG)
    coins = body.get("coins", None)
    if coins is not None:
        if isinstance(coins, str):
            coins = re.split(r"[\s,;]+", coins)
        out = []
        for c in coins:
            c = re.sub(r"[^A-Z0-9]", "", str(c).upper())
            if c.endswith("USDT") and len(c) > 4:
                c = c[:-4]
            if c and c not in out:
                out.append(c)
        if out:
            cfg["coins"] = out[:MAX_COINS]
    if body.get("tf") in TFS:
        cfg["tf"] = body["tf"]
    try:
        if "ema" in body:
            cfg["ema"] = max(2, min(200, int(body["ema"])))
        if "near" in body:
            cfg["near"] = max(0.01, min(10.0, float(body["near"])))
    except (TypeError, ValueError):
        pass
    return cfg


async def fetch_lighter_perps():
    """Alle Perp-Maerkte von Lighter (Symbole) per /api/v1/orderBooks. None bei Fehler."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(BASE_URL + "/api/v1/orderBooks", timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
    except Exception as e:
        debug_log("⚠️ [VWAP-Scanner] Lighter-Marktliste konnte nicht geladen werden", {"error": str(e)})
        return None
    books = data.get("order_books") if isinstance(data, dict) else None
    if not isinstance(books, list):
        return None
    out = []
    for b in books:
        if not isinstance(b, dict):
            continue
        sym = re.sub(r"[^A-Z0-9]", "", str(b.get("symbol", "")).upper().split("-")[0])
        if not sym or "/" in str(b.get("symbol", "")):
            continue
        if str(b.get("market_type", "perp")).lower() not in ("perp", "perps", "perpetual", ""):
            continue
        if str(b.get("status", "active")).lower() not in ("active", ""):
            continue
        if sym not in out:
            out.append(sym)
        try:
            if b.get("market_id") is not None:
                _LIGHTER_IDS[sym] = int(b["market_id"])
        except (TypeError, ValueError):
            pass
    return out or None


def _spot_pair(coin):
    """Binance-Spot-Paar zu einem Lighter-Symbol (Lighter nutzt teils '1000PEPE' / '1MBABYDOGE', Binance 'PEPEUSDT')."""
    if coin in BINANCE_SYMBOL_MAP:
        return BINANCE_SYMBOL_MAP[coin]
    base = coin
    if base.startswith("1000") and len(base) > 4:
        base = base[4:]
    elif base.startswith("1M") and len(base) > 2:
        base = base[2:]
    return base + "USDT"


_LIGHTER_LOCK = None


def _f(x, default=None):
    try:
        return float(x)
    except Exception:
        return default


def _parse_lighter(rows, tf_ms):
    out = []
    prev = None
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        t = r.get("t", r.get("timestamp"))
        if t is None:
            continue
        t = int(t)
        if t < 10 ** 12:
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
        out.append({"ts": t // tf_ms * tf_ms, "o": o, "h": h, "l": l, "c": c, "v": v, "tb": 0.0, "n": 0})
        prev = c
    out.sort(key=lambda x: x["ts"])
    return out


async def _lighter_klines(market_id, interval, start_ms, end_ms, limit):
    """Kerzen von Lighter (max. 500 je Abruf). None bei Fehler."""
    global _LIGHTER_LOCK
    if _LIGHTER_LOCK is None:
        _LIGHTER_LOCK = asyncio.Lock()
    tf_ms = {"1m": 60_000, "15m": 900_000}[interval]
    params = {"market_id": market_id, "resolution": interval, "start_timestamp": int(start_ms),
              "end_timestamp": int(end_ms), "count_back": min(int(limit), 500)}
    async with _LIGHTER_LOCK:                    # hoechstens eine Anfrage gleichzeitig, kleine Pause dazwischen
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(BASE_URL + "/api/v1/candles", params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json(content_type=None)
        except Exception as e:
            debug_log(f"⚠️ [VWAP-Scanner] Lighter-Kerzen fehlgeschlagen (Markt {market_id})", {"error": str(e)})
            return None
        finally:
            await asyncio.sleep(0.2)
    rows = (data.get("c") or data.get("candlesticks") or data.get("candles") or []) if isinstance(data, dict) else []
    return [c for c in _parse_lighter(rows, tf_ms) if start_ms - tf_ms < c["ts"] <= end_ms]


def _agg(c1m, tf_ms):
    """1m-Kerzen zu groesseren Kerzen (z.B. 3m) zusammensetzen; die erste, unvollstaendige Gruppe faellt weg."""
    out = {}
    for c in c1m:
        b = c["ts"] // tf_ms * tf_ms
        x = out.get(b)
        if x is None:
            out[b] = {"ts": b, "o": c["o"], "h": c["h"], "l": c["l"], "c": c["c"], "v": c["v"], "tb": 0.0, "n": 0, "_first": c["ts"]}
        else:
            x["h"] = max(x["h"], c["h"])
            x["l"] = min(x["l"], c["l"])
            x["c"] = c["c"]
            x["v"] += c["v"]
    res = [out[k] for k in sorted(out)]
    if res and res[0]["_first"] != res[0]["ts"]:
        res = res[1:]
    for x in res:
        x.pop("_first", None)
    return res


async def _klines(src, key, interval, start_ms, end_ms, limit):
    if src == "lighter":
        return await _lighter_klines(key, interval, start_ms, end_ms, limit)
    return await binance_ws._rest_klines_range(src, key, interval, start_ms, end_ms, limit)


async def _fetch_base(src, key, now_ms):
    """Abgeschlossene 15m-Kerzen seit Wochen-/Monatsbeginn (zwischengespeichert, je Aufruf nur Nachladen)."""
    base_start = min(_period_start(now_ms, "m"), _period_start(now_ms, "w"))
    ck = (src, key)
    ent = _BASE.get(ck)
    if ent is None or ent["start"] != base_start:
        ent = {"bars": [], "start": base_start}
        _BASE[ck] = ent
    bars = ent["bars"]
    page = 500 if src == "lighter" else 1000
    next_ts = bars[-1]["ts"] + BASE_MS if bars else base_start
    while next_ts + BASE_MS <= now_ms:
        end = min(now_ms, next_ts + page * BASE_MS - 1)
        got = await _klines(src, key, "15m", next_ts, end, page)
        if got is None:
            return bars or None
        got = [c for c in got if c["ts"] >= next_ts and c["ts"] + BASE_MS <= now_ms]
        if not got:
            if end >= now_ms:
                break
            next_ts = end + 1        # Luecke im Fenster (z.B. Markt noch nicht gelistet) -> naechstes Fenster
            continue
        bars.extend(got)
        next_ts = bars[-1]["ts"] + BASE_MS
    return bars


def compute_rows(tfc, bars, tf_ms, ema_len, now_ms):
    """Reine Rechnung (ohne Netzwerk). tfc = Kerzen des Zeitrahmens (aufsteigend, letzte = laufende Kerze),
    bars = abgeschlossene 15m-Kerzen. -> dict je 'd'/'w'/'m': pct, vwap, cross(letzter Cross)."""
    first_ts = tfc[0]["ts"]
    t_w = -(-first_ts // BASE_MS) * BASE_MS          # erste volle 15m-Grenze im Fenster
    items = [(b["ts"], (b["h"] + b["l"] + b["c"]) / 3.0 * b["v"], b["v"], None) for b in bars if b["ts"] < t_w]
    j0 = None
    for j, c in enumerate(tfc):
        if c["ts"] >= t_w:
            if j0 is None:
                j0 = j
            items.append((c["ts"], (c["h"] + c["l"] + c["c"]) / 3.0 * c["v"], c["v"], j))
    n = len(tfc)
    if j0 is None or n - j0 < 3:
        return None
    ema = _ema([c["c"] for c in tfc], ema_len)
    out = {}
    for k in ("d", "w", "m"):
        vw = [None] * n
        resets = set()
        cur_ps, spv, sv = None, 0.0, 0.0
        for ts, pv, v, j in items:
            ps = _period_start(ts, k)
            if ps != cur_ps:
                cur_ps, spv, sv = ps, 0.0, 0.0
                if j is not None:
                    resets.add(j)
            spv += pv
            sv += v
            if j is not None and sv > 0:
                vw[j] = spv / sv
        last = n - 1
        if vw[last] is None:
            out[k] = {"novol": True}          # keine Umsaetze -> VWAP nicht berechenbar
            continue
        pct = (ema[last] - vw[last]) / vw[last] * 100.0
        cross = None
        for j in range(j0 + 1, last):                  # nur abgeschlossene Kerzen (die laufende zaehlt nicht)
            if vw[j] is None or vw[j - 1] is None or j in resets:   # Perioden-Start-Sprung ist kein Cross
                continue
            p = ema[j - 1] - vw[j - 1]
            q = ema[j] - vw[j]
            if p <= 0 < q:
                cross = (tfc[j]["ts"], "buy")
            elif p >= 0 > q:
                cross = (tfc[j]["ts"], "sell")
        out[k] = {
            "pct": round(pct, 4), "vwap": vw[last],
            "cross": {"dir": cross[1], "age": max(0, int((now_ms - (cross[0] + tf_ms)) / 1000))} if cross else None,
        }
    return out


def _pair(src, coin):
    if src == "spot":
        return _spot_pair(coin)
    return BINANCE_SYMBOL_MAP.get(coin, coin + "USDT")      # Futures: Lighter-Name + USDT (z.B. 1000PEPEUSDT)


async def _lighter_id(coin):
    global _LIGHTER_IDS_AT
    if coin not in _LIGHTER_IDS and time.time() - _LIGHTER_IDS_AT > 3600:
        _LIGHTER_IDS_AT = time.time()
        await fetch_lighter_perps()
    return _LIGHTER_IDS.get(coin, MARKET_INDICES.get(coin))


async def _try_source(src, coin, cfg, now_ms):
    """-> (Kerzen des Zeitrahmens, 15m-Basiskerzen) oder None."""
    tf = cfg["tf"]
    tf_ms = TFS[tf]
    if src == "lighter":
        key = await _lighter_id(coin)
        if key is None:
            return None
        c1 = await _lighter_klines(key, "1m", now_ms - 500 * 60_000, now_ms, 500)
        if not c1 or len(c1) < 30:
            return None
        tfc = c1 if tf == "1m" else _agg(c1, tf_ms)
    else:
        if src == "spot" and coin in BINANCE_FUTURES_ONLY_SYMBOLS:
            return None
        key = _pair(src, coin)
        tfc = await binance_ws._rest_klines_range(src, key, tf, now_ms - TF_CANDLES * tf_ms, now_ms, TF_CANDLES)
    if not tfc or len(tfc) < 30:
        return None
    bars = await _fetch_base(src, key, now_ms)
    if not bars:
        return None
    return tfc, bars


async def _compute_coin(coin, cfg, now_ms):
    from strategies import _binance_is_banned
    miss = _NODATA.get(coin)
    if miss and miss[0] >= 3 and time.time() < miss[1]:
        return {"coin": coin, "err": "keine Daten (Spot, Futures, Lighter)"}
    cached = _SRC.get(coin)
    order = [cached[0]] if cached and time.time() < cached[1] else ["spot", "futures", "lighter"]
    found = None
    for src in order:
        got = await _try_source(src, coin, cfg, now_ms)
        if got:
            found = (src, got)
            break
    if not found and len(order) == 1:                  # gemerkte Quelle liefert nichts mehr -> alle Quellen neu pruefen
        _SRC.pop(coin, None)
        for src in ("spot", "futures", "lighter"):
            if src == order[0]:
                continue
            got = await _try_source(src, coin, cfg, now_ms)
            if got:
                found = (src, got)
                break
    if not found:
        if not (_binance_is_banned("spot") or _binance_is_banned("futures")):   # bei Bann nicht als "keine Daten" merken
            n = (miss[0] if miss else 0) + 1
            _NODATA[coin] = (n, time.time() + 3600)    # nach 3 Fehlversuchen 1 Stunde Pause fuer diesen Coin
        return {"coin": coin, "err": "keine Daten (Spot, Futures, Lighter)"}
    src, (tfc, bars) = found
    _NODATA.pop(coin, None)
    if not cached or cached[0] != src or time.time() >= cached[1]:
        _SRC[coin] = (src, time.time() + 6 * 3600)
    tf_ms = TFS[cfg["tf"]]
    res = await asyncio.to_thread(compute_rows, tfc, bars, tf_ms, int(cfg["ema"]), now_ms)
    if not res:
        return {"coin": coin, "err": "zu wenig Kerzen"}
    row = {"coin": coin, "price": tfc[-1]["c"], "tf": cfg["tf"], "ts": time.time(), "src": SRC_TAGS[src],
           "win_h": round(len(tfc) * tf_ms / 3_600_000, 1)}
    row.update(res)
    return row


# ---------------------------------------------------------------------------
# Hintergrund-Loop, Speicherung, Web
# ---------------------------------------------------------------------------
async def load_vwap_scanner_cfg():
    saved = False
    r = await get_redis()
    if r is not None:
        try:
            raw = await asyncio.wait_for(r.get(REDIS_KEY), timeout=5)
            if raw:
                CFG.update(_clean_cfg(json.loads(raw), dict(CFG_DEFAULT)))
                saved = True
                debug_log("✅ VWAP-Scanner-Einstellungen aus Redis geladen", CFG)
        except Exception as e:
            debug_log("⚠️ Laden der VWAP-Scanner-Einstellungen fehlgeschlagen", {"error": str(e)})
    if not saved:
        # Erster Start: alle Perp-Maerkte von Lighter als Coin-Liste (auf der Seite frei aenderbar)
        coins = await fetch_lighter_perps()
        if coins:
            CFG["coins"] = coins[:MAX_COINS]
            debug_log(f"✅ VWAP-Scanner: {len(CFG['coins'])} Lighter-Perp-Coins als Startliste")


async def vwap_scanner_loop():
    last_start = 0.0
    while True:
        try:
            now = time.time()
            viewed = now - STATE["last_view"] < IDLE_AFTER
            if viewed and (STATE["force"] or now - last_start >= REFRESH_SECONDS):
                STATE["force"] = False
                last_start = now
                cfg = dict(CFG)
                coins = list(cfg["coins"])
                for coin in coins:
                    try:
                        row = await _compute_coin(coin, cfg, int(time.time() * 1000))
                    except Exception as e:
                        debug_log(f"⚠️ [VWAP-Scanner] {coin} fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
                        row = {"coin": coin, "err": "Fehler"}
                    old = STATE["rows"].get(coin)
                    if row.get("err") and old and not old.get("err") and old.get("tf") == cfg["tf"]:
                        old["stale"] = True      # letzte gute Werte behalten, aber als veraltet markieren
                    else:
                        STATE["rows"][coin] = row
                    if STATE["force"]:           # Einstellungen wurden waehrend der Runde geaendert -> neu starten
                        break
                for c in list(STATE["rows"]):
                    if c not in CFG["coins"]:
                        STATE["rows"].pop(c, None)
                STATE["updated"] = time.time()
                STATE["cycle_sec"] = round(time.time() - now, 1)
        except Exception as e:
            debug_log("⚠️ [VWAP-Scanner] Runde fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        await asyncio.sleep(1)


async def handle_vwap_index(request):
    return web.Response(text=VWAP_HTML, content_type="text/html")


async def handle_vwap_api(request):
    STATE["last_view"] = time.time()
    rows = []
    for c in CFG["coins"]:
        r = STATE["rows"].get(c)
        rows.append(r if (r and (r.get("err") or r.get("tf") == CFG["tf"])) else {"coin": c, "loading": True})
    return web.json_response({"cfg": CFG, "default": CFG_DEFAULT, "updated": STATE["updated"], "refresh": REFRESH_SECONDS,
                              "server_time": time.time(), "cycle_sec": STATE["cycle_sec"], "rows": rows})


async def handle_vwap_lighter(request):
    coins = await fetch_lighter_perps()
    if not coins:
        return web.json_response({"ok": False, "error": "Lighter-Marktliste nicht erreichbar"})
    return web.json_response({"ok": True, "coins": coins[:MAX_COINS], "total": len(coins)})


async def handle_vwap_cfg_set(request):
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "kein JSON"}, status=400)
    new = _clean_cfg(CFG_DEFAULT if body.get("reset") else body)
    if new["tf"] != CFG["tf"] or new["ema"] != CFG["ema"]:
        STATE["rows"].clear()
    CFG.clear()
    CFG.update(new)
    STATE["force"] = True
    STATE["last_view"] = time.time()
    r = await get_redis()
    if r is not None:
        try:
            await asyncio.wait_for(r.set(REDIS_KEY, json.dumps(CFG)), timeout=5)
        except Exception as e:
            debug_log("⚠️ Speichern der VWAP-Scanner-Einstellungen fehlgeschlagen", {"error": str(e)})
    return web.json_response({"ok": True, "cfg": CFG})


VWAP_HTML = r"""<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>VWAP-Scanner</title>
<style>
:root{--bg:#07080b;--panel:#14171d;--pb:#242932;--text:#e9ecf1;--dim:#7d8696;--green:#1fcf6e;--red:#f0354b;--yellow:#f5b83d}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font-family:Inter,-apple-system,"Segoe UI",sans-serif;padding-bottom:30px}
.mono{font-family:'JetBrains Mono',ui-monospace,Menlo,Consolas,monospace}
header{display:flex;justify-content:space-between;align-items:center;padding:14px 24px;background:rgba(7,8,11,.9);border-bottom:1px solid var(--pb);margin-bottom:22px;flex-wrap:wrap;gap:10px}
.brand{display:flex;align-items:center;gap:10px;font-size:19px;font-weight:700;color:#fff}
.dot{width:10px;height:10px;border-radius:50%;background:var(--green);box-shadow:0 0 12px rgba(31,207,110,.6)}
nav a{color:#93c5fd;text-decoration:none;font-size:13px;margin-left:16px}
.wrap{padding:0 24px;max-width:1400px;margin:0 auto}
.panel{background:linear-gradient(180deg,#171a21,#12151b);border:1px solid var(--pb);border-radius:16px;padding:16px}
h2{font-size:11px;color:var(--dim);text-transform:uppercase;letter-spacing:.1em;margin:0 0 12px;font-weight:600}
.row{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:14px}
.lab{font-size:11px;color:var(--dim);text-transform:uppercase;letter-spacing:.03em;margin-right:2px}
button.pill{padding:0 14px;height:34px;border-radius:20px;background:#0d1015;color:var(--dim);border:1px solid #2a303a;font:600 12px Inter,sans-serif;cursor:pointer}
button.pill.on{background:rgba(31,207,110,.12);border-color:var(--green);color:#fff}
button.pill:hover{color:#fff}
.info{margin-left:auto;font-size:12px;color:var(--dim)}
table{width:100%;border-collapse:collapse;font-size:13px;table-layout:fixed}
th,td{text-align:left;padding:10px 12px;border-bottom:1px solid var(--pb);vertical-align:top}
th{color:var(--dim);font-weight:600;font-size:11px;text-transform:uppercase;letter-spacing:.03em}
tr.near td{background:rgba(245,184,61,.05)}
.coin{font-weight:700;font-size:14px;color:#fff}.sub{font-size:11px;color:var(--dim);margin-top:3px}
.top{display:flex;align-items:center;justify-content:space-between;margin-bottom:6px}
.pct{font-weight:700;font-size:13px}.green{color:var(--green)}.red{color:var(--red)}.yellow{color:var(--yellow)}
.badge{padding:2px 10px;border-radius:20px;font-size:11px;font-weight:700}
.b-up{background:rgba(34,197,94,.15);color:#86efac;border:1px solid rgba(34,197,94,.35)}
.b-dn{background:rgba(240,82,107,.15);color:#fca5b1;border:1px solid rgba(240,82,107,.4)}
.b-nr{background:rgba(251,191,36,.15);color:#fde68a;border:1px solid rgba(251,191,36,.35)}
.trk{position:relative;height:6px;background:#0d1015;border-radius:3px;border:1px solid #1d2128}
.mid{position:absolute;left:50%;top:-3px;width:1px;height:10px;background:#3a4150}
.f{position:absolute;top:0;height:4px;border-radius:3px}
.last{font-size:11px;color:var(--dim);margin-top:6px}
.leg{display:flex;gap:18px;flex-wrap:wrap;font-size:12px;color:var(--dim);margin-top:14px}
.err{color:var(--dim);font-size:12px}
textarea,input{background:#0d1015;border:1px solid var(--pb);border-radius:10px;color:var(--text);padding:9px 10px;font:13px Inter,sans-serif;color-scheme:dark}
textarea{width:100%;min-height:64px}
input[type=number]{width:90px}
button.act{padding:8px 16px;background:#1d2128;color:#e9ecf1;border:1px solid #2a303a;border-radius:10px;font:700 12px Inter,sans-serif;cursor:pointer}
button.act:hover{filter:brightness(1.15)}
#edit{display:none;margin-bottom:14px}
th.sh{cursor:pointer;user-select:none}th.sh:hover{color:#e9ecf1}th.sh.on{color:#f5b83d}
button.rm{background:transparent;border:1px solid #2a303a;color:#8b93a1;border-radius:8px;width:30px;height:30px;font:700 14px Inter,sans-serif;cursor:pointer}button.rm:hover{color:#ef4444;border-color:#ef4444}
</style>
</head>
<body>
<header>
  <div class="brand"><span class="dot"></span>VWAP-Scanner</div>
  <nav><a href="/">← Dashboard</a><a href="/screener">Coin Screener</a><a href="/scalp">Scalp</a></nav>
</header>
<div class="wrap"><div class="panel">
  <h2>Markt-Scanner · EMA <span id="elen">9</span> gegen VWAP · Binance Spot, Futures, Lighter</h2>
  <div class="row">
    <span class="lab">Zeitrahmen</span><button class="pill" id="tf-1m">1m</button><button class="pill" id="tf-3m">3m</button>
    <span class="lab" style="margin-left:14px">Sortieren</span>
    <button class="pill" id="s-d">Nächster Cross Day</button><button class="pill" id="s-w">Week</button><button class="pill" id="s-m">Month</button><button class="pill" id="s-l">Liste</button>
    <button class="pill" id="btn-edit" style="margin-left:14px">Coins bearbeiten</button>
    <span class="info mono" id="info">lädt …</span>
  </div>
  <div id="edit">
    <div class="lab" style="margin-bottom:6px">Coins (Komma oder Leerzeichen, Reihenfolge = Reihenfolge der Liste, max. 150)</div>
    <textarea id="coins"></textarea>
    <div class="row" style="margin-top:10px">
      <span class="lab">EMA-Länge</span><input type="number" id="ema" min="2" max="200">
      <span class="lab" style="margin-left:10px">Cross nah unter (%)</span><input type="number" id="near" min="0.01" max="10" step="0.05">
      <button class="act" id="btn-save">Speichern</button><button class="act" id="btn-reset">Standard</button>
      <button class="act" id="btn-lighter">Alle Lighter-Futures laden</button><button class="act" id="btn-nospot">Coins ohne Daten entfernen</button>
      <span class="err" id="edit-msg"></span>
    </div>
  </div>
  <table><thead><tr><th style="width:13%">Coin</th><th class="sh" id="h-d">Day-VWAP<span class="ar"></span></th><th class="sh" id="h-w">Week-VWAP<span class="ar"></span></th><th class="sh" id="h-m">Month-VWAP<span class="ar"></span></th><th style="width:44px"></th></tr></thead><tbody id="tb"></tbody></table>
  <div class="leg"><span>Balken rechts der Mitte: EMA über VWAP · links: darunter</span><span>Prozent: Abstand bis zum Cross</span><span class="yellow">Gelb: Cross nah</span><span>Buy = EMA kreuzt VWAP nach oben, Sell = nach unten</span></div>
</div></div>
<script>
var CFG=null,DATA=null,SORT='d',DIR=1,busy=false;
try{SORT=localStorage.getItem('vwapSort')||'d';DIR=localStorage.getItem('vwapDir')==='-1'?-1:1;}catch(e){}
function $(i){return document.getElementById(i);}
function esc(s){return String(s).replace(/[&<>"]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c];});}
function fmt(v){return Math.abs(v).toFixed(2).replace('.',',');}
function price(p){if(p==null)return '';return Number(p).toLocaleString('de-DE',{maximumSignificantDigits:6});}
function age(s){if(s<90)return '1 min';if(s<5400)return Math.round(s/60)+' min';if(s<172800)return Math.round(s/3600)+' h';return Math.round(s/86400)+' T';}
function cell(o,near,win){
  if(!o||o.novol)return '<div class="err">'+(o&&o.novol?'kein Volumen':'keine Daten')+'</div>';
  var up=o.pct>=0,n=Math.abs(o.pct)<near,a=Math.min(Math.abs(o.pct),3)/3*50;
  var cls=n?'b-nr':(up?'b-up':'b-dn'),tx=n?'Cross nah':(up?'darüber':'darunter');
  var lc=o.cross?('Letzter Cross: '+(o.cross.dir==='buy'?'Buy':'Sell')+' vor '+age(o.cross.age)):('Kein Cross in den letzten '+Math.round(win)+' h');
  return '<div class="top"><span class="pct mono '+(up?'green':'red')+'">'+(up?'+':'-')+fmt(o.pct)+' %</span><span class="badge '+cls+'">'+tx+'</span></div>'
   +'<div class="trk"><div class="mid"></div><div class="f" style="'+(up?'left:50%':'right:50%')+';width:'+a+'%;background:'+(up?'var(--green)':'var(--red)')+'"></div></div>'
   +'<div class="last">'+lc+'</div>';
}
function rmBtn(c){return '<td style="text-align:right"><button class="rm" data-c="'+esc(c)+'" title="Coin aus der Liste entfernen">✕</button></td>';}
function sortRows(rows){
  var r=rows.slice();
  if(SORT==='l')return r;
  r.sort(function(a,b){var x=(a[SORT]&&a[SORT].pct!=null)?Math.abs(a[SORT].pct):1e9,y=(b[SORT]&&b[SORT].pct!=null)?Math.abs(b[SORT].pct):1e9;return (x-y)*DIR;});
  return r;
}
function render(){
  if(!DATA)return;
  var near=DATA.cfg.near,tb=[];
  sortRows(DATA.rows).forEach(function(r){
    if(r.loading){tb.push('<tr><td><div class="coin">'+esc(r.coin)+'</div></td><td colspan="3" class="err">lädt …</td>'+rmBtn(r.coin)+'</tr>');return;}
    if(r.err){tb.push('<tr><td><div class="coin">'+esc(r.coin)+'</div></td><td colspan="3" class="err">'+esc(r.err)+'</td>'+rmBtn(r.coin)+'</tr>');return;}
    var nr=['d','w','m'].some(function(k){return r[k]&&r[k].pct!=null&&Math.abs(r[k].pct)<near;});
    tb.push('<tr class="'+(nr?'near':'')+'"><td><div class="coin">'+esc(r.coin)+'</div><div class="sub mono">'+price(r.price)+' · '+esc(r.src||'')+(r.stale?' · veraltet':'')+'</div></td><td>'+cell(r.d,near,r.win_h)+'</td><td>'+cell(r.w,near,r.win_h)+'</td><td>'+cell(r.m,near,r.win_h)+'</td>'+rmBtn(r.coin)+'</tr>');
  });
  $('tb').innerHTML=tb.join('');
  ['d','w','m','l'].forEach(function(k){$('s-'+k).className='pill'+(SORT===k?' on':'');});
  ['d','w','m'].forEach(function(k){$('h-'+k).className='sh'+(SORT===k?' on':'');$('h-'+k).querySelector('.ar').textContent=SORT===k?(DIR===1?' ▲':' ▼'):'';});
  ['1m','3m'].forEach(function(k){$('tf-'+k).className='pill'+(DATA.cfg.tf===k?' on':'');});
  $('elen').textContent=DATA.cfg.ema;
  var t=DATA.updated?new Date(DATA.updated*1000).toLocaleTimeString('de-DE'):'–';
  $('info').textContent='Stand '+t+' · '+DATA.cfg.coins.length+' Coins · alle '+Math.round(DATA.refresh)+' s'+(DATA.cycle_sec?' · Runde '+DATA.cycle_sec+' s':'');
}
function load(){
  if(busy)return;busy=true;
  fetch('/api/vwap_scanner').then(function(r){return r.json();}).then(function(d){
    DATA=d;
    if(!CFG){CFG=d.cfg;$('coins').value=d.cfg.coins.join(', ');$('ema').value=d.cfg.ema;$('near').value=d.cfg.near;}
    render();
  }).catch(function(){$('info').textContent='Verbindung unterbrochen …';}).then(function(){busy=false;});
}
function save(body){
  fetch('/api/vwap_scanner/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)})
   .then(function(r){return r.json();}).then(function(d){
     if(d.ok){CFG=d.cfg;$('coins').value=d.cfg.coins.join(', ');$('ema').value=d.cfg.ema;$('near').value=d.cfg.near;load();}
   });
}
['1m','3m'].forEach(function(k){$('tf-'+k).onclick=function(){save({tf:k});};});
function setSort(k,toggle){if(toggle&&SORT===k)DIR=-DIR;else{SORT=k;DIR=1;}try{localStorage.setItem('vwapSort',SORT);localStorage.setItem('vwapDir',String(DIR));}catch(e){}render();}
['d','w','m','l'].forEach(function(k){$('s-'+k).onclick=function(){setSort(k,false);};});
['d','w','m'].forEach(function(k){$('h-'+k).onclick=function(){setSort(k,true);};});
$('tb').onclick=function(e){
  var b=e.target.closest?e.target.closest('button.rm'):null;if(!b||!DATA)return;
  var c=b.getAttribute('data-c'),keep=DATA.cfg.coins.filter(function(x){return x!==c;});
  if(!keep.length){$('info').textContent='Mindestens ein Coin muss bleiben';return;}
  DATA.cfg.coins=keep;DATA.rows=DATA.rows.filter(function(r){return r.coin!==c;});render();save({coins:keep});
};
$('btn-edit').onclick=function(){var e=$('edit');e.style.display=e.style.display==='block'?'none':'block';};
$('btn-save').onclick=function(){save({coins:$('coins').value,ema:$('ema').value,near:$('near').value});};
$('btn-reset').onclick=function(){save({reset:true});};
$('btn-lighter').onclick=function(){
  $('edit-msg').textContent='lädt …';
  fetch('/api/vwap_scanner/lighter').then(function(r){return r.json();}).then(function(d){
    if(d.ok){$('edit-msg').textContent=d.total+' Lighter-Futures geladen';save({coins:d.coins});}else{$('edit-msg').textContent=d.error||'Fehler';}
  }).catch(function(){$('edit-msg').textContent='Fehler';});
};
$('btn-nospot').onclick=function(){
  if(!DATA)return;
  var keep=DATA.rows.filter(function(r){return !(r.err&&r.err.indexOf('keine Daten')>=0);}).map(function(r){return r.coin;});
  var removed=DATA.rows.length-keep.length;
  if(!keep.length){$('edit-msg').textContent='Erst nach der ersten Berechnung möglich';return;}
  $('edit-msg').textContent=removed+' Coins entfernt';save({coins:keep});
};
load();setInterval(load,5000);
</script>
</body>
</html>
"""
