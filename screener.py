"""
screener.py - Coin-Screener: Unterseite /screener mit einer Kachel pro Coin.

Jede Kachel zeigt (pro Zeitrahmen 1m/5m/15m/1h/4h, umschaltbar) vier Kreise (Moneyflow, RSI, MACD,
ADX) und drei Signal-Balken (Wellenanker, Liquidity Waves, MO7). Ein Klick auf die Kachel oeffnet
ein Popup mit allen Zeitrahmen nebeneinander.

WICHTIG - kein eigener Binance-Traffic: alle Daten kommen ausschliesslich aus dem WebSocket-Kerzen-
Cache (binance_ws.get_cached_candles_ext). Der Screener abonniert dort nur zusaetzliche Streams
(ensure_subscribed) - das Nachladen der Historie und die Bann-/Drossel-Logik uebernimmt die bereits
bestehende Infrastruktur. Solange ein Stream noch nicht warm ist, zeigt die Kachel "laedt".

Die Indikator-Logik ist 1:1 den Pine-Skripten nachempfunden (Wellenanker, Liquidity Waves, MO7);
die dazugehoerigen Parameter stehen als Konstanten direkt unten und lassen sich dort anpassen.
"""

import asyncio
import math
import os
import time
import traceback

from aiohttp import web

from bot_core import debug_log, SYMBOLS, BOTS
import binance_ws
from strategies import (
    BINANCE_SYMBOL_MAP, BINANCE_FUTURES_ONLY_SYMBOLS,
    compute_rsi, compute_adx, compute_atr, compute_mfi, compute_wavetrend_series,
    _ema_series, _sma_series,
)

# ============================================================================
# KONFIGURATION
# ============================================================================
TIMEFRAMES = ["1m", "5m", "15m", "1h", "4h"]
# Wie viele Kerzen je Zeitrahmen aus dem Cache gelesen werden (der Cache selbst haelt mehr/weniger,
# je nach REST-Seed - 4h hat z.B. nur ~200). Mehr als ~600 bringt fuer die Signale nichts.
CANDLES_BACK = {"1m": 600, "5m": 600, "15m": 500, "1h": 300, "4h": 200}
MIN_CANDLES = 60  # darunter gilt der Zeitrahmen als "laedt noch"
SCREENER_INTERVAL = float(os.getenv("SCREENER_INTERVAL", "10"))  # Sekunden zwischen zwei Berechnungsrunden
# Welche Coins? Standard: dieselben wie der Bot (GRID_SYMBOLS). Optional eigene Liste per Umgebungs-
# variable SCREENER_COINS="BTC,ETH,SOL" (Coins ohne Binance-Paar werden als "keine Daten" gezeigt).
SCREENER_COINS = [s.strip().upper() for s in os.getenv("SCREENER_COINS", "").split(",") if s.strip()] or list(SYMBOLS)

# Moneyflow-Kreis: Buyers% = Taker-Buy-Volumen / Gesamtvolumen ueber die letzten N Kerzen des Zeitrahmens
MF_WINDOW = 30

# Wellenanker (Pine: "Wellenanker")
WA_N1, WA_N2, WA_SIG, WA_SCALE = 10, 21, 4, 1.35   # Kanal-Laenge, Durchschnitt-Laenge, Signallinie, Wellen-Skalierung
WA_Z1, WA_Z3 = 36.0, 75.0                           # Long-/Short-Zone 1 (±) und Extrem-Zone 3 (±)
WA_RSI_LEN, WA_RSI_LONG_MAX, WA_RSI_SHORT_MIN = 14, 40.0, 60.0   # "starke Punkte": RSI muss mitziehen ...
WA_MF_LEN, WA_MF_SCALE = 14, 1.2                    # ... und der Geldfluss (f_mf im Original) steigen/fallen

# Liquidity Waves (Pine: "Liquidity Waves (Kleiner Körper)")
LW_BODY_MAX, LW_MAX_LEVELS = 50.0, 50
LW_ENTRY, LW_TP1, LW_TP2 = 5.0, 50.0, 85.0
LW_TP1_NEED_PLUS = True

# MO7 (Pine: "MO7 Buy/Sell Signal")
MO7_LEN = 14
MO7_MACD_FAST, MO7_MACD_SLOW = 12, 26
MO7_LOOKBACK = 500
MO7_BUY, MO7_SELL = 20.0, 85.0
MO7_TREND_EMA = 200
MO7_SCAN = 60  # wie viele der letzten Kerzen auf Signale durchsucht werden (MO7 ist pro Kerze teuer)

SCREENER_STATE = {"updated": 0.0, "coins": {}}
_last_err_log = {}


# ============================================================================
# HILFSFUNKTIONEN
# ============================================================================
def _r(x, nd=2):
    if x is None:
        return None
    try:
        if math.isnan(x) or math.isinf(x):
            return None
    except TypeError:
        return None
    return round(float(x), nd)


def _market_for(symbol):
    if symbol in BINANCE_FUTURES_ONLY_SYMBOLS:
        return "futures"
    try:
        return BOTS[symbol]["config"].get("binance_market_type", "spot")
    except Exception:
        return "spot"


def _stoch_series(h, l, c, length):
    """Pine ta.stoch(close, high, low, length) - identisch zu 100 + ta.wpr(length)."""
    n = len(c)
    out = [50.0] * n
    for i in range(n):
        s = max(0, i - length + 1)
        hh = max(h[s:i + 1])
        ll = min(l[s:i + 1])
        out[i] = (c[i] - ll) / (hh - ll) * 100 if hh > ll else 50.0
    return out


def _percentrank(c, i, length=100):
    """Pine ta.percentrank: Anteil der `length` Werte VOR i, die <= c[i] sind (in %). None, solange zu wenig Historie."""
    if i < length:
        return None
    w = c[i - length:i]
    cur = c[i]
    return sum(1 for x in w if x <= cur) / length * 100


def _event_age(idx, last_closed):
    return max(0, last_closed - idx)


# ============================================================================
# RINGE: Moneyflow, RSI, MACD, ADX
# ============================================================================
def _moneyflow(candles):
    w = candles[-MF_WINDOW:]
    vol = sum(x["v"] for x in w)
    tb = sum(x.get("tb", 0.0) for x in w)
    if vol <= 0 or (tb <= 0 and sum(x.get("n", 0) for x in w) == 0):
        return None  # kein Volumen bzw. Kerzen ohne Taker-Daten (sollte nicht vorkommen, schuetzt vor "0% Buyers")
    buyers = tb / vol * 100
    net_usd = sum((2 * x.get("tb", 0.0) - x["v"]) * x["c"] for x in w)
    vol_usd = sum(x["v"] * x["c"] for x in w)
    trades = sum(x.get("n", 0) for x in w)
    return {"buyers": _r(buyers, 1), "net_usd": _r(net_usd, 0), "vol_usd": _r(vol_usd, 0), "trades": int(trades),
            "window": len(w)}


def _macd(c):
    ef = _ema_series(c, 12)
    es = _ema_series(c, 26)
    line = [ef[i] - es[i] for i in range(len(c))]
    sig = _ema_series(line, 9)
    hist = [line[i] - sig[i] for i in range(len(c))]
    ref = max((abs(x) for x in hist[-50:]), default=0.0)
    strength = min(100.0, abs(hist[-1]) / ref * 100) if ref > 0 else 0.0
    return {"hist": hist[-1], "bull": hist[-1] > 0, "strength": _r(strength, 0),
            "line": line[-1], "signal": sig[-1]}


# ============================================================================
# WELLENANKER
# ============================================================================
def _wave_moneyflow(h, l, c, v):
    """f_mf() aus dem Pine-Skript (Geldfluss-Flaeche, wird fuer 'starke Punkte' gebraucht)."""
    n = len(c)
    src = [(h[i] + l[i] + c[i]) / 3 for i in range(n)]
    up = [0.0] * n
    dn = [0.0] * n
    for i in range(1, n):
        ch = src[i] - src[i - 1]
        if ch > 0:
            up[i] = src[i] * v[i]
        elif ch < 0:
            dn[i] = src[i] * v[i]
    upv = _sma_series(up, WA_MF_LEN)
    dnv = _sma_series(dn, WA_MF_LEN)
    mfi = []
    for i in range(n):
        if dnv[i] == 0:
            mfi.append(50.0 if upv[i] == 0 else 100.0)
        else:
            mfi.append(100.0 - 100.0 / (1.0 + upv[i] / dnv[i]))
    return _ema_series([(m - 50.0) * WA_MF_SCALE for m in mfi], 3)


def _wellenanker(o, h, l, c, v):
    n = len(c)
    wt1, wt2 = compute_wavetrend_series(o, h, l, c, WA_N1, WA_N2, WA_SIG, "close", WA_SCALE)
    rsi = compute_rsi(c, WA_RSI_LEN)
    mf = _wave_moneyflow(h, l, c, v)
    last_closed = n - 2  # letzte GESCHLOSSENE Kerze (die letzte im Cache laeuft noch)
    events = []
    for i in range(max(1, n - 250), last_closed + 1):
        cross_up = wt1[i] > wt2[i] and wt1[i - 1] <= wt2[i - 1]
        cross_dn = wt1[i] < wt2[i] and wt1[i - 1] >= wt2[i - 1]
        if cross_up and wt2[i] < -WA_Z1:
            events.append({"kind": "long", "idx": i, "ext": wt2[i] < -WA_Z3,
                           "strong": rsi[i] < WA_RSI_LONG_MAX and mf[i] > mf[i - 1]})
        elif cross_dn and wt2[i] > WA_Z1:
            events.append({"kind": "short", "idx": i, "ext": wt2[i] > WA_Z3,
                           "strong": rsi[i] > WA_RSI_SHORT_MIN and mf[i] < mf[i - 1]})

    def pack(e):
        if not e:
            return None
        return {"kind": e["kind"], "strong": e["strong"], "ext": e["ext"], "ago": _event_age(e["idx"], last_closed)}

    # Zustand wie f_state() im Panel: LONG/SHORT nur, solange die Welle in der Zone steht UND richtig herum liegt
    a, b = wt1[-1], wt2[-1]
    state = 1 if (b < -WA_Z1 and a > b) else -1 if (b > WA_Z1 and a < b) else 0
    return {"wt1": _r(a, 1), "wt2": _r(b, 1), "diff": _r(a - b, 1), "z1": WA_Z1, "z3": WA_Z3, "state": state,
            "last": pack(events[-1] if events else None), "prev": pack(events[-2] if len(events) > 1 else None)}


# ============================================================================
# LIQUIDITY WAVES
# ============================================================================
def _liquidity_waves(o, h, l, c, v):
    """Nachbau von compute_liqwave_series + der Positionslogik aus dem Pine-Skript, plus die
    Anzahl/Dollar-Summen der Levels auf der letzten (geschlossenen) Kerze. Wird auf GESCHLOSSENEN
    Kerzen gerechnet (Levels entstehen im Original nur bei barstate.isconfirmed)."""
    n = len(c)
    lvls = []  # [price, is_high, usd]
    buy = [None] * n
    sell = [None] * n
    final = (0, 0, 0.0, 0.0)
    for i in range(n):
        rng = h[i] - l[i]
        body = (abs(c[i] - o[i]) / rng * 100) if rng > 0 else 100.0
        price = None
        if body <= LW_BODY_MAX and c[i] < o[i]:
            price, is_high = h[i], True
        elif body <= LW_BODY_MAX and c[i] > o[i]:
            price, is_high = l[i], False
        if price is not None:
            lvls = [lv for lv in lvls if not (lv[1] == is_high and abs(lv[0] - price) <= 0.0)]
            lvls.append([price, is_high, v[i] * c[i]])
            if len(lvls) > 150:
                lvls.pop(0)
        n_hi = n_lo = 0
        usd_hi = usd_lo = 0.0
        seen = 0
        for lv in reversed(lvls):
            if seen >= LW_MAX_LEVELS:
                break
            seen += 1
            valid = (lv[0] > c[i]) if lv[1] else (lv[0] < c[i])
            if valid:
                if lv[1]:
                    n_hi += 1
                    usd_hi += lv[2]
                else:
                    n_lo += 1
                    usd_lo += lv[2]
        tot = n_hi + n_lo
        if tot > 0:
            sell[i] = n_hi / tot * 100
            buy[i] = n_lo / tot * 100
        if i == n - 1:
            final = (n_lo, n_hi, usd_lo, usd_hi)

    # Positionsstatus (1:1 wie im Pine-Skript): TP zuerst pruefen, dann Einstieg
    pos = 0
    tp1_done = tp2_done = False
    entry_px = None
    entry_idx = None
    end_kind = None
    end_idx = None
    for i in range(n):
        if buy[i] is None:
            continue
        if pos == 1:
            if not tp1_done and buy[i] >= LW_TP1:
                tp1_done = True
            if not tp2_done and buy[i] >= LW_TP2:
                tp2_done = True
                pos = 0
                end_kind, end_idx = "tp2", i
        elif pos == -1:
            if not tp1_done and sell[i] >= LW_TP1:
                tp1_done = True
            if not tp2_done and sell[i] >= LW_TP2:
                tp2_done = True
                pos = 0
                end_kind, end_idx = "tp2", i
        if buy[i] <= LW_ENTRY and pos != 1:
            pos, entry_px, entry_idx, tp1_done, tp2_done, end_kind, end_idx = 1, c[i], i, False, False, None, None
        elif sell[i] <= LW_ENTRY and pos != -1:
            pos, entry_px, entry_idx, tp1_done, tp2_done, end_kind, end_idx = -1, c[i], i, False, False, None, None

    last_i = n - 1
    n_lo, n_hi, usd_lo, usd_hi = final
    tot = n_lo + n_hi
    return {"buyers": _r(buy[last_i], 1), "sellers": _r(sell[last_i], 1), "n_buy": n_lo, "n_sell": n_hi, "n": tot,
            "usd_below": _r(usd_lo, 0), "usd_above": _r(usd_hi, 0),
            "pos": "long" if pos == 1 else "short" if pos == -1 else "none",
            "tp1": tp1_done, "tp2": tp2_done,
            "since": (last_i - entry_idx) if entry_idx is not None else None,
            "ended": end_kind, "ended_ago": (last_i - end_idx) if end_idx is not None else None}


# ============================================================================
# MO7
# ============================================================================
def _mo7(o, h, l, c, v):
    n = len(c)
    if n < 120:
        return None
    rsi = compute_rsi(c, MO7_LEN)
    stoch = _stoch_series(h, l, c, MO7_LEN)       # WPR(14)+100 ist mathematisch identisch zu Stochastic %K(14)
    mfi = compute_mfi(h, l, c, v, MO7_LEN)
    ef = _ema_series(c, MO7_MACD_FAST)
    es = _ema_series(c, MO7_MACD_SLOW)
    macd = [ef[i] - es[i] for i in range(n)]
    roc = [0.0] + [((c[i] - c[i - 1]) / c[i - 1] * 100) if c[i - 1] else 0.0 for i in range(1, n)]
    ema_t = _ema_series(c, MO7_TREND_EMA)
    atr = compute_atr(h, l, c, MO7_LEN)

    def at(i):
        pr = _percentrank(c, i, 100)
        if pr is None:
            return None
        s = max(0, i - MO7_LOOKBACK + 1)
        wm = macd[s:i + 1]
        mn, mx = min(wm), max(wm)
        m_norm = (macd[i] - mn) / (mx - mn) * 100 if mx != mn else 50.0
        wr = roc[s:i + 1]
        rn, rx = min(wr), max(wr)
        r_norm = (roc[i] - rn) / (rx - rn) * 100 if rx != rn else 50.0
        parts = [rsi[i], stoch[i], stoch[i], mfi[i], m_norm, r_norm, pr]
        return sum(parts) / 7.0, parts

    start = max(101, n - MO7_SCAN)
    series = {}
    for i in range(start - 1, n):
        res = at(i)
        if res is not None:
            series[i] = res
    last_closed = n - 2
    events = []
    for i in range(start, last_closed + 1):
        if i not in series or (i - 1) not in series:
            continue
        cur, prev = series[i][0], series[i - 1][0]
        trend_ok = c[i] > ema_t[i]
        if cur > MO7_BUY and prev <= MO7_BUY and trend_ok:
            events.append({"kind": "buy", "idx": i})
        elif cur < MO7_SELL and prev >= MO7_SELL and trend_ok:
            events.append({"kind": "sell", "idx": i})
    if (n - 1) not in series:
        return None
    val, parts = series[n - 1]
    last = events[-1] if events else None
    return {"value": _r(val, 1), "buy": MO7_BUY, "sell": MO7_SELL,
            "status": "oversold" if val < MO7_BUY else "overbought" if val > MO7_SELL else "neutral",
            "trend_ok": bool(c[-1] > ema_t[-1]), "vola": _r(atr[-1] / c[-1] * 100, 2) if c[-1] else None,
            "parts": [_r(x, 0) for x in parts],
            "last": {"kind": last["kind"], "ago": _event_age(last["idx"], last_closed)} if last else None}


# ============================================================================
# ZUSAMMENBAU PRO ZEITRAHMEN / COIN
# ============================================================================
def compute_tf(candles):
    """candles: Liste von Dicts (ts,o,h,l,c,v,tb,n) - die letzte Kerze laeuft noch. Gibt ein JSON-faehiges Dict zurueck."""
    n = len(candles)
    if n < MIN_CANDLES:
        return {"loading": True, "n": n}
    o = [x["o"] for x in candles]
    h = [x["h"] for x in candles]
    l = [x["l"] for x in candles]
    c = [x["c"] for x in candles]
    v = [x["v"] for x in candles]
    out = {"loading": False, "n": n, "price": c[-1]}

    def safe(name, fn, *a):
        try:
            return fn(*a)
        except Exception as e:
            now = time.time()
            if now - _last_err_log.get(name, 0) > 300:
                _last_err_log[name] = now
                debug_log(f"⚠️ [Screener] Berechnung '{name}' fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
            return None

    out["mf"] = safe("moneyflow", _moneyflow, candles)
    rsi = safe("rsi", compute_rsi, c, 14)
    out["rsi"] = _r(rsi[-1], 1) if rsi else None
    out["macd"] = safe("macd", _macd, c)
    if out["macd"]:
        out["macd"] = {"bull": out["macd"]["bull"], "strength": out["macd"]["strength"]}
    adx = safe("adx", compute_adx, h, l, c, 14)
    out["adx"] = {"value": _r(adx[0][-1], 1), "up": adx[1][-1] > adx[2][-1]} if adx else None
    out["wa"] = safe("wellenanker", _wellenanker, o, h, l, c, v)
    closed = slice(0, n - 1)  # Liquidity Waves nur auf geschlossenen Kerzen
    out["lw"] = safe("liquidity_waves", _liquidity_waves, o[closed], h[closed], l[closed], c[closed], v[closed])
    out["mo7"] = safe("mo7", _mo7, o, h, l, c, v)
    return out


def _compute_coin(tf_candles):
    """Laeuft im Thread (rein CPU): {tf: candles|None} -> {tf: ergebnis|None}"""
    result = {}
    for tf, candles in tf_candles.items():
        result[tf] = compute_tf(candles) if candles else None
    return result


async def screener_loop():
    """In main.py's asyncio.gather einhaengen. Rechnet alle SCREENER_INTERVAL Sekunden jeden Coin
    in allen Zeitrahmen neu - ausschliesslich aus dem WS-Cache."""
    await asyncio.sleep(5)  # Bot-Start (Seeds der anderen Strategien) zuerst durchlaufen lassen
    while True:
        t0 = time.time()
        for sym in SCREENER_COINS:
            try:
                pair = BINANCE_SYMBOL_MAP.get(sym)
                if not pair:
                    SCREENER_STATE["coins"][sym] = {"coin": sym, "available": False}
                    continue
                market = _market_for(sym)
                tf_candles = {}
                for tf in TIMEFRAMES:
                    binance_ws.ensure_subscribed(market, pair, tf)
                    tf_candles[tf] = binance_ws.get_cached_candles_ext(market, pair, tf, CANDLES_BACK[tf])
                res = await asyncio.to_thread(_compute_coin, tf_candles)
                price = None
                for tf in TIMEFRAMES:  # kleinster verfuegbarer Zeitrahmen = frischester Preis
                    if tf_candles[tf]:
                        price = tf_candles[tf][-1]["c"]
                        break
                chg24 = None
                h1 = tf_candles.get("1h")
                if h1 and len(h1) >= 25 and h1[-25]["c"]:
                    chg24 = (h1[-1]["c"] / h1[-25]["c"] - 1) * 100
                SCREENER_STATE["coins"][sym] = {
                    "coin": sym, "available": True, "market": market, "price": price, "chg24": _r(chg24, 2),
                    "tfs": res, "ready": sum(1 for x in res.values() if x and not x.get("loading")),
                }
            except Exception as e:
                now = time.time()
                if now - _last_err_log.get("loop:" + sym, 0) > 300:
                    _last_err_log["loop:" + sym] = now
                    debug_log(f"⚠️ [{sym}] Screener-Runde fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
            await asyncio.sleep(0)
        SCREENER_STATE["updated"] = time.time()
        await asyncio.sleep(max(1.0, SCREENER_INTERVAL - (time.time() - t0)))


# ============================================================================
# WEB: Seite + API
# ============================================================================
async def handle_screener_api(request):
    coins = [SCREENER_STATE["coins"].get(s, {"coin": s, "available": True, "price": None, "tfs": {}, "ready": 0})
             for s in SCREENER_COINS]
    return web.json_response({"updated": SCREENER_STATE["updated"], "timeframes": TIMEFRAMES, "coins": coins})


async def handle_screener_index(request):
    return web.Response(text=SCREENER_HTML, content_type="text/html")


SCREENER_HTML = r"""<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Coin Screener</title>
<style>
*{box-sizing:border-box}
body{margin:0;background:#07080b;color:#e9ecf1;font-family:Inter,system-ui,-apple-system,Segoe UI,sans-serif}
.mono{font-family:'JetBrains Mono',ui-monospace,Menlo,Consolas,monospace}
header{display:flex;justify-content:space-between;align-items:center;padding:18px 28px;border-bottom:1px solid #1a1e26}
header h1{margin:0;font-size:18px;font-weight:700}
header nav a{color:#7d8696;text-decoration:none;font-size:13px;margin-left:18px}
header nav a:hover{color:#fff}
#status{font-size:12px;color:#7d8696;margin-left:16px}
#grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(520px,1fr));gap:22px;padding:24px 28px}
.tile{background:linear-gradient(180deg,#171a21 0%,#12151b 100%);border:1px solid #242932;border-radius:16px;padding:20px 22px;display:flex;flex-direction:column;gap:14px;box-shadow:0 10px 30px rgba(0,0,0,.4);cursor:pointer;transition:border-color .15s}
.tile:hover{border-color:#3a4150}
.tile.off{opacity:.55;cursor:default}
.row{display:flex;flex-direction:row}
.between{justify-content:space-between}
.center{align-items:center}
.coin{font-size:26px;font-weight:700;color:#fff;letter-spacing:-.01em}
.pill{font-size:12px;font-weight:600;padding:3px 9px;border-radius:8px;background:#232830;color:#aab2c0}
.price{font-size:14px;font-weight:500;color:#7d8696}
.tfbar{display:flex;gap:4px;padding:4px;background:#0d1015;border:1px solid #1f242c;border-radius:12px}
.tfbar button{flex:1;border:0;background:transparent;color:#7d8696;font:600 12px Inter,system-ui,sans-serif;padding:8px 0;border-radius:9px;cursor:pointer}
.tfbar button.act{background:#2a303a;color:#fff}
.rings{display:flex;justify-content:space-between;gap:12px}
.ring{flex:1;min-width:0;display:flex;flex-direction:column;align-items:center;gap:8px}
.ringbox{position:relative;width:104px;height:104px}
.ringbox .mid{position:absolute;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:2px}
.ringbox .val{font-size:24px;font-weight:600;letter-spacing:-.01em}
.cap{font-size:9px;font-weight:600;letter-spacing:.1em;text-transform:uppercase;color:#7d8696}
.ring .title{font-size:11px;font-weight:600;letter-spacing:.1em;text-transform:uppercase;color:#7d8696}
.bar{display:flex;flex-direction:column;gap:11px;padding:13px 15px 14px;background:#0d1015;border:1px solid #1f242c;border-radius:12px}
.bar .name{font-size:13px;font-weight:700;color:#fff}
.chip{font-size:11px;font-weight:700;padding:5px 12px;border-radius:20px}
.stats{display:flex;gap:9px}
.stat{flex:1;display:flex;flex-direction:column;gap:3px;padding:8px 11px;background:#12151b;border-radius:10px}
.stat b{font-size:15px}
.track{position:relative;height:34px}
.track .base{position:absolute;left:0;right:0;top:14px;height:6px;border-radius:3px;background:#1d2128}
.track .z{position:absolute;top:14px;height:6px}
.dot{position:absolute;border-radius:50%;box-sizing:border-box}
.scale{display:flex;justify-content:space-between;font-size:10px;color:#5b6472}
.states{display:flex;gap:6px}
.states div{flex:1;display:flex;flex-direction:column;align-items:center;gap:3px;padding:7px 0;background:#12151b;border-radius:9px}
.states .t{font-size:10px;font-weight:600;letter-spacing:.08em;color:#7d8696}
.states .s{font-size:10px;font-weight:700}
.note{font-size:11px;color:#7d8696}
.split{height:14px;border-radius:7px;background:#1d2128;display:flex;overflow:hidden;gap:2px;position:relative}
.loading{padding:40px 0;text-align:center;color:#7d8696;font-size:13px}
#modal{position:fixed;inset:0;background:rgba(0,0,0,.7);display:none;align-items:flex-start;justify-content:center;padding:40px 16px;overflow:auto;z-index:10}
#modal .box{background:#12151b;border:1px solid #242932;border-radius:16px;padding:22px 24px;width:100%;max-width:880px}
#modal table{width:100%;border-collapse:collapse;font-size:13px}
#modal th,#modal td{padding:8px 10px;text-align:right;border-bottom:1px solid #1d2128}
#modal th:first-child,#modal td:first-child{text-align:left;color:#7d8696}
#modal th{color:#aab2c0;font-weight:600}
#modal .sec td{padding-top:16px;color:#fff;font-weight:700;border-bottom:1px solid #2a303a}
.x{background:none;border:0;color:#7d8696;font-size:20px;cursor:pointer}
@media(max-width:600px){#grid{grid-template-columns:1fr;padding:14px}.ringbox{width:84px;height:84px}.ringbox svg{width:84px;height:84px}}
</style>
</head>
<body>
<header>
  <h1>Coin Screener <span id="status"></span></h1>
  <nav><a href="/">Bot-Dashboard</a><a href="/copytrading">Copy-Trading</a></nav>
</header>
<div id="grid"><div class="loading">Lade ...</div></div>
<div id="modal"><div class="box" id="modalbox"></div></div>
<script>
const G='#1fcf6e',R='#f0354b',Y='#f5b83d',B='#8aa4d6',GR='#7d8696',TRK='#262b35';
const GL={[G]:'rgba(31,207,110,.35)',[R]:'rgba(240,53,75,.35)',[Y]:'rgba(245,184,61,.3)',[B]:'rgba(138,164,214,.25)',[GR]:'rgba(125,134,150,.2)'};
const TF_LABEL={'1m':'1 min','5m':'5 min','15m':'15 min','1h':'1 Std','4h':'4 Std'};
const CIRC=2*Math.PI*44;
let DATA=null, TFS=['1m','5m','15m','1h','4h'], SEL={};
try{SEL=JSON.parse(localStorage.getItem('screener_tf')||'{}')}catch(e){}
function saveSel(){try{localStorage.setItem('screener_tf',JSON.stringify(SEL))}catch(e){}}
const clamp=(v,a,b)=>Math.max(a,Math.min(b,v));
const dash=p=>(CIRC*clamp(p,0,100)/100).toFixed(1)+' '+CIRC.toFixed(1);
function fmtPrice(p){if(p==null)return '–';const a=Math.abs(p);const d=a>=1000?2:a>=10?3:a>=1?4:6;return p.toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d})}
function fmtUsd(v){if(v==null)return '–';const a=Math.abs(v),s=v<0?'-':'';if(a>=1e9)return s+'$'+(a/1e9).toFixed(2)+'B';if(a>=1e6)return s+'$'+(a/1e6).toFixed(1)+'M';if(a>=1e3)return s+'$'+(a/1e3).toFixed(1)+'K';return s+'$'+a.toFixed(0)}
const sgn=(v,d=1)=>v==null?'–':(v>0?'+':'')+v.toFixed(d);
function ago(n){return n===0?'auf der letzten Kerze':n===1?'vor 1 Kerze':'vor '+n+' Kerzen'}

function ring(title,value,sub,color,arc,track){
  return `<div class="ring"><div class="ringbox"><svg width="104" height="104" viewBox="0 0 104 104" style="filter:drop-shadow(0 0 7px ${GL[color]||'transparent'})">
  <circle cx="52" cy="52" r="44" fill="none" stroke="${track||TRK}" stroke-width="7"/>
  <circle cx="52" cy="52" r="44" fill="none" stroke="${color}" stroke-width="7" stroke-linecap="round" stroke-dasharray="${dash(arc)}" transform="rotate(-90 52 52)"/></svg>
  <div class="mid"><div class="val" style="color:${color}">${value}</div><div class="cap">${sub}</div></div></div><div class="title">${title}</div></div>`;
}
function rings(d){
  const out=[];
  const mf=d.mf;
  if(mf&&mf.buyers!=null){const buy=mf.buyers>=50;const p=buy?mf.buyers:100-mf.buyers;
    out.push(ring('Moneyflow',p.toFixed(0)+'%',buy?'Buyers':'Sellers',buy?G:R,p,buy?R:G));}
  else out.push(ring('Moneyflow','–','keine Daten',GR,0));
  const rsi=d.rsi;
  if(rsi!=null){const col=rsi>=70?R:rsi<=30?G:B;out.push(ring('RSI',rsi.toFixed(0),rsi>=70?'Überkauft':rsi<=30?'Überverkauft':'Neutral',col,rsi));}
  else out.push(ring('RSI','–','',GR,0));
  const m=d.macd;
  if(m){out.push(ring('MACD',m.bull?'▲':'▼',m.bull?'Bullish':'Bearish',m.bull?G:R,m.strength));}
  else out.push(ring('MACD','–','',GR,0));
  const a=d.adx;
  if(a&&a.value!=null){const v=a.value;const col=v<20?GR:v<40?Y:G;
    const lab=(v<20?'Seitwärts':v<40?'Trend':'Stark')+(v>=20?(a.up?' ▲':' ▼'):'');
    out.push(ring('ADX',v.toFixed(0),lab,col,v));}
  else out.push(ring('ADX','–','',GR,0));
  return `<div class="rings">${out.join('')}</div>`;
}
const posPct=v=>((clamp(v,-100,100)+100)/2).toFixed(1)+'%';
function waBar(c,tf){
  const w=c.tfs[tf]&&c.tfs[tf].wa;
  if(!w)return `<div class="bar"><div class="name">Wellenanker</div><div class="note">noch keine Daten</div></div>`;
  const l=w.last,fresh=l&&l.ago<=3;
  let chip='Kein Punkt',col=GR,bg='#1d2128';
  if(fresh){const lg=l.kind==='long';col=lg?G:R;bg=lg?'rgba(31,207,110,.14)':'rgba(240,53,75,.14)';
    chip=(l.strong?'◉ Starker ':l.ext?'◆ Extrem ':'● ')+(lg?'Long':'Short')+(l.strong||l.ext?'':'-Punkt');}
  const dcol=w.diff>0?(w.wt2<-w.z1?G:GR):(w.wt2>w.z1?R:GR);
  const lo=Math.min(w.wt1,w.wt2),hi=Math.max(w.wt1,w.wt2);
  const zw=((100-w.z1)/2).toFixed(1)+'%';
  const states=TFS.map(t=>{const x=c.tfs[t]&&c.tfs[t].wa;const s=x?x.state:null;
    return `<div><span class="t">${t}</span><span class="s" style="color:${s===1?G:s===-1?R:GR}">${s===1?'LONG':s===-1?'SHORT':s===null?'–':'ABWARTEN'}</span></div>`}).join('');
  const lastTxt=l?('Letzter Punkt: '+(l.kind==='long'?'Long ':'Short ')+ago(l.ago)+(w.prev?' · davor '+(w.prev.kind==='long'?'Long ':'Short ')+ago(w.prev.ago):'')):'Noch kein Punkt in den letzten Kerzen';
  return `<div class="bar">
  <div class="row between center"><div class="row center" style="gap:8px"><span class="name">Wellenanker</span><span class="cap">WaveTrend</span></div>
  <span class="chip" style="background:${bg};color:${col};box-shadow:0 0 12px ${GL[col]||'transparent'}">${chip}</span></div>
  <div class="track"><div class="base"></div>
   <div class="z" style="left:0;width:${zw};border-radius:3px 0 0 3px;background:rgba(31,207,110,.28)"></div>
   <div class="z" style="right:0;width:${zw};border-radius:0 3px 3px 0;background:rgba(240,53,75,.28)"></div>
   <div style="position:absolute;left:50%;top:10px;width:1px;height:14px;background:#3a4150"></div>
   <div style="position:absolute;left:${posPct(lo)};width:${((clamp(hi,-100,100)-clamp(lo,-100,100))/2).toFixed(1)}%;top:15px;height:4px;border-radius:2px;background:${dcol}"></div>
   <div class="dot" style="left:${posPct(w.wt2)};top:11px;width:12px;height:12px;margin-left:-6px;border:2px solid #aab2c0;background:#12151b"></div>
   <div class="dot" style="left:${posPct(w.wt1)};top:9px;width:16px;height:16px;margin-left:-8px;background:#fff;box-shadow:0 0 10px rgba(255,255,255,.5)"></div></div>
  <div class="scale mono"><span>−100</span><span style="color:${G}">−${w.z1} Long-Zone</span><span>0</span><span style="color:${R}">Short-Zone +${w.z1}</span><span>+100</span></div>
  <div class="stats"><div class="stat"><span class="cap">WT1 Welle</span><b class="mono">${sgn(w.wt1)}</b></div>
   <div class="stat"><span class="cap">WT2 Signal</span><b class="mono" style="color:#aab2c0">${sgn(w.wt2)}</b></div>
   <div class="stat"><span class="cap">Differenz</span><b class="mono" style="color:${dcol}">${sgn(w.diff)}</b></div></div>
  <div class="states">${states}</div><div class="note">${lastTxt}</div></div>`;
}
function lwBar(c,tf){
  const w=c.tfs[tf]&&c.tfs[tf].lw;
  if(!w||w.buyers==null)return `<div class="bar"><div class="name">Liquidity Waves</div><div class="note">noch keine Levels</div></div>`;
  const lg=w.pos==='long',sh=w.pos==='short';
  const col=lg?G:sh?R:GR,bg=lg?'rgba(31,207,110,.14)':sh?'rgba(240,53,75,.14)':'#1d2128';
  const chip=lg?'LONG aktiv':sh?'SHORT aktiv':(w.ended==='tp2'?'TP2 erreicht':'Kein Signal');
  const stp=(lab,txt,cc)=>`<div><span class="t">${lab}</span><span class="s" style="color:${cc}">${txt}</span></div>`;
  const steps=(lg||sh)?stp('EINSTIEG',(lg?'BUY':'SELL')+(w.since!=null?' · '+w.since+'K':''),col)+stp('TP1 ≥ 50%',w.tp1?'✓ erreicht':'offen',w.tp1?col:GR)+stp('TP2 ≥ 85%',w.tp2?'✓ erreicht':'offen',w.tp2?col:GR)
    :stp('EINSTIEG','BUY ≤ 5% / SELL ≤ 5%',GR)+stp('TP1 ≥ 50%','–',GR)+stp('TP2 ≥ 85%',w.ended==='tp2'?'✓ erreicht':'–',w.ended==='tp2'?G:GR);
  return `<div class="bar">
  <div class="row between center"><div class="row center" style="gap:8px"><span class="name">Liquidity Waves</span><span class="cap">Levels ${w.n}</span></div>
  <span class="chip" style="background:${bg};color:${col};box-shadow:0 0 12px ${GL[col]||'transparent'}">${chip}</span></div>
  <div class="row between" style="align-items:baseline">
   <div class="row" style="gap:8px;align-items:baseline"><b class="mono" style="font-size:20px;color:${G}">${w.buyers.toFixed(1)}%</b><span class="cap">Buyers · ${w.n_buy}/${w.n}</span></div>
   <div class="row" style="gap:8px;align-items:baseline"><span class="cap">Sellers · ${w.n_sell}/${w.n}</span><b class="mono" style="font-size:20px;color:${R}">${w.sellers.toFixed(1)}%</b></div></div>
  <div style="position:relative"><div class="split"><div style="width:${w.buyers}%;background:linear-gradient(90deg,#16a85a,#1fcf6e)"></div><div style="flex:1;background:linear-gradient(90deg,#c42a40,#f0354b)"></div></div>
   <div style="position:absolute;left:5%;top:-3px;width:1px;height:20px;background:#e9ecf1;opacity:.55"></div>
   <div style="position:absolute;left:95%;top:-3px;width:1px;height:20px;background:#e9ecf1;opacity:.55"></div></div>
  <div class="scale mono"><span>BUY ≤ 5%</span><span>unter Kurs ${fmtUsd(w.usd_below)} · über Kurs ${fmtUsd(w.usd_above)}</span><span>SELL ≤ 5%</span></div>
  <div class="states">${steps}</div></div>`;
}
function moBar(c,tf){
  const m=c.tfs[tf]&&c.tfs[tf].mo7;
  if(!m)return `<div class="bar"><div class="name">MO7</div><div class="note">zu wenig Kerzen (braucht ~120)</div></div>`;
  const v=m.value,os=m.status==='oversold',ob=m.status==='overbought';
  const col=os?G:ob?R:'#aab2c0';
  const l=m.last,fresh=l&&l.ago<=3;
  const chip=fresh?(l.kind==='buy'?'BUY-Signal':'SELL-Signal'):'Kein Signal';
  const ccol=fresh?(l.kind==='buy'?G:R):GR,cbg=fresh?(l.kind==='buy'?'rgba(31,207,110,.14)':'rgba(240,53,75,.14)'):'#1d2128';
  const names=['RSI','STOCH','WPR','MFI','MACD','ROC','PR'];
  const parts=m.parts.map((x,i)=>`<div style="display:flex;flex-direction:column;align-items:center;gap:3px;padding:7px 0;background:#12151b;border-radius:9px"><span class="cap" style="font-size:9px">${names[i]}</span><b class="mono" style="font-size:12px;color:${x<20?G:x>85?R:'#e9ecf1'}">${x}</b></div>`).join('');
  const lastTxt=l?('Letztes Signal: '+(l.kind==='buy'?'BUY ':'SELL ')+ago(l.ago)):'Kein Signal in den letzten Kerzen';
  return `<div class="bar">
  <div class="row between center"><div class="row center" style="gap:8px"><span class="name">MO7</span><span class="cap">Buy/Sell Signal</span></div>
  <span class="chip" style="background:${cbg};color:${ccol};box-shadow:0 0 12px ${GL[ccol]||'transparent'}">${chip}</span></div>
  <div class="row between" style="align-items:baseline">
   <div class="row" style="gap:8px;align-items:baseline"><b class="mono" style="font-size:20px;color:${col}">${v.toFixed(1)}</b><span class="cap" style="color:${col}">${os?'Überverkauft':ob?'Überkauft':'Neutral'}</span></div>
   <div class="row center" style="gap:8px"><span class="pill" style="color:${m.trend_ok?G:R}">Trend ${m.trend_ok?'BULL':'BEAR'}</span><span class="pill">Vola ${m.vola!=null?m.vola.toFixed(2)+'%':'–'}</span></div></div>
  <div class="track"><div class="base"></div>
   <div class="z" style="left:0;width:${m.buy}%;border-radius:3px 0 0 3px;background:rgba(31,207,110,.28)"></div>
   <div class="z" style="right:0;width:${100-m.sell}%;border-radius:0 3px 3px 0;background:rgba(240,53,75,.28)"></div>
   <div style="position:absolute;left:${m.buy}%;top:10px;width:1px;height:14px;background:#3a4150"></div>
   <div style="position:absolute;left:${m.sell}%;top:10px;width:1px;height:14px;background:#3a4150"></div>
   <div class="dot" style="left:${clamp(v,0,100)}%;top:9px;width:16px;height:16px;margin-left:-8px;background:#fff;box-shadow:0 0 10px rgba(255,255,255,.5)"></div></div>
  <div class="scale mono"><span>0</span><span style="color:${G}">Buy ↑ ${m.buy}</span><span style="color:${R}">Sell ↓ ${m.sell}</span><span>100</span></div>
  <div style="display:grid;grid-template-columns:repeat(7,minmax(0,1fr));gap:6px">${parts}</div>
  <div class="note">${lastTxt}</div></div>`;
}
function tile(c){
  if(!c.available)return `<div class="tile off"><div class="row between center"><span class="coin">${c.coin}</span></div><div class="loading">Keine Binance-Daten für diesen Coin</div></div>`;
  const tf=SEL[c.coin]||'15m';
  const d=c.tfs&&c.tfs[tf];
  const chg=c.chg24;
  const head=`<div class="row between" style="align-items:flex-start"><div style="display:flex;flex-direction:column;gap:6px">
    <div class="row center" style="gap:10px"><span class="coin">${c.coin}</span><span class="pill">${tf}</span></div>
    <span class="price mono">${fmtPrice(c.price)}</span></div>
    <div style="text-align:right"><div class="mono" style="font-size:13px;font-weight:700;color:${chg==null?GR:chg>=0?G:R}">${chg==null?'':sgn(chg,2)+'%'}</div><div class="cap" style="margin-top:4px">${chg==null?'':'24h'}</div></div></div>`;
  const bar=`<div class="tfbar">${TFS.map(t=>`<button data-coin="${c.coin}" data-tf="${t}" class="${t===tf?'act':''}">${TF_LABEL[t]}</button>`).join('')}</div>`;
  let body;
  if(!d||d.loading)body=`<div class="loading">Lade Kerzen … (${d?d.n:0}/60, WS-Cache wärmt auf)</div>`;
  else body=rings(d)+waBar(c,tf)+lwBar(c,tf)+moBar(c,tf);
  return `<div class="tile" data-coin="${c.coin}">${head}${bar}${body}</div>`;
}
function render(){
  if(!DATA)return;
  document.getElementById('grid').innerHTML=DATA.coins.map(tile).join('');
  document.getElementById('status').textContent=DATA.updated?'· Stand '+new Date(DATA.updated*1000).toLocaleTimeString('de-DE'):'· wartet auf erste Berechnung';
  if(openCoin)renderModal();
}
let openCoin=null;
function renderModal(){
  const c=DATA.coins.find(x=>x.coin===openCoin);if(!c)return;
  const cols=TFS.map(t=>c.tfs&&c.tfs[t]&&!c.tfs[t].loading?c.tfs[t]:null);
  const row=(label,f)=>`<tr><td>${label}</td>${cols.map(d=>`<td class="mono">${d?(f(d)??'–'):'…'}</td>`).join('')}</tr>`;
  const sec=t=>`<tr class="sec"><td colspan="${TFS.length+1}">${t}</td></tr>`;
  const col=(txt,cc)=>`<span style="color:${cc}">${txt}</span>`;
  document.getElementById('modalbox').innerHTML=`
  <div class="row between center" style="margin-bottom:12px"><div class="row center" style="gap:12px"><span class="coin">${c.coin}</span><span class="price mono">${fmtPrice(c.price)}</span></div><button class="x" id="closeModal">✕</button></div>
  <table><thead><tr><th></th>${TFS.map(t=>`<th>${TF_LABEL[t]}</th>`).join('')}</tr></thead><tbody>
  ${sec('Moneyflow (letzte '+MF_N+' Kerzen)')}
  ${row('Buyers %',d=>d.mf&&d.mf.buyers!=null?col(d.mf.buyers.toFixed(1)+'%',d.mf.buyers>=50?G:R):null)}
  ${row('Net Delta',d=>d.mf?col(fmtUsd(d.mf.net_usd),d.mf.net_usd>=0?G:R):null)}
  ${row('Volumen',d=>d.mf?fmtUsd(d.mf.vol_usd):null)}
  ${row('Trades',d=>d.mf?d.mf.trades.toLocaleString('en-US'):null)}
  ${sec('Trend & Momentum')}
  ${row('RSI (14)',d=>d.rsi!=null?col(d.rsi.toFixed(1),d.rsi>=70?R:d.rsi<=30?G:'#e9ecf1'):null)}
  ${row('MACD',d=>d.macd?col((d.macd.bull?'▲ ':'▼ ')+d.macd.strength+'%',d.macd.bull?G:R):null)}
  ${row('ADX (14)',d=>d.adx&&d.adx.value!=null?col(d.adx.value.toFixed(1)+(d.adx.value>=20?(d.adx.up?' ▲':' ▼'):''),d.adx.value<20?GR:d.adx.value<40?Y:G):null)}
  ${sec('Wellenanker')}
  ${row('WT1 / WT2',d=>d.wa?sgn(d.wa.wt1)+' / '+sgn(d.wa.wt2):null)}
  ${row('Differenz',d=>d.wa?sgn(d.wa.diff):null)}
  ${row('Zustand',d=>d.wa?col(d.wa.state===1?'LONG':d.wa.state===-1?'SHORT':'ABWARTEN',d.wa.state===1?G:d.wa.state===-1?R:GR):null)}
  ${row('Letzter Punkt',d=>d.wa&&d.wa.last?col((d.wa.last.kind==='long'?'Long':'Short')+(d.wa.last.strong?' ◉':d.wa.last.ext?' ◆':'')+' · '+d.wa.last.ago+'K',d.wa.last.kind==='long'?G:R):'–')}
  ${sec('Liquidity Waves')}
  ${row('Buyers / Sellers',d=>d.lw&&d.lw.buyers!=null?d.lw.buyers.toFixed(1)+' / '+d.lw.sellers.toFixed(1)+' %':null)}
  ${row('Position',d=>d.lw?col(d.lw.pos==='long'?'LONG':d.lw.pos==='short'?'SHORT':'–',d.lw.pos==='long'?G:d.lw.pos==='short'?R:GR):null)}
  ${row('TP1 / TP2',d=>d.lw?(d.lw.tp1?'✓':'·')+' / '+(d.lw.tp2?'✓':'·'):null)}
  ${sec('MO7')}
  ${row('MO7 Wert',d=>d.mo7?col(d.mo7.value.toFixed(1),d.mo7.status==='oversold'?G:d.mo7.status==='overbought'?R:'#e9ecf1'):null)}
  ${row('Trend (EMA 200)',d=>d.mo7?col(d.mo7.trend_ok?'BULL':'BEAR',d.mo7.trend_ok?G:R):null)}
  ${row('Letztes Signal',d=>d.mo7&&d.mo7.last?col((d.mo7.last.kind==='buy'?'BUY':'SELL')+' · '+d.mo7.last.ago+'K',d.mo7.last.kind==='buy'?G:R):'–')}
  </tbody></table>`;
  document.getElementById('modal').style.display='flex';
}
const MF_N=__MF_WINDOW__;
function closeModal(){openCoin=null;document.getElementById('modal').style.display='none'}
document.getElementById('modal').addEventListener('click',e=>{if(e.target.id==='modal'||e.target.id==='closeModal')closeModal()});
document.addEventListener('keydown',e=>{if(e.key==='Escape')closeModal()});
document.getElementById('grid').addEventListener('click',e=>{
  const b=e.target.closest('button[data-tf]');
  if(b){SEL[b.dataset.coin]=b.dataset.tf;saveSel();render();e.stopPropagation();return}
  const t=e.target.closest('.tile[data-coin]');
  if(t){openCoin=t.dataset.coin;renderModal()}
});
async function load(){
  try{const r=await fetch('/api/screener');if(r.ok){DATA=await r.json();if(DATA.timeframes)TFS=DATA.timeframes;render()}}catch(e){}
}
load();setInterval(load,5000);
</script>
</body>
</html>
""".replace("__MF_WINDOW__", str(MF_WINDOW))
