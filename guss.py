"""
guss.py - eigene Strategie "GUSS Sniperentry" (nach dem PDF "GUSS Sniperentry Strategie", Kryptomano).

Regeln (Long; Short ist exakt gespiegelt):
  1. ANKER: neues hoeheres Hoch (hoeher als die letzten `hh_lookback` Hochs), dessen Abstand zur 21 EMA mindestens
     `ext_atr` x ATR betraegt ("ueberdehnt").
  2. KORREKTUR: danach darf KEINE bullishe Kerze (Close > Open) mehr kommen, bis der Kurs die 50 EMA beruehrt.
     Eine einzige Gegenkerze macht das Setup ungueltig (Nutzer-Vorgabe: "jede Gegenkerze in dem Timeframe").
  3. ENTRY: erste Kerze, deren Tief die 50 EMA beruehrt (Limit-Kauf auf der 50 EMA). Die Beruehrungskerze selbst darf
     jede Farbe haben.
  4. FIB-KONFLUENZ: Fib-Level 0.5 / 0.559 / 0.618 / 0.667 / 0.786 des Aufwaertsbeins (Tief vor dem Anker -> Anker-Hoch);
     faellt die 50 EMA auf eines davon (Abstand <= `fib_tol_atr` x ATR), gibt es Bonuspunkte.
  5. SL unter dem Tief des Aufwaertsbeins (mindestens `min_risk_atr` x ATR Abstand), TP1 = 21 EMA (Teilgewinn
     `tp1_share`, danach SL auf Break-Even), TP2 = das Anker-Hoch.

Dieses Modul ist absichtlich eigenstaendig: der Kern (Indikatoren, Detektor, Score, Simulation) braucht keine
Bot-Importe und laesst sich ohne den restlichen Bot testen. Bot-Importe (bot_core/strategies) passieren erst
in den Web-/Backtest-Funktionen weiter unten.

Short-Spiegelung: alle Preise werden negiert (o'=-o, h'=-l, l'=-h, c'=-c). EMA ist linear, die ATR bleibt gleich,
dadurch ist ein Short-Setup in diesem Raum ein normales Long-Setup. Ergebnisse werden zurueckgespiegelt.
"""

import asyncio
import json
import os
import time
import traceback

# ============================================================================
# PARAMETER
# ============================================================================
DEFAULTS = {
    "ema_fast": 21, "ema_mid": 50, "ema_slow": 200, "atr_len": 14,
    "ext_atr": 2.5,          # Mindestabstand Hoch <-> 21 EMA in ATR (Startwert geschaetzt, per Backtest pruefen!)
    "hh_lookback": 20,       # "neues hoeheres Hoch" = hoeher als die letzten N Hochs; auch Suchfenster fuer das Tief davor
    "max_corr": 40,          # laenger als N Kerzen ohne Beruehrung der 50 EMA -> Setup verfaellt
    "fib_levels": [0.5, 0.559, 0.618, 0.667, 0.786],
    "fib_tol_atr": 0.15,     # so nah (in ATR) muss die 50 EMA an einem Fib-Level liegen
    "min_swing_atr": 1.0,    # Aufwaertsbein kleiner als N ATR -> keine Fibs
    "sl_buffer_atr": 0.1,    # SL = Tief des Beins - Puffer
    "min_risk_atr": 0.5,     # SL mindestens so weit (in ATR) vom Entry weg
    "tp1_share": 0.25,       # Anteil, der an der 21 EMA verkauft wird (PDF: z. B. 25 %)
    "max_hold": 96,          # Backtest: nach N Kerzen wird zum Schlusskurs bewertet
    "warmup": 150,           # EMAs sind erst nach ~150 Kerzen eingeschwungen
    "active_bars": 2,        # Setup gilt als "aktiv", solange die Beruehrung hoechstens so viele Kerzen zurueckliegt
}
PARAM_RANGES = {  # (min, max) fuer Eingaben von der Seite
    "ext_atr": (0.5, 10.0), "hh_lookback": (5, 100), "max_corr": (3, 200), "fib_tol_atr": (0.05, 2.0),
    "min_swing_atr": (0.2, 10.0), "sl_buffer_atr": (0.0, 2.0), "min_risk_atr": (0.1, 5.0),
    "tp1_share": (0.0, 1.0), "max_hold": (10, 1000),
}
INT_PARAMS = {"hh_lookback", "max_corr", "max_hold"}

TFS_ALL = ["5m", "15m", "30m", "1h"]
HTF = {"5m": "15m", "15m": "1h", "30m": "1h", "1h": "4h", "4h": None}   # uebergeordneter Zeitrahmen fuer den Trend-Bonus

# Score-Gewichte (Maximalpunkte je Faktor). "Basis" = nur aus dem Chart des Zeitrahmens berechenbar.
SCORE_MAX = {"ext": 20, "corr": 15, "fib": 25, "rr": 10, "htf": 15, "wa": 10, "liq": 5}
SCORE_BASE = ("ext", "corr", "fib", "rr")


def clean_params(body):
    """Nimmt nur bekannte Zahlenfelder aus `body`, begrenzt sie und liefert vollstaendige Parameter."""
    p = dict(DEFAULTS)
    for k, (lo, hi) in PARAM_RANGES.items():
        if k in body and body[k] not in (None, ""):
            try:
                v = float(str(body[k]).replace(",", "."))
            except (TypeError, ValueError):
                continue
            v = max(lo, min(hi, v))
            p[k] = int(round(v)) if k in INT_PARAMS else v
    return p


# ============================================================================
# INDIKATOREN (eigenstaendig, gleiche Formeln wie im Bot: EMA mit erstem Wert als Start, ATR nach Wilder)
# ============================================================================
def _ema(values, length):
    if not values:
        return []
    k = 2.0 / (length + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def _atr(h, l, c, period):
    n = len(c)
    if n == 0:
        return []
    tr = [h[0] - l[0]] + [max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1])) for i in range(1, n)]
    out = [tr[0]] * n
    for i in range(1, n):
        out[i] = sum(tr[:i + 1]) / (i + 1) if i < period else (out[i - 1] * (period - 1) + tr[i]) / period
    return out


def _indicators(c, h, l, p):
    return (_ema(c, p["ema_fast"]), _ema(c, p["ema_mid"]), _ema(c, p["ema_slow"]), _atr(h, l, c, p["atr_len"]))


def _flip(o, h, l, c):
    """Short-Spiegelung (siehe Modul-Docstring)."""
    return [-x for x in o], [-x for x in l], [-x for x in h], [-x for x in c]


# ============================================================================
# DETEKTOR (arbeitet immer im "Long-Raum")
# ============================================================================
def _make_event(o, h, l, c, ind, p, k, ih, corr, entry=None):
    """Baut ein Setup mit allen Kennzahlen. k = Beruehrungskerze (bzw. letzte Kerze bei einer Vorschau), ih = Anker-Kerze.
    Gibt None zurueck, wenn das Setup nicht sinnvoll handelbar ist (Entry unter dem Tief des Beins, 21 EMA nicht ueber der 50 EMA)."""
    e21, e50, e200, atr = ind
    a = atr[k]
    if a <= 0:
        return None
    entry = min(e50[k], o[k]) if entry is None else entry
    top = h[ih]
    lo_start = max(0, ih - p["hh_lookback"])
    # Aufwaertsbein: tiefstes Tief im selben Fenster, in dem das Anker-Hoch das hoechste ist (hh_lookback Kerzen)
    ll = min(range(lo_start, ih + 1), key=lambda j: l[j])
    low = l[ll]
    swing = top - low
    if entry <= low + 0.1 * a or e21[k] <= entry:
        return None
    ext = (top - e21[ih]) / atr[ih] if atr[ih] > 0 else 0.0
    fib = None
    if swing >= p["min_swing_atr"] * a:
        best = None
        for r in p["fib_levels"]:
            price = top - r * swing
            d = abs(price - entry) / a
            if best is None or d < best[2]:
                best = (r, price, d)
        if best and best[2] <= p["fib_tol_atr"]:
            fib = {"level": best[0], "price": best[1], "dist_atr": round(best[2], 3)}
    sl = min(low - p["sl_buffer_atr"] * a, entry - p["min_risk_atr"] * a)
    risk = entry - sl
    tp1 = e21[k]
    return {
        "i": k, "ih": ih, "entry": entry, "sl": sl, "tp1": tp1, "top": top, "low": low, "atr": a,
        "ext": round(ext, 2), "n_corr": corr, "retr": round((top - entry) / swing, 3) if swing > 0 else None,
        "fib": fib, "rr1": round((tp1 - entry) / risk, 2) if risk > 0 else 0.0,
        "trend200": bool(e50[k] > e200[k]),
    }


def _detect_long(o, h, l, c, p):
    """Laeuft einmal ueber alle (geschlossenen) Kerzen. -> (events, tail, ind)
    tail = Zustand am Ende: offener Anker (noch nicht beruehrt) bzw. juengste Ungueltigkeit."""
    n = len(c)
    ind = _indicators(c, h, l, p)
    e21, e50, e200, atr = ind
    lb = p["hh_lookback"]
    events = []
    anchor, corr = None, 0
    last_invalid = None
    for i in range(max(p["warmup"], lb + 1), n):
        a = atr[i]
        if a <= 0:
            continue
        if anchor is not None and h[i] > h[anchor]:
            anchor = None   # Anker-Hoch wurde ueberboten: Korrektur war keine, neu bewerten
        if h[i] > max(h[i - lb:i]) and (h[i] - e21[i]) / a >= p["ext_atr"]:
            anchor, corr = i, 0    # neues, ueberdehntes Hoch (auch als Ersatz fuer einen alten Anker)
            continue
        if anchor is None:
            continue
        if i - anchor > p["max_corr"]:
            last_invalid = {"idx": i, "anchor": anchor, "reason": "timeout"}
            anchor = None
            continue
        if l[i] <= e50[i]:                       # Beruehrung der 50 EMA = Entry
            ev = _make_event(o, h, l, c, ind, p, i, anchor, corr)
            if ev is not None:
                events.append(ev)
            anchor = None
            continue
        if c[i] > o[i]:                          # bullishe Gegenkerze vor der Beruehrung -> ungueltig
            last_invalid = {"idx": i, "anchor": anchor, "reason": "bullish"}
            anchor = None
            continue
        corr += 1
    tail = {"anchor": anchor, "corr": corr, "last_invalid": last_invalid, "n": n}
    return events, tail, ind


def _simulate_event(ev, o, h, l, c, ind, p):
    """Bewertet ein Setup im Long-Raum. R-Vielfache: 1R = Entry-SL.
      sl      : SL zuerst getroffen (-1R)
      tp1     : 21 EMA erreicht (danach SL auf Break-Even, Rest laeuft bis zum Anker-Hoch = TP2)
      timeout : nach max_hold Kerzen zum Schlusskurs bewertet
    Auf der Entry-Kerze zaehlt nur der SL (Reihenfolge innerhalb der Kerze ist unbekannt -> konservativ)."""
    e21 = ind[0]
    n = len(c)
    k, entry, sl, top = ev["i"], ev["entry"], ev["sl"], ev["top"]
    risk = entry - sl
    share = p["tp1_share"]
    if risk <= 0:
        return {"out": "invalid", "r_scalp": 0.0, "r_run": 0.0, "tp2": False, "bars": 0}
    if l[k] <= sl:
        return {"out": "sl", "r_scalp": -1.0, "r_run": -1.0, "tp2": False, "bars": 0}
    end = min(n - 1, k + p["max_hold"])
    j = k + 1
    while j <= end:
        if l[j] <= sl:
            return {"out": "sl", "r_scalp": -1.0, "r_run": -1.0, "tp2": False, "bars": j - k}
        if h[j] >= e21[j]:
            r1 = (max(e21[j], entry) - entry) / risk
            # Rest: SL auf Break-Even (ab der naechsten Kerze), Ziel = Anker-Hoch
            r2, tp2, m = 0.0, False, j + 1
            while m <= end:
                if l[m] <= entry:
                    break
                if h[m] >= top:
                    r2, tp2 = (top - entry) / risk, True
                    break
                m += 1
            else:
                r2 = (c[end] - entry) / risk       # Zeit abgelaufen: Rest zum Schlusskurs
            return {"out": "tp1", "r_scalp": r1, "r_run": share * r1 + (1 - share) * r2, "tp2": tp2, "bars": j - k}
        j += 1
    r = (c[end] - entry) / risk
    return {"out": "timeout", "r_scalp": r, "r_run": r, "tp2": False, "bars": end - k}


def _unflip_event(ev):
    """Preise eines Short-Setups (im gespiegelten Raum) zurueck in echte Preise."""
    out = dict(ev)
    for key in ("entry", "sl", "tp1", "top", "low"):
        out[key] = -ev[key]
    if ev.get("fib"):
        out["fib"] = dict(ev["fib"], price=-ev["fib"]["price"])
    return out


def detect_all(ts, o, h, l, c, p, simulate=True):
    """Alle Setups einer Kerzenreihe (nur GESCHLOSSENE Kerzen!), je Richtung. -> {"long": [...], "short": [...]}
    Jedes Setup enthaelt Zeit (ms), Kennzahlen, (optional) Ergebnis und die Basis-Bewertung."""
    res = {}
    for direction in ("long", "short"):
        if direction == "long":
            oo, hh, ll, cc = o, h, l, c
        else:
            oo, hh, ll, cc = _flip(o, h, l, c)
        events, tail, ind = _detect_long(oo, hh, ll, cc, p)
        out = []
        for ev in events:
            row = dict(ev)
            if simulate:
                row.update(_simulate_event(ev, oo, hh, ll, cc, ind, p))
            row["ts"] = ts[ev["i"]]
            row["dir"] = direction
            row["score_base"] = guss_score(ev, None, None, None)["score"]
            if direction == "short":
                row.update({k: v for k, v in _unflip_event(ev).items() if k in ("entry", "sl", "tp1", "top", "low", "fib")})
            out.append(row)
        res[direction] = out
    return res


# ============================================================================
# SCORE
# ============================================================================
def guss_score(ev, htf_ok, wa, rank, p=None):
    """Bewertung 0-100 aus Einzelfaktoren. Faktoren, die nicht bekannt sind (htf_ok/wa/rank = None), werden
    NICHT als 0 gewertet, sondern aus dem Maximum herausgenommen - so bleibt der Score vergleichbar.
      ext  : Ueberdehnung (Hoch-21EMA in ATR), 8..20 Pkt.
      corr : Anzahl sauberer (bearisher) Korrekturkerzen, bis 15
      fib  : 50 EMA genau auf einem Fib-Level bis 25 (je naeher, desto mehr); nur im 0.5-0.786-Bereich ohne Treffer: 6
      rr   : Chance-Risiko bis zur 21 EMA (>=2: 10, >=1: 7, >=0.5: 4)
      htf  : uebergeordneter Zeitrahmen im Aufwaertstrend (21>50 & Kurs>50: 10; 50>200: +5)
      wa   : Wellenanker zeigt in dieselbe Richtung (frisches Signal 10, nur Zustand 6)
      liq  : Liquiditaet des Coins (Rang 1-10: 5, 11-20: 3, sonst 1)
    htf_ok: dict {"ok": bool, "trend200": bool} oder None. wa: dict wie screener._wellenanker bereits auf die Richtung
    gedreht {"state_ok": bool, "fresh": bool} oder None."""
    p = p or DEFAULTS
    parts = {}
    ext_min = p["ext_atr"]
    frac = max(0.0, min(1.0, (ev["ext"] - ext_min) / 2.5))
    parts["ext"] = 8 + 12 * frac
    parts["corr"] = min(15, 3 + 3 * ev["n_corr"])
    if ev["fib"]:
        parts["fib"] = 25 * (1 - 0.5 * ev["fib"]["dist_atr"] / max(1e-9, p["fib_tol_atr"]))
    elif ev.get("retr") is not None and 0.5 <= ev["retr"] <= 0.8:
        parts["fib"] = 6
    else:
        parts["fib"] = 0
    rr = ev["rr1"]
    parts["rr"] = 10 if rr >= 2 else 7 if rr >= 1 else 4 if rr >= 0.5 else 1
    if htf_ok is not None:
        parts["htf"] = (10 if htf_ok.get("ok") else 0) + (5 if htf_ok.get("trend200") else 0)
    if wa is not None:
        parts["wa"] = 10 if wa.get("fresh") else 6 if wa.get("state_ok") else 0
    if rank is not None:
        parts["liq"] = 5 if rank <= 10 else 3 if rank <= 20 else 1
    got = sum(parts.values())
    mx = sum(SCORE_MAX[k] for k in parts)
    score = round(got / mx * 100) if mx else 0
    return {"score": score, "grade": "A" if score >= 75 else "B" if score >= 60 else "C" if score >= 45 else "D",
            "parts": {k: round(v, 1) for k, v in parts.items()}, "max": {k: SCORE_MAX[k] for k in parts}}


# ============================================================================
# LIVE-ZUSTAND EINES ZEITRAHMENS (fuer die spaetere Top-30-Seite; hier schon fertig und getestet)
# ============================================================================
def analyze_tf(candles, p=None, htf=None, wa=None, rank=None):
    """candles: Liste von Dicts (ts,o,h,l,c), die LETZTE Kerze laeuft noch (wie im WS-Cache). Gibt je Richtung zurueck:
      state: "none" | "overextended" (Hoch ueberdehnt, Korrektur hat noch nicht begonnen) | "coming" (Korrektur laeuft
             sauber auf die 50 EMA zu) | "active" (50 EMA beruehrt, Entry-Zone) | "invalid" (Gegenkerze kam dazwischen)
    htf = {"long": {...}, "short": {...}} bzw. None, wa = {"long": {...}, "short": {...}} bzw. None."""
    p = p or DEFAULTS
    if len(candles) < p["warmup"] + p["hh_lookback"] + 5:
        return {"loading": True, "n": len(candles)}
    closed, live = candles[:-1], candles[-1]
    ts = [x["ts"] for x in closed]
    o = [x["o"] for x in closed]; h = [x["h"] for x in closed]; l = [x["l"] for x in closed]; c = [x["c"] for x in closed]
    out = {"loading": False, "price": live["c"], "ts": ts[-1]}
    for direction in ("long", "short"):
        if direction == "long":
            oo, hh, ll, cc, live_low = o, h, l, c, live["l"]
        else:
            oo, hh, ll, cc = _flip(o, h, l, c)
            live_low = -live["h"]
        events, tail, ind = _detect_long(oo, hh, ll, cc, p)
        n = len(cc)
        e50 = ind[1]
        info = {"state": "none"}
        ev = None
        age = None
        if tail["anchor"] is not None:
            ia = tail["anchor"]
            if ia == n - 1:
                info["state"] = "overextended"
            else:
                info["state"] = "coming"
            ev = _make_event(oo, hh, ll, cc, ind, p, n - 1, ia, tail["corr"], entry=e50[n - 1]) if ia < n - 1 else None
            if ev is None and ia == n - 1:
                # Anker ist die letzte Kerze: Kennzahlen fuer die Vorschau (noch keine Korrektur)
                ev = _make_event(oo, hh, ll, cc, ind, p, n - 1, ia, 0, entry=e50[n - 1])
            if info["state"] == "coming" and live_low <= e50[n - 1]:
                info["state"] = "active"
                age = 0
        elif events and events[-1]["i"] >= n - 1 - p["active_bars"]:
            ev = events[-1]
            info["state"] = "active"
            age = n - 1 - ev["i"]
        elif tail["last_invalid"] and tail["last_invalid"]["idx"] >= n - 5:
            info["state"] = "invalid"
            info["reason"] = tail["last_invalid"]["reason"]
            info["ago"] = n - 1 - tail["last_invalid"]["idx"]
        if ev is not None and info["state"] in ("overextended", "coming", "active"):
            sc = guss_score(ev, (htf or {}).get(direction), (wa or {}).get(direction), rank, p)
            ev_out = _unflip_event(ev) if direction == "short" else ev
            info.update({"score": sc["score"], "grade": sc["grade"], "parts": sc["parts"], "max": sc["max"],
                         "entry": ev_out["entry"], "sl": ev_out["sl"], "tp1": ev_out["tp1"], "top": ev_out["top"],
                         "ext": ev["ext"], "n_corr": ev["n_corr"], "retr": ev["retr"], "rr1": ev["rr1"],
                         "fib": ev_out["fib"], "trend200": ev["trend200"],
                         "dist_atr": round((cc[-1] - e50[n - 1]) / ev["atr"], 2), "age": age,
                         "since": ts[ev["ih"]]})
        out[direction] = info
    return out


def htf_trend(candles, direction, p=None):
    """Trend-Check eines uebergeordneten Zeitrahmens (letzte geschlossene Kerze): {"ok", "trend200"} oder None."""
    p = p or DEFAULTS
    if not candles or len(candles) < 60:
        return None
    cl = [x["c"] for x in candles[:-1]]
    if direction == "short":
        cl = [-x for x in cl]
    e21, e50, e200 = _ema(cl, p["ema_fast"]), _ema(cl, p["ema_mid"]), _ema(cl, p["ema_slow"])
    return {"ok": bool(e21[-1] > e50[-1] and cl[-1] > e50[-1]), "trend200": bool(e50[-1] > e200[-1])}


# ============================================================================
# STATISTIK
# ============================================================================
def _mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


def stats(evs):
    n = len(evs)
    if n == 0:
        return {"n": 0}
    cnt = lambda name: sum(1 for e in evs if e["out"] == name)
    return {
        "n": n, "tp1_pct": round(cnt("tp1") / n * 100, 1), "sl_pct": round(cnt("sl") / n * 100, 1),
        "to_pct": round(cnt("timeout") / n * 100, 1), "tp2_pct": round(sum(1 for e in evs if e["tp2"]) / n * 100, 1),
        "avg_r_scalp": round(_mean([e["r_scalp"] for e in evs]), 3),
        "avg_r_run": round(_mean([e["r_run"] for e in evs]), 3),
        "avg_rr": round(_mean([e["rr1"] for e in evs]), 2),
        "avg_bars": round(_mean([e["bars"] for e in evs]), 1),
    }


def aggregate(events):
    """events: flache Liste der Setups (je mit coin, tf, dir, fib, score_base, Ergebnis)."""
    def group(keyfn):
        d = {}
        for e in events:
            d.setdefault(keyfn(e), []).append(e)
        return d
    buckets = [(0, 45), (45, 60), (60, 75), (75, 101)]
    names = ["< 45 (D)", "45-59 (C)", "60-74 (B)", ">= 75 (A)"]
    return {
        "all": stats(events),
        "by_tf": {k: stats(v) for k, v in sorted(group(lambda e: e["tf"]).items())},
        "by_dir": {k: stats(v) for k, v in sorted(group(lambda e: e["dir"]).items())},
        "by_fib": {("mit Fib-Konfluenz" if k else "ohne Fib-Konfluenz"): stats(v)
                   for k, v in sorted(group(lambda e: bool(e["fib"])).items(), reverse=True)},
        "by_score": {names[i]: stats([e for e in events if lo <= e["score_base"] < hi]) for i, (lo, hi) in enumerate(buckets)},
        "by_coin": {k: stats(v) for k, v in sorted(group(lambda e: e["coin"]).items())},
    }


# ============================================================================
# BACKTEST UEBER VIELE COINS (Bot-Importe erst hier, siehe Modul-Docstring)
# ============================================================================
GUSS_BT = {"running": False, "progress": {"done": 0, "total": 0, "current": ""}, "result": None, "error": None}
_BT_KEY = "gridbot:guss_bt"
_UNIVERSE = {"ts": 0.0, "coins": []}
_bt_task = None
UNIVERSE_SIZE = int(os.getenv("GUSS_TOP_N", "30"))


def _log(msg, data=None):
    try:
        from bot_core import debug_log
        debug_log(msg, data)
    except Exception:
        print(msg, data or "")


async def _binance_futures_pairs():
    """Alle handelbaren USDT-Paare auf Binance Futures (eine einzige Anfrage, Gewicht 1). None bei Fehler/Bann."""
    import aiohttp
    from strategies import _binance_is_banned, _binance_register_ban, _binance_note_response, _binance_throttle
    if _binance_is_banned("futures"):
        return None
    await _binance_throttle("futures", "guss-exchangeinfo")
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get("https://fapi.binance.com/fapi/v1/exchangeInfo", timeout=aiohttp.ClientTimeout(total=20)) as r:
                _binance_note_response("futures", r)
                if r.status in (418, 429):
                    _binance_register_ban("futures", "exchangeInfo", r.status, await r.text())
                    return None
                if r.status != 200:
                    return None
                data = await r.json()
    except Exception as e:
        _log("⚠️ GUSS: Binance-Futures-Liste fehlgeschlagen", {"error": str(e)})
        return None
    return {x["symbol"] for x in data.get("symbols", [])
            if x.get("status") == "TRADING" and x.get("quoteAsset") == "USDT" and "_" not in x["symbol"]}


async def _lighter_symbols():
    """Perp-Maerkte von Lighter in der von der API gelieferten Reihenfolge (nach Tagesvolumen sortiert, falls die API es mitliefert)."""
    import aiohttp
    from bot_core import BASE_URL
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{BASE_URL}/api/v1/orderBooks", timeout=aiohttp.ClientTimeout(total=15)) as r:
                data = await r.json(content_type=None)
    except Exception as e:
        _log("⚠️ GUSS: Lighter-Marktliste fehlgeschlagen", {"error": str(e)})
        return []
    rows = data.get("order_books") or data.get("orderBooks") or []
    vol_keys = ("daily_quote_token_volume", "daily_volume", "quote_volume_24h", "volume_24h")
    items = []
    for x in rows:
        sym = str(x.get("symbol", "")).upper()
        if not sym or not sym.isalnum() or str(x.get("market_type", "perp")).lower() == "spot":
            continue
        vol = next((float(x[k]) for k in vol_keys if x.get(k) not in (None, "") and str(x[k]).replace(".", "", 1).isdigit()), None)
        items.append((sym, vol))
    if any(v is not None for _, v in items):
        items.sort(key=lambda t: -(t[1] or 0.0))
    return [s for s, _ in items]


async def guss_universe(force=False):
    """Die Coins fuer Screener/Backtest: Lighter-Maerkte, die es auf Binance Futures gibt (dort kommen die Kerzen her),
    hoechstens GUSS_TOP_N. Eigene Liste per Umgebungsvariable GUSS_COINS="BTC,ETH,..." moeglich. 1 Stunde im Speicher."""
    if not force and _UNIVERSE["coins"] and time.time() - _UNIVERSE["ts"] < 3600:
        return _UNIVERSE["coins"]
    from strategies import BINANCE_SYMBOL_MAP
    from bot_core import MARKET_INDICES
    env = [s.strip().upper() for s in os.getenv("GUSS_COINS", "").split(",") if s.strip()]
    cand = env or await _lighter_symbols() or [s for s in MARKET_INDICES if s in BINANCE_SYMBOL_MAP]
    pairs = await _binance_futures_pairs()
    coins = []
    for sym in cand:
        pair = None
        if pairs is not None:
            pair = next((x for x in (f"{sym}USDT", f"1000{sym}USDT") if x in pairs), None)
        elif sym in BINANCE_SYMBOL_MAP:
            pair = BINANCE_SYMBOL_MAP[sym]
        if pair:
            BINANCE_SYMBOL_MAP.setdefault(sym, pair)
            coins.append(sym)
        if len(coins) >= UNIVERSE_SIZE:
            break
    if coins:
        _UNIVERSE.update({"ts": time.time(), "coins": coins})
    return coins or _UNIVERSE["coins"]


def _compute_cell(coin, tf, candles, p):
    """Rein CPU (laeuft im Thread): Setups beider Richtungen einer Kerzenreihe."""
    ts, o, h, l, c = candles[0][:-1], candles[1][:-1], candles[2][:-1], candles[3][:-1], candles[4][:-1]   # laufende Kerze weg
    found = detect_all(ts, o, h, l, c, p)
    flat = []
    for d in ("long", "short"):
        for e in found[d]:
            if e.get("out") == "invalid":
                continue
            e["coin"], e["tf"] = coin, tf
            flat.append(e)
    return flat, len(ts), (ts[-1] - ts[0]) / 86_400_000 if len(ts) > 1 else 0


def _slim(e):
    """Kompakte Form eines Setups fuer die Ergebnisliste (gleiche Felder wie fuer die Statistik + Anzeige)."""
    return {"coin": e["coin"], "tf": e["tf"], "dir": e["dir"], "ts": e["ts"], "entry": e["entry"], "sl": e["sl"],
            "tp1": e["tp1"], "top": e["top"], "ext": e["ext"], "n_corr": e["n_corr"], "retr": e["retr"],
            "fib": e["fib"], "rr1": e["rr1"], "score_base": e["score_base"], "out": e["out"],
            "r_scalp": round(e["r_scalp"], 3), "r_run": round(e["r_run"], 3), "tp2": e["tp2"], "bars": e["bars"]}


async def _bt_job(coins, tfs, days, p):
    from strategies import fetch_historical_candles_binance_vol
    st = GUSS_BT
    st.update({"running": True, "error": None, "result": None,
               "progress": {"done": 0, "total": len(coins) * len(tfs), "current": ""}})
    events, cells, errors = [], {}, {}
    started = time.time()
    try:
        for coin in coins:
            for tf in tfs:
                st["progress"]["current"] = f"{coin} {tf}"
                key = f"{coin}|{tf}"
                try:
                    candles, err = await fetch_historical_candles_binance_vol(coin, tf, days, 100_000, market_type="futures")
                except Exception as e:
                    candles, err = None, f"Abruf fehlgeschlagen: {e}"
                st["progress"]["done"] += 1
                if err or not candles or len(candles[4]) < p["warmup"] + p["hh_lookback"] + 20:
                    errors[key] = err or "zu wenig Kerzen"
                    if err and ("Bann" in err or "Ratelimit" in err):
                        raise RuntimeError(f"Abbruch bei {key}: {err}")
                    continue
                flat, n, covered = await asyncio.to_thread(_compute_cell, coin, tf, candles, p)
                events.extend(flat)
                cells[key] = {"coin": coin, "tf": tf, "candles": n, "days": round(covered, 1),
                              "long": stats([e for e in flat if e["dir"] == "long"]),
                              "short": stats([e for e in flat if e["dir"] == "short"]),
                              "all": stats(flat),
                              "recent": [_slim(e) for e in sorted(flat, key=lambda e: -e["ts"])[:5]]}
    except Exception as e:
        st["error"] = str(e)
        _log("⚠️ GUSS-Backtest abgebrochen", {"error": str(e), "traceback": traceback.format_exc()})
    result = {"params": {k: p[k] for k in PARAM_RANGES}, "days": days, "tfs": tfs, "coins": coins,
              "started": started, "finished": time.time(), "agg": aggregate(events), "cells": cells, "errors": errors,
              "partial": bool(st["error"])}
    st["result"] = result
    st["running"] = False
    try:
        from bot_core import get_redis
        r = await get_redis()
        if r is not None:
            await asyncio.wait_for(r.set(_BT_KEY, json.dumps(result)), timeout=10)
    except Exception as e:
        _log("⚠️ GUSS: Backtest-Ergebnis konnte nicht in Redis gespeichert werden", {"error": str(e)})


# ============================================================================
# WEB
# ============================================================================
async def handle_guss_index(request):
    from aiohttp import web
    return web.Response(text=GUSS_HTML, content_type="text/html")


async def handle_guss_universe(request):
    from aiohttp import web
    return web.json_response({"coins": await guss_universe(force=request.query.get("refresh") == "1")})


async def handle_guss_bt_status(request):
    from aiohttp import web
    if GUSS_BT["result"] is None and not GUSS_BT["running"]:
        try:
            from bot_core import get_redis
            r = await get_redis()
            raw = await asyncio.wait_for(r.get(_BT_KEY), timeout=5) if r is not None else None
            if raw:
                GUSS_BT["result"] = json.loads(raw)
        except Exception as e:
            _log("⚠️ GUSS: gespeichertes Backtest-Ergebnis nicht lesbar", {"error": str(e)})
    return web.json_response({"running": GUSS_BT["running"], "progress": GUSS_BT["progress"],
                              "error": GUSS_BT["error"], "result": GUSS_BT["result"], "defaults": {k: DEFAULTS[k] for k in PARAM_RANGES}})


async def handle_guss_bt_start(request):
    global _bt_task
    from aiohttp import web
    if GUSS_BT["running"]:
        return web.json_response({"ok": False, "error": "Es läuft bereits ein Backtest."}, status=409)
    try:
        body = await request.json()
    except Exception:
        body = {}
    p = clean_params(body)
    try:
        days = max(3, min(90, int(float(body.get("days", 30)))))
    except (TypeError, ValueError):
        days = 30
    tfs = [t for t in body.get("tfs", TFS_ALL) if t in TFS_ALL] or list(TFS_ALL)
    coin = str(body.get("coin", "ALL")).strip().upper()
    coins = await guss_universe() if coin in ("", "ALL") else [coin]
    if not coins:
        return web.json_response({"ok": False, "error": "Keine Coins gefunden (Lighter-/Binance-Liste nicht erreichbar)."}, status=503)
    if coin not in ("", "ALL"):
        from strategies import BINANCE_SYMBOL_MAP
        if coin not in BINANCE_SYMBOL_MAP:
            await guss_universe(force=True)
        if coin not in BINANCE_SYMBOL_MAP:
            return web.json_response({"ok": False, "error": f"{coin} nicht auf Binance Futures gefunden."}, status=404)
    _bt_task = asyncio.create_task(_bt_job(coins, tfs, days, p))
    return web.json_response({"ok": True, "coins": len(coins), "tfs": tfs, "days": days})


GUSS_HTML = r"""<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>GUSS Backtest</title>
<style>
*{box-sizing:border-box}
body{margin:0;background:#07080b;color:#e9ecf1;font-family:Inter,system-ui,-apple-system,Segoe UI,sans-serif;font-size:14px}
header{display:flex;justify-content:space-between;align-items:center;padding:18px 28px;border-bottom:1px solid #1a1e26}
header h1{margin:0;font-size:18px}
header nav a{color:#7d8696;text-decoration:none;font-size:13px;margin-left:18px}
header nav a:hover{color:#fff}
main{padding:24px 28px;max-width:1200px;margin:0 auto}
.card{background:#12151b;border:1px solid #242932;border-radius:14px;padding:18px 20px;margin-bottom:18px}
.card h2{margin:0 0 12px;font-size:15px}
.note{color:#7d8696;font-size:12px;line-height:1.5}
.g{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:12px;margin:10px 0 14px}
label{display:flex;flex-direction:column;gap:4px;font-size:11px;color:#7d8696}
input,select{background:#0d1015;border:1px solid #2a303a;color:#e9ecf1;border-radius:8px;padding:7px 9px;font:500 13px Inter,system-ui,sans-serif;width:100%}
.tfs{display:flex;gap:14px;align-items:center;margin:6px 0 12px}
.tfs label{flex-direction:row;align-items:center;gap:6px;color:#e9ecf1;font-size:13px}
.tfs input{width:auto}
button{background:#2a303a;border:1px solid #3a4150;color:#fff;font:600 13px Inter,system-ui,sans-serif;padding:9px 18px;border-radius:9px;cursor:pointer}
button:hover{background:#343b47}
button:disabled{opacity:.5;cursor:default}
.bar{height:8px;background:#1d2128;border-radius:4px;overflow:hidden;margin:10px 0 4px}
.bar i{display:block;height:100%;background:#1fcf6e;width:0}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{padding:7px 9px;text-align:right;border-bottom:1px solid #1d2128;white-space:nowrap}
th:first-child,td:first-child{text-align:left}
th{color:#aab2c0;font-weight:600;cursor:default}
th.s{cursor:pointer}
th.s:hover{color:#fff}
.pos{color:#1fcf6e}.neg{color:#ff5d5d}.dim{color:#7d8696}
.scroll{overflow-x:auto}
#err{color:#ff5d5d;font-size:13px;margin-top:8px}
</style>
</head>
<body>
<header><h1>GUSS Sniperentry · Backtest</h1><nav><a href="/">Bot-Dashboard</a><a href="/screener">Coin Screener</a><a href="/scalp">Scalp</a></nav></header>
<main>
<div class="card">
  <h2>Backtest starten</h2>
  <div class="note">Holt Binance-Futures-Kerzen (kein Spot), erkennt alle GUSS-Setups (Long und Short) und prüft, ob der Kurs danach die 21 EMA (TP1) vor dem SL erreicht hat. Ergebnis in R (1R = Abstand Entry–SL). Läuft im Hintergrund, 30 Coins × 4 Zeitrahmen dauern einige Minuten.</div>
  <div class="g">
    <label>Coin<select id="coin"><option value="ALL">Alle (Top 30)</option></select></label>
    <label>Zeitraum (Tage)<input id="days" type="number" min="3" max="90" value="30"></label>
    <label>Überdehnung (ATR)<input id="ext_atr" type="number" step="0.1"></label>
    <label>Fib-Toleranz (ATR)<input id="fib_tol_atr" type="number" step="0.05"></label>
    <label>Hoch-Lookback (Kerzen)<input id="hh_lookback" type="number" step="1"></label>
    <label>Max. Korrektur (Kerzen)<input id="max_corr" type="number" step="1"></label>
    <label>Mind. SL-Abstand (ATR)<input id="min_risk_atr" type="number" step="0.1"></label>
    <label>Haltedauer max. (Kerzen)<input id="max_hold" type="number" step="1"></label>
    <label>Teilgewinn an 21 EMA (0-1)<input id="tp1_share" type="number" step="0.05"></label>
  </div>
  <div class="tfs"><span class="note">Zeitrahmen:</span>
    <label><input type="checkbox" class="tf" value="5m" checked>5m</label>
    <label><input type="checkbox" class="tf" value="15m" checked>15m</label>
    <label><input type="checkbox" class="tf" value="30m" checked>30m</label>
    <label><input type="checkbox" class="tf" value="1h" checked>1h</label>
  </div>
  <button id="go">Backtest starten</button>
  <div class="bar"><i id="pbar"></i></div><div class="note" id="ptxt">Bereit.</div><div id="err"></div>
</div>
<div id="out"></div>
</main>
<script>
var $ = function(id){return document.getElementById(id)};
var PKEYS = ["ext_atr","fib_tol_atr","hh_lookback","max_corr","min_risk_atr","max_hold","tp1_share"];
var polling = null, sortKey = "n", sortDir = -1, lastRes = null;
function esc(s){return String(s).replace(/[&<>"]/g,function(c){return {"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]})}
function cls(v){return v>0?"pos":(v<0?"neg":"dim")}
function fmt(v,nd){return (v===null||v===undefined)?"–":Number(v).toFixed(nd===undefined?1:nd)}
function srow(name,s){
  if(!s||!s.n) return "<tr><td>"+esc(name)+"</td><td class='dim'>0</td><td colspan='7' class='dim'>keine Setups</td></tr>";
  return "<tr><td>"+esc(name)+"</td><td>"+s.n+"</td><td>"+fmt(s.tp1_pct)+" %</td><td>"+fmt(s.sl_pct)+" %</td><td>"+fmt(s.to_pct)+" %</td><td>"+fmt(s.tp2_pct)+" %</td><td class='"+cls(s.avg_r_scalp)+"'>"+fmt(s.avg_r_scalp,2)+" R</td><td class='"+cls(s.avg_r_run)+"'>"+fmt(s.avg_r_run,2)+" R</td><td>"+fmt(s.avg_rr,2)+"</td></tr>";
}
var HEAD = "<thead><tr><th>Gruppe</th><th>Setups</th><th>TP1 erreicht</th><th>SL zuerst</th><th>Timeout</th><th>bis Hoch (TP2)</th><th>Ø R (alles an 21 EMA)</th><th>Ø R (25 % + Rest zum Hoch)</th><th>Ø Chance/Risiko</th></tr></thead>";
function block(title,obj,note){
  var rows = Object.keys(obj).map(function(k){return srow(k,obj[k])}).join("");
  return "<div class='card'><h2>"+esc(title)+"</h2>"+(note?"<div class='note' style='margin-bottom:8px'>"+note+"</div>":"")+"<div class='scroll'><table>"+HEAD+"<tbody>"+rows+"</tbody></table></div></div>";
}
function coinTable(res){
  var rows = [];
  Object.keys(res.cells).forEach(function(k){
    var c = res.cells[k];
    ["long","short"].forEach(function(d){
      var s = c[d]; if(!s||!s.n) return;
      rows.push({coin:c.coin,tf:c.tf,dir:d,n:s.n,tp1:s.tp1_pct,sl:s.sl_pct,r:s.avg_r_scalp,rr:s.avg_r_run});
    });
  });
  rows.sort(function(a,b){var x=a[sortKey],y=b[sortKey]; if(typeof x==="string") return sortDir*x.localeCompare(y); return sortDir*(x-y)});
  var h = [["coin","Coin"],["tf","TF"],["dir","Richtung"],["n","Setups"],["tp1","TP1 %"],["sl","SL %"],["r","Ø R (21 EMA)"],["rr","Ø R (Runner)"]];
  var th = h.map(function(x){return "<th class='s' data-k='"+x[0]+"'>"+x[1]+(sortKey===x[0]?(sortDir<0?" ▼":" ▲"):"")+"</th>"}).join("");
  var tb = rows.map(function(r){return "<tr><td>"+esc(r.coin)+"</td><td>"+r.tf+"</td><td>"+(r.dir==="long"?"Long":"Short")+"</td><td>"+r.n+"</td><td>"+fmt(r.tp1)+"</td><td>"+fmt(r.sl)+"</td><td class='"+cls(r.r)+"'>"+fmt(r.r,2)+"</td><td class='"+cls(r.rr)+"'>"+fmt(r.rr,2)+"</td></tr>"}).join("");
  return "<div class='card'><h2>Je Coin, Zeitrahmen und Richtung</h2><div class='note' style='margin-bottom:8px'>Spaltenkopf anklicken zum Sortieren. Bei wenigen Setups (unter ca. 10) ist die Quote Zufall.</div><div class='scroll'><table><thead><tr>"+th+"</tr></thead><tbody>"+tb+"</tbody></table></div></div>";
}
function recent(res){
  var all = [];
  Object.keys(res.cells).forEach(function(k){res.cells[k].recent.forEach(function(e){all.push(e)})});
  all.sort(function(a,b){return b.ts-a.ts});
  all = all.slice(0,40);
  if(!all.length) return "";
  var tb = all.map(function(e){
    var d = new Date(e.ts);
    var t = d.toLocaleString("de-DE",{day:"2-digit",month:"2-digit",hour:"2-digit",minute:"2-digit"});
    return "<tr><td>"+esc(e.coin)+"</td><td>"+e.tf+"</td><td>"+(e.dir==="long"?"Long":"Short")+"</td><td>"+t+"</td><td>"+fmt(e.entry,4)+"</td><td>"+fmt(e.sl,4)+"</td><td>"+fmt(e.tp1,4)+"</td><td>"+fmt(e.ext,1)+"</td><td>"+e.n_corr+"</td><td>"+(e.fib?e.fib.level:"–")+"</td><td>"+e.score_base+"</td><td>"+esc(e.out)+"</td><td class='"+cls(e.r_scalp)+"'>"+fmt(e.r_scalp,2)+"</td></tr>";
  }).join("");
  return "<div class='card'><h2>Letzte Setups zum Gegenprüfen im TradingView-Chart</h2><div class='note' style='margin-bottom:8px'>Zeit = Beginn der Kerze, die die 50 EMA berührt hat (deine Browser-Zeitzone). Entry/SL/TP1 sind die Preise, mit denen gerechnet wurde.</div><div class='scroll'><table><thead><tr><th>Coin</th><th>TF</th><th>Richtung</th><th>Zeit</th><th>Entry</th><th>SL</th><th>TP1</th><th>Überdehnung ATR</th><th>Korr.-Kerzen</th><th>Fib</th><th>Basis-Score</th><th>Ergebnis</th><th>R</th></tr></thead><tbody>"+tb+"</tbody></table></div></div>";
}
function render(res){
  lastRes = res;
  var a = res.agg, errs = Object.keys(res.errors||{});
  var days = res.days;
  var html = "";
  var s = a.all;
  html += "<div class='card'><h2>Gesamt · "+res.coins.length+" Coins · "+days+" Tage"+(res.partial?" · <span class='neg'>unvollständig (abgebrochen)</span>":"")+"</h2>"
    + "<div class='scroll'><table>"+HEAD+"<tbody>"+srow("Alle Setups",s)+"</tbody></table></div>"
    + "<div class='note' style='margin-top:8px'>TP1 = Kurs erreicht die 21 EMA, bevor der SL fällt. „Ø R“ ist der Durchschnittsgewinn pro Setup in R (1R = Abstand Entry–SL), positiv heißt: lohnt sich im Schnitt. Entry wird als Limit auf der 50 EMA angenommen und gilt immer als gefüllt, sobald die Kerze sie berührt (in der Realität kann die Orderausführung abweichen).</div></div>";
  html += block("Nach Zeitrahmen",a.by_tf);
  html += block("Long vs. Short",a.by_dir);
  html += block("Mit und ohne Fibonacci-Konfluenz",a.by_fib,"Konfluenz = 50 EMA liegt auf einem der Level 0.5 / 0.559 / 0.618 / 0.667 / 0.786. Wenn das PDF recht hat, müsste die obere Zeile besser sein.");
  html += block("Nach Basis-Score",a.by_score,"Score nur aus Überdehnung, Korrektur-Sauberkeit, Fib und Chance/Risiko (Wellenanker und höherer Zeitrahmen sind im Backtest nicht enthalten). Ein brauchbarer Score zeigt von unten nach oben steigende TP1-Quoten.");
  html += coinTable(res);
  html += recent(res);
  if(errs.length) html += "<div class='card'><h2>Nicht ausgewertet</h2><div class='note'>"+errs.map(function(k){return esc(k)+": "+esc(res.errors[k])}).join("<br>")+"</div></div>";
  $("out").innerHTML = html;
  Array.prototype.forEach.call(document.querySelectorAll("th.s"),function(th){th.onclick=function(){var k=th.getAttribute("data-k"); if(sortKey===k) sortDir=-sortDir; else {sortKey=k; sortDir=(k==="coin"||k==="tf"||k==="dir")?1:-1} render(lastRes)}});
}
function poll(){
  fetch("/api/guss/backtest").then(function(r){return r.json()}).then(function(d){
    if(!$(PKEYS[0]).value){ PKEYS.forEach(function(k){$(k).value = d.defaults[k]}) }
    var p = d.progress||{};
    var pct = p.total? Math.round(p.done/p.total*100):0;
    $("pbar").style.width = (d.running?pct:(d.result?100:0))+"%";
    $("ptxt").textContent = d.running ? ("Läuft: "+p.done+" / "+p.total+" ("+(p.current||"")+")") : (d.result ? "Fertig." : "Bereit.");
    $("err").textContent = d.error || "";
    $("go").disabled = !!d.running;
    if(d.result) render(d.result);
    if(d.running){ if(!polling) polling = setInterval(poll, 3000) } else if(polling){ clearInterval(polling); polling = null }
  }).catch(function(e){ $("err").textContent = "Status nicht abrufbar: "+e });
}
$("go").onclick = function(){
  var body = {coin:$("coin").value, days:$("days").value, tfs:[]};
  PKEYS.forEach(function(k){body[k]=$(k).value});
  Array.prototype.forEach.call(document.querySelectorAll(".tf"),function(c){if(c.checked) body.tfs.push(c.value)});
  $("err").textContent = "";
  fetch("/api/guss/backtest",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)})
    .then(function(r){return r.json()}).then(function(d){ if(!d.ok) $("err").textContent = d.error||"Start fehlgeschlagen"; poll(); if(!polling) polling = setInterval(poll,3000) });
};
fetch("/api/guss/universe").then(function(r){return r.json()}).then(function(d){
  (d.coins||[]).forEach(function(c){var o=document.createElement("option"); o.value=c; o.textContent=c; $("coin").appendChild(o)});
}).catch(function(){});
poll();
</script>
</body>
</html>
"""
