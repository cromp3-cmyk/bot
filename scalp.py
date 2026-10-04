"""
scalp.py - Scalp-Dashboard: Unterseite /scalp zum manuellen Handeln (plus optionalem Autotrade).

Was die Seite kann:
  - Chart je Coin mit den Zeitebenen 10s / 15s / 30s / 1m / 5m / 15m (Kerzen aus dem Binance-WS-Cache, wie der
    Screener - KEIN eigener Binance-Traffic) und darunter der Wellenanker (WaveTrend) mit Signalpunkten.
  - Market-Orders Long/Short/Schliessen mit derselben Groesse wie bei den Bots (margin x leverage aus der Bot-Config).
    Gleiche Richtung = Nachkauf, Gegenrichtung = erst schliessen, dann neu eroeffnen.
  - TP/SL als Linien im Chart (mit der Maus ziehbar) oder per Eingabefeld. Sie werden BOT-SEITIG ueberwacht
    (Kurspruefung jede Sekunde, dann Market-Exit) - es liegen KEINE Stop-Orders auf der Boerse.
  - Autotrade je Coin: bei einem neuen Wellenanker-Signal (auswaehlbar: normal / stark / extrem, Long/Short) auf der
    im Chart gewaehlten Zeitebene wird automatisch eine Market-Order ausgefuehrt, danach TP/SL in Dollar gesetzt.

Sicherheitsregeln:
  - Autotrade startet immer AUS (auch nach einem Neustart) und feuert nie auf ein Signal, das schon vor dem
    Einschalten da war. Hoechstens eine Auto-Order pro Kerze.
  - Ob echt oder Dry-Run gehandelt wird, entscheidet allein cfg["dry_run"] des jeweiligen Bots (wie ueberall).
  - TP/SL/Autotrade-Einstellungen liegen nur im Speicher und sind nach einem Neustart weg. Die Position selbst
    (Bot-State) bleibt natuerlich bestehen - dann ist sie ohne TP/SL, bis du neue setzt.
  - Der Chart zeigt Binance-Kerzen, ausgefuehrt wird auf Lighter: Preise koennen minimal abweichen.
"""

import asyncio
import os
import time
import traceback

from aiohttp import web

from bot_core import debug_log, SYMBOLS, BOTS, execute_entry, execute_exit, get_redis
import binance_ws
from strategies import (
    BINANCE_SYMBOL_MAP, BINANCE_FUTURES_ONLY_SYMBOLS,
    compute_rsi, compute_wavetrend_series,
)
from screener import (
    _resample_1s, _wave_moneyflow, _market_for, _r, SCREENER_STATE,
    WA_N1, WA_N2, WA_SIG, WA_SCALE, WA_Z1, WA_Z3,
    WA_RSI_LEN, WA_RSI_LONG_MAX, WA_RSI_SHORT_MIN,
)

# ============================================================================
# KONFIGURATION
# ============================================================================
TFS = ["10s", "15s", "30s", "1m", "5m", "15m"]
SECOND_AGG = {"10s": 10, "15s": 15, "30s": 30}   # aus 1s-Spot-Kerzen zusammengerechnet
CHART_BARS = 200                                  # so viele Kerzen gehen an den Browser
WARMUP_BARS = 150                                 # zusaetzliche Kerzen fuer das Einschwingen der Indikatoren
MIN_BARS = 60                                     # Autotrade: darunter keine Signale (Indikator noch nicht eingeschwungen)
MIN_CHART_BARS = 30                               # Chart: ab so vielen Kerzen wird schon gezeichnet (mit Hinweis)
TFSTATE_TTL = 5.0                                 # Sekunden, wie lange die Zeitebenen-Uebersicht gecacht wird
SCALP_COINS = [s for s in SYMBOLS]
AUTO_CHECK_EVERY = 2                              # Sekunden zwischen zwei Signalpruefungen des Autotrades

# ============================================================================
# ZUSTAND (nur im Speicher)
# ============================================================================
TPSL = {}        # sym -> {"side": "long"/"short", "tp": float|None, "sl": float|None}
AUTO = {}        # sym -> Autotrade-Einstellungen
AUTO_LOG = []    # neueste zuerst
WA_SOURCES = ("close", "hlc3", "ohlc4", "hl2")
WA_CFG = {"n1": WA_N1, "n2": WA_N2, "z1": WA_Z1, "src": "close"}   # Wellenanker-Einstellungen (gelten fuer alle Coins)
WA_FILE = os.getenv("SCALP_SETTINGS_FILE", "scalp_settings.json")


def _z3():
    return max(WA_Z3, WA_CFG["z1"] + 15.0)


REDIS_KEY = "scalp:settings"   # eigener Schluessel - die Bot-Configs (gridbot:*) bleiben unangetastet


async def _load_wa():
    """Laedt die Wellenanker-Einstellungen: zuerst aus Redis (ueberlebt Neustarts/Deploys), sonst aus der Datei."""
    import json
    raw = None
    try:
        r = await get_redis()
        if r is not None:
            raw = await asyncio.wait_for(r.get(REDIS_KEY), timeout=5)
    except Exception as e:
        debug_log("⚠️ Scalp: Einstellungen aus Redis laden fehlgeschlagen", {"error": str(e)})
    if not raw:
        try:
            with open(WA_FILE) as f:
                raw = f.read()
        except Exception:
            raw = None
    if raw:
        try:
            _apply_wa(json.loads(raw))
            debug_log("✅ Scalp: Wellenanker-Einstellungen geladen", dict(WA_CFG))
        except Exception as e:
            debug_log("⚠️ Scalp: gespeicherte Einstellungen ungueltig", {"error": str(e)})


async def _save_wa():
    """Speichert in Redis (dauerhaft) und zusaetzlich in eine Datei als Notnagel. -> True, wenn Redis geklappt hat."""
    import json
    data = json.dumps(WA_CFG)
    ok = False
    try:
        r = await get_redis()
        if r is not None:
            await asyncio.wait_for(r.set(REDIS_KEY, data), timeout=5)
            ok = True
    except Exception as e:
        debug_log("⚠️ Scalp: Einstellungen in Redis speichern fehlgeschlagen", {"error": str(e)})
    try:
        with open(WA_FILE, "w") as f:
            f.write(data)
    except Exception:
        pass
    return ok


def _apply_wa(d):
    """Validiert und uebernimmt Wellenanker-Einstellungen. Wirft ValueError mit Klartext."""
    new = dict(WA_CFG)
    try:
        if "n1" in d:
            new["n1"] = int(d["n1"])
        if "n2" in d:
            new["n2"] = int(d["n2"])
        if "z1" in d:
            new["z1"] = float(d["z1"])
    except (TypeError, ValueError):
        raise ValueError("Kanal-/Durchschnitt-Länge und Zone müssen Zahlen sein")
    if "src" in d:
        if d["src"] not in WA_SOURCES:
            raise ValueError("Quelle muss eine von " + ", ".join(WA_SOURCES) + " sein")
        new["src"] = d["src"]
    if not 2 <= new["n1"] <= 100:
        raise ValueError("Kanal-Länge: 2 bis 100")
    if not 2 <= new["n2"] <= 200:
        raise ValueError("Durchschnitt-Länge: 2 bis 200")
    if not 5 <= new["z1"] <= 90:
        raise ValueError("Zone: 5 bis 90")
    WA_CFG.update(new)
_locks = {}
_tfstate_cache = {}   # sym -> (ts, {tf: state})
_last_err_log = {}


def _lock(sym):
    lk = _locks.get(sym)
    if lk is None:
        lk = asyncio.Lock()
        _locks[sym] = lk
    return lk


def _auto_cfg(sym):
    a = AUTO.get(sym)
    if a is None:
        a = {"enabled": False, "normal": False, "strong": True, "ext": True, "long": True, "short": True,
             "tp_usd": 8.0, "sl_usd": 4.0, "usd": 0.0, "tf": "1m", "last_ts": None}
        AUTO[sym] = a
    return a


def _log(sym, text, kind="info"):
    AUTO_LOG.insert(0, {"t": time.time(), "coin": sym, "text": text, "kind": kind})
    del AUTO_LOG[40:]
    debug_log(f"🎯 [{sym}] Scalp: {text}")


# ============================================================================
# KERZEN + WELLENANKER
# ============================================================================
def _raw_candles(sym, tf):
    """-> (candles|None, reason|None). Abonniert die noetigen Streams im WS-Cache (kein eigener Traffic)."""
    pair = BINANCE_SYMBOL_MAP.get(sym)
    if not pair:
        return None, "Kein Binance-Paar für diesen Coin – Chart nicht verfügbar"
    if tf in SECOND_AGG:
        if sym in BINANCE_FUTURES_ONLY_SYMBOLS:
            return None, f"{tf} nicht verfügbar (nur Futures, dort gibt es keine 1s-Kerzen)"
        binance_ws.ensure_subscribed("spot", pair, "1s")
        back = SECOND_AGG[tf] * (CHART_BARS + WARMUP_BARS)
        return binance_ws.get_cached_candles_ext("spot", pair, "1s", back), None
    market = _market_for(sym)
    binance_ws.ensure_subscribed(market, pair, tf)
    return binance_ws.get_cached_candles_ext(market, pair, tf, CHART_BARS + WARMUP_BARS), None


def _prepare(raw, tf, min_bars=MIN_BARS):
    if not raw:
        return None
    if tf in SECOND_AGG:
        raw = _resample_1s(raw, SECOND_AGG[tf])
    return raw if len(raw) >= min_bars else None


def _wa_series(cs):
    o = [x["o"] for x in cs]
    h = [x["h"] for x in cs]
    l = [x["l"] for x in cs]
    c = [x["c"] for x in cs]
    v = [x["v"] for x in cs]
    n = len(c)
    z1, z3 = WA_CFG["z1"], _z3()
    wt1, wt2 = compute_wavetrend_series(o, h, l, c, WA_CFG["n1"], WA_CFG["n2"], WA_SIG, WA_CFG["src"], WA_SCALE)
    rsi = compute_rsi(c, WA_RSI_LEN)
    mf = _wave_moneyflow(h, l, c, v)
    events = []
    for i in range(1, n):
        up = wt1[i] > wt2[i] and wt1[i - 1] <= wt2[i - 1]
        dn = wt1[i] < wt2[i] and wt1[i - 1] >= wt2[i - 1]
        if up and wt2[i] < -z1:
            events.append({"kind": "long", "idx": i, "ext": wt2[i] < -z3,
                           "strong": rsi[i] < WA_RSI_LONG_MAX and mf[i] > mf[i - 1]})
        elif dn and wt2[i] > z1:
            events.append({"kind": "short", "idx": i, "ext": wt2[i] > z3,
                           "strong": rsi[i] > WA_RSI_SHORT_MIN and mf[i] < mf[i - 1]})
    return wt1, wt2, events


def _tf_state(cs):
    o = [x["o"] for x in cs]
    h = [x["h"] for x in cs]
    l = [x["l"] for x in cs]
    c = [x["c"] for x in cs]
    z1 = WA_CFG["z1"]
    wt1, wt2 = compute_wavetrend_series(o, h, l, c, WA_CFG["n1"], WA_CFG["n2"], WA_SIG, WA_CFG["src"], WA_SCALE)
    a, b = wt1[-1], wt2[-1]
    return 1 if (b < -z1 and a > b) else -1 if (b > z1 and a < b) else 0


def _compute_chart(main_raw, tf, other_raws):
    """Laeuft im Thread. other_raws: {tf: raw} nur fuer die Zeitebenen-Uebersicht (kann leer sein)."""
    cs = _prepare(main_raw, tf, MIN_CHART_BARS)
    states = {}
    for t, raw in other_raws.items():
        c2 = _prepare(raw, t, MIN_CHART_BARS)
        states[t] = None if c2 is None else _tf_state(c2)
    if cs is None:
        return None, states
    wt1, wt2, events = _wa_series(cs)
    states[tf] = _tf_state(cs)
    n = len(cs)
    off = max(0, n - CHART_BARS)
    candles, w1, w2 = [], [], []
    for i in range(off, n):
        x = cs[i]
        t = x["ts"] // 1000
        candles.append({"time": t, "open": x["o"], "high": x["h"], "low": x["l"], "close": x["c"]})
        a, b = _r(wt1[i], 2), _r(wt2[i], 2)
        # Leere Punkte als "Whitespace" mitgeben, damit WT-Reihen und Kerzen gleich viele Eintraege haben
        # (der Wellenanker-Chart folgt dem Preis-Chart ueber den Index)
        w1.append({"time": t, "value": a} if a is not None else {"time": t})
        w2.append({"time": t, "value": b} if b is not None else {"time": t})
    markers = []
    for e in events:
        if e["idx"] >= off:
            x = cs[e["idx"]]
            markers.append({"time": x["ts"] // 1000, "kind": e["kind"], "strong": bool(e["strong"]),
                            "ext": bool(e["ext"]), "price": x["c"], "live": e["idx"] == n - 1})
    a, b = wt1[-1], wt2[-1]
    return {"candles": candles, "wt1": w1, "wt2": w2, "markers": markers, "warm": n >= MIN_BARS, "bars": n,
            "wt1v": _r(a, 1), "wt2v": _r(b, 1), "diff": _r(a - b, 1)}, states


def _auto_eval(raw, tf, last_ts):
    """Thread: (ts_der_letzten_geschlossenen_Kerze, event|None). Rechnet nur, wenn eine neue Kerze zu ist."""
    cs = _prepare(raw, tf)
    if cs is None or len(cs) < MIN_BARS:
        return None, None
    closed_idx = len(cs) - 2
    ts = cs[closed_idx]["ts"]
    if last_ts is not None and ts <= last_ts:
        return ts, None
    if last_ts is None:
        return ts, None   # Basislinie setzen: Signale, die schon da waren, zaehlen nicht
    _, _, events = _wa_series(cs)
    for e in reversed(events):
        if e["idx"] == closed_idx:
            return ts, {"kind": e["kind"], "strong": bool(e["strong"]), "ext": bool(e["ext"]),
                        "price": cs[closed_idx]["c"]}
        if e["idx"] < closed_idx:
            break
    return ts, None


# ============================================================================
# ORDERS
# ============================================================================
def _pos_info(sym):
    st = BOTS[sym]["state"]
    side = st.get("position")
    if side is None:
        return None
    price = st.get("last_price")
    avg = st.get("avg_entry_price")
    size = st.get("total_coin_size")
    if not avg or not size:
        return {"side": side, "size": size, "avg": avg, "pnl": None, "roi": None}
    pnl = None
    roi = None
    if price:
        pnl = (price - avg) * size if side == "long" else (avg - price) * size
        margin = BOTS[sym]["config"].get("margin") or 0
        roi = (pnl / margin * 100) if margin else None
    return {"side": side, "size": size, "avg": avg, "pnl": _r(pnl, 2), "roi": _r(roi, 1)}


def _clear_tpsl(sym):
    TPSL.pop(sym, None)


MAX_SIZE_FACTOR = 20.0   # Schutz vor Tippfehlern: hoechstens das 20-fache der Bot-Standardgroesse


def _size_mult(sym, usd):
    """Gewuenschte Positionsgroesse in USDC -> size_multiplier fuer execute_entry (1.0 = Bot-Standard)."""
    if not usd:
        return 1.0, None
    cfg = BOTS[sym]["config"]
    base = (cfg.get("margin") or 0) * (cfg.get("leverage") or 0)
    if base <= 0:
        return 1.0, None
    mult = float(usd) / base
    if mult > MAX_SIZE_FACTOR:
        return None, f"Größe zu hoch (max. {int(base * MAX_SIZE_FACTOR)} USDC = {int(MAX_SIZE_FACTOR)}× Bot-Größe)"
    return mult, None


async def _place(sym, direction, source, usd=None):
    """Gemeinsame Order-Logik (wie handle_manual_trade). usd = Positionsgroesse in USDC (leer = Bot-Standard).
    -> (ok, fehlertext)"""
    mult, err = _size_mult(sym, usd)
    if err:
        return False, err
    async with _lock(sym):
        st = BOTS[sym]["state"]
        price = st.get("last_price")
        if price is None:
            return False, "kein aktueller Preis bekannt"
        if st["position"] is not None and st["position"] != direction:
            await execute_exit(sym, price, f"{source}-REVERSE")
            if st["position"] is not None:
                return False, "Schließen fehlgeschlagen – siehe Log"
            _clear_tpsl(sym)
            price = st.get("last_price") or price
        is_add_on = st["position"] == direction
        ok = await execute_entry(sym, direction, price, is_add_on=is_add_on, size_multiplier=mult)
        if not ok:
            return False, "Order fehlgeschlagen – siehe Log"
        return True, None


async def _close(sym, source):
    async with _lock(sym):
        st = BOTS[sym]["state"]
        if st["position"] is None:
            return False, "keine offene Position"
        price = st.get("last_price")
        if price is None:
            return False, "kein aktueller Preis bekannt"
        await execute_exit(sym, price, source)
        if st["position"] is not None:
            return False, "Schließen fehlgeschlagen – siehe Log"
        _clear_tpsl(sym)
        return True, None


def _set_tpsl_from_usd(sym, tp_usd, sl_usd):
    st = BOTS[sym]["state"]
    side, avg, size = st.get("position"), st.get("avg_entry_price"), st.get("total_coin_size")
    if side is None or not avg or not size:
        return
    d = 1 if side == "long" else -1
    tp = avg + d * tp_usd / size if tp_usd and tp_usd > 0 else None
    sl = avg - d * sl_usd / size if sl_usd and sl_usd > 0 else None
    TPSL[sym] = {"side": side, "tp": tp, "sl": sl}


# ============================================================================
# HINTERGRUND-LOOP: TP/SL-Ueberwachung (jede Sekunde) + Autotrade (alle AUTO_CHECK_EVERY Sekunden)
# ============================================================================
async def _monitor_tpsl():
    for sym in list(TPSL.keys()):
        t = TPSL.get(sym)
        if not t:
            continue
        st = BOTS[sym]["state"]
        if st.get("position") != t["side"]:
            _clear_tpsl(sym)   # Position wurde anderweitig geschlossen/gedreht
            continue
        price = st.get("last_price")
        if price is None:
            continue
        hit = None
        if t["side"] == "long":
            if t["tp"] is not None and price >= t["tp"]:
                hit = "TP"
            elif t["sl"] is not None and price <= t["sl"]:
                hit = "SL"
        else:
            if t["tp"] is not None and price <= t["tp"]:
                hit = "TP"
            elif t["sl"] is not None and price >= t["sl"]:
                hit = "SL"
        if hit:
            ok, err = await _close(sym, f"SCALP-{hit}")
            _log(sym, f"{hit} erreicht @ {price} → " + ("geschlossen" if ok else f"FEHLER: {err}"), "tp" if hit == "TP" else "sl")


async def _run_auto(sym):
    a = _auto_cfg(sym)
    raw, reason = _raw_candles(sym, a["tf"])
    if raw is None:
        return
    ts, ev = await asyncio.to_thread(_auto_eval, raw, a["tf"], a["last_ts"])
    if ts is None:
        return
    a["last_ts"] = ts
    if ev is None:
        return
    kind_ok = (a["normal"] and not ev["strong"] and not ev["ext"]) or (a["strong"] and ev["strong"]) or (a["ext"] and ev["ext"])
    if not kind_ok or not a[ev["kind"]]:
        return
    sig = ("◆" if ev["ext"] else "◉" if ev["strong"] else "●")
    st = BOTS[sym]["state"]
    if st.get("position") == ev["kind"]:
        _log(sym, f"{a['tf']} {ev['kind'].capitalize()} {sig} – Position läuft schon in diese Richtung, übersprungen")
        return
    ok, err = await _place(sym, ev["kind"], "SCALP-AUTO", a.get("usd") or None)
    if not ok:
        _log(sym, f"{a['tf']} {ev['kind'].capitalize()} {sig} → FEHLER: {err}", "err")
        return
    _set_tpsl_from_usd(sym, a["tp_usd"], a["sl_usd"])
    dry = BOTS[sym]["config"].get("dry_run")
    _log(sym, f"{a['tf']} {ev['kind'].capitalize()} {sig} → Market {ev['kind']}" + (" (Dry-Run)" if dry else ""), ev["kind"])


async def scalp_loop():
    """In main.py's asyncio.gather einhaengen."""
    await _load_wa()
    await asyncio.sleep(8)
    tick = 0
    while True:
        try:
            await _monitor_tpsl()
        except Exception as e:
            _err_once("tpsl", e)
        if tick % AUTO_CHECK_EVERY == 0:
            for sym, a in list(AUTO.items()):
                if not a.get("enabled"):
                    continue
                try:
                    await _run_auto(sym)
                except Exception as e:
                    _err_once("auto:" + sym, e)
        tick += 1
        await asyncio.sleep(1)


def _err_once(key, e):
    now = time.time()
    if now - _last_err_log.get(key, 0) > 300:
        _last_err_log[key] = now
        debug_log(f"⚠️ Scalp-Loop Fehler ({key})", {"error": str(e), "traceback": traceback.format_exc()})


# ============================================================================
# WEB: API
# ============================================================================
def _coin_arg(request):
    sym = request.query.get("coin", "").upper()
    return sym if sym in BOTS else None


async def handle_scalp_chart(request):
    sym = _coin_arg(request)
    tf = request.query.get("tf", "1m")
    if sym is None or tf not in TFS:
        return web.json_response({"error": "unknown coin/tf"}, status=400)
    main_raw, reason = _raw_candles(sym, tf)
    if reason:
        return web.json_response({"ok": False, "reason": reason, "tfstates": {}})
    cached = _tfstate_cache.get(sym)
    other = {}
    if cached is None or time.time() - cached[0] > TFSTATE_TTL:
        for t in TFS:
            if t != tf:
                raw, _ = _raw_candles(sym, t)
                other[t] = raw
    data, states = await asyncio.to_thread(_compute_chart, main_raw, tf, other)
    if other:
        _tfstate_cache[sym] = (time.time(), {k: v for k, v in states.items() if k != tf})
    elif cached:
        states = dict(cached[1], **states)
    if data is None:
        return web.json_response({"ok": False, "reason": "lädt noch … (Kerzen werden aufgebaut)", "tfstates": states})
    data.update({"ok": True, "tfstates": states, "tf": tf, "coin": sym, "wa": dict(WA_CFG, z3=_z3())})
    return web.json_response(data)


async def handle_scalp_status(request):
    sym = _coin_arg(request)
    coins = []
    for s in SCALP_COINS:
        st = BOTS[s]["state"]
        sc = SCREENER_STATE.get("coins", {}).get(s) or {}
        coins.append({"coin": s, "price": st.get("last_price"), "chg24": sc.get("chg24"), "pos": st.get("position"),
                      "auto": bool(AUTO.get(s, {}).get("enabled")),
                      "binance": s in BINANCE_SYMBOL_MAP})
    out = {"coins": coins, "log": AUTO_LOG[:12]}
    if sym:
        cfg = BOTS[sym]["config"]
        st = BOTS[sym]["state"]
        t = TPSL.get(sym)
        a = dict(_auto_cfg(sym))
        a.pop("last_ts", None)
        out["coin"] = {
            "symbol": sym, "price": st.get("last_price"), "margin": cfg.get("margin"), "leverage": cfg.get("leverage"),
            "notional": (cfg.get("margin") or 0) * (cfg.get("leverage") or 0), "dry_run": bool(cfg.get("dry_run")),
            "position": _pos_info(sym), "tp": t["tp"] if t else None, "sl": t["sl"] if t else None, "auto": a,
        }
    return web.json_response(out)


async def _body(request):
    try:
        return await request.json()
    except Exception:
        return {}


async def handle_scalp_order(request):
    b = await _body(request)
    sym = str(b.get("coin", "")).upper()
    direction = b.get("direction")
    if sym not in BOTS or direction not in ("long", "short"):
        return web.json_response({"error": "coin/direction ungültig"}, status=400)
    try:
        usd = float(b.get("usd")) if b.get("usd") not in (None, "") else None
    except (TypeError, ValueError):
        return web.json_response({"error": "Größe ist keine Zahl"}, status=400)
    if usd is not None and usd <= 0:
        usd = None
    ok, err = await _place(sym, direction, "SCALP", usd)
    if not ok:
        return web.json_response({"error": err}, status=500)
    return web.json_response({"success": True})


async def handle_scalp_close(request):
    b = await _body(request)
    sym = str(b.get("coin", "")).upper()
    if sym not in BOTS:
        return web.json_response({"error": "coin ungültig"}, status=400)
    ok, err = await _close(sym, "SCALP-MANUAL")
    if not ok:
        return web.json_response({"error": err}, status=400)
    return web.json_response({"success": True})


async def handle_scalp_tpsl(request):
    b = await _body(request)
    sym = str(b.get("coin", "")).upper()
    if sym not in BOTS:
        return web.json_response({"error": "coin ungültig"}, status=400)
    st = BOTS[sym]["state"]
    side, price = st.get("position"), st.get("last_price")
    if side is None:
        return web.json_response({"error": "keine offene Position – TP/SL gelten nur für eine offene Position"}, status=400)
    if price is None:
        return web.json_response({"error": "kein aktueller Preis bekannt"}, status=400)
    vals = {}
    for k in ("tp", "sl"):
        v = b.get(k)
        if v is None or v == "":
            vals[k] = None
            continue
        try:
            vals[k] = float(v)
        except (TypeError, ValueError):
            return web.json_response({"error": f"{k.upper()} ist keine Zahl"}, status=400)
        if vals[k] <= 0:
            return web.json_response({"error": f"{k.upper()} muss > 0 sein"}, status=400)
    d = 1 if side == "long" else -1
    if vals["tp"] is not None and (vals["tp"] - price) * d <= 0:
        return web.json_response({"error": "TP liegt auf der falschen Seite des aktuellen Kurses (würde sofort auslösen)"}, status=400)
    if vals["sl"] is not None and (price - vals["sl"]) * d <= 0:
        return web.json_response({"error": "SL liegt auf der falschen Seite des aktuellen Kurses (würde sofort auslösen)"}, status=400)
    if vals["tp"] is None and vals["sl"] is None:
        _clear_tpsl(sym)
    else:
        TPSL[sym] = {"side": side, "tp": vals["tp"], "sl": vals["sl"]}
    return web.json_response({"success": True})


async def handle_scalp_auto(request):
    b = await _body(request)
    if isinstance(b.get("wa"), dict):
        # Wellenanker-Einstellungen laufen ueber diese (schon immer vorhandene) Route - so braucht es dafuer keine
        # zusaetzliche Route in main.py
        return await _wa_update(b["wa"])
    sym = str(b.get("coin", "")).upper()
    if sym not in BOTS:
        return web.json_response({"error": "coin ungültig"}, status=400)
    a = _auto_cfg(sym)
    new = dict(a)
    for k in ("normal", "strong", "ext", "long", "short", "enabled"):
        if k in b:
            new[k] = bool(b[k])
    for k in ("tp_usd", "sl_usd", "usd"):
        if k in b:
            try:
                new[k] = max(0.0, float(b[k]))
            except (TypeError, ValueError):
                return web.json_response({"error": f"{k} ist keine Zahl"}, status=400)
    if "tf" in b:
        if b["tf"] not in TFS:
            return web.json_response({"error": "tf ungültig"}, status=400)
        new["tf"] = b["tf"]
    if new["usd"]:
        _, serr = _size_mult(sym, new["usd"])
        if serr:
            return web.json_response({"error": serr}, status=400)
    if new["enabled"] and not (new["normal"] or new["strong"] or new["ext"]):
        return web.json_response({"error": "Mindestens eine Signalart wählen (normal / stark / extrem)"}, status=400)
    if new["enabled"] and not (new["long"] or new["short"]):
        return web.json_response({"error": "Mindestens eine Richtung wählen (Long / Short)"}, status=400)
    if new["enabled"] and new["tf"] in SECOND_AGG and sym in BINANCE_FUTURES_ONLY_SYMBOLS:
        return web.json_response({"error": f"{new['tf']} gibt es für {sym} nicht"}, status=400)
    if new["enabled"] and sym not in BINANCE_SYMBOL_MAP:
        return web.json_response({"error": f"{sym} hat kein Binance-Paar – keine Signale möglich"}, status=400)
    if (not a["enabled"] and new["enabled"]) or new["tf"] != a["tf"]:
        new["last_ts"] = None   # neue Basislinie: nur Signale NACH jetzt zaehlen
    was = a["enabled"]
    a.update(new)
    if a["enabled"] != was:
        dry = BOTS[sym]["config"].get("dry_run")
        _log(sym, f"Autotrade {'AN' if a['enabled'] else 'AUS'} ({a['tf']})" + (" – Dry-Run" if dry and a["enabled"] else (" – LIVE!" if a["enabled"] else "")))
    return web.json_response({"success": True})


async def handle_scalp_wa(request):
    return await _wa_update(await _body(request))


async def _wa_update(b):
    try:
        _apply_wa(b)
    except ValueError as e:
        return web.json_response({"error": str(e)}, status=400)
    saved = await _save_wa()
    _tfstate_cache.clear()
    for a in AUTO.values():
        a["last_ts"] = None   # neue Basislinie: Signale mit den neuen Einstellungen zaehlen erst ab jetzt
    _log("ALLE", f"Wellenanker-Einstellungen: Kanal {WA_CFG['n1']}, Durchschnitt {WA_CFG['n2']}, Zone ±{WA_CFG['z1']:g}, Quelle {WA_CFG['src']}")
    return web.json_response({"success": True, "persistent": saved, "wa": dict(WA_CFG, z3=_z3())})


async def handle_scalp_index(request):
    return web.Response(text=SCALP_HTML, content_type="text/html")


# ============================================================================
# WEB: Seite
# ============================================================================
SCALP_HTML = r"""<!DOCTYPE html>
<html lang="de"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Scalp Dashboard</title>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@500;600;700&family=JetBrains+Mono:wght@500;700&display=swap">
<script src="https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.production.js"></script>
<style>
:root{--g:#1fcf6e;--r:#f0354b;--y:#f5b83d;--mut:#7d8696;--bd:#242932}
*{box-sizing:border-box}
body{margin:0;background:#07080b;color:#e9ecf1;font-family:'Inter',sans-serif}
a{color:var(--mut);text-decoration:none}a:hover{color:#fff}
button{font-family:inherit;cursor:pointer}
.wrap{max-width:1600px;margin:0 auto;padding:20px 24px 32px;display:flex;flex-direction:column;gap:16px}
.row{display:flex;flex-wrap:wrap;align-items:center;gap:12px}
.card{background:linear-gradient(180deg,#171a21,#12151b);border:1px solid var(--bd);border-radius:16px;padding:16px;display:flex;flex-direction:column;gap:12px}
.mono{font-family:'JetBrains Mono',monospace}
.lab{font-size:10px;font-weight:600;letter-spacing:.1em;text-transform:uppercase;color:var(--mut)}
.chips{display:flex;gap:8px;overflow-x:auto;padding-bottom:4px}
.chip{flex:0 0 auto;min-width:96px;min-height:56px;display:flex;flex-direction:column;gap:3px;padding:8px 14px;text-align:left;border-radius:12px;border:1px solid var(--bd);background:#12151b;color:#e9ecf1}
.chip.on{border-color:#3b82f6;background:rgba(59,130,246,.12)}
.chip.off{opacity:.45}
.chip .t{display:flex;justify-content:space-between;align-items:center;gap:8px;font-size:14px;font-weight:700}
.dot{display:block;width:8px;height:8px;border-radius:50%;background:#3a4150}
.tfbar{display:flex;gap:4px;padding:4px;background:#0d1015;border:1px solid #1f242c;border-radius:12px}
.tfbar button{min-width:44px;min-height:36px;padding:0 12px;border:0;border-radius:9px;background:transparent;color:var(--mut);font-size:12px;font-weight:600}
.tfbar button.on{background:#2a303a;color:#fff}
#chart{height:440px;position:relative}#wachart{height:170px}
.cwrap{position:relative;border-top:1px solid #1f242c;border-bottom:1px solid #1f242c}
#cmsg{position:absolute;inset:0;display:none;align-items:center;justify-content:center;text-align:center;padding:20px;color:var(--mut);font-size:13px;background:rgba(18,21,27,.85);z-index:5}
.grid2{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
.cell{display:flex;flex-direction:column;gap:3px;padding:9px 12px;background:#0d1015;border-radius:10px}
.cell b{font-family:'JetBrains Mono',monospace;font-size:14px}
.btn{min-height:56px;border:0;border-radius:12px;font-size:15px;font-weight:700}
.btn.long{background:linear-gradient(180deg,#22d27a,#16a85a);color:#07080b;box-shadow:0 0 16px rgba(31,207,110,.3)}
.btn.short{background:linear-gradient(180deg,#f2475b,#c42a40);color:#fff;box-shadow:0 0 16px rgba(240,53,75,.3)}
.btn.ghost{min-height:44px;border:1px solid var(--r);background:transparent;color:var(--r);font-size:13px}
.btn:disabled{opacity:.4;cursor:not-allowed}
.pill{font-size:11px;font-weight:700;padding:5px 12px;border-radius:20px}
.pill.g{background:rgba(31,207,110,.14);color:var(--g)}.pill.r{background:rgba(240,53,75,.14);color:var(--r)}.pill.y{background:rgba(245,184,61,.14);color:var(--y)}.pill.n{background:#1d2128;color:var(--mut)}
.inp{flex:1;min-width:0;min-height:44px;padding:0 12px;border-radius:10px;border:1px solid #2a303a;background:#0d1015;color:#fff;font-family:'JetBrains Mono',monospace;font-size:14px;font-weight:700}
.inp.tp{border-color:var(--g)}.inp.sl{border-color:var(--r)}
.q{display:flex;gap:6px}.q button{flex:1;min-height:36px;border:1px solid #2a303a;border-radius:9px;background:#1d2128;color:#cfd5e0;font-size:12px;font-weight:600}
.chk{min-height:36px;display:flex;align-items:center;gap:6px;padding:0 12px;border-radius:9px;background:#0d1015;border:1px solid #2a303a;font-size:12px;font-weight:600;color:var(--mut);cursor:pointer;user-select:none}
.chk.on{background:rgba(31,207,110,.12);border-color:var(--g);color:#fff}
.chk.on.s{background:rgba(240,53,75,.12);border-color:var(--r)}
.chk input{display:none}
.sw{display:flex;align-items:center;gap:8px;padding:4px 6px 4px 12px;border-radius:20px;background:#1d2128;color:var(--mut);font-size:11px;font-weight:700;border:0}
.sw i{display:block;width:36px;height:20px;border-radius:10px;background:#3a4150;position:relative}
.sw i b{position:absolute;left:2px;top:2px;width:16px;height:16px;border-radius:50%;background:#07080b}
.sw.on{background:rgba(31,207,110,.14);color:var(--g)}.sw.on i{background:var(--g)}.sw.on i b{left:auto;right:2px}
.tfs{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:6px}
.tfs div{display:flex;flex-direction:column;align-items:center;gap:3px;padding:8px 0;background:#0d1015;border-radius:9px;font-size:10px;font-weight:700}
.feed{display:flex;justify-content:space-between;align-items:center;gap:8px;padding:8px 10px;background:#0d1015;border-radius:9px;font-size:11px}
.wafld{display:flex;flex-direction:column;gap:4px;flex:1 1 110px;min-width:100px}.wafld .inp{min-height:38px;font-size:13px;width:100%}
.small{font-size:11px;color:var(--mut);line-height:1.5}
#toast{position:fixed;left:50%;bottom:24px;transform:translateX(-50%);padding:10px 18px;border-radius:10px;background:#1d2128;border:1px solid #3a4150;color:#fff;font-size:13px;display:none;z-index:50;max-width:90vw}
#toast.bad{border-color:var(--r)}
.main{display:flex;flex-wrap:wrap;gap:16px;align-items:flex-start}
.left{flex:999 1 640px;min-width:0}.right{flex:1 1 340px;max-width:420px;min-width:0;display:flex;flex-direction:column;gap:14px}
</style></head><body>
<div class="wrap">
 <div class="row" style="justify-content:space-between">
  <div class="row"><h1 style="margin:0;font-size:18px">Scalp Dashboard</h1><span id="badge" class="pill n">…</span></div>
  <nav class="row" style="gap:18px;font-size:13px"><a href="/">Bot-Dashboard</a><a href="/screener">Coin Screener</a><a href="/copytrading">Copy-Trading</a></nav>
 </div>
 <div class="chips" id="chips"></div>
 <div class="main">
  <div class="card left">
   <div class="row" style="justify-content:space-between">
    <div class="row" style="align-items:baseline"><span id="cname" style="font-size:22px;font-weight:700">–</span><span id="cprice" class="mono" style="font-size:20px;font-weight:700">–</span></div>
    <div class="tfbar" id="tfbar"></div>
   </div>
   <div class="cwrap"><div id="chart"></div><div id="cmsg"></div></div>
   <div class="row" style="justify-content:space-between;padding-top:6px">
    <div class="row" style="gap:10px;align-items:baseline"><b style="font-size:13px">Wellenanker</b><span class="lab" id="wazones">WaveTrend</span></div>
    <div class="row mono" style="gap:14px;font-size:12px;font-weight:700"><span id="wt1v" style="color:#7FB5F0">WT1 –</span><span id="wt2v" style="color:#2F7BFF">WT2 –</span><span id="wdv" style="color:#aab2c0">Δ –</span></div>
   </div>
   <div class="row" style="gap:8px">
    <label class="wafld"><span class="lab">Kanal-Länge</span><input class="inp" id="wa-n1" inputmode="numeric"></label>
    <label class="wafld"><span class="lab">Durchschnitt-Länge</span><input class="inp" id="wa-n2" inputmode="numeric"></label>
    <label class="wafld"><span class="lab">Zone ±</span><input class="inp" id="wa-z1" inputmode="decimal"></label>
    <label class="wafld"><span class="lab">Quelle der Welle</span><select class="inp" id="wa-src"><option value="close">close</option><option value="hlc3">hlc3 (H+L+C)/3</option><option value="ohlc4">ohlc4</option><option value="hl2">hl2 (H+L)/2</option></select></label>
   </div>
   <div class="cwrap"><div id="wachart"></div></div>
   <div class="row small" style="gap:16px"><span>▲▼ Long/Short-Signal</span><span>◉ stark (RSI + Geldfluss)</span><span>◆ extrem (±75)</span><span>TP/SL-Linie mit der Maus ziehen</span></div>
   <div class="small">Chart = Binance-Kerzen. Orders, TP/SL und Kurs oben = Lighter. TP/SL überwacht der Bot (Kurs-Check jede Sekunde) – es liegen keine Stop-Orders auf der Börse.</div>
  </div>
  <div class="right">
   <div class="card">
    <div class="row" style="justify-content:space-between"><b style="font-size:13px" id="posT">Position</b><span id="posPill" class="pill n">FLAT</span></div>
    <div class="grid2">
     <div class="cell"><span class="lab">Größe</span><b id="pSize">–</b></div>
     <div class="cell"><span class="lab">Einstieg Ø</span><b id="pAvg">–</b></div>
     <div class="cell"><span class="lab">Kurs</span><b id="pPrice">–</b></div>
     <div class="cell"><span class="lab">Gewinn / Verlust</span><b id="pPnl">–</b></div>
    </div>
    <button class="btn ghost" id="bClose">Position schließen</button>
   </div>
   <div class="card">
    <b style="font-size:13px">Order · Market</b>
    <div class="row" style="flex-wrap:nowrap"><button class="btn long" id="bLong" style="flex:1">Long / Buy</button><button class="btn short" id="bShort" style="flex:1">Short / Sell</button></div>
    <div style="display:flex;flex-direction:column;gap:6px"><span class="lab">Größe (USDC, Positionswert)</span>
     <div class="row" style="flex-wrap:nowrap;gap:8px"><input class="inp" id="o-usd" inputmode="decimal" placeholder="Bot-Größe"><span class="mono small" style="white-space:nowrap">USDC</span></div>
     <div class="q"><button data-s="1">1×</button><button data-s="2">2×</button><button data-s="5">5×</button></div></div>
    <div style="font-size:12px;color:#aab2c0;line-height:1.5" id="sizeTxt">Größe wie bei den Bots</div>
    <div class="small">Gleiche Richtung = Nachkauf (Ø-Einstieg wird angepasst). Gegenrichtung = erst schließen, dann neu eröffnen.</div>
   </div>
   <div class="card" id="autoCard">
    <div class="row" style="justify-content:space-between;flex-wrap:nowrap"><b style="font-size:13px" id="autoT">Autotrade</b><button class="sw" id="autoSw">AUS<i><b></b></i></button></div>
    <div><div class="lab" style="margin-bottom:6px">Einstieg bei Signal</div>
     <div class="row" style="gap:6px"><label class="chk" id="c-normal"><input type="checkbox" id="a-normal">● normal</label><label class="chk" id="c-strong"><input type="checkbox" id="a-strong">◉ stark</label><label class="chk" id="c-ext"><input type="checkbox" id="a-ext">◆ extrem</label></div></div>
    <div class="row" style="gap:6px"><label class="chk" id="c-long"><input type="checkbox" id="a-long">Long</label><label class="chk s" id="c-short"><input type="checkbox" id="a-short">Short</label></div>
    <div class="grid2">
     <div class="cell"><span class="lab">Zeitebene</span><b id="aTf" style="font-size:13px">folgt Chart</b></div>
     <div class="cell"><span class="lab">Gegensignal</span><b style="font-size:13px">schließen + neu</b></div>
     <div class="cell" style="grid-column:span 2"><span class="lab">Größe (USDC, leer = Bot-Größe)</span><input class="inp" id="a-usd" inputmode="decimal" style="min-height:30px;padding:0;border:0;background:transparent;font-size:13px"></div>
     <div class="cell"><span class="lab" style="color:var(--g)">Auto-TP ($)</span><input class="inp tp" id="a-tp" inputmode="decimal" style="min-height:30px;padding:0;border:0;background:transparent;font-size:13px"></div>
     <div class="cell"><span class="lab" style="color:var(--r)">Auto-SL ($)</span><input class="inp sl" id="a-sl" inputmode="decimal" style="min-height:30px;padding:0;border:0;background:transparent;font-size:13px"></div>
    </div>
    <div id="alog" style="display:flex;flex-direction:column;gap:6px"></div>
    <div class="small">Gilt nur für den gewählten Coin. Je Kerze höchstens eine Auto-Order, Signale vor dem Einschalten zählen nicht. Läuft die Position schon in Signalrichtung, wird nicht nachgekauft. Beim Neustart des Bots ist Autotrade wieder AUS.</div>
   </div>
   <div class="card">
    <div class="row" style="justify-content:space-between"><b style="font-size:13px">Take-Profit / Stop-Loss</b><span class="pill n" id="tpPill">keine</span></div>
    <div style="display:flex;flex-direction:column;gap:6px"><span class="lab" style="color:var(--g)">Take-Profit</span>
     <div class="row" style="flex-wrap:nowrap;gap:8px"><input class="inp tp" id="tp-in" inputmode="decimal" placeholder="Preis"><span class="mono" id="tpPnl" style="font-size:12px;font-weight:700;color:var(--g);white-space:nowrap"></span></div>
     <div class="q"><button data-k="tp" data-u="5">+$5</button><button data-k="tp" data-u="10">+$10</button><button data-k="tp" data-u="20">+$20</button></div></div>
    <div style="display:flex;flex-direction:column;gap:6px"><span class="lab" style="color:var(--r)">Stop-Loss</span>
     <div class="row" style="flex-wrap:nowrap;gap:8px"><input class="inp sl" id="sl-in" inputmode="decimal" placeholder="Preis"><span class="mono" id="slPnl" style="font-size:12px;font-weight:700;color:var(--r);white-space:nowrap"></span></div>
     <div class="q"><button data-k="sl" data-u="3">−$3</button><button data-k="sl" data-u="5">−$5</button><button data-k="sl" data-u="10">−$10</button></div></div>
    <div class="small">Eingabe und Linien im Chart gehören zusammen. Beim Erreichen schließt der Bot per Market-Order. Leeres Feld = entfernen.</div>
   </div>
   <div class="card"><b style="font-size:13px">Wellenanker je Zeitebene</b><div class="tfs" id="tfs"></div></div>
   <div class="card"><b style="font-size:13px">Letzte Signale</b><div id="feed" style="display:flex;flex-direction:column;gap:8px"></div></div>
  </div>
 </div>
</div>
<div id="toast"></div>
<script>
const G='#1fcf6e',R='#f0354b',TFS=['10s','15s','30s','1m','5m','15m'];
const $=id=>document.getElementById(id);
function ls(k,v){try{if(v===undefined)return localStorage.getItem(k);localStorage.setItem(k,v)}catch(e){return null}}
let waKey='',lastT=0,coin=null,tf=TFS.includes(ls('scalp_tf'))?ls('scalp_tf'):'1m',ST=null,loadedKey=null,lastChart=null;
const tzoff=-new Date().getTimezoneOffset()*60;
function fmtP(p){if(p==null||isNaN(p))return '–';const d=p>=1000?1:p>=10?2:p>=1?3:5;return Number(p).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d})}
function digits(p){return p>=1000?1:p>=10?2:p>=1?3:5}
function sg(v){return(v>=0?'+$':'−$')+Math.abs(v).toFixed(2)}
function toast(m,bad){const t=$('toast');t.textContent=m;t.className=bad?'bad':'';t.style.display='block';clearTimeout(toast._t);toast._t=setTimeout(()=>t.style.display='none',4500)}
async function api(path,body){const r=await fetch(path,body?{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}:undefined);let j={};try{j=await r.json()}catch(e){}if(!r.ok)throw new Error(j.error||('HTTP '+r.status));return j}

/* ---------- Charts ---------- */
const LW=window.LightweightCharts;
const base={layout:{background:{type:'solid',color:'#12151b'},textColor:'#7d8696',fontFamily:"'JetBrains Mono',monospace",fontSize:10},
 grid:{vertLines:{color:'rgba(255,255,255,0.04)'},horzLines:{color:'rgba(255,255,255,0.045)'}},
 rightPriceScale:{borderColor:'#1f242c',minimumWidth:76},timeScale:{borderColor:'#1f242c',timeVisible:true,secondsVisible:true,rightOffset:6},autoSize:true};
const pc=LW.createChart($('chart'),base),wc=LW.createChart($('wachart'),Object.assign({},base,{handleScroll:false,handleScale:false}));
const candle=pc.addCandlestickSeries({upColor:G,downColor:R,borderVisible:false,wickUpColor:G,wickDownColor:R,lastValueVisible:true,priceLineVisible:true});
const wt1s=wc.addLineSeries({color:'#7FB5F0',lineWidth:2,priceLineVisible:false,lastValueVisible:false});
const wt2s=wc.addLineSeries({color:'#2F7BFF',lineWidth:2,priceLineVisible:false,lastValueVisible:false});
let zoneLines=[];
function setZones(z1,z3){
 zoneLines.forEach(l=>wt2s.removePriceLine(l));zoneLines=[];
 [[z3,'rgba(255,255,255,.25)',3],[z1,'rgba(240,53,75,.6)',2],[0,'#3a4150',0],[-z1,'rgba(31,207,110,.6)',2],[-z3,'rgba(255,255,255,.25)',3]].forEach(a=>zoneLines.push(wt2s.createPriceLine({price:a[0],color:a[1],lineWidth:1,lineStyle:a[2],axisLabelVisible:true,title:''})));
 $('wazones').textContent='WaveTrend · Zonen ±'+z1+' / ±'+z3;
}
setZones(36,75);
pc.timeScale().subscribeVisibleLogicalRangeChange(r=>{if(r)try{wc.timeScale().setVisibleLogicalRange(r)}catch(e){}});

/* ---------- Linien (Einstieg / TP / SL) + Ziehen ---------- */
const lines={entry:null,tp:null,sl:null},vals={tp:null,sl:null};let drag=null;
function setLine(kind,price,opts){
 if(price==null){if(lines[kind]){candle.removePriceLine(lines[kind]);lines[kind]=null}return}
 if(lines[kind])lines[kind].applyOptions(Object.assign({price},opts));
 else lines[kind]=candle.createPriceLine(Object.assign({price,lineWidth:1,lineStyle:2,axisLabelVisible:true},opts));
}
function pnlAt(p){const pos=ST&&ST.coin&&ST.coin.position;if(!pos||!pos.avg||!pos.size)return null;return(pos.side==='long'?p-pos.avg:pos.avg-p)*pos.size}
function lineTitle(kind,p){const v=pnlAt(p);return(kind==='tp'?'TP ':'SL ')+(v==null?'':sg(v))}
function nearLine(y){for(const k of['tp','sl']){if(lines[k]&&vals[k]!=null){const yy=candle.priceToCoordinate(vals[k]);if(yy!=null&&Math.abs(yy-y)<=9)return k}}return null}
const chartEl=$('chart');
function yOf(ev){return ev.clientY-chartEl.getBoundingClientRect().top}
['mousedown','touchstart'].forEach(n=>chartEl.addEventListener(n,ev=>{const t=ev.touches?ev.touches[0]:ev;if(nearLine(t.clientY-chartEl.getBoundingClientRect().top)){ev.stopPropagation()}},true));
chartEl.addEventListener('pointerdown',ev=>{const k=nearLine(yOf(ev));if(!k)return;drag=k;ev.stopPropagation();ev.preventDefault();pc.applyOptions({handleScroll:false,handleScale:false});try{chartEl.setPointerCapture(ev.pointerId)}catch(e){}},true);
chartEl.addEventListener('pointermove',ev=>{
 if(!drag){chartEl.style.cursor=nearLine(yOf(ev))?'ns-resize':'';return}
 const p=candle.coordinateToPrice(yOf(ev));if(p==null||p<=0)return;
 vals[drag]=p;setLine(drag,p,{title:lineTitle(drag,p)});$(drag+'-in').value=p.toFixed(digits(p));updTpslPnl();
});
async function endDrag(ev){if(!drag)return;drag=null;pc.applyOptions({handleScroll:true,handleScale:true});await saveTpsl()}
chartEl.addEventListener('pointerup',endDrag);chartEl.addEventListener('pointercancel',endDrag);

function updTpslPnl(){
 $('tpPnl').textContent=vals.tp!=null&&pnlAt(vals.tp)!=null?sg(pnlAt(vals.tp)):'';
 $('slPnl').textContent=vals.sl!=null&&pnlAt(vals.sl)!=null?sg(pnlAt(vals.sl)):'';
 $('tpPill').textContent=(vals.tp!=null||vals.sl!=null)?'Bot überwacht':'keine';$('tpPill').className='pill '+((vals.tp!=null||vals.sl!=null)?'g':'n');
}
async function saveTpsl(){
 try{await api('/api/scalp/tpsl',{coin,tp:vals.tp,sl:vals.sl})}catch(e){toast(e.message,true)}
 await pollStatusOnce();
}
['tp','sl'].forEach(k=>{$(k+'-in').addEventListener('change',async()=>{const t=$(k+'-in').value.trim().replace(',','.');vals[k]=t===''?null:parseFloat(t);if(vals[k]!=null&&isNaN(vals[k]))vals[k]=null;await saveTpsl()})});
document.querySelectorAll('.q button[data-k]').forEach(b=>b.addEventListener('click',async()=>{
 const pos=ST&&ST.coin&&ST.coin.position;if(!pos||!pos.avg||!pos.size){toast('Keine offene Position',true);return}
 const d=pos.side==='long'?1:-1,u=parseFloat(b.dataset.u),off=u/pos.size;
 vals[b.dataset.k]=b.dataset.k==='tp'?pos.avg+d*off:pos.avg-d*off;await saveTpsl()}));

/* ---------- Chart-Daten ---------- */
function shift(arr){return arr.map(x=>Object.assign({},x,{time:x.time+tzoff}))}
function applyChart(d,key){
 renderTfStates(d.tfstates||{});
 if(d.wa){
  const k=d.wa.n1+'|'+d.wa.n2+'|'+d.wa.z1+'|'+d.wa.src;
  if(k!==waKey){waKey=k;setZones(d.wa.z1,d.wa.z3);if(!waBusy())fillWa(d.wa)}
 }
 const msg=$('cmsg');
 if(!d.ok){msg.textContent=d.reason||'keine Daten';msg.style.display='flex';return}
 msg.style.display='none';
 const cd=shift(d.candles),w1=shift(d.wt1),w2=shift(d.wt2);
 if(loadedKey!==key){
  const p=cd.length?cd[cd.length-1].close:1,dg=digits(p);
  candle.applyOptions({priceFormat:{type:'price',precision:dg,minMove:Math.pow(10,-dg)}});
  candle.setData(cd);wt1s.setData(w1);wt2s.setData(w2);
  const n=cd.length;pc.timeScale().setVisibleLogicalRange({from:Math.max(0,n-110),to:n+6});
  loadedKey=key;lastT=cd.length?cd[cd.length-1].time:0;
 }else if(cd.length){
  /* nur die letzte Kerze aktualisieren bzw. neue anhaengen (aeltere Zeiten wuerde die Bibliothek ablehnen) */
  const upd=(s,arr)=>{for(const p of arr){if(p.time>=lastT)s.update(p)}};
  try{upd(candle,cd);upd(wt1s,w1);upd(wt2s,w2)}
  catch(e){candle.setData(cd);wt1s.setData(w1);wt2s.setData(w2)}
  lastT=cd[cd.length-1].time;
 }
 const ms=d.markers.map(m=>({time:m.time+tzoff,pos:m.kind==='long'?'belowBar':'aboveBar',color:m.kind==='long'?G:R,shape:m.kind==='long'?'arrowUp':'arrowDown',text:m.ext?'◆':m.strong?'◉':''}));
 candle.setMarkers(ms.map(m=>({time:m.time,position:m.pos,color:m.color,shape:m.shape,text:m.text})));
 wt2s.setMarkers(d.markers.map(m=>({time:m.time+tzoff,position:'inBar',color:m.kind==='long'?G:R,shape:m.ext?'square':'circle',size:m.strong?2:1})));
 $('wt1v').textContent='WT1 '+(d.wt1v>0?'+':'')+d.wt1v;$('wt2v').textContent='WT2 '+(d.wt2v>0?'+':'')+d.wt2v;$('wdv').textContent='Δ '+(d.diff>0?'+':'')+d.diff+(d.warm?'':'  ⚠ nur '+d.bars+' Kerzen Historie – Indikator noch ungenau');
 const f=d.markers.slice(-5).reverse();
 $('feed').innerHTML=f.length?f.map(m=>{const l=m.kind==='long',t=new Date(m.time*1000).toLocaleTimeString('de-DE',{hour:'2-digit',minute:'2-digit',second:'2-digit'});
  return '<div class="feed"><span class="mono" style="color:var(--mut)">'+t+' · '+d.tf+'</span><span class="pill '+(l?'g':'r')+'" style="padding:3px 10px">'+(l?'Long':'Short')+' '+(m.ext?'◆':m.strong?'◉':'●')+'</span><span class="mono" style="color:#cfd5e0">'+fmtP(m.price)+'</span></div>'}).join(''):'<div class="small">Noch keine Signale im sichtbaren Bereich.</div>';
}
function renderTfStates(s){
 $('tfs').innerHTML=TFS.map(t=>{const v=s[t];const txt=v===undefined||v===null?'…':v===1?'LONG':v===-1?'SHORT':'ABWARTEN';const c=v===1?G:v===-1?R:'#7d8696';
  return '<div><span style="letter-spacing:.08em;color:var(--mut)">'+t+'</span><span style="color:'+c+'">'+txt+'</span></div>'}).join('');
}
async function loadChart(){
 if(!coin)return;const key=coin+'|'+tf;
 try{const d=await api('/api/scalp/chart?coin='+coin+'&tf='+tf);if(key!==coin+'|'+tf)return;applyChart(d,key)}catch(e){}
}
async function pollChart(){await loadChart();setTimeout(pollChart,1000)}

/* ---------- Status ---------- */
let chipsSig='';
$('chips').addEventListener('click',ev=>{const b=ev.target.closest('.chip');if(b)switchCoin(b.dataset.c)});
function renderChips(list){
 /* Buttons bleiben bestehen (sonst gehen Klicks verloren, weil sie jede Sekunde neu gebaut wuerden) - nur der Inhalt wird aktualisiert */
 const sig=list.map(c=>c.coin).join(',');
 if(sig!==chipsSig){chipsSig=sig;$('chips').innerHTML=list.map(c=>'<button class="chip" data-c="'+c.coin+'"></button>').join('')}
 const bs=$('chips').children;
 list.forEach((c,i)=>{const b=bs[i];if(!b)return;b.className='chip'+(c.coin===coin?' on':'')+(c.binance?'':' off');b.innerHTML=chipHtml(c)});
}
function chipHtml(c){
 return (c=>{const up=c.chg24==null?'':(c.chg24>=0?'+':'')+c.chg24.toFixed(2)+'%',col=c.chg24==null?'var(--mut)':c.chg24>=0?G:R;
  const dot=c.pos==='long'?G:c.pos==='short'?R:'#3a4150';
  return '<span class="t">'+c.coin+'<span class="dot" style="background:'+dot+'"></span></span><span class="mono" style="font-size:11px;color:#aab2c0">'+fmtP(c.price)+'</span><span class="mono" style="font-size:11px;font-weight:700;color:'+col+'">'+(up||(c.auto?'AUTO':'&nbsp;'))+'</span>'})(c);
}
function setChk(id,v){$('a-'+id).checked=!!v;$('c-'+id).classList.toggle('on',!!v)}
function renderStatus(s){
 renderChips(s.coins);
 const c=s.coin;if(!c)return;
 $('cname').textContent=c.symbol;$('cprice').textContent=fmtP(c.price);$('pPrice').textContent=fmtP(c.price);
 const b=$('badge');b.textContent=c.dry_run?'DRY-RUN':'LIVE';b.className='pill '+(c.dry_run?'y':'r');
 $('sizeTxt').innerHTML='Bot-Größe: <b style="color:#fff">'+c.margin+' USDC × '+c.leverage+'x = '+Number(c.notional).toLocaleString('en-US')+' USDC</b> (leer lassen = diese Größe)';window.__notional=c.notional;
 $('posT').textContent='Position · '+c.symbol;
 const p=c.position;
 if(p){const l=p.side==='long';$('posPill').textContent=l?'LONG':'SHORT';$('posPill').className='pill '+(l?'g':'r');
  $('pSize').textContent=(p.size!=null?Number(p.size).toFixed(4):'–')+' '+c.symbol;$('pAvg').textContent=fmtP(p.avg);
  const pn=p.pnl;$('pPnl').textContent=pn==null?'–':sg(pn)+(p.roi!=null?' ('+(p.roi>=0?'+':'−')+Math.abs(p.roi).toFixed(1)+'%)':'');$('pPnl').style.color=pn==null?'#fff':pn>=0?G:R}
 else{$('posPill').textContent='FLAT';$('posPill').className='pill n';['pSize','pAvg','pPnl'].forEach(i=>{$(i).textContent='–';$(i).style.color='#fff'})}
 $('bClose').disabled=!p;
 /* Linien */
 if(!drag){
  setLine('entry',p&&p.avg?p.avg:null,{color:'rgba(233,236,241,.7)',title:p?(p.side==='long'?'LONG ':'SHORT ')+(p.pnl!=null?sg(p.pnl):''):''});
  vals.tp=c.tp;vals.sl=c.sl;
  setLine('tp',c.tp,{color:G,title:c.tp!=null?lineTitle('tp',c.tp):''});setLine('sl',c.sl,{color:R,title:c.sl!=null?lineTitle('sl',c.sl):''});
  if(document.activeElement!==$('tp-in'))$('tp-in').value=c.tp!=null?c.tp.toFixed(digits(c.tp)):'';
  if(document.activeElement!==$('sl-in'))$('sl-in').value=c.sl!=null?c.sl.toFixed(digits(c.sl)):'';
  updTpslPnl();
 }
 /* Autotrade */
 const a=c.auto;$('autoSw').className='sw'+(a.enabled?' on':'');$('autoSw').firstChild.textContent=a.enabled?'AN':'AUS';
 $('autoCard').style.borderColor=a.enabled?G:'';$('autoCard').style.boxShadow=a.enabled?'0 0 18px rgba(31,207,110,.12)':'';
 $('autoT').textContent='Autotrade · '+c.symbol;$('aTf').textContent='folgt Chart · '+(a.enabled?a.tf:tf);
 ['normal','strong','ext','long','short'].forEach(k=>{if(!autoDirty)setChk(k,a[k])});
 if(document.activeElement!==$('a-usd')&&!autoDirty)$('a-usd').value=a.usd||'';if(document.activeElement!==$('a-tp')&&!autoDirty)$('a-tp').value=a.tp_usd;if(document.activeElement!==$('a-sl')&&!autoDirty)$('a-sl').value=a.sl_usd;
 const lg=(s.log||[]).slice(0,4);
 $('alog').innerHTML=lg.length?lg.map(e=>{const t=new Date(e.t*1000).toLocaleTimeString('de-DE',{hour:'2-digit',minute:'2-digit'});return '<div class="feed" style="justify-content:flex-start;gap:8px"><span class="mono" style="color:var(--mut)">'+t+'</span><span>'+e.coin+' · '+e.text+'</span></div>'}).join(''):'';
}
let autoDirty=false;
async function pollStatusOnce(){
 try{const s=await api('/api/scalp/status'+(coin?('?coin='+coin):''));if(!coin&&s.coins.length){coin=ls('scalp_coin')&&s.coins.some(c=>c.coin===ls('scalp_coin'))?ls('scalp_coin'):s.coins[0].coin;return pollStatusOnce()}ST=s;renderStatus(s)}catch(e){}
}
async function pollStatus(){await pollStatusOnce();setTimeout(pollStatus,1000)}

/* ---------- Wellenanker-Einstellungen ---------- */
function waBusy(){return ['wa-n1','wa-n2','wa-z1','wa-src'].some(i=>document.activeElement===$(i))}
function fillWa(w){$('wa-n1').value=w.n1;$('wa-n2').value=w.n2;$('wa-z1').value=w.z1;$('wa-src').value=w.src}
async function saveWa(){
 const body={n1:$('wa-n1').value,n2:$('wa-n2').value,z1:String($('wa-z1').value).replace(',','.'),src:$('wa-src').value};
 try{const r=await api('/api/scalp/auto',{coin,wa:body});toast(r.persistent?'Wellenanker-Einstellungen gespeichert (Autotrade startet mit neuer Basislinie)':'Übernommen, aber NICHT dauerhaft gespeichert (kein Redis erreichbar) – nach Neustart weg',!r.persistent);loadedKey=null;waKey='';loadChart()}
 catch(e){toast(e.message,true)}
}
['wa-n1','wa-n2','wa-z1','wa-src'].forEach(i=>$(i).addEventListener('change',saveWa));

/* ---------- Aktionen ---------- */
function switchCoin(c){if(c===coin)return;coin=c;ls('scalp_coin',c);loadedKey=null;ST=null;['entry','tp','sl'].forEach(k=>setLine(k,null));candle.setData([]);wt1s.setData([]);wt2s.setData([]);candle.setMarkers([]);wt2s.setMarkers([]);autoDirty=false;$('cname').textContent=c;pollStatusOnce();loadChart()}
function renderTfBar(){$('tfbar').innerHTML=TFS.map(t=>'<button data-t="'+t+'" class="'+(t===tf?'on':'')+'">'+t+'</button>').join('');
 document.querySelectorAll('#tfbar button').forEach(b=>b.addEventListener('click',async()=>{tf=b.dataset.t;ls('scalp_tf',tf);loadedKey=null;renderTfBar();loadChart();
  if(ST&&ST.coin&&ST.coin.auto.enabled){try{await api('/api/scalp/auto',{coin,tf});toast('Autotrade folgt jetzt '+tf+' (neue Basislinie)')}catch(e){toast(e.message,true)}pollStatusOnce()}}))}
async function order(dir){
 const c=ST&&ST.coin;if(!c)return;
 const usd=parseFloat($('o-usd').value.replace(',','.'))||null;
 try{await api('/api/scalp/order',{coin,direction:dir,usd});toast((dir==='long'?'Long':'Short')+' ausgeführt'+(c.dry_run?' (Dry-Run)':''))}catch(e){toast(e.message,true)}
 pollStatusOnce();
}
$('bLong').addEventListener('click',()=>order('long'));$('bShort').addEventListener('click',()=>order('short'));
$('bClose').addEventListener('click',async()=>{try{await api('/api/scalp/close',{coin});toast('Position geschlossen')}catch(e){toast(e.message,true)}pollStatusOnce()});
async function autoSend(extra){
 const body=Object.assign({coin,tf,normal:$('a-normal').checked,strong:$('a-strong').checked,ext:$('a-ext').checked,long:$('a-long').checked,short:$('a-short').checked,
  usd:parseFloat($('a-usd').value.replace(',','.'))||0,tp_usd:parseFloat($('a-tp').value.replace(',','.'))||0,sl_usd:parseFloat($('a-sl').value.replace(',','.'))||0},extra||{});
 try{await api('/api/scalp/auto',body)}catch(e){toast(e.message,true)}
 autoDirty=false;pollStatusOnce();
}
['normal','strong','ext','long','short'].forEach(k=>$('a-'+k).addEventListener('change',()=>{$('c-'+k).classList.toggle('on',$('a-'+k).checked);autoDirty=true;autoSend()}));
['a-tp','a-sl','a-usd'].forEach(i=>$(i).addEventListener('change',()=>{autoDirty=true;autoSend()}));
$('autoSw').addEventListener('click',()=>{
 const c=ST&&ST.coin;if(!c)return;const turnOn=!c.auto.enabled;
 if(turnOn&&!c.dry_run&&!confirm('Autotrade LIVE einschalten?\n\nBei jedem passenden Signal ('+tf+') wird auf '+coin+' automatisch eine ECHTE Market-Order ausgeführt ('+c.notional+' USDC).')){return}
 autoSend({enabled:turnOn});
});
document.querySelectorAll('.q button[data-s]').forEach(b=>b.addEventListener('click',()=>{const n=window.__notional||0;if(n)$('o-usd').value=Math.round(n*parseFloat(b.dataset.s))}));
document.querySelectorAll('.inp').forEach(i=>i.addEventListener('focus',()=>setTimeout(()=>i.select(),0)));
renderTfBar();renderTfStates({});pollStatus();pollChart();
</script></body></html>
"""
