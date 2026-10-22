"""
vwap_cross.py - eigene Strategie "EMA 9 + VWAP" (entry_mode "vwap_cross", Config-Praefix vc_*).

Prinzip:
  * EMA 9 (Laenge einstellbar) gegen den VWAP (hlc3 x Volumen, Reset je Tag / Woche / Monat in UTC, Woche beginnt Montag).
    Day-, Week- und Month-VWAP sind einzeln waehlbar.
  * LONG  = EMA 9 kreuzt einen gewaehlten VWAP nach oben, SHORT = nach unten. Signal bei Kerzenschluss.
  * Der Sprung des VWAP beim Start einer neuen Periode zaehlt nie als Kreuzung. Optional ("Erster Cross nach Perioden-Start
    ueberspringen") wird ausserdem der erste echte Cross nach jedem Perioden-Start nicht gehandelt.
  * Stop-Loss: "Kerze" = unter dem Tief der letzten GANZEN Kerze unterhalb des VWAP (Long) bzw. ueber dem Hoch der letzten Kerze
    oberhalb des VWAP (Short), oder fester $-Betrag. Der $-Wert dient bei "Kerze" als Notfall-Limit/Fallback.
  * Take-Profit: Kreuzung eines waehlbaren VWAP in Gegenrichtung (Standard Day), fester $-Gewinn, Gegen-Signal (Gegentrade) oder keiner.
  * Live: Kerzen und Volumen per Binance-REST nur bei neu geschlossener Kerze; VWAP = 15m-Basiskerzen seit Perioden-Beginn
    (zwischengespeichert, siehe vwap_scanner) + Kerzen der gewaehlten Zeitebene. Backtest: Kerzen der Zeitebene inkl. Vorlauf.
"""

import asyncio
import time
import traceback

import binance_ws
from bot_core import BOTS, debug_log, execute_entry, execute_exit, execute_reverse
from strategies import (
    BINANCE_SYMBOL_MAP, BINANCE_FUTURES_ONLY_SYMBOLS, _tf_ms, summarize_backtest_trades,
    _fetch_cached_mo7_backtest_candles, BACKTEST_MAX_CANDLES, _bt_close_trade,
)
from vwap_scanner import _period_start, _ema, _fetch_base, BASE_MS

VC_TFS = ["1m", "3m", "5m", "15m", "30m", "1h", "4h"]
KINDS = ("d", "w", "m")
KIND_NAMES = {"d": "Day", "w": "Week", "m": "Month"}
WARM_DAYS = {"d": 1.0, "w": 7.0, "m": 31.0}
LIVE_CANDLES = 600
SL_LOOKBACK = 200


def _vc_params(cfg):
    res = cfg.get("vc_resolution", "15m")
    use = [k for k, key in (("d", "vc_use_day"), ("w", "vc_use_week"), ("m", "vc_use_month")) if cfg.get(key)]
    sl_mode = cfg.get("vc_sl_mode", "kerze")
    tp_mode = cfg.get("vc_tp_mode", "kreuzung")
    rule = cfg.get("vc_sl_candle_rule", "whole")
    direction = cfg.get("vc_direction_mode", "both")
    tpv = cfg.get("vc_tp_vwap", "d")
    return {
        "res": res if res in VC_TFS else "15m",
        "ema": max(2, min(200, int(cfg.get("vc_ema_len", 9) or 9))),
        "use": use or ["d"],
        "skip_first": bool(cfg.get("vc_skip_first", True)),
        "direction": direction if direction in ("both", "long_only", "short_only") else "both",
        "sl_mode": sl_mode if sl_mode in ("kerze", "usd") else "kerze",
        "rule": rule if rule in ("whole", "close") else "whole",
        "sl_buf": max(0.0, float(cfg.get("vc_sl_buffer_pct", 0.0) or 0.0)),
        "sl_usd": max(0.0, float(cfg.get("vc_sl_usd", 5.0) or 0.0)),
        "tp_mode": tp_mode if tp_mode in ("kreuzung", "fester_betrag", "gegentrade", "aus") else "kreuzung",
        "tp_usd": max(0.0, float(cfg.get("vc_tp_usd", 5.0) or 0.0)),
        "tp_vwap": tpv if tpv in KINDS else "d",
    }


# ============================================================================
# RECHNUNG (rein, ohne Netzwerk)
# ============================================================================
def vc_build(ts, h, l, c, v, bars, ema_len):
    """EMA + VWAP je Kerze. bars = abgeschlossene 15m-Kerzen (live) fuer den Vorlauf vor dem Kerzenfenster, im Backtest None
    (dort laeuft der VWAP direkt ueber alle Kerzen des Zeitrahmens)."""
    n = len(c)
    ema = _ema(c, ema_len)
    items = []
    j0 = 0
    if bars:
        t_w = -(-ts[0] // BASE_MS) * BASE_MS
        for b in bars:
            if b["ts"] < t_w:
                items.append((b["ts"], (b["h"] + b["l"] + b["c"]) / 3.0 * b["v"], b["v"], None))
        j0 = None
        for j in range(n):
            if ts[j] >= t_w:
                if j0 is None:
                    j0 = j
                items.append((ts[j], (h[j] + l[j] + c[j]) / 3.0 * v[j], v[j], j))
        if j0 is None:
            j0 = n
    else:
        for j in range(n):
            items.append((ts[j], (h[j] + l[j] + c[j]) / 3.0 * v[j], v[j], j))
    vw, resets = {}, {}
    for k in KINDS:
        arr = [None] * n
        rs = set()
        cur, spv, sv = None, 0.0, 0.0
        for t, pv, vol, j in items:
            ps = _period_start(t, k)
            if ps != cur:
                if cur is not None and j is not None:
                    rs.add(j)                       # Periodenwechsel auf dieser Kerze (VWAP springt)
                cur, spv, sv = ps, 0.0, 0.0
            spv += pv
            sv += vol
            if j is not None and sv > 0:
                arr[j] = spv / sv
        vw[k], resets[k] = arr, rs
    return {"ema": ema, "vw": vw, "resets": resets, "j0": j0, "n": n}


def vc_signals(arr, p):
    """Kreuzungen je VWAP + Einstiegssignale. -> dict(buy, sell, trig, tp_up, tp_dn, up, dn)."""
    n, ema, j0 = arr["n"], arr["ema"], arr["j0"]
    up = {k: [False] * n for k in KINDS}
    dn = {k: [False] * n for k in KINDS}
    first = {k: [False] * n for k in KINDS}
    for k in KINDS:
        vw, rs = arr["vw"][k], arr["resets"][k]
        seen, crossed = False, False
        for j in range(j0, n):
            if j in rs:
                seen, crossed = True, False
            if j == j0:
                continue
            a, b = vw[j], vw[j - 1]
            if a is None or b is None or j in rs:   # Perioden-Start-Sprung ist keine Kreuzung
                continue
            p0, q = ema[j - 1] - b, ema[j] - a
            if p0 <= 0 < q:
                up[k][j] = True
            elif p0 >= 0 > q:
                dn[k][j] = True
            else:
                continue
            if seen and not crossed:
                first[k][j] = True                  # erster Cross seit dem Perioden-Start
            crossed = True
    buy, sell, trig = [False] * n, [False] * n, [None] * n
    for j in range(j0 + 1, n):
        bk = [k for k in p["use"] if up[k][j] and not (p["skip_first"] and first[k][j])]
        sk = [k for k in p["use"] if dn[k][j] and not (p["skip_first"] and first[k][j])]
        if bk and not sk:
            buy[j], trig[j] = True, bk[0]
        elif sk and not bk:
            sell[j], trig[j] = True, sk[0]
    return {"buy": buy, "sell": sell, "trig": trig, "tp_up": up[p["tp_vwap"]], "tp_dn": dn[p["tp_vwap"]], "up": up, "dn": dn}


def vc_sl_price(direction, j, k, arr, h, l, c, entry, size, p):
    """Stop-Loss-Preis oder None. 'kerze': Extrem der letzten GANZEN Kerze auf der falschen VWAP-Seite (+ Puffer), mit $-Limit/Fallback."""
    sl = None
    long = direction == "long"
    if p["sl_mode"] == "kerze" and k:
        vw = arr["vw"][k]
        buf = p["sl_buf"] / 100.0
        for m in range(j, max(0, j - SL_LOOKBACK) - 1, -1):
            vv = vw[m]
            if vv is None:
                continue
            if long:
                if (h[m] < vv) if p["rule"] == "whole" else (c[m] < vv):
                    sl = l[m] * (1 - buf)
                    break
            else:
                if (l[m] > vv) if p["rule"] == "whole" else (c[m] > vv):
                    sl = h[m] * (1 + buf)
                    break
        if sl is not None and ((long and sl >= entry) or (not long and sl <= entry)):
            sl = None
    if p["sl_usd"] > 0 and size > 0:
        cap = entry - p["sl_usd"] / size if long else entry + p["sl_usd"] / size
        if sl is None:
            sl = cap
        else:
            sl = max(sl, cap) if long else min(sl, cap)     # $-Limit begrenzt den Verlust
    return sl


def vc_tp_price(direction, entry, size, p):
    if p["tp_mode"] != "fester_betrag" or p["tp_usd"] <= 0 or size <= 0:
        return None
    return entry + p["tp_usd"] / size if direction == "long" else entry - p["tp_usd"] / size


def _allowed(target, p):
    return not ((target == "long" and p["direction"] == "short_only") or (target == "short" and p["direction"] == "long_only"))


# ============================================================================
# LIVE
# ============================================================================
def _vc_reset_state(st):
    st["vc_sl"] = None
    st["vc_tp"] = None


async def check_vc_sl_tp(symbol, price):
    """Jeden Tick: SL und fester TP (preisbasiert). Kreuzungs-TP und Gegentrade werden bei Kerzenschluss geprueft."""
    st = BOTS[symbol]["state"]
    pos = st["position"]
    if pos is None or price is None:
        return
    sl, tp = st.get("vc_sl"), st.get("vc_tp")
    if sl is not None and ((pos == "long" and price <= sl) or (pos == "short" and price >= sl)):
        debug_log(f"🚪 [{symbol}] EMA9+VWAP SL: {pos.upper()} @ {price} (Ziel war {round(sl, 6)})")
        await execute_exit(symbol, price, "SL")
        if st["position"] is None:
            _vc_reset_state(st)
        return
    if tp is not None and ((pos == "long" and price >= tp) or (pos == "short" and price <= tp)):
        debug_log(f"🎯 [{symbol}] EMA9+VWAP TP (fester Betrag): {pos.upper()} @ {price} (Ziel war {round(tp, 6)})")
        await execute_exit(symbol, price, "TP-FESTER-BETRAG")
        if st["position"] is None:
            _vc_reset_state(st)


def _vc_market(symbol, cfg):
    mt = cfg.get("binance_market_type", "spot")
    if symbol in BINANCE_FUTURES_ONLY_SYMBOLS:
        mt = "futures"
    return mt if mt in ("spot", "futures") else "spot"


async def _vc_fetch(symbol, cfg, p):
    """-> (Kerzen der Zeitebene als Dicts inkl. laufender Kerze, 15m-Basiskerzen) oder None."""
    pair = BINANCE_SYMBOL_MAP.get(symbol)
    if not pair:
        return None
    mt = _vc_market(symbol, cfg)
    tf_ms = _tf_ms(p["res"])
    now_ms = int(time.time() * 1000)
    tfc = await binance_ws._rest_klines_range(mt, pair, p["res"], now_ms - LIVE_CANDLES * tf_ms, now_ms, LIVE_CANDLES)
    if not tfc or len(tfc) < max(60, p["ema"] * 3):
        return None
    bars = await _fetch_base(mt, pair, now_ms)
    if not bars:
        return None
    return tfc, bars


def _set_levels(st, cfg, target, price, j, k, arr, h, l, c, p):
    entry = st.get("avg_entry_price") or price
    size = st.get("total_coin_size") or 0
    st["vc_sl"] = vc_sl_price(target, j, k, arr, h, l, c, entry, size, p)
    st["vc_tp"] = vc_tp_price(target, entry, size, p)


async def vc_check_candle(symbol, cfg, p, closed, arr, sig, price):
    """Auswertung der neu geschlossenen Kerze (letzter Index)."""
    st = BOTS[symbol]["state"]
    if not cfg["bot_active"] or price is None:
        return
    h = [x["h"] for x in closed]
    l = [x["l"] for x in closed]
    c = [x["c"] for x in closed]
    j = len(closed) - 1
    pos = st["position"]
    target = "long" if sig["buy"][j] else "short" if sig["sell"][j] else None
    trig = sig["trig"][j]
    info = f"{KIND_NAMES.get(trig, '')}-VWAP" if trig else ""
    if pos is not None:
        is_long = pos == "long"
        tp_cross = p["tp_mode"] == "kreuzung" and (sig["tp_dn"][j] if is_long else sig["tp_up"][j])
        opposite = target is not None and target != pos
        if tp_cross or (p["tp_mode"] == "gegentrade" and opposite):
            reason = "TP-KREUZUNG" if tp_cross else "VC-FLIP"
            flip_to = target if opposite and _allowed(target, p) else None
            if flip_to:
                debug_log(f"🔄 [{symbol}] EMA9+VWAP {reason}: {pos.upper()} -> {flip_to.upper()} @ {price} ({info})")
                if await execute_reverse(symbol, price, reason, size_multiplier=1.0):
                    _vc_reset_state(st)
                    if st["position"] is not None:
                        _set_levels(st, cfg, flip_to, price, j, trig, arr, h, l, c, p)
            else:
                debug_log(f"🎯 [{symbol}] EMA9+VWAP {reason}: {pos.upper()} schließen @ {price}")
                await execute_exit(symbol, price, reason)
                if st["position"] is None:
                    _vc_reset_state(st)
        return
    if target is None or not _allowed(target, p):
        return
    debug_log(f"📡 [{symbol}] EMA9+VWAP Signal: {target.upper()} @ {price} ({info})")
    await execute_entry(symbol, target, price, is_add_on=False)
    if st["position"] is not None:
        _set_levels(st, cfg, target, price, j, trig, arr, h, l, c, p)


async def vc_poll_loop(symbol):
    b = BOTS[symbol]
    last_processed = None
    last_heartbeat = 0.0
    warned = False
    while True:
        wait = 2.0
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "vwap_cross" and cfg["bot_active"] and b["state"].get("session_started"):
                st = b["state"]
                p = _vc_params(cfg)
                price = st.get("last_price")
                now = time.time()
                if st["position"] is not None and price is not None:
                    await check_vc_sl_tp(symbol, price)
                tf_ms = _tf_ms(p["res"])
                expect_ts = int(now * 1000) // tf_ms * tf_ms - tf_ms      # Eroeffnung der zuletzt geschlossenen Kerze
                if last_processed is None or expect_ts > last_processed:
                    if not BINANCE_SYMBOL_MAP.get(symbol):
                        if not warned:
                            warned = True
                            debug_log(f"⚠️ [{symbol}] EMA9+VWAP: Coin hat kein Binance-Paar - keine Daten für die Strategie")
                    else:
                        data = await _vc_fetch(symbol, cfg, p)
                        if data:
                            tfc, bars = data
                            now_ms = int(time.time() * 1000)
                            closed = [x for x in tfc if x["ts"] + tf_ms <= now_ms]
                            if closed and closed[-1]["ts"] == expect_ts:
                                plausible = price is None or abs(closed[-1]["c"] - price) / price * 100 <= 2.0
                                if not plausible:
                                    debug_log(f"⚠️ [{symbol}] EMA9+VWAP: Kerzendaten unplausibel (Kerze {closed[-1]['c']} vs. Live {price}) - überspringe.")
                                else:
                                    first_run = last_processed is None
                                    last_processed = expect_ts
                                    if not first_run:         # erste Kerze nach dem Start nur merken, nicht handeln
                                        ts = [x["ts"] for x in closed]
                                        arr = await asyncio.to_thread(
                                            vc_build, ts, [x["h"] for x in closed], [x["l"] for x in closed],
                                            [x["c"] for x in closed], [x["v"] for x in closed], bars, p["ema"])
                                        sig = vc_signals(arr, p)
                                        await vc_check_candle(symbol, cfg, p, closed, arr, sig, price if price is not None else closed[-1]["c"])
                            else:
                                wait = 3.0                    # neue Kerze noch nicht bei Binance verfuegbar -> gleich nochmal
                        else:
                            wait = 5.0
                if now - last_heartbeat > 300:
                    last_heartbeat = now
                    debug_log(f"💓 [{symbol}] EMA9+VWAP aktiv: {p['res']}, VWAP {'/'.join(KIND_NAMES[k] for k in p['use'])}, SL={p['sl_mode']}, TP={p['tp_mode']}, Preis={price}")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] EMA9+VWAP-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        await asyncio.sleep(wait if b["state"].get("position") is None else min(wait, 1.0))


# ============================================================================
# BACKTEST
# ============================================================================
def vc_simulate(ts, o, h, l, c, arr, sig, cfg, p, i_start):
    """Kerze i: zuerst SL / fester TP einer schon offenen Position ueber Hoch/Tief (SL gewinnt bei Gleichzeitigkeit), dann bei
    Kerzenschluss Kreuzungs-TP / Gegentrade, dann das Einstiegssignal zum Schlusskurs."""
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    position, trades = None, []

    def open_pos(target, i):
        size = (margin * leverage) / c[i]
        sl = vc_sl_price(target, i, sig["trig"][i], arr, h, l, c, c[i], size, p)
        tp = vc_tp_price(target, c[i], size, p)
        return {"dir": target, "entry": c[i], "size": size, "entry_i": i, "sl": sl, "tp": tp}

    for i in range(max(1, i_start), n):
        if position is not None and position["entry_i"] < i:
            d, e, sgn = position["dir"], position["entry"], (1 if position["dir"] == "long" else -1)
            sl, tp = position["sl"], position["tp"]
            if sl is not None and ((sgn > 0 and l[i] <= sl) or (sgn < 0 and h[i] >= sl)):
                _bt_close_trade(trades, d, e, sl, position["size"], i, position["entry_i"], "SL", ts=ts)
                position = None
            elif tp is not None and ((sgn > 0 and h[i] >= tp) or (sgn < 0 and l[i] <= tp)):
                _bt_close_trade(trades, d, e, tp, position["size"], i, position["entry_i"], "TP-FESTER-BETRAG", ts=ts)
                position = None
        target = "long" if sig["buy"][i] else "short" if sig["sell"][i] else None
        if position is not None and position["entry_i"] < i:
            d = position["dir"]
            tp_cross = p["tp_mode"] == "kreuzung" and (sig["tp_dn"][i] if d == "long" else sig["tp_up"][i])
            opposite = target is not None and target != d
            if tp_cross or (p["tp_mode"] == "gegentrade" and opposite):
                _bt_close_trade(trades, d, position["entry"], c[i], position["size"], i, position["entry_i"],
                                "TP-KREUZUNG" if tp_cross else "VC-FLIP", ts=ts)
                position = None
        if position is None and target is not None and _allowed(target, p):
            position = open_pos(target, i)
    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)
    return trades


async def run_vc_backtest(symbol, cfg, days, exclude_top_n=1):
    p = _vc_params(cfg)
    need = set(p["use"])
    if p["tp_mode"] == "kreuzung":
        need.add(p["tp_vwap"])
    warm_days = max(WARM_DAYS[k] for k in need) + 1.0      # Vorlauf: Beginn der laengsten Periode + EMA-Einschwingen
    max_candles = BACKTEST_MAX_CANDLES.get("vwap_cross", 100_000)
    candles, err = await _fetch_cached_mo7_backtest_candles(symbol, p["res"], days + warm_days, max_candles, market_type=cfg.get("binance_market_type", "spot"))
    if err:
        return {"error": err}
    if not candles or len(candles[4]) < max(100, p["ema"] * 3):
        return {"error": "Zu wenig historische Kerzen für einen aussagekräftigen Backtest erhalten."}
    ts, o, h, l, c, v = candles
    if not any(v):
        return {"error": "Keine Volumendaten erhalten - der VWAP braucht Volumen."}
    start_ts = ts[-1] - int(days * 86_400_000)
    i_start = next((i for i, t in enumerate(ts) if t >= start_ts), 1)
    arr = await asyncio.to_thread(vc_build, ts, h, l, c, v, None, p["ema"])
    sig = vc_signals(arr, p)
    trades = vc_simulate(ts, o, h, l, c, arr, sig, cfg, p, i_start)
    stats = summarize_backtest_trades(trades, exclude_top_n)
    warm_ok = ts[0] <= start_ts - int((max(WARM_DAYS[k] for k in need)) * 86_400_000)
    note = None if warm_ok else (f"Hinweis: Für den Vorlauf der VWAP-Periode ({', '.join(KIND_NAMES[k] for k in sorted(need))}) gab es nicht genug Kerzen "
                                 f"(Obergrenze {max_candles} Kerzen) - der VWAP zu Beginn des Testzeitraums beginnt später als im Chart. Kürzeren Zeitraum oder größere Zeitebene wählen.")
    sig_buy = sum(1 for i in range(i_start, len(ts)) if sig["buy"][i])
    sig_sell = sum(1 for i in range(i_start, len(ts)) if sig["sell"][i])
    return {
        "symbol": symbol, "entry_mode": "vwap_cross", "resolution": p["res"],
        "requested_days": days, "actual_days_covered": round((ts[-1] - ts[i_start]) / 86_400_000, 1),
        "candles_processed": len(ts), "candle_cap": max_candles, "cache_used": False,
        "signals_buy": sig_buy, "signals_sell": sig_sell, "vwap_note": note,
        "stats": stats,
        "stats_long": summarize_backtest_trades([t for t in trades if t["dir"] == "long"], exclude_top_n),
        "stats_short": summarize_backtest_trades([t for t in trades if t["dir"] == "short"], exclude_top_n),
        "trades": trades,
    }


# ============================================================================
# CHART
# ============================================================================
async def vc_chart_payload(symbol, cfg, ts, o, h, l, c, tf):
    """Kerzen des Charts + Kreuzungs-Markierungen (nur wenn die Chart-Zeitebene der Strategie-Zeitebene entspricht)."""
    p = _vc_params(cfg)
    n = len(c)
    off = max(0, n - 1000)
    candles = [{"time": ts[i] // 1000, "open": o[i], "high": h[i], "low": l[i], "close": c[i]} for i in range(off, n)]
    markers = []
    if tf == p["res"]:
        try:
            data = await _vc_fetch(symbol, cfg, p)
            if data:
                tfc, bars = data
                tf_ms = _tf_ms(p["res"])
                now_ms = int(time.time() * 1000)
                closed = [x for x in tfc if x["ts"] + tf_ms <= now_ms]
                if len(closed) > 30:
                    arr = await asyncio.to_thread(vc_build, [x["ts"] for x in closed], [x["h"] for x in closed], [x["l"] for x in closed],
                                                  [x["c"] for x in closed], [x["v"] for x in closed], bars, p["ema"])
                    sig = vc_signals(arr, p)
                    first_t = candles[0]["time"] if candles else 0
                    for j, x in enumerate(closed):
                        kind = "long" if sig["buy"][j] else "short" if sig["sell"][j] else None
                        if kind and x["ts"] // 1000 >= first_t:
                            markers.append({"time": x["ts"] // 1000, "kind": kind, "price": x["c"]})
        except Exception as e:
            debug_log(f"⚠️ EMA9+VWAP Chart-Markierungen fehlgeschlagen: {e}")
    return {"candles": candles, "wt1": [], "wt2": [], "markers": markers, "levels": [], "entry_mode": "vwap_cross", "wt1v": 0, "wt2v": 0, "bars": n}
