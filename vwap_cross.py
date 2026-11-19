"""
vwap_cross.py - eigene Strategie "EMA 9 + VWAP" (entry_mode "vwap_cross", Config-Praefix vc_*).

Prinzip:
  * EIN VWAP (Pine "Volume Weighted Average Price" von AustrianTradingMachine): waehlbare Quelle (hlc3 / Schluss / hl2 / ohlc4)
    mal Volumen, Reset je Tag / Woche / Monat in UTC (Woche ab Montag). Dazu Standardabweichungs-Baender um den VWAP.
  * LONG  = EMA 9 kreuzt den VWAP nach oben, SHORT = nach unten. JEDE Kreuzung bei Kerzenschluss ist ein Signal.
  * Der Sprung des VWAP beim Start einer neuen Periode zaehlt nie als Kreuzung.
  * Stop-Loss: hinter der VORLETZTEN Kerze - Long unter ihrem Tief, Short ueber ihrem Hoch (plus Puffer). Liegt dieses Extrem
    auf der falschen Seite des Einstiegs, zaehlt ersatzweise das Extrem der Signalkerze.
  * Take-Profit: Kreuzung EMA x VWAP in Gegenrichtung, Wellenanker-Kreuzung (wt1 x wt2, eigene Zeiteinheit moeglich),
    fester $-Gewinn, Gegen-Signal oder keiner.
  * Live: Kerzen und Volumen per Binance-REST nur bei neu geschlossener Kerze; VWAP = 15m-Basiskerzen seit Perioden-Beginn
    (zwischengespeichert, siehe vwap_scanner) + Kerzen der gewaehlten Zeitebene. Backtest: Kerzen der Zeitebene inkl. Vorlauf.
"""

import asyncio
import random
import re
import time
import traceback

import binance_ws
from bot_core import BOTS, debug_log, execute_entry, execute_exit, execute_partial_exit, execute_reverse
from strategies import (
    BINANCE_SYMBOL_MAP, BINANCE_FUTURES_ONLY_SYMBOLS, _tf_ms, summarize_backtest_trades, SUB_MINUTE_RESOLUTIONS,
    _fetch_cached_mo7_backtest_candles, BACKTEST_MAX_CANDLES, _bt_close_trade,
)
from bisect import bisect_left

from vwap_scanner import _period_start, _ema, _fetch_base, BASE_MS

VC_TFS = ["10s", "15s", "30s", "45s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"]
BINANCE_NATIVE_TFS = ("1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d")   # alles andere wird aus kleineren Kerzen zusammengesetzt


def _vc_res(res):
    """Erlaubt die Liste oben und eigene Minuten/Sekunden (z. B. "8m"), wie bei den anderen Strategien."""
    res = str(res or "15m")
    if res in VC_TFS:
        return res
    m = re.match(r"^(\d+)([sm])$", res)
    if m and 1 <= int(m.group(1)) <= 1440:
        return res
    return "15m"
KINDS = ("d", "w", "m")
KIND_NAMES = {"d": "Tag", "w": "Woche", "m": "Monat"}
WARM_DAYS = {"d": 1.0, "w": 7.0, "m": 31.0}
SRC_NAMES = {"hlc3": "hlc3", "close": "Schluss", "hl2": "hl2", "ohlc4": "ohlc4"}
LIVE_CANDLES = 600
SL_LOOKBACK = 200


def _vc_params(cfg):
    res = cfg.get("vc_resolution", "15m")
    wa_src = cfg.get("vc_wa_src", cfg.get("wa_src", "close"))
    wa_tf = str(cfg.get("vc_wa_tf", "") or "").strip().lower()
    wa_tf = wa_tf if (wa_tf and _vc_res(wa_tf) == wa_tf) else ""
    tp_mode = cfg.get("vc_tp_mode", "kreuzung")
    direction = cfg.get("vc_direction_mode", "both")
    anchor = cfg.get("vc_anchor", "d")
    src = cfg.get("vc_source", "hlc3")
    return {
        "res": _vc_res(res),
        "ema": max(2, min(200, int(cfg.get("vc_ema_len", 9) or 9))),
        "anchor": anchor if anchor in KINDS else "d",       # Reset des VWAP: Tag / Woche / Monat
        "src": src if src in SRC_NAMES else "hlc3",         # Quelle des VWAP (Pine-Eingabe "Source")
        "band_mult": max(0.1, float(cfg.get("vc_band_mult", 1.0) or 1.0)),
        "band_vol": bool(cfg.get("vc_band_volume", True)),  # Baender zusaetzlich volumengewichtet (Pine "Volume weighted")
        "direction": direction if direction in ("both", "long_only", "short_only") else "both",
        "sl_buf": max(0.0, float(cfg.get("vc_sl_buffer_pct", 0.0) or 0.0)),
        "be_pct": max(0.0, float(cfg.get("vc_be_pct", 0.0) or 0.0)),       # ab dieser Kursbewegung wandert der SL auf den Einstieg (0 = aus)
        "tp_mode": tp_mode if tp_mode in ("kreuzung", "fester_betrag", "gegentrade", "wellenanker", "aus") else "kreuzung",
        # Wellenanker (WaveTrend) als Ausstieg: wt1 kreuzt wt2 gegen die Position
        "wa_n1": max(1, int(cfg.get("vc_wa_n1", cfg.get("wa_n1", 10)) or 10)),
        "wa_n2": max(1, int(cfg.get("vc_wa_n2", cfg.get("wa_n2", 21)) or 21)),
        "wa_sig": max(1, int(cfg.get("vc_wa_sig_len", cfg.get("wa_sig_len", 4)) or 4)),
        "wa_scale": max(0.1, float(cfg.get("vc_wa_wave_scale", cfg.get("wa_wave_scale", 1.35)) or 1.35)),
        "wa_src": wa_src if wa_src in ("close", "hlc3", "ohlc4", "hl2") else "close",
        "wa_zone": max(0.0, float(cfg.get("vc_wa_zone", 0.0) or 0.0)),   # 0 = jede Kreuzung zaehlt
        "wa_tf": wa_tf,                                 # eigene Zeiteinheit des Wellenankers ("" = wie die Strategie)
        "tp_usd": max(0.0, float(cfg.get("vc_tp_usd", 5.0) or 0.0)),
        "tp1_on": bool(cfg.get("vc_tp1_on", False)),                       # Teilgewinn bei TP1, danach SL auf den Einstieg
        "tp1_rr": max(0.1, float(cfg.get("vc_tp1_rr", 1.0) or 1.0)),       # Vielfaches des Risikos (Abstand Einstieg -> SL)
        "tp1_pct": min(100.0, max(0.0, float(cfg.get("vc_tp1_pct", 50.0) or 0.0))),
        # ADX-Filter: nur handeln, wenn der Trend stark genug ist (0 = aus)
        "adx_min": max(0.0, float(cfg.get("vc_adx_min", 0.0) or 0.0)),
        "adx_len": max(2, int(cfg.get("vc_adx_len", 14) or 14)),
        "adx_dir": bool(cfg.get("vc_adx_directional", False)),
        # gegen Saegezahn-Kreuzungen: Mindestabstand und Bestaetigung ueber mehrere Kerzen
        "cross_min_pct": max(0.0, float(cfg.get("vc_cross_min_pct", 0.0) or 0.0)),
        "confirm": max(1, min(20, int(cfg.get("vc_cross_confirm", 1) or 1))),
    }


def vc_adx_ok(ts, o, h, l, c, p):
    """(long_ok, short_ok) je Kerze oder (None, None), wenn der ADX-Filter aus ist.
    ADX laeuft auf den EIGENEN Kerzen der Strategie, eine zweite Zeitebene braucht er nicht."""
    if p["adx_min"] <= 0:
        return None, None
    from strategies import compute_adx_filter_series
    return compute_adx_filter_series((ts, o or [0.0] * len(c), h, l, c), p["adx_len"], p["adx_min"],
                                     directional=p["adx_dir"], mode="require_trend")


def _vc_adx_text(p):
    return f"ADX({p['adx_len']}) unter {p['adx_min']:g}" + (" oder falsche Richtung (+DI/-DI)" if p["adx_dir"] else "")


def vc_wa_cross(o, h, l, c, p):
    """(up, dn) je Kerze: Kreuzung der Wellenanker-Linien wt1 x wt2, gerechnet wie in der Wellenanker-Strategie.
    up = wt1 kreuzt wt2 nach oben (schliesst einen Short), dn = nach unten (schliesst einen Long).
    Der Zonen-Filter verlangt, dass wt2 weit genug von der Nulllinie weg ist (0 = jede Kreuzung zaehlt)."""
    from strategies import compute_wavetrend_series
    n = len(c)
    wt1, wt2 = compute_wavetrend_series(o or list(c), h, l, c, p["wa_n1"], p["wa_n2"], p["wa_sig"], p["wa_src"], p["wa_scale"])
    z = p["wa_zone"]
    up, dn = [False] * n, [False] * n
    for i in range(1, n):
        if wt1[i] > wt2[i] and wt1[i - 1] <= wt2[i - 1]:
            up[i] = (wt2[i] < -z) if z > 0 else True
        elif wt1[i] < wt2[i] and wt1[i - 1] >= wt2[i - 1]:
            dn[i] = (wt2[i] > z) if z > 0 else True
    return up, dn


def _vc_wa_align(base_ts, base_ms, wts, wa_ms, up, dn):
    """Kreuzungen einer anderen Zeiteinheit auf die Basis-Kerzen legen: eine Kreuzung wirkt ab der ersten Basis-Kerze,
    die NACH dem Abschluss der Wellenanker-Kerze schliesst - so kann der Backtest nicht in die Zukunft schauen."""
    n = len(base_ts)
    bu, bd = [False] * n, [False] * n
    for i in range(len(wts)):
        if not (up[i] or dn[i]):
            continue
        k = bisect_left(base_ts, wts[i] + wa_ms - base_ms)
        if 0 <= k < n:
            if up[i]:
                bu[k] = True
            if dn[i]:
                bd[k] = True
    return bu, bd


async def vc_wa_cross_tf(symbol, cfg, p, base_ts=None, days=None):
    """Wellenanker-Kreuzungen auf seiner EIGENEN Zeiteinheit. Mit base_ts werden sie auf die Basis-Kerzen gelegt
    (Backtest), ohne base_ts kommen die Rohreihen der letzten Kerzen zurueck (live). -> (up, dn) oder None."""
    tf = p["wa_tf"] or p["res"]
    mt = _vc_market(symbol, cfg)
    need = max(200, p["wa_n2"] * 6 + p["wa_sig"] + 10)
    if base_ts is None:
        from strategies import fetch_candles_binance_multi
        d = await fetch_candles_binance_multi(symbol, tf, count_back=need, market_type=mt)
        if not d or len(d[4]) < need // 4:
            return None
        return await asyncio.to_thread(vc_wa_cross, d[1], d[2], d[3], d[4], p), d[0]
    wa_ms, base_ms = _tf_ms(tf), (base_ts[1] - base_ts[0] if len(base_ts) > 1 else _tf_ms(p["res"]))
    warm = need * wa_ms / 86_400_000
    data, err = await _fetch_cached_mo7_backtest_candles(symbol, tf, days + warm + 1, 50_000, market_type=cfg.get("binance_market_type", "spot"))
    if err or not data or len(data[4]) < 50:
        return None
    up, dn = await asyncio.to_thread(vc_wa_cross, data[1], data[2], data[3], data[4], p)
    return _vc_wa_align(base_ts, base_ms, data[0], wa_ms, up, dn)


def _src_price(src, o, h, l, c):
    """Quelle einer Kerze. o kann None sein (15m-Basiskerzen des Vorlaufs haben keine Eroeffnung) -> dann hlc3."""
    if src == "close":
        return c
    if src == "hl2":
        return (h + l) / 2.0
    if src == "ohlc4" and o is not None:
        return (o + h + l + c) / 4.0
    return (h + l + c) / 3.0


# ============================================================================
# RECHNUNG (rein, ohne Netzwerk)
# ============================================================================
def vc_build(ts, o, h, l, c, v, bars, p):
    """EMA, VWAP und Standardabweichungs-Baender je Kerze. bars = abgeschlossene 15m-Kerzen (live) fuer den Vorlauf vor dem
    Kerzenfenster, im Backtest None (dort laeuft der VWAP direkt ueber alle Kerzen des Zeitrahmens)."""
    n = len(c)
    ema = _ema(c, p["ema"])
    src, k = p["src"], p["anchor"]
    items = []
    j0 = 0
    if bars:
        t_w = -(-ts[0] // BASE_MS) * BASE_MS
        for b in bars:
            if b["ts"] < t_w:
                items.append((b["ts"], _src_price(src, b.get("o"), b["h"], b["l"], b["c"]), b["v"], None))
        j0 = None
        for j in range(n):
            if ts[j] >= t_w:
                if j0 is None:
                    j0 = j
                items.append((ts[j], _src_price(src, o[j] if o else None, h[j], l[j], c[j]), v[j], j))
        if j0 is None:
            j0 = n
    else:
        for j in range(n):
            items.append((ts[j], _src_price(src, o[j] if o else None, h[j], l[j], c[j]), v[j], j))
    vw = [None] * n
    sd = [None] * n
    rs = set()
    cur = None
    spv = sv = 0.0            # fuer den VWAP: immer mit Volumen gewichtet
    sw = swx = swx2 = 0.0     # fuer die Baender: Gewicht nach Einstellung
    cnt = 0
    for t, px, vol, j in items:
        ps = _period_start(t, k)
        if ps != cur:
            if cur is not None and j is not None:
                rs.add(j)                       # Periodenwechsel auf dieser Kerze (VWAP springt)
            cur, spv, sv, sw, swx, swx2, cnt = ps, 0.0, 0.0, 0.0, 0.0, 0.0, 0
        w = vol if p["band_vol"] else 1.0
        spv += px * vol
        sv += vol
        sw += w
        swx += w * px
        swx2 += w * px * px
        cnt += 1
        if j is not None and sv > 0:
            vw[j] = spv / sv
            if cnt > 1 and sw > 0:
                mu = swx / sw
                var = max(0.0, swx2 / sw - mu * mu)
                sd[j] = var ** 0.5
            else:
                sd[j] = 0.0
    return {"ema": ema, "vw": vw, "sd": sd, "resets": rs, "j0": j0, "n": n}


def vc_signals(arr, p):
    """Kreuzung EMA x VWAP bei Kerzenschluss. Zwei Filter gegen Saegezahn-Kreuzungen:
      * Bestaetigung: die EMA muss N Kerzen lang auf der neuen Seite bleiben; das Signal faellt auf die N-te Kerze.
      * Mindestabstand: auf der Signalkerze muss die EMA um mindestens X % vom VWAP entfernt sein.
    Beide wirken auf Einstieg UND Kreuzungs-Ausstieg. N = 1 und X = 0 ist die ungefilterte Kreuzung.
    -> dict(buy, sell, trig, tp_up, tp_dn)."""
    n, ema, j0 = arr["n"], arr["ema"], arr["j0"]
    vw, rs = arr["vw"], arr["resets"]
    buy, sell = [False] * n, [False] * n
    need, minp = p["confirm"], p["cross_min_pct"] / 100.0
    for j in range(j0 + 1, n):
        a, b = vw[j], vw[j - 1]
        if a is None or b is None or j in rs:       # Perioden-Start-Sprung ist keine Kreuzung
            continue
        p0, q = ema[j - 1] - b, ema[j] - a
        up = p0 <= 0 < q
        dn = p0 >= 0 > q
        if not (up or dn):
            continue
        k = j + need - 1                            # Kerze, auf der das bestaetigte Signal faellt
        if k >= n:
            continue
        ok = True
        for m in range(j, k + 1):
            if vw[m] is None or (m > j and m in rs):    # Perioden-Sprung mittendrin -> nicht vergleichbar
                ok = False
                break
            d = ema[m] - vw[m]
            if (up and d <= 0) or (dn and d >= 0):      # EMA faellt zurueck -> keine Bestaetigung
                ok = False
                break
        if not ok:
            continue
        if minp > 0 and (not vw[k] or abs(ema[k] - vw[k]) / vw[k] < minp):
            continue
        if up:
            buy[k] = True
        else:
            sell[k] = True
    for j in range(n):
        if buy[j] and sell[j]:                      # beide Richtungen auf derselben Kerze -> nichts tun
            buy[j] = sell[j] = False
    return {"buy": buy, "sell": sell, "trig": [None] * n, "tp_up": buy, "tp_dn": sell}


def vc_sl_price(direction, j, h, l, entry, p):
    """Stop-Loss hinter der VORLETZTEN Kerze: Long unter ihrem Tief, Short ueber ihrem Hoch (plus Puffer).
    Liegt dieses Extrem auf der falschen Seite des Einstiegs, zaehlt ersatzweise das Extrem der Signalkerze j."""
    long = direction == "long"
    buf = p["sl_buf"] / 100.0
    for m in (j - 1, j):
        if m < 0:
            continue
        sl = l[m] * (1 - buf) if long else h[m] * (1 + buf)
        if (long and sl < entry) or (not long and sl > entry):
            return sl
    return None


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
    st["vc_tp1"] = None
    st["vc_tp1_done"] = False
    st["vc_be_done"] = False


def _vc_tp1_price(direction, entry, sl, p):
    """TP1 = Einstieg + Vielfaches des Risikos. Ohne Stop-Loss gibt es kein Risiko und damit kein TP1."""
    if not p["tp1_on"] or sl is None or not entry:
        return None
    risk = abs(entry - sl)
    if risk <= 0:
        return None
    return entry + risk * p["tp1_rr"] if direction == "long" else entry - risk * p["tp1_rr"]


def _vc_be_trigger(direction, entry, be_pct):
    """Kurs, ab dem der Stop-Loss auf den Einstieg wandert. None, wenn Breakeven aus ist."""
    if not be_pct or not entry:
        return None
    return entry * (1 + be_pct / 100.0) if direction == "long" else entry * (1 - be_pct / 100.0)


async def check_vc_sl_tp(symbol, price):
    """Jeden Tick: SL und fester TP (preisbasiert). Kreuzungs-TP und Gegentrade werden bei Kerzenschluss geprueft."""
    st = BOTS[symbol]["state"]
    pos = st["position"]
    if pos is None or price is None:
        return
    cfg = BOTS[symbol]["config"]
    p = _vc_params(cfg)
    entry = st.get("avg_entry_price")
    tp1 = st.get("vc_tp1")
    if tp1 is not None and not st.get("vc_tp1_done") and ((pos == "long" and price >= tp1) or (pos == "short" and price <= tp1)):
        pct = p["tp1_pct"]
        st["vc_tp1_done"] = True
        if pct >= 100:
            debug_log(f"🎯 [{symbol}] EMA9+VWAP TP1: {pos.upper()} @ {price} - ganze Position")
            await execute_exit(symbol, price, "TP1")
            if st["position"] is None:
                _vc_reset_state(st)
            return
        if pct > 0:
            await execute_partial_exit(symbol, price, pct / 100.0, "TP1")
        if st["position"] is not None and entry:
            st["vc_sl"] = entry          # bei TP1 wandert der Stop-Loss auf den Einstieg
            st["vc_be_done"] = True
            debug_log(f"🎯 [{symbol}] EMA9+VWAP TP1: {pos.upper()} @ {price}, {pct:.0f}% verkauft - SL auf den Einstieg {round(entry, 6)}")
        return
    be_pct = max(0.0, float(cfg.get("vc_be_pct", 0.0) or 0.0))
    trig = _vc_be_trigger(pos, entry, be_pct)
    if trig is not None and not st.get("vc_be_done") and ((pos == "long" and price >= trig) or (pos == "short" and price <= trig)):
        old = st.get("vc_sl")
        # nur nachziehen, nie lockern
        if old is None or (pos == "long" and entry > old) or (pos == "short" and entry < old):
            st["vc_sl"] = entry
            debug_log(f"🛡️ [{symbol}] EMA9+VWAP Breakeven: {pos.upper()} {be_pct}% im Plus @ {price} - SL auf den Einstieg {round(entry, 6)}")
        st["vc_be_done"] = True
    sl, tp = st.get("vc_sl"), st.get("vc_tp")
    if sl is not None and ((pos == "long" and price <= sl) or (pos == "short" and price >= sl)):
        reason = "BREAKEVEN" if st.get("vc_be_done") and entry and abs(sl - entry) < 1e-12 else "SL"
        debug_log(f"🚪 [{symbol}] EMA9+VWAP {reason}: {pos.upper()} @ {price} (Ziel war {round(sl, 6)})")
        await execute_exit(symbol, price, reason)
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
    if p["res"] in BINANCE_NATIVE_TFS:
        tfc = await binance_ws._rest_klines_range(mt, pair, p["res"], now_ms - LIVE_CANDLES * tf_ms, now_ms, LIVE_CANDLES)
    else:
        from strategies import fetch_candles_binance_multi      # Sekunden-Kerzen und eigene Minuten werden dort zusammengesetzt
        d = await fetch_candles_binance_multi(symbol, p["res"], count_back=LIVE_CANDLES, market_type=mt)
        tfc = [{"ts": d[0][i], "o": d[1][i], "h": d[2][i], "l": d[3][i], "c": d[4][i], "v": d[5][i]} for i in range(len(d[0]))] if d else None
    if not tfc or len(tfc) < max(60, p["ema"] * 3):
        return None
    bars = await _fetch_base(mt, pair, now_ms)
    if not bars:
        return None
    return tfc, bars


def _set_levels(st, cfg, target, price, j, h, l, p):
    entry = st.get("avg_entry_price") or price
    size = st.get("total_coin_size") or 0
    st["vc_sl"] = vc_sl_price(target, j, h, l, entry, p)
    st["vc_tp"] = vc_tp_price(target, entry, size, p)
    st["vc_tp1"] = _vc_tp1_price(target, entry, st["vc_sl"], p)
    st["vc_tp1_done"] = False
    st["vc_be_done"] = False


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
    info = f"{KIND_NAMES[p['anchor']]}-VWAP ({SRC_NAMES[p['src']]})"
    adx_ok = True
    if target is not None and p["adx_min"] > 0:
        lo, so = vc_adx_ok([x["ts"] for x in closed], [x.get("o") or x["c"] for x in closed], h, l, c, p)
        adx_ok = (lo if target == "long" else so)[j]
    if pos is not None:
        is_long = pos == "long"
        tp_cross = p["tp_mode"] == "kreuzung" and (sig["tp_dn"][j] if is_long else sig["tp_up"][j])
        if p["tp_mode"] == "wellenanker" and not p["wa_tf"]:     # eigene Zeiteinheit wird in der Schleife geprueft
            wa_up, wa_dn = await asyncio.to_thread(vc_wa_cross, [x.get("o") or x["c"] for x in closed], h, l, c, p)
            tp_cross = wa_dn[j] if is_long else wa_up[j]
        opposite = target is not None and target != pos
        if tp_cross or (p["tp_mode"] == "gegentrade" and opposite):
            reason = ("TP-WELLENANKER" if p["tp_mode"] == "wellenanker" else "TP-KREUZUNG") if tp_cross else "VC-FLIP"
            flip_to = target if opposite and _allowed(target, p) and adx_ok else None
            if flip_to:
                debug_log(f"🔄 [{symbol}] EMA9+VWAP {reason}: {pos.upper()} -> {flip_to.upper()} @ {price} ({info})")
                if await execute_reverse(symbol, price, reason, size_multiplier=1.0):
                    _vc_reset_state(st)
                    if st["position"] is not None:
                        _set_levels(st, cfg, flip_to, price, j, h, l, p)
            else:
                debug_log(f"🎯 [{symbol}] EMA9+VWAP {reason}: {pos.upper()} schließen @ {price}")
                await execute_exit(symbol, price, reason)
                if st["position"] is None:
                    _vc_reset_state(st)
        return
    if target is None or not _allowed(target, p):
        return
    if not adx_ok:
        debug_log(f"⛔ [{symbol}] EMA9+VWAP Signal {target.upper()} verworfen - {_vc_adx_text(p)}")
        return
    debug_log(f"📡 [{symbol}] EMA9+VWAP Signal: {target.upper()} @ {price} ({info})")
    await execute_entry(symbol, target, price, is_add_on=False)
    if st["position"] is not None:
        _set_levels(st, cfg, target, price, j, h, l, p)


async def vc_check_wa_exit(symbol, cfg, p, price):
    """Ausstieg nach der Wellenanker-Kreuzung auf DESSEN eigener Zeiteinheit. True = geprueft (Kerzen waren da)."""
    st = BOTS[symbol]["state"]
    if st["position"] is None or price is None:
        return True
    got = await vc_wa_cross_tf(symbol, cfg, p)
    if not got:
        return False
    (up, dn), _wts = got
    i = len(up) - 1
    hit = dn[i] if st["position"] == "long" else up[i]
    if hit:
        debug_log(f"🎯 [{symbol}] EMA9+VWAP TP-WELLENANKER ({p['wa_tf']}): {st['position'].upper()} schließen @ {price}")
        await execute_exit(symbol, price, "TP-WELLENANKER")
        if st["position"] is None:
            _vc_reset_state(st)
    return True


async def vc_poll_loop(symbol):
    b = BOTS[symbol]
    last_wa = None
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
                                            vc_build, ts, [x.get("o") for x in closed], [x["h"] for x in closed], [x["l"] for x in closed],
                                            [x["c"] for x in closed], [x["v"] for x in closed], bars, p)
                                        sig = vc_signals(arr, p)
                                        await vc_check_candle(symbol, cfg, p, closed, arr, sig, price if price is not None else closed[-1]["c"])
                            else:
                                wait = 3.0                    # neue Kerze noch nicht bei Binance verfuegbar -> gleich nochmal
                        else:
                            wait = 5.0
                if (st["position"] is not None and p["tp_mode"] == "wellenanker" and p["wa_tf"] and p["wa_tf"] != p["res"]
                        and BINANCE_SYMBOL_MAP.get(symbol)):
                    wa_ms = _tf_ms(p["wa_tf"])
                    expect_wa = int(now * 1000) // wa_ms * wa_ms - wa_ms
                    if last_wa is None or expect_wa > last_wa:
                        if await vc_check_wa_exit(symbol, cfg, p, price):
                            last_wa = expect_wa
                        else:
                            wait = min(wait, 3.0)
                if now - last_heartbeat > 300:
                    last_heartbeat = now
                    debug_log(f"💓 [{symbol}] EMA9+VWAP aktiv: {p['res']}, VWAP {KIND_NAMES[p['anchor']]}/{SRC_NAMES[p['src']]}, SL=vorletzte Kerze, TP={p['tp_mode']}, Preis={price}")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] EMA9+VWAP-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        await asyncio.sleep(wait if b["state"].get("position") is None else min(wait, 1.0))


# ============================================================================
# BACKTEST
# ============================================================================
def vc_simulate(ts, o, h, l, c, arr, sig, cfg, p, i_start, adx=None, wa=None):
    """Kerze i: zuerst SL / fester TP einer schon offenen Position ueber Hoch/Tief (SL gewinnt bei Gleichzeitigkeit), dann bei
    Kerzenschluss Kreuzungs-TP / Gegentrade, dann das Einstiegssignal zum Schlusskurs."""
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    position, trades = None, []

    def open_pos(target, i):
        size = (margin * leverage) / c[i]
        sl = vc_sl_price(target, i, h, l, c[i], p)
        tp = vc_tp_price(target, c[i], size, p)
        return {"dir": target, "entry": c[i], "size": size, "entry_i": i, "sl": sl, "tp": tp, "be": False,
                "tp1": _vc_tp1_price(target, c[i], sl, p), "tp1_done": False}

    for i in range(max(1, i_start), n):
        if position is not None and position["entry_i"] < i:
            d, e, sgn = position["dir"], position["entry"], (1 if position["dir"] == "long" else -1)
            sl, tp = position["sl"], position["tp"]
            if sl is not None and ((sgn > 0 and l[i] <= sl) or (sgn < 0 and h[i] >= sl)):
                reason = "BREAKEVEN" if position["be"] and abs(sl - e) < 1e-12 else "SL"
                _bt_close_trade(trades, d, e, sl, position["size"], i, position["entry_i"], reason, ts=ts)
                position = None
            elif tp is not None and ((sgn > 0 and h[i] >= tp) or (sgn < 0 and l[i] <= tp)):
                _bt_close_trade(trades, d, e, tp, position["size"], i, position["entry_i"], "TP-FESTER-BETRAG", ts=ts)
                position = None
            else:
                t1 = position["tp1"]
                if t1 is not None and not position["tp1_done"] and ((sgn > 0 and h[i] >= t1) or (sgn < 0 and l[i] <= t1)):
                    pct = p["tp1_pct"]
                    position["tp1_done"] = True
                    if pct >= 100:
                        _bt_close_trade(trades, d, e, t1, position["size"], i, position["entry_i"], "TP1", ts=ts)
                        position = None
                    else:
                        if pct > 0:
                            _bt_close_trade(trades, d, e, t1, position["size"] * pct / 100.0, i, position["entry_i"], "TP1", ts=ts)
                            position["size"] -= position["size"] * pct / 100.0
                        position["sl"] = e          # bei TP1 wandert der Stop-Loss auf den Einstieg
                        position["be"] = True
                if position is None:
                    continue
                trig = _vc_be_trigger(d, e, p["be_pct"])     # Breakeven erst nach der SL-Pruefung dieser Kerze
                if trig is not None and not position["be"] and ((sgn > 0 and h[i] >= trig) or (sgn < 0 and l[i] <= trig)):
                    if position["sl"] is None or (sgn > 0 and e > position["sl"]) or (sgn < 0 and e < position["sl"]):
                        position["sl"] = e
                    position["be"] = True
        target = "long" if sig["buy"][i] else "short" if sig["sell"][i] else None
        if target is not None and adx is not None and not (adx[0] if target == "long" else adx[1])[i]:
            target = None                                   # ADX zu schwach (oder falsche Richtung)
        if position is not None and position["entry_i"] < i:
            d = position["dir"]
            tp_cross = p["tp_mode"] == "kreuzung" and (sig["tp_dn"][i] if d == "long" else sig["tp_up"][i])
            if p["tp_mode"] == "wellenanker" and wa is not None:
                tp_cross = (wa[1][i] if d == "long" else wa[0][i])
            opposite = target is not None and target != d
            if tp_cross or (p["tp_mode"] == "gegentrade" and opposite):
                _bt_close_trade(trades, d, position["entry"], c[i], position["size"], i, position["entry_i"],
                                (("TP-WELLENANKER" if p["tp_mode"] == "wellenanker" else "TP-KREUZUNG") if tp_cross else "VC-FLIP"), ts=ts)
                position = None
        if position is None and target is not None and _allowed(target, p):
            position = open_pos(target, i)
    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)
    return trades


async def run_vc_backtest(symbol, cfg, days, exclude_top_n=1):
    p = _vc_params(cfg)
    need = {p["anchor"]}
    warm_days = WARM_DAYS[p["anchor"]] + 1.0               # Vorlauf: Beginn der VWAP-Periode + EMA-Einschwingen
    max_candles = BACKTEST_MAX_CANDLES.get("vwap_cross", 100_000)
    if p["res"] in SUB_MINUTE_RESOLUTIONS:
        max_candles = min(max_candles, 5000)       # Sekunden-Kerzen werden aus 1s-Daten gebaut - sonst laeuft der Abruf ins Zeitlimit
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
    arr = await asyncio.to_thread(vc_build, ts, o, h, l, c, v, None, p)
    sig = vc_signals(arr, p)
    lo, so = vc_adx_ok(ts, o, h, l, c, p)
    wa = None
    if p["tp_mode"] == "wellenanker":
        if p["wa_tf"] and p["wa_tf"] != p["res"]:
            wa = await vc_wa_cross_tf(symbol, cfg, p, ts, days + warm_days)
            if wa is None:
                return {"error": f"Keine Kerzen für die Wellenanker-Zeiteinheit {p['wa_tf']} erhalten."}
        else:
            wa = await asyncio.to_thread(vc_wa_cross, o, h, l, c, p)
    trades = vc_simulate(ts, o, h, l, c, arr, sig, cfg, p, i_start, (lo, so) if lo is not None else None, wa)
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
    markers, lines = [], []
    if tf == p["res"]:
        try:
            data = await _vc_fetch(symbol, cfg, p)
            if data:
                tfc, bars = data
                tf_ms = _tf_ms(p["res"])
                now_ms = int(time.time() * 1000)
                closed = [x for x in tfc if x["ts"] + tf_ms <= now_ms]
                if len(closed) > 30:
                    arr = await asyncio.to_thread(vc_build, [x["ts"] for x in closed], [x.get("o") for x in closed], [x["h"] for x in closed],
                                                  [x["l"] for x in closed], [x["c"] for x in closed], [x["v"] for x in closed], bars, p)
                    sig = vc_signals(arr, p)
                    # Pfeile nur dort, wo ALLE Einstellungen passen - sonst zeigt der Chart Signale,
                    # die der Bot gar nicht nehmen wuerde (ADX-Filter, Richtungsbegrenzung).
                    lo, so = vc_adx_ok([x["ts"] for x in closed], [x.get("o") or x["c"] for x in closed],
                                       [x["h"] for x in closed], [x["l"] for x in closed], [x["c"] for x in closed], p)
                    first_t = candles[0]["time"] if candles else 0
                    for j, x in enumerate(closed):
                        kind = "long" if sig["buy"][j] else "short" if sig["sell"][j] else None
                        if not kind or x["ts"] // 1000 < first_t:
                            continue
                        if not _allowed(kind, p):
                            continue
                        if lo is not None and not (lo[j] if kind == "long" else so[j]):
                            continue
                        markers.append({"time": x["ts"] // 1000, "kind": kind, "price": x["c"]})
                    vw, sd, mult = arr["vw"], arr["sd"], p["band_mult"]
                    line, bup, blo = [], [], []
                    for j, x in enumerate(closed):
                        t = x["ts"] // 1000
                        if t < first_t or vw[j] is None:
                            continue
                        line.append({"time": t, "value": vw[j]})
                        if sd[j] is not None:
                            bup.append({"time": t, "value": vw[j] + mult * sd[j]})
                            blo.append({"time": t, "value": vw[j] - mult * sd[j]})
                    lines = [{"key": "vc_vwap", "label": f"VWAP {KIND_NAMES[p['anchor']]}", "data": line},
                             {"key": "vc_bup", "label": "VWAP Band oben", "data": bup},
                             {"key": "vc_blo", "label": "VWAP Band unten", "data": blo}]
        except Exception as e:
            debug_log(f"⚠️ EMA9+VWAP Chart-Markierungen fehlgeschlagen: {e}")
    return {"candles": candles, "wt1": [], "wt2": [], "markers": markers, "levels": [], "entry_mode": "vwap_cross", "wt1v": 0, "wt2v": 0, "bars": n,
            "vc_lines": lines}


# ============================================================================
# MONTE-CARLO (Raster ueber die Werte, die das Signal veraendern)
# ============================================================================
VC_MC_MAX_RUNS = 2000
VC_MC_MAX_VALUES = 12          # je Achse hoechstens so viele Werte


def vc_mc_values(text, cast, lo, hi):
    """"1, 2,3" -> [1, 2, 3]. Ungueltiges und Doppeltes faellt weg, Reihenfolge bleibt."""
    out = []
    for part in str(text or "").replace(";", ",").replace(" ", ",").split(","):
        s = part.strip()
        if not s:
            continue
        try:
            val = cast(s)
        except (TypeError, ValueError):
            continue
        if lo <= val <= hi and val not in out:
            out.append(val)
    return out[:VC_MC_MAX_VALUES]


def vc_mc_resolutions(text):
    out = []
    for part in str(text or "").replace(";", ",").replace(" ", ",").split(","):
        r = part.strip().lower()
        if r and _vc_res(r) == r and r not in out:
            out.append(r)
    return out[:VC_MC_MAX_VALUES]


def _vc_mc_compute(prepared, base_cfg, combos, exclude_top_n, deadline):
    """prepared: {Zeitebene: (candles, i_start)}. combos: (Zeitebene, EMA, Mindestabstand, Bestaetigung, ADX-Schwelle)."""
    base_p = _vc_params(base_cfg)
    # Nach (Zeitebene, EMA) sortieren: so liegt immer nur EINE Rechnung im Speicher statt einer je Kombination.
    combos = sorted(combos, key=lambda x: (x[0], x[1]))
    cur_key, arr = None, None
    adx_cache, wa_cache = {}, {}
    out = []
    for res, ema, minp, conf, adxm in combos:
        if time.time() > deadline:
            break
        candles, i_start = prepared[res]
        ts, o, h, l, c, v = candles
        p = dict(base_p, res=res, ema=ema, cross_min_pct=minp, confirm=conf, adx_min=adxm)
        if cur_key != (res, ema):
            arr = None                                  # alte Rechnung zuerst freigeben
            arr = vc_build(ts, o, h, l, c, v, None, p)
            cur_key = (res, ema)
        sig = vc_signals(arr, p)
        adx = None
        if adxm > 0:
            if (res, adxm) not in adx_cache:
                adx_cache[(res, adxm)] = vc_adx_ok(ts, o, h, l, c, p)
            lo, so = adx_cache[(res, adxm)]
            adx = (lo, so) if lo is not None else None
        if p["tp_mode"] == "wellenanker" and res not in wa_cache:
            wa_cache[res] = vc_wa_cross(o, h, l, c, p)
        trades = vc_simulate(ts, o, h, l, c, arr, sig, base_cfg, p, i_start, adx, wa_cache.get(res))
        st = summarize_backtest_trades(trades, exclude_top_n)
        out.append({"vc_resolution": res, "vc_ema_len": ema, "vc_cross_min_pct": minp, "vc_cross_confirm": conf,
                    "vc_adx_min": adxm, "trades": st["trades"], "win_rate_pct": st["win_rate_pct"],
                    "total_pnl_usd": st["total_pnl_usd"], "total_pnl_excl_top_n_usd": st["total_pnl_excl_top_n_usd"],
                    "max_drawdown_usd": st["max_drawdown_usd"], "avg_bars_held": st["avg_bars_held"]})
    return out


async def run_vc_montecarlo(symbol, cfg, days, resolutions="", ema_lens="", min_pcts="", confirms="", adx_mins="",
                            runs=500, exclude_top_n=1, seed=None):
    """Testet Kombinationen aus Zeitebene x EMA-Laenge x Mindestabstand x Bestaetigung x ADX-Schwelle auf denselben Kerzen.
    Jede Achse ist einzeln zuschaltbar: leer heisst "Wert aus den Einstellungen", also nur ein Wert auf dieser Achse.
    Alles andere (VWAP-Quelle und -Periode, Stop-Loss, TP, TP1, Breakeven, Richtung, Margin, Hebel) kommt aus den Einstellungen."""
    cfg = dict(cfg)
    base_p = _vc_params(cfg)
    res_list = vc_mc_resolutions(resolutions) or [base_p["res"]]
    ema_list = vc_mc_values(ema_lens, int, 2, 200) or [base_p["ema"]]
    minp_list = vc_mc_values(min_pcts, float, 0.0, 10.0) or [base_p["cross_min_pct"]]
    conf_list = vc_mc_values(confirms, int, 1, 20) or [base_p["confirm"]]
    adx_list = vc_mc_values(adx_mins, float, 0.0, 100.0) or [base_p["adx_min"]]
    runs = max(1, min(VC_MC_MAX_RUNS, int(runs)))
    mt = cfg.get("binance_market_type", "spot")
    warm_days = WARM_DAYS[base_p["anchor"]] + 1.0
    max_candles = BACKTEST_MAX_CANDLES.get("vwap_cross", 100_000)
    combos = [(r, e, m, cf, a) for r in res_list for e in ema_list for m in minp_list for cf in conf_list for a in adx_list]
    requested = len(combos)
    rng = random.Random(seed)
    if len(combos) > runs:
        combos = rng.sample(combos, runs)
    # Zeitebene fuer Zeitebene, damit nie mehrere Kerzensaetze gleichzeitig im Speicher liegen.
    skipped, days_cov, n_max, results, used_res = [], 0.0, 0, [], []
    deadline = time.time() + 240
    for res in res_list:
        grp = [x for x in combos if x[0] == res]
        if not grp:
            continue
        cap = min(max_candles, 5000) if res in ("10s", "15s", "30s", "45s") else max_candles
        candles, err = await _fetch_cached_mo7_backtest_candles(symbol, res, days + warm_days, cap, market_type=mt)
        if err or not candles or len(candles[4]) < 100 or not any(candles[5]):
            skipped.append(res)
            continue
        ts = candles[0]
        start_ts = ts[-1] - int(days * 86_400_000)
        i_start = next((i for i, t in enumerate(ts) if t >= start_ts), 1)
        days_cov = max(days_cov, (ts[-1] - ts[i_start]) / 86_400_000)
        n_max = max(n_max, len(ts))
        used_res.append(res)
        results += await asyncio.to_thread(_vc_mc_compute, {res: (candles, i_start)}, cfg, grp, exclude_top_n, deadline)
        candles = None
        if time.time() > deadline:
            break
    if not results:
        return {"error": "Für keine der gewählten Zeitebenen gab es genug Kerzen mit Volumen"
                         + (f" (übersprungen: {', '.join(skipped)})." if skipped else ".")}
    if not results:
        return {"error": "Keine Kombination konnte in der verfügbaren Zeit gerechnet werden - weniger Durchläufe oder kürzeren Zeitraum wählen."}
    pnls = sorted(r["total_pnl_usd"] for r in results)
    median = pnls[len(pnls) // 2] if len(pnls) % 2 else (pnls[len(pnls) // 2 - 1] + pnls[len(pnls) // 2]) / 2
    pos = sum(1 for r in results if r["total_pnl_usd"] > 0)
    min_trades = 5
    best = sorted(results, key=lambda r: (r["trades"] >= min_trades, r["total_pnl_usd"]), reverse=True)
    worst = sorted(results, key=lambda r: r["total_pnl_usd"])
    return {
        "symbol": symbol, "entry_mode": "vwap_cross", "days": days, "actual_days_covered": round(days_cov, 1),
        "candles_processed": n_max, "resolutions": used_res, "skipped_resolutions": skipped,
        "ema_lens": ema_list, "min_pcts": minp_list, "confirms": conf_list, "adx_mins": adx_list,
        "min_reliable_trades": min_trades, "seed": seed,
        "combos_tested": len(results), "combos_requested": requested, "combos_drawn": len(combos),
        "profitable_pct": round(pos / len(results) * 100, 1) if results else 0,
        "median_pnl_usd": round(median, 2), "best_pnl_usd": pnls[-1] if pnls else 0, "worst_pnl_usd": pnls[0] if pnls else 0,
        "results": best[:30], "worst_results": worst[:20],
    }
