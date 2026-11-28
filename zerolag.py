"""
zerolag.py - eigene Strategie "Zero Lag Trend" (entry_mode "zero_lag", Config-Praefix zl_*).

Nachbau von "Zero Lag Trend Signals (MTF)" von AlgoAlpha:
  * Zero-Lag-EMA: zlema = EMA(close + (close - close[lag]), length) mit lag = (length - 1) // 2.
    Der Zuschlag (close - close[lag]) nimmt die Verzoegerung der EMA vorweg, daher "Zero Lag".
  * Band: volatility = hoechster ATR(length) der letzten length*3 Kerzen, mal Faktor.
  * Trendzustand (haelt an, bis er kippt): Schluss kreuzt ueber zlema + Band -> Trend auf, Schluss kreuzt
    unter zlema - Band -> Trend ab.
  * Einstiegssignal (die kleinen Pfeile im Original): Schluss kreuzt die zlema-Linie, waehrend der Trend
    schon in dieselbe Richtung zeigt und auch auf der Kerze davor zeigte.
  * Zusaetzlich waehlbar: Einstieg direkt beim Kippen des Trends (die grossen Pfeile des Originals).
  * Breakeven in $ (eigener Zusatz): ab X $ Buchgewinn wandert der Stop-Loss auf den Einstieg.
  * $-Trailing-Stop (eigener Zusatz): setzt beim Einstiegswert ein und folgt dem besten Kurs
    in festem $-Abstand. Kein ATR. Beide rechnen mit der Groesse beim Einstieg und ziehen nur nach.
  * Trailing-Startwert (eigener Zusatz): der $-Trail wird erst aktiv, wenn der Buchgewinn den eingestellten Startwert erreicht hat.
  * EMA-Filter (eigener Zusatz): Long nur ueber, Short nur unter einer EMA (z. B. 50) - die EMA laeuft auf einer frei
    waehlbaren Zeitebene (leer = Zeitebene der Strategie), ohne Blick in die Zukunft.
  * ADX-Filter (eigener Zusatz, nicht im Original): nur handeln, wenn der ADX ueber der Schwelle liegt -
    wahlweise zusaetzlich mit passender +DI/-DI-Ausrichtung. Laeuft auf den eigenen Kerzen, keine zweite Zeiteinheit.
  * Wellenanker-Bestaetigung (eigener Zusatz): ein Signal zaehlt nur, wenn die Welle (WaveTrend) in den letzten
    N Kerzen gekreuzt hat - Long: WT1 kreuzt WT2 nach oben UNTER -Zone, Short: nach unten UEBER +Zone.
  * Umkehr (waehlbar): aus einem Kaufsignal wird ein Short und umgekehrt - fuer Maerkte, in denen das
    Original gegen den Kurs laeuft.

Ausstieg (waehlbar): Trendwechsel, Gegensignal, oder nur SL/TP; das Gegensignal kann die Position auch drehen.
Stop-Loss: am gegenueberliegenden Band (wahlweise nachziehend), fester $-Betrag oder keiner.
Gewinnmitnahme in zwei Stufen: TP1 und TP2 als $-Gewinn der Position mit einstellbarem Anteil.
Bei TP1 wandert der Stop-Loss auf den Einstieg.
"""

import asyncio
import time
import traceback
from collections import deque

from bot_core import BOTS, debug_log, execute_entry, execute_exit, execute_partial_exit, execute_reverse
from strategies import (
    BINANCE_SYMBOL_MAP, _tf_ms, summarize_backtest_trades, compute_atr, SUB_MINUTE_RESOLUTIONS,
    _fetch_cached_mo7_backtest_candles, BACKTEST_MAX_CANDLES, _bt_close_trade,
)

ZL_TFS = ["5s", "10s", "15s", "30s", "45s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"]
LIVE_CANDLES = 600


def _zl_res(res, default="5m"):
    import re
    res = str(res or default)
    if res in ZL_TFS:
        return res
    m = re.match(r"^(\d+)([smhd])$", res)
    if m and 1 <= int(m.group(1)) <= 1440:
        return res
    return default


def _zl_params(cfg):
    direction = cfg.get("zl_direction_mode", "both")
    entry = cfg.get("zl_entry_mode", "signal")
    exit_mode = cfg.get("zl_exit_mode", "trend_flip")
    sl_mode = cfg.get("zl_sl_mode", "band")
    return {
        "res": _zl_res(cfg.get("zl_resolution", "5m")),
        "length": max(2, min(500, int(cfg.get("zl_length", 70) or 70))),      # Original: 70
        "mult": max(0.1, float(cfg.get("zl_mult", 1.2) or 1.2)),              # Original: 1.2
        "direction": direction if direction in ("both", "long_only", "short_only") else "both",
        "entry_mode": entry if entry in ("signal", "trend") else "signal",
        "exit_mode": exit_mode if exit_mode in ("trend_flip", "signal", "sltp_only") else "trend_flip",
        "flip": bool(cfg.get("zl_flip_on_exit", False)),                      # Gegensignal dreht die Position
        "sl_mode": sl_mode if sl_mode in ("band", "usd", "aus") else "band",
        "sl_trail": bool(cfg.get("zl_sl_trail", False)),                      # Band-Stop zieht nach, nie zurueck
        "sl_usd": max(0.0, float(cfg.get("zl_sl_usd", 5.0) or 0.0)),
        "tp1_usd": max(0.0, float(cfg.get("zl_tp1_usd", 0.0) or 0.0)),       # 0 = TP1 aus
        "tp1_pct": min(100.0, max(0.0, float(cfg.get("zl_tp1_pct", 50.0) or 0.0))),
        "tp2_usd": max(0.0, float(cfg.get("zl_tp2_usd", 0.0) or 0.0)),       # 0 = TP2 aus
        "tp2_pct": min(100.0, max(0.0, float(cfg.get("zl_tp2_pct", 100.0) or 0.0))),
        # Breakeven: ab diesem Buchgewinn in $ wandert der Stop-Loss auf den Einstieg (0 = aus).
        # Unabhaengig von TP1 - greift also auch ohne TP1 und meist deutlich frueher.
        "be_usd": max(0.0, float(cfg.get("zl_be_usd", 0.0) or 0.0)),
        # $-Trailing-Stop (0 = aus): der Stop folgt dem besten Kurs in diesem festen $-Abstand,
        # sobald der Buchgewinn den Abstand erreicht hat - er liegt dann genau auf dem Einstieg
        # und wandert ab da mit. Er zieht nur nach, nie zurueck, und macht den eingestellten
        # Stop-Loss nie schlechter. Kein ATR, nur $.
        "trail_usd": max(0.0, float(cfg.get("zl_trail_usd", 0.0) or 0.0)),
        # Trail-Aktivierung: erst ab diesem Buchgewinn in $ beginnt der Trail (0 = wie bisher: sobald der Gewinn den Abstand erreicht hat)
        "trail_start_usd": max(0.0, float(cfg.get("zl_trail_start_usd", 0.0) or 0.0)),
        # ADX-Filter: nur handeln, wenn der Trend stark genug ist (0 = aus)
        "adx_min": max(0.0, float(cfg.get("zl_adx_min", 0.0) or 0.0)),
        "adx_len": max(2, int(cfg.get("zl_adx_len", 14) or 14)),
        "adx_dir": bool(cfg.get("zl_adx_directional", False)),                # zusaetzlich +DI/-DI in Signalrichtung
        "invert": bool(cfg.get("zl_invert", False)),                          # Signale umdrehen: Long-Signal -> Short
        # Wellenanker-Bestaetigung: Signal nur, wenn die Welle vorher (max. wf_bars Kerzen zurueck) ueber/unter der Zone gekreuzt hat
        "wf_on": bool(cfg.get("zl_wf_on", False)),
        "wf_zone": min(90.0, max(0.0, float(cfg.get("zl_wf_zone", 30.0) or 0.0))),
        "wf_bars": min(200, max(1, int(cfg.get("zl_wf_bars", 10) or 10))),
        # EMA-Filter: Long nur wenn der Schluss ueber der EMA liegt, Short nur darunter. ema_tf "" = Zeitebene der Strategie.
        "ema_on": bool(cfg.get("zl_ema_on", False)),
        "ema_len": min(1000, max(2, int(cfg.get("zl_ema_len", 50) or 50))),
        "ema_tf": _zl_res(cfg.get("zl_ema_tf", ""), "") if str(cfg.get("zl_ema_tf", "") or "").strip() else "",
    }


# ============================================================================
# RECHNUNG (rein, ohne Netzwerk)
# ============================================================================
def _ema(src, length):
    """EMA wie in Pine: erster Wert ist der Startwert, danach alpha = 2 / (length + 1)."""
    a = 2.0 / (length + 1)
    out = [None] * len(src)
    prev = None
    for i, v in enumerate(src):
        prev = v if prev is None else a * v + (1 - a) * prev
        out[i] = prev
    return out


def _highest(vals, window):
    """Laufendes Maximum der letzten `window` Werte (None zaehlt nicht mit)."""
    n = len(vals)
    out = [None] * n
    dq = deque()                    # Indizes, Werte absteigend
    for i in range(n):
        v = vals[i]
        if v is not None:
            while dq and vals[dq[-1]] <= v:
                dq.pop()
            dq.append(i)
        while dq and dq[0] <= i - window:
            dq.popleft()
        out[i] = vals[dq[0]] if dq else None
    return out


def zl_build(h, l, c, p):
    """Zero-Lag-Linie, Band, Trendzustand und Signale je Kerze. -> dict."""
    n = len(c)
    length = p["length"]
    lag = (length - 1) // 2
    src = [c[i] + (c[i] - c[i - lag]) if i >= lag else c[i] for i in range(n)]
    zlema = _ema(src, length)
    # Pine's ta.atr liefert vor der Einschwingphase na. compute_atr rechnet dort schon mit weniger Kerzen,
    # deshalb werden diese Werte verworfen - sonst weicht das hoechste ATR am Anfang vom Original ab.
    atr_raw = compute_atr(h, l, c, length)
    atr = [None if i < length - 1 else atr_raw[i] for i in range(len(atr_raw))]
    vol = [None if v is None else v * p["mult"] for v in _highest(atr, length * 3)]

    trend = [0] * n
    cur = 0
    for i in range(n):
        if i > 0 and zlema[i] is not None and vol[i] is not None and zlema[i - 1] is not None and vol[i - 1] is not None:
            up_now, up_prev = zlema[i] + vol[i], zlema[i - 1] + vol[i - 1]
            dn_now, dn_prev = zlema[i] - vol[i], zlema[i - 1] - vol[i - 1]
            if c[i] > up_now and c[i - 1] <= up_prev:
                cur = 1
            elif c[i] < dn_now and c[i - 1] >= dn_prev:
                cur = -1
        trend[i] = cur

    # Einstiegssignale: Schluss kreuzt die Linie, Trend zeigt schon seit der Vorkerze in dieselbe Richtung
    buy, sell = [False] * n, [False] * n
    flip_up, flip_dn = [False] * n, [False] * n
    for i in range(1, n):
        if zlema[i] is None or zlema[i - 1] is None:
            continue
        if trend[i] == 1 and trend[i - 1] != 1:
            flip_up[i] = True
        elif trend[i] == -1 and trend[i - 1] != -1:
            flip_dn[i] = True
        if c[i] > zlema[i] and c[i - 1] <= zlema[i - 1] and trend[i] == 1 and trend[i - 1] == 1:
            buy[i] = True
        elif c[i] < zlema[i] and c[i - 1] >= zlema[i - 1] and trend[i] == -1 and trend[i - 1] == -1:
            sell[i] = True
    if p["invert"]:
        # Signale umdrehen. Das geschieht EINMAL hier, damit Einstieg, Ausstieg, Stop-Loss-Seite
        # und die Pfeile im Chart alle dieselbe gedrehte Sicht benutzen.
        buy, sell = sell, buy
        flip_up, flip_dn = flip_dn, flip_up
    return {"zlema": zlema, "vol": vol, "trend": trend, "buy": buy, "sell": sell,
            "flip_up": flip_up, "flip_dn": flip_dn, "n": n}


def zl_entry_signal(arr, i, p):
    """-> "long" / "short" / None je nach Einstiegs-Modus."""
    if p["entry_mode"] == "trend":
        return "long" if arr["flip_up"][i] else "short" if arr["flip_dn"][i] else None
    return "long" if arr["buy"][i] else "short" if arr["sell"][i] else None


def zl_exit_signal(arr, i, pos, p):
    """True, wenn die offene Position nach dem gewaehlten Ausstieg geschlossen werden soll."""
    if p["exit_mode"] == "sltp_only":
        return False
    if p["exit_mode"] == "trend_flip":
        return arr["flip_dn"][i] if pos == "long" else arr["flip_up"][i]
    return arr["sell"][i] if pos == "long" else arr["buy"][i]


def zl_levels(direction, entry, size, arr, i, p):
    """(Stop-Loss, TP1, TP2) beim Einstieg. Band-Stop = die Bandlinie auf der Verlustseite,
    TP1/TP2 = Preis, bei dem die GANZE Position den eingestellten $-Gewinn haette."""
    sl = None
    if p["sl_mode"] == "band":
        z, v = arr["zlema"][i], arr["vol"][i]
        if z is not None and v is not None:
            sl = z - v if direction == "long" else z + v
            if (direction == "long" and sl >= entry) or (direction == "short" and sl <= entry):
                sl = None                     # Band liegt auf der falschen Seite -> kein Stop
    elif p["sl_mode"] == "usd" and p["sl_usd"] > 0 and size > 0:
        sl = entry - p["sl_usd"] / size if direction == "long" else entry + p["sl_usd"] / size
    sgn = 1 if direction == "long" else -1
    tp1 = entry + sgn * p["tp1_usd"] / size if (p["tp1_usd"] > 0 and size > 0) else None
    tp2 = entry + sgn * p["tp2_usd"] / size if (p["tp2_usd"] > 0 and size > 0) else None
    be = entry + sgn * p["be_usd"] / size if (p["be_usd"] > 0 and size > 0) else None
    return sl, tp1, tp2, be


def zl_trail(direction, sl, arr, i, p):
    """Band-Stop nachziehen - nie zurueck."""
    if p["sl_mode"] != "band" or not p["sl_trail"]:
        return sl
    z, v = arr["zlema"][i], arr["vol"][i]
    if z is None or v is None:
        return sl
    cand = z - v if direction == "long" else z + v
    if sl is None:
        return cand
    return max(sl, cand) if direction == "long" else min(sl, cand)


def _zl_clamp_be(direction, sl, entry, be_done):
    """Nach dem Breakeven darf der Stop nie wieder hinter den Einstieg zurueck - auch nicht,
    wenn der nachziehende Band-Stop weiter weg liegt."""
    if not be_done or not entry:
        return sl
    if sl is None:
        return entry
    return max(sl, entry) if direction == "long" else min(sl, entry)


def _zl_trail_usd(direction, sl, entry, best, size, p):
    """$-Trailing-Stop: folgt dem besten Kurs seit dem Einstieg in festem $-Abstand.

    Er greift erst, wenn der Buchgewinn den Abstand erreicht hat - dann liegt der Stop genau auf
    dem Einstieg und wandert ab da mit. Vorher bleibt der eingestellte Stop-Loss unberuehrt, ein
    enger Trail kann also nie einen weiteren Stop-Loss ersetzen. Zieht nur nach, nie zurueck."""
    if p["trail_usd"] <= 0 or not size or best is None or not entry:
        return sl
    dist = p["trail_usd"] / size
    cand = best - dist if direction == "long" else best + dist
    start = p.get("trail_start_usd", 0.0)
    if start > 0:
        # Mit Startwert: der Trail beginnt erst, wenn der beste Kurs den Gewinn `start` erreicht hat. Danach folgt der Stop
        # dem besten Kurs im festen Abstand - auch wenn er dabei noch unter dem Einstieg liegt (Abstand groesser als Startwert).
        gain = (best - entry) * size if direction == "long" else (entry - best) * size
        if gain < start:
            return sl
    elif (direction == "long" and cand < entry) or (direction == "short" and cand > entry):
        return sl                     # noch nicht genug Gewinn fuer den Trail
    if sl is None:
        return cand
    return max(sl, cand) if direction == "long" else min(sl, cand)


def _zl_stop_reason(direction, sl, entry, tp1_done, be_done):
    """Grund fuer einen Stop-Treffer: BREAKEVEN (Stop genau auf dem Einstieg), TRAIL (Stop schon
    im Gewinn) oder SL."""
    if sl is None or not entry:
        return "SL"
    if abs(sl - entry) < 1e-12:
        return "BREAKEVEN" if (tp1_done or be_done) else "SL"
    if (direction == "long" and sl > entry) or (direction == "short" and sl < entry):
        return "TRAIL"
    return "SL"


def _allowed(target, p):
    return not ((target == "long" and p["direction"] == "short_only") or (target == "short" and p["direction"] == "long_only"))


# ============================================================================
# LIVE
# ============================================================================
def _zl_reset_state(st):
    st["zl_sl"] = None
    st["zl_tp1"] = None
    st["zl_tp2"] = None
    st["zl_tp1_done"] = False
    st["zl_be"] = None
    st["zl_be_done"] = False
    st["zl_best"] = None
    st["zl_size0"] = None


async def check_zl_sl_tp(symbol, price):
    """Jeden Tick: Stop-Loss zuerst, dann TP1 (Teilverkauf, SL auf Einstieg) und TP2."""
    st = BOTS[symbol]["state"]
    pos = st["position"]
    if pos is None or price is None:
        return
    cfg = BOTS[symbol]["config"]
    p = _zl_params(cfg)
    entry = st.get("avg_entry_price")
    sl = st.get("zl_sl")
    if sl is not None and ((pos == "long" and price <= sl) or (pos == "short" and price >= sl)):
        reason = _zl_stop_reason(pos, sl, entry, st.get("zl_tp1_done"), st.get("zl_be_done"))
        debug_log(f"🚪 [{symbol}] Zero Lag {reason}: {pos.upper()} @ {price} (Ziel war {round(sl, 6)})")
        await execute_exit(symbol, price, reason)
        if st["position"] is None:
            _zl_reset_state(st)
        return
    be = st.get("zl_be")
    if be is not None and not st.get("zl_be_done") and ((pos == "long" and price >= be) or (pos == "short" and price <= be)):
        st["zl_be_done"] = True
        if entry:
            st["zl_sl"] = _zl_clamp_be(pos, st.get("zl_sl"), entry, True)
            debug_log(f"🛡️ [{symbol}] Zero Lag Breakeven: {p['be_usd']:g} $ im Plus - SL auf den Einstieg {round(entry, 6)}")
        # kein return: TP1/TP2 koennen im selben Tick noch greifen
    if p["trail_usd"] > 0 and entry:
        best = st.get("zl_best") or entry
        best = max(best, price) if pos == "long" else min(best, price)
        st["zl_best"] = best
        size0 = st.get("zl_size0") or st.get("total_coin_size") or 0
        new_sl = _zl_trail_usd(pos, st.get("zl_sl"), entry, best, size0, p)
        if new_sl is not None and (st.get("zl_sl") is None or abs(new_sl - st["zl_sl"]) > 1e-12):
            st["zl_sl"] = new_sl
            debug_log(f"🪜 [{symbol}] Zero Lag Trailing-Stop nachgezogen: {round(new_sl, 6)} "
                      f"({p['trail_usd']:g} $ hinter {round(best, 6)})")
    tp1 = st.get("zl_tp1")
    if tp1 is not None and not st.get("zl_tp1_done") and ((pos == "long" and price >= tp1) or (pos == "short" and price <= tp1)):
        st["zl_tp1_done"] = True
        pct = p["tp1_pct"]
        if pct >= 100:
            debug_log(f"🎯 [{symbol}] Zero Lag TP1: {pos.upper()} @ {price} - ganze Position")
            await execute_exit(symbol, price, "TP1")
            if st["position"] is None:
                _zl_reset_state(st)
            return
        if pct > 0:
            await execute_partial_exit(symbol, price, pct / 100.0, "TP1")
        if st["position"] is not None and entry:
            st["zl_sl"] = entry                  # bei TP1 wandert der Stop-Loss auf den Einstieg
            debug_log(f"🎯 [{symbol}] Zero Lag TP1: {pos.upper()} @ {price}, {pct:.0f}% verkauft - SL auf den Einstieg {round(entry, 6)}")
        return
    tp2 = st.get("zl_tp2")
    if tp2 is not None and ((pos == "long" and price >= tp2) or (pos == "short" and price <= tp2)):
        pct = p["tp2_pct"]
        if pct >= 100 or not st.get("zl_tp1_done"):
            debug_log(f"🎯 [{symbol}] Zero Lag TP2: {pos.upper()} @ {price} - Rest der Position")
            await execute_exit(symbol, price, "TP2")
            if st["position"] is None:
                _zl_reset_state(st)
            return
        if pct > 0:
            debug_log(f"🎯 [{symbol}] Zero Lag TP2: {pos.upper()} @ {price}, {pct:.0f}% des Restes")
            await execute_partial_exit(symbol, price, pct / 100.0, "TP2")


def _zl_market(symbol, cfg):
    from strategies import BINANCE_FUTURES_ONLY_SYMBOLS
    mt = cfg.get("binance_market_type", "spot")
    if symbol in BINANCE_FUTURES_ONLY_SYMBOLS:
        mt = "futures"
    return mt if mt in ("spot", "futures") else "spot"


async def _zl_fetch(symbol, cfg, p, res=None, bars=None):
    """Geschlossene Kerzen der gewuenschten Zeiteinheit als (ts, o, h, l, c, v) oder None."""
    from strategies import fetch_candles_binance_multi
    res = res or p["res"]
    need = bars or max(LIVE_CANDLES, p["length"] * 4 + 60)
    d = await fetch_candles_binance_multi(symbol, res, count_back=need, market_type=_zl_market(symbol, cfg))
    if not d or len(d[4]) < max(60, p["length"] + 10):
        return None
    return d


def zl_wt(o, h, l, c):
    """Wellenanker-Welle (WT1, WT2) mit den Standard-Werten des Bots."""
    from strategies import compute_wavetrend_series
    try:
        from screener import WA_N1, WA_N2, WA_SIG, WA_SCALE
    except Exception:
        WA_N1, WA_N2, WA_SIG, WA_SCALE = 10, 21, 4, 1.35
    return compute_wavetrend_series(o or c, h, l, c, WA_N1, WA_N2, WA_SIG, "close", WA_SCALE)


def zl_wf_from_wt(wt1, wt2, zone, bars):
    """(long_ok, short_ok) je Kerze: True, wenn in den letzten `bars` Kerzen (inkl. dieser) eine passende Wellen-Kreuzung war.
    Long: WT1 kreuzt WT2 nach oben, WT2 unter -zone. Short: WT1 kreuzt WT2 nach unten, WT2 ueber +zone."""
    n = len(wt1)
    last_up = last_dn = -10 ** 9
    lo, so = [False] * n, [False] * n
    for i in range(n):
        if i > 0 and None not in (wt1[i], wt2[i], wt1[i - 1], wt2[i - 1]):
            if wt1[i] > wt2[i] and wt1[i - 1] <= wt2[i - 1] and wt2[i] < -zone:
                last_up = i
            elif wt1[i] < wt2[i] and wt1[i - 1] >= wt2[i - 1] and wt2[i] > zone:
                last_dn = i
        lo[i] = i - last_up <= bars
        so[i] = i - last_dn <= bars
    return lo, so


def zl_adx_ok(ts, o, h, l, c, p):
    """(long_ok, short_ok) je Kerze oder (None, None), wenn ADX-Filter UND Wellenanker-Bestaetigung aus sind.
    Beide laufen auf den EIGENEN Kerzen der Strategie - eine zweite Zeiteinheit brauchen sie nicht.
    (Der Name bleibt, damit Live, Backtest, Chart und Monte-Carlo ohne Aenderung beide Filter nutzen.)"""
    res = None
    if p["adx_min"] > 0:
        from strategies import compute_adx_filter_series
        a = compute_adx_filter_series((ts, o or [0.0] * len(c), h, l, c), p["adx_len"], p["adx_min"],
                                      directional=p["adx_dir"], mode="require_trend")
        if a is not None and a[0] is not None:
            res = (list(a[0]), list(a[1]))
    if p.get("wf_on"):
        wt1, wt2 = zl_wt(o, h, l, c)
        w = zl_wf_from_wt(wt1, wt2, p["wf_zone"], p["wf_bars"])
        res = w if res is None else ([x and y for x, y in zip(res[0], w[0])], [x and y for x, y in zip(res[1], w[1])])
    return res if res is not None else (None, None)


def _zl_and_filters(a, b):
    """Zwei (long_ok, short_ok)-Paare verknuepfen. None = Filter aus."""
    if a is None or a[0] is None:
        return b
    if b is None or b[0] is None:
        return a
    return ([x and y for x, y in zip(a[0], b[0])], [x and y for x, y in zip(a[1], b[1])])


def zl_ema_align(base_ts, base_c, base_ms, hts, hc, h_ms, ema_len):
    """(long_ok, short_ok) je Basis-Kerze: Schluss ueber / unter der EMA der Filter-Zeitebene. Es zaehlt nur die letzte GESCHLOSSENE
    Filter-Kerze zum Schlusszeitpunkt der Basis-Kerze - so schaut der Backtest nicht in die Zukunft."""
    from bisect import bisect_right
    ema = _ema(hc, ema_len)
    close_ts = [t + h_ms for t in hts]
    n = len(base_c)
    lo, so = [False] * n, [False] * n
    for i in range(n):
        k = bisect_right(close_ts, base_ts[i] + base_ms) - 1
        if k < max(1, ema_len // 2) or ema[k] is None:
            continue
        lo[i] = base_c[i] > ema[k]
        so[i] = base_c[i] < ema[k]
    return lo, so


async def zl_ema_filter(symbol, cfg, p, base_ts, base_c, days=None):
    """EMA-Filter auf eigener Zeitebene. days=None -> live (letzte Kerzen), sonst Backtest ueber `days` Tage.
    -> (long_ok, short_ok) passend zu base_ts oder None, wenn der Filter aus ist / keine Daten kamen."""
    if not p.get("ema_on"):
        return None
    base_ms = (base_ts[1] - base_ts[0]) if len(base_ts) > 1 else _tf_ms(p["res"])
    tf = p["ema_tf"] or p["res"]
    if tf == p["res"]:
        return zl_ema_align(base_ts, base_c, base_ms, base_ts, base_c, base_ms, p["ema_len"])
    h_ms = _tf_ms(tf)
    need = p["ema_len"] * 4 + 60
    if days is None:
        from strategies import fetch_candles_binance_multi
        d = await fetch_candles_binance_multi(symbol, tf, count_back=min(1000, need), market_type=_zl_market(symbol, cfg))
        if not d or len(d[4]) < max(10, p["ema_len"] // 2):
            return None
        hts, hc = d[0], d[4]
    else:
        warm_days = need * h_ms / 86_400_000
        cap = 5000 if tf in SUB_MINUTE_RESOLUTIONS else 50_000
        data, err = await _fetch_cached_mo7_backtest_candles(symbol, tf, days + warm_days + 1, cap, market_type=cfg.get("binance_market_type", "spot"))
        if err or not data or len(data[4]) < max(10, p["ema_len"] // 2):
            return None
        hts, hc = data[0], data[4]
    return await asyncio.to_thread(zl_ema_align, base_ts, base_c, base_ms, hts, hc, h_ms, p["ema_len"])


def _zl_adx_text(p):
    parts = []
    if p["adx_min"] > 0:
        parts.append(f"ADX({p['adx_len']}) unter {p['adx_min']:g}" + (" oder falsche Richtung (+DI/-DI)" if p["adx_dir"] else ""))
    if p.get("wf_on"):
        parts.append(f"keine Wellenanker-Kreuzung (Zone ±{p['wf_zone']:g}, max. {p['wf_bars']} Kerzen zurück)")
    if p.get("ema_on"):
        parts.append(f"Kurs auf falscher Seite der EMA {p['ema_len']} ({p['ema_tf'] or p['res']})")
    return " / ".join(parts) or "Filter"


def _zl_adx_ok_at(target, adx, i):
    """True = Kerze i darf in diese Richtung handeln. adx = (long_ok, short_ok) oder None (Filter aus)."""
    if adx is None or adx[0] is None:
        return True
    series = adx[0] if target == "long" else adx[1]
    return bool(series[i]) if 0 <= i < len(series) else False


def _set_levels(st, cfg, target, price, arr, i, p):
    entry = st.get("avg_entry_price") or price
    size = st.get("total_coin_size") or 0
    st["zl_sl"], st["zl_tp1"], st["zl_tp2"], st["zl_be"] = zl_levels(target, entry, size, arr, i, p)
    st["zl_tp1_done"] = False
    st["zl_be_done"] = False
    st["zl_best"] = entry             # bester Kurs seit dem Einstieg (fuer den $-Trailing-Stop)
    st["zl_size0"] = size or None     # Groesse beim Einstieg: $-Abstaende rechnen immer damit,
                                      # auch nachdem TP1/TP2 einen Teil verkauft haben


async def zl_check_candle(symbol, cfg, p, arr, i, price, adx=None):
    """Auswertung der neu geschlossenen Kerze (Index i)."""
    st = BOTS[symbol]["state"]
    if not cfg["bot_active"] or price is None:
        return
    pos = st["position"]
    target = zl_entry_signal(arr, i, p)
    if pos is not None:
        st["zl_sl"] = _zl_clamp_be(pos, zl_trail(pos, st.get("zl_sl"), arr, i, p),
                                   st.get("avg_entry_price"), st.get("zl_be_done"))
        if not zl_exit_signal(arr, i, pos, p):
            return
        opposite = target is not None and target != pos
        flip_to = target if (p["flip"] and opposite and _allowed(target, p) and _zl_adx_ok_at(target, adx, i)) else None
        if flip_to:
            debug_log(f"🔄 [{symbol}] Zero Lag Flip: {pos.upper()} -> {flip_to.upper()} @ {price}")
            if await execute_reverse(symbol, price, "ZL-FLIP", size_multiplier=1.0):
                _zl_reset_state(st)
                if st["position"] is not None:
                    _set_levels(st, cfg, flip_to, price, arr, i, p)
        else:
            reason = "ZL-TRENDWECHSEL" if p["exit_mode"] == "trend_flip" else "ZL-GEGENSIGNAL"
            debug_log(f"🎯 [{symbol}] Zero Lag {reason}: {pos.upper()} schließen @ {price}")
            await execute_exit(symbol, price, reason)
            if st["position"] is None:
                _zl_reset_state(st)
        return
    if target is None or not _allowed(target, p):
        return
    if not _zl_adx_ok_at(target, adx, i):
        debug_log(f"⛔ [{symbol}] Zero Lag Signal {target.upper()} verworfen - {_zl_adx_text(p)}")
        return
    debug_log(f"📡 [{symbol}] Zero Lag Signal: {target.upper()} @ {price} (Trend {arr['trend'][i]})")
    await execute_entry(symbol, target, price, is_add_on=False)
    if st["position"] is not None:
        _set_levels(st, cfg, target, price, arr, i, p)


async def zl_poll_loop(symbol):
    b = BOTS[symbol]
    last_processed = None
    last_heartbeat = 0.0
    warned = False
    while True:
        wait = 2.0
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "zero_lag" and cfg["bot_active"] and b["state"].get("session_started"):
                st = b["state"]
                p = _zl_params(cfg)
                price = st.get("last_price")
                now = time.time()
                if st["position"] is not None and price is not None:
                    await check_zl_sl_tp(symbol, price)
                tf_ms = _tf_ms(p["res"])
                expect_ts = int(now * 1000) // tf_ms * tf_ms - tf_ms
                if last_processed is None or expect_ts > last_processed:
                    if not BINANCE_SYMBOL_MAP.get(symbol):
                        if not warned:
                            warned = True
                            debug_log(f"⚠️ [{symbol}] Zero Lag: Coin hat kein Binance-Paar - keine Daten für die Strategie")
                    else:
                        d = await _zl_fetch(symbol, cfg, p)
                        if d and d[0] and d[0][-1] == expect_ts:
                            plausible = price is None or abs(d[4][-1] - price) / price * 100 <= 2.0
                            if not plausible:
                                debug_log(f"⚠️ [{symbol}] Zero Lag: Kerzendaten unplausibel (Kerze {d[4][-1]} vs. Live {price}) - überspringe.")
                            else:
                                first_run = last_processed is None
                                last_processed = expect_ts
                                if not first_run:          # erste Kerze nach dem Start nur merken, nicht handeln
                                    arr = await asyncio.to_thread(zl_build, d[2], d[3], d[4], p)
                                    lo, so = await asyncio.to_thread(zl_adx_ok, d[0], d[1], d[2], d[3], d[4], p)
                                    flt = (lo, so) if lo is not None else None
                                    if p["ema_on"]:
                                        ef = await zl_ema_filter(symbol, cfg, p, d[0], d[4])
                                        if ef is None:
                                            debug_log(f"⚠️ [{symbol}] Zero Lag: EMA-Filter ohne Kerzen ({p['ema_tf'] or p['res']}) - Signal wird verworfen.")
                                            ef = ([False] * len(d[4]), [False] * len(d[4]))
                                        flt = _zl_and_filters(flt, ef)
                                    await zl_check_candle(symbol, cfg, p, arr, len(d[4]) - 1,
                                                          price if price is not None else d[4][-1], flt)
                        elif d:
                            wait = 3.0                     # neue Kerze noch nicht bei Binance verfuegbar
                        else:
                            wait = 5.0
                if now - last_heartbeat > 300:
                    last_heartbeat = now
                    debug_log(f"💓 [{symbol}] Zero Lag aktiv: {p['res']}, Länge {p['length']}, Faktor {p['mult']}, "
                              f"Einstieg={p['entry_mode']}, Ausstieg={p['exit_mode']}, Preis={price}")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Zero-Lag-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        await asyncio.sleep(wait if b["state"].get("position") is None else min(wait, 1.0))


# ============================================================================
# BACKTEST
# ============================================================================
def zl_simulate(ts, h, l, c, arr, cfg, p, i_start, adx=None):
    """Kerze i: zuerst SL / TP einer offenen Position ueber Hoch/Tief (SL gewinnt bei Gleichstand),
    dann bei Kerzenschluss der Ausstieg, dann das Einstiegssignal zum Schlusskurs."""
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    position, trades = None, []
    for i in range(max(1, i_start), n):
        if position is not None and position["entry_i"] < i:
            d, e = position["dir"], position["entry"]
            sgn = 1 if d == "long" else -1
            if not position["tp1_done"]:
                position["sl"] = _zl_clamp_be(d, zl_trail(d, position["sl"], arr, i - 1, p), e, position["be_done"])
            sl = position["sl"]
            if sl is not None and ((sgn > 0 and l[i] <= sl) or (sgn < 0 and h[i] >= sl)):
                reason = _zl_stop_reason(d, sl, e, position["tp1_done"], position["be_done"])
                _bt_close_trade(trades, d, e, sl, position["size"], i, position["entry_i"], reason, ts=ts)
                position = None
            else:
                t1, t2 = position["tp1"], position["tp2"]
                if t1 is not None and not position["tp1_done"] and ((sgn > 0 and h[i] >= t1) or (sgn < 0 and l[i] <= t1)):
                    position["tp1_done"] = True
                    if p["tp1_pct"] >= 100:
                        _bt_close_trade(trades, d, e, t1, position["size"], i, position["entry_i"], "TP1", ts=ts)
                        position = None
                    else:
                        if p["tp1_pct"] > 0:
                            part = position["size"] * p["tp1_pct"] / 100.0
                            _bt_close_trade(trades, d, e, t1, part, i, position["entry_i"], "TP1", ts=ts)
                            position["size"] -= part
                        position["sl"] = e          # bei TP1 wandert der Stop-Loss auf den Einstieg
                if position is not None and t2 is not None and ((sgn > 0 and h[i] >= t2) or (sgn < 0 and l[i] <= t2)):
                    if p["tp2_pct"] >= 100 or not position["tp1_done"]:
                        _bt_close_trade(trades, d, e, t2, position["size"], i, position["entry_i"], "TP2", ts=ts)
                        position = None
                    elif p["tp2_pct"] > 0:
                        part = position["size"] * p["tp2_pct"] / 100.0
                        _bt_close_trade(trades, d, e, t2, part, i, position["entry_i"], "TP2", ts=ts)
                        position["size"] -= part
                # Breakeven ERST JETZT pruefen: innerhalb einer Kerze ist die Reihenfolge der Kurse
                # unbekannt, deshalb gilt fuer SL/TP dieser Kerze noch der alte Stop. Sonst wuerde
                # der Backtest einen Stop nutzen, der zum Zeitpunkt des Tiefs noch nicht dort lag.
                if position is not None and position["be"] is not None and not position["be_done"] \
                        and ((sgn > 0 and h[i] >= position["be"]) or (sgn < 0 and l[i] <= position["be"])):
                    position["be_done"] = True
                    position["sl"] = _zl_clamp_be(d, position["sl"], e, True)
                # $-Trailing-Stop ebenfalls erst jetzt: der beste Kurs DIESER Kerze darf den Stop
                # erst fuer die FOLGENDEN Kerzen enger machen, sonst waere es ein Blick nach vorn.
                if position is not None and p["trail_usd"] > 0:
                    position["best"] = max(position["best"], h[i]) if sgn > 0 else min(position["best"], l[i])
                    position["sl"] = _zl_trail_usd(d, position["sl"], e, position["best"], position["size0"], p)
        target = zl_entry_signal(arr, i, p)
        if target is not None and not _zl_adx_ok_at(target, adx, i):
            target = None
        if position is not None and position["entry_i"] < i and zl_exit_signal(arr, i, position["dir"], p):
            reason = "ZL-TRENDWECHSEL" if p["exit_mode"] == "trend_flip" else "ZL-GEGENSIGNAL"
            opposite = target is not None and target != position["dir"]
            if p["flip"] and opposite and _allowed(target, p):
                reason = "ZL-FLIP"
            _bt_close_trade(trades, position["dir"], position["entry"], c[i], position["size"], i, position["entry_i"], reason, ts=ts)
            position = None
        if position is None and target is not None and _allowed(target, p):
            size = (margin * leverage) / c[i]
            sl, tp1, tp2, be = zl_levels(target, c[i], size, arr, i, p)
            position = {"dir": target, "entry": c[i], "size": size, "size0": size, "entry_i": i,
                        "sl": sl, "tp1": tp1, "tp2": tp2, "tp1_done": False, "be": be,
                        "be_done": False, "best": c[i]}
    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1,
                        position["entry_i"], "END-OF-BACKTEST", ts=ts)
    return trades


async def run_zl_backtest(symbol, cfg, days, exclude_top_n=1):
    p = _zl_params(cfg)
    warm_bars = p["length"] * 4 + 60
    warm_days = warm_bars * _tf_ms(p["res"]) / 86_400_000
    max_candles = BACKTEST_MAX_CANDLES.get("zero_lag", 100_000)
    if p["res"] in SUB_MINUTE_RESOLUTIONS:
        max_candles = min(max_candles, 5000)       # Sekunden-Kerzen werden aus 1s-Daten gebaut - sonst laeuft der Abruf ins Zeitlimit
    candles, err = await _fetch_cached_mo7_backtest_candles(symbol, p["res"], days + warm_days + 1, max_candles,
                                                            market_type=cfg.get("binance_market_type", "spot"))
    if err:
        return {"error": err}
    if not candles or len(candles[4]) < max(100, p["length"] * 2):
        return {"error": "Zu wenig historische Kerzen für einen aussagekräftigen Backtest erhalten."}
    ts, o, h, l, c, v = candles
    start_ts = ts[-1] - int(days * 86_400_000)
    i_start = next((i for i, t in enumerate(ts) if t >= start_ts), 1)
    arr = await asyncio.to_thread(zl_build, h, l, c, p)
    lo, so = await asyncio.to_thread(zl_adx_ok, ts, o, h, l, c, p)
    adx = (lo, so) if lo is not None else None
    if p["ema_on"]:
        ef = await zl_ema_filter(symbol, cfg, p, ts, c, days + warm_days)
        if ef is None:
            return {"error": f"Keine Kerzen für die EMA-Filter-Zeitebene {p['ema_tf'] or p['res']} erhalten."}
        adx = _zl_and_filters(adx, ef)
    trades = zl_simulate(ts, h, l, c, arr, cfg, p, i_start, adx)
    stats = summarize_backtest_trades(trades, exclude_top_n)
    # Nur Signale zaehlen, die auch durch den ADX-Filter kommen - sonst passt die Zahl nicht zu den Trades
    sig_buy = sum(1 for i in range(i_start, len(ts))
                  if zl_entry_signal(arr, i, p) == "long" and _zl_adx_ok_at("long", adx, i))
    sig_sell = sum(1 for i in range(i_start, len(ts))
                   if zl_entry_signal(arr, i, p) == "short" and _zl_adx_ok_at("short", adx, i))
    return {
        "symbol": symbol, "entry_mode": "zero_lag", "resolution": p["res"],
        "requested_days": days, "actual_days_covered": round((ts[-1] - ts[i_start]) / 86_400_000, 1),
        "candles_processed": len(ts), "candle_cap": max_candles, "cache_used": False,
        "signals_buy": sig_buy, "signals_sell": sig_sell,
        "stats": stats,
        "stats_long": summarize_backtest_trades([t for t in trades if t["dir"] == "long"], exclude_top_n),
        "stats_short": summarize_backtest_trades([t for t in trades if t["dir"] == "short"], exclude_top_n),
        "trades": trades,
    }


# ============================================================================
# CHART
# ============================================================================
async def zl_chart_payload(symbol, cfg, ts, o, h, l, c, tf):
    """Kerzen + Einstiegs-Markierungen, Zero-Lag-Linie und Bänder (nur wenn die Chart-Zeitebene passt)."""
    p = _zl_params(cfg)
    n = len(c)
    off = max(0, n - 1000)
    candles = [{"time": ts[i] // 1000, "open": o[i], "high": h[i], "low": l[i], "close": c[i]} for i in range(off, n)]
    markers, lines = [], []
    if tf == p["res"]:
        try:
            d = await _zl_fetch(symbol, cfg, p)
            if d:
                arr = await asyncio.to_thread(zl_build, d[2], d[3], d[4], p)
                # Pfeile nur dort, wo ALLE Einstellungen passen (Richtung, ADX-Filter) - sonst zeigt
                # der Chart Signale, die der Bot gar nicht nehmen wuerde.
                lo, so = await asyncio.to_thread(zl_adx_ok, d[0], d[1], d[2], d[3], d[4], p)
                adx = (lo, so) if lo is not None else None
                if p["ema_on"]:
                    adx = _zl_and_filters(adx, await zl_ema_filter(symbol, cfg, p, d[0], d[4]) or ([False] * len(d[4]), [False] * len(d[4])))
                first_t = candles[0]["time"] if candles else 0
                base, up, dn = [], [], []
                for i in range(len(d[0])):
                    t = d[0][i] // 1000
                    if t < first_t:
                        continue
                    kind = zl_entry_signal(arr, i, p)
                    if kind and _allowed(kind, p) and _zl_adx_ok_at(kind, adx, i):
                        markers.append({"time": t, "kind": kind, "price": d[4][i]})
                    if arr["zlema"][i] is not None:
                        base.append({"time": t, "value": arr["zlema"][i]})
                        if arr["vol"][i] is not None:
                            up.append({"time": t, "value": arr["zlema"][i] + arr["vol"][i]})
                            dn.append({"time": t, "value": arr["zlema"][i] - arr["vol"][i]})
                lines = [{"key": "zl_basis", "label": "Zero-Lag-Linie", "data": base},
                         {"key": "zl_up", "label": "Zero-Lag Band oben", "data": up},
                         {"key": "zl_dn", "label": "Zero-Lag Band unten", "data": dn}]
        except Exception as e:
            debug_log(f"⚠️ Zero-Lag Chart-Markierungen fehlgeschlagen: {e}")
    return {"candles": candles, "wt1": [], "wt2": [], "markers": markers, "levels": [], "entry_mode": "zero_lag",
            "wt1v": 0, "wt2v": 0, "bars": n, "zl_lines": lines}


# ============================================================================
# MONTE-CARLO
# ============================================================================
ZL_MC_MAX_RUNS = 2000
ZL_MC_MAX_VALUES = 12


def zl_mc_values(text, cast, lo, hi):
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
    return out[:ZL_MC_MAX_VALUES]


def zl_mc_range(rng, cast, hard_lo, hard_hi, maxn=40):
    """{"min": .., "max": .., "step": ..} -> Werteliste. Leer/ungueltig -> []. Hoechstens maxn Werte."""
    if not isinstance(rng, dict):
        return []
    try:
        lo, hi, step = float(rng.get("min")), float(rng.get("max")), float(rng.get("step"))
    except (TypeError, ValueError):
        return []
    if step <= 0 or hi < lo:
        return []
    out, i = [], 0
    while len(out) < maxn:
        v = lo + i * step
        if v > hi + 1e-9:
            break
        val = int(round(v)) if cast is int else round(v, 6)
        if hard_lo <= val <= hard_hi and val not in out:
            out.append(val)
        i += 1
    return out


def zl_mc_resolutions(text):
    out = []
    for part in str(text or "").replace(";", ",").replace(" ", ",").split(","):
        r = part.strip().lower()
        if r and _zl_res(r, "") == r and r not in out:
            out.append(r)
    return out[:ZL_MC_MAX_VALUES]


def _zl_mc_compute(candles, i_start, base_cfg, combos, exclude_top_n, deadline, ema_f=None):
    """candles: (ts,o,h,l,c,v) der eingestellten Zeitebene. combos: (Laenge, Faktor, WA-Zone, WA-Kerzen zurueck).
    Alles andere (SL/TP/ADX/Breakeven/Trailing/Ausstieg) kommt aus den Einstellungen."""
    base_p = _zl_params(base_cfg)
    ts, o, h, l, c, v = candles
    combos = sorted(combos, key=lambda x: (x[0], x[1]))   # je (Laenge, Faktor) nur EINE Rechnung im Speicher
    cur_key, arr = None, None
    adx_only = None
    if base_p["adx_min"] > 0:                              # ADX-Filter der Einstellungen einmal rechnen
        lo, so = zl_adx_ok(ts, o, h, l, c, dict(base_p, wf_on=False))
        adx_only = (lo, so) if lo is not None else None
    wt1, wt2 = zl_wt(o, h, l, c)                           # Welle einmal rechnen, Zone/Kerzen nur noch neu auswerten
    wf_cache = {}
    out = []
    for length, mult, zone, bars in combos:
        if time.time() > deadline:
            break
        p = dict(base_p, length=length, mult=mult, wf_on=True, wf_zone=zone, wf_bars=bars)
        if cur_key != (length, mult):
            arr = None
            arr = zl_build(h, l, c, p)
            cur_key = (length, mult)
        if (zone, bars) not in wf_cache:
            wf_cache[(zone, bars)] = zl_wf_from_wt(wt1, wt2, zone, bars)
        w = wf_cache[(zone, bars)]
        flt = w if adx_only is None else ([x and y for x, y in zip(adx_only[0], w[0])], [x and y for x, y in zip(adx_only[1], w[1])])
        if ema_f is not None:
            flt = _zl_and_filters(flt, ema_f)
        trades = zl_simulate(ts, h, l, c, arr, base_cfg, p, i_start, flt)
        st = summarize_backtest_trades(trades, exclude_top_n)
        out.append({"zl_length": length, "zl_mult": mult, "zl_wf_zone": zone, "zl_wf_bars": bars,
                    "trades": st["trades"], "win_rate_pct": st["win_rate_pct"],
                    "total_pnl_usd": st["total_pnl_usd"], "total_pnl_excl_top_n_usd": st["total_pnl_excl_top_n_usd"],
                    "max_drawdown_usd": st["max_drawdown_usd"], "avg_bars_held": st["avg_bars_held"]})
    return out


async def run_zl_montecarlo(symbol, cfg, days, lengths=None, mults=None, wf_zones=None, wf_barss=None, runs=500,
                            exclude_top_n=1, seed=None):
    """Raster ueber Laenge x Band-Faktor x Wellenanker-Zone (Kreuzung unter/ueber +-Zone) x Wellenanker-Kerzen (wie weit die Kreuzung
    zurueckliegen darf). Die Achsen kommen als {"min": .., "max": .., "step": ..} (oder kommagetrennte Liste); eine leere Achse nimmt den
    Wert aus den Einstellungen. Der Wellenanker-Filter ist im Test AN, sobald eine seiner beiden Achsen gesetzt ist, sonst gilt die
    Einstellung. Zeitebene, Einstieg, Ausstieg, SL/TP, ADX, Breakeven, Trailing, Margin und Hebel kommen aus den Einstellungen."""
    import random
    cfg = dict(cfg)
    base_p = _zl_params(cfg)

    def axis(val, cast, lo, hi, fallback):
        return (zl_mc_range(val, cast, lo, hi) if isinstance(val, dict) else zl_mc_values(val, cast, lo, hi)) or [fallback]

    wf_axes = bool(isinstance(wf_zones, dict) or isinstance(wf_barss, dict) or wf_zones or wf_barss)
    len_list = axis(lengths, int, 2, 500, base_p["length"])
    mult_list = axis(mults, float, 0.1, 10.0, base_p["mult"])
    zone_list = axis(wf_zones, float, 0.0, 90.0, base_p["wf_zone"])
    bars_list = axis(wf_barss, int, 1, 200, base_p["wf_bars"])
    if not wf_axes and not base_p["wf_on"]:
        zone_list, bars_list = [base_p["wf_zone"]], [base_p["wf_bars"]]   # Filter aus -> keine Wellenanker-Achsen sinnvoll
    runs = max(1, min(ZL_MC_MAX_RUNS, int(runs)))
    mt = cfg.get("binance_market_type", "spot")
    res = base_p["res"]
    max_candles = BACKTEST_MAX_CANDLES.get("zero_lag", 100_000)
    combos = [(ln, m, z, b) for ln in len_list for m in mult_list for z in zone_list for b in bars_list]
    requested = len(combos)
    rng = random.Random(seed)
    if len(combos) > runs:
        combos = rng.sample(combos, runs)
    warm = (max(len_list) * 4 + 60) * _tf_ms(res) / 86_400_000
    cap = min(max_candles, 5000) if res in SUB_MINUTE_RESOLUTIONS else max_candles
    candles, err = await _fetch_cached_mo7_backtest_candles(symbol, res, days + warm + 1, cap, market_type=mt)
    if err or not candles or len(candles[4]) < 100:
        return {"error": f"Für die Zeitebene {res} gab es nicht genug Kerzen" + (f" ({err})." if err else ".")}
    ts = candles[0]
    start_ts = ts[-1] - int(days * 86_400_000)
    i_start = next((i for i, t in enumerate(ts) if t >= start_ts), 1)
    days_cov = (ts[-1] - ts[i_start]) / 86_400_000
    n_max = len(ts)
    deadline = time.time() + 240
    ema_f = None
    if base_p["ema_on"]:
        ema_f = await zl_ema_filter(symbol, cfg, base_p, ts, candles[4], days + warm)
        if ema_f is None:
            return {"error": f"Keine Kerzen für die EMA-Filter-Zeitebene {base_p['ema_tf'] or res} erhalten."}
    results = await asyncio.to_thread(_zl_mc_compute, candles, i_start, cfg, combos, exclude_top_n, deadline, ema_f)
    candles = None
    if not results:
        return {"error": "Keine Kombination konnte in der verfügbaren Zeit gerechnet werden - weniger Durchläufe oder kürzeren Zeitraum wählen."}
    pnls = sorted(r["total_pnl_usd"] for r in results)
    median = pnls[len(pnls) // 2] if len(pnls) % 2 else (pnls[len(pnls) // 2 - 1] + pnls[len(pnls) // 2]) / 2
    pos = sum(1 for r in results if r["total_pnl_usd"] > 0)
    min_trades = 5
    best = sorted(results, key=lambda r: (r["trades"] >= min_trades, r["total_pnl_usd"]), reverse=True)
    worst = sorted(results, key=lambda r: r["total_pnl_usd"])
    return {
        "symbol": symbol, "entry_mode": "zero_lag", "days": days, "actual_days_covered": round(days_cov, 1),
        "candles_processed": n_max, "resolution": res,
        "lengths": len_list, "mults": mult_list, "wf_zones": zone_list, "wf_bars": bars_list,
        "min_reliable_trades": min_trades, "seed": seed,
        "combos_tested": len(results), "combos_requested": requested, "combos_drawn": len(combos),
        "profitable_pct": round(pos / len(results) * 100, 1) if results else 0,
        "median_pnl_usd": round(median, 2), "best_pnl_usd": pnls[-1] if pnls else 0, "worst_pnl_usd": pnls[0] if pnls else 0,
        "results": best[:30], "worst_results": worst[:20],
    }
