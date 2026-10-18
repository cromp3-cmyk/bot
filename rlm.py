"""
rlm.py - eigene Strategie "Reaction Level Matrix" (entry_mode "rlm", Config-Praefix rlm_*).
Nachbau des TradingView-Indikators "Reaction Level Matrix [WillyAlgoTrader]".

Prinzip:
  * Aus bestaetigten Swing-Hochs/-Tiefs (Pivot-Laenge rlm_swing_len) werden online PREIS-LEVEL (Baender) gebaut. Jeder weitere Swing
    in der Naehe (Toleranz x ATR(34)) zaehlt als Beruehrung und staerkt das Level; Staerke zerfaellt mit der Halbwertszeit, ein
    Schlusskurs durch das Level schwaecht es. Score 0-100 aus der Staerke (Level mit Hoch- UND Tief-Beruehrungen bekommen einen Bonus).
  * Es zaehlen nur die nächsten rlm_levels_per_side Level je Seite (Unterstuetzung unter / Widerstand ueber dem Kurs) mit genug Beruehrungen.
  * LONG  = Ablehnung an einer Unterstuetzung: Kerze taucht ins Band, schliesst wieder darueber und im oberen Teil ihrer Spanne.
    SHORT = Ablehnung an einem Widerstand (Spiegelbild). Signal erst bei Kerzenschluss.
  * Ausstieg: SL (fester $-Betrag ODER 'Wick' = hinter dem Docht der Signalkerze), TP1/TP2/TP3 mit Teilverkaeufen, Break-Even nach TP1,
    Gegen-Signal dreht die Position (wie im Pine-Skript).
"""

import asyncio
import math
import time
import traceback

from bot_core import BOTS, debug_log, execute_entry, execute_exit, execute_reverse, execute_partial_exit
from strategies import (
    SUB_MINUTE_RESOLUTIONS, get_seconds_candles, fetch_candles_binance_multi, _tf_ms,
    summarize_backtest_trades, _fetch_cached_mo7_backtest_candles, BACKTEST_MAX_CANDLES, _bt_close_trade,
)

RLM_TFS = ["10s", "15s", "30s", "45s", "1m", "3m", "5m", "15m", "30m", "1h", "4h"]

# feste Konstanten des Pine-Originals
STRUCT_ATR_LEN = 34
MIN_HALF_WIDTH_ATR = 0.15
REACTION_CAP = 2.5
SCORE_SCALE = 6.0
POLARITY_BONUS = 1.25
CROSS_PENALTY = 0.7
MIN_STRENGTH = 0.25
MAX_LEVELS = 60
MERGE_PASSES = 5
WICK_BUFFER_ATR = 0.25
WICK_MIN_ATR = 0.5


def _res_seconds(res):
    if res in SUB_MINUTE_RESOLUTIONS:
        return SUB_MINUTE_RESOLUTIONS[res]
    return max(1, int(_tf_ms(res) / 1000))


def _rlm_params(cfg):
    res = cfg.get("rlm_resolution", "5m")
    mode = cfg.get("rlm_sl_mode", "usd")
    return {
        "res": res if res in RLM_TFS else "5m",
        "swing": max(2, min(30, int(cfg.get("rlm_swing_len", 6) or 6))),
        "tol": max(0.2, min(2.0, float(cfg.get("rlm_level_tol", 0.6) or 0.6))),
        "touches": max(2, min(10, int(cfg.get("rlm_min_touches", 2) or 2))),
        "half_life": max(50, min(5000, int(cfg.get("rlm_half_life", 500) or 500))),
        "per_side": max(1, min(8, int(cfg.get("rlm_levels_per_side", 3) or 3))),
        "min_score": max(0.0, min(100.0, float(cfg.get("rlm_min_score", 50) or 0))),
        "clv": max(0.5, min(0.9, float(cfg.get("rlm_reject_clv", 0.6) or 0.6))),
        "cooldown": max(0, int(cfg.get("rlm_cooldown", 10) or 0)),
        "atr_len": max(5, min(50, int(cfg.get("rlm_atr_len", 14) or 14))),
        "direction": cfg.get("rlm_direction_mode", "both") if cfg.get("rlm_direction_mode", "both") in ("both", "long_only", "short_only") else "both",
        "sl_mode": mode if mode in ("usd", "wick") else "usd",
        "sl_usd": max(0.0, float(cfg.get("rlm_sl_usd", 5.0) or 0.0)),
        "tp_usd": max(0.0, float(cfg.get("rlm_tp_usd", 5.0) or 0.0)),
        "tp_r": [max(0.1, float(cfg.get("rlm_tp1_r", 1.0) or 1.0)), max(0.1, float(cfg.get("rlm_tp2_r", 2.0) or 2.0)), max(0.1, float(cfg.get("rlm_tp3_r", 3.0) or 3.0))],
        "tp1_pct": max(1.0, min(100.0, float(cfg.get("rlm_tp1_close_pct", 33) or 33))),
        "tp2_pct": max(1.0, min(100.0, float(cfg.get("rlm_tp2_close_pct", 50) or 50))),
        "be": bool(cfg.get("rlm_be_after_tp1", True)),
        "reverse": bool(cfg.get("rlm_reverse", True)),
    }


# ============================================================================
# LEVEL-ENGINE (reine Rechnung, 1:1 nach dem Pine-Skript)
# ============================================================================
def _rma_atr(h, l, c, n):
    """ta.atr: RMA der True Range (Start = Mittel der ersten n Werte). Davor 0.0 (nz)."""
    m = len(c)
    out = [0.0] * m
    if m < n:
        return out
    tr = [h[0] - l[0]] + [max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1])) for i in range(1, m)]
    val = sum(tr[:n]) / n
    out[n - 1] = val
    for i in range(n, m):
        val = (val * (n - 1) + tr[i]) / n
        out[i] = val
    return out


class _Lv:
    __slots__ = ("lo", "hi", "mid", "sum_w", "sum_wp", "sum_wp2", "strength", "touches", "high_t", "low_t",
                 "first_i", "last_signal", "side", "side_i", "crosses")


def _score(lv):
    s = (lv.strength or 0.0) * (POLARITY_BONUS if (lv.high_t > 0 and lv.low_t > 0) else 1.0)
    return 100.0 * (1.0 - math.exp(-s / SCORE_SCALE))


def _refit(lv, a, tol):
    m = lv.sum_wp / lv.sum_w if lv.sum_w else lv.mid
    vr = (lv.sum_wp2 / lv.sum_w if lv.sum_w else 0.0) - m * m
    hw = min(max(math.sqrt(max(vr, 0.0)), a * MIN_HALF_WIDTH_ATR), a * tol)
    lv.mid, lv.lo, lv.hi = m, m - hw, m + hw


def _add_touch(lv, price, w, is_high, a, tol):
    lv.sum_w += w
    lv.sum_wp += w * price
    lv.sum_wp2 += w * price * price
    lv.strength += w
    lv.touches += 1
    if is_high:
        lv.high_t += 1
    else:
        lv.low_t += 1
    _refit(lv, a, tol)


def _new_level(price, w, is_high, p_idx, close, i, a, tol):
    lv = _Lv()
    lv.mid = lv.lo = lv.hi = price
    lv.sum_w = lv.sum_wp = lv.sum_wp2 = lv.strength = 0.0
    lv.touches = lv.high_t = lv.low_t = lv.crosses = 0
    lv.first_i = p_idx
    lv.last_signal = None
    lv.side = 1 if close > price else -1
    lv.side_i = i
    _add_touch(lv, price, w, is_high, a, tol)
    return lv


def _merge(t, o, a, tol):
    t.sum_w += o.sum_w
    t.sum_wp += o.sum_wp
    t.sum_wp2 += o.sum_wp2
    t.strength += o.strength
    t.touches += o.touches
    t.high_t += o.high_t
    t.low_t += o.low_t
    t.first_i = min(t.first_i, o.first_i)
    t.crosses = max(t.crosses, o.crosses)
    if o.last_signal is not None:
        t.last_signal = o.last_signal if t.last_signal is None else max(t.last_signal, o.last_signal)
    _refit(t, a, tol)


def _register(levels, price, w, is_high, p_idx, close, i, a, tol_mult):
    tol = a * tol_mult
    t_idx, best = -1, tol
    for k, lv in enumerate(levels):
        d = abs(lv.mid - price)
        if d <= best:
            best, t_idx = d, k
    if t_idx >= 0:
        t = levels[t_idx]
        _add_touch(t, price, w, is_high, a, tol_mult)
        again, passes = True, 0
        while again and passes < MERGE_PASSES:
            again = False
            passes += 1
            k = len(levels) - 1
            while k >= 0:
                if k != t_idx:
                    o = levels[k]
                    if o.lo <= t.hi and t.lo <= o.hi:
                        _merge(t, o, a, tol_mult)
                        levels.pop(k)
                        if k < t_idx:
                            t_idx -= 1
                        again = True
                k -= 1
    else:
        levels.append(_new_level(price, w, is_high, p_idx, close, i, a, tol_mult))
    while len(levels) > MAX_LEVELS:
        weak = min(range(len(levels)), key=lambda j: levels[j].strength)
        levels.pop(weak)


def _age_and_cross(levels, decay, close, i, min_touches):
    k = len(levels) - 1
    while k >= 0:
        lv = levels[k]
        lv.strength *= decay
        new_side = 1 if close > lv.hi else (-1 if close < lv.lo else lv.side)
        if new_side != lv.side:
            if lv.touches >= min_touches:
                lv.crosses += 1
                lv.strength *= CROSS_PENALTY
            lv.side = new_side
            lv.side_i = i
        if lv.strength < MIN_STRENGTH:
            levels.pop(k)
        k -= 1


def _select(levels, side_wanted, n, close, min_touches):
    cands = [lv for lv in levels if lv.touches >= min_touches and lv.side == side_wanted]
    cands.sort(key=lambda lv: abs(close - lv.mid))
    return cands[:n]


def rlm_engine(ts, o, h, l, c, p):
    """Laeuft ueber alle Kerzen (jede gilt als geschlossen) und liefert je Kerze das Signal:
    {"sig": [0|1|-1], "score": [..], "info": [..], "atr_risk": [..], "levels": [letzter Stand der angezeigten Level]}.
    Gleiche Reihenfolge wie im Pine-Skript: altern/kreuzen -> neue Swings einsortieren -> naechste Level waehlen -> Signal pruefen."""
    n = len(c)
    L = p["swing"]
    atr34 = _rma_atr(h, l, c, STRUCT_ATR_LEN)
    atr_risk = _rma_atr(h, l, c, p["atr_len"])
    decay = math.pow(0.5, 1.0 / p["half_life"])
    warm = max(STRUCT_ATR_LEN, 2 * L + 1)
    levels = []
    sig, score, info = [0] * n, [None] * n, [""] * n
    min_t, tol = p["touches"], p["tol"]
    for i in range(n):
        a = atr34[i]
        ok = a > 0
        close = c[i]
        if ok:
            _age_and_cross(levels, decay, close, i, min_t)
            if i >= 2 * L:
                pi = i - L
                a_sw = atr34[pi] if atr34[pi] > 0 else a
                hp = h[pi]
                if all(hp > h[j] for j in range(pi - L, pi)) and all(hp >= h[j] for j in range(pi + 1, i + 1)):
                    w = 1.0 + min(((hp - min(l[i - L + 1:i + 1])) / a_sw) if a_sw else 0.0, REACTION_CAP)
                    _register(levels, hp, w, True, pi, close, i, a, tol)
                lp = l[pi]
                if all(lp < l[j] for j in range(pi - L, pi)) and all(lp <= l[j] for j in range(pi + 1, i + 1)):
                    w = 1.0 + min(((max(h[i - L + 1:i + 1]) - lp) / a_sw) if a_sw else 0.0, REACTION_CAP)
                    _register(levels, lp, w, False, pi, close, i, a, tol)
        if ok and i >= warm:
            rng = h[i] - l[i]
            long_hit = short_hit = False
            l_score = s_score = None
            l_info = s_info = ""
            for lv in _select(levels, 1, p["per_side"], close, min_t):          # Unterstuetzung
                clv = (close - l[i]) / rng if rng > 0 else 0.5
                if (lv.side == 1 and lv.side_i < i and l[i] <= lv.hi and close > lv.hi and clv >= p["clv"]
                        and _score(lv) >= p["min_score"] and (lv.last_signal is None or i - lv.last_signal >= p["cooldown"])):
                    lv.last_signal = i
                    long_hit, l_score, l_info = True, _score(lv), f"{round(_score(lv))} · {lv.touches}T @ {round(lv.mid, 6)}"
                    break
            for lv in _select(levels, -1, p["per_side"], close, min_t):         # Widerstand
                clv = (h[i] - close) / rng if rng > 0 else 0.5
                if (lv.side == -1 and lv.side_i < i and h[i] >= lv.lo and close < lv.lo and clv >= p["clv"]
                        and _score(lv) >= p["min_score"] and (lv.last_signal is None or i - lv.last_signal >= p["cooldown"])):
                    lv.last_signal = i
                    short_hit, s_score, s_info = True, _score(lv), f"{round(_score(lv))} · {lv.touches}T @ {round(lv.mid, 6)}"
                    break
            if long_hit:
                sig[i], score[i], info[i] = 1, l_score, l_info
            elif short_hit:
                sig[i], score[i], info[i] = -1, s_score, s_info
    shown = []
    if n and atr34[-1] > 0:
        last_c = c[-1]
        for side in (-1, 1):
            for lv in _select(levels, side, p["per_side"], last_c, min_t):
                shown.append({"mid": lv.mid, "lo": lv.lo, "hi": lv.hi, "score": round(_score(lv)), "touches": lv.touches,
                              "side": "support" if side == 1 else "resistance", "polar": lv.high_t > 0 and lv.low_t > 0})
    return {"sig": sig, "score": score, "info": info, "atr_risk": atr_risk, "levels": shown}


# ============================================================================
# SL / TP
# ============================================================================
def _levels_for_entry(direction, entry, size, h_sig, l_sig, c_sig, atr_r, p):
    """-> (sl|None, [tp1, tp2, tp3] | []). usd: SL/TP-Schritt aus $ und Positionsgroesse; wick: SL hinter dem Docht der Signalkerze
    (mind. 0.5 x ATR, Puffer 0.25 x ATR), TPs als Vielfache des Risikos."""
    sgn = 1 if direction == "long" else -1
    if p["sl_mode"] == "wick":
        if not atr_r or atr_r <= 0:
            return None, []
        if sgn > 0:
            sl = min(l_sig - atr_r * WICK_BUFFER_ATR, c_sig - atr_r * WICK_MIN_ATR)
        else:
            sl = max(h_sig + atr_r * WICK_BUFFER_ATR, c_sig + atr_r * WICK_MIN_ATR)
        risk = (entry - sl) * sgn
        if risk <= 0:
            return None, []
        return sl, [entry + sgn * risk * m for m in p["tp_r"]]
    if not size or size <= 0:
        return None, []
    sl = entry - sgn * p["sl_usd"] / size if p["sl_usd"] > 0 else None
    if p["tp_usd"] <= 0:
        return sl, []
    step = p["tp_usd"] / size
    return sl, [entry + sgn * step * k for k in (1, 2, 3)]


# ============================================================================
# LIVE
# ============================================================================
def _rlm_reset_state(st):
    st["rlm_sl"] = None
    st["rlm_tps"] = []
    st["rlm_tp1_done"] = False
    st["rlm_tp2_done"] = False
    st["rlm_be"] = False


async def _closed(symbol, cfg, st, tf, bars):
    """Geschlossene Kerzen (ts, o, h, l, c) - Sekunden aus dem 1s-Puffer, sonst Binance (ohne die laufende Kerze)."""
    if tf in SUB_MINUTE_RESOLUTIONS:
        return get_seconds_candles(st, SUB_MINUTE_RESOLUTIONS[tf], bars)
    data = await fetch_candles_binance_multi(symbol, tf, count_back=min(1000, bars), market_type=cfg.get("binance_market_type", "spot"))
    if not data:
        return None
    ts, o, h, l, c = data[:5]
    return ts[:-1], o[:-1], h[:-1], l[:-1], c[:-1]


async def check_rlm_sl_tp(symbol, price):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    pos = st["position"]
    if pos is None or price is None:
        return
    p = _rlm_params(cfg)
    sl = st.get("rlm_sl")
    if sl is not None and ((pos == "long" and price <= sl) or (pos == "short" and price >= sl)):
        reason = "BREAKEVEN" if st.get("rlm_be") else "SL"
        debug_log(f"🚪 [{symbol}] RLM {reason}: {pos.upper()} @ {price} (Ziel war {round(sl, 6)})")
        await execute_exit(symbol, price, reason)
        if st["position"] is None:
            _rlm_reset_state(st)
        return
    tps = st.get("rlm_tps") or []
    if len(tps) < 3:
        return

    def hit(px):
        return (pos == "long" and price >= px) or (pos == "short" and price <= px)

    if not st.get("rlm_tp1_done"):
        if not hit(tps[0]):
            return
        if p["tp1_pct"] >= 99.5:
            debug_log(f"🎯 [{symbol}] RLM TP1 (ganze Position) @ {price}")
            await execute_exit(symbol, price, "TP1")
            if st["position"] is None:
                _rlm_reset_state(st)
            return
        if not await execute_partial_exit(symbol, price, p["tp1_pct"] / 100.0, "TP1"):
            return
        st["rlm_tp1_done"] = True
        if p["be"] and st.get("avg_entry_price"):
            st["rlm_sl"] = st["avg_entry_price"]
            st["rlm_be"] = True
            debug_log(f"📡 [{symbol}] RLM TP1 erreicht - SL auf Break-Even")
    if st.get("rlm_tp1_done") and not st.get("rlm_tp2_done"):
        if not hit(tps[1]):
            return
        if p["tp2_pct"] >= 99.5:
            await execute_exit(symbol, price, "TP2")
            if st["position"] is None:
                _rlm_reset_state(st)
            return
        if not await execute_partial_exit(symbol, price, p["tp2_pct"] / 100.0, "TP2"):
            return
        st["rlm_tp2_done"] = True
        debug_log(f"📡 [{symbol}] RLM TP2 erreicht")
    if st.get("rlm_tp2_done") and hit(tps[2]):
        debug_log(f"🎯 [{symbol}] RLM TP3 (Rest) @ {price}")
        await execute_exit(symbol, price, "TP3")
        if st["position"] is None:
            _rlm_reset_state(st)


def _set_levels_after_entry(st, target, price, bar, atr_r, p):
    entry = st.get("avg_entry_price") or price
    size = st.get("total_coin_size") or 0
    sl, tps = _levels_for_entry(target, entry, size, bar[0], bar[1], bar[2], atr_r, p)
    _rlm_reset_state(st)
    st["rlm_sl"], st["rlm_tps"] = sl, tps


async def check_rlm_signal(symbol, direction, price, bar, atr_r, info):
    """direction 1 / -1 auf der neu geschlossenen Kerze (bar = (high, low, close) der Signalkerze)."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or price is None or not direction:
        return
    p = _rlm_params(cfg)
    target = "long" if direction > 0 else "short"
    allowed = not ((target == "long" and p["direction"] == "short_only") or (target == "short" and p["direction"] == "long_only"))
    pos = st["position"]
    if pos == target:
        return   # gleiche Richtung: kein Nachkauf (wie im Pine-Skript)
    if pos is not None:   # Gegen-Signal
        if not p["reverse"]:
            return
        if allowed:
            debug_log(f"🔄 [{symbol}] RLM Wechsel {pos.upper()} -> {target.upper()} @ {price} ({info})")
            if await execute_reverse(symbol, price, "RLM-FLIP", size_multiplier=1.0):
                _set_levels_after_entry(st, target, price, bar, atr_r, p)
        else:
            debug_log(f"🔄 [{symbol}] RLM Gegen-Signal {target.upper()} (Richtung gesperrt) - Position schließen @ {price}")
            await execute_exit(symbol, price, "RLM-FLIP")
            if st["position"] is None:
                _rlm_reset_state(st)
        return
    if not allowed:
        return
    debug_log(f"📡 [{symbol}] RLM Signal: {target.upper()} @ {price} ({info})")
    await execute_entry(symbol, target, price, is_add_on=False)
    if st["position"] is not None:
        _set_levels_after_entry(st, target, price, bar, atr_r, p)


def _poll_delay(res, st):
    per = _res_seconds(res)
    if st.get("position") is not None or per <= 60:
        return 1.0
    into = time.time() % per
    if into < 6:
        return 1.0
    return max(1.0, min(5.0, per - into - 0.5))


async def rlm_poll_loop(symbol):
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0
    cache = {"key": None, "res": None}
    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "rlm" and cfg["bot_active"] and b["state"].get("session_started"):
                st = b["state"]
                p = _rlm_params(cfg)
                now = time.time()
                price = st.get("last_price")
                if st["position"] is not None and price is not None:
                    await check_rlm_sl_tp(symbol, price)
                cs = await _closed(symbol, cfg, st, p["res"], 1000)
                if cs and len(cs[4]) >= 2 * p["swing"] + STRUCT_ATR_LEN + 5:
                    ts, o, h, l, c = cs
                    age = (now * 1000 - ts[-1]) / 1000
                    plausible = price is None or not c[-1] or abs(c[-1] - price) / price * 100 <= 2.0
                    if age > 300 + _res_seconds(p["res"]):
                        if now - last_heartbeat > 300:
                            last_heartbeat = now
                            debug_log(f"⚠️ [{symbol}] RLM: letzte Kerze wirkt veraltet ({round(age)}s alt, {p['res']}) - überspringe.")
                    elif not plausible:
                        debug_log(f"⚠️ [{symbol}] RLM: Kerzendaten unplausibel (Kerze {c[-1]} vs. Live {price}) - überspringe.")
                    elif last_processed_ts is None:
                        last_processed_ts = ts[-1]        # erste Kerze nach dem Start nur merken, nicht handeln
                    elif ts[-1] > last_processed_ts:
                        last_processed_ts = ts[-1]        # bei Luecken nur die NEUESTE Kerze werten
                        res = await asyncio.to_thread(rlm_engine, ts, o, h, l, c, p)
                        st["rlm_levels"] = res["levels"]
                        if res["sig"][-1]:
                            await check_rlm_signal(symbol, res["sig"][-1], price if price is not None else c[-1],
                                                   (h[-1], l[-1], c[-1]), res["atr_risk"][-1], res["info"][-1])
                    if now - last_heartbeat > 300:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] RLM aktiv: Kerzen {p['res']}, Swing {p['swing']}, Score ≥ {p['min_score']}, SL={p['sl_mode']}, Preis={price}")
                elif now - last_heartbeat > 300:
                    last_heartbeat = now
                    debug_log(f"⏳ [{symbol}] RLM wartet: zu wenig Kerzen ({p['res']})")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] RLM-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        await asyncio.sleep(_poll_delay(b["config"].get("rlm_resolution", "5m"), b["state"]))


# ============================================================================
# BACKTEST
# ============================================================================
def _rlm_simulate(candles, cfg, eng, p):
    """Backtest-Gegenstueck: Kerze i - zuerst SL/TP einer schon offenen Position ueber Hoch/Tief (SL gewinnt bei Gleichzeitigkeit,
    ein auf dieser Kerze nachgezogener Break-Even-SL greift erst ab der naechsten), dann das Signal zum Schlusskurs."""
    ts, h, l, c = candles[0], candles[2], candles[3], candles[4]
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    sig, atr_r = eng["sig"], eng["atr_risk"]
    position, trades = None, []

    def open_pos(target, i):
        size = (margin * leverage) / c[i]
        sl, tps = _levels_for_entry(target, c[i], size, h[i], l[i], c[i], atr_r[i], p)
        if p["sl_mode"] == "wick" and sl is None:
            return None
        return {"dir": target, "entry": c[i], "size": size, "entry_i": i, "sl": sl, "tps": tps, "tp1": False, "tp2": False}

    for i in range(1, n):
        if position is not None and position["entry_i"] < i:
            d, e = position["dir"], position["entry"]
            sgn = 1 if d == "long" else -1
            sl = position["sl"]
            if sl is not None and ((sgn > 0 and l[i] <= sl) or (sgn < 0 and h[i] >= sl)):
                _bt_close_trade(trades, d, e, sl, position["size"], i, position["entry_i"], "BREAKEVEN" if position["tp1"] and p["be"] else "SL", ts=ts)
                position = None
            elif len(position["tps"]) == 3:
                def hit(px):
                    return (sgn > 0 and h[i] >= px) or (sgn < 0 and l[i] <= px)
                tps = position["tps"]
                if not position["tp1"] and hit(tps[0]):
                    if p["tp1_pct"] >= 99.5:
                        _bt_close_trade(trades, d, e, tps[0], position["size"], i, position["entry_i"], "TP1", ts=ts)
                        position = None
                    else:
                        cs = position["size"] * p["tp1_pct"] / 100.0
                        _bt_close_trade(trades, d, e, tps[0], cs, i, position["entry_i"], "TP1", ts=ts)
                        position["size"] -= cs
                        position["tp1"] = True
                        if p["be"]:
                            position["sl"] = e
                if position is not None and position["tp1"] and not position["tp2"] and hit(tps[1]):
                    if p["tp2_pct"] >= 99.5:
                        _bt_close_trade(trades, d, e, tps[1], position["size"], i, position["entry_i"], "TP2", ts=ts)
                        position = None
                    else:
                        cs = position["size"] * p["tp2_pct"] / 100.0
                        _bt_close_trade(trades, d, e, tps[1], cs, i, position["entry_i"], "TP2", ts=ts)
                        position["size"] -= cs
                        position["tp2"] = True
                if position is not None and position["tp2"] and hit(tps[2]):
                    _bt_close_trade(trades, d, e, tps[2], position["size"], i, position["entry_i"], "TP3", ts=ts)
                    position = None
        s = sig[i]
        if not s:
            continue
        target = "long" if s > 0 else "short"
        allowed = not ((target == "long" and p["direction"] == "short_only") or (target == "short" and p["direction"] == "long_only"))
        if position is not None:
            if position["dir"] == target or not p["reverse"]:
                continue
            _bt_close_trade(trades, position["dir"], position["entry"], c[i], position["size"], i, position["entry_i"], "RLM-FLIP", ts=ts)
            position = None
        if allowed:
            position = open_pos(target, i)
    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)
    return trades


async def run_rlm_backtest(symbol, cfg, days, exclude_top_n=1):
    max_candles = BACKTEST_MAX_CANDLES.get("rlm", 30_000)
    p = _rlm_params(cfg)
    if p["res"] in SUB_MINUTE_RESOLUTIONS:
        max_candles = min(max_candles, 5000)
    candles, err = await _fetch_cached_mo7_backtest_candles(symbol, p["res"], days, max_candles, market_type=cfg.get("binance_market_type", "spot"))
    if err:
        return {"error": err}
    warm = max(STRUCT_ATR_LEN, 2 * p["swing"] + 1)
    if not candles or len(candles[4]) < warm + 100:
        return {"error": "Zu wenig historische Kerzen für einen aussagekräftigen Backtest erhalten."}
    ts, o, h, l, c = candles[0], candles[1], candles[2], candles[3], candles[4]
    eng = await asyncio.to_thread(rlm_engine, ts, o, h, l, c, p)
    trades = _rlm_simulate(candles, cfg, eng, p)
    stats = summarize_backtest_trades(trades, exclude_top_n)
    actual_days = (ts[-1] - ts[0]) / 86_400_000
    return {
        "symbol": symbol, "entry_mode": "rlm", "resolution": p["res"],
        "requested_days": days, "actual_days_covered": round(actual_days, 1),
        "candles_processed": len(ts), "candle_cap": max_candles, "cache_used": False,
        "signals_buy": sum(1 for x in eng["sig"] if x > 0), "signals_sell": sum(1 for x in eng["sig"] if x < 0),
        "stats": stats,
        "stats_long": summarize_backtest_trades([t for t in trades if t["dir"] == "long"], exclude_top_n),
        "stats_short": summarize_backtest_trades([t for t in trades if t["dir"] == "short"], exclude_top_n),
        "trades": trades,
    }


# ============================================================================
# CHART
# ============================================================================
def rlm_chart_payload(ts, o, h, l, c, cfg, max_bars=1000):
    p = _rlm_params(cfg)
    n = len(c)
    eng = rlm_engine(ts, o, h, l, c, p)
    off = max(0, n - max_bars)
    candles = [{"time": ts[i] // 1000, "open": o[i], "high": h[i], "low": l[i], "close": c[i]} for i in range(off, n)]
    markers = [{"time": ts[i] // 1000, "kind": "long" if eng["sig"][i] > 0 else "short", "price": c[i]} for i in range(off, n) if eng["sig"][i]]
    return {"candles": candles, "wt1": [], "wt2": [], "markers": markers, "levels": [], "entry_mode": "rlm", "wt1v": 0, "wt2v": 0, "bars": n,
            "rlm_levels": eng["levels"]}
