"""
wt_div.py - eigene Strategie "WT-Divergenz" (entry_mode "wt_div", Config-Praefix wd_*).

Einstieg bei WaveTrend-DIVERGENZEN. Die WaveTrend-Welle ist exakt die des Wellenankers (compute_wavetrend_series:
esa = EMA(src, n1), d = EMA(|src-esa|, n1), ci = (src-esa)/(0.015*d), wt1 = EMA(ci, n2) * Skalierung, wt2 = SMA(wt1, Signal)).

Pivots (wie ta.pivotlow / ta.pivothigh mit links/rechts Kerzen) werden auf der gewaehlten Linie (wt1 oder wt2) gesucht. Ein Pivot ist
erst nach "rechts" weiteren GESCHLOSSENEN Kerzen bestaetigt - das Signal kommt also bewusst erst auf der Bestaetigungs-Kerze
(kein Repaint, kein Blick in die Zukunft). Der Kurs-Wert des Pivots ist das Tief/Hoch der Pivot-Kerze.

  Bullish (Long)  regulaer : Welle hoeheres Tief  UND Kurs tieferes Tief   (Kurs faellt, Momentum nicht mehr)
                  hidden   : Welle tieferes Tief  UND Kurs hoeheres Tief   (Trendfortsetzung)
  Bearish (Short) regulaer : Welle tieferes Hoch  UND Kurs hoeheres Hoch
                  hidden   : Welle hoeheres Hoch  UND Kurs tieferes Hoch
Wie im Original-Pine (Wellenanker, Abschnitt 4 Divergenzen): Ein Pivot zaehlt NUR, wenn er extrem genug ist (Pivot-Tief der Welle <= "Bullisch ab" = -65,
Pivot-Hoch >= "Baerisch ab" = 45); nur solche Pivots werden gemerkt und mit dem jeweils vorigen gemerkten Pivot verglichen. Der vorige Pivot darf
hoechstens max_range Kerzen zurueckliegen (min_range 1 = kein Minimum wie im Pine). Defaults = Pine-Defaults: Welle 9/12/3, Skalierung 1,0, HLC3,
Pivot 2/2, Linie wt2, nur regulaere Divergenz.

Ausstieg (wd_exit_mode):
  flip        Gegen-Divergenz dreht die Position (Standard)
  wt_cross    Long endet, wenn wt1 unter wt2 kreuzt; Short, wenn wt1 ueber wt2 kreuzt (Gegen-Divergenz hat Vorrang)
  wt_cross_zone  wie wt_cross, aber nur wenn der Cross auf der ANDEREN Seite passiert (wie der Short-/Long-Punkt im Wellenanker):
              Long endet bei wt1 unter wt2 UND wt2 > +exit_zone; Short endet bei wt1 ueber wt2 UND wt2 < -exit_zone (Gegen-Divergenz hat Vorrang)
  sltp_only   keine Signal-Ausstiege, nur SL/TP (Gegen-Divergenzen werden ignoriert)
  dazu immer optional fester $-SL/-TP (0 = aus), Cooldown nach SL, Richtungswahl.

Take-Profit-Art (wd_tp_mode):
  single   ein fester $-TP (wd_tp_usd) schliesst die ganze Position (wie bisher)
  partial  Teil-TPs wie beim RLM / Ema-RSI: TP1, TP2, TP3 je als $-Gewinn der GANZEN Position (0 = Stufe aus). TP1 schliesst wd_tp1_pct %
           der Position, TP2 wd_tp2_pct % der VERBLEIBENDEN Position, die letzte aktive Stufe immer den Rest. Nach TP1 rutscht der SL auf
           Break-Even (optional), nach TP2 auf TP1 (optional). Signal-Ausstiege (Flip, Cross) schliessen jederzeit den Rest.
"""

import asyncio
import random
import time
import traceback

from bot_core import BOTS, debug_log, execute_entry, execute_exit, execute_reverse, execute_partial_exit
from strategies import (
    SUB_MINUTE_RESOLUTIONS, get_seconds_candles, fetch_candles_binance_multi, compute_wavetrend_series,
    summarize_backtest_trades, BACKTEST_MAX_CANDLES, _bt_close_trade,
)
from wae_ema import _we_candles, _res_seconds, _poll_delay

EXIT_MODES = ("flip", "wt_cross", "wt_cross_zone", "sltp_only")


def _wd_params(cfg):
    mode = cfg.get("wd_direction_mode", "both")
    em = cfg.get("wd_exit_mode", "flip")
    line = cfg.get("wd_line", "wt2")
    src = cfg.get("wd_src", "close")
    return {
        "src": src if src in ("close", "hlc3", "ohlc4", "hl2") else "close",
        "n1": max(2, int(cfg.get("wd_n1", 9) or 9)), "n2": max(2, int(cfg.get("wd_n2", 12) or 12)),
        "sig": max(1, int(cfg.get("wd_sig_len", 3) or 3)), "scale": float(cfg.get("wd_wave_scale", 1.0) or 1.0),
        "line": line if line in ("wt1", "wt2") else "wt2",
        "pl": max(1, int(cfg.get("wd_pivot_left", 2) or 2)), "pr": max(1, int(cfg.get("wd_pivot_right", 2) or 2)),
        "minr": max(1, int(cfg.get("wd_min_range", 1) or 1)), "maxr": max(2, int(cfg.get("wd_max_range", 120) or 120)),
        "ob": float(cfg.get("wd_div_ob", 45) if cfg.get("wd_div_ob") not in (None, "") else 45),
        "os": float(cfg.get("wd_div_os", -65) if cfg.get("wd_div_os") not in (None, "") else -65),
        "regular": bool(cfg.get("wd_use_regular", True)), "hidden": bool(cfg.get("wd_use_hidden", False)),
        "exit_mode": em if em in EXIT_MODES else "flip",
        "exit_zone": abs(float(cfg.get("wd_exit_zone", 53) if cfg.get("wd_exit_zone") not in (None, "") else 53)),
        "direction": mode if mode in ("both", "long_only", "short_only") else "both",
        "sl_usd": max(0.0, float(cfg.get("wd_sl_usd", 0.0) or 0.0)),
        "tp_usd": max(0.0, float(cfg.get("wd_tp_usd", 0.0) or 0.0)),
        "tp_mode": "partial" if cfg.get("wd_tp_mode", "single") == "partial" else "single",
        "tp1_usd": max(0.0, float(cfg.get("wd_tp1_usd", 5.0) or 0.0)), "tp2_usd": max(0.0, float(cfg.get("wd_tp2_usd", 10.0) or 0.0)),
        "tp3_usd": max(0.0, float(cfg.get("wd_tp3_usd", 15.0) or 0.0)),
        "tp1_pct": max(1.0, min(100.0, float(cfg.get("wd_tp1_pct", 33) or 33))), "tp2_pct": max(1.0, min(100.0, float(cfg.get("wd_tp2_pct", 33) or 33))),
        "be": bool(cfg.get("wd_be_after_tp1", True)), "sl_to_tp1": bool(cfg.get("wd_sl_to_tp1_on_tp2", True)),
    }


def _wd_warm(p):
    """Kerzen, bis die Welle eingeschwungen ist (+ Platz fuer die Pivots)."""
    return (p["n1"] + p["n2"]) * 5 + p["sig"] + p["pl"] + p["pr"] + p["maxr"]


# ============================================================================
# RECHNUNG (rein, ohne Netzwerk)
# ============================================================================
def wd_wave(o, h, l, c, p):
    return compute_wavetrend_series(o, h, l, c, p["n1"], p["n2"], p["sig"], p["src"], p["scale"])


def wd_divergences(h, l, osc, p, start=0, pairs=None):
    """-> (bull[], bear[]) - True auf der BESTAETIGUNGS-Kerze (Pivot-Kerze + 'rechts'). Pivots werden ab 'start' gesammelt."""
    n = len(osc)
    bull, bear = [False] * n, [False] * n
    pl, pr, minr, maxr, ob, os_ = p["pl"], p["pr"], p["minr"], p["maxr"], p["ob"], p["os"]
    last_lo = last_hi = None          # (index, osc, kurs)
    for i in range(max(pl, start), n - pr):
        v = osc[i]
        left = osc[i - pl:i]
        right = osc[i + 1:i + pr + 1]
        j = i + pr
        if v < min(left) and v <= min(right) and v <= os_:         # Pivot-Tief der Welle, nur wenn extrem genug (Pine: plWt <= divOS)
            if last_lo is not None and minr <= i - last_lo[0] <= maxr:
                reg = v > last_lo[1] and l[i] < last_lo[2]
                hid = v < last_lo[1] and l[i] > last_lo[2]
                if (p["regular"] and reg) or (p["hidden"] and hid):
                    bull[j] = True
                    if pairs is not None:
                        pairs.append({"kind": "long", "hidden": not reg, "i1": last_lo[0], "i2": i, "w1": last_lo[1], "w2": v, "p1": last_lo[2], "p2": l[i], "conf": j})
            last_lo = (i, v, l[i])
        if v > max(left) and v >= max(right) and v >= ob:          # Pivot-Hoch der Welle, nur wenn extrem genug (Pine: phWt >= divOB)
            if last_hi is not None and minr <= i - last_hi[0] <= maxr:
                reg = v < last_hi[1] and h[i] > last_hi[2]
                hid = v > last_hi[1] and h[i] < last_hi[2]
                if (p["regular"] and reg) or (p["hidden"] and hid):
                    bear[j] = True
                    if pairs is not None:
                        pairs.append({"kind": "short", "hidden": not reg, "i1": last_hi[0], "i2": i, "w1": last_hi[1], "w2": v, "p1": last_hi[2], "p2": h[i], "conf": j})
            last_hi = (i, v, h[i])
    return bull, bear


def wd_compute(o, h, l, c, p):
    """-> (bull[], bear[], xdn[], xup[], wt1, wt2): xdn = wt1 kreuzt unter wt2, xup = wt1 kreuzt ueber wt2."""
    wt1, wt2 = wd_wave(o, h, l, c, p)
    osc = wt1 if p["line"] == "wt1" else wt2
    bull, bear = wd_divergences(h, l, osc, p)
    n = len(c)
    xdn, xup = [False] * n, [False] * n
    zoned, xz = p["exit_mode"] == "wt_cross_zone", p["exit_zone"]
    for i in range(1, n):
        if wt1[i] < wt2[i] and wt1[i - 1] >= wt2[i - 1]:
            xdn[i] = (not zoned) or wt2[i] > xz
        elif wt1[i] > wt2[i] and wt1[i - 1] <= wt2[i - 1]:
            xup[i] = (not zoned) or wt2[i] < -xz
    return bull, bear, xdn, xup, wt1, wt2


def _wd_levels(direction, entry, size, p):
    if not size or size <= 0:
        return None, None
    sgn = 1 if direction == "long" else -1
    sl = entry - sgn * p["sl_usd"] / size if p["sl_usd"] > 0 else None
    tp = entry + sgn * p["tp_usd"] / size if (p["tp_usd"] > 0 and p["tp_mode"] != "partial") else None
    return sl, tp


def _wd_ladder(direction, entry, size, p):
    """Teil-TP-Leiter -> [(Preis, Prozent), ...] der aktiven Stufen (nur im Modus 'partial'). Eine Stufe zaehlt nur, wenn ihr $-Wert > 0 UND
    groesser als der der vorigen aktiven Stufe ist. Die LETZTE Stufe schliesst immer den Rest (Prozent 100)."""
    if p["tp_mode"] != "partial" or not size or size <= 0:
        return []
    sgn = 1 if direction == "long" else -1
    out, prev = [], 0.0
    for usd, pct in ((p["tp1_usd"], p["tp1_pct"]), (p["tp2_usd"], p["tp2_pct"]), (p["tp3_usd"], 100.0)):
        if usd > prev:
            out.append([entry + sgn * usd / size, pct])
            prev = usd
    if out:
        out[-1][1] = 100.0
    return [(px, pc) for px, pc in out]


def _allowed(target, p):
    return not ((target == "long" and p["direction"] == "short_only") or (target == "short" and p["direction"] == "long_only"))


def _signal_exit(pos, xdn_i, xup_i, p):
    if p["exit_mode"] in ("wt_cross", "wt_cross_zone") and ((pos == "long" and xdn_i) or (pos == "short" and xup_i)):
        return "WD-WT-X"
    return None


# ============================================================================
# LIVE
# ============================================================================
def _wd_reset_state(st):
    st["wd_sl_price"] = None
    st["wd_tp_price"] = None
    st["wd_tps"] = []
    st["wd_tp_pcts"] = []
    st["wd_tp_done"] = 0
    st["wd_be"] = False


async def check_wd_sl_tp(symbol, price):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    pos = st["position"]
    if pos is None or price is None:
        return
    p = _wd_params(cfg)
    sl, tp = st.get("wd_sl_price"), st.get("wd_tp_price")
    if sl is not None and ((pos == "long" and price <= sl) or (pos == "short" and price >= sl)):
        prot = st.get("wd_be")
        reason = "SL-TP1" if prot == "tp1" else "BREAKEVEN" if prot else "SL"
        debug_log(f"🚪 [{symbol}] WT-Divergenz {reason}: {pos.upper()} @ {price} (Ziel war {round(sl, 6)})")
        await execute_exit(symbol, price, reason)
        if st["position"] is None:
            if not prot:
                st["wd_sl_cooldown_until"] = time.time() + cfg.get("wd_sl_cooldown_seconds", 30)
            _wd_reset_state(st)
        return
    tps = st.get("wd_tps") or []
    if tps:
        pcts = st.get("wd_tp_pcts") or []
        done = int(st.get("wd_tp_done") or 0)
        while done < len(tps) and ((pos == "long" and price >= tps[done]) or (pos == "short" and price <= tps[done])):
            pct = pcts[done] if done < len(pcts) else 100.0
            if done == len(tps) - 1 or pct >= 99.5:
                debug_log(f"🎯 [{symbol}] WT-Divergenz TP{done + 1} (Rest): {pos.upper()} @ {price}")
                await execute_exit(symbol, price, f"TP{done + 1}")
                if st["position"] is None:
                    _wd_reset_state(st)
                return
            if not await execute_partial_exit(symbol, price, pct / 100.0, f"TP{done + 1}"):
                return
            done += 1
            st["wd_tp_done"] = done
            debug_log(f"📡 [{symbol}] WT-Divergenz TP{done} erreicht - {pct:g}% geschlossen")
            if done == 1 and p["be"] and st.get("avg_entry_price"):
                st["wd_sl_price"], st["wd_be"] = st["avg_entry_price"], "be"
                debug_log(f"📡 [{symbol}] WT-Divergenz TP1 erreicht - SL auf Break-Even")
            elif done == 2 and p["sl_to_tp1"]:
                st["wd_sl_price"], st["wd_be"] = tps[0], "tp1"
                debug_log(f"📡 [{symbol}] WT-Divergenz TP2 erreicht - SL auf TP1")
        return
    if tp is not None and ((pos == "long" and price >= tp) or (pos == "short" and price <= tp)):
        debug_log(f"🎯 [{symbol}] WT-Divergenz TP: {pos.upper()} @ {price} (Ziel war {round(tp, 6)})")
        await execute_exit(symbol, price, "TP")
        if st["position"] is None:
            _wd_reset_state(st)


def _set_levels(st, cfg, target, price):
    entry = st.get("avg_entry_price") or price
    size = st.get("total_coin_size") or 0
    p = _wd_params(cfg)
    st["wd_sl_price"], st["wd_tp_price"] = _wd_levels(target, entry, size, p)
    lad = _wd_ladder(target, entry, size, p)
    st["wd_tps"], st["wd_tp_pcts"], st["wd_tp_done"], st["wd_be"] = [x[0] for x in lad], [x[1] for x in lad], 0, False


async def check_wd_signal(symbol, bull_i, bear_i, xdn_i, xup_i, price):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or price is None:
        return
    p = _wd_params(cfg)
    pos = st["position"]
    tri = "long" if bull_i else "short" if bear_i else None
    if bull_i and bear_i:
        tri = None
    if pos is None:
        if tri is None or not _allowed(tri, p) or time.time() < st.get("wd_sl_cooldown_until", 0.0):
            return
        debug_log(f"📡 [{symbol}] WT-Divergenz Einstieg: {'bullish' if tri == 'long' else 'bearish'} ({tri.upper()}) @ {price}")
        await execute_entry(symbol, tri, price, is_add_on=False)
        if st["position"] is not None:
            _wd_reset_state(st)
            _set_levels(st, cfg, tri, price)
        return
    if p["exit_mode"] == "sltp_only":
        return
    if tri is not None and tri != pos:
        if _allowed(tri, p):
            debug_log(f"🔄 [{symbol}] WT-Divergenz Flip: {pos.upper()} -> {tri.upper()} @ {price}")
            if await execute_reverse(symbol, price, "WD-FLIP", size_multiplier=1.0):
                _wd_reset_state(st)
                _set_levels(st, cfg, tri, price)
        else:
            debug_log(f"🚪 [{symbol}] WT-Divergenz Gegensignal (Richtung gesperrt): {pos.upper()} schließen @ {price}")
            await execute_exit(symbol, price, "WD-EXIT-DIR")
            if st["position"] is None:
                _wd_reset_state(st)
        return
    reason = _signal_exit(pos, xdn_i, xup_i, p)
    if reason:
        debug_log(f"🎯 [{symbol}] WT-Divergenz {reason}: {pos.upper()} beendet @ {price}")
        await execute_exit(symbol, price, reason)
        if st["position"] is None:
            _wd_reset_state(st)


async def wd_poll_loop(symbol):
    """WT-Divergenz live: wertet NUR abgeschlossene Kerzen aus; SL/TP laufend gegen den Live-Preis."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0
    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "wt_div" and cfg["bot_active"] and b["state"].get("session_started"):
                resolution = cfg.get("wd_resolution", "15m")
                p = _wd_params(cfg)
                res_s = _res_seconds(resolution)
                min_needed = _wd_warm(p) + 2
                needed_bars = min(1000, max(min_needed * 2, 500))
                st = b["state"]
                closed_ts = None
                if resolution in SUB_MINUTE_RESOLUTIONS:
                    local = get_seconds_candles(st, SUB_MINUTE_RESOLUTIONS[resolution], needed_bars)
                    if local:
                        closed_ts, closed_o, closed_h, closed_l, closed_c = local
                else:
                    _need_fetch = last_processed_ts is None or (time.time() % res_s) < 6 or res_s <= 60
                    data = await fetch_candles_binance_multi(symbol, resolution, count_back=needed_bars, market_type=cfg.get("binance_market_type", "spot")) if _need_fetch else None
                    if data:
                        timestamps, opens, highs, lows, closes = data[:5]
                        closed_ts, closed_o, closed_h, closed_l, closed_c = timestamps[:-1], opens[:-1], highs[:-1], lows[:-1], closes[:-1]

                now = time.time()
                due_heartbeat = now - last_heartbeat > 300
                if st["position"] is not None and st["last_price"] is not None:
                    await check_wd_sl_tp(symbol, st["last_price"])

                if closed_ts and len(closed_c) > min_needed:
                    age = (now * 1000 - closed_ts[-1]) / 1000
                    if age > 300 + res_s:
                        debug_log(f"⚠️ [{symbol}] WT-Divergenz: letzte Kerze wirkt veraltet ({round(age)}s alt, Auflösung {resolution}) - überspringe Durchlauf.")
                        closed_ts = None
                    elif st["last_price"] is not None and closed_c[-1] and abs(closed_c[-1] - st["last_price"]) / st["last_price"] * 100 > 2.0:
                        debug_log(f"⚠️ [{symbol}] WT-Divergenz: Kerzendaten unplausibel (Kerze {closed_c[-1]} vs. Live {st['last_price']}) - überspringe Durchlauf.")
                        closed_ts = None

                if closed_ts and len(closed_c) > min_needed and (last_processed_ts is None or closed_ts[-1] > last_processed_ts):
                    bull, bear, xdn, xup, wt1, wt2 = await asyncio.to_thread(wd_compute, closed_o, closed_h, closed_l, closed_c, p)
                    if due_heartbeat:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] WT-Divergenz aktiv: {resolution}, Welle {p['n1']}/{p['n2']}/{p['sig']} ({p['line']}), Pivot {p['pl']}/{p['pr']}, "
                                  f"regulär={'an' if p['regular'] else 'aus'} hidden={'an' if p['hidden'] else 'aus'}, Ausstieg={p['exit_mode']}, "
                                  f"SL=${p['sl_usd']} TP=${p['tp_usd']}, Preis={closed_c[-1]}")
                    if last_processed_ts is None:
                        new_indices = [len(closed_ts) - 1]
                        first_run = True
                    else:
                        new_indices = [i for i in range(len(closed_ts)) if closed_ts[i] > last_processed_ts]
                        first_run = False
                    for idx in new_indices:
                        price_i = st["last_price"] if (idx == len(closed_ts) - 1 and st["last_price"] is not None) else closed_c[idx]
                        last_processed_ts = closed_ts[idx]
                        if first_run:
                            continue   # erste Kerze nach dem Start nur merken (kein Einstieg auf ein altes Signal)
                        await check_wd_signal(symbol, bull[idx], bear[idx], xdn[idx], xup[idx], price_i)
                elif due_heartbeat and not closed_ts:
                    last_heartbeat = now
                    debug_log(f"⏳ [{symbol}] WT-Divergenz wartet: keine Kerzen erhalten (Auflösung {resolution})")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] WT-Divergenz-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        await asyncio.sleep(_poll_delay(b["config"].get("wd_resolution", "15m"), b["state"]))


# ============================================================================
# BACKTEST
# ============================================================================
def _wd_simulate(candles, cfg, bull, bear, xdn, xup, start_i, p):
    """Backtest-Gegenstueck zu check_wd_signal / check_wd_sl_tp: Einstieg/Flip zum Schlusskurs der Bestaetigungs-Kerze, SL/TP ab der
    naechsten Kerze ueber Hoch/Tief (SL gewinnt bei Gleichzeitigkeit), Cooldown nach SL, Gegen-Divergenz vor WT-Kreuzung."""
    ts, h, l, c = candles[0], candles[2], candles[3], candles[4]
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    cooldown_ms = cfg.get("wd_sl_cooldown_seconds", 30) * 1000
    position, trades, cool_until = None, [], None

    def open_pos(direction, price, i):
        size = (margin * leverage) / price
        sl, tp = _wd_levels(direction, price, size, p)
        return {"dir": direction, "entry": price, "size": size, "entry_i": i, "sl": sl, "tp": tp,
                "tps": _wd_ladder(direction, price, size, p), "done": 0, "prot": False}

    for i in range(max(start_i, 1), n):
        if position is not None and position["entry_i"] < i:
            d, e, sl, tp = position["dir"], position["entry"], position["sl"], position["tp"]
            if sl is not None and ((d == "long" and l[i] <= sl) or (d == "short" and h[i] >= sl)):
                prot = position["prot"]
                _bt_close_trade(trades, d, e, sl, position["size"], i, position["entry_i"], "SL-TP1" if prot == "tp1" else "BREAKEVEN" if prot else "SL", ts=ts)
                position = None
                if not prot:
                    cool_until = ts[i] + cooldown_ms
            elif position["tps"]:
                lad = position["tps"]
                while position is not None and position["done"] < len(lad):
                    px, pct = lad[position["done"]]
                    if not ((d == "long" and h[i] >= px) or (d == "short" and l[i] <= px)):
                        break
                    k = position["done"]
                    if k == len(lad) - 1 or pct >= 99.5:
                        _bt_close_trade(trades, d, e, px, position["size"], i, position["entry_i"], f"TP{k + 1}", ts=ts)
                        position = None
                        break
                    cs = position["size"] * pct / 100.0
                    _bt_close_trade(trades, d, e, px, cs, i, position["entry_i"], f"TP{k + 1}", ts=ts)
                    position["size"] -= cs
                    position["done"] = k + 1
                    if k == 0 and p["be"]:
                        position["sl"], position["prot"] = e, "be"
                    elif k == 1 and p["sl_to_tp1"]:
                        position["sl"], position["prot"] = lad[0][0], "tp1"
            elif tp is not None and ((d == "long" and h[i] >= tp) or (d == "short" and l[i] <= tp)):
                _bt_close_trade(trades, d, e, tp, position["size"], i, position["entry_i"], "TP", ts=ts)
                position = None
        tri = None if (bull[i] and bear[i]) else "long" if bull[i] else "short" if bear[i] else None
        price = c[i]
        if position is None:
            if tri is None or not _allowed(tri, p) or (cool_until is not None and ts[i] < cool_until):
                continue
            position = open_pos(tri, price, i)
            continue
        if p["exit_mode"] == "sltp_only":
            continue
        d = position["dir"]
        if tri is not None and tri != d:
            ok = _allowed(tri, p)
            _bt_close_trade(trades, d, position["entry"], price, position["size"], i, position["entry_i"], "WD-FLIP" if ok else "WD-EXIT-DIR", ts=ts)
            position = open_pos(tri, price, i) if ok else None
        else:
            reason = _signal_exit(d, xdn[i], xup[i], p)
            if reason:
                _bt_close_trade(trades, d, position["entry"], price, position["size"], i, position["entry_i"], reason, ts=ts)
                position = None
    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)
    return trades


def _lead_days(p, res_s):
    return _wd_warm(p) * res_s / 86400 + 0.1


def _window_start(ts, days):
    from bisect import bisect_left
    return bisect_left(ts, ts[-1] - int(days * 86_400_000))


async def run_wd_backtest(symbol, cfg, days, exclude_top_n=1):
    max_candles = BACKTEST_MAX_CANDLES.get("wt_div", 100_000)
    resolution = cfg.get("wd_resolution", "15m")
    if resolution in SUB_MINUTE_RESOLUTIONS:
        max_candles = min(max_candles, 5000)
    p = _wd_params(cfg)
    res_s = _res_seconds(resolution)
    lead = _lead_days(p, res_s) if resolution not in SUB_MINUTE_RESOLUTIONS else 0.0
    t_fetch = time.time()
    candles, err, src = await _we_candles(symbol, resolution, days + lead, max_candles, market_type=cfg.get("binance_market_type", "spot"))
    t_fetch = time.time() - t_fetch
    if err:
        return {"error": err}
    warm = _wd_warm(p)
    if not candles or len(candles[4]) < warm + 100:
        return {"error": f"Zu wenig historische Kerzen für einen aussagekräftigen Backtest erhalten (mind. ~{warm + 100} nötig)."}
    ts, o, h, l, c = candles[0], candles[1], candles[2], candles[3], candles[4]
    t_calc = time.time()
    bull, bear, xdn, xup, wt1, wt2 = await asyncio.to_thread(wd_compute, o, h, l, c, p)
    start_i = max(warm, _window_start(ts, days))
    trades = _wd_simulate(candles, cfg, bull, bear, xdn, xup, start_i, p)
    t_calc = time.time() - t_calc
    stats = summarize_backtest_trades(trades, exclude_top_n)
    usable_days = (ts[-1] - ts[start_i]) / 86_400_000 if start_i < len(ts) else 0
    return {
        "symbol": symbol, "entry_mode": "wt_div", "resolution": resolution,
        "requested_days": days, "actual_days_covered": round(usable_days, 1),
        "candles_processed": len(ts) - start_i, "candle_cap": max_candles, "cache_used": src != "neu",
        "note": f"Kerzen holen {t_fetch:.1f}s ({ {'cache': 'Cache', 'abgeleitet': 'aus feinerem Zeitrahmen berechnet', 'neu': 'neu von Binance'}[src] }), Berechnung {t_calc:.1f}s. "
                f"Getestet wird das Fenster der letzten {round(usable_days, 1)} Tage (davor läuft die Welle ein); Ausstieg: {p['exit_mode']}.",
        "signals_buy": sum(bull[start_i:]), "signals_sell": sum(bear[start_i:]),
        "stats": stats,
        "stats_long": summarize_backtest_trades([t for t in trades if t["dir"] == "long"], exclude_top_n),
        "stats_short": summarize_backtest_trades([t for t in trades if t["dir"] == "short"], exclude_top_n),
        "trades": trades,
    }


# ============================================================================
# CHART
# ============================================================================
def wd_chart_payload(ts, o, h, l, c, cfg, max_bars=1000):
    """Kerzen + Welle (wt1/wt2) + Divergenz-Linien + Marker (nur geschlossene Kerzen). Die Marker sitzen auf der Bestaetigungs-Kerze,
    dort steigt der Bot ein. Je Divergenz eine Linie im Kurs-Fenster (Tief->Tief bzw. Hoch->Hoch) und eine im Wellen-Fenster."""
    p = _wd_params(cfg)
    n = len(c)
    if n < 20:
        return None
    wt1, wt2 = wd_wave(o[:-1], h[:-1], l[:-1], c[:-1], p)
    osc = wt1 if p["line"] == "wt1" else wt2
    pairs = []
    bull, bear = wd_divergences(h[:-1], l[:-1], osc, p, 0, pairs)
    bull, bear = bull + [False], bear + [False]
    off = max(0, n - max_bars)
    candles = [{"time": ts[i] // 1000, "open": o[i], "high": h[i], "low": l[i], "close": c[i]} for i in range(off, n)]
    markers = []
    for i in range(off, n):
        if bull[i]:
            markers.append({"time": ts[i] // 1000, "kind": "long", "price": c[i]})
        if bear[i]:
            markers.append({"time": ts[i] // 1000, "kind": "short", "price": c[i]})
    div_lines = [{"kind": d["kind"], "hidden": d["hidden"], "t1": ts[d["i1"]] // 1000, "t2": ts[d["i2"]] // 1000,
                  "w1": round(d["w1"], 2), "w2": round(d["w2"], 2), "p1": d["p1"], "p2": d["p2"]}
                 for d in pairs if d["i1"] >= off][-60:]
    wt1l = [{"time": ts[i] // 1000, "value": round(wt1[i], 2)} for i in range(off, n - 1)]
    wt2l = [{"time": ts[i] // 1000, "value": round(wt2[i], 2)} for i in range(off, n - 1)]
    levels = sorted({p["os"], p["ob"]} | ({p["exit_zone"], -p["exit_zone"]} if p["exit_mode"] == "wt_cross_zone" else set()))
    return {"candles": candles, "wt1": wt1l, "wt2": wt2l, "markers": markers, "levels": levels, "div_lines": div_lines, "entry_mode": "wd",
            "wt1v": round(wt1[-1], 2), "wt2v": round(wt2[-1], 2), "bars": n}


# ============================================================================
# MONTE-CARLO: zufaellige Kombinationen der AUSGEWAEHLTEN Werte, Rest bleibt wie in den Einstellungen
# ============================================================================
WD_MC_PARAMS = {   # Kurzschluessel -> (Config-Schluessel, Typ, Nachkommastellen)
    "n1": ("wd_n1", int, 0), "n2": ("wd_n2", int, 0), "sig": ("wd_sig_len", int, 0), "scale": ("wd_wave_scale", float, 2),
    "pl": ("wd_pivot_left", int, 0), "pr": ("wd_pivot_right", int, 0), "minr": ("wd_min_range", int, 0),
    "maxr": ("wd_max_range", int, 0), "ob": ("wd_div_ob", float, 0), "os": ("wd_div_os", float, 0),
    "exit_zone": ("wd_exit_zone", float, 0), "sl_usd": ("wd_sl_usd", float, 2), "tp_usd": ("wd_tp_usd", float, 2),
    "tp1_usd": ("wd_tp1_usd", float, 2), "tp2_usd": ("wd_tp2_usd", float, 2), "tp3_usd": ("wd_tp3_usd", float, 2),
    "tp1_pct": ("wd_tp1_pct", float, 0), "tp2_pct": ("wd_tp2_pct", float, 0),
}
WD_MC_MAX_RUNS = 1000
WD_MC_MIN_RELIABLE_TRADES = 5
WD_MC_TEST_FRACTION = 0.3
WD_MC_MAX_CANDLES = 80_000
WD_MC_TIME_BUDGET = 70


def _wd_mc_compute(candles, cfg, combos, base_p, start_i, exclude_top_n, deadline):
    ts, o, h, l, c = candles[0], candles[1], candles[2], candles[3], candles[4]
    n = len(c)
    split_ts = ts[start_i + int((n - start_i) * (1 - WD_MC_TEST_FRACTION))]

    def run_one(over):
        p = dict(base_p, **over)
        bull, bear, xdn, xup, _w1, _w2 = wd_compute(o, h, l, c, p)
        trades = _wd_simulate(candles, cfg, bull, bear, xdn, xup, start_i, p)
        st = summarize_backtest_trades(trades, exclude_top_n)
        tr = summarize_backtest_trades([t for t in trades if t.get("entry_ts", 0) < split_ts], exclude_top_n)
        te = summarize_backtest_trades([t for t in trades if t.get("entry_ts", 0) >= split_ts], exclude_top_n)
        row = {WD_MC_PARAMS[k][0]: p[k] for k in WD_MC_PARAMS}
        row.update({"trades": st["trades"], "win_rate_pct": st["win_rate_pct"], "total_pnl_usd": st["total_pnl_usd"],
                    "total_pnl_excl_top_n_usd": st["total_pnl_excl_top_n_usd"], "max_drawdown_usd": st["max_drawdown_usd"],
                    "pnl_train": tr["total_pnl_usd"], "pnl_test": te["total_pnl_usd"], "trades_test": te["trades"]})
        return row

    out = []
    for over in combos:
        if time.time() > deadline:
            break
        out.append(run_one(over))
    return out, run_one({})


async def run_wd_montecarlo(symbol, cfg, days, runs, vary, exclude_top_n=1, seed=None, max_dd_usd=0.0):
    """vary: {'pl': (a, b), ...} - nur diese Werte werden zufaellig gezogen, alle anderen bleiben wie in der Config. Die aktuellen
    Einstellungen laufen IMMER als Referenz (= identisch mit dem Backtest). Alle Kombinationen handeln im selben Fenster 'letzte N Tage'."""
    base_p = _wd_params(cfg)
    resolution = cfg.get("wd_resolution", "15m")
    res_s = _res_seconds(resolution)
    if not vary:
        return {"error": "Bitte mindestens einen Wert zum Variieren auswählen."}
    ranges = {}
    try:
        for k, (a, b) in vary.items():
            if k not in WD_MC_PARAMS:
                return {"error": f"Unbekannter Wert '{k}'."}
            typ = WD_MC_PARAMS[k][1]
            lo, hi = sorted((typ(a), typ(b)))
            if (k in ("sl_usd", "tp_usd", "tp1_usd", "tp2_usd", "tp3_usd", "exit_zone") and lo < 0) or (k not in ("ob", "os", "sl_usd", "tp_usd", "tp1_usd", "tp2_usd", "tp3_usd", "exit_zone") and lo <= 0):
                return {"error": "Alle Bereiche müssen größer als 0 sein."}
            ranges[k] = (lo, hi)
    except (TypeError, ValueError):
        return {"error": "Ungültige Bereiche für die Parameter."}
    if base_p["tp_mode"] != "partial" and any(k in ranges for k in ("tp1_usd", "tp2_usd", "tp3_usd", "tp1_pct", "tp2_pct")):
        return {"error": "TP1/TP2/TP3 wirken nur, wenn oben bei „Take-Profit-Art“ „Teil-TPs“ gewählt ist. Bitte dort umstellen (oder die Haken entfernen)."}
    if base_p["tp_mode"] == "partial" and "tp_usd" in ranges:
        return {"error": "Der einzelne Take-Profit wirkt im Modus „Teil-TPs“ nicht - bitte stattdessen TP1/TP2/TP3 variieren."}
    max_candles = BACKTEST_MAX_CANDLES.get("wt_div", 100_000)
    if resolution in SUB_MINUTE_RESOLUTIONS:
        max_candles = min(max_candles, 5000)
    p_max = dict(base_p, **{k: ranges[k][1] for k in ranges})
    lead = _lead_days(p_max, res_s) if resolution not in SUB_MINUTE_RESOLUTIONS else 0.0
    candles, err, src = await _we_candles(symbol, resolution, days + lead, max_candles, market_type=cfg.get("binance_market_type", "spot"))
    if err:
        return {"error": err}
    n = len(candles[4]) if candles else 0
    note = {"cache": "Kerzen aus dem Cache.", "abgeleitet": "Kerzen aus einem feineren Zeitrahmen im Cache berechnet.", "neu": "Kerzen neu von Binance geladen."}[src]
    if n > WD_MC_MAX_CANDLES:
        candles = tuple(x[-WD_MC_MAX_CANDLES:] for x in candles)
        note += f" Hinweis: aus Speichergründen wurden nur die jüngsten {WD_MC_MAX_CANDLES} von {n} Kerzen getestet."
        n = WD_MC_MAX_CANDLES
    ts = candles[0]
    start_i = max(_wd_warm(p_max), _window_start(ts, days)) if n else 0
    if n < 400 or n - start_i < 300:
        return {"error": f"Zu wenig Kerzen: {n} erhalten, die Welle braucht ~{_wd_warm(p_max)} Kerzen Einlaufzeit, danach sollten noch mind. 300 zum Testen übrig sein. Mehr Tage wählen."}
    runs = max(1, min(WD_MC_MAX_RUNS, int(runs)))
    rng = random.Random(seed)

    def draw(k):
        lo, hi = ranges[k]
        typ, dec = WD_MC_PARAMS[k][1], WD_MC_PARAMS[k][2]
        return rng.randint(lo, hi) if typ is int else round(rng.uniform(lo, hi), dec)

    keys = sorted(ranges)
    seen, combos = {tuple(base_p[k] for k in keys)}, []
    for _ in range(runs * 30):
        if len(combos) >= runs:
            break
        over = {k: draw(k) for k in ranges}
        eff = dict(base_p, **over)
        if eff["minr"] >= eff["maxr"]:
            continue
        if eff["tp_mode"] == "partial" and not (eff["tp1_usd"] > 0 and (eff["tp2_usd"] == 0 or eff["tp2_usd"] > eff["tp1_usd"])
                                                and (eff["tp3_usd"] == 0 or eff["tp3_usd"] > max(eff["tp1_usd"], eff["tp2_usd"]))):
            continue   # TP-Stufen muessen aufsteigend sein (TP1 < TP2 < TP3)
        key = tuple(over[k] for k in keys)
        if key not in seen:
            seen.add(key)
            combos.append(over)
    deadline = time.time() + WD_MC_TIME_BUDGET
    results, baseline = await asyncio.to_thread(_wd_mc_compute, candles, cfg, combos, base_p, start_i, exclude_top_n, deadline)
    reliable = [r for r in results if r["trades"] >= WD_MC_MIN_RELIABLE_TRADES]
    robust = [r for r in reliable if r["pnl_train"] > 0 and r["pnl_test"] > 0 and r["trades_test"] >= 3]
    best = sorted(results, key=lambda r: (r["trades"] >= WD_MC_MIN_RELIABLE_TRADES, r["total_pnl_usd"]), reverse=True)
    robust_sorted = sorted(robust, key=lambda r: r["pnl_test"], reverse=True)
    worst = sorted(results, key=lambda r: r["total_pnl_usd"])
    lowdd = []
    for r in reliable:
        dd = abs(r["max_drawdown_usd"] or 0)
        if r["total_pnl_usd"] <= 0 or (max_dd_usd > 0 and dd > max_dd_usd):
            continue
        lowdd.append(dict(r, pnl_dd_ratio=round(r["total_pnl_usd"] / dd, 2) if dd > 0 else 999.0))
    lowdd.sort(key=lambda r: (r["pnl_dd_ratio"], r["pnl_test"]), reverse=True)
    pnls = sorted(r["total_pnl_usd"] for r in reliable)
    usable_days = (ts[-1] - ts[start_i]) / 86_400_000
    return {
        "symbol": symbol, "resolution": resolution, "exit_mode": base_p["exit_mode"],
        "requested_days": days, "actual_days_covered": round((ts[-1] - ts[0]) / 86_400_000, 1), "usable_days": round(usable_days, 1),
        "candles_processed": n, "min_reliable_trades": WD_MC_MIN_RELIABLE_TRADES,
        "combos_tested": len(results), "combos_requested": len(combos), "test_fraction_pct": int(WD_MC_TEST_FRACTION * 100),
        "varied": [WD_MC_PARAMS[k][0] for k in WD_MC_PARAMS if k in ranges], "note": note,
        "baseline": baseline,
        "profitable_pct": round(sum(1 for x in pnls if x > 0) / len(pnls) * 100, 1) if pnls else 0,
        "median_pnl_usd": round(pnls[len(pnls) // 2], 2) if pnls else 0,
        "robust_count": len(robust), "lowdd_count": len(lowdd), "max_dd_limit": max_dd_usd,
        "lowdd_results": lowdd[:30], "robust_results": robust_sorted[:30], "results": best[:30], "worst_results": worst[:20],
    }
