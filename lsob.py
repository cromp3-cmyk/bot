"""
lsob.py - eigene Strategie "LSOB" (entry_mode "lsob", Config-Praefix lb_*).

Regeln nach dem Bootcamp-Video von Claudius Vertesi (Tag 7) und den Eingaben des Original-Indikators LSOB 3.0 (Long; Short spiegelverkehrt):
  1. SWEEP: Pivot-Tief, das UNTER dem vorigen Pivot-Tief liegt (lb_require_sweep).
  2. REAKTION: 1-2 bullische Kerzen direkt danach (lb_need_reaction).
  3. BOX: Tief..Hoch der tiefsten Kerze (lb_box_mode: range / body).
  4. AUSBRUCH: eine Kerze schliesst UEBER der Box (lb_need_breakout) -> Box ist 'scharf'.
  5. RETEST: Docht sticht in die Box und/oder Koerper schliesst am Rand; ein Schluss darf hoechstens lb_retest_tol_pct % der Boxhoehe in die Box
     (tiefer = Box ungueltig); Docht hoechstens lb_max_pen_pct %; Schluss weiter als lb_inval_tol_pct % jenseits der Box = gebrochen.
  6. ABLEHNUNGSKERZE: bullisch mit Docht nach unten (lb_need_rejection). Einstieg = OPEN der Folgekerze (Market).
  7. SL = Boxende (lb_sl_mode "box", wie vom Nutzer beschrieben) oder Docht der Ablehnungskerze ("wick", wie im Video); TP = lb_rr x Risiko;
     bei lb_be_frac des Weges SL auf Einstieg (+ lb_be_offset_pct fuer Gebuehren); Mindest-TP-Abstand lb_min_tp_pct (Video: 0,5 %);
     optionaler Zeitfilter (lb_session_*, Europa/Berlin). Kein Signal-Ausstieg, keine Teil-TPs.
Nicht umgesetzt: Risiko-basierte Positionsgroesse, Limit-Order-TP, Wall-/Widerstandsfilter (subjektiv).
Kein Blick in die Zukunft: Pivot erst nach 'rechts' Kerzen bestaetigt, Signale nur auf geschlossenen Kerzen.
"""

import asyncio
import time
import traceback

from bot_core import BOTS, debug_log, execute_entry, execute_exit, execute_partial_exit
from strategies import (
    SUB_MINUTE_RESOLUTIONS, get_seconds_candles, fetch_candles_binance_multi,
    summarize_backtest_trades, BACKTEST_MAX_CANDLES, _bt_close_trade,
)
from wae_ema import _we_candles, _res_seconds, _poll_delay


def _f(cfg, key, default):
    v = cfg.get(key, default)
    return default if v in (None, "") else float(v)


def _lb_params(cfg):
    mode = cfg.get("lb_direction_mode", "both")
    ent = cfg.get("lb_entry_mode", "both")
    return {
        "pl": max(1, int(_f(cfg, "lb_pivot_left", 5))), "pr": max(1, int(_f(cfg, "lb_pivot_right", 5))),
        "inval": max(0.0, _f(cfg, "lb_inval_tol_pct", 20)) / 100.0, "retest": max(0.0, _f(cfg, "lb_retest_tol_pct", 10)) / 100.0,
        "strict": bool(cfg.get("lb_strict_wick", False)), "max_pen": max(0.0, _f(cfg, "lb_max_pen_pct", 50)) / 100.0,
        "sweep": bool(cfg.get("lb_require_sweep", True)),
        "need_reaction": bool(cfg.get("lb_need_reaction", True)), "need_breakout": bool(cfg.get("lb_need_breakout", True)),
        "need_rejection": bool(cfg.get("lb_need_rejection", True)),
        "box": "body" if cfg.get("lb_box_mode", "range") == "body" else "range",
        "entry": ent if ent in ("touch", "close_in", "both") else "both",
        "sl_mode": "wick" if cfg.get("lb_sl_mode", "box") == "wick" else "box",
        "max_age": max(5, int(_f(cfg, "lb_max_age", 2000))),
        "direction": mode if mode in ("both", "long_only", "short_only") else "both",
        "rr": max(0.1, _f(cfg, "lb_rr", 2.0)),
        "be_frac": max(0.0, min(0.99, _f(cfg, "lb_be_frac", 0.5))), "be_offset_pct": max(0.0, _f(cfg, "lb_be_offset_pct", 0.0)), "tp1_pct": max(0.0, min(99.0, _f(cfg, "lb_tp1_pct", 50))),
        "buffer_pct": max(0.0, _f(cfg, "lb_sl_buffer_pct", 0.0)),
        "min_risk_pct": max(0.0, _f(cfg, "lb_min_risk_pct", 0.0)), "max_risk_pct": max(0.0, _f(cfg, "lb_max_risk_pct", 0.0)),
        "min_tp_pct": max(0.0, _f(cfg, "lb_min_tp_pct", 0.0)),
        "session_on": bool(cfg.get("lb_session_on", False)),
        "session_from": int(_f(cfg, "lb_session_from", 9)), "session_to": int(_f(cfg, "lb_session_to", 23)),
    }


def _lb_warm(p):
    return p["pl"] + p["pr"] + min(p["max_age"], 300) + 10   # Boxen aelter als die geladene Historie (max. ~1000 Kerzen live) sieht der Bot nicht


def _in_session(ts_ms, p):
    """Einstiege nur zwischen session_from und session_to Uhr (Europe/Berlin), falls aktiviert."""
    if not p["session_on"]:
        return True
    try:
        from zoneinfo import ZoneInfo
        from datetime import datetime
        hr = datetime.fromtimestamp(ts_ms / 1000.0, ZoneInfo("Europe/Berlin")).hour
    except Exception:
        return True
    a, b = p["session_from"], p["session_to"]
    return (a <= hr < b) if a <= b else (hr >= a or hr < b)


def _entry_check(direction, entry, stop, p):
    """-> 'ok' | 'wrong_side' (Einstieg schon jenseits des SL) | 'filter' (Risiko-/Mindest-TP-Filter)."""
    risk = (entry - stop) if direction == "long" else (stop - entry)
    if not entry or risk <= 0:
        return "wrong_side"
    rp = risk / entry * 100.0
    if (p["min_risk_pct"] > 0 and rp < p["min_risk_pct"]) or (p["max_risk_pct"] > 0 and rp > p["max_risk_pct"]) or (p["min_tp_pct"] > 0 and rp * p["rr"] < p["min_tp_pct"]):
        return "filter"
    return "ok"


# ============================================================================
# RECHNUNG (rein, ohne Netzwerk)
# ============================================================================
def lb_signals(o, h, l, c, p):
    """-> (sig[], sl[], boxes[]). sig[j] = +1 Long / -1 Short auf der Ablehnungskerze des Retests (Einstieg dann zum OPEN der naechsten Kerze);
    sl[j] = Stop-Preis. boxes = alle Boxen: {kind, i0, conf, top, bottom, armed, end, state: open/used/broken/expired/skipped}.
    Ablauf (Long): Sweep (neues Pivot-Tief unter dem vorigen) -> 1-2 bullische Reaktionskerzen -> Box um die tiefste Kerze (Tief..Hoch) -> eine Kerze schliesst
    UEBER der Box (Ausbruch, 'armed') -> Retest: Docht sticht in die Box und/oder Koerper schliesst an der Kante, hoechstens 'retest' % der Boxhoehe hinein
    (tiefer = Box ungueltig) -> bullische Ablehnungskerze mit Docht nach unten. Short spiegelverkehrt."""
    n = len(c)
    pl, pr = p["pl"], p["pr"]
    sig, sl = [0] * n, [None] * n
    boxes, active = [], []
    last_lo = last_hi = None
    for j in range(n):
        # 1) bestehende Boxen pruefen (nur Boxen, die VOR dieser Kerze bestaetigt waren)
        hits = []
        for b in list(active):
            if j - b["conf"] > p["max_age"]:
                b["state"], b["end"] = "expired", j
                active.remove(b)
                continue
            hb = max(b["top"] - b["bottom"], 1e-12)
            long_ = b["kind"] == "long"
            far_break = ((l[j] if p["strict"] else c[j]) < b["bottom"] - p["inval"] * hb) if long_ else ((h[j] if p["strict"] else c[j]) > b["top"] + p["inval"] * hb)
            if far_break:
                b["state"], b["end"] = "broken", j
                active.remove(b)
                continue
            if not b["armed"]:
                if (c[j] > b["top"]) if long_ else (c[j] < b["bottom"]):
                    b["armed"] = True   # Ausbruch: Kerze schliesst jenseits der Box -> ab der naechsten Kerze zaehlt ein Retest
                continue
            if long_:
                depth = b["top"] - c[j]                                  # > 0: Schluss in der Box
                pen = b["top"] - l[j]                                    # Eindringtiefe des Dochts
                touch = l[j] <= b["top"] and pen <= p["max_pen"] * hb
                close_ok = c[j] <= b["top"]
                rej = (not p["need_rejection"]) or (c[j] > o[j] and min(o[j], c[j]) - l[j] > 0)
            else:
                depth = c[j] - b["bottom"]
                pen = h[j] - b["bottom"]
                touch = h[j] >= b["bottom"] and pen <= p["max_pen"] * hb
                close_ok = c[j] >= b["bottom"]
                rej = (not p["need_rejection"]) or (c[j] < o[j] and h[j] - max(o[j], c[j]) > 0)
            if depth > p["retest"] * hb:           # Schluss tiefer als erlaubt in der Box -> Setup ungueltig
                b["state"], b["end"] = "broken", j
                active.remove(b)
                continue
            if ((p["entry"] in ("touch", "both") and touch) or (p["entry"] in ("close_in", "both") and close_ok)) and rej:
                b["state"], b["end"] = "used", j
                active.remove(b)
                hits.append(b)
        lh = [b for b in hits if b["kind"] == "long"]
        sh = [b for b in hits if b["kind"] == "short"]
        pick = None
        if lh and not sh:
            pick = max(lh, key=lambda b: b["conf"])
        elif sh and not lh:
            pick = max(sh, key=lambda b: b["conf"])
        if pick is not None:
            if pick["kind"] == "long":
                base = l[j] if p["sl_mode"] == "wick" else pick["bottom"]
                stop = base * (1 - p["buffer_pct"] / 100.0)
            else:
                base = h[j] if p["sl_mode"] == "wick" else pick["top"]
                stop = base * (1 + p["buffer_pct"] / 100.0)
            chk = _entry_check(pick["kind"], c[j], stop, p)
            if chk == "wrong_side":
                pick["state"], pick["end"] = "open", None   # Schluss schon jenseits des SL: kein Einstieg, Box bleibt lebendig
                active.append(pick)
            elif chk == "filter":
                pick["state"] = "skipped"
            else:
                sig[j] = 1 if pick["kind"] == "long" else -1
                sl[j] = stop
                pick["trig"] = j
        # 2) neue Pivots, die auf dieser Kerze BESTAETIGT werden (Pivot-Kerze = j - rechts)
        i = j - pr
        if i >= pl:
            body_lo, body_hi = min(o[i], c[i]), max(o[i], c[i])
            if l[i] < min(l[i - pl:i]) and l[i] <= min(l[i + 1:i + pr + 1]):
                if (not p["sweep"]) or last_lo is None or l[i] < last_lo:
                    top, bot = (h[i], l[i]) if p["box"] == "range" else (body_hi, body_lo)
                    react = (not p["need_reaction"]) or any(c[k] > o[k] for k in range(i + 1, min(i + 2, j) + 1))
                    if react:
                        armed = (not p["need_breakout"]) or any(c[k] > top for k in range(i + 1, j + 1))
                        bx = {"kind": "long", "i0": i, "conf": j, "top": top, "bottom": bot, "armed": armed, "end": None, "state": "open"}
                        boxes.append(bx)
                        active.append(bx)
                last_lo = l[i]
            if h[i] > max(h[i - pl:i]) and h[i] >= max(h[i + 1:i + pr + 1]):
                if (not p["sweep"]) or last_hi is None or h[i] > last_hi:
                    top, bot = (h[i], l[i]) if p["box"] == "range" else (body_hi, body_lo)
                    react = (not p["need_reaction"]) or any(c[k] < o[k] for k in range(i + 1, min(i + 2, j) + 1))
                    if react:
                        armed = (not p["need_breakout"]) or any(c[k] < bot for k in range(i + 1, j + 1))
                        bx = {"kind": "short", "i0": i, "conf": j, "top": top, "bottom": bot, "armed": armed, "end": None, "state": "open"}
                        boxes.append(bx)
                        active.append(bx)
                last_hi = h[i]
    return sig, sl, boxes


def _allowed(target, p):
    return not ((target == "long" and p["direction"] == "short_only") or (target == "short" and p["direction"] == "long_only"))


def _levels(direction, entry, stop, p):
    """-> (sl, tp, be_trigger) aus Einstieg und Stop."""
    risk = abs(entry - stop)
    sgn = 1 if direction == "long" else -1
    tp = entry + sgn * risk * p["rr"]
    be = entry + sgn * risk * p["rr"] * p["be_frac"] if p["be_frac"] > 0 else None
    return stop, tp, be


def _be_price(direction, entry, p):
    """Stop nach dem Break-Even-Schritt: Einstieg, optional ein paar Cent (Prozent) im Gewinn, um Gebuehren abzudecken."""
    sgn = 1 if direction == "long" else -1
    return entry * (1 + sgn * p["be_offset_pct"] / 100.0)


# ============================================================================
# LIVE
# ============================================================================
def _lb_reset_state(st):
    st["lb_sl_price"] = None
    st["lb_tp_price"] = None
    st["lb_be_price"] = None
    st["lb_be"] = False


async def check_lb_sl_tp(symbol, price):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    pos = st["position"]
    if pos is None or price is None:
        return
    sl, tp, be_px = st.get("lb_sl_price"), st.get("lb_tp_price"), st.get("lb_be_price")
    if sl is not None and ((pos == "long" and price <= sl) or (pos == "short" and price >= sl)):
        prot = st.get("lb_be")
        reason = "BREAKEVEN" if prot else "SL"
        debug_log(f"🚪 [{symbol}] LSOB {reason}: {pos.upper()} @ {price} (Ziel war {round(sl, 6)})")
        await execute_exit(symbol, price, reason)
        if st["position"] is None:
            if not prot:
                st["lb_sl_cooldown_until"] = time.time() + cfg.get("lb_sl_cooldown_seconds", 30)
            _lb_reset_state(st)
        return
    if tp is not None and ((pos == "long" and price >= tp) or (pos == "short" and price <= tp)):
        debug_log(f"🎯 [{symbol}] LSOB TP: {pos.upper()} @ {price} (Ziel war {round(tp, 6)})")
        await execute_exit(symbol, price, "TP")
        if st["position"] is None:
            _lb_reset_state(st)
        return
    if be_px is not None and not st.get("lb_be") and st.get("avg_entry_price") and ((pos == "long" and price >= be_px) or (pos == "short" and price <= be_px)):
        p_ = _lb_params(cfg)
        if p_["tp1_pct"] > 0:
            if not await execute_partial_exit(symbol, price, p_["tp1_pct"] / 100.0, "TP1"):
                return   # Teilverkauf fehlgeschlagen -> naechster Tick versucht es erneut
            if st["position"] is None:
                _lb_reset_state(st)
                return
            debug_log(f"🎯 [{symbol}] LSOB TP1 ({p_['tp1_pct']:g} %) bei halbem Weg erreicht")
        st["lb_sl_price"], st["lb_be"] = _be_price(pos, st["avg_entry_price"], p_), True
        debug_log(f"📡 [{symbol}] LSOB Break-Even-Schritt erreicht - SL auf Einstieg ({round(st['lb_sl_price'], 6)})")


async def check_lb_signal(symbol, sig_i, sl_i, price):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or price is None or not sig_i or sl_i is None:
        return
    if st["position"] is not None:
        return   # Signal waehrend einer offenen Position: ignoriert (die Box ist trotzdem verbraucht)
    p = _lb_params(cfg)
    tri = "long" if sig_i > 0 else "short"
    if not _allowed(tri, p) or time.time() < st.get("lb_sl_cooldown_until", 0.0):
        return
    if not _in_session(time.time() * 1000, p):
        debug_log(f"⏭️ [{symbol}] LSOB Signal {tri.upper()} außerhalb der Handelszeit ({p['session_from']}-{p['session_to']} Uhr) übersprungen")
        return
    chk = _entry_check(tri, price, sl_i, p)
    if chk != "ok":
        debug_log(f"⏭️ [{symbol}] LSOB Signal {tri.upper()} übersprungen ({'Preis schon jenseits des SL' if chk == 'wrong_side' else 'Risiko-/Mindest-TP-Filter'}): Preis {price}, SL {round(sl_i, 6)}")
        return
    debug_log(f"📡 [{symbol}] LSOB Einstieg: {tri.upper()} @ {price}, SL {round(sl_i, 6)} ({p['sl_mode']})")
    await execute_entry(symbol, tri, price, is_add_on=False)
    if st["position"] is not None:
        _lb_reset_state(st)
        entry = st.get("avg_entry_price") or price
        sl, tp, be = _levels(tri, entry, sl_i, p)
        st["lb_sl_price"], st["lb_tp_price"], st["lb_be_price"] = sl, tp, be


async def lb_poll_loop(symbol):
    """LSOB live: wertet NUR abgeschlossene Kerzen aus; SL/TP/Break-Even laufend gegen den Live-Preis."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0
    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "lsob" and cfg["bot_active"] and b["state"].get("session_started"):
                resolution = cfg.get("lb_resolution", "15m")
                p = _lb_params(cfg)
                res_s = _res_seconds(resolution)
                min_needed = _lb_warm(p) + 2
                needed_bars = min(1000, max(min_needed * 2, 500))
                st = b["state"]
                closed_ts = None
                if resolution in SUB_MINUTE_RESOLUTIONS:
                    local = get_seconds_candles(st, SUB_MINUTE_RESOLUTIONS[resolution], needed_bars)
                    if local:
                        closed_ts, closed_o, closed_h, closed_l, closed_c = local
                        sec_len = SUB_MINUTE_RESOLUTIONS[resolution]
                        if closed_ts and closed_ts[-1] + sec_len * 1000 > time.time() * 1000:   # laufende Kerze nie auswerten
                            closed_ts, closed_o, closed_h, closed_l, closed_c = closed_ts[:-1], closed_o[:-1], closed_h[:-1], closed_l[:-1], closed_c[:-1]
                else:
                    _need_fetch = last_processed_ts is None or (time.time() % res_s) < 6 or res_s <= 60
                    data = await fetch_candles_binance_multi(symbol, resolution, count_back=needed_bars, market_type=cfg.get("binance_market_type", "spot")) if _need_fetch else None
                    if data:
                        timestamps, opens, highs, lows, closes = data[:5]
                        closed_ts, closed_o, closed_h, closed_l, closed_c = timestamps[:-1], opens[:-1], highs[:-1], lows[:-1], closes[:-1]

                now = time.time()
                due_heartbeat = now - last_heartbeat > 300
                if st["position"] is not None and st["last_price"] is not None:
                    await check_lb_sl_tp(symbol, st["last_price"])

                if closed_ts and len(closed_c) > min_needed:
                    age = (now * 1000 - closed_ts[-1]) / 1000
                    if age > 300 + res_s:
                        debug_log(f"⚠️ [{symbol}] LSOB: letzte Kerze wirkt veraltet ({round(age)}s alt, Auflösung {resolution}) - überspringe Durchlauf.")
                        closed_ts = None
                    elif st["last_price"] is not None and closed_c[-1] and abs(closed_c[-1] - st["last_price"]) / st["last_price"] * 100 > 2.0:
                        debug_log(f"⚠️ [{symbol}] LSOB: Kerzendaten unplausibel (Kerze {closed_c[-1]} vs. Live {st['last_price']}) - überspringe Durchlauf.")
                        closed_ts = None

                if closed_ts and len(closed_c) > min_needed and (last_processed_ts is None or closed_ts[-1] > last_processed_ts):
                    sig, slv, _bx = await asyncio.to_thread(lb_signals, closed_o, closed_h, closed_l, closed_c, p)
                    if due_heartbeat:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] LSOB aktiv: {resolution}, Pivot {p['pl']}/{p['pr']}, Einstieg={p['entry']}, RR 1:{p['rr']:g}, "
                                  f"BE bei {p['be_frac']:g} des Ziels, Preis={closed_c[-1]}")
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
                        await check_lb_signal(symbol, sig[idx], slv[idx], price_i)
                elif due_heartbeat and not closed_ts:
                    last_heartbeat = now
                    debug_log(f"⏳ [{symbol}] LSOB wartet: keine Kerzen erhalten (Auflösung {resolution})")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] LSOB-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        await asyncio.sleep(_poll_delay(b["config"].get("lb_resolution", "15m"), b["state"]))


# ============================================================================
# BACKTEST
# ============================================================================
def _lb_simulate(candles, cfg, sig, slv, start_i, p):
    """Gegenstueck zu check_lb_signal / check_lb_sl_tp: Signal auf der Ablehnungskerze i -> Einstieg zum OPEN der Kerze i+1 (Market). Ab dieser Kerze
    (inklusive ihr selbst) SL/TP/Break-Even ueber Hoch/Tief; bei Gleichzeitigkeit gewinnt der SL; Break-Even gilt erst ab der Kerze NACH dem Erreichen des
    Break-Even-Schritts. Cooldown nach SL wie live."""
    ts, o, h, l, c = candles[0], candles[1], candles[2], candles[3], candles[4]
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    cooldown_ms = cfg.get("lb_sl_cooldown_seconds", 30) * 1000
    pos, trades, cool_until = None, [], None
    for i in range(max(start_i, 1), n):
        if pos is None and sig[i - 1] and slv[i - 1] is not None and i - 1 >= start_i - 1:
            d = "long" if sig[i - 1] > 0 else "short"
            entry = o[i]
            if _allowed(d, p) and not (cool_until is not None and ts[i] < cool_until) and _in_session(ts[i], p) and _entry_check(d, entry, slv[i - 1], p) == "ok":
                sl, tp, be_px = _levels(d, entry, slv[i - 1], p)
                pos = {"dir": d, "entry": entry, "size": (margin * leverage) / entry, "entry_i": i, "sl": sl, "tp": tp, "be_px": be_px, "be": False}
        if pos is not None:
            d, e = pos["dir"], pos["entry"]
            if (d == "long" and l[i] <= pos["sl"]) or (d == "short" and h[i] >= pos["sl"]):
                be = pos["be"]
                _bt_close_trade(trades, d, e, pos["sl"], pos["size"], i, pos["entry_i"], "BREAKEVEN" if be else "SL", ts=ts)
                if not be:
                    cool_until = ts[i] + cooldown_ms
                pos = None
            elif (d == "long" and h[i] >= pos["tp"]) or (d == "short" and l[i] <= pos["tp"]):
                _bt_close_trade(trades, d, e, pos["tp"], pos["size"], i, pos["entry_i"], "TP", ts=ts)
                pos = None
            elif pos["be_px"] is not None and not pos["be"] and ((d == "long" and h[i] >= pos["be_px"]) or (d == "short" and l[i] <= pos["be_px"])):
                if p["tp1_pct"] > 0:
                    part = pos["size"] * p["tp1_pct"] / 100.0
                    _bt_close_trade(trades, d, e, pos["be_px"], part, i, pos["entry_i"], "TP1", ts=ts)
                    pos["size"] -= part
                pos["sl"], pos["be"] = _be_price(d, e, p), True
    if pos is not None:
        _bt_close_trade(trades, pos["dir"], pos["entry"], c[n - 1], pos["size"], n - 1, pos["entry_i"], "END-OF-BACKTEST", ts=ts)
    return trades


def _lead_days(p, res_s):
    return _lb_warm(p) * res_s / 86400 + 0.1


def _window_start(ts, days):
    from bisect import bisect_left
    return bisect_left(ts, ts[-1] - int(days * 86_400_000))


async def run_lb_backtest(symbol, cfg, days, exclude_top_n=1):
    max_candles = BACKTEST_MAX_CANDLES.get("lsob", 100_000)
    resolution = cfg.get("lb_resolution", "15m")
    if resolution in SUB_MINUTE_RESOLUTIONS:
        max_candles = min(max_candles, 5000)
    p = _lb_params(cfg)
    res_s = _res_seconds(resolution)
    lead = _lead_days(p, res_s) if resolution not in SUB_MINUTE_RESOLUTIONS else 0.0
    t_fetch = time.time()
    candles, err, src = await _we_candles(symbol, resolution, days + lead, max_candles, market_type=cfg.get("binance_market_type", "spot"))
    t_fetch = time.time() - t_fetch
    if err:
        return {"error": err}
    warm = _lb_warm(p)
    if not candles or len(candles[4]) < warm + 100:
        return {"error": f"Zu wenig historische Kerzen für einen aussagekräftigen Backtest erhalten (mind. ~{warm + 100} nötig)."}
    ts, o, h, l, c = candles[0], candles[1], candles[2], candles[3], candles[4]
    t_calc = time.time()
    sig, slv, boxes = await asyncio.to_thread(lb_signals, o, h, l, c, p)
    start_i = max(warm, _window_start(ts, days))
    trades = _lb_simulate(candles, cfg, sig, slv, start_i, p)
    t_calc = time.time() - t_calc
    stats = summarize_backtest_trades(trades, exclude_top_n)
    usable_days = (ts[-1] - ts[start_i]) / 86_400_000 if start_i < len(ts) else 0
    states = {}
    for bx in boxes:
        if bx["conf"] >= start_i:
            states[bx["state"]] = states.get(bx["state"], 0) + 1
    return {
        "symbol": symbol, "entry_mode": "lsob", "resolution": resolution,
        "requested_days": days, "actual_days_covered": round(usable_days, 1),
        "candles_processed": len(ts) - start_i, "candle_cap": max_candles, "cache_used": src != "neu",
        "note": f"Kerzen holen {t_fetch:.1f}s ({ {'cache': 'Cache', 'abgeleitet': 'aus feinerem Zeitrahmen berechnet', 'neu': 'neu von Binance'}[src] }), Berechnung {t_calc:.1f}s. "
                f"Boxen im Fenster: {sum(states.values())} (ausgelöst {states.get('used', 0)}, gebrochen {states.get('broken', 0)}, abgelaufen {states.get('expired', 0)}, "
                f"wegen Risiko-Filter übersprungen {states.get('skipped', 0)}, noch offen {states.get('open', 0)}). SL = Box-Ende, TP = 1:{p['rr']:g}.",
        "signals_buy": sum(1 for x in sig[start_i:] if x > 0), "signals_sell": sum(1 for x in sig[start_i:] if x < 0),
        "stats": stats,
        "stats_long": summarize_backtest_trades([t for t in trades if t["dir"] == "long"], exclude_top_n),
        "stats_short": summarize_backtest_trades([t for t in trades if t["dir"] == "short"], exclude_top_n),
        "trades": trades,
    }


# ============================================================================
# CHART
# ============================================================================
def lb_chart_payload(ts, o, h, l, c, cfg, max_bars=1000):
    """Kerzen + Boxen (rot = Short-Box oben, gruen = Long-Box unten) + Signal-Marker. Nur geschlossene Kerzen werden gerechnet. Eine Box reicht von der
    Pivot-Kerze bis zu ihrem Ende (ausgeloest / gebrochen / abgelaufen) bzw. bis zur aktuellen Kerze, solange sie noch offen ist."""
    p = _lb_params(cfg)
    n = len(c)
    if n < _lb_warm(p) // 2:
        return None
    sig, slv, boxes = lb_signals(o[:-1], h[:-1], l[:-1], c[:-1], p)
    off = max(0, n - max_bars)
    candles = [{"time": ts[i] // 1000, "open": o[i], "high": h[i], "low": l[i], "close": c[i]} for i in range(off, n)]
    markers, trades = [], []
    busy_until = -1   # solange ein Trade laeuft (bis SL/TP/Break-Even), kommt kein neuer
    for i in range(max(off, 1), n - 1):
        if sig[i] and i + 1 > busy_until:
            d = "long" if sig[i] > 0 else "short"
            e_i = i + 1                      # Einstieg zum Open der Folgekerze
            entry = o[e_i]
            if slv[i] is None or _entry_check(d, entry, slv[i], p) != "ok":
                continue
            sl0, tp, be_px = _levels(d, entry, slv[i], p)
            sl, be, res, x_i, x_px = sl0, False, "offen", n - 1, c[n - 1]
            for k in range(e_i, n):
                hit_sl = l[k] <= sl if d == "long" else h[k] >= sl
                hit_tp = h[k] >= tp if d == "long" else l[k] <= tp
                if hit_sl:
                    res, x_i, x_px = ("BE" if be else "SL"), k, sl
                    break
                if hit_tp:
                    res, x_i, x_px = "TP", k, tp
                    break
                if be_px is not None and not be and (h[k] >= be_px if d == "long" else l[k] <= be_px):
                    sl, be = _be_price(d, entry, p), True
            busy_until = x_i
            markers.append({"time": ts[e_i] // 1000, "kind": d, "price": entry, "text": "LSOB " + ("LONG" if d == "long" else "SHORT")})
            trades.append({"kind": d, "t1": ts[e_i] // 1000, "t2": ts[x_i] // 1000, "entry": entry, "sl": sl0, "tp": tp, "result": res})
    out = []
    for bx in boxes:
        if bx["state"] in ("broken", "expired", "skipped"):
            continue   # ungueltige Boxen verschwinden wie im Original-Indikator
        end = bx["end"] if bx["end"] is not None else n - 1
        if end < off:
            continue
        out.append({"kind": bx["kind"], "t1": ts[max(bx["i0"], off)] // 1000, "t2": ts[end] // 1000, "top": bx["top"], "bottom": bx["bottom"], "state": bx["state"], "armed": bool(bx.get("armed"))})
    out = out[-8:]
    return {"candles": candles, "wt1": [], "wt2": [], "markers": markers, "levels": [], "boxes": out, "lb_trades": trades[-12:], "entry_mode": "lsob", "bars": n}
