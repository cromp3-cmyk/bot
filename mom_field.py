"""
mom_field.py - eigene Strategie "Momentum-Feld" (entry_mode "mom_field", Config-Praefix mf_*).

Nachbau des Pine-Skripts "Momentum: Kerzenfaerbung + Punkte + Wert + MTF", Hintergrund-Modus "Durchgehend (Feld)":

  c    = hl2 - hl2[1]
  Mom  = 100 * EMA(EMA(c, MomLength), SmoothLength) / max(EMA(EMA(|c|, MomLength), SmoothLength), 0.000001)
  Kerze steigend (momUp) = Mom >= Mom[1]

  Feld/Signal: Sobald das Vorzeichen von Mom wechselt (Wert kreuzt die 0; erstes Vorzeichen zaehlt auch), beginnt ein neues Feld
    Mom > 0 -> gruenes Feld + BUY,  Mom < 0 -> rotes Feld + SELL.
  Das Feld beginnt RUECKWIRKEND bei der Kerze, an der die Kerzenfarbe (momUp) zuletzt gewechselt hat (runStart), bevor der Wert die 0
  kreuzte (mindestens 1 Kerze nach dem Start des vorigen Feldes). BUY/SELL-Label sitzen im Chart auf diesem Feldbeginn (wie im Pine).
  Der BOT handelt aber nicht rueckwirkend, sondern beim Schlusskurs der Kerze, auf der der Wert die 0 kreuzt (kein Repaint, nur geschlossene Kerzen).

Einstieg (mf_entry_mode): color (Standard) = sobald die Kerzenfarbe gegen das aktuelle Vorzeichen kippt (Feld "wird rot/gruen"), zusaetzlich beim Nulldurchgang;
cross = nur beim Nulldurchgang (wie das Pine-Signal live, ohne Rueckwirkung).

Ausstieg (mf_exit_mode): flip = Gegensignal dreht die Position (Standard); sltp_only = nur SL/TP (Gegensignale werden ignoriert).
Alles andere (SL/TP in $, Teil-TPs mit Break-Even, Mehrfach-Einstiege, Mindestabstand, nur im Plus schliessen, Cooldown nach SL, Richtungswahl)
funktioniert wie bei Momentum-Feld.
"""

import asyncio
import time
import traceback

from bot_core import BOTS, debug_log, execute_entry, execute_exit, execute_reverse, execute_partial_exit
from strategies import (
    SUB_MINUTE_RESOLUTIONS, get_seconds_candles, fetch_candles_binance_multi,
    summarize_backtest_trades, BACKTEST_MAX_CANDLES, _bt_close_trade,
)
from wae_ema import _we_candles, _res_seconds, _poll_delay

EXIT_MODES = ("flip", "sltp_only")


def _mf_params(cfg):
    em = cfg.get("mf_exit_mode", "flip")
    mode = cfg.get("mf_direction_mode", "both")
    return {
        "mom": max(1, int(cfg.get("mf_mom_len", 12) or 12)), "smooth": max(1, int(cfg.get("mf_smooth_len", 21) or 21)),
        "entry": "cross" if cfg.get("mf_entry_mode", "color") == "cross" else "color",
        "exit_mode": em if em in EXIT_MODES else "flip",
        "direction": mode if mode in ("both", "long_only", "short_only") else "both",
        "sl_usd": max(0.0, float(cfg.get("mf_sl_usd", 0.0) or 0.0)),
        "tp_usd": max(0.0, float(cfg.get("mf_tp_usd", 0.0) or 0.0)),
        "max_entries": max(1, min(20, int(cfg.get("mf_max_entries", 1) or 1))),
        "profit_only": bool(cfg.get("mf_profit_only_close", False)),
        "add_dist_pct": max(0.0, float(cfg.get("mf_add_dist_pct", 0) or 0)),
        "add_dist_dir": cfg.get("mf_add_dist_dir", "any") if cfg.get("mf_add_dist_dir", "any") in ("any", "against", "with") else "any",
        "tp_mode": "partial" if cfg.get("mf_tp_mode", "single") == "partial" else "single",
        "tp1_usd": max(0.0, float(cfg.get("mf_tp1_usd", 5.0) or 0.0)), "tp2_usd": max(0.0, float(cfg.get("mf_tp2_usd", 10.0) or 0.0)),
        "tp3_usd": max(0.0, float(cfg.get("mf_tp3_usd", 15.0) or 0.0)),
        "tp1_pct": max(1.0, min(100.0, float(cfg.get("mf_tp1_pct", 33) or 33))), "tp2_pct": max(1.0, min(100.0, float(cfg.get("mf_tp2_pct", 33) or 33))),
        "be": bool(cfg.get("mf_be_after_tp1", True)), "sl_to_tp1": bool(cfg.get("mf_sl_to_tp1_on_tp2", True)),
    }


def _mf_warm(p):
    """Kerzen, bis die doppelte EMA eingeschwungen ist."""
    return (p["mom"] + p["smooth"]) * 6 + 50


# ============================================================================
# RECHNUNG (rein, ohne Netzwerk)
# ============================================================================
def _ema(vals, n):
    """Pine ta.ema: Startwert = erster Wert, danach alpha = 2/(n+1)."""
    a = 2.0 / (n + 1)
    out, prev = [], None
    for v in vals:
        prev = v if prev is None else a * v + (1 - a) * prev
        out.append(prev)
    return out


def mf_momentum(h, l, mom_len, smooth_len):
    """-> (m[], m_prev[]) wie f_mom im Pine (m_prev[0] = m[0] wegen nz(m[1], m))."""
    hl2 = [(h[i] + l[i]) / 2.0 for i in range(len(h))]
    c = [0.0] + [hl2[i] - hl2[i - 1] for i in range(1, len(hl2))]
    num = _ema(_ema(c, mom_len), smooth_len)
    den = _ema(_ema([abs(x) for x in c], mom_len), smooth_len)
    m = [100.0 * num[i] / max(den[i], 0.000001) for i in range(len(c))]
    return m, ([m[0]] + m[:-1] if m else [])


def mf_series(h, l, p):
    """-> (bull[], bear[], m[], m_prev[], fields[]). bull/bear = Signal auf der Kerze, auf der der Wert die 0 kreuzt (Pine fieldBuy/fieldSell).
    fields = [{"kind": "long"/"short", "start": Feldbeginn (rueckwirkend), "cross": Kerze des Signals}] in zeitlicher Reihenfolge."""
    m, mp = mf_momentum(h, l, p["mom"], p["smooth"])
    n = len(m)
    up = [m[i] >= mp[i] for i in range(n)]
    bull, bear, fields = [False] * n, [False] * n, []
    regime = reg_start = run_start = 0
    have_box = False
    for i in range(n):
        if i > 0 and up[i] != up[i - 1]:
            run_start = i
        sgn = 1 if m[i] > 0 else -1 if m[i] < 0 else 0
        if sgn != 0 and sgn != regime:
            b = run_start
            if have_box:
                b = max(b, reg_start + 1)
            fields.append({"kind": "long" if sgn > 0 else "short", "start": b, "cross": i})
            have_box = True
            regime, reg_start = sgn, b
            if sgn > 0:
                bull[i] = True
            else:
                bear[i] = True
    if p.get("entry") == "color":
        # Fruehes Signal: sobald die Kerzenfarbe (Momentum steigt/faellt) GEGEN das aktuelle Vorzeichen kippt - Wert noch > 0 und faellt -> SELL,
        # Wert noch < 0 und steigt -> BUY. Das ist der Moment, in dem das Feld im Chart "rot/gruen wird"; der Nulldurchgang folgt spaeter.
        # Faellt ein Nulldurchgang auf dieselbe Kerze, gilt der Nulldurchgang.
        for i in range(1, n):
            if up[i] != up[i - 1] and not bull[i] and not bear[i]:
                if not up[i] and m[i] > 0:
                    bear[i] = True
                elif up[i] and m[i] < 0:
                    bull[i] = True
    return bull, bear, m, mp, fields


def mf_compute(o, h, l, c, p):
    """-> (bull[], bear[], xdn[], xup[], m, m_prev) - gleiche Form wie bei Momentum-Feld (xdn/xup gibt es hier nicht)."""
    bull, bear, m, mp, _f = mf_series(h, l, p)
    n = len(c)
    return bull, bear, [False] * n, [False] * n, m, mp


def _signal_exit(pos, xdn_i, xup_i, p):
    return None   # keine eigenen Signal-Ausstiege ausser dem Gegensignal (Flip)


def _mf_levels(direction, entry, size, p):
    if not size or size <= 0:
        return None, None
    sgn = 1 if direction == "long" else -1
    sl = entry - sgn * p["sl_usd"] / size if p["sl_usd"] > 0 else None
    tp = entry + sgn * p["tp_usd"] / size if (p["tp_usd"] > 0 and p["tp_mode"] != "partial") else None
    return sl, tp


def _mf_ladder(direction, entry, size, p):
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


def _mf_reset_state(st):
    st["mf_sl_price"] = None
    st["mf_tp_price"] = None
    st["mf_tps"] = []
    st["mf_tp_pcts"] = []
    st["mf_tp_done"] = 0
    st["mf_be"] = False
    st["mf_entries"] = 0
    st["mf_last_entry"] = None


async def check_mf_sl_tp(symbol, price):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    pos = st["position"]
    if pos is None or price is None:
        return
    p = _mf_params(cfg)
    sl, tp = st.get("mf_sl_price"), st.get("mf_tp_price")
    if sl is not None and ((pos == "long" and price <= sl) or (pos == "short" and price >= sl)):
        prot = st.get("mf_be")
        reason = "SL-TP1" if prot == "tp1" else "BREAKEVEN" if prot else "SL"
        debug_log(f"🚪 [{symbol}] Momentum-Feld {reason}: {pos.upper()} @ {price} (Ziel war {round(sl, 6)})")
        await execute_exit(symbol, price, reason)
        if st["position"] is None:
            if not prot:
                st["mf_sl_cooldown_until"] = time.time() + cfg.get("mf_sl_cooldown_seconds", 30)
            _mf_reset_state(st)
        return
    tps = st.get("mf_tps") or []
    if tps:
        pcts = st.get("mf_tp_pcts") or []
        done = int(st.get("mf_tp_done") or 0)
        while done < len(tps) and ((pos == "long" and price >= tps[done]) or (pos == "short" and price <= tps[done])):
            pct = pcts[done] if done < len(pcts) else 100.0
            if done == len(tps) - 1 or pct >= 99.5:
                debug_log(f"🎯 [{symbol}] Momentum-Feld TP{done + 1} (Rest): {pos.upper()} @ {price}")
                await execute_exit(symbol, price, f"TP{done + 1}")
                if st["position"] is None:
                    _mf_reset_state(st)
                return
            if not await execute_partial_exit(symbol, price, pct / 100.0, f"TP{done + 1}"):
                return
            done += 1
            st["mf_tp_done"] = done
            debug_log(f"📡 [{symbol}] Momentum-Feld TP{done} erreicht - {pct:g}% geschlossen")
            if done == 1 and p["be"] and st.get("avg_entry_price"):
                st["mf_sl_price"], st["mf_be"] = st["avg_entry_price"], "be"
                debug_log(f"📡 [{symbol}] Momentum-Feld TP1 erreicht - SL auf Break-Even")
            elif done == 2 and p["sl_to_tp1"]:
                st["mf_sl_price"], st["mf_be"] = tps[0], "tp1"
                debug_log(f"📡 [{symbol}] Momentum-Feld TP2 erreicht - SL auf TP1")
        return
    if tp is not None and ((pos == "long" and price >= tp) or (pos == "short" and price <= tp)):
        debug_log(f"🎯 [{symbol}] Momentum-Feld TP: {pos.upper()} @ {price} (Ziel war {round(tp, 6)})")
        await execute_exit(symbol, price, "TP")
        if st["position"] is None:
            _mf_reset_state(st)


def _set_levels(st, cfg, target, price):
    entry = st.get("avg_entry_price") or price
    size = st.get("total_coin_size") or 0
    p = _mf_params(cfg)
    st["mf_sl_price"], st["mf_tp_price"] = _mf_levels(target, entry, size, p)
    lad = _mf_ladder(target, entry, size, p)
    st["mf_tps"], st["mf_tp_pcts"], st["mf_tp_done"], st["mf_be"] = [x[0] for x in lad], [x[1] for x in lad], 0, False
    st["mf_entries"] = 1
    st["mf_last_entry"] = price


def _refresh_levels(st, cfg, target, price):
    """Nach einem Nachkauf: SL/TP neu an Ø-Einstieg und Gesamtgroesse haengen - aber nur, solange noch kein TP erreicht / kein Break-Even gesetzt ist."""
    if int(st.get("mf_tp_done") or 0) > 0 or st.get("mf_be"):
        return
    entry = st.get("avg_entry_price") or price
    size = st.get("total_coin_size") or 0
    p = _mf_params(cfg)
    st["mf_sl_price"], st["mf_tp_price"] = _mf_levels(target, entry, size, p)
    lad = _mf_ladder(target, entry, size, p)
    st["mf_tps"], st["mf_tp_pcts"] = [x[0] for x in lad], [x[1] for x in lad]


def _dist_ok(direction, last, price, p):
    """Mindestabstand fuer einen Nachkauf erfuellt? (Prozent vom letzten Einstiegspreis, Richtung laut mf_add_dist_dir)"""
    if p["add_dist_pct"] <= 0 or not last:
        return True
    move = (price - last) / last * 100.0 * (1 if direction == "long" else -1)   # + = in Gewinnrichtung
    if p["add_dist_dir"] == "against":
        return -move >= p["add_dist_pct"]
    if p["add_dist_dir"] == "with":
        return move >= p["add_dist_pct"]
    return abs(move) >= p["add_dist_pct"]


def _in_profit(st, pos, price):
    avg, size = st.get("avg_entry_price"), st.get("total_coin_size") or 0
    if not avg or size <= 0:
        return True
    return ((price - avg) if pos == "long" else (avg - price)) * size > 0


async def check_mf_signal(symbol, bull_i, bear_i, xdn_i, xup_i, price):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or price is None:
        return
    p = _mf_params(cfg)
    pos = st["position"]
    tri = "long" if bull_i else "short" if bear_i else None
    if bull_i and bear_i:
        tri = None
    if pos is None:
        if tri is None or not _allowed(tri, p) or time.time() < st.get("mf_sl_cooldown_until", 0.0):
            return
        debug_log(f"📡 [{symbol}] Momentum-Feld Einstieg: {'BUY' if tri == 'long' else 'SELL'} ({tri.upper()}) @ {price}")
        await execute_entry(symbol, tri, price, is_add_on=False)
        if st["position"] is not None:
            _mf_reset_state(st)
            _set_levels(st, cfg, tri, price)
        return
    if tri is not None and tri == pos:   # weiteres Signal in dieselbe Richtung -> Nachkauf (wenn erlaubt)
        n_ent = int(st.get("mf_entries") or 1)
        if p["max_entries"] > 1 and n_ent < p["max_entries"] and _allowed(tri, p):
            last = st.get("mf_last_entry") or st.get("avg_entry_price")
            if not _dist_ok(tri, last, price, p):
                debug_log(f"⏭️ [{symbol}] Momentum-Feld Nachkauf übersprungen - Abstand zum letzten Einstieg ({last}) kleiner als {p['add_dist_pct']}% ({p['add_dist_dir']})")
                return
            before = st.get("total_coin_size") or 0
            debug_log(f"➕ [{symbol}] Momentum-Feld Nachkauf #{n_ent + 1}/{p['max_entries']}: {tri.upper()} @ {price}")
            await execute_entry(symbol, tri, price, is_add_on=True)
            if (st.get("total_coin_size") or 0) > before:
                st["mf_entries"] = n_ent + 1
                st["mf_last_entry"] = price
                _refresh_levels(st, cfg, tri, price)
        return
    if p["exit_mode"] == "sltp_only":
        return
    if tri is not None and tri != pos:
        if p["profit_only"] and not _in_profit(st, pos, price):
            debug_log(f"⏸️ [{symbol}] Momentum-Feld Gegensignal {tri.upper()} ignoriert - {pos.upper()} ist im Minus (nur im Plus schließen)")
            return
        if _allowed(tri, p):
            debug_log(f"🔄 [{symbol}] Momentum-Feld Flip: {pos.upper()} -> {tri.upper()} @ {price}")
            if await execute_reverse(symbol, price, "MF-FLIP", size_multiplier=1.0):
                _mf_reset_state(st)
                _set_levels(st, cfg, tri, price)
        else:
            debug_log(f"🚪 [{symbol}] Momentum-Feld Gegensignal (Richtung gesperrt): {pos.upper()} schließen @ {price}")
            await execute_exit(symbol, price, "MF-EXIT-DIR")
            if st["position"] is None:
                _mf_reset_state(st)
        return
    reason = _signal_exit(pos, xdn_i, xup_i, p)
    if reason:
        if p["profit_only"] and not _in_profit(st, pos, price):
            debug_log(f"⏸️ [{symbol}] Momentum-Feld Cross-Ausstieg ignoriert - {pos.upper()} ist im Minus (nur im Plus schließen)")
            return
        debug_log(f"🎯 [{symbol}] Momentum-Feld {reason}: {pos.upper()} beendet @ {price}")
        await execute_exit(symbol, price, reason)
        if st["position"] is None:
            _mf_reset_state(st)


async def mf_poll_loop(symbol):
    """Momentum-Feld live: wertet NUR abgeschlossene Kerzen aus; SL/TP laufend gegen den Live-Preis."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0
    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "mom_field" and cfg["bot_active"] and b["state"].get("session_started"):
                resolution = cfg.get("mf_resolution", "15m")
                p = _mf_params(cfg)
                res_s = _res_seconds(resolution)
                min_needed = _mf_warm(p) + 2
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
                    await check_mf_sl_tp(symbol, st["last_price"])

                if closed_ts and len(closed_c) > min_needed:
                    age = (now * 1000 - closed_ts[-1]) / 1000
                    if age > 300 + res_s:
                        debug_log(f"⚠️ [{symbol}] Momentum-Feld: letzte Kerze wirkt veraltet ({round(age)}s alt, Auflösung {resolution}) - überspringe Durchlauf.")
                        closed_ts = None
                    elif st["last_price"] is not None and closed_c[-1] and abs(closed_c[-1] - st["last_price"]) / st["last_price"] * 100 > 2.0:
                        debug_log(f"⚠️ [{symbol}] Momentum-Feld: Kerzendaten unplausibel (Kerze {closed_c[-1]} vs. Live {st['last_price']}) - überspringe Durchlauf.")
                        closed_ts = None

                if closed_ts and len(closed_c) > min_needed and (last_processed_ts is None or closed_ts[-1] > last_processed_ts):
                    bull, bear, xdn, xup, wt1, wt2 = await asyncio.to_thread(mf_compute, closed_o, closed_h, closed_l, closed_c, p)
                    if due_heartbeat:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] Momentum-Feld aktiv: {resolution}, Momentum {p['mom']}/{p['smooth']}, "
                                  f"Ausstieg={p['exit_mode']}, "
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
                        await check_mf_signal(symbol, bull[idx], bear[idx], xdn[idx], xup[idx], price_i)
                elif due_heartbeat and not closed_ts:
                    last_heartbeat = now
                    debug_log(f"⏳ [{symbol}] Momentum-Feld wartet: keine Kerzen erhalten (Auflösung {resolution})")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Momentum-Feld-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        await asyncio.sleep(_poll_delay(b["config"].get("mf_resolution", "15m"), b["state"]))


def _mf_simulate(candles, cfg, bull, bear, xdn, xup, start_i, p):
    """Backtest-Gegenstueck zu check_mf_signal / check_mf_sl_tp: Einstieg/Flip zum Schlusskurs der Bestaetigungs-Kerze, SL/TP ab der
    naechsten Kerze ueber Hoch/Tief (SL gewinnt bei Gleichzeitigkeit), Cooldown nach SL, Gegen-Divergenz vor WT-Kreuzung."""
    ts, h, l, c = candles[0], candles[2], candles[3], candles[4]
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    cooldown_ms = cfg.get("mf_sl_cooldown_seconds", 30) * 1000
    position, trades, cool_until = None, [], None

    def open_pos(direction, price, i):
        size = (margin * leverage) / price
        sl, tp = _mf_levels(direction, price, size, p)
        return {"dir": direction, "entry": price, "size": size, "entry_i": i, "sl": sl, "tp": tp,
                "tps": _mf_ladder(direction, price, size, p), "done": 0, "prot": False, "entries": 1, "last": price}

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
        d = position["dir"]
        if tri == d:   # Nachkauf
            if p["max_entries"] > 1 and position["entries"] < p["max_entries"] and _allowed(tri, p) and _dist_ok(d, position["last"], price, p):
                position["last"] = price
                add = (margin * leverage) / price
                tot = position["size"] + add
                position["entry"] = (position["entry"] * position["size"] + price * add) / tot
                position["size"] = tot
                position["entries"] += 1
                if position["done"] == 0 and not position["prot"]:
                    position["sl"], position["tp"] = _mf_levels(d, position["entry"], tot, p)
                    position["tps"] = _mf_ladder(d, position["entry"], tot, p)
            continue
        if p["exit_mode"] == "sltp_only":
            continue
        pnl_now = ((price - position["entry"]) if d == "long" else (position["entry"] - price)) * position["size"]
        if tri is not None and tri != d:
            if p["profit_only"] and pnl_now <= 0:
                continue
            ok = _allowed(tri, p)
            _bt_close_trade(trades, d, position["entry"], price, position["size"], i, position["entry_i"], "MF-FLIP" if ok else "MF-EXIT-DIR", ts=ts)
            position = open_pos(tri, price, i) if ok else None
        else:
            reason = _signal_exit(d, xdn[i], xup[i], p)
            if reason and not (p["profit_only"] and pnl_now <= 0):
                _bt_close_trade(trades, d, position["entry"], price, position["size"], i, position["entry_i"], reason, ts=ts)
                position = None
    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)
    return trades


def _lead_days(p, res_s):
    return _mf_warm(p) * res_s / 86400 + 0.1


def _window_start(ts, days):
    from bisect import bisect_left
    return bisect_left(ts, ts[-1] - int(days * 86_400_000))


async def run_mf_backtest(symbol, cfg, days, exclude_top_n=1):
    max_candles = BACKTEST_MAX_CANDLES.get("mom_field", 100_000)
    resolution = cfg.get("mf_resolution", "15m")
    if resolution in SUB_MINUTE_RESOLUTIONS:
        max_candles = min(max_candles, 5000)
    p = _mf_params(cfg)
    res_s = _res_seconds(resolution)
    lead = _lead_days(p, res_s) if resolution not in SUB_MINUTE_RESOLUTIONS else 0.0
    t_fetch = time.time()
    candles, err, src = await _we_candles(symbol, resolution, days + lead, max_candles, market_type=cfg.get("binance_market_type", "spot"))
    t_fetch = time.time() - t_fetch
    if err:
        return {"error": err}
    warm = _mf_warm(p)
    if not candles or len(candles[4]) < warm + 100:
        return {"error": f"Zu wenig historische Kerzen für einen aussagekräftigen Backtest erhalten (mind. ~{warm + 100} nötig)."}
    ts, o, h, l, c = candles[0], candles[1], candles[2], candles[3], candles[4]
    t_calc = time.time()
    bull, bear, xdn, xup, wt1, wt2 = await asyncio.to_thread(mf_compute, o, h, l, c, p)
    start_i = max(warm, _window_start(ts, days))
    trades = _mf_simulate(candles, cfg, bull, bear, xdn, xup, start_i, p)
    t_calc = time.time() - t_calc
    stats = summarize_backtest_trades(trades, exclude_top_n)
    usable_days = (ts[-1] - ts[start_i]) / 86_400_000 if start_i < len(ts) else 0
    return {
        "symbol": symbol, "entry_mode": "mom_field", "resolution": resolution,
        "requested_days": days, "actual_days_covered": round(usable_days, 1),
        "candles_processed": len(ts) - start_i, "candle_cap": max_candles, "cache_used": src != "neu",
        "note": f"Kerzen holen {t_fetch:.1f}s ({ {'cache': 'Cache', 'abgeleitet': 'aus feinerem Zeitrahmen berechnet', 'neu': 'neu von Binance'}[src] }), Berechnung {t_calc:.1f}s. "
                f"Getestet wird das Fenster der letzten {round(usable_days, 1)} Tage (davor läuft das Momentum ein); Ausstieg: {p['exit_mode']}.",
        "signals_buy": sum(bull[start_i:]), "signals_sell": sum(bear[start_i:]),
        "stats": stats,
        "stats_long": summarize_backtest_trades([t for t in trades if t["dir"] == "long"], exclude_top_n),
        "stats_short": summarize_backtest_trades([t for t in trades if t["dir"] == "short"], exclude_top_n),
        "trades": trades,
    }

# ============================================================================
# CHART
# ============================================================================
def mf_chart_payload(ts, o, h, l, c, cfg, max_bars=1000):
    """Kerzen + Momentum-Linie + Felder (roter/gruener Hintergrund) + BUY/SELL-Marker. Nur geschlossene Kerzen werden gerechnet; das aktuelle Feld
    reicht bis zur laufenden Kerze. BUY/SELL sitzen auf dem (rueckwirkenden) Feldbeginn wie im Pine - der Bot steigt bei 'cross' ein."""
    p = _mf_params(cfg)
    n = len(c)
    if n < 30:
        return None
    bull, bear, m, mp, fields = mf_series(h[:-1], l[:-1], p)
    off = max(0, n - max_bars)
    candles = [{"time": ts[i] // 1000, "open": o[i], "high": h[i], "low": l[i], "close": c[i]} for i in range(off, n)]
    markers, bg = [], []
    for k, f in enumerate(fields):
        end = fields[k + 1]["start"] - 1 if k + 1 < len(fields) else n - 1
        if end >= off:
            bg.append({"t1": ts[max(f["start"], off)] // 1000, "t2": ts[end] // 1000, "kind": f["kind"]})
        if f["start"] >= off:
            markers.append({"time": ts[f["start"]] // 1000, "kind": f["kind"], "price": c[f["start"]],
                            "text": "BUY" if f["kind"] == "long" else "SELL", "entry_time": ts[f["cross"]] // 1000})
    wt1 = [{"time": ts[i] // 1000, "value": round(m[i], 2)} for i in range(off, n - 1)]
    wt2 = [{"time": ts[i] // 1000, "value": round(mp[i], 2)} for i in range(off, n - 1)]
    return {"candles": candles, "wt1": wt1, "wt2": [], "markers": markers, "levels": [], "bg_fields": bg, "entry_mode": "mf",
            "wt1v": round(m[-1], 2), "wt2v": round(mp[-1], 2), "bars": n}
