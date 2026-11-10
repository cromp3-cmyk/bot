"""
pivot.py - eigene Strategie "Pivot Reversal" (entry_mode "pivot_reversal", Config-Praefix pv_*).

Nachbau des Pine-Skripts "ACW PIVOT REVERSAL v3". Signal auf GESCHLOSSENEN Kerzen:

  Pivot-Hoch / Pivot-Tief = pivothigh/pivotlow(links, rechts), Standard 1/1 (ein Pivot ist erst 'rechts' Kerzen spaeter bestaetigt)
  hprice / lprice          = letztes Pivot-Hoch / -Tief
  LONG   : das Hoch einer Kerze kreuzt (von unten) das letzte Pivot-Hoch (+ Puffer)
  SHORT  : das Tief einer Kerze kreuzt (von oben) das letzte Pivot-Tief (- Puffer)
  Wechsel: Signale kommen nur abwechselnd (nach LONG erst wieder SHORT und umgekehrt) - wie last_signal im Pine.

Pine zeigt das Signal schon, sobald der Kurs das Level IRGENDWANN in der Kerze beruehrt. Der Bot wertet nur fertige Kerzen aus und steigt
zum Schlusskurs der Signalkerze ein (kein Repaint). pv_exit_mode: flip = Gegensignal dreht die Position (Standard), sltp_only = nur SL/TP.
SL/TP in $, Break-Even bei Gewinn ($), Teil-TPs, Mehrfach-Einstiege, Richtungswahl und Cooldown nach SL wie bei den anderen Flip-Strategien.
"Puffer %" ersetzt syminfo.mintick des Pine (0 = Level selbst).
"""

import asyncio
import random
import time
import traceback

from bot_core import BOTS, debug_log, execute_entry, execute_exit, execute_reverse, execute_partial_exit
from strategies import (
    SUB_MINUTE_RESOLUTIONS, get_seconds_candles, fetch_candles_binance_multi,
    summarize_backtest_trades, BACKTEST_MAX_CANDLES, _bt_close_trade,
)
from wae_ema import _we_candles, _res_seconds, _poll_delay

EXIT_MODES = ("flip", "sltp_only")


def _fl(cfg, key, default):
    v = cfg.get(key, default)
    return float(default if v in (None, "") else v)


def _pv_params(cfg):
    em = cfg.get("pv_exit_mode", "flip")
    mode = cfg.get("pv_direction_mode", "both")
    return {
        "left": max(1, int(_fl(cfg, "pv_left", 1))), "right": max(1, int(_fl(cfg, "pv_right", 1))),
        "buffer": max(0.0, _fl(cfg, "pv_buffer_pct", 0.0)),
        "candle": cfg.get("pv_candle_type", "normal") if cfg.get("pv_candle_type", "normal") in ("normal", "heikinashi", "renko") else "normal",
        "renko_mode": "pct" if cfg.get("pv_renko_mode", "atr") == "pct" else "atr",
        "renko_atr": max(1, int(_fl(cfg, "pv_renko_atr_len", 14))), "renko_mult": max(0.05, _fl(cfg, "pv_renko_mult", 1.0)),
        "renko_pct": max(0.001, _fl(cfg, "pv_renko_pct", 0.1)),
        "exit_mode": em if em in EXIT_MODES else "flip",
        "direction": mode if mode in ("both", "long_only", "short_only") else "both",
        "sl_usd": max(0.0, _fl(cfg, "pv_sl_usd", 0.0)), "tp_usd": max(0.0, _fl(cfg, "pv_tp_usd", 0.0)),
        "max_entries": max(1, min(20, int(_fl(cfg, "pv_max_entries", 1)))),
        "profit_only": bool(cfg.get("pv_profit_only_close", False)),
        "add_dist_pct": max(0.0, _fl(cfg, "pv_add_dist_pct", 0)),
        "add_dist_dir": cfg.get("pv_add_dist_dir", "any") if cfg.get("pv_add_dist_dir", "any") in ("any", "against", "with") else "any",
        "tp_mode": "partial" if cfg.get("pv_tp_mode", "single") == "partial" else "single",
        "tp1_usd": max(0.0, _fl(cfg, "pv_tp1_usd", 5.0)), "tp2_usd": max(0.0, _fl(cfg, "pv_tp2_usd", 10.0)), "tp3_usd": max(0.0, _fl(cfg, "pv_tp3_usd", 15.0)),
        "tp1_pct": max(1.0, min(100.0, _fl(cfg, "pv_tp1_pct", 33))), "tp2_pct": max(1.0, min(100.0, _fl(cfg, "pv_tp2_pct", 33))),
        "be_usd": max(0.0, _fl(cfg, "pv_be_trigger_usd", 0.0)),
        "be": bool(cfg.get("pv_be_after_tp1", True)), "sl_to_tp1": bool(cfg.get("pv_sl_to_tp1_on_tp2", True)),
    }


def _pv_warm(p):
    """Kerzen Einlaufzeit: bis die ersten Pivots stehen."""
    return (p["left"] + p["right"]) * 3 + 50 + (p["renko_atr"] * 2 if p["candle"] == "renko" else 0)


# ============================================================================
# RECHNUNG (rein, ohne Netzwerk)
# ============================================================================
def heikin_ashi(o, h, l, c):
    """Heikin-Ashi: c = (o+h+l+c)/4, o = Mitte der Vor-HA-Kerze (erste: (o+c)/2), h/l = Extreme aus echtem Hoch/Tief und HA-o/c."""
    n = len(c)
    ho, hh, hl, hc = [0.0] * n, [0.0] * n, [0.0] * n, [0.0] * n
    for i in range(n):
        hc[i] = (o[i] + h[i] + l[i] + c[i]) / 4.0
        ho[i] = (o[0] + c[0]) / 2.0 if i == 0 else (ho[i - 1] + hc[i - 1]) / 2.0
        hh[i] = max(h[i], ho[i], hc[i])
        hl[i] = min(l[i], ho[i], hc[i])
    return ho, hh, hl, hc


def _atr(h, l, c, n):
    """Pine ta.atr = RMA(TR): bis Kerze n-1 None, dort der Durchschnitt der ersten n TR, danach alpha = 1/n."""
    out, rma, tr = [], None, []
    for i in range(len(c)):
        t = h[i] - l[i] if i == 0 else max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1]))
        tr.append(t)
        if i < n - 1:
            out.append(None)
        elif i == n - 1:
            rma = sum(tr) / n
            out.append(rma)
        else:
            rma = (rma * (n - 1) + t) / n
            out.append(rma)
    return out


def renko_bricks(o, h, l, c, p):
    """Renko aus Schlusskursen (wie TradingView 'Close'): -> (bo[], bh[], bl[], bc[], src[]). src[k] = Index der echten Kerze, in der Ziegel k fertig wurde.
    Ziegelgroesse: 'atr' = ATR(len) x Faktor (zum Zeitpunkt der Kerze), 'pct' = Prozent vom Schlusskurs. Fortsetzung = 1 Ziegel, Umkehr = 2 Ziegel (wie ueblich:
    erst wenn der Kurs den OPEN des letzten Ziegels um eine Ziegelgroesse unterschreitet/ueberschreitet). Kein Blick in die Zukunft."""
    n = len(c)
    atr = _atr(h, l, c, p["renko_atr"]) if p["renko_mode"] == "atr" else None
    bo, bh, bl, bc, src = [], [], [], [], []
    last_o = last_c = None
    d = 0
    for i in range(n):
        if p["renko_mode"] == "atr":
            if atr[i] is None:
                continue
            sz = atr[i] * p["renko_mult"]
        else:
            sz = c[i] * p["renko_pct"] / 100.0
        if sz <= 0:
            continue
        if last_c is None:
            last_o = last_c = c[i]
            continue
        while True:
            if d == 0 and c[i] >= last_c + sz:
                nb_o, nb_c, nd = last_c, last_c + sz, 1
            elif d == 0 and c[i] <= last_c - sz:
                nb_o, nb_c, nd = last_c, last_c - sz, -1
            elif d == 1 and c[i] >= last_c + sz:
                nb_o, nb_c, nd = last_c, last_c + sz, 1
            elif d == 1 and c[i] <= last_o - sz:
                nb_o, nb_c, nd = last_o, last_o - sz, -1
            elif d == -1 and c[i] <= last_c - sz:
                nb_o, nb_c, nd = last_c, last_c - sz, -1
            elif d == -1 and c[i] >= last_o + sz:
                nb_o, nb_c, nd = last_o, last_o + sz, 1
            else:
                break
            bo.append(nb_o); bc.append(nb_c); bh.append(max(nb_o, nb_c)); bl.append(min(nb_o, nb_c)); src.append(i)
            last_o, last_c, d = nb_o, nb_c, nd
    return bo, bh, bl, bc, src


def _pv_core(h, l, p):
    """Pine-Logik auf einer (ggf. umgerechneten) Kerzenreihe -> (bull[], bear[], state[], hprice[], lprice[])."""
    n = len(h)
    L, R, buf = p["left"], p["right"], p["buffer"] / 100.0
    bull, bear, state, hp_l, lp_l = [False] * n, [False] * n, [0] * n, [0.0] * n, [0.0] * n
    hpr = lpr = 0.0
    last = 0
    prev_hl = prev_ll = None   # Levels der Vorkerze (hprice + Puffer, lprice - Puffer)
    for j in range(n):
        i = j - R
        if i >= L:
            if h[i] > max(h[i - L:i]) and h[i] >= max(h[i + 1:i + R + 1]):
                hpr = h[i]
            if l[i] < min(l[i - L:i]) and l[i] <= min(l[i + 1:i + R + 1]):
                lpr = l[i]
        hl, ll = hpr * (1 + buf), lpr * (1 - buf)
        long_ = j > 0 and prev_hl is not None and h[j] > hl and h[j - 1] <= prev_hl      # crossover(high, hprice + tick)
        short_ = j > 0 and prev_ll is not None and l[j] < ll and l[j - 1] >= prev_ll     # crossunder(low, lprice - tick)
        long_final = long_ and last in (0, -1)
        short_final = short_ and last in (0, 1)
        last = 1 if long_final else -1 if short_final else last
        bull[j], bear[j], state[j], hp_l[j], lp_l[j] = long_final, short_final, last, hpr, lpr
        prev_hl, prev_ll = hl, ll
    return bull, bear, state, hp_l, lp_l


def pv_display(o, h, l, c, p):
    """Kerzen, auf denen die Signale berechnet werden (Heikin-Ashi: gleiche Anzahl wie echt; sonst die echten)."""
    return heikin_ashi(o, h, l, c) if p["candle"] == "heikinashi" else (o, h, l, c)


def pv_series(o, h, l, c, p):
    """-> (bull[], bear[], state[], hprice[], lprice[]), immer auf die ECHTEN Kerzen abgebildet (Signal = Schluss der echten Kerze, in der das Signal entsteht).
    normal/Heikin-Ashi: Signale auf den (umgerechneten) Kerzen; Renko: Signale auf den Ziegeln, abgebildet auf die Kerze, die den Ziegel fertigstellt."""
    n = len(c)
    if p["candle"] != "renko":
        so, sh, sl_, sc = pv_display(o, h, l, c, p)
        return _pv_core(sh, sl_, p)
    bo, bh, bl, bc, src = renko_bricks(o, h, l, c, p)
    bull_b, bear_b, state_b, hp_b, lp_b = _pv_core(bh, bl, p)
    bull, bear, state, hp_l, lp_l = [False] * n, [False] * n, [0] * n, [0.0] * n, [0.0] * n
    idx_last = {}
    for k, i in enumerate(src):
        idx_last[i] = k
        if bull_b[k]:
            bull[i], bear[i] = True, False   # mehrere Ziegel in einer Kerze: der spaetere Wechsel gewinnt
        if bear_b[k]:
            bear[i], bull[i] = True, False
    cur_s, cur_h, cur_l = 0, 0.0, 0.0
    for i in range(n):
        if i in idx_last:
            k = idx_last[i]
            cur_s, cur_h, cur_l = state_b[k], hp_b[k], lp_b[k]
        state[i], hp_l[i], lp_l[i] = cur_s, cur_h, cur_l
    return bull, bear, state, hp_l, lp_l


def pv_compute(o, h, l, c, p):
    """-> (bull[], bear[], xdn[], xup[], hprice, lprice) - gleiche Form wie bei den anderen Flip-Strategien (xdn/xup gibt es hier nicht)."""
    bull, bear, _st, hp, lp = pv_series(o, h, l, c, p)
    n = len(c)
    return bull, bear, [False] * n, [False] * n, hp, lp


def _signal_exit(pos, xdn_i, xup_i, p):
    return None   # keine eigenen Signal-Ausstiege ausser dem Gegensignal (Flip)


def _pv_levels(direction, entry, size, p):
    if not size or size <= 0:
        return None, None
    sgn = 1 if direction == "long" else -1
    sl = entry - sgn * p["sl_usd"] / size if p["sl_usd"] > 0 else None
    tp = entry + sgn * p["tp_usd"] / size if (p["tp_usd"] > 0 and p["tp_mode"] != "partial") else None
    return sl, tp


def _pv_ladder(direction, entry, size, p):
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


def _pv_reset_state(st):
    st["pv_sl_price"] = None
    st["pv_tp_price"] = None
    st["pv_tps"] = []
    st["pv_tp_pcts"] = []
    st["pv_tp_done"] = 0
    st["pv_be"] = False
    st["pv_entries"] = 0
    st["pv_last_entry"] = None


async def check_pv_sl_tp(symbol, price):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    pos = st["position"]
    if pos is None or price is None:
        return
    p = _pv_params(cfg)
    sl, tp = st.get("pv_sl_price"), st.get("pv_tp_price")
    if sl is not None and ((pos == "long" and price <= sl) or (pos == "short" and price >= sl)):
        prot = st.get("pv_be")
        reason = "SL-TP1" if prot == "tp1" else "BREAKEVEN" if prot else "SL"
        debug_log(f"🚪 [{symbol}] Pivot Reversal {reason}: {pos.upper()} @ {price} (Ziel war {round(sl, 6)})")
        await execute_exit(symbol, price, reason)
        if st["position"] is None:
            if not prot:
                st["pv_sl_cooldown_until"] = time.time() + cfg.get("pv_sl_cooldown_seconds", 30)
            _pv_reset_state(st)
        return
    if p["be_usd"] > 0 and not st.get("pv_be") and st.get("avg_entry_price") and (st.get("total_coin_size") or 0) > 0:
        entry_, size_ = st["avg_entry_price"], st["total_coin_size"]
        pnl_ = ((price - entry_) if pos == "long" else (entry_ - price)) * size_
        if pnl_ >= p["be_usd"]:
            st["pv_sl_price"], st["pv_be"] = entry_, "be"
            debug_log(f"📡 [{symbol}] Pivot Reversal: {round(pnl_, 2)} $ Gewinn erreicht (Ziel {p['be_usd']:g} $) - SL auf Einstieg {round(entry_, 6)}")
    tps = st.get("pv_tps") or []
    if tps:
        pcts = st.get("pv_tp_pcts") or []
        done = int(st.get("pv_tp_done") or 0)
        while done < len(tps) and ((pos == "long" and price >= tps[done]) or (pos == "short" and price <= tps[done])):
            pct = pcts[done] if done < len(pcts) else 100.0
            if done == len(tps) - 1 or pct >= 99.5:
                debug_log(f"🎯 [{symbol}] Pivot Reversal TP{done + 1} (Rest): {pos.upper()} @ {price}")
                await execute_exit(symbol, price, f"TP{done + 1}")
                if st["position"] is None:
                    _pv_reset_state(st)
                return
            if not await execute_partial_exit(symbol, price, pct / 100.0, f"TP{done + 1}"):
                return
            done += 1
            st["pv_tp_done"] = done
            debug_log(f"📡 [{symbol}] Pivot Reversal TP{done} erreicht - {pct:g}% geschlossen")
            if done == 1 and p["be"] and st.get("avg_entry_price"):
                st["pv_sl_price"], st["pv_be"] = st["avg_entry_price"], "be"
                debug_log(f"📡 [{symbol}] Pivot Reversal TP1 erreicht - SL auf Break-Even")
            elif done == 2 and p["sl_to_tp1"]:
                st["pv_sl_price"], st["pv_be"] = tps[0], "tp1"
                debug_log(f"📡 [{symbol}] Pivot Reversal TP2 erreicht - SL auf TP1")
        return
    if tp is not None and ((pos == "long" and price >= tp) or (pos == "short" and price <= tp)):
        debug_log(f"🎯 [{symbol}] Pivot Reversal TP: {pos.upper()} @ {price} (Ziel war {round(tp, 6)})")
        await execute_exit(symbol, price, "TP")
        if st["position"] is None:
            _pv_reset_state(st)


def _set_levels(st, cfg, target, price):
    entry = st.get("avg_entry_price") or price
    size = st.get("total_coin_size") or 0
    p = _pv_params(cfg)
    st["pv_sl_price"], st["pv_tp_price"] = _pv_levels(target, entry, size, p)
    lad = _pv_ladder(target, entry, size, p)
    st["pv_tps"], st["pv_tp_pcts"], st["pv_tp_done"], st["pv_be"] = [x[0] for x in lad], [x[1] for x in lad], 0, False
    st["pv_entries"] = 1
    st["pv_last_entry"] = price


def _refresh_levels(st, cfg, target, price):
    """Nach einem Nachkauf: SL/TP neu an Ø-Einstieg und Gesamtgroesse haengen - aber nur, solange noch kein TP erreicht / kein Break-Even gesetzt ist."""
    if int(st.get("pv_tp_done") or 0) > 0 or st.get("pv_be"):
        return
    entry = st.get("avg_entry_price") or price
    size = st.get("total_coin_size") or 0
    p = _pv_params(cfg)
    st["pv_sl_price"], st["pv_tp_price"] = _pv_levels(target, entry, size, p)
    lad = _pv_ladder(target, entry, size, p)
    st["pv_tps"], st["pv_tp_pcts"] = [x[0] for x in lad], [x[1] for x in lad]


def _dist_ok(direction, last, price, p):
    """Mindestabstand fuer einen Nachkauf erfuellt? (Prozent vom letzten Einstiegspreis, Richtung laut pv_add_dist_dir)"""
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


async def check_pv_signal(symbol, bull_i, bear_i, xdn_i, xup_i, price):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or price is None:
        return
    p = _pv_params(cfg)
    pos = st["position"]
    tri = "long" if bull_i else "short" if bear_i else None
    if bull_i and bear_i:
        tri = None
    if pos is None:
        if tri is None or not _allowed(tri, p) or time.time() < st.get("pv_sl_cooldown_until", 0.0):
            return
        debug_log(f"📡 [{symbol}] Pivot Reversal Einstieg: {'BUY' if tri == 'long' else 'SELL'} ({tri.upper()}) @ {price}")
        await execute_entry(symbol, tri, price, is_add_on=False)
        if st["position"] is not None:
            _pv_reset_state(st)
            _set_levels(st, cfg, tri, price)
        return
    if tri is not None and tri == pos:   # weiteres Signal in dieselbe Richtung -> Nachkauf (wenn erlaubt)
        n_ent = int(st.get("pv_entries") or 1)
        if p["max_entries"] > 1 and n_ent < p["max_entries"] and _allowed(tri, p):
            last = st.get("pv_last_entry") or st.get("avg_entry_price")
            if not _dist_ok(tri, last, price, p):
                debug_log(f"⏭️ [{symbol}] Pivot Reversal Nachkauf übersprungen - Abstand zum letzten Einstieg ({last}) kleiner als {p['add_dist_pct']}% ({p['add_dist_dir']})")
                return
            before = st.get("total_coin_size") or 0
            debug_log(f"➕ [{symbol}] Pivot Reversal Nachkauf #{n_ent + 1}/{p['max_entries']}: {tri.upper()} @ {price}")
            await execute_entry(symbol, tri, price, is_add_on=True)
            if (st.get("total_coin_size") or 0) > before:
                st["pv_entries"] = n_ent + 1
                st["pv_last_entry"] = price
                _refresh_levels(st, cfg, tri, price)
        return
    if p["exit_mode"] == "sltp_only":
        return
    if tri is not None and tri != pos:
        if p["profit_only"] and not _in_profit(st, pos, price):
            debug_log(f"⏸️ [{symbol}] Pivot Reversal Gegensignal {tri.upper()} ignoriert - {pos.upper()} ist im Minus (nur im Plus schließen)")
            return
        if _allowed(tri, p):
            debug_log(f"🔄 [{symbol}] Pivot Reversal Flip: {pos.upper()} -> {tri.upper()} @ {price}")
            if await execute_reverse(symbol, price, "PIV-FLIP", size_multiplier=1.0):
                _pv_reset_state(st)
                _set_levels(st, cfg, tri, price)
        else:
            debug_log(f"🚪 [{symbol}] Pivot Reversal Gegensignal (Richtung gesperrt): {pos.upper()} schließen @ {price}")
            await execute_exit(symbol, price, "PIV-EXIT-DIR")
            if st["position"] is None:
                _pv_reset_state(st)
        return
    reason = _signal_exit(pos, xdn_i, xup_i, p)
    if reason:
        if p["profit_only"] and not _in_profit(st, pos, price):
            debug_log(f"⏸️ [{symbol}] Pivot Reversal Cross-Ausstieg ignoriert - {pos.upper()} ist im Minus (nur im Plus schließen)")
            return
        debug_log(f"🎯 [{symbol}] Pivot Reversal {reason}: {pos.upper()} beendet @ {price}")
        await execute_exit(symbol, price, reason)
        if st["position"] is None:
            _pv_reset_state(st)


async def pv_poll_loop(symbol):
    """Pivot Reversal live: wertet NUR abgeschlossene Kerzen aus; SL/TP laufend gegen den Live-Preis."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0
    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "pivot_reversal" and cfg["bot_active"] and b["state"].get("session_started"):
                resolution = cfg.get("pv_resolution", "15m")
                p = _pv_params(cfg)
                res_s = _res_seconds(resolution)
                min_needed = _pv_warm(p) + 2
                needed_bars = min(1000, max(min_needed * 2, 500))
                st = b["state"]
                closed_ts = None
                if resolution in SUB_MINUTE_RESOLUTIONS:
                    local = get_seconds_candles(st, SUB_MINUTE_RESOLUTIONS[resolution], needed_bars)
                    if local:
                        closed_ts, closed_o, closed_h, closed_l, closed_c = local
                        # Sekunden-Kerzen: die letzte ist meist noch die LAUFENDE (nicht fertige) - die darf nie ausgewertet werden,
                        # sonst flackert das Momentum und der Bot steigt auf ein Signal ein, das bei Kerzenschluss wieder verschwindet.
                        sec_len = SUB_MINUTE_RESOLUTIONS[resolution]
                        if closed_ts and closed_ts[-1] + sec_len * 1000 > time.time() * 1000:
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
                    await check_pv_sl_tp(symbol, st["last_price"])

                if closed_ts and len(closed_c) > min_needed:
                    age = (now * 1000 - closed_ts[-1]) / 1000
                    if age > 300 + res_s:
                        debug_log(f"⚠️ [{symbol}] Pivot Reversal: letzte Kerze wirkt veraltet ({round(age)}s alt, Auflösung {resolution}) - überspringe Durchlauf.")
                        closed_ts = None
                    elif st["last_price"] is not None and closed_c[-1] and abs(closed_c[-1] - st["last_price"]) / st["last_price"] * 100 > 2.0:
                        debug_log(f"⚠️ [{symbol}] Pivot Reversal: Kerzendaten unplausibel (Kerze {closed_c[-1]} vs. Live {st['last_price']}) - überspringe Durchlauf.")
                        closed_ts = None

                if closed_ts and len(closed_c) > min_needed and (last_processed_ts is None or closed_ts[-1] > last_processed_ts):
                    bull, bear, xdn, xup, wt1, wt2 = await asyncio.to_thread(pv_compute, closed_o, closed_h, closed_l, closed_c, p)
                    if due_heartbeat:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] Pivot Reversal aktiv: {resolution}, Pivot {p['left']}/{p['right']}, Puffer {p['buffer']}%, "
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
                        await check_pv_signal(symbol, bull[idx], bear[idx], xdn[idx], xup[idx], price_i)
                elif due_heartbeat and not closed_ts:
                    last_heartbeat = now
                    debug_log(f"⏳ [{symbol}] Pivot Reversal wartet: keine Kerzen erhalten (Auflösung {resolution})")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Pivot Reversal-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        await asyncio.sleep(_poll_delay(b["config"].get("pv_resolution", "15m"), b["state"]))


def _pv_simulate(candles, cfg, bull, bear, xdn, xup, start_i, p):
    """Backtest-Gegenstueck zu check_pv_signal / check_pv_sl_tp: Einstieg/Flip zum Schlusskurs der Bestaetigungs-Kerze, SL/TP ab der
    naechsten Kerze ueber Hoch/Tief (SL gewinnt bei Gleichzeitigkeit), Cooldown nach SL, Gegen-Divergenz vor WT-Kreuzung."""
    ts, h, l, c = candles[0], candles[2], candles[3], candles[4]
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    cooldown_ms = cfg.get("pv_sl_cooldown_seconds", 30) * 1000
    position, trades, cool_until = None, [], None

    def open_pos(direction, price, i):
        size = (margin * leverage) / price
        sl, tp = _pv_levels(direction, price, size, p)
        return {"dir": direction, "entry": price, "size": size, "entry_i": i, "sl": sl, "tp": tp,
                "tps": _pv_ladder(direction, price, size, p), "done": 0, "prot": False, "entries": 1, "last": price}

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
            if position is not None and p["be_usd"] > 0 and not position["prot"]:
                # Gewinn-Schwelle in $ erreicht (Hoch/Tief der Kerze) -> SL auf Einstieg, gilt ab der naechsten Kerze
                move = p["be_usd"] / position["size"]
                if (d == "long" and h[i] - position["entry"] >= move) or (d == "short" and position["entry"] - l[i] >= move):
                    position["sl"], position["prot"] = position["entry"], "be"
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
                    position["sl"], position["tp"] = _pv_levels(d, position["entry"], tot, p)
                    position["tps"] = _pv_ladder(d, position["entry"], tot, p)
            continue
        if p["exit_mode"] == "sltp_only":
            continue
        pnl_now = ((price - position["entry"]) if d == "long" else (position["entry"] - price)) * position["size"]
        if tri is not None and tri != d:
            if p["profit_only"] and pnl_now <= 0:
                continue
            ok = _allowed(tri, p)
            _bt_close_trade(trades, d, position["entry"], price, position["size"], i, position["entry_i"], "PIV-FLIP" if ok else "PIV-EXIT-DIR", ts=ts)
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
    return _pv_warm(p) * res_s / 86400 + 0.1


def _window_start(ts, days):
    from bisect import bisect_left
    return bisect_left(ts, ts[-1] - int(days * 86_400_000))


async def run_pv_backtest(symbol, cfg, days, exclude_top_n=1):
    max_candles = BACKTEST_MAX_CANDLES.get("pivot_reversal", 100_000)
    resolution = cfg.get("pv_resolution", "15m")
    if resolution in SUB_MINUTE_RESOLUTIONS:
        max_candles = min(max_candles, 5000)
    p = _pv_params(cfg)
    res_s = _res_seconds(resolution)
    lead = _lead_days(p, res_s) if resolution not in SUB_MINUTE_RESOLUTIONS else 0.0
    t_fetch = time.time()
    candles, err, src = await _we_candles(symbol, resolution, days + lead, max_candles, market_type=cfg.get("binance_market_type", "spot"))
    t_fetch = time.time() - t_fetch
    if err:
        return {"error": err}
    warm = _pv_warm(p)
    if not candles or len(candles[4]) < warm + 100:
        return {"error": f"Zu wenig historische Kerzen für einen aussagekräftigen Backtest erhalten (mind. ~{warm + 100} nötig)."}
    ts, o, h, l, c = candles[0], candles[1], candles[2], candles[3], candles[4]
    t_calc = time.time()
    bull, bear, xdn, xup, wt1, wt2 = await asyncio.to_thread(pv_compute, o, h, l, c, p)
    start_i = max(warm, _window_start(ts, days))
    trades = _pv_simulate(candles, cfg, bull, bear, xdn, xup, start_i, p)
    t_calc = time.time() - t_calc
    stats = summarize_backtest_trades(trades, exclude_top_n)
    usable_days = (ts[-1] - ts[start_i]) / 86_400_000 if start_i < len(ts) else 0
    return {
        "symbol": symbol, "entry_mode": "pivot_reversal", "resolution": resolution,
        "requested_days": days, "actual_days_covered": round(usable_days, 1),
        "candles_processed": len(ts) - start_i, "candle_cap": max_candles, "cache_used": src != "neu",
        "note": f"Kerzen holen {t_fetch:.1f}s ({ {'cache': 'Cache', 'abgeleitet': 'aus feinerem Zeitrahmen berechnet', 'neu': 'neu von Binance'}[src] }), Berechnung {t_calc:.1f}s. "
                f"Getestet wird das Fenster der letzten {round(usable_days, 1)} Tage (davor läuft der Trend-Score ein); Ausstieg: {p['exit_mode']}.",
        "signals_buy": sum(bull[start_i:]), "signals_sell": sum(bear[start_i:]),
        "stats": stats,
        "stats_long": summarize_backtest_trades([t for t in trades if t["dir"] == "long"], exclude_top_n),
        "stats_short": summarize_backtest_trades([t for t in trades if t["dir"] == "short"], exclude_top_n),
        "trades": trades,
    }

# ============================================================================
# CHART
# ============================================================================
def pv_chart_payload(ts, o, h, l, c, cfg, max_bars=1000):
    """Kerzen + Hintergrund (gruen = letztes Signal LONG, rot = SHORT) + LONG/SHORT-Marker + die beiden aktuellen Pivot-Level. Nur geschlossene Kerzen."""
    p = _pv_params(cfg)
    n = len(c)
    if n < 30:
        return None
    bull, bear, state, hp, lp = pv_series(o[:-1], h[:-1], l[:-1], c[:-1], p)
    m = n - 1
    off = max(0, n - max_bars)
    do, dh, dl, dc = (heikin_ashi(o, h, l, c) if p["candle"] == "heikinashi" else (o, h, l, c))
    candles = [{"time": ts[i] // 1000, "open": do[i], "high": dh[i], "low": dl[i], "close": dc[i]} for i in range(off, n)]
    markers, bg, cur = [], [], None
    for i in range(off, m):
        if bull[i] and not bear[i]:
            markers.append({"time": ts[i] // 1000, "kind": "long", "price": c[i], "text": "LONG"})
        elif bear[i] and not bull[i]:
            markers.append({"time": ts[i] // 1000, "kind": "short", "price": c[i], "text": "SHORT"})
        k = "long" if state[i] > 0 else "short" if state[i] < 0 else None
        if k is None:
            cur = None
            continue
        if cur is not None and cur["kind"] == k:
            cur["t2"] = ts[i] // 1000
        else:
            cur = {"t1": ts[i] // 1000, "t2": ts[i] // 1000, "kind": k}
            bg.append(cur)
    if bg and m < n:
        bg[-1]["t2"] = ts[n - 1] // 1000
    levels = [round(x, 6) for x in (hp[m - 1], lp[m - 1]) if x]
    return {"candles": candles, "wt1": [], "wt2": [], "markers": markers, "levels": levels, "bg_fields": bg, "entry_mode": "pv",
            "wt1v": round(hp[m - 1], 4), "wt2v": round(lp[m - 1], 4), "bars": n}


# ============================================================================
# MONTE-CARLO: zufaellige Kombinationen der AUSGEWAEHLTEN Werte, Rest bleibt wie in den Einstellungen
# ============================================================================
PV_MC_PARAMS = {   # Kurzschluessel (= Schluessel in _pv_params) -> (Config-Schluessel, Typ, Nachkommastellen)
    "left": ("pv_left", int, 0), "right": ("pv_right", int, 0), "buffer": ("pv_buffer_pct", float, 3), "be_usd": ("pv_be_trigger_usd", float, 2),
    "sl_usd": ("pv_sl_usd", float, 2), "tp_usd": ("pv_tp_usd", float, 2),
    "tp1_usd": ("pv_tp1_usd", float, 2), "tp2_usd": ("pv_tp2_usd", float, 2), "tp3_usd": ("pv_tp3_usd", float, 2),
    "tp1_pct": ("pv_tp1_pct", float, 0), "tp2_pct": ("pv_tp2_pct", float, 0),
}
PV_MC_MAX_RUNS = 1000
PV_MC_MIN_RELIABLE_TRADES = 5
PV_MC_TEST_FRACTION = 0.3
PV_MC_MAX_CANDLES = 80_000
PV_MC_TIME_BUDGET = 70
_PV_MC_ZERO_OK = ("be_usd", "sl_usd", "tp_usd", "tp1_usd", "tp2_usd", "tp3_usd", "buffer")


def _pv_mc_compute(candles, cfg, combos, base_p, start_i, exclude_top_n, deadline):
    ts, o, h, l, c = candles[0], candles[1], candles[2], candles[3], candles[4]
    n = len(c)
    split_ts = ts[start_i + int((n - start_i) * (1 - PV_MC_TEST_FRACTION))]

    def run_one(over):
        p = dict(base_p, **over)
        bull, bear, xdn, xup, _s, _t = pv_compute(o, h, l, c, p)
        trades = _pv_simulate(candles, cfg, bull, bear, xdn, xup, start_i, p)
        st = summarize_backtest_trades(trades, exclude_top_n)
        tr = summarize_backtest_trades([t for t in trades if t.get("entry_ts", 0) < split_ts], exclude_top_n)
        te = summarize_backtest_trades([t for t in trades if t.get("entry_ts", 0) >= split_ts], exclude_top_n)
        row = {PV_MC_PARAMS[k][0]: p[k] for k in PV_MC_PARAMS}
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


async def run_pv_montecarlo(symbol, cfg, days, runs, vary, exclude_top_n=1, seed=None, max_dd_usd=0.0):
    """vary: {'thr': (a, b), ...} - nur diese Werte werden zufaellig gezogen, alle anderen bleiben wie in der Config. Die aktuellen
    Einstellungen laufen IMMER als Referenz (= identisch mit dem Backtest). Alle Kombinationen handeln im selben Fenster 'letzte N Tage'.
    Training (erste 70 %) / Test (letzte 30 %): 'robust' = in beiden im Plus."""
    base_p = _pv_params(cfg)
    resolution = cfg.get("pv_resolution", "15m")
    res_s = _res_seconds(resolution)
    if not vary:
        return {"error": "Bitte mindestens einen Wert zum Variieren auswählen."}
    ranges = {}
    try:
        for k, (a, b) in vary.items():
            if k not in PV_MC_PARAMS:
                return {"error": f"Unbekannter Wert '{k}'."}
            typ = PV_MC_PARAMS[k][1]
            lo, hi = sorted((typ(a), typ(b)))
            if (k in _PV_MC_ZERO_OK and lo < 0) or (k not in _PV_MC_ZERO_OK and lo <= 0):
                return {"error": "Alle Bereiche müssen größer als 0 sein (SL/TP, Break-Even und Puffer dürfen 0 sein)."}
            ranges[k] = (lo, hi)
    except (TypeError, ValueError):
        return {"error": "Ungültige Bereiche für die Parameter."}
    if base_p["tp_mode"] != "partial" and any(k in ranges for k in ("tp1_usd", "tp2_usd", "tp3_usd", "tp1_pct", "tp2_pct")):
        return {"error": "TP1/TP2/TP3 wirken nur, wenn oben bei „Take-Profit-Art“ „Teil-TPs“ gewählt ist. Bitte dort umstellen (oder die Haken entfernen)."}
    if base_p["tp_mode"] == "partial" and "tp_usd" in ranges:
        return {"error": "Der einzelne Take-Profit wirkt im Modus „Teil-TPs“ nicht - bitte stattdessen TP1/TP2/TP3 variieren."}
    max_candles = BACKTEST_MAX_CANDLES.get("pivot_reversal", 100_000)
    if resolution in SUB_MINUTE_RESOLUTIONS:
        max_candles = min(max_candles, 5000)
    p_max = dict(base_p, **{k: ranges[k][1] for k in ranges if k in base_p})
    lead = _lead_days(p_max, res_s) if resolution not in SUB_MINUTE_RESOLUTIONS else 0.0
    candles, err, src = await _we_candles(symbol, resolution, days + lead, max_candles, market_type=cfg.get("binance_market_type", "spot"))
    if err:
        return {"error": err}
    n = len(candles[4]) if candles else 0
    note = {"cache": "Kerzen aus dem Cache.", "abgeleitet": "Kerzen aus einem feineren Zeitrahmen im Cache berechnet.", "neu": "Kerzen neu von Binance geladen."}[src]
    if n > PV_MC_MAX_CANDLES:
        candles = tuple(x[-PV_MC_MAX_CANDLES:] for x in candles)
        note += f" Hinweis: aus Speichergründen wurden nur die jüngsten {PV_MC_MAX_CANDLES} von {n} Kerzen getestet."
        n = PV_MC_MAX_CANDLES
    ts = candles[0]
    start_i = max(_pv_warm(p_max), _window_start(ts, days)) if n else 0
    if n < 400 or n - start_i < 300:
        return {"error": f"Zu wenig Kerzen: {n} erhalten, die Pivots brauchen ~{_pv_warm(p_max)} Kerzen Einlaufzeit, danach sollten noch mind. 300 zum Testen übrig sein. Mehr Tage wählen."}
    runs = max(1, min(PV_MC_MAX_RUNS, int(runs)))
    rng = random.Random(seed)

    def draw(k):
        lo, hi = ranges[k]
        typ, dec = PV_MC_PARAMS[k][1], PV_MC_PARAMS[k][2]
        return rng.randint(lo, hi) if typ is int else round(rng.uniform(lo, hi), dec)

    keys = sorted(ranges)
    seen, combos = {tuple(base_p[k] for k in keys)}, []
    for _ in range(runs * 30):
        if len(combos) >= runs:
            break
        over = {k: draw(k) for k in ranges}
        eff = dict(base_p, **over)
        if eff["tp_mode"] == "partial" and not (eff["tp1_usd"] > 0 and (eff["tp2_usd"] == 0 or eff["tp2_usd"] > eff["tp1_usd"])
                                                and (eff["tp3_usd"] == 0 or eff["tp3_usd"] > max(eff["tp1_usd"], eff["tp2_usd"]))):
            continue   # TP-Stufen muessen aufsteigend sein (TP1 < TP2 < TP3)
        key = tuple(over[k] for k in keys)
        if key not in seen:
            seen.add(key)
            combos.append(over)
    deadline = time.time() + PV_MC_TIME_BUDGET
    results, baseline = await asyncio.to_thread(_pv_mc_compute, candles, cfg, combos, base_p, start_i, exclude_top_n, deadline)
    reliable = [r for r in results if r["trades"] >= PV_MC_MIN_RELIABLE_TRADES]
    robust = [r for r in reliable if r["pnl_train"] > 0 and r["pnl_test"] > 0 and r["trades_test"] >= 3]
    best = sorted(results, key=lambda r: (r["trades"] >= PV_MC_MIN_RELIABLE_TRADES, r["total_pnl_usd"]), reverse=True)
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
        "candles_processed": n, "min_reliable_trades": PV_MC_MIN_RELIABLE_TRADES,
        "combos_tested": len(results), "combos_requested": len(combos), "test_fraction_pct": int(PV_MC_TEST_FRACTION * 100),
        "varied": [PV_MC_PARAMS[k][0] for k in PV_MC_PARAMS if k in ranges], "note": note,
        "baseline": baseline,
        "profitable_pct": round(sum(1 for x in pnls if x > 0) / len(pnls) * 100, 1) if pnls else 0,
        "median_pnl_usd": round(pnls[len(pnls) // 2], 2) if pnls else 0,
        "robust_count": len(robust), "lowdd_count": len(lowdd), "max_dd_limit": max_dd_usd,
        "lowdd_results": lowdd[:30], "robust_results": robust_sorted[:30], "results": best[:30], "worst_results": worst[:20],
    }
