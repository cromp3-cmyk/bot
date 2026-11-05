"""
auction.py - eigene Strategie "Auction Range Migration Trend" (entry_mode "auction_trend", Config-Praefix ar_*).

Nachbau des Pine-Skripts "Auction Range Migration Trend" (SchizoQuant). Signal = Trendwechsel (SQ-Zustand), nur auf GESCHLOSSENEN Kerzen
(Pine: barstate.isconfirmed) - dadurch kein Repaint:

  ATR            = RMA(TR, atr_len)  (vor dem Einschwingen: Kerzenspanne)
  Migration      = Mitte-Bewegung*center_inf + Schlusskurs-Bewegung*(1-center_inf)   (beides durch ATR geteilt)
  Territorium    = Schluss ueber vorigem Hoch (+) bzw. unter vorigem Tief (-), je max. 1 ATR
  Drive          = clamp(Migration*(1+0.5*Range-Expansion) + Territorium*territory_inf, -shock, +shock)
  Druck          = EMA(Drive, drive_len);  Persistenz = EMA(Stimme(Drive > +-min_migration), persist_len)
  Trend-Score    = Druck*(1-persist_inf) + Persistenz*persist_inf
  Schwelle       = reversal_thr + Congestion*overlap_prot   (Congestion aus der EMA der Kerzen-Ueberlappung)
  Zustand SQ     = erste bereite Kerze: Vorzeichen des Scores; danach LONG, wenn SQ=-1 und Score > Schwelle; SHORT, wenn SQ=+1 und Score < -Schwelle.

Der Bot handelt jeden Wechsel (LONG/SHORT) zum Schlusskurs der Signalkerze (ar_exit_mode: flip = Gegensignal dreht die Position, Standard; sltp_only = nur SL/TP).
SL/TP in $, Teil-TPs mit Break-Even, Mehrfach-Einstiege, Richtungswahl und Cooldown nach SL funktionieren wie bei Momentum-Feld.
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


def _ar_params(cfg):
    em = cfg.get("ar_exit_mode", "flip")
    mode = cfg.get("ar_direction_mode", "both")
    return {
        "atr": max(1, int(_fl(cfg, "ar_atr_len", 14))), "center": max(0.0, min(1.0, _fl(cfg, "ar_center_inf", 0.35))),
        "terr": max(0.0, min(2.0, _fl(cfg, "ar_territory_inf", 0.45))), "shock": max(0.5, min(5.0, _fl(cfg, "ar_shock", 2.0))),
        "drive": max(1, int(_fl(cfg, "ar_drive_len", 20))), "persist": max(2, int(_fl(cfg, "ar_persist_len", 13))),
        "min_mig": max(0.0, min(1.0, _fl(cfg, "ar_min_migration", 0.03))), "persist_inf": max(0.0, min(1.0, _fl(cfg, "ar_persist_inf", 0.40))),
        "thr": max(0.01, min(1.0, _fl(cfg, "ar_reversal_thr", 0.19))), "overlap": max(2, int(_fl(cfg, "ar_overlap_len", 2))),
        "prot": max(0.0, min(1.0, _fl(cfg, "ar_overlap_prot", 0.25))),
        "exit_mode": em if em in EXIT_MODES else "flip",
        "direction": mode if mode in ("both", "long_only", "short_only") else "both",
        "sl_usd": max(0.0, _fl(cfg, "ar_sl_usd", 0.0)), "tp_usd": max(0.0, _fl(cfg, "ar_tp_usd", 0.0)),
        "max_entries": max(1, min(20, int(_fl(cfg, "ar_max_entries", 1)))),
        "profit_only": bool(cfg.get("ar_profit_only_close", False)),
        "add_dist_pct": max(0.0, _fl(cfg, "ar_add_dist_pct", 0)),
        "add_dist_dir": cfg.get("ar_add_dist_dir", "any") if cfg.get("ar_add_dist_dir", "any") in ("any", "against", "with") else "any",
        "tp_mode": "partial" if cfg.get("ar_tp_mode", "single") == "partial" else "single",
        "tp1_usd": max(0.0, _fl(cfg, "ar_tp1_usd", 5.0)), "tp2_usd": max(0.0, _fl(cfg, "ar_tp2_usd", 10.0)), "tp3_usd": max(0.0, _fl(cfg, "ar_tp3_usd", 15.0)),
        "tp1_pct": max(1.0, min(100.0, _fl(cfg, "ar_tp1_pct", 33))), "tp2_pct": max(1.0, min(100.0, _fl(cfg, "ar_tp2_pct", 33))),
        "be_usd": max(0.0, _fl(cfg, "ar_be_trigger_usd", 0.0)),
        "be": bool(cfg.get("ar_be_after_tp1", True)), "sl_to_tp1": bool(cfg.get("ar_sl_to_tp1_on_tp2", True)),
    }


def _ar_warm(p):
    """Kerzen, bis EMAs/ATR eingeschwungen sind (Pine: warmup = max der Laengen; die EMAs brauchen davor noch Auslauf)."""
    return max(p["atr"], p["drive"], p["persist"], p["overlap"]) * 4 + 30


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


def _atr(h, l, c, n):
    """Pine ta.atr = RMA(TR): bis Kerze n-1 na (None), dort der Durchschnitt der ersten n TR, danach alpha = 1/n."""
    out, rma = [], None
    tr = []
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


def ar_series(o, h, l, c, p, mintick=1e-8):
    """-> (bull[], bear[], state[], path[], score[], thr[]). bull/bear = Trendwechsel auf der (geschlossenen) Kerze; state = SQ (+1/-1/0)."""
    n = len(c)
    atr = _atr(h, l, c, p["atr"])
    drive, vote, ovl = [], [], []
    for i in range(n):
        safe = max(atr[i] if atr[i] is not None else (h[i] - l[i]), mintick)
        center = (h[i] + l[i]) * 0.5
        pcenter = (h[i - 1] + l[i - 1]) * 0.5 if i > 0 else center
        cmove = (center - pcenter) / safe
        clmove = (c[i] - (c[i - 1] if i > 0 else c[i])) / safe
        mig = cmove * p["center"] + clmove * (1.0 - p["center"])
        ph, pl = (h[i - 1], l[i - 1]) if i > 0 else (h[i], l[i])
        od = max(min(h[i], ph) - max(l[i], pl), 0.0)
        cr, pr = max(h[i] - l[i], mintick), max(ph - pl, mintick)
        ratio = min(od / max(cr, pr), 1.0)
        exp = 1.0 - ratio
        bt = min((c[i] - ph) / safe, 1.0) if c[i] > ph else 0.0
        st_ = min((pl - c[i]) / safe, 1.0) if c[i] < pl else 0.0
        raw = mig * (1.0 + exp * 0.50) + (bt - st_) * p["terr"]
        d = max(-p["shock"], min(p["shock"], raw))
        drive.append(d)
        vote.append(1.0 if d > p["min_mig"] else -1.0 if d < -p["min_mig"] else 0.0)
        ovl.append(ratio)
    press = _ema(drive, p["drive"])
    pers = _ema(vote, p["persist"])
    avg_ov = _ema(ovl, p["overlap"])
    score = [press[i] * (1.0 - p["persist_inf"]) + pers[i] * p["persist_inf"] for i in range(n)]
    thr = [p["thr"] + max(0.0, (avg_ov[i] - 0.30) / 0.70) * p["prot"] for i in range(n)]
    warm = max(p["atr"], p["drive"], p["persist"], p["overlap"])
    bull, bear, state = [False] * n, [False] * n, [0] * n
    sq = 0
    for i in range(n):
        if i >= warm and atr[i] is not None:
            if sq == 0:
                sq = 1 if score[i] >= 0.0 else -1
            elif sq == -1 and score[i] > thr[i]:
                sq, bull[i] = 1, True
            elif sq == 1 and score[i] < -thr[i]:
                sq, bear[i] = -1, True
        state[i] = sq
    # Auction Path (nur zur Anzeige)
    path, prev = [], None
    for i in range(n):
        ph, pl = (h[i - 1], l[i - 1]) if i > 0 else (h[i], l[i])
        od = max(min(h[i], ph) - max(l[i], pl), 0.0)
        ratio = min(od / max(max(h[i] - l[i], mintick), max(ph - pl, mintick)), 1.0)
        speed = 0.12 + 0.38 * (1.0 - ratio)
        center = (h[i] + l[i]) * 0.5
        prev = center if prev is None else prev + speed * (center - prev)
        path.append(prev)
    return bull, bear, state, path, score, thr


def ar_compute(o, h, l, c, p):
    """-> (bull[], bear[], xdn[], xup[], score, thr) - gleiche Form wie bei Momentum-Feld (xdn/xup gibt es hier nicht)."""
    bull, bear, _st, _path, score, thr = ar_series(o, h, l, c, p)
    n = len(c)
    return bull, bear, [False] * n, [False] * n, score, thr


def _signal_exit(pos, xdn_i, xup_i, p):
    return None   # keine eigenen Signal-Ausstiege ausser dem Gegensignal (Flip)


def _ar_levels(direction, entry, size, p):
    if not size or size <= 0:
        return None, None
    sgn = 1 if direction == "long" else -1
    sl = entry - sgn * p["sl_usd"] / size if p["sl_usd"] > 0 else None
    tp = entry + sgn * p["tp_usd"] / size if (p["tp_usd"] > 0 and p["tp_mode"] != "partial") else None
    return sl, tp


def _ar_ladder(direction, entry, size, p):
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


def _ar_reset_state(st):
    st["ar_sl_price"] = None
    st["ar_tp_price"] = None
    st["ar_tps"] = []
    st["ar_tp_pcts"] = []
    st["ar_tp_done"] = 0
    st["ar_be"] = False
    st["ar_entries"] = 0
    st["ar_last_entry"] = None


async def check_ar_sl_tp(symbol, price):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    pos = st["position"]
    if pos is None or price is None:
        return
    p = _ar_params(cfg)
    sl, tp = st.get("ar_sl_price"), st.get("ar_tp_price")
    if sl is not None and ((pos == "long" and price <= sl) or (pos == "short" and price >= sl)):
        prot = st.get("ar_be")
        reason = "SL-TP1" if prot == "tp1" else "BREAKEVEN" if prot else "SL"
        debug_log(f"🚪 [{symbol}] Auction Range Migration {reason}: {pos.upper()} @ {price} (Ziel war {round(sl, 6)})")
        await execute_exit(symbol, price, reason)
        if st["position"] is None:
            if not prot:
                st["ar_sl_cooldown_until"] = time.time() + cfg.get("ar_sl_cooldown_seconds", 30)
            _ar_reset_state(st)
        return
    if p["be_usd"] > 0 and not st.get("ar_be") and st.get("avg_entry_price") and (st.get("total_coin_size") or 0) > 0:
        entry_, size_ = st["avg_entry_price"], st["total_coin_size"]
        pnl_ = ((price - entry_) if pos == "long" else (entry_ - price)) * size_
        if pnl_ >= p["be_usd"]:
            st["ar_sl_price"], st["ar_be"] = entry_, "be"
            debug_log(f"📡 [{symbol}] Auction Range Migration: {round(pnl_, 2)} $ Gewinn erreicht (Ziel {p['be_usd']:g} $) - SL auf Einstieg {round(entry_, 6)}")
    tps = st.get("ar_tps") or []
    if tps:
        pcts = st.get("ar_tp_pcts") or []
        done = int(st.get("ar_tp_done") or 0)
        while done < len(tps) and ((pos == "long" and price >= tps[done]) or (pos == "short" and price <= tps[done])):
            pct = pcts[done] if done < len(pcts) else 100.0
            if done == len(tps) - 1 or pct >= 99.5:
                debug_log(f"🎯 [{symbol}] Auction Range Migration TP{done + 1} (Rest): {pos.upper()} @ {price}")
                await execute_exit(symbol, price, f"TP{done + 1}")
                if st["position"] is None:
                    _ar_reset_state(st)
                return
            if not await execute_partial_exit(symbol, price, pct / 100.0, f"TP{done + 1}"):
                return
            done += 1
            st["ar_tp_done"] = done
            debug_log(f"📡 [{symbol}] Auction Range Migration TP{done} erreicht - {pct:g}% geschlossen")
            if done == 1 and p["be"] and st.get("avg_entry_price"):
                st["ar_sl_price"], st["ar_be"] = st["avg_entry_price"], "be"
                debug_log(f"📡 [{symbol}] Auction Range Migration TP1 erreicht - SL auf Break-Even")
            elif done == 2 and p["sl_to_tp1"]:
                st["ar_sl_price"], st["ar_be"] = tps[0], "tp1"
                debug_log(f"📡 [{symbol}] Auction Range Migration TP2 erreicht - SL auf TP1")
        return
    if tp is not None and ((pos == "long" and price >= tp) or (pos == "short" and price <= tp)):
        debug_log(f"🎯 [{symbol}] Auction Range Migration TP: {pos.upper()} @ {price} (Ziel war {round(tp, 6)})")
        await execute_exit(symbol, price, "TP")
        if st["position"] is None:
            _ar_reset_state(st)


def _set_levels(st, cfg, target, price):
    entry = st.get("avg_entry_price") or price
    size = st.get("total_coin_size") or 0
    p = _ar_params(cfg)
    st["ar_sl_price"], st["ar_tp_price"] = _ar_levels(target, entry, size, p)
    lad = _ar_ladder(target, entry, size, p)
    st["ar_tps"], st["ar_tp_pcts"], st["ar_tp_done"], st["ar_be"] = [x[0] for x in lad], [x[1] for x in lad], 0, False
    st["ar_entries"] = 1
    st["ar_last_entry"] = price


def _refresh_levels(st, cfg, target, price):
    """Nach einem Nachkauf: SL/TP neu an Ø-Einstieg und Gesamtgroesse haengen - aber nur, solange noch kein TP erreicht / kein Break-Even gesetzt ist."""
    if int(st.get("ar_tp_done") or 0) > 0 or st.get("ar_be"):
        return
    entry = st.get("avg_entry_price") or price
    size = st.get("total_coin_size") or 0
    p = _ar_params(cfg)
    st["ar_sl_price"], st["ar_tp_price"] = _ar_levels(target, entry, size, p)
    lad = _ar_ladder(target, entry, size, p)
    st["ar_tps"], st["ar_tp_pcts"] = [x[0] for x in lad], [x[1] for x in lad]


def _dist_ok(direction, last, price, p):
    """Mindestabstand fuer einen Nachkauf erfuellt? (Prozent vom letzten Einstiegspreis, Richtung laut ar_add_dist_dir)"""
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


async def check_ar_signal(symbol, bull_i, bear_i, xdn_i, xup_i, price):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or price is None:
        return
    p = _ar_params(cfg)
    pos = st["position"]
    tri = "long" if bull_i else "short" if bear_i else None
    if bull_i and bear_i:
        tri = None
    if pos is None:
        if tri is None or not _allowed(tri, p) or time.time() < st.get("ar_sl_cooldown_until", 0.0):
            return
        debug_log(f"📡 [{symbol}] Auction Range Migration Einstieg: {'BUY' if tri == 'long' else 'SELL'} ({tri.upper()}) @ {price}")
        await execute_entry(symbol, tri, price, is_add_on=False)
        if st["position"] is not None:
            _ar_reset_state(st)
            _set_levels(st, cfg, tri, price)
        return
    if tri is not None and tri == pos:   # weiteres Signal in dieselbe Richtung -> Nachkauf (wenn erlaubt)
        n_ent = int(st.get("ar_entries") or 1)
        if p["max_entries"] > 1 and n_ent < p["max_entries"] and _allowed(tri, p):
            last = st.get("ar_last_entry") or st.get("avg_entry_price")
            if not _dist_ok(tri, last, price, p):
                debug_log(f"⏭️ [{symbol}] Auction Range Migration Nachkauf übersprungen - Abstand zum letzten Einstieg ({last}) kleiner als {p['add_dist_pct']}% ({p['add_dist_dir']})")
                return
            before = st.get("total_coin_size") or 0
            debug_log(f"➕ [{symbol}] Auction Range Migration Nachkauf #{n_ent + 1}/{p['max_entries']}: {tri.upper()} @ {price}")
            await execute_entry(symbol, tri, price, is_add_on=True)
            if (st.get("total_coin_size") or 0) > before:
                st["ar_entries"] = n_ent + 1
                st["ar_last_entry"] = price
                _refresh_levels(st, cfg, tri, price)
        return
    if p["exit_mode"] == "sltp_only":
        return
    if tri is not None and tri != pos:
        if p["profit_only"] and not _in_profit(st, pos, price):
            debug_log(f"⏸️ [{symbol}] Auction Range Migration Gegensignal {tri.upper()} ignoriert - {pos.upper()} ist im Minus (nur im Plus schließen)")
            return
        if _allowed(tri, p):
            debug_log(f"🔄 [{symbol}] Auction Range Migration Flip: {pos.upper()} -> {tri.upper()} @ {price}")
            if await execute_reverse(symbol, price, "ARM-FLIP", size_multiplier=1.0):
                _ar_reset_state(st)
                _set_levels(st, cfg, tri, price)
        else:
            debug_log(f"🚪 [{symbol}] Auction Range Migration Gegensignal (Richtung gesperrt): {pos.upper()} schließen @ {price}")
            await execute_exit(symbol, price, "ARM-EXIT-DIR")
            if st["position"] is None:
                _ar_reset_state(st)
        return
    reason = _signal_exit(pos, xdn_i, xup_i, p)
    if reason:
        if p["profit_only"] and not _in_profit(st, pos, price):
            debug_log(f"⏸️ [{symbol}] Auction Range Migration Cross-Ausstieg ignoriert - {pos.upper()} ist im Minus (nur im Plus schließen)")
            return
        debug_log(f"🎯 [{symbol}] Auction Range Migration {reason}: {pos.upper()} beendet @ {price}")
        await execute_exit(symbol, price, reason)
        if st["position"] is None:
            _ar_reset_state(st)


async def ar_poll_loop(symbol):
    """Auction Range Migration live: wertet NUR abgeschlossene Kerzen aus; SL/TP laufend gegen den Live-Preis."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0
    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "auction_trend" and cfg["bot_active"] and b["state"].get("session_started"):
                resolution = cfg.get("ar_resolution", "15m")
                p = _ar_params(cfg)
                res_s = _res_seconds(resolution)
                min_needed = _ar_warm(p) + 2
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
                    await check_ar_sl_tp(symbol, st["last_price"])

                if closed_ts and len(closed_c) > min_needed:
                    age = (now * 1000 - closed_ts[-1]) / 1000
                    if age > 300 + res_s:
                        debug_log(f"⚠️ [{symbol}] Auction Range Migration: letzte Kerze wirkt veraltet ({round(age)}s alt, Auflösung {resolution}) - überspringe Durchlauf.")
                        closed_ts = None
                    elif st["last_price"] is not None and closed_c[-1] and abs(closed_c[-1] - st["last_price"]) / st["last_price"] * 100 > 2.0:
                        debug_log(f"⚠️ [{symbol}] Auction Range Migration: Kerzendaten unplausibel (Kerze {closed_c[-1]} vs. Live {st['last_price']}) - überspringe Durchlauf.")
                        closed_ts = None

                if closed_ts and len(closed_c) > min_needed and (last_processed_ts is None or closed_ts[-1] > last_processed_ts):
                    bull, bear, xdn, xup, wt1, wt2 = await asyncio.to_thread(ar_compute, closed_o, closed_h, closed_l, closed_c, p)
                    if due_heartbeat:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] Auction Range Migration aktiv: {resolution}, ATR {p['atr']}, Drive {p['drive']}, Persistenz {p['persist']}, "
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
                        await check_ar_signal(symbol, bull[idx], bear[idx], xdn[idx], xup[idx], price_i)
                elif due_heartbeat and not closed_ts:
                    last_heartbeat = now
                    debug_log(f"⏳ [{symbol}] Auction Range Migration wartet: keine Kerzen erhalten (Auflösung {resolution})")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Auction Range Migration-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        await asyncio.sleep(_poll_delay(b["config"].get("ar_resolution", "15m"), b["state"]))


def _ar_simulate(candles, cfg, bull, bear, xdn, xup, start_i, p):
    """Backtest-Gegenstueck zu check_ar_signal / check_ar_sl_tp: Einstieg/Flip zum Schlusskurs der Bestaetigungs-Kerze, SL/TP ab der
    naechsten Kerze ueber Hoch/Tief (SL gewinnt bei Gleichzeitigkeit), Cooldown nach SL, Gegen-Divergenz vor WT-Kreuzung."""
    ts, h, l, c = candles[0], candles[2], candles[3], candles[4]
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    cooldown_ms = cfg.get("ar_sl_cooldown_seconds", 30) * 1000
    position, trades, cool_until = None, [], None

    def open_pos(direction, price, i):
        size = (margin * leverage) / price
        sl, tp = _ar_levels(direction, price, size, p)
        return {"dir": direction, "entry": price, "size": size, "entry_i": i, "sl": sl, "tp": tp,
                "tps": _ar_ladder(direction, price, size, p), "done": 0, "prot": False, "entries": 1, "last": price}

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
                    position["sl"], position["tp"] = _ar_levels(d, position["entry"], tot, p)
                    position["tps"] = _ar_ladder(d, position["entry"], tot, p)
            continue
        if p["exit_mode"] == "sltp_only":
            continue
        pnl_now = ((price - position["entry"]) if d == "long" else (position["entry"] - price)) * position["size"]
        if tri is not None and tri != d:
            if p["profit_only"] and pnl_now <= 0:
                continue
            ok = _allowed(tri, p)
            _bt_close_trade(trades, d, position["entry"], price, position["size"], i, position["entry_i"], "ARM-FLIP" if ok else "ARM-EXIT-DIR", ts=ts)
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
    return _ar_warm(p) * res_s / 86400 + 0.1


def _window_start(ts, days):
    from bisect import bisect_left
    return bisect_left(ts, ts[-1] - int(days * 86_400_000))


async def run_ar_backtest(symbol, cfg, days, exclude_top_n=1):
    max_candles = BACKTEST_MAX_CANDLES.get("auction_trend", 100_000)
    resolution = cfg.get("ar_resolution", "15m")
    if resolution in SUB_MINUTE_RESOLUTIONS:
        max_candles = min(max_candles, 5000)
    p = _ar_params(cfg)
    res_s = _res_seconds(resolution)
    lead = _lead_days(p, res_s) if resolution not in SUB_MINUTE_RESOLUTIONS else 0.0
    t_fetch = time.time()
    candles, err, src = await _we_candles(symbol, resolution, days + lead, max_candles, market_type=cfg.get("binance_market_type", "spot"))
    t_fetch = time.time() - t_fetch
    if err:
        return {"error": err}
    warm = _ar_warm(p)
    if not candles or len(candles[4]) < warm + 100:
        return {"error": f"Zu wenig historische Kerzen für einen aussagekräftigen Backtest erhalten (mind. ~{warm + 100} nötig)."}
    ts, o, h, l, c = candles[0], candles[1], candles[2], candles[3], candles[4]
    t_calc = time.time()
    bull, bear, xdn, xup, wt1, wt2 = await asyncio.to_thread(ar_compute, o, h, l, c, p)
    start_i = max(warm, _window_start(ts, days))
    trades = _ar_simulate(candles, cfg, bull, bear, xdn, xup, start_i, p)
    t_calc = time.time() - t_calc
    stats = summarize_backtest_trades(trades, exclude_top_n)
    usable_days = (ts[-1] - ts[start_i]) / 86_400_000 if start_i < len(ts) else 0
    return {
        "symbol": symbol, "entry_mode": "auction_trend", "resolution": resolution,
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
def ar_chart_payload(ts, o, h, l, c, cfg, max_bars=1000):
    """Kerzen + Auction-Path (gruen = LONG-Zustand, lila = SHORT) + Hintergrundfelder + LONG/SHORT-Marker; unten Trend-Score und Schwelle.
    Nur geschlossene Kerzen werden gerechnet."""
    p = _ar_params(cfg)
    n = len(c)
    if n < 30:
        return None
    bull, bear, state, path, score, thr = ar_series(o[:-1], h[:-1], l[:-1], c[:-1], p)
    m = n - 1
    off = max(0, n - max_bars)
    candles = [{"time": ts[i] // 1000, "open": o[i], "high": h[i], "low": l[i], "close": c[i]} for i in range(off, n)]
    markers, bg, cur = [], [], None
    for i in range(off, m):
        if bull[i] or bear[i]:
            markers.append({"time": ts[i] // 1000, "kind": "long" if bull[i] else "short", "price": c[i], "text": "LONG" if bull[i] else "SHORT"})
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
    ar_path = [{"time": ts[i] // 1000, "value": round(path[i], 6), "color": "#39ff14" if state[i] > 0 else "#8a2be2" if state[i] < 0 else "#888888"} for i in range(off, m)]
    wt1 = [{"time": ts[i] // 1000, "value": round(score[i], 4)} for i in range(off, m)]
    wt2 = [{"time": ts[i] // 1000, "value": round(thr[i], 4)} for i in range(off, m)]
    return {"candles": candles, "wt1": wt1, "wt2": wt2, "markers": markers, "levels": [], "bg_fields": bg, "ar_path": ar_path, "entry_mode": "ar",
            "wt1v": round(score[m - 1], 3), "wt2v": round(thr[m - 1], 3), "bars": n}


# ============================================================================
# MONTE-CARLO: zufaellige Kombinationen der AUSGEWAEHLTEN Werte, Rest bleibt wie in den Einstellungen
# ============================================================================
AR_MC_PARAMS = {   # Kurzschluessel (= Schluessel in _ar_params) -> (Config-Schluessel, Typ, Nachkommastellen)
    "atr": ("ar_atr_len", int, 0), "center": ("ar_center_inf", float, 2), "terr": ("ar_territory_inf", float, 2), "shock": ("ar_shock", float, 2),
    "drive": ("ar_drive_len", int, 0), "persist": ("ar_persist_len", int, 0), "min_mig": ("ar_min_migration", float, 3),
    "persist_inf": ("ar_persist_inf", float, 2), "thr": ("ar_reversal_thr", float, 3), "overlap": ("ar_overlap_len", int, 0),
    "prot": ("ar_overlap_prot", float, 2), "be_usd": ("ar_be_trigger_usd", float, 2),
    "sl_usd": ("ar_sl_usd", float, 2), "tp_usd": ("ar_tp_usd", float, 2),
    "tp1_usd": ("ar_tp1_usd", float, 2), "tp2_usd": ("ar_tp2_usd", float, 2), "tp3_usd": ("ar_tp3_usd", float, 2),
    "tp1_pct": ("ar_tp1_pct", float, 0), "tp2_pct": ("ar_tp2_pct", float, 0),
}
AR_MC_MAX_RUNS = 1000
AR_MC_MIN_RELIABLE_TRADES = 5
AR_MC_TEST_FRACTION = 0.3
AR_MC_MAX_CANDLES = 80_000
AR_MC_TIME_BUDGET = 70
_AR_MC_ZERO_OK = ("be_usd", "sl_usd", "tp_usd", "tp1_usd", "tp2_usd", "tp3_usd", "prot", "min_mig")


def _ar_mc_compute(candles, cfg, combos, base_p, start_i, exclude_top_n, deadline):
    ts, o, h, l, c = candles[0], candles[1], candles[2], candles[3], candles[4]
    n = len(c)
    split_ts = ts[start_i + int((n - start_i) * (1 - AR_MC_TEST_FRACTION))]

    def run_one(over):
        p = dict(base_p, **over)
        bull, bear, xdn, xup, _s, _t = ar_compute(o, h, l, c, p)
        trades = _ar_simulate(candles, cfg, bull, bear, xdn, xup, start_i, p)
        st = summarize_backtest_trades(trades, exclude_top_n)
        tr = summarize_backtest_trades([t for t in trades if t.get("entry_ts", 0) < split_ts], exclude_top_n)
        te = summarize_backtest_trades([t for t in trades if t.get("entry_ts", 0) >= split_ts], exclude_top_n)
        row = {AR_MC_PARAMS[k][0]: p[k] for k in AR_MC_PARAMS}
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


async def run_ar_montecarlo(symbol, cfg, days, runs, vary, exclude_top_n=1, seed=None, max_dd_usd=0.0):
    """vary: {'thr': (a, b), ...} - nur diese Werte werden zufaellig gezogen, alle anderen bleiben wie in der Config. Die aktuellen
    Einstellungen laufen IMMER als Referenz (= identisch mit dem Backtest). Alle Kombinationen handeln im selben Fenster 'letzte N Tage'.
    Training (erste 70 %) / Test (letzte 30 %): 'robust' = in beiden im Plus."""
    base_p = _ar_params(cfg)
    resolution = cfg.get("ar_resolution", "15m")
    res_s = _res_seconds(resolution)
    if not vary:
        return {"error": "Bitte mindestens einen Wert zum Variieren auswählen."}
    ranges = {}
    try:
        for k, (a, b) in vary.items():
            if k not in AR_MC_PARAMS:
                return {"error": f"Unbekannter Wert '{k}'."}
            typ = AR_MC_PARAMS[k][1]
            lo, hi = sorted((typ(a), typ(b)))
            if (k in _AR_MC_ZERO_OK and lo < 0) or (k not in _AR_MC_ZERO_OK and lo <= 0):
                return {"error": "Alle Bereiche müssen größer als 0 sein (SL/TP, Overlap-Schutz und Meaningful Migration dürfen 0 sein)."}
            ranges[k] = (lo, hi)
    except (TypeError, ValueError):
        return {"error": "Ungültige Bereiche für die Parameter."}
    if base_p["tp_mode"] != "partial" and any(k in ranges for k in ("tp1_usd", "tp2_usd", "tp3_usd", "tp1_pct", "tp2_pct")):
        return {"error": "TP1/TP2/TP3 wirken nur, wenn oben bei „Take-Profit-Art“ „Teil-TPs“ gewählt ist. Bitte dort umstellen (oder die Haken entfernen)."}
    if base_p["tp_mode"] == "partial" and "tp_usd" in ranges:
        return {"error": "Der einzelne Take-Profit wirkt im Modus „Teil-TPs“ nicht - bitte stattdessen TP1/TP2/TP3 variieren."}
    if "persist" in ranges and ranges["persist"][0] < 2:
        return {"error": "Directional Persistence muss mindestens 2 sein."}
    if "overlap" in ranges and ranges["overlap"][0] < 2:
        return {"error": "Range-Overlap-Länge muss mindestens 2 sein."}
    max_candles = BACKTEST_MAX_CANDLES.get("auction_trend", 100_000)
    if resolution in SUB_MINUTE_RESOLUTIONS:
        max_candles = min(max_candles, 5000)
    p_max = dict(base_p, **{k: ranges[k][1] for k in ranges if k in base_p})
    lead = _lead_days(p_max, res_s) if resolution not in SUB_MINUTE_RESOLUTIONS else 0.0
    candles, err, src = await _we_candles(symbol, resolution, days + lead, max_candles, market_type=cfg.get("binance_market_type", "spot"))
    if err:
        return {"error": err}
    n = len(candles[4]) if candles else 0
    note = {"cache": "Kerzen aus dem Cache.", "abgeleitet": "Kerzen aus einem feineren Zeitrahmen im Cache berechnet.", "neu": "Kerzen neu von Binance geladen."}[src]
    if n > AR_MC_MAX_CANDLES:
        candles = tuple(x[-AR_MC_MAX_CANDLES:] for x in candles)
        note += f" Hinweis: aus Speichergründen wurden nur die jüngsten {AR_MC_MAX_CANDLES} von {n} Kerzen getestet."
        n = AR_MC_MAX_CANDLES
    ts = candles[0]
    start_i = max(_ar_warm(p_max), _window_start(ts, days)) if n else 0
    if n < 400 or n - start_i < 300:
        return {"error": f"Zu wenig Kerzen: {n} erhalten, der Trend-Score braucht ~{_ar_warm(p_max)} Kerzen Einlaufzeit, danach sollten noch mind. 300 zum Testen übrig sein. Mehr Tage wählen."}
    runs = max(1, min(AR_MC_MAX_RUNS, int(runs)))
    rng = random.Random(seed)

    def draw(k):
        lo, hi = ranges[k]
        typ, dec = AR_MC_PARAMS[k][1], AR_MC_PARAMS[k][2]
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
    deadline = time.time() + AR_MC_TIME_BUDGET
    results, baseline = await asyncio.to_thread(_ar_mc_compute, candles, cfg, combos, base_p, start_i, exclude_top_n, deadline)
    reliable = [r for r in results if r["trades"] >= AR_MC_MIN_RELIABLE_TRADES]
    robust = [r for r in reliable if r["pnl_train"] > 0 and r["pnl_test"] > 0 and r["trades_test"] >= 3]
    best = sorted(results, key=lambda r: (r["trades"] >= AR_MC_MIN_RELIABLE_TRADES, r["total_pnl_usd"]), reverse=True)
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
        "candles_processed": n, "min_reliable_trades": AR_MC_MIN_RELIABLE_TRADES,
        "combos_tested": len(results), "combos_requested": len(combos), "test_fraction_pct": int(AR_MC_TEST_FRACTION * 100),
        "varied": [AR_MC_PARAMS[k][0] for k in AR_MC_PARAMS if k in ranges], "note": note,
        "baseline": baseline,
        "profitable_pct": round(sum(1 for x in pnls if x > 0) / len(pnls) * 100, 1) if pnls else 0,
        "median_pnl_usd": round(pnls[len(pnls) // 2], 2) if pnls else 0,
        "robust_count": len(robust), "lowdd_count": len(lowdd), "max_dd_limit": max_dd_usd,
        "lowdd_results": lowdd[:30], "robust_results": robust_sorted[:30], "results": best[:30], "worst_results": worst[:20],
    }
