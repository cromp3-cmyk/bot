"""
wae_ema.py - eigene Strategie "WAE-Zonen + EMA9" (entry_mode "wae_ema", Config-Praefix we_*).

Portiert aus dem TradingView-Indikator "WAE 1H Zones + EMA9 Signals":
  Zone     : Waddah-Attar-Explosion-Zustand eines HOEHEREN Zeitrahmens (Standard 1h): +1 gruen, -1 rot, 0 keine
             t1  = (MACD_jetzt - MACD_vorher) * Sensitivity,  MACD = EMA(fast) - EMA(slow)
             e1  = 2 * BB-Mult * stdev(close, Kanal)           (Bollinger-Breite)
             dz  = RMA(TrueRange, dead_len) * dead_mult        (Dead Zone)
             gruen: t1>=0 und t1>e1 und t1>dz,  rot: -t1>e1 und -t1>dz  (t1<0)
  Signal   : auf dem kleineren Signal-Zeitrahmen, auf der GESCHLOSSENEN Kerze, nur beim ERSTEN Erfuellen der Bedingung
             CALL (Long) : Zone gruen UND close > EMA9
             PUT  (Short): Zone rot   UND close < EMA9
  Ausstieg : im Pine-Skript nicht vorhanden -> waehlbar (we_exit_mode):
             flip       Gegensignal dreht die Position (Standard)
             ema_cross  Close kreuzt die EMA9 zurueck (Gegensignal hat Vorrang)
             zone_end   Zone ist nicht mehr gruen/rot (Gegensignal hat Vorrang)
             sltp_only  keine Signal-Ausstiege, nur SL/TP (Gegensignale werden ignoriert)
             dazu immer optional fester $-SL/-TP (0 = aus).

Repainting: im Pine-Skript sieht man mit lookahead_on die Zone schon ab der ersten Minute der Stunde (Zukunftsdaten im Verlauf).
Der Bot nutzt NIE Zukunftsdaten:
  we_zone_mode = "closed": Zone = Zustand der zuletzt ABGESCHLOSSENEN Zeitrahmen-Kerze vor der Signalkerze (stabil, entspricht
                           Pine "Closed hour" - eine Stunde Verzoegerung)
  we_zone_mode = "live"  : Zone = Zustand der LAUFENDEN Zeitrahmen-Kerze, berechnet aus allem, was bis zum Schluss der
                           Signalkerze bekannt war (so wie Pine "Live" ohne lookahead in Echtzeit). Backtest und Live rechnen gleich.
"""

import asyncio
import math
import time
import traceback
from array import array
from bisect import bisect_left
from collections import OrderedDict

from bot_core import BOTS, debug_log, execute_entry, execute_exit, execute_reverse
from strategies import (
    SUB_MINUTE_RESOLUTIONS, get_seconds_candles, fetch_candles_binance_multi, _ema_series,
    summarize_backtest_trades, _fetch_cached_mo7_backtest_candles, BACKTEST_MAX_CANDLES, _bt_close_trade,
    fetch_historical_candles_binance_vol,
)

EXIT_MODES = ("flip", "ema_cross", "zone_end", "sltp_only")


def _res_seconds(res):
    if res in SUB_MINUTE_RESOLUTIONS:
        return SUB_MINUTE_RESOLUTIONS[res]
    try:
        n, u = int(res[:-1]), res[-1]
        return n * {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}.get(u, 60)
    except (ValueError, TypeError, IndexError):
        return 60


def _poll_delay(res, st):
    per = _res_seconds(res)
    if st.get("position") is not None or per <= 60:
        return 1.0
    into = time.time() % per
    if into < 6:
        return 1.0
    return max(1.0, min(5.0, per - into - 0.5))


def _we_params(cfg):
    mode = cfg.get("we_direction_mode", "both")
    exit_mode = cfg.get("we_exit_mode", "flip")
    zmode = cfg.get("we_zone_mode", "closed")
    return {
        "zone_tf": str(cfg.get("we_zone_tf", "1h") or "1h"),
        "zone_mode": zmode if zmode in ("closed", "live") else "closed",
        "ema_len": max(1, int(cfg.get("we_ema_len", 9) or 9)),
        "sens": float(cfg.get("we_sensitivity", 150) or 150),
        "fast": max(1, int(cfg.get("we_fast_len", 20) or 20)),
        "slow": max(2, int(cfg.get("we_slow_len", 40) or 40)),
        "chan": max(2, int(cfg.get("we_channel_len", 20) or 20)),
        "bbm": float(cfg.get("we_bb_mult", 2.0) or 2.0),
        "dlen": max(2, int(cfg.get("we_dead_len", 100) or 100)),
        "dmult": float(cfg.get("we_dead_mult", 3.7) or 3.7),
        "exit_mode": exit_mode if exit_mode in EXIT_MODES else "flip",
        "direction": mode if mode in ("both", "long_only", "short_only") else "both",
        "sl_usd": max(0.0, float(cfg.get("we_sl_usd", 0.0) or 0.0)),
        "tp_usd": max(0.0, float(cfg.get("we_tp_usd", 0.0) or 0.0)),
    }


def _we_warm_htf(p):
    """So viele Zeitrahmen-Kerzen braucht der Zustand, bis EMA/RMA/stdev eingeschwungen sind."""
    return max(p["dlen"], p["slow"] * 3, p["chan"]) + 10


# ============================================================================
# RECHNUNG (rein, ohne Netzwerk)
# ============================================================================
def _state_of(t1, e1, dz):
    if t1 is None or e1 is None:
        return 0
    up = t1 if t1 >= 0 else 0.0
    dn = -t1 if t1 < 0 else 0.0
    if up > e1 and up > dz:
        return 1
    if dn > e1 and dn > dz:
        return -1
    return 0


def _stdev_pop(vals):
    n = len(vals)
    m = sum(vals) / n
    return math.sqrt(sum((v - m) * (v - m) for v in vals) / n)


def we_htf_series(hh, hl, hc, p):
    """Zustand je abgeschlossener Zeitrahmen-Kerze + die Zwischenwerte, die fuer den 'live'-Zustand gebraucht werden."""
    n = len(hc)
    ef, es = _ema_series(hc, p["fast"]), _ema_series(hc, p["slow"])
    macd = [ef[i] - es[i] for i in range(n)]
    tr = [hh[0] - hl[0] if n else 0.0]
    for i in range(1, n):
        tr.append(max(hh[i] - hl[i], abs(hh[i] - hc[i - 1]), abs(hl[i] - hc[i - 1])))
    L = p["dlen"]
    rma = [None] * n
    if n >= L:
        rma[L - 1] = sum(tr[:L]) / L
        for i in range(L, n):
            rma[i] = (rma[i - 1] * (L - 1) + tr[i]) / L
    state = [0] * n
    for i in range(1, n):
        t1 = (macd[i] - macd[i - 1]) * p["sens"]
        e1 = 2 * p["bbm"] * _stdev_pop(hc[i - p["chan"] + 1:i + 1]) if i >= p["chan"] - 1 else None
        dz = (rma[i] or 0.0) * p["dmult"]
        state[i] = _state_of(t1, e1, dz)
    return {"state": state, "ef": ef, "es": es, "macd": macd, "rma": rma, "c": hc}


def _we_dev_state(S, k1, ph, pl, pc, p):
    """Zustand der LAUFENDEN Zeitrahmen-Kerze (Nachfolger von Kerze k1) mit bisherigem Hoch ph, Tief pl, aktuellem Kurs pc."""
    af, as_ = 2 / (p["fast"] + 1), 2 / (p["slow"] + 1)
    ef = pc * af + S["ef"][k1] * (1 - af)
    es = pc * as_ + S["es"][k1] * (1 - as_)
    t1 = ((ef - es) - S["macd"][k1]) * p["sens"]
    ch = p["chan"]
    if k1 < ch - 2:
        e1 = None
    else:
        e1 = 2 * p["bbm"] * _stdev_pop(S["c"][k1 - (ch - 2):k1 + 1] + [pc])
    prev_close = S["c"][k1]
    trd = max(ph - pl, abs(ph - prev_close), abs(pl - prev_close))
    L = p["dlen"]
    r_prev = S["rma"][k1]
    rma_d = (r_prev * (L - 1) + trd) / L if r_prev is not None else 0.0
    return _state_of(t1, e1, rma_d * p["dmult"])


def we_aggregate(ts, o, h, l, c, zone_ms):
    """Signal-Kerzen -> Zeitrahmen-Kerzen (UTC-ausgerichtet, wie bei Binance). Eine angefangene erste Kerze wird verworfen."""
    hts, ho, hh, hl, hc = [], [], [], [], []
    cur = None
    for i in range(len(ts)):
        b = ts[i] // zone_ms * zone_ms
        if b != cur:
            cur = b
            hts.append(b); ho.append(o[i]); hh.append(h[i]); hl.append(l[i]); hc.append(c[i])
        else:
            if h[i] > hh[-1]:
                hh[-1] = h[i]
            if l[i] < hl[-1]:
                hl[-1] = l[i]
            hc[-1] = c[i]
    if ts and ts[0] % zone_ms != 0 and hts:
        hts, ho, hh, hl, hc = hts[1:], ho[1:], hh[1:], hl[1:], hc[1:]
    return hts, ho, hh, hl, hc


def we_compute(ts, c, h, l, htf, p, zone_ms):
    """htf = (hts, hh, hl, hc) -> (zone[], ema[], call[], put[]) je Signal-Kerze (alle Kerzen sind geschlossen)."""
    hts, hh, hl, hc = htf
    n = len(c)
    S = we_htf_series(hh, hl, hc, p)
    warm = _we_warm_htf(p)
    ema = _ema_series(c, p["ema_len"])
    zone = [0] * n
    live = p["zone_mode"] == "live"
    cur_b, ph, pl = None, 0.0, 0.0
    for i in range(n):
        b = ts[i] // zone_ms * zone_ms
        if b != cur_b:
            cur_b, ph, pl = b, h[i], l[i]
        else:
            ph, pl = max(ph, h[i]), min(pl, l[i])
        k1 = bisect_left(hts, b) - 1       # letzte Zeitrahmen-Kerze, die VOR dieser Signalkerze begonnen und abgeschlossen ist
        if k1 < warm:
            continue
        zone[i] = _we_dev_state(S, k1, ph, pl, c[i], p) if live else S["state"][k1]
    call, put = [False] * n, [False] * n
    pc = pp = False
    for i in range(n):
        cc = zone[i] == 1 and c[i] > ema[i]
        pcond = zone[i] == -1 and c[i] < ema[i]
        call[i] = cc and not pc
        put[i] = pcond and not pp
        pc, pp = cc, pcond
    return zone, ema, call, put


def _we_levels(direction, entry, size, p):
    if not size or size <= 0:
        return None, None
    sgn = 1 if direction == "long" else -1
    sl = entry - sgn * p["sl_usd"] / size if p["sl_usd"] > 0 else None
    tp = entry + sgn * p["tp_usd"] / size if p["tp_usd"] > 0 else None
    return sl, tp


def _allowed(target, p):
    return not ((target == "long" and p["direction"] == "short_only") or (target == "short" and p["direction"] == "long_only"))


def _signal_exit(pos, zone_i, ema_i, price, p):
    """Ausstieg OHNE Gegensignal je nach Modus -> Grund oder None."""
    m = p["exit_mode"]
    if m == "ema_cross" and ((pos == "long" and price < ema_i) or (pos == "short" and price > ema_i)):
        return "WE-EMA-X"
    if m == "zone_end" and zone_i != (1 if pos == "long" else -1):
        return "WE-ZONE-END"
    return None


# ============================================================================
# LIVE
# ============================================================================
def _we_reset_state(st):
    st["we_sl_price"] = None
    st["we_tp_price"] = None


async def check_we_sl_tp(symbol, price):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    pos = st["position"]
    if pos is None or price is None:
        return
    sl, tp = st.get("we_sl_price"), st.get("we_tp_price")
    if sl is not None and ((pos == "long" and price <= sl) or (pos == "short" and price >= sl)):
        debug_log(f"🚪 [{symbol}] WAE-EMA SL: {pos.upper()} @ {price} (Ziel war {round(sl, 6)})")
        await execute_exit(symbol, price, "SL")
        if st["position"] is None:
            st["we_sl_cooldown_until"] = time.time() + cfg.get("we_sl_cooldown_seconds", 30)
            _we_reset_state(st)
        return
    if tp is not None and ((pos == "long" and price >= tp) or (pos == "short" and price <= tp)):
        debug_log(f"🎯 [{symbol}] WAE-EMA TP: {pos.upper()} @ {price} (Ziel war {round(tp, 6)})")
        await execute_exit(symbol, price, "TP")
        if st["position"] is None:
            _we_reset_state(st)


def _set_levels(st, cfg, target, price):
    entry = st.get("avg_entry_price") or price
    st["we_sl_price"], st["we_tp_price"] = _we_levels(target, entry, st.get("total_coin_size") or 0, _we_params(cfg))


async def check_we_signal(symbol, call_i, put_i, zone_i, ema_i, close_i, price):
    """Auswertung der neu geschlossenen Signalkerze. Prioritaet: Gegensignal (Flip) vor EMA-/Zonen-Ausstieg."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or price is None:
        return
    p = _we_params(cfg)
    pos = st["position"]
    tri = "long" if call_i else "short" if put_i else None
    if pos is None:
        if tri is None or not _allowed(tri, p) or time.time() < st.get("we_sl_cooldown_until", 0.0):
            return
        debug_log(f"📡 [{symbol}] WAE-EMA Einstieg: {'CALL' if tri == 'long' else 'PUT'} ({tri.upper()}) @ {price}")
        await execute_entry(symbol, tri, price, is_add_on=False)
        if st["position"] is not None:
            _we_reset_state(st)
            _set_levels(st, cfg, tri, price)
        return
    if p["exit_mode"] == "sltp_only":
        return
    if tri is not None and tri != pos:
        if _allowed(tri, p):
            debug_log(f"🔄 [{symbol}] WAE-EMA Flip: {pos.upper()} -> {tri.upper()} @ {price}")
            if await execute_reverse(symbol, price, "WE-FLIP", size_multiplier=1.0):
                _we_reset_state(st)
                _set_levels(st, cfg, tri, price)
        else:
            debug_log(f"🚪 [{symbol}] WAE-EMA Gegensignal (Richtung gesperrt): {pos.upper()} schließen @ {price}")
            await execute_exit(symbol, price, "WE-EXIT-DIR")
            if st["position"] is None:
                _we_reset_state(st)
        return
    reason = _signal_exit(pos, zone_i, ema_i, close_i, p)
    if reason:
        debug_log(f"🎯 [{symbol}] WAE-EMA {reason}: {pos.upper()} beendet @ {price}")
        await execute_exit(symbol, price, reason)
        if st["position"] is None:
            _we_reset_state(st)


async def we_poll_loop(symbol):
    """WAE-EMA live: wertet NUR abgeschlossene Signal-Kerzen aus; die Zone kommt aus dem hoeheren Zeitrahmen."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0
    warned_tf = False
    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "wae_ema" and cfg["bot_active"] and b["state"].get("session_started"):
                resolution = cfg.get("we_resolution", "5m")
                p = _we_params(cfg)
                res_s, zone_s = _res_seconds(resolution), _res_seconds(p["zone_tf"])
                st = b["state"]
                if res_s >= zone_s:
                    if not warned_tf:
                        warned_tf = True
                        debug_log(f"⚠️ [{symbol}] WAE-EMA: Signal-Zeitrahmen ({resolution}) muss KLEINER sein als der Zonen-Zeitrahmen ({p['zone_tf']}) - keine Signale.")
                else:
                    warned_tf = False
                    zone_ms = zone_s * 1000
                    min_needed = p["ema_len"] * 5 + 10
                    needed_bars = min(1000, max(min_needed * 2, int(zone_s / res_s) * 2 + p["ema_len"] * 6, 300))
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
                        await check_we_sl_tp(symbol, st["last_price"])

                    if closed_ts and len(closed_c) > min_needed:
                        age = (now * 1000 - closed_ts[-1]) / 1000
                        if age > 300 + res_s:
                            debug_log(f"⚠️ [{symbol}] WAE-EMA: letzte Kerze wirkt veraltet ({round(age)}s alt, Auflösung {resolution}) - überspringe Durchlauf.")
                            closed_ts = None
                        elif st["last_price"] is not None and closed_c[-1] and abs(closed_c[-1] - st["last_price"]) / st["last_price"] * 100 > 2.0:
                            debug_log(f"⚠️ [{symbol}] WAE-EMA: Kerzendaten unplausibel (Kerze {closed_c[-1]} vs. Live {st['last_price']}) - überspringe Durchlauf.")
                            closed_ts = None

                    if closed_ts and len(closed_c) > min_needed and (last_processed_ts is None or closed_ts[-1] > last_processed_ts):
                        hdata = await fetch_candles_binance_multi(symbol, p["zone_tf"], count_back=min(1000, _we_warm_htf(p) + 60), market_type=cfg.get("binance_market_type", "spot"))
                        if not hdata or len(hdata[4]) < _we_warm_htf(p) + 5:
                            if due_heartbeat:
                                last_heartbeat = now
                                debug_log(f"⏳ [{symbol}] WAE-EMA wartet: zu wenig {p['zone_tf']}-Kerzen für die Zone erhalten")
                        else:
                            htf = (hdata[0], hdata[2], hdata[3], hdata[4])
                            zone, ema, call, put = await asyncio.to_thread(we_compute, closed_ts, closed_c, closed_h, closed_l, htf, p, zone_ms)
                            if due_heartbeat:
                                last_heartbeat = now
                                zname = {1: "grün", -1: "rot", 0: "keine"}[zone[-1]]
                                debug_log(f"💓 [{symbol}] WAE-EMA aktiv: {resolution}, Zone {p['zone_tf']} ({p['zone_mode']}) = {zname}, EMA{p['ema_len']}={round(ema[-1], 6)}, "
                                          f"Ausstieg={p['exit_mode']}, SL=${p['sl_usd']} TP=${p['tp_usd']}, Preis={closed_c[-1]}")
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
                                await check_we_signal(symbol, call[idx], put[idx], zone[idx], ema[idx], closed_c[idx], price_i)
                    elif due_heartbeat and not closed_ts:
                        last_heartbeat = now
                        debug_log(f"⏳ [{symbol}] WAE-EMA wartet: keine Kerzen erhalten (Auflösung {resolution})")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] WAE-EMA-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        await asyncio.sleep(_poll_delay(b["config"].get("we_resolution", "5m"), b["state"]))


# ============================================================================
# BACKTEST
# ============================================================================
def _we_simulate(candles, cfg, zone, ema, call, put, start_i, p):
    """Backtest-Gegenstueck zu check_we_signal / check_we_sl_tp: Einstieg/Flip zum Schlusskurs der Signalkerze, SL/TP ab der
    naechsten Kerze ueber Hoch/Tief (SL gewinnt bei Gleichzeitigkeit), Cooldown nach SL, Gegensignal vor EMA-/Zonen-Ausstieg."""
    ts, h, l, c = candles[0], candles[2], candles[3], candles[4]
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    cooldown_ms = cfg.get("we_sl_cooldown_seconds", 30) * 1000
    position, trades, cool_until = None, [], None

    def open_pos(direction, price, i):
        size = (margin * leverage) / price
        sl, tp = _we_levels(direction, price, size, p)
        return {"dir": direction, "entry": price, "size": size, "entry_i": i, "sl": sl, "tp": tp}

    for i in range(max(start_i, 1), n):
        if position is not None and position["entry_i"] < i:
            d, e, sl, tp = position["dir"], position["entry"], position["sl"], position["tp"]
            if sl is not None and ((d == "long" and l[i] <= sl) or (d == "short" and h[i] >= sl)):
                _bt_close_trade(trades, d, e, sl, position["size"], i, position["entry_i"], "SL", ts=ts)
                position = None
                cool_until = ts[i] + cooldown_ms
            elif tp is not None and ((d == "long" and h[i] >= tp) or (d == "short" and l[i] <= tp)):
                _bt_close_trade(trades, d, e, tp, position["size"], i, position["entry_i"], "TP", ts=ts)
                position = None
        tri = "long" if call[i] else "short" if put[i] else None
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
            _bt_close_trade(trades, d, position["entry"], price, position["size"], i, position["entry_i"], "WE-FLIP" if ok else "WE-EXIT-DIR", ts=ts)
            position = open_pos(tri, price, i) if ok else None
        else:
            reason = _signal_exit(d, zone[i], ema[i], price, p)
            if reason:
                _bt_close_trade(trades, d, position["entry"], price, position["size"], i, position["entry_i"], reason, ts=ts)
                position = None
    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)
    return trades



# ============================================================================
# KERZEN-CACHE fuer Backtest/Monte-Carlo: nicht bei jedem Klick neu von Binance holen
#  - eine Kopie je (Coin, Zeitrahmen), kompakt als array gespeichert (4x weniger Speicher als Listen)
#  - aktuell genug (< 10 Min) -> sofort aus dem Cache; aelter -> nur die fehlenden neuen Kerzen nachladen und anhaengen
#  - groeberer Zeitrahmen (z.B. 15m, 1h) wird aus einem feineren Cache (z.B. 5m, 1m) berechnet - ohne Abruf
# ============================================================================
_WE_CACHE = OrderedDict()   # (coin, zeitrahmen, markt) -> {"data": (ts, o, h, l, c, v) als array, "fetched_at": ...}
WE_CACHE_MAX = 3
WE_CACHE_FRESH_S = 600
_WE_INFLIGHT = {}           # laufende Abrufe (verhindert doppeltes Laden bei erneutem Klick)
_NATIVE_RES_S = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600, "8h": 28800, "12h": 43200, "1d": 86400}


def _pack(cs):
    return (array("q", [int(x) for x in cs[0]]),) + tuple(array("d", x) for x in cs[1:6])


def _lists(data, lo=0):
    return tuple(list(x[lo:]) for x in data)


def _slice(data, days, max_candles):
    ts = data[0]
    lo = max(bisect_left(ts, ts[-1] - int(days * 86_400_000)), len(ts) - max_candles, 0)
    return _lists(data, lo)


def _covers(entry, days, res_s, max_candles):
    """start_req = bis wohin zurueck der damalige Abruf angefragt war. Hat ein Coin kuerzere Historie (z.B. neu gelistet),
    liegt die erste Kerze spaeter - das darf NICHT als 'nicht abgedeckt' gelten, sonst wuerde jedes Mal neu geladen."""
    need_start = time.time() * 1000 - days * 86_400_000
    tol = max(3 * res_s * 1000, days * 86_400_000 * 0.03)
    return len(entry["data"][0]) > 100 and (entry.get("start_req", entry["data"][0][0]) <= need_start + tol or len(entry["data"][0]) >= max_candles)


def _resample(data, res_s):
    """Feinere (epoch-ausgerichtete) Kerzen -> groebere. Eine angefangene erste Kerze wird verworfen."""
    ms = res_s * 1000
    ts, o, h, l, c, v = data
    out = ([], [], [], [], [], [])
    cur = None
    for i in range(len(ts)):
        b = ts[i] // ms * ms
        if b != cur:
            cur = b
            out[0].append(b); out[1].append(o[i]); out[2].append(h[i]); out[3].append(l[i]); out[4].append(c[i]); out[5].append(v[i])
        else:
            if h[i] > out[2][-1]:
                out[2][-1] = h[i]
            if l[i] < out[3][-1]:
                out[3][-1] = l[i]
            out[4][-1] = c[i]; out[5][-1] += v[i]
    if ts and ts[0] % ms != 0 and out[0]:
        out = tuple(x[1:] for x in out)
    return out


async def _topup(key, symbol, res, market):
    """Haengt die seit dem Abruf neu entstandenen Kerzen an. -> True bei Erfolg."""
    entry = _WE_CACHE.get(key)
    if not entry:
        return False
    res_s = _NATIVE_RES_S[res]
    data = entry["data"]
    gap_days = (time.time() * 1000 - data[0][-1]) / 86_400_000 + 3 * res_s / 86400
    if gap_days * 86400 / res_s > 4000:
        return False                       # zu lange her -> lieber komplett neu holen
    new, err = await fetch_historical_candles_binance_vol(symbol, res, gap_days, 5000, market_type=market)
    if err or not new or len(new[0]) == 0:
        return False
    keep = bisect_left(data[0], new[0][0])   # die damals noch laufende letzte Kerze wird durch die neue ersetzt
    merged = tuple(list(data[i][:keep]) + list(new[i]) for i in range(6))
    entry["data"] = _pack(merged)
    entry["fetched_at"] = time.time()
    return True


async def _we_candles(symbol, res, days, max_candles, market_type="spot"):
    """-> (candles, err, quelle) mit quelle in {"cache", "abgeleitet", "neu"}."""
    if res not in _NATIVE_RES_S:   # eigene Minuten / Sekunden: bestehender Cache der Bot-Basis
        cs, err = await _fetch_cached_mo7_backtest_candles(symbol, res, days, max_candles, market_type=market_type)
        return cs, err, "neu"
    res_s = _NATIVE_RES_S[res]
    key = (symbol, res, market_type)
    entry = _WE_CACHE.get(key)
    if entry and _covers(entry, days, res_s, max_candles):
        if time.time() - entry["fetched_at"] > WE_CACHE_FRESH_S:
            await _topup(key, symbol, res, market_type)
        entry = _WE_CACHE.get(key)
        _WE_CACHE.move_to_end(key)
        debug_log(f"⚡ WAE-Kerzen-Cache: {symbol} {res} aus dem Cache ({len(entry['data'][0])} Kerzen)")
        return _slice(entry["data"], days, max_candles), None, "cache"
    for (sym2, res2, mk2), e2 in list(_WE_CACHE.items()):     # aus feinerem Cache ableiten
        fs = _NATIVE_RES_S[res2]
        if sym2 == symbol and mk2 == market_type and fs < res_s and res_s % fs == 0 and _covers(e2, days, fs, max_candles * res_s // fs):
            if time.time() - e2["fetched_at"] > WE_CACHE_FRESH_S:
                await _topup((sym2, res2, mk2), symbol, res2, market_type)
            data = _resample(_lists(_WE_CACHE[(sym2, res2, mk2)]["data"]), res_s)
            if len(data[0]) > 100:
                packed = _pack(data)
                _WE_CACHE[key] = {"data": packed, "fetched_at": _WE_CACHE[(sym2, res2, mk2)]["fetched_at"], "start_req": _WE_CACHE[(sym2, res2, mk2)].get("start_req")}
                debug_log(f"⚡ WAE-Kerzen-Cache: {symbol} {res} aus {res2} abgeleitet (kein Binance-Abruf)")
                while len(_WE_CACHE) > WE_CACHE_MAX:
                    _WE_CACHE.popitem(last=False)
                return _slice(packed, days, max_candles), None, "abgeleitet"
    # Der Abruf laeuft als eigene Aufgabe, die ein Abbruch der Anfrage (Dashboard-Zeitlimit 90 s) ueberlebt: bei langen Zeitraeumen
    # braucht Binance laenger als das Limit - ohne das wuerde NIE etwas im Cache landen und jeder Klick fiele wieder ins Zeitlimit.
    fk = (key, days, max_candles)
    task = _WE_INFLIGHT.get(fk)
    if task is None:
        task = asyncio.ensure_future(_we_cold_fetch(key, symbol, res, days, max_candles, market_type))
        _WE_INFLIGHT[fk] = task
        task.add_done_callback(lambda _t, _fk=fk: _WE_INFLIGHT.pop(_fk, None))
    else:
        debug_log(f"⏳ WAE-Kerzen-Cache: {symbol} {res} {days} Tage werden schon geladen - warte darauf")
    try:
        cs, err = await asyncio.shield(task)
    except asyncio.CancelledError:
        debug_log(f"⏱️ WAE-Kerzen-Cache: {symbol} {res} {days} Tage - Anfrage abgebrochen, der Abruf läuft im Hintergrund weiter; "
                  f"nach einigen Sekunden den Test nochmal starten (dann aus dem Cache)")
        raise
    return cs, err, "neu"


async def _we_cold_fetch(key, symbol, res, days, max_candles, market_type):
    try:
        cs, err = await fetch_historical_candles_binance_vol(symbol, res, days, max_candles, market_type=market_type)
    except Exception as e:                      # nie unbehandelt in der Aufgabe lassen
        return None, f"Kerzen-Abruf fehlgeschlagen: {e}"
    if cs and len(cs[0]) > 100:
        _WE_CACHE[key] = {"data": _pack(cs), "fetched_at": time.time(), "start_req": time.time() * 1000 - days * 86_400_000}
        debug_log(f"📡 WAE-Kerzen-Cache: {symbol} {res} {days} Tage neu von Binance geladen ({len(cs[0])} Kerzen)")
        _WE_CACHE.move_to_end(key)
        while len(_WE_CACHE) > WE_CACHE_MAX:
            _WE_CACHE.popitem(last=False)
    return cs, err


def _lead_days(p, zone_s, resolution):
    """Vorlauf (Tage), der ZUSAETZLICH zum Testzeitraum geladen wird, damit die Zone (EMA/RMA) schon beim ersten Test-Tag eingeschwungen ist.
    Sekunden-Zeitrahmen holen die Zonen-Kerzen separat, dort ist kein Vorlauf der Signal-Kerzen noetig."""
    if resolution in SUB_MINUTE_RESOLUTIONS:
        return 0.0
    return ((_we_warm_htf(p) + 5) * zone_s) / 86400 + 0.25


def _window_start(ts, days):
    """Index der ersten Kerze im Testfenster 'letzte N Tage'."""
    return bisect_left(ts, ts[-1] - int(days * 86_400_000))


def _first_valid_index(zone):
    for i, z in enumerate(zone):
        if z != 0:
            return max(i - 1, 1)
    return len(zone)


async def _we_get_htf(symbol, cfg, p, resolution, ts, o, h, l, c):
    """Zonen-Kerzen (hts, hh, hl, hc). Normalfall: aus den Signal-Kerzen zusammengefasst. Bei Sekunden-Zeitrahmen reichen die (auf 5000
    begrenzten) Signal-Kerzen nicht fuer die Einlaufzeit der Zone -> dann werden die Zonen-Kerzen separat und mit Vorlauf geholt."""
    zone_s = _res_seconds(p["zone_tf"])
    if resolution not in SUB_MINUTE_RESOLUTIONS:
        hts, ho, hh, hl, hc = await asyncio.to_thread(we_aggregate, ts, o, h, l, c, zone_s * 1000)
        return (hts, hh, hl, hc), None
    span_s = (ts[-1] - ts[0]) / 1000
    htf_days = int((span_s + (_we_warm_htf(p) + 30) * zone_s) / 86400) + 2
    hc_data, err, _src = await _we_candles(symbol, p["zone_tf"], htf_days, 100_000, market_type=cfg.get("binance_market_type", "spot"))
    if err:
        return None, f"Zonen-Kerzen ({p['zone_tf']}) konnten nicht geladen werden: {err}"
    return (hc_data[0], hc_data[2], hc_data[3], hc_data[4]), None


async def run_we_backtest(symbol, cfg, days, exclude_top_n=1):
    max_candles = BACKTEST_MAX_CANDLES.get("wae_ema", 100_000)
    resolution = cfg.get("we_resolution", "5m")
    if resolution in SUB_MINUTE_RESOLUTIONS:
        max_candles = min(max_candles, 5000)
    p = _we_params(cfg)
    res_s, zone_s = _res_seconds(resolution), _res_seconds(p["zone_tf"])
    if res_s >= zone_s:
        return {"error": f"Der Signal-Zeitrahmen ({resolution}) muss kleiner sein als der Zonen-Zeitrahmen ({p['zone_tf']})."}
    lead = _lead_days(p, zone_s, resolution)
    t_fetch = time.time()
    candles, err, src = await _we_candles(symbol, resolution, days + lead, max_candles, market_type=cfg.get("binance_market_type", "spot"))
    t_fetch = time.time() - t_fetch
    if err:
        return {"error": err}
    need_candles = (_we_warm_htf(p) + 20) * zone_s // res_s if resolution not in SUB_MINUTE_RESOLUTIONS else 200
    if not candles or len(candles[4]) < need_candles + 50:
        have = len(candles[4]) if candles else 0
        return {"error": f"Zu wenig Kerzen: die Zone braucht ca. {_we_warm_htf(p)} {p['zone_tf']}-Kerzen Einlaufzeit (~{need_candles} {resolution}-Kerzen), erhalten {have}. "
                         f"Mehr Tage wählen (mind. ~{int(need_candles * res_s / 86400) + 3}) oder die Einlaufzeit-Parameter verkleinern (Dead-Zone-Länge)."}
    ts, o, h, l, c = candles[0], candles[1], candles[2], candles[3], candles[4]
    zone_ms = zone_s * 1000

    htf, herr = await _we_get_htf(symbol, cfg, p, resolution, ts, o, h, l, c)
    if herr:
        return {"error": herr}
    t_calc = time.time()
    zone, ema, call, put = await asyncio.to_thread(we_compute, ts, c, h, l, htf, p, zone_ms)
    start_i = max(_first_valid_index(zone), _window_start(ts, days))   # gehandelt wird nur im Fenster "letzte N Tage" (Vorlauf nur zum Einlaufen)
    trades = _we_simulate(candles, cfg, zone, ema, call, put, start_i, p)
    t_calc = time.time() - t_calc
    stats = summarize_backtest_trades(trades, exclude_top_n)
    usable_days = (ts[-1] - ts[start_i]) / 86_400_000 if start_i < len(ts) else 0
    actual_days = usable_days
    return {
        "symbol": symbol, "entry_mode": "wae_ema", "resolution": resolution,
        "requested_days": days, "actual_days_covered": round(actual_days, 1),
        "candles_processed": len(ts) - start_i, "candle_cap": max_candles, "cache_used": src != "neu",
        "note": f"Kerzen holen {t_fetch:.1f}s ({ {'cache': 'Cache', 'abgeleitet': 'aus feinerem Zeitrahmen berechnet', 'neu': 'neu von Binance'}[src] }), Berechnung {t_calc:.1f}s. "
                f"Getestet wird das Fenster der letzten {round(usable_days, 1)} Tage (die Zone läuft vorher ~{round(lead, 1)} Tage ein); {p['zone_mode']}-Modus, Ausstieg: {p['exit_mode']}.",
        "signals_buy": sum(call), "signals_sell": sum(put),
        "stats": stats,
        "stats_long": summarize_backtest_trades([t for t in trades if t["dir"] == "long"], exclude_top_n),
        "stats_short": summarize_backtest_trades([t for t in trades if t["dir"] == "short"], exclude_top_n),
        "trades": trades,
    }


# ============================================================================
# CHART
# ============================================================================
async def we_chart_payload(symbol, cfg, ts, o, h, l, c, tf, max_bars=1000):
    """Kerzen + CALL/PUT-Marker (nur geschlossene Kerzen). Die Zone kommt aus einem separat geholten Zeitrahmen."""
    p = _we_params(cfg)
    n = len(c)
    res_s, zone_s = _res_seconds(tf), _res_seconds(p["zone_tf"])
    if n < 5:
        return None
    markers = []
    if res_s < zone_s:
        hdata = await fetch_candles_binance_multi(symbol, p["zone_tf"], count_back=min(1000, _we_warm_htf(p) + int(n * res_s / zone_s) + 40), market_type=cfg.get("binance_market_type", "spot"))
        if hdata and len(hdata[4]) > _we_warm_htf(p) + 5:
            htf = (hdata[0], hdata[2], hdata[3], hdata[4])
            zone, ema, call, put = await asyncio.to_thread(we_compute, ts[:-1], c[:-1], h[:-1], l[:-1], htf, p, zone_s * 1000)
            call, put = call + [False], put + [False]
            for i in range(max(0, n - max_bars), n):
                if call[i]:
                    markers.append({"time": ts[i] // 1000, "kind": "long", "price": c[i]})
                if put[i]:
                    markers.append({"time": ts[i] // 1000, "kind": "short", "price": c[i]})
    off = max(0, n - max_bars)
    candles = [{"time": ts[i] // 1000, "open": o[i], "high": h[i], "low": l[i], "close": c[i]} for i in range(off, n)]
    return {"candles": candles, "wt1": [], "wt2": [], "markers": markers, "levels": [], "entry_mode": "we", "wt1v": 0, "wt2v": 0, "bars": n}


# ============================================================================
# MONTE-CARLO: zufaellige Kombinationen der AUSGEWAEHLTEN WAE-/EMA-Werte, Rest bleibt wie in den Einstellungen
# ============================================================================
import random

WE_MC_PARAMS = {   # kurzer Schluessel -> (Config-Schluessel, Typ, Nachkommastellen)
    "ema_len": ("we_ema_len", int, 0), "sens": ("we_sensitivity", int, 0),
    "fast": ("we_fast_len", int, 0), "slow": ("we_slow_len", int, 0), "chan": ("we_channel_len", int, 0),
    "bbm": ("we_bb_mult", float, 1), "dlen": ("we_dead_len", int, 0), "dmult": ("we_dead_mult", float, 1),
}
WE_MC_MAX_RUNS = 1000
WE_MC_MIN_RELIABLE_TRADES = 5
WE_MC_TEST_FRACTION = 0.3
WE_MC_MAX_CANDLES = 80_000
WE_MC_TIME_BUDGET = 70       # Sekunden; der Aufruf im Dashboard bricht nach 90 s ab - lieber weniger Kombinationen als gar kein Ergebnis


def _we_mc_compute(candles, cfg, combos, base_p, htf, zone_ms, start_i, exclude_top_n, deadline):
    ts, h, l, c = candles[0], candles[2], candles[3], candles[4]
    n = len(c)
    split_ts = ts[start_i + int((n - start_i) * (1 - WE_MC_TEST_FRACTION))]

    def run_one(over):
        p = dict(base_p, **over)
        zone, ema, call, put = we_compute(ts, c, h, l, htf, p, zone_ms)
        trades = _we_simulate(candles, cfg, zone, ema, call, put, start_i, p)
        st = summarize_backtest_trades(trades, exclude_top_n)
        tr = summarize_backtest_trades([t for t in trades if t.get("entry_ts", 0) < split_ts], exclude_top_n)
        te = summarize_backtest_trades([t for t in trades if t.get("entry_ts", 0) >= split_ts], exclude_top_n)
        row = {WE_MC_PARAMS[k][0]: p[k] for k in WE_MC_PARAMS}
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


async def run_we_montecarlo(symbol, cfg, days, runs, vary, exclude_top_n=1, seed=None, max_dd_usd=0.0):
    """vary: {'sens': (min, max), 'dmult': (min, max), ...} - nur diese Werte werden zufaellig gezogen, alle anderen bleiben wie in der Config.
    Die aktuellen Einstellungen laufen IMMER als Referenz. Alle Kombinationen handeln im selben Zeitfenster (ab der Einlaufzeit der laengsten
    Zone-Einstellung), sonst waeren sie nicht vergleichbar."""
    base_p = _we_params(cfg)
    resolution = cfg.get("we_resolution", "5m")
    res_s, zone_s = _res_seconds(resolution), _res_seconds(base_p["zone_tf"])
    if res_s >= zone_s:
        return {"error": f"Der Signal-Zeitrahmen ({resolution}) muss kleiner sein als der Zonen-Zeitrahmen ({base_p['zone_tf']})."}
    if not vary:
        return {"error": "Bitte mindestens einen Wert zum Variieren auswählen."}
    ranges = {}
    try:
        for k, (a, b) in vary.items():
            if k not in WE_MC_PARAMS:
                return {"error": f"Unbekannter Wert '{k}'."}
            typ = WE_MC_PARAMS[k][1]
            lo, hi = sorted((typ(a), typ(b)))
            if lo <= 0:
                return {"error": "Alle Bereiche müssen größer als 0 sein."}
            ranges[k] = (lo, hi)
    except (TypeError, ValueError):
        return {"error": "Ungültige Bereiche für die Parameter."}
    max_candles = BACKTEST_MAX_CANDLES.get("wae_ema", 100_000)
    if resolution in SUB_MINUTE_RESOLUTIONS:
        max_candles = min(max_candles, 5000)
    p_max = dict(base_p, **{k: ranges[k][1] for k in ranges})
    lead = _lead_days(p_max, zone_s, resolution)
    candles, err, src = await _we_candles(symbol, resolution, days + lead, max_candles, market_type=cfg.get("binance_market_type", "spot"))
    if err:
        return {"error": err}
    n = len(candles[4]) if candles else 0
    note = {"cache": "Kerzen aus dem Cache.", "abgeleitet": "Kerzen aus einem feineren Zeitrahmen im Cache berechnet.", "neu": "Kerzen neu von Binance geladen."}[src]
    if n > WE_MC_MAX_CANDLES:
        candles = tuple(x[-WE_MC_MAX_CANDLES:] for x in candles)
        note = ((note + " ") if note else "") + f"Hinweis: aus Speichergründen wurden nur die jüngsten {WE_MC_MAX_CANDLES} von {n} Kerzen getestet."
        n = WE_MC_MAX_CANDLES
    ts, o, h, l, c = candles[0], candles[1], candles[2], candles[3], candles[4]
    zone_ms = zone_s * 1000
    max_warm = max(_we_warm_htf(dict(base_p, **{k: ranges[k][1] for k in ranges})), _we_warm_htf(base_p))
    htf, herr = await _we_get_htf(symbol, cfg, base_p, resolution, ts, o, h, l, c)
    if herr:
        return {"error": herr}
    hts, hh, hl, hc = htf
    start_i = len(ts)
    for i in range(len(ts)):
        if bisect_left(hts, ts[i] // zone_ms * zone_ms) - 1 >= max_warm:
            start_i = max(i - 1, 1)
            break
    start_i = max(start_i, _window_start(ts, days)) if start_i < len(ts) else start_i   # gehandelt wird nur im Fenster "letzte N Tage" - genau wie im Backtest
    if n < 400 or n - start_i < 300:
        return {"error": f"Zu wenig Kerzen: die Zone braucht bis zu {max_warm} {base_p['zone_tf']}-Kerzen Einlaufzeit, danach sollte noch genug Zeitraum zum Testen übrig sein. "
                         f"Mehr Tage wählen (Richtwert: mind. {int(max_warm * zone_s / 86400 / 0.4) + 1}) oder kleinere Dead-Zone-Länge."}
    runs = max(1, min(WE_MC_MAX_RUNS, int(runs)))
    rng = random.Random(seed)

    def draw(k):
        lo, hi = ranges[k]
        typ, dec = WE_MC_PARAMS[k][1], WE_MC_PARAMS[k][2]
        return rng.randint(lo, hi) if typ is int else round(rng.uniform(lo, hi), dec)

    base_key = tuple(base_p[k] for k in sorted(ranges))
    seen, combos = {base_key}, []
    for _ in range(runs * 30):
        if len(combos) >= runs:
            break
        over = {k: draw(k) for k in ranges}
        eff = dict(base_p, **over)
        if eff["fast"] >= eff["slow"]:
            continue
        key = tuple(over[k] for k in sorted(ranges))
        if key not in seen:
            seen.add(key)
            combos.append(over)
    deadline = time.time() + WE_MC_TIME_BUDGET
    results, baseline = await asyncio.to_thread(_we_mc_compute, candles, cfg, combos, base_p, htf, zone_ms, start_i, exclude_top_n, deadline)
    reliable = [r for r in results if r["trades"] >= WE_MC_MIN_RELIABLE_TRADES]
    robust = [r for r in reliable if r["pnl_train"] > 0 and r["pnl_test"] > 0 and r["trades_test"] >= 3]
    best = sorted(results, key=lambda r: (r["trades"] >= WE_MC_MIN_RELIABLE_TRADES, r["total_pnl_usd"]), reverse=True)
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
        "symbol": symbol, "resolution": resolution, "zone_tf": base_p["zone_tf"], "zone_mode": base_p["zone_mode"], "exit_mode": base_p["exit_mode"],
        "requested_days": days, "actual_days_covered": round((ts[-1] - ts[0]) / 86_400_000, 1), "usable_days": round(usable_days, 1),
        "candles_processed": n, "min_reliable_trades": WE_MC_MIN_RELIABLE_TRADES,
        "combos_tested": len(results), "combos_requested": len(combos), "test_fraction_pct": int(WE_MC_TEST_FRACTION * 100),
        "varied": [WE_MC_PARAMS[k][0] for k in WE_MC_PARAMS if k in ranges], "note": note,
        "baseline": baseline,
        "profitable_pct": round(sum(1 for x in pnls if x > 0) / len(pnls) * 100, 1) if pnls else 0,
        "median_pnl_usd": round(pnls[len(pnls) // 2], 2) if pnls else 0,
        "robust_count": len(robust), "lowdd_count": len(lowdd), "max_dd_limit": max_dd_usd,
        "lowdd_results": lowdd[:30], "robust_results": robust_sorted[:30], "results": best[:30], "worst_results": worst[:20],
    }
