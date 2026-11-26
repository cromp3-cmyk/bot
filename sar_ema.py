"""
sar_ema.py - eigene Strategie "SAR + EMA10 Wechsel" (entry_mode "sar_ema", Config-Praefix sa_*).

Portiert aus dem TradingView-Indikator "SAR + EMA10 Signale" (Pine v6), aber NUR als Wechsel-System:
  * Parabolic SAR (Start / Schritt / Maximum, Pine-ta.sar) bestimmt den Trend: SAR unter dem Schluss = Aufwaerts.
  * Kippt der SAR-Trend, gilt ein Fenster von N Kerzen (die erste Kerze des neuen Trends zaehlt als Kerze 1).
  * In diesem Fenster ist das Signal die ERSTE Kerze, die ueber (Long) bzw. unter (Short) der EMA schliesst -
    sie darf die EMA kreuzen oder schon darueber/darunter liegen. Pro SAR-Trend hoechstens ein Signal.
  * Wechsel: ein Gegensignal schliesst die Position und eroeffnet SOFORT die Gegenrichtung (Reverse).
  * SAR-Ende (wie im Original, Option "Trade beenden, sobald SAR gegen die Position dreht", Standard AN): kippt der SAR gegen die
    Position und es gibt auf dieser Kerze kein Gegensignal, wird die Position geschlossen und es bleibt flach bis zum naechsten Signal.
Optional: fester Dollar-SL / Dollar-TP (jeweils 0 = aus). Standard: nur der Wechsel.
"""

import asyncio
import time
import traceback

from bot_core import BOTS, debug_log, execute_entry, execute_exit
from strategies import (
    SUB_MINUTE_RESOLUTIONS, get_seconds_candles, fetch_candles_binance_multi, _ema_series,
    summarize_backtest_trades, _fetch_cached_mo7_backtest_candles, BACKTEST_MAX_CANDLES, _bt_close_trade,
)


def _res_seconds(res):
    """Kerzenlaenge in Sekunden ("10s", "1m", "4h", "1d" ...)."""
    if res in SUB_MINUTE_RESOLUTIONS:
        return SUB_MINUTE_RESOLUTIONS[res]
    try:
        n, u = int(res[:-1]), res[-1]
        return n * {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}.get(u, 60)
    except (ValueError, TypeError, IndexError):
        return 60


def _poll_delay(res, st):
    """Abfrage-Takt: 1 s bei offener Position, bei Sekunden-Kerzen und rund um den Kerzenschluss, sonst bis kurz davor schlafen."""
    per = _res_seconds(res)
    if st.get("position") is not None or per <= 60:
        return 1.0
    into = time.time() % per
    if into < 6:
        return 1.0
    return max(1.0, min(5.0, per - into - 0.5))


def _sa_params(cfg):
    return {
        "start": max(0.001, float(cfg.get("sa_start", 0.02) or 0.02)),
        "inc": max(0.001, float(cfg.get("sa_inc", 0.02) or 0.02)),
        "max": max(0.001, float(cfg.get("sa_max", 0.2) or 0.2)),
        "ema_len": max(1, int(cfg.get("sa_ema_len", 10) or 10)),
        "max_bars": max(1, int(cfg.get("sa_max_bars", 3) or 3)),
        "exit_on_flip": bool(cfg.get("sa_exit_on_flip", True)),
        "sl_usd": max(0.0, float(cfg.get("sa_sl_usd", 0.0) or 0.0)),
        "tp_usd": max(0.0, float(cfg.get("sa_tp_usd", 0.0) or 0.0)),
    }


def _sa_warmup(p):
    return max(3 * p["ema_len"], 50) + 5


# ============================================================================
# RECHNUNG (rein)
# ============================================================================
def sar_series(h, l, c, start, inc, mx):
    """Parabolic SAR wie Pine ta.sar (Referenz-Code aus der Pine-Dokumentation). Index 0 = None."""
    n = len(c)
    out = [None] * n
    if n < 2:
        return out
    result = max_min = accel = None
    is_below = False
    for i in range(1, n):
        first_trend_bar = False
        if i == 1:
            if c[1] > c[0]:
                is_below, max_min, result = True, h[1], l[0]
            else:
                is_below, max_min, result = False, l[1], h[0]
            first_trend_bar = True
            accel = start
        result = result + accel * (max_min - result)
        if is_below:
            if result > l[i]:
                first_trend_bar = True
                is_below = False
                result = max(h[i], max_min)
                max_min = l[i]
                accel = start
        else:
            if result < h[i]:
                first_trend_bar = True
                is_below = True
                result = min(l[i], max_min)
                max_min = h[i]
                accel = start
        if not first_trend_bar:
            if is_below:
                if h[i] > max_min:
                    max_min = h[i]
                    accel = min(accel + inc, mx)
            else:
                if l[i] < max_min:
                    max_min = l[i]
                    accel = min(accel + inc, mx)
        if is_below:
            result = min(result, l[i - 1])
            if i > 1:
                result = min(result, l[i - 2])
        else:
            result = max(result, h[i - 1])
            if i > 1:
                result = max(result, h[i - 2])
        out[i] = result
    return out


def sa_build(h, l, c, p):
    """-> dict: sar, ema, up (Trend aufwaerts), buy[], sell[] je Kerze (Signale wie im Pine-Script)."""
    sar = sar_series(h, l, c, p["start"], p["inc"], p["max"])
    ema = _ema_series(c, p["ema_len"])
    return sa_signals(sar, ema, c, p["max_bars"])


def sa_signals(sar, ema, c, max_bars):
    """Signale aus fertigen SAR-/EMA-Reihen (Monte-Carlo rechnet SAR und EMA nur einmal je Wert)."""
    n = len(c)
    p = {"max_bars": max_bars}
    up = [sar[i] is not None and sar[i] < c[i] for i in range(n)]
    buy, sell = [False] * n, [False] * n
    flip_up, flip_dn = [False] * n, [False] * n
    since = None          # Kerzen seit dem letzten SAR-Kippen (0 = erste Kerze des neuen Trends)
    done = False
    for i in range(1, n):
        flip = up[i] != up[i - 1]
        if flip:
            flip_up[i], flip_dn[i] = up[i], not up[i]
            done = False
            since = 0
        elif since is not None:
            since += 1
        if since is None or since >= p["max_bars"] or done or ema[i] is None:
            continue
        if up[i] and c[i] > ema[i]:
            buy[i] = True
            done = True
        elif (not up[i]) and c[i] < ema[i]:
            sell[i] = True
            done = True
    return {"sar": sar, "ema": ema, "up": up, "buy": buy, "sell": sell, "flip_up": flip_up, "flip_dn": flip_dn}


def _sa_levels(direction, entry, size, p):
    """Fester Dollar-SL/-TP -> Preise (None = aus)."""
    if not size or size <= 0:
        return None, None
    sgn = 1 if direction == "long" else -1
    sl = entry - sgn * p["sl_usd"] / size if p["sl_usd"] > 0 else None
    tp = entry + sgn * p["tp_usd"] / size if p["tp_usd"] > 0 else None
    return sl, tp


def _sa_reset_state(st):
    st["sa_sl_price"] = None
    st["sa_tp_price"] = None


# ============================================================================
# LIVE
# ============================================================================
async def check_sa_sl_tp(symbol, price):
    st = BOTS[symbol]["state"]
    if st["position"] is None or price is None:
        return
    pos = st["position"]
    sl_price, tp_price = st.get("sa_sl_price"), st.get("sa_tp_price")
    if sl_price is not None and ((pos == "long" and price <= sl_price) or (pos == "short" and price >= sl_price)):
        debug_log(f"🚪 [{symbol}] SAR+EMA SL: {pos.upper()} @ {price} (Ziel war {round(sl_price, 4)})")
        await execute_exit(symbol, price, "SL")
        if st["position"] is None:
            _sa_reset_state(st)
        return
    if tp_price is not None and ((pos == "long" and price >= tp_price) or (pos == "short" and price <= tp_price)):
        debug_log(f"🎯 [{symbol}] SAR+EMA TP: {pos.upper()} @ {price} (Ziel war {round(tp_price, 4)})")
        await execute_exit(symbol, price, "TP")
        if st["position"] is None:
            _sa_reset_state(st)


async def check_sa_entry(symbol, buy_signal, sell_signal, price, flip_up=False, flip_dn=False):
    """Signal auf der geschlossenen Kerze. Gegen-Signal schliesst und dreht, gleiche Richtung wird ignoriert.
    Kippt der SAR gegen die Position (und es gibt kein Gegensignal), wird sie geschlossen (Option sa_exit_on_flip)."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or price is None:
        return
    pos0 = st["position"]
    if pos0 is not None and _sa_params(cfg)["exit_on_flip"]:
        # wie im Original: exitLong = Long und (Verkaufssignal oder SAR kippt nach unten) - Signal VOR der Richtungs-Sperre
        raw_opp = sell_signal if pos0 == "long" else buy_signal
        sar_end = flip_dn if pos0 == "long" else flip_up
        if sar_end and not raw_opp:
            debug_log(f"🏁 [{symbol}] SAR+EMA SAR-Ende: {pos0.upper()} schließen @ {price}")
            await execute_exit(symbol, price, "SA-SAR-ENDE")
            if st["position"] is None:
                _sa_reset_state(st)
            return
    mode = cfg.get("sa_direction_mode", "both")
    if mode == "long_only":
        sell_signal = False
    elif mode == "short_only":
        buy_signal = False
    if buy_signal == sell_signal:
        return
    target = "long" if buy_signal else "short"
    pos = st["position"]
    if pos == target:
        return
    if pos is not None:
        debug_log(f"🔄 [{symbol}] SAR+EMA Wechsel: {pos.upper()} -> {target.upper()} @ {price}")
        await execute_exit(symbol, price, "SA-FLIP")
        if st["position"] is not None:
            return
        _sa_reset_state(st)
    debug_log(f"📡 [{symbol}] SAR+EMA Signal: {target.upper()} @ {price}")
    await execute_entry(symbol, target, price, is_add_on=False)
    if st["position"] is None:
        return
    _sa_reset_state(st)
    entry = st.get("avg_entry_price") or price
    st["sa_sl_price"], st["sa_tp_price"] = _sa_levels(target, entry, st.get("total_coin_size") or 0, _sa_params(cfg))


async def sa_poll_loop(symbol):
    """SAR + EMA Wechsel - Signal bei Kerzenschluss der Zeitebene, optionaler SL/TP laufend gegen den Live-Preis."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0
    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "sar_ema" and cfg["bot_active"] and b["state"].get("session_started"):
                resolution = cfg.get("sa_resolution", "5m")
                p = _sa_params(cfg)
                min_needed = _sa_warmup(p) + 2
                needed_bars = 600
                st = b["state"]
                closed_ts = None
                _need_fetch = True
                if resolution in SUB_MINUTE_RESOLUTIONS:
                    local = get_seconds_candles(st, SUB_MINUTE_RESOLUTIONS[resolution], needed_bars)
                    if local:
                        closed_ts, closed_o, closed_h, closed_l, closed_c = local
                else:
                    _per = _res_seconds(resolution)
                    _need_fetch = last_processed_ts is None or (time.time() % _per) < 6 or _per <= 60
                    data = await fetch_candles_binance_multi(symbol, resolution, count_back=needed_bars, market_type=cfg.get("binance_market_type", "spot")) if _need_fetch else None
                    if data:
                        timestamps, opens, highs, lows, closes = data[:5]
                        closed_ts, closed_o, closed_h, closed_l, closed_c = timestamps[:-1], opens[:-1], highs[:-1], lows[:-1], closes[:-1]

                now = time.time()
                due_heartbeat = now - last_heartbeat > 300
                if st["position"] is not None and st["last_price"] is not None:
                    await check_sa_sl_tp(symbol, st["last_price"])

                if closed_ts and len(closed_c) > min_needed:
                    age = (now * 1000 - closed_ts[-1]) / 1000
                    if age > 300 + _res_seconds(resolution):
                        debug_log(f"⚠️ [{symbol}] SAR+EMA: letzte Kerze wirkt veraltet ({round(age)}s alt, Auflösung {resolution}) - überspringe Durchlauf.")
                        closed_ts = None
                    elif st["last_price"] is not None and closed_c[-1] and abs(closed_c[-1] - st["last_price"]) / st["last_price"] * 100 > 2.0:
                        debug_log(f"⚠️ [{symbol}] SAR+EMA: Kerzendaten unplausibel (Kerze {closed_c[-1]} vs. Live {st['last_price']}) - überspringe Durchlauf.")
                        closed_ts = None

                if closed_ts and len(closed_c) > min_needed and (last_processed_ts is None or closed_ts[-1] > last_processed_ts):
                    arr = await asyncio.to_thread(sa_build, closed_h, closed_l, closed_c, p)
                    if due_heartbeat:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] SAR+EMA aktiv: {resolution}, SAR {p['start']}/{p['inc']}/{p['max']}, EMA {p['ema_len']}, "
                                  f"Fenster {p['max_bars']} Kerzen, Trend {'auf' if arr['up'][-1] else 'ab'}, Preis={closed_c[-1]}")
                    if last_processed_ts is None:
                        new_indices = [len(closed_ts) - 1]
                    else:
                        new_indices = [i for i in range(len(closed_ts)) if closed_ts[i] > last_processed_ts]
                    for idx in new_indices:
                        price_i = st["last_price"] if (idx == len(closed_ts) - 1 and st["last_price"] is not None) else closed_c[idx]
                        last_processed_ts = closed_ts[idx]
                        await check_sa_entry(symbol, arr["buy"][idx], arr["sell"][idx], price_i, arr["flip_up"][idx], arr["flip_dn"][idx])
                elif due_heartbeat and not closed_ts and (resolution in SUB_MINUTE_RESOLUTIONS or _need_fetch):
                    last_heartbeat = now
                    debug_log(f"⏳ [{symbol}] SAR+EMA wartet: keine Kerzen erhalten (Auflösung {resolution})")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] SAR+EMA-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        await asyncio.sleep(_poll_delay(cfg.get("sa_resolution", "5m"), b["state"]))


# ============================================================================
# BACKTEST
# ============================================================================
def _sa_simulate(candles, cfg, buy_sig, sell_sig, warmup, p, flip_up=None, flip_dn=None):
    """Gegenstueck zu check_sa_entry/check_sa_sl_tp: Einstieg zum Schlusskurs der Signal-Kerze, optionaler SL/TP ab der
    naechsten Kerze ueber Hoch/Tief (SL gewinnt bei Gleichzeitigkeit), Gegen-Signal dreht."""
    ts, h, l, c = candles[0], candles[2], candles[3], candles[4]
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    mode = cfg.get("sa_direction_mode", "both")
    position, trades = None, []
    for i in range(max(warmup, 1), n):
        if position is not None:
            pdir, entry, sl, tp = position["dir"], position["entry"], position["sl"], position["tp"]
            if sl is not None and ((pdir == "long" and l[i] <= sl) or (pdir == "short" and h[i] >= sl)):
                _bt_close_trade(trades, pdir, entry, sl, position["size"], i, position["entry_i"], "SL", ts=ts)
                position = None
            elif tp is not None and ((pdir == "long" and h[i] >= tp) or (pdir == "short" and l[i] <= tp)):
                _bt_close_trade(trades, pdir, entry, tp, position["size"], i, position["entry_i"], "TP", ts=ts)
                position = None
        buy, sell = buy_sig[i], sell_sig[i]
        if position is not None and p["exit_on_flip"] and flip_up is not None:
            raw_opp = sell if position["dir"] == "long" else buy
            if (flip_dn[i] if position["dir"] == "long" else flip_up[i]) and not raw_opp:
                _bt_close_trade(trades, position["dir"], position["entry"], c[i], position["size"], i, position["entry_i"], "SA-SAR-ENDE", ts=ts)
                position = None
                continue
        if mode == "long_only":
            sell = False
        elif mode == "short_only":
            buy = False
        if buy == sell:
            continue
        target = "long" if buy else "short"
        if position is not None and position["dir"] == target:
            continue
        price = c[i]
        if position is not None:
            _bt_close_trade(trades, position["dir"], position["entry"], price, position["size"], i, position["entry_i"], "SA-FLIP", ts=ts)
            position = None
        size = (margin * leverage) / price
        sl, tp = _sa_levels(target, price, size, p)
        position = {"dir": target, "entry": price, "size": size, "entry_i": i, "sl": sl, "tp": tp}
    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)
    return trades


async def run_sa_backtest(symbol, cfg, days, exclude_top_n=1):
    max_candles = BACKTEST_MAX_CANDLES.get("sar_ema", 100_000)
    resolution = cfg.get("sa_resolution", "5m")
    if resolution in SUB_MINUTE_RESOLUTIONS:
        max_candles = min(max_candles, 5000)
    candles, err = await _fetch_cached_mo7_backtest_candles(symbol, resolution, days, max_candles, market_type=cfg.get("binance_market_type", "spot"))
    if err:
        return {"error": err}
    p = _sa_params(cfg)
    min_needed = _sa_warmup(p) + 10
    if not candles or len(candles[4]) < min_needed:
        return {"error": f"Zu wenig historische Kerzen für einen aussagekräftigen Backtest erhalten (mind. ~{min_needed} nötig)."}
    base_ts = candles[0]
    arr = await asyncio.to_thread(sa_build, candles[2], candles[3], candles[4], p)
    trades = _sa_simulate(candles, cfg, arr["buy"], arr["sell"], _sa_warmup(p), p, arr["flip_up"], arr["flip_dn"])
    stats = summarize_backtest_trades(trades, exclude_top_n)
    actual_days = (base_ts[-1] - base_ts[0]) / 86_400_000
    return {
        "symbol": symbol, "entry_mode": "sar_ema", "resolution": resolution,
        "requested_days": days, "actual_days_covered": round(actual_days, 1),
        "candles_processed": len(base_ts), "candle_cap": max_candles, "cache_used": False,
        "signals_buy": sum(arr["buy"]), "signals_sell": sum(arr["sell"]),
        "stats": stats,
        "stats_long": summarize_backtest_trades([t for t in trades if t["dir"] == "long"], exclude_top_n),
        "stats_short": summarize_backtest_trades([t for t in trades if t["dir"] == "short"], exclude_top_n),
        "trades": trades,
    }


# ============================================================================
# CHART
# ============================================================================
def sa_chart_payload(ts, o, h, l, c, cfg, max_bars=1000):
    """Kerzen, EMA, SAR (gruen/rot getrennt) und Pfeile bei jedem Signal."""
    p = _sa_params(cfg)
    if len(ts) < 3:
        return None
    arr = sa_build(h, l, c, p)
    n = len(c)
    off = max(0, n - max_bars)
    candles, markers, ema_l, sar_up, sar_dn = [], [], [], [], []
    mode = cfg.get("sa_direction_mode", "both")
    pos = None   # Positionsverlauf nur aus den Signalen (ohne SL/TP) -> zeigt, wo ein SAR-Ende die Position beenden wuerde
    exits = {}
    for i in range(1, n):
        buy, sell = arr["buy"][i], arr["sell"][i]
        if pos and p["exit_on_flip"] and (arr["flip_dn"][i] if pos == "long" else arr["flip_up"][i]) and not (sell if pos == "long" else buy):
            exits[i] = pos
            pos = None
        if mode == "long_only":
            sell = False
        elif mode == "short_only":
            buy = False
        if buy != sell:
            pos = "long" if buy else "short"
    for i in range(off, n):
        t = ts[i] // 1000
        candles.append({"time": t, "open": o[i], "high": h[i], "low": l[i], "close": c[i]})
        if arr["ema"][i] is not None:
            ema_l.append({"time": t, "value": arr["ema"][i]})
        s = arr["sar"][i]
        if s is not None:
            sar_up.append({"time": t, "value": s} if arr["up"][i] else {"time": t})   # Luecken als Whitespace
            sar_dn.append({"time": t} if arr["up"][i] else {"time": t, "value": s})
        if arr["buy"][i]:
            markers.append({"time": t, "kind": "long", "price": c[i], "text": "BUY"})
        if arr["sell"][i]:
            markers.append({"time": t, "kind": "short", "price": c[i], "text": "SELL"})
        if i in exits:
            markers.append({"time": t, "kind": "exit_x", "dir": exits[i], "price": c[i], "text": "SAR-Ende"})
    lines = [{"key": "sa_ema", "label": f"EMA {p['ema_len']}", "data": ema_l},
             {"key": "sa_up", "label": "SAR (Aufwärts)", "data": sar_up},
             {"key": "sa_dn", "label": "SAR (Abwärts)", "data": sar_dn}]
    return {"candles": candles, "wt1": [], "wt2": [], "markers": markers, "levels": [], "entry_mode": "sar_ema",
            "wt1v": 0, "wt2v": 0, "bars": n, "sa_lines": lines}


# ============================================================================
# MONTE-CARLO
# ============================================================================
def _sa_mc_compute(candles, i_start, base_cfg, combos, exclude_top_n, deadline):
    """combos: (SAR Start, SAR Schritt, SAR Maximum, EMA-Laenge). Fenster, Richtung, SL/TP, Margin und Hebel kommen aus den Einstellungen."""
    base_p = _sa_params(base_cfg)
    ts, o, h, l, c, v = candles
    combos = sorted(combos)
    sar_cache, ema_cache, out = {}, {}, []
    for start, inc, mx, eln in combos:
        if time.time() > deadline:
            break
        p = dict(base_p, start=start, inc=inc, max=mx, ema_len=eln)
        k = (start, inc, mx)
        if k not in sar_cache:
            if len(sar_cache) > 3:
                sar_cache.clear()    # sortiert -> alte Schluessel werden nicht mehr gebraucht
            sar_cache[k] = sar_series(h, l, c, start, inc, mx)
        if eln not in ema_cache:
            ema_cache[eln] = _ema_series(c, eln)
        arr = sa_signals(sar_cache[k], ema_cache[eln], c, p["max_bars"])
        trades = _sa_simulate(candles, base_cfg, arr["buy"], arr["sell"], i_start, p, arr["flip_up"], arr["flip_dn"])
        st = summarize_backtest_trades(trades, exclude_top_n)
        out.append({"sa_start": start, "sa_inc": inc, "sa_max": mx, "sa_ema_len": eln,
                    "trades": st["trades"], "win_rate_pct": st["win_rate_pct"],
                    "total_pnl_usd": st["total_pnl_usd"], "total_pnl_excl_top_n_usd": st["total_pnl_excl_top_n_usd"],
                    "max_drawdown_usd": st["max_drawdown_usd"], "avg_bars_held": st["avg_bars_held"]})
    return out


async def run_sa_montecarlo(symbol, cfg, days, starts=None, incs=None, maxs=None, emas=None, runs=500, exclude_top_n=1, seed=None):
    """Raster ueber SAR Start x SAR Schritt x SAR Maximum x EMA-Laenge. Achsen als {"min","max","step"} (oder Liste); eine leere Achse
    nimmt den Wert aus den Einstellungen. Zeitebene, Fenster, Richtung, SAR-Ende, SL/TP, Margin und Hebel kommen aus den Einstellungen."""
    import random
    from zerolag import zl_mc_range, zl_mc_values, ZL_MC_MAX_RUNS
    cfg = dict(cfg)
    base_p = _sa_params(cfg)

    def axis(val, cast, lo, hi, fallback):
        return (zl_mc_range(val, cast, lo, hi) if isinstance(val, dict) else zl_mc_values(val, cast, lo, hi)) or [fallback]

    start_l = axis(starts, float, 0.001, 1.0, base_p["start"])
    inc_l = axis(incs, float, 0.001, 1.0, base_p["inc"])
    max_l = axis(maxs, float, 0.001, 2.0, base_p["max"])
    ema_l = axis(emas, int, 1, 500, base_p["ema_len"])
    runs = max(1, min(ZL_MC_MAX_RUNS, int(runs)))
    res = cfg.get("sa_resolution", "5m")
    mt = cfg.get("binance_market_type", "spot")
    max_candles = BACKTEST_MAX_CANDLES.get("sar_ema", 100_000)
    combos = [(a, b, m, e) for a in start_l for b in inc_l for m in max_l for e in ema_l]
    requested = len(combos)
    rng = random.Random(seed)
    if len(combos) > runs:
        combos = rng.sample(combos, runs)
    warm = (max(ema_l) * 3 + 250) * _res_seconds(res) / 86_400
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
    results = await asyncio.to_thread(_sa_mc_compute, candles, i_start, cfg, combos, exclude_top_n, deadline)
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
        "symbol": symbol, "entry_mode": "sar_ema", "days": days, "actual_days_covered": round(days_cov, 1),
        "candles_processed": n_max, "resolution": res,
        "starts": start_l, "incs": inc_l, "maxs": max_l, "emas": ema_l,
        "min_reliable_trades": min_trades, "seed": seed,
        "combos_tested": len(results), "combos_requested": requested, "combos_drawn": len(combos),
        "profitable_pct": round(pos / len(results) * 100, 1),
        "median_pnl_usd": round(median, 2), "best_pnl_usd": pnls[-1], "worst_pnl_usd": pnls[0],
        "results": best[:30], "worst_results": worst[:20],
    }
