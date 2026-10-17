"""
trendfolge.py - eigene Strategie "Trendfolge" (entry_mode "trendfolge", Config-Praefix tfl_*).

Einfaches Prinzip:
  * TREND = Marktstruktur (BOS/CHoCH, intern oder swing) auf einer frei waehlbaren Trend-Zeitebene (ab 10 Sekunden).
  * Trend AUFWAERTS  -> bei JEDER ROTEN Kerze (Kerzenschluss unter Eroeffnung) wird LONG gekauft / nachgekauft.
  * Trend ABWAERTS   -> bei JEDER GRUENEN Kerze wird SHORT verkauft / nachverkauft.
  * Fester $-TP auf die GESAMTE Position (Gewinn vom Durchschnittseinstieg), optional fester $-SL.
  * Trendwechsel -> Position wird sofort geschlossen (kein Gegen-Einstieg; der kommt erst bei der naechsten passenden Kerze).
Kerzen-Zeitebene (auf der rot/gruen gezaehlt wird) und Trend-Zeitebene sind getrennt einstellbar.
"""

import asyncio
import time
import traceback

from bot_core import BOTS, debug_log, execute_entry, execute_exit
from strategies import (
    SUB_MINUTE_RESOLUTIONS, get_seconds_candles, fetch_candles_binance_multi, ms_trend_series, _trend_align, _tf_ms,
    summarize_backtest_trades, _fetch_cached_mo7_backtest_candles, BACKTEST_MAX_CANDLES, _bt_close_trade,
)

TFL_TFS = ["10s", "15s", "30s", "45s", "1m", "5m", "15m", "30m", "1h", "4h", "1d"]


def _res_seconds(res):
    if res in SUB_MINUTE_RESOLUTIONS:
        return SUB_MINUTE_RESOLUTIONS[res]
    return max(1, int(_tf_ms(res) / 1000))


def _tfl_params(cfg):
    res = cfg.get("tfl_resolution", "1m")
    ttf = cfg.get("tfl_trend_tf", "5m")
    mode = cfg.get("tfl_trend_mode", "swing")
    return {
        "res": res if res in TFL_TFS else "1m",
        "trend_tf": ttf if ttf in TFL_TFS else "5m",
        "trend_mode": mode if mode in ("intern", "swing") else "swing",
        "ilen": max(2, int(cfg.get("tfl_trend_ilen", 4) or 4)),
        "slen": max(2, int(cfg.get("tfl_trend_slen", 50) or 50)),
        "tp_usd": max(0.0, float(cfg.get("tfl_tp_usd", 3.0) or 0.0)),
        "sl_usd": max(0.0, float(cfg.get("tfl_sl_usd", 0.0) or 0.0)),
        "max_entries": max(0, int(cfg.get("tfl_max_entries", 10) or 0)),
        "direction": cfg.get("tfl_direction_mode", "both"),
    }


def _poll_delay(res, st):
    per = _res_seconds(res)
    if st.get("position") is not None or per <= 60:
        return 1.0
    into = time.time() % per
    if into < 6:
        return 1.0
    return max(1.0, min(5.0, per - into - 0.5))


async def _tfl_closed(symbol, cfg, st, tf, bars):
    """Geschlossene Kerzen (ts, o, h, l, c) der Zeitebene tf - Sekunden-Kerzen aus dem 1s-Puffer, sonst Binance (ohne die laufende Kerze)."""
    if tf in SUB_MINUTE_RESOLUTIONS:
        return get_seconds_candles(st, SUB_MINUTE_RESOLUTIONS[tf], bars)
    data = await fetch_candles_binance_multi(symbol, tf, count_back=min(1000, bars), market_type=cfg.get("binance_market_type", "spot"))
    if not data:
        return None
    ts, o, h, l, c = data[:5]
    return ts[:-1], o[:-1], h[:-1], l[:-1], c[:-1]


_trend_cache = {}


async def tfl_trend_now(symbol, cfg, st):
    """Aktueller Trend 1 / -1 / 0 (unbekannt) aus geschlossenen Kerzen der Trend-Zeitebene. Neuberechnung nur bei neuer Kerze."""
    p = _tfl_params(cfg)
    L = p["ilen"] if p["trend_mode"] == "intern" else p["slen"]
    key = (symbol, p["trend_tf"], p["trend_mode"], p["ilen"], p["slen"])
    cached = _trend_cache.get(key)
    if cached and time.time() - cached["at"] < (2.0 if p["trend_tf"] in SUB_MINUTE_RESOLUTIONS else 10.0):
        return cached["val"]
    cs = await _tfl_closed(symbol, cfg, st, p["trend_tf"], 2 * L + 150)
    if not cs or len(cs[4]) < 2 * L + 6:
        return cached["val"] if cached else 0
    ts, _o, h, l, c = cs
    if cached and cached["last"] == ts[-1]:
        cached["at"] = time.time()
        return cached["val"]
    itr, swt, _ev = await asyncio.to_thread(ms_trend_series, h, l, c, p["ilen"], p["slen"])
    val = (itr if p["trend_mode"] == "intern" else swt)[-1]
    _trend_cache[key] = {"at": time.time(), "last": ts[-1], "val": val}
    return val


def _unreal(st, price):
    avg, size = st.get("avg_entry_price"), st.get("total_coin_size") or 0
    if not avg or not size:
        return 0.0
    return (price - avg) * size if st["position"] == "long" else (avg - price) * size


async def check_tfl_tp_sl(symbol, price):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if st["position"] is None or price is None:
        return
    p = _tfl_params(cfg)
    pnl = _unreal(st, price)
    if p["sl_usd"] > 0 and pnl <= -p["sl_usd"]:
        debug_log(f"🚪 [{symbol}] Trendfolge SL: {st['position'].upper()} @ {price} (uPnL {round(pnl, 2)} $)")
        await execute_exit(symbol, price, "SL")
    elif p["tp_usd"] > 0 and pnl >= p["tp_usd"]:
        debug_log(f"🎯 [{symbol}] Trendfolge TP: {st['position'].upper()} @ {price} (uPnL {round(pnl, 2)} $)")
        await execute_exit(symbol, price, "TP")


async def check_tfl_trend_flip(symbol, trend, price):
    """Trendwechsel gegen die offene Position -> sofort schliessen."""
    st = BOTS[symbol]["state"]
    pos = st["position"]
    if pos is None or not trend or price is None:
        return
    if (pos == "long" and trend < 0) or (pos == "short" and trend > 0):
        debug_log(f"🧭 [{symbol}] Trendfolge: Trend dreht ({pos.upper()} gegen neuen Trend) - Position schließen @ {price}")
        await execute_exit(symbol, price, "TREND-FLIP")


async def check_tfl_entry(symbol, candle_color, trend, price):
    """Eine neu geschlossene Kerze (candle_color 'red'/'green'): im Aufwaertstrend rot = Kauf, im Abwaertstrend gruen = Short."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or price is None or not trend:
        return
    p = _tfl_params(cfg)
    if trend > 0 and candle_color == "red":
        target = "long"
    elif trend < 0 and candle_color == "green":
        target = "short"
    else:
        return
    if (target == "long" and p["direction"] == "short_only") or (target == "short" and p["direction"] == "long_only"):
        return
    pos = st["position"]
    if pos is not None and pos != target:
        return   # offene Position gegen den Trend: wird ueber den Trendwechsel geschlossen, nicht hier gedreht
    if pos is not None and p["max_entries"] > 0 and (st.get("entry_count") or 0) >= p["max_entries"]:
        return
    debug_log(f"📡 [{symbol}] Trendfolge: {'rote' if candle_color == 'red' else 'grüne'} Kerze im {'Aufwärts' if trend > 0 else 'Abwärts'}trend -> {target.upper()}"
              f"{' (Nachkauf ' + str((st.get('entry_count') or 0) + 1) + ')' if pos is not None else ''} @ {price}")
    await execute_entry(symbol, target, price, is_add_on=pos is not None)


def _color(o, c):
    return "green" if c > o else ("red" if c < o else None)


async def tfl_poll_loop(symbol):
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0
    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "trendfolge" and cfg["bot_active"] and b["state"].get("session_started"):
                st = b["state"]
                p = _tfl_params(cfg)
                now = time.time()
                price = st.get("last_price")
                trend = await tfl_trend_now(symbol, cfg, st)
                st["tfl_trend"] = trend
                if st["position"] is not None and price is not None:
                    await check_tfl_tp_sl(symbol, price)
                if st["position"] is not None and price is not None:
                    await check_tfl_trend_flip(symbol, trend, price)

                cs = await _tfl_closed(symbol, cfg, st, p["res"], 30)
                if cs and len(cs[4]) >= 2:
                    ts, o, h, l, c = cs
                    age = (now * 1000 - ts[-1]) / 1000
                    plausible = price is None or not c[-1] or abs(c[-1] - price) / price * 100 <= 2.0
                    if age > 300 + _res_seconds(p["res"]):
                        if now - last_heartbeat > 300:
                            last_heartbeat = now
                            debug_log(f"⚠️ [{symbol}] Trendfolge: letzte Kerze wirkt veraltet ({round(age)}s alt, {p['res']}) - überspringe.")
                    elif not plausible:
                        debug_log(f"⚠️ [{symbol}] Trendfolge: Kerzendaten unplausibel (Kerze {c[-1]} vs. Live {price}) - überspringe.")
                    elif last_processed_ts is None:
                        last_processed_ts = ts[-1]        # erste Kerze nach dem Start nur merken, nicht handeln
                    elif ts[-1] > last_processed_ts:
                        last_processed_ts = ts[-1]        # bei Luecken nur die NEUESTE Kerze werten (kein Nachhol-Burst)
                        col = _color(o[-1], c[-1])
                        if col:
                            await check_tfl_entry(symbol, col, trend, price if price is not None else c[-1])
                    if now - last_heartbeat > 300:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] Trendfolge aktiv: Trend={'↑' if trend > 0 else '↓' if trend < 0 else '?'} "
                                  f"({p['trend_tf']}, {p['trend_mode']}), Kerzen {p['res']}, TP=${p['tp_usd']}, Preis={price}")
                elif now - last_heartbeat > 300:
                    last_heartbeat = now
                    debug_log(f"⏳ [{symbol}] Trendfolge wartet: keine Kerzen erhalten ({p['res']})")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Trendfolge-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        await asyncio.sleep(_poll_delay(b["config"].get("tfl_resolution", "1m"), b["state"]))


# ============================================================
# Backtest
# ============================================================
def _tfl_simulate(candles, cfg, trend, warmup, p):
    """Backtest-Gegenstueck: Kerze i wird bei Schluss bewertet. Zuerst TP/SL einer schon offenen Position ueber Hoch/Tief
    (SL gewinnt bei Gleichzeitigkeit), dann Trendwechsel (Schlusskurs), dann Einstieg/Nachkauf zum Schlusskurs."""
    ts, o, h, l, c = candles[0], candles[1], candles[2], candles[3], candles[4]
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    position, trades = None, []
    for i in range(max(warmup, 1), n):
        if position is not None:
            sgn = 1 if position["dir"] == "long" else -1
            avg = position["cost"] / position["size"]
            sl = avg - sgn * p["sl_usd"] / position["size"] if p["sl_usd"] > 0 else None
            tp = avg + sgn * p["tp_usd"] / position["size"] if p["tp_usd"] > 0 else None
            if sl is not None and ((sgn > 0 and l[i] <= sl) or (sgn < 0 and h[i] >= sl)):
                _bt_close_trade(trades, position["dir"], avg, sl, position["size"], i, position["entry_i"], "SL", ts=ts)
                position = None
            elif tp is not None and ((sgn > 0 and h[i] >= tp) or (sgn < 0 and l[i] <= tp)):
                _bt_close_trade(trades, position["dir"], avg, tp, position["size"], i, position["entry_i"], "TP", ts=ts)
                position = None
        t = trend[i]
        if position is not None and t and ((position["dir"] == "long" and t < 0) or (position["dir"] == "short" and t > 0)):
            avg = position["cost"] / position["size"]
            _bt_close_trade(trades, position["dir"], avg, c[i], position["size"], i, position["entry_i"], "TREND-FLIP", ts=ts)
            position = None
        col = _color(o[i], c[i])
        if not t or not col:
            continue
        if t > 0 and col == "red":
            target = "long"
        elif t < 0 and col == "green":
            target = "short"
        else:
            continue
        if (target == "long" and p["direction"] == "short_only") or (target == "short" and p["direction"] == "long_only"):
            continue
        if position is not None and position["dir"] != target:
            continue
        if position is not None and p["max_entries"] > 0 and position["n"] >= p["max_entries"]:
            continue
        size = (margin * leverage) / c[i]
        if position is None:
            position = {"dir": target, "size": size, "cost": c[i] * size, "n": 1, "entry_i": i}
        else:
            position["size"] += size
            position["cost"] += c[i] * size
            position["n"] += 1
    if position is not None:
        _bt_close_trade(trades, position["dir"], position["cost"] / position["size"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)
    return trades


async def _tfl_trend_for_backtest(symbol, cfg, candles, days, p):
    """Trend-Array passend zu den Basis-Kerzen (nur geschlossene Trend-Kerzen, kein Blick in die Zukunft) oder (None, Fehlertext)."""
    ts, h, l, c = candles[0], candles[2], candles[3], candles[4]
    L = p["ilen"] if p["trend_mode"] == "intern" else p["slen"]
    if p["trend_tf"] == p["res"]:
        itr, swt, _ev = await asyncio.to_thread(ms_trend_series, h, l, c, p["ilen"], p["slen"])
        return (itr if p["trend_mode"] == "intern" else swt), None
    if p["trend_tf"] in SUB_MINUTE_RESOLUTIONS:
        return None, "Backtest: Trend-Zeitebene unter 1 Minute geht nur, wenn sie gleich der Kerzen-Zeitebene ist (keine Sekunden-Historie)."
    htf_ms = _tf_ms(p["trend_tf"])
    warm_days = (2 * L + 30) * htf_ms / 86_400_000
    data, err = await _fetch_cached_mo7_backtest_candles(symbol, p["trend_tf"], days + warm_days + 1, 5000, market_type=cfg.get("binance_market_type", "spot"))
    if err or not data or len(data[4]) < 2 * L + 5:
        return None, err or "Zu wenig Kerzen der Trend-Zeitebene für den Backtest."
    hts, _o, hh, hl, hc, _v = data
    itr, swt, _ev = await asyncio.to_thread(ms_trend_series, hh, hl, hc, p["ilen"], p["slen"])
    base_ms = ts[1] - ts[0] if len(ts) > 1 else _tf_ms(p["res"])
    return _trend_align(ts, base_ms, hts, htf_ms, itr if p["trend_mode"] == "intern" else swt), None


async def run_tfl_backtest(symbol, cfg, days, exclude_top_n=1):
    max_candles = BACKTEST_MAX_CANDLES.get("trendfolge", 100_000)
    p = _tfl_params(cfg)
    if p["res"] in SUB_MINUTE_RESOLUTIONS:
        max_candles = min(max_candles, 5000)
    candles, err = await _fetch_cached_mo7_backtest_candles(symbol, p["res"], days, max_candles, market_type=cfg.get("binance_market_type", "spot"))
    if err:
        return {"error": err}
    if not candles or len(candles[4]) < 200:
        return {"error": "Zu wenig historische Kerzen für einen aussagekräftigen Backtest erhalten (mind. ~200 nötig)."}
    trend, terr = await _tfl_trend_for_backtest(symbol, cfg, candles, days, p)
    if trend is None:
        return {"error": terr}
    L = p["ilen"] if p["trend_mode"] == "intern" else p["slen"]
    warm = 2 * L + 6 if p["trend_tf"] == p["res"] else 5
    trades = _tfl_simulate(candles, cfg, trend, warm, p)
    stats = summarize_backtest_trades(trades, exclude_top_n)
    base_ts = candles[0]
    actual_days = (base_ts[-1] - base_ts[0]) / 86_400_000
    return {
        "symbol": symbol, "entry_mode": "trendfolge", "resolution": p["res"],
        "requested_days": days, "actual_days_covered": round(actual_days, 1),
        "candles_processed": len(base_ts), "candle_cap": max_candles, "cache_used": False,
        "trend_candles_valid": sum(1 for x in trend if x), "signals_buy": sum(1 for x in trend if x > 0), "signals_sell": sum(1 for x in trend if x < 0),
        "stats": stats,
        "stats_long": summarize_backtest_trades([t for t in trades if t["dir"] == "long"], exclude_top_n),
        "stats_short": summarize_backtest_trades([t for t in trades if t["dir"] == "short"], exclude_top_n),
        "trades": trades,
    }


# ============================================================
# Chart
# ============================================================
def tfl_chart_payload(ts, o, h, l, c, cfg, max_bars=1000):
    n = len(c)
    off = max(0, n - max_bars)
    candles = [{"time": ts[i] // 1000, "open": o[i], "high": h[i], "low": l[i], "close": c[i]} for i in range(off, n)]
    return {"candles": candles, "wt1": [], "wt2": [], "markers": [], "levels": [], "entry_mode": "tfl", "wt1v": 0, "wt2v": 0, "bars": n}


async def tfl_structure_events(symbol, cfg, st, chart_ts):
    """BOS/CHoCH der Trend-Zeitebene (gewaehlter Modus) auf die Chart-Kerzen abgebildet - fuers Chart-Overlay."""
    p = _tfl_params(cfg)
    L = p["ilen"] if p["trend_mode"] == "intern" else p["slen"]
    cs = await _tfl_closed(symbol, cfg, st, p["trend_tf"], 2 * L + 400)
    if not cs or len(cs[4]) < 2 * L + 6 or not chart_ts:
        return []
    hts, _o, h, l, c = cs
    _itr, _swt, events = await asyncio.to_thread(ms_trend_series, h, l, c, p["ilen"], p["slen"])
    htf_ms = _res_seconds(p["trend_tf"]) * 1000
    out, j = [], 0
    for idx, kind, direction, scope in events:
        if scope != p["trend_mode"]:
            continue
        t_close = hts[idx] + htf_ms
        if t_close < chart_ts[0]:
            continue
        while j < len(chart_ts) and chart_ts[j] < t_close:
            j += 1
        if j >= len(chart_ts):
            break
        out.append({"time": chart_ts[j] // 1000, "kind": kind, "dir": direction})
    return out
