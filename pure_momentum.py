"""
pure_momentum.py - eigene Strategie "Pure Momentum Oscillator" (entry_mode "pure_momentum", Config-Praefix pm_*).

Portiert aus dem TradingView-Indikator "Pure Momentum Oscillator":
  cd  = 2*close - EMA(close, N)            (entspricht einer DEMA-artigen Entzerrung)
  cda = WMA(cd, N)                          (geglaettete Linie)
  Momentum = (cda - cda[1]) / ATR(N)
Farbe: Momentum > Long-Schwelle = GRUEN, Momentum < Short-Schwelle = ROT, dazwischen bleibt die letzte Farbe.
SIGNAL: bei JEDEM Farbwechsel auf der geschlossenen Kerze (gruen->rot = SELL, rot->gruen = BUY).
Die erste Farbe nach dem Start setzt nur den Ausgangszustand (kein Signal).
Ausstieg: "flip" (Gegen-Signal dreht die Position, optionaler ATR-SL) oder "plan" (ATR-SL + TP1/TP2/TP3 mit Teilverkaeufen).
Die Handels-Ausfuehrung (Order, Teilverkauf, Backtest-Simulation) nutzt dieselben Bausteine wie die anderen Strategien.
"""

import asyncio
import time
import traceback

from bot_core import BOTS, debug_log, execute_entry, execute_exit, execute_partial_exit
from strategies import (
    SUB_MINUTE_RESOLUTIONS, get_seconds_candles, fetch_candles_binance_multi, _ema_series, compute_atr, _tf_ms,
    summarize_backtest_trades, _fetch_cached_mo7_backtest_candles, BACKTEST_MAX_CANDLES,
    _mt_plan_levels, _simulate_mt_trades,
)


def _pm_params(cfg):
    return {
        "len": max(2, int(cfg.get("pm_len", 45))),
        "long": float(cfg.get("pm_long", 0.05)), "short": float(cfg.get("pm_short", -0.05)),
        "atr_len": max(1, int(cfg.get("pm_atr_len", 14))),
        "sl_atr_mult": max(0.0, float(cfg.get("pm_sl_atr_mult", 2.0))),
        "tp_mode": cfg.get("pm_tp_mode", "atr"), "tp_step_usd": float(cfg.get("pm_tp_step_usd", 10.0)),
        "tp_step_atr_mult": float(cfg.get("pm_tp_step_atr_mult", 1.0)),
    }


def _pm_warmup(p):
    return max(p["atr_len"], 3 * p["len"]) + 5


def _wma_series(x, n):
    out = [None] * len(x)
    den = n * (n + 1) / 2.0
    for i in range(n - 1, len(x)):
        out[i] = sum(x[i - n + 1 + k] * (k + 1) for k in range(n)) / den
    return out


def pm_momentum_series(h, l, c, n):
    """Momentum je Kerze (None, solange die Linien noch einschwingen)."""
    ca = _ema_series(c, n)
    cd = [2.0 * c[i] - ca[i] for i in range(len(c))]
    cda = _wma_series(cd, n)
    atr = compute_atr(h, l, c, n) or [0.0] * len(c)
    out = [None] * len(c)
    for i in range(max(1, 2 * n), len(c)):
        if cda[i] is None or cda[i - 1] is None or not atr[i]:
            continue
        out[i] = (cda[i] - cda[i - 1]) / atr[i]
    return out


def pm_signals(mom, p):
    """-> (buy[], sell[]): Signal bei jedem Farbwechsel."""
    n = len(mom)
    buy, sell = [False] * n, [False] * n
    col = 0
    for i in range(n):
        v = mom[i]
        if v is None:
            continue
        new = 1 if v > p["long"] else (-1 if v < p["short"] else col)
        if new != col:
            if col != 0:
                if new == 1:
                    buy[i] = True
                else:
                    sell[i] = True
            col = new
    return buy, sell


def _pm_as_mt_cfg(cfg):
    """Backtest-Simulation ist von der MTF-Strategie uebernommen und liest mt_*-Schluessel -> pm_* dorthin spiegeln."""
    c2 = dict(cfg)
    for k, v in cfg.items():
        if k.startswith("pm_"):
            c2["mt_" + k[3:]] = v
    return c2


def _pm_reset_state(st):
    st["pm_sl_price"] = None
    st["pm_tp1_price"] = None
    st["pm_tp2_price"] = None
    st["pm_tp3_price"] = None
    st["pm_tp1_done"] = False
    st["pm_tp2_done"] = False


async def _pm_load_htf_backtest(symbol, cfg, base_ts, p):
    htf = {}
    for tf in _pm_all_tfs(p):
        warm_bars = 3 * (p["n1"] + p["n2"] + p["sig"]) + 20
        src = _pm_source(tf)
        if src:
            src_tf, factor = src
            period = -(-(warm_bars * factor - 20) // 5)
            data, err = await _fetch_trend_filter_backtest_candles(symbol, cfg, base_ts, src_tf, period)
            if err:
                return None, err.replace("SuperTrend-Trendfilter-Zeiteinheit", "Pure Momentum-Zeiteinheit")
            htf[tf] = _pm_resample_ms(data, _tf_ms(tf))
        else:
            period = -(-(warm_bars - 20) // 5)
            data, err = await _fetch_trend_filter_backtest_candles(symbol, cfg, base_ts, tf, period)
            if err:
                return None, err.replace("SuperTrend-Trendfilter-Zeiteinheit", "Pure Momentum-Zeiteinheit")
            htf[tf] = tuple(data[:5])
    return htf, None


async def _pm_load_htf_live(symbol, cfg, p):
    htf = {}
    now_ms = time.time() * 1000
    for tf in _pm_all_tfs(p):
        try:
            src = _pm_source(tf)
            if src:
                d = await fetch_candles_binance_multi(symbol, src[0], count_back=min(1000, src[1] * 130), market_type=cfg.get("binance_market_type", "spot"))
                d = _pm_resample_ms(d[:5], _tf_ms(tf)) if d else None
            else:
                d = await fetch_candles_binance_multi(symbol, tf, count_back=300, market_type=cfg.get("binance_market_type", "spot"))
                d = tuple(d[:5]) if d else None
        except Exception:
            d = None
        if not d or len(d[4]) < 5:
            return None
        ms = _tf_ms(tf)
        keep = [i for i in range(len(d[0])) if d[0][i] + ms <= now_ms]   # nur geschlossene HTF-Kerzen
        if len(keep) < 5:
            return None
        htf[tf] = tuple([x[i] for i in keep] for x in d)
    return htf




async def check_pm_sl_tp(symbol, price):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if st["position"] is None or price is None:
        return
    pos = st["position"]
    sl_price = st.get("pm_sl_price")
    if sl_price is not None and ((pos == "long" and price <= sl_price) or (pos == "short" and price >= sl_price)):
        reason = "SL-AUF-TP1" if st.get("pm_tp2_done") else ("BREAKEVEN" if st.get("pm_tp1_done") else "SL")
        debug_log(f"🚪 [{symbol}] Pure Momentum {reason}: {pos.upper()} @ {price} (Ziel war {round(sl_price, 4)})")
        await execute_exit(symbol, price, reason)
        if st["position"] is None:
            st["pm_sl_cooldown_until"] = time.time() + cfg.get("pm_sl_cooldown_seconds", 30)
            _pm_reset_state(st)
        return
    if cfg.get("pm_exit_mode", "flip") != "plan":
        return
    if not st.get("pm_tp1_done") and st.get("pm_tp1_price") is not None:
        tp1 = st["pm_tp1_price"]
        if (pos == "long" and price >= tp1) or (pos == "short" and price <= tp1):
            if not await execute_partial_exit(symbol, price, cfg.get("pm_tp1_close_pct", 33) / 100, "TP1"):
                return
            st["pm_tp1_done"] = True
            if cfg.get("pm_sl_to_breakeven_on_tp1", True) and st.get("avg_entry_price"):
                st["pm_sl_price"] = st["avg_entry_price"]
                debug_log(f"📡 [{symbol}] Pure Momentum TP1 erreicht - SL auf Break-Even")
        else:
            return
    if not st.get("pm_tp2_done") and st.get("pm_tp2_price") is not None:
        tp2 = st["pm_tp2_price"]
        if (pos == "long" and price >= tp2) or (pos == "short" and price <= tp2):
            if not await execute_partial_exit(symbol, price, cfg.get("pm_tp2_close_pct", 50) / 100, "TP2"):
                return
            st["pm_tp2_done"] = True
            if cfg.get("pm_sl_to_tp1_on_tp2", True) and st.get("pm_tp1_price") is not None:
                st["pm_sl_price"] = st["pm_tp1_price"]
                debug_log(f"📡 [{symbol}] Pure Momentum TP2 erreicht - SL auf TP1")
        else:
            return
    tp3 = st.get("pm_tp3_price")
    if tp3 is not None and ((pos == "long" and price >= tp3) or (pos == "short" and price <= tp3)):
        debug_log(f"🚪 [{symbol}] Pure Momentum TP3 (Rest): {pos.upper()} @ {price}")
        await execute_exit(symbol, price, "TP3")
        if st["position"] is None:
            _pm_reset_state(st)


async def check_pm_entry(symbol, buy_signal, sell_signal, price, atr_now):
    """Einstieg auf der Signal-Kerze. Plan: nur wenn flach. Flip: Gegen-Signal schliesst und dreht."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or price is None:
        return
    mode = cfg.get("pm_direction_mode", "both")
    if mode == "long_only":
        sell_signal = False
    elif mode == "short_only":
        buy_signal = False
    if buy_signal == sell_signal:
        return
    target = "long" if buy_signal else "short"
    pos = st["position"]
    flip = cfg.get("pm_exit_mode", "flip") == "flip"
    if pos is not None:
        if not flip or pos == target:
            return
    elif time.time() < st.get("pm_sl_cooldown_until", 0.0):
        return
    p = _pm_params(cfg)
    levels = _mt_plan_levels(target, price, atr_now, p, need_tp=not flip)
    if levels is None:
        debug_log(f"⚠️ [{symbol}] Pure Momentum Signal {target.upper()} @ {price} übersprungen: SL/TP-Abstand ungültig (ATR=0 oder Plan ohne SL)")
        return
    if pos is not None:
        debug_log(f"🔄 [{symbol}] Pure Momentum Wechsel: {pos.upper()} -> {target.upper()} @ {price}")
        await execute_exit(symbol, price, "PM-FLIP")
        if st["position"] is not None:
            return
        _pm_reset_state(st)
    debug_log(f"📡 [{symbol}] Pure Momentum Signal: {target.upper()} @ {price}")
    await execute_entry(symbol, target, price, is_add_on=False)
    if st["position"] is None:
        return
    _pm_reset_state(st)
    sl, tp1, tp2, tp3 = levels
    st["pm_sl_price"] = sl
    if not flip:
        st["pm_tp1_price"], st["pm_tp2_price"], st["pm_tp3_price"] = tp1, tp2, tp3


async def pm_poll_loop(symbol):
    """Pure Momentum - Einstieg bei Kerzenschluss der Basis-Zeitebene (Farbwechsel), SL/TP laufend gegen den Live-Preis."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0
    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "pure_momentum" and cfg["bot_active"] and b["state"].get("session_started"):
                resolution = cfg.get("pm_resolution", "5m")
                p = _pm_params(cfg)
                min_needed = _pm_warmup(p) + 2
                needed_bars = min(1000, max(300, 4 * p["len"]))
                st = b["state"]
                closed_ts = None
                if resolution in SUB_MINUTE_RESOLUTIONS:
                    local = get_seconds_candles(st, SUB_MINUTE_RESOLUTIONS[resolution], needed_bars)
                    if local:
                        closed_ts, closed_o, closed_h, closed_l, closed_c = local
                else:
                    data = await fetch_candles_binance_multi(symbol, resolution, count_back=needed_bars, market_type=cfg.get("binance_market_type", "spot"))
                    if data:
                        timestamps, opens, highs, lows, closes = data[:5]
                        closed_ts, closed_o, closed_h, closed_l, closed_c = timestamps[:-1], opens[:-1], highs[:-1], lows[:-1], closes[:-1]

                now = time.time()
                due_heartbeat = now - last_heartbeat > 300
                if st["position"] is not None and st["last_price"] is not None:
                    await check_pm_sl_tp(symbol, st["last_price"])

                if closed_ts and len(closed_c) > min_needed:
                    age = (now * 1000 - closed_ts[-1]) / 1000
                    if age > 300:
                        debug_log(f"⚠️ [{symbol}] Pure Momentum: letzte Kerze wirkt veraltet ({round(age)}s alt, Auflösung {resolution}) - überspringe Durchlauf.")
                        closed_ts = None
                    elif st["last_price"] is not None and closed_c[-1] and abs(closed_c[-1] - st["last_price"]) / st["last_price"] * 100 > 2.0:
                        debug_log(f"⚠️ [{symbol}] Pure Momentum: Kerzendaten unplausibel (Kerze {closed_c[-1]} vs. Live {st['last_price']}) - überspringe Durchlauf.")
                        closed_ts = None

                if closed_ts and len(closed_c) > min_needed and (last_processed_ts is None or closed_ts[-1] > last_processed_ts):
                    mom = await asyncio.to_thread(pm_momentum_series, closed_h, closed_l, closed_c, p["len"])
                    buy, sell = pm_signals(mom, p)
                    atr = compute_atr(closed_h, closed_l, closed_c, p["atr_len"]) or [0.0] * len(closed_c)
                    st["pm_momentum"] = mom[-1]
                    if due_heartbeat:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] Pure Momentum aktiv: Momentum={None if mom[-1] is None else round(mom[-1], 3)}, "
                                  f"Schwellen {p['short']}/{p['long']}, Ausstieg={cfg.get('pm_exit_mode', 'flip')}, Preis={closed_c[-1]}")
                    if last_processed_ts is None:
                        new_indices = [len(closed_ts) - 1]
                    else:
                        new_indices = [i for i in range(len(closed_ts)) if closed_ts[i] > last_processed_ts]
                    for idx in new_indices:
                        price_i = st["last_price"] if (idx == len(closed_ts) - 1 and st["last_price"] is not None) else closed_c[idx]
                        last_processed_ts = closed_ts[idx]
                        await check_pm_entry(symbol, buy[idx], sell[idx], price_i, atr[idx])
                elif due_heartbeat and not closed_ts:
                    last_heartbeat = now
                    debug_log(f"⏳ [{symbol}] Pure Momentum wartet: keine Kerzen erhalten (Auflösung {resolution})")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Pure-Momentum-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        await asyncio.sleep(5)


async def run_pm_backtest(symbol, cfg, days, exclude_top_n=1):
    max_candles = BACKTEST_MAX_CANDLES.get("pure_momentum", 100_000)
    resolution = cfg.get("pm_resolution", "5m")
    if resolution in SUB_MINUTE_RESOLUTIONS:
        max_candles = min(max_candles, 5000)
    candles, err = await _fetch_cached_mo7_backtest_candles(symbol, resolution, days, max_candles, market_type=cfg.get("binance_market_type", "spot"))
    if err:
        return {"error": err}
    p = _pm_params(cfg)
    min_needed = _pm_warmup(p) + 10
    if not candles or len(candles[4]) < min_needed:
        return {"error": f"Zu wenig historische Kerzen für einen aussagekräftigen Backtest erhalten (mind. ~{min_needed} nötig)."}
    base_ts = candles[0]
    mom = await asyncio.to_thread(pm_momentum_series, candles[2], candles[3], candles[4], p["len"])
    buy, sell = pm_signals(mom, p)
    atr = compute_atr(candles[2], candles[3], candles[4], p["atr_len"]) or [0.0] * len(base_ts)
    trades = _simulate_mt_trades(candles, _pm_as_mt_cfg(cfg), buy, sell, atr, _pm_warmup(p), p)
    stats = summarize_backtest_trades(trades, exclude_top_n)
    actual_days = (base_ts[-1] - base_ts[0]) / 86_400_000
    return {
        "symbol": symbol, "entry_mode": "pure_momentum", "resolution": resolution,
        "requested_days": days, "actual_days_covered": round(actual_days, 1),
        "candles_processed": len(base_ts), "candle_cap": max_candles, "cache_used": False,
        "trend_candles_valid": sum(1 for x in mom if x is not None), "signals_buy": sum(buy), "signals_sell": sum(sell),
        "stats": stats,
        "stats_long": summarize_backtest_trades([t for t in trades if t["dir"] == "long"], exclude_top_n),
        "stats_short": summarize_backtest_trades([t for t in trades if t["dir"] == "short"], exclude_top_n),
        "trades": trades,
    }


def pm_chart_payload(ts, o, h, l, c, cfg, max_bars=1000):
    """Chart-Daten fuer das Bot-Dashboard: Kerzen, Momentum im unteren Fenster, Schwellen, Pfeile bei jedem Farbwechsel."""
    p = _pm_params(cfg)
    if len(ts) < 3:
        return None
    mom = pm_momentum_series(h, l, c, p["len"])
    buy, sell = pm_signals(mom, p)
    n = len(c)
    off = max(0, n - max_bars)
    candles, w1, markers = [], [], []
    for i in range(off, n):
        t = ts[i] // 1000
        candles.append({"time": t, "open": o[i], "high": h[i], "low": l[i], "close": c[i]})
        if mom[i] is not None:
            w1.append({"time": t, "value": round(mom[i], 4)})
        if buy[i]:
            markers.append({"time": t, "kind": "long", "price": c[i]})
        if sell[i]:
            markers.append({"time": t, "kind": "short", "price": c[i]})
    last = next((x for x in reversed(mom) if x is not None), 0.0)
    return {"candles": candles, "wt1": w1, "wt2": [], "markers": markers, "levels": [p["short"], p["long"]],
            "entry_mode": "mtf", "wt1v": round(last, 3), "wt2v": 0, "bars": n}
