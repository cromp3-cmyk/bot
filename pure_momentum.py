"""
pure_momentum.py - eigene Strategie "Pure Momentum Oscillator" (entry_mode "pure_momentum", Config-Praefix pm_*).

Portiert aus dem TradingView-Indikator "Pure Momentum Oscillator":
  cd  = 2*close - EMA(close, N)            (entspricht einer DEMA-artigen Entzerrung)
  cda = WMA(cd, N)                          (geglaettete Linie)
  Momentum = (cda - cda[1]) / ATR(N)        (ATR gehoert zur Indikator-Formel, kein SL/TP-Thema)
Farbe: Momentum > Long-Schwelle = GRUEN, Momentum < Short-Schwelle = ROT, dazwischen bleibt die letzte Farbe.
SIGNAL: bei JEDEM Farbwechsel auf der geschlossenen Kerze (gruen->rot = SELL, rot->gruen = BUY).
Die erste Farbe nach dem Start setzt nur den Ausgangszustand (kein Signal).
Ausstieg: Gegen-Signal dreht die Position; dazu optional fester Dollar-SL (Verlust der Position) und fester Dollar-TP
(Gewinn der Position), jeweils 0 = aus. Kein ATR, kein Plan-Modus.
"""

import asyncio
import time
import traceback

from bot_core import BOTS, debug_log, execute_entry, execute_exit
from strategies import (
    SUB_MINUTE_RESOLUTIONS, get_seconds_candles, fetch_candles_binance_multi, _ema_series, compute_atr,
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
    """Abfrage-Takt: 1 s bei offener Position (SL/TP/Trail), bei Sekunden-Kerzen und rund um den Kerzenschluss,
    sonst bis kurz vor den naechsten Kerzenschluss schlafen (max. 5 s)."""
    per = _res_seconds(res)
    if st.get("position") is not None or per <= 60:
        return 1.0
    into = time.time() % per
    if into < 6:
        return 1.0
    return max(1.0, min(5.0, per - into - 0.5))


def _pm_params(cfg):
    return {
        "len": max(2, int(cfg.get("pm_len", 45))),
        "long": float(cfg.get("pm_long", 0.05)), "short": float(cfg.get("pm_short", -0.05)),
        "sl_usd": max(0.0, float(cfg.get("pm_sl_usd", 5.0))), "tp_usd": max(0.0, float(cfg.get("pm_tp_usd", 5.0))),
        "trail_usd": max(0.0, float(cfg.get("pm_trail_usd", 0.0) or 0.0)),
    }


def _pm_warmup(p):
    return 3 * p["len"] + 5


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


def _pm_levels(direction, entry, size, p):
    """Fester Dollar-SL/-TP -> Preise (None = aus). size = Coin-Menge der Position."""
    if not size or size <= 0:
        return None, None
    sgn = 1 if direction == "long" else -1
    sl = entry - sgn * p["sl_usd"] / size if p["sl_usd"] > 0 else None
    tp = entry + sgn * p["tp_usd"] / size if p["tp_usd"] > 0 else None
    return sl, tp


def _pm_reset_state(st):
    st["pm_sl_price"] = None
    st["pm_trail_extreme"] = None
    st["pm_tp_price"] = None


async def check_pm_sl_tp(symbol, price):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if st["position"] is None or price is None:
        return
    pos = st["position"]
    sl_price, tp_price = st.get("pm_sl_price"), st.get("pm_tp_price")
    sl_reason = "SL"
    trail_usd = max(0.0, float(cfg.get("pm_trail_usd", 0) or 0))
    size = st.get("total_coin_size") or 0
    if trail_usd > 0 and size > 0:
        ext = st.get("pm_trail_extreme")
        if ext is None:
            ext = st.get("avg_entry_price") or price
        ext = max(ext, price) if pos == "long" else min(ext, price)
        st["pm_trail_extreme"] = ext
        tr = ext - trail_usd / size if pos == "long" else ext + trail_usd / size
        if sl_price is None or (pos == "long" and tr > sl_price) or (pos == "short" and tr < sl_price):
            sl_price, sl_reason = tr, "TRAIL"
    if sl_price is not None and ((pos == "long" and price <= sl_price) or (pos == "short" and price >= sl_price)):
        debug_log(f"🚪 [{symbol}] Pure Momentum SL/Trail ({sl_reason}): {pos.upper()} @ {price} (Ziel war {round(sl_price, 4)})")
        await execute_exit(symbol, price, sl_reason)
        if st["position"] is None:
            st["pm_sl_cooldown_until"] = time.time() + cfg.get("pm_sl_cooldown_seconds", 30)
            _pm_reset_state(st)
        return
    if tp_price is not None and ((pos == "long" and price >= tp_price) or (pos == "short" and price <= tp_price)):
        debug_log(f"🎯 [{symbol}] Pure Momentum TP: {pos.upper()} @ {price} (Ziel war {round(tp_price, 4)})")
        await execute_exit(symbol, price, "TP")
        if st["position"] is None:
            _pm_reset_state(st)


async def check_pm_entry(symbol, buy_signal, sell_signal, price):
    """Einstieg auf der Signal-Kerze (Farbwechsel). Gegen-Signal schliesst und dreht, gleiche Richtung wird ignoriert."""
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
    if pos is not None:
        if pos == target:
            return
    elif time.time() < st.get("pm_sl_cooldown_until", 0.0):
        return
    if pos is not None:
        debug_log(f"🔄 [{symbol}] Pure Momentum Farbwechsel: {pos.upper()} -> {target.upper()} @ {price}")
        await execute_exit(symbol, price, "PM-FLIP")
        if st["position"] is not None:
            return
        _pm_reset_state(st)
    debug_log(f"📡 [{symbol}] Pure Momentum Signal: {target.upper()} @ {price}")
    await execute_entry(symbol, target, price, is_add_on=False)
    if st["position"] is None:
        return
    _pm_reset_state(st)
    entry = st.get("avg_entry_price") or price
    st["pm_sl_price"], st["pm_tp_price"] = _pm_levels(target, entry, st.get("total_coin_size") or 0, _pm_params(cfg))
    st["pm_trail_extreme"] = entry


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
                    # Kerzen nur rund um den Kerzenschluss neu holen (dort jede Sekunde), sonst genuegt die Ruhe
                    _per = _res_seconds(resolution)
                    _need_fetch = last_processed_ts is None or (time.time() % _per) < 6 or _per <= 60
                    data = await fetch_candles_binance_multi(symbol, resolution, count_back=needed_bars, market_type=cfg.get("binance_market_type", "spot")) if _need_fetch else None
                    if data:
                        timestamps, opens, highs, lows, closes = data[:5]
                        closed_ts, closed_o, closed_h, closed_l, closed_c = timestamps[:-1], opens[:-1], highs[:-1], lows[:-1], closes[:-1]

                now = time.time()
                due_heartbeat = now - last_heartbeat > 300
                if st["position"] is not None and st["last_price"] is not None:
                    await check_pm_sl_tp(symbol, st["last_price"])

                if closed_ts and len(closed_c) > min_needed:
                    age = (now * 1000 - closed_ts[-1]) / 1000
                    if age > 300 + _res_seconds(resolution):
                        debug_log(f"⚠️ [{symbol}] Pure Momentum: letzte Kerze wirkt veraltet ({round(age)}s alt, Auflösung {resolution}) - überspringe Durchlauf.")
                        closed_ts = None
                    elif st["last_price"] is not None and closed_c[-1] and abs(closed_c[-1] - st["last_price"]) / st["last_price"] * 100 > 2.0:
                        debug_log(f"⚠️ [{symbol}] Pure Momentum: Kerzendaten unplausibel (Kerze {closed_c[-1]} vs. Live {st['last_price']}) - überspringe Durchlauf.")
                        closed_ts = None

                if closed_ts and len(closed_c) > min_needed and (last_processed_ts is None or closed_ts[-1] > last_processed_ts):
                    mom = await asyncio.to_thread(pm_momentum_series, closed_h, closed_l, closed_c, p["len"])
                    buy, sell = pm_signals(mom, p)
                    st["pm_momentum"] = mom[-1]
                    if due_heartbeat:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] Pure Momentum aktiv: Momentum={None if mom[-1] is None else round(mom[-1], 3)}, "
                                  f"Schwellen {p['short']}/{p['long']}, SL=${p['sl_usd']} TP=${p['tp_usd']}, Preis={closed_c[-1]}")
                    if last_processed_ts is None:
                        new_indices = [len(closed_ts) - 1]
                    else:
                        new_indices = [i for i in range(len(closed_ts)) if closed_ts[i] > last_processed_ts]
                    for idx in new_indices:
                        price_i = st["last_price"] if (idx == len(closed_ts) - 1 and st["last_price"] is not None) else closed_c[idx]
                        last_processed_ts = closed_ts[idx]
                        await check_pm_entry(symbol, buy[idx], sell[idx], price_i)
                elif due_heartbeat and not closed_ts and (resolution in SUB_MINUTE_RESOLUTIONS or _need_fetch):
                    last_heartbeat = now
                    debug_log(f"⏳ [{symbol}] Pure Momentum wartet: keine Kerzen erhalten (Auflösung {resolution})")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Pure-Momentum-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        await asyncio.sleep(_poll_delay(cfg.get("pm_resolution", "5m"), b["state"]))


def _pm_simulate(candles, cfg, buy_sig, sell_sig, warmup, p):
    """Backtest-Gegenstueck zu check_pm_entry/check_pm_sl_tp: Einstieg zum Schlusskurs der Signal-Kerze, SL/TP ab der
    naechsten Kerze ueber Hoch/Tief (SL gewinnt bei Gleichzeitigkeit), Gegen-Signal dreht."""
    ts, h, l, c = candles[0], candles[2], candles[3], candles[4]
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    cooldown_ms = cfg.get("pm_sl_cooldown_seconds", 30) * 1000
    mode = cfg.get("pm_direction_mode", "both")
    position, trades, cool_until = None, [], None
    for i in range(max(warmup, 1), n):
        if position is not None:
            pdir, entry, sl, tp = position["dir"], position["entry"], position["sl"], position["tp"]
            sl_reason = "SL"
            if p.get("trail_usd", 0) > 0:
                tr = position["ext"] - (1 if pdir == "long" else -1) * p["trail_usd"] / position["size"]
                if sl is None or (pdir == "long" and tr > sl) or (pdir == "short" and tr < sl):
                    sl, sl_reason = tr, "TRAIL"
            if sl is not None and ((pdir == "long" and l[i] <= sl) or (pdir == "short" and h[i] >= sl)):
                _bt_close_trade(trades, pdir, entry, sl, position["size"], i, position["entry_i"], sl_reason, ts=ts)
                position = None
                cool_until = ts[i] + cooldown_ms
            elif tp is not None and ((pdir == "long" and h[i] >= tp) or (pdir == "short" and l[i] <= tp)):
                _bt_close_trade(trades, pdir, entry, tp, position["size"], i, position["entry_i"], "TP", ts=ts)
                position = None
            else:
                position["ext"] = max(position["ext"], h[i]) if pdir == "long" else min(position["ext"], l[i])
        buy, sell = buy_sig[i], sell_sig[i]
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
            _bt_close_trade(trades, position["dir"], position["entry"], price, position["size"], i, position["entry_i"], "PM-FLIP", ts=ts)
            position = None
        elif cool_until is not None and ts[i] < cool_until:
            continue
        size = (margin * leverage) / price
        sl, tp = _pm_levels(target, price, size, p)
        position = {"dir": target, "entry": price, "size": size, "entry_i": i, "sl": sl, "tp": tp, "ext": price}
    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)
    return trades


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
    trades = _pm_simulate(candles, cfg, buy, sell, _pm_warmup(p), p)
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
