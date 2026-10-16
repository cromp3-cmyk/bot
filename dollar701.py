"""
dollar701.py - eigene Strategie "Dollar701 SuperTrend" (entry_mode "dollar701", Config-Praefix dv_*).

Portiert aus dem TradingView-Indikator "Dollar701 - Allround" NUR das Buy/Sell-Signal (SuperTrend); Smart Trail, Reversal
Cloud, Trend Follower, Zonen, CHOCH und das Datenpanel sind reine Anzeige und nicht portiert.
  Perioden   = round((Sensitivity + 1) * 10)        (Pine: Sensitivity ist ganzzahlig 1-15; hier mit 2 Nachkommastellen)
  Multiplikator = (Sensitivity + 1) / 2
  src = hl2, ATR = Wilder-ATR(Perioden)
  up = src - M*ATR (nur steigend, solange close[1] > up[1]);  dn = src + M*ATR (nur fallend, solange close[1] < dn[1])
  Trend wird gruen, wenn close > dn[1]; rot, wenn close < up[1].   BUY = Wechsel auf gruen, SELL = Wechsel auf rot.
Signal auf der geschlossenen Kerze. Ausstieg: Gegen-Signal dreht die Position; dazu optional fester Dollar-SL und
Dollar-TP (0 = aus). Dazu ein Monte-Carlo-Backtest ueber Sensitivity x SL x TP (run_dv_montecarlo).
"""

import asyncio
import random
import time
import traceback

from bot_core import BOTS, debug_log, execute_entry, execute_exit, execute_reverse
from strategies import (
    SUB_MINUTE_RESOLUTIONS, get_seconds_candles, fetch_candles_binance_multi, compute_atr,
    summarize_backtest_trades, _fetch_cached_mo7_backtest_candles, BACKTEST_MAX_CANDLES, _bt_close_trade,
)


def _dv_params(cfg):
    sens = min(15.0, max(1.0, round(float(cfg.get("dv_sens", 7.0)), 2)))
    return {
        "sens": sens, "periods": _dv_periods(sens),
        "sl_usd": max(0.0, float(cfg.get("dv_sl_usd", 5.0))), "tp_usd": max(0.0, float(cfg.get("dv_tp_usd", 5.0))),
        "trail_usd": max(0.0, float(cfg.get("dv_trail_usd", 0.0) or 0.0)),
        "reverse": cfg.get("dv_exit_mode", "sltp") == "reverse",
    }


def _dv_periods(sens):
    return max(2, int(round((sens + 1.0) * 10)))


def _dv_warmup(p):
    return p["periods"] + 10


def dv_supertrend(h, l, c, sens, atr=None):
    """-> (trend[] (+1/-1), buy[], sell[]). Exakt die Pine-Logik; atr optional vorberechnet (Monte Carlo)."""
    n = len(c)
    periods = _dv_periods(sens)
    mult = (sens + 1.0) / 2.0
    if atr is None:
        atr = compute_atr(h, l, c, periods) or [0.0] * n
    trend = [1] * n
    buy, sell = [False] * n, [False] * n
    up_prev = dn_prev = None
    tr = 1
    for i in range(n):
        src = (h[i] + l[i]) / 2.0
        up = src - mult * atr[i]
        dn = src + mult * atr[i]
        up1 = up if up_prev is None else up_prev
        dn1 = dn if dn_prev is None else dn_prev
        if i > 0 and c[i - 1] > up1:
            up = max(up, up1)
        if i > 0 and c[i - 1] < dn1:
            dn = min(dn, dn1)
        prev = tr
        if tr == -1 and c[i] > dn1:
            tr = 1
        elif tr == 1 and c[i] < up1:
            tr = -1
        trend[i] = tr
        if i > 0:
            if tr == 1 and prev == -1:
                buy[i] = True
            elif tr == -1 and prev == 1:
                sell[i] = True
        up_prev, dn_prev = up, dn
    return trend, buy, sell


def _dv_levels(direction, entry, size, p):
    """Fester Dollar-SL/-TP -> Preise (None = aus). size = Coin-Menge der Position."""
    if not size or size <= 0:
        return None, None
    sgn = 1 if direction == "long" else -1
    sl = entry - sgn * p["sl_usd"] / size if p["sl_usd"] > 0 else None
    tp = entry + sgn * p["tp_usd"] / size if p["tp_usd"] > 0 else None
    return sl, tp


def _dv_reset_state(st):
    st["dv_sl_price"] = None
    st["dv_trail_extreme"] = None
    st["dv_tp_price"] = None


async def check_dv_sl_tp(symbol, price):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if st["position"] is None or price is None:
        return
    if cfg.get("dv_exit_mode", "sltp") == "reverse":
        return  # Reverse-Modus: kein SL/TP/Trail, nur Gegen-Signal
    pos = st["position"]
    sl_price, tp_price = st.get("dv_sl_price"), st.get("dv_tp_price")
    sl_reason = "SL"
    trail_usd = max(0.0, float(cfg.get("dv_trail_usd", 0) or 0))
    size = st.get("total_coin_size") or 0
    if trail_usd > 0 and size > 0:
        ext = st.get("dv_trail_extreme")
        if ext is None:
            ext = st.get("avg_entry_price") or price
        ext = max(ext, price) if pos == "long" else min(ext, price)
        st["dv_trail_extreme"] = ext
        tr = ext - trail_usd / size if pos == "long" else ext + trail_usd / size
        if sl_price is None or (pos == "long" and tr > sl_price) or (pos == "short" and tr < sl_price):
            sl_price, sl_reason = tr, "TRAIL"
    if sl_price is not None and ((pos == "long" and price <= sl_price) or (pos == "short" and price >= sl_price)):
        debug_log(f"🚪 [{symbol}] Dollar701 SL/Trail ({sl_reason}): {pos.upper()} @ {price} (Ziel war {round(sl_price, 4)})")
        await execute_exit(symbol, price, sl_reason)
        if st["position"] is None:
            st["dv_sl_cooldown_until"] = time.time() + cfg.get("dv_sl_cooldown_seconds", 30)
            _dv_reset_state(st)
        return
    if tp_price is not None and ((pos == "long" and price >= tp_price) or (pos == "short" and price <= tp_price)):
        debug_log(f"🎯 [{symbol}] Dollar701 TP: {pos.upper()} @ {price} (Ziel war {round(tp_price, 4)})")
        await execute_exit(symbol, price, "TP")
        if st["position"] is None:
            _dv_reset_state(st)


async def check_dv_entry(symbol, buy_signal, sell_signal, price):
    """Einstieg auf der Signal-Kerze (Farbwechsel). Gegen-Signal schliesst und dreht, gleiche Richtung wird ignoriert."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or price is None:
        return
    mode = cfg.get("dv_direction_mode", "both")
    reverse = cfg.get("dv_exit_mode", "sltp") == "reverse"
    pos = st["position"]
    if reverse and pos is not None and buy_signal != sell_signal:
        # Reverse-Modus mit eingeschraenkter Richtung: Gegen-Signal schliesst, ohne gegenlaeufig neu zu oeffnen
        want = "long" if buy_signal else "short"
        if want != pos and ((mode == "long_only" and want == "short") or (mode == "short_only" and want == "long")):
            debug_log(f"🔄 [{symbol}] Dollar701 Reverse: {pos.upper()} schliessen @ {price}")
            await execute_exit(symbol, price, "DV-FLIP")
            if st["position"] is None:
                _dv_reset_state(st)
            return
    if mode == "long_only":
        sell_signal = False
    elif mode == "short_only":
        buy_signal = False
    if buy_signal == sell_signal:
        return
    target = "long" if buy_signal else "short"
    if pos is not None:
        if pos == target:
            return
    elif time.time() < st.get("dv_sl_cooldown_until", 0.0):
        return
    if pos is not None and reverse:
        # Reverse-Modus: EINE Market-Order dreht die Position direkt (schneller Wechsel, nie flat)
        debug_log(f"⚡ [{symbol}] Dollar701 Reverse: {pos.upper()} -> {target.upper()} @ {price}")
        if await execute_reverse(symbol, price, "DV-FLIP", size_multiplier=1.0):
            _dv_reset_state(st)
            st["dv_trail_extreme"] = st.get("avg_entry_price") or price
        return
    if pos is not None:
        debug_log(f"🔄 [{symbol}] Dollar701 Farbwechsel: {pos.upper()} -> {target.upper()} @ {price}")
        await execute_exit(symbol, price, "DV-FLIP")
        if st["position"] is not None:
            return
        _dv_reset_state(st)
    debug_log(f"📡 [{symbol}] Dollar701 Signal: {target.upper()} @ {price}")
    await execute_entry(symbol, target, price, is_add_on=False)
    if st["position"] is None:
        return
    _dv_reset_state(st)
    entry = st.get("avg_entry_price") or price
    if cfg.get("dv_exit_mode", "sltp") != "reverse":
        st["dv_sl_price"], st["dv_tp_price"] = _dv_levels(target, entry, st.get("total_coin_size") or 0, _dv_params(cfg))
    st["dv_trail_extreme"] = entry


async def dv_poll_loop(symbol):
    """Dollar701 - Einstieg bei Kerzenschluss der Basis-Zeitebene (Farbwechsel), SL/TP laufend gegen den Live-Preis."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0
    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "dollar701" and cfg["bot_active"] and b["state"].get("session_started"):
                resolution = cfg.get("dv_resolution", "5m")
                p = _dv_params(cfg)
                min_needed = _dv_warmup(p) + 2
                needed_bars = min(1000, max(300, p["periods"] * 3))
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
                    await check_dv_sl_tp(symbol, st["last_price"])

                if closed_ts and len(closed_c) > min_needed:
                    age = (now * 1000 - closed_ts[-1]) / 1000
                    if age > 300:
                        debug_log(f"⚠️ [{symbol}] Dollar701: letzte Kerze wirkt veraltet ({round(age)}s alt, Auflösung {resolution}) - überspringe Durchlauf.")
                        closed_ts = None
                    elif st["last_price"] is not None and closed_c[-1] and abs(closed_c[-1] - st["last_price"]) / st["last_price"] * 100 > 2.0:
                        debug_log(f"⚠️ [{symbol}] Dollar701: Kerzendaten unplausibel (Kerze {closed_c[-1]} vs. Live {st['last_price']}) - überspringe Durchlauf.")
                        closed_ts = None

                if closed_ts and len(closed_c) > min_needed and (last_processed_ts is None or closed_ts[-1] > last_processed_ts):
                    trend_arr, buy, sell = await asyncio.to_thread(dv_supertrend, closed_h, closed_l, closed_c, p["sens"])
                    st["dv_trend"] = trend_arr[-1]
                    if due_heartbeat:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] Dollar701 aktiv: Trend={'grün' if trend_arr[-1] == 1 else 'rot'}, "
                                  f"Sensitivity={p['sens']}, SL=${p['sl_usd']} TP=${p['tp_usd']}, Preis={closed_c[-1]}")
                    if last_processed_ts is None:
                        new_indices = [len(closed_ts) - 1]
                    else:
                        new_indices = [i for i in range(len(closed_ts)) if closed_ts[i] > last_processed_ts]
                    for idx in new_indices:
                        price_i = st["last_price"] if (idx == len(closed_ts) - 1 and st["last_price"] is not None) else closed_c[idx]
                        last_processed_ts = closed_ts[idx]
                        await check_dv_entry(symbol, buy[idx], sell[idx], price_i)
                elif due_heartbeat and not closed_ts:
                    last_heartbeat = now
                    debug_log(f"⏳ [{symbol}] Dollar701 wartet: keine Kerzen erhalten (Auflösung {resolution})")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Dollar701-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        await asyncio.sleep(5)


def _dv_simulate(candles, cfg, buy_sig, sell_sig, warmup, p):
    """Backtest-Gegenstueck zu check_dv_entry/check_dv_sl_tp: Einstieg zum Schlusskurs der Signal-Kerze, SL/TP ab der
    naechsten Kerze ueber Hoch/Tief (SL gewinnt bei Gleichzeitigkeit), Gegen-Signal dreht."""
    ts, h, l, c = candles[0], candles[2], candles[3], candles[4]
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    cooldown_ms = cfg.get("dv_sl_cooldown_seconds", 30) * 1000
    mode = cfg.get("dv_direction_mode", "both")
    position, trades, cool_until = None, [], None
    for i in range(max(warmup, 1), n):
        if position is not None and not p.get("reverse"):
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
        if p.get("reverse") and position is not None and buy != sell:
            want = "long" if buy else "short"
            if want != position["dir"] and ((mode == "long_only" and want == "short") or (mode == "short_only" and want == "long")):
                _bt_close_trade(trades, position["dir"], position["entry"], c[i], position["size"], i, position["entry_i"], "DV-FLIP", ts=ts)
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
            _bt_close_trade(trades, position["dir"], position["entry"], price, position["size"], i, position["entry_i"], "DV-FLIP", ts=ts)
            position = None
        elif cool_until is not None and ts[i] < cool_until:
            continue
        size = (margin * leverage) / price
        sl, tp = (None, None) if p.get("reverse") else _dv_levels(target, price, size, p)
        position = {"dir": target, "entry": price, "size": size, "entry_i": i, "sl": sl, "tp": tp, "ext": price}
    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)
    return trades


async def run_dv_backtest(symbol, cfg, days, exclude_top_n=1):
    max_candles = BACKTEST_MAX_CANDLES.get("dollar701", 100_000)
    resolution = cfg.get("dv_resolution", "5m")
    if resolution in SUB_MINUTE_RESOLUTIONS:
        max_candles = min(max_candles, 5000)
    candles, err = await _fetch_cached_mo7_backtest_candles(symbol, resolution, days, max_candles, market_type=cfg.get("binance_market_type", "spot"))
    if err:
        return {"error": err}
    p = _dv_params(cfg)
    min_needed = _dv_warmup(p) + 10
    if not candles or len(candles[4]) < min_needed:
        return {"error": f"Zu wenig historische Kerzen für einen aussagekräftigen Backtest erhalten (mind. ~{min_needed} nötig)."}
    base_ts = candles[0]
    trend_arr, buy, sell = await asyncio.to_thread(dv_supertrend, candles[2], candles[3], candles[4], p["sens"])
    trades = _dv_simulate(candles, cfg, buy, sell, _dv_warmup(p), p)
    stats = summarize_backtest_trades(trades, exclude_top_n)
    actual_days = (base_ts[-1] - base_ts[0]) / 86_400_000
    return {
        "symbol": symbol, "entry_mode": "dollar701", "resolution": resolution,
        "requested_days": days, "actual_days_covered": round(actual_days, 1),
        "candles_processed": len(base_ts), "candle_cap": max_candles, "cache_used": False,
        "trend_candles_valid": len(trend_arr), "signals_buy": sum(buy), "signals_sell": sum(sell),
        "stats": stats,
        "stats_long": summarize_backtest_trades([t for t in trades if t["dir"] == "long"], exclude_top_n),
        "stats_short": summarize_backtest_trades([t for t in trades if t["dir"] == "short"], exclude_top_n),
        "trades": trades,
    }


def dv_chart_payload(ts, o, h, l, c, cfg, max_bars=1000):
    """Chart-Daten fuer das Bot-Dashboard: Kerzen + Pfeile bei jedem SuperTrend-Wechsel."""
    p = _dv_params(cfg)
    if len(ts) < 3:
        return None
    trend_arr, buy, sell = dv_supertrend(h, l, c, p["sens"])
    n = len(c)
    off = max(0, n - max_bars)
    candles, markers = [], []
    for i in range(off, n):
        t = ts[i] // 1000
        candles.append({"time": t, "open": o[i], "high": h[i], "low": l[i], "close": c[i]})
        if buy[i]:
            markers.append({"time": t, "kind": "long", "price": c[i]})
        if sell[i]:
            markers.append({"time": t, "kind": "short", "price": c[i]})
    return {"candles": candles, "wt1": [], "wt2": [], "markers": markers, "levels": [],
            "entry_mode": "dv", "wt1v": 0, "wt2v": 0, "bars": n}


# ============================================================
# Monte-Carlo-Backtest: zufaellige Kombinationen aus Sensitivity x SL ($) x TP ($) auf denselben Kerzen
# ============================================================
DV_MC_MAX_RUNS = 2000
DV_MC_MIN_RELIABLE_TRADES = 5


def _dv_mc_compute(candles, base_cfg, combos, warmup_extra, exclude_top_n, deadline):
    h, l, c = candles[2], candles[3], candles[4]
    atr_cache, st_cache, out = {}, {}, []
    for sens, sl, tp in combos:
        if time.time() > deadline:
            break
        per = _dv_periods(sens)
        if per not in atr_cache:
            atr_cache[per] = compute_atr(h, l, c, per) or [0.0] * len(c)
        if sens not in st_cache:
            st_cache[sens] = dv_supertrend(h, l, c, sens, atr_cache[per])
        _, buy, sell = st_cache[sens]
        p = {"sens": sens, "periods": per, "sl_usd": sl, "tp_usd": tp, "trail_usd": max(0.0, float(base_cfg.get("dv_trail_usd", 0) or 0)), "reverse": base_cfg.get("dv_exit_mode", "sltp") == "reverse"}
        trades = _dv_simulate(candles, base_cfg, buy, sell, per + 10, p)
        st = summarize_backtest_trades(trades, exclude_top_n)
        out.append({"dv_sens": sens, "dv_sl_usd": sl, "dv_tp_usd": tp, "trades": st["trades"], "win_rate_pct": st["win_rate_pct"],
                    "total_pnl_usd": st["total_pnl_usd"], "total_pnl_excl_top_n_usd": st["total_pnl_excl_top_n_usd"],
                    "max_drawdown_usd": st["max_drawdown_usd"], "avg_bars_held": st["avg_bars_held"]})
    return out


async def run_dv_montecarlo(symbol, cfg, days, runs=300, sens_min=3.0, sens_max=12.0, sl_min=1.0, sl_max=10.0,
                            tp_min=1.0, tp_max=10.0, exclude_top_n=1, seed=None):
    """Zieht `runs` zufaellige Kombinationen (Sensitivity mit 2 Nachkommastellen, SL/TP in 0,1-$-Schritten, 0 = aus ist
    NICHT dabei) und simuliert jede auf denselben Kerzen. Alles andere (Zeitrahmen, Richtung, Cooldown, Margin, Hebel)
    kommt aus der Config."""
    max_candles = BACKTEST_MAX_CANDLES.get("dollar701", 100_000)
    resolution = cfg.get("dv_resolution", "5m")
    if resolution in SUB_MINUTE_RESOLUTIONS:
        max_candles = min(max_candles, 5000)
    candles, err = await _fetch_cached_mo7_backtest_candles(symbol, resolution, days, max_candles, market_type=cfg.get("binance_market_type", "spot"))
    if err:
        return {"error": err}
    n = len(candles[4]) if candles else 0
    if n < 200:
        return {"error": "Zu wenig historische Kerzen für einen aussagekräftigen Monte-Carlo-Test erhalten (mind. ~200 nötig)."}
    runs = max(1, min(DV_MC_MAX_RUNS, int(runs)))
    sens_min, sens_max = sorted((min(15.0, max(1.0, float(sens_min))), min(15.0, max(1.0, float(sens_max)))))
    sl_min, sl_max = sorted((max(0.1, float(sl_min)), max(0.1, float(sl_max))))
    tp_min, tp_max = sorted((max(0.1, float(tp_min)), max(0.1, float(tp_max))))
    rng = random.Random(seed)
    reverse = cfg.get("dv_exit_mode", "sltp") == "reverse"   # Flip-Modus: kein SL/TP -> nur Sensitivity wird variiert
    seen, combos = set(), []
    for _ in range(runs * 5):
        if len(combos) >= runs:
            break
        if reverse:
            k = (round(rng.uniform(sens_min, sens_max), 2), 0.0, 0.0)
        else:
            k = (round(rng.uniform(sens_min, sens_max), 2), round(rng.uniform(sl_min, sl_max), 1), round(rng.uniform(tp_min, tp_max), 1))
        if k not in seen:
            seen.add(k)
            combos.append(k)
    deadline = time.time() + 240
    results = await asyncio.to_thread(_dv_mc_compute, candles, cfg, combos, 0, exclude_top_n, deadline)
    rank_key = lambda r: (r["trades"] >= DV_MC_MIN_RELIABLE_TRADES, r["total_pnl_usd"])
    best = sorted(results, key=rank_key, reverse=True)
    worst = sorted(results, key=lambda r: r["total_pnl_usd"])
    pnls = sorted(r["total_pnl_usd"] for r in results)
    pos = sum(1 for x in pnls if x > 0)
    median = pnls[len(pnls) // 2] if pnls else 0
    actual_days = (candles[0][-1] - candles[0][0]) / 86_400_000
    return {
        "exit_mode": "reverse" if reverse else "sltp", "symbol": symbol, "resolution": resolution, "requested_days": days, "actual_days_covered": round(actual_days, 1),
        "candles_processed": n, "min_reliable_trades": DV_MC_MIN_RELIABLE_TRADES,
        "combos_tested": len(results), "combos_requested": runs, "profitable_pct": round(pos / len(results) * 100, 1) if results else 0,
        "median_pnl_usd": round(median, 2), "best_pnl_usd": pnls[-1] if pnls else 0, "worst_pnl_usd": pnls[0] if pnls else 0,
        "results": best[:30], "worst_results": worst[:20],
    }
