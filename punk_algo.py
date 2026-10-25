"""
punk_algo.py - eigene Strategie "Punk Algo" (entry_mode "punk_algo", Config-Praefix pa_*).

Portiert aus dem TradingView-Indikator "Punk Algo" NUR die Bausteine, die Einstiege/Ausstiege steuern:
  Einstieg : Range-Filter-Dreiecke  ▲ (bull) / ▼ (bear), auf der GESCHLOSSENEN Kerze (Pine nutzt close -> kein Repaint)
  Ausstieg : Gegen-Dreieck dreht die Position (hat Vorrang), ODER das WaveTrend-"X"
             (cyan X beendet Shorts, pinkes X beendet Longs; wavetrend(hlc3, 9, 12), wt2 = SMA(wt1, 3), Zone +-53),
             dazu optional fester Dollar-SL / -TP (0 = aus).
Nicht portiert (beeinflussen die Einstiege im Original nicht): EMA144, Dashboard, RSI, Intelligent Trend, Momentum-Kerzen.

Range-Filter (Pine 1:1):
  smoothrng(x, t, m) = ema(ema(|x - x[1]|, t), 2t-1) * m
  smrng = (smoothrng(close, P1, M1) + smoothrng(close, P2, M2)) / 2
  filt  = nachziehende Linie um close (rngfilt), up/dn = Zaehler aufeinanderfolgender Anstiege/Abfaelle
  bull  = bullCond UND vorheriger Zustand war bear (und umgekehrt)
Standard laut Pine-Script: 27 / 9,1 / 55 / 1,3.

Dazu ein Monte-Carlo-Backtest (run_pa_montecarlo) ueber die vier Range-Filter-Parameter mit Train/Test-Trennung:
die ersten 70 % des Zeitraums sind "Training", die letzten 30 % "Test" - eine Kombination, die nur im Training glaenzt,
ist ueberangepasst.
"""

import asyncio
from array import array
import random
import time
import traceback

from bot_core import BOTS, debug_log, execute_entry, execute_exit, execute_reverse
from strategies import (
    SUB_MINUTE_RESOLUTIONS, get_seconds_candles, fetch_candles_binance_multi, _ema_series, compute_wavetrend_series,
    summarize_backtest_trades, _fetch_cached_mo7_backtest_candles, BACKTEST_MAX_CANDLES, _bt_close_trade,
)

PA_DEFAULT = (27, 9.1, 55, 1.3)   # (Periode 1, Multiplikator 1, Periode 2, Multiplikator 2) laut Pine-Script


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


def _pa_params(cfg):
    mode = cfg.get("pa_direction_mode", "both")
    return {
        "p1": max(2, int(cfg.get("pa_period1", 27) or 27)), "m1": max(0.1, float(cfg.get("pa_mult1", 9.1) or 9.1)),
        "p2": max(2, int(cfg.get("pa_period2", 55) or 55)), "m2": max(0.1, float(cfg.get("pa_mult2", 1.3) or 1.3)),
        "level": max(1.0, float(cfg.get("pa_wt_level", 53) or 53)),
        "wt_exit": bool(cfg.get("pa_wt_exit_enabled", True)),
        "direction": mode if mode in ("both", "long_only", "short_only") else "both",
        "sl_usd": max(0.0, float(cfg.get("pa_sl_usd", 0.0) or 0.0)),
        "tp_usd": max(0.0, float(cfg.get("pa_tp_usd", 0.0) or 0.0)),
    }


def _pa_warmup(p1, p2):
    """EMA-Ketten brauchen ca. 4x ihre Laenge, bis sie eingeschwungen sind."""
    return max(p1, p2) * 4 + 20


# ============================================================================
# RECHNUNG (rein, ohne Netzwerk)
# ============================================================================
def pa_smoothrng_base(closes, t):
    """smoothrng OHNE den Multiplikator (der wird beim Zusammensetzen angewendet) - so rechnet der Monte-Carlo die teure
    EMA-Kette je Periode nur EINMAL."""
    n = len(closes)
    if n < 2:
        return [0.0] * n
    diffs = [abs(closes[i] - closes[i - 1]) for i in range(1, n)]
    base = _ema_series(_ema_series(diffs, t), t * 2 - 1)
    return [base[0]] + base   # Bar 0 hat in Pine keinen Wert (x[1] = na)


def pa_range_signals(closes, p1, m1, p2, m2, base1=None, base2=None):
    """-> (bull[], bear[]): True = Dreieck auf dieser Kerze."""
    n = len(closes)
    bull, bear = [False] * n, [False] * n
    if n < 3:
        return bull, bear
    if base1 is None:
        base1 = pa_smoothrng_base(closes, p1)
    if base2 is None:
        base2 = pa_smoothrng_base(closes, p2)
    filt_prev = closes[0]
    up = dn = 0
    last_cond = 0
    for i in range(1, n):
        x, x_prev = closes[i], closes[i - 1]
        r = (base1[i] * m1 + base2[i] * m2) / 2.0
        prev = filt_prev
        if x > prev:
            f = prev if (x - r) < prev else (x - r)
        else:
            f = prev if (x + r) > prev else (x + r)
        if f > prev:
            up += 1
            dn = 0
        elif f < prev:
            up = 0
            dn += 1
        bull_cond = x > f and x != x_prev and up > 0   # Pine: (x>f and x>x[1] and up>0) or (x>f and x<x[1] and up>0)
        bear_cond = x < f and x != x_prev and dn > 0
        bull[i] = bull_cond and last_cond == -1
        bear[i] = bear_cond and last_cond == 1
        if bull_cond:
            last_cond = 1
        elif bear_cond:
            last_cond = -1
        filt_prev = f
    return bull, bear


def pa_wt_exits(o, h, l, c, level=53.0):
    """WaveTrend-X: -> (exit_long[], exit_short[]).
    Cyan X : wt1 kreuzt wt2 nach oben UND wt2 <= -level -> beendet SHORTS
    Pink X : wt1 kreuzt wt2 nach unten UND wt2 >= level -> beendet LONGS"""
    n = len(c)
    xl, xs = [False] * n, [False] * n
    if n < 3:
        return xl, xs
    wt1, wt2 = compute_wavetrend_series(o, h, l, c, 9, 12, 3, "hlc3", 1.0)
    for i in range(1, n):
        if wt1[i] > wt2[i] and wt1[i - 1] <= wt2[i - 1] and wt2[i] <= -level:
            xs[i] = True
        elif wt1[i] < wt2[i] and wt1[i - 1] >= wt2[i - 1] and wt2[i] >= level:
            xl[i] = True
    return xl, xs


def _pa_levels(direction, entry, size, p):
    """Fester Dollar-SL/-TP -> Preise (None = aus)."""
    if not size or size <= 0:
        return None, None
    sgn = 1 if direction == "long" else -1
    sl = entry - sgn * p["sl_usd"] / size if p["sl_usd"] > 0 else None
    tp = entry + sgn * p["tp_usd"] / size if p["tp_usd"] > 0 else None
    return sl, tp


def _allowed(target, p):
    return not ((target == "long" and p["direction"] == "short_only") or (target == "short" and p["direction"] == "long_only"))


# ============================================================================
# LIVE
# ============================================================================
def _pa_reset_state(st):
    st["pa_sl_price"] = None
    st["pa_tp_price"] = None


async def check_pa_sl_tp(symbol, price):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    pos = st["position"]
    if pos is None or price is None:
        return
    sl, tp = st.get("pa_sl_price"), st.get("pa_tp_price")
    if sl is not None and ((pos == "long" and price <= sl) or (pos == "short" and price >= sl)):
        debug_log(f"🚪 [{symbol}] Punk Algo SL: {pos.upper()} @ {price} (Ziel war {round(sl, 6)})")
        await execute_exit(symbol, price, "SL")
        if st["position"] is None:
            st["pa_sl_cooldown_until"] = time.time() + cfg.get("pa_sl_cooldown_seconds", 30)
            _pa_reset_state(st)
        return
    if tp is not None and ((pos == "long" and price >= tp) or (pos == "short" and price <= tp)):
        debug_log(f"🎯 [{symbol}] Punk Algo TP: {pos.upper()} @ {price} (Ziel war {round(tp, 6)})")
        await execute_exit(symbol, price, "TP")
        if st["position"] is None:
            _pa_reset_state(st)


def _set_levels(st, cfg, target, price):
    entry = st.get("avg_entry_price") or price
    st["pa_sl_price"], st["pa_tp_price"] = _pa_levels(target, entry, st.get("total_coin_size") or 0, _pa_params(cfg))


async def check_pa_signal(symbol, bull_i, bear_i, xl_i, xs_i, price):
    """Auswertung der neu geschlossenen Kerze. Prioritaet: Gegen-Dreieck (Flip) vor WaveTrend-X. Nach einem X-Ausstieg
    bleibt der Bot flach, bis das naechste Dreieck kommt."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or price is None:
        return
    p = _pa_params(cfg)
    pos = st["position"]
    tri = "long" if bull_i else "short" if bear_i else None
    if pos is None:
        if tri is None or not _allowed(tri, p) or time.time() < st.get("pa_sl_cooldown_until", 0.0):
            return
        debug_log(f"📡 [{symbol}] Punk Algo Einstieg: {tri.upper()} @ {price}")
        await execute_entry(symbol, tri, price, is_add_on=False)
        if st["position"] is not None:
            _pa_reset_state(st)
            _set_levels(st, cfg, tri, price)
        return
    if tri is not None and tri != pos:
        if _allowed(tri, p):
            debug_log(f"🔄 [{symbol}] Punk Algo Flip: {pos.upper()} -> {tri.upper()} @ {price}")
            if await execute_reverse(symbol, price, "PA-FLIP", size_multiplier=1.0):
                _pa_reset_state(st)
                _set_levels(st, cfg, tri, price)
        else:
            debug_log(f"🚪 [{symbol}] Punk Algo Gegen-Dreieck (Richtung gesperrt): {pos.upper()} schließen @ {price}")
            await execute_exit(symbol, price, "PA-EXIT-DIR")
            if st["position"] is None:
                _pa_reset_state(st)
        return
    if p["wt_exit"] and ((pos == "long" and xl_i) or (pos == "short" and xs_i)):
        debug_log(f"🎯 [{symbol}] Punk Algo WaveTrend-X: {pos.upper()} beendet @ {price}")
        await execute_exit(symbol, price, "PA-WT-X")
        if st["position"] is None:
            _pa_reset_state(st)


async def pa_poll_loop(symbol):
    """Punk Algo live: wertet NUR abgeschlossene Kerzen aus, Ausfuehrung zum aktuellen Preis nach Kerzenschluss;
    SL/TP laufend gegen den Live-Preis."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0
    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "punk_algo" and cfg["bot_active"] and b["state"].get("session_started"):
                resolution = cfg.get("pa_resolution", "5m")
                p = _pa_params(cfg)
                min_needed = _pa_warmup(p["p1"], p["p2"]) + 2
                needed_bars = min(1000, max(min_needed * 2, 500))
                st = b["state"]
                closed_ts = None
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
                    await check_pa_sl_tp(symbol, st["last_price"])

                if closed_ts and len(closed_c) > min_needed:
                    age = (now * 1000 - closed_ts[-1]) / 1000
                    if age > 300 + _res_seconds(resolution):
                        debug_log(f"⚠️ [{symbol}] Punk Algo: letzte Kerze wirkt veraltet ({round(age)}s alt, Auflösung {resolution}) - überspringe Durchlauf.")
                        closed_ts = None
                    elif st["last_price"] is not None and closed_c[-1] and abs(closed_c[-1] - st["last_price"]) / st["last_price"] * 100 > 2.0:
                        debug_log(f"⚠️ [{symbol}] Punk Algo: Kerzendaten unplausibel (Kerze {closed_c[-1]} vs. Live {st['last_price']}) - überspringe Durchlauf.")
                        closed_ts = None

                if closed_ts and len(closed_c) > min_needed and (last_processed_ts is None or closed_ts[-1] > last_processed_ts):
                    def _calc():
                        bu, be = pa_range_signals(closed_c, p["p1"], p["m1"], p["p2"], p["m2"])
                        xl, xs = pa_wt_exits(closed_o, closed_h, closed_l, closed_c, p["level"])
                        return bu, be, xl, xs
                    bull, bear, xl, xs = await asyncio.to_thread(_calc)
                    if due_heartbeat:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] Punk Algo aktiv: {resolution}, Range-Filter {p['p1']}/{p['m1']}/{p['p2']}/{p['m2']}, "
                                  f"WT-X={'an' if p['wt_exit'] else 'aus'}, SL=${p['sl_usd']} TP=${p['tp_usd']}, Preis={closed_c[-1]}")
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
                            continue   # erste Kerze nach dem Start nur merken (kein Einstieg auf ein altes Dreieck)
                        await check_pa_signal(symbol, bull[idx], bear[idx], xl[idx], xs[idx], price_i)
                elif due_heartbeat and not closed_ts:
                    last_heartbeat = now
                    debug_log(f"⏳ [{symbol}] Punk Algo wartet: keine Kerzen erhalten (Auflösung {resolution})")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Punk-Algo-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        await asyncio.sleep(_poll_delay(b["config"].get("pa_resolution", "5m"), b["state"]))


# ============================================================================
# BACKTEST
# ============================================================================
def _pa_simulate(candles, cfg, bull, bear, xl, xs, warmup, p):
    """Backtest-Gegenstueck zu check_pa_signal / check_pa_sl_tp: Einstieg/Flip zum Schlusskurs der Signalkerze, SL/TP ab der
    naechsten Kerze ueber Hoch/Tief (SL gewinnt bei Gleichzeitigkeit), Cooldown nach SL, Gegen-Dreieck vor WaveTrend-X."""
    ts, h, l, c = candles[0], candles[2], candles[3], candles[4]
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    cooldown_ms = cfg.get("pa_sl_cooldown_seconds", 30) * 1000
    position, trades, cool_until = None, [], None

    def open_pos(direction, price, i):
        size = (margin * leverage) / price
        sl, tp = _pa_levels(direction, price, size, p)
        return {"dir": direction, "entry": price, "size": size, "entry_i": i, "sl": sl, "tp": tp}

    for i in range(max(warmup, 1), n):
        if position is not None and position["entry_i"] < i:
            d, e, sl, tp = position["dir"], position["entry"], position["sl"], position["tp"]
            if sl is not None and ((d == "long" and l[i] <= sl) or (d == "short" and h[i] >= sl)):
                _bt_close_trade(trades, d, e, sl, position["size"], i, position["entry_i"], "SL", ts=ts)
                position = None
                cool_until = ts[i] + cooldown_ms
            elif tp is not None and ((d == "long" and h[i] >= tp) or (d == "short" and l[i] <= tp)):
                _bt_close_trade(trades, d, e, tp, position["size"], i, position["entry_i"], "TP", ts=ts)
                position = None
        tri = "long" if bull[i] else "short" if bear[i] else None
        price = c[i]
        if position is None:
            if tri is None or not _allowed(tri, p) or (cool_until is not None and ts[i] < cool_until):
                continue
            position = open_pos(tri, price, i)
            continue
        d = position["dir"]
        if tri is not None and tri != d:
            ok = _allowed(tri, p)
            _bt_close_trade(trades, d, position["entry"], price, position["size"], i, position["entry_i"], "PA-FLIP" if ok else "PA-EXIT-DIR", ts=ts)
            position = open_pos(tri, price, i) if ok else None
        elif p["wt_exit"] and ((d == "long" and xl[i]) or (d == "short" and xs[i])):
            _bt_close_trade(trades, d, position["entry"], price, position["size"], i, position["entry_i"], "PA-WT-X", ts=ts)
            position = None
    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)
    return trades


async def run_pa_backtest(symbol, cfg, days, exclude_top_n=1):
    max_candles = BACKTEST_MAX_CANDLES.get("punk_algo", 100_000)
    resolution = cfg.get("pa_resolution", "5m")
    if resolution in SUB_MINUTE_RESOLUTIONS:
        max_candles = min(max_candles, 5000)
    candles, err = await _fetch_cached_mo7_backtest_candles(symbol, resolution, days, max_candles, market_type=cfg.get("binance_market_type", "spot"))
    if err:
        return {"error": err}
    p = _pa_params(cfg)
    warm = _pa_warmup(p["p1"], p["p2"])
    if not candles or len(candles[4]) < warm + 100:
        return {"error": f"Zu wenig historische Kerzen für einen aussagekräftigen Backtest erhalten (mind. ~{warm + 100} nötig)."}
    ts, o, h, l, c = candles[0], candles[1], candles[2], candles[3], candles[4]

    def _calc():
        bu, be = pa_range_signals(c, p["p1"], p["m1"], p["p2"], p["m2"])
        xl, xs = pa_wt_exits(o, h, l, c, p["level"])
        return bu, be, xl, xs
    bull, bear, xl, xs = await asyncio.to_thread(_calc)
    trades = _pa_simulate(candles, cfg, bull, bear, xl, xs, warm, p)
    stats = summarize_backtest_trades(trades, exclude_top_n)
    actual_days = (ts[-1] - ts[0]) / 86_400_000
    return {
        "symbol": symbol, "entry_mode": "punk_algo", "resolution": resolution,
        "requested_days": days, "actual_days_covered": round(actual_days, 1),
        "candles_processed": len(ts), "candle_cap": max_candles, "cache_used": False,
        "signals_buy": sum(bull), "signals_sell": sum(bear),
        "stats": stats,
        "stats_long": summarize_backtest_trades([t for t in trades if t["dir"] == "long"], exclude_top_n),
        "stats_short": summarize_backtest_trades([t for t in trades if t["dir"] == "short"], exclude_top_n),
        "trades": trades,
    }


# ============================================================================
# CHART
# ============================================================================
def pa_chart_payload(ts, o, h, l, c, cfg, max_bars=1000):
    """Kerzen + Dreiecke (nur geschlossene Kerzen; die letzte laufende wird nicht bewertet)."""
    p = _pa_params(cfg)
    n = len(c)
    if n < 5:
        return None
    bull, bear = pa_range_signals(c[:-1], p["p1"], p["m1"], p["p2"], p["m2"])
    bull, bear = bull + [False], bear + [False]
    off = max(0, n - max_bars)
    candles = [{"time": ts[i] // 1000, "open": o[i], "high": h[i], "low": l[i], "close": c[i]} for i in range(off, n)]
    markers = []
    for i in range(off, n):
        if bull[i]:
            markers.append({"time": ts[i] // 1000, "kind": "long", "price": c[i]})
        if bear[i]:
            markers.append({"time": ts[i] // 1000, "kind": "short", "price": c[i]})
    return {"candles": candles, "wt1": [], "wt2": [], "markers": markers, "levels": [], "entry_mode": "pa", "wt1v": 0, "wt2v": 0, "bars": n}


# ============================================================================
# MONTE-CARLO: zufaellige Kombinationen aus Periode 1 x Multiplikator 1 x Periode 2 x Multiplikator 2, mit Train/Test-Trennung
# ============================================================================
PA_MC_MAX_RUNS = 1000
PA_MC_MIN_RELIABLE_TRADES = 5
PA_MC_TEST_FRACTION = 0.3   # letzte 30 % des Zeitraums = Test (Out-of-Sample)
PA_MC_MAX_CANDLES = 40_000  # Speicherschutz (Render hat 512 MB fuer den ganzen Bot): es zaehlen nur die juengsten Kerzen
PA_MC_BASE_CACHE = 6        # so viele EMA-Reihen (je Periode) bleiben gleichzeitig im Speicher


def _pa_mc_compute(candles, cfg, combos, base_p, exclude_top_n, deadline):
    ts, o, h, l, c = candles[0], candles[1], candles[2], candles[3], candles[4]
    n = len(c)
    xl, xs = pa_wt_exits(o, h, l, c, base_p["level"])
    split_ts = ts[int(n * (1 - PA_MC_TEST_FRACTION))]
    base_cache = {}   # Periode -> array('d'); klein halten: eine Python-Liste kostet ~32 Byte/Kerze, ein array nur 8, und der Cache ist begrenzt

    def base_for(t):
        if t not in base_cache:
            if len(base_cache) >= PA_MC_BASE_CACHE:
                base_cache.pop(next(iter(base_cache)))
            base_cache[t] = array("d", pa_smoothrng_base(c, t))
        return base_cache[t]

    def run_one(p1, m1, p2, m2, wt_exit):
        p = dict(base_p, p1=p1, m1=m1, p2=p2, m2=m2, wt_exit=wt_exit)
        bu, be = pa_range_signals(c, p1, m1, p2, m2, base_for(p1), base_for(p2))
        trades = _pa_simulate(candles, cfg, bu, be, xl, xs, _pa_warmup(p1, p2), p)
        st = summarize_backtest_trades(trades, exclude_top_n)
        tr = summarize_backtest_trades([t for t in trades if t.get("entry_ts", 0) < split_ts], exclude_top_n)
        te = summarize_backtest_trades([t for t in trades if t.get("entry_ts", 0) >= split_ts], exclude_top_n)
        return {"pa_period1": p1, "pa_mult1": m1, "pa_period2": p2, "pa_mult2": m2, "wt_exit": wt_exit,
                "trades": st["trades"], "win_rate_pct": st["win_rate_pct"], "total_pnl_usd": st["total_pnl_usd"],
                "total_pnl_excl_top_n_usd": st["total_pnl_excl_top_n_usd"], "max_drawdown_usd": st["max_drawdown_usd"],
                "avg_bars_held": st["avg_bars_held"],
                "pnl_train": tr["total_pnl_usd"], "pnl_test": te["total_pnl_usd"], "trades_test": te["trades"]}

    out = []
    for p1, m1, p2, m2 in combos:
        if time.time() > deadline:
            break
        out.append(run_one(p1, m1, p2, m2, base_p["wt_exit"]))
    return out, run_one(*PA_DEFAULT, True), run_one(*PA_DEFAULT, False)


async def run_pa_montecarlo(symbol, cfg, days, runs, ranges, exclude_top_n=1, seed=None, max_dd_usd=0.0):
    """ranges: {'p1':(a,b), 'm1':(a,b), 'p2':(a,b), 'm2':(a,b)}. Die Pine-Standardwerte (27 / 9,1 / 55 / 1,3) laufen IMMER als Referenz
    mit (einmal mit, einmal ohne WaveTrend-X). Alles andere (Zeitrahmen, WT-X an/aus, Richtung, SL/TP, Margin, Hebel) kommt aus der Config."""
    max_candles = BACKTEST_MAX_CANDLES.get("punk_algo", 100_000)
    resolution = cfg.get("pa_resolution", "5m")
    if resolution in SUB_MINUTE_RESOLUTIONS:
        max_candles = min(max_candles, 5000)
    candles, err = await _fetch_cached_mo7_backtest_candles(symbol, resolution, days, max_candles, market_type=cfg.get("binance_market_type", "spot"))
    if err:
        return {"error": err}
    n = len(candles[4]) if candles else 0
    if n < 400:
        return {"error": "Zu wenig historische Kerzen für einen aussagekräftigen Monte-Carlo-Test erhalten (mind. ~400 nötig)."}
    note = None
    if n > PA_MC_MAX_CANDLES:
        candles = tuple(x[-PA_MC_MAX_CANDLES:] for x in candles)   # nur die juengsten Kerzen (Speicherschutz)
        note = f"Hinweis: aus Speichergründen wurden nur die jüngsten {PA_MC_MAX_CANDLES} von {n} Kerzen getestet."
        n = PA_MC_MAX_CANDLES
    runs = max(1, min(PA_MC_MAX_RUNS, int(runs)))
    try:
        p1r = sorted((int(ranges["p1"][0]), int(ranges["p1"][1]))); p2r = sorted((int(ranges["p2"][0]), int(ranges["p2"][1])))
        m1r = sorted((float(ranges["m1"][0]), float(ranges["m1"][1]))); m2r = sorted((float(ranges["m2"][0]), float(ranges["m2"][1])))
    except (KeyError, TypeError, ValueError):
        return {"error": "Ungültige Bereiche für die Parameter."}
    if p1r[0] < 2 or p2r[0] < 2 or m1r[0] <= 0 or m2r[0] <= 0:
        return {"error": "Perioden müssen ≥ 2 und Multiplikatoren > 0 sein."}
    rng = random.Random(seed)
    seen, combos = {PA_DEFAULT}, [PA_DEFAULT]
    for _ in range(runs * 20):
        if len(combos) >= runs:
            break
        k = (rng.randint(*p1r), round(rng.uniform(*m1r), 1), rng.randint(*p2r), round(rng.uniform(*m2r), 2))
        if k not in seen:
            seen.add(k)
            combos.append(k)
    base_p = _pa_params(cfg)
    combos.sort(key=lambda k: (k[0], k[2]))   # gleiche Perioden hintereinander -> der kleine Cache trifft oefter
    deadline = time.time() + 240
    results, base_wt, base_nowt = await asyncio.to_thread(_pa_mc_compute, candles, cfg, combos, base_p, exclude_top_n, deadline)
    reliable = [r for r in results if r["trades"] >= PA_MC_MIN_RELIABLE_TRADES]
    robust = [r for r in reliable if r["pnl_train"] > 0 and r["pnl_test"] > 0 and r["trades_test"] >= 3]
    best = sorted(results, key=lambda r: (r["trades"] >= PA_MC_MIN_RELIABLE_TRADES, r["total_pnl_usd"]), reverse=True)
    robust_sorted = sorted(robust, key=lambda r: r["pnl_test"], reverse=True)
    worst = sorted(results, key=lambda r: r["total_pnl_usd"])
    # Risiko-Sicht: nur Kombinationen mit Gewinn, optional mit Obergrenze fuer den Max-Drawdown, sortiert nach PnL / Drawdown
    lowdd = []
    for r in reliable:
        dd = abs(r["max_drawdown_usd"] or 0)
        if r["total_pnl_usd"] <= 0 or (max_dd_usd > 0 and dd > max_dd_usd):
            continue
        lowdd.append(dict(r, pnl_dd_ratio=round(r["total_pnl_usd"] / dd, 2) if dd > 0 else 999.0))
    lowdd.sort(key=lambda r: (r["pnl_dd_ratio"], r["pnl_test"]), reverse=True)
    pnls = sorted(r["total_pnl_usd"] for r in reliable)
    ts = candles[0]
    return {
        "symbol": symbol, "resolution": resolution, "requested_days": days, "actual_days_covered": round((ts[-1] - ts[0]) / 86_400_000, 1),
        "candles_processed": n, "min_reliable_trades": PA_MC_MIN_RELIABLE_TRADES,
        "combos_tested": len(results), "combos_requested": runs, "test_fraction_pct": int(PA_MC_TEST_FRACTION * 100),
        "wt_exit": base_p["wt_exit"], "note": note,
        "baseline_wt_on": base_wt, "baseline_wt_off": base_nowt,
        "profitable_pct": round(sum(1 for x in pnls if x > 0) / len(pnls) * 100, 1) if pnls else 0,
        "median_pnl_usd": round(pnls[len(pnls) // 2], 2) if pnls else 0,
        "best_pnl_usd": pnls[-1] if pnls else 0, "worst_pnl_usd": pnls[0] if pnls else 0,
        "robust_count": len(robust), "lowdd_count": len(lowdd), "max_dd_limit": max_dd_usd,
        "lowdd_results": lowdd[:30],
        "results": best[:30], "robust_results": robust_sorted[:30], "worst_results": worst[:20],
    }
