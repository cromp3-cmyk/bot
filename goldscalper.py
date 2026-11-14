"""
goldscalper.py - eigene Strategie "Gold Scalper" (entry_mode "gold_scalper", Config-Praefix gq_*).

Portiert aus dem TradingView-Indikator "Gold scalper 1 min or 30 sec" (GodeyeThelasthope) das Buy/Sell-Signal:
  Referenz   = Hoch/Tief der LETZTEN ABGESCHLOSSENEN Kerze des Referenz-Zeitrahmens (Pine: request.security(high[1]), Standard 4h)
  Banker     = SMA( clamp(1,5 * (RSI(rsi_len) - 50), 0, 20), ma_len )          (Pine: rsi_function + ta.sma, Standard 50 / 8)
  BUY  = Banker kreuzt NACH OBEN ueber buy_level   UND Schlusskurs >= Referenz-Hoch   (Pine: ta.crossover)
  SELL = Banker kreuzt NACH UNTEN unter sell_level UND Schlusskurs <= Referenz-Tief   (Pine: ta.crossunder)
Das Pine-Skript hat keinen Ausstieg. Zwei waehlbare Ausstiege (gq_exit_mode):
  "bands"  (ATR-Baender, Indikator "ATR Bands"): ATR(atr_len) der Signal-Kerze ist fest.
             SL  = Einstieg -/+ ATR * gq_sl_mult                    (Abstand = Risiko R)
             TP1 = Einstieg +/- R * gq_tp1_rr (Standard 1:1)        -> gq_tp1_pct % der Position werden verkauft,
                   danach SL auf den Einstieg (Break-Even)
             TP2 = Rest laeuft mit einem MITLAUFENDEN ATR-Band: nach jedem Kerzenschluss  Schluss -/+ ATR * gq_trail_mult,
                   der Stop wird nur nachgezogen, nie gelockert; Ausstieg, wenn der Kurs ihn beruehrt.
  "signal" (von Signal zu Signal): kein SL/TP, das Gegen-Signal schliesst und dreht die Position.
Signal auf der geschlossenen Kerze. Im Modus "bands" werden weitere Signale bei offener Position ignoriert.
Nicht uebernommen: session.regular (Handelszeiten-Filter der Boerse - Lighter handelt durchgehend).
Dazu ein Monte-Carlo-Test (run_gq_montecarlo) ueber Zeitrahmen x ATR-Baender.
"""

import asyncio
import itertools
import random
import re
import time
import traceback
from bisect import bisect_right

from bot_core import BOTS, debug_log, execute_entry, execute_exit, execute_partial_exit, execute_reverse
from dollar701 import _res_seconds, _poll_delay
from strategies import (
    SUB_MINUTE_RESOLUTIONS, get_seconds_candles, fetch_candles_binance_multi, compute_atr, compute_rsi,
    summarize_backtest_trades, _fetch_cached_mo7_backtest_candles, BACKTEST_MAX_CANDLES, _bt_close_trade,
)

RSI_SENS = 1.5     # im Pine-Skript fest (SensitivityBanker)
RSI_BASE = 50.0    # im Pine-Skript fest (RSIBaseBanker)


def _f(cfg, key, default):
    try:
        v = cfg.get(key)
        return float(default if v is None else v)
    except (TypeError, ValueError):
        return float(default)


def _gq_params(cfg):
    return {
        "rsi_len": max(2, int(cfg.get("gq_rsi_len", 50) or 50)),
        "ma_len": max(1, int(cfg.get("gq_ma_len", 8) or 8)),
        "buy_level": _f(cfg, "gq_buy_level", 1.0),
        "sell_level": _f(cfg, "gq_sell_level", 1.0),
        "atr_len": max(1, int(cfg.get("gq_atr_len", 14) or 14)),
        "exit_mode": "signal" if cfg.get("gq_exit_mode", "bands") == "signal" else "bands",
        "sl_mult": max(0.1, _f(cfg, "gq_sl_mult", 1.0)),          # SL-Band = ATR * Faktor (= Risiko R)
        "tp1_rr": max(0.1, _f(cfg, "gq_tp1_rr", 1.0)),            # TP1 = R * Faktor (1 = 1:1)
        "tp1_pct": min(100.0, max(0.0, _f(cfg, "gq_tp1_pct", 50.0))),   # Anteil der Position, der bei TP1 verkauft wird
        "trail_mult": max(0.1, _f(cfg, "gq_trail_mult", 1.0)),    # mitlaufendes ATR-Band nach TP1
        "htf": cfg.get("gq_htf_resolution", "4h"),
    }


def _gq_warmup(p):
    return p["rsi_len"] + p["ma_len"] + 5


def _sma(values, length):
    """Pine ta.sma: erst ab voller Fensterlaenge gueltig (davor None)."""
    n = len(values)
    out = [None] * n
    s = 0.0
    for i in range(n):
        s += values[i]
        if i >= length:
            s -= values[i - length]
        if i >= length - 1:
            out[i] = s / length
    return out


def gq_reference(base_ts, htf_ts, htf_h, htf_l):
    """Fuer jede Basis-Kerze: Hoch/Tief der VORLETZTEN-bis-letzten abgeschlossenen Referenz-Kerze, d. h. der Kerze VOR der
    Referenz-Kerze, in der die Basis-Kerze liegt (Pine: high[1] mit lookahead_on). None, wenn keine Vorgaenger-Kerze bekannt."""
    ref_hi, ref_lo = [None] * len(base_ts), [None] * len(base_ts)
    if not htf_ts:
        return ref_hi, ref_lo
    for i, t in enumerate(base_ts):
        j = bisect_right(htf_ts, t) - 1
        if j >= 1:
            ref_hi[i], ref_lo[i] = htf_h[j - 1], htf_l[j - 1]
    return ref_hi, ref_lo


def gq_signals(base_ts, h, l, c, htf, p, atr=None):
    """-> (buy[], sell[], atr[]). htf = (htf_ts, htf_h, htf_l). Exakt die Pine-Logik auf geschlossenen Kerzen."""
    n = len(c)
    rsi = compute_rsi(c, p["rsi_len"])
    banker = [min(20.0, max(0.0, RSI_SENS * (x - RSI_BASE))) for x in rsi]
    ma = _sma(banker, p["ma_len"])
    ref_hi, ref_lo = gq_reference(base_ts, htf[0], htf[1], htf[2])
    if atr is None:
        atr = compute_atr(h, l, c, p["atr_len"]) or [0.0] * n
    buy, sell = [False] * n, [False] * n
    for i in range(1, n):
        a, b = ma[i], ma[i - 1]
        if a is None or b is None:
            continue
        if a > p["buy_level"] and b <= p["buy_level"] and ref_hi[i] is not None and c[i] >= ref_hi[i]:
            buy[i] = True
        if a < p["sell_level"] and b >= p["sell_level"] and ref_lo[i] is not None and c[i] <= ref_lo[i]:
            sell[i] = True
    return buy, sell, atr


def _gq_levels(direction, entry, atr, p):
    """ATR-Band -> (SL, TP1) als Preise. R = ATR * sl_mult; TP1 = Einstieg +/- R * tp1_rr."""
    if not atr or atr <= 0:
        return None, None
    sgn = 1 if direction == "long" else -1
    r = atr * p["sl_mult"]
    return entry - sgn * r, entry + sgn * r * p["tp1_rr"]


def _gq_trail_candidate(direction, close, atr, p):
    """Mitlaufendes ATR-Band (wie "ATR Bands" mit Quelle = Schluss): unteres Band bei Long, oberes bei Short."""
    return close - atr * p["trail_mult"] if direction == "long" else close + atr * p["trail_mult"]


def _gq_ratchet(direction, sl, cand):
    """Stop nur nachziehen, nie lockern."""
    if sl is None:
        return cand
    return max(sl, cand) if direction == "long" else min(sl, cand)


def _gq_sl_reason(direction, entry, sl, tp1_done):
    if not tp1_done:
        return "SL"
    if sl is not None and ((direction == "long" and sl > entry) or (direction == "short" and sl < entry)):
        return "TRAIL"
    return "BREAKEVEN"


def _gq_reset_state(st):
    st["gq_sl_price"] = None
    st["gq_tp1_price"] = None
    st["gq_tp1_done"] = False
    st["gq_tp1_ms"] = None


async def check_gq_sl_tp(symbol, price):
    """Modus "bands": SL (bzw. nach TP1 Break-Even/mitlaufendes Band) und TP1 laufend gegen den Live-Preis."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if st["position"] is None or price is None:
        return
    p = _gq_params(cfg)
    if p["exit_mode"] != "bands":
        return
    pos = st["position"]
    sl_price, tp1_price = st.get("gq_sl_price"), st.get("gq_tp1_price")
    entry = st.get("avg_entry_price") or price
    if sl_price is not None and ((pos == "long" and price <= sl_price) or (pos == "short" and price >= sl_price)):
        reason = _gq_sl_reason(pos, entry, sl_price, st.get("gq_tp1_done"))
        debug_log(f"🚪 [{symbol}] Gold Scalper {reason}: {pos.upper()} @ {price} (Ziel war {round(sl_price, 4)})")
        await execute_exit(symbol, price, reason)
        if st["position"] is None:
            if reason == "SL":
                st["gq_sl_cooldown_until"] = time.time() + cfg.get("gq_sl_cooldown_seconds", 30)
            _gq_reset_state(st)
        return
    if not st.get("gq_tp1_done") and tp1_price is not None and ((pos == "long" and price >= tp1_price) or (pos == "short" and price <= tp1_price)):
        pct = p["tp1_pct"]
        if pct >= 100:
            debug_log(f"🎯 [{symbol}] Gold Scalper TP1 (100 %): {pos.upper()} @ {price} (Ziel war {round(tp1_price, 4)})")
            await execute_exit(symbol, price, "TP1")
            if st["position"] is None:
                _gq_reset_state(st)
            return
        if pct > 0:
            if not await execute_partial_exit(symbol, price, pct / 100.0, "TP1"):
                return          # Teilverkauf fehlgeschlagen -> naechster Tick versucht es erneut
            if st["position"] is None:
                _gq_reset_state(st)
                return
        st["gq_tp1_done"] = True
        st["gq_tp1_ms"] = int(time.time() * 1000)
        st["gq_sl_price"] = entry
        debug_log(f"🎯 [{symbol}] Gold Scalper TP1 ({pct:g} %) @ {price} erreicht - SL auf Einstieg ({round(entry, 4)}), Rest läuft mit ATR-Band ({p['trail_mult']}×ATR)")


async def update_gq_trail(symbol, candle_end_ms, close, atr):
    """Nach jedem Kerzenschluss: mitlaufendes ATR-Band nachziehen (nur nach TP1, nur in Richtung Gewinn)."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    pos = st["position"]
    if pos is None or not st.get("gq_tp1_done") or not atr or atr <= 0:
        return
    p = _gq_params(cfg)
    if p["exit_mode"] != "bands":
        return
    if candle_end_ms <= (st.get("gq_tp1_ms") or 0):
        return          # Kerze vor TP1 geschlossen: zaehlt nicht
    old = st.get("gq_sl_price")
    new = _gq_ratchet(pos, old, _gq_trail_candidate(pos, close, atr, p))
    if old is None or new != old:
        st["gq_sl_price"] = new
        debug_log(f"📈 [{symbol}] Gold Scalper: mitlaufendes ATR-Band nachgezogen auf {round(new, 4)}")


async def check_gq_entry(symbol, buy_signal, sell_signal, price, atr):
    """Einstieg auf der Signal-Kerze.
    bands : offene Position -> Signale ignoriert (Ausstieg nur ueber SL/TP1/mitlaufendes Band).
    signal: Gegen-Signal schliesst und dreht die Position (von Signal zu Signal), gleiche Richtung wird ignoriert."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or price is None:
        return
    p = _gq_params(cfg)
    mode = cfg.get("gq_direction_mode", "both")
    pos = st["position"]
    if p["exit_mode"] == "signal":
        if pos is not None and buy_signal != sell_signal:
            want = "long" if buy_signal else "short"
            if want == pos:
                return
            if (mode == "long_only" and want == "short") or (mode == "short_only" and want == "long"):
                debug_log(f"🔄 [{symbol}] Gold Scalper Gegen-Signal: {pos.upper()} schliessen @ {price}")
                await execute_exit(symbol, price, "GQ-FLIP")
                if st["position"] is None:
                    _gq_reset_state(st)
                return
            debug_log(f"⚡ [{symbol}] Gold Scalper Signal zu Signal: {pos.upper()} -> {want.upper()} @ {price}")
            if await execute_reverse(symbol, price, "GQ-FLIP", size_multiplier=1.0):
                _gq_reset_state(st)
            return
    if mode == "long_only":
        sell_signal = False
    elif mode == "short_only":
        buy_signal = False
    if buy_signal == sell_signal:
        return
    if pos is not None:
        return
    if time.time() < st.get("gq_sl_cooldown_until", 0.0):
        return
    target = "long" if buy_signal else "short"
    debug_log(f"📡 [{symbol}] Gold Scalper Signal: {target.upper()} @ {price} (ATR {round(atr or 0, 6)})")
    await execute_entry(symbol, target, price, is_add_on=False)
    if st["position"] is None:
        return
    _gq_reset_state(st)
    if p["exit_mode"] == "bands":
        entry = st.get("avg_entry_price") or price
        st["gq_sl_price"], st["gq_tp1_price"] = _gq_levels(target, entry, atr, p)


async def _fetch_htf(symbol, htf, cfg, bars=60):
    """Referenz-Kerzen (ts, high, low) inkl. der laufenden letzten Kerze; None bei Fehler."""
    data = await fetch_candles_binance_multi(symbol, htf, count_back=bars, market_type=cfg.get("binance_market_type", "spot"))
    if not data:
        return None
    return data[0], data[2], data[3]


async def gq_poll_loop(symbol):
    """Gold Scalper - Einstieg bei Kerzenschluss der Basis-Zeitebene, SL/TP (ATR-Baender) laufend gegen den Live-Preis."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0
    htf_cache = {"key": None, "data": None}
    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "gold_scalper" and cfg["bot_active"] and b["state"].get("session_started"):
                resolution = cfg.get("gq_resolution", "1m")
                p = _gq_params(cfg)
                min_needed = _gq_warmup(p) + 2
                needed_bars = min(1000, max(300, p["rsi_len"] * 4))
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
                    await check_gq_sl_tp(symbol, st["last_price"])

                if closed_ts and len(closed_c) > min_needed:
                    age = (now * 1000 - closed_ts[-1]) / 1000
                    if age > 300 + _res_seconds(resolution):
                        debug_log(f"⚠️ [{symbol}] Gold Scalper: letzte Kerze wirkt veraltet ({round(age)}s alt, Auflösung {resolution}) - überspringe Durchlauf.")
                        closed_ts = None
                    elif st["last_price"] is not None and closed_c[-1] and abs(closed_c[-1] - st["last_price"]) / st["last_price"] * 100 > 2.0:
                        debug_log(f"⚠️ [{symbol}] Gold Scalper: Kerzendaten unplausibel (Kerze {closed_c[-1]} vs. Live {st['last_price']}) - überspringe Durchlauf.")
                        closed_ts = None

                if closed_ts and len(closed_c) > min_needed and (last_processed_ts is None or closed_ts[-1] > last_processed_ts):
                    # Referenz-Kerzen: pro Referenz-Kerze nur einmal holen (Hoch/Tief der Vorgaengerin aendert sich innerhalb der Kerze nicht)
                    htf_per = _res_seconds(p["htf"])
                    htf_key = (p["htf"], int(now // htf_per))
                    htf_start_ms = int(now // htf_per) * htf_per * 1000
                    if htf_cache["key"] != htf_key or htf_cache["data"] is None:
                        got = await _fetch_htf(symbol, p["htf"], cfg)
                        if got is None:
                            if htf_cache["data"] is None:
                                await asyncio.sleep(2.0)
                                continue          # keine Referenz-Kerzen: noch kein Signal moeglich
                            # sonst: mit dem letzten Stand weiterrechnen
                        elif (got[0] and got[0][-1] >= htf_start_ms) or (now - htf_start_ms / 1000.0) > 90:
                            # neue Referenz-Kerze vorhanden - oder die Boerse liefert sie nicht (z. B. Marktpause): dann Stand uebernehmen
                            htf_cache["key"], htf_cache["data"] = htf_key, got
                        else:
                            await asyncio.sleep(1.0)
                            continue              # neue Referenz-Kerze noch nicht da: im naechsten Durchlauf erneut
                    htf = htf_cache["data"]
                    buy, sell, atr = await asyncio.to_thread(gq_signals, closed_ts, closed_h, closed_l, closed_c, htf, p)
                    if due_heartbeat:
                        last_heartbeat = now
                        _ex = ("von Signal zu Signal" if p["exit_mode"] == "signal" else
                               f"SL {p['sl_mult']}×ATR, TP1 {p['tp1_rr']:g}R ({p['tp1_pct']:g} %), Rest mit {p['trail_mult']}×ATR-Band")
                        debug_log(f"💓 [{symbol}] Gold Scalper aktiv: Zeitrahmen {resolution}, Referenz {p['htf']}, {_ex}, Preis={closed_c[-1]}")
                    if last_processed_ts is None:
                        new_indices = [len(closed_ts) - 1]
                    else:
                        new_indices = [i for i in range(len(closed_ts)) if closed_ts[i] > last_processed_ts]
                    for idx in new_indices:
                        price_i = st["last_price"] if (idx == len(closed_ts) - 1 and st["last_price"] is not None) else closed_c[idx]
                        last_processed_ts = closed_ts[idx]
                        await update_gq_trail(symbol, closed_ts[idx] + _res_seconds(resolution) * 1000, closed_c[idx], atr[idx])
                        await check_gq_entry(symbol, buy[idx], sell[idx], price_i, atr[idx])
                elif due_heartbeat and not closed_ts and (resolution in SUB_MINUTE_RESOLUTIONS or _need_fetch):
                    last_heartbeat = now
                    debug_log(f"⏳ [{symbol}] Gold Scalper wartet: keine Kerzen erhalten (Auflösung {resolution})")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Gold-Scalper-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        await asyncio.sleep(_poll_delay(cfg.get("gq_resolution", "1m"), b["state"]))


def _gq_simulate(candles, cfg, buy_sig, sell_sig, atr, warmup, p):
    """Backtest-Gegenstueck zu check_gq_entry / check_gq_sl_tp / update_gq_trail: Einstieg zum Schlusskurs der Signal-Kerze.
    bands : Pro Kerze zuerst SL (gewinnt bei Gleichzeitigkeit mit TP1), dann TP1 (Teilverkauf, SL -> Einstieg; in derselben Kerze
            wird der neue SL nicht mehr geprueft), danach Band nachziehen (Schluss -/+ ATR*Faktor). Offene Position: Signale ignoriert.
    signal: Gegen-Signal schliesst und dreht die Position zum Schlusskurs der Signal-Kerze."""
    ts, h, l, c = candles[0], candles[2], candles[3], candles[4]
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    cooldown_ms = cfg.get("gq_sl_cooldown_seconds", 30) * 1000
    mode = cfg.get("gq_direction_mode", "both")
    signal_mode = p["exit_mode"] == "signal"
    position, trades, cool_until = None, [], None
    for i in range(max(warmup, 1), n):
        if position is not None and not signal_mode:
            d, e, sl = position["dir"], position["entry"], position["sl"]
            if (d == "long" and l[i] <= sl) or (d == "short" and h[i] >= sl):
                reason = _gq_sl_reason(d, e, sl, position["tp1_done"])
                _bt_close_trade(trades, d, e, sl, position["size"], i, position["entry_i"], reason, ts=ts)
                position = None
                if reason == "SL":
                    cool_until = ts[i] + cooldown_ms
            elif not position["tp1_done"] and ((d == "long" and h[i] >= position["tp1"]) or (d == "short" and l[i] <= position["tp1"])):
                pct = p["tp1_pct"]
                if pct >= 100:
                    _bt_close_trade(trades, d, e, position["tp1"], position["size"], i, position["entry_i"], "TP1", ts=ts)
                    position = None
                else:
                    if pct > 0:
                        part = position["size"] * pct / 100.0
                        _bt_close_trade(trades, d, e, position["tp1"], part, i, position["entry_i"], "TP1", ts=ts)
                        position["size"] -= part
                    position["tp1_done"] = True
                    position["sl"] = e
            if position is not None and position["tp1_done"] and atr[i] > 0:
                position["sl"] = _gq_ratchet(d, position["sl"], _gq_trail_candidate(d, c[i], atr[i], p))
        buy, sell = buy_sig[i], sell_sig[i]
        if signal_mode and position is not None and buy != sell:
            want = "long" if buy else "short"
            if want == position["dir"]:
                continue
            _bt_close_trade(trades, position["dir"], position["entry"], c[i], position["size"], i, position["entry_i"], "GQ-FLIP", ts=ts)
            position = None
            if (mode == "long_only" and want == "short") or (mode == "short_only" and want == "long"):
                continue
            size = (margin * leverage) / c[i]
            position = {"dir": want, "entry": c[i], "size": size, "entry_i": i, "sl": None, "tp1": None, "tp1_done": False}
            continue
        if position is not None:
            continue
        if mode == "long_only":
            sell = False
        elif mode == "short_only":
            buy = False
        if buy == sell:
            continue
        if cool_until is not None and ts[i] < cool_until:
            continue
        target = "long" if buy else "short"
        price = c[i]
        size = (margin * leverage) / price
        sl, tp1 = (None, None) if signal_mode else _gq_levels(target, price, atr[i], p)
        position = {"dir": target, "entry": price, "size": size, "entry_i": i, "sl": sl, "tp1": tp1, "tp1_done": False}
    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)
    return trades


async def run_gq_backtest(symbol, cfg, days, exclude_top_n=1):
    max_candles = BACKTEST_MAX_CANDLES.get("gold_scalper", 100_000)
    resolution = cfg.get("gq_resolution", "1m")
    if resolution in SUB_MINUTE_RESOLUTIONS:
        max_candles = min(max_candles, 5000)
    mt = cfg.get("binance_market_type", "spot")
    candles, err = await _fetch_cached_mo7_backtest_candles(symbol, resolution, days, max_candles, market_type=mt)
    if err:
        return {"error": err}
    p = _gq_params(cfg)
    min_needed = _gq_warmup(p) + 10
    if not candles or len(candles[4]) < min_needed:
        return {"error": f"Zu wenig historische Kerzen für einen aussagekräftigen Backtest erhalten (mind. ~{min_needed} nötig)."}
    htf_days = int(days) + max(2, int(_res_seconds(p["htf"]) // 86400) + 2)     # genug Vorlauf: die Referenz-Kerze VOR der ersten Basis-Kerze
    htf_candles, err = await _fetch_cached_mo7_backtest_candles(symbol, p["htf"], htf_days, 20_000, market_type=mt)
    if err or not htf_candles:
        return {"error": err or f"Keine Referenz-Kerzen ({p['htf']}) erhalten."}
    base_ts = candles[0]
    buy, sell, atr = await asyncio.to_thread(gq_signals, base_ts, candles[2], candles[3], candles[4],
                                             (htf_candles[0], htf_candles[2], htf_candles[3]), p)
    trades = _gq_simulate(candles, cfg, buy, sell, atr, _gq_warmup(p), p)
    stats = summarize_backtest_trades(trades, exclude_top_n)
    actual_days = (base_ts[-1] - base_ts[0]) / 86_400_000
    return {
        "symbol": symbol, "entry_mode": "gold_scalper", "resolution": resolution,
        "requested_days": days, "actual_days_covered": round(actual_days, 1),
        "candles_processed": len(base_ts), "candle_cap": max_candles, "cache_used": False,
        "signals_buy": sum(buy), "signals_sell": sum(sell),
        "stats": stats,
        "stats_long": summarize_backtest_trades([t for t in trades if t["dir"] == "long"], exclude_top_n),
        "stats_short": summarize_backtest_trades([t for t in trades if t["dir"] == "short"], exclude_top_n),
        "trades": trades,
    }


async def gq_chart_payload(symbol, cfg, ts, o, h, l, c, max_bars=1000):
    """Chart-Daten fuer das Bot-Dashboard: Kerzen + Pfeile bei jedem Kauf-/Verkaufssignal."""
    p = _gq_params(cfg)
    if len(ts) < 3:
        return None
    span_ms = max(1, ts[-1] - ts[0])
    bars = min(1000, int(span_ms / (_res_seconds(p["htf"]) * 1000)) + 6)
    htf = await _fetch_htf(symbol, p["htf"], cfg, bars=bars) or ([], [], [])
    buy, sell, _ = await asyncio.to_thread(gq_signals, ts, h, l, c, htf, p)
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
            "entry_mode": "gq", "wt1v": 0, "wt2v": 0, "bars": n}


# ============================================================
# Monte-Carlo-Backtest: Zeitrahmen x ATR-Baender (SL-Band und mitlaufendes Band, z. B. 1 bis 3 in 0,2-Schritten)
# ============================================================
GQ_MC_MAX_RUNS = 2000
GQ_MC_MAX_RESOLUTIONS = 8
GQ_MC_MIN_RELIABLE_TRADES = 5
_RES_RE = re.compile(r"^\d{1,4}[mhd]$")


def gq_parse_resolutions(text):
    """"30s, 1m,5m" -> ["30s", "1m", "5m"] (nur gueltige Zeitrahmen, ohne Doppelte, max. GQ_MC_MAX_RESOLUTIONS)."""
    out = []
    for part in str(text or "").replace(";", ",").replace(" ", ",").split(","):
        r = part.strip().lower()
        if r and (r in SUB_MINUTE_RESOLUTIONS or _RES_RE.match(r)) and r not in out:
            out.append(r)
    return out[:GQ_MC_MAX_RESOLUTIONS]


def gq_mult_values(vmin, vmax, step):
    """z. B. 1, 3, 0.2 -> [1.0, 1.2, ..., 3.0]"""
    vmin, vmax = sorted((max(0.1, float(vmin)), max(0.1, float(vmax))))
    step = max(0.05, float(step))
    out, k = [], 0
    while True:
        v = round(vmin + k * step, 4)
        if v > vmax + 1e-9 or k > 400:
            break
        out.append(v)
        k += 1
    return out


def _gq_mc_compute(prepared, base_cfg, combos, exclude_top_n, deadline):
    """prepared: {Zeitrahmen: (candles, buy, sell, atr)}. combos: (Zeitrahmen, SL-Band, mitlaufendes Band)."""
    base_p = _gq_params(base_cfg)
    out = []
    for res, slm, trm in combos:
        if time.time() > deadline:
            break
        candles, buy, sell, atr = prepared[res]
        p = dict(base_p, sl_mult=max(0.1, slm) if slm else base_p["sl_mult"], trail_mult=max(0.1, trm) if trm else base_p["trail_mult"])
        trades = _gq_simulate(candles, base_cfg, buy, sell, atr, _gq_warmup(p), p)
        st = summarize_backtest_trades(trades, exclude_top_n)
        out.append({"gq_resolution": res, "gq_sl_mult": slm, "gq_trail_mult": trm, "trades": st["trades"], "win_rate_pct": st["win_rate_pct"],
                    "total_pnl_usd": st["total_pnl_usd"], "total_pnl_excl_top_n_usd": st["total_pnl_excl_top_n_usd"],
                    "max_drawdown_usd": st["max_drawdown_usd"], "avg_bars_held": st["avg_bars_held"]})
    return out


async def run_gq_montecarlo(symbol, cfg, days, resolutions, mult_min=1.0, mult_max=3.0, mult_step=0.2, runs=500,
                            exclude_top_n=1, seed=None, exit_mode=None):
    """Testet alle (bzw. `runs` zufaellig gezogene) Kombinationen aus Zeitrahmen x SL-Band x mitlaufendes Band auf denselben
    Kerzen. Alles andere (Richtung, RSI/Banker, ATR-Laenge, TP1 1:1 + Anteil, Cooldown, Margin, Hebel) kommt aus den
    Einstellungen. Im Modus "signal" (von Signal zu Signal) gibt es keine Baender: dann wird nur der Zeitrahmen variiert."""
    cfg = dict(cfg)
    if exit_mode in ("bands", "signal"):
        cfg["gq_exit_mode"] = exit_mode
    p = _gq_params(cfg)
    resolutions = gq_parse_resolutions(",".join(resolutions) if isinstance(resolutions, (list, tuple)) else resolutions)
    if not resolutions:
        return {"error": "Keine gültigen Zeitrahmen angegeben (z. B. 30s, 1m, 5m, 15m)."}
    values = gq_mult_values(mult_min, mult_max, mult_step)
    runs = max(1, min(GQ_MC_MAX_RUNS, int(runs)))
    mt = cfg.get("binance_market_type", "spot")
    htf_days = int(days) + max(2, int(_res_seconds(p["htf"]) // 86400) + 2)
    htf_candles, err = await _fetch_cached_mo7_backtest_candles(symbol, p["htf"], htf_days, 20_000, market_type=mt)
    if err or not htf_candles:
        return {"error": err or f"Keine Referenz-Kerzen ({p['htf']}) erhalten."}
    htf = (htf_candles[0], htf_candles[2], htf_candles[3])
    prepared, skipped, n_max, days_cov = {}, [], 0, 0.0
    for res in resolutions:
        cap = BACKTEST_MAX_CANDLES.get("gold_scalper", 100_000)
        if res in SUB_MINUTE_RESOLUTIONS:
            cap = min(cap, 5000)
        candles, err = await _fetch_cached_mo7_backtest_candles(symbol, res, days, cap, market_type=mt)
        if err or not candles or len(candles[4]) < _gq_warmup(p) + 10:
            skipped.append(res)
            continue
        buy, sell, atr = await asyncio.to_thread(gq_signals, candles[0], candles[2], candles[3], candles[4], htf, p)
        prepared[res] = (candles, buy, sell, atr)
        n_max = max(n_max, len(candles[4]))
        days_cov = max(days_cov, (candles[0][-1] - candles[0][0]) / 86_400_000)
    if not prepared:
        return {"error": "Für keinen der gewählten Zeitrahmen gab es genug Kerzen" + (f" (übersprungen: {', '.join(skipped)})." if skipped else ".")}
    signal_mode = p["exit_mode"] == "signal"
    if signal_mode:
        combos = [(r, 0.0, 0.0) for r in prepared]
    else:
        combos = list(itertools.product(list(prepared), values, values))
    requested = len(combos)
    rng = random.Random(seed)
    if len(combos) > runs:
        combos = rng.sample(combos, runs)
    deadline = time.time() + 240
    results = await asyncio.to_thread(_gq_mc_compute, prepared, cfg, combos, exclude_top_n, deadline)
    rank_key = lambda r: (r["trades"] >= GQ_MC_MIN_RELIABLE_TRADES, r["total_pnl_usd"])
    best = sorted(results, key=rank_key, reverse=True)
    worst = sorted(results, key=lambda r: r["total_pnl_usd"])
    pnls = sorted(r["total_pnl_usd"] for r in results)
    pos = sum(1 for x in pnls if x > 0)
    median = pnls[len(pnls) // 2] if pnls else 0
    return {
        "exit_mode": p["exit_mode"], "symbol": symbol, "resolutions": list(prepared), "skipped_resolutions": skipped,
        "band_values": [] if signal_mode else values, "requested_days": days, "actual_days_covered": round(days_cov, 1),
        "candles_processed": n_max, "min_reliable_trades": GQ_MC_MIN_RELIABLE_TRADES,
        "combos_tested": len(results), "combos_requested": requested, "combos_drawn": len(combos),
        "profitable_pct": round(pos / len(results) * 100, 1) if results else 0,
        "median_pnl_usd": round(median, 2), "best_pnl_usd": pnls[-1] if pnls else 0, "worst_pnl_usd": pnls[0] if pnls else 0,
        "results": best[:30], "worst_results": worst[:20],
    }
