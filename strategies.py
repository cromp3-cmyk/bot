"""
strategies.py - Die Handelsstrategien: Grid, OBI-Scalp, MACD-Dual + Stochastic.
Nutzt gemeinsame Infrastruktur aus bot_core.py.
"""

import asyncio
import websockets
import aiohttp
import json
import time
import traceback

from bot_core import (
    debug_log, WS_URL, SYMBOLS, MARKET_INDICES, MARKET_INDEX_TO_SYMBOL,
    BOTS, execute_entry, execute_exit, execute_partial_exit, compute_step_abs,
)

BINANCE_SYMBOL_MAP = {
    "BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT", "DOGE": "DOGEUSDT", "XRP": "XRPUSDT",
    "LINK": "LINKUSDT", "AVAX": "AVAXUSDT", "NEAR": "NEARUSDT", "DOT": "DOTUSDT", "TON": "TONUSDT",
    "SUI": "SUIUSDT", "BNB": "BNBUSDT", "UNI": "UNIUSDT", "APT": "APTUSDT", "ADA": "ADAUSDT",
    "TRX": "TRXUSDT", "LTC": "LTCUSDT", "BCH": "BCHUSDT", "HBAR": "HBARUSDT", "ICP": "ICPUSDT",
    # HYPE und Forex/Rohstoffe (EURUSD, XAU, WTI, ...) gibt es nicht auf Binance - dafuer greift der Lighter-Fallback
}


async def fetch_candles_binance(symbol, resolution, count_back=150):
    """Alternative Kerzenquelle - Binance hat deutlich mehr Liquiditaet als Lighter,
    kann daher weniger anfaellig fuer kurze Preis-Spikes/Wicks sein, die auf einer
    kleineren Perp-DEX Fehlsignale ausloesen wuerden."""
    pair = BINANCE_SYMBOL_MAP.get(symbol)
    if not pair:
        return None
    try:
        url = f"https://api.binance.com/api/v3/klines?symbol={pair}&interval={resolution}&limit={min(count_back, 500)}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    debug_log(f"⚠️ [{symbol}] Binance-Kerzenabfrage HTTP {resp.status}")
                    return None
                data = await resp.json()
    except Exception as e:
        debug_log(f"⚠️ [{symbol}] Binance-Kerzenabfrage fehlgeschlagen", {"error": str(e)})
        return None

    if not data or not isinstance(data, list):
        return None

    timestamps, opens, highs, lows, closes = [], [], [], [], []
    for k in data:
        timestamps.append(int(k[0]))
        opens.append(float(k[1]))
        highs.append(float(k[2]))
        lows.append(float(k[3]))
        closes.append(float(k[4]))
    return timestamps, opens, highs, lows, closes


def _ema_series(values, length):
    if not values:
        return []
    k = 2 / (length + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def compute_macd_histogram(closes, fast_period, slow_period, signal_period):
    """MACD-Histogramm = MACD-Linie (EMA-schnell minus EMA-langsam) minus Signallinie
    (EMA der MACD-Linie). Nur das Histogramm wird fuer die Strategie genutzt."""
    ema_fast = _ema_series(closes, fast_period)
    ema_slow = _ema_series(closes, slow_period)
    macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
    signal_line = _ema_series(macd_line, signal_period)
    histogram = [m - s for m, s in zip(macd_line, signal_line)]
    return histogram


def compute_stochastic(highs, lows, closes, k_period, k_smooth, d_period):
    """%K (geglaettet) und %D fuer den Stochastic-Oszillator."""
    n = len(closes)
    if n < k_period + k_smooth + d_period:
        return [], []
    raw_k = []
    for i in range(n):
        if i < k_period - 1:
            raw_k.append(50.0)
            continue
        window_h = highs[i - k_period + 1:i + 1]
        window_l = lows[i - k_period + 1:i + 1]
        hh, ll = max(window_h), min(window_l)
        raw_k.append(50.0 if hh == ll else (closes[i] - ll) / (hh - ll) * 100)

    def sma(values, length):
        out = []
        for i in range(len(values)):
            if i < length - 1:
                out.append(sum(values[:i + 1]) / (i + 1))
            else:
                out.append(sum(values[i - length + 1:i + 1]) / length)
        return out

    k_smoothed = sma(raw_k, k_smooth)
    d_line = sma(k_smoothed, d_period)
    return k_smoothed, d_line


async def macd_stoch_poll_loop(symbol):
    """MACD-Dual + Stochastic: Einstieg wenn der langsame MACD-Histogramm-Trend
    (34/144/9) und ein Nulldurchgang des schnellen Histogramms (13/21/9) in dieselbe
    Richtung zeigen, optional bestaetigt durch die Stochastic-%K/%D-Lage.
    TP: fester $-Wert ODER schnelles Histogramm faerbt sich hell (Momentum laesst nach).
    SL: immer fester $-Wert (siehe on_price_update)."""
    b = BOTS[symbol]
    last_fast_hist = None
    last_slow_hist = None
    last_processed_ts = None
    last_entry_signal_ts = None

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "macd_stoch":
                needed_bars = min(500, cfg["macd_slow_slow"] + cfg["macd_slow_signal"] + 60)
                data = await fetch_candles_binance(symbol, cfg["macd_resolution"], count_back=needed_bars)
                if data:
                    timestamps, opens, highs, lows, closes = data
                    closed_ts = timestamps[:-1]
                    closed_h, closed_l, closed_c = highs[:-1], lows[:-1], closes[:-1]

                    if len(closed_c) > cfg["macd_slow_slow"] + cfg["macd_slow_signal"]:
                        fast_hist = compute_macd_histogram(closed_c, cfg["macd_fast_fast"], cfg["macd_fast_slow"], cfg["macd_fast_signal"])
                        slow_hist = compute_macd_histogram(closed_c, cfg["macd_slow_fast"], cfg["macd_slow_slow"], cfg["macd_slow_signal"])
                        st = b["state"]
                        curr_fast = fast_hist[-1]
                        curr_slow = slow_hist[-1]
                        st["macd_fast_hist"] = round(curr_fast, 4)
                        st["macd_slow_hist"] = round(curr_slow, 4)

                        stoch_k = stoch_d = None
                        if cfg.get("macd_use_stochastic", True):
                            k_series, d_series = compute_stochastic(closed_h, closed_l, closed_c,
                                                                      cfg["stoch_k_period"], cfg["stoch_k_smooth"], cfg["stoch_d_period"])
                            if k_series and d_series:
                                stoch_k, stoch_d = k_series[-1], d_series[-1]
                                st["macd_stoch_k"] = round(stoch_k, 2)
                                st["macd_stoch_d"] = round(stoch_d, 2)

                        # WICHTIG: Faerbung/Signal nur EINMAL pro tatsaechlich neu geschlossener
                        # Kerze auswerten - nicht bei jedem 5-Sek-Poll, sonst wird der noch offene
                        # letzte Kerzenwert mit sich selbst verglichen und faelschlich als "heller"
                        # (Fade) gewertet, bevor die Kerze ueberhaupt geschlossen hat.
                        signal_key = closed_ts[-1]
                        if last_processed_ts != signal_key:
                            # Fade-Erkennung fuer eine offene Position: schnelles Histogramm
                            # ist noch in Positions-Richtung, wird aber schwaecher (hell statt dunkel)
                            if st["position"] is not None and last_fast_hist is not None:
                                if st["position"] == "long" and curr_fast > 0 and curr_fast <= last_fast_hist:
                                    st["macd_fade_exit"] = True
                                elif st["position"] == "short" and curr_fast < 0 and curr_fast >= last_fast_hist:
                                    st["macd_fade_exit"] = True

                            # Optionale zusaetzliche Fade-Erkennung auf dem LANGSAMEN Histogramm
                            if (cfg.get("macd_slow_fade_exit_enabled", False)
                                    and st["position"] is not None and last_slow_hist is not None):
                                if st["position"] == "long" and curr_slow > 0 and curr_slow <= last_slow_hist:
                                    st["macd_slow_fade_exit"] = True
                                elif st["position"] == "short" and curr_slow < 0 and curr_slow >= last_slow_hist:
                                    st["macd_slow_fade_exit"] = True

                            # Trendumkehr: langsames Histogramm wechselt komplett das Vorzeichen
                            # gegen die Position (nicht nur schwaecher, sondern die Trend-Praemisse
                            # der Pullback-Strategie ist hinfaellig) -> sofortiger Exit
                            if (cfg.get("macd_slow_reversal_exit_enabled", True) and st["position"] is not None):
                                if st["position"] == "long" and curr_slow < 0:
                                    st["macd_slow_reversal_exit"] = True
                                elif st["position"] == "short" and curr_slow > 0:
                                    st["macd_slow_reversal_exit"] = True

                            # Einstieg (Pullback-faehig): langsames Histogramm gibt die Trendrichtung
                            # vor (jeder Gruen-/Rot-Ton reicht), schnelles Histogramm wird GERADE
                            # dunkler in Trendrichtung (steigt bei Gruen, faellt bei Rot gegenueber
                            # dem letzten Balken) - das deckt sowohl den ersten Nulllinien-Durchgang
                            # als auch ein Wiederaufleben nach einem kurzen Gegenfarben-Dip
                            # (Pullback) ab, da waehrend einer offenen Position ohnehin nicht neu
                            # eingestiegen wird.
                            if (st["position"] is None and cfg["bot_active"]
                                    and last_entry_signal_ts != signal_key and last_fast_hist is not None):
                                direction = None
                                if curr_slow > 0 and curr_fast > 0 and curr_fast > last_fast_hist:
                                    direction = "long"
                                elif curr_slow < 0 and curr_fast < 0 and curr_fast < last_fast_hist:
                                    direction = "short"

                                if direction and cfg.get("macd_use_stochastic", True):
                                    if stoch_k is None or stoch_d is None:
                                        direction = None
                                    elif direction == "long" and stoch_k <= stoch_d:
                                        direction = None
                                    elif direction == "short" and stoch_k >= stoch_d:
                                        direction = None

                                if direction:
                                    last_entry_signal_ts = signal_key
                                    # WICHTIG: fuer den tatsaechlichen Einstieg den LIVE-Preis nutzen,
                                    # nicht den (ggf. Sekunden bis Minuten alten) Kerzenschlusspreis -
                                    # sonst weicht avg_entry_price vom echten Fuellpreis ab und SL/TP
                                    # feuern sofort falsch, weil on_price_update mit dem Live-Preis rechnet.
                                    signal_price = closed_c[-1]
                                    price = st["last_price"] if st["last_price"] is not None else signal_price
                                    debug_log(f"📡 [{symbol}] MACD-Dual Signal: {direction.upper()} @ {price} "
                                              f"(Signal-Kerze {signal_price} / schnell {round(curr_fast,4)} / langsam {round(curr_slow,4)}"
                                              f"{f' / Stoch %K={round(stoch_k,1)} %D={round(stoch_d,1)}' if stoch_k is not None else ''})")
                                    st["macd_fade_exit"] = False
                                    st["macd_slow_fade_exit"] = False
                                    st["macd_slow_reversal_exit"] = False
                                    await execute_entry(symbol, direction, price, is_add_on=False)

                            last_fast_hist = curr_fast
                            last_slow_hist = curr_slow
                            last_processed_ts = signal_key
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] MACD-Dual-Abfrage fehlgeschlagen", {"error": str(e)})

        await asyncio.sleep(5)


def compute_fib_swing(highs, lows, lookback):
    """Sucht im Lookback-Fenster den hoechsten High- und tiefsten Low-Punkt und leitet
    daraus die Fib-Richtung ab: kam das Low NACH dem High, ist der letzte Impuls ein
    Abwaertsmove -> Long-Setup (Einstieg tief im Retracement, Ziel: Rueckkehr Richtung High).
    Kam das High NACH dem Low, ist der letzte Impuls ein Aufwaertsmove -> Short-Setup."""
    window_h = highs[-lookback:]
    window_l = lows[-lookback:]
    if len(window_h) < 5:
        return None
    high_idx = window_h.index(max(window_h))
    low_idx = window_l.index(min(window_l))
    if high_idx == low_idx:
        return None
    high_val = window_h[high_idx]
    low_val = window_l[low_idx]
    direction = "long" if low_idx > high_idx else "short"
    return {"high": high_val, "low": low_val, "direction": direction}


def build_fib_levels(swing, cfg):
    """Berechnet aus Swing-High/Low und den konfigurierten Fib-Prozentwerten die
    tatsaechlichen Preise fuer Einstieg 1/2, TP1/TP2 und SL."""
    high, low, direction = swing["high"], swing["low"], swing["direction"]
    span = high - low

    def price_at(level):
        # long: 0% = High, 100% = Low (Retracement von oben nach unten gemessen)
        # short: 0% = Low, 100% = High (Retracement von unten nach oben gemessen)
        return (high - level * span) if direction == "long" else (low + level * span)

    return {
        "direction": direction,
        "high": round(high, 4), "low": round(low, 4),
        "entry1_price": round(price_at(cfg["fib_entry1_level"]), 4),
        "entry2_price": round(price_at(cfg["fib_entry2_level"]), 4),
        "tp1_price": round(price_at(cfg["fib_tp1_level"]), 4),
        "tp2_price": round(price_at(cfg["fib_tp2_level"]), 4),
        "sl_price": round(price_at(cfg["fib_sl_level"]), 4),
    }


async def fib_reversal_poll_loop(symbol):
    """Fibonacci-Reversal: solange noch kein Einstieg erfolgt ist, wird die Fib bei
    jedem Poll auf den neuesten Swing (letzte fib_lookback_candles Kerzen) neu gezogen.
    Sobald Einstieg 1 ausgefuehrt wurde, wird die Fib eingefroren (kein Nachziehen mehr),
    bis die Position komplett geschlossen ist (SL oder TP2)."""
    b = BOTS[symbol]

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "fib_reversal":
                st = b["state"]
                if not st["fib_entry1_done"]:
                    needed_bars = cfg["fib_lookback_candles"] + 5
                    data = await fetch_candles_binance(symbol, cfg["fib_resolution"], count_back=needed_bars)
                    if data:
                        timestamps, opens, highs, lows, closes = data
                        closed_h, closed_l = highs[:-1], lows[:-1]
                        if len(closed_h) >= 5:
                            swing = compute_fib_swing(closed_h, closed_l, cfg["fib_lookback_candles"])
                            if swing:
                                st["fib"] = build_fib_levels(swing, cfg)
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Fib-Reversal-Abfrage fehlgeschlagen", {"error": str(e)})

        await asyncio.sleep(30)


def calc_obi(symbol, levels):
    book = BOTS[symbol]["state"]["obi_book"]
    bids_sorted = sorted(book["bids"].items(), key=lambda x: float(x[0]), reverse=True)[:levels]
    asks_sorted = sorted(book["asks"].items(), key=lambda x: float(x[0]))[:levels]
    bid_vol = sum(v for _, v in bids_sorted)
    ask_vol = sum(v for _, v in asks_sorted)
    total = bid_vol + ask_vol
    return 0.0 if total == 0 else (bid_vol - ask_vol) / total


def update_obi_windows(symbol, raw_obi, fast_s, medium_s, slow_s):
    """Ein gemeinsamer Rohwert-Puffer, daraus werden alle drei Zeitfenster berechnet -
    effizienter als drei getrennte Puffer, und alle drei sehen exakt dieselben Rohdaten."""
    st = BOTS[symbol]["state"]
    now = time.time()
    buf = st["obi_avg_buffer"]
    buf.append((raw_obi, now))
    max_window = max(fast_s, medium_s, slow_s)
    cutoff = now - max_window
    buf = [d for d in buf if d[1] >= cutoff]
    st["obi_avg_buffer"] = buf

    def avg_over(seconds):
        window_cutoff = now - seconds
        vals = [v for v, ts in buf if ts >= window_cutoff]
        return sum(vals) / len(vals) if vals else 0.0

    return avg_over(fast_s), avg_over(medium_s), avg_over(slow_s)


def check_obi_reversal(st, fast, long_threshold, short_threshold, min_bounce):
    """Separate Long-/Short-Logik für den OBI-Reversal-Modus mit zwei eigenen Schwellenwerten:
    Läuft der OBI-EMA-Verlauf über +short_threshold (überkauft) und dreht danach wieder nach
    unten -> Short (der Kurs dreht laut Beobachtung nach dem Hoch nach unten).
    Läuft er unter -long_threshold (überverkauft) und erholt sich danach wieder -> Long
    (der Kurs steigt laut Beobachtung, sobald sich die Unterseite erholt).
    long_threshold und short_threshold werden als positive Beträge angegeben
    (z.B. long_threshold=0.20 -> Long-Zone ab OBI <= -0.20, short_threshold=0.30 -> Short-Zone ab OBI >= +0.30).
    Es wird jeweils der Extremwert innerhalb der Zone gemerkt, damit erst bei einer
    echten Umkehr (Rückprall um min_bounce) ausgelöst wird, nicht schon beim ersten Zittern."""
    prev = st.get("obi_prev_fast")
    st["obi_prev_fast"] = fast

    if fast >= short_threshold:
        st["obi_extreme_zone"] = "overbought"
        st["obi_extreme_value"] = max(fast, st.get("obi_extreme_value") if st.get("obi_extreme_value") is not None else fast)
    elif fast <= -long_threshold:
        st["obi_extreme_zone"] = "oversold"
        st["obi_extreme_value"] = min(fast, st.get("obi_extreme_value") if st.get("obi_extreme_value") is not None else fast)

    zone = st.get("obi_extreme_zone")
    if zone is None or prev is None:
        return None

    direction = None
    if zone == "overbought" and fast < prev:
        drop = st["obi_extreme_value"] - fast
        if drop >= min_bounce:
            direction = "short"
    elif zone == "oversold" and fast > prev:
        rise = fast - st["obi_extreme_value"]
        if rise >= min_bounce:
            direction = "long"

    if direction is not None:
        # Zone zuruecksetzen, damit die naechste Extremzone wieder frisch erkannt wird
        st["obi_extreme_zone"] = None
        st["obi_extreme_value"] = None

    return direction


async def handle_obi_order_book_update(symbol, msg):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if cfg["entry_mode"] != "obi_scalp":
        return

    ob = msg.get("order_book", {})
    book = st["obi_book"]
    for side_key in ("bids", "asks"):
        for level in ob.get(side_key, []):
            price = level["price"]
            size = float(level["size"])
            if size == 0:
                book[side_key].pop(price, None)
            else:
                book[side_key][price] = size

    raw_obi = calc_obi(symbol, cfg["obi_levels"])
    fast, medium, slow = update_obi_windows(
        symbol, raw_obi,
        cfg["obi_window_fast_seconds"], cfg["obi_window_medium_seconds"], cfg["obi_window_slow_seconds"],
    )
    st["obi_fast"] = round(fast, 4)
    st["obi_medium"] = round(medium, 4)
    st["obi_slow"] = round(slow, 4)
    st["obi_current"] = st["obi_fast"]

    st["obi_history"].append({"ts": int(time.time() * 1000), "fast": st["obi_fast"], "medium": st["obi_medium"], "slow": st["obi_slow"]})
    if len(st["obi_history"]) > 300:
        st["obi_history"].pop(0)

    if st["position"] is not None or not cfg["bot_active"]:
        return

    now = time.time()
    if now - st["obi_last_trade_time"] < cfg["obi_cooldown_seconds"]:
        return

    obi_mode = cfg.get("obi_mode", "momentum")
    threshold = cfg["obi_threshold"]

    if obi_mode == "reversal":
        # Separater Long-/Short-Einstieg bei Umkehr aus einer Extremzone, mit eigenen Schwellenwerten
        direction = check_obi_reversal(
            st, fast,
            cfg.get("obi_long_threshold", 0.20), cfg.get("obi_short_threshold", 0.30),
            cfg.get("obi_reversal_min_bounce", 0.05),
        )
        if direction is None:
            return
    else:
        mean_reversion = obi_mode == "mean_reversion"

        def side_of(value):
            if value >= threshold:
                return "short" if mean_reversion else "long"
            if value <= -threshold:
                return "long" if mean_reversion else "short"
            return None

        fast_dir, medium_dir, slow_dir = side_of(fast), side_of(medium), side_of(slow)

        if fast_dir is None or fast_dir != medium_dir or fast_dir != slow_dir:
            st["obi_last_signal_direction"] = None
            return
        direction = fast_dir

        if direction == st["obi_last_signal_direction"]:
            return

    # Optionaler Trendfilter: nur Longs ueber der lebenden EMA, nur Shorts darunter
    if cfg["obi_trend_filter"] and st["obi_trend_ema"] is not None and st["last_price"] is not None:
        if direction == "long" and st["last_price"] <= st["obi_trend_ema"]:
            return
        if direction == "short" and st["last_price"] >= st["obi_trend_ema"]:
            return

    if st["last_price"] is None:
        return

    st["obi_last_signal_direction"] = direction
    st["obi_last_trade_time"] = now
    debug_log(f"📡 [{symbol}] OBI-Scalp Signal: {direction.upper()} @ {st['last_price']} (schnell {round(fast,3)} / mittel {round(medium,3)} / langsam {round(slow,3)})")
    await execute_entry(symbol, direction, st["last_price"], is_add_on=False)


def update_obi_trend_ema(symbol, price, ema_length):
    st = BOTS[symbol]["state"]
    if st["obi_trend_ema"] is None:
        st["obi_trend_ema"] = price
        return
    k = 2 / (ema_length + 1)
    st["obi_trend_ema"] = price * k + st["obi_trend_ema"] * (1 - k)



async def on_price_update(symbol, price):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    st["last_price"] = price

    st["price_history"].append({"ts": int(time.time() * 1000), "price": price})
    if len(st["price_history"]) > 500:
        st["price_history"].pop(0)

    if st["anchor_price"] is None:
        st["anchor_price"] = price
        return

    bot_active = cfg["bot_active"]

    if cfg["entry_mode"] == "obi_scalp":
        if cfg["obi_trend_filter"]:
            update_obi_trend_ema(symbol, price, cfg["obi_trend_ema_length"])
        if st["position"] is not None:
            entry = st["avg_entry_price"]
            if cfg.get("obi_tp_sl_mode", "pct") == "usd":
                pnl_usd = (price - entry) * st["total_coin_size"] if st["position"] == "long" else (entry - price) * st["total_coin_size"]
                if pnl_usd >= cfg["obi_tp_usd"]:
                    await execute_exit(symbol, price, "TP")
                elif pnl_usd <= -cfg["obi_sl_usd"]:
                    await execute_exit(symbol, price, "SL")
            else:
                pnl_pct = (price - entry) / entry * 100 if st["position"] == "long" else (entry - price) / entry * 100
                if pnl_pct >= cfg["obi_tp_pct"]:
                    await execute_exit(symbol, price, "TP")
                elif pnl_pct <= -cfg["obi_sl_pct"]:
                    await execute_exit(symbol, price, "SL")
        return

    if cfg["entry_mode"] == "macd_stoch":
        if st["position"] is not None:
            entry = st["avg_entry_price"]
            pnl_usd = (price - entry) * st["total_coin_size"] if st["position"] == "long" else (entry - price) * st["total_coin_size"]
            if pnl_usd >= cfg["macd_tp_usd"]:
                await execute_exit(symbol, price, "TP")
                st["macd_fade_exit"] = False
            elif pnl_usd <= -cfg["macd_sl_usd"]:
                await execute_exit(symbol, price, "SL")
                st["macd_fade_exit"] = False
            elif cfg.get("macd_slow_reversal_exit_enabled", True) and st.get("macd_slow_reversal_exit"):
                await execute_exit(symbol, price, "MACD-SLOW-REVERSAL")
                st["macd_slow_reversal_exit"] = False
                st["macd_fade_exit"] = False
                st["macd_slow_fade_exit"] = False
            elif cfg.get("macd_fast_fade_exit_enabled", True) and st.get("macd_fade_exit"):
                await execute_exit(symbol, price, "MACD-FADE")
                st["macd_fade_exit"] = False
            elif cfg.get("macd_slow_fade_exit_enabled", False) and st.get("macd_slow_fade_exit"):
                await execute_exit(symbol, price, "MACD-SLOW-FADE")
                st["macd_slow_fade_exit"] = False
        return

    if cfg["entry_mode"] == "fib_reversal":
        fib = st.get("fib")
        if fib is None:
            return
        direction = fib["direction"]
        now = time.time()

        if st["position"] is None:
            if not bot_active or now - st["fib_last_trade_time"] < cfg["fib_cooldown_seconds"]:
                return
            reached = price <= fib["entry1_price"] if direction == "long" else price >= fib["entry1_price"]
            if reached:
                ok = await execute_entry(symbol, direction, price, is_add_on=False)
                if ok:
                    st["fib_entry1_done"] = True
                    st["fib_sl_active_price"] = fib["sl_price"]
                    debug_log(f"📡 [{symbol}] Fib-Reversal Einstieg 1: {direction.upper()} @ {price} "
                              f"(Level {cfg['fib_entry1_level']}, High {fib['high']} / Low {fib['low']})")
            return

        # Nachkauf (Einstieg 2), falls Kurs noch tiefer ins Retracement laeuft
        if not st["fib_entry2_done"]:
            reached2 = price <= fib["entry2_price"] if direction == "long" else price >= fib["entry2_price"]
            if reached2:
                ok = await execute_entry(symbol, direction, price, is_add_on=True)
                if ok:
                    st["fib_entry2_done"] = True
                    debug_log(f"📡 [{symbol}] Fib-Reversal Einstieg 2 (Nachkauf): {direction.upper()} @ {price} (Level {cfg['fib_entry2_level']})")

        # Stop-Loss (springt nach TP1 auf Ø-Einstieg = Break-Even)
        sl_price = st["fib_sl_active_price"]
        sl_hit = price <= sl_price if direction == "long" else price >= sl_price
        if sl_hit:
            await execute_exit(symbol, price, "SL")
            st["fib_entry1_done"] = False
            st["fib_entry2_done"] = False
            st["fib_tp1_done"] = False
            st["fib_sl_active_price"] = None
            st["fib_last_trade_time"] = now
            st["fib"] = None
            return

        # TP1: Teilverkauf + SL auf Break-Even
        if not st["fib_tp1_done"]:
            tp1_hit = price >= fib["tp1_price"] if direction == "long" else price <= fib["tp1_price"]
            if tp1_hit:
                fraction = cfg["fib_tp1_close_pct"] / 100
                ok = await execute_partial_exit(symbol, price, fraction, "TP1")
                if ok:
                    st["fib_tp1_done"] = True
                    st["fib_sl_active_price"] = st["avg_entry_price"]
                    debug_log(f"📡 [{symbol}] Fib-Reversal TP1 erreicht - SL auf Break-Even ({st['avg_entry_price']}) gesetzt")
            return

        # TP2: Rest schliessen
        tp2_hit = price >= fib["tp2_price"] if direction == "long" else price <= fib["tp2_price"]
        if tp2_hit:
            await execute_exit(symbol, price, "TP2")
            st["fib_entry1_done"] = False
            st["fib_entry2_done"] = False
            st["fib_tp1_done"] = False
            st["fib_sl_active_price"] = None
            st["fib"] = None
        return

    if st["position"] is None:
        if not bot_active or cfg["entry_mode"] != "grid":
            return
        grid_step_abs = compute_step_abs(st["anchor_price"], cfg, "grid")
        if price <= st["anchor_price"] - grid_step_abs:
            await execute_entry(symbol, "long", price, is_add_on=False)
        elif price >= st["anchor_price"] + grid_step_abs:
            await execute_entry(symbol, "short", price, is_add_on=False)
        return

    if cfg["entry_mode"] != "grid":
        return

    tp_step_abs = compute_step_abs(st["avg_entry_price"], cfg, "tp")
    # Nachkauf-Abstand wird vom LETZTEN Kaufpreis gemessen, nicht vom laufenden
    # Durchschnitt - sonst schrumpft der Abstand zwischen Nachkaeufen immer weiter.
    grid_step_abs = compute_step_abs(st["last_entry_price"] or st["avg_entry_price"], cfg, "grid")
    max_nachkauf = cfg["max_nachkauf"]

    if st["position"] == "long":
        if price >= st["avg_entry_price"] + tp_step_abs:
            await execute_exit(symbol, price, "TP")
        elif bot_active and price <= st["last_entry_price"] - grid_step_abs and (max_nachkauf == 0 or st["entry_count"] < max_nachkauf):
            await execute_entry(symbol, "long", price, is_add_on=True)
    elif st["position"] == "short":
        if price <= st["avg_entry_price"] - tp_step_abs:
            await execute_exit(symbol, price, "TP")
        elif bot_active and price >= st["last_entry_price"] + grid_step_abs and (max_nachkauf == 0 or st["entry_count"] < max_nachkauf):
            await execute_entry(symbol, "short", price, is_add_on=True)



async def trading_loop():
    last_status_log = 0.0

    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20) as ws:
                for s in SYMBOLS:
                    await ws.send(json.dumps({"type": "subscribe", "channel": f"trade/{MARKET_INDICES[s]}"}))
                    await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{MARKET_INDICES[s]}"}))
                debug_log(f"✅ Verbunden für {', '.join(SYMBOLS)}")

                async for raw in ws:
                    msg = json.loads(raw)
                    channel = msg.get("channel", "")
                    try:
                        market_index = int(channel.split(":")[1].split("/")[0]) if ":" in channel else int(channel.split("/")[1])
                    except Exception:
                        market_index = None
                    symbol = MARKET_INDEX_TO_SYMBOL.get(market_index)

                    if channel.startswith("trade") and symbol:
                        trades = msg.get("trades", [])
                        if trades:
                            price = float(trades[-1]["price"])
                            await on_price_update(symbol, price)
                    elif channel.startswith("order_book") and symbol:
                        await handle_obi_order_book_update(symbol, msg)

                    now = time.time()
                    if now - last_status_log >= 20:
                        last_status_log = now
                        summary = {s: {"pos": BOTS[s]["state"]["position"] or "flach", "preis": BOTS[s]["state"]["last_price"],
                                       "trades": BOTS[s]["state"]["stats"]["trades"]} for s in SYMBOLS}
                        debug_log("📊 Multi-Coin Status", summary)
        except Exception as e:
            debug_log("⚠️ Verbindung verloren, reconnect in 5s", {"error": str(e), "traceback": traceback.format_exc()})
            await asyncio.sleep(5)


# ========== WEB-DASHBOARD ==========
