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
        url = f"https://api.binance.com/api/v3/klines?symbol={pair}&interval={resolution}&limit={min(count_back, 1000)}"
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


BINANCE_INTERVAL_MS = {
    "1m": 60_000, "2m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
    "30m": 1_800_000, "1h": 3_600_000, "4h": 14_400_000,
}


SYNTHETIC_RESOLUTIONS = {"2m": ("1m", 2), "10s": ("1s", 10), "15s": ("1s", 15), "30s": ("1s", 30), "45s": ("1s", 45)}  # Zeitrahmen, die Binance nicht nativ anbietet


async def fetch_historical_candles_binance(symbol, resolution, days, max_candles):
    """Holt bis zu 'days' Tage Kerzenhistorie von Binance fuer Backtests, in 1000er-
    Batches paginiert (endTime schrittweise nach hinten). '2m'/'30s' werden - wie live -
    aus 1m- bzw. 1s-Kerzen synthetisch zusammengesetzt (siehe SYNTHETIC_RESOLUTIONS).
    max_candles begrenzt hart, wie viele Kerzen am Ende verarbeitet werden
    (Performance-Schutz fuer den Render-Server)."""
    pair = BINANCE_SYMBOL_MAP.get(symbol)
    if not pair:
        return None, "Coin nicht auf Binance verfügbar"

    synth = SYNTHETIC_RESOLUTIONS.get(resolution)
    base_resolution = synth[0] if synth else resolution
    fetch_factor = synth[1] if synth else 1
    total_ms = days * 24 * 60 * 60 * 1000
    end_time = int(time.time() * 1000)
    start_time = end_time - total_ms
    # Hartes Limit an Basis-Kerzen (vor evtl. Zusammenfassung), damit die Anfrage nicht ausufert
    hard_candle_cap = max_candles * fetch_factor + 2000

    all_rows = []
    cursor = end_time
    requests_made = 0
    try:
        async with aiohttp.ClientSession() as session:
            while cursor > start_time and len(all_rows) < hard_candle_cap:
                url = f"https://api.binance.com/api/v3/klines?symbol={pair}&interval={base_resolution}&limit=1000&endTime={cursor}"
                retry_count = 0
                batch = None
                while retry_count < 5:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                        if resp.status == 429 or resp.status == 418:
                            # Rate-Limit erreicht - NICHT abbrechen, sondern warten und
                            # denselben Request nochmal versuchen (mit steigender Wartezeit).
                            # Wichtig bei 30s-Anfragen, da die 2x so viele Basis-Kerzen wie
                            # 15s brauchen und dadurch viel eher an das Limit stossen.
                            retry_after = resp.headers.get("Retry-After")
                            wait_s = float(retry_after) if retry_after else (1.5 * (retry_count + 1))
                            await asyncio.sleep(wait_s)
                            retry_count += 1
                            continue
                        if resp.status != 200:
                            batch = None
                            break
                        batch = await resp.json()
                    break
                if batch is None:
                    if retry_count >= 5:
                        return None, f"Binance-Ratelimit nach {requests_made} Anfragen und 5 Wiederholungsversuchen weiterhin aktiv - bitte kurz warten und erneut versuchen."
                    break
                requests_made += 1
                if not batch:
                    break
                all_rows = batch + all_rows
                cursor = int(batch[0][0]) - 1
                if len(batch) < 1000:
                    break
                await asyncio.sleep(0.25)  # Binance-Ratelimit-freundlich (leicht erhoeht)
    except Exception as e:
        return None, f"Abruf fehlgeschlagen nach {requests_made} Anfragen: {e}"

    if not all_rows:
        return None, "Keine Daten erhalten"

    all_rows = [r for r in all_rows if int(r[0]) >= start_time]
    timestamps = [int(r[0]) for r in all_rows]
    opens = [float(r[1]) for r in all_rows]
    highs = [float(r[2]) for r in all_rows]
    lows = [float(r[3]) for r in all_rows]
    closes = [float(r[4]) for r in all_rows]

    if synth:
        timestamps, opens, highs, lows, closes = resample_candles((timestamps, opens, highs, lows, closes), synth[1])

    if len(closes) > max_candles:
        timestamps = timestamps[-max_candles:]
        opens = opens[-max_candles:]
        highs = highs[-max_candles:]
        lows = lows[-max_candles:]
        closes = closes[-max_candles:]

    return (timestamps, opens, highs, lows, closes), None


def resample_candles(data, factor):
    """Fasst je 'factor' aufeinanderfolgende Kerzen zu einer groesseren Kerze zusammen
    (Open der ersten, High/Low ueber alle, Close der letzten, Zeitstempel der ersten).
    Noetig fuer Zeitrahmen, die Binance nicht direkt anbietet (z.B. 2m)."""
    timestamps, opens, highs, lows, closes = data
    n = (len(closes) // factor) * factor
    if n == 0:
        return [], [], [], [], []
    out_ts, out_o, out_h, out_l, out_c = [], [], [], [], []
    for i in range(0, n, factor):
        chunk_o = opens[i:i + factor]
        chunk_h = highs[i:i + factor]
        chunk_l = lows[i:i + factor]
        chunk_c = closes[i:i + factor]
        out_ts.append(timestamps[i])
        out_o.append(chunk_o[0])
        out_h.append(max(chunk_h))
        out_l.append(min(chunk_l))
        out_c.append(chunk_c[-1])
    return out_ts, out_o, out_h, out_l, out_c


SUB_MINUTE_RESOLUTIONS = {"10s": 10, "15s": 15, "30s": 30, "45s": 45}  # Sekunden je Kerze, alle aus dem 1s-Puffer


def get_seconds_candles(state, seconds, needed_bars):
    """Baut Kerzen mit 'seconds' Sekunden Laenge (10/15/30). Bevorzugt den Puffer echter
    Binance-1s-Kerzen (siehe binance_1s_poll_loop) - reicht der noch nicht (frisch
    gestartet oder Coin nicht auf Binance gelistet), faellt die Funktion automatisch auf
    die selbst aus Live-Lighter-Ticks gebauten 1s-Kerzen zurueck (siehe on_price_update),
    damit wirklich JEDER Coin Sekunden-Zeitrahmen nutzen kann. Liefert None, wenn auch
    davon noch nicht genug da ist."""
    binance_buf = state.get("binance_1s_buffer", [])
    source = binance_buf if len(binance_buf) >= seconds else state.get("local_1s_buffer", [])
    if len(source) < seconds:
        return None
    ts = [c["ts"] for c in source]
    o = [c["o"] for c in source]
    h = [c["h"] for c in source]
    l = [c["l"] for c in source]
    cl = [c["c"] for c in source]
    r_ts, r_o, r_h, r_l, r_c = resample_candles((ts, o, h, l, cl), seconds)
    if len(r_c) > needed_bars:
        return r_ts[-needed_bars:], r_o[-needed_bars:], r_h[-needed_bars:], r_l[-needed_bars:], r_c[-needed_bars:]
    return r_ts, r_o, r_h, r_l, r_c


async def binance_1s_poll_loop(symbol):
    """Sammelt fortlaufend echte Binance-1s-Kerzen in einen wachsenden Puffer pro Coin.
    Noetig, weil eine einzelne Live-Abfrage auf max. 1000 Kerzen begrenzt ist - das
    reicht bei 1s-Basis nur fuer ~33 x 30s-Kerzen, zu wenig Aufwaermphase fuer die
    meisten Strategien. Beim Start wird der Puffer per paginiertem Bulk-Abruf SOFORT
    mit ca. 3 Stunden Historie vorbefuellt (dauert nur ein paar Sekunden), damit man
    nicht erst 15+ Minuten in Echtzeit auf genug Kerzen warten muss. Danach waechst
    der Puffer laufend per 5-Sekunden-Poll weiter (dedupliziert per Zeitstempel)."""
    if BINANCE_SYMBOL_MAP.get(symbol) is None:
        return  # Coin nicht auf Binance - keine 1s-Daten moeglich, Loop hat nichts zu tun
    b = BOTS[symbol]
    st = b["state"]

    if not st.get("binance_1s_buffer"):
        try:
            seed, err = await fetch_historical_candles_binance(symbol, "1s", days=0.125, max_candles=10800)  # ~3 Stunden
            if seed:
                timestamps, opens, highs, lows, closes = seed
                st["binance_1s_buffer"] = [
                    {"ts": timestamps[i], "o": opens[i], "h": highs[i], "l": lows[i], "c": closes[i]}
                    for i in range(len(timestamps))
                ]
                debug_log(f"✅ [{symbol}] Binance-1s-Puffer vorbefüllt", {"kerzen": len(timestamps)})
            elif err:
                debug_log(f"⚠️ [{symbol}] Binance-1s-Vorbefüllung fehlgeschlagen", {"error": err})
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Binance-1s-Vorbefüllung fehlgeschlagen", {"error": str(e)})

    while True:
        try:
            data = await fetch_candles_binance(symbol, "1s", count_back=1000)
            if data:
                timestamps, opens, highs, lows, closes = data
                buffer = st.get("binance_1s_buffer", [])
                existing_ts = {c["ts"] for c in buffer}
                for i in range(len(timestamps)):
                    if timestamps[i] not in existing_ts:
                        buffer.append({"ts": timestamps[i], "o": opens[i], "h": highs[i], "l": lows[i], "c": closes[i]})
                buffer.sort(key=lambda c: c["ts"])
                if len(buffer) > 10000:  # ~2.75 Stunden 1s-Historie (reduziert wegen Speicherlimit)
                    buffer = buffer[-10000:]
                st["binance_1s_buffer"] = buffer
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Binance-1s-Puffer-Abfrage fehlgeschlagen", {"error": str(e)})
        await asyncio.sleep(5)


async def fetch_candles_binance_multi(symbol, resolution, count_back=150):
    """Wie fetch_candles_binance, kann aber zusaetzlich synthetische Zeitrahmen liefern
    (z.B. 2m), die Binance selbst nicht unterstuetzt - dafuer wird die naechstkleinere
    native Aufloesung geholt und zu groesseren Kerzen zusammengefasst."""
    if resolution in SYNTHETIC_RESOLUTIONS:
        base_resolution, factor = SYNTHETIC_RESOLUTIONS[resolution]
        data = await fetch_candles_binance(symbol, base_resolution, count_back=count_back * factor)
        if data is None:
            return None
        return resample_candles(data, factor)
    return await fetch_candles_binance(symbol, resolution, count_back=count_back)


def _ema_series(values, length):
    if not values:
        return []
    k = 2 / (length + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


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
                    resolution = cfg["fib_resolution"]
                    if resolution in SUB_MINUTE_RESOLUTIONS:
                        local = get_seconds_candles(st, SUB_MINUTE_RESOLUTIONS[resolution], needed_bars)
                        if local:
                            _, _, closed_h, closed_l, _ = local
                        else:
                            closed_h = None
                    else:
                        data = await fetch_candles_binance(symbol, resolution, count_back=needed_bars)
                        if data:
                            timestamps, opens, highs, lows, closes = data
                            closed_h, closed_l = highs[:-1], lows[:-1]
                        else:
                            closed_h = None
                    if closed_h and len(closed_h) >= 5:
                        swing = compute_fib_swing(closed_h, closed_l, cfg["fib_lookback_candles"])
                        if swing:
                            st["fib"] = build_fib_levels(swing, cfg)
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Fib-Reversal-Abfrage fehlgeschlagen", {"error": str(e)})

        await asyncio.sleep(30)


async def stoch_cross_poll_loop(symbol):
    """Einfache Stochastic-Cross-Strategie: %K kreuzt von unten nach oben durch die
    Ueberverkauft-Schwelle (Standard 20) -> Long. %K kreuzt von oben nach unten durch
    die Ueberkauft-Schwelle (Standard 80) -> Short.
    Optionale, standardmaessig AUSgeschaltete Erweiterungen (aendern nichts, solange
    nicht im Dashboard aktiviert):
    - Trendfilter: nur Long ueber, nur Short unter einer EMA
    - ATR-basiertes SL/TP statt festem $-Betrag
    - Range-Profile-Kontext: nur Long unter, nur Short ueber der POC-Mittellinie
    - Squeeze-Bestaetigung: nur Einstieg, wenn direkt vorher eine Kanal-Verengung war"""
    b = BOTS[symbol]
    last_k = None
    last_processed_ts = None
    last_entry_signal_ts = None

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "stoch_cross":
                base_needed = cfg["stoch_cross_k_period"] + cfg["stoch_cross_k_smooth"] + cfg["stoch_cross_d_period"] + 20
                needed_bars = max(60, base_needed)
                if cfg.get("stoch_cross_trend_filter_enabled", False):
                    needed_bars = max(needed_bars, cfg["stoch_cross_trend_ema_period"] * 5)
                if cfg.get("stoch_cross_sl_tp_mode", "fixed") == "atr":
                    needed_bars = max(needed_bars, cfg["stoch_cross_atr_period"] * 5 + 20)
                if cfg.get("stoch_cross_rp_filter_enabled", False) or cfg.get("stoch_cross_require_squeeze", False):
                    needed_bars = max(needed_bars, cfg["stoch_cross_rp_lookback"] + 10)
                needed_bars = min(1000, needed_bars)

                resolution = cfg["stoch_cross_resolution"]
                if resolution in SUB_MINUTE_RESOLUTIONS:
                    local = get_seconds_candles(b["state"], SUB_MINUTE_RESOLUTIONS[resolution], needed_bars)
                    if local:
                        closed_ts, closed_o, closed_h, closed_l, closed_c = local
                    else:
                        closed_ts = None
                else:
                    data = await fetch_candles_binance_multi(symbol, resolution, count_back=needed_bars)
                    if data:
                        timestamps, opens, highs, lows, closes = data
                        closed_ts = timestamps[:-1]
                        closed_o, closed_h, closed_l, closed_c = opens[:-1], highs[:-1], lows[:-1], closes[:-1]
                    else:
                        closed_ts = None

                if closed_ts:
                    if len(closed_c) >= cfg["stoch_cross_k_period"] + cfg["stoch_cross_k_smooth"] + cfg["stoch_cross_d_period"]:
                        k_series, d_series = compute_stochastic(closed_h, closed_l, closed_c,
                                                                  cfg["stoch_cross_k_period"], cfg["stoch_cross_k_smooth"], cfg["stoch_cross_d_period"])
                        if k_series:
                            st = b["state"]
                            curr_k = k_series[-1]
                            curr_d = d_series[-1] if d_series else None
                            st["stoch_cross_k"] = round(curr_k, 2)
                            st["stoch_cross_d"] = round(curr_d, 2) if curr_d is not None else None

                            # Nur einmal pro tatsaechlich neu geschlossener Kerze auswerten
                            signal_key = closed_ts[-1]
                            if last_processed_ts != signal_key:
                                # Optionaler Range-Profile-Kontext + Squeeze (teilen sich denselben Snapshot)
                                rp_snap = None
                                squeeze_before_entry = False
                                if cfg.get("stoch_cross_rp_filter_enabled", False) or cfg.get("stoch_cross_require_squeeze", False):
                                    rp_lookback = cfg["stoch_cross_rp_lookback"]
                                    if len(closed_c) > rp_lookback + 2:
                                        rp_snap = compute_range_profile_snapshot(closed_h, closed_l, closed_c, closed_o, rp_lookback, 50, 80.0)
                                        if rp_snap:
                                            st["stoch_cross_rp_mid"] = round(rp_snap["mid_price"], 4)
                                            channel_width = rp_snap["range_high"] - rp_snap["range_low"]
                                            squeeze_before_entry = st.get("stoch_cross_squeeze_active", False)
                                            width_history = st.get("stoch_cross_width_history", [])
                                            avg_width = sum(width_history) / len(width_history) if len(width_history) >= 5 else None
                                            squeeze_now = (avg_width is not None
                                                           and channel_width < avg_width * (cfg["stoch_cross_squeeze_threshold_pct"] / 100))
                                            st["stoch_cross_channel_width"] = round(channel_width, 4)
                                            st["stoch_cross_avg_width"] = round(avg_width, 4) if avg_width is not None else None
                                            st["stoch_cross_squeeze_active"] = squeeze_now
                                            width_history.append(channel_width)
                                            if len(width_history) > cfg["stoch_cross_squeeze_lookback"]:
                                                width_history = width_history[-cfg["stoch_cross_squeeze_lookback"]:]
                                            st["stoch_cross_width_history"] = width_history

                                # Optionaler ATR-Wert (fuer SL/TP-Modus "atr")
                                atr_val = None
                                if cfg.get("stoch_cross_sl_tp_mode", "fixed") == "atr":
                                    atr_series = compute_atr(closed_h, closed_l, closed_c, cfg["stoch_cross_atr_period"])
                                    if atr_series:
                                        atr_val = atr_series[-1]

                                if (st["position"] is None and cfg["bot_active"]
                                        and last_entry_signal_ts != signal_key and last_k is not None):
                                    direction = None
                                    if last_k <= cfg["stoch_cross_oversold"] and curr_k > cfg["stoch_cross_oversold"]:
                                        direction = "long"
                                    elif last_k >= cfg["stoch_cross_overbought"] and curr_k < cfg["stoch_cross_overbought"]:
                                        direction = "short"

                                    # Optionaler Trendfilter
                                    if direction and cfg.get("stoch_cross_trend_filter_enabled", False):
                                        ema_series = _ema_series(closed_c, cfg["stoch_cross_trend_ema_period"])
                                        trend_ema = ema_series[-1] if ema_series else None
                                        if trend_ema is not None:
                                            price_now = closed_c[-1]
                                            if direction == "long" and price_now <= trend_ema:
                                                direction = None
                                            elif direction == "short" and price_now >= trend_ema:
                                                direction = None

                                    # Optionaler Range-Profile-Kontextfilter
                                    if direction and cfg.get("stoch_cross_rp_filter_enabled", False) and rp_snap:
                                        price_now = closed_c[-1]
                                        if direction == "long" and price_now >= rp_snap["mid_price"]:
                                            direction = None
                                        elif direction == "short" and price_now <= rp_snap["mid_price"]:
                                            direction = None

                                    # Optionale Squeeze-Bestaetigung
                                    if direction and cfg.get("stoch_cross_require_squeeze", False) and not squeeze_before_entry:
                                        direction = None

                                    if direction:
                                        last_entry_signal_ts = signal_key
                                        price = st["last_price"] if st["last_price"] is not None else closed_c[-1]

                                        if cfg.get("stoch_cross_sl_tp_mode", "fixed") == "atr" and atr_val:
                                            risk_dist = atr_val * cfg["stoch_cross_sl_atr_mult"]
                                            reward_dist = atr_val * cfg["stoch_cross_tp_atr_mult"]
                                            sl_price = price - risk_dist if direction == "long" else price + risk_dist
                                            tp_price = price + reward_dist if direction == "long" else price - reward_dist
                                            st["stoch_cross_sl_price"] = sl_price
                                            st["stoch_cross_tp_price"] = tp_price
                                        else:
                                            st["stoch_cross_sl_price"] = None
                                            st["stoch_cross_tp_price"] = None

                                        debug_log(f"📡 [{symbol}] Stochastic-Cross Signal: {direction.upper()} @ {price} (%K {round(curr_k,2)})")
                                        await execute_entry(symbol, direction, price, is_add_on=False)

                                last_k = curr_k
                                last_processed_ts = signal_key
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Stochastic-Cross-Abfrage fehlgeschlagen", {"error": str(e)})

        await asyncio.sleep(5)


def compute_atr(highs, lows, closes, period):
    """ATR mit Wilder-RMA-Glaettung (wie Pine's ta.atr), fuer marktadaptive SL-Groesse."""
    n = len(closes)
    if n < 2:
        return []
    tr = [highs[0] - lows[0]] + [0.0] * (n - 1)
    for i in range(1, n):
        tr[i] = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
    atr = [tr[0]] * n
    for i in range(1, n):
        if i < period:
            atr[i] = sum(tr[:i + 1]) / (i + 1)
        else:
            atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period
    return atr


def compute_pivot_centers(highs, lows, prd):
    """Portiert die Pivot-Point-Center-Linie aus 'Pivot Point SuperTrend' (LonesomeTheBlue).
    Ein Pivot-Hoch/-Tief bei Index j gilt erst als bestaetigt, sobald prd Kerzen NACH j
    bekannt sind (kein Repainting) - deshalb bezieht sich Index i in der Rueckgabe auf
    das Pivot bei i-prd, nicht auf i selbst. Haengt NUR von prd ab (nicht vom ATR-Faktor),
    kann daher fuer alle Faktor-Varianten eines Sweeps wiederverwendet werden."""
    n = len(highs)
    center_arr = [None] * n
    center = None
    for i in range(n):
        j = i - prd
        lastpp = None
        if j - prd >= 0 and j + prd == i:
            window_h = highs[j - prd:j + prd + 1]
            window_l = lows[j - prd:j + prd + 1]
            if highs[j] == max(window_h):
                lastpp = highs[j]
            elif lows[j] == min(window_l):
                lastpp = lows[j]
        if lastpp is not None:
            center = lastpp if center is None else (center * 2 + lastpp) / 3
        center_arr[i] = center
    return center_arr


def compute_pivot_supertrend_trend(closes, atr_series, center_arr, factor):
    """Baut aus der Center-Linie + ATR-Faktor die Trail-Stop-Linien (TUp/TDown) und den
    Trend (+1/-1) - das ist der Teil, der vom ATR-Faktor abhaengt und pro Sweep-Kombination
    neu gerechnet werden muss (aber billig ist, da nur O(n) ohne die teure Pivot-Suche)."""
    n = len(closes)
    trend = [1] * n
    tup = [None] * n
    tdown = [None] * n
    for i in range(n):
        center = center_arr[i]
        a = atr_series[i] if i < len(atr_series) else None
        if center is None or a is None:
            tup[i] = tup[i - 1] if i > 0 else None
            tdown[i] = tdown[i - 1] if i > 0 else None
            trend[i] = trend[i - 1] if i > 0 else 1
            continue
        up = center - factor * a
        dn = center + factor * a
        prev_close = closes[i - 1] if i > 0 else closes[i]
        prev_tup = tup[i - 1] if i > 0 and tup[i - 1] is not None else up
        prev_tdown = tdown[i - 1] if i > 0 and tdown[i - 1] is not None else dn
        cur_tup = max(up, prev_tup) if prev_close > prev_tup else up
        cur_tdown = min(dn, prev_tdown) if prev_close < prev_tdown else dn
        tup[i] = cur_tup
        tdown[i] = cur_tdown
        prev_trend = trend[i - 1] if i > 0 else 1
        if closes[i] > prev_tdown:
            cur_trend = 1
        elif closes[i] < prev_tup:
            cur_trend = -1
        else:
            cur_trend = prev_trend
        trend[i] = cur_trend
    return trend, tup, tdown


def compute_wma(values, length):
    """Gewichteter gleitender Durchschnitt (linear steigende Gewichte, neuester Wert
    zaehlt am staerksten) - wie Pine's ta.wma. Erste (length-1) Werte sind None (nicht
    genug Historie fuer ein volles Fenster)."""
    n = len(values)
    out = [None] * n
    weight_sum = length * (length + 1) / 2
    for i in range(n):
        if i < length - 1:
            continue
        window = values[i - length + 1:i + 1]
        weighted = sum(v * (j + 1) for j, v in enumerate(window))
        out[i] = weighted / weight_sum
    return out


def compute_nsdt_channel(highs, lows, length, upperspace, lowerspace):
    """Portiert den 'NSDT Scalping Channel': obere Linie = WMA(High * upperspace),
    untere Linie = WMA(Low * lowerspace). Long-Signal wenn die untere Linie 2 Kerzen
    in Folge steigt, Short-Signal wenn die obere Linie 2 Kerzen in Folge faellt."""
    upper_raw = [h * upperspace for h in highs]
    lower_raw = [l * lowerspace for l in lows]
    upper_line = compute_wma(upper_raw, length)
    lower_line = compute_wma(lower_raw, length)
    return upper_line, lower_line


def compute_range_profile_snapshot(highs, lows, closes, opens, lookback, bins, ob_os_level):
    """Portiert aus dem Pine-Script 'Range Profile Oscillator': baut ueber die letzten
    'lookback' Kerzen ein Bullen-/Baeren-gewichtetes Histogramm (Volumen-Profil-Prinzip,
    aber gewichtet nach Kerzenrichtung statt echtem Volumen), findet den Point of Control
    (staerkste Preiszone) als Mittellinie und zieht einen Kanal, der ob_os_level% des
    Gesamtgewichts um die Mittellinie einschliesst. Gibt None zurueck, wenn nicht genug
    Daten oder eine Preisspanne von 0 vorliegt."""
    if len(closes) < lookback:
        return None
    h, l, c, o = highs[-lookback:], lows[-lookback:], closes[-lookback:], opens[-lookback:]

    minL, maxH = min(l), max(h)
    price_range = maxH - minL
    if price_range <= 0:
        return None
    bin_size = price_range / bins
    bull_w = [0.0] * bins
    bear_w = [0.0] * bins

    for i in range(lookback):
        candle_size = h[i] - l[i]
        if candle_size <= 0:
            continue
        b1 = max(0, min(bins - 1, int((l[i] - minL) / bin_size)))
        b2 = max(0, min(bins - 1, int((h[i] - minL) / bin_size)))
        is_bull = c[i] >= o[i]
        for b in range(b1, b2 + 1):
            if is_bull:
                bull_w[b] += bin_size
            else:
                bear_w[b] += bin_size

    total_per_bin = [bull_w[i] + bear_w[i] for i in range(bins)]
    mid_bin = max(range(bins), key=lambda i: total_per_bin[i])
    mid_price = minL + bin_size * (mid_bin + 0.5)

    total = sum(total_per_bin)
    if total <= 0:
        return None
    remaining = total * (ob_os_level / 100) - total_per_bin[mid_bin]
    lowB, highB = mid_bin, mid_bin
    while remaining > 0 and (lowB > 0 or highB < bins - 1):
        upper = total_per_bin[highB + 1] if highB < bins - 1 else -1
        lower = total_per_bin[lowB - 1] if lowB > 0 else -1
        if upper >= lower and upper >= 0:
            highB += 1
            remaining -= upper
        elif lower >= 0:
            lowB -= 1
            remaining -= lower
        else:
            break

    range_low = minL + lowB * bin_size
    range_high = minL + (highB + 1) * bin_size
    half_range = (range_high - range_low) / 2
    if half_range <= 0:
        return None
    osc = (c[-1] - mid_price) / half_range * ob_os_level
    return {"mid_price": mid_price, "range_high": range_high, "range_low": range_low, "osc": osc}


async def range_profile_poll_loop(symbol):
    """Range-Profile-Strategie (portiert aus dem 'Range Profile Oscillator'-Indikator),
    zwei waehlbare Modi:
    - reversion (empfohlen): Ausbruch ueber/unter den Kanal wird als Gegenrichtung gehandelt.
    - momentum: wie im Original-Indikator, Ausbruch wird in Ausbruchsrichtung gehandelt.
    TP und SL sind feste $-Betraege (siehe on_price_update), plus optionale Gewinn-
    Absicherung (Breakeven-Lock), genau wie bei MACD-Simple."""
    b = BOTS[symbol]
    last_osc = None
    last_processed_ts = None
    last_entry_signal_ts = None

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "range_profile":
                lookback = cfg["rp_lookback"]
                needed_bars = min(1000, lookback + 60)
                resolution = cfg["rp_resolution"]
                if resolution in SUB_MINUTE_RESOLUTIONS:
                    local = get_seconds_candles(b["state"], SUB_MINUTE_RESOLUTIONS[resolution], needed_bars)
                    if local:
                        closed_ts, closed_o, closed_h, closed_l, closed_c = local
                    else:
                        closed_ts = None
                else:
                    data = await fetch_candles_binance_multi(symbol, resolution, count_back=needed_bars)
                    if data:
                        timestamps, opens, highs, lows, closes = data
                        closed_ts = timestamps[:-1]
                        closed_o, closed_h, closed_l, closed_c = opens[:-1], highs[:-1], lows[:-1], closes[:-1]
                    else:
                        closed_ts = None

                if closed_ts:
                    if len(closed_c) > lookback + 2:
                        snap = compute_range_profile_snapshot(closed_h, closed_l, closed_c, closed_o, lookback, 50, cfg["rp_ob_os_level"])
                        if snap:
                            st = b["state"]
                            curr_osc = snap["osc"]
                            st["rp_osc"] = round(curr_osc, 2)
                            st["rp_mid_price"] = round(snap["mid_price"], 4)
                            st["rp_range_high"] = round(snap["range_high"], 4)
                            st["rp_range_low"] = round(snap["range_low"], 4)

                            signal_key = closed_ts[-1]
                            if last_processed_ts != signal_key:
                                # Squeeze-Erkennung: Kanalbreite dieser Kerze vs. Durchschnitt der
                                # letzten rp_squeeze_lookback Kerzen. squeeze_before_entry ist der
                                # Zustand VOR dieser Kerze - das ist die eigentliche Vorwarnung.
                                channel_width = snap["range_high"] - snap["range_low"]
                                squeeze_before_entry = st.get("rp_squeeze_active", False)
                                width_history = st.get("rp_width_history", [])
                                avg_width = sum(width_history) / len(width_history) if len(width_history) >= 5 else None
                                squeeze_now = (avg_width is not None
                                               and channel_width < avg_width * (cfg["rp_squeeze_threshold_pct"] / 100))
                                st["rp_channel_width"] = round(channel_width, 4)
                                st["rp_avg_width"] = round(avg_width, 4) if avg_width is not None else None
                                st["rp_squeeze_active"] = squeeze_now
                                width_history.append(channel_width)
                                if len(width_history) > cfg["rp_squeeze_lookback"]:
                                    width_history = width_history[-cfg["rp_squeeze_lookback"]:]
                                st["rp_width_history"] = width_history

                                if (st["position"] is None and cfg["bot_active"]
                                        and last_entry_signal_ts != signal_key and last_osc is not None):
                                    ob = cfg["rp_ob_os_level"]
                                    breakout_up = last_osc <= ob and curr_osc > ob
                                    breakout_down = last_osc >= -ob and curr_osc < -ob
                                    mode = cfg.get("rp_mode", "reversion")

                                    direction = None
                                    if breakout_up:
                                        direction = "short" if mode == "reversion" else "long"
                                    elif breakout_down:
                                        direction = "long" if mode == "reversion" else "short"

                                    if direction and cfg.get("rp_require_squeeze", False) and not squeeze_before_entry:
                                        direction = None

                                    if direction:
                                        last_entry_signal_ts = signal_key
                                        price = st["last_price"] if st["last_price"] is not None else closed_c[-1]
                                        debug_log(f"📡 [{symbol}] Range-Profile Signal ({mode}): {direction.upper()} @ {price} "
                                                  f"| Mitte {round(snap['mid_price'],4)} | Oszillator {round(curr_osc,2)}")
                                        await execute_entry(symbol, direction, price, is_add_on=False)

                                last_osc = curr_osc
                                last_processed_ts = signal_key
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Range-Profile-Abfrage fehlgeschlagen", {"error": str(e)})

        await asyncio.sleep(5)


async def pp_supertrend_poll_loop(symbol):
    """Pivot Point SuperTrend (portiert aus dem Indikator von LonesomeTheBlue): Trail-Stop
    aus Pivot-Hochs/-Tiefs statt einfachem Schlusskurs, dadurch ruhiger als klassischer
    SuperTrend. Einstieg bei Trendwechsel (Buy: Trend -1 -> 1, Sell: Trend 1 -> -1).
    TP/SL sind feste $-Betraege."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_trend = None

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "pp_supertrend":
                prd = cfg["pps_period"]
                atr_period = cfg["pps_atr_period"]
                needed_bars = min(1000, max(prd * 4, atr_period * 5, 60) + 5)
                resolution = cfg["pps_resolution"]

                if resolution in SUB_MINUTE_RESOLUTIONS:
                    local = get_seconds_candles(b["state"], SUB_MINUTE_RESOLUTIONS[resolution], needed_bars)
                    if local:
                        closed_ts, _, closed_h, closed_l, closed_c = local
                    else:
                        closed_ts = None
                else:
                    data = await fetch_candles_binance_multi(symbol, resolution, count_back=needed_bars)
                    if data:
                        timestamps, opens, highs, lows, closes = data
                        closed_ts = timestamps[:-1]
                        closed_h, closed_l, closed_c = highs[:-1], lows[:-1], closes[:-1]
                    else:
                        closed_ts = None

                if closed_ts and len(closed_c) > max(prd * 3, atr_period + 2):
                    atr_series = compute_atr(closed_h, closed_l, closed_c, atr_period)
                    centers = compute_pivot_centers(closed_h, closed_l, prd)
                    trend, tup, tdown = compute_pivot_supertrend_trend(closed_c, atr_series, centers, cfg["pps_atr_factor"])
                    st = b["state"]
                    curr_trend = trend[-1]
                    st["pps_trend"] = curr_trend
                    st["pps_trailing_sl"] = round((tup[-1] if curr_trend == 1 else tdown[-1]) or 0, 4)

                    signal_key = closed_ts[-1]
                    if last_processed_ts != signal_key:
                        last_processed_ts = signal_key
                        if last_trend is not None and curr_trend != last_trend and cfg["bot_active"]:
                            direction = "long" if curr_trend == 1 else "short"
                            price = st["last_price"] if st["last_price"] is not None else closed_c[-1]

                            if st["position"] is None:
                                debug_log(f"📡 [{symbol}] Pivot-SuperTrend Signal: {direction.upper()} @ {price} (Trend {last_trend}->{curr_trend})")
                                await execute_entry(symbol, direction, price, is_add_on=False)
                            elif st["position"] != direction:
                                # Ein Gegensignal macht die urspruengliche These zunichte -
                                # die Position wird IMMER umgedreht, egal ob TP schon erreicht
                                # war oder nicht (unabhaengig vom Exit-Modus). Der Exit-Modus
                                # steuert nur noch, ob TP zusaetzlich als fruehzeitiger Ausstieg
                                # zaehlt (siehe on_price_update) - SL bleibt so oder so aktiv.
                                debug_log(f"🔄 [{symbol}] Pivot-SuperTrend Signal-Umkehr: {direction.upper()} @ {price} (Trend {last_trend}->{curr_trend})")
                                await execute_exit(symbol, price, "PPS-REVERSE")
                                price_after = st["last_price"] if st["last_price"] is not None else price
                                await execute_entry(symbol, direction, price_after, is_add_on=False)
                        last_trend = curr_trend
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Pivot-SuperTrend-Abfrage fehlgeschlagen", {"error": str(e)})

        await asyncio.sleep(5)


async def nsdt_channel_poll_loop(symbol):
    """NSDT Scalping Channel: obere Linie = WMA(High*upperspace, 9), untere Linie =
    WMA(Low*lowerspace, 9). Long wenn die untere Linie 2 Kerzen in Folge steigt, Short
    wenn die obere Linie 2 Kerzen in Folge faellt - reagiert auf die Richtungsaenderung
    der Linien selbst, nicht erst auf einen Durchbruch. TP/SL sind feste $-Betraege."""
    b = BOTS[symbol]
    last_processed_ts = None

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "nsdt_channel":
                length = cfg["nsdt_length"]
                needed_bars = min(1000, length * 4 + 10)
                resolution = cfg["nsdt_resolution"]

                if resolution in SUB_MINUTE_RESOLUTIONS:
                    local = get_seconds_candles(b["state"], SUB_MINUTE_RESOLUTIONS[resolution], needed_bars)
                    if local:
                        closed_ts, _, closed_h, closed_l, closed_c = local
                    else:
                        closed_ts = None
                else:
                    data = await fetch_candles_binance_multi(symbol, resolution, count_back=needed_bars)
                    if data:
                        timestamps, opens, highs, lows, closes = data
                        closed_ts = timestamps[:-1]
                        closed_h, closed_l, closed_c = highs[:-1], lows[:-1], closes[:-1]
                    else:
                        closed_ts = None

                if closed_ts and len(closed_c) > length + 3:
                    upper, lower = compute_nsdt_channel(closed_h, closed_l, length, cfg["nsdt_upperspace"], cfg["nsdt_lowerspace"])
                    if upper[-1] is not None and lower[-1] is not None and upper[-3] is not None and lower[-3] is not None:
                        st = b["state"]
                        st["nsdt_upper"] = round(upper[-1], 4)
                        st["nsdt_lower"] = round(lower[-1], 4)

                        signal_key = closed_ts[-1]
                        if last_processed_ts != signal_key:
                            last_processed_ts = signal_key
                            go_long = lower[-1] > lower[-2] > lower[-3]  # ta.rising(lower, 2)
                            go_short = upper[-1] < upper[-2] < upper[-3]  # ta.falling(upper, 2)

                            direction = None
                            if go_long and not go_short:
                                direction = "long"
                            elif go_short and not go_long:
                                direction = "short"

                            if direction and st["position"] is None and cfg["bot_active"]:
                                price = st["last_price"] if st["last_price"] is not None else closed_c[-1]
                                debug_log(f"📡 [{symbol}] NSDT-Channel Signal: {direction.upper()} @ {price} (Obere {round(upper[-1],4)} / Untere {round(lower[-1],4)})")
                                await execute_entry(symbol, direction, price, is_add_on=False)
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] NSDT-Channel-Abfrage fehlgeschlagen", {"error": str(e)})

        await asyncio.sleep(5)


def calc_obi(symbol, levels, depth_weighting=False, min_liquidity=0.0):
    """Berechnet das Orderbuch-Ungleichgewicht ueber die naechsten 'levels' Preisstufen.
    depth_weighting: gewichtet Level naeher am aktuellen Kurs staerker (1/(i+1)) statt
    jedes Level gleich zu zaehlen - Orders weit weg im Buch sind oft nur Deko.
    min_liquidity: ist die Gesamtliquiditaet (bid_vol+ask_vol) unter diesem Wert, wird
    0.0 zurueckgegeben statt einer verrauschten Prozentzahl - bei duennem Buch schlaegt
    schon eine kleine Order stark auf den Prozentwert durch, ohne dass das wirklich
    etwas bedeutet."""
    book = BOTS[symbol]["state"]["obi_book"]
    bids_sorted = sorted(book["bids"].items(), key=lambda x: float(x[0]), reverse=True)[:levels]
    asks_sorted = sorted(book["asks"].items(), key=lambda x: float(x[0]))[:levels]

    if depth_weighting:
        bid_vol = sum(v / (i + 1) for i, (_, v) in enumerate(bids_sorted))
        ask_vol = sum(v / (i + 1) for i, (_, v) in enumerate(asks_sorted))
        raw_bid_vol = sum(v for _, v in bids_sorted)
        raw_ask_vol = sum(v for _, v in asks_sorted)
    else:
        bid_vol = sum(v for _, v in bids_sorted)
        ask_vol = sum(v for _, v in asks_sorted)
        raw_bid_vol, raw_ask_vol = bid_vol, ask_vol

    if raw_bid_vol + raw_ask_vol < min_liquidity:
        return 0.0

    total = bid_vol + ask_vol
    return 0.0 if total == 0 else (bid_vol - ask_vol) / total


def update_obi_windows(symbol, raw_obi, fast_s, medium_s, slow_s, use_median=False):
    """Ein gemeinsamer Rohwert-Puffer, daraus werden alle drei Zeitfenster berechnet -
    effizienter als drei getrennte Puffer, und alle drei sehen exakt dieselben Rohdaten.
    use_median: Median statt Durchschnitt - ein einzelner Ausreisser-Tick (kurz
    aufblitzende Grossorder) kippt dann nicht mehr den ganzen Fensterwert."""
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
        if not vals:
            return 0.0
        if use_median:
            svals = sorted(vals)
            mid = len(svals) // 2
            return svals[mid] if len(svals) % 2 == 1 else (svals[mid - 1] + svals[mid]) / 2
        return sum(vals) / len(vals)

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

    raw_obi = calc_obi(symbol, cfg["obi_levels"],
                        depth_weighting=cfg.get("obi_depth_weighting_enabled", False),
                        min_liquidity=cfg.get("obi_min_liquidity", 0.0))
    fast, medium, slow = update_obi_windows(
        symbol, raw_obi,
        cfg["obi_window_fast_seconds"], cfg["obi_window_medium_seconds"], cfg["obi_window_slow_seconds"],
        use_median=cfg.get("obi_use_median", False),
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
    elif obi_mode == "reversal_instant":
        # Wie "reversal", aber OHNE auf eine bestaetigte Umkehr zu warten - sobald die
        # jeweilige Schwelle durchbrochen wird, wird sofort gehandelt. Getrennte
        # Long-/Short-Schwellen wie bei "reversal", nur ohne Rueckprall-Verzoegerung.
        # HYSTERESE: nach einem Trigger muss der Wert erst wieder deutlich unter die
        # Schwelle zurueckfallen (obi_instant_reset_ratio, Standard 50% der Schwelle),
        # bevor er erneut ausloesen darf - sonst wuerde jedes Zittern direkt an der
        # Schwelle (z.B. 0.48/0.52/0.49/0.51...) staendig neu feuern, ohne dass eine
        # echte neue Bewegung stattgefunden hat.
        long_th = cfg.get("obi_long_threshold", 0.20)
        short_th = cfg.get("obi_short_threshold", 0.30)
        reset_ratio = cfg.get("obi_instant_reset_ratio", 0.5)

        if st.get("obi_instant_armed_short") is None:
            st["obi_instant_armed_short"] = True
        if st.get("obi_instant_armed_long") is None:
            st["obi_instant_armed_long"] = True

        direction = None
        if fast >= short_th and st["obi_instant_armed_short"]:
            direction = "short"
            st["obi_instant_armed_short"] = False
        elif fast <= -long_th and st["obi_instant_armed_long"]:
            direction = "long"
            st["obi_instant_armed_long"] = False

        # Erst wenn der Wert deutlich zurueckgefallen ist, wird die jeweilige Seite
        # wieder "scharf" gemacht
        if fast < short_th * reset_ratio:
            st["obi_instant_armed_short"] = True
        if fast > -long_th * reset_ratio:
            st["obi_instant_armed_long"] = True

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

    # 1s-Mini-Kerzen aus dem Live-Tick bauen - laeuft fuer JEDEN Coin mit, dient als
    # Fallback fuer Sekunden-Zeitrahmen (10s/15s/30s) bei Coins, die es auf Binance nicht
    # gibt (HYPE, XAU, XAG, WTI, Forex-Paare, ...). Bei Binance-Coins wird bevorzugt der
    # echte binance_1s_buffer genutzt (siehe get_seconds_candles), das hier ist nur der
    # Rueckfall, damit wirklich JEDER Coin Sekunden-Zeitrahmen nutzen kann.
    now_epoch = time.time()
    bucket_start = int(now_epoch)
    if st["local_1s_bucket_start"] is None:
        st["local_1s_bucket_start"] = bucket_start
        st["local_1s_candle_open"] = price
        st["local_1s_candle_high"] = price
        st["local_1s_candle_low"] = price
        st["local_1s_candle_last"] = price
    elif bucket_start != st["local_1s_bucket_start"]:
        # Als Close den letzten Preis nehmen, der noch IN der alten Sekunde lag -
        # NICHT den neuen Tick, der schon zur naechsten Sekunde gehoert.
        buffer = st["local_1s_buffer"]
        buffer.append({
            "ts": st["local_1s_bucket_start"] * 1000, "o": st["local_1s_candle_open"],
            "h": st["local_1s_candle_high"], "l": st["local_1s_candle_low"],
            "c": st["local_1s_candle_last"],
        })
        if len(buffer) > 10000:  # ~2.75 Stunden (reduziert wegen Speicherlimit)
            buffer = buffer[-10000:]
        st["local_1s_buffer"] = buffer
        st["local_1s_bucket_start"] = bucket_start
        st["local_1s_candle_open"] = price
        st["local_1s_candle_high"] = price
        st["local_1s_candle_low"] = price
        st["local_1s_candle_last"] = price
    else:
        st["local_1s_candle_high"] = max(st["local_1s_candle_high"], price)
        st["local_1s_candle_low"] = min(st["local_1s_candle_low"], price)
        st["local_1s_candle_last"] = price

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
        if st["position"] is None:
            st["obi_breakeven_triggered"] = False
        else:
            entry = st["avg_entry_price"]
            breakeven_enabled = cfg.get("obi_breakeven_enabled", False)
            if cfg.get("obi_tp_sl_mode", "pct") == "usd":
                pnl_usd = (price - entry) * st["total_coin_size"] if st["position"] == "long" else (entry - price) * st["total_coin_size"]
                sl_floor = -cfg["obi_sl_usd"]
                if breakeven_enabled:
                    trigger_level = cfg["obi_tp_usd"] * cfg.get("obi_breakeven_trigger_ratio", 0.5)
                    if not st["obi_breakeven_triggered"] and pnl_usd >= trigger_level:
                        st["obi_breakeven_triggered"] = True
                    if st["obi_breakeven_triggered"]:
                        sl_floor = cfg.get("obi_breakeven_lock_usd", 0.1)
                if pnl_usd >= cfg["obi_tp_usd"]:
                    await execute_exit(symbol, price, "TP")
                elif pnl_usd <= sl_floor:
                    await execute_exit(symbol, price, "SL" if sl_floor < 0 else "BREAKEVEN-LOCK")
            else:
                pnl_pct = (price - entry) / entry * 100 if st["position"] == "long" else (entry - price) / entry * 100
                sl_floor = -cfg["obi_sl_pct"]
                if breakeven_enabled:
                    trigger_level = cfg["obi_tp_pct"] * cfg.get("obi_breakeven_trigger_ratio", 0.5)
                    if not st["obi_breakeven_triggered"] and pnl_pct >= trigger_level:
                        st["obi_breakeven_triggered"] = True
                    if st["obi_breakeven_triggered"]:
                        sl_floor = cfg.get("obi_breakeven_lock_pct", 0.1)
                if pnl_pct >= cfg["obi_tp_pct"]:
                    await execute_exit(symbol, price, "TP")
                elif pnl_pct <= sl_floor:
                    await execute_exit(symbol, price, "SL" if sl_floor < 0 else "BREAKEVEN-LOCK")
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

    if cfg["entry_mode"] == "stoch_cross":
        if st["position"] is not None:
            if cfg.get("stoch_cross_sl_tp_mode", "fixed") == "atr" and st.get("stoch_cross_sl_price") is not None:
                sl_price = st["stoch_cross_sl_price"]
                tp_price = st["stoch_cross_tp_price"]
                sl_hit = price <= sl_price if st["position"] == "long" else price >= sl_price
                tp_hit = price >= tp_price if st["position"] == "long" else price <= tp_price
                if sl_hit:
                    await execute_exit(symbol, price, "SL")
                    st["stoch_cross_sl_price"] = None
                    st["stoch_cross_tp_price"] = None
                elif tp_hit:
                    await execute_exit(symbol, price, "TP")
                    st["stoch_cross_sl_price"] = None
                    st["stoch_cross_tp_price"] = None
            else:
                entry = st["avg_entry_price"]
                pnl_usd = (price - entry) * st["total_coin_size"] if st["position"] == "long" else (entry - price) * st["total_coin_size"]
                if pnl_usd <= -cfg["stoch_cross_sl_usd"]:
                    await execute_exit(symbol, price, "SL")
                elif pnl_usd >= cfg["stoch_cross_tp_usd"]:
                    await execute_exit(symbol, price, "TP")
        return

    if cfg["entry_mode"] == "range_profile":
        if st["position"] is None:
            st["rp_breakeven_triggered"] = False
        else:
            entry = st["avg_entry_price"]
            pnl_usd = (price - entry) * st["total_coin_size"] if st["position"] == "long" else (entry - price) * st["total_coin_size"]
            sl_floor = -cfg["rp_sl_usd"]
            if cfg.get("rp_breakeven_enabled", False):
                if not st["rp_breakeven_triggered"] and pnl_usd >= cfg.get("rp_breakeven_trigger_usd", 3):
                    st["rp_breakeven_triggered"] = True
                if st["rp_breakeven_triggered"]:
                    sl_floor = cfg.get("rp_breakeven_lock_usd", 0.5)
            if pnl_usd <= sl_floor:
                await execute_exit(symbol, price, "SL" if sl_floor < 0 else "BREAKEVEN-LOCK")
            elif pnl_usd >= cfg["rp_tp_usd"]:
                await execute_exit(symbol, price, "TP")
        return

    if cfg["entry_mode"] == "pp_supertrend":
        if st["position"] is None:
            st["pps_breakeven_triggered"] = False
        else:
            entry = st["avg_entry_price"]
            pnl_usd = (price - entry) * st["total_coin_size"] if st["position"] == "long" else (entry - price) * st["total_coin_size"]
            sl_floor = -cfg["pps_sl_usd"]
            if cfg.get("pps_breakeven_enabled", False):
                if not st["pps_breakeven_triggered"] and pnl_usd >= cfg.get("pps_breakeven_trigger_usd", 2):
                    st["pps_breakeven_triggered"] = True
                if st["pps_breakeven_triggered"]:
                    sl_floor = cfg.get("pps_breakeven_lock_usd", 0.1)
            if pnl_usd <= sl_floor:
                await execute_exit(symbol, price, "SL" if sl_floor < 0 else "BREAKEVEN-LOCK")
            elif cfg.get("pps_exit_mode", "tp_sl") == "tp_sl" and pnl_usd >= cfg["pps_tp_usd"]:
                await execute_exit(symbol, price, "TP")
        return

    if cfg["entry_mode"] == "nsdt_channel":
        if st["position"] is not None:
            entry = st["avg_entry_price"]
            pnl_usd = (price - entry) * st["total_coin_size"] if st["position"] == "long" else (entry - price) * st["total_coin_size"]
            if pnl_usd <= -cfg["nsdt_sl_usd"]:
                await execute_exit(symbol, price, "SL")
            elif pnl_usd >= cfg["nsdt_tp_usd"]:
                await execute_exit(symbol, price, "TP")
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
                        active_symbols = [s for s in SYMBOLS if BOTS[s]["config"]["bot_active"]]
                        summary = {s: {"pos": BOTS[s]["state"]["position"] or "flach", "preis": BOTS[s]["state"]["last_price"],
                                       "trades": BOTS[s]["state"]["stats"]["trades"]} for s in active_symbols}
                        if summary:
                            debug_log("📊 Multi-Coin Status", summary)
        except Exception as e:
            debug_log("⚠️ Verbindung verloren, reconnect in 5s", {"error": str(e), "traceback": traceback.format_exc()})
            await asyncio.sleep(5)


# ========== WEB-DASHBOARD ==========


# ========== BACKTEST-ENGINE ==========
# Alle Simulationen nutzen dieselben Berechnungsfunktionen wie die Live-Strategien
# (compute_macd_histogram, compute_stochastic, compute_range_profile_snapshot, compute_atr,
# compute_fib_swing, build_fib_levels), damit Backtest und Live-Verhalten nicht auseinanderlaufen.
# WICHTIGE EINSCHRAENKUNG: SL/TP und Indikator-Exits werden pro Kerze am SCHLUSSKURS geprueft,
# nicht Tick-fuer-Tick wie live - ein kurzes Durchstechen von SL/TP innerhalb einer Kerze, das
# sich bis zum Kerzenschluss wieder erholt, wird also nicht erkannt. Fuer eine erste Einschaetzung
# der Strategie-Qualitaet reicht das aber aus.
# Lighter.xyz ist gebuehrenfrei - es werden daher keine Handelsgebuehren simuliert.

BACKTEST_MAX_CANDLES = {
    "stoch_cross": 100_000, "fib_reversal": 100_000, "range_profile": 30_000, "pp_supertrend": 100_000,
    "nsdt_channel": 100_000,
}


def _bt_close_trade(trades, direction, entry, exit_price, size, i, entry_i, reason):
    pnl = (exit_price - entry) * size if direction == "long" else (entry - exit_price) * size
    trades.append({"dir": direction, "entry": entry, "exit": exit_price, "reason": reason,
                    "pnl": pnl, "bars_held": i - entry_i})


def backtest_stoch_cross(candles, cfg):
    ts, o, h, l, c = candles
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]

    k_series, d_series = compute_stochastic(h, l, c, cfg["stoch_cross_k_period"], cfg["stoch_cross_k_smooth"], cfg["stoch_cross_d_period"])
    trend_ema = _ema_series(c, cfg["stoch_cross_trend_ema_period"]) if cfg.get("stoch_cross_trend_filter_enabled", False) else None
    atr_series = compute_atr(h, l, c, cfg["stoch_cross_atr_period"]) if cfg.get("stoch_cross_sl_tp_mode", "fixed") == "atr" else None

    use_rp = cfg.get("stoch_cross_rp_filter_enabled", False) or cfg.get("stoch_cross_require_squeeze", False)
    rp_lookback = cfg.get("stoch_cross_rp_lookback", 110)

    warmup = max(cfg["stoch_cross_k_period"] + cfg["stoch_cross_k_smooth"] + cfg["stoch_cross_d_period"], rp_lookback if use_rp else 0) + 5
    position = None
    trades = []
    last_k = None
    width_history = []
    squeeze_active = False

    for i in range(warmup, n):
        curr_k, price = k_series[i], c[i]

        if position is not None:
            direction, entry, size = position["dir"], position["entry"], position["size"]
            reason = None
            if position.get("sl_price") is not None:
                sl_hit = price <= position["sl_price"] if direction == "long" else price >= position["sl_price"]
                tp_hit = price >= position["tp_price"] if direction == "long" else price <= position["tp_price"]
                if sl_hit:
                    reason = "SL"
                elif tp_hit:
                    reason = "TP"
            else:
                pnl_usd = (price - entry) * size if direction == "long" else (entry - price) * size
                if pnl_usd <= -cfg["stoch_cross_sl_usd"]:
                    reason = "SL"
                elif pnl_usd >= cfg["stoch_cross_tp_usd"]:
                    reason = "TP"
            if reason:
                _bt_close_trade(trades, direction, entry, price, size, i, position["entry_i"], reason)
                position = None

        squeeze_before_entry = squeeze_active
        if use_rp and i >= rp_lookback:
            snap = compute_range_profile_snapshot(h[i - rp_lookback + 1:i + 1], l[i - rp_lookback + 1:i + 1],
                                                    c[i - rp_lookback + 1:i + 1], o[i - rp_lookback + 1:i + 1], rp_lookback, 50, 80.0)
            if snap:
                width = snap["range_high"] - snap["range_low"]
                avg_width = sum(width_history) / len(width_history) if len(width_history) >= 5 else None
                squeeze_active = avg_width is not None and width < avg_width * (cfg["stoch_cross_squeeze_threshold_pct"] / 100)
                width_history.append(width)
                if len(width_history) > cfg["stoch_cross_squeeze_lookback"]:
                    width_history = width_history[-cfg["stoch_cross_squeeze_lookback"]:]
            else:
                snap = None
        else:
            snap = None

        if position is None and last_k is not None:
            direction = None
            if last_k <= cfg["stoch_cross_oversold"] and curr_k > cfg["stoch_cross_oversold"]:
                direction = "long"
            elif last_k >= cfg["stoch_cross_overbought"] and curr_k < cfg["stoch_cross_overbought"]:
                direction = "short"

            if direction and trend_ema is not None:
                if direction == "long" and price <= trend_ema[i]:
                    direction = None
                elif direction == "short" and price >= trend_ema[i]:
                    direction = None

            if direction and cfg.get("stoch_cross_rp_filter_enabled", False) and snap:
                if direction == "long" and price >= snap["mid_price"]:
                    direction = None
                elif direction == "short" and price <= snap["mid_price"]:
                    direction = None

            if direction and cfg.get("stoch_cross_require_squeeze", False) and not squeeze_before_entry:
                direction = None

            if direction:
                size = (margin * leverage) / price
                position = {"dir": direction, "entry": price, "size": size, "entry_i": i,
                            "sl_price": None, "tp_price": None}
                if atr_series and atr_series[i]:
                    risk = atr_series[i] * cfg["stoch_cross_sl_atr_mult"]
                    reward = atr_series[i] * cfg["stoch_cross_tp_atr_mult"]
                    position["sl_price"] = price - risk if direction == "long" else price + risk
                    position["tp_price"] = price + reward if direction == "long" else price - reward

        last_k = curr_k

    return trades


def backtest_range_profile(candles, cfg):
    ts, o, h, l, c = candles
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    lookback = cfg["rp_lookback"]
    ob = cfg["rp_ob_os_level"]
    mode = cfg.get("rp_mode", "reversion")
    breakeven_enabled = cfg.get("rp_breakeven_enabled", False)
    breakeven_trigger = cfg.get("rp_breakeven_trigger_usd", 3)
    breakeven_lock = cfg.get("rp_breakeven_lock_usd", 0.5)

    warmup = lookback + 5
    position = None
    trades = []
    last_osc = None
    width_history = []
    squeeze_active = False
    breakeven_triggered = False

    for i in range(warmup, n):
        snap = compute_range_profile_snapshot(h[i - lookback + 1:i + 1], l[i - lookback + 1:i + 1],
                                                c[i - lookback + 1:i + 1], o[i - lookback + 1:i + 1], lookback, 50, ob)
        if snap is None:
            continue
        curr_osc, price = snap["osc"], c[i]

        if position is not None:
            direction, entry, size = position["dir"], position["entry"], position["size"]
            pnl_usd = (price - entry) * size if direction == "long" else (entry - price) * size
            sl_floor = -cfg["rp_sl_usd"]
            if breakeven_enabled:
                if not breakeven_triggered and pnl_usd >= breakeven_trigger:
                    breakeven_triggered = True
                if breakeven_triggered:
                    sl_floor = breakeven_lock
            if pnl_usd <= sl_floor:
                _bt_close_trade(trades, direction, entry, price, size, i, position["entry_i"], "SL" if sl_floor < 0 else "BREAKEVEN-LOCK")
                position = None
                breakeven_triggered = False
            elif pnl_usd >= cfg["rp_tp_usd"]:
                _bt_close_trade(trades, direction, entry, price, size, i, position["entry_i"], "TP")
                position = None
                breakeven_triggered = False

        squeeze_before_entry = squeeze_active
        channel_width = snap["range_high"] - snap["range_low"]
        avg_width = sum(width_history) / len(width_history) if len(width_history) >= 5 else None
        squeeze_active = avg_width is not None and channel_width < avg_width * (cfg["rp_squeeze_threshold_pct"] / 100)
        width_history.append(channel_width)
        if len(width_history) > cfg["rp_squeeze_lookback"]:
            width_history = width_history[-cfg["rp_squeeze_lookback"]:]

        if position is None and last_osc is not None:
            breakout_up = last_osc <= ob and curr_osc > ob
            breakout_down = last_osc >= -ob and curr_osc < -ob
            direction = None
            if breakout_up:
                direction = "short" if mode == "reversion" else "long"
            elif breakout_down:
                direction = "long" if mode == "reversion" else "short"

            if direction and cfg.get("rp_require_squeeze", False) and not squeeze_before_entry:
                direction = None

            if direction:
                size = (margin * leverage) / price
                position = {"dir": direction, "entry": price, "size": size, "entry_i": i}
                breakeven_triggered = False

        last_osc = curr_osc

    return trades


def backtest_fib_reversal(candles, cfg):
    ts, o, h, l, c = candles
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    lookback = cfg["fib_lookback_candles"]

    warmup = lookback + 5
    position = None  # {"dir","avg_entry","size","entry1_done","entry2_done","tp1_done","sl_active","fib","entry_i"}
    trades = []

    for i in range(warmup, n):
        price = c[i]

        if position is None:
            swing = compute_fib_swing(h[max(0, i - lookback + 1):i + 1], l[max(0, i - lookback + 1):i + 1], lookback)
            if swing is None:
                continue
            fib = build_fib_levels(swing, cfg)
            direction = fib["direction"]
            reached = price <= fib["entry1_price"] if direction == "long" else price >= fib["entry1_price"]
            if reached:
                size = (margin * leverage) / price
                position = {"dir": direction, "avg_entry": price, "size": size, "entry1_done": True,
                            "entry2_done": False, "tp1_done": False, "sl_active": fib["sl_price"],
                            "fib": fib, "entry_i": i}
            continue

        direction, fib = position["dir"], position["fib"]

        if not position["entry2_done"]:
            reached2 = price <= fib["entry2_price"] if direction == "long" else price >= fib["entry2_price"]
            if reached2:
                add_size = (margin * leverage) / price
                total_size = position["size"] + add_size
                position["avg_entry"] = (position["avg_entry"] * position["size"] + price * add_size) / total_size
                position["size"] = total_size
                position["entry2_done"] = True

        sl_hit = price <= position["sl_active"] if direction == "long" else price >= position["sl_active"]
        if sl_hit:
            _bt_close_trade(trades, direction, position["avg_entry"], price, position["size"], i, position["entry_i"], "SL")
            position = None
            continue

        if not position["tp1_done"]:
            tp1_hit = price >= fib["tp1_price"] if direction == "long" else price <= fib["tp1_price"]
            if tp1_hit:
                fraction = cfg["fib_tp1_close_pct"] / 100
                close_size = position["size"] * fraction
                _bt_close_trade(trades, direction, position["avg_entry"], price, close_size, i, position["entry_i"], "TP1")
                position["size"] -= close_size
                position["tp1_done"] = True
                position["sl_active"] = position["avg_entry"]
            continue

        tp2_hit = price >= fib["tp2_price"] if direction == "long" else price <= fib["tp2_price"]
        if tp2_hit:
            _bt_close_trade(trades, direction, position["avg_entry"], price, position["size"], i, position["entry_i"], "TP2")
            position = None

    return trades


def backtest_pp_supertrend(candles, cfg):
    ts, o, h, l, c = candles
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    prd = cfg["pps_period"]
    factor = cfg["pps_atr_factor"]
    atr_period = cfg["pps_atr_period"]
    tp_usd = cfg["pps_tp_usd"]
    sl_usd = cfg["pps_sl_usd"]
    breakeven_enabled = cfg.get("pps_breakeven_enabled", False)
    breakeven_trigger = cfg.get("pps_breakeven_trigger_usd", 2)
    breakeven_lock = cfg.get("pps_breakeven_lock_usd", 0.1)
    exit_mode = cfg.get("pps_exit_mode", "tp_sl")

    atr_series = compute_atr(h, l, c, atr_period)
    centers = compute_pivot_centers(h, l, prd)
    trend, _, _ = compute_pivot_supertrend_trend(c, atr_series, centers, factor)

    warmup = max(prd * 3, atr_period + 2)
    position = None
    trades = []
    last_trend = None
    breakeven_triggered = False

    for i in range(warmup, n):
        price = c[i]

        if position is not None:
            direction, entry, size = position["dir"], position["entry"], position["size"]
            pnl_usd = (price - entry) * size if direction == "long" else (entry - price) * size
            sl_floor = -sl_usd
            if breakeven_enabled:
                if not breakeven_triggered and pnl_usd >= breakeven_trigger:
                    breakeven_triggered = True
                if breakeven_triggered:
                    sl_floor = breakeven_lock
            if pnl_usd <= sl_floor:
                _bt_close_trade(trades, direction, entry, price, size, i, position["entry_i"], "SL" if sl_floor < 0 else "BREAKEVEN-LOCK")
                position = None
                breakeven_triggered = False
            elif exit_mode == "tp_sl" and pnl_usd >= tp_usd:
                _bt_close_trade(trades, direction, entry, price, size, i, position["entry_i"], "TP")
                position = None
                breakeven_triggered = False

        curr_trend = trend[i]
        if last_trend is not None and curr_trend != last_trend:
            direction = "long" if curr_trend == 1 else "short"
            if position is None:
                size = (margin * leverage) / price
                position = {"dir": direction, "entry": price, "size": size, "entry_i": i}
                breakeven_triggered = False
            elif position["dir"] != direction:
                _bt_close_trade(trades, position["dir"], position["entry"], price, position["size"], i, position["entry_i"], "PPS-REVERSE")
                size = (margin * leverage) / price
                position = {"dir": direction, "entry": price, "size": size, "entry_i": i}
                breakeven_triggered = False
        last_trend = curr_trend

    return trades


def backtest_nsdt_channel(candles, cfg):
    ts, o, h, l, c = candles
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    length = cfg["nsdt_length"]
    upperspace = cfg["nsdt_upperspace"]
    lowerspace = cfg["nsdt_lowerspace"]
    tp_usd = cfg["nsdt_tp_usd"]
    sl_usd = cfg["nsdt_sl_usd"]

    upper, lower = compute_nsdt_channel(h, l, length, upperspace, lowerspace)

    warmup = length + 3
    position = None
    trades = []

    for i in range(warmup, n):
        price = c[i]

        if position is not None:
            direction, entry, size = position["dir"], position["entry"], position["size"]
            pnl_usd = (price - entry) * size if direction == "long" else (entry - price) * size
            if pnl_usd <= -sl_usd:
                _bt_close_trade(trades, direction, entry, price, size, i, position["entry_i"], "SL")
                position = None
            elif pnl_usd >= tp_usd:
                _bt_close_trade(trades, direction, entry, price, size, i, position["entry_i"], "TP")
                position = None

        if upper[i] is None or lower[i] is None or upper[i - 2] is None or lower[i - 2] is None:
            continue

        go_long = lower[i] > lower[i - 1] > lower[i - 2]
        go_short = upper[i] < upper[i - 1] < upper[i - 2]
        direction = None
        if go_long and not go_short:
            direction = "long"
        elif go_short and not go_long:
            direction = "short"

        if direction and position is None:
            size = (margin * leverage) / price
            position = {"dir": direction, "entry": price, "size": size, "entry_i": i}

    return trades


BACKTEST_FUNCS = {
    "stoch_cross": backtest_stoch_cross,
    "range_profile": backtest_range_profile,
    "fib_reversal": backtest_fib_reversal,
    "pp_supertrend": backtest_pp_supertrend,
    "nsdt_channel": backtest_nsdt_channel,
}


def summarize_backtest_trades(trades):
    n = len(trades)
    if n == 0:
        return {"trades": 0, "win_rate_pct": 0, "total_pnl_usd": 0, "avg_win_usd": 0, "avg_loss_usd": 0,
                "max_drawdown_usd": 0, "avg_bars_held": 0}
    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    total_pnl = sum(t["pnl"] for t in trades)
    equity = peak = max_dd = 0.0
    for t in trades:
        equity += t["pnl"]
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
    return {
        "trades": n,
        "win_rate_pct": round(len(wins) / n * 100, 1),
        "total_pnl_usd": round(total_pnl, 2),
        "avg_win_usd": round(sum(t["pnl"] for t in wins) / len(wins), 2) if wins else 0,
        "avg_loss_usd": round(sum(t["pnl"] for t in losses) / len(losses), 2) if losses else 0,
        "max_drawdown_usd": round(max_dd, 2),
        "avg_bars_held": round(sum(t["bars_held"] for t in trades) / n, 1),
    }


from collections import OrderedDict

_backtest_candle_cache = OrderedDict()  # key: (symbol, resolution) -> {"fetched_at": float, "days": int, "candles": (...)}
BACKTEST_CACHE_TTL_SECONDS = 900  # 15 Minuten - fuer Backtest-Zwecke muss die Historie nicht
# sekundenaktuell sein, das erspart bei wiederholten Tests (z.B. nur SL geaendert) unnoetige
# Neuabrufe derselben Coin+Zeitrahmen-Kombination
BACKTEST_CACHE_MAX_ENTRIES = 5  # Hartes Limit: nur die 5 zuletzt genutzten Coin+Zeitrahmen-Kombinationen
# werden im Speicher gehalten (LRU) - sonst wuerde jede je getestete Kombination fuer immer im
# Arbeitsspeicher bleiben (bis zu ~500.000 Kerzen pro Eintrag = mehrere hundert MB) und den
# Render-Server irgendwann zum Absturz wegen Speicherueberlauf bringen.


def _backtest_cache_get(cache_key):
    entry = _backtest_candle_cache.get(cache_key)
    if entry is not None:
        _backtest_candle_cache.move_to_end(cache_key)  # als zuletzt genutzt markieren
    return entry


def _backtest_cache_set(cache_key, entry):
    _backtest_candle_cache[cache_key] = entry
    _backtest_candle_cache.move_to_end(cache_key)
    while len(_backtest_candle_cache) > BACKTEST_CACHE_MAX_ENTRIES:
        _backtest_candle_cache.popitem(last=False)  # aeltesten (am laengsten ungenutzten) Eintrag entfernen


def _trim_candles_to_days(candles, days, max_candles):
    ts, o, h, l, c = candles
    if not ts:
        return candles
    cutoff = ts[-1] - days * 24 * 60 * 60 * 1000
    idx = 0
    for i, t in enumerate(ts):
        if t >= cutoff:
            idx = i
            break
    ts, o, h, l, c = ts[idx:], o[idx:], h[idx:], l[idx:], c[idx:]
    if len(c) > max_candles:
        ts, o, h, l, c = ts[-max_candles:], o[-max_candles:], h[-max_candles:], l[-max_candles:], c[-max_candles:]
    return ts, o, h, l, c


async def run_backtest(symbol, entry_mode, cfg, days):
    if entry_mode not in BACKTEST_FUNCS:
        return {"error": f"Backtest für '{entry_mode}' nicht unterstützt (nur stoch_cross, range_profile, fib_reversal, pp_supertrend, nsdt_channel - Grid/OBI-Scalp brauchen historische Tick-/Orderbuchdaten, die es nicht gibt)."}

    resolution_key = {"stoch_cross": "stoch_cross_resolution",
                       "range_profile": "rp_resolution", "fib_reversal": "fib_resolution",
                       "pp_supertrend": "pps_resolution", "nsdt_channel": "nsdt_resolution"}[entry_mode]
    resolution = cfg.get(resolution_key, "1m")
    max_candles = BACKTEST_MAX_CANDLES[entry_mode]
    if resolution in SUB_MINUTE_RESOLUTIONS:
        # 10s/15s/30s-Kerzen kommen aus 1s-Basisdaten (10-30x mehr Rohdaten je Zeitraum) -
        # Obergrenze bewusst strenger, sonst waeren das bei laengeren Zeitraeumen zu viele
        # Binance-Anfragen.
        max_candles = min(max_candles, 5000)

    cache_key = (symbol, resolution)
    cached = _backtest_cache_get(cache_key)
    now = time.time()
    cache_used = False

    # WICHTIG: der Cache darf nur genutzt werden, wenn er mit einer mindestens genauso
    # hohen Kerzen-Obergrenze befuellt wurde wie die aktuelle Anfrage braucht - sonst
    # bekaeme z.B. MACD-Dual (500.000 Kerzen erlaubt) stillschweigend den kleineren,
    # von Range-Profile (nur 30.000 erlaubt) gecachten Datensatz serviert.
    if (cached and (now - cached["fetched_at"] < BACKTEST_CACHE_TTL_SECONDS)
            and cached["days"] >= days and cached.get("max_candles", 0) >= max_candles
            and len(cached["candles"][4]) >= 100):
        candles = _trim_candles_to_days(cached["candles"], days, max_candles)
        err = None
        cache_used = True
    else:
        candles, err = await fetch_historical_candles_binance(symbol, resolution, days, max_candles)
        if candles:
            _backtest_cache_set(cache_key, {"fetched_at": now, "days": days, "max_candles": max_candles, "candles": candles})

    if err:
        return {"error": err}
    if not candles or len(candles[4]) < 100:
        return {"error": "Zu wenig historische Kerzen für einen aussagekräftigen Backtest erhalten."}

    n_candles = len(candles[4])
    backtest_fn = BACKTEST_FUNCS[entry_mode]
    trades = backtest_fn(candles, cfg)
    stats = summarize_backtest_trades(trades)

    actual_days = (candles[0][-1] - candles[0][0]) / (24 * 60 * 60 * 1000)
    return {
        "symbol": symbol, "entry_mode": entry_mode, "resolution": resolution,
        "requested_days": days, "actual_days_covered": round(actual_days, 1),
        "candles_processed": n_candles, "candle_cap": max_candles, "cache_used": cache_used,
        "stats": stats, "trades": trades[-50:],  # letzte 50 fuers Dashboard, nicht alle
    }


async def run_backtest_sweep_pp_supertrend(symbol, base_cfg, days):
    """2D-Parameter-Sweep fuer Pivot-Point-SuperTrend: Pivot-Periode (ganzzahlig 1-10)
    x ATR-Faktor (0.5-5.0 in 0.1-Schritten, 46 Werte) = 460 Kombinationen. Zeitrahmen
    und ATR-Periode bleiben fix (wie in den gespeicherten Einstellungen). Effizient, weil
    die teure Pivot-Erkennung nur einmal pro Periode (10x) berechnet wird, nicht pro
    Kombination (460x) - der ATR-Faktor beeinflusst nur die guenstige Trend-Berechnung."""
    resolution = base_cfg.get("pps_resolution", "1m")
    max_candles = BACKTEST_MAX_CANDLES["pp_supertrend"]
    if resolution in SUB_MINUTE_RESOLUTIONS:
        max_candles = min(max_candles, 5000)

    cache_key = (symbol, resolution)
    cached = _backtest_cache_get(cache_key)
    now = time.time()

    if (cached and (now - cached["fetched_at"] < BACKTEST_CACHE_TTL_SECONDS)
            and cached["days"] >= days and cached.get("max_candles", 0) >= max_candles
            and len(cached["candles"][4]) >= 100):
        candles = _trim_candles_to_days(cached["candles"], days, max_candles)
    else:
        candles, err = await fetch_historical_candles_binance(symbol, resolution, days, max_candles)
        if err:
            return {"error": err}
        if candles:
            _backtest_cache_set(cache_key, {"fetched_at": now, "days": days, "max_candles": max_candles, "candles": candles})

    if not candles or len(candles[4]) < 100:
        return {"error": "Zu wenig historische Kerzen für einen aussagekräftigen Sweep erhalten."}

    ts, o, h, l, c = candles
    atr_period = base_cfg.get("pps_atr_period", 10)
    tp_usd = base_cfg.get("pps_tp_usd", 3)
    sl_usd = base_cfg.get("pps_sl_usd", 3)
    breakeven_enabled = base_cfg.get("pps_breakeven_enabled", False)
    breakeven_trigger = base_cfg.get("pps_breakeven_trigger_usd", 2)
    breakeven_lock = base_cfg.get("pps_breakeven_lock_usd", 0.1)
    exit_mode = base_cfg.get("pps_exit_mode", "tp_sl")
    margin, leverage = base_cfg["margin"], base_cfg["leverage"]
    atr_series = compute_atr(h, l, c, atr_period)

    periods = list(range(1, 11))  # 1..10
    factors = [round(0.5 + i * 0.1, 1) for i in range(46)]  # 0.5..5.0 in 0.1-Schritten

    results = []
    for prd in periods:
        centers = compute_pivot_centers(h, l, prd)  # teuer, nur 1x pro Periode
        warmup = max(prd * 3, atr_period + 2)
        for factor in factors:
            trend, _, _ = compute_pivot_supertrend_trend(c, atr_series, centers, factor)  # billig

            position = None
            trades = []
            last_trend = None
            breakeven_triggered = False
            for i in range(warmup, len(c)):
                price = c[i]
                if position is not None:
                    direction, entry, size = position["dir"], position["entry"], position["size"]
                    pnl_usd = (price - entry) * size if direction == "long" else (entry - price) * size
                    sl_floor = -sl_usd
                    if breakeven_enabled:
                        if not breakeven_triggered and pnl_usd >= breakeven_trigger:
                            breakeven_triggered = True
                        if breakeven_triggered:
                            sl_floor = breakeven_lock
                    if pnl_usd <= sl_floor:
                        _bt_close_trade(trades, direction, entry, price, size, i, position["entry_i"], "SL" if sl_floor < 0 else "BREAKEVEN-LOCK")
                        position = None
                        breakeven_triggered = False
                    elif exit_mode == "tp_sl" and pnl_usd >= tp_usd:
                        _bt_close_trade(trades, direction, entry, price, size, i, position["entry_i"], "TP")
                        position = None
                        breakeven_triggered = False
                curr_trend = trend[i]
                if last_trend is not None and curr_trend != last_trend:
                    direction = "long" if curr_trend == 1 else "short"
                    if position is None:
                        size = (margin * leverage) / price
                        position = {"dir": direction, "entry": price, "size": size, "entry_i": i}
                        breakeven_triggered = False
                    elif position["dir"] != direction:
                        _bt_close_trade(trades, position["dir"], position["entry"], price, position["size"], i, position["entry_i"], "PPS-REVERSE")
                        size = (margin * leverage) / price
                        position = {"dir": direction, "entry": price, "size": size, "entry_i": i}
                        breakeven_triggered = False
                last_trend = curr_trend

            stats = summarize_backtest_trades(trades)
            results.append({
                "label": f"Periode {prd}, ATR-Faktor {factor}",
                "period": prd, "atr_factor": factor,
                "stats": stats,
            })

    results = [r for r in results if r["stats"]["total_pnl_usd"] > 0]  # nur profitable Kombinationen
    results.sort(key=lambda r: r["stats"]["total_pnl_usd"], reverse=True)
    n_candles = len(c)
    actual_days = (ts[-1] - ts[0]) / (24 * 60 * 60 * 1000)
    return {
        "symbol": symbol, "entry_mode": "pp_supertrend", "resolution": resolution,
        "candles_processed": n_candles, "actual_days_covered": round(actual_days, 1),
        "combinations_tested": len(results),
        "results": results,
    }


async def run_backtest_sweep_pp_tpsl(symbol, base_cfg, days):
    """TP/SL-Sweep fuer Pivot-Point-SuperTrend: Periode und ATR-Faktor bleiben bei den
    gespeicherten Einstellungen fix, dafuer wird TP $1-20 x SL $1-20 komplett
    durchgetestet (400 Kombinationen). Getrennt vom Perioden/ATR-Sweep, weil eine
    Kombination aller vier Dimensionen (460 x 400 = 184.000) viel zu lange dauern wuerde -
    die teure Trend-Berechnung laeuft hier nur EINMAL, danach nur noch die guenstige
    SL/TP-Simulation pro Kombination."""
    resolution = base_cfg.get("pps_resolution", "1m")
    max_candles = BACKTEST_MAX_CANDLES["pp_supertrend"]
    if resolution in SUB_MINUTE_RESOLUTIONS:
        max_candles = min(max_candles, 5000)

    cache_key = (symbol, resolution)
    cached = _backtest_cache_get(cache_key)
    now = time.time()

    if (cached and (now - cached["fetched_at"] < BACKTEST_CACHE_TTL_SECONDS)
            and cached["days"] >= days and cached.get("max_candles", 0) >= max_candles
            and len(cached["candles"][4]) >= 100):
        candles = _trim_candles_to_days(cached["candles"], days, max_candles)
    else:
        candles, err = await fetch_historical_candles_binance(symbol, resolution, days, max_candles)
        if err:
            return {"error": err}
        if candles:
            _backtest_cache_set(cache_key, {"fetched_at": now, "days": days, "max_candles": max_candles, "candles": candles})

    if not candles or len(candles[4]) < 100:
        return {"error": "Zu wenig historische Kerzen für einen aussagekräftigen Sweep erhalten."}

    ts, o, h, l, c = candles
    prd = base_cfg.get("pps_period", 2)
    factor = base_cfg.get("pps_atr_factor", 3)
    atr_period = base_cfg.get("pps_atr_period", 10)
    breakeven_enabled = base_cfg.get("pps_breakeven_enabled", False)
    breakeven_trigger = base_cfg.get("pps_breakeven_trigger_usd", 2)
    breakeven_lock = base_cfg.get("pps_breakeven_lock_usd", 0.1)
    exit_mode = base_cfg.get("pps_exit_mode", "tp_sl")
    margin, leverage = base_cfg["margin"], base_cfg["leverage"]

    atr_series = compute_atr(h, l, c, atr_period)
    centers = compute_pivot_centers(h, l, prd)  # nur EINMAL, teuer
    trend, _, _ = compute_pivot_supertrend_trend(c, atr_series, centers, factor)  # nur EINMAL
    warmup = max(prd * 3, atr_period + 2)

    results = []
    for tp_usd in range(1, 21):
        for sl_usd in range(1, 21):
            position = None
            trades = []
            last_trend = None
            breakeven_triggered = False
            for i in range(warmup, len(c)):
                price = c[i]
                if position is not None:
                    direction, entry, size = position["dir"], position["entry"], position["size"]
                    pnl_usd = (price - entry) * size if direction == "long" else (entry - price) * size
                    sl_floor = -sl_usd
                    if breakeven_enabled:
                        if not breakeven_triggered and pnl_usd >= breakeven_trigger:
                            breakeven_triggered = True
                        if breakeven_triggered:
                            sl_floor = breakeven_lock
                    if pnl_usd <= sl_floor:
                        _bt_close_trade(trades, direction, entry, price, size, i, position["entry_i"], "SL" if sl_floor < 0 else "BREAKEVEN-LOCK")
                        position = None
                        breakeven_triggered = False
                    elif exit_mode == "tp_sl" and pnl_usd >= tp_usd:
                        _bt_close_trade(trades, direction, entry, price, size, i, position["entry_i"], "TP")
                        position = None
                        breakeven_triggered = False
                curr_trend = trend[i]
                if last_trend is not None and curr_trend != last_trend:
                    direction = "long" if curr_trend == 1 else "short"
                    if position is None:
                        size = (margin * leverage) / price
                        position = {"dir": direction, "entry": price, "size": size, "entry_i": i}
                        breakeven_triggered = False
                    elif position["dir"] != direction:
                        _bt_close_trade(trades, position["dir"], position["entry"], price, position["size"], i, position["entry_i"], "PPS-REVERSE")
                        size = (margin * leverage) / price
                        position = {"dir": direction, "entry": price, "size": size, "entry_i": i}
                        breakeven_triggered = False
                last_trend = curr_trend

            stats = summarize_backtest_trades(trades)
            results.append({
                "label": f"TP ${tp_usd} / SL ${sl_usd}",
                "tp_usd": tp_usd, "sl_usd": sl_usd,
                "stats": stats,
            })

    results = [r for r in results if r["stats"]["total_pnl_usd"] > 0]  # nur profitable Kombinationen
    results.sort(key=lambda r: r["stats"]["total_pnl_usd"], reverse=True)
    n_candles = len(c)
    actual_days = (ts[-1] - ts[0]) / (24 * 60 * 60 * 1000)
    return {
        "symbol": symbol, "entry_mode": "pp_supertrend", "resolution": resolution,
        "candles_processed": n_candles, "actual_days_covered": round(actual_days, 1),
        "combinations_tested": len(results),
        "results": results,
    }
