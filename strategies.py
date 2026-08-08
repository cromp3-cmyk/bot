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
import bisect
import math

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


async def fetch_candles_binance_vol(symbol, resolution, count_back=150):
    """Wie fetch_candles_binance, liefert zusaetzlich das Handelsvolumen pro Kerze -
    fuer Strategien wie BLSH-Composite, die Volumen brauchen (z.B. MFI). Bewusst
    eine eigene Funktion statt die bestehende zu erweitern, um nicht die vielen
    bestehenden Aufrufer (die ein 5er-Tupel erwarten) zu gefaehrden."""
    pair = BINANCE_SYMBOL_MAP.get(symbol)
    if not pair:
        return None
    try:
        url = f"https://api.binance.com/api/v3/klines?symbol={pair}&interval={resolution}&limit={min(count_back, 1000)}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    debug_log(f"⚠️ [{symbol}] Binance-Kerzenabfrage (mit Volumen) HTTP {resp.status}")
                    return None
                data = await resp.json()
    except Exception as e:
        debug_log(f"⚠️ [{symbol}] Binance-Kerzenabfrage (mit Volumen) fehlgeschlagen", {"error": str(e)})
        return None

    if not data or not isinstance(data, list):
        return None

    timestamps, opens, highs, lows, closes, volumes = [], [], [], [], [], []
    for k in data:
        timestamps.append(int(k[0]))
        opens.append(float(k[1]))
        highs.append(float(k[2]))
        lows.append(float(k[3]))
        closes.append(float(k[4]))
        volumes.append(float(k[5]))
    return timestamps, opens, highs, lows, closes, volumes


def resample_candles_vol(data, factor):
    """Wie resample_candles, aber fuer das 6er-Tupel (ts,o,h,l,c,v) - Volumen wird pro
    zusammengefasster Kerze aufsummiert."""
    timestamps, opens, highs, lows, closes, volumes = data
    n = (len(closes) // factor) * factor
    if n == 0:
        return [], [], [], [], [], []
    out_ts, out_o, out_h, out_l, out_c, out_v = [], [], [], [], [], []
    for i in range(0, n, factor):
        out_ts.append(timestamps[i])
        out_o.append(opens[i:i + factor][0])
        out_h.append(max(highs[i:i + factor]))
        out_l.append(min(lows[i:i + factor]))
        out_c.append(closes[i:i + factor][-1])
        out_v.append(sum(volumes[i:i + factor]))
    return out_ts, out_o, out_h, out_l, out_c, out_v


async def fetch_candles_binance_multi_vol(symbol, resolution, count_back=150):
    """Wie fetch_candles_binance_multi, aber mit Volumen (siehe fetch_candles_binance_vol).
    Unterstuetzt nur "2m" als synthetische Aufloesung (aus 1m) - die Sekunden-Zeitrahmen
    (10s/15s/30s/45s) brauchen den 1s-Puffer, der kein Volumen mitfuehrt."""
    if resolution == "2m":
        data = await fetch_candles_binance_vol(symbol, "1m", count_back=count_back * 2)
        if data is None:
            return None
        return resample_candles_vol(data, 2)
    return await fetch_candles_binance_vol(symbol, resolution, count_back=count_back)


async def fetch_historical_candles_binance_vol(symbol, resolution, days, max_candles):
    """Wie fetch_historical_candles_binance, aber mit Volumen fuer den BLSH-Backtest.
    Unterstuetzt nur "2m" als synthetische Aufloesung (aus 1m)."""
    pair = BINANCE_SYMBOL_MAP.get(symbol)
    if not pair:
        return None, "Coin nicht auf Binance verfügbar"

    base_resolution = "1m" if resolution == "2m" else resolution
    fetch_factor = 2 if resolution == "2m" else 1
    total_ms = days * 24 * 60 * 60 * 1000
    end_time = int(time.time() * 1000)
    start_time = end_time - total_ms
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
                await asyncio.sleep(0.25)
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
    volumes = [float(r[5]) for r in all_rows]

    if resolution == "2m":
        timestamps, opens, highs, lows, closes, volumes = resample_candles_vol((timestamps, opens, highs, lows, closes, volumes), 2)

    if len(closes) > max_candles:
        timestamps, opens, highs, lows, closes, volumes = (
            timestamps[-max_candles:], opens[-max_candles:], highs[-max_candles:],
            lows[-max_candles:], closes[-max_candles:], volumes[-max_candles:],
        )

    return (timestamps, opens, highs, lows, closes, volumes), None


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
                # Nur neue Kerzen anhaengen statt jedes Mal den kompletten Puffer zu
                # deduplizieren+sortieren (war bei vielen Coins gleichzeitig ein spuerbarer
                # Speicher-/CPU-Fresser). Binance liefert die Kerzen bereits aufsteigend
                # sortiert, daher reicht ein einfacher Vergleich mit dem letzten Timestamp.
                last_ts = buffer[-1]["ts"] if buffer else -1
                for i in range(len(timestamps)):
                    if timestamps[i] > last_ts:
                        buffer.append({"ts": timestamps[i], "o": opens[i], "h": highs[i], "l": lows[i], "c": closes[i]})
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


def _true_range_series(highs, lows, closes):
    n = len(closes)
    tr = [highs[0] - lows[0]] + [0.0] * (n - 1)
    for i in range(1, n):
        tr[i] = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
    return tr


def compute_choppiness_index(highs, lows, closes, length):
    """Portiert aus 'SuperTrend Fusion - ATP' (Pine v6): Choppiness Index nach Standardformel -
    100 * log10(Summe der wahren Handelsspannen der letzten 'length' Kerzen / (Hoechst-Hoch minus
    Tiefst-Tief im selben Fenster)) / log10(length).
    Werte nahe 100 = starke Seitwaerts-/Chop-Phase, Werte nahe 0 = klarer, starker Trend.
    Gibt None zurueck bei zu wenig Daten oder einer Preisspanne von 0 (z.B. komplett flacher Markt)."""
    n = len(closes)
    if n < length + 1 or length <= 1:
        return None
    tr = _true_range_series(highs, lows, closes)
    window_tr_sum = sum(tr[-length:])
    range_hl = max(highs[-length:]) - min(lows[-length:])
    if range_hl <= 0 or window_tr_sum <= 0:
        return None
    return 100 * math.log10(window_tr_sum / range_hl) / math.log10(length)


def compute_choppiness_series(highs, lows, closes, length):
    """Serienversion von compute_choppiness_index fuer den Backtest (vermeidet O(n^2) durch
    wiederholtes Neuberechnen mit wachsenden Listen)."""
    n = len(closes)
    tr = _true_range_series(highs, lows, closes)
    out = [None] * n
    for i in range(length - 1, n):
        start = i - length + 1
        window_tr_sum = sum(tr[start:i + 1])
        rng = max(highs[start:i + 1]) - min(lows[start:i + 1])
        out[i] = None if rng <= 0 or window_tr_sum <= 0 else 100 * math.log10(window_tr_sum / rng) / math.log10(length)
    return out


def compute_average_force(closes, highs, lows, length, smooth):
    """Portiert aus 'SuperTrend Fusion - ATP': Position des Schlusskurses innerhalb der
    Hoch-Tief-Spanne der letzten 'length' Kerzen (0 = am Tief, 1 = am Hoch, 0.5 = Mitte),
    zentriert um 0 (also -0.5..+0.5) und mit SMA('smooth') geglaettet. Positiv = bullisches
    Momentum, negativ = baerisches Momentum. Urspruenglich von racer8 veroeffentlicht."""
    n = len(closes)
    p = max(1, int(length))
    raw = [0.0] * n
    for i in range(n):
        start = max(0, i - p + 1)
        hh = max(highs[start:i + 1])
        ll = min(lows[start:i + 1])
        rng = hh - ll
        raw[i] = 0.0 if rng == 0 else (closes[i] - ll) / rng - 0.5
    sm = max(1, int(smooth))
    out = [0.0] * n
    for i in range(n):
        start = max(0, i - sm + 1)
        window = raw[start:i + 1]
        out[i] = sum(window) / len(window)
    return out


def compute_chandelier_exit(highs, lows, closes, atr_period, atr_mult, use_close=True):
    """Portiert aus 'MG signal [The_lurker]' - nur der Chandelier-Exit-Teil (Trailing-Stop mit
    Richtungswechsel), der dort tatsaechlich die Buy/Sell-Signale liefert; MagicTrend und
    Order-Blocks sind rein visuell und wurden bewusst weggelassen.
    Achtung Vorzeichen-Konvention (ANDERS als compute_supertrend!): direction 1 = bullisch/Long,
    -1 = baerisch/Short - das ist die Konvention des Original-Skripts.
    Gibt (direction, long_stop, short_stop) als Listen zurueck. 'length' wird im Original-Skript
    fuer ATR-Periode UND Hoechst-/Tiefstkurs-Fenster gleichzeitig verwendet - hier atr_period."""
    n = len(closes)
    atr = compute_atr(highs, lows, closes, atr_period)
    atr_ce = [atr_mult * a for a in atr]
    long_stop = [0.0] * n
    short_stop = [0.0] * n
    direction = [1] * n
    for i in range(n):
        start = max(0, i - atr_period + 1)
        if use_close:
            hh = max(closes[start:i + 1])
            ll = min(closes[start:i + 1])
        else:
            hh = max(highs[start:i + 1])
            ll = min(lows[start:i + 1])
        ls_raw = hh - atr_ce[i]
        ss_raw = ll + atr_ce[i]
        if i == 0:
            long_stop[i] = ls_raw
            short_stop[i] = ss_raw
            direction[i] = 1
            continue
        ls_prev = long_stop[i - 1]
        ss_prev = short_stop[i - 1]
        long_stop[i] = max(ls_raw, ls_prev) if closes[i - 1] > ls_prev else ls_raw
        short_stop[i] = min(ss_raw, ss_prev) if closes[i - 1] < ss_prev else ss_raw
        if closes[i] > ss_prev:
            direction[i] = 1
        elif closes[i] < ls_prev:
            direction[i] = -1
        else:
            direction[i] = direction[i - 1]
    return direction, long_stop, short_stop


def compute_supertrend(highs, lows, closes, atr_period, factor):
    """Standard-SuperTrend-Berechnung (wie Pine's ta.supertrend). Gibt (st_value, direction)
    zurueck - direction[-1] < 0 bedeutet Aufwaertstrend (aktiv), > 0 Abwaertstrend, passend zu
    TradingViews eigener Konvention."""
    n = len(closes)
    atr = compute_atr(highs, lows, closes, atr_period)
    hl2 = [(highs[i] + lows[i]) / 2 for i in range(n)]
    upper = [hl2[i] + factor * atr[i] for i in range(n)]
    lower = [hl2[i] - factor * atr[i] for i in range(n)]
    final_upper = [0.0] * n
    final_lower = [0.0] * n
    direction = [0] * n
    st_value = [0.0] * n
    final_upper[0] = upper[0]
    final_lower[0] = lower[0]
    direction[0] = -1
    st_value[0] = final_lower[0]
    for i in range(1, n):
        final_upper[i] = upper[i] if (upper[i] < final_upper[i - 1] or closes[i - 1] > final_upper[i - 1]) else final_upper[i - 1]
        final_lower[i] = lower[i] if (lower[i] > final_lower[i - 1] or closes[i - 1] < final_lower[i - 1]) else final_lower[i - 1]
        if direction[i - 1] == -1:
            direction[i] = -1 if closes[i] > final_lower[i] else 1
        else:
            direction[i] = 1 if closes[i] < final_upper[i] else -1
        st_value[i] = final_lower[i] if direction[i] == -1 else final_upper[i]
    return st_value, direction


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



def compute_zscore_trend(closes, lookback_period, ema_smooth):
    """Rolling Z-Score Trend (portiert aus dem Indikator 'Rolling Z-Score Trend [QuantAlgo]'):
    misst pro Kerze, wie viele Standardabweichungen der Schlusskurs vom gleitenden
    Durchschnitt der letzten lookback_period Kerzen entfernt ist (Z-Score), geglaettet
    mit einer kurzen EMA. Trend gilt als bullisch sobald der geglaettete Z-Score ueber 0
    liegt (Kurs ueber seinem juengsten Mittelwert), baerisch darunter - das Signal ist
    also der Nulllinien-Durchgang von smoothZ, wie im Original per alertcondition."""
    n = len(closes)
    if n < lookback_period + 1:
        return [0.0] * n
    z = [0.0] * n
    for i in range(lookback_period - 1, n):
        window = closes[i - lookback_period + 1:i + 1]
        mean = sum(window) / lookback_period
        variance = sum((x - mean) ** 2 for x in window) / lookback_period
        stddev = variance ** 0.5
        z[i] = (closes[i] - mean) / stddev if stddev > 0 else 0.0
    return _ema_series(z, ema_smooth)


def compute_rsi(closes, period):
    """Standard-RSI mit Wilder-Glaettung (wie Pine's ta.rsi)."""
    n = len(closes)
    if n < 2:
        return [50.0] * n
    gains = [0.0] * n
    losses = [0.0] * n
    for i in range(1, n):
        diff = closes[i] - closes[i - 1]
        gains[i] = diff if diff > 0 else 0.0
        losses[i] = -diff if diff < 0 else 0.0

    def _wilder_rma(values):
        out = [values[0]] * n
        for i in range(1, n):
            if i < period:
                out[i] = sum(values[:i + 1]) / (i + 1)
            else:
                out[i] = (out[i - 1] * (period - 1) + values[i]) / period
        return out

    avg_gain = _wilder_rma(gains)
    avg_loss = _wilder_rma(losses)
    rsi = [50.0] * n
    for i in range(n):
        if avg_loss[i] == 0:
            rsi[i] = 100.0 if avg_gain[i] > 0 else 50.0
        else:
            rs = avg_gain[i] / avg_loss[i]
            rsi[i] = 100 - (100 / (1 + rs))
    return rsi


def compute_mfi(highs, lows, closes, volumes, period):
    """Money Flow Index: RSI-artiger Oszillator auf Basis von volumengewichtetem
    typischem Preis (hlc3) statt reinem Schlusskurs - misst Geldfluss statt Preis."""
    n = len(closes)
    if n < 2:
        return [50.0] * n
    typical = [(highs[i] + lows[i] + closes[i]) / 3 for i in range(n)]
    raw_flow = [typical[i] * volumes[i] for i in range(n)]
    pos_flow = [0.0] * n
    neg_flow = [0.0] * n
    for i in range(1, n):
        if typical[i] > typical[i - 1]:
            pos_flow[i] = raw_flow[i]
        elif typical[i] < typical[i - 1]:
            neg_flow[i] = raw_flow[i]

    def _rolling_sum(values):
        out = [0.0] * n
        for i in range(n):
            start = max(0, i - period + 1)
            out[i] = sum(values[start:i + 1])
        return out

    pos_sum = _rolling_sum(pos_flow)
    neg_sum = _rolling_sum(neg_flow)
    mfi = [50.0] * n
    for i in range(n):
        if neg_sum[i] == 0:
            mfi[i] = 100.0 if pos_sum[i] > 0 else 50.0
        else:
            money_ratio = pos_sum[i] / neg_sum[i]
            mfi[i] = 100 - (100 / (1 + money_ratio))
    return mfi


def compute_macd_line_and_signal(closes, fast, slow, signal_period):
    """Rohe MACD-Linie und Signal-Linie (SMA-basiert, wie im BLSH-Original) - fuer den
    reinen Crossover-Modus (gruener/roter Punkt: MACD kreuzt seine Signal-Linie),
    unabhaengig von der Composite-Schwelle."""
    ema_f = _ema_series(closes, fast)
    ema_s = _ema_series(closes, slow)
    macd = [ema_f[i] - ema_s[i] for i in range(len(closes))]
    macd_signal = []
    for i in range(len(macd)):
        start = max(0, i - signal_period + 1)
        window = macd[start:i + 1]
        macd_signal.append(sum(window) / len(window))
    return macd, macd_signal


def compute_trend_meter_dots(closes, cfg):
    """Portiert aus dem TradingView-Indikator 'Trend Meter' (Lij_MC), reduziert auf die vom
    Nutzer gewuenschten 4 Signale (3 Punkte + obere Linie), Standard-Einstellungen des Original-
    Indikators als Vorgabe:
    - Punkt 1: schnelles MACD-Histogramm (Standard 8/21/5) > 0 -> gruen
    - Punkt 2: RSI (Standard-Periode 13) > 50 -> gruen
    - Punkt 3: RSI (Standard-Periode 5) > 50 -> gruen
    - Obere Linie: EMA-Crossover (Standard 5 > 11) -> gruen
    Gibt (dot1, dot2, dot3, line) als bool (True=gruen/False=rot) zurueck, oder None bei zu
    wenig Kerzen fuer die laengste eingestellte Periode."""
    n = len(closes)
    min_needed = max(cfg["tm_macd_slow"], cfg["tm_rsi1_period"], cfg["tm_rsi2_period"], cfg["tm_ma_slow"]) + 2
    if n < min_needed:
        return None
    macd, macd_signal = compute_macd_line_and_signal(closes, cfg["tm_macd_fast"], cfg["tm_macd_slow"], cfg["tm_macd_signal"])
    dot1 = (macd[-1] - macd_signal[-1]) > 0
    dot2 = compute_rsi(closes, cfg["tm_rsi1_period"])[-1] > 50
    dot3 = compute_rsi(closes, cfg["tm_rsi2_period"])[-1] > 50
    ma_fast = _ema_series(closes, cfg["tm_ma_fast"])[-1]
    ma_slow = _ema_series(closes, cfg["tm_ma_slow"])[-1]
    line = ma_fast > ma_slow
    return dot1, dot2, dot3, line


async def check_trend_meter_entry(symbol, dot1, dot2, dot3, line, price):
    """Long: alle 3 Punkte UND die Linie gruen -> sofort Long.
    Short: alle 3 Punkte UND die Linie rot -> sofort Short.
    Sonst (gemischt): kein Einstieg.
    Mit 'Invertiert'-Modus (tm_invert_direction) wurden dot1..line schon VOR dem Aufruf
    umgedreht - hier ist also kein weiterer Invertier-Schritt noetig.
    Nach einem SL-Exit greift optional eine Cooldown-Sperre (tm_sl_cooldown_seconds), damit
    der Bot nicht sofort wieder in dieselbe Lage reinlaeuft, aus der der SL ihn gerade
    rausgeholt hat."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or st["position"] is not None or price is None:
        return
    if cfg.get("tm_sl_enabled", False) and time.time() < st.get("tm_sl_cooldown_until", 0.0):
        return
    if dot1 and dot2 and dot3 and line:
        direction = "long"
    elif not dot1 and not dot2 and not dot3 and not line:
        direction = "short"
    else:
        return
    marks = "".join("🟢" if v else "🔴" for v in (dot1, dot2, dot3, line))
    debug_log(f"📡 [{symbol}] Trend-Meter Signal: {direction.upper()} @ {price} (Punkte+Linie {marks}{' [invertiert]' if cfg.get('tm_invert_direction', False) else ''})")
    await execute_entry(symbol, direction, price, is_add_on=False)


async def check_trend_meter_exit(symbol, dot1, dot2, dot3, line, price):
    """Zwei Exit-Modi (tm_exit_mode):
    - 'any_signal' (Standard): sobald auch nur einer der 4 (Punkte oder Linie) gegen die
      Position dreht -> Exit.
    - 'line_only': die 3 schnellen Punkte werden fuer den Exit komplett ignoriert - die
      Position laeuft weiter, solange die (traege) Linie noch in dieselbe Richtung zeigt.
      Erst wenn sich die Linie SELBST dreht, wird geschlossen. Laesst Trades laenger laufen,
      ohne dass kurzes Punkte-Rauschen den Trade vorzeitig beendet."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or st["position"] is None or price is None:
        return
    if cfg.get("tm_exit_mode", "any_signal") == "line_only":
        if st["position"] == "long" and not line:
            debug_log(f"🚪 [{symbol}] Trend-Meter Exit: LONG @ {price} (Linie hat gedreht, Punkte ignoriert)")
            await execute_exit(symbol, price, "TM-LINE-EXIT")
        elif st["position"] == "short" and line:
            debug_log(f"🚪 [{symbol}] Trend-Meter Exit: SHORT @ {price} (Linie hat gedreht, Punkte ignoriert)")
            await execute_exit(symbol, price, "TM-LINE-EXIT")
        return
    any_red = not (dot1 and dot2 and dot3 and line)
    any_green = dot1 or dot2 or dot3 or line
    if st["position"] == "long" and any_red:
        debug_log(f"🚪 [{symbol}] Trend-Meter Exit: LONG @ {price} (mind. 1 Punkt/Linie rot)")
        await execute_exit(symbol, price, "TM-SIGNAL-EXIT")
    elif st["position"] == "short" and any_green:
        debug_log(f"🚪 [{symbol}] Trend-Meter Exit: SHORT @ {price} (mind. 1 Punkt/Linie grün)")
        await execute_exit(symbol, price, "TM-SIGNAL-EXIT")


def _tm_resolve_invert(cfg, highs, lows, closes):
    """Entscheidet, ob die Trend-Meter-Auswertung gerade invertiert werden soll. Ist der
    Regime-Filter aktiv, entscheidet der Choppiness-Index (>= Schwelle = Seitwaerts -> invertiert,
    darunter = Trend -> normal) und ueberschreibt den manuellen Schalter. Sonst zaehlt einfach
    der manuelle 'tm_invert_direction'-Schalter. Gibt (invert: bool, chop_value: float|None) zurueck."""
    if cfg.get("tm_regime_filter_enabled", False):
        chop = compute_choppiness_index(highs, lows, closes, cfg.get("tm_regime_chop_length", 14))
        invert = chop is not None and chop >= cfg.get("tm_regime_chop_threshold", 50)
        return invert, chop
    return cfg.get("tm_invert_direction", False), None


async def trend_meter_poll_loop(symbol):
    """Eigene Strategie 'Trend-Meter' (portiert aus dem TradingView-Indikator 'Trend Meter'
    von Lij_MC): 3 Signal-Punkte (schnelles MACD-Histogramm, RSI13 vs. 50, RSI5 vs. 50) plus
    eine 'obere Linie' (EMA-Crossover) - siehe compute_trend_meter_dots.
    Kein SL/TP im Kerzenschluss-Poll-Loop noetig - beide (optional, fester $-Betrag) laufen
    tick-basiert geprueft, wie ueberall im Bot. Ein-/Ausstieg sind je EINZELN umschaltbar
    zwischen kerzenbasiert (nur hier im Poll-Loop bei echtem Kerzenschluss) und tick-basiert
    (reagiert sofort auf jeden Preis-Tick in on_price_update, indem die noch offene letzte
    Kerze live mit dem aktuellen Preis nachgerechnet wird - dafuer werden die Schlusskurse
    hier alle 5 Sek. fuer die Live-Auswertung gecacht).
    Optionaler Regime-Filter (siehe _tm_resolve_invert): schaltet automatisch zwischen normal
    und invertiert um, je nachdem ob der Choppiness-Index gerade Seitwaerts oder Trend anzeigt."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "trend_meter":
                resolution = cfg["tm_resolution"]
                min_needed = max(cfg["tm_macd_slow"], cfg["tm_rsi1_period"], cfg["tm_rsi2_period"], cfg["tm_ma_slow"],
                                  cfg.get("tm_regime_chop_length", 14)) + 2
                needed_bars = min(1000, max(min_needed * 3, 60))
                st = b["state"]

                if resolution in SUB_MINUTE_RESOLUTIONS:
                    local = get_seconds_candles(st, SUB_MINUTE_RESOLUTIONS[resolution], needed_bars)
                    if local:
                        closed_ts, _, closed_h, closed_l, closed_c = local
                    else:
                        closed_ts = None
                else:
                    data = await fetch_candles_binance_multi(symbol, resolution, count_back=needed_bars)
                    if data:
                        timestamps, opens, highs, lows, closes = data
                        closed_ts = timestamps[:-1]
                        closed_h = highs[:-1]
                        closed_l = lows[:-1]
                        closed_c = closes[:-1]
                    else:
                        closed_ts = None

                now = time.time()
                due_heartbeat = now - last_heartbeat > 300

                if closed_ts and len(closed_c) > min_needed:
                    signal_key = closed_ts[-1]
                    is_new_candle = last_processed_ts != signal_key
                    price = st["last_price"] if st["last_price"] is not None else closed_c[-1]

                    # Fuer die tick-basierte Live-Auswertung in on_price_update cachen
                    keep = min_needed + 5
                    st["tm_closes"] = closed_c[-keep:]
                    st["tm_highs"] = closed_h[-keep:]
                    st["tm_lows"] = closed_l[-keep:]

                    dots = compute_trend_meter_dots(closed_c, cfg)
                    if dots:
                        dot1, dot2, dot3, line = dots
                        st["tm_dot1"], st["tm_dot2"], st["tm_dot3"], st["tm_line"] = dot1, dot2, dot3, line
                        invert, chop_value = _tm_resolve_invert(cfg, closed_h, closed_l, closed_c)
                        st["tm_chop_value"] = chop_value

                        if due_heartbeat:
                            last_heartbeat = now
                            marks = "".join("🟢" if v else "🔴" for v in (dot1, dot2, dot3, line))
                            regime_info = f", Chop={round(chop_value,1) if chop_value is not None else '-'}, invertiert={invert}" if cfg.get("tm_regime_filter_enabled", False) else ""
                            debug_log(f"💓 [{symbol}] Trend-Meter aktiv: {marks}{regime_info}, Preis={closed_c[-1]}, Kerzen={len(closed_c)}, bot_active={cfg['bot_active']}")

                        if is_new_candle:
                            last_processed_ts = signal_key
                            e1, e2, e3, el = (not dot1, not dot2, not dot3, not line) if invert else (dot1, dot2, dot3, line)
                            if cfg.get("tm_entry_trigger", "candle_close") == "candle_close":
                                await check_trend_meter_entry(symbol, e1, e2, e3, el, price)
                            if cfg.get("tm_exit_trigger", "candle_close") == "candle_close":
                                await check_trend_meter_exit(symbol, e1, e2, e3, el, price)
                elif due_heartbeat:
                    last_heartbeat = now
                    if not closed_ts:
                        debug_log(f"⏳ [{symbol}] Trend-Meter wartet: keine Kerzen erhalten (Auflösung {resolution})")
                    else:
                        debug_log(f"⏳ [{symbol}] Trend-Meter wartet: zu wenig Kerzen ({len(closed_c)}/{min_needed + 1} nötig)")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Trend-Meter-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

        await asyncio.sleep(5)


async def check_stf_entry(symbol, direction, prev_direction, bull_ok, bear_ok, price):
    """Einstieg nur im Moment des Trend-Flips (nicht solange der Trend nur andauert), und nur
    wenn die aktiven Filter (Average Force, Choppiness) zustimmen."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or st["position"] is not None or price is None:
        return
    flip_to_up = direction == -1 and prev_direction != -1
    flip_to_down = direction == 1 and prev_direction != 1
    if flip_to_up and bull_ok:
        entry_direction = "long"
    elif flip_to_down and bear_ok:
        entry_direction = "short"
    else:
        return
    debug_log(f"📡 [{symbol}] SuperTrend Fusion Signal: {entry_direction.upper()} @ {price}")
    await execute_entry(symbol, entry_direction, price, is_add_on=False)


async def check_stf_exit(symbol, direction, price):
    """Ausstieg immer sobald der SuperTrend selbst dreht - unabhaengig von den Filtern."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or st["position"] is None or price is None:
        return
    if st["position"] == "long" and direction == 1:
        debug_log(f"🚪 [{symbol}] SuperTrend Fusion Exit: LONG @ {price} (Trend gedreht)")
        await execute_exit(symbol, price, "ST-FLIP-EXIT")
    elif st["position"] == "short" and direction == -1:
        debug_log(f"🚪 [{symbol}] SuperTrend Fusion Exit: SHORT @ {price} (Trend gedreht)")
        await execute_exit(symbol, price, "ST-FLIP-EXIT")


def compute_stf_state(highs, lows, closes, cfg):
    """Gibt bereits die EFFEKTIVEN Werte zurueck (nach Invertiert-Modus und EMA-Filter
    angewendet) - 'direction'/'prev_direction' sind also schon 'gedreht', wenn stf_invert_direction
    aktiv ist, und bull_ok/bear_ok beruecksichtigen bereits den optionalen EMA(200)-Trendfilter
    (nur Long ueber der EMA, nur Short darunter - unabhaengig vom Invertiert-Modus, gilt immer
    fuer die tatsaechlich einzugehende Richtung)."""
    n = len(closes)
    ema_len = cfg.get("stf_ema_length", 200) if cfg.get("stf_use_ema_filter", False) else 0
    min_needed = max(cfg["stf_atr_period"], cfg["stf_af_period"] + cfg["stf_af_smooth"], cfg["stf_chop_length"], ema_len) + 3
    if n < min_needed:
        return None
    st_val, direction = compute_supertrend(highs, lows, closes, cfg["stf_atr_period"], cfg["stf_factor"])
    bull_ok, bear_ok = True, True
    if cfg.get("stf_use_af_filter", True):
        af = compute_average_force(closes, highs, lows, cfg["stf_af_period"], cfg["stf_af_smooth"])
        bull_ok = bull_ok and af[-1] > 0
        bear_ok = bear_ok and af[-1] < 0
    chop_value = None
    if cfg.get("stf_use_chop_filter", True):
        chop_value = compute_choppiness_index(highs, lows, closes, cfg["stf_chop_length"])
        trending = chop_value is not None and chop_value < cfg.get("stf_chop_threshold", 50)
        bull_ok = bull_ok and trending
        bear_ok = bear_ok and trending

    invert = cfg.get("stf_invert_direction", False)
    eff_direction = -direction[-1] if invert else direction[-1]
    eff_prev_direction = -direction[-2] if invert else direction[-2]
    eff_bull_ok = bear_ok if invert else bull_ok
    eff_bear_ok = bull_ok if invert else bear_ok

    if cfg.get("stf_use_ema_filter", False):
        ema = _ema_series(closes, cfg.get("stf_ema_length", 200))
        above_ema = closes[-1] > ema[-1]
        eff_bull_ok = eff_bull_ok and above_ema
        eff_bear_ok = eff_bear_ok and not above_ema

    return {"direction": eff_direction, "prev_direction": eff_prev_direction, "bull_ok": eff_bull_ok, "bear_ok": eff_bear_ok, "chop_value": chop_value}


async def stf_poll_loop(symbol):
    """SuperTrend Fusion (portiert aus 'SuperTrend Fusion - ATP' von AlgoTrade_Pro; der Average-
    Force-Baustein stammt urspruenglich von racer8): SuperTrend-Basis (ATR-Baender) mit zwei
    optionalen Filtern - Average-Force-Momentum (muss in Trendrichtung zeigen) und Choppiness-
    Index (Markt muss gerade als 'trending' gelten, nicht seitwaerts).
    Einstieg nur im Moment des Trend-Flips, wenn die aktiven Filter zustimmen. Ausstieg immer
    sobald der SuperTrend selbst dreht, unabhaengig von den Filtern. Optional fester $-SL/TP.
    Ein-/Ausstieg je einzeln umschaltbar zwischen kerzenbasiert und tick-basiert (live
    nachgerechnete offene Kerze, wie bei Trend-Meter)."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "supertrend_fusion":
                resolution = cfg["stf_resolution"]
                ema_len = cfg.get("stf_ema_length", 200) if cfg.get("stf_use_ema_filter", False) else 0
                min_needed = max(cfg["stf_atr_period"], cfg["stf_af_period"] + cfg["stf_af_smooth"], cfg["stf_chop_length"], ema_len) + 3
                needed_bars = min(1000, max(min_needed * 3, 60))
                st = b["state"]

                if resolution in SUB_MINUTE_RESOLUTIONS:
                    local = get_seconds_candles(st, SUB_MINUTE_RESOLUTIONS[resolution], needed_bars)
                    if local:
                        closed_ts, _, closed_h, closed_l, closed_c = local
                    else:
                        closed_ts = None
                else:
                    data = await fetch_candles_binance_multi(symbol, resolution, count_back=needed_bars)
                    if data:
                        timestamps, opens, highs, lows, closes = data
                        closed_ts = timestamps[:-1]
                        closed_h = highs[:-1]
                        closed_l = lows[:-1]
                        closed_c = closes[:-1]
                    else:
                        closed_ts = None

                now = time.time()
                due_heartbeat = now - last_heartbeat > 300

                if closed_ts and len(closed_c) > min_needed:
                    signal_key = closed_ts[-1]
                    is_new_candle = last_processed_ts != signal_key
                    price = st["last_price"] if st["last_price"] is not None else closed_c[-1]

                    keep = min_needed + 5
                    st["stf_highs"] = closed_h[-keep:]
                    st["stf_lows"] = closed_l[-keep:]
                    st["stf_closes"] = closed_c[-keep:]

                    state = compute_stf_state(closed_h, closed_l, closed_c, cfg)
                    if state:
                        st["stf_direction"] = state["direction"]
                        st["stf_chop_value"] = state["chop_value"]

                        if due_heartbeat:
                            last_heartbeat = now
                            trend_label = "AUFWÄRTS" if state["direction"] == -1 else "ABWÄRTS"
                            debug_log(f"💓 [{symbol}] SuperTrend Fusion aktiv: Trend={trend_label}, Chop={state['chop_value']}, Preis={closed_c[-1]}, Kerzen={len(closed_c)}, bot_active={cfg['bot_active']}")

                        if is_new_candle:
                            last_processed_ts = signal_key
                            if cfg.get("stf_entry_trigger", "candle_close") == "candle_close":
                                await check_stf_entry(symbol, state["direction"], state["prev_direction"], state["bull_ok"], state["bear_ok"], price)
                            if cfg.get("stf_exit_trigger", "candle_close") == "candle_close":
                                await check_stf_exit(symbol, state["direction"], price)
                elif due_heartbeat:
                    last_heartbeat = now
                    if not closed_ts:
                        debug_log(f"⏳ [{symbol}] SuperTrend Fusion wartet: keine Kerzen erhalten (Auflösung {resolution})")
                    else:
                        debug_log(f"⏳ [{symbol}] SuperTrend Fusion wartet: zu wenig Kerzen ({len(closed_c)}/{min_needed + 1} nötig)")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] SuperTrend-Fusion-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

        await asyncio.sleep(5)


async def check_ce_entry(symbol, buy_signal, sell_signal, price):
    """Einstieg nur beim Buy/Sell-Flip. Ist der SuperTrend-Fusion-Richtungsfilter aktiv, muss
    dessen aktuelle Ausrichtung (auf dem HOEHEREN Zeitrahmen ce_stf_resolution) mit dem Signal
    uebereinstimmen. Stimmt sie noch nicht ueberein, wird das Signal als 'pending' gemerkt statt
    verworfen - siehe check_ce_pending: sobald SuperTrend umschwenkt UND das Chandelier-Signal
    zwischenzeitlich nicht wieder gedreht hat, wird die Order alsdann nachtraeglich platziert."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or st["position"] is not None or price is None:
        return
    if cfg.get("ce_sl_enabled", False) and time.time() < st.get("ce_sl_cooldown_until", 0.0):
        return
    if not (buy_signal or sell_signal):
        return
    flip_direction = "long" if buy_signal else "short"
    if not cfg.get("ce_stf_filter_enabled", False):
        st["ce_pending_direction"] = None
        debug_log(f"📡 [{symbol}] Chandelier Signal: {flip_direction.upper()} @ {price}")
        await execute_entry(symbol, flip_direction, price, is_add_on=False)
        return
    bias = st.get("ce_stf_bias")
    if bias == flip_direction:
        st["ce_pending_direction"] = None
        debug_log(f"📡 [{symbol}] Chandelier Signal: {flip_direction.upper()} @ {price} (SuperTrend-Filter bestätigt)")
        await execute_entry(symbol, flip_direction, price, is_add_on=False)
    else:
        st["ce_pending_direction"] = flip_direction
        debug_log(f"⏸️ [{symbol}] Chandelier Signal {flip_direction.upper()} wartet auf SuperTrend-Bestätigung (aktuell: {bias})")


async def check_ce_exit(symbol, buy_signal, sell_signal, price):
    """Ausstieg immer beim Gegen-Signal - unabhaengig vom SuperTrend-Filter (der filtert nur
    Einstiege, nie Ausstiege)."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or st["position"] is None or price is None:
        return
    if st["position"] == "long" and sell_signal:
        debug_log(f"🚪 [{symbol}] Chandelier Exit: LONG @ {price} (Sell-Signal)")
        await execute_exit(symbol, price, "CE-FLIP-EXIT")
    elif st["position"] == "short" and buy_signal:
        debug_log(f"🚪 [{symbol}] Chandelier Exit: SHORT @ {price} (Buy-Signal)")
        await execute_exit(symbol, price, "CE-FLIP-EXIT")


async def check_ce_pending(symbol, dir_now, price):
    """Prueft bei JEDEM Zyklus (nicht nur bei neuer Chandelier-Kerze), ob ein wartendes Signal
    jetzt durch den SuperTrend-Filter bestaetigt wird. Wird verworfen, sobald die Chandelier-
    Richtung zwischenzeitlich wieder gedreht hat (das urspruengliche 'buy'/'sell' ist dann nicht
    mehr aktiv)."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or st["position"] is not None or price is None:
        return
    if cfg.get("ce_sl_enabled", False) and time.time() < st.get("ce_sl_cooldown_until", 0.0):
        return
    pending = st.get("ce_pending_direction")
    if not pending or not cfg.get("ce_stf_filter_enabled", False):
        return
    still_valid = (pending == "long" and dir_now == 1) or (pending == "short" and dir_now == -1)
    if not still_valid:
        st["ce_pending_direction"] = None
        return
    bias = st.get("ce_stf_bias")
    if bias == pending:
        st["ce_pending_direction"] = None
        debug_log(f"📡 [{symbol}] Chandelier Pending-Order ausgelöst: {pending.upper()} @ {price} (SuperTrend jetzt bestätigt)")
        await execute_entry(symbol, pending, price, is_add_on=False)


async def ce_poll_loop(symbol):
    """Chandelier Exit (portiert aus 'MG signal [The_lurker]' - nur der Buy/Sell-Signal-Teil).
    Optionaler SuperTrend-Fusion-Richtungsfilter auf einem HOEHEREN Zeitrahmen (ce_stf_resolution):
    SuperTrend bullisch -> nur Longs, baerisch -> nur Shorts. Kommt ein Signal, waehrend
    SuperTrend noch dagegen steht, wird es als 'pending' gemerkt und nachtraeglich ausgefuehrt,
    sobald SuperTrend umschwenkt - vorausgesetzt das Chandelier-Signal ist bis dahin noch aktiv
    (siehe check_ce_pending). Optional fester $-Take-Profit (kein SL vorgesehen). Ein-/Ausstieg
    je einzeln umschaltbar zwischen kerzenbasiert und tick-basiert."""
    b = BOTS[symbol]
    last_ce_ts = None
    last_heartbeat = 0.0

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "chandelier_exit":
                st = b["state"]
                resolution = cfg["ce_resolution"]
                atr_period = cfg["ce_atr_period"]
                min_needed = atr_period + 3
                needed_bars = min(1000, max(min_needed * 3, 60))

                if resolution in SUB_MINUTE_RESOLUTIONS:
                    local = get_seconds_candles(st, SUB_MINUTE_RESOLUTIONS[resolution], needed_bars)
                    if local:
                        closed_ts, _, closed_h, closed_l, closed_c = local
                    else:
                        closed_ts = None
                else:
                    data = await fetch_candles_binance_multi(symbol, resolution, count_back=needed_bars)
                    if data:
                        timestamps, opens, highs, lows, closes = data
                        closed_ts, closed_h, closed_l, closed_c = timestamps[:-1], highs[:-1], lows[:-1], closes[:-1]
                    else:
                        closed_ts = None

                stf_filter_enabled = cfg.get("ce_stf_filter_enabled", False)
                if stf_filter_enabled:
                    stf_resolution = cfg.get("ce_stf_resolution", "5m")
                    stf_ema_len = cfg.get("stf_ema_length", 200) if cfg.get("stf_use_ema_filter", False) else 0
                    stf_min_needed = max(cfg["stf_atr_period"], cfg["stf_af_period"] + cfg["stf_af_smooth"], cfg["stf_chop_length"], stf_ema_len) + 3
                    stf_needed_bars = min(1000, max(stf_min_needed * 3, 60))
                    if stf_resolution in SUB_MINUTE_RESOLUTIONS:
                        stf_local = get_seconds_candles(st, SUB_MINUTE_RESOLUTIONS[stf_resolution], stf_needed_bars)
                        if stf_local:
                            stf_ts, _, stf_h, stf_l, stf_c = stf_local
                        else:
                            stf_ts = None
                    else:
                        stf_data = await fetch_candles_binance_multi(symbol, stf_resolution, count_back=stf_needed_bars)
                        if stf_data:
                            s_ts, s_o, s_h, s_l, s_c = stf_data
                            stf_ts, stf_h, stf_l, stf_c = s_ts[:-1], s_h[:-1], s_l[:-1], s_c[:-1]
                        else:
                            stf_ts = None
                    if stf_ts and len(stf_c) > stf_min_needed:
                        stf_state = compute_stf_state(stf_h, stf_l, stf_c, cfg)
                        if stf_state:
                            st["ce_stf_bias"] = "long" if stf_state["direction"] == -1 else "short"
                    # Falls (noch) keine STF-Daten da sind, bleibt ce_stf_bias auf dem letzten
                    # bekannten Wert stehen (oder None ganz am Anfang) - Entries bleiben dann
                    # so lange 'pending', bis eine Bestaetigung vorliegt.
                else:
                    st["ce_stf_bias"] = None

                now = time.time()
                due_heartbeat = now - last_heartbeat > 300

                if closed_ts and len(closed_c) > min_needed:
                    signal_key = closed_ts[-1]
                    is_new_candle = last_ce_ts != signal_key
                    price = st["last_price"] if st["last_price"] is not None else closed_c[-1]

                    keep = min_needed + 5
                    st["ce_highs"] = closed_h[-keep:]
                    st["ce_lows"] = closed_l[-keep:]
                    st["ce_closes"] = closed_c[-keep:]

                    direction, long_stop, short_stop = compute_chandelier_exit(
                        closed_h, closed_l, closed_c, atr_period, cfg["ce_atr_mult"], cfg.get("ce_use_close", True))
                    invert = cfg.get("ce_invert_direction", False)
                    dir_now = -direction[-1] if invert else direction[-1]
                    dir_prev = -direction[-2] if invert else direction[-2]
                    st["ce_direction"] = dir_now

                    if due_heartbeat:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] Chandelier Exit aktiv: dir={'LONG' if dir_now==1 else 'SHORT'}, "
                                  f"STF-Filter={'an' if stf_filter_enabled else 'aus'}, STF-Bias={st.get('ce_stf_bias')}, "
                                  f"Pending={st.get('ce_pending_direction')}, Preis={closed_c[-1]}, bot_active={cfg['bot_active']}")

                    if is_new_candle:
                        last_ce_ts = signal_key
                        buy_signal = dir_now == 1 and dir_prev == -1
                        sell_signal = dir_now == -1 and dir_prev == 1
                        if cfg.get("ce_exit_trigger", "candle_close") == "candle_close":
                            await check_ce_exit(symbol, buy_signal, sell_signal, price)
                        if cfg.get("ce_entry_trigger", "candle_close") == "candle_close":
                            await check_ce_entry(symbol, buy_signal, sell_signal, price)

                    # Pending-Order jeden Zyklus pruefen, nicht nur bei neuer Chandelier-Kerze -
                    # ein STF-Wechsel soll zeitnah greifen, nicht erst bei der naechsten Kerze.
                    await check_ce_pending(symbol, dir_now, price)
                elif due_heartbeat:
                    last_heartbeat = now
                    if not closed_ts:
                        debug_log(f"⏳ [{symbol}] Chandelier Exit wartet: keine Kerzen erhalten (Auflösung {resolution})")
                    else:
                        debug_log(f"⏳ [{symbol}] Chandelier Exit wartet: zu wenig Kerzen ({len(closed_c)}/{min_needed + 1} nötig)")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Chandelier-Exit-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

        await asyncio.sleep(5)


def compute_blsh_composite(highs, lows, closes, volumes, atr_period, rsi_period,
                            ema_fast, ema_slow, macd_fast, macd_slow, macd_signal_period, mfi_period):
    """Portiert aus dem Indikator 'Buy Low Sell High Composite': kombiniert vier auf
    -1..+1 normierte Komponenten (EMA-Differenz als Trendrichtung, RSI, MACD-Histogramm,
    MFI als volumenbasiertes Pendant zum RSI) zu einem einzigen Oszillator. Die
    Trend-/Momentum-Komponenten (EMA-Diff, MACD-Histogramm) werden relativ zur aktuellen
    Volatilitaet normiert (2x ATR), RSI/MFI relativ zu ihrer ueblichen 25-75-Spanne.
    compositeNormalized > 0 = bullisch (gruen im Original), < 0 = baerisch (rot)."""
    n = len(closes)

    def _normalize(value, lo, hi):
        rng = hi - lo
        if rng == 0:
            rng = 0.0001
        return -1 + ((value - lo) / rng) * 2

    atr_series = compute_atr(highs, lows, closes, atr_period)
    price_range = [2 * a for a in atr_series]

    rsi_series = compute_rsi(closes, rsi_period)
    ema_f = _ema_series(closes, ema_fast)
    ema_s = _ema_series(closes, ema_slow)
    ema_diff = [ema_f[i] - ema_s[i] for i in range(n)]

    macd_f = _ema_series(closes, macd_fast)
    macd_s = _ema_series(closes, macd_slow)
    macd = [macd_f[i] - macd_s[i] for i in range(n)]
    # macdSignal ist im Original ein SMA(macd, 9), keine EMA
    macd_signal = []
    for i in range(n):
        start = max(0, i - macd_signal_period + 1)
        window = macd[start:i + 1]
        macd_signal.append(sum(window) / len(window))
    macd_hist = [macd[i] - macd_signal[i] for i in range(n)]

    mfi_series = compute_mfi(highs, lows, closes, volumes, mfi_period)

    composite = [0.0] * n
    for i in range(n):
        rsi_norm = _normalize(rsi_series[i], 25, 75)
        ema_diff_norm = _normalize(ema_diff[i], -price_range[i], price_range[i])
        macd_hist_norm = _normalize(macd_hist[i], -price_range[i], price_range[i])
        mfi_norm = _normalize(mfi_series[i], 25, 75)
        composite_value = ema_diff_norm + rsi_norm + macd_hist_norm + mfi_norm
        composite[i] = _normalize(composite_value, -4, 4)
    return composite


async def zscore_trend_poll_loop(symbol):
    """Eigene Strategie 'Z-Score-Trend': eine Schwelle X (Long bei +X, Short bei -X,
    siehe compute_zscore_trend fuer die Z-Score-Berechnung). Ausstieg (falls TP noch
    nicht erreicht): Rueckkreuzung der Nulllinie. TP1 (fester $-Betrag) schliesst einen
    Teil der Position und setzt den SL auf Einstieg + Lock-Betrag (im Plus). TP2 (fester
    $-Betrag) schliesst den Rest final.

    Dieser Loop holt nur alle 5 Sek. die historischen (abgeschlossenen) Kerzen und
    cached die Fensterdaten (letzte Schlusskurse + zuletzt bestaetigter geglaetteter
    Z-Score als EMA-Ausgangswert). Die eigentliche Signal-Erkennung (Einstieg UND
    Nulllinien-Exit) laeuft tick-basiert in on_price_update, damit kein Kerzenschluss
    abgewartet werden muss."""
    b = BOTS[symbol]
    last_heartbeat = 0.0

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "zscore_trend":
                lookback = cfg["zscore_lookback_period"]
                ema_smooth = cfg["zscore_ema_smooth"]
                threshold = cfg["zscore_threshold"]
                needed_bars = min(1000, max(lookback * 4, 60) + 5)
                resolution = cfg["zscore_resolution"]

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
                        closed_c = closes[:-1]
                    else:
                        closed_ts = None

                now = time.time()
                due_heartbeat = now - last_heartbeat > 300

                if closed_ts and len(closed_c) > lookback + 1:
                    smooth_z = compute_zscore_trend(closed_c, lookback, ema_smooth)
                    st = b["state"]
                    curr_z = smooth_z[-1]

                    # Cache fuer die tick-basierte Live-Auswertung in on_price_update
                    st["zscore_window_closes"] = closed_c[-(lookback - 1):]
                    st["zscore_ema_seed"] = curr_z

                    st["zscore_value"] = round(curr_z, 3)
                    st["zscore_history"].append({"ts": int(time.time() * 1000), "z": round(curr_z, 3)})
                    if len(st["zscore_history"]) > 300:
                        st["zscore_history"].pop(0)

                    if due_heartbeat:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] Z-Score-Trend aktiv: Z={curr_z:.2f} (Schwelle ±{threshold}), Preis={closed_c[-1]}, Kerzen={len(closed_c)}, bot_active={cfg['bot_active']}")
                elif due_heartbeat:
                    last_heartbeat = now
                    if not closed_ts:
                        debug_log(f"⏳ [{symbol}] Z-Score-Trend wartet: keine Kerzen erhalten (Auflösung {resolution})")
                    else:
                        debug_log(f"⏳ [{symbol}] Z-Score-Trend wartet: zu wenig Kerzen ({len(closed_c)}/{lookback + 2} nötig)")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Z-Score-Trend-Abfrage fehlgeschlagen", {"error": str(e)})

        await asyncio.sleep(5)



async def blsh_trend_poll_loop(symbol):
    """Eigene Strategie 'BLSH-Composite' (portiert aus 'Buy Low Sell High Composite'),
    zwei waehlbare Signal-Modi:
    - "composite" (Standard): kombiniert RSI, EMA-Differenz, MACD-Histogramm und MFI zu
      einem einzigen, auf -1..+1 normierten Oszillator (siehe compute_blsh_composite).
      Long bei Kreuzung ueber +Schwelle, Short bei Kreuzung unter -Schwelle, Ausstieg
      (falls TP nicht erreicht) bei Rueckkreuzung der Nulllinie. TP1/TP2/SL/Cooldown
      laufen tick-basiert in on_price_update.
    - "macd_cross": reiner Wechsel-Modus wie im Original-Indikator (gruener/roter Punkt
      = MACD kreuzt seine eigene Signal-Linie) - kein Threshold, kein TP/SL, immer im
      Markt: bei jeder Kreuzung wird sofort umgedreht (Position geschlossen + Gegen-
      position eroeffnet), gleiches Verhalten wie frueher bei Pivot-SuperTrend.
    Die Signal-Erkennung ist in beiden Modi bewusst KERZENBASIERT (wartet auf
    Kerzenschluss), nicht tick-live - die Composite kombiniert RSI/MACD/MFI (inkl.
    Volumen), das liesse sich nicht sinnvoll wie ein einzelner EMA-Schritt pro Tick
    fortschreiben."""
    b = BOTS[symbol]
    last_processed_ts = None
    prev_composite = None
    prev_macd_diff = None
    last_heartbeat = 0.0

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "blsh_trend":
                signal_mode = cfg.get("blsh_signal_mode", "composite")
                resolution = cfg["blsh_resolution"]
                atr_period = cfg["blsh_atr_period"]
                rsi_period = cfg["blsh_rsi_period"]
                ema_fast = cfg["blsh_ema_fast"]
                ema_slow = cfg["blsh_ema_slow"]
                macd_fast = cfg["blsh_macd_fast"]
                macd_slow = cfg["blsh_macd_slow"]
                macd_signal_period = cfg["blsh_macd_signal"]
                mfi_period = cfg["blsh_mfi_period"]
                threshold = cfg["blsh_threshold"]
                min_needed = max(ema_slow, macd_slow, atr_period, rsi_period, mfi_period) + 5
                needed_bars = min(1000, max(min_needed * 3, 60))

                data = await fetch_candles_binance_multi_vol(symbol, resolution, count_back=needed_bars)
                if data:
                    timestamps, opens, highs, lows, closes, volumes = data
                    closed_ts = timestamps[:-1]
                    closed_h, closed_l, closed_c, closed_v = highs[:-1], lows[:-1], closes[:-1], volumes[:-1]
                else:
                    closed_ts = None

                now = time.time()
                due_heartbeat = now - last_heartbeat > 300

                if closed_ts and len(closed_c) > min_needed:
                    st = b["state"]
                    signal_key = closed_ts[-1]
                    is_new_candle = last_processed_ts != signal_key
                    if is_new_candle:
                        last_processed_ts = signal_key
                    price = st["last_price"] if st["last_price"] is not None else closed_c[-1]

                    if signal_mode == "macd_cross":
                        macd, macd_signal = compute_macd_line_and_signal(closed_c, macd_fast, macd_slow, macd_signal_period)
                        curr_diff = macd[-1] - macd_signal[-1]
                        st["blsh_value"] = round(curr_diff, 5)
                        st["blsh_history"].append({"ts": int(time.time() * 1000), "v": round(curr_diff, 5)})
                        if len(st["blsh_history"]) > 300:
                            st["blsh_history"].pop(0)

                        if due_heartbeat:
                            last_heartbeat = now
                            debug_log(f"💓 [{symbol}] BLSH-MACD-Cross aktiv: MACD-Signal-Diff={curr_diff:.4f}, Preis={closed_c[-1]}, Kerzen={len(closed_c)}, bot_active={cfg['bot_active']}")

                        bullish_cross = is_new_candle and prev_macd_diff is not None and prev_macd_diff <= 0 and curr_diff > 0
                        bearish_cross = is_new_candle and prev_macd_diff is not None and prev_macd_diff >= 0 and curr_diff < 0

                        dir_mode = cfg.get("blsh_direction_mode", "both")
                        allow_entry_long = dir_mode != "short_only"
                        allow_entry_short = dir_mode != "long_only"
                        direction = "long" if bullish_cross else ("short" if bearish_cross else None)

                        if direction and cfg["bot_active"]:
                            if st["position"] is not None and st["position"] != direction:
                                debug_log(f"🔄 [{symbol}] BLSH-MACD-Cross Wechsel: {direction.upper()} @ {price} (gruener/roter Punkt)")
                                await execute_exit(symbol, price, "BLSH-MACD-REVERSE")
                            if st["position"] is None and ((direction == "long" and allow_entry_long) or (direction == "short" and allow_entry_short)):
                                st["blsh_trend"] = 1 if direction == "long" else -1
                                debug_log(f"📡 [{symbol}] BLSH-MACD-Cross Signal: {direction.upper()} @ {price}")
                                price_after = st["last_price"] if st["last_price"] is not None else price
                                await execute_entry(symbol, direction, price_after, is_add_on=False)

                        if is_new_candle:
                            prev_macd_diff = curr_diff

                    else:
                        composite = compute_blsh_composite(closed_h, closed_l, closed_c, closed_v,
                                                            atr_period, rsi_period, ema_fast, ema_slow,
                                                            macd_fast, macd_slow, macd_signal_period, mfi_period)
                        curr = composite[-1]
                        st["blsh_value"] = round(curr, 3)
                        st["blsh_history"].append({"ts": int(time.time() * 1000), "v": round(curr, 3)})
                        if len(st["blsh_history"]) > 300:
                            st["blsh_history"].pop(0)

                        if due_heartbeat:
                            last_heartbeat = now
                            debug_log(f"💓 [{symbol}] BLSH-Composite aktiv: V={curr:.2f} (Schwelle ±{threshold}), Preis={closed_c[-1]}, Kerzen={len(closed_c)}, bot_active={cfg['bot_active']}")

                        long_entry = is_new_candle and prev_composite is not None and prev_composite <= threshold and curr > threshold
                        short_entry = is_new_candle and prev_composite is not None and prev_composite >= -threshold and curr < -threshold
                        long_exit = is_new_candle and prev_composite is not None and prev_composite >= 0 and curr < 0
                        short_exit = is_new_candle and prev_composite is not None and prev_composite <= 0 and curr > 0

                        dir_mode = cfg.get("blsh_direction_mode", "both")
                        if dir_mode == "long_only":
                            short_entry = False
                        elif dir_mode == "short_only":
                            long_entry = False

                        if cfg["bot_active"] and st["position"] is not None:
                            if (st["position"] == "long" and long_exit) or (st["position"] == "short" and short_exit):
                                debug_log(f"🚪 [{symbol}] BLSH-Composite Nulllinien-Exit: {st['position'].upper()} @ {price} (V {curr:.2f})")
                                await execute_exit(symbol, price, "BLSH-ZERO-EXIT")
                                st["blsh_last_exit_ts"] = time.time()

                        cooldown = cfg.get("blsh_cooldown_seconds", 0)
                        since_last_exit = time.time() - st.get("blsh_last_exit_ts", 0)
                        in_cooldown = cooldown > 0 and since_last_exit < cooldown

                        direction = "long" if long_entry else ("short" if short_entry else None)
                        if direction and cfg["bot_active"] and st["position"] is None and not in_cooldown:
                            st["blsh_trend"] = 1 if direction == "long" else -1
                            debug_log(f"📡 [{symbol}] BLSH-Composite Signal: {direction.upper()} @ {price} (V {curr:.2f})")
                            price_after = st["last_price"] if st["last_price"] is not None else price
                            await execute_entry(symbol, direction, price_after, is_add_on=False)

                        if is_new_candle:
                            prev_composite = curr
                elif due_heartbeat:
                    last_heartbeat = now
                    if not closed_ts:
                        debug_log(f"⏳ [{symbol}] BLSH-Composite wartet: keine Kerzen erhalten (Auflösung {resolution})")
                    else:
                        debug_log(f"⏳ [{symbol}] BLSH-Composite wartet: zu wenig Kerzen ({len(closed_c)}/{min_needed + 1} nötig)")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] BLSH-Composite-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

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


def calc_spread_pct(symbol):
    """Bid/Ask-Spread der besten Preisstufe in Prozent vom Mid-Preis. Ein ungewoehnlich
    weiter Spread zeigt ein duennes/chaotisches Buch an - genau dort liefert OBI laut
    Microstructure-Forschung die unzuverlaessigsten Signale (hoher Spread korreliert mit
    hoeheren Handelskosten und weniger belastbarem Orderbuch-Signal). Gibt None zurueck,
    wenn keine Seite des Buchs Daten hat."""
    book = BOTS[symbol]["state"]["obi_book"]
    if not book["bids"] or not book["asks"]:
        return None
    best_bid = max(float(p) for p in book["bids"].keys())
    best_ask = min(float(p) for p in book["asks"].keys())
    mid = (best_bid + best_ask) / 2
    if mid <= 0:
        return None
    return (best_ask - best_bid) / mid * 100


def calc_recent_vol_pct(symbol, window_seconds):
    """Kurzfristige Volatilitaet als Hoch-Tief-Spanne der letzten 'window_seconds' Sekunden
    Tick-Preise, in Prozent vom Durchschnittspreis im Fenster. Nutzt den bereits mitgefuehrten
    price_history-Puffer (keine zusaetzliche Datenquelle noetig). Dient als grober
    Volatilitaets-Regime-Filter: zu ruhig = Seitwaerts-Rauschen ohne Fortsetzung, zu wild =
    Spike-/Wick-Risiko. Gibt None zurueck, wenn zu wenige Ticks im Fenster liegen."""
    st = BOTS[symbol]["state"]
    now_ms = int(time.time() * 1000)
    cutoff = now_ms - int(window_seconds * 1000)
    prices = [p["price"] for p in st["price_history"] if p["ts"] >= cutoff]
    if len(prices) < 3:
        return None
    avg = sum(prices) / len(prices)
    if avg <= 0:
        return None
    return (max(prices) - min(prices)) / avg * 100


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

    st["obi_spread_pct"] = calc_spread_pct(symbol)
    st["obi_recent_vol_pct"] = calc_recent_vol_pct(symbol, cfg.get("obi_vol_window_seconds", 30))

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

    # Optionaler Spread-Filter: verwirft Signale bei ungewoehnlich weitem Bid/Ask-Spread
    # (duennes/chaotisches Buch, OBI dort am unzuverlaessigsten)
    if cfg.get("obi_spread_filter_enabled", False):
        spread = st.get("obi_spread_pct")
        if spread is None or spread > cfg.get("obi_max_spread_pct", 0.05):
            return

    # Optionaler Volatilitaets-Regime-Filter: verwirft Signale ausserhalb des Normalbands
    # (zu ruhig = Rauschen ohne Fortsetzung, zu wild = Spike-/Wick-Risiko)
    if cfg.get("obi_vol_filter_enabled", False):
        vol = st.get("obi_recent_vol_pct")
        if vol is None or vol < cfg.get("obi_vol_min_pct", 0.0) or vol > cfg.get("obi_vol_max_pct", 1.0):
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

    if cfg["entry_mode"] == "zscore_trend":
        # Live-Tick-basierte Signal-Erkennung: kein Warten auf Kerzenschluss mehr, weder
        # fuer Einstieg noch fuer den Nulllinien-Exit. Die Fensterdaten (Schlusskurse der
        # letzten abgeschlossenen Kerzen + der zuletzt bestaetigte geglaettete Z-Score als
        # EMA-Ausgangswert) werden alle 5 Sek. im Poll-Loop aktualisiert; hier wird bei
        # JEDEM Preis-Tick ein neuer Z-Score mit dem aktuellen Live-Preis als juengstem
        # Punkt berechnet (ein einzelner EMA-Schritt ab dem gecachten Ausgangswert) -
        # exakt wie TradingViews live nachgezeichnete, noch nicht abgeschlossene Kerze.
        # WICHTIG: eigenes try/except, damit ein Fehler hier nicht die komplette
        # WebSocket-Verbindung (und damit ALLE Coins/Strategien) abreisst.
        try:
            window = st.get("zscore_window_closes")
            seed = st.get("zscore_ema_seed")
            lookback = cfg.get("zscore_lookback_period")
            ema_smooth = cfg.get("zscore_ema_smooth")
            if window and seed is not None and lookback and len(window) >= lookback - 1:
                full_window = window[-(lookback - 1):] + [price]
                mean = sum(full_window) / lookback
                variance = sum((x - mean) ** 2 for x in full_window) / lookback
                stddev = variance ** 0.5
                raw_z = (price - mean) / stddev if stddev > 0 else 0.0
                k = 2 / (ema_smooth + 1)
                live_z = raw_z * k + seed * (1 - k)
                st["zscore_value"] = round(live_z, 3)

                threshold = cfg["zscore_threshold"]
                prev_live_z = st.get("zscore_live_prev_z")
                long_entry = prev_live_z is not None and prev_live_z <= threshold and live_z > threshold
                short_entry = prev_live_z is not None and prev_live_z >= -threshold and live_z < -threshold
                long_exit = prev_live_z is not None and prev_live_z >= 0 and live_z < 0
                short_exit = prev_live_z is not None and prev_live_z <= 0 and live_z > 0
                st["zscore_live_prev_z"] = live_z

                dir_mode = cfg.get("zscore_direction_mode", "both")
                if dir_mode == "long_only":
                    short_entry = False
                elif dir_mode == "short_only":
                    long_entry = False

                # Exit zuerst: Nulllinien-Rueckkreuzung schliesst sofort, ohne umzudrehen
                if cfg["bot_active"] and st["position"] is not None:
                    if (st["position"] == "long" and long_exit) or (st["position"] == "short" and short_exit):
                        debug_log(f"🚪 [{symbol}] Z-Score-Trend Nulllinien-Exit (live): {st['position'].upper()} @ {price} (Z {live_z:.2f})")
                        await execute_exit(symbol, price, "ZSCORE-ZERO-EXIT")
                        st["zscore_last_exit_ts"] = time.time()

                cooldown = cfg.get("zscore_cooldown_seconds", 0)
                since_last_exit = time.time() - st.get("zscore_last_exit_ts", 0)
                in_cooldown = cooldown > 0 and since_last_exit < cooldown

                direction = "long" if long_entry else ("short" if short_entry else None)
                if direction and cfg["bot_active"] and st["position"] is None:
                    if in_cooldown:
                        pass  # Cooldown nach dem letzten Exit noch aktiv - Signal wird uebersprungen
                    else:
                        st["zscore_trend"] = 1 if direction == "long" else -1
                        debug_log(f"📡 [{symbol}] Z-Score-Trend Signal (live): {direction.upper()} @ {price} (Z {live_z:.2f})")
                        await execute_entry(symbol, direction, price, is_add_on=False)
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Z-Score-Trend Live-Tick-Auswertung fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

        if st["position"] is None:
            st["zscore_tp1_done"] = False
        else:
            entry = st["avg_entry_price"]
            pnl_usd = (price - entry) * st["total_coin_size"] if st["position"] == "long" else (entry - price) * st["total_coin_size"]
            if not st["zscore_tp1_done"]:
                # Vor TP1: normaler fester SL (abschaltbar)
                if cfg.get("zscore_sl_enabled", True) and pnl_usd <= -cfg["zscore_sl_usd"]:
                    await execute_exit(symbol, price, "SL")
                    st["zscore_last_exit_ts"] = time.time()
                elif pnl_usd >= cfg["zscore_tp1_usd"]:
                    if cfg.get("zscore_tp2_enabled", True):
                        fraction = cfg["zscore_tp1_close_pct"] / 100
                        ok = await execute_partial_exit(symbol, price, fraction, "TP1")
                        if ok:
                            st["zscore_tp1_done"] = True
                            debug_log(f"📡 [{symbol}] Z-Score-Trend TP1 erreicht" + (f" - SL auf Einstieg+{cfg['zscore_breakeven_lock_usd']} gesetzt" if cfg.get("zscore_breakeven_enabled", True) else " - kein Break-Even-Stop aktiv, laeuft bis TP2/Gegensignal"))
                    else:
                        # TP2 aus: TP1 ist der finale, vollstaendige Ausstieg
                        await execute_exit(symbol, price, "TP1")
                        st["zscore_last_exit_ts"] = time.time()
            else:
                # Nach TP1: SL liegt jetzt im Plus (Einstieg + Lock-Betrag, abschaltbar), TP2 ist das finale Ziel (abschaltbar)
                if cfg.get("zscore_breakeven_enabled", True) and pnl_usd <= cfg["zscore_breakeven_lock_usd"]:
                    await execute_exit(symbol, price, "BREAKEVEN-LOCK")
                    st["zscore_last_exit_ts"] = time.time()
                elif cfg.get("zscore_tp2_enabled", True) and pnl_usd >= cfg["zscore_tp2_usd"]:
                    await execute_exit(symbol, price, "TP2")
                    st["zscore_last_exit_ts"] = time.time()
        return

    if cfg["entry_mode"] == "blsh_trend":
        if cfg.get("blsh_signal_mode", "composite") == "macd_cross":
            # Reiner Wechsel-Modus: kein TP/SL, Ein-/Ausstieg laeuft komplett
            # kerzenbasiert im Poll-Loop (siehe blsh_trend_poll_loop)
            return
        # Nulllinien-Exit/Cooldown-Logik ist bereits kerzenbasiert im Poll-Loop erledigt
        # (siehe blsh_trend_poll_loop) - hier nur TP1/TP2/SL, tick-basiert wie ueberall.
        if st["position"] is None:
            st["blsh_tp1_done"] = False
        else:
            entry = st["avg_entry_price"]
            pnl_usd = (price - entry) * st["total_coin_size"] if st["position"] == "long" else (entry - price) * st["total_coin_size"]
            if not st["blsh_tp1_done"]:
                if cfg.get("blsh_sl_enabled", True) and pnl_usd <= -cfg["blsh_sl_usd"]:
                    await execute_exit(symbol, price, "SL")
                    st["blsh_last_exit_ts"] = time.time()
                elif pnl_usd >= cfg["blsh_tp1_usd"]:
                    if cfg.get("blsh_tp2_enabled", True):
                        fraction = cfg["blsh_tp1_close_pct"] / 100
                        ok = await execute_partial_exit(symbol, price, fraction, "TP1")
                        if ok:
                            st["blsh_tp1_done"] = True
                            debug_log(f"📡 [{symbol}] BLSH-Composite TP1 erreicht" + (f" - SL auf Einstieg+{cfg['blsh_breakeven_lock_usd']} gesetzt" if cfg.get("blsh_breakeven_enabled", True) else " - kein Break-Even-Stop aktiv, laeuft bis TP2/Gegensignal"))
                    else:
                        await execute_exit(symbol, price, "TP1")
                        st["blsh_last_exit_ts"] = time.time()
            else:
                if cfg.get("blsh_breakeven_enabled", True) and pnl_usd <= cfg["blsh_breakeven_lock_usd"]:
                    await execute_exit(symbol, price, "BREAKEVEN-LOCK")
                    st["blsh_last_exit_ts"] = time.time()
                elif cfg.get("blsh_tp2_enabled", True) and pnl_usd >= cfg["blsh_tp2_usd"]:
                    await execute_exit(symbol, price, "TP2")
                    st["blsh_last_exit_ts"] = time.time()
        return

    if cfg["entry_mode"] == "trend_meter":
        # Kerzenbasierter Teil (Ein-/Ausstieg bei echtem Kerzenschluss) laeuft im
        # trend_meter_poll_loop. Hier nur: tick-basierte Live-Auswertung (falls fuer
        # Einstieg und/oder Ausstieg aktiviert) sowie der optionale $-Take-Profit
        # (immer tick-basiert, wie ueberall im Bot). SL/TP: beide optional, fester $-Betrag.
        entry_trigger = cfg.get("tm_entry_trigger", "candle_close")
        exit_trigger = cfg.get("tm_exit_trigger", "candle_close")
        if entry_trigger == "tick" or exit_trigger == "tick":
            try:
                cached_closes = st.get("tm_closes")
                cached_highs = st.get("tm_highs")
                cached_lows = st.get("tm_lows")
                if cached_closes and len(cached_closes) >= 2:
                    live_closes = cached_closes[:-1] + [price]
                    live_highs = (cached_highs[:-1] + [max(cached_highs[-1], price)]) if cached_highs else live_closes
                    live_lows = (cached_lows[:-1] + [min(cached_lows[-1], price)]) if cached_lows else live_closes
                    dots = compute_trend_meter_dots(live_closes, cfg)
                    if dots:
                        dot1, dot2, dot3, line = dots
                        invert, _ = _tm_resolve_invert(cfg, live_highs, live_lows, live_closes)
                        if invert:
                            dot1, dot2, dot3, line = not dot1, not dot2, not dot3, not line
                        if entry_trigger == "tick":
                            await check_trend_meter_entry(symbol, dot1, dot2, dot3, line, price)
                        if exit_trigger == "tick":
                            await check_trend_meter_exit(symbol, dot1, dot2, dot3, line, price)
            except Exception as e:
                debug_log(f"⚠️ [{symbol}] Trend-Meter Live-Tick-Auswertung fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

        if st["position"] is not None and (cfg.get("tm_tp_enabled", False) or cfg.get("tm_sl_enabled", False)):
            entry = st["avg_entry_price"]
            pnl_usd = (price - entry) * st["total_coin_size"] if st["position"] == "long" else (entry - price) * st["total_coin_size"]
            if cfg.get("tm_sl_enabled", False) and pnl_usd <= -abs(cfg.get("tm_sl_usd", 3)):
                await execute_exit(symbol, price, "SL")
                st["tm_sl_cooldown_until"] = time.time() + cfg.get("tm_sl_cooldown_seconds", 30)
            elif cfg.get("tm_tp_enabled", False) and pnl_usd >= abs(cfg.get("tm_tp_usd", 3)):
                await execute_exit(symbol, price, "TP")
        return

    if cfg["entry_mode"] == "supertrend_fusion":
        entry_trigger = cfg.get("stf_entry_trigger", "candle_close")
        exit_trigger = cfg.get("stf_exit_trigger", "candle_close")
        if entry_trigger == "tick" or exit_trigger == "tick":
            try:
                ch, cl, cc = st.get("stf_highs"), st.get("stf_lows"), st.get("stf_closes")
                if ch and cl and cc and len(cc) >= 2:
                    live_h = ch[:-1] + [max(ch[-1], price)]
                    live_l = cl[:-1] + [min(cl[-1], price)]
                    live_c = cc[:-1] + [price]
                    state = compute_stf_state(live_h, live_l, live_c, cfg)
                    if state:
                        if entry_trigger == "tick":
                            await check_stf_entry(symbol, state["direction"], state["prev_direction"], state["bull_ok"], state["bear_ok"], price)
                        if exit_trigger == "tick":
                            await check_stf_exit(symbol, state["direction"], price)
            except Exception as e:
                debug_log(f"⚠️ [{symbol}] SuperTrend Fusion Live-Tick-Auswertung fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

        if st["position"] is not None and (cfg.get("stf_tp_enabled", False) or cfg.get("stf_sl_enabled", False)):
            entry = st["avg_entry_price"]
            pnl_usd = (price - entry) * st["total_coin_size"] if st["position"] == "long" else (entry - price) * st["total_coin_size"]
            if cfg.get("stf_sl_enabled", False) and pnl_usd <= -abs(cfg.get("stf_sl_usd", 3)):
                await execute_exit(symbol, price, "SL")
            elif cfg.get("stf_tp_enabled", False) and pnl_usd >= abs(cfg.get("stf_tp_usd", 3)):
                await execute_exit(symbol, price, "TP")
        return

    if cfg["entry_mode"] == "chandelier_exit":
        entry_trigger = cfg.get("ce_entry_trigger", "candle_close")
        exit_trigger = cfg.get("ce_exit_trigger", "candle_close")
        if entry_trigger == "tick" or exit_trigger == "tick":
            try:
                ch, cl, cc = st.get("ce_highs"), st.get("ce_lows"), st.get("ce_closes")
                if ch and cl and cc and len(cc) >= 2:
                    live_h = ch[:-1] + [max(ch[-1], price)]
                    live_l = cl[:-1] + [min(cl[-1], price)]
                    live_c = cc[:-1] + [price]
                    direction, _, _ = compute_chandelier_exit(live_h, live_l, live_c, cfg["ce_atr_period"], cfg["ce_atr_mult"], cfg.get("ce_use_close", True))
                    invert = cfg.get("ce_invert_direction", False)
                    dir_now = -direction[-1] if invert else direction[-1]
                    dir_prev = -direction[-2] if invert else direction[-2]
                    buy_signal = dir_now == 1 and dir_prev == -1
                    sell_signal = dir_now == -1 and dir_prev == 1
                    if exit_trigger == "tick":
                        await check_ce_exit(symbol, buy_signal, sell_signal, price)
                    if entry_trigger == "tick":
                        await check_ce_entry(symbol, buy_signal, sell_signal, price)
                    await check_ce_pending(symbol, dir_now, price)
            except Exception as e:
                debug_log(f"⚠️ [{symbol}] Chandelier Exit Live-Tick-Auswertung fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

        if st["position"] is not None and (cfg.get("ce_tp_enabled", False) or cfg.get("ce_sl_enabled", False)):
            entry = st["avg_entry_price"]
            pnl_usd = (price - entry) * st["total_coin_size"] if st["position"] == "long" else (entry - price) * st["total_coin_size"]
            if cfg.get("ce_sl_enabled", False) and pnl_usd <= -abs(cfg.get("ce_sl_usd", 3)):
                await execute_exit(symbol, price, "SL")
                st["ce_sl_cooldown_until"] = time.time() + cfg.get("ce_sl_cooldown_seconds", 30)
            elif cfg.get("ce_tp_enabled", False) and pnl_usd >= abs(cfg.get("ce_tp_usd", 3)):
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
                            try:
                                await on_price_update(symbol, price)
                            except Exception as e:
                                debug_log(f"⚠️ [{symbol}] on_price_update fehlgeschlagen (Verbindung bleibt bestehen)", {"error": str(e), "traceback": traceback.format_exc()})
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

def backtest_trend_meter(candles, cfg):
    """Backtest laeuft immer wie 'kerzenbasiert' (bei jedem Kerzenschluss ausgewertet) - eine
    echte Tick-Simulation ist mit historischen OHLC-Daten nicht moeglich, das betrifft nur
    den optionalen Tick-Modus im Live-Betrieb. SL/TP optional, beide fester $-Betrag."""
    ts, o, h, l, c = candles
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    tp_enabled = cfg.get("tm_tp_enabled", False)
    tp_usd = abs(cfg.get("tm_tp_usd", 3))
    sl_enabled = cfg.get("tm_sl_enabled", False)
    sl_usd = abs(cfg.get("tm_sl_usd", 3))
    sl_cooldown_ms = cfg.get("tm_sl_cooldown_seconds", 30) * 1000
    invert = cfg.get("tm_invert_direction", False)
    exit_mode = cfg.get("tm_exit_mode", "any_signal")
    regime_enabled = cfg.get("tm_regime_filter_enabled", False)
    regime_chop_len = cfg.get("tm_regime_chop_length", 14)
    regime_chop_threshold = cfg.get("tm_regime_chop_threshold", 50)
    chop_series = compute_choppiness_series(h, l, c, regime_chop_len) if regime_enabled else None

    macd, macd_signal = compute_macd_line_and_signal(c, cfg["tm_macd_fast"], cfg["tm_macd_slow"], cfg["tm_macd_signal"])
    rsi1 = compute_rsi(c, cfg["tm_rsi1_period"])
    rsi2 = compute_rsi(c, cfg["tm_rsi2_period"])
    ma_fast = _ema_series(c, cfg["tm_ma_fast"])
    ma_slow = _ema_series(c, cfg["tm_ma_slow"])

    warmup = max(cfg["tm_macd_slow"], cfg["tm_rsi1_period"], cfg["tm_rsi2_period"], cfg["tm_ma_slow"], regime_chop_len) + 2
    position = None  # {"dir","entry","size","entry_i"}
    trades = []
    sl_cooldown_until_ts = None

    for i in range(warmup, n):
        price = c[i]

        if position is not None and (tp_enabled or sl_enabled):
            direction, entry, size = position["dir"], position["entry"], position["size"]
            pnl_usd = (price - entry) * size if direction == "long" else (entry - price) * size
            if sl_enabled and pnl_usd <= -sl_usd:
                _bt_close_trade(trades, direction, entry, price, size, i, position["entry_i"], "SL", ts=ts)
                position = None
                if sl_enabled:
                    sl_cooldown_until_ts = ts[i] + sl_cooldown_ms
            elif tp_enabled and pnl_usd >= tp_usd:
                _bt_close_trade(trades, direction, entry, price, size, i, position["entry_i"], "TP", ts=ts)
                position = None

        dot1 = (macd[i] - macd_signal[i]) > 0
        dot2 = rsi1[i] > 50
        dot3 = rsi2[i] > 50
        line = ma_fast[i] > ma_slow[i]
        invert_i = (chop_series[i] is not None and chop_series[i] >= regime_chop_threshold) if regime_enabled else invert
        if invert_i:
            dot1, dot2, dot3, line = not dot1, not dot2, not dot3, not line
        all_green = dot1 and dot2 and dot3 and line
        all_red = not dot1 and not dot2 and not dot3 and not line
        any_red = not all_green
        any_green = dot1 or dot2 or dot3 or line

        if position is not None:
            if exit_mode == "line_only":
                exit_now = (position["dir"] == "long" and not line) or (position["dir"] == "short" and line)
                reason = "TM-LINE-EXIT"
            else:
                exit_now = (position["dir"] == "long" and any_red) or (position["dir"] == "short" and any_green)
                reason = "TM-SIGNAL-EXIT"
            if exit_now:
                _bt_close_trade(trades, position["dir"], position["entry"], price, position["size"], i, position["entry_i"], reason, ts=ts)
                position = None

        if position is None and not (sl_enabled and sl_cooldown_until_ts is not None and ts[i] < sl_cooldown_until_ts):
            direction = "long" if all_green else ("short" if all_red else None)
            if direction:
                size = (margin * leverage) / price
                position = {"dir": direction, "entry": price, "size": size, "entry_i": i}

    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)

    return trades


def backtest_supertrend_fusion(candles, cfg):
    """Backtest laeuft immer 'kerzenbasiert' - eine echte Tick-Simulation ist mit historischen
    OHLC-Daten nicht moeglich, das betrifft nur den optionalen Tick-Modus im Live-Betrieb."""
    ts, o, h, l, c = candles
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    tp_enabled = cfg.get("stf_tp_enabled", False)
    tp_usd = abs(cfg.get("stf_tp_usd", 3))
    sl_enabled = cfg.get("stf_sl_enabled", False)
    sl_usd = abs(cfg.get("stf_sl_usd", 3))
    use_af = cfg.get("stf_use_af_filter", True)
    use_chop = cfg.get("stf_use_chop_filter", True)
    chop_len = cfg.get("stf_chop_length", 14)
    chop_threshold = cfg.get("stf_chop_threshold", 50)
    invert = cfg.get("stf_invert_direction", False)
    use_ema = cfg.get("stf_use_ema_filter", False)
    ema_len = cfg.get("stf_ema_length", 200)

    st_val, direction = compute_supertrend(h, l, c, cfg["stf_atr_period"], cfg["stf_factor"])
    af = compute_average_force(c, h, l, cfg["stf_af_period"], cfg["stf_af_smooth"]) if use_af else None
    chop = compute_choppiness_series(h, l, c, chop_len) if use_chop else None
    ema = _ema_series(c, ema_len) if use_ema else None

    warmup = max(cfg["stf_atr_period"], cfg["stf_af_period"] + cfg["stf_af_smooth"], chop_len, ema_len if use_ema else 0) + 3
    position = None
    trades = []

    for i in range(warmup, n):
        price = c[i]

        if position is not None and (tp_enabled or sl_enabled):
            pdir, entry, size = position["dir"], position["entry"], position["size"]
            pnl_usd = (price - entry) * size if pdir == "long" else (entry - price) * size
            if sl_enabled and pnl_usd <= -sl_usd:
                _bt_close_trade(trades, pdir, entry, price, size, i, position["entry_i"], "SL", ts=ts)
                position = None
            elif tp_enabled and pnl_usd >= tp_usd:
                _bt_close_trade(trades, pdir, entry, price, size, i, position["entry_i"], "TP", ts=ts)
                position = None

        eff_dir_i = -direction[i] if invert else direction[i]
        eff_dir_prev = -direction[i - 1] if invert else direction[i - 1]

        if position is not None:
            if (position["dir"] == "long" and eff_dir_i == 1) or (position["dir"] == "short" and eff_dir_i == -1):
                _bt_close_trade(trades, position["dir"], position["entry"], price, position["size"], i, position["entry_i"], "ST-FLIP-EXIT", ts=ts)
                position = None

        if position is None:
            flip_to_up = eff_dir_i == -1 and eff_dir_prev != -1
            flip_to_down = eff_dir_i == 1 and eff_dir_prev != 1
            bull_ok = True
            bear_ok = True
            if af is not None:
                bull_ok = bull_ok and af[i] > 0
                bear_ok = bear_ok and af[i] < 0
            if chop is not None:
                trending = chop[i] is not None and chop[i] < chop_threshold
                bull_ok = bull_ok and trending
                bear_ok = bear_ok and trending
            eff_bull_ok = bear_ok if invert else bull_ok
            eff_bear_ok = bull_ok if invert else bear_ok
            if ema is not None:
                above_ema = c[i] > ema[i]
                eff_bull_ok = eff_bull_ok and above_ema
                eff_bear_ok = eff_bear_ok and not above_ema
            entry_direction = "long" if (flip_to_up and eff_bull_ok) else ("short" if (flip_to_down and eff_bear_ok) else None)
            if entry_direction:
                size = (margin * leverage) / price
                position = {"dir": entry_direction, "entry": price, "size": size, "entry_i": i}

    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)

    return trades


def compute_stf_effective_direction_series(highs, lows, closes, cfg):
    """Serienversion der reinen SuperTrend-Richtung (inkl. Invertiert-Modus, OHNE die AF-/Chop-/
    EMA-Filter - fuer den Chandelier-Richtungsfilter zaehlt nur 'ist der hoehere Zeitrahmen
    gerade bullisch oder baerisch', nicht ob SuperTrend selbst dort einsteigen wuerde).
    None an Positionen, wo noch nicht genug Kerzen fuer eine verlaessliche ATR-Berechnung da sind."""
    n = len(closes)
    st_val, direction = compute_supertrend(highs, lows, closes, cfg["stf_atr_period"], cfg["stf_factor"])
    invert = cfg.get("stf_invert_direction", False)
    warmup = cfg["stf_atr_period"] + 3
    out = [None] * n
    for i in range(warmup, n):
        out[i] = -direction[i] if invert else direction[i]
    return out


def backtest_chandelier_exit(candles, cfg, stf_candles=None):
    """Backtest laeuft immer 'kerzenbasiert'. Der optionale SuperTrend-Richtungsfilter (hoeherer
    Zeitrahmen) wird ueber die echten Zeitstempel der STF-Kerzen jedem Chandelier-Bar zugeordnet
    (letzte abgeschlossene STF-Kerze zum jeweiligen Zeitpunkt), genau wie im Live-Betrieb.
    Pending-Order-Logik: kommt ein Signal gegen den aktuellen STF-Bias, wird es gemerkt und
    nachtraeglich ausgefuehrt, sobald STF umschwenkt - solange das Chandelier-Signal bis dahin
    nicht selbst wieder gedreht hat. Optional SL (mit Cooldown danach, verhindert sofortiges
    Wieder-Einsteigen in dieselbe Lage) und optionaler fester $-Take-Profit."""
    ts, o, h, l, c = candles
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    tp_enabled = cfg.get("ce_tp_enabled", False)
    tp_usd = abs(cfg.get("ce_tp_usd", 3))
    sl_enabled = cfg.get("ce_sl_enabled", False)
    sl_usd = abs(cfg.get("ce_sl_usd", 3))
    sl_cooldown_ms = cfg.get("ce_sl_cooldown_seconds", 30) * 1000
    atr_period = cfg["ce_atr_period"]
    atr_mult = cfg["ce_atr_mult"]
    use_close = cfg.get("ce_use_close", True)
    stf_filter_enabled = cfg.get("ce_stf_filter_enabled", False) and stf_candles is not None

    direction, long_stop, short_stop = compute_chandelier_exit(h, l, c, atr_period, atr_mult, use_close)

    stf_ts_list = None
    stf_direction_series = None
    if stf_filter_enabled:
        stf_ts_list, stf_o, stf_h, stf_l, stf_c = stf_candles
        stf_direction_series = compute_stf_effective_direction_series(stf_h, stf_l, stf_c, cfg)

    def stf_bias_for_ts(t):
        if not stf_filter_enabled:
            return None
        idx = bisect.bisect_right(stf_ts_list, t) - 1
        if idx < 0:
            return None
        d = stf_direction_series[idx]
        return None if d is None else ("long" if d == -1 else "short")

    warmup = atr_period + 3
    position = None
    pending_direction = None
    trades = []
    invert = cfg.get("ce_invert_direction", False)
    sl_cooldown_until_ts = None

    for i in range(warmup, n):
        price = c[i]
        dir_now = -direction[i] if invert else direction[i]
        dir_prev = -direction[i - 1] if invert else direction[i - 1]

        if position is not None and (tp_enabled or sl_enabled):
            pdir, entry, size = position["dir"], position["entry"], position["size"]
            # Intrabar-Pruefung ueber Hoch/Tief statt nur Schlusskurs: sonst kann der Kurs
            # innerhalb einer Kerze (v.a. bei laengeren Zeitrahmen wie 1h) weit durch die
            # SL-/TP-Schwelle durchlaufen, bevor ueberhaupt geprueft wird - das wuerde live
            # (tick-basiert) nie passieren. sl_price/tp_price ist der exakte Kurs, bei dem die
            # $-Schwelle erreicht wird; wird er von Hoch oder Tief der Kerze beruehrt, gilt das
            # als ausgeloest - realistischer als "erst beim naechsten Kerzenschluss pruefen".
            sl_price = None
            if sl_enabled:
                sl_price = (entry - sl_usd / size) if pdir == "long" else (entry + sl_usd / size)
            tp_price = None
            if tp_enabled:
                tp_price = (entry + tp_usd / size) if pdir == "long" else (entry - tp_usd / size)
            hit_sl = sl_price is not None and ((pdir == "long" and l[i] <= sl_price) or (pdir == "short" and h[i] >= sl_price))
            hit_tp = tp_price is not None and ((pdir == "long" and h[i] >= tp_price) or (pdir == "short" and l[i] <= tp_price))
            if hit_sl:
                _bt_close_trade(trades, pdir, entry, sl_price, size, i, position["entry_i"], "SL", ts=ts)
                position = None
                sl_cooldown_until_ts = ts[i] + sl_cooldown_ms
            elif hit_tp:
                _bt_close_trade(trades, pdir, entry, tp_price, size, i, position["entry_i"], "TP", ts=ts)
                position = None

        buy_signal = dir_now == 1 and dir_prev == -1
        sell_signal = dir_now == -1 and dir_prev == 1

        if position is not None:
            if (position["dir"] == "long" and sell_signal) or (position["dir"] == "short" and buy_signal):
                _bt_close_trade(trades, position["dir"], position["entry"], price, position["size"], i, position["entry_i"], "CE-FLIP-EXIT", ts=ts)
                position = None

        in_sl_cooldown = sl_enabled and sl_cooldown_until_ts is not None and ts[i] < sl_cooldown_until_ts

        if position is None and not in_sl_cooldown:
            bias = stf_bias_for_ts(ts[i]) if stf_filter_enabled else None
            entered_this_bar = False
            if buy_signal or sell_signal:
                flip_dir = "long" if buy_signal else "short"
                if not stf_filter_enabled or bias == flip_dir:
                    size = (margin * leverage) / price
                    position = {"dir": flip_dir, "entry": price, "size": size, "entry_i": i}
                    pending_direction = None
                    entered_this_bar = True
                else:
                    pending_direction = flip_dir

            if not entered_this_bar and stf_filter_enabled and pending_direction:
                still_valid = (pending_direction == "long" and dir_now == 1) or (pending_direction == "short" and dir_now == -1)
                if not still_valid:
                    pending_direction = None
                elif bias == pending_direction:
                    size = (margin * leverage) / price
                    position = {"dir": pending_direction, "entry": price, "size": size, "entry_i": i}
                    pending_direction = None

    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)

    return trades


BACKTEST_MAX_CANDLES = {
    "fib_reversal": 100_000, "range_profile": 30_000, "zscore_trend": 100_000, "blsh_trend": 100_000,
    "trend_meter": 100_000, "supertrend_fusion": 100_000, "chandelier_exit": 100_000,
}


def _bt_close_trade(trades, direction, entry, exit_price, size, i, entry_i, reason, ts=None):
    pnl = (exit_price - entry) * size if direction == "long" else (entry - exit_price) * size
    trade = {"dir": direction, "entry": entry, "exit": exit_price, "reason": reason,
             "pnl": pnl, "bars_held": i - entry_i}
    if ts is not None:
        trade["entry_ts"] = ts[entry_i]
        trade["exit_ts"] = ts[i]
    trades.append(trade)



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
                _bt_close_trade(trades, direction, entry, price, size, i, position["entry_i"], "SL" if sl_floor < 0 else "BREAKEVEN-LOCK", ts=ts)
                position = None
                breakeven_triggered = False
            elif pnl_usd >= cfg["rp_tp_usd"]:
                _bt_close_trade(trades, direction, entry, price, size, i, position["entry_i"], "TP", ts=ts)
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
            _bt_close_trade(trades, direction, position["avg_entry"], price, position["size"], i, position["entry_i"], "SL", ts=ts)
            position = None
            continue

        if not position["tp1_done"]:
            tp1_hit = price >= fib["tp1_price"] if direction == "long" else price <= fib["tp1_price"]
            if tp1_hit:
                fraction = cfg["fib_tp1_close_pct"] / 100
                close_size = position["size"] * fraction
                _bt_close_trade(trades, direction, position["avg_entry"], price, close_size, i, position["entry_i"], "TP1", ts=ts)
                position["size"] -= close_size
                position["tp1_done"] = True
                position["sl_active"] = position["avg_entry"]
            continue

        tp2_hit = price >= fib["tp2_price"] if direction == "long" else price <= fib["tp2_price"]
        if tp2_hit:
            _bt_close_trade(trades, direction, position["avg_entry"], price, position["size"], i, position["entry_i"], "TP2", ts=ts)
            position = None

    return trades


def backtest_zscore_trend(candles, cfg):
    ts, o, h, l, c = candles
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    lookback = cfg["zscore_lookback_period"]
    ema_smooth = cfg["zscore_ema_smooth"]
    threshold = cfg["zscore_threshold"]
    sl_usd = cfg["zscore_sl_usd"]
    tp1_usd = cfg["zscore_tp1_usd"]
    tp1_close_pct = cfg["zscore_tp1_close_pct"] / 100
    breakeven_lock = cfg["zscore_breakeven_lock_usd"]
    tp2_usd = cfg["zscore_tp2_usd"]
    sl_enabled = cfg.get("zscore_sl_enabled", True)
    breakeven_enabled = cfg.get("zscore_breakeven_enabled", True)
    tp2_enabled = cfg.get("zscore_tp2_enabled", True)
    dir_mode = cfg.get("zscore_direction_mode", "both")
    cooldown_ms = cfg.get("zscore_cooldown_seconds", 0) * 1000

    smooth_z = compute_zscore_trend(c, lookback, ema_smooth)

    warmup = lookback + 5
    position = None  # {"dir","entry","size","tp1_done","entry_i"}
    trades = []
    last_exit_ts = -10 ** 15

    for i in range(warmup, n):
        price = c[i]

        if position is not None:
            direction, entry, size = position["dir"], position["entry"], position["size"]
            pnl_usd = (price - entry) * size if direction == "long" else (entry - price) * size
            if not position["tp1_done"]:
                if sl_enabled and pnl_usd <= -sl_usd:
                    _bt_close_trade(trades, direction, entry, price, size, i, position["entry_i"], "SL", ts=ts)
                    position = None
                    last_exit_ts = ts[i]
                elif pnl_usd >= tp1_usd:
                    if tp2_enabled:
                        close_size = size * tp1_close_pct
                        _bt_close_trade(trades, direction, entry, price, close_size, i, position["entry_i"], "TP1", ts=ts)
                        position["size"] -= close_size
                        position["tp1_done"] = True
                    else:
                        # TP2 aus: TP1 ist der finale, vollstaendige Ausstieg
                        _bt_close_trade(trades, direction, entry, price, size, i, position["entry_i"], "TP1", ts=ts)
                        position = None
                        last_exit_ts = ts[i]
            else:
                if breakeven_enabled and pnl_usd <= breakeven_lock:
                    _bt_close_trade(trades, direction, entry, price, size, i, position["entry_i"], "BREAKEVEN-LOCK", ts=ts)
                    position = None
                    last_exit_ts = ts[i]
                elif tp2_enabled and pnl_usd >= tp2_usd:
                    _bt_close_trade(trades, direction, entry, price, size, i, position["entry_i"], "TP2", ts=ts)
                    position = None
                    last_exit_ts = ts[i]

        prev_z, curr_z = smooth_z[i - 1], smooth_z[i]
        long_entry = prev_z <= threshold and curr_z > threshold
        short_entry = prev_z >= -threshold and curr_z < -threshold
        long_exit = prev_z >= 0 and curr_z < 0
        short_exit = prev_z <= 0 and curr_z > 0
        if dir_mode == "long_only":
            short_entry = False
        elif dir_mode == "short_only":
            long_entry = False

        # Exit zuerst: Nulllinien-Rueckkreuzung schliesst eine noch offene Position
        # (falls TP nicht schon vorher lief), ohne automatisch umzudrehen
        if position is not None:
            if (position["dir"] == "long" and long_exit) or (position["dir"] == "short" and short_exit):
                _bt_close_trade(trades, position["dir"], position["entry"], price, position["size"], i, position["entry_i"], "ZSCORE-ZERO-EXIT", ts=ts)
                position = None
                last_exit_ts = ts[i]

        direction = "long" if long_entry else ("short" if short_entry else None)
        if direction and position is None and (ts[i] - last_exit_ts) >= cooldown_ms:
            size = (margin * leverage) / price
            position = {"dir": direction, "entry": price, "size": size, "tp1_done": False, "entry_i": i}

    if position is not None:
        # Position am Ende des Testzeitraums noch offen - nicht stillschweigend fallen
        # lassen (sonst wuerde ein evtl. Verlust komplett aus der Statistik verschwinden),
        # sondern zum letzten bekannten Kurs schliessen und klar als solche markieren.
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)

    return trades


def backtest_blsh_macd_cross(candles, cfg):
    """Backtest fuer den reinen Wechsel-Modus (siehe blsh_trend_poll_loop, "macd_cross"):
    kein TP/SL, immer im Markt, dreht bei jeder MACD/Signal-Kreuzung sofort um."""
    ts, o, h, l, c, v = candles
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    dir_mode = cfg.get("blsh_direction_mode", "both")
    allow_long = dir_mode != "short_only"
    allow_short = dir_mode != "long_only"

    macd, macd_signal = compute_macd_line_and_signal(c, cfg["blsh_macd_fast"], cfg["blsh_macd_slow"], cfg["blsh_macd_signal"])
    diff = [macd[i] - macd_signal[i] for i in range(n)]

    warmup = max(cfg["blsh_macd_slow"], cfg["blsh_macd_signal"]) + 5
    position = None
    trades = []

    for i in range(warmup, n):
        price = c[i]
        prev_d, curr_d = diff[i - 1], diff[i]
        bullish = prev_d <= 0 and curr_d > 0
        bearish = prev_d >= 0 and curr_d < 0
        direction = "long" if bullish else ("short" if bearish else None)

        if direction:
            if position is not None and position["dir"] != direction:
                _bt_close_trade(trades, position["dir"], position["entry"], price, position["size"], i, position["entry_i"], "BLSH-MACD-REVERSE", ts=ts)
                position = None
            if position is None and ((direction == "long" and allow_long) or (direction == "short" and allow_short)):
                size = (margin * leverage) / price
                position = {"dir": direction, "entry": price, "size": size, "entry_i": i}

    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)

    return trades


def backtest_blsh_trend(candles, cfg):
    if cfg.get("blsh_signal_mode", "composite") == "macd_cross":
        return backtest_blsh_macd_cross(candles, cfg)

    ts, o, h, l, c, v = candles
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    threshold = cfg["blsh_threshold"]
    sl_usd = cfg["blsh_sl_usd"]
    tp1_usd = cfg["blsh_tp1_usd"]
    tp1_close_pct = cfg["blsh_tp1_close_pct"] / 100
    breakeven_lock = cfg["blsh_breakeven_lock_usd"]
    tp2_usd = cfg["blsh_tp2_usd"]
    sl_enabled = cfg.get("blsh_sl_enabled", True)
    breakeven_enabled = cfg.get("blsh_breakeven_enabled", True)
    tp2_enabled = cfg.get("blsh_tp2_enabled", True)
    dir_mode = cfg.get("blsh_direction_mode", "both")
    cooldown_ms = cfg.get("blsh_cooldown_seconds", 0) * 1000

    composite = compute_blsh_composite(
        h, l, c, v, cfg["blsh_atr_period"], cfg["blsh_rsi_period"], cfg["blsh_ema_fast"],
        cfg["blsh_ema_slow"], cfg["blsh_macd_fast"], cfg["blsh_macd_slow"], cfg["blsh_macd_signal"], cfg["blsh_mfi_period"],
    )

    warmup = max(cfg["blsh_ema_slow"], cfg["blsh_macd_slow"], cfg["blsh_atr_period"], cfg["blsh_rsi_period"], cfg["blsh_mfi_period"]) + 5
    position = None
    trades = []
    last_exit_ts = -10 ** 15

    for i in range(warmup, n):
        price = c[i]

        if position is not None:
            direction, entry, size = position["dir"], position["entry"], position["size"]
            pnl_usd = (price - entry) * size if direction == "long" else (entry - price) * size
            if not position["tp1_done"]:
                if sl_enabled and pnl_usd <= -sl_usd:
                    _bt_close_trade(trades, direction, entry, price, size, i, position["entry_i"], "SL", ts=ts)
                    position = None
                    last_exit_ts = ts[i]
                elif pnl_usd >= tp1_usd:
                    if tp2_enabled:
                        close_size = size * tp1_close_pct
                        _bt_close_trade(trades, direction, entry, price, close_size, i, position["entry_i"], "TP1", ts=ts)
                        position["size"] -= close_size
                        position["tp1_done"] = True
                    else:
                        _bt_close_trade(trades, direction, entry, price, size, i, position["entry_i"], "TP1", ts=ts)
                        position = None
                        last_exit_ts = ts[i]
            else:
                if breakeven_enabled and pnl_usd <= breakeven_lock:
                    _bt_close_trade(trades, direction, entry, price, size, i, position["entry_i"], "BREAKEVEN-LOCK", ts=ts)
                    position = None
                    last_exit_ts = ts[i]
                elif tp2_enabled and pnl_usd >= tp2_usd:
                    _bt_close_trade(trades, direction, entry, price, size, i, position["entry_i"], "TP2", ts=ts)
                    position = None
                    last_exit_ts = ts[i]

        prev_v, curr_v = composite[i - 1], composite[i]
        long_entry = prev_v <= threshold and curr_v > threshold
        short_entry = prev_v >= -threshold and curr_v < -threshold
        long_exit = prev_v >= 0 and curr_v < 0
        short_exit = prev_v <= 0 and curr_v > 0
        if dir_mode == "long_only":
            short_entry = False
        elif dir_mode == "short_only":
            long_entry = False

        if position is not None:
            if (position["dir"] == "long" and long_exit) or (position["dir"] == "short" and short_exit):
                _bt_close_trade(trades, position["dir"], position["entry"], price, position["size"], i, position["entry_i"], "BLSH-ZERO-EXIT", ts=ts)
                position = None
                last_exit_ts = ts[i]

        direction = "long" if long_entry else ("short" if short_entry else None)
        if direction and position is None and (ts[i] - last_exit_ts) >= cooldown_ms:
            size = (margin * leverage) / price
            position = {"dir": direction, "entry": price, "size": size, "tp1_done": False, "entry_i": i}

    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)

    return trades


BACKTEST_FUNCS = {
    "range_profile": backtest_range_profile,
    "fib_reversal": backtest_fib_reversal,
    "zscore_trend": backtest_zscore_trend,
    "blsh_trend": backtest_blsh_trend,
    "trend_meter": backtest_trend_meter,
    "supertrend_fusion": backtest_supertrend_fusion,
    "chandelier_exit": backtest_chandelier_exit,
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


CE_SWEEP_MAX_COMBOS = 400
CE_SWEEP_MIN_RELIABLE_TRADES = 5


async def run_ce_param_sweep(symbol, cfg, days, atr_period_min, atr_period_max, atr_period_step,
                              atr_mult_min, atr_mult_max, atr_mult_step, stf_filter_enabled):
    """'Monte-Carlo'-Parametersweep fuer Chandelier Exit: testet alle Kombinationen aus
    ATR-Periode und ATR-Multiplikator im angegebenen Bereich gegeneinander und liefert die
    besten zurueck. Kerzen werden nur EINMAL geladen (auch fuer den optionalen SuperTrend-
    Filter), danach laufen alle Kombinationen rein rechnerisch auf denselben Daten - das macht
    auch mehrere hundert Kombinationen in Sekunden statt Minuten moeglich."""
    max_candles = BACKTEST_MAX_CANDLES["chandelier_exit"]
    resolution = cfg.get("ce_resolution", "1m")
    candles, err, cache_used = await _fetch_cached_backtest_candles(symbol, resolution, days, max_candles)
    if err:
        return {"error": err}
    if not candles or len(candles[4]) < 100:
        return {"error": "Zu wenig historische Kerzen für einen aussagekräftigen Sweep erhalten."}

    stf_candles = None
    if stf_filter_enabled:
        stf_resolution = cfg.get("ce_stf_resolution", "5m")
        stf_candles, stf_err, _ = await _fetch_cached_backtest_candles(symbol, stf_resolution, days, max_candles)
        if stf_err or not stf_candles or len(stf_candles[4]) < 100:
            return {"error": f"Zu wenig historische Kerzen für den SuperTrend-Filter-Zeitrahmen ({stf_resolution}) erhalten."}

    periods = sorted(set(int(round(atr_period_min + i * atr_period_step))
                          for i in range(int((atr_period_max - atr_period_min) / max(atr_period_step, 1e-9)) + 1)
                          if atr_period_min + i * atr_period_step <= atr_period_max + 1e-9))
    mults = sorted(set(round(atr_mult_min + i * atr_mult_step, 4)
                        for i in range(int((atr_mult_max - atr_mult_min) / max(atr_mult_step, 1e-9)) + 1)
                        if atr_mult_min + i * atr_mult_step <= atr_mult_max + 1e-9))
    periods = [p for p in periods if p >= 1]
    mults = [m for m in mults if m > 0]

    total_combos = len(periods) * len(mults)
    if total_combos == 0:
        return {"error": "Der eingestellte Bereich ergibt keine gültigen Kombinationen."}
    if total_combos > CE_SWEEP_MAX_COMBOS:
        return {"error": f"Zu viele Kombinationen ({total_combos}, Limit {CE_SWEEP_MAX_COMBOS}) - Bereich oder Schrittweite vergrößern."}

    results = []
    for period in periods:
        for mult in mults:
            cfg_copy = dict(cfg)
            cfg_copy["ce_atr_period"] = period
            cfg_copy["ce_atr_mult"] = mult
            cfg_copy["ce_stf_filter_enabled"] = stf_filter_enabled
            trades = backtest_chandelier_exit(candles, cfg_copy, stf_candles=stf_candles if stf_filter_enabled else None)
            stats = summarize_backtest_trades(trades)
            results.append({"ce_atr_period": period, "ce_atr_mult": mult, **stats})

    # Zuverlaessige Ergebnisse (genug Trades) zuerst, darunter nach PnL sortiert - Kombinationen
    # mit zu wenig Trades sind statistisch kaum aussagekraeftig, sollen aber sichtbar bleiben
    results.sort(key=lambda r: (r["trades"] >= CE_SWEEP_MIN_RELIABLE_TRADES, r["total_pnl_usd"]), reverse=True)

    actual_days = (candles[0][-1] - candles[0][0]) / (24 * 60 * 60 * 1000)
    return {
        "symbol": symbol, "resolution": resolution, "requested_days": days,
        "actual_days_covered": round(actual_days, 1), "candles_processed": len(candles[4]),
        "stf_filter_used": stf_filter_enabled, "min_reliable_trades": CE_SWEEP_MIN_RELIABLE_TRADES,
        "combos_tested": total_combos,
        "results": results[:30],
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


async def _fetch_cached_backtest_candles(symbol, resolution, days, max_candles):
    """Gemeinsame Kerzen-Cache-Logik (sonst 1:1 dupliziert) - wird gebraucht, weil Chandelier
    Exit im Backtest ggf. ZWEI verschiedene Aufloesungen gleichzeitig braucht (eigener
    Zeitrahmen + hoeherer SuperTrend-Filter-Zeitrahmen)."""
    if resolution in SUB_MINUTE_RESOLUTIONS:
        max_candles = min(max_candles, 5000)
    cache_key = (symbol, resolution)
    cached = _backtest_cache_get(cache_key)
    now = time.time()
    cache_used = False
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
    return candles, err, cache_used


async def run_backtest(symbol, entry_mode, cfg, days):
    if entry_mode not in BACKTEST_FUNCS:
        return {"error": f"Backtest für '{entry_mode}' nicht unterstützt (nur range_profile, fib_reversal, zscore_trend, blsh_trend, trend_meter, supertrend_fusion, chandelier_exit - Grid/OBI-Scalp brauchen historische Tick-/Orderbuchdaten, die es nicht gibt)."}

    max_candles = BACKTEST_MAX_CANDLES[entry_mode]

    if entry_mode == "blsh_trend":
        # Eigener Pfad: braucht Volumen (6er- statt 5er-Tupel), nutzt daher nicht den
        # gemeinsamen Kerzen-Cache der anderen Strategien.
        resolution = cfg.get("blsh_resolution", "1m")
        candles, err = await fetch_historical_candles_binance_vol(symbol, resolution, days, max_candles)
        if err:
            return {"error": err}
        if not candles or len(candles[4]) < 100:
            return {"error": "Zu wenig historische Kerzen für einen aussagekräftigen Backtest erhalten."}
        n_candles = len(candles[4])
        trades = backtest_blsh_trend(candles, cfg)
        stats = summarize_backtest_trades(trades)
        stats_long = summarize_backtest_trades([t for t in trades if t["dir"] == "long"])
        stats_short = summarize_backtest_trades([t for t in trades if t["dir"] == "short"])
        actual_days = (candles[0][-1] - candles[0][0]) / (24 * 60 * 60 * 1000)
        return {
            "symbol": symbol, "entry_mode": entry_mode, "resolution": resolution,
            "requested_days": days, "actual_days_covered": round(actual_days, 1),
            "candles_processed": n_candles, "candle_cap": max_candles, "cache_used": False,
            "stats": stats, "stats_long": stats_long, "stats_short": stats_short,
            "trades": trades[-50:],
        }

    if entry_mode == "chandelier_exit":
        resolution = cfg.get("ce_resolution", "1m")
        candles, err, cache_used = await _fetch_cached_backtest_candles(symbol, resolution, days, max_candles)
        if err:
            return {"error": err}
        if not candles or len(candles[4]) < 100:
            return {"error": "Zu wenig historische Kerzen für einen aussagekräftigen Backtest erhalten."}

        stf_candles = None
        if cfg.get("ce_stf_filter_enabled", False):
            stf_resolution = cfg.get("ce_stf_resolution", "5m")
            stf_candles, stf_err, _ = await _fetch_cached_backtest_candles(symbol, stf_resolution, days, max_candles)
            if stf_err or not stf_candles or len(stf_candles[4]) < 100:
                return {"error": f"Zu wenig historische Kerzen für den SuperTrend-Filter-Zeitrahmen ({stf_resolution}) erhalten."}

        n_candles = len(candles[4])
        trades = backtest_chandelier_exit(candles, cfg, stf_candles=stf_candles)
        stats = summarize_backtest_trades(trades)
        stats_long = summarize_backtest_trades([t for t in trades if t["dir"] == "long"])
        stats_short = summarize_backtest_trades([t for t in trades if t["dir"] == "short"])
        actual_days = (candles[0][-1] - candles[0][0]) / (24 * 60 * 60 * 1000)
        return {
            "symbol": symbol, "entry_mode": entry_mode, "resolution": resolution,
            "requested_days": days, "actual_days_covered": round(actual_days, 1),
            "candles_processed": n_candles, "candle_cap": max_candles, "cache_used": cache_used,
            "stats": stats, "stats_long": stats_long, "stats_short": stats_short,
            "trades": trades[-50:],
        }

    resolution_key = {"range_profile": "rp_resolution", "fib_reversal": "fib_resolution",
                       "zscore_trend": "zscore_resolution", "trend_meter": "tm_resolution",
                       "supertrend_fusion": "stf_resolution"}[entry_mode]
    resolution = cfg.get(resolution_key, "1m")
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
    stats_long = summarize_backtest_trades([t for t in trades if t["dir"] == "long"])
    stats_short = summarize_backtest_trades([t for t in trades if t["dir"] == "short"])

    actual_days = (candles[0][-1] - candles[0][0]) / (24 * 60 * 60 * 1000)
    return {
        "symbol": symbol, "entry_mode": entry_mode, "resolution": resolution,
        "requested_days": days, "actual_days_covered": round(actual_days, 1),
        "candles_processed": n_candles, "candle_cap": max_candles, "cache_used": cache_used,
        "stats": stats, "stats_long": stats_long, "stats_short": stats_short,
        "trades": trades[-50:],  # letzte 50 fuers Dashboard, nicht alle
        "tm_invert_direction": cfg.get("tm_invert_direction", False) if entry_mode == "trend_meter" else None,
        "tm_regime_filter_enabled": cfg.get("tm_regime_filter_enabled", False) if entry_mode == "trend_meter" else None,
    }

