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
                    body = await resp.text()
                    debug_log(f"⚠️ [{symbol}] Binance-Kerzenabfrage HTTP {resp.status}", {"body": body[:300]})
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
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
    "30m": 1_800_000, "1h": 3_600_000, "4h": 14_400_000,
}


SYNTHETIC_RESOLUTIONS = {"10s": ("1s", 10), "15s": ("1s", 15), "30s": ("1s", 30), "45s": ("1s", 45)}  # Zeitrahmen, die Binance nicht nativ anbietet


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



def resample_candles(data, factor):
    """Fasst 1m-Kerzen zu groesseren Kerzen zusammen (z.B. 2m), AUSGERICHTET AN ECHTEN
    UHRZEIT-GRENZEN (:00-:02, :02-:04, ...) - genau wie TradingView/Binance das bei nativ
    unterstuetzten Zeitrahmen machen. Vorher wurde stur ab dem Anfang des geladenen Arrays in
    Zweierpaaren gruppiert (Zeile 'range(0, n, factor)') - je nachdem, wann der Bot gerade
    Daten abgerufen hat, verschob sich dadurch die Kerzengrenze und stimmte nicht mehr mit dem
    TradingView-Chart ueberein (z.B. unsere Kerze 12:01-12:03 statt TradingViews 12:00-12:02) -
    das erklaerte reale Abweichungen zwischen Chart-Signalen und Backtest-Ergebnissen."""
    timestamps, opens, highs, lows, closes = data
    n = len(closes)
    if n == 0:
        return [], [], [], [], []
    bucket_ms = factor * 60_000
    out_ts, out_o, out_h, out_l, out_c = [], [], [], [], []
    i = 0
    while i < n:
        bucket = timestamps[i] // bucket_ms
        j = i
        while j < n and timestamps[j] // bucket_ms == bucket:
            j += 1
        # Nur vollstaendige Buckets (genau 'factor' Kerzen drin) uebernehmen - ein am Rand
        # angeschnittener Bucket wuerde eine unvollstaendige, verzerrte Kerze erzeugen.
        if j - i == factor:
            out_ts.append(timestamps[i])
            out_o.append(opens[i])
            out_h.append(max(highs[i:j]))
            out_l.append(min(lows[i:j]))
            out_c.append(closes[j - 1])
        i = j
    return out_ts, out_o, out_h, out_l, out_c


SUB_MINUTE_RESOLUTIONS = {"10s": 10, "15s": 15, "30s": 30, "45s": 45}  # Sekunden je Kerze, alle aus dem 1s-Puffer


def _resample_seconds_candles(data, seconds):
    """Wie resample_candles(), aber fuer SEKUNDEN-Buckets statt Minuten - resample_candles
    geht fest von Minuten-Kerzen aus (bucket_ms = factor * 60_000), was bei 1-Sekunden-
    Quelldaten und z.B. seconds=30 einen 30-MINUTEN-Bucket ergeben wuerde (1800 statt 30
    Kerzen pro Bucket) und dadurch praktisch nie einen vollstaendigen Bucket liefert. Das war
    ein echter Bug: get_seconds_candles() rief bisher direkt resample_candles(..., seconds)
    auf, wodurch alle Sekunden-Zeitrahmen (10s/15s/30s/45s) ueber den echten
    Binance-1s-Puffer faktisch nie genug Kerzen zurueckgaben."""
    timestamps, opens, highs, lows, closes = data
    n = len(closes)
    if n == 0:
        return [], [], [], [], []
    bucket_ms = seconds * 1000
    out_ts, out_o, out_h, out_l, out_c = [], [], [], [], []
    i = 0
    while i < n:
        bucket = timestamps[i] // bucket_ms
        j = i
        while j < n and timestamps[j] // bucket_ms == bucket:
            j += 1
        if j - i == seconds:
            out_ts.append(timestamps[i])
            out_o.append(opens[i])
            out_h.append(max(highs[i:j]))
            out_l.append(min(lows[i:j]))
            out_c.append(closes[j - 1])
        i = j
    return out_ts, out_o, out_h, out_l, out_c


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
    r_ts, r_o, r_h, r_l, r_c = _resample_seconds_candles((ts, o, h, l, cl), seconds)
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
            cfg = b["config"]
            if cfg["bot_active"]:
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
            if cfg["entry_mode"] == "fib_reversal" and cfg["bot_active"]:
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



def compute_stochastic(highs, lows, closes, k_period, smooth_k, d_period):
    """Standard-Stochastic-Oszillator: %K = 100 * (Close - Tiefstes Tief) / (Hoechstes Hoch -
    Tiefstes Tief) ueber k_period, danach %K geglaettet (smooth_k) und %D als SMA von %K
    (d_period). Reagiert bei kurzen Perioden schneller auf rohe Kursausschlaege als RSI -
    deshalb fuers Scalp-Board als Haupt-Timing-Oszillator genutzt."""
    n = len(closes)
    if n == 0:
        return [], []
    raw_k = [50.0] * n
    for i in range(n):
        start = max(0, i - k_period + 1)
        hh = max(highs[start:i + 1])
        ll = min(lows[start:i + 1])
        raw_k[i] = 50.0 if hh == ll else 100 * (closes[i] - ll) / (hh - ll)
    k = _sma_series(raw_k, smooth_k) if smooth_k > 1 else raw_k
    d = _sma_series(k, d_period)
    return k, d


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


def _sma_series(values, length):
    n = len(values)
    out = [0.0] * n
    for i in range(n):
        start = max(0, i - length + 1)
        window = values[start:i + 1]
        out[i] = sum(window) / len(window)
    return out


def compute_ut_bot(highs, lows, closes, atr_period, key_value):
    """Portiert aus dem 'UT Bot'-Baustein des 'Wave Cipher SMC Flow System' (urspruenglich ein
    eigenstaendiges, weit verbreitetes TradingView-Skript): EIN gemeinsamer ATR-Trailing-Stop
    (nicht zwei getrennte Long-/Short-Baender wie bei Chandelier Exit), der sich nur nachzieht
    wenn der Kurs bereits mehrere Kerzen auf derselben Seite war. Buy/Sell entsteht, wenn der
    Kurs die Stop-Linie kreuzt.
    Gibt (stop, buy, sell) zurueck - stop ist die Trailing-Stop-Linie, buy/sell sind Bool-Listen
    (True an der Kerze, an der die Kreuzung stattfindet)."""
    n = len(closes)
    atr = compute_atr(highs, lows, closes, atr_period)
    n_loss = [key_value * a for a in atr]
    stop = [0.0] * n
    if n == 0:
        return stop, [], []
    stop[0] = closes[0] - n_loss[0]
    for i in range(1, n):
        prev = stop[i - 1]
        src, src_prev = closes[i], closes[i - 1]
        if src > prev and src_prev > prev:
            stop[i] = max(prev, src - n_loss[i])
        elif src < prev and src_prev < prev:
            stop[i] = min(prev, src + n_loss[i])
        elif src > prev:
            stop[i] = src - n_loss[i]
        else:
            stop[i] = src + n_loss[i]
    buy = [False] * n
    sell = [False] * n
    for i in range(1, n):
        buy[i] = closes[i - 1] <= stop[i - 1] and closes[i] > stop[i]
        sell[i] = closes[i - 1] >= stop[i - 1] and closes[i] < stop[i]
    return stop, buy, sell


def compute_wavetrend(highs, lows, closes, chlen, avg_len, ma_len):
    """Portiert aus dem 'Cipher B'-WaveTrend-Baustein des 'Wave Cipher SMC Flow System'
    (urspruenglich vom Cipher-B-Skript von falconCoin/LazyBear-Ableitungen). Quelle ist hlc3
    (Durchschnitt aus Hoch/Tief/Schluss), wie im Original. Gibt (wt1, wt2) zurueck."""
    n = len(closes)
    src = [(highs[i] + lows[i] + closes[i]) / 3 for i in range(n)]
    esa = _ema_series(src, chlen)
    de = _ema_series([abs(src[i] - esa[i]) for i in range(n)], chlen)
    ci = [0.0] * n
    for i in range(n):
        denom = 0.015 * de[i]
        ci[i] = 0.0 if denom == 0 else (src[i] - esa[i]) / denom
    wt1 = _ema_series(ci, avg_len)
    wt2 = _sma_series(wt1, ma_len)
    return wt1, wt2


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


def compute_halftrend(highs, lows, closes, amplitude, channel_deviation):
    """Portiert aus 'HalfTrend Long/Short Signal Engine [BigBeluga]' (Basis-Trendlogik von
    everget's originalem HalfTrend-Indikator) - nur der Teil, der tatsaechlich Long/Short
    bestimmt (Swing-Hoch/-Tief-Vergleich gegen gleitende Durchschnitte). Die Risk-Management-
    und Dashboard-Teile des Original-Skripts sind rein visuell und wurden weggelassen.
    ATR-Periode ist im Original FEST auf 100 (ta.atr(100)/2 als 'atr2') - nur amplitude
    (Swing-Lookback-Fenster) ist ein echter Signal-Parameter. channel_deviation beeinflusst im
    Original NUR die geplotteten Kanal-Baender, nicht das Signal selbst - in diesem Bot wird es
    stattdessen als SL-Abstand (in ATR2-Vielfachen) verwendet (siehe backtest_halftrend/
    check_ht_entry), damit der Parameter hier tatsaechlich etwas bewirkt.
    Gibt (ht_line, trend, atr2) zurueck - trend 0 = bullisch (Long), 1 = baerisch (Short), wie
    im Original-Skript. atr2 wird fuer die SL-/TP-Abstandsberechnung (Base Risk) mitgeliefert."""
    n = len(closes)
    if n == 0:
        return [], [], []
    atr = compute_atr(highs, lows, closes, 100)
    atr2 = [a / 2.0 for a in atr]
    highma = _sma_series(highs, amplitude)
    lowma = _sma_series(lows, amplitude)

    trend = 0
    next_trend = 0
    max_low_price = lows[0]
    min_high_price = highs[0]
    up = 0.0
    down = 0.0
    prev_trend_final = None

    ht_line = [0.0] * n
    trend_out = [0] * n

    for i in range(n):
        start = max(0, i - amplitude + 1)
        high_price = max(highs[start:i + 1])
        low_price = min(lows[start:i + 1])
        prev_low = lows[i - 1] if i > 0 else lows[i]
        prev_high = highs[i - 1] if i > 0 else highs[i]

        if next_trend == 1:
            max_low_price = max(low_price, max_low_price)
            if highma[i] < max_low_price and closes[i] < prev_low:
                trend = 1
                next_trend = 0
                min_high_price = high_price
        else:
            min_high_price = min(high_price, min_high_price)
            if lowma[i] > min_high_price and closes[i] > prev_high:
                trend = 0
                next_trend = 1
                max_low_price = low_price

        up_before, down_before = up, down

        if trend == 0:
            if i > 0 and prev_trend_final is not None and prev_trend_final != 0:
                up = down_before
            else:
                up = max_low_price if i == 0 else max(max_low_price, up_before)
            ht_line[i] = up
        else:
            if i > 0 and prev_trend_final is not None and prev_trend_final != 1:
                down = up_before
            else:
                down = min_high_price if i == 0 else min(min_high_price, down_before)
            ht_line[i] = down

        trend_out[i] = trend
        prev_trend_final = trend

    return ht_line, trend_out, atr2


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
            if cfg["entry_mode"] == "range_profile" and cfg["bot_active"]:
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
            if cfg["entry_mode"] == "supertrend_fusion" and cfg["bot_active"]:
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
            if cfg["entry_mode"] == "chandelier_exit" and cfg["bot_active"]:
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


async def check_ut_entry(symbol, buy_signal, sell_signal, price):
    """Einstieg beim Buy/Sell-Signal des UT-Bot-Trailing-Stops. Mit Invertiert-Modus wurden
    buy_signal/sell_signal schon VOR dem Aufruf getauscht."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or st["position"] is not None or price is None:
        return
    if cfg.get("ut_sl_enabled", False) and time.time() < st.get("ut_sl_cooldown_until", 0.0):
        return
    if not (buy_signal or sell_signal):
        return
    direction = "long" if buy_signal else "short"
    debug_log(f"📡 [{symbol}] UT-Bot Signal: {direction.upper()} @ {price}")
    await execute_entry(symbol, direction, price, is_add_on=False)


async def check_ut_exit(symbol, buy_signal, sell_signal, price):
    """Ausstieg beim Gegen-Signal - Flip-System wie Chandelier Exit."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or st["position"] is None or price is None:
        return
    if st["position"] == "long" and sell_signal:
        debug_log(f"🚪 [{symbol}] UT-Bot Exit: LONG @ {price} (Sell-Signal)")
        await execute_exit(symbol, price, "UT-FLIP-EXIT")
    elif st["position"] == "short" and buy_signal:
        debug_log(f"🚪 [{symbol}] UT-Bot Exit: SHORT @ {price} (Buy-Signal)")
        await execute_exit(symbol, price, "UT-FLIP-EXIT")


async def ut_poll_loop(symbol):
    """UT-Bot-Trailing-Stop (portiert aus dem 'UT Bot'-Baustein des 'Wave Cipher SMC Flow
    System'): EIN gemeinsamer ATR-Trailing-Stop (ATR-Periode + Key-Value-Multiplikator beide
    einstellbar). Buy/Sell beim Kreuzen der Stop-Linie, Ausstieg beim Gegen-Signal (Flip-System).
    Optional SL (mit Cooldown) + TP fest $, Invertiert-Modus, Ein-/Ausstieg je einzeln
    tick-/kerzenbasiert, alle Zeitrahmen, Backtest-faehig."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "ut_bot" and cfg["bot_active"]:
                resolution = cfg["ut_resolution"]
                atr_period = cfg["ut_atr_period"]
                min_needed = atr_period + 3
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
                        closed_ts, closed_h, closed_l, closed_c = timestamps[:-1], highs[:-1], lows[:-1], closes[:-1]
                    else:
                        closed_ts = None

                now = time.time()
                due_heartbeat = now - last_heartbeat > 300

                if closed_ts and len(closed_c) > min_needed:
                    signal_key = closed_ts[-1]
                    is_new_candle = last_processed_ts != signal_key
                    price = st["last_price"] if st["last_price"] is not None else closed_c[-1]

                    keep = min_needed + 5
                    st["ut_highs"] = closed_h[-keep:]
                    st["ut_lows"] = closed_l[-keep:]
                    st["ut_closes"] = closed_c[-keep:]

                    stop, buy, sell = compute_ut_bot(closed_h, closed_l, closed_c, atr_period, cfg["ut_key_value"])
                    st["ut_stop_value"] = stop[-1]

                    if due_heartbeat:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] UT-Bot aktiv: Stop={round(stop[-1],4)}, Preis={closed_c[-1]}, Kerzen={len(closed_c)}, bot_active={cfg['bot_active']}")

                    if is_new_candle:
                        last_processed_ts = signal_key
                        buy_signal, sell_signal = buy[-1], sell[-1]
                        if cfg.get("ut_invert_direction", False):
                            buy_signal, sell_signal = sell_signal, buy_signal
                        if cfg.get("ut_exit_trigger", "candle_close") == "candle_close":
                            await check_ut_exit(symbol, buy_signal, sell_signal, price)
                        if cfg.get("ut_entry_trigger", "candle_close") == "candle_close":
                            await check_ut_entry(symbol, buy_signal, sell_signal, price)
                elif due_heartbeat:
                    last_heartbeat = now
                    if not closed_ts:
                        debug_log(f"⏳ [{symbol}] UT-Bot wartet: keine Kerzen erhalten (Auflösung {resolution})")
                    else:
                        debug_log(f"⏳ [{symbol}] UT-Bot wartet: zu wenig Kerzen ({len(closed_c)}/{min_needed + 1} nötig)")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] UT-Bot-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

        await asyncio.sleep(5)


def _ht_reset_state(st):
    """Setzt alle SL-/TP-Preise und Teilverkauf-Flags zurueck (bei jedem vollstaendigen
    Ausstieg noetig, egal ob durch SL, TP3 oder Flip-Exit)."""
    st["ht_sl_price"] = None
    st["ht_tp1_price"] = None
    st["ht_tp2_price"] = None
    st["ht_tp3_price"] = None
    st["ht_tp1_done"] = False
    st["ht_tp2_done"] = False


async def check_ht_sl_tp(symbol, price):
    """Prueft SL sowie die drei Teilgewinn-Stufen TP1/TP2/TP3 aus dem Original-Skript (dort nur
    Statistik-Tracking, hier als echte Teilverkaeufe umgesetzt - siehe Skript-Tooltip 'Base Risk':
    'Sets SL distance from HalfTrend line, and TP1 distance from entry. TP2 is 2x, TP3 is 3x.'):
    TP1 -> Teilverkauf (ht_tp1_close_pct % der Position) + SL springt auf Ø-Einstieg (Break-Even),
    TP2 -> weiterer Teilverkauf (ht_tp2_close_pct % der VERBLEIBENDEN Position),
    TP3 -> Rest vollstaendig schliessen. SL wird zuerst geprueft (hat Vorrang vor TP-Treffern im
    selben Tick)."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if st["position"] is None or price is None:
        return
    pos = st["position"]

    sl_price = st.get("ht_sl_price")
    if sl_price is not None:
        hit_sl = (pos == "long" and price <= sl_price) or (pos == "short" and price >= sl_price)
        if hit_sl:
            reason = "BREAKEVEN" if st.get("ht_tp1_done") else "SL"
            debug_log(f"🚪 [{symbol}] HalfTrend {reason}: {pos.upper()} @ {price} (Ziel war {round(sl_price, 4)})")
            await execute_exit(symbol, price, reason)
            st["ht_sl_cooldown_until"] = time.time() + cfg.get("ht_sl_cooldown_seconds", 30)
            _ht_reset_state(st)
            return

    if not cfg.get("ht_tp_enabled", True):
        return

    if not st.get("ht_tp1_done") and st.get("ht_tp1_price") is not None:
        tp1_price = st["ht_tp1_price"]
        if (pos == "long" and price >= tp1_price) or (pos == "short" and price <= tp1_price):
            fraction = cfg.get("ht_tp1_close_pct", 33) / 100
            ok = await execute_partial_exit(symbol, price, fraction, "TP1")
            if ok:
                st["ht_tp1_done"] = True
                st["ht_sl_price"] = st["avg_entry_price"]  # Break-Even
                debug_log(f"📡 [{symbol}] HalfTrend TP1 erreicht - SL auf Break-Even ({round(st['avg_entry_price'],4)}) gesetzt")
        return

    if not st.get("ht_tp2_done") and st.get("ht_tp2_price") is not None:
        tp2_price = st["ht_tp2_price"]
        if (pos == "long" and price >= tp2_price) or (pos == "short" and price <= tp2_price):
            fraction = cfg.get("ht_tp2_close_pct", 50) / 100
            ok = await execute_partial_exit(symbol, price, fraction, "TP2")
            if ok:
                st["ht_tp2_done"] = True
        return

    tp3_price = st.get("ht_tp3_price")
    if tp3_price is not None:
        if (pos == "long" and price >= tp3_price) or (pos == "short" and price <= tp3_price):
            debug_log(f"🚪 [{symbol}] HalfTrend TP3 (Rest): {pos.upper()} @ {price}")
            await execute_exit(symbol, price, "TP3")
            _ht_reset_state(st)


async def check_ht_entry(symbol, buy_signal, sell_signal, price, atr2_now):
    """Einstieg beim HalfTrend-Flip-Signal. Setzt bei erfolgreichem Einstieg SL sowie alle drei
    TP-Stufen fest (ATR2 zum Einstiegszeitpunkt * Channel-Deviation bzw. Base-Risk-Multiplikator,
    TP2 = 2x, TP3 = 3x Base-Risk-Abstand) - bleiben bis zum jeweiligen Treffer unveraendert
    (kein Nachziehen), analog zum Original-Skript."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or st["position"] is not None or price is None:
        return
    if time.time() < st.get("ht_sl_cooldown_until", 0.0):
        return
    if not (buy_signal or sell_signal):
        return
    direction = "long" if buy_signal else "short"
    debug_log(f"📡 [{symbol}] HalfTrend Signal: {direction.upper()} @ {price}")
    await execute_entry(symbol, direction, price, is_add_on=False)
    if st["position"] is None:
        return  # Einstieg (z.B. dry_run-Fehler) hat nicht geklappt
    _ht_reset_state(st)
    if cfg.get("ht_sl_enabled", True) and atr2_now is not None:
        dist_sl = atr2_now * cfg.get("ht_channel_deviation", 2.0)
        st["ht_sl_price"] = price - dist_sl if direction == "long" else price + dist_sl
    if cfg.get("ht_tp_enabled", True) and atr2_now is not None:
        dist = atr2_now * cfg.get("ht_base_risk_mult", 3.0)
        if direction == "long":
            st["ht_tp1_price"] = price + dist
            st["ht_tp2_price"] = price + dist * 2
            st["ht_tp3_price"] = price + dist * 3
        else:
            st["ht_tp1_price"] = price - dist
            st["ht_tp2_price"] = price - dist * 2
            st["ht_tp3_price"] = price - dist * 3


async def check_ht_exit(symbol, buy_signal, sell_signal, price):
    """Ausstieg immer beim Gegen-Signal - Flip-System wie Chandelier/UT-Bot, unabhaengig davon,
    welche TP-Stufe gerade aktiv ist (schliesst dann den kompletten Rest der Position)."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or st["position"] is None or price is None:
        return
    if st["position"] == "long" and sell_signal:
        debug_log(f"🚪 [{symbol}] HalfTrend Exit: LONG @ {price} (Sell-Signal)")
        await execute_exit(symbol, price, "HT-FLIP-EXIT")
        _ht_reset_state(st)
    elif st["position"] == "short" and buy_signal:
        debug_log(f"🚪 [{symbol}] HalfTrend Exit: SHORT @ {price} (Buy-Signal)")
        await execute_exit(symbol, price, "HT-FLIP-EXIT")
        _ht_reset_state(st)


async def ht_poll_loop(symbol):
    """HalfTrend (portiert aus 'HalfTrend Long/Short Signal Engine [BigBeluga]', Basis: everget's
    HalfTrend). Swing-Hoch/-Tief-Vergleich gegen SMA(amplitude) bestimmt den Trend, Flip = Signal.
    ATR-Periode ist im Original fest auf 100. SL (Channel-Deviation * ATR2) und TP (Base-Risk-
    Multiplikator * ATR2) sind optional und werden einmalig bei Einstieg berechnet, Ausstieg
    sonst immer beim Gegen-Signal (Flip-System wie Chandelier/UT-Bot). Ein-/Ausstieg je einzeln
    tick-/kerzenbasiert, alle Zeitrahmen, Backtest-faehig."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "halftrend" and cfg["bot_active"]:
                resolution = cfg["ht_resolution"]
                amplitude = cfg["ht_amplitude"]
                min_needed = max(100, amplitude) + 5
                needed_bars = min(1000, max(min_needed * 2, 200))
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
                        closed_ts, closed_h, closed_l, closed_c = timestamps[:-1], highs[:-1], lows[:-1], closes[:-1]
                    else:
                        closed_ts = None

                now = time.time()
                due_heartbeat = now - last_heartbeat > 300

                if closed_ts and len(closed_c) > min_needed:
                    signal_key = closed_ts[-1]
                    is_new_candle = last_processed_ts != signal_key
                    price = st["last_price"] if st["last_price"] is not None else closed_c[-1]

                    keep = min_needed + 5
                    st["ht_highs"] = closed_h[-keep:]
                    st["ht_lows"] = closed_l[-keep:]
                    st["ht_closes"] = closed_c[-keep:]

                    ht_line, trend, atr2 = compute_halftrend(closed_h, closed_l, closed_c, amplitude, cfg["ht_channel_deviation"])
                    invert = cfg.get("ht_invert_direction", False)
                    dir_now = trend[-1]
                    st["ht_direction"] = (1 if dir_now == 0 else -1) * (-1 if invert else 1)
                    st["ht_atr2_last"] = atr2[-1]

                    if due_heartbeat:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] HalfTrend aktiv: Trend={'AUFWÄRTS' if dir_now==0 else 'ABWÄRTS'}, "
                                  f"ATR2={round(atr2[-1],4)}, Preis={closed_c[-1]}, Kerzen={len(closed_c)}, bot_active={cfg['bot_active']}")

                    if is_new_candle:
                        last_processed_ts = signal_key
                        buy_signal = trend[-1] == 0 and trend[-2] == 1
                        sell_signal = trend[-1] == 1 and trend[-2] == 0
                        if invert:
                            buy_signal, sell_signal = sell_signal, buy_signal
                        if cfg.get("ht_exit_trigger", "candle_close") == "candle_close":
                            await check_ht_exit(symbol, buy_signal, sell_signal, price)
                        if cfg.get("ht_entry_trigger", "candle_close") == "candle_close":
                            await check_ht_entry(symbol, buy_signal, sell_signal, price, atr2[-1])

                    await check_ht_sl_tp(symbol, price)
                elif due_heartbeat:
                    last_heartbeat = now
                    if not closed_ts:
                        debug_log(f"⏳ [{symbol}] HalfTrend wartet: keine Kerzen erhalten (Auflösung {resolution})")
                    else:
                        debug_log(f"⏳ [{symbol}] HalfTrend wartet: zu wenig Kerzen ({len(closed_c)}/{min_needed + 1} nötig)")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] HalfTrend-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

        await asyncio.sleep(5)


async def check_wtc_entry(symbol, buy_signal, sell_signal, price):
    """Einstieg beim Buy/Sell-Signal. Erweiterungen:
    - Richtungsmodus (wtc_direction_mode): 'long_only'/'short_only' blendet die jeweils andere
      Einstiegsrichtung aus (Exit bei Gegen-Signal bleibt trotzdem aktiv, dreht aber nicht um).
    - Optionaler SuperTrend-Fusion-Richtungsfilter auf hoeherem Zeitrahmen (wie bei Chandelier
      Exit): stimmt die Richtung nicht mit dem SuperTrend-Bias ueberein, wird das Signal als
      'pending' gemerkt statt verworfen - siehe check_wtc_pending.
    Der Nachkauf (DCA) laeuft NICHT hierueber, sondern ueber check_wtc_dca - Kreuzungs-Signale
    sind einmalige Ereignisse und koennen strukturell nicht ein zweites Mal in dieselbe Richtung
    feuern, waehrend die Position noch offen ist (dafuer muesste erst das Gegen-Signal kommen,
    das die Position ohnehin schliessen wuerde)."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or price is None or st["position"] is not None:
        return
    if cfg.get("wtc_sl_enabled", False) and time.time() < st.get("wtc_sl_cooldown_until", 0.0):
        return
    if not (buy_signal or sell_signal):
        return

    direction = "long" if buy_signal else "short"
    dir_mode = cfg.get("wtc_direction_mode", "both")
    if dir_mode == "long_only" and direction == "short":
        return
    if dir_mode == "short_only" and direction == "long":
        return

    stf_filter_enabled = cfg.get("wtc_stf_filter_enabled", False)

    if not stf_filter_enabled:
        st["wtc_pending_direction"] = None
        debug_log(f"📡 [{symbol}] WaveTrend-Cross Signal: {direction.upper()} @ {price}")
        await execute_entry(symbol, direction, price, is_add_on=False)
        return

    bias = st.get("wtc_stf_bias")
    if bias == direction:
        st["wtc_pending_direction"] = None
        debug_log(f"📡 [{symbol}] WaveTrend-Cross Signal: {direction.upper()} @ {price} (SuperTrend-Filter bestätigt)")
        await execute_entry(symbol, direction, price, is_add_on=False)
    else:
        st["wtc_pending_direction"] = direction
        debug_log(f"⏸️ [{symbol}] WaveTrend-Cross Signal {direction.upper()} wartet auf SuperTrend-Bestätigung (aktuell: {bias})")


async def check_wtc_pending(symbol, buy_signal, sell_signal, price):
    """Prueft jeden Zyklus, ob ein wartendes Signal jetzt durch den SuperTrend-Filter bestaetigt
    wird - analog zu check_ce_pending bei Chandelier Exit. 'buy_signal'/'sell_signal' hier sind
    die AKTUELLEN Werte an dieser Kerze/diesem Tick (nicht die des urspruenglichen Signals) -
    wird verworfen, sobald das WaveTrend-Signal zwischenzeitlich in die Gegenrichtung gedreht hat."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or st["position"] is not None or price is None:
        return
    pending = st.get("wtc_pending_direction")
    if not pending or not cfg.get("wtc_stf_filter_enabled", False):
        return
    if sell_signal and pending == "long":
        st["wtc_pending_direction"] = None
        return
    if buy_signal and pending == "short":
        st["wtc_pending_direction"] = None
        return
    bias = st.get("wtc_stf_bias")
    if bias == pending:
        st["wtc_pending_direction"] = None
        debug_log(f"📡 [{symbol}] WaveTrend-Cross Pending-Order ausgelöst: {pending.upper()} @ {price} (SuperTrend jetzt bestätigt)")
        await execute_entry(symbol, pending, price, is_add_on=False)


def _wtc_dca_condition(wt1, wt2, i, direction, cfg):
    """Level-basierte Nachkauf-Bedingung (anders als das einmalige Kreuzungs-Signal): 'noch
    long-guenstig' heisst wt2 weiterhin im ueberverkauft-Bereich (bzw. wt1 noch ueber wt2, wenn
    die OB/OS-Pflicht aus ist) - das kann ueber mehrere Kerzen hinweg stabil wahr bleiben, im
    Gegensatz zur Kreuzung selbst, die nur einmalig feuert."""
    require_obos = cfg.get("wtc_require_obos", True)
    if direction == "long":
        if require_obos:
            return wt2[i] <= cfg.get("wtc_os_level", -53)
        return wt1[i] > wt2[i]
    else:
        if require_obos:
            return wt2[i] >= cfg.get("wtc_ob_level", 53)
        return wt1[i] < wt2[i]


async def check_wtc_dca(symbol, wt1, wt2, price):
    """Nachkauf: prueft JEDEN Zyklus (nicht nur bei neuer Kerze), ob die Nachkauf-Bedingung
    weiterhin erfuellt ist, und kauft mit Mindestabstand (wtc_dca_cooldown_seconds) nach, bis
    wtc_dca_max_entries Stufen erreicht sind."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or st["position"] is None or price is None:
        return
    if not cfg.get("wtc_dca_enabled", False):
        return
    direction = st["position"]
    if st["entry_count"] >= cfg.get("wtc_dca_max_entries", 10):
        return
    cooldown = cfg.get("wtc_dca_cooldown_seconds", 60)
    if time.time() - st.get("wtc_last_dca_ts", 0.0) < cooldown:
        return
    if not wt1 or not _wtc_dca_condition(wt1, wt2, len(wt1) - 1, direction, cfg):
        return
    if cfg.get("wtc_stf_filter_enabled", False) and st.get("wtc_stf_bias") != direction:
        return
    st["wtc_last_dca_ts"] = time.time()
    debug_log(f"➕ [{symbol}] WaveTrend-Cross Nachkauf #{st['entry_count'] + 1}: {direction.upper()} @ {price}")
    await execute_entry(symbol, direction, price, is_add_on=True)


def compute_sg_tp_abs(reference_price, cfg):
    """Analog zu compute_step_abs beim Grid-Bot, nur mit eigenen sg_-Feldern - so kann
    Signal-Grid unabhaengig vom normalen Grid konfiguriert werden."""
    if cfg.get("sg_tp_mode", "pct") == "usd":
        return cfg.get("sg_tp_step_usd", 5.0)
    return reference_price * (cfg.get("sg_tp_step_pct", 1.0) / 100)


def compute_sg_signal(highs, lows, closes, cfg):
    """Liefert (buy_entry, sell_entry) fuer die LETZTE Kerze - ein ECHTES, einmaliges
    Signal-Ereignis (Kreuzung bzw. Schwellenwert-Durchbruch), kein anhaltender Zustand.
    Bei Signal-Grid loest das sowohl den Erst-Einstieg als auch jeden Nachkauf aus - das
    funktioniert, WEIL es (anders als bei WaveTrend-Cross selbst) keinen Flip-Exit gibt: die
    Position bleibt offen, waehrend wt1/wt2 mehrfach hin- und herkreuzen koennen, und jede
    neue Kreuzung in dieselbe Richtung ist ein echtes 'naechstes Signal'."""
    source = cfg.get("sg_signal_source", "wavetrend")
    if source == "wavetrend":
        wt1, wt2 = compute_wavetrend(highs, lows, closes, cfg["wtc_channel_length"], cfg["wtc_average_length"], cfg["wtc_ma_length"])
        buy_entry, sell_entry = _wtc_signals(wt1, wt2, cfg)
    else:  # "zscore"
        z = compute_zscore_trend(closes, cfg["zscore_lookback_period"], cfg["zscore_ema_smooth"])
        threshold = cfg["zscore_threshold"]
        i = len(z) - 1
        buy_entry = z[i - 1] <= threshold and z[i] > threshold
        sell_entry = z[i - 1] >= -threshold and z[i] < -threshold
    if cfg.get("sg_invert_direction", False):
        buy_entry, sell_entry = sell_entry, buy_entry
    return buy_entry, sell_entry


async def check_sg_signal(symbol, buy_entry, sell_entry, price):
    """Reagiert auf ein ECHTES Signal (Kreuzung/Schwellenwert-Durchbruch), nicht auf eine
    anhaltende Bedingung. Ohne Position: Erst-Einstieg. Mit Position in DERSELBEN Richtung:
    Nachkauf (bis sg_max_nachkauf Stufen, mit Mindestabstand als Sicherheitsnetz - Kreuzungen
    sind aber ohnehin selten, nicht jeden Tick). Gegen-Signal waehrend eine Position offen ist:
    wird ignoriert (kein Flip-Exit bei Signal-Grid, nur TP)."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or price is None:
        return
    if not (buy_entry or sell_entry):
        return
    direction = "long" if buy_entry else "short"

    if st["position"] is None:
        debug_log(f"📡 [{symbol}] Signal-Grid Einstieg: {direction.upper()} @ {price}")
        await execute_entry(symbol, direction, price, is_add_on=False)
        return

    if st["position"] != direction:
        return

    max_nachkauf = cfg.get("sg_max_nachkauf", 0)
    if max_nachkauf and st["entry_count"] >= max_nachkauf:
        return
    cooldown = cfg.get("sg_dca_cooldown_seconds", 10)
    if time.time() - st.get("sg_last_dca_ts", 0.0) < cooldown:
        return
    st["sg_last_dca_ts"] = time.time()
    debug_log(f"➕ [{symbol}] Signal-Grid Nachkauf #{st['entry_count'] + 1}: {direction.upper()} @ {price}")
    await execute_entry(symbol, direction, price, is_add_on=True)


async def check_sg_tp(symbol, price):
    """Ausstieg: kein Flip-Exit, nur TP. Zwei Modi:
    - '%': Preis-Abstand vom Ø-Einstieg (wie beim normalen Grid).
    - '$': ECHTER Gewinn in Dollar auf die gesamte (ggf. mehrfach nachgekaufte) Position -
      konsistent mit allen anderen Strategien im Bot (Trend-Meter, Chandelier etc.), NICHT ein
      reiner Kursabstand wie beim alten Grid (der bei einem teuren Coin wie BTC nur Cent-Betraege
      bedeuten wuerde)."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or st["position"] is None or price is None:
        return
    if cfg.get("sg_tp_mode", "pct") == "usd":
        entry = st["avg_entry_price"]
        pnl_usd = (price - entry) * st["total_coin_size"] if st["position"] == "long" else (entry - price) * st["total_coin_size"]
        if pnl_usd >= abs(cfg.get("sg_tp_step_usd", 5.0)):
            await execute_exit(symbol, price, "TP")
        return
    tp_abs = compute_sg_tp_abs(st["avg_entry_price"], cfg)
    if st["position"] == "long" and price >= st["avg_entry_price"] + tp_abs:
        await execute_exit(symbol, price, "TP")
    elif st["position"] == "short" and price <= st["avg_entry_price"] - tp_abs:
        await execute_exit(symbol, price, "TP")


async def oms_rsi_poll_loop(symbol):
    """Separater, traeger laufender Poll-Loop nur fuer den optionalen RSI-Regime-Filter von
    OBI-Momentum-Scalp (RSI < Mittellinie -> nur Short erlaubt, RSI > Mittellinie -> nur Long
    erlaubt). Laeuft unabhaengig vom tick-schnellen OBI/CVD-Signal, da RSI auf Kerzen (nicht auf
    einzelnen Trades) beruht - ein 5-Sekunden-Takt waere unnoetig oft fuer einen 1-Minuten-RSI.
    Wird nur abgefragt, wenn der Filter tatsaechlich aktiviert ist (kein unnoetiger API-Traffic)."""
    b = BOTS[symbol]
    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "oms_scalp" and cfg["bot_active"] and cfg.get("oms_rsi_filter_enabled", False):
                resolution = cfg.get("oms_rsi_resolution", "1m")
                period = cfg.get("oms_rsi_period", 14)
                needed_bars = min(500, max(period * 3, 60))
                st = b["state"]

                if resolution in SUB_MINUTE_RESOLUTIONS:
                    local = get_seconds_candles(st, SUB_MINUTE_RESOLUTIONS[resolution], needed_bars)
                    closed_c = local[4] if local else None
                else:
                    data = await fetch_candles_binance_multi(symbol, resolution, count_back=needed_bars)
                    closed_c = data[4][:-1] if data else None

                if closed_c and len(closed_c) > period:
                    rsi_series = compute_rsi(closed_c, period)
                    st["oms_rsi"] = round(rsi_series[-1], 2)
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] OMS-RSI-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        await asyncio.sleep(10)


SCALP_BOARD_TIMEFRAMES = [("30s", 30), ("45s", 45), ("60s", 60)]


async def scalp_board_poll_loop(symbol):
    """Manuelles Scalp-Board: RSI (kurze Periode), Stochastic und MACD auf 30s/45s/60s
    PARALLEL berechnet, plus CVD ueber dieselben drei Fenster (aus dem sowieso schon global
    gepflegten Trade-Puffer, siehe _cvd_ratio_over). Bewusst UNABHAENGIG vom aktuellen
    entry_mode - das ist ein reines Beobachtungs-/Handwerkszeug fuer manuelles Scalping, egal
    welche automatische Strategie gerade laeuft. Laeuft nur, solange der Bot fuer den Coin
    aktiv ist (bot_active), da die 30s/45s-Kerzen aus demselben 1s-Puffer stammen, der aus
    Kostengruenden nur bei aktivem Bot gefuellt wird (siehe binance_1s_poll_loop)."""
    b = BOTS[symbol]
    while True:
        try:
            cfg = b["config"]
            if cfg["bot_active"]:
                st = b["state"]
                board = {}
                for label, seconds in SCALP_BOARD_TIMEFRAMES:
                    if seconds == 60:
                        data = await fetch_candles_binance_multi(symbol, "1m", count_back=120)
                        candles = (data[2][:-1], data[3][:-1], data[4][:-1]) if data else None
                    else:
                        local = get_seconds_candles(st, seconds, 120)
                        candles = (local[2], local[3], local[4]) if local else None

                    if candles and len(candles[2]) > 20:
                        h, l, c = candles
                        rsi_series = compute_rsi(c, 8)
                        k, d = compute_stochastic(h, l, c, 5, 3, 3)
                        macd, macd_sig = compute_macd_line_and_signal(c, 5, 13, 3)
                        board[label] = {
                            "rsi": round(rsi_series[-1], 1),
                            "stoch_k": round(k[-1], 1), "stoch_d": round(d[-1], 1),
                            "macd_hist": round(macd[-1] - macd_sig[-1], 5),
                            "cvd": _cvd_ratio_over(st, seconds),
                        }
                    else:
                        board[label] = None
                st["scalp_board"] = board
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Scalp-Board-Berechnung fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        await asyncio.sleep(5)


async def sg_poll_loop(symbol):
    """Signal-Grid: Grid-Mechanik dupliziert (kein Flip-Exit, TP als %/$ vom Ø-Einstieg,
    Nachkauf bis max. Stufen) - aber Ein-/Nachkauf werden nicht durch Preisabstand ausgeloest,
    sondern durch ein Indikator-Signal (WaveTrend-Kreuzung oder Z-Score-Schwellenwert-
    Durchbruch, umschaltbar ueber sg_signal_source). Loest das Problem reiner Flip-Strategien,
    bei denen ein einzelnes Gegen-Signal sofort die komplette (evtl. mehrfach nachgekaufte)
    Position schliesst."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "signal_grid" and cfg["bot_active"]:
                st = b["state"]
                resolution = cfg["sg_resolution"]
                source = cfg.get("sg_signal_source", "wavetrend")
                if source == "wavetrend":
                    min_needed = max(cfg["wtc_channel_length"], cfg["wtc_average_length"], cfg["wtc_ma_length"]) * 3 + 5
                else:
                    min_needed = cfg["zscore_lookback_period"] + 5
                needed_bars = min(1000, max(min_needed * 2, 80))

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

                now = time.time()
                due_heartbeat = now - last_heartbeat > 300

                if closed_ts and len(closed_c) > min_needed:
                    signal_key = closed_ts[-1]
                    is_new_candle = last_processed_ts != signal_key
                    price = st["last_price"] if st["last_price"] is not None else closed_c[-1]

                    keep = min_needed + 5
                    st["sg_highs"] = closed_h[-keep:]
                    st["sg_lows"] = closed_l[-keep:]
                    st["sg_closes"] = closed_c[-keep:]

                    buy_entry, sell_entry = compute_sg_signal(closed_h, closed_l, closed_c, cfg)

                    if due_heartbeat:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] Signal-Grid aktiv: Quelle={source}, Preis={closed_c[-1]}, Kerzen={len(closed_c)}, Stufe={st['entry_count']}, bot_active={cfg['bot_active']}")

                    if is_new_candle:
                        last_processed_ts = signal_key
                        if cfg.get("sg_entry_trigger", "candle_close") == "candle_close":
                            await check_sg_signal(symbol, buy_entry, sell_entry, price)

                    await check_sg_tp(symbol, price)
                elif due_heartbeat:
                    last_heartbeat = now
                    if not closed_ts:
                        debug_log(f"⏳ [{symbol}] Signal-Grid wartet: keine Kerzen erhalten (Auflösung {resolution})")
                    else:
                        debug_log(f"⏳ [{symbol}] Signal-Grid wartet: zu wenig Kerzen ({len(closed_c)}/{min_needed + 1} nötig)")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Signal-Grid-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

        await asyncio.sleep(5)


async def check_wtc_exit(symbol, buy_signal, sell_signal, price):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or st["position"] is None or price is None:
        return
    if st["position"] == "long" and sell_signal:
        debug_log(f"🚪 [{symbol}] WaveTrend-Cross Exit: LONG @ {price} (Sell-Signal)")
        await execute_exit(symbol, price, "WTC-FLIP-EXIT")
    elif st["position"] == "short" and buy_signal:
        debug_log(f"🚪 [{symbol}] WaveTrend-Cross Exit: SHORT @ {price} (Buy-Signal)")
        await execute_exit(symbol, price, "WTC-FLIP-EXIT")


def _wtc_signals(wt1, wt2, cfg):
    """Kreuzung wt1/wt2 an der letzten Kerze, optional nur im ueberkauft/ueberverkauft-Bereich
    (wie im Original-Skript per 'wtOversold'/'wtOverbought' vorgesehen)."""
    n = len(wt1)
    cross_up = wt1[n - 2] <= wt2[n - 2] and wt1[n - 1] > wt2[n - 1]
    cross_down = wt1[n - 2] >= wt2[n - 2] and wt1[n - 1] < wt2[n - 1]
    require_obos = cfg.get("wtc_require_obos", True)
    buy_signal = cross_up and (not require_obos or wt2[n - 1] <= cfg.get("wtc_os_level", -53))
    sell_signal = cross_down and (not require_obos or wt2[n - 1] >= cfg.get("wtc_ob_level", 53))
    return buy_signal, sell_signal


async def wtc_poll_loop(symbol):
    """WaveTrend-Cross (portiert aus dem Cipher-B-WaveTrend-Baustein des 'Wave Cipher SMC Flow
    System'): wt1 kreuzt wt2, optional nur wenn wt2 gerade im ueberkauft/ueberverkauft-Bereich
    steht (Standard-Level wie im Original: +53/-53). Kanal-/Durchschnitts-/Glaettungslaenge
    alle einstellbar. Ausstieg beim Gegen-Signal (Flip-System). Optional SL (mit Cooldown) + TP
    fest $, Invertiert-Modus, Ein-/Ausstieg je einzeln tick-/kerzenbasiert, alle Zeitrahmen,
    Backtest-faehig."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "wavetrend_cross" and cfg["bot_active"]:
                resolution = cfg["wtc_resolution"]
                min_needed = max(cfg["wtc_channel_length"], cfg["wtc_average_length"], cfg["wtc_ma_length"]) * 3 + 5
                needed_bars = min(1000, max(min_needed * 2, 80))
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
                        closed_ts, closed_h, closed_l, closed_c = timestamps[:-1], highs[:-1], lows[:-1], closes[:-1]
                    else:
                        closed_ts = None

                now = time.time()
                due_heartbeat = now - last_heartbeat > 300

                stf_filter_enabled = cfg.get("wtc_stf_filter_enabled", False)
                if stf_filter_enabled:
                    stf_resolution = cfg.get("wtc_stf_resolution", "5m")
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
                            st["wtc_stf_bias"] = "long" if stf_state["direction"] == -1 else "short"
                else:
                    st["wtc_stf_bias"] = None

                if closed_ts and len(closed_c) > min_needed:
                    signal_key = closed_ts[-1]
                    is_new_candle = last_processed_ts != signal_key
                    price = st["last_price"] if st["last_price"] is not None else closed_c[-1]

                    keep = min_needed + 5
                    st["wtc_highs"] = closed_h[-keep:]
                    st["wtc_lows"] = closed_l[-keep:]
                    st["wtc_closes"] = closed_c[-keep:]

                    wt1, wt2 = compute_wavetrend(closed_h, closed_l, closed_c, cfg["wtc_channel_length"], cfg["wtc_average_length"], cfg["wtc_ma_length"])
                    st["wtc_wt1"], st["wtc_wt2"] = wt1[-1], wt2[-1]

                    if due_heartbeat:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] WaveTrend-Cross aktiv: wt1={round(wt1[-1],2)} wt2={round(wt2[-1],2)}, STF-Filter={'an' if stf_filter_enabled else 'aus'}, STF-Bias={st.get('wtc_stf_bias')}, Pending={st.get('wtc_pending_direction')}, Preis={closed_c[-1]}, Kerzen={len(closed_c)}, bot_active={cfg['bot_active']}")

                    buy_signal, sell_signal = _wtc_signals(wt1, wt2, cfg)
                    if cfg.get("wtc_invert_direction", False):
                        buy_signal, sell_signal = sell_signal, buy_signal

                    if is_new_candle:
                        last_processed_ts = signal_key
                        if cfg.get("wtc_exit_trigger", "candle_close") == "candle_close":
                            await check_wtc_exit(symbol, buy_signal, sell_signal, price)
                        if cfg.get("wtc_entry_trigger", "candle_close") == "candle_close":
                            await check_wtc_entry(symbol, buy_signal, sell_signal, price)

                    # Pending-Order jeden Zyklus pruefen, nicht nur bei neuer Kerze
                    await check_wtc_pending(symbol, buy_signal, sell_signal, price)
                    # Nachkauf jeden Zyklus pruefen (level-basiert, nicht an das Kreuzungs-Signal gebunden)
                    await check_wtc_dca(symbol, wt1, wt2, price)
                elif due_heartbeat:
                    last_heartbeat = now
                    if not closed_ts:
                        debug_log(f"⏳ [{symbol}] WaveTrend-Cross wartet: keine Kerzen erhalten (Auflösung {resolution})")
                    else:
                        debug_log(f"⏳ [{symbol}] WaveTrend-Cross wartet: zu wenig Kerzen ({len(closed_c)}/{min_needed + 1} nötig)")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] WaveTrend-Cross-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

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

    if cfg["entry_mode"] == "oms_scalp":
        await handle_oms_signal_check(symbol)
        return
    if cfg["entry_mode"] != "obi_scalp":
        return

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


# ========== OBI-MOMENTUM-SCALP (oms_) - eigenstaendige neue Strategie ==========
# Einstieg: OBI ueber drei Zeitfenster (muessen uebereinstimmen, wie OBI-Scalp) UND CVD
# (echte Trade-Tape-Richtung aus dem Trade-Kanal - is_maker_ask gibt an, ob der Taker
# gekauft oder verkauft hat) bestaetigt dieselbe Richtung. Optionaler Funding-Filter
# verhindert Nachlegen in eine bereits ueberfuellte Richtung. Exit: TP1 (Teilverkauf) +
# enger Trailing-Stop auf den Rest, SL von Anfang an ein fester $-Betrag auf die GESAMTE
# Position (bewusst NICHT die Liquidation als Stop). Nachkauf: max. N Stufen, fallende
# Groesse, nur wenn das Signal nach spuerbarem Pullback erneut bestaetigt - kein blindes
# Preis-Grid. st["oms_signal"] wird IMMER aktualisiert (auch bei bot_active=False oder
# Cooldown) - das ist das Trend-Meter zum manuellen Nachhandeln im Dashboard.

def update_oms_cvd(symbol, trades):
    """CVD (Cumulative Volume Delta) aus dem echten Lighter-Trade-Tape: is_maker_ask=True
    heisst, die Ask-Seite war die ruhende (Maker-)Order - der Taker hat also AGGRESSIV
    GEKAUFT (Delta +size). is_maker_ask=False heisst umgekehrt, der Taker hat aggressiv
    VERKAUFT (Delta -size). Als Verhaeltnis (net/vol, -1..1) gespeichert statt absoluter
    Groesse - bleibt so vergleichbar mit OBI und unabhaengig vom Volumen-Niveau des Coins."""
    if symbol not in BOTS or not trades:
        return
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    now = time.time()
    buf = st["oms_cvd_buffer"]
    for t in trades:
        try:
            size = float(t.get("size", 0) or 0)
        except (TypeError, ValueError):
            continue
        if size <= 0:
            continue
        is_maker_ask = bool(t.get("is_maker_ask", False))
        delta = size if is_maker_ask else -size
        buf.append((delta, size, now))
    # Puffer wird auf MINDESTENS 60s vorgehalten (nicht nur das konfigurierte OMS-Fenster) -
    # das manuelle Scalp-Board braucht dieselben Rohdaten fuer eigene 30s/45s/60s-CVD-Werte,
    # ohne einen zweiten, redundanten Trade-Puffer zu pflegen.
    window = max(cfg.get("oms_cvd_window_seconds", 10), 60)
    cutoff = now - window
    buf = [d for d in buf if d[2] >= cutoff]
    st["oms_cvd_buffer"] = buf
    net = sum(d for d, v, ts in buf)
    vol = sum(v for d, v, ts in buf)
    st["oms_cvd_ratio"] = round(0.0 if vol == 0 else net / vol, 4)


def _cvd_ratio_over(st, window_seconds):
    """Berechnet CVD-Verhaeltnis ueber ein BELIEBIGES Zeitfenster aus demselben Rohpuffer, den
    update_oms_cvd() sowieso schon fuer JEDES Symbol kontinuierlich pflegt (unabhaengig vom
    aktuellen entry_mode) - fuers Scalp-Board, das 30s/45s/60s parallel braucht."""
    now = time.time()
    cutoff = now - window_seconds
    buf = [d for d in st.get("oms_cvd_buffer", []) if d[2] >= cutoff]
    net = sum(d for d, v, ts in buf)
    vol = sum(v for d, v, ts in buf)
    return round(0.0 if vol == 0 else net / vol, 4)


def update_oms_liquidations(symbol, liq_trades):
    """Liquidationen aus dem trade-Kanal (separates 'liquidation_trades'-Array, getrennt von den
    normalen Trades). Gleiche Ratio-Logik wie CVD (is_maker_ask=True -> Short wurde zwangsweise
    zugekauft -> bullischer Druck, is_maker_ask=False -> Long wurde zwangsweise verkauft ->
    baerischer Druck), aber laengeres Zeitfenster als CVD, da Liquidationen seltener sind als
    normale Trades und ein kurzes Fenster meist einfach leer waere."""
    if symbol not in BOTS or not liq_trades:
        return
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    now = time.time()
    buf = st["oms_liq_buffer"]
    for t in liq_trades:
        try:
            size = float(t.get("size", 0) or 0)
        except (TypeError, ValueError):
            continue
        if size <= 0:
            continue
        is_maker_ask = bool(t.get("is_maker_ask", False))
        delta = size if is_maker_ask else -size
        buf.append((delta, size, now))
    window = cfg.get("oms_liq_window_seconds", 60)
    cutoff = now - window
    buf = [d for d in buf if d[2] >= cutoff]
    st["oms_liq_buffer"] = buf
    net = sum(d for d, v, ts in buf)
    vol = sum(v for d, v, ts in buf)
    st["oms_liq_ratio"] = round(0.0 if vol == 0 else net / vol, 4)
    st["oms_liq_count"] = len(buf)


def update_oms_oi(symbol, oi_now):
    """Open Interest kombiniert mit der Preisrichtung im selben Zeitfenster (aus dem ohnehin
    gepflegten oms_price_history), um herzuleiten, welche Seite gerade dominiert:
    Preis rauf + OI rauf = neue Longs (starkes bullisches Signal, +1.0)
    Preis rauf + OI runter = Short-Eindeckung (schwaecher bullisch, +0.4 - reiner Squeeze,
                              keine neue Ueberzeugung)
    Preis runter + OI rauf = neue Shorts (starkes baerisches Signal, -1.0)
    Preis runter + OI runter = Long-Kapitulation (schwaecher baerisch, -0.4)
    OI aendert sich kaum = neutral (0.0). Open Interest selbst hat keine 'Richtung' (Long- und
    Short-OI sind immer gleich gross) - erst die Kombination mit der Preisrichtung macht daraus
    ein Signal, wer gerade tatsaechlich Position aufbaut statt nur hin- und herzuhandeln."""
    if symbol not in BOTS or oi_now is None:
        return
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    now = time.time()
    hist = st["oms_oi_history"]
    hist.append((now, oi_now))
    window = cfg.get("oms_oi_window_seconds", 30)
    cutoff = now - window
    hist = [h for h in hist if h[0] >= cutoff]
    st["oms_oi_history"] = hist
    st["oms_open_interest"] = oi_now

    if len(hist) < 2:
        st["oms_oi_score"] = None
        return
    oi_old = hist[0][1]
    oi_delta_pct = (oi_now - oi_old) / oi_old if oi_old else 0.0

    price_hist = [(ts, p) for ts, p in st.get("oms_price_history", []) if ts >= cutoff]
    if len(price_hist) < 2:
        st["oms_oi_score"] = None
        return
    price_old, price_now = price_hist[0][1], price_hist[-1][1]
    price_delta_pct = (price_now - price_old) / price_old if price_old else 0.0

    oi_threshold = cfg.get("oms_oi_min_change_pct", 0.001)
    if abs(oi_delta_pct) < oi_threshold or price_delta_pct == 0:
        st["oms_oi_score"] = 0.0
    elif price_delta_pct > 0 and oi_delta_pct > 0:
        st["oms_oi_score"] = 1.0
    elif price_delta_pct > 0 and oi_delta_pct < 0:
        st["oms_oi_score"] = 0.4
    elif price_delta_pct < 0 and oi_delta_pct > 0:
        st["oms_oi_score"] = -1.0
    else:
        st["oms_oi_score"] = -0.4


def _oms_reset_position_state(st):
    st["oms_tp1_done"] = False
    st["oms_trail_price"] = None
    st["oms_dca_count"] = 0
    st["oms_last_entry_price"] = None
    st["oms_last_signal_direction"] = None  # sofort wieder offen fuer ein neues Signal in jede Richtung


def _oms_record_price(st, price):
    """Rollender Preisverlauf fuers Mini-Chart im Dashboard - nach Zeit statt Anzahl begrenzt
    (letzte 15 Minuten), damit die Chart-Breite unabhaengig von der Tick-Frequenz des Coins ist."""
    now = time.time()
    hist = st["oms_price_history"]
    hist.append((now, price))
    cutoff = now - 900
    if len(hist) > 20 and hist[0][0] < cutoff:
        st["oms_price_history"] = [h for h in hist if h[0] >= cutoff]


def _oms_record_marker(st, price, kind):
    """kind: entry_long, entry_short, dca_long, dca_short, exit_sl, exit_tp1, exit_trail"""
    st["oms_markers"].append({"ts": time.time(), "price": price, "kind": kind})
    if len(st["oms_markers"]) > 60:
        st["oms_markers"] = st["oms_markers"][-60:]


async def handle_oms_signal_check(symbol):
    """Wird bei jedem Orderbuch-Update aufgerufen (Buch selbst ist schon von
    handle_obi_order_book_update aktualisiert). Berechnet OBI ueber drei eigene
    Zeitfenster, bestaetigt per CVD, filtert per Funding-Rate, aktualisiert das
    Live-Signal fuers Trend-Meter IMMER - und loest bei aktivem Bot Einstieg/Nachkauf aus."""
    if symbol not in BOTS:
        return
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if cfg["entry_mode"] != "oms_scalp":
        return

    raw_obi = calc_obi(symbol, cfg["oms_levels"])
    now = time.time()
    buf = st["oms_obi_buffer"]
    buf.append((raw_obi, now))
    max_window = max(cfg["oms_window_fast_seconds"], cfg["oms_window_medium_seconds"], cfg["oms_window_slow_seconds"])
    cutoff = now - max_window
    buf = [d for d in buf if d[1] >= cutoff]
    st["oms_obi_buffer"] = buf

    def avg_over(seconds):
        wc = now - seconds
        vals = [v for v, ts in buf if ts >= wc]
        return sum(vals) / len(vals) if vals else 0.0

    fast = avg_over(cfg["oms_window_fast_seconds"])
    medium = avg_over(cfg["oms_window_medium_seconds"])
    slow = avg_over(cfg["oms_window_slow_seconds"])
    st["oms_obi_fast"] = round(fast, 4)
    st["oms_obi_medium"] = round(medium, 4)
    st["oms_obi_slow"] = round(slow, 4)

    threshold = cfg["oms_obi_threshold"]

    def side_of(v):
        if v >= threshold:
            return "long"
        if v <= -threshold:
            return "short"
        return None

    fast_dir, medium_dir, slow_dir = side_of(fast), side_of(medium), side_of(slow)
    obi_direction = fast_dir if (fast_dir is not None and fast_dir == medium_dir == slow_dir) else None
    direction = obi_direction
    # Fuer die Bedingungs-Checkliste im Dashboard: jede Stufe einzeln festhalten, nicht nur
    # das Endergebnis - so sieht man WARUM ein Signal (nicht) durchkommt
    st["oms_obi_direction"] = obi_direction

    # CVD-Bestaetigung: die tatsaechliche Trade-Richtung muss zum Orderbuch-Signal passen,
    # sonst koennte es eine Spoofing-Wand sein (sichtbare Wall, aber niemand handelt dagegen)
    cvd_enabled = cfg.get("oms_cvd_confirm_enabled", True)
    cvd_ok = None
    if obi_direction is not None and cvd_enabled:
        cvd_ratio = st.get("oms_cvd_ratio") or 0.0
        min_ratio = cfg.get("oms_cvd_min_ratio", 0.15)
        cvd_ok = not ((obi_direction == "long" and cvd_ratio < min_ratio) or
                      (obi_direction == "short" and cvd_ratio > -min_ratio))
        if not cvd_ok:
            direction = None
    st["oms_cvd_ok"] = cvd_ok

    # Funding-Filter: nicht in eine bereits ueberfuellte Richtung nachlegen (Squeeze-Risiko)
    funding_enabled = cfg.get("oms_funding_filter_enabled", True)
    funding_ok = None
    if direction is not None and funding_enabled:
        funding = st.get("oms_funding_rate")
        max_abs = cfg.get("oms_funding_max_abs", 0.0005)
        if funding is not None:
            funding_ok = not ((direction == "long" and funding > max_abs) or
                               (direction == "short" and funding < -max_abs))
            if not funding_ok:
                direction = None
    st["oms_funding_ok"] = funding_ok

    # RSI-Regime-Filter: RSI unter Mittellinie -> nur Short erlaubt, RSI ueber Mittellinie ->
    # nur Long erlaubt (Wilder-RSI auf Kerzen, separat per oms_rsi_poll_loop aktualisiert, da
    # das ein traegerer Regime-Filter ist, kein tick-schnelles Signal wie OBI/CVD)
    rsi_enabled = cfg.get("oms_rsi_filter_enabled", False)
    rsi_ok = None
    if direction is not None and rsi_enabled:
        rsi_val = st.get("oms_rsi")
        midline = cfg.get("oms_rsi_midline", 50)
        if rsi_val is not None:
            rsi_ok = not ((direction == "long" and rsi_val < midline) or
                          (direction == "short" and rsi_val > midline))
            if not rsi_ok:
                direction = None
    st["oms_rsi_ok"] = rsi_ok

    # OI-Filter: nur in die Richtung erlauben, die der OI-Score gerade stuetzt (siehe update_oms_oi
    # fuer die Herleitung: Preisrichtung + OI-Aenderung kombiniert)
    oi_enabled = cfg.get("oms_oi_filter_enabled", False)
    oi_ok = None
    if direction is not None and oi_enabled:
        oi_score = st.get("oms_oi_score")
        min_score = cfg.get("oms_oi_min_score", 0.3)
        if oi_score is not None:
            oi_ok = not ((direction == "long" and oi_score < min_score) or
                        (direction == "short" and oi_score > -min_score))
            if not oi_ok:
                direction = None
    st["oms_oi_ok"] = oi_ok

    # Liquidations-Filter: bestaetigt Richtung, wenn Zwangs-Liquidationen (echte Events, kein
    # Sentiment) in dieselbe Richtung zeigen wie das Signal
    liq_enabled = cfg.get("oms_liq_filter_enabled", False)
    liq_ok = None
    if direction is not None and liq_enabled:
        liq_ratio = st.get("oms_liq_ratio")
        min_ratio = cfg.get("oms_liq_min_ratio", 0.2)
        if liq_ratio is not None and st.get("oms_liq_count", 0) > 0:
            liq_ok = not ((direction == "long" and liq_ratio < min_ratio) or
                         (direction == "short" and liq_ratio > -min_ratio))
            if not liq_ok:
                direction = None
        # Wenn im Fenster keine Liquidationen stattfanden, blockiert der Filter NICHT (kein
        # Liquidations-Event ist kein Gegen-Signal, nur ein fehlendes Bestaetigungs-Signal)
    st["oms_liq_ok"] = liq_ok

    # Trend-Meter: IMMER aktualisieren, unabhaengig von bot_active/Cooldown/Position - das ist
    # die Live-Anzeige "JETZT LONG"/"JETZT SHORT" zum manuellen Nachhandeln
    st["oms_signal"] = direction
    if st["last_price"] is not None:
        _oms_record_price(st, st["last_price"])
    st["oms_obi_history"].append({"ts": int(now * 1000), "fast": fast, "medium": medium, "slow": slow})
    if len(st["oms_obi_history"]) > 300:
        st["oms_obi_history"].pop(0)

    if direction is None or st["last_price"] is None:
        return
    if not cfg["bot_active"]:
        return
    if now - st["oms_last_trade_time"] < cfg["oms_cooldown_seconds"]:
        return

    if st["position"] is None:
        if direction == st["oms_last_signal_direction"]:
            return  # dasselbe Signal wie beim letzten Mal - nicht wiederholt in dieselbe Richtung feuern
        st["oms_last_signal_direction"] = direction
        st["oms_last_trade_time"] = now
        debug_log(f"📡 [{symbol}] OBI-Momentum-Scalp Signal: {direction.upper()} @ {st['last_price']} "
                  f"(OBI {round(fast,3)}/{round(medium,3)}/{round(slow,3)}, CVD {st.get('oms_cvd_ratio')})")
        await execute_entry(symbol, direction, st["last_price"], is_add_on=False)
        if st["position"] is not None:
            _oms_reset_position_state(st)
            st["oms_last_entry_price"] = st["last_price"]
            _oms_record_marker(st, st["last_price"], "entry_long" if direction == "long" else "entry_short")

    elif (cfg.get("oms_dca_enabled", True) and direction == st["position"]
          and st["oms_dca_count"] < cfg.get("oms_dca_max_entries", 2) and not st["oms_tp1_done"]):
        # Nachkauf nur wenn Signal erneut bestaetigt UND der Preis seit dem letzten Einstieg
        # spuerbar gegen die Position gelaufen ist - kein reines "Signal wieder da" (sonst
        # Dauerfeuer), kein Nachkauf mehr nachdem TP1 schon getroffen wurde (Rest wird getrailt)
        last_entry = st["oms_last_entry_price"] or st["avg_entry_price"]
        moved_against = (last_entry - st["last_price"]) if st["position"] == "long" else (st["last_price"] - last_entry)
        moved_against_usd = moved_against * st["total_coin_size"]
        if moved_against_usd >= cfg.get("oms_dca_min_pullback_usd", 1.0):
            st["oms_last_trade_time"] = now
            fraction = cfg.get("oms_dca_size_fraction", 0.6) ** (st["oms_dca_count"] + 1)
            debug_log(f"📡 [{symbol}] OBI-Momentum-Scalp Nachkauf {st['oms_dca_count']+1}: "
                      f"{direction.upper()} @ {st['last_price']} (Signal erneut bestätigt, Größe ×{round(fraction,2)})")
            ok = await execute_entry(symbol, direction, st["last_price"], is_add_on=True, size_multiplier=fraction)
            if ok:
                st["oms_dca_count"] += 1
                st["oms_last_entry_price"] = st["last_price"]
                _oms_record_marker(st, st["last_price"], "dca_long" if direction == "long" else "dca_short")

    elif cfg.get("oms_reverse_on_signal", False) and direction != st["position"]:
        # Optional: Gegen-Signal dreht die Position sofort um, statt auf SL/TP1/Trail zu warten.
        # "direction" hat zu diesem Zeitpunkt bereits alle Filter durchlaufen (OBI 3-Fenster +
        # CVD + Funding) - ist also kein rohes Einzelsignal, sondern derselbe bestaetigte Signal-
        # Typ, der auch fuer einen Neueinstieg gelten wuerde. Standardmaessig AUS, da haeufiges
        # Drehen in Seitwaertsmaerkten teuer werden kann (Spread/Slippage bei jedem Dreh).
        old_direction = st["position"]
        debug_log(f"🔄 [{symbol}] OBI-Momentum-Scalp Reverse: {old_direction.upper()} -> {direction.upper()} @ {st['last_price']} "
                  f"(OBI {round(fast,3)}/{round(medium,3)}/{round(slow,3)}, CVD {st.get('oms_cvd_ratio')})")
        await execute_exit(symbol, st["last_price"], "REVERSE")
        _oms_record_marker(st, st["last_price"], "exit_reverse")
        _oms_reset_position_state(st)
        st["oms_last_trade_time"] = now
        st["oms_last_signal_direction"] = direction
        await execute_entry(symbol, direction, st["last_price"], is_add_on=False)
        if st["position"] is not None:
            _oms_reset_position_state(st)
            st["oms_last_entry_price"] = st["last_price"]
            _oms_record_marker(st, st["last_price"], "entry_long" if direction == "long" else "entry_short")


async def check_oms_exit_management(symbol, price):
    """Wird bei jedem Preis-Tick aufgerufen. SL zuerst (fester $-Betrag auf die GESAMTE
    Position, NICHT die Liquidation), danach je nach oms_exit_mode entweder:
    - "single_tp": einfacher TP - kompletter Ausstieg sobald oms_tp1_usd erreicht ist.
    - "tp1_trail": TP1 (Teilverkauf) - danach startet Trailing fuer den Rest; kein separater
      Break-Even-SL noetig, der TP1-Gewinn ist schon real realisiert."""
    if symbol not in BOTS:
        return
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if cfg["entry_mode"] != "oms_scalp" or price is None:
        return
    _oms_record_price(st, price)
    if st["position"] is None:
        return

    pos = st["position"]
    entry = st["avg_entry_price"]
    size = st["total_coin_size"]
    if not size:
        return
    pnl_usd = (price - entry) * size if pos == "long" else (entry - price) * size

    if pnl_usd <= -cfg["oms_sl_usd"]:
        await execute_exit(symbol, price, "SL")
        _oms_record_marker(st, price, "exit_sl")
        _oms_reset_position_state(st)
        return

    exit_mode = cfg.get("oms_exit_mode", "tp1_trail")

    if exit_mode == "single_tp":
        if pnl_usd >= cfg["oms_tp1_usd"]:
            await execute_exit(symbol, price, "TP")
            _oms_record_marker(st, price, "exit_tp")
            _oms_reset_position_state(st)
        return

    if not st["oms_tp1_done"]:
        if pnl_usd >= cfg["oms_tp1_usd"]:
            fraction = cfg["oms_tp1_close_pct"] / 100
            ok = await execute_partial_exit(symbol, price, fraction, "TP1")
            if ok:
                st["oms_tp1_done"] = True
                st["oms_trail_price"] = price  # Trailing-Referenz startet ab hier
                _oms_record_marker(st, price, "exit_tp1")
        return

    # Nach TP1: Rest eng nachziehen
    trail_dist = cfg["oms_trail_distance_usd"] / size if size else 0
    if pos == "long":
        if price > (st["oms_trail_price"] or price):
            st["oms_trail_price"] = price
        if price <= (st["oms_trail_price"] or price) - trail_dist:
            await execute_exit(symbol, price, "TRAIL")
            _oms_record_marker(st, price, "exit_trail")
            _oms_reset_position_state(st)
    else:
        if price < (st["oms_trail_price"] or price):
            st["oms_trail_price"] = price
        if price >= (st["oms_trail_price"] or price) + trail_dist:
            await execute_exit(symbol, price, "TRAIL")
            _oms_record_marker(st, price, "exit_trail")
            _oms_reset_position_state(st)


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

    if cfg["entry_mode"] == "oms_scalp":
        await check_oms_exit_management(symbol, price)
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

    if cfg["entry_mode"] == "ut_bot":
        entry_trigger = cfg.get("ut_entry_trigger", "candle_close")
        exit_trigger = cfg.get("ut_exit_trigger", "candle_close")
        if entry_trigger == "tick" or exit_trigger == "tick":
            try:
                ch, cl, cc = st.get("ut_highs"), st.get("ut_lows"), st.get("ut_closes")
                if ch and cl and cc and len(cc) >= 2:
                    live_h = ch[:-1] + [max(ch[-1], price)]
                    live_l = cl[:-1] + [min(cl[-1], price)]
                    live_c = cc[:-1] + [price]
                    stop, buy, sell = compute_ut_bot(live_h, live_l, live_c, cfg["ut_atr_period"], cfg["ut_key_value"])
                    buy_signal, sell_signal = buy[-1], sell[-1]
                    if cfg.get("ut_invert_direction", False):
                        buy_signal, sell_signal = sell_signal, buy_signal
                    if exit_trigger == "tick":
                        await check_ut_exit(symbol, buy_signal, sell_signal, price)
                    if entry_trigger == "tick":
                        await check_ut_entry(symbol, buy_signal, sell_signal, price)
            except Exception as e:
                debug_log(f"⚠️ [{symbol}] UT-Bot Live-Tick-Auswertung fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

        if st["position"] is not None and (cfg.get("ut_tp_enabled", False) or cfg.get("ut_sl_enabled", False)):
            entry = st["avg_entry_price"]
            pnl_usd = (price - entry) * st["total_coin_size"] if st["position"] == "long" else (entry - price) * st["total_coin_size"]
            if cfg.get("ut_sl_enabled", False) and pnl_usd <= -abs(cfg.get("ut_sl_usd", 3)):
                await execute_exit(symbol, price, "SL")
                st["ut_sl_cooldown_until"] = time.time() + cfg.get("ut_sl_cooldown_seconds", 30)
            elif cfg.get("ut_tp_enabled", False) and pnl_usd >= abs(cfg.get("ut_tp_usd", 3)):
                await execute_exit(symbol, price, "TP")
        return

    if cfg["entry_mode"] == "halftrend":
        entry_trigger = cfg.get("ht_entry_trigger", "candle_close")
        exit_trigger = cfg.get("ht_exit_trigger", "candle_close")
        if entry_trigger == "tick" or exit_trigger == "tick":
            try:
                ch, cl, cc = st.get("ht_highs"), st.get("ht_lows"), st.get("ht_closes")
                if ch and cl and cc and len(cc) >= 2:
                    live_h = ch[:-1] + [max(ch[-1], price)]
                    live_l = cl[:-1] + [min(cl[-1], price)]
                    live_c = cc[:-1] + [price]
                    _, trend, atr2 = compute_halftrend(live_h, live_l, live_c, cfg["ht_amplitude"], cfg["ht_channel_deviation"])
                    buy_signal = trend[-1] == 0 and trend[-2] == 1
                    sell_signal = trend[-1] == 1 and trend[-2] == 0
                    if cfg.get("ht_invert_direction", False):
                        buy_signal, sell_signal = sell_signal, buy_signal
                    if exit_trigger == "tick":
                        await check_ht_exit(symbol, buy_signal, sell_signal, price)
                    if entry_trigger == "tick":
                        await check_ht_entry(symbol, buy_signal, sell_signal, price, atr2[-1])
            except Exception as e:
                debug_log(f"⚠️ [{symbol}] HalfTrend Live-Tick-Auswertung fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

        await check_ht_sl_tp(symbol, price)
        return

    if cfg["entry_mode"] == "wavetrend_cross":
        entry_trigger = cfg.get("wtc_entry_trigger", "candle_close")
        exit_trigger = cfg.get("wtc_exit_trigger", "candle_close")
        if entry_trigger == "tick" or exit_trigger == "tick":
            try:
                ch, cl, cc = st.get("wtc_highs"), st.get("wtc_lows"), st.get("wtc_closes")
                if ch and cl and cc and len(cc) >= 2:
                    live_h = ch[:-1] + [max(ch[-1], price)]
                    live_l = cl[:-1] + [min(cl[-1], price)]
                    live_c = cc[:-1] + [price]
                    wt1, wt2 = compute_wavetrend(live_h, live_l, live_c, cfg["wtc_channel_length"], cfg["wtc_average_length"], cfg["wtc_ma_length"])
                    buy_signal, sell_signal = _wtc_signals(wt1, wt2, cfg)
                    if cfg.get("wtc_invert_direction", False):
                        buy_signal, sell_signal = sell_signal, buy_signal
                    if exit_trigger == "tick":
                        await check_wtc_exit(symbol, buy_signal, sell_signal, price)
                    if entry_trigger == "tick":
                        await check_wtc_entry(symbol, buy_signal, sell_signal, price)
                    await check_wtc_pending(symbol, buy_signal, sell_signal, price)
                    await check_wtc_dca(symbol, wt1, wt2, price)
            except Exception as e:
                debug_log(f"⚠️ [{symbol}] WaveTrend-Cross Live-Tick-Auswertung fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

        if st["position"] is not None and (cfg.get("wtc_tp_enabled", False) or cfg.get("wtc_sl_enabled", False)):
            entry = st["avg_entry_price"]
            pnl_usd = (price - entry) * st["total_coin_size"] if st["position"] == "long" else (entry - price) * st["total_coin_size"]
            if cfg.get("wtc_sl_enabled", False) and pnl_usd <= -abs(cfg.get("wtc_sl_usd", 3)):
                await execute_exit(symbol, price, "SL")
                st["wtc_sl_cooldown_until"] = time.time() + cfg.get("wtc_sl_cooldown_seconds", 30)
            elif cfg.get("wtc_tp_enabled", False) and pnl_usd >= abs(cfg.get("wtc_tp_usd", 3)):
                await execute_exit(symbol, price, "TP")
        return

    if cfg["entry_mode"] == "signal_grid":
        if cfg.get("sg_entry_trigger", "candle_close") == "tick":
            try:
                ch, cl, cc = st.get("sg_highs"), st.get("sg_lows"), st.get("sg_closes")
                if ch and cl and cc and len(cc) >= 2:
                    live_h = ch[:-1] + [max(ch[-1], price)]
                    live_l = cl[:-1] + [min(cl[-1], price)]
                    live_c = cc[:-1] + [price]
                    buy_entry, sell_entry = compute_sg_signal(live_h, live_l, live_c, cfg)
                    await check_sg_signal(symbol, buy_entry, sell_entry, price)
            except Exception as e:
                debug_log(f"⚠️ [{symbol}] Signal-Grid Live-Tick-Auswertung fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        # TP laeuft immer tick-basiert (wie beim normalen Grid), unabhaengig vom Einstiegs-Trigger.
        await check_sg_tp(symbol, price)
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
                    await ws.send(json.dumps({"type": "subscribe", "channel": f"market_stats/{MARKET_INDICES[s]}"}))
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
                        liq_trades = msg.get("liquidation_trades", [])
                        update_oms_cvd(symbol, trades)
                        update_oms_liquidations(symbol, liq_trades)
                        if trades:
                            price = float(trades[-1]["price"])
                            try:
                                await on_price_update(symbol, price)
                            except Exception as e:
                                debug_log(f"⚠️ [{symbol}] on_price_update fehlgeschlagen (Verbindung bleibt bestehen)", {"error": str(e), "traceback": traceback.format_exc()})
                    elif channel.startswith("order_book") and symbol:
                        await handle_obi_order_book_update(symbol, msg)
                    elif channel.startswith("market_stats") and symbol:
                        stats = msg.get("market_stats", {})
                        rate = stats.get("current_funding_rate")
                        if rate is not None:
                            try:
                                BOTS[symbol]["state"]["oms_funding_rate"] = float(rate)
                            except (TypeError, ValueError):
                                pass
                        oi = stats.get("open_interest")
                        if oi is not None:
                            try:
                                update_oms_oi(symbol, float(oi))
                            except (TypeError, ValueError):
                                pass

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
            elif hit_tp:
                _bt_close_trade(trades, pdir, entry, tp_price, size, i, position["entry_i"], "TP", ts=ts)
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


def backtest_ut_bot(candles, cfg):
    """Backtest laeuft immer 'kerzenbasiert'. Intrabar-Pruefung ueber Hoch/Tief fuer SL/TP
    statt nur Schlusskurs (siehe Chandelier-Exit-Backtest fuer die Begruendung)."""
    ts, o, h, l, c = candles
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    tp_enabled = cfg.get("ut_tp_enabled", False)
    tp_usd = abs(cfg.get("ut_tp_usd", 3))
    sl_enabled = cfg.get("ut_sl_enabled", False)
    sl_usd = abs(cfg.get("ut_sl_usd", 3))
    sl_cooldown_ms = cfg.get("ut_sl_cooldown_seconds", 30) * 1000
    invert = cfg.get("ut_invert_direction", False)
    atr_period = cfg["ut_atr_period"]

    stop, buy, sell = compute_ut_bot(h, l, c, atr_period, cfg["ut_key_value"])
    if invert:
        buy, sell = sell, buy

    warmup = atr_period + 3
    position = None
    trades = []
    sl_cooldown_until_ts = None

    for i in range(warmup, n):
        price = c[i]

        if position is not None and (tp_enabled or sl_enabled):
            pdir, entry, size = position["dir"], position["entry"], position["size"]
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

        buy_signal, sell_signal = buy[i], sell[i]

        if position is not None:
            if (position["dir"] == "long" and sell_signal) or (position["dir"] == "short" and buy_signal):
                _bt_close_trade(trades, position["dir"], position["entry"], price, position["size"], i, position["entry_i"], "UT-FLIP-EXIT", ts=ts)
                position = None

        in_sl_cooldown = sl_enabled and sl_cooldown_until_ts is not None and ts[i] < sl_cooldown_until_ts
        if position is None and not in_sl_cooldown and (buy_signal or sell_signal):
            direction = "long" if buy_signal else "short"
            size = (margin * leverage) / price
            position = {"dir": direction, "entry": price, "size": size, "entry_i": i}

    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)

    return trades


def _simulate_halftrend_trades(candles, cfg, trend, atr2, warmup):
    """Kern-Simulation fuer HalfTrend, getrennt von der Trend-/ATR2-Berechnung (compute_halftrend)
    damit der Parameter-Sweep trend/atr2 nur EINMAL pro Amplitude berechnen muss und Channel-
    Deviation/Base-Risk-Kombinationen rein rechnerisch (schnell) durchtestet - wie beim Chandelier-
    Exit-Sweep, der Kerzen auch nur einmal laedt. Bildet die drei TP-Stufen des Original-Skripts
    als echte Teilverkaeufe nach (dort nur Statistik-Tracking): TP1 -> Teilverkauf + SL auf
    Break-Even, TP2 -> weiterer Teilverkauf, TP3 -> Rest schliessen. Analog zu backtest_fib_reversal."""
    ts, o, h, l, c = candles
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    channel_deviation = cfg["ht_channel_deviation"]
    base_risk_mult = cfg["ht_base_risk_mult"]
    tp_enabled = cfg.get("ht_tp_enabled", True)
    sl_enabled = cfg.get("ht_sl_enabled", True)
    tp1_frac = cfg.get("ht_tp1_close_pct", 33) / 100
    tp2_frac = cfg.get("ht_tp2_close_pct", 50) / 100
    sl_cooldown_ms = cfg.get("ht_sl_cooldown_seconds", 30) * 1000
    invert = cfg.get("ht_invert_direction", False)

    position = None  # {"dir","entry","size","entry_i","sl_price","tp1_price","tp2_price","tp3_price","tp1_done","tp2_done"}
    trades = []
    sl_cooldown_until_ts = None

    for i in range(warmup, n):
        price = c[i]

        if position is not None:
            pdir, entry = position["dir"], position["entry"]
            sl_price = position.get("sl_price")
            hit_sl = sl_price is not None and ((pdir == "long" and l[i] <= sl_price) or (pdir == "short" and h[i] >= sl_price))
            if hit_sl:
                reason = "BREAKEVEN" if position["tp1_done"] else "SL"
                _bt_close_trade(trades, pdir, entry, sl_price, position["size"], i, position["entry_i"], reason, ts=ts)
                position = None
                sl_cooldown_until_ts = ts[i] + sl_cooldown_ms
            elif tp_enabled and not position["tp1_done"] and position.get("tp1_price") is not None:
                tp1_price = position["tp1_price"]
                if (pdir == "long" and h[i] >= tp1_price) or (pdir == "short" and l[i] <= tp1_price):
                    close_size = position["size"] * tp1_frac
                    _bt_close_trade(trades, pdir, entry, tp1_price, close_size, i, position["entry_i"], "TP1", ts=ts)
                    position["size"] -= close_size
                    position["tp1_done"] = True
                    position["sl_price"] = entry  # Break-Even
            elif tp_enabled and position["tp1_done"] and not position["tp2_done"] and position.get("tp2_price") is not None:
                tp2_price = position["tp2_price"]
                if (pdir == "long" and h[i] >= tp2_price) or (pdir == "short" and l[i] <= tp2_price):
                    close_size = position["size"] * tp2_frac
                    _bt_close_trade(trades, pdir, entry, tp2_price, close_size, i, position["entry_i"], "TP2", ts=ts)
                    position["size"] -= close_size
                    position["tp2_done"] = True
            elif tp_enabled and position["tp1_done"] and position["tp2_done"] and position.get("tp3_price") is not None:
                tp3_price = position["tp3_price"]
                if (pdir == "long" and h[i] >= tp3_price) or (pdir == "short" and l[i] <= tp3_price):
                    _bt_close_trade(trades, pdir, entry, tp3_price, position["size"], i, position["entry_i"], "TP3", ts=ts)
                    position = None

        buy_signal = trend[i] == 0 and trend[i - 1] == 1
        sell_signal = trend[i] == 1 and trend[i - 1] == 0
        if invert:
            buy_signal, sell_signal = sell_signal, buy_signal

        if position is not None:
            if (position["dir"] == "long" and sell_signal) or (position["dir"] == "short" and buy_signal):
                _bt_close_trade(trades, position["dir"], position["entry"], price, position["size"], i, position["entry_i"], "HT-FLIP-EXIT", ts=ts)
                position = None

        in_sl_cooldown = sl_enabled and sl_cooldown_until_ts is not None and ts[i] < sl_cooldown_until_ts
        if position is None and not in_sl_cooldown and (buy_signal or sell_signal):
            direction = "long" if buy_signal else "short"
            size = (margin * leverage) / price
            dist_sl = atr2[i] * channel_deviation if sl_enabled else None
            sl_price = (price - dist_sl if direction == "long" else price + dist_sl) if dist_sl is not None else None
            tp1_price = tp2_price = tp3_price = None
            if tp_enabled:
                dist = atr2[i] * base_risk_mult
                if direction == "long":
                    tp1_price, tp2_price, tp3_price = price + dist, price + dist * 2, price + dist * 3
                else:
                    tp1_price, tp2_price, tp3_price = price - dist, price - dist * 2, price - dist * 3
            position = {"dir": direction, "entry": price, "size": size, "entry_i": i,
                        "sl_price": sl_price, "tp1_price": tp1_price, "tp2_price": tp2_price, "tp3_price": tp3_price,
                        "tp1_done": False, "tp2_done": False}

    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)

    return trades


def backtest_halftrend(candles, cfg):
    """Backtest laeuft immer 'kerzenbasiert'. Intrabar-Pruefung fuer SL/TP1/TP2/TP3 wie bei den
    anderen Strategien - hier sind die Abstaende ATR2-basiert (Channel-Deviation bzw. Base-Risk-
    Multiplikator), nicht fest in $, und werden bei jedem Einstieg neu aus dem dann aktuellen
    ATR2 berechnet (siehe _simulate_halftrend_trades)."""
    h, l, c = candles[2], candles[3], candles[4]
    amplitude = cfg["ht_amplitude"]
    _, trend, atr2 = compute_halftrend(h, l, c, amplitude, cfg["ht_channel_deviation"])
    warmup = max(100, amplitude) + 5
    return _simulate_halftrend_trades(candles, cfg, trend, atr2, warmup)


def backtest_wavetrend_cross(candles, cfg, stf_candles=None):
    """Backtest laeuft immer 'kerzenbasiert'. Intrabar-Pruefung fuer SL/TP wie bei den anderen
    Strategien. Unterstuetzt Richtungsmodus (long_only/short_only), Nachkauf (DCA) und den
    optionalen SuperTrend-Fusion-Richtungsfilter auf hoeherem Zeitrahmen inkl. Pending-Order-
    Logik (wie bei Chandelier Exit)."""
    ts, o, h, l, c = candles
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    tp_enabled = cfg.get("wtc_tp_enabled", False)
    tp_usd = abs(cfg.get("wtc_tp_usd", 3))
    sl_enabled = cfg.get("wtc_sl_enabled", False)
    sl_usd = abs(cfg.get("wtc_sl_usd", 3))
    sl_cooldown_ms = cfg.get("wtc_sl_cooldown_seconds", 30) * 1000
    invert = cfg.get("wtc_invert_direction", False)
    require_obos = cfg.get("wtc_require_obos", True)
    ob_level = cfg.get("wtc_ob_level", 53)
    os_level = cfg.get("wtc_os_level", -53)
    dir_mode = cfg.get("wtc_direction_mode", "both")
    dca_enabled = cfg.get("wtc_dca_enabled", False)
    dca_max = cfg.get("wtc_dca_max_entries", 10)
    dca_cooldown_ms = cfg.get("wtc_dca_cooldown_seconds", 60) * 1000
    stf_filter_enabled = cfg.get("wtc_stf_filter_enabled", False) and stf_candles is not None

    wt1, wt2 = compute_wavetrend(h, l, c, cfg["wtc_channel_length"], cfg["wtc_average_length"], cfg["wtc_ma_length"])

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

    warmup = max(cfg["wtc_channel_length"], cfg["wtc_average_length"], cfg["wtc_ma_length"]) * 3 + 5
    position = None  # {"dir","entry","size","entry_i","entry_count"}
    pending_direction = None
    trades = []
    sl_cooldown_until_ts = None

    for i in range(warmup, n):
        price = c[i]

        if position is not None and (tp_enabled or sl_enabled):
            pdir, entry, size = position["dir"], position["entry"], position["size"]
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

        cross_up = wt1[i - 1] <= wt2[i - 1] and wt1[i] > wt2[i]
        cross_down = wt1[i - 1] >= wt2[i - 1] and wt1[i] < wt2[i]
        buy_signal = cross_up and (not require_obos or wt2[i] <= os_level)
        sell_signal = cross_down and (not require_obos or wt2[i] >= ob_level)
        if invert:
            buy_signal, sell_signal = sell_signal, buy_signal

        if position is not None:
            if (position["dir"] == "long" and sell_signal) or (position["dir"] == "short" and buy_signal):
                _bt_close_trade(trades, position["dir"], position["entry"], price, position["size"], i, position["entry_i"], "WTC-FLIP-EXIT", ts=ts)
                position = None

        in_sl_cooldown = sl_enabled and sl_cooldown_until_ts is not None and ts[i] < sl_cooldown_until_ts

        # Nachkauf: level-basiert, jede Kerze geprueft, nicht an das einmalige Kreuzungs-Signal
        # gebunden (das koennte in derselben Richtung strukturell nicht wiederholt feuern).
        if position is not None and dca_enabled and position["entry_count"] < dca_max:
            direction = position["dir"]
            last_dca = position.get("last_dca_ts", ts[position["entry_i"]])
            if ts[i] - last_dca >= dca_cooldown_ms and _wtc_dca_condition(wt1, wt2, i, direction, cfg):
                bias = stf_bias_for_ts(ts[i]) if stf_filter_enabled else None
                if not stf_filter_enabled or bias == direction:
                    add_size = (margin * leverage) / price
                    total_size = position["size"] + add_size
                    position["entry"] = (position["entry"] * position["size"] + price * add_size) / total_size
                    position["size"] = total_size
                    position["entry_count"] += 1
                    position["last_dca_ts"] = ts[i]

        if buy_signal or sell_signal:
            signal_dir = "long" if buy_signal else "short"
            allowed = not ((dir_mode == "long_only" and signal_dir == "short") or (dir_mode == "short_only" and signal_dir == "long"))

            if allowed and position is None and not in_sl_cooldown:
                bias = stf_bias_for_ts(ts[i]) if stf_filter_enabled else None
                if not stf_filter_enabled or bias == signal_dir:
                    size = (margin * leverage) / price
                    position = {"dir": signal_dir, "entry": price, "size": size, "entry_i": i, "entry_count": 1, "last_dca_ts": ts[i]}
                    pending_direction = None
                else:
                    pending_direction = signal_dir

        if position is None and stf_filter_enabled and pending_direction:
            if sell_signal and pending_direction == "long":
                pending_direction = None
            elif buy_signal and pending_direction == "short":
                pending_direction = None
            else:
                bias = stf_bias_for_ts(ts[i])
                if bias == pending_direction:
                    size = (margin * leverage) / price
                    position = {"dir": pending_direction, "entry": price, "size": size, "entry_i": i, "entry_count": 1, "last_dca_ts": ts[i]}
                    pending_direction = None

    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)

    return trades


def backtest_signal_grid(candles, cfg):
    """Signal-Grid-Backtest: Grid-Mechanik (TP %/$ vom Ø-Einstieg, kein Flip-Exit) - Ein- UND
    Nachkauf werden durch dasselbe ECHTE Signal-Ereignis ausgeloest (Kreuzung/Schwellenwert-
    Durchbruch), nicht durch eine anhaltende Bedingung. Funktioniert nur bei Signal-Grid (nicht
    bei WaveTrend-Cross selbst), weil es keinen Flip-Exit gibt - die Position bleibt offen,
    waehrend das Signal mehrfach in dieselbe Richtung feuern kann. Intrabar-Pruefung fuer TP
    wie bei den anderen Strategien."""
    ts, o, h, l, c = candles
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    source = cfg.get("sg_signal_source", "wavetrend")
    max_nachkauf = cfg.get("sg_max_nachkauf", 0)
    dca_cooldown_ms = cfg.get("sg_dca_cooldown_seconds", 10) * 1000
    invert = cfg.get("sg_invert_direction", False)

    if source == "wavetrend":
        wt1, wt2 = compute_wavetrend(h, l, c, cfg["wtc_channel_length"], cfg["wtc_average_length"], cfg["wtc_ma_length"])
        require_obos = cfg.get("wtc_require_obos", True)
        ob_level = cfg.get("wtc_ob_level", 53)
        os_level = cfg.get("wtc_os_level", -53)
        warmup = max(cfg["wtc_channel_length"], cfg["wtc_average_length"], cfg["wtc_ma_length"]) * 3 + 5

        def entry_signals(i):
            cross_up = wt1[i - 1] <= wt2[i - 1] and wt1[i] > wt2[i]
            cross_down = wt1[i - 1] >= wt2[i - 1] and wt1[i] < wt2[i]
            return (cross_up and (not require_obos or wt2[i] <= os_level),
                    cross_down and (not require_obos or wt2[i] >= ob_level))
    else:
        z = compute_zscore_trend(c, cfg["zscore_lookback_period"], cfg["zscore_ema_smooth"])
        threshold = cfg["zscore_threshold"]
        warmup = cfg["zscore_lookback_period"] + 5

        def entry_signals(i):
            return (z[i - 1] <= threshold and z[i] > threshold, z[i - 1] >= -threshold and z[i] < -threshold)

    position = None  # {"dir","entry","size","entry_i","entry_count","last_dca_ts"}
    trades = []

    for i in range(warmup, n):
        price = c[i]
        buy_entry, sell_entry = entry_signals(i)
        if invert:
            buy_entry, sell_entry = sell_entry, buy_entry

        if position is not None:
            direction, entry, size = position["dir"], position["entry"], position["size"]
            if cfg.get("sg_tp_mode", "pct") == "usd":
                tp_usd = abs(cfg.get("sg_tp_step_usd", 5.0))
                tp_price = entry + tp_usd / size if direction == "long" else entry - tp_usd / size
            else:
                tp_abs = compute_sg_tp_abs(entry, cfg)
                tp_price = entry + tp_abs if direction == "long" else entry - tp_abs
            hit_tp = (direction == "long" and h[i] >= tp_price) or (direction == "short" and l[i] <= tp_price)
            if hit_tp:
                _bt_close_trade(trades, direction, entry, tp_price, size, i, position["entry_i"], "TP", ts=ts)
                position = None

        if position is not None and (buy_entry or sell_entry):
            signal_dir = "long" if buy_entry else "short"
            if position["dir"] == signal_dir:
                if (max_nachkauf == 0 or position["entry_count"] < max_nachkauf) and ts[i] - position["last_dca_ts"] >= dca_cooldown_ms:
                    add_size = (margin * leverage) / price
                    total_size = position["size"] + add_size
                    position["entry"] = (position["entry"] * position["size"] + price * add_size) / total_size
                    position["size"] = total_size
                    position["entry_count"] += 1
                    position["last_dca_ts"] = ts[i]

        elif position is None and (buy_entry or sell_entry):
            direction = "long" if buy_entry else "short"
            size = (margin * leverage) / price
            position = {"dir": direction, "entry": price, "size": size, "entry_i": i, "entry_count": 1, "last_dca_ts": ts[i]}

    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)

    return trades


BACKTEST_MAX_CANDLES = {
    "fib_reversal": 100_000, "range_profile": 30_000,
    "supertrend_fusion": 100_000, "chandelier_exit": 100_000,
    "ut_bot": 100_000, "wavetrend_cross": 100_000, "signal_grid": 100_000,
    "halftrend": 100_000,
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
            # Intrabar-Pruefung ueber Hoch/Tief statt nur Schlusskurs. Reihenfolge-Annahme fuer
            # den Break-Even-Mechanismus (bei OHLC-Daten unvermeidbar, da keine Tick-Reihenfolge
            # bekannt ist): wir pruefen erst, ob der Break-Even-Trigger in dieser Kerze erreicht
            # wurde, bevor wir den (dann ggf. schon nachgezogenen) SL/Break-Even-Floor gegen
            # dieselbe Kerze pruefen - das ist die ueblichere, leicht optimistische Annahme.
            sl_price = entry - cfg["rp_sl_usd"] / size if direction == "long" else entry + cfg["rp_sl_usd"] / size
            tp_price = entry + cfg["rp_tp_usd"] / size if direction == "long" else entry - cfg["rp_tp_usd"] / size
            if breakeven_enabled and not breakeven_triggered:
                trigger_price = entry + breakeven_trigger / size if direction == "long" else entry - breakeven_trigger / size
                reached_trigger = (direction == "long" and h[i] >= trigger_price) or (direction == "short" and l[i] <= trigger_price)
                if reached_trigger:
                    breakeven_triggered = True
            floor_is_breakeven = breakeven_enabled and breakeven_triggered
            floor_price = (entry + breakeven_lock / size if direction == "long" else entry - breakeven_lock / size) if floor_is_breakeven else sl_price
            hit_floor = (direction == "long" and l[i] <= floor_price) or (direction == "short" and h[i] >= floor_price)
            hit_tp = (direction == "long" and h[i] >= tp_price) or (direction == "short" and l[i] <= tp_price)
            if hit_floor:
                _bt_close_trade(trades, direction, entry, floor_price, size, i, position["entry_i"], "BREAKEVEN-LOCK" if floor_is_breakeven else "SL", ts=ts)
                position = None
                breakeven_triggered = False
            elif hit_tp:
                _bt_close_trade(trades, direction, entry, tp_price, size, i, position["entry_i"], "TP", ts=ts)
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
            # Intrabar: reicht die Kerze bis zum Entry-Niveau, statt nur der Schlusskurs
            reached = l[i] <= fib["entry1_price"] if direction == "long" else h[i] >= fib["entry1_price"]
            if reached:
                entry_price = fib["entry1_price"]
                size = (margin * leverage) / entry_price
                position = {"dir": direction, "avg_entry": entry_price, "size": size, "entry1_done": True,
                            "entry2_done": False, "tp1_done": False, "sl_active": fib["sl_price"],
                            "fib": fib, "entry_i": i}
            continue

        direction, fib = position["dir"], position["fib"]

        if not position["entry2_done"]:
            reached2 = l[i] <= fib["entry2_price"] if direction == "long" else h[i] >= fib["entry2_price"]
            if reached2:
                entry2_price = fib["entry2_price"]
                add_size = (margin * leverage) / entry2_price
                total_size = position["size"] + add_size
                position["avg_entry"] = (position["avg_entry"] * position["size"] + entry2_price * add_size) / total_size
                position["size"] = total_size
                position["entry2_done"] = True

        sl_hit = l[i] <= position["sl_active"] if direction == "long" else h[i] >= position["sl_active"]
        if sl_hit:
            _bt_close_trade(trades, direction, position["avg_entry"], position["sl_active"], position["size"], i, position["entry_i"], "SL", ts=ts)
            position = None
            continue

        if not position["tp1_done"]:
            tp1_hit = h[i] >= fib["tp1_price"] if direction == "long" else l[i] <= fib["tp1_price"]
            if tp1_hit:
                fraction = cfg["fib_tp1_close_pct"] / 100
                close_size = position["size"] * fraction
                _bt_close_trade(trades, direction, position["avg_entry"], fib["tp1_price"], close_size, i, position["entry_i"], "TP1", ts=ts)
                position["size"] -= close_size
                position["tp1_done"] = True
                position["sl_active"] = position["avg_entry"]
            continue

        tp2_hit = h[i] >= fib["tp2_price"] if direction == "long" else l[i] <= fib["tp2_price"]
        if tp2_hit:
            _bt_close_trade(trades, direction, position["avg_entry"], fib["tp2_price"], position["size"], i, position["entry_i"], "TP2", ts=ts)
            position = None

    return trades


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


async def run_stf_param_sweep(symbol, cfg, days, atr_period_min, atr_period_max, atr_period_step,
                               factor_min, factor_max, factor_step):
    """'Monte-Carlo'-Parametersweep fuer SuperTrend Fusion: testet alle Kombinationen aus
    ATR-Periode und Faktor (Band-Breite) im angegebenen Bereich gegeneinander. Gleiches Prinzip
    wie beim Chandelier-/UT-Bot-Sweep - die Average-Force-/Choppiness-Filter bleiben dabei so
    eingestellt, wie sie aktuell im Formular stehen."""
    max_candles = BACKTEST_MAX_CANDLES["supertrend_fusion"]
    resolution = cfg.get("stf_resolution", "5m")
    candles, err, cache_used = await _fetch_cached_backtest_candles(symbol, resolution, days, max_candles)
    if err:
        return {"error": err}
    if not candles or len(candles[4]) < 100:
        return {"error": "Zu wenig historische Kerzen für einen aussagekräftigen Sweep erhalten."}

    periods = sorted(set(int(round(atr_period_min + i * atr_period_step))
                          for i in range(int((atr_period_max - atr_period_min) / max(atr_period_step, 1e-9)) + 1)
                          if atr_period_min + i * atr_period_step <= atr_period_max + 1e-9))
    factors = sorted(set(round(factor_min + i * factor_step, 4)
                          for i in range(int((factor_max - factor_min) / max(factor_step, 1e-9)) + 1)
                          if factor_min + i * factor_step <= factor_max + 1e-9))
    periods = [p for p in periods if p >= 1]
    factors = [f for f in factors if f > 0]

    total_combos = len(periods) * len(factors)
    if total_combos == 0:
        return {"error": "Der eingestellte Bereich ergibt keine gültigen Kombinationen."}
    if total_combos > CE_SWEEP_MAX_COMBOS:
        return {"error": f"Zu viele Kombinationen ({total_combos}, Limit {CE_SWEEP_MAX_COMBOS}) - Bereich oder Schrittweite vergrößern."}

    results = []
    for period in periods:
        for factor in factors:
            cfg_copy = dict(cfg)
            cfg_copy["stf_atr_period"] = period
            cfg_copy["stf_factor"] = factor
            trades = backtest_supertrend_fusion(candles, cfg_copy)
            stats = summarize_backtest_trades(trades)
            results.append({"stf_atr_period": period, "stf_factor": factor, **stats})

    best_sorted = sorted(results, key=lambda r: (r["trades"] >= CE_SWEEP_MIN_RELIABLE_TRADES, r["total_pnl_usd"]), reverse=True)
    worst_sorted = sorted(results, key=lambda r: r["total_pnl_usd"])

    actual_days = (candles[0][-1] - candles[0][0]) / (24 * 60 * 60 * 1000)
    return {
        "symbol": symbol, "resolution": resolution, "requested_days": days,
        "actual_days_covered": round(actual_days, 1), "candles_processed": len(candles[4]),
        "min_reliable_trades": CE_SWEEP_MIN_RELIABLE_TRADES,
        "combos_tested": total_combos,
        "results": best_sorted[:30],
        "worst_results": worst_sorted[:20],
    }


async def run_ut_param_sweep(symbol, cfg, days, atr_period_min, atr_period_max, atr_period_step,
                              key_value_min, key_value_max, key_value_step):
    """'Monte-Carlo'-Parametersweep fuer UT-Bot: testet alle Kombinationen aus ATR-Periode und
    Key-Value-Multiplikator im angegebenen Bereich gegeneinander. Gleiches Prinzip wie beim
    Chandelier-Sweep, nur ohne SuperTrend-Filter (den gibt's bei UT-Bot nicht)."""
    max_candles = BACKTEST_MAX_CANDLES["ut_bot"]
    resolution = cfg.get("ut_resolution", "5m")
    candles, err, cache_used = await _fetch_cached_backtest_candles(symbol, resolution, days, max_candles)
    if err:
        return {"error": err}
    if not candles or len(candles[4]) < 100:
        return {"error": "Zu wenig historische Kerzen für einen aussagekräftigen Sweep erhalten."}

    periods = sorted(set(int(round(atr_period_min + i * atr_period_step))
                          for i in range(int((atr_period_max - atr_period_min) / max(atr_period_step, 1e-9)) + 1)
                          if atr_period_min + i * atr_period_step <= atr_period_max + 1e-9))
    key_values = sorted(set(round(key_value_min + i * key_value_step, 4)
                             for i in range(int((key_value_max - key_value_min) / max(key_value_step, 1e-9)) + 1)
                             if key_value_min + i * key_value_step <= key_value_max + 1e-9))
    periods = [p for p in periods if p >= 1]
    key_values = [k for k in key_values if k > 0]

    total_combos = len(periods) * len(key_values)
    if total_combos == 0:
        return {"error": "Der eingestellte Bereich ergibt keine gültigen Kombinationen."}
    if total_combos > CE_SWEEP_MAX_COMBOS:
        return {"error": f"Zu viele Kombinationen ({total_combos}, Limit {CE_SWEEP_MAX_COMBOS}) - Bereich oder Schrittweite vergrößern."}

    results = []
    for period in periods:
        for kv in key_values:
            cfg_copy = dict(cfg)
            cfg_copy["ut_atr_period"] = period
            cfg_copy["ut_key_value"] = kv
            trades = backtest_ut_bot(candles, cfg_copy)
            stats = summarize_backtest_trades(trades)
            results.append({"ut_atr_period": period, "ut_key_value": kv, **stats})

    best_sorted = sorted(results, key=lambda r: (r["trades"] >= CE_SWEEP_MIN_RELIABLE_TRADES, r["total_pnl_usd"]), reverse=True)
    worst_sorted = sorted(results, key=lambda r: r["total_pnl_usd"])

    actual_days = (candles[0][-1] - candles[0][0]) / (24 * 60 * 60 * 1000)
    return {
        "symbol": symbol, "resolution": resolution, "requested_days": days,
        "actual_days_covered": round(actual_days, 1), "candles_processed": len(candles[4]),
        "min_reliable_trades": CE_SWEEP_MIN_RELIABLE_TRADES,
        "combos_tested": total_combos,
        "results": best_sorted[:30],
        "worst_results": worst_sorted[:20],
    }


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
    best_sorted = sorted(results, key=lambda r: (r["trades"] >= CE_SWEEP_MIN_RELIABLE_TRADES, r["total_pnl_usd"]), reverse=True)
    # Schlechteste zuerst nach reinem PnL (unabhaengig von der Trade-Anzahl) - hier soll man
    # gerade auch sehen, welche Kombinationen durchgehend schlecht abschneiden
    worst_sorted = sorted(results, key=lambda r: r["total_pnl_usd"])

    actual_days = (candles[0][-1] - candles[0][0]) / (24 * 60 * 60 * 1000)
    return {
        "symbol": symbol, "resolution": resolution, "requested_days": days,
        "actual_days_covered": round(actual_days, 1), "candles_processed": len(candles[4]),
        "stf_filter_used": stf_filter_enabled, "min_reliable_trades": CE_SWEEP_MIN_RELIABLE_TRADES,
        "combos_tested": total_combos,
        "results": best_sorted[:30],
        "worst_results": worst_sorted[:20],
    }


HT_SWEEP_MAX_COMBOS = 500
HT_SWEEP_MIN_RELIABLE_TRADES = 5


async def run_ht_param_sweep(symbol, cfg, days, amplitude_min, amplitude_max, amplitude_step,
                              channel_dev_min, channel_dev_max, channel_dev_step,
                              base_risk_min, base_risk_max, base_risk_step):
    """'Monte-Carlo'-Parametersweep fuer HalfTrend: testet alle Kombinationen aus Amplitude,
    Channel-Deviation (SL-Abstand) und Base-Risk-Multiplikator (TP-Abstand) im angegebenen
    Bereich gegeneinander. Amplitude bestimmt den eigentlichen Trend/Signal-Verlauf (aufwendig
    zu berechnen), Channel-Deviation/Base-Risk wirken sich NUR auf die SL-/TP-Preise aus (billig
    zu berechnen) - deshalb wird trend/atr2 nur EINMAL pro Amplitude neu berechnet und fuer alle
    Channel-Deviation x Base-Risk-Kombinationen wiederverwendet, aehnlich dem Chandelier-Sweep."""
    max_candles = BACKTEST_MAX_CANDLES["halftrend"]
    resolution = cfg.get("ht_resolution", "5m")
    candles, err, cache_used = await _fetch_cached_backtest_candles(symbol, resolution, days, max_candles)
    if err:
        return {"error": err}
    if not candles or len(candles[4]) < 150:
        return {"error": "Zu wenig historische Kerzen für einen aussagekräftigen Sweep erhalten."}

    amplitudes = sorted(set(int(round(amplitude_min + i * amplitude_step))
                             for i in range(int((amplitude_max - amplitude_min) / max(amplitude_step, 1e-9)) + 1)
                             if amplitude_min + i * amplitude_step <= amplitude_max + 1e-9))
    channel_devs = sorted(set(round(channel_dev_min + i * channel_dev_step, 4)
                               for i in range(int((channel_dev_max - channel_dev_min) / max(channel_dev_step, 1e-9)) + 1)
                               if channel_dev_min + i * channel_dev_step <= channel_dev_max + 1e-9))
    base_risks = sorted(set(round(base_risk_min + i * base_risk_step, 4)
                             for i in range(int((base_risk_max - base_risk_min) / max(base_risk_step, 1e-9)) + 1)
                             if base_risk_min + i * base_risk_step <= base_risk_max + 1e-9))
    amplitudes = [a for a in amplitudes if a >= 2]
    channel_devs = [d for d in channel_devs if d > 0]
    base_risks = [r for r in base_risks if r > 0]

    total_combos = len(amplitudes) * len(channel_devs) * len(base_risks)
    if total_combos == 0:
        return {"error": "Der eingestellte Bereich ergibt keine gültigen Kombinationen."}
    if total_combos > HT_SWEEP_MAX_COMBOS:
        return {"error": f"Zu viele Kombinationen ({total_combos}, Limit {HT_SWEEP_MAX_COMBOS}) - Bereich oder Schrittweite vergrößern."}

    h, l, c = candles[2], candles[3], candles[4]
    results = []
    for amp in amplitudes:
        _, trend, atr2 = compute_halftrend(h, l, c, amp, 1.0)  # channel_deviation wirkt nicht auf trend/atr2
        warmup = max(100, amp) + 5
        for cd in channel_devs:
            for br in base_risks:
                cfg_copy = dict(cfg)
                cfg_copy["ht_amplitude"] = amp
                cfg_copy["ht_channel_deviation"] = cd
                cfg_copy["ht_base_risk_mult"] = br
                trades = _simulate_halftrend_trades(candles, cfg_copy, trend, atr2, warmup)
                stats = summarize_backtest_trades(trades)
                results.append({"ht_amplitude": amp, "ht_channel_deviation": cd, "ht_base_risk_mult": br, **stats})

    best_sorted = sorted(results, key=lambda r: (r["trades"] >= HT_SWEEP_MIN_RELIABLE_TRADES, r["total_pnl_usd"]), reverse=True)
    worst_sorted = sorted(results, key=lambda r: r["total_pnl_usd"])

    actual_days = (candles[0][-1] - candles[0][0]) / (24 * 60 * 60 * 1000)
    return {
        "symbol": symbol, "resolution": resolution, "requested_days": days,
        "actual_days_covered": round(actual_days, 1), "candles_processed": len(candles[4]),
        "min_reliable_trades": HT_SWEEP_MIN_RELIABLE_TRADES,
        "combos_tested": total_combos,
        "results": best_sorted[:30],
        "worst_results": worst_sorted[:20],
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


BACKTEST_FUNCS = {
    "range_profile": backtest_range_profile,
    "fib_reversal": backtest_fib_reversal,
    "supertrend_fusion": backtest_supertrend_fusion,
    "chandelier_exit": backtest_chandelier_exit,
    "ut_bot": backtest_ut_bot,
    "wavetrend_cross": backtest_wavetrend_cross,
    "signal_grid": backtest_signal_grid,
    "halftrend": backtest_halftrend,
}


async def run_backtest(symbol, entry_mode, cfg, days):
    if entry_mode not in BACKTEST_FUNCS:
        return {"error": f"Backtest für '{entry_mode}' nicht unterstützt (nur range_profile, fib_reversal, supertrend_fusion, chandelier_exit, ut_bot, wavetrend_cross, signal_grid, halftrend - Grid/OBI-Scalp brauchen historische Tick-/Orderbuchdaten, die es nicht gibt)."}

    max_candles = BACKTEST_MAX_CANDLES[entry_mode]

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

    if entry_mode == "wavetrend_cross":
        resolution = cfg.get("wtc_resolution", "5m")
        candles, err, cache_used = await _fetch_cached_backtest_candles(symbol, resolution, days, max_candles)
        if err:
            return {"error": err}
        if not candles or len(candles[4]) < 100:
            return {"error": "Zu wenig historische Kerzen für einen aussagekräftigen Backtest erhalten."}

        stf_candles = None
        if cfg.get("wtc_stf_filter_enabled", False):
            stf_resolution = cfg.get("wtc_stf_resolution", "5m")
            stf_candles, stf_err, _ = await _fetch_cached_backtest_candles(symbol, stf_resolution, days, max_candles)
            if stf_err or not stf_candles or len(stf_candles[4]) < 100:
                return {"error": f"Zu wenig historische Kerzen für den SuperTrend-Filter-Zeitrahmen ({stf_resolution}) erhalten."}

        n_candles = len(candles[4])
        trades = backtest_wavetrend_cross(candles, cfg, stf_candles=stf_candles)
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
                       "supertrend_fusion": "stf_resolution", "ut_bot": "ut_resolution",
                       "wavetrend_cross": "wtc_resolution", "signal_grid": "sg_resolution",
                       "halftrend": "ht_resolution"}[entry_mode]
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
    }

