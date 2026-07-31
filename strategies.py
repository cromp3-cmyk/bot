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


async def fetch_historical_candles_binance(symbol, resolution, days, max_candles):
    """Holt bis zu 'days' Tage Kerzenhistorie von Binance fuer Backtests, in 1000er-
    Batches paginiert (endTime schrittweise nach hinten). '2m' wird - wie live - aus
    1m-Kerzen synthetisch zusammengesetzt. max_candles begrenzt hart, wie viele Kerzen
    am Ende verarbeitet werden (Performance-Schutz fuer den Render-Server)."""
    pair = BINANCE_SYMBOL_MAP.get(symbol)
    if not pair:
        return None, "Coin nicht auf Binance verfügbar"

    base_resolution = "1m" if resolution == "2m" else resolution
    interval_ms = BINANCE_INTERVAL_MS.get(base_resolution, 60_000)
    fetch_factor = 2 if resolution == "2m" else 1
    total_ms = days * 24 * 60 * 60 * 1000
    end_time = int(time.time() * 1000)
    start_time = end_time - total_ms
    # Hartes Limit an Basis-Kerzen (vor evtl. 2m-Zusammenfassung), damit die Anfrage nicht ausufert
    hard_candle_cap = max_candles * fetch_factor + 2000

    all_rows = []
    cursor = end_time
    requests_made = 0
    try:
        async with aiohttp.ClientSession() as session:
            while cursor > start_time and len(all_rows) < hard_candle_cap:
                url = f"https://api.binance.com/api/v3/klines?symbol={pair}&interval={base_resolution}&limit=1000&endTime={cursor}"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status != 200:
                        break
                    batch = await resp.json()
                requests_made += 1
                if not batch:
                    break
                all_rows = batch + all_rows
                cursor = int(batch[0][0]) - 1
                if len(batch) < 1000:
                    break
                await asyncio.sleep(0.2)  # Binance-Ratelimit-freundlich
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

    if resolution == "2m":
        timestamps, opens, highs, lows, closes = resample_candles((timestamps, opens, highs, lows, closes), 2)

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


SYNTHETIC_RESOLUTIONS = {"2m": ("1m", 2)}  # Zeitrahmen, die Binance nicht nativ anbietet


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


def ema_step(prev_ema, value, period):
    """Ein einzelner EMA-Rechenschritt - damit kann eine bestehende EMA (Stand: letzte
    geschlossene Kerze) mit einem einzelnen Live-Preis-Tick weitergerechnet werden,
    ohne die komplette Historie neu zu holen. Fuer den Live-Zero-Cross-Exit noetig."""
    k = 2 / (period + 1)
    return value * k + prev_ema * (1 - k)


def compute_macd_histogram(closes, fast_period, slow_period, signal_period, return_components=False):
    """MACD-Histogramm = MACD-Linie (EMA-schnell minus EMA-langsam) minus Signallinie
    (EMA der MACD-Linie). Nur das Histogramm wird fuer die Strategie genutzt.
    Mit return_components=True werden zusaetzlich die letzten EMA-Werte (ema_fast,
    ema_slow, signal) zurueckgegeben, um das Histogramm live mit dem aktuellen
    Preis-Tick weiterzurechnen (siehe ema_step)."""
    ema_fast = _ema_series(closes, fast_period)
    ema_slow = _ema_series(closes, slow_period)
    macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
    signal_line = _ema_series(macd_line, signal_period)
    histogram = [m - s for m, s in zip(macd_line, signal_line)]
    if return_components:
        return histogram, ema_fast[-1], ema_slow[-1], signal_line[-1]
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
                data = await fetch_candles_binance_multi(symbol, cfg["macd_resolution"], count_back=needed_bars)
                if data:
                    timestamps, opens, highs, lows, closes = data
                    closed_ts = timestamps[:-1]
                    closed_h, closed_l, closed_c = highs[:-1], lows[:-1], closes[:-1]

                    if len(closed_c) > cfg["macd_slow_slow"] + cfg["macd_slow_signal"]:
                        fast_hist, fast_ema_fast_val, fast_ema_slow_val, fast_signal_val = compute_macd_histogram(
                            closed_c, cfg["macd_fast_fast"], cfg["macd_fast_slow"], cfg["macd_fast_signal"], return_components=True)
                        slow_hist = compute_macd_histogram(closed_c, cfg["macd_slow_fast"], cfg["macd_slow_slow"], cfg["macd_slow_signal"])
                        st = b["state"]
                        curr_fast = fast_hist[-1]
                        curr_slow = slow_hist[-1]
                        st["macd_fast_hist"] = round(curr_fast, 4)
                        st["macd_slow_hist"] = round(curr_slow, 4)
                        # Basiswerte (Stand: letzte geschlossene Kerze) fuer den Live-Zero-Cross-Exit
                        st["macd_fast_ema_fast_val"] = fast_ema_fast_val
                        st["macd_fast_ema_slow_val"] = fast_ema_slow_val
                        st["macd_fast_signal_val"] = fast_signal_val

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

                            # TP-Zero-Cross wird jetzt LIVE in on_price_update mit dem aktuellen
                            # Preis-Tick geprueft (siehe ema_step) statt hier auf den naechsten
                            # Kerzenschluss zu warten - kein Poll-Loop-Flag mehr noetig.

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
                                    st["macd_fast_zero_cross_exit"] = False
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

                data = await fetch_candles_binance_multi(symbol, cfg["stoch_cross_resolution"], count_back=needed_bars)
                if data:
                    timestamps, opens, highs, lows, closes = data
                    closed_ts = timestamps[:-1]
                    closed_o, closed_h, closed_l, closed_c = opens[:-1], highs[:-1], lows[:-1], closes[:-1]

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
    - reversion (empfohlen): Ausbruch ueber/unter den Kanal wird als Gegenrichtung
      gehandelt, TP ist die tatsaechlich berechnete Mittellinie (Point of Control) -
      passt sich also automatisch der Marktlage an statt einem festen Betrag.
    - momentum: wie im Original-Indikator, Ausbruch wird in Ausbruchsrichtung gehandelt.
    In beiden Modi ist der SL ATR-basiert (marktadaptiv), im Momentum-Modus ist das TP
    ein Risk/Reward-Vielfaches des ATR-Risikos."""
    b = BOTS[symbol]
    last_osc = None
    last_processed_ts = None
    last_entry_signal_ts = None

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "range_profile":
                lookback = cfg["rp_lookback"]
                needed_bars = min(1000, lookback + max(cfg["rp_atr_period"] * 5, 60) + 5)
                data = await fetch_candles_binance_multi(symbol, cfg["rp_resolution"], count_back=needed_bars)
                if data:
                    timestamps, opens, highs, lows, closes = data
                    closed_ts = timestamps[:-1]
                    closed_o, closed_h, closed_l, closed_c = opens[:-1], highs[:-1], lows[:-1], closes[:-1]

                    if len(closed_c) > lookback + 2:
                        snap = compute_range_profile_snapshot(closed_h, closed_l, closed_c, closed_o, lookback, 50, cfg["rp_ob_os_level"])
                        atr_series = compute_atr(closed_h, closed_l, closed_c, cfg["rp_atr_period"])
                        if snap and atr_series:
                            st = b["state"]
                            curr_osc = snap["osc"]
                            curr_atr = atr_series[-1]
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
                                        risk_dist = curr_atr * cfg["rp_sl_atr_mult"]
                                        sl_price = price - risk_dist if direction == "long" else price + risk_dist
                                        if mode == "reversion":
                                            tp_price = snap["mid_price"]
                                        else:
                                            tp_price = price + risk_dist * cfg["rp_tp_rr"] if direction == "long" else price - risk_dist * cfg["rp_tp_rr"]

                                        debug_log(f"📡 [{symbol}] Range-Profile Signal ({mode}): {direction.upper()} @ {price} "
                                                  f"| Mitte {round(snap['mid_price'],4)} | SL {round(sl_price,4)} | TP {round(tp_price,4)} | Oszillator {round(curr_osc,2)}")
                                        st["rp_sl_price"] = sl_price
                                        st["rp_tp_price"] = tp_price
                                        await execute_entry(symbol, direction, price, is_add_on=False)

                                last_osc = curr_osc
                                last_processed_ts = signal_key
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Range-Profile-Abfrage fehlgeschlagen", {"error": str(e)})

        await asyncio.sleep(5)


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

            # Live-Zero-Cross: rechnet die schnelle MACD-EMA mit dem AKTUELLEN Preis-Tick
            # weiter (ausgehend vom Stand der letzten geschlossenen Kerze), statt auf den
            # naechsten Kerzenschluss zu warten - reagiert also sofort, sobald der Live-Preis
            # die Nulllinie ueberschreiten wuerde.
            live_zero_cross = False
            if cfg.get("macd_fast_zero_cross_exit_enabled", True):
                base_ema_fast = st.get("macd_fast_ema_fast_val")
                base_ema_slow = st.get("macd_fast_ema_slow_val")
                base_signal = st.get("macd_fast_signal_val")
                last_closed_fast = st.get("macd_fast_hist")
                if base_ema_fast is not None and base_ema_slow is not None and base_signal is not None and last_closed_fast is not None:
                    live_ema_fast = ema_step(base_ema_fast, price, cfg["macd_fast_fast"])
                    live_ema_slow = ema_step(base_ema_slow, price, cfg["macd_fast_slow"])
                    live_macd_line = live_ema_fast - live_ema_slow
                    live_signal = ema_step(base_signal, live_macd_line, cfg["macd_fast_signal"])
                    live_fast_hist = live_macd_line - live_signal
                    if st["position"] == "short" and last_closed_fast <= 0 and live_fast_hist > 0:
                        live_zero_cross = True
                    elif st["position"] == "long" and last_closed_fast >= 0 and live_fast_hist < 0:
                        live_zero_cross = True

            if pnl_usd <= -cfg["macd_sl_usd"]:
                await execute_exit(symbol, price, "SL")
                st["macd_fade_exit"] = False
            elif not cfg.get("macd_fast_zero_cross_exit_enabled", True) and pnl_usd >= cfg["macd_tp_usd"]:
                # Fester TP-Betrag greift nur, wenn der Nulllinien-Exit AUS ist - sonst wuerde er
                # die Position ja genau in dem Moment schliessen, den der Nulllinien-Exit bewusst
                # laufen lassen soll (Pullback-Trend geht weiter).
                await execute_exit(symbol, price, "TP")
                st["macd_fade_exit"] = False
            elif cfg.get("macd_slow_reversal_exit_enabled", True) and st.get("macd_slow_reversal_exit"):
                await execute_exit(symbol, price, "MACD-SLOW-REVERSAL")
                st["macd_slow_reversal_exit"] = False
                st["macd_fade_exit"] = False
                st["macd_slow_fade_exit"] = False
            elif live_zero_cross:
                await execute_exit(symbol, price, "MACD-ZERO-CROSS")
                st["macd_fast_zero_cross_exit"] = False
                st["macd_fade_exit"] = False
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
        if st["position"] is not None:
            sl_price = st.get("rp_sl_price")
            tp_price = st.get("rp_tp_price")
            if sl_price is not None:
                sl_hit = price <= sl_price if st["position"] == "long" else price >= sl_price
                if sl_hit:
                    await execute_exit(symbol, price, "SL")
                    st["rp_sl_price"] = None
                    st["rp_tp_price"] = None
                    return
            if tp_price is not None:
                tp_hit = price >= tp_price if st["position"] == "long" else price <= tp_price
                if tp_hit:
                    await execute_exit(symbol, price, "TP")
                    st["rp_sl_price"] = None
                    st["rp_tp_price"] = None
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
    "macd_stoch": 500_000, "stoch_cross": 500_000, "fib_reversal": 500_000, "range_profile": 30_000,
}


def _bt_close_trade(trades, direction, entry, exit_price, size, i, entry_i, reason):
    pnl = (exit_price - entry) * size if direction == "long" else (entry - exit_price) * size
    trades.append({"dir": direction, "entry": entry, "exit": exit_price, "reason": reason,
                    "pnl": pnl, "bars_held": i - entry_i})


def backtest_macd_stoch(candles, cfg):
    ts, o, h, l, c = candles
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]

    fast_hist = compute_macd_histogram(c, cfg["macd_fast_fast"], cfg["macd_fast_slow"], cfg["macd_fast_signal"])
    slow_hist = compute_macd_histogram(c, cfg["macd_slow_fast"], cfg["macd_slow_slow"], cfg["macd_slow_signal"])
    use_stoch = cfg.get("macd_use_stochastic", True)
    k_series = d_series = None
    if use_stoch:
        k_series, d_series = compute_stochastic(h, l, c, cfg["stoch_k_period"], cfg["stoch_k_smooth"], cfg["stoch_d_period"])

    warmup = cfg["macd_slow_slow"] + cfg["macd_slow_signal"] + 5
    position = None
    trades = []
    last_fast = last_slow = None

    for i in range(warmup, n):
        curr_fast, curr_slow, price = fast_hist[i], slow_hist[i], c[i]

        if position is not None:
            size, entry, direction = position["size"], position["entry"], position["dir"]
            pnl_usd = (price - entry) * size if direction == "long" else (entry - price) * size
            reason = None
            if pnl_usd <= -cfg["macd_sl_usd"]:
                reason = "SL"
            elif not cfg.get("macd_fast_zero_cross_exit_enabled", True) and pnl_usd >= cfg["macd_tp_usd"]:
                reason = "TP"
            elif cfg.get("macd_slow_reversal_exit_enabled", True) and (
                    (direction == "long" and curr_slow < 0) or (direction == "short" and curr_slow > 0)):
                reason = "MACD-SLOW-REVERSAL"
            elif cfg.get("macd_fast_zero_cross_exit_enabled", True) and last_fast is not None and (
                    (direction == "short" and last_fast <= 0 and curr_fast > 0) or
                    (direction == "long" and last_fast >= 0 and curr_fast < 0)):
                reason = "MACD-ZERO-CROSS"
            elif cfg.get("macd_fast_fade_exit_enabled", True) and last_fast is not None and (
                    (direction == "long" and curr_fast > 0 and curr_fast <= last_fast) or
                    (direction == "short" and curr_fast < 0 and curr_fast >= last_fast)):
                reason = "MACD-FADE"
            elif cfg.get("macd_slow_fade_exit_enabled", False) and last_slow is not None and (
                    (direction == "long" and curr_slow > 0 and curr_slow <= last_slow) or
                    (direction == "short" and curr_slow < 0 and curr_slow >= last_slow)):
                reason = "MACD-SLOW-FADE"
            if reason:
                _bt_close_trade(trades, direction, entry, price, size, i, position["entry_i"], reason)
                position = None

        if position is None and last_fast is not None:
            direction = None
            if curr_slow > 0 and curr_fast > 0 and curr_fast > last_fast:
                direction = "long"
            elif curr_slow < 0 and curr_fast < 0 and curr_fast < last_fast:
                direction = "short"
            if direction and use_stoch and k_series:
                stoch_k, stoch_d = k_series[i], d_series[i]
                if direction == "long" and stoch_k <= stoch_d:
                    direction = None
                elif direction == "short" and stoch_k >= stoch_d:
                    direction = None
            if direction:
                size = (margin * leverage) / price
                position = {"dir": direction, "entry": price, "size": size, "entry_i": i}

        last_fast, last_slow = curr_fast, curr_slow

    return trades


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

    atr_series = compute_atr(h, l, c, cfg["rp_atr_period"])
    warmup = lookback + cfg["rp_atr_period"] + 5
    position = None
    trades = []
    last_osc = None
    width_history = []
    squeeze_active = False

    for i in range(warmup, n):
        snap = compute_range_profile_snapshot(h[i - lookback + 1:i + 1], l[i - lookback + 1:i + 1],
                                                c[i - lookback + 1:i + 1], o[i - lookback + 1:i + 1], lookback, 50, ob)
        if snap is None:
            continue
        curr_osc, price, curr_atr = snap["osc"], c[i], atr_series[i]

        if position is not None:
            direction, entry, size = position["dir"], position["entry"], position["size"]
            sl_hit = price <= position["sl"] if direction == "long" else price >= position["sl"]
            tp_hit = price >= position["tp"] if direction == "long" else price <= position["tp"]
            if sl_hit:
                _bt_close_trade(trades, direction, entry, price, size, i, position["entry_i"], "SL")
                position = None
            elif tp_hit:
                _bt_close_trade(trades, direction, entry, price, size, i, position["entry_i"], "TP")
                position = None

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

            if direction and curr_atr:
                risk_dist = curr_atr * cfg["rp_sl_atr_mult"]
                sl_price = price - risk_dist if direction == "long" else price + risk_dist
                if mode == "momentum":
                    tp_price = price + risk_dist * cfg["rp_tp_rr"] if direction == "long" else price - risk_dist * cfg["rp_tp_rr"]
                else:
                    tp_price = snap["mid_price"]
                    valid = (direction == "long" and tp_price > price) or (direction == "short" and tp_price < price)
                    if not valid:
                        direction = None
                if direction:
                    size = (margin * leverage) / price
                    position = {"dir": direction, "entry": price, "size": size, "entry_i": i, "sl": sl_price, "tp": tp_price}

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


BACKTEST_FUNCS = {
    "macd_stoch": backtest_macd_stoch,
    "stoch_cross": backtest_stoch_cross,
    "range_profile": backtest_range_profile,
    "fib_reversal": backtest_fib_reversal,
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


_backtest_candle_cache = {}  # key: (symbol, resolution) -> {"fetched_at": float, "days": int, "candles": (...)}
BACKTEST_CACHE_TTL_SECONDS = 300  # 5 Minuten - danach gilt der Cache als zu alt und wird neu geladen


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
        return {"error": f"Backtest für '{entry_mode}' nicht unterstützt (nur macd_stoch, stoch_cross, range_profile, fib_reversal - Grid/OBI-Scalp brauchen historische Tick-/Orderbuchdaten, die es nicht gibt)."}

    resolution_key = {"macd_stoch": "macd_resolution", "stoch_cross": "stoch_cross_resolution",
                       "range_profile": "rp_resolution", "fib_reversal": "fib_resolution"}[entry_mode]
    resolution = cfg.get(resolution_key, "1m")
    max_candles = BACKTEST_MAX_CANDLES[entry_mode]

    cache_key = (symbol, resolution)
    cached = _backtest_candle_cache.get(cache_key)
    now = time.time()
    cache_used = False

    if (cached and (now - cached["fetched_at"] < BACKTEST_CACHE_TTL_SECONDS)
            and cached["days"] >= days and len(cached["candles"][4]) >= 100):
        candles = _trim_candles_to_days(cached["candles"], days, max_candles)
        err = None
        cache_used = True
    else:
        candles, err = await fetch_historical_candles_binance(symbol, resolution, days, max_candles)
        if candles:
            _backtest_candle_cache[cache_key] = {"fetched_at": now, "days": days, "candles": candles}

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
