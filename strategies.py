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
import re
from collections import deque

from bot_core import (
    debug_log, WS_URL, SYMBOLS, MARKET_INDICES, MARKET_INDEX_TO_SYMBOL,
    BOTS, execute_entry, execute_exit, execute_partial_exit, compute_step_abs, GLOBAL_SETTINGS,
)

BINANCE_SYMBOL_MAP = {
    "BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT", "DOGE": "DOGEUSDT", "XRP": "XRPUSDT",
    "LINK": "LINKUSDT", "AVAX": "AVAXUSDT", "NEAR": "NEARUSDT", "DOT": "DOTUSDT", "TON": "TONUSDT",
    "SUI": "SUIUSDT", "BNB": "BNBUSDT", "UNI": "UNIUSDT", "APT": "APTUSDT", "ADA": "ADAUSDT",
    "TRX": "TRXUSDT", "LTC": "LTCUSDT", "BCH": "BCHUSDT", "HBAR": "HBARUSDT", "ICP": "ICPUSDT",
    "XAU": "XAUUSDT", "XAG": "XAGUSDT",  # Seit Jan. 2026 auf Binance, aber NUR als USDT-Perpetual-
    # Future ("TradFi"-Kategorie) - es gibt dafuer KEIN Spot-Paar, siehe BINANCE_FUTURES_ONLY_SYMBOLS
    # HYPE und WTI/Forex (EURUSD, ...) gibt es weiterhin nicht auf Binance - dafuer greift der Lighter-Fallback
}

BINANCE_FUTURES_ONLY_SYMBOLS = {"XAU", "XAG"}  # existieren auf Binance NUR als Futures, kein Spot-Paar


# Globale Anfragen-Drossel: OHNE das feuern alle 7 Poll-Loops (binance_1s, da, es, ht,
# scalp_board, quad_stoch, oms_rsi) x alle aktiven Coins IM SELBEN MOMENT, weil sie alle exakt
# 5 Sekunden schlafen und beim Bot-Start fast gleichzeitig gestartet sind - das bleibt fuer immer
# synchron (klassisches "Thundering Herd"-Muster) und riss in der Praxis das Rate-Limit, obwohl
# die DURCHSCHNITTLICHE Anfragenrate eigentlich im gruenen Bereich gewesen waere. Diese Drossel
# verteilt alle Binance-Anfragen (ueber alle Coins/Loops hinweg) gleichmaessig statt in Buendeln.
_binance_last_request_ts = 0.0
_binance_throttle_lock = None  # wird beim ersten Gebrauch lazy angelegt (braucht einen laufenden Event-Loop)
BINANCE_MIN_REQUEST_INTERVAL = 0.15  # Sekunden zwischen zwei Binance-Anfragen = max. ~6.7 Anfragen/Sekunde global


async def _binance_throttle():
    global _binance_last_request_ts, _binance_throttle_lock
    if _binance_throttle_lock is None:
        _binance_throttle_lock = asyncio.Lock()
    async with _binance_throttle_lock:
        now = time.time()
        wait = BINANCE_MIN_REQUEST_INTERVAL - (now - _binance_last_request_ts)
        if wait > 0:
            await asyncio.sleep(wait)
        _binance_last_request_ts = time.time()


BINANCE_BASE_URLS = {
    "spot": "https://api.binance.com/api/v3/klines",
    "futures": "https://fapi.binance.com/fapi/v1/klines",  # USD-M Perpetual Futures - gleiche
    # Symbolnamen wie Spot (z.B. "BTCUSDT"), aber eigener Preis (leicht abweichend von Spot,
    # das ist die Quelle, die z.B. "BTCUSDT.P" auf TradingView zeigt)
}

# Globaler IP-Bann-Schutz: Binance antwortet bei zu vielen Anfragen mit HTTP 418 und einem
# "banned until <epoch_ms>"-Zeitstempel. OHNE diesen Schutz wuerden alle Poll-Loops (viele Coins
# x viele Strategien, jede alle 5s) WEITER Anfragen stellen, WAEHREND der Bann noch laeuft - das
# verlaengert den Bann bei jeder weiteren Anfrage nur immer weiter (beobachtet: "banned until"
# stieg mit jeder neuen Zeile im Log). Spot (api.binance.com) und Futures (fapi.binance.com) sind
# getrennte Dienste mit eigenen Rate-Limits, deshalb getrennte Bann-Zeiten je market_type.
_binance_ban_until_ms = {"spot": 0.0, "futures": 0.0}
_binance_ban_logged_until = {"spot": 0.0, "futures": 0.0}  # verhindert Log-Spam waehrend des Banns


def _binance_is_banned(market_type):
    return time.time() * 1000 < _binance_ban_until_ms.get(market_type, 0.0)


def _binance_register_ban(market_type, symbol, status, body_text):
    import re
    match = re.search(r"banned until (\d+)", body_text)
    if match:
        until_ms = int(match.group(1))
    else:
        # Kein Zeitstempel im Body gefunden (z.B. einfaches 429 ohne Bann) - trotzdem
        # sicherheitshalber 60 Sekunden pausieren, statt sofort weiter zu haemmern
        until_ms = time.time() * 1000 + 60_000
    if until_ms > _binance_ban_until_ms.get(market_type, 0.0):
        _binance_ban_until_ms[market_type] = until_ms
    if _binance_ban_logged_until.get(market_type, 0.0) < until_ms:
        _binance_ban_logged_until[market_type] = until_ms
        wait_s = max(0, (until_ms - time.time() * 1000) / 1000)
        debug_log(f"🚫 [Binance-{market_type}] IP-Bann erkannt (HTTP {status}, ausgelöst durch {symbol}) - "
                  f"pausiere ALLE {market_type}-Anfragen für {round(wait_s)}s (bis {time.strftime('%H:%M:%S', time.localtime(until_ms/1000))})")


async def fetch_candles_binance(symbol, resolution, count_back=150, market_type="spot"):
    """Alternative Kerzenquelle - Binance hat deutlich mehr Liquiditaet als Lighter,
    kann daher weniger anfaellig fuer kurze Preis-Spikes/Wicks sein, die auf einer
    kleineren Perp-DEX Fehlsignale ausloesen wuerden. market_type waehlt zwischen Binance-Spot
    (Standard) und Binance-USD-M-Futures (Perpetual) - falls man 1:1 mit einem TradingView-Chart
    auf ".P"-Symbolen vergleichen will, braucht man 'futures', da Spot- und Futures-Kurs leicht
    voneinander abweichen."""
    pair = BINANCE_SYMBOL_MAP.get(symbol)
    if not pair:
        return None
    if resolution == "1s" and symbol in BINANCE_FUTURES_ONLY_SYMBOLS:
        # Weder Spot (kein XAUUSDT/XAGUSDT-Paar) noch Futures (keine 1s-Kerzen) koennen das
        # liefern - erst gar keine Anfrage stellen statt sie mit Sicherheit fehlschlagen zu
        # lassen (das hat vorher alle 5s "Invalid interval" produziert, da die "XAU/XAG->Futures"-
        # Regel faelschlich Vorrang vor der "1s->immer Spot"-Regel hatte).
        return None
    try:
        # Binance Futures (fapi) bietet KEINE 1-Sekunden-Kerzen an (nur Spot) - die sind aber
        # die Grundlage fuer alle Sekunden-Aufloesungen (10s/15s/30s/45s). Diese Regel hat
        # IMMER Vorrang vor der XAU/XAG-Futures-Regel (siehe Check oben), sonst wuerde "1s" an
        # den Futures-Endpunkt gehen, der Sekunden-Intervalle gar nicht kennt ("Invalid interval").
        if resolution == "1s":
            effective_market_type = "spot"
        elif symbol in BINANCE_FUTURES_ONLY_SYMBOLS:
            effective_market_type = "futures"  # XAU/XAG gibt's nur als Future, kein Spot-Paar
        else:
            effective_market_type = market_type

        if _binance_is_banned(effective_market_type):
            return None  # aktiver Bann - keine Anfrage stellen, das wuerde ihn nur verlaengern

        base_url = BINANCE_BASE_URLS.get(effective_market_type, BINANCE_BASE_URLS["spot"])
        url = f"{base_url}?symbol={pair}&interval={resolution}&limit={min(count_back, 1000)}"
        await _binance_throttle()
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status in (418, 429):
                    body = await resp.text()
                    _binance_register_ban(effective_market_type, symbol, resp.status, body)
                    return None
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


SYNTHETIC_RESOLUTIONS = {"10s": ("1s", 10), "15s": ("1s", 15), "30s": ("1s", 30), "45s": ("1s", 45), "2m": ("1m", 2)}  # Zeitrahmen, die Binance nicht nativ anbietet
NATIVE_BINANCE_MINUTE_INTERVALS = {1, 3, 5, 15, 30}  # von Binance nativ unterstuetzte Minuten-Intervalle


def resolve_synthetic_resolution(resolution):
    """Gibt (Basis-Aufloesung, Faktor) zurueck, falls 'resolution' aus einer kleineren nativen
    Binance-Aufloesung zusammengesetzt werden muss - sonst None (native Aufloesung, direkt
    abrufbar). Deckt sowohl die festen Sekunden-Faelle (10s/15s/30s/45s aus 1s) als auch JEDE
    beliebige, nicht-native Minutenzahl ab (z.B. '8m' oder '24m' aus 1m-Kerzen zusammengesetzt) -
    Binance selbst bietet nativ nur 1m/3m/5m/15m/30m an, alles andere muss client-seitig
    zusammengefasst werden."""
    if resolution in SYNTHETIC_RESOLUTIONS:
        return SYNTHETIC_RESOLUTIONS[resolution]
    m = re.match(r"^(\d+)m$", resolution)
    if m:
        minutes = int(m.group(1))
        if minutes > 0 and minutes not in NATIVE_BINANCE_MINUTE_INTERVALS:
            return ("1m", minutes)
    return None


async def fetch_historical_candles_binance(symbol, resolution, days, max_candles, market_type="spot"):
    """Holt bis zu 'days' Tage Kerzenhistorie von Binance fuer Backtests, in 1000er-
    Batches paginiert (endTime schrittweise nach hinten). '2m'/'30s' werden - wie live -
    aus 1m- bzw. 1s-Kerzen synthetisch zusammengesetzt (siehe SYNTHETIC_RESOLUTIONS).
    max_candles begrenzt hart, wie viele Kerzen am Ende verarbeitet werden
    (Performance-Schutz fuer den Render-Server). market_type: 'spot' oder 'futures'."""
    pair = BINANCE_SYMBOL_MAP.get(symbol)
    if not pair:
        return None, "Coin nicht auf Binance verfügbar"

    synth = resolve_synthetic_resolution(resolution)
    base_resolution = synth[0] if synth else resolution
    fetch_factor = synth[1] if synth else 1
    total_ms = days * 24 * 60 * 60 * 1000
    end_time = int(time.time() * 1000)
    start_time = end_time - total_ms
    # Hartes Limit an Basis-Kerzen (vor evtl. Zusammenfassung), damit die Anfrage nicht ausufert
    hard_candle_cap = max_candles * fetch_factor + 2000
    # Binance Futures (fapi) bietet KEINE 1-Sekunden-Kerzen an (nur Spot) - ohne diesen
    # Fallback wuerde jede Sekunden-Aufloesung (10s/15s/30s/45s) mit "Keine Daten erhalten"
    # fehlschlagen, sobald "futures" als Datenquelle eingestellt ist.
    effective_market_type = "spot" if base_resolution == "1s" and market_type == "futures" else market_type
    if symbol in BINANCE_FUTURES_ONLY_SYMBOLS:
        effective_market_type = "futures"  # XAU/XAG gibt's nur als Future, kein Spot-Paar - hat
        # Vorrang, ausser bei Sekunden-Aufloesungen (die gehen fuer XAU/XAG dann leider gar
        # nicht, da es weder ein Spot-Paar noch 1s-Futures-Kerzen gibt)
        if base_resolution == "1s":
            return None, "Sekunden-Auflösungen (10s/15s/30s/45s) sind für XAU/XAG nicht möglich - Binance bietet dafür weder ein Spot-Paar noch 1-Sekunden-Futures-Kerzen an."
    base_url = BINANCE_BASE_URLS.get(effective_market_type, BINANCE_BASE_URLS["spot"])

    all_rows = []
    cursor = end_time
    requests_made = 0
    try:
        async with aiohttp.ClientSession() as session:
            while cursor > start_time and len(all_rows) < hard_candle_cap:
                if _binance_is_banned(effective_market_type):
                    # Aktiver Bann (von dieser oder einer ANDEREN gleichzeitig laufenden Coin-
                    # Abfrage ausgeloest) - sofort abbrechen statt weiter zu haemmern, das
                    # wuerde den Bann nur verlaengern.
                    wait_s = max(0, (_binance_ban_until_ms.get(effective_market_type, 0.0) - time.time() * 1000) / 1000)
                    return None, f"Binance-IP-Bann aktiv, noch ca. {round(wait_s)}s - bitte warten und erneut versuchen."

                url = f"{base_url}?symbol={pair}&interval={base_resolution}&limit=1000&endTime={cursor}"
                await _binance_throttle()
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status in (418, 429):
                        # NICHT blind mit kurzer Pause wiederholen - das hat den Bann in der
                        # Praxis immer weiter verlaengert. Stattdessen die tatsaechliche
                        # "banned until"-Zeit aus der Antwort lesen, global fuer ALLE
                        # gleichzeitig laufenden Coins sperren, und sofort abbrechen.
                        body = await resp.text()
                        _binance_register_ban(effective_market_type, symbol, resp.status, body)
                        wait_s = max(0, (_binance_ban_until_ms.get(effective_market_type, 0.0) - time.time() * 1000) / 1000)
                        return None, f"Binance-Ratelimit erreicht (IP-Bann für ca. {round(wait_s)}s) - bitte warten und erneut versuchen."
                    if resp.status != 200:
                        batch = None
                    else:
                        batch = await resp.json()
                if batch is None:
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
        # Sekunden-Basis (10s/15s/30s/45s aus 1s-Kerzen) braucht die SEKUNDEN-Bucket-Funktion,
        # nicht die Minuten-basierte resample_candles() - sonst entsteht derselbe Bug wie vorher
        # bei get_seconds_candles() (30-Minuten- statt 30-Sekunden-Buckets, praktisch nie genug
        # Kerzen pro Bucket). Minuten-Basis (z.B. 2m aus 1m-Kerzen) nutzt weiterhin die normale
        # resample_candles(), da deren Minuten-Mathematik dafuer korrekt ist.
        if base_resolution == "1s":
            timestamps, opens, highs, lows, closes = _resample_seconds_candles((timestamps, opens, highs, lows, closes), synth[1])
        else:
            timestamps, opens, highs, lows, closes = resample_candles((timestamps, opens, highs, lows, closes), synth[1])

    if len(closes) > max_candles:
        timestamps = timestamps[-max_candles:]
        opens = opens[-max_candles:]
        highs = highs[-max_candles:]
        lows = lows[-max_candles:]
        closes = closes[-max_candles:]

    return (timestamps, opens, highs, lows, closes), None


async def fetch_historical_candles_binance_vol(symbol, resolution, days, max_candles, market_type="spot"):
    """Wie fetch_historical_candles_binance, liefert zusaetzlich das Handelsvolumen - fuer MO7
    (braucht MFI). Bewusst eine eigene, einfachere Variante statt die grosse Funktion umzubauen:
    NUR native Binance-Intervalle werden unterstuetzt (kein 2m/10s/15s/30s/45s/custom - die
    muessten sonst auch volumen-bewusst nachgebaut werden, was den Umfang stark aufblaeht).
    MO7 auf 1m/3m/5m/15m/30m/1h/2h/4h einzuschraenken ist dafuer ein vertretbarer Kompromiss."""
    pair = BINANCE_SYMBOL_MAP.get(symbol)
    if not pair:
        return None, "Coin nicht auf Binance verfügbar"
    total_ms = days * 24 * 60 * 60 * 1000
    end_time = int(time.time() * 1000)
    start_time = end_time - total_ms
    hard_candle_cap = max_candles + 2000
    base_url = BINANCE_BASE_URLS.get(market_type, BINANCE_BASE_URLS["spot"])

    all_rows = []
    cursor = end_time
    requests_made = 0
    try:
        async with aiohttp.ClientSession() as session:
            while cursor > start_time and len(all_rows) < hard_candle_cap:
                if _binance_is_banned(market_type):
                    wait_s = max(0, (_binance_ban_until_ms.get(market_type, 0.0) - time.time() * 1000) / 1000)
                    return None, f"Binance-IP-Bann aktiv, noch ca. {round(wait_s)}s - bitte warten und erneut versuchen."
                url = f"{base_url}?symbol={pair}&interval={resolution}&limit=1000&endTime={cursor}"
                await _binance_throttle()
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status in (418, 429):
                        body = await resp.text()
                        _binance_register_ban(market_type, symbol, resp.status, body)
                        wait_s = max(0, (_binance_ban_until_ms.get(market_type, 0.0) - time.time() * 1000) / 1000)
                        return None, f"Binance-Ratelimit erreicht (IP-Bann für ca. {round(wait_s)}s) - bitte warten und erneut versuchen."
                    if resp.status != 200:
                        batch = None
                    else:
                        batch = await resp.json()
                if batch is None:
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

    if len(closes) > max_candles:
        timestamps, opens, highs, lows, closes, volumes = (
            timestamps[-max_candles:], opens[-max_candles:], highs[-max_candles:],
            lows[-max_candles:], closes[-max_candles:], volumes[-max_candles:])

    return (timestamps, opens, highs, lows, closes, volumes), None


async def fetch_candles_binance_vol(symbol, resolution, count_back=150):
    """Wie fetch_candles_binance, liefert zusaetzlich das Handelsvolumen pro Kerze -
    fuer Strategien wie BLSH-Composite, die Volumen brauchen (z.B. MFI). Bewusst
    eine eigene Funktion statt die bestehende zu erweitern, um nicht die vielen
    bestehenden Aufrufer (die ein 5er-Tupel erwarten) zu gefaehrden."""
    pair = BINANCE_SYMBOL_MAP.get(symbol)
    if not pair:
        return None
    if _binance_is_banned("spot"):
        return None  # aktiver Bann - keine Anfrage stellen, das wuerde ihn nur verlaengern
    try:
        await _binance_throttle()
        url = f"https://api.binance.com/api/v3/klines?symbol={pair}&interval={resolution}&limit={min(count_back, 1000)}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status in (418, 429):
                    body = await resp.text()
                    _binance_register_ban("spot", symbol, resp.status, body)
                    return None
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
    Binance-1s-Puffer faktisch nie genug Kerzen zurueckgaben.

    ZWEITER BUG (behoben): frueher wurde ein Bucket nur akzeptiert, wenn er EXAKT 'seconds'
    Roh-Kerzen enthielt (j - i == seconds). Reale 1s-Daten haben aber gelegentlich kleine
    Luecken (WS-Reconnect, verpasster Tick, Rate-Limit) - bei 45s faellt EIN fehlender Tick
    kaum ins Gewicht (~2% der Kerze), bei 10s/15s aber viel staerker (~7-10%), wodurch bei
    kurzen Aufloesungen SEHR VIEL MEHR Buckets komplett verworfen wurden und die Strategie
    dort effektiv kaum neue, abgeschlossene Kerzen bekam (== kaum je ein neuer Trigger-Wechsel,
    obwohl der Kurs sich eigentlich bewegt hat). Jetzt wird jeder BEREITS VERGANGENE Bucket
    auch mit weniger Ticks akzeptiert (er ist ja trotzdem echt abgeschlossen) - nur der ALLER-
    LETZTE Bucket (koennte die gerade noch laufende, unfertige Kerze sein) muss weiterhin
    vollstaendig sein, sonst wird er verworfen (Repainting-Schutz bleibt erhalten)."""
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
        is_last_bucket = j == n
        complete_enough = (j - i == seconds) if is_last_bucket else (j - i >= 1)
        if complete_enough:
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
            market_type = b["config"].get("binance_market_type", "spot")
            seed, err = await fetch_historical_candles_binance(symbol, "1s", days=0.125, max_candles=10800, market_type=market_type)  # ~3 Stunden
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
                # count_back klein halten (nicht mehr 1000!) - wir brauchen bei einem 5-Sekunden-
                # Poll-Intervall nur eine kleine Ueberlappung zurueck, um bereits gespeicherte,
                # aber von Binance zwischenzeitlich noch nachtraeglich stabilisierte/korrigierte
                # juengste 1s-Kerzen zu UEBERSCHREIBEN statt sie fuer immer im alten (moeglicherweise
                # unvollstaendigen) Zustand haengen zu lassen. Frueher wurden hier 1000 Kerzen pro
                # Abfrage geholt, aber praktisch nur die paar neuesten benutzt - der Rest wurde
                # verworfen und hat nur unnoetig Bandbreite gekostet (siehe Render-Bandbreitenlimit).
                # 20 Sekunden Ueberlappung reichen bei 5s-Poll-Abstand mit deutlichem Sicherheitspuffer.
                data = await fetch_candles_binance(symbol, "1s", count_back=20)
                if data:
                    timestamps, opens, highs, lows, closes = data
                    buffer = st.get("binance_1s_buffer", [])
                    by_ts = {c["ts"]: idx for idx, c in enumerate(buffer[-25:])}  # nur die Ueberlappungszone durchsuchen, nicht den ganzen Puffer
                    offset = len(buffer) - len(buffer[-25:])
                    for i in range(len(timestamps)):
                        new_candle = {"ts": timestamps[i], "o": opens[i], "h": highs[i], "l": lows[i], "c": closes[i]}
                        if timestamps[i] in by_ts:
                            buffer[offset + by_ts[timestamps[i]]] = new_candle  # bereits vorhanden -> mit frischem Wert ueberschreiben
                        else:
                            buffer.append(new_candle)  # wirklich neu -> anhaengen
                    if len(buffer) > 10000:  # ~2.75 Stunden 1s-Historie (reduziert wegen Speicherlimit)
                        buffer = buffer[-10000:]
                    st["binance_1s_buffer"] = buffer
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Binance-1s-Puffer-Abfrage fehlgeschlagen", {"error": str(e)})
        await asyncio.sleep(5)


async def fetch_candles_binance_multi(symbol, resolution, count_back=150, market_type="spot"):
    """Wie fetch_candles_binance, kann aber zusaetzlich synthetische Zeitrahmen liefern
    (z.B. 2m oder eigene Minutenwerte wie 8m/24m), die Binance selbst nicht unterstuetzt -
    dafuer wird die naechstkleinere native Aufloesung geholt und zu groesseren Kerzen
    zusammengefasst."""
    synth = resolve_synthetic_resolution(resolution)
    if synth:
        base_resolution, factor = synth
        data = await fetch_candles_binance(symbol, base_resolution, count_back=count_back * factor, market_type=market_type)
        if data is None:
            return None
        if base_resolution == "1s":
            return _resample_seconds_candles(data, factor)
        return resample_candles(data, factor)
    return await fetch_candles_binance(symbol, resolution, count_back=count_back, market_type=market_type)


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


def _sma_series(values, length):
    n = len(values)
    out = [0.0] * n
    for i in range(n):
        start = max(0, i - length + 1)
        window = values[start:i + 1]
        out[i] = sum(window) / len(window)
    return out


def compute_rolling_zscore(closes, lookback, smooth):
    """Portiert aus 'Rolling Z-Score Trend [QuantAlgo]' (Pine v6): misst, wie viele
    Standardabweichungen der aktuelle Kurs vom gleitenden Durchschnitt der letzten 'lookback'
    Kerzen entfernt ist (SMA + STDEV), danach leicht geglaettet (EMA 'smooth'). Wird hier als
    generischer, wiederverwendbarer Baustein fuer den optionalen Long/Short-Filter bei mehreren
    Strategien genutzt (>0 = Long erlaubt, <0 = Short erlaubt)."""
    n = len(closes)
    mean = _sma_series(closes, lookback)
    z_raw = [0.0] * n
    for i in range(n):
        start = max(0, i - lookback + 1)
        window = closes[start:i + 1]
        m = len(window)
        if m > 1:
            avg = sum(window) / m
            variance = sum((x - avg) ** 2 for x in window) / m
            stdev = variance ** 0.5
        else:
            stdev = 0.0
        z_raw[i] = (closes[i] - mean[i]) / stdev if stdev > 0 else 0.0
    return _ema_series(z_raw, smooth)


def compute_diamond_supertrend(highs, lows, closes, factor, atr_period):
    """Portiert aus 'Diamond Algo' (Pine v5) - der SuperTrend-Kernbaustein (Standard-SuperTrend-
    Algorithmus). factor = Sensitivity * 2 (siehe Original: supertrend(close, nsensitivity*2, 11)).
    Gibt (supertrend_line, direction) zurueck - direction 1 = bullisch (Linie = unteres Band),
    -1 = baerisch (Linie = oberes Band), wie im Original-Skript (NICHT dieselbe Konvention wie bei
    compute_halftrend, dort ist 0=bullisch - hier bewusst beim Original-Vorzeichen geblieben)."""
    n = len(closes)
    if n == 0:
        return [], []
    atr = compute_atr(highs, lows, closes, atr_period)
    lower_band_prev = 0.0
    upper_band_prev = 0.0
    st_prev = None
    st_out = [0.0] * n
    dir_out = [1] * n
    for i in range(n):
        basic_upper = closes[i] + factor * atr[i]
        basic_lower = closes[i] - factor * atr[i]
        prev_close = closes[i - 1] if i > 0 else closes[i]

        lower_band = basic_lower if (basic_lower > lower_band_prev or prev_close < lower_band_prev) else lower_band_prev
        upper_band = basic_upper if (basic_upper < upper_band_prev or prev_close > upper_band_prev) else upper_band_prev

        if i == 0:
            direction = 1
        elif st_prev == upper_band_prev:
            direction = -1 if closes[i] > upper_band else 1
        else:
            direction = 1 if closes[i] < lower_band else -1

        st = lower_band if direction == -1 else upper_band
        st_out[i] = st
        dir_out[i] = direction

        lower_band_prev = lower_band
        upper_band_prev = upper_band
        st_prev = st
    return st_out, dir_out


def compute_wma_series(values, period):
    """Gewichteter gleitender Durchschnitt (Pine's wma) - je juenger der Wert, desto hoeher das
    Gewicht (1..period)."""
    n = len(values)
    out = [None] * n
    denom = period * (period + 1) / 2
    for i in range(n):
        if i < period - 1:
            continue
        window = values[i - period + 1:i + 1]
        out[i] = sum(window[j] * (j + 1) for j in range(period)) / denom
    return out


def compute_hull_ma(closes, period):
    """Hull Moving Average (Pine's HMA): WMA(2*WMA(n/2) - WMA(n), sqrt(n)) - reagiert schneller
    und mit weniger Verzoegerung als eine normale MA. Gibt die HMA-Linie zurueck; die Farbe
    (gruen/rot) ergibt sich aus dem Vorzeichen der Steigung (aktueller Wert vs. vorheriger)."""
    n = len(closes)
    half = max(1, round(period / 2))
    sqn = max(1, round(math.sqrt(period)))
    wma_half = compute_wma_series(closes, half)
    wma_full = compute_wma_series(closes, period)
    diff = [(2 * wma_half[i] - wma_full[i]) if (wma_half[i] is not None and wma_full[i] is not None) else None for i in range(n)]
    # compute_wma_series braucht `sqn` weitere echte Werte danach - fehlende (None) Stellen am
    # Anfang einfach mit dem ersten gueltigen Wert auffuellen, damit compute_wma_series darauf
    # rechnen kann (matcht die Pine-Verhalten "na" ohnehin nur als Warmup-Bereich).
    first_valid = next((x for x in diff if x is not None), 0.0)
    diff_filled = [x if x is not None else first_valid for x in diff]
    hma = compute_wma_series(diff_filled, sqn)
    return hma


def compute_ut_bot(opens, highs, lows, closes, atr_period, sensitivity, use_heikin_ashi=False):
    """UT Bot Alerts (weit verbreitetes Pine-Script): ATR-Trailing-Stop-Linie, BUY wenn der Kurs
    (bzw. Heikin-Ashi-Kurs) von unten nach oben ueber die Stop-Linie kreuzt, SELL umgekehrt.
    Gibt (buy, sell, stop_line) zurueck."""
    if use_heikin_ashi:
        _, h_src, l_src, src = compute_heikin_ashi(opens, highs, lows, closes)
    else:
        h_src, l_src, src = highs, lows, closes
    atr = compute_atr(h_src, l_src, src, atr_period)
    n = len(src)
    nloss = [sensitivity * (atr[i] or 0) for i in range(n)]
    stop = [0.0] * n
    for i in range(n):
        prev_stop = stop[i - 1] if i > 0 else 0.0
        prev_src = src[i - 1] if i > 0 else src[i]
        if src[i] > prev_stop and prev_src > prev_stop:
            stop[i] = max(prev_stop, src[i] - nloss[i])
        elif src[i] < prev_stop and prev_src < prev_stop:
            stop[i] = min(prev_stop, src[i] + nloss[i])
        else:
            stop[i] = src[i] - nloss[i] if src[i] > prev_stop else src[i] + nloss[i]

    buy = [False] * n
    sell = [False] * n
    for i in range(1, n):
        above = src[i - 1] <= stop[i - 1] and src[i] > stop[i]
        below = src[i - 1] >= stop[i - 1] and src[i] < stop[i]
        buy[i] = src[i] > stop[i] and above
        sell[i] = src[i] < stop[i] and below
    return buy, sell, stop


def compute_heikin_ashi(opens, highs, lows, closes):
    """Rechnet normale OHLC-Kerzen in Heikin-Ashi-Kerzen um (wie bei TradingView, wenn man den
    Chart-Typ auf 'Heikin Ashi' umstellt). Heikin-Ashi glaettet den Kursverlauf, indem jede Kerze
    den Durchschnitt der vorherigen mit einrechnet - Trends wirken dadurch 'glatter' (weniger
    kleine Gegenkerzen), Wendepunkte fallen dafuer etwas verzoegert auf. Gibt (ha_open, ha_high,
    ha_low, ha_close) zurueck - diese vier werden dann anstelle der normalen OHLC-Werte in die
    Signal-Berechnung (compute_diamond_signal, compute_atr) gegeben."""
    n = len(closes)
    ha_close = [(opens[i] + highs[i] + lows[i] + closes[i]) / 4 for i in range(n)]
    ha_open = [0.0] * n
    ha_high = [0.0] * n
    ha_low = [0.0] * n
    for i in range(n):
        ha_open[i] = (opens[i] + closes[i]) / 2 if i == 0 else (ha_open[i - 1] + ha_close[i - 1]) / 2
        ha_high[i] = max(highs[i], ha_open[i], ha_close[i])
        ha_low[i] = min(lows[i], ha_open[i], ha_close[i])
    return ha_open, ha_high, ha_low, ha_close


def compute_elte_supertrend(opens, highs, lows, closes, factor, atr_period):
    """Portiert aus 'ELTE SMART' (Pine v5) - SuperTrend auf ohlc4 (Durchschnitt aus O/H/L/C)
    statt nur Close, etwas geglaettet. WICHTIG: andere Vorzeichen-Konvention als bei Diamond
    Algo! Hier gilt die STANDARD-Konvention: direction=1 -> lowerBand (bullisch, Linie unter dem
    Kurs), direction=-1 -> upperBand (baerisch, Linie ueber dem Kurs) - im Original-Skript exakt
    so verdrahtet (anders als beim Diamond-Algo-Original, das die Zuordnung vertauscht hatte).
    'factor' darf ein fester Wert ODER eine pro-Kerze wechselnde Liste sein (fuer Auto-Sensitivity,
    da die sich mit der Marktvolatilitaet Kerze fuer Kerze aendert)."""
    n = len(closes)
    if n == 0:
        return [], []
    is_series = isinstance(factor, (list, tuple))
    ohlc4 = [(opens[i] + highs[i] + lows[i] + closes[i]) / 4 for i in range(n)]
    atr = compute_atr(highs, lows, closes, atr_period)
    lower_band_prev = 0.0
    upper_band_prev = 0.0
    st_prev = None
    st_out = [0.0] * n
    dir_out = [1] * n
    for i in range(n):
        f = factor[i] if is_series else factor
        basic_upper = ohlc4[i] + f * atr[i]
        basic_lower = ohlc4[i] - f * atr[i]
        prev_close = closes[i - 1] if i > 0 else closes[i]

        lower_band = basic_lower if (basic_lower > lower_band_prev or prev_close < lower_band_prev) else lower_band_prev
        upper_band = basic_upper if (basic_upper < upper_band_prev or prev_close > upper_band_prev) else upper_band_prev

        if i == 0:
            direction = 1
        elif st_prev == upper_band_prev:
            direction = 1 if closes[i] > upper_band else -1
        else:
            direction = -1 if closes[i] < lower_band else 1

        st = lower_band if direction == 1 else upper_band
        st_out[i] = st
        dir_out[i] = direction

        lower_band_prev = lower_band
        upper_band_prev = upper_band
        st_prev = st
    return st_out, dir_out


def compute_es_auto_sensitivity(closes, vol_period, vol_ma_len):
    """Portiert aus 'ELTE SMART' - EWMA-Volatilitaet (Lambda-gewichteter gleitender Durchschnitt
    der quadrierten Log-Returns), dann Vergleich mit dem eigenen 55er-Durchschnitt in relativen
    Baendern (60%/20%/140%/180%/240%) -> daraus ergibt sich automatisch ein SuperTrend-
    Sensitivity-Wert zwischen 2.85 und 4.0, je nachdem ob die aktuelle Volatilitaet ueber/unter
    ihrem eigenen historischen Schnitt liegt. Der Annualisierungsfaktor (sqrt(365)*100 im
    Original) kuerzt sich in den relativen Bandvergleichen komplett heraus und wird hier deshalb
    weggelassen (aendert das Ergebnis nicht, nur die absolute Hv-Skala)."""
    n = len(closes)
    if n < 2:
        return [3.0] * n
    lam = (vol_period - 1) / (vol_period + 1)
    logr = [0.0] + [math.log(closes[i] / closes[i - 1]) if closes[i - 1] > 0 else 0.0 for i in range(1, n)]
    squared = [x * x for x in logr]
    v = [0.0] * n
    for i in range(n):
        v[i] = squared[i] if i == 0 else lam * v[i - 1] + (1 - lam) * squared[i]
    hv = [math.sqrt(x) for x in v]
    avg_hv = _sma_series(hv, vol_ma_len)

    sensitivity = [3.0] * n
    for i in range(n):
        h, a = hv[i], avg_hv[i]
        maa, mab, mac = a * 1.40, a * 1.80, a * 2.40
        mad_, mae_ = a * 0.60, a * 0.20
        if h < maa and h > a:
            sensitivity[i] = 3.15
        elif h < mab and h > maa:
            sensitivity[i] = 3.5
        elif h < mac and h > mab:
            sensitivity[i] = 3.6
        elif h > mac:
            sensitivity[i] = 4.0
        elif h < maa and h > mad_:
            sensitivity[i] = 3.0
        elif h < mad_ and h > mae_:
            sensitivity[i] = 2.85
        elif h < mae_:
            sensitivity[i] = 3.0
    return sensitivity


def compute_diamond_signal(highs, lows, closes, atr_period, sensitivity, sma_period, ema_trend_period):
    """Komplettes Diamond-Algo-Signal: SuperTrend-Crossover + SMA-Filter (Basis-Signal), plus
    200er-EMA-Trendfilter fuer die 'Smart'-Qualifizierung (im Original nur Label-Text, hier ein
    echter, waehlbarer Filter - siehe da_signal_mode). Gibt (buy, sell, smart_buy, smart_sell)
    als Bool-Listen zurueck."""
    n = len(closes)
    factor = sensitivity * 2
    st_line, _ = compute_diamond_supertrend(highs, lows, closes, factor, atr_period)
    sma = _sma_series(closes, sma_period)
    ema200 = _ema_series(closes, ema_trend_period)

    buy = [False] * n
    sell = [False] * n
    smart_buy = [False] * n
    smart_sell = [False] * n
    for i in range(1, n):
        crossover = closes[i - 1] <= st_line[i - 1] and closes[i] > st_line[i]
        crossunder = closes[i - 1] >= st_line[i - 1] and closes[i] < st_line[i]
        buy[i] = crossover and closes[i] >= sma[i]
        sell[i] = crossunder and closes[i] <= sma[i]
        smart_buy[i] = buy[i] and closes[i] > ema200[i]
        smart_sell[i] = sell[i] and closes[i] < ema200[i]
    return buy, sell, smart_buy, smart_sell


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
                    data = await fetch_candles_binance_multi(symbol, resolution, count_back=needed_bars, market_type=cfg.get("binance_market_type", "spot"))
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

                    # Nachhol-Mechanismus (siehe ELTE Smart fuer die ausfuehrliche Begruendung):
                    # normalerweise hoechstens EINE neue Kerze zwischen zwei 5-Sekunden-Polls,
                    # aber bei kurzen Aufloesungen kann der Loop mal hinterherhinken - ohne diesen
                    # Nachholmechanismus gingen dazwischenliegende Signale fuer immer verloren.
                    if last_processed_ts is None:
                        new_indices = [len(closed_ts) - 1]
                    else:
                        try:
                            last_idx = closed_ts.index(last_processed_ts)
                            new_indices = list(range(last_idx + 1, len(closed_ts)))
                        except ValueError:
                            new_indices = [len(closed_ts) - 1]

                    for idx in new_indices:
                        if idx < 1:
                            continue
                        buy_signal = trend[idx] == 0 and trend[idx - 1] == 1
                        sell_signal = trend[idx] == 1 and trend[idx - 1] == 0
                        if invert:
                            buy_signal, sell_signal = sell_signal, buy_signal
                        price_i = price if idx == len(closed_ts) - 1 else closed_c[idx]
                        last_processed_ts = closed_ts[idx]
                        if cfg.get("ht_exit_trigger", "candle_close") == "candle_close":
                            await check_ht_exit(symbol, buy_signal, sell_signal, price_i)
                        if cfg.get("ht_entry_trigger", "candle_close") == "candle_close":
                            await check_ht_entry(symbol, buy_signal, sell_signal, price_i, atr2[idx])

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


def _da_reset_state(st):
    st["da_sl_price"] = None
    st["da_tp_price"] = None


async def check_da_sl_tp(symbol, price):
    """Prueft SL/TP - beide als fester $-Betrag auf die GESAMTE Position, bei Einstieg einmalig
    aus ATR(da_risk_atr_period)*da_risk_mult berechnet (wie im Original: atrBand = ta.atr(atrLen)
    * atrRisk), TP-Abstand = SL-Abstand * da_tp_rr (Original hat TP1/TP2/TP3 bei 1:1/2:1/3:1 -
    hier ein frei waehlbarer einzelner R:R-Multiplikator statt gestufter Teilverkaeufe)."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if st["position"] is None or price is None:
        return
    pos = st["position"]
    sl_price = st.get("da_sl_price")
    tp_price = st.get("da_tp_price")
    if sl_price is None and tp_price is None:
        return
    hit_sl = sl_price is not None and ((pos == "long" and price <= sl_price) or (pos == "short" and price >= sl_price))
    hit_tp = tp_price is not None and ((pos == "long" and price >= tp_price) or (pos == "short" and price <= tp_price))
    if hit_sl:
        debug_log(f"🚪 [{symbol}] Diamond Algo SL: {pos.upper()} @ {price} (Ziel war {round(sl_price, 4)})")
        await execute_exit(symbol, price, "SL")
        st["da_sl_cooldown_until"] = time.time() + cfg.get("da_sl_cooldown_seconds", 30)
        _da_reset_state(st)
    elif hit_tp:
        debug_log(f"🚪 [{symbol}] Diamond Algo TP: {pos.upper()} @ {price} (Ziel war {round(tp_price, 4)})")
        await execute_exit(symbol, price, "TP")
        _da_reset_state(st)


async def check_da_entry(symbol, buy_signal, sell_signal, price, atr_risk_now):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or st["position"] is not None or price is None:
        return
    if time.time() < st.get("da_sl_cooldown_until", 0.0):
        return
    if not (buy_signal or sell_signal):
        return
    direction = "long" if buy_signal else "short"
    debug_log(f"📡 [{symbol}] Diamond Algo Signal: {direction.upper()} @ {price}")
    await execute_entry(symbol, direction, price, is_add_on=False)
    if st["position"] is None:
        return
    _da_reset_state(st)
    if cfg.get("da_sl_enabled", True) and atr_risk_now is not None:
        dist_sl = atr_risk_now * cfg.get("da_risk_mult", 1.0)
        st["da_sl_price"] = price - dist_sl if direction == "long" else price + dist_sl
        if cfg.get("da_tp_enabled", True):
            dist_tp = dist_sl * cfg.get("da_tp_rr", 1.0)
            st["da_tp_price"] = price + dist_tp if direction == "long" else price - dist_tp
    elif cfg.get("da_tp_enabled", True) and atr_risk_now is not None:
        # TP auch ohne SL moeglich (dann wird der R:R-Multiplikator direkt auf den Risiko-ATR-
        # Abstand angewandt, ohne dass ein SL tatsaechlich gesetzt wird)
        dist_tp = atr_risk_now * cfg.get("da_risk_mult", 1.0) * cfg.get("da_tp_rr", 1.0)
        st["da_tp_price"] = price + dist_tp if direction == "long" else price - dist_tp


async def check_da_exit(symbol, buy_signal, sell_signal, price):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or st["position"] is None or price is None:
        return
    if st["position"] == "long" and sell_signal:
        debug_log(f"🚪 [{symbol}] Diamond Algo Exit: LONG @ {price} (Sell-Signal)")
        await execute_exit(symbol, price, "DA-FLIP-EXIT")
        _da_reset_state(st)
    elif st["position"] == "short" and buy_signal:
        debug_log(f"🚪 [{symbol}] Diamond Algo Exit: SHORT @ {price} (Buy-Signal)")
        await execute_exit(symbol, price, "DA-FLIP-EXIT")
        _da_reset_state(st)


async def da_poll_loop(symbol):
    """Diamond Algo (portiert aus dem gleichnamigen Pine-v5-Indikator) - nur der Signal-Kern:
    SuperTrend(Sensitivity*2, ATR-Periode) mit SMA-Filter fuer Buy/Sell, plus optionaler 200er-
    EMA-Trendfilter fuer 'Smart'-Signale (im Original nur Label-Text, hier ein echter waehlbarer
    Filter - siehe da_signal_mode). SL/TP optional, ATR-basiert (wie im Original: atrBand =
    ta.atr(atrLen) * atrRisk), TP als R:R-Vielfaches vom SL-Abstand. Ausstieg sonst immer beim
    Gegen-Signal (Flip-System). Kein Supply/Demand, keine Trend Cloud/Session-Anzeige - das war
    im Original rein optisch/Beiwerk ohne Einfluss auf Buy/Sell."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "diamond_algo" and cfg["bot_active"]:
                resolution = cfg["da_resolution"]
                atr_period = cfg["da_atr_period"]
                risk_atr_period = cfg.get("da_risk_atr_period", 14)
                sma_period = cfg["da_sma_period"]
                ema_trend_period = cfg["da_ema_trend_period"]
                min_needed = max(atr_period, risk_atr_period, sma_period, ema_trend_period) + 5
                needed_bars = min(1000, max(min_needed * 2, 200))
                st = b["state"]

                if resolution in SUB_MINUTE_RESOLUTIONS:
                    local = get_seconds_candles(st, SUB_MINUTE_RESOLUTIONS[resolution], needed_bars)
                    if local:
                        closed_ts, closed_o, closed_h, closed_l, closed_c = local
                    else:
                        closed_ts = None
                else:
                    data = await fetch_candles_binance_multi(symbol, resolution, count_back=needed_bars, market_type=cfg.get("binance_market_type", "spot"))
                    if data:
                        timestamps, opens, highs, lows, closes = data
                        closed_ts, closed_o, closed_h, closed_l, closed_c = timestamps[:-1], opens[:-1], highs[:-1], lows[:-1], closes[:-1]
                    else:
                        closed_ts = None

                now = time.time()
                due_heartbeat = now - last_heartbeat > 300

                if closed_ts and len(closed_c) > min_needed:
                    signal_key = closed_ts[-1]
                    is_new_candle = last_processed_ts != signal_key
                    price = st["last_price"] if st["last_price"] is not None else closed_c[-1]

                    if cfg.get("da_use_heikin_ashi", False):
                        # Heikin-Ashi-Umrechnung VOR der Signal-Berechnung - wie bei TradingView,
                        # wenn man den Chart-Typ umstellt. Reale Preise (price/last_price) bleiben
                        # fuer die tatsaechliche Order-Ausfuehrung unveraendert, nur das SIGNAL
                        # selbst rechnet auf den geglaetteten HA-Kerzen.
                        _, sig_h, sig_l, sig_c = compute_heikin_ashi(closed_o, closed_h, closed_l, closed_c)
                    else:
                        sig_h, sig_l, sig_c = closed_h, closed_l, closed_c

                    buy, sell, smart_buy, smart_sell = compute_diamond_signal(
                        sig_h, sig_l, sig_c, atr_period, cfg["da_sensitivity"], sma_period, ema_trend_period)
                    atr_risk_series = compute_atr(sig_h, sig_l, sig_c, risk_atr_period)
                    keep = min_needed + 5
                    st["da_opens"] = closed_o[-keep:]
                    st["da_highs"] = closed_h[-keep:]
                    st["da_lows"] = closed_l[-keep:]
                    st["da_closes"] = closed_c[-keep:]
                    invert = cfg.get("da_invert_direction", False)
                    signal_mode = cfg.get("da_signal_mode", "all")
                    raw_direction = 1 if buy[-1] else (-1 if sell[-1] else st.get("da_direction"))
                    st["da_direction"] = raw_direction * (-1 if invert else 1) if raw_direction is not None else None
                    st["da_atr_risk_last"] = atr_risk_series[-1]

                    if due_heartbeat:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] Diamond Algo aktiv: Preis={closed_c[-1]}, ATR-Risk={round(atr_risk_series[-1],4)}, "
                                  f"Modus={signal_mode}, Kerzen={len(closed_c)}, bot_active={cfg['bot_active']}")

                    # Nachhol-Mechanismus (siehe ELTE Smart fuer die ausfuehrliche Begruendung):
                    # normalerweise hoechstens EINE neue Kerze zwischen zwei 5-Sekunden-Polls,
                    # aber bei kurzen Aufloesungen kann der Loop mal hinterherhinken - ohne diesen
                    # Nachholmechanismus gingen dazwischenliegende Signale fuer immer verloren.
                    if last_processed_ts is None:
                        new_indices = [len(closed_ts) - 1]
                    else:
                        try:
                            last_idx = closed_ts.index(last_processed_ts)
                            new_indices = list(range(last_idx + 1, len(closed_ts)))
                        except ValueError:
                            new_indices = [len(closed_ts) - 1]

                    for idx in new_indices:
                        if idx < 1:
                            continue
                        buy_i = smart_buy[idx] if signal_mode == "smart_only" else buy[idx]
                        sell_i = smart_sell[idx] if signal_mode == "smart_only" else sell[idx]
                        if invert:
                            buy_i, sell_i = sell_i, buy_i
                        price_i = price if idx == len(closed_ts) - 1 else closed_c[idx]
                        last_processed_ts = closed_ts[idx]
                        if cfg.get("da_exit_trigger", "candle_close") == "candle_close":
                            await check_da_exit(symbol, buy_i, sell_i, price_i)
                        if cfg.get("da_entry_trigger", "candle_close") == "candle_close":
                            await check_da_entry(symbol, buy_i, sell_i, price_i, atr_risk_series[idx])

                    await check_da_sl_tp(symbol, price)
                elif due_heartbeat:
                    last_heartbeat = now
                    if not closed_ts:
                        debug_log(f"⏳ [{symbol}] Diamond Algo wartet: keine Kerzen erhalten (Auflösung {resolution})")
                    else:
                        debug_log(f"⏳ [{symbol}] Diamond Algo wartet: zu wenig Kerzen ({len(closed_c)}/{min_needed + 1} nötig)")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Diamond-Algo-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

        await asyncio.sleep(5)


def _es_reset_state(st):
    st["es_sl_price"] = None
    st["es_tp1_price"] = None
    st["es_tp2_price"] = None
    st["es_tp3_price"] = None
    st["es_tp1_done"] = False
    st["es_tp2_done"] = False
    st["es_breakeven_pct_done"] = False


async def check_es_sl_tp(symbol, price):
    """Prueft SL sowie TP1/TP2/TP3 (im Original nur Preis-Linien zur Orientierung, hier als
    echte Teilverkaeufe umgesetzt - analog HalfTrend, aber mit einem zusaetzlichen Schritt, den
    das Original nicht hat: SL springt nach TP1 auf Break-Even UND nach TP2 nochmal weiter auf
    den TP1-Preis (statt auf Break-Even stehen zu bleiben) - so ist ab TP2 immer schon ein
    Teilgewinn abgesichert, nicht nur die Einstiegsposition."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if st["position"] is None or price is None:
        return
    pos = st["position"]

    # Prozent-Break-Even (eigenstaendig, unabhaengig vom TP1-Break-Even): sobald sich der Kurs
    # um X% in die richtige Richtung bewegt hat, SL sofort auf Einstieg ziehen - auch wenn TP1
    # noch laengst nicht erreicht ist. Verbessert den SL nur (nie verschlechtern), laeuft nur
    # einmal pro Position.
    if cfg.get("es_breakeven_pct_enabled", False) and not st.get("es_breakeven_pct_done"):
        entry = st["avg_entry_price"]
        trigger_pct = cfg.get("es_breakeven_trigger_pct", 0.1) / 100
        moved_pct = (price - entry) / entry if pos == "long" else (entry - price) / entry
        if moved_pct >= trigger_pct:
            current_sl = st.get("es_sl_price")
            if current_sl is None or (pos == "long" and entry > current_sl) or (pos == "short" and entry < current_sl):
                st["es_sl_price"] = entry
                debug_log(f"📡 [{symbol}] ELTE Smart Prozent-Break-Even ausgelöst ({trigger_pct*100:.2f}% erreicht) - SL auf Einstieg ({round(entry,4)}) gesetzt")
            st["es_breakeven_pct_done"] = True

    sl_price = st.get("es_sl_price")
    if sl_price is not None:
        hit_sl = (pos == "long" and price <= sl_price) or (pos == "short" and price >= sl_price)
        if hit_sl:
            if st.get("es_tp2_done"):
                reason = "TP1-LOCK"
            elif st.get("es_tp1_done"):
                reason = "BREAKEVEN"
            elif st.get("es_breakeven_pct_done") and abs(sl_price - st["avg_entry_price"]) < 1e-9:
                reason = "BREAKEVEN-PCT"
            else:
                reason = "SL"
            debug_log(f"🚪 [{symbol}] ELTE Smart {reason}: {pos.upper()} @ {price} (Ziel war {round(sl_price, 4)})")
            await execute_exit(symbol, price, reason)
            if reason == "SL":
                st["es_sl_cooldown_until"] = time.time() + cfg.get("es_sl_cooldown_seconds", 30)
            _es_reset_state(st)
            return

    if not st.get("es_tp1_done") and st.get("es_tp1_price") is not None:
        tp1_price = st["es_tp1_price"]
        if (pos == "long" and price >= tp1_price) or (pos == "short" and price <= tp1_price):
            if cfg.get("es_tp_mode", "atr") == "manual":
                # Fester TP-$-Betrag = EIN einzelnes Ziel, komplette Position schliesst dort -
                # kein Teilverkauf, kein TP2/TP3 (die sind in diesem Modus ohnehin nicht gesetzt),
                # genau wie beim festen SL auch nur ein einzelner Wert ist.
                debug_log(f"🚪 [{symbol}] ELTE Smart TP (fest): {pos.upper()} @ {price} (Ziel war {round(tp1_price, 4)})")
                await execute_exit(symbol, price, "TP")
                _es_reset_state(st)
                return
            fraction = cfg.get("es_tp1_close_pct", 50) / 100
            ok = await execute_partial_exit(symbol, price, fraction, "TP1")
            if ok:
                st["es_tp1_done"] = True
                st["es_sl_price"] = st["avg_entry_price"]  # Break-Even
                debug_log(f"📡 [{symbol}] ELTE Smart TP1 erreicht - SL auf Break-Even ({round(st['avg_entry_price'],4)}) gesetzt")
        return

    if not st.get("es_tp2_done") and st.get("es_tp2_price") is not None:
        tp2_price = st["es_tp2_price"]
        if (pos == "long" and price >= tp2_price) or (pos == "short" and price <= tp2_price):
            fraction = cfg.get("es_tp2_close_pct", 50) / 100
            ok = await execute_partial_exit(symbol, price, fraction, "TP2")
            if ok:
                st["es_tp2_done"] = True
                st["es_sl_price"] = st["es_tp1_price"]  # SL zieht weiter auf TP1-Preis
                debug_log(f"📡 [{symbol}] ELTE Smart TP2 erreicht - SL auf TP1-Preis ({round(st['es_tp1_price'],4)}) gezogen")
        return

    tp3_price = st.get("es_tp3_price")
    if tp3_price is not None:
        if (pos == "long" and price >= tp3_price) or (pos == "short" and price <= tp3_price):
            debug_log(f"🚪 [{symbol}] ELTE Smart TP3 (Rest): {pos.upper()} @ {price}")
            await execute_exit(symbol, price, "TP3")
            _es_reset_state(st)


async def check_es_entry(symbol, buy_signal, sell_signal, price, risk_atr_now, signal_low=None, signal_high=None):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or st["position"] is not None or price is None:
        return
    if time.time() < st.get("es_sl_cooldown_until", 0.0):
        return
    if not (buy_signal or sell_signal):
        return
    direction = "long" if buy_signal else "short"
    debug_log(f"📡 [{symbol}] ELTE Smart Signal: {direction.upper()} @ {price}")
    await execute_entry(symbol, direction, price, is_add_on=False)
    if st["position"] is None:
        return
    _es_reset_state(st)

    sl_enabled = cfg.get("es_sl_enabled", True)
    tp_enabled = cfg.get("es_tp_enabled", True)
    if not sl_enabled and not tp_enabled:
        return  # reines Flip-System - keine SL-/TP-Preise setzen, check_es_sl_tp hat dann nichts zu tun

    if risk_atr_now is not None:
        atr_band = risk_atr_now * cfg.get("es_risk_mult", 2.2)

        # Original-Formel: atrStop = trigger ? low - atrBand : high + atrBand - der SL geht vom
        # TIEF/HOCH der Signalkerze aus, NICHT vom Schlusskurs (Entry). TP1/TP2/TP3 sind dann
        # Vielfache des TATSAECHLICHEN Einstieg-zu-SL-Abstands (der dadurch groesser ist als der
        # reine ATR-Wert, wegen des Kerzendochts) - nicht des reinen ATR-Bands. Fallback auf die
        # alte, einfachere Rechnung, falls kein Tief/Hoch übergeben wurde (z.B. Tick-Trigger ohne
        # Kerzendaten).
        if sl_enabled and cfg.get("es_sl_mode", "atr") == "manual":
            size = st.get("total_coin_size") or 0
            manual_usd = cfg.get("es_sl_manual_usd", 5.0)
            dist_sl = (manual_usd / size) if size > 0 else atr_band  # fester $-Verlust auf die GESAMTE Position umgerechnet in Preisabstand
            sl_price = price - dist_sl if direction == "long" else price + dist_sl
            dist_for_tp = atr_band  # TP bleibt bei manuellem SL rein ATR-basiert, wie besprochen
        else:
            if direction == "long" and signal_low is not None:
                sl_price = signal_low - atr_band
            elif direction == "short" and signal_high is not None:
                sl_price = signal_high + atr_band
            else:
                sl_price = price - atr_band if direction == "long" else price + atr_band
            dist_for_tp = abs(price - sl_price)  # ECHTER Einstieg-zu-SL-Abstand, wie im Original

        if direction == "long":
            if sl_enabled:
                st["es_sl_price"] = sl_price
            if tp_enabled:
                if cfg.get("es_tp_mode", "atr") == "manual":
                    size = st.get("total_coin_size") or 0
                    tp_manual_usd = cfg.get("es_tp_manual_usd", 5.0)
                    dist_tp = (tp_manual_usd / size) if size > 0 else dist_for_tp
                    st["es_tp1_price"] = price + dist_tp  # EIN Ziel, TP2/TP3 bleiben unbenutzt (None)
                else:
                    st["es_tp1_price"] = price + dist_for_tp * cfg.get("es_tp1_rr", 1.0)
                    st["es_tp2_price"] = price + dist_for_tp * cfg.get("es_tp2_rr", 2.0)
                    st["es_tp3_price"] = price + dist_for_tp * cfg.get("es_tp3_rr", 3.0)
        else:
            if sl_enabled:
                st["es_sl_price"] = sl_price
            if tp_enabled:
                if cfg.get("es_tp_mode", "atr") == "manual":
                    size = st.get("total_coin_size") or 0
                    tp_manual_usd = cfg.get("es_tp_manual_usd", 5.0)
                    dist_tp = (tp_manual_usd / size) if size > 0 else dist_for_tp
                    st["es_tp1_price"] = price - dist_tp
                else:
                    st["es_tp1_price"] = price - dist_for_tp * cfg.get("es_tp1_rr", 1.0)
                    st["es_tp2_price"] = price - dist_for_tp * cfg.get("es_tp2_rr", 2.0)
                    st["es_tp3_price"] = price - dist_for_tp * cfg.get("es_tp3_rr", 3.0)


async def check_es_exit(symbol, buy_signal, sell_signal, price):
    """Ausstieg immer beim Gegen-Signal (schliesst den kompletten Rest, egal welche TP-Stufe
    gerade aktiv ist). Gibt True zurueck, wenn gerade ein Flip-Exit passiert ist - der Aufrufer
    nutzt das, um (falls es_reenter_on_flip aus ist, Standard) den Einstieg fuer DIESEN Bar zu
    ueberspringen, auch wenn dasselbe Signal technisch auch eine neue Position eroeffnen wuerde.
    So wird auf ein wirklich NEUES Signal gewartet, statt sofort in die Gegenrichtung zu drehen."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or st["position"] is None or price is None:
        return False
    if st["position"] == "long" and sell_signal:
        debug_log(f"🚪 [{symbol}] ELTE Smart Exit: LONG @ {price} (Sell-Signal)")
        await execute_exit(symbol, price, "ES-FLIP-EXIT")
        _es_reset_state(st)
        return True
    elif st["position"] == "short" and buy_signal:
        debug_log(f"🚪 [{symbol}] ELTE Smart Exit: SHORT @ {price} (Buy-Signal)")
        await execute_exit(symbol, price, "ES-FLIP-EXIT")
        _es_reset_state(st)
        return True
    return False


async def es_poll_loop(symbol):
    """ELTE Smart (portiert aus dem gleichnamigen Pine-v5-Indikator, nur 'Normal'-Modus + Auto-
    Sensitivity) - SuperTrend(ohlc4) mit automatisch aus der Marktvolatilitaet abgeleiteter
    Sensitivity (siehe compute_es_auto_sensitivity), reiner Crossover-Trigger ohne Zusatzfilter.
    TP1(50%)->Break-Even, TP2(50% vom Rest)->SL auf TP1, TP3(Rest) - alles ATR-basiert
    (Risiko-ATR-Periode x Risiko-Multiplikator), Ausstieg sonst immer beim Gegen-Signal."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "elte_smart" and cfg["bot_active"]:
                resolution = cfg["es_resolution"]
                atr_period = cfg["es_atr_period"]
                risk_atr_period = cfg.get("es_risk_atr_period", 14)
                vol_period = cfg.get("es_vol_period", 10)
                vol_ma_len = cfg.get("es_vol_ma_len", 55)
                min_needed = max(atr_period, risk_atr_period, vol_ma_len + vol_period) + 5
                needed_bars = min(1000, max(min_needed * 2, 200))
                st = b["state"]

                if resolution in SUB_MINUTE_RESOLUTIONS:
                    local = get_seconds_candles(st, SUB_MINUTE_RESOLUTIONS[resolution], needed_bars)
                    if local:
                        closed_ts, closed_o, closed_h, closed_l, closed_c = local
                    else:
                        closed_ts = None
                else:
                    data = await fetch_candles_binance_multi(symbol, resolution, count_back=needed_bars, market_type=cfg.get("binance_market_type", "spot"))
                    if data:
                        timestamps, opens, highs, lows, closes = data
                        closed_ts, closed_o, closed_h, closed_l, closed_c = timestamps[:-1], opens[:-1], highs[:-1], lows[:-1], closes[:-1]
                    else:
                        closed_ts = None

                now = time.time()
                due_heartbeat = now - last_heartbeat > 300

                if closed_ts and len(closed_c) > min_needed:
                    signal_key = closed_ts[-1]
                    is_new_candle = last_processed_ts != signal_key
                    price = st["last_price"] if st["last_price"] is not None else closed_c[-1]

                    if cfg.get("es_auto_sensitivity", True):
                        sensitivity = compute_es_auto_sensitivity(closed_c, vol_period, vol_ma_len)
                    else:
                        sensitivity = cfg.get("es_sensitivity", 3.0)
                    st_line, direction = compute_elte_supertrend(closed_o, closed_h, closed_l, closed_c, sensitivity, atr_period)
                    risk_atr_series = compute_atr(closed_h, closed_l, closed_c, risk_atr_period)

                    keep = min_needed + 5
                    st["es_opens"] = closed_o[-keep:]
                    st["es_highs"] = closed_h[-keep:]
                    st["es_lows"] = closed_l[-keep:]
                    st["es_closes"] = closed_c[-keep:]

                    invert = cfg.get("es_invert_direction", False)
                    st["es_direction"] = direction[-1] * (-1 if invert else 1)
                    st["es_sensitivity_last"] = sensitivity[-1] if isinstance(sensitivity, list) else sensitivity
                    st["es_risk_atr_last"] = risk_atr_series[-1]

                    if due_heartbeat:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] ELTE Smart aktiv: Preis={closed_c[-1]}, Sensitivity={round(st['es_sensitivity_last'],2)}, "
                                  f"Risk-ATR={round(risk_atr_series[-1],4)}, Kerzen={len(closed_c)}, bot_active={cfg['bot_active']}")

                    # Nachhol-Mechanismus: normalerweise ist zwischen zwei 5-Sekunden-Polls
                    # hoechstens EINE neue Kerze fertig geworden - aber bei kurzen Aufloesungen
                    # (10s/15s) und vielen gleichzeitig laufenden Coins/Strategien im selben
                    # Event-Loop kann der Poll-Loop mal kurz hinterherhinken. Wuerde man dann nur
                    # die JEWEILS NEUESTE Kerze pruefen, gingen alle dazwischenliegenden Kerzen
                    # (und moegliche Signale darauf) fuer immer verloren - das erklaerte den
                    # beobachteten Unterschied zwischen Live (wenige Trades) und Backtest (viele
                    # Trades) fuer denselben Zeitraum, da der Backtest jede Kerze einzeln abarbeitet.
                    if last_processed_ts is None:
                        new_indices = [len(closed_ts) - 1]
                    else:
                        try:
                            last_idx = closed_ts.index(last_processed_ts)
                            new_indices = list(range(last_idx + 1, len(closed_ts)))
                        except ValueError:
                            new_indices = [len(closed_ts) - 1]  # alter Zeitstempel aus dem Fenster gefallen

                    for idx in new_indices:
                        if idx < 1:
                            continue
                        buy_i = closed_c[idx - 1] <= st_line[idx - 1] and closed_c[idx] > st_line[idx]
                        sell_i = closed_c[idx - 1] >= st_line[idx - 1] and closed_c[idx] < st_line[idx]
                        if invert:
                            buy_i, sell_i = sell_i, buy_i
                        # Bei der neuesten Kerze den aktuellen Live-Preis nutzen (praeziser),
                        # bei nachtraeglich aufgeholten (bereits vergangenen) Kerzen deren
                        # eigenen Schlusskurs - der Live-Preis hat sich ja laengst weiterbewegt.
                        price_i = price if idx == len(closed_ts) - 1 else closed_c[idx]
                        last_processed_ts = closed_ts[idx]
                        just_flipped = False
                        if cfg.get("es_exit_trigger", "candle_close") == "candle_close":
                            just_flipped = await check_es_exit(symbol, buy_i, sell_i, price_i)
                        if cfg.get("es_entry_trigger", "candle_close") == "candle_close":
                            if not just_flipped or cfg.get("es_reenter_on_flip", False):
                                await check_es_entry(symbol, buy_i, sell_i, price_i, risk_atr_series[idx], signal_low=closed_l[idx], signal_high=closed_h[idx])

                    await check_es_sl_tp(symbol, price)
                elif due_heartbeat:
                    last_heartbeat = now
                    if not closed_ts:
                        debug_log(f"⏳ [{symbol}] ELTE Smart wartet: keine Kerzen erhalten (Auflösung {resolution})")
                    else:
                        debug_log(f"⏳ [{symbol}] ELTE Smart wartet: zu wenig Kerzen ({len(closed_c)}/{min_needed + 1} nötig)")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] ELTE-Smart-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

        await asyncio.sleep(5)


def compute_smma_series(values, length):
    """Smoothed Moving Average (Pine f_smma) - Startwert ist der einfache SMA der ersten
    `length` Werte, danach rekursiv smma[i] = (smma[i-1]*(length-1) + value[i]) / length.
    Wird nur fuer den optionalen 'Strict'-Filter des Engulfing-Patterns gebraucht (MA1/MA4)."""
    n = len(values)
    out = [None] * n
    if n < length:
        return out
    out[length - 1] = sum(values[:length]) / length
    for i in range(length, n):
        out[i] = (out[i - 1] * (length - 1) + values[i]) / length
    return out


def compute_three_line_strike(opens, closes, rsi=None, strict=True):
    """3 Line Strike (portiert aus dem TMA-Overlay-Pine-Script): bullSig = 3 rote Kerzen in Folge,
    danach eine gruene Kerze die ueber das Open der 1. der 3 roten Kerzen schliesst (bearSig
    spiegelverkehrt). Im 'Strict'-Modus zusaetzlich wie im Pine-Script: RSI(14) muss zur
    Signalrichtung passen (bullisch nur wenn RSI > 50, bearisch nur wenn RSI < 50) - das raw
    Muster feuert sonst auf kurzen Zeitrahmen viel zu oft/zu ungefiltert."""
    n = len(closes)
    bull = [False] * n
    bear = [False] * n
    for i in range(3, n):
        bull_raw = (closes[i - 3] < opens[i - 3] and closes[i - 2] < opens[i - 2]
                    and closes[i - 1] < opens[i - 1] and closes[i] > opens[i - 1])
        bear_raw = (closes[i - 3] > opens[i - 3] and closes[i - 2] > opens[i - 2]
                    and closes[i - 1] > opens[i - 1] and closes[i] < opens[i - 1])
        if strict and rsi is not None and rsi[i] is not None:
            bull[i] = bull_raw and rsi[i] > 50
            bear[i] = bear_raw and rsi[i] < 50
        elif strict:
            continue  # RSI noch nicht verfuegbar (Warmup) -> kein Signal
        else:
            bull[i] = bull_raw
            bear[i] = bear_raw
    return bull, bear


def compute_engulfing(opens, closes, ma1=None, ma4=None, strict=True):
    """Engulfing ('Big A$$ Candles', portiert aus dem TMA-Overlay-Pine-Script). Im 'Strict'-Modus
    zusaetzlich verlangt: Schlusskurs zwischen MA1(21, SMMA) und MA4(200, SMMA) - genau wie im
    Original (typeofMA1-Default war SMMA, hier fest so uebernommen statt aller 9 waehlbaren
    MA-Typen aus dem Original, um den Umfang nicht zu sprengen)."""
    n = len(closes)
    bull = [False] * n
    bear = [False] * n
    for i in range(1, n):
        open_cur, close_cur = opens[i], closes[i]
        open_prev, close_prev = opens[i - 1], closes[i - 1]
        base_bull = open_cur <= close_prev and open_cur < open_prev and close_cur > open_prev
        base_bear = open_cur >= close_prev and open_cur > open_prev and close_cur < open_prev
        if strict:
            if ma1 is None or ma4 is None or ma1[i] is None or ma4[i] is None:
                continue
            bull[i] = base_bull and close_cur < ma1[i] and close_cur > ma4[i]
            bear[i] = base_bear and close_cur > ma1[i] and close_cur < ma4[i]
        else:
            bull[i] = base_bull
            bear[i] = base_bear
    return bull, bear


def compute_cp_signals(opens, highs, lows, closes, cfg):
    """Kombiniert die gewaehlte(n) Signalquelle(n) (cp_signal_source: three_line_strike /
    engulfing / both) zu einem gemeinsamen Buy/Sell-Signal-Array."""
    source = cfg.get("cp_signal_source", "three_line_strike")
    n = len(closes)
    bull = [False] * n
    bear = [False] * n
    if source in ("three_line_strike", "both"):
        tls_strict = cfg.get("cp_three_line_strict", True)
        rsi = compute_rsi(closes, 14) if tls_strict else None
        b1, s1 = compute_three_line_strike(opens, closes, rsi=rsi, strict=tls_strict)
        bull = [a or b for a, b in zip(bull, b1)]
        bear = [a or b for a, b in zip(bear, s1)]
    if source in ("engulfing", "both"):
        strict = cfg.get("cp_engulfing_strict", True)
        ma1 = compute_smma_series(closes, 21) if strict else None
        ma4 = compute_smma_series(closes, 200) if strict else None
        b2, s2 = compute_engulfing(opens, closes, ma1, ma4, strict)
        bull = [a or b for a, b in zip(bull, b2)]
        bear = [a or b for a, b in zip(bear, s2)]
    return bull, bear


def _cp_reset_state(st):
    st["cp_sl_price"] = None
    st["cp_tp_price"] = None
    st["cp_breakeven_done"] = False


async def check_cp_sl_tp(symbol, price):
    """SL/TP je einzeln (ATR-basiert ODER fester $-Betrag, siehe check_cp_entry) plus optionaler
    ATR-Breakeven: sobald sich der Kurs um cp_breakeven_trigger_mult x Risiko-ATR in die richtige
    Richtung bewegt hat, wandert der SL auf den Einstiegspreis (wie bei 'The Phoenix', dort fest
    0.5x - hier einstellbar). Verbessert den SL nur, verschlechtert ihn nie."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if st["position"] is None or price is None:
        return
    pos = st["position"]

    if cfg.get("cp_breakeven_enabled", True) and not st.get("cp_breakeven_done") and st.get("cp_risk_atr_last") is not None:
        entry = st["avg_entry_price"]
        trigger_dist = st["cp_risk_atr_last"] * cfg.get("cp_breakeven_trigger_mult", 0.5)
        moved = (price - entry) if pos == "long" else (entry - price)
        if trigger_dist > 0 and moved >= trigger_dist:
            current_sl = st.get("cp_sl_price")
            if current_sl is None or (pos == "long" and entry > current_sl) or (pos == "short" and entry < current_sl):
                st["cp_sl_price"] = entry
                debug_log(f"📡 [{symbol}] Candle-Patterns Breakeven ausgelöst - SL auf Einstieg ({round(entry,4)}) gesetzt")
            st["cp_breakeven_done"] = True

    sl_price = st.get("cp_sl_price")
    tp_price = st.get("cp_tp_price")
    if sl_price is None and tp_price is None:
        return
    hit_sl = sl_price is not None and ((pos == "long" and price <= sl_price) or (pos == "short" and price >= sl_price))
    hit_tp = tp_price is not None and ((pos == "long" and price >= tp_price) or (pos == "short" and price <= tp_price))
    if hit_sl:
        reason = "BREAKEVEN" if st.get("cp_breakeven_done") and abs(sl_price - st["avg_entry_price"]) < 1e-9 else "SL"
        debug_log(f"🚪 [{symbol}] Candle-Patterns {reason}: {pos.upper()} @ {price} (Ziel war {round(sl_price, 4)})")
        await execute_exit(symbol, price, reason)
        if reason == "SL":
            st["cp_sl_cooldown_until"] = time.time() + cfg.get("cp_sl_cooldown_seconds", 30)
        _cp_reset_state(st)
    elif hit_tp:
        debug_log(f"🚪 [{symbol}] Candle-Patterns TP: {pos.upper()} @ {price} (Ziel war {round(tp_price, 4)})")
        await execute_exit(symbol, price, "TP")
        _cp_reset_state(st)


async def check_cp_entry(symbol, buy_signal, sell_signal, price, risk_atr_now):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or st["position"] is not None or price is None:
        return
    if time.time() < st.get("cp_sl_cooldown_until", 0.0):
        return
    direction_mode = cfg.get("cp_direction_mode", "both")
    if direction_mode == "long_only":
        sell_signal = False
    elif direction_mode == "short_only":
        buy_signal = False
    if not (buy_signal or sell_signal):
        return
    direction = "long" if buy_signal else "short"
    debug_log(f"📡 [{symbol}] Candle-Patterns Signal: {direction.upper()} @ {price}")
    await execute_entry(symbol, direction, price, is_add_on=False)
    if st["position"] is None:
        return
    _cp_reset_state(st)
    st["cp_risk_atr_last"] = risk_atr_now

    sl_enabled = cfg.get("cp_sl_enabled", True)
    tp_enabled = cfg.get("cp_tp_enabled", True)
    if not sl_enabled and not tp_enabled:
        return  # reines Flip-System - keine SL-/TP-Preise setzen
    if risk_atr_now is None:
        return

    size = st.get("total_coin_size") or 0
    atr_band = risk_atr_now * cfg.get("cp_risk_mult", 1.5)
    dist_for_tp = atr_band
    if sl_enabled:
        if cfg.get("cp_sl_mode", "atr") == "manual":
            manual_usd = cfg.get("cp_sl_manual_usd", 5.0)
            dist_sl = (manual_usd / size) if size > 0 else atr_band
            dist_for_tp = dist_sl  # TP-Basis folgt dem manuellen SL-Abstand, wie bei ELTE Smart
        else:
            dist_sl = atr_band
        st["cp_sl_price"] = price - dist_sl if direction == "long" else price + dist_sl
    if tp_enabled:
        if cfg.get("cp_tp_mode", "atr") == "manual":
            manual_usd = cfg.get("cp_tp_manual_usd", 5.0)
            dist_tp = (manual_usd / size) if size > 0 else dist_for_tp
        else:
            dist_tp = dist_for_tp * cfg.get("cp_tp_rr", 1.0)
        st["cp_tp_price"] = price + dist_tp if direction == "long" else price - dist_tp


async def check_cp_exit(symbol, buy_signal, sell_signal, price):
    """Optionaler Flip-Exit beim Gegen-Signal (cp_flip_exit_enabled, Standard An). Gibt True
    zurueck wenn gerade geflippt wurde, damit derselbe Bar nicht sofort wieder einen Einstieg
    ausloest."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or st["position"] is None or price is None or not cfg.get("cp_flip_exit_enabled", True):
        return False
    if st["position"] == "long" and sell_signal:
        debug_log(f"🚪 [{symbol}] Candle-Patterns Exit: LONG @ {price} (Gegen-Signal)")
        await execute_exit(symbol, price, "CP-FLIP-EXIT")
        _cp_reset_state(st)
        return True
    elif st["position"] == "short" and buy_signal:
        debug_log(f"🚪 [{symbol}] Candle-Patterns Exit: SHORT @ {price} (Gegen-Signal)")
        await execute_exit(symbol, price, "CP-FLIP-EXIT")
        _cp_reset_state(st)
        return True
    return False


async def cp_poll_loop(symbol):
    """Candle-Patterns (3 Line Strike / Engulfing, portiert aus dem TMA-Overlay-Pine-Script) als
    eigenstaendige Einstiegs-Strategie, Risk-Management-Rahmen 1:1 nach dem Vorbild von ELTE
    Smart (SL/TP je einzeln ATR-basiert ODER fester $-Betrag, Cooldown nach SL) plus einem
    ATR-basierten Breakeven (wie bei 'The Phoenix'). Einstieg/Ausstieg immer bei Kerzenschluss
    (kein Tick-Trigger, anders als bei Diamond Algo/ELTE Smart - Candle-Patterns brauchen per
    Definition eine abgeschlossene Kerze)."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "candle_patterns" and cfg["bot_active"]:
                resolution = cfg.get("cp_resolution", "5m")
                risk_atr_period = cfg.get("cp_risk_atr_period", 14)
                source = cfg.get("cp_signal_source", "three_line_strike")
                needs_ma = source in ("engulfing", "both") and cfg.get("cp_engulfing_strict", True)
                needs_rsi = source in ("three_line_strike", "both") and cfg.get("cp_three_line_strict", True)
                min_needed = max(risk_atr_period, 14 if needs_rsi else 3, 200 if needs_ma else 3) + 5
                needed_bars = min(1000, max(min_needed * 2, 220))
                st = b["state"]

                if resolution in SUB_MINUTE_RESOLUTIONS:
                    local = get_seconds_candles(st, SUB_MINUTE_RESOLUTIONS[resolution], needed_bars)
                    if local:
                        closed_ts, closed_o, closed_h, closed_l, closed_c = local
                    else:
                        closed_ts = None
                else:
                    data = await fetch_candles_binance_multi(symbol, resolution, count_back=needed_bars, market_type=cfg.get("binance_market_type", "spot"))
                    if data:
                        timestamps, opens, highs, lows, closes = data
                        closed_ts, closed_o, closed_h, closed_l, closed_c = timestamps[:-1], opens[:-1], highs[:-1], lows[:-1], closes[:-1]
                    else:
                        closed_ts = None

                now = time.time()
                due_heartbeat = now - last_heartbeat > 300

                if closed_ts and len(closed_c) > min_needed:
                    price = st["last_price"] if st["last_price"] is not None else closed_c[-1]
                    bull, bear = compute_cp_signals(closed_o, closed_h, closed_l, closed_c, cfg)
                    risk_atr_series = compute_atr(closed_h, closed_l, closed_c, risk_atr_period)

                    keep = min_needed + 5
                    st["cp_opens"] = closed_o[-keep:]
                    st["cp_highs"] = closed_h[-keep:]
                    st["cp_lows"] = closed_l[-keep:]
                    st["cp_closes"] = closed_c[-keep:]
                    st["cp_risk_atr_last"] = risk_atr_series[-1]
                    st["cp_last_signal"] = "long" if bull[-1] else ("short" if bear[-1] else st.get("cp_last_signal"))

                    if due_heartbeat:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] Candle-Patterns aktiv: Preis={closed_c[-1]}, Quelle={source}, "
                                  f"Risk-ATR={round(risk_atr_series[-1] or 0,4)}, Kerzen={len(closed_c)}, bot_active={cfg['bot_active']}")

                    if last_processed_ts is None:
                        new_indices = [len(closed_ts) - 1]
                    else:
                        try:
                            last_idx = closed_ts.index(last_processed_ts)
                            new_indices = list(range(last_idx + 1, len(closed_ts)))
                        except ValueError:
                            new_indices = [len(closed_ts) - 1]

                    for idx in new_indices:
                        if idx < 3:
                            continue
                        buy_i, sell_i = bull[idx], bear[idx]
                        price_i = price if idx == len(closed_ts) - 1 else closed_c[idx]
                        last_processed_ts = closed_ts[idx]
                        just_flipped = await check_cp_exit(symbol, buy_i, sell_i, price_i)
                        if not just_flipped:
                            await check_cp_entry(symbol, buy_i, sell_i, price_i, risk_atr_series[idx])

                    await check_cp_sl_tp(symbol, price)
                elif due_heartbeat:
                    last_heartbeat = now
                    if not closed_ts:
                        debug_log(f"⏳ [{symbol}] Candle-Patterns wartet: keine Kerzen erhalten (Auflösung {resolution})")
                    else:
                        debug_log(f"⏳ [{symbol}] Candle-Patterns wartet: zu wenig Kerzen ({len(closed_c)}/{min_needed + 1} nötig)")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Candle-Patterns-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

        await asyncio.sleep(5)


def compute_wpr(highs, lows, closes, period):
    """Williams %R (Pine's ta.wpr): -100 bis 0, misst Abstand des Schlusskurses vom
    Hoch-/Tief-Bereich der letzten `period` Kerzen."""
    n = len(closes)
    out = [-50.0] * n
    for i in range(n):
        start = max(0, i - period + 1)
        hh = max(highs[start:i + 1])
        ll = min(lows[start:i + 1])
        out[i] = -50.0 if hh == ll else -100 * (hh - closes[i]) / (hh - ll)
    return out


def compute_percentrank(values, period):
    """Perzentil-Rang (Pine's ta.percentrank): Anteil der letzten `period` Werte (VOR dem
    aktuellen), die kleiner sind als der aktuelle Wert, in Prozent (0-100)."""
    n = len(values)
    out = [50.0] * n
    for i in range(n):
        window = values[max(0, i - period):i]
        if not window:
            continue
        count = sum(1 for v in window if v < values[i])
        out[i] = 100.0 * count / len(window)
    return out


def _rolling_min_max(values, window):
    """Effizientes gleitendes Minimum/Maximum (monotone Deque, O(n) statt O(n*window)) - noetig
    weil MO7 ein 500-Kerzen-Fenster fuer MACD-/ROC-Normierung braucht und das bei 100.000
    Backtest-Kerzen sonst zu langsam waere."""
    n = len(values)
    mins = [None] * n
    maxs = [None] * n
    min_dq = deque()
    max_dq = deque()
    for i in range(n):
        v = values[i]
        while min_dq and values[min_dq[-1]] >= v:
            min_dq.pop()
        min_dq.append(i)
        while max_dq and values[max_dq[-1]] <= v:
            max_dq.pop()
        max_dq.append(i)
        while min_dq[0] <= i - window:
            min_dq.popleft()
        while max_dq[0] <= i - window:
            max_dq.popleft()
        mins[i] = values[min_dq[0]]
        maxs[i] = values[max_dq[0]]
    return mins, maxs


def compute_mo7_series_novolume(highs, lows, closes, cfg):
    """Wie compute_mo7_series, aber OHNE den MFI-Bestandteil (Mittel aus 6 statt 7 Komponenten) -
    fuer die Sekunden-Zeitrahmen im Scalp-Board, deren Kerzen-Puffer (siehe get_seconds_candles/
    binance_1s_poll_loop) kein Volumen mitfuehrt. Leicht ungenauer als das Original, aber fuer die
    rein manuelle Entscheidungshilfe im Scalp-Board ausreichend."""
    n = len(closes)
    rsi = compute_rsi(closes, cfg.get("mo7_rsi_len", 14))
    stoch_k, _ = compute_stochastic(highs, lows, closes, cfg.get("mo7_stoch_len", 14), 1, 1)
    wpr_raw = compute_wpr(highs, lows, closes, cfg.get("mo7_wpr_len", 14))
    wpr_val = [100 + w for w in wpr_raw]
    macd_line, _ = compute_macd_line_and_signal(closes, cfg.get("mo7_macd_fast", 12), cfg.get("mo7_macd_slow", 26), 9)
    macd_mins, macd_maxs = _rolling_min_max(macd_line, 500)
    macd_norm = [50.0 if macd_maxs[i] == macd_mins[i] else (macd_line[i] - macd_mins[i]) / (macd_maxs[i] - macd_mins[i]) * 100 for i in range(n)]
    roc = [0.0] * n
    for i in range(1, n):
        roc[i] = (closes[i] - closes[i - 1]) / closes[i - 1] * 100 if closes[i - 1] != 0 else 0.0
    roc_mins, roc_maxs = _rolling_min_max(roc, 500)
    roc_norm = [50.0 if roc_maxs[i] == roc_mins[i] else (roc[i] - roc_mins[i]) / (roc_maxs[i] - roc_mins[i]) * 100 for i in range(n)]
    pr = compute_percentrank(closes, 100)
    return [(rsi[i] + stoch_k[i] + wpr_val[i] + macd_norm[i] + roc_norm[i] + pr[i]) / 6 for i in range(n)]


def compute_mo7_series(highs, lows, closes, volumes, cfg):
    """MO7-Composite-Score (portiert aus dem 'MO7 Buy/Sell Signal'-Pine-Script): Mittelwert aus
    RSI, Stochastic %K, Williams %R (normiert 0-100), MFI, MACD-Linie (normiert 0-100 ueber ein
    500-Kerzen-Fenster), ROC (normiert 0-100 ueber ein 500-Kerzen-Fenster) und Percent-Rank(100).
    Ergebnis 0-100, wie im Original."""
    n = len(closes)
    rsi = compute_rsi(closes, cfg.get("mo7_rsi_len", 14))
    stoch_k, _ = compute_stochastic(highs, lows, closes, cfg.get("mo7_stoch_len", 14), 1, 1)
    wpr_raw = compute_wpr(highs, lows, closes, cfg.get("mo7_wpr_len", 14))
    wpr_val = [100 + w for w in wpr_raw]
    mfi = compute_mfi(highs, lows, closes, volumes, cfg.get("mo7_mfi_len", 14))
    macd_line, _ = compute_macd_line_and_signal(closes, cfg.get("mo7_macd_fast", 12), cfg.get("mo7_macd_slow", 26), 9)
    macd_mins, macd_maxs = _rolling_min_max(macd_line, 500)
    macd_norm = [50.0 if macd_maxs[i] == macd_mins[i] else (macd_line[i] - macd_mins[i]) / (macd_maxs[i] - macd_mins[i]) * 100 for i in range(n)]
    roc = [0.0] * n
    for i in range(1, n):
        roc[i] = (closes[i] - closes[i - 1]) / closes[i - 1] * 100 if closes[i - 1] != 0 else 0.0
    roc_mins, roc_maxs = _rolling_min_max(roc, 500)
    roc_norm = [50.0 if roc_maxs[i] == roc_mins[i] else (roc[i] - roc_mins[i]) / (roc_maxs[i] - roc_mins[i]) * 100 for i in range(n)]
    pr = compute_percentrank(closes, 100)

    return [(rsi[i] + stoch_k[i] + wpr_val[i] + mfi[i] + macd_norm[i] + roc_norm[i] + pr[i]) / 7 for i in range(n)]


def compute_mo7_signals(mo7, cfg):
    """Drei waehlbare Einstiegsmodi (mo7_entry_mode):
    - 'threshold_cross': Einstieg nur beim UEBERSCHREITEN der Schwelle (wie das Alert-Cooldown im
      Original), nicht bei jeder Kerze innerhalb der Zone -> BUY wenn MO7 gerade unter
      mo7_buy_threshold faellt, SELL wenn gerade ueber mo7_sell_threshold steigt.
    - 'five_candle_sum': eigene Idee - Summe der letzten 5 MO7-Werte < mo7_sum_low (stark
      ueberverkauft ueber mehrere Kerzen hinweg) -> Long, > mo7_sum_high (stark ueberkauft) -> Short.
    - 'trend_state': eigene Idee - MO7 als durchgehender Trend-Regime-Zustand statt Einzelsignal:
      ueber der Schwelle (Default 55) = Uptrend-Zustand -> Long, darunter = Downtrend-Zustand ->
      Short. Anders als bei den anderen Modi ist das NICHT nur ein Momentan-Ereignis, sondern gilt
      fuer JEDE Kerze in der jeweiligen Zone - der Bot bleibt dadurch quasi immer entsprechend dem
      aktuellen Regime positioniert (Flip erfolgt mit ~1 Kerze Verzoegerung: Exit auf dieser Kerze,
      Wiedereinstieg auf der naechsten, weil check_mo7_exit/check_mo7_entry im selben Tick nicht
      beides gleichzeitig ausloesen). Optionale Totzone (mo7_trend_deadband) um die Schwelle herum
      reduziert Hin-und-Her bei Werten, die genau um die Schwelle pendeln."""
    n = len(mo7)
    bull = [False] * n
    bear = [False] * n
    mode = cfg.get("mo7_entry_mode", "threshold_cross")
    if mode == "five_candle_sum":
        sum_low = cfg.get("mo7_sum_low", 100.0)
        sum_high = cfg.get("mo7_sum_high", 400.0)
        for i in range(4, n):
            window_sum = sum(mo7[i - 4:i + 1])
            bull[i] = window_sum < sum_low
            bear[i] = window_sum > sum_high
    elif mode == "trend_state":
        threshold = cfg.get("mo7_trend_threshold", 55.0)
        deadband = cfg.get("mo7_trend_deadband", 0.0)
        for i in range(n):
            bull[i] = mo7[i] > threshold + deadband
            bear[i] = mo7[i] < threshold - deadband
    else:
        buy_th = cfg.get("mo7_buy_threshold", 20.0)
        sell_th = cfg.get("mo7_sell_threshold", 85.0)
        for i in range(1, n):
            bull[i] = mo7[i] < buy_th and mo7[i - 1] >= buy_th
            bear[i] = mo7[i] > sell_th and mo7[i - 1] <= sell_th
    return bull, bear


def _mo7_reset_state(st):
    st["mo7_sl_price"] = None
    st["mo7_tp_price"] = None


async def check_mo7_sl_tp(symbol, price):
    """Nur fester SL/TP (kein ATR-Modus, wie explizit gewuenscht) - simpler als bei Candle
    Patterns/ELTE Smart: kein Breakeven."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if st["position"] is None or price is None:
        return
    pos = st["position"]
    sl_price = st.get("mo7_sl_price")
    tp_price = st.get("mo7_tp_price")
    if sl_price is None and tp_price is None:
        return
    hit_sl = sl_price is not None and ((pos == "long" and price <= sl_price) or (pos == "short" and price >= sl_price))
    hit_tp = tp_price is not None and ((pos == "long" and price >= tp_price) or (pos == "short" and price <= tp_price))
    if hit_sl:
        debug_log(f"🚪 [{symbol}] MO7 SL: {pos.upper()} @ {price} (Ziel war {round(sl_price, 4)})")
        await execute_exit(symbol, price, "SL")
        st["mo7_sl_cooldown_until"] = time.time() + cfg.get("mo7_sl_cooldown_seconds", 30)
        _mo7_reset_state(st)
    elif hit_tp:
        debug_log(f"🚪 [{symbol}] MO7 TP: {pos.upper()} @ {price} (Ziel war {round(tp_price, 4)})")
        await execute_exit(symbol, price, "TP")
        _mo7_reset_state(st)


async def check_mo7_entry(symbol, buy_signal, sell_signal, price):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or st["position"] is not None or price is None:
        return
    if time.time() < st.get("mo7_sl_cooldown_until", 0.0):
        return
    direction_mode = cfg.get("mo7_direction_mode", "both")
    if direction_mode == "long_only":
        sell_signal = False
    elif direction_mode == "short_only":
        buy_signal = False
    if not (buy_signal or sell_signal):
        return
    direction = "long" if buy_signal else "short"
    debug_log(f"📡 [{symbol}] MO7 Signal: {direction.upper()} @ {price}")
    await execute_entry(symbol, direction, price, is_add_on=False)
    if st["position"] is None:
        return
    _mo7_reset_state(st)
    size = st.get("total_coin_size") or 0
    if cfg.get("mo7_sl_enabled", True) and size > 0:
        dist_sl = cfg.get("mo7_sl_manual_usd", 5.0) / size
        st["mo7_sl_price"] = price - dist_sl if direction == "long" else price + dist_sl
    if cfg.get("mo7_tp_enabled", True) and size > 0:
        dist_tp = cfg.get("mo7_tp_manual_usd", 5.0) / size
        st["mo7_tp_price"] = price + dist_tp if direction == "long" else price - dist_tp


async def check_mo7_exit(symbol, buy_signal, sell_signal, price):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or st["position"] is None or price is None or not cfg.get("mo7_flip_exit_enabled", True):
        return False
    if st["position"] == "long" and sell_signal:
        debug_log(f"🚪 [{symbol}] MO7 Exit: LONG @ {price} (Gegen-Signal)")
        await execute_exit(symbol, price, "MO7-FLIP-EXIT")
        _mo7_reset_state(st)
        return True
    elif st["position"] == "short" and buy_signal:
        debug_log(f"🚪 [{symbol}] MO7 Exit: SHORT @ {price} (Gegen-Signal)")
        await execute_exit(symbol, price, "MO7-FLIP-EXIT")
        _mo7_reset_state(st)
        return True
    return False


async def mo7_poll_loop(symbol):
    """MO7-Composite-Oszillator (portiert aus dem 'MO7 Buy/Sell Signal'-Pine-Script) als eigene
    Strategie. NUR native Binance-Zeitrahmen (kein 2m/10s/15s/30s/45s/custom), weil MFI
    Handelsvolumen braucht und das nur fuer native Intervalle unkompliziert abrufbar ist."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "mo7_scalp" and cfg["bot_active"]:
                resolution = cfg.get("mo7_resolution", "5m")
                needed_bars = 520  # 500er-Normierungsfenster + Puffer
                st = b["state"]

                data = await fetch_candles_binance_vol(symbol, resolution, count_back=needed_bars)
                if data:
                    timestamps, opens, highs, lows, closes, volumes = data
                    closed_ts, closed_h, closed_l, closed_c, closed_v = timestamps[:-1], highs[:-1], lows[:-1], closes[:-1], volumes[:-1]
                else:
                    closed_ts = None

                now = time.time()
                due_heartbeat = now - last_heartbeat > 300

                if closed_ts and len(closed_c) > 20:
                    price = st["last_price"] if st["last_price"] is not None else closed_c[-1]
                    mo7 = compute_mo7_series(closed_h, closed_l, closed_c, closed_v, cfg)
                    bull, bear = compute_mo7_signals(mo7, cfg)

                    st["mo7_last_value"] = mo7[-1]

                    if due_heartbeat:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] MO7 aktiv: Preis={closed_c[-1]}, MO7={round(mo7[-1],2)}, "
                                  f"Modus={cfg.get('mo7_entry_mode')}, Kerzen={len(closed_c)}, bot_active={cfg['bot_active']}")

                    if last_processed_ts is None:
                        new_indices = [len(closed_ts) - 1]
                    else:
                        try:
                            last_idx = closed_ts.index(last_processed_ts)
                            new_indices = list(range(last_idx + 1, len(closed_ts)))
                        except ValueError:
                            new_indices = [len(closed_ts) - 1]

                    for idx in new_indices:
                        if idx < 4:
                            continue
                        buy_i, sell_i = bull[idx], bear[idx]
                        price_i = price if idx == len(closed_ts) - 1 else closed_c[idx]
                        last_processed_ts = closed_ts[idx]
                        just_flipped = await check_mo7_exit(symbol, buy_i, sell_i, price_i)
                        if not just_flipped:
                            await check_mo7_entry(symbol, buy_i, sell_i, price_i)

                    await check_mo7_sl_tp(symbol, price)
                elif due_heartbeat:
                    last_heartbeat = now
                    debug_log(f"⏳ [{symbol}] MO7 wartet: keine/zu wenig Kerzen erhalten (Auflösung {resolution})")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] MO7-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

        await asyncio.sleep(5)


def compute_ut_hull_flip_signals(buy, sell, hull_green, cfg):
    """Bestimmt WANN geflippt wird - waehlbar (utb_flip_trigger):
    - 'hull_color': sobald die Hull-MA-Farbe wechselt (UT-Bot-Signal wird nur fuer den
      allerersten Einstieg gebraucht, danach entscheidet nur noch Hull).
    - 'hull_and_signal': Hull-Farbe UND UT-Bot-Signal muessen am selben Balken beide da sein.
    - 'opposite_signal': nur das naechste Gegen-Signal von UT-Bot zaehlt, Hull spielt beim Flip
      keine Rolle mehr (nur beim allerersten Einstieg als Filter).
    - 'signal_only': NUR UT-Bot Buy/Sell im Wechsel, Hull spielt ueberhaupt keine Rolle - auch
      nicht beim Ersteinstieg (siehe check_uh_signal/_simulate_uh_trades)."""
    trigger = cfg.get("utb_flip_trigger", "hull_color")
    n = len(buy)
    long_flip = [False] * n
    short_flip = [False] * n
    if trigger in ("opposite_signal", "signal_only"):
        for i in range(n):
            long_flip[i] = buy[i]
            short_flip[i] = sell[i]
    elif trigger == "hull_and_signal":
        for i in range(n):
            if hull_green[i] is None:
                continue
            long_flip[i] = buy[i] and hull_green[i]
            short_flip[i] = sell[i] and not hull_green[i]
    else:  # "hull_color"
        for i in range(1, n):
            if hull_green[i] is None or hull_green[i - 1] is None:
                continue
            long_flip[i] = hull_green[i] and not hull_green[i - 1]
            short_flip[i] = (not hull_green[i]) and hull_green[i - 1]
    return long_flip, short_flip


def _uh_set_sl(st, cfg, direction, entry_price):
    """Setzt den festen SL-Preis (fester $-Betrag, wie bei den anderen Strategien) fuer die
    gerade eroeffnete Position - oder loescht ihn, falls SL deaktiviert/Positionsgroesse
    unbekannt ist."""
    if not cfg.get("utb_sl_enabled", False):
        st["utb_sl_price"] = None
        return
    size = st.get("total_coin_size") or 0
    if size <= 0:
        st["utb_sl_price"] = None
        return
    dist_sl = cfg.get("utb_sl_manual_usd", 5.0) / size
    st["utb_sl_price"] = entry_price - dist_sl if direction == "long" else entry_price + dist_sl


async def check_uh_sl(symbol, price):
    """Optionaler fester SL (fester $-Betrag, eingebbar). Anders als der Flip-Mechanismus geht
    die Position bei SL-Treffer erstmal GLATT (nicht direkt in die Gegenrichtung) und wartet -
    nach einem kurzen Cooldown - auf das naechste gueltige Ersteinstiegs-Signal, genau wie bei
    Candle Patterns/MO7. Das durchbricht das 'immer im Markt'-Prinzip bewusst nur im
    SL-Fall - ein Trailing-Loss soll die Position tatsaechlich beenden koennen."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if st["position"] is None or price is None:
        return
    sl_price = st.get("utb_sl_price")
    if sl_price is None:
        return
    pos = st["position"]
    hit_sl = (pos == "long" and price <= sl_price) or (pos == "short" and price >= sl_price)
    if hit_sl:
        debug_log(f"🚪 [{symbol}] UT-Bot+Hull SL: {pos.upper()} @ {price} (Ziel war {round(sl_price, 4)})")
        await execute_exit(symbol, price, "SL")
        st["utb_sl_price"] = None
        st["utb_sl_cooldown_until"] = time.time() + cfg.get("utb_sl_cooldown_seconds", 30)


async def check_uh_signal(symbol, buy_i, sell_i, long_flip_i, short_flip_i, hull_green_i, price, trend_pct=None):
    """Immer-im-Markt-System: beim allerersten Einstieg braucht es ein echtes UT-Bot-Signal PLUS
    passende Hull-Farbe - AUSSER im Modus 'signal_only', da zaehlt nur das UT-Bot-Signal, Hull
    wird komplett ignoriert. Danach entscheidet nur noch der gewaehlte Flip-Trigger
    (compute_ut_hull_flip_signals) ueber den naechsten Richtungswechsel. Bei Long-/Short-only
    wird bei einem Gegen-Flip nicht auf die andere Seite gedreht, sondern nur glattgestellt
    (echtes 'immer im Markt' ergibt bei einseitiger Richtung ja keinen Sinn). Optionaler fester
    SL (siehe check_uh_sl) unterbricht 'immer im Markt' nur im SL-Fall. Optionaler MTF-Trend%-
    Filter (wie bei Pieki Algo, siehe compute_pk_trend_percent/_pk_build_mtf_trend_pct) gilt fuer
    JEDEN Einstieg, auch beim Flip in die Gegenrichtung: Long nur wenn trend_pct ueber der
    Long-Schwelle, Short nur wenn darunter."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or price is None:
        return
    if time.time() < st.get("utb_sl_cooldown_until", 0.0):
        return
    direction_mode = cfg.get("utb_direction_mode", "both")
    signal_only = cfg.get("utb_flip_trigger", "hull_color") == "signal_only"
    mtf_enabled = cfg.get("utb_mtf_filter_enabled", False)
    long_thr = cfg.get("utb_mtf_long_threshold", 0.5)
    short_thr = cfg.get("utb_mtf_short_threshold", -0.5)
    long_ok = direction_mode != "short_only" and (not mtf_enabled or trend_pct is None or trend_pct > long_thr)
    short_ok = direction_mode != "long_only" and (not mtf_enabled or trend_pct is None or trend_pct < short_thr)
    pos = st["position"]

    if pos is None:
        if signal_only:
            if buy_i and long_ok:
                debug_log(f"📡 [{symbol}] UT-Bot+Hull Ersteinstieg (nur Signal): LONG @ {price}")
                await execute_entry(symbol, "long", price, is_add_on=False)
                if st["position"] is not None:
                    _uh_set_sl(st, cfg, "long", price)
            elif sell_i and short_ok:
                debug_log(f"📡 [{symbol}] UT-Bot+Hull Ersteinstieg (nur Signal): SHORT @ {price}")
                await execute_entry(symbol, "short", price, is_add_on=False)
                if st["position"] is not None:
                    _uh_set_sl(st, cfg, "short", price)
            return
        if hull_green_i is None:
            return
        if buy_i and hull_green_i and long_ok:
            debug_log(f"📡 [{symbol}] UT-Bot+Hull Ersteinstieg: LONG @ {price}")
            await execute_entry(symbol, "long", price, is_add_on=False)
            if st["position"] is not None:
                _uh_set_sl(st, cfg, "long", price)
        elif sell_i and not hull_green_i and short_ok:
            debug_log(f"📡 [{symbol}] UT-Bot+Hull Ersteinstieg: SHORT @ {price}")
            await execute_entry(symbol, "short", price, is_add_on=False)
            if st["position"] is not None:
                _uh_set_sl(st, cfg, "short", price)
        return

    if pos == "long" and short_flip_i:
        if direction_mode == "long_only":
            debug_log(f"🚪 [{symbol}] UT-Bot+Hull Exit (Richtung=Nur Long): LONG @ {price}")
            await execute_exit(symbol, price, "UTB-HULL-EXIT-DIR")
            st["utb_sl_price"] = None
        elif not short_ok:
            debug_log(f"🚪 [{symbol}] UT-Bot+Hull Exit (MTF-Filter blockiert Short, Trend%={round(trend_pct,2) if trend_pct is not None else '?'}): LONG @ {price}")
            await execute_exit(symbol, price, "UTB-HULL-EXIT-MTF")
            st["utb_sl_price"] = None
        else:
            debug_log(f"🔄 [{symbol}] UT-Bot+Hull Flip: LONG -> SHORT @ {price}")
            await execute_exit(symbol, price, "UTB-HULL-FLIP")
            await execute_entry(symbol, "short", price, is_add_on=False)
            if st["position"] is not None:
                _uh_set_sl(st, cfg, "short", price)
    elif pos == "short" and long_flip_i:
        if direction_mode == "short_only":
            debug_log(f"🚪 [{symbol}] UT-Bot+Hull Exit (Richtung=Nur Short): SHORT @ {price}")
            await execute_exit(symbol, price, "UTB-HULL-EXIT-DIR")
            st["utb_sl_price"] = None
        elif not long_ok:
            debug_log(f"🚪 [{symbol}] UT-Bot+Hull Exit (MTF-Filter blockiert Long, Trend%={round(trend_pct,2) if trend_pct is not None else '?'}): SHORT @ {price}")
            await execute_exit(symbol, price, "UTB-HULL-EXIT-MTF")
            st["utb_sl_price"] = None
        else:
            debug_log(f"🔄 [{symbol}] UT-Bot+Hull Flip: SHORT -> LONG @ {price}")
            await execute_exit(symbol, price, "UTB-HULL-FLIP")
            await execute_entry(symbol, "long", price, is_add_on=False)
            if st["position"] is not None:
                _uh_set_sl(st, cfg, "long", price)


async def utb_poll_loop(symbol):
    """UT Bot Alerts (ATR-Trailing-Stop) + Hull-MA-Filter, immer im Markt (Flip statt Exit),
    optional mit festem SL (fester $-Betrag, siehe check_uh_sl - unterbricht 'immer im Markt'
    nur im SL-Fall). Nutzt dieselbe Kerzenquelle/Aufloesungs-Logik wie Candle Patterns (inkl.
    Sekunden-Aufloesungen und eigene Minuten), da hier - anders als bei MO7 - kein
    Handelsvolumen gebraucht wird."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "ut_bot_hull" and cfg["bot_active"]:
                resolution = cfg.get("utb_resolution", "5m")
                hull_period = cfg.get("utb_hull_period", 31)
                atr_period = cfg.get("utb_atr_period", 1)
                min_needed = max(atr_period, hull_period + round(math.sqrt(hull_period)) + 2, 5) + 5
                needed_bars = min(1000, max(min_needed * 2, 220))
                st = b["state"]

                if resolution in SUB_MINUTE_RESOLUTIONS:
                    local = get_seconds_candles(st, SUB_MINUTE_RESOLUTIONS[resolution], needed_bars)
                    if local:
                        closed_ts, closed_o, closed_h, closed_l, closed_c = local
                    else:
                        closed_ts = None
                else:
                    data = await fetch_candles_binance_multi(symbol, resolution, count_back=needed_bars, market_type=cfg.get("binance_market_type", "spot"))
                    if data:
                        timestamps, opens, highs, lows, closes = data
                        closed_ts, closed_o, closed_h, closed_l, closed_c = timestamps[:-1], opens[:-1], highs[:-1], lows[:-1], closes[:-1]
                    else:
                        closed_ts = None

                now = time.time()
                due_heartbeat = now - last_heartbeat > 300

                if closed_ts and len(closed_c) > min_needed:
                    price = st["last_price"] if st["last_price"] is not None else closed_c[-1]
                    buy, sell, stop_line = compute_ut_bot(closed_o, closed_h, closed_l, closed_c, atr_period, cfg.get("utb_sensitivity", 1.0), cfg.get("utb_heikin_ashi", False))
                    hma = compute_hull_ma(closed_c, hull_period)
                    hull_green = [None] * len(closed_c)
                    for i in range(1, len(closed_c)):
                        if hma[i] is not None and hma[i - 1] is not None:
                            hull_green[i] = hma[i] > hma[i - 1]
                    long_flip, short_flip = compute_ut_hull_flip_signals(buy, sell, hull_green, cfg)

                    st["utb_last_hull_green"] = hull_green[-1]

                    mtf_fast = cfg.get("utb_mtf_fast_len", 5)
                    mtf_slow = cfg.get("utb_mtf_slow_len", 9)
                    mtf_atr = cfg.get("utb_mtf_atr_len", 14)
                    active_tfs = [cfg.get(f"utb_mtf_tf{i}", "off") for i in (1, 2, 3)]
                    active_tfs = [tf for tf in active_tfs if tf not in (None, "", "off")]
                    tf_values = []  # letzter Trend%-Wert je aktiver Zeiteinheit - am Ende gemittelt (wie bei Pieki Algo)
                    for tf in active_tfs:
                        if tf == resolution:
                            tf_h, tf_l, tf_c = closed_h, closed_l, closed_c
                        else:
                            mtf_needed = min(500, max(mtf_slow, mtf_atr, 5) * 3 + 20)
                            if tf in SUB_MINUTE_RESOLUTIONS:
                                mtf_local = get_seconds_candles(st, SUB_MINUTE_RESOLUTIONS[tf], mtf_needed)
                                tf_h = mtf_local[2] if mtf_local else None
                                tf_l = mtf_local[3] if mtf_local else None
                                tf_c = mtf_local[4] if mtf_local else None
                            else:
                                mtf_data = await fetch_candles_binance_multi(symbol, tf, count_back=mtf_needed, market_type=cfg.get("binance_market_type", "spot"))
                                if mtf_data:
                                    _, _, tf_h, tf_l, tf_c = mtf_data
                                    tf_h, tf_l, tf_c = tf_h[:-1], tf_l[:-1], tf_c[:-1]
                                else:
                                    tf_h = tf_l = tf_c = None
                        if tf_c and len(tf_c) > max(mtf_slow, mtf_atr):
                            tf_trend = compute_pk_trend_percent(tf_h, tf_l, tf_c, mtf_fast, mtf_slow, mtf_atr)
                            tf_values.append(tf_trend[-1])
                    if tf_values:
                        trend_now = sum(tf_values) / len(tf_values)
                    else:
                        trend_now = compute_pk_trend_percent(closed_h, closed_l, closed_c, mtf_fast, mtf_slow, mtf_atr)[-1]
                    st["utb_trend_pct_last"] = trend_now

                    if due_heartbeat:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] UT-Bot+Hull aktiv: Preis={closed_c[-1]}, Hull-grün={hull_green[-1]}, "
                                  f"buy_i={buy[-1]}, sell_i={sell[-1]}, "
                                  f"Trigger={cfg.get('utb_flip_trigger')}, Trend%={round(trend_now,2)}, Kerzen={len(closed_c)}, bot_active={cfg['bot_active']}")

                    if last_processed_ts is None:
                        new_indices = [len(closed_ts) - 1]
                    else:
                        try:
                            last_idx = closed_ts.index(last_processed_ts)
                            new_indices = list(range(last_idx + 1, len(closed_ts)))
                        except ValueError:
                            new_indices = [len(closed_ts) - 1]

                    for idx in new_indices:
                        if idx < 2:
                            continue
                        price_i = price if idx == len(closed_ts) - 1 else closed_c[idx]
                        last_processed_ts = closed_ts[idx]
                        await check_uh_signal(symbol, buy[idx], sell[idx], long_flip[idx], short_flip[idx], hull_green[idx], price_i, trend_now)

                    await check_uh_sl(symbol, price)
                elif due_heartbeat:
                    last_heartbeat = now
                    if not closed_ts:
                        debug_log(f"⏳ [{symbol}] UT-Bot+Hull wartet: keine Kerzen erhalten (Auflösung {resolution})")
                    else:
                        debug_log(f"⏳ [{symbol}] UT-Bot+Hull wartet: zu wenig Kerzen ({len(closed_c)}/{min_needed + 1} nötig)")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] UT-Bot+Hull-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

        await asyncio.sleep(5)


def compute_wavetrend(highs, lows, closes, chlen=9, avg=12, malen=3):
    """WaveTrend-Oszillator (Kernbaustein von 'Cipher B'): esa=EMA(hlc3, chlen), de=EMA(|hlc3-esa|,
    chlen), ci=(hlc3-esa)/(0.015*de), wt1=EMA(ci, avg), wt2=SMA(wt1, malen). Gibt (wt1, wt2)
    zurueck (beide ungefaehr im Bereich -100..100, Nulllinien-Cross = Momentum-Wechsel)."""
    n = len(closes)
    hlc3 = [(highs[i] + lows[i] + closes[i]) / 3 for i in range(n)]
    esa = _ema_series(hlc3, chlen)
    de = _ema_series([abs(hlc3[i] - esa[i]) for i in range(n)], chlen)
    ci = [(hlc3[i] - esa[i]) / (0.015 * de[i]) if de[i] else 0.0 for i in range(n)]
    wt1 = _ema_series(ci, avg)
    wt2 = _sma_series(wt1, malen)
    return wt1, wt2


def compute_wtc_signals(highs, lows, closes, cfg):
    """WaveTrend-Cross-Signal (die gruenen/roten Punkte aus 'Cipher B'): wt1 kreuzt wt2. Im
    Zonen-Modus (wtc_require_zone, Standard An) zaehlt der Cross nur, wenn er im
    ueberverkauften (bullisch) bzw. ueberkauften (bearisch) Bereich passiert - wie im
    Original-Script (buySignal/sellSignal). Ohne Zonen-Filter zaehlt jeder Cross."""
    chlen = cfg.get("wtc_channel_len", 9)
    avg = cfg.get("wtc_average_len", 12)
    malen = cfg.get("wtc_ma_len", 3)
    os_level = cfg.get("wtc_os_level", -53)
    ob_level = cfg.get("wtc_ob_level", 53)
    require_zone = cfg.get("wtc_require_zone", True)

    wt1, wt2 = compute_wavetrend(highs, lows, closes, chlen, avg, malen)
    n = len(closes)
    bull = [False] * n
    bear = [False] * n
    for i in range(1, n):
        diff_now = wt1[i] - wt2[i]
        diff_prev = wt1[i - 1] - wt2[i - 1]
        crossed = (diff_prev <= 0 and diff_now > 0) or (diff_prev >= 0 and diff_now < 0)
        if not crossed:
            continue
        cross_up = diff_now > 0
        if require_zone:
            bull[i] = cross_up and wt2[i] <= os_level
            bear[i] = (not cross_up) and wt2[i] >= ob_level
        else:
            bull[i] = cross_up
            bear[i] = not cross_up
    return bull, bear, wt1, wt2


def _wtc_reset_state(st):
    st["wtc_sl_price"] = None
    st["wtc_tp_price"] = None


def _wtc_set_sl_tp(st, cfg, direction, entry_price):
    size = st.get("total_coin_size") or 0
    if cfg.get("wtc_sl_enabled", True) and size > 0:
        dist_sl = cfg.get("wtc_sl_manual_usd", 5.0) / size
        st["wtc_sl_price"] = entry_price - dist_sl if direction == "long" else entry_price + dist_sl
    else:
        st["wtc_sl_price"] = None
    if cfg.get("wtc_tp_enabled", True) and size > 0:
        dist_tp = cfg.get("wtc_tp_manual_usd", 5.0) / size
        st["wtc_tp_price"] = entry_price + dist_tp if direction == "long" else entry_price - dist_tp
    else:
        st["wtc_tp_price"] = None


async def check_wtc_sl_tp(symbol, price):
    """Fester SL/TP (fester $-Betrag, kein ATR-Modus - wie gewuenscht). Bei SL geht die Position
    glatt + Cooldown, bei TP ebenso (auch im 'immer im Markt'-Modus - ein SL/TP-Treffer soll die
    Position wirklich beenden koennen, genau wie beim optionalen SL bei UT Bot + Hull)."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if st["position"] is None or price is None:
        return
    pos = st["position"]
    sl_price = st.get("wtc_sl_price")
    tp_price = st.get("wtc_tp_price")
    if sl_price is None and tp_price is None:
        return
    hit_sl = sl_price is not None and ((pos == "long" and price <= sl_price) or (pos == "short" and price >= sl_price))
    hit_tp = tp_price is not None and ((pos == "long" and price >= tp_price) or (pos == "short" and price <= tp_price))
    if hit_sl:
        debug_log(f"🚪 [{symbol}] WaveTrend-Cross SL: {pos.upper()} @ {price} (Ziel war {round(sl_price, 4)})")
        await execute_exit(symbol, price, "SL")
        st["wtc_sl_cooldown_until"] = time.time() + cfg.get("wtc_sl_cooldown_seconds", 30)
        _wtc_reset_state(st)
    elif hit_tp:
        debug_log(f"🚪 [{symbol}] WaveTrend-Cross TP: {pos.upper()} @ {price} (Ziel war {round(tp_price, 4)})")
        await execute_exit(symbol, price, "TP")
        _wtc_reset_state(st)


async def check_wtc_signal(symbol, buy_i, sell_i, price):
    """Zwei waehlbare Betriebsarten (wtc_always_in_market):
    - Aus (Standard): normaler Einstieg nur wenn flach, Ausstieg via festem SL/TP, optional
      zusaetzlich sofort bei Gegen-Signal (wtc_flip_exit_enabled) - danach wieder flach, wartet
      auf das naechste frische Signal (wie bei Candle Patterns).
    - An: immer im Markt (wie bei UT Bot + Hull) - Buy/Sell wechseln sich ab, dreht direkt bei
      Gegen-Signal, SL/TP unterbricht 'immer im Markt' nur im Treffer-Fall."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or price is None:
        return
    if time.time() < st.get("wtc_sl_cooldown_until", 0.0):
        return
    direction_mode = cfg.get("wtc_direction_mode", "both")
    always_in_market = cfg.get("wtc_always_in_market", False)
    if direction_mode == "long_only":
        sell_i = False
    elif direction_mode == "short_only":
        buy_i = False
    pos = st["position"]

    if pos is None:
        if not (buy_i or sell_i):
            return
        direction = "long" if buy_i else "short"
        debug_log(f"📡 [{symbol}] WaveTrend-Cross Signal: {direction.upper()} @ {price}")
        await execute_entry(symbol, direction, price, is_add_on=False)
        if st["position"] is not None:
            _wtc_reset_state(st)
            _wtc_set_sl_tp(st, cfg, direction, price)
        return

    if always_in_market:
        if pos == "long" and sell_i:
            debug_log(f"🔄 [{symbol}] WaveTrend-Cross Flip: LONG -> SHORT @ {price}")
            await execute_exit(symbol, price, "WTC-FLIP")
            await execute_entry(symbol, "short", price, is_add_on=False)
            if st["position"] is not None:
                _wtc_reset_state(st)
                _wtc_set_sl_tp(st, cfg, "short", price)
        elif pos == "short" and buy_i:
            debug_log(f"🔄 [{symbol}] WaveTrend-Cross Flip: SHORT -> LONG @ {price}")
            await execute_exit(symbol, price, "WTC-FLIP")
            await execute_entry(symbol, "long", price, is_add_on=False)
            if st["position"] is not None:
                _wtc_reset_state(st)
                _wtc_set_sl_tp(st, cfg, "long", price)
    else:
        if not cfg.get("wtc_flip_exit_enabled", True):
            return
        if (pos == "long" and sell_i) or (pos == "short" and buy_i):
            debug_log(f"🚪 [{symbol}] WaveTrend-Cross Exit: {pos.upper()} @ {price} (Gegen-Signal)")
            await execute_exit(symbol, price, "WTC-FLIP-EXIT")
            _wtc_reset_state(st)


async def wtc_poll_loop(symbol):
    """WaveTrend-Cross ('Cipher B'-Kernsignal): wt1/wt2-Cross, optional nur in Ueberkauft-/
    Ueberverkauft-Zone (wtc_require_zone), optional 'immer im Markt' mit direktem Flip
    (wtc_always_in_market) statt normalem Einstieg/Ausstieg. Fester SL/TP (fester $-Betrag)."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "wavetrend_cross" and cfg["bot_active"]:
                resolution = cfg.get("wtc_resolution", "5m")
                chlen = cfg.get("wtc_channel_len", 9)
                avg = cfg.get("wtc_average_len", 12)
                min_needed = max(chlen, avg, 20) + 10
                needed_bars = min(1000, max(min_needed * 2, 220))
                st = b["state"]

                if resolution in SUB_MINUTE_RESOLUTIONS:
                    local = get_seconds_candles(st, SUB_MINUTE_RESOLUTIONS[resolution], needed_bars)
                    if local:
                        closed_ts, closed_o, closed_h, closed_l, closed_c = local
                    else:
                        closed_ts = None
                else:
                    data = await fetch_candles_binance_multi(symbol, resolution, count_back=needed_bars, market_type=cfg.get("binance_market_type", "spot"))
                    if data:
                        timestamps, opens, highs, lows, closes = data
                        closed_ts, closed_o, closed_h, closed_l, closed_c = timestamps[:-1], opens[:-1], highs[:-1], lows[:-1], closes[:-1]
                    else:
                        closed_ts = None

                now = time.time()
                due_heartbeat = now - last_heartbeat > 300

                if closed_ts and len(closed_c) > min_needed:
                    price = st["last_price"] if st["last_price"] is not None else closed_c[-1]
                    bull, bear, wt1, wt2 = compute_wtc_signals(closed_h, closed_l, closed_c, cfg)

                    st["wtc_last_wt1"] = wt1[-1]
                    st["wtc_last_wt2"] = wt2[-1]

                    if due_heartbeat:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] WaveTrend-Cross aktiv: Preis={closed_c[-1]}, wt1={round(wt1[-1],2)}, wt2={round(wt2[-1],2)}, "
                                  f"Kerzen={len(closed_c)}, bot_active={cfg['bot_active']}")

                    if last_processed_ts is None:
                        new_indices = [len(closed_ts) - 1]
                    else:
                        try:
                            last_idx = closed_ts.index(last_processed_ts)
                            new_indices = list(range(last_idx + 1, len(closed_ts)))
                        except ValueError:
                            new_indices = [len(closed_ts) - 1]

                    for idx in new_indices:
                        if idx < 2:
                            continue
                        price_i = price if idx == len(closed_ts) - 1 else closed_c[idx]
                        last_processed_ts = closed_ts[idx]
                        await check_wtc_signal(symbol, bull[idx], bear[idx], price_i)

                    await check_wtc_sl_tp(symbol, price)
                elif due_heartbeat:
                    last_heartbeat = now
                    if not closed_ts:
                        debug_log(f"⏳ [{symbol}] WaveTrend-Cross wartet: keine Kerzen erhalten (Auflösung {resolution})")
                    else:
                        debug_log(f"⏳ [{symbol}] WaveTrend-Cross wartet: zu wenig Kerzen ({len(closed_c)}/{min_needed + 1} nötig)")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] WaveTrend-Cross-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

        await asyncio.sleep(5)


def _align_htf_series(base_ts, htf_ts, htf_vals):
    """Bildet eine hoehere-Zeiteinheit-Werteserie (htf_ts/htf_vals, z.B. Trend% auf 1h-Kerzen) auf
    die Zeitstempel einer feineren Serie (base_ts, z.B. 1m-Handels-Kerzen) ab - per Forward-Fill
    (letzter zum Zeitpunkt base_ts[i] bereits GESCHLOSSENER htf-Wert). Bewusst kein Blick in die
    Zukunft (kein Wert aus einer noch nicht geschlossenen hoeheren Kerze), sonst waere der Backtest
    zu optimistisch (Look-Ahead-Bias)."""
    n = len(base_ts)
    m = len(htf_ts)
    out = [0.0] * n
    j = 0
    last_val = 0.0
    for i in range(n):
        while j < m and htf_ts[j] <= base_ts[i]:
            last_val = htf_vals[j]
            j += 1
        out[i] = last_val
    return out


def compute_pk_trend_percent(highs, lows, closes, fast_len, slow_len, atr_len):
    """Vereinfachte, EIN-Zeitrahmen-Version des Trend%-Werts aus dem 'MTF EMA Spread'-Dashboard-
    Indikator: (EMA_fast - EMA_slow) / ATR * 100, auf ±100 begrenzt. Das Original-Pine-Script
    mittelt diesen Wert ueber 9 verschiedene Zeitrahmen (3 Bloecke a 3 TFs) - hier bewusst nur auf
    dem eigenen Handels-Zeitrahmen der Strategie berechnet, um nicht fuer jeden Tick zusaetzliche
    Anfragen auf 9 Aufloesungen zu brauchen. Dient nur als optionaler Richtungsfilter
    (pk_mtf_filter_enabled), nicht als eigenstaendiges Signal."""
    n = len(closes)
    fast_ema = _ema_series(closes, fast_len)
    slow_ema = _ema_series(closes, slow_len)
    atr = compute_atr(highs, lows, closes, atr_len)
    trend = [0.0] * n
    for i in range(n):
        a = atr[i]
        raw = ((fast_ema[i] - slow_ema[i]) / a * 100) if a else 0.0
        trend[i] = max(min(raw, 100), -100)
    return trend


def compute_pk_signals(highs, lows, closes, sensitivity, atr_period, sma_period):
    """Portiert aus 'Pieki Algo | Signals & Overlays' (Pine v5): Standard-SuperTrend (Faktor =
    Sensitivity*2, wie im Original 'supertrend(close, sigsensiviti*2, 11)') + SMA-Bestaetigung.
    bull = Kurs kreuzt SuperTrend von unten nach oben UND Kurs >= SMA. bear = umgekehrt. Nutzt
    denselben SuperTrend-Kernbaustein wie Diamond Algo (compute_diamond_supertrend), da beide
    Pine-Scripts exakt dieselbe SuperTrend-Formel verwenden."""
    n = len(closes)
    factor = sensitivity * 2
    st_line, st_dir = compute_diamond_supertrend(highs, lows, closes, factor, atr_period)
    sma = _sma_series(closes, sma_period)
    bull = [False] * n
    bear = [False] * n
    for i in range(1, n):
        crossover = closes[i - 1] <= st_line[i - 1] and closes[i] > st_line[i]
        crossunder = closes[i - 1] >= st_line[i - 1] and closes[i] < st_line[i]
        bull[i] = crossover and closes[i] >= sma[i]
        bear[i] = crossunder and closes[i] <= sma[i]
    return bull, bear, st_line, sma


def _pk_reset_state(st):
    st["pk_sl_price"] = None
    st["pk_tp_price"] = None
    st["pk_trail_active"] = False
    st["pk_trail_best_price"] = None


def _pk_set_sl_tp(st, cfg, direction, entry_price):
    """Fester SL/TP (fester $-Betrag) - wird NUR im Exit-Modus 'fixed_tp_sl' aufgerufen. Im
    Exit-Modus 'flip' bleiben pk_sl_price/pk_tp_price immer None, check_pk_sl_tp greift dann
    also nie ein."""
    size = st.get("total_coin_size") or 0
    if cfg.get("pk_sl_enabled", True) and size > 0:
        dist_sl = cfg.get("pk_sl_manual_usd", 5.0) / size
        st["pk_sl_price"] = entry_price - dist_sl if direction == "long" else entry_price + dist_sl
    else:
        st["pk_sl_price"] = None
    if cfg.get("pk_tp_enabled", True) and size > 0:
        dist_tp = cfg.get("pk_tp_manual_usd", 10.0) / size
        st["pk_tp_price"] = entry_price + dist_tp if direction == "long" else entry_price - dist_tp
    else:
        st["pk_tp_price"] = None
    st["pk_trail_active"] = False
    st["pk_trail_best_price"] = entry_price


def _pk_apply_trailing(st, cfg, direction, entry_price, price):
    """Trailing-Stop auf Prozent-Basis (relativ zum Einstiegspreis, NICHT zum aktuellen Preis -
    wie ueblich bei Trailing-Stops): sobald der Trade um pk_trailing_activation_pct % im Profit
    war, springt der SL einmalig auf Breakeven (Einstiegspreis) und wird danach im Abstand von
    pk_trailing_step_pct % zum bisher besten erreichten Preis nachgezogen. Verbessert den SL nur,
    verschlechtert ihn nie (weder beim Aktivieren noch beim Nachziehen)."""
    if not cfg.get("pk_trailing_enabled", False):
        return
    best = st.get("pk_trail_best_price")
    if best is None:
        best = entry_price
    best = max(best, price) if direction == "long" else min(best, price)
    st["pk_trail_best_price"] = best

    activation_pct = cfg.get("pk_trailing_activation_pct", 0.2)
    step_pct = cfg.get("pk_trailing_step_pct", 0.2)
    profit_pct = ((best - entry_price) / entry_price * 100) if direction == "long" else ((entry_price - best) / entry_price * 100)

    if not st.get("pk_trail_active") and profit_pct >= activation_pct:
        st["pk_trail_active"] = True
        current_sl = st.get("pk_sl_price")
        breakeven = entry_price
        if direction == "long":
            st["pk_sl_price"] = breakeven if current_sl is None else max(current_sl, breakeven)
        else:
            st["pk_sl_price"] = breakeven if current_sl is None else min(current_sl, breakeven)

    if st.get("pk_trail_active"):
        trail_sl = best * (1 - step_pct / 100) if direction == "long" else best * (1 + step_pct / 100)
        current_sl = st.get("pk_sl_price")
        if direction == "long":
            st["pk_sl_price"] = trail_sl if current_sl is None else max(current_sl, trail_sl)
        else:
            st["pk_sl_price"] = trail_sl if current_sl is None else min(current_sl, trail_sl)


async def check_pk_sl_tp(symbol, price):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if st["position"] is None or price is None:
        return
    pos = st["position"]
    entry_price = st.get("avg_entry_price")
    if entry_price is not None:
        _pk_apply_trailing(st, cfg, pos, entry_price, price)
    sl_price = st.get("pk_sl_price")
    tp_price = st.get("pk_tp_price")
    if sl_price is None and tp_price is None:
        return
    hit_sl = sl_price is not None and ((pos == "long" and price <= sl_price) or (pos == "short" and price >= sl_price))
    hit_tp = tp_price is not None and ((pos == "long" and price >= tp_price) or (pos == "short" and price <= tp_price))
    if hit_sl:
        reason = "TRAIL-SL" if st.get("pk_trail_active") else "SL"
        debug_log(f"🚪 [{symbol}] Pieki-Algo {reason}: {pos.upper()} @ {price} (Ziel war {round(sl_price, 4)})")
        await execute_exit(symbol, price, reason)
        st["pk_sl_cooldown_until"] = time.time() + cfg.get("pk_sl_cooldown_seconds", 30)
        _pk_reset_state(st)
    elif hit_tp:
        debug_log(f"🚪 [{symbol}] Pieki-Algo TP: {pos.upper()} @ {price} (Ziel war {round(tp_price, 4)})")
        await execute_exit(symbol, price, "TP")
        _pk_reset_state(st)


async def check_pk_signal(symbol, buy_i, sell_i, price, trend_pct):
    """Exit-Modus waehlbar (pk_exit_mode):
    - 'flip': immer im Markt, dreht direkt bei Gegen-Signal (wie UT Bot + Hull / WaveTrend-Cross
      'immer im Markt'). Bei Long-/Short-only wird bei Gegen-Signal nur glattgestellt.
    - 'fixed_tp_sl': normaler Ein-/Ausstieg, verlaesst die Position NUR ueber check_pk_sl_tp -
      ein Gegen-Signal waehrend einer offenen Position wird ignoriert (wartet auf SL/TP).
    Optionaler MTF-Trend%-Filter (pk_mtf_filter_enabled): Long nur wenn trend_pct > Long-Schwelle,
    Short nur wenn trend_pct < Short-Schwelle - gilt fuer JEDEN Einstieg, auch beim Flip in die
    Gegenrichtung."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or price is None:
        return
    if time.time() < st.get("pk_sl_cooldown_until", 0.0):
        return
    direction_mode = cfg.get("pk_direction_mode", "both")
    exit_mode = cfg.get("pk_exit_mode", "flip")
    mtf_enabled = cfg.get("pk_mtf_filter_enabled", False)
    long_thr = cfg.get("pk_mtf_long_threshold", 0.5)
    short_thr = cfg.get("pk_mtf_short_threshold", -0.5)
    long_ok = direction_mode != "short_only" and (not mtf_enabled or trend_pct is None or trend_pct > long_thr)
    short_ok = direction_mode != "long_only" and (not mtf_enabled or trend_pct is None or trend_pct < short_thr)
    pos = st["position"]

    if pos is None:
        if buy_i and long_ok:
            debug_log(f"📡 [{symbol}] Pieki-Algo Signal: LONG @ {price}" + (f" (Trend%={round(trend_pct,2)})" if trend_pct is not None else ""))
            await execute_entry(symbol, "long", price, is_add_on=False)
            if st["position"] is not None:
                _pk_reset_state(st)
                if exit_mode == "fixed_tp_sl":
                    _pk_set_sl_tp(st, cfg, "long", price)
        elif sell_i and short_ok:
            debug_log(f"📡 [{symbol}] Pieki-Algo Signal: SHORT @ {price}" + (f" (Trend%={round(trend_pct,2)})" if trend_pct is not None else ""))
            await execute_entry(symbol, "short", price, is_add_on=False)
            if st["position"] is not None:
                _pk_reset_state(st)
                if exit_mode == "fixed_tp_sl":
                    _pk_set_sl_tp(st, cfg, "short", price)
        return

    if exit_mode != "flip":
        return  # 'fixed_tp_sl': Gegen-Signal wird ignoriert, nur check_pk_sl_tp darf schliessen

    if pos == "long" and sell_i:
        if direction_mode == "long_only" or not short_ok:
            debug_log(f"🚪 [{symbol}] Pieki-Algo Exit: LONG @ {price}")
            await execute_exit(symbol, price, "PK-EXIT")
            _pk_reset_state(st)
        else:
            debug_log(f"🔄 [{symbol}] Pieki-Algo Flip: LONG -> SHORT @ {price}")
            await execute_exit(symbol, price, "PK-FLIP")
            await execute_entry(symbol, "short", price, is_add_on=False)
            if st["position"] is not None:
                _pk_reset_state(st)
    elif pos == "short" and buy_i:
        if direction_mode == "short_only" or not long_ok:
            debug_log(f"🚪 [{symbol}] Pieki-Algo Exit: SHORT @ {price}")
            await execute_exit(symbol, price, "PK-EXIT")
            _pk_reset_state(st)
        else:
            debug_log(f"🔄 [{symbol}] Pieki-Algo Flip: SHORT -> LONG @ {price}")
            await execute_exit(symbol, price, "PK-FLIP")
            await execute_entry(symbol, "long", price, is_add_on=False)
            if st["position"] is not None:
                _pk_reset_state(st)


async def pk_poll_loop(symbol):
    """Pieki Algo: SuperTrend+SMA9-Signal (siehe compute_pk_signals), Exit-Modus waehlbar
    (Flip/Fest-SL-TP), optionaler MTF-Trend%-Filter (siehe compute_pk_trend_percent)."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "pieki_algo" and cfg["bot_active"]:
                resolution = cfg.get("pk_resolution", "5m")
                atr_period = cfg.get("pk_atr_period", 11)
                sma_period = cfg.get("pk_sma_period", 13)
                mtf_slow_len = cfg.get("pk_mtf_slow_len", 9)
                mtf_atr_len = cfg.get("pk_mtf_atr_len", 14)
                min_needed = max(atr_period, sma_period, mtf_slow_len, mtf_atr_len, 5) + 5
                needed_bars = min(1000, max(min_needed * 2, 220))
                st = b["state"]

                if resolution in SUB_MINUTE_RESOLUTIONS:
                    local = get_seconds_candles(st, SUB_MINUTE_RESOLUTIONS[resolution], needed_bars)
                    if local:
                        closed_ts, closed_o, closed_h, closed_l, closed_c = local
                    else:
                        closed_ts = None
                else:
                    data = await fetch_candles_binance_multi(symbol, resolution, count_back=needed_bars, market_type=cfg.get("binance_market_type", "spot"))
                    if data:
                        timestamps, opens, highs, lows, closes = data
                        closed_ts, closed_o, closed_h, closed_l, closed_c = timestamps[:-1], opens[:-1], highs[:-1], lows[:-1], closes[:-1]
                    else:
                        closed_ts = None

                now = time.time()
                due_heartbeat = now - last_heartbeat > 300

                if closed_ts and len(closed_c) > min_needed:
                    price = st["last_price"] if st["last_price"] is not None else closed_c[-1]
                    bull, bear, st_line, sma = compute_pk_signals(closed_h, closed_l, closed_c, cfg.get("pk_sensitivity", 3.0), atr_period, sma_period)

                    mtf_fast = cfg.get("pk_mtf_fast_len", 5)
                    active_tfs = [cfg.get(f"pk_mtf_tf{i}", "off") for i in (1, 2, 3)]
                    active_tfs = [tf for tf in active_tfs if tf not in (None, "", "off")]
                    tf_values = []  # letzter Trend%-Wert je aktiver Zeiteinheit - am Ende gemittelt (wie "Block 1" im Original: avgB1 = Durchschnitt aus 3 TFs)
                    for tf in active_tfs:
                        if tf == resolution:
                            tf_h, tf_l, tf_c = closed_h, closed_l, closed_c
                        else:
                            mtf_needed = min(500, max(mtf_slow_len, mtf_atr_len, 5) * 3 + 20)
                            if tf in SUB_MINUTE_RESOLUTIONS:
                                mtf_local = get_seconds_candles(st, SUB_MINUTE_RESOLUTIONS[tf], mtf_needed)
                                tf_h = mtf_local[2] if mtf_local else None
                                tf_l = mtf_local[3] if mtf_local else None
                                tf_c = mtf_local[4] if mtf_local else None
                            else:
                                mtf_data = await fetch_candles_binance_multi(symbol, tf, count_back=mtf_needed, market_type=cfg.get("binance_market_type", "spot"))
                                if mtf_data:
                                    _, _, tf_h, tf_l, tf_c = mtf_data
                                    tf_h, tf_l, tf_c = tf_h[:-1], tf_l[:-1], tf_c[:-1]
                                else:
                                    tf_h = tf_l = tf_c = None
                        if tf_c and len(tf_c) > max(mtf_slow_len, mtf_atr_len):
                            tf_trend = compute_pk_trend_percent(tf_h, tf_l, tf_c, mtf_fast, mtf_slow_len, mtf_atr_len)
                            tf_values.append(tf_trend[-1])
                        # fehlende/zu kurze TF-Kerzen werden einfach ausgelassen statt das Signal zu blockieren

                    if tf_values:
                        trend_now = sum(tf_values) / len(tf_values)
                    else:
                        # keine aktive TF konfiguriert (oder alle grad nicht verfuegbar) - Fallback auf
                        # den eigenen Handels-Zeitrahmen, damit der Filter nie hart blockiert
                        trend_now = compute_pk_trend_percent(closed_h, closed_l, closed_c, mtf_fast, mtf_slow_len, mtf_atr_len)[-1]
                    trend_pct = [trend_now] * len(closed_c)  # konstant fuer diesen Durchlauf - siehe trend_pct_now unten

                    st["pk_trend_pct_last"] = trend_now

                    if due_heartbeat:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] Pieki-Algo aktiv: Preis={closed_c[-1]}, Trend%={round(trend_pct[-1],2)}, "
                                  f"Exit-Modus={cfg.get('pk_exit_mode')}, Kerzen={len(closed_c)}, bot_active={cfg['bot_active']}")

                    if last_processed_ts is None:
                        new_indices = [len(closed_ts) - 1]
                    else:
                        try:
                            last_idx = closed_ts.index(last_processed_ts)
                            new_indices = list(range(last_idx + 1, len(closed_ts)))
                        except ValueError:
                            new_indices = [len(closed_ts) - 1]

                    for idx in new_indices:
                        if idx < 2:
                            continue
                        price_i = price if idx == len(closed_ts) - 1 else closed_c[idx]
                        last_processed_ts = closed_ts[idx]
                        # trend_pct[idx] nur bei GLEICHER Aufloesung sicher indizierbar (siehe oben) -
                        # bei abweichender MTF-Zeiteinheit hat trend_pct eine andere Laenge/Taktung,
                        # deshalb im Live-Betrieb immer den JEWEILS AKTUELLSTEN Trend%-Wert anwenden.
                        trend_pct_now = trend_pct[idx] if len(trend_pct) == len(closed_c) else trend_pct[-1]
                        await check_pk_signal(symbol, bull[idx], bear[idx], price_i, trend_pct_now)

                    await check_pk_sl_tp(symbol, price)
                elif due_heartbeat:
                    last_heartbeat = now
                    if not closed_ts:
                        debug_log(f"⏳ [{symbol}] Pieki-Algo wartet: keine Kerzen erhalten (Auflösung {resolution})")
                    else:
                        debug_log(f"⏳ [{symbol}] Pieki-Algo wartet: zu wenig Kerzen ({len(closed_c)}/{min_needed + 1} nötig)")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Pieki-Algo-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

        await asyncio.sleep(5)


def compute_fractals(highs, lows, n):
    """Williams Fractals, portiert aus 'Williams Fractals' (Pine v6) - VEREINFACHT: das Original
    hat zusaetzliche Gleichstand-Sonderfaelle (upflagUpFrontier1-4/downflagUpFrontier1-4), die bei
    fast identischen Hoch-/Tiefwerten noch mehr Fraktale zulassen. Hier stattdessen die strengere
    Standard-Definition: Pivot muss ECHT hoeher/tiefer sein als alle n Kerzen links UND rechts,
    keine Gleichstaende erlaubt. Gibt (up_fractal, down_fractal) zurueck - beide als Bool-Listen,
    wobei up_fractal[i]/down_fractal[i] bedeutet: die Kerze bei Index (i-n) wurde bei Index i als
    Fraktal BESTAETIGT (man braucht ja die n Kerzen danach, um es zu wissen - kein Echtzeit-Signal,
    immer n Kerzen im Nachhinein)."""
    total = len(highs)
    up_fractal = [False] * total
    down_fractal = [False] * total
    for i in range(2 * n, total):
        pivot = i - n
        if highs[pivot] > max(highs[pivot - n:pivot]) and highs[pivot] > max(highs[pivot + 1:pivot + n + 1]):
            up_fractal[i] = True
        if lows[pivot] < min(lows[pivot - n:pivot]) and lows[pivot] < min(lows[pivot + 1:pivot + n + 1]):
            down_fractal[i] = True
    return up_fractal, down_fractal


def _fr_set_sl(st, cfg, direction, entry_price):
    """Setzt den festen SL-Preis (fester $-Betrag) fuer die gerade eroeffnete Position - siehe
    _uh_set_sl bei UT-Bot+Hull, identisches Muster."""
    if not cfg.get("fr_sl_enabled", False):
        st["fr_sl_price"] = None
        return
    size = st.get("total_coin_size") or 0
    if size <= 0:
        st["fr_sl_price"] = None
        return
    dist_sl = cfg.get("fr_sl_manual_usd", 5.0) / size
    st["fr_sl_price"] = entry_price - dist_sl if direction == "long" else entry_price + dist_sl


async def check_fr_sl(symbol, price):
    """Optionaler fester SL (fester $-Betrag) - durchbricht 'immer im Markt' NUR im SL-Fall,
    Position geht dann glatt (nicht Flip) und wartet nach Cooldown auf das naechste gueltige
    Ersteinstiegs-Signal. Siehe check_uh_sl bei UT-Bot+Hull, identisches Muster."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if st["position"] is None or price is None:
        return
    sl_price = st.get("fr_sl_price")
    if sl_price is None:
        return
    pos = st["position"]
    hit_sl = (pos == "long" and price <= sl_price) or (pos == "short" and price >= sl_price)
    if hit_sl:
        debug_log(f"🚪 [{symbol}] Fractals SL: {pos.upper()} @ {price} (Ziel war {round(sl_price, 4)})")
        await execute_exit(symbol, price, "SL")
        st["fr_sl_price"] = None
        st["fr_sl_cooldown_until"] = time.time() + cfg.get("fr_sl_cooldown_seconds", 30)


async def check_fr_signal(symbol, buy_i, sell_i, price, zscore=None):
    """Immer im Markt, reiner Buy/Sell-Wechsel: Tief-Fraktal (down_fractal) = Kauf-Signal,
    Hoch-Fraktal (up_fractal) = Verkauf-Signal - oder umgekehrt, wenn fr_invert_direction an ist
    (buy_i/sell_i kommen von fr_poll_loop bereits entsprechend vertauscht, siehe dort). Optionaler
    Z-Score-Filter (fr_zscore_filter_enabled, siehe compute_rolling_zscore): Long nur wenn
    zscore > 0, Short nur wenn zscore < 0 - gilt fuer jeden Einstieg, auch beim Flip. Optionaler
    fester SL (siehe check_fr_sl) unterbricht 'immer im Markt' nur im SL-Fall - dreht sonst beim
    jeweils naechsten (erlaubten) Gegen-Signal direkt. Bei Long-/Short-only bzw. vom Filter
    blockiertem Flip wird nur glattgestellt."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or price is None:
        return
    if time.time() < st.get("fr_sl_cooldown_until", 0.0):
        return
    direction_mode = cfg.get("fr_direction_mode", "both")
    zscore_enabled = cfg.get("fr_zscore_filter_enabled", False)
    long_ok = direction_mode != "short_only" and (not zscore_enabled or zscore is None or zscore > 0)
    short_ok = direction_mode != "long_only" and (not zscore_enabled or zscore is None or zscore < 0)
    pos = st["position"]

    if pos is None:
        if buy_i and long_ok:
            debug_log(f"📡 [{symbol}] Fractals Ersteinstieg: LONG @ {price}")
            await execute_entry(symbol, "long", price, is_add_on=False)
            if st["position"] is not None:
                _fr_set_sl(st, cfg, "long", price)
        elif sell_i and short_ok:
            debug_log(f"📡 [{symbol}] Fractals Ersteinstieg: SHORT @ {price}")
            await execute_entry(symbol, "short", price, is_add_on=False)
            if st["position"] is not None:
                _fr_set_sl(st, cfg, "short", price)
        return

    if pos == "long" and sell_i:
        if direction_mode == "long_only" or not short_ok:
            debug_log(f"🚪 [{symbol}] Fractals Exit: LONG @ {price}")
            await execute_exit(symbol, price, "FR-EXIT-DIR" if direction_mode == "long_only" else "FR-EXIT-ZSCORE")
            st["fr_sl_price"] = None
        else:
            debug_log(f"🔄 [{symbol}] Fractals Flip: LONG -> SHORT @ {price}")
            await execute_exit(symbol, price, "FR-FLIP")
            await execute_entry(symbol, "short", price, is_add_on=False)
            if st["position"] is not None:
                _fr_set_sl(st, cfg, "short", price)
    elif pos == "short" and buy_i:
        if direction_mode == "short_only" or not long_ok:
            debug_log(f"🚪 [{symbol}] Fractals Exit: SHORT @ {price}")
            await execute_exit(symbol, price, "FR-EXIT-DIR" if direction_mode == "short_only" else "FR-EXIT-ZSCORE")
            st["fr_sl_price"] = None
        else:
            debug_log(f"🔄 [{symbol}] Fractals Flip: SHORT -> LONG @ {price}")
            await execute_exit(symbol, price, "FR-FLIP")
            await execute_entry(symbol, "long", price, is_add_on=False)
            if st["position"] is not None:
                _fr_set_sl(st, cfg, "long", price)


async def fr_poll_loop(symbol):
    """Williams Fractals: Tief-Fraktal = Kauf, Hoch-Fraktal = Verkauf, immer im Markt, reiner
    Buy/Sell-Wechsel (siehe check_fr_signal/compute_fractals)."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "fractals_flip" and cfg["bot_active"]:
                resolution = cfg.get("fr_resolution", "5m")
                n = cfg.get("fr_periods", 2)
                min_needed = 2 * n + 5
                needed_bars = min(1000, max(min_needed * 2, 220))
                st = b["state"]

                if resolution in SUB_MINUTE_RESOLUTIONS:
                    local = get_seconds_candles(st, SUB_MINUTE_RESOLUTIONS[resolution], needed_bars)
                    if local:
                        closed_ts, closed_o, closed_h, closed_l, closed_c = local
                    else:
                        closed_ts = None
                else:
                    data = await fetch_candles_binance_multi(symbol, resolution, count_back=needed_bars, market_type=cfg.get("binance_market_type", "spot"))
                    if data:
                        timestamps, opens, highs, lows, closes = data
                        closed_ts, closed_o, closed_h, closed_l, closed_c = timestamps[:-1], opens[:-1], highs[:-1], lows[:-1], closes[:-1]
                    else:
                        closed_ts = None

                now = time.time()
                due_heartbeat = now - last_heartbeat > 300

                if closed_ts and len(closed_c) > min_needed:
                    price = st["last_price"] if st["last_price"] is not None else closed_c[-1]
                    up_fractal, down_fractal = compute_fractals(closed_h, closed_l, n)
                    buy_signal, sell_signal = (up_fractal, down_fractal) if cfg.get("fr_invert_direction", False) else (down_fractal, up_fractal)

                    zs_lookback = cfg.get("fr_zscore_lookback", 20)
                    zs_smooth = cfg.get("fr_zscore_smooth", 3)
                    zscore_resolution = cfg.get("fr_zscore_resolution", "same")
                    if zscore_resolution in (None, "", "same") or zscore_resolution == resolution:
                        zscore_series = compute_rolling_zscore(closed_c, zs_lookback, zs_smooth)
                    else:
                        zs_needed = min(500, max(zs_lookback, zs_smooth, 5) * 3 + 20)
                        if zscore_resolution in SUB_MINUTE_RESOLUTIONS:
                            zs_local = get_seconds_candles(st, SUB_MINUTE_RESOLUTIONS[zscore_resolution], zs_needed)
                            zs_c = zs_local[4] if zs_local else None
                        else:
                            zs_data = await fetch_candles_binance_multi(symbol, zscore_resolution, count_back=zs_needed, market_type=cfg.get("binance_market_type", "spot"))
                            zs_c = zs_data[4][:-1] if zs_data else None
                        if zs_c and len(zs_c) > max(zs_lookback, zs_smooth):
                            zscore_now = compute_rolling_zscore(zs_c, zs_lookback, zs_smooth)[-1]
                        else:
                            zscore_now = compute_rolling_zscore(closed_c, zs_lookback, zs_smooth)[-1]  # Fallback, falls (noch) keine Daten
                        zscore_series = [zscore_now] * len(closed_c)  # abweichende Zeiteinheit: nur der aktuellste Wert zaehlt live (siehe unten)

                    if due_heartbeat:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] Fractals aktiv: Preis={closed_c[-1]}, Kerzen={len(closed_c)}, bot_active={cfg['bot_active']}")

                    if last_processed_ts is None:
                        new_indices = [len(closed_ts) - 1]
                    else:
                        try:
                            last_idx = closed_ts.index(last_processed_ts)
                            new_indices = list(range(last_idx + 1, len(closed_ts)))
                        except ValueError:
                            new_indices = [len(closed_ts) - 1]

                    for idx in new_indices:
                        if idx < 2 * n:
                            continue
                        # Bewusst ANDERS als bei den anderen Strategien: hier IMMER der
                        # tatsaechliche Kerzenschlusskurs, nicht der aktuelle Live-Preis fuer die
                        # juengste Kerze - Fraktal-Bestaetigung soll exakt zum Kerzenschluss
                        # ausgefuehrt werden, nicht zum Preis im Moment der Abfrage (bis zu 5 Sek.
                        # nach dem eigentlichen Schluss).
                        price_i = closed_c[idx]
                        last_processed_ts = closed_ts[idx]
                        await check_fr_signal(symbol, buy_signal[idx], sell_signal[idx], price_i, zscore_series[idx])

                    await check_fr_sl(symbol, price)
                elif due_heartbeat:
                    last_heartbeat = now
                    if not closed_ts:
                        debug_log(f"⏳ [{symbol}] Fractals wartet: keine Kerzen erhalten (Auflösung {resolution})")
                    else:
                        debug_log(f"⏳ [{symbol}] Fractals wartet: zu wenig Kerzen ({len(closed_c)}/{min_needed + 1} nötig)")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Fractals-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

        await asyncio.sleep(5)


def compute_candle_dna(opens, highs, lows, closes, rejection_mult):
    """Eigene Entwicklung (kein Port): Konviktions-Score je Kerze von -100 (voll bearisch) bis
    +100 (voll bullisch). Basis: Koerper-Anteil an der Hoch-Tief-Spanne (100*body/range,
    vorzeichenbehaftet - Marubozu-artige Kerzen landen nah an +-100, Doji-artige nah an 0). Dazu
    ein Bonus/Abzug, wenn ein Docht auf der Gegenseite mindestens 'rejection_mult'-mal so lang
    wie der Koerper ist (Hammer = bullischer Bonus, Shooting Star = bearischer Abzug) - reine
    Preisaktion, kein nachlaufender Indikator."""
    n = len(closes)
    score = [0.0] * n
    for i in range(n):
        o, h, l, c = opens[i], highs[i], lows[i], closes[i]
        rng = h - l
        if rng <= 0:
            continue
        body = c - o
        base = 100 * body / rng
        upper_wick = h - max(o, c)
        lower_wick = min(o, c) - l
        body_abs = abs(body)
        bonus = 0.0
        if lower_wick > body_abs * rejection_mult and lower_wick > upper_wick:
            bonus += 30.0  # Hammer-artige Ablehnung nach unten -> zusaetzlich bullisch
        if upper_wick > body_abs * rejection_mult and upper_wick > lower_wick:
            bonus -= 30.0  # Shooting-Star-artige Ablehnung nach oben -> zusaetzlich bearisch
        score[i] = max(-100.0, min(100.0, base + bonus))
    return score


def compute_cd_signals(opens, highs, lows, closes, rejection_mult, threshold):
    """buy_i/sell_i = Score kreuzt die Schwelle (bzw. -Schwelle) von innen nach aussen -
    verhindert, dass er bei jeder Kerze ueber der Schwelle neu feuert, solange er dort bleibt."""
    score = compute_candle_dna(opens, highs, lows, closes, rejection_mult)
    n = len(score)
    buy_i = [False] * n
    sell_i = [False] * n
    for i in range(1, n):
        if score[i - 1] <= threshold and score[i] > threshold:
            buy_i[i] = True
        if score[i - 1] >= -threshold and score[i] < -threshold:
            sell_i[i] = True
    return buy_i, sell_i, score


def _cd_set_sl(st, cfg, direction, entry_price):
    """Setzt den festen SL-Preis (fester $-Betrag) - siehe _fr_set_sl/_uh_set_sl, identisches
    Muster."""
    if not cfg.get("cd_sl_enabled", False):
        st["cd_sl_price"] = None
        return
    size = st.get("total_coin_size") or 0
    if size <= 0:
        st["cd_sl_price"] = None
        return
    dist_sl = cfg.get("cd_sl_manual_usd", 5.0) / size
    st["cd_sl_price"] = entry_price - dist_sl if direction == "long" else entry_price + dist_sl


async def check_cd_sl(symbol, price):
    """Optionaler fester SL - siehe check_fr_sl/check_uh_sl, identisches Muster."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if st["position"] is None or price is None:
        return
    sl_price = st.get("cd_sl_price")
    if sl_price is None:
        return
    pos = st["position"]
    hit_sl = (pos == "long" and price <= sl_price) or (pos == "short" and price >= sl_price)
    if hit_sl:
        debug_log(f"🚪 [{symbol}] Kerzen-DNA SL: {pos.upper()} @ {price} (Ziel war {round(sl_price, 4)})")
        await execute_exit(symbol, price, "SL")
        st["cd_sl_price"] = None
        st["cd_sl_cooldown_until"] = time.time() + cfg.get("cd_sl_cooldown_seconds", 30)


async def check_cd_signal(symbol, buy_i, sell_i, price, zscore=None):
    """Immer im Markt, reiner Buy/Sell-Wechsel - siehe check_fr_signal, identisches Muster
    inkl. optionalem Z-Score-Filter (cd_zscore_filter_enabled) und optionalem festem SL
    (cd_sl_enabled, siehe check_cd_sl)."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or price is None:
        return
    if time.time() < st.get("cd_sl_cooldown_until", 0.0):
        return
    direction_mode = cfg.get("cd_direction_mode", "both")
    zscore_enabled = cfg.get("cd_zscore_filter_enabled", False)
    long_ok = direction_mode != "short_only" and (not zscore_enabled or zscore is None or zscore > 0)
    short_ok = direction_mode != "long_only" and (not zscore_enabled or zscore is None or zscore < 0)
    pos = st["position"]

    if pos is None:
        if buy_i and long_ok:
            debug_log(f"📡 [{symbol}] Kerzen-DNA Ersteinstieg: LONG @ {price}")
            await execute_entry(symbol, "long", price, is_add_on=False)
            if st["position"] is not None:
                _cd_set_sl(st, cfg, "long", price)
        elif sell_i and short_ok:
            debug_log(f"📡 [{symbol}] Kerzen-DNA Ersteinstieg: SHORT @ {price}")
            await execute_entry(symbol, "short", price, is_add_on=False)
            if st["position"] is not None:
                _cd_set_sl(st, cfg, "short", price)
        return

    if pos == "long" and sell_i:
        if direction_mode == "long_only" or not short_ok:
            debug_log(f"🚪 [{symbol}] Kerzen-DNA Exit: LONG @ {price}")
            await execute_exit(symbol, price, "CD-EXIT-DIR" if direction_mode == "long_only" else "CD-EXIT-ZSCORE")
            st["cd_sl_price"] = None
        else:
            debug_log(f"🔄 [{symbol}] Kerzen-DNA Flip: LONG -> SHORT @ {price}")
            await execute_exit(symbol, price, "CD-FLIP")
            await execute_entry(symbol, "short", price, is_add_on=False)
            if st["position"] is not None:
                _cd_set_sl(st, cfg, "short", price)
    elif pos == "short" and buy_i:
        if direction_mode == "short_only" or not long_ok:
            debug_log(f"🚪 [{symbol}] Kerzen-DNA Exit: SHORT @ {price}")
            await execute_exit(symbol, price, "CD-EXIT-DIR" if direction_mode == "short_only" else "CD-EXIT-ZSCORE")
            st["cd_sl_price"] = None
        else:
            debug_log(f"🔄 [{symbol}] Kerzen-DNA Flip: SHORT -> LONG @ {price}")
            await execute_exit(symbol, price, "CD-FLIP")
            await execute_entry(symbol, "long", price, is_add_on=False)
            if st["position"] is not None:
                _cd_set_sl(st, cfg, "long", price)


async def cd_poll_loop(symbol):
    """Kerzen-DNA: eigener Konviktions-Score aus Koerper+Docht je Kerze, immer im Markt, reiner
    Buy/Sell-Wechsel (siehe check_cd_signal/compute_cd_signals). Wie bei Fractals: Ausfuehrung
    IMMER zum tatsaechlichen Kerzenschlusskurs, nicht zum Live-Preis - passend zur reinen
    Preisaktions-Philosophie (die abgeschlossene Kerze selbst IST das Signal)."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "candle_dna" and cfg["bot_active"]:
                resolution = cfg.get("cd_resolution", "1m")
                threshold = cfg.get("cd_threshold", 50)
                rejection_mult = cfg.get("cd_rejection_mult", 1.5)
                min_needed = 5
                needed_bars = min(1000, max(min_needed * 2, 220))
                st = b["state"]

                if resolution in SUB_MINUTE_RESOLUTIONS:
                    local = get_seconds_candles(st, SUB_MINUTE_RESOLUTIONS[resolution], needed_bars)
                    if local:
                        closed_ts, closed_o, closed_h, closed_l, closed_c = local
                    else:
                        closed_ts = None
                else:
                    data = await fetch_candles_binance_multi(symbol, resolution, count_back=needed_bars, market_type=cfg.get("binance_market_type", "spot"))
                    if data:
                        timestamps, opens, highs, lows, closes = data
                        closed_ts, closed_o, closed_h, closed_l, closed_c = timestamps[:-1], opens[:-1], highs[:-1], lows[:-1], closes[:-1]
                    else:
                        closed_ts = None

                now = time.time()
                due_heartbeat = now - last_heartbeat > 300

                if closed_ts and len(closed_c) > min_needed:
                    price = st["last_price"] if st["last_price"] is not None else closed_c[-1]
                    buy_signal, sell_signal, score = compute_cd_signals(closed_o, closed_h, closed_l, closed_c, rejection_mult, threshold)
                    zs_lookback = cfg.get("cd_zscore_lookback", 20)
                    zs_smooth = cfg.get("cd_zscore_smooth", 3)
                    zscore_resolution = cfg.get("cd_zscore_resolution", "same")
                    if zscore_resolution in (None, "", "same") or zscore_resolution == resolution:
                        zscore_series = compute_rolling_zscore(closed_c, zs_lookback, zs_smooth)
                    else:
                        zs_needed = min(500, max(zs_lookback, zs_smooth, 5) * 3 + 20)
                        if zscore_resolution in SUB_MINUTE_RESOLUTIONS:
                            zs_local = get_seconds_candles(st, SUB_MINUTE_RESOLUTIONS[zscore_resolution], zs_needed)
                            zs_c = zs_local[4] if zs_local else None
                        else:
                            zs_data = await fetch_candles_binance_multi(symbol, zscore_resolution, count_back=zs_needed, market_type=cfg.get("binance_market_type", "spot"))
                            zs_c = zs_data[4][:-1] if zs_data else None
                        if zs_c and len(zs_c) > max(zs_lookback, zs_smooth):
                            zscore_now = compute_rolling_zscore(zs_c, zs_lookback, zs_smooth)[-1]
                        else:
                            zscore_now = compute_rolling_zscore(closed_c, zs_lookback, zs_smooth)[-1]
                        zscore_series = [zscore_now] * len(closed_c)

                    if due_heartbeat:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] Kerzen-DNA aktiv: Preis={closed_c[-1]}, Score={round(score[-1],1)}, Kerzen={len(closed_c)}, bot_active={cfg['bot_active']}")

                    if last_processed_ts is None:
                        new_indices = [len(closed_ts) - 1]
                    else:
                        try:
                            last_idx = closed_ts.index(last_processed_ts)
                            new_indices = list(range(last_idx + 1, len(closed_ts)))
                        except ValueError:
                            new_indices = [len(closed_ts) - 1]

                    for idx in new_indices:
                        if idx < 1:
                            continue
                        price_i = closed_c[idx]
                        last_processed_ts = closed_ts[idx]
                        await check_cd_signal(symbol, buy_signal[idx], sell_signal[idx], price_i, zscore_series[idx])

                    await check_cd_sl(symbol, price)
                elif due_heartbeat:
                    last_heartbeat = now
                    if not closed_ts:
                        debug_log(f"⏳ [{symbol}] Kerzen-DNA wartet: keine Kerzen erhalten (Auflösung {resolution})")
                    else:
                        debug_log(f"⏳ [{symbol}] Kerzen-DNA wartet: zu wenig Kerzen ({len(closed_c)}/{min_needed + 1} nötig)")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Kerzen-DNA-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

        await asyncio.sleep(5)


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
                    data = await fetch_candles_binance_multi(symbol, resolution, count_back=needed_bars, market_type=cfg.get("binance_market_type", "spot"))
                    closed_c = data[4][:-1] if data else None

                if closed_c and len(closed_c) > period:
                    rsi_series = compute_rsi(closed_c, period)
                    st["oms_rsi"] = round(rsi_series[-1], 2)
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] OMS-RSI-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        await asyncio.sleep(10)


SCALP_BOARD_TIMEFRAMES = [("10s", 10), ("30s", 30), ("45s", 45), ("60s", 60)]


async def scalp_board_poll_loop(symbol):
    """Manuelles Scalp-Board: RSI (kurze Periode), Stochastic, MACD und MO7 (ohne Volumen-Anteil,
    siehe compute_mo7_series_novolume) auf 10s/30s/45s/60s PARALLEL berechnet, plus CVD ueber
    dieselben vier Fenster (aus dem sowieso schon global gepflegten Trade-Puffer, siehe
    _cvd_ratio_over) und OBI (schnell/mittel/langsam, direkt aus dem Live-Orderbuch-State, siehe
    update_obi_windows). Bewusst UNABHAENGIG vom aktuellen entry_mode - das ist ein reines
    Beobachtungs-/Handwerkszeug fuer manuelles Scalping, egal welche automatische Strategie gerade
    laeuft. Laeuft nur, solange der Bot fuer den Coin aktiv ist (bot_active), da die
    10s/30s/45s-Kerzen aus demselben 1s-Puffer stammen, der aus Kostengruenden nur bei aktivem Bot
    gefuellt wird (siehe binance_1s_poll_loop)."""
    b = BOTS[symbol]
    while True:
        try:
            cfg = b["config"]
            st = b["state"]
            if not GLOBAL_SETTINGS.get("scalp_board_enabled", True) or not cfg["bot_active"]:
                # Globaler Schalter aus, ODER dieser Coin ist gerade gar nicht aktiv - keine
                # Berechnung, spart CPU/RAM (siehe Render-Ressourcenlimit). Board-Anzeige zeigt
                # dann einfach "sammelt noch Daten" im Dashboard, bis wieder aktiviert.
                await asyncio.sleep(5)
                continue
            board = {}
            for label, seconds in SCALP_BOARD_TIMEFRAMES:
                if seconds == 60:
                    # Laufende Kerze live mit dabei (wie TradingView - Kerze steigt, Wert
                    # bewegt sich mit, nicht erst bei Kerzenschluss)
                    data = await fetch_candles_binance_multi(symbol, "1m", count_back=120, market_type=cfg.get("binance_market_type", "spot"))
                    candles = (data[2], data[3], data[4]) if data else None
                else:
                    # 30s/45s brauchen den echten 1s-Puffer (binance_1s_poll_loop), der aus
                    # Kostengruenden weiterhin nur bei aktivem Bot gefuellt wird - liefert also
                    # erst Daten, sobald der Bot fuer den Coin mal aktiv war/ist. Die 60s-Spalte
                    # oben ist ein direkter, einzelner REST-Call und funktioniert unabhaengig
                    # davon sofort, auch bei pausiertem Bot.
                    local = get_seconds_candles(st, seconds, 120)
                    candles = (local[2], local[3], local[4]) if local else None

                if candles and len(candles[2]) > 20:
                    h, l, c = candles
                    rsi_series = compute_rsi(c, 8)
                    k, d = compute_stochastic(h, l, c, 5, 3, 3)
                    macd, macd_sig = compute_macd_line_and_signal(c, 5, 13, 3)
                    mo7_series = compute_mo7_series_novolume(h, l, c, cfg)
                    board[label] = {
                        "rsi": round(rsi_series[-1], 1),
                        "stoch_k": round(k[-1], 1), "stoch_d": round(d[-1], 1),
                        "macd_hist": round(macd[-1] - macd_sig[-1], 5),
                        "cvd": _cvd_ratio_over(st, seconds),
                        "mo7": round(mo7_series[-1], 1),
                    }
                else:
                    board[label] = None
            board["obi"] = {
                "fast": st.get("obi_fast"), "medium": st.get("obi_medium"), "slow": st.get("obi_slow"),
            }
            st["scalp_board"] = board
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Scalp-Board-Berechnung fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        await asyncio.sleep(5)


async def quad_stoch_poll_loop(symbol):
    """Reiner Anzeige-Verlauf (wie OBI-Verlauf) fuer 4 Stochastics unterschiedlicher Laenge
    (9/3, 14/3, 40/4, 60/10 - wie im 'Quad Stochastic Divergence'-Indikator, nur die Linien
    selbst, ohne die Pivot-/Divergenz-Erkennung). Zeitrahmen ueber quad_stoch_resolution frei
    waehlbar (30s/1m/5m), unabhaengig vom entry_mode UND vom bot_active-Status - reiner
    Beobachtungs-Chart, soll auch bei pausiertem Bot funktionieren (nur die 30s-Aufloesung
    braucht weiterhin den bot_active-gated 1s-Puffer, 1m/5m laufen per REST immer).
    NUTZT BEWUSST DIE LAUFENDE (noch unfertige) Kerze live mit - wie bei TradingView bewegt
    sich die Linie WAEHREND die Kerze laeuft mit dem Kurs mit (Kerze steigt -> Linie steigt),
    nicht nur einmal pro abgeschlossener Kerze. Kein Trading-Signal, daher unproblematisch,
    dass sich der letzte Punkt noch aendern kann, bevor die Kerze schliesst (kein Repainting-
    Risiko wie bei einer echten Strategie, die hier eine Entscheidung treffen wuerde)."""
    b = BOTS[symbol]
    while True:
        try:
            cfg = b["config"]
            st = b["state"]
            resolution = cfg.get("quad_stoch_resolution", "1m")
            needed_bars = 150
            if resolution == "30s":
                local = get_seconds_candles(st, 30, needed_bars)
                candles = (local[2], local[3], local[4]) if local else None
            else:
                data = await fetch_candles_binance_multi(symbol, resolution, count_back=needed_bars, market_type=cfg.get("binance_market_type", "spot"))
                candles = (data[2], data[3], data[4]) if data else None

            if candles and len(candles[2]) > 60:
                h, l, c = candles
                s1, _ = compute_stochastic(h, l, c, 9, 3, 1)
                s2, _ = compute_stochastic(h, l, c, 14, 3, 1)
                s3, _ = compute_stochastic(h, l, c, 40, 4, 1)
                s4, _ = compute_stochastic(h, l, c, 60, 10, 1)
                hist = st["quad_stoch_history"]
                hist.append({"ts": int(time.time() * 1000), "s1": round(s1[-1], 1), "s2": round(s2[-1], 1),
                             "s3": round(s3[-1], 1), "s4": round(s4[-1], 1)})
                if len(hist) > 300:
                    st["quad_stoch_history"] = hist[-300:]
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Quad-Stochastic-Berechnung fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
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

    if cfg["entry_mode"] == "diamond_algo":
        entry_trigger = cfg.get("da_entry_trigger", "candle_close")
        exit_trigger = cfg.get("da_exit_trigger", "candle_close")
        if entry_trigger == "tick" or exit_trigger == "tick":
            try:
                co, ch, cl, cc = st.get("da_opens"), st.get("da_highs"), st.get("da_lows"), st.get("da_closes")
                if co and ch and cl and cc and len(cc) >= 2:
                    live_o = co
                    live_h = ch[:-1] + [max(ch[-1], price)]
                    live_l = cl[:-1] + [min(cl[-1], price)]
                    live_c = cc[:-1] + [price]
                    if cfg.get("da_use_heikin_ashi", False):
                        _, sig_h, sig_l, sig_c = compute_heikin_ashi(live_o, live_h, live_l, live_c)
                    else:
                        sig_h, sig_l, sig_c = live_h, live_l, live_c
                    buy, sell, smart_buy, smart_sell = compute_diamond_signal(
                        sig_h, sig_l, sig_c, cfg["da_atr_period"], cfg["da_sensitivity"],
                        cfg["da_sma_period"], cfg["da_ema_trend_period"])
                    signal_mode = cfg.get("da_signal_mode", "all")
                    buy_now = smart_buy[-1] if signal_mode == "smart_only" else buy[-1]
                    sell_now = smart_sell[-1] if signal_mode == "smart_only" else sell[-1]
                    if cfg.get("da_invert_direction", False):
                        buy_now, sell_now = sell_now, buy_now
                    atr_risk_series = compute_atr(sig_h, sig_l, sig_c, cfg.get("da_risk_atr_period", 14))
                    if exit_trigger == "tick":
                        await check_da_exit(symbol, buy_now, sell_now, price)
                    if entry_trigger == "tick":
                        await check_da_entry(symbol, buy_now, sell_now, price, atr_risk_series[-1])
            except Exception as e:
                debug_log(f"⚠️ [{symbol}] Diamond Algo Live-Tick-Auswertung fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

        await check_da_sl_tp(symbol, price)
        return

    if cfg["entry_mode"] == "elte_smart":
        entry_trigger = cfg.get("es_entry_trigger", "candle_close")
        exit_trigger = cfg.get("es_exit_trigger", "candle_close")
        if entry_trigger == "tick" or exit_trigger == "tick":
            try:
                co, ch, cl, cc = st.get("es_opens"), st.get("es_highs"), st.get("es_lows"), st.get("es_closes")
                if co and ch and cl and cc and len(cc) >= 2:
                    live_o = co
                    live_h = ch[:-1] + [max(ch[-1], price)]
                    live_l = cl[:-1] + [min(cl[-1], price)]
                    live_c = cc[:-1] + [price]
                    if cfg.get("es_auto_sensitivity", True):
                        sensitivity = compute_es_auto_sensitivity(live_c, cfg.get("es_vol_period", 10), cfg.get("es_vol_ma_len", 55))
                    else:
                        sensitivity = cfg.get("es_sensitivity", 3.0)
                    st_line, _ = compute_elte_supertrend(live_o, live_h, live_l, live_c, sensitivity, cfg["es_atr_period"])
                    buy_now = live_c[-2] <= st_line[-2] and live_c[-1] > st_line[-1]
                    sell_now = live_c[-2] >= st_line[-2] and live_c[-1] < st_line[-1]
                    if cfg.get("es_invert_direction", False):
                        buy_now, sell_now = sell_now, buy_now
                    risk_atr_series = compute_atr(live_h, live_l, live_c, cfg.get("es_risk_atr_period", 14))
                    just_flipped = False
                    if exit_trigger == "tick":
                        just_flipped = await check_es_exit(symbol, buy_now, sell_now, price)
                    if entry_trigger == "tick":
                        if not just_flipped or cfg.get("es_reenter_on_flip", False):
                            await check_es_entry(symbol, buy_now, sell_now, price, risk_atr_series[-1], signal_low=live_l[-1], signal_high=live_h[-1])
            except Exception as e:
                debug_log(f"⚠️ [{symbol}] ELTE Smart Live-Tick-Auswertung fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

        await check_es_sl_tp(symbol, price)
        return

    if st["position"] is None:
        if not bot_active or cfg["entry_mode"] != "grid":
            return
        direction_mode = cfg.get("grid_direction_mode", "both")
        grid_step_abs = compute_step_abs(st["anchor_price"], cfg, "grid")
        if price <= st["anchor_price"] - grid_step_abs and direction_mode != "short_only":
            await execute_entry(symbol, "long", price, is_add_on=False)
        elif price >= st["anchor_price"] + grid_step_abs and direction_mode != "long_only":
            await execute_entry(symbol, "short", price, is_add_on=False)
        return

    if cfg["entry_mode"] != "grid":
        return

    # Fester SL (fester $-Betrag, optional) - schliesst die GESAMTE Grid-Position (ueber alle
    # Nachkaeufe hinweg) sofort, wenn der unrealisierte Verlust den eingegebenen Betrag
    # erreicht. Anders als TP/Nachkauf ist das ein reiner Notausstieg, kein Teil des normalen
    # Grid-Zyklus - er greift unabhaengig davon, ob noch weitere Nachkauf-Stufen frei waeren.
    if cfg.get("grid_sl_enabled", False) and st.get("avg_entry_price") is not None and st.get("total_coin_size"):
        avg_entry = st["avg_entry_price"]
        size = st["total_coin_size"]
        unrealized_pnl = (price - avg_entry) * size if st["position"] == "long" else (avg_entry - price) * size
        sl_usd = cfg.get("grid_sl_manual_usd", 20.0)
        if unrealized_pnl <= -sl_usd:
            debug_log(f"🚪 [{symbol}] Grid SL: {st['position'].upper()} @ {price} (unrealisierter Verlust {round(unrealized_pnl, 2)} $ erreicht -{sl_usd} $)")
            await execute_exit(symbol, price, "SL")
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
# (compute_halftrend, compute_fib_swing, build_fib_levels), damit Backtest und Live-Verhalten
# nicht auseinanderlaufen.
# WICHTIGE EINSCHRAENKUNG: SL/TP und Indikator-Exits werden pro Kerze am SCHLUSSKURS geprueft,
# nicht Tick-fuer-Tick wie live - ein kurzes Durchstechen von SL/TP innerhalb einer Kerze, das
# sich bis zum Kerzenschluss wieder erholt, wird also nicht erkannt. Fuer eine erste Einschaetzung
# der Strategie-Qualitaet reicht das aber aus.
# Lighter.xyz ist gebuehrenfrei - es werden daher keine Handelsgebuehren simuliert.

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


def _simulate_es_trades(candles, cfg, buy, sell, risk_atr, warmup):
    """Kern-Simulation fuer ELTE Smart. Wie _simulate_halftrend_trades, aber mit einem
    zusaetzlichen Schritt, den HalfTrend nicht hat: SL springt nach TP2 nochmal weiter auf den
    TP1-Preis (statt auf Break-Even stehen zu bleiben) - ab TP2 ist also immer schon ein
    Teilgewinn abgesichert."""
    ts, o, h, l, c = candles
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    risk_mult = cfg.get("es_risk_mult", 2.2)
    sl_enabled = cfg.get("es_sl_enabled", True)
    tp_enabled = cfg.get("es_tp_enabled", True)
    sl_mode = cfg.get("es_sl_mode", "atr")
    sl_manual_usd = cfg.get("es_sl_manual_usd", 5.0)
    tp_mode = cfg.get("es_tp_mode", "atr")
    tp_manual_usd = cfg.get("es_tp_manual_usd", 5.0)
    tp1_frac = cfg.get("es_tp1_close_pct", 50) / 100
    tp2_frac = cfg.get("es_tp2_close_pct", 50) / 100
    tp1_rr = cfg.get("es_tp1_rr", 1.0)
    tp2_rr = cfg.get("es_tp2_rr", 2.0)
    tp3_rr = cfg.get("es_tp3_rr", 3.0)
    sl_cooldown_ms = cfg.get("es_sl_cooldown_seconds", 30) * 1000
    invert = cfg.get("es_invert_direction", False)
    breakeven_pct_enabled = cfg.get("es_breakeven_pct_enabled", False)
    breakeven_trigger_pct = cfg.get("es_breakeven_trigger_pct", 0.1) / 100

    position = None
    trades = []
    sl_cooldown_until_ts = None

    for i in range(warmup, n):
        price = c[i]

        if position is not None:
            pdir, entry = position["dir"], position["entry"]

            # Prozent-Break-Even (siehe check_es_sl_tp fuer die ausfuehrliche Begruendung):
            # nutzt das GUENSTIGSTE Preis-Extrem innerhalb dieses Balkens (Hoch bei Long, Tief
            # bei Short), um zu pruefen ob die Schwelle innerhalb der Kerze erreicht wurde -
            # verbessert den SL nur, verschlechtert ihn nie, laeuft nur einmal pro Position.
            if breakeven_pct_enabled and not position.get("breakeven_pct_done"):
                best_price = h[i] if pdir == "long" else l[i]
                moved_pct = (best_price - entry) / entry if pdir == "long" else (entry - best_price) / entry
                if moved_pct >= breakeven_trigger_pct:
                    current_sl = position.get("sl_price")
                    if current_sl is None or (pdir == "long" and entry > current_sl) or (pdir == "short" and entry < current_sl):
                        position["sl_price"] = entry
                    position["breakeven_pct_done"] = True

            sl_price = position.get("sl_price")
            hit_sl = sl_price is not None and ((pdir == "long" and l[i] <= sl_price) or (pdir == "short" and h[i] >= sl_price))
            if hit_sl:
                if position["tp2_done"]:
                    reason = "TP1-LOCK"
                elif position["tp1_done"]:
                    reason = "BREAKEVEN"
                elif position.get("breakeven_pct_done") and abs(sl_price - entry) < 1e-9:
                    reason = "BREAKEVEN-PCT"
                else:
                    reason = "SL"
                _bt_close_trade(trades, pdir, entry, sl_price, position["size"], i, position["entry_i"], reason, ts=ts)
                position = None
                sl_cooldown_until_ts = ts[i] + sl_cooldown_ms
            elif not position["tp1_done"] and position.get("tp1_price") is not None:
                tp1_price = position["tp1_price"]
                if (pdir == "long" and h[i] >= tp1_price) or (pdir == "short" and l[i] <= tp1_price):
                    if position.get("tp_mode") == "manual":
                        _bt_close_trade(trades, pdir, entry, tp1_price, position["size"], i, position["entry_i"], "TP", ts=ts)
                        position = None
                    else:
                        close_size = position["size"] * tp1_frac
                        _bt_close_trade(trades, pdir, entry, tp1_price, close_size, i, position["entry_i"], "TP1", ts=ts)
                        position["size"] -= close_size
                        position["tp1_done"] = True
                        position["sl_price"] = entry  # Break-Even
            elif position["tp1_done"] and not position["tp2_done"] and position.get("tp2_price") is not None:
                tp2_price = position["tp2_price"]
                if (pdir == "long" and h[i] >= tp2_price) or (pdir == "short" and l[i] <= tp2_price):
                    close_size = position["size"] * tp2_frac
                    _bt_close_trade(trades, pdir, entry, tp2_price, close_size, i, position["entry_i"], "TP2", ts=ts)
                    position["size"] -= close_size
                    position["tp2_done"] = True
                    position["sl_price"] = position["tp1_price"]  # SL zieht weiter auf TP1
            elif position["tp1_done"] and position["tp2_done"] and position.get("tp3_price") is not None:
                tp3_price = position["tp3_price"]
                if (pdir == "long" and h[i] >= tp3_price) or (pdir == "short" and l[i] <= tp3_price):
                    _bt_close_trade(trades, pdir, entry, tp3_price, position["size"], i, position["entry_i"], "TP3", ts=ts)
                    position = None

        buy_signal, sell_signal = buy[i], sell[i]
        if invert:
            buy_signal, sell_signal = sell_signal, buy_signal

        just_flipped = False
        if position is not None:
            if (position["dir"] == "long" and sell_signal) or (position["dir"] == "short" and buy_signal):
                _bt_close_trade(trades, position["dir"], position["entry"], price, position["size"], i, position["entry_i"], "ES-FLIP-EXIT", ts=ts)
                position = None
                just_flipped = True

        in_sl_cooldown = sl_cooldown_until_ts is not None and ts[i] < sl_cooldown_until_ts
        allow_entry = not just_flipped or cfg.get("es_reenter_on_flip", False)
        if position is None and not in_sl_cooldown and allow_entry and (buy_signal or sell_signal):
            direction = "long" if buy_signal else "short"
            size = (margin * leverage) / price
            sl_price = tp1_price = tp2_price = tp3_price = None
            if sl_enabled or tp_enabled:
                atr_band = risk_atr[i] * risk_mult
                if sl_enabled and sl_mode == "manual" and size > 0:
                    dist_sl = sl_manual_usd / size
                    sl_price = price - dist_sl if direction == "long" else price + dist_sl
                    dist_for_tp = atr_band  # TP bleibt bei manuellem SL rein ATR-basiert, wie besprochen
                else:
                    # Original-Formel: atrStop = trigger ? low - atrBand : high + atrBand - SL
                    # geht vom Tief/Hoch der Signalkerze aus, nicht vom Schlusskurs. TP1/TP2/TP3
                    # sind Vielfache des TATSAECHLICHEN Einstieg-zu-SL-Abstands.
                    sl_price = (l[i] - atr_band) if direction == "long" else (h[i] + atr_band)
                    dist_for_tp = abs(price - sl_price)
                if not sl_enabled:
                    sl_price = None
                if tp_enabled:
                    if tp_mode == "manual" and size > 0:
                        dist_tp = tp_manual_usd / size
                        tp1_price = price + dist_tp if direction == "long" else price - dist_tp
                        tp2_price = tp3_price = None  # nur EIN Ziel, wie beim festen SL
                    elif direction == "long":
                        tp1_price, tp2_price, tp3_price = price + dist_for_tp * tp1_rr, price + dist_for_tp * tp2_rr, price + dist_for_tp * tp3_rr
                    else:
                        tp1_price, tp2_price, tp3_price = price - dist_for_tp * tp1_rr, price - dist_for_tp * tp2_rr, price - dist_for_tp * tp3_rr
            position = {"dir": direction, "entry": price, "size": size, "entry_i": i,
                        "sl_price": sl_price, "tp1_price": tp1_price, "tp2_price": tp2_price, "tp3_price": tp3_price,
                        "tp_mode": tp_mode,
                        "tp1_done": False, "tp2_done": False}

    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)

    return trades


def backtest_elte_smart(candles, cfg):
    o, h, l, c = candles[1], candles[2], candles[3], candles[4]
    atr_period = cfg["es_atr_period"]
    risk_atr_period = cfg.get("es_risk_atr_period", 14)
    vol_period = cfg.get("es_vol_period", 10)
    vol_ma_len = cfg.get("es_vol_ma_len", 55)
    if cfg.get("es_auto_sensitivity", True):
        sensitivity = compute_es_auto_sensitivity(c, vol_period, vol_ma_len)
    else:
        sensitivity = cfg.get("es_sensitivity", 3.0)
    st_line, _ = compute_elte_supertrend(o, h, l, c, sensitivity, atr_period)
    n = len(c)
    buy = [False] * n
    sell = [False] * n
    for i in range(1, n):
        buy[i] = c[i - 1] <= st_line[i - 1] and c[i] > st_line[i]
        sell[i] = c[i - 1] >= st_line[i - 1] and c[i] < st_line[i]
    risk_atr = compute_atr(h, l, c, risk_atr_period)
    warmup = max(atr_period, risk_atr_period, vol_ma_len + vol_period) + 5
    return _simulate_es_trades(candles, cfg, buy, sell, risk_atr, warmup)


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


def _simulate_cp_trades(candles, cfg, bull, bear, risk_atr, warmup):
    """Kern-Simulation fuer Candle-Patterns. Wie _simulate_es_trades, aber nur EIN SL/EIN TP
    (keine TP1/TP2/TP3-Stufen - passt besser zu einem einzelnen, seltenen Umkehr-Signal statt
    einem durchlaufenden Trend-System), dafuer mit ATR-Breakeven statt Prozent-Breakeven und
    optionalem Flip-Exit."""
    ts, o, h, l, c = candles
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    risk_mult = cfg.get("cp_risk_mult", 1.5)
    tp_rr = cfg.get("cp_tp_rr", 1.0)
    sl_enabled = cfg.get("cp_sl_enabled", True)
    tp_enabled = cfg.get("cp_tp_enabled", True)
    sl_mode = cfg.get("cp_sl_mode", "atr")
    sl_manual_usd = cfg.get("cp_sl_manual_usd", 5.0)
    tp_mode = cfg.get("cp_tp_mode", "atr")
    tp_manual_usd = cfg.get("cp_tp_manual_usd", 5.0)
    sl_cooldown_ms = cfg.get("cp_sl_cooldown_seconds", 30) * 1000
    direction_mode = cfg.get("cp_direction_mode", "both")
    flip_exit_enabled = cfg.get("cp_flip_exit_enabled", True)
    breakeven_enabled = cfg.get("cp_breakeven_enabled", True)
    breakeven_trigger_mult = cfg.get("cp_breakeven_trigger_mult", 0.5)

    position = None
    trades = []
    sl_cooldown_until_ts = None

    for i in range(warmup, n):
        price = c[i]

        if position is not None:
            pdir, entry = position["dir"], position["entry"]

            if breakeven_enabled and not position.get("breakeven_done"):
                atr_now = risk_atr[i] or 0
                trigger_dist = atr_now * breakeven_trigger_mult
                best_price = h[i] if pdir == "long" else l[i]
                moved = (best_price - entry) if pdir == "long" else (entry - best_price)
                if trigger_dist > 0 and moved >= trigger_dist:
                    current_sl = position.get("sl_price")
                    if current_sl is None or (pdir == "long" and entry > current_sl) or (pdir == "short" and entry < current_sl):
                        position["sl_price"] = entry
                    position["breakeven_done"] = True

            sl_price = position.get("sl_price")
            hit_sl = sl_price is not None and ((pdir == "long" and l[i] <= sl_price) or (pdir == "short" and h[i] >= sl_price))
            if hit_sl:
                reason = "BREAKEVEN" if position.get("breakeven_done") and abs(sl_price - entry) < 1e-9 else "SL"
                _bt_close_trade(trades, pdir, entry, sl_price, position["size"], i, position["entry_i"], reason, ts=ts)
                position = None
                sl_cooldown_until_ts = ts[i] + sl_cooldown_ms
            else:
                tp_price = position.get("tp_price")
                hit_tp = tp_price is not None and ((pdir == "long" and h[i] >= tp_price) or (pdir == "short" and l[i] <= tp_price))
                if hit_tp:
                    _bt_close_trade(trades, pdir, entry, tp_price, position["size"], i, position["entry_i"], "TP", ts=ts)
                    position = None

        buy_signal, sell_signal = bull[i], bear[i]
        if direction_mode == "long_only":
            sell_signal = False
        elif direction_mode == "short_only":
            buy_signal = False

        just_flipped = False
        if position is not None and flip_exit_enabled:
            if (position["dir"] == "long" and sell_signal) or (position["dir"] == "short" and buy_signal):
                _bt_close_trade(trades, position["dir"], position["entry"], price, position["size"], i, position["entry_i"], "CP-FLIP-EXIT", ts=ts)
                position = None
                just_flipped = True

        in_sl_cooldown = sl_cooldown_until_ts is not None and ts[i] < sl_cooldown_until_ts
        if position is None and not in_sl_cooldown and not just_flipped and (buy_signal or sell_signal):
            direction = "long" if buy_signal else "short"
            size = (margin * leverage) / price
            sl_price = tp_price = None
            if sl_enabled or tp_enabled:
                atr_band = (risk_atr[i] or 0) * risk_mult
                dist_for_tp = atr_band
                if sl_enabled:
                    if sl_mode == "manual" and size > 0:
                        dist_sl = sl_manual_usd / size
                        dist_for_tp = dist_sl
                    else:
                        dist_sl = atr_band
                    sl_price = price - dist_sl if direction == "long" else price + dist_sl
                if tp_enabled:
                    if tp_mode == "manual" and size > 0:
                        dist_tp = tp_manual_usd / size
                    else:
                        dist_tp = dist_for_tp * tp_rr
                    tp_price = price + dist_tp if direction == "long" else price - dist_tp
            position = {"dir": direction, "entry": price, "size": size, "entry_i": i,
                        "sl_price": sl_price, "tp_price": tp_price, "breakeven_done": False}

    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)

    return trades


def backtest_candle_patterns(candles, cfg):
    o, h, l, c = candles[1], candles[2], candles[3], candles[4]
    bull, bear = compute_cp_signals(o, h, l, c, cfg)
    risk_atr_period = cfg.get("cp_risk_atr_period", 14)
    source = cfg.get("cp_signal_source", "three_line_strike")
    needs_ma = source in ("engulfing", "both") and cfg.get("cp_engulfing_strict", True)
    needs_rsi = source in ("three_line_strike", "both") and cfg.get("cp_three_line_strict", True)
    risk_atr = compute_atr(h, l, c, risk_atr_period)
    warmup = max(risk_atr_period, 14 if needs_rsi else 3, 200 if needs_ma else 3) + 5
    return _simulate_cp_trades(candles, cfg, bull, bear, risk_atr, warmup)


def _simulate_diamond_trades(candles, cfg, buy, sell, smart_buy, smart_sell, atr_risk, warmup):
    """Kern-Simulation fuer Diamond Algo, getrennt von der Signal-Berechnung (compute_diamond_signal)
    damit der Parameter-Sweep buy/sell/atr_risk nur EINMAL pro ATR-Periode x Sensitivity-Kombination
    berechnen muss. SL/TP-Abstand wird bei jedem Einstieg neu aus dem dann aktuellen ATR(risk_atr_period)
    berechnet (wie im Original: atrBand = ta.atr(atrLen) * atrRisk), TP = SL-Abstand * R:R-Multiplikator."""
    ts, o, h, l, c = candles
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    signal_mode = cfg.get("da_signal_mode", "all")
    invert = cfg.get("da_invert_direction", False)
    sl_enabled = cfg.get("da_sl_enabled", True)
    tp_enabled = cfg.get("da_tp_enabled", True)
    risk_mult = cfg.get("da_risk_mult", 1.0)
    tp_rr = cfg.get("da_tp_rr", 1.0)
    sl_cooldown_ms = cfg.get("da_sl_cooldown_seconds", 30) * 1000

    position = None
    trades = []
    sl_cooldown_until_ts = None

    for i in range(warmup, n):
        price = c[i]

        if position is not None:
            pdir, entry, size = position["dir"], position["entry"], position["size"]
            sl_price, tp_price = position.get("sl_price"), position.get("tp_price")
            hit_sl = sl_price is not None and ((pdir == "long" and l[i] <= sl_price) or (pdir == "short" and h[i] >= sl_price))
            hit_tp = tp_price is not None and ((pdir == "long" and h[i] >= tp_price) or (pdir == "short" and l[i] <= tp_price))
            if hit_sl:
                _bt_close_trade(trades, pdir, entry, sl_price, size, i, position["entry_i"], "SL", ts=ts)
                position = None
                sl_cooldown_until_ts = ts[i] + sl_cooldown_ms
            elif hit_tp:
                _bt_close_trade(trades, pdir, entry, tp_price, size, i, position["entry_i"], "TP", ts=ts)
                position = None

        buy_now = smart_buy[i] if signal_mode == "smart_only" else buy[i]
        sell_now = smart_sell[i] if signal_mode == "smart_only" else sell[i]
        if invert:
            buy_now, sell_now = sell_now, buy_now

        if position is not None:
            if (position["dir"] == "long" and sell_now) or (position["dir"] == "short" and buy_now):
                _bt_close_trade(trades, position["dir"], position["entry"], price, position["size"], i, position["entry_i"], "DA-FLIP-EXIT", ts=ts)
                position = None

        in_sl_cooldown = sl_enabled and sl_cooldown_until_ts is not None and ts[i] < sl_cooldown_until_ts
        if position is None and not in_sl_cooldown and (buy_now or sell_now):
            direction = "long" if buy_now else "short"
            size = (margin * leverage) / price
            dist_sl = atr_risk[i] * risk_mult if (sl_enabled or tp_enabled) else None
            sl_price = (price - dist_sl if direction == "long" else price + dist_sl) if (sl_enabled and dist_sl is not None) else None
            tp_price = None
            if tp_enabled and dist_sl is not None:
                dist_tp = dist_sl * tp_rr
                tp_price = price + dist_tp if direction == "long" else price - dist_tp
            position = {"dir": direction, "entry": price, "size": size, "entry_i": i, "sl_price": sl_price, "tp_price": tp_price}

    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)

    return trades


def backtest_diamond_algo(candles, cfg):
    o, h, l, c = candles[1], candles[2], candles[3], candles[4]
    atr_period = cfg["da_atr_period"]
    risk_atr_period = cfg.get("da_risk_atr_period", 14)
    sma_period = cfg["da_sma_period"]
    ema_trend_period = cfg["da_ema_trend_period"]
    if cfg.get("da_use_heikin_ashi", False):
        # Signal UND Risiko-ATR rechnen auf Heikin-Ashi-Kerzen, SL/TP-Ausloesung im Backtest
        # bleibt trotzdem an den ECHTEN Kerzen (candles) haengen, da im Live-Handel auch der
        # echte Marktpreis ausgeloest wird, nicht der geglaettete HA-Wert.
        _, sig_h, sig_l, sig_c = compute_heikin_ashi(o, h, l, c)
    else:
        sig_h, sig_l, sig_c = h, l, c
    buy, sell, smart_buy, smart_sell = compute_diamond_signal(sig_h, sig_l, sig_c, atr_period, cfg["da_sensitivity"], sma_period, ema_trend_period)
    atr_risk = compute_atr(sig_h, sig_l, sig_c, risk_atr_period)
    warmup = max(atr_period, risk_atr_period, sma_period, ema_trend_period) + 5
    return _simulate_diamond_trades(candles, cfg, buy, sell, smart_buy, smart_sell, atr_risk, warmup)


def _bt_close_trade(trades, direction, entry, exit_price, size, i, entry_i, reason, ts=None):
    pnl = (exit_price - entry) * size if direction == "long" else (entry - exit_price) * size
    trade = {"dir": direction, "entry": entry, "exit": exit_price, "reason": reason,
             "pnl": pnl, "bars_held": i - entry_i}
    if ts is not None:
        trade["entry_ts"] = ts[entry_i]
        trade["exit_ts"] = ts[i]
    trades.append(trade)



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


def summarize_backtest_trades(trades, exclude_top_n=1):
    """WICHTIG: 'trades' kann mehrere Zeilen fuer EINE echte Position enthalten (TP1/TP2/TP3
    als separate Teilverkaeufe derselben Position - siehe _bt_close_trade). Trefferquote und
    Ø-Gewinn/-Verlust werden deshalb auf POSITIONS-Ebene berechnet (alle Zeilen mit demselben
    Einstiegszeitpunkt werden zu einem Netto-Ergebnis zusammengefasst) - sonst wuerde eine
    Position, die TP1+TP2+TP3 durchlaeuft, dreifach als 'Gewinn' gezaehlt, eine SL-Position aber
    nur einfach als 'Verlust' - das verzerrt die Trefferquote massiv nach oben (in der Praxis
    beobachtet: 70% pro Teilverkauf-Zeile vs. 52% pro echter Position auf denselben Daten).
    Max-Drawdown bleibt bewusst auf Zeilenebene (echter Zeitreihen-Wert, jeder Teilverkauf
    veraendert das Konto tatsaechlich genau dann, wenn er passiert).

    `exclude_top_n`: Robustheits-Check - wie viele der besten Einzel-Trades (Positionen) sollen
    aus 'total_pnl_excl_top_n_usd' herausgerechnet werden? Wichtig bei 'immer im Markt'-Systemen
    (z.B. UT Bot + Hull Flip), wo ein einzelner grosser Pump/Dump-Trade das Gesamtergebnis
    dominieren und den Backtest/Sweep unrepraesentativ machen kann."""
    n = len(trades)
    if n == 0:
        return {"trades": 0, "fills": 0, "win_rate_pct": 0, "total_pnl_usd": 0, "avg_win_usd": 0, "avg_loss_usd": 0,
                "max_drawdown_usd": 0, "avg_bars_held": 0, "best_trade_pnl_usd": 0, "worst_trade_pnl_usd": 0,
                "median_trade_pnl_usd": 0, "total_pnl_excl_best_trade_usd": 0,
                "top_n_excluded_count": 0, "top_n_excluded_sum_usd": 0, "total_pnl_excl_top_n_usd": 0}

    total_pnl = sum(t["pnl"] for t in trades)
    equity = peak = max_dd = 0.0
    for t in trades:
        equity += t["pnl"]
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)

    # Teilverkaeufe zu echten Positionen gruppieren (gleicher Einstiegszeitpunkt = dieselbe
    # Position). Fallback auf einzeln zaehlen, falls mal kein entry_ts vorhanden sein sollte.
    positions = {}
    order = []
    for t in trades:
        key = t.get("entry_ts", id(t))
        if key not in positions:
            positions[key] = {"pnl": 0.0, "last_exit_i": None, "entry_i": None, "bars_held": 0}
            order.append(key)
        positions[key]["pnl"] += t["pnl"]
        positions[key]["bars_held"] = max(positions[key]["bars_held"], t["bars_held"])

    pos_list = [positions[k] for k in order]
    wins = [p for p in pos_list if p["pnl"] > 0]
    losses = [p for p in pos_list if p["pnl"] <= 0]
    n_pos = len(pos_list)

    sorted_desc = sorted(pos_list, key=lambda p: p["pnl"], reverse=True)
    best_trade_pnl = sorted_desc[0]["pnl"] if pos_list else 0.0
    worst_trade_pnl = sorted_desc[-1]["pnl"] if pos_list else 0.0
    pnls_sorted = sorted(p["pnl"] for p in pos_list)
    mid = len(pnls_sorted) // 2
    median_pnl = pnls_sorted[mid] if len(pnls_sorted) % 2 == 1 else (pnls_sorted[mid - 1] + pnls_sorted[mid]) / 2 if pnls_sorted else 0.0

    n_exclude = max(0, min(int(exclude_top_n), len(sorted_desc)))
    excluded_sum = sum(p["pnl"] for p in sorted_desc[:n_exclude])

    return {
        "trades": n_pos,
        "fills": n,
        "win_rate_pct": round(len(wins) / n_pos * 100, 1),
        "total_pnl_usd": round(total_pnl, 2),
        "avg_win_usd": round(sum(p["pnl"] for p in wins) / len(wins), 2) if wins else 0,
        "avg_loss_usd": round(sum(p["pnl"] for p in losses) / len(losses), 2) if losses else 0,
        "max_drawdown_usd": round(max_dd, 2),
        "avg_bars_held": round(sum(p["bars_held"] for p in pos_list) / n_pos, 1),
        "best_trade_pnl_usd": round(best_trade_pnl, 2),
        "worst_trade_pnl_usd": round(worst_trade_pnl, 2),
        "median_trade_pnl_usd": round(median_pnl, 2),
        "total_pnl_excl_best_trade_usd": round(total_pnl - best_trade_pnl, 2),
        "top_n_excluded_count": n_exclude,
        "top_n_excluded_sum_usd": round(excluded_sum, 2),
        "total_pnl_excl_top_n_usd": round(total_pnl - excluded_sum, 2),
    }


CE_SWEEP_MAX_COMBOS = 400
CE_SWEEP_MIN_RELIABLE_TRADES = 5


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
    candles, err, cache_used = await _fetch_cached_backtest_candles(symbol, resolution, days, max_candles, market_type=cfg.get("binance_market_type", "spot"))
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


DA_SWEEP_MAX_COMBOS = 400
DA_SWEEP_MIN_RELIABLE_TRADES = 5


async def run_da_param_sweep(symbol, cfg, days, atr_period_min, atr_period_max, atr_period_step,
                              sensitivity_min, sensitivity_max, sensitivity_step):
    """'Monte-Carlo'-Parametersweep fuer Diamond Algo: testet einen Bereich von ATR-Periode
    (SuperTrend-Kernbaustein) und Sensitivity (ATR-Multiplikator = Sensitivity*2) gegeneinander -
    das sind die beiden Parameter, die im Original wirklich das Signal beeinflussen (SMA-/EMA-
    Perioden bleiben auf den aktuell gespeicherten Werten, da sie selten geaendert werden)."""
    max_candles = BACKTEST_MAX_CANDLES["diamond_algo"]
    resolution = cfg.get("da_resolution", "5m")
    candles, err, cache_used = await _fetch_cached_backtest_candles(symbol, resolution, days, max_candles, market_type=cfg.get("binance_market_type", "spot"))
    if err:
        return {"error": err}
    if not candles or len(candles[4]) < 150:
        return {"error": "Zu wenig historische Kerzen für einen aussagekräftigen Sweep erhalten."}

    atr_periods = sorted(set(int(round(atr_period_min + i * atr_period_step))
                              for i in range(int((atr_period_max - atr_period_min) / max(atr_period_step, 1e-9)) + 1)
                              if atr_period_min + i * atr_period_step <= atr_period_max + 1e-9))
    sensitivities = sorted(set(round(sensitivity_min + i * sensitivity_step, 4)
                                for i in range(int((sensitivity_max - sensitivity_min) / max(sensitivity_step, 1e-9)) + 1)
                                if sensitivity_min + i * sensitivity_step <= sensitivity_max + 1e-9))
    atr_periods = [a for a in atr_periods if a >= 1]
    sensitivities = [s for s in sensitivities if s > 0]

    total_combos = len(atr_periods) * len(sensitivities)
    if total_combos == 0:
        return {"error": "Der eingestellte Bereich ergibt keine gültigen Kombinationen."}
    if total_combos > DA_SWEEP_MAX_COMBOS:
        return {"error": f"Zu viele Kombinationen ({total_combos}, Limit {DA_SWEEP_MAX_COMBOS}) - Bereich oder Schrittweite vergrößern."}

    o, h, l, c = candles[1], candles[2], candles[3], candles[4]
    sma_period = cfg["da_sma_period"]
    ema_trend_period = cfg["da_ema_trend_period"]
    risk_atr_period = cfg.get("da_risk_atr_period", 14)
    if cfg.get("da_use_heikin_ashi", False):
        _, sig_h, sig_l, sig_c = compute_heikin_ashi(o, h, l, c)
    else:
        sig_h, sig_l, sig_c = h, l, c
    atr_risk = compute_atr(sig_h, sig_l, sig_c, risk_atr_period)
    warmup = max(max(atr_periods), risk_atr_period, sma_period, ema_trend_period) + 5

    results = []
    for atr_p in atr_periods:
        for sens in sensitivities:
            buy, sell, smart_buy, smart_sell = compute_diamond_signal(sig_h, sig_l, sig_c, atr_p, sens, sma_period, ema_trend_period)
            cfg_copy = dict(cfg)
            cfg_copy["da_atr_period"] = atr_p
            cfg_copy["da_sensitivity"] = sens
            trades = _simulate_diamond_trades(candles, cfg_copy, buy, sell, smart_buy, smart_sell, atr_risk, warmup)
            stats = summarize_backtest_trades(trades)
            results.append({"da_atr_period": atr_p, "da_sensitivity": sens, **stats})

    best_sorted = sorted(results, key=lambda r: (r["trades"] >= DA_SWEEP_MIN_RELIABLE_TRADES, r["total_pnl_usd"]), reverse=True)
    worst_sorted = sorted(results, key=lambda r: r["total_pnl_usd"])

    actual_days = (candles[0][-1] - candles[0][0]) / (24 * 60 * 60 * 1000)
    return {
        "symbol": symbol, "resolution": resolution, "requested_days": days,
        "actual_days_covered": round(actual_days, 1), "candles_processed": len(candles[4]),
        "min_reliable_trades": DA_SWEEP_MIN_RELIABLE_TRADES,
        "combos_tested": total_combos,
        "results": best_sorted[:30],
        "worst_results": worst_sorted[:20],
    }


ES_SENS_SWEEP_MAX_COMBOS = 2000
ES_SENS_SWEEP_MIN_RELIABLE_TRADES = 5


async def run_es_sensitivity_sweep(symbol, cfg, days, sens_min, sens_max, sens_step):
    """'Monte-Carlo'-Parametersweep fuer ELTE Smart, NUR ueber die manuelle Sensitivity - mit
    zwei Nachkommastellen wie im Original-Skript (sensitivity11 = input.float(..., step=0.01,
    minval=0.11, maxval=20)). Auto-Sensitivity wird fuer den Sweep zwangsweise deaktiviert -
    der Sinn des Tests ist ja gerade, verschiedene FESTE Sensitivity-Werte gegeneinander zu
    vergleichen (bei Auto-Sensitivity waere der Wert ja gar nicht mehr frei waehlbar). Alle
    anderen ELTE-Smart-Einstellungen (ATR-Periode, SL/TP-Modus, R:R usw.) bleiben auf den
    aktuell gespeicherten Werten."""
    max_candles = BACKTEST_MAX_CANDLES["elte_smart"]
    resolution = cfg.get("es_resolution", "5m")
    candles, err, cache_used = await _fetch_cached_backtest_candles(symbol, resolution, days, max_candles, market_type=cfg.get("binance_market_type", "spot"))
    if err:
        return {"error": err}
    if not candles or len(candles[4]) < 150:
        return {"error": "Zu wenig historische Kerzen für einen aussagekräftigen Sweep erhalten."}

    steps = int(round((sens_max - sens_min) / max(sens_step, 1e-9)))
    sens_values = sorted(set(round(sens_min + i * sens_step, 2) for i in range(steps + 1)
                              if sens_min + i * sens_step <= sens_max + 1e-9))
    sens_values = [v for v in sens_values if v > 0]

    if len(sens_values) == 0:
        return {"error": "Der eingestellte Bereich ergibt keine gültigen Werte."}
    if len(sens_values) > ES_SENS_SWEEP_MAX_COMBOS:
        return {"error": f"Zu viele Werte ({len(sens_values)}, Limit {ES_SENS_SWEEP_MAX_COMBOS}) - Bereich oder Schrittweite vergrößern."}

    results = []
    for sens in sens_values:
        cfg_copy = dict(cfg)
        cfg_copy["es_auto_sensitivity"] = False
        cfg_copy["es_sensitivity"] = sens
        trades = backtest_elte_smart(candles, cfg_copy)
        stats = summarize_backtest_trades(trades)
        results.append({"es_sensitivity": sens, **stats})

    best_sorted = sorted(results, key=lambda r: (r["trades"] >= ES_SENS_SWEEP_MIN_RELIABLE_TRADES, r["total_pnl_usd"]), reverse=True)
    worst_sorted = sorted(results, key=lambda r: r["total_pnl_usd"])

    actual_days = (candles[0][-1] - candles[0][0]) / (24 * 60 * 60 * 1000)
    return {
        "symbol": symbol, "resolution": resolution, "requested_days": days,
        "actual_days_covered": round(actual_days, 1), "candles_processed": len(candles[4]),
        "min_reliable_trades": ES_SENS_SWEEP_MIN_RELIABLE_TRADES,
        "combos_tested": len(sens_values),
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


async def _fetch_cached_backtest_candles(symbol, resolution, days, max_candles, market_type="spot"):
    """Gemeinsame Kerzen-Cache-Logik (sonst 1:1 dupliziert) - wird gebraucht, weil Chandelier
    Exit im Backtest ggf. ZWEI verschiedene Aufloesungen gleichzeitig braucht (eigener
    Zeitrahmen + hoeherer SuperTrend-Filter-Zeitrahmen)."""
    if resolution in SUB_MINUTE_RESOLUTIONS:
        max_candles = min(max_candles, 5000)
    cache_key = (symbol, resolution, market_type)
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
        candles, err = await fetch_historical_candles_binance(symbol, resolution, days, max_candles, market_type=market_type)
        if candles:
            _backtest_cache_set(cache_key, {"fetched_at": now, "days": days, "max_candles": max_candles, "candles": candles})
    return candles, err, cache_used


def _simulate_mo7_trades(candles_ts, mo7, cfg, bull, bear, warmup):
    """Kern-Simulation fuer MO7: nur fester SL/TP (kein ATR-Modus), optionaler Flip-Exit,
    Cooldown nach SL. `candles_ts` ist das 6er-Tupel (ts,o,h,l,c,volumes)."""
    ts, o, h, l, c, v = candles_ts
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    sl_enabled = cfg.get("mo7_sl_enabled", True)
    tp_enabled = cfg.get("mo7_tp_enabled", True)
    sl_manual_usd = cfg.get("mo7_sl_manual_usd", 5.0)
    tp_manual_usd = cfg.get("mo7_tp_manual_usd", 5.0)
    sl_cooldown_ms = cfg.get("mo7_sl_cooldown_seconds", 30) * 1000
    direction_mode = cfg.get("mo7_direction_mode", "both")
    flip_exit_enabled = cfg.get("mo7_flip_exit_enabled", True)

    position = None
    trades = []
    sl_cooldown_until_ts = None

    for i in range(warmup, n):
        price = c[i]

        if position is not None:
            pdir, entry = position["dir"], position["entry"]
            sl_price = position.get("sl_price")
            hit_sl = sl_price is not None and ((pdir == "long" and l[i] <= sl_price) or (pdir == "short" and h[i] >= sl_price))
            if hit_sl:
                _bt_close_trade(trades, pdir, entry, sl_price, position["size"], i, position["entry_i"], "SL", ts=ts)
                position = None
                sl_cooldown_until_ts = ts[i] + sl_cooldown_ms
            else:
                tp_price = position.get("tp_price")
                hit_tp = tp_price is not None and ((pdir == "long" and h[i] >= tp_price) or (pdir == "short" and l[i] <= tp_price))
                if hit_tp:
                    _bt_close_trade(trades, pdir, entry, tp_price, position["size"], i, position["entry_i"], "TP", ts=ts)
                    position = None

        buy_signal, sell_signal = bull[i], bear[i]
        if direction_mode == "long_only":
            sell_signal = False
        elif direction_mode == "short_only":
            buy_signal = False

        just_flipped = False
        if position is not None and flip_exit_enabled:
            if (position["dir"] == "long" and sell_signal) or (position["dir"] == "short" and buy_signal):
                _bt_close_trade(trades, position["dir"], position["entry"], price, position["size"], i, position["entry_i"], "MO7-FLIP-EXIT", ts=ts)
                position = None
                just_flipped = True

        in_sl_cooldown = sl_cooldown_until_ts is not None and ts[i] < sl_cooldown_until_ts
        if position is None and not in_sl_cooldown and not just_flipped and (buy_signal or sell_signal):
            direction = "long" if buy_signal else "short"
            size = (margin * leverage) / price
            sl_price = tp_price = None
            if sl_enabled and size > 0:
                dist_sl = sl_manual_usd / size
                sl_price = price - dist_sl if direction == "long" else price + dist_sl
            if tp_enabled and size > 0:
                dist_tp = tp_manual_usd / size
                tp_price = price + dist_tp if direction == "long" else price - dist_tp
            position = {"dir": direction, "entry": price, "size": size, "entry_i": i,
                        "sl_price": sl_price, "tp_price": tp_price}

    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)

    return trades


def backtest_mo7(candles_vol, cfg):
    ts, o, h, l, c, v = candles_vol
    mo7 = compute_mo7_series(h, l, c, v, cfg)
    bull, bear = compute_mo7_signals(mo7, cfg)
    warmup = 505  # 500er-Normierungsfenster + Puffer fuer RSI/Stoch/WPR/MFI-Anlauf
    return _simulate_mo7_trades(candles_vol, mo7, cfg, bull, bear, warmup)


MO7_SUM_SWEEP_MAX_COMBOS = 2000
MO7_SUM_SWEEP_MIN_RELIABLE_TRADES = 5


async def _fetch_cached_mo7_backtest_candles(symbol, resolution, days, max_candles, market_type="spot"):
    """Eigene, einfachere Cache-Variante fuer MO7 (6er-Tupel MIT Volumen statt 5er) - bewusst
    getrennt von _fetch_cached_backtest_candles, um die dort genutzte 5er-Tupel-Annahme (candles[4]
    fuer closes) nicht zu gefaehrden."""
    cache_key = ("mo7", symbol, resolution, market_type)
    cached = _backtest_cache_get(cache_key)
    now = time.time()
    if (cached and (now - cached["fetched_at"] < BACKTEST_CACHE_TTL_SECONDS)
            and cached["days"] >= days and cached.get("max_candles", 0) >= max_candles
            and len(cached["candles"][4]) >= 100):
        ts, o, h, l, c, vol = cached["candles"]
        cutoff = ts[-1] - days * 24 * 60 * 60 * 1000
        idx = 0
        for i, t in enumerate(ts):
            if t >= cutoff:
                idx = i
                break
        candles = (ts[idx:], o[idx:], h[idx:], l[idx:], c[idx:], vol[idx:])
        if len(candles[4]) > max_candles:
            candles = tuple(x[-max_candles:] for x in candles)
        return candles, None
    candles, err = await fetch_historical_candles_binance_vol(symbol, resolution, days, max_candles, market_type=market_type)
    if candles:
        _backtest_cache_set(cache_key, {"fetched_at": now, "days": days, "max_candles": max_candles, "candles": candles})
    return candles, err


async def run_mo7_sum_sweep(symbol, cfg, days, sum_low_min, sum_low_max, sum_low_step, sum_high_min, sum_high_max, sum_high_step, exclude_top_n=1):
    """'Monte-Carlo'-Parametersweep fuer den 'five_candle_sum'-Einstiegsmodus: testet einen
    Bereich von mo7_sum_low (Long-Schwelle) und mo7_sum_high (Short-Schwelle) gegeneinander.
    Der MO7-Score selbst wird NUR EINMAL berechnet (unabhaengig von den Schwellen) und fuer alle
    Kombinationen wiederverwendet - sonst waere der Sweep bei vielen Kombinationen viel zu
    langsam."""
    max_candles = BACKTEST_MAX_CANDLES["mo7_scalp"]
    resolution = cfg.get("mo7_resolution", "5m")
    candles, err = await _fetch_cached_mo7_backtest_candles(symbol, resolution, days, max_candles, market_type=cfg.get("binance_market_type", "spot"))
    if err:
        return {"error": err}
    if not candles or len(candles[4]) < 550:
        return {"error": "Zu wenig historische Kerzen für einen aussagekräftigen Sweep erhalten (mind. ~550 für das 500er-Normierungsfenster nötig)."}

    ts, o, h, l, c, v = candles
    mo7 = compute_mo7_series(h, l, c, v, cfg)
    warmup = 505

    sum_lows = sorted(set(round(sum_low_min + i * sum_low_step, 1)
                           for i in range(int((sum_low_max - sum_low_min) / max(sum_low_step, 1e-9)) + 1)
                           if sum_low_min + i * sum_low_step <= sum_low_max + 1e-9))
    sum_highs = sorted(set(round(sum_high_min + i * sum_high_step, 1)
                            for i in range(int((sum_high_max - sum_high_min) / max(sum_high_step, 1e-9)) + 1)
                            if sum_high_min + i * sum_high_step <= sum_high_max + 1e-9))
    sum_lows = [x for x in sum_lows if x > 0]
    sum_highs = [x for x in sum_highs if x > 0]

    total_combos = len(sum_lows) * len(sum_highs)
    if total_combos == 0:
        return {"error": "Der eingestellte Bereich ergibt keine gültigen Kombinationen."}
    if total_combos > MO7_SUM_SWEEP_MAX_COMBOS:
        return {"error": f"Zu viele Kombinationen ({total_combos}, Limit {MO7_SUM_SWEEP_MAX_COMBOS}) - Bereich oder Schrittweite vergrößern."}

    results = []
    for sum_low in sum_lows:
        for sum_high in sum_highs:
            cfg_copy = dict(cfg)
            cfg_copy["mo7_entry_mode"] = "five_candle_sum"
            cfg_copy["mo7_sum_low"] = sum_low
            cfg_copy["mo7_sum_high"] = sum_high
            bull, bear = compute_mo7_signals(mo7, cfg_copy)
            trades = _simulate_mo7_trades(candles, mo7, cfg_copy, bull, bear, warmup)
            stats = summarize_backtest_trades(trades, exclude_top_n)
            results.append({"mo7_sum_low": sum_low, "mo7_sum_high": sum_high, **stats})

    best_sorted = sorted(results, key=lambda r: (r["trades"] >= MO7_SUM_SWEEP_MIN_RELIABLE_TRADES, r["total_pnl_usd"]), reverse=True)
    worst_sorted = sorted(results, key=lambda r: r["total_pnl_usd"])

    actual_days = (ts[-1] - ts[0]) / (24 * 60 * 60 * 1000)
    return {
        "symbol": symbol, "resolution": resolution, "requested_days": days,
        "actual_days_covered": round(actual_days, 1), "candles_processed": len(c),
        "min_reliable_trades": MO7_SUM_SWEEP_MIN_RELIABLE_TRADES,
        "combos_tested": total_combos,
        "results": best_sorted[:30],
        "worst_results": worst_sorted[:20],
    }


def _uh_bt_set_sl(position, cfg, margin, leverage):
    """Backtest-Pendant zu _uh_set_sl - setzt sl_price auf der Position (fester $-Betrag)."""
    if not cfg.get("utb_sl_enabled", False):
        position["sl_price"] = None
        return
    size = position["size"]
    if size <= 0:
        position["sl_price"] = None
        return
    dist_sl = cfg.get("utb_sl_manual_usd", 5.0) / size
    entry = position["entry"]
    position["sl_price"] = entry - dist_sl if position["dir"] == "long" else entry + dist_sl


def _simulate_uh_trades(candles, cfg, buy, sell, long_flip, short_flip, hull_green, warmup, trend_pct=None):
    """Immer-im-Markt-Simulation fuer UT Bot + Hull Flip (Flip statt Exit), optional mit festem
    SL (fester $-Betrag) - bei SL-Treffer geht die Position glatt und wartet (nach Cooldown) auf
    das naechste gueltige Ersteinstiegs-Signal, statt direkt zu drehen. Siehe check_uh_signal/
    check_uh_sl fuer die identische Logik im Live-Betrieb. Optionaler MTF-Trend%-Filter
    (trend_pct, siehe _pk_build_mtf_trend_pct) gilt fuer JEDEN Einstieg, auch beim Flip."""
    ts, o, h, l, c = candles
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    direction_mode = cfg.get("utb_direction_mode", "both")
    signal_only = cfg.get("utb_flip_trigger", "hull_color") == "signal_only"
    sl_cooldown_ms = cfg.get("utb_sl_cooldown_seconds", 30) * 1000
    mtf_enabled = cfg.get("utb_mtf_filter_enabled", False)
    long_thr = cfg.get("utb_mtf_long_threshold", 0.5)
    short_thr = cfg.get("utb_mtf_short_threshold", -0.5)

    def long_ok(i):
        return direction_mode != "short_only" and (not mtf_enabled or trend_pct is None or trend_pct[i] > long_thr)

    def short_ok(i):
        return direction_mode != "long_only" and (not mtf_enabled or trend_pct is None or trend_pct[i] < short_thr)

    position = None
    trades = []
    sl_cooldown_until_ts = None

    for i in range(warmup, n):
        price = c[i]

        if position is not None:
            sl_price = position.get("sl_price")
            hit_sl = sl_price is not None and ((position["dir"] == "long" and l[i] <= sl_price) or (position["dir"] == "short" and h[i] >= sl_price))
            if hit_sl:
                _bt_close_trade(trades, position["dir"], position["entry"], sl_price, position["size"], i, position["entry_i"], "SL", ts=ts)
                position = None
                sl_cooldown_until_ts = ts[i] + sl_cooldown_ms

        in_sl_cooldown = sl_cooldown_until_ts is not None and ts[i] < sl_cooldown_until_ts

        if position is None:
            if in_sl_cooldown:
                continue
            if signal_only:
                if buy[i] and long_ok(i):
                    size = (margin * leverage) / price
                    position = {"dir": "long", "entry": price, "size": size, "entry_i": i}
                    _uh_bt_set_sl(position, cfg, margin, leverage)
                elif sell[i] and short_ok(i):
                    size = (margin * leverage) / price
                    position = {"dir": "short", "entry": price, "size": size, "entry_i": i}
                    _uh_bt_set_sl(position, cfg, margin, leverage)
                continue
            if hull_green[i] is None:
                continue
            if buy[i] and hull_green[i] and long_ok(i):
                size = (margin * leverage) / price
                position = {"dir": "long", "entry": price, "size": size, "entry_i": i}
                _uh_bt_set_sl(position, cfg, margin, leverage)
            elif sell[i] and not hull_green[i] and short_ok(i):
                size = (margin * leverage) / price
                position = {"dir": "short", "entry": price, "size": size, "entry_i": i}
                _uh_bt_set_sl(position, cfg, margin, leverage)
            continue

        if position["dir"] == "long" and short_flip[i]:
            can_flip = direction_mode != "long_only" and short_ok(i)
            if can_flip:
                reason = "UTB-HULL-FLIP"
            elif direction_mode == "long_only":
                reason = "UTB-HULL-EXIT-DIR"
            else:
                reason = "UTB-HULL-EXIT-MTF"
            _bt_close_trade(trades, "long", position["entry"], price, position["size"], i, position["entry_i"], reason, ts=ts)
            if not can_flip:
                position = None
            else:
                size = (margin * leverage) / price
                position = {"dir": "short", "entry": price, "size": size, "entry_i": i}
                _uh_bt_set_sl(position, cfg, margin, leverage)
        elif position["dir"] == "short" and long_flip[i]:
            can_flip = direction_mode != "short_only" and long_ok(i)
            if can_flip:
                reason = "UTB-HULL-FLIP"
            elif direction_mode == "short_only":
                reason = "UTB-HULL-EXIT-DIR"
            else:
                reason = "UTB-HULL-EXIT-MTF"
            _bt_close_trade(trades, "short", position["entry"], price, position["size"], i, position["entry_i"], reason, ts=ts)
            if not can_flip:
                position = None
            else:
                size = (margin * leverage) / price
                position = {"dir": "long", "entry": price, "size": size, "entry_i": i}
                _uh_bt_set_sl(position, cfg, margin, leverage)

    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)

    return trades


def backtest_ut_bot_hull(candles, cfg):
    ts, o, h, l, c = candles
    atr_period = cfg.get("utb_atr_period", 1)
    sensitivity = cfg.get("utb_sensitivity", 1.0)
    use_ha = cfg.get("utb_heikin_ashi", False)
    hull_period = cfg.get("utb_hull_period", 31)
    buy, sell, stop_line = compute_ut_bot(o, h, l, c, atr_period, sensitivity, use_ha)
    hma = compute_hull_ma(c, hull_period)
    hull_green = [None] * len(c)
    for i in range(1, len(c)):
        if hma[i] is not None and hma[i - 1] is not None:
            hull_green[i] = hma[i] > hma[i - 1]
    long_flip, short_flip = compute_ut_hull_flip_signals(buy, sell, hull_green, cfg)
    trend_pct = cfg.get("_utb_trend_pct_precomputed")  # von run_backtest vorab async berechnet
    # (siehe _pk_build_mtf_trend_pct) - diese Funktion selbst ist NICHT async (einheitliche
    # BACKTEST_FUNCS-Signatur (candles, cfg))
    if trend_pct is None and cfg.get("utb_mtf_filter_enabled", False):
        mtf_fast = cfg.get("utb_mtf_fast_len", 5)
        mtf_slow = cfg.get("utb_mtf_slow_len", 9)
        mtf_atr = cfg.get("utb_mtf_atr_len", 14)
        trend_pct = compute_pk_trend_percent(h, l, c, mtf_fast, mtf_slow, mtf_atr)
    warmup = max(atr_period, hull_period + round(math.sqrt(hull_period)) + 2, 5) + 2
    return _simulate_uh_trades(candles, cfg, buy, sell, long_flip, short_flip, hull_green, warmup, trend_pct)


UTB_SWEEP_MAX_COMBOS = 2000
UTB_SWEEP_MIN_RELIABLE_TRADES = 5


async def run_utb_param_sweep(symbol, cfg, days, atr_period_min, atr_period_max, atr_period_step,
                               sensitivity_min, sensitivity_max, sensitivity_step, exclude_top_n=1,
                               long_threshold_min=None, long_threshold_max=None, long_threshold_step=0.5,
                               short_threshold_min=None, short_threshold_max=None, short_threshold_step=0.5):
    """'Monte-Carlo'-Parametersweep fuer UT Bot + Hull Flip: testet einen Bereich von ATR-Periode
    und Sensitivity (die beiden Parameter, die im Original-Pine-Script beide irrefuehrend
    'Period' heissen) gegeneinander - optional zusaetzlich noch die MTF-Trend%-Long-/Short-
    Schwelle (nur wirksam, wenn utb_mtf_filter_enabled an ist; Min==Max bedeutet einfach 1
    fester Wert, keine zusaetzlichen Kombinationen - das ist auch der Default, wenn die
    Schwellen-Parameter weggelassen werden). Die Hull-MA UND der optionale MTF-Trend%-Filter
    werden nur EINMAL berechnet (unabhaengig von allen gesweepten Parametern) und fuer alle
    Kombinationen wiederverwendet."""
    max_candles = BACKTEST_MAX_CANDLES["ut_bot_hull"]
    resolution = cfg.get("utb_resolution", "5m")
    candles, err, cache_used = await _fetch_cached_backtest_candles(symbol, resolution, days, max_candles, market_type=cfg.get("binance_market_type", "spot"))
    if err:
        return {"error": err}
    if not candles or len(candles[4]) < 150:
        return {"error": "Zu wenig historische Kerzen für einen aussagekräftigen Sweep erhalten."}

    atr_periods = sorted(set(int(round(atr_period_min + i * atr_period_step))
                              for i in range(int((atr_period_max - atr_period_min) / max(atr_period_step, 1e-9)) + 1)
                              if atr_period_min + i * atr_period_step <= atr_period_max + 1e-9))
    sensitivities = sorted(set(round(sensitivity_min + i * sensitivity_step, 4)
                                for i in range(int((sensitivity_max - sensitivity_min) / max(sensitivity_step, 1e-9)) + 1)
                                if sensitivity_min + i * sensitivity_step <= sensitivity_max + 1e-9))
    atr_periods = [a for a in atr_periods if a >= 1]
    sensitivities = [s for s in sensitivities if s > 0]

    if long_threshold_min is None:
        long_threshold_min = cfg.get("utb_mtf_long_threshold", 0.5)
    if long_threshold_max is None:
        long_threshold_max = long_threshold_min
    if short_threshold_min is None:
        short_threshold_min = cfg.get("utb_mtf_short_threshold", -0.5)
    if short_threshold_max is None:
        short_threshold_max = short_threshold_min
    long_thresholds = sorted(set(round(long_threshold_min + i * long_threshold_step, 4)
                                  for i in range(int((long_threshold_max - long_threshold_min) / max(long_threshold_step, 1e-9)) + 1)
                                  if long_threshold_min + i * long_threshold_step <= long_threshold_max + 1e-9))
    short_thresholds = sorted(set(round(short_threshold_min + i * short_threshold_step, 4)
                                   for i in range(int((short_threshold_max - short_threshold_min) / max(short_threshold_step, 1e-9)) + 1)
                                   if short_threshold_min + i * short_threshold_step <= short_threshold_max + 1e-9))
    if not long_thresholds:
        long_thresholds = [long_threshold_min]
    if not short_thresholds:
        short_thresholds = [short_threshold_min]

    total_combos = len(atr_periods) * len(sensitivities) * len(long_thresholds) * len(short_thresholds)
    if total_combos == 0:
        return {"error": "Der eingestellte Bereich ergibt keine gültigen Kombinationen."}
    if total_combos > UTB_SWEEP_MAX_COMBOS:
        return {"error": f"Zu viele Kombinationen ({total_combos}, Limit {UTB_SWEEP_MAX_COMBOS}) - Bereich oder Schrittweite vergrößern."}

    o, h, l, c = candles[1], candles[2], candles[3], candles[4]
    hull_period = cfg.get("utb_hull_period", 31)
    hma = compute_hull_ma(c, hull_period)
    hull_green = [None] * len(c)
    for i in range(1, len(c)):
        if hma[i] is not None and hma[i - 1] is not None:
            hull_green[i] = hma[i] > hma[i - 1]

    trend_pct = None
    if cfg.get("utb_mtf_filter_enabled", False):
        trend_pct, mtf_err = await _pk_build_mtf_trend_pct(symbol, cfg, days, candles[0], h, l, c, resolution, prefix="utb")
        if mtf_err:
            return {"error": mtf_err}

    results = []
    for atr_p in atr_periods:
        for sens in sensitivities:
            buy, sell, stop_line = compute_ut_bot(o, h, l, c, atr_p, sens, cfg.get("utb_heikin_ashi", False))
            long_flip, short_flip = compute_ut_hull_flip_signals(buy, sell, hull_green, cfg)
            warmup = max(atr_p, hull_period + round(math.sqrt(hull_period)) + 2, 5) + 2
            for long_thr in long_thresholds:
                for short_thr in short_thresholds:
                    cfg_combo = dict(cfg)
                    cfg_combo["utb_mtf_long_threshold"] = long_thr
                    cfg_combo["utb_mtf_short_threshold"] = short_thr
                    trades = _simulate_uh_trades(candles, cfg_combo, buy, sell, long_flip, short_flip, hull_green, warmup, trend_pct)
                    stats = summarize_backtest_trades(trades, exclude_top_n)
                    results.append({"utb_atr_period": atr_p, "utb_sensitivity": sens,
                                     "utb_mtf_long_threshold": long_thr, "utb_mtf_short_threshold": short_thr, **stats})

    best_sorted = sorted(results, key=lambda r: (r["trades"] >= UTB_SWEEP_MIN_RELIABLE_TRADES, r["total_pnl_usd"]), reverse=True)
    worst_sorted = sorted(results, key=lambda r: r["total_pnl_usd"])

    actual_days = (candles[0][-1] - candles[0][0]) / (24 * 60 * 60 * 1000)
    return {
        "symbol": symbol, "resolution": resolution, "requested_days": days,
        "actual_days_covered": round(actual_days, 1), "candles_processed": len(c),
        "min_reliable_trades": UTB_SWEEP_MIN_RELIABLE_TRADES,
        "combos_tested": total_combos,
        "results": best_sorted[:30],
        "worst_results": worst_sorted[:20],
    }


def _simulate_wtc_trades(candles, cfg, bull, bear, warmup):
    """Backtest-Pendant zu check_wtc_signal/check_wtc_sl_tp - beide Betriebsarten
    (wtc_always_in_market An/Aus) identisch nachgebildet."""
    ts, o, h, l, c = candles
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    direction_mode = cfg.get("wtc_direction_mode", "both")
    always_in_market = cfg.get("wtc_always_in_market", False)
    flip_exit_enabled = cfg.get("wtc_flip_exit_enabled", True)
    sl_enabled = cfg.get("wtc_sl_enabled", True)
    tp_enabled = cfg.get("wtc_tp_enabled", True)
    sl_manual_usd = cfg.get("wtc_sl_manual_usd", 5.0)
    tp_manual_usd = cfg.get("wtc_tp_manual_usd", 5.0)
    sl_cooldown_ms = cfg.get("wtc_sl_cooldown_seconds", 30) * 1000

    def make_position(direction, price, i):
        size = (margin * leverage) / price
        pos = {"dir": direction, "entry": price, "size": size, "entry_i": i, "sl_price": None, "tp_price": None}
        if sl_enabled and size > 0:
            dist_sl = sl_manual_usd / size
            pos["sl_price"] = price - dist_sl if direction == "long" else price + dist_sl
        if tp_enabled and size > 0:
            dist_tp = tp_manual_usd / size
            pos["tp_price"] = price + dist_tp if direction == "long" else price - dist_tp
        return pos

    position = None
    trades = []
    sl_cooldown_until_ts = None

    for i in range(warmup, n):
        price = c[i]
        buy_i, sell_i = bull[i], bear[i]
        if direction_mode == "long_only":
            sell_i = False
        elif direction_mode == "short_only":
            buy_i = False

        if position is not None:
            sl_price = position.get("sl_price")
            tp_price = position.get("tp_price")
            hit_sl = sl_price is not None and ((position["dir"] == "long" and l[i] <= sl_price) or (position["dir"] == "short" and h[i] >= sl_price))
            hit_tp = tp_price is not None and ((position["dir"] == "long" and h[i] >= tp_price) or (position["dir"] == "short" and l[i] <= tp_price))
            if hit_sl:
                _bt_close_trade(trades, position["dir"], position["entry"], sl_price, position["size"], i, position["entry_i"], "SL", ts=ts)
                position = None
                sl_cooldown_until_ts = ts[i] + sl_cooldown_ms
            elif hit_tp:
                _bt_close_trade(trades, position["dir"], position["entry"], tp_price, position["size"], i, position["entry_i"], "TP", ts=ts)
                position = None

        if position is not None:
            if always_in_market:
                if position["dir"] == "long" and sell_i:
                    _bt_close_trade(trades, "long", position["entry"], price, position["size"], i, position["entry_i"], "WTC-FLIP", ts=ts)
                    position = make_position("short", price, i)
                elif position["dir"] == "short" and buy_i:
                    _bt_close_trade(trades, "short", position["entry"], price, position["size"], i, position["entry_i"], "WTC-FLIP", ts=ts)
                    position = make_position("long", price, i)
            elif flip_exit_enabled:
                if (position["dir"] == "long" and sell_i) or (position["dir"] == "short" and buy_i):
                    _bt_close_trade(trades, position["dir"], position["entry"], price, position["size"], i, position["entry_i"], "WTC-FLIP-EXIT", ts=ts)
                    position = None

        in_sl_cooldown = sl_cooldown_until_ts is not None and ts[i] < sl_cooldown_until_ts
        if position is None and not in_sl_cooldown and (buy_i or sell_i):
            direction = "long" if buy_i else "short"
            position = make_position(direction, price, i)

    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)

    return trades


def backtest_wavetrend_cross(candles, cfg):
    ts, o, h, l, c = candles
    bull, bear, wt1, wt2 = compute_wtc_signals(h, l, c, cfg)
    chlen = cfg.get("wtc_channel_len", 9)
    avg = cfg.get("wtc_average_len", 12)
    warmup = max(chlen, avg, 20) + 10
    return _simulate_wtc_trades(candles, cfg, bull, bear, warmup)


def _fr_bt_set_sl(position, cfg, margin, leverage):
    """Backtest-Pendant zu _fr_set_sl - setzt sl_price auf der Position (fester $-Betrag)."""
    if not cfg.get("fr_sl_enabled", False):
        position["sl_price"] = None
        return
    size = position["size"]
    if size <= 0:
        position["sl_price"] = None
        return
    dist_sl = cfg.get("fr_sl_manual_usd", 5.0) / size
    entry = position["entry"]
    position["sl_price"] = entry - dist_sl if position["dir"] == "long" else entry + dist_sl


def _simulate_fr_trades(candles, cfg, up_fractal, down_fractal, warmup, zscore=None):
    """Backtest-Pendant zu check_fr_signal - immer im Markt, reiner Buy/Sell-Wechsel, optional
    mit Z-Score-Filter (siehe compute_rolling_zscore) und optionalem festem SL (siehe
    _fr_bt_set_sl - durchbricht 'immer im Markt' NUR im SL-Fall, Position geht dann glatt und
    wartet nach Cooldown). Tief-Fraktal = Kauf, Hoch-Fraktal = Verkauf."""
    ts, o, h, l, c = candles
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    direction_mode = cfg.get("fr_direction_mode", "both")
    zscore_enabled = cfg.get("fr_zscore_filter_enabled", False)
    sl_cooldown_ms = cfg.get("fr_sl_cooldown_seconds", 30) * 1000

    def long_ok(i):
        return direction_mode != "short_only" and (not zscore_enabled or zscore is None or zscore[i] > 0)

    def short_ok(i):
        return direction_mode != "long_only" and (not zscore_enabled or zscore is None or zscore[i] < 0)

    position = None
    trades = []
    sl_cooldown_until_ts = None

    for i in range(warmup, n):
        price = c[i]
        buy_i = down_fractal[i]
        sell_i = up_fractal[i]

        if position is not None:
            sl_price = position.get("sl_price")
            hit_sl = sl_price is not None and ((position["dir"] == "long" and l[i] <= sl_price) or (position["dir"] == "short" and h[i] >= sl_price))
            if hit_sl:
                _bt_close_trade(trades, position["dir"], position["entry"], sl_price, position["size"], i, position["entry_i"], "SL", ts=ts)
                position = None
                sl_cooldown_until_ts = ts[i] + sl_cooldown_ms

        in_sl_cooldown = sl_cooldown_until_ts is not None and ts[i] < sl_cooldown_until_ts

        if position is None:
            if in_sl_cooldown:
                continue
            if buy_i and long_ok(i):
                size = (margin * leverage) / price
                position = {"dir": "long", "entry": price, "size": size, "entry_i": i}
                _fr_bt_set_sl(position, cfg, margin, leverage)
            elif sell_i and short_ok(i):
                size = (margin * leverage) / price
                position = {"dir": "short", "entry": price, "size": size, "entry_i": i}
                _fr_bt_set_sl(position, cfg, margin, leverage)
            continue

        if position["dir"] == "long" and sell_i:
            can_flip = direction_mode != "long_only" and short_ok(i)
            reason = "FR-FLIP" if can_flip else ("FR-EXIT-DIR" if direction_mode == "long_only" else "FR-EXIT-ZSCORE")
            _bt_close_trade(trades, "long", position["entry"], price, position["size"], i, position["entry_i"], reason, ts=ts)
            if not can_flip:
                position = None
            else:
                size = (margin * leverage) / price
                position = {"dir": "short", "entry": price, "size": size, "entry_i": i}
                _fr_bt_set_sl(position, cfg, margin, leverage)
        elif position["dir"] == "short" and buy_i:
            can_flip = direction_mode != "short_only" and long_ok(i)
            reason = "FR-FLIP" if can_flip else ("FR-EXIT-DIR" if direction_mode == "short_only" else "FR-EXIT-ZSCORE")
            _bt_close_trade(trades, "short", position["entry"], price, position["size"], i, position["entry_i"], reason, ts=ts)
            if not can_flip:
                position = None
            else:
                size = (margin * leverage) / price
                position = {"dir": "long", "entry": price, "size": size, "entry_i": i}
                _fr_bt_set_sl(position, cfg, margin, leverage)

    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)

    return trades


def backtest_fractals_flip(candles, cfg):
    ts, o, h, l, c = candles
    n_periods = cfg.get("fr_periods", 2)
    up_fractal, down_fractal = compute_fractals(h, l, n_periods)
    if cfg.get("fr_invert_direction", False):
        up_fractal, down_fractal = down_fractal, up_fractal
    zscore = cfg.get("_fr_zscore_precomputed")  # von run_backtest vorab async berechnet, falls
    # eine abweichende Zeiteinheit gewaehlt ist (siehe _build_zscore_series_for_backtest) - diese
    # Funktion selbst ist NICHT async (einheitliche BACKTEST_FUNCS-Signatur (candles, cfg))
    if zscore is None and cfg.get("fr_zscore_filter_enabled", False):
        zscore = compute_rolling_zscore(c, cfg.get("fr_zscore_lookback", 20), cfg.get("fr_zscore_smooth", 3))
    warmup = 2 * n_periods + 5
    return _simulate_fr_trades(candles, cfg, up_fractal, down_fractal, warmup, zscore)


def _cd_bt_set_sl(position, cfg, margin, leverage):
    """Backtest-Pendant zu _cd_set_sl - setzt sl_price auf der Position (fester $-Betrag)."""
    if not cfg.get("cd_sl_enabled", False):
        position["sl_price"] = None
        return
    size = position["size"]
    if size <= 0:
        position["sl_price"] = None
        return
    dist_sl = cfg.get("cd_sl_manual_usd", 5.0) / size
    entry = position["entry"]
    position["sl_price"] = entry - dist_sl if position["dir"] == "long" else entry + dist_sl


def _simulate_cd_trades(candles, cfg, buy_signal, sell_signal, warmup, zscore=None):
    """Backtest-Pendant zu check_cd_signal - immer im Markt, reiner Buy/Sell-Wechsel, optional
    mit Z-Score-Filter (siehe compute_rolling_zscore) und optionalem festem SL (siehe
    _cd_bt_set_sl)."""
    ts, o, h, l, c = candles
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    direction_mode = cfg.get("cd_direction_mode", "both")
    zscore_enabled = cfg.get("cd_zscore_filter_enabled", False)
    sl_cooldown_ms = cfg.get("cd_sl_cooldown_seconds", 30) * 1000

    def long_ok(i):
        return direction_mode != "short_only" and (not zscore_enabled or zscore is None or zscore[i] > 0)

    def short_ok(i):
        return direction_mode != "long_only" and (not zscore_enabled or zscore is None or zscore[i] < 0)

    position = None
    trades = []
    sl_cooldown_until_ts = None

    for i in range(warmup, n):
        price = c[i]
        buy_i = buy_signal[i]
        sell_i = sell_signal[i]

        if position is not None:
            sl_price = position.get("sl_price")
            hit_sl = sl_price is not None and ((position["dir"] == "long" and l[i] <= sl_price) or (position["dir"] == "short" and h[i] >= sl_price))
            if hit_sl:
                _bt_close_trade(trades, position["dir"], position["entry"], sl_price, position["size"], i, position["entry_i"], "SL", ts=ts)
                position = None
                sl_cooldown_until_ts = ts[i] + sl_cooldown_ms

        in_sl_cooldown = sl_cooldown_until_ts is not None and ts[i] < sl_cooldown_until_ts

        if position is None:
            if in_sl_cooldown:
                continue
            if buy_i and long_ok(i):
                size = (margin * leverage) / price
                position = {"dir": "long", "entry": price, "size": size, "entry_i": i}
                _cd_bt_set_sl(position, cfg, margin, leverage)
            elif sell_i and short_ok(i):
                size = (margin * leverage) / price
                position = {"dir": "short", "entry": price, "size": size, "entry_i": i}
                _cd_bt_set_sl(position, cfg, margin, leverage)
            continue

        if position["dir"] == "long" and sell_i:
            can_flip = direction_mode != "long_only" and short_ok(i)
            reason = "CD-FLIP" if can_flip else ("CD-EXIT-DIR" if direction_mode == "long_only" else "CD-EXIT-ZSCORE")
            _bt_close_trade(trades, "long", position["entry"], price, position["size"], i, position["entry_i"], reason, ts=ts)
            if not can_flip:
                position = None
            else:
                size = (margin * leverage) / price
                position = {"dir": "short", "entry": price, "size": size, "entry_i": i}
                _cd_bt_set_sl(position, cfg, margin, leverage)
        elif position["dir"] == "short" and buy_i:
            can_flip = direction_mode != "short_only" and long_ok(i)
            reason = "CD-FLIP" if can_flip else ("CD-EXIT-DIR" if direction_mode == "short_only" else "CD-EXIT-ZSCORE")
            _bt_close_trade(trades, "short", position["entry"], price, position["size"], i, position["entry_i"], reason, ts=ts)
            if not can_flip:
                position = None
            else:
                size = (margin * leverage) / price
                position = {"dir": "long", "entry": price, "size": size, "entry_i": i}
                _cd_bt_set_sl(position, cfg, margin, leverage)

    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)

    return trades


def backtest_candle_dna(candles, cfg):
    ts, o, h, l, c = candles
    threshold = cfg.get("cd_threshold", 50)
    rejection_mult = cfg.get("cd_rejection_mult", 1.5)
    buy_signal, sell_signal, score = compute_cd_signals(o, h, l, c, rejection_mult, threshold)
    zscore = cfg.get("_cd_zscore_precomputed")
    if zscore is None and cfg.get("cd_zscore_filter_enabled", False):
        zscore = compute_rolling_zscore(c, cfg.get("cd_zscore_lookback", 20), cfg.get("cd_zscore_smooth", 3))
    return _simulate_cd_trades(candles, cfg, buy_signal, sell_signal, warmup=1, zscore=zscore)


def _simulate_pk_trades(candles, cfg, bull, bear, trend_pct, warmup):
    """Backtest-Pendant zu check_pk_signal/check_pk_sl_tp - siehe dort fuer die identische Logik
    im Live-Betrieb. trend_pct wird UNABHAENGIG von Sensitivity berechnet und beim Sweep nur
    einmal fuer alle Sensitivity-Werte wiederverwendet (wie hull_green beim UT-Bot-Sweep)."""
    ts, o, h, l, c = candles
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    direction_mode = cfg.get("pk_direction_mode", "both")
    exit_mode = cfg.get("pk_exit_mode", "flip")
    mtf_enabled = cfg.get("pk_mtf_filter_enabled", False)
    long_thr = cfg.get("pk_mtf_long_threshold", 0.5)
    short_thr = cfg.get("pk_mtf_short_threshold", -0.5)
    sl_enabled = cfg.get("pk_sl_enabled", True)
    tp_enabled = cfg.get("pk_tp_enabled", True)
    sl_usd = cfg.get("pk_sl_manual_usd", 5.0)
    tp_usd = cfg.get("pk_tp_manual_usd", 10.0)
    sl_cooldown_ms = cfg.get("pk_sl_cooldown_seconds", 30) * 1000
    trailing_enabled = cfg.get("pk_trailing_enabled", False)
    trailing_activation_pct = cfg.get("pk_trailing_activation_pct", 0.2)
    trailing_step_pct = cfg.get("pk_trailing_step_pct", 0.2)

    def long_ok(tp):
        return direction_mode != "short_only" and (not mtf_enabled or tp is None or tp > long_thr)

    def short_ok(tp):
        return direction_mode != "long_only" and (not mtf_enabled or tp is None or tp < short_thr)

    def apply_trailing(position, bar_high, bar_low):
        """Backtest-Pendant zu _pk_apply_trailing - nutzt das Hoch/Tief der Kerze (statt nur des
        Schlusskurses) als bestmoeglich erreichten Preis innerhalb der Kerze, wie bei den
        SL/TP-Treffer-Checks ueberall sonst im Backtest auch."""
        if not trailing_enabled:
            return
        direction = position["dir"]
        entry = position["entry"]
        extreme = bar_high if direction == "long" else bar_low
        best = position.get("trail_best")
        best = extreme if best is None else (max(best, extreme) if direction == "long" else min(best, extreme))
        position["trail_best"] = best
        profit_pct = ((best - entry) / entry * 100) if direction == "long" else ((entry - best) / entry * 100)

        if not position.get("trail_active") and profit_pct >= trailing_activation_pct:
            position["trail_active"] = True
            current_sl = position.get("sl_price")
            if direction == "long":
                position["sl_price"] = entry if current_sl is None else max(current_sl, entry)
            else:
                position["sl_price"] = entry if current_sl is None else min(current_sl, entry)

        if position.get("trail_active"):
            trail_sl = best * (1 - trailing_step_pct / 100) if direction == "long" else best * (1 + trailing_step_pct / 100)
            current_sl = position.get("sl_price")
            if direction == "long":
                position["sl_price"] = trail_sl if current_sl is None else max(current_sl, trail_sl)
            else:
                position["sl_price"] = trail_sl if current_sl is None else min(current_sl, trail_sl)

    position = None
    trades = []
    sl_cooldown_until_ts = None

    for i in range(warmup, n):
        price = c[i]
        tpct = trend_pct[i]

        if position is not None:
            # Trailing greift IMMER (wenn aktiviert), unabhaengig vom Exit-Modus - im Modus
            # "Wechsel" unterbricht ein Trailing-Treffer das "immer im Markt"-Prinzip NUR in
            # diesem Fall (genau wie der optionale SL bei UT-Bot + Hull das dort auch tut).
            # Im Modus "fixed_tp_sl" kommt zusaetzlich noch der normale feste SL/TP dazu.
            apply_trailing(position, h[i], l[i])
            sl_price = position.get("sl_price")
            tp_price = position.get("tp_price")
            hit_sl = sl_price is not None and ((position["dir"] == "long" and l[i] <= sl_price) or (position["dir"] == "short" and h[i] >= sl_price))
            hit_tp = tp_price is not None and ((position["dir"] == "long" and h[i] >= tp_price) or (position["dir"] == "short" and l[i] <= tp_price))
            if hit_sl:
                reason = "TRAIL-SL" if position.get("trail_active") else "SL"
                _bt_close_trade(trades, position["dir"], position["entry"], sl_price, position["size"], i, position["entry_i"], reason, ts=ts)
                position = None
                sl_cooldown_until_ts = ts[i] + sl_cooldown_ms
            elif hit_tp:
                _bt_close_trade(trades, position["dir"], position["entry"], tp_price, position["size"], i, position["entry_i"], "TP", ts=ts)
                position = None

        in_cooldown = sl_cooldown_until_ts is not None and ts[i] < sl_cooldown_until_ts

        if position is None:
            if in_cooldown:
                continue
            if bull[i] and long_ok(tpct):
                size = (margin * leverage) / price
                position = {"dir": "long", "entry": price, "size": size, "entry_i": i, "sl_price": None, "tp_price": None, "trail_active": False, "trail_best": price}
                if exit_mode == "fixed_tp_sl":
                    if sl_enabled:
                        position["sl_price"] = price - sl_usd / size
                    if tp_enabled:
                        position["tp_price"] = price + tp_usd / size
            elif bear[i] and short_ok(tpct):
                size = (margin * leverage) / price
                position = {"dir": "short", "entry": price, "size": size, "entry_i": i, "sl_price": None, "tp_price": None, "trail_active": False, "trail_best": price}
                if exit_mode == "fixed_tp_sl":
                    if sl_enabled:
                        position["sl_price"] = price + sl_usd / size
                    if tp_enabled:
                        position["tp_price"] = price - tp_usd / size
            continue

        if exit_mode != "flip":
            continue  # 'fixed_tp_sl': nur der Block oben (SL/TP) darf schliessen

        if position["dir"] == "long" and bear[i]:
            if direction_mode == "long_only" or not short_ok(tpct):
                _bt_close_trade(trades, "long", position["entry"], price, position["size"], i, position["entry_i"], "PK-EXIT", ts=ts)
                position = None
            else:
                _bt_close_trade(trades, "long", position["entry"], price, position["size"], i, position["entry_i"], "PK-FLIP", ts=ts)
                size = (margin * leverage) / price
                position = {"dir": "short", "entry": price, "size": size, "entry_i": i, "sl_price": None, "tp_price": None}
        elif position["dir"] == "short" and bull[i]:
            if direction_mode == "short_only" or not long_ok(tpct):
                _bt_close_trade(trades, "short", position["entry"], price, position["size"], i, position["entry_i"], "PK-EXIT", ts=ts)
                position = None
            else:
                _bt_close_trade(trades, "short", position["entry"], price, position["size"], i, position["entry_i"], "PK-FLIP", ts=ts)
                size = (margin * leverage) / price
                position = {"dir": "long", "entry": price, "size": size, "entry_i": i, "sl_price": None, "tp_price": None}

    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)

    return trades


async def _build_zscore_series_for_backtest(symbol, cfg, days, base_ts, base_c, primary_resolution, prefix):
    """Baut den Z-Score-Filter-Wert fuer eine ABWEICHENDE Zeiteinheit (<prefix>_zscore_resolution)
    fuer den Backtest: holt die historischen Kerzen dieser Zeiteinheit, berechnet den Z-Score
    darauf, bildet ihn per Forward-Fill (siehe _align_htf_series) auf die Zeitstempel der
    Einstiegs-Kerzen ab - kein Blick in die Zukunft. Bei 'same'/eigenem Handels-Zeitrahmen wird
    einfach direkt auf den eigenen Kerzen gerechnet. Gibt (zscore, error) zurueck."""
    resolution = cfg.get(f"{prefix}_zscore_resolution", "same")
    lookback = cfg.get(f"{prefix}_zscore_lookback", 20)
    smooth = cfg.get(f"{prefix}_zscore_smooth", 3)
    if resolution in (None, "", "same") or resolution == primary_resolution:
        return compute_rolling_zscore(base_c, lookback, smooth), None
    tf_candles, err, _ = await _fetch_cached_backtest_candles(symbol, resolution, days, 20_000, market_type=cfg.get("binance_market_type", "spot"))
    if err:
        return None, f"Z-Score-Zeiteinheit ({resolution}): {err}"
    if not tf_candles or len(tf_candles[4]) < 20:
        return None, f"Zu wenig historische Kerzen für die Z-Score-Zeiteinheit ({resolution}) erhalten."
    t_ts, t_o, t_h, t_l, t_c = tf_candles
    zscore_htf = compute_rolling_zscore(t_c, lookback, smooth)
    return _align_htf_series(base_ts, t_ts, zscore_htf), None


async def _pk_build_mtf_trend_pct(symbol, cfg, days, base_ts, base_h, base_l, base_c, primary_resolution, prefix="pk"):
    """Baut den Trend%-Filter-Wert aus bis zu 3 waehlbaren Zeiteinheiten (<prefix>_mtf_tf1/2/3) -
    wie 'Block 1' im Original-Pine-Indikator (avgB1 = Durchschnitt aus 3 TFs). Generisch ueber
    'prefix' fuer mehrere Strategien nutzbar (Pieki Algo: prefix='pk', UT-Bot+Hull: prefix='utb').
    Inaktive TFs ("off") werden ausgelassen, sind ALLE aus wird auf den eigenen Handels-Zeitrahmen
    zurueckgefallen. Jede aktive TF-Serie wird per Forward-Fill (siehe _align_htf_series) auf die
    Zeitstempel der Einstiegs-Kerzen (base_ts) abgebildet, dann elementweise gemittelt. Gibt
    (trend_pct, error) zurueck."""
    mtf_fast = cfg.get(f"{prefix}_mtf_fast_len", 5)
    mtf_slow = cfg.get(f"{prefix}_mtf_slow_len", 9)
    mtf_atr = cfg.get(f"{prefix}_mtf_atr_len", 14)
    tf_list = [cfg.get(f"{prefix}_mtf_tf{i}", "off") for i in (1, 2, 3)]
    active_tfs = [tf for tf in tf_list if tf not in (None, "", "off")]
    if not active_tfs:
        return compute_pk_trend_percent(base_h, base_l, base_c, mtf_fast, mtf_slow, mtf_atr), None

    aligned_series = []
    for tf in active_tfs:
        if tf == primary_resolution:
            aligned_series.append(compute_pk_trend_percent(base_h, base_l, base_c, mtf_fast, mtf_slow, mtf_atr))
            continue
        tf_candles, err, _ = await _fetch_cached_backtest_candles(symbol, tf, days, 20_000, market_type=cfg.get("binance_market_type", "spot"))
        if err:
            return None, f"MTF-Zeiteinheit ({tf}): {err}"
        if not tf_candles or len(tf_candles[4]) < 20:
            return None, f"Zu wenig historische Kerzen für die MTF-Zeiteinheit ({tf}) erhalten."
        t_ts, t_o, t_h, t_l, t_c = tf_candles
        trend_htf = compute_pk_trend_percent(t_h, t_l, t_c, mtf_fast, mtf_slow, mtf_atr)
        aligned_series.append(_align_htf_series(base_ts, t_ts, trend_htf))

    n = len(base_ts)
    trend_pct = [sum(s[i] for s in aligned_series) / len(aligned_series) for i in range(n)]
    return trend_pct, None


def backtest_peki_algo(candles, cfg):
    ts, o, h, l, c = candles
    sensitivity = cfg.get("pk_sensitivity", 3.0)
    atr_period = cfg.get("pk_atr_period", 11)
    sma_period = cfg.get("pk_sma_period", 13)
    bull, bear, st_line, sma = compute_pk_signals(h, l, c, sensitivity, atr_period, sma_period)
    trend_pct = cfg.get("_pk_trend_pct_precomputed")  # von run_backtest vorab async berechnet
    # (siehe _pk_build_mtf_trend_pct) - diese Funktion selbst ist NICHT async (einheitliche
    # BACKTEST_FUNCS-Signatur (candles, cfg))
    if trend_pct is None:
        mtf_fast = cfg.get("pk_mtf_fast_len", 5)
        mtf_slow = cfg.get("pk_mtf_slow_len", 9)
        mtf_atr = cfg.get("pk_mtf_atr_len", 14)
        trend_pct = compute_pk_trend_percent(h, l, c, mtf_fast, mtf_slow, mtf_atr)
    warmup = max(atr_period, sma_period, cfg.get("pk_mtf_slow_len", 9), cfg.get("pk_mtf_atr_len", 14), 5) + 2
    return _simulate_pk_trades(candles, cfg, bull, bear, trend_pct, warmup)


PK_SWEEP_MAX_COMBOS = 2000
PK_SWEEP_MIN_RELIABLE_TRADES = 5


async def run_pk_sensitivity_sweep(symbol, cfg, days, sens_min, sens_max, sens_step):
    """'Monte-Carlo'-Parametersweep fuer Pieki Algo, NUR ueber die Sensitivity (2 Nachkommastellen,
    Schritt 0.01 wie im Original-Pine-Script). trend_pct haengt nicht von der Sensitivity ab und
    wird deshalb nur EINMAL berechnet (ueber bis zu 3 gemittelte Zeiteinheiten, siehe
    _pk_build_mtf_trend_pct) und fuer alle Sensitivity-Werte wiederverwendet."""
    max_candles = BACKTEST_MAX_CANDLES["pieki_algo"]
    resolution = cfg.get("pk_resolution", "5m")
    candles, err, cache_used = await _fetch_cached_backtest_candles(symbol, resolution, days, max_candles, market_type=cfg.get("binance_market_type", "spot"))
    if err:
        return {"error": err}
    if not candles or len(candles[4]) < 150:
        return {"error": "Zu wenig historische Kerzen für einen aussagekräftigen Sweep erhalten."}

    steps = int(round((sens_max - sens_min) / max(sens_step, 1e-9)))
    sens_values = sorted(set(round(sens_min + i * sens_step, 2) for i in range(steps + 1)
                              if sens_min + i * sens_step <= sens_max + 1e-9))
    sens_values = [v for v in sens_values if v > 0]

    if len(sens_values) == 0:
        return {"error": "Der eingestellte Bereich ergibt keine gültigen Werte."}
    if len(sens_values) > PK_SWEEP_MAX_COMBOS:
        return {"error": f"Zu viele Werte ({len(sens_values)}, Limit {PK_SWEEP_MAX_COMBOS}) - Bereich oder Schrittweite vergrößern."}

    o, h, l, c = candles[1], candles[2], candles[3], candles[4]
    atr_period = cfg.get("pk_atr_period", 11)
    sma_period = cfg.get("pk_sma_period", 13)
    mtf_slow = cfg.get("pk_mtf_slow_len", 9)
    mtf_atr = cfg.get("pk_mtf_atr_len", 14)
    trend_pct, mtf_err = await _pk_build_mtf_trend_pct(symbol, cfg, days, candles[0], h, l, c, resolution)
    if mtf_err:
        return {"error": mtf_err}
    warmup = max(atr_period, sma_period, mtf_slow, mtf_atr, 5) + 2

    results = []
    for sens in sens_values:
        bull, bear, st_line, sma = compute_pk_signals(h, l, c, sens, atr_period, sma_period)
        trades = _simulate_pk_trades(candles, cfg, bull, bear, trend_pct, warmup)
        stats = summarize_backtest_trades(trades)
        results.append({"pk_sensitivity": sens, **stats})

    best_sorted = sorted(results, key=lambda r: (r["trades"] >= PK_SWEEP_MIN_RELIABLE_TRADES, r["total_pnl_usd"]), reverse=True)
    worst_sorted = sorted(results, key=lambda r: r["total_pnl_usd"])


    actual_days = (candles[0][-1] - candles[0][0]) / (24 * 60 * 60 * 1000)
    return {
        "symbol": symbol, "resolution": resolution, "requested_days": days,
        "actual_days_covered": round(actual_days, 1), "candles_processed": len(c),
        "min_reliable_trades": PK_SWEEP_MIN_RELIABLE_TRADES,
        "combos_tested": len(sens_values),
        "results": best_sorted[:30],
        "worst_results": worst_sorted[:20],
    }


BACKTEST_MAX_CANDLES = {
    "fib_reversal": 100_000,
    "halftrend": 100_000,
    "diamond_algo": 100_000,
    "elte_smart": 100_000,
    "candle_patterns": 100_000,
    "mo7_scalp": 100_000,
    "ut_bot_hull": 100_000,
    "wavetrend_cross": 100_000,
    "pieki_algo": 100_000,
    "fractals_flip": 100_000,
    "candle_dna": 100_000,
}

BACKTEST_FUNCS = {
    "fib_reversal": backtest_fib_reversal,
    "halftrend": backtest_halftrend,
    "diamond_algo": backtest_diamond_algo,
    "elte_smart": backtest_elte_smart,
    "candle_patterns": backtest_candle_patterns,
    "ut_bot_hull": backtest_ut_bot_hull,
    "wavetrend_cross": backtest_wavetrend_cross,
    "pieki_algo": backtest_peki_algo,
    "fractals_flip": backtest_fractals_flip,
    "candle_dna": backtest_candle_dna,
    # "mo7_scalp" bewusst NICHT hier drin - braucht eine 6er-Tupel-Kerzenquelle MIT Volumen
    # (MFI-Baustein), deshalb in run_backtest() als Sonderfall behandelt statt ueber diesen
    # generischen 5er-Tupel-Dispatch.
}


async def run_backtest(symbol, entry_mode, cfg, days, exclude_top_n=1):
    if entry_mode == "mo7_scalp":
        max_candles = BACKTEST_MAX_CANDLES["mo7_scalp"]
        resolution = cfg.get("mo7_resolution", "5m")
        candles, err = await _fetch_cached_mo7_backtest_candles(symbol, resolution, days, max_candles, market_type=cfg.get("binance_market_type", "spot"))
        if err:
            return {"error": err}
        if not candles or len(candles[4]) < 550:
            return {"error": "Zu wenig historische Kerzen für einen aussagekräftigen Backtest erhalten (mind. ~550 für das 500er-Normierungsfenster nötig)."}
        n_candles = len(candles[4])
        trades = backtest_mo7(candles, cfg)
        stats = summarize_backtest_trades(trades, exclude_top_n)
        stats_long = summarize_backtest_trades([t for t in trades if t["dir"] == "long"], exclude_top_n)
        stats_short = summarize_backtest_trades([t for t in trades if t["dir"] == "short"], exclude_top_n)
        actual_days = (candles[0][-1] - candles[0][0]) / (24 * 60 * 60 * 1000)
        return {
            "symbol": symbol, "entry_mode": entry_mode, "resolution": resolution,
            "requested_days": days, "actual_days_covered": round(actual_days, 1),
            "candles_processed": n_candles, "candle_cap": max_candles, "cache_used": False,
            "stats": stats, "stats_long": stats_long, "stats_short": stats_short,
            "trades": trades[-50:],
        }

    if entry_mode not in BACKTEST_FUNCS:
        return {"error": f"Backtest für '{entry_mode}' nicht unterstützt (nur fib_reversal, halftrend, diamond_algo, elte_smart, candle_patterns, mo7_scalp, ut_bot_hull, wavetrend_cross, pieki_algo, fractals_flip, candle_dna - Grid/OBI-Scalp/OBI-Momentum-Scalp brauchen historische Tick-/Orderbuchdaten, die es nicht gibt)."}

    max_candles = BACKTEST_MAX_CANDLES[entry_mode]

    resolution_key = {"fib_reversal": "fib_resolution", "halftrend": "ht_resolution", "diamond_algo": "da_resolution", "elte_smart": "es_resolution", "candle_patterns": "cp_resolution", "ut_bot_hull": "utb_resolution", "wavetrend_cross": "wtc_resolution", "pieki_algo": "pk_resolution", "fractals_flip": "fr_resolution", "candle_dna": "cd_resolution"}[entry_mode]
    resolution = cfg.get(resolution_key, "1m")
    if resolution in SUB_MINUTE_RESOLUTIONS:
        # 10s/15s/30s-Kerzen kommen aus 1s-Basisdaten (10-30x mehr Rohdaten je Zeitraum) -
        # Obergrenze bewusst strenger, sonst waeren das bei laengeren Zeitraeumen zu viele
        # Binance-Anfragen.
        max_candles = min(max_candles, 5000)

    cache_key = (symbol, resolution, cfg.get("binance_market_type", "spot"))
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
        candles, err = await fetch_historical_candles_binance(symbol, resolution, days, max_candles, market_type=cfg.get("binance_market_type", "spot"))
        if candles:
            _backtest_cache_set(cache_key, {"fetched_at": now, "days": days, "max_candles": max_candles, "candles": candles})

    if err:
        return {"error": err}
    if not candles or len(candles[4]) < 100:
        return {"error": "Zu wenig historische Kerzen für einen aussagekräftigen Backtest erhalten."}

    n_candles = len(candles[4])
    if entry_mode == "pieki_algo":
        trend_pct, mtf_err = await _pk_build_mtf_trend_pct(symbol, cfg, days, candles[0], candles[2], candles[3], candles[4], resolution)
        if mtf_err:
            return {"error": mtf_err}
        cfg = dict(cfg)  # eigene Kopie - der private Cache-Key darf nicht in die Aufrufer-Config
        cfg["_pk_trend_pct_precomputed"] = trend_pct  # von backtest_peki_algo gelesen (nicht async, siehe dort)
    elif entry_mode == "ut_bot_hull" and cfg.get("utb_mtf_filter_enabled", False):
        trend_pct, mtf_err = await _pk_build_mtf_trend_pct(symbol, cfg, days, candles[0], candles[2], candles[3], candles[4], resolution, prefix="utb")
        if mtf_err:
            return {"error": mtf_err}
        cfg = dict(cfg)
        cfg["_utb_trend_pct_precomputed"] = trend_pct  # von backtest_ut_bot_hull gelesen (nicht async, siehe dort)
    elif entry_mode == "fractals_flip" and cfg.get("fr_zscore_filter_enabled", False):
        zscore, zs_err = await _build_zscore_series_for_backtest(symbol, cfg, days, candles[0], candles[4], resolution, "fr")
        if zs_err:
            return {"error": zs_err}
        cfg = dict(cfg)
        cfg["_fr_zscore_precomputed"] = zscore  # von backtest_fractals_flip gelesen (nicht async, siehe dort)
    elif entry_mode == "candle_dna" and cfg.get("cd_zscore_filter_enabled", False):
        zscore, zs_err = await _build_zscore_series_for_backtest(symbol, cfg, days, candles[0], candles[4], resolution, "cd")
        if zs_err:
            return {"error": zs_err}
        cfg = dict(cfg)
        cfg["_cd_zscore_precomputed"] = zscore  # von backtest_candle_dna gelesen (nicht async, siehe dort)
    backtest_fn = BACKTEST_FUNCS[entry_mode]
    trades = backtest_fn(candles, cfg)
    stats = summarize_backtest_trades(trades, exclude_top_n)
    stats_long = summarize_backtest_trades([t for t in trades if t["dir"] == "long"], exclude_top_n)
    stats_short = summarize_backtest_trades([t for t in trades if t["dir"] == "short"], exclude_top_n)

    actual_days = (candles[0][-1] - candles[0][0]) / (24 * 60 * 60 * 1000)
    return {
        "symbol": symbol, "entry_mode": entry_mode, "resolution": resolution,
        "requested_days": days, "actual_days_covered": round(actual_days, 1),
        "candles_processed": n_candles, "candle_cap": max_candles, "cache_used": cache_used,
        "stats": stats, "stats_long": stats_long, "stats_short": stats_short,
        "trades": trades[-50:],  # letzte 50 fuers Dashboard, nicht alle
    }

