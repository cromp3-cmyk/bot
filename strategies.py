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
from collections import deque, OrderedDict

from bot_core import (
    debug_log, WS_URL, SYMBOLS, MARKET_INDICES, MARKET_INDEX_TO_SYMBOL,
    BOTS, execute_entry, execute_exit, execute_partial_exit, compute_step_abs, compute_step_abs_g2, GLOBAL_SETTINGS,
)
import binance_ws  # WebSocket-Kerzen-Cache - reduziert REST-Traffic gegen Binance drastisch,
# siehe fetch_candles_binance()/fetch_candles_binance_vol() weiter unten

BINANCE_SYMBOL_MAP = {
    "BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT", "DOGE": "DOGEUSDT", "XRP": "XRPUSDT",
    "LINK": "LINKUSDT", "AVAX": "AVAXUSDT", "NEAR": "NEARUSDT", "DOT": "DOTUSDT", "TON": "TONUSDT",
    "SUI": "SUIUSDT", "BNB": "BNBUSDT", "UNI": "UNIUSDT", "APT": "APTUSDT", "ADA": "ADAUSDT",
    "TRX": "TRXUSDT", "LTC": "LTCUSDT", "BCH": "BCHUSDT", "HBAR": "HBARUSDT", "ICP": "ICPUSDT",
    "XAU": "XAUUSDT", "XAG": "XAGUSDT",  # Seit Jan. 2026 auf Binance, aber NUR als USDT-Perpetual-
    # Future ("TradFi"-Kategorie) - es gibt dafuer KEIN Spot-Paar, siehe BINANCE_FUTURES_ONLY_SYMBOLS
    "LIT": "LITUSDT",  # Seit 23.12.2025 auf Binance Futures (Pre-Market-Start), ebenfalls nur als
    # USDT-Perpetual, kein Spot-Paar - siehe BINANCE_FUTURES_ONLY_SYMBOLS
    # HYPE und WTI/Forex (EURUSD, ...) gibt es weiterhin nicht auf Binance - dafuer greift der Lighter-Fallback
}

BINANCE_FUTURES_ONLY_SYMBOLS = {"XAU", "XAG", "LIT"}  # existieren auf Binance NUR als Futures, kein Spot-Paar


# Globale Anfragen-Drossel: OHNE das feuern alle 7 Poll-Loops (binance_1s, da, es, ht,
# scalp_board, quad_stoch, oms_rsi) x alle aktiven Coins IM SELBEN MOMENT, weil sie alle exakt
# 5 Sekunden schlafen und beim Bot-Start fast gleichzeitig gestartet sind - das bleibt fuer immer
# synchron (klassisches "Thundering Herd"-Muster) und riss in der Praxis das Rate-Limit, obwohl
# die DURCHSCHNITTLICHE Anfragenrate eigentlich im gruenen Bereich gewesen waere. Diese Drossel
# verteilt alle Binance-Anfragen (ueber alle Coins/Loops hinweg) gleichmaessig statt in Buendeln.
_binance_last_request_ts = 0.0
_binance_throttle_lock = None  # wird beim ersten Gebrauch lazy angelegt (braucht einen laufenden Event-Loop)
BINANCE_MIN_REQUEST_INTERVAL = 0.15  # Sekunden zwischen zwei Binance-Anfragen = max. ~6.7 Anfragen/Sekunde global

# Gewichts-bewusste Drossel: Binance zaehlt pro IP ein "Gewicht" pro Minute (Spot 6000, Futures 2400) -
# NICHT die Anfragenzahl. Eine Klines-Anfrage mit limit=1000 kostet bei Futures 5 Gewicht (Spot 2), die
# reine 0,15-s-Drossel oben kann also trotz "nur" ~400 Anfragen/Min das Futures-Limit reissen. Binance
# meldet in JEDER Antwort den aktuellen IP-weiten Verbrauch (Header X-MBX-USED-WEIGHT-1M) - inklusive dem
# anderer Programme auf derselben (z.B. geteilten Render-)IP. Ab BINANCE_WEIGHT_SOFT_FRACTION des Limits
# werden Anfragen stufenlos gebremst (0,5 s bis 5 s Zusatzpause), lange bevor es ein 429/418 (IP-Bann) gibt.
BINANCE_WEIGHT_LIMIT_1M = {"spot": 6000, "futures": 2400}
BINANCE_WEIGHT_SOFT_FRACTION = 0.6
_binance_used_weight = {"spot": (0, 0.0), "futures": (0, 0.0)}  # (Gewicht laut letzter Antwort, Zeitpunkt)
_binance_req_counts = {}  # (market_type, kind) -> Anzahl seit dem letzten Statistik-Log
_binance_stats_last_log = time.time()
_binance_slowdown_logged_at = 0.0
BINANCE_STATS_LOG_INTERVAL = 300


def _binance_note_response(market_type, resp):
    """Merkt sich den von Binance gemeldeten IP-Gewichtsverbrauch der letzten Minute."""
    try:
        raw = resp.headers.get("X-MBX-USED-WEIGHT-1M")
        if raw is not None:
            _binance_used_weight[market_type] = (int(raw), time.time())
    except Exception:
        pass


def _binance_weight_delay(market_type):
    """Zusatzpause in Sekunden je nach zuletzt gemeldetem Gewichtsverbrauch (0 unter der Schwelle).

    WICHTIG (Bugfix): ts==0.0 heisst "in diesem Prozess noch NIE eine echte Weight-Antwort von
    Binance bekommen" (direkt nach jedem Deploy/Neustart der Fall, da _binance_used_weight nur ein
    In-Memory-Dict ist). Binance selbst fuehrt das IP-Gewicht aber UEBER Neustarts hinweg weiter -
    war es kurz vor dem Neustart schon hoch (z.B. durch viele aktive Filter/Coins), ist die Bremse
    hier direkt nach dem Start "blind" und liess bisher ungebremst (0s Zusatzpause) einen Schwung
    Seed-Anfragen fuer alle Coins/Streams raus, BEVOR die erste echte Antwort zurueckkam - live
    beobachtet: IP-Bann (HTTP 418, futures) Sekunden nach einem Redeploy. Fix: ohne echten Messwert
    lieber vorsichtig bremsen (wie ein mittlerer Auslastungsgrad) statt "kein Messwert" faelschlich
    als "safe" zu werten - sobald die erste echte Antwort da ist, greift die praezise Regel unten."""
    weight, ts = _binance_used_weight.get(market_type, (0, 0.0))
    if ts == 0.0:
        return 1.0
    if time.time() - ts >= 60:
        return 0.0  # Messwert aelter als das 1-Minuten-Fenster - gilt als zurueckgesetzt
    limit = BINANCE_WEIGHT_LIMIT_1M.get(market_type, 6000)
    frac = weight / limit
    if frac < BINANCE_WEIGHT_SOFT_FRACTION:
        return 0.0
    return 0.5 + min(1.0, (frac - BINANCE_WEIGHT_SOFT_FRACTION) / (1 - BINANCE_WEIGHT_SOFT_FRACTION)) * 4.5


def _binance_log_stats_if_due():
    """Alle 5 Minuten eine Zeile: wie viele Anfragen kamen von WEM (Art:Intervall) und wie hoch war der
    IP-Gewichtsverbrauch - so sieht man im Log, WOHER der Traffic kommt (oder dass er gar nicht von uns ist)."""
    global _binance_stats_last_log
    now = time.time()
    if now - _binance_stats_last_log < BINANCE_STATS_LOG_INTERVAL or not _binance_req_counts:
        return
    minutes = (now - _binance_stats_last_log) / 60
    _binance_stats_last_log = now
    parts = []
    for mt in ("spot", "futures"):
        items = sorted(((k[1], n) for k, n in _binance_req_counts.items() if k[0] == mt), key=lambda x: -x[1])
        if not items:
            continue
        total = sum(n for _, n in items)
        weight, ts = _binance_used_weight.get(mt, (0, 0.0))
        top = ", ".join(f"{kind}:{n}" for kind, n in items[:6])
        parts.append(f"{mt} {total} Anfragen (~{round(total / minutes, 1)}/Min; {top}), zuletzt gemeldetes IP-Gewicht {weight}/{BINANCE_WEIGHT_LIMIT_1M[mt]} pro Min")
    _binance_req_counts.clear()
    debug_log("📊 [Binance-REST] letzte " + str(round(minutes)) + " Min: " + " | ".join(parts))


async def _binance_throttle(market_type="spot", kind="klines"):
    global _binance_last_request_ts, _binance_throttle_lock, _binance_slowdown_logged_at
    if _binance_throttle_lock is None:
        _binance_throttle_lock = asyncio.Lock()
    async with _binance_throttle_lock:
        now = time.time()
        wait = BINANCE_MIN_REQUEST_INTERVAL - (now - _binance_last_request_ts)
        extra = _binance_weight_delay(market_type)
        if extra > 0:
            wait = max(wait, extra)
            if now - _binance_slowdown_logged_at > 60:
                _binance_slowdown_logged_at = now
                weight, _ts = _binance_used_weight.get(market_type, (0, 0.0))
                debug_log(f"🐢 [Binance-{market_type}] IP-Gewicht {weight}/{BINANCE_WEIGHT_LIMIT_1M.get(market_type, 6000)} pro Min - "
                          f"bremse Anfragen ({round(extra, 1)}s Pause), um einen Bann zu vermeiden")
        if wait > 0:
            await asyncio.sleep(wait)
        _binance_last_request_ts = time.time()
        key = (market_type, kind)
        _binance_req_counts[key] = _binance_req_counts.get(key, 0) + 1
        _binance_log_stats_if_due()


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

        # NEU: zuerst den WebSocket-Cache versuchen - liefert er etwas, entfaellt die
        # komplette REST-Anfrage darunter. ensure_subscribed() merkt den Stream fuer den
        # WS-Manager vor (no-op, falls schon bekannt); solange der Stream noch nicht warm
        # ist, liefert get_cached_candles() None und der bisherige REST-Weg greift wie vorher.
        binance_ws.ensure_subscribed(effective_market_type, pair, resolution)
        cached = binance_ws.get_cached_candles(effective_market_type, pair, resolution, count_back)
        if cached is not None:
            ts, o, h, l, c, _v = cached
            if ts:
                return ts, o, h, l, c

        if _binance_is_banned(effective_market_type):
            return None  # aktiver Bann - keine Anfrage stellen, das wuerde ihn nur verlaengern

        base_url = BINANCE_BASE_URLS.get(effective_market_type, BINANCE_BASE_URLS["spot"])
        url = f"{base_url}?symbol={pair}&interval={resolution}&limit={min(count_back, 1000)}"
        await _binance_throttle(effective_market_type, f"live:{resolution}")
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                _binance_note_response(effective_market_type, resp)
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


_g2_smart_direction_cache = {}  # symbol -> {"direction": ..., "fetched_at": ...}


async def get_smart_direction_g2(symbol):
    """Fuer Grid 2's 'Smart'-Richtungsmodus: schaut sich die 24h-Preisaenderung auf Binance an
    und schlaegt eine Richtung vor (Momentum-Annahme: laeuft der Preis, geht der Trend weiter).
    Wird 5 Minuten gecacht, um nicht bei jedem Preis-Tick neu abzufragen. Nutzt bewusst dieselbe
    Drossel-/Bann-Infrastruktur wie die uebrigen Binance-Anfragen (_binance_throttle/_is_banned/
    _register_ban), damit diese zusaetzliche Anfrage nicht am bestehenden Rate-Limit-Schutz
    vorbeigeht."""
    cached = _g2_smart_direction_cache.get(symbol)
    if cached and time.time() - cached["fetched_at"] < 300:
        return cached["direction"]

    pair = BINANCE_SYMBOL_MAP.get(symbol)
    direction = None
    if pair:
        effective_market_type = "futures" if symbol in BINANCE_FUTURES_ONLY_SYMBOLS else "spot"
        if not _binance_is_banned(effective_market_type):
            try:
                base = "https://fapi.binance.com/fapi/v1/ticker/24hr" if effective_market_type == "futures" \
                    else "https://api.binance.com/api/v3/ticker/24hr"
                await _binance_throttle(effective_market_type, "ticker")
                url = f"{base}?symbol={pair}"
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        _binance_note_response(effective_market_type, resp)
                        if resp.status in (418, 429):
                            body = await resp.text()
                            _binance_register_ban(effective_market_type, symbol, resp.status, body)
                        elif resp.status == 200:
                            data = await resp.json()
                            change_pct = float(data.get("priceChangePercent", 0))
                            direction = "long" if change_pct >= 0 else "short"
            except Exception as e:
                debug_log(f"⚠️ [{symbol}] Grid-2 Smart-Mode 24h-Abfrage fehlgeschlagen", {"error": str(e)})

    _g2_smart_direction_cache[symbol] = {"direction": direction, "fetched_at": time.time()}
    return direction


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
                await _binance_throttle(effective_market_type, f"history:{base_resolution}")
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    _binance_note_response(effective_market_type, resp)
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
    (braucht MFI) und VWAP-Deviation (SuperTrend+RSI). Unterstuetzt wie die normale Variante
    auch synthetische Aufloesungen (2m, eigene Minuten, 10s/15s/30s/45s - siehe
    resolve_synthetic_resolution), das Volumen wird beim Zusammenfassen pro Bucket aufsummiert
    (siehe resample_candles_with_volume/_resample_seconds_candles_with_volume)."""
    pair = BINANCE_SYMBOL_MAP.get(symbol)
    if not pair:
        return None, "Coin nicht auf Binance verfügbar"

    synth = resolve_synthetic_resolution(resolution)
    base_resolution = synth[0] if synth else resolution
    fetch_factor = synth[1] if synth else 1
    total_ms = days * 24 * 60 * 60 * 1000
    end_time = int(time.time() * 1000)
    start_time = end_time - total_ms
    hard_candle_cap = max_candles * fetch_factor + 2000
    effective_market_type = "spot" if base_resolution == "1s" and market_type == "futures" else market_type
    if symbol in BINANCE_FUTURES_ONLY_SYMBOLS:
        effective_market_type = "futures"
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
                    wait_s = max(0, (_binance_ban_until_ms.get(effective_market_type, 0.0) - time.time() * 1000) / 1000)
                    return None, f"Binance-IP-Bann aktiv, noch ca. {round(wait_s)}s - bitte warten und erneut versuchen."
                url = f"{base_url}?symbol={pair}&interval={base_resolution}&limit=1000&endTime={cursor}"
                await _binance_throttle(effective_market_type, f"history-vol:{base_resolution}")
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    _binance_note_response(effective_market_type, resp)
                    if resp.status in (418, 429):
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

    if synth:
        if base_resolution == "1s":
            timestamps, opens, highs, lows, closes, volumes = _resample_seconds_candles_with_volume((timestamps, opens, highs, lows, closes, volumes), synth[1])
        else:
            timestamps, opens, highs, lows, closes, volumes = resample_candles_with_volume((timestamps, opens, highs, lows, closes, volumes), synth[1])

    if len(closes) > max_candles:
        timestamps, opens, highs, lows, closes, volumes = (
            timestamps[-max_candles:], opens[-max_candles:], highs[-max_candles:],
            lows[-max_candles:], closes[-max_candles:], volumes[-max_candles:])

    return (timestamps, opens, highs, lows, closes, volumes), None


async def fetch_candles_binance_vol(symbol, resolution, count_back=150):
    """Wie fetch_candles_binance, liefert zusaetzlich das Handelsvolumen pro Kerze -
    fuer Strategien wie BLSH-Composite, die Volumen brauchen (z.B. MFI), und fuer
    VWAP-Deviation (SuperTrend+RSI). Unterstuetzt wie die Backtest-Variante auch
    synthetische Aufloesungen (2m, eigene Minuten, 10s/15s/30s/45s - siehe
    resolve_synthetic_resolution): der WS-Cache deckt nur native Intervalle ab (siehe
    binance_ws.CACHEABLE_INTERVALS), fuer alles andere wird die Basis-Aufloesung per REST
    geholt und das Volumen beim Zusammenfassen pro Bucket aufsummiert.

    Markt: Spot - AUSSER bei Coins, die es auf Binance nur als Futures gibt (BINANCE_FUTURES_ONLY_SYMBOLS:
    LIT, XAU, XAG). Vorher ging auch dafuer die Spot-Anfrage raus: fuer LIT lieferte sie die Kerzen eines
    laengst delisteten alten LIT-Spot-Paars (letzte Kerze ~587 Tage alt) - die Strategie-Loops
    ueberspringen so etwas zwar (Veraltet-Pruefung), bekamen aber nie gueltige Daten, und der WS-Cache
    hat den toten Stream alle paar Minuten neu geseedet."""
    pair = BINANCE_SYMBOL_MAP.get(symbol)
    if not pair:
        return None

    mt = "futures" if symbol in BINANCE_FUTURES_ONLY_SYMBOLS else "spot"
    synth = resolve_synthetic_resolution(resolution)
    if mt == "futures" and synth and synth[0] == "1s":
        return None  # Binance-Futures bietet keine 1s-Kerzen an - Sekunden-Zeitrahmen gehen hier nicht

    if not synth:
        binance_ws.ensure_subscribed(mt, pair, resolution)
        cached = binance_ws.get_cached_candles(mt, pair, resolution, count_back)
        if cached is not None:
            ts, o, h, l, c, v = cached
            if ts:
                return ts, o, h, l, c, v

    base_resolution, factor = synth if synth else (resolution, 1)
    # WICHTIG: 'fetch_limit' ist die Basis-Kerzen-Anzahl, die noetig waere, um 'count_back'
    # FERTIGE synthetische Kerzen zu bekommen (z.B. 30s aus 1s: factor=30, also 30x so viele
    # Basis-Kerzen). Der WS-Cache (get_cached_candles) liest aus einem lokalen Ringpuffer und hat
    # KEIN 1000er-Limit - das gilt nur fuer eine EINZELNE REST-Anfrage (Binance erlaubt max. 1000
    # Kerzen pro Aufruf). Frueher wurde derselbe 'min(1000, ...)'-gedeckelte Wert faelschlich auch
    # fuer den Cache-Read benutzt: bei z.B. 30s (factor=30) kamen so nie mehr als 1000/30 ≈ 33
    # fertige Kerzen zusammen, EGAL wie lange der Bot lief - ein struktureller Bug, kein
    # Aufwaerm-Timing (beobachtet als dauerhaft haengenbleibendes "zu wenig Kerzen (33/51 nötig)").
    # Fix: fuer den Cache-Read den vollen, UNGEDECKELTEN Bedarf anfragen (begrenzt nur noch durch
    # das, was der Cache tatsaechlich gespeichert hat - siehe MAX_CANDLES_PER_STREAM in
    # binance_ws.py), und den 1000er-Deckel nur noch fuer die REST-FALLBACK-Anfrage anwenden.
    cache_fetch_limit = count_back * factor + factor + 5
    fetch_limit = min(1000, cache_fetch_limit)

    if synth:
        # Zusammengesetzte Zeitrahmen (10s/15s/30s/45s aus 1s, 2m/eigene Minuten aus 1m) kamen bisher
        # bei JEDEM Durchlauf (alle 5 s, je Coin) per REST - jetzt wie die nativen Intervalle aus dem
        # WebSocket-Cache der Basis-Aufloesung (1s/1m, beide gecacht) zusammengesetzt; REST nur noch,
        # wenn der Cache (noch) nicht warm oder eingefroren ist.
        binance_ws.ensure_subscribed(mt, pair, base_resolution)
        cached = binance_ws.get_cached_candles(mt, pair, base_resolution, cache_fetch_limit)
        if cached is not None and cached[0]:
            if base_resolution == "1s":
                out = _resample_seconds_candles_with_volume(cached, factor)
            else:
                out = resample_candles_with_volume(cached, factor)
            if out and out[4]:
                if len(out[4]) > count_back:
                    out = tuple(series[-count_back:] for series in out)
                return out

    if _binance_is_banned(mt):
        return None  # aktiver Bann - keine Anfrage stellen, das wuerde ihn nur verlaengern
    try:
        await _binance_throttle(mt, f"vol:{base_resolution}")
        url = f"{BINANCE_BASE_URLS[mt]}?symbol={pair}&interval={base_resolution}&limit={fetch_limit}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                _binance_note_response(mt, resp)
                if resp.status in (418, 429):
                    body = await resp.text()
                    _binance_register_ban(mt, symbol, resp.status, body)
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

    if synth:
        if base_resolution == "1s":
            timestamps, opens, highs, lows, closes, volumes = _resample_seconds_candles_with_volume((timestamps, opens, highs, lows, closes, volumes), factor)
        else:
            timestamps, opens, highs, lows, closes, volumes = resample_candles_with_volume((timestamps, opens, highs, lows, closes, volumes), factor)
        if len(closes) > count_back:
            timestamps, opens, highs, lows, closes, volumes = (
                timestamps[-count_back:], opens[-count_back:], highs[-count_back:],
                lows[-count_back:], closes[-count_back:], volumes[-count_back:])

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


def resample_candles_with_volume(data, factor):
    """Wie resample_candles(), aber fuer 6er-Tupel MIT Volumen (Volumen wird pro Bucket
    aufsummiert) - noetig, damit synthetische Aufloesungen (2m, eigene Minuten) auch dort
    funktionieren, wo echtes Handelsvolumen gebraucht wird (VWAP-Deviation, SuperTrend+RSI-
    Volumen-Filter historisch)."""
    timestamps, opens, highs, lows, closes, volumes = data
    n = len(closes)
    if n == 0:
        return [], [], [], [], [], []
    bucket_ms = factor * 60_000
    out_ts, out_o, out_h, out_l, out_c, out_v = [], [], [], [], [], []
    i = 0
    while i < n:
        bucket = timestamps[i] // bucket_ms
        j = i
        while j < n and timestamps[j] // bucket_ms == bucket:
            j += 1
        if j - i == factor:
            out_ts.append(timestamps[i])
            out_o.append(opens[i])
            out_h.append(max(highs[i:j]))
            out_l.append(min(lows[i:j]))
            out_c.append(closes[j - 1])
            out_v.append(sum(volumes[i:j]))
        i = j
    return out_ts, out_o, out_h, out_l, out_c, out_v


def _resample_seconds_candles_with_volume(data, seconds):
    """Wie _resample_seconds_candles(), aber fuer 6er-Tupel MIT Volumen (siehe
    resample_candles_with_volume fuer den Grund)."""
    timestamps, opens, highs, lows, closes, volumes = data
    n = len(closes)
    if n == 0:
        return [], [], [], [], [], []
    bucket_ms = seconds * 1000
    out_ts, out_o, out_h, out_l, out_c, out_v = [], [], [], [], [], []
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
            out_v.append(sum(volumes[i:j]))
        i = j
    return out_ts, out_o, out_h, out_l, out_c, out_v


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

    # Der 1s-Puffer wird NUR von Sekunden-Zeitrahmen (10s/15s/30s/45s) gebraucht (get_seconds_candles) -
    # egal ob als Handels-Zeitrahmen oder als Zeitrahmen eines Filters. Vorher lief Vorbefuellung
    # (~11 REST-Seiten je Coin) + 5-s-Abruf + 1s-WebSocket-Stream fuer JEDEN Coin in GRID_SYMBOLS, auch
    # wenn dort gar keine Sekunden-Strategie eingestellt ist. Jetzt nur noch fuer Coins, in deren
    # Config irgendwo ein Sekunden-Zeitrahmen steht (wird laufend neu geprueft, Umschalten wirkt sofort).
    def _needs_1s_buffer(cfg):
        return any(isinstance(v, str) and v in SUB_MINUTE_RESOLUTIONS for v in cfg.values())

    prefill_done = bool(st.get("binance_1s_buffer"))
    while not prefill_done:
        if not _needs_1s_buffer(b["config"]):
            await asyncio.sleep(10)
            continue
        prefill_done = True
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
            if cfg["bot_active"] and _needs_1s_buffer(cfg):
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


async def ab_poll_loop(symbol):
    """Al-Shatri Breakout - siehe Kommentar-Block oben fuer die Signal-Logik. Ein-/Ausstieg (Wechsel bei
    Gegen-Signal) immer bei Kerzenschluss, Flanken-Erkennung (jetzt erfuellt, letzte Kerze nicht) wie im
    Original; der optionale feste Dollar-SL wird dagegen laufend gegen den Live-Preis geprueft. Alle
    Zeitrahmen, Backtest-faehig."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "ab_breakout" and cfg["bot_active"]:
                resolution = cfg.get("ab_resolution", "5m")
                params = _ab_effective_params(cfg)
                min_needed = max(params["slow_len"], params["lookback"], params["atr_len"], params["rsi_len"]) + 5
                needed_bars = min(1000, max(min_needed * 2, 200))
                st = b["state"]

                if resolution in SUB_MINUTE_RESOLUTIONS:
                    local = get_seconds_candles(st, SUB_MINUTE_RESOLUTIONS[resolution], needed_bars)
                    if local:
                        closed_ts, closed_o, closed_h, closed_l, closed_c = local
                        closed_v = [0.0] * len(closed_c)  # Sekunden-Puffer fuehrt kein Volumen mit
                    else:
                        closed_ts = None
                else:
                    # Nur Binance-Spot liefert Volumen ueber fetch_candles_binance_vol (keine
                    # market_type-Auswahl wie beim volumenlosen fetch_candles_binance_multi) -
                    # fuer den optionalen Volumen-Filter (Standard aus) reicht das.
                    data = await fetch_candles_binance_vol(symbol, resolution, count_back=needed_bars)
                    if data:
                        timestamps, opens, highs, lows, closes, volumes = data
                        closed_ts, closed_o, closed_h, closed_l, closed_c, closed_v = timestamps[:-1], opens[:-1], highs[:-1], lows[:-1], closes[:-1], volumes[:-1]
                    else:
                        closed_ts = None

                now = time.time()
                due_heartbeat = now - last_heartbeat > 300

                # SL/TP-Pruefung ist bewusst NICHT vom Kerzen-Abruf abhaengig: sie braucht nur den
                # aktuellen Live-Tick-Preis (st["last_price"], kommt per WS unabhaengig von diesem
                # Loop rein). So wird der SL auch dann weiter alle ~5s geprueft, wenn die Kerzen-
                # Abfrage gerade fehlschlaegt oder ein Binance-Rate-Limit aktiv ist - vorher haengte
                # die SL-Pruefung am erfolgreichen Kerzen-Fetch und blieb waehrend eines Banns
                # komplett aus, wodurch der SL erst mit der Verzoegerung des Banns griff.
                if st["position"] is not None and st["last_price"] is not None:
                    if cfg.get("ab_exit_mode", "flip") == "plan":
                        await check_ab_sl_tp(symbol, st["last_price"])
                    else:
                        await check_ab_sl(symbol, st["last_price"])

                if closed_ts and len(closed_c) > min_needed:
                    # Zwei Plausibilitaets-Checks gegen kaputte/veraltete Kerzendaten (z.B. durch eine
                    # abgeschnittene oder aus dem Cache wiederverwendete REST-Antwort waehrend eines
                    # Binance-Rate-Limits): (1) die letzte Kerze darf zeitlich nicht zu alt sein -
                    # das faengt "haengengebliebene" Daten ab, auch wenn deren Preis zufaellig nah am
                    # aktuellen Kurs liegt; (2) grobe Preis-Ausreisser als zusaetzliches Sicherheitsnetz.
                    candle_age_seconds = (now * 1000 - closed_ts[-1]) / 1000
                    max_age_seconds = 300  # grosszuegig fuer alle hier ueblichen Aufloesungen (10s-1h)
                    if candle_age_seconds > max_age_seconds:
                        debug_log(f"⚠️ [{symbol}] Al-Shatri Breakout: letzte Kerze wirkt veraltet "
                                  f"({round(candle_age_seconds)}s alt, Auflösung {resolution}) - überspringe Signal-Berechnung diesen Durchlauf.")
                        closed_ts = None
                    elif st["last_price"] is not None and closed_c[-1]:
                        deviation_pct = abs(closed_c[-1] - st["last_price"]) / st["last_price"] * 100
                        if deviation_pct > 2.0:
                            debug_log(f"⚠️ [{symbol}] Al-Shatri Breakout: Kerzendaten wirken unplausibel "
                                      f"(letzte Kerze {closed_c[-1]} vs. Live-Preis {st['last_price']}, "
                                      f"{round(deviation_pct, 2)}% Abweichung) - überspringe Signal-Berechnung diesen Durchlauf.")
                            closed_ts = None

                if closed_ts and len(closed_c) > min_needed:
                    signal_key = closed_ts[-1]
                    price = st["last_price"] if st["last_price"] is not None else closed_c[-1]

                    if cfg.get("ab_use_heikin_ashi", False):
                        # Heikin-Ashi-Umrechnung VOR der Signal-Berechnung - wie bei TradingView, wenn
                        # man den Chart-Typ umstellt. Reale Preise (price/last_price) bleiben fuer die
                        # tatsaechliche Order-Ausfuehrung unveraendert, nur das SIGNAL (und der daraus
                        # abgeleitete ATR fuer den Plan-Modus) rechnet auf den geglaetteten HA-Kerzen -
                        # genau wie bei Diamond Algo/UT Bot/Candle DNA in diesem Bot.
                        _, ab_sig_h, ab_sig_l, ab_sig_c = compute_heikin_ashi(closed_o, closed_h, closed_l, closed_c)
                    else:
                        ab_sig_h, ab_sig_l, ab_sig_c = closed_h, closed_l, closed_c
                    long_setup, short_setup, atr = compute_ab_breakout_signals(ab_sig_h, ab_sig_l, ab_sig_c, closed_v, params)
                    st["ab_atr_last"] = atr[-1]

                    # Optionaler uebergeordneter SuperTrend-Trendfilter (eigene, hoehere Zeiteinheit) -
                    # wie bei [Hoss] VWAP+RSI+Hull+DI: Long nur wenn dort bullisch, Short nur wenn
                    # baerisch. AND-Gate direkt auf die rohen Setup-Serien VOR der Flanken-Erkennung -
                    # ein Signal, das der Filter im selben Moment nicht bestaetigt, verfaellt einfach
                    # (kein Wartefenster wie bei hvd, um den Umfang nicht zu sprengen).
                    if cfg.get("ab_trend_filter_enabled", False):
                        tf_resolution = cfg.get("ab_trend_filter_resolution", "15m")
                        tf_atr_period = cfg.get("ab_trend_filter_atr_period", 10)
                        tf_multiplier = cfg.get("ab_trend_filter_multiplier", 3.0)
                        n_sig = len(closed_c)
                        if tf_resolution == resolution:
                            tf_st_line, _ = compute_diamond_supertrend(closed_h, closed_l, closed_c, tf_multiplier, tf_atr_period)
                            tf_long_ok = [tf_st_line[i] is not None and closed_c[i] > tf_st_line[i] for i in range(n_sig)]
                            tf_short_ok = [tf_st_line[i] is not None and closed_c[i] < tf_st_line[i] for i in range(n_sig)]
                        else:
                            tf_closed = await _fetch_trend_filter_candles_live(symbol, st, cfg, tf_resolution, tf_atr_period)
                            if tf_closed:
                                tf_h, tf_l, tf_c = tf_closed
                                tf_st_line_now, _ = compute_diamond_supertrend(tf_h, tf_l, tf_c, tf_multiplier, tf_atr_period)
                                tf_bullish_now = tf_st_line_now[-1] is not None and tf_c[-1] > tf_st_line_now[-1]
                                tf_long_ok = [tf_bullish_now] * n_sig
                                tf_short_ok = [not tf_bullish_now] * n_sig
                            else:
                                tf_long_ok = [True] * n_sig  # Fallback, falls (noch) keine Daten
                                tf_short_ok = [True] * n_sig
                        long_setup = [long_setup[i] and tf_long_ok[i] for i in range(n_sig)]
                        short_setup = [short_setup[i] and tf_short_ok[i] for i in range(n_sig)]

                    # Optionaler ASO-Sentiment-Filter (Nutzer-Idee, aus eigenem 'Average Sentiment
                    # Oscillator'-Pine-Script portiert, siehe compute_aso_filter): Long nur wenn
                    # ASOBulls>ASOBears, Short nur umgekehrt - AND-Gate wie der SuperTrend-Filter
                    # oben, aber immer auf derselben Zeiteinheit (keine hoehere Zeiteinheit noetig,
                    # der ASO ist als Kerzen-Sentiment gedacht, nicht als groesserer Trendfilter).
                    if cfg.get("ab_aso_filter_enabled", False):
                        n_sig = len(closed_c)
                        aso_bull_ok, aso_bear_ok = compute_aso_filter(
                            closed_o, closed_h, closed_l, closed_c,
                            cfg.get("ab_aso_filter_length", 10), cfg.get("ab_aso_filter_mode", 0),
                            cfg.get("ab_aso_filter_confirm_bars", 1))
                        long_setup = [long_setup[i] and aso_bull_ok[i] for i in range(n_sig)]
                        short_setup = [short_setup[i] and aso_bear_ok[i] for i in range(n_sig)]

                    if due_heartbeat:
                        last_heartbeat = now
                        debug_log(f"💓 [{symbol}] Al-Shatri Breakout aktiv: Preset={cfg.get('ab_preset','intraday')}, "
                                  f"Ausstieg={'Plan (ATR-SL + TP1/2/3)' if cfg.get('ab_exit_mode', 'flip') == 'plan' else 'Wechsel bei Gegen-Signal, SL=' + ('$' + str(cfg.get('ab_sl_manual_usd', 5.0)) if cfg.get('ab_sl_enabled', True) else 'aus')}, "
                                  f"ATR={round(atr[-1] or 0,4)}, Preis={closed_c[-1]}, Kerzen={len(closed_c)}, bot_active={cfg['bot_active']}")

                    if last_processed_ts is None:
                        new_indices = [len(closed_ts) - 1]
                    else:
                        # Numerischer Zeitstempel-Vergleich statt .index()-Lookup: findet zuverlaessig
                        # alle Kerzen NEUER als die zuletzt verarbeitete, auch wenn deren exakter
                        # Zeitstempel gerade aus dem geholten Fenster gerutscht ist (z.B. nach einem
                        # uebersprungenen Durchlauf durch die Plausibilitaets-Checks oben). Der alte
                        # ".index()"-Ansatz fiel in so einem Fall auf "immer nur die neueste Kerze"
                        # zurueck - OHNE zu pruefen, ob genau diese Kerze schon verarbeitet wurde. War
                        # der urspruengliche Einstiegsversuch fehlgeschlagen (Position blieb leer),
                        # wurde dieselbe (laengst abgeschlossene) Kerze dadurch immer wieder als "neu"
                        # gewertet und das Signal wiederholt ausgeloest - die "Phantom-Signale".
                        new_indices = [idx for idx in range(len(closed_ts)) if closed_ts[idx] > last_processed_ts]

                    for idx in new_indices:
                        if idx < 1:
                            continue
                        buy_signal = long_setup[idx] and not long_setup[idx - 1]
                        sell_signal = short_setup[idx] and not short_setup[idx - 1]
                        price_i = price if idx == len(closed_ts) - 1 else closed_c[idx]
                        last_processed_ts = closed_ts[idx]
                        await check_ab_entry(symbol, buy_signal, sell_signal, price_i, atr[idx])
                elif due_heartbeat:
                    last_heartbeat = now
                    if not closed_ts:
                        debug_log(f"⏳ [{symbol}] Al-Shatri Breakout wartet: keine Kerzen erhalten (Auflösung {resolution})")
                    else:
                        debug_log(f"⏳ [{symbol}] Al-Shatri Breakout wartet: zu wenig Kerzen ({len(closed_c)}/{min_needed + 1} nötig)")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Al-Shatri Breakout-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

        await asyncio.sleep(5)


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

    await check_grid_v2_tick(symbol, price)

    if st["position"] is None:
        if not bot_active or cfg["entry_mode"] != "grid":
            return
        # Cooldown nach Grid-SL: ohne das baut der Bot die gerade gerissene Position
        # sofort wieder auf und macht im Trend aus einem -20$-Tag einen -200$-Tag.
        if time.time() < float(st.get("grid_sl_cooldown_until") or 0.0):
            return
        direction_mode = cfg.get("grid_direction_mode", "both")

        # Anker-Nachfuehrung (optional): in long_only/short_only kann der Kurs beliebig weit
        # in die GESPERRTE Richtung weglaufen, ohne dass je ein Entry triggert - der Bot wuerde
        # sonst endlos auf eine Rueckkehr zur alten Zone warten (siehe Kursverlauf-Chart: Anker
        # weit unter dem aktuellen Kurs). Ist der Abstand zu gross, wird der Anker auf den
        # aktuellen Kurs nachgezogen, damit die Entry-Schwelle wieder in erreichbarer Naehe liegt.
        # Bewusst NUR fuer long_only/short_only - bei "both" bleibt irgendwann immer eine Seite
        # erreichbar, dort wuerde Nachfuehren nur unnoetig fruehe Entries erzeugen.
        if cfg.get("grid_anchor_follow_enabled", False) and st["anchor_price"]:
            old_anchor = st["anchor_price"]
            follow_abs = old_anchor * (cfg.get("grid_anchor_follow_pct", 1.0) / 100.0)
            if direction_mode == "long_only" and price > old_anchor + follow_abs:
                actual_pct = round((price - old_anchor) / old_anchor * 100, 2)
                debug_log(f"⚓ [{symbol}] Grid-Anker nachgezogen (long_only): {round(old_anchor,4)} -> {price} (Kurs war {actual_pct}% über dem Anker)")
                st["anchor_price"] = price
            elif direction_mode == "short_only" and price < old_anchor - follow_abs:
                actual_pct = round((old_anchor - price) / old_anchor * 100, 2)
                debug_log(f"⚓ [{symbol}] Grid-Anker nachgezogen (short_only): {round(old_anchor,4)} -> {price} (Kurs war {actual_pct}% unter dem Anker)")
                st["anchor_price"] = price
            elif direction_mode == "both" and abs(price - old_anchor) > follow_abs:
                # Bei "both" ist zwar keine Richtung gesperrt, der Anker kann nach einem
                # Trendtag trotzdem weit weg liegen. Symmetrisch nachziehen. Sicher, weil
                # dieser ganze Block nur laeuft, wenn st["position"] is None - also nie
                # unter einer offenen Position, wo das Raster wegrutschen wuerde.
                actual_pct = round(abs(price - old_anchor) / old_anchor * 100, 2)
                debug_log(f"⚓ [{symbol}] Grid-Anker nachgezogen (both): {round(old_anchor,4)} -> {price} (Abstand war {actual_pct}%)")
                st["anchor_price"] = price

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
            cd_min = float(cfg.get("grid_sl_cooldown_min", 0) or 0)
            if cd_min > 0:
                st["grid_sl_cooldown_until"] = time.time() + cd_min * 60
                debug_log(f"⏸️ [{symbol}] Grid-Cooldown aktiv fuer {cd_min} Min. - kein Wiedereinstieg bis dahin")
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


def _g2_nachkauf_size_multiplier(st, cfg):
    """Verdopplung (optional): 1. Nachkauf 1x, 2. Nachkauf 2x, 3. Nachkauf 4x, 4. Nachkauf 8x, ...
    st['entry_count'] ist zum Zeitpunkt des Aufrufs die Anzahl der BISHERIGEN Einstiege (1 nach
    dem Ersteinstieg, 2 nach dem 1. Nachkauf, usw.), execute_entry erhoeht ihn erst DANACH -
    der naechste Nachkauf ist also Stufe (entry_count), 0-indiziert fuer die Verdopplung.
    Alternative zur festen Verdopplung: g2_size_multiplier erlaubt einen frei waehlbaren Faktor
    (z.B. 1.5x pro Stufe statt fix 2x) - greift nur, wenn g2_double_enabled AUS ist."""
    if cfg.get("g2_double_enabled", False):
        return float(2 ** (st["entry_count"] - 1))
    size_mult = cfg.get("g2_size_multiplier", 1.0)
    if size_mult and size_mult != 1.0:
        return float(size_mult ** (st["entry_count"] - 1))
    return 1.0


async def check_grid_v2_tick(symbol, price):
    """Grid 2 - zweite, unabhaengige Grid-Strategie (eigenes Feld-Praefix g2_, eigene Werte,
    komplett unabhaengig von der ersten Grid-Strategie). Identische Grundmechanik (Anker,
    Nachkauf, TP, SL, Richtung, Anker-Nachfuehrung, Nach-TP-sofort-drehen) - siehe der erste
    Grid-Block oben fuer die identische Kommentierung dieser Teile. Zwei zusaetzliche, unabhaengig
    zuschaltbare Optionen:

    g2_revisit_enabled: laeuft GENAUSO wie beim ersten Grid weiter absteigend (jeder neue, tiefere
    Level braucht einen NEUEN, weiter entfernten Kurs - Abstand vom letzten Kaufpreis aus
    gemessen) - ZUSAETZLICH kann aber auch das ZULETZT gekaufte Level ein weiteres Mal ausloesen,
    wenn der Kurs zwischenzeitlich darueber (long) bzw. darunter (short) zurueckgekehrt ist. Jedes
    Level hat dafuer einen "scharf/entschaerft"-Zustand: direkt nach einem Kauf ist es entschaerft,
    erst wenn der Kurs wieder darueber steigt (long) wird es erneut scharf und kann beim naechsten
    Erreichen nochmal ausloesen. Beispiel: Kurs faellt von 1$ auf 90 Cent -> Nachkauf (entschaerft).
    Kurs faellt weiter auf 80 Cent -> Nachkauf (neuer, tieferer Level, wie beim ersten Grid).
    Kurs steigt auf 85 Cent -> die 80-Cent-Schwelle wird wieder scharf. Kurs faellt zurueck auf
    80 Cent -> Nachkauf ERNEUT auf demselben Level (nicht erst bei 70 Cent noetig).

    g2_double_enabled: jede Nachkauf-Stufe verdoppelt die Positionsgroesse der vorherigen (siehe
    _g2_nachkauf_size_multiplier) - nutzt den bereits vorhandenen size_multiplier-Parameter von
    execute_entry, keine Sonderlogik noetig.

    g2_size_multiplier: Alternative zu g2_double_enabled mit frei waehlbarem Faktor statt fixer
    Verdopplung - greift nur, wenn g2_double_enabled AUS ist.

    g2_deviation_multiplier: jeder weitere Nachkauf braucht einen groesseren Preis-Abstand als
    der vorherige (1.0 = fix wie bisher, Standard).

    g2_direction_mode="smart": statt fester Richtung wird alle 5 Min. per Binance-24h-Aenderung
    (siehe get_smart_direction_g2) eine Richtung vorgeschlagen - Entry passiert nur, wenn der
    Kurs zusaetzlich auch tatsaechlich in diese Richtung die Grid-Schwelle kreuzt.

    g2_sl_mode: "usd" (bisher, fester Betrag) oder "pct" (Prozent vom Ø-Einstieg) fuer den
    optionalen Notausstieg auf die GESAMTE Position.
    """
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if cfg["entry_mode"] != "grid_v2" or price is None:
        return
    bot_active = cfg["bot_active"]

    if st["anchor_price"] is None:
        st["anchor_price"] = price
        return

    if st["position"] is None:
        if not bot_active:
            return
        direction_mode = cfg.get("g2_direction_mode", "both")

        # Anker-Nachfuehrung: bei long_only/short_only ist die "gesperrte" Richtung fix, bei
        # "smart" wird sie alle 5 Min. neu vorgeschlagen (siehe get_smart_direction_g2) - in
        # beiden Faellen kann der Kurs beliebig weit in die JEWEILS NICHT gehandelte Richtung
        # weglaufen, ohne dass je ein Entry triggert. Deshalb hier einheitlich behandelt: die
        # aktuell "gesperrte" Richtung wird ermittelt (fix bei long_only/short_only, dynamisch
        # bei smart), und nur bei zu grossem Abstand in GENAU dieser Richtung nachgezogen.
        effective_lock = None
        if direction_mode == "long_only":
            effective_lock = "long"
        elif direction_mode == "short_only":
            effective_lock = "short"
        elif direction_mode == "smart":
            effective_lock = await get_smart_direction_g2(symbol)  # kann None sein (Binance-Daten fehlen)

        if cfg.get("g2_anchor_follow_enabled", False) and effective_lock and st["anchor_price"]:
            old_anchor = st["anchor_price"]
            follow_abs = old_anchor * (cfg.get("g2_anchor_follow_pct", 1.0) / 100.0)
            if effective_lock == "long" and price > old_anchor + follow_abs:
                actual_pct = round((price - old_anchor) / old_anchor * 100, 2)
                debug_log(f"⚓ [{symbol}] Grid-2-Anker nachgezogen ({direction_mode}, aktuell long gesperrt): {round(old_anchor,4)} -> {price} (Kurs war {actual_pct}% über dem Anker)")
                st["anchor_price"] = price
            elif effective_lock == "short" and price < old_anchor - follow_abs:
                actual_pct = round((old_anchor - price) / old_anchor * 100, 2)
                debug_log(f"⚓ [{symbol}] Grid-2-Anker nachgezogen ({direction_mode}, aktuell short gesperrt): {round(old_anchor,4)} -> {price} (Kurs war {actual_pct}% unter dem Anker)")
                st["anchor_price"] = price

        grid_step_abs = compute_step_abs_g2(st["anchor_price"], cfg, "grid")
        crossed_down = price <= st["anchor_price"] - grid_step_abs
        crossed_up = price >= st["anchor_price"] + grid_step_abs

        if direction_mode == "smart":
            if crossed_down or crossed_up:
                smart_dir = await get_smart_direction_g2(symbol)
                if smart_dir == "long" and crossed_down:
                    st["g2_trigger_armed"] = True
                    await execute_entry(symbol, "long", price, is_add_on=False)
                elif smart_dir == "short" and crossed_up:
                    st["g2_trigger_armed"] = True
                    await execute_entry(symbol, "short", price, is_add_on=False)
                # passt die 24h-Richtung nicht zur gekreuzten Seite, wird einfach weiter gewartet
        elif crossed_down and direction_mode != "short_only":
            st["g2_trigger_armed"] = True
            await execute_entry(symbol, "long", price, is_add_on=False)
        elif crossed_up and direction_mode != "long_only":
            st["g2_trigger_armed"] = True
            await execute_entry(symbol, "short", price, is_add_on=False)
        return

    if cfg.get("g2_sl_enabled", False) and st.get("avg_entry_price") is not None and st.get("total_coin_size"):
        avg_entry = st["avg_entry_price"]
        size = st["total_coin_size"]
        unrealized_pnl = (price - avg_entry) * size if st["position"] == "long" else (avg_entry - price) * size
        sl_mode = cfg.get("g2_sl_mode", "usd")
        if sl_mode == "pct":
            pnl_pct = (price - avg_entry) / avg_entry * 100 if st["position"] == "long" else (avg_entry - price) / avg_entry * 100
            sl_pct = cfg.get("g2_sl_pct", 5.0)
            if pnl_pct <= -sl_pct:
                debug_log(f"🚪 [{symbol}] Grid-2 SL: {st['position'].upper()} @ {price} (Verlust {round(pnl_pct, 2)}% erreicht -{sl_pct}%)")
                await execute_exit(symbol, price, "SL")
                return
        else:
            sl_usd = cfg.get("g2_sl_manual_usd", 20.0)
            if unrealized_pnl <= -sl_usd:
                debug_log(f"🚪 [{symbol}] Grid-2 SL: {st['position'].upper()} @ {price} (unrealisierter Verlust {round(unrealized_pnl, 2)} $ erreicht -{sl_usd} $)")
                await execute_exit(symbol, price, "SL")
                return

    tp_step_abs = compute_step_abs_g2(st["avg_entry_price"], cfg, "tp")
    max_nachkauf = cfg.get("g2_max_nachkauf", 5)
    revisit_enabled = cfg.get("g2_revisit_enabled", False)
    can_nachkauf = bot_active and (max_nachkauf == 0 or st["entry_count"] < max_nachkauf)
    direction = st["position"]

    # g2_levels: EIN Eintrag pro bisher gekauftem Level in diesem Zyklus ({"price":..,"armed":..}),
    # nicht nur der letzte - jedes Level bekommt seinen EIGENEN Scharf/Entschaerft-Status. Direkt
    # nach dem Kauf ist ein Level entschaerft; es wird erst wieder scharf, wenn der Kurs ERNEUT
    # darueber (long) bzw. darunter (short) steigt/faellt - unabhaengig davon, ob zwischenzeitlich
    # TIEFERE (long) Level dazugekauft wurden. Beispiel: 90/80/70 Cent gekauft (alle drei
    # entschaerft), Kurs steigt auf 87 Cent -> nur 80 und 70 Cent werden wieder scharf (90 Cent
    # bleibt entschaerft, da nie wieder erreicht) -> faellt der Kurs zurueck, kauft der Bot
    # WIEDER bei 80 Cent, dann WIEDER bei 70 Cent, dann erst bei einem NEUEN, tieferen Level
    # (60 Cent). Steigt der Kurs danach durch, OHNE zu fallen, wird an 70/80 Cent NICHT gekauft
    # (Kaeufe passieren nur beim Faellen/Steigen in die JEWEILIGE Nachkauf-Richtung, nie beim
    # Erholen in die Gegenrichtung).
    if not st.get("g2_levels"):
        st["g2_levels"] = [{"price": st["avg_entry_price"], "armed": False}]
    levels = st["g2_levels"]

    deepest_price = min(l["price"] for l in levels) if direction == "long" else max(l["price"] for l in levels)
    base_step_abs = compute_step_abs_g2(deepest_price, cfg, "grid")
    # Deviation Multiplier: jeder weitere Nachkauf braucht einen groesseren Abstand als der
    # vorherige (1.0 = fix wie bisher). Gleiche Stufen-Indizierung wie _g2_nachkauf_size_multiplier
    # (entry_count-1), damit der 1. Nachkauf immer den unveraenderten Basis-Abstand nutzt.
    deviation_mult = cfg.get("g2_deviation_multiplier", 1.0)
    step_abs = base_step_abs * (deviation_mult ** max(st["entry_count"] - 1, 0))

    if revisit_enabled:
        # Mindest-Erholung, bevor ein Level wieder "scharf" wird (Bruchteil der Grid-Stufe, siehe
        # g2_revisit_rearm_pct) - OHNE das reicht schon normales Markt-Rauschen (ein paar Cent hin
        # und her um genau ein Level herum) aus, um dasselbe Level dutzende Male pro Minute
        # auszuloesen (live beobachtet: 17+ Trades in 4 Minuten bei nur ~$1 Kursspanne und $0,50
        # Grid-Stufe - der Kurs hat rein zufaellig ein Level mehrfach ganz knapp uebersprungen).
        rearm_margin = step_abs * (cfg.get("g2_revisit_rearm_pct", 50.0) / 100.0)
        for lvl in levels:
            if (direction == "long" and price > lvl["price"] + rearm_margin) or (direction == "short" and price < lvl["price"] - rearm_margin):
                lvl["armed"] = True

    new_level_price = deepest_price - step_abs if direction == "long" else deepest_price + step_abs

    if direction == "long":
        if price >= st["avg_entry_price"] + tp_step_abs:
            await execute_exit(symbol, price, "TP")
            return
        if price <= new_level_price and can_nachkauf:
            # Klassischer, NEUER (tieferer) Level - wie beim ersten Grid, unabhaengig vom
            # Revisit-Status. WICHTIG: levels.append() PASSIERT VOR dem await execute_entry(...) -
            # kommen mehrere Preis-Updates kurz hintereinander rein (live beobachtet: "3x
            # gleichzeitig"), wuerden sonst ALLE noch den alten Zustand sehen, bevor die erste
            # Order ueberhaupt verbucht ist, und ALLE parallel feuern. Da zwischen zwei await-
            # Punkten in Python niemals ein anderer Task dazwischenfunkt, schliesst das
            # Vorziehen der Zustandsaenderung dieses Zeitfenster komplett.
            new_level = {"price": price, "armed": False}
            levels.append(new_level)
            ok = await execute_entry(symbol, "long", price, is_add_on=True, size_multiplier=_g2_nachkauf_size_multiplier(st, cfg))
            if not ok:
                levels.remove(new_level)  # Order fehlgeschlagen - Level-Eintrag zurücknehmen
            return
        if revisit_enabled and can_nachkauf:
            # Von den scharfen Leveln bei/ueber dem aktuellen Kurs nur das HOECHSTE (naechstliegende)
            # nehmen - pro Tick maximal EIN Nachkauf, sonst koennte ein Ruckstand/Sprung mehrere
            # Level auf einmal ausloesen (siehe echter Vorfall bei Fractals+DCA).
            candidates = [l for l in levels if l["armed"] and price <= l["price"]]
            if candidates:
                target = max(candidates, key=lambda l: l["price"])
                target["armed"] = False  # VOR dem await - siehe Kommentar oben
                ok = await execute_entry(symbol, "long", price, is_add_on=True, size_multiplier=_g2_nachkauf_size_multiplier(st, cfg))
                if not ok:
                    target["armed"] = True  # Order fehlgeschlagen - Level bleibt scharf
    else:
        if price <= st["avg_entry_price"] - tp_step_abs:
            await execute_exit(symbol, price, "TP")
            return
        if price >= new_level_price and can_nachkauf:
            new_level = {"price": price, "armed": False}
            levels.append(new_level)
            ok = await execute_entry(symbol, "short", price, is_add_on=True, size_multiplier=_g2_nachkauf_size_multiplier(st, cfg))
            if not ok:
                levels.remove(new_level)
            return
        if revisit_enabled and can_nachkauf:
            candidates = [l for l in levels if l["armed"] and price >= l["price"]]
            if candidates:
                target = min(candidates, key=lambda l: l["price"])
                target["armed"] = False  # VOR dem await - siehe Kommentar oben
                ok = await execute_entry(symbol, "short", price, is_add_on=True, size_multiplier=_g2_nachkauf_size_multiplier(st, cfg))
                if not ok:
                    target["armed"] = True



async def trading_loop():
    last_status_log = 0.0

    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20) as ws:
                for s in SYMBOLS:
                    await ws.send(json.dumps({"type": "subscribe", "channel": f"trade/{MARKET_INDICES[s]}"}))
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


def backtest_ab_breakout(candles, cfg, trend_filter_long_ok=None, trend_filter_short_ok=None):
    """Backtest fuer Al-Shatri Breakout. Braucht (anders als die meisten anderen Strategien) eine
    6er-Kerzenquelle MIT Volumen fuer den optionalen Volumen-Filter (Standard aus) - deshalb wie
    mo7_scalp/maverick_edge in run_backtest() als Sonderfall behandelt statt ueber den generischen
    5er-Tupel-Dispatch (BACKTEST_FUNCS). trend_filter_long_ok/short_ok (optional): vorab berechnete
    Listen fuer den SuperTrend-Trendfilter bei ABWEICHENDER Zeiteinheit (von run_backtest async
    vorbereitet und ausgerichtet, siehe _align_htf_series); bei gleicher Zeiteinheit/deaktiviertem
    Filter wird hier intern berechnet - wie bei backtest_hvd_signal."""
    o, h, l, c, v = candles[1], candles[2], candles[3], candles[4], candles[5]
    params = _ab_effective_params(cfg)
    if cfg.get("ab_use_heikin_ashi", False):
        # Signal UND ATR (fuer den Plan-Modus) rechnen auf Heikin-Ashi-Kerzen, SL/TP-Ausloesung im
        # Backtest bleibt trotzdem an den ECHTEN Kerzen (candles/_simulate_ab_trades), da im
        # Live-Handel auch der echte Marktpreis ausloest, nicht der geglaettete HA-Wert - wie bei
        # Diamond Algo (backtest_diamond_algo).
        _, sig_h, sig_l, sig_c = compute_heikin_ashi(o, h, l, c)
    else:
        sig_h, sig_l, sig_c = h, l, c
    long_setup, short_setup, atr = compute_ab_breakout_signals(sig_h, sig_l, sig_c, v, params)

    if trend_filter_long_ok is None and cfg.get("ab_trend_filter_enabled", False):
        tf_resolution = cfg.get("ab_trend_filter_resolution", "15m")
        if tf_resolution in (None, "", "same") or tf_resolution == cfg.get("ab_resolution", "1m"):
            tf_atr_period = cfg.get("ab_trend_filter_atr_period", 10)
            tf_multiplier = cfg.get("ab_trend_filter_multiplier", 3.0)
            tf_st_line, _ = compute_diamond_supertrend(h, l, c, tf_multiplier, tf_atr_period)
            trend_filter_long_ok = [tf_st_line[i] is not None and c[i] > tf_st_line[i] for i in range(len(c))]
            trend_filter_short_ok = [tf_st_line[i] is not None and c[i] < tf_st_line[i] for i in range(len(c))]
        # Bei ABWEICHENDER Zeiteinheit wird trend_filter_long_ok/short_ok von run_backtest (async) uebergeben.

    if trend_filter_long_ok is not None:
        long_setup = [long_setup[i] and trend_filter_long_ok[i] for i in range(len(long_setup))]
        short_setup = [short_setup[i] and trend_filter_short_ok[i] for i in range(len(short_setup))]

    if cfg.get("ab_aso_filter_enabled", False):
        o = candles[1]
        aso_bull_ok, aso_bear_ok = compute_aso_filter(
            o, h, l, c, cfg.get("ab_aso_filter_length", 10), cfg.get("ab_aso_filter_mode", 0),
            cfg.get("ab_aso_filter_confirm_bars", 1))
        long_setup = [long_setup[i] and aso_bull_ok[i] for i in range(len(long_setup))]
        short_setup = [short_setup[i] and aso_bear_ok[i] for i in range(len(short_setup))]

    warmup = max(params["slow_len"], params["lookback"], params["atr_len"], params["rsi_len"]) + 5
    return _simulate_ab_trades(candles, cfg, long_setup, short_setup, atr, warmup)


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


async def run_backtest(symbol, entry_mode, cfg, days, exclude_top_n=1):
    if entry_mode == "ab_breakout":
        max_candles = BACKTEST_MAX_CANDLES.get("ab_breakout", 100_000)
        resolution = cfg.get("ab_resolution", "15s")
        if resolution in SUB_MINUTE_RESOLUTIONS:
            # Sekunden-Aufloesungen kommen aus 1s-Basisdaten (10-30x mehr Rohdaten je Zeitraum) -
            # Obergrenze bewusst strenger, wie bei allen anderen Sub-Minuten-faehigen Strategien.
            max_candles = min(max_candles, 5000)
        candles, err = await _fetch_cached_mo7_backtest_candles(symbol, resolution, days, max_candles, market_type=cfg.get("binance_market_type", "spot"))
        if err:
            return {"error": err}
        params = _ab_effective_params(cfg)
        min_needed = max(params["slow_len"], params["lookback"], params["atr_len"], params["rsi_len"]) + 10
        if not candles or len(candles[4]) < min_needed:
            return {"error": f"Zu wenig historische Kerzen für einen aussagekräftigen Backtest erhalten (mind. ~{min_needed} nötig)."}
        n_candles = len(candles[4])

        trend_filter_long_ok = None
        trend_filter_short_ok = None
        if cfg.get("ab_trend_filter_enabled", False):
            tf_resolution = cfg.get("ab_trend_filter_resolution", "15m")
            tf_atr_period = cfg.get("ab_trend_filter_atr_period", 10)
            tf_multiplier = cfg.get("ab_trend_filter_multiplier", 3.0)
            if not (tf_resolution in (None, "", "same") or tf_resolution == resolution):
                tf_candles, tf_err = await _fetch_trend_filter_backtest_candles(symbol, cfg, candles[0], tf_resolution, tf_atr_period)
                if tf_err:
                    return {"error": tf_err}
                trend_filter_long_ok, trend_filter_short_ok = _trend_filter_ok_series(candles[0], tf_candles, tf_multiplier, tf_atr_period)
            # Bei gleicher Zeiteinheit: bleibt None, backtest_ab_breakout berechnet es selbst intern.

        trades = backtest_ab_breakout(candles, cfg, trend_filter_long_ok=trend_filter_long_ok, trend_filter_short_ok=trend_filter_short_ok)
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

    if entry_mode == "rsi_signal":
        max_candles = BACKTEST_MAX_CANDLES.get("rsi_signal", 100_000)
        resolution = cfg.get("rsi_resolution", "5m")
        if resolution in SUB_MINUTE_RESOLUTIONS:
            max_candles = min(max_candles, 5000)
        candles, err = await _fetch_cached_mo7_backtest_candles(symbol, resolution, days, max_candles, market_type=cfg.get("binance_market_type", "spot"))
        if err:
            return {"error": err}
        min_needed = cfg.get("rsi_length", 14) + 10
        if not candles or len(candles[4]) < min_needed:
            return {"error": f"Zu wenig historische Kerzen für einen aussagekräftigen Backtest erhalten (mind. ~{min_needed} nötig)."}
        n_candles = len(candles[4])

        trend_filter_long_ok = trend_filter_short_ok = None
        if cfg.get("rsi_supertrend_filter_enabled", False):
            tf_resolution = cfg.get("rsi_supertrend_filter_resolution", "15m")
            tf_atr_period = cfg.get("rsi_supertrend_filter_atr_period", 10)
            tf_multiplier = cfg.get("rsi_supertrend_filter_multiplier", 3.0)
            trend_filter_long_ok, trend_filter_short_ok, tf_err = await compute_supertrend_filter_backtest(
                symbol, cfg, candles[0], tf_resolution, tf_multiplier, tf_atr_period)
            if tf_err:
                return {"error": tf_err}

        adx_long_ok = adx_short_ok = None
        if cfg.get("rsi_adx_filter_enabled", False):
            adx_long_ok, adx_short_ok = compute_adx_filter_series(
                (candles[0], candles[1], candles[2], candles[3], candles[4]), cfg.get("rsi_adx_filter_length", 14), cfg.get("rsi_adx_filter_threshold", 20),
                directional=cfg.get("rsi_adx_filter_directional", True))

        macd_long_ok = macd_short_ok = None
        if cfg.get("rsi_macd_filter_enabled", False):
            macd_long_ok, macd_short_ok = compute_macd_filter_series(
                (candles[0], candles[1], candles[2], candles[3], candles[4]), cfg.get("rsi_macd_filter_fast", 12), cfg.get("rsi_macd_filter_slow", 26), cfg.get("rsi_macd_filter_signal", 9))

        trades = backtest_rsi_signal(candles, cfg, trend_filter_long_ok=trend_filter_long_ok, trend_filter_short_ok=trend_filter_short_ok,
                                      adx_long_ok=adx_long_ok, adx_short_ok=adx_short_ok, macd_long_ok=macd_long_ok, macd_short_ok=macd_short_ok)
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

    if entry_mode == "mvwap_mf_signal":
        max_candles = BACKTEST_MAX_CANDLES.get("mvwap_mf_signal", 100_000)
        resolution = cfg.get("mvwap_resolution", "5m")
        if resolution in SUB_MINUTE_RESOLUTIONS:
            max_candles = min(max_candles, 5000)
        candles, err = await _fetch_cached_mo7_backtest_candles(symbol, resolution, days, max_candles, market_type=cfg.get("binance_market_type", "spot"))
        if err:
            return {"error": err}
        params = _mvwap_effective_params(cfg)
        min_needed = max(params["mf_length"], params["cmf_length"]) + 30
        if not candles or len(candles[4]) < min_needed:
            return {"error": f"Zu wenig historische Kerzen für einen aussagekräftigen Backtest erhalten (mind. ~{min_needed} nötig)."}
        n_candles = len(candles[4])

        trend_filter_long_ok = trend_filter_short_ok = None
        if cfg.get("mvwap_supertrend_filter_enabled", False):
            tf_resolution = cfg.get("mvwap_supertrend_filter_resolution", "15m")
            tf_atr_period = cfg.get("mvwap_supertrend_filter_atr_period", 10)
            tf_multiplier = cfg.get("mvwap_supertrend_filter_multiplier", 3.0)
            trend_filter_long_ok, trend_filter_short_ok, tf_err = await compute_supertrend_filter_backtest(
                symbol, cfg, candles[0], tf_resolution, tf_multiplier, tf_atr_period)
            if tf_err:
                return {"error": tf_err}

        adx_long_ok = adx_short_ok = None
        if cfg.get("mvwap_adx_filter_enabled", False):
            adx_long_ok, adx_short_ok = compute_adx_filter_series(
                candles, cfg.get("mvwap_adx_filter_length", 14), cfg.get("mvwap_adx_filter_threshold", 20),
                directional=cfg.get("mvwap_adx_filter_directional", True))

        macd_long_ok = macd_short_ok = None
        if cfg.get("mvwap_macd_filter_enabled", False):
            macd_long_ok, macd_short_ok = compute_macd_filter_series(
                candles, cfg.get("mvwap_macd_filter_fast", 12), cfg.get("mvwap_macd_filter_slow", 26), cfg.get("mvwap_macd_filter_signal", 9))

        trades = backtest_mvwap_signal(candles, cfg, trend_filter_long_ok=trend_filter_long_ok, trend_filter_short_ok=trend_filter_short_ok,
                                        adx_long_ok=adx_long_ok, adx_short_ok=adx_short_ok, macd_long_ok=macd_long_ok, macd_short_ok=macd_short_ok)
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

    return {"error": f"Backtest für '{entry_mode}' nicht unterstützt (nur ab_breakout, rsi_signal, mvwap_mf_signal - Grid braucht historische Tick-/Orderbuchdaten, die es nicht gibt)."}
AB_SWEEP_MAX_COMBOS = 600
AB_SWEEP_MIN_RELIABLE_TRADES = 5


def _ab_sweep_compute(candles, cfg, tf_data, multipliers, tf_atr_period, exclude_top_n):
    """Rechenteil des Al-Shatri-Sweeps (reine CPU-Arbeit, laeuft ausserhalb des Event-Loops im Thread,
    damit die Live-Loops waehrend eines grossen Sweeps nicht blockiert werden). Die rohen Breakout-
    Setups (Range/EMA/RSI/Volumen + optional ASO) sind unabhaengig vom SuperTrend und werden nur
    EINMAL berechnet; je (Zeiteinheit, Multiplikator) werden nur noch SuperTrend-Filter und
    Trade-Simulation neu gerechnet. tf_data = [(zeiteinheit, kerzen | None)] - None = gleiche
    Zeiteinheit wie der Handels-Zeitrahmen."""
    ts, o, h, l, c, v = candles
    n = len(c)
    params = _ab_effective_params(cfg)
    if cfg.get("ab_use_heikin_ashi", False):
        _, sig_h, sig_l, sig_c = compute_heikin_ashi(o, h, l, c)
    else:
        sig_h, sig_l, sig_c = h, l, c
    long_raw, short_raw, _atr = compute_ab_breakout_signals(sig_h, sig_l, sig_c, v, params)
    if cfg.get("ab_aso_filter_enabled", False):
        aso_bull_ok, aso_bear_ok = compute_aso_filter(
            o, h, l, c, cfg.get("ab_aso_filter_length", 10), cfg.get("ab_aso_filter_mode", 0),
            cfg.get("ab_aso_filter_confirm_bars", 1))
        long_raw = [long_raw[i] and aso_bull_ok[i] for i in range(n)]
        short_raw = [short_raw[i] and aso_bear_ok[i] for i in range(n)]
    warmup = max(params["slow_len"], params["lookback"], params["atr_len"], params["rsi_len"]) + 5

    results = []
    for tf_resolution, tf_candles in tf_data:
        for mult in multipliers:
            if tf_candles is None:
                st_line, _ = compute_diamond_supertrend(h, l, c, mult, tf_atr_period)
                long_ok = [st_line[i] is not None and c[i] > st_line[i] for i in range(n)]
                short_ok = [st_line[i] is not None and c[i] < st_line[i] for i in range(n)]
            else:
                long_ok, short_ok = _trend_filter_ok_series(ts, tf_candles, mult, tf_atr_period)
            long_setup = [long_raw[i] and long_ok[i] for i in range(n)]
            short_setup = [short_raw[i] and short_ok[i] for i in range(n)]
            trades = _simulate_ab_trades(candles, cfg, long_setup, short_setup, _atr, warmup)
            stats = summarize_backtest_trades(trades, exclude_top_n)
            results.append({"ab_trend_filter_resolution": tf_resolution, "ab_trend_filter_multiplier": mult, **stats})
    return results


async def run_ab_sweep(symbol, cfg, days, timeframes, st_mult_min=0.1, st_mult_max=3.0, st_mult_step=0.1, exclude_top_n=1):
    """'Monte-Carlo'-Parametersweep fuer Al-Shatri Breakout (Nutzer-Vorgabe): testet den
    uebergeordneten SuperTrend-Trendfilter ueber ALLE gewaehlten Zeiteinheiten x einen Bereich von
    Multiplikatoren (Standard 0.1 bis 3.0 in 0.1-Schritten). Der Filter wird dabei fuer jede
    Kombination fest eingeschaltet (unabhaengig vom Schalter im Strategie-Panel); alles andere -
    Signal-Parameter, fester Dollar-SL, Richtung, ASO-Filter, ATR-Periode des SuperTrends - kommt aus
    der aktuellen Config. Zeiteinheiten, die sich nicht laden lassen (z.B. Sekunden-Zeitrahmen bei zu
    langem Zeitraum), werden uebersprungen und separat gemeldet statt den ganzen Sweep abzubrechen."""
    max_candles = BACKTEST_MAX_CANDLES.get("ab_breakout", 100_000)
    resolution = cfg.get("ab_resolution", "1m")
    if resolution in SUB_MINUTE_RESOLUTIONS:
        max_candles = min(max_candles, 5000)
    candles, err = await _fetch_cached_mo7_backtest_candles(symbol, resolution, days, max_candles, market_type=cfg.get("binance_market_type", "spot"))
    if err:
        return {"error": err}
    params = _ab_effective_params(cfg)
    min_needed = max(params["slow_len"], params["lookback"], params["atr_len"], params["rsi_len"]) + 10
    if not candles or len(candles[4]) < min_needed:
        return {"error": f"Zu wenig historische Kerzen für einen aussagekräftigen Sweep erhalten (mind. ~{min_needed} nötig)."}
    ts = candles[0]

    # +1e-9 gegen Gleitkomma-Abrundung: (3.0 - 0.1) / 0.1 ergibt 28.999999999999996 -> ohne die
    # Korrektur fehlte der Endwert 3.0 im Sweep.
    multipliers = sorted(set(round(st_mult_min + i * st_mult_step, 2)
                              for i in range(int((st_mult_max - st_mult_min) / max(st_mult_step, 1e-9) + 1e-9) + 1)
                              if st_mult_min + i * st_mult_step <= st_mult_max + 1e-9))
    multipliers = [m for m in multipliers if m > 0]
    if not multipliers:
        return {"error": "Der eingestellte SuperTrend-Multiplikator-Bereich ergibt keine gültigen Werte."}

    tfs, skipped = [], []
    for tf in timeframes or []:
        tf = str(tf).strip().lower()
        if not tf or tf in tfs:
            continue
        if _resolution_ms(tf) is None:
            skipped.append({"timeframe": tf, "reason": "unbekanntes Format (erlaubt: 10s/15s/30s/45s, 1m, 5m, 15m, 30m, 1h, 4h oder eigene Minuten wie 8m)"})
            continue
        tfs.append(tf)
    if not tfs:
        return {"error": "Keine gültige Zeiteinheit für den SuperTrend ausgewählt."}

    total_combos = len(tfs) * len(multipliers)
    if total_combos > AB_SWEEP_MAX_COMBOS:
        return {"error": f"Zu viele Kombinationen ({total_combos}, Limit {AB_SWEEP_MAX_COMBOS}) - weniger Zeiteinheiten wählen oder die Multiplikator-Schrittweite vergrößern."}

    tf_atr_period = cfg.get("ab_trend_filter_atr_period", 10)
    tf_data = []
    for tf in tfs:
        if tf == resolution:
            tf_data.append((tf, None))
            continue
        tf_candles, tf_err = await _fetch_trend_filter_backtest_candles(symbol, cfg, ts, tf, tf_atr_period)
        if tf_err:
            skipped.append({"timeframe": tf, "reason": tf_err})
            continue
        tf_data.append((tf, tf_candles))
    if not tf_data:
        return {"error": "Keine der gewählten SuperTrend-Zeiteinheiten ließ sich laden: " + "; ".join(f"{x['timeframe']}: {x['reason']}" for x in skipped)}

    loop = asyncio.get_event_loop()
    results = await loop.run_in_executor(None, _ab_sweep_compute, candles, cfg, tf_data, multipliers, tf_atr_period, exclude_top_n)

    rank_key = lambda r: (r["trades"] >= AB_SWEEP_MIN_RELIABLE_TRADES, r["total_pnl_usd"])
    best_sorted = sorted(results, key=rank_key, reverse=True)
    worst_sorted = sorted(results, key=lambda r: r["total_pnl_usd"])
    best_per_tf = []
    for tf, _tfc in tf_data:
        rows = [r for r in results if r["ab_trend_filter_resolution"] == tf]
        if rows:
            best_per_tf.append(max(rows, key=rank_key))
    best_per_tf.sort(key=rank_key, reverse=True)

    actual_days = (ts[-1] - ts[0]) / (24 * 60 * 60 * 1000)
    return {
        "symbol": symbol, "resolution": resolution, "requested_days": days,
        "actual_days_covered": round(actual_days, 1), "candles_processed": len(ts),
        "min_reliable_trades": AB_SWEEP_MIN_RELIABLE_TRADES,
        "combos_tested": len(results), "multipliers_tested": len(multipliers),
        "timeframes_tested": [tf for tf, _c in tf_data], "skipped_timeframes": skipped,
        "exit_mode": cfg.get("ab_exit_mode", "flip"),
        "sl_enabled": cfg.get("ab_sl_enabled", True), "sl_usd": cfg.get("ab_sl_manual_usd", 5.0),
        "results": best_sorted[:30], "worst_results": worst_sorted[:20], "best_per_timeframe": best_per_tf,
    }


AB_SIGNAL_SWEEP_MAX_COMBOS = 600
AB_SIGNAL_SWEEP_MIN_RELIABLE_TRADES = 5


def _ab_signal_sweep_compute(candles, cfg, base_params, trend_ok, aso_ok, combos, exclude_top_n):
    """Rechenteil des Al-Shatri Signal-Sweeps (Breakout-Range x schnelle EMA x langsame EMA),
    reine CPU-Arbeit im Thread. Der SuperTrend-Trendfilter und der ASO-Filter sind hier FEST -
    sie werden genau EINMAL vorab berechnet (trend_ok/aso_ok, je nach Config an/aus) und dann bei
    jeder Kombination unveraendert per AND auf die Signale gelegt; nur Range/EMA/RSI/Volumen aus
    compute_ab_breakout_signals wird je Kombination neu gerechnet. 'unabhaengig vom SuperTrend'
    (Nutzer-Vorgabe): dieser Sweep variiert den Trendfilter NICHT mit, anders als der bestehende
    SuperTrend-Sweep (run_ab_sweep) - er bleibt exakt so, wie im Strategie-Panel eingestellt."""
    ts, o, h, l, c, v = candles
    n = len(c)
    if cfg.get("ab_use_heikin_ashi", False):
        _, sig_h, sig_l, sig_c = compute_heikin_ashi(o, h, l, c)
    else:
        sig_h, sig_l, sig_c = h, l, c
    results = []
    for lookback, fast_len, slow_len in combos:
        params = dict(base_params, lookback=lookback, fast_len=fast_len, slow_len=slow_len)
        long_setup, short_setup, atr = compute_ab_breakout_signals(sig_h, sig_l, sig_c, v, params)
        if trend_ok is not None:
            trend_long_ok, trend_short_ok = trend_ok
            long_setup = [long_setup[i] and trend_long_ok[i] for i in range(n)]
            short_setup = [short_setup[i] and trend_short_ok[i] for i in range(n)]
        if aso_ok is not None:
            aso_bull_ok, aso_bear_ok = aso_ok
            long_setup = [long_setup[i] and aso_bull_ok[i] for i in range(n)]
            short_setup = [short_setup[i] and aso_bear_ok[i] for i in range(n)]
        warmup = max(slow_len, lookback, params["atr_len"], params["rsi_len"]) + 5
        trades = _simulate_ab_trades(candles, cfg, long_setup, short_setup, atr, warmup)
        stats = summarize_backtest_trades(trades, exclude_top_n)
        results.append({"ab_lookback": lookback, "ab_fast_len": fast_len, "ab_slow_len": slow_len, **stats})
    return results


async def run_ab_signal_sweep(symbol, cfg, days, lookback_min=10, lookback_max=60, lookback_step=10,
                               fast_min=10, fast_max=60, fast_step=10, slow_min=30, slow_max=150,
                               slow_step=20, exclude_top_n=1):
    """'Monte-Carlo'-Sweep fuer Al-Shatri Breakout ueber Breakout-Range (Kerzen), schnelle EMA und
    langsame EMA (Nutzer-Vorgabe) - unabhaengig vom SuperTrend-Trendfilter: der bleibt exakt so, wie
    im Strategie-Panel eingestellt (an oder aus, mit seiner konfigurierten Zeiteinheit/Multiplikator),
    und wird hier NICHT mitvariiert - dafuer gibt es den separaten run_ab_sweep. RSI/Volumen/ATR-
    Periode und der optionale ASO-Filter kommen ebenfalls unveraendert aus der aktuellen Config.
    Kombinationen mit schneller >= langsamer EMA werden uebersprungen (ungueltig, siehe Validierung
    im Original-Skript)."""
    max_candles = BACKTEST_MAX_CANDLES.get("ab_breakout", 100_000)
    resolution = cfg.get("ab_resolution", "1m")
    if resolution in SUB_MINUTE_RESOLUTIONS:
        max_candles = min(max_candles, 5000)
    candles, err = await _fetch_cached_mo7_backtest_candles(symbol, resolution, days, max_candles, market_type=cfg.get("binance_market_type", "spot"))
    if err:
        return {"error": err}
    ts, o, h, l, c, v = candles
    n = len(c)
    if n < max(slow_max, lookback_max) + 10:
        return {"error": f"Zu wenig historische Kerzen für einen aussagekräftigen Sweep erhalten (mind. ~{max(slow_max, lookback_max) + 10} nötig)."}

    def _int_range(lo, hi, step):
        lo, hi, step = int(lo), int(hi), max(1, int(step))
        return sorted(set(v for v in range(lo, hi + 1, step) if v >= 2))

    lookbacks = _int_range(lookback_min, lookback_max, lookback_step)
    fasts = _int_range(fast_min, fast_max, fast_step)
    slows = _int_range(slow_min, slow_max, slow_step)
    if not lookbacks or not fasts or not slows:
        return {"error": "Die eingestellten Bereiche für Breakout-Range/EMA ergeben keine gültigen Werte."}

    combos = [(lb, f, sl) for lb in lookbacks for f in fasts for sl in slows if f < sl]
    if not combos:
        return {"error": "Keine gültige Kombination: die schnelle EMA muss in jeder Kombination kleiner als die langsame sein - Bereiche prüfen."}
    if len(combos) > AB_SIGNAL_SWEEP_MAX_COMBOS:
        return {"error": f"Zu viele Kombinationen ({len(combos)}, Limit {AB_SIGNAL_SWEEP_MAX_COMBOS}) - Bereiche verkleinern oder Schrittweiten vergrößern."}

    base_params = dict(_ab_effective_params(cfg))  # rsi_len/rsi_gate/use_volume/vol_mult/atr_len bleiben fest

    trend_ok = None
    if cfg.get("ab_trend_filter_enabled", False):
        tf_resolution = cfg.get("ab_trend_filter_resolution", "15m")
        tf_atr_period = cfg.get("ab_trend_filter_atr_period", 10)
        tf_multiplier = cfg.get("ab_trend_filter_multiplier", 3.0)
        if tf_resolution in (None, "", "same") or tf_resolution == resolution:
            tf_st_line, _ = compute_diamond_supertrend(h, l, c, tf_multiplier, tf_atr_period)
            trend_ok = ([tf_st_line[i] is not None and c[i] > tf_st_line[i] for i in range(n)],
                        [tf_st_line[i] is not None and c[i] < tf_st_line[i] for i in range(n)])
        else:
            tf_candles, tf_err = await _fetch_trend_filter_backtest_candles(symbol, cfg, ts, tf_resolution, tf_atr_period)
            if tf_err:
                return {"error": f"SuperTrend-Trendfilter ({tf_resolution}): {tf_err}"}
            trend_ok = _trend_filter_ok_series(ts, tf_candles, tf_multiplier, tf_atr_period)

    aso_ok = None
    if cfg.get("ab_aso_filter_enabled", False):
        aso_ok = compute_aso_filter(o, h, l, c, cfg.get("ab_aso_filter_length", 10),
                                     cfg.get("ab_aso_filter_mode", 0), cfg.get("ab_aso_filter_confirm_bars", 1))

    loop = asyncio.get_event_loop()
    results = await loop.run_in_executor(None, _ab_signal_sweep_compute, candles, cfg, base_params, trend_ok, aso_ok, combos, exclude_top_n)

    rank_key = lambda r: (r["trades"] >= AB_SIGNAL_SWEEP_MIN_RELIABLE_TRADES, r["total_pnl_usd"])
    best_sorted = sorted(results, key=rank_key, reverse=True)
    worst_sorted = sorted(results, key=lambda r: r["total_pnl_usd"])

    actual_days = (ts[-1] - ts[0]) / (24 * 60 * 60 * 1000)
    return {
        "symbol": symbol, "resolution": resolution, "requested_days": days,
        "actual_days_covered": round(actual_days, 1), "candles_processed": n,
        "min_reliable_trades": AB_SIGNAL_SWEEP_MIN_RELIABLE_TRADES,
        "combos_tested": len(results), "lookbacks_tested": lookbacks, "fasts_tested": fasts, "slows_tested": slows,
        "exit_mode": cfg.get("ab_exit_mode", "flip"),
        "sl_enabled": cfg.get("ab_sl_enabled", True), "sl_usd": cfg.get("ab_sl_manual_usd", 5.0),
        "trend_filter_enabled": cfg.get("ab_trend_filter_enabled", False),
        "trend_filter_resolution": cfg.get("ab_trend_filter_resolution", "15m") if cfg.get("ab_trend_filter_enabled", False) else None,
        "results": best_sorted[:30], "worst_results": worst_sorted[:20],
    }




# ============================================================
# Wiederhergestellte, von Al-Shatri Breakout / Grid mitgenutzte Hilfsfunktionen -
# waren versehentlich mit dem entfernten fib_reversal/da..cd-Strategie-Bloecken
# mitgeloescht worden (lagen dazwischen), sind aber gemeinsam genutzt.
# ============================================================

BACKTEST_CACHE_MAX_ENTRIES = 5


BACKTEST_CACHE_TTL_SECONDS = 900


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
    "range_filter": 100_000,
    "maverick_edge": 100_000,
    "st_rsi_signal": 100_000,
    "hvd_signal": 100_000,
    "ab_breakout": 100_000,
}


_backtest_candle_cache = OrderedDict()  # key: (symbol, resolution) -> {"fetched_at": float, "days": int, "candles": (...)}


def _backtest_cache_get(cache_key):
    entry = _backtest_candle_cache.get(cache_key)
    if entry is not None:
        _backtest_candle_cache.move_to_end(cache_key)  # als zuletzt genutzt markieren
    return entry


def _backtest_cache_set(cache_key, entry):
    _backtest_candle_cache[cache_key] = entry
    _backtest_candle_cache.move_to_end(cache_key)
    while len(_backtest_candle_cache) > BACKTEST_CACHE_MAX_ENTRIES:
        _backtest_candle_cache.popitem(last=False)


def _bt_close_trade(trades, direction, entry, exit_price, size, i, entry_i, reason, ts=None):
    pnl = (exit_price - entry) * size if direction == "long" else (entry - exit_price) * size
    trade = {"dir": direction, "entry": entry, "exit": exit_price, "reason": reason,
             "pnl": pnl, "bars_held": i - entry_i}
    if ts is not None:
        trade["entry_ts"] = ts[entry_i]
        trade["exit_ts"] = ts[i]
    trades.append(trade)


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


def _min_ts_step(ts):
    """Kleinster positiver Abstand zwischen zwei Zeitstempeln = Kerzenlaenge (auch bei Luecken)."""
    best = None
    for k in range(1, len(ts)):
        d = ts[k] - ts[k - 1]
        if d > 0 and (best is None or d < best):
            best = d
    return best


def _resolution_ms(resolution):
    """Kerzenlaenge einer Zeiteinheit in Millisekunden (10s/15s/30s/45s, native Binance-Intervalle
    und beliebige Minutenwerte wie '8m'/'24m') - None bei unbekanntem Format."""
    if resolution in SUB_MINUTE_RESOLUTIONS:
        return SUB_MINUTE_RESOLUTIONS[resolution] * 1000
    if resolution in BINANCE_INTERVAL_MS:
        return BINANCE_INTERVAL_MS[resolution]
    m = re.match(r"^(\d+)m$", resolution or "")
    if m and int(m.group(1)) > 0:
        return int(m.group(1)) * 60_000
    return None


async def _fetch_trend_filter_backtest_candles(symbol, cfg, base_ts, tf_resolution, tf_atr_period):
    """Backtest-Gegenstueck: holt die Kerzen der Trendfilter-Zeiteinheit passend zum ZEITRAUM der
    Handels-Kerzen (base_ts) - plus Vorlauf fuer die SuperTrend-Einschwingphase. Liefert
    (candles, None) oder (None, fertige Fehlermeldung). Vorher: feste 20.000-Kerzen-Grenze, bei
    feinen Zeiteinheiten (z.B. 1m ueber 30 Tage) fehlte dadurch der Anfang des Zeitraums."""
    tf_ms = _resolution_ms(tf_resolution)
    if tf_ms is None:
        return None, f"SuperTrend-Trendfilter-Zeiteinheit ({tf_resolution}): unbekanntes Format."

    synth = resolve_synthetic_resolution(tf_resolution)
    if synth and synth[0] == "1m":
        # Aus 1m zusammengesetzte Minuten-Zeiteinheiten (6m-14m, 16m-20m, ...): die 1m-Historie wird
        # EINMAL geladen (Cache-Schluessel gleich fuer alle diese Zeiteinheiten, weil das Fenster auf den
        # Vorlauf von mindestens 20 Minuten-Kerzen ausgelegt ist) und hier lokal zusammengesetzt -
        # statt fuer jede Zeiteinheit dieselben ~45 REST-Seiten (30 Tage) erneut zu holen.
        wide_ms = max(tf_ms, 20 * 60_000)
        fetch_ms = (base_ts[-1] - base_ts[0]) + (tf_atr_period * 5 + 20) * wide_ms + wide_ms
        fetch_ms = -(-fetch_ms // 3_600_000) * 3_600_000  # auf ganze Stunden aufrunden -> gleicher Cache-Schluessel
        needed_1m = int(fetch_ms // 60_000) + 10
        base_1m, err, _ = await _fetch_cached_backtest_candles(
            symbol, "1m", fetch_ms / 86_400_000, min(max(needed_1m, 200), 150_000),
            market_type=cfg.get("binance_market_type", "spot"))
        if err:
            return None, f"SuperTrend-Trendfilter-Zeiteinheit ({tf_resolution}): {err}"
        candles = resample_candles(base_1m, synth[1]) if base_1m else None
        if not candles or len(candles[4]) < tf_atr_period + 5:
            return None, f"Zu wenig historische Kerzen für die Trendfilter-Zeiteinheit ({tf_resolution}) erhalten."
        return candles, None

    warm_ms = (tf_atr_period * 5 + 20) * tf_ms
    span_ms = (base_ts[-1] - base_ts[0]) + warm_ms + tf_ms
    needed = int(span_ms // tf_ms) + 10
    if tf_resolution in SUB_MINUTE_RESOLUTIONS and needed > 5000:
        cover_h = round(5000 * tf_ms / 3_600_000, 1)
        return None, (f"SuperTrend-Trendfilter-Zeiteinheit ({tf_resolution}): Sekunden-Zeiteinheiten sind im Backtest auf "
                      f"5000 Kerzen (~{cover_h} Std.) begrenzt, der Backtest-Zeitraum ist länger. Kürzeren Zeitraum "
                      f"oder eine Trendfilter-Zeiteinheit ab 1 Minute wählen.")
    candles, err, _ = await _fetch_cached_backtest_candles(
        symbol, tf_resolution, span_ms / 86_400_000, min(max(needed, 200), 100_000),
        market_type=cfg.get("binance_market_type", "spot"))
    if err:
        return None, f"SuperTrend-Trendfilter-Zeiteinheit ({tf_resolution}): {err}"
    if not candles or len(candles[4]) < tf_atr_period + 5:
        return None, f"Zu wenig historische Kerzen für die Trendfilter-Zeiteinheit ({tf_resolution}) erhalten."
    return candles, None


_trend_filter_warn_last = {}  # (symbol, resolution) -> letzter Warn-Zeitpunkt, damit die "zu wenig Kerzen"-Meldung hoechstens alle 5 Min. pro Kombination im Log steht


async def _fetch_trend_filter_candles_live(symbol, st, cfg, tf_resolution, tf_atr_period):
    """Liefert (highs, lows, closes) der ABGESCHLOSSENEN Kerzen der SuperTrend-Trendfilter-Zeiteinheit
    (live, fuer ab_breakout und hvd_signal) - oder None, wenn (noch) nicht genug Daten da sind (der
    Aufrufer laesst den Filter dann wie bisher durch). Unterstuetzt ALLE Zeiteinheiten der Strategien:
    Sekunden-Zeitrahmen (10s/15s/30s/45s) kommen aus dem 1s-Puffer (wie beim eigenen Handels-Zeitrahmen,
    kein REST-Traffic), native Minuten/Stunden aus dem Binance-Cache, eigene Minutenwerte (z.B. 8m, 24m)
    werden aus 1m-Kerzen zusammengesetzt."""
    tf_needed = min(500, tf_atr_period * 5 + 20)
    tf_h = tf_l = tf_c = None
    if tf_resolution in SUB_MINUTE_RESOLUTIONS:
        local = get_seconds_candles(st, SUB_MINUTE_RESOLUTIONS[tf_resolution], tf_needed)
        if local:
            # get_seconds_candles liefert bereits nur abgeschlossene Buckets (der letzte muss
            # vollstaendig sein) - deshalb hier KEIN "[:-1]" wie bei den Minuten-Zeitrahmen.
            _, _, tf_h, tf_l, tf_c = local
    else:
        synth = resolve_synthetic_resolution(tf_resolution)
        factor = synth[1] if synth else 1
        # Zusammengesetzte Zeitrahmen (z.B. 24m) brauchen factor-mal so viele Basis-Kerzen - die
        # Basis-Abfrage bleibt bewusst unter ~900 Kerzen (REST-Limit 1000), sonst kaeme gar nichts an.
        count = tf_needed if factor == 1 else max(1, min(tf_needed, 900 // factor))
        tf_data = await fetch_candles_binance_multi(symbol, tf_resolution, count_back=count, market_type=cfg.get("binance_market_type", "spot"))
        if tf_data:
            _, _, tf_h, tf_l, tf_c = tf_data
            tf_h, tf_l, tf_c = tf_h[:-1], tf_l[:-1], tf_c[:-1]
    if not tf_c or len(tf_c) <= tf_atr_period:
        now = time.time()
        key = (symbol, tf_resolution)
        if now - _trend_filter_warn_last.get(key, 0.0) > 300:
            _trend_filter_warn_last[key] = now
            debug_log(f"⚠️ [{symbol}] SuperTrend-Trendfilter ({tf_resolution}): noch nicht genug Kerzen "
                      f"({len(tf_c) if tf_c else 0}/{tf_atr_period + 1} nötig) - Filter lässt Signale vorerst durch.")
        return None
    return tf_h, tf_l, tf_c


def _trend_filter_ok_series(base_ts, tf_candles, tf_multiplier, tf_atr_period):
    """(long_ok[], short_ok[]) je Handels-Kerze aus dem SuperTrend der Trendfilter-Zeiteinheit.

    KEIN Look-Ahead: eine Filter-Kerze ist fuer eine Handels-Kerze erst verwendbar, wenn sie zu deren
    SCHLUSS bereits geschlossen ist (Filter-Kerzenende <= Handels-Kerzenende) - genau wie live, wo nur
    die letzte abgeschlossene Filter-Kerze zaehlt. Vorher wurde nach dem ERÖFFNUNGSzeitpunkt
    zugeordnet: eine 15m-Kerze war damit schon ab ihrer ersten Minute mit ihrem spaeteren Schlusskurs
    (und damit ihrer spaeteren SuperTrend-Richtung) sichtbar - der Backtest 'wusste' die Richtung der
    laufenden Filter-Kerze im Voraus und war deutlich zu optimistisch (bei Sweeps besonders bei kleinen
    Multiplikatoren, weil die den Look-Ahead am staerksten ausnutzen).
    Handels-Kerzen VOR der ersten verwendbaren Filter-Kerze bekommen (False, False) = kein Signal."""
    tf_ts, _o, tf_h, tf_l, tf_c = tf_candles
    tf_st_line, _ = compute_diamond_supertrend(tf_h, tf_l, tf_c, tf_multiplier, tf_atr_period)
    tf_bullish = [tf_st_line[i] is not None and tf_c[i] > tf_st_line[i] for i in range(len(tf_c))]
    base_ms = _min_ts_step(base_ts) or 60_000
    tf_ms = _min_ts_step(tf_ts) or base_ms
    # verwendbar ab: tf_ts[j] + tf_ms <= base_ts[i] + base_ms  <=>  (tf_ts[j] + tf_ms - base_ms) <= base_ts[i]
    available_ts = [t + tf_ms - base_ms for t in tf_ts]
    aligned = _align_htf_series(base_ts, available_ts, tf_bullish)
    first_ts = available_ts[0]
    long_ok = [bool(aligned[i]) and base_ts[i] >= first_ts for i in range(len(base_ts))]
    short_ok = [(not aligned[i]) and base_ts[i] >= first_ts for i in range(len(base_ts))]
    return long_ok, short_ok


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


def compute_aso_filter(opens, highs, lows, closes, length=10, mode=0, confirm_bars=1):
    """Portiert aus dem Nutzer-Pine-Indikator 'Average Sentiment Oscillator' (ASO, KivancOzbilgic) -
    misst Bullen-/Baerendruck aus Intrabar- UND Gruppen-Kerzen-Bewegung (Intrabar = aktuelle Kerze,
    Gruppe = die letzten 'length' Kerzen inkl. Range-Hoch/-Tief und dem Open von vor 'length-1'
    Kerzen, wie im Original 'open[length-1]'). mode: 0 = Mittel aus Intrabar+Gruppe (Original-
    Standard), 1 = nur Intrabar, 2 = nur Gruppe. Als Filter genutzt (nicht als Chart-Linien wie im
    Pine-Original): bull_ok[i] = ASOBulls > ASOBears an Kerze i, bear_ok[i] = umgekehrt.
    confirm_bars > 1 verlangt zusaetzlich, dass die letzten confirm_bars Kerzen ALLE in dieselbe
    Richtung zeigen (verhindert Filterwechsel bei jedem kleinen Wackler, wie beim separaten
    ASO-Filter-Pine-Script). Gibt (bull_ok, bear_ok) als Bool-Listen zurueck."""
    n = len(closes)
    if n == 0:
        return [], []
    lowest, _ = _rolling_min_max(lows, length)
    _, highest = _rolling_min_max(highs, length)
    bulls_raw = [0.0] * n
    bears_raw = [0.0] * n
    for i in range(n):
        intrarange = highs[i] - lows[i]
        k1 = intrarange if intrarange != 0 else 1
        grouplow = lowest[i]
        grouphigh = highest[i]
        group_open_i = i - length + 1
        groupopen = opens[group_open_i] if group_open_i >= 0 else opens[0]
        grouprange = grouphigh - grouplow
        k2 = grouprange if grouprange != 0 else 1
        intrabar_bulls = (((closes[i] - lows[i]) + (highs[i] - opens[i])) / 2 * 100) / k1
        group_bulls = (((closes[i] - grouplow) + (grouphigh - groupopen)) / 2 * 100) / k2
        intrabar_bears = (((highs[i] - closes[i]) + (opens[i] - lows[i])) / 2 * 100) / k1
        group_bears = (((grouphigh - closes[i]) + (groupopen - grouplow)) / 2 * 100) / k2
        if mode == 1:
            bulls_raw[i] = intrabar_bulls
            bears_raw[i] = intrabar_bears
        elif mode == 2:
            bulls_raw[i] = group_bulls
            bears_raw[i] = group_bears
        else:
            bulls_raw[i] = (intrabar_bulls + group_bulls) / 2
            bears_raw[i] = (intrabar_bears + group_bears) / 2
    aso_bulls = _sma_series(bulls_raw, length)
    aso_bears = _sma_series(bears_raw, length)
    raw_bullish = [aso_bulls[i] > aso_bears[i] for i in range(n)]
    if confirm_bars <= 1:
        return raw_bullish, [not b for b in raw_bullish]
    bull_ok = [False] * n
    bear_ok = [False] * n
    for i in range(n):
        if i < confirm_bars - 1:
            continue
        window = raw_bullish[i - confirm_bars + 1:i + 1]
        bull_ok[i] = all(window)
        bear_ok[i] = not any(window)
    return bull_ok, bear_ok


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




# ============================================================
# Wiederhergestellt: Al-Shatri Breakout Kern-Funktionen - waren versehentlich mit
# entfernten Strategie-Bloecken mitgeloescht worden.
# ============================================================

AB_PRESETS = {
    "scalping": {"lookback": 10, "fast_len": 9, "slow_len": 21, "rsi_len": 14, "rsi_gate": 52,
                 "use_volume": False, "vol_mult": 1.0, "atr_len": 14, "atr_mult": 1.0,
                 "r1": 0.5, "r2": 1.0, "r3": 1.5},
    "intraday": {"lookback": 20, "fast_len": 20, "slow_len": 50, "rsi_len": 14, "rsi_gate": 55,
                 "use_volume": False, "vol_mult": 1.5, "atr_len": 14, "atr_mult": 1.5,
                 "r1": 1.0, "r2": 2.0, "r3": 3.0},
    "swing": {"lookback": 50, "fast_len": 50, "slow_len": 200, "rsi_len": 14, "rsi_gate": 58,
              "use_volume": True, "vol_mult": 1.5, "atr_len": 21, "atr_mult": 2.0,
              "r1": 1.5, "r2": 3.0, "r3": 5.0},
}


def _ab_effective_params(cfg):
    """Preset 'Scalping'/'Intraday'/'Swing' uebernimmt die Original-Presets aus dem Pine-Script
    1:1 (siehe Preset-Logik dort), 'custom' nutzt die frei eingestellten ab_*-Werte."""
    preset = cfg.get("ab_preset", "intraday")
    if preset in AB_PRESETS:
        return AB_PRESETS[preset]
    return {
        "lookback": cfg.get("ab_lookback", 20), "fast_len": cfg.get("ab_fast_len", 20),
        "slow_len": cfg.get("ab_slow_len", 50), "rsi_len": cfg.get("ab_rsi_len", 14),
        "rsi_gate": cfg.get("ab_rsi_gate", 55), "use_volume": cfg.get("ab_use_volume", False),
        "vol_mult": cfg.get("ab_vol_mult", 1.5), "atr_len": cfg.get("ab_atr_len", 14),
        "atr_mult": cfg.get("ab_atr_mult", 1.5), "r1": cfg.get("ab_r1", 1.0),
        "r2": cfg.get("ab_r2", 2.0), "r3": cfg.get("ab_r3", 3.0),
    }


def _ab_reset_state(st):
    st["ab_sl_price"] = None
    st["ab_tp1_price"] = None
    st["ab_tp2_price"] = None
    st["ab_tp3_price"] = None
    st["ab_tp1_done"] = False
    st["ab_tp2_done"] = False
    st["ab_be_done"] = False


def compute_ab_breakout_signals(highs, lows, closes, volumes, params):
    """Liefert (long_setup[], short_setup[], atr[]) je Kerze. long_setup/short_setup sind die
    ROHEN Bedingungen (Pine 'longSetup'/'shortSetup', noch KEINE Flanken-Erkennung) - der
    tatsaechliche Trigger ist erst 'jetzt erfuellt, letzte Kerze nicht' (siehe ab_poll_loop /
    _simulate_ab_trades, analog zu Pine's 'longSetup and not longSetup[1]')."""
    n = len(closes)
    lookback, fast_len, slow_len = params["lookback"], params["fast_len"], params["slow_len"]
    rsi_len, rsi_gate = params["rsi_len"], params["rsi_gate"]
    use_volume, vol_mult = params["use_volume"], params["vol_mult"]
    atr_len = params["atr_len"]

    ema_fast = _ema_series(closes, fast_len)
    ema_slow = _ema_series(closes, slow_len)
    rsi = compute_rsi(closes, rsi_len)
    atr = compute_atr(highs, lows, closes, atr_len)
    _, highs_roll_max = _rolling_min_max(highs, lookback)
    lows_roll_min, _ = _rolling_min_max(lows, lookback)
    vol_avg = _sma_series(volumes, 20) if volumes else [0.0] * n

    warmup = max(slow_len, lookback, atr_len, rsi_len)
    long_setup = [False] * n
    short_setup = [False] * n
    for i in range(n):
        if i < warmup or i < 1:
            continue
        if atr[i] is None or atr[i] <= 0:
            continue
        # [1]-Verschiebung wie im Original: das Range-Hoch/-Tief und der Volumen-Durchschnitt
        # der VORHERIGEN Kerze werden gegen den AKTUELLEN Schlusskurs geprueft, damit die
        # gerade schliessende Kerze ihre eigene Range nicht mitzaehlt (sonst waere ein Ausbruch
        # trivial immer "wahr").
        upper = highs_roll_max[i - 1]
        lower = lows_roll_min[i - 1]
        if upper is None or lower is None:
            continue
        if use_volume:
            v_avg_prev = vol_avg[i - 1] if i - 1 < len(vol_avg) else 0.0
            vol_ok = v_avg_prev is not None and v_avg_prev > 0 and volumes[i] >= v_avg_prev * vol_mult
        else:
            vol_ok = True

        long_setup[i] = (closes[i] > upper and closes[i] > ema_fast[i] and ema_fast[i] > ema_slow[i]
                          and rsi[i] >= rsi_gate and vol_ok)
        short_setup[i] = (closes[i] < lower and closes[i] < ema_fast[i] and ema_fast[i] < ema_slow[i]
                           and rsi[i] <= 100 - rsi_gate and vol_ok)
    return long_setup, short_setup, atr


async def check_ab_sl(symbol, price):
    """Wechsel-Modus: optionaler fester Dollar-SL (ab_sl_enabled/ab_sl_manual_usd) und optional
    'SL auf Einstieg' (ab_be_enabled/ab_be_trigger_usd): sobald die Position um den eingestellten
    Dollar-Betrag im Gewinn ist (Preisabstand = Betrag / Positionsgroesse, wie beim SL), wird der SL auf
    den Einstiegskurs gesetzt (Break-Even) - auch wenn der feste $-SL abgeschaltet ist, dann entsteht
    der SL erst mit dem Break-Even. Wird bei jedem Loop-Durchlauf gegen den Live-Preis geprueft
    (unabhaengig vom Kerzen-Abruf, siehe ab_poll_loop). Schlaegt der Exit fehl (Position bleibt
    offen), bleibt der SL bestehen und wird beim naechsten Durchlauf erneut versucht - statt ihn
    faelschlich als erledigt zu vergessen."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if st["position"] is None or price is None:
        return
    pos = st["position"]
    if not cfg.get("ab_sl_enabled", True) and not st.get("ab_be_done"):
        st["ab_sl_price"] = None  # SL wurde bei offener Position abgeschaltet (ein Break-Even-SL bleibt)
    if cfg.get("ab_be_enabled", False) and not st.get("ab_be_done"):
        size = st.get("total_coin_size") or 0
        entry_ref = st.get("avg_entry_price")
        if size > 0 and entry_ref:
            dist_be = cfg.get("ab_be_trigger_usd", 5.0) / size
            reached = price >= entry_ref + dist_be if pos == "long" else price <= entry_ref - dist_be
            if reached:
                st["ab_sl_price"] = entry_ref
                st["ab_be_done"] = True
                debug_log(f"📡 [{symbol}] Al-Shatri Breakout: ${cfg.get('ab_be_trigger_usd', 5.0)} Gewinn erreicht - SL auf Einstieg ({round(entry_ref, 4)}) gesetzt")
    sl_price = st.get("ab_sl_price")
    if sl_price is None:
        return
    hit_sl = (pos == "long" and price <= sl_price) or (pos == "short" and price >= sl_price)
    if not hit_sl:
        return
    reason = "BREAKEVEN" if st.get("ab_be_done") else "SL"
    debug_log(f"🚪 [{symbol}] Al-Shatri Breakout {reason}: {pos.upper()} @ {price} (Ziel war {round(sl_price, 4)})")
    await execute_exit(symbol, price, reason)
    if st["position"] is None:
        st["ab_sl_cooldown_until"] = time.time() + cfg.get("ab_sl_cooldown_seconds", 30)
        _ab_reset_state(st)


async def _check_ab_flip(symbol, buy_signal, sell_signal, price):
    """Wechsel-System: immer im Markt. Ein Signal in Richtung der schon offenen Position tut
    nichts; das Gegen-Signal schliesst die Position (Grund 'AB-FLIP') und oeffnet im selben Schritt
    die Gegenrichtung - der erste Buy bleibt also offen, bis das erste Sell kommt, usw.
    Richtung 'nur Long'/'nur Short': das Gegen-Signal schliesst weiterhin, eroeffnet aber keine
    Position in der gesperrten Richtung (danach flach bis zum naechsten erlaubten Signal).
    Alle Filter (EMA/RSI/Volumen/SuperTrend/ASO) wirken bereits VOR dieser Funktion auf die
    Setup-Serien - ein vom Filter blockiertes Gegen-Signal dreht die Position also nicht.
    Nach dem Einstieg wird (falls aktiv) der feste Dollar-SL gesetzt: SL-Betrag / Positionsgroesse
    = Preisabstand, d.h. der Verlust der GESAMTEN Position betraegt beim SL genau den Betrag."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or price is None:
        return
    if buy_signal:
        target = "long"
    elif sell_signal:
        target = "short"
    else:
        return
    pos = st["position"]
    if pos == target:
        return  # schon in dieser Richtung offen - nur das Gegen-Signal zaehlt

    direction_mode = cfg.get("ab_direction_mode", "both")
    can_open = (direction_mode == "both"
                or (direction_mode == "long_only" and target == "long")
                or (direction_mode == "short_only" and target == "short"))

    if pos is not None:
        debug_log(f"🔄 [{symbol}] Al-Shatri Breakout Wechsel: {pos.upper()} -> {target.upper() if can_open else 'FLACH'} @ {price}")
        await execute_exit(symbol, price, "AB-FLIP")
        if st["position"] is not None:
            # Exit fehlgeschlagen (Details im execute_exit-Log) - Position bleibt offen, deshalb
            # KEIN Gegen-Einstieg, sonst waeren beide Richtungen gleichzeitig im Bestand.
            return
        _ab_reset_state(st)
    elif time.time() < st.get("ab_sl_cooldown_until", 0.0):
        return  # flach nach einem SL - Cooldown laeuft noch

    if not can_open:
        return
    debug_log(f"📡 [{symbol}] Al-Shatri Breakout Signal: {target.upper()} @ {price}")
    await execute_entry(symbol, target, price, is_add_on=False)
    if st["position"] is None:
        return  # Einstieg (z.B. dry_run-Fehler) hat nicht geklappt
    _ab_reset_state(st)
    size = st.get("total_coin_size") or 0
    if cfg.get("ab_sl_enabled", True) and size > 0:
        entry_ref = st.get("avg_entry_price") or price
        dist_sl = cfg.get("ab_sl_manual_usd", 5.0) / size
        st["ab_sl_price"] = entry_ref - dist_sl if target == "long" else entry_ref + dist_sl


async def _check_ab_plan_entry(symbol, buy_signal, sell_signal, price, atr_now):
    """Plan-Modus (wie Original-Skript): Einstieg nur wenn flach (kein 'active'-Plan laeuft) - das
    Original zeichnet waehrend eines laufenden Plans keine neuen Linien. SL/TP1/TP2/TP3 werden
    einmalig aus dem ATR-Risk-Abstand zum Einstiegszeitpunkt berechnet (kein Nachziehen ausser den
    beiden abschaltbaren SL-Stufen nach TP1/TP2, siehe check_ab_sl_tp)."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or st["position"] is not None or price is None:
        return
    if time.time() < st.get("ab_sl_cooldown_until", 0.0):
        return
    direction_mode = cfg.get("ab_direction_mode", "both")
    if direction_mode == "long_only":
        sell_signal = False
    elif direction_mode == "short_only":
        buy_signal = False
    if not (buy_signal or sell_signal):
        return
    if atr_now is None or atr_now <= 0:
        return
    direction = "long" if buy_signal else "short"
    debug_log(f"📡 [{symbol}] Al-Shatri Breakout Signal: {direction.upper()} @ {price}")
    await execute_entry(symbol, direction, price, is_add_on=False)
    if st["position"] is None:
        return  # Einstieg (z.B. dry_run-Fehler) hat nicht geklappt
    _ab_reset_state(st)
    params = _ab_effective_params(cfg)
    risk = atr_now * params["atr_mult"]
    st["ab_sl_price"] = price - risk if direction == "long" else price + risk
    r1, r2, r3 = params["r1"], params["r2"], params["r3"]
    if direction == "long":
        st["ab_tp1_price"] = price + risk * r1
        st["ab_tp2_price"] = price + risk * r2
        st["ab_tp3_price"] = price + risk * r3
    else:
        st["ab_tp1_price"] = price - risk * r1
        st["ab_tp2_price"] = price - risk * r2
        st["ab_tp3_price"] = price - risk * r3


async def check_ab_entry(symbol, buy_signal, sell_signal, price, atr_now):
    """Waehlt je nach ab_exit_mode: 'plan' (ATR-SL + TP1/TP2/TP3 wie im Original-Skript) oder
    'flip' (Standard: Wechsel bei Gegen-Signal + optionaler fester Dollar-SL)."""
    if BOTS[symbol]["config"].get("ab_exit_mode", "flip") == "plan":
        await _check_ab_plan_entry(symbol, buy_signal, sell_signal, price, atr_now)
    else:
        await _check_ab_flip(symbol, buy_signal, sell_signal, price)


async def check_ab_sl_tp(symbol, price):
    """SL zuerst, dann TP1 (Teilverkauf + optional SL->Break-Even), TP2 (weiterer Teilverkauf +
    optional SL->TP1), TP3 (Rest schliessen) - identisches Muster zu check_ht_sl_tp, nur mit
    zwei EINZELN abschaltbaren Nachzieh-Stufen statt fest immer Break-Even bei TP1."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if st["position"] is None or price is None:
        return
    pos = st["position"]

    sl_price = st.get("ab_sl_price")
    if sl_price is not None:
        hit_sl = (pos == "long" and price <= sl_price) or (pos == "short" and price >= sl_price)
        if hit_sl:
            if st.get("ab_tp2_done"):
                reason = "SL-AUF-TP1"
            elif st.get("ab_tp1_done"):
                reason = "BREAKEVEN"
            else:
                reason = "SL"
            debug_log(f"🚪 [{symbol}] Al-Shatri Breakout {reason}: {pos.upper()} @ {price} (Ziel war {round(sl_price, 4)})")
            await execute_exit(symbol, price, reason)
            st["ab_sl_cooldown_until"] = time.time() + cfg.get("ab_sl_cooldown_seconds", 30)
            _ab_reset_state(st)
            return

    # TP1/TP2 faellt bewusst OHNE Return direkt zur naechsten Stufe durch, falls der Kurs seit dem
    # letzten Check (alle ~5s live, oder innerhalb einer Kerze im Backtest) so weit gesprungen ist,
    # dass mehrere Ziele auf einmal erreicht wurden - vorher wurde pro Aufruf nur EINE Stufe
    # verarbeitet, wodurch eine uebersprungene Stufe (z.B. TP2) nie nachgeholt wurde, wenn der Kurs
    # bis zum naechsten Check schon wieder zurueckgelaufen war.
    if not st.get("ab_tp1_done") and st.get("ab_tp1_price") is not None:
        tp1_price = st["ab_tp1_price"]
        if (pos == "long" and price >= tp1_price) or (pos == "short" and price <= tp1_price):
            fraction = cfg.get("ab_tp1_close_pct", 33) / 100
            ok = await execute_partial_exit(symbol, price, fraction, "TP1")
            if ok:
                st["ab_tp1_done"] = True
                if cfg.get("ab_sl_to_breakeven_on_tp1", True):
                    st["ab_sl_price"] = st["avg_entry_price"]
                    debug_log(f"📡 [{symbol}] Al-Shatri Breakout TP1 erreicht - SL auf Break-Even ({round(st['avg_entry_price'],4)}) gesetzt")
                else:
                    debug_log(f"📡 [{symbol}] Al-Shatri Breakout TP1 erreicht - SL-Nachzug deaktiviert, SL bleibt unveraendert")
            else:
                return  # Teilverkauf fehlgeschlagen - nicht so tun als waere TP1 schon durch
        else:
            return  # TP1 noch nicht erreicht -> TP2/TP3 koennen es dann erst recht nicht sein

    if not st.get("ab_tp2_done") and st.get("ab_tp2_price") is not None:
        tp2_price = st["ab_tp2_price"]
        if (pos == "long" and price >= tp2_price) or (pos == "short" and price <= tp2_price):
            fraction = cfg.get("ab_tp2_close_pct", 50) / 100
            ok = await execute_partial_exit(symbol, price, fraction, "TP2")
            if ok:
                st["ab_tp2_done"] = True
                if cfg.get("ab_sl_to_tp1_on_tp2", True) and st.get("ab_tp1_price") is not None:
                    st["ab_sl_price"] = st["ab_tp1_price"]
                    debug_log(f"📡 [{symbol}] Al-Shatri Breakout TP2 erreicht - SL auf TP1 ({round(st['ab_tp1_price'],4)}) gesetzt")
                else:
                    debug_log(f"📡 [{symbol}] Al-Shatri Breakout TP2 erreicht - SL-Nachzug deaktiviert, SL bleibt unveraendert")
            else:
                return
        else:
            return

    tp3_price = st.get("ab_tp3_price")
    if tp3_price is not None:
        if (pos == "long" and price >= tp3_price) or (pos == "short" and price <= tp3_price):
            debug_log(f"🚪 [{symbol}] Al-Shatri Breakout TP3 (Rest): {pos.upper()} @ {price}")
            await execute_exit(symbol, price, "TP3")
            _ab_reset_state(st)


def _simulate_ab_flip_trades(candles, cfg, long_setup, short_setup, warmup):
    """Backtest-Gegenstueck zu check_ab_entry/check_ab_sl: Wechsel-System (Gegen-Signal schliesst zum
    Schlusskurs und oeffnet die Gegenrichtung) plus optionaler fester Dollar-SL (Preisabstand =
    SL-Betrag / Positionsgroesse, Ausloesung ueber Hoch/Tief der Kerze, Ausfuehrung zum SL-Preis) mit
    Cooldown nach SL. Der Einstieg passiert zum Schlusskurs der Signal-Kerze; der SL wird erst ab der
    NAECHSTEN Kerze geprueft. Am Ende des Zeitraums wird eine offene Position zum letzten Schlusskurs
    bewertet (END-OF-BACKTEST)."""
    ts, h, l, c = candles[0], candles[2], candles[3], candles[4]
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    sl_enabled = cfg.get("ab_sl_enabled", True)
    sl_manual_usd = cfg.get("ab_sl_manual_usd", 5.0)
    sl_cooldown_ms = cfg.get("ab_sl_cooldown_seconds", 30) * 1000
    direction_mode = cfg.get("ab_direction_mode", "both")
    be_enabled = cfg.get("ab_be_enabled", False)
    be_trigger_usd = cfg.get("ab_be_trigger_usd", 5.0)

    position = None  # {"dir","entry","size","entry_i","sl_price","be_done"}
    trades = []
    sl_cooldown_until_ts = None

    for i in range(max(warmup, 1), n):
        if position is not None:
            pdir, entry = position["dir"], position["entry"]
            sl_price = position.get("sl_price")
            hit_sl = sl_price is not None and ((pdir == "long" and l[i] <= sl_price) or (pdir == "short" and h[i] >= sl_price))
            if hit_sl:
                reason = "BREAKEVEN" if position["be_done"] else "SL"
                _bt_close_trade(trades, pdir, entry, sl_price, position["size"], i, position["entry_i"], reason, ts=ts)
                position = None
                sl_cooldown_until_ts = ts[i] + sl_cooldown_ms
            elif be_enabled and not position["be_done"] and position["size"] > 0:
                # 'SL auf Einstieg' ab X $ Gewinn: Ausloesung ueber Hoch/Tief der Kerze; der neue SL gilt
                # (wie der urspruengliche SL und der SL-Nachzug im Plan-Modus) erst ab der NAECHSTEN Kerze.
                dist_be = be_trigger_usd / position["size"]
                if (pdir == "long" and h[i] >= entry + dist_be) or (pdir == "short" and l[i] <= entry - dist_be):
                    position["sl_price"] = entry
                    position["be_done"] = True

        if long_setup[i] and not long_setup[i - 1]:
            target = "long"
        elif short_setup[i] and not short_setup[i - 1]:
            target = "short"
        else:
            continue
        if position is not None and position["dir"] == target:
            continue
        price = c[i]
        if position is not None:
            _bt_close_trade(trades, position["dir"], position["entry"], price, position["size"], i, position["entry_i"], "AB-FLIP", ts=ts)
            position = None
        elif sl_cooldown_until_ts is not None and ts[i] < sl_cooldown_until_ts:
            continue  # flach nach einem SL - Cooldown laeuft noch
        can_open = (direction_mode == "both"
                    or (direction_mode == "long_only" and target == "long")
                    or (direction_mode == "short_only" and target == "short"))
        if not can_open:
            continue
        size = (margin * leverage) / price
        sl_price = None
        if sl_enabled and size > 0:
            dist_sl = sl_manual_usd / size
            sl_price = price - dist_sl if target == "long" else price + dist_sl
        position = {"dir": target, "entry": price, "size": size, "entry_i": i, "sl_price": sl_price, "be_done": False}

    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)

    return trades


def _simulate_ab_plan_trades(candles, cfg, long_setup, short_setup, atr, warmup):
    """Kern-Simulation fuer Al-Shatri Breakout - wie _simulate_halftrend_trades (TP1/TP2/TP3 als
    echte Teilverkaeufe), aber KEIN Flip-Exit (das Original bleibt bis SL/TP3 im Plan, ein neues
    Gegen-Signal wird waehrend 'active' schlicht ignoriert) und mit den zwei EINZELN abschaltbaren
    SL-Nachzieh-Stufen (Break-Even bei TP1, SL-auf-TP1 bei TP2) statt fest immer Break-Even.
    Plan-Modus (ab_exit_mode='plan'), Gegenstueck zu check_ab_sl_tp/_check_ab_plan_entry."""
    ts, o, h, l, c = candles[0], candles[1], candles[2], candles[3], candles[4]
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    params = _ab_effective_params(cfg)
    r1, r2, r3, atr_mult = params["r1"], params["r2"], params["r3"], params["atr_mult"]
    tp1_frac = cfg.get("ab_tp1_close_pct", 33) / 100
    tp2_frac = cfg.get("ab_tp2_close_pct", 50) / 100
    sl_to_be_on_tp1 = cfg.get("ab_sl_to_breakeven_on_tp1", True)
    sl_to_tp1_on_tp2 = cfg.get("ab_sl_to_tp1_on_tp2", True)
    sl_cooldown_ms = cfg.get("ab_sl_cooldown_seconds", 30) * 1000
    direction_mode = cfg.get("ab_direction_mode", "both")

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
                reason = "SL-AUF-TP1" if position["tp2_done"] else ("BREAKEVEN" if position["tp1_done"] else "SL")
                _bt_close_trade(trades, pdir, entry, sl_price, position["size"], i, position["entry_i"], reason, ts=ts)
                position = None
                sl_cooldown_until_ts = ts[i] + sl_cooldown_ms
            else:
                # TP1/TP2/TP3 faellt bewusst OHNE elif/Abbruch durch: da alle drei Ziele in
                # dieselbe Richtung geordnet sind (TP1 naeher am Einstieg als TP2 als TP3), kann
                # eine einzelne grosse Kerze (voller Range in h[i]/l[i]) rechnerisch eindeutig
                # mehrere Stufen auf einmal erreichen - das darf nicht auf die naechste Kerze
                # verschoben werden, sonst fehlt z.B. TP2 komplett, wenn die naechste Kerze schon
                # wieder zurücklaeuft (siehe check_ab_sl_tp fuer dieselbe Korrektur live).
                if not position["tp1_done"] and position.get("tp1_price") is not None:
                    tp1_price = position["tp1_price"]
                    if (pdir == "long" and h[i] >= tp1_price) or (pdir == "short" and l[i] <= tp1_price):
                        close_size = position["size"] * tp1_frac
                        _bt_close_trade(trades, pdir, entry, tp1_price, close_size, i, position["entry_i"], "TP1", ts=ts)
                        position["size"] -= close_size
                        position["tp1_done"] = True
                        if sl_to_be_on_tp1:
                            position["sl_price"] = entry

                if position is not None and position["tp1_done"] and not position["tp2_done"] and position.get("tp2_price") is not None:
                    tp2_price = position["tp2_price"]
                    if (pdir == "long" and h[i] >= tp2_price) or (pdir == "short" and l[i] <= tp2_price):
                        close_size = position["size"] * tp2_frac
                        _bt_close_trade(trades, pdir, entry, tp2_price, close_size, i, position["entry_i"], "TP2", ts=ts)
                        position["size"] -= close_size
                        position["tp2_done"] = True
                        if sl_to_tp1_on_tp2 and position.get("tp1_price") is not None:
                            position["sl_price"] = position["tp1_price"]

                if position is not None and position["tp1_done"] and position["tp2_done"] and position.get("tp3_price") is not None:
                    tp3_price = position["tp3_price"]
                    if (pdir == "long" and h[i] >= tp3_price) or (pdir == "short" and l[i] <= tp3_price):
                        _bt_close_trade(trades, pdir, entry, tp3_price, position["size"], i, position["entry_i"], "TP3", ts=ts)
                        position = None

        buy_signal = long_setup[i] and not long_setup[i - 1]
        sell_signal = short_setup[i] and not short_setup[i - 1]
        if direction_mode == "long_only":
            sell_signal = False
        elif direction_mode == "short_only":
            buy_signal = False

        in_sl_cooldown = sl_cooldown_until_ts is not None and ts[i] < sl_cooldown_until_ts
        if position is None and not in_sl_cooldown and (buy_signal or sell_signal) and atr[i] and atr[i] > 0:
            direction = "long" if buy_signal else "short"
            size = (margin * leverage) / price
            risk = atr[i] * atr_mult
            sl_price = price - risk if direction == "long" else price + risk
            if direction == "long":
                tp1_price, tp2_price, tp3_price = price + risk * r1, price + risk * r2, price + risk * r3
            else:
                tp1_price, tp2_price, tp3_price = price - risk * r1, price - risk * r2, price - risk * r3
            position = {"dir": direction, "entry": price, "size": size, "entry_i": i,
                        "sl_price": sl_price, "tp1_price": tp1_price, "tp2_price": tp2_price, "tp3_price": tp3_price,
                        "tp1_done": False, "tp2_done": False}

    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)

    return trades


def _simulate_ab_trades(candles, cfg, long_setup, short_setup, atr, warmup):
    """Waehlt je nach ab_exit_mode die Simulation: 'plan' (ATR-SL + TP1/TP2/TP3 wie im Original) oder
    'flip' (Standard: Wechsel bei Gegen-Signal + optionaler fester Dollar-SL)."""
    if cfg.get("ab_exit_mode", "flip") == "plan":
        return _simulate_ab_plan_trades(candles, cfg, long_setup, short_setup, atr, warmup)
    return _simulate_ab_flip_trades(candles, cfg, long_setup, short_setup, warmup)


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


def _sma_series(values, length):
    n = len(values)
    out = [0.0] * n
    for i in range(n):
        start = max(0, i - length + 1)
        window = values[start:i + 1]
        out[i] = sum(window) / len(window)
    return out




# ============================================================
# Wiederhergestellt: compute_adx/compute_macd_line_and_signal - werden vom neuen
# generischen Filter-Baukasten (ADX-/MACD-Filter) unten genutzt.
# ============================================================

def compute_adx(highs, lows, closes, period=14):
    """Klassischer Wilder-ADX/DMI (wie Pine's ta.dmi), Wilder-RMA-Glaettung wie compute_atr.
    Gibt (adx, plus_di, minus_di) zurueck - Standardnutzung als Trendfilter: ADX > Schwelle
    (z.B. 20) heisst 'genug Trendstaerke vorhanden', +DI > -DI heisst 'Richtung ist bullisch',
    -DI > +DI heisst 'Richtung ist bearisch'. Wird hier als optionaler Long/Short-Filter fuer
    Kerzen-DNA genutzt (siehe cd_adx_filter_enabled)."""
    n = len(closes)
    if n < 2:
        return [0.0] * n, [0.0] * n, [0.0] * n

    tr = [highs[0] - lows[0]] + [0.0] * (n - 1)
    plus_dm = [0.0] * n
    minus_dm = [0.0] * n
    for i in range(1, n):
        tr[i] = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        up_move = highs[i] - highs[i - 1]
        down_move = lows[i - 1] - lows[i]
        plus_dm[i] = up_move if (up_move > down_move and up_move > 0) else 0.0
        minus_dm[i] = down_move if (down_move > up_move and down_move > 0) else 0.0

    def _wilder_smooth(values):
        out = [values[0]] * n
        for i in range(1, n):
            if i < period:
                out[i] = sum(values[:i + 1]) / (i + 1)
            else:
                out[i] = out[i - 1] - (out[i - 1] / period) + values[i]
        return out

    tr_smooth = _wilder_smooth(tr)
    plus_dm_smooth = _wilder_smooth(plus_dm)
    minus_dm_smooth = _wilder_smooth(minus_dm)

    plus_di = [100 * plus_dm_smooth[i] / tr_smooth[i] if tr_smooth[i] > 0 else 0.0 for i in range(n)]
    minus_di = [100 * minus_dm_smooth[i] / tr_smooth[i] if tr_smooth[i] > 0 else 0.0 for i in range(n)]
    dx = [100 * abs(plus_di[i] - minus_di[i]) / (plus_di[i] + minus_di[i]) if (plus_di[i] + minus_di[i]) > 0 else 0.0 for i in range(n)]

    adx = [dx[0]] * n
    for i in range(1, n):
        if i < period:
            adx[i] = sum(dx[:i + 1]) / (i + 1)
        else:
            adx[i] = (adx[i - 1] * (period - 1) + dx[i]) / period

    return adx, plus_di, minus_di


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




# ============================================================================
# GENERISCHER FILTER-BAUKASTEN
# Fuer neue Strategien (z.B. RSI): ein Signalgeber liefert nur long_raw/short_raw
# (die reine Handelsidee), und haengt hier beliebig viele Filter per UND dran, ohne
# selbst irgendetwas ueber SuperTrend/ADX/MACD wissen zu muessen. Jeder Filter gibt
# IMMER dieselbe Form zurueck: (long_ok[], short_ok[]) - True/False je Kerze, ob die
# jeweilige Richtung laut diesem Filter gerade erlaubt ist. combine_filters() legt
# beliebig viele davon per UND auf das Rohsignal.
#
# WICHTIG: _fetch_trend_filter_backtest_candles()/_fetch_trend_filter_candles_live()
# (siehe oben, urspruenglich fuer Al-Shatri Breakout gebaut) sind bereits generisch -
# sie nehmen Zeiteinheit/ATR-Periode als normale Parameter entgegen, nichts davon ist
# an "ab_" gebunden. Der SuperTrend-Filter unten nutzt sie direkt weiter, ohne sie zu
# veraendern - Al-Shatri bleibt dadurch komplett unberuehrt.
# ============================================================================

def combine_filters(long_raw, short_raw, filters):
    """filters: Liste von (long_ok, short_ok)-Tupeln (gleiche Laenge wie long_raw/short_raw).
    Kombiniert alles per UND - ein einzelner Filter mit False an einer Kerze blockiert die
    jeweilige Richtung, unabhaengig davon was die anderen Filter oder das Rohsignal sagen.
    Leere filters-Liste gibt long_raw/short_raw unveraendert zurueck."""
    n = len(long_raw)
    long_final = list(long_raw)
    short_final = list(short_raw)
    for long_ok, short_ok in filters:
        long_final = [long_final[i] and long_ok[i] for i in range(n)]
        short_final = [short_final[i] and short_ok[i] for i in range(n)]
    return long_final, short_final


def compute_supertrend_filter_series(candles_htf, multiplier, atr_period):
    """SuperTrend-Trendfilter auf einer (meist hoeheren) Zeiteinheit - reiner Rechenteil, wenn die
    Filter-Kerzen schon vorliegen (Backtest: ueber _fetch_trend_filter_backtest_candles() holen,
    dann hier + _trend_filter_ok_series() fuer die Kerzen-Ausrichtung nutzen - siehe
    compute_supertrend_filter_backtest() unten fuer die fertige Komplett-Variante).
    candles_htf = (ts, o, h, l, c) der Filter-Zeiteinheit. Gibt (long_ok, short_ok) OHNE
    Ausrichtung an eine andere Zeitreihe zurueck (Kurs ueber der Linie = long_ok)."""
    _ts, _o, h, l, c = candles_htf
    line, _ = compute_diamond_supertrend(h, l, c, multiplier, atr_period)
    n = len(c)
    long_ok = [line[i] is not None and c[i] > line[i] for i in range(n)]
    short_ok = [line[i] is not None and c[i] < line[i] for i in range(n)]
    return long_ok, short_ok


async def compute_supertrend_filter_backtest(symbol, cfg, base_ts, resolution, multiplier, atr_period):
    """Fertiger SuperTrend-Trendfilter fuers Backtest, ausgerichtet auf base_ts (die Kerzen des
    Signalgebers). resolution == 'same' oder leer/None: der Filter laeuft auf DENSELBEN Kerzen wie
    das Signal (Aufrufer muss dann selbst compute_diamond_supertrend auf seinen eigenen h/l/c
    anwenden - hier nicht sinnvoll ohne die Original-Kerzen). Gibt (long_ok, short_ok, error)
    zurueck; error ist ein fertiger Fehlertext oder None."""
    if resolution in (None, "", "same"):
        return None, None, "SuperTrend-Filter mit 'gleiche Zeiteinheit' braucht die eigenen Kerzen des Signalgebers - resolution explizit angeben."
    tf_candles, err = await _fetch_trend_filter_backtest_candles(symbol, cfg, base_ts, resolution, atr_period)
    if err:
        return None, None, err
    long_ok, short_ok = _trend_filter_ok_series(base_ts, tf_candles, multiplier, atr_period)
    return long_ok, short_ok, None


async def compute_supertrend_filter_live(symbol, st, cfg, resolution, multiplier, atr_period, n_bars):
    """Live-Gegenstueck: liefert (long_ok, short_ok) als [wert]*n_bars (der aktuelle SuperTrend-Stand
    gilt fuer alle 'n_bars' zuletzt verarbeiteten Signal-Kerzen, wie bei Al-Shatri) - oder
    (None, None), wenn (noch) nicht genug Filter-Kerzen da sind; der Aufrufer soll den Filter dann
    wie ueblich durchlassen (alles True)."""
    tf_closed = await _fetch_trend_filter_candles_live(symbol, st, cfg, resolution, atr_period)
    if not tf_closed:
        return None, None
    tf_h, tf_l, tf_c = tf_closed
    line, _ = compute_diamond_supertrend(tf_h, tf_l, tf_c, multiplier, atr_period)
    bullish_now = line[-1] is not None and tf_c[-1] > line[-1]
    return [bullish_now] * n_bars, [not bullish_now] * n_bars


def compute_adx_filter_series(candles, length, threshold, directional=True):
    """ADX/DMI-Trendfilter auf den EIGENEN Kerzen des Signalgebers (kein HTF-Fetch noetig - ADX
    braucht anders als SuperTrend ueblicherweise keine hoehere Zeiteinheit). candles = (ts,o,h,l,c).
    directional=True (Standard): long_ok nur wenn ADX>Schwelle UND +DI>-DI (Trend UND Richtung
    stimmen), short_ok umgekehrt. directional=False: long_ok==short_ok==(ADX>Schwelle) - reiner
    Trendstaerke-Filter ohne Richtungsvorgabe (z.B. um Seitwaerts-Phasen generell zu blocken).
    Nimmt sowohl 5er- (ts,o,h,l,c) als auch 6er-Tupel (ts,o,h,l,c,v) entgegen - ein eventuell
    mitgegebenes Volumen wird schlicht ignoriert (ADX braucht keins)."""
    _ts, _o, h, l, c = candles[0], candles[1], candles[2], candles[3], candles[4]
    adx, plus_di, minus_di = compute_adx(h, l, c, length)
    n = len(c)
    if directional:
        long_ok = [adx[i] is not None and adx[i] > threshold and plus_di[i] > minus_di[i] for i in range(n)]
        short_ok = [adx[i] is not None and adx[i] > threshold and minus_di[i] > plus_di[i] for i in range(n)]
    else:
        ok = [adx[i] is not None and adx[i] > threshold for i in range(n)]
        long_ok, short_ok = ok, list(ok)
    return long_ok, short_ok


def compute_macd_filter_series(candles, fast_len, slow_len, signal_len):
    """MACD-Trendfilter auf den EIGENEN Kerzen des Signalgebers. long_ok wenn die MACD-Linie ueber
    ihrer Signal-Linie steht (bullischer Zustand), short_ok umgekehrt - kein reiner Crossover-Moment,
    sondern der jeweils AKTUELLE Zustand (wie beim SuperTrend), damit der Filter bei jedem
    Signalgeber-Bar sofort eine Antwort hat statt nur an Kreuzungs-Bars. Nimmt sowohl 5er- als
    auch 6er-Tupel (mit Volumen) entgegen - siehe compute_adx_filter_series."""
    _ts, _o, _h, _l, c = candles[0], candles[1], candles[2], candles[3], candles[4]
    macd_line, signal_line = compute_macd_line_and_signal(c, fast_len, slow_len, signal_len)
    n = len(c)
    long_ok = [macd_line[i] is not None and signal_line[i] is not None and macd_line[i] > signal_line[i] for i in range(n)]
    short_ok = [macd_line[i] is not None and signal_line[i] is not None and macd_line[i] < signal_line[i] for i in range(n)]
    return long_ok, short_ok


def compute_rsi_filter_series(candles, length, os_level, ob_level):
    """RSI-Ueberdehnungsfilter (portiert aus dem Pine-Skript "MVWAP-MF Osc"): long_ok nur wenn der
    RSI unter os_level liegt (ueberverkauft -> Long-Reversal-Chance erlaubt), short_ok nur wenn er
    ueber ob_level liegt (ueberkauft -> Short-Reversal-Chance erlaubt). Reiner Zustands-Filter wie
    ADX/MACD oben - kein Crossover-Moment, gilt auf jeder Kerze einzeln. Nimmt 5er- oder 6er-Tupel
    entgegen (Volumen wird ignoriert)."""
    c = candles[4]
    rsi = compute_rsi(c, length)
    n = len(c)
    long_ok = [rsi[i] < os_level for i in range(n)]
    short_ok = [rsi[i] > ob_level for i in range(n)]
    return long_ok, short_ok


def _rolling_vw_mean_dev(closes, volumes, length):
    """Gleitender volumengewichteter Mittelwert + mittlere absolute Abweichung ueber ein Fenster
    der letzten `length` Kerzen (inkl. der aktuellen) - direkter Port von Pine's pine_vwmean()/
    pine_vwavdev() aus dem "[Hoss] VWAP+RSI+Hull+DI System"-Skript. Faellt bei Volumen-Summe 0
    (z.B. ganz am Anfang der Historie) auf den reinen Schlusskurs / Abweichung 0 zurueck."""
    n = len(closes)
    mean = [0.0] * n
    dev = [0.0] * n
    for i in range(n):
        start = max(0, i - length + 1)
        window_c = closes[start:i + 1]
        window_v = volumes[start:i + 1]
        w_sum = sum(window_v)
        if w_sum <= 0:
            mean[i] = closes[i]
            dev[i] = 0.0
            continue
        m = sum(cw * cd for cw, cd in zip(window_v, window_c)) / w_sum
        mean[i] = m
        dev[i] = sum(cw * abs(cd - m) for cw, cd in zip(window_v, window_c)) / w_sum
    return mean, dev


def compute_cloud_filter_series(candles, length, dev_mult, touch_arm=False):
    """"Cloud"-Filter (portiert aus dem "[Hoss] VWAP+RSI+Hull+DI System"-Skript): volumengewichtete
    Abweichungsbaender um einen gleitenden VWAP. Beruehrt der Kurs das obere Band, ist ab da nur
    noch Short erlaubt (bis das untere Band beruehrt wird und umgekehrt fuer Long) - der Zustand
    ("scharf geschaltet") bleibt ueber viele Kerzen bestehen, nicht nur auf der Beruehrungs-Kerze
    selbst. touch_arm=False (Standard): Kerzenschluss ueber/unter dem Band scharf schaltet;
    touch_arm=True: schon eine Docht-Beruehrung (High/Low) reicht. Braucht ein 6er-Tupel MIT
    Volumen (ts,o,h,l,c,v)."""
    _ts, _o, h, l, c, v = candles
    mean, dev = _rolling_vw_mean_dev(c, v, length)
    n = len(c)
    upper = [mean[i] + dev[i] * dev_mult for i in range(n)]
    lower = [mean[i] - dev[i] * dev_mult for i in range(n)]
    band_state = 0
    long_ok = [False] * n
    short_ok = [False] * n
    for i in range(n):
        touched_upper = (h[i] >= upper[i]) if touch_arm else (c[i] > upper[i])
        touched_lower = (l[i] <= lower[i]) if touch_arm else (c[i] < lower[i])
        if touched_upper:
            band_state = 1
        if touched_lower:
            band_state = -1
        short_ok[i] = band_state == 1
        long_ok[i] = band_state == -1
    return long_ok, short_ok


# ============================================================================
# RSI Signal (kauf bei ueberverkauft, verkauf bei ueberkauft) - erste Strategie nach dem neuen
# Baukasten-Prinzip: NUR die Signal-Bedingung ist strategie-eigen (unten, compute_rsi_signals).
# Ein-/Ausstieg ist das exakt gleiche, bereits getestete Wechsel-System wie bei Al-Shatri Breakout
# (_check_ab_flip/check_ab_sl als Vorlage) - fester Dollar-SL optional, "SL auf Einstieg" optional.
# Trendfilter (SuperTrend/ADX/MACD) kommen unveraendert aus dem generischen Filter-Baukasten oben -
# fuer eine kuenftige Strategie reicht es, eine eigene compute_X_signals() zu schreiben und diese
# Ein-/Ausstiegs- und Filter-Bausteine wiederzuverwenden, statt alles neu zu bauen.
# ============================================================================

def compute_rsi_signals(closes, length, oversold, overbought):
    """Reine Signal-Idee, sonst nichts: RSI < oversold -> long erlaubt, RSI > overbought -> short
    erlaubt. Keine Filter, keine Ausstiegslogik - die kommen separat dazu (Filter-Baukasten bzw.
    das Wechsel-System unten)."""
    rsi = compute_rsi(closes, length)
    n = len(closes)
    long_raw = [rsi[i] is not None and rsi[i] < oversold for i in range(n)]
    short_raw = [rsi[i] is not None and rsi[i] > overbought for i in range(n)]
    return long_raw, short_raw, rsi


def _rsi_reset_state(st):
    st["rsi_sl_price"] = None
    st["rsi_tp_price"] = None
    st["rsi_be_done"] = False


async def check_rsi_sl(symbol, price):
    """Identisch zu check_ab_sl (siehe dort fuer Kommentare) - nur mit rsi_-Config-Feldern, plus
    optionalem festen Dollar-TP (rsi_tp_enabled/rsi_tp_manual_usd): schliesst die GANZE Position,
    kein Teilverkauf. Bei Konflikt (beide in derselben Pruefung erreichbar) gewinnt der SL zuerst -
    wie ueberall sonst im Bot."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if st["position"] is None or price is None:
        return
    pos = st["position"]
    if not cfg.get("rsi_sl_enabled", True) and not st.get("rsi_be_done"):
        st["rsi_sl_price"] = None
    if cfg.get("rsi_be_enabled", False) and not st.get("rsi_be_done"):
        size = st.get("total_coin_size") or 0
        entry_ref = st.get("avg_entry_price")
        if size > 0 and entry_ref:
            dist_be = cfg.get("rsi_be_trigger_usd", 5.0) / size
            reached = price >= entry_ref + dist_be if pos == "long" else price <= entry_ref - dist_be
            if reached:
                st["rsi_sl_price"] = entry_ref
                st["rsi_be_done"] = True
                debug_log(f"📡 [{symbol}] RSI Signal: ${cfg.get('rsi_be_trigger_usd', 5.0)} Gewinn erreicht - SL auf Einstieg ({round(entry_ref, 4)}) gesetzt")
    sl_price = st.get("rsi_sl_price")
    hit_sl = sl_price is not None and ((pos == "long" and price <= sl_price) or (pos == "short" and price >= sl_price))
    if hit_sl:
        reason = "BREAKEVEN" if st.get("rsi_be_done") else "SL"
        debug_log(f"🚪 [{symbol}] RSI Signal {reason}: {pos.upper()} @ {price} (Ziel war {round(sl_price, 4)})")
        await execute_exit(symbol, price, reason)
        if st["position"] is None:
            st["rsi_sl_cooldown_until"] = time.time() + cfg.get("rsi_sl_cooldown_seconds", 30)
            _rsi_reset_state(st)
        return
    tp_price = st.get("rsi_tp_price")
    hit_tp = tp_price is not None and ((pos == "long" and price >= tp_price) or (pos == "short" and price <= tp_price))
    if hit_tp:
        debug_log(f"🎯 [{symbol}] RSI Signal TP: {pos.upper()} @ {price} (Ziel war {round(tp_price, 4)})")
        await execute_exit(symbol, price, "TP")
        if st["position"] is None:
            _rsi_reset_state(st)


async def check_rsi_entry(symbol, buy_signal, sell_signal, price):
    """Identisch zu _check_ab_flip (siehe dort fuer Kommentare) - nur mit rsi_-Config-Feldern."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or price is None:
        return
    if buy_signal:
        target = "long"
    elif sell_signal:
        target = "short"
    else:
        return
    pos = st["position"]
    if pos == target:
        return

    direction_mode = cfg.get("rsi_direction_mode", "both")
    can_open = (direction_mode == "both"
                or (direction_mode == "long_only" and target == "long")
                or (direction_mode == "short_only" and target == "short"))

    if pos is not None:
        debug_log(f"🔄 [{symbol}] RSI Signal Wechsel: {pos.upper()} -> {target.upper() if can_open else 'FLACH'} @ {price}")
        await execute_exit(symbol, price, "RSI-FLIP")
        if st["position"] is not None:
            return
        _rsi_reset_state(st)
    elif time.time() < st.get("rsi_sl_cooldown_until", 0.0):
        return

    if not can_open:
        return
    debug_log(f"📡 [{symbol}] RSI Signal: {target.upper()} @ {price}")
    await execute_entry(symbol, target, price, is_add_on=False)
    if st["position"] is None:
        return
    _rsi_reset_state(st)
    size = st.get("total_coin_size") or 0
    if cfg.get("rsi_sl_enabled", True) and size > 0:
        entry_ref = st.get("avg_entry_price") or price
        dist_sl = cfg.get("rsi_sl_manual_usd", 5.0) / size
        st["rsi_sl_price"] = entry_ref - dist_sl if target == "long" else entry_ref + dist_sl
    if cfg.get("rsi_tp_enabled", False) and size > 0:
        entry_ref = st.get("avg_entry_price") or price
        dist_tp = cfg.get("rsi_tp_manual_usd", 10.0) / size
        st["rsi_tp_price"] = entry_ref + dist_tp if target == "long" else entry_ref - dist_tp


async def _rsi_apply_filters(symbol, st, cfg, candles, long_raw, short_raw):
    """Wendet die im Formular aktivierten Filter (SuperTrend/ADX/MACD) per UND auf long_raw/
    short_raw an - live. Jeder Filter, der (noch) keine Daten liefern kann, laesst wie ueblich
    durch (alles True), statt das Signal fälschlich zu blockieren."""
    n = len(long_raw)
    active = []
    if cfg.get("rsi_supertrend_filter_enabled", False):
        lo, so = await compute_supertrend_filter_live(
            symbol, st, cfg, cfg.get("rsi_supertrend_filter_resolution", "15m"),
            cfg.get("rsi_supertrend_filter_multiplier", 3.0), cfg.get("rsi_supertrend_filter_atr_period", 10), n)
        if lo is not None:
            active.append((lo, so))
    if cfg.get("rsi_adx_filter_enabled", False):
        active.append(compute_adx_filter_series(
            candles, cfg.get("rsi_adx_filter_length", 14), cfg.get("rsi_adx_filter_threshold", 20),
            directional=cfg.get("rsi_adx_filter_directional", True)))
    if cfg.get("rsi_macd_filter_enabled", False):
        active.append(compute_macd_filter_series(
            candles, cfg.get("rsi_macd_filter_fast", 12), cfg.get("rsi_macd_filter_slow", 26), cfg.get("rsi_macd_filter_signal", 9)))
    if not active:
        return long_raw, short_raw
    return combine_filters(long_raw, short_raw, active)


async def rsi_poll_loop(symbol):
    """RSI Signal - Kerzenschluss-Signal (wie Al-Shatri), Wechsel-System als Ausstieg, optional
    SuperTrend-/ADX-/MACD-Filter aus dem generischen Baukasten oben."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "rsi_signal" and cfg["bot_active"]:
                resolution = cfg.get("rsi_resolution", "5m")
                length = cfg.get("rsi_length", 14)
                min_needed = length + 5
                needed_bars = min(1000, max(min_needed * 2, 200))
                st = b["state"]

                data = await fetch_candles_binance_multi(symbol, resolution, count_back=needed_bars, market_type=cfg.get("binance_market_type", "spot"))
                if data:
                    timestamps, opens, highs, lows, closes = data
                    closed_ts, closed_o, closed_h, closed_l, closed_c = timestamps[:-1], opens[:-1], highs[:-1], lows[:-1], closes[:-1]
                else:
                    closed_ts = None

                now = time.time()
                due_heartbeat = now - last_heartbeat > 300

                if st["position"] is not None and st["last_price"] is not None:
                    await check_rsi_sl(symbol, st["last_price"])

                if closed_ts and len(closed_c) > min_needed:
                    candle_age_seconds = (now * 1000 - closed_ts[-1]) / 1000
                    max_age_seconds = 300
                    if candle_age_seconds > max_age_seconds:
                        debug_log(f"⚠️ [{symbol}] RSI Signal: letzte Kerze wirkt veraltet ({round(candle_age_seconds)}s alt, Auflösung {resolution}) - überspringe Signal-Berechnung diesen Durchlauf.")
                    else:
                        last_ts = closed_ts[-1]
                        if last_ts != last_processed_ts:
                            last_processed_ts = last_ts
                            long_raw, short_raw, rsi_series = compute_rsi_signals(closed_c, length, cfg.get("rsi_oversold", 30), cfg.get("rsi_overbought", 70))
                            candles = (closed_ts, closed_o, closed_h, closed_l, closed_c)
                            long_final, short_final = await _rsi_apply_filters(symbol, st, cfg, candles, long_raw, short_raw)
                            buy_signal = long_final[-1]
                            sell_signal = short_final[-1]
                            st["rsi_last"] = rsi_series[-1]
                            await check_rsi_entry(symbol, buy_signal, sell_signal, closed_c[-1])
                        if due_heartbeat:
                            last_heartbeat = now
                            debug_log(f"💓 [{symbol}] RSI Signal aktiv: RSI={round(st.get('rsi_last') or 0, 1)}, "
                                      f"Preis={closed_c[-1]}, Kerzen={len(closed_c)}, bot_active={cfg['bot_active']}")
                elif due_heartbeat:
                    last_heartbeat = now
                    if not closed_ts:
                        debug_log(f"⏳ [{symbol}] RSI Signal wartet: keine Kerzen erhalten (Auflösung {resolution})")
                    else:
                        debug_log(f"⏳ [{symbol}] RSI Signal wartet: zu wenig Kerzen ({len(closed_c)}/{min_needed + 1} nötig)")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] RSI Signal-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

        await asyncio.sleep(5)


def _simulate_rsi_trades(candles, cfg, long_setup, short_setup):
    """Backtest-Simulation - identisch zu _simulate_ab_flip_trades (siehe dort fuer Kommentare),
    nur mit rsi_-Config-Feldern. Kein Plan-Modus - RSI Signal kennt nur das Wechsel-System.
    Optionaler fester Dollar-TP schliesst die GANZE Position (kein Teilverkauf) - bei Konflikt
    (SL und TP in derselben Kerze erreichbar) gewinnt der SL zuerst, wie live."""
    ts, h, l, c = candles[0], candles[2], candles[3], candles[4]
    n = len(c)
    margin, leverage = cfg["margin"], cfg["leverage"]
    sl_enabled = cfg.get("rsi_sl_enabled", True)
    sl_manual_usd = cfg.get("rsi_sl_manual_usd", 5.0)
    sl_cooldown_ms = cfg.get("rsi_sl_cooldown_seconds", 30) * 1000
    direction_mode = cfg.get("rsi_direction_mode", "both")
    be_enabled = cfg.get("rsi_be_enabled", False)
    be_trigger_usd = cfg.get("rsi_be_trigger_usd", 5.0)
    tp_enabled = cfg.get("rsi_tp_enabled", False)
    tp_manual_usd = cfg.get("rsi_tp_manual_usd", 10.0)

    position = None
    trades = []
    sl_cooldown_until_ts = None

    for i in range(1, n):
        if position is not None:
            pdir, entry = position["dir"], position["entry"]
            sl_price = position.get("sl_price")
            hit_sl = sl_price is not None and ((pdir == "long" and l[i] <= sl_price) or (pdir == "short" and h[i] >= sl_price))
            if hit_sl:
                reason = "BREAKEVEN" if position["be_done"] else "SL"
                _bt_close_trade(trades, pdir, entry, sl_price, position["size"], i, position["entry_i"], reason, ts=ts)
                position = None
                sl_cooldown_until_ts = ts[i] + sl_cooldown_ms
            else:
                if be_enabled and not position["be_done"] and position["size"] > 0:
                    dist_be = be_trigger_usd / position["size"]
                    if (pdir == "long" and h[i] >= entry + dist_be) or (pdir == "short" and l[i] <= entry - dist_be):
                        position["sl_price"] = entry
                        position["be_done"] = True
                tp_price = position.get("tp_price")
                hit_tp = tp_price is not None and ((pdir == "long" and h[i] >= tp_price) or (pdir == "short" and l[i] <= tp_price))
                if hit_tp:
                    _bt_close_trade(trades, pdir, entry, tp_price, position["size"], i, position["entry_i"], "TP", ts=ts)
                    position = None

        if long_setup[i] and not long_setup[i - 1]:
            target = "long"
        elif short_setup[i] and not short_setup[i - 1]:
            target = "short"
        else:
            continue
        if position is not None and position["dir"] == target:
            continue
        price = c[i]
        if position is not None:
            _bt_close_trade(trades, position["dir"], position["entry"], price, position["size"], i, position["entry_i"], "RSI-FLIP", ts=ts)
            position = None
        elif sl_cooldown_until_ts is not None and ts[i] < sl_cooldown_until_ts:
            continue
        can_open = (direction_mode == "both"
                    or (direction_mode == "long_only" and target == "long")
                    or (direction_mode == "short_only" and target == "short"))
        if not can_open:
            continue
        size = (margin * leverage) / price
        sl_price = None
        if sl_enabled and size > 0:
            dist_sl = sl_manual_usd / size
            sl_price = price - dist_sl if target == "long" else price + dist_sl
        tp_price = None
        if tp_enabled and size > 0:
            dist_tp = tp_manual_usd / size
            tp_price = price + dist_tp if target == "long" else price - dist_tp
        position = {"dir": target, "entry": price, "size": size, "entry_i": i, "sl_price": sl_price, "tp_price": tp_price, "be_done": False}

    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)

    return trades


def backtest_rsi_signal(candles, cfg, trend_filter_long_ok=None, trend_filter_short_ok=None,
                         adx_long_ok=None, adx_short_ok=None, macd_long_ok=None, macd_short_ok=None):
    """Backtest fuer RSI Signal. Trendfilter-Serien werden von run_backtest() VORAB berechnet und
    hier nur noch per combine_filters() angewandt (wie bei Al-Shatri: ein Filter wird pro Sweep-
    Kombination oft wiederverwendet, deshalb hier nicht selbst neu holen)."""
    o, h, l, c = candles[1], candles[2], candles[3], candles[4]
    length = cfg.get("rsi_length", 14)
    long_raw, short_raw, _rsi = compute_rsi_signals(c, length, cfg.get("rsi_oversold", 30), cfg.get("rsi_overbought", 70))
    filters = []
    if trend_filter_long_ok is not None:
        filters.append((trend_filter_long_ok, trend_filter_short_ok))
    if adx_long_ok is not None:
        filters.append((adx_long_ok, adx_short_ok))
    if macd_long_ok is not None:
        filters.append((macd_long_ok, macd_short_ok))
    if filters:
        long_raw, short_raw = combine_filters(long_raw, short_raw, filters)
    return _simulate_rsi_trades(candles, cfg, long_raw, short_raw)


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



# ============================================================================
# Multi-VWAP Money-Flow Signal (portiert aus dem Pine-Indikator "Multi-VWAP Money Flow
# Oszillator [Divergenzen]") - Signal-Kern: gewichteter Verbund aus Daily/Weekly/Monthly-VWAP-
# Abweichung (in %) + MFI oder CMF, EMA-geglaettet. Buy/Sell = der Oszillator dreht die Richtung
# (steigt nach vorherigem Fallen -> Buy, faellt nach vorherigem Steigen -> Sell), optional nur
# ausserhalb der Overbought/Oversold-Zone. Aus-/Einstieg identisch zum RSI-Signal-Wechsel-System
# (Gegen-Signal dreht die Position, optionaler $-SL/-TP, Break-Even, Cooldown) - Filter kommen
# unveraendert aus dem generischen Filter-Baukasten (SuperTrend/ADX/MACD).
# ============================================================================

def compute_anchored_vwap_series(ts_ms, highs, lows, closes, volumes, anchor):
    """Anchored VWAP (wie Pine's ta.vwap(hlc3, newPeriod, 1)) - setzt die kumulierte Summe bei
    jedem neuen Kalendertag/-woche/-monat (UTC) zurueck. anchor: 'D', 'W' oder 'M'."""
    import datetime
    n = len(closes)
    vwap = [None] * n
    cum_pv = 0.0
    cum_vol = 0.0
    last_key = None
    for i in range(n):
        dt = datetime.datetime.utcfromtimestamp(ts_ms[i] / 1000)
        if anchor == "D":
            key = dt.date()
        elif anchor == "W":
            key = dt.isocalendar()[:2]
        else:
            key = (dt.year, dt.month)
        if key != last_key:
            cum_pv = 0.0
            cum_vol = 0.0
            last_key = key
        typical = (highs[i] + lows[i] + closes[i]) / 3
        cum_pv += typical * volumes[i]
        cum_vol += volumes[i]
        vwap[i] = cum_pv / cum_vol if cum_vol > 0 else typical
    return vwap


def compute_cmf(highs, lows, closes, volumes, length):
    """Chaikin Money Flow: rollierende Summe aus (Money-Flow-Multiplikator * Volumen) geteilt
    durch rollierende Volumensumme - wie Pine's ta.cmf()."""
    n = len(closes)
    mfv = [0.0] * n
    for i in range(n):
        rng = highs[i] - lows[i]
        mfm = 0.0 if rng == 0 else ((closes[i] - lows[i]) - (highs[i] - closes[i])) / rng
        mfv[i] = mfm * volumes[i]
    cmf = [0.0] * n
    for i in range(n):
        start = max(0, i - length + 1)
        vol_sum = sum(volumes[start:i + 1])
        cmf[i] = sum(mfv[start:i + 1]) / vol_sum if vol_sum > 0 else 0.0
    return cmf


def compute_mvwap_mf_oscillator(ts_ms, highs, lows, closes, volumes, params):
    """Composite-Oszillator: (1-mfWeight)*VWAP-Verbund + mfWeight*(MFI oder CMF, normiert auf
    ca. -2..2). Gibt (osc, mf_raw) zurueck - mf_raw (normiert -1..1) wird auch fuer den separaten
    Money-Flow-Filter gebraucht."""
    n = len(closes)
    dev_d = compute_anchored_vwap_series(ts_ms, highs, lows, closes, volumes, "D") if params["use_daily"] else None
    dev_w = compute_anchored_vwap_series(ts_ms, highs, lows, closes, volumes, "W") if params["use_weekly"] else None
    dev_m = compute_anchored_vwap_series(ts_ms, highs, lows, closes, volumes, "M") if params["use_monthly"] else None

    w_daily, w_weekly, w_monthly = params["w_daily"], params["w_weekly"], params["w_monthly"]
    w_sum = (w_daily if params["use_daily"] else 0) + (w_weekly if params["use_weekly"] else 0) + (w_monthly if params["use_monthly"] else 0)
    w_sum_safe = w_sum if w_sum != 0 else 1

    vwap_composite = [0.0] * n
    for i in range(n):
        dd = ((closes[i] - dev_d[i]) / dev_d[i] * 100) if dev_d is not None else 0.0
        dw = ((closes[i] - dev_w[i]) / dev_w[i] * 100) if dev_w is not None else 0.0
        dm = ((closes[i] - dev_m[i]) / dev_m[i] * 100) if dev_m is not None else 0.0
        vwap_composite[i] = (dd * w_daily + dw * w_weekly + dm * w_monthly) / w_sum_safe

    if params["mf_source"] == "MFI":
        mfi = compute_mfi(highs, lows, closes, volumes, params["mf_length"])
        mf_raw = [(v - 50) / 50 for v in mfi]
    else:
        mf_raw = compute_cmf(highs, lows, closes, volumes, params["cmf_length"])
    mf_scaled = [v * 2.0 for v in mf_raw]

    mf_weight = params["mf_weight"]
    osc_raw = [(1 - mf_weight) * vwap_composite[i] + mf_weight * mf_scaled[i] for i in range(n)]
    osc = _ema_series(osc_raw, params["smooth_len"])
    return osc, mf_raw


def compute_mvwap_mf_signals(osc, mf_raw, params):
    """Buy/Sell = Richtungswechsel des Oszillators (wie im Original-Skript: oscUp/oscDown-
    Flankenwechsel), optional nur bei Ueberdehnung (INNERHALB der OB/OS-Zone): Buy nur wenn
    der Oszillator unter os_level liegt (nach unten ueberdehnt -> Reversal-Kaufchance), Sell
    nur wenn er ueber ob_level liegt (nach oben ueberdehnt -> Reversal-Verkaufschance).
    FIX: vorher war das vertauscht (osc[i] < ob / osc[i] > os_), wodurch der Filter fast
    IMMER durchliess statt nur in den Extremzonen."""
    n = len(osc)
    osc_up = [osc[i] > osc[i - 1] if i > 0 else False for i in range(n)]
    osc_down = [not v for v in osc_up]
    buy_raw = [osc_up[i] and not (osc_up[i - 1] if i > 0 else False) for i in range(n)]
    sell_raw = [osc_down[i] and not (osc_down[i - 1] if i > 0 else False) for i in range(n)]
    if params.get("use_zone_filter", False):
        ob, os_ = params["ob_level"], params["os_level"]
        buy_raw = [buy_raw[i] and osc[i] < os_ for i in range(n)]
        sell_raw = [sell_raw[i] and osc[i] > ob for i in range(n)]
    return buy_raw, sell_raw


def compute_mvwap_reversal_signals(osc):
    """Grobe ECHTE Trendwende (Nulllinien-Durchbruch) - fuer den KOMPLETT-Ausstieg einer
    Nachkauf-Position gebraucht, NICHT fuer buy_raw/sell_raw (siehe compute_mvwap_mf_signals):
    buy_raw/sell_raw feuern bei JEDEM kleinen Richtungswechsel des Oszillators - weil osc_up und
    osc_down exakte Gegensaetze sind, wechseln sich buy_raw und sell_raw dabei zwingend IMMER ab
    (zwischen zwei buy_raw-Pulsen MUSS mindestens ein sell_raw-Puls liegen, der Oszillator muss ja
    erst wieder fallen, bevor er erneut steigen kann). Wird buy_raw/sell_raw fuer den Nachkauf UND
    fuer den Komplett-Ausstieg beim jeweils GEGENTEILIGEN Puls benutzt, schliesst deshalb JEDER
    Nachkauf-Versuch zwangslaeufig zuerst die alte Position, statt draufzulegen (live beobachtet:
    "beendet jeden Sell nacheinander und macht einen neuen auf" - strukturell unmoeglich, echten
    Nachkauf zu bekommen, weil zwischen zwei gleichgerichteten Pulsen immer ein Gegen-Puls liegt).
    Der Nulllinien-Durchbruch (Vorzeichenwechsel von osc) ist dagegen ein GROBES, viel selteneres
    Signal - ein einzelner kleiner Wendepunkt (buy_raw/sell_raw) reisst die Nulllinie normalerweise
    nicht, mehrere Nachkauf-Stufen bleiben also moeglich, bis der Oszillator wirklich die Seite
    wechselt."""
    n = len(osc)
    bull_cross = [osc[i] > 0 and osc[i - 1] <= 0 if i > 0 else False for i in range(n)]
    bear_cross = [osc[i] < 0 and osc[i - 1] >= 0 if i > 0 else False for i in range(n)]
    return bull_cross, bear_cross


def _mvwap_effective_params(cfg):
    return {
        "use_daily": cfg.get("mvwap_use_daily", True), "use_weekly": cfg.get("mvwap_use_weekly", True),
        "use_monthly": cfg.get("mvwap_use_monthly", True),
        "w_daily": cfg.get("mvwap_w_daily", 0.5), "w_weekly": cfg.get("mvwap_w_weekly", 0.3), "w_monthly": cfg.get("mvwap_w_monthly", 0.2),
        "mf_source": cfg.get("mvwap_mf_source", "MFI"), "mf_length": cfg.get("mvwap_mf_length", 14),
        "cmf_length": cfg.get("mvwap_cmf_length", 20), "mf_weight": cfg.get("mvwap_mf_weight", 0.35),
        "smooth_len": cfg.get("mvwap_smooth_len", 3),
        "use_zone_filter": cfg.get("mvwap_use_zone_filter", False),
        "ob_level": cfg.get("mvwap_ob_level", 2.0), "os_level": cfg.get("mvwap_os_level", -2.0),
    }


def _mvwap_reset_state(st):
    st["mvwap_sl_price"] = None
    st["mvwap_tp_price"] = None
    st["mvwap_be_done"] = False


async def check_mvwap_sl(symbol, price):
    """Identisch zu check_rsi_sl (siehe dort fuer Kommentare) - mit mvwap_-Config-Feldern."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if st["position"] is None or price is None:
        return
    pos = st["position"]
    if not cfg.get("mvwap_sl_enabled", True) and not st.get("mvwap_be_done"):
        st["mvwap_sl_price"] = None
    if cfg.get("mvwap_be_enabled", False) and not st.get("mvwap_be_done"):
        size = st.get("total_coin_size") or 0
        entry_ref = st.get("avg_entry_price")
        if size > 0 and entry_ref:
            dist_be = cfg.get("mvwap_be_trigger_usd", 5.0) / size
            reached = price >= entry_ref + dist_be if pos == "long" else price <= entry_ref - dist_be
            if reached:
                st["mvwap_sl_price"] = entry_ref
                st["mvwap_be_done"] = True
                debug_log(f"📡 [{symbol}] Multi-VWAP Money-Flow: ${cfg.get('mvwap_be_trigger_usd', 5.0)} Gewinn erreicht - SL auf Einstieg ({round(entry_ref, 4)}) gesetzt")
    sl_price = st.get("mvwap_sl_price")
    hit_sl = sl_price is not None and ((pos == "long" and price <= sl_price) or (pos == "short" and price >= sl_price))
    if hit_sl:
        reason = "BREAKEVEN" if st.get("mvwap_be_done") else "SL"
        debug_log(f"🚪 [{symbol}] Multi-VWAP Money-Flow {reason}: {pos.upper()} @ {price} (Ziel war {round(sl_price, 4)})")
        await execute_exit(symbol, price, reason)
        if st["position"] is None:
            st["mvwap_sl_cooldown_until"] = time.time() + cfg.get("mvwap_sl_cooldown_seconds", 30)
            _mvwap_reset_state(st)
        return
    tp_price = st.get("mvwap_tp_price")
    hit_tp = tp_price is not None and ((pos == "long" and price >= tp_price) or (pos == "short" and price <= tp_price))
    if hit_tp:
        debug_log(f"🎯 [{symbol}] Multi-VWAP Money-Flow TP: {pos.upper()} @ {price} (Ziel war {round(tp_price, 4)})")
        await execute_exit(symbol, price, "TP")
        if st["position"] is None:
            _mvwap_reset_state(st)


def _mvwap_update_sl_tp(st, cfg):
    """Setzt/aktualisiert SL- und TP-Preis nach einem Erst- ODER Nachkauf-Einstieg, aus dem
    AKTUELLEN Ø-Einstiegspreis und der AKTUELLEN Gesamtgroesse - wichtig bei Nachkaeufen
    (mvwap_max_entries > 1), weil sich beides mit jeder weiteren Stufe aendert (der SL/TP-Abstand
    in Kursnaehe muss kleiner werden, je groesser die Gesamtposition ist, um denselben
    Dollar-Betrag zu ergeben)."""
    pos = st["position"]
    size = st.get("total_coin_size") or 0
    entry_ref = st.get("avg_entry_price")
    if pos is None or size <= 0 or not entry_ref:
        return
    if cfg.get("mvwap_sl_enabled", True):
        dist_sl = cfg.get("mvwap_sl_manual_usd", 5.0) / size
        st["mvwap_sl_price"] = entry_ref - dist_sl if pos == "long" else entry_ref + dist_sl
    if cfg.get("mvwap_tp_enabled", False):
        dist_tp = cfg.get("mvwap_tp_manual_usd", 10.0) / size
        st["mvwap_tp_price"] = entry_ref + dist_tp if pos == "long" else entry_ref - dist_tp


async def check_mvwap_entry(symbol, buy_reversal_edge, sell_reversal_edge, buy_final_edge, sell_final_edge, price):
    """Identisch zu check_rsi_entry (siehe dort fuer Kommentare) - mit mvwap_-Config-Feldern,
    UND mit Nachkauf-Unterstuetzung (mvwap_max_entries): jede neue BUY-Flanke waehrend einer
    offenen Long-Position (bzw. SELL waehrend Short) legt eine weitere Stufe nach, bis zur
    eingestellten Obergrenze - die ERSTE ECHTE Trendwende (Nulllinien-Kreuzung, siehe
    compute_mvwap_reversal_signals) schliesst die KOMPLETTE Position (alle Stufen), unabhaengig
    davon wie viele es waren. Alle vier Signal-Parameter sind FLANKEN (True nur genau die eine
    Kerze, in der das Signal neu auftritt), nicht Zustaende - sonst wuerde ein ueber viele Kerzen
    anhaltendes Level-Signal bei jedem Kerzenschluss einen weiteren Nachkauf ausloesen.

    WICHTIG (Bugfix #1): der Komplett-Ausstieg laeuft bewusst auf dem GROBEN Nulllinien-
    Kreuzungssignal (buy_reversal_edge/sell_reversal_edge), NICHT auf den haeufigen buy/sell-
    Wendepunkt-Pulsen (die auch fuer buy_final_edge/sell_final_edge zugrunde liegen): die
    Wendepunkt-Pulse wechseln sich zwingend IMMER ab (osc_up/osc_down sind exakte Gegensaetze,
    zwischen zwei gleichgerichteten Pulsen MUSS ein Gegen-Puls liegen) - wurden sie fuer den
    Komplett-Ausstieg benutzt, schloss JEDER Nachkauf-Versuch zwangslaeufig zuerst die alte
    Position statt draufzulegen (live beobachtet: "beendet jeden Sell nacheinander und macht
    einen neuen auf"). Das Nulllinien-Kreuzungssignal ist dagegen viel seltener und uebersteht
    einzelne kleine Wendepunkte, wodurch mehrere Nachkauf-Stufen ueberhaupt erst moeglich werden.

    WICHTIG (Bugfix #2): der Ausstieg laeuft ausserdem bewusst auf dem UNGEFILTERTEN Signal, nicht
    auf dem gefilterten (buy_final_edge/sell_final_edge, nach SuperTrend/ADX/MACD/RSI/Cloud).
    Vorher wurde derselbe gefilterte Wert fuer Ausstieg UND Neueinstieg benutzt: ein aktiver
    Filter (z.B. SuperTrend auf einer hoeheren Zeiteinheit, die sich seltener dreht) verhinderte
    dann nicht nur einen falschen Neueinstieg, sondern hielt auch eine bestehende Position fest,
    bis der Filter selbst wieder mitspielte - live beobachtet: eine BTC-Position blieb offen,
    obwohl das Money-Flow-Signal laengst gedreht hatte ("haette laengst schliessen sollen"), weil
    der HTF-SuperTrend-Filter das Gegen-Signal noch blockierte. Ein Filter soll nur ungewollte
    NEUE Einstiege (Erst- und Nachkauf) verhindern, niemals das Schliessen einer bereits offenen
    Position."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if not cfg["bot_active"] or price is None:
        return

    pos = st["position"]

    # Komplett-Ausstieg bei der ERSTEN echten Trendwende (Nulllinien-Kreuzung) - schliesst ALLE
    # Nachkauf-Stufen auf einmal, unabhaengig von mvwap_max_entries.
    if (pos == "long" and sell_reversal_edge) or (pos == "short" and buy_reversal_edge):
        debug_log(f"🔄 [{symbol}] Multi-VWAP Money-Flow: erstes Gegensignal - schliesse {pos.upper()}-Position komplett ({st.get('entry_count', 0)} Stufen) @ {price} (Rohsignal - Filter gilt nur fuer Neueinstiege)")
        await execute_exit(symbol, price, "MVWAP-FLIP")
        if st["position"] is not None:
            return
        _mvwap_reset_state(st)
        pos = None

    max_entries = max(1, int(cfg.get("mvwap_max_entries", 1) or 1))

    # Nachkauf: Position ist schon in dieselbe Richtung offen UND eine neue (gefilterte) Flanke
    # derselben Richtung kommt - solange die Stufen-Obergrenze noch nicht erreicht ist.
    if pos == "long" and buy_final_edge:
        if st.get("entry_count", 0) >= max_entries:
            return
        await execute_entry(symbol, "long", price, is_add_on=True)
        _mvwap_update_sl_tp(st, cfg)
        return
    if pos == "short" and sell_final_edge:
        if st.get("entry_count", 0) >= max_entries:
            return
        await execute_entry(symbol, "short", price, is_add_on=True)
        _mvwap_update_sl_tp(st, cfg)
        return

    if pos is not None:
        return  # Position offen, aber kein (Nachkauf-)Signal in dieselbe Richtung diese Kerze

    if time.time() < st.get("mvwap_sl_cooldown_until", 0.0):
        return

    if buy_final_edge:
        target = "long"
    elif sell_final_edge:
        target = "short"
    else:
        return

    direction_mode = cfg.get("mvwap_direction_mode", "both")
    can_open = (direction_mode == "both"
                or (direction_mode == "long_only" and target == "long")
                or (direction_mode == "short_only" and target == "short"))
    if not can_open:
        return
    debug_log(f"📡 [{symbol}] Multi-VWAP Money-Flow: {target.upper()} @ {price}")
    await execute_entry(symbol, target, price, is_add_on=False)
    if st["position"] is None:
        return
    _mvwap_reset_state(st)
    _mvwap_update_sl_tp(st, cfg)


async def _mvwap_apply_filters(symbol, st, cfg, candles, long_raw, short_raw):
    """Identisch zu _rsi_apply_filters (siehe dort fuer Kommentare) - mit mvwap_-Config-Feldern.
    candles ist hier ein 6er-Tupel MIT Volumen (ts,o,h,l,c,v) - wird fuer den Cloud-Filter
    gebraucht, ADX/MACD ignorieren das Volumen einfach (siehe deren Docstrings)."""
    n = len(long_raw)
    active = []
    if cfg.get("mvwap_supertrend_filter_enabled", False):
        lo, so = await compute_supertrend_filter_live(
            symbol, st, cfg, cfg.get("mvwap_supertrend_filter_resolution", "15m"),
            cfg.get("mvwap_supertrend_filter_multiplier", 3.0), cfg.get("mvwap_supertrend_filter_atr_period", 10), n)
        if lo is not None:
            active.append((lo, so))
    if cfg.get("mvwap_adx_filter_enabled", False):
        active.append(compute_adx_filter_series(
            candles, cfg.get("mvwap_adx_filter_length", 14), cfg.get("mvwap_adx_filter_threshold", 20),
            directional=cfg.get("mvwap_adx_filter_directional", True)))
    if cfg.get("mvwap_macd_filter_enabled", False):
        active.append(compute_macd_filter_series(
            candles, cfg.get("mvwap_macd_filter_fast", 12), cfg.get("mvwap_macd_filter_slow", 26), cfg.get("mvwap_macd_filter_signal", 9)))
    if cfg.get("mvwap_rsi_filter_enabled", False):
        active.append(compute_rsi_filter_series(
            candles, cfg.get("mvwap_rsi_filter_length", 14),
            cfg.get("mvwap_rsi_filter_os_level", 30), cfg.get("mvwap_rsi_filter_ob_level", 70)))
    if cfg.get("mvwap_cloud_filter_enabled", False):
        active.append(compute_cloud_filter_series(
            candles, cfg.get("mvwap_cloud_filter_length", 60), cfg.get("mvwap_cloud_filter_dev_mult", 2.0),
            touch_arm=cfg.get("mvwap_cloud_filter_touch_arm", False)))
    if not active:
        return long_raw, short_raw
    return combine_filters(long_raw, short_raw, active)


async def mvwap_poll_loop(symbol):
    """Multi-VWAP Money-Flow Signal - Kerzenschluss-Signal wie RSI, Wechsel-System als Ausstieg,
    optional SuperTrend-/ADX-/MACD-Filter aus dem generischen Baukasten. Braucht Volumen (fuer
    VWAP/MFI/CMF) - deshalb fetch_candles_binance_vol statt _multi."""
    b = BOTS[symbol]
    last_processed_ts = None
    last_heartbeat = 0.0

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "mvwap_mf_signal" and cfg["bot_active"]:
                resolution = cfg.get("mvwap_resolution", "5m")
                params = _mvwap_effective_params(cfg)
                min_needed = max(params["mf_length"], params["cmf_length"]) + 30
                needed_bars = min(1000, max(min_needed * 2, 300))
                st = b["state"]

                data = await fetch_candles_binance_vol(symbol, resolution, count_back=needed_bars)
                if data:
                    timestamps, opens, highs, lows, closes, volumes = data
                    closed_ts, closed_o, closed_h, closed_l, closed_c, closed_v = timestamps[:-1], opens[:-1], highs[:-1], lows[:-1], closes[:-1], volumes[:-1]
                else:
                    closed_ts = None

                now = time.time()
                due_heartbeat = now - last_heartbeat > 300

                if st["position"] is not None and st["last_price"] is not None:
                    await check_mvwap_sl(symbol, st["last_price"])

                if closed_ts and len(closed_c) > min_needed:
                    candle_age_seconds = (now * 1000 - closed_ts[-1]) / 1000
                    max_age_seconds = 300
                    if candle_age_seconds > max_age_seconds:
                        debug_log(f"⚠️ [{symbol}] Multi-VWAP Money-Flow: letzte Kerze wirkt veraltet ({round(candle_age_seconds)}s alt, Auflösung {resolution}) - überspringe Signal-Berechnung diesen Durchlauf.")
                    else:
                        last_ts = closed_ts[-1]
                        if last_ts != last_processed_ts:
                            last_processed_ts = last_ts
                            osc, mf_raw = compute_mvwap_mf_oscillator(closed_ts, closed_h, closed_l, closed_c, closed_v, params)
                            long_raw, short_raw = compute_mvwap_mf_signals(osc, mf_raw, params)
                            bull_cross, bear_cross = compute_mvwap_reversal_signals(osc)
                            candles = (closed_ts, closed_o, closed_h, closed_l, closed_c, closed_v)
                            long_final, short_final = await _mvwap_apply_filters(symbol, st, cfg, candles, long_raw, short_raw)
                            buy_signal = long_final[-1]
                            sell_signal = short_final[-1]
                            st["mvwap_osc_last"] = osc[-1]
                            # Flanken statt Zustand (siehe check_mvwap_entry-Docstring): sonst
                            # wuerde ein ueber mehrere Kerzen anhaltendes Signal bei JEDEM
                            # Kerzenschluss einen weiteren Nachkauf ausloesen statt nur einmal pro
                            # neuem Signalwechsel. Fuer den KOMPLETT-Ausstieg wird bewusst NICHT
                            # long_raw/short_raw benutzt (das sind schon einzelne Wendepunkt-Pulse,
                            # die sich zwingend mit jedem Nachkauf-Puls abwechseln - siehe
                            # compute_mvwap_reversal_signals-Docstring, das war der Grund, warum
                            # "jeder Nachkauf die alte Position zuerst beendet hat"), sondern das
                            # viel seltenere Nulllinien-Kreuzungssignal (bull_cross/bear_cross).
                            buy_reversal_now, sell_reversal_now = bull_cross[-1], bear_cross[-1]
                            buy_reversal_edge = buy_reversal_now and not st.get("mvwap_prev_buy_reversal", False)
                            sell_reversal_edge = sell_reversal_now and not st.get("mvwap_prev_sell_reversal", False)
                            buy_final_edge = buy_signal and not st.get("mvwap_prev_buy_final", False)
                            sell_final_edge = sell_signal and not st.get("mvwap_prev_sell_final", False)
                            st["mvwap_prev_buy_reversal"] = buy_reversal_now
                            st["mvwap_prev_sell_reversal"] = sell_reversal_now
                            st["mvwap_prev_buy_final"] = buy_signal
                            st["mvwap_prev_sell_final"] = sell_signal
                            await check_mvwap_entry(symbol, buy_reversal_edge, sell_reversal_edge, buy_final_edge, sell_final_edge, closed_c[-1])
                        if due_heartbeat:
                            last_heartbeat = now
                            debug_log(f"💓 [{symbol}] Multi-VWAP Money-Flow aktiv: Oszillator={round(st.get('mvwap_osc_last') or 0, 2)}, "
                                      f"Preis={closed_c[-1]}, Kerzen={len(closed_c)}, bot_active={cfg['bot_active']}")
                elif due_heartbeat:
                    last_heartbeat = now
                    if not closed_ts:
                        debug_log(f"⏳ [{symbol}] Multi-VWAP Money-Flow wartet: keine Kerzen erhalten (Auflösung {resolution})")
                    else:
                        debug_log(f"⏳ [{symbol}] Multi-VWAP Money-Flow wartet: zu wenig Kerzen ({len(closed_c)}/{min_needed + 1} nötig)")
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Multi-VWAP Money-Flow-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})

        await asyncio.sleep(5)


def _simulate_mvwap_trades(candles, cfg, long_raw, short_raw, long_final=None, short_final=None):
    """Backtest-Simulation - identisch zu _simulate_rsi_trades (siehe dort fuer Kommentare), mit
    mvwap_-Config-Feldern (inkl. optionalem festen Dollar-TP, SL gewinnt bei Konflikt) UND
    Nachkauf-Unterstuetzung (mvwap_max_entries), spiegelbildlich zu check_mvwap_entry (live):
    jede neue gefilterte Signal-Flanke in dieselbe Richtung wie die offene Position legt eine
    weitere Stufe nach (bis zur Obergrenze), die ERSTE Flanke im long_raw/short_raw-Parameter
    schliesst die komplette Position auf einen Schlag. WICHTIG: long_raw/short_raw MUSS hier das
    GROBE Nulllinien-Kreuzungssignal sein (compute_mvwap_reversal_signals), NICHT die haeufigen
    buy/sell-Wendepunkt-Pulse aus compute_mvwap_mf_signals - die wechseln sich zwingend mit jedem
    Nachkauf-Puls ab, wodurch echter Nachkauf sonst strukturell unmoeglich waere (jeder Nachkauf
    haette zuerst die alte Position geschlossen). long_final/short_final sind optional
    (abwaertskompatibel: ohne Filter sind sie identisch zum ersten Parameterpaar)."""
    ts, h, l, c = candles[0], candles[2], candles[3], candles[4]
    n = len(c)
    if long_final is None:
        long_final, short_final = long_raw, short_raw
    margin, leverage = cfg["margin"], cfg["leverage"]
    sl_enabled = cfg.get("mvwap_sl_enabled", True)
    sl_manual_usd = cfg.get("mvwap_sl_manual_usd", 5.0)
    sl_cooldown_ms = cfg.get("mvwap_sl_cooldown_seconds", 30) * 1000
    direction_mode = cfg.get("mvwap_direction_mode", "both")
    be_enabled = cfg.get("mvwap_be_enabled", False)
    be_trigger_usd = cfg.get("mvwap_be_trigger_usd", 5.0)
    tp_enabled = cfg.get("mvwap_tp_enabled", False)
    tp_manual_usd = cfg.get("mvwap_tp_manual_usd", 10.0)
    max_entries = max(1, int(cfg.get("mvwap_max_entries", 1) or 1))

    position = None
    trades = []
    sl_cooldown_until_ts = None

    def _recalc_sl_tp(pos):
        size, entry, pdir = pos["size"], pos["entry"], pos["dir"]
        if sl_enabled and size > 0:
            dist_sl = sl_manual_usd / size
            pos["sl_price"] = entry - dist_sl if pdir == "long" else entry + dist_sl
        if tp_enabled and size > 0:
            dist_tp = tp_manual_usd / size
            pos["tp_price"] = entry + dist_tp if pdir == "long" else entry - dist_tp

    for i in range(1, n):
        if position is not None:
            pdir, entry = position["dir"], position["entry"]
            sl_price = position.get("sl_price")
            hit_sl = sl_price is not None and ((pdir == "long" and l[i] <= sl_price) or (pdir == "short" and h[i] >= sl_price))
            if hit_sl:
                reason = "BREAKEVEN" if position["be_done"] else "SL"
                _bt_close_trade(trades, pdir, entry, sl_price, position["size"], i, position["entry_i"], reason, ts=ts)
                position = None
                sl_cooldown_until_ts = ts[i] + sl_cooldown_ms
            else:
                if be_enabled and not position["be_done"] and position["size"] > 0:
                    dist_be = be_trigger_usd / position["size"]
                    if (pdir == "long" and h[i] >= entry + dist_be) or (pdir == "short" and l[i] <= entry - dist_be):
                        position["sl_price"] = entry
                        position["be_done"] = True
                tp_price = position.get("tp_price")
                hit_tp = tp_price is not None and ((pdir == "long" and h[i] >= tp_price) or (pdir == "short" and l[i] <= tp_price))
                if hit_tp:
                    _bt_close_trade(trades, pdir, entry, tp_price, position["size"], i, position["entry_i"], "TP", ts=ts)
                    position = None

        price = c[i]
        buy_raw_edge = long_raw[i] and not long_raw[i - 1]
        sell_raw_edge = short_raw[i] and not short_raw[i - 1]
        buy_final_edge = long_final[i] and not long_final[i - 1]
        sell_final_edge = short_final[i] and not short_final[i - 1]

        # Komplett-Ausstieg bei der ERSTEN Gegen-Flanke (ungefiltert) - siehe check_mvwap_entry.
        if position is not None and ((position["dir"] == "long" and sell_raw_edge) or (position["dir"] == "short" and buy_raw_edge)):
            _bt_close_trade(trades, position["dir"], position["entry"], price, position["size"], i, position["entry_i"], "MVWAP-FLIP", ts=ts)
            position = None

        if position is not None:
            # Nachkauf: weitere gefilterte Flanke in dieselbe Richtung, Stufenlimit noch offen.
            same_dir_edge = (position["dir"] == "long" and buy_final_edge) or (position["dir"] == "short" and sell_final_edge)
            if same_dir_edge and position["entries"] < max_entries:
                add_size = (margin * leverage) / price
                old_size = position["size"]
                new_size = old_size + add_size
                position["entry"] = (position["entry"] * old_size + price * add_size) / new_size
                position["size"] = new_size
                position["entries"] += 1
                _recalc_sl_tp(position)
            continue  # Position (weiterhin) offen - keine Neueinstiegs-Pruefung diese Kerze

        if buy_final_edge:
            target = "long"
        elif sell_final_edge:
            target = "short"
        else:
            continue
        if sl_cooldown_until_ts is not None and ts[i] < sl_cooldown_until_ts:
            continue
        can_open = (direction_mode == "both"
                    or (direction_mode == "long_only" and target == "long")
                    or (direction_mode == "short_only" and target == "short"))
        if not can_open:
            continue
        size = (margin * leverage) / price
        position = {"dir": target, "entry": price, "size": size, "entry_i": i, "sl_price": None, "tp_price": None, "be_done": False, "entries": 1}
        _recalc_sl_tp(position)

    if position is not None:
        _bt_close_trade(trades, position["dir"], position["entry"], c[n - 1], position["size"], n - 1, position["entry_i"], "END-OF-BACKTEST", ts=ts)

    return trades


def backtest_mvwap_signal(candles, cfg, trend_filter_long_ok=None, trend_filter_short_ok=None,
                           adx_long_ok=None, adx_short_ok=None, macd_long_ok=None, macd_short_ok=None):
    """Backtest fuer Multi-VWAP Money-Flow Signal. candles muss ein 6er-Tupel MIT Volumen sein."""
    ts, o, h, l, c, v = candles
    params = _mvwap_effective_params(cfg)
    osc, mf_raw = compute_mvwap_mf_oscillator(ts, h, l, c, v, params)
    long_raw, short_raw = compute_mvwap_mf_signals(osc, mf_raw, params)
    bull_cross, bear_cross = compute_mvwap_reversal_signals(osc)
    filters = []
    if trend_filter_long_ok is not None:
        filters.append((trend_filter_long_ok, trend_filter_short_ok))
    if adx_long_ok is not None:
        filters.append((adx_long_ok, adx_short_ok))
    if macd_long_ok is not None:
        filters.append((macd_long_ok, macd_short_ok))
    # RSI- und Cloud-Filter brauchen (anders als SuperTrend/ADX/MACD oben, die teils externe HTF-
    # Kerzen brauchen) nur die eigenen candles - werden deshalb direkt hier berechnet statt vom
    # Aufrufer (run_backtest) durchgereicht zu werden.
    if cfg.get("mvwap_rsi_filter_enabled", False):
        filters.append(compute_rsi_filter_series(
            candles, cfg.get("mvwap_rsi_filter_length", 14),
            cfg.get("mvwap_rsi_filter_os_level", 30), cfg.get("mvwap_rsi_filter_ob_level", 70)))
    if cfg.get("mvwap_cloud_filter_enabled", False):
        filters.append(compute_cloud_filter_series(
            candles, cfg.get("mvwap_cloud_filter_length", 60), cfg.get("mvwap_cloud_filter_dev_mult", 2.0),
            touch_arm=cfg.get("mvwap_cloud_filter_touch_arm", False)))
    # bull_cross/bear_cross (Nulllinien-Kreuzung, NICHT long_raw/short_raw) fuer den Komplett-
    # Ausstieg an die Simulation weitergeben - siehe _simulate_mvwap_trades/check_mvwap_entry-
    # Docstring: long_raw/short_raw wechseln sich als Wendepunkt-Pulse zwingend IMMER ab, damit
    # waere echter Nachkauf strukturell unmoeglich (jeder Nachkauf haette zuerst schliessen
    # muessen). Die gefilterte Fassung (long_final/short_final) geht weiterhin fuer Ein-/Nachkauf
    # mit, damit ein aktiver Filter wie live nur Neueinstiege/Nachkaeufe verhindert, nie das
    # Schliessen einer offenen Position.
    long_final, short_final = (combine_filters(long_raw, short_raw, filters) if filters else (long_raw, short_raw))
    return _simulate_mvwap_trades(candles, cfg, bull_cross, bear_cross, long_final, short_final)
