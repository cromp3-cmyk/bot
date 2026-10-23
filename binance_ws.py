"""
binance_ws.py - WebSocket-basierter Kerzen-Cache fuer Binance.

Ersetzt einen Grossteil der bisherigen REST-Polling-Anfragen (fetch_candles_binance /
fetch_candles_binance_vol in strategies.py, bisher von JEDEM der ~15 Poll-Loops x jeder
Coin alle 5 Sekunden einzeln aufgerufen) durch EINE dauerhafte WebSocket-Verbindung pro
Markttyp (spot/futures), die alle tatsaechlich benoetigten Kerzen-Streams (Symbol+Intervall)
buendelt. Binance PUSHT die Daten dann selbst - wir muessen nicht mehr aktiv fragen.

Design bewusst additiv/nicht-invasiv:
- Ein neuer Stream wird beim ersten Bedarf per ensure_subscribed() vorgemerkt und beim
  naechsten Durchlauf des Manager-Loops abonniert; die Historie wird EINMALIG per REST
  nachgeladen (Seed), danach uebernimmt ausschliesslich der WS-Push die Aktualisierung.
- get_cached_candles() liefert None, solange ein Stream noch nicht "warm" ist (frisch
  abonniert, Seed noch nicht durch) oder fuer nicht abgedeckte Aufloesungen - der Aufrufer
  faellt dann automatisch auf den bisherigen REST-Weg zurueck. Es gibt also nie einen
  Zustand, in dem eine Anfrage ins Leere laeuft, nur eine Uebergangsphase mit noch normalem
  REST-Traffic bis der jeweilige Stream einmal warmgelaufen ist.
- Synthetische Aufloesungen (2m/10s/15s/30s/45s) werden weiterhin aus den nativen 1m/1s-
  Kerzen zusammengesetzt (siehe resolve_synthetic_resolution in strategies.py) - die
  koennen aber SELBST aus diesem Cache kommen, da 1m und 1s hier abgedeckt sind.
"""

import asyncio
import json
import time
from collections import deque

import websockets

from bot_core import debug_log

WS_HOSTS = {
    "spot": "wss://stream.binance.com:9443/ws",
    "futures": "wss://fstream.binance.com/ws",
}

# Nur Intervalle, die Binance nativ als Kline-Stream anbietet, werden hier gecacht.
CACHEABLE_INTERVALS = {"1s", "1m", "3m", "5m", "15m", "30m", "1h", "4h"}

MAX_CANDLES_PER_STREAM = 1500  # deckt alle bisherigen count_back-Werte komfortabel ab

# Wie viele Kerzen beim erstmaligen Abonnieren eines Streams per REST vorgeladen werden
# (EINMALIG pro Stream, nicht wiederholt - danach nur noch WS-Push).
REST_SEED_LIMIT = {
    "1s": 1000, "1m": 1000, "3m": 1000, "5m": 1000,
    "15m": 500, "30m": 500, "1h": 300, "4h": 200,
}

BINANCE_BASE_URLS = {
    "spot": "https://api.binance.com/api/v3/klines",
    "futures": "https://fapi.binance.com/fapi/v1/klines",
}

# Sekunden je Intervall - Basis fuer die Veraltungsgrenze (siehe STALENESS_MULTIPLIER unten).
INTERVAL_SECONDS = {
    "1s": 1, "1m": 60, "3m": 180, "5m": 300,
    "15m": 900, "30m": 1800, "1h": 3600, "4h": 14400,
}

# Kommt seit mehr als (Intervall-Dauer * Multiplikator + Puffer) kein WS-Update mehr an,
# gilt der Stream als eingefroren/nicht mehr vertrauenswuerdig - get_cached_candles() liefert
# dann None (Aufrufer faellt automatisch auf REST zurueck), UND der Stream wird zur
# Neu-Subscription vorgemerkt. Verhindert, dass eine lautlos abgebrochene WS-Verbindung
# fuer immer denselben eingefrorenen Preis/Score ausliefert.
STALENESS_MULTIPLIER = 3
STALENESS_BUFFER_SECONDS = 30


class _StreamState:
    __slots__ = ("candles", "ready", "last_update_ts")

    def __init__(self):
        self.candles = deque(maxlen=MAX_CANDLES_PER_STREAM)  # Dicts: ts,o,h,l,c,v
        self.ready = False  # True, sobald der einmalige REST-Seed durch ist
        self.last_update_ts = 0.0  # time.time() der letzten WS-Aktualisierung (oder des Seeds)


# market_type -> {"BTCUSDT|1m": _StreamState}
_streams = {"spot": {}, "futures": {}}
_pending_subscribe = {"spot": set(), "futures": set()}


def _key(pair, interval):
    return f"{pair}|{interval}"


def _stream_name(pair, interval):
    return f"{pair.lower()}@kline_{interval}"


def ensure_subscribed(market_type, pair, interval):
    """Merkt einen Stream als benoetigt vor (falls noch nicht bekannt). Rein synchron/
    nicht-blockierend - das eigentliche Abonnieren + der REST-Seed passieren asynchron
    im Manager-Loop. Sicher von ueberall aufrufbar, auch bevor die WS-Verbindung steht."""
    if market_type not in _streams or interval not in CACHEABLE_INTERVALS:
        return
    k = _key(pair, interval)
    if k not in _streams[market_type]:
        _streams[market_type][k] = _StreamState()
        _pending_subscribe[market_type].add(k)


def get_cached_candles(market_type, pair, interval, count_back):
    """Gibt (timestamps, opens, highs, lows, closes, volumes) zurueck, wenn der Stream
    bereits warm UND frisch genug ist - sonst None (Aufrufer soll auf REST zurueckfallen).
    Ein Stream, der laenger als die Veraltungsgrenze kein WS-Update mehr bekommen hat, gilt
    als eingefroren: er wird hier zurueckgewiesen UND zur Neu-Subscription vorgemerkt, damit
    sich eine lautlos abgebrochene WS-Verbindung von selbst erholt, statt fuer immer denselben
    alten Preis/Score auszuliefern."""
    st = _streams.get(market_type, {}).get(_key(pair, interval))
    if st is None or not st.ready or not st.candles:
        return None

    max_staleness = INTERVAL_SECONDS.get(interval, 60) * STALENESS_MULTIPLIER + STALENESS_BUFFER_SECONDS
    if time.time() - st.last_update_ts > max_staleness:
        st.ready = False  # bis zum naechsten erfolgreichen Seed faellt der Aufrufer auf REST zurueck
        _pending_subscribe[market_type].add(_key(pair, interval))
        debug_log(f"⚠️ [WS-Cache] {pair} {interval} ({market_type}) eingefroren erkannt - falle auf REST zurueck und abonniere neu")
        return None

    candles = list(st.candles)[-count_back:]
    if not candles:
        return None
    timestamps = [c["ts"] for c in candles]
    opens = [c["o"] for c in candles]
    highs = [c["h"] for c in candles]
    lows = [c["l"] for c in candles]
    closes = [c["c"] for c in candles]
    volumes = [c["v"] for c in candles]
    return timestamps, opens, highs, lows, closes, volumes


async def _seed_stream_history(market_type, pair, interval):
    """Laedt einmalig Historie per REST fuer einen frisch abonnierten Stream. Nutzt
    bewusst die Bann-/Throttle-Infrastruktur aus strategies.py (per Lazy-Import, um einen
    Zirkelimport beim Modul-Laden zu vermeiden - strategies.py importiert dieses Modul
    bereits auf oberster Ebene)."""
    import aiohttp
    from strategies import _binance_throttle, _binance_is_banned, _binance_register_ban

    if _binance_is_banned(market_type):
        # Aktiver Bann - Seed spaeter nachholen, damit wir ihn nicht verlaengern. Der
        # Stream bleibt in _streams (ready=False), also faellt der Aufrufer bis dahin
        # weiterhin auf REST zurueck, ohne dass etwas verloren geht.
        _pending_subscribe[market_type].add(_key(pair, interval))
        return

    base_url = BINANCE_BASE_URLS.get(market_type, BINANCE_BASE_URLS["spot"])
    limit = REST_SEED_LIMIT.get(interval, 500)
    try:
        await _binance_throttle()
        url = f"{base_url}?symbol={pair}&interval={interval}&limit={limit}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status in (418, 429):
                    body = await resp.text()
                    _binance_register_ban(market_type, pair, resp.status, body)
                    return
                if resp.status != 200:
                    return
                data = await resp.json()
    except Exception as e:
        debug_log(f"⚠️ [WS-Cache] Seed-Historie fehlgeschlagen ({pair} {interval})", {"error": str(e)})
        return

    if not data or not isinstance(data, list):
        return

    st = _streams[market_type].get(_key(pair, interval))
    if st is None:
        return  # Stream wurde inzwischen entfernt - nichts mehr zu tun
    for k in data:
        st.candles.append({
            "ts": int(k[0]), "o": float(k[1]), "h": float(k[2]),
            "l": float(k[3]), "c": float(k[4]), "v": float(k[5]),
        })
    st.ready = True
    st.last_update_ts = time.time()
    debug_log(f"✅ [WS-Cache] {pair} {interval} ({market_type}) bereit", {"kerzen": len(st.candles)})


def _apply_kline_event(market_type, payload):
    k = payload.get("k") or {}
    pair = k.get("s")
    interval = k.get("i")
    if not pair or not interval:
        return
    st = _streams.get(market_type, {}).get(_key(pair, interval))
    if st is None:
        return  # Kein Stream, den wir aktuell verfolgen - ignorieren

    ts = int(k["t"])
    candle = {
        "ts": ts, "o": float(k["o"]), "h": float(k["h"]),
        "l": float(k["l"]), "c": float(k["c"]), "v": float(k["v"]),
    }
    if st.candles and st.candles[-1]["ts"] == ts:
        st.candles[-1] = candle  # laufende (noch nicht geschlossene) Kerze aktualisieren
    elif not st.candles or ts > st.candles[-1]["ts"]:
        st.candles.append(candle)  # neue, geschlossene Kerze angehaengt
    else:
        return  # aeltere/doppelte Zeitstempel (Nachzuegler) werden ignoriert - kein Update-Zeitstempel
    st.last_update_ts = time.time()


async def _websocket_loop(market_type):
    """Haelt EINE dauerhafte WS-Verbindung pro Markttyp offen. Neue Streams werden
    dynamisch per SUBSCRIBE-Nachricht nachgereicht (kein Reconnect noetig). Bei
    Verbindungsabbruch: Reconnect mit exponentiellem Backoff, alle bereits bekannten
    Streams werden automatisch neu abonniert (die lokale Kerzenhistorie bleibt dabei
    erhalten - nur die Zeit der Unterbrechung fehlt als kleine Luecke)."""
    host = WS_HOSTS[market_type]
    backoff = 1
    while True:
        try:
            async with websockets.connect(host, ping_interval=20, ping_timeout=20) as ws:
                debug_log(f"🔌 [WS-Cache] Verbunden ({market_type})")
                backoff = 1

                existing = list(_streams.get(market_type, {}).keys())
                if existing:
                    await ws.send(json.dumps({
                        "method": "SUBSCRIBE",
                        "params": [_stream_name(*k.split("|")) for k in existing],
                        "id": int(time.time()),
                    }))
                    for k in existing:
                        pair, interval = k.split("|")
                        if not _streams[market_type][k].ready:
                            asyncio.create_task(_seed_stream_history(market_type, pair, interval))

                async def _subscriber_loop():
                    while True:
                        await asyncio.sleep(1)
                        pending = _pending_subscribe.get(market_type)
                        if pending:
                            batch = list(pending)
                            pending.clear()
                            await ws.send(json.dumps({
                                "method": "SUBSCRIBE",
                                "params": [_stream_name(*k.split("|")) for k in batch],
                                "id": int(time.time()),
                            }))
                            for k in batch:
                                pair, interval = k.split("|")
                                asyncio.create_task(_seed_stream_history(market_type, pair, interval))

                sub_task = asyncio.create_task(_subscriber_loop())
                try:
                    async for message in ws:
                        try:
                            payload = json.loads(message)
                        except Exception:
                            continue
                        if "k" in payload:  # rohes /ws-Format liefert Events direkt (keine "stream"-Huelle)
                            _apply_kline_event(market_type, payload)
                finally:
                    sub_task.cancel()
        except Exception as e:
            debug_log(f"⚠️ [WS-Cache] Verbindung getrennt ({market_type}), reconnect in {backoff}s", {"error": str(e)})
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)


async def binance_ws_cache_loop():
    """In main.py's asyncio.gather einhaengen - haelt je eine dauerhafte WS-Verbindung
    fuer Spot und Futures am Laufen (jede mit eigenem Reconnect-Backoff)."""
    await asyncio.gather(
        _websocket_loop("spot"),
        _websocket_loop("futures"),
    )
