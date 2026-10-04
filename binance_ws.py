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

# War vorher 1500 - fuer synthetische Sekunden-Aufloesungen (10s/15s/30s/45s, siehe strategies.py
# resolve_synthetic_resolution) reicht das NICHT: die werden aus dem "1s"-Basis-Stream
# zusammengerechnet, und ein Verbraucher, der z.B. mindestens 51 fertige 30s-Kerzen braucht,
# braucht dafuer 51*30=1530 rohe 1s-Kerzen - mehr als die alten 1500 ueberhaupt je liefern
# konnten (der Puffer haette also SELBST BEI VOLLER FUELLUNG nie genug geliefert, ganz unabhaengig
# vom separat gefixten 1000er-Deckel in strategies.py's fetch_candles_binance_vol). Live beobachtet:
# "wartet: zu wenig Kerzen (33/51 nötig)" haengt DAUERHAFT fest, nicht nur waehrend des Aufwaermens.
# 20000 deckt auch 45s-Kerzen mit hohem count_back komfortabel ab; der Speicher-Mehrbedarf ist
# trivial (kleine Dicts, gilt zudem nur fuer den "1s"-Stream in der Praxis - andere Intervalle
# fuellen sich so langsam, dass sie diese Grenze nie erreichen).
MAX_CANDLES_PER_STREAM = 20000

# Wie viele Kerzen beim erstmaligen Abonnieren eines Streams per REST vorgeladen werden
# (EINMALIG pro Stream, nicht wiederholt - danach nur noch WS-Push).
REST_SEED_LIMIT = {
    "1s": 1000, "1m": 1000, "3m": 1000, "5m": 1000,
    "15m": 500, "30m": 500, "1h": 300, "4h": 200,
}

# Wie viele REST-Seiten (je REST_SEED_LIMIT Kerzen) rueckwaerts beim Seed geholt werden. Nur "1s": dort reichen 1000
# Kerzen (~16 Min) fuer 15s/30s-Charts nicht aus.
SEED_PAGES = {"1s": 4}

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
    __slots__ = ("candles", "ready", "last_update_ts", "saved_ts")

    def __init__(self):
        self.candles = deque(maxlen=MAX_CANDLES_PER_STREAM)  # Dicts: ts,o,h,l,c,v
        self.ready = False  # True, sobald der einmalige REST-Seed durch ist
        self.last_update_ts = 0.0  # time.time() der letzten WS-Aktualisierung (oder des Seeds)
        self.saved_ts = 0.0  # time.time() der letzten Sicherung in Redis (siehe snapshot_loop)


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


def get_cached_candles_ext(market_type, pair, interval, count_back):
    """Wie get_cached_candles, liefert aber die Kerzen als Liste von Dicts MIT den Zusatzfeldern
    'tb' (Taker-Buy-Basisvolumen) und 'n' (Trades) - fuer den Coin-Screener. Gleiche Frische-/
    Warm-Regeln: None, solange der Stream nicht warm oder eingefroren ist. Rein lesend, kein
    REST-Fallback (der Screener soll NIE selbst Binance anfragen)."""
    st = _streams.get(market_type, {}).get(_key(pair, interval))
    if st is None or not st.ready or not st.candles:
        return None
    max_staleness = INTERVAL_SECONDS.get(interval, 60) * STALENESS_MULTIPLIER + STALENESS_BUFFER_SECONDS
    if time.time() - st.last_update_ts > max_staleness:
        # eingefroren: wie in get_cached_candles neu abonnieren lassen (sonst koennte ein Stream, den
        # NUR der Screener nutzt, nie wieder anspringen)
        st.ready = False
        _pending_subscribe[market_type].add(_key(pair, interval))
        return None
    return [dict(c) for c in list(st.candles)[-count_back:]]


# ============================================================================
# KERZEN-SNAPSHOT IN REDIS (ueberlebt Deploys/Neustarts)
# ----------------------------------------------------------------------------
# Problem: nach jedem Deploy war der Kerzen-Cache leer, und JEDER Stream (Coin x Zeitrahmen) hat seine komplette
# Historie neu per REST geholt - viele Anfragen auf einmal = IP-Bann bei Binance. Jetzt wird der Cache regelmaessig
# komprimiert in Redis gesichert. Beim Start laedt jeder Stream zuerst seinen Snapshot und holt nur noch die
# Kerzen nach, die waehrend des Neustarts gefehlt haben (meist 0-3 Stueck). Fehlt der Snapshot oder ist er zu alt,
# laeuft der bisherige komplette Seed wie gehabt.
# ============================================================================
SNAPSHOT_INTERVAL_SECONDS = 120   # wie oft gesichert wird (nur Streams mit neuen Daten)
SNAPSHOT_TTL_SECONDS = 3 * 86400  # Redis raeumt alte Snapshots selbst auf
SNAPSHOT_MIN_CANDLES = 30         # kleinere Snapshots sind nicht den Aufwand wert


def _snap_key(market_type, pair, interval):
    return f"bwscache:{market_type}:{pair}:{interval}"


def _seed_window(interval):
    """So viele Kerzen deckt ein kompletter Seed ab - aeltere Snapshots sind nicht mehr 'nur eine kleine Luecke'."""
    return REST_SEED_LIMIT.get(interval, 500) * SEED_PAGES.get(interval, 1)


def _k_to_candle(k):
    return {
        "ts": int(k[0]), "o": float(k[1]), "h": float(k[2]), "l": float(k[3]), "c": float(k[4]), "v": float(k[5]),
        # Zusatzfelder fuer den Coin-Screener (Moneyflow/Buyers%): REST-Kline-Index 8 = Anzahl Trades, 9 = Taker-Buy-Basisvolumen.
        "tb": float(k[9]) if len(k) > 9 else 0.0,
        "n": int(k[8]) if len(k) > 8 else 0,
    }


def _encode_snapshot(candles):
    import base64
    import zlib
    rows = [[c["ts"], c["o"], c["h"], c["l"], c["c"], c["v"], c.get("tb", 0.0), c.get("n", 0)] for c in candles]
    return base64.b64encode(zlib.compress(json.dumps(rows, separators=(",", ":")).encode(), 6)).decode()


def _decode_snapshot(raw):
    import base64
    import zlib
    rows = json.loads(zlib.decompress(base64.b64decode(raw)).decode())
    out = [{"ts": int(r[0]), "o": float(r[1]), "h": float(r[2]), "l": float(r[3]), "c": float(r[4]), "v": float(r[5]),
            "tb": float(r[6]), "n": int(r[7])} for r in rows]
    if any(out[i]["ts"] >= out[i + 1]["ts"] for i in range(len(out) - 1)):
        return None  # nicht streng aufsteigend -> beschaedigt, lieber verwerfen
    return out


async def _load_snapshot(market_type, pair, interval):
    """-> Liste von Kerzen-Dicts oder None. Fehler/fehlendes Redis sind kein Problem (dann normaler Seed)."""
    try:
        from bot_core import get_redis
        r = await get_redis()
        if r is None:
            return None
        raw = await asyncio.wait_for(r.get(_snap_key(market_type, pair, interval)), timeout=5)
        if not raw:
            return None
        candles = await asyncio.to_thread(_decode_snapshot, raw)
        if not candles or len(candles) < SNAPSHOT_MIN_CANDLES:
            return None
        return candles
    except Exception as e:
        debug_log(f"⚠️ [WS-Cache] Snapshot laden fehlgeschlagen ({pair} {interval})", {"error": str(e)})
        return None


async def _save_snapshot(market_type, pair, interval, st):
    from bot_core import get_redis
    r = await get_redis()
    if r is None:
        return False
    keep = _seed_window(interval)
    candles = [dict(c) for c in list(st.candles)[-keep:]]
    if len(candles) < SNAPSHOT_MIN_CANDLES:
        return False
    data = await asyncio.to_thread(_encode_snapshot, candles)
    await asyncio.wait_for(r.set(_snap_key(market_type, pair, interval), data, ex=SNAPSHOT_TTL_SECONDS), timeout=5)
    return True


async def snapshot_loop():
    """In binance_ws_cache_loop eingehaengt: sichert alle SNAPSHOT_INTERVAL_SECONDS die Streams, die seit der letzten
    Sicherung neue Daten bekommen haben (Redis-Schreibzugriffe nacheinander, nicht als Schwung)."""
    await asyncio.sleep(SNAPSHOT_INTERVAL_SECONDS)
    while True:
        saved = 0
        try:
            for market_type in ("spot", "futures"):
                for k, st in list(_streams.get(market_type, {}).items()):
                    if not st.ready or not st.candles or st.last_update_ts <= st.saved_ts:
                        continue
                    pair, interval = k.split("|")
                    try:
                        t_mark = time.time()
                        if await _save_snapshot(market_type, pair, interval, st):
                            st.saved_ts = t_mark
                            saved += 1
                    except Exception as e:
                        debug_log(f"⚠️ [WS-Cache] Snapshot speichern fehlgeschlagen ({pair} {interval})", {"error": str(e)})
                    await asyncio.sleep(0.05)
        except Exception as e:
            debug_log("⚠️ [WS-Cache] Snapshot-Runde fehlgeschlagen", {"error": str(e)})
        if saved:
            debug_log(f"💾 [WS-Cache] {saved} Kerzen-Snapshots in Redis gesichert")
        await asyncio.sleep(SNAPSHOT_INTERVAL_SECONDS)


def _merge_into_stream(st, *candle_lists):
    """Fuehrt Kerzenlisten zu einer lueckenlos aufsteigenden Reihe zusammen. Spaetere Listen gewinnen bei gleichem
    Zeitstempel; schon im Stream liegende (per WS eingetroffene) Kerzen haben zuletzt Vorrang."""
    d = {}
    for lst in candle_lists:
        for c in lst:
            d[c["ts"]] = c
    for c in st.candles:
        d[c["ts"]] = c
    merged = [d[t] for t in sorted(d)]
    st.candles.clear()
    st.candles.extend(merged)


async def _seed_stream_history(market_type, pair, interval):
    """Laedt einmalig Historie fuer einen frisch abonnierten Stream. Reihenfolge:
      1. Snapshot aus Redis laden (falls vorhanden und nicht zu alt) -> es muessen nur die Kerzen der Luecke nachgeholt
         werden (oft gar keine: dann KEINE REST-Anfrage).
      2. Sonst kompletter REST-Seed wie bisher (Bann-/Throttle-Infrastruktur aus strategies.py, per Lazy-Import, um
         einen Zirkelimport beim Modul-Laden zu vermeiden)."""
    import aiohttp
    from strategies import _binance_throttle, _binance_is_banned, _binance_register_ban, _binance_note_response

    interval_ms = INTERVAL_SECONDS.get(interval, 60) * 1000
    snapshot = await _load_snapshot(market_type, pair, interval)
    gap_start = None   # ab diesem Zeitstempel (inkl.) muss nachgeholt werden
    if snapshot:
        last_ts = snapshot[-1]["ts"]
        missing = int(time.time() * 1000 - last_ts) // interval_ms + 1   # inkl. der damals evtl. noch laufenden Kerze
        if missing > _seed_window(interval):
            snapshot = None  # Luecke groesser als ein kompletter Seed -> gleich alles frisch holen
        elif missing <= 1:
            # Letzte gesicherte Kerze ist noch die aktuelle: nichts fehlt, der WS liefert sie sowieso gleich komplett.
            st = _streams[market_type].get(_key(pair, interval))
            if st is None:
                return
            _merge_into_stream(st, snapshot)
            st.ready = True
            st.last_update_ts = time.time()
            debug_log(f"✅ [WS-Cache] {pair} {interval} ({market_type}) aus Snapshot bereit - keine REST-Anfrage nötig", {"kerzen": len(st.candles)})
            return
        else:
            gap_start = last_ts   # die damals laufende (unvollstaendige) Kerze wird mit neu geholt

    if _binance_is_banned(market_type):
        # Aktiver Bann - Seed spaeter nachholen, damit wir ihn nicht verlaengern. Der
        # Stream bleibt in _streams (ready=False), also faellt der Aufrufer bis dahin
        # weiterhin auf REST zurueck, ohne dass etwas verloren geht.
        _pending_subscribe[market_type].add(_key(pair, interval))
        return

    base_url = BINANCE_BASE_URLS.get(market_type, BINANCE_BASE_URLS["spot"])
    limit = REST_SEED_LIMIT.get(interval, 500)
    pages = SEED_PAGES.get(interval, 1)  # 1s: mehrere Seiten rueckwaerts (Scalp-Dashboard braucht mehr Sekunden-Historie)
    data = []
    try:
        if gap_start is not None:
            # NUR DIE LUECKE: vorwaerts ab der letzten gesicherten Kerze bis jetzt
            cursor = gap_start
            for page in range(6):
                await _binance_throttle(market_type, f"seed:{interval}")
                if _binance_is_banned(market_type):
                    _pending_subscribe[market_type].add(_key(pair, interval))
                    return
                url = f"{base_url}?symbol={pair}&interval={interval}&startTime={cursor}&limit={limit}"
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                        _binance_note_response(market_type, resp)
                        if resp.status in (418, 429):
                            body = await resp.text()
                            _binance_register_ban(market_type, pair, resp.status, body)
                            return
                        if resp.status != 200:
                            return
                        chunk = await resp.json()
                if not chunk or not isinstance(chunk, list):
                    break
                data.extend(chunk)
                cursor = int(chunk[-1][0]) + 1
                if len(chunk) < limit or cursor >= time.time() * 1000:
                    break
        else:
            end_time = None
            for page in range(pages):
                await _binance_throttle(market_type, f"seed:{interval}")
                # ERNEUT pruefen NACH dem Warten: waehrend der Drossel-Pause kann eine ANDERE gleichzeitig
                # laufende Coin-Abfrage (viele Streams werden beim Bot-Start fast zeitgleich abonniert) in
                # der Zwischenzeit einen Bann registriert haben - ohne diesen zweiten Check wuerden wir
                # trotzdem noch feuern und einen aktiven Bann bei Binance nur weiter verlaengern.
                if _binance_is_banned(market_type):
                    if page == 0:
                        _pending_subscribe[market_type].add(_key(pair, interval))
                        return
                    break  # Bann mitten im Nachladen: mit dem, was da ist, weitermachen
                url = f"{base_url}?symbol={pair}&interval={interval}&limit={limit}"
                if end_time is not None:
                    url += f"&endTime={end_time}"
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                        _binance_note_response(market_type, resp)
                        if resp.status in (418, 429):
                            body = await resp.text()
                            _binance_register_ban(market_type, pair, resp.status, body)
                            if page == 0:
                                return
                            break
                        if resp.status != 200:
                            if page == 0:
                                return
                            break
                        chunk = await resp.json()
                if not chunk or not isinstance(chunk, list):
                    break
                data = chunk + data  # aeltere Seite DAVOR
                end_time = int(chunk[0][0]) - 1
                if len(chunk) < limit:
                    break
    except Exception as e:
        debug_log(f"⚠️ [WS-Cache] Seed-Historie fehlgeschlagen ({pair} {interval})", {"error": str(e)})
        if not data:
            return

    if (not data or not isinstance(data, list)) and not snapshot:
        return

    st = _streams[market_type].get(_key(pair, interval))
    if st is None:
        return  # Stream wurde inzwischen entfernt - nichts mehr zu tun
    # zusammenfuehren statt anhaengen: waehrend des Seeds sind evtl. schon WS-Kerzen eingetroffen (Reihenfolge/Dubletten)
    _merge_into_stream(st, snapshot or [], [_k_to_candle(k) for k in (data or [])])
    st.ready = True
    st.last_update_ts = time.time()
    debug_log(f"✅ [WS-Cache] {pair} {interval} ({market_type}) bereit" + (" (Snapshot + Lücke nachgeholt)" if snapshot else ""),
              {"kerzen": len(st.candles), "nachgeholt": len(data or [])})


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
        # Kline-Event: "V" = Taker-Buy-Basisvolumen, "n" = Anzahl Trades (fuer den Coin-Screener)
        "tb": float(k.get("V", 0.0)), "n": int(k.get("n", 0)),
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
        snapshot_loop(),
    )
