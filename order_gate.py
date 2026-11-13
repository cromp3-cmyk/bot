"""
Zentrale Sende-Schleuse fuer ALLE Lighter-Orders (Grid Classic, Grid-Scalp, Trendfolge, Pivot, Copytrading, ...).

Was sie macht:
  1. EIN Sende-Lock fuer alle Coins/Strategien (wiedereintrittsfaehig, damit verschachtelte Aufrufe nicht blockieren).
  2. Mindestabstand zwischen zwei Sendungen (MIN_GAP), damit der Bot das Anfragelimit gar nicht erst reisst.
  3. Backoff bei Anfragelimit (429 / 23000 / WAF 405): 15 s, dann 30, 60, max. 120 s. Nach 5 ruhigen Minuten wieder von vorn.
     Die Pause gilt fuer ALLE Coins gemeinsam (das Limit gilt pro IP/Konto, nicht pro Coin).
  4. Sicherung (Circuit Breaker): 5 Ablehnungen in Folge fuer denselben Coin -> dieser Coin sendet 60 s lang keine NEUEN Orders.
     Stornos und Notausstiege (Markt-Orders) werden davon nie blockiert.
  5. Nonce-Fehler (21104): Nonce frisch von der Boerse holen und neu senden (bis zu 3 Versuche).
"""
import asyncio
import random
import time

MIN_GAP = 0.15            # Sekunden zwischen zwei Sendungen
BACKOFF_START = 15.0
BACKOFF_MAX = 120.0
BACKOFF_RESET_AFTER = 300.0
BREAKER_FAILS = 5
BREAKER_PAUSE = 60.0
NONCE_RETRIES = 3


class _ReentrantLock:
    """asyncio.Lock, den dieselbe Task mehrfach nehmen darf (z. B. Grid-Code haelt ihn und ruft place_market_order, das ihn auch nimmt)."""

    def __init__(self):
        self._lock = asyncio.Lock()
        self._owner = None
        self._count = 0

    async def __aenter__(self):
        task = asyncio.current_task()
        if self._owner is task:
            self._count += 1
            return self
        await self._lock.acquire()
        self._owner = task
        self._count = 1
        return self

    async def __aexit__(self, *exc):
        self._count -= 1
        if self._count <= 0:
            self._owner = None
            self._count = 0
            self._lock.release()
        return False


ORDER_LOCK = _ReentrantLock()

_last_send = 0.0
_pause_until = 0.0
_backoff = 0.0
_last_rate_err = 0.0
_fail_streak = {}        # key (Coin) -> Anzahl Ablehnungen in Folge
_breaker_until = {}      # key (Coin) -> Zeitpunkt


def _log(msg):
    try:
        from bot_core import debug_log
        debug_log(msg)
    except Exception:
        print(msg, flush=True)


def is_rate_err(err):
    s = str(err) if err is not None else ""
    return ("429" in s or "Too Many Requests" in s or "23000" in s or "405" in s
            or "Human Verification" in s or "awswaf" in s)


def is_nonce_err(err):
    s = str(err) if err is not None else ""
    return "21104" in s or "invalid nonce" in s.lower()


def rate_pause_remaining():
    return max(0.0, _pause_until - time.time())


def breaker_remaining(key):
    return max(0.0, _breaker_until.get(key, 0.0) - time.time())


def on_rate_limit(who=""):
    """Anfragelimit getroffen -> gemeinsame Pause mit steigender Dauer. Gibt die Pausendauer in Sekunden zurueck."""
    global _pause_until, _backoff, _last_rate_err
    now = time.time()
    if now - _last_rate_err > BACKOFF_RESET_AFTER:
        _backoff = 0.0
    _backoff = BACKOFF_START if _backoff <= 0 else min(BACKOFF_MAX, _backoff * 2)
    _last_rate_err = now
    wait = _backoff * (1 + random.random() * 0.25)
    _pause_until = max(_pause_until, now + wait)
    return wait


def _err_of(res):
    if isinstance(res, tuple) and len(res) >= 3:
        return res[2]
    return None


async def _refresh_nonce(client):
    """Nonce frisch von der Boerse holen (je nach SDK-Version synchron oder async)."""
    if client is None:
        return
    try:
        nm = getattr(client, "nonce_manager", None)
        if nm is None:
            return
        key = getattr(client, "api_key_index", None)
        fn = getattr(nm, "hard_refresh_nonce", None)
        if fn is not None:
            r = fn(key)
            if asyncio.iscoroutine(r):
                await r
    except Exception:
        pass


async def send(fn, *, kind="place", key="", client=None, urgent=False):
    """fn: async Funktion ohne Argumente, die die Order sendet und (tx, tx_hash, err) liefert.
    kind: "place" (neue Limit-Order), "cancel", "market" (Notausstieg/Einstieg per Markt).
    urgent=True: Pause und Sicherung ignorieren (nur Mindestabstand) - fuer Notausstiege."""
    global _last_send
    res = None
    for attempt in range(NONCE_RETRIES):
        async with ORDER_LOCK:
            if not urgent:
                if rate_pause_remaining() > 0:
                    return None, None, f"Pause wegen Anfragelimit ({rate_pause_remaining():.0f}s)"
                if kind == "place" and breaker_remaining(key) > 0:
                    return None, None, f"Sicherung aktiv ({breaker_remaining(key):.0f}s)"
            gap = MIN_GAP - (time.time() - _last_send)
            if gap > 0:
                await asyncio.sleep(gap)
            try:
                res = await fn()
            except Exception as e:
                _last_send = time.time()
                if is_rate_err(e):
                    w = on_rate_limit(key)
                    _log(f"🚦 [{key}] Anfragelimit beim Senden - alle Coins pausieren {w:.0f}s")
                raise
            _last_send = time.time()
        err = _err_of(res)
        if err and is_nonce_err(err):
            await _refresh_nonce(client)
            await asyncio.sleep(0.4 * (attempt + 1))
            continue
        break

    err = _err_of(res)
    if err:
        if is_rate_err(err):
            w = on_rate_limit(key)
            _log(f"🚦 [{key}] Anfragelimit (Antwort der Boerse) - alle Coins pausieren {w:.0f}s")
        elif kind == "place" and not is_nonce_err(err):
            n = _fail_streak.get(key, 0) + 1
            _fail_streak[key] = n
            if n >= BREAKER_FAILS:
                _fail_streak[key] = 0
                _breaker_until[key] = time.time() + BREAKER_PAUSE
                _log(f"🛑 [{key}] {BREAKER_FAILS} abgelehnte Orders in Folge - neue Orders fuer diesen Coin {BREAKER_PAUSE:.0f}s pausiert (Sicherung). Letzter Fehler: {str(err)[:160]}")
    elif kind == "place":
        _fail_streak[key] = 0
    return res
