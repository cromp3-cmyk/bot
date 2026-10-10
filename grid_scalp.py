"""
grid_scalp.py - Maker-Only Grid-Scalper (entry_mode "grid_scalp")

WARUM EIN EIGENES MODUL UND KEIN PATCH IM GRID:
Der komplette Bot laeuft ueber place_market_order() mit IOC - eine Order fuellt
sofort, danach pollt _execute_entry_locked() den echten Ø-Einstieg von der Boerse.
Eine Post-Only-Order fuellt NICHT sofort, sie liegt im Buch. Damit bricht dieser
Ablauf komplett. Deshalb bringt dieses Modul seine eigene Order-Schicht mit
(post-only posten, canceln, offene Orders lesen) - die gab es im Bot bisher nirgends.

ARCHITEKTUR: deklarative Reconciliation statt Event-Tracking.
Jeder Tick liest den IST-Zustand von der Boerse (Position + offene Orders),
berechnet den SOLL-Zustand und gleicht die Differenz an. Fills werden nicht
mitgeschrieben, sondern aus der Differenz erkannt. Das ueberlebt Render-Redeploys,
WS-Abbrueche und verpasste Fills - event-basiertes Tracking tut das nicht.

Der lokale BOTS[symbol]["state"] wird dabei aus der Boersen-Wahrheit gespiegelt,
damit Dashboard/PnL/Trade-Log weiter stimmen.
"""

import asyncio
import os
import time
import traceback

from bot_core import (
    BOTS, SYMBOLS, MARKET_INDICES, debug_log, save_bot_state,
    get_lighter_client, get_precision, get_price_decimals, get_min_base_amount,
    get_account_position_from_exchange, now_local, EXCHANGE_CALL_TIMEOUT_SECONDS,
)


# ============================================================================
# Config-Defaults - in bot_core.py in den Config-Block einfuegen (siehe PATCH.md)
# ============================================================================

GRID_SCALP_DEFAULTS = {
    "gs_step_notional_usd": 1000.0,   # Notional pro Stufe. Obergrenze kommt vom Spread!
    "gs_max_levels": 5,
    "gs_step_pct": 0.10,              # Abstand zwischen Nachkauf-Stufen in %
    "gs_tp_usd": 1.00,                # echter $-Gewinn auf die GESAMTposition
    "gs_flatten_usd": 25.0,           # Notausstieg als Market bei diesem uPnL
    "gs_cooldown_min": 30.0,
    "gs_anchor_follow_pct": 1.0,
    "gs_requote_ticks": 2,            # Order neu setzen ab dieser Preisdrift
    "gs_max_open_orders": 8,
    "gs_poll_seconds": 2.0,
}

GRID_SCALP_STATE_KEYS = {
    "gs_anchor": None,
    "gs_cooldown_until": 0.0,
    "gs_tag_map": {},        # tag -> client_order_index
    "gs_last_error": None,
    "gs_last_heartbeat": 0.0,
    "gs_last_error_log": 0.0,
    # Dry-Run-Simulation
    "gs_sim_orders": [],
    "gs_sim_pos": 0.0,
    "gs_sim_avg": None,
    "gs_sim_stats": {"trades": 0, "gewinne": 0, "verluste": 0, "pnl": 0.0},
}


MAKER_DEFAULTS = {
    "ms_notional_usd": 300.0,     # Notional je Order
    "ms_tp_bps": 5.0,             # Take-Profit ab Durchschnittseinstieg (1 bps = 0.01%)
    "ms_sl_bps": 30.0,            # harter Stopp gegen die Position
    "ms_add_step_bps": 4.0,       # Mindestabstand des Nachkaufs zum Durchschnittseinstieg
    "ms_max_levels": 3,           # Inventar-Limit (Einstieg + Nachkaeufe)
    "ms_vol_mult": 2.0,           # 1m-Spanne > x * Durchschnitt -> Entry-Orders weg
    "ms_vol_pause_s": 60,
    "ms_sl_pause_s": 300,         # Pause nach hartem Stopp
    "ms_max_sl_per_day": 3,       # danach Pause bis Tageswechsel (UTC)
    "ms_ema_fast": 20,
    "ms_ema_slow": 50,
    "ms_flat_band_bps": 1.0,      # EMA-Abstand darunter = "flat" (beide Seiten quoten)
}
TICK_DEFAULTS = {
    "ts_notional_usd": 300.0,     # Notional je Order (eine Position gleichzeitig)
    "ts_direction": "both",       # long / short / both
    "ts_imb_min": 0.15,           # Orderbuch-Ungleichgewicht (-1..+1), ab dem eingestiegen wird (0 = aus)
    "ts_min_spread_ticks": 1,     # nur quoten, wenn der Spread mindestens so viele Ticks hat
    "ts_tp_ticks": 3,             # Take-Profit in Ticks ab Einstieg (Post-only, reduce-only)
    "ts_sl_ticks": 15,            # harter Stopp in Ticks gegen die Position (0 = aus)
    "ts_max_hold_s": 60,          # Zeitlimit: nach so vielen Sekunden zum Marktpreis raus (0 = aus)
    "ts_entry_ttl_s": 20,         # unbefuellte Einstiegsorder nach so vielen Sekunden neu setzen
    "ts_max_losses_row": 4,       # Verluste in Folge -> Pause
    "ts_pause_s": 300,
    "ts_daily_loss_usd": 10.0,    # Tagesverlust-Limit (0 = aus), Pause bis Tageswechsel (UTC)
    "ts_max_orders_min": 40,      # Order-Budget (posten+stornieren) pro Minute, nur live
    "ts_poll_seconds": 1.0,       # live wird intern auf mind. 4 s begrenzt (REST fuer Position/Orders)
    "ts_book_max_age_s": 5.0,     # aelter -> WS-Buch gilt als veraltet, REST-Fallback
}
MODES = ("grid_scalp", "maker_scalp", "grid_classic", "tick_scalp")


def _ts(cfg, key):
    v = cfg.get(key)
    return TICK_DEFAULTS[key] if v is None else v


def _ms(cfg, key):
    v = cfg.get(key)
    return MAKER_DEFAULTS[key] if v is None else v


# ============================================================================
# Order-Schicht (neu - existierte im Bot bisher nicht)
# ============================================================================

_order_calls = []   # Zeitstempel aller posten/stornieren-Aufrufe (Order-Budget des Tick-Scalpers)


def _orders_last_min():
    now = time.time()
    while _order_calls and now - _order_calls[0] > 60:
        _order_calls.pop(0)
    return len(_order_calls)


async def place_post_only_order(client, market_index, symbol, is_ask, base_amount,
                                price, coi, reduce_only=False):
    """Post-Only-Limit. Wuerde die Order das Buch kreuzen, verwirft die Boerse sie -
    das ist KEIN Fehler, sondern der Sinn von Post-Only. Naechster Tick setzt neu."""
    _order_calls.append(time.time())
    price_decimals = get_price_decimals(symbol)
    price_scaled = int(round(price * (10 ** price_decimals)))
    # Timeout wie bei place_market_order in bot_core.py: grid_scalp_poll_loop(symbol) laeuft zwar
    # als eigene Task pro Coin (ein Haenger hier reisst andere Coins nicht mit) - ohne Zeitlimit
    # wuerde ein haengender Aufruf aber trotzdem GENAU DIESEN Coin fuer immer einfrieren, ohne
    # jemals eine Exception zu werfen, die der try/except im Poll-Loop auffangen koennte.
    try:
        tx, tx_hash, err = await asyncio.wait_for(client.create_order(
            market_index=market_index,
            client_order_index=coi,
            base_amount=base_amount,
            price=price_scaled,
            is_ask=is_ask,
            order_type=client.ORDER_TYPE_LIMIT,
            time_in_force=client.ORDER_TIME_IN_FORCE_POST_ONLY,
            reduce_only=reduce_only,
            order_expiry=client.DEFAULT_28_DAY_ORDER_EXPIRY,
        ), timeout=EXCHANGE_CALL_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        debug_log(f"⚠️ [{symbol}] Grid-Scalp Post-Only-Order Timeout nach {EXCHANGE_CALL_TIMEOUT_SECONDS}s")
        return None, None, f"Timeout nach {EXCHANGE_CALL_TIMEOUT_SECONDS}s"
    return tx, tx_hash, err


async def cancel_order(client, market_index, coi):
    _order_calls.append(time.time())
    return await asyncio.wait_for(client.cancel_order(market_index=market_index, order_index=coi), timeout=EXCHANGE_CALL_TIMEOUT_SECONDS)


_auth_cache = {}  # account_index -> (token, expires_ts)


async def _auth_token(client):
    key = client.account_index
    cached = _auth_cache.get(key)
    if cached and time.time() < cached[1] - 30:
        return cached[0]
    token, err = client.create_auth_token_with_expiry()
    if err:
        raise RuntimeError(f"Auth-Token fehlgeschlagen: {err}")
    _auth_cache[key] = (token, time.time() + 600)
    return token


_sig_cache = {}


def _resolve_kwargs(func, wanted):
    """Baut die Kwargs aus dem, was die Funktion TATSAECHLICH akzeptiert.

    Grund: die Parameternamen der Lighter-SDK haben sich zwischen Versionen geaendert
    (auth / authorization, market_id / market_index). Hart verdrahtete Namen brechen
    dann bei jedem SDK-Update. wanted ist {kandidat_name: wert} - jeder Kandidat wird
    nur uebernommen, wenn die Signatur ihn kennt. Akzeptiert die Funktion **kwargs,
    wird alles durchgereicht.

    Nicht untergebrachte Werte kommen als zweiter Rueckgabewert zurueck, damit der
    Aufrufer entscheiden kann (z.B. Auth stattdessen als Header setzen).
    """
    import inspect
    key = f"{func.__module__}.{func.__qualname__}"
    if key not in _sig_cache:
        try:
            params = inspect.signature(func).parameters
            _sig_cache[key] = (
                set(params.keys()),
                any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()),
            )
            debug_log(f"\U0001f50e SDK-Signatur erkannt: {func.__qualname__}",
                      {"parameter": sorted(_sig_cache[key][0])})
        except (TypeError, ValueError):
            _sig_cache[key] = (set(), True)

    known, has_varkw = _sig_cache[key]
    kwargs, uebrig = {}, {}
    for gruppe, wert in wanted.items():
        untergebracht = False
        for name in gruppe.split("|"):
            if name in known or has_varkw:
                kwargs[name] = wert
                untergebracht = True
                break
        if not untergebracht:
            uebrig[gruppe] = wert
    return kwargs, uebrig


async def read_open_orders(client, market_index):
    """Gibt Liste von Dicts zurueck: coi, is_ask, price, size, reduce_only."""
    import lighter
    order_api = lighter.OrderApi(client.api_client)
    token = await _auth_token(client)

    kwargs, uebrig = _resolve_kwargs(order_api.account_active_orders, {
        "account_index": client.account_index,
        "market_id|market_index": market_index,
        "auth|authorization": token,
    })

    # Kennt die Signatur gar keinen Auth-Parameter, erwartet die SDK-Version den Token
    # als Header. Beides einmal versuchen ist billiger als es falsch zu raten.
    if "auth|authorization" in uebrig:
        try:
            client.api_client.default_headers["Authorization"] = token
        except Exception:
            pass

    resp = await order_api.account_active_orders(**kwargs)
    out = []
    for o in (getattr(resp, "orders", None) or []):
        try:
            out.append({
                "coi": int(o.client_order_index),
                "is_ask": bool(o.is_ask),
                "price": float(o.price),
                "size": float(o.remaining_base_amount),
                "reduce_only": bool(getattr(o, "reduce_only", False)),
            })
        except (TypeError, ValueError, AttributeError):
            continue
    return out


async def read_best_bid_ask(client, market_index):
    import lighter
    order_api = lighter.OrderApi(client.api_client)
    ob = await asyncio.wait_for(order_api.order_book_orders(market_index, 1), timeout=EXCHANGE_CALL_TIMEOUT_SECONDS)
    if not ob.bids or not ob.asks:
        return None, None
    return float(ob.bids[0].price), float(ob.asks[0].price)


# ============================================================================
# Lighter-Orderbuch per WebSocket (fuer tick_scalp) - ersetzt das REST-Polling von order_book_orders.
# Eine Verbindung fuer alle Maerkte, Snapshot + Deltas (size 0 = Level entfernt). Ist das Buch aelter als
# ts_book_max_age_s oder gekreuzt (verpasstes Delta), faellt get_top() auf REST zurueck (max. alle 5 s)
# und abonniert den Markt neu.
# ============================================================================

_book = {}            # market_index -> {"bids": {preis: groesse}, "asks": {...}, "t": letzte Aenderung}
_book_want = set()
_book_resub = set()
_book_task = None
_book_info = {"connected": False, "msgs": 0, "reconnects": 0, "last_error": None}
_rest_top = {}        # market_index -> (t, bid, ask)


def _book_apply(mi, ob, snapshot):
    b = _book.setdefault(mi, {"bids": {}, "asks": {}, "t": 0.0})
    if snapshot:
        b["bids"].clear()
        b["asks"].clear()
    for side in ("bids", "asks"):
        for lv in (ob.get(side) or []):
            try:
                p = float(lv["price"])
                sz = float(lv["size"])
            except (KeyError, TypeError, ValueError):
                continue
            if sz <= 0:
                b[side].pop(p, None)
            else:
                b[side][p] = sz
    b["t"] = time.time()


def _book_handle(msg):
    """Verarbeitet eine WS-Nachricht (dict). Gibt 'pong' zurueck, wenn geantwortet werden muss."""
    typ = str(msg.get("type", ""))
    if typ == "ping":
        return "pong"
    if typ in ("subscribed/order_book", "update/order_book"):
        ch = str(msg.get("channel", ""))
        try:
            mi = int(ch.replace("order_book:", "").replace("order_book/", ""))
        except ValueError:
            return None
        _book_apply(mi, msg.get("order_book") or {}, typ.startswith("subscribed"))
        _book_info["msgs"] += 1
    return None


async def _book_ws_loop():
    import json
    import websockets
    from bot_core import WS_URL
    wait = 2.0
    while True:
        subscribed = set()
        sub_task = None
        try:
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                _book_info["connected"] = True
                wait = 2.0

                async def _subscriber():
                    while True:
                        for mi in list(_book_resub):
                            _book_resub.discard(mi)
                            subscribed.discard(mi)
                            await ws.send(json.dumps({"type": "unsubscribe", "channel": f"order_book/{mi}"}))
                        for mi in list(_book_want - subscribed):
                            await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{mi}"}))
                            subscribed.add(mi)
                        await asyncio.sleep(1.0)

                sub_task = asyncio.create_task(_subscriber())
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except ValueError:
                        continue
                    if _book_handle(msg) == "pong":
                        await ws.send(json.dumps({"type": "pong"}))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _book_info["last_error"] = str(e)[:200]
            debug_log(f"\u26a0\ufe0f Lighter-Orderbuch-WS getrennt: {str(e)[:200]} - neu in {wait:.0f}s")
        finally:
            _book_info["connected"] = False
            _book_info["reconnects"] += 1
            for b in _book.values():
                b["t"] = 0.0
            if sub_task:
                sub_task.cancel()
        await asyncio.sleep(wait)
        wait = min(30.0, wait * 2)


def _ensure_book_task():
    global _book_task
    if _book_task is None or _book_task.done():
        _book_task = asyncio.create_task(_book_ws_loop())


def _top_from_book(b, depth=5):
    """(bid, ask, imbalance) aus dem lokalen Buch oder None, wenn leer/gekreuzt."""
    if not b or not b["bids"] or not b["asks"]:
        return None
    bids = sorted(b["bids"], reverse=True)[:depth]
    asks = sorted(b["asks"])[:depth]
    if bids[0] >= asks[0]:
        return None
    bsz = sum(b["bids"][p] for p in bids)
    asz = sum(b["asks"][p] for p in asks)
    imb = (bsz - asz) / (bsz + asz) if (bsz + asz) > 0 else 0.0
    return bids[0], asks[0], imb


async def get_top(client, market_index, max_age=5.0):
    """-> (bid, ask, imbalance, quelle 'ws'|'rest', alter_s)."""
    _book_want.add(market_index)
    _ensure_book_task()
    b = _book.get(market_index)
    age = time.time() - b["t"] if b else None
    if b and age is not None and age <= max_age:
        top = _top_from_book(b)
        if top:
            return top[0], top[1], top[2], "ws", age
        b["bids"].clear()
        b["asks"].clear()
        b["t"] = 0.0
        _book_resub.add(market_index)
    c = _rest_top.get(market_index)
    if c and time.time() - c[0] < 5.0:
        return c[1], c[2], 0.0, "rest", time.time() - c[0]
    bid, ask = await read_best_bid_ask(client, market_index)
    _rest_top[market_index] = (time.time(), bid, ask)
    return bid, ask, 0.0, "rest", 0.0


# ============================================================================
# SOLL-Zustand
# ============================================================================

def _tick_size(symbol):
    return 1.0 / (10 ** get_price_decimals(symbol))


def _desired_orders(symbol, cfg, st, pos_size, avg_entry, best_bid, best_ask):
    """pos_size: signed float (long positiv, short negativ), in Coin-Einheiten.
    Gibt Liste von Dicts zurueck: tag, is_ask, price, size, reduce_only."""
    mid = (best_bid + best_ask) / 2.0
    tick = _tick_size(symbol)
    step = cfg.get("gs_step_pct", 0.10) / 100.0
    tp_usd = cfg.get("gs_tp_usd", 1.0)
    max_levels = int(cfg.get("gs_max_levels", 5))
    step_notional = cfg.get("gs_step_notional_usd", 1000.0)
    anchor = st.get("gs_anchor") or mid

    out = []

    # --- TP auf die bestehende Position (reduce-only) ---
    if pos_size != 0 and avg_entry:
        offset = tp_usd / abs(pos_size)
        if pos_size > 0:
            tp_price = max(avg_entry + offset, best_ask)  # Post-Only darf nicht kreuzen
            out.append({"tag": "tp", "is_ask": True, "price": tp_price,
                        "size": abs(pos_size), "reduce_only": True})
        else:
            tp_price = min(avg_entry - offset, best_bid)
            out.append({"tag": "tp", "is_ask": False, "price": tp_price,
                        "size": abs(pos_size), "reduce_only": True})

    # --- Einstieg / Nachkauf ---
    base_size = step_notional / mid
    levels_used = 0
    if pos_size != 0 and avg_entry:
        levels_used = min(max_levels,
                          int(abs(pos_size) * avg_entry / max(step_notional, 1e-9) + 0.5))

    if pos_size == 0:
        # flat -> beide Seiten am Spread quoten
        out.append({"tag": "buy_0", "is_ask": False, "price": best_bid,
                    "size": base_size, "reduce_only": False})
        out.append({"tag": "sell_0", "is_ask": True, "price": best_ask,
                    "size": base_size, "reduce_only": False})
    elif pos_size > 0:
        for k in range(levels_used, max_levels):
            px = min(anchor * (1.0 - step * (k + 1)), best_bid)
            out.append({"tag": f"buy_{k+1}", "is_ask": False, "price": px,
                        "size": base_size, "reduce_only": False})
    else:
        for k in range(levels_used, max_levels):
            px = max(anchor * (1.0 + step * (k + 1)), best_ask)
            out.append({"tag": f"sell_{k+1}", "is_ask": True, "price": px,
                        "size": base_size, "reduce_only": False})

    for d in out:
        d["price"] = round(round(d["price"] / tick) * tick, get_price_decimals(symbol))

    return out[: int(cfg.get("gs_max_open_orders", 8))]


# ============================================================================
# maker_scalp: Marktkontext (Trend + Volatilitaet aus Binance-1m-Futures-Kerzen,
# kommen aus dem WS-Cache, kein zusaetzlicher REST-Traffic sobald der Stream warm ist)
# ============================================================================

_ctx_cache = {}


def _ema(values, n):
    k = 2.0 / (n + 1)
    e = values[0]
    for v in values[1:]:
        e = e + k * (v - e)
    return e


_int_candles = {}   # symbol -> {"closed": [(h,l,c)], "cur": [minute,h,l,c]}


def _feed_internal(symbol, mid):
    """Baut 1m-Kerzen aus Lighters eigenem Mid-Preis (Fallback, wenn Binance nichts liefert)."""
    d = _int_candles.setdefault(symbol, {"closed": [], "cur": None})
    m = int(time.time() // 60)
    cur = d["cur"]
    if cur is None or cur[0] != m:
        if cur is not None:
            d["closed"].append((cur[1], cur[2], cur[3]))
            if len(d["closed"]) > 300:
                del d["closed"][:len(d["closed"]) - 300]
        d["cur"] = [m, mid, mid, mid]
    else:
        cur[1] = max(cur[1], mid); cur[2] = min(cur[2], mid); cur[3] = mid


_seed_state = {}   # symbol -> {"done": bool, "last_try": ts}


async def _seed_lighter(symbol):
    import lighter
    from bot_core import BASE_URL
    configuration = lighter.Configuration(host=BASE_URL)
    async with lighter.ApiClient(configuration) as api_client:
        candle_api = lighter.CandlestickApi(api_client)
        now_ms = int(time.time() * 1000)
        resp = await candle_api.candles(
            market_id=MARKET_INDICES[symbol], resolution="1m",
            start_timestamp=now_ms - 6 * 3600 * 1000, end_timestamp=now_ms,
            count_back=150, set_timestamp_to_end=True)
    out = []
    for c in (getattr(resp, "c", None) or []):
        try:
            t = int(c.t); t = t // 1000 if t > 1e12 else t
            out.append((t, float(c.h), float(c.l), float(c.c)))
        except (TypeError, ValueError, AttributeError):
            continue
    return out


async def _seed_hyperliquid(symbol):
    import aiohttp
    now_ms = int(time.time() * 1000)
    body = {"type": "candleSnapshot", "req": {"coin": symbol, "interval": "1m",
                                               "startTime": now_ms - 3 * 3600 * 1000, "endTime": now_ms}}
    async with aiohttp.ClientSession() as sess:
        async with sess.post("https://api.hyperliquid.xyz/info", json=body,
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return []
            data = await r.json()
    return [(int(k["t"]) // 1000, float(k["h"]), float(k["l"]), float(k["c"])) for k in data]


async def _seed_internal(symbol, cfg):
    """Fuellt die eigenen 1m-Kerzen EINMALIG mit Historie (Lighter, sonst Hyperliquid) -
    dadurch entfaellt die Aufwaermzeit nach einem Deploy."""
    stt = _seed_state.setdefault(symbol, {"done": False, "last_try": 0.0})
    if stt["done"] or time.time() - stt["last_try"] < 60:
        return
    stt["last_try"] = time.time()
    candles = []
    for name, fn in (("Lighter", _seed_lighter), ("Hyperliquid", _seed_hyperliquid)):
        try:
            candles = await asyncio.wait_for(fn(symbol), timeout=15)
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Maker-Scalp Seed {name} fehlgeschlagen", {"error": str(e)})
            candles = []
        if len(candles) >= 20:
            cur_min = int(time.time() // 60)
            candles = sorted(candles)
            closed = [(h, l, c) for t, h, l, c in candles if t // 60 < cur_min]
            d = _int_candles.setdefault(symbol, {"closed": [], "cur": None})
            d["closed"] = closed[-300:] + d["closed"]
            stt["done"] = True
            debug_log(f"🌱 [{symbol}] Maker-Scalp: {len(closed)} Historien-Kerzen geladen ({name}) - keine Aufwaermzeit")
            return


async def _maker_context(symbol, cfg):
    """Gibt {"trend": up/down/flat, "vol_spike": bool, "quelle": ...} zurueck oder None.
    Quelle 1: Binance-1m-Futures (WS-Cache). Quelle 2 (Fallback): eigene 1m-Kerzen aus dem
    Lighter-Preis - braucht nach einem Neustart ca. ms_ema_slow Minuten Aufwaermzeit."""
    c = _ctx_cache.get(symbol)
    if c and time.time() - c["t"] < 5:
        return c
    slow_n = int(_ms(cfg, "ms_ema_slow"))
    closed_c = hi_r = lo_r = cur_rng = None
    quelle = None
    try:
        from strategies import fetch_candles_binance  # Lazy-Import (Zirkelimport vermeiden)
        res = await fetch_candles_binance(symbol, "1m", 120, "futures")
    except Exception:
        res = None
    if res:
        _ts, _o, hi, lo, cl = res[:5]
        if len(cl) >= slow_n + 10:
            closed_c = cl[:-1]                       # letzte Kerze laeuft noch
            hi_r, lo_r = hi[-21:-1], lo[-21:-1]
            cur_rng = hi[-1] - lo[-1]
            quelle = "binance"
    if closed_c is None:
        d = _int_candles.get(symbol)
        if d and len(d["closed"]) >= slow_n and d["cur"]:
            closed_c = [x[2] for x in d["closed"]]
            hi_r = [x[0] for x in d["closed"][-20:]]
            lo_r = [x[1] for x in d["closed"][-20:]]
            cur_rng = d["cur"][1] - d["cur"][2]
            quelle = "lighter-eigen"
    if closed_c is None:
        return None
    fast = _ema(closed_c[-(slow_n * 3):], int(_ms(cfg, "ms_ema_fast")))
    slow = _ema(closed_c[-(slow_n * 3):], slow_n)
    d_bps = (fast - slow) / slow * 1e4
    band = float(_ms(cfg, "ms_flat_band_bps"))
    trend = "up" if d_bps > band else "down" if d_bps < -band else "flat"
    ranges = [h - l for h, l in zip(hi_r, lo_r)]
    avg_rng = sum(ranges) / len(ranges) if ranges else 0.0
    spike = avg_rng > 0 and cur_rng > float(_ms(cfg, "ms_vol_mult")) * avg_rng
    ctx = {"t": time.time(), "trend": trend, "vol_spike": spike, "ema_bps": round(d_bps, 2), "quelle": quelle}
    _ctx_cache[symbol] = ctx
    return ctx


def _desired_maker(symbol, cfg, pos_size, avg_entry, best_bid, best_ask, trend, entries_allowed):
    mid = (best_bid + best_ask) / 2.0
    tick = _tick_size(symbol)
    dec = get_price_decimals(symbol)
    notional = float(_ms(cfg, "ms_notional_usd"))
    tp = float(_ms(cfg, "ms_tp_bps")) / 1e4
    step = float(_ms(cfg, "ms_add_step_bps")) / 1e4
    max_levels = int(_ms(cfg, "ms_max_levels"))
    out = []

    if pos_size != 0 and avg_entry:
        if pos_size > 0:
            out.append({"tag": "tp", "is_ask": True, "price": max(avg_entry * (1 + tp), best_ask),
                        "size": abs(pos_size), "reduce_only": True})
        else:
            out.append({"tag": "tp", "is_ask": False, "price": min(avg_entry * (1 - tp), best_bid),
                        "size": abs(pos_size), "reduce_only": True})

    if entries_allowed:
        base = notional / mid
        spread = best_ask - best_bid
        join_bid = best_bid + tick if spread > 1.5 * tick else best_bid
        join_ask = best_ask - tick if spread > 1.5 * tick else best_ask
        levels_used = int(abs(pos_size) * (avg_entry or mid) / max(notional, 1e-9) + 0.5) if pos_size else 0
        if pos_size == 0:
            if trend in ("up", "flat"):
                out.append({"tag": "buy_0", "is_ask": False, "price": join_bid, "size": base, "reduce_only": False})
            if trend in ("down", "flat"):
                out.append({"tag": "sell_0", "is_ask": True, "price": join_ask, "size": base, "reduce_only": False})
        elif pos_size > 0 and levels_used < max_levels and trend != "down":
            out.append({"tag": "buy_add", "is_ask": False, "price": min(join_bid, avg_entry * (1 - step)),
                        "size": base, "reduce_only": False})
        elif pos_size < 0 and levels_used < max_levels and trend != "up":
            out.append({"tag": "sell_add", "is_ask": True, "price": max(join_ask, avg_entry * (1 + step)),
                        "size": base, "reduce_only": False})

    for d in out:
        d["price"] = round(round(d["price"] / tick) * tick, dec)
    return out


# ============================================================================
# tick_scalp: schneller Maker-Scalper (eine Position, Tick-TP, Zeitlimit, Orderbuch-Ungleichgewicht)
# ============================================================================

def _desired_tick(symbol, cfg, pos_size, avg_entry, best_bid, best_ask, imb, entries_ok):
    mid = (best_bid + best_ask) / 2.0
    tick = _tick_size(symbol)
    dec = get_price_decimals(symbol)
    out = []
    if pos_size != 0 and avg_entry:
        n = max(1, int(_ts(cfg, "ts_tp_ticks")))
        if pos_size > 0:
            out.append({"tag": "tp", "is_ask": True, "price": max(avg_entry + n * tick, best_ask),
                        "size": abs(pos_size), "reduce_only": True})
        else:
            out.append({"tag": "tp", "is_ask": False, "price": min(avg_entry - n * tick, best_bid),
                        "size": abs(pos_size), "reduce_only": True})
    elif entries_ok:
        spread = best_ask - best_bid
        if spread >= float(_ts(cfg, "ts_min_spread_ticks")) * tick - 1e-12:
            d = str(_ts(cfg, "ts_direction"))
            need = float(_ts(cfg, "ts_imb_min"))
            if need <= 0:
                long_ok, short_ok = d in ("long", "both"), d in ("short", "both")
            else:
                long_ok = d in ("long", "both") and imb >= need
                short_ok = d in ("short", "both") and imb <= -need
            base = float(_ts(cfg, "ts_notional_usd")) / mid
            if long_ok:
                out.append({"tag": "buy_0", "is_ask": False, "price": best_bid, "size": base, "reduce_only": False})
            if short_ok:
                out.append({"tag": "sell_0", "is_ask": True, "price": best_ask, "size": base, "reduce_only": False})
    for o in out:
        o["price"] = round(round(o["price"] / tick) * tick, dec)
    return out


def _ts_track(symbol, st, cfg, pos_size, avg_entry, mid):
    """Erkennt abgeschlossene Trades (Position von !=0 auf 0), pflegt Serie/Tagesverlust und setzt Pausen."""
    stats = st.setdefault("ts_stats", {"trades": 0, "wins": 0, "pnl": 0.0, "streak": 0, "day": None, "day_pnl": 0.0})
    now = time.time()
    day = int(now // 86400)
    if stats.get("day") != day:
        stats["day"], stats["day_pnl"], stats["streak"] = day, 0.0, 0
    prev = float(st.get("ts_prev_pos") or 0.0)
    if st.get("ts_pnl_snap") is None:
        st["ts_pnl_snap"] = _sim_stats(st)["pnl"]
    if prev != 0 and pos_size == 0:
        if cfg["dry_run"]:
            pnl = _sim_stats(st)["pnl"] - float(st.get("ts_pnl_snap") or 0.0)
        else:
            pa = st.get("ts_prev_avg")
            pnl = (mid - pa) * prev if pa else 0.0      # live nur geschaetzt (Fill-Preis unbekannt)
        stats["trades"] += 1
        stats["pnl"] = round(stats["pnl"] + pnl, 4)
        stats["day_pnl"] = round(stats["day_pnl"] + pnl, 4)
        if pnl > 0:
            stats["wins"] += 1
            stats["streak"] = 0
        else:
            stats["streak"] += 1
        held = now - float(st.get("ts_pos_since") or now)
        st["ts_last_trade"] = f"{'+' if pnl >= 0 else ''}{round(pnl, 4)}$ nach {round(held)}s"
        lim_n = int(_ts(cfg, "ts_max_losses_row"))
        lim_d = float(_ts(cfg, "ts_daily_loss_usd"))
        if lim_d > 0 and stats["day_pnl"] <= -lim_d:
            until = (day + 1) * 86400
            st["gs_cooldown_until"] = max(float(st.get("gs_cooldown_until") or 0), until)
            st["ts_pause_reason"] = f"Tagesverlust {stats['day_pnl']}$ <= -{lim_d}$ - Pause bis Tageswechsel (UTC)"
            debug_log(f"\u26d4 [{symbol}] Tick-Scalp: {st['ts_pause_reason']}")
        elif lim_n > 0 and stats["streak"] >= lim_n:
            st["gs_cooldown_until"] = max(float(st.get("gs_cooldown_until") or 0), now + float(_ts(cfg, "ts_pause_s")))
            st["ts_pause_reason"] = f"{stats['streak']} Verluste in Folge - Pause {int(_ts(cfg, 'ts_pause_s'))}s"
            stats["streak"] = 0
            debug_log(f"\u23f8\ufe0f [{symbol}] Tick-Scalp: {st['ts_pause_reason']}")
        st["ts_pnl_snap"] = _sim_stats(st)["pnl"]
        st["ts_pos_since"] = None
    elif pos_size != 0 and prev == 0:
        st["ts_pos_since"] = now
    st["ts_prev_pos"] = pos_size
    st["ts_prev_avg"] = avg_entry
    return stats


def _ts_publish(symbol, st, cfg, src, age, imb, bid, ask, pos_size, avg_entry, desired):
    tick = _tick_size(symbol)
    stats = st.get("ts_stats") or {}
    n = stats.get("trades", 0)
    held = (time.time() - float(st["ts_pos_since"])) if st.get("ts_pos_since") and pos_size else None
    cd = float(st.get("gs_cooldown_until") or 0) - time.time()
    st["ts_view"] = {
        "src": src, "book_age": None if age is None else round(age, 2), "imb": round(imb, 3),
        "bid": bid, "ask": ask, "spread_ticks": round((ask - bid) / tick, 1),
        "pos": round(pos_size, 6), "avg": avg_entry, "held_s": None if held is None else round(held, 1),
        "trades": n, "wins": stats.get("wins", 0), "pnl": stats.get("pnl", 0.0),
        "day_pnl": stats.get("day_pnl", 0.0), "streak": stats.get("streak", 0),
        "last_trade": st.get("ts_last_trade"), "pause": (round(cd) if cd > 0 else 0),
        "pause_reason": st.get("ts_pause_reason") if cd > 0 else None,
        "orders_min": _orders_last_min(), "markout": _markout_summary(st),
        "dry": bool(cfg["dry_run"]), "ws": dict(_book_info),
        "wants": [{"side": "SELL" if d["is_ask"] else "BUY", "tag": d["tag"], "price": d["price"]} for d in desired],
        "upnl": round((((bid + ask) / 2 - avg_entry) * pos_size), 4) if (pos_size and avg_entry) else 0.0,
    }


# ============================================================================
# Dry-Run-Fill-Simulation
#
# WARUM DAS OPTIMISTISCH IST - bitte lesen, bevor du den Zahlen glaubst:
# Ob eine Limit-Order fuellt, haengt an der QUEUE-POSITION. Auf deinem Preislevel
# liegen andere Orders vor dir; du kommst erst dran, wenn die abgeraeumt sind.
# Das laesst sich von aussen nicht nachbilden - die Boerse verraet nicht, wie viel
# Groesse vor dir liegt und wie viel davon storniert statt gehandelt wird.
#
# Die Regel hier ist bewusst strenger als "Preis hat das Level beruehrt": der Markt
# muss KOMPLETT durchgelaufen sein (fuer einen Kauf bei P muss der Brief-Kurs unter P
# fallen, der Markt also mindestens den Spread weit durch dich durch). Trotzdem gilt:
# die echte Fill-Rate wird NIEDRIGER sein als hier, nie hoeher. Nimm die Winrate als
# Obergrenze, nicht als Prognose. Die einzige ehrliche Messung ist eine echte Order
# im Buch - notfalls mit 200 $ Notional.
# ============================================================================

def _sim_reset(st):
    st["gs_sim_orders"] = []
    st["gs_sim_pos"] = 0.0
    st["gs_sim_avg"] = None


def _sim_stats(st):
    stats = st.get("gs_sim_stats")
    if not isinstance(stats, dict):
        stats = {"trades": 0, "gewinne": 0, "verluste": 0, "pnl": 0.0}
        st["gs_sim_stats"] = stats
    return stats


def _dash_entry(st, price, size, is_add_on):
    """Dashboard: Tabelle 'Laufende Nachkaeufe' fuettern (Dry-Run)."""
    st.setdefault("current_position_entries", []).append({
        "time": now_local().isoformat(), "price": round(price, 6), "size": round(size, 8),
        "stufe": int(st.get("entry_count") or 0) + 1, "is_add_on": is_add_on})
    st["entry_count"] = int(st.get("entry_count") or 0) + 1
    st["last_entry_price"] = price
    if not is_add_on:
        st["position_opened_at"] = now_local().isoformat()


def _dash_trade(st, side, avg, exit_px, pnl, reason):
    """Dashboard: Trade-Log + Statistik (Dry-Run)."""
    stats = st.setdefault("stats", {"trades": 0, "wins": 0, "losses": 0, "total_pnl_usd": 0.0})
    stats["trades"] += 1
    stats["total_pnl_usd"] += pnl
    stats["wins" if pnl > 0 else "losses"] += 1
    log = st.setdefault("trade_log", [])
    log.append({"side": side, "avg_entry": round(avg, 4), "exit": round(exit_px, 4),
                "entries": int(st.get("entry_count") or 0), "pnl_usd": round(pnl, 3),
                "opened_at": st.get("position_opened_at"), "closed_at": now_local().isoformat(),
                "reason": reason})
    if len(log) > 200:
        del log[:len(log) - 200]


def _dash_reset_cycle(st):
    st["entry_count"] = 0
    st["current_position_entries"] = []
    st["position_opened_at"] = None
    st["last_entry_price"] = None


def _publish_levels(st, cfg, mode, desired, pos_size, avg_entry, symbol=None):
    """Linien fuer Chart/Anzeige: TP, SL, naechster Nachkauf bzw. Einstiegs-Level."""
    lv = {"tp_price": None, "sl_price": None, "next_nachkauf_price": None,
          "next_entry_long": None, "next_entry_short": None}
    for d in desired:
        t = d["tag"]
        if t == "tp":
            lv["tp_price"] = d["price"]
        elif t in ("buy_0", "sell_0") and pos_size == 0:
            lv["next_entry_long" if t == "buy_0" else "next_entry_short"] = d["price"]
        elif not d["reduce_only"] and lv["next_nachkauf_price"] is None:
            lv["next_nachkauf_price"] = d["price"]
    if pos_size != 0 and avg_entry:
        if mode == "maker_scalp":
            dist = avg_entry * float(_ms(cfg, "ms_sl_bps")) / 1e4
        elif mode == "tick_scalp":
            dist = float(_ts(cfg, "ts_sl_ticks")) * (_tick_size(symbol) if symbol else 0.0)
        else:
            dist = abs(float(cfg.get("gs_flatten_usd", 25.0))) / max(abs(pos_size), 1e-12)
        lv["sl_price"] = round(avg_entry - dist if pos_size > 0 else avg_entry + dist, 6)
    st["gs_levels"] = lv


def _sim_process_fills(symbol, st, cfg, best_bid, best_ask):
    """Prueft alle simulierten Orders auf Fill und verbucht sie.
    Gibt (pos_size, avg_entry) nach den Fills zurueck."""
    orders = st.setdefault("gs_sim_orders", [])
    pos = float(st.get("gs_sim_pos") or 0.0)
    avg = st.get("gs_sim_avg")
    stats = _sim_stats(st)
    verbleibend = []

    for o in orders:
        # Strenge Regel: der Markt muss durch das Level DURCH sein, nicht nur dran.
        if o["is_ask"]:
            gefuellt = best_bid > o["price"]
        else:
            gefuellt = best_ask < o["price"]

        if not gefuellt:
            verbleibend.append(o)
            continue

        menge = o["size"] * (-1 if o["is_ask"] else 1)
        st.setdefault("gs_sim_pending", []).append(
            {"ts": time.time(), "is_ask": o["is_ask"], "price": o["price"], "m": {}})

        if o.get("reduce_only") or (pos != 0 and (pos > 0) != (menge > 0)):
            # Schliessender Fill -> realisierter PnL
            geschlossen = min(abs(pos), abs(menge))
            if avg is not None:
                pnl = (o["price"] - avg) * geschlossen * (1 if pos > 0 else -1)
                stats["trades"] += 1
                stats["pnl"] = round(stats["pnl"] + pnl, 4)
                if pnl >= 0:
                    stats["gewinne"] += 1
                else:
                    stats["verluste"] += 1
                _dash_trade(st, "long" if pos > 0 else "short", avg, o["price"], pnl,
                            "TP" if o.get("reduce_only") else "Gegen-Order")
                quote = round(stats["gewinne"] / stats["trades"] * 100, 1)
                debug_log(
                    f"\U0001f4b0 [{symbol}] SIM-TRADE #{stats['trades']}: "
                    f"{'LONG' if pos > 0 else 'SHORT'} zu {o['price']} geschlossen | "
                    f"PnL {round(pnl, 4)}$",
                    {"gesamt_pnl": stats["pnl"], "winrate": f"{quote}%",
                     "gewinne": stats["gewinne"], "verluste": stats["verluste"]})
            pos += menge
            if abs(pos) < 1e-12:
                pos, avg = 0.0, None
                _dash_reset_cycle(st)
        else:
            # Oeffnender/aufstockender Fill
            _dash_entry(st, o["price"], o["size"], is_add_on=not (pos == 0 or avg is None))
            if pos == 0 or avg is None:
                pos, avg = menge, o["price"]
            else:
                gesamt = pos + menge
                avg = (avg * pos + o["price"] * menge) / gesamt
                pos = gesamt
            debug_log(f"\u2705 [{symbol}] SIM-FILL {o['tag']}: "
                      f"{'SELL' if o['is_ask'] else 'BUY'} {round(o['size'], 6)} @ {o['price']} "
                      f"| Position {round(pos, 6)} @ {round(avg, 6)}")

    st["gs_sim_orders"] = verbleibend
    st["gs_sim_pos"] = pos
    st["gs_sim_avg"] = avg

    # Lokalen State spiegeln, damit das Dashboard die simulierte Position zeigt
    st["position"] = None if pos == 0 else ("long" if pos > 0 else "short")
    st["avg_entry_price"] = avg
    st["total_coin_size"] = abs(pos)
    return pos, avg


def _sim_open_orders(st):
    return [dict(o) for o in st.get("gs_sim_orders", [])]


def _sim_reconcile(symbol, st, cfg, desired):
    """Dry-Run: SOLL-Orders in die simulierte Buch-Liste uebernehmen (Requote nur bei Drift)."""
    drift_limit = int(cfg.get("gs_requote_ticks", 2)) * _tick_size(symbol)
    by_tag = {o["tag"]: o for o in st.get("gs_sim_orders", [])}
    new = []
    for d in desired:
        old = by_tag.get(d["tag"])
        if old is not None and abs(old["price"] - d["price"]) <= drift_limit and old["reduce_only"] == d["reduce_only"]:
            old["size"] = d["size"] if d["reduce_only"] else old["size"]
            new.append(old)
        else:
            new.append(dict(d))
    st["gs_sim_orders"] = new


def _sim_markouts(st, mid):
    """Mid-Preis 5/30/60s nach jedem simulierten Fill -> Adverse-Selection-Messung (bps)."""
    now = time.time()
    marks = st.setdefault("ms_marks", [])
    keep = []
    for p in st.get("gs_sim_pending", []):
        for sec in (5, 30, 60):
            if sec not in p["m"] and now - p["ts"] >= sec:
                p["m"][sec] = mid
        if len(p["m"]) == 3:
            sign = -1 if p["is_ask"] else 1
            marks.append([sign * (p["m"][x] - p["price"]) / p["price"] * 1e4 for x in (5, 30, 60)])
        else:
            keep.append(p)
    st["gs_sim_pending"] = keep
    if len(marks) > 2000:
        del marks[:len(marks) - 2000]


def _markout_summary(st):
    marks = st.get("ms_marks") or []
    if not marks:
        return "noch keine (n=0)"
    n = len(marks)
    a = [round(sum(m[i] for m in marks) / n, 2) for i in range(3)]
    return f"{a[0]:+}/{a[1]:+}/{a[2]:+} bps (5/30/60s, n={n})"


def _sim_close_market(st, bid, ask, reason):
    """Dry-Run-Notausstieg: simulierte Position als Taker schliessen."""
    pos = float(st.get("gs_sim_pos") or 0.0)
    avg = st.get("gs_sim_avg")
    if pos == 0 or avg is None:
        _sim_reset(st)
        return
    px = bid if pos > 0 else ask
    pnl = (px - avg) * abs(pos) * (1 if pos > 0 else -1)
    stats = _sim_stats(st)
    stats["trades"] += 1
    stats["pnl"] = round(stats["pnl"] + pnl, 4)
    stats["gewinne" if pnl >= 0 else "verluste"] += 1
    debug_log(f"\U0001f9ea SIM-FLATTEN ({reason}): zu {px} | PnL {round(pnl, 4)}$",
              {"gesamt_pnl": stats["pnl"], "gewinne": stats["gewinne"], "verluste": stats["verluste"]})
    _dash_trade(st, "long" if pos > 0 else "short", avg, px, pnl, reason)
    _dash_reset_cycle(st)
    _sim_reset(st)
    st["position"] = None
    st["avg_entry_price"] = None
    st["total_coin_size"] = 0.0


# ============================================================================
# Zustand von der Boerse lesen und in BOTS[...]["state"] spiegeln
# ============================================================================

async def _sync_position(client, symbol, market_index):
    """Gibt (signed_size, avg_entry) zurueck und spiegelt es in den lokalen State,
    damit Dashboard, PnL-Anzeige und Liq-Schaetzung weiter stimmen."""
    st = BOTS[symbol]["state"]
    pos = await get_account_position_from_exchange(client, market_index, retries=2, delay=0.4)
    if pos is None:
        return 0.0, None

    try:
        size = abs(float(pos.position))
    except (TypeError, ValueError):
        return 0.0, None

    if size == 0:
        if st["position"] is not None:
            debug_log(f"✅ [{symbol}] Grid-Scalp: Position geschlossen (Boerse meldet flat)")
        st["position"] = None
        st["avg_entry_price"] = None
        st["total_coin_size"] = 0.0
        st["entry_count"] = 0
        return 0.0, None

    try:
        sign = int(getattr(pos, "sign", 1) or 1)
    except (TypeError, ValueError):
        sign = 1
    if sign == 0:
        sign = 1
    try:
        avg_entry = float(pos.avg_entry_price)
    except (TypeError, ValueError):
        avg_entry = st.get("avg_entry_price")

    direction = "long" if sign > 0 else "short"
    if st["position"] != direction:
        st["position_opened_at"] = now_local().isoformat()
    st["position"] = direction
    st["avg_entry_price"] = avg_entry
    st["total_coin_size"] = size
    return size * sign, avg_entry


async def _flatten_market(client, symbol, market_index, pos_size, reason, cooldown_s=None):
    """Notausstieg. EINZIGE Taker-Order im ganzen Modul - hier ist Fill wichtiger
    als Spread."""
    from bot_core import place_market_order

    st, cfg = BOTS[symbol]["state"], BOTS[symbol]["config"]

    # Offene Orders zuerst weg, sonst kollidiert das Flatten mit den eigenen Quotes
    try:
        if not cfg["dry_run"]:
            for o in await read_open_orders(client, market_index):
                await cancel_order(client, market_index, o["coi"])
    except Exception as e:
        debug_log(f"⚠️ [{symbol}] Grid-Scalp: Orders vor Flatten nicht abraeumbar", {"error": str(e)})

    is_ask = pos_size > 0
    base_amount = int(abs(pos_size) * get_precision(symbol))
    price = st.get("last_price") or 0.0

    if cfg["dry_run"]:
        debug_log(f"🧪 [{symbol}] DRY FLATTEN ({reason}): {'SELL' if is_ask else 'BUY'} {abs(pos_size)}")
        _sim_close_market(st, st.get("last_bid") or price, st.get("last_ask") or price, reason)
        st["gs_sim_orders"] = []
    else:
        tx, tx_hash, err = await place_market_order(
            client, market_index, symbol, is_ask, base_amount, price, reduce_only=True)
        if err:
            debug_log(f"⚠️ [{symbol}] Grid-Scalp FLATTEN FEHLGESCHLAGEN", {"error": str(err)})
            return
        debug_log(f"🚪 [{symbol}] Grid-Scalp FLATTEN ausgefuehrt ({reason})", {"tx_hash": str(tx_hash)})

    st["gs_tag_map"] = {}
    st["gs_levels"] = None
    st["gs_cooldown_until"] = time.time() + (float(cooldown_s) if cooldown_s is not None
                                             else float(cfg.get("gs_cooldown_min", 30.0)) * 60)
    st["gs_anchor"] = None
    await save_bot_state()


# ============================================================================
# Haupt-Tick
# ============================================================================

async def grid_scalp_tick(client, symbol):
    cfg, st = BOTS[symbol]["config"], BOTS[symbol]["state"]
    market_index = MARKET_INDICES[symbol]
    if cfg.get("entry_mode") == "grid_classic":
        return await grid_classic_tick(client, symbol)

    mode = cfg.get("entry_mode")
    t_imb, t_src, t_age = 0.0, "rest", None
    if mode == "tick_scalp":
        best_bid, best_ask, t_imb, t_src, t_age = await get_top(client, market_index, float(_ts(cfg, "ts_book_max_age_s")))
    else:
        best_bid, best_ask = await read_best_bid_ask(client, market_index)
    if best_bid is None:
        return
    mid = (best_bid + best_ask) / 2.0
    st["last_price"] = mid
    st["last_bid"], st["last_ask"] = best_bid, best_ask

    if cfg["dry_run"]:
        # Dry-Run: simulierte Position/Orders statt Boersen-Zustand (vorher lief die Simulation nie)
        pos_size, avg_entry = _sim_process_fills(symbol, st, cfg, best_bid, best_ask)
        _sim_markouts(st, mid)
    else:
        pos_size, avg_entry = await _sync_position(client, symbol, market_index)

    # 1) Notausstieg ZUERST - vor allem, was neue Orders posten koennte
    if mode == "tick_scalp":
        _ts_track(symbol, st, cfg, pos_size, avg_entry, mid)
        _ts_publish(symbol, st, cfg, t_src, t_age, t_imb, best_bid, best_ask, pos_size, avg_entry, [])
        if pos_size != 0 and avg_entry:
            tk = _tick_size(symbol)
            px = best_bid if pos_size > 0 else best_ask
            adverse = ((avg_entry - px) if pos_size > 0 else (px - avg_entry)) / tk
            held = time.time() - float(st.get("ts_pos_since") or time.time())
            sl_t, max_h = float(_ts(cfg, "ts_sl_ticks")), float(_ts(cfg, "ts_max_hold_s"))
            why = None
            if sl_t > 0 and adverse >= sl_t:
                why = f"tick-stopp ({round(adverse, 1)} Ticks)"
            elif max_h > 0 and held >= max_h:
                why = f"zeitlimit ({round(held)}s)"
            if why:
                debug_log(f"\U0001f6d1 [{symbol}] Tick-Scalp Ausstieg: {why}")
                await _flatten_market(client, symbol, market_index, pos_size, why, cooldown_s=0)
                return
    elif mode == "maker_scalp" and pos_size != 0 and avg_entry:
        px = best_bid if pos_size > 0 else best_ask
        pnl_bps = (px - avg_entry) / avg_entry * 1e4 * (1 if pos_size > 0 else -1)
        if pnl_bps <= -float(_ms(cfg, "ms_sl_bps")):
            today = int(time.time() // 86400)
            if st.get("ms_day") != today:
                st["ms_day"], st["ms_sl_today"] = today, 0
            st["ms_sl_today"] = int(st.get("ms_sl_today") or 0) + 1
            debug_log(f"🛑 [{symbol}] Maker-Scalp Stopp: {round(pnl_bps,1)} bps <= -{_ms(cfg,'ms_sl_bps')} bps "
                      f"(Stopp #{st['ms_sl_today']} heute)")
            if st["ms_sl_today"] >= int(_ms(cfg, "ms_max_sl_per_day")):
                pause = (today + 1) * 86400 - time.time()
                debug_log(f"⛔ [{symbol}] Maker-Scalp: Tageslimit an Stopps erreicht - Pause bis Tageswechsel (UTC)")
            else:
                pause = float(_ms(cfg, "ms_sl_pause_s"))
            await _flatten_market(client, symbol, market_index, pos_size, "maker-stopp", cooldown_s=pause)
            return
    elif mode not in ("maker_scalp", "tick_scalp") and pos_size != 0 and avg_entry:
        upnl = (mid - avg_entry) * pos_size
        limit = -abs(float(cfg.get("gs_flatten_usd", 25.0)))
        if upnl <= limit:
            debug_log(f"🛑 [{symbol}] Grid-Scalp Notausstieg: uPnL {round(upnl,2)}$ <= {limit}$")
            await _flatten_market(client, symbol, market_index, pos_size, "notausstieg")
            return

    # 2) Cooldown - verhindert, dass der Bot im Trend sofort dieselbe Position
    #    wieder aufbaut und aus einem -25$-Tag einen -200$-Tag macht
    if time.time() < float(st.get("gs_cooldown_until") or 0.0):
        if pos_size == 0:
            rest_min = (float(st["gs_cooldown_until"]) - time.time()) / 60
            if time.time() - float(st.get("gs_pause_log") or 0) >= 30:
                st["gs_pause_log"] = time.time()
                debug_log(f"\u23f8\ufe0f [{symbol}] Grid-Scalp pausiert (Cooldown), noch {round(rest_min,1)} Min.")
            if cfg["dry_run"]:
                st["gs_sim_orders"] = []
            else:
                for o in await read_open_orders(client, market_index):
                    await cancel_order(client, market_index, o["coi"])
            st.setdefault("gs_tag_map", {}).clear()
            return

    if not cfg.get("bot_active", True):
        debug_log(f"\u26d4 [{symbol}] Grid-Scalp: bot_active=False - raeume Orders ab und warte")
        if cfg["dry_run"]:
            st["gs_sim_orders"] = []
        else:
            for o in await read_open_orders(client, market_index):
                await cancel_order(client, market_index, o["coi"])
        st.setdefault("gs_tag_map", {}).clear()
        return

    # 3) Anker setzen / nachfuehren - NUR wenn flat. Mit offener Position wuerdest
    #    du das Raster unter den laufenden Nachkauf-Stufen wegziehen.
    if st.get("gs_anchor") is None:
        st["gs_anchor"] = mid
    elif pos_size == 0:
        drift = abs(mid - st["gs_anchor"]) / st["gs_anchor"]
        if drift > float(cfg.get("gs_anchor_follow_pct", 1.0)) / 100.0:
            debug_log(f"⚓ [{symbol}] Grid-Scalp Anker: {round(st['gs_anchor'],4)} -> {round(mid,4)} "
                      f"(Drift {round(drift*100,2)}%)")
            st["gs_anchor"] = mid

    # 4) SOLL gegen IST abgleichen
    if mode == "maker_scalp":
        _feed_internal(symbol, mid)
        await _seed_internal(symbol, cfg)
        ctx = await _maker_context(symbol, cfg)
        entries_ok = ctx is not None
        trend = ctx["trend"] if ctx else "flat"
        if ctx and ctx["vol_spike"]:
            if time.time() >= float(st.get("ms_pause_until") or 0):
                debug_log(f"🌊 [{symbol}] Maker-Scalp: Volatilitaets-Rueckzug {_ms(cfg,'ms_vol_pause_s')}s")
            st["ms_pause_until"] = time.time() + float(_ms(cfg, "ms_vol_pause_s"))
        if time.time() < float(st.get("ms_pause_until") or 0):
            entries_ok = False
        st["ms_trend"] = trend
        if ctx is None:
            d_ = _int_candles.get(symbol)
            st["ms_ctx"] = (f"Aufwaermen: eigene 1m-Kerzen {len(d_['closed']) if d_ else 0}/{int(_ms(cfg,'ms_ema_slow'))}, "
                            f"Binance liefert nichts -> keine Entries")
            if time.time() - float(st.get("ms_ctx_warn") or 0) >= 60:
                st["ms_ctx_warn"] = time.time()
                debug_log(f"⚠️ [{symbol}] Maker-Scalp: Binance liefert keine 1m-Kerzen - nutze eigene Lighter-Kerzen, warte auf Aufwaermzeit")
        elif time.time() < float(st.get("ms_pause_until") or 0):
            st["ms_ctx"] = f"Vol-Pause noch {round(float(st['ms_pause_until']) - time.time())}s"
        else:
            st["ms_ctx"] = f"ok [{ctx.get('quelle')}] (EMA-Abstand {ctx.get('ema_bps')} bps)"
        desired = _desired_maker(symbol, cfg, pos_size, avg_entry, best_bid, best_ask, trend, entries_ok)
    elif mode == "tick_scalp":
        now_ = time.time()
        # unbefuellte Einstiegsorder nach ts_entry_ttl_s neu setzen (eine Sekunde Luecke -> Cancel + frischer Preis)
        if pos_size == 0:
            if st.get("ts_entry_since") is None:
                st["ts_entry_since"] = now_
            ttl = float(_ts(cfg, "ts_entry_ttl_s"))
            if ttl > 0 and now_ - st["ts_entry_since"] >= ttl:
                st["ts_entry_since"] = None
                st["ts_skip_until"] = now_ + 1.0
        else:
            st["ts_entry_since"] = None
        if now_ < float(st.get("ts_skip_until") or 0):
            desired = []
        else:
            desired = _desired_tick(symbol, cfg, pos_size, avg_entry, best_bid, best_ask, t_imb, True)
        _ts_publish(symbol, st, cfg, t_src, t_age, t_imb, best_bid, best_ask, pos_size, avg_entry, desired)
    else:
        desired = _desired_orders(symbol, cfg, st, pos_size, avg_entry, best_bid, best_ask)

    _publish_levels(st, cfg, mode, desired, pos_size, avg_entry, symbol)

    if cfg["dry_run"]:
        _sim_reconcile(symbol, st, cfg, desired)
        now = time.time()
        if now - float(st.get("gs_last_heartbeat") or 0) >= 30:
            st["gs_last_heartbeat"] = now
            stt = _sim_stats(st)
            wr = round(stt["gewinne"] / stt["trades"] * 100, 1) if stt["trades"] else 0
            debug_log(f"\U0001f493 [{symbol}] {mode} SIM Heartbeat", {
                "mid": round(mid, 6), "bid/ask": f"{best_bid}/{best_ask}",
                "trend": st.get("ms_trend"), "kontext": st.get("ms_ctx"), "position": f"{round(pos_size,6)} @ {avg_entry}" if pos_size else "flat",
                "sim_orders": len(st["gs_sim_orders"]), "sim_trades": stt["trades"], "sim_winrate": f"{wr}%",
                "sim_pnl_usd": stt["pnl"],
                "sim_upnl_usd": round((mid - avg_entry) * pos_size, 3) if (pos_size and avg_entry) else 0.0,
                "markout": _markout_summary(st)})
        return

    if mode == "tick_scalp" and _orders_last_min() >= int(_ts(cfg, "ts_max_orders_min")):
        if time.time() - float(st.get("ts_budget_log") or 0) >= 30:
            st["ts_budget_log"] = time.time()
            debug_log(f"\U0001f6a6 [{symbol}] Tick-Scalp: Order-Budget ({_ts(cfg, 'ts_max_orders_min')}/Min) erreicht - warte")
        return

    live = await read_open_orders(client, market_index)

    tag_map = st.setdefault("gs_tag_map", {})
    coi_to_tag = {int(v): k for k, v in tag_map.items()}
    live_by_tag = {}
    for o in live:
        tag = coi_to_tag.get(o["coi"])
        if tag:
            live_by_tag[tag] = o

    desired_tags = {d["tag"] for d in desired}
    tick = _tick_size(symbol)
    drift_limit = int(cfg.get("gs_requote_ticks", 2)) * tick
    precision = get_precision(symbol)
    min_base = get_min_base_amount(symbol)

    # 4a) verwaiste Orders killen (nicht mehr im SOLL, oder unbekannter Ursprung)
    for o in live:
        tag = coi_to_tag.get(o["coi"])
        if tag is None or tag not in desired_tags:
            if not cfg["dry_run"]:
                await cancel_order(client, market_index, o["coi"])
            if tag:
                tag_map.pop(tag, None)

    # 4b) fehlende posten, verdriftete neu setzen
    for d in desired:
        existing = live_by_tag.get(d["tag"])
        if existing is not None and abs(existing["price"] - d["price"]) <= drift_limit:
            continue
        if existing is not None:
            if not cfg["dry_run"]:
                await cancel_order(client, market_index, existing["coi"])
            tag_map.pop(d["tag"], None)

        if d["size"] < min_base:
            continue
        base_amount = int(d["size"] * precision)
        coi = int(time.time() * 1000) % (2 ** 40) + len(tag_map)

        if cfg["dry_run"]:
            debug_log(f"🧪 [{symbol}] DRY POST {d['tag']}: "
                      f"{'SELL' if d['is_ask'] else 'BUY'} {round(d['size'],6)} @ {d['price']}")
            tag_map[d["tag"]] = coi
            continue

        tx, tx_hash, err = await place_post_only_order(
            client, market_index, symbol, d["is_ask"], base_amount,
            d["price"], coi, reduce_only=d["reduce_only"])
        if err:
            # Post-Only, das gekreuzt haette, wird verworfen - erwartetes Verhalten.
            # Haeuft es sich aber, stimmt was anderes nicht (Groesse, Margin, Auth),
            # deshalb alle 60s einmal ins Log statt komplett stumm.
            st["gs_last_error"] = str(err)
            if time.time() - float(st.get("gs_last_error_log") or 0) >= 60:
                st["gs_last_error_log"] = time.time()
                debug_log(f"\u26a0\ufe0f [{symbol}] Grid-Scalp: Order abgelehnt ({d['tag']})",
                          {"error": str(err), "preis": d["price"], "groesse": round(d["size"], 6)})
            continue
        tag_map[d["tag"]] = coi

    st["gs_open_orders"] = len(desired)

    # Heartbeat - gedrosselt auf alle 30s, damit das Log nicht zulaeuft. Ohne den
    # sieht "Loop laeuft, postet aber nichts" genauso aus wie "Loop laeuft gar nicht".
    now = time.time()
    if now - float(st.get("gs_last_heartbeat") or 0) >= 30:
        st["gs_last_heartbeat"] = now
        pos_txt = f"{round(pos_size,6)} @ {avg_entry}" if pos_size else "flat"
        debug_log(f"\U0001f493 [{symbol}] Grid-Scalp Heartbeat", {
            "mid": round(mid, 6),
            "bid/ask": f"{best_bid}/{best_ask}",
            "spread_pct": round((best_ask - best_bid) / mid * 100, 5),
            "position": pos_txt,
            "soll_orders": len(desired),
            "ist_orders": len(live),
            "dry_run": cfg["dry_run"],
            "letzter_fehler": st.get("gs_last_error"),
        })


async def grid_scalp_poll_loop(symbol):
    """In main.py's asyncio.gather einhaengen."""
    client = None
    last_idle_log = 0.0
    debug_log(f"\U0001f680 [{symbol}] Grid-Scalp Loop gestartet "
              f"(aktueller entry_mode: {BOTS[symbol]['config'].get('entry_mode')})")
    while True:
        cfg = BOTS[symbol]["config"]
        if cfg.get("entry_mode") not in MODES:
            if client is not None:
                try:
                    await asyncio.wait_for(client.close(), timeout=EXCHANGE_CALL_TIMEOUT_SECONDS)
                except Exception:
                    pass
                client = None
            # Alle 5 Minuten melden, WARUM nichts passiert. Sonst ist "Strategie nicht
            # aktiv" im Log nicht von "Loop laeuft gar nicht" zu unterscheiden.
            if time.time() - last_idle_log >= 300:
                last_idle_log = time.time()
                debug_log(f"\U0001f4a4 [{symbol}] Grid-Scalp inaktiv "
                          f"(entry_mode ist '{cfg.get('entry_mode')}', erwartet 'grid_scalp' oder 'maker_scalp')")
            await asyncio.sleep(5)
            continue

        try:
            if client is None:
                # Persistenter Client - get_lighter_client() baut sonst pro Order einen
                # neuen auf, was bei einem Maker-Loop mit ~30 Orders/Minute nicht geht
                client = get_lighter_client("grid")
                if client is None:
                    await asyncio.sleep(10)
                    continue
                BOTS[symbol]["state"]["gs_account"] = {"index": client.account_index, "sub": bool(os.getenv("GRID_ACCOUNT_INDEX") and os.getenv("GRID_PRIVATE_KEY"))}
                debug_log(f"🔌 [{symbol}] Grid-Bot: Client verbunden (Konto {client.account_index}{' = Unterkonto' if os.getenv('GRID_ACCOUNT_INDEX') else ''})")
            await grid_scalp_tick(client, symbol)
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Grid-Scalp Tick-Fehler",
                      {"error": str(e), "traceback": traceback.format_exc()})
            try:
                await asyncio.wait_for(client.close(), timeout=EXCHANGE_CALL_TIMEOUT_SECONDS)
            except Exception:
                pass
            client = None
            # Lighter-WAF ("Human Verification", HTTP 405) = zu viele Anfragen von dieser IP: laenger pausieren,
            # sonst bleibt die Sperre bestehen. Zufaellige Streuung, damit nicht alle Coins gleichzeitig wiederkommen.
            _err = str(e)
            if "405" in _err or "Human Verification" in _err or "awswaf" in _err:
                import random
                _w = 45 + random.random() * 30
                debug_log(f"🚦 [{symbol}] Lighter-Anfragelimit (WAF) - pausiere {_w:.0f}s. Poll-Intervall im Grid-Bot auf 5+ Sekunden stellen!")
                await asyncio.sleep(_w)
            else:
                await asyncio.sleep(5)

        _c = BOTS[symbol]["config"]
        if _c.get("entry_mode") == "tick_scalp":
            _p = float(_ts(_c, "ts_poll_seconds"))
            # live braucht pro Tick REST fuer Position + offene Orders -> Untergrenze gegen die Lighter-WAF-Sperre
            await asyncio.sleep(max(0.25, _p) if _c.get("dry_run") else max(4.0, _p))
        else:
            await asyncio.sleep(float(_c.get("gc_poll_seconds", 2.0) if _c.get("entry_mode") == "grid_classic" else _c.get("gs_poll_seconds", 2.0)))


# ============================================================================
# KLASSISCHER GRID-BOT (entry_mode "grid_classic")
# ----------------------------------------------------------------------------
# Festes Preisgitter zwischen gc_lower und gc_upper mit gc_levels Abschnitten.
# LONG-Gitter: auf jedem Level UNTER dem Kurs liegt eine Kauf-Limit-Order. Fuellt sie, entsteht ein "Lot" und eine
# Verkaufs-Order (reduce-only) EIN Level hoeher wird gesetzt. Fuellt die, ist der Stufengewinn realisiert und die
# Kauf-Order kommt zurueck. SHORT-Gitter ist das Spiegelbild. Es wird nie mit Verlust verkauft - faellt der Kurs,
# bleiben die Lots offen und warten (Notbremse optional: gc_stop_pct).
# Lighter hat pro Markt nur EINE Nettoposition - die Lots sind darum ein virtuelles Buch (State), die Boerse kennt nur
# die Summe. Gewinn je Stufe = Lot-Groesse x Level-Abstand.
#
# WEITERLAUFEN NACH DEPLOY: Loop braucht nur bot_active (gespeichert), NICHT session_started. Lots/Range/offene Orders
# (gc_prev) liegen in Redis. Eigene Orders erkennt der Bot an der client_order_index (kodiert Markt + Level + Art),
# also auch ohne tag_map. Fills waehrend des Neustarts werden aus verschwundenen Orders + Positionsabgleich erkannt.
# ============================================================================

GC_DEFAULTS = {
    "gc_direction": "long",        # long / short
    "gc_lower": 0.0,               # 0 = automatisch beim Start (Kurs -/+ gc_auto_pct)
    "gc_upper": 0.0,
    "gc_auto_pct": 5.0,
    "gc_levels": 20,               # Abschnitte (= Anzahl Stufen)
    "gc_spacing": "arith",         # arith (gleiche Abstaende) / geo (gleiche Prozent)
    "gc_size_usd": 200.0,          # Notional je Stufe (Lot)
    "gc_max_lots": 10,             # maximal gleichzeitig gehaltene Lots (Inventar-Limit)
    "gc_open_each_side": 6,        # so viele Einstiegs-Orders liegen gleichzeitig im Buch
    "gc_stop_pct": 0.0,            # Notbremse: % ausserhalb der Spanne -> alles schliessen + stoppen (0 = aus)
    "gc_poll_seconds": 2.0,
}
GC_COI_BASE = 10 ** 11


def _gcv(cfg, key):
    v = cfg.get(key)
    return GC_DEFAULTS[key] if v is None else v


def _gc_coi(market_index, idx, is_exit):
    # Ziffern: Zeit (zyklisch, damit eine frische Order nie dieselbe ID wie eine gerade gefuellte hat) | Markt | Level*2+Art
    return GC_COI_BASE + (int(time.time()) % 10000) * 10 ** 7 + int(market_index) * 1000 + int(idx) * 2 + (1 if is_exit else 0)


def _gc_decode(coi, market_index):
    if coi < GC_COI_BASE or coi >= GC_COI_BASE + 10 ** 11:
        return None
    low = coi % 10 ** 7
    if low // 1000 != int(market_index):
        return None
    r = low % 1000
    return ("x" if r & 1 else "e") + str(r >> 1)


def _gc_levels(rng):
    lo, hi, n = float(rng["lower"]), float(rng["upper"]), int(rng["n"])
    if rng.get("spacing") == "geo" and lo > 0:
        ratio = (hi / lo) ** (1.0 / n)
        return [lo * ratio ** i for i in range(n + 1)]
    return [lo + (hi - lo) * i / n for i in range(n + 1)]


def _gc_sig(cfg):
    return [str(_gcv(cfg, "gc_direction")), float(_gcv(cfg, "gc_lower")), float(_gcv(cfg, "gc_upper")),
            int(_gcv(cfg, "gc_levels")), str(_gcv(cfg, "gc_spacing"))]


def _gc_nearest(levels, px):
    return min(range(len(levels)), key=lambda i: abs(levels[i] - px))


def _gc_valid_entry(d, k, n):
    return (0 <= k <= n - 1) if d > 0 else (1 <= k <= n)


def _gc_desired(symbol, cfg, st, rng, levels, best_bid, best_ask):
    d = 1 if rng["dir"] == "long" else -1
    n = len(levels) - 1
    tick = _tick_size(symbol)
    dec = get_price_decimals(symbol)
    precision = get_precision(symbol)
    lots = st.get("gc_lots") or []
    out = []

    def rnd(p):
        return round(round(p / tick) * tick, dec)

    def qty(usd, px):
        return int((usd / px) * precision) / precision

    groups = {}
    for lot in lots:
        t = min(n, max(0, int(lot["k"]) + d))
        groups.setdefault(t, 0.0)
        groups[t] += float(lot["size"])
    for t, size in sorted(groups.items()):
        px = max(levels[t], best_ask) if d > 0 else min(levels[t], best_bid)
        out.append({"tag": f"x{t}", "is_ask": d > 0, "price": rnd(px), "size": size, "reduce_only": True, "idx": t, "exit": True})

    held = {int(l["k"]) for l in lots}
    free = max(0, int(_gcv(cfg, "gc_max_lots")) - len(lots))
    each = min(int(_gcv(cfg, "gc_open_each_side")), free)
    cands = []
    for k in range(n + 1):
        if not _gc_valid_entry(d, k, n) or k in held:
            continue
        if d > 0 and levels[k] <= best_bid - tick:
            cands.append(k)
        elif d < 0 and levels[k] >= best_ask + tick:
            cands.append(k)
    cands.sort(key=lambda k: -levels[k] if d > 0 else levels[k])
    usd = float(_gcv(cfg, "gc_size_usd"))
    for k in cands[:each]:
        out.append({"tag": f"e{k}", "is_ask": d < 0, "price": rnd(levels[k]), "size": qty(usd, levels[k]),
                    "reduce_only": False, "idx": k, "exit": False})
    return out


def _gc_apply_fills(st, rng, levels, filled, prev):
    """filled: Tags, die verschwunden sind (= gefuellt). prev: Orders des letzten Ticks (tag -> price/size0)."""
    d = 1 if rng["dir"] == "long" else -1
    lots = st.setdefault("gc_lots", [])
    n = len(levels) - 1
    for tag in sorted(filled):
        info = prev.get(tag) or {}
        idx = int(tag[1:])
        if tag[0] == "e":
            size = float(info.get("size0") or info.get("size") or 0)
            if size <= 0 or any(int(l["k"]) == idx for l in lots):
                continue
            lots.append({"k": idx, "px": levels[idx], "size": size, "t": now_local().isoformat()})
            _dash_entry(st, levels[idx], size, len(lots) > 1)
            debug_log(f"🟢 Grid: Lot eröffnet @ {levels[idx]:.6g} (Level {idx}, {size}), Lots: {len(lots)}")
        else:
            hit = [l for l in lots if min(n, max(0, int(l["k"]) + d)) == idx]
            if not hit:
                continue
            exit_px = float(info.get("price") or levels[idx])
            for l in hit:
                pnl = (exit_px - float(l["px"])) * float(l["size"]) * d
                st["gc_realized"] = float(st.get("gc_realized") or 0.0) + pnl
                st["gc_cycles"] = int(st.get("gc_cycles") or 0) + 1
                _dash_trade(st, "long" if d > 0 else "short", float(l["px"]), exit_px, pnl, "GRID-STUFE")
                lots.remove(l)
                ent = st.get("current_position_entries") or []
                for e in ent:
                    if abs(float(e.get("price", 0)) - float(l["px"])) < 1e-9:
                        ent.remove(e)
                        break
                debug_log(f"💰 Grid: Stufe geschlossen {l['px']:.6g} → {exit_px:.6g}: {pnl:+.3f} $")
    st["entry_count"] = len(lots)
    if lots:
        st["last_entry_price"] = lots[-1]["px"]


def _gc_rebuild_lots(st, rng, levels, pos_signed, mid, lot_size_coin):
    """Notfall-Abgleich: Boersen-Position und Lot-Buch passen nicht zusammen (verpasste Fills, manueller Trade)."""
    d = 1 if rng["dir"] == "long" else -1
    n = len(levels) - 1
    held = abs(pos_signed) if pos_signed * d > 0 else 0.0
    cnt = int(round(held / lot_size_coin)) if lot_size_coin > 0 else 0
    cand = [k for k in range(n + 1) if _gc_valid_entry(d, k, n) and ((levels[k] > mid) if d > 0 else (levels[k] < mid))]
    cand.sort(key=lambda k: levels[k] if d > 0 else -levels[k])
    lots = []
    for k in cand[:cnt]:
        lots.append({"k": k, "px": levels[k], "size": lot_size_coin, "t": now_local().isoformat()})
    st["gc_lots"] = lots
    st["entry_count"] = len(lots)
    st["current_position_entries"] = [{"time": l["t"], "price": round(l["px"], 6), "size": round(l["size"], 8),
                                       "stufe": i + 1, "is_add_on": i > 0} for i, l in enumerate(lots)]
    return len(lots)


def gc_reset_state(st, keep_stats=True):
    for k in ("gc_lots", "gc_prev", "gc_sim", "gc_range", "gc_sig", "gc_stopped"):
        st.pop(k, None)
    st["gc_lots"] = []
    st["entry_count"] = 0
    st["current_position_entries"] = []
    if not keep_stats:
        st["gc_realized"] = 0.0
        st["gc_cycles"] = 0


async def _gc_cancel_own(client, market_index, cfg):
    n = 0
    if cfg["dry_run"]:
        return 0
    for o in await read_open_orders(client, market_index):
        if _gc_decode(o["coi"], market_index):
            try:
                await cancel_order(client, market_index, o["coi"])
                n += 1
            except Exception as e:
                debug_log(f"⚠️ Grid: Cancel fehlgeschlagen", {"error": str(e)})
    return n


async def grid_classic_tick(client, symbol):
    from bot_core import save_bot_configs
    cfg, st = BOTS[symbol]["config"], BOTS[symbol]["state"]
    market_index = MARKET_INDICES[symbol]
    best_bid, best_ask = await read_best_bid_ask(client, market_index)
    if best_bid is None:
        return
    mid = (best_bid + best_ask) / 2.0
    st["last_price"], st["last_bid"], st["last_ask"] = mid, best_bid, best_ask
    st["session_started"] = bool(cfg.get("bot_active"))   # nach Deploy KEIN manueller Start noetig
    dry = bool(cfg.get("dry_run"))

    if not cfg.get("bot_active"):
        # gestoppt: eigene Orders abraeumen, Lots/Buch bleiben erhalten (Position liegt ja weiter auf der Boerse)
        if dry:
            st["gc_sim"] = {}
        elif st.get("gc_prev") or time.time() - float(st.get("gc_idle_clean") or 0) > 60:
            st["gc_idle_clean"] = time.time()
            await _gc_cancel_own(client, market_index, cfg)
        st["gc_prev"] = {}
        st["gc_view"] = None
        return

    # ---- Range / Gitter bestimmen ---------------------------------------
    sig = _gc_sig(cfg)
    rng = st.get("gc_range")
    lots = st.setdefault("gc_lots", [])
    cfg_dir = "short" if _gcv(cfg, "gc_direction") == "short" else "long"
    if rng is None:
        lo, hi = float(_gcv(cfg, "gc_lower")), float(_gcv(cfg, "gc_upper"))
        if lo <= 0 or hi <= lo:
            p = float(_gcv(cfg, "gc_auto_pct")) / 100.0
            lo, hi = mid * (1 - p), mid * (1 + p)
        rng = {"lower": lo, "upper": hi, "n": max(2, min(200, int(_gcv(cfg, "gc_levels")))),
               "spacing": _gcv(cfg, "gc_spacing"), "dir": cfg_dir}
        st["gc_range"], st["gc_sig"] = rng, sig
        st["gc_stopped"] = None
        debug_log(f"🧱 [{symbol}] Grid-Bot: Gitter {rng['dir']} {lo:.6g} … {hi:.6g}, {rng['n']} Stufen ({rng['spacing']})")
    elif sig != st.get("gc_sig") or (cfg_dir != rng.get("dir") and not lots):
        # 2. Bedingung: Richtung im Formular != Richtung des laufenden Gitters, aber keine Lots offen -> umstellen.
        # (Frueher wurde gc_sig auch dann aktualisiert, wenn der Wechsel wegen offener Lots verweigert wurde - danach
        # fiel der Unterschied nie mehr auf und der Bot blieb dauerhaft in der alten Richtung.)
        lo, hi = float(_gcv(cfg, "gc_lower")), float(_gcv(cfg, "gc_upper"))
        if lo <= 0 or hi <= lo:
            lo, hi = rng["lower"], rng["upper"]       # Auto-Spanne bleibt, wie sie beim Start festgelegt wurde
        new_dir = cfg_dir
        if lots and new_dir != rng["dir"]:
            if time.time() - float(st.get("gc_dir_warn") or 0) > 300:
                st["gc_dir_warn"] = time.time()
                debug_log(f"⚠️ [{symbol}] Grid-Bot: Richtung kann erst gewechselt werden, wenn keine Lots offen sind - bleibt {rng['dir']}")
            new_dir = rng["dir"]
        old_levels = _gc_levels(rng)
        rng = {"lower": lo, "upper": hi, "n": max(2, min(200, int(_gcv(cfg, "gc_levels")))),
               "spacing": _gcv(cfg, "gc_spacing"), "dir": new_dir}
        new_levels = _gc_levels(rng)
        for l in lots:
            l["k"] = _gc_nearest(new_levels, float(l["px"]))
            l["px"] = new_levels[l["k"]]
        st["gc_range"], st["gc_sig"] = rng, sig
        debug_log(f"🧱 [{symbol}] Grid-Bot: Gitter geändert → {lo:.6g} … {hi:.6g}, {rng['n']} Stufen ({len(lots)} Lots neu zugeordnet)")
    if lots and cfg_dir != rng["dir"] and time.time() - float(st.get("gc_dir_warn") or 0) > 300:
        st["gc_dir_warn"] = time.time()
        debug_log(f"⚠️ [{symbol}] Grid-Bot: Formular steht auf {cfg_dir}, das Gitter läuft aber noch {rng['dir']} ({len(lots)} Lots offen). "
                  f"Zum Wechsel: Stop, Position schließen, 'Gitter & Lots zurücksetzen', dann neu starten.")
    levels = _gc_levels(rng)
    n = len(levels) - 1
    d = 1 if rng["dir"] == "long" else -1

    # ---- Position von der Boerse ------------------------------------------
    pos_signed, pos_known = 0.0, True
    if dry:
        pos_signed = d * sum(float(l["size"]) for l in lots)
    else:
        pos = await get_account_position_from_exchange(client, market_index, retries=2, delay=0.4)
        if pos is None:
            pos_known = False
        else:
            try:
                size = abs(float(pos.position))
                sign = int(getattr(pos, "sign", 1) or 1) or 1
                pos_signed = size * sign
                avg = float(pos.avg_entry_price) if size else None
                st["position"] = ("long" if sign > 0 else "short") if size else None
                st["avg_entry_price"] = avg
                st["total_coin_size"] = size
            except (TypeError, ValueError, AttributeError):
                pos_known = False

    # ---- Notbremse ---------------------------------------------------------
    stop_pct = float(_gcv(cfg, "gc_stop_pct"))
    if stop_pct > 0 and pos_known:
        breached = (mid < rng["lower"] * (1 - stop_pct / 100)) if d > 0 else (mid > rng["upper"] * (1 + stop_pct / 100))
        if breached:
            debug_log(f"🛑 [{symbol}] Grid-Bot NOTBREMSE: Kurs {mid:.6g} {stop_pct}% ausserhalb der Spanne - alles schließen, Bot stoppt")
            if not dry and pos_signed != 0:
                await _flatten_market(client, symbol, market_index, pos_signed, "grid-notbremse", cooldown_s=0)
            elif not dry:
                await _gc_cancel_own(client, market_index, cfg)
            gc_reset_state(st)
            st["gc_stopped"] = f"Notbremse bei {mid:.6g}"
            cfg["bot_active"] = False
            st["session_started"] = False
            await save_bot_configs()
            await save_bot_state()
            return

    # ---- Aktuelle Orders (Boerse bzw. Simulation) -------------------------
    prev = st.get("gc_prev") or {}
    live = {}   # tag -> {price,size,is_ask,coi}
    if dry:
        sim = st.setdefault("gc_sim", {})
        for tag, o in list(sim.items()):
            hit = (best_ask <= o["price"]) if not o["is_ask"] else (best_bid >= o["price"])
            if hit:
                sim.pop(tag)       # gefuellt -> verschwindet wie an der Boerse
        live = {t: dict(o) for t, o in sim.items()}
    else:
        for o in await read_open_orders(client, market_index):
            tag = _gc_decode(o["coi"], market_index)
            if tag:
                live[tag] = o

    filled = {t for t in prev if t not in live}
    # Orders, die der Bot selbst nur umgesetzt hat, stehen nicht in prev-Verschwunden: prev wird unten immer mit dem
    # tatsaechlichen Stand nach dem Abgleich ueberschrieben.
    if filled and pos_known:
        before = len(lots)
        _gc_apply_fills(st, rng, levels, filled, prev)

    # Positions-Abgleich (nur live): Boerse ist die Wahrheit
    if not dry and pos_known:
        lot_coin = (float(_gcv(cfg, "gc_size_usd")) / mid)
        have = d * sum(float(l["size"]) for l in lots)
        if abs(pos_signed - have) > 0.5 * lot_coin:
            if time.time() - float(st.get("gc_resync_log") or 0) > 30:
                st["gc_resync_log"] = time.time()
                debug_log(f"⚠️ [{symbol}] Grid-Bot: Lot-Buch ({have:.6g}) ≠ Börsen-Position ({pos_signed:.6g}) - Lots werden neu abgeleitet")
            _gc_rebuild_lots(st, rng, levels, pos_signed, mid, lot_coin)
    if dry:
        tot = sum(float(l["size"]) for l in lots)
        st["position"] = ("long" if d > 0 else "short") if tot else None
        st["total_coin_size"] = tot
        st["avg_entry_price"] = (sum(float(l["px"]) * float(l["size"]) for l in lots) / tot) if tot else None

    # ---- SOLL aufbauen und angleichen -------------------------------------
    desired = _gc_desired(symbol, cfg, st, rng, levels, best_bid, best_ask)
    desired_tags = {x["tag"] for x in desired}
    tick = _tick_size(symbol)
    precision = get_precision(symbol)
    min_base = get_min_base_amount(symbol)
    new_prev = {}
    actions = 0

    if dry:
        sim = st.setdefault("gc_sim", {})
        for tag in list(sim):
            if tag not in desired_tags:
                sim.pop(tag)
        for x in desired:
            ex = sim.get(x["tag"])
            if ex is None or abs(ex["price"] - x["price"]) > tick or abs(ex["size"] - x["size"]) > 1e-12:
                sim[x["tag"]] = {"price": x["price"], "size": x["size"], "size0": x["size"], "is_ask": x["is_ask"]}
        new_prev = {t: dict(o) for t, o in sim.items()}
    else:
        for tag, o in live.items():
            if tag not in desired_tags:
                try:
                    await cancel_order(client, market_index, o["coi"])
                except Exception as e:
                    debug_log(f"⚠️ [{symbol}] Grid: Cancel {tag} fehlgeschlagen", {"error": str(e)})
        for x in desired:
            ex = live.get(x["tag"])
            if ex is not None and abs(ex["price"] - x["price"]) <= tick and abs(ex["size"] - x["size"]) <= max(1e-12, x["size"] * 0.02):
                new_prev[x["tag"]] = {"price": ex["price"], "size": ex["size"], "size0": (prev.get(x["tag"]) or {}).get("size0") or ex["size"], "is_ask": x["is_ask"]}
                continue
            if ex is not None:
                if x["exit"] is False and ex["size"] < x["size"] * 0.98 and abs(ex["price"] - x["price"]) <= tick:
                    # teilgefuellter Einstieg: weiterlaufen lassen, nicht neu setzen
                    new_prev[x["tag"]] = {"price": ex["price"], "size": ex["size"], "size0": (prev.get(x["tag"]) or {}).get("size0") or x["size"], "is_ask": x["is_ask"]}
                    continue
                try:
                    await cancel_order(client, market_index, ex["coi"])
                except Exception as e:
                    debug_log(f"⚠️ [{symbol}] Grid: Cancel {x['tag']} fehlgeschlagen", {"error": str(e)})
            if x["size"] * precision < max(1, min_base * precision) or actions >= 8:
                continue
            base_amount = int(x["size"] * precision)
            coi = _gc_coi(market_index, x["idx"], x["exit"])
            tx, tx_hash, err = await place_post_only_order(client, market_index, symbol, x["is_ask"], base_amount,
                                                           x["price"], coi, reduce_only=x["reduce_only"])
            actions += 1
            if err:
                st["gs_last_error"] = str(err)
                if time.time() - float(st.get("gs_last_error_log") or 0) >= 60:
                    st["gs_last_error_log"] = time.time()
                    debug_log(f"⚠️ [{symbol}] Grid-Bot: Order abgelehnt ({x['tag']})", {"error": str(err), "preis": x["price"], "groesse": x["size"]})
                continue
            new_prev[x["tag"]] = {"price": x["price"], "size": x["size"], "size0": x["size"], "is_ask": x["is_ask"]}
    changed = (set(new_prev) != set(prev)) or bool(filled)
    st["gc_prev"] = new_prev

    # ---- Anzeige ------------------------------------------------------------
    upnl = 0.0
    for l in lots:
        upnl += (mid - float(l["px"])) * float(l["size"]) * d
    st["gc_view"] = {
        "dir": rng["dir"], "lower": rng["lower"], "upper": rng["upper"], "n": n, "levels": [round(x, 8) for x in levels],
        "lots": [{"k": l["k"], "px": l["px"], "size": l["size"]} for l in lots],
        "orders": [{"tag": t, "price": o["price"], "is_ask": o["is_ask"], "size": o["size"]} for t, o in new_prev.items()],
        "realized": round(float(st.get("gc_realized") or 0.0), 4), "cycles": int(st.get("gc_cycles") or 0),
        "upnl": round(upnl, 4), "max_lots": int(_gcv(cfg, "gc_max_lots")),
        "step_profit": round(float(_gcv(cfg, "gc_size_usd")) * ((rng["upper"] - rng["lower"]) / n) / max(mid, 1e-9), 4),
        "stopped": st.get("gc_stopped"),
    }
    if changed:
        now = time.time()
        if now - float(st.get("gc_last_save") or 0) > 1.0:
            st["gc_last_save"] = now
            await save_bot_state()
    now = time.time()
    if now - float(st.get("gs_last_heartbeat") or 0) >= 60:
        st["gs_last_heartbeat"] = now
        debug_log(f"💓 [{symbol}] Grid-Bot {rng['dir']} {'(Dry-Run) ' if dry else ''}Kurs {mid:.6g}, Lots {len(lots)}/{int(_gcv(cfg, 'gc_max_lots'))}, "
                  f"Orders {len(new_prev)}, Gewinn {float(st.get('gc_realized') or 0):+.3f} $ in {int(st.get('gc_cycles') or 0)} Stufen, offen {upnl:+.3f} $")


# ----------------------------------------------------------------------------
# Backtest (Kerzen: Hoch/Tief je Kerze, Pfad O -> L -> H -> C bzw. O -> H -> L -> C)
# ----------------------------------------------------------------------------
def gc_backtest(candles, cfg):
    """candles: (ts, o, h, l, c, v). Gibt Kennzahlen + Stufen-Liste zurueck."""
    ts, o, h, l, c = candles[0], candles[1], candles[2], candles[3], candles[4]
    n_c = len(c)
    d = 1 if _gcv(cfg, "gc_direction") != "short" else -1
    lo, hi = float(_gcv(cfg, "gc_lower")), float(_gcv(cfg, "gc_upper"))
    if lo <= 0 or hi <= lo:
        p = float(_gcv(cfg, "gc_auto_pct")) / 100.0
        lo, hi = o[0] * (1 - p), o[0] * (1 + p)
    rng = {"lower": lo, "upper": hi, "n": max(2, min(200, int(_gcv(cfg, "gc_levels")))), "spacing": _gcv(cfg, "gc_spacing")}
    levels = _gc_levels(rng)
    n = rng["n"]
    usd = float(_gcv(cfg, "gc_size_usd"))
    cap = int(_gcv(cfg, "gc_max_lots"))
    stop_pct = float(_gcv(cfg, "gc_stop_pct"))
    lots = {}          # k -> (px, size)
    realized, cycles, max_lots, worst_float, stopped_at = 0.0, 0, 0, 0.0, None
    steps = []

    def mtm(price):
        return sum((price - px) * sz * d for px, sz in lots.values())

    def move(a, b, i):
        nonlocal realized, cycles, max_lots
        if d > 0:
            if b < a:      # fallend: Kaeufe auf Levels dazwischen (absteigend)
                for k in range(n - 1, -1, -1):
                    if b <= levels[k] < a and k not in lots and len(lots) < cap:
                        lots[k] = (levels[k], usd / levels[k])
            elif b > a:    # steigend: Verkaeufe
                for k in sorted(lots):
                    if a < levels[k + 1] <= b:
                        px, sz = lots.pop(k)
                        pnl = (levels[k + 1] - px) * sz
                        realized += pnl; cycles += 1; steps.append({"ts": ts[i], "px": px, "exit": levels[k + 1], "pnl": pnl})
        else:
            if b > a:
                for k in range(1, n + 1):
                    if a < levels[k] <= b and k not in lots and len(lots) < cap:
                        lots[k] = (levels[k], usd / levels[k])
            elif b < a:
                for k in sorted(lots, reverse=True):
                    if b <= levels[k - 1] < a:
                        px, sz = lots.pop(k)
                        pnl = (px - levels[k - 1]) * sz
                        realized += pnl; cycles += 1; steps.append({"ts": ts[i], "px": px, "exit": levels[k - 1], "pnl": -pnl * -1})
        max_lots = max(max_lots, len(lots))

    for i in range(n_c):
        path = (o[i], l[i], h[i], c[i]) if c[i] >= o[i] else (o[i], h[i], l[i], c[i])
        for a, b in zip(path, path[1:]):
            move(a, b, i)
            if lots:
                worst_float = min(worst_float, mtm(b))
        if stop_pct > 0 and ((d > 0 and l[i] < lo * (1 - stop_pct / 100)) or (d < 0 and h[i] > hi * (1 + stop_pct / 100))):
            loss = mtm(lo * (1 - stop_pct / 100) if d > 0 else hi * (1 + stop_pct / 100))
            realized += loss; stopped_at = ts[i]; lots.clear()
            break
    final_float = mtm(c[-1]) if lots else 0.0
    days = max((ts[-1] - ts[0]) / 86400000.0, 1e-9)
    return {"levels": levels, "lower": lo, "upper": hi, "n": n, "direction": "long" if d > 0 else "short",
            "cycles": cycles, "realized": round(realized, 2), "open_lots": len(lots), "open_float": round(final_float, 2),
            "total": round(realized + final_float, 2), "max_lots": max_lots, "worst_float": round(worst_float, 2),
            "per_day": round(realized / days, 2), "stopped_at": stopped_at, "days": round(days, 1),
            "step_profit": round(usd * ((hi - lo) / n) / max(o[0], 1e-9), 3), "margin_peak_notional": round(max_lots * usd, 0),
            "candles": n_c, "steps": steps[-300:]}


async def handle_gc_backtest(request):
    from aiohttp import web
    from strategies import _fetch_cached_mo7_backtest_candles
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    body = await request.json()
    cfg = dict(BOTS[symbol]["config"])
    ov = body.get("config")
    if isinstance(ov, dict):
        cfg.update({k: v for k, v in ov.items() if k.startswith("gc_")})
    try:
        days = max(1.0 / 24, min(90.0, float(body.get("days", 14))))
    except (TypeError, ValueError):
        days = 14.0
    try:
        cand, err = await asyncio.wait_for(_fetch_cached_mo7_backtest_candles(symbol, "1m", days, 100_000, market_type=cfg.get("binance_market_type", "spot")), timeout=90)
        if err:
            return web.json_response({"error": err}, status=400)
        res = await asyncio.to_thread(gc_backtest, cand, cfg)
    except asyncio.TimeoutError:
        return web.json_response({"error": "Backtest nach 90 s abgebrochen - kürzeren Zeitraum wählen"}, status=504)
    except Exception as e:
        return web.json_response({"error": f"Backtest fehlgeschlagen: {e}"}, status=500)
    return web.json_response(res)


async def handle_gc_reset(request):
    from aiohttp import web
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    st = BOTS[symbol]["state"]
    cfg = BOTS[symbol]["config"]
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    if cfg.get("bot_active") and not body.get("force"):
        return web.json_response({"error": "Bot erst stoppen (Orders werden dabei abgeräumt), dann Gitter zurücksetzen."}, status=409)
    gc_reset_state(st, keep_stats=not body.get("stats"))
    await save_bot_state()
    return web.json_response({"success": True})


# ============================================================================
# Probe-Helfer: einmal laufen lassen, BEVOR dry_run ausgeht
# ============================================================================

async def probe_grid_scalp(symbol):
    """Prueft die Feldnamen der API-Antworten und misst den echten Spread.
    Ueber die Konsole aufrufen oder einmalig in main() einhaengen."""
    client = get_lighter_client("grid")
    market_index = MARKET_INDICES[symbol]
    print(f"=== {symbol} (market_index={market_index}) ===")
    pos = await get_account_position_from_exchange(client, market_index, retries=1, delay=0)
    print("POSITION RAW:", pos)
    try:
        print("OPEN ORDERS:", await read_open_orders(client, market_index))
    except Exception as e:
        print("OPEN ORDERS FEHLER:", e)

    vals = []
    for _ in range(30):
        bb, ba = await read_best_bid_ask(client, market_index)
        if bb:
            vals.append((ba - bb) / ((ba + bb) / 2))
        await asyncio.sleep(1)
    if vals:
        vals.sort()
        med = vals[len(vals) // 2]
        tp = BOTS[symbol]["config"].get("gs_tp_usd", 1.0)
        print(f"Spread Median: {round(med*100, 4)} % | Round-Trip als Taker: {round(med*200, 4)} %")
        print(f"-> empfohlenes gs_step_notional_usd: {round(tp / (5 * med * 2))} $")
    await asyncio.wait_for(client.close(), timeout=EXCHANGE_CALL_TIMEOUT_SECONDS)
