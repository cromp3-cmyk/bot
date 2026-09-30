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
MODES = ("grid_scalp", "maker_scalp")


def _ms(cfg, key):
    v = cfg.get(key)
    return MAKER_DEFAULTS[key] if v is None else v


# ============================================================================
# Order-Schicht (neu - existierte im Bot bisher nicht)
# ============================================================================

async def place_post_only_order(client, market_index, symbol, is_ask, base_amount,
                                price, coi, reduce_only=False):
    """Post-Only-Limit. Wuerde die Order das Buch kreuzen, verwirft die Boerse sie -
    das ist KEIN Fehler, sondern der Sinn von Post-Only. Naechster Tick setzt neu."""
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


def _publish_levels(st, cfg, mode, desired, pos_size, avg_entry):
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

    best_bid, best_ask = await read_best_bid_ask(client, market_index)
    if best_bid is None:
        return
    mid = (best_bid + best_ask) / 2.0
    st["last_price"] = mid
    st["last_bid"], st["last_ask"] = best_bid, best_ask
    mode = cfg.get("entry_mode")

    if cfg["dry_run"]:
        # Dry-Run: simulierte Position/Orders statt Boersen-Zustand (vorher lief die Simulation nie)
        pos_size, avg_entry = _sim_process_fills(symbol, st, cfg, best_bid, best_ask)
        _sim_markouts(st, mid)
    else:
        pos_size, avg_entry = await _sync_position(client, symbol, market_index)

    # 1) Notausstieg ZUERST - vor allem, was neue Orders posten koennte
    if mode == "maker_scalp" and pos_size != 0 and avg_entry:
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
    elif mode != "maker_scalp" and pos_size != 0 and avg_entry:
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
    else:
        desired = _desired_orders(symbol, cfg, st, pos_size, avg_entry, best_bid, best_ask)

    _publish_levels(st, cfg, mode, desired, pos_size, avg_entry)

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
                client = get_lighter_client()
                if client is None:
                    await asyncio.sleep(10)
                    continue
                debug_log(f"🔌 [{symbol}] Grid-Scalp: Client verbunden")
            await grid_scalp_tick(client, symbol)
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] Grid-Scalp Tick-Fehler",
                      {"error": str(e), "traceback": traceback.format_exc()})
            try:
                await asyncio.wait_for(client.close(), timeout=EXCHANGE_CALL_TIMEOUT_SECONDS)
            except Exception:
                pass
            client = None
            await asyncio.sleep(5)

        await asyncio.sleep(float(BOTS[symbol]["config"].get("gs_poll_seconds", 2.0)))


# ============================================================================
# Probe-Helfer: einmal laufen lassen, BEVOR dry_run ausgeht
# ============================================================================

async def probe_grid_scalp(symbol):
    """Prueft die Feldnamen der API-Antworten und misst den echten Spread.
    Ueber die Konsole aufrufen oder einmalig in main() einhaengen."""
    client = get_lighter_client()
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
