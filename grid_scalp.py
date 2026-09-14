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
    get_account_position_from_exchange, now_local,
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


# ============================================================================
# Order-Schicht (neu - existierte im Bot bisher nicht)
# ============================================================================

async def place_post_only_order(client, market_index, symbol, is_ask, base_amount,
                                price, coi, reduce_only=False):
    """Post-Only-Limit. Wuerde die Order das Buch kreuzen, verwirft die Boerse sie -
    das ist KEIN Fehler, sondern der Sinn von Post-Only. Naechster Tick setzt neu."""
    price_decimals = get_price_decimals(symbol)
    price_scaled = int(round(price * (10 ** price_decimals)))
    tx, tx_hash, err = await client.create_order(
        market_index=market_index,
        client_order_index=coi,
        base_amount=base_amount,
        price=price_scaled,
        is_ask=is_ask,
        order_type=client.ORDER_TYPE_LIMIT,
        time_in_force=client.ORDER_TIME_IN_FORCE_POST_ONLY,
        reduce_only=reduce_only,
        order_expiry=client.DEFAULT_28_DAY_ORDER_EXPIRY,
    )
    return tx, tx_hash, err


async def cancel_order(client, market_index, coi):
    return await client.cancel_order(market_index=market_index, order_index=coi)


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
    ob = await order_api.order_book_orders(market_index, 1)
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
        else:
            # Oeffnender/aufstockender Fill
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


async def _flatten_market(client, symbol, market_index, pos_size, reason):
    """Notausstieg. EINZIGE Taker-Order im ganzen Modul - hier ist Fill wichtiger
    als Spread."""
    from bot_core import place_market_order

    st, cfg = BOTS[symbol]["state"], BOTS[symbol]["config"]

    # Offene Orders zuerst weg, sonst kollidiert das Flatten mit den eigenen Quotes
    try:
        for o in await read_open_orders(client, market_index):
            await cancel_order(client, market_index, o["coi"])
    except Exception as e:
        debug_log(f"⚠️ [{symbol}] Grid-Scalp: Orders vor Flatten nicht abraeumbar", {"error": str(e)})

    is_ask = pos_size > 0
    base_amount = int(abs(pos_size) * get_precision(symbol))
    price = st.get("last_price") or 0.0

    if cfg["dry_run"]:
        debug_log(f"🧪 [{symbol}] DRY FLATTEN ({reason}): {'SELL' if is_ask else 'BUY'} {abs(pos_size)}")
    else:
        tx, tx_hash, err = await place_market_order(
            client, market_index, symbol, is_ask, base_amount, price, reduce_only=True)
        if err:
            debug_log(f"⚠️ [{symbol}] Grid-Scalp FLATTEN FEHLGESCHLAGEN", {"error": str(err)})
            return
        debug_log(f"🚪 [{symbol}] Grid-Scalp FLATTEN ausgefuehrt ({reason})", {"tx_hash": str(tx_hash)})

    st["gs_tag_map"] = {}
    st["gs_cooldown_until"] = time.time() + float(cfg.get("gs_cooldown_min", 30.0)) * 60
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

    pos_size, avg_entry = await _sync_position(client, symbol, market_index)

    # 1) Notausstieg ZUERST - vor allem, was neue Orders posten koennte
    if pos_size != 0 and avg_entry:
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
            for o in await read_open_orders(client, market_index):
                await cancel_order(client, market_index, o["coi"])
            st.setdefault("gs_tag_map", {}).clear()
            return

    if not cfg.get("bot_active", True):
        debug_log(f"\u26d4 [{symbol}] Grid-Scalp: bot_active=False - raeume Orders ab und warte")
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
    desired = _desired_orders(symbol, cfg, st, pos_size, avg_entry, best_bid, best_ask)
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
        if cfg.get("entry_mode") != "grid_scalp":
            if client is not None:
                try:
                    await client.close()
                except Exception:
                    pass
                client = None
            # Alle 5 Minuten melden, WARUM nichts passiert. Sonst ist "Strategie nicht
            # aktiv" im Log nicht von "Loop laeuft gar nicht" zu unterscheiden.
            if time.time() - last_idle_log >= 300:
                last_idle_log = time.time()
                debug_log(f"\U0001f4a4 [{symbol}] Grid-Scalp inaktiv "
                          f"(entry_mode ist '{cfg.get('entry_mode')}', erwartet 'grid_scalp')")
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
                await client.close()
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
    await client.close()
