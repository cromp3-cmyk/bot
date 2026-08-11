"""
copytrade.py - Copy-Trading: Hyperliquid Top-Trader beobachten und optional
auf Lighter nachbilden. Nutzt gemeinsame Infrastruktur aus bot_core.py.
"""

import asyncio
import aiohttp
import json
import time
import os
import traceback
from datetime import datetime
from aiohttp import web

from bot_core import (
    debug_log, get_lighter_client, place_market_order, get_precision,
    get_price_decimals, get_min_base_amount, MARKET_INDICES, get_redis,
)

HL_INFO_URL = "https://api.hyperliquid.xyz/info"
HL_LEADERBOARD_URL = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"

CT_CONFIG = {
    "dry_run": os.getenv("CT_DRY_RUN", os.getenv("DRY_RUN", "true")).lower() == "true",
    "copy_margin": float(os.getenv("COPY_MARGIN", "10")),
    "copy_leverage": int(os.getenv("COPY_LEVERAGE", "3")),
    "leaderboard_top_n": int(os.getenv("LEADERBOARD_TOP_N", "100")),
    "leaderboard_refresh_minutes": int(os.getenv("LEADERBOARD_REFRESH_MINUTES", "30")),
    "poll_interval_seconds": int(os.getenv("CT_POLL_INTERVAL_SECONDS", "15")),
}

CT_MANUAL_ADDRESSES = [a.strip() for a in os.getenv("MANUAL_WALLET_ADDRESSES", "").split(",") if a.strip()]

CT_STATE = {
    "leaderboard": [],
    "leaderboard_last_fetch": None,
    "leaderboard_error": None,
    "watched": {},
    "copy_log": [],  # sichtbares Log aller Copy-Versuche (dry_run/success/error/skipped) fuers Dashboard
    "copy_positions": {},  # address -> {coin: {direction, entry_price, size, opened_at, margin, leverage}} - UNSERE eigenen kopierten Positionen, getrennt von den Positionen des Traders
    "copy_stats": {},  # address -> {trades, wins, losses, total_pnl_usd} - echte Einstieg->Ausstieg-PnL-Statistik
}
CT_COPY_LOG_MAX = 200



async def execute_copy_trade(symbol, direction, reference_price, margin, leverage):
    """Kopiert die RICHTUNG eines Trades mit der fuer diesen Trader/Coin eingestellten Margin/Hebel.
    Gibt ein Status-Dict zurueck (fuer das sichtbare Copy-Trade-Log im Dashboard) statt nur zu
    loggen - vorher gab es keine Stelle, an der man im Dashboard nachvollziehen konnte, was
    tatsaechlich (simuliert oder echt) kopiert wurde."""
    if symbol not in MARKET_INDICES:
        msg = f"Coin {symbol} nicht auf Lighter gemappt"
        debug_log(f"⚠️ [CopyTrading] {msg} - übersprungen")
        return {"status": "skipped", "detail": msg}

    if CT_CONFIG["dry_run"]:
        debug_log(f"🧪 [CopyTrading] DRY_RUN - würde kopieren: {direction.upper()} {symbol} @ ~{reference_price} (Margin {margin}, Hebel {leverage}x)")
        return {"status": "dry_run", "detail": None}

    client = get_lighter_client()
    if client is None:
        return {"status": "error", "detail": "Kein Lighter-Client verfügbar"}
    try:
        market_index = MARKET_INDICES[symbol]
        precision = get_precision(symbol)
        min_base = get_min_base_amount(symbol)
        position_usdc = margin * leverage
        coin_amount = position_usdc / reference_price
        base_amount = int(coin_amount * precision)
        if base_amount * (1 / precision) < min_base:
            msg = f"Order-Größe für {symbol} unter Mindestgröße"
            debug_log(f"⚠️ [CopyTrading] {msg}")
            return {"status": "skipped", "detail": msg}
        is_ask = direction == "short"
        try:
            await client.update_leverage(market_index=market_index, leverage=leverage, margin_mode=0)
        except Exception as e:
            debug_log("[CopyTrading] Hebel setzen fehlgeschlagen", {"error": str(e)})
        tx, tx_hash, err = await place_market_order(client, market_index, symbol, is_ask, base_amount, reference_price)
        if err:
            debug_log(f"⚠️ [CopyTrading] Order fehlgeschlagen für {symbol}", {"error": str(err)})
            return {"status": "error", "detail": str(err)}
        else:
            debug_log(f"✅ [CopyTrading] ECHTER Copy-Trade: {direction.upper()} {symbol} @ ~{reference_price}", {"tx_hash": str(tx_hash)})
            return {"status": "success", "detail": str(tx_hash)}
    finally:
        await client.close()


async def execute_copy_close(symbol, position_direction, size_coin_units, reference_price):
    """Schliesst eine kopierte Position in EXAKT der Groesse, die beim Einstieg (bzw. nach
    Nachkaeufen) tatsaechlich kopiert wurde - NICHT neu aus Margin*Hebel berechnet, da sich der
    Preis seither veraendert haben kann und execute_copy_trade() sonst eine falsche Groesse
    zum Schliessen verwenden wuerde. Schliessen einer Long-Position heisst verkaufen (Ask),
    Schliessen einer Short-Position heisst kaufen (Bid)."""
    if symbol not in MARKET_INDICES:
        msg = f"Coin {symbol} nicht auf Lighter gemappt"
        debug_log(f"⚠️ [CopyTrading] {msg} - Schliessen übersprungen")
        return {"status": "skipped", "detail": msg}

    if CT_CONFIG["dry_run"]:
        debug_log(f"🧪 [CopyTrading] DRY_RUN - würde schliessen: {position_direction.upper()}-Position {symbol} Größe {round(size_coin_units,6)} @ ~{reference_price}")
        return {"status": "dry_run", "detail": None}

    client = get_lighter_client()
    if client is None:
        return {"status": "error", "detail": "Kein Lighter-Client verfügbar"}
    try:
        market_index = MARKET_INDICES[symbol]
        precision = get_precision(symbol)
        base_amount = int(size_coin_units * precision)
        if base_amount <= 0:
            return {"status": "skipped", "detail": "Größe nach Rundung 0"}
        is_ask = position_direction == "long"  # Long schliessen = verkaufen
        tx, tx_hash, err = await place_market_order(client, market_index, symbol, is_ask, base_amount, reference_price)
        if err:
            debug_log(f"⚠️ [CopyTrading] Schliess-Order fehlgeschlagen für {symbol}", {"error": str(err)})
            return {"status": "error", "detail": str(err)}
        else:
            debug_log(f"✅ [CopyTrading] ECHTE Position geschlossen: {position_direction.upper()} {symbol} @ ~{reference_price}", {"tx_hash": str(tx_hash)})
            return {"status": "success", "detail": str(tx_hash)}
    finally:
        await client.close()


def _record_copy_close(address, label, coin, direction, entry_price, exit_price, size, reason):
    """Verbucht das Ergebnis einer geschlossenen kopierten Position in copy_stats (aggregiert
    pro Trader) und haengt einen PnL-Eintrag ans Copy-Trade-Log an - das ist die eigentliche
    Einstieg->Ausstieg-PnL-Verfolgung, unabhaengig von den einzelnen Kopier-Versuchen."""
    pnl_usd = (exit_price - entry_price) * size if direction == "long" else (entry_price - exit_price) * size
    stats = CT_STATE["copy_stats"].setdefault(address, {"trades": 0, "wins": 0, "losses": 0, "total_pnl_usd": 0.0})
    stats["trades"] += 1
    if pnl_usd >= 0:
        stats["wins"] += 1
    else:
        stats["losses"] += 1
    stats["total_pnl_usd"] = round(stats["total_pnl_usd"] + pnl_usd, 4)
    CT_STATE["copy_log"].insert(0, {
        "ts": int(time.time() * 1000), "trader_label": label, "address": address,
        "coin": coin, "direction": direction, "price": exit_price, "dry_run": CT_CONFIG["dry_run"],
        "status": "closed", "action": reason, "pnl_usd": round(pnl_usd, 4),
        "entry_price": entry_price, "size": size,
    })
    CT_STATE["copy_log"] = CT_STATE["copy_log"][:CT_COPY_LOG_MAX]
    return pnl_usd



async def fetch_leaderboard(session):
    """Best-effort Parsing eines INOFFIZIELLEN Endpoints - Feldnamen sind eine Annahme."""
    try:
        async with session.get(HL_LEADERBOARD_URL, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status != 200:
                text = await resp.text()
                CT_STATE["leaderboard_error"] = f"HTTP {resp.status}: {text[:300]}"
                debug_log(f"⚠️ [CopyTrading] Leaderboard HTTP {resp.status}", {"body": text[:500]})
                return None
            data = await resp.json(content_type=None)
    except Exception as e:
        CT_STATE["leaderboard_error"] = str(e)
        debug_log("⚠️ [CopyTrading] Leaderboard-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        return None

    debug_log("🔎 [CopyTrading] Leaderboard Rohantwort (gekürzt)", {
        "typ": str(type(data)),
        "keys_oder_laenge": (list(data.keys()) if isinstance(data, dict) else len(data) if isinstance(data, list) else None),
    })

    rows = None
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        for key in ("leaderboardRows", "rows", "data", "leaderboard"):
            if key in data and isinstance(data[key], list):
                rows = data[key]
                break

    if not rows:
        CT_STATE["leaderboard_error"] = "Konnte Liste in der Antwort nicht finden - Rohstruktur siehe Debug-Log"
        return None

    parsed = []
    for row in rows[:500]:
        if not isinstance(row, dict):
            continue
        address = row.get("ethAddress") or row.get("address") or row.get("user")
        pnl = row.get("pnl") or row.get("allTimePnl") or row.get("accountValue")
        if pnl is None and "windowPerformances" in row:
            try:
                pnl = dict(row["windowPerformances"]).get("allTime", {}).get("pnl")
            except Exception:
                pass
        if address:
            parsed.append({"address": address, "pnl": pnl})

    parsed.sort(key=lambda r: float(r["pnl"]) if r["pnl"] not in (None, "") else 0.0, reverse=True)
    return parsed


async def fetch_user_state(session, address):
    try:
        async with session.post(HL_INFO_URL, json={"type": "clearinghouseState", "user": address},
                                 timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                return None
            return await resp.json(content_type=None)
    except Exception as e:
        debug_log(f"⚠️ [CopyTrading] userState fehlgeschlagen für {address}", {"error": str(e)})
        return None


async def fetch_user_fills(session, address):
    try:
        async with session.post(HL_INFO_URL, json={"type": "userFills", "user": address},
                                 timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                return None
            return await resp.json(content_type=None)
    except Exception as e:
        debug_log(f"⚠️ [CopyTrading] userFills fehlgeschlagen für {address}", {"error": str(e)})
        return None


def extract_ct_positions(user_state):
    if not user_state:
        return []
    out = []
    for ap in user_state.get("assetPositions", []):
        pos = ap.get("position", {})
        szi = float(pos.get("szi", 0) or 0)
        if szi == 0:
            continue
        out.append({
            "coin": pos.get("coin"), "side": "long" if szi > 0 else "short", "size": abs(szi),
            "entry_price": pos.get("entryPx"), "unrealized_pnl": pos.get("unrealizedPnl"),
        })
    return out



async def ct_leaderboard_refresh_loop():
    async with aiohttp.ClientSession() as session:
        while True:
            debug_log("📡 [CopyTrading] Aktualisiere Hyperliquid-Leaderboard...")
            rows = await fetch_leaderboard(session)
            if rows:
                CT_STATE["leaderboard"] = rows[:CT_CONFIG["leaderboard_top_n"]]
                CT_STATE["leaderboard_last_fetch"] = datetime.now().isoformat()
                CT_STATE["leaderboard_error"] = None
                debug_log(f"✅ [CopyTrading] Leaderboard aktualisiert: {len(CT_STATE['leaderboard'])} Trader")
                for i, row in enumerate(CT_STATE["leaderboard"]):
                    addr = row["address"]
                    if addr not in CT_STATE["watched"]:
                        # monitor_enabled bewusst False: neue Leaderboard-Trader werden nur GELISTET
                        # (Adresse+PnL sichtbar), aber NICHT per userState/userFills abgefragt, bis
                        # der Nutzer sie explizit per "Beobachten"-Schalter dazu freigibt. Verhindert,
                        # dass alle leaderboard_top_n Trader dauerhaft im 15s-Takt abgefragt werden.
                        CT_STATE["watched"][addr] = {
                            "label": f"#{i+1} (Leaderboard)", "copy_enabled": False, "monitor_enabled": False,
                            "copy_skip_nachkauf": False,
                            "coin_settings": {}, "copy_margin": CT_CONFIG["copy_margin"], "copy_leverage": CT_CONFIG["copy_leverage"],
                            "last_fill_time": None, "positions": [], "recent_fills": [], "source": "leaderboard",
                            "position_meta": {}, "leaderboard_pnl": row.get("pnl"),
                            "behavior_stats": {"neu": 0, "nachkauf": 0, "reverse": 0},
                        }
                    else:
                        # PnL bei jedem Refresh nachziehen, auch fuer schon bekannte Trader
                        CT_STATE["watched"][addr]["leaderboard_pnl"] = row.get("pnl")
            await asyncio.sleep(CT_CONFIG["leaderboard_refresh_minutes"] * 60)


async def ct_watch_loop():
    for addr in CT_MANUAL_ADDRESSES:
        if addr not in CT_STATE["watched"]:
            CT_STATE["watched"][addr] = {
                "label": "Manuell hinzugefügt", "copy_enabled": False, "monitor_enabled": False,
                "copy_skip_nachkauf": False,
                "coin_settings": {}, "copy_margin": CT_CONFIG["copy_margin"], "copy_leverage": CT_CONFIG["copy_leverage"],
                "last_fill_time": None, "positions": [], "recent_fills": [], "source": "manual",
                "position_meta": {}, "leaderboard_pnl": None,
                "behavior_stats": {"neu": 0, "nachkauf": 0, "reverse": 0},
            }

    async with aiohttp.ClientSession() as session:
        while True:
            for address, info in list(CT_STATE["watched"].items()):
                # Nur Trader abfragen, die der Nutzer aktiv beobachtet ODER kopiert - alle
                # anderen bleiben im Leaderboard sichtbar (Adresse+PnL), aber ohne staendige
                # userState/userFills-Anfragen an Hyperliquid.
                if not (info.get("monitor_enabled") or info.get("copy_enabled")):
                    continue

                user_state = await fetch_user_state(session, address)
                info["positions"] = extract_ct_positions(user_state)

                # Position-Metadaten aufräumen: Coins, die nicht mehr offen sind, aus der Historie entfernen
                meta = info.setdefault("position_meta", {})
                open_coins = {p["coin"] for p in info["positions"]}
                for coin in list(meta.keys()):
                    if coin not in open_coins:
                        del meta[coin]

                fills = await fetch_user_fills(session, address)
                if fills:
                    fills_sorted = sorted(fills, key=lambda f: f.get("time", 0))
                    info["recent_fills"] = fills_sorted[-20:]

                    if info["last_fill_time"] is None:
                        info["last_fill_time"] = fills_sorted[-1]["time"] if fills_sorted else int(time.time() * 1000)
                    else:
                        new_fills = [f for f in fills_sorted if f.get("time", 0) > info["last_fill_time"]]
                        copy_actions = 0
                        stats = info.setdefault("behavior_stats", {"neu": 0, "nachkauf": 0, "reverse": 0})

                        for f in new_fills:
                            info["last_fill_time"] = f["time"]
                            coin = f.get("coin")
                            side = f.get("side")
                            fill_direction = "long" if side == "B" else "short"
                            price = float(f.get("px", 0) or 0)
                            fill_size = float(f.get("sz", 0) or 0)
                            dir_field = f.get("dir", "") or ""
                            is_close_fill = dir_field.startswith("Close")
                            is_open_fill = dir_field.startswith("Open") or not dir_field  # Fallback falls dir mal fehlt

                            # --- Klassifikation nur fuers Verhalten/Anzeige (Neu/Nachkauf/Reverse-Spalte) ---
                            action = None
                            prev_meta = meta.get(coin)
                            now_iso = datetime.now().isoformat()
                            if prev_meta is None:
                                meta[coin] = {"opened_at": now_iso, "direction": fill_direction, "entries": 1, "last_action": "Neu"}
                                stats["neu"] += 1
                                action = "Neu"
                            elif prev_meta["direction"] == fill_direction:
                                prev_meta["entries"] += 1
                                prev_meta["last_action"] = "Nachkauf"
                                stats["nachkauf"] += 1
                                action = "Nachkauf"
                            else:
                                meta[coin] = {"opened_at": now_iso, "direction": fill_direction, "entries": 1, "last_action": "Reverse"}
                                stats["reverse"] += 1
                                action = "Reverse"

                            if not (info["copy_enabled"] and price > 0):
                                continue
                            coin_cfg = (info.get("coin_settings") or {}).get(coin)
                            if not (coin_cfg and coin_cfg.get("enabled", True)):
                                continue

                            our_pos = CT_STATE["copy_positions"].get(address, {}).get(coin)

                            # --- Trader beginnt/vollendet einen Ausstieg -> UNSERE ganze kopierte
                            # Position schliessen (Einstieg->Ausstieg-PnL wird hier verbucht).
                            # Bewusst die GESAMTE Position schliessen statt anteilig, da wir die
                            # Restgroesse des Traders nicht 1:1 nachbilden (andere Margin/Hebel). ---
                            if is_close_fill and our_pos:
                                close_result = await execute_copy_close(coin, our_pos["direction"], our_pos["size"], price)
                                pnl_usd = _record_copy_close(address, info["label"], coin, our_pos["direction"],
                                                              our_pos["entry_price"], price, our_pos["size"], "Close")
                                debug_log(f"🏁 [CopyTrading] Kopierte Position geschlossen bei {info['label']} ({address[:8]}...): "
                                          f"{our_pos['direction'].upper()} {coin} PnL ${round(pnl_usd,3)} ({close_result['status']})")
                                del CT_STATE["copy_positions"][address][coin]
                                copy_actions += 1
                                await save_ct_copy_state()
                                continue

                            if not is_open_fill:
                                continue  # z.B. reine "Close"-Fills ohne eigene offene Position - nichts zu tun

                            skip_as_nachkauf = info.get("copy_skip_nachkauf", False) and action == "Nachkauf"

                            # Verteidigend: falls der Trader ohne separaten Close-Fill direkt dreht
                            # (unser our_pos zeigt noch in die alte Richtung) - erst schliessen
                            if our_pos and our_pos["direction"] != fill_direction:
                                close_result = await execute_copy_close(coin, our_pos["direction"], our_pos["size"], price)
                                pnl_usd = _record_copy_close(address, info["label"], coin, our_pos["direction"],
                                                              our_pos["entry_price"], price, our_pos["size"], "Reverse")
                                debug_log(f"🔄 [CopyTrading] Kopierte Position gedreht bei {info['label']} ({address[:8]}...): "
                                          f"PnL ${round(pnl_usd,3)} ({close_result['status']})")
                                del CT_STATE["copy_positions"][address][coin]
                                our_pos = None
                                copy_actions += 1
                                await save_ct_copy_state()

                            if skip_as_nachkauf:
                                debug_log(f"⏭️ [CopyTrading] Nachkauf bei {info['label']} ({address[:8]}...) übersprungen (nur Neu/Reverse aktiv): {fill_direction.upper()} {coin} @ {price}")
                                continue

                            margin = coin_cfg.get("margin") or info.get("copy_margin", CT_CONFIG["copy_margin"])
                            leverage = coin_cfg.get("leverage") or info.get("copy_leverage", CT_CONFIG["copy_leverage"])
                            debug_log(f"🆕 [CopyTrading] Kopiere Fill ({action}) bei {info['label']} ({address[:8]}...): {fill_direction.upper()} {coin} @ {price}")
                            result = await execute_copy_trade(coin, fill_direction, price, margin, leverage)
                            CT_STATE["copy_log"].insert(0, {
                                "ts": int(time.time() * 1000), "trader_label": info["label"], "address": address,
                                "coin": coin, "direction": fill_direction, "price": price, "margin": margin,
                                "leverage": leverage, "dry_run": CT_CONFIG["dry_run"],
                                "status": result["status"], "detail": result.get("detail"), "action": action,
                            })
                            CT_STATE["copy_log"] = CT_STATE["copy_log"][:CT_COPY_LOG_MAX]
                            copy_actions += 1

                            if result["status"] in ("success", "dry_run"):
                                add_size = (margin * leverage) / price
                                trader_positions = CT_STATE["copy_positions"].setdefault(address, {})
                                if our_pos:
                                    total_size = our_pos["size"] + add_size
                                    our_pos["entry_price"] = (our_pos["entry_price"] * our_pos["size"] + price * add_size) / total_size
                                    our_pos["size"] = total_size
                                else:
                                    trader_positions[coin] = {
                                        "direction": fill_direction, "entry_price": price, "size": add_size,
                                        "opened_at": now_iso, "margin": margin, "leverage": leverage,
                                    }
                                await save_ct_copy_state()

                        if new_fills:
                            # Eine Sammelzeile statt einer Zeile pro Fill - haelt das Log lesbar,
                            # auch wenn der Trader (z.B. Market-Maker) hunderte Fills auf einmal macht
                            debug_log(f"📊 [CopyTrading] {info['label']} ({address[:8]}...): {len(new_fills)} neue Fills erkannt, {copy_actions} davon kopiert")

                await asyncio.sleep(0.5)

            await asyncio.sleep(CT_CONFIG["poll_interval_seconds"])


# ========== LIGHTER CLIENT ==========

# ========== REDIS-PERSISTENZ (Copy-Trading-Einstellungen) ==========
async def save_ct_watched():
    r = await get_redis()
    if r is None:
        return
    try:
        trimmed = {
            addr: {
                "label": info.get("label"), "copy_enabled": info.get("copy_enabled", False),
                "monitor_enabled": info.get("monitor_enabled", False),
                "copy_skip_nachkauf": info.get("copy_skip_nachkauf", False),
                "coin_settings": info.get("coin_settings", {}), "copy_margin": info.get("copy_margin"),
                "copy_leverage": info.get("copy_leverage"), "source": info.get("source"),
                "behavior_stats": info.get("behavior_stats", {"neu": 0, "nachkauf": 0, "reverse": 0}),
            }
            for addr, info in CT_STATE["watched"].items()
        }
        await r.set("gridbot:ct_watched", json.dumps(trimmed))
    except Exception as e:
        debug_log("⚠️ Speichern der Copy-Trading-Einstellungen fehlgeschlagen", {"error": str(e)})


async def load_ct_watched():
    r = await get_redis()
    if r is None:
        return
    try:
        raw_watched = await r.get("gridbot:ct_watched")
        if raw_watched:
            saved = json.loads(raw_watched)
            for addr, cfg in saved.items():
                CT_STATE["watched"][addr] = {
                    "label": cfg.get("label", "Wiederhergestellt"), "copy_enabled": cfg.get("copy_enabled", False),
                    "monitor_enabled": cfg.get("monitor_enabled", False),
                    "copy_skip_nachkauf": cfg.get("copy_skip_nachkauf", False),
                    "coin_settings": cfg.get("coin_settings", {}), "copy_margin": cfg.get("copy_margin", CT_CONFIG["copy_margin"]),
                    "copy_leverage": cfg.get("copy_leverage", CT_CONFIG["copy_leverage"]), "source": cfg.get("source", "manual"),
                    "last_fill_time": None, "positions": [], "recent_fills": [], "position_meta": {},
                    "leaderboard_pnl": None, "behavior_stats": cfg.get("behavior_stats", {"neu": 0, "nachkauf": 0, "reverse": 0}),
                }
            debug_log(f"✅ {len(saved)} Copy-Trading-Trader aus Redis wiederhergestellt")
    except Exception as e:
        debug_log("⚠️ Laden der Copy-Trading-Einstellungen fehlgeschlagen", {"error": str(e)})
    await load_ct_copy_state()


async def save_ct_copy_state():
    """Speichert offene kopierte Positionen + PnL-Statistik separat von den Trader-Einstellungen,
    da sich diese bei jedem Fill aendern koennen (nicht nur bei manuellen Schalter-Klicks) -
    echte offene Positionen (reales Geld, falls nicht DRY_RUN) muessen einen Neustart ueberleben."""
    r = await get_redis()
    if r is None:
        return
    try:
        await r.set("gridbot:ct_copy_positions", json.dumps(CT_STATE["copy_positions"]))
        await r.set("gridbot:ct_copy_stats", json.dumps(CT_STATE["copy_stats"]))
    except Exception as e:
        debug_log("⚠️ Speichern der Copy-Positionen fehlgeschlagen", {"error": str(e)})


async def load_ct_copy_state():
    r = await get_redis()
    if r is None:
        return
    try:
        raw_pos = await r.get("gridbot:ct_copy_positions")
        if raw_pos:
            CT_STATE["copy_positions"] = json.loads(raw_pos)
        raw_stats = await r.get("gridbot:ct_copy_stats")
        if raw_stats:
            CT_STATE["copy_stats"] = json.loads(raw_stats)
        if raw_pos or raw_stats:
            debug_log("✅ Copy-Trading-Positionen/Statistik aus Redis wiederhergestellt")
    except Exception as e:
        debug_log("⚠️ Laden der Copy-Positionen fehlgeschlagen", {"error": str(e)})


CT_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8"><title>Copy-Trading Dashboard</title>
<style>
  :root { --bg:#060a18; --panel:#0e1526; --border:rgba(96,165,250,0.14); --accent:#3b82f6; --green:#22c55e; --red:#f0526b; --text:#e8ecf5; --dim:#7c8aa8; }
  * { box-sizing:border-box; }
  body { font-family:-apple-system,sans-serif; background:var(--bg); color:var(--text); margin:0; padding:24px; }
  h1 { font-size:20px; display:flex; align-items:center; gap:14px; }
  h1 a { font-size:13px; color:var(--accent); text-decoration:none; }
  h2 { font-size:13px; color:var(--dim); text-transform:uppercase; letter-spacing:0.05em; margin-top:28px; }
  .green{color:var(--green)!important;} .red{color:var(--red)!important;} .yellow{color:#fbbf24!important;}
  table { width:100%; border-collapse:collapse; font-size:13px; margin-top:10px; background:var(--panel); border:1px solid var(--border); border-radius:14px; overflow:hidden; }
  th,td { text-align:left; padding:9px 12px; border-bottom:1px solid var(--border); }
  th { color:var(--dim); font-size:11px; text-transform:uppercase; }
  th.sortable { cursor:pointer; user-select:none; }
  th.sortable:hover { color:var(--text); }
  input[type=text] { padding:8px 10px; background:#080d1c; border:1px solid var(--border); border-radius:8px; color:var(--text); }
  input.new-address { width:340px; }
  input.coin-filter { width:130px; font-size:12px; }
  button { padding:7px 14px; background:linear-gradient(135deg,var(--accent),#2563eb); color:#fff; border:none; border-radius:8px; cursor:pointer; font-weight:700; font-size:12px; }
  button.copy-on { background:linear-gradient(135deg,#22c55e,#15803d); }
  button.copy-off { background:linear-gradient(135deg,#475569,#334155); }
  button.monitor-on { background:linear-gradient(135deg,#3b82f6,#1d4ed8); }
  button.monitor-off { background:linear-gradient(135deg,#475569,#334155); }
  .warn { background:rgba(240,82,107,0.12); border:1px solid rgba(240,82,107,0.35); color:#fca5b1; padding:10px 14px; border-radius:10px; font-size:13px; margin-top:10px; }
  .addr { font-family:monospace; font-size:12px; color:var(--dim); }
  .fill-buy { color:var(--green); } .fill-sell { color:var(--red); }
  .bar-wrap { background:#080d1c; border-radius:6px; overflow:hidden; height:18px; display:flex; min-width:120px; }
  .bar-long { background:var(--green); height:100%; } .bar-short { background:var(--red); height:100%; }
  .action-neu { color:#93c5fd; } .action-nachkauf { color:#fbbf24; } .action-reverse { color:#f0526b; }
</style>
</head>
<body>
<h1>📡 Copy-Trading <span id="mode-badge"></span> <a href="/">← zurück zum Grid-Bot</a></h1>
<div id="leaderboard-error"></div>

<h2>Trendmeter (Top 20 Coins - Long/Short-Verteilung der beobachteten Trader)</h2>
<table id="trend-table">
  <thead><tr><th>Coin</th><th>Long</th><th>Short</th><th>Verteilung</th></tr></thead>
  <tbody></tbody>
</table>

<h2>Manuelle Wallet hinzufügen</h2>
<div>
  <input type="text" class="new-address" id="new-address" placeholder="0x... Hyperliquid Wallet-Adresse">
  <button id="btn-add-address">Hinzufügen</button>
</div>

<h2>Beobachtete Trader - Positionen im Detail</h2>
<table id="watch-table">
  <thead><tr>
    <th class="sortable" data-key="label">Label ⇅</th>
    <th class="sortable" data-key="addr">Adresse ⇅</th>
    <th class="sortable" data-key="coin">Coin ⇅</th>
    <th class="sortable" data-key="side">Seite ⇅</th>
    <th class="sortable" data-key="size">Größe ⇅</th>
    <th class="sortable" data-key="entry_price">Ø-Einstieg ⇅</th>
    <th class="sortable" data-key="pnl">PnL ⇅</th>
    <th class="sortable" data-key="opened_at">Eröffnet ⇅</th>
    <th class="sortable" data-key="last_action">Aktion ⇅</th>
  </tr></thead>
  <tbody></tbody>
</table>

<h2>Beobachtete Trader (anklicken für Details)</h2>
<div style="color:var(--dim); font-size:12px; margin-top:6px;">Nur Trader mit "Beobachten AN" oder "Copy AN" werden laufend abgefragt (userState/userFills) - alle anderen stehen nur mit ihrer Leaderboard-PnL in der Liste, ohne staendige Anfragen an Hyperliquid.</div>
<table id="copy-table">
  <thead><tr><th>Label</th><th>Adresse</th><th>Leaderboard-PnL</th><th>Unser Copy-PnL</th><th>Offene Positionen</th><th>Verhalten (Neu/Nachkauf/Reverse)</th><th>Konfigurierte Coins</th><th>Beobachten</th><th>Copy</th><th>Copy-Filter</th></tr></thead>
  <tbody></tbody>
</table>

<h2>📋 Copy-Trade-Log <span id="copy-log-mode" style="font-size:13px; font-weight:normal;"></span></h2>
<div id="copy-log-empty" style="color:var(--dim); font-size:13px; display:none;">Noch keine Copy-Versuche - entweder wurde noch kein neuer Fill bei einem beobachteten Trader erkannt, oder für den Coin ist keine Einstellung hinterlegt (siehe Trader-Details).</div>
<table id="copy-log-table" style="display:none;">
  <thead><tr><th>Zeit</th><th>Trader</th><th>Coin</th><th>Richtung</th><th>Preis</th><th>Margin/Hebel</th><th>Status</th><th>PnL $</th></tr></thead>
  <tbody></tbody>
</table>

<!-- Detail-Modal: zeigt nur die Trades/Einstellungen EINES Traders -->
<div id="trader-modal" style="display:none; position:fixed; top:0; left:0; right:0; bottom:0; background:rgba(0,0,0,0.7); z-index:100; overflow-y:auto;">
  <div style="max-width:900px; margin:40px auto; background:var(--panel); border:1px solid var(--border); border-radius:16px; padding:24px;">
    <div style="display:flex; justify-content:space-between; align-items:center;">
      <h2 id="modal-title" style="margin:0;">Trader-Details</h2>
      <button onclick="closeModal()" style="background:#374151;">✖️ Schließen</button>
    </div>

    <h2>Standard-Margin/Hebel für diesen Trader</h2>
    <div>
      <input type="text" id="modal-default-margin" placeholder="Margin $" style="width:100px;">
      <input type="text" id="modal-default-leverage" placeholder="Hebel" style="width:80px;">
      <button onclick="saveTraderDefaults()">💾 Speichern</button>
      <span style="font-size:12px; color:var(--dim);"> (gilt für Coins ohne eigene Einstellung unten)</span>
    </div>

    <h2>Coins zum Kopieren freischalten</h2>
    <table>
      <thead><tr><th>Coin</th><th>Margin $ (leer = Standard)</th><th>Hebel (leer = Standard)</th><th>Aktiv</th><th></th></tr></thead>
      <tbody id="modal-coin-settings"></tbody>
    </table>
    <div style="margin-top:10px;">
      <input type="text" id="modal-new-coin" placeholder="z.B. BTC" style="width:100px;">
      <input type="text" id="modal-new-margin" placeholder="Margin $" style="width:100px;">
      <input type="text" id="modal-new-leverage" placeholder="Hebel" style="width:80px;">
      <button onclick="addCoinSetting()">+ Coin hinzufügen</button>
    </div>

    <h2>Offene Positionen dieses Traders</h2>
    <table>
      <thead><tr><th>Coin</th><th>Seite</th><th>Größe</th><th>Ø-Einstieg</th><th>PnL</th><th>Eröffnet</th><th>Aktion</th></tr></thead>
      <tbody id="modal-positions"></tbody>
    </table>

    <h2>Letzte Fills dieses Traders</h2>
    <table>
      <thead><tr><th>Zeit</th><th>Seite</th><th>Coin</th><th>Preis</th></tr></thead>
      <tbody id="modal-fills"></tbody>
    </table>
  </div>
</div>

<script>
function fmtTime(iso) {
  return iso ? new Date(iso).toLocaleString('de-DE', {day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'}) : '-';
}

let sortKey = null;
let sortAsc = true;

function renderPosTable() {
  let rows = [...(window.posData || [])];
  if (sortKey) {
    rows.sort((a, b) => {
      let av = a[sortKey], bv = b[sortKey];
      if (typeof av === 'string') av = av.toLowerCase();
      if (typeof bv === 'string') bv = bv.toLowerCase();
      if (av === null || av === undefined) av = '';
      if (bv === null || bv === undefined) bv = '';
      if (av < bv) return sortAsc ? -1 : 1;
      if (av > bv) return sortAsc ? 1 : -1;
      return 0;
    });
  }
  document.querySelector('#watch-table tbody').innerHTML = rows.map(r => `
    <tr>
      <td>${r.label}</td>
      <td class="addr">${r.addr_short}</td>
      <td><b>${r.coin}</b></td>
      <td class="${r.side==='long'?'green':'red'}">${r.side==='long'?'🟢 LONG':'🔴 SHORT'}</td>
      <td>${r.size}</td>
      <td>${r.entry_price ?? '-'}</td>
      <td class="${r.pnl>=0?'green':'red'}">$${r.pnl.toFixed(2)}</td>
      <td>${fmtTime(r.opened_at)}</td>
      <td class="${r.actionClass}">${r.last_action || '-'}${r.entries>1?' ('+r.entries+'x)':''}</td>
    </tr>`).join('') || '<tr><td colspan="9">Noch keine offenen Positionen erfasst...</td></tr>';
}

document.querySelectorAll('#watch-table th.sortable').forEach(th => {
  th.addEventListener('click', () => {
    const key = th.dataset.key;
    if (sortKey === key) { sortAsc = !sortAsc; } else { sortKey = key; sortAsc = true; }
    renderPosTable();
  });
});

async function refresh() {
  const res = await fetch('/api/ct/status');
  const data = await res.json();

  document.getElementById('mode-badge').innerHTML = data.dry_run
    ? '<span style="background:rgba(99,102,241,.18);color:#a5b4fc;padding:4px 12px;border-radius:20px;font-size:12px;">DRY RUN</span>'
    : '<span style="background:rgba(240,82,107,.15);color:#fca5b1;padding:4px 12px;border-radius:20px;font-size:12px;">LIVE</span>';

  document.getElementById('leaderboard-error').innerHTML = data.leaderboard_error
    ? `<div class="warn">⚠️ Leaderboard-Fehler: ${data.leaderboard_error} - manuelle Adressen funktionieren trotzdem.</div>` : '';

  // Trendmeter
  document.querySelector('#trend-table tbody').innerHTML = (data.trend_meter || []).map(t => {
    const pct = t.pct_long;
    const barHtml = pct === null ? '<span style="color:var(--dim);">keine Daten</span>' :
      `<div class="bar-wrap"><div class="bar-long" style="width:${pct}%"></div><div class="bar-short" style="width:${100-pct}%"></div></div> ${pct}% long`;
    return `<tr><td><b>${t.coin}</b></td><td class="green">${t.long}</td><td class="red">${t.short}</td><td>${barHtml}</td></tr>`;
  }).join('');

  // Positions-Detailtabelle (eine Zeile pro Position)
  let posData = [];
  Object.entries(data.watched).forEach(([addr, info]) => {
    const meta = info.position_meta || {};
    (info.positions || []).forEach(p => {
      const m = meta[p.coin] || {};
      const pnl = parseFloat(p.unrealized_pnl || 0);
      const actionClass = m.last_action === 'Neu' ? 'action-neu' : m.last_action === 'Nachkauf' ? 'action-nachkauf' : m.last_action === 'Reverse' ? 'action-reverse' : '';
      posData.push({
        label: info.label, addr_short: addr.slice(0,8) + '...' + addr.slice(-4),
        coin: p.coin, side: p.side, size: parseFloat(p.size || 0), entry_price: p.entry_price,
        pnl, opened_at: m.opened_at || '', last_action: m.last_action || '', entries: m.entries || 0, actionClass,
      });
    });
  });
  window.posData = posData;
  renderPosTable();

  window.watchedData = data.watched;  // fuer's Modal zwischenspeichern
  const copyStats = data.copy_stats || {};
  const copyPositions = data.copy_positions || {};

  // Trader-Übersicht (klickbar)
  const copyRows = Object.entries(data.watched).map(([addr, info]) => {
    const posSummary = (info.positions || []).map(p => `${p.side==='long'?'🟢':'🔴'} ${p.coin}`).join(', ') || '-';
    const coinCount = Object.keys(info.coin_settings || {}).length;
    const pnl = info.leaderboard_pnl;
    const pnlHtml = (pnl === null || pnl === undefined || pnl === '') ? '-' :
      `<span class="${parseFloat(pnl) >= 0 ? 'green' : 'red'}">${parseFloat(pnl).toLocaleString('de-DE', {maximumFractionDigits:0})}$</span>`;
    const stats = info.behavior_stats || {neu:0, nachkauf:0, reverse:0};
    const statsTotal = stats.neu + stats.nachkauf + stats.reverse;
    const nachkaufQuote = statsTotal > 0 ? Math.round(stats.nachkauf / statsTotal * 100) : null;
    const statsHtml = statsTotal === 0 ? '<span style="color:var(--dim);">noch keine Daten</span>' :
      `${stats.neu} Neu / ${stats.nachkauf} Nachkauf / ${stats.reverse} Reverse${nachkaufQuote !== null ? ` <span style="color:var(--dim);">(${nachkaufQuote}% Nachkauf-Quote)</span>` : ''}`;
    const cs = copyStats[addr];
    const openCount = Object.keys(copyPositions[addr] || {}).length;
    const copyPnlHtml = !cs || cs.trades === 0
      ? '<span style="color:var(--dim);">noch keine geschlossenen Copy-Trades</span>'
      : `<span class="${cs.total_pnl_usd >= 0 ? 'green' : 'red'}">${cs.total_pnl_usd}$</span> <span style="color:var(--dim); font-size:11px;">(${cs.trades} Trades, ${Math.round(cs.wins/cs.trades*100)}% Trefferquote${openCount>0?`, ${openCount} offen`:''})</span>`;
    return `
      <tr>
        <td style="cursor:pointer; color:var(--accent);" onclick="openModal('${addr}')">${info.label} 🔍</td>
        <td class="addr">${addr.slice(0,10)}...${addr.slice(-6)}</td>
        <td>${pnlHtml}</td>
        <td style="font-size:12px;">${copyPnlHtml}</td>
        <td>${posSummary}</td>
        <td style="font-size:12px;">${statsHtml}</td>
        <td>${coinCount} Coin${coinCount===1?'':'s'} konfiguriert</td>
        <td><button class="${info.monitor_enabled?'monitor-on':'monitor-off'}" onclick="toggleMonitor('${addr}', ${!info.monitor_enabled})">${info.monitor_enabled?'Beobachten AN':'Beobachten AUS'}</button></td>
        <td><button class="${info.copy_enabled?'copy-on':'copy-off'}" onclick="toggleCopy('${addr}', ${!info.copy_enabled})">${info.copy_enabled?'Copy AN':'Copy AUS'}</button></td>
        <td><button class="${info.copy_skip_nachkauf?'copy-on':'copy-off'}" onclick="toggleSkipNachkauf('${addr}', ${!info.copy_skip_nachkauf})" title="Bei An werden nur frische Einstiege (Neu) und Richtungswechsel (Reverse) kopiert, Nachkäufe (DCA) werden übersprungen">${info.copy_skip_nachkauf?'Nur Neu/Reverse':'Alles kopieren'}</button></td>
      </tr>`;
  }).join('');
  document.querySelector('#copy-table tbody').innerHTML = copyRows || '<tr><td colspan="10">Noch keine Trader beobachtet...</td></tr>';

  // Copy-Trade-Log: jeder Versuch, egal ob simuliert (DRY_RUN), erfolgreich oder fehlgeschlagen
  document.getElementById('copy-log-mode').innerText = data.dry_run ? '(DRY_RUN - hier siehst du, was simuliert würde)' : '(LIVE)';
  const copyLog = data.copy_log || [];
  if (copyLog.length === 0) {
    document.getElementById('copy-log-empty').style.display = '';
    document.getElementById('copy-log-table').style.display = 'none';
  } else {
    document.getElementById('copy-log-empty').style.display = 'none';
    document.getElementById('copy-log-table').style.display = '';
    const statusLabel = {dry_run: '🧪 simuliert', success: '✅ erfolgreich', error: '❌ Fehler', skipped: '⏭️ übersprungen', closed: '🏁 geschlossen'};
    document.querySelector('#copy-log-table tbody').innerHTML = copyLog.map(e => `
      <tr>
        <td>${new Date(e.ts).toLocaleString('de-DE')}</td>
        <td>${e.trader_label}</td>
        <td>${e.coin}</td>
        <td class="${e.direction==='long'?'green':'red'}">${e.direction==='long'?'🟢 Long':'🔴 Short'}</td>
        <td>${e.price}</td>
        <td>${e.margin != null ? `${e.margin}$ / ${e.leverage}x` : '-'}</td>
        <td title="${e.detail || e.action || ''}">${statusLabel[e.status] || e.status}${e.action ? ` (${e.action})` : ''}</td>
        <td class="${e.pnl_usd == null ? '' : (e.pnl_usd >= 0 ? 'green' : 'red')}">${e.pnl_usd != null ? e.pnl_usd : '-'}</td>
      </tr>`).join('');
  }

  // Falls das Modal gerade offen ist, dessen Inhalt mit aktualisieren
  if (window.currentModalAddress) renderModal(window.currentModalAddress);
}

let currentModalAddress = null;

function openModal(address) {
  window.currentModalAddress = address;
  document.getElementById('trader-modal').style.display = 'block';
  renderModal(address);
}

function closeModal() {
  window.currentModalAddress = null;
  document.getElementById('trader-modal').style.display = 'none';
}

function renderModal(address) {
  const info = (window.watchedData || {})[address];
  if (!info) return;

  document.getElementById('modal-title').innerText = `${info.label} (${address.slice(0,10)}...${address.slice(-6)})`;
  document.getElementById('modal-default-margin').value = info.copy_margin ?? '';
  document.getElementById('modal-default-leverage').value = info.copy_leverage ?? '';

  const coinSettings = info.coin_settings || {};
  document.getElementById('modal-coin-settings').innerHTML = Object.entries(coinSettings).map(([coin, cfg]) => `
    <tr>
      <td><b>${coin}</b></td>
      <td><input type="text" style="width:80px;" id="cs-margin-${coin}" value="${cfg.margin ?? ''}"></td>
      <td><input type="text" style="width:60px;" id="cs-lev-${coin}" value="${cfg.leverage ?? ''}"></td>
      <td><input type="checkbox" id="cs-enabled-${coin}" ${cfg.enabled ? 'checked' : ''}></td>
      <td>
        <button onclick="saveCoinSetting('${address}','${coin}')">💾</button>
        <button onclick="removeCoinSetting('${address}','${coin}')" style="background:#7f1d1d;">🗑️</button>
      </td>
    </tr>`).join('') || '<tr><td colspan="5" style="color:var(--dim);">Noch keine Coins freigeschaltet - unten hinzufügen</td></tr>';

  const meta = info.position_meta || {};
  document.getElementById('modal-positions').innerHTML = (info.positions || []).map(p => {
    const m = meta[p.coin] || {};
    const pnl = parseFloat(p.unrealized_pnl || 0);
    return `<tr>
      <td><b>${p.coin}</b></td>
      <td class="${p.side==='long'?'green':'red'}">${p.side==='long'?'🟢 LONG':'🔴 SHORT'}</td>
      <td>${p.size}</td><td>${p.entry_price ?? '-'}</td>
      <td class="${pnl>=0?'green':'red'}">$${pnl.toFixed(2)}</td>
      <td>${fmtTime(m.opened_at)}</td><td>${m.last_action ?? '-'}</td>
    </tr>`;
  }).join('') || '<tr><td colspan="7" style="color:var(--dim);">Keine offenen Positionen</td></tr>';

  document.getElementById('modal-fills').innerHTML = (info.recent_fills || []).slice(-20).reverse().map(f => `
    <tr>
      <td>${fmtTime(new Date(f.time).toISOString())}</td>
      <td class="${f.side==='B'?'fill-buy':'fill-sell'}">${f.side==='B'?'BUY':'SELL'}</td>
      <td>${f.coin}</td><td>${f.px}</td>
    </tr>`).join('') || '<tr><td colspan="4" style="color:var(--dim);">Noch keine Fills erfasst</td></tr>';
}

// Deutsche Kommaschreibweise (z.B. "10,5") in Punkt umwandeln, bevor der Wert an den
// Server geht - Python's float() kann mit Komma nichts anfangen und wuerde sonst mit
// einem Fehler abbrechen, der bisher NICHT im Dashboard angezeigt wurde (stilles
// Scheitern -> Coin-Margin wird nie gespeichert -> faellt auf die Trader-Standard-Margin
// zurueck, obwohl eine eigene Margin eingestellt wurde).
function deNum(v) {
  return typeof v === 'string' ? v.replace(',', '.') : v;
}

async function postCtJson(url, body) {
  const resp = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  let data = null;
  try { data = await resp.json(); } catch (e) {}
  if (!resp.ok || (data && data.error)) {
    alert('Fehler beim Speichern: ' + (data && data.error ? data.error : `HTTP ${resp.status}`));
    return false;
  }
  return true;
}

async function saveTraderDefaults() {
  const address = window.currentModalAddress;
  await postCtJson('/api/ct/trader_defaults', {
    address,
    copy_margin: deNum(document.getElementById('modal-default-margin').value),
    copy_leverage: deNum(document.getElementById('modal-default-leverage').value),
  });
  refresh();
}

async function saveCoinSetting(address, coin) {
  const ok = await postCtJson('/api/ct/coin_setting', {
    address, coin,
    margin: deNum(document.getElementById(`cs-margin-${coin}`).value),
    leverage: deNum(document.getElementById(`cs-lev-${coin}`).value),
    enabled: document.getElementById(`cs-enabled-${coin}`).checked,
  });
  if (ok) refresh();
}

async function removeCoinSetting(address, coin) {
  await fetch('/api/ct/remove_coin_setting', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({address, coin}) });
  refresh();
}

async function addCoinSetting() {
  const address = window.currentModalAddress;
  const coin = document.getElementById('modal-new-coin').value.trim().toUpperCase();
  if (!coin) return;
  const ok = await postCtJson('/api/ct/coin_setting', {
    address, coin,
    margin: deNum(document.getElementById('modal-new-margin').value),
    leverage: deNum(document.getElementById('modal-new-leverage').value),
    enabled: true,
  });
  if (ok) {
    document.getElementById('modal-new-coin').value = '';
    document.getElementById('modal-new-margin').value = '';
    document.getElementById('modal-new-leverage').value = '';
    refresh();
  }
}

async function toggleCopy(address, enable) {
  await fetch('/api/ct/copy', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({address, enable}) });
  refresh();
}

async function toggleMonitor(address, enable) {
  await fetch('/api/ct/monitor', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({address, enable}) });
  refresh();
}

async function toggleSkipNachkauf(address, enable) {
  await fetch('/api/ct/skip_nachkauf', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({address, enable}) });
  refresh();
}

document.getElementById('btn-add-address').addEventListener('click', async () => {
  const addr = document.getElementById('new-address').value.trim();
  if (!addr) return;
  await fetch('/api/ct/watch', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({address: addr}) });
  document.getElementById('new-address').value = '';
  refresh();
});

refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>
"""


async def handle_ct_index(request):
    return web.Response(text=CT_DASHBOARD_HTML, content_type="text/html")


TREND_METER_COINS = ["BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "ADA", "AVAX", "LINK", "DOT",
                      "TON", "SUI", "LTC", "TRX", "HYPE", "NEAR", "APT", "UNI", "ICP", "BCH"]


def compute_trend_meter():
    tally = {c: {"long": 0, "short": 0} for c in TREND_METER_COINS}
    for info in CT_STATE["watched"].values():
        for p in info.get("positions", []):
            coin = p.get("coin")
            side = p.get("side")
            if coin in tally and side in ("long", "short"):
                tally[coin][side] += 1

    result = []
    for c in TREND_METER_COINS:
        l, s = tally[c]["long"], tally[c]["short"]
        total = l + s
        pct_long = round(l / total * 100, 1) if total else None
        result.append({"coin": c, "long": l, "short": s, "pct_long": pct_long})
    return result


async def handle_ct_status(request):
    return web.json_response({
        "dry_run": CT_CONFIG["dry_run"],
        "leaderboard_last_fetch": CT_STATE["leaderboard_last_fetch"],
        "leaderboard_error": CT_STATE["leaderboard_error"],
        "watched": CT_STATE["watched"],
        "trend_meter": compute_trend_meter(),
        "copy_log": CT_STATE["copy_log"][:100],
        "copy_positions": CT_STATE["copy_positions"],
        "copy_stats": CT_STATE["copy_stats"],
    })


async def handle_ct_watch(request):
    body = await request.json()
    addr = body.get("address", "").strip()
    if not addr:
        return web.json_response({"error": "keine Adresse"}, status=400)
    if addr not in CT_STATE["watched"]:
        CT_STATE["watched"][addr] = {
            "label": "Manuell hinzugefügt", "copy_enabled": False, "monitor_enabled": True,
            "copy_skip_nachkauf": False,
            "coin_settings": {}, "copy_margin": CT_CONFIG["copy_margin"], "copy_leverage": CT_CONFIG["copy_leverage"],
            "last_fill_time": None, "positions": [], "recent_fills": [], "source": "manual",
            "position_meta": {}, "leaderboard_pnl": None,
            "behavior_stats": {"neu": 0, "nachkauf": 0, "reverse": 0},
        }
        await save_ct_watched()
    return web.json_response({"success": True})


async def handle_ct_copy_toggle(request):
    body = await request.json()
    addr = body.get("address")
    enable = bool(body.get("enable"))
    if addr in CT_STATE["watched"]:
        CT_STATE["watched"][addr]["copy_enabled"] = enable
        debug_log(f"{'✅' if enable else '⏸️'} [CopyTrading] Copy für {addr} {'aktiviert' if enable else 'deaktiviert'}")
        await save_ct_watched()
    return web.json_response({"success": True})


async def handle_ct_monitor_toggle(request):
    """Schaltet NUR die Beobachtung an/aus (userState/userFills-Abfragen), unabhaengig vom
    Copy-Schalter - damit man einen Trader analysieren kann, ohne ihm gleich zu folgen."""
    body = await request.json()
    addr = body.get("address")
    enable = bool(body.get("enable"))
    if addr in CT_STATE["watched"]:
        CT_STATE["watched"][addr]["monitor_enabled"] = enable
        debug_log(f"{'👁️' if enable else '🚫'} [CopyTrading] Beobachtung für {addr} {'aktiviert' if enable else 'deaktiviert'}")
        await save_ct_watched()
    return web.json_response({"success": True})


async def handle_ct_skip_nachkauf_toggle(request):
    """Schaltet 'Nur Neu/Reverse kopieren' an/aus - bei An werden Fills, die als reiner
    Nachkauf klassifiziert wurden (gleiche Richtung wie die schon offene Position bei diesem
    Trader auf diesem Coin), NICHT mitkopiert, nur frische Einstiege und Richtungswechsel."""
    body = await request.json()
    addr = body.get("address")
    enable = bool(body.get("enable"))
    if addr in CT_STATE["watched"]:
        CT_STATE["watched"][addr]["copy_skip_nachkauf"] = enable
        debug_log(f"{'🎯' if enable else '➕'} [CopyTrading] 'Nur Neu/Reverse' für {addr} {'aktiviert' if enable else 'deaktiviert'}")
        await save_ct_watched()
    return web.json_response({"success": True})


async def handle_ct_set_coin_setting(request):
    """Setzt/aktualisiert Margin, Hebel und An/Aus fuer EINEN Coin bei EINEM Trader."""
    body = await request.json()
    addr = body.get("address")
    coin = (body.get("coin") or "").strip().upper()
    if addr not in CT_STATE["watched"] or not coin:
        return web.json_response({"error": "ungültige Adresse oder Coin"}, status=400)

    info = CT_STATE["watched"][addr]
    settings = info.setdefault("coin_settings", {})
    try:
        margin_val = float(body["margin"]) if body.get("margin") not in (None, "") else None
        leverage_val = int(float(body["leverage"])) if body.get("leverage") not in (None, "") else None
    except (TypeError, ValueError):
        return web.json_response({"error": "Margin/Hebel müssen Zahlen sein (Komma oder Punkt als Dezimaltrennzeichen)"}, status=400)
    settings[coin] = {
        "enabled": bool(body.get("enabled", True)),
        "margin": margin_val,
        "leverage": leverage_val,
    }
    debug_log(f"⚙️ [CopyTrading] Coin-Einstellung für {addr} / {coin} gesetzt", settings[coin])
    await save_ct_watched()
    return web.json_response({"success": True, "coin_settings": settings})


async def handle_ct_remove_coin_setting(request):
    body = await request.json()
    addr = body.get("address")
    coin = (body.get("coin") or "").strip().upper()
    if addr in CT_STATE["watched"]:
        CT_STATE["watched"][addr].get("coin_settings", {}).pop(coin, None)
        await save_ct_watched()
    return web.json_response({"success": True})


async def handle_ct_set_trader_defaults(request):
    """Setzt Standard-Margin/Hebel fuer einen Trader (greift fuer Coins ohne eigene Einstellung)."""
    body = await request.json()
    addr = body.get("address")
    if addr not in CT_STATE["watched"]:
        return web.json_response({"error": "unbekannte Adresse"}, status=400)
    info = CT_STATE["watched"][addr]
    try:
        if body.get("copy_margin") not in (None, ""):
            info["copy_margin"] = float(body["copy_margin"])
        if body.get("copy_leverage") not in (None, ""):
            info["copy_leverage"] = int(float(body["copy_leverage"]))
    except (TypeError, ValueError):
        return web.json_response({"error": "Margin/Hebel müssen Zahlen sein (Komma oder Punkt als Dezimaltrennzeichen)"}, status=400)
    await save_ct_watched()
    return web.json_response({"success": True})


