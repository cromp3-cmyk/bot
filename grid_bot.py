"""
Multi-Coin neutraler Grid-Bot für Lighter (zkLighter) - MIT LIVE-DASHBOARD
=============================================================================
Laeuft mehrere Coins GLEICHZEITIG in einem einzigen Prozess (eine WebSocket-
Verbindung, mehrere Kanaele) - jeder Coin hat komplett eigene Einstellungen
und eigenen Zustand, kein zusaetzlicher Render-Service noetig.

Grid-Logik pro Coin (unabhaengig von den anderen):
1. Start: aktueller Preis = "Anker"
2. Preis bewegt sich GRID_STEP_PCT vom Anker weg -> Position eroeffnen
3. Position im Plus um TP_STEP_PCT (ab Ø-Einstieg) -> TP, optional sofort
   Gegenposition (auto_reverse)
4. Position im Minus um GRID_STEP_PCT (weitere Stufe) -> Nachkauf, bis MAX_NACHKAUF

WICHTIG - RENDER SERVICE-TYP: als "Web Service" laufen lassen (nicht Worker).
WICHTIG - SICHERHEIT: Erst DRY_RUN=true testen!
"""

import asyncio
import websockets
import aiohttp
import json
import time
import os
import traceback
from datetime import datetime
from aiohttp import web

BASE_URL = "https://mainnet.zklighter.elliot.ai"
WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"

DEBUG_MODE = os.getenv("DEBUG_MODE", "true").lower() == "true"


def debug_log(msg, data=None):
    if DEBUG_MODE:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        print(f"[DEBUG {timestamp}] {msg}", flush=True)
        if data:
            print(f"   DATA: {json.dumps(data, indent=2, default=str)}", flush=True)


# ========== MARKET / COIN CONFIG ==========
MARKET_INDICES = {
    "ETH": 0, "BTC": 1, "SOL": 2, "DOGE": 3, "XRP": 7, "LINK": 8, "AVAX": 9,
    "NEAR": 10, "DOT": 11, "TON": 12, "SUI": 16, "BNB": 25, "UNI": 30, "APT": 31,
    "ADA": 39, "TRX": 43, "LTC": 35, "BCH": 58, "HBAR": 59, "ICP": 102, "HYPE": 24,
    "EURUSD": 96, "GBPUSD": 97, "USDJPY": 98, "USDCHF": 99, "USDCAD": 100,
    "AUDUSD": 106, "NZDUSD": 107, "USDKRW": 105,
    "XAU": 92, "XAG": 93, "WTI": 145,
}
PRECISION_MAP = {
    "BTC": 100000, "ETH": 10000, "SOL": 1000, "LTC": 1000,
    "AVAX": 100, "BNB": 100, "UNI": 100, "APT": 100, "XAG": 100,
    "LINK": 10, "NEAR": 10, "DOT": 10, "SUI": 10, "ADA": 10, "EURUSD": 10, "GBPUSD": 10, "USDCHF": 10, "USDCAD": 10,
    "DOGE": 1, "XRP": 1, "TRX": 1,
    "USDJPY": 1000, "AUDUSD": 10, "NZDUSD": 10, "USDKRW": 10, "XAU": 10000,
    # BCH, HBAR, ICP, TON, WTI: keine explizite Angabe in der Quelle - Standardwert (10000) greift,
    # bitte vor dem Live-Handel dieser Coins/Rohstoffe unbedingt mit kleiner Größe testen!
}
PRICE_DECIMALS_MAP = {
    "BTC": 1, "ETH": 2, "SOL": 3, "LTC": 3, "XAU": 1,
    "AVAX": 3, "BNB": 4, "UNI": 4, "APT": 4,
    "LINK": 5, "NEAR": 5, "DOT": 5, "SUI": 5, "ADA": 5, "EURUSD": 5, "GBPUSD": 5, "USDCHF": 5, "USDCAD": 5,
    "DOGE": 6, "XRP": 6, "XAG": 6,
    "USDJPY": 3, "AUDUSD": 5, "NZDUSD": 5, "USDKRW": 5,
}
MIN_BASE_AMOUNT_MAP = {
    "BTC": 0.00020, "ETH": 0.005, "SOL": 0.05, "LTC": 0.1, "BCH": 0.01,
    "AVAX": 0.5, "BNB": 0.02, "UNI": 1.0, "APT": 2.0, "XAU": 0.003, "XAG": 0.15,
    "LINK": 1.0, "NEAR": 2.0, "DOT": 2.0, "SUI": 3.0, "ADA": 10.0,
    "DOGE": 10, "XRP": 20, "HBAR": 20.0,
    "EURUSD": 10.0, "GBPUSD": 10.0, "USDJPY": 0.05, "USDCHF": 8.0, "USDCAD": 10.0,
    "AUDUSD": 10.0, "NZDUSD": 10.0, "USDKRW": 10.0,
}


def get_precision(symbol):
    return PRECISION_MAP.get(symbol, 10000)


def get_price_decimals(symbol):
    return PRICE_DECIMALS_MAP.get(symbol, 2)


def get_min_base_amount(symbol):
    return MIN_BASE_AMOUNT_MAP.get(symbol, 0.001)


PORT = int(os.getenv("PORT", "10000"))

# ========== WELCHE COINS LAUFEN SOLLEN ==========
# Komma-getrennt, z.B. GRID_SYMBOLS="BTC,SOL,ETH". Default: nur BTC (abwaertskompatibel).
SYMBOLS = [s.strip().upper() for s in os.getenv("GRID_SYMBOLS", os.getenv("GRID_SYMBOL", "BTC")).split(",") if s.strip()]
for _s in SYMBOLS:
    if _s not in MARKET_INDICES:
        raise ValueError(f"Symbol {_s} nicht in MARKET_INDICES - hier ergänzen")

MARKET_INDEX_TO_SYMBOL = {MARKET_INDICES[s]: s for s in SYMBOLS}


def default_config():
    return {
        "dry_run": os.getenv("DRY_RUN", "true").lower() == "true",
        "margin": float(os.getenv("GRID_MARGIN", "20")),
        "leverage": int(os.getenv("GRID_LEVERAGE", "3")),
        "entry_mode": os.getenv("ENTRY_MODE", "grid"),  # "grid" oder "ha_st"
        "grid_mode": os.getenv("GRID_MODE", "pct"),  # "pct" oder "usd"
        "grid_step_pct": float(os.getenv("GRID_STEP_PCT", "0.25")),
        "tp_step_pct": float(os.getenv("TP_STEP_PCT", "0.25")),
        "grid_step_usd": float(os.getenv("GRID_STEP_USD", "150")),
        "tp_step_usd": float(os.getenv("TP_STEP_USD", "150")),
        "max_nachkauf": int(os.getenv("MAX_NACHKAUF", "5")),
        "bot_active": True,
        "auto_reverse": os.getenv("AUTO_REVERSE", "true").lower() == "true",
        "ha_st_resolution": os.getenv("HA_ST_RESOLUTION", "5m"),
        "ha_st_atr_period": int(os.getenv("HA_ST_ATR_PERIOD", "5")),
        "ha_st_atr_mult": float(os.getenv("HA_ST_ATR_MULT", "1.5")),
        "ha_st_trend_filter": os.getenv("HA_ST_TREND_FILTER", "true").lower() == "true",
        "ha_st_trend_ema_length": int(os.getenv("HA_ST_TREND_EMA_LENGTH", "200")),
        "cc_resolution_seconds": int(os.getenv("CC_RESOLUTION_SECONDS", "60")),
        "cc_confirm_delay_seconds": int(os.getenv("CC_CONFIRM_DELAY_SECONDS", "20")),
        "cc_auto_reverse": os.getenv("CC_AUTO_REVERSE", "true").lower() == "true",
        "cc_early_exit": os.getenv("CC_EARLY_EXIT", "true").lower() == "true",
    }


def default_state():
    return {
        "position": None, "avg_entry_price": None, "total_coin_size": 0.0,
        "entry_count": 0, "anchor_price": None, "last_price": None,
        "price_history": [],
        "ha_st_stop_price": None, "position_opened_at": None,
        "cc_candle_start": None, "cc_candle_open": None, "cc_entered_this_candle": False, "cc_last_color": None,
        "stats": {"trades": 0, "wins": 0, "losses": 0, "total_pnl_usd": 0.0},
        "trade_log": [],
    }


# ========== GLOBALER STATE - EIN EINTRAG PRO COIN ==========
BOTS = {s: {"config": default_config(), "state": default_state()} for s in SYMBOLS}


# ==========================================================================
# COPY-TRADING: Hyperliquid Top-Trader beobachten und optional auf Lighter kopieren
# ==========================================================================
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
}


async def execute_copy_trade(symbol, direction, reference_price):
    """Kopiert die RICHTUNG eines Trades mit EIGENER Margin/Hebel (keine Groessen-Kopie)."""
    if symbol not in MARKET_INDICES:
        debug_log(f"⚠️ [CopyTrading] Coin {symbol} nicht auf Lighter gemappt - übersprungen")
        return

    if CT_CONFIG["dry_run"]:
        debug_log(f"🧪 [CopyTrading] DRY_RUN - würde kopieren: {direction.upper()} {symbol} @ ~{reference_price}")
        return

    client = get_lighter_client()
    if client is None:
        return
    try:
        market_index = MARKET_INDICES[symbol]
        precision = get_precision(symbol)
        min_base = get_min_base_amount(symbol)
        position_usdc = CT_CONFIG["copy_margin"] * CT_CONFIG["copy_leverage"]
        coin_amount = position_usdc / reference_price
        base_amount = int(coin_amount * precision)
        if base_amount * (1 / precision) < min_base:
            debug_log(f"⚠️ [CopyTrading] Order-Größe für {symbol} unter Mindestgröße")
            return
        is_ask = direction == "short"
        try:
            await client.update_leverage(market_index=market_index, leverage=CT_CONFIG["copy_leverage"], margin_mode=0)
        except Exception as e:
            debug_log("[CopyTrading] Hebel setzen fehlgeschlagen", {"error": str(e)})
        tx, tx_hash, err = await place_market_order(client, market_index, symbol, is_ask, base_amount, reference_price)
        if err:
            debug_log(f"⚠️ [CopyTrading] Order fehlgeschlagen für {symbol}", {"error": str(err)})
        else:
            debug_log(f"✅ [CopyTrading] ECHTER Copy-Trade: {direction.upper()} {symbol} @ ~{reference_price}", {"tx_hash": str(tx_hash)})
    finally:
        await client.close()


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
                        CT_STATE["watched"][addr] = {
                            "label": f"#{i+1} (Leaderboard)", "copy_enabled": False, "copy_coins": [],
                            "last_fill_time": None, "positions": [], "recent_fills": [], "source": "leaderboard",
                            "position_meta": {},
                        }
            await asyncio.sleep(CT_CONFIG["leaderboard_refresh_minutes"] * 60)


async def ct_watch_loop():
    for addr in CT_MANUAL_ADDRESSES:
        if addr not in CT_STATE["watched"]:
            CT_STATE["watched"][addr] = {
                "label": "Manuell hinzugefügt", "copy_enabled": False, "copy_coins": [],
                "last_fill_time": None, "positions": [], "recent_fills": [], "source": "manual",
                "position_meta": {},
            }

    async with aiohttp.ClientSession() as session:
        while True:
            for address, info in list(CT_STATE["watched"].items()):
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
                        for f in new_fills:
                            info["last_fill_time"] = f["time"]
                            coin = f.get("coin")
                            side = f.get("side")
                            direction = "long" if side == "B" else "short"
                            price = float(f.get("px", 0) or 0)

                            prev_meta = meta.get(coin)
                            now_iso = datetime.now().isoformat()
                            if prev_meta is None:
                                meta[coin] = {"opened_at": now_iso, "direction": direction, "entries": 1, "last_action": "Neu"}
                                action = "Neu"
                            elif prev_meta["direction"] == direction:
                                prev_meta["entries"] += 1
                                prev_meta["last_action"] = "Nachkauf"
                                action = "Nachkauf"
                            else:
                                meta[coin] = {"opened_at": now_iso, "direction": direction, "entries": 1, "last_action": "Reverse"}
                                action = "Reverse"

                            debug_log(f"🆕 [CopyTrading] Neuer Fill bei {info['label']} ({address[:8]}...): {direction.upper()} {coin} @ {price} [{action}]")

                            allowed_coins = info.get("copy_coins") or []
                            coin_allowed = (not allowed_coins) or (coin in allowed_coins)
                            if info["copy_enabled"] and price > 0 and coin_allowed:
                                await execute_copy_trade(coin, direction, price)

                await asyncio.sleep(0.5)

            await asyncio.sleep(CT_CONFIG["poll_interval_seconds"])


# ========== LIGHTER CLIENT ==========
def get_lighter_client():
    try:
        import lighter
        API_KEY_INDEX = int(os.getenv("API_KEY_INDEX", "5"))
        PRIVATE_KEY = os.getenv("PRIVATE_KEY")
        ACCOUNT_INDEX = int(os.getenv("ACCOUNT_INDEX", "50960"))
        return lighter.SignerClient(
            url=BASE_URL,
            api_private_keys={API_KEY_INDEX: PRIVATE_KEY},
            account_index=ACCOUNT_INDEX
        )
    except Exception as e:
        debug_log("Lighter Client Fehler", {"error": str(e), "traceback": traceback.format_exc()})
        return None


async def place_market_order(client, market_index, symbol, is_ask, base_amount, reference_price, reduce_only=False):
    price_decimals = get_price_decimals(symbol)
    adjusted_price = reference_price * 0.98 if is_ask else reference_price * 1.02
    price_scaled = int(adjusted_price * (10 ** price_decimals))
    tx, tx_hash, err = await client.create_order(
        market_index=market_index, client_order_index=int(time.time() * 1000),
        base_amount=base_amount, price=price_scaled, is_ask=is_ask,
        order_type=client.ORDER_TYPE_MARKET,
        time_in_force=client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL, reduce_only=reduce_only,
        order_expiry=client.DEFAULT_IOC_EXPIRY,
    )
    return tx, tx_hash, err


def estimate_liquidation_price(symbol):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if st["position"] is None or st["avg_entry_price"] is None or cfg["leverage"] <= 0:
        return None
    factor = 1 / cfg["leverage"]
    if st["position"] == "long":
        return round(st["avg_entry_price"] * (1 - factor), 2)
    else:
        return round(st["avg_entry_price"] * (1 + factor), 2)


def calc_unrealized_pnl(symbol):
    st = BOTS[symbol]["state"]
    if st["position"] is None or st["avg_entry_price"] is None or st["last_price"] is None:
        return 0.0
    if st["position"] == "long":
        return round((st["last_price"] - st["avg_entry_price"]) * st["total_coin_size"], 4)
    else:
        return round((st["avg_entry_price"] - st["last_price"]) * st["total_coin_size"], 4)


async def fetch_candles_ohlc(symbol, resolution, count_back=150):
    """Kerzendaten inkl. Open (fuer Heikin Ashi noetig)."""
    import lighter
    configuration = lighter.Configuration(host=BASE_URL)
    async with lighter.ApiClient(configuration) as api_client:
        candle_api = lighter.CandlestickApi(api_client)
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - 60 * 60 * 24 * 7 * 1000
        response = await candle_api.candles(
            market_id=MARKET_INDICES[symbol], resolution=resolution,
            start_timestamp=start_ms, end_timestamp=now_ms,
            count_back=min(count_back, 500), set_timestamp_to_end=True,
        )
        candles = getattr(response, "c", None)
        if not candles:
            return None
        timestamps, opens, highs, lows, closes = [], [], [], [], []
        for candle in candles:
            t_ = getattr(candle, "t", None)
            o_ = getattr(candle, "o", None)
            h_ = getattr(candle, "h", None)
            l_ = getattr(candle, "l", None)
            c_ = getattr(candle, "c", None)
            if None in (t_, o_, h_, l_, c_):
                continue
            timestamps.append(int(t_)); opens.append(float(o_)); highs.append(float(h_))
            lows.append(float(l_)); closes.append(float(c_))
        return timestamps, opens, highs, lows, closes


def compute_ha_supertrend(opens, highs, lows, closes, period=5, multiplier=1.5):
    """Portiert aus dem Pine-Script: Heikin-Ashi-Kerzen + Custom-ATR-Baender/Trend."""
    n = len(closes)
    if n < period + 2:
        return None, None

    ha_close = [(opens[i] + highs[i] + lows[i] + closes[i]) / 4 for i in range(n)]
    ha_open = [0.0] * n
    ha_open[0] = (opens[0] + closes[0]) / 2
    for i in range(1, n):
        ha_open[i] = (ha_open[i - 1] + ha_close[i - 1]) / 2

    # ATR auf den ECHTEN Kerzen (wie im Pine-Script: ta.atr nutzt die realen high/low/close)
    tr = [highs[0] - lows[0]] + [0.0] * (n - 1)
    for i in range(1, n):
        tr[i] = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
    atr = [tr[0]] * n
    for i in range(1, n):
        if i < period:
            atr[i] = sum(tr[:i + 1]) / (i + 1)
        else:
            atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period

    up = [0.0] * n
    dn = [0.0] * n
    trend = [1] * n

    up[0] = ha_close[0] - multiplier * atr[0]
    dn[0] = ha_close[0] + multiplier * atr[0]
    last_up1 = up[0]
    last_dn1 = dn[0]

    for i in range(1, n):
        basic_up = ha_close[i] - multiplier * atr[i]
        basic_dn = ha_close[i] + multiplier * atr[i]

        if ha_close[i - 1] > up[i - 1]:
            last_up1 = up[i - 1]
        up1 = last_up1
        up[i] = max(basic_up, up1) if ha_close[i - 1] > up1 else basic_up

        if ha_close[i - 1] < dn[i - 1]:
            last_dn1 = dn[i - 1]
        dn1 = last_dn1
        dn[i] = min(basic_dn, dn1) if ha_close[i - 1] < dn1 else basic_dn

        prev_trend = trend[i - 1]
        if prev_trend == -1 and ha_close[i] > dn1:
            trend[i] = 1
        elif prev_trend == 1 and ha_close[i] < up1:
            trend[i] = -1
        else:
            trend[i] = prev_trend

    return trend, ha_close


async def ha_supertrend_poll_loop(symbol):
    """Reiner Buy/Sell-Wechsel-Bot auf Basis von Heikin-Ashi-Supertrend-Flips,
    SL an der High/Low der ausloesenden (echten) Kerze - kein TP."""
    b = BOTS[symbol]
    last_signal_ts = None

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "ha_st":
                needed_bars = max(150, cfg["ha_st_trend_ema_length"] + 10) if cfg["ha_st_trend_filter"] else 150
                data = await fetch_candles_ohlc(symbol, cfg["ha_st_resolution"], count_back=needed_bars)
                if data:
                    timestamps, opens, highs, lows, closes = data
                    closed_ts = timestamps[:-1]
                    closed_o, closed_h, closed_l, closed_c = opens[:-1], highs[:-1], lows[:-1], closes[:-1]
                    trend, ha_close = compute_ha_supertrend(closed_o, closed_h, closed_l, closed_c,
                                                              cfg["ha_st_atr_period"], cfg["ha_st_atr_mult"])
                    if trend and len(trend) >= 2:
                        st = b["state"]
                        signal_key = closed_ts[-1]

                        flipped_bullish = trend[-1] == 1 and trend[-2] == -1
                        flipped_bearish = trend[-1] == -1 and trend[-2] == 1

                        if (flipped_bullish or flipped_bearish) and last_signal_ts != signal_key:
                            last_signal_ts = signal_key
                            candle_age_seconds = round(time.time() - signal_key / 1000, 1)
                            if cfg["bot_active"]:
                                price = closed_c[-1]
                                new_direction = "long" if flipped_bullish else "short"
                                # SL an der auslösenden Kerze: unter ihrem Low (long) bzw. über ihrem High (short)
                                new_sl = closed_l[-1] if new_direction == "long" else closed_h[-1]

                                # Trendfilter: nur Longs im Aufwärtstrend (Preis > lange EMA), nur Shorts im Abwärtstrend
                                trend_ok = True
                                ema_trend_val = None
                                if cfg["ha_st_trend_filter"] and len(closed_c) > cfg["ha_st_trend_ema_length"]:
                                    ema_trend_val = round(_ema_series(closed_c, cfg["ha_st_trend_ema_length"])[-1], 4)
                                    if new_direction == "long" and price <= ema_trend_val:
                                        trend_ok = False
                                    elif new_direction == "short" and price >= ema_trend_val:
                                        trend_ok = False

                                debug_log(f"📡 [{symbol}] HA-Supertrend-Flip: {new_direction.upper()} @ {price} | SL {new_sl} | Trendfilter {'OK' if trend_ok else 'BLOCKIERT'}", {
                                    "kerze_alter_sekunden": candle_age_seconds,
                                    "ema_trend": ema_trend_val,
                                })

                                if st["position"] is not None and st["position"] != new_direction:
                                    await execute_exit(symbol, price, "HA-REVERSE")
                                    if trend_ok:
                                        await execute_entry(symbol, new_direction, price, is_add_on=False)
                                        st["ha_st_stop_price"] = new_sl
                                elif st["position"] is None and trend_ok:
                                    await execute_entry(symbol, new_direction, price, is_add_on=False)
                                    st["ha_st_stop_price"] = new_sl
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] HA-Supertrend-Abfrage fehlgeschlagen", {"error": str(e)})

        await asyncio.sleep(5)


def _ema_series(values, length):
    if not values:
        return []
    k = 2 / (length + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def compute_step_abs(reference_price, cfg, which):
    """which: 'grid' oder 'tp' - liefert den Abstand in Preiseinheiten, je nach grid_mode."""
    if cfg["grid_mode"] == "usd":
        return cfg["grid_step_usd"] if which == "grid" else cfg["tp_step_usd"]
    pct = cfg["grid_step_pct"] if which == "grid" else cfg["tp_step_pct"]
    return reference_price * (pct / 100)


def calc_grid_levels(symbol):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    levels = {"anchor": st["anchor_price"], "tp_price": None, "next_nachkauf_price": None,
              "grid_step_abs": None, "tp_step_abs": None}
    if st["position"] is None:
        if st["anchor_price"] is not None:
            step = compute_step_abs(st["anchor_price"], cfg, "grid")
            levels["next_entry_long"] = round(st["anchor_price"] - step, 4)
            levels["next_entry_short"] = round(st["anchor_price"] + step, 4)
            levels["grid_step_abs"] = round(step, 4)
    elif st["avg_entry_price"] is not None:
        tp_step = compute_step_abs(st["avg_entry_price"], cfg, "tp")
        grid_step = compute_step_abs(st["avg_entry_price"], cfg, "grid")
        levels["tp_step_abs"] = round(tp_step, 4)
        levels["grid_step_abs"] = round(grid_step, 4)
        if st["position"] == "long":
            levels["tp_price"] = round(st["avg_entry_price"] + tp_step, 4)
            levels["next_nachkauf_price"] = round(st["avg_entry_price"] - grid_step, 4)
        else:
            levels["tp_price"] = round(st["avg_entry_price"] - tp_step, 4)
            levels["next_nachkauf_price"] = round(st["avg_entry_price"] + grid_step, 4)
    return levels


async def execute_entry(symbol, direction, price, is_add_on):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    market_index = MARKET_INDICES[symbol]

    position_usdc = cfg["margin"] * cfg["leverage"]
    raw_units = position_usdc / price
    precision = get_precision(symbol)
    base_amount = int(raw_units * precision)
    new_units = base_amount / precision

    if not cfg["dry_run"]:
        client = get_lighter_client()
        if client is None:
            debug_log(f"⚠️ [{symbol}] Kein Lighter-Client - Order übersprungen")
            return False
        min_base = get_min_base_amount(symbol)
        if base_amount * (1 / precision) < min_base:
            debug_log(f"⚠️ [{symbol}] Order-Größe unter Mindestgröße")
            return False
        is_ask = direction == "short"
        tx, tx_hash, err = await place_market_order(client, market_index, symbol, is_ask, base_amount, price, reduce_only=False)
        await client.close()
        if err:
            debug_log(f"⚠️ [{symbol}] Entry-Order fehlgeschlagen", {"error": str(err)})
            return False
        debug_log(f"✅ [{symbol}] ECHTE Order ausgeführt: {direction.upper()} @ ~{price}", {"tx_hash": str(tx_hash)})

    if is_add_on:
        total_value = st["avg_entry_price"] * st["total_coin_size"] + price * new_units
        st["total_coin_size"] += new_units
        st["avg_entry_price"] = total_value / st["total_coin_size"]
    else:
        st["avg_entry_price"] = price
        st["total_coin_size"] = new_units
        st["position"] = direction
        st["position_opened_at"] = datetime.now().isoformat()

    st["entry_count"] += 1
    debug_log(f"📈 [{symbol}] {'Nachkauf' if is_add_on else 'Neue Position'}: {direction.upper()} @ {price} | Ø-Einstieg {round(st['avg_entry_price'], 2)} | Stufe {st['entry_count']}")
    return True


async def execute_exit(symbol, price, reason):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    market_index = MARKET_INDICES[symbol]

    pnl_usd = (price - st["avg_entry_price"]) * st["total_coin_size"] if st["position"] == "long" else (st["avg_entry_price"] - price) * st["total_coin_size"]
    closing_side = st["position"]

    if not cfg["dry_run"]:
        client = get_lighter_client()
        if client is None:
            debug_log(f"⚠️ [{symbol}] Kein Lighter-Client - Exit übersprungen (Position bleibt offen!)")
            return
        precision = get_precision(symbol)
        base_amount = int(round(st["total_coin_size"] * precision))
        is_ask = st["position"] == "long"
        tx, tx_hash, err = await place_market_order(client, market_index, symbol, is_ask, base_amount, price, reduce_only=True)
        await client.close()
        if err:
            debug_log(f"⚠️ [{symbol}] Exit-Order fehlgeschlagen - Position bleibt offen!", {"error": str(err)})
            return

    stats = st["stats"]
    stats["trades"] += 1
    stats["total_pnl_usd"] += pnl_usd
    stats["wins" if pnl_usd > 0 else "losses"] += 1
    st["trade_log"].append({
        "side": st["position"], "avg_entry": round(st["avg_entry_price"], 2), "exit": price,
        "entries": st["entry_count"], "pnl_usd": round(pnl_usd, 3),
        "opened_at": st.get("position_opened_at"), "closed_at": datetime.now().isoformat(), "reason": reason,
    })

    debug_log(f"🏁 [{symbol}] Position geschlossen ({reason}): {st['position'].upper()} Ø{round(st['avg_entry_price'],2)} -> {price} | PnL ${round(pnl_usd,3)}")

    st["position"] = None
    st["avg_entry_price"] = None
    st["total_coin_size"] = 0.0
    st["entry_count"] = 0
    st["anchor_price"] = price
    st["ha_st_stop_price"] = None
    st["position_opened_at"] = None

    if cfg.get("auto_reverse", True) and cfg["bot_active"] and cfg["entry_mode"] == "grid":
        opposite = "short" if closing_side == "long" else "long"
        await execute_entry(symbol, opposite, price, is_add_on=False)


async def handle_candle_color_tick(symbol, price):
    """Reine Preis-Feed-Strategie, keine Kerzen-API noetig - Kerzengrenzen werden
    lokal aus der Uhrzeit berechnet. Frueher Einstieg nach X Sekunden Kerzenlaufzeit
    (aktuelle Farbe), Ausstieg nur bei tatsaechlich geschlossener Gegenfarben-Kerze."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]

    now = time.time()
    resolution = cfg["cc_resolution_seconds"]
    confirm_delay = cfg["cc_confirm_delay_seconds"]
    candle_start = int(now // resolution) * resolution

    if st["cc_candle_start"] is None:
        st["cc_candle_start"] = candle_start
        st["cc_candle_open"] = price
        st["cc_entered_this_candle"] = False
        return

    if candle_start != st["cc_candle_start"]:
        # Vorherige Kerze ist soeben geschlossen - ihre Endfarbe bestimmen
        prev_open = st["cc_candle_open"]
        prev_close = price  # letzter bekannter Preis vor der neuen Kerze
        closed_color = "green" if prev_close > prev_open else ("red" if prev_close < prev_open else st["cc_last_color"])
        st["cc_last_color"] = closed_color

        if st["position"] is not None:
            position_color = "green" if st["position"] == "long" else "red"
            if closed_color is not None and closed_color != position_color:
                await execute_exit(symbol, price, "CC-REVERSE")

        # Neue Kerze beginnt
        st["cc_candle_start"] = candle_start
        st["cc_candle_open"] = price
        st["cc_entered_this_candle"] = False

    if not cfg["bot_active"]:
        return

    candle_age = now - st["cc_candle_start"]

    # Fruehe Ausstiegs-Pruefung: kontinuierlich (jeden Tick), nicht nur einmal pro Kerze -
    # sobald die AKTUELL LAUFENDE Kerze nach confirm_delay Sekunden die Gegenfarbe zeigt,
    # wird sofort geschlossen, OHNE auf den Kerzenschluss zu warten.
    if cfg.get("cc_early_exit", True) and st["position"] is not None and candle_age >= confirm_delay:
        current_color = "green" if price > st["cc_candle_open"] else ("red" if price < st["cc_candle_open"] else None)
        position_color = "green" if st["position"] == "long" else "red"
        if current_color is not None and current_color != position_color:
            await execute_exit(symbol, price, "CC-EARLY-EXIT")
            st["cc_entered_this_candle"] = True
            if cfg.get("cc_auto_reverse", True):
                new_direction = "long" if current_color == "green" else "short"
                await execute_entry(symbol, new_direction, price, is_add_on=False)
            return

    if not st["cc_entered_this_candle"] and candle_age >= confirm_delay:
        st["cc_entered_this_candle"] = True
        current_color = "green" if price > st["cc_candle_open"] else ("red" if price < st["cc_candle_open"] else None)
        if current_color is None:
            return
        direction = "long" if current_color == "green" else "short"

        if st["position"] is None:
            await execute_entry(symbol, direction, price, is_add_on=False)
        elif st["position"] != direction and cfg.get("cc_auto_reverse", True):
            # Sicherheitsnetz, falls die fruehe Ausstiegs-Pruefung oben deaktiviert ist
            await execute_exit(symbol, price, "CC-REVERSE")
            await execute_entry(symbol, direction, price, is_add_on=False)


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

    if cfg["entry_mode"] == "candle_color":
        await handle_candle_color_tick(symbol, price)
        return

    if st["position"] is None:
        if not bot_active or cfg["entry_mode"] != "grid":
            return  # im HA-Supertrend-Modus übernimmt der Poll-Loop den Einstieg
        grid_step_abs = compute_step_abs(st["anchor_price"], cfg, "grid")
        if price <= st["anchor_price"] - grid_step_abs:
            await execute_entry(symbol, "long", price, is_add_on=False)
        elif price >= st["anchor_price"] + grid_step_abs:
            await execute_entry(symbol, "short", price, is_add_on=False)
        return

    if cfg["entry_mode"] == "ha_st":
        sl = st.get("ha_st_stop_price")
        if sl is not None:
            if (st["position"] == "long" and price <= sl) or (st["position"] == "short" and price >= sl):
                await execute_exit(symbol, price, "SL")
        return

    if cfg["entry_mode"] != "grid":
        return

    tp_step_abs = compute_step_abs(st["avg_entry_price"], cfg, "tp")
    grid_step_abs = compute_step_abs(st["avg_entry_price"], cfg, "grid")
    max_nachkauf = cfg["max_nachkauf"]

    if st["position"] == "long":
        if price >= st["avg_entry_price"] + tp_step_abs:
            await execute_exit(symbol, price, "TP")
        elif bot_active and price <= st["avg_entry_price"] - grid_step_abs and (max_nachkauf == 0 or st["entry_count"] < max_nachkauf):
            await execute_entry(symbol, "long", price, is_add_on=True)
    elif st["position"] == "short":
        if price <= st["avg_entry_price"] - tp_step_abs:
            await execute_exit(symbol, price, "TP")
        elif bot_active and price >= st["avg_entry_price"] + grid_step_abs and (max_nachkauf == 0 or st["entry_count"] < max_nachkauf):
            await execute_entry(symbol, "short", price, is_add_on=True)


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
                    if channel.startswith("trade"):
                        try:
                            market_index = int(channel.split(":")[1].split("/")[0]) if ":" in channel else int(channel.split("/")[1])
                        except Exception:
                            market_index = None
                        symbol = MARKET_INDEX_TO_SYMBOL.get(market_index)
                        if symbol is None:
                            continue
                        trades = msg.get("trades", [])
                        if trades:
                            price = float(trades[-1]["price"])
                            await on_price_update(symbol, price)

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
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8"><title>Grid-Bot Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
  :root {
    --bg: #060a18;
    --panel: #0e1526;
    --panel-border: rgba(96, 165, 250, 0.14);
    --accent: #3b82f6;
    --accent2: #8b5cf6;
    --text: #e8ecf5;
    --text-dim: #7c8aa8;
    --green: #22c55e;
    --red: #f0526b;
  }
  * { box-sizing: border-box; }
  body {
    font-family: -apple-system, "Segoe UI", sans-serif;
    background:
      radial-gradient(ellipse 800px 500px at 90% -5%, rgba(59,130,246,0.16), transparent 60%),
      radial-gradient(ellipse 700px 500px at -5% 15%, rgba(139,92,246,0.12), transparent 60%),
      var(--bg);
    color: var(--text);
    margin: 0;
    padding: 0 0 40px 0;
    min-height: 100vh;
  }
  .topbar {
    display: flex; align-items: center; justify-content: space-between;
    padding: 16px 28px; background: rgba(10,14,28,0.85); backdrop-filter: blur(8px);
    border-bottom: 1px solid var(--panel-border); margin-bottom: 24px; flex-wrap: wrap; gap: 12px;
  }
  .brand { display:flex; align-items:center; gap:10px; font-size:19px; font-weight:700; color:#fff; }
  .brand .dot { width:10px; height:10px; border-radius:50%; background:linear-gradient(135deg,var(--accent),var(--accent2)); box-shadow:0 0 12px var(--accent); }
  .topbar-right { display:flex; align-items:center; gap:10px; flex-wrap: wrap; }
  select#symbol-select {
    font-size:14px; font-weight:600; padding:8px 16px; background:var(--panel); color:var(--text);
    border:1px solid var(--panel-border); border-radius:10px; cursor:pointer;
  }
  .container { padding: 0 28px; }
  h2.section-title { font-size: 13px; color: var(--text-dim); text-transform: uppercase; letter-spacing: 0.06em; margin: 28px 0 12px; font-weight: 600; }
  .grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(190px,1fr)); gap:14px; margin-bottom:18px; }
  .card {
    background: var(--panel); border: 1px solid var(--panel-border); border-radius: 18px;
    padding: 18px 20px; box-shadow: 0 8px 24px rgba(0,0,0,0.25);
  }
  .card .label { font-size: 11px; color: var(--text-dim); text-transform: uppercase; letter-spacing: 0.04em; }
  .card .value { font-size: 22px; font-weight: 700; margin-top: 6px; color: #fff; }
  .green { color: var(--green) !important; } .red { color: var(--red) !important; } .yellow { color: #fbbf24 !important; }
  .badge { display:inline-block; padding:4px 14px; border-radius:20px; font-size:12px; font-weight:700; letter-spacing:0.03em; }
  .badge.dry { background:rgba(99,102,241,0.18); color:#a5b4fc; border:1px solid rgba(99,102,241,0.35); }
  .badge.live { background:rgba(240,82,107,0.15); color:#fca5b1; border:1px solid rgba(240,82,107,0.4); }
  .badge.active { background:rgba(34,197,94,0.15); color:#86efac; border:1px solid rgba(34,197,94,0.35); }
  .badge.paused { background:rgba(251,191,36,0.15); color:#fde68a; border:1px solid rgba(251,191,36,0.35); }
  .panel-card { background: var(--panel); border: 1px solid var(--panel-border); border-radius: 20px; padding: 22px; margin-bottom: 20px; box-shadow: 0 8px 24px rgba(0,0,0,0.25); }
  form { display:grid; grid-template-columns: repeat(auto-fit, minmax(170px,1fr)); gap:14px; align-items:end; }
  label { display:block; font-size:11px; color: var(--text-dim); text-transform:uppercase; letter-spacing:0.03em; margin-bottom:6px; }
  input, select.cfg {
    width:100%; padding:9px 10px; background:#080d1c; border:1px solid var(--panel-border);
    border-radius:8px; color:var(--text); box-sizing:border-box; font-size:13px;
  }
  input:focus, select.cfg:focus { outline:none; border-color: var(--accent); }
  button {
    padding:10px 20px; background:linear-gradient(135deg,var(--accent),#2563eb); color:white; border:none;
    border-radius:10px; cursor:pointer; font-weight:700; font-size:13px; transition: transform 0.1s;
  }
  button:hover { transform: translateY(-1px); filter: brightness(1.1); }
  button.stop { background:linear-gradient(135deg,#f0526b,#dc2626); }
  button.start { background:linear-gradient(135deg,#22c55e,#15803d); }
  button.danger { background:linear-gradient(135deg,#ef4444,#b91c1c); }
  button.neutral { background:linear-gradient(135deg,#475569,#334155); }
  table { width:100%; border-collapse:collapse; font-size:13px; margin-top:6px; }
  th, td { text-align:left; padding:9px 10px; border-bottom:1px solid var(--panel-border); }
  th { color: var(--text-dim); font-weight:600; font-size:11px; text-transform:uppercase; letter-spacing:0.03em; }
  tr:hover td { background: rgba(59,130,246,0.05); }
  .warn { background:rgba(240,82,107,0.12); border:1px solid rgba(240,82,107,0.35); color:#fca5b1; padding:10px 14px; border-radius:10px; font-size:13px; margin-top:10px; display:none; }
  canvas { background: var(--panel); border: 1px solid var(--panel-border); border-radius: 18px; padding: 14px; box-shadow: 0 8px 24px rgba(0,0,0,0.25); }
  #priceChart { max-height: 420px; }
  .coin-overview { display:flex; gap:8px; flex-wrap:wrap; margin-bottom:18px; }
  .coin-pill { background: var(--panel); border:1px solid var(--panel-border); border-radius:20px; padding:6px 16px; font-size:13px; cursor:pointer; transition: border-color 0.15s; }
  .coin-pill:hover { border-color: rgba(96,165,250,0.4); }
  .coin-pill.selected { border-color: var(--accent); background: rgba(59,130,246,0.12); }
</style>
</head>
<body>
<div class="topbar">
  <div class="brand"><span class="dot"></span>⚡ GridBot <select id="symbol-select"></select></div>
  <div class="topbar-right"><a href="/copytrading" style="color:#93c5fd; text-decoration:none; font-size:13px; margin-right:14px;">📡 Copy-Trading →</a><span id="mode-badge"></span><span id="active-badge"></span></div>
</div>
<div class="container">

<div class="coin-overview" id="coin-overview"></div>

<div style="margin-bottom:20px;">
  <button id="btn-start" class="start">▶️ Start</button>
  <button id="btn-stop" class="stop">⏸️ Stop</button>
  <button id="btn-close" class="danger">✖️ Position jetzt schließen</button>
  <button id="btn-reset" class="neutral">🔄 Reset (Statistik)</button>
</div>

<h2 class="section-title">Übersicht</h2>
<div class="grid" id="status-grid"></div>

<h2 class="section-title">Kursverlauf</h2>
<canvas id="priceChart" height="400"></canvas>

<h2 class="section-title">Einstellungen (nur für den ausgewählten Coin)</h2>
<div class="panel-card">
<form id="config-form">
  <div><label>Margin (USDC)</label><input type="number" step="any" id="margin"></div>

  <div><label>Hebel</label><input type="number" step="1" id="leverage"></div>
  <div><label>Strategie</label>
    <select class="cfg" id="entry_mode">
      <option value="grid">Neutrales Grid (Ø-Einstieg/Nachkauf/TP)</option>
      <option value="ha_st">Heikin Ashi Supertrend (Buy/Sell, SL an Signalkerze)</option>
      <option value="candle_color">Kerzenfarbe (früher Einstieg, Exit bei Gegenkerze)</option>
    </select>
  </div>
  <div data-mode="ha_st"><label>HA-Supertrend Zeitrahmen</label>
    <select class="cfg" id="ha_st_resolution">
      <option value="1m">1 Minute</option>
      <option value="3m">3 Minuten</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="30m">30 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
    </select>
  </div>
  <div data-mode="ha_st"><label>HA-ATR Periode</label><input type="number" step="1" id="ha_st_atr_period"></div>
  <div data-mode="ha_st"><label>HA-ATR Multiplikator</label><input type="number" step="0.1" id="ha_st_atr_mult"></div>
  <div data-mode="ha_st"><label>Trendfilter (Long nur aufwärts, Short nur abwärts)</label>
    <select class="cfg" id="ha_st_trend_filter">
      <option value="true">An</option>
      <option value="false">Aus</option>
    </select>
  </div>
  <div data-mode="ha_st"><label>Trend-EMA Länge</label><input type="number" step="1" id="ha_st_trend_ema_length"></div>
  <div data-mode="candle_color"><label>Kerzenlänge (Sekunden)</label><input type="number" step="1" id="cc_resolution_seconds"></div>
  <div data-mode="candle_color"><label>Bestätigung nach (Sekunden)</label><input type="number" step="1" id="cc_confirm_delay_seconds"></div>
  <div data-mode="candle_color"><label>Nach Gegenkerze sofort drehen</label>
    <select class="cfg" id="cc_auto_reverse">
      <option value="true">Ja</option>
      <option value="false">Nein</option>
    </select>
  </div>
  <div data-mode="candle_color"><label>Früher Ausstieg (nicht auf Schluss warten)</label>
    <select class="cfg" id="cc_early_exit">
      <option value="true">Ja - sofort bei Gegenfarbe</option>
      <option value="false">Nein - erst bei fertigem Kerzenschluss</option>
    </select>
  </div>
  <div data-mode="grid"><label>Grid-Modus</label>
    <select class="cfg" id="grid_mode">
      <option value="pct">Prozent (%)</option>
      <option value="usd">Fester $-Betrag</option>
    </select>
  </div>
  <div data-mode="grid"><label>Grid-Stufe (%)</label><input type="number" step="any" id="grid_step_pct"></div>
  <div data-mode="grid"><label>TP-Stufe (%)</label><input type="number" step="any" id="tp_step_pct"></div>
  <div data-mode="grid"><label>Grid-Stufe ($)</label><input type="number" step="any" id="grid_step_usd"></div>
  <div data-mode="grid"><label>TP-Stufe ($)</label><input type="number" step="any" id="tp_step_usd"></div>
  <div data-mode="grid"><label>Max. Nachkauf</label><input type="number" step="1" id="max_nachkauf"></div>
  <div data-mode="grid"><label>Nach TP sofort drehen</label>
    <select class="cfg" id="auto_reverse">
      <option value="true">Ja - sofort Gegenposition</option>
      <option value="false">Nein - warten auf neues Gitter-Signal</option>
    </select>
  </div>
  <div><label>Modus</label>
    <select class="cfg" id="dry_run">
      <option value="true">DRY RUN (Simulation)</option>
      <option value="false">LIVE (echte Orders!)</option>
    </select>
  </div>
  <button type="submit">Speichern</button>
</form>
<div class="warn" id="live-warn">⚠️ LIVE-Modus aktiv - echte Orders werden platziert!</div>
<div style="font-size:12px; color:var(--text-dim); margin-top:8px;" id="abs-distances"></div>
</div>

<h2 class="section-title">Letzte abgeschlossene Trades</h2>
<div class="panel-card">
<table id="trades-table"><thead><tr><th>Eröffnet</th><th>Geschlossen</th><th>Seite</th><th>Ø-Einstieg</th><th>Exit</th><th>Stufen</th><th>Grund</th><th>PnL $</th></tr></thead><tbody></tbody></table>
</div>
</div>

<script>
let priceChart;
let currentSymbol = null;
let allSymbols = [];

function updateModeFields() {
  const mode = document.getElementById('entry_mode').value;
  document.querySelectorAll('[data-mode]').forEach(el => {
    el.style.display = (el.dataset.mode === mode) ? '' : 'none';
  });
}
document.getElementById('entry_mode').addEventListener('change', () => {
  window.formTouched = true;
  updateModeFields();
});

async function loadSymbols() {
  const res = await fetch('/api/symbols');
  const data = await res.json();
  allSymbols = data.symbols;
  const sel = document.getElementById('symbol-select');
  sel.innerHTML = allSymbols.map(s => `<option value="${s}">${s}</option>`).join('');
  currentSymbol = allSymbols[0];
  sel.value = currentSymbol;
  sel.addEventListener('change', () => { currentSymbol = sel.value; window.formTouched = false; refresh(); });
}

document.getElementById('btn-start').addEventListener('click', async () => {
  await fetch(`/api/control?symbol=${currentSymbol}`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({bot_active:true}) });
});
document.getElementById('btn-stop').addEventListener('click', async () => {
  await fetch(`/api/control?symbol=${currentSymbol}`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({bot_active:false}) });
});
document.getElementById('btn-close').addEventListener('click', async () => {
  if (!confirm(`Position für ${currentSymbol} jetzt zum aktuellen Preis schließen?`)) return;
  const res = await fetch(`/api/close?symbol=${currentSymbol}`, { method:'POST' });
  const data = await res.json();
  if (data.error) alert(data.error);
  refresh();
});
document.getElementById('btn-reset').addEventListener('click', async () => {
  if (!confirm(`Statistik/Trade-Log für ${currentSymbol} zurücksetzen? (nur möglich wenn flach)`)) return;
  const res = await fetch(`/api/reset?symbol=${currentSymbol}`, { method:'POST' });
  const data = await res.json();
  if (data.error) alert(data.error);
  refresh();
});

async function refresh() {
  if (!currentSymbol) return;
  const res = await fetch(`/api/status?symbol=${currentSymbol}`);
  const data = await res.json();

  // Uebersichts-Pills fuer alle Coins
  const overviewRes = await fetch('/api/overview');
  const overview = await overviewRes.json();
  document.getElementById('coin-overview').innerHTML = Object.entries(overview).map(([sym, o]) => `
    <div class="coin-pill ${sym===currentSymbol?'selected':''}" onclick="document.getElementById('symbol-select').value='${sym}'; document.getElementById('symbol-select').dispatchEvent(new Event('change'));">
      ${sym}: ${o.position || 'flach'} | PnL $${o.total_pnl_usd}
    </div>
  `).join('');

  document.getElementById('mode-badge').innerHTML =
    data.config.dry_run ? '<span class="badge dry">DRY RUN</span>' : '<span class="badge live">LIVE</span>';
  document.getElementById('active-badge').innerHTML =
    data.config.bot_active ? '<span class="badge active">AKTIV</span>' : '<span class="badge paused">GESTOPPT</span>';
  document.getElementById('live-warn').style.display = data.config.dry_run ? 'none' : 'block';

  const gl = data.grid_levels || {};
  document.getElementById('status-grid').innerHTML = `
    <div class="card"><div class="label">Symbol</div><div class="value">${data.symbol}</div></div>
    <div class="card"><div class="label">Preis</div><div class="value">${data.last_price ?? '-'}</div></div>
    <div class="card"><div class="label">Position</div><div class="value ${data.position==='long'?'green':data.position==='short'?'red':'yellow'}">${data.position || 'flach'}</div></div>
    <div class="card"><div class="label">Ø-Einstieg</div><div class="value">${data.avg_entry_price ?? '-'}</div></div>
    <div class="card"><div class="label">Unrealisiert $</div><div class="value ${data.unrealized_pnl_usd>=0?'green':'red'}">${data.unrealized_pnl_usd}</div></div>
    <div class="card"><div class="label">Nachkauf-Stufe</div><div class="value">${data.entry_count} / ${data.config.max_nachkauf || '∞'}</div></div>
    <div class="card"><div class="label">Geschätzter Liq.-Preis</div><div class="value red">${data.liquidation_price ?? '-'}</div></div>
    <div class="card"><div class="label">HA-Supertrend SL (${data.config.entry_mode==='ha_st'?'aktiv':'inaktiv'})</div><div class="value red">${data.ha_st_stop_price ?? '-'}</div></div>
    <div class="card"><div class="label">Letzte Kerzenfarbe (${data.config.entry_mode==='candle_color'?'aktiv':'inaktiv'})</div><div class="value ${data.cc_last_color==='green'?'green':data.cc_last_color==='red'?'red':'yellow'}">${data.cc_last_color ?? '-'}</div></div>
    <div class="card"><div class="label">Realisiert (gesamt) $</div><div class="value ${data.stats.total_pnl_usd>=0?'green':'red'}">${data.stats.total_pnl_usd}</div></div>
    <div class="card"><div class="label">Trades / Trefferquote</div><div class="value">${data.stats.trades} / ${data.stats.win_rate_pct}%</div></div>
  `;

  if (!window.formTouched) {
    document.getElementById('margin').value = data.config.margin;
    document.getElementById('leverage').value = data.config.leverage;
    document.getElementById('entry_mode').value = data.config.entry_mode;
    document.getElementById('ha_st_resolution').value = data.config.ha_st_resolution;
    document.getElementById('ha_st_atr_period').value = data.config.ha_st_atr_period;
    document.getElementById('ha_st_atr_mult').value = data.config.ha_st_atr_mult;
    document.getElementById('ha_st_trend_filter').value = String(data.config.ha_st_trend_filter);
    document.getElementById('ha_st_trend_ema_length').value = data.config.ha_st_trend_ema_length;
    document.getElementById('cc_resolution_seconds').value = data.config.cc_resolution_seconds;
    document.getElementById('cc_confirm_delay_seconds').value = data.config.cc_confirm_delay_seconds;
    document.getElementById('cc_auto_reverse').value = String(data.config.cc_auto_reverse);
    document.getElementById('cc_early_exit').value = String(data.config.cc_early_exit);
    document.getElementById('grid_mode').value = data.config.grid_mode;
    document.getElementById('grid_step_pct').value = data.config.grid_step_pct;
    document.getElementById('tp_step_pct').value = data.config.tp_step_pct;
    document.getElementById('grid_step_usd').value = data.config.grid_step_usd;
    document.getElementById('tp_step_usd').value = data.config.tp_step_usd;
    document.getElementById('max_nachkauf').value = data.config.max_nachkauf;
    document.getElementById('dry_run').value = String(data.config.dry_run);
    document.getElementById('auto_reverse').value = String(data.config.auto_reverse);
  }
  updateModeFields();

  document.getElementById('abs-distances').innerText =
    `Aktuelle Abstände in $: Grid-Stufe ≈ ${gl.grid_step_abs ?? '-'} | TP-Stufe ≈ ${gl.tp_step_abs ?? '-'}`;

  const hist = data.price_history || [];
  const labels = hist.map(p => new Date(p.ts).toLocaleTimeString());
  const prices = hist.map(p => p.price);
  const n = labels.length;

  const datasets = [{ label: 'Preis', data: prices, borderColor:'#60a5fa', pointRadius:0, borderWidth:2 }];
  if (gl.anchor) datasets.push({ label:'Anker', data: Array(n).fill(gl.anchor), borderColor:'#9ca3af', borderDash:[4,4], pointRadius:0, borderWidth:1 });
  if (gl.tp_price) datasets.push({ label:'TP', data: Array(n).fill(gl.tp_price), borderColor:'#4ade80', borderDash:[6,3], pointRadius:0, borderWidth:1 });
  if (gl.next_nachkauf_price) datasets.push({ label:'Nächster Nachkauf', data: Array(n).fill(gl.next_nachkauf_price), borderColor:'#f87171', borderDash:[6,3], pointRadius:0, borderWidth:1 });
  if (gl.next_entry_long) datasets.push({ label:'Entry Long ab', data: Array(n).fill(gl.next_entry_long), borderColor:'#4ade80', borderDash:[2,2], pointRadius:0, borderWidth:1 });
  if (gl.next_entry_short) datasets.push({ label:'Entry Short ab', data: Array(n).fill(gl.next_entry_short), borderColor:'#f87171', borderDash:[2,2], pointRadius:0, borderWidth:1 });

  if (priceChart) priceChart.destroy();
  priceChart = new Chart(document.getElementById('priceChart'), {
    type: 'line',
    data: { labels, datasets },
    options: { responsive:true, maintainAspectRatio:false, animation:false, scales:{ x:{ display:false }, y:{ ticks:{color:'#9ca3af'} } }, plugins:{legend:{labels:{color:'#e5e7eb'}}} }
  });

  const trades = (data.trade_log || []).slice(-15).reverse();
  const fmtTime = (iso) => iso ? new Date(iso).toLocaleString('de-DE', {day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit'}) : '-';
  document.querySelector('#trades-table tbody').innerHTML = trades.map(t => `
    <tr><td>${fmtTime(t.opened_at)}</td><td>${fmtTime(t.closed_at)}</td><td>${t.side}</td><td>${t.avg_entry}</td><td>${t.exit}</td><td>${t.entries}</td><td>${t.reason ?? '-'}</td>
    <td class="${t.pnl_usd>=0?'green':'red'}">${t.pnl_usd}</td></tr>
  `).join('');
}

document.getElementById('config-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const payload = {
    margin: parseFloat(document.getElementById('margin').value),
    leverage: parseInt(document.getElementById('leverage').value),
    entry_mode: document.getElementById('entry_mode').value,
    ha_st_resolution: document.getElementById('ha_st_resolution').value,
    ha_st_atr_period: parseInt(document.getElementById('ha_st_atr_period').value),
    ha_st_atr_mult: parseFloat(document.getElementById('ha_st_atr_mult').value),
    ha_st_trend_filter: document.getElementById('ha_st_trend_filter').value === 'true',
    ha_st_trend_ema_length: parseInt(document.getElementById('ha_st_trend_ema_length').value),
    cc_resolution_seconds: parseInt(document.getElementById('cc_resolution_seconds').value),
    cc_confirm_delay_seconds: parseInt(document.getElementById('cc_confirm_delay_seconds').value),
    cc_auto_reverse: document.getElementById('cc_auto_reverse').value === 'true',
    cc_early_exit: document.getElementById('cc_early_exit').value === 'true',
    grid_mode: document.getElementById('grid_mode').value,
    grid_step_pct: parseFloat(document.getElementById('grid_step_pct').value),
    tp_step_pct: parseFloat(document.getElementById('tp_step_pct').value),
    grid_step_usd: parseFloat(document.getElementById('grid_step_usd').value),
    tp_step_usd: parseFloat(document.getElementById('tp_step_usd').value),
    max_nachkauf: parseInt(document.getElementById('max_nachkauf').value),
    dry_run: document.getElementById('dry_run').value === 'true',
    auto_reverse: document.getElementById('auto_reverse').value === 'true',
  };
  await fetch(`/api/config?symbol=${currentSymbol}`, { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(payload) });
  window.formTouched = false;
  alert(`Gespeichert für ${currentSymbol}!`);
});

['margin','leverage','entry_mode','ha_st_resolution','ha_st_atr_period','ha_st_atr_mult','ha_st_trend_filter','ha_st_trend_ema_length','cc_resolution_seconds','cc_confirm_delay_seconds','cc_auto_reverse','cc_early_exit','grid_mode','grid_step_pct','tp_step_pct','grid_step_usd','tp_step_usd','max_nachkauf','dry_run','auto_reverse'].forEach(id => {
  document.getElementById(id).addEventListener('input', (e) => {
    window.formTouched = true;
    if (typeof e.target.value === 'string' && e.target.value.includes(',')) {
      e.target.value = e.target.value.replace(',', '.');
    }
  });
});

(async () => {
  await loadSymbols();
  refresh();
  setInterval(refresh, 3000);
})();
</script>
</body>
</html>
"""


async def handle_index(request):
    return web.Response(text=DASHBOARD_HTML, content_type="text/html")


async def handle_symbols(request):
    return web.json_response({"symbols": SYMBOLS})


async def handle_overview(request):
    result = {}
    for s in SYMBOLS:
        st = BOTS[s]["state"]
        result[s] = {"position": st["position"], "total_pnl_usd": round(st["stats"]["total_pnl_usd"], 3)}
    return web.json_response(result)


async def handle_status(request):
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    b = BOTS[symbol]
    st, cfg, stats = b["state"], b["config"], b["state"]["stats"]
    win_rate = round(stats["wins"] / stats["trades"] * 100, 1) if stats["trades"] else 0
    payload = {
        "symbol": symbol, "last_price": st["last_price"], "anchor_price": st["anchor_price"],
        "position": st["position"], "avg_entry_price": round(st["avg_entry_price"], 2) if st["avg_entry_price"] else None,
        "entry_count": st["entry_count"], "liquidation_price": estimate_liquidation_price(symbol),
        "unrealized_pnl_usd": calc_unrealized_pnl(symbol),
        "grid_levels": calc_grid_levels(symbol),
        "ha_st_stop_price": st.get("ha_st_stop_price"),
        "cc_last_color": st.get("cc_last_color"),
        "config": cfg,
        "stats": {"trades": stats["trades"], "win_rate_pct": win_rate, "total_pnl_usd": round(stats["total_pnl_usd"], 3)},
        "trade_log": st["trade_log"][-20:],
        "price_history": st["price_history"][-200:],
    }
    return web.json_response(payload)


async def handle_config_update(request):
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    body = await request.json()
    cfg = BOTS[symbol]["config"]
    for key in ["margin", "leverage", "entry_mode", "grid_mode", "grid_step_pct", "tp_step_pct",
                "grid_step_usd", "tp_step_usd", "max_nachkauf", "dry_run", "auto_reverse",
                "ha_st_resolution", "ha_st_atr_period", "ha_st_atr_mult",
                "ha_st_trend_filter", "ha_st_trend_ema_length",
                "cc_resolution_seconds", "cc_confirm_delay_seconds", "cc_auto_reverse", "cc_early_exit"]:
        if key in body:
            cfg[key] = body[key]
    debug_log(f"⚙️ [{symbol}] Konfiguration aktualisiert", cfg)
    return web.json_response({"success": True, "config": cfg})


async def handle_control(request):
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    body = await request.json()
    cfg = BOTS[symbol]["config"]
    if "bot_active" in body:
        cfg["bot_active"] = bool(body["bot_active"])
        debug_log(f"{'▶️' if cfg['bot_active'] else '⏸️'} [{symbol}] Bot {'gestartet' if cfg['bot_active'] else 'gestoppt'}")
    return web.json_response({"success": True, "bot_active": cfg["bot_active"]})


async def handle_close_position(request):
    """Manuelles sofortiges Schliessen der offenen Position (Market-Order, egal ob TP/SL erreicht)."""
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    st = BOTS[symbol]["state"]
    if st["position"] is None:
        return web.json_response({"error": "keine offene Position"}, status=400)
    if st["last_price"] is None:
        return web.json_response({"error": "kein aktueller Preis bekannt"}, status=400)
    await execute_exit(symbol, st["last_price"], "MANUAL")
    return web.json_response({"success": True})


async def handle_reset(request):
    """Setzt Statistik/Trade-Log/Anker zurueck - nur erlaubt wenn der Bot gerade flach ist."""
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    st = BOTS[symbol]["state"]
    if st["position"] is not None:
        return web.json_response({"error": "Position ist noch offen - erst schliessen, dann reset"}, status=400)
    st["stats"] = {"trades": 0, "wins": 0, "losses": 0, "total_pnl_usd": 0.0}
    st["trade_log"] = []
    st["anchor_price"] = st["last_price"]
    st["entry_count"] = 0
    debug_log(f"🔄 [{symbol}] Zurückgesetzt (Statistik, Trade-Log, neuer Anker)")
    return web.json_response({"success": True})


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
  input[type=text] { padding:8px 10px; background:#080d1c; border:1px solid var(--border); border-radius:8px; color:var(--text); }
  input.new-address { width:340px; }
  input.coin-filter { width:130px; font-size:12px; }
  button { padding:7px 14px; background:linear-gradient(135deg,var(--accent),#2563eb); color:#fff; border:none; border-radius:8px; cursor:pointer; font-weight:700; font-size:12px; }
  button.copy-on { background:linear-gradient(135deg,#22c55e,#15803d); }
  button.copy-off { background:linear-gradient(135deg,#475569,#334155); }
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
  <thead><tr><th>Label</th><th>Adresse</th><th>Coin</th><th>Seite</th><th>Größe</th><th>Ø-Einstieg</th><th>PnL</th><th>Eröffnet</th><th>Aktion</th></tr></thead>
  <tbody></tbody>
</table>

<h2>Copy-Einstellungen pro Trader</h2>
<table id="copy-table">
  <thead><tr><th>Label</th><th>Adresse</th><th>Letzte Fills</th><th>Coin-Filter (leer = alle)</th><th>Copy</th></tr></thead>
  <tbody></tbody>
</table>

<script>
function fmtTime(iso) {
  return iso ? new Date(iso).toLocaleString('de-DE', {day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'}) : '-';
}

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
  let posRows = [];
  Object.entries(data.watched).forEach(([addr, info]) => {
    const meta = info.position_meta || {};
    (info.positions || []).forEach(p => {
      const m = meta[p.coin] || {};
      const pnl = parseFloat(p.unrealized_pnl || 0);
      const actionClass = m.last_action === 'Neu' ? 'action-neu' : m.last_action === 'Nachkauf' ? 'action-nachkauf' : m.last_action === 'Reverse' ? 'action-reverse' : '';
      posRows.push(`
        <tr>
          <td>${info.label}</td>
          <td class="addr">${addr.slice(0,8)}...${addr.slice(-4)}</td>
          <td><b>${p.coin}</b></td>
          <td class="${p.side==='long'?'green':'red'}">${p.side==='long'?'🟢 LONG':'🔴 SHORT'}</td>
          <td>${p.size}</td>
          <td>${p.entry_price ?? '-'}</td>
          <td class="${pnl>=0?'green':'red'}">$${pnl.toFixed(2)}</td>
          <td>${fmtTime(m.opened_at)}</td>
          <td class="${actionClass}">${m.last_action ?? '-'}${m.entries>1?' ('+m.entries+'x)':''}</td>
        </tr>`);
    });
  });
  document.querySelector('#watch-table tbody').innerHTML = posRows.join('') || '<tr><td colspan="9">Noch keine offenen Positionen erfasst...</td></tr>';

  // Copy-Einstellungen pro Trader
  const copyRows = Object.entries(data.watched).map(([addr, info]) => {
    const fillsText = (info.recent_fills || []).slice(-3).reverse().map(f =>
      `<span class="${f.side==='B'?'fill-buy':'fill-sell'}">${f.side==='B'?'BUY':'SELL'} ${f.coin} @ ${f.px}</span>`
    ).join('<br>') || '-';
    const coinFilterValue = (info.copy_coins || []).join(',');
    return `
      <tr>
        <td>${info.label}</td>
        <td class="addr">${addr.slice(0,10)}...${addr.slice(-6)}</td>
        <td>${fillsText}</td>
        <td>
          <input type="text" class="coin-filter" id="coins-${addr}" placeholder="z.B. BTC,ETH" value="${coinFilterValue}">
          <button onclick="saveCoins('${addr}')">💾</button>
        </td>
        <td><button class="${info.copy_enabled?'copy-on':'copy-off'}" onclick="toggleCopy('${addr}', ${!info.copy_enabled})">${info.copy_enabled?'Copy AN':'Copy AUS'}</button></td>
      </tr>`;
  }).join('');
  document.querySelector('#copy-table tbody').innerHTML = copyRows || '<tr><td colspan="5">Noch keine Trader beobachtet...</td></tr>';
}

async function toggleCopy(address, enable) {
  await fetch('/api/ct/copy', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({address, enable}) });
  refresh();
}

async function saveCoins(address) {
  const coins = document.getElementById(`coins-${address}`).value;
  await fetch('/api/ct/coins', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({address, coins}) });
  alert('Coin-Filter gespeichert für ' + address.slice(0,10) + '...');
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
    })


async def handle_ct_watch(request):
    body = await request.json()
    addr = body.get("address", "").strip()
    if not addr:
        return web.json_response({"error": "keine Adresse"}, status=400)
    if addr not in CT_STATE["watched"]:
        CT_STATE["watched"][addr] = {
            "label": "Manuell hinzugefügt", "copy_enabled": False, "copy_coins": [],
            "last_fill_time": None, "positions": [], "recent_fills": [], "source": "manual",
            "position_meta": {},
        }
    return web.json_response({"success": True})


async def handle_ct_copy_toggle(request):
    body = await request.json()
    addr = body.get("address")
    enable = bool(body.get("enable"))
    if addr in CT_STATE["watched"]:
        CT_STATE["watched"][addr]["copy_enabled"] = enable
        debug_log(f"{'✅' if enable else '⏸️'} [CopyTrading] Copy für {addr} {'aktiviert' if enable else 'deaktiviert'}")
    return web.json_response({"success": True})


async def handle_ct_set_coins(request):
    body = await request.json()
    addr = body.get("address")
    coins_raw = body.get("coins", "")
    coins = [c.strip().upper() for c in coins_raw.split(",") if c.strip()]
    if addr in CT_STATE["watched"]:
        CT_STATE["watched"][addr]["copy_coins"] = coins
        debug_log(f"⚙️ [CopyTrading] Coin-Filter für {addr} gesetzt", {"coins": coins or "alle"})
    return web.json_response({"success": True, "copy_coins": coins})


async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/symbols", handle_symbols)
    app.router.add_get("/api/overview", handle_overview)
    app.router.add_get("/api/status", handle_status)
    app.router.add_post("/api/config", handle_config_update)
    app.router.add_post("/api/control", handle_control)
    app.router.add_post("/api/close", handle_close_position)
    app.router.add_post("/api/reset", handle_reset)
    app.router.add_get("/copytrading", handle_ct_index)
    app.router.add_get("/api/ct/status", handle_ct_status)
    app.router.add_post("/api/ct/watch", handle_ct_watch)
    app.router.add_post("/api/ct/copy", handle_ct_copy_toggle)
    app.router.add_post("/api/ct/coins", handle_ct_set_coins)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    debug_log(f"🌐 Dashboard läuft auf Port {PORT}")


async def main():
    print("=" * 60)
    print(f"🚀 Multi-Coin Grid-Bot - Dashboard auf Port {PORT}")
    print(f"   Coins: {', '.join(SYMBOLS)}")
    for s in SYMBOLS:
        cfg = BOTS[s]["config"]
        print(f"   [{s}] DRY_RUN={cfg['dry_run']} Margin={cfg['margin']} Hebel={cfg['leverage']}x Grid={cfg['grid_step_pct']}% TP={cfg['tp_step_pct']}%")
    print("=" * 60)

    await start_web_server()
    await asyncio.gather(
        trading_loop(),
        *[ha_supertrend_poll_loop(s) for s in SYMBOLS],
        ct_leaderboard_refresh_loop(),
        ct_watch_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
