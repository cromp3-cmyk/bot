"""
Market-Making Bot für Lighter (zkLighter)
============================================
Sicherer Market-Maker mit strengen Positionslimits
"""

import asyncio
import websockets
import json
import time
import os
import traceback
from datetime import datetime

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
MARKET_INDICES = {"ETH": 0, "BTC": 1, "SOL": 2, "DOGE": 3, "AVAX": 9, "SUI": 16}

PRECISION_MAP = {"BTC": 100000, "ETH": 10000, "SOL": 1000, "AVAX": 100, "SUI": 10}
PRICE_DECIMALS_MAP = {"BTC": 1, "ETH": 2, "SOL": 3, "AVAX": 3, "SUI": 5}
MIN_BASE_AMOUNT_MAP = {"BTC": 0.00020, "ETH": 0.005, "SOL": 0.05, "AVAX": 0.5, "SUI": 3.0}


def get_precision(symbol):
    return PRECISION_MAP.get(symbol, 10000)


def get_price_decimals(symbol):
    return PRICE_DECIMALS_MAP.get(symbol, 2)


def get_min_base_amount(symbol):
    return MIN_BASE_AMOUNT_MAP.get(symbol, 0.001)


# ========== KONFIGURATION (alles per Env-Variable) ==========
SYMBOL = os.getenv("MM_SYMBOL", "BTC")
if SYMBOL not in MARKET_INDICES:
    raise ValueError(f"Symbol {SYMBOL} nicht in MARKET_INDICES - hier ergänzen")
MARKET_INDEX = MARKET_INDICES[SYMBOL]

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

# === DEINE KORREKTEN PARAMETER ===
MARGIN = float(os.getenv("MM_MARGIN", "10"))              # 10 USDC Margin
LEVERAGE = int(os.getenv("MM_LEVERAGE", "2"))             # 2x Hebel (dein Wunsch)
MAX_POSITION_USDC = float(os.getenv("MM_MAX_POSITION_USDC", "10"))  # HARTES LIMIT: 10 USDC

# Order-Größe: 5% der Max-Position = 0.5 USDC
ORDER_SIZE_USDC = float(os.getenv("MM_ORDER_SIZE_USDC", "0.5"))

SPREAD_PCT = float(os.getenv("MM_SPREAD_PCT", "0.05"))       # 0.05%
REQUOTE_SECONDS = float(os.getenv("MM_REQUOTE_SECONDS", "5"))  
REQUOTE_MOVE_THRESHOLD_PCT = float(os.getenv("MM_REQUOTE_MOVE_PCT", "0.03"))

OBI_SKEW_ENABLED = os.getenv("MM_OBI_SKEW_ENABLED", "true").lower() == "true"
OBI_LEVELS = int(os.getenv("MM_OBI_LEVELS", "15"))
OBI_SKEW_FACTOR = float(os.getenv("MM_OBI_SKEW_FACTOR", "0.02"))

ACCOUNT_SYNC_SECONDS = float(os.getenv("MM_ACCOUNT_SYNC_SECONDS", "120"))

# === SCHUTZPARAMETER ===
MAX_LOSS_USDC = float(os.getenv("MM_MAX_LOSS_USDC", "3.0"))   # Stop-Loss bei -3 USDC
ORDER_TIMEOUT_SECONDS = float(os.getenv("MM_ORDER_TIMEOUT", "30"))  # 30s Timeout


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


async def place_post_only_limit(client, is_ask, price, base_amount):
    """Platziert eine echte Post-Only Limit-Order."""
    price_decimals = get_price_decimals(SYMBOL)
    price_scaled = int(price * (10 ** price_decimals))

    tx, tx_hash, err = await client.create_order(
        market_index=MARKET_INDEX,
        client_order_index=int(time.time() * 1000),
        base_amount=base_amount,
        price=price_scaled,
        is_ask=is_ask,
        order_type=client.ORDER_TYPE_LIMIT,
        time_in_force=client.ORDER_TIME_IN_FORCE_POST_ONLY,
        reduce_only=False,
    )
    return tx, tx_hash, err


async def cancel_all_real_orders(client):
    try:
        await client.cancel_all_orders(market_index=MARKET_INDEX)
    except Exception as e:
        debug_log("⚠️ cancel_all_orders fehlgeschlagen", {"error": str(e)})


async def sync_real_position():
    """Synchronisiert mit echtem Kontostand."""
    if DRY_RUN:
        return
    try:
        import lighter
        account_index = int(os.getenv("ACCOUNT_INDEX", "50960"))
        configuration = lighter.Configuration(host=BASE_URL)
        async with lighter.ApiClient(configuration) as api_client:
            account_api = lighter.AccountApi(api_client)
            response = await account_api.account(by="index", value=str(account_index))
            accounts = getattr(response, "accounts", None) or []
            if not accounts:
                return
            positions = getattr(accounts[0], "positions", []) or []
            for pos in positions:
                if getattr(pos, "market_index", None) == MARKET_INDEX:
                    real_size = float(getattr(pos, "position", 0) or 0)
                    debug_log("🔎 Echter Positionsabgleich", {
                        "echte_position_size": real_size,
                        "unsere_buchhaltung": STATE["inventory"],
                        "differenz": round(real_size - STATE["inventory"], 6),
                    })
                    # Korrigiere wenn nötig
                    if abs(real_size - STATE["inventory"]) > 0.0001:
                        STATE["inventory"] = real_size
                    return
    except Exception as e:
        debug_log("⚠️ Positions-Abgleich fehlgeschlagen", {"error": str(e)})


# ========== STATE ==========
order_book = {"bids": {}, "asks": {}}
STATE = {
    "inventory": 0.0,
    "avg_entry_price": 0.0,
    "realized_pnl_usdc": 0.0,
    "fills": 0,
    "resting_bid": None,
    "resting_ask": None,
    "last_mid": None,
    "is_stopped": False,  # Stop-Loss Flag
}
last_trade_price = None
last_trade_received_at = 0.0


def apply_order_book_update(msg):
    ob = msg.get("order_book", {})
    for side_key, book in (("bids", order_book["bids"]), ("asks", order_book["asks"])):
        for level in ob.get(side_key, []):
            price = level["price"]
            size = float(level["size"])
            if size == 0:
                book.pop(price, None)
            else:
                book[price] = size


def best_bid_ask():
    if not order_book["bids"] or not order_book["asks"]:
        return None, None
    return max(float(p) for p in order_book["bids"].keys()), min(float(p) for p in order_book["asks"].keys())


def calc_obi(levels=OBI_LEVELS):
    bids_sorted = sorted(order_book["bids"].items(), key=lambda x: float(x[0]), reverse=True)[:levels]
    asks_sorted = sorted(order_book["asks"].items(), key=lambda x: float(x[0]))[:levels]
    bid_vol = sum(v for _, v in bids_sorted)
    ask_vol = sum(v for _, v in asks_sorted)
    total = bid_vol + ask_vol
    return 0.0 if total == 0 else (bid_vol - ask_vol) / total


def check_safety_conditions(mid):
    """Prüft alle Sicherheitsbedingungen."""
    # 1. Stop-Loss
    if STATE["realized_pnl_usdc"] < -MAX_LOSS_USDC:
        debug_log(f"🚨 STOP-LOSS AUSGELÖST: {STATE['realized_pnl_usdc']:.2f} USDC")
        STATE["is_stopped"] = True
        return False
    
    # 2. Max Position
    current_position_usdc = abs(STATE["inventory"]) * mid
    if current_position_usdc >= MAX_POSITION_USDC * 1.1:  # 10% Toleranz
        debug_log(f"🚨 MAX POSITION ÜBERSCHRITTEN: {current_position_usdc:.2f} USDC")
        STATE["is_stopped"] = True
        return False
    
    # 3. Preis-Validierung
    if mid < 1000 or mid > 200000:
        debug_log(f"🚨 UNGÜLTIGER PREIS: {mid}")
        STATE["is_stopped"] = True
        return False
    
    # 4. Order-Timeouts
    now = time.time()
    for side, key in [("bid", "resting_bid"), ("ask", "resting_ask")]:
        order = STATE.get(key)
        if order and now - order.get("placed_at", 0) > ORDER_TIMEOUT_SECONDS:
            debug_log(f"⏰ {side.upper()} Timeout - Cancelling")
            STATE[key] = None
            if not DRY_RUN:
                asyncio.create_task(cancel_all_real_orders(get_lighter_client()))
    
    return True


def compute_target_quotes(mid, obi):
    """Berechnet Ziel-Bid/Ask mit Sicherheitschecks."""
    if STATE["is_stopped"]:
        return None, None
    
    # 1. Prüfe Sicherheit
    if not check_safety_conditions(mid):
        return None, None
    
    # 2. Berechne aktuelle Position
    position_usdc = STATE["inventory"] * mid
    position_pct = abs(position_usdc) / MAX_POSITION_USDC if MAX_POSITION_USDC > 0 else 0
    
    # 3. Basis-Spread
    skew = (obi * OBI_SKEW_FACTOR * mid) if OBI_SKEW_ENABLED else 0.0
    
    bid_price = mid * (1 - SPREAD_PCT / 100) + skew
    ask_price = mid * (1 + SPREAD_PCT / 100) + skew
    
    # 4. Inventory-Limits
    quote_bid = True
    quote_ask = True
    
    if position_usdc >= MAX_POSITION_USDC:
        quote_bid = False
        debug_log("⚠️ Nur ASK (Position zu Long)")
    elif position_usdc <= -MAX_POSITION_USDC:
        quote_ask = False
        debug_log("⚠️ Nur BID (Position zu Short)")
    
    # 5. Bei >80% Position nur noch reduzierende Seite
    if position_pct > 0.8:
        if position_usdc > 0:  # Long
            quote_bid = False
        else:  # Short
            quote_ask = False
    
    # 6. Wenn PnL negativ, reduziere Risiko
    if STATE["realized_pnl_usdc"] < -1.0:
        # Nur noch halbe Order-Größe
        global ORDER_SIZE_USDC_HALVED
        ORDER_SIZE_USDC_HALVED = ORDER_SIZE_USDC * 0.5
    else:
        ORDER_SIZE_USDC_HALVED = ORDER_SIZE_USDC
    
    return (bid_price if quote_bid else None), (ask_price if quote_ask else None)


def record_fill(side, price, base_amount):
    """Aktualisiert Inventory + PnL."""
    if STATE["is_stopped"]:
        return
    
    old_inventory = STATE["inventory"]
    position_usdc_before = abs(old_inventory) * price
    
    # Prüfe ob Fill die Max-Position überschreitet
    if side == "buy":
        new_inventory = old_inventory + base_amount
        if new_inventory * price > MAX_POSITION_USDC:
            debug_log(f"⚠️ FILL würde Max-Position überschreiten - ignoriert")
            return
    else:
        new_inventory = old_inventory - base_amount
        if -new_inventory * price > MAX_POSITION_USDC:
            debug_log(f"⚠️ FILL würde Max-Position überschreiten - ignoriert")
            return
    
    # PnL Berechnung
    if side == "buy":
        if old_inventory < 0:
            closed_amount = min(base_amount, -old_inventory)
            pnl = (STATE["avg_entry_price"] - price) * closed_amount
            STATE["realized_pnl_usdc"] += pnl
    else:
        if old_inventory > 0:
            closed_amount = min(base_amount, old_inventory)
            pnl = (price - STATE["avg_entry_price"]) * closed_amount
            STATE["realized_pnl_usdc"] += pnl
    
    # Update avg_entry_price
    if (old_inventory >= 0 and side == "buy") or (old_inventory <= 0 and side == "sell"):
        total_value = STATE["avg_entry_price"] * abs(old_inventory) + price * base_amount
        new_total = abs(old_inventory) + base_amount
        STATE["avg_entry_price"] = total_value / new_total if new_total > 0 else price
    elif new_inventory != 0 and (old_inventory >= 0) != (new_inventory >= 0):
        STATE["avg_entry_price"] = price
    
    STATE["inventory"] = new_inventory
    STATE["fills"] += 1
    
    debug_log(f"✅ FILL: {side.upper()} {base_amount:.8f} {SYMBOL} @ {price:.2f}", {
        "neues_inventory": round(new_inventory, 6),
        "inventory_usdc": round(new_inventory * price, 2),
        "avg_entry_price": round(STATE["avg_entry_price"], 4),
        "realized_pnl_usdc": round(STATE["realized_pnl_usdc"], 4),
        "max_position": MAX_POSITION_USDC,
        "position_pct": f"{abs(new_inventory * price) / MAX_POSITION_USDC * 100:.1f}%"
    })


def get_order_size_btc(mid):
    """Berechnet Order-Größe in BTC (sicher)"""
    # 1. Basis-Größe: 0.5 USDC (oder reduziert bei Verlust)
    order_size_usdc = min(ORDER_SIZE_USDC, ORDER_SIZE_USDC_HALVED)
    
    # 2. Prüfe verfügbaren Platz bis zum Limit
    current_position_usdc = abs(STATE["inventory"]) * mid
    remaining_usdc = max(0, MAX_POSITION_USDC - current_position_usdc)
    
    # 3. Order-Größe = min(Basis, verfügbarer Platz)
    safe_order_usdc = min(order_size_usdc, remaining_usdc * 0.9)
    
    # 4. In BTC umrechnen
    size_btc = safe_order_usdc / mid if mid > 0 else 0
    
    # 5. Minimum Exchange Size
    min_size_btc = get_min_base_amount(SYMBOL)
    
    if size_btc < min_size_btc:
        debug_log(f"⚠️ Order-Größe zu klein: {size_btc:.8f} < {min_size_btc}")
        return 0
    
    return size_btc


async def manage_quotes(client, mid, obi):
    """Requoting-Logik mit Sicherheitschecks."""
    if STATE["is_stopped"]:
        if not DRY_RUN:
            await cancel_all_real_orders(client)
        STATE["resting_bid"] = None
        STATE["resting_ask"] = None
        return
    
    target_bid, target_ask = compute_target_quotes(mid, obi)
    now = time.time()
    
    # Prüfe ob Requote nötig
    need_requote = STATE["last_mid"] is None or abs(mid - STATE["last_mid"]) / mid * 100 >= REQUOTE_MOVE_THRESHOLD_PCT
    time_based = (STATE["resting_bid"] is None or now - STATE["resting_bid"]["placed_at"] >= REQUOTE_SECONDS) and \
                 (STATE["resting_ask"] is None or now - STATE["resting_ask"]["placed_at"] >= REQUOTE_SECONDS)
    
    if not (need_requote or time_based):
        return
    
    STATE["last_mid"] = mid
    
    # Berechne Order-Größe
    base_amount = get_order_size_btc(mid)
    if base_amount <= 0:
        debug_log("⚠️ Keine gültige Order-Größe")
        return
    
    if DRY_RUN:
        STATE["resting_bid"] = {"price": target_bid, "placed_at": now} if target_bid else None
        STATE["resting_ask"] = {"price": target_ask, "placed_at": now} if target_ask else None
        return
    
    # Echtes Requoting
    await cancel_all_real_orders(client)
    
    if target_bid:
        tx, tx_hash, err = await place_post_only_limit(client, False, target_bid, base_amount)
        if err:
            debug_log("⚠️ Bid-Order fehlgeschlagen", {"error": str(err)})
        else:
            STATE["resting_bid"] = {"price": target_bid, "placed_at": now}
    else:
        STATE["resting_bid"] = None
    
    if target_ask:
        tx, tx_hash, err = await place_post_only_limit(client, True, target_ask, base_amount)
        if err:
            debug_log("⚠️ Ask-Order fehlgeschlagen", {"error": str(err)})
        else:
            STATE["resting_ask"] = {"price": target_ask, "placed_at": now}
    else:
        STATE["resting_ask"] = None
    
    debug_log("🔄 Requote", {
        "bid": target_bid, 
        "ask": target_ask, 
        "size_btc": base_amount,
        "size_usdc": base_amount * mid,
        "inventory": STATE["inventory"],
        "inventory_usdc": STATE["inventory"] * mid,
        "position_pct": f"{abs(STATE['inventory'] * mid) / MAX_POSITION_USDC * 100:.1f}%"
    })


def check_fills_from_trade_tape(trade_price, trade_received_at):
    """Approximierte Fill-Erkennung."""
    if STATE["is_stopped"] or not trade_price:
        return
    
    order_size_usdc = ORDER_SIZE_USDC_HALVED
    coin_size = order_size_usdc / trade_price if trade_price else 0
    
    if coin_size <= 0:
        return
    
    rb = STATE["resting_bid"]
    if rb and trade_received_at > rb["placed_at"] and trade_price <= rb["price"]:
        record_fill("buy", rb["price"], coin_size)
        STATE["resting_bid"] = None
    
    ra = STATE["resting_ask"]
    if ra and trade_received_at > ra["placed_at"] and trade_price >= ra["price"]:
        record_fill("sell", ra["price"], coin_size)
        STATE["resting_ask"] = None


async def listen():
    global last_trade_price, last_trade_received_at, ORDER_SIZE_USDC_HALVED
    
    ORDER_SIZE_USDC_HALVED = ORDER_SIZE_USDC
    last_status_log = 0.0
    last_account_sync = 0.0
    
    client = get_lighter_client() if not DRY_RUN else None
    
    async with websockets.connect(WS_URL, ping_interval=20) as ws:
        await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{MARKET_INDEX}"}))
        await ws.send(json.dumps({"type": "subscribe", "channel": f"trade/{MARKET_INDEX}"}))
        debug_log(f"✅ Verbunden für {SYMBOL} (Market Index {MARKET_INDEX}) | DRY_RUN={DRY_RUN}")
        
        async for raw in ws:
            msg = json.loads(raw)
            channel = msg.get("channel", "")
            
            if channel.startswith("order_book"):
                apply_order_book_update(msg)
                bb, ba = best_bid_ask()
                if bb is None or ba is None:
                    continue
                mid = (bb + ba) / 2
                obi = calc_obi()
                
                await manage_quotes(client, mid, obi)
                check_fills_from_trade_tape(last_trade_price, last_trade_received_at)
                
                now = time.time()
                if now - last_status_log >= 30 or STATE["is_stopped"]:
                    last_status_log = now
                    position_usdc = STATE["inventory"] * mid
                    debug_log("📊 Market-Maker Status", {
                        "mid_preis": round(mid, 4),
                        "obi": round(obi, 3),
                        "inventory": round(STATE["inventory"], 6),
                        "inventory_usdc": round(position_usdc, 2),
                        "position_limit": MAX_POSITION_USDC,
                        "position_pct": f"{abs(position_usdc) / MAX_POSITION_USDC * 100:.1f}%",
                        "avg_entry_price": round(STATE["avg_entry_price"], 4),
                        "realized_pnl_usdc": round(STATE["realized_pnl_usdc"], 4),
                        "fills_gesamt": STATE["fills"],
                        "stopped": STATE["is_stopped"],
                        "resting_bid": STATE["resting_bid"],
                        "resting_ask": STATE["resting_ask"],
                    })
                
                if not DRY_RUN and now - last_account_sync >= ACCOUNT_SYNC_SECONDS:
                    last_account_sync = now
                    await sync_real_position()
                
                # Wenn gestoppt, nichts weiter tun
                if STATE["is_stopped"]:
                    debug_log("🛑 BOT GESTOPPT - Keine neuen Orders")
                    await cancel_all_real_orders(client)
                    continue
                
            elif channel.startswith("trade"):
                trades = msg.get("trades", [])
                if trades:
                    last_trade_price = float(trades[-1]["price"])
                    last_trade_received_at = time.time()


async def main():
    print("=" * 60)
    print(f"🚀 Market-Making Bot für {SYMBOL}")
    print(f"   DRY_RUN: {DRY_RUN} | Margin: {MARGIN} USDC | Hebel: {LEVERAGE}x")
    print(f"   Spread: {SPREAD_PCT}% | Requote alle {REQUOTE_SECONDS}s")
    print(f"   Max Position: {MAX_POSITION_USDC} USDC (HARTES LIMIT)")
    print(f"   Order Size: {ORDER_SIZE_USDC} USDC pro Order")
    print(f"   Stop-Loss: -{MAX_LOSS_USDC} USDC")
    print(f"   OBI-Skew: {OBI_SKEW_ENABLED}")
    if not DRY_RUN:
        print("   ⚠️  LIVE-MODUS - platziert echte Post-Only Orders!")
    print("=" * 60)
    
    while True:
        try:
            await listen()
        except Exception as e:
            debug_log("⚠️ Verbindung verloren, reconnect in 5s", {"error": str(e), "traceback": traceback.format_exc()})
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
