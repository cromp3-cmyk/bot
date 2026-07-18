"""
Market-Making Bot für Lighter (zkLighter)
============================================
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
SYMBOL = os.getenv("MM_SYMBOL", "SOL")
if SYMBOL not in MARKET_INDICES:
    raise ValueError(f"Symbol {SYMBOL} nicht in MARKET_INDICES - hier ergänzen")
MARKET_INDEX = MARKET_INDICES[SYMBOL]

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

MARGIN = float(os.getenv("MM_MARGIN", "10"))
LEVERAGE = int(os.getenv("MM_LEVERAGE", "10"))

SPREAD_PCT = float(os.getenv("MM_SPREAD_PCT", "0.08"))
REQUOTE_SECONDS = float(os.getenv("MM_REQUOTE_SECONDS", "5"))
REQUOTE_MOVE_THRESHOLD_PCT = float(os.getenv("MM_REQUOTE_MOVE_PCT", "0.03"))

MAX_POSITION_USDC = float(os.getenv("MM_MAX_POSITION_USDC", "100"))
ORDER_SIZE_USDC = float(os.getenv("MM_ORDER_SIZE_USDC", "10.0"))

OBI_SKEW_ENABLED = os.getenv("MM_OBI_SKEW_ENABLED", "true").lower() == "true"
OBI_LEVELS = int(os.getenv("MM_OBI_LEVELS", "15"))
OBI_SKEW_FACTOR = float(os.getenv("MM_OBI_SKEW_FACTOR", "0.02"))

ACCOUNT_SYNC_SECONDS = float(os.getenv("MM_ACCOUNT_SYNC_SECONDS", "120"))
MAX_LOSS_USDC = float(os.getenv("MM_MAX_LOSS_USDC", "3.0"))


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
    
    # 🔥 FIX: base_amount muss ein Integer sein!
    precision = get_precision(SYMBOL)
    base_amount_scaled = int(base_amount * precision)

    tx, tx_hash, err = await client.create_order(
        market_index=MARKET_INDEX,
        client_order_index=int(time.time() * 1000),
        base_amount=base_amount_scaled,
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
                    debug_log("🔎 Echter Positionsabgleich (Lighter-API)", {
                        "echte_position_size": real_size,
                        "unsere_buchhaltung_inventory": STATE["inventory"],
                        "differenz": round(real_size - STATE["inventory"], 6),
                    })
                    return
            debug_log("🔎 Echter Positionsabgleich: keine offene Position auf Lighter gefunden", {
                "unsere_buchhaltung_inventory": STATE["inventory"],
            })
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
    "is_stopped": False,
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
    if STATE["realized_pnl_usdc"] < -MAX_LOSS_USDC:
        debug_log(f"🚨 STOP-LOSS AUSGELÖST: {STATE['realized_pnl_usdc']:.2f} USDC")
        STATE["is_stopped"] = True
        return False
    
    current_position_usdc = abs(STATE["inventory"]) * mid
    if current_position_usdc >= MAX_POSITION_USDC * 1.1:
        debug_log(f"🚨 MAX POSITION ÜBERSCHRITTEN: {current_position_usdc:.2f} USDC")
        STATE["is_stopped"] = True
        return False
    
    if mid < 1 or mid > 1000000:
        debug_log(f"🚨 UNGÜLTIGER PREIS: {mid}")
        STATE["is_stopped"] = True
        return False
    
    return True


def compute_target_quotes(mid, obi):
    if STATE["is_stopped"]:
        return None, None
    
    check_safety_conditions(mid)
    
    skew = (obi * OBI_SKEW_FACTOR * mid) if OBI_SKEW_ENABLED else 0.0

    bid_price = mid * (1 - SPREAD_PCT / 100) + skew
    ask_price = mid * (1 + SPREAD_PCT / 100) + skew

    position_usdc = STATE["inventory"] * mid
    quote_bid = True
    quote_ask = True

    if position_usdc >= MAX_POSITION_USDC:
        quote_bid = False
    elif position_usdc <= -MAX_POSITION_USDC:
        quote_ask = False

    return (bid_price if quote_bid else None), (ask_price if quote_ask else None)


def record_fill(side, price, base_amount):
    if STATE["is_stopped"]:
        return
    
    old_inventory = STATE["inventory"]

    if side == "buy":
        if old_inventory < 0:
            closed_amount = min(base_amount, -old_inventory)
            pnl = (STATE["avg_entry_price"] - price) * closed_amount
            STATE["realized_pnl_usdc"] += pnl
        new_inventory = old_inventory + base_amount
    else:
        if old_inventory > 0:
            closed_amount = min(base_amount, old_inventory)
            pnl = (price - STATE["avg_entry_price"]) * closed_amount
            STATE["realized_pnl_usdc"] += pnl
        new_inventory = old_inventory - base_amount

    if (old_inventory >= 0 and side == "buy") or (old_inventory <= 0 and side == "sell"):
        total_value = STATE["avg_entry_price"] * abs(old_inventory) + price * base_amount
        STATE["avg_entry_price"] = total_value / (abs(old_inventory) + base_amount) if (abs(old_inventory) + base_amount) > 0 else price
    elif new_inventory != 0 and (old_inventory >= 0) != (new_inventory >= 0):
        STATE["avg_entry_price"] = price

    STATE["inventory"] = new_inventory
    STATE["fills"] += 1

    debug_log(f"✅ FILL: {side.upper()} {base_amount:.6f} {SYMBOL} @ {price:.4f}", {
        "neues_inventory": round(new_inventory, 6),
        "avg_entry_price": round(STATE["avg_entry_price"], 4),
        "realized_pnl_usdc": round(STATE["realized_pnl_usdc"], 4),
    })


def order_size_base_amount(mid):
    """Positionsgroesse pro Quote-Seite, in Coin-Einheiten."""
    order_size_usdc = ORDER_SIZE_USDC
    
    min_coin = get_min_base_amount(SYMBOL)
    min_usdc = min_coin * mid
    
    if order_size_usdc < min_usdc:
        debug_log(f"⚠️ ORDER_SIZE_USDC ({order_size_usdc} USDC) unter Minimum ({min_usdc:.2f} USDC)")
        order_size_usdc = min_usdc
        debug_log(f"   → Erhöht auf Minimum: {order_size_usdc:.2f} USDC")
    
    current_position_usdc = abs(STATE["inventory"]) * mid
    if current_position_usdc + order_size_usdc > MAX_POSITION_USDC:
        debug_log(f"⚠️ Order ({order_size_usdc:.2f} USDC) würde Max-Position ({MAX_POSITION_USDC:.0f} USDC) überschreiten")
        order_size_usdc = max(0, MAX_POSITION_USDC - current_position_usdc)
        if order_size_usdc < min_usdc:
            debug_log(f"⚠️ Kein Platz für Order (benötigt {min_usdc:.2f} USDC, verfügbar {order_size_usdc:.2f} USDC)")
            return 0
    
    return order_size_usdc / mid


async def manage_quotes(client, mid, obi):
    if STATE["is_stopped"]:
        if not DRY_RUN:
            await cancel_all_real_orders(client)
        STATE["resting_bid"] = None
        STATE["resting_ask"] = None
        return
    
    target_bid, target_ask = compute_target_quotes(mid, obi)
    now = time.time()

    need_requote = STATE["last_mid"] is None or abs(mid - STATE["last_mid"]) / mid * 100 >= REQUOTE_MOVE_THRESHOLD_PCT
    time_based = (STATE["resting_bid"] is None or now - STATE["resting_bid"]["placed_at"] >= REQUOTE_SECONDS) and \
                 (STATE["resting_ask"] is None or now - STATE["resting_ask"]["placed_at"] >= REQUOTE_SECONDS)

    if not (need_requote or time_based):
        return

    STATE["last_mid"] = mid
    
    base_amount = order_size_base_amount(mid)
    if base_amount == 0:
        return

    if DRY_RUN:
        STATE["resting_bid"] = {"price": target_bid, "placed_at": now} if target_bid else None
        STATE["resting_ask"] = {"price": target_ask, "placed_at": now} if target_ask else None
        return

    # Echtes Requoting: alte Orders canceln, neue setzen
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

    debug_log("🔄 Requote", {"bid": target_bid, "ask": target_ask, "inventory": STATE["inventory"]})


def check_fills_from_trade_tape(trade_price, trade_received_at):
    if STATE["is_stopped"] or not trade_price:
        return

    coin_size = ORDER_SIZE_USDC / trade_price if trade_price else 0

    rb = STATE["resting_bid"]
    if rb and trade_received_at > rb["placed_at"] and trade_price <= rb["price"]:
        record_fill("buy", rb["price"], coin_size)
        STATE["resting_bid"] = None

    ra = STATE["resting_ask"]
    if ra and trade_received_at > ra["placed_at"] and trade_price >= ra["price"]:
        record_fill("sell", ra["price"], coin_size)
        STATE["resting_ask"] = None


async def listen():
    global last_trade_price, last_trade_received_at

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
                
                if STATE["is_stopped"]:
                    debug_log("🛑 BOT GESTOPPT - Keine neuen Orders")
                    if not DRY_RUN:
                        await cancel_all_real_orders(client)

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
    print(f"   Max Position: {MAX_POSITION_USDC:.0f} USDC | OBI-Skew: {OBI_SKEW_ENABLED}")
    print(f"   Order Size: {ORDER_SIZE_USDC:.1f} USDC | Stop-Loss: {MAX_LOSS_USDC:.1f} USDC")
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
