"""
Autonomer Orderbuch-Signal-Bot für Lighter (zkLighter) - ULTRA-SCHNELL
================================================================================
- Sofort-Reaktion auf OBI (0 Ticks Bestätigung)
- Preis-Momentum Erkennung
- Stop-Loss bei 0.2%
- Profit-Target bei $0.50
- Korrekte Reverse-Logik (wechselt Richtung, statt nur zu adden)
"""

import asyncio
import websockets
import json
import time
import os
import traceback
from collections import deque
from datetime import datetime

# ========== BASE_URL ==========
BASE_URL = "https://mainnet.zklighter.elliot.ai"
WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"

# ========== DEBUG ==========
DEBUG_MODE = os.getenv("DEBUG_MODE", "true").lower() == "true"

def debug_log(msg, data=None):
    if DEBUG_MODE:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        print(f"[DEBUG {timestamp}] {msg}")
        if data:
            print(f"   DATA: {json.dumps(data, indent=2, default=str)}")

# ========== MARKET INDICES ==========
MARKET_INDICES = {
    "ETH": 0, "BTC": 1, "SOL": 2, "DOGE": 3, "1000PEPE": 4,
    "WIF": 5, "WLD": 6, "XRP": 7, "LINK": 8, "AVAX": 9,
    "NEAR": 10, "DOT": 11, "TON": 12, "TAO": 13, "POL": 14,
    "TRUMP": 15, "SUI": 16, "1000SHIB": 17, "1000BONK": 18,
    "1000FLOKI": 19, "BERA": 20, "FARTCOIN": 21, "AI16Z": 22,
    "POPCAT": 23, "HYPE": 24, "BNB": 25, "JUP": 26,
    "AAVE": 27, "MKR": 28, "ENA": 29, "UNI": 30,
    "APT": 31, "SEI": 32, "KAITO": 33, "DATA": 34,
    "LTC": 35, "CRV": 36, "PENDLE": 37, "ONDO": 38,
    "ADA": 39, "S": 40, "VIRTUAL": 41, "SPX": 42,
    "TRX": 43, "SYRUP": 44, "PUMP": 45, "LDO": 46,
    "PENGU": 47, "PAXG": 48, "EIGEN": 49, "ARB": 50,
    "XLM": 119,
}

# ========== COIN-PARAMETER ==========
def get_precision(symbol):
    precision_map = {
        "BTC": 100000, "ETH": 10000, "SOL": 1000, "DOGE": 1, "XRP": 1,
        "LINK": 10, "AVAX": 100, "NEAR": 10, "DOT": 10, "BNB": 100,
        "SUI": 10, "ADA": 10, "ARB": 10, "OP": 10, "XLM": 10,
        "WIF": 10, "WLD": 10, "TON": 10, "JUP": 10, "ENA": 10,
        "SEI": 10, "ONDO": 10, "CRV": 10, "LDO": 10, "EIGEN": 10,
    }
    return precision_map.get(symbol, 10000)

def get_price_decimals(symbol):
    decimals_map = {
        "BTC": 1, "ETH": 2, "SOL": 3, "LTC": 3, "BCH": 3,
        "AVAX": 3, "BNB": 4, "UNI": 4, "APT": 4, "PENDLE": 4,
        "LINK": 5, "NEAR": 5, "DOT": 5, "SUI": 5, "ADA": 5,
        "ARB": 5, "OP": 5, "WIF": 5, "WLD": 5, "TON": 5,
        "JUP": 5, "ENA": 5, "SEI": 5, "ONDO": 5, "CRV": 5,
        "XLM": 5, "DOGE": 6, "XRP": 6,
    }
    return decimals_map.get(symbol, 2)

def get_min_base_amount(symbol):
    min_amount_map = {
        "BTC": 0.00020, "ETH": 0.005, "SOL": 0.05, "DOGE": 10, "XRP": 20,
        "LINK": 1.0, "AVAX": 0.5, "NEAR": 2.0, "DOT": 2.0, "BNB": 0.02,
        "SUI": 3.0, "ADA": 10.0, "ARB": 20.0, "OP": 10.0, "XLM": 30,
    }
    return min_amount_map.get(symbol, 0.001)

# ========== PARAMETER ==========
SYMBOL = os.getenv("OB_SYMBOL", "SOL")
if SYMBOL not in MARKET_INDICES:
    raise ValueError(f"Symbol {SYMBOL} nicht in MARKET_INDICES")
MARKET_INDEX = MARKET_INDICES[SYMBOL]

OBI_LEVELS = int(os.getenv("OBI_LEVELS", "10"))
OBI_THRESHOLD = float(os.getenv("OBI_THRESHOLD", "0.25"))
COOLDOWN_SECONDS = float(os.getenv("COOLDOWN_SECONDS", "2"))
MARGIN = float(os.getenv("OB_MARGIN", "30"))
LEVERAGE = int(os.getenv("OB_LEVERAGE", "20"))
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
STOP_LOSS_PERCENT = float(os.getenv("STOP_LOSS_PERCENT", "0.002"))
PROFIT_TARGET_USDC = float(os.getenv("PROFIT_TARGET_USDC", "0.50"))

# ========== LIGHTER CLIENT ==========
def get_lighter_client():
    try:
        import lighter
        API_KEY_INDEX = int(os.getenv("API_KEY_INDEX", "5"))
        PRIVATE_KEY = os.getenv("PRIVATE_KEY")
        ACCOUNT_INDEX = int(os.getenv("ACCOUNT_INDEX", "50960"))
        client = lighter.SignerClient(
            url=BASE_URL,
            api_private_keys={API_KEY_INDEX: PRIVATE_KEY},
            account_index=ACCOUNT_INDEX
        )
        return client
    except Exception as e:
        debug_log("Lighter Client Fehler", {"error": str(e)})
        return None

async def create_order_with_price(client, market_index, base_amount, is_ask, symbol, price, reduce_only=False):
    price_decimals = get_price_decimals(symbol)
    adjusted_price = price * 0.95 if is_ask else price * 1.05
    price_scaled = int(adjusted_price * (10 ** price_decimals))

    tx, tx_hash, err = await client.create_order(
        market_index=market_index,
        client_order_index=int(time.time() * 1000),
        base_amount=base_amount,
        price=price_scaled,
        is_ask=is_ask,
        order_type=client.ORDER_TYPE_MARKET,
        time_in_force=client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
        reduce_only=reduce_only,
        order_expiry=client.DEFAULT_IOC_EXPIRY,
    )
    return tx, tx_hash, err

async def open_or_reverse_position(action, symbol, margin, leverage, current_price):
    """Öffnet, reversed oder addet zu einer Position"""
    client = get_lighter_client()
    if client is None:
        return {"error": "Client konnte nicht initialisiert werden"}

    try:
        market_index = MARKET_INDICES[symbol]
        precision = get_precision(symbol)

        position_usdc = margin * leverage
        coin_amount = position_usdc / current_price
        base_amount = int(coin_amount * precision)

        if base_amount == 0:
            return {"error": f"Base Amount ist 0 für {symbol}"}

        new_side = "long" if action == "buy" else "short"
        new_is_ask = action != "buy"

        try:
            await client.update_leverage(market_index=market_index, leverage=leverage, margin_mode=0)
        except:
            pass

        await asyncio.sleep(0.5)

        if symbol in OPEN_POSITIONS:
            existing_pos = OPEN_POSITIONS[symbol]
            
            # ===== GLEICHE RICHTUNG =====
            if existing_pos["side"] == new_side:
                tx, tx_hash, err = await create_order_with_price(
                    client, market_index, base_amount, new_is_ask, symbol, current_price, reduce_only=False
                )
                if err:
                    return {"error": f"Nachkauf fehlgeschlagen: {err}"}
                
                # Position aktualisieren
                old_coin = existing_pos["coin_amount"]
                old_price = existing_pos["open_price"]
                total_value = (old_price * old_coin) + (current_price * coin_amount)
                avg_price = total_value / (old_coin + coin_amount)
                
                existing_pos["position_usdc"] += position_usdc
                existing_pos["coin_amount"] += coin_amount
                existing_pos["base_amount"] += base_amount
                existing_pos["margin"] += margin
                existing_pos["open_price"] = avg_price
                
                return {"success": True, "action": "add_to_position", "side": new_side, "tx_hash": str(tx_hash)}
            
            # ===== ANDERE RICHTUNG =====
            else:
                # 1. Alte Position schließen
                close_is_ask = existing_pos["side"] == "long"
                tx1, tx_hash1, err1 = await create_order_with_price(
                    client, market_index, existing_pos["base_amount"], close_is_ask, symbol,
                    existing_pos["open_price"], reduce_only=True
                )
                if err1:
                    return {"error": f"Close fehlgeschlagen: {err1}"}

                await asyncio.sleep(1)

                # 2. Neue Position eröffnen
                tx2, tx_hash2, err2 = await create_order_with_price(
                    client, market_index, base_amount, new_is_ask, symbol, current_price, reduce_only=False
                )
                if err2:
                    OPEN_POSITIONS.pop(symbol, None)
                    return {"error": f"Open fehlgeschlagen: {err2}"}

                # 3. Position speichern
                OPEN_POSITIONS[symbol] = {
                    "side": new_side,
                    "position_usdc": position_usdc,
                    "coin_amount": coin_amount,
                    "base_amount": base_amount,
                    "margin": margin,
                    "leverage": leverage,
                    "open_price": current_price,
                    "open_time": datetime.now().isoformat()
                }
                return {"success": True, "action": "reverse", "to_side": new_side, "tx_hash": str(tx_hash2)}
        
        # ===== NEUE POSITION =====
        else:
            tx, tx_hash, err = await create_order_with_price(
                client, market_index, base_amount, new_is_ask, symbol, current_price, reduce_only=False
            )
            if err:
                return {"error": str(err)}

            OPEN_POSITIONS[symbol] = {
                "side": new_side,
                "position_usdc": position_usdc,
                "coin_amount": coin_amount,
                "base_amount": base_amount,
                "margin": margin,
                "leverage": leverage,
                "open_price": current_price,
                "open_time": datetime.now().isoformat()
            }
            return {"success": True, "action": "open", "side": new_side, "tx_hash": str(tx_hash)}

    except Exception as e:
        debug_log("Exception", {"error": str(e)})
        return {"error": str(e)}
    finally:
        await client.close()

async def close_position(symbol, percent=100, current_price=None):
    """Schließt eine Position"""
    client = get_lighter_client()
    if client is None:
        return {"error": "Client nicht verfügbar"}

    try:
        market_index = MARKET_INDICES[symbol]
        if symbol not in OPEN_POSITIONS:
            return {"error": f"Keine Position für {symbol}"}

        pos = OPEN_POSITIONS[symbol]
        if current_price is None:
            current_price = pos["open_price"]

        close_percent = percent / 100.0
        close_base_amount = int(pos["base_amount"] * close_percent)

        if close_base_amount == 0:
            return {"error": "Close Base Amount ist 0"}

        is_ask = pos["side"] == "long"
        tx, tx_hash, err = await create_order_with_price(
            client, market_index, close_base_amount, is_ask, symbol, current_price, reduce_only=True
        )

        if err:
            return {"error": str(err)}

        if percent >= 100:
            del OPEN_POSITIONS[symbol]
        else:
            OPEN_POSITIONS[symbol]["base_amount"] -= close_base_amount

        return {"success": True, "action": "close", "tx_hash": str(tx_hash)}

    except Exception as e:
        return {"error": str(e)}
    finally:
        await client.close()

# ========== GLOBAL STATE ==========
OPEN_POSITIONS = {}
order_book = {"bids": {}, "asks": {}}
last_trade_price = None
last_trade_time = 0.0
price_history = deque(maxlen=5)

def apply_order_book_update(msg):
    """Wendet Orderbuch-Update an"""
    ob = msg.get("order_book", {})
    for side_key, book in (("bids", order_book["bids"]), ("asks", order_book["asks"])):
        for level in ob.get(side_key, []):
            price = level["price"]
            size = float(level["size"])
            if size == 0:
                book.pop(price, None)
            else:
                book[price] = size

def calc_obi(levels=OBI_LEVELS):
    """Berechnet Order Book Imbalance"""
    bids_sorted = sorted(order_book["bids"].items(), key=lambda x: float(x[0]), reverse=True)[:levels]
    asks_sorted = sorted(order_book["asks"].items(), key=lambda x: float(x[0]))[:levels]
    bid_vol = sum(v for _, v in bids_sorted)
    ask_vol = sum(v for _, v in asks_sorted)
    total = bid_vol + ask_vol
    return 0.0 if total == 0 else (bid_vol - ask_vol) / total

async def check_stop_loss_and_profit(symbol, current_price):
    """Prüft Stop-Loss und Profit-Ziel"""
    if symbol not in OPEN_POSITIONS:
        return False
    
    pos = OPEN_POSITIONS[symbol]
    entry = pos["open_price"]
    side = pos["side"]
    
    if side == "long":
        profit_percent = (current_price - entry) / entry
        profit_usdc = (current_price - entry) * pos["coin_amount"]
    else:
        profit_percent = (entry - current_price) / entry
        profit_usdc = (entry - current_price) * pos["coin_amount"]
    
    # Stop-Loss
    if profit_percent < -STOP_LOSS_PERCENT:
        debug_log(f"🛑 STOP-LOSS", {
            "loss": round(profit_usdc, 3),
            "percent": round(profit_percent * 100, 2)
        })
        if not DRY_RUN:
            await close_position(symbol, 100, current_price)
        return True
    
    # Profit-Target
    if profit_usdc >= PROFIT_TARGET_USDC:
        debug_log(f"🎯 PROFIT", {
            "profit": round(profit_usdc, 3),
            "percent": round(profit_percent * 100, 2)
        })
        if not DRY_RUN:
            await close_position(symbol, 100, current_price)
        return True
    
    return False

async def execute_signal(direction, price):
    """Führt Trading-Signal aus"""
    global last_trade_time
    
    now = time.time()
    
    # Cooldown
    if now - last_trade_time < COOLDOWN_SECONDS:
        return
    
    # ===== PRÜFE AKTUELLE POSITION =====
    current_side = None
    if SYMBOL in OPEN_POSITIONS:
        current_side = OPEN_POSITIONS[SYMBOL]["side"]
    
    # Schon in dieser Richtung? → Überspringen
    if current_side == direction:
        return
    
    debug_log(f"⚡ TRADE: {direction.upper()} {SYMBOL} @ {price}")
    
    if DRY_RUN:
        last_trade_time = now
        return
    
    # Trade ausführen
    result = await open_or_reverse_position(direction, SYMBOL, MARGIN, LEVERAGE, price)
    debug_log("Order", result)
    
    if result.get("success"):
        last_trade_time = now

async def listen():
    """Haupt-WebSocket-Loop"""
    global last_trade_price, last_trade_time
    
    last_status_log = 0.0
    STATUS_INTERVAL = 5
    counter = 0
    
    async with websockets.connect(WS_URL, ping_interval=20) as ws:
        # Subscribe
        await ws.send(json.dumps({
            "type": "subscribe", 
            "channel": f"order_book/{MARKET_INDEX}"
        }))
        await ws.send(json.dumps({
            "type": "subscribe", 
            "channel": f"trade/{MARKET_INDEX}"
        }))
        
        debug_log(f"✅ Verbunden, abonniert order_book/{MARKET_INDEX} und trade/{MARKET_INDEX}")
        
        async for raw in ws:
            try:
                msg = json.loads(raw)
                channel = msg.get("channel", "")
                
                counter += 1
                if counter % 20 == 0:
                    debug_log(f"📨 Nachricht #{counter}", {
                        "channel": channel,
                        "type": msg.get("type", "")
                    })
                
                # ===== ORDER BUCH =====
                if "order_book" in channel:
                    apply_order_book_update(msg)
                    obi = calc_obi()
                    
                    if last_trade_price is not None:
                        # Stop-Loss & Profit prüfen
                        await check_stop_loss_and_profit(SYMBOL, last_trade_price)
                        
                        # OBI-Signal (Sofort-Reaktion!)
                        if obi > OBI_THRESHOLD:
                            await execute_signal("buy", last_trade_price)
                        elif obi < -OBI_THRESHOLD:
                            await execute_signal("sell", last_trade_price)
                        
                        # Preis-Momentum
                        price_history.append(last_trade_price)
                        if len(price_history) >= 3:
                            price_change = (price_history[-1] - price_history[-3]) / price_history[-3]
                            if price_change > 0.003:  # 0.3% Aufwärts
                                await execute_signal("buy", last_trade_price)
                            elif price_change < -0.003:  # 0.3% Abwärts
                                await execute_signal("sell", last_trade_price)
                    
                    # Status-Log
                    now = time.time()
                    if now - last_status_log >= STATUS_INTERVAL:
                        last_status_log = now
                        
                        # Aktuelle Position
                        current_side = None
                        if SYMBOL in OPEN_POSITIONS:
                            current_side = OPEN_POSITIONS[SYMBOL]["side"]
                        
                        if obi > 0.05:
                            lean = "Käufer" if obi < OBI_THRESHOLD else "Käufer STARK"
                        elif obi < -0.05:
                            lean = "Verkäufer" if obi > -OBI_THRESHOLD else "Verkäufer STARK"
                        else:
                            lean = "ausgeglichen"
                        
                        debug_log(f"📊 {SYMBOL}", {
                            "OBI": round(obi, 3),
                            "richtung": lean,
                            "schwelle": OBI_THRESHOLD,
                            "preis": last_trade_price,
                            "position": current_side or "flach",
                            "offen": SYMBOL in OPEN_POSITIONS,
                            "bids": len(order_book["bids"]),
                            "asks": len(order_book["asks"])
                        })
                
                # ===== TRADES =====
                elif "trade" in channel:
                    trades = msg.get("trades", [])
                    if trades:
                        last_trade_price = float(trades[-1]["price"])
                        
            except json.JSONDecodeError:
                pass
            except Exception as e:
                debug_log("⚠️ Fehler", {"error": str(e)})

async def main():
    """Main Loop"""
    print("=" * 60)
    print(f"⚡ ULTRA-SCHNELLER BOT für {SYMBOL}")
    print(f"   DRY_RUN: {DRY_RUN}")
    print(f"   OBI Schwelle: {OBI_THRESHOLD}")
    print(f"   Cooldown: {COOLDOWN_SECONDS}s")
    print(f"   Stop-Loss: {STOP_LOSS_PERCENT*100}%")
    print(f"   Profit-Ziel: ${PROFIT_TARGET_USDC}")
    print(f"   Margin: ${MARGIN} | Hebel: {LEVERAGE}x")
    print(f"   KEINE Bestätigungs-Ticks! Sofort-Reaktion!")
    print("=" * 60)
    
    while True:
        try:
            await listen()
        except Exception as e:
            debug_log("⚠️ Reconnect in 3s", {"error": str(e)})
            await asyncio.sleep(3)

if __name__ == "__main__":
    asyncio.run(main())
