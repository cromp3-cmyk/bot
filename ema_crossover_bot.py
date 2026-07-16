"""
EMA 20/100 Crossover Bot für Lighter - MIT WEBSOCKET CANDLES
==================================================================================
Test: EMA20 und EMA100 – sollten weit auseinander sein → kein Trade!
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
WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"

# ========== DEBUG ==========
DEBUG_MODE = os.getenv("DEBUG_MODE", "true").lower() == "true"

def debug_log(msg, data=None):
    if DEBUG_MODE:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        print(f"[DEBUG {timestamp}] {msg}", flush=True)
        if data:
            print(f"   DATA: {json.dumps(data, indent=2, default=str)}", flush=True)

# ========== MARKET INDICES ==========
MARKET_INDICES = {
    "ETH": 0, "BTC": 1, "SOL": 2, "DOGE": 3, "1000PEPE": 4,
    "WIF": 5, "WLD": 6, "XRP": 7, "LINK": 8, "AVAX": 9,
    "NEAR": 10, "DOT": 11, "TON": 12, "TAO": 13, "POL": 14,
    "TRUMP": 15, "SUI": 16, "XLM": 119,
}

def get_precision(symbol):
    precision_map = {
        "BTC": 100000, "ETH": 10000, "SOL": 1000, "AVAX": 100,
        "LINK": 10, "NEAR": 10, "DOT": 10, "SUI": 10,
        "DOGE": 1, "XRP": 1, "POL": 1,
    }
    return precision_map.get(symbol, 10000)

def get_price_decimals(symbol):
    decimals_map = {
        "BTC": 1, "ETH": 2, "SOL": 3, "AVAX": 3,
        "LINK": 5, "NEAR": 5, "DOT": 5, "SUI": 5,
        "DOGE": 6, "XRP": 6, "POL": 6,
    }
    return decimals_map.get(symbol, 2)

def get_min_base_amount(symbol):
    min_amount_map = {
        "BTC": 0.00020, "ETH": 0.005, "SOL": 0.05, "AVAX": 0.5,
        "LINK": 1.0, "NEAR": 2.0, "DOT": 2.0, "SUI": 3.0,
        "DOGE": 10, "XRP": 20,
    }
    return min_amount_map.get(symbol, 0.001)

def get_lighter_client():
    try:
        import lighter
        API_KEY_INDEX = int(os.getenv("API_KEY_INDEX", "5"))
        PRIVATE_KEY = os.getenv("PRIVATE_KEY")
        ACCOUNT_INDEX = int(os.getenv("ACCOUNT_INDEX", "50960"))
        client = lighter.SignerClient(
            url="https://mainnet.zklighter.elliot.ai",
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

async def get_current_position(symbol):
    try:
        import lighter
        account_index = int(os.getenv("ACCOUNT_INDEX", "50960"))
        api_client = lighter.ApiClient(configuration=lighter.Configuration(host="https://mainnet.zklighter.elliot.ai"))
        account_api = lighter.AccountApi(api_client)

        response = await account_api.account(by="index", value=str(account_index))
        accounts = getattr(response, "accounts", None) or []
        if not accounts:
            return None

        positions = getattr(accounts[0], "positions", []) or []
        market_index = MARKET_INDICES[symbol]

        for pos in positions:
            if getattr(pos, "market_index", None) != market_index:
                continue
            size = float(getattr(pos, "position", 0) or 0)
            if size == 0:
                continue

            side = "long" if size > 0 else "short"
            open_price = float(getattr(pos, "avg_entry_price", 0) or 0)
            base_amount = int(abs(size) * get_precision(symbol))

            return {
                "side": side,
                "base_amount": base_amount,
                "open_price": open_price,
                "size": abs(size)
            }
        return None
    except Exception as e:
        debug_log("Fehler beim Abrufen der Position", {"error": str(e)})
        return None

async def open_or_reverse_position(action, symbol, margin, leverage, current_price):
    client = get_lighter_client()
    if client is None:
        return {"error": "Client konnte nicht initialisiert werden"}

    try:
        market_index = MARKET_INDICES[symbol]
        precision = get_precision(symbol)
        min_base_amount = get_min_base_amount(symbol)

        position_usdc = margin * leverage
        coin_amount = position_usdc / current_price
        base_amount = int(coin_amount * precision)

        if base_amount == 0:
            min_margin_needed = (min_base_amount * current_price) / leverage
            return {
                "error": f"Base Amount ist 0 für {symbol}",
                "suggestion": f"Erhöhe Margin auf mindestens {min_margin_needed:.2f} USDC"
            }

        new_side = "long" if action == "buy" else "short"
        new_is_ask = action != "buy"

        try:
            await client.update_leverage(market_index=market_index, leverage=leverage, margin_mode=0)
        except Exception as e:
            debug_log("Hebel setzen fehlgeschlagen", {"error": str(e)})

        await asyncio.sleep(1)

        current_pos = await get_current_position(symbol)

        if current_pos:
            if current_pos["side"] == new_side:
                debug_log(f"⏭️ Bereits {new_side}, ignoriere")
                return {"success": True, "action": "ignoriert", "side": new_side}

            debug_log(f"🔄 Wechsel von {current_pos['side']} zu {new_side}")

            close_is_ask = current_pos["side"] == "long"
            tx1, tx_hash1, err1 = await create_order_with_price(
                client, market_index, current_pos["base_amount"], close_is_ask, symbol,
                current_pos["open_price"], reduce_only=True
            )
            if err1:
                return {"error": f"Close fehlgeschlagen: {err1}"}

            debug_log(f"✅ {current_pos['side']} geschlossen")
            await asyncio.sleep(1)

            tx2, tx_hash2, err2 = await create_order_with_price(
                client, market_index, base_amount, new_is_ask, symbol, current_price, reduce_only=False
            )
            if err2:
                return {"error": f"Open fehlgeschlagen: {err2}"}

            debug_log(f"✅ {new_side} eröffnet")
            return {"success": True, "action": "reverse", "to_side": new_side, "tx_hash": str(tx_hash2)}

        else:
            debug_log(f"🆕 Keine Position, eröffne {new_side}")
            tx, tx_hash, err = await create_order_with_price(
                client, market_index, base_amount, new_is_ask, symbol, current_price, reduce_only=False
            )
            if err:
                return {"error": str(err)}

            debug_log(f"✅ {new_side} eröffnet")
            return {"success": True, "action": "open", "side": new_side, "tx_hash": str(tx_hash)}

    except Exception as e:
        debug_log("Exception", {"error": str(e), "traceback": traceback.format_exc()})
        return {"error": str(e)}
    finally:
        await client.close()

# ========== EMA CALCULATOR (KORREKT) ==========
class EMACalculator:
    def __init__(self, period):
        self.period = period
        self.closes = deque(maxlen=period * 2)
        self.ema = None
        self.is_initialized = False
        self.sma_calculated = False

    def add_candle(self, close_price):
        self.closes.append(close_price)
        
        if len(self.closes) == self.period:
            # Erster EMA = SMA
            self.ema = sum(self.closes) / self.period
            self.is_initialized = True
            self.sma_calculated = True
        elif len(self.closes) > self.period:
            # EMA-Formel
            multiplier = 2 / (self.period + 1)
            self.ema = (close_price - self.ema) * multiplier + self.ema

# ========== KONFIGURATION ==========
SYMBOL = os.getenv("EMA_SYMBOL", "SOL")
if SYMBOL not in MARKET_INDICES:
    raise ValueError(f"Symbol {SYMBOL} nicht in MARKET_INDICES")
MARKET_INDEX = MARKET_INDICES[SYMBOL]

# ===== TEST: EMA20 und EMA100 =====
EMA_FAST_LEN = int(os.getenv("EMA_FAST_LEN", "20"))
EMA_SLOW_LEN = int(os.getenv("EMA_SLOW_LEN", "100"))

MIN_CANDLES = int(os.getenv("MIN_CANDLES", "100"))
MIN_CROSSOVER_DIFF = float(os.getenv("MIN_CROSSOVER_DIFF", "0.02"))

MARGIN = float(os.getenv("EMA_MARGIN", "10"))
LEVERAGE = int(os.getenv("EMA_LEVERAGE", "20"))
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

# ========== LOKALER STATE ==========
last_trade_price = None
current_position_side = None
fast_ema = EMACalculator(EMA_FAST_LEN)
slow_ema = EMACalculator(EMA_SLOW_LEN)
last_crossover_state = None
candle_count = 0

async def execute_signal(direction, price):
    global current_position_side, last_crossover_state

    if current_position_side == direction:
        return

    debug_log(f"📡 SIGNAL: {direction.upper()} {SYMBOL} @ {price}")

    if DRY_RUN:
        debug_log("🧪 DRY_RUN - keine Order")
        current_position_side = direction
        last_crossover_state = "up" if direction == "buy" else "down"
        return

    result = await open_or_reverse_position(direction, SYMBOL, MARGIN, LEVERAGE, price)
    debug_log("Order-Ergebnis", result)

    if result.get("success"):
        if result.get("to_side"):
            current_position_side = result.get("to_side")
        elif result.get("side"):
            current_position_side = result.get("side")
        
        last_crossover_state = "up" if current_position_side == "long" else "down"
        debug_log(f"🔒 Crossover-State gesetzt: {last_crossover_state}")

async def listen():
    global last_trade_price, last_crossover_state, candle_count

    last_status_log = 0.0
    STATUS_LOG_INTERVAL = 10

    async with websockets.connect(WS_URL, ping_interval=20) as ws:
        await ws.send(json.dumps({
            "type": "subscribe",
            "channel": f"mark_price_candle/{MARKET_INDEX}/1m"
        }))
        debug_log(f"✅ Abonniert: mark_price_candle/{MARKET_INDEX}/1m")

        await ws.send(json.dumps({"type": "subscribe", "channel": f"trade/{MARKET_INDEX}"}))
        debug_log(f"✅ Abonniert: trade/{MARKET_INDEX}")

        debug_log(f"📊 Test: EMA{EMA_FAST_LEN} und EMA{EMA_SLOW_LEN}")

        async for raw in ws:
            msg = json.loads(raw)
            channel = msg.get("channel", "")

            if "mark_price_candle" in channel:
                candles = msg.get("candles", [])
                if not candles:
                    continue

                for candle in candles:
                    close = candle.get("c")
                    if close is None:
                        continue
                    
                    fast_ema.add_candle(float(close))
                    slow_ema.add_candle(float(close))
                    candle_count += 1
                    
                    if candle_count < MIN_CANDLES:
                        if candle_count % 10 == 0:
                            debug_log(f"⏳ Sammle Candles... {candle_count}/{MIN_CANDLES}")
                        continue
                    
                    if fast_ema.is_initialized and slow_ema.is_initialized:
                        diff = abs(fast_ema.ema - slow_ema.ema)
                        
                        if diff >= MIN_CROSSOVER_DIFF:
                            current_state = "up" if fast_ema.ema > slow_ema.ema else "down"
                        else:
                            current_state = last_crossover_state
                            debug_log(f"⏭️ EMAs zu nah: {diff:.4f}")
                        
                        if last_crossover_state is not None and current_state != last_crossover_state:
                            if current_state == "up":
                                debug_log(f"📈 CROSSOVER UP: EMA{EMA_FAST_LEN} ({fast_ema.ema:.3f}) > EMA{EMA_SLOW_LEN} ({slow_ema.ema:.3f})")
                                if last_trade_price:
                                    await execute_signal("buy", last_trade_price)
                            else:
                                debug_log(f"📉 CROSSOVER DOWN: EMA{EMA_FAST_LEN} ({fast_ema.ema:.3f}) < EMA{EMA_SLOW_LEN} ({slow_ema.ema:.3f})")
                                if last_trade_price:
                                    await execute_signal("sell", last_trade_price)
                        
                        if last_crossover_state is None:
                            last_crossover_state = current_state
                            debug_log(f"🔒 Initialer Crossover-State: {last_crossover_state}")

            elif "trade" in channel:
                trades = msg.get("trades", [])
                if trades:
                    last_trade_price = float(trades[-1]["price"])

            now = time.time()
            if now - last_status_log >= STATUS_LOG_INTERVAL:
                last_status_log = now
                debug_log(f"📊 Status {SYMBOL}", {
                    "candles": candle_count,
                    f"ema_{EMA_FAST_LEN}": round(fast_ema.ema, 3) if fast_ema.ema else None,
                    f"ema_{EMA_SLOW_LEN}": round(slow_ema.ema, 3) if slow_ema.ema else None,
                    "position": current_position_side or "flach",
                    "crossover": last_crossover_state or "keiner",
                })

async def main():
    global current_position_side, last_crossover_state

    print("=" * 60)
    print(f"🚀 EMA {EMA_FAST_LEN}/{EMA_SLOW_LEN} Crossover Bot für {SYMBOL}")
    print(f"   WebSocket Mark Price Candles (1m)")
    print(f"   Wartet auf {MIN_CANDLES} Candles")
    print(f"   DRY_RUN: {DRY_RUN}")
    print(f"   Margin: {MARGIN} USDC | Hebel: {LEVERAGE}x")
    print("=" * 60)

    if not DRY_RUN:
        pos = await get_current_position(SYMBOL)
        if pos:
            current_position_side = pos["side"]
            last_crossover_state = "up" if current_position_side == "long" else "down"
            debug_log(f"📌 Position: {current_position_side}, Crossover: {last_crossover_state}")

    while True:
        try:
            await listen()
        except Exception as e:
            debug_log("⚠️ Reconnect in 5s", {"error": str(e)})
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())asyncio.run(main())
