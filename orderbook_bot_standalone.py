"""
Autonomer EMA-Crossover-Bot für Lighter (zkLighter)
================================================================================
Einfacher Bot der auf EMA-Crossover handelt (7/21) 
Verbindet sich per WebSocket mit Lighter und tradet autonom
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
    "XLM": 119, "SOL2": 2,
}

# ========== COIN-PARAMETER ==========
def get_precision(symbol):
    precision_map = {
        "BTC": 100000, "ETH": 10000, "SOL": 1000, "DOGE": 1,
        "XRP": 1, "LINK": 10, "AVAX": 100, "NEAR": 10,
        "DOT": 10, "BNB": 100, "SUI": 10, "ADA": 10,
        "ARB": 10, "OP": 10, "XLM": 10,
    }
    return precision_map.get(symbol, 10000)

def get_price_decimals(symbol):
    decimals_map = {
        "BTC": 1, "ETH": 2, "SOL": 3, "DOGE": 6,
        "XRP": 6, "LINK": 5, "AVAX": 3, "NEAR": 5,
        "DOT": 5, "BNB": 4, "SUI": 5, "ADA": 5,
        "ARB": 5, "OP": 5, "XLM": 5,
    }
    return decimals_map.get(symbol, 2)

def get_min_base_amount(symbol):
    min_amount_map = {
        "BTC": 0.00020, "ETH": 0.005, "SOL": 0.05, "DOGE": 10,
        "XRP": 20, "LINK": 1.0, "AVAX": 0.5, "NEAR": 2.0,
        "DOT": 2.0, "BNB": 0.02, "SUI": 3.0, "ADA": 10.0,
        "ARB": 20.0, "OP": 10.0, "XLM": 30,
    }
    return min_amount_map.get(symbol, 0.001)

# ========== EMA CALCULATOR (wie TradingView) ==========
class EMACalculator:
    """Berechnet EMA GENAU WIE TRADINGVIEW"""
    def __init__(self, period):
        self.period = period
        self.prices = deque(maxlen=period * 2)
        self.ema = None
        self.is_initialized = False
        
    def add_price(self, price):
        self.prices.append(price)
        
        if len(self.prices) == self.period:
            # Erster EMA = SMA
            self.ema = sum(self.prices) / self.period
            self.is_initialized = True
        elif len(self.prices) > self.period:
            # EMA Formel (wie TradingView!)
            multiplier = 2 / (self.period + 1)
            self.ema = (price - self.ema) * multiplier + self.ema

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
        debug_log("Lighter Client erstellt", {"api_key_index": API_KEY_INDEX, "account_index": ACCOUNT_INDEX})
        return client
    except Exception as e:
        debug_log("Lighter Client Fehler", {"error": str(e), "traceback": traceback.format_exc()})
        return None

async def create_order_with_price(client, market_index, base_amount, is_ask, symbol, price, reduce_only=False):
    """Erstellt eine Market-Order mit Preis + 5% Slippage-Puffer."""
    price_decimals = get_price_decimals(symbol)

    adjusted_price = price * 0.95 if is_ask else price * 1.05
    price_scaled = int(adjusted_price * (10 ** price_decimals))

    debug_log("Order wird erstellt", {
        "symbol": symbol, "original_price": price, "adjusted_price": adjusted_price,
        "is_ask": is_ask, "base_amount": base_amount
    })

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
    """Öffnet, reversed oder legt auf eine Position nach."""
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

        if symbol in OPEN_POSITIONS:
            existing_pos = OPEN_POSITIONS[symbol]

            if existing_pos["side"] == new_side:
                # Nachkaufen auf bestehende Position (gleiche Richtung)
                tx, tx_hash, err = await create_order_with_price(
                    client, market_index, base_amount, new_is_ask, symbol, current_price, reduce_only=False
                )
                if err:
                    return {"error": f"Nachkauf fehlgeschlagen: {err}"}

                old_coin_amount = existing_pos["coin_amount"]
                old_open_price = existing_pos["open_price"]
                total_value = (old_open_price * old_coin_amount) + (current_price * coin_amount)
                avg_price = total_value / (old_coin_amount + coin_amount)

                existing_pos["position_usdc"] += position_usdc
                existing_pos["coin_amount"] += coin_amount
                existing_pos["base_amount"] += base_amount
                existing_pos["margin"] += margin
                existing_pos["open_price"] = avg_price

                return {"success": True, "action": "add_to_position", "side": new_side, "tx_hash": str(tx_hash)}

            else:
                # Reverse: alte Position schließen, neue eröffnen
                close_is_ask = existing_pos["side"] == "long"
                tx1, tx_hash1, err1 = await create_order_with_price(
                    client, market_index, existing_pos["base_amount"], close_is_ask, symbol,
                    existing_pos["open_price"], reduce_only=True
                )
                if err1:
                    return {"error": f"Close fehlgeschlagen: {err1}"}

                await asyncio.sleep(2)

                tx2, tx_hash2, err2 = await create_order_with_price(
                    client, market_index, base_amount, new_is_ask, symbol, current_price, reduce_only=False
                )
                if err2:
                    OPEN_POSITIONS.pop(symbol, None)
                    return {"error": f"Position geschlossen, aber Open fehlgeschlagen: {err2}"}

                OPEN_POSITIONS[symbol] = {
                    "side": new_side, "position_usdc": position_usdc, "coin_amount": coin_amount,
                    "base_amount": base_amount, "margin": margin, "leverage": leverage,
                    "open_price": current_price, "open_time": datetime.now().isoformat()
                }
                return {"success": True, "action": "reverse", "to_side": new_side, "tx_hash": str(tx_hash2)}

        else:
            tx, tx_hash, err = await create_order_with_price(
                client, market_index, base_amount, new_is_ask, symbol, current_price, reduce_only=False
            )
            if err:
                return {"error": str(err)}

            OPEN_POSITIONS[symbol] = {
                "side": new_side, "position_usdc": position_usdc, "coin_amount": coin_amount,
                "base_amount": base_amount, "margin": margin, "leverage": leverage,
                "open_price": current_price, "open_time": datetime.now().isoformat()
            }
            return {"success": True, "action": "open", "side": new_side, "tx_hash": str(tx_hash)}

    except Exception as e:
        debug_log("Exception in open_or_reverse_position", {"error": str(e), "traceback": traceback.format_exc()})
        return {"error": str(e)}
    finally:
        await client.close()

async def sync_open_position_from_exchange(symbol):
    """Synchronisiert offene Positionen beim Start"""
    try:
        import lighter
        account_index = int(os.getenv("ACCOUNT_INDEX", "50960"))
        api_client = lighter.ApiClient(configuration=lighter.Configuration(host=BASE_URL))
        account_api = lighter.AccountApi(api_client)

        response = await account_api.account(by="index", value=str(account_index))
        accounts = getattr(response, "accounts", None) or []
        if not accounts:
            debug_log("⚠️ Keine Account-Daten beim Sync gefunden - starte mit leerem Positions-State")
            return

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

            OPEN_POSITIONS[symbol] = {
                "side": side,
                "position_usdc": abs(size) * open_price,
                "coin_amount": abs(size),
                "base_amount": int(abs(size) * get_precision(symbol)),
                "margin": abs(size) * open_price / max(int(os.getenv("OB_LEVERAGE", "10")), 1),
                "leverage": int(os.getenv("OB_LEVERAGE", "10")),
                "open_price": open_price,
                "open_time": datetime.now().isoformat(),
            }
            debug_log("✅ Bestehende Position beim Start erkannt", OPEN_POSITIONS[symbol])
            return

        debug_log(f"Keine offene Position für {symbol} beim Start gefunden - starte flach")

    except Exception as e:
        debug_log("⚠️ Positions-Sync fehlgeschlagen - starte mit leerem State", {
            "error": str(e), "traceback": traceback.format_exc()
        })

# ========== State ==========
OPEN_POSITIONS = {}

# ========== Konfiguration ==========
SYMBOL = os.getenv("OB_SYMBOL", "SOL").upper()
if SYMBOL not in MARKET_INDICES:
    raise ValueError(f"Symbol {SYMBOL} nicht in MARKET_INDICES gefunden")
MARKET_INDEX = MARKET_INDICES[SYMBOL]

# EMA Parameter (wie TradingView)
EMA_FAST = int(os.getenv("EMA_FAST", "7"))
EMA_SLOW = int(os.getenv("EMA_SLOW", "21"))

# Trading Parameter
MARGIN = float(os.getenv("OB_MARGIN", "10"))
LEVERAGE = int(os.getenv("OB_LEVERAGE", "20"))
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

COOLDOWN_SECONDS = float(os.getenv("COOLDOWN_SECONDS", "30"))
MIN_HOLD_SECONDS = float(os.getenv("MIN_HOLD_SECONDS", "120"))

# ========== LOKALER STATE ==========
last_trade_price = None
current_position_side = None
position_opened_at = 0.0
last_trade_time = 0.0
last_signal_time = 0
SIGNAL_COOLDOWN = 30  # 30 Sekunden zwischen Signalen

# ========== EMAs ==========
fast_ema = EMACalculator(EMA_FAST)
slow_ema = EMACalculator(EMA_SLOW)

async def execute_signal(direction, price):
    global current_position_side, last_trade_time, position_opened_at, last_signal_time

    now = time.time()
    
    # Cooldown prüfen
    if now - last_signal_time < SIGNAL_COOLDOWN:
        return
    
    if current_position_side == direction:
        return
    
    if current_position_side is not None and (now - position_opened_at) < MIN_HOLD_SECONDS:
        debug_log(f"⏳ Reverse blockiert - Mindesthaltedauer noch nicht erreicht", {
            "aktuelle_position_seit_sekunden": round(now - position_opened_at, 1),
            "min_hold_seconds": MIN_HOLD_SECONDS,
        })
        return

    debug_log(f"📡 EMA-Signal: {direction.upper()} {SYMBOL} @ {price}")

    if DRY_RUN:
        debug_log("🧪 DRY_RUN aktiv - keine echte Order ausgeführt")
        current_position_side = direction
        position_opened_at = now
        last_trade_time = now
        last_signal_time = now
        return

    result = await open_or_reverse_position(direction, SYMBOL, MARGIN, LEVERAGE, price)
    debug_log("Order-Ergebnis", result)

    current_position_side = direction
    position_opened_at = now
    last_trade_time = now
    last_signal_time = now

async def listen():
    global last_trade_price

    last_status_log = 0.0
    STATUS_LOG_INTERVAL = 10

    async with websockets.connect(WS_URL, ping_interval=20) as ws:
        # Nur Trades abonnieren - KEIN ORDERBUCH!
        await ws.send(json.dumps({"type": "subscribe", "channel": f"trade/{MARKET_INDEX}"}))

        debug_log(f"✅ Verbunden, abonniert trade:{MARKET_INDEX}")
        debug_log(f"📊 EMA: {EMA_FAST}/{EMA_SLOW} (Tick-basiert, wie TradingView)")

        async for raw in ws:
            msg = json.loads(raw)
            channel = msg.get("channel", "")

            if channel.startswith("trade"):
                trades = msg.get("trades", [])
                if trades:
                    # Letzten Trade nehmen
                    last_trade = trades[-1]
                    price = float(last_trade["price"])
                    last_trade_price = price
                    
                    # EMAs updaten
                    fast_ema.add_price(price)
                    slow_ema.add_price(price)
                    
                    now = time.time()
                    
                    # Crossover prüfen (wenn EMAs initialisiert)
                    if fast_ema.is_initialized and slow_ema.is_initialized:
                        fast = fast_ema.ema
                        slow = slow_ema.ema
                        
                        # Crossover Up: EMA7 > EMA21
                        if fast > slow and current_position_side != "buy":
                            debug_log(f"📈 CROSSOVER UP: EMA{EMA_FAST} ({fast:.3f}) > EMA{EMA_SLOW} ({slow:.3f})")
                            await execute_signal("buy", price)
                        
                        # Crossover Down: EMA7 < EMA21
                        elif fast < slow and current_position_side != "sell":
                            debug_log(f"📉 CROSSOVER DOWN: EMA{EMA_FAST} ({fast:.3f}) < EMA{EMA_SLOW} ({slow:.3f})")
                            await execute_signal("sell", price)
                    
                    # Status-Log
                    if now - last_status_log >= STATUS_LOG_INTERVAL:
                        last_status_log = now
                        debug_log(f"📊 Status {SYMBOL}", {
                            "preis": price,
                            f"ema_{EMA_FAST}": round(fast_ema.ema, 3) if fast_ema.ema else None,
                            f"ema_{EMA_SLOW}": round(slow_ema.ema, 3) if slow_ema.ema else None,
                            "position": current_position_side or "flach",
                            "datenpunkte": len(fast_ema.prices)
                        })

async def main():
    global current_position_side

    print("=" * 60)
    print(f"🚀 EMA-Crossover-Bot für {SYMBOL} (Market Index {MARKET_INDEX})")
    print(f"   DRY_RUN: {DRY_RUN}")
    print(f"   EMA: {EMA_FAST}/{EMA_SLOW} (Tick-basiert)")
    print(f"   Margin: {MARGIN} USDC | Hebel: {LEVERAGE}x | Cooldown: {COOLDOWN_SECONDS}s")
    print("=" * 60)

    if not DRY_RUN:
        await sync_open_position_from_exchange(SYMBOL)
        if SYMBOL in OPEN_POSITIONS:
            current_position_side = OPEN_POSITIONS[SYMBOL]["side"]

    while True:
        try:
            await listen()
        except Exception as e:
            debug_log("⚠️ WebSocket-Verbindung verloren, reconnect in 5s", {"error": str(e)})
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
