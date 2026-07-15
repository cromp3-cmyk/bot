"""
EMA-Crossover-Bot für Lighter
- Holt 1-Minuten Candles über Lighter SDK (CandlestickApi.candles)
- EMA7 + EMA21 basierend auf Candle-Closes
- Kauft/Verkauft bei Crossover
"""

import asyncio
import websockets
import json
import time
import os
import traceback
from collections import deque
from datetime import datetime

# ========== BASE URL ==========
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
    """Berechnet EMA basierend auf Candle-Closes (wie TradingView)"""
    def __init__(self, period):
        self.period = period
        self.closes = []
        self.ema = None
        self.is_initialized = False
        
    def add_candle(self, close_price):
        """Fügt Candle-Close hinzu (NUR 1 Wert pro Minute!)"""
        self.closes.append(close_price)
        
        # Nur die letzten Perioden behalten
        if len(self.closes) > self.period * 2:
            self.closes = self.closes[-self.period * 2:]
        
        if len(self.closes) == self.period:
            # Erster EMA = SMA (wie TradingView)
            self.ema = sum(self.closes) / self.period
            self.is_initialized = True
        elif len(self.closes) > self.period:
            # EMA Formel (wie TradingView!)
            multiplier = 2 / (self.period + 1)
            self.ema = (close_price - self.ema) * multiplier + self.ema

# ========== CANDLES VIA LIGHTER SDK (ENDGÜLTIG KORREKT) ==========
async def get_candles_from_lighter(market_id, resolution="1m", count_back=200):
    """Holt Candles über das offizielle Lighter Python SDK"""
    try:
        import lighter
        from lighter import CandlestickApi
        
        # Timestamps berechnen
        end_timestamp = int(time.time())
        start_timestamp = end_timestamp - (count_back * 60)  # count_back Minuten zurück
        
        debug_log(f"📡 Hole Candles über Lighter SDK: market_id={market_id}, resolution={resolution}")
        debug_log(f"   start_timestamp={start_timestamp}, end_timestamp={end_timestamp}, count_back={count_back}")
        
        # API Client erstellen
        client = lighter.ApiClient()
        candle_api = CandlestickApi(client)
        
        # Candles abrufen - ALLE 4 Parameter!
        response = await candle_api.candles(
            market_id=market_id,
            resolution=resolution,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            count_back=count_back
        )
        
        # Client schließen
        await client.close()
        
        # Candles aus Response extrahieren
        candles = getattr(response, 'candles', []) or []
        
        if not candles:
            debug_log("⚠️ Keine Candles von Lighter SDK erhalten")
            return []
        
        debug_log(f"✅ {len(candles)} Candles von Lighter SDK erhalten")
        
        # Candles als Liste von Dictionaries zurückgeben
        result = []
        for c in candles:
            result.append({
                "timestamp": getattr(c, 't', 0),
                "open": float(getattr(c, 'o', 0)),
                "high": float(getattr(c, 'h', 0)),
                "low": float(getattr(c, 'l', 0)),
                "close": float(getattr(c, 'c', 0)),
                "volume": float(getattr(c, 'v', 0))
            })
        
        return result
        
    except Exception as e:
        debug_log("❌ Fehler beim Abrufen der Candles über SDK", {
            "error": str(e),
            "traceback": traceback.format_exc()
        })
        return []

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
    """Erstellt eine Market-Order"""
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
    """Öffnet oder reversed eine Position"""
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
            return

    except Exception as e:
        debug_log("⚠️ Positions-Sync fehlgeschlagen", {"error": str(e)})

# ========== STATE ==========
OPEN_POSITIONS = {}

# ========== KONFIGURATION ==========
SYMBOL = os.getenv("OB_SYMBOL", "SOL").upper()
if SYMBOL not in MARKET_INDICES:
    raise ValueError(f"Symbol {SYMBOL} nicht in MARKET_INDICES gefunden")

MARKET_INDEX = MARKET_INDICES[SYMBOL]

# EMA Parameter
EMA_FAST = int(os.getenv("EMA_FAST", "7"))
EMA_SLOW = int(os.getenv("EMA_SLOW", "21"))

# Trading Parameter
MARGIN = float(os.getenv("OB_MARGIN", "10"))
LEVERAGE = int(os.getenv("OB_LEVERAGE", "20"))
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

# Candle Parameter
CANDLE_LIMIT = int(os.getenv("CANDLE_LIMIT", "200"))
MIN_HOLD_SECONDS = float(os.getenv("MIN_HOLD_SECONDS", "120"))
SIGNAL_COOLDOWN = float(os.getenv("SIGNAL_COOLDOWN", "30"))

# ========== LOKALER STATE ==========
fast_ema = EMACalculator(EMA_FAST)
slow_ema = EMACalculator(EMA_SLOW)
current_position_side = None
last_signal_time = 0
position_opened_at = 0
current_candle = None

async def execute_signal(direction, price):
    """Führt Signal aus"""
    global current_position_side, last_signal_time, position_opened_at

    now = time.time()
    
    if now - last_signal_time < SIGNAL_COOLDOWN:
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
        last_signal_time = now
        return

    result = await open_or_reverse_position(direction, SYMBOL, MARGIN, LEVERAGE, price)
    debug_log("Order-Ergebnis", result)

    if result.get("success"):
        current_position_side = direction
        position_opened_at = now
        last_signal_time = now

async def init_emas_from_lighter():
    """Initialisiert EMAs mit Candles vom Lighter SDK"""
    debug_log(f"📊 Initialisiere EMAs mit Candles von Lighter SDK...")
    
    candles = await get_candles_from_lighter(MARKET_INDEX, "1m", CANDLE_LIMIT)
    
    if not candles:
        debug_log("⚠️ Keine Candles von Lighter SDK erhalten")
        return False
    
    for candle in candles:
        close_price = candle["close"]
        if close_price > 0:
            fast_ema.add_candle(close_price)
            slow_ema.add_candle(close_price)
    
    debug_log(f"✅ EMAs initialisiert mit {len(candles)} Candles")
    debug_log(f"   EMA{EMA_FAST}: {fast_ema.ema:.3f}" if fast_ema.ema else "   EMA7: None")
    debug_log(f"   EMA{EMA_SLOW}: {slow_ema.ema:.3f}" if slow_ema.ema else "   EMA21: None")
    
    return True

async def listen():
    global current_candle

    last_status_log = 0.0
    STATUS_LOG_INTERVAL = 10

    async with websockets.connect(WS_URL, ping_interval=20) as ws:
        await ws.send(json.dumps({"type": "subscribe", "channel": f"trade/{MARKET_INDEX}"}))

        debug_log(f"✅ Verbunden, abonniert trade:{MARKET_INDEX}")
        debug_log(f"📊 EMA: {EMA_FAST}/{EMA_SLOW} auf 1-Minuten Candles (via Lighter SDK)")

        async for raw in ws:
            msg = json.loads(raw)
            channel = msg.get("channel", "")

            if channel.startswith("trade"):
                trades = msg.get("trades", [])
                if not trades:
                    continue

                for trade in trades:
                    price = float(trade["price"])
                    size = float(trade["size"])
                    minute = int(time.time() / 60)
                    
                    if current_candle is None or current_candle["minute"] != minute:
                        if current_candle is not None:
                            close = current_candle["close"]
                            fast_ema.add_candle(close)
                            slow_ema.add_candle(close)
                            
                            debug_log(f"🕐 Neue Candle: Close={close:.3f}")
                            
                            if fast_ema.is_initialized and slow_ema.is_initialized:
                                now = time.time()
                                if now - last_signal_time >= SIGNAL_COOLDOWN:
                                    if fast_ema.ema > slow_ema.ema and current_position_side != "buy":
                                        debug_log(f"📈 CROSSOVER UP: EMA7 ({fast_ema.ema:.3f}) > EMA21 ({slow_ema.ema:.3f})")
                                        await execute_signal("buy", close)
                                    elif fast_ema.ema < slow_ema.ema and current_position_side != "sell":
                                        debug_log(f"📉 CROSSOVER DOWN: EMA7 ({fast_ema.ema:.3f}) < EMA21 ({slow_ema.ema:.3f})")
                                        await execute_signal("sell", close)
                        
                        current_candle = {
                            "minute": minute,
                            "open": price,
                            "high": price,
                            "low": price,
                            "close": price,
                            "volume": 0
                        }
                    else:
                        current_candle["high"] = max(current_candle["high"], price)
                        current_candle["low"] = min(current_candle["low"], price)
                        current_candle["close"] = price
                        current_candle["volume"] += size

                    now = time.time()
                    if now - last_status_log >= STATUS_LOG_INTERVAL:
                        last_status_log = now
                        debug_log(f"📊 Status {SYMBOL}", {
                            "preis": price,
                            "candle_close": current_candle["close"],
                            f"ema_{EMA_FAST}": round(fast_ema.ema, 3) if fast_ema.ema else None,
                            f"ema_{EMA_SLOW}": round(slow_ema.ema, 3) if slow_ema.ema else None,
                            "position": current_position_side or "flach",
                            "candles": len(fast_ema.closes)
                        })

async def main():
    global current_position_side

    print("=" * 70)
    print(f"🚀 EMA-Crossover-Bot für {SYMBOL}")
    print(f"   EMA: {EMA_FAST}/{EMA_SLOW} auf 1-Minuten Candles (via Lighter SDK)")
    print(f"   DRY_RUN: {DRY_RUN}")
    print(f"   Margin: {MARGIN} USDC | Hebel: {LEVERAGE}x")
    print(f"   Min Hold: {MIN_HOLD_SECONDS}s | Cooldown: {SIGNAL_COOLDOWN}s")
    print("=" * 70)

    await init_emas_from_lighter()

    if not DRY_RUN:
        await sync_open_position_from_exchange(SYMBOL)
        if SYMBOL in OPEN_POSITIONS:
            current_position_side = OPEN_POSITIONS[SYMBOL]["side"]
            debug_log(f"📌 Bestehende Position: {current_position_side}")

    while True:
        try:
            await listen()
        except Exception as e:
            debug_log("⚠️ WebSocket-Verbindung verloren, reconnect in 5s", {"error": str(e)})
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
