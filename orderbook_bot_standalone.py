"""
Autonomer Orderbuch-Signal-Bot für Lighter (zkLighter) - MIT 2-MINUTEN EMA
================================================================================
Kombiniert Order Book Imbalance (OBI) mit 2-Minuten EMA-Trend-Filter
"""

# ========== IMPORTS ==========
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
            print(f"   DATA: {json.dumps(data, indent=2, default=str, ensure_ascii=False)}")

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
        "BTC": 100000,
        "ETH": 10000, "XAU": 10000, "TSLA": 10000, "MSFT": 10000,
        "GOOGL": 10000, "META": 10000, "NVDA": 10000,
        "SOL": 1000, "TAO": 1000, "AAVE": 1000, "LTC": 1000,
        "BCH": 1000, "XMR": 1000, "ZEC": 1000, "USDJPY": 1000,
        "AVAX": 100, "BNB": 100, "HYPE": 100, "TRUMP": 100,
        "UNI": 100, "APT": 100, "PENDLE": 100, "GMX": 100,
        "VVV": 100, "XAG": 100,
        "LINK": 10, "NEAR": 10, "DOT": 10, "SUI": 10, "ADA": 10,
        "ARB": 10, "OP": 10, "WIF": 10, "WLD": 10, "TON": 10,
        "JUP": 10, "ENA": 10, "SEI": 10, "ONDO": 10, "CRV": 10,
        "LDO": 10, "EIGEN": 10, "GRASS": 10, "ZRO": 10, "DYDX": 10,
        "XLM": 10,
        "DOGE": 1, "XRP": 1, "POL": 1, "1000PEPE": 1, "1000SHIB": 1,
        "1000BONK": 1, "1000FLOKI": 1, "PUMP": 1, "PENGU": 1,
    }
    return precision_map.get(symbol, 10000)

def get_price_decimals(symbol):
    decimals_map = {
        "BTC": 1, "XAU": 1,
        "ETH": 2,
        "SOL": 3, "LTC": 3, "BCH": 3, "XMR": 3, "ZEC": 3,
        "AAVE": 3, "TAO": 3, "USDJPY": 3,
        "AVAX": 3, "BNB": 4, "UNI": 4, "APT": 4, "PENDLE": 4,
        "GMX": 4, "VVV": 4, "TRUMP": 4, "HYPE": 4,
        "LINK": 5, "NEAR": 5, "DOT": 5, "SUI": 5, "ADA": 5,
        "ARB": 5, "OP": 5, "WIF": 5, "WLD": 5, "TON": 5,
        "JUP": 5, "ENA": 5, "SEI": 5, "ONDO": 5, "CRV": 5,
        "XLM": 5,
        "DOGE": 6, "XRP": 6, "POL": 6, "1000PEPE": 6, "1000SHIB": 6,
        "1000BONK": 6, "1000FLOKI": 6, "ZK": 6, "XAG": 6,
    }
    return decimals_map.get(symbol, 2)

def get_min_base_amount(symbol):
    min_amount_map = {
        "BTC": 0.00020, "ETH": 0.005, "SOL": 0.05, "DOGE": 10, "XRP": 20,
        "LINK": 1.0, "AVAX": 0.5, "NEAR": 2.0, "DOT": 2.0, "BNB": 0.02,
        "HYPE": 0.50, "SUI": 3.0, "ADA": 10.0, "ARB": 20.0, "OP": 10.0,
        "XLM": 30,
    }
    return min_amount_map.get(symbol, 0.001)

# ========== 2-MINUTEN EMA TREND FILTER ==========
class TimeBasedEMAFilter:
    """
    EMA basierend auf 2-Minuten Intervallen
    """
    def __init__(self, fast_minutes=1, slow_minutes=2):
        self.fast_minutes = fast_minutes
        self.slow_minutes = slow_minutes
        self.price_history = []  # Liste von (timestamp, price)
        self.fast_ema = None
        self.slow_ema = None
        self.last_update_time = 0
        self.last_price = None
        
    def add_price(self, price):
        """Fügt Preis mit Zeitstempel hinzu"""
        now = time.time()
        self.price_history.append((now, price))
        self.last_price = price
        
        # Alte Daten entfernen (älter als 10 Minuten)
        cutoff = now - 600  # 10 Minuten
        self.price_history = [(t, p) for t, p in self.price_history if t > cutoff]
        
        # EMAs berechnen
        self.calculate_time_ema()
    
    def get_prices_in_last_minutes(self, minutes):
        """Holt alle Preise der letzten X Minuten"""
        now = time.time()
        cutoff = now - (minutes * 60)
        return [p for t, p in self.price_history if t > cutoff]
    
    def calculate_ema_from_prices(self, prices, period_in_minutes):
        """Berechnet EMA aus Preisliste mit exponentieller Gewichtung"""
        if len(prices) < 2:
            return prices[-1] if prices else None
        
        # Exponentielle Gewichtung basierend auf Zeit
        # Je neuer der Preis, desto höher die Gewichtung
        multiplier = 2 / (period_in_minutes * 60 + 1)
        ema = prices[0]
        
        for price in prices[1:]:
            ema = (price - ema) * multiplier + ema
        
        return ema
    
    def calculate_time_ema(self):
        """Berechnet EMAs basierend auf Zeit"""
        # Hole Preise der letzten X Minuten
        fast_prices = self.get_prices_in_last_minutes(self.fast_minutes)
        slow_prices = self.get_prices_in_last_minutes(self.slow_minutes)
        
        if fast_prices:
            self.fast_ema = self.calculate_ema_from_prices(fast_prices, self.fast_minutes)
        else:
            self.fast_ema = None
            
        if slow_prices:
            self.slow_ema = self.calculate_ema_from_prices(slow_prices, self.slow_minutes)
        else:
            self.slow_ema = None
    
    def get_trend(self):
        """
        Gibt Trend zurück:
        - "up": Aufwärtstrend
        - "down": Abwärtstrend
        - "neutral": Seitwärts
        """
        if self.fast_ema is None or self.slow_ema is None:
            return "neutral", 0
        
        diff_percent = ((self.fast_ema - self.slow_ema) / self.slow_ema) * 100
        
        # 0.1% Puffer gegen Rauschen (niedriger für 2-Minuten EMA)
        if diff_percent > 0.1:
            return "up", abs(diff_percent) / 100
        elif diff_percent < -0.1:
            return "down", abs(diff_percent) / 100
        else:
            return "neutral", 0
    
    def get_trend_strength(self):
        """Gibt Trendstärke zurück (0-1)"""
        if self.fast_ema is None or self.slow_ema is None:
            return 0
        
        diff = abs(self.fast_ema - self.slow_ema) / self.slow_ema
        return min(diff * 20, 1)  # Skaliert auf 0-1 (höhere Empfindlichkeit)
    
    def get_status(self):
        """Gibt kompletten Status zurück (für Debug)"""
        trend, strength = self.get_trend()
        return {
            "fast_ema": round(self.fast_ema, 3) if self.fast_ema else None,
            "slow_ema": round(self.slow_ema, 3) if self.slow_ema else None,
            "trend": trend,
            "strength": round(strength, 2),
            "data_points": len(self.price_history),
            "fast_period": f"{self.fast_minutes}m",
            "slow_period": f"{self.slow_minutes}m"
        }

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

# ========== STATE ==========
OPEN_POSITIONS = {}

# ========== KONFIGURATION ==========
SYMBOL = os.getenv("OB_SYMBOL", "BTC").upper()
if SYMBOL not in MARKET_INDICES:
    raise ValueError(f"Symbol {SYMBOL} nicht in MARKET_INDICES gefunden")

MARKET_INDEX = MARKET_INDICES[SYMBOL]

OBI_LEVELS = int(os.getenv("OBI_LEVELS", "15"))
OBI_THRESHOLD = float(os.getenv("OBI_THRESHOLD", "0.12"))
OBI_CONFIRM_SECONDS = float(os.getenv("OBI_CONFIRM_SECONDS", "5"))
COOLDOWN_SECONDS = float(os.getenv("COOLDOWN_SECONDS", "30"))
MIN_HOLD_SECONDS = float(os.getenv("MIN_HOLD_SECONDS", "120"))

MARGIN = float(os.getenv("OB_MARGIN", "10"))
LEVERAGE = int(os.getenv("OB_LEVERAGE", "20"))

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

# 2-Minuten EMA Parameter
EMA_FAST_MINUTES = int(os.getenv("EMA_FAST_MINUTES", "1"))   # 1-Minute EMA
EMA_SLOW_MINUTES = int(os.getenv("EMA_SLOW_MINUTES", "2"))   # 2-Minute EMA
MIN_TREND_STRENGTH = float(os.getenv("MIN_TREND_STRENGTH", "0.05"))

# ========== LOKALER STATE ==========
order_book = {"bids": {}, "asks": {}}
last_trade_price = None
current_position_side = None
position_opened_at = 0.0
last_trade_time = 0.0
obi_history = deque(maxlen=20)
lean_direction = None
lean_since = 0.0

# ========== 2-MINUTEN EMA TREND FILTER INSTANZ ==========
trend_filter = TimeBasedEMAFilter(
    fast_minutes=EMA_FAST_MINUTES,
    slow_minutes=EMA_SLOW_MINUTES
)

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

def calc_obi(levels=OBI_LEVELS):
    bids_sorted = sorted(order_book["bids"].items(), key=lambda x: float(x[0]), reverse=True)[:levels]
    asks_sorted = sorted(order_book["asks"].items(), key=lambda x: float(x[0]))[:levels]
    bid_vol = sum(v for _, v in bids_sorted)
    ask_vol = sum(v for _, v in asks_sorted)
    total = bid_vol + ask_vol
    return 0.0 if total == 0 else (bid_vol - ask_vol) / total

async def execute_signal(direction, price):
    global current_position_side, last_trade_time, position_opened_at

    now = time.time()
    if now - last_trade_time < COOLDOWN_SECONDS:
        return
    if current_position_side == direction:
        return
    if current_position_side is not None and (now - position_opened_at) < MIN_HOLD_SECONDS:
        debug_log(f"⏳ Reverse blockiert - Mindesthaltedauer noch nicht erreicht", {
            "aktuelle_position_seit_sekunden": round(now - position_opened_at, 1),
            "min_hold_seconds": MIN_HOLD_SECONDS,
        })
        return

    debug_log(f"📡 OBI-Signal bestätigt: {direction.upper()} {SYMBOL} @ {price}", {
        "bestaetigt_seit_sekunden": round(now - lean_since, 1) if lean_since else None,
    })

    if DRY_RUN:
        debug_log("🧪 DRY_RUN aktiv - keine echte Order ausgeführt")
        current_position_side = direction
        position_opened_at = now
        last_trade_time = now
        return

    result = await open_or_reverse_position(direction, SYMBOL, MARGIN, LEVERAGE, price)
    debug_log("Order-Ergebnis", result)

    current_position_side = direction
    position_opened_at = now
    last_trade_time = now

async def listen():
    global last_trade_price, lean_direction, lean_since

    last_status_log = 0.0
    STATUS_LOG_INTERVAL = 10

    async with websockets.connect(WS_URL, ping_interval=20) as ws:
        await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{MARKET_INDEX}"}))
        await ws.send(json.dumps({"type": "subscribe", "channel": f"trade/{MARKET_INDEX}"}))

        debug_log(f"✅ Verbunden, abonniert order_book:{MARKET_INDEX} und trade:{MARKET_INDEX}")

        async for raw in ws:
            msg = json.loads(raw)
            channel = msg.get("channel", "")
            msg_type = msg.get("type", "")

            if channel.startswith("order_book"):
                apply_order_book_update(msg)
                obi = calc_obi()
                obi_history.append(obi)

                now = time.time()

                # ===== 2-MINUTEN TREND-CHECK MIT EMA =====
                trend = "neutral"
                trend_strength = 0
                if last_trade_price is not None:
                    trend_filter.add_price(last_trade_price)
                    trend, trend_strength = trend_filter.get_trend()

                # ===== SIGNAL MIT TREND-BESTÄTIGUNG =====
                # Nur handeln wenn OBI und Trend übereinstimmen
                if obi >= OBI_THRESHOLD and trend == "up" and trend_strength >= MIN_TREND_STRENGTH:
                    current_lean = "buy"
                elif obi <= -OBI_THRESHOLD and trend == "down" and trend_strength >= MIN_TREND_STRENGTH:
                    current_lean = "sell"
                else:
                    current_lean = None

                # ===== LEAN TIMER =====
                if current_lean != lean_direction:
                    if lean_direction is not None:
                        debug_log(f"🔄 Lean-Richtung geändert (mit 2-Minuten EMA)", {
                            "von": lean_direction,
                            "zu": current_lean,
                            "obi": round(obi, 3),
                            "trend": trend,
                            "trend_strength": round(trend_strength, 2)
                        })
                    lean_direction = current_lean
                    lean_since = now if current_lean is not None else 0.0
                elif current_lean is not None and (now - lean_since) >= OBI_CONFIRM_SECONDS and last_trade_price is not None:
                    await execute_signal(current_lean, last_trade_price)

                # ===== STATUS-LOG =====
                if now - last_status_log >= STATUS_LOG_INTERVAL:
                    last_status_log = now
                    status = trend_filter.get_status()
                    debug_log(f"📊 Status {SYMBOL} mit 2-Minuten EMA", {
                        "aktueller_OBI": round(obi, 3),
                        "schwelle": OBI_THRESHOLD,
                        "trend": status["trend"],
                        "trend_staerke": status["strength"],
                        "fast_ema_1m": status["fast_ema"],
                        "slow_ema_2m": status["slow_ema"],
                        "letzter_preis": last_trade_price,
                        "bot_position": current_position_side or "flach",
                        "lean_seit": round(now - lean_since, 1) if lean_direction else 0,
                        "braucht_fuer_signal": OBI_CONFIRM_SECONDS
                    })

            elif channel.startswith("trade"):
                trades = msg.get("trades", [])
                if trades:
                    last_trade_price = float(trades[-1]["price"])
                    # Preis sofort in Trend-Filter geben
                    trend_filter.add_price(last_trade_price)

async def main():
    global current_position_side

    print("=" * 70)
    print(f"🚀 Orderbuch-Bot mit 2-Minuten EMA-Trend-Filter gestartet für {SYMBOL}")
    print(f"   DRY_RUN: {DRY_RUN}")
    print(f"   OBI Levels: {OBI_LEVELS} | Schwelle: {OBI_THRESHOLD} | Bestätigung: {OBI_CONFIRM_SECONDS}s")
    print(f"   EMA: {EMA_FAST_MINUTES}m / {EMA_SLOW_MINUTES}m | Min Trend Stärke: {MIN_TREND_STRENGTH}")
    print(f"   Margin: {MARGIN} USDC | Hebel: {LEVERAGE}x | Cooldown: {COOLDOWN_SECONDS}s")
    print("=" * 70)

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
