"""
Autonomer Orderbuch-Signal-Bot für Lighter - NORMAL (FINAL)
================================================================================
SELL Signal → Short eröffnen (oder von Long wechseln)
BUY Signal  → Long eröffnen  (oder von Short wechseln)
Keine doppelten Signale, keine doppelten Positionen!
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

async def get_current_position_from_exchange(symbol):
    """Holt die aktuelle Position vom Exchange"""
    try:
        import lighter
        account_index = int(os.getenv("ACCOUNT_INDEX", "50960"))
        api_client = lighter.ApiClient(configuration=lighter.Configuration(host=BASE_URL))
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
    """Öffnet oder reversed eine Position - NORMAL (KEIN doppeltes Öffnen!)"""
    client = get_lighter_client()
    if client is None:
        return {"error": "Client konnte nicht initialisiert werden"}

    try:
        market_index = MARKET_INDICES[symbol]
        precision = get_precision(symbol)
        min_base_amount = get_min_base_amount(symbol)

        # ===== 1. BESTEHENDE POSITION VOM EXCHANGE HOLEN =====
        current_pos = await get_current_position_from_exchange(symbol)

        # ===== 2. NEUE POSITIONSGRÖSSE BERECHNEN =====
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

        # ===== 3. WENN POSITION EXISTIERT → Prüfen! =====
        if current_pos:
            debug_log(f"📌 Bestehende Position: {current_pos['side']} @ {current_pos['open_price']}")

            # ==== GLEICHE RICHTUNG → NICHTS TUN! ====
            if current_pos["side"] == new_side:
                debug_log(f"⏭️ Bereits {new_side}, ignoriere Signal!")
                return {"success": True, "action": "ignoriert", "side": new_side}

            # ==== ANDERE RICHTUNG → Position schließen + neue öffnen ====
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

        # ===== 4. KEINE POSITION → Neu eröffnen =====
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

# ========== STATE ==========
OPEN_POSITIONS = {}

# ========== KONFIGURATION ==========
SYMBOL = os.getenv("OB_SYMBOL", "SOL").upper()
if SYMBOL not in MARKET_INDICES:
    raise ValueError(f"Symbol {SYMBOL} nicht in MARKET_INDICES gefunden")
MARKET_INDEX = MARKET_INDICES[SYMBOL]

OBI_LEVELS = int(os.getenv("OBI_LEVELS", "15"))
OBI_THRESHOLD = float(os.getenv("OBI_THRESHOLD", "0.12"))
NORMALIZE_SECONDS = float(os.getenv("NORMALIZE_SECONDS", "10"))
SIGNAL_COOLDOWN = float(os.getenv("SIGNAL_COOLDOWN", "1.0"))  # 1 Sekunde zwischen Signalen

MARGIN = float(os.getenv("OB_MARGIN", "10"))
LEVERAGE = int(os.getenv("OB_LEVERAGE", "20"))
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

# ========== NORMALISIERUNG ==========
class OBINormalizer:
    def __init__(self, window_seconds=10):
        self.window_seconds = window_seconds
        self.buffer = deque()
        self.normalized_obi = 0.0

    def add(self, raw_obi):
        now = time.time()
        self.buffer.append((now, raw_obi))
        cutoff = now - self.window_seconds
        while self.buffer and self.buffer[0][0] < cutoff:
            self.buffer.popleft()

        if self.buffer:
            total = sum(v for _, v in self.buffer)
            self.normalized_obi = total / len(self.buffer)
        else:
            self.normalized_obi = 0.0
        return self.normalized_obi

    def get(self):
        return self.normalized_obi

    def get_raw_count(self):
        return len(self.buffer)

# ========== LOKALER STATE ==========
order_book = {"bids": {}, "asks": {}}
last_trade_price = None
current_position_side = None
obi_normalizer = OBINormalizer(window_seconds=NORMALIZE_SECONDS)
current_lean = None
last_signal_time = 0

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

def calc_raw_obi(levels=OBI_LEVELS):
    bids_sorted = sorted(order_book["bids"].items(), key=lambda x: float(x[0]), reverse=True)[:levels]
    asks_sorted = sorted(order_book["asks"].items(), key=lambda x: float(x[0]))[:levels]
    bid_vol = sum(v for _, v in bids_sorted)
    ask_vol = sum(v for _, v in asks_sorted)
    total = bid_vol + ask_vol
    return 0.0 if total == 0 else (bid_vol - ask_vol) / total

async def execute_signal(direction, price):
    global current_position_side, last_signal_time

    # ===== COOLDOWN: Keine doppelten Signale! =====
    now = time.time()
    if now - last_signal_time < SIGNAL_COOLDOWN:
        debug_log(f"⏭️ Signal ignoriert (Cooldown: {now - last_signal_time:.2f}s)")
        return

    if current_position_side == direction:
        debug_log(f"⏭️ Bereits {direction}, ignoriere")
        return

    debug_log(f"📡 SIGNAL: {direction.upper()} {SYMBOL} @ {price}")
    last_signal_time = now

    if DRY_RUN:
        debug_log("🧪 DRY_RUN - keine Order")
        current_position_side = direction
        return

    result = await open_or_reverse_position(direction, SYMBOL, MARGIN, LEVERAGE, price)
    debug_log("Order-Ergebnis", result)

    if result.get("success"):
        if result.get("to_side"):
            current_position_side = result.get("to_side")
        elif result.get("side"):
            current_position_side = result.get("side")

async def listen():
    global last_trade_price, current_lean, last_signal_time

    last_status_log = 0.0
    STATUS_LOG_INTERVAL = 10

    async with websockets.connect(WS_URL, ping_interval=20) as ws:
        await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{MARKET_INDEX}"}))
        await ws.send(json.dumps({"type": "subscribe", "channel": f"trade/{MARKET_INDEX}"}))

        debug_log(f"✅ Verbunden | NORMAL: SELL→Short, BUY→Long")
        debug_log(f"   Normalisierung: {NORMALIZE_SECONDS}s | Schwelle: {OBI_THRESHOLD}")
        debug_log(f"   Signal-Cooldown: {SIGNAL_COOLDOWN}s | Keine doppelten Positionen!")

        async for raw in ws:
            msg = json.loads(raw)
            channel = msg.get("channel", "")

            if channel.startswith("order_book"):
                apply_order_book_update(msg)
                raw_obi = calc_raw_obi()
                normalized_obi = obi_normalizer.add(raw_obi)
                now = time.time()

                if normalized_obi >= OBI_THRESHOLD:
                    new_lean = "buy"
                elif normalized_obi <= -OBI_THRESHOLD:
                    new_lean = "sell"
                else:
                    new_lean = None

                # ===== NUR BEI LEAN-ÄNDERUNG UND COOLDOWN =====
                if new_lean != current_lean and new_lean is not None and last_trade_price is not None:
                    if now - last_signal_time >= SIGNAL_COOLDOWN:
                        await execute_signal(new_lean, last_trade_price)
                        current_lean = new_lean
                    else:
                        debug_log(f"⏭️ Lean-Änderung ignoriert (Cooldown)")
                else:
                    if new_lean is None:
                        current_lean = None

                if now - last_status_log >= STATUS_LOG_INTERVAL:
                    last_status_log = now

                    if normalized_obi > 0.05:
                        richtung = "Käufer dominieren" if normalized_obi < OBI_THRESHOLD else "Käufer STARK"
                    elif normalized_obi < -0.05:
                        richtung = "Verkäufer dominieren" if normalized_obi > -OBI_THRESHOLD else "Verkäufer STARK"
                    else:
                        richtung = "ausgeglichen"

                    debug_log(f"📊 Status {SYMBOL} (NORMAL)", {
                        "normalisierung": f"{NORMALIZE_SECONDS}s",
                        "roh_OBI": round(raw_obi, 3),
                        "normalisiert": round(normalized_obi, 3),
                        "richtung": richtung,
                        "schwelle": OBI_THRESHOLD,
                        "buffer": obi_normalizer.get_raw_count(),
                        "preis": last_trade_price,
                        "position": current_position_side or "flach",
                        "lean": current_lean or "neutral",
                    })

            elif channel.startswith("trade"):
                trades = msg.get("trades", [])
                if trades:
                    last_trade_price = float(trades[-1]["price"])

async def main():
    global current_position_side

    print("=" * 60)
    print(f"🚀 NORMALER Signal-Follower Bot für {SYMBOL}")
    print(f"   SELL → Short | BUY → Long")
    print(f"   DRY_RUN: {DRY_RUN}")
    print(f"   Normalisierung: {NORMALIZE_SECONDS}s | Schwelle: {OBI_THRESHOLD}")
    print(f"   Signal-Cooldown: {SIGNAL_COOLDOWN}s")
    print(f"   Margin: {MARGIN} USDC | Hebel: {LEVERAGE}x")
    print("=" * 60)

    if not DRY_RUN:
        pos = await get_current_position_from_exchange(SYMBOL)
        if pos:
            current_position_side = pos["side"]
            debug_log(f"📌 Position: {current_position_side}")

    while True:
        try:
            await listen()
        except Exception as e:
            debug_log("⚠️ Reconnect in 5s", {"error": str(e)})
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
