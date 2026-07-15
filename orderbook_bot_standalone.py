"""
Autonomer Orderbuch-Signal-Bot für Lighter (zkLighter) - EIGENSTÄNDIGE VERSION
================================================================================
Läuft komplett unabhängig, ohne Abhängigkeit zu einem anderen Bot/Repo.
Verbindet sich per WebSocket mit Lighter, berechnet Order Book Imbalance (OBI)
und tradet autonom bei bestätigtem Signal.

10-Sekunden-Normalisierung: Der OBI wird über die letzten 10 Sekunden gemittelt.
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


# ========== State für offene Positionen ==========
OPEN_POSITIONS = {}

# ========== Konfiguration ==========
SYMBOL = os.getenv("OB_SYMBOL", "SOL").upper()
if SYMBOL not in MARKET_INDICES:
    raise ValueError(f"Symbol {SYMBOL} nicht in MARKET_INDICES gefunden")
MARKET_INDEX = MARKET_INDICES[SYMBOL]

OBI_LEVELS = int(os.getenv("OBI_LEVELS", "15"))
OBI_THRESHOLD = float(os.getenv("OBI_THRESHOLD", "0.12"))
OBI_CONFIRM_SECONDS = float(os.getenv("OBI_CONFIRM_SECONDS", "5"))
COOLDOWN_SECONDS = float(os.getenv("COOLDOWN_SECONDS", "15"))
MIN_HOLD_SECONDS = float(os.getenv("MIN_HOLD_SECONDS", "30"))

MARGIN = float(os.getenv("OB_MARGIN", "10"))
LEVERAGE = int(os.getenv("OB_LEVERAGE", "20"))

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

# ========== 10-Sekunden-Normalisierung ==========
class OBINormalizer:
    """Normalisiert OBI über ein Zeitfenster (10 Sekunden)"""
    def __init__(self, window_seconds=10):
        self.window_seconds = window_seconds
        self.buffer = deque()  # (timestamp, obi_value)
        self.normalized_obi = 0.0
        
    def add(self, raw_obi):
        """Fügt einen Roh-OBI-Wert hinzu und berechnet den normalisierten Wert"""
        now = time.time()
        self.buffer.append((now, raw_obi))
        
        # Alte Werte entfernen (älter als window_seconds)
        cutoff = now - self.window_seconds
        while self.buffer and self.buffer[0][0] < cutoff:
            self.buffer.popleft()
        
        # Mittelwert über alle Werte im Fenster
        if self.buffer:
            total = sum(v for _, v in self.buffer)
            self.normalized_obi = total / len(self.buffer)
        else:
            self.normalized_obi = 0.0
        
        return self.normalized_obi
    
    def get(self):
        """Gibt den aktuellen normalisierten OBI zurück"""
        return self.normalized_obi
    
    def get_raw_count(self):
        """Anzahl der Roh-Werte im Buffer"""
        return len(self.buffer)


# ========== Lokaler Orderbuch-State ==========
order_book = {"bids": {}, "asks": {}}
last_trade_price = None
current_position_side = None
position_opened_at = 0.0
last_trade_time = 0.0

# 10-Sekunden-Normalisierer
obi_normalizer = OBINormalizer(window_seconds=10)

# Zeit-basierte Bestätigung
lean_direction = None
lean_since = 0.0


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
    """Berechnet den Roh-OBI (ohne Normalisierung)"""
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
        "normalisierter_OBI": round(obi_normalizer.get(), 3),
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
        debug_log(f"📊 10-Sekunden-Normalisierung aktiv | Schwelle: {OBI_THRESHOLD}")

        async for raw in ws:
            msg = json.loads(raw)
            channel = msg.get("channel", "")

            if channel.startswith("order_book"):
                apply_order_book_update(msg)
                
                # Roh-OBI berechnen und normalisieren
                raw_obi = calc_raw_obi()
                normalized_obi = obi_normalizer.add(raw_obi)
                
                now = time.time()

                # ===== Signal mit normalisiertem OBI =====
                if normalized_obi >= OBI_THRESHOLD:
                    current_lean = "buy"
                elif normalized_obi <= -OBI_THRESHOLD:
                    current_lean = "sell"
                else:
                    current_lean = None

                if current_lean != lean_direction:
                    lean_direction = current_lean
                    lean_since = now if current_lean is not None else 0.0
                elif current_lean is not None and (now - lean_since) >= OBI_CONFIRM_SECONDS and last_trade_price is not None:
                    await execute_signal(current_lean, last_trade_price)

                # ===== Status-Log =====
                if now - last_status_log >= STATUS_LOG_INTERVAL:
                    last_status_log = now
                    
                    if normalized_obi > 0.05:
                        richtung = "Käufer dominieren leicht" if normalized_obi < OBI_THRESHOLD else "Käufer dominieren STARK"
                    elif normalized_obi < -0.05:
                        richtung = "Verkäufer dominieren leicht" if normalized_obi > -OBI_THRESHOLD else "Verkäufer dominieren STARK"
                    else:
                        richtung = "ausgeglichen"

                    debug_log(f"📊 Status {SYMBOL} (10s-normalisiert)", {
                        "roh_OBI": round(raw_obi, 3),
                        "normalisierter_OBI": round(normalized_obi, 3),
                        "richtung": richtung,
                        "schwelle": OBI_THRESHOLD,
                        "buffer_groesse": obi_normalizer.get_raw_count(),
                        "letzter_preis": last_trade_price,
                        "bot_position": current_position_side or "flach",
                        "lean_haelt_seit_sekunden": round(now - lean_since, 1) if lean_direction else 0,
                        "braucht_sekunden_fuer_signal": OBI_CONFIRM_SECONDS,
                    })

            elif channel.startswith("trade"):
                trades = msg.get("trades", [])
                if trades:
                    last_trade_price = float(trades[-1]["price"])


async def main():
    global current_position_side

    print("=" * 60)
    print(f"🚀 Orderbuch-Bot mit 10-Sekunden-Normalisierung für {SYMBOL}")
    print(f"   DRY_RUN: {DRY_RUN}")
    print(f"   OBI Levels: {OBI_LEVELS} | Schwelle: {OBI_THRESHOLD}")
    print(f"   Bestätigung: {OBI_CONFIRM_SECONDS}s | Min. Haltedauer: {MIN_HOLD_SECONDS}s")
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
