"""
Autonomer Orderbuch-Signal-Bot für Lighter (zkLighter)
========================================================
Verbindet sich direkt per WebSocket mit Lighter,
berechnet Order Book Imbalance (OBI) aus der Live-Orderbuch-Tiefe und tradet
autonom, wenn das Signal über mehrere aufeinanderfolgende Updates bestätigt wird.
"""

import asyncio
import websockets
import json
import time
import os
from collections import deque
from datetime import datetime

# ===== KONFIGURATION (ohne Import) =====

# Market Indices - HIER MUSST DU DIE RICHTIGEN WERTE EINTRAGEN!
# Diese findest du in der Lighter Dokumentation oder API
MARKET_INDICES = {
    "BTC": 1,    # Beispiel - BITTE ÜBERPRÜFEN!
    "ETH": 2,    # Beispiel - BITTE ÜBERPRÜFEN!
    "SOL": 3,    # Beispiel - BITTE ÜBERPRÜFEN!
}

OPEN_POSITIONS = {}

def debug_log(message, data=None):
    """Einfache Logging-Funktion"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if data:
        print(f"[{timestamp}] {message}: {json.dumps(data, indent=2)}")
    else:
        print(f"[{timestamp}] {message}")

async def get_lighter_client():
    """Lighter Client - falls du einen brauchst"""
    # Hier kommt deine Lighter-Client-Initialisierung rein
    pass

async def open_or_reverse_position(direction, symbol, margin, leverage, price):
    """
    Trading-Funktion - HIER DIE ECHTE LOGIK EINFÜGEN!
    
    Diese Funktion wird aufgerufen, wenn ein Trade-Signal ausgelöst wird.
    """
    debug_log(f"🔴 TRADE SIGNAL: {direction.upper()} {symbol} @ {price}", {
        "direction": direction,
        "symbol": symbol,
        "margin": margin,
        "leverage": leverage,
        "price": price
    })
    
    # HIER DIE ECHTE TRADING-LOGIK EINFÜGEN
    # Beispiel: Order platzieren, Position öffnen/schließen, etc.
    
    # Für DRY_RUN nur loggen
    return {
        "status": "dry_run",
        "direction": direction,
        "symbol": symbol,
        "price": price,
        "margin": margin,
        "leverage": leverage
    }

# ===== BOT CODE =====

WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"

# Konfiguration (per Umgebungsvariable überschreibbar)
SYMBOL = os.getenv("OB_SYMBOL", "ETH")
if SYMBOL not in MARKET_INDICES:
    raise ValueError(f"Symbol {SYMBOL} nicht in MARKET_INDICES gefunden")
MARKET_INDEX = MARKET_INDICES[SYMBOL]

OBI_LEVELS = int(os.getenv("OBI_LEVELS", "10"))
OBI_THRESHOLD = float(os.getenv("OBI_THRESHOLD", "0.30"))
OBI_CONFIRM_TICKS = int(os.getenv("OBI_CONFIRM_TICKS", "5"))
COOLDOWN_SECONDS = float(os.getenv("COOLDOWN_SECONDS", "30"))
MARGIN = float(os.getenv("OB_MARGIN", "100"))
LEVERAGE = int(os.getenv("OB_LEVERAGE", "10"))
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

# Lokaler State
order_book = {"bids": {}, "asks": {}}
last_trade_price = None
current_position_side = None
last_trade_time = 0.0
obi_history = deque(maxlen=OBI_CONFIRM_TICKS)


def apply_order_book_update(msg):
    """Wendet Snapshot oder Delta-Update auf den lokalen Orderbuch-State an."""
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
    """Order Book Imbalance: +1 = nur Käufer-Druck, -1 = nur Verkäufer-Druck."""
    bids_sorted = sorted(order_book["bids"].items(), key=lambda x: float(x[0]), reverse=True)[:levels]
    asks_sorted = sorted(order_book["asks"].items(), key=lambda x: float(x[0]))[:levels]

    bid_vol = sum(v for _, v in bids_sorted)
    ask_vol = sum(v for _, v in asks_sorted)

    total = bid_vol + ask_vol
    if total == 0:
        return 0.0
    return (bid_vol - ask_vol) / total


async def execute_signal(direction, price):
    global current_position_side, last_trade_time

    now = time.time()
    if now - last_trade_time < COOLDOWN_SECONDS:
        debug_log(f"⏳ Cooldown aktiv - noch {COOLDOWN_SECONDS - (now - last_trade_time):.1f}s warten")
        return
    
    if current_position_side == direction:
        debug_log(f"ℹ️ Bereits in {direction} Position, kein erneutes Signal nötig")
        return

    debug_log(f"📡 OBI-Signal: {direction.upper()} {SYMBOL} @ {price}", {
        "symbol": SYMBOL,
        "direction": direction,
        "price": price,
        "obi_history": list(obi_history),
    })

    if DRY_RUN:
        debug_log("🧪 DRY_RUN aktiv - keine echte Order ausgeführt")
        current_position_side = direction
        last_trade_time = now
        return

    # Echte Order ausführen
    result = await open_or_reverse_position(direction, SYMBOL, MARGIN, LEVERAGE, price)
    debug_log("✅ Order-Ergebnis", result)

    current_position_side = direction
    last_trade_time = now


async def listen():
    global last_trade_price

    debug_log(f"🔄 Verbinde zu {WS_URL}...")
    
    try:
        async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=60) as ws:
            # Subscribe zu Order Book
            await ws.send(json.dumps({
                "type": "subscribe", 
                "channel": f"order_book:{MARKET_INDEX}"
            }))
            
            # Subscribe zu Trades
            await ws.send(json.dumps({
                "type": "subscribe", 
                "channel": f"trade:{MARKET_INDEX}"
            }))

            debug_log(f"✅ Verbunden, abonniert order_book:{MARKET_INDEX} und trade:{MARKET_INDEX}")

            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    channel = msg.get("channel", "")

                    if channel.startswith("order_book"):
                        apply_order_book_update(msg)
                        obi = calc_obi()
                        obi_history.append(obi)
                        
                        # Prüfe ob Signal bestätigt ist
                        if len(obi_history) == OBI_CONFIRM_TICKS and last_trade_price is not None:
                            if all(v >= OBI_THRESHOLD for v in obi_history):
                                await execute_signal("buy", last_trade_price)
                            elif all(v <= -OBI_THRESHOLD for v in obi_history):
                                await execute_signal("sell", last_trade_price)

                    elif channel.startswith("trade"):
                        trades = msg.get("trades", [])
                        if trades:
                            last_trade_price = float(trades[-1]["price"])
                            debug_log(f"💹 Letzter Trade: {last_trade_price}")
                            
                except json.JSONDecodeError as e:
                    debug_log("⚠️ JSON Parse Fehler", {"error": str(e), "raw": raw[:100]})
                except Exception as e:
                    debug_log("⚠️ Fehler beim Verarbeiten der Nachricht", {"error": str(e)})
                    
    except websockets.exceptions.ConnectionClosed:
        debug_log("🔌 WebSocket-Verbindung geschlossen")
        raise
    except Exception as e:
        debug_log("❌ WebSocket-Fehler", {"error": str(e)})
        raise


async def main():
    print("=" * 60)
    print(f"🚀 Orderbuch-Bot gestartet für {SYMBOL} (Market Index {MARKET_INDEX})")
    print(f"   DRY_RUN: {DRY_RUN}")
    print(f"   OBI Levels: {OBI_LEVELS} | Schwelle: {OBI_THRESHOLD} | Bestätigung: {OBI_CONFIRM_TICKS} Ticks")
    print(f"   Margin: {MARGIN} USDC | Hebel: {LEVERAGE}x | Cooldown: {COOLDOWN_SECONDS}s")
    print("=" * 60)

    while True:
        try:
            await listen()
        except Exception as e:
            debug_log("⚠️ Verbindung verloren, reconnect in 5s", {"error": str(e)})
            await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Bot beendet")
