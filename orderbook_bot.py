"""
Autonomer Orderbuch-Signal-Bot für Lighter (zkLighter)
========================================================
Verbindet sich direkt per WebSocket mit Lighter (keine TradingView-Abhängigkeit),
berechnet Order Book Imbalance (OBI) aus der Live-Orderbuch-Tiefe und tradet
autonom, wenn das Signal über mehrere aufeinanderfolgende Updates bestätigt wird.

WICHTIG:
- Läuft als eigener Prozess, NICHT im Flask-Prozess (asyncio + WebSocket
  passt nicht gut in den synchronen Flask-Loop).
- Importiert Hilfsfunktionen/Config aus deiner bestehenden Bot-Datei.
  Passe den Import unten an den Dateinamen deines Flask-Skripts an
  (hier angenommen: die Datei heißt "app.py" -> "from app import ...").
- Starte erst mit DRY_RUN=true, um zu sehen, wie oft/wann Signale
  entstehen, BEVOR echte Orders ausgelöst werden.
"""

import asyncio
import websockets
import json
import time
import os
from collections import deque
from datetime import datetime

# ==== Import aus deiner bestehenden Bot-Datei (webhook_server.py) ====
from webhook_server import (
    MARKET_INDICES,
    get_lighter_client,
    open_or_reverse_position,
    OPEN_POSITIONS,
    debug_log,
)

WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"

# ==== Konfiguration (per Umgebungsvariable überschreibbar) ====
SYMBOL = os.getenv("OB_SYMBOL", "BTC")
if SYMBOL not in MARKET_INDICES:
    raise ValueError(f"Symbol {SYMBOL} nicht in MARKET_INDICES gefunden")
MARKET_INDEX = MARKET_INDICES[SYMBOL]

OBI_LEVELS = int(os.getenv("OBI_LEVELS", "10"))            # Anzahl Preis-Level für OBI-Berechnung
OBI_THRESHOLD = float(os.getenv("OBI_THRESHOLD", "0.30"))  # Wert zwischen 0 und 1
OBI_CONFIRM_TICKS = int(os.getenv("OBI_CONFIRM_TICKS", "5"))  # so viele Updates in Folge nötig
COOLDOWN_SECONDS = float(os.getenv("COOLDOWN_SECONDS", "30"))  # Mindestabstand zwischen Trades

MARGIN = float(os.getenv("OB_MARGIN", "100"))
LEVERAGE = int(os.getenv("OB_LEVERAGE", "10"))

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

# ==== Lokaler State ====
order_book = {"bids": {}, "asks": {}}   # price(str) -> size(float)
last_trade_price = None
current_position_side = None            # 'buy' (long) / 'sell' (short) / None
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
        return
    if current_position_side == direction:
        return  # schon in dieser Richtung positioniert, kein erneutes Signal nötig

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

    result = await open_or_reverse_position(direction, SYMBOL, MARGIN, LEVERAGE, price)
    debug_log("Order-Ergebnis", result)

    current_position_side = direction
    last_trade_time = now


async def listen():
    global last_trade_price

    async with websockets.connect(WS_URL, ping_interval=20) as ws:
        await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book:{MARKET_INDEX}"}))
        await ws.send(json.dumps({"type": "subscribe", "channel": f"trade:{MARKET_INDEX}"}))

        debug_log(f"✅ Verbunden, abonniert order_book:{MARKET_INDEX} und trade:{MARKET_INDEX}")

        async for raw in ws:
            msg = json.loads(raw)
            channel = msg.get("channel", "")

            if channel.startswith("order_book"):
                apply_order_book_update(msg)
                obi = calc_obi()
                obi_history.append(obi)

                if len(obi_history) == OBI_CONFIRM_TICKS and last_trade_price is not None:
                    if all(v >= OBI_THRESHOLD for v in obi_history):
                        await execute_signal("buy", last_trade_price)
                    elif all(v <= -OBI_THRESHOLD for v in obi_history):
                        await execute_signal("sell", last_trade_price)

            elif channel.startswith("trade"):
                trades = msg.get("trades", [])
                if trades:
                    last_trade_price = float(trades[-1]["price"])


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
            debug_log("⚠️ WebSocket-Verbindung verloren, reconnect in 5s", {"error": str(e)})
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
