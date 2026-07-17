"""
OBI Market-Trader für Lighter (zkLighter)
==========================================
Schnelle Market-Order Ausführung für OBI-basierte Signale.
Positionen werden blitzschnell eröffnet und geschlossen.

STRATEGIE:
- OBI-Signal → Sofortiger Market-Entry
- Take-Profit bei 0.1-0.2% → Market-Exit
- Stop-Loss bei 0.1-0.15% → Market-Exit
- Max 5 Sekunden halten → Timeout-Exit

VORTEILE:
- Blitzschnelle Ausführung (< 1 Sekunde)
- Nutzt kurzfristige OBI-Signale optimal
- Hohe Trade-Frequenz (20-30 pro Stunde)
- Kein Warten auf Order-Fills

NACHTEILE:
- Zahlt Spread (0.05-0.1%)
- Höhere Gebühren (Taker)
- Aber: Schnelligkeit > Spread-Kosten
"""

import asyncio
import websockets
import json
import time
import os
from collections import deque
from datetime import datetime

WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"

DEBUG_MODE = os.getenv("DEBUG_MODE", "true").lower() == "true"


def debug_log(msg, data=None):
    if DEBUG_MODE:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        print(f"[DEBUG {timestamp}] {msg}", flush=True)
        if data:
            print(f"   DATA: {json.dumps(data, indent=2, default=str)}", flush=True)


# ========== KONFIGURATION ==========
MARKET_INDICES = {"ETH": 0, "BTC": 1, "SOL": 2, "DOGE": 3, "AVAX": 9, "SUI": 16}
SYMBOL = os.getenv("OB_SYMBOL", "BTC")
MARKET_INDEX = MARKET_INDICES[SYMBOL]

# OBI Konfiguration
OBI_LEVELS = int(os.getenv("OBI_LEVELS", "25"))
OBI_THRESHOLD = float(os.getenv("OBI_THRESHOLD", "0.30"))
OBI_AVG_WINDOW_SECONDS = float(os.getenv("OBI_AVG_WINDOW_SECONDS", "5"))  # Kürzer für schnellere Signale
COOLDOWN_SECONDS = float(os.getenv("COOLDOWN_SECONDS", "5"))  # Kürzer für mehr Trades

# 📊 TRADING PARAMETER
TP_PERCENT = float(os.getenv("TP_PERCENT", "0.10"))  # 0.1% Gewinn
SL_PERCENT = float(os.getenv("SL_PERCENT", "0.10"))  # 0.1% Verlust
MAX_POSITION_TIME = float(os.getenv("MAX_POSITION_TIME", "5"))  # Max 5 Sekunden halten
MIN_TRADE_INTERVAL = float(os.getenv("MIN_TRADE_INTERVAL", "3"))  # Mindestens 3s zwischen Trades

# ========== STATE ==========
order_book = {"bids": {}, "asks": {}}
obi_avg_buffer = deque()

last_signal_direction = None
last_signal_time = 0.0
last_trade_time = 0.0

# Offene Position
open_position = None  # {"side": "buy"/"sell", "entry_price": x, "entry_time": ts, "entry_trade_id": x}
position_opened_at = 0.0

# Statistik
stats = {
    "signals": 0,
    "trades_completed": 0,
    "wins": 0,
    "losses": 0,
    "total_pnl_pct": 0.0,
    "avg_hold_time": 0.0,
    "total_hold_time": 0.0,
    "max_win": 0.0,
    "max_loss": 0.0,
}
trade_log = []


# ========== ORDERBOOK FUNKTIONEN ==========
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


def best_bid():
    if not order_book["bids"]:
        return None
    return max(float(p) for p in order_book["bids"].keys())


def best_ask():
    if not order_book["asks"]:
        return None
    return min(float(p) for p in order_book["asks"].keys())


def mid_price():
    bb, ba = best_bid(), best_ask()
    if bb is None or ba is None:
        return None
    return (bb + ba) / 2


def calc_obi(levels=OBI_LEVELS):
    bids_sorted = sorted(order_book["bids"].items(), key=lambda x: float(x[0]), reverse=True)[:levels]
    asks_sorted = sorted(order_book["asks"].items(), key=lambda x: float(x[0]))[:levels]
    bid_vol = sum(v for _, v in bids_sorted)
    ask_vol = sum(v for _, v in asks_sorted)
    total = bid_vol + ask_vol
    return 0.0 if total == 0 else (bid_vol - ask_vol) / total


def update_obi_average(raw_obi):
    """Gleitender Durchschnitt über OBI_AVG_WINDOW_SECONDS"""
    now = time.time()
    obi_avg_buffer.append((raw_obi, now))
    cutoff = now - OBI_AVG_WINDOW_SECONDS
    while obi_avg_buffer and obi_avg_buffer[0][1] < cutoff:
        obi_avg_buffer.popleft()
    if not obi_avg_buffer:
        return 0.0
    return sum(v for v, _ in obi_avg_buffer) / len(obi_avg_buffer)


# ========== TRADING LOGIK ==========
def check_signal_and_execute(avg_obi):
    """
    Prüft OBI-Signal und führt SOFORT eine Market-Order aus.
    Kein Warten - sofortige Ausführung!
    """
    global open_position, position_opened_at, last_signal_time, last_signal_direction, last_trade_time
    
    now = time.time()
    
    # Mindestabstand zwischen Trades
    if now - last_trade_time < MIN_TRADE_INTERVAL:
        return
    
    # Signal erkennen
    if avg_obi >= OBI_THRESHOLD:
        direction = "buy"
    elif avg_obi <= -OBI_THRESHOLD:
        direction = "sell"
    else:
        last_signal_direction = None
        return
    
    # Cooldown prüfen (verhindert Doppeltrades)
    if now - last_signal_time < COOLDOWN_SECONDS:
        return
    
    # Keine doppelten Signale in gleicher Richtung
    if direction == last_signal_direction:
        return
    
    # Nur handeln wenn keine Position offen
    if open_position is not None:
        return
    
    # 💥 SOFORTIGE MARKET-EXECUTION
    bb, ba = best_bid(), best_ask()
    if bb is None or ba is None:
        debug_log("⚠️ Kein OrderBook für Entry verfügbar")
        return
    
    # Market-Preis = Best Ask (für Buy) oder Best Bid (für Sell)
    if direction == "buy":
        entry_price = ba  # Kaufe zum besten Ask
    else:
        entry_price = bb  # Verkaufe zum besten Bid
    
    # Position eröffnen
    open_position = {
        "side": direction,
        "entry_price": entry_price,
        "entry_time": now,
        "entry_obi": avg_obi,
        "entry_bb": bb,
        "entry_ba": ba
    }
    position_opened_at = now
    last_signal_time = now
    last_signal_direction = direction
    last_trade_time = now
    
    stats["signals"] += 1
    
    # Spread-Log
    spread_pct = (ba - bb) / bb * 100 if bb > 0 else 0
    
    debug_log(
        f"💥 MARKET-ENTRY: {direction.upper()} @ {entry_price} "
        f"(OBI: {round(avg_obi, 3)}, Spread: {round(spread_pct, 2)}%, "
        f"Bid: {bb}, Ask: {ba})"
    )


def check_position_exit(last_trade_price, last_trade_received_at):
    """
    Prüft ob Position geschlossen werden muss.
    Wird bei jedem neuen Trade-Event aufgerufen.
    """
    global open_position, position_opened_at
    
    if open_position is None or last_trade_price is None:
        return
    
    entry_price = open_position["entry_price"]
    side = open_position["side"]
    now = time.time()
    
    # Berechne aktuellen PnL
    if side == "buy":
        pnl_pct = (last_trade_price - entry_price) / entry_price * 100
        # Für Buy: Verkaufen zum besten Bid (nicht zum Trade-Preis)
        exit_price = best_bid()
    else:
        pnl_pct = (entry_price - last_trade_price) / entry_price * 100
        # Für Sell: Kaufen zum besten Ask (nicht zum Trade-Preis)
        exit_price = best_ask()
    
    # Falls kein Market-Preis verfügbar, Trade-Preis verwenden
    if exit_price is None:
        exit_price = last_trade_price
        debug_log(f"⚠️ Kein Market-Preis, verwende Trade-Price: {exit_price}")
    
    # 🎯 TAKE-PROFIT erreicht?
    if pnl_pct >= TP_PERCENT:
        close_position(exit_price, pnl_pct, "TP")
        return
    
    # 🛑 STOP-LOSS erreicht?
    if pnl_pct <= -SL_PERCENT:
        close_position(exit_price, pnl_pct, "SL")
        return
    
    # ⏰ TIMEOUT - zu lange offen
    hold_time = now - position_opened_at
    if hold_time > MAX_POSITION_TIME:
        close_position(exit_price, pnl_pct, f"TIMEOUT ({round(hold_time, 1)}s)")
        return
    
    # 📉 Bei negativem PnL und > 3 Sekunden: Vorsichtiger Exit
    if pnl_pct < -0.02 and hold_time > 3:
        debug_log(f"⚠️ Leichter Verlust ({round(pnl_pct, 2)}%) nach {round(hold_time, 1)}s - erwäge Exit")
        # Wenn der Verlust wächst, früher aussteigen
        if pnl_pct < -0.05:
            close_position(exit_price, pnl_pct, "EARLY_SL")


def close_position(price, pnl_pct, reason):
    """
    Schließt Position mit Market-Order.
    💥 Sofortige Ausführung!
    """
    global open_position, position_opened_at, last_trade_time
    
    if open_position is None:
        return
    
    side = open_position["side"]
    entry_price = open_position["entry_price"]
    hold_time = time.time() - position_opened_at
    
    # 💥 MARKET-EXIT
    stats["trades_completed"] += 1
    stats["total_pnl_pct"] += pnl_pct
    stats["total_hold_time"] += hold_time
    stats["avg_hold_time"] = stats["total_hold_time"] / stats["trades_completed"]
    
    if pnl_pct > 0:
        stats["wins"] += 1
        if pnl_pct > stats["max_win"]:
            stats["max_win"] = pnl_pct
    else:
        stats["losses"] += 1
        if pnl_pct < stats["max_loss"]:
            stats["max_loss"] = pnl_pct
    
    # Trade loggen
    trade_entry = {
        "side": side,
        "entry": round(entry_price, 2),
        "exit": round(price, 2),
        "pnl_pct": round(pnl_pct, 4),
        "pnl_abs": round((price - entry_price) if side == "buy" else (entry_price - price), 2),
        "reason": reason,
        "hold_time": round(hold_time, 2),
        "entry_obi": round(open_position.get("entry_obi", 0), 3),
        "closed_at": datetime.now().isoformat()
    }
    trade_log.append(trade_entry)
    
    # Ausführliches Log
    emoji = "✅" if pnl_pct > 0 else "❌"
    debug_log(
        f"{emoji} MARKET-EXIT: {side.upper()} @ {price} | "
        f"PnL: {round(pnl_pct, 2)}% | "
        f"Dauer: {round(hold_time, 2)}s | "
        f"Grund: {reason}"
    )
    
    # Position zurücksetzen
    open_position = None
    position_opened_at = 0.0
    last_trade_time = time.time()


def log_status(raw_obi, avg_obi):
    """Loggt den aktuellen Status"""
    win_rate = round(stats["wins"] / stats["trades_completed"] * 100, 1) if stats["trades_completed"] else 0
    avg_pnl = round(stats["total_pnl_pct"] / stats["trades_completed"], 4) if stats["trades_completed"] else 0
    
    status_data = {
        "obi_roh": round(raw_obi, 3),
        "obi_avg": round(avg_obi, 3),
        "signale": stats["signals"],
        "trades": stats["trades_completed"],
        "win_rate": win_rate,
        "avg_pnl_pct": avg_pnl,
        "total_pnl_pct": round(stats["total_pnl_pct"], 4),
        "max_win": round(stats["max_win"], 2),
        "max_loss": round(stats["max_loss"], 2),
        "avg_hold_time": round(stats["avg_hold_time"], 2),
        "offene_position": open_position,
        "last_trade": trade_log[-1] if trade_log else None
    }
    
    if open_position:
        hold_time = time.time() - position_opened_at
        status_data["position_dauer"] = round(hold_time, 1)
        bb, ba = best_bid(), best_ask()
        if bb and ba:
            if open_position["side"] == "buy":
                unrealized = (bb - open_position["entry_price"]) / open_position["entry_price"] * 100
            else:
                unrealized = (open_position["entry_price"] - ba) / open_position["entry_price"] * 100
            status_data["unrealized_pnl"] = round(unrealized, 2)
    
    debug_log("📊 OBI Market-Trader Status", status_data)


# ========== WEBSOCKET VERBINDUNG ==========
async def listen():
    last_trade_price = None
    last_trade_received_at = 0.0
    last_status_log = 0.0
    last_obi_check = 0.0
    
    async with websockets.connect(WS_URL, ping_interval=20) as ws:
        await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{MARKET_INDEX}"}))
        await ws.send(json.dumps({"type": "subscribe", "channel": f"trade/{MARKET_INDEX}"}))
        debug_log(f"✅ Verbunden für {SYMBOL} (Market Index {MARKET_INDEX})")
        debug_log(f"⚡ Strategie: Market-Execution | TP: {TP_PERCENT}% | SL: {SL_PERCENT}% | Max-Halt: {MAX_POSITION_TIME}s")

        async for raw in ws:
            msg = json.loads(raw)
            channel = msg.get("channel", "")

            if channel.startswith("order_book"):
                apply_order_book_update(msg)
                
                # OBI berechnen (aber nicht zu oft)
                now = time.time()
                if now - last_obi_check > 0.5:  # Max 2x pro Sekunde
                    raw_obi = calc_obi()
                    avg_obi = update_obi_average(raw_obi)
                    last_obi_check = now
                    
                    # 💥 Bei Signal: Sofort Market-Entry
                    check_signal_and_execute(avg_obi)
                    
                    # Status alle 30s
                    if now - last_status_log >= 30:
                        last_status_log = now
                        log_status(raw_obi, avg_obi)

            elif channel.startswith("trade"):
                trades = msg.get("trades", [])
                if trades:
                    last_trade_price = float(trades[-1]["price"])
                    last_trade_received_at = time.time()
                    
                    # 💥 Bei jedem Trade: Position prüfen
                    if open_position is not None:
                        check_position_exit(last_trade_price, last_trade_received_at)


# ========== MAIN ==========
async def main():
    print("=" * 70)
    print(f"⚡ OBI MARKET-TRADER für {SYMBOL}")
    print(f"   📊 OBI Schwelle: {OBI_THRESHOLD} | Ø-Fenster: {OBI_AVG_WINDOW_SECONDS}s")
    print(f"   🎯 TP: {TP_PERCENT}% | 🛑 SL: {SL_PERCENT}% | ⏰ Max-Halt: {MAX_POSITION_TIME}s")
    print(f"   🚀 Ausführung: SOFORTIGE MARKET-ORDERS")
    print(f"   ⚡ Ziel: 20-30 Trades/Stunde | Positionen < 5 Sekunden")
    print("=" * 70)
    print("   💰 Vorteile: Blitzschnell, nutzt OBI-Signale optimal")
    print("   💸 Nachteile: Zahlt Spread (0.05-0.1%)")
    print("   🎯 Fazit: Schnelligkeit > Spread-Kosten!")
    print("=" * 70)
    print()

    while True:
        try:
            await listen()
        except Exception as e:
            debug_log("⚠️ Verbindung verloren, reconnect in 3s", {"error": str(e)})
            await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(main())
