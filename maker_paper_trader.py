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
OBI_THRESHOLD = float(os.getenv("OBI_THRESHOLD", "0.35"))  # 🆕 Erhöht für bessere Signale
OBI_AVG_WINDOW_SECONDS = float(os.getenv("OBI_AVG_WINDOW_SECONDS", "3"))  # 🆕 Kürzer für schnellere Reaktion
COOLDOWN_SECONDS = float(os.getenv("COOLDOWN_SECONDS", "3"))  # 🆕 Kürzer für mehr Trades

# 📊 TRADING PARAMETER - OPTIMIERT
TP_PERCENT = float(os.getenv("TP_PERCENT", "0.08"))  # 🆕 0.08% Gewinn (realistischer)
SL_PERCENT = float(os.getenv("SL_PERCENT", "0.06"))  # 🆕 0.06% Verlust (engerer SL)
MAX_POSITION_TIME = float(os.getenv("MAX_POSITION_TIME", "6"))  # 🆕 6 Sekunden halten
MIN_TRADE_INTERVAL = float(os.getenv("MIN_TRADE_INTERVAL", "2"))  # 🆕 2s zwischen Trades

# 🆕 MARGIN KONFIGURATION
ACCOUNT_BALANCE = float(os.getenv("ACCOUNT_BALANCE", "10000"))
LEVERAGE = float(os.getenv("LEVERAGE", "1.0"))
POSITION_SIZE_PCT = float(os.getenv("POSITION_SIZE_PCT", "100"))

# ========== STATE ==========
order_book = {"bids": {}, "asks": {}}
obi_avg_buffer = deque()

last_signal_direction = None
last_signal_time = 0.0
last_trade_time = 0.0

# Offene Position
open_position = None
position_opened_at = 0.0

# Statistik
stats = {
    "signals": 0,
    "trades_completed": 0,
    "wins": 0,
    "losses": 0,
    "total_pnl_pct": 0.0,
    "total_pnl_usd": 0.0,  # 🆕 USD PnL
    "avg_hold_time": 0.0,
    "total_hold_time": 0.0,
    "max_win": 0.0,
    "max_loss": 0.0,
    "current_balance": ACCOUNT_BALANCE,  # 🆕 Aktueller Kontostand
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


def calc_spread():
    """🆕 Berechnet den Spread in %"""
    bb, ba = best_bid(), best_ask()
    if bb is None or ba is None or bb == 0:
        return 0.0
    return (ba - bb) / bb * 100


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


# 🆕 POSITION SIZING
def calculate_position_size(entry_price):
    """Berechnet die Positionsgröße basierend auf Hebel"""
    base_size = stats["current_balance"] * (POSITION_SIZE_PCT / 100)
    leveraged_size = base_size * LEVERAGE
    units = leveraged_size / entry_price if entry_price > 0 else 0
    
    return {
        "usd_size": round(leveraged_size, 2),
        "units": round(units, 6),
        "leverage": LEVERAGE,
        "margin_used": round(leveraged_size / LEVERAGE if LEVERAGE > 0 else leveraged_size, 2)
    }


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
    
    # 🔥 KORREKTE ENTRY-PREISE
    if direction == "buy":
        entry_price = ba  # Kaufe zum Ask
    else:
        entry_price = bb  # Verkaufe zum Bid
    
    # 🆕 Positionsgröße berechnen
    position_info = calculate_position_size(entry_price)
    
    # Spread berechnen
    spread_pct = calc_spread()
    
    # Position eröffnen
    open_position = {
        "side": direction,
        "entry_price": entry_price,
        "entry_time": now,
        "entry_obi": avg_obi,
        "entry_bb": bb,
        "entry_ba": ba,
        "spread": spread_pct,
        "size_usd": position_info["usd_size"],
        "units": position_info["units"],
        "leverage": position_info["leverage"],
        "margin_used": position_info["margin_used"]
    }
    position_opened_at = now
    last_signal_time = now
    last_signal_direction = direction
    last_trade_time = now
    
    stats["signals"] += 1
    
    debug_log(
        f"💥 MARKET-ENTRY: {direction.upper()} @ {entry_price}\n"
        f"   📊 Spread: {round(spread_pct, 3)}% | Bid: {bb} | Ask: {ba}\n"
        f"   📊 Size: ${position_info['usd_size']} ({position_info['units']} {SYMBOL})\n"
        f"   🔧 Hebel: {position_info['leverage']}x | Margin: ${position_info['margin_used']}\n"
        f"   📈 OBI: {round(avg_obi, 3)}"
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
    
    # 🔥 KORREKTE PnL BERECHNUNG
    if side == "buy":
        # Bei Buy: Verkaufen zum besten Bid
        exit_price = best_bid()
        if exit_price is None:
            exit_price = last_trade_price
        pnl_pct = (exit_price - entry_price) / entry_price * 100
    else:
        # Bei Sell: Kaufen zum besten Ask
        exit_price = best_ask()
        if exit_price is None:
            exit_price = last_trade_price
        pnl_pct = (entry_price - exit_price) / entry_price * 100
    
    # 🆕 PnL in USD
    pnl_usd = pnl_pct / 100 * open_position["size_usd"]
    
    # 🎯 TAKE-PROFIT erreicht?
    if pnl_pct >= TP_PERCENT:
        close_position(exit_price, pnl_pct, pnl_usd, "TP")
        return
    
    # 🛑 STOP-LOSS erreicht?
    if pnl_pct <= -SL_PERCENT:
        close_position(exit_price, pnl_pct, pnl_usd, "SL")
        return
    
    # ⏰ TIMEOUT - zu lange offen
    hold_time = now - position_opened_at
    if hold_time > MAX_POSITION_TIME:
        close_position(exit_price, pnl_pct, pnl_usd, f"TIMEOUT ({round(hold_time, 1)}s)")
        return


def close_position(price, pnl_pct, pnl_usd, reason):
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
    stats["total_pnl_usd"] += pnl_usd
    stats["total_hold_time"] += hold_time
    stats["avg_hold_time"] = stats["total_hold_time"] / stats["trades_completed"]
    stats["current_balance"] += pnl_usd
    
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
        "pnl_usd": round(pnl_usd, 2),
        "reason": reason,
        "hold_time": round(hold_time, 2),
        "entry_obi": round(open_position.get("entry_obi", 0), 3),
        "leverage": open_position.get("leverage", 1.0),
        "size_usd": round(open_position.get("size_usd", 0), 2),
        "spread": round(open_position.get("spread", 0), 3),
        "closed_at": datetime.now().isoformat()
    }
    trade_log.append(trade_entry)
    
    # Ausführliches Log
    emoji = "✅" if pnl_pct > 0 else "❌"
    debug_log(
        f"{emoji} MARKET-EXIT: {side.upper()} @ {price} | "
        f"PnL: {round(pnl_pct, 2)}% (${round(pnl_usd, 2)}) | "
        f"Dauer: {round(hold_time, 2)}s | "
        f"Grund: {reason} | "
        f"Balance: ${round(stats['current_balance'], 2)}"
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
        "total_pnl_usd": round(stats["total_pnl_usd"], 2),
        "balance": round(stats["current_balance"], 2),
        "leverage": LEVERAGE,
        "max_win": round(stats["max_win"], 2),
        "max_loss": round(stats["max_loss"], 2),
        "avg_hold_time": round(stats["avg_hold_time"], 2),
        "offene_position": open_position is not None,
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
            status_data["spread"] = round(calc_spread(), 3)
    
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
        debug_log(f"💰 Hebel: {LEVERAGE}x | Balance: ${ACCOUNT_BALANCE}")

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
    print(f"   💰 Hebel: {LEVERAGE}x | Balance: ${ACCOUNT_BALANCE}")
    print(f"   🚀 Ausführung: SOFORTIGE MARKET-ORDERS")
    print(f"   ⚡ Ziel: 20-30 Trades/Stunde | Positionen < 6 Sekunden")
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
