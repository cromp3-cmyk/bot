"""
OBI Market-Trader mit dynamischem TP/SL
========================================
TP/SL passen sich automatisch an die Marktbewegung an.
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
OBI_THRESHOLD = float(os.getenv("OBI_THRESHOLD", "0.35"))
OBI_AVG_WINDOW_SECONDS = float(os.getenv("OBI_AVG_WINDOW_SECONDS", "3"))
COOLDOWN_SECONDS = float(os.getenv("COOLDOWN_SECONDS", "3"))

# 🆕 DYNAMISCHE TRADING PARAMETER
BASE_TP_PERCENT = float(os.getenv("TP_PERCENT", "0.06"))  # Basis TP
BASE_SL_PERCENT = float(os.getenv("SL_PERCENT", "0.04"))  # Basis SL
MAX_POSITION_TIME = float(os.getenv("MAX_POSITION_TIME", "8"))  # 🆕 Länger für mehr Bewegung
MIN_TRADE_INTERVAL = float(os.getenv("MIN_TRADE_INTERVAL", "2"))

# Margin
ACCOUNT_BALANCE = float(os.getenv("ACCOUNT_BALANCE", "10000"))
LEVERAGE = float(os.getenv("LEVERAGE", "1.0"))
POSITION_SIZE_PCT = float(os.getenv("POSITION_SIZE_PCT", "100"))

# ========== STATE ==========
order_book = {"bids": {}, "asks": {}}
obi_avg_buffer = deque()
price_history = deque(maxlen=20)  # 🆕 Für Volatilitätsberechnung

last_signal_direction = None
last_signal_time = 0.0
last_trade_time = 0.0

open_position = None
position_opened_at = 0.0

stats = {
    "signals": 0,
    "trades_completed": 0,
    "wins": 0,
    "losses": 0,
    "total_pnl_pct": 0.0,
    "total_pnl_usd": 0.0,
    "avg_hold_time": 0.0,
    "total_hold_time": 0.0,
    "max_win": 0.0,
    "max_loss": 0.0,
    "current_balance": ACCOUNT_BALANCE,
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

def calc_spread():
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
    now = time.time()
    obi_avg_buffer.append((raw_obi, now))
    cutoff = now - OBI_AVG_WINDOW_SECONDS
    while obi_avg_buffer and obi_avg_buffer[0][1] < cutoff:
        obi_avg_buffer.popleft()
    if not obi_avg_buffer:
        return 0.0
    return sum(v for v, _ in obi_avg_buffer) / len(obi_avg_buffer)

# 🆕 VOLATILITÄT & DYNAMISCHE TP/SL
def calc_volatility():
    """Berechnet die Volatilität aus den letzten Preisbewegungen"""
    if len(price_history) < 5:
        return 0.02  # Standard: 0.02% pro 5 Sekunden
    
    prices = list(price_history)
    changes = []
    for i in range(1, len(prices)):
        if prices[i-1] > 0:
            change = abs(prices[i] - prices[i-1]) / prices[i-1] * 100
            changes.append(change)
    
    if not changes:
        return 0.02
    
    # Durchschnittliche Veränderung pro Tick
    avg_change = sum(changes) / len(changes)
    # Hochrechnen auf 5 Sekunden
    avg_change_5s = avg_change * (5 / (len(prices) * 0.5))  # Schätzung
    return max(0.01, min(0.15, avg_change_5s))

def calculate_dynamic_tp_sl():
    """Berechnet dynamische TP/SL basierend auf Volatilität"""
    volatility = calc_volatility()
    
    # 🆕 TP = Volatilität * 2.5 (etwas höher für Gewinnchance)
    tp = volatility * 2.5
    # 🆕 SL = Volatilität * 1.5 (enger für Risikomanagement)
    sl = volatility * 1.5
    
    # Begrenzungen
    tp = max(0.02, min(0.15, tp))
    sl = max(0.015, min(0.10, sl))
    
    return tp, sl

def calculate_position_size(entry_price):
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
    global open_position, position_opened_at, last_signal_time, last_signal_direction, last_trade_time
    
    now = time.time()
    
    if now - last_trade_time < MIN_TRADE_INTERVAL:
        return
    
    if avg_obi >= OBI_THRESHOLD:
        direction = "buy"
    elif avg_obi <= -OBI_THRESHOLD:
        direction = "sell"
    else:
        last_signal_direction = None
        return
    
    if now - last_signal_time < COOLDOWN_SECONDS:
        return
    if direction == last_signal_direction:
        return
    if open_position is not None:
        return
    
    bb, ba = best_bid(), best_ask()
    if bb is None or ba is None:
        debug_log("⚠️ Kein OrderBook für Entry verfügbar")
        return
    
    # Korrekte Entry-Preise
    if direction == "buy":
        entry_price = ba
    else:
        entry_price = bb
    
    # 🆕 Dynamische TP/SL berechnen
    tp, sl = calculate_dynamic_tp_sl()
    
    position_info = calculate_position_size(entry_price)
    spread_pct = calc_spread()
    
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
        "margin_used": position_info["margin_used"],
        "tp": tp,  # 🆕 Dynamischer TP
        "sl": sl,  # 🆕 Dynamischer SL
    }
    position_opened_at = now
    last_signal_time = now
    last_signal_direction = direction
    last_trade_time = now
    stats["signals"] += 1
    
    debug_log(
        f"💥 MARKET-ENTRY: {direction.upper()} @ {entry_price}\n"
        f"   📊 Spread: {round(spread_pct, 3)}% | Bid: {bb} | Ask: {ba}\n"
        f"   🎯 TP: {round(tp, 2)}% | 🛑 SL: {round(sl, 2)}%\n"
        f"   📊 Size: ${position_info['usd_size']} ({position_info['units']} {SYMBOL})\n"
        f"   📈 OBI: {round(avg_obi, 3)}"
    )

def check_position_exit(last_trade_price, last_trade_received_at):
    global open_position, position_opened_at
    
    if open_position is None or last_trade_price is None:
        return
    
    entry_price = open_position["entry_price"]
    side = open_position["side"]
    now = time.time()
    
    # 🆕 Dynamische TP/SL aus der Position
    tp = open_position.get("tp", BASE_TP_PERCENT)
    sl = open_position.get("sl", BASE_SL_PERCENT)
    
    # Exit-Preis und PnL berechnen
    if side == "buy":
        exit_price = best_bid()
        if exit_price is None:
            exit_price = last_trade_price
        pnl_pct = (exit_price - entry_price) / entry_price * 100
    else:
        exit_price = best_ask()
        if exit_price is None:
            exit_price = last_trade_price
        pnl_pct = (entry_price - exit_price) / entry_price * 100
    
    pnl_usd = pnl_pct / 100 * open_position["size_usd"]
    hold_time = now - position_opened_at
    
    # 🎯 DYNAMISCHER TAKE-PROFIT
    if pnl_pct >= tp:
        close_position(exit_price, pnl_pct, pnl_usd, f"TP ({round(tp, 2)}%)")
        return
    
    # 🛑 DYNAMISCHER STOP-LOSS
    if pnl_pct <= -sl:
        close_position(exit_price, pnl_pct, pnl_usd, f"SL ({round(sl, 2)}%)")
        return
    
    # ⏰ TIMEOUT - aber nur wenn keine Bewegung
    if hold_time > MAX_POSITION_TIME:
        # 🆕 Prüfen ob sich der Preis bewegt hat
        if abs(pnl_pct) < 0.005:  # Weniger als 0.005% Bewegung
            close_position(exit_price, pnl_pct, pnl_usd, f"NO_MOVEMENT ({round(hold_time, 1)}s)")
        else:
            # Es gibt Bewegung, länger warten
            debug_log(f"⏳ Bewegung erkannt ({round(pnl_pct, 2)}%), weiter warten...")
            # Timeout verlängern
            if hold_time < MAX_POSITION_TIME * 2:
                return
    
    # 🆕 Nach 10 Sekunden immer schließen
    if hold_time > MAX_POSITION_TIME * 2:
        close_position(exit_price, pnl_pct, pnl_usd, f"FORCE_CLOSE ({round(hold_time, 1)}s)")

def close_position(price, pnl_pct, pnl_usd, reason):
    global open_position, position_opened_at, last_trade_time
    
    if open_position is None:
        return
    
    side = open_position["side"]
    entry_price = open_position["entry_price"]
    hold_time = time.time() - position_opened_at
    
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
    
    trade_entry = {
        "side": side,
        "entry": round(entry_price, 2),
        "exit": round(price, 2),
        "pnl_pct": round(pnl_pct, 4),
        "pnl_usd": round(pnl_usd, 2),
        "reason": reason,
        "hold_time": round(hold_time, 2),
        "entry_obi": round(open_position.get("entry_obi", 0), 3),
        "tp": round(open_position.get("tp", 0), 2),
        "sl": round(open_position.get("sl", 0), 2),
        "leverage": open_position.get("leverage", 1.0),
        "size_usd": round(open_position.get("size_usd", 0), 2),
        "closed_at": datetime.now().isoformat()
    }
    trade_log.append(trade_entry)
    
    emoji = "✅" if pnl_pct > 0 else "❌"
    debug_log(
        f"{emoji} MARKET-EXIT: {side.upper()} @ {price} | "
        f"PnL: {round(pnl_pct, 2)}% (${round(pnl_usd, 2)}) | "
        f"Dauer: {round(hold_time, 2)}s | "
        f"Grund: {reason} | "
        f"Balance: ${round(stats['current_balance'], 2)}"
    )
    
    open_position = None
    position_opened_at = 0.0
    last_trade_time = time.time()

def log_status(raw_obi, avg_obi):
    win_rate = stats["wins"] / stats["trades_completed"] * 100 if stats["trades_completed"] else 0
    avg_pnl = stats["total_pnl_pct"] / stats["trades_completed"] if stats["trades_completed"] else 0
    
    status_data = {
        "obi": round(raw_obi, 3),
        "obi_avg": round(avg_obi, 3),
        "signale": stats["signals"],
        "trades": stats["trades_completed"],
        "win_rate": round(win_rate, 1),
        "avg_pnl": round(avg_pnl, 4),
        "total_pnl_pct": round(stats["total_pnl_pct"], 2),
        "total_pnl_usd": round(stats["total_pnl_usd"], 2),
        "balance": round(stats["current_balance"], 2),
        "volatility": round(calc_volatility(), 3),
        "leverage": LEVERAGE,
        "open_position": open_position is not None,
        "last_trade": trade_log[-1] if trade_log else None
    }
    
    if open_position:
        status_data["tp"] = round(open_position.get("tp", 0), 2)
        status_data["sl"] = round(open_position.get("sl", 0), 2)
        status_data["hold_time"] = round(time.time() - position_opened_at, 1)
    
    debug_log("📊 OBI Market-Trader Status", status_data)

# ========== WEBSOCKET ==========
async def listen():
    last_trade_price = None
    last_trade_received_at = 0.0
    last_status_log = 0.0
    last_obi_check = 0.0
    
    async with websockets.connect(WS_URL, ping_interval=20) as ws:
        await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{MARKET_INDEX}"}))
        await ws.send(json.dumps({"type": "subscribe", "channel": f"trade/{MARKET_INDEX}"}))
        debug_log(f"✅ Verbunden für {SYMBOL}")
        debug_log(f"⚡ Dynamische TP/SL | Max-Halt: {MAX_POSITION_TIME*2}s")

        async for raw in ws:
            msg = json.loads(raw)
            channel = msg.get("channel", "")

            if channel.startswith("order_book"):
                apply_order_book_update(msg)
                now = time.time()
                if now - last_obi_check > 0.5:
                    raw_obi = calc_obi()
                    avg_obi = update_obi_average(raw_obi)
                    last_obi_check = now
                    
                    check_signal_and_execute(avg_obi)
                    
                    if now - last_status_log >= 30:
                        last_status_log = now
                        log_status(raw_obi, avg_obi)

            elif channel.startswith("trade"):
                trades = msg.get("trades", [])
                if trades:
                    last_trade_price = float(trades[-1]["price"])
                    last_trade_received_at = time.time()
                    
                    # 🆕 Preis-Historie für Volatilität
                    price_history.append(last_trade_price)
                    
                    if open_position is not None:
                        check_position_exit(last_trade_price, last_trade_received_at)

# ========== MAIN ==========
async def main():
    print("=" * 70)
    print(f"⚡ OBI MARKET-TRADER mit dynamischem TP/SL")
    print(f"   📊 OBI Schwelle: {OBI_THRESHOLD}")
    print(f"   🎯 TP: automatisch ({BASE_TP_PERCENT}% Basis)")
    print(f"   🛑 SL: automatisch ({BASE_SL_PERCENT}% Basis)")
    print(f"   ⏰ Max-Halt: {MAX_POSITION_TIME*2}s")
    print(f"   💰 Hebel: {LEVERAGE}x | Balance: ${ACCOUNT_BALANCE}")
    print("=" * 70)

    while True:
        try:
            await listen()
        except Exception as e:
            debug_log("⚠️ Verbindung verloren, reconnect in 3s", {"error": str(e)})
            await asyncio.sleep(3)

if __name__ == "__main__":
    asyncio.run(main())
