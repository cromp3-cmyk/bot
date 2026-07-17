"""
OBI Scalping Bot - 50x Hebel, $10 Einsatz, $1 Gewinn
=====================================================
Ziel: 100+ Trades/Tag mit kleinen Gewinnen
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

# 📊 SCALPING PARAMETER
OBI_LEVELS = int(os.getenv("OBI_LEVELS", "25"))
OBI_THRESHOLD = float(os.getenv("OBI_THRESHOLD", "0.25"))  # Niedriger für viele Signale
OBI_AVG_WINDOW_SECONDS = float(os.getenv("OBI_AVG_WINDOW_SECONDS", "2"))  # Sehr kurz
COOLDOWN_SECONDS = float(os.getenv("COOLDOWN_SECONDS", "1"))  # 1 Sekunde zwischen Trades

# 🎯 SCALPING TP/SL (0.2% = $1 bei $500 Position)
TP_PERCENT = float(os.getenv("TP_PERCENT", "0.20"))  # 0.2% = $1 Gewinn
SL_PERCENT = float(os.getenv("SL_PERCENT", "0.10"))  # 0.1% = $0.50 Verlust
MAX_POSITION_TIME = float(os.getenv("MAX_POSITION_TIME", "3"))  # Max 3 Sekunden halten

# 💰 HEBEL & POSITION
ACCOUNT_BALANCE = float(os.getenv("ACCOUNT_BALANCE", "1000"))  # $1000 Kapital
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "10"))  # $10 Einsatz pro Trade
LEVERAGE = float(os.getenv("LEVERAGE", "50"))  # 50x Hebel

# ========== STATE ==========
order_book = {"bids": {}, "asks": {}}
obi_avg_buffer = deque()

last_signal_direction = None
last_signal_time = 0.0
last_trade_time = 0.0
start_time = time.time()

open_position = None
position_opened_at = 0.0

stats = {
    "signals": 0,
    "trades_completed": 0,
    "wins": 0,
    "losses": 0,
    "total_pnl_usd": 0.0,
    "avg_hold_time": 0.0,
    "total_hold_time": 0.0,
    "max_win": 0.0,
    "max_loss": 0.0,
    "current_balance": ACCOUNT_BALANCE,
}
trade_log = []

# ========== ORDERBOOK ==========
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

# ========== POSITION SIZING ==========
def calculate_position_size(entry_price):
    """$10 Einsatz mit 50x Hebel = $500 Position"""
    position_usd = RISK_PER_TRADE * LEVERAGE  # $10 * 50 = $500
    units = position_usd / entry_price if entry_price > 0 else 0
    
    return {
        "usd_size": round(position_usd, 2),
        "units": round(units, 6),
        "leverage": LEVERAGE,
        "margin_used": RISK_PER_TRADE,  # $10 Einsatz
        "target_profit": round(position_usd * (TP_PERCENT / 100), 2),  # $1
        "target_loss": round(position_usd * (SL_PERCENT / 100), 2),   # $0.50
    }

# ========== TRADING ==========
def check_signal_and_execute(avg_obi):
    global open_position, position_opened_at, last_signal_time, last_signal_direction, last_trade_time
    
    now = time.time()
    
    # Min 0.5s zwischen Trades (für Scalping)
    if now - last_trade_time < 0.5:
        return
    
    # Signal erkennen (niedrigere Schwelle für viele Trades)
    if avg_obi >= OBI_THRESHOLD:
        direction = "buy"
    elif avg_obi <= -OBI_THRESHOLD:
        direction = "sell"
    else:
        last_signal_direction = None
        return
    
    # Cooldown (1s)
    if now - last_signal_time < COOLDOWN_SECONDS:
        return
    if direction == last_signal_direction:
        return
    if open_position is not None:
        return
    
    bb, ba = best_bid(), best_ask()
    if bb is None or ba is None:
        return
    
    # Entry zum besten Preis
    entry_price = ba if direction == "buy" else bb
    
    # Positionsgröße berechnen
    position_info = calculate_position_size(entry_price)
    spread_pct = calc_spread()
    
    # Position öffnen
    open_position = {
        "side": direction,
        "entry_price": entry_price,
        "entry_time": now,
        "entry_obi": avg_obi,
        "size_usd": position_info["usd_size"],
        "units": position_info["units"],
        "leverage": position_info["leverage"],
        "margin_used": position_info["margin_used"],
        "target_profit": position_info["target_profit"],
        "target_loss": position_info["target_loss"],
        "spread": spread_pct,
    }
    position_opened_at = now
    last_signal_time = now
    last_signal_direction = direction
    last_trade_time = now
    stats["signals"] += 1
    
    debug_log(
        f"⚡ ENTRY: {direction.upper()} @ {entry_price}\n"
        f"   💰 Position: ${position_info['usd_size']} ({position_info['units']} {SYMBOL})\n"
        f"   💵 Margin: ${position_info['margin_used']} | Hebel: {position_info['leverage']}x\n"
        f"   🎯 Ziel: +${position_info['target_profit']} | 🛑 Stop: -${position_info['target_loss']}\n"
        f"   📈 OBI: {round(avg_obi, 3)} | Spread: {round(spread_pct, 3)}%"
    )

def check_position_exit(last_trade_price, last_trade_received_at):
    global open_position, position_opened_at
    
    if open_position is None or last_trade_price is None:
        return
    
    entry_price = open_position["entry_price"]
    side = open_position["side"]
    now = time.time()
    hold_time = now - position_opened_at
    
    # Exit-Preis
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
    target_profit = open_position.get("target_profit", 1.0)
    target_loss = open_position.get("target_loss", 0.5)
    
    # 🎯 TAKE-PROFIT ($1 Gewinn)
    if pnl_usd >= target_profit:
        close_position(exit_price, pnl_pct, pnl_usd, f"TP (+${target_profit})")
        return
    
    # 🛑 STOP-LOSS ($0.50 Verlust)
    if pnl_usd <= -target_loss:
        close_position(exit_price, pnl_pct, pnl_usd, f"SL (-${target_loss})")
        return
    
    # ⏰ TIMEOUT - nach 3 Sekunden schließen (auch bei kleinen Gewinn/Verlust)
    if hold_time > MAX_POSITION_TIME:
        close_position(exit_price, pnl_pct, pnl_usd, f"TIMEOUT ({round(hold_time, 1)}s)")
        return

def close_position(price, pnl_pct, pnl_usd, reason):
    global open_position, position_opened_at, last_trade_time
    
    if open_position is None:
        return
    
    side = open_position["side"]
    entry_price = open_position["entry_price"]
    hold_time = time.time() - position_opened_at
    
    # Stats
    stats["trades_completed"] += 1
    stats["total_pnl_usd"] += pnl_usd
    stats["total_hold_time"] += hold_time
    stats["avg_hold_time"] = stats["total_hold_time"] / stats["trades_completed"]
    stats["current_balance"] += pnl_usd
    
    if pnl_usd > 0:
        stats["wins"] += 1
        if pnl_usd > stats["max_win"]:
            stats["max_win"] = pnl_usd
    else:
        stats["losses"] += 1
        if pnl_usd < stats["max_loss"]:
            stats["max_loss"] = pnl_usd
    
    # Trade log
    trade_entry = {
        "side": side,
        "entry": round(entry_price, 2),
        "exit": round(price, 2),
        "pnl_usd": round(pnl_usd, 2),
        "pnl_pct": round(pnl_pct, 2),
        "reason": reason,
        "hold_time": round(hold_time, 2),
        "entry_obi": round(open_position.get("entry_obi", 0), 3),
        "leverage": open_position.get("leverage", 0),
        "size_usd": round(open_position.get("size_usd", 0), 2),
        "margin": round(open_position.get("margin_used", 0), 2),
        "closed_at": datetime.now().isoformat()
    }
    trade_log.append(trade_entry)
    
    # Log
    emoji = "✅" if pnl_usd > 0 else "❌"
    debug_log(
        f"{emoji} EXIT: {side.upper()} @ {price} | "
        f"PnL: ${round(pnl_usd, 2)} ({round(pnl_pct, 2)}%) | "
        f"Dauer: {round(hold_time, 2)}s | "
        f"Grund: {reason} | "
        f"Balance: ${round(stats['current_balance'], 2)}"
    )
    
    open_position = None
    position_opened_at = 0.0
    last_trade_time = time.time()

def log_status(raw_obi, avg_obi):
    win_rate = stats["wins"] / stats["trades_completed"] * 100 if stats["trades_completed"] else 0
    trades_per_hour = stats["trades_completed"] / ((time.time() - start_time) / 3600) if start_time else 0
    
    debug_log("📊 SCALPING STATUS", {
        "trades": stats["trades_completed"],
        "win_rate": round(win_rate, 1),
        "total_pnl": round(stats["total_pnl_usd"], 2),
        "balance": round(stats["current_balance"], 2),
        "trades/h": round(trades_per_hour, 1),
        "avg_hold": round(stats["avg_hold_time"], 2),
        "max_win": round(stats["max_win"], 2),
        "max_loss": round(stats["max_loss"], 2),
        "open": open_position is not None,
        "obi": round(avg_obi, 3),
        "last": trade_log[-1] if trade_log else None
    })

# ========== WEBSOCKET ==========
async def listen():
    last_trade_price = None
    last_trade_received_at = 0.0
    last_status_log = 0.0
    last_obi_check = 0.0
    
    async with websockets.connect(WS_URL, ping_interval=20) as ws:
        await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{MARKET_INDEX}"}))
        await ws.send(json.dumps({"type": "subscribe", "channel": f"trade/{MARKET_INDEX}"}))
        
        debug_log(f"✅ Verbunden | Scalping Mode")
        debug_log(f"💵 $10 Einsatz | 50x Hebel | $500 Position | Ziel: +$1 | Stop: -$0.50")

        async for raw in ws:
            msg = json.loads(raw)
            channel = msg.get("channel", "")

            if channel.startswith("order_book"):
                apply_order_book_update(msg)
                now = time.time()
                if now - last_obi_check > 0.3:  # 3x pro Sekunde für schnelle Reaktion
                    raw_obi = calc_obi()
                    avg_obi = update_obi_average(raw_obi)
                    last_obi_check = now
                    check_signal_and_execute(avg_obi)
                    if now - last_status_log >= 10:  # Status alle 10 Sekunden
                        last_status_log = now
                        log_status(raw_obi, avg_obi)

            elif channel.startswith("trade"):
                trades = msg.get("trades", [])
                if trades:
                    last_trade_price = float(trades[-1]["price"])
                    last_trade_received_at = time.time()
                    if open_position is not None:
                        check_position_exit(last_trade_price, last_trade_received_at)

# ========== MAIN ==========
async def main():
    print("=" * 70)
    print(f"⚡ OBI SCALPING BOT - 50x Hebel")
    print(f"   💵 Einsatz: ${RISK_PER_TRADE} | Hebel: {LEVERAGE}x")
    print(f"   📊 Position: ${RISK_PER_TRADE * LEVERAGE}")
    print(f"   🎯 Ziel: +${RISK_PER_TRADE * LEVERAGE * TP_PERCENT / 100:.2f} pro Trade")
    print(f"   🛑 Stop: -${RISK_PER_TRADE * LEVERAGE * SL_PERCENT / 100:.2f} pro Trade")
    print(f"   ⚡ Ziel: 100+ Trades/Tag = ${RISK_PER_TRADE * LEVERAGE * TP_PERCENT / 100 * 100:.0f}/Tag")
    print("=" * 70)
    print("   ✅ Keine Gebühren auf Lighter!")
    print("   ✅ 50x Hebel = kleine Einsätze, große Wirkung")
    print("   ✅ 3 Sekunden pro Trade = viele Trades")
    print("=" * 70)

    while True:
        try:
            await listen()
        except Exception as e:
            debug_log("⚠️ Reconnect...", {"error": str(e)})
            await asyncio.sleep(3)

if __name__ == "__main__":
    asyncio.run(main())
