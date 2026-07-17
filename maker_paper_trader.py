"""
MOMENTUM SCALPING BOT - MIT DEBUG
==================================
Zeigt alle Preisänderungen an damit wir sehen warum keine Trades kommen
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

# 📊 MOMENTUM PARAMETER
MOMENTUM_THRESHOLD = float(os.getenv("MOMENTUM_THRESHOLD", "0.005"))  # SEHR NIEDRIG FÜR TESTS!
SIGNAL_MODE = os.getenv("SIGNAL_MODE", "simple")

# 🎯 TP/SL
TP_PERCENT = float(os.getenv("TP_PERCENT", "0.02"))
SL_PERCENT = float(os.getenv("SL_PERCENT", "0.01"))
MAX_POSITION_TIME = float(os.getenv("MAX_POSITION_TIME", "5"))

# 💰 HEBEL & POSITION
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "10"))
LEVERAGE = float(os.getenv("LEVERAGE", "50"))
ACCOUNT_BALANCE = float(os.getenv("ACCOUNT_BALANCE", "1000"))

# ========== STATE ==========
order_book = {"bids": {}, "asks": {}}
price_history = deque(maxlen=20)

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

def mid_price():
    bb, ba = best_bid(), best_ask()
    if bb is None or ba is None:
        return None
    return (bb + ba) / 2

def calc_spread():
    bb, ba = best_bid(), best_ask()
    if bb is None or ba is None or bb == 0:
        return 0.0
    return (ba - bb) / bb * 100

def calc_obi(levels=25):
    bids_sorted = sorted(order_book["bids"].items(), key=lambda x: float(x[0]), reverse=True)[:levels]
    asks_sorted = sorted(order_book["asks"].items(), key=lambda x: float(x[0]))[:levels]
    bid_vol = sum(v for _, v in bids_sorted)
    ask_vol = sum(v for _, v in asks_sorted)
    total = bid_vol + ask_vol
    return 0.0 if total == 0 else (bid_vol - ask_vol) / total

# ========== MOMENTUM SIGNAL MIT DEBUG ==========
def check_momentum_simple():
    """Simple Momentum: 1 Sekunde Preisänderung - MIT DEBUG"""
    if len(price_history) < 3:
        debug_log(f"⏳ Preis-Historie: {len(price_history)}/3 - warte auf mehr Daten...")
        return None
    
    price_now = price_history[-1]
    price_1s_ago = price_history[-2]
    
    if price_1s_ago == 0:
        return None
    
    change_pct = (price_now - price_1s_ago) / price_1s_ago * 100
    
    # 🔥 DEBUG: Zeige jede Preisänderung
    if abs(change_pct) > 0.001:  # Nur wenn mehr als 0.001%
        debug_log(f"📊 Preis: {price_now:.2f} | Änderung: {change_pct:.3f}% | Threshold: {MOMENTUM_THRESHOLD}%")
    
    if change_pct > MOMENTUM_THRESHOLD:
        debug_log(f"🚀 BUY SIGNAL! {change_pct:.3f}% > {MOMENTUM_THRESHOLD}%")
        return "buy"
    elif change_pct < -MOMENTUM_THRESHOLD:
        debug_log(f"🚀 SELL SIGNAL! {change_pct:.3f}% < -{MOMENTUM_THRESHOLD}%")
        return "sell"
    return None

def check_obi_fallback():
    """OBI als Fallback"""
    raw_obi = calc_obi()
    debug_log(f"📊 OBI: {raw_obi:.3f}")
    if raw_obi > 0.35:
        debug_log(f"🚀 OBI BUY SIGNAL! {raw_obi:.3f} > 0.35")
        return "buy"
    elif raw_obi < -0.35:
        debug_log(f"🚀 OBI SELL SIGNAL! {raw_obi:.3f} < -0.35")
        return "sell"
    return None

def get_momentum_signal():
    """Hauptsignal-Funktion"""
    
    # 1. Momentum Signal
    signal = check_momentum_simple()
    
    # 2. Fallback: OBI
    if not signal and SIGNAL_MODE == "obi_fallback":
        signal = check_obi_fallback()
    
    return signal

# ========== POSITION SIZING ==========
def calculate_position_size(entry_price):
    position_usd = RISK_PER_TRADE * LEVERAGE
    units = position_usd / entry_price if entry_price > 0 else 0
    return {
        "usd_size": round(position_usd, 2),
        "units": round(units, 6),
        "leverage": LEVERAGE,
        "margin_used": RISK_PER_TRADE,
        "target_profit": round(position_usd * (TP_PERCENT / 100), 2),
        "target_loss": round(position_usd * (SL_PERCENT / 100), 2),
    }

# ========== TRADING ==========
def check_signal_and_execute():
    global open_position, position_opened_at, last_trade_time
    
    now = time.time()
    
    # Mindestabstand zwischen Trades
    if now - last_trade_time < 0.5:
        return
    
    # Keine Position offen
    if open_position is not None:
        return
    
    # 🚀 SIGNAL PRÜFEN
    signal = get_momentum_signal()
    if not signal:
        return
    
    # 💥 ENTRY AUSFÜHREN
    bb, ba = best_bid(), best_ask()
    if bb is None or ba is None:
        debug_log("⚠️ Kein OrderBook für Entry")
        return
    
    entry_price = ba if signal == "buy" else bb
    position_info = calculate_position_size(entry_price)
    spread_pct = calc_spread()
    
    open_position = {
        "side": signal,
        "entry_price": entry_price,
        "entry_time": now,
        "size_usd": position_info["usd_size"],
        "units": position_info["units"],
        "leverage": position_info["leverage"],
        "margin_used": position_info["margin_used"],
        "target_profit": position_info["target_profit"],
        "target_loss": position_info["target_loss"],
        "spread": spread_pct,
    }
    position_opened_at = now
    last_trade_time = now
    stats["signals"] += 1
    
    debug_log(
        f"⚡ ENTRY: {signal.upper()} @ {entry_price}\n"
        f"   💰 Position: ${position_info['usd_size']} ({position_info['units']} {SYMBOL})\n"
        f"   💵 Margin: ${position_info['margin_used']} | Hebel: {position_info['leverage']}x\n"
        f"   🎯 Ziel: +${position_info['target_profit']} | 🛑 Stop: -${position_info['target_loss']}"
    )

def check_position_exit(last_trade_price):
    global open_position, position_opened_at
    
    if open_position is None or last_trade_price is None:
        return
    
    entry_price = open_position["entry_price"]
    side = open_position["side"]
    now = time.time()
    hold_time = now - position_opened_at
    
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
    target_profit = open_position.get("target_profit", 0.10)
    target_loss = open_position.get("target_loss", 0.05)
    
    if pnl_usd >= target_profit:
        close_position(exit_price, pnl_pct, pnl_usd, f"TP (+${target_profit:.2f})")
        return
    
    if pnl_usd <= -target_loss:
        close_position(exit_price, pnl_pct, pnl_usd, f"SL (-${target_loss:.2f})")
        return
    
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
    
    trade_entry = {
        "side": side,
        "entry": round(entry_price, 2),
        "exit": round(price, 2),
        "pnl_usd": round(pnl_usd, 2),
        "pnl_pct": round(pnl_pct, 2),
        "reason": reason,
        "hold_time": round(hold_time, 2),
        "leverage": open_position.get("leverage", 0),
        "size_usd": round(open_position.get("size_usd", 0), 2),
        "closed_at": datetime.now().isoformat()
    }
    trade_log.append(trade_entry)
    
    emoji = "✅" if pnl_usd > 0 else "❌"
    debug_log(f"{emoji} EXIT: {side.upper()} @ {price} | PnL: ${round(pnl_usd, 2)} | Dauer: {round(hold_time, 2)}s | Grund: {reason}")
    
    open_position = None
    position_opened_at = 0.0
    last_trade_time = time.time()

def log_status():
    win_rate = stats["wins"] / stats["trades_completed"] * 100 if stats["trades_completed"] else 0
    trades_per_hour = stats["trades_completed"] / ((time.time() - start_time) / 3600) if start_time else 0
    
    debug_log("📊 STATUS", {
        "mode": SIGNAL_MODE,
        "trades": stats["trades_completed"],
        "win_rate": round(win_rate, 1),
        "total_pnl": round(stats["total_pnl_usd"], 2),
        "balance": round(stats["current_balance"], 2),
        "trades/h": round(trades_per_hour, 1),
        "open": open_position is not None,
        "price_history": len(price_history),
        "last": trade_log[-1] if trade_log else None
    })

# ========== WEBSOCKET ==========
async def listen():
    last_trade_price = None
    last_status_log = 0.0
    last_price_log = 0.0
    
    async with websockets.connect(WS_URL, ping_interval=20) as ws:
        await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{MARKET_INDEX}"}))
        await ws.send(json.dumps({"type": "subscribe", "channel": f"trade/{MARKET_INDEX}"}))
        
        debug_log(f"✅ Verbunden | Mode: {SIGNAL_MODE} | Threshold: {MOMENTUM_THRESHOLD}%")

        async for raw in ws:
            msg = json.loads(raw)
            channel = msg.get("channel", "")

            if channel.startswith("order_book"):
                apply_order_book_update(msg)
                
                # 🔥 Preis aus OrderBook
                mid = mid_price()
                if mid:
                    price_history.append(mid)
                
                # Signal prüfen
                check_signal_and_execute()
                
                # Status alle 30s
                now = time.time()
                if now - last_status_log >= 30:
                    last_status_log = now
                    log_status()

            elif channel.startswith("trade"):
                trades = msg.get("trades", [])
                if trades:
                    price = float(trades[-1]["price"])
                    price_history.append(price)
                    last_trade_price = price
                    
                    if open_position is not None:
                        check_position_exit(last_trade_price)

# ========== MAIN ==========
async def main():
    print("=" * 70)
    print(f"⚡ MOMENTUM BOT - MIT DEBUG")
    print(f"   📊 Threshold: {MOMENTUM_THRESHOLD}%")
    print(f"   🎯 TP: {TP_PERCENT}% | SL: {SL_PERCENT}%")
    print(f"   💰 Hebel: {LEVERAGE}x | Einsatz: ${RISK_PER_TRADE}")
    print("=" * 70)
    print("   🔍 DEBUG: Zeigt alle Preisänderungen an")
    print("   📊 Siehst du Preisänderungen?")
    print("=" * 70)

    while True:
        try:
            await listen()
        except Exception as e:
            debug_log(f"⚠️ Reconnect... {e}")
            await asyncio.sleep(3)

if __name__ == "__main__":
    asyncio.run(main())
