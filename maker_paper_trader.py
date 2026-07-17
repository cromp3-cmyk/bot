"""
OBI Market-Trader für Lighter (zkLighter) mit Margin/Leverage
============================================================
Schnelle Market-Order Ausführung für OBI-basierte Signale.
Unterstützt Hebel von 1x bis 10x mit Risikomanagement.

STRATEGIE:
- OBI-Signal → Sofortiger Market-Entry
- Take-Profit bei 0.1-0.2% → Market-Exit
- Stop-Loss bei 0.1-0.15% → Market-Exit
- Max 5 Sekunden halten → Timeout-Exit
- Positionsgröße basierend auf Hebel und Risiko

FEATURES:
- 💰 Margin/Leverage Unterstützung (1x - 10x)
- 📊 Eingebautes Web-Dashboard
- 📈 Live-Statistiken
- 🔄 Auto-Refresh
- 💾 Trade-History
"""

import asyncio
import websockets
import json
import time
import os
import threading
from collections import deque
from datetime import datetime
from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS

# ========== WEBSOCKET KONFIGURATION ==========
WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
DEBUG_MODE = os.getenv("DEBUG_MODE", "true").lower() == "true"

def debug_log(msg, data=None):
    if DEBUG_MODE:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        print(f"[DEBUG {timestamp}] {msg}", flush=True)
        if data:
            print(f"   DATA: {json.dumps(data, indent=2, default=str)}", flush=True)

# ========== MARKET KONFIGURATION ==========
MARKET_INDICES = {"ETH": 0, "BTC": 1, "SOL": 2, "DOGE": 3, "AVAX": 9, "SUI": 16}
SYMBOL = os.getenv("OB_SYMBOL", "BTC")
MARKET_INDEX = MARKET_INDICES[SYMBOL]

# ========== OBI KONFIGURATION ==========
OBI_LEVELS = int(os.getenv("OBI_LEVELS", "25"))
OBI_THRESHOLD = float(os.getenv("OBI_THRESHOLD", "0.30"))
OBI_AVG_WINDOW_SECONDS = float(os.getenv("OBI_AVG_WINDOW_SECONDS", "3"))
COOLDOWN_SECONDS = float(os.getenv("COOLDOWN_SECONDS", "3"))

# ========== TRADING KONFIGURATION ==========
TP_PERCENT = float(os.getenv("TP_PERCENT", "0.10"))  # 0.1% Gewinn
SL_PERCENT = float(os.getenv("SL_PERCENT", "0.10"))  # 0.1% Verlust
MAX_POSITION_TIME = float(os.getenv("MAX_POSITION_TIME", "5"))  # Max 5 Sekunden halten
MIN_TRADE_INTERVAL = float(os.getenv("MIN_TRADE_INTERVAL", "3"))  # Mindestens 3s zwischen Trades

# ========== MARGIN KONFIGURATION ==========
ACCOUNT_BALANCE = float(os.getenv("ACCOUNT_BALANCE", "10000"))  # Kontostand in USD
LEVERAGE = float(os.getenv("LEVERAGE", "1.0"))  # Hebel (1x = kein Hebel)
POSITION_SIZE_PCT = float(os.getenv("POSITION_SIZE_PCT", "100"))  # % des Kapitals pro Trade
MAX_POSITION_SIZE = float(os.getenv("MAX_POSITION_SIZE", "10000"))  # Max USD pro Trade
MIN_POSITION_SIZE = float(os.getenv("MIN_POSITION_SIZE", "10"))  # Min USD pro Trade
MAX_RISK_PER_TRADE = float(os.getenv("MAX_RISK_PER_TRADE", "2"))  # Max 2% Verlust pro Trade

# ========== STATE ==========
order_book = {"bids": {}, "asks": {}}
obi_avg_buffer = deque()

last_signal_direction = None
last_signal_time = 0.0
last_trade_time = 0.0
start_time = time.time()

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
    "total_pnl_usd": 0.0,
    "avg_hold_time": 0.0,
    "total_hold_time": 0.0,
    "max_win": 0.0,
    "max_loss": 0.0,
    "current_balance": ACCOUNT_BALANCE,
}
trade_log = []

# ========== API STATE ==========
api_state = {
    "status": "Offline",
    "stats": {},
    "trades": [],
    "position": {"open": False},
    "obi": {"current": 0, "avg": 0},
    "margin": {
        "leverage": LEVERAGE,
        "balance": ACCOUNT_BALANCE,
        "margin_used": 0,
        "exposure": 0,
        "available_margin": ACCOUNT_BALANCE,
        "position_size": 0
    }
}

# ========== FLASK API ==========
app = Flask(__name__, static_folder='.')
CORS(app)

@app.route('/')
def index():
    """Dashboard HTML Seite"""
    return send_from_directory('.', 'dashboard.html')

@app.route('/api/trader-status')
def get_status():
    """API Endpoint für Live-Daten"""
    return jsonify(api_state)

def update_api_state():
    """Aktualisiert den API-State"""
    global api_state, open_position, position_opened_at
    
    # Stats
    win_rate = stats["wins"] / stats["trades_completed"] * 100 if stats["trades_completed"] else 0
    avg_pnl = stats["total_pnl_pct"] / stats["trades_completed"] if stats["trades_completed"] else 0
    
    api_state["status"] = "Online"
    api_state["stats"] = {
        "totalPnl": round(stats["total_pnl_pct"], 2),
        "totalPnlUsd": round(stats["total_pnl_usd"], 2),
        "trades": stats["trades_completed"],
        "wins": stats["wins"],
        "losses": stats["losses"],
        "avgPnl": round(avg_pnl, 4),
        "maxWin": round(stats["max_win"], 2),
        "maxLoss": round(stats["max_loss"], 2),
        "avgHoldTime": round(stats["avg_hold_time"], 2),
        "tradesPerHour": round(stats["trades_completed"] / ((time.time() - start_time) / 3600), 1) if start_time else 0,
        "winRate": round(win_rate, 1),
        "performance": round(stats["total_pnl_pct"], 2),
        "currentBalance": round(stats["current_balance"], 2)
    }
    
    # Trades (letzte 50)
    api_state["trades"] = []
    for trade in trade_log[-50:]:
        api_state["trades"].append({
            "side": trade.get("side"),
            "entry": trade.get("entry"),
            "exit": trade.get("exit"),
            "pnl_pct": trade.get("pnl_pct"),
            "pnl_usd": trade.get("pnl_usd"),
            "hold_time": trade.get("hold_time"),
            "reason": trade.get("reason"),
            "closed_at": trade.get("closed_at"),
            "leverage": trade.get("leverage", 1.0),
            "size_usd": trade.get("size_usd", 0)
        })
    
    # Position
    if open_position:
        bb, ba = best_bid(), best_ask()
        current = None
        if open_position["side"] == "buy":
            current = bb if bb else open_position["entry_price"]
        else:
            current = ba if ba else open_position["entry_price"]
        
        if current:
            if open_position["side"] == "buy":
                pnl_pct = (current - open_position["entry_price"]) / open_position["entry_price"] * 100
                pnl_usd = pnl_pct / 100 * open_position["size_usd"]
            else:
                pnl_pct = (open_position["entry_price"] - current) / open_position["entry_price"] * 100
                pnl_usd = pnl_pct / 100 * open_position["size_usd"]
            
            api_state["position"] = {
                "open": True,
                "side": open_position["side"],
                "entry": round(open_position["entry_price"], 2),
                "current": round(current, 2),
                "pnl": round(pnl_pct, 2),
                "pnlUsd": round(pnl_usd, 2),
                "holdTime": round(time.time() - position_opened_at, 1),
                "size_usd": round(open_position.get("size_usd", 0), 2),
                "units": round(open_position.get("units", 0), 6),
                "leverage": open_position.get("leverage", 1.0)
            }
        else:
            api_state["position"] = {"open": False}
    else:
        api_state["position"] = {"open": False}
    
    # Margin
    api_state["margin"] = {
        "leverage": LEVERAGE,
        "balance": round(stats["current_balance"], 2),
        "margin_used": round(open_position.get("margin_used", 0), 2) if open_position else 0,
        "exposure": round(open_position.get("size_usd", 0), 2) if open_position else 0,
        "available_margin": round(stats["current_balance"] - (open_position.get("margin_used", 0) if open_position else 0), 2),
        "position_size": round(open_position.get("units", 0), 6) if open_position else 0
    }

def start_api_server():
    """Startet den Flask API Server"""
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)

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
    now = time.time()
    obi_avg_buffer.append((raw_obi, now))
    cutoff = now - OBI_AVG_WINDOW_SECONDS
    while obi_avg_buffer and obi_avg_buffer[0][1] < cutoff:
        obi_avg_buffer.popleft()
    if not obi_avg_buffer:
        return 0.0
    return sum(v for v, _ in obi_avg_buffer) / len(obi_avg_buffer)

# ========== MARGIN & POSITION SIZING ==========
def calculate_position_size(entry_price, stop_loss_price=None):
    """
    Berechnet die Positionsgröße basierend auf Hebel und Risiko
    """
    # Basisposition: % des Kontos
    base_size = stats["current_balance"] * (POSITION_SIZE_PCT / 100)
    
    # Mit Hebel multiplizieren
    leveraged_size = base_size * LEVERAGE
    
    # Auf Max begrenzen
    final_size = min(leveraged_size, MAX_POSITION_SIZE)
    final_size = max(final_size, MIN_POSITION_SIZE)
    
    # Risikobasiertes Sizing (wenn SL angegeben)
    if stop_loss_price and entry_price:
        risk_per_unit = abs(entry_price - stop_loss_price)
        max_loss_usd = stats["current_balance"] * (MAX_RISK_PER_TRADE / 100)
        if risk_per_unit > 0:
            risk_based_size = max_loss_usd / (risk_per_unit / entry_price)
            final_size = min(final_size, risk_based_size)
    
    # In Einheiten umrechnen
    units = final_size / entry_price if entry_price > 0 else 0
    
    return {
        "usd_size": round(final_size, 2),
        "units": round(units, 6),
        "leverage": LEVERAGE,
        "margin_used": round(final_size / LEVERAGE if LEVERAGE > 0 else final_size, 2),
        "max_loss_usd": round(final_size * (MAX_RISK_PER_TRADE / 100), 2) if stop_loss_price else None
    }

# ========== TRADING LOGIK ==========
def check_signal_and_execute(avg_obi):
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
    
    # Cooldown prüfen
    if now - last_signal_time < COOLDOWN_SECONDS:
        return
    
    # Keine doppelten Signale
    if direction == last_signal_direction:
        return
    
    # Nur handeln wenn keine Position offen
    if open_position is not None:
        return
    
    # 💥 MARKT-PREIS FÜR ENTRY
    bb, ba = best_bid(), best_ask()
    if bb is None or ba is None:
        debug_log("⚠️ Kein OrderBook für Entry verfügbar")
        return
    
    if direction == "buy":
        entry_price = ba
    else:
        entry_price = bb
    
    # 💰 POSITIONSGRÖSSE BERECHNEN
    position_info = calculate_position_size(entry_price)
    
    # Prüfen ob genug Margin vorhanden
    if position_info["margin_used"] > stats["current_balance"]:
        debug_log(f"⚠️ Nicht genug Margin! Benötigt: ${position_info['margin_used']}, Verfügbar: ${stats['current_balance']}")
        return
    
    spread_pct = (ba - bb) / bb * 100 if bb > 0 else 0
    
    # 💥 POSITION ERÖFFNEN
    open_position = {
        "side": direction,
        "entry_price": entry_price,
        "entry_time": now,
        "size_usd": position_info["usd_size"],
        "units": position_info["units"],
        "leverage": position_info["leverage"],
        "margin_used": position_info["margin_used"],
        "entry_obi": avg_obi,
        "entry_bb": bb,
        "entry_ba": ba
    }
    position_opened_at = now
    last_signal_time = now
    last_signal_direction = direction
    last_trade_time = now
    
    stats["signals"] += 1
    
    debug_log(
        f"💥 MARKET-ENTRY: {direction.upper()} @ {entry_price}\n"
        f"   📊 Size: ${position_info['usd_size']} ({position_info['units']} {SYMBOL})\n"
        f"   🔧 Hebel: {position_info['leverage']}x | Margin: ${position_info['margin_used']}\n"
        f"   📈 OBI: {round(avg_obi, 3)} | Spread: {round(spread_pct, 2)}%"
    )

def check_position_exit(last_trade_price, last_trade_received_at):
    global open_position, position_opened_at
    
    if open_position is None or last_trade_price is None:
        return
    
    entry_price = open_position["entry_price"]
    side = open_position["side"]
    now = time.time()
    
    # Marktpreis für Exit
    bb, ba = best_bid(), best_ask()
    if side == "buy":
        exit_price = bb if bb else last_trade_price
        pnl_pct = (exit_price - entry_price) / entry_price * 100
    else:
        exit_price = ba if ba else last_trade_price
        pnl_pct = (entry_price - exit_price) / entry_price * 100
    
    # Wenn kein Market-Preis verfügbar, Trade-Preis verwenden
    if exit_price is None:
        exit_price = last_trade_price
        if side == "buy":
            pnl_pct = (exit_price - entry_price) / entry_price * 100
        else:
            pnl_pct = (entry_price - exit_price) / entry_price * 100
    
    # 🎯 TAKE-PROFIT
    if pnl_pct >= TP_PERCENT:
        close_position(exit_price, pnl_pct, "TP")
        return
    
    # 🛑 STOP-LOSS
    if pnl_pct <= -SL_PERCENT:
        close_position(exit_price, pnl_pct, "SL")
        return
    
    # ⏰ TIMEOUT
    hold_time = now - position_opened_at
    if hold_time > MAX_POSITION_TIME:
        close_position(exit_price, pnl_pct, f"TIMEOUT ({round(hold_time, 1)}s)")
        return
    
    # 📉 Frühzeitiger Exit bei Verlust
    if pnl_pct < -0.05 and hold_time > 3:
        close_position(exit_price, pnl_pct, "EARLY_SL")

def close_position(price, pnl_pct, reason):
    global open_position, position_opened_at, last_trade_time
    
    if open_position is None:
        return
    
    side = open_position["side"]
    entry_price = open_position["entry_price"]
    hold_time = time.time() - position_opened_at
    
    # 💰 PnL in USD berechnen
    pnl_usd = pnl_pct / 100 * open_position["size_usd"]
    
    # 💥 STATISTIK AKTUALISIEREN
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
    
    # 📋 TRADE LOG
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
        "margin_used": round(open_position.get("margin_used", 0), 2),
        "closed_at": datetime.now().isoformat()
    }
    trade_log.append(trade_entry)
    
    # 📊 LOG
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
    win_rate = round(stats["wins"] / stats["trades_completed"] * 100, 1) if stats["trades_completed"] else 0
    avg_pnl = round(stats["total_pnl_pct"] / stats["trades_completed"], 4) if stats["trades_completed"] else 0
    
    debug_log("📊 OBI Market-Trader Status", {
        "obi": round(raw_obi, 3),
        "obi_avg": round(avg_obi, 3),
        "signale": stats["signals"],
        "trades": stats["trades_completed"],
        "win_rate": win_rate,
        "total_pnl": round(stats["total_pnl_pct"], 2),
        "total_pnl_usd": round(stats["total_pnl_usd"], 2),
        "balance": round(stats["current_balance"], 2),
        "leverage": LEVERAGE,
        "avg_hold": round(stats["avg_hold_time"], 2),
        "open_position": open_position is not None
    })

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
        debug_log(f"⚡ Strategie: Market-Execution | TP: {TP_PERCENT}% | SL: {SL_PERCENT}%")
        debug_log(f"💰 Margin: {LEVERAGE}x Hebel | Balance: ${ACCOUNT_BALANCE} | Size: {POSITION_SIZE_PCT}%")

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
                    
                    # OBI für API
                    api_state["obi"]["current"] = round(raw_obi, 3)
                    api_state["obi"]["avg"] = round(avg_obi, 3)
                    
                    if now - last_status_log >= 30:
                        last_status_log = now
                        log_status(raw_obi, avg_obi)
                        update_api_state()

            elif channel.startswith("trade"):
                trades = msg.get("trades", [])
                if trades:
                    last_trade_price = float(trades[-1]["price"])
                    last_trade_received_at = time.time()
                    
                    if open_position is not None:
                        check_position_exit(last_trade_price, last_trade_received_at)
                        update_api_state()

# ========== MAIN ==========
async def main():
    print("=" * 70)
    print(f"⚡ OBI MARKET-TRADER für {SYMBOL}")
    print(f"   📊 OBI Schwelle: {OBI_THRESHOLD} | Ø-Fenster: {OBI_AVG_WINDOW_SECONDS}s")
    print(f"   🎯 TP: {TP_PERCENT}% | 🛑 SL: {SL_PERCENT}% | ⏰ Max-Halt: {MAX_POSITION_TIME}s")
    print(f"   💰 Hebel: {LEVERAGE}x | Balance: ${ACCOUNT_BALANCE} | Size: {POSITION_SIZE_PCT}%")
    print(f"   🚀 Ausführung: SOFORTIGE MARKET-ORDERS")
    print(f"   🌐 Dashboard: http://localhost:5000")
    print("=" * 70)

    # API Server in eigenem Thread starten
    api_thread = threading.Thread(target=start_api_server, daemon=True)
    api_thread.start()
    debug_log("🌐 API Server gestartet auf Port 5000")

    while True:
        try:
            await listen()
        except Exception as e:
            debug_log("⚠️ Verbindung verloren, reconnect in 3s", {"error": str(e)})
            await asyncio.sleep(3)

if __name__ == "__main__":
    asyncio.run(main())
