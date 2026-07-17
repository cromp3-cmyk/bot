"""
Maker-Strategie Paper-Trader für Lighter (zkLighter)
=======================================================
Simuliert eine OBI-basierte Strategie, die statt Market-Orders (die den
Spread ZAHLEN) passive Post-Only Limit-Orders am besten Bid/Ask nutzt (die
den Spread VERDIENEN, wenn sie gefuellt werden).

WICHTIG: Dies ist reines PAPER-TRADING / LOGGING - es werden KEINE echten
Orders platziert. Ziel ist, ehrlich zu messen:
1. Wie oft wuerde eine passive Order ueberhaupt gefuellt (Fill-Rate)?
2. Wie sieht das PnL aus, WENN sie gefuellt wird (Entry + Exit beide als Maker)?
3. Vergleich zur reinen Market-Order-Variante (die wir vorher gebaut haben)

Erst wenn diese Zahlen ueber mehrere Tage plausibel/profitabel aussehen,
macht es Sinn, auf echte Post-Only-Orders umzustellen (ORDER_TYPE_LIMIT +
ORDER_TIME_IN_FORCE_POST_ONLY, beide im installierten SDK bestaetigt).

FUNKTIONSWEISE DER SIMULATION:
- Bei bestaetigtem OBI-Signal wird eine "gedankliche" Order am aktuellen
  besten Bid (bei Buy-Signal) bzw. besten Ask (bei Sell-Signal) platziert -
  das ist die Preis-Ebene, die eine echte Post-Only-Order dort einnehmen wuerde.
- Die Order gilt als "gefuellt", sobald ein echter Trade zum Order-Preis
  oder guenstiger fuer die Gegenseite auftaucht (d.h. jemand hat unsere
  Order tatsaechlich "getroffen").
- Nach Fill wird auf dieselbe Weise eine Exit-Order (TP) auf der
  Gegenseite simuliert.
- Timeout: wenn eine Order nach X Sekunden nicht gefuellt ist, gilt sie
  als "verpasst" (in der Realitaet wuerde man sie canceln/nachziehen).
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


# ========== Konfiguration ==========
MARKET_INDICES = {"ETH": 0, "BTC": 1, "SOL": 2, "DOGE": 3, "AVAX": 9, "SUI": 16}
SYMBOL = os.getenv("OB_SYMBOL", "BTC")
MARKET_INDEX = MARKET_INDICES[SYMBOL]

OBI_LEVELS = int(os.getenv("OBI_LEVELS", "25"))
OBI_THRESHOLD = float(os.getenv("OBI_THRESHOLD", "0.30"))
COOLDOWN_SECONDS = float(os.getenv("COOLDOWN_SECONDS", "10"))

TP_PERCENT = float(os.getenv("TP_PERCENT", "0.1"))  # Ziel-Gewinn als Maker, in %
ORDER_TIMEOUT_SECONDS = float(os.getenv("ORDER_TIMEOUT_SECONDS", "20"))  # wie lange auf Fill warten

# ========== NEUE EXIT-KONFIGURATION ==========
EXIT_PRICE_SLIPPAGE = float(os.getenv("EXIT_PRICE_SLIPPAGE", "0.001"))  # 0.1% pro Retry
MAX_EXIT_RETRIES = int(os.getenv("MAX_EXIT_RETRIES", "5"))  # Nach 5 Versuchen Market-Order
EXIT_RETRY_INTERVAL = float(os.getenv("EXIT_RETRY_INTERVAL", "10"))  # Sekunden zwischen Retries

# ========== State ==========
order_book = {"bids": {}, "asks": {}}
obi_avg_buffer = deque()  # [(value, timestamp), ...] fuer den gleitenden Durchschnitt

OBI_AVG_WINDOW_SECONDS = float(os.getenv("OBI_AVG_WINDOW_SECONDS", "10"))

last_signal_direction = None
last_signal_time = 0.0

# Simulierte offene "Order" (Entry, wartet auf Fill)
pending_entry = None   # {"side": "buy"/"sell", "price": x, "placed_at": ts}
open_position = None   # {"side": ..., "entry_price": ..., "filled_at": ...}
pending_exit = None    # {"side": ..., "price": ..., "placed_at": ..., "retry_count": ...}

# ========== NEUER STATE FÜR EXIT-RETRIES ==========
exit_retry_count = 0
last_exit_attempt_time = 0.0

# Statistik
stats = {
    "signals": 0,
    "entries_filled": 0,
    "entries_timeout": 0,
    "exits_filled": 0,
    "exits_timeout": 0,
    "completed_trades": 0,
    "total_pnl_pct": 0.0,
    "wins": 0,
    "losses": 0,
    "emergency_exits": 0,  # 🆕 Zähler für Notfall-Market-Exits
}
trade_log = []


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


def calc_obi(levels=OBI_LEVELS):
    bids_sorted = sorted(order_book["bids"].items(), key=lambda x: float(x[0]), reverse=True)[:levels]
    asks_sorted = sorted(order_book["asks"].items(), key=lambda x: float(x[0]))[:levels]
    bid_vol = sum(v for _, v in bids_sorted)
    ask_vol = sum(v for _, v in asks_sorted)
    total = bid_vol + ask_vol
    return 0.0 if total == 0 else (bid_vol - ask_vol) / total


def update_obi_average(raw_obi):
    """Gleitender Durchschnitt über OBI_AVG_WINDOW_SECONDS - glättet kurze Ausreißer."""
    now = time.time()
    obi_avg_buffer.append((raw_obi, now))
    cutoff = now - OBI_AVG_WINDOW_SECONDS
    while obi_avg_buffer and obi_avg_buffer[0][1] < cutoff:
        obi_avg_buffer.popleft()
    if not obi_avg_buffer:
        return 0.0
    return sum(v for v, _ in obi_avg_buffer) / len(obi_avg_buffer)


def check_signal_and_place_entry(avg_obi):
    """Prueft den geglätteten OBI-Durchschnitt und 'platziert' bei Bedarf eine simulierte Entry-Order."""
    global last_signal_direction, last_signal_time, pending_entry

    now = time.time()

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

    if pending_entry is not None or open_position is not None:
        return

    bb, ba = best_bid(), best_ask()
    if bb is None or ba is None:
        return

    price = bb if direction == "buy" else ba
    pending_entry = {"side": direction, "price": price, "placed_at": now}
    last_signal_time = now
    last_signal_direction = direction
    stats["signals"] += 1

    debug_log(f"📝 Simulierte Entry-Order platziert: {direction.upper()} {SYMBOL} @ {price} (Ø-OBI {round(avg_obi,3)}, Maker, wartet auf Fill)")


def check_entry_fill(last_trade_price, last_trade_received_at):
    """Simuliert Fill: ein Trade NACH Order-Platzierung zum Order-Preis (oder besser) fuellt uns."""
    global pending_entry, open_position

    if pending_entry is None or last_trade_price is None:
        return

    if last_trade_received_at <= pending_entry["placed_at"]:
        return

    side = pending_entry["side"]
    price = pending_entry["price"]

    filled = (side == "buy" and last_trade_price <= price) or (side == "sell" and last_trade_price >= price)

    if filled:
        open_position = {"side": side, "entry_price": price, "filled_at": time.time()}
        stats["entries_filled"] += 1
        debug_log(f"✅ Entry GEFÜLLT (Maker): {side.upper()} @ {price}")
        pending_entry = None
        return

    if time.time() - pending_entry["placed_at"] > ORDER_TIMEOUT_SECONDS:
        stats["entries_timeout"] += 1
        debug_log(f"⏱️ Entry-Order Timeout, nie gefüllt: {side.upper()} @ {price}")
        pending_entry = None


# ========== VERBESSERTE EXIT-LOGIK ==========
def calculate_exit_price_with_sliding(entry_price, side, current_market_price, retry_count):
    """
    Berechnet den Exit-Preis mit Preis-Sliding bei jedem Retry.
    Je mehr Retries, desto näher am Marktpreis.
    """
    if side == "buy":
        # BUY-Position: Verkauf über Einstiegspreis
        base_target = entry_price * (1 + TP_PERCENT / 100)
        
        if retry_count > 0:
            # Bei jedem Retry näher an den Marktpreis heranrücken
            if current_market_price < base_target:
                # Der Markt ist unter unserem Ziel - wir müssen runtergehen
                # Maximal 30% der Differenz pro Schritt, aber nicht mehr als Slippage * Retry
                max_adjustment = base_target * EXIT_PRICE_SLIPPAGE * retry_count
                ideal_adjustment = (base_target - current_market_price) * 0.3
                adjustment = min(max_adjustment, ideal_adjustment)
                target = base_target - adjustment
            else:
                target = base_target
        else:
            target = base_target
        
        # Sicherheitscheck: Nicht unter den Einstiegspreis (außer bei Notfall)
        if retry_count < MAX_EXIT_RETRIES:
            min_price = entry_price * 0.998  # Max 0.2% Verlust
            if target < min_price:
                target = min_price
        else:
            # Letzter Versuch: Näher am Markt
            target = current_market_price * 0.999  # 0.1% unter Markt
        
        return target, "sell"
    
    else:
        # SELL-Position: Kauf unter Einstiegspreis
        base_target = entry_price * (1 - TP_PERCENT / 100)
        
        if retry_count > 0:
            if current_market_price > base_target:
                max_adjustment = base_target * EXIT_PRICE_SLIPPAGE * retry_count
                ideal_adjustment = (current_market_price - base_target) * 0.3
                adjustment = min(max_adjustment, ideal_adjustment)
                target = base_target + adjustment
            else:
                target = base_target
        else:
            target = base_target
        
        # Sicherheitscheck: Nicht über den Einstiegspreis
        if retry_count < MAX_EXIT_RETRIES:
            max_price = entry_price * 1.002  # Max 0.2% Verlust
            if target > max_price:
                target = max_price
        else:
            target = current_market_price * 1.001  # 0.1% über Markt
        
        return target, "buy"


def maybe_place_exit():
    """Platziert Exit-Order mit Preis-Sliding bei Wiederholung"""
    global open_position, pending_exit, exit_retry_count, last_exit_attempt_time

    if open_position is None:
        return
    
    # Prüfen ob wir schon eine Exit-Order haben
    if pending_exit is not None:
        return
    
    # Prüfen ob genug Zeit seit letztem Versuch vergangen ist
    now = time.time()
    if now - last_exit_attempt_time < EXIT_RETRY_INTERVAL and exit_retry_count > 0:
        return

    entry_price = open_position["entry_price"]
    side = open_position["side"]
    
    # Hole aktuellen Marktpreis
    bb, ba = best_bid(), best_ask()
    if bb is None or ba is None:
        debug_log("⚠️ Kein OrderBook verfügbar für Exit")
        return
    
    current_market_price = ba if side == "buy" else bb
    
    # Berechne Exit-Preis mit Sliding
    target_price, exit_side = calculate_exit_price_with_sliding(
        entry_price, side, current_market_price, exit_retry_count
    )
    
    # Log mit Retry-Info
    retry_info = f" (Retry {exit_retry_count+1}/{MAX_EXIT_RETRIES})" if exit_retry_count > 0 else ""
    debug_log(
        f"📝 Simulierte Exit-Order platziert: {exit_side.upper()} {SYMBOL} @ {round(target_price, 4)} "
        f"(TP {TP_PERCENT}%, Markt: {current_market_price}){retry_info}"
    )
    
    pending_exit = {
        "side": exit_side,
        "price": target_price,
        "placed_at": now,
        "retry_count": exit_retry_count,
        "market_price": current_market_price
    }
    
    last_exit_attempt_time = now


def force_market_exit():
    """Notfall: Schließt Position mit simulierter Market-Order"""
    global open_position, pending_exit, exit_retry_count
    
    if open_position is None:
        return
    
    bb, ba = best_bid(), best_ask()
    if bb is None or ba is None:
        debug_log("❌ Kein Preis für Market-Exit verfügbar")
        return
    
    entry_price = open_position["entry_price"]
    side = open_position["side"]
    
    # Market-Preis ist der beste Bid (für Verkauf) oder Ask (für Kauf)
    if side == "buy":
        # Wir verkaufen zum besten Bid
        exit_price = bb
        pnl_pct = (exit_price - entry_price) / entry_price * 100
    else:
        # Wir kaufen zum besten Ask
        exit_price = ba
        pnl_pct = (entry_price - exit_price) / entry_price * 100
    
    stats["emergency_exits"] += 1
    stats["exits_filled"] += 1
    stats["completed_trades"] += 1
    stats["total_pnl_pct"] += pnl_pct
    
    if pnl_pct > 0:
        stats["wins"] += 1
    else:
        stats["losses"] += 1
    
    trade_log.append({
        "side": side,
        "entry": entry_price,
        "exit": exit_price,
        "pnl_pct": round(pnl_pct, 4),
        "closed_at": datetime.now().isoformat(),
        "exit_type": "EMERGENCY_MARKET",
        "retries": exit_retry_count
    })
    
    debug_log(f"🚨 NOTFALL-MARKET-EXIT: {side.upper()} @ {exit_price} | PnL: {round(pnl_pct, 4)}%")
    
    open_position = None
    pending_exit = None
    exit_retry_count = 0


def check_exit_fill(last_trade_price, last_trade_received_at):
    """Prüft ob Exit-Order gefüllt wurde oder Timeout"""
    global pending_exit, open_position, exit_retry_count

    if pending_exit is None or last_trade_price is None:
        return

    if last_trade_received_at <= pending_exit["placed_at"]:
        return

    side = pending_exit["side"]
    price = pending_exit["price"]
    retry_count = pending_exit.get("retry_count", 0)

    filled = (side == "buy" and last_trade_price <= price) or (side == "sell" and last_trade_price >= price)

    if filled:
        entry_price = open_position["entry_price"]
        entry_side = open_position["side"]
        if entry_side == "buy":
            pnl_pct = (price - entry_price) / entry_price * 100
        else:
            pnl_pct = (entry_price - price) / entry_price * 100

        stats["exits_filled"] += 1
        stats["completed_trades"] += 1
        stats["total_pnl_pct"] += pnl_pct
        if pnl_pct > 0:
            stats["wins"] += 1
        else:
            stats["losses"] += 1

        trade_log.append({
            "side": entry_side,
            "entry": entry_price,
            "exit": price,
            "pnl_pct": round(pnl_pct, 4),
            "closed_at": datetime.now().isoformat(),
            "exit_type": "MAKER_LIMIT",
            "retries": retry_count
        })

        debug_log(f"✅ Exit GEFÜLLT (Maker): {side.upper()} @ {price} | Trade-PnL: {round(pnl_pct, 4)}% (Retries: {retry_count})")

        open_position = None
        pending_exit = None
        exit_retry_count = 0
        return

    # Timeout-Check
    if time.time() - pending_exit["placed_at"] > ORDER_TIMEOUT_SECONDS:
        current_retry = retry_count + 1
        
        debug_log(f"⏱️ Exit-Order Timeout (Retry {current_retry}/{MAX_EXIT_RETRIES})")
        
        if current_retry >= MAX_EXIT_RETRIES:
            # Max Retries erreicht -> Notfall-Market-Exit
            debug_log(f"🚨 MAX_RETRIES erreicht ({MAX_EXIT_RETRIES}) - Führe NOTFALL-MARKET-EXIT aus!")
            force_market_exit()
        else:
            # Order löschen und im nächsten Tick neu platzieren (mit Sliding)
            exit_retry_count = current_retry
            pending_exit = None
            debug_log(f"🔄 Warte {EXIT_RETRY_INTERVAL}s vor nächstem Versuch mit angepasstem Preis")


def check_market_movement():
    """Prüft ob sich der Markt extrem gegen uns bewegt hat"""
    if open_position is None:
        return
    
    bb, ba = best_bid(), best_ask()
    if bb is None or ba is None:
        return
    
    entry_price = open_position["entry_price"]
    side = open_position["side"]
    current_price = ba if side == "buy" else bb
    
    # Berechne unrealisierten Verlust
    if side == "buy":
        loss_pct = (entry_price - current_price) / entry_price
    else:
        loss_pct = (current_price - entry_price) / entry_price
    
    # Bei >2% Verlust: Notfall-Exit
    if loss_pct > 0.02:
        debug_log(f"🚨 Starke Gegenbewegung ({loss_pct*100:.2f}%) - Notfall-Market-Exit!")
        force_market_exit()
        return True
    
    # Bei >1% Verlust und vielen Retries: beschleunigen
    if loss_pct > 0.01 and exit_retry_count > 2:
        debug_log(f"⚠️ Markt bewegt sich gegen uns ({loss_pct*100:.2f}%) - verkürze Timeout")
        # Force die nächste Exit-Prüfung
        if pending_exit:
            pending_exit["placed_at"] = time.time() - ORDER_TIMEOUT_SECONDS + 5
    
    return False


async def listen():
    last_trade_price = None
    last_trade_received_at = 0.0
    last_status_log = 0.0

    async with websockets.connect(WS_URL, ping_interval=20) as ws:
        await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{MARKET_INDEX}"}))
        await ws.send(json.dumps({"type": "subscribe", "channel": f"trade/{MARKET_INDEX}"}))
        debug_log(f"✅ Verbunden für {SYMBOL} (Market Index {MARKET_INDEX})")

        async for raw in ws:
            msg = json.loads(raw)
            channel = msg.get("channel", "")

            if channel.startswith("order_book"):
                apply_order_book_update(msg)
                raw_obi = calc_obi()
                avg_obi = update_obi_average(raw_obi)

                check_signal_and_place_entry(avg_obi)
                check_entry_fill(last_trade_price, last_trade_received_at)

                if open_position is not None:
                    # Prüfe auf extreme Marktbewegungen
                    check_market_movement()
                    
                    # Versuche Exit zu platzieren
                    maybe_place_exit()
                    check_exit_fill(last_trade_price, last_trade_received_at)

                now = time.time()
                if now - last_status_log >= 30:
                    last_status_log = now
                    win_rate = round(stats["wins"] / stats["completed_trades"] * 100, 1) if stats["completed_trades"] else 0
                    fill_rate = round(stats["entries_filled"] / stats["signals"] * 100, 1) if stats["signals"] else 0
                    
                    status_data = {
                        "obi_roh": round(raw_obi, 3),
                        "obi_avg": round(avg_obi, 3),
                        "signale_gesamt": stats["signals"],
                        "entry_fill_rate_pct": fill_rate,
                        "abgeschlossene_trades": stats["completed_trades"],
                        "trefferquote_pct": win_rate,
                        "gesamt_pnl_pct": round(stats["total_pnl_pct"], 4),
                        "exit_retry_count": exit_retry_count,
                        "emergency_exits": stats["emergency_exits"],
                        "offene_position": open_position,
                        "wartende_entry_order": pending_entry,
                        "wartende_exit_order": pending_exit,
                    }
                    
                    # Extra Infos bei offener Position
                    if open_position and pending_exit:
                        time_in_position = time.time() - open_position["filled_at"]
                        status_data["position_dauer_sec"] = round(time_in_position, 1)
                        if pending_exit:
                            status_data["exit_retry"] = pending_exit.get("retry_count", 0)
                    
                    debug_log("📊 Maker-Strategie Status", status_data)

            elif channel.startswith("trade"):
                trades = msg.get("trades", [])
                if trades:
                    last_trade_price = float(trades[-1]["price"])
                    last_trade_received_at = time.time()


async def main():
    print("=" * 60)
    print(f"🚀 Maker-Strategie Paper-Trader für {SYMBOL}")
    print(f"   OBI Schwelle: {OBI_THRESHOLD} | Ø-Fenster: {OBI_AVG_WINDOW_SECONDS}s | Cooldown: {COOLDOWN_SECONDS}s")
    print(f"   TP: {TP_PERCENT}% | Order-Timeout: {ORDER_TIMEOUT_SECONDS}s")
    print(f"   🆕 Exit-Sliding: {EXIT_PRICE_SLIPPAGE*100}% pro Retry | Max Retries: {MAX_EXIT_RETRIES}")
    print(f"   🆕 Exit-Retry-Intervall: {EXIT_RETRY_INTERVAL}s")
    print("   NUR SIMULATION - keine echten Orders")
    print("=" * 60)

    while True:
        try:
            await listen()
        except Exception as e:
            debug_log("⚠️ Verbindung verloren, reconnect in 5s", {"error": str(e)})
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
