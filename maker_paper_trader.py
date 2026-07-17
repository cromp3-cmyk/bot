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

# ========== State ==========
order_book = {"bids": {}, "asks": {}}
obi_avg_buffer = deque()  # [(value, timestamp), ...] fuer den gleitenden Durchschnitt

OBI_AVG_WINDOW_SECONDS = float(os.getenv("OBI_AVG_WINDOW_SECONDS", "10"))

last_signal_direction = None
last_signal_time = 0.0

# Simulierte offene "Order" (Entry, wartet auf Fill)
pending_entry = None   # {"side": "buy"/"sell", "price": x, "placed_at": ts}
open_position = None   # {"side": ..., "entry_price": ..., "filled_at": ...}
pending_exit = None    # {"side": ..., "price": ..., "placed_at": ts}

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
        last_signal_direction = None  # zurueck im neutralen Bereich - naechstes Signal darf wieder feuern
        return

    if now - last_signal_time < COOLDOWN_SECONDS:
        return

    if direction == last_signal_direction:
        return  # gleiche Richtung wie zuletzt, kein erneutes Signal noetig

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


def check_entry_fill(last_trade_price, last_trade_ts):
    """Simuliert Fill: ein Trade zum Order-Preis (oder besser fuer den Taker) fuellt uns."""
    global pending_entry, open_position

    if pending_entry is None or last_trade_price is None:
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


def maybe_place_exit():
    global open_position, pending_exit

    if open_position is None or pending_exit is not None:
        return

    entry_price = open_position["entry_price"]
    side = open_position["side"]

    if side == "buy":
        target = entry_price * (1 + TP_PERCENT / 100)
        exit_side = "sell"
    else:
        target = entry_price * (1 - TP_PERCENT / 100)
        exit_side = "buy"

    pending_exit = {"side": exit_side, "price": target, "placed_at": time.time()}
    debug_log(f"📝 Simulierte Exit-Order platziert: {exit_side.upper()} {SYMBOL} @ {round(target, 4)} (TP {TP_PERCENT}%)")


def check_exit_fill(last_trade_price):
    global pending_exit, open_position

    if pending_exit is None or last_trade_price is None:
        return

    side = pending_exit["side"]
    price = pending_exit["price"]

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
            "side": entry_side, "entry": entry_price, "exit": price,
            "pnl_pct": round(pnl_pct, 4), "closed_at": datetime.now().isoformat(),
        })

        debug_log(f"✅ Exit GEFÜLLT (Maker): {side.upper()} @ {price} | Trade-PnL: {round(pnl_pct, 4)}% (beide Seiten Maker = Spread verdient statt gezahlt)")

        open_position = None
        pending_exit = None
        return

    if time.time() - pending_exit["placed_at"] > ORDER_TIMEOUT_SECONDS:
        stats["exits_timeout"] += 1
        debug_log("⏱️ Exit-Order Timeout - Position bleibt offen, versuche es beim naechsten Tick erneut")
        pending_exit = None  # naechster Tick versucht erneut mit aktuellem Preis


async def listen():
    last_trade_price = None
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
                check_entry_fill(last_trade_price, time.time())

                if open_position is not None:
                    maybe_place_exit()
                    check_exit_fill(last_trade_price)

                now = time.time()
                if now - last_status_log >= 30:
                    last_status_log = now
                    win_rate = round(stats["wins"] / stats["completed_trades"] * 100, 1) if stats["completed_trades"] else 0
                    fill_rate = round(stats["entries_filled"] / stats["signals"] * 100, 1) if stats["signals"] else 0
                    debug_log("📊 Maker-Strategie Status", {
                        "obi_roh": round(raw_obi, 3),
                        "obi_avg": round(avg_obi, 3),
                        "signale_gesamt": stats["signals"],
                        "entry_fill_rate_pct": fill_rate,
                        "abgeschlossene_trades": stats["completed_trades"],
                        "trefferquote_pct": win_rate,
                        "gesamt_pnl_pct": round(stats["total_pnl_pct"], 4),
                        "offene_position": open_position,
                        "wartende_entry_order": pending_entry,
                        "wartende_exit_order": pending_exit,
                    })

            elif channel.startswith("trade"):
                trades = msg.get("trades", [])
                if trades:
                    last_trade_price = float(trades[-1]["price"])


async def main():
    print("=" * 60)
    print(f"🚀 Maker-Strategie Paper-Trader für {SYMBOL}")
    print(f"   OBI Schwelle: {OBI_THRESHOLD} | Ø-Fenster: {OBI_AVG_WINDOW_SECONDS}s | Cooldown: {COOLDOWN_SECONDS}s")
    print(f"   TP: {TP_PERCENT}% | Order-Timeout: {ORDER_TIMEOUT_SECONDS}s")
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
