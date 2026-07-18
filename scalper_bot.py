"""
Schneller Scalping-Bot für Lighter (zkLighter)
==================================================
Im Gegensatz zum Market-Making-Bot: sofortige Ausführung per MARKET-Order
(keine Wartezeit auf Fill), dafür zahlst du den (bei Lighter sehr kleinen)
Spread bei Entry und Exit. Da keine Trading-Gebühren anfallen, ist der reale
Kostenfaktor pro Trade = Spread, der laut unseren Live-Beobachtungen bei
BTC meist bei 0,0002-0,01% liegt - deutlich kleiner als typische TP/SL-Ziele.

WICHTIGER UNTERSCHIED ZU FRÜHEREN VERSUCHEN:
- Symmetrisches Take-Profit UND Stop-Loss (der EMA-Backtest hatte nur TP,
  keinen SL - das hat die Verlust-Trades unbegrenzt laufen lassen)
- OBI-Signal ueber gleitenden Durchschnitt bestaetigt (kein Tick-Geflacker)
- Nur EINE Position gleichzeitig, kein Pyramiding

WICHTIG: Erst DRY_RUN=true testen, mehrere Stunden/Tage beobachten,
bevor du auf DRY_RUN=false stellst.
"""

import asyncio
import websockets
import json
import time
import os
import traceback
from collections import deque
from datetime import datetime

BASE_URL = "https://mainnet.zklighter.elliot.ai"
WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"

DEBUG_MODE = os.getenv("DEBUG_MODE", "true").lower() == "true"


def debug_log(msg, data=None):
    if DEBUG_MODE:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        print(f"[DEBUG {timestamp}] {msg}", flush=True)
        if data:
            print(f"   DATA: {json.dumps(data, indent=2, default=str)}", flush=True)


# ========== MARKET / COIN CONFIG ==========
MARKET_INDICES = {"ETH": 0, "BTC": 1, "SOL": 2, "DOGE": 3, "AVAX": 9, "SUI": 16}
PRECISION_MAP = {"BTC": 100000, "ETH": 10000, "SOL": 1000, "AVAX": 100, "SUI": 10}
PRICE_DECIMALS_MAP = {"BTC": 1, "ETH": 2, "SOL": 3, "AVAX": 3, "SUI": 5}
MIN_BASE_AMOUNT_MAP = {"BTC": 0.00020, "ETH": 0.005, "SOL": 0.05, "AVAX": 0.5, "SUI": 3.0}


def get_precision(symbol):
    return PRECISION_MAP.get(symbol, 10000)


def get_price_decimals(symbol):
    return PRICE_DECIMALS_MAP.get(symbol, 2)


def get_min_base_amount(symbol):
    return MIN_BASE_AMOUNT_MAP.get(symbol, 0.001)


# ========== KONFIGURATION ==========
SYMBOL = os.getenv("SCALP_SYMBOL", "BTC")
if SYMBOL not in MARKET_INDICES:
    raise ValueError(f"Symbol {SYMBOL} nicht in MARKET_INDICES - hier ergänzen")
MARKET_INDEX = MARKET_INDICES[SYMBOL]

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

MARGIN = float(os.getenv("SCALP_MARGIN", "10"))
LEVERAGE = int(os.getenv("SCALP_LEVERAGE", "20"))

OBI_LEVELS = int(os.getenv("OBI_LEVELS", "15"))
OBI_THRESHOLD = float(os.getenv("OBI_THRESHOLD", "0.30"))
OBI_AVG_WINDOW_SECONDS = float(os.getenv("OBI_AVG_WINDOW_SECONDS", "5"))

TP_PCT = float(os.getenv("SCALP_TP_PCT", "0.15"))   # Take-Profit-Ziel in %
SL_PCT = float(os.getenv("SCALP_SL_PCT", "0.15"))   # Stop-Loss in % (symmetrisch zu TP als Start)

COOLDOWN_SECONDS = float(os.getenv("COOLDOWN_SECONDS", "5"))
MAX_HOLD_SECONDS = float(os.getenv("SCALP_MAX_HOLD_SECONDS", "300"))  # Notausstieg falls weder TP noch SL nach X Sek.


# ========== LIGHTER CLIENT ==========
def get_lighter_client():
    try:
        import lighter
        API_KEY_INDEX = int(os.getenv("API_KEY_INDEX", "5"))
        PRIVATE_KEY = os.getenv("PRIVATE_KEY")
        ACCOUNT_INDEX = int(os.getenv("ACCOUNT_INDEX", "50960"))
        return lighter.SignerClient(
            url=BASE_URL,
            api_private_keys={API_KEY_INDEX: PRIVATE_KEY},
            account_index=ACCOUNT_INDEX
        )
    except Exception as e:
        debug_log("Lighter Client Fehler", {"error": str(e), "traceback": traceback.format_exc()})
        return None


async def place_market_order(client, is_ask, base_amount, reference_price):
    """Echte Market-Order. reference_price nur fuer den 'worst acceptable price'-Sicherheitsrahmen."""
    price_decimals = get_price_decimals(SYMBOL)
    adjusted_price = reference_price * 0.98 if is_ask else reference_price * 1.02  # 2% Sicherheitsrahmen, nicht der echte Fill-Preis
    price_scaled = int(adjusted_price * (10 ** price_decimals))

    tx, tx_hash, err = await client.create_order(
        market_index=MARKET_INDEX,
        client_order_index=int(time.time() * 1000),
        base_amount=base_amount,
        price=price_scaled,
        is_ask=is_ask,
        order_type=client.ORDER_TYPE_MARKET,
        time_in_force=client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
        reduce_only=False,
    )
    return tx, tx_hash, err


# ========== STATE ==========
order_book = {"bids": {}, "asks": {}}
obi_avg_buffer = deque()

open_position = None   # {"side": "buy"/"sell", "entry_price": x, "opened_at": ts}
last_signal_direction = None
last_trade_time = 0.0

stats = {"trades": 0, "wins": 0, "losses": 0, "total_pnl_pct": 0.0}
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


async def maybe_enter(client, avg_obi, current_price):
    global open_position, last_signal_direction, last_trade_time

    if open_position is not None or current_price is None:
        return

    now = time.time()
    if now - last_trade_time < COOLDOWN_SECONDS:
        return

    if avg_obi >= OBI_THRESHOLD:
        direction = "buy"
    elif avg_obi <= -OBI_THRESHOLD:
        direction = "sell"
    else:
        last_signal_direction = None
        return

    if direction == last_signal_direction:
        return

    last_signal_direction = direction
    last_trade_time = now

    if not DRY_RUN:
        position_usdc = MARGIN * LEVERAGE
        coin_amount = position_usdc / current_price
        precision = get_precision(SYMBOL)
        base_amount = int(coin_amount * precision)
        min_base = get_min_base_amount(SYMBOL)
        if base_amount * (1 / precision) < min_base:
            debug_log("⚠️ Order-Größe unter Mindestgröße - MARGIN/LEVERAGE erhöhen")
            return
        is_ask = direction == "sell"
        tx, tx_hash, err = await place_market_order(client, is_ask, base_amount, current_price)
        if err:
            debug_log("⚠️ Entry-Order fehlgeschlagen", {"error": str(err)})
            return
        debug_log(f"✅ ECHTE Order ausgeführt: {direction.upper()} @ ~{current_price}", {"tx_hash": str(tx_hash)})

    open_position = {"side": direction, "entry_price": current_price, "opened_at": now}
    debug_log(f"📈 Position eröffnet: {direction.upper()} {SYMBOL} @ {current_price} (Ø-OBI {round(avg_obi,3)})")


async def manage_open_position(client, current_price):
    global open_position

    if open_position is None or current_price is None:
        return

    side = open_position["side"]
    entry = open_position["entry_price"]
    now = time.time()

    if side == "buy":
        pnl_pct = (current_price - entry) / entry * 100
    else:
        pnl_pct = (entry - current_price) / entry * 100

    hit_tp = pnl_pct >= TP_PCT
    hit_sl = pnl_pct <= -SL_PCT
    timeout = (now - open_position["opened_at"]) >= MAX_HOLD_SECONDS

    if not (hit_tp or hit_sl or timeout):
        return

    reason = "TP" if hit_tp else ("SL" if hit_sl else "TIMEOUT")

    if not DRY_RUN:
        position_usdc = MARGIN * LEVERAGE
        coin_amount = position_usdc / entry
        precision = get_precision(SYMBOL)
        base_amount = int(coin_amount * precision)
        is_ask = side == "buy"  # Gegenrichtung zum Schliessen
        tx, tx_hash, err = await place_market_order(client, is_ask, base_amount, current_price)
        if err:
            debug_log("⚠️ Exit-Order fehlgeschlagen - Position bleibt (Vorsicht!)", {"error": str(err)})
            return
        debug_log(f"✅ ECHTE Exit-Order ausgeführt ({reason})", {"tx_hash": str(tx_hash)})

    stats["trades"] += 1
    stats["total_pnl_pct"] += pnl_pct
    if pnl_pct > 0:
        stats["wins"] += 1
    else:
        stats["losses"] += 1

    trade_log.append({
        "side": side, "entry": entry, "exit": current_price,
        "pnl_pct": round(pnl_pct, 4), "reason": reason, "closed_at": datetime.now().isoformat(),
    })

    debug_log(f"🏁 Position geschlossen ({reason}): {side.upper()} {entry} -> {current_price} | PnL {round(pnl_pct,4)}%")

    open_position = None


async def listen():
    last_trade_price = None
    last_status_log = 0.0

    client = get_lighter_client() if not DRY_RUN else None

    async with websockets.connect(WS_URL, ping_interval=20) as ws:
        await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{MARKET_INDEX}"}))
        await ws.send(json.dumps({"type": "subscribe", "channel": f"trade/{MARKET_INDEX}"}))
        debug_log(f"✅ Verbunden für {SYMBOL} (Market Index {MARKET_INDEX}) | DRY_RUN={DRY_RUN}")

        async for raw in ws:
            msg = json.loads(raw)
            channel = msg.get("channel", "")

            if channel.startswith("order_book"):
                apply_order_book_update(msg)
                raw_obi = calc_obi()
                avg_obi = update_obi_average(raw_obi)

                if open_position is None:
                    await maybe_enter(client, avg_obi, last_trade_price)
                else:
                    await manage_open_position(client, last_trade_price)

                now = time.time()
                if now - last_status_log >= 30:
                    last_status_log = now
                    win_rate = round(stats["wins"] / stats["trades"] * 100, 1) if stats["trades"] else 0
                    debug_log("📊 Scalper Status", {
                        "obi_avg": round(avg_obi, 3),
                        "letzter_preis": last_trade_price,
                        "offene_position": open_position,
                        "trades_gesamt": stats["trades"],
                        "trefferquote_pct": win_rate,
                        "gesamt_pnl_pct": round(stats["total_pnl_pct"], 4),
                    })

            elif channel.startswith("trade"):
                trades = msg.get("trades", [])
                if trades:
                    last_trade_price = float(trades[-1]["price"])


async def main():
    print("=" * 60)
    print(f"🚀 Scalping Bot für {SYMBOL}")
    print(f"   DRY_RUN: {DRY_RUN} | Margin: {MARGIN} USDC | Hebel: {LEVERAGE}x")
    print(f"   TP: {TP_PCT}% | SL: {SL_PCT}% | Max Haltedauer: {MAX_HOLD_SECONDS}s")
    print(f"   OBI Schwelle: {OBI_THRESHOLD} | Ø-Fenster: {OBI_AVG_WINDOW_SECONDS}s | Cooldown: {COOLDOWN_SECONDS}s")
    if not DRY_RUN:
        print("   ⚠️  LIVE-MODUS - platziert echte Market-Orders!")
    print("=" * 60)

    while True:
        try:
            await listen()
        except Exception as e:
            debug_log("⚠️ Verbindung verloren, reconnect in 5s", {"error": str(e), "traceback": traceback.format_exc()})
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
