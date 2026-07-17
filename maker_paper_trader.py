"""
Market-Making Bot für Lighter (zkLighter)
============================================
Statt eine Richtung vorherzusagen (wie EMA-Crossover oder reines OBI-Signal),
quotet dieser Bot GLEICHZEITIG auf beiden Seiten (Bid + Ask) knapp um den
Mittelpreis herum - der Gewinn kommt aus dem Spread, nicht aus einer
Richtungswette. Das ist das Prinzip echter Market-Maker.

WICHTIG - LIES DAS BEVOR DU DRY_RUN=false SETZT:
- Inventory-Risiko: Bei einem starken Trend (nicht Hin-und-Her) wird nur eine
  Seite laufend gefuellt - der Bot baut dann einseitigen Bestand auf, der an
  Wert verliert. MAX_POSITION_USDC begrenzt das, verhindert es aber nicht
  komplett.
- Fill-Erkennung ist eine ANNAEHERUNG (basiert auf oeffentlichem Trade-Tape,
  nicht auf einer autoritativen Order-Bestaetigung). Nutze
  sync_real_position() regelmaessig, um gegen den echten Kontostand
  abzugleichen.
- Bei DRY_RUN=false werden ECHTE Post-Only Limit-Orders auf Lighter platziert
  und bei Bedarf storniert/neu gesetzt (Requoting).

Start erst mit DRY_RUN=true, mehrere Stunden beobachten, danach klein
(kleine MARGIN) live testen.
"""

import asyncio
import websockets
import json
import time
import os
import traceback
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


# ========== KONFIGURATION (alles per Env-Variable) ==========
SYMBOL = os.getenv("MM_SYMBOL", "BTC")
if SYMBOL not in MARKET_INDICES:
    raise ValueError(f"Symbol {SYMBOL} nicht in MARKET_INDICES - hier ergänzen")
MARKET_INDEX = MARKET_INDICES[SYMBOL]

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

MARGIN = float(os.getenv("MM_MARGIN", "10"))         # z.B. 10 USDC pro Order-Seite
LEVERAGE = int(os.getenv("MM_LEVERAGE", "20"))        # z.B. 20x

SPREAD_PCT = float(os.getenv("MM_SPREAD_PCT", "0.05"))       # Abstand jeder Quote vom Mid, in %
REQUOTE_SECONDS = float(os.getenv("MM_REQUOTE_SECONDS", "5"))  # wie oft Quotes ueberpruefen/nachziehen
REQUOTE_MOVE_THRESHOLD_PCT = float(os.getenv("MM_REQUOTE_MOVE_PCT", "0.03"))  # ab wie viel Mid-Bewegung requoten

MAX_POSITION_USDC = float(os.getenv("MM_MAX_POSITION_USDC", "50"))  # max. Netto-Bestand, danach nur noch abbauende Seite

OBI_SKEW_ENABLED = os.getenv("MM_OBI_SKEW_ENABLED", "true").lower() == "true"
OBI_LEVELS = int(os.getenv("MM_OBI_LEVELS", "15"))
OBI_SKEW_FACTOR = float(os.getenv("MM_OBI_SKEW_FACTOR", "0.02"))  # wie stark OBI die Quotes verschiebt

ACCOUNT_SYNC_SECONDS = float(os.getenv("MM_ACCOUNT_SYNC_SECONDS", "120"))


# ========== LIGHTER CLIENT (nur fuer echte Orders, DRY_RUN=false) ==========
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


async def place_post_only_limit(client, is_ask, price, base_amount):
    """Platziert eine echte Post-Only Limit-Order (nur Maker, wird sonst storniert)."""
    price_decimals = get_price_decimals(SYMBOL)
    price_scaled = int(price * (10 ** price_decimals))

    tx, tx_hash, err = await client.create_order(
        market_index=MARKET_INDEX,
        client_order_index=int(time.time() * 1000),
        base_amount=base_amount,
        price=price_scaled,
        is_ask=is_ask,
        order_type=client.ORDER_TYPE_LIMIT,
        time_in_force=client.ORDER_TIME_IN_FORCE_POST_ONLY,
        reduce_only=False,
    )
    return tx, tx_hash, err


async def cancel_all_real_orders(client):
    try:
        await client.cancel_all_orders(market_index=MARKET_INDEX)
    except Exception as e:
        debug_log("⚠️ cancel_all_orders fehlgeschlagen", {"error": str(e)})


async def sync_real_position():
    """Fragt den ECHTEN Kontostand ab, um die eigene (approximierte) Inventory-Buchhaltung zu pruefen."""
    if DRY_RUN:
        return
    try:
        import lighter
        account_index = int(os.getenv("ACCOUNT_INDEX", "50960"))
        configuration = lighter.Configuration(host=BASE_URL)
        async with lighter.ApiClient(configuration) as api_client:
            account_api = lighter.AccountApi(api_client)
            response = await account_api.account(by="index", value=str(account_index))
            accounts = getattr(response, "accounts", None) or []
            if not accounts:
                return
            positions = getattr(accounts[0], "positions", []) or []
            for pos in positions:
                if getattr(pos, "market_index", None) == MARKET_INDEX:
                    real_size = float(getattr(pos, "position", 0) or 0)
                    debug_log("🔎 Echter Positionsabgleich (Lighter-API)", {
                        "echte_position_size": real_size,
                        "unsere_buchhaltung_inventory": STATE["inventory"],
                        "differenz": round(real_size - STATE["inventory"], 6),
                    })
                    return
            debug_log("🔎 Echter Positionsabgleich: keine offene Position auf Lighter gefunden", {
                "unsere_buchhaltung_inventory": STATE["inventory"],
            })
    except Exception as e:
        debug_log("⚠️ Positions-Abgleich fehlgeschlagen", {"error": str(e)})


# ========== STATE ==========
order_book = {"bids": {}, "asks": {}}
STATE = {
    "inventory": 0.0,        # netto Coin-Bestand (positiv = long, negativ = short)
    "avg_entry_price": 0.0,
    "realized_pnl_usdc": 0.0,
    "fills": 0,
    "resting_bid": None,     # {"price":, "placed_at":}
    "resting_ask": None,
    "last_mid": None,
}
last_trade_price = None
last_trade_received_at = 0.0


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


def best_bid_ask():
    if not order_book["bids"] or not order_book["asks"]:
        return None, None
    return max(float(p) for p in order_book["bids"].keys()), min(float(p) for p in order_book["asks"].keys())


def calc_obi(levels=OBI_LEVELS):
    bids_sorted = sorted(order_book["bids"].items(), key=lambda x: float(x[0]), reverse=True)[:levels]
    asks_sorted = sorted(order_book["asks"].items(), key=lambda x: float(x[0]))[:levels]
    bid_vol = sum(v for _, v in bids_sorted)
    ask_vol = sum(v for _, v in asks_sorted)
    total = bid_vol + ask_vol
    return 0.0 if total == 0 else (bid_vol - ask_vol) / total


def compute_target_quotes(mid, obi):
    """Berechnet Ziel-Bid/Ask-Preise inkl. optionalem OBI-Skew und Inventory-Limit."""
    skew = (obi * OBI_SKEW_FACTOR * mid) if OBI_SKEW_ENABLED else 0.0

    bid_price = mid * (1 - SPREAD_PCT / 100) + skew
    ask_price = mid * (1 + SPREAD_PCT / 100) + skew

    position_usdc = STATE["inventory"] * mid
    quote_bid = True
    quote_ask = True

    if position_usdc >= MAX_POSITION_USDC:
        quote_bid = False   # schon zu viel long -> nicht weiter long aufbauen
    elif position_usdc <= -MAX_POSITION_USDC:
        quote_ask = False   # schon zu viel short -> nicht weiter short aufbauen

    return (bid_price if quote_bid else None), (ask_price if quote_ask else None)


def record_fill(side, price, base_amount):
    """side: 'buy' oder 'sell' - aktualisiert Inventory + realisiertes PnL bei Reduktion."""
    old_inventory = STATE["inventory"]

    if side == "buy":
        if old_inventory < 0:
            # Short-Bestand wird reduziert -> realisiertes PnL
            closed_amount = min(base_amount, -old_inventory)
            pnl = (STATE["avg_entry_price"] - price) * closed_amount
            STATE["realized_pnl_usdc"] += pnl
        new_inventory = old_inventory + base_amount
    else:
        if old_inventory > 0:
            closed_amount = min(base_amount, old_inventory)
            pnl = (price - STATE["avg_entry_price"]) * closed_amount
            STATE["realized_pnl_usdc"] += pnl
        new_inventory = old_inventory - base_amount

    # Durchschnittspreis nur neu berechnen, wenn Position in dieselbe Richtung waechst
    if (old_inventory >= 0 and side == "buy") or (old_inventory <= 0 and side == "sell"):
        total_value = STATE["avg_entry_price"] * abs(old_inventory) + price * base_amount
        STATE["avg_entry_price"] = total_value / (abs(old_inventory) + base_amount) if (abs(old_inventory) + base_amount) > 0 else price
    elif new_inventory != 0 and (old_inventory >= 0) != (new_inventory >= 0):
        STATE["avg_entry_price"] = price  # Richtung gedreht

    STATE["inventory"] = new_inventory
    STATE["fills"] += 1

    debug_log(f"✅ FILL: {side.upper()} {base_amount} {SYMBOL} @ {price}", {
        "neues_inventory": round(new_inventory, 6),
        "avg_entry_price": round(STATE["avg_entry_price"], 4),
        "realized_pnl_usdc": round(STATE["realized_pnl_usdc"], 4),
    })


def order_size_base_amount():
    """Positionsgroesse pro Quote-Seite, in Coin-Einheiten (base_amount, skaliert)."""
    position_usdc = MARGIN * LEVERAGE
    return position_usdc  # in USDC-Wert; Umrechnung in Coin-Menge passiert beim Platzieren


async def manage_quotes(client, mid, obi):
    """Requoting-Logik: prueft ob Quotes aktualisiert werden muessen, setzt neue (paper oder echt)."""
    target_bid, target_ask = compute_target_quotes(mid, obi)
    now = time.time()

    need_requote = STATE["last_mid"] is None or abs(mid - STATE["last_mid"]) / mid * 100 >= REQUOTE_MOVE_THRESHOLD_PCT
    time_based = (STATE["resting_bid"] is None or now - STATE["resting_bid"]["placed_at"] >= REQUOTE_SECONDS) and \
                 (STATE["resting_ask"] is None or now - STATE["resting_ask"]["placed_at"] >= REQUOTE_SECONDS)

    if not (need_requote or time_based):
        return

    STATE["last_mid"] = mid

    if DRY_RUN:
        STATE["resting_bid"] = {"price": target_bid, "placed_at": now} if target_bid else None
        STATE["resting_ask"] = {"price": target_ask, "placed_at": now} if target_ask else None
        return

    # Echtes Requoting: alte Orders canceln, neue setzen
    await cancel_all_real_orders(client)

    position_usdc = MARGIN * LEVERAGE
    coin_amount = position_usdc / mid
    precision = get_precision(SYMBOL)
    base_amount = int(coin_amount * precision)
    min_base = get_min_base_amount(SYMBOL)

    if base_amount * (1 / precision) < min_base:
        debug_log("⚠️ Order-Größe unter Mindestgröße - MARGIN/LEVERAGE erhöhen")
        return

    if target_bid:
        tx, tx_hash, err = await place_post_only_limit(client, False, target_bid, base_amount)
        if err:
            debug_log("⚠️ Bid-Order fehlgeschlagen", {"error": str(err)})
        else:
            STATE["resting_bid"] = {"price": target_bid, "placed_at": now}
    else:
        STATE["resting_bid"] = None

    if target_ask:
        tx, tx_hash, err = await place_post_only_limit(client, True, target_ask, base_amount)
        if err:
            debug_log("⚠️ Ask-Order fehlgeschlagen", {"error": str(err)})
        else:
            STATE["resting_ask"] = {"price": target_ask, "placed_at": now}
    else:
        STATE["resting_ask"] = None

    debug_log("🔄 Requote", {"bid": target_bid, "ask": target_ask, "inventory": STATE["inventory"]})


def check_fills_from_trade_tape(trade_price, trade_received_at):
    """Approximation: ein Trade zum/durch unseren Quote-Preis NACH Platzierung = Fill."""
    order_size_usdc = MARGIN * LEVERAGE
    coin_size = order_size_usdc / trade_price if trade_price else 0

    rb = STATE["resting_bid"]
    if rb and trade_received_at > rb["placed_at"] and trade_price <= rb["price"]:
        record_fill("buy", rb["price"], coin_size)
        STATE["resting_bid"] = None

    ra = STATE["resting_ask"]
    if ra and trade_received_at > ra["placed_at"] and trade_price >= ra["price"]:
        record_fill("sell", ra["price"], coin_size)
        STATE["resting_ask"] = None


async def listen():
    global last_trade_price, last_trade_received_at

    last_status_log = 0.0
    last_account_sync = 0.0

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
                bb, ba = best_bid_ask()
                if bb is None or ba is None:
                    continue
                mid = (bb + ba) / 2
                obi = calc_obi()

                await manage_quotes(client, mid, obi)
                check_fills_from_trade_tape(last_trade_price, last_trade_received_at)

                now = time.time()
                if now - last_status_log >= 30:
                    last_status_log = now
                    debug_log("📊 Market-Maker Status", {
                        "mid_preis": round(mid, 4),
                        "obi": round(obi, 3),
                        "inventory": round(STATE["inventory"], 6),
                        "inventory_usdc": round(STATE["inventory"] * mid, 2),
                        "avg_entry_price": round(STATE["avg_entry_price"], 4),
                        "realized_pnl_usdc": round(STATE["realized_pnl_usdc"], 4),
                        "fills_gesamt": STATE["fills"],
                        "resting_bid": STATE["resting_bid"],
                        "resting_ask": STATE["resting_ask"],
                    })

                if not DRY_RUN and now - last_account_sync >= ACCOUNT_SYNC_SECONDS:
                    last_account_sync = now
                    await sync_real_position()

            elif channel.startswith("trade"):
                trades = msg.get("trades", [])
                if trades:
                    last_trade_price = float(trades[-1]["price"])
                    last_trade_received_at = time.time()


async def main():
    print("=" * 60)
    print(f"🚀 Market-Making Bot für {SYMBOL}")
    print(f"   DRY_RUN: {DRY_RUN} | Margin: {MARGIN} USDC | Hebel: {LEVERAGE}x")
    print(f"   Spread: {SPREAD_PCT}% | Requote alle {REQUOTE_SECONDS}s")
    print(f"   Max Position: {MAX_POSITION_USDC} USDC | OBI-Skew: {OBI_SKEW_ENABLED}")
    if not DRY_RUN:
        print("   ⚠️  LIVE-MODUS - platziert echte Post-Only Orders!")
    print("=" * 60)

    while True:
        try:
            await listen()
        except Exception as e:
            debug_log("⚠️ Verbindung verloren, reconnect in 5s", {"error": str(e), "traceback": traceback.format_exc()})
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
