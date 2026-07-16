"""
Autonomer EMA 9/21 Crossover Bot für Lighter (zkLighter) - KORREKTE EMA
==================================================================================
"""

import asyncio
import time
import os
import traceback
from datetime import datetime

# ========== BASE_URL ==========
BASE_URL = "https://mainnet.zklighter.elliot.ai"

# ========== DEBUG ==========
DEBUG_MODE = os.getenv("DEBUG_MODE", "true").lower() == "true"

def debug_log(msg, data=None):
    if DEBUG_MODE:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        print(f"[DEBUG {timestamp}] {msg}", flush=True)
        if data:
            import json
            print(f"   DATA: {json.dumps(data, indent=2, default=str)}", flush=True)

# ========== MARKET INDICES ==========
MARKET_INDICES = {
    "ETH": 0, "BTC": 1, "SOL": 2, "DOGE": 3, "1000PEPE": 4,
    "WIF": 5, "WLD": 6, "XRP": 7, "LINK": 8, "AVAX": 9,
    "NEAR": 10, "DOT": 11, "TON": 12, "TAO": 13, "POL": 14,
    "TRUMP": 15, "SUI": 16, "XLM": 119,
}

def get_precision(symbol):
    precision_map = {
        "BTC": 100000, "ETH": 10000, "SOL": 1000, "AVAX": 100,
        "LINK": 10, "NEAR": 10, "DOT": 10, "SUI": 10,
        "DOGE": 1, "XRP": 1, "POL": 1,
    }
    return precision_map.get(symbol, 10000)

def get_price_decimals(symbol):
    decimals_map = {
        "BTC": 1, "ETH": 2, "SOL": 3, "AVAX": 3,
        "LINK": 5, "NEAR": 5, "DOT": 5, "SUI": 5,
        "DOGE": 6, "XRP": 6, "POL": 6,
    }
    return decimals_map.get(symbol, 2)

def get_min_base_amount(symbol):
    min_amount_map = {
        "BTC": 0.00020, "ETH": 0.005, "SOL": 0.05, "AVAX": 0.5,
        "LINK": 1.0, "NEAR": 2.0, "DOT": 2.0, "SUI": 3.0,
        "DOGE": 10, "XRP": 20,
    }
    return min_amount_map.get(symbol, 0.001)

def get_lighter_client():
    try:
        import lighter
        API_KEY_INDEX = int(os.getenv("API_KEY_INDEX", "5"))
        PRIVATE_KEY = os.getenv("PRIVATE_KEY")
        ACCOUNT_INDEX = int(os.getenv("ACCOUNT_INDEX", "50960"))
        client = lighter.SignerClient(
            url=BASE_URL,
            api_private_keys={API_KEY_INDEX: PRIVATE_KEY},
            account_index=ACCOUNT_INDEX
        )
        return client
    except Exception as e:
        debug_log("Lighter Signer Client Fehler", {"error": str(e), "traceback": traceback.format_exc()})
        return None

async def fetch_candles(market_id, resolution, count_back=100):
    import lighter
    configuration = lighter.Configuration(host=BASE_URL)
    async with lighter.ApiClient(configuration) as api_client:
        candle_api = lighter.CandlestickApi(api_client)
        now = int(time.time())
        start = now - 60 * 60 * 24 * 7
        response = await candle_api.candles(
            market_id=market_id,
            resolution=resolution,
            start_timestamp=start,
            end_timestamp=now,
            count_back=count_back,
            set_timestamp_to_end=True,
        )
        return response

# ========== KORREKTE EMA-Berechnung (wie TradingView) ==========
def calc_ema_series(closes, length):
    """
    Berechnet EMA genau wie TradingView:
    1. Erster EMA = SMA der ersten 'length' Candles
    2. Danach: EMA = (Close - vorheriger_EMA) * (2/(length+1)) + vorheriger_EMA
    """
    if len(closes) < length:
        return []
    
    ema_values = []
    
    # 1. Erster EMA = SMA
    sma = sum(closes[:length]) / length
    ema_values.append(sma)
    
    # 2. EMA-Formel für alle weiteren
    k = 2 / (length + 1)
    for i in range(length, len(closes)):
        ema = closes[i] * k + ema_values[-1] * (1 - k)
        ema_values.append(ema)
    
    return ema_values

async def create_order_with_price(client, market_index, base_amount, is_ask, symbol, price, reduce_only=False):
    price_decimals = get_price_decimals(symbol)
    adjusted_price = price * 0.95 if is_ask else price * 1.05
    price_scaled = int(adjusted_price * (10 ** price_decimals))

    tx, tx_hash, err = await client.create_order(
        market_index=market_index,
        client_order_index=int(time.time() * 1000),
        base_amount=base_amount,
        price=price_scaled,
        is_ask=is_ask,
        order_type=client.ORDER_TYPE_MARKET,
        time_in_force=client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
        reduce_only=reduce_only,
        order_expiry=client.DEFAULT_IOC_EXPIRY,
    )
    return tx, tx_hash, err

async def open_or_reverse_position(action, symbol, margin, leverage, current_price):
    client = get_lighter_client()
    if client is None:
        return {"error": "Client konnte nicht initialisiert werden"}

    try:
        market_index = MARKET_INDICES[symbol]
        precision = get_precision(symbol)
        min_base_amount = get_min_base_amount(symbol)

        position_usdc = margin * leverage
        coin_amount = position_usdc / current_price
        base_amount = int(coin_amount * precision)

        if base_amount == 0:
            min_margin_needed = (min_base_amount * current_price) / leverage
            return {"error": f"Base Amount ist 0", "suggestion": f"Margin auf mind. {min_margin_needed:.2f} USDC erhöhen"}

        new_side = "long" if action == "buy" else "short"
        new_is_ask = action != "buy"

        try:
            await client.update_leverage(market_index=market_index, leverage=leverage, margin_mode=0)
        except Exception as e:
            debug_log("Hebel setzen fehlgeschlagen", {"error": str(e)})

        await asyncio.sleep(1)

        if symbol in OPEN_POSITIONS:
            existing_pos = OPEN_POSITIONS[symbol]
            if existing_pos["side"] != new_side:
                close_is_ask = existing_pos["side"] == "long"
                tx1, tx_hash1, err1 = await create_order_with_price(
                    client, market_index, existing_pos["base_amount"], close_is_ask, symbol,
                    existing_pos["open_price"], reduce_only=True
                )
                if err1:
                    return {"error": f"Close fehlgeschlagen: {err1}"}
                await asyncio.sleep(2)

                tx2, tx_hash2, err2 = await create_order_with_price(
                    client, market_index, base_amount, new_is_ask, symbol, current_price, reduce_only=False
                )
                if err2:
                    OPEN_POSITIONS.pop(symbol, None)
                    return {"error": f"Open nach Close fehlgeschlagen: {err2}"}

                OPEN_POSITIONS[symbol] = {
                    "side": new_side, "position_usdc": position_usdc, "coin_amount": coin_amount,
                    "base_amount": base_amount, "margin": margin, "leverage": leverage,
                    "open_price": current_price, "open_time": datetime.now().isoformat()
                }
                return {"success": True, "action": "reverse", "to_side": new_side, "tx_hash": str(tx_hash2)}
            else:
                return {"success": True, "action": "already_positioned", "side": new_side}
        else:
            tx, tx_hash, err = await create_order_with_price(
                client, market_index, base_amount, new_is_ask, symbol, current_price, reduce_only=False
            )
            if err:
                return {"error": str(err)}

            OPEN_POSITIONS[symbol] = {
                "side": new_side, "position_usdc": position_usdc, "coin_amount": coin_amount,
                "base_amount": base_amount, "margin": margin, "leverage": leverage,
                "open_price": current_price, "open_time": datetime.now().isoformat()
            }
            return {"success": True, "action": "open", "side": new_side, "tx_hash": str(tx_hash)}

    except Exception as e:
        debug_log("Exception in open_or_reverse_position", {"error": str(e), "traceback": traceback.format_exc()})
        return {"error": str(e)}
    finally:
        await client.close()

# ========== State ==========
OPEN_POSITIONS = {}

# ========== Konfiguration ==========
SYMBOL = os.getenv("EMA_SYMBOL", "SOL")
if SYMBOL not in MARKET_INDICES:
    raise ValueError(f"Symbol {SYMBOL} nicht in MARKET_INDICES")
MARKET_INDEX = MARKET_INDICES[SYMBOL]

RESOLUTION = os.getenv("EMA_RESOLUTION", "1m")
# ===== RICHTIGE EMA PARAMETER (wie TradingView) =====
EMA_FAST_LEN = int(os.getenv("EMA_FAST_LEN", "9"))    # Smoothing Length 9
EMA_SLOW_LEN = int(os.getenv("EMA_SLOW_LEN", "21"))   # Smoothing Length 21

# ===== 50 CANDLES REICHEN (wenn Berechnung korrekt ist) =====
MIN_CANDLES = int(os.getenv("MIN_CANDLES", "50"))

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "15"))

MARGIN = float(os.getenv("EMA_MARGIN", "10"))
LEVERAGE = int(os.getenv("EMA_LEVERAGE", "20"))

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

current_position_side = None
last_processed_candle_ts = None
last_relation = None

def extract_close_prices_and_ts(raw_response):
    candles = getattr(raw_response, "candlesticks", None)
    if candles is None and isinstance(raw_response, dict):
        candles = raw_response.get("candlesticks", [])
    if not candles:
        return [], []

    timestamps, closes = [], []
    for c in candles:
        ts = getattr(c, "timestamp", None) or getattr(c, "end_period_ts", None) or (c.get("timestamp") if isinstance(c, dict) else None)
        close = getattr(c, "close", None) or (c.get("close") if isinstance(c, dict) else None)
        if ts is not None and close is not None:
            timestamps.append(int(ts))
            closes.append(float(close))
    return timestamps, closes

async def check_for_signal():
    global last_processed_candle_ts, last_relation, current_position_side

    try:
        raw = await fetch_candles(MARKET_INDEX, RESOLUTION, count_back=max(EMA_SLOW_LEN * 5, 100))
    except Exception as e:
        debug_log("⚠️ Kerzen-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        return

    timestamps, closes = extract_close_prices_and_ts(raw)

    if len(closes) < EMA_SLOW_LEN + 2:
        debug_log("⚠️ Zu wenig Kerzendaten", {"erhalten": len(closes), "benötigt": EMA_SLOW_LEN + 2})
        return

    # Letzte Kerze weglassen (noch nicht geschlossen)
    closed_ts = timestamps[:-1]
    closed_closes = closes[:-1]
    
    if len(closed_closes) < MIN_CANDLES:
        debug_log(f"⏳ Sammle Candles... {len(closed_closes)}/{MIN_CANDLES}")
        return

    # ===== KORREKTE EMA-BERECHNUNG =====
    ema_fast = calc_ema_series(closed_closes, EMA_FAST_LEN)
    ema_slow = calc_ema_series(closed_closes, EMA_SLOW_LEN)

    if len(ema_fast) < 2 or len(ema_slow) < 2:
        debug_log("⚠️ EMAs konnten nicht berechnet werden")
        return

    latest_ts = closed_ts[-1]
    latest_fast = ema_fast[-1]
    latest_slow = ema_slow[-1]
    latest_close = closed_closes[-1]

    current_relation = "above" if latest_fast > latest_slow else "below"

    debug_log(f"📊 EMA Status {SYMBOL}", {
        "candles": len(closed_closes),
        "close_preis": latest_close,
        f"ema_{EMA_FAST_LEN}": round(latest_fast, 4),
        f"ema_{EMA_SLOW_LEN}": round(latest_slow, 4),
        "beziehung": current_relation,
        "position": current_position_side or "flach",
    })

    if last_processed_candle_ts == latest_ts:
        return
    last_processed_candle_ts = latest_ts

    if last_relation is not None and current_relation != last_relation:
        direction = "buy" if current_relation == "above" else "sell"
        debug_log(f"📡 EMA Cross erkannt: {direction.upper()} {SYMBOL} @ {latest_close}")

        if current_position_side != direction:
            if DRY_RUN:
                debug_log("🧪 DRY_RUN - keine Order")
                current_position_side = direction
            else:
                result = await open_or_reverse_position(direction, SYMBOL, MARGIN, LEVERAGE, latest_close)
                debug_log("Order-Ergebnis", result)
                if result.get("success"):
                    current_position_side = direction

    last_relation = current_relation

async def main():
    print("=" * 60)
    print(f"🚀 EMA {EMA_FAST_LEN}/{EMA_SLOW_LEN} Crossover Bot für {SYMBOL}")
    print(f"   Resolution: {RESOLUTION} | Poll: {POLL_INTERVAL_SECONDS}s")
    print(f"   Wartet auf {MIN_CANDLES} Candles")
    print(f"   DRY_RUN: {DRY_RUN} | Margin: {MARGIN} | Hebel: {LEVERAGE}x")
    print("=" * 60)

    while True:
        await check_for_signal()
        await asyncio.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    asyncio.run(main())
