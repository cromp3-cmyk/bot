"""
Autonomer EMA Crossover Bot für Lighter (zkLighter) - MIT BACKTEST
================================================================================
- Holt echte Kerzendaten über die Lighter Candlestick-API
- Handelt autonom bei EMA-Crossover (Kerzenschluss-Basis, kein Repainting)
- Führt beim Start einen Backtest über die letzten X Stunden durch
- Zeigt Backtest-Ergebnisse im Log

WICHTIG - SICHERHEIT:
Erst mit DRY_RUN=true testen!
"""

import asyncio
import time
import os
import json
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
            print(f"   DATA: {json.dumps(data, indent=2, default=str)}", flush=True)


# ========== MARKET INDICES ==========
MARKET_INDICES = {
    "ETH": 0, "BTC": 1, "SOL": 2, "DOGE": 3, "1000PEPE": 4,
    "WIF": 5, "WLD": 6, "XRP": 7, "LINK": 8, "AVAX": 9,
    "NEAR": 10, "DOT": 11, "TON": 12, "TAO": 13, "POL": 14,
    "TRUMP": 15, "SUI": 16, "XLM": 119,
}

# ========== COIN-PARAMETER ==========
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


# ========== LIGHTER CLIENTS ==========
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


async def fetch_candles(market_id, resolution, count_back=100, end_ms=None):
    """Holt Kerzendaten über die öffentliche Candlestick-API."""
    import lighter
    configuration = lighter.Configuration(host=BASE_URL)
    async with lighter.ApiClient(configuration) as api_client:
        candle_api = lighter.CandlestickApi(api_client)
        now_ms = end_ms if end_ms is not None else int(time.time() * 1000)
        start_ms = now_ms - 60 * 60 * 24 * 30 * 1000

        response = await candle_api.candles(
            market_id=market_id,
            resolution=resolution,
            start_timestamp=start_ms,
            end_timestamp=now_ms,
            count_back=min(count_back, 500),
            set_timestamp_to_end=True,
        )
        return response


def extract_close_prices_and_ts(raw_response):
    candles = getattr(raw_response, "c", None)
    if not candles:
        return [], []

    timestamps, closes = [], []
    for candle in candles:
        ts = getattr(candle, "t", None)
        close = getattr(candle, "c", None)
        if ts is not None and close is not None:
            timestamps.append(int(ts))
            closes.append(float(close))
    return timestamps, closes


# ========== EMA-Berechnung ==========
def calc_ema_series(closes, length):
    if not closes:
        return []
    k = 2 / (length + 1)
    ema_values = [closes[0]]
    for price in closes[1:]:
        ema_values.append(price * k + ema_values[-1] * (1 - k))
    return ema_values


# ========== BACKTEST ==========
def run_backtest(timestamps, closes, fast_len, slow_len):
    """Backtest: Bei jedem Crossover wird die Position gewechselt"""
    if len(closes) < slow_len + 2:
        return None
    
    ema_fast = calc_ema_series(closes, fast_len)
    ema_slow = calc_ema_series(closes, slow_len)

    trades = []
    position = None
    entry_price = 0
    entry_ts = 0
    last_relation = None
    cumulative_pnl = 0.0

    for i in range(len(closes)):
        relation = "above" if ema_fast[i] > ema_slow[i] else "below"
        
        if last_relation is None:
            last_relation = relation
            continue

        if relation != last_relation:
            current_price = closes[i]
            current_ts = timestamps[i]
            
            if relation == "above":  # BUY -> LONG
                if position is not None:
                    if position == "long":
                        pnl_pct = (current_price - entry_price) / entry_price * 100
                    else:
                        pnl_pct = (entry_price - current_price) / entry_price * 100
                    
                    cumulative_pnl += pnl_pct
                    trades.append({
                        "direction": position,
                        "entry": entry_price,
                        "exit": current_price,
                        "pnl_pct": round(pnl_pct, 4),
                        "entry_ts": entry_ts,
                        "exit_ts": current_ts,
                    })
                
                position = "long"
                entry_price = current_price
                entry_ts = current_ts
            
            else:  # SELL -> SHORT
                if position is not None:
                    if position == "long":
                        pnl_pct = (current_price - entry_price) / entry_price * 100
                    else:
                        pnl_pct = (entry_price - current_price) / entry_price * 100
                    
                    cumulative_pnl += pnl_pct
                    trades.append({
                        "direction": position,
                        "entry": entry_price,
                        "exit": current_price,
                        "pnl_pct": round(pnl_pct, 4),
                        "entry_ts": entry_ts,
                        "exit_ts": current_ts,
                    })
                
                position = "short"
                entry_price = current_price
                entry_ts = current_ts

            last_relation = relation

    # Letzte Position schließen
    if position is not None:
        last_price = closes[-1]
        last_ts = timestamps[-1]
        
        if position == "long":
            pnl_pct = (last_price - entry_price) / entry_price * 100
        else:
            pnl_pct = (entry_price - last_price) / entry_price * 100
        
        cumulative_pnl += pnl_pct
        trades.append({
            "direction": position,
            "entry": entry_price,
            "exit": last_price,
            "pnl_pct": round(pnl_pct, 4),
            "entry_ts": entry_ts,
            "exit_ts": last_ts,
        })

    completed_trades = [t for t in trades if t.get("exit") is not None]
    wins = [t for t in completed_trades if t["pnl_pct"] > 0]
    losses = [t for t in completed_trades if t["pnl_pct"] <= 0]
    total_pnl = sum(t["pnl_pct"] for t in completed_trades)

    return {
        "trades": completed_trades,
        "num_trades": len(completed_trades),
        "num_wins": len(wins),
        "num_losses": len(losses),
        "win_rate_pct": round(len(wins) / len(completed_trades) * 100, 1) if completed_trades else 0,
        "total_pnl_pct": round(total_pnl, 4),
        "avg_win_pct": round(sum(t["pnl_pct"] for t in wins) / len(wins), 4) if wins else 0,
        "avg_loss_pct": round(sum(t["pnl_pct"] for t in losses) / len(losses), 4) if losses else 0,
    }


async def run_backtest_and_print():
    """Führt Backtest durch und gibt Ergebnisse im Log aus."""
    if not BACKTEST_ENABLED:
        return
    
    try:
        print(f"\n🔄 Backtest wird gestartet... ({BACKTEST_HOURS}h, {SYMBOL}, {RESOLUTION}m, EMA {EMA_FAST_LEN}/{EMA_SLOW_LEN})")
        
        # HOL DIR DIE KERZEN - GENAU WIE BEIM LIVE-TRADING
        raw = await fetch_candles(
            MARKET_INDEX, 
            int(RESOLUTION),  # Als Integer!
            count_back=CANDLE_COUNT_BACK
        )
        
        timestamps, closes = extract_close_prices_and_ts(raw)
        
        if len(closes) < EMA_SLOW_LEN + 2:
            print(f"⚠️ Zu wenig Daten für Backtest (nur {len(closes)} Kerzen)")
            return
        
        # Auf die letzten X Stunden begrenzen
        if BACKTEST_HOURS > 0:
            cutoff_ts = int(time.time() * 1000) - BACKTEST_HOURS * 3600 * 1000
            filtered = [(t, c) for t, c in zip(timestamps, closes) if t >= cutoff_ts]
            if len(filtered) < EMA_SLOW_LEN + 2:
                print(f"⚠️ Zu wenig Daten für {BACKTEST_HOURS}h (nur {len(filtered)} Kerzen)")
                return
            timestamps, closes = zip(*filtered)
            timestamps = list(timestamps)
            closes = list(closes)
        
        result = run_backtest(timestamps, closes, EMA_FAST_LEN, EMA_SLOW_LEN)
        
        if result:
            print_backtest_results(result, BACKTEST_HOURS, SYMBOL, EMA_FAST_LEN, EMA_SLOW_LEN, RESOLUTION)
        else:
            print("❌ Backtest fehlgeschlagen")
        
    except Exception as e:
        print(f"❌ Backtest fehlgeschlagen: {e}")
        debug_log("⚠️ Backtest fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})


def print_backtest_results(result, hours_back, symbol, fast_len, slow_len, resolution):
    """Zeigt Backtest-Ergebnisse schön formatiert im Log an."""
    print("\n" + "=" * 80)
    print(f"📊 BACKTEST ERGEBNISSE - {symbol} | {resolution}m | EMA {fast_len}/{slow_len} | {hours_back}h")
    print("=" * 80)
    print(f"  📈 Anzahl Trades:      {result['num_trades']}")
    print(f"  ✅ Gewinne:            {result['num_wins']}")
    print(f"  ❌ Verluste:           {result['num_losses']}")
    print(f"  🎯 Trefferquote:       {result['win_rate_pct']}%")
    print(f"  💰 Total PnL:          {result['total_pnl_pct']:+.2f}%")
    print(f"  📊 Avg. Gewinn:        {result['avg_win_pct']:+.2f}%")
    print(f"  📉 Avg. Verlust:       {result['avg_loss_pct']:+.2f}%")
    print("-" * 80)
    
    if result['trades']:
        print("  📋 Letzte 10 Trades:")
        for i, trade in enumerate(result['trades'][-10:], 1):
            direction = "📈 LONG" if trade['direction'] == 'long' else "📉 SHORT"
            pnl = trade['pnl_pct']
            pnl_str = f"{pnl:+.2f}%"
            print(f"    {i:2}. {direction} | Entry: ${trade['entry']:.2f} | Exit: ${trade['exit']:.2f} | PnL: {pnl_str}")
    else:
        print("  📭 Keine Trades")
    
    print("=" * 80 + "\n")


# ========== LIVE TRADING ==========
OPEN_POSITIONS = {}
current_position_side = None
last_processed_candle_ts = None
last_relation = None


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
            return {"error": "Base Amount ist 0", "suggestion": f"Margin auf mind. {min_margin_needed:.2f} USDC erhöhen"}

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


async def check_for_signal():
    global last_processed_candle_ts, last_relation, current_position_side

    raw = None
    for attempt in range(3):
        try:
            raw = await fetch_candles(MARKET_INDEX, int(RESOLUTION), count_back=CANDLE_COUNT_BACK)
            timestamps, closes = extract_close_prices_and_ts(raw)
            if closes:
                break
            await asyncio.sleep(2)
        except Exception as e:
            debug_log(f"⚠️ Kerzen-Abfrage fehlgeschlagen (Versuch {attempt + 1}/3)", {"error": str(e)})
            await asyncio.sleep(2)
    else:
        return

    timestamps, closes = extract_close_prices_and_ts(raw)

    if len(closes) < EMA_SLOW_LEN + 2:
        return

    closed_ts = timestamps[:-1]
    closed_closes = closes[:-1]

    ema_fast = calc_ema_series(closed_closes, EMA_FAST_LEN)
    ema_slow = calc_ema_series(closed_closes, EMA_SLOW_LEN)

    latest_ts = closed_ts[-1]
    latest_fast = ema_fast[-1]
    latest_slow = ema_slow[-1]
    latest_close = closed_closes[-1]

    current_relation = "above" if latest_fast > latest_slow else "below"

    if last_relation is None:
        last_relation = current_relation
        print(f"🔄 Initialer Zustand: {current_relation} (EMA {EMA_FAST_LEN}: {latest_fast:.2f}, EMA {EMA_SLOW_LEN}: {latest_slow:.2f})")
        return

    if last_processed_candle_ts == latest_ts:
        return
    last_processed_candle_ts = latest_ts

    if current_relation != last_relation:
        direction = "buy" if current_relation == "above" else "sell"
        print(f"\n📡 EMA CROSSOVER erkannt: {direction.upper()} {SYMBOL} @ ${latest_close:.2f}")
        print(f"   EMA {EMA_FAST_LEN}: {latest_fast:.2f} | EMA {EMA_SLOW_LEN}: {latest_slow:.2f}")

        if current_position_side != direction:
            if DRY_RUN:
                print(f"🧪 DRY_RUN: {direction.upper()} (keine echte Order ausgeführt)")
                current_position_side = direction
            else:
                result = await open_or_reverse_position(direction, SYMBOL, MARGIN, LEVERAGE, latest_close)
                print(f"✅ Order-Ergebnis: {result}")
                current_position_side = direction

    last_relation = current_relation


async def trading_loop():
    while True:
        await check_for_signal()
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


# ========== KONFIGURATION ==========
SYMBOL = os.getenv("EMA_SYMBOL", "BTC")
if SYMBOL not in MARKET_INDICES:
    raise ValueError(f"Symbol {SYMBOL} nicht in MARKET_INDICES")
MARKET_INDEX = MARKET_INDICES[SYMBOL]

RESOLUTION = os.getenv("EMA_RESOLUTION", "5")
EMA_FAST_LEN = int(os.getenv("EMA_FAST_LEN", "7"))
EMA_SLOW_LEN = int(os.getenv("EMA_SLOW_LEN", "21"))
CANDLE_COUNT_BACK = int(os.getenv("CANDLE_COUNT_BACK", "100"))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "15"))

MARGIN = float(os.getenv("EMA_MARGIN", "100"))
LEVERAGE = int(os.getenv("EMA_LEVERAGE", "10"))

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

BACKTEST_ENABLED = os.getenv("BACKTEST_ENABLED", "true").lower() == "true"
BACKTEST_HOURS = int(os.getenv("BACKTEST_HOURS", "48"))


async def main():
    print("=" * 80)
    print(f"🚀 EMA {EMA_FAST_LEN}/{EMA_SLOW_LEN} Crossover Bot für {SYMBOL}")
    print(f"   Resolution: {RESOLUTION}m | Poll-Intervall: {POLL_INTERVAL_SECONDS}s")
    print(f"   DRY_RUN: {DRY_RUN} | Margin: {MARGIN} USDC | Hebel: {LEVERAGE}x")
    print(f"   Backtest: {'AN' if BACKTEST_ENABLED else 'AUS'} ({BACKTEST_HOURS}h)")
    print("=" * 80)

    # Backtest beim Start
    if BACKTEST_ENABLED:
        await run_backtest_and_print()
    
    print("\n🚀 Starte Live-Trading...")
    print("-" * 80)
    
    await trading_loop()


if __name__ == "__main__":
    asyncio.run(main())
