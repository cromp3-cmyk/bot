"""
Autonomer EMA Crossover Bot für Lighter (zkLighter) - MIT BACKTEST & SOFORT-REAKTION
======================================================================================
- Holt echte Kerzendaten über die Lighter Candlestick-API
- Handelt autonom bei EMA-Crossover (Kerzenschluss-Basis, kein Repainting)
- REAGIERT SOFORT nach Kerzenschluss (keine 3 Minuten Verzögerung!)
- Führt optional beim Start (und danach periodisch) einen Backtest durch
- Zeigt Backtest-Ergebnisse im Log

WICHTIG - SICHERHEIT:
Erst mit DRY_RUN=true testen! Schau dir den Backtest UND ein paar Stunden
Live-Beobachtung an, bevor du auf DRY_RUN=false stellst.
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


# ========== RESOLUTION HELPER ==========
def get_resolution_int():
    """Holt die Resolution als Integer (entfernt 'm' etc.)"""
    res = os.getenv("EMA_RESOLUTION", "5")
    return int(str(res).replace('m', '').replace('in', '').strip())


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
        start_ms = now_ms - 60 * 60 * 24 * 7 * 1000

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


# ========== Order-Ausführung ==========
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


# ========== BACKTEST-MODUL ==========
async def fetch_candles_paginated(market_id, resolution, hours_back):
    """Holt Kerzen in mehreren 500er-Haeppchen rueckwaerts."""
    all_ts, all_closes = [], []
    now_ms = int(time.time() * 1000)
    target_start_ms = now_ms - hours_back * 3600 * 1000 - 3600 * 1000
    end_ms = now_ms

    for _ in range(30):
        raw = await fetch_candles(market_id, resolution, count_back=500, end_ms=end_ms)
        ts, closes = extract_close_prices_and_ts(raw)
        if not ts:
            break

        combined = sorted(zip(ts, closes), key=lambda x: x[0])
        batch_ts = [t for t, _ in combined]
        batch_closes = [c for _, c in combined]

        all_ts = batch_ts + all_ts
        all_closes = batch_closes + all_closes

        oldest_ts = batch_ts[0]
        if oldest_ts <= target_start_ms or len(batch_ts) < 2:
            break
        end_ms = oldest_ts - 1
        await asyncio.sleep(0.2)

    seen = set()
    dedup_ts, dedup_closes = [], []
    for t, c in zip(all_ts, all_closes):
        if t not in seen:
            seen.add(t)
            dedup_ts.append(t)
            dedup_closes.append(c)
    combined = sorted(zip(dedup_ts, dedup_closes), key=lambda x: x[0])
    return [t for t, _ in combined], [c for _, c in combined]


def print_backtest_results(result, hours_back, symbol, fast_len, slow_len):
    """Zeigt Backtest-Ergebnisse schön formatiert im Log an."""
    print("\n" + "=" * 80)
    print(f"📊 BACKTEST ERGEBNISSE - {symbol} | EMA {fast_len}/{slow_len} | {hours_back}h")
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
            direction = "📈 LONG" if trade['direction'] == 'buy' else "📉 SHORT"
            pnl = trade['pnl_pct']
            pnl_str = f"{pnl:+.2f}%"
            print(f"    {i:2}. {direction} | Entry: ${trade['entry']:.2f} | Exit: ${trade['exit']:.2f} | PnL: {pnl_str}")
    else:
        print("  📭 Keine Trades")
    
    print("=" * 80 + "\n")


def run_backtest(timestamps, closes, fast_len, slow_len):
    ema_fast = calc_ema_series(closes, fast_len)
    ema_slow = calc_ema_series(closes, slow_len)

    trades = []
    position = None
    last_relation = None
    equity_curve = []
    cumulative_pnl = 0.0

    for i in range(len(closes)):
        relation = "above" if ema_fast[i] > ema_slow[i] else "below"

        if last_relation is not None and relation != last_relation:
            direction = "buy" if relation == "above" else "sell"
            price = closes[i]

            if position is not None:
                entry = position["entry_price"]
                if position["direction"] == "buy":
                    pnl_pct = (price - entry) / entry * 100
                else:
                    pnl_pct = (entry - price) / entry * 100
                cumulative_pnl += pnl_pct
                trades.append({
                    "direction": position["direction"], "entry": entry, "exit": price,
                    "pnl_pct": round(pnl_pct, 4), "entry_ts": position["entry_ts"], "exit_ts": timestamps[i],
                })
                equity_curve.append({"ts": timestamps[i], "cum_pnl_pct": round(cumulative_pnl, 4)})

            position = {"direction": direction, "entry_price": price, "entry_ts": timestamps[i]}

        last_relation = relation

    wins = [t for t in trades if t["pnl_pct"] > 0]
    losses = [t for t in trades if t["pnl_pct"] <= 0]
    total_pnl = sum(t["pnl_pct"] for t in trades)

    return {
        "trades": trades,
        "equity_curve": equity_curve,
        "num_trades": len(trades),
        "num_wins": len(wins),
        "num_losses": len(losses),
        "win_rate_pct": round(len(wins) / len(trades) * 100, 1) if trades else 0,
        "total_pnl_pct": round(total_pnl, 4),
        "avg_win_pct": round(sum(t["pnl_pct"] for t in wins) / len(wins), 4) if wins else 0,
        "avg_loss_pct": round(sum(t["pnl_pct"] for t in losses) / len(losses), 4) if losses else 0,
    }


async def run_backtest_and_print():
    """Führt Backtest durch und gibt Ergebnisse im Log aus."""
    if not BACKTEST_ENABLED:
        return
    
    try:
        print(f"\n🔄 Backtest wird gestartet... ({BACKTEST_HOURS}h, {SYMBOL}, EMA {EMA_FAST_LEN}/{EMA_SLOW_LEN})")
        
        timestamps, closes = await fetch_candles_paginated(MARKET_INDEX, get_resolution_int(), BACKTEST_HOURS)
        
        if len(closes) < EMA_SLOW_LEN + 2:
            print(f"⚠️ Zu wenig Daten für Backtest (nur {len(closes)} Kerzen)")
            return
        
        result = run_backtest(timestamps, closes, EMA_FAST_LEN, EMA_SLOW_LEN)
        
        print_backtest_results(result, BACKTEST_HOURS, SYMBOL, EMA_FAST_LEN, EMA_SLOW_LEN)
        
        STATE["backtest_last_run"] = datetime.now().isoformat()
        
    except Exception as e:
        print(f"❌ Backtest fehlgeschlagen: {e}")
        debug_log("⚠️ Backtest fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})


# ========== LIVE TRADING - SOFORT-REAKTION ==========
OPEN_POSITIONS = {}
STATE = {
    "status": "startet...",
    "symbol": None, "resolution": None, "ema_fast_len": None, "ema_slow_len": None,
    "current": {}, "backtest_last_run": None,
    "dry_run": None, "started_at": datetime.now().isoformat(),
}

current_position_side = None
last_processed_candle_ts = None
last_relation = None


async def get_latest_candle(market_id, resolution):
    """Holt NUR die neueste geschlossene Kerze - für SOFORT-REAKTION"""
    import lighter
    configuration = lighter.Configuration(host=BASE_URL)
    async with lighter.ApiClient(configuration) as api_client:
        candle_api = lighter.CandlestickApi(api_client)
        
        now_ms = int(time.time() * 1000)
        # Nur die letzten 5 Minuten für Geschwindigkeit
        start_ms = now_ms - 5 * 60 * 1000
        
        response = await candle_api.candles(
            market_id=market_id,
            resolution=resolution,
            start_timestamp=start_ms,
            end_timestamp=now_ms,
            count_back=10,  # Nur die letzten 10 Kerzen
            set_timestamp_to_end=True,
        )
        return response


async def check_for_signal():
    global last_processed_candle_ts, last_relation, current_position_side

    try:
        # HOL DIE NEUESTEN KERZEN - SCHNELL!
        raw = await get_latest_candle(MARKET_INDEX, get_resolution_int())
        timestamps, closes = extract_close_prices_and_ts(raw)
        
        if len(closes) < EMA_SLOW_LEN + 2:
            return

        # ALLE Kerzen für EMA-Berechnung
        ema_fast = calc_ema_series(closes, EMA_FAST_LEN)
        ema_slow = calc_ema_series(closes, EMA_SLOW_LEN)
        
        # LETZTE geschlossene Kerze
        latest_ts = timestamps[-1]
        latest_close = closes[-1]
        latest_fast = ema_fast[-1]
        latest_slow = ema_slow[-1]
        
        current_relation = "above" if latest_fast > latest_slow else "below"

        # NUR bei NEUER Kerze reagieren
        if last_processed_candle_ts == latest_ts:
            return
        last_processed_candle_ts = latest_ts

        # Beim ersten Start nur initialisieren
        if last_relation is None:
            last_relation = current_relation
            print(f"🔄 Initialer Zustand: {current_relation} (EMA {EMA_FAST_LEN}: {latest_fast:.2f}, EMA {EMA_SLOW_LEN}: {latest_slow:.2f})")
            return

        # CROSSOVER erkannt! SOFORT handeln!
        if current_relation != last_relation:
            direction = "buy" if current_relation == "above" else "sell"
            print(f"\n📡 EMA CROSSOVER erkannt: {direction.upper()} {SYMBOL} @ ${latest_close:.2f}")
            print(f"   Zeit: {datetime.fromtimestamp(latest_ts/1000).strftime('%H:%M:%S')}")
            print(f"   EMA {EMA_FAST_LEN}: {latest_fast:.2f} | EMA {EMA_SLOW_LEN}: {latest_slow:.2f}")

            if current_position_side != direction:
                if DRY_RUN:
                    print(f"🧪 DRY_RUN: {direction.upper()} (keine echte Order)")
                    current_position_side = direction
                else:
                    # SOFORT Order ausführen!
                    result = await open_or_reverse_position(direction, SYMBOL, MARGIN, LEVERAGE, latest_close)
                    print(f"✅ Order ausgeführt: {result}")
                    current_position_side = direction

        last_relation = current_relation
        
    except Exception as e:
        debug_log("⚠️ check_for_signal Fehler", {"error": str(e), "traceback": traceback.format_exc()})


async def trading_loop():
    """Trading-Loop mit SOFORT-REAKTION auf Kerzenschluss"""
    print("🚀 Trading-Loop mit Sofort-Reaktion gestartet")
    
    while True:
        # Prüfe auf neue Kerze
        await check_for_signal()
        
        # Warte bis zur nächsten vollen Minute + 2 Sekunden
        # So reagiert der Bot SOFORT nach Kerzenschluss!
        now = datetime.now()
        seconds_to_next_minute = 60 - now.second
        
        if seconds_to_next_minute <= 2:
            # Kurz vor Kerzenschluss - ganz kurz warten
            await asyncio.sleep(1)
        elif seconds_to_next_minute <= 5:
            # Direkt nach Kerzenschluss - sofort prüfen
            await asyncio.sleep(1)
        else:
            # Normaler Poll-Intervall
            await asyncio.sleep(min(POLL_INTERVAL_SECONDS, seconds_to_next_minute - 2))


async def backtest_loop():
    """Periodischer Backtest"""
    await run_backtest_and_print()
    while True:
        await asyncio.sleep(BACKTEST_REFRESH_MINUTES * 60)
        await run_backtest_and_print()


# ========== KONFIGURATION ==========
SYMBOL = os.getenv("EMA_SYMBOL", "BTC")
if SYMBOL not in MARKET_INDICES:
    raise ValueError(f"Symbol {SYMBOL} nicht in MARKET_INDICES")
MARKET_INDEX = MARKET_INDICES[SYMBOL]

RESOLUTION = os.getenv("EMA_RESOLUTION", "5")
EMA_FAST_LEN = int(os.getenv("EMA_FAST_LEN", "7"))
EMA_SLOW_LEN = int(os.getenv("EMA_SLOW_LEN", "21"))
CANDLE_COUNT_BACK = int(os.getenv("CANDLE_COUNT_BACK", "100"))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "5"))  # ⬅️ Auf 5 Sekunden reduziert!

MARGIN = float(os.getenv("EMA_MARGIN", "100"))
LEVERAGE = int(os.getenv("EMA_LEVERAGE", "10"))

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

BACKTEST_ENABLED = os.getenv("BACKTEST_ENABLED", "true").lower() == "true"
BACKTEST_HOURS = int(os.getenv("BACKTEST_HOURS", "48"))
BACKTEST_REFRESH_MINUTES = int(os.getenv("BACKTEST_REFRESH_MINUTES", "60"))


async def main():
    print("=" * 80)
    print(f"🚀 EMA {EMA_FAST_LEN}/{EMA_SLOW_LEN} Crossover Bot für {SYMBOL}")
    print(f"   Resolution: {RESOLUTION}m | Poll-Intervall: {POLL_INTERVAL_SECONDS}s")
    print(f"   ⚡ Sofort-Reaktion: Ja (prüft bei jeder neuen Kerze)")
    print(f"   DRY_RUN: {DRY_RUN} | Margin: {MARGIN} USDC | Hebel: {LEVERAGE}x")
    print(f"   Backtest: {'AN' if BACKTEST_ENABLED else 'AUS'} ({BACKTEST_HOURS}h, alle {BACKTEST_REFRESH_MINUTES}min)")
    print("=" * 80)
    print("\n💡 Backtest-Ergebnisse werden hier im Log angezeigt!")
    print("⚡ Bot reagiert SOFORT nach Kerzenschluss!\n")

    tasks = []
    if BACKTEST_ENABLED:
        tasks.append(backtest_loop())
    tasks.append(trading_loop())
    
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
