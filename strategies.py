"""
strategies.py - Die 4 Handelsstrategien: Grid, Heikin-Ashi-Supertrend,
Kerzenfarbe, OBI-Scalp. Nutzt gemeinsame Infrastruktur aus bot_core.py.
"""

import asyncio
import websockets
import aiohttp
import json
import time
import traceback

from bot_core import (
    debug_log, BASE_URL, WS_URL, SYMBOLS, MARKET_INDICES, MARKET_INDEX_TO_SYMBOL,
    BOTS, execute_entry, execute_exit, compute_step_abs,
)

BINANCE_SYMBOL_MAP = {
    "BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT", "DOGE": "DOGEUSDT", "XRP": "XRPUSDT",
    "LINK": "LINKUSDT", "AVAX": "AVAXUSDT", "NEAR": "NEARUSDT", "DOT": "DOTUSDT", "TON": "TONUSDT",
    "SUI": "SUIUSDT", "BNB": "BNBUSDT", "UNI": "UNIUSDT", "APT": "APTUSDT", "ADA": "ADAUSDT",
    "TRX": "TRXUSDT", "LTC": "LTCUSDT", "BCH": "BCHUSDT", "HBAR": "HBARUSDT", "ICP": "ICPUSDT",
    # HYPE und Forex/Rohstoffe (EURUSD, XAU, WTI, ...) gibt es nicht auf Binance - dafuer greift der Lighter-Fallback
}


async def fetch_candles_binance(symbol, resolution, count_back=150):
    """Alternative Kerzenquelle - Binance hat deutlich mehr Liquiditaet als Lighter,
    kann daher weniger anfaellig fuer kurze Preis-Spikes/Wicks sein, die auf einer
    kleineren Perp-DEX Fehlsignale ausloesen wuerden."""
    pair = BINANCE_SYMBOL_MAP.get(symbol)
    if not pair:
        return None
    try:
        url = f"https://api.binance.com/api/v3/klines?symbol={pair}&interval={resolution}&limit={min(count_back, 500)}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    debug_log(f"⚠️ [{symbol}] Binance-Kerzenabfrage HTTP {resp.status}")
                    return None
                data = await resp.json()
    except Exception as e:
        debug_log(f"⚠️ [{symbol}] Binance-Kerzenabfrage fehlgeschlagen", {"error": str(e)})
        return None

    if not data or not isinstance(data, list):
        return None

    timestamps, opens, highs, lows, closes = [], [], [], [], []
    for k in data:
        timestamps.append(int(k[0]))
        opens.append(float(k[1]))
        highs.append(float(k[2]))
        lows.append(float(k[3]))
        closes.append(float(k[4]))
    return timestamps, opens, highs, lows, closes


async def fetch_candles_ohlc(symbol, resolution, count_back=150):
    """Kerzendaten inkl. Open (fuer Heikin Ashi noetig)."""
    import lighter
    configuration = lighter.Configuration(host=BASE_URL)
    async with lighter.ApiClient(configuration) as api_client:
        candle_api = lighter.CandlestickApi(api_client)
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - 60 * 60 * 24 * 7 * 1000
        response = await candle_api.candles(
            market_id=MARKET_INDICES[symbol], resolution=resolution,
            start_timestamp=start_ms, end_timestamp=now_ms,
            count_back=min(count_back, 500), set_timestamp_to_end=True,
        )
        candles = getattr(response, "c", None)
        if not candles:
            return None
        timestamps, opens, highs, lows, closes = [], [], [], [], []
        for candle in candles:
            t_ = getattr(candle, "t", None)
            o_ = getattr(candle, "o", None)
            h_ = getattr(candle, "h", None)
            l_ = getattr(candle, "l", None)
            c_ = getattr(candle, "c", None)
            if None in (t_, o_, h_, l_, c_):
                continue
            timestamps.append(int(t_)); opens.append(float(o_)); highs.append(float(h_))
            lows.append(float(l_)); closes.append(float(c_))
        return timestamps, opens, highs, lows, closes



def compute_ha_supertrend(opens, highs, lows, closes, period=5, multiplier=1.5):
    """Portiert aus dem Pine-Script: Heikin-Ashi-Kerzen + Custom-ATR-Baender/Trend."""
    n = len(closes)
    if n < period + 2:
        return None, None

    ha_close = [(opens[i] + highs[i] + lows[i] + closes[i]) / 4 for i in range(n)]
    ha_open = [0.0] * n
    ha_open[0] = (opens[0] + closes[0]) / 2
    for i in range(1, n):
        ha_open[i] = (ha_open[i - 1] + ha_close[i - 1]) / 2

    # ATR auf den ECHTEN Kerzen (wie im Pine-Script: ta.atr nutzt die realen high/low/close)
    tr = [highs[0] - lows[0]] + [0.0] * (n - 1)
    for i in range(1, n):
        tr[i] = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
    atr = [tr[0]] * n
    for i in range(1, n):
        if i < period:
            atr[i] = sum(tr[:i + 1]) / (i + 1)
        else:
            atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period

    up = [0.0] * n
    dn = [0.0] * n
    trend = [1] * n

    up[0] = ha_close[0] - multiplier * atr[0]
    dn[0] = ha_close[0] + multiplier * atr[0]
    last_up1 = up[0]
    last_dn1 = dn[0]

    for i in range(1, n):
        basic_up = ha_close[i] - multiplier * atr[i]
        basic_dn = ha_close[i] + multiplier * atr[i]

        if ha_close[i - 1] > up[i - 1]:
            last_up1 = up[i - 1]
        up1 = last_up1
        up[i] = max(basic_up, up1) if ha_close[i - 1] > up1 else basic_up

        if ha_close[i - 1] < dn[i - 1]:
            last_dn1 = dn[i - 1]
        dn1 = last_dn1
        dn[i] = min(basic_dn, dn1) if ha_close[i - 1] < dn1 else basic_dn

        prev_trend = trend[i - 1]
        if prev_trend == -1 and ha_close[i] > dn1:
            trend[i] = 1
        elif prev_trend == 1 and ha_close[i] < up1:
            trend[i] = -1
        else:
            trend[i] = prev_trend

    return trend, ha_close


async def ha_supertrend_poll_loop(symbol):
    """Reiner Buy/Sell-Wechsel-Bot auf Basis von Heikin-Ashi-Supertrend-Flips,
    SL an der High/Low der ausloesenden (echten) Kerze - kein TP."""
    b = BOTS[symbol]
    last_signal_ts = None

    while True:
        try:
            cfg = b["config"]
            if cfg["entry_mode"] == "ha_st":
                needed_bars = max(150, cfg["ha_st_trend_ema_length"] + 10) if cfg["ha_st_trend_filter"] else 150
                data = None
                if cfg.get("ha_st_candle_source", "binance") == "binance":
                    data = await fetch_candles_binance(symbol, cfg["ha_st_resolution"], count_back=needed_bars)
                    if data is None:
                        debug_log(f"ℹ️ [{symbol}] Nicht auf Binance verfügbar - nutze Lighter-Kerzen")
                if data is None:
                    data = await fetch_candles_ohlc(symbol, cfg["ha_st_resolution"], count_back=needed_bars)
                if data:
                    timestamps, opens, highs, lows, closes = data
                    closed_ts = timestamps[:-1]
                    closed_o, closed_h, closed_l, closed_c = opens[:-1], highs[:-1], lows[:-1], closes[:-1]
                    trend, ha_close = compute_ha_supertrend(closed_o, closed_h, closed_l, closed_c,
                                                              cfg["ha_st_atr_period"], cfg["ha_st_atr_mult"])
                    if trend and len(trend) >= 2:
                        st = b["state"]
                        signal_key = closed_ts[-1]

                        flipped_bullish = trend[-1] == 1 and trend[-2] == -1
                        flipped_bearish = trend[-1] == -1 and trend[-2] == 1

                        if (flipped_bullish or flipped_bearish) and last_signal_ts != signal_key:
                            last_signal_ts = signal_key
                            candle_age_seconds = round(time.time() - signal_key / 1000, 1)
                            if cfg["bot_active"]:
                                price = closed_c[-1]
                                new_direction = "long" if flipped_bullish else "short"
                                # SL an der auslösenden Kerze: unter ihrem Low (long) bzw. über ihrem High (short)
                                new_sl = closed_l[-1] if new_direction == "long" else closed_h[-1]

                                # Trendfilter: nur Longs im Aufwärtstrend (Preis > lange EMA), nur Shorts im Abwärtstrend
                                trend_ok = True
                                ema_trend_val = None
                                if cfg["ha_st_trend_filter"] and len(closed_c) > cfg["ha_st_trend_ema_length"]:
                                    ema_trend_val = round(_ema_series(closed_c, cfg["ha_st_trend_ema_length"])[-1], 4)
                                    if new_direction == "long" and price <= ema_trend_val:
                                        trend_ok = False
                                    elif new_direction == "short" and price >= ema_trend_val:
                                        trend_ok = False

                                debug_log(f"📡 [{symbol}] HA-Supertrend-Flip: {new_direction.upper()} @ {price} | SL {new_sl} | Trendfilter {'OK' if trend_ok else 'BLOCKIERT'}", {
                                    "kerze_alter_sekunden": candle_age_seconds,
                                    "ema_trend": ema_trend_val,
                                })

                                if st["position"] is not None and st["position"] != new_direction:
                                    await execute_exit(symbol, price, "HA-REVERSE")
                                    if trend_ok:
                                        await execute_entry(symbol, new_direction, price, is_add_on=False)
                                        st["ha_st_stop_price"] = new_sl
                                elif st["position"] is None and trend_ok:
                                    await execute_entry(symbol, new_direction, price, is_add_on=False)
                                    st["ha_st_stop_price"] = new_sl
        except Exception as e:
            debug_log(f"⚠️ [{symbol}] HA-Supertrend-Abfrage fehlgeschlagen", {"error": str(e)})

        await asyncio.sleep(5)



def _ema_series(values, length):
    if not values:
        return []
    k = 2 / (length + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out



async def handle_candle_color_tick(symbol, price):
    """Reine Preis-Feed-Strategie, keine Kerzen-API noetig - Kerzengrenzen werden
    lokal aus der Uhrzeit berechnet. Frueher Einstieg nach X Sekunden Kerzenlaufzeit
    (aktuelle Farbe), Ausstieg nur bei tatsaechlich geschlossener Gegenfarben-Kerze."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]

    now = time.time()
    resolution = cfg["cc_resolution_seconds"]
    confirm_delay = cfg["cc_confirm_delay_seconds"]
    candle_start = int(now // resolution) * resolution

    if st["cc_candle_start"] is None:
        st["cc_candle_start"] = candle_start
        st["cc_candle_open"] = price
        st["cc_entered_this_candle"] = False
        return

    if candle_start != st["cc_candle_start"]:
        # Vorherige Kerze ist soeben geschlossen - ihre Endfarbe bestimmen
        prev_open = st["cc_candle_open"]
        prev_close = price  # letzter bekannter Preis vor der neuen Kerze
        closed_color = "green" if prev_close > prev_open else ("red" if prev_close < prev_open else st["cc_last_color"])
        st["cc_last_color"] = closed_color

        if st["position"] is not None:
            position_color = "green" if st["position"] == "long" else "red"
            if closed_color is not None and closed_color != position_color:
                await execute_exit(symbol, price, "CC-REVERSE")

        # Neue Kerze beginnt
        st["cc_candle_start"] = candle_start
        st["cc_candle_open"] = price
        st["cc_entered_this_candle"] = False

    if not cfg["bot_active"]:
        return

    candle_age = now - st["cc_candle_start"]

    # Fruehe Ausstiegs-Pruefung: kontinuierlich (jeden Tick), nicht nur einmal pro Kerze -
    # sobald die AKTUELL LAUFENDE Kerze nach confirm_delay Sekunden die Gegenfarbe zeigt,
    # wird sofort geschlossen, OHNE auf den Kerzenschluss zu warten.
    if cfg.get("cc_early_exit", True) and st["position"] is not None and candle_age >= confirm_delay:
        current_color = "green" if price > st["cc_candle_open"] else ("red" if price < st["cc_candle_open"] else None)
        position_color = "green" if st["position"] == "long" else "red"
        if current_color is not None and current_color != position_color:
            await execute_exit(symbol, price, "CC-EARLY-EXIT")
            st["cc_entered_this_candle"] = True
            if cfg.get("cc_auto_reverse", True):
                new_direction = "long" if current_color == "green" else "short"
                await execute_entry(symbol, new_direction, price, is_add_on=False)
            return

    if not st["cc_entered_this_candle"] and candle_age >= confirm_delay:
        st["cc_entered_this_candle"] = True
        current_color = "green" if price > st["cc_candle_open"] else ("red" if price < st["cc_candle_open"] else None)
        if current_color is None:
            return
        direction = "long" if current_color == "green" else "short"

        if st["position"] is None:
            await execute_entry(symbol, direction, price, is_add_on=False)
        elif st["position"] != direction and cfg.get("cc_auto_reverse", True):
            # Sicherheitsnetz, falls die fruehe Ausstiegs-Pruefung oben deaktiviert ist
            await execute_exit(symbol, price, "CC-REVERSE")
            await execute_entry(symbol, direction, price, is_add_on=False)



def calc_obi(symbol, levels):
    book = BOTS[symbol]["state"]["obi_book"]
    bids_sorted = sorted(book["bids"].items(), key=lambda x: float(x[0]), reverse=True)[:levels]
    asks_sorted = sorted(book["asks"].items(), key=lambda x: float(x[0]))[:levels]
    bid_vol = sum(v for _, v in bids_sorted)
    ask_vol = sum(v for _, v in asks_sorted)
    total = bid_vol + ask_vol
    return 0.0 if total == 0 else (bid_vol - ask_vol) / total


def update_obi_windows(symbol, raw_obi, fast_s, medium_s, slow_s):
    """Ein gemeinsamer Rohwert-Puffer, daraus werden alle drei Zeitfenster berechnet -
    effizienter als drei getrennte Puffer, und alle drei sehen exakt dieselben Rohdaten."""
    st = BOTS[symbol]["state"]
    now = time.time()
    buf = st["obi_avg_buffer"]
    buf.append((raw_obi, now))
    max_window = max(fast_s, medium_s, slow_s)
    cutoff = now - max_window
    buf = [d for d in buf if d[1] >= cutoff]
    st["obi_avg_buffer"] = buf

    def avg_over(seconds):
        window_cutoff = now - seconds
        vals = [v for v, ts in buf if ts >= window_cutoff]
        return sum(vals) / len(vals) if vals else 0.0

    return avg_over(fast_s), avg_over(medium_s), avg_over(slow_s)


def check_obi_reversal(st, fast, long_threshold, short_threshold, min_bounce):
    """Separate Long-/Short-Logik für den OBI-Reversal-Modus mit zwei eigenen Schwellenwerten:
    Läuft der OBI-EMA-Verlauf über +short_threshold (überkauft) und dreht danach wieder nach
    unten -> Short (der Kurs dreht laut Beobachtung nach dem Hoch nach unten).
    Läuft er unter -long_threshold (überverkauft) und erholt sich danach wieder -> Long
    (der Kurs steigt laut Beobachtung, sobald sich die Unterseite erholt).
    long_threshold und short_threshold werden als positive Beträge angegeben
    (z.B. long_threshold=0.20 -> Long-Zone ab OBI <= -0.20, short_threshold=0.30 -> Short-Zone ab OBI >= +0.30).
    Es wird jeweils der Extremwert innerhalb der Zone gemerkt, damit erst bei einer
    echten Umkehr (Rückprall um min_bounce) ausgelöst wird, nicht schon beim ersten Zittern."""
    prev = st.get("obi_prev_fast")
    st["obi_prev_fast"] = fast

    if fast >= short_threshold:
        st["obi_extreme_zone"] = "overbought"
        st["obi_extreme_value"] = max(fast, st.get("obi_extreme_value") if st.get("obi_extreme_value") is not None else fast)
    elif fast <= -long_threshold:
        st["obi_extreme_zone"] = "oversold"
        st["obi_extreme_value"] = min(fast, st.get("obi_extreme_value") if st.get("obi_extreme_value") is not None else fast)

    zone = st.get("obi_extreme_zone")
    if zone is None or prev is None:
        return None

    direction = None
    if zone == "overbought" and fast < prev:
        drop = st["obi_extreme_value"] - fast
        if drop >= min_bounce:
            direction = "short"
    elif zone == "oversold" and fast > prev:
        rise = fast - st["obi_extreme_value"]
        if rise >= min_bounce:
            direction = "long"

    if direction is not None:
        # Zone zuruecksetzen, damit die naechste Extremzone wieder frisch erkannt wird
        st["obi_extreme_zone"] = None
        st["obi_extreme_value"] = None

    return direction


async def handle_obi_order_book_update(symbol, msg):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if cfg["entry_mode"] != "obi_scalp":
        return

    ob = msg.get("order_book", {})
    book = st["obi_book"]
    for side_key in ("bids", "asks"):
        for level in ob.get(side_key, []):
            price = level["price"]
            size = float(level["size"])
            if size == 0:
                book[side_key].pop(price, None)
            else:
                book[side_key][price] = size

    raw_obi = calc_obi(symbol, cfg["obi_levels"])
    fast, medium, slow = update_obi_windows(
        symbol, raw_obi,
        cfg["obi_window_fast_seconds"], cfg["obi_window_medium_seconds"], cfg["obi_window_slow_seconds"],
    )
    st["obi_fast"] = round(fast, 4)
    st["obi_medium"] = round(medium, 4)
    st["obi_slow"] = round(slow, 4)
    st["obi_current"] = st["obi_fast"]

    st["obi_history"].append({"ts": int(time.time() * 1000), "fast": st["obi_fast"], "medium": st["obi_medium"], "slow": st["obi_slow"]})
    if len(st["obi_history"]) > 300:
        st["obi_history"].pop(0)

    if st["position"] is not None or not cfg["bot_active"]:
        return

    now = time.time()
    if now - st["obi_last_trade_time"] < cfg["obi_cooldown_seconds"]:
        return

    obi_mode = cfg.get("obi_mode", "momentum")
    threshold = cfg["obi_threshold"]

    if obi_mode == "reversal":
        # Separater Long-/Short-Einstieg bei Umkehr aus einer Extremzone, mit eigenen Schwellenwerten
        direction = check_obi_reversal(
            st, fast,
            cfg.get("obi_long_threshold", 0.20), cfg.get("obi_short_threshold", 0.30),
            cfg.get("obi_reversal_min_bounce", 0.05),
        )
        if direction is None:
            return
    else:
        mean_reversion = obi_mode == "mean_reversion"

        def side_of(value):
            if value >= threshold:
                return "short" if mean_reversion else "long"
            if value <= -threshold:
                return "long" if mean_reversion else "short"
            return None

        fast_dir, medium_dir, slow_dir = side_of(fast), side_of(medium), side_of(slow)

        if fast_dir is None or fast_dir != medium_dir or fast_dir != slow_dir:
            st["obi_last_signal_direction"] = None
            return
        direction = fast_dir

        if direction == st["obi_last_signal_direction"]:
            return

    # Optionaler Trendfilter: nur Longs ueber der lebenden EMA, nur Shorts darunter
    if cfg["obi_trend_filter"] and st["obi_trend_ema"] is not None and st["last_price"] is not None:
        if direction == "long" and st["last_price"] <= st["obi_trend_ema"]:
            return
        if direction == "short" and st["last_price"] >= st["obi_trend_ema"]:
            return

    if st["last_price"] is None:
        return

    st["obi_last_signal_direction"] = direction
    st["obi_last_trade_time"] = now
    debug_log(f"📡 [{symbol}] OBI-Scalp Signal: {direction.upper()} @ {st['last_price']} (schnell {round(fast,3)} / mittel {round(medium,3)} / langsam {round(slow,3)})")
    await execute_entry(symbol, direction, st["last_price"], is_add_on=False)


def update_obi_trend_ema(symbol, price, ema_length):
    st = BOTS[symbol]["state"]
    if st["obi_trend_ema"] is None:
        st["obi_trend_ema"] = price
        return
    k = 2 / (ema_length + 1)
    st["obi_trend_ema"] = price * k + st["obi_trend_ema"] * (1 - k)



async def on_price_update(symbol, price):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    st["last_price"] = price

    st["price_history"].append({"ts": int(time.time() * 1000), "price": price})
    if len(st["price_history"]) > 500:
        st["price_history"].pop(0)

    if st["anchor_price"] is None:
        st["anchor_price"] = price
        return

    bot_active = cfg["bot_active"]

    if cfg["entry_mode"] == "obi_scalp":
        if cfg["obi_trend_filter"]:
            update_obi_trend_ema(symbol, price, cfg["obi_trend_ema_length"])
        if st["position"] is not None:
            entry = st["avg_entry_price"]
            if cfg.get("obi_tp_sl_mode", "pct") == "usd":
                pnl_usd = (price - entry) * st["total_coin_size"] if st["position"] == "long" else (entry - price) * st["total_coin_size"]
                if pnl_usd >= cfg["obi_tp_usd"]:
                    await execute_exit(symbol, price, "TP")
                elif pnl_usd <= -cfg["obi_sl_usd"]:
                    await execute_exit(symbol, price, "SL")
            else:
                pnl_pct = (price - entry) / entry * 100 if st["position"] == "long" else (entry - price) / entry * 100
                if pnl_pct >= cfg["obi_tp_pct"]:
                    await execute_exit(symbol, price, "TP")
                elif pnl_pct <= -cfg["obi_sl_pct"]:
                    await execute_exit(symbol, price, "SL")
        return

    if cfg["entry_mode"] == "candle_color":
        await handle_candle_color_tick(symbol, price)
        return

    if st["position"] is None:
        if not bot_active or cfg["entry_mode"] != "grid":
            return  # im HA-Supertrend-Modus übernimmt der Poll-Loop den Einstieg
        grid_step_abs = compute_step_abs(st["anchor_price"], cfg, "grid")
        if price <= st["anchor_price"] - grid_step_abs:
            await execute_entry(symbol, "long", price, is_add_on=False)
        elif price >= st["anchor_price"] + grid_step_abs:
            await execute_entry(symbol, "short", price, is_add_on=False)
        return

    if cfg["entry_mode"] == "ha_st":
        sl = st.get("ha_st_stop_price")
        if sl is not None:
            if (st["position"] == "long" and price <= sl) or (st["position"] == "short" and price >= sl):
                await execute_exit(symbol, price, "SL")
        return

    if cfg["entry_mode"] != "grid":
        return

    tp_step_abs = compute_step_abs(st["avg_entry_price"], cfg, "tp")
    # Nachkauf-Abstand wird vom LETZTEN Kaufpreis gemessen, nicht vom laufenden
    # Durchschnitt - sonst schrumpft der Abstand zwischen Nachkaeufen immer weiter.
    grid_step_abs = compute_step_abs(st["last_entry_price"] or st["avg_entry_price"], cfg, "grid")
    max_nachkauf = cfg["max_nachkauf"]

    if st["position"] == "long":
        if price >= st["avg_entry_price"] + tp_step_abs:
            await execute_exit(symbol, price, "TP")
        elif bot_active and price <= st["last_entry_price"] - grid_step_abs and (max_nachkauf == 0 or st["entry_count"] < max_nachkauf):
            await execute_entry(symbol, "long", price, is_add_on=True)
    elif st["position"] == "short":
        if price <= st["avg_entry_price"] - tp_step_abs:
            await execute_exit(symbol, price, "TP")
        elif bot_active and price >= st["last_entry_price"] + grid_step_abs and (max_nachkauf == 0 or st["entry_count"] < max_nachkauf):
            await execute_entry(symbol, "short", price, is_add_on=True)



async def trading_loop():
    last_status_log = 0.0

    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20) as ws:
                for s in SYMBOLS:
                    await ws.send(json.dumps({"type": "subscribe", "channel": f"trade/{MARKET_INDICES[s]}"}))
                    await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{MARKET_INDICES[s]}"}))
                debug_log(f"✅ Verbunden für {', '.join(SYMBOLS)}")

                async for raw in ws:
                    msg = json.loads(raw)
                    channel = msg.get("channel", "")
                    try:
                        market_index = int(channel.split(":")[1].split("/")[0]) if ":" in channel else int(channel.split("/")[1])
                    except Exception:
                        market_index = None
                    symbol = MARKET_INDEX_TO_SYMBOL.get(market_index)

                    if channel.startswith("trade") and symbol:
                        trades = msg.get("trades", [])
                        if trades:
                            price = float(trades[-1]["price"])
                            await on_price_update(symbol, price)
                    elif channel.startswith("order_book") and symbol:
                        await handle_obi_order_book_update(symbol, msg)

                    now = time.time()
                    if now - last_status_log >= 20:
                        last_status_log = now
                        summary = {s: {"pos": BOTS[s]["state"]["position"] or "flach", "preis": BOTS[s]["state"]["last_price"],
                                       "trades": BOTS[s]["state"]["stats"]["trades"]} for s in SYMBOLS}
                        debug_log("📊 Multi-Coin Status", summary)
        except Exception as e:
            debug_log("⚠️ Verbindung verloren, reconnect in 5s", {"error": str(e), "traceback": traceback.format_exc()})
            await asyncio.sleep(5)


# ========== WEB-DASHBOARD ==========
