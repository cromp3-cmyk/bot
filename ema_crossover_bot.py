"""
Autonomer EMA Crossover Bot für Lighter (zkLighter) - MIT LIVE-DASHBOARD & BACKTEST
======================================================================================
- Holt echte Kerzendaten über die Lighter Candlestick-API
- Handelt autonom bei EMA-Crossover (Kerzenschluss-Basis, kein Repainting)
- Führt optional beim Start (und danach periodisch) einen Backtest über die
  letzten X Stunden durch
- Serviert ein Live-HTML-Dashboard (Status, EMA-Werte, Position, Backtest-Chart)

WICHTIG - RENDER SERVICE-TYP:
Dieses Skript startet jetzt einen eingebauten Webserver (aiohttp) für das
Dashboard. Damit du das im Browser siehst, muss der Render-Service als
"Web Service" laufen (nicht "Background Worker"), und der Start Command
bleibt "python -u ema_crossover_bot.py" - Render setzt automatisch die
Umgebungsvariable $PORT, die dieses Skript nutzt.

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
from aiohttp import web

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


# ========== MARKET INDICES (Ausschnitt - bei Bedarf ergänzen) ==========
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
    """Holt Kerzendaten über die öffentliche Candlestick-API (kein Private Key nötig)."""
    import lighter
    configuration = lighter.Configuration(host=BASE_URL)
    async with lighter.ApiClient(configuration) as api_client:
        candle_api = lighter.CandlestickApi(api_client)
        now_ms = end_ms if end_ms is not None else int(time.time() * 1000)
        start_ms = now_ms - 60 * 60 * 24 * 7 * 1000  # 7 Tage Puffer zurück (in ms)

        response = await candle_api.candles(
            market_id=market_id,
            resolution=resolution,
            start_timestamp=start_ms,
            end_timestamp=now_ms,
            count_back=min(count_back, 500),  # API-Limit: max 500 pro Call
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
    """Holt Kerzen in mehreren 500er-Haeppchen rueckwaerts, bis der Zeitraum abgedeckt ist."""
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

    # Duplikate entfernen, sortiert halten
    seen = set()
    dedup_ts, dedup_closes = [], []
    for t, c in zip(all_ts, all_closes):
        if t not in seen:
            seen.add(t)
            dedup_ts.append(t)
            dedup_closes.append(c)
    combined = sorted(zip(dedup_ts, dedup_closes), key=lambda x: x[0])
    return [t for t, _ in combined], [c for _, c in combined]


def run_backtest(timestamps, closes, fast_len, slow_len):
    ema_fast = calc_ema_series(closes, fast_len)
    ema_slow = calc_ema_series(closes, slow_len)

    trades = []
    position = None
    last_relation = None
    equity_curve = []  # [(ts, cumulative_pnl_pct), ...] fuers Chart
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


async def run_backtest_and_store():
    if not BACKTEST_ENABLED:
        return
    try:
        debug_log(f"📈 Starte Backtest ({BACKTEST_HOURS}h, {SYMBOL}, {RESOLUTION}, EMA {EMA_FAST_LEN}/{EMA_SLOW_LEN})...")
        timestamps, closes = await fetch_candles_paginated(MARKET_INDEX, RESOLUTION, BACKTEST_HOURS)
        if len(closes) < EMA_SLOW_LEN + 2:
            debug_log("⚠️ Zu wenig Daten für Backtest", {"erhalten": len(closes)})
            return
        result = run_backtest(timestamps, closes, EMA_FAST_LEN, EMA_SLOW_LEN)
        result["price_series"] = [{"ts": t, "close": c} for t, c in zip(timestamps, closes)]
        STATE["backtest"] = result
        STATE["backtest_last_run"] = datetime.now().isoformat()
        debug_log("✅ Backtest fertig", {
            "trades": result["num_trades"], "win_rate": result["win_rate_pct"],
            "total_pnl_pct": result["total_pnl_pct"],
        })
    except Exception as e:
        debug_log("⚠️ Backtest fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})


# ========== State ==========
OPEN_POSITIONS = {}
STATE = {
    "status": "startet...",
    "symbol": None, "resolution": None, "ema_fast_len": None, "ema_slow_len": None,
    "current": {}, "backtest": None, "backtest_last_run": None,
    "dry_run": None, "started_at": datetime.now().isoformat(),
}

# ========== Konfiguration ==========
SYMBOL = os.getenv("EMA_SYMBOL", "BTC")
if SYMBOL not in MARKET_INDICES:
    raise ValueError(f"Symbol {SYMBOL} nicht in MARKET_INDICES - Liste in dieser Datei ergänzen")
MARKET_INDEX = MARKET_INDICES[SYMBOL]

RESOLUTION = os.getenv("EMA_RESOLUTION", "5")  # "1","2","5","10","15" etc - Zeitrahmen
EMA_FAST_LEN = int(os.getenv("EMA_FAST_LEN", "7"))
EMA_SLOW_LEN = int(os.getenv("EMA_SLOW_LEN", "21"))
CANDLE_COUNT_BACK = int(os.getenv("CANDLE_COUNT_BACK", "100"))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "15"))

MARGIN = float(os.getenv("EMA_MARGIN", "100"))
LEVERAGE = int(os.getenv("EMA_LEVERAGE", "10"))

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

BACKTEST_ENABLED = os.getenv("BACKTEST_ENABLED", "true").lower() == "true"
BACKTEST_HOURS = int(os.getenv("BACKTEST_HOURS", "48"))
BACKTEST_REFRESH_MINUTES = int(os.getenv("BACKTEST_REFRESH_MINUTES", "60"))

PORT = int(os.getenv("PORT", "10000"))

current_position_side = None
last_processed_candle_ts = None
last_relation = None


async def check_for_signal():
    global last_processed_candle_ts, last_relation, current_position_side

    raw = None
    for attempt in range(3):
        try:
            raw = await fetch_candles(MARKET_INDEX, RESOLUTION, count_back=CANDLE_COUNT_BACK)
            timestamps, closes = extract_close_prices_and_ts(raw)
            if closes:
                break
            debug_log(f"⚠️ Leere Kerzenantwort, Versuch {attempt + 1}/3 - retry in 2s")
            await asyncio.sleep(2)
        except Exception as e:
            debug_log(f"⚠️ Kerzen-Abfrage fehlgeschlagen (Versuch {attempt + 1}/3)", {"error": str(e)})
            await asyncio.sleep(2)
    else:
        debug_log("⚠️ Kerzen-Abfrage nach 3 Versuchen weiterhin ohne Daten - überspringe diese Runde")
        return

    timestamps, closes = extract_close_prices_and_ts(raw)

    if len(closes) < EMA_SLOW_LEN + 2:
        debug_log("⚠️ Zu wenig Kerzendaten für EMA-Berechnung", {"erhaltene_kerzen": len(closes)})
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

    STATE["current"] = {
        "letzte_kerze_ts": latest_ts, "close": latest_close,
        "ema_fast": round(latest_fast, 4), "ema_slow": round(latest_slow, 4),
        "beziehung": current_relation, "updated_at": datetime.now().isoformat(),
        "price_history": [{"ts": t, "close": c} for t, c in zip(closed_ts[-100:], closed_closes[-100:])],
    }
    STATE["status"] = "läuft"

    debug_log(f"📊 EMA Status {SYMBOL}", {
        "close_preis": latest_close, f"ema_{EMA_FAST_LEN}": round(latest_fast, 4),
        f"ema_{EMA_SLOW_LEN}": round(latest_slow, 4), "beziehung": current_relation,
        "bot_position": current_position_side or "flach",
    })

    if last_processed_candle_ts == latest_ts:
        return
    last_processed_candle_ts = latest_ts

    if last_relation is not None and current_relation != last_relation:
        direction = "buy" if current_relation == "above" else "sell"
        debug_log(f"📡 EMA Cross erkannt: {direction.upper()} {SYMBOL} @ {latest_close}")

        if current_position_side != direction:
            if DRY_RUN:
                debug_log("🧪 DRY_RUN aktiv - keine echte Order ausgeführt")
                current_position_side = direction
            else:
                result = await open_or_reverse_position(direction, SYMBOL, MARGIN, LEVERAGE, latest_close)
                debug_log("Order-Ergebnis", result)
                current_position_side = direction
            STATE["current"]["bot_position"] = current_position_side

    last_relation = current_relation


async def trading_loop():
    while True:
        await check_for_signal()
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def backtest_loop():
    await run_backtest_and_store()
    while True:
        await asyncio.sleep(BACKTEST_REFRESH_MINUTES * 60)
        await run_backtest_and_store()


# ========== WEB-DASHBOARD ==========
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<title>EMA Crossover Bot Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
  body { font-family: -apple-system, sans-serif; background:#0f1117; color:#e5e7eb; margin:0; padding:20px; }
  h1 { font-size: 20px; }
  .grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(200px,1fr)); gap:12px; margin-bottom:20px; }
  .card { background:#1a1d29; border-radius:10px; padding:14px; }
  .card .label { font-size:12px; color:#9ca3af; text-transform:uppercase; }
  .card .value { font-size:22px; font-weight:600; margin-top:4px; }
  .green { color:#4ade80; } .red { color:#f87171; } .yellow { color:#fbbf24; }
  canvas { background:#1a1d29; border-radius:10px; padding:10px; margin-bottom:20px; }
  .badge { display:inline-block; padding:2px 10px; border-radius:12px; font-size:12px; font-weight:600; }
  .badge.dry { background:#3730a3; color:#c7d2fe; }
  .badge.live { background:#7f1d1d; color:#fecaca; }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th, td { text-align:left; padding:6px 8px; border-bottom:1px solid #2a2e3f; }
  th { color:#9ca3af; font-weight:500; }
</style>
</head>
<body>
<h1>📡 EMA Crossover Bot <span id="mode-badge"></span></h1>
<div class="grid" id="status-grid"></div>
<canvas id="priceChart" height="90"></canvas>
<canvas id="equityChart" height="90"></canvas>
<h2>Letzte Backtest-Trades</h2>
<table id="trades-table"><thead><tr><th>Richtung</th><th>Entry</th><th>Exit</th><th>PnL %</th></tr></thead><tbody></tbody></table>

<script>
let priceChart, equityChart;

async function refresh() {
  const res = await fetch('/api/status');
  const data = await res.json();

  document.getElementById('mode-badge').innerHTML =
    data.dry_run ? '<span class="badge dry">DRY RUN</span>' : '<span class="badge live">LIVE</span>';

  const c = data.current || {};
  const bt = data.backtest || {};

  document.getElementById('status-grid').innerHTML = `
    <div class="card"><div class="label">Symbol</div><div class="value">${data.symbol || '-'}</div></div>
    <div class="card"><div class="label">Zeitrahmen</div><div class="value">${data.resolution || '-'}</div></div>
    <div class="card"><div class="label">EMA</div><div class="value">${data.ema_fast_len}/${data.ema_slow_len}</div></div>
    <div class="card"><div class="label">Preis</div><div class="value">${c.close ?? '-'}</div></div>
    <div class="card"><div class="label">Beziehung</div><div class="value ${c.beziehung==='above'?'green':'red'}">${c.beziehung || '-'}</div></div>
    <div class="card"><div class="label">Bot-Position</div><div class="value yellow">${c.bot_position || 'flach'}</div></div>
    <div class="card"><div class="label">Backtest Trades (${data.backtest_hours}h)</div><div class="value">${bt.num_trades ?? '-'}</div></div>
    <div class="card"><div class="label">Trefferquote</div><div class="value">${bt.win_rate_pct ?? '-'}%</div></div>
    <div class="card"><div class="label">Backtest PnL (ungehebelt)</div><div class="value ${(bt.total_pnl_pct||0)>=0?'green':'red'}">${bt.total_pnl_pct ?? '-'}%</div></div>
  `;

  const priceHist = c.price_history || [];
  const priceLabels = priceHist.map(p => new Date(p.ts).toLocaleTimeString());
  const priceData = priceHist.map(p => p.close);

  if (!priceChart) {
    priceChart = new Chart(document.getElementById('priceChart'), {
      type: 'line',
      data: { labels: priceLabels, datasets: [{ label: 'Preis (live, letzte 100 Kerzen)', data: priceData, borderColor:'#60a5fa', pointRadius:0 }] },
      options: { responsive:true, scales:{ x:{ display:false }, y:{ ticks:{color:'#9ca3af'} } }, plugins:{legend:{labels:{color:'#e5e7eb'}}} }
    });
  } else {
    priceChart.data.labels = priceLabels;
    priceChart.data.datasets[0].data = priceData;
    priceChart.update();
  }

  const eq = bt.equity_curve || [];
  const eqLabels = eq.map(p => new Date(p.ts).toLocaleString());
  const eqData = eq.map(p => p.cum_pnl_pct);

  if (!equityChart) {
    equityChart = new Chart(document.getElementById('equityChart'), {
      type: 'line',
      data: { labels: eqLabels, datasets: [{ label: `Backtest Equity-Kurve (${data.backtest_hours}h, kumuliert %)`, data: eqData, borderColor:'#4ade80', pointRadius:0 }] },
      options: { responsive:true, scales:{ x:{ display:false }, y:{ ticks:{color:'#9ca3af'} } }, plugins:{legend:{labels:{color:'#e5e7eb'}}} }
    });
  } else {
    equityChart.data.labels = eqLabels;
    equityChart.data.datasets[0].data = eqData;
    equityChart.update();
  }

  const trades = (bt.trades || []).slice(-20).reverse();
  document.querySelector('#trades-table tbody').innerHTML = trades.map(t => `
    <tr>
      <td>${t.direction}</td>
      <td>${t.entry}</td>
      <td>${t.exit}</td>
      <td class="${t.pnl_pct>=0?'green':'red'}">${t.pnl_pct}%</td>
    </tr>
  `).join('');
}

refresh();
setInterval(refresh, 3000);
</script>
</body>
</html>
"""


async def handle_index(request):
    return web.Response(text=DASHBOARD_HTML, content_type="text/html")


async def handle_status(request):
    payload = {
        "status": STATE["status"], "symbol": SYMBOL, "resolution": RESOLUTION,
        "ema_fast_len": EMA_FAST_LEN, "ema_slow_len": EMA_SLOW_LEN,
        "dry_run": DRY_RUN, "current": STATE["current"],
        "backtest": STATE["backtest"], "backtest_last_run": STATE["backtest_last_run"],
        "backtest_hours": BACKTEST_HOURS, "started_at": STATE["started_at"],
    }
    return web.json_response(payload)


async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/status", handle_status)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    debug_log(f"🌐 Dashboard läuft auf Port {PORT}")


async def main():
    print("=" * 60)
    print(f"🚀 EMA {EMA_FAST_LEN}/{EMA_SLOW_LEN} Crossover Bot gestartet für {SYMBOL}")
    print(f"   Resolution: {RESOLUTION} | Poll-Intervall: {POLL_INTERVAL_SECONDS}s")
    print(f"   DRY_RUN: {DRY_RUN} | Margin: {MARGIN} USDC | Hebel: {LEVERAGE}x")
    print(f"   Backtest: {'AN' if BACKTEST_ENABLED else 'AUS'} ({BACKTEST_HOURS}h, refresh alle {BACKTEST_REFRESH_MINUTES}min)")
    print(f"   Dashboard-Port: {PORT}")
    print("=" * 60)

    await start_web_server()
    await asyncio.gather(
        trading_loop(),
        backtest_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
ain())
