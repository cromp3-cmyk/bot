"""
Einfacher neutraler Grid-Bot für Lighter (zkLighter) - MIT CHART-DASHBOARD
================================================================================
Dashboard mit Live-Chart: Grid-Levels, Entry-Preise, TP, aktuelle Position
"""

import asyncio
import websockets
import json
import time
import os
import traceback
from datetime import datetime
from collections import deque
from aiohttp import web

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

SYMBOL = os.getenv("GRID_SYMBOL", "BTC")
MARKET_INDEX = MARKET_INDICES[SYMBOL]
PORT = int(os.getenv("PORT", "10000"))

# ========== LIVE-KONFIGURIERBARE EINSTELLUNGEN ==========
CONFIG = {
    "dry_run": os.getenv("DRY_RUN", "true").lower() == "true",
    "margin": float(os.getenv("GRID_MARGIN", "20")),
    "leverage": int(os.getenv("GRID_LEVERAGE", "3")),
    "grid_step_pct": float(os.getenv("GRID_STEP_PCT", "0.25")),
    "tp_step_pct": float(os.getenv("TP_STEP_PCT", "0.25")),
    "max_nachkauf": int(os.getenv("MAX_NACHKAUF", "5")),
}

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
    price_decimals = get_price_decimals(SYMBOL)
    adjusted_price = reference_price * 0.98 if is_ask else reference_price * 1.02
    price_scaled = int(adjusted_price * (10 ** price_decimals))
    tx, tx_hash, err = await client.create_order(
        market_index=MARKET_INDEX, client_order_index=int(time.time() * 1000),
        base_amount=base_amount, price=price_scaled, is_ask=is_ask,
        order_type=client.ORDER_TYPE_MARKET,
        time_in_force=client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL, reduce_only=False,
    )
    return tx, tx_hash, err

# ========== STATE ==========
position = None
avg_entry_price = None
total_coin_size = 0.0
entry_count = 0
anchor_price = None
last_price = None
entry_prices = []  # Alle Entry-Preise für Chart

stats = {"trades": 0, "wins": 0, "losses": 0, "total_pnl_usd": 0.0}
trade_log = []

# Preis-Historie für Chart (max 1000 Einträge)
price_history = deque(maxlen=1000)
grid_levels = {"long": [], "short": [], "tp": []}  # Für Chart-Markierungen

def update_grid_levels():
    """Berechnet die Grid-Levels für Long und Short basierend auf aktuellem Anker"""
    global grid_levels
    if anchor_price is None:
        return
    
    grid_levels = {"long": [], "short": [], "tp": []}
    
    # Long Grid-Levels (nach unten)
    for i in range(1, CONFIG["max_nachkauf"] + 1):
        level = anchor_price * (1 - (CONFIG["grid_step_pct"] / 100) * i)
        grid_levels["long"].append(round(level, 2))
    
    # Short Grid-Levels (nach oben)
    for i in range(1, CONFIG["max_nachkauf"] + 1):
        level = anchor_price * (1 + (CONFIG["grid_step_pct"] / 100) * i)
        grid_levels["short"].append(round(level, 2))
    
    # TP Level (wenn Position offen)
    if position and avg_entry_price:
        if position == "long":
            tp = avg_entry_price * (1 + CONFIG["tp_step_pct"] / 100)
            grid_levels["tp"].append(round(tp, 2))
        else:
            tp = avg_entry_price * (1 - CONFIG["tp_step_pct"] / 100)
            grid_levels["tp"].append(round(tp, 2))

def estimate_liquidation_price():
    if position is None or avg_entry_price is None or CONFIG["leverage"] <= 0:
        return None
    factor = 1 / CONFIG["leverage"]
    if position == "long":
        return round(avg_entry_price * (1 - factor), 2)
    else:
        return round(avg_entry_price * (1 + factor), 2)

async def execute_entry(direction, price, is_add_on):
    global position, avg_entry_price, total_coin_size, entry_count, entry_prices

    position_usdc = CONFIG["margin"] * CONFIG["leverage"]
    new_units = position_usdc / price

    if not CONFIG["dry_run"]:
        client = get_lighter_client()
        if client is None:
            debug_log("⚠️ Kein Lighter-Client - Order übersprungen")
            return False
        precision = get_precision(SYMBOL)
        base_amount = int(new_units * precision)
        min_base = get_min_base_amount(SYMBOL)
        if base_amount * (1 / precision) < min_base:
            debug_log("⚠️ Order-Größe unter Mindestgröße")
            return False
        is_ask = direction == "short"
        tx, tx_hash, err = await place_market_order(client, is_ask, base_amount, price)
        await client.close()
        if err:
            debug_log("⚠️ Entry-Order fehlgeschlagen", {"error": str(err)})
            return False
        debug_log(f"✅ ECHTE Order ausgeführt: {direction.upper()} @ ~{price}", {"tx_hash": str(tx_hash)})

    if is_add_on:
        total_value = avg_entry_price * total_coin_size + price * new_units
        total_coin_size = total_coin_size + new_units
        avg_entry_price = total_value / total_coin_size
    else:
        avg_entry_price = price
        total_coin_size = new_units
        position = direction

    entry_prices.append(round(price, 2))
    entry_count += 1
    update_grid_levels()
    
    debug_log(f"📈 {'Nachkauf' if is_add_on else 'Neue Position'}: {direction.upper()} {SYMBOL} @ {price} | Ø-Einstieg {round(avg_entry_price, 2)} | Stufe {entry_count}")
    return True

async def execute_exit(price, reason):
    global position, avg_entry_price, total_coin_size, entry_count, anchor_price, entry_prices

    pnl_usd = (price - avg_entry_price) * total_coin_size if position == "long" else (avg_entry_price - price) * total_coin_size

    if not CONFIG["dry_run"]:
        client = get_lighter_client()
        if client is None:
            debug_log("⚠️ Kein Lighter-Client - Exit übersprungen (Position bleibt offen!)")
            return
        precision = get_precision(SYMBOL)
        base_amount = int(total_coin_size * precision)
        is_ask = position == "long"
        tx, tx_hash, err = await place_market_order(client, is_ask, base_amount, price)
        await client.close()
        if err:
            debug_log("⚠️ Exit-Order fehlgeschlagen - Position bleibt offen!", {"error": str(err)})
            return

    stats["trades"] += 1
    stats["total_pnl_usd"] += pnl_usd
    stats["wins" if pnl_usd > 0 else "losses"] += 1
    
    trade_entry = {
        "side": position, 
        "avg_entry": round(avg_entry_price, 2), 
        "exit": price,
        "entries": entry_count, 
        "pnl_usd": round(pnl_usd, 3), 
        "closed_at": datetime.now().isoformat(),
        "entry_prices": entry_prices.copy()  # Alle Entry-Preise für diesen Trade
    }
    trade_log.append(trade_entry)

    debug_log(f"🏁 Position geschlossen ({reason}): {position.upper()} Ø{round(avg_entry_price,2)} -> {price} | PnL ${round(pnl_usd,3)}")

    position = None
    avg_entry_price = None
    total_coin_size = 0.0
    entry_count = 0
    anchor_price = price
    entry_prices = []
    update_grid_levels()

async def on_price_update(price):
    global position, anchor_price, last_price
    last_price = price

    # Preis-Historie für Chart
    price_history.append({"time": int(time.time() * 1000), "value": price})

    if price is None:
        return

    if anchor_price is None:
        anchor_price = price
        update_grid_levels()
        debug_log(f"⚓ Anker gesetzt bei {price}")
        return

    if position is None:
        grid_step_abs = anchor_price * (CONFIG["grid_step_pct"] / 100)
        if price <= anchor_price - grid_step_abs:
            await execute_entry("long", price, is_add_on=False)
        elif price >= anchor_price + grid_step_abs:
            await execute_entry("short", price, is_add_on=False)
        return

    tp_step_abs = avg_entry_price * (CONFIG["tp_step_pct"] / 100)
    grid_step_abs = avg_entry_price * (CONFIG["grid_step_pct"] / 100)
    max_nachkauf = CONFIG["max_nachkauf"]

    if position == "long":
        if price >= avg_entry_price + tp_step_abs:
            await execute_exit(price, "TP")
        elif price <= avg_entry_price - grid_step_abs and (max_nachkauf == 0 or entry_count < max_nachkauf):
            await execute_entry("long", price, is_add_on=True)
    elif position == "short":
        if price <= avg_entry_price - tp_step_abs:
            await execute_exit(price, "TP")
        elif price >= avg_entry_price + grid_step_abs and (max_nachkauf == 0 or entry_count < max_nachkauf):
            await execute_entry("short", price, is_add_on=True)

async def trading_loop():
    last_status_log = 0.0
    retry_delay = 1
    max_delay = 60

    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20) as ws:
                retry_delay = 1
                await ws.send(json.dumps({"type": "subscribe", "channel": f"trade/{MARKET_INDEX}"}))
                debug_log(f"✅ Verbunden für {SYMBOL} (Market Index {MARKET_INDEX})")

                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("channel", "").startswith("trade"):
                        trades = msg.get("trades", [])
                        if trades:
                            price = float(trades[-1]["price"])
                            await on_price_update(price)

                            now = time.time()
                            if now - last_status_log >= 15:
                                last_status_log = now
                                debug_log("📊 Grid-Bot Status", {
                                    "preis": price, "position": position or "flach",
                                    "trades": stats["trades"], "pnl_usd": round(stats["total_pnl_usd"], 3),
                                })
        except Exception as e:
            debug_log("⚠️ Verbindung verloren, reconnect", {"error": str(e), "retry_in": retry_delay})
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, max_delay)

# ========== WEB-DASHBOARD MIT CHART ==========
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Grid-Bot Dashboard mit Chart</title>
<script src="https://unpkg.com/lightweight-charts@4.0.1/dist/lightweight-charts.standalone.production.js"></script>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, sans-serif; background:#0f1117; color:#e5e7eb; padding:20px; }
  
  .header { display:flex; justify-content:space-between; align-items:center; margin-bottom:20px; flex-wrap:wrap; gap:10px; }
  h1 { font-size:20px; display:flex; align-items:center; gap:12px; }
  .badge { padding:4px 12px; border-radius:12px; font-size:12px; font-weight:600; }
  .badge.dry { background:#3730a3; color:#c7d2fe; }
  .badge.live { background:#7f1d1d; color:#fecaca; }
  
  .grid-stats { display:grid; grid-template-columns: repeat(auto-fit, minmax(140px,1fr)); gap:12px; margin-bottom:16px; }
  .stat-card { background:#1a1d29; border-radius:10px; padding:12px; }
  .stat-card .label { font-size:11px; color:#9ca3af; text-transform:uppercase; }
  .stat-card .value { font-size:18px; font-weight:600; margin-top:4px; }
  .green { color:#4ade80; }
  .red { color:#f87171; }
  .yellow { color:#fbbf24; }
  
  .chart-container { background:#1a1d29; border-radius:10px; padding:16px; margin-bottom:16px; height:450px; position:relative; }
  #chart { width:100%; height:100%; }
  
  .legend { position:absolute; top:20px; right:20px; background:rgba(15,17,23,0.9); padding:10px 14px; border-radius:8px; font-size:12px; }
  .legend-item { display:flex; align-items:center; gap:8px; margin:4px 0; }
  .legend-color { width:16px; height:3px; border-radius:2px; }
  
  .config-section { background:#1a1d29; border-radius:10px; padding:16px; margin-bottom:16px; }
  .config-grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(150px,1fr)); gap:12px; }
  .config-grid label { display:block; font-size:11px; color:#9ca3af; margin-bottom:4px; }
  .config-grid input, .config-grid select { width:100%; padding:6px 8px; background:#0f1117; border:1px solid #2a2e3f; border-radius:6px; color:#e5e7eb; }
  .config-grid button { padding:8px 16px; background:#4f46e5; color:white; border:none; border-radius:6px; cursor:pointer; font-weight:600; }
  .config-grid button:hover { background:#4338ca; }
  
  .warn { background:#7f1d1d; color:#fecaca; padding:8px 12px; border-radius:8px; font-size:13px; margin-top:10px; display:none; }
  
  .trades-section { background:#1a1d29; border-radius:10px; padding:16px; }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th, td { text-align:left; padding:6px 8px; border-bottom:1px solid #2a2e3f; }
  th { color:#9ca3af; font-weight:500; }
</style>
</head>
<body>
<div class="header">
  <h1>📡 Grid-Bot <span id="mode-badge"></span></h1>
  <div style="font-size:13px; color:#9ca3af;" id="symbol-display">BTC</div>
</div>

<div class="grid-stats" id="stats-grid"></div>

<div class="chart-container">
  <div id="chart"></div>
  <div class="legend" id="chart-legend"></div>
</div>

<div class="warn" id="live-warn">⚠️ LIVE-Modus aktiv - echte Orders werden platziert!</div>

<div class="config-section">
  <form id="config-form" class="config-grid">
    <div><label>Margin (USDC)</label><input type="number" step="1" id="margin"></div>
    <div><label>Hebel</label><input type="number" step="1" id="leverage"></div>
    <div><label>Grid-Stufe (%)</label><input type="number" step="0.01" id="grid_step_pct"></div>
    <div><label>TP-Stufe (%)</label><input type="number" step="0.01" id="tp_step_pct"></div>
    <div><label>Max. Nachkauf</label><input type="number" step="1" id="max_nachkauf"></div>
    <div><label>Modus</label>
      <select id="dry_run">
        <option value="true">DRY RUN</option>
        <option value="false">LIVE</option>
      </select>
    </div>
    <button type="submit">Speichern</button>
  </form>
</div>

<div class="trades-section">
  <h2 style="font-size:15px; color:#9ca3af; margin-bottom:10px;">Letzte Trades</h2>
  <table id="trades-table"><thead><tr><th>Seite</th><th>Ø-Einstieg</th><th>Exit</th><th>Stufen</th><th>PnL $</th></tr></thead><tbody></tbody></table>
</div>

<script>
let chart = null;
let series = null;
let markers = [];

function initChart() {
  chart = LightweightCharts.createChart(document.getElementById('chart'), {
    width: document.getElementById('chart').parentElement.clientWidth - 32,
    height: 400,
    layout: { background: { color: '#1a1d29' }, textColor: '#9ca3af' },
    grid: { vertLines: { color: '#2a2e3f' }, horzLines: { color: '#2a2e3f' } },
    timeScale: { timeVisible: true, secondsVisible: false, borderColor: '#2a2e3f' },
  });
  
  series = chart.addLineSeries({
    color: '#4ade80',
    lineWidth: 2,
    priceLineVisible: false,
    lastValueVisible: true,
  });
  
  chart.priceScale('right').applyOptions({
    borderColor: '#2a2e3f',
    scaleMargins: { top: 0.1, bottom: 0.1 },
  });
}

function updateChart(data) {
  if (!chart || !series) return;
  
  // Preis-Historie
  const priceData = (data.price_history || []).map(p => ({
    time: Math.floor(p.time / 1000),
    value: p.value
  }));
  series.setData(priceData);
  
  // Markierungen
  const newMarkers = [];
  
  // Anker
  if (data.anchor_price) {
    newMarkers.push({
      time: priceData.length > 0 ? priceData[priceData.length-1].time : Math.floor(Date.now()/1000),
      position: 'belowBar',
      color: '#fbbf24',
      shape: 'arrowDown',
      text: `⚓ ${data.anchor_price}`,
    });
  }
  
  // Grid-Levels (Long)
  (data.grid_levels?.long || []).forEach((level, i) => {
    newMarkers.push({
      time: priceData.length > 0 ? priceData[priceData.length-1].time - (priceData.length - i) * 60 : Math.floor(Date.now()/1000),
      position: 'belowBar',
      color: '#f87171',
      shape: 'circle',
      text: `⬇️ L${i+1} ${level}`,
    });
  });
  
  // Grid-Levels (Short)
  (data.grid_levels?.short || []).forEach((level, i) => {
    newMarkers.push({
      time: priceData.length > 0 ? priceData[priceData.length-1].time - (priceData.length - i) * 60 : Math.floor(Date.now()/1000),
      position: 'aboveBar',
      color: '#60a5fa',
      shape: 'circle',
      text: `⬆️ S${i+1} ${level}`,
    });
  });
  
  // TP Level
  (data.grid_levels?.tp || []).forEach(level => {
    newMarkers.push({
      time: priceData.length > 0 ? priceData[priceData.length-1].time : Math.floor(Date.now()/1000),
      position: 'aboveBar',
      color: '#4ade80',
      shape: 'arrowUp',
      text: `🎯 TP ${level}`,
    });
  });
  
  // Entry-Preise
  (data.entry_prices || []).forEach((price, i) => {
    newMarkers.push({
      time: priceData.length > 0 ? priceData[priceData.length-1].time - (priceData.length - i) * 120 : Math.floor(Date.now()/1000),
      position: 'belowBar',
      color: data.position === 'long' ? '#4ade80' : '#f87171',
      shape: 'square',
      text: `📥 ${price}`,
    });
  });
  
  // Aktuelle Position
  if (data.position && data.last_price) {
    newMarkers.push({
      time: priceData.length > 0 ? priceData[priceData.length-1].time : Math.floor(Date.now()/1000),
      position: data.position === 'long' ? 'belowBar' : 'aboveBar',
      color: data.position === 'long' ? '#4ade80' : '#f87171',
      shape: 'arrowUp',
      text: `${data.position.toUpperCase()} @ ${data.last_price}`,
    });
  }
  
  series.setMarkers(newMarkers);
  
  // Legend
  document.getElementById('chart-legend').innerHTML = `
    <div class="legend-item"><span class="legend-color" style="background:#4ade80;"></span> Preis</div>
    ${data.anchor_price ? `<div class="legend-item"><span class="legend-color" style="background:#fbbf24;"></span> Anker ${data.anchor_price}</div>` : ''}
    ${data.position === 'long' ? `<div class="legend-item"><span class="legend-color" style="background:#4ade80;"></span> Long @ ${data.avg_entry_price}</div>` : ''}
    ${data.position === 'short' ? `<div class="legend-item"><span class="legend-color" style="background:#f87171;"></span> Short @ ${data.avg_entry_price}</div>` : ''}
  `;
}

async function refresh() {
  const res = await fetch('/api/status');
  const data = await res.json();
  
  // Badge
  document.getElementById('mode-badge').innerHTML =
    data.config.dry_run ? '<span class="badge dry">DRY RUN</span>' : '<span class="badge live">LIVE</span>';
  document.getElementById('live-warn').style.display = data.config.dry_run ? 'none' : 'block';
  document.getElementById('symbol-display').textContent = data.symbol;
  
  // Stats
  document.getElementById('stats-grid').innerHTML = `
    <div class="stat-card"><div class="label">Preis</div><div class="value">${data.last_price ?? '-'}</div></div>
    <div class="stat-card"><div class="label">Position</div><div class="value ${data.position==='long'?'green':data.position==='short'?'red':'yellow'}">${data.position || 'flach'}</div></div>
    <div class="stat-card"><div class="label">Ø-Einstieg</div><div class="value">${data.avg_entry_price ?? '-'}</div></div>
    <div class="stat-card"><div class="label">Stufe</div><div class="value">${data.entry_count} / ${data.config.max_nachkauf || '∞'}</div></div>
    <div class="stat-card"><div class="label">Trades</div><div class="value">${data.stats.trades}</div></div>
    <div class="stat-card"><div class="label">Win Rate</div><div class="value green">${data.stats.win_rate_pct}%</div></div>
    <div class="stat-card"><div class="label">Gesamt-PnL $</div><div class="value ${data.stats.total_pnl_usd>=0?'green':'red'}">${data.stats.total_pnl_usd}</div></div>
    <div class="stat-card"><div class="label">Liquidation</div><div class="value red">${data.liquidation_price ?? '-'}</div></div>
  `;
  
  // Chart
  updateChart(data);
  
  // Config
  if (!window.formTouched) {
    document.getElementById('margin').value = data.config.margin;
    document.getElementById('leverage').value = data.config.leverage;
    document.getElementById('grid_step_pct').value = data.config.grid_step_pct;
    document.getElementById('tp_step_pct').value = data.config.tp_step_pct;
    document.getElementById('max_nachkauf').value = data.config.max_nachkauf;
    document.getElementById('dry_run').value = String(data.config.dry_run);
  }
  
  // Trades
  const trades = (data.trade_log || []).slice(-15).reverse();
  document.querySelector('#trades-table tbody').innerHTML = trades.map(t => `
    <tr>
      <td>${t.side}</td>
      <td>${t.avg_entry}</td>
      <td>${t.exit}</td>
      <td>${t.entries}</td>
      <td class="${t.pnl_usd>=0?'green':'red'}">${t.pnl_usd}</td>
    </tr>
  `).join('');
}

// Config Form
document.getElementById('config-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const payload = {
    margin: parseFloat(document.getElementById('margin').value),
    leverage: parseInt(document.getElementById('leverage').value),
    grid_step_pct: parseFloat(document.getElementById('grid_step_pct').value),
    tp_step_pct: parseFloat(document.getElementById('tp_step_pct').value),
    max_nachkauf: parseInt(document.getElementById('max_nachkauf').value),
    dry_run: document.getElementById('dry_run').value === 'true',
  };
  await fetch('/api/config', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(payload) });
  window.formTouched = false;
  alert('✅ Konfiguration gespeichert!');
});

['margin','leverage','grid_step_pct','tp_step_pct','max_nachkauf','dry_run'].forEach(id => {
  document.getElementById(id).addEventListener('input', () => { window.formTouched = true; });
});

// Init
initChart();
refresh();
setInterval(refresh, 3000);

// Resize Chart
window.addEventListener('resize', () => {
  if (chart) {
    chart.resize(document.getElementById('chart').parentElement.clientWidth - 32, 400);
  }
});
</script>
</body>
</html>
"""

# ========== WEB SERVER ==========
async def handle_index(request):
    return web.Response(text=DASHBOARD_HTML, content_type="text/html")

async def handle_status(request):
    win_rate = round(stats["wins"] / stats["trades"] * 100, 1) if stats["trades"] else 0
    payload = {
        "symbol": SYMBOL,
        "last_price": last_price,
        "anchor_price": anchor_price,
        "position": position,
        "avg_entry_price": round(avg_entry_price, 2) if avg_entry_price else None,
        "entry_count": entry_count,
        "entry_prices": entry_prices,
        "liquidation_price": estimate_liquidation_price(),
        "config": CONFIG,
        "grid_levels": grid_levels,
        "price_history": list(price_history),
        "stats": {
            "trades": stats["trades"],
            "win_rate_pct": win_rate,
            "total_pnl_usd": round(stats["total_pnl_usd"], 3)
        },
        "trade_log": trade_log[-20:],
    }
    return web.json_response(payload)

async def handle_config_update(request):
    body = await request.json()
    for key in ["margin", "leverage", "grid_step_pct", "tp_step_pct", "max_nachkauf", "dry_run"]:
        if key in body:
            CONFIG[key] = body[key]
    update_grid_levels()
    debug_log("⚙️ Konfiguration aktualisiert", CONFIG)
    return web.json_response({"success": True, "config": CONFIG})

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/status", handle_status)
    app.router.add_post("/api/config", handle_config_update)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    debug_log(f"🌐 Dashboard läuft auf Port {PORT}")

async def main():
    print("=" * 60)
    print(f"🚀 Neutraler Grid-Bot für {SYMBOL} - Dashboard auf Port {PORT}")
    print(f"   DRY_RUN: {CONFIG['dry_run']} | Margin: {CONFIG['margin']} | Hebel: {CONFIG['leverage']}x")
    print("=" * 60)
    
    await start_web_server()
    await trading_loop()

if __name__ == "__main__":
    asyncio.run(main())
