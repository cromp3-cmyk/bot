"""
Multi-Coin neutraler Grid-Bot für Lighter (zkLighter) - MIT LIVE-DASHBOARD
=============================================================================
Laeuft mehrere Coins GLEICHZEITIG in einem einzigen Prozess (eine WebSocket-
Verbindung, mehrere Kanaele) - jeder Coin hat komplett eigene Einstellungen
und eigenen Zustand, kein zusaetzlicher Render-Service noetig.

Grid-Logik pro Coin (unabhaengig von den anderen):
1. Start: aktueller Preis = "Anker"
2. Preis bewegt sich GRID_STEP_PCT vom Anker weg -> Position eroeffnen
3. Position im Plus um TP_STEP_PCT (ab Ø-Einstieg) -> TP, optional sofort
   Gegenposition (auto_reverse)
4. Position im Minus um GRID_STEP_PCT (weitere Stufe) -> Nachkauf, bis MAX_NACHKAUF

WICHTIG - RENDER SERVICE-TYP: als "Web Service" laufen lassen (nicht Worker).
WICHTIG - SICHERHEIT: Erst DRY_RUN=true testen!
"""

import asyncio
import websockets
import json
import time
import os
import traceback
from datetime import datetime
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
MARKET_INDICES = {
    "ETH": 0, "BTC": 1, "SOL": 2, "DOGE": 3, "AVAX": 9, "SUI": 16,
    "EURUSD": 96, "GBPUSD": 97, "USDJPY": 98, "USDCHF": 99, "USDCAD": 100,
    "AUDUSD": 106, "NZDUSD": 107, "USDKRW": 105,
}
PRECISION_MAP = {
    "BTC": 100000, "ETH": 10000, "SOL": 1000, "AVAX": 100, "SUI": 10,
    "EURUSD": 10, "GBPUSD": 10, "USDJPY": 1000, "USDCHF": 10, "USDCAD": 10,
    "AUDUSD": 10, "NZDUSD": 10, "USDKRW": 10,
}
PRICE_DECIMALS_MAP = {
    "BTC": 1, "ETH": 2, "SOL": 3, "AVAX": 3, "SUI": 5,
    "EURUSD": 5, "GBPUSD": 5, "USDJPY": 3, "USDCHF": 5, "USDCAD": 5,
    "AUDUSD": 5, "NZDUSD": 5, "USDKRW": 5,
}
MIN_BASE_AMOUNT_MAP = {
    "BTC": 0.00020, "ETH": 0.005, "SOL": 0.05, "AVAX": 0.5, "SUI": 3.0,
    "EURUSD": 10.0, "GBPUSD": 10.0, "USDJPY": 0.05, "USDCHF": 8.0, "USDCAD": 10.0,
    "AUDUSD": 10.0, "NZDUSD": 10.0, "USDKRW": 10.0,
}


def get_precision(symbol):
    return PRECISION_MAP.get(symbol, 10000)


def get_price_decimals(symbol):
    return PRICE_DECIMALS_MAP.get(symbol, 2)


def get_min_base_amount(symbol):
    return MIN_BASE_AMOUNT_MAP.get(symbol, 0.001)


PORT = int(os.getenv("PORT", "10000"))

# ========== WELCHE COINS LAUFEN SOLLEN ==========
# Komma-getrennt, z.B. GRID_SYMBOLS="BTC,SOL,ETH". Default: nur BTC (abwaertskompatibel).
SYMBOLS = [s.strip().upper() for s in os.getenv("GRID_SYMBOLS", os.getenv("GRID_SYMBOL", "BTC")).split(",") if s.strip()]
for _s in SYMBOLS:
    if _s not in MARKET_INDICES:
        raise ValueError(f"Symbol {_s} nicht in MARKET_INDICES - hier ergänzen")

MARKET_INDEX_TO_SYMBOL = {MARKET_INDICES[s]: s for s in SYMBOLS}


def default_config():
    return {
        "dry_run": os.getenv("DRY_RUN", "true").lower() == "true",
        "margin": float(os.getenv("GRID_MARGIN", "20")),
        "leverage": int(os.getenv("GRID_LEVERAGE", "3")),
        "grid_step_pct": float(os.getenv("GRID_STEP_PCT", "0.25")),
        "tp_step_pct": float(os.getenv("TP_STEP_PCT", "0.25")),
        "max_nachkauf": int(os.getenv("MAX_NACHKAUF", "5")),
        "bot_active": True,
        "auto_reverse": os.getenv("AUTO_REVERSE", "true").lower() == "true",
    }


def default_state():
    return {
        "position": None, "avg_entry_price": None, "total_coin_size": 0.0,
        "entry_count": 0, "anchor_price": None, "last_price": None,
        "price_history": [],
        "stats": {"trades": 0, "wins": 0, "losses": 0, "total_pnl_usd": 0.0},
        "trade_log": [],
    }


# ========== GLOBALER STATE - EIN EINTRAG PRO COIN ==========
BOTS = {s: {"config": default_config(), "state": default_state()} for s in SYMBOLS}


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


async def place_market_order(client, market_index, symbol, is_ask, base_amount, reference_price, reduce_only=False):
    price_decimals = get_price_decimals(symbol)
    adjusted_price = reference_price * 0.98 if is_ask else reference_price * 1.02
    price_scaled = int(adjusted_price * (10 ** price_decimals))
    tx, tx_hash, err = await client.create_order(
        market_index=market_index, client_order_index=int(time.time() * 1000),
        base_amount=base_amount, price=price_scaled, is_ask=is_ask,
        order_type=client.ORDER_TYPE_MARKET,
        time_in_force=client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL, reduce_only=reduce_only,
        order_expiry=client.DEFAULT_IOC_EXPIRY,
    )
    return tx, tx_hash, err


def estimate_liquidation_price(symbol):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    if st["position"] is None or st["avg_entry_price"] is None or cfg["leverage"] <= 0:
        return None
    factor = 1 / cfg["leverage"]
    if st["position"] == "long":
        return round(st["avg_entry_price"] * (1 - factor), 2)
    else:
        return round(st["avg_entry_price"] * (1 + factor), 2)


def calc_unrealized_pnl(symbol):
    st = BOTS[symbol]["state"]
    if st["position"] is None or st["avg_entry_price"] is None or st["last_price"] is None:
        return 0.0
    if st["position"] == "long":
        return round((st["last_price"] - st["avg_entry_price"]) * st["total_coin_size"], 4)
    else:
        return round((st["avg_entry_price"] - st["last_price"]) * st["total_coin_size"], 4)


def calc_grid_levels(symbol):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    levels = {"anchor": st["anchor_price"], "tp_price": None, "next_nachkauf_price": None,
              "grid_step_abs": None, "tp_step_abs": None}
    if st["position"] is None:
        if st["anchor_price"] is not None:
            step = st["anchor_price"] * (cfg["grid_step_pct"] / 100)
            levels["next_entry_long"] = round(st["anchor_price"] - step, 4)
            levels["next_entry_short"] = round(st["anchor_price"] + step, 4)
            levels["grid_step_abs"] = round(step, 4)
    elif st["avg_entry_price"] is not None:
        tp_step = st["avg_entry_price"] * (cfg["tp_step_pct"] / 100)
        grid_step = st["avg_entry_price"] * (cfg["grid_step_pct"] / 100)
        levels["tp_step_abs"] = round(tp_step, 4)
        levels["grid_step_abs"] = round(grid_step, 4)
        if st["position"] == "long":
            levels["tp_price"] = round(st["avg_entry_price"] + tp_step, 4)
            levels["next_nachkauf_price"] = round(st["avg_entry_price"] - grid_step, 4)
        else:
            levels["tp_price"] = round(st["avg_entry_price"] - tp_step, 4)
            levels["next_nachkauf_price"] = round(st["avg_entry_price"] + grid_step, 4)
    return levels


async def execute_entry(symbol, direction, price, is_add_on):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    market_index = MARKET_INDICES[symbol]

    position_usdc = cfg["margin"] * cfg["leverage"]
    raw_units = position_usdc / price
    precision = get_precision(symbol)
    base_amount = int(raw_units * precision)
    new_units = base_amount / precision

    if not cfg["dry_run"]:
        client = get_lighter_client()
        if client is None:
            debug_log(f"⚠️ [{symbol}] Kein Lighter-Client - Order übersprungen")
            return False
        min_base = get_min_base_amount(symbol)
        if base_amount * (1 / precision) < min_base:
            debug_log(f"⚠️ [{symbol}] Order-Größe unter Mindestgröße")
            return False
        is_ask = direction == "short"
        tx, tx_hash, err = await place_market_order(client, market_index, symbol, is_ask, base_amount, price, reduce_only=False)
        await client.close()
        if err:
            debug_log(f"⚠️ [{symbol}] Entry-Order fehlgeschlagen", {"error": str(err)})
            return False
        debug_log(f"✅ [{symbol}] ECHTE Order ausgeführt: {direction.upper()} @ ~{price}", {"tx_hash": str(tx_hash)})

    if is_add_on:
        total_value = st["avg_entry_price"] * st["total_coin_size"] + price * new_units
        st["total_coin_size"] += new_units
        st["avg_entry_price"] = total_value / st["total_coin_size"]
    else:
        st["avg_entry_price"] = price
        st["total_coin_size"] = new_units
        st["position"] = direction

    st["entry_count"] += 1
    debug_log(f"📈 [{symbol}] {'Nachkauf' if is_add_on else 'Neue Position'}: {direction.upper()} @ {price} | Ø-Einstieg {round(st['avg_entry_price'], 2)} | Stufe {st['entry_count']}")
    return True


async def execute_exit(symbol, price, reason):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    market_index = MARKET_INDICES[symbol]

    pnl_usd = (price - st["avg_entry_price"]) * st["total_coin_size"] if st["position"] == "long" else (st["avg_entry_price"] - price) * st["total_coin_size"]
    closing_side = st["position"]

    if not cfg["dry_run"]:
        client = get_lighter_client()
        if client is None:
            debug_log(f"⚠️ [{symbol}] Kein Lighter-Client - Exit übersprungen (Position bleibt offen!)")
            return
        precision = get_precision(symbol)
        base_amount = int(round(st["total_coin_size"] * precision))
        is_ask = st["position"] == "long"
        tx, tx_hash, err = await place_market_order(client, market_index, symbol, is_ask, base_amount, price, reduce_only=True)
        await client.close()
        if err:
            debug_log(f"⚠️ [{symbol}] Exit-Order fehlgeschlagen - Position bleibt offen!", {"error": str(err)})
            return

    stats = st["stats"]
    stats["trades"] += 1
    stats["total_pnl_usd"] += pnl_usd
    stats["wins" if pnl_usd > 0 else "losses"] += 1
    st["trade_log"].append({
        "side": st["position"], "avg_entry": round(st["avg_entry_price"], 2), "exit": price,
        "entries": st["entry_count"], "pnl_usd": round(pnl_usd, 3), "closed_at": datetime.now().isoformat(),
    })

    debug_log(f"🏁 [{symbol}] Position geschlossen ({reason}): {st['position'].upper()} Ø{round(st['avg_entry_price'],2)} -> {price} | PnL ${round(pnl_usd,3)}")

    st["position"] = None
    st["avg_entry_price"] = None
    st["total_coin_size"] = 0.0
    st["entry_count"] = 0
    st["anchor_price"] = price

    if cfg.get("auto_reverse", True) and cfg["bot_active"]:
        opposite = "short" if closing_side == "long" else "long"
        await execute_entry(symbol, opposite, price, is_add_on=False)


async def on_price_update(symbol, price):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    st["last_price"] = price

    st["price_history"].append({"ts": int(time.time() * 1000), "price": price})
    if len(st["price_history"]) > 200:
        st["price_history"].pop(0)

    if st["anchor_price"] is None:
        st["anchor_price"] = price
        return

    bot_active = cfg["bot_active"]

    if st["position"] is None:
        if not bot_active:
            return
        grid_step_abs = st["anchor_price"] * (cfg["grid_step_pct"] / 100)
        if price <= st["anchor_price"] - grid_step_abs:
            await execute_entry(symbol, "long", price, is_add_on=False)
        elif price >= st["anchor_price"] + grid_step_abs:
            await execute_entry(symbol, "short", price, is_add_on=False)
        return

    tp_step_abs = st["avg_entry_price"] * (cfg["tp_step_pct"] / 100)
    grid_step_abs = st["avg_entry_price"] * (cfg["grid_step_pct"] / 100)
    max_nachkauf = cfg["max_nachkauf"]

    if st["position"] == "long":
        if price >= st["avg_entry_price"] + tp_step_abs:
            await execute_exit(symbol, price, "TP")
        elif bot_active and price <= st["avg_entry_price"] - grid_step_abs and (max_nachkauf == 0 or st["entry_count"] < max_nachkauf):
            await execute_entry(symbol, "long", price, is_add_on=True)
    elif st["position"] == "short":
        if price <= st["avg_entry_price"] - tp_step_abs:
            await execute_exit(symbol, price, "TP")
        elif bot_active and price >= st["avg_entry_price"] + grid_step_abs and (max_nachkauf == 0 or st["entry_count"] < max_nachkauf):
            await execute_entry(symbol, "short", price, is_add_on=True)


async def trading_loop():
    last_status_log = 0.0

    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20) as ws:
                for s in SYMBOLS:
                    await ws.send(json.dumps({"type": "subscribe", "channel": f"trade/{MARKET_INDICES[s]}"}))
                debug_log(f"✅ Verbunden für {', '.join(SYMBOLS)}")

                async for raw in ws:
                    msg = json.loads(raw)
                    channel = msg.get("channel", "")
                    if channel.startswith("trade"):
                        try:
                            market_index = int(channel.split(":")[1].split("/")[0]) if ":" in channel else int(channel.split("/")[1])
                        except Exception:
                            market_index = None
                        symbol = MARKET_INDEX_TO_SYMBOL.get(market_index)
                        if symbol is None:
                            continue
                        trades = msg.get("trades", [])
                        if trades:
                            price = float(trades[-1]["price"])
                            await on_price_update(symbol, price)

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
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8"><title>Grid-Bot Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
  body { font-family: -apple-system, sans-serif; background:#0f1117; color:#e5e7eb; margin:0; padding:20px; }
  h1 { font-size: 20px; display:flex; align-items:center; gap:12px; flex-wrap:wrap; }
  h2 { font-size: 15px; color:#9ca3af; margin-top: 24px; }
  select#symbol-select { font-size:16px; padding:6px 12px; background:#1a1d29; color:#e5e7eb; border:1px solid #2a2e3f; border-radius:8px; }
  .grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(180px,1fr)); gap:12px; margin-bottom:16px; }
  .card { background:#1a1d29; border-radius:10px; padding:12px; }
  .card .label { font-size:11px; color:#9ca3af; text-transform:uppercase; }
  .card .value { font-size:20px; font-weight:600; margin-top:4px; }
  .green { color:#4ade80; } .red { color:#f87171; } .yellow { color:#fbbf24; }
  .badge { display:inline-block; padding:2px 10px; border-radius:12px; font-size:12px; font-weight:600; }
  .badge.dry { background:#3730a3; color:#c7d2fe; } .badge.live { background:#7f1d1d; color:#fecaca; }
  .badge.active { background:#14532d; color:#bbf7d0; } .badge.paused { background:#78350f; color:#fde68a; }
  form { background:#1a1d29; border-radius:10px; padding:16px; display:grid; grid-template-columns: repeat(auto-fit, minmax(160px,1fr)); gap:12px; align-items:end; }
  label { display:block; font-size:12px; color:#9ca3af; margin-bottom:4px; }
  input, select.cfg { width:100%; padding:6px 8px; background:#0f1117; border:1px solid #2a2e3f; border-radius:6px; color:#e5e7eb; box-sizing:border-box; }
  button { padding:8px 16px; background:#4f46e5; color:white; border:none; border-radius:6px; cursor:pointer; font-weight:600; }
  button:hover { background:#4338ca; }
  button.stop { background:#b91c1c; } button.stop:hover { background:#991b1b; }
  button.start { background:#15803d; } button.start:hover { background:#166534; }
  table { width:100%; border-collapse:collapse; font-size:13px; margin-top:10px; }
  th, td { text-align:left; padding:6px 8px; border-bottom:1px solid #2a2e3f; }
  th { color:#9ca3af; font-weight:500; }
  .warn { background:#7f1d1d; color:#fecaca; padding:8px 12px; border-radius:8px; font-size:13px; margin-top:10px; display:none; }
  canvas { background:#1a1d29; border-radius:10px; padding:10px; margin-top:10px; }
  .coin-overview { display:flex; gap:8px; flex-wrap:wrap; margin-bottom:16px; }
  .coin-pill { background:#1a1d29; border:1px solid #2a2e3f; border-radius:20px; padding:4px 14px; font-size:13px; cursor:pointer; }
  .coin-pill.selected { border-color:#4f46e5; background:#1e1b4b; }
</style>
</head>
<body>
<h1>📡 Grid-Bot <select id="symbol-select"></select><span id="mode-badge"></span><span id="active-badge"></span></h1>

<div class="coin-overview" id="coin-overview"></div>

<div style="margin-bottom:16px;">
  <button id="btn-start" class="start">▶️ Start</button>
  <button id="btn-stop" class="stop">⏸️ Stop</button>
</div>

<div class="grid" id="status-grid"></div>

<canvas id="priceChart" height="90"></canvas>

<h2>Einstellungen ändern (nur für den ausgewählten Coin)</h2>
<form id="config-form">
  <div><label>Margin (USDC)</label><input type="number" step="1" id="margin"></div>
  <div><label>Hebel</label><input type="number" step="1" id="leverage"></div>
  <div><label>Grid-Stufe (%)</label><input type="number" step="0.01" id="grid_step_pct"></div>
  <div><label>TP-Stufe (%)</label><input type="number" step="0.01" id="tp_step_pct"></div>
  <div><label>Max. Nachkauf</label><input type="number" step="1" id="max_nachkauf"></div>
  <div><label>Nach TP sofort drehen</label>
    <select class="cfg" id="auto_reverse">
      <option value="true">Ja - sofort Gegenposition</option>
      <option value="false">Nein - warten auf neues Gitter-Signal</option>
    </select>
  </div>
  <div><label>Modus</label>
    <select class="cfg" id="dry_run">
      <option value="true">DRY RUN (Simulation)</option>
      <option value="false">LIVE (echte Orders!)</option>
    </select>
  </div>
  <button type="submit">Speichern</button>
</form>
<div class="warn" id="live-warn">⚠️ LIVE-Modus aktiv - echte Orders werden platziert!</div>
<div style="font-size:12px; color:#9ca3af; margin-top:8px;" id="abs-distances"></div>

<h2>Letzte abgeschlossene Trades</h2>
<table id="trades-table"><thead><tr><th>Seite</th><th>Ø-Einstieg</th><th>Exit</th><th>Stufen</th><th>PnL $</th></tr></thead><tbody></tbody></table>

<script>
let priceChart;
let currentSymbol = null;
let allSymbols = [];

async function loadSymbols() {
  const res = await fetch('/api/symbols');
  const data = await res.json();
  allSymbols = data.symbols;
  const sel = document.getElementById('symbol-select');
  sel.innerHTML = allSymbols.map(s => `<option value="${s}">${s}</option>`).join('');
  currentSymbol = allSymbols[0];
  sel.value = currentSymbol;
  sel.addEventListener('change', () => { currentSymbol = sel.value; window.formTouched = false; refresh(); });
}

document.getElementById('btn-start').addEventListener('click', async () => {
  await fetch(`/api/control?symbol=${currentSymbol}`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({bot_active:true}) });
});
document.getElementById('btn-stop').addEventListener('click', async () => {
  await fetch(`/api/control?symbol=${currentSymbol}`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({bot_active:false}) });
});

async function refresh() {
  if (!currentSymbol) return;
  const res = await fetch(`/api/status?symbol=${currentSymbol}`);
  const data = await res.json();

  // Uebersichts-Pills fuer alle Coins
  const overviewRes = await fetch('/api/overview');
  const overview = await overviewRes.json();
  document.getElementById('coin-overview').innerHTML = Object.entries(overview).map(([sym, o]) => `
    <div class="coin-pill ${sym===currentSymbol?'selected':''}" onclick="document.getElementById('symbol-select').value='${sym}'; document.getElementById('symbol-select').dispatchEvent(new Event('change'));">
      ${sym}: ${o.position || 'flach'} | PnL $${o.total_pnl_usd}
    </div>
  `).join('');

  document.getElementById('mode-badge').innerHTML =
    data.config.dry_run ? '<span class="badge dry">DRY RUN</span>' : '<span class="badge live">LIVE</span>';
  document.getElementById('active-badge').innerHTML =
    data.config.bot_active ? '<span class="badge active">AKTIV</span>' : '<span class="badge paused">GESTOPPT</span>';
  document.getElementById('live-warn').style.display = data.config.dry_run ? 'none' : 'block';

  const gl = data.grid_levels || {};
  document.getElementById('status-grid').innerHTML = `
    <div class="card"><div class="label">Symbol</div><div class="value">${data.symbol}</div></div>
    <div class="card"><div class="label">Preis</div><div class="value">${data.last_price ?? '-'}</div></div>
    <div class="card"><div class="label">Position</div><div class="value ${data.position==='long'?'green':data.position==='short'?'red':'yellow'}">${data.position || 'flach'}</div></div>
    <div class="card"><div class="label">Ø-Einstieg</div><div class="value">${data.avg_entry_price ?? '-'}</div></div>
    <div class="card"><div class="label">Unrealisiert $</div><div class="value ${data.unrealized_pnl_usd>=0?'green':'red'}">${data.unrealized_pnl_usd}</div></div>
    <div class="card"><div class="label">Nachkauf-Stufe</div><div class="value">${data.entry_count} / ${data.config.max_nachkauf || '∞'}</div></div>
    <div class="card"><div class="label">Geschätzter Liq.-Preis</div><div class="value red">${data.liquidation_price ?? '-'}</div></div>
    <div class="card"><div class="label">Realisiert (gesamt) $</div><div class="value ${data.stats.total_pnl_usd>=0?'green':'red'}">${data.stats.total_pnl_usd}</div></div>
    <div class="card"><div class="label">Trades / Trefferquote</div><div class="value">${data.stats.trades} / ${data.stats.win_rate_pct}%</div></div>
  `;

  if (!window.formTouched) {
    document.getElementById('margin').value = data.config.margin;
    document.getElementById('leverage').value = data.config.leverage;
    document.getElementById('grid_step_pct').value = data.config.grid_step_pct;
    document.getElementById('tp_step_pct').value = data.config.tp_step_pct;
    document.getElementById('max_nachkauf').value = data.config.max_nachkauf;
    document.getElementById('dry_run').value = String(data.config.dry_run);
    document.getElementById('auto_reverse').value = String(data.config.auto_reverse);
  }

  document.getElementById('abs-distances').innerText =
    `Aktuelle Abstände in $: Grid-Stufe ≈ ${gl.grid_step_abs ?? '-'} | TP-Stufe ≈ ${gl.tp_step_abs ?? '-'}`;

  const hist = data.price_history || [];
  const labels = hist.map(p => new Date(p.ts).toLocaleTimeString());
  const prices = hist.map(p => p.price);
  const n = labels.length;

  const datasets = [{ label: 'Preis', data: prices, borderColor:'#60a5fa', pointRadius:0, borderWidth:2 }];
  if (gl.anchor) datasets.push({ label:'Anker', data: Array(n).fill(gl.anchor), borderColor:'#9ca3af', borderDash:[4,4], pointRadius:0, borderWidth:1 });
  if (gl.tp_price) datasets.push({ label:'TP', data: Array(n).fill(gl.tp_price), borderColor:'#4ade80', borderDash:[6,3], pointRadius:0, borderWidth:1 });
  if (gl.next_nachkauf_price) datasets.push({ label:'Nächster Nachkauf', data: Array(n).fill(gl.next_nachkauf_price), borderColor:'#f87171', borderDash:[6,3], pointRadius:0, borderWidth:1 });
  if (gl.next_entry_long) datasets.push({ label:'Entry Long ab', data: Array(n).fill(gl.next_entry_long), borderColor:'#4ade80', borderDash:[2,2], pointRadius:0, borderWidth:1 });
  if (gl.next_entry_short) datasets.push({ label:'Entry Short ab', data: Array(n).fill(gl.next_entry_short), borderColor:'#f87171', borderDash:[2,2], pointRadius:0, borderWidth:1 });

  if (priceChart) priceChart.destroy();
  priceChart = new Chart(document.getElementById('priceChart'), {
    type: 'line',
    data: { labels, datasets },
    options: { responsive:true, animation:false, scales:{ x:{ display:false }, y:{ ticks:{color:'#9ca3af'} } }, plugins:{legend:{labels:{color:'#e5e7eb'}}} }
  });

  const trades = (data.trade_log || []).slice(-15).reverse();
  document.querySelector('#trades-table tbody').innerHTML = trades.map(t => `
    <tr><td>${t.side}</td><td>${t.avg_entry}</td><td>${t.exit}</td><td>${t.entries}</td>
    <td class="${t.pnl_usd>=0?'green':'red'}">${t.pnl_usd}</td></tr>
  `).join('');
}

document.getElementById('config-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const payload = {
    margin: parseFloat(document.getElementById('margin').value),
    leverage: parseInt(document.getElementById('leverage').value),
    grid_step_pct: parseFloat(document.getElementById('grid_step_pct').value),
    tp_step_pct: parseFloat(document.getElementById('tp_step_pct').value),
    max_nachkauf: parseInt(document.getElementById('max_nachkauf').value),
    dry_run: document.getElementById('dry_run').value === 'true',
    auto_reverse: document.getElementById('auto_reverse').value === 'true',
  };
  await fetch(`/api/config?symbol=${currentSymbol}`, { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(payload) });
  window.formTouched = false;
  alert(`Gespeichert für ${currentSymbol}!`);
});

['margin','leverage','grid_step_pct','tp_step_pct','max_nachkauf','dry_run','auto_reverse'].forEach(id => {
  document.getElementById(id).addEventListener('input', () => { window.formTouched = true; });
});

(async () => {
  await loadSymbols();
  refresh();
  setInterval(refresh, 3000);
})();
</script>
</body>
</html>
"""


async def handle_index(request):
    return web.Response(text=DASHBOARD_HTML, content_type="text/html")


async def handle_symbols(request):
    return web.json_response({"symbols": SYMBOLS})


async def handle_overview(request):
    result = {}
    for s in SYMBOLS:
        st = BOTS[s]["state"]
        result[s] = {"position": st["position"], "total_pnl_usd": round(st["stats"]["total_pnl_usd"], 3)}
    return web.json_response(result)


async def handle_status(request):
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    b = BOTS[symbol]
    st, cfg, stats = b["state"], b["config"], b["state"]["stats"]
    win_rate = round(stats["wins"] / stats["trades"] * 100, 1) if stats["trades"] else 0
    payload = {
        "symbol": symbol, "last_price": st["last_price"], "anchor_price": st["anchor_price"],
        "position": st["position"], "avg_entry_price": round(st["avg_entry_price"], 2) if st["avg_entry_price"] else None,
        "entry_count": st["entry_count"], "liquidation_price": estimate_liquidation_price(symbol),
        "unrealized_pnl_usd": calc_unrealized_pnl(symbol),
        "grid_levels": calc_grid_levels(symbol),
        "config": cfg,
        "stats": {"trades": stats["trades"], "win_rate_pct": win_rate, "total_pnl_usd": round(stats["total_pnl_usd"], 3)},
        "trade_log": st["trade_log"][-20:],
        "price_history": st["price_history"][-200:],
    }
    return web.json_response(payload)


async def handle_config_update(request):
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    body = await request.json()
    cfg = BOTS[symbol]["config"]
    for key in ["margin", "leverage", "grid_step_pct", "tp_step_pct", "max_nachkauf", "dry_run", "auto_reverse"]:
        if key in body:
            cfg[key] = body[key]
    debug_log(f"⚙️ [{symbol}] Konfiguration aktualisiert", cfg)
    return web.json_response({"success": True, "config": cfg})


async def handle_control(request):
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    body = await request.json()
    cfg = BOTS[symbol]["config"]
    if "bot_active" in body:
        cfg["bot_active"] = bool(body["bot_active"])
        debug_log(f"{'▶️' if cfg['bot_active'] else '⏸️'} [{symbol}] Bot {'gestartet' if cfg['bot_active'] else 'gestoppt'}")
    return web.json_response({"success": True, "bot_active": cfg["bot_active"]})


async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/symbols", handle_symbols)
    app.router.add_get("/api/overview", handle_overview)
    app.router.add_get("/api/status", handle_status)
    app.router.add_post("/api/config", handle_config_update)
    app.router.add_post("/api/control", handle_control)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    debug_log(f"🌐 Dashboard läuft auf Port {PORT}")


async def main():
    print("=" * 60)
    print(f"🚀 Multi-Coin Grid-Bot - Dashboard auf Port {PORT}")
    print(f"   Coins: {', '.join(SYMBOLS)}")
    for s in SYMBOLS:
        cfg = BOTS[s]["config"]
        print(f"   [{s}] DRY_RUN={cfg['dry_run']} Margin={cfg['margin']} Hebel={cfg['leverage']}x Grid={cfg['grid_step_pct']}% TP={cfg['tp_step_pct']}%")
    print("=" * 60)

    await start_web_server()
    await trading_loop()


if __name__ == "__main__":
    asyncio.run(main())
