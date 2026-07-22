"""
Einfacher neutraler Grid-Bot für Lighter (zkLighter) - MIT LIVE-DASHBOARD
=============================================================================
Keine Richtungsvorhersage - reine Preis-Gitter-Logik mit prozentualer Stufe
(bleibt konsistent, egal auf welchem Preisniveau der Coin steht).

Da Lighter nur EINE Netto-Position pro Markt erlaubt (long ODER short, nicht
beides gleichzeitig):
1. Start: aktueller Preis = "Anker"
2. Preis bewegt sich GRID_STEP_PCT vom Anker weg -> Position eroeffnen
3. Position im Plus um TP_STEP_PCT (ab Ø-Einstieg) -> TP, neuer Anker
4. Position im Minus um GRID_STEP_PCT (weitere Stufe) -> Nachkauf, bis MAX_NACHKAUF

WICHTIG - RENDER SERVICE-TYP:
Dieses Skript startet einen eingebauten Webserver (aiohttp) fuers Dashboard.
Der Render-Service muss als "Web Service" laufen (nicht "Background Worker"),
damit du eine URL bekommst. Render setzt automatisch $PORT.

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

# ========== LIVE-KONFIGURIERBARE EINSTELLUNGEN (per Dashboard aenderbar) ==========
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

stats = {"trades": 0, "wins": 0, "losses": 0, "total_pnl_usd": 0.0}
trade_log = []


def estimate_liquidation_price():
    """Grobe Naeherung, ignoriert Maintenance-Margin-Details und Cross-Margin-Gesamtkonto."""
    if position is None or avg_entry_price is None or CONFIG["leverage"] <= 0:
        return None
    factor = 1 / CONFIG["leverage"]
    if position == "long":
        return round(avg_entry_price * (1 - factor), 2)
    else:
        return round(avg_entry_price * (1 + factor), 2)


async def execute_entry(direction, price, is_add_on):
    global position, avg_entry_price, total_coin_size, entry_count

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

    entry_count += 1
    debug_log(f"📈 {'Nachkauf' if is_add_on else 'Neue Position'}: {direction.upper()} {SYMBOL} @ {price} | Ø-Einstieg {round(avg_entry_price, 2)} | Stufe {entry_count}")
    return True


async def execute_exit(price, reason):
    global position, avg_entry_price, total_coin_size, entry_count, anchor_price

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
    trade_log.append({
        "side": position, "avg_entry": round(avg_entry_price, 2), "exit": price,
        "entries": entry_count, "pnl_usd": round(pnl_usd, 3), "closed_at": datetime.now().isoformat(),
    })

    debug_log(f"🏁 Position geschlossen ({reason}): {position.upper()} Ø{round(avg_entry_price,2)} -> {price} | PnL ${round(pnl_usd,3)}")

    position = None
    avg_entry_price = None
    total_coin_size = 0.0
    entry_count = 0
    anchor_price = price


async def on_price_update(price):
    global position, anchor_price, last_price
    last_price = price

    if price is None:
        return

    if anchor_price is None:
        anchor_price = price
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

    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20) as ws:
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
            debug_log("⚠️ Verbindung verloren, reconnect in 5s", {"error": str(e)})
            await asyncio.sleep(5)


# ========== WEB-DASHBOARD ==========
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8"><title>Grid-Bot Dashboard</title>
<style>
  body { font-family: -apple-system, sans-serif; background:#0f1117; color:#e5e7eb; margin:0; padding:20px; }
  h1 { font-size: 20px; } h2 { font-size: 15px; color:#9ca3af; margin-top: 24px; }
  .grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(180px,1fr)); gap:12px; margin-bottom:16px; }
  .card { background:#1a1d29; border-radius:10px; padding:12px; }
  .card .label { font-size:11px; color:#9ca3af; text-transform:uppercase; }
  .card .value { font-size:20px; font-weight:600; margin-top:4px; }
  .green { color:#4ade80; } .red { color:#f87171; } .yellow { color:#fbbf24; }
  .badge { display:inline-block; padding:2px 10px; border-radius:12px; font-size:12px; font-weight:600; }
  .badge.dry { background:#3730a3; color:#c7d2fe; } .badge.live { background:#7f1d1d; color:#fecaca; }
  form { background:#1a1d29; border-radius:10px; padding:16px; display:grid; grid-template-columns: repeat(auto-fit, minmax(160px,1fr)); gap:12px; align-items:end; }
  label { display:block; font-size:12px; color:#9ca3af; margin-bottom:4px; }
  input, select { width:100%; padding:6px 8px; background:#0f1117; border:1px solid #2a2e3f; border-radius:6px; color:#e5e7eb; box-sizing:border-box; }
  button { grid-column: span 1; padding:8px 16px; background:#4f46e5; color:white; border:none; border-radius:6px; cursor:pointer; font-weight:600; }
  button:hover { background:#4338ca; }
  table { width:100%; border-collapse:collapse; font-size:13px; margin-top:10px; }
  th, td { text-align:left; padding:6px 8px; border-bottom:1px solid #2a2e3f; }
  th { color:#9ca3af; font-weight:500; }
  .warn { background:#7f1d1d; color:#fecaca; padding:8px 12px; border-radius:8px; font-size:13px; margin-top:10px; display:none; }
</style>
</head>
<body>
<h1>📡 Grid-Bot <span id="mode-badge"></span></h1>
<div class="grid" id="status-grid"></div>

<h2>Einstellungen ändern</h2>
<form id="config-form">
  <div><label>Margin (USDC)</label><input type="number" step="1" id="margin"></div>
  <div><label>Hebel</label><input type="number" step="1" id="leverage"></div>
  <div><label>Grid-Stufe (%)</label><input type="number" step="0.01" id="grid_step_pct"></div>
  <div><label>TP-Stufe (%)</label><input type="number" step="0.01" id="tp_step_pct"></div>
  <div><label>Max. Nachkauf</label><input type="number" step="1" id="max_nachkauf"></div>
  <div><label>Modus</label>
    <select id="dry_run">
      <option value="true">DRY RUN (Simulation)</option>
      <option value="false">LIVE (echte Orders!)</option>
    </select>
  </div>
  <button type="submit">Speichern</button>
</form>
<div class="warn" id="live-warn">⚠️ LIVE-Modus aktiv - echte Orders werden platziert!</div>

<h2>Letzte abgeschlossene Trades</h2>
<table id="trades-table"><thead><tr><th>Seite</th><th>Ø-Einstieg</th><th>Exit</th><th>Stufen</th><th>PnL $</th></tr></thead><tbody></tbody></table>

<script>
async function refresh() {
  const res = await fetch('/api/status');
  const data = await res.json();

  document.getElementById('mode-badge').innerHTML =
    data.config.dry_run ? '<span class="badge dry">DRY RUN</span>' : '<span class="badge live">LIVE</span>';
  document.getElementById('live-warn').style.display = data.config.dry_run ? 'none' : 'block';

  document.getElementById('status-grid').innerHTML = `
    <div class="card"><div class="label">Symbol</div><div class="value">${data.symbol}</div></div>
    <div class="card"><div class="label">Preis</div><div class="value">${data.last_price ?? '-'}</div></div>
    <div class="card"><div class="label">Anker</div><div class="value">${data.anchor_price ?? '-'}</div></div>
    <div class="card"><div class="label">Position</div><div class="value ${data.position==='long'?'green':data.position==='short'?'red':'yellow'}">${data.position || 'flach'}</div></div>
    <div class="card"><div class="label">Ø-Einstieg</div><div class="value">${data.avg_entry_price ?? '-'}</div></div>
    <div class="card"><div class="label">Nachkauf-Stufe</div><div class="value">${data.entry_count} / ${data.config.max_nachkauf || '∞'}</div></div>
    <div class="card"><div class="label">Geschätzter Liq.-Preis</div><div class="value red">${data.liquidation_price ?? '-'}</div></div>
    <div class="card"><div class="label">Trades gesamt</div><div class="value">${data.stats.trades}</div></div>
    <div class="card"><div class="label">Trefferquote</div><div class="value">${data.stats.win_rate_pct}%</div></div>
    <div class="card"><div class="label">Gesamt-PnL $</div><div class="value ${data.stats.total_pnl_usd>=0?'green':'red'}">${data.stats.total_pnl_usd}</div></div>
  `;

  if (!window.formTouched) {
    document.getElementById('margin').value = data.config.margin;
    document.getElementById('leverage').value = data.config.leverage;
    document.getElementById('grid_step_pct').value = data.config.grid_step_pct;
    document.getElementById('tp_step_pct').value = data.config.tp_step_pct;
    document.getElementById('max_nachkauf').value = data.config.max_nachkauf;
    document.getElementById('dry_run').value = String(data.config.dry_run);
  }

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
  };
  await fetch('/api/config', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(payload) });
  window.formTouched = false;
  alert('Gespeichert!');
});

['margin','leverage','grid_step_pct','tp_step_pct','max_nachkauf','dry_run'].forEach(id => {
  document.getElementById(id).addEventListener('input', () => { window.formTouched = true; });
});

refresh();
setInterval(refresh, 3000);
</script>
</body>
</html>
"""


async def handle_index(request):
    return web.Response(text=DASHBOARD_HTML, content_type="text/html")


async def handle_status(request):
    win_rate = round(stats["wins"] / stats["trades"] * 100, 1) if stats["trades"] else 0
    payload = {
        "symbol": SYMBOL, "last_price": last_price, "anchor_price": anchor_price,
        "position": position, "avg_entry_price": round(avg_entry_price, 2) if avg_entry_price else None,
        "entry_count": entry_count, "liquidation_price": estimate_liquidation_price(),
        "config": CONFIG,
        "stats": {"trades": stats["trades"], "win_rate_pct": win_rate, "total_pnl_usd": round(stats["total_pnl_usd"], 3)},
        "trade_log": trade_log[-20:],
    }
    return web.json_response(payload)


async def handle_config_update(request):
    body = await request.json()
    for key in ["margin", "leverage", "grid_step_pct", "tp_step_pct", "max_nachkauf", "dry_run"]:
        if key in body:
            CONFIG[key] = body[key]
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
