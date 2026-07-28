"""
copytrade.py - Copy-Trading: Hyperliquid Top-Trader beobachten und optional
auf Lighter nachbilden. Nutzt gemeinsame Infrastruktur aus bot_core.py.
"""

import asyncio
import aiohttp
import json
import time
import os
import traceback
from datetime import datetime
from aiohttp import web

from bot_core import (
    debug_log, get_lighter_client, place_market_order, get_precision,
    get_price_decimals, get_min_base_amount, MARKET_INDICES, get_redis,
)

HL_INFO_URL = "https://api.hyperliquid.xyz/info"
HL_LEADERBOARD_URL = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"

CT_CONFIG = {
    "dry_run": os.getenv("CT_DRY_RUN", os.getenv("DRY_RUN", "true")).lower() == "true",
    "copy_margin": float(os.getenv("COPY_MARGIN", "10")),
    "copy_leverage": int(os.getenv("COPY_LEVERAGE", "3")),
    "leaderboard_top_n": int(os.getenv("LEADERBOARD_TOP_N", "100")),
    "leaderboard_refresh_minutes": int(os.getenv("LEADERBOARD_REFRESH_MINUTES", "30")),
    "poll_interval_seconds": int(os.getenv("CT_POLL_INTERVAL_SECONDS", "15")),
}

CT_MANUAL_ADDRESSES = [a.strip() for a in os.getenv("MANUAL_WALLET_ADDRESSES", "").split(",") if a.strip()]

CT_STATE = {
    "leaderboard": [],
    "leaderboard_last_fetch": None,
    "leaderboard_error": None,
    "watched": {},
}



async def execute_copy_trade(symbol, direction, reference_price, margin, leverage):
    """Kopiert die RICHTUNG eines Trades mit der fuer diesen Trader/Coin eingestellten Margin/Hebel."""
    if symbol not in MARKET_INDICES:
        debug_log(f"⚠️ [CopyTrading] Coin {symbol} nicht auf Lighter gemappt - übersprungen")
        return

    if CT_CONFIG["dry_run"]:
        debug_log(f"🧪 [CopyTrading] DRY_RUN - würde kopieren: {direction.upper()} {symbol} @ ~{reference_price} (Margin {margin}, Hebel {leverage}x)")
        return

    client = get_lighter_client()
    if client is None:
        return
    try:
        market_index = MARKET_INDICES[symbol]
        precision = get_precision(symbol)
        min_base = get_min_base_amount(symbol)
        position_usdc = margin * leverage
        coin_amount = position_usdc / reference_price
        base_amount = int(coin_amount * precision)
        if base_amount * (1 / precision) < min_base:
            debug_log(f"⚠️ [CopyTrading] Order-Größe für {symbol} unter Mindestgröße")
            return
        is_ask = direction == "short"
        try:
            await client.update_leverage(market_index=market_index, leverage=leverage, margin_mode=0)
        except Exception as e:
            debug_log("[CopyTrading] Hebel setzen fehlgeschlagen", {"error": str(e)})
        tx, tx_hash, err = await place_market_order(client, market_index, symbol, is_ask, base_amount, reference_price)
        if err:
            debug_log(f"⚠️ [CopyTrading] Order fehlgeschlagen für {symbol}", {"error": str(err)})
        else:
            debug_log(f"✅ [CopyTrading] ECHTER Copy-Trade: {direction.upper()} {symbol} @ ~{reference_price}", {"tx_hash": str(tx_hash)})
    finally:
        await client.close()



async def fetch_leaderboard(session):
    """Best-effort Parsing eines INOFFIZIELLEN Endpoints - Feldnamen sind eine Annahme."""
    try:
        async with session.get(HL_LEADERBOARD_URL, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status != 200:
                text = await resp.text()
                CT_STATE["leaderboard_error"] = f"HTTP {resp.status}: {text[:300]}"
                debug_log(f"⚠️ [CopyTrading] Leaderboard HTTP {resp.status}", {"body": text[:500]})
                return None
            data = await resp.json(content_type=None)
    except Exception as e:
        CT_STATE["leaderboard_error"] = str(e)
        debug_log("⚠️ [CopyTrading] Leaderboard-Abfrage fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        return None

    debug_log("🔎 [CopyTrading] Leaderboard Rohantwort (gekürzt)", {
        "typ": str(type(data)),
        "keys_oder_laenge": (list(data.keys()) if isinstance(data, dict) else len(data) if isinstance(data, list) else None),
    })

    rows = None
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        for key in ("leaderboardRows", "rows", "data", "leaderboard"):
            if key in data and isinstance(data[key], list):
                rows = data[key]
                break

    if not rows:
        CT_STATE["leaderboard_error"] = "Konnte Liste in der Antwort nicht finden - Rohstruktur siehe Debug-Log"
        return None

    parsed = []
    for row in rows[:500]:
        if not isinstance(row, dict):
            continue
        address = row.get("ethAddress") or row.get("address") or row.get("user")
        pnl = row.get("pnl") or row.get("allTimePnl") or row.get("accountValue")
        if pnl is None and "windowPerformances" in row:
            try:
                pnl = dict(row["windowPerformances"]).get("allTime", {}).get("pnl")
            except Exception:
                pass
        if address:
            parsed.append({"address": address, "pnl": pnl})

    parsed.sort(key=lambda r: float(r["pnl"]) if r["pnl"] not in (None, "") else 0.0, reverse=True)
    return parsed


async def fetch_user_state(session, address):
    try:
        async with session.post(HL_INFO_URL, json={"type": "clearinghouseState", "user": address},
                                 timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                return None
            return await resp.json(content_type=None)
    except Exception as e:
        debug_log(f"⚠️ [CopyTrading] userState fehlgeschlagen für {address}", {"error": str(e)})
        return None


async def fetch_user_fills(session, address):
    try:
        async with session.post(HL_INFO_URL, json={"type": "userFills", "user": address},
                                 timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                return None
            return await resp.json(content_type=None)
    except Exception as e:
        debug_log(f"⚠️ [CopyTrading] userFills fehlgeschlagen für {address}", {"error": str(e)})
        return None


def extract_ct_positions(user_state):
    if not user_state:
        return []
    out = []
    for ap in user_state.get("assetPositions", []):
        pos = ap.get("position", {})
        szi = float(pos.get("szi", 0) or 0)
        if szi == 0:
            continue
        out.append({
            "coin": pos.get("coin"), "side": "long" if szi > 0 else "short", "size": abs(szi),
            "entry_price": pos.get("entryPx"), "unrealized_pnl": pos.get("unrealizedPnl"),
        })
    return out



async def ct_leaderboard_refresh_loop():
    async with aiohttp.ClientSession() as session:
        while True:
            debug_log("📡 [CopyTrading] Aktualisiere Hyperliquid-Leaderboard...")
            rows = await fetch_leaderboard(session)
            if rows:
                CT_STATE["leaderboard"] = rows[:CT_CONFIG["leaderboard_top_n"]]
                CT_STATE["leaderboard_last_fetch"] = datetime.now().isoformat()
                CT_STATE["leaderboard_error"] = None
                debug_log(f"✅ [CopyTrading] Leaderboard aktualisiert: {len(CT_STATE['leaderboard'])} Trader")
                for i, row in enumerate(CT_STATE["leaderboard"]):
                    addr = row["address"]
                    if addr not in CT_STATE["watched"]:
                        CT_STATE["watched"][addr] = {
                            "label": f"#{i+1} (Leaderboard)", "copy_enabled": False, "coin_settings": {},
                            "copy_margin": CT_CONFIG["copy_margin"], "copy_leverage": CT_CONFIG["copy_leverage"],
                            "last_fill_time": None, "positions": [], "recent_fills": [], "source": "leaderboard",
                            "position_meta": {},
                        }
            await asyncio.sleep(CT_CONFIG["leaderboard_refresh_minutes"] * 60)


async def ct_watch_loop():
    for addr in CT_MANUAL_ADDRESSES:
        if addr not in CT_STATE["watched"]:
            CT_STATE["watched"][addr] = {
                "label": "Manuell hinzugefügt", "copy_enabled": False, "coin_settings": {},
                "copy_margin": CT_CONFIG["copy_margin"], "copy_leverage": CT_CONFIG["copy_leverage"],
                "last_fill_time": None, "positions": [], "recent_fills": [], "source": "manual",
                "position_meta": {},
            }

    async with aiohttp.ClientSession() as session:
        while True:
            for address, info in list(CT_STATE["watched"].items()):
                user_state = await fetch_user_state(session, address)
                info["positions"] = extract_ct_positions(user_state)

                # Position-Metadaten aufräumen: Coins, die nicht mehr offen sind, aus der Historie entfernen
                meta = info.setdefault("position_meta", {})
                open_coins = {p["coin"] for p in info["positions"]}
                for coin in list(meta.keys()):
                    if coin not in open_coins:
                        del meta[coin]

                fills = await fetch_user_fills(session, address)
                if fills:
                    fills_sorted = sorted(fills, key=lambda f: f.get("time", 0))
                    info["recent_fills"] = fills_sorted[-20:]

                    if info["last_fill_time"] is None:
                        info["last_fill_time"] = fills_sorted[-1]["time"] if fills_sorted else int(time.time() * 1000)
                    else:
                        new_fills = [f for f in fills_sorted if f.get("time", 0) > info["last_fill_time"]]
                        copy_actions = 0

                        for f in new_fills:
                            info["last_fill_time"] = f["time"]
                            coin = f.get("coin")
                            side = f.get("side")
                            direction = "long" if side == "B" else "short"
                            price = float(f.get("px", 0) or 0)

                            prev_meta = meta.get(coin)
                            now_iso = datetime.now().isoformat()
                            if prev_meta is None:
                                meta[coin] = {"opened_at": now_iso, "direction": direction, "entries": 1, "last_action": "Neu"}
                            elif prev_meta["direction"] == direction:
                                prev_meta["entries"] += 1
                                prev_meta["last_action"] = "Nachkauf"
                            else:
                                meta[coin] = {"opened_at": now_iso, "direction": direction, "entries": 1, "last_action": "Reverse"}

                            # Nur kopieren, wenn Copy an ist UND dieser Coin explizit fuer diesen
                            # Trader freigeschaltet wurde (keine Coin-Einstellung = wird NICHT kopiert)
                            coin_cfg = (info.get("coin_settings") or {}).get(coin)
                            if info["copy_enabled"] and coin_cfg and coin_cfg.get("enabled", True) and price > 0:
                                margin = coin_cfg.get("margin") or info.get("copy_margin", CT_CONFIG["copy_margin"])
                                leverage = coin_cfg.get("leverage") or info.get("copy_leverage", CT_CONFIG["copy_leverage"])
                                debug_log(f"🆕 [CopyTrading] Kopiere Fill bei {info['label']} ({address[:8]}...): {direction.upper()} {coin} @ {price}")
                                await execute_copy_trade(coin, direction, price, margin, leverage)
                                copy_actions += 1

                        if new_fills:
                            # Eine Sammelzeile statt einer Zeile pro Fill - haelt das Log lesbar,
                            # auch wenn der Trader (z.B. Market-Maker) hunderte Fills auf einmal macht
                            debug_log(f"📊 [CopyTrading] {info['label']} ({address[:8]}...): {len(new_fills)} neue Fills erkannt, {copy_actions} davon kopiert")

                await asyncio.sleep(0.5)

            await asyncio.sleep(CT_CONFIG["poll_interval_seconds"])


# ========== LIGHTER CLIENT ==========

# ========== REDIS-PERSISTENZ (Copy-Trading-Einstellungen) ==========
async def save_ct_watched():
    r = await get_redis()
    if r is None:
        return
    try:
        trimmed = {
            addr: {
                "label": info.get("label"), "copy_enabled": info.get("copy_enabled", False),
                "coin_settings": info.get("coin_settings", {}), "copy_margin": info.get("copy_margin"),
                "copy_leverage": info.get("copy_leverage"), "source": info.get("source"),
            }
            for addr, info in CT_STATE["watched"].items()
        }
        await r.set("gridbot:ct_watched", json.dumps(trimmed))
    except Exception as e:
        debug_log("⚠️ Speichern der Copy-Trading-Einstellungen fehlgeschlagen", {"error": str(e)})


async def load_ct_watched():
    r = await get_redis()
    if r is None:
        return
    try:
        raw_watched = await r.get("gridbot:ct_watched")
        if raw_watched:
            saved = json.loads(raw_watched)
            for addr, cfg in saved.items():
                CT_STATE["watched"][addr] = {
                    "label": cfg.get("label", "Wiederhergestellt"), "copy_enabled": cfg.get("copy_enabled", False),
                    "coin_settings": cfg.get("coin_settings", {}), "copy_margin": cfg.get("copy_margin", CT_CONFIG["copy_margin"]),
                    "copy_leverage": cfg.get("copy_leverage", CT_CONFIG["copy_leverage"]), "source": cfg.get("source", "manual"),
                    "last_fill_time": None, "positions": [], "recent_fills": [], "position_meta": {},
                }
            debug_log(f"✅ {len(saved)} Copy-Trading-Trader aus Redis wiederhergestellt")
    except Exception as e:
        debug_log("⚠️ Laden der Copy-Trading-Einstellungen fehlgeschlagen", {"error": str(e)})


CT_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8"><title>Copy-Trading Dashboard</title>
<style>
  :root { --bg:#060a18; --panel:#0e1526; --border:rgba(96,165,250,0.14); --accent:#3b82f6; --green:#22c55e; --red:#f0526b; --text:#e8ecf5; --dim:#7c8aa8; }
  * { box-sizing:border-box; }
  body { font-family:-apple-system,sans-serif; background:var(--bg); color:var(--text); margin:0; padding:24px; }
  h1 { font-size:20px; display:flex; align-items:center; gap:14px; }
  h1 a { font-size:13px; color:var(--accent); text-decoration:none; }
  h2 { font-size:13px; color:var(--dim); text-transform:uppercase; letter-spacing:0.05em; margin-top:28px; }
  .green{color:var(--green)!important;} .red{color:var(--red)!important;} .yellow{color:#fbbf24!important;}
  table { width:100%; border-collapse:collapse; font-size:13px; margin-top:10px; background:var(--panel); border:1px solid var(--border); border-radius:14px; overflow:hidden; }
  th,td { text-align:left; padding:9px 12px; border-bottom:1px solid var(--border); }
  th { color:var(--dim); font-size:11px; text-transform:uppercase; }
  th.sortable { cursor:pointer; user-select:none; }
  th.sortable:hover { color:var(--text); }
  input[type=text] { padding:8px 10px; background:#080d1c; border:1px solid var(--border); border-radius:8px; color:var(--text); }
  input.new-address { width:340px; }
  input.coin-filter { width:130px; font-size:12px; }
  button { padding:7px 14px; background:linear-gradient(135deg,var(--accent),#2563eb); color:#fff; border:none; border-radius:8px; cursor:pointer; font-weight:700; font-size:12px; }
  button.copy-on { background:linear-gradient(135deg,#22c55e,#15803d); }
  button.copy-off { background:linear-gradient(135deg,#475569,#334155); }
  .warn { background:rgba(240,82,107,0.12); border:1px solid rgba(240,82,107,0.35); color:#fca5b1; padding:10px 14px; border-radius:10px; font-size:13px; margin-top:10px; }
  .addr { font-family:monospace; font-size:12px; color:var(--dim); }
  .fill-buy { color:var(--green); } .fill-sell { color:var(--red); }
  .bar-wrap { background:#080d1c; border-radius:6px; overflow:hidden; height:18px; display:flex; min-width:120px; }
  .bar-long { background:var(--green); height:100%; } .bar-short { background:var(--red); height:100%; }
  .action-neu { color:#93c5fd; } .action-nachkauf { color:#fbbf24; } .action-reverse { color:#f0526b; }
</style>
</head>
<body>
<h1>📡 Copy-Trading <span id="mode-badge"></span> <a href="/">← zurück zum Grid-Bot</a></h1>
<div id="leaderboard-error"></div>

<h2>Trendmeter (Top 20 Coins - Long/Short-Verteilung der beobachteten Trader)</h2>
<table id="trend-table">
  <thead><tr><th>Coin</th><th>Long</th><th>Short</th><th>Verteilung</th></tr></thead>
  <tbody></tbody>
</table>

<h2>Manuelle Wallet hinzufügen</h2>
<div>
  <input type="text" class="new-address" id="new-address" placeholder="0x... Hyperliquid Wallet-Adresse">
  <button id="btn-add-address">Hinzufügen</button>
</div>

<h2>Beobachtete Trader - Positionen im Detail</h2>
<table id="watch-table">
  <thead><tr>
    <th class="sortable" data-key="label">Label ⇅</th>
    <th class="sortable" data-key="addr">Adresse ⇅</th>
    <th class="sortable" data-key="coin">Coin ⇅</th>
    <th class="sortable" data-key="side">Seite ⇅</th>
    <th class="sortable" data-key="size">Größe ⇅</th>
    <th class="sortable" data-key="entry_price">Ø-Einstieg ⇅</th>
    <th class="sortable" data-key="pnl">PnL ⇅</th>
    <th class="sortable" data-key="opened_at">Eröffnet ⇅</th>
    <th class="sortable" data-key="last_action">Aktion ⇅</th>
  </tr></thead>
  <tbody></tbody>
</table>

<h2>Beobachtete Trader (anklicken für Details)</h2>
<table id="copy-table">
  <thead><tr><th>Label</th><th>Adresse</th><th>Offene Positionen</th><th>Konfigurierte Coins</th><th>Copy</th></tr></thead>
  <tbody></tbody>
</table>

<!-- Detail-Modal: zeigt nur die Trades/Einstellungen EINES Traders -->
<div id="trader-modal" style="display:none; position:fixed; top:0; left:0; right:0; bottom:0; background:rgba(0,0,0,0.7); z-index:100; overflow-y:auto;">
  <div style="max-width:900px; margin:40px auto; background:var(--panel); border:1px solid var(--border); border-radius:16px; padding:24px;">
    <div style="display:flex; justify-content:space-between; align-items:center;">
      <h2 id="modal-title" style="margin:0;">Trader-Details</h2>
      <button onclick="closeModal()" style="background:#374151;">✖️ Schließen</button>
    </div>

    <h2>Standard-Margin/Hebel für diesen Trader</h2>
    <div>
      <input type="text" id="modal-default-margin" placeholder="Margin $" style="width:100px;">
      <input type="text" id="modal-default-leverage" placeholder="Hebel" style="width:80px;">
      <button onclick="saveTraderDefaults()">💾 Speichern</button>
      <span style="font-size:12px; color:var(--dim);"> (gilt für Coins ohne eigene Einstellung unten)</span>
    </div>

    <h2>Coins zum Kopieren freischalten</h2>
    <table>
      <thead><tr><th>Coin</th><th>Margin $ (leer = Standard)</th><th>Hebel (leer = Standard)</th><th>Aktiv</th><th></th></tr></thead>
      <tbody id="modal-coin-settings"></tbody>
    </table>
    <div style="margin-top:10px;">
      <input type="text" id="modal-new-coin" placeholder="z.B. BTC" style="width:100px;">
      <input type="text" id="modal-new-margin" placeholder="Margin $" style="width:100px;">
      <input type="text" id="modal-new-leverage" placeholder="Hebel" style="width:80px;">
      <button onclick="addCoinSetting()">+ Coin hinzufügen</button>
    </div>

    <h2>Offene Positionen dieses Traders</h2>
    <table>
      <thead><tr><th>Coin</th><th>Seite</th><th>Größe</th><th>Ø-Einstieg</th><th>PnL</th><th>Eröffnet</th><th>Aktion</th></tr></thead>
      <tbody id="modal-positions"></tbody>
    </table>

    <h2>Letzte Fills dieses Traders</h2>
    <table>
      <thead><tr><th>Zeit</th><th>Seite</th><th>Coin</th><th>Preis</th></tr></thead>
      <tbody id="modal-fills"></tbody>
    </table>
  </div>
</div>

<script>
function fmtTime(iso) {
  return iso ? new Date(iso).toLocaleString('de-DE', {day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'}) : '-';
}

let sortKey = null;
let sortAsc = true;

function renderPosTable() {
  let rows = [...(window.posData || [])];
  if (sortKey) {
    rows.sort((a, b) => {
      let av = a[sortKey], bv = b[sortKey];
      if (typeof av === 'string') av = av.toLowerCase();
      if (typeof bv === 'string') bv = bv.toLowerCase();
      if (av === null || av === undefined) av = '';
      if (bv === null || bv === undefined) bv = '';
      if (av < bv) return sortAsc ? -1 : 1;
      if (av > bv) return sortAsc ? 1 : -1;
      return 0;
    });
  }
  document.querySelector('#watch-table tbody').innerHTML = rows.map(r => `
    <tr>
      <td>${r.label}</td>
      <td class="addr">${r.addr_short}</td>
      <td><b>${r.coin}</b></td>
      <td class="${r.side==='long'?'green':'red'}">${r.side==='long'?'🟢 LONG':'🔴 SHORT'}</td>
      <td>${r.size}</td>
      <td>${r.entry_price ?? '-'}</td>
      <td class="${r.pnl>=0?'green':'red'}">$${r.pnl.toFixed(2)}</td>
      <td>${fmtTime(r.opened_at)}</td>
      <td class="${r.actionClass}">${r.last_action || '-'}${r.entries>1?' ('+r.entries+'x)':''}</td>
    </tr>`).join('') || '<tr><td colspan="9">Noch keine offenen Positionen erfasst...</td></tr>';
}

document.querySelectorAll('#watch-table th.sortable').forEach(th => {
  th.addEventListener('click', () => {
    const key = th.dataset.key;
    if (sortKey === key) { sortAsc = !sortAsc; } else { sortKey = key; sortAsc = true; }
    renderPosTable();
  });
});

async function refresh() {
  const res = await fetch('/api/ct/status');
  const data = await res.json();

  document.getElementById('mode-badge').innerHTML = data.dry_run
    ? '<span style="background:rgba(99,102,241,.18);color:#a5b4fc;padding:4px 12px;border-radius:20px;font-size:12px;">DRY RUN</span>'
    : '<span style="background:rgba(240,82,107,.15);color:#fca5b1;padding:4px 12px;border-radius:20px;font-size:12px;">LIVE</span>';

  document.getElementById('leaderboard-error').innerHTML = data.leaderboard_error
    ? `<div class="warn">⚠️ Leaderboard-Fehler: ${data.leaderboard_error} - manuelle Adressen funktionieren trotzdem.</div>` : '';

  // Trendmeter
  document.querySelector('#trend-table tbody').innerHTML = (data.trend_meter || []).map(t => {
    const pct = t.pct_long;
    const barHtml = pct === null ? '<span style="color:var(--dim);">keine Daten</span>' :
      `<div class="bar-wrap"><div class="bar-long" style="width:${pct}%"></div><div class="bar-short" style="width:${100-pct}%"></div></div> ${pct}% long`;
    return `<tr><td><b>${t.coin}</b></td><td class="green">${t.long}</td><td class="red">${t.short}</td><td>${barHtml}</td></tr>`;
  }).join('');

  // Positions-Detailtabelle (eine Zeile pro Position)
  let posData = [];
  Object.entries(data.watched).forEach(([addr, info]) => {
    const meta = info.position_meta || {};
    (info.positions || []).forEach(p => {
      const m = meta[p.coin] || {};
      const pnl = parseFloat(p.unrealized_pnl || 0);
      const actionClass = m.last_action === 'Neu' ? 'action-neu' : m.last_action === 'Nachkauf' ? 'action-nachkauf' : m.last_action === 'Reverse' ? 'action-reverse' : '';
      posData.push({
        label: info.label, addr_short: addr.slice(0,8) + '...' + addr.slice(-4),
        coin: p.coin, side: p.side, size: parseFloat(p.size || 0), entry_price: p.entry_price,
        pnl, opened_at: m.opened_at || '', last_action: m.last_action || '', entries: m.entries || 0, actionClass,
      });
    });
  });
  window.posData = posData;
  renderPosTable();

  window.watchedData = data.watched;  // fuer's Modal zwischenspeichern

  // Trader-Übersicht (klickbar)
  const copyRows = Object.entries(data.watched).map(([addr, info]) => {
    const posSummary = (info.positions || []).map(p => `${p.side==='long'?'🟢':'🔴'} ${p.coin}`).join(', ') || '-';
    const coinCount = Object.keys(info.coin_settings || {}).length;
    return `
      <tr>
        <td style="cursor:pointer; color:var(--accent);" onclick="openModal('${addr}')">${info.label} 🔍</td>
        <td class="addr">${addr.slice(0,10)}...${addr.slice(-6)}</td>
        <td>${posSummary}</td>
        <td>${coinCount} Coin${coinCount===1?'':'s'} konfiguriert</td>
        <td><button class="${info.copy_enabled?'copy-on':'copy-off'}" onclick="toggleCopy('${addr}', ${!info.copy_enabled})">${info.copy_enabled?'Copy AN':'Copy AUS'}</button></td>
      </tr>`;
  }).join('');
  document.querySelector('#copy-table tbody').innerHTML = copyRows || '<tr><td colspan="5">Noch keine Trader beobachtet...</td></tr>';

  // Falls das Modal gerade offen ist, dessen Inhalt mit aktualisieren
  if (window.currentModalAddress) renderModal(window.currentModalAddress);
}

let currentModalAddress = null;

function openModal(address) {
  window.currentModalAddress = address;
  document.getElementById('trader-modal').style.display = 'block';
  renderModal(address);
}

function closeModal() {
  window.currentModalAddress = null;
  document.getElementById('trader-modal').style.display = 'none';
}

function renderModal(address) {
  const info = (window.watchedData || {})[address];
  if (!info) return;

  document.getElementById('modal-title').innerText = `${info.label} (${address.slice(0,10)}...${address.slice(-6)})`;
  document.getElementById('modal-default-margin').value = info.copy_margin ?? '';
  document.getElementById('modal-default-leverage').value = info.copy_leverage ?? '';

  const coinSettings = info.coin_settings || {};
  document.getElementById('modal-coin-settings').innerHTML = Object.entries(coinSettings).map(([coin, cfg]) => `
    <tr>
      <td><b>${coin}</b></td>
      <td><input type="text" style="width:80px;" id="cs-margin-${coin}" value="${cfg.margin ?? ''}"></td>
      <td><input type="text" style="width:60px;" id="cs-lev-${coin}" value="${cfg.leverage ?? ''}"></td>
      <td><input type="checkbox" id="cs-enabled-${coin}" ${cfg.enabled ? 'checked' : ''}></td>
      <td>
        <button onclick="saveCoinSetting('${address}','${coin}')">💾</button>
        <button onclick="removeCoinSetting('${address}','${coin}')" style="background:#7f1d1d;">🗑️</button>
      </td>
    </tr>`).join('') || '<tr><td colspan="5" style="color:var(--dim);">Noch keine Coins freigeschaltet - unten hinzufügen</td></tr>';

  const meta = info.position_meta || {};
  document.getElementById('modal-positions').innerHTML = (info.positions || []).map(p => {
    const m = meta[p.coin] || {};
    const pnl = parseFloat(p.unrealized_pnl || 0);
    return `<tr>
      <td><b>${p.coin}</b></td>
      <td class="${p.side==='long'?'green':'red'}">${p.side==='long'?'🟢 LONG':'🔴 SHORT'}</td>
      <td>${p.size}</td><td>${p.entry_price ?? '-'}</td>
      <td class="${pnl>=0?'green':'red'}">$${pnl.toFixed(2)}</td>
      <td>${fmtTime(m.opened_at)}</td><td>${m.last_action ?? '-'}</td>
    </tr>`;
  }).join('') || '<tr><td colspan="7" style="color:var(--dim);">Keine offenen Positionen</td></tr>';

  document.getElementById('modal-fills').innerHTML = (info.recent_fills || []).slice(-20).reverse().map(f => `
    <tr>
      <td>${fmtTime(new Date(f.time).toISOString())}</td>
      <td class="${f.side==='B'?'fill-buy':'fill-sell'}">${f.side==='B'?'BUY':'SELL'}</td>
      <td>${f.coin}</td><td>${f.px}</td>
    </tr>`).join('') || '<tr><td colspan="4" style="color:var(--dim);">Noch keine Fills erfasst</td></tr>';
}

async function saveTraderDefaults() {
  const address = window.currentModalAddress;
  await fetch('/api/ct/trader_defaults', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({
    address,
    copy_margin: document.getElementById('modal-default-margin').value,
    copy_leverage: document.getElementById('modal-default-leverage').value,
  })});
  refresh();
}

async function saveCoinSetting(address, coin) {
  await fetch('/api/ct/coin_setting', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({
    address, coin,
    margin: document.getElementById(`cs-margin-${coin}`).value,
    leverage: document.getElementById(`cs-lev-${coin}`).value,
    enabled: document.getElementById(`cs-enabled-${coin}`).checked,
  })});
  refresh();
}

async function removeCoinSetting(address, coin) {
  await fetch('/api/ct/remove_coin_setting', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({address, coin}) });
  refresh();
}

async function addCoinSetting() {
  const address = window.currentModalAddress;
  const coin = document.getElementById('modal-new-coin').value.trim().toUpperCase();
  if (!coin) return;
  await fetch('/api/ct/coin_setting', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({
    address, coin,
    margin: document.getElementById('modal-new-margin').value,
    leverage: document.getElementById('modal-new-leverage').value,
    enabled: true,
  })});
  document.getElementById('modal-new-coin').value = '';
  document.getElementById('modal-new-margin').value = '';
  document.getElementById('modal-new-leverage').value = '';
  refresh();
}

async function toggleCopy(address, enable) {
  await fetch('/api/ct/copy', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({address, enable}) });
  refresh();
}

document.getElementById('btn-add-address').addEventListener('click', async () => {
  const addr = document.getElementById('new-address').value.trim();
  if (!addr) return;
  await fetch('/api/ct/watch', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({address: addr}) });
  document.getElementById('new-address').value = '';
  refresh();
});

refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>
"""


async def handle_ct_index(request):
    return web.Response(text=CT_DASHBOARD_HTML, content_type="text/html")


TREND_METER_COINS = ["BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "ADA", "AVAX", "LINK", "DOT",
                      "TON", "SUI", "LTC", "TRX", "HYPE", "NEAR", "APT", "UNI", "ICP", "BCH"]


def compute_trend_meter():
    tally = {c: {"long": 0, "short": 0} for c in TREND_METER_COINS}
    for info in CT_STATE["watched"].values():
        for p in info.get("positions", []):
            coin = p.get("coin")
            side = p.get("side")
            if coin in tally and side in ("long", "short"):
                tally[coin][side] += 1

    result = []
    for c in TREND_METER_COINS:
        l, s = tally[c]["long"], tally[c]["short"]
        total = l + s
        pct_long = round(l / total * 100, 1) if total else None
        result.append({"coin": c, "long": l, "short": s, "pct_long": pct_long})
    return result


async def handle_ct_status(request):
    return web.json_response({
        "dry_run": CT_CONFIG["dry_run"],
        "leaderboard_last_fetch": CT_STATE["leaderboard_last_fetch"],
        "leaderboard_error": CT_STATE["leaderboard_error"],
        "watched": CT_STATE["watched"],
        "trend_meter": compute_trend_meter(),
    })


async def handle_ct_watch(request):
    body = await request.json()
    addr = body.get("address", "").strip()
    if not addr:
        return web.json_response({"error": "keine Adresse"}, status=400)
    if addr not in CT_STATE["watched"]:
        CT_STATE["watched"][addr] = {
            "label": "Manuell hinzugefügt", "copy_enabled": False, "coin_settings": {},
            "copy_margin": CT_CONFIG["copy_margin"], "copy_leverage": CT_CONFIG["copy_leverage"],
            "last_fill_time": None, "positions": [], "recent_fills": [], "source": "manual",
            "position_meta": {},
        }
        await save_ct_watched()
    return web.json_response({"success": True})


async def handle_ct_copy_toggle(request):
    body = await request.json()
    addr = body.get("address")
    enable = bool(body.get("enable"))
    if addr in CT_STATE["watched"]:
        CT_STATE["watched"][addr]["copy_enabled"] = enable
        debug_log(f"{'✅' if enable else '⏸️'} [CopyTrading] Copy für {addr} {'aktiviert' if enable else 'deaktiviert'}")
        await save_ct_watched()
    return web.json_response({"success": True})


async def handle_ct_set_coin_setting(request):
    """Setzt/aktualisiert Margin, Hebel und An/Aus fuer EINEN Coin bei EINEM Trader."""
    body = await request.json()
    addr = body.get("address")
    coin = (body.get("coin") or "").strip().upper()
    if addr not in CT_STATE["watched"] or not coin:
        return web.json_response({"error": "ungültige Adresse oder Coin"}, status=400)

    info = CT_STATE["watched"][addr]
    settings = info.setdefault("coin_settings", {})
    settings[coin] = {
        "enabled": bool(body.get("enabled", True)),
        "margin": float(body["margin"]) if body.get("margin") not in (None, "") else None,
        "leverage": int(body["leverage"]) if body.get("leverage") not in (None, "") else None,
    }
    debug_log(f"⚙️ [CopyTrading] Coin-Einstellung für {addr} / {coin} gesetzt", settings[coin])
    await save_ct_watched()
    return web.json_response({"success": True, "coin_settings": settings})


async def handle_ct_remove_coin_setting(request):
    body = await request.json()
    addr = body.get("address")
    coin = (body.get("coin") or "").strip().upper()
    if addr in CT_STATE["watched"]:
        CT_STATE["watched"][addr].get("coin_settings", {}).pop(coin, None)
        await save_ct_watched()
    return web.json_response({"success": True})


async def handle_ct_set_trader_defaults(request):
    """Setzt Standard-Margin/Hebel fuer einen Trader (greift fuer Coins ohne eigene Einstellung)."""
    body = await request.json()
    addr = body.get("address")
    if addr not in CT_STATE["watched"]:
        return web.json_response({"error": "unbekannte Adresse"}, status=400)
    info = CT_STATE["watched"][addr]
    if body.get("copy_margin") not in (None, ""):
        info["copy_margin"] = float(body["copy_margin"])
    if body.get("copy_leverage") not in (None, ""):
        info["copy_leverage"] = int(body["copy_leverage"])
    await save_ct_watched()
    return web.json_response({"success": True})


