"""
bot_core.py - Gemeinsames Fundament fuer Grid-Bot + Strategien + Copy-Trading
=================================================================================
Enthaelt: Lighter-Client, Coin-Konfiguration, Config/State-Struktur, Redis-
Persistenz, Positions-Ein-/Ausstieg (execute_entry/execute_exit), und das
Haupt-Dashboard (Grid/HA-Supertrend/Kerzenfarbe/OBI-Scalp Uebersicht).

Wird von strategies.py UND copytrade.py importiert - main.py bindet alles
zusammen und startet den Server + alle Hintergrund-Loops.
"""

import asyncio
import websockets
import aiohttp
import json
import time
import os
import traceback
from datetime import datetime
from aiohttp import web

try:
    import redis.asyncio as redis_lib
except ImportError:
    redis_lib = None

BASE_URL = "https://mainnet.zklighter.elliot.ai"
WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"

DEBUG_MODE = os.getenv("DEBUG_MODE", "true").lower() == "true"


def debug_log(msg, data=None):
    if DEBUG_MODE:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        print(f"[DEBUG {timestamp}] {msg}", flush=True)
        if data:
            print(f"   DATA: {json.dumps(data, indent=2, default=str)}", flush=True)


MARKET_INDICES = {
    "ETH": 0, "BTC": 1, "SOL": 2, "DOGE": 3, "XRP": 7, "LINK": 8, "AVAX": 9,
    "NEAR": 10, "DOT": 11, "TON": 12, "SUI": 16, "BNB": 25, "UNI": 30, "APT": 31,
    "ADA": 39, "TRX": 43, "LTC": 35, "BCH": 58, "HBAR": 59, "ICP": 102, "HYPE": 24,
    "EURUSD": 96, "GBPUSD": 97, "USDJPY": 98, "USDCHF": 99, "USDCAD": 100,
    "AUDUSD": 106, "NZDUSD": 107, "USDKRW": 105,
    "XAU": 92, "XAG": 93, "WTI": 145,
}
PRECISION_MAP = {
    "BTC": 100000, "ETH": 10000, "SOL": 1000, "LTC": 1000,
    "AVAX": 100, "BNB": 100, "UNI": 100, "APT": 100, "XAG": 100,
    "LINK": 10, "NEAR": 10, "DOT": 10, "SUI": 10, "ADA": 10, "EURUSD": 10, "GBPUSD": 10, "USDCHF": 10, "USDCAD": 10,
    "DOGE": 1, "XRP": 1, "TRX": 1,
    "USDJPY": 1000, "AUDUSD": 10, "NZDUSD": 10, "USDKRW": 10, "XAU": 10000,
    # BCH, HBAR, ICP, TON, WTI: keine explizite Angabe in der Quelle - Standardwert (10000) greift,
    # bitte vor dem Live-Handel dieser Coins/Rohstoffe unbedingt mit kleiner Größe testen!
}
PRICE_DECIMALS_MAP = {
    "BTC": 1, "ETH": 2, "SOL": 3, "LTC": 3, "XAU": 1,
    "AVAX": 3, "BNB": 4, "UNI": 4, "APT": 4,
    "LINK": 5, "NEAR": 5, "DOT": 5, "SUI": 5, "ADA": 5, "EURUSD": 5, "GBPUSD": 5, "USDCHF": 5, "USDCAD": 5,
    "DOGE": 6, "XRP": 6, "XAG": 6,
    "USDJPY": 3, "AUDUSD": 5, "NZDUSD": 5, "USDKRW": 5,
}
MIN_BASE_AMOUNT_MAP = {
    "BTC": 0.00020, "ETH": 0.005, "SOL": 0.05, "LTC": 0.1, "BCH": 0.01,
    "AVAX": 0.5, "BNB": 0.02, "UNI": 1.0, "APT": 2.0, "XAU": 0.003, "XAG": 0.15,
    "LINK": 1.0, "NEAR": 2.0, "DOT": 2.0, "SUI": 3.0, "ADA": 10.0,
    "DOGE": 10, "XRP": 20, "HBAR": 20.0,
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
        "entry_mode": os.getenv("ENTRY_MODE", "grid"),  # "grid" oder "ha_st"
        "grid_mode": os.getenv("GRID_MODE", "pct"),  # "pct" oder "usd"
        "grid_step_pct": float(os.getenv("GRID_STEP_PCT", "0.25")),
        "tp_step_pct": float(os.getenv("TP_STEP_PCT", "0.25")),
        "grid_step_usd": float(os.getenv("GRID_STEP_USD", "150")),
        "tp_step_usd": float(os.getenv("TP_STEP_USD", "150")),
        "max_nachkauf": int(os.getenv("MAX_NACHKAUF", "5")),
        "bot_active": True,
        "auto_reverse": os.getenv("AUTO_REVERSE", "true").lower() == "true",
        "ha_st_resolution": os.getenv("HA_ST_RESOLUTION", "5m"),
        "ha_st_atr_period": int(os.getenv("HA_ST_ATR_PERIOD", "5")),
        "ha_st_atr_mult": float(os.getenv("HA_ST_ATR_MULT", "1.5")),
        "ha_st_trend_filter": os.getenv("HA_ST_TREND_FILTER", "true").lower() == "true",
        "ha_st_trend_ema_length": int(os.getenv("HA_ST_TREND_EMA_LENGTH", "200")),
        "ha_st_candle_source": os.getenv("HA_ST_CANDLE_SOURCE", "binance"),  # "lighter" oder "binance"
        "cc_resolution_seconds": int(os.getenv("CC_RESOLUTION_SECONDS", "60")),
        "cc_confirm_delay_seconds": int(os.getenv("CC_CONFIRM_DELAY_SECONDS", "20")),
        "cc_auto_reverse": os.getenv("CC_AUTO_REVERSE", "true").lower() == "true",
        "cc_early_exit": os.getenv("CC_EARLY_EXIT", "true").lower() == "true",
        "obi_threshold": float(os.getenv("OBI_THRESHOLD", "0.30")),
        "obi_mode": os.getenv("OBI_MODE", "momentum"),  # "momentum" (mit dem Ungleichgewicht) oder "mean_reversion" (dagegen)
        "obi_window_fast_seconds": float(os.getenv("OBI_WINDOW_FAST_SECONDS", "5")),
        "obi_window_medium_seconds": float(os.getenv("OBI_WINDOW_MEDIUM_SECONDS", "20")),
        "obi_window_slow_seconds": float(os.getenv("OBI_WINDOW_SLOW_SECONDS", "60")),
        "obi_levels": int(os.getenv("OBI_LEVELS", "15")),
        "obi_tp_sl_mode": os.getenv("OBI_TP_SL_MODE", "pct"),  # "pct" oder "usd"
        "obi_tp_pct": float(os.getenv("OBI_TP_PCT", "0.15")),
        "obi_sl_pct": float(os.getenv("OBI_SL_PCT", "0.15")),
        "obi_tp_usd": float(os.getenv("OBI_TP_USD", "1")),
        "obi_sl_usd": float(os.getenv("OBI_SL_USD", "1")),
        "obi_cooldown_seconds": float(os.getenv("OBI_COOLDOWN_SECONDS", "7")),
        "obi_trend_filter": os.getenv("OBI_TREND_FILTER", "false").lower() == "true",
        "obi_trend_ema_length": int(os.getenv("OBI_TREND_EMA_LENGTH", "300")),
    }


def default_state():
    return {
        "position": None, "avg_entry_price": None, "total_coin_size": 0.0,
        "entry_count": 0, "anchor_price": None, "last_price": None,
        "price_history": [],
        "ha_st_stop_price": None, "position_opened_at": None,
        "cc_candle_start": None, "cc_candle_open": None, "cc_entered_this_candle": False, "cc_last_color": None,
        "obi_book": {"bids": {}, "asks": {}}, "obi_avg_buffer": [], "obi_last_signal_direction": None,
        "obi_fast": None, "obi_medium": None, "obi_slow": None, "obi_history": [],
        "last_entry_price": None,
        "obi_last_trade_time": 0.0, "obi_trend_ema": None, "obi_current": None,
        "stats": {"trades": 0, "wins": 0, "losses": 0, "total_pnl_usd": 0.0},
        "trade_log": [],
    }


# ========== GLOBALER STATE - EIN EINTRAG PRO COIN ==========
BOTS = {s: {"config": default_config(), "state": default_state()} for s in SYMBOLS}

# ========== REDIS-PERSISTENZ (Grid-Bot-Configs) ==========
REDIS_URL = os.getenv("REDIS_URL", "").strip().strip('"').strip("'")
_redis_client = None


async def get_redis():
    global _redis_client
    if not REDIS_URL or redis_lib is None:
        return None
    if _redis_client is None:
        try:
            debug_log("🔎 Redis-URL Diagnose (Passwort verdeckt)", {
                "laenge": len(REDIS_URL),
                "beginnt_mit": REDIS_URL[:12] + "...",
                "startet_korrekt_mit_redis://": REDIS_URL.startswith("redis://"),
            })
            _redis_client = redis_lib.from_url(REDIS_URL, decode_responses=True)
            await _redis_client.ping()
            debug_log("✅ Redis verbunden - Einstellungen werden ab jetzt gespeichert")
        except Exception as e:
            debug_log("⚠️ Redis-Verbindung fehlgeschlagen - läuft ohne Persistenz weiter", {"error": str(e)})
            _redis_client = None
    return _redis_client


async def save_bot_configs():
    r = await get_redis()
    if r is None:
        return
    try:
        data = {s: BOTS[s]["config"] for s in SYMBOLS}
        await r.set("gridbot:configs", json.dumps(data))
    except Exception as e:
        debug_log("⚠️ Speichern der Grid-Bot-Configs fehlgeschlagen", {"error": str(e)})


VALID_RESOLUTIONS = {"1m", "5m", "15m", "30m", "1h", "4h"}


async def load_bot_configs():
    r = await get_redis()
    if r is None:
        return
    try:
        raw_configs = await r.get("gridbot:configs")
        if raw_configs:
            saved = json.loads(raw_configs)
            for s in SYMBOLS:
                if s in saved:
                    incoming = saved[s]
                    for res_key in ("ha_st_resolution",):
                        if res_key in incoming and incoming[res_key] not in VALID_RESOLUTIONS:
                            debug_log(f"⚠️ [{s}] Ungültiger gespeicherter Zeitrahmen '{incoming[res_key]}' - auf Standard zurückgesetzt")
                            incoming.pop(res_key)
                    BOTS[s]["config"].update(incoming)
            debug_log("✅ Grid-Bot-Configs aus Redis geladen", {"coins": list(saved.keys())})
    except Exception as e:
        debug_log("⚠️ Laden der Grid-Bot-Configs fehlgeschlagen", {"error": str(e)})


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



def compute_step_abs(reference_price, cfg, which):
    """which: 'grid' oder 'tp' - liefert den Abstand in Preiseinheiten, je nach grid_mode."""
    if cfg["grid_mode"] == "usd":
        return cfg["grid_step_usd"] if which == "grid" else cfg["tp_step_usd"]
    pct = cfg["grid_step_pct"] if which == "grid" else cfg["tp_step_pct"]
    return reference_price * (pct / 100)


def calc_grid_levels(symbol):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    levels = {"anchor": st["anchor_price"], "tp_price": None, "next_nachkauf_price": None,
              "grid_step_abs": None, "tp_step_abs": None}
    if st["position"] is None:
        if st["anchor_price"] is not None:
            step = compute_step_abs(st["anchor_price"], cfg, "grid")
            levels["next_entry_long"] = round(st["anchor_price"] - step, 4)
            levels["next_entry_short"] = round(st["anchor_price"] + step, 4)
            levels["grid_step_abs"] = round(step, 4)
    elif st["avg_entry_price"] is not None:
        tp_step = compute_step_abs(st["avg_entry_price"], cfg, "tp")
        grid_step = compute_step_abs(st["avg_entry_price"], cfg, "grid")
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
        st["position_opened_at"] = datetime.now().isoformat()

    st["last_entry_price"] = price
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
        "entries": st["entry_count"], "pnl_usd": round(pnl_usd, 3),
        "opened_at": st.get("position_opened_at"), "closed_at": datetime.now().isoformat(), "reason": reason,
    })

    debug_log(f"🏁 [{symbol}] Position geschlossen ({reason}): {st['position'].upper()} Ø{round(st['avg_entry_price'],2)} -> {price} | PnL ${round(pnl_usd,3)}")

    st["position"] = None
    st["avg_entry_price"] = None
    st["total_coin_size"] = 0.0
    st["entry_count"] = 0
    st["anchor_price"] = price
    st["ha_st_stop_price"] = None
    st["position_opened_at"] = None
    st["last_entry_price"] = None

    if cfg.get("auto_reverse", True) and cfg["bot_active"] and cfg["entry_mode"] == "grid":
        opposite = "short" if closing_side == "long" else "long"
        await execute_entry(symbol, opposite, price, is_add_on=False)



DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8"><title>Grid-Bot Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
  :root {
    --bg: #060a18;
    --panel: #0e1526;
    --panel-border: rgba(96, 165, 250, 0.14);
    --accent: #3b82f6;
    --accent2: #8b5cf6;
    --text: #e8ecf5;
    --text-dim: #7c8aa8;
    --green: #22c55e;
    --red: #f0526b;
  }
  * { box-sizing: border-box; }
  body {
    font-family: -apple-system, "Segoe UI", sans-serif;
    background:
      radial-gradient(ellipse 800px 500px at 90% -5%, rgba(59,130,246,0.16), transparent 60%),
      radial-gradient(ellipse 700px 500px at -5% 15%, rgba(139,92,246,0.12), transparent 60%),
      var(--bg);
    color: var(--text);
    margin: 0;
    padding: 0 0 40px 0;
    min-height: 100vh;
  }
  .topbar {
    display: flex; align-items: center; justify-content: space-between;
    padding: 16px 28px; background: rgba(10,14,28,0.85); backdrop-filter: blur(8px);
    border-bottom: 1px solid var(--panel-border); margin-bottom: 24px; flex-wrap: wrap; gap: 12px;
  }
  .brand { display:flex; align-items:center; gap:10px; font-size:19px; font-weight:700; color:#fff; }
  .brand .dot { width:10px; height:10px; border-radius:50%; background:linear-gradient(135deg,var(--accent),var(--accent2)); box-shadow:0 0 12px var(--accent); }
  .topbar-right { display:flex; align-items:center; gap:10px; flex-wrap: wrap; }
  select#symbol-select {
    font-size:14px; font-weight:600; padding:8px 16px; background:var(--panel); color:var(--text);
    border:1px solid var(--panel-border); border-radius:10px; cursor:pointer;
  }
  .container { padding: 0 28px; }
  h2.section-title { font-size: 13px; color: var(--text-dim); text-transform: uppercase; letter-spacing: 0.06em; margin: 28px 0 12px; font-weight: 600; }
  .grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(190px,1fr)); gap:14px; margin-bottom:18px; }
  .card {
    background: var(--panel); border: 1px solid var(--panel-border); border-radius: 18px;
    padding: 18px 20px; box-shadow: 0 8px 24px rgba(0,0,0,0.25);
  }
  .card .label { font-size: 11px; color: var(--text-dim); text-transform: uppercase; letter-spacing: 0.04em; }
  .card .value { font-size: 22px; font-weight: 700; margin-top: 6px; color: #fff; }
  .green { color: var(--green) !important; } .red { color: var(--red) !important; } .yellow { color: #fbbf24 !important; }
  .badge { display:inline-block; padding:4px 14px; border-radius:20px; font-size:12px; font-weight:700; letter-spacing:0.03em; }
  .badge.dry { background:rgba(99,102,241,0.18); color:#a5b4fc; border:1px solid rgba(99,102,241,0.35); }
  .badge.live { background:rgba(240,82,107,0.15); color:#fca5b1; border:1px solid rgba(240,82,107,0.4); }
  .badge.active { background:rgba(34,197,94,0.15); color:#86efac; border:1px solid rgba(34,197,94,0.35); }
  .badge.paused { background:rgba(251,191,36,0.15); color:#fde68a; border:1px solid rgba(251,191,36,0.35); }
  .panel-card { background: var(--panel); border: 1px solid var(--panel-border); border-radius: 20px; padding: 22px; margin-bottom: 20px; box-shadow: 0 8px 24px rgba(0,0,0,0.25); }
  form { display:grid; grid-template-columns: repeat(auto-fit, minmax(170px,1fr)); gap:14px; align-items:end; }
  label { display:block; font-size:11px; color: var(--text-dim); text-transform:uppercase; letter-spacing:0.03em; margin-bottom:6px; }
  input, select.cfg {
    width:100%; padding:9px 10px; background:#080d1c; border:1px solid var(--panel-border);
    border-radius:8px; color:var(--text); box-sizing:border-box; font-size:13px;
  }
  input:focus, select.cfg:focus { outline:none; border-color: var(--accent); }
  button {
    padding:10px 20px; background:linear-gradient(135deg,var(--accent),#2563eb); color:white; border:none;
    border-radius:10px; cursor:pointer; font-weight:700; font-size:13px; transition: transform 0.1s;
  }
  button:hover { transform: translateY(-1px); filter: brightness(1.1); }
  button.stop { background:linear-gradient(135deg,#f0526b,#dc2626); }
  button.start { background:linear-gradient(135deg,#22c55e,#15803d); }
  button.danger { background:linear-gradient(135deg,#ef4444,#b91c1c); }
  button.neutral { background:linear-gradient(135deg,#475569,#334155); }
  table { width:100%; border-collapse:collapse; font-size:13px; margin-top:6px; }
  th, td { text-align:left; padding:9px 10px; border-bottom:1px solid var(--panel-border); }
  th { color: var(--text-dim); font-weight:600; font-size:11px; text-transform:uppercase; letter-spacing:0.03em; }
  tr:hover td { background: rgba(59,130,246,0.05); }
  .warn { background:rgba(240,82,107,0.12); border:1px solid rgba(240,82,107,0.35); color:#fca5b1; padding:10px 14px; border-radius:10px; font-size:13px; margin-top:10px; display:none; }
  canvas { background: var(--panel); border: 1px solid var(--panel-border); border-radius: 18px; padding: 14px; box-shadow: 0 8px 24px rgba(0,0,0,0.25); }
  #priceChart { max-height: 420px; }
  .coin-overview { display:flex; gap:8px; flex-wrap:wrap; margin-bottom:18px; }
  .coin-pill { background: var(--panel); border:1px solid var(--panel-border); border-radius:20px; padding:6px 16px; font-size:13px; cursor:pointer; transition: border-color 0.15s; }
  .coin-pill:hover { border-color: rgba(96,165,250,0.4); }
  .coin-pill.selected { border-color: var(--accent); background: rgba(59,130,246,0.12); }
</style>
</head>
<body>
<div class="topbar">
  <div class="brand"><span class="dot"></span>⚡ GridBot <select id="symbol-select"></select></div>
  <div class="topbar-right"><a href="/copytrading" style="color:#93c5fd; text-decoration:none; font-size:13px; margin-right:14px;">📡 Copy-Trading →</a><span id="mode-badge"></span><span id="active-badge"></span></div>
</div>
<div class="container">

<div class="coin-overview" id="coin-overview"></div>

<div style="margin-bottom:20px;">
  <button id="btn-start" class="start">▶️ Start</button>
  <button id="btn-stop" class="stop">⏸️ Stop</button>
  <button id="btn-close" class="danger">✖️ Position jetzt schließen</button>
  <button id="btn-reset" class="neutral">🔄 Reset (Statistik)</button>
</div>

<h2 class="section-title">Übersicht</h2>
<div class="grid" id="status-grid"></div>

<h2 class="section-title">Kursverlauf</h2>
<canvas id="priceChart" height="400"></canvas>

<div id="obi-chart-section" style="display:none;">
  <h2 class="section-title">OBI-Verlauf (schnell / mittel / langsam)</h2>
  <canvas id="obiChart" height="250"></canvas>
</div>

<h2 class="section-title">Einstellungen (nur für den ausgewählten Coin)</h2>
<div class="panel-card">
<form id="config-form">
  <div><label>Margin (USDC)</label><input type="number" step="any" id="margin"></div>

  <div><label>Hebel</label><input type="number" step="1" id="leverage"></div>
  <div><label>Strategie</label>
    <select class="cfg" id="entry_mode">
      <option value="grid">Neutrales Grid (Ø-Einstieg/Nachkauf/TP)</option>
      <option value="ha_st">Heikin Ashi Supertrend (Buy/Sell, SL an Signalkerze)</option>
      <option value="candle_color">Kerzenfarbe (früher Einstieg, Exit bei Gegenkerze)</option>
      <option value="obi_scalp">OBI-Scalp (Orderbuch-Ungleichgewicht, symmetrisches TP/SL)</option>
    </select>
  </div>
  <div data-mode="ha_st"><label>HA-Supertrend Zeitrahmen</label>
    <select class="cfg" id="ha_st_resolution">
      <option value="1m">1 Minute</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="30m">30 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
    </select>
  </div>
  <div data-mode="ha_st"><label>HA-ATR Periode</label><input type="number" step="1" id="ha_st_atr_period"></div>
  <div data-mode="ha_st"><label>HA-ATR Multiplikator</label><input type="number" step="0.1" id="ha_st_atr_mult"></div>
  <div data-mode="ha_st"><label>Trendfilter (Long nur aufwärts, Short nur abwärts)</label>
    <select class="cfg" id="ha_st_trend_filter">
      <option value="true">An</option>
      <option value="false">Aus</option>
    </select>
  </div>
  <div data-mode="ha_st"><label>Trend-EMA Länge</label><input type="number" step="1" id="ha_st_trend_ema_length"></div>
  <div data-mode="ha_st"><label>Kerzenquelle</label>
    <select class="cfg" id="ha_st_candle_source">
      <option value="binance">Binance (mehr Liquidität, Fallback: Lighter)</option>
      <option value="lighter">Lighter (Original-Handelsdaten)</option>
    </select>
  </div>
  <div data-mode="candle_color"><label>Kerzenlänge (Sekunden)</label><input type="number" step="1" id="cc_resolution_seconds"></div>
  <div data-mode="candle_color"><label>Bestätigung nach (Sekunden)</label><input type="number" step="1" id="cc_confirm_delay_seconds"></div>
  <div data-mode="candle_color"><label>Nach Gegenkerze sofort drehen</label>
    <select class="cfg" id="cc_auto_reverse">
      <option value="true">Ja</option>
      <option value="false">Nein</option>
    </select>
  </div>
  <div data-mode="candle_color"><label>Früher Ausstieg (nicht auf Schluss warten)</label>
    <select class="cfg" id="cc_early_exit">
      <option value="true">Ja - sofort bei Gegenfarbe</option>
      <option value="false">Nein - erst bei fertigem Kerzenschluss</option>
    </select>
  </div>
  <div data-mode="obi_scalp"><label>OBI Schwelle</label><input type="number" step="0.01" id="obi_threshold"></div>
  <div data-mode="obi_scalp"><label>OBI Modus</label>
    <select class="cfg" id="obi_mode">
      <option value="momentum">Momentum (mit dem Ungleichgewicht)</option>
      <option value="mean_reversion">Mean-Reversion (dagegen, wie RSI)</option>
    </select>
  </div>
  <div data-mode="obi_scalp"><label>OBI schnell (Sek.)</label><input type="number" step="1" id="obi_window_fast_seconds"></div>
  <div data-mode="obi_scalp"><label>OBI mittel (Sek.)</label><input type="number" step="1" id="obi_window_medium_seconds"></div>
  <div data-mode="obi_scalp"><label>OBI langsam (Sek.)</label><input type="number" step="1" id="obi_window_slow_seconds"></div>
  <div data-mode="obi_scalp"><label>OBI Orderbuch-Level</label><input type="number" step="1" id="obi_levels"></div>
  <div data-mode="obi_scalp"><label>TP/SL Modus</label>
    <select class="cfg" id="obi_tp_sl_mode">
      <option value="pct">Prozent (%)</option>
      <option value="usd">Fester $-Betrag</option>
    </select>
  </div>
  <div data-mode="obi_scalp"><label>TP (%)</label><input type="number" step="any" id="obi_tp_pct"></div>
  <div data-mode="obi_scalp"><label>SL (%)</label><input type="number" step="any" id="obi_sl_pct"></div>
  <div data-mode="obi_scalp"><label>TP ($)</label><input type="number" step="any" id="obi_tp_usd"></div>
  <div data-mode="obi_scalp"><label>SL ($)</label><input type="number" step="any" id="obi_sl_usd"></div>
  <div data-mode="obi_scalp"><label>Cooldown (Sek.)</label><input type="number" step="1" id="obi_cooldown_seconds"></div>
  <div data-mode="obi_scalp"><label>Trendfilter (EMA)</label>
    <select class="cfg" id="obi_trend_filter">
      <option value="false">Aus</option>
      <option value="true">An - nur Longs über/Shorts unter EMA</option>
    </select>
  </div>
  <div data-mode="obi_scalp"><label>Trend-EMA Länge (Trades)</label><input type="number" step="1" id="obi_trend_ema_length"></div>
  <div data-mode="grid"><label>Grid-Modus</label>
    <select class="cfg" id="grid_mode">
      <option value="pct">Prozent (%)</option>
      <option value="usd">Fester $-Betrag</option>
    </select>
  </div>
  <div data-mode="grid"><label>Grid-Stufe (%)</label><input type="number" step="any" id="grid_step_pct"></div>
  <div data-mode="grid"><label>TP-Stufe (%)</label><input type="number" step="any" id="tp_step_pct"></div>
  <div data-mode="grid"><label>Grid-Stufe ($)</label><input type="number" step="any" id="grid_step_usd"></div>
  <div data-mode="grid"><label>TP-Stufe ($)</label><input type="number" step="any" id="tp_step_usd"></div>
  <div data-mode="grid"><label>Max. Nachkauf</label><input type="number" step="1" id="max_nachkauf"></div>
  <div data-mode="grid"><label>Nach TP sofort drehen</label>
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
<div style="font-size:12px; color:var(--text-dim); margin-top:8px;" id="abs-distances"></div>
</div>

<h2 class="section-title">Letzte abgeschlossene Trades</h2>
<div class="panel-card">
<table id="trades-table"><thead><tr><th>Eröffnet</th><th>Geschlossen</th><th>Seite</th><th>Ø-Einstieg</th><th>Exit</th><th>Stufen</th><th>Grund</th><th>PnL $</th></tr></thead><tbody></tbody></table>
</div>
</div>

<script>
let priceChart;
let obiChart;
let currentSymbol = null;
let allSymbols = [];

function updateModeFields() {
  const mode = document.getElementById('entry_mode').value;
  document.querySelectorAll('[data-mode]').forEach(el => {
    el.style.display = (el.dataset.mode === mode) ? '' : 'none';
  });
}
document.getElementById('entry_mode').addEventListener('change', () => {
  window.formTouched = true;
  updateModeFields();
});

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
document.getElementById('btn-close').addEventListener('click', async () => {
  if (!confirm(`Position für ${currentSymbol} jetzt zum aktuellen Preis schließen?`)) return;
  const res = await fetch(`/api/close?symbol=${currentSymbol}`, { method:'POST' });
  const data = await res.json();
  if (data.error) alert(data.error);
  refresh();
});
document.getElementById('btn-reset').addEventListener('click', async () => {
  if (!confirm(`Statistik/Trade-Log für ${currentSymbol} zurücksetzen? (nur möglich wenn flach)`)) return;
  const res = await fetch(`/api/reset?symbol=${currentSymbol}`, { method:'POST' });
  const data = await res.json();
  if (data.error) alert(data.error);
  refresh();
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
    <div class="card"><div class="label">HA-Supertrend SL (${data.config.entry_mode==='ha_st'?'aktiv':'inaktiv'})</div><div class="value red">${data.ha_st_stop_price ?? '-'}</div></div>
    <div class="card"><div class="label">Letzte Kerzenfarbe (${data.config.entry_mode==='candle_color'?'aktiv':'inaktiv'})</div><div class="value ${data.cc_last_color==='green'?'green':data.cc_last_color==='red'?'red':'yellow'}">${data.cc_last_color ?? '-'}</div></div>
    <div class="card"><div class="label">OBI schnell (${data.config.entry_mode==='obi_scalp'?'aktiv':'inaktiv'})</div><div class="value ${data.obi_fast>=0?'green':'red'}">${data.obi_fast ?? '-'}</div></div>
    <div class="card"><div class="label">OBI mittel</div><div class="value ${data.obi_medium>=0?'green':'red'}">${data.obi_medium ?? '-'}</div></div>
    <div class="card"><div class="label">OBI langsam</div><div class="value ${data.obi_slow>=0?'green':'red'}">${data.obi_slow ?? '-'}</div></div>
    <div class="card"><div class="label">Realisiert (gesamt) $</div><div class="value ${data.stats.total_pnl_usd>=0?'green':'red'}">${data.stats.total_pnl_usd}</div></div>
    <div class="card"><div class="label">Trades / Trefferquote</div><div class="value">${data.stats.trades} / ${data.stats.win_rate_pct}%</div></div>
  `;

  if (!window.formTouched) {
    document.getElementById('margin').value = data.config.margin;
    document.getElementById('leverage').value = data.config.leverage;
    document.getElementById('entry_mode').value = data.config.entry_mode;
    document.getElementById('ha_st_resolution').value = data.config.ha_st_resolution;
    document.getElementById('ha_st_atr_period').value = data.config.ha_st_atr_period;
    document.getElementById('ha_st_atr_mult').value = data.config.ha_st_atr_mult;
    document.getElementById('ha_st_trend_filter').value = String(data.config.ha_st_trend_filter);
    document.getElementById('ha_st_trend_ema_length').value = data.config.ha_st_trend_ema_length;
    document.getElementById('ha_st_candle_source').value = data.config.ha_st_candle_source;
    document.getElementById('cc_resolution_seconds').value = data.config.cc_resolution_seconds;
    document.getElementById('cc_confirm_delay_seconds').value = data.config.cc_confirm_delay_seconds;
    document.getElementById('cc_auto_reverse').value = String(data.config.cc_auto_reverse);
    document.getElementById('cc_early_exit').value = String(data.config.cc_early_exit);
    document.getElementById('obi_threshold').value = data.config.obi_threshold;
    document.getElementById('obi_mode').value = data.config.obi_mode;
    document.getElementById('obi_window_fast_seconds').value = data.config.obi_window_fast_seconds;
    document.getElementById('obi_window_medium_seconds').value = data.config.obi_window_medium_seconds;
    document.getElementById('obi_window_slow_seconds').value = data.config.obi_window_slow_seconds;
    document.getElementById('obi_levels').value = data.config.obi_levels;
    document.getElementById('obi_tp_sl_mode').value = data.config.obi_tp_sl_mode;
    document.getElementById('obi_tp_pct').value = data.config.obi_tp_pct;
    document.getElementById('obi_sl_pct').value = data.config.obi_sl_pct;
    document.getElementById('obi_tp_usd').value = data.config.obi_tp_usd;
    document.getElementById('obi_sl_usd').value = data.config.obi_sl_usd;
    document.getElementById('obi_cooldown_seconds').value = data.config.obi_cooldown_seconds;
    document.getElementById('obi_trend_filter').value = String(data.config.obi_trend_filter);
    document.getElementById('obi_trend_ema_length').value = data.config.obi_trend_ema_length;
    document.getElementById('grid_mode').value = data.config.grid_mode;
    document.getElementById('grid_step_pct').value = data.config.grid_step_pct;
    document.getElementById('tp_step_pct').value = data.config.tp_step_pct;
    document.getElementById('grid_step_usd').value = data.config.grid_step_usd;
    document.getElementById('tp_step_usd').value = data.config.tp_step_usd;
    document.getElementById('max_nachkauf').value = data.config.max_nachkauf;
    document.getElementById('dry_run').value = String(data.config.dry_run);
    document.getElementById('auto_reverse').value = String(data.config.auto_reverse);
  }
  updateModeFields();

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
    options: { responsive:true, maintainAspectRatio:false, animation:false, scales:{ x:{ display:false }, y:{ ticks:{color:'#9ca3af'} } }, plugins:{legend:{labels:{color:'#e5e7eb'}}} }
  });

  const obiSection = document.getElementById('obi-chart-section');
  if (data.config.entry_mode === 'obi_scalp' && (data.obi_history || []).length > 0) {
    obiSection.style.display = 'block';
    const obiHist = data.obi_history || [];
    const obiLabels = obiHist.map(p => new Date(p.ts).toLocaleTimeString());
    const obiDatasets = [
      { label:'Schnell', data: obiHist.map(p=>p.fast), borderColor:'#f87171', pointRadius:0, borderWidth:2 },
      { label:'Mittel', data: obiHist.map(p=>p.medium), borderColor:'#fbbf24', pointRadius:0, borderWidth:2 },
      { label:'Langsam', data: obiHist.map(p=>p.slow), borderColor:'#60a5fa', pointRadius:0, borderWidth:2 },
      { label:'Schwelle +', data: Array(obiHist.length).fill(data.config.obi_threshold), borderColor:'#4ade80', borderDash:[4,4], pointRadius:0, borderWidth:1 },
      { label:'Schwelle -', data: Array(obiHist.length).fill(-data.config.obi_threshold), borderColor:'#4ade80', borderDash:[4,4], pointRadius:0, borderWidth:1 },
      { label:'Null', data: Array(obiHist.length).fill(0), borderColor:'#4b5563', pointRadius:0, borderWidth:1 },
    ];
    if (obiChart) obiChart.destroy();
    obiChart = new Chart(document.getElementById('obiChart'), {
      type: 'line',
      data: { labels: obiLabels, datasets: obiDatasets },
      options: {
        responsive:true, maintainAspectRatio:false, animation:false,
        scales: { x:{ display:false }, y:{ min:-1, max:1, ticks:{color:'#9ca3af'} } },
        plugins:{legend:{labels:{color:'#e5e7eb'}}}
      }
    });
  } else {
    obiSection.style.display = 'none';
  }

  const trades = (data.trade_log || []).slice(-15).reverse();
  const fmtTime = (iso) => iso ? new Date(iso).toLocaleString('de-DE', {day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit'}) : '-';
  document.querySelector('#trades-table tbody').innerHTML = trades.map(t => `
    <tr><td>${fmtTime(t.opened_at)}</td><td>${fmtTime(t.closed_at)}</td><td>${t.side}</td><td>${t.avg_entry}</td><td>${t.exit}</td><td>${t.entries}</td><td>${t.reason ?? '-'}</td>
    <td class="${t.pnl_usd>=0?'green':'red'}">${t.pnl_usd}</td></tr>
  `).join('');
}

document.getElementById('config-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const payload = {
    margin: parseFloat(document.getElementById('margin').value),
    leverage: parseInt(document.getElementById('leverage').value),
    entry_mode: document.getElementById('entry_mode').value,
    ha_st_resolution: document.getElementById('ha_st_resolution').value,
    ha_st_atr_period: parseInt(document.getElementById('ha_st_atr_period').value),
    ha_st_atr_mult: parseFloat(document.getElementById('ha_st_atr_mult').value),
    ha_st_trend_filter: document.getElementById('ha_st_trend_filter').value === 'true',
    ha_st_trend_ema_length: parseInt(document.getElementById('ha_st_trend_ema_length').value),
    ha_st_candle_source: document.getElementById('ha_st_candle_source').value,
    cc_resolution_seconds: parseInt(document.getElementById('cc_resolution_seconds').value),
    cc_confirm_delay_seconds: parseInt(document.getElementById('cc_confirm_delay_seconds').value),
    cc_auto_reverse: document.getElementById('cc_auto_reverse').value === 'true',
    cc_early_exit: document.getElementById('cc_early_exit').value === 'true',
    obi_threshold: parseFloat(document.getElementById('obi_threshold').value),
    obi_mode: document.getElementById('obi_mode').value,
    obi_window_fast_seconds: parseFloat(document.getElementById('obi_window_fast_seconds').value),
    obi_window_medium_seconds: parseFloat(document.getElementById('obi_window_medium_seconds').value),
    obi_window_slow_seconds: parseFloat(document.getElementById('obi_window_slow_seconds').value),
    obi_levels: parseInt(document.getElementById('obi_levels').value),
    obi_tp_sl_mode: document.getElementById('obi_tp_sl_mode').value,
    obi_tp_pct: parseFloat(document.getElementById('obi_tp_pct').value),
    obi_sl_pct: parseFloat(document.getElementById('obi_sl_pct').value),
    obi_tp_usd: parseFloat(document.getElementById('obi_tp_usd').value),
    obi_sl_usd: parseFloat(document.getElementById('obi_sl_usd').value),
    obi_cooldown_seconds: parseFloat(document.getElementById('obi_cooldown_seconds').value),
    obi_trend_filter: document.getElementById('obi_trend_filter').value === 'true',
    obi_trend_ema_length: parseInt(document.getElementById('obi_trend_ema_length').value),
    grid_mode: document.getElementById('grid_mode').value,
    grid_step_pct: parseFloat(document.getElementById('grid_step_pct').value),
    tp_step_pct: parseFloat(document.getElementById('tp_step_pct').value),
    grid_step_usd: parseFloat(document.getElementById('grid_step_usd').value),
    tp_step_usd: parseFloat(document.getElementById('tp_step_usd').value),
    max_nachkauf: parseInt(document.getElementById('max_nachkauf').value),
    dry_run: document.getElementById('dry_run').value === 'true',
    auto_reverse: document.getElementById('auto_reverse').value === 'true',
  };
  await fetch(`/api/config?symbol=${currentSymbol}`, { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(payload) });
  window.formTouched = false;
  alert(`Gespeichert für ${currentSymbol}!`);
});

['margin','leverage','entry_mode','ha_st_resolution','ha_st_atr_period','ha_st_atr_mult','ha_st_trend_filter','ha_st_trend_ema_length','ha_st_candle_source','cc_resolution_seconds','cc_confirm_delay_seconds','cc_auto_reverse','cc_early_exit','obi_threshold','obi_mode','obi_window_fast_seconds','obi_window_medium_seconds','obi_window_slow_seconds','obi_levels','obi_tp_sl_mode','obi_tp_pct','obi_sl_pct','obi_tp_usd','obi_sl_usd','obi_cooldown_seconds','obi_trend_filter','obi_trend_ema_length','grid_mode','grid_step_pct','tp_step_pct','grid_step_usd','tp_step_usd','max_nachkauf','dry_run','auto_reverse'].forEach(id => {
  document.getElementById(id).addEventListener('input', (e) => {
    window.formTouched = true;
    if (typeof e.target.value === 'string' && e.target.value.includes(',')) {
      e.target.value = e.target.value.replace(',', '.');
    }
  });
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
        "ha_st_stop_price": st.get("ha_st_stop_price"),
        "cc_last_color": st.get("cc_last_color"),
        "obi_current": st.get("obi_current"), "obi_fast": st.get("obi_fast"),
        "obi_medium": st.get("obi_medium"), "obi_slow": st.get("obi_slow"),
        "obi_history": st.get("obi_history", [])[-300:],
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
    for key in ["margin", "leverage", "entry_mode", "grid_mode", "grid_step_pct", "tp_step_pct",
                "grid_step_usd", "tp_step_usd", "max_nachkauf", "dry_run", "auto_reverse",
                "ha_st_resolution", "ha_st_atr_period", "ha_st_atr_mult",
                "ha_st_trend_filter", "ha_st_trend_ema_length", "ha_st_candle_source",
                "cc_resolution_seconds", "cc_confirm_delay_seconds", "cc_auto_reverse", "cc_early_exit",
                "obi_threshold", "obi_mode", "obi_window_fast_seconds", "obi_window_medium_seconds", "obi_window_slow_seconds", "obi_levels", "obi_tp_sl_mode", "obi_tp_pct", "obi_sl_pct", "obi_tp_usd", "obi_sl_usd",
                "obi_cooldown_seconds", "obi_trend_filter", "obi_trend_ema_length"]:
        if key in body:
            cfg[key] = body[key]
    debug_log(f"⚙️ [{symbol}] Konfiguration aktualisiert", cfg)
    await save_bot_configs()
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


async def handle_close_position(request):
    """Manuelles sofortiges Schliessen der offenen Position (Market-Order, egal ob TP/SL erreicht)."""
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    st = BOTS[symbol]["state"]
    if st["position"] is None:
        return web.json_response({"error": "keine offene Position"}, status=400)
    if st["last_price"] is None:
        return web.json_response({"error": "kein aktueller Preis bekannt"}, status=400)
    await execute_exit(symbol, st["last_price"], "MANUAL")
    return web.json_response({"success": True})


async def handle_reset(request):
    """Setzt Statistik/Trade-Log/Anker zurueck - nur erlaubt wenn der Bot gerade flach ist."""
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    st = BOTS[symbol]["state"]
    if st["position"] is not None:
        return web.json_response({"error": "Position ist noch offen - erst schliessen, dann reset"}, status=400)
    st["stats"] = {"trades": 0, "wins": 0, "losses": 0, "total_pnl_usd": 0.0}
    st["trade_log"] = []
    st["anchor_price"] = st["last_price"]
    st["entry_count"] = 0
    debug_log(f"🔄 [{symbol}] Zurückgesetzt (Statistik, Trade-Log, neuer Anker)")
    return web.json_response({"success": True})


