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
import secrets
import base64
import traceback
from datetime import datetime, timedelta
from aiohttp import web

try:
    from zoneinfo import ZoneInfo
    DISPLAY_TZ = ZoneInfo("Europe/Berlin")
except Exception:
    DISPLAY_TZ = None


def _last_sunday(year, month):
    next_month_first = datetime(year + 1, 1, 1) if month == 12 else datetime(year, month + 1, 1)
    last_day = next_month_first - timedelta(days=1)
    return last_day - timedelta(days=(last_day.weekday() - 6) % 7)


def _eu_dst_active(utc_naive_dt):
    """EU-Sommerzeitregel (gilt fuer Deutschland): letzter Sonntag Maerz 01:00 UTC bis
    letzter Sonntag Oktober 01:00 UTC. Fallback ohne tzdata-Paket - falls zoneinfo im
    Container aus irgendeinem Grund fehlschlaegt (z.B. schlankes Docker-Image ohne tzdata),
    damit die Zeitzone NIE unbemerkt auf UTC zurueckfaellt."""
    year = utc_naive_dt.year
    dst_start = _last_sunday(year, 3).replace(hour=1)
    dst_end = _last_sunday(year, 10).replace(hour=1)
    return dst_start <= utc_naive_dt < dst_end


def now_local():
    """Render-Server laufen in UTC - datetime.now() alleine wuerde also 2h (Sommerzeit) bzw.
    1h (Winterzeit) hinter der deutschen TradingView-Chartzeit liegen. Alle Trade-Zeitstempel
    nutzen diese Funktion, damit sie 1:1 mit dem Chart vergleichbar sind."""
    if DISPLAY_TZ is not None:
        return datetime.now(DISPLAY_TZ)
    utc_now = datetime.utcnow()
    offset_hours = 2 if _eu_dst_active(utc_now) else 1
    return utc_now + timedelta(hours=offset_hours)

try:
    import redis.asyncio as redis_lib
except ImportError:
    redis_lib = None

BASE_URL = "https://mainnet.zklighter.elliot.ai"
WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"

DEBUG_MODE = os.getenv("DEBUG_MODE", "true").lower() == "true"


def debug_log(msg, data=None):
    if DEBUG_MODE:
        timestamp = now_local().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        print(f"[DEBUG {timestamp}] {msg}", flush=True)
        if data:
            print(f"   DATA: {json.dumps(data, indent=2, default=str)}", flush=True)


MARKET_INDICES = {
    "ETH": 0, "BTC": 1, "SOL": 2, "DOGE": 3, "XRP": 7, "LINK": 8, "AVAX": 9,
    "NEAR": 10, "DOT": 11, "GRAM": 12, "SUI": 16, "BNB": 25, "UNI": 30, "APT": 31,
    "ADA": 39, "TRX": 43, "LTC": 35, "BCH": 58, "HBAR": 59, "ICP": 102, "HYPE": 24,
    "EURUSD": 96, "GBPUSD": 97, "USDJPY": 98, "USDCHF": 99, "USDCAD": 100,
    "AUDUSD": 106, "NZDUSD": 107, "USDKRW": 105,
    "XAU": 92, "XAG": 93, "WTI": 145,
    # ACHTUNG: "TON" wurde entfernt - market_id 12 gehoert auf Lighter inzwischen zu "GRAM",
    # nicht mehr zu TON. Falls TON weiterhin gehandelt werden soll, zuerst bei Lighter die
    # aktuelle market_id fuer TON pruefen (apidocs.lighter.xyz -> /api/v1/orderBooks) und hier
    # neu eintragen - NICHT einfach wieder auf 12 setzen, das ist jetzt ein anderer Coin!
}
PRECISION_MAP = {
    # Werte 1:1 von der Lighter-API (/api/v1/orderBooks, supported_size_decimals) uebernommen,
    # Precision = 10 ** supported_size_decimals. Zuletzt geprueft: siehe Chat-Verlauf.
    "ETH": 10000, "BTC": 100000, "SOL": 1000, "DOGE": 1, "XRP": 1, "LINK": 10, "AVAX": 100,
    "NEAR": 10, "DOT": 10, "GRAM": 10, "SUI": 10, "BNB": 100, "UNI": 100, "APT": 100,
    "ADA": 10, "TRX": 10, "LTC": 1000, "BCH": 1000, "HBAR": 10, "ICP": 100, "HYPE": 100,
    "EURUSD": 10, "GBPUSD": 10, "USDJPY": 1000, "USDCHF": 10, "USDCAD": 10,
    "AUDUSD": 10, "NZDUSD": 10, "USDKRW": 10000, "XAU": 10000, "XAG": 100, "WTI": 1000,
}
PRICE_DECIMALS_MAP = {
    "ETH": 2, "BTC": 1, "SOL": 3, "DOGE": 6, "XRP": 6, "LINK": 5, "AVAX": 4,
    "NEAR": 5, "DOT": 5, "GRAM": 5, "SUI": 5, "BNB": 4, "UNI": 4, "APT": 4,
    "ADA": 5, "TRX": 5, "LTC": 3, "BCH": 3, "HBAR": 5, "ICP": 4, "HYPE": 4,
    "EURUSD": 5, "GBPUSD": 5, "USDJPY": 3, "USDCHF": 5, "USDCAD": 5,
    "AUDUSD": 5, "NZDUSD": 5, "USDKRW": 2, "XAU": 2, "XAG": 4, "WTI": 3,
}
MIN_BASE_AMOUNT_MAP = {
    "ETH": 0.005, "BTC": 0.0001, "SOL": 0.1, "DOGE": 100.0, "XRP": 7.0, "LINK": 1.0, "AVAX": 1.0,
    "NEAR": 4.0, "DOT": 9.5, "GRAM": 5.0, "SUI": 10.0, "BNB": 0.02, "UNI": 2.0, "APT": 10.0,
    "ADA": 45.0, "TRX": 25.0, "LTC": 0.15, "BCH": 0.035, "HBAR": 100.0, "ICP": 3.5, "HYPE": 0.15,
    "EURUSD": 6.5, "GBPUSD": 5.5, "USDJPY": 0.05, "USDCHF": 8.0, "USDCAD": 5.5,
    "AUDUSD": 10.0, "NZDUSD": 10.0, "USDKRW": 0.005, "XAU": 0.002, "XAG": 0.15, "WTI": 0.1,
}


def get_precision(symbol):
    return PRECISION_MAP.get(symbol, 10000)


def get_price_decimals(symbol):
    return PRICE_DECIMALS_MAP.get(symbol, 2)


def get_min_base_amount(symbol):
    return MIN_BASE_AMOUNT_MAP.get(symbol, 0.001)


PORT = int(os.getenv("PORT", "10000"))

# ========== DASHBOARD-ZUGANGSSCHUTZ ==========
# Ohne das ist das Dashboard fuer jeden mit dem Link offen einsehbar UND bedienbar
# (Config aendern, Positionen schliessen, Bot stoppen). Passwort per Env-Var DASHBOARD_PASSWORD
# setzen (in Render unter "Environment"), sonst wird bei jedem Start ein zufaelliges Passwort
# generiert und einmalig ins Log geschrieben - dann aber bei jedem Neustart/Redeploy ein anderes!
# Fuer dauerhaften, gleichbleibenden Zugriff DASHBOARD_PASSWORD unbedingt in Render setzen.
DASHBOARD_USERNAME = os.getenv("DASHBOARD_USERNAME", "admin")
DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD")
DASHBOARD_PASSWORD_GENERATED = False
if not DASHBOARD_PASSWORD:
    DASHBOARD_PASSWORD = secrets.token_urlsafe(12)
    DASHBOARD_PASSWORD_GENERATED = True


@web.middleware
async def basic_auth_middleware(request, handler):
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Basic "):
        try:
            decoded = base64.b64decode(auth_header[6:]).decode("utf-8")
            username, _, password = decoded.partition(":")
        except Exception:
            username, password = "", ""
        if secrets.compare_digest(username, DASHBOARD_USERNAME) and secrets.compare_digest(password, DASHBOARD_PASSWORD):
            return await handler(request)
    return web.Response(
        status=401,
        headers={"WWW-Authenticate": 'Basic realm="Trading Bot Dashboard"'},
        text="401 Unauthorized - Dashboard ist passwortgeschuetzt",
    )

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
        "entry_mode": os.getenv("ENTRY_MODE", "grid"),  # "grid", "obi_scalp" oder "macd_stoch"
        "grid_mode": os.getenv("GRID_MODE", "pct"),  # "pct" oder "usd"
        "grid_step_pct": float(os.getenv("GRID_STEP_PCT", "0.25")),
        "tp_step_pct": float(os.getenv("TP_STEP_PCT", "0.25")),
        "grid_step_usd": float(os.getenv("GRID_STEP_USD", "150")),
        "tp_step_usd": float(os.getenv("TP_STEP_USD", "150")),
        "max_nachkauf": int(os.getenv("MAX_NACHKAUF", "5")),
        "bot_active": True,
        "auto_reverse": os.getenv("AUTO_REVERSE", "true").lower() == "true",
        "obi_threshold": float(os.getenv("OBI_THRESHOLD", "0.30")),
        "obi_mode": os.getenv("OBI_MODE", "momentum"),  # "momentum" (mit dem Ungleichgewicht), "mean_reversion" (dagegen) oder "reversal" (separater Long/Short-Einstieg bei Umkehr aus Extremzone)
        "obi_long_threshold": float(os.getenv("OBI_LONG_THRESHOLD", "0.20")),  # nur Reversal-Modus: Long-Zone ab OBI <= -Wert
        "obi_short_threshold": float(os.getenv("OBI_SHORT_THRESHOLD", "0.30")),  # nur Reversal-Modus: Short-Zone ab OBI >= +Wert
        "obi_reversal_min_bounce": float(os.getenv("OBI_REVERSAL_MIN_BOUNCE", "0.05")),
        "obi_window_fast_seconds": float(os.getenv("OBI_WINDOW_FAST_SECONDS", "5")),
        "obi_window_medium_seconds": float(os.getenv("OBI_WINDOW_MEDIUM_SECONDS", "20")),
        "obi_window_slow_seconds": float(os.getenv("OBI_WINDOW_SLOW_SECONDS", "60")),
        "obi_levels": int(os.getenv("OBI_LEVELS", "15")),
        "obi_depth_weighting_enabled": os.getenv("OBI_DEPTH_WEIGHTING_ENABLED", "false").lower() == "true",
        "obi_use_median": os.getenv("OBI_USE_MEDIAN", "false").lower() == "true",
        "obi_min_liquidity": float(os.getenv("OBI_MIN_LIQUIDITY", "0")),
        "obi_breakeven_enabled": os.getenv("OBI_BREAKEVEN_ENABLED", "false").lower() == "true",
        "obi_breakeven_trigger_ratio": float(os.getenv("OBI_BREAKEVEN_TRIGGER_RATIO", "0.5")),
        "obi_breakeven_lock_usd": float(os.getenv("OBI_BREAKEVEN_LOCK_USD", "0.1")),
        "obi_breakeven_lock_pct": float(os.getenv("OBI_BREAKEVEN_LOCK_PCT", "0.1")),
        "obi_instant_reset_ratio": float(os.getenv("OBI_INSTANT_RESET_RATIO", "0.5")),
        "obi_tp_sl_mode": os.getenv("OBI_TP_SL_MODE", "pct"),  # "pct" oder "usd"
        "obi_tp_pct": float(os.getenv("OBI_TP_PCT", "0.15")),
        "obi_sl_pct": float(os.getenv("OBI_SL_PCT", "0.15")),
        "obi_tp_usd": float(os.getenv("OBI_TP_USD", "1")),
        "obi_sl_usd": float(os.getenv("OBI_SL_USD", "1")),
        "obi_cooldown_seconds": float(os.getenv("OBI_COOLDOWN_SECONDS", "7")),
        "obi_trend_filter": os.getenv("OBI_TREND_FILTER", "false").lower() == "true",
        "obi_trend_ema_length": int(os.getenv("OBI_TREND_EMA_LENGTH", "300")),
        "macd_resolution": os.getenv("MACD_RESOLUTION", "1m"),  # "1m", "5m" oder "15m"
        "macd_fast_fast": int(os.getenv("MACD_FAST_FAST", "13")),
        "macd_fast_slow": int(os.getenv("MACD_FAST_SLOW", "21")),
        "macd_fast_signal": int(os.getenv("MACD_FAST_SIGNAL", "9")),
        "macd_slow_fast": int(os.getenv("MACD_SLOW_FAST", "34")),
        "macd_slow_slow": int(os.getenv("MACD_SLOW_SLOW", "144")),
        "macd_slow_signal": int(os.getenv("MACD_SLOW_SIGNAL", "9")),
        "macd_use_stochastic": os.getenv("MACD_USE_STOCHASTIC", "true").lower() == "true",
        "stoch_k_period": int(os.getenv("STOCH_K_PERIOD", "7")),
        "stoch_k_smooth": int(os.getenv("STOCH_K_SMOOTH", "3")),
        "stoch_d_period": int(os.getenv("STOCH_D_PERIOD", "3")),
        "macd_tp_usd": float(os.getenv("MACD_TP_USD", "1")),
        "macd_sl_usd": float(os.getenv("MACD_SL_USD", "1")),
        "macd_fast_fade_exit_enabled": os.getenv("MACD_FAST_FADE_EXIT_ENABLED", "false").lower() == "true",
        "macd_slow_fade_exit_enabled": os.getenv("MACD_SLOW_FADE_EXIT_ENABLED", "false").lower() == "true",
        "macd_slow_reversal_exit_enabled": os.getenv("MACD_SLOW_REVERSAL_EXIT_ENABLED", "true").lower() == "true",
        "macd_fast_zero_cross_exit_enabled": os.getenv("MACD_FAST_ZERO_CROSS_EXIT_ENABLED", "true").lower() == "true",
        "fib_resolution": os.getenv("FIB_RESOLUTION", "1h"),  # "1h" oder "4h"
        "fib_lookback_candles": int(os.getenv("FIB_LOOKBACK_CANDLES", "100")),
        "fib_entry1_level": float(os.getenv("FIB_ENTRY1_LEVEL", "0.882")),
        "fib_entry2_level": float(os.getenv("FIB_ENTRY2_LEVEL", "0.941")),
        "fib_tp1_level": float(os.getenv("FIB_TP1_LEVEL", "0.786")),
        "fib_tp2_level": float(os.getenv("FIB_TP2_LEVEL", "0.667")),
        "fib_sl_level": float(os.getenv("FIB_SL_LEVEL", "1.0")),
        "fib_tp1_close_pct": float(os.getenv("FIB_TP1_CLOSE_PCT", "50")),
        "fib_cooldown_seconds": float(os.getenv("FIB_COOLDOWN_SECONDS", "300")),
        "stoch_cross_resolution": os.getenv("STOCH_CROSS_RESOLUTION", "1m"),  # "1m", "2m", "5m" oder "15m"
        "stoch_cross_k_period": int(os.getenv("STOCH_CROSS_K_PERIOD", "7")),
        "stoch_cross_k_smooth": int(os.getenv("STOCH_CROSS_K_SMOOTH", "3")),
        "stoch_cross_d_period": int(os.getenv("STOCH_CROSS_D_PERIOD", "3")),
        "stoch_cross_oversold": float(os.getenv("STOCH_CROSS_OVERSOLD", "20")),
        "stoch_cross_overbought": float(os.getenv("STOCH_CROSS_OVERBOUGHT", "80")),
        "stoch_cross_tp_usd": float(os.getenv("STOCH_CROSS_TP_USD", "3")),
        "stoch_cross_sl_usd": float(os.getenv("STOCH_CROSS_SL_USD", "3")),
        "stoch_cross_trend_filter_enabled": os.getenv("STOCH_CROSS_TREND_FILTER_ENABLED", "false").lower() == "true",
        "stoch_cross_trend_ema_period": int(os.getenv("STOCH_CROSS_TREND_EMA_PERIOD", "200")),
        "stoch_cross_sl_tp_mode": os.getenv("STOCH_CROSS_SL_TP_MODE", "fixed"),  # "fixed" oder "atr"
        "stoch_cross_atr_period": int(os.getenv("STOCH_CROSS_ATR_PERIOD", "14")),
        "stoch_cross_sl_atr_mult": float(os.getenv("STOCH_CROSS_SL_ATR_MULT", "1.5")),
        "stoch_cross_tp_atr_mult": float(os.getenv("STOCH_CROSS_TP_ATR_MULT", "1.5")),
        "stoch_cross_rp_filter_enabled": os.getenv("STOCH_CROSS_RP_FILTER_ENABLED", "false").lower() == "true",
        "stoch_cross_rp_lookback": int(os.getenv("STOCH_CROSS_RP_LOOKBACK", "110")),
        "stoch_cross_require_squeeze": os.getenv("STOCH_CROSS_REQUIRE_SQUEEZE", "false").lower() == "true",
        "stoch_cross_squeeze_lookback": int(os.getenv("STOCH_CROSS_SQUEEZE_LOOKBACK", "50")),
        "stoch_cross_squeeze_threshold_pct": float(os.getenv("STOCH_CROSS_SQUEEZE_THRESHOLD_PCT", "70")),
        "rp_mode": os.getenv("RP_MODE", "reversion"),  # "reversion" (empfohlen) oder "momentum"
        "rp_resolution": os.getenv("RP_RESOLUTION", "1m"),
        "rp_lookback": int(os.getenv("RP_LOOKBACK", "110")),
        "rp_ob_os_level": float(os.getenv("RP_OB_OS_LEVEL", "80")),
        "rp_tp_usd": float(os.getenv("RP_TP_USD", "3")),
        "rp_sl_usd": float(os.getenv("RP_SL_USD", "3")),
        "rp_breakeven_enabled": os.getenv("RP_BREAKEVEN_ENABLED", "false").lower() == "true",
        "rp_breakeven_trigger_usd": float(os.getenv("RP_BREAKEVEN_TRIGGER_USD", "3")),
        "rp_breakeven_lock_usd": float(os.getenv("RP_BREAKEVEN_LOCK_USD", "0.5")),
        "rp_squeeze_lookback": int(os.getenv("RP_SQUEEZE_LOOKBACK", "50")),
        "rp_squeeze_threshold_pct": float(os.getenv("RP_SQUEEZE_THRESHOLD_PCT", "70")),
        "macd_simple_resolution": os.getenv("MACD_SIMPLE_RESOLUTION", "30s"),  # "30s","1m","2m","5m","15m"
        "macd_simple_fast": int(os.getenv("MACD_SIMPLE_FAST", "13")),
        "macd_simple_slow": int(os.getenv("MACD_SIMPLE_SLOW", "21")),
        "macd_simple_signal": int(os.getenv("MACD_SIMPLE_SIGNAL", "9")),
        "macd_simple_tp_usd": float(os.getenv("MACD_SIMPLE_TP_USD", "3")),
        "macd_simple_sl_usd": float(os.getenv("MACD_SIMPLE_SL_USD", "3")),
        "macd_simple_exit_mode": os.getenv("MACD_SIMPLE_EXIT_MODE", "tp_sl"),  # "tp_sl" oder "reverse"
        "macd_simple_early_exit_enabled": os.getenv("MACD_SIMPLE_EARLY_EXIT_ENABLED", "false").lower() == "true",
        "macd_simple_early_exit_bars": int(os.getenv("MACD_SIMPLE_EARLY_EXIT_BARS", "3")),
        "macd_simple_early_exit_reverse": os.getenv("MACD_SIMPLE_EARLY_EXIT_REVERSE", "false").lower() == "true",
        "macd_simple_breakeven_enabled": os.getenv("MACD_SIMPLE_BREAKEVEN_ENABLED", "false").lower() == "true",
        "macd_simple_breakeven_trigger_usd": float(os.getenv("MACD_SIMPLE_BREAKEVEN_TRIGGER_USD", "3")),
        "macd_simple_breakeven_lock_usd": float(os.getenv("MACD_SIMPLE_BREAKEVEN_LOCK_USD", "0.5")),
        "rp_require_squeeze": os.getenv("RP_REQUIRE_SQUEEZE", "false").lower() == "true",
        "cf_resolution": os.getenv("CF_RESOLUTION", "10s"),  # "10s","15s","30s","1m","2m","5m"
        "cf_min_body_pct": float(os.getenv("CF_MIN_BODY_PCT", "0")),
        "cf_sl_usd": float(os.getenv("CF_SL_USD", "5")),
    }


def default_state():
    return {
        "position": None, "avg_entry_price": None, "total_coin_size": 0.0,
        "entry_count": 0, "anchor_price": None, "last_price": None,
        "price_history": [],
        "position_opened_at": None,
        "obi_book": {"bids": {}, "asks": {}}, "obi_avg_buffer": [], "obi_last_signal_direction": None,
        "obi_breakeven_triggered": False,
        "obi_instant_armed_short": True, "obi_instant_armed_long": True,
        "obi_fast": None, "obi_medium": None, "obi_slow": None, "obi_history": [],
        "last_entry_price": None,
        "obi_last_trade_time": 0.0, "obi_trend_ema": None, "obi_current": None,
        "obi_extreme_zone": None, "obi_extreme_value": None, "obi_prev_fast": None,
        "macd_fast_hist": None, "macd_slow_hist": None, "macd_stoch_k": None, "macd_stoch_d": None,
        "macd_fade_exit": False, "macd_slow_fade_exit": False, "macd_slow_reversal_exit": False,
        "macd_fast_zero_cross_exit": False,
        "macd_fast_ema_fast_val": None, "macd_fast_ema_slow_val": None, "macd_fast_signal_val": None,
        "fib": None, "fib_entry1_done": False, "fib_entry2_done": False, "fib_tp1_done": False,
        "fib_sl_active_price": None, "fib_last_trade_time": 0.0,
        "stoch_cross_k": None, "stoch_cross_d": None,
        "stoch_cross_sl_price": None, "stoch_cross_tp_price": None,
        "stoch_cross_rp_mid": None, "stoch_cross_channel_width": None, "stoch_cross_avg_width": None,
        "stoch_cross_squeeze_active": False, "stoch_cross_width_history": [],
        "rp_osc": None, "rp_mid_price": None, "rp_range_high": None, "rp_range_low": None,
        "rp_breakeven_triggered": False,
        "rp_width_history": [], "rp_channel_width": None, "rp_avg_width": None,
        "rp_squeeze_active": False, "rp_squeeze_was_active": False,
        "macd_simple_hist": None, "macd_simple_hist_history": [],
        "macd_simple_breakeven_triggered": False,
        "binance_1s_buffer": [],
        "local_1s_bucket_start": None, "local_1s_candle_open": None,
        "local_1s_candle_high": None, "local_1s_candle_low": None, "local_1s_candle_last": None,
        "local_1s_buffer": [],
        "cf_last_color": None, "cf_last_body_pct": None,
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
        st["position_opened_at"] = now_local().isoformat()

    st["last_entry_price"] = price
    st["entry_count"] += 1
    debug_log(f"📈 [{symbol}] {'Nachkauf' if is_add_on else 'Neue Position'}: {direction.upper()} @ {price} | Ø-Einstieg {round(st['avg_entry_price'], 2)} | Stufe {st['entry_count']}")
    return True


async def execute_partial_exit(symbol, price, fraction, reason):
    """Schliesst nur einen Teil der Position (z.B. 0.5 = 50%), Rest bleibt offen mit
    unveraendertem Ø-Einstiegspreis. Zaehlt NICHT in stats.trades/wins/losses, damit die
    Trefferquote nicht durch Teilverkaeufe verzerrt wird - nur der PnL wird verbucht."""
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    market_index = MARKET_INDICES[symbol]

    if st["position"] is None or st["total_coin_size"] <= 0:
        return False

    close_size = st["total_coin_size"] * fraction
    position_side = st["position"]
    pnl_usd = (price - st["avg_entry_price"]) * close_size if position_side == "long" else (st["avg_entry_price"] - price) * close_size

    if not cfg["dry_run"]:
        client = get_lighter_client()
        if client is None:
            debug_log(f"⚠️ [{symbol}] Kein Lighter-Client - Teil-Exit übersprungen")
            return False
        precision = get_precision(symbol)
        base_amount = int(round(close_size * precision))
        min_base = get_min_base_amount(symbol)
        if base_amount * (1 / precision) < min_base:
            debug_log(f"⚠️ [{symbol}] Teil-Exit-Größe unter Mindestgröße - übersprungen")
            await client.close()
            return False
        is_ask = position_side == "long"
        tx, tx_hash, err = await place_market_order(client, market_index, symbol, is_ask, base_amount, price, reduce_only=True)
        await client.close()
        if err:
            debug_log(f"⚠️ [{symbol}] Teil-Exit-Order fehlgeschlagen", {"error": str(err)})
            return False

    st["stats"]["total_pnl_usd"] += pnl_usd
    st["trade_log"].append({
        "side": position_side, "avg_entry": round(st["avg_entry_price"], 2), "exit": price,
        "entries": st["entry_count"], "pnl_usd": round(pnl_usd, 3),
        "opened_at": st.get("position_opened_at"), "closed_at": now_local().isoformat(),
        "reason": reason, "partial": True, "fraction": fraction,
    })

    st["total_coin_size"] -= close_size
    debug_log(f"✂️ [{symbol}] Teil-Exit ({reason}): {position_side.upper()} {round(fraction*100)}% @ {price} | PnL ${round(pnl_usd,3)} | Rest {round(st['total_coin_size'],6)}")
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
        "opened_at": st.get("position_opened_at"), "closed_at": now_local().isoformat(), "reason": reason,
    })

    debug_log(f"🏁 [{symbol}] Position geschlossen ({reason}): {st['position'].upper()} Ø{round(st['avg_entry_price'],2)} -> {price} | PnL ${round(pnl_usd,3)}")

    st["position"] = None
    st["avg_entry_price"] = None
    st["total_coin_size"] = 0.0
    st["entry_count"] = 0
    st["anchor_price"] = price
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
<div style="position:relative; height:400px;"><canvas id="priceChart"></canvas></div>

<div id="pocket-trading-section" style="display:none;">
  <h2 class="section-title">⚡ Pocket-Trading (manuell, läuft parallel zur Automatik)</h2>
  <div class="panel-card">
    <div style="display:flex; gap:24px; flex-wrap:wrap; margin-bottom:16px;">
      <div><div class="label">Margin (nächster Klick)</div><div class="value" id="pocket-margin">-</div></div>
      <div><div class="label">Position</div><div class="value" id="pocket-position">-</div></div>
      <div><div class="label">Ø-Einstieg</div><div class="value" id="pocket-entry">-</div></div>
      <div><div class="label">Unrealisiert $</div><div class="value" id="pocket-pnl">-</div></div>
    </div>
    <div style="display:flex; gap:12px; margin-bottom:18px;">
      <button id="btn-manual-buy" style="flex:1; padding:24px 10px; font-size:20px; font-weight:700; background:#16a34a; color:white; border:none; border-radius:14px; cursor:pointer;">⬆️ BUY</button>
      <button id="btn-manual-sell" style="flex:1; padding:24px 10px; font-size:20px; font-weight:700; background:#dc2626; color:white; border:none; border-radius:14px; cursor:pointer;">⬇️ SELL</button>
      <button id="btn-manual-tp" style="flex:1; padding:24px 10px; font-size:20px; font-weight:700; background:#2563eb; color:white; border:none; border-radius:14px; cursor:pointer;">✅ TP</button>
    </div>
    <div class="label" style="margin-bottom:6px;">Letzte 10 Kerzen (aus dem Live-Preis-Tick zusammengesetzt)</div>
    <div id="mini-candles" style="display:flex; gap:4px; align-items:center; height:70px;"></div>
  </div>
</div>

<div id="obi-chart-section" style="display:none;">
  <h2 class="section-title">OBI-Verlauf (schnell / mittel / langsam)</h2>
  <div style="position:relative; height:250px;"><canvas id="obiChart"></canvas></div>
</div>

<div id="macd-simple-chart-section" style="display:none;">
  <h2 class="section-title">MACD-Simple Histogramm-Verlauf</h2>
  <div style="position:relative; height:250px;"><canvas id="macdSimpleChart"></canvas></div>
</div>

<h2 class="section-title">Einstellungen (nur für den ausgewählten Coin)</h2>
<div class="panel-card">
<form id="config-form">
  <div><label>Margin (USDC)</label><input type="number" step="any" id="margin"></div>

  <div><label>Hebel</label><input type="number" step="1" id="leverage"></div>
  <div><label>Strategie</label>
    <select class="cfg" id="entry_mode">
      <option value="grid">Neutrales Grid (Ø-Einstieg/Nachkauf/TP)</option>
      <option value="obi_scalp">OBI-Scalp (Orderbuch-Ungleichgewicht, symmetrisches TP/SL)</option>
      <option value="macd_stoch">MACD-Dual + Stochastic (Histogramm-Trend, optional Stochastic-Filter)</option>
      <option value="fib_reversal">Fibonacci-Reversal (Einstieg 0.882/0.941, TP 0.786/0.667, SL 1.0)</option>
      <option value="stoch_cross">Stochastic-Cross (unter 20 = Long, über 80 = Short, fester TP/SL)</option>
      <option value="range_profile">Range-Profile (Point-of-Control-Kanal, Reversion oder Momentum, ATR-SL)</option>
      <option value="macd_simple">MACD-Simple (schnelles Histogramm wird grün/rot = Buy/Sell, fester TP/SL, auch 30s)</option>
      <option value="candle_flip">Candle-Flip (immer im Markt, dreht bei jedem Kerzenfarbwechsel, nur SL als Schutz)</option>
    </select>
  </div>
  <div data-mode="obi_scalp"><label>OBI Schwelle</label><input type="number" step="0.01" id="obi_threshold"></div>
  <div data-mode="obi_scalp"><label>OBI Modus</label>
    <select class="cfg" id="obi_mode">
      <option value="momentum">Momentum (mit dem Ungleichgewicht)</option>
      <option value="mean_reversion">Mean-Reversion (dagegen, wie RSI)</option>
      <option value="reversal">Reversal (separater Long/Short-Einstieg bei Umkehr aus Extremzone)</option>
      <option value="reversal_instant">Reversal-Sofort (getrennte Long/Short-Schwellen, sofort bei Durchbruch, ohne Rückprall-Wartezeit)</option>
    </select>
  </div>
  <div data-mode="obi_scalp"><label>Reversal OBI-Wert Long (überverkauft, negativ)</label><input type="number" step="0.01" id="obi_long_threshold"></div>
  <div data-mode="obi_scalp"><label>Reversal OBI-Wert Short (überkauft, positiv)</label><input type="number" step="0.01" id="obi_short_threshold"></div>
  <div data-mode="obi_scalp"><label>Reversal Rückprall-Schwelle</label><input type="number" step="0.01" id="obi_reversal_min_bounce"></div>
  <div data-mode="obi_scalp"><label>Reversal-Sofort: Reset-Verhältnis (Anteil der Schwelle, z.B. 0.5 = 50%)</label><input type="number" step="0.05" id="obi_instant_reset_ratio"></div>
  <div data-mode="obi_scalp"><label>OBI schnell (Sek.)</label><input type="number" step="1" id="obi_window_fast_seconds"></div>
  <div data-mode="obi_scalp"><label>OBI mittel (Sek.)</label><input type="number" step="1" id="obi_window_medium_seconds"></div>
  <div data-mode="obi_scalp"><label>OBI langsam (Sek.)</label><input type="number" step="1" id="obi_window_slow_seconds"></div>
  <div data-mode="obi_scalp"><label>OBI Orderbuch-Level</label><input type="number" step="1" id="obi_levels"></div>
  <div data-mode="obi_scalp"><label>Tiefen-Gewichtung (nahe Level zählen mehr)</label>
    <select class="cfg" id="obi_depth_weighting_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="obi_scalp"><label>Median statt Durchschnitt (robuster gegen Ausreißer)</label>
    <select class="cfg" id="obi_use_median">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="obi_scalp"><label>Mindest-Liquidität (Buch-Gesamtvolumen, 0 = aus)</label><input type="number" step="any" id="obi_min_liquidity"></div>
  <div data-mode="obi_scalp"><label>Gewinn absichern (SL springt bei X% vom TP auf kleinen Gewinn)</label>
    <select class="cfg" id="obi_breakeven_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="obi_scalp"><label>Auslöser (Anteil vom TP, z.B. 0.5 = 50%)</label><input type="number" step="0.05" id="obi_breakeven_trigger_ratio"></div>
  <div data-mode="obi_scalp"><label>Abgesicherter Gewinn ($, nur $-Modus)</label><input type="number" step="any" id="obi_breakeven_lock_usd"></div>
  <div data-mode="obi_scalp"><label>Abgesicherter Gewinn (%, nur %-Modus)</label><input type="number" step="0.01" id="obi_breakeven_lock_pct"></div>
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
  <div data-mode="macd_stoch"><label>Zeitrahmen</label>
    <select class="cfg" id="macd_resolution">
      <option value="10s">10 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="15s">15 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="30s">30 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="1m">1 Minute</option>
      <option value="2m">2 Minuten (synthetisch aus 1m-Kerzen zusammengesetzt)</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
    </select>
  </div>
  <div data-mode="macd_stoch"><label>MACD schnell - Fast</label><input type="number" step="1" id="macd_fast_fast"></div>
  <div data-mode="macd_stoch"><label>MACD schnell - Slow</label><input type="number" step="1" id="macd_fast_slow"></div>
  <div data-mode="macd_stoch"><label>MACD schnell - Signal</label><input type="number" step="1" id="macd_fast_signal"></div>
  <div data-mode="macd_stoch"><label>MACD langsam - Fast</label><input type="number" step="1" id="macd_slow_fast"></div>
  <div data-mode="macd_stoch"><label>MACD langsam - Slow</label><input type="number" step="1" id="macd_slow_slow"></div>
  <div data-mode="macd_stoch"><label>MACD langsam - Signal</label><input type="number" step="1" id="macd_slow_signal"></div>
  <div data-mode="macd_stoch"><label>Stochastic-Filter</label>
    <select class="cfg" id="macd_use_stochastic">
      <option value="true">An - nur bei %K/%D in Signal-Richtung</option>
      <option value="false">Aus - nur MACD-Histogramme</option>
    </select>
  </div>
  <div data-mode="macd_stoch"><label>Stochastic %K-Periode</label><input type="number" step="1" id="stoch_k_period"></div>
  <div data-mode="macd_stoch"><label>Stochastic %K-Glättung</label><input type="number" step="1" id="stoch_k_smooth"></div>
  <div data-mode="macd_stoch"><label>Stochastic %D-Periode</label><input type="number" step="1" id="stoch_d_period"></div>
  <div data-mode="macd_stoch"><label>TP ($, fest)</label><input type="number" step="any" id="macd_tp_usd"></div>
  <div data-mode="macd_stoch"><label>SL ($, fest)</label><input type="number" step="any" id="macd_sl_usd"></div>
  <div data-mode="macd_stoch"><label>TP bei Farbwechsel schnelles Histogramm</label>
    <select class="cfg" id="macd_fast_fade_exit_enabled">
      <option value="true">An</option>
      <option value="false">Aus - nur noch fester $-Betrag als TP</option>
    </select>
  </div>
  <div data-mode="macd_stoch"><label>Zusätzlicher TP bei Farbwechsel langsames Histogramm</label>
    <select class="cfg" id="macd_slow_fade_exit_enabled">
      <option value="false">Aus</option>
      <option value="true">An - zusätzlich zum schnellen Histogramm</option>
    </select>
  </div>
  <div data-mode="macd_stoch"><label>Sofort-Exit bei Trendumkehr (langsamer MACD wechselt Vorzeichen)</label>
    <select class="cfg" id="macd_slow_reversal_exit_enabled">
      <option value="true">An - empfohlen für Pullback-Strategie</option>
      <option value="false">Aus</option>
    </select>
  </div>
  <div data-mode="macd_stoch"><label>TP bei Nulllinien-Durchgang schnelles Histogramm</label>
    <select class="cfg" id="macd_fast_zero_cross_exit_enabled">
      <option value="true">An - lässt Trend durch Pullback-Dips laufen, exit erst bei echtem Rückkreuzen</option>
      <option value="false">Aus</option>
    </select>
  </div>
  <div data-mode="fib_reversal"><label>Zeitrahmen</label>
    <select class="cfg" id="fib_resolution">
      <option value="10s">10 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="15s">15 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="30s">30 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
    </select>
  </div>
  <div data-mode="fib_reversal"><label>Lookback (Kerzen für Swing-High/Low)</label><input type="number" step="1" id="fib_lookback_candles"></div>
  <div data-mode="fib_reversal"><label>Einstieg 1 (Fib-Level)</label><input type="number" step="0.001" id="fib_entry1_level"></div>
  <div data-mode="fib_reversal"><label>Einstieg 2 / Nachkauf (Fib-Level)</label><input type="number" step="0.001" id="fib_entry2_level"></div>
  <div data-mode="fib_reversal"><label>TP1 (Fib-Level)</label><input type="number" step="0.001" id="fib_tp1_level"></div>
  <div data-mode="fib_reversal"><label>TP1 Teilverkauf (%)</label><input type="number" step="1" id="fib_tp1_close_pct"></div>
  <div data-mode="fib_reversal"><label>TP2 (Fib-Level, Rest schließen)</label><input type="number" step="0.001" id="fib_tp2_level"></div>
  <div data-mode="fib_reversal"><label>Stop-Loss (Fib-Level)</label><input type="number" step="0.001" id="fib_sl_level"></div>
  <div data-mode="fib_reversal"><label>Cooldown nach SL (Sek.)</label><input type="number" step="1" id="fib_cooldown_seconds"></div>
  <div data-mode="stoch_cross"><label>Zeitrahmen</label>
    <select class="cfg" id="stoch_cross_resolution">
      <option value="10s">10 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="15s">15 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="30s">30 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="1m">1 Minute</option>
      <option value="2m">2 Minuten (synthetisch)</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
    </select>
  </div>
  <div data-mode="stoch_cross"><label>Stochastic %K-Periode</label><input type="number" step="1" id="stoch_cross_k_period"></div>
  <div data-mode="stoch_cross"><label>Stochastic %K-Glättung</label><input type="number" step="1" id="stoch_cross_k_smooth"></div>
  <div data-mode="stoch_cross"><label>Stochastic %D-Periode</label><input type="number" step="1" id="stoch_cross_d_period"></div>
  <div data-mode="stoch_cross"><label>Überverkauft-Schwelle (Long-Kreuzung)</label><input type="number" step="1" id="stoch_cross_oversold"></div>
  <div data-mode="stoch_cross"><label>Überkauft-Schwelle (Short-Kreuzung)</label><input type="number" step="1" id="stoch_cross_overbought"></div>
  <div data-mode="stoch_cross"><label>TP ($, fest)</label><input type="number" step="any" id="stoch_cross_tp_usd"></div>
  <div data-mode="stoch_cross"><label>SL ($, fest)</label><input type="number" step="any" id="stoch_cross_sl_usd"></div>
  <div data-mode="stoch_cross"><label>Trendfilter (EMA)</label>
    <select class="cfg" id="stoch_cross_trend_filter_enabled">
      <option value="false">Aus</option>
      <option value="true">An - nur Long über EMA, nur Short unter EMA</option>
    </select>
  </div>
  <div data-mode="stoch_cross"><label>Trend-EMA Periode</label><input type="number" step="1" id="stoch_cross_trend_ema_period"></div>
  <div data-mode="stoch_cross"><label>SL/TP-Modus</label>
    <select class="cfg" id="stoch_cross_sl_tp_mode">
      <option value="fixed">Fest ($-Betrag oben)</option>
      <option value="atr">ATR-basiert (marktadaptiv)</option>
    </select>
  </div>
  <div data-mode="stoch_cross"><label>ATR-Periode</label><input type="number" step="1" id="stoch_cross_atr_period"></div>
  <div data-mode="stoch_cross"><label>SL ATR-Multiplikator</label><input type="number" step="0.1" id="stoch_cross_sl_atr_mult"></div>
  <div data-mode="stoch_cross"><label>TP ATR-Multiplikator</label><input type="number" step="0.1" id="stoch_cross_tp_atr_mult"></div>
  <div data-mode="stoch_cross"><label>Range-Profile-Kontext-Filter</label>
    <select class="cfg" id="stoch_cross_rp_filter_enabled">
      <option value="false">Aus</option>
      <option value="true">An - Long nur unter, Short nur über der POC-Mittellinie</option>
    </select>
  </div>
  <div data-mode="stoch_cross"><label>Range-Profile Lookback (Kerzen)</label><input type="number" step="1" id="stoch_cross_rp_lookback"></div>
  <div data-mode="stoch_cross"><label>Nur nach Squeeze einsteigen</label>
    <select class="cfg" id="stoch_cross_require_squeeze">
      <option value="false">Aus</option>
      <option value="true">An - nur wenn direkt vorher Kanal-Squeeze aktiv war</option>
    </select>
  </div>
  <div data-mode="stoch_cross"><label>Squeeze Lookback (Kerzen)</label><input type="number" step="1" id="stoch_cross_squeeze_lookback"></div>
  <div data-mode="stoch_cross"><label>Squeeze-Schwelle (%)</label><input type="number" step="5" id="stoch_cross_squeeze_threshold_pct"></div>
  <div data-mode="range_profile"><label>Modus</label>
    <select class="cfg" id="rp_mode">
      <option value="reversion">Reversion - Ausbruch = Gegenrichtung, TP = Mittellinie</option>
      <option value="momentum">Momentum - Ausbruch = Fortsetzung (wie Original-Indikator)</option>
    </select>
  </div>
  <div data-mode="range_profile"><label>Zeitrahmen</label>
    <select class="cfg" id="rp_resolution">
      <option value="10s">10 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="15s">15 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="30s">30 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="1m">1 Minute</option>
      <option value="2m">2 Minuten (synthetisch)</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
    </select>
  </div>
  <div data-mode="range_profile"><label>Lookback (Kerzen)</label><input type="number" step="1" id="rp_lookback"></div>
  <div data-mode="range_profile"><label>OB/OS-Level (%)</label><input type="number" step="1" id="rp_ob_os_level"></div>
  <div data-mode="range_profile"><label>TP ($, fest)</label><input type="number" step="any" id="rp_tp_usd"></div>
  <div data-mode="range_profile"><label>SL ($, fest)</label><input type="number" step="any" id="rp_sl_usd"></div>
  <div data-mode="range_profile"><label>Gewinn absichern (SL springt bei X$ auf kleinen Gewinn)</label>
    <select class="cfg" id="rp_breakeven_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="range_profile"><label>Auslöser ($, z.B. 3)</label><input type="number" step="any" id="rp_breakeven_trigger_usd"></div>
  <div data-mode="range_profile"><label>Abgesicherter Gewinn ($)</label><input type="number" step="any" id="rp_breakeven_lock_usd"></div>
  <div data-mode="range_profile"><label>Squeeze Lookback (Kerzen)</label><input type="number" step="1" id="rp_squeeze_lookback"></div>
  <div data-mode="range_profile"><label>Squeeze-Schwelle (%)</label><input type="number" step="5" id="rp_squeeze_threshold_pct"></div>
  <div data-mode="macd_simple"><label>Zeitrahmen</label>
    <select class="cfg" id="macd_simple_resolution">
      <option value="10s">10 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="15s">15 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="30s">30 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="1m">1 Minute</option>
      <option value="2m">2 Minuten (synthetisch)</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
    </select>
  </div>
  <div data-mode="macd_simple"><label>MACD Fast</label><input type="number" step="1" id="macd_simple_fast"></div>
  <div data-mode="macd_simple"><label>MACD Slow</label><input type="number" step="1" id="macd_simple_slow"></div>
  <div data-mode="macd_simple"><label>MACD Signal</label><input type="number" step="1" id="macd_simple_signal"></div>
  <div data-mode="macd_simple"><label>TP ($, fest)</label><input type="number" step="any" id="macd_simple_tp_usd"></div>
  <div data-mode="macd_simple"><label>SL ($, fest)</label><input type="number" step="any" id="macd_simple_sl_usd"></div>
  <div data-mode="macd_simple"><label>Exit-Modus</label>
    <select class="cfg" id="macd_simple_exit_mode">
      <option value="tp_sl">Fester TP/SL</option>
      <option value="reverse">Position bei Farbwechsel drehen (SL bleibt als Schutz aktiv, TP entfällt)</option>
    </select>
  </div>
  <div data-mode="macd_simple"><label>Früh-Exit bei schwächer werdendem Histogramm</label>
    <select class="cfg" id="macd_simple_early_exit_enabled">
      <option value="false">Aus</option>
      <option value="true">An - schließt schon vor dem vollen Farbwechsel</option>
    </select>
  </div>
  <div data-mode="macd_simple"><label>Früh-Exit nach X Kerzen in Folge schwächer</label><input type="number" step="1" id="macd_simple_early_exit_bars"></div>
  <div data-mode="macd_simple"><label>Früh-Exit dreht sofort in Gegenrichtung</label>
    <select class="cfg" id="macd_simple_early_exit_reverse">
      <option value="false">Aus - schließt nur</option>
      <option value="true">An - öffnet direkt die Gegenposition</option>
    </select>
  </div>
  <div data-mode="macd_simple"><label>Gewinn absichern (SL springt bei X$ auf kleinen Gewinn)</label>
    <select class="cfg" id="macd_simple_breakeven_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="macd_simple"><label>Auslöser ($, z.B. 3)</label><input type="number" step="any" id="macd_simple_breakeven_trigger_usd"></div>
  <div data-mode="macd_simple"><label>Abgesicherter Gewinn ($)</label><input type="number" step="any" id="macd_simple_breakeven_lock_usd"></div>
  <div data-mode="candle_flip"><label>Zeitrahmen</label>
    <select class="cfg" id="cf_resolution">
      <option value="10s">10 Sekunden</option>
      <option value="15s">15 Sekunden</option>
      <option value="30s">30 Sekunden</option>
      <option value="1m">1 Minute</option>
      <option value="2m">2 Minuten (synthetisch)</option>
      <option value="5m">5 Minuten</option>
    </select>
  </div>
  <div data-mode="candle_flip"><label>Mindest-Kerzenkörper (%) - filtert Rauschen</label><input type="number" step="0.01" id="cf_min_body_pct"></div>
  <div data-mode="candle_flip"><label>SL ($, fest - Sicherheitsnetz)</label><input type="number" step="any" id="cf_sl_usd"></div>
  <div data-mode="range_profile"><label>Nur nach Squeeze einsteigen</label>
    <select class="cfg" id="rp_require_squeeze">
      <option value="false">Aus - Squeeze nur zur Anzeige</option>
      <option value="true">An - Einstieg nur wenn direkt vorher ein Squeeze aktiv war</option>
    </select>
  </div>
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

<h2 class="section-title">📊 Backtest (mit den oben gespeicherten Einstellungen)</h2>
<div class="panel-card">
  <div style="font-size:13px; color:var(--text-dim); margin-bottom:12px;">
    Testet die aktuell gespeicherten Strategie-Einstellungen gegen echte historische Binance-Kerzen.
    Nur für MACD-Dual, MACD-Simple, Candle-Flip, Stochastic-Cross, Range-Profile und Fibonacci-Reversal (Grid/OBI-Scalp brauchen
    historische Orderbuch-Daten, die es nicht gibt). SL/TP werden pro Kerze am Schlusskurs geprüft,
    nicht Tick-für-Tick wie live. Lighter ist gebührenfrei, es werden also keine Gebühren simuliert.
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:16px;">
    <div><label>Zeitraum (Tage)</label><input type="number" step="1" id="backtest-days" value="30" style="width:100px;"></div>
    <button id="btn-backtest" style="padding:12px 24px;">▶️ Backtest starten</button>
    <button id="btn-sweep" style="padding:12px 24px;">🔬 Preset-Sweep starten (nur MACD-Simple)</button>
  </div>
  <div id="backtest-status" style="color:var(--text-dim); font-size:13px;"></div>
  <div id="backtest-results" style="display:none; margin-top:16px;">
    <div style="display:flex; gap:20px; flex-wrap:wrap; margin-bottom:12px;">
      <div><div class="label">Kerzen verarbeitet</div><div class="value" id="bt-candles">-</div></div>
      <div><div class="label">Zeitraum tatsächlich</div><div class="value" id="bt-days">-</div></div>
      <div><div class="label">Trades</div><div class="value" id="bt-trades">-</div></div>
      <div><div class="label">Trefferquote</div><div class="value" id="bt-winrate">-</div></div>
      <div><div class="label">Gesamt-PnL $</div><div class="value" id="bt-pnl">-</div></div>
      <div><div class="label">Max Drawdown $</div><div class="value" id="bt-dd">-</div></div>
      <div><div class="label">Ø Gewinn / Ø Verlust $</div><div class="value" id="bt-avg">-</div></div>
    </div>
  </div>
  <div id="sweep-results" style="display:none; margin-top:20px;">
    <div class="label" style="margin-bottom:8px;">Preset-Sweep-Rangliste (beste zuerst)</div>
    <table id="sweep-table">
      <thead><tr><th>#</th><th>Kombination</th><th>Trades</th><th>Trefferquote</th><th>Gesamt-PnL $</th><th>Max Drawdown $</th></tr></thead>
      <tbody></tbody>
    </table>
  </div>
</div>

<h2 class="section-title">Letzte abgeschlossene Trades</h2>
<div class="panel-card">
<table id="trades-table"><thead><tr><th>Eröffnet</th><th>Geschlossen</th><th>Seite</th><th>Ø-Einstieg</th><th>Exit</th><th>Stufen</th><th>Grund</th><th>PnL $</th></tr></thead><tbody></tbody></table>
</div>
</div>

<script>
let priceChart;
let obiChart;
let macdSimpleChart;
let currentSymbol = null;
let allSymbols = [];

function computeEMA(values, period) {
  if (!values.length) return [];
  const k = 2 / (period + 1);
  const out = [values[0]];
  for (let i = 1; i < values.length; i++) out.push(values[i] * k + out[i-1] * (1 - k));
  return out;
}

function renderMiniCandles(hist) {
  const container = document.getElementById('mini-candles');
  if (!container) return;
  if (!hist || hist.length < 2) { container.innerHTML = '<span style="color:#6b7280;">noch nicht genug Daten</span>'; return; }
  const numCandles = 10;
  const chunkSize = Math.max(1, Math.floor(hist.length / numCandles));
  const candles = [];
  for (let i = 0; i < hist.length; i += chunkSize) {
    const chunk = hist.slice(i, i + chunkSize).map(p => p.price);
    if (!chunk.length) continue;
    candles.push({ open: chunk[0], close: chunk[chunk.length-1], high: Math.max(...chunk), low: Math.min(...chunk) });
  }
  const last10 = candles.slice(-numCandles);
  const globalMin = Math.min(...last10.map(c => c.low));
  const globalMax = Math.max(...last10.map(c => c.high));
  const range = (globalMax - globalMin) || 1;
  const maxPx = 60;
  container.innerHTML = last10.map(c => {
    const isGreen = c.close >= c.open;
    const bodyTop = maxPx * (1 - (Math.max(c.open, c.close) - globalMin) / range);
    const bodyHeight = Math.max(2, maxPx * (Math.abs(c.close - c.open) / range));
    const wickTop = maxPx * (1 - (c.high - globalMin) / range);
    const wickHeight = Math.max(1, maxPx * ((c.high - c.low) / range));
    const color = isGreen ? '#4ade80' : '#f87171';
    return `<div style="position:relative; width:18px; height:${maxPx}px;">
      <div style="position:absolute; left:8px; top:${wickTop}px; width:2px; height:${wickHeight}px; background:${color};"></div>
      <div style="position:absolute; left:2px; top:${bodyTop}px; width:14px; height:${bodyHeight}px; background:${color}; border-radius:2px;"></div>
    </div>`;
  }).join('');
}

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

async function manualTrade(direction) {
  const res = await fetch(`/api/manual_trade?symbol=${currentSymbol}`, {
    method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({direction})
  });
  const data = await res.json();
  if (data.error) alert(data.error);
  refresh();
}
document.getElementById('btn-manual-buy').addEventListener('click', () => manualTrade('long'));
document.getElementById('btn-manual-sell').addEventListener('click', () => manualTrade('short'));
document.getElementById('btn-manual-tp').addEventListener('click', async () => {
  const res = await fetch(`/api/close?symbol=${currentSymbol}`, { method:'POST' });
  const data = await res.json();
  if (data.error) alert(data.error);
  refresh();
});

document.getElementById('btn-backtest').addEventListener('click', async () => {
  const days = parseInt(document.getElementById('backtest-days').value) || 30;
  const btn = document.getElementById('btn-backtest');
  const statusEl = document.getElementById('backtest-status');
  const resultsEl = document.getElementById('backtest-results');
  btn.disabled = true;
  resultsEl.style.display = 'none';
  statusEl.innerText = `⏳ Lade Kerzen von Binance und simuliere... kann bei langen Zeiträumen 1-2 Minuten dauern.`;
  try {
    const res = await fetch(`/api/backtest?symbol=${currentSymbol}`, {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({days})
    });
    const data = await res.json();
    if (data.error) {
      statusEl.innerText = `❌ ${data.error}`;
    } else {
      statusEl.innerText = `${data.cache_used ? '⚡ aus Cache' : '📡 neu von Binance geladen'} - ${data.candles_processed} Kerzen verarbeitet (${data.actual_days_covered} Tage, Zeitrahmen ${data.resolution})` +
        (data.candles_processed >= data.candle_cap ? ` - auf ${data.candle_cap} Kerzen begrenzt (Performance-Schutz)` : '');
      document.getElementById('bt-candles').innerText = data.candles_processed;
      document.getElementById('bt-days').innerText = data.actual_days_covered;
      document.getElementById('bt-trades').innerText = data.stats.trades;
      document.getElementById('bt-winrate').innerText = data.stats.win_rate_pct + '%';
      const pnlEl = document.getElementById('bt-pnl');
      pnlEl.innerText = data.stats.total_pnl_usd;
      pnlEl.className = data.stats.total_pnl_usd >= 0 ? 'value green' : 'value red';
      document.getElementById('bt-dd').innerText = data.stats.max_drawdown_usd;
      document.getElementById('bt-avg').innerText = `${data.stats.avg_win_usd} / ${data.stats.avg_loss_usd}`;
      resultsEl.style.display = 'block';
    }
  } catch (e) {
    statusEl.innerText = `❌ Fehler: ${e}`;
  }
  btn.disabled = false;
});

document.getElementById('btn-sweep').addEventListener('click', async () => {
  const days = parseInt(document.getElementById('backtest-days').value) || 30;
  const btn = document.getElementById('btn-sweep');
  const statusEl = document.getElementById('backtest-status');
  const sweepEl = document.getElementById('sweep-results');
  btn.disabled = true;
  sweepEl.style.display = 'none';
  statusEl.innerText = `⏳ Lade Kerzen und teste bis zu 20 Kombinationen durch... kann 1-2 Minuten dauern.`;
  try {
    const res = await fetch(`/api/backtest_sweep?symbol=${currentSymbol}`, {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({days})
    });
    const data = await res.json();
    if (data.error) {
      statusEl.innerText = `❌ ${data.error}`;
    } else {
      statusEl.innerText = `✅ ${data.combinations_tested} Kombinationen getestet - ${data.candles_processed} Kerzen (${data.actual_days_covered} Tage, Zeitrahmen ${data.resolution})`;
      const tbody = document.querySelector('#sweep-table tbody');
      tbody.innerHTML = data.results.map((r, i) => `
        <tr>
          <td>${i + 1}</td>
          <td>${r.label}</td>
          <td>${r.stats.trades}</td>
          <td>${r.stats.win_rate_pct}%</td>
          <td class="${r.stats.total_pnl_usd >= 0 ? 'green' : 'red'}">${r.stats.total_pnl_usd}</td>
          <td>${r.stats.max_drawdown_usd}</td>
        </tr>
      `).join('');
      sweepEl.style.display = 'block';
    }
  } catch (e) {
    statusEl.innerText = `❌ Fehler: ${e}`;
  }
  btn.disabled = false;
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
    <div class="card"><div class="label">OBI schnell (${data.config.entry_mode==='obi_scalp'?'aktiv':'inaktiv'})</div><div class="value ${data.obi_fast>=0?'green':'red'}">${data.obi_fast ?? '-'}</div></div>
    <div class="card"><div class="label">OBI mittel</div><div class="value ${data.obi_medium>=0?'green':'red'}">${data.obi_medium ?? '-'}</div></div>
    <div class="card"><div class="label">OBI langsam</div><div class="value ${data.obi_slow>=0?'green':'red'}">${data.obi_slow ?? '-'}</div></div>
    <div class="card"><div class="label">MACD schnell (${data.config.entry_mode==='macd_stoch'?'aktiv':'inaktiv'})</div><div class="value ${data.macd_fast_hist>=0?'green':'red'}">${data.macd_fast_hist ?? '-'}</div></div>
    <div class="card"><div class="label">MACD langsam</div><div class="value ${data.macd_slow_hist>=0?'green':'red'}">${data.macd_slow_hist ?? '-'}</div></div>
    <div class="card"><div class="label">Stochastic %K / %D</div><div class="value">${data.macd_stoch_k ?? '-'} / ${data.macd_stoch_d ?? '-'}</div></div>
    <div class="card"><div class="label">Fib High / Low (${data.config.entry_mode==='fib_reversal'?'aktiv':'inaktiv'})</div><div class="value">${data.fib?.high ?? '-'} / ${data.fib?.low ?? '-'}</div></div>
    <div class="card"><div class="label">Fib Einstieg 1 / 2</div><div class="value">${data.fib?.entry1_price ?? '-'} / ${data.fib?.entry2_price ?? '-'}</div></div>
    <div class="card"><div class="label">Fib TP1 / TP2 / SL</div><div class="value">${data.fib?.tp1_price ?? '-'} / ${data.fib?.tp2_price ?? '-'} / ${data.fib?.sl_price ?? '-'}</div></div>
    <div class="card"><div class="label">Stochastic-Cross %K / %D (${data.config.entry_mode==='stoch_cross'?'aktiv':'inaktiv'})</div><div class="value">${data.stoch_cross_k ?? '-'} / ${data.stoch_cross_d ?? '-'}</div></div>
    <div class="card"><div class="label">Stochastic-Cross SL / TP (aktive Position)</div><div class="value">${data.stoch_cross_sl_price ?? '-'} / ${data.stoch_cross_tp_price ?? '-'}</div></div>
    <div class="card"><div class="label">Stochastic-Cross POC-Mittellinie</div><div class="value">${data.stoch_cross_rp_mid ?? '-'}</div></div>
    <div class="card"><div class="label">Stochastic-Cross ⚠ Squeeze</div><div class="value ${data.stoch_cross_squeeze_active?'red':'green'}">${data.stoch_cross_squeeze_active ? 'AKTIV' : 'nein'}</div></div>
    <div class="card"><div class="label">Range-Profile Oszillator (${data.config.entry_mode==='range_profile'?'aktiv':'inaktiv'})</div><div class="value ${(data.rp_osc??0)>=0?'green':'red'}">${data.rp_osc ?? '-'}</div></div>
    <div class="card"><div class="label">Range-Profile Mitte / Kanal</div><div class="value">${data.rp_mid_price ?? '-'} (${data.rp_range_low ?? '-'} – ${data.rp_range_high ?? '-'})</div></div>
    <div class="card"><div class="label">Range-Profile TP / SL (fest, $)</div><div class="value">${data.config.rp_tp_usd ?? '-'} / ${data.config.rp_sl_usd ?? '-'}${data.rp_breakeven_triggered ? ' 🔒' : ''}</div></div>
    <div class="card"><div class="label">Range-Profile Kanalbreite (Ø)</div><div class="value">${data.rp_channel_width ?? '-'} (Ø ${data.rp_avg_width ?? '-'})</div></div>
    <div class="card"><div class="label">⚠ Squeeze (Ausbruch könnte bevorstehen)</div><div class="value ${data.rp_squeeze_active?'red':'green'}">${data.rp_squeeze_active ? 'AKTIV' : 'nein'}</div></div>
    <div class="card"><div class="label">MACD-Simple Histogramm (${data.config.entry_mode==='macd_simple'?'aktiv':'inaktiv'})</div><div class="value ${(data.macd_simple_hist??0)>=0?'green':'red'}">${data.macd_simple_hist ?? '-'}</div></div>
    <div class="card"><div class="label">Binance-1s-Puffer (Diagnose)</div><div class="value">${data.binance_1s_buffer_size ?? 0} Kerzen / ${Math.round((data.binance_1s_buffer_span_sec ?? 0)/60)} Min</div></div>
    <div class="card"><div class="label">Lighter-Tick-Fallback-Puffer (Diagnose)</div><div class="value">${data.local_1s_buffer_size ?? 0} Kerzen</div></div>
    <div class="card"><div class="label">Candle-Flip letzte Farbe (${data.config.entry_mode==='candle_flip'?'aktiv':'inaktiv'})</div><div class="value ${data.cf_last_color==='green'?'green':data.cf_last_color==='red'?'red':''}">${data.cf_last_color ?? '-'} (${data.cf_last_body_pct ?? '-'}%)</div></div>
    <div class="card"><div class="label">Realisiert (gesamt) $</div><div class="value ${data.stats.total_pnl_usd>=0?'green':'red'}">${data.stats.total_pnl_usd}</div></div>
    <div class="card"><div class="label">Trades / Trefferquote</div><div class="value">${data.stats.trades} / ${data.stats.win_rate_pct}%</div></div>
  `;

  if (!window.formTouched) {
    document.getElementById('margin').value = data.config.margin;
    document.getElementById('leverage').value = data.config.leverage;
    document.getElementById('entry_mode').value = data.config.entry_mode;
    document.getElementById('obi_threshold').value = data.config.obi_threshold;
    document.getElementById('obi_mode').value = data.config.obi_mode;
    document.getElementById('obi_long_threshold').value = data.config.obi_long_threshold;
    document.getElementById('obi_short_threshold').value = data.config.obi_short_threshold;
    document.getElementById('obi_reversal_min_bounce').value = data.config.obi_reversal_min_bounce;
    document.getElementById('obi_instant_reset_ratio').value = data.config.obi_instant_reset_ratio;
    document.getElementById('obi_window_fast_seconds').value = data.config.obi_window_fast_seconds;
    document.getElementById('obi_window_medium_seconds').value = data.config.obi_window_medium_seconds;
    document.getElementById('obi_window_slow_seconds').value = data.config.obi_window_slow_seconds;
    document.getElementById('obi_levels').value = data.config.obi_levels;
    document.getElementById('obi_depth_weighting_enabled').value = String(data.config.obi_depth_weighting_enabled);
    document.getElementById('obi_use_median').value = String(data.config.obi_use_median);
    document.getElementById('obi_min_liquidity').value = data.config.obi_min_liquidity;
    document.getElementById('obi_breakeven_enabled').value = String(data.config.obi_breakeven_enabled);
    document.getElementById('obi_breakeven_trigger_ratio').value = data.config.obi_breakeven_trigger_ratio;
    document.getElementById('obi_breakeven_lock_usd').value = data.config.obi_breakeven_lock_usd;
    document.getElementById('obi_breakeven_lock_pct').value = data.config.obi_breakeven_lock_pct;
    document.getElementById('obi_tp_sl_mode').value = data.config.obi_tp_sl_mode;
    document.getElementById('obi_tp_pct').value = data.config.obi_tp_pct;
    document.getElementById('obi_sl_pct').value = data.config.obi_sl_pct;
    document.getElementById('obi_tp_usd').value = data.config.obi_tp_usd;
    document.getElementById('obi_sl_usd').value = data.config.obi_sl_usd;
    document.getElementById('obi_cooldown_seconds').value = data.config.obi_cooldown_seconds;
    document.getElementById('obi_trend_filter').value = String(data.config.obi_trend_filter);
    document.getElementById('obi_trend_ema_length').value = data.config.obi_trend_ema_length;
    document.getElementById('macd_resolution').value = data.config.macd_resolution;
    document.getElementById('macd_fast_fast').value = data.config.macd_fast_fast;
    document.getElementById('macd_fast_slow').value = data.config.macd_fast_slow;
    document.getElementById('macd_fast_signal').value = data.config.macd_fast_signal;
    document.getElementById('macd_slow_fast').value = data.config.macd_slow_fast;
    document.getElementById('macd_slow_slow').value = data.config.macd_slow_slow;
    document.getElementById('macd_slow_signal').value = data.config.macd_slow_signal;
    document.getElementById('macd_use_stochastic').value = String(data.config.macd_use_stochastic);
    document.getElementById('stoch_k_period').value = data.config.stoch_k_period;
    document.getElementById('stoch_k_smooth').value = data.config.stoch_k_smooth;
    document.getElementById('stoch_d_period').value = data.config.stoch_d_period;
    document.getElementById('macd_tp_usd').value = data.config.macd_tp_usd;
    document.getElementById('macd_sl_usd').value = data.config.macd_sl_usd;
    document.getElementById('macd_fast_fade_exit_enabled').value = String(data.config.macd_fast_fade_exit_enabled);
    document.getElementById('macd_slow_fade_exit_enabled').value = String(data.config.macd_slow_fade_exit_enabled);
    document.getElementById('macd_slow_reversal_exit_enabled').value = String(data.config.macd_slow_reversal_exit_enabled);
    document.getElementById('macd_fast_zero_cross_exit_enabled').value = String(data.config.macd_fast_zero_cross_exit_enabled);
    document.getElementById('fib_resolution').value = data.config.fib_resolution;
    document.getElementById('fib_lookback_candles').value = data.config.fib_lookback_candles;
    document.getElementById('fib_entry1_level').value = data.config.fib_entry1_level;
    document.getElementById('fib_entry2_level').value = data.config.fib_entry2_level;
    document.getElementById('fib_tp1_level').value = data.config.fib_tp1_level;
    document.getElementById('fib_tp1_close_pct').value = data.config.fib_tp1_close_pct;
    document.getElementById('fib_tp2_level').value = data.config.fib_tp2_level;
    document.getElementById('fib_sl_level').value = data.config.fib_sl_level;
    document.getElementById('fib_cooldown_seconds').value = data.config.fib_cooldown_seconds;
    document.getElementById('stoch_cross_resolution').value = data.config.stoch_cross_resolution;
    document.getElementById('stoch_cross_k_period').value = data.config.stoch_cross_k_period;
    document.getElementById('stoch_cross_k_smooth').value = data.config.stoch_cross_k_smooth;
    document.getElementById('stoch_cross_d_period').value = data.config.stoch_cross_d_period;
    document.getElementById('stoch_cross_oversold').value = data.config.stoch_cross_oversold;
    document.getElementById('stoch_cross_overbought').value = data.config.stoch_cross_overbought;
    document.getElementById('stoch_cross_tp_usd').value = data.config.stoch_cross_tp_usd;
    document.getElementById('stoch_cross_sl_usd').value = data.config.stoch_cross_sl_usd;
    document.getElementById('stoch_cross_trend_filter_enabled').value = String(data.config.stoch_cross_trend_filter_enabled);
    document.getElementById('stoch_cross_trend_ema_period').value = data.config.stoch_cross_trend_ema_period;
    document.getElementById('stoch_cross_sl_tp_mode').value = data.config.stoch_cross_sl_tp_mode;
    document.getElementById('stoch_cross_atr_period').value = data.config.stoch_cross_atr_period;
    document.getElementById('stoch_cross_sl_atr_mult').value = data.config.stoch_cross_sl_atr_mult;
    document.getElementById('stoch_cross_tp_atr_mult').value = data.config.stoch_cross_tp_atr_mult;
    document.getElementById('stoch_cross_rp_filter_enabled').value = String(data.config.stoch_cross_rp_filter_enabled);
    document.getElementById('stoch_cross_rp_lookback').value = data.config.stoch_cross_rp_lookback;
    document.getElementById('stoch_cross_require_squeeze').value = String(data.config.stoch_cross_require_squeeze);
    document.getElementById('stoch_cross_squeeze_lookback').value = data.config.stoch_cross_squeeze_lookback;
    document.getElementById('stoch_cross_squeeze_threshold_pct').value = data.config.stoch_cross_squeeze_threshold_pct;
    document.getElementById('rp_mode').value = data.config.rp_mode;
    document.getElementById('rp_resolution').value = data.config.rp_resolution;
    document.getElementById('rp_lookback').value = data.config.rp_lookback;
    document.getElementById('rp_ob_os_level').value = data.config.rp_ob_os_level;
    document.getElementById('rp_tp_usd').value = data.config.rp_tp_usd;
    document.getElementById('rp_sl_usd').value = data.config.rp_sl_usd;
    document.getElementById('rp_breakeven_enabled').value = String(data.config.rp_breakeven_enabled);
    document.getElementById('rp_breakeven_trigger_usd').value = data.config.rp_breakeven_trigger_usd;
    document.getElementById('rp_breakeven_lock_usd').value = data.config.rp_breakeven_lock_usd;
    document.getElementById('rp_squeeze_lookback').value = data.config.rp_squeeze_lookback;
    document.getElementById('rp_squeeze_threshold_pct').value = data.config.rp_squeeze_threshold_pct;
    document.getElementById('macd_simple_resolution').value = data.config.macd_simple_resolution;
    document.getElementById('macd_simple_fast').value = data.config.macd_simple_fast;
    document.getElementById('macd_simple_slow').value = data.config.macd_simple_slow;
    document.getElementById('macd_simple_signal').value = data.config.macd_simple_signal;
    document.getElementById('macd_simple_tp_usd').value = data.config.macd_simple_tp_usd;
    document.getElementById('macd_simple_sl_usd').value = data.config.macd_simple_sl_usd;
    document.getElementById('macd_simple_exit_mode').value = data.config.macd_simple_exit_mode;
    document.getElementById('macd_simple_early_exit_enabled').value = String(data.config.macd_simple_early_exit_enabled);
    document.getElementById('macd_simple_early_exit_bars').value = data.config.macd_simple_early_exit_bars;
    document.getElementById('macd_simple_early_exit_reverse').value = String(data.config.macd_simple_early_exit_reverse);
    document.getElementById('macd_simple_breakeven_enabled').value = String(data.config.macd_simple_breakeven_enabled);
    document.getElementById('macd_simple_breakeven_trigger_usd').value = data.config.macd_simple_breakeven_trigger_usd;
    document.getElementById('macd_simple_breakeven_lock_usd').value = data.config.macd_simple_breakeven_lock_usd;
    document.getElementById('cf_resolution').value = data.config.cf_resolution;
    document.getElementById('cf_min_body_pct').value = data.config.cf_min_body_pct;
    document.getElementById('cf_sl_usd').value = data.config.cf_sl_usd;
    document.getElementById('rp_require_squeeze').value = String(data.config.rp_require_squeeze);
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

  if (data.config.entry_mode === 'obi_scalp' && prices.length > 5) {
    datasets.push({ label:'EMA 9', data: computeEMA(prices, 9), borderColor:'#fbbf24', pointRadius:0, borderWidth:1.5 });
    datasets.push({ label:'EMA 21', data: computeEMA(prices, 21), borderColor:'#a78bfa', pointRadius:0, borderWidth:1.5 });
  }

  if (!priceChart) {
    priceChart = new Chart(document.getElementById('priceChart'), {
      type: 'line',
      data: { labels, datasets },
      options: { responsive:true, maintainAspectRatio:false, animation:false, scales:{ x:{ display:false }, y:{ ticks:{color:'#9ca3af'} } }, plugins:{legend:{labels:{color:'#e5e7eb'}}} }
    });
  } else {
    priceChart.data.labels = labels;
    priceChart.data.datasets = datasets;
    priceChart.update('none');
  }

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
    if (!obiChart) {
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
      obiChart.data.labels = obiLabels;
      obiChart.data.datasets = obiDatasets;
      obiChart.update('none');
    }
  } else {
    obiSection.style.display = 'none';
  }

  const macdSimpleSection = document.getElementById('macd-simple-chart-section');
  if (data.config.entry_mode === 'macd_simple' && (data.macd_simple_hist_history || []).length > 0) {
    macdSimpleSection.style.display = 'block';
    const mHist = data.macd_simple_hist_history || [];
    const mLabels = mHist.map(p => new Date(p.ts).toLocaleTimeString());
    const mValues = mHist.map(p => p.hist);
    const mColors = mValues.map(v => v >= 0 ? '#4ade80' : '#f87171');
    const macdDatasets = [
      { label: 'Histogramm', data: mValues, backgroundColor: mColors, borderWidth: 0, maxBarThickness: 14, categoryPercentage: 0.9, barPercentage: 0.9 },
    ];
    if (!macdSimpleChart) {
      macdSimpleChart = new Chart(document.getElementById('macdSimpleChart'), {
        type: 'bar',
        data: { labels: mLabels, datasets: macdDatasets },
        options: {
          responsive: true, maintainAspectRatio: false, animation: false,
          scales: { x: { display: false }, y: { ticks: { color: '#9ca3af' } } },
          plugins: { legend: { display: false } }
        }
      });
    } else {
      macdSimpleChart.data.labels = mLabels;
      macdSimpleChart.data.datasets[0].data = mValues;
      macdSimpleChart.data.datasets[0].backgroundColor = mColors;
      macdSimpleChart.update('none');
    }
  } else {
    macdSimpleSection.style.display = 'none';
  }

  const pocketSection = document.getElementById('pocket-trading-section');
  if (data.config.entry_mode === 'obi_scalp') {
    pocketSection.style.display = 'block';
    document.getElementById('pocket-margin').innerText = `$${data.config.margin} (${data.config.leverage}x)`;
    document.getElementById('pocket-position').innerText = data.position ? data.position.toUpperCase() : 'flach';
    document.getElementById('pocket-entry').innerText = data.avg_entry_price ?? '-';
    const pnlEl = document.getElementById('pocket-pnl');
    pnlEl.innerText = data.unrealized_pnl_usd ?? '-';
    pnlEl.className = (data.unrealized_pnl_usd ?? 0) >= 0 ? 'value green' : 'value red';
    renderMiniCandles(hist);
  } else {
    pocketSection.style.display = 'none';
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
    obi_threshold: parseFloat(document.getElementById('obi_threshold').value),
    obi_mode: document.getElementById('obi_mode').value,
    obi_long_threshold: parseFloat(document.getElementById('obi_long_threshold').value),
    obi_short_threshold: parseFloat(document.getElementById('obi_short_threshold').value),
    obi_reversal_min_bounce: parseFloat(document.getElementById('obi_reversal_min_bounce').value),
    obi_instant_reset_ratio: parseFloat(document.getElementById('obi_instant_reset_ratio').value),
    obi_window_fast_seconds: parseFloat(document.getElementById('obi_window_fast_seconds').value),
    obi_window_medium_seconds: parseFloat(document.getElementById('obi_window_medium_seconds').value),
    obi_window_slow_seconds: parseFloat(document.getElementById('obi_window_slow_seconds').value),
    obi_levels: parseInt(document.getElementById('obi_levels').value),
    obi_depth_weighting_enabled: document.getElementById('obi_depth_weighting_enabled').value === 'true',
    obi_use_median: document.getElementById('obi_use_median').value === 'true',
    obi_min_liquidity: parseFloat(document.getElementById('obi_min_liquidity').value),
    obi_breakeven_enabled: document.getElementById('obi_breakeven_enabled').value === 'true',
    obi_breakeven_trigger_ratio: parseFloat(document.getElementById('obi_breakeven_trigger_ratio').value),
    obi_breakeven_lock_usd: parseFloat(document.getElementById('obi_breakeven_lock_usd').value),
    obi_breakeven_lock_pct: parseFloat(document.getElementById('obi_breakeven_lock_pct').value),
    obi_tp_sl_mode: document.getElementById('obi_tp_sl_mode').value,
    obi_tp_pct: parseFloat(document.getElementById('obi_tp_pct').value),
    obi_sl_pct: parseFloat(document.getElementById('obi_sl_pct').value),
    obi_tp_usd: parseFloat(document.getElementById('obi_tp_usd').value),
    obi_sl_usd: parseFloat(document.getElementById('obi_sl_usd').value),
    obi_cooldown_seconds: parseFloat(document.getElementById('obi_cooldown_seconds').value),
    obi_trend_filter: document.getElementById('obi_trend_filter').value === 'true',
    obi_trend_ema_length: parseInt(document.getElementById('obi_trend_ema_length').value),
    macd_resolution: document.getElementById('macd_resolution').value,
    macd_fast_fast: parseInt(document.getElementById('macd_fast_fast').value),
    macd_fast_slow: parseInt(document.getElementById('macd_fast_slow').value),
    macd_fast_signal: parseInt(document.getElementById('macd_fast_signal').value),
    macd_slow_fast: parseInt(document.getElementById('macd_slow_fast').value),
    macd_slow_slow: parseInt(document.getElementById('macd_slow_slow').value),
    macd_slow_signal: parseInt(document.getElementById('macd_slow_signal').value),
    macd_use_stochastic: document.getElementById('macd_use_stochastic').value === 'true',
    stoch_k_period: parseInt(document.getElementById('stoch_k_period').value),
    stoch_k_smooth: parseInt(document.getElementById('stoch_k_smooth').value),
    stoch_d_period: parseInt(document.getElementById('stoch_d_period').value),
    macd_tp_usd: parseFloat(document.getElementById('macd_tp_usd').value),
    macd_sl_usd: parseFloat(document.getElementById('macd_sl_usd').value),
    macd_fast_fade_exit_enabled: document.getElementById('macd_fast_fade_exit_enabled').value === 'true',
    macd_slow_fade_exit_enabled: document.getElementById('macd_slow_fade_exit_enabled').value === 'true',
    macd_slow_reversal_exit_enabled: document.getElementById('macd_slow_reversal_exit_enabled').value === 'true',
    macd_fast_zero_cross_exit_enabled: document.getElementById('macd_fast_zero_cross_exit_enabled').value === 'true',
    fib_resolution: document.getElementById('fib_resolution').value,
    fib_lookback_candles: parseInt(document.getElementById('fib_lookback_candles').value),
    fib_entry1_level: parseFloat(document.getElementById('fib_entry1_level').value),
    fib_entry2_level: parseFloat(document.getElementById('fib_entry2_level').value),
    fib_tp1_level: parseFloat(document.getElementById('fib_tp1_level').value),
    fib_tp1_close_pct: parseFloat(document.getElementById('fib_tp1_close_pct').value),
    fib_tp2_level: parseFloat(document.getElementById('fib_tp2_level').value),
    fib_sl_level: parseFloat(document.getElementById('fib_sl_level').value),
    fib_cooldown_seconds: parseFloat(document.getElementById('fib_cooldown_seconds').value),
    stoch_cross_resolution: document.getElementById('stoch_cross_resolution').value,
    stoch_cross_k_period: parseInt(document.getElementById('stoch_cross_k_period').value),
    stoch_cross_k_smooth: parseInt(document.getElementById('stoch_cross_k_smooth').value),
    stoch_cross_d_period: parseInt(document.getElementById('stoch_cross_d_period').value),
    stoch_cross_oversold: parseFloat(document.getElementById('stoch_cross_oversold').value),
    stoch_cross_overbought: parseFloat(document.getElementById('stoch_cross_overbought').value),
    stoch_cross_tp_usd: parseFloat(document.getElementById('stoch_cross_tp_usd').value),
    stoch_cross_sl_usd: parseFloat(document.getElementById('stoch_cross_sl_usd').value),
    stoch_cross_trend_filter_enabled: document.getElementById('stoch_cross_trend_filter_enabled').value === 'true',
    stoch_cross_trend_ema_period: parseInt(document.getElementById('stoch_cross_trend_ema_period').value),
    stoch_cross_sl_tp_mode: document.getElementById('stoch_cross_sl_tp_mode').value,
    stoch_cross_atr_period: parseInt(document.getElementById('stoch_cross_atr_period').value),
    stoch_cross_sl_atr_mult: parseFloat(document.getElementById('stoch_cross_sl_atr_mult').value),
    stoch_cross_tp_atr_mult: parseFloat(document.getElementById('stoch_cross_tp_atr_mult').value),
    stoch_cross_rp_filter_enabled: document.getElementById('stoch_cross_rp_filter_enabled').value === 'true',
    stoch_cross_rp_lookback: parseInt(document.getElementById('stoch_cross_rp_lookback').value),
    stoch_cross_require_squeeze: document.getElementById('stoch_cross_require_squeeze').value === 'true',
    stoch_cross_squeeze_lookback: parseInt(document.getElementById('stoch_cross_squeeze_lookback').value),
    stoch_cross_squeeze_threshold_pct: parseFloat(document.getElementById('stoch_cross_squeeze_threshold_pct').value),
    rp_mode: document.getElementById('rp_mode').value,
    rp_resolution: document.getElementById('rp_resolution').value,
    rp_lookback: parseInt(document.getElementById('rp_lookback').value),
    rp_ob_os_level: parseFloat(document.getElementById('rp_ob_os_level').value),
    rp_tp_usd: parseFloat(document.getElementById('rp_tp_usd').value),
    rp_sl_usd: parseFloat(document.getElementById('rp_sl_usd').value),
    rp_breakeven_enabled: document.getElementById('rp_breakeven_enabled').value === 'true',
    rp_breakeven_trigger_usd: parseFloat(document.getElementById('rp_breakeven_trigger_usd').value),
    rp_breakeven_lock_usd: parseFloat(document.getElementById('rp_breakeven_lock_usd').value),
    rp_squeeze_lookback: parseInt(document.getElementById('rp_squeeze_lookback').value),
    rp_squeeze_threshold_pct: parseFloat(document.getElementById('rp_squeeze_threshold_pct').value),
    macd_simple_resolution: document.getElementById('macd_simple_resolution').value,
    macd_simple_fast: parseInt(document.getElementById('macd_simple_fast').value),
    macd_simple_slow: parseInt(document.getElementById('macd_simple_slow').value),
    macd_simple_signal: parseInt(document.getElementById('macd_simple_signal').value),
    macd_simple_tp_usd: parseFloat(document.getElementById('macd_simple_tp_usd').value),
    macd_simple_sl_usd: parseFloat(document.getElementById('macd_simple_sl_usd').value),
    macd_simple_exit_mode: document.getElementById('macd_simple_exit_mode').value,
    macd_simple_early_exit_enabled: document.getElementById('macd_simple_early_exit_enabled').value === 'true',
    macd_simple_early_exit_bars: parseInt(document.getElementById('macd_simple_early_exit_bars').value),
    macd_simple_early_exit_reverse: document.getElementById('macd_simple_early_exit_reverse').value === 'true',
    macd_simple_breakeven_enabled: document.getElementById('macd_simple_breakeven_enabled').value === 'true',
    macd_simple_breakeven_trigger_usd: parseFloat(document.getElementById('macd_simple_breakeven_trigger_usd').value),
    macd_simple_breakeven_lock_usd: parseFloat(document.getElementById('macd_simple_breakeven_lock_usd').value),
    cf_resolution: document.getElementById('cf_resolution').value,
    cf_min_body_pct: parseFloat(document.getElementById('cf_min_body_pct').value),
    cf_sl_usd: parseFloat(document.getElementById('cf_sl_usd').value),
    rp_require_squeeze: document.getElementById('rp_require_squeeze').value === 'true',
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

['margin','leverage','entry_mode','obi_threshold','obi_mode','obi_long_threshold','obi_short_threshold','obi_reversal_min_bounce','obi_instant_reset_ratio','obi_window_fast_seconds','obi_window_medium_seconds','obi_window_slow_seconds','obi_levels','obi_depth_weighting_enabled','obi_use_median','obi_min_liquidity','obi_breakeven_enabled','obi_breakeven_trigger_ratio','obi_breakeven_lock_usd','obi_breakeven_lock_pct','obi_tp_sl_mode','obi_tp_pct','obi_sl_pct','obi_tp_usd','obi_sl_usd','obi_cooldown_seconds','obi_trend_filter','obi_trend_ema_length','macd_resolution','macd_fast_fast','macd_fast_slow','macd_fast_signal','macd_slow_fast','macd_slow_slow','macd_slow_signal','macd_use_stochastic','stoch_k_period','stoch_k_smooth','stoch_d_period','macd_tp_usd','macd_sl_usd','macd_fast_fade_exit_enabled','macd_slow_fade_exit_enabled','macd_slow_reversal_exit_enabled','macd_fast_zero_cross_exit_enabled','fib_resolution','fib_lookback_candles','fib_entry1_level','fib_entry2_level','fib_tp1_level','fib_tp1_close_pct','fib_tp2_level','fib_sl_level','fib_cooldown_seconds','stoch_cross_resolution','stoch_cross_k_period','stoch_cross_k_smooth','stoch_cross_d_period','stoch_cross_oversold','stoch_cross_overbought','stoch_cross_tp_usd','stoch_cross_sl_usd','stoch_cross_trend_filter_enabled','stoch_cross_trend_ema_period','stoch_cross_sl_tp_mode','stoch_cross_atr_period','stoch_cross_sl_atr_mult','stoch_cross_tp_atr_mult','stoch_cross_rp_filter_enabled','stoch_cross_rp_lookback','stoch_cross_require_squeeze','stoch_cross_squeeze_lookback','stoch_cross_squeeze_threshold_pct','rp_mode','rp_resolution','rp_lookback','rp_ob_os_level','rp_tp_usd','rp_sl_usd','rp_breakeven_enabled','rp_breakeven_trigger_usd','rp_breakeven_lock_usd','rp_squeeze_lookback','rp_squeeze_threshold_pct','rp_require_squeeze','macd_simple_resolution','macd_simple_fast','macd_simple_slow','macd_simple_signal','macd_simple_tp_usd','macd_simple_sl_usd','macd_simple_exit_mode','macd_simple_early_exit_enabled','macd_simple_early_exit_bars','macd_simple_early_exit_reverse','macd_simple_breakeven_enabled','macd_simple_breakeven_trigger_usd','macd_simple_breakeven_lock_usd','cf_resolution','cf_min_body_pct','cf_sl_usd','grid_mode','grid_step_pct','tp_step_pct','grid_step_usd','tp_step_usd','max_nachkauf','dry_run','auto_reverse'].forEach(id => {
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
        "obi_current": st.get("obi_current"), "obi_fast": st.get("obi_fast"),
        "obi_medium": st.get("obi_medium"), "obi_slow": st.get("obi_slow"),
        "obi_history": st.get("obi_history", [])[-300:],
        "macd_fast_hist": st.get("macd_fast_hist"), "macd_slow_hist": st.get("macd_slow_hist"),
        "macd_stoch_k": st.get("macd_stoch_k"), "macd_stoch_d": st.get("macd_stoch_d"),
        "fib": st.get("fib"),
        "stoch_cross_k": st.get("stoch_cross_k"), "stoch_cross_d": st.get("stoch_cross_d"),
        "stoch_cross_sl_price": st.get("stoch_cross_sl_price"), "stoch_cross_tp_price": st.get("stoch_cross_tp_price"),
        "stoch_cross_rp_mid": st.get("stoch_cross_rp_mid"), "stoch_cross_squeeze_active": st.get("stoch_cross_squeeze_active"),
        "rp_osc": st.get("rp_osc"), "rp_mid_price": st.get("rp_mid_price"),
        "rp_range_high": st.get("rp_range_high"), "rp_range_low": st.get("rp_range_low"),
        "rp_breakeven_triggered": st.get("rp_breakeven_triggered"),
        "rp_channel_width": st.get("rp_channel_width"), "rp_avg_width": st.get("rp_avg_width"),
        "rp_squeeze_active": st.get("rp_squeeze_active"),
        "macd_simple_hist": st.get("macd_simple_hist"),
        "macd_simple_hist_history": st.get("macd_simple_hist_history", [])[-200:],
        "binance_1s_buffer_size": len(st.get("binance_1s_buffer", [])),
        "binance_1s_buffer_span_sec": (
            (st["binance_1s_buffer"][-1]["ts"] - st["binance_1s_buffer"][0]["ts"]) // 1000
            if len(st.get("binance_1s_buffer", [])) > 1 else 0
        ),
        "local_1s_buffer_size": len(st.get("local_1s_buffer", [])),
        "cf_last_color": st.get("cf_last_color"), "cf_last_body_pct": st.get("cf_last_body_pct"),
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
                "obi_threshold", "obi_mode", "obi_long_threshold", "obi_short_threshold", "obi_reversal_min_bounce", "obi_instant_reset_ratio", "obi_window_fast_seconds", "obi_window_medium_seconds", "obi_window_slow_seconds", "obi_levels", "obi_depth_weighting_enabled", "obi_use_median", "obi_min_liquidity", "obi_breakeven_enabled", "obi_breakeven_trigger_ratio", "obi_breakeven_lock_usd", "obi_breakeven_lock_pct", "obi_tp_sl_mode", "obi_tp_pct", "obi_sl_pct", "obi_tp_usd", "obi_sl_usd",
                "obi_cooldown_seconds", "obi_trend_filter", "obi_trend_ema_length",
                "macd_resolution", "macd_fast_fast", "macd_fast_slow", "macd_fast_signal",
                "macd_slow_fast", "macd_slow_slow", "macd_slow_signal", "macd_use_stochastic",
                "stoch_k_period", "stoch_k_smooth", "stoch_d_period", "macd_tp_usd", "macd_sl_usd",
                "macd_fast_fade_exit_enabled",
                "macd_slow_fade_exit_enabled",
                "macd_slow_reversal_exit_enabled",
                "macd_fast_zero_cross_exit_enabled",
                "fib_resolution", "fib_lookback_candles", "fib_entry1_level", "fib_entry2_level",
                "fib_tp1_level", "fib_tp1_close_pct", "fib_tp2_level", "fib_sl_level", "fib_cooldown_seconds",
                "stoch_cross_resolution", "stoch_cross_k_period", "stoch_cross_k_smooth", "stoch_cross_d_period",
                "stoch_cross_oversold", "stoch_cross_overbought", "stoch_cross_tp_usd", "stoch_cross_sl_usd",
                "stoch_cross_trend_filter_enabled", "stoch_cross_trend_ema_period",
                "stoch_cross_sl_tp_mode", "stoch_cross_atr_period", "stoch_cross_sl_atr_mult", "stoch_cross_tp_atr_mult",
                "stoch_cross_rp_filter_enabled", "stoch_cross_rp_lookback",
                "stoch_cross_require_squeeze", "stoch_cross_squeeze_lookback", "stoch_cross_squeeze_threshold_pct",
                "rp_mode", "rp_resolution", "rp_lookback", "rp_ob_os_level", "rp_tp_usd", "rp_sl_usd",
                "rp_breakeven_enabled", "rp_breakeven_trigger_usd", "rp_breakeven_lock_usd",
                "rp_squeeze_lookback", "rp_squeeze_threshold_pct", "rp_require_squeeze",
                "macd_simple_resolution", "macd_simple_fast", "macd_simple_slow", "macd_simple_signal",
                "macd_simple_tp_usd", "macd_simple_sl_usd", "macd_simple_exit_mode",
                "macd_simple_early_exit_enabled", "macd_simple_early_exit_bars", "macd_simple_early_exit_reverse",
                "macd_simple_breakeven_enabled", "macd_simple_breakeven_trigger_usd", "macd_simple_breakeven_lock_usd",
                "cf_resolution", "cf_min_body_pct", "cf_sl_usd"]:
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


async def handle_backtest(request):
    from strategies import run_backtest
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    body = await request.json()
    days = body.get("days", 30)
    try:
        days = max(1, min(365, int(days)))
    except (TypeError, ValueError):
        days = 30
    cfg = dict(BOTS[symbol]["config"])  # Kopie - Backtest darf die Live-Config nicht veraendern
    entry_mode = cfg["entry_mode"]
    result = await run_backtest(symbol, entry_mode, cfg, days)
    return web.json_response(result)


async def handle_backtest_sweep(request):
    from strategies import run_backtest_sweep
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    body = await request.json()
    days = body.get("days", 30)
    try:
        days = max(1, min(365, int(days)))
    except (TypeError, ValueError):
        days = 30
    cfg = dict(BOTS[symbol]["config"])  # Kopie - Sweep darf die Live-Config nicht veraendern
    entry_mode = cfg["entry_mode"]
    result = await run_backtest_sweep(symbol, entry_mode, cfg, days)
    return web.json_response(result)


async def handle_manual_trade(request):
    """Manueller Buy/Sell-Button (Pocket-Trading-Panel, laeuft parallel zur Automatik):
    - flach -> neue Position in die geklickte Richtung
    - gleiche Richtung bereits offen -> Nachkauf (Ø-Einstieg wird angepasst)
    - Gegenrichtung offen -> erst schliessen, dann in die geklickte Richtung neu eroeffnen"""
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    body = await request.json()
    direction = body.get("direction")
    if direction not in ("long", "short"):
        return web.json_response({"error": "direction muss 'long' oder 'short' sein"}, status=400)
    st = BOTS[symbol]["state"]
    if st["last_price"] is None:
        return web.json_response({"error": "kein aktueller Preis bekannt"}, status=400)
    price = st["last_price"]

    if st["position"] is not None and st["position"] != direction:
        await execute_exit(symbol, price, "MANUAL-REVERSE")
        price = st["last_price"]

    is_add_on = st["position"] == direction
    ok = await execute_entry(symbol, direction, price, is_add_on=is_add_on)
    if not ok:
        return web.json_response({"error": "Order fehlgeschlagen - siehe Log"}, status=500)
    return web.json_response({"success": True, "position": st["position"], "avg_entry_price": st["avg_entry_price"]})


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


