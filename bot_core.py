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
        "entry_mode": os.getenv("ENTRY_MODE", "grid"),  # "grid", "obi_scalp", "fib_reversal", "range_profile", "supertrend_fusion", "chandelier_exit", "ut_bot", "wavetrend_cross", "signal_grid"
        "margin": float(os.getenv("GRID_MARGIN", "20")),
        "leverage": int(os.getenv("GRID_LEVERAGE", "3")),
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
        # Spread-Filter: verwirft Signale bei ungewoehnlich weitem Bid/Ask-Spread (Prozent vom Mid-Preis).
        # Ein weiter Spread bedeutet duennes/chaotisches Buch - genau dort ist OBI am unzuverlaessigsten
        # (Microstructure-Forschung: hoher Spread korreliert mit hoeheren Handelskosten und weniger
        # verlaesslichem Orderbuch-Signal).
        "obi_spread_filter_enabled": os.getenv("OBI_SPREAD_FILTER_ENABLED", "false").lower() == "true",
        "obi_max_spread_pct": float(os.getenv("OBI_MAX_SPREAD_PCT", "0.05")),
        # Volatilitaets-Regime-Filter: verwirft Signale, wenn die kurzfristige Preis-Schwankung (Hoch-Tief-
        # Spanne der letzten Ticks in % vom Durchschnittspreis) ausserhalb eines Normalbands liegt.
        # Zu niedrig = totes/seitwaertsrauschendes Buch (OBI-Zittern ohne Fortsetzung), zu hoch = News-Spike/
        # Wick-Risiko (OBI kann in Sekunden komplett drehen). Beide Enden erzeugen erfahrungsgemaess
        # ueberproportional viele Fehlsignale.
        "obi_vol_filter_enabled": os.getenv("OBI_VOL_FILTER_ENABLED", "false").lower() == "true",
        "obi_vol_window_seconds": float(os.getenv("OBI_VOL_WINDOW_SECONDS", "30")),
        "obi_vol_min_pct": float(os.getenv("OBI_VOL_MIN_PCT", "0.0")),
        "obi_vol_max_pct": float(os.getenv("OBI_VOL_MAX_PCT", "1.0")),
        # OBI-Momentum-Scalp (oms_): eigenstaendige neue Strategie - OBI (3-Fenster) + CVD-
        # Bestaetigung (echtes Trade-Tape) + optionaler Funding-Filter. Exit: TP1 (Teilverkauf)
        # + Trailing-Stop auf Rest, SL von Anfang an fester $-Betrag (NICHT die Liquidation).
        "oms_levels": int(os.getenv("OMS_LEVELS", "10")),
        "oms_obi_threshold": float(os.getenv("OMS_OBI_THRESHOLD", "0.35")),
        "oms_window_fast_seconds": float(os.getenv("OMS_WINDOW_FAST_SECONDS", "3")),
        "oms_window_medium_seconds": float(os.getenv("OMS_WINDOW_MEDIUM_SECONDS", "10")),
        "oms_window_slow_seconds": float(os.getenv("OMS_WINDOW_SLOW_SECONDS", "30")),
        "oms_cvd_confirm_enabled": os.getenv("OMS_CVD_CONFIRM_ENABLED", "true").lower() == "true",
        "oms_cvd_window_seconds": float(os.getenv("OMS_CVD_WINDOW_SECONDS", "10")),
        "oms_cvd_min_ratio": float(os.getenv("OMS_CVD_MIN_RATIO", "0.15")),
        "oms_funding_filter_enabled": os.getenv("OMS_FUNDING_FILTER_ENABLED", "true").lower() == "true",
        "oms_funding_max_abs": float(os.getenv("OMS_FUNDING_MAX_ABS", "0.0005")),
        "oms_cooldown_seconds": float(os.getenv("OMS_COOLDOWN_SECONDS", "5")),
        "oms_tp1_usd": float(os.getenv("OMS_TP1_USD", "2.5")),
        "oms_tp1_close_pct": float(os.getenv("OMS_TP1_CLOSE_PCT", "50")),
        "oms_sl_usd": float(os.getenv("OMS_SL_USD", "3.5")),
        "oms_trail_distance_usd": float(os.getenv("OMS_TRAIL_DISTANCE_USD", "1.5")),
        "oms_dca_enabled": os.getenv("OMS_DCA_ENABLED", "true").lower() == "true",
        "oms_dca_max_entries": int(os.getenv("OMS_DCA_MAX_ENTRIES", "2")),
        "oms_dca_size_fraction": float(os.getenv("OMS_DCA_SIZE_FRACTION", "0.6")),
        "oms_dca_min_pullback_usd": float(os.getenv("OMS_DCA_MIN_PULLBACK_USD", "1.0")),
        "fib_resolution": os.getenv("FIB_RESOLUTION", "1h"),  # "1h" oder "4h"
        "fib_lookback_candles": int(os.getenv("FIB_LOOKBACK_CANDLES", "100")),
        "fib_entry1_level": float(os.getenv("FIB_ENTRY1_LEVEL", "0.882")),
        "fib_entry2_level": float(os.getenv("FIB_ENTRY2_LEVEL", "0.941")),
        "fib_tp1_level": float(os.getenv("FIB_TP1_LEVEL", "0.786")),
        "fib_tp2_level": float(os.getenv("FIB_TP2_LEVEL", "0.667")),
        "fib_sl_level": float(os.getenv("FIB_SL_LEVEL", "1.0")),
        "fib_tp1_close_pct": float(os.getenv("FIB_TP1_CLOSE_PCT", "50")),
        "fib_cooldown_seconds": float(os.getenv("FIB_COOLDOWN_SECONDS", "300")),
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
        "rp_require_squeeze": os.getenv("RP_REQUIRE_SQUEEZE", "false").lower() == "true",
        "zscore_lookback_period": int(os.getenv("ZSCORE_LOOKBACK_PERIOD", "20")),
        "zscore_ema_smooth": int(os.getenv("ZSCORE_EMA_SMOOTH", "3")),
        "zscore_threshold": float(os.getenv("ZSCORE_THRESHOLD", "0.1")),  # Long bei +Schwelle, Short bei -Schwelle
        # Trend-Meter (portiert aus dem TradingView-Indikator "Trend Meter" von Lij_MC):
        # 3 Punkte + obere Linie, alle 4 muessen uebereinstimmen fuer einen Einstieg.
        # Regime-Filter: schaltet automatisch zwischen normal und invertiert um, je nachdem ob
        # der Choppiness-Index gerade "Seitwaerts" oder "Trend" anzeigt (siehe SuperTrend Fusion
        # SuperTrend Fusion (portiert aus "SuperTrend Fusion - ATP" von AlgoTrade_Pro,
        # Average-Force-Baustein urspruenglich von racer8):
        "stf_resolution": os.getenv("STF_RESOLUTION", "5m"),
        "stf_atr_period": int(os.getenv("STF_ATR_PERIOD", "10")),
        "stf_factor": float(os.getenv("STF_FACTOR", "3")),
        "stf_use_af_filter": os.getenv("STF_USE_AF_FILTER", "true").lower() == "true",
        "stf_af_period": int(os.getenv("STF_AF_PERIOD", "18")),
        "stf_af_smooth": int(os.getenv("STF_AF_SMOOTH", "6")),
        "stf_use_chop_filter": os.getenv("STF_USE_CHOP_FILTER", "true").lower() == "true",
        "stf_chop_length": int(os.getenv("STF_CHOP_LENGTH", "14")),
        "stf_chop_threshold": int(os.getenv("STF_CHOP_THRESHOLD", "50")),
        "stf_entry_trigger": os.getenv("STF_ENTRY_TRIGGER", "candle_close"),
        "stf_exit_trigger": os.getenv("STF_EXIT_TRIGGER", "candle_close"),
        "stf_invert_direction": os.getenv("STF_INVERT_DIRECTION", "false").lower() == "true",
        "stf_use_ema_filter": os.getenv("STF_USE_EMA_FILTER", "false").lower() == "true",
        "stf_ema_length": int(os.getenv("STF_EMA_LENGTH", "200")),
        "stf_tp_enabled": os.getenv("STF_TP_ENABLED", "false").lower() == "true",
        "stf_tp_usd": float(os.getenv("STF_TP_USD", "3")),
        "stf_sl_enabled": os.getenv("STF_SL_ENABLED", "false").lower() == "true",
        "stf_sl_usd": float(os.getenv("STF_SL_USD", "3")),
        # Chandelier Exit (portiert aus "MG signal [The_lurker]" - nur der Buy/Sell-Signal-Teil,
        # MagicTrend und Order-Blocks wurden bewusst weggelassen):
        "ce_resolution": os.getenv("CE_RESOLUTION", "1m"),
        "ce_atr_period": int(os.getenv("CE_ATR_PERIOD", "2")),
        "ce_atr_mult": float(os.getenv("CE_ATR_MULT", "1.85")),
        "ce_use_close": os.getenv("CE_USE_CLOSE", "true").lower() == "true",
        "ce_invert_direction": os.getenv("CE_INVERT_DIRECTION", "false").lower() == "true",
        "ce_entry_trigger": os.getenv("CE_ENTRY_TRIGGER", "candle_close"),
        "ce_exit_trigger": os.getenv("CE_EXIT_TRIGGER", "candle_close"),
        "ce_tp_enabled": os.getenv("CE_TP_ENABLED", "false").lower() == "true",
        "ce_tp_usd": float(os.getenv("CE_TP_USD", "3")),
        "ce_sl_enabled": os.getenv("CE_SL_ENABLED", "false").lower() == "true",
        "ce_sl_usd": float(os.getenv("CE_SL_USD", "3")),
        "ce_sl_cooldown_seconds": float(os.getenv("CE_SL_COOLDOWN_SECONDS", "30")),
        # SuperTrend-Fusion-Richtungsfilter auf hoeherem Zeitrahmen (nutzt dieselben stf_*-Filter-
        # Parameter wie oben, nur mit eigener - typischerweise hoeherer - Aufloesung):
        "ce_stf_filter_enabled": os.getenv("CE_STF_FILTER_ENABLED", "false").lower() == "true",
        "ce_stf_resolution": os.getenv("CE_STF_RESOLUTION", "5m"),
        # UT-Bot-Trailing-Stop (portiert aus dem "UT Bot"-Baustein des "Wave Cipher SMC Flow
        # System"):
        "ut_resolution": os.getenv("UT_RESOLUTION", "5m"),
        "ut_atr_period": int(os.getenv("UT_ATR_PERIOD", "6")),
        "ut_key_value": float(os.getenv("UT_KEY_VALUE", "2")),
        "ut_entry_trigger": os.getenv("UT_ENTRY_TRIGGER", "candle_close"),
        "ut_exit_trigger": os.getenv("UT_EXIT_TRIGGER", "candle_close"),
        "ut_invert_direction": os.getenv("UT_INVERT_DIRECTION", "false").lower() == "true",
        "ut_tp_enabled": os.getenv("UT_TP_ENABLED", "false").lower() == "true",
        "ut_tp_usd": float(os.getenv("UT_TP_USD", "3")),
        "ut_sl_enabled": os.getenv("UT_SL_ENABLED", "false").lower() == "true",
        "ut_sl_usd": float(os.getenv("UT_SL_USD", "3")),
        "ut_sl_cooldown_seconds": float(os.getenv("UT_SL_COOLDOWN_SECONDS", "30")),
        # HalfTrend (portiert aus "HalfTrend Long/Short Signal Engine [BigBeluga]", Basis:
        # everget's HalfTrend-Indikator): ATR-Periode ist im Original fest auf 100. Channel-
        # Deviation und Base-Risk-Multiplikator sind hier (anders als im rein optischen Original)
        # echte SL-/TP-Abstands-Multiplikatoren (in ATR2-Vielfachen), damit beide Parameter
        # tatsaechlich das Backtest-/Sweep-Ergebnis beeinflussen:
        "ht_resolution": os.getenv("HT_RESOLUTION", "5m"),
        "ht_amplitude": int(os.getenv("HT_AMPLITUDE", "20")),
        "ht_channel_deviation": float(os.getenv("HT_CHANNEL_DEVIATION", "2.0")),
        "ht_base_risk_mult": float(os.getenv("HT_BASE_RISK_MULT", "3.0")),
        "ht_entry_trigger": os.getenv("HT_ENTRY_TRIGGER", "candle_close"),
        "ht_exit_trigger": os.getenv("HT_EXIT_TRIGGER", "candle_close"),
        "ht_invert_direction": os.getenv("HT_INVERT_DIRECTION", "false").lower() == "true",
        "ht_tp_enabled": os.getenv("HT_TP_ENABLED", "true").lower() == "true",
        "ht_tp1_close_pct": float(os.getenv("HT_TP1_CLOSE_PCT", "33")),
        "ht_tp2_close_pct": float(os.getenv("HT_TP2_CLOSE_PCT", "50")),
        "ht_sl_enabled": os.getenv("HT_SL_ENABLED", "true").lower() == "true",
        "ht_sl_cooldown_seconds": float(os.getenv("HT_SL_COOLDOWN_SECONDS", "30")),
        # WaveTrend-Cross (portiert aus dem Cipher-B-WaveTrend-Baustein des "Wave Cipher SMC
        # Flow System"):
        "wtc_resolution": os.getenv("WTC_RESOLUTION", "5m"),
        "wtc_channel_length": int(os.getenv("WTC_CHANNEL_LENGTH", "9")),
        "wtc_average_length": int(os.getenv("WTC_AVERAGE_LENGTH", "12")),
        "wtc_ma_length": int(os.getenv("WTC_MA_LENGTH", "3")),
        "wtc_require_obos": os.getenv("WTC_REQUIRE_OBOS", "true").lower() == "true",
        "wtc_ob_level": int(os.getenv("WTC_OB_LEVEL", "53")),
        "wtc_os_level": int(os.getenv("WTC_OS_LEVEL", "-53")),
        "wtc_entry_trigger": os.getenv("WTC_ENTRY_TRIGGER", "candle_close"),
        "wtc_exit_trigger": os.getenv("WTC_EXIT_TRIGGER", "candle_close"),
        "wtc_invert_direction": os.getenv("WTC_INVERT_DIRECTION", "false").lower() == "true",
        "wtc_tp_enabled": os.getenv("WTC_TP_ENABLED", "false").lower() == "true",
        "wtc_tp_usd": float(os.getenv("WTC_TP_USD", "3")),
        "wtc_sl_enabled": os.getenv("WTC_SL_ENABLED", "false").lower() == "true",
        "wtc_sl_usd": float(os.getenv("WTC_SL_USD", "3")),
        "wtc_sl_cooldown_seconds": float(os.getenv("WTC_SL_COOLDOWN_SECONDS", "30")),
        # Richtungsmodus, Nachkauf und SuperTrend-Richtungsfilter (analog Chandelier Exit):
        "wtc_direction_mode": os.getenv("WTC_DIRECTION_MODE", "both"),  # "both"/"long_only"/"short_only"
        "wtc_dca_enabled": os.getenv("WTC_DCA_ENABLED", "false").lower() == "true",
        "wtc_dca_max_entries": int(os.getenv("WTC_DCA_MAX_ENTRIES", "10")),
        "wtc_dca_cooldown_seconds": float(os.getenv("WTC_DCA_COOLDOWN_SECONDS", "60")),
        "wtc_stf_filter_enabled": os.getenv("WTC_STF_FILTER_ENABLED", "false").lower() == "true",
        "wtc_stf_resolution": os.getenv("WTC_STF_RESOLUTION", "5m"),
        # Signal-Grid (Grid-Mechanik dupliziert, aber Ein-/Nachkauf durch Indikator-Signal statt
        # Preisabstand - nutzt dieselben wtc_*/zscore_*-Felder je nach gewaehlter Quelle):
        "sg_signal_source": os.getenv("SG_SIGNAL_SOURCE", "wavetrend"),  # "wavetrend" oder "zscore"
        "sg_resolution": os.getenv("SG_RESOLUTION", "5m"),
        "sg_entry_trigger": os.getenv("SG_ENTRY_TRIGGER", "candle_close"),
        "sg_tp_mode": os.getenv("SG_TP_MODE", "pct"),  # "pct" oder "usd"
        "sg_tp_step_pct": float(os.getenv("SG_TP_STEP_PCT", "1.0")),
        "sg_tp_step_usd": float(os.getenv("SG_TP_STEP_USD", "5")),
        "sg_max_nachkauf": int(os.getenv("SG_MAX_NACHKAUF", "0")),  # 0 = unbegrenzt, wie beim Grid
        "sg_dca_cooldown_seconds": float(os.getenv("SG_DCA_COOLDOWN_SECONDS", "10")),
        "sg_invert_direction": os.getenv("SG_INVERT_DIRECTION", "false").lower() == "true",
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
        "obi_spread_pct": None, "obi_recent_vol_pct": None,
        "oms_obi_buffer": [], "oms_obi_fast": None, "oms_obi_medium": None, "oms_obi_slow": None,
        "oms_cvd_buffer": [], "oms_cvd_ratio": None,
        "oms_funding_rate": None, "oms_last_signal_direction": None, "oms_last_trade_time": 0.0,
        "oms_signal": None, "oms_obi_direction": None, "oms_cvd_ok": None, "oms_funding_ok": None,
        "oms_obi_history": [],
        "oms_tp1_done": False, "oms_trail_price": None,
        "oms_dca_count": 0, "oms_last_entry_price": None,
        "oms_price_history": [], "oms_markers": [],
        "fib": None, "fib_entry1_done": False, "fib_entry2_done": False, "fib_tp1_done": False,
        "fib_sl_active_price": None, "fib_last_trade_time": 0.0,
        "rp_osc": None, "rp_mid_price": None, "rp_range_high": None, "rp_range_low": None,
        "rp_breakeven_triggered": False,
        "stf_highs": [], "stf_lows": [], "stf_closes": [], "stf_direction": None, "stf_chop_value": None,
        "ce_highs": [], "ce_lows": [], "ce_closes": [], "ce_direction": None,
        "ce_stf_bias": None, "ce_pending_direction": None, "ce_sl_cooldown_until": 0.0,
        "ut_highs": [], "ut_lows": [], "ut_closes": [], "ut_stop_value": None, "ut_sl_cooldown_until": 0.0,
        "wtc_highs": [], "wtc_lows": [], "wtc_closes": [], "wtc_wt1": None, "wtc_wt2": None, "wtc_sl_cooldown_until": 0.0,
        "wtc_stf_bias": None, "wtc_pending_direction": None, "wtc_last_dca_ts": 0.0,
        "sg_highs": [], "sg_lows": [], "sg_closes": [], "sg_last_dca_ts": 0.0,
        "rp_width_history": [], "rp_channel_width": None, "rp_avg_width": None,
        "rp_squeeze_active": False, "rp_squeeze_was_active": False,
        "binance_1s_buffer": [],
        "local_1s_bucket_start": None, "local_1s_candle_open": None,
        "local_1s_candle_high": None, "local_1s_candle_low": None, "local_1s_candle_last": None,
        "local_1s_buffer": [],
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


# Nur diese State-Felder ueberleben einen Redeploy - bewusst OHNE die grossen/kurzlebigen
# Arbeitspuffer (Preis-Historie, Orderbuch, 1s-Kerzen-Puffer etc.), die sich ohnehin
# innerhalb von Sekunden bis Minuten nach dem Neustart von selbst wieder auffuellen.
# Ohne das hier wuerde jeder Bot nach jedem Redeploy "vergessen", dass er gerade in
# einer Position steckt, wie viele Nachkaeufe schon liefen und wie sein Ø-Einstieg war.
PERSISTED_STATE_KEYS = [
    "position", "avg_entry_price", "total_coin_size", "entry_count", "anchor_price",
    "position_opened_at", "last_entry_price", "stats", "trade_log",
    "fib", "fib_entry1_done", "fib_entry2_done", "fib_tp1_done", "fib_sl_active_price",
    "rp_breakeven_triggered", "obi_breakeven_triggered",
    "ht_sl_price", "ht_tp1_price", "ht_tp2_price", "ht_tp3_price", "ht_tp1_done", "ht_tp2_done",
]


async def save_bot_state():
    r = await get_redis()
    if r is None:
        return
    try:
        data = {}
        for s in SYMBOLS:
            st = BOTS[s]["state"]
            entry = {k: st[k] for k in PERSISTED_STATE_KEYS if k in st}
            if "trade_log" in entry:
                entry["trade_log"] = entry["trade_log"][-200:]  # nicht unbegrenzt wachsen lassen
            data[s] = entry
        await r.set("gridbot:state", json.dumps(data, default=str))
    except Exception as e:
        debug_log("⚠️ Speichern des Bot-States fehlgeschlagen", {"error": str(e)})


async def load_bot_state():
    r = await get_redis()
    if r is None:
        return
    try:
        raw_state = await r.get("gridbot:state")
        if raw_state:
            saved = json.loads(raw_state)
            for s in SYMBOLS:
                if s in saved:
                    BOTS[s]["state"].update(saved[s])
            debug_log("✅ Bot-State (offene Positionen etc.) aus Redis geladen", {"coins": list(saved.keys())})
    except Exception as e:
        debug_log("⚠️ Laden des Bot-States fehlgeschlagen", {"error": str(e)})


async def state_persist_loop():
    """Sicherheitsnetz: speichert den Bot-State auch periodisch, nicht nur direkt bei
    Entry/Exit - faengt z.B. Breakeven-Trigger ab, die zwischen zwei Trades passieren."""
    while True:
        await asyncio.sleep(60)
        await save_bot_state()


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



async def execute_entry(symbol, direction, price, is_add_on, size_multiplier=1.0):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    market_index = MARKET_INDICES[symbol]

    position_usdc = cfg["margin"] * cfg["leverage"] * size_multiplier
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
            await client.close()
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
    await save_bot_state()
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
    await save_bot_state()
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
    await save_bot_state()

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
  th.sortable { cursor:pointer; user-select:none; }
  th.sortable:hover { color:var(--accent); }
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

<div id="oms-trend-meter" style="display:none; margin-bottom:12px; padding:20px; border-radius:14px; text-align:center; font-weight:800; font-size:28px; letter-spacing:0.03em; transition:background 0.3s;"></div>
<div id="oms-trend-meter-detail" style="display:none; margin-bottom:12px; font-size:13px; color:var(--text-dim); text-align:center;"></div>
<div id="oms-gauge-wrap" style="display:none; margin-bottom:12px;"></div>
<div id="oms-checklist-wrap" style="display:none; margin-bottom:12px;"></div>
<div id="oms-chart-wrap" style="display:none; margin-bottom:12px;"></div>

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

<div id="obi-chart-section" style="display:none; margin-bottom:12px;">
  <h2 class="section-title">OBI-Verlauf (schnell / mittel / langsam)</h2>
  <div style="position:relative; height:200px;"><canvas id="obiChart"></canvas></div>
</div>

<div id="generic-chart-wrap">
  <h2 class="section-title">Kursverlauf</h2>
  <div style="position:relative; height:400px;"><canvas id="priceChart"></canvas></div>
</div>

<details id="zone-settings" open style="margin-top:8px;">
<summary style="cursor:pointer; font-size:18px; font-weight:700; padding:10px 0; color:var(--text);">⚙️ Steuerung &amp; Einstellungen (aufklappen/einklappen)</summary>

<div style="margin-bottom:20px;">
  <button id="btn-start" class="start">▶️ Start</button>
  <button id="btn-stop" class="stop">⏸️ Stop</button>
  <button id="btn-close" class="danger">✖️ Position jetzt schließen</button>
  <button id="btn-reset" class="neutral">🔄 Reset (Statistik)</button>
</div>

<h2 class="section-title">Übersicht</h2>
<div class="grid" id="status-grid"></div>

<h2 class="section-title">Einstellungen (nur für den ausgewählten Coin)</h2>
<div class="panel-card">
<form id="config-form">
  <div><label>Margin (USDC)</label><input type="number" step="any" id="margin"></div>

  <div><label>Hebel</label><input type="number" step="1" id="leverage"></div>
  <div><label>Strategie</label>
    <select class="cfg" id="entry_mode">
      <option value="grid">Neutrales Grid (Ø-Einstieg/Nachkauf/TP)</option>
      <option value="obi_scalp">OBI-Scalp (Orderbuch-Ungleichgewicht, symmetrisches TP/SL)</option>
      <option value="oms_scalp">OBI-Momentum-Scalp (OBI + CVD-Bestätigung + Funding-Filter, TP1+Trailing, Nachkauf)</option>
      <option value="fib_reversal">Fibonacci-Reversal (Einstieg 0.882/0.941, TP 0.786/0.667, SL 1.0)</option>
      <option value="range_profile">Range-Profile (Point-of-Control-Kanal, Reversion oder Momentum, fester TP/SL)</option>
      <option value="supertrend_fusion">SuperTrend Fusion (ATR-SuperTrend + Average-Force-Momentum + Choppiness-Filter, optional SL+TP)</option>
      <option value="chandelier_exit">Chandelier Exit (Trailing-Stop-Flip aus "MG signal", optional TP + SuperTrend-Richtungsfilter im höheren Zeitrahmen)</option>
      <option value="ut_bot">UT-Bot (ATR-Trailing-Stop, ein gemeinsames Band, optional SL+TP, invertierbar)</option>
      <option value="halftrend">HalfTrend (Swing-Hoch/-Tief-Trendwechsel, optional ATR2-basiertes SL+TP, invertierbar)</option>
      <option value="wavetrend_cross">WaveTrend-Cross (Cipher-B-WaveTrend-Kreuzung, optional nur überkauft/überverkauft, optional SL+TP, invertierbar, Richtungsmodus, Nachkauf, SuperTrend-Richtungsfilter)</option>
      <option value="signal_grid">Signal-Grid (Grid-Mechanik: kein Flip-Exit, TP %/$ vom Ø-Einstieg, Nachkauf bei jedem neuen WaveTrend-/Z-Score-Signal in dieselbe Richtung, bis Stufen-Limit)</option>
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
  <div data-mode="obi_scalp"><label>Spread-Filter (verwirft Signale bei zu weitem Bid/Ask-Spread)</label>
    <select class="cfg" id="obi_spread_filter_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="obi_scalp"><label>Max. Spread (% vom Mid-Preis)</label><input type="number" step="0.0001" id="obi_max_spread_pct"></div>
  <div data-mode="obi_scalp"><label>Volatilitäts-Regime-Filter (verwirft Signale außerhalb Normalband)</label>
    <select class="cfg" id="obi_vol_filter_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="obi_scalp"><label>Volatilitäts-Fenster (Sek.)</label><input type="number" step="1" id="obi_vol_window_seconds"></div>
  <div data-mode="obi_scalp"><label>Min. Volatilität (% Hoch-Tief-Spanne, darunter = zu ruhig)</label><input type="number" step="0.0001" id="obi_vol_min_pct"></div>
  <div data-mode="obi_scalp"><label>Max. Volatilität (% Hoch-Tief-Spanne, darüber = zu wild)</label><input type="number" step="0.0001" id="obi_vol_max_pct"></div>

  <div data-mode="oms_scalp" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:6px 0;">
    📡 <b>Einstieg</b> nur wenn Orderbuch (OBI) UND echte Trades (CVD) übereinstimmend in dieselbe Richtung zeigen.
    🎯 <b>Ausstieg</b>: erst Teilgewinn (TP1), Rest wird eng nachgezogen (Trailing). SL ist ein fester $-Betrag von Anfang an.
    ➕ <b>Nachkauf</b>: nur wenn Signal nach Rücksetzer erneut bestätigt, mit fallender Größe.
  </div>
  <div data-mode="oms_scalp"><label>Orderbuch-Tiefe (Preisstufen)</label><input type="number" step="1" id="oms_levels"></div>
  <div data-mode="oms_scalp"><label>OBI-Schwelle (0-1, höher = strenger)</label><input type="number" step="0.01" id="oms_obi_threshold"></div>
  <div data-mode="oms_scalp"><label>OBI Zeitfenster schnell (Sek.)</label><input type="number" step="1" id="oms_window_fast_seconds"></div>
  <div data-mode="oms_scalp"><label>OBI Zeitfenster mittel (Sek.)</label><input type="number" step="1" id="oms_window_medium_seconds"></div>
  <div data-mode="oms_scalp"><label>OBI Zeitfenster langsam (Sek.)</label><input type="number" step="1" id="oms_window_slow_seconds"></div>
  <div data-mode="oms_scalp"><label>CVD-Bestätigung (echte Trade-Richtung muss zustimmen)</label>
    <select class="cfg" id="oms_cvd_confirm_enabled">
      <option value="true">An (empfohlen)</option>
      <option value="false">Aus</option>
    </select>
  </div>
  <div data-mode="oms_scalp"><label>CVD Zeitfenster (Sek.)</label><input type="number" step="1" id="oms_cvd_window_seconds"></div>
  <div data-mode="oms_scalp"><label>CVD Mindest-Verhältnis (0-1)</label><input type="number" step="0.01" id="oms_cvd_min_ratio"></div>
  <div data-mode="oms_scalp"><label>Funding-Filter (nicht in überfüllte Richtung nachlegen)</label>
    <select class="cfg" id="oms_funding_filter_enabled">
      <option value="true">An (empfohlen)</option>
      <option value="false">Aus</option>
    </select>
  </div>
  <div data-mode="oms_scalp"><label>Funding-Grenze (absolut, z.B. 0.0005 = 0.05%)</label><input type="number" step="0.0001" id="oms_funding_max_abs"></div>
  <div data-mode="oms_scalp"><label>Cooldown zwischen Signalen (Sek.)</label><input type="number" step="1" id="oms_cooldown_seconds"></div>
  <div data-mode="oms_scalp"><label>TP1 Ziel ($, Teilverkauf)</label><input type="number" step="0.1" id="oms_tp1_usd"></div>
  <div data-mode="oms_scalp"><label>TP1 Teilverkauf (% der Position)</label><input type="number" step="1" id="oms_tp1_close_pct"></div>
  <div data-mode="oms_scalp"><label>Stop-Loss ($, gesamte Position - NICHT die Liquidation)</label><input type="number" step="0.1" id="oms_sl_usd"></div>
  <div data-mode="oms_scalp"><label>Trailing-Abstand nach TP1 ($)</label><input type="number" step="0.1" id="oms_trail_distance_usd"></div>
  <div data-mode="oms_scalp"><label>Nachkauf (DCA)</label>
    <select class="cfg" id="oms_dca_enabled">
      <option value="true">An</option>
      <option value="false">Aus</option>
    </select>
  </div>
  <div data-mode="oms_scalp"><label>Nachkauf: max. Stufen</label><input type="number" step="1" id="oms_dca_max_entries"></div>
  <div data-mode="oms_scalp"><label>Nachkauf: Größen-Faktor je Stufe (0-1, fallend)</label><input type="number" step="0.05" id="oms_dca_size_fraction"></div>
  <div data-mode="oms_scalp"><label>Nachkauf: Mindest-Rücksetzer ($, bevor nachgekauft wird)</label><input type="number" step="0.1" id="oms_dca_min_pullback_usd"></div>

  <div data-mode="fib_reversal"><label>Zeitrahmen</label>
    <select class="cfg" id="fib_resolution">
      <option value="10s">10 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="15s">15 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="30s">30 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="45s">45 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
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
      <option value="45s">45 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="1m">1 Minute</option>
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
  <div data-mode="range_profile"><label>Nur nach Squeeze einsteigen</label>
    <select class="cfg" id="rp_require_squeeze">
      <option value="false">Aus - Squeeze nur zur Anzeige</option>
      <option value="true">An - Einstieg nur wenn direkt vorher ein Squeeze aktiv war</option>
    </select>
  </div>
  <div data-mode="signal_grid"><label>Z-Score-Preset (aus dem Original-Indikator, setzt Lookback-Periode + Schwelle - nur bei Signal-Quelle Z-Score)</label>
    <select id="zscore_preset" onchange="if(this.value){const [lb,th]=this.value.split(':'); document.getElementById('zscore_lookback_period').value=lb; document.getElementById('zscore_threshold').value=th; window.formTouched=true;} this.value='';">
      <option value="">- auswählen -</option>
      <option value="10:1.0">Scalping (Lookback 10, Schwelle 1.0) - 1-15min Charts, sehr reaktionsschnell</option>
      <option value="20:1.5">Default (Lookback 20, Schwelle 1.5) - universell einsetzbar</option>
      <option value="25:1.8">Swing Trading (Lookback 25, Schwelle 1.8) - 1-4h/Tages-Charts</option>
      <option value="40:2.2">Trend Following (Lookback 40, Schwelle 2.2) - nur etablierte Trends, Tages-/Wochen-Charts</option>
    </select>
  </div>
  <div data-mode="signal_grid"><label>Z-Score: Lookback-Periode (nur bei Signal-Quelle Z-Score)</label><input type="number" step="1" id="zscore_lookback_period"></div>
  <div data-mode="signal_grid"><label>Z-Score: EMA-Glättung (nur bei Signal-Quelle Z-Score)</label><input type="number" step="1" id="zscore_ema_smooth"></div>
  <div data-mode="signal_grid"><label>Z-Score: Schwelle (nur bei Signal-Quelle Z-Score)</label><input type="number" step="0.1" id="zscore_threshold"></div>
  <div data-mode="supertrend_fusion"><label>Zeitrahmen</label>
    <select class="cfg" id="stf_resolution">
      <option value="10s">10 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="15s">15 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="30s">30 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="45s">45 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="1m">1 Minute</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
    </select>
  </div>
  <div data-mode="supertrend_fusion"><label>ATR-Länge</label><input type="number" step="1" id="stf_atr_period"></div>
  <div data-mode="supertrend_fusion"><label>Faktor (Band-Breite)</label><input type="number" step="0.1" id="stf_factor"></div>
  <div data-mode="supertrend_fusion"><label>Average-Force-Filter (Momentum muss in Trendrichtung zeigen)</label>
    <select class="cfg" id="stf_use_af_filter">
      <option value="true">An</option>
      <option value="false">Aus</option>
    </select>
  </div>
  <div data-mode="supertrend_fusion"><label>Average-Force: Periode</label><input type="number" step="1" id="stf_af_period"></div>
  <div data-mode="supertrend_fusion"><label>Average-Force: Glättung</label><input type="number" step="1" id="stf_af_smooth"></div>
  <div data-mode="supertrend_fusion"><label>Choppiness-Filter (nur bei erkanntem Trend einsteigen)</label>
    <select class="cfg" id="stf_use_chop_filter">
      <option value="true">An</option>
      <option value="false">Aus</option>
    </select>
  </div>
  <div data-mode="supertrend_fusion"><label>Choppiness: Fenster (Kerzen)</label><input type="number" step="1" id="stf_chop_length"></div>
  <div data-mode="supertrend_fusion"><label>Choppiness: Schwelle (darunter = Trend, erlaubt Einstieg)</label><input type="number" step="1" min="1" max="100" id="stf_chop_threshold"></div>
  <div data-mode="supertrend_fusion"><label>Einstieg auslösen</label>
    <select class="cfg" id="stf_entry_trigger">
      <option value="candle_close">Bei Kerzenschluss</option>
      <option value="tick">Sofort bei jedem Preis-Tick</option>
    </select>
  </div>
  <div data-mode="supertrend_fusion"><label>Ausstieg auslösen</label>
    <select class="cfg" id="stf_exit_trigger">
      <option value="candle_close">Bei Kerzenschluss</option>
      <option value="tick">Sofort bei jedem Preis-Tick</option>
    </select>
  </div>
  <div data-mode="supertrend_fusion"><label>Richtung invertieren (Kontra-Modus: SuperTrend-Long → Short, SuperTrend-Short → Long)</label>
    <select class="cfg" id="stf_invert_direction">
      <option value="false">Aus (normal)</option>
      <option value="true">An (invertiert)</option>
    </select>
  </div>
  <div data-mode="supertrend_fusion"><label>EMA-Trendfilter (nur Long über der EMA, nur Short darunter - gilt für die tatsächliche Richtung, auch bei Invertiert)</label>
    <select class="cfg" id="stf_use_ema_filter">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="supertrend_fusion"><label>EMA-Länge</label><input type="number" step="1" id="stf_ema_length"></div>
  <div data-mode="supertrend_fusion"><label>Take-Profit</label>
    <select class="cfg" id="stf_tp_enabled">
      <option value="false">Aus (nur Trend-Flip-Exit)</option>
      <option value="true">An - fester $-Betrag</option>
    </select>
  </div>
  <div data-mode="supertrend_fusion"><label>TP-Betrag ($, nur wenn TP an)</label><input type="number" step="any" id="stf_tp_usd"></div>
  <div data-mode="supertrend_fusion"><label>Stop-Loss</label>
    <select class="cfg" id="stf_sl_enabled">
      <option value="false">Aus (nur Trend-Flip-Exit)</option>
      <option value="true">An - fester $-Betrag</option>
    </select>
  </div>
  <div data-mode="supertrend_fusion"><label>SL-Betrag ($, nur wenn SL an)</label><input type="number" step="any" id="stf_sl_usd"></div>
  <div data-mode="chandelier_exit"><label>Zeitrahmen (Buy/Sell-Signal)</label>
    <select class="cfg" id="ce_resolution">
      <option value="10s">10 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="15s">15 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="30s">30 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="45s">45 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="1m">1 Minute</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
    </select>
  </div>
  <div data-mode="chandelier_exit"><label>ATR-Periode (auch Hoch/Tief-Fenster)</label><input type="number" step="1" id="ce_atr_period"></div>
  <div data-mode="chandelier_exit"><label>ATR-Multiplikator</label><input type="number" step="0.05" id="ce_atr_mult"></div>
  <div data-mode="chandelier_exit"><label>Extrempunkte aus Schlusskurs (an) oder Docht Hoch/Tief (aus)</label>
    <select class="cfg" id="ce_use_close">
      <option value="true">Schlusskurs</option>
      <option value="false">Docht (Hoch/Tief)</option>
    </select>
  </div>
  <div data-mode="chandelier_exit"><label>Richtung invertieren (Kontra-Modus: Buy-Signal → Short, Sell-Signal → Long)</label>
    <select class="cfg" id="ce_invert_direction">
      <option value="false">Aus (normal)</option>
      <option value="true">An (invertiert)</option>
    </select>
  </div>
  <div data-mode="chandelier_exit"><label>Einstieg auslösen</label>
    <select class="cfg" id="ce_entry_trigger">
      <option value="candle_close">Bei Kerzenschluss</option>
      <option value="tick">Sofort bei jedem Preis-Tick</option>
    </select>
  </div>
  <div data-mode="chandelier_exit"><label>Ausstieg auslösen</label>
    <select class="cfg" id="ce_exit_trigger">
      <option value="candle_close">Bei Kerzenschluss</option>
      <option value="tick">Sofort bei jedem Preis-Tick</option>
    </select>
  </div>
  <div data-mode="chandelier_exit"><label>Take-Profit (kein SL vorgesehen)</label>
    <select class="cfg" id="ce_tp_enabled">
      <option value="false">Aus (nur Gegen-Signal-Exit)</option>
      <option value="true">An - fester $-Betrag</option>
    </select>
  </div>
  <div data-mode="chandelier_exit"><label>TP-Betrag ($, nur wenn TP an)</label><input type="number" step="any" id="ce_tp_usd"></div>
  <div data-mode="chandelier_exit"><label>Stop-Loss</label>
    <select class="cfg" id="ce_sl_enabled">
      <option value="false">Aus (nur Gegen-Signal-Exit)</option>
      <option value="true">An - fester $-Betrag</option>
    </select>
  </div>
  <div data-mode="chandelier_exit"><label>SL-Betrag ($, nur wenn SL an)</label><input type="number" step="any" id="ce_sl_usd"></div>
  <div data-mode="chandelier_exit"><label>Cooldown nach SL (Sek., verhindert sofortiges Wieder-Einsteigen)</label><input type="number" step="1" id="ce_sl_cooldown_seconds"></div>
  <div data-mode="chandelier_exit"><label>SuperTrend-Richtungsfilter (höherer Zeitrahmen, nutzt die SuperTrend-Fusion-Filtereinstellungen oben)</label>
    <select class="cfg" id="ce_stf_filter_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="chandelier_exit"><label>SuperTrend-Zeitrahmen (sollte höher sein als das Signal oben)</label>
    <select class="cfg" id="ce_stf_resolution">
      <option value="1m">1 Minute</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
    </select>
  </div>
  <div data-mode="ut_bot"><label>Zeitrahmen</label>
    <select class="cfg" id="ut_resolution">
      <option value="10s">10 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="15s">15 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="30s">30 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="45s">45 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="1m">1 Minute</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
    </select>
  </div>
  <div data-mode="ut_bot"><label>ATR-Periode</label><input type="number" step="1" id="ut_atr_period"></div>
  <div data-mode="ut_bot"><label>Key Value (ATR-Multiplikator)</label><input type="number" step="0.1" id="ut_key_value"></div>
  <div data-mode="ut_bot"><label>Einstieg auslösen</label>
    <select class="cfg" id="ut_entry_trigger">
      <option value="candle_close">Bei Kerzenschluss</option>
      <option value="tick">Sofort bei jedem Preis-Tick</option>
    </select>
  </div>
  <div data-mode="ut_bot"><label>Ausstieg auslösen</label>
    <select class="cfg" id="ut_exit_trigger">
      <option value="candle_close">Bei Kerzenschluss</option>
      <option value="tick">Sofort bei jedem Preis-Tick</option>
    </select>
  </div>
  <div data-mode="ut_bot"><label>Richtung invertieren (Kontra-Modus)</label>
    <select class="cfg" id="ut_invert_direction">
      <option value="false">Aus (normal)</option>
      <option value="true">An (invertiert)</option>
    </select>
  </div>
  <div data-mode="ut_bot"><label>Take-Profit</label>
    <select class="cfg" id="ut_tp_enabled">
      <option value="false">Aus (nur Gegen-Signal-Exit)</option>
      <option value="true">An - fester $-Betrag</option>
    </select>
  </div>
  <div data-mode="ut_bot"><label>TP-Betrag ($, nur wenn TP an)</label><input type="number" step="any" id="ut_tp_usd"></div>
  <div data-mode="ut_bot"><label>Stop-Loss</label>
    <select class="cfg" id="ut_sl_enabled">
      <option value="false">Aus (nur Gegen-Signal-Exit)</option>
      <option value="true">An - fester $-Betrag</option>
    </select>
  </div>
  <div data-mode="ut_bot"><label>SL-Betrag ($, nur wenn SL an)</label><input type="number" step="any" id="ut_sl_usd"></div>
  <div data-mode="ut_bot"><label>Cooldown nach SL (Sek.)</label><input type="number" step="1" id="ut_sl_cooldown_seconds"></div>
  <div data-mode="halftrend"><label>Zeitrahmen</label>
    <select class="cfg" id="ht_resolution">
      <option value="10s">10 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="15s">15 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="30s">30 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="45s">45 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="1m">1 Minute</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
    </select>
  </div>
  <div data-mode="halftrend"><label>Amplitude (Swing-Lookback)</label><input type="number" step="1" id="ht_amplitude"></div>
  <div data-mode="halftrend"><label>Channel Deviation (SL-Abstand in ATR2-Vielfachen)</label><input type="number" step="0.1" id="ht_channel_deviation"></div>
  <div data-mode="halftrend"><label>Base Risk (TP-Abstand in ATR2-Vielfachen)</label><input type="number" step="0.1" id="ht_base_risk_mult"></div>
  <div data-mode="halftrend"><label>Einstieg auslösen</label>
    <select class="cfg" id="ht_entry_trigger">
      <option value="candle_close">Bei Kerzenschluss</option>
      <option value="tick">Sofort bei jedem Preis-Tick</option>
    </select>
  </div>
  <div data-mode="halftrend"><label>Ausstieg auslösen</label>
    <select class="cfg" id="ht_exit_trigger">
      <option value="candle_close">Bei Kerzenschluss</option>
      <option value="tick">Sofort bei jedem Preis-Tick</option>
    </select>
  </div>
  <div data-mode="halftrend"><label>Richtung invertieren (Kontra-Modus)</label>
    <select class="cfg" id="ht_invert_direction">
      <option value="false">Aus (normal)</option>
      <option value="true">An (invertiert)</option>
    </select>
  </div>
  <div data-mode="halftrend"><label>Take-Profit-Stufen (TP1/TP2/TP3, ATR2 × Base Risk 1x/2x/3x)</label>
    <select class="cfg" id="ht_tp_enabled">
      <option value="false">Aus (nur Gegen-Signal-Exit)</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="halftrend"><label>TP1 Teilverkauf (% der Position)</label><input type="number" step="1" id="ht_tp1_close_pct"></div>
  <div data-mode="halftrend"><label>TP2 Teilverkauf (% der verbleibenden Position)</label><input type="number" step="1" id="ht_tp2_close_pct"></div>
  <div data-mode="halftrend"><label>Stop-Loss (ATR2 × Channel Deviation, springt nach TP1 auf Break-Even)</label>
    <select class="cfg" id="ht_sl_enabled">
      <option value="false">Aus (nur Gegen-Signal-Exit)</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="halftrend"><label>Cooldown nach SL (Sek.)</label><input type="number" step="1" id="ht_sl_cooldown_seconds"></div>
  <div data-mode="wavetrend_cross"><label>Zeitrahmen</label>
    <select class="cfg" id="wtc_resolution">
      <option value="10s">10 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="15s">15 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="30s">30 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="45s">45 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="1m">1 Minute</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
    </select>
  </div>
  <div data-mode="wavetrend_cross"><label>Kanal-Länge</label><input type="number" step="1" id="wtc_channel_length"></div>
  <div data-mode="wavetrend_cross"><label>Durchschnitts-Länge</label><input type="number" step="1" id="wtc_average_length"></div>
  <div data-mode="wavetrend_cross"><label>Glättungs-Länge (SMA)</label><input type="number" step="1" id="wtc_ma_length"></div>
  <div data-mode="wavetrend_cross"><label>Nur bei überkauft/überverkauft (sonst jede Kreuzung)</label>
    <select class="cfg" id="wtc_require_obos">
      <option value="true">An (selektiver)</option>
      <option value="false">Aus (jede Kreuzung zählt)</option>
    </select>
  </div>
  <div data-mode="wavetrend_cross"><label>Überkauft-Level (Sell-Schwelle)</label><input type="number" step="1" id="wtc_ob_level"></div>
  <div data-mode="wavetrend_cross"><label>Überverkauft-Level (Buy-Schwelle)</label><input type="number" step="1" id="wtc_os_level"></div>
  <div data-mode="wavetrend_cross"><label>Einstieg auslösen</label>
    <select class="cfg" id="wtc_entry_trigger">
      <option value="candle_close">Bei Kerzenschluss</option>
      <option value="tick">Sofort bei jedem Preis-Tick</option>
    </select>
  </div>
  <div data-mode="wavetrend_cross"><label>Ausstieg auslösen</label>
    <select class="cfg" id="wtc_exit_trigger">
      <option value="candle_close">Bei Kerzenschluss</option>
      <option value="tick">Sofort bei jedem Preis-Tick</option>
    </select>
  </div>
  <div data-mode="wavetrend_cross"><label>Richtung invertieren (Kontra-Modus)</label>
    <select class="cfg" id="wtc_invert_direction">
      <option value="false">Aus (normal)</option>
      <option value="true">An (invertiert)</option>
    </select>
  </div>
  <div data-mode="wavetrend_cross"><label>Take-Profit</label>
    <select class="cfg" id="wtc_tp_enabled">
      <option value="false">Aus (nur Gegen-Signal-Exit)</option>
      <option value="true">An - fester $-Betrag</option>
    </select>
  </div>
  <div data-mode="wavetrend_cross"><label>TP-Betrag ($, nur wenn TP an)</label><input type="number" step="any" id="wtc_tp_usd"></div>
  <div data-mode="wavetrend_cross"><label>Stop-Loss</label>
    <select class="cfg" id="wtc_sl_enabled">
      <option value="false">Aus (nur Gegen-Signal-Exit)</option>
      <option value="true">An - fester $-Betrag</option>
    </select>
  </div>
  <div data-mode="wavetrend_cross"><label>SL-Betrag ($, nur wenn SL an)</label><input type="number" step="any" id="wtc_sl_usd"></div>
  <div data-mode="wavetrend_cross"><label>Cooldown nach SL (Sek.)</label><input type="number" step="1" id="wtc_sl_cooldown_seconds"></div>
  <div data-mode="wavetrend_cross"><label>Richtungsmodus</label>
    <select class="cfg" id="wtc_direction_mode">
      <option value="both">Beide Richtungen</option>
      <option value="long_only">Nur Long</option>
      <option value="short_only">Nur Short</option>
    </select>
  </div>
  <div data-mode="wavetrend_cross"><label>Nachkauf (bei anhaltendem Signal in Richtung der offenen Position)</label>
    <select class="cfg" id="wtc_dca_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="wavetrend_cross"><label>Max. Nachkauf-Stufen</label><input type="number" step="1" id="wtc_dca_max_entries"></div>
  <div data-mode="wavetrend_cross"><label>Mindestabstand zwischen Nachkäufen (Sek.)</label><input type="number" step="1" id="wtc_dca_cooldown_seconds"></div>
  <div data-mode="wavetrend_cross"><label>SuperTrend-Richtungsfilter (höherer Zeitrahmen, nutzt die SuperTrend-Fusion-Filtereinstellungen)</label>
    <select class="cfg" id="wtc_stf_filter_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="wavetrend_cross"><label>SuperTrend-Zeitrahmen (sollte höher sein als das Signal oben)</label>
    <select class="cfg" id="wtc_stf_resolution">
      <option value="1m">1 Minute</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
    </select>
  </div>
  <div data-mode="signal_grid"><label>Signal-Quelle</label>
    <select class="cfg" id="sg_signal_source">
      <option value="wavetrend">WaveTrend (Cipher B)</option>
      <option value="zscore">Z-Score-Trend</option>
    </select>
  </div>
  <div data-mode="signal_grid"><label>Zeitrahmen</label>
    <select class="cfg" id="sg_resolution">
      <option value="10s">10 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="15s">15 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="30s">30 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="45s">45 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="1m">1 Minute</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
    </select>
  </div>
  <div data-mode="signal_grid"><label>Einstieg auslösen</label>
    <select class="cfg" id="sg_entry_trigger">
      <option value="candle_close">Bei Kerzenschluss</option>
      <option value="tick">Sofort bei jedem Preis-Tick</option>
    </select>
  </div>
  <div data-mode="signal_grid"><label>Richtung invertieren (Kontra-Modus)</label>
    <select class="cfg" id="sg_invert_direction">
      <option value="false">Aus (normal)</option>
      <option value="true">An (invertiert)</option>
    </select>
  </div>
  <div data-mode="signal_grid"><label>Take-Profit-Modus (kein Stop-Loss, wie beim normalen Grid)</label>
    <select class="cfg" id="sg_tp_mode">
      <option value="pct">Prozent (%) vom Ø-Einstieg</option>
      <option value="usd">Fester $-Betrag</option>
    </select>
  </div>
  <div data-mode="signal_grid"><label>TP-Abstand (%)</label><input type="number" step="0.1" id="sg_tp_step_pct"></div>
  <div data-mode="signal_grid"><label>TP-Abstand ($)</label><input type="number" step="any" id="sg_tp_step_usd"></div>
  <div data-mode="signal_grid"><label>Max. Nachkauf-Stufen (0 = unbegrenzt)</label><input type="number" step="1" id="sg_max_nachkauf"></div>
  <div data-mode="signal_grid"><label>Mindestabstand zwischen Nachkäufen (Sek., Sicherheitsnetz - Nachkauf braucht ohnehin ein echtes neues Signal)</label><input type="number" step="1" id="sg_dca_cooldown_seconds"></div>
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

<div id="backtest-zone">
<h2 class="section-title">📊 Backtest (mit den oben gespeicherten Einstellungen)</h2>
<div class="panel-card">
  <div style="font-size:13px; color:var(--text-dim); margin-bottom:12px;">
    Testet die aktuell gespeicherten Strategie-Einstellungen gegen echte historische Binance-Kerzen.
    Nur für Range-Profile, Fibonacci-Reversal und Z-Score-Trend (Grid/OBI-Scalp brauchen
    historische Orderbuch-Daten, die es nicht gibt). SL/TP werden pro Kerze am Schlusskurs geprüft,
    nicht Tick-für-Tick wie live. Lighter ist gebührenfrei, es werden also keine Gebühren simuliert.
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:16px;">
    <div><label>Zeitraum (Tage)</label><input type="number" step="1" id="backtest-days" value="30" style="width:100px;"></div>
    <button id="btn-backtest" style="padding:12px 24px;">▶️ Backtest starten</button>
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
    <div style="display:flex; gap:24px; flex-wrap:wrap; margin-top:8px; padding-top:12px; border-top:1px solid var(--border);">
      <div>
        <div class="label" style="margin-bottom:6px;">🟢 Nur Long</div>
        <div style="display:flex; gap:16px; flex-wrap:wrap;">
          <div><div class="label">Trades</div><div class="value" id="bt-long-trades">-</div></div>
          <div><div class="label">Trefferquote</div><div class="value" id="bt-long-winrate">-</div></div>
          <div><div class="label">PnL $</div><div class="value" id="bt-long-pnl">-</div></div>
          <div><div class="label">Ø Gewinn / Ø Verlust $</div><div class="value" id="bt-long-avg">-</div></div>
        </div>
      </div>
      <div>
        <div class="label" style="margin-bottom:6px;">🔴 Nur Short</div>
        <div style="display:flex; gap:16px; flex-wrap:wrap;">
          <div><div class="label">Trades</div><div class="value" id="bt-short-trades">-</div></div>
          <div><div class="label">Trefferquote</div><div class="value" id="bt-short-winrate">-</div></div>
          <div><div class="label">PnL $</div><div class="value" id="bt-short-pnl">-</div></div>
          <div><div class="label">Ø Gewinn / Ø Verlust $</div><div class="value" id="bt-short-avg">-</div></div>
        </div>
      </div>
    </div>
    <div class="label" style="margin-top:16px; margin-bottom:8px;">Letzte Trades (max. 50, neueste zuerst)</div>
    <table id="bt-trades-table">
      <thead><tr>
        <th class="sortable" data-key="entry_ts">Start ⇅</th>
        <th class="sortable" data-key="dir">Richtung ⇅</th>
        <th class="sortable" data-key="entry">Einstieg $ ⇅</th>
        <th class="sortable" data-key="exit_ts">Ende ⇅</th>
        <th class="sortable" data-key="exit">Ausstieg $ ⇅</th>
        <th class="sortable" data-key="reason">Grund ⇅</th>
        <th class="sortable" data-key="pnl">PnL $ ⇅</th>
      </tr></thead>
      <tbody></tbody>
    </table>
  </div>
</div>

<div data-mode-section="chandelier_exit" style="display:none;">
<h2 class="section-title">🎲 Chandelier-Parameter-Sweep (ATR-Periode × ATR-Multiplikator)</h2>
<div class="panel-card">
  <div style="font-size:13px; color:var(--text-dim); margin-bottom:12px;">
    Testet alle Kombinationen aus ATR-Periode und ATR-Multiplikator im angegebenen Bereich gegeneinander
    (auf denselben, nur einmal geladenen Kerzen) und zeigt die besten zuerst. Ergebnisse mit weniger als
    5 Trades sind statistisch kaum aussagekräftig und werden nach unten sortiert, aber nicht versteckt.
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Zeitraum (Tage)</label><input type="number" step="1" id="sweep-days" value="30" style="width:90px;"></div>
    <div><label>ATR-Periode von</label><input type="number" step="1" id="sweep-period-min" value="1" style="width:80px;"></div>
    <div><label>bis</label><input type="number" step="1" id="sweep-period-max" value="10" style="width:80px;"></div>
    <div><label>Schritt</label><input type="number" step="1" id="sweep-period-step" value="1" style="width:70px;"></div>
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>ATR-Multiplikator von</label><input type="number" step="0.1" id="sweep-mult-min" value="0.5" style="width:80px;"></div>
    <div><label>bis</label><input type="number" step="0.1" id="sweep-mult-max" value="3.0" style="width:80px;"></div>
    <div><label>Schritt</label><input type="number" step="0.05" id="sweep-mult-step" value="0.1" style="width:70px;"></div>
    <div><label>SuperTrend-Filter im Sweep</label>
      <select id="sweep-stf-enabled" style="width:110px;">
        <option value="false">Aus</option>
        <option value="true">An</option>
      </select>
    </div>
    <button id="btn-sweep" style="padding:12px 24px;">🎲 Sweep starten</button>
  </div>
  <div id="sweep-status" style="color:var(--text-dim); font-size:13px;"></div>
  <table id="sweep-results-table" style="display:none; margin-top:12px;">
    <thead><tr>
      <th class="sortable" data-key="ce_atr_period">ATR-Periode ⇅</th>
      <th class="sortable" data-key="ce_atr_mult">ATR-Multiplikator ⇅</th>
      <th class="sortable" data-key="trades">Trades ⇅</th>
      <th class="sortable" data-key="win_rate_pct">Trefferquote ⇅</th>
      <th class="sortable" data-key="total_pnl_usd">PnL $ ⇅</th>
      <th class="sortable" data-key="max_drawdown_usd">Max DD $ ⇅</th>
      <th class="sortable" data-key="avg_bars_held">Ø Kerzen gehalten ⇅</th>
    </tr></thead>
    <tbody></tbody>
  </table>
  <h3 style="margin-top:20px; font-size:14px; color:var(--text-dim); display:none;" id="sweep-worst-title">📉 Die 20 schlechtesten Kombinationen (nach PnL, unabhängig von der Trade-Anzahl)</h3>
  <table id="sweep-worst-table" style="display:none; margin-top:8px;">
    <thead><tr>
      <th class="sortable" data-key="ce_atr_period">ATR-Periode ⇅</th>
      <th class="sortable" data-key="ce_atr_mult">ATR-Multiplikator ⇅</th>
      <th class="sortable" data-key="trades">Trades ⇅</th>
      <th class="sortable" data-key="win_rate_pct">Trefferquote ⇅</th>
      <th class="sortable" data-key="total_pnl_usd">PnL $ ⇅</th>
      <th class="sortable" data-key="max_drawdown_usd">Max DD $ ⇅</th>
      <th class="sortable" data-key="avg_bars_held">Ø Kerzen gehalten ⇅</th>
    </tr></thead>
    <tbody></tbody>
  </table>
</div>
</div>

<div data-mode-section="ut_bot" style="display:none;">
<h2 class="section-title">🎲 UT-Bot-Parameter-Sweep (ATR-Periode × Key Value)</h2>
<div class="panel-card">
  <div style="font-size:13px; color:var(--text-dim); margin-bottom:12px;">
    Testet alle Kombinationen aus ATR-Periode und Key-Value-Multiplikator im angegebenen Bereich gegeneinander
    (auf denselben, nur einmal geladenen Kerzen) und zeigt die besten zuerst. Ergebnisse mit weniger als
    5 Trades sind statistisch kaum aussagekräftig und werden nach unten sortiert, aber nicht versteckt.
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Zeitraum (Tage)</label><input type="number" step="1" id="ut-sweep-days" value="30" style="width:90px;"></div>
    <div><label>ATR-Periode von</label><input type="number" step="1" id="ut-sweep-period-min" value="1" style="width:80px;"></div>
    <div><label>bis</label><input type="number" step="1" id="ut-sweep-period-max" value="10" style="width:80px;"></div>
    <div><label>Schritt</label><input type="number" step="1" id="ut-sweep-period-step" value="1" style="width:70px;"></div>
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Key Value von</label><input type="number" step="0.1" id="ut-sweep-kv-min" value="0.5" style="width:80px;"></div>
    <div><label>bis</label><input type="number" step="0.1" id="ut-sweep-kv-max" value="3.0" style="width:80px;"></div>
    <div><label>Schritt</label><input type="number" step="0.05" id="ut-sweep-kv-step" value="0.1" style="width:70px;"></div>
    <button id="btn-ut-sweep" style="padding:12px 24px;">🎲 Sweep starten</button>
  </div>
  <div id="ut-sweep-status" style="color:var(--text-dim); font-size:13px;"></div>
  <table id="ut-sweep-results-table" style="display:none; margin-top:12px;">
    <thead><tr>
      <th class="sortable" data-key="ut_atr_period">ATR-Periode ⇅</th>
      <th class="sortable" data-key="ut_key_value">Key Value ⇅</th>
      <th class="sortable" data-key="trades">Trades ⇅</th>
      <th class="sortable" data-key="win_rate_pct">Trefferquote ⇅</th>
      <th class="sortable" data-key="total_pnl_usd">PnL $ ⇅</th>
      <th class="sortable" data-key="max_drawdown_usd">Max DD $ ⇅</th>
      <th class="sortable" data-key="avg_bars_held">Ø Kerzen gehalten ⇅</th>
    </tr></thead>
    <tbody></tbody>
  </table>
  <h3 style="margin-top:20px; font-size:14px; color:var(--text-dim); display:none;" id="ut-sweep-worst-title">📉 Die 20 schlechtesten Kombinationen (nach PnL, unabhängig von der Trade-Anzahl)</h3>
  <table id="ut-sweep-worst-table" style="display:none; margin-top:8px;">
    <thead><tr>
      <th class="sortable" data-key="ut_atr_period">ATR-Periode ⇅</th>
      <th class="sortable" data-key="ut_key_value">Key Value ⇅</th>
      <th class="sortable" data-key="trades">Trades ⇅</th>
      <th class="sortable" data-key="win_rate_pct">Trefferquote ⇅</th>
      <th class="sortable" data-key="total_pnl_usd">PnL $ ⇅</th>
      <th class="sortable" data-key="max_drawdown_usd">Max DD $ ⇅</th>
      <th class="sortable" data-key="avg_bars_held">Ø Kerzen gehalten ⇅</th>
    </tr></thead>
    <tbody></tbody>
  </table>
</div>
</div>

<div data-mode-section="halftrend" style="display:none;">
<h2 class="section-title">🎲 HalfTrend-Parameter-Sweep (Amplitude × Channel Deviation × Base Risk)</h2>
<div class="panel-card">
  <div style="font-size:13px; color:var(--text-dim); margin-bottom:12px;">
    Testet alle Kombinationen aus Amplitude (Swing-Lookback), Channel Deviation (SL-Abstand) und
    Base Risk (TP-Abstand) im angegebenen Bereich gegeneinander (Kerzen werden nur einmal geladen,
    Trend/ATR2 nur einmal pro Amplitude neu berechnet) und zeigt die besten zuerst. Ergebnisse mit
    weniger als 5 Trades sind statistisch kaum aussagekräftig und werden nach unten sortiert, aber
    nicht versteckt.
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Zeitraum (Tage)</label><input type="number" step="1" id="ht-sweep-days" value="30" style="width:90px;"></div>
    <div><label>Amplitude von</label><input type="number" step="1" id="ht-sweep-amp-min" value="10" style="width:80px;"></div>
    <div><label>bis</label><input type="number" step="1" id="ht-sweep-amp-max" value="40" style="width:80px;"></div>
    <div><label>Schritt</label><input type="number" step="1" id="ht-sweep-amp-step" value="2" style="width:70px;"></div>
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Channel Dev von</label><input type="number" step="0.1" id="ht-sweep-cd-min" value="1.0" style="width:80px;"></div>
    <div><label>bis</label><input type="number" step="0.1" id="ht-sweep-cd-max" value="4.0" style="width:80px;"></div>
    <div><label>Schritt</label><input type="number" step="0.1" id="ht-sweep-cd-step" value="0.5" style="width:70px;"></div>
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Base Risk von</label><input type="number" step="0.1" id="ht-sweep-br-min" value="1.0" style="width:80px;"></div>
    <div><label>bis</label><input type="number" step="0.1" id="ht-sweep-br-max" value="5.0" style="width:80px;"></div>
    <div><label>Schritt</label><input type="number" step="0.5" id="ht-sweep-br-step" value="0.5" style="width:70px;"></div>
    <button id="btn-ht-sweep" style="padding:12px 24px;">🎲 Sweep starten</button>
  </div>
  <div id="ht-sweep-status" style="color:var(--text-dim); font-size:13px;"></div>
  <table id="ht-sweep-results-table" style="display:none; margin-top:12px;">
    <thead><tr>
      <th class="sortable" data-key="ht_amplitude">Amplitude ⇅</th>
      <th class="sortable" data-key="ht_channel_deviation">Channel Dev ⇅</th>
      <th class="sortable" data-key="ht_base_risk_mult">Base Risk ⇅</th>
      <th class="sortable" data-key="trades">Trades ⇅</th>
      <th class="sortable" data-key="win_rate_pct">Trefferquote ⇅</th>
      <th class="sortable" data-key="total_pnl_usd">PnL $ ⇅</th>
      <th class="sortable" data-key="max_drawdown_usd">Max DD $ ⇅</th>
      <th class="sortable" data-key="avg_bars_held">Ø Kerzen gehalten ⇅</th>
    </tr></thead>
    <tbody></tbody>
  </table>
  <h3 style="margin-top:20px; font-size:14px; color:var(--text-dim); display:none;" id="ht-sweep-worst-title">📉 Die 20 schlechtesten Kombinationen (nach PnL, unabhängig von der Trade-Anzahl)</h3>
  <table id="ht-sweep-worst-table" style="display:none; margin-top:8px;">
    <thead><tr>
      <th class="sortable" data-key="ht_amplitude">Amplitude ⇅</th>
      <th class="sortable" data-key="ht_channel_deviation">Channel Dev ⇅</th>
      <th class="sortable" data-key="ht_base_risk_mult">Base Risk ⇅</th>
      <th class="sortable" data-key="trades">Trades ⇅</th>
      <th class="sortable" data-key="win_rate_pct">Trefferquote ⇅</th>
      <th class="sortable" data-key="total_pnl_usd">PnL $ ⇅</th>
      <th class="sortable" data-key="max_drawdown_usd">Max DD $ ⇅</th>
      <th class="sortable" data-key="avg_bars_held">Ø Kerzen gehalten ⇅</th>
    </tr></thead>
    <tbody></tbody>
  </table>
</div>
</div>

<div data-mode-section="supertrend_fusion" style="display:none;">
<h2 class="section-title">🎲 SuperTrend-Fusion-Parameter-Sweep (ATR-Periode × Faktor)</h2>
<div class="panel-card">
  <div style="font-size:13px; color:var(--text-dim); margin-bottom:12px;">
    Testet alle Kombinationen aus ATR-Periode und Faktor (Band-Breite) im angegebenen Bereich gegeneinander
    (auf denselben, nur einmal geladenen Kerzen) und zeigt die besten zuerst. Average-Force-/Choppiness-Filter
    bleiben dabei so eingestellt, wie sie aktuell im Formular stehen. Ergebnisse mit weniger als
    5 Trades sind statistisch kaum aussagekräftig und werden nach unten sortiert, aber nicht versteckt.
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Zeitraum (Tage)</label><input type="number" step="1" id="stf-sweep-days" value="30" style="width:90px;"></div>
    <div><label>ATR-Periode von</label><input type="number" step="1" id="stf-sweep-period-min" value="1" style="width:80px;"></div>
    <div><label>bis</label><input type="number" step="1" id="stf-sweep-period-max" value="10" style="width:80px;"></div>
    <div><label>Schritt</label><input type="number" step="1" id="stf-sweep-period-step" value="1" style="width:70px;"></div>
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Faktor von</label><input type="number" step="0.1" id="stf-sweep-factor-min" value="1.0" style="width:80px;"></div>
    <div><label>bis</label><input type="number" step="0.1" id="stf-sweep-factor-max" value="5.0" style="width:80px;"></div>
    <div><label>Schritt</label><input type="number" step="0.1" id="stf-sweep-factor-step" value="0.5" style="width:70px;"></div>
    <button id="btn-stf-sweep" style="padding:12px 24px;">🎲 Sweep starten</button>
  </div>
  <div id="stf-sweep-status" style="color:var(--text-dim); font-size:13px;"></div>
  <table id="stf-sweep-results-table" style="display:none; margin-top:12px;">
    <thead><tr>
      <th class="sortable" data-key="stf_atr_period">ATR-Periode ⇅</th>
      <th class="sortable" data-key="stf_factor">Faktor ⇅</th>
      <th class="sortable" data-key="trades">Trades ⇅</th>
      <th class="sortable" data-key="win_rate_pct">Trefferquote ⇅</th>
      <th class="sortable" data-key="total_pnl_usd">PnL $ ⇅</th>
      <th class="sortable" data-key="max_drawdown_usd">Max DD $ ⇅</th>
      <th class="sortable" data-key="avg_bars_held">Ø Kerzen gehalten ⇅</th>
    </tr></thead>
    <tbody></tbody>
  </table>
  <h3 style="margin-top:20px; font-size:14px; color:var(--text-dim); display:none;" id="stf-sweep-worst-title">📉 Die 20 schlechtesten Kombinationen (nach PnL, unabhängig von der Trade-Anzahl)</h3>
  <table id="stf-sweep-worst-table" style="display:none; margin-top:8px;">
    <thead><tr>
      <th class="sortable" data-key="stf_atr_period">ATR-Periode ⇅</th>
      <th class="sortable" data-key="stf_factor">Faktor ⇅</th>
      <th class="sortable" data-key="trades">Trades ⇅</th>
      <th class="sortable" data-key="win_rate_pct">Trefferquote ⇅</th>
      <th class="sortable" data-key="total_pnl_usd">PnL $ ⇅</th>
      <th class="sortable" data-key="max_drawdown_usd">Max DD $ ⇅</th>
      <th class="sortable" data-key="avg_bars_held">Ø Kerzen gehalten ⇅</th>
    </tr></thead>
    <tbody></tbody>
  </table>
</div>
</div>

</div>

</details>

<h2 class="section-title">Letzte abgeschlossene Trades <span id="trades-debug" style="font-size:11px; color:var(--text-dim); font-weight:normal;"></span></h2>
<div class="panel-card">
<table id="trades-table"><thead><tr><th>Eröffnet</th><th>Geschlossen</th><th>Seite</th><th>Ø-Einstieg</th><th>Exit</th><th>Stufen</th><th>Grund</th><th>PnL $</th></tr></thead><tbody></tbody></table>
</div>
</div>

<script>
let priceChart;
let obiChart;
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
  document.querySelectorAll('[data-mode-section]').forEach(el => {
    el.style.display = (el.dataset.modeSection === mode) ? '' : 'none';
  });
  // OBI-Momentum-Scalp hat keinen Kerzen-Backtest (braucht Orderbuch/Trade-Tape/Funding, die es
  // historisch nicht gibt) und der grosse generische Kursverlauf-Chart ist redundant zum
  // kompakten Mini-Chart oben - beides ausblenden, damit die Seite aufgeraeumt bleibt
  const isOms = mode === 'oms_scalp';
  document.getElementById('backtest-zone').style.display = isOms ? 'none' : '';
  document.getElementById('generic-chart-wrap').style.display = isOms ? 'none' : '';
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
  sel.addEventListener('change', () => { currentSymbol = sel.value; window.formTouched = false; resetBacktestUI(); refresh(); });
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
  const btSymbol = currentSymbol;
  btn.disabled = true;
  resultsEl.style.display = 'none';
  statusEl.innerText = `⏳ Lade Kerzen von Binance und simuliere... kann bei langen Zeiträumen 1-2 Minuten dauern.`;
  try {
    const res = await fetch(`/api/backtest?symbol=${btSymbol}`, {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({days, config: buildConfigPayload()})
    });
    const data = await res.json();
    if (btSymbol !== currentSymbol) return;  // Coin wurde gewechselt während der Backtest lief
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
      document.getElementById('bt-long-trades').innerText = data.stats_long.trades;
      document.getElementById('bt-long-winrate').innerText = data.stats_long.win_rate_pct + '%';
      const longPnlEl = document.getElementById('bt-long-pnl');
      longPnlEl.innerText = data.stats_long.total_pnl_usd;
      longPnlEl.className = data.stats_long.total_pnl_usd >= 0 ? 'value green' : 'value red';
      document.getElementById('bt-long-avg').innerText = `${data.stats_long.avg_win_usd} / ${data.stats_long.avg_loss_usd}`;
      document.getElementById('bt-short-trades').innerText = data.stats_short.trades;
      document.getElementById('bt-short-winrate').innerText = data.stats_short.win_rate_pct + '%';
      const shortPnlEl = document.getElementById('bt-short-pnl');
      shortPnlEl.innerText = data.stats_short.total_pnl_usd;
      shortPnlEl.className = data.stats_short.total_pnl_usd >= 0 ? 'value green' : 'value red';
      document.getElementById('bt-short-avg').innerText = `${data.stats_short.avg_win_usd} / ${data.stats_short.avg_loss_usd}`;
      window.btTradesData = [...(data.trades || [])].reverse();  // neueste zuerst
      renderBtTrades();
      resultsEl.style.display = 'block';
    }
  } catch (e) {
    if (btSymbol !== currentSymbol) return;
    statusEl.innerText = `❌ Fehler: ${e}`;
  }
  if (btSymbol === currentSymbol) btn.disabled = false;
});

function makeSortableTable(tableId, getData, rowHtml) {
  let sortKey = null, sortAsc = true;
  function render() {
    let rows = [...getData()];
    if (sortKey) {
      rows.sort((a, b) => {
        let av = a[sortKey], bv = b[sortKey];
        if (av === null || av === undefined) av = -Infinity;
        if (bv === null || bv === undefined) bv = -Infinity;
        if (av < bv) return sortAsc ? -1 : 1;
        if (av > bv) return sortAsc ? 1 : -1;
        return 0;
      });
    }
    document.querySelector(`#${tableId} tbody`).innerHTML = rows.map(rowHtml).join('');
  }
  document.querySelectorAll(`#${tableId} th.sortable`).forEach(th => {
    th.addEventListener('click', () => {
      const key = th.dataset.key;
      if (sortKey === key) { sortAsc = !sortAsc; } else { sortKey = key; sortAsc = true; }
      render();
    });
  });
  return render;
}

function fmtTs(ts) {
  if (!ts) return '-';
  return new Date(ts).toLocaleString('de-DE', {timeZone: 'Europe/Berlin', day:'2-digit', month:'2-digit', hour:'2-digit', minute:'2-digit', second:'2-digit'});
}

window.btTradesData = [];
const renderBtTrades = makeSortableTable('bt-trades-table', () => window.btTradesData, (r, i) => `
  <tr>
    <td>${fmtTs(r.entry_ts)}</td>
    <td>${r.dir === 'long' ? '🟢 Long' : '🔴 Short'}</td>
    <td>${r.entry}</td>
    <td>${fmtTs(r.exit_ts)}</td>
    <td>${r.exit}</td>
    <td>${r.reason}</td>
    <td class="${r.pnl >= 0 ? 'green' : 'red'}">${r.pnl.toFixed(2)}</td>
  </tr>`);

function resetBacktestUI() {
  document.getElementById('backtest-results').style.display = 'none';
  document.getElementById('backtest-status').innerText = '';
  window.btTradesData = [];
  document.getElementById('sweep-status').innerText = '';
  document.getElementById('sweep-results-table').style.display = 'none';
  document.getElementById('sweep-worst-table').style.display = 'none';
  document.getElementById('sweep-worst-title').style.display = 'none';
  window.sweepResultsData = [];
  window.sweepWorstData = [];
  document.getElementById('ut-sweep-status').innerText = '';
  document.getElementById('ut-sweep-results-table').style.display = 'none';
  document.getElementById('ut-sweep-worst-table').style.display = 'none';
  document.getElementById('ut-sweep-worst-title').style.display = 'none';
  window.utSweepResultsData = [];
  window.utSweepWorstData = [];
  document.getElementById('ht-sweep-status').innerText = '';
  document.getElementById('ht-sweep-results-table').style.display = 'none';
  document.getElementById('ht-sweep-worst-table').style.display = 'none';
  document.getElementById('ht-sweep-worst-title').style.display = 'none';
  window.htSweepResultsData = [];
  window.htSweepWorstData = [];
  document.getElementById('stf-sweep-status').innerText = '';
  document.getElementById('stf-sweep-results-table').style.display = 'none';
  document.getElementById('stf-sweep-worst-table').style.display = 'none';
  document.getElementById('stf-sweep-worst-title').style.display = 'none';
  window.stfSweepResultsData = [];
  window.stfSweepWorstData = [];
}

document.getElementById('btn-sweep').addEventListener('click', async () => {
  const btn = document.getElementById('btn-sweep');
  const statusEl = document.getElementById('sweep-status');
  const tableEl = document.getElementById('sweep-results-table');
  const worstTableEl = document.getElementById('sweep-worst-table');
  const worstTitleEl = document.getElementById('sweep-worst-title');
  const sweepSymbol = currentSymbol;
  const payload = {
    days: parseInt(document.getElementById('sweep-days').value) || 30,
    atr_period_min: parseInt(document.getElementById('sweep-period-min').value),
    atr_period_max: parseInt(document.getElementById('sweep-period-max').value),
    atr_period_step: parseInt(document.getElementById('sweep-period-step').value),
    atr_mult_min: parseFloat(document.getElementById('sweep-mult-min').value),
    atr_mult_max: parseFloat(document.getElementById('sweep-mult-max').value),
    atr_mult_step: parseFloat(document.getElementById('sweep-mult-step').value),
    stf_filter_enabled: document.getElementById('sweep-stf-enabled').value === 'true',
    config: buildConfigPayload(),
  };
  btn.disabled = true;
  tableEl.style.display = 'none';
  worstTableEl.style.display = 'none';
  worstTitleEl.style.display = 'none';
  statusEl.innerText = `⏳ Lade Kerzen und teste alle Kombinationen... kann bei vielen Kombinationen etwas dauern.`;
  try {
    const res = await fetch(`/api/ce_sweep?symbol=${sweepSymbol}`, {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)
    });
    const data = await res.json();
    if (sweepSymbol !== currentSymbol) return;
    if (data.error) {
      statusEl.innerText = `❌ ${data.error}`;
    } else {
      statusEl.innerText = `${data.combos_tested} Kombinationen getestet auf ${data.candles_processed} Kerzen (${data.actual_days_covered} Tage, ${data.resolution})` +
        (data.stf_filter_used ? ' - mit SuperTrend-Filter' : ' - ohne SuperTrend-Filter') +
        ` - Ergebnisse mit weniger als ${data.min_reliable_trades} Trades sind unten einsortiert.`;
      window.sweepResultsData = data.results || [];
      window.sweepWorstData = data.worst_results || [];
      renderSweepResults();
      renderSweepWorst();
      tableEl.style.display = '';
      worstTableEl.style.display = '';
      worstTitleEl.style.display = '';
    }
  } catch (e) {
    if (sweepSymbol !== currentSymbol) return;
    statusEl.innerText = `❌ Fehler: ${e}`;
  }
  if (sweepSymbol === currentSymbol) btn.disabled = false;
});

window.sweepResultsData = [];
window.sweepWorstData = [];
const sweepRowHtml = (r) => `
  <tr>
    <td>${r.ce_atr_period}</td>
    <td>${r.ce_atr_mult}</td>
    <td>${r.trades}</td>
    <td>${r.win_rate_pct}%</td>
    <td class="${r.total_pnl_usd >= 0 ? 'green' : 'red'}">${r.total_pnl_usd}</td>
    <td>${r.max_drawdown_usd}</td>
    <td>${r.avg_bars_held}</td>
  </tr>`;
const renderSweepResults = makeSortableTable('sweep-results-table', () => window.sweepResultsData, sweepRowHtml);
const renderSweepWorst = makeSortableTable('sweep-worst-table', () => window.sweepWorstData, sweepRowHtml);

document.getElementById('btn-ut-sweep').addEventListener('click', async () => {
  const btn = document.getElementById('btn-ut-sweep');
  const statusEl = document.getElementById('ut-sweep-status');
  const tableEl = document.getElementById('ut-sweep-results-table');
  const worstTableEl = document.getElementById('ut-sweep-worst-table');
  const worstTitleEl = document.getElementById('ut-sweep-worst-title');
  const sweepSymbol = currentSymbol;
  const payload = {
    days: parseInt(document.getElementById('ut-sweep-days').value) || 30,
    atr_period_min: parseInt(document.getElementById('ut-sweep-period-min').value),
    atr_period_max: parseInt(document.getElementById('ut-sweep-period-max').value),
    atr_period_step: parseInt(document.getElementById('ut-sweep-period-step').value),
    key_value_min: parseFloat(document.getElementById('ut-sweep-kv-min').value),
    key_value_max: parseFloat(document.getElementById('ut-sweep-kv-max').value),
    key_value_step: parseFloat(document.getElementById('ut-sweep-kv-step').value),
    config: buildConfigPayload(),
  };
  btn.disabled = true;
  tableEl.style.display = 'none';
  worstTableEl.style.display = 'none';
  worstTitleEl.style.display = 'none';
  statusEl.innerText = `⏳ Lade Kerzen und teste alle Kombinationen... kann bei vielen Kombinationen etwas dauern.`;
  try {
    const res = await fetch(`/api/ut_sweep?symbol=${sweepSymbol}`, {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)
    });
    const data = await res.json();
    if (sweepSymbol !== currentSymbol) return;
    if (data.error) {
      statusEl.innerText = `❌ ${data.error}`;
    } else {
      statusEl.innerText = `${data.combos_tested} Kombinationen getestet auf ${data.candles_processed} Kerzen (${data.actual_days_covered} Tage, ${data.resolution}) - Ergebnisse mit weniger als ${data.min_reliable_trades} Trades sind unten einsortiert.`;
      window.utSweepResultsData = data.results || [];
      window.utSweepWorstData = data.worst_results || [];
      renderUtSweepResults();
      renderUtSweepWorst();
      tableEl.style.display = '';
      worstTableEl.style.display = '';
      worstTitleEl.style.display = '';
    }
  } catch (e) {
    if (sweepSymbol !== currentSymbol) return;
    statusEl.innerText = `❌ Fehler: ${e}`;
  }
  if (sweepSymbol === currentSymbol) btn.disabled = false;
});

window.utSweepResultsData = [];
window.utSweepWorstData = [];
const utSweepRowHtml = (r) => `
  <tr>
    <td>${r.ut_atr_period}</td>
    <td>${r.ut_key_value}</td>
    <td>${r.trades}</td>
    <td>${r.win_rate_pct}%</td>
    <td class="${r.total_pnl_usd >= 0 ? 'green' : 'red'}">${r.total_pnl_usd}</td>
    <td>${r.max_drawdown_usd}</td>
    <td>${r.avg_bars_held}</td>
  </tr>`;
const renderUtSweepResults = makeSortableTable('ut-sweep-results-table', () => window.utSweepResultsData, utSweepRowHtml);
const renderUtSweepWorst = makeSortableTable('ut-sweep-worst-table', () => window.utSweepWorstData, utSweepRowHtml);

document.getElementById('btn-ht-sweep').addEventListener('click', async () => {
  const btn = document.getElementById('btn-ht-sweep');
  const statusEl = document.getElementById('ht-sweep-status');
  const tableEl = document.getElementById('ht-sweep-results-table');
  const worstTableEl = document.getElementById('ht-sweep-worst-table');
  const worstTitleEl = document.getElementById('ht-sweep-worst-title');
  const sweepSymbol = currentSymbol;
  const payload = {
    days: parseInt(document.getElementById('ht-sweep-days').value) || 30,
    amplitude_min: parseInt(document.getElementById('ht-sweep-amp-min').value),
    amplitude_max: parseInt(document.getElementById('ht-sweep-amp-max').value),
    amplitude_step: parseInt(document.getElementById('ht-sweep-amp-step').value),
    channel_dev_min: parseFloat(document.getElementById('ht-sweep-cd-min').value),
    channel_dev_max: parseFloat(document.getElementById('ht-sweep-cd-max').value),
    channel_dev_step: parseFloat(document.getElementById('ht-sweep-cd-step').value),
    base_risk_min: parseFloat(document.getElementById('ht-sweep-br-min').value),
    base_risk_max: parseFloat(document.getElementById('ht-sweep-br-max').value),
    base_risk_step: parseFloat(document.getElementById('ht-sweep-br-step').value),
    config: buildConfigPayload(),
  };
  btn.disabled = true;
  tableEl.style.display = 'none';
  worstTableEl.style.display = 'none';
  worstTitleEl.style.display = 'none';
  statusEl.innerText = `⏳ Lade Kerzen und teste alle Kombinationen... kann bei vielen Kombinationen etwas dauern.`;
  try {
    const res = await fetch(`/api/ht_sweep?symbol=${sweepSymbol}`, {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)
    });
    const data = await res.json();
    if (sweepSymbol !== currentSymbol) return;
    if (data.error) {
      statusEl.innerText = `❌ ${data.error}`;
    } else {
      statusEl.innerText = `${data.combos_tested} Kombinationen getestet auf ${data.candles_processed} Kerzen (${data.actual_days_covered} Tage, ${data.resolution}) - Ergebnisse mit weniger als ${data.min_reliable_trades} Trades sind unten einsortiert.`;
      window.htSweepResultsData = data.results || [];
      window.htSweepWorstData = data.worst_results || [];
      renderHtSweepResults();
      renderHtSweepWorst();
      tableEl.style.display = '';
      worstTableEl.style.display = '';
      worstTitleEl.style.display = '';
    }
  } catch (e) {
    if (sweepSymbol !== currentSymbol) return;
    statusEl.innerText = `❌ Fehler: ${e}`;
  }
  if (sweepSymbol === currentSymbol) btn.disabled = false;
});

window.htSweepResultsData = [];
window.htSweepWorstData = [];
const htSweepRowHtml = (r) => `
  <tr>
    <td>${r.ht_amplitude}</td>
    <td>${r.ht_channel_deviation}</td>
    <td>${r.ht_base_risk_mult}</td>
    <td>${r.trades}</td>
    <td>${r.win_rate_pct}%</td>
    <td class="${r.total_pnl_usd >= 0 ? 'green' : 'red'}">${r.total_pnl_usd}</td>
    <td>${r.max_drawdown_usd}</td>
    <td>${r.avg_bars_held}</td>
  </tr>`;
const renderHtSweepResults = makeSortableTable('ht-sweep-results-table', () => window.htSweepResultsData, htSweepRowHtml);
const renderHtSweepWorst = makeSortableTable('ht-sweep-worst-table', () => window.htSweepWorstData, htSweepRowHtml);

document.getElementById('btn-stf-sweep').addEventListener('click', async () => {
  const btn = document.getElementById('btn-stf-sweep');
  const statusEl = document.getElementById('stf-sweep-status');
  const tableEl = document.getElementById('stf-sweep-results-table');
  const worstTableEl = document.getElementById('stf-sweep-worst-table');
  const worstTitleEl = document.getElementById('stf-sweep-worst-title');
  const sweepSymbol = currentSymbol;
  const payload = {
    days: parseInt(document.getElementById('stf-sweep-days').value) || 30,
    atr_period_min: parseInt(document.getElementById('stf-sweep-period-min').value),
    atr_period_max: parseInt(document.getElementById('stf-sweep-period-max').value),
    atr_period_step: parseInt(document.getElementById('stf-sweep-period-step').value),
    factor_min: parseFloat(document.getElementById('stf-sweep-factor-min').value),
    factor_max: parseFloat(document.getElementById('stf-sweep-factor-max').value),
    factor_step: parseFloat(document.getElementById('stf-sweep-factor-step').value),
    config: buildConfigPayload(),
  };
  btn.disabled = true;
  tableEl.style.display = 'none';
  worstTableEl.style.display = 'none';
  worstTitleEl.style.display = 'none';
  statusEl.innerText = `⏳ Lade Kerzen und teste alle Kombinationen... kann bei vielen Kombinationen etwas dauern.`;
  try {
    const res = await fetch(`/api/stf_sweep?symbol=${sweepSymbol}`, {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)
    });
    const data = await res.json();
    if (sweepSymbol !== currentSymbol) return;
    if (data.error) {
      statusEl.innerText = `❌ ${data.error}`;
    } else {
      statusEl.innerText = `${data.combos_tested} Kombinationen getestet auf ${data.candles_processed} Kerzen (${data.actual_days_covered} Tage, ${data.resolution}) - Ergebnisse mit weniger als ${data.min_reliable_trades} Trades sind unten einsortiert.`;
      window.stfSweepResultsData = data.results || [];
      window.stfSweepWorstData = data.worst_results || [];
      renderStfSweepResults();
      renderStfSweepWorst();
      tableEl.style.display = '';
      worstTableEl.style.display = '';
      worstTitleEl.style.display = '';
    }
  } catch (e) {
    if (sweepSymbol !== currentSymbol) return;
    statusEl.innerText = `❌ Fehler: ${e}`;
  }
  if (sweepSymbol === currentSymbol) btn.disabled = false;
});

window.stfSweepResultsData = [];
window.stfSweepWorstData = [];
const stfSweepRowHtml = (r) => `
  <tr>
    <td>${r.stf_atr_period}</td>
    <td>${r.stf_factor}</td>
    <td>${r.trades}</td>
    <td>${r.win_rate_pct}%</td>
    <td class="${r.total_pnl_usd >= 0 ? 'green' : 'red'}">${r.total_pnl_usd}</td>
    <td>${r.max_drawdown_usd}</td>
    <td>${r.avg_bars_held}</td>
  </tr>`;
const renderStfSweepResults = makeSortableTable('stf-sweep-results-table', () => window.stfSweepResultsData, stfSweepRowHtml);
const renderStfSweepWorst = makeSortableTable('stf-sweep-worst-table', () => window.stfSweepWorstData, stfSweepRowHtml);





function renderOmsGauge(fast, medium, slow, threshold) {
  const t = threshold ?? 0.35;
  const clamp = v => Math.max(-1, Math.min(1, v ?? 0));
  const pctOf = v => (clamp(v) + 1) / 2 * 100;
  const stageLabel = v => {
    if (v == null) return 'Keine Daten';
    if (v >= t) return 'STARK LONG';
    if (v >= t / 2) return 'Vor-Long';
    if (v > -t / 2) return 'Neutral';
    if (v > -t) return 'Vor-Short';
    return 'STARK SHORT';
  };
  const zoneStop1 = ((1 - t) / 2 * 100).toFixed(0);
  const zoneStop2 = (50 + t / 2 * 50).toFixed(0);
  return `<div class="panel-card" style="padding:14px;">
    <div style="font-size:12px; color:var(--text-dim); margin-bottom:8px;">Orderbuch-Ungleichgewicht (OBI) — Stufe: <b style="color:var(--text);">${stageLabel(fast)}</b></div>
    <div style="position:relative; height:28px; border-radius:6px; background:linear-gradient(90deg, #f0526b 0%, #7c3f47 ${zoneStop1}%, #3a3f52 48%, #3a3f52 52%, #2f6b45 ${zoneStop2}%, #22c55e 100%);">
      <div style="position:absolute; top:-4px; left:${pctOf(fast)}%; width:2px; height:36px; background:#fff; transform:translateX(-1px);" title="schnelles Fenster"></div>
      <div style="position:absolute; top:11px; left:${pctOf(medium)}%; width:6px; height:6px; border-radius:50%; background:#fff; opacity:0.6; transform:translateX(-3px);" title="mittleres Fenster"></div>
      <div style="position:absolute; top:11px; left:${pctOf(slow)}%; width:6px; height:6px; border-radius:50%; background:#fff; opacity:0.35; transform:translateX(-3px);" title="langsames Fenster"></div>
    </div>
    <div style="display:flex; justify-content:space-between; font-size:10px; color:var(--text-dim); margin-top:4px;">
      <span>Stark Short</span><span>Neutral</span><span>Stark Long</span>
    </div>
    <div style="font-size:11px; color:var(--text-dim); margin-top:6px;">Weiße Linie = schnelles Fenster (jetzt) · Punkte = mittel/langsam (blasser = älteres Fenster) - alle drei müssen in dieselbe Zone zeigen, damit ein Signal entsteht.</div>
  </div>`;
}

function renderOmsChecklist(data) {
  const row = (label, status, detail) => {
    const icon = status === true ? '✅' : status === false ? '❌' : '➖';
    return `<div style="display:flex; justify-content:space-between; align-items:center; padding:7px 0; border-bottom:1px solid var(--panel-border);">
      <span>${icon} ${label}</span><span style="color:var(--text-dim); font-size:12px;">${detail}</span>
    </div>`;
  };
  const obiOk = data.oms_obi_direction != null;
  const obiDetail = `schnell ${data.oms_obi_fast ?? '-'} / mittel ${data.oms_obi_medium ?? '-'} / langsam ${data.oms_obi_slow ?? '-'}`;
  const cvdDetail = data.config.oms_cvd_confirm_enabled ? `CVD ${data.oms_cvd_ratio ?? '-'} (min. ${data.config.oms_cvd_min_ratio})` : 'deaktiviert';
  const fundingDetail = data.config.oms_funding_filter_enabled ? `${data.oms_funding_rate != null ? (data.oms_funding_rate*100).toFixed(4)+'%' : '-'} (Grenze ${(data.config.oms_funding_max_abs*100).toFixed(3)}%)` : 'deaktiviert';
  return `<div class="panel-card" style="padding:14px;">
    <div style="font-size:12px; color:var(--text-dim); margin-bottom:6px;">Warum feuert (nicht)?</div>
    ${row('OBI-Übereinstimmung (3 Fenster gleiche Richtung)', obiOk, obiDetail)}
    ${row('CVD-Bestätigung', data.oms_cvd_ok, cvdDetail)}
    ${row('Funding-Filter bestanden', data.oms_funding_ok, fundingDetail)}
  </div>`;
}

function renderOmsChart(history, markers, pos) {
  if (!history || history.length < 2) {
    return '<div class="panel-card" style="padding:10px; color:var(--text-dim); font-size:12px;">Preisverlauf sammelt noch Daten...</div>';
  }
  const prices = history.map(h => h[1]);
  const times = history.map(h => h[0]);
  let minP = Math.min(...prices), maxP = Math.max(...prices);
  const minT = times[0], maxT = times[times.length - 1];

  // SL-/TP1-/Trailing-Linien mit einrechnen, damit sie nicht aus dem sichtbaren Bereich fallen
  let slPrice = null, tp1Price = null, trailPrice = null;
  if (pos && pos.position && pos.size) {
    const slDist = pos.sl_usd / pos.size, tp1Dist = pos.tp1_usd / pos.size;
    slPrice = pos.position === 'long' ? pos.avg_entry_price - slDist : pos.avg_entry_price + slDist;
    if (!pos.tp1_done) tp1Price = pos.position === 'long' ? pos.avg_entry_price + tp1Dist : pos.avg_entry_price - tp1Dist;
    else if (pos.trail_price != null) trailPrice = pos.trail_price;
    [slPrice, tp1Price, trailPrice].forEach(v => { if (v != null) { minP = Math.min(minP, v); maxP = Math.max(maxP, v); } });
  }

  const w = 800, h = 130, pad = 10;
  const pRange = (maxP - minP) || (maxP * 0.001) || 1;
  const tRange = (maxT - minT) || 1;
  const xOf = t => pad + (t - minT) / tRange * (w - 2 * pad);
  const yOf = p => h - pad - (p - minP) / pRange * (h - 2 * pad);
  const points = history.map(([t, p]) => `${xOf(t).toFixed(1)},${yOf(p).toFixed(1)}`).join(' ');

  let levelLines = '';
  if (slPrice != null) levelLines += `<line x1="${pad}" y1="${yOf(slPrice).toFixed(1)}" x2="${w-pad}" y2="${yOf(slPrice).toFixed(1)}" stroke="#f0526b" stroke-width="1" stroke-dasharray="4,3"/><text x="${w-pad}" y="${(yOf(slPrice)-3).toFixed(1)}" fill="#f0526b" font-size="9" text-anchor="end">SL</text>`;
  if (tp1Price != null) levelLines += `<line x1="${pad}" y1="${yOf(tp1Price).toFixed(1)}" x2="${w-pad}" y2="${yOf(tp1Price).toFixed(1)}" stroke="#22c55e" stroke-width="1" stroke-dasharray="4,3"/><text x="${w-pad}" y="${(yOf(tp1Price)-3).toFixed(1)}" fill="#22c55e" font-size="9" text-anchor="end">TP1</text>`;
  if (trailPrice != null) levelLines += `<line x1="${pad}" y1="${yOf(trailPrice).toFixed(1)}" x2="${w-pad}" y2="${yOf(trailPrice).toFixed(1)}" stroke="#3b82f6" stroke-width="1" stroke-dasharray="4,3"/><text x="${w-pad}" y="${(yOf(trailPrice)-3).toFixed(1)}" fill="#3b82f6" font-size="9" text-anchor="end">Trail</text>`;

  const styles = {
    entry_long: { shape: 'triUp', color: '#22c55e', label: 'LONG' },
    entry_short: { shape: 'triDown', color: '#f0526b', label: 'SHORT' },
    dca_long: { shape: 'circle', color: '#86efac', r: 3, label: '+' },
    dca_short: { shape: 'circle', color: '#fca5a5', r: 3, label: '+' },
    exit_sl: { shape: 'x', color: '#f0526b', label: 'SL' },
    exit_tp1: { shape: 'circle', color: '#22c55e', r: 4, label: 'TP1' },
    exit_trail: { shape: 'circle', color: '#3b82f6', r: 4, label: 'Exit' },
  };
  const markerSvgs = (markers || []).filter(m => m.ts >= minT && m.ts <= maxT).map(m => {
    const x = xOf(m.ts), y = yOf(m.price);
    const st = styles[m.kind] || { shape: 'circle', color: '#888', r: 3, label: '' };
    let shape = '';
    if (st.shape === 'triUp') shape = `<polygon points="${x},${y-6} ${x-5},${y+4} ${x+5},${y+4}" fill="${st.color}"/>`;
    else if (st.shape === 'triDown') shape = `<polygon points="${x},${y+6} ${x-5},${y-4} ${x+5},${y-4}" fill="${st.color}"/>`;
    else if (st.shape === 'x') shape = `<line x1="${x-4}" y1="${y-4}" x2="${x+4}" y2="${y+4}" stroke="${st.color}" stroke-width="2"/><line x1="${x-4}" y1="${y+4}" x2="${x+4}" y2="${y-4}" stroke="${st.color}" stroke-width="2"/>`;
    else shape = `<circle cx="${x}" cy="${y}" r="${st.r||3}" fill="${st.color}"/>`;
    // Textlabel nur bei Ein-/Ausstieg (nicht bei Nachkauf-Kreisen), damit es nicht zu voll wird
    const label = (st.shape === 'triUp' || st.shape === 'triDown')
      ? `<text x="${x}" y="${st.shape==='triUp' ? y-9 : y+15}" fill="${st.color}" font-size="9" font-weight="700" text-anchor="middle">${st.label}</text>` : '';
    return shape + label;
  }).join('');

  return `<div class="panel-card" style="padding:8px 10px;">
    <div style="font-size:11px; color:var(--text-dim); margin-bottom:4px;">Preisverlauf (15 Min) · 🔺LONG · 🔻SHORT · ⭕Nachkauf · ✖️SL · 🟢TP1 · 🔵Trail-Exit</div>
    <svg viewBox="0 0 ${w} ${h}" style="width:100%; height:130px; display:block;">
      ${levelLines}
      <polyline points="${points}" fill="none" stroke="var(--accent)" stroke-width="1.5"/>
      ${markerSvgs}
    </svg>
  </div>`;
}

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

  // OBI-Momentum-Scalp Trend-Meter: grosse, prominente Live-Anzeige der aktuellen
  // Signal-Richtung - auch nutzbar wenn der Bot pausiert ist, zum manuellen Nachhandeln
  const trendMeterEl = document.getElementById('oms-trend-meter');
  const trendMeterDetailEl = document.getElementById('oms-trend-meter-detail');
  const gaugeWrap = document.getElementById('oms-gauge-wrap');
  const checklistWrap = document.getElementById('oms-checklist-wrap');
  const chartWrap = document.getElementById('oms-chart-wrap');

  if (data.config.entry_mode === 'oms_scalp') {
    trendMeterEl.style.display = '';
    trendMeterDetailEl.style.display = '';
    gaugeWrap.style.display = '';
    checklistWrap.style.display = '';
    chartWrap.style.display = '';

    const sig = data.oms_signal;
    if (sig === 'long') {
      trendMeterEl.style.background = 'rgba(34,197,94,0.18)';
      trendMeterEl.style.color = '#22c55e';
      trendMeterEl.innerText = '🟢 JETZT LONG';
    } else if (sig === 'short') {
      trendMeterEl.style.background = 'rgba(240,82,107,0.18)';
      trendMeterEl.style.color = '#f0526b';
      trendMeterEl.innerText = '🔴 JETZT SHORT';
    } else {
      trendMeterEl.style.background = 'rgba(124,138,168,0.12)';
      trendMeterEl.style.color = 'var(--text-dim)';
      trendMeterEl.innerText = '⚪ KEIN SIGNAL';
    }
    trendMeterDetailEl.innerText =
      `OBI schnell/mittel/langsam: ${data.oms_obi_fast ?? '-'} / ${data.oms_obi_medium ?? '-'} / ${data.oms_obi_slow ?? '-'}  |  ` +
      `CVD: ${data.oms_cvd_ratio ?? '-'}  |  Funding: ${data.oms_funding_rate != null ? (data.oms_funding_rate*100).toFixed(4)+'%' : '-'}`;

    gaugeWrap.innerHTML = renderOmsGauge(data.oms_obi_fast, data.oms_obi_medium, data.oms_obi_slow, data.config.oms_obi_threshold);
    checklistWrap.innerHTML = renderOmsChecklist(data);
    chartWrap.innerHTML = renderOmsChart(data.oms_price_history, data.oms_markers, {
      position: data.position, avg_entry_price: data.avg_entry_price, size: data.total_coin_size,
      sl_usd: data.config.oms_sl_usd, tp1_usd: data.config.oms_tp1_usd,
      tp1_done: data.oms_tp1_done, trail_price: data.oms_trail_price,
    });
  } else {
    trendMeterEl.style.display = 'none';
    trendMeterDetailEl.style.display = 'none';
    gaugeWrap.style.display = 'none';
    checklistWrap.style.display = 'none';
    chartWrap.style.display = 'none';
  }

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
    <div class="card"><div class="label">OMS TP1 erreicht?</div><div class="value ${data.oms_tp1_done?'green':''}">${data.config.entry_mode==='oms_scalp'?(data.oms_tp1_done?'Ja - Rest wird getrailt':'Nein'):'-'}</div></div>
    <div class="card"><div class="label">OMS Trailing-Referenz</div><div class="value">${data.oms_trail_price ?? '-'}</div></div>
    <div class="card"><div class="label">OMS Nachkauf-Stufe</div><div class="value">${data.config.entry_mode==='oms_scalp'?`${data.oms_dca_count ?? 0} / ${data.config.oms_dca_max_entries}`:'-'}</div></div>
    <div class="card"><div class="label">Spread % (Filter ${data.config.obi_spread_filter_enabled?'an':'aus'})</div><div class="value ${data.config.obi_spread_filter_enabled && data.obi_spread_pct!=null && data.obi_spread_pct>data.config.obi_max_spread_pct?'red':''}">${data.obi_spread_pct!=null?data.obi_spread_pct.toFixed(4):'-'}</div></div>
    <div class="card"><div class="label">Volatilität % (Filter ${data.config.obi_vol_filter_enabled?'an':'aus'})</div><div class="value ${data.config.obi_vol_filter_enabled && data.obi_recent_vol_pct!=null && (data.obi_recent_vol_pct<data.config.obi_vol_min_pct || data.obi_recent_vol_pct>data.config.obi_vol_max_pct)?'red':''}">${data.obi_recent_vol_pct!=null?data.obi_recent_vol_pct.toFixed(4):'-'}</div></div>
    <div class="card"><div class="label">Fib High / Low (${data.config.entry_mode==='fib_reversal'?'aktiv':'inaktiv'})</div><div class="value">${data.fib?.high ?? '-'} / ${data.fib?.low ?? '-'}</div></div>
    <div class="card"><div class="label">Fib Einstieg 1 / 2</div><div class="value">${data.fib?.entry1_price ?? '-'} / ${data.fib?.entry2_price ?? '-'}</div></div>
    <div class="card"><div class="label">Fib TP1 / TP2 / SL</div><div class="value">${data.fib?.tp1_price ?? '-'} / ${data.fib?.tp2_price ?? '-'} / ${data.fib?.sl_price ?? '-'}</div></div>
    <div class="card"><div class="label">Range-Profile Oszillator (${data.config.entry_mode==='range_profile'?'aktiv':'inaktiv'})</div><div class="value ${(data.rp_osc??0)>=0?'green':'red'}">${data.rp_osc ?? '-'}</div></div>
    <div class="card"><div class="label">Range-Profile Mitte / Kanal</div><div class="value">${data.rp_mid_price ?? '-'} (${data.rp_range_low ?? '-'} – ${data.rp_range_high ?? '-'})</div></div>
    <div class="card"><div class="label">Range-Profile TP / SL (fest, $)</div><div class="value">${data.config.rp_tp_usd ?? '-'} / ${data.config.rp_sl_usd ?? '-'}${data.rp_breakeven_triggered ? ' 🔒' : ''}</div></div>
    <div class="card"><div class="label">Range-Profile Kanalbreite (Ø)</div><div class="value">${data.rp_channel_width ?? '-'} (Ø ${data.rp_avg_width ?? '-'})</div></div>
    <div class="card"><div class="label">⚠ Squeeze (Ausbruch könnte bevorstehen)</div><div class="value ${data.rp_squeeze_active?'red':'green'}">${data.rp_squeeze_active ? 'AKTIV' : 'nein'}</div></div>
    <div class="card"><div class="label">SuperTrend Fusion (${data.config.entry_mode==='supertrend_fusion'?'aktiv':'inaktiv'})</div><div class="value ${data.stf_direction===-1?'green':data.stf_direction===1?'red':''}">${data.stf_direction===-1?'AUFWÄRTS':data.stf_direction===1?'ABWÄRTS':'-'} (Chop ${data.stf_chop_value!=null?data.stf_chop_value.toFixed(1):'-'})</div></div>
    <div class="card"><div class="label">Chandelier Exit (${data.config.entry_mode==='chandelier_exit'?'aktiv':'inaktiv'})</div><div class="value ${data.ce_direction===1?'green':data.ce_direction===-1?'red':''}">${data.ce_direction===1?'LONG-Signal':data.ce_direction===-1?'SHORT-Signal':'-'}</div></div>
    <div class="card"><div class="label">Chandelier SuperTrend-Filter (${data.config.ce_stf_filter_enabled?'an':'aus'})</div><div class="value ${data.ce_stf_bias==='long'?'green':data.ce_stf_bias==='short'?'red':''}">${data.ce_stf_bias ?? '-'} ${data.ce_pending_direction?`⏸️ wartet auf ${data.ce_pending_direction}`:''}</div></div>
    <div class="card"><div class="label">UT-Bot Stop-Linie (${data.config.entry_mode==='ut_bot'?'aktiv':'inaktiv'})</div><div class="value">${data.ut_stop_value!=null?data.ut_stop_value.toFixed(4):'-'}</div></div>
    <div class="card"><div class="label">HalfTrend (${data.config.entry_mode==='halftrend'?'aktiv':'inaktiv'})</div><div class="value ${data.ht_direction===1?'green':data.ht_direction===-1?'red':''}">${data.ht_direction===1?'LONG-Signal':data.ht_direction===-1?'SHORT-Signal':'-'}</div></div>
    <div class="card"><div class="label">HalfTrend SL</div><div class="value">${data.ht_sl_price!=null?data.ht_sl_price.toFixed(4):'-'}${data.ht_tp1_done?' (Break-Even)':''}</div></div>
    <div class="card"><div class="label">HalfTrend TP1 / TP2 / TP3</div><div class="value">${data.ht_tp1_price!=null?data.ht_tp1_price.toFixed(4):'-'}${data.ht_tp1_done?'✓':''} / ${data.ht_tp2_price!=null?data.ht_tp2_price.toFixed(4):'-'}${data.ht_tp2_done?'✓':''} / ${data.ht_tp3_price!=null?data.ht_tp3_price.toFixed(4):'-'}</div></div>
    <div class="card"><div class="label">WaveTrend wt1 / wt2 (${data.config.entry_mode==='wavetrend_cross'?'aktiv':'inaktiv'})</div><div class="value">${data.wtc_wt1!=null?data.wtc_wt1.toFixed(2):'-'} / ${data.wtc_wt2!=null?data.wtc_wt2.toFixed(2):'-'}</div></div>
    <div class="card"><div class="label">WaveTrend SuperTrend-Filter (${data.config.wtc_stf_filter_enabled?'an':'aus'})</div><div class="value ${data.wtc_stf_bias==='long'?'green':data.wtc_stf_bias==='short'?'red':''}">${data.wtc_stf_bias ?? '-'} ${data.wtc_pending_direction?`⏸️ wartet auf ${data.wtc_pending_direction}`:''}</div></div>
    <div class="card"><div class="label">Binance-1s-Puffer (Diagnose)</div><div class="value">${data.binance_1s_buffer_size ?? 0} Kerzen / ${Math.round((data.binance_1s_buffer_span_sec ?? 0)/60)} Min</div></div>
    <div class="card"><div class="label">Lighter-Tick-Fallback-Puffer (Diagnose)</div><div class="value">${data.local_1s_buffer_size ?? 0} Kerzen</div></div>
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
    document.getElementById('obi_spread_filter_enabled').value = String(data.config.obi_spread_filter_enabled);
    document.getElementById('obi_max_spread_pct').value = data.config.obi_max_spread_pct;
    document.getElementById('obi_vol_filter_enabled').value = String(data.config.obi_vol_filter_enabled);
    document.getElementById('obi_vol_window_seconds').value = data.config.obi_vol_window_seconds;
    document.getElementById('obi_vol_min_pct').value = data.config.obi_vol_min_pct;
    document.getElementById('obi_vol_max_pct').value = data.config.obi_vol_max_pct;
    document.getElementById('oms_levels').value = data.config.oms_levels;
    document.getElementById('oms_obi_threshold').value = data.config.oms_obi_threshold;
    document.getElementById('oms_window_fast_seconds').value = data.config.oms_window_fast_seconds;
    document.getElementById('oms_window_medium_seconds').value = data.config.oms_window_medium_seconds;
    document.getElementById('oms_window_slow_seconds').value = data.config.oms_window_slow_seconds;
    document.getElementById('oms_cvd_confirm_enabled').value = String(data.config.oms_cvd_confirm_enabled);
    document.getElementById('oms_cvd_window_seconds').value = data.config.oms_cvd_window_seconds;
    document.getElementById('oms_cvd_min_ratio').value = data.config.oms_cvd_min_ratio;
    document.getElementById('oms_funding_filter_enabled').value = String(data.config.oms_funding_filter_enabled);
    document.getElementById('oms_funding_max_abs').value = data.config.oms_funding_max_abs;
    document.getElementById('oms_cooldown_seconds').value = data.config.oms_cooldown_seconds;
    document.getElementById('oms_tp1_usd').value = data.config.oms_tp1_usd;
    document.getElementById('oms_tp1_close_pct').value = data.config.oms_tp1_close_pct;
    document.getElementById('oms_sl_usd').value = data.config.oms_sl_usd;
    document.getElementById('oms_trail_distance_usd').value = data.config.oms_trail_distance_usd;
    document.getElementById('oms_dca_enabled').value = String(data.config.oms_dca_enabled);
    document.getElementById('oms_dca_max_entries').value = data.config.oms_dca_max_entries;
    document.getElementById('oms_dca_size_fraction').value = data.config.oms_dca_size_fraction;
    document.getElementById('oms_dca_min_pullback_usd').value = data.config.oms_dca_min_pullback_usd;
    document.getElementById('fib_resolution').value = data.config.fib_resolution;
    document.getElementById('fib_lookback_candles').value = data.config.fib_lookback_candles;
    document.getElementById('fib_entry1_level').value = data.config.fib_entry1_level;
    document.getElementById('fib_entry2_level').value = data.config.fib_entry2_level;
    document.getElementById('fib_tp1_level').value = data.config.fib_tp1_level;
    document.getElementById('fib_tp1_close_pct').value = data.config.fib_tp1_close_pct;
    document.getElementById('fib_tp2_level').value = data.config.fib_tp2_level;
    document.getElementById('fib_sl_level').value = data.config.fib_sl_level;
    document.getElementById('fib_cooldown_seconds').value = data.config.fib_cooldown_seconds;
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
    document.getElementById('rp_require_squeeze').value = String(data.config.rp_require_squeeze);
    document.getElementById('zscore_lookback_period').value = data.config.zscore_lookback_period;
    document.getElementById('zscore_ema_smooth').value = data.config.zscore_ema_smooth;
    document.getElementById('zscore_threshold').value = data.config.zscore_threshold;
    document.getElementById('stf_resolution').value = data.config.stf_resolution;
    document.getElementById('stf_atr_period').value = data.config.stf_atr_period;
    document.getElementById('stf_factor').value = data.config.stf_factor;
    document.getElementById('stf_use_af_filter').value = String(data.config.stf_use_af_filter);
    document.getElementById('stf_af_period').value = data.config.stf_af_period;
    document.getElementById('stf_af_smooth').value = data.config.stf_af_smooth;
    document.getElementById('stf_use_chop_filter').value = String(data.config.stf_use_chop_filter);
    document.getElementById('stf_chop_length').value = data.config.stf_chop_length;
    document.getElementById('stf_chop_threshold').value = data.config.stf_chop_threshold;
    document.getElementById('stf_entry_trigger').value = data.config.stf_entry_trigger;
    document.getElementById('stf_exit_trigger').value = data.config.stf_exit_trigger;
    document.getElementById('stf_invert_direction').value = String(data.config.stf_invert_direction);
    document.getElementById('stf_use_ema_filter').value = String(data.config.stf_use_ema_filter);
    document.getElementById('stf_ema_length').value = data.config.stf_ema_length;
    document.getElementById('stf_tp_enabled').value = String(data.config.stf_tp_enabled);
    document.getElementById('stf_tp_usd').value = data.config.stf_tp_usd;
    document.getElementById('stf_sl_enabled').value = String(data.config.stf_sl_enabled);
    document.getElementById('stf_sl_usd').value = data.config.stf_sl_usd;
    document.getElementById('ce_resolution').value = data.config.ce_resolution;
    document.getElementById('ce_atr_period').value = data.config.ce_atr_period;
    document.getElementById('ce_atr_mult').value = data.config.ce_atr_mult;
    document.getElementById('ce_use_close').value = String(data.config.ce_use_close);
    document.getElementById('ce_invert_direction').value = String(data.config.ce_invert_direction);
    document.getElementById('ce_entry_trigger').value = data.config.ce_entry_trigger;
    document.getElementById('ce_exit_trigger').value = data.config.ce_exit_trigger;
    document.getElementById('ce_tp_enabled').value = String(data.config.ce_tp_enabled);
    document.getElementById('ce_tp_usd').value = data.config.ce_tp_usd;
    document.getElementById('ce_sl_enabled').value = String(data.config.ce_sl_enabled);
    document.getElementById('ce_sl_usd').value = data.config.ce_sl_usd;
    document.getElementById('ce_sl_cooldown_seconds').value = data.config.ce_sl_cooldown_seconds;
    document.getElementById('ce_stf_filter_enabled').value = String(data.config.ce_stf_filter_enabled);
    document.getElementById('ce_stf_resolution').value = data.config.ce_stf_resolution;
    document.getElementById('ut_resolution').value = data.config.ut_resolution;
    document.getElementById('ut_atr_period').value = data.config.ut_atr_period;
    document.getElementById('ut_key_value').value = data.config.ut_key_value;
    document.getElementById('ut_entry_trigger').value = data.config.ut_entry_trigger;
    document.getElementById('ut_exit_trigger').value = data.config.ut_exit_trigger;
    document.getElementById('ut_invert_direction').value = String(data.config.ut_invert_direction);
    document.getElementById('ut_tp_enabled').value = String(data.config.ut_tp_enabled);
    document.getElementById('ut_tp_usd').value = data.config.ut_tp_usd;
    document.getElementById('ut_sl_enabled').value = String(data.config.ut_sl_enabled);
    document.getElementById('ut_sl_usd').value = data.config.ut_sl_usd;
    document.getElementById('ut_sl_cooldown_seconds').value = data.config.ut_sl_cooldown_seconds;
    document.getElementById('ht_resolution').value = data.config.ht_resolution;
    document.getElementById('ht_amplitude').value = data.config.ht_amplitude;
    document.getElementById('ht_channel_deviation').value = data.config.ht_channel_deviation;
    document.getElementById('ht_base_risk_mult').value = data.config.ht_base_risk_mult;
    document.getElementById('ht_entry_trigger').value = data.config.ht_entry_trigger;
    document.getElementById('ht_exit_trigger').value = data.config.ht_exit_trigger;
    document.getElementById('ht_invert_direction').value = String(data.config.ht_invert_direction);
    document.getElementById('ht_tp_enabled').value = String(data.config.ht_tp_enabled);
    document.getElementById('ht_tp1_close_pct').value = data.config.ht_tp1_close_pct;
    document.getElementById('ht_tp2_close_pct').value = data.config.ht_tp2_close_pct;
    document.getElementById('ht_sl_enabled').value = String(data.config.ht_sl_enabled);
    document.getElementById('ht_sl_cooldown_seconds').value = data.config.ht_sl_cooldown_seconds;
    document.getElementById('wtc_resolution').value = data.config.wtc_resolution;
    document.getElementById('wtc_channel_length').value = data.config.wtc_channel_length;
    document.getElementById('wtc_average_length').value = data.config.wtc_average_length;
    document.getElementById('wtc_ma_length').value = data.config.wtc_ma_length;
    document.getElementById('wtc_require_obos').value = String(data.config.wtc_require_obos);
    document.getElementById('wtc_ob_level').value = data.config.wtc_ob_level;
    document.getElementById('wtc_os_level').value = data.config.wtc_os_level;
    document.getElementById('wtc_entry_trigger').value = data.config.wtc_entry_trigger;
    document.getElementById('wtc_exit_trigger').value = data.config.wtc_exit_trigger;
    document.getElementById('wtc_invert_direction').value = String(data.config.wtc_invert_direction);
    document.getElementById('wtc_tp_enabled').value = String(data.config.wtc_tp_enabled);
    document.getElementById('wtc_tp_usd').value = data.config.wtc_tp_usd;
    document.getElementById('wtc_sl_enabled').value = String(data.config.wtc_sl_enabled);
    document.getElementById('wtc_sl_usd').value = data.config.wtc_sl_usd;
    document.getElementById('wtc_sl_cooldown_seconds').value = data.config.wtc_sl_cooldown_seconds;
    document.getElementById('wtc_direction_mode').value = data.config.wtc_direction_mode;
    document.getElementById('wtc_dca_enabled').value = String(data.config.wtc_dca_enabled);
    document.getElementById('wtc_dca_max_entries').value = data.config.wtc_dca_max_entries;
    document.getElementById('wtc_dca_cooldown_seconds').value = data.config.wtc_dca_cooldown_seconds;
    document.getElementById('wtc_stf_filter_enabled').value = String(data.config.wtc_stf_filter_enabled);
    document.getElementById('wtc_stf_resolution').value = data.config.wtc_stf_resolution;
    document.getElementById('sg_signal_source').value = data.config.sg_signal_source;
    document.getElementById('sg_resolution').value = data.config.sg_resolution;
    document.getElementById('sg_entry_trigger').value = data.config.sg_entry_trigger;
    document.getElementById('sg_invert_direction').value = String(data.config.sg_invert_direction);
    document.getElementById('sg_tp_mode').value = data.config.sg_tp_mode;
    document.getElementById('sg_tp_step_pct').value = data.config.sg_tp_step_pct;
    document.getElementById('sg_tp_step_usd').value = data.config.sg_tp_step_usd;
    document.getElementById('sg_max_nachkauf').value = data.config.sg_max_nachkauf;
    document.getElementById('sg_dca_cooldown_seconds').value = data.config.sg_dca_cooldown_seconds;
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

  try {
    const obiSection = document.getElementById('obi-chart-section');
    const isObiLike = data.config.entry_mode === 'obi_scalp' || data.config.entry_mode === 'oms_scalp';
    const rawHist = data.config.entry_mode === 'oms_scalp' ? (data.oms_obi_history || []) : (data.obi_history || []);
    const threshold = data.config.entry_mode === 'oms_scalp' ? data.config.oms_obi_threshold : data.config.obi_threshold;
    if (isObiLike && rawHist.length > 0) {
      obiSection.style.display = 'block';
      const obiHist = rawHist;
      const obiLabels = obiHist.map(p => new Date(p.ts).toLocaleTimeString());
      const obiDatasets = [
        { label:'Schnell', data: obiHist.map(p=>p.fast), borderColor:'#f87171', pointRadius:0, borderWidth:2 },
        { label:'Mittel', data: obiHist.map(p=>p.medium), borderColor:'#fbbf24', pointRadius:0, borderWidth:2 },
        { label:'Langsam', data: obiHist.map(p=>p.slow), borderColor:'#60a5fa', pointRadius:0, borderWidth:2 },
        { label:'Schwelle +', data: Array(obiHist.length).fill(threshold), borderColor:'#4ade80', borderDash:[4,4], pointRadius:0, borderWidth:1 },
        { label:'Schwelle -', data: Array(obiHist.length).fill(-threshold), borderColor:'#4ade80', borderDash:[4,4], pointRadius:0, borderWidth:1 },
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
  } catch (e) {
    console.error('OBI-Chart-Fehler:', e);
  }

  try {
    const pocketSection = document.getElementById('pocket-trading-section');
    if (data.config.entry_mode === 'obi_scalp' || data.config.entry_mode === 'oms_scalp') {
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
  } catch (e) {
    console.error('Pocket-Trading-Fehler:', e);
  }

  try {
    const trades = (data.trade_log || []).slice(-15).reverse();
    document.getElementById('trades-debug').innerText = '';
    const fmtTime = (iso) => iso ? new Date(iso).toLocaleString('de-DE', {day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit'}) : '-';
    document.querySelector('#trades-table tbody').innerHTML = trades.map(t => `
      <tr><td>${fmtTime(t.opened_at)}</td><td>${fmtTime(t.closed_at)}</td><td>${t.side}</td><td>${t.avg_entry}</td><td>${t.exit}</td><td>${t.entries}</td><td>${t.reason ?? '-'}</td>
      <td class="${t.pnl_usd>=0?'green':'red'}">${t.pnl_usd}</td></tr>
    `).join('') || '<tr><td colspan="8" style="color:var(--text-dim);">Noch keine abgeschlossenen Trades</td></tr>';
  } catch (e) {
    console.error('Trade-Tabelle-Fehler:', e);
    const dbg = document.getElementById('trades-debug');
    if (dbg) dbg.innerText = `(Fehler: ${e})`;
  }
}

function buildConfigPayload() {
  return {
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
    obi_spread_filter_enabled: document.getElementById('obi_spread_filter_enabled').value === 'true',
    obi_max_spread_pct: parseFloat(document.getElementById('obi_max_spread_pct').value),
    obi_vol_filter_enabled: document.getElementById('obi_vol_filter_enabled').value === 'true',
    obi_vol_window_seconds: parseFloat(document.getElementById('obi_vol_window_seconds').value),
    obi_vol_min_pct: parseFloat(document.getElementById('obi_vol_min_pct').value),
    obi_vol_max_pct: parseFloat(document.getElementById('obi_vol_max_pct').value),
    oms_levels: parseInt(document.getElementById('oms_levels').value),
    oms_obi_threshold: parseFloat(document.getElementById('oms_obi_threshold').value),
    oms_window_fast_seconds: parseFloat(document.getElementById('oms_window_fast_seconds').value),
    oms_window_medium_seconds: parseFloat(document.getElementById('oms_window_medium_seconds').value),
    oms_window_slow_seconds: parseFloat(document.getElementById('oms_window_slow_seconds').value),
    oms_cvd_confirm_enabled: document.getElementById('oms_cvd_confirm_enabled').value === 'true',
    oms_cvd_window_seconds: parseFloat(document.getElementById('oms_cvd_window_seconds').value),
    oms_cvd_min_ratio: parseFloat(document.getElementById('oms_cvd_min_ratio').value),
    oms_funding_filter_enabled: document.getElementById('oms_funding_filter_enabled').value === 'true',
    oms_funding_max_abs: parseFloat(document.getElementById('oms_funding_max_abs').value),
    oms_cooldown_seconds: parseFloat(document.getElementById('oms_cooldown_seconds').value),
    oms_tp1_usd: parseFloat(document.getElementById('oms_tp1_usd').value),
    oms_tp1_close_pct: parseFloat(document.getElementById('oms_tp1_close_pct').value),
    oms_sl_usd: parseFloat(document.getElementById('oms_sl_usd').value),
    oms_trail_distance_usd: parseFloat(document.getElementById('oms_trail_distance_usd').value),
    oms_dca_enabled: document.getElementById('oms_dca_enabled').value === 'true',
    oms_dca_max_entries: parseInt(document.getElementById('oms_dca_max_entries').value),
    oms_dca_size_fraction: parseFloat(document.getElementById('oms_dca_size_fraction').value),
    oms_dca_min_pullback_usd: parseFloat(document.getElementById('oms_dca_min_pullback_usd').value),
    fib_resolution: document.getElementById('fib_resolution').value,
    fib_lookback_candles: parseInt(document.getElementById('fib_lookback_candles').value),
    fib_entry1_level: parseFloat(document.getElementById('fib_entry1_level').value),
    fib_entry2_level: parseFloat(document.getElementById('fib_entry2_level').value),
    fib_tp1_level: parseFloat(document.getElementById('fib_tp1_level').value),
    fib_tp1_close_pct: parseFloat(document.getElementById('fib_tp1_close_pct').value),
    fib_tp2_level: parseFloat(document.getElementById('fib_tp2_level').value),
    fib_sl_level: parseFloat(document.getElementById('fib_sl_level').value),
    fib_cooldown_seconds: parseFloat(document.getElementById('fib_cooldown_seconds').value),
    zscore_lookback_period: parseInt(document.getElementById('zscore_lookback_period').value),
    zscore_ema_smooth: parseInt(document.getElementById('zscore_ema_smooth').value),
    zscore_threshold: parseFloat(document.getElementById('zscore_threshold').value),
    stf_resolution: document.getElementById('stf_resolution').value,
    stf_atr_period: parseInt(document.getElementById('stf_atr_period').value),
    stf_factor: parseFloat(document.getElementById('stf_factor').value),
    stf_use_af_filter: document.getElementById('stf_use_af_filter').value === 'true',
    stf_af_period: parseInt(document.getElementById('stf_af_period').value),
    stf_af_smooth: parseInt(document.getElementById('stf_af_smooth').value),
    stf_use_chop_filter: document.getElementById('stf_use_chop_filter').value === 'true',
    stf_chop_length: parseInt(document.getElementById('stf_chop_length').value),
    stf_chop_threshold: parseInt(document.getElementById('stf_chop_threshold').value),
    stf_entry_trigger: document.getElementById('stf_entry_trigger').value,
    stf_exit_trigger: document.getElementById('stf_exit_trigger').value,
    stf_invert_direction: document.getElementById('stf_invert_direction').value === 'true',
    stf_use_ema_filter: document.getElementById('stf_use_ema_filter').value === 'true',
    stf_ema_length: parseInt(document.getElementById('stf_ema_length').value),
    stf_tp_enabled: document.getElementById('stf_tp_enabled').value === 'true',
    stf_tp_usd: parseFloat(document.getElementById('stf_tp_usd').value),
    stf_sl_enabled: document.getElementById('stf_sl_enabled').value === 'true',
    stf_sl_usd: parseFloat(document.getElementById('stf_sl_usd').value),
    ce_resolution: document.getElementById('ce_resolution').value,
    ce_atr_period: parseInt(document.getElementById('ce_atr_period').value),
    ce_atr_mult: parseFloat(document.getElementById('ce_atr_mult').value),
    ce_use_close: document.getElementById('ce_use_close').value === 'true',
    ce_invert_direction: document.getElementById('ce_invert_direction').value === 'true',
    ce_entry_trigger: document.getElementById('ce_entry_trigger').value,
    ce_exit_trigger: document.getElementById('ce_exit_trigger').value,
    ce_tp_enabled: document.getElementById('ce_tp_enabled').value === 'true',
    ce_tp_usd: parseFloat(document.getElementById('ce_tp_usd').value),
    ce_sl_enabled: document.getElementById('ce_sl_enabled').value === 'true',
    ce_sl_usd: parseFloat(document.getElementById('ce_sl_usd').value),
    ce_sl_cooldown_seconds: parseFloat(document.getElementById('ce_sl_cooldown_seconds').value),
    ce_stf_filter_enabled: document.getElementById('ce_stf_filter_enabled').value === 'true',
    ce_stf_resolution: document.getElementById('ce_stf_resolution').value,
    ut_resolution: document.getElementById('ut_resolution').value,
    ut_atr_period: parseInt(document.getElementById('ut_atr_period').value),
    ut_key_value: parseFloat(document.getElementById('ut_key_value').value),
    ut_entry_trigger: document.getElementById('ut_entry_trigger').value,
    ut_exit_trigger: document.getElementById('ut_exit_trigger').value,
    ut_invert_direction: document.getElementById('ut_invert_direction').value === 'true',
    ut_tp_enabled: document.getElementById('ut_tp_enabled').value === 'true',
    ut_tp_usd: parseFloat(document.getElementById('ut_tp_usd').value),
    ut_sl_enabled: document.getElementById('ut_sl_enabled').value === 'true',
    ut_sl_usd: parseFloat(document.getElementById('ut_sl_usd').value),
    ut_sl_cooldown_seconds: parseFloat(document.getElementById('ut_sl_cooldown_seconds').value),
    ht_resolution: document.getElementById('ht_resolution').value,
    ht_amplitude: parseInt(document.getElementById('ht_amplitude').value),
    ht_channel_deviation: parseFloat(document.getElementById('ht_channel_deviation').value),
    ht_base_risk_mult: parseFloat(document.getElementById('ht_base_risk_mult').value),
    ht_entry_trigger: document.getElementById('ht_entry_trigger').value,
    ht_exit_trigger: document.getElementById('ht_exit_trigger').value,
    ht_invert_direction: document.getElementById('ht_invert_direction').value === 'true',
    ht_tp_enabled: document.getElementById('ht_tp_enabled').value === 'true',
    ht_tp1_close_pct: parseFloat(document.getElementById('ht_tp1_close_pct').value),
    ht_tp2_close_pct: parseFloat(document.getElementById('ht_tp2_close_pct').value),
    ht_sl_enabled: document.getElementById('ht_sl_enabled').value === 'true',
    ht_sl_cooldown_seconds: parseFloat(document.getElementById('ht_sl_cooldown_seconds').value),
    wtc_resolution: document.getElementById('wtc_resolution').value,
    wtc_channel_length: parseInt(document.getElementById('wtc_channel_length').value),
    wtc_average_length: parseInt(document.getElementById('wtc_average_length').value),
    wtc_ma_length: parseInt(document.getElementById('wtc_ma_length').value),
    wtc_require_obos: document.getElementById('wtc_require_obos').value === 'true',
    wtc_ob_level: parseInt(document.getElementById('wtc_ob_level').value),
    wtc_os_level: parseInt(document.getElementById('wtc_os_level').value),
    wtc_entry_trigger: document.getElementById('wtc_entry_trigger').value,
    wtc_exit_trigger: document.getElementById('wtc_exit_trigger').value,
    wtc_invert_direction: document.getElementById('wtc_invert_direction').value === 'true',
    wtc_tp_enabled: document.getElementById('wtc_tp_enabled').value === 'true',
    wtc_tp_usd: parseFloat(document.getElementById('wtc_tp_usd').value),
    wtc_sl_enabled: document.getElementById('wtc_sl_enabled').value === 'true',
    wtc_sl_usd: parseFloat(document.getElementById('wtc_sl_usd').value),
    wtc_sl_cooldown_seconds: parseFloat(document.getElementById('wtc_sl_cooldown_seconds').value),
    wtc_direction_mode: document.getElementById('wtc_direction_mode').value,
    wtc_dca_enabled: document.getElementById('wtc_dca_enabled').value === 'true',
    wtc_dca_max_entries: parseInt(document.getElementById('wtc_dca_max_entries').value),
    wtc_dca_cooldown_seconds: parseFloat(document.getElementById('wtc_dca_cooldown_seconds').value),
    wtc_stf_filter_enabled: document.getElementById('wtc_stf_filter_enabled').value === 'true',
    wtc_stf_resolution: document.getElementById('wtc_stf_resolution').value,
    sg_signal_source: document.getElementById('sg_signal_source').value,
    sg_resolution: document.getElementById('sg_resolution').value,
    sg_entry_trigger: document.getElementById('sg_entry_trigger').value,
    sg_invert_direction: document.getElementById('sg_invert_direction').value === 'true',
    sg_tp_mode: document.getElementById('sg_tp_mode').value,
    sg_tp_step_pct: parseFloat(document.getElementById('sg_tp_step_pct').value),
    sg_tp_step_usd: parseFloat(document.getElementById('sg_tp_step_usd').value),
    sg_max_nachkauf: parseInt(document.getElementById('sg_max_nachkauf').value),
    sg_dca_cooldown_seconds: parseFloat(document.getElementById('sg_dca_cooldown_seconds').value),
    grid_mode: document.getElementById('grid_mode').value,
    grid_step_pct: parseFloat(document.getElementById('grid_step_pct').value),
    tp_step_pct: parseFloat(document.getElementById('tp_step_pct').value),
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
    rp_require_squeeze: document.getElementById('rp_require_squeeze').value === 'true',
    grid_step_usd: parseFloat(document.getElementById('grid_step_usd').value),
    tp_step_usd: parseFloat(document.getElementById('tp_step_usd').value),
    max_nachkauf: parseInt(document.getElementById('max_nachkauf').value),
    dry_run: document.getElementById('dry_run').value === 'true',
    auto_reverse: document.getElementById('auto_reverse').value === 'true',
  };
}

document.getElementById('config-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const payload = buildConfigPayload();
  await fetch(`/api/config?symbol=${currentSymbol}`, { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(payload) });
  window.formTouched = false;
  showToast(`Gespeichert für ${currentSymbol}!`);
});

function showToast(msg) {
  let el = document.getElementById('save-toast');
  if (!el) {
    el = document.createElement('div');
    el.id = 'save-toast';
    el.style.cssText = 'position:fixed;bottom:20px;right:20px;background:#1e293b;color:#fff;padding:10px 16px;border-radius:6px;box-shadow:0 2px 8px rgba(0,0,0,.3);z-index:9999;font-size:14px;transition:opacity .3s;';
    document.body.appendChild(el);
  }
  el.textContent = msg;
  el.style.opacity = '1';
  clearTimeout(el._hideTimer);
  el._hideTimer = setTimeout(() => { el.style.opacity = '0'; }, 1500);
}

['margin','leverage','entry_mode','obi_threshold','obi_mode','obi_long_threshold','obi_short_threshold','obi_reversal_min_bounce','obi_instant_reset_ratio','obi_window_fast_seconds','obi_window_medium_seconds','obi_window_slow_seconds','obi_levels','obi_depth_weighting_enabled','obi_use_median','obi_min_liquidity','obi_breakeven_enabled','obi_breakeven_trigger_ratio','obi_breakeven_lock_usd','obi_breakeven_lock_pct','obi_tp_sl_mode','obi_tp_pct','obi_sl_pct','obi_tp_usd','obi_sl_usd','obi_cooldown_seconds','obi_trend_filter','obi_trend_ema_length','obi_spread_filter_enabled','obi_max_spread_pct','obi_vol_filter_enabled','obi_vol_window_seconds','obi_vol_min_pct','obi_vol_max_pct','oms_levels','oms_obi_threshold','oms_window_fast_seconds','oms_window_medium_seconds','oms_window_slow_seconds','oms_cvd_window_seconds','oms_cvd_min_ratio','oms_funding_max_abs','oms_cooldown_seconds','oms_tp1_usd','oms_tp1_close_pct','oms_sl_usd','oms_trail_distance_usd','oms_dca_max_entries','oms_dca_size_fraction','oms_dca_min_pullback_usd','fib_resolution','fib_lookback_candles','fib_entry1_level','fib_entry2_level','fib_tp1_level','fib_tp1_close_pct','fib_tp2_level','fib_sl_level','fib_cooldown_seconds','rp_mode','rp_resolution','rp_lookback','rp_ob_os_level','rp_tp_usd','rp_sl_usd','rp_breakeven_enabled','rp_breakeven_trigger_usd','rp_breakeven_lock_usd','rp_squeeze_lookback','rp_squeeze_threshold_pct','rp_require_squeeze','zscore_lookback_period','zscore_ema_smooth','zscore_threshold','stf_atr_period','stf_factor','stf_af_period','stf_af_smooth','stf_chop_length','stf_chop_threshold','stf_ema_length','stf_tp_usd','stf_sl_usd','ce_atr_period','ce_atr_mult','ce_invert_direction','ce_tp_usd','ce_sl_usd','ce_sl_cooldown_seconds','ut_atr_period','ut_key_value','ut_invert_direction','ut_tp_usd','ut_sl_usd','ut_sl_cooldown_seconds','ht_amplitude','ht_channel_deviation','ht_base_risk_mult','ht_invert_direction','ht_tp1_close_pct','ht_tp2_close_pct','ht_sl_cooldown_seconds','wtc_channel_length','wtc_average_length','wtc_ma_length','wtc_require_obos','wtc_ob_level','wtc_os_level','wtc_invert_direction','wtc_tp_usd','wtc_sl_usd','wtc_sl_cooldown_seconds','wtc_direction_mode','wtc_dca_enabled','wtc_dca_max_entries','wtc_dca_cooldown_seconds','wtc_stf_filter_enabled','wtc_stf_resolution','sg_tp_step_pct','sg_tp_step_usd','sg_max_nachkauf','sg_dca_cooldown_seconds','sg_invert_direction','grid_mode','grid_step_pct','tp_step_pct','grid_step_usd','tp_step_usd','max_nachkauf','dry_run','auto_reverse'].forEach(id => {
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
        "total_coin_size": st["total_coin_size"],
        "entry_count": st["entry_count"], "liquidation_price": estimate_liquidation_price(symbol),
        "unrealized_pnl_usd": calc_unrealized_pnl(symbol),
        "grid_levels": calc_grid_levels(symbol),
        "obi_current": st.get("obi_current"), "obi_fast": st.get("obi_fast"),
        "obi_medium": st.get("obi_medium"), "obi_slow": st.get("obi_slow"),
        "oms_signal": st.get("oms_signal"), "oms_obi_fast": st.get("oms_obi_fast"),
        "oms_obi_medium": st.get("oms_obi_medium"), "oms_obi_slow": st.get("oms_obi_slow"),
        "oms_obi_direction": st.get("oms_obi_direction"), "oms_cvd_ok": st.get("oms_cvd_ok"),
        "oms_funding_ok": st.get("oms_funding_ok"),
        "oms_cvd_ratio": st.get("oms_cvd_ratio"), "oms_funding_rate": st.get("oms_funding_rate"),
        "oms_tp1_done": st.get("oms_tp1_done"), "oms_trail_price": st.get("oms_trail_price"),
        "oms_dca_count": st.get("oms_dca_count"),
        "oms_price_history": [[round(ts, 1), price] for ts, price in st.get("oms_price_history", [])[-200:]],
        "oms_markers": st.get("oms_markers", [])[-30:],
        "oms_obi_history": st.get("oms_obi_history", [])[-300:],
        "obi_history": st.get("obi_history", [])[-300:],
        "obi_spread_pct": st.get("obi_spread_pct"), "obi_recent_vol_pct": st.get("obi_recent_vol_pct"),
        "fib": st.get("fib"),
        "rp_osc": st.get("rp_osc"), "rp_mid_price": st.get("rp_mid_price"),
        "rp_range_high": st.get("rp_range_high"), "rp_range_low": st.get("rp_range_low"),
        "rp_breakeven_triggered": st.get("rp_breakeven_triggered"),
        "rp_channel_width": st.get("rp_channel_width"), "rp_avg_width": st.get("rp_avg_width"),
        "rp_squeeze_active": st.get("rp_squeeze_active"),
        "stf_direction": st.get("stf_direction"), "stf_chop_value": st.get("stf_chop_value"),
        "ce_direction": st.get("ce_direction"), "ce_stf_bias": st.get("ce_stf_bias"),
        "ce_pending_direction": st.get("ce_pending_direction"),
        "ut_stop_value": st.get("ut_stop_value"),
        "ht_direction": st.get("ht_direction"), "ht_sl_price": st.get("ht_sl_price"),
        "ht_tp1_price": st.get("ht_tp1_price"), "ht_tp2_price": st.get("ht_tp2_price"), "ht_tp3_price": st.get("ht_tp3_price"),
        "ht_tp1_done": st.get("ht_tp1_done"), "ht_tp2_done": st.get("ht_tp2_done"),
        "wtc_wt1": st.get("wtc_wt1"), "wtc_wt2": st.get("wtc_wt2"),
        "wtc_stf_bias": st.get("wtc_stf_bias"), "wtc_pending_direction": st.get("wtc_pending_direction"),
        "binance_1s_buffer_size": len(st.get("binance_1s_buffer", [])),
        "binance_1s_buffer_span_sec": (
            (st["binance_1s_buffer"][-1]["ts"] - st["binance_1s_buffer"][0]["ts"]) // 1000
            if len(st.get("binance_1s_buffer", [])) > 1 else 0
        ),
        "local_1s_buffer_size": len(st.get("local_1s_buffer", [])),
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
                "obi_spread_filter_enabled", "obi_max_spread_pct",
                "obi_vol_filter_enabled", "obi_vol_window_seconds", "obi_vol_min_pct", "obi_vol_max_pct",
                "oms_levels", "oms_obi_threshold", "oms_window_fast_seconds", "oms_window_medium_seconds",
                "oms_window_slow_seconds", "oms_cvd_confirm_enabled", "oms_cvd_window_seconds", "oms_cvd_min_ratio",
                "oms_funding_filter_enabled", "oms_funding_max_abs", "oms_cooldown_seconds",
                "oms_tp1_usd", "oms_tp1_close_pct", "oms_sl_usd", "oms_trail_distance_usd",
                "oms_dca_enabled", "oms_dca_max_entries", "oms_dca_size_fraction", "oms_dca_min_pullback_usd",
                "fib_resolution", "fib_lookback_candles", "fib_entry1_level", "fib_entry2_level",
                "fib_tp1_level", "fib_tp1_close_pct", "fib_tp2_level", "fib_sl_level", "fib_cooldown_seconds",
                "rp_mode", "rp_resolution", "rp_lookback", "rp_ob_os_level", "rp_tp_usd", "rp_sl_usd",
                "rp_breakeven_enabled", "rp_breakeven_trigger_usd", "rp_breakeven_lock_usd",
                "rp_squeeze_lookback", "rp_squeeze_threshold_pct", "rp_require_squeeze",
                "stf_resolution", "stf_atr_period", "stf_factor",
                "stf_use_af_filter", "stf_af_period", "stf_af_smooth",
                "stf_use_chop_filter", "stf_chop_length", "stf_chop_threshold",
                "stf_entry_trigger", "stf_exit_trigger", "stf_invert_direction",
                "stf_use_ema_filter", "stf_ema_length",
                "stf_tp_enabled", "stf_tp_usd", "stf_sl_enabled", "stf_sl_usd",
                "ce_resolution", "ce_atr_period", "ce_atr_mult", "ce_use_close", "ce_invert_direction",
                "ce_entry_trigger", "ce_exit_trigger", "ce_tp_enabled", "ce_tp_usd",
                "ce_sl_enabled", "ce_sl_usd", "ce_sl_cooldown_seconds",
                "ce_stf_filter_enabled", "ce_stf_resolution",
                "ut_resolution", "ut_atr_period", "ut_key_value", "ut_entry_trigger", "ut_exit_trigger",
                "ut_invert_direction", "ut_tp_enabled", "ut_tp_usd", "ut_sl_enabled", "ut_sl_usd", "ut_sl_cooldown_seconds",
                "ht_resolution", "ht_amplitude", "ht_channel_deviation", "ht_base_risk_mult",
                "ht_entry_trigger", "ht_exit_trigger", "ht_invert_direction",
                "ht_tp_enabled", "ht_tp1_close_pct", "ht_tp2_close_pct", "ht_sl_enabled", "ht_sl_cooldown_seconds",
                "wtc_resolution", "wtc_channel_length", "wtc_average_length", "wtc_ma_length",
                "wtc_require_obos", "wtc_ob_level", "wtc_os_level", "wtc_entry_trigger", "wtc_exit_trigger",
                "wtc_invert_direction", "wtc_tp_enabled", "wtc_tp_usd", "wtc_sl_enabled", "wtc_sl_usd", "wtc_sl_cooldown_seconds",
                "wtc_direction_mode", "wtc_dca_enabled", "wtc_dca_max_entries", "wtc_dca_cooldown_seconds",
                "wtc_stf_filter_enabled", "wtc_stf_resolution",
                "sg_signal_source", "sg_resolution", "sg_entry_trigger", "sg_invert_direction",
                "sg_tp_mode", "sg_tp_step_pct", "sg_tp_step_usd", "sg_max_nachkauf", "sg_dca_cooldown_seconds"]:
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
    overrides = body.get("config")
    if isinstance(overrides, dict):
        # Nur bekannte Config-Felder uebernehmen (das Formular schickt ohnehin nur solche) -
        # so testet der Backtest immer das, was gerade im Formular steht, auch wenn noch
        # nicht auf "Speichern" geklickt wurde.
        cfg.update({k: v for k, v in overrides.items() if k in cfg})
    entry_mode = cfg["entry_mode"]
    result = await run_backtest(symbol, entry_mode, cfg, days)
    return web.json_response(result)


async def handle_ce_sweep(request):
    """'Monte-Carlo'-Parametersweep fuer Chandelier Exit: testet einen Bereich von ATR-Periode
    und ATR-Multiplikator gegeneinander und gibt die besten Kombinationen zurueck."""
    from strategies import run_ce_param_sweep
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    body = await request.json()
    try:
        days = max(1, min(365, int(body.get("days", 30))))
        atr_period_min = max(1, int(body.get("atr_period_min", 1)))
        atr_period_max = max(atr_period_min, int(body.get("atr_period_max", 10)))
        atr_period_step = max(1, int(body.get("atr_period_step", 1)))
        atr_mult_min = max(0.01, float(body.get("atr_mult_min", 0.5)))
        atr_mult_max = max(atr_mult_min, float(body.get("atr_mult_max", 3.0)))
        atr_mult_step = max(0.01, float(body.get("atr_mult_step", 0.1)))
    except (TypeError, ValueError):
        return web.json_response({"error": "Ungültige Zahlenwerte im Sweep-Bereich."}, status=400)
    stf_filter_enabled = bool(body.get("stf_filter_enabled", False))

    cfg = dict(BOTS[symbol]["config"])
    overrides = body.get("config")
    if isinstance(overrides, dict):
        cfg.update({k: v for k, v in overrides.items() if k in cfg})

    result = await run_ce_param_sweep(symbol, cfg, days, atr_period_min, atr_period_max, atr_period_step,
                                       atr_mult_min, atr_mult_max, atr_mult_step, stf_filter_enabled)
    return web.json_response(result)


async def handle_ut_sweep(request):
    """'Monte-Carlo'-Parametersweep fuer UT-Bot: testet einen Bereich von ATR-Periode und
    Key-Value-Multiplikator gegeneinander und gibt die besten/schlechtesten Kombinationen
    zurueck."""
    from strategies import run_ut_param_sweep
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    body = await request.json()
    try:
        days = max(1, min(365, int(body.get("days", 30))))
        atr_period_min = max(1, int(body.get("atr_period_min", 1)))
        atr_period_max = max(atr_period_min, int(body.get("atr_period_max", 10)))
        atr_period_step = max(1, int(body.get("atr_period_step", 1)))
        key_value_min = max(0.01, float(body.get("key_value_min", 0.5)))
        key_value_max = max(key_value_min, float(body.get("key_value_max", 3.0)))
        key_value_step = max(0.01, float(body.get("key_value_step", 0.1)))
    except (TypeError, ValueError):
        return web.json_response({"error": "Ungültige Zahlenwerte im Sweep-Bereich."}, status=400)

    cfg = dict(BOTS[symbol]["config"])
    overrides = body.get("config")
    if isinstance(overrides, dict):
        cfg.update({k: v for k, v in overrides.items() if k in cfg})

    result = await run_ut_param_sweep(symbol, cfg, days, atr_period_min, atr_period_max, atr_period_step,
                                       key_value_min, key_value_max, key_value_step)
    return web.json_response(result)


async def handle_ht_sweep(request):
    """'Monte-Carlo'-Parametersweep fuer HalfTrend: testet einen Bereich von Amplitude,
    Channel Deviation (SL-Abstand) und Base Risk (TP-Abstand) gegeneinander und gibt die
    besten/schlechtesten Kombinationen zurueck."""
    from strategies import run_ht_param_sweep
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    body = await request.json()
    try:
        days = max(1, min(365, int(body.get("days", 30))))
        amplitude_min = max(2, int(body.get("amplitude_min", 10)))
        amplitude_max = max(amplitude_min, int(body.get("amplitude_max", 40)))
        amplitude_step = max(1, int(body.get("amplitude_step", 2)))
        channel_dev_min = max(0.01, float(body.get("channel_dev_min", 1.0)))
        channel_dev_max = max(channel_dev_min, float(body.get("channel_dev_max", 4.0)))
        channel_dev_step = max(0.01, float(body.get("channel_dev_step", 0.5)))
        base_risk_min = max(0.01, float(body.get("base_risk_min", 1.0)))
        base_risk_max = max(base_risk_min, float(body.get("base_risk_max", 5.0)))
        base_risk_step = max(0.01, float(body.get("base_risk_step", 0.5)))
    except (TypeError, ValueError):
        return web.json_response({"error": "Ungültige Zahlenwerte im Sweep-Bereich."}, status=400)

    cfg = dict(BOTS[symbol]["config"])
    overrides = body.get("config")
    if isinstance(overrides, dict):
        cfg.update({k: v for k, v in overrides.items() if k in cfg})

    result = await run_ht_param_sweep(symbol, cfg, days, amplitude_min, amplitude_max, amplitude_step,
                                       channel_dev_min, channel_dev_max, channel_dev_step,
                                       base_risk_min, base_risk_max, base_risk_step)
    return web.json_response(result)


async def handle_stf_sweep(request):
    """'Monte-Carlo'-Parametersweep fuer SuperTrend Fusion: testet einen Bereich von
    ATR-Periode und Faktor gegeneinander und gibt die besten/schlechtesten Kombinationen
    zurueck."""
    from strategies import run_stf_param_sweep
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    body = await request.json()
    try:
        days = max(1, min(365, int(body.get("days", 30))))
        atr_period_min = max(1, int(body.get("atr_period_min", 1)))
        atr_period_max = max(atr_period_min, int(body.get("atr_period_max", 10)))
        atr_period_step = max(1, int(body.get("atr_period_step", 1)))
        factor_min = max(0.01, float(body.get("factor_min", 1.0)))
        factor_max = max(factor_min, float(body.get("factor_max", 5.0)))
        factor_step = max(0.01, float(body.get("factor_step", 0.5)))
    except (TypeError, ValueError):
        return web.json_response({"error": "Ungültige Zahlenwerte im Sweep-Bereich."}, status=400)

    cfg = dict(BOTS[symbol]["config"])
    overrides = body.get("config")
    if isinstance(overrides, dict):
        cfg.update({k: v for k, v in overrides.items() if k in cfg})

    result = await run_stf_param_sweep(symbol, cfg, days, atr_period_min, atr_period_max, atr_period_step,
                                        factor_min, factor_max, factor_step)
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
    await save_bot_state()
    return web.json_response({"success": True})


