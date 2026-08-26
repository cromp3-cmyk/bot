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
        "binance_market_type": os.getenv("BINANCE_MARKET_TYPE", "spot"),  # "spot" oder "futures" -
        # gilt global fuer JEDE Kerzen-basierte Strategie (Backtest UND live): "futures" nutzt
        # Binance USD-M Perpetual (fapi.binance.com) statt Spot - dieselben Symbolnamen, aber
        # eigener (leicht abweichender) Kurs. Wichtig zum 1:1-Vergleich mit TradingView-Charts
        # auf ".P"-Symbolen (z.B. "BTCUSDT.P"), die selbst auf dem Perpetual-Kurs basieren.
        "entry_mode": os.getenv("ENTRY_MODE", "grid"),  # "grid", "obi_scalp", "oms_scalp", "fib_reversal", "halftrend"
        "margin": float(os.getenv("GRID_MARGIN", "20")),
        "leverage": int(os.getenv("GRID_LEVERAGE", "3")),
        "grid_mode": os.getenv("GRID_MODE", "pct"),  # "pct" oder "usd"
        "grid_direction_mode": os.getenv("GRID_DIRECTION_MODE", "both"),  # "both" | "long_only" | "short_only"
        "grid_step_pct": float(os.getenv("GRID_STEP_PCT", "0.25")),
        "tp_step_pct": float(os.getenv("TP_STEP_PCT", "0.25")),
        "grid_step_usd": float(os.getenv("GRID_STEP_USD", "150")),
        "tp_step_usd": float(os.getenv("TP_STEP_USD", "150")),
        "max_nachkauf": int(os.getenv("MAX_NACHKAUF", "5")),
        "grid_sl_enabled": os.getenv("GRID_SL_ENABLED", "false").lower() == "true",
        "grid_sl_manual_usd": float(os.getenv("GRID_SL_MANUAL_USD", "20.0")),
        "grid_anchor_follow_enabled": os.getenv("GRID_ANCHOR_FOLLOW_ENABLED", "false").lower() == "true",  # nur relevant bei long_only/short_only - siehe on_price_update
        "grid_anchor_follow_pct": float(os.getenv("GRID_ANCHOR_FOLLOW_PCT", "1.0")),  # ab wie viel % Abstand vom Anker (in der gesperrten Richtung) der Anker auf den aktuellen Kurs nachgezogen wird
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
        "oms_exit_mode": os.getenv("OMS_EXIT_MODE", "tp1_trail"),  # "tp1_trail" oder "single_tp"
        "oms_tp1_close_pct": float(os.getenv("OMS_TP1_CLOSE_PCT", "50")),
        "oms_sl_usd": float(os.getenv("OMS_SL_USD", "3.5")),
        "oms_trail_distance_usd": float(os.getenv("OMS_TRAIL_DISTANCE_USD", "1.5")),
        "oms_dca_enabled": os.getenv("OMS_DCA_ENABLED", "true").lower() == "true",
        "oms_dca_max_entries": int(os.getenv("OMS_DCA_MAX_ENTRIES", "2")),
        "oms_dca_size_fraction": float(os.getenv("OMS_DCA_SIZE_FRACTION", "0.6")),
        "oms_dca_min_pullback_usd": float(os.getenv("OMS_DCA_MIN_PULLBACK_USD", "1.0")),
        "oms_reverse_on_signal": os.getenv("OMS_REVERSE_ON_SIGNAL", "false").lower() == "true",
        "oms_rsi_filter_enabled": os.getenv("OMS_RSI_FILTER_ENABLED", "false").lower() == "true",
        "oms_rsi_resolution": os.getenv("OMS_RSI_RESOLUTION", "1m"),
        "oms_rsi_period": int(os.getenv("OMS_RSI_PERIOD", "14")),
        "oms_rsi_midline": float(os.getenv("OMS_RSI_MIDLINE", "50")),
        "oms_oi_filter_enabled": os.getenv("OMS_OI_FILTER_ENABLED", "false").lower() == "true",
        "oms_oi_window_seconds": float(os.getenv("OMS_OI_WINDOW_SECONDS", "30")),
        "oms_oi_min_change_pct": float(os.getenv("OMS_OI_MIN_CHANGE_PCT", "0.001")),
        "oms_oi_min_score": float(os.getenv("OMS_OI_MIN_SCORE", "0.3")),
        "oms_liq_filter_enabled": os.getenv("OMS_LIQ_FILTER_ENABLED", "false").lower() == "true",
        "oms_liq_window_seconds": float(os.getenv("OMS_LIQ_WINDOW_SECONDS", "60")),
        "oms_liq_min_ratio": float(os.getenv("OMS_LIQ_MIN_RATIO", "0.2")),
        "quad_stoch_resolution": os.getenv("QUAD_STOCH_RESOLUTION", "1m"),
        "fib_resolution": os.getenv("FIB_RESOLUTION", "1h"),  # "1h" oder "4h"
        "fib_lookback_candles": int(os.getenv("FIB_LOOKBACK_CANDLES", "100")),
        "fib_entry1_level": float(os.getenv("FIB_ENTRY1_LEVEL", "0.882")),
        "fib_entry2_level": float(os.getenv("FIB_ENTRY2_LEVEL", "0.941")),
        "fib_tp1_level": float(os.getenv("FIB_TP1_LEVEL", "0.786")),
        "fib_tp2_level": float(os.getenv("FIB_TP2_LEVEL", "0.667")),
        "fib_sl_level": float(os.getenv("FIB_SL_LEVEL", "1.0")),
        "fib_tp1_close_pct": float(os.getenv("FIB_TP1_CLOSE_PCT", "50")),
        "fib_cooldown_seconds": float(os.getenv("FIB_COOLDOWN_SECONDS", "300")),
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
        # Diamond Algo (portiert aus dem gleichnamigen Pine-v5-Indikator) - nur der Signal-Kern:
        # SuperTrend(Sensitivity*2, ATR-Periode) + SMA-Filter, optionaler 200er-EMA-Trendfilter
        # fuer "Smart"-Signale (im Original nur Label-Text, hier ein echter Filter). SL/TP
        # ATR-basiert wie im Original (atrBand = ta.atr(atrLen) * atrRisk), TP als R:R-Vielfaches:
        "da_resolution": os.getenv("DA_RESOLUTION", "5m"),
        "da_atr_period": int(os.getenv("DA_ATR_PERIOD", "11")),
        "da_sensitivity": float(os.getenv("DA_SENSITIVITY", "2.0")),
        "da_sma_period": int(os.getenv("DA_SMA_PERIOD", "13")),
        "da_ema_trend_period": int(os.getenv("DA_EMA_TREND_PERIOD", "200")),
        "da_signal_mode": os.getenv("DA_SIGNAL_MODE", "all"),  # "all" oder "smart_only"
        "da_entry_trigger": os.getenv("DA_ENTRY_TRIGGER", "candle_close"),
        "da_exit_trigger": os.getenv("DA_EXIT_TRIGGER", "candle_close"),
        "da_invert_direction": os.getenv("DA_INVERT_DIRECTION", "false").lower() == "true",
        "da_sl_enabled": os.getenv("DA_SL_ENABLED", "true").lower() == "true",
        "da_tp_enabled": os.getenv("DA_TP_ENABLED", "true").lower() == "true",
        "da_risk_atr_period": int(os.getenv("DA_RISK_ATR_PERIOD", "14")),
        "da_risk_mult": float(os.getenv("DA_RISK_MULT", "1.0")),
        "da_tp_rr": float(os.getenv("DA_TP_RR", "2.0")),
        "da_sl_cooldown_seconds": float(os.getenv("DA_SL_COOLDOWN_SECONDS", "30")),
        "da_use_heikin_ashi": os.getenv("DA_USE_HEIKIN_ASHI", "false").lower() == "true",
        # ELTE Smart (portiert aus dem gleichnamigen Pine-v5-Indikator, nur "Normal"-Modus):
        # SuperTrend(ohlc4) mit automatisch aus der Marktvolatilitaet abgeleiteter Sensitivity.
        # TP1(50%)->Break-Even, TP2(50% vom Rest=25% gesamt)->SL auf TP1, TP3(Rest):
        "es_resolution": os.getenv("ES_RESOLUTION", "5m"),
        "es_atr_period": int(os.getenv("ES_ATR_PERIOD", "10")),
        "es_auto_sensitivity": os.getenv("ES_AUTO_SENSITIVITY", "true").lower() == "true",
        "es_sensitivity": float(os.getenv("ES_SENSITIVITY", "3.0")),
        "es_vol_period": int(os.getenv("ES_VOL_PERIOD", "10")),
        "es_vol_ma_len": int(os.getenv("ES_VOL_MA_LEN", "55")),
        "es_entry_trigger": os.getenv("ES_ENTRY_TRIGGER", "candle_close"),
        "es_exit_trigger": os.getenv("ES_EXIT_TRIGGER", "candle_close"),
        "es_invert_direction": os.getenv("ES_INVERT_DIRECTION", "false").lower() == "true",
        "es_risk_atr_period": int(os.getenv("ES_RISK_ATR_PERIOD", "14")),
        "es_risk_mult": float(os.getenv("ES_RISK_MULT", "2.2")),
        "es_tp1_close_pct": float(os.getenv("ES_TP1_CLOSE_PCT", "50")),
        "es_tp2_close_pct": float(os.getenv("ES_TP2_CLOSE_PCT", "50")),
        "es_tp1_rr": float(os.getenv("ES_TP1_RR", "1.0")),
        "es_tp2_rr": float(os.getenv("ES_TP2_RR", "2.0")),
        "es_tp3_rr": float(os.getenv("ES_TP3_RR", "3.0")),
        "es_sl_cooldown_seconds": float(os.getenv("ES_SL_COOLDOWN_SECONDS", "30")),
        "es_reenter_on_flip": os.getenv("ES_REENTER_ON_FLIP", "false").lower() == "true",
        "es_sl_enabled": os.getenv("ES_SL_ENABLED", "true").lower() == "true",
        "es_tp_enabled": os.getenv("ES_TP_ENABLED", "true").lower() == "true",
        "es_sl_mode": os.getenv("ES_SL_MODE", "atr"),  # "atr" oder "manual"
        "es_sl_manual_usd": float(os.getenv("ES_SL_MANUAL_USD", "5.0")),
        "es_tp_mode": os.getenv("ES_TP_MODE", "atr"),  # "atr" (TP1/TP2/TP3-Stufen) oder "manual" (ein einzelnes festes $-Ziel)
        "es_tp_manual_usd": float(os.getenv("ES_TP_MANUAL_USD", "5.0")),
        "es_breakeven_pct_enabled": os.getenv("ES_BREAKEVEN_PCT_ENABLED", "false").lower() == "true",
        "es_breakeven_trigger_pct": float(os.getenv("ES_BREAKEVEN_TRIGGER_PCT", "0.1")),
        "cp_resolution": os.getenv("CP_RESOLUTION", "5m"),
        "cp_signal_source": os.getenv("CP_SIGNAL_SOURCE", "three_line_strike"),  # "three_line_strike" | "engulfing" | "both"
        "cp_three_line_strict": os.getenv("CP_THREE_LINE_STRICT", "true").lower() == "true",
        "cp_engulfing_strict": os.getenv("CP_ENGULFING_STRICT", "true").lower() == "true",
        "cp_direction_mode": os.getenv("CP_DIRECTION_MODE", "both"),  # "both" | "long_only" | "short_only"
        "cp_flip_exit_enabled": os.getenv("CP_FLIP_EXIT_ENABLED", "true").lower() == "true",
        "cp_risk_atr_period": int(os.getenv("CP_RISK_ATR_PERIOD", "14")),
        "cp_risk_mult": float(os.getenv("CP_RISK_MULT", "1.5")),
        "cp_tp_rr": float(os.getenv("CP_TP_RR", "1.0")),
        "cp_sl_enabled": os.getenv("CP_SL_ENABLED", "true").lower() == "true",
        "cp_sl_mode": os.getenv("CP_SL_MODE", "atr"),  # "atr" oder "manual"
        "cp_sl_manual_usd": float(os.getenv("CP_SL_MANUAL_USD", "5.0")),
        "cp_tp_enabled": os.getenv("CP_TP_ENABLED", "true").lower() == "true",
        "cp_tp_mode": os.getenv("CP_TP_MODE", "atr"),  # "atr" oder "manual"
        "cp_tp_manual_usd": float(os.getenv("CP_TP_MANUAL_USD", "5.0")),
        "cp_sl_cooldown_seconds": float(os.getenv("CP_SL_COOLDOWN_SECONDS", "30")),
        "cp_breakeven_enabled": os.getenv("CP_BREAKEVEN_ENABLED", "true").lower() == "true",
        "cp_breakeven_trigger_mult": float(os.getenv("CP_BREAKEVEN_TRIGGER_MULT", "0.5")),
        "mo7_resolution": os.getenv("MO7_RESOLUTION", "5m"),
        "mo7_entry_mode": os.getenv("MO7_ENTRY_MODE", "threshold_cross"),  # "threshold_cross" | "five_candle_sum"
        "mo7_rsi_len": int(os.getenv("MO7_RSI_LEN", "14")),
        "mo7_stoch_len": int(os.getenv("MO7_STOCH_LEN", "14")),
        "mo7_wpr_len": int(os.getenv("MO7_WPR_LEN", "14")),
        "mo7_mfi_len": int(os.getenv("MO7_MFI_LEN", "14")),
        "mo7_macd_fast": int(os.getenv("MO7_MACD_FAST", "12")),
        "mo7_macd_slow": int(os.getenv("MO7_MACD_SLOW", "26")),
        "mo7_buy_threshold": float(os.getenv("MO7_BUY_THRESHOLD", "20")),
        "mo7_sell_threshold": float(os.getenv("MO7_SELL_THRESHOLD", "85")),
        "mo7_sum_low": float(os.getenv("MO7_SUM_LOW", "100")),
        "mo7_sum_high": float(os.getenv("MO7_SUM_HIGH", "400")),
        "mo7_trend_threshold": float(os.getenv("MO7_TREND_THRESHOLD", "55")),
        "mo7_trend_deadband": float(os.getenv("MO7_TREND_DEADBAND", "0")),
        "mo7_direction_mode": os.getenv("MO7_DIRECTION_MODE", "both"),
        "mo7_flip_exit_enabled": os.getenv("MO7_FLIP_EXIT_ENABLED", "true").lower() == "true",
        "mo7_sl_enabled": os.getenv("MO7_SL_ENABLED", "true").lower() == "true",
        "mo7_sl_manual_usd": float(os.getenv("MO7_SL_MANUAL_USD", "5.0")),
        "mo7_tp_enabled": os.getenv("MO7_TP_ENABLED", "true").lower() == "true",
        "mo7_tp_manual_usd": float(os.getenv("MO7_TP_MANUAL_USD", "5.0")),
        "mo7_sl_cooldown_seconds": float(os.getenv("MO7_SL_COOLDOWN_SECONDS", "30")),
        "utb_resolution": os.getenv("UTB_RESOLUTION", "5m"),
        "utb_atr_period": int(os.getenv("UTB_ATR_PERIOD", "1")),
        "utb_sensitivity": float(os.getenv("UTB_SENSITIVITY", "1.0")),
        "utb_heikin_ashi": os.getenv("UTB_HEIKIN_ASHI", "false").lower() == "true",
        "utb_hull_period": int(os.getenv("UTB_HULL_PERIOD", "31")),
        "utb_flip_trigger": os.getenv("UTB_FLIP_TRIGGER", "hull_color"),  # "hull_color" | "hull_and_signal" | "opposite_signal" | "signal_only"
        "utb_direction_mode": os.getenv("UTB_DIRECTION_MODE", "both"),
        "utb_sl_enabled": os.getenv("UTB_SL_ENABLED", "false").lower() == "true",
        "utb_sl_manual_usd": float(os.getenv("UTB_SL_MANUAL_USD", "5.0")),
        "utb_sl_cooldown_seconds": float(os.getenv("UTB_SL_COOLDOWN_SECONDS", "30")),
        "utb_mtf_filter_enabled": os.getenv("UTB_MTF_FILTER_ENABLED", "false").lower() == "true",
        "utb_mtf_tf1": os.getenv("UTB_MTF_TF1", "1m"),  # wie bei Pieki Algo: bis zu 3 Zeiteinheiten gemittelt
        "utb_mtf_tf2": os.getenv("UTB_MTF_TF2", "2m"),
        "utb_mtf_tf3": os.getenv("UTB_MTF_TF3", "3m"),  # "off" = diese TF nicht mit einbeziehen
        "utb_mtf_fast_len": int(os.getenv("UTB_MTF_FAST_LEN", "5")),
        "utb_mtf_slow_len": int(os.getenv("UTB_MTF_SLOW_LEN", "9")),
        "utb_mtf_atr_len": int(os.getenv("UTB_MTF_ATR_LEN", "14")),
        "utb_mtf_long_threshold": float(os.getenv("UTB_MTF_LONG_THRESHOLD", "0.5")),
        "utb_mtf_short_threshold": float(os.getenv("UTB_MTF_SHORT_THRESHOLD", "-0.5")),
        "wtc_resolution": os.getenv("WTC_RESOLUTION", "5m"),
        "wtc_channel_len": int(os.getenv("WTC_CHANNEL_LEN", "9")),
        "wtc_average_len": int(os.getenv("WTC_AVERAGE_LEN", "12")),
        "wtc_ma_len": int(os.getenv("WTC_MA_LEN", "3")),
        "wtc_os_level": float(os.getenv("WTC_OS_LEVEL", "-53")),
        "wtc_ob_level": float(os.getenv("WTC_OB_LEVEL", "53")),
        "wtc_require_zone": os.getenv("WTC_REQUIRE_ZONE", "true").lower() == "true",
        "wtc_direction_mode": os.getenv("WTC_DIRECTION_MODE", "both"),
        "wtc_always_in_market": os.getenv("WTC_ALWAYS_IN_MARKET", "false").lower() == "true",
        "wtc_flip_exit_enabled": os.getenv("WTC_FLIP_EXIT_ENABLED", "true").lower() == "true",
        "wtc_sl_enabled": os.getenv("WTC_SL_ENABLED", "true").lower() == "true",
        "wtc_sl_manual_usd": float(os.getenv("WTC_SL_MANUAL_USD", "5.0")),
        "wtc_tp_enabled": os.getenv("WTC_TP_ENABLED", "true").lower() == "true",
        "wtc_tp_manual_usd": float(os.getenv("WTC_TP_MANUAL_USD", "5.0")),
        "wtc_sl_cooldown_seconds": float(os.getenv("WTC_SL_COOLDOWN_SECONDS", "30")),
        "pk_resolution": os.getenv("PK_RESOLUTION", "5m"),
        "pk_sensitivity": float(os.getenv("PK_SENSITIVITY", "3.0")),  # Original-Pine-Default "Sensivity" (Faktor = sensitivity*2)
        "pk_atr_period": int(os.getenv("PK_ATR_PERIOD", "11")),  # Original fest auf 11 verdrahtet, hier einstellbar
        "pk_sma_period": int(os.getenv("PK_SMA_PERIOD", "13")),  # sma9 im Original (13-Perioden-SMA trotz des Namens)
        "pk_direction_mode": os.getenv("PK_DIRECTION_MODE", "both"),  # "both" | "long_only" | "short_only"
        "pk_exit_mode": os.getenv("PK_EXIT_MODE", "flip"),  # "flip" (immer im Markt, Wechsel bei Gegen-Signal) | "fixed_tp_sl"
        "pk_sl_enabled": os.getenv("PK_SL_ENABLED", "true").lower() == "true",
        "pk_sl_manual_usd": float(os.getenv("PK_SL_MANUAL_USD", "5.0")),
        "pk_tp_enabled": os.getenv("PK_TP_ENABLED", "true").lower() == "true",
        "pk_tp_manual_usd": float(os.getenv("PK_TP_MANUAL_USD", "10.0")),
        "pk_sl_cooldown_seconds": float(os.getenv("PK_SL_COOLDOWN_SECONDS", "30")),
        "pk_trailing_enabled": os.getenv("PK_TRAILING_ENABLED", "false").lower() == "true",
        "pk_trailing_activation_pct": float(os.getenv("PK_TRAILING_ACTIVATION_PCT", "0.2")),  # Trade muss um X% im Profit sein, bevor Trailing aktiviert (SL -> Breakeven)
        "pk_trailing_step_pct": float(os.getenv("PK_TRAILING_STEP_PCT", "0.2")),  # danach wird der SL im Abstand von X% zum bisherigen Best-Preis nachgezogen
        "pk_mtf_filter_enabled": os.getenv("PK_MTF_FILTER_ENABLED", "false").lower() == "true",
        "pk_mtf_tf1": os.getenv("PK_MTF_TF1", "1m"),  # bis zu 3 Zeiteinheiten, wie "Block 1" im Original (avgB1 = Durchschnitt aus 3 TFs)
        "pk_mtf_tf2": os.getenv("PK_MTF_TF2", "2m"),
        "pk_mtf_tf3": os.getenv("PK_MTF_TF3", "3m"),  # "off" = diese TF nicht mit einbeziehen
        "pk_mtf_fast_len": int(os.getenv("PK_MTF_FAST_LEN", "5")),
        "pk_mtf_slow_len": int(os.getenv("PK_MTF_SLOW_LEN", "9")),
        "pk_mtf_atr_len": int(os.getenv("PK_MTF_ATR_LEN", "14")),
        "pk_mtf_long_threshold": float(os.getenv("PK_MTF_LONG_THRESHOLD", "0.5")),
        "pk_mtf_short_threshold": float(os.getenv("PK_MTF_SHORT_THRESHOLD", "-0.5")),
        "fr_resolution": os.getenv("FR_RESOLUTION", "5m"),
        "fr_periods": int(os.getenv("FR_PERIODS", "2")),  # "n" im Original-Pine-Script (Kerzen links+rechts fuer die Fraktal-Bestaetigung)
        "fr_direction_mode": os.getenv("FR_DIRECTION_MODE", "both"),  # "both" | "long_only" | "short_only"
        "fr_invert_direction": os.getenv("FR_INVERT_DIRECTION", "false").lower() == "true",  # Tief-Fraktal=Verkauf, Hoch-Fraktal=Kauf statt umgekehrt
        "fr_zscore_filter_enabled": os.getenv("FR_ZSCORE_FILTER_ENABLED", "false").lower() == "true",
        "fr_zscore_resolution": os.getenv("FR_ZSCORE_RESOLUTION", "same"),  # "same" = eigener Handels-Zeitrahmen, sonst z.B. "15m"/"1h"
        "fr_zscore_lookback": int(os.getenv("FR_ZSCORE_LOOKBACK", "20")),
        "fr_zscore_smooth": int(os.getenv("FR_ZSCORE_SMOOTH", "3")),
        "fr_sl_enabled": os.getenv("FR_SL_ENABLED", "false").lower() == "true",
        "fr_sl_manual_usd": float(os.getenv("FR_SL_MANUAL_USD", "5.0")),
        "fr_sl_cooldown_seconds": float(os.getenv("FR_SL_COOLDOWN_SECONDS", "30")),
        "cd_resolution": os.getenv("CD_RESOLUTION", "1m"),
        "cd_threshold": float(os.getenv("CD_THRESHOLD", "50")),  # Konviktions-Score (-100..100) muss diese Schwelle kreuzen
        "cd_rejection_mult": float(os.getenv("CD_REJECTION_MULT", "1.5")),  # Docht muss X-mal so lang wie der Koerper sein, um als Ablehnung (Hammer/Shooting-Star) zu zaehlen
        "cd_direction_mode": os.getenv("CD_DIRECTION_MODE", "both"),  # "both" | "long_only" | "short_only"
        "cd_zscore_filter_enabled": os.getenv("CD_ZSCORE_FILTER_ENABLED", "false").lower() == "true",
        "cd_zscore_resolution": os.getenv("CD_ZSCORE_RESOLUTION", "same"),  # "same" = eigener Handels-Zeitrahmen, sonst z.B. "15m"/"1h"
        "cd_zscore_lookback": int(os.getenv("CD_ZSCORE_LOOKBACK", "20")),
        "cd_zscore_smooth": int(os.getenv("CD_ZSCORE_SMOOTH", "3")),
        "cd_rsi_filter_enabled": os.getenv("CD_RSI_FILTER_ENABLED", "false").lower() == "true",  # RSI-Regime-Filter: RSI > Mittellinie -> nur Long, RSI < Mittellinie -> nur Short (auf demselben Zeitrahmen wie das Kerzen-DNA-Signal)
        "cd_rsi_length": int(os.getenv("CD_RSI_LENGTH", "14")),
        "cd_rsi_midline": float(os.getenv("CD_RSI_MIDLINE", "50")),
        "cd_adx_filter_enabled": os.getenv("CD_ADX_FILTER_ENABLED", "false").lower() == "true",  # ADX/DI-Trendfilter: ADX > Schwelle UND +DI>-DI -> nur Long, ADX > Schwelle UND -DI>+DI -> nur Short (sonst, inkl. ADX unter Schwelle = kein klarer Trend, BEIDE Richtungen gesperrt)
        "cd_adx_length": int(os.getenv("CD_ADX_LENGTH", "14")),
        "cd_adx_threshold": float(os.getenv("CD_ADX_THRESHOLD", "20")),
        "cd_sl_enabled": os.getenv("CD_SL_ENABLED", "false").lower() == "true",
        "cd_sl_manual_usd": float(os.getenv("CD_SL_MANUAL_USD", "5.0")),
        "cd_sl_cooldown_seconds": float(os.getenv("CD_SL_COOLDOWN_SECONDS", "30")),
        "cd_use_heikin_ashi": os.getenv("CD_USE_HEIKIN_ASHI", "false").lower() == "true",  # Score wird auf HA-Kerzen berechnet, Ein-/Ausstieg trotzdem immer zum echten Kurs
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
        "oms_signal": None, "oms_obi_direction": None, "oms_cvd_ok": None, "oms_funding_ok": None, "oms_rsi_ok": None, "oms_rsi": None,
        "oms_liq_buffer": [], "oms_liq_ratio": None, "oms_liq_count": 0, "oms_liq_ok": None,
        "scalp_board": {},
        "quad_stoch_history": [],
        "da_opens": [], "da_highs": [], "da_lows": [], "da_closes": [], "da_direction": None,
        "da_atr_risk_last": None, "da_sl_price": None, "da_tp_price": None, "da_sl_cooldown_until": 0.0,
        "es_opens": [], "es_highs": [], "es_lows": [], "es_closes": [], "es_direction": None,
        "es_sensitivity_last": None, "es_risk_atr_last": None, "es_sl_cooldown_until": 0.0,
        "es_sl_price": None, "es_tp1_price": None, "es_tp2_price": None, "es_tp3_price": None,
        "es_tp1_done": False, "es_tp2_done": False, "es_breakeven_pct_done": False,
        "cp_opens": [], "cp_highs": [], "cp_lows": [], "cp_closes": [], "cp_last_signal": None,
        "cp_risk_atr_last": None, "cp_sl_cooldown_until": 0.0,
        "cp_sl_price": None, "cp_tp_price": None, "cp_breakeven_done": False,
        "mo7_last_value": None, "mo7_sl_cooldown_until": 0.0,
        "mo7_sl_price": None, "mo7_tp_price": None,
        "utb_last_hull_green": None,
        "utb_sl_price": None, "utb_sl_cooldown_until": 0.0,
        "fr_sl_price": None, "fr_sl_cooldown_until": 0.0,
        "cd_sl_price": None, "cd_sl_cooldown_until": 0.0,
        "utb_trend_pct_last": None,
        "wtc_last_wt1": None, "wtc_last_wt2": None, "wtc_sl_cooldown_until": 0.0,
        "wtc_sl_price": None, "wtc_tp_price": None,
        "pk_sl_price": None, "pk_tp_price": None, "pk_sl_cooldown_until": 0.0,
        "pk_trail_active": False, "pk_trail_best_price": None,
        "pk_trend_pct_last": None,
        "oms_oi_history": [], "oms_oi_score": None, "oms_oi_ok": None, "oms_open_interest": None,
        "oms_obi_history": [],
        "oms_tp1_done": False, "oms_trail_price": None,
        "oms_dca_count": 0, "oms_last_entry_price": None,
        "oms_price_history": [], "oms_markers": [],
        "fib": None, "fib_entry1_done": False, "fib_entry2_done": False, "fib_tp1_done": False,
        "fib_sl_active_price": None, "fib_last_trade_time": 0.0,
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


# Globale Schalter, unabhaengig von einzelnen Coins - z.B. um bei knappen Server-Ressourcen
# (siehe Render Memory/CPU-Limit) Last komplett abzuschalten, ohne jeden Coin einzeln umzustellen.
GLOBAL_SETTINGS = {
    "scalp_board_enabled": True,   # Scalp-Board-Berechnung (RSI/Stoch/MACD/MO7/OBI auf 10-60s) fuer ALLE Coins
    "copytrading_enabled": True,   # Copytrading vom Hyperliquid-Leaderboard komplett an/aus
}


async def save_global_settings():
    r = await get_redis()
    if r is None:
        return
    try:
        await r.set("gridbot:global_settings", json.dumps(GLOBAL_SETTINGS))
    except Exception as e:
        debug_log("⚠️ Speichern der globalen Einstellungen fehlgeschlagen", {"error": str(e)})


async def load_global_settings():
    r = await get_redis()
    if r is None:
        return
    try:
        raw = await r.get("gridbot:global_settings")
        if raw:
            GLOBAL_SETTINGS.update(json.loads(raw))
            debug_log("✅ Globale Einstellungen aus Redis geladen", GLOBAL_SETTINGS)
    except Exception as e:
        debug_log("⚠️ Laden der globalen Einstellungen fehlgeschlagen", {"error": str(e)})


async def handle_global_settings_get(request):
    return web.json_response(GLOBAL_SETTINGS)


async def handle_global_settings_update(request):
    body = await request.json()
    changed = False
    for key in ("scalp_board_enabled", "copytrading_enabled"):
        if key in body:
            GLOBAL_SETTINGS[key] = bool(body[key])
            changed = True
    if changed:
        await save_global_settings()
        debug_log("⚙️ Globale Einstellungen geändert", GLOBAL_SETTINGS)
    return web.json_response({"success": True, **GLOBAL_SETTINGS})


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
    "obi_breakeven_triggered",
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
        direction_mode = cfg.get("grid_direction_mode", "both")
        if direction_mode == "both" or (direction_mode == "long_only" and opposite == "long") or (direction_mode == "short_only" and opposite == "short"):
            await execute_entry(symbol, opposite, price, is_add_on=False)
        # sonst (Richtung erlaubt die Gegenrichtung nicht): bleibt flach, wartet auf das naechste
        # Grid-Level in der erlaubten Richtung (siehe on_price_update)



DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8"><title>Grid-Bot Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<link href="https://cdn.jsdelivr.net/npm/gridstack@10/dist/gridstack.min.css" rel="stylesheet"/>
<script src="https://cdn.jsdelivr.net/npm/gridstack@10/dist/gridstack-all.js"></script>
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
  .grid-stack-item-content { background: var(--panel); border: 1px solid var(--panel-border); border-radius: 14px; overflow: hidden; display: flex; flex-direction: column; }
  .widget-drag-handle { cursor: move; padding: 8px 12px; font-size: 12px; font-weight: 700; color: var(--text-dim); background: rgba(255,255,255,0.03); border-bottom: 1px solid var(--panel-border); user-select: none; display: flex; align-items: center; gap: 6px; flex-shrink: 0; }
  .widget-drag-handle::before { content: "⠿"; opacity: 0.5; }
  .widget-body { padding: 10px; overflow: auto; flex: 1; min-height: 0; }
  .widget-body .panel-card { margin-bottom: 0; border: none; padding: 0; box-shadow: none; border-radius: 0; background: transparent; }
  #btn-reset-layout { background: rgba(124,138,168,0.15); color: var(--text-dim); border: 1px solid var(--panel-border); border-radius: 8px; padding: 6px 12px; font-size: 12px; cursor: pointer; float: right; }
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
  <div class="topbar-right">
    <label style="font-size:12px; color:var(--text-dim); margin-right:14px; display:inline-flex; align-items:center; gap:5px; cursor:pointer;" title="Scalp-Board-Berechnung (RSI/Stoch/MACD/MO7/OBI) für ALLE Coins global an/aus - spart CPU/RAM, wenn du gerade nicht manuell scalpst">
      <input type="checkbox" id="toggle-scalp-board-global" style="cursor:pointer;"> ⚡ Scalp-Details
    </label>
    <label style="font-size:12px; color:var(--text-dim); margin-right:14px; display:inline-flex; align-items:center; gap:5px; cursor:pointer;" title="Copytrading komplett an/aus - pausiert Leaderboard-Abruf und alle Trader-Beobachtung/Kopie">
      <input type="checkbox" id="toggle-copytrading-global" style="cursor:pointer;"> 📡 Copytrading
    </label>
    <a href="/copytrading" style="color:#93c5fd; text-decoration:none; font-size:13px; margin-right:14px;">📡 Copy-Trading →</a><span id="mode-badge"></span><span id="active-badge"></span>
  </div>
</div>
<div class="container">

<div class="coin-overview" id="coin-overview"></div>

<div id="oms-grid-header" style="display:none; margin-bottom:8px;">
  <button id="btn-reset-layout" type="button">↺ Layout zurücksetzen</button>
  <div style="font-size:11px; color:var(--text-dim); padding-top:8px;">Ziehe an der Titelleiste eines Kachel, um sie zu verschieben - an der unteren rechten Ecke ziehen, um die Größe zu ändern.</div>
</div>
<div class="grid-stack" id="oms-grid" style="margin-bottom:12px;"></div>

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
<form id="config-form" novalidate>
  <div><label>Margin (USDC)</label><input type="number" step="any" id="margin"></div>

  <div><label>Hebel</label><input type="number" step="1" id="leverage"></div>
  <div><label>Strategie</label>
    <select class="cfg" id="entry_mode">
      <option value="grid">Neutrales Grid (Ø-Einstieg/Nachkauf/TP)</option>
      <option value="obi_scalp">OBI-Scalp (Orderbuch-Ungleichgewicht, symmetrisches TP/SL)</option>
      <option value="oms_scalp">OBI-Momentum-Scalp (OBI + CVD-Bestätigung + Funding-Filter, TP1+Trailing, Nachkauf)</option>
      <option value="fib_reversal">Fibonacci-Reversal (Einstieg 0.882/0.941, TP 0.786/0.667, SL 1.0)</option>
      <option value="halftrend">HalfTrend (Swing-Hoch/-Tief-Trendwechsel, optional ATR2-basiertes SL+TP, invertierbar)</option>
      <option value="diamond_algo">Diamond Algo (SuperTrend+SMA-Signal, optional 200-EMA-Smart-Filter, ATR-basiertes SL+TP)</option>
      <option value="elte_smart">ELTE Smart (SuperTrend auf ohlc4 mit Auto-Sensitivity, TP1/TP2/TP3 gestufte Teilverkäufe mit nachziehendem SL)</option>
      <option value="candle_patterns">Candle Patterns (3 Line Strike / Engulfing, SL+TP fest oder ATR-basiert, ATR-Breakeven)</option>
      <option value="mo7_scalp">MO7 Scalp (Composite-Oszillator aus 7 Indikatoren, Schwellenwert-Cross oder 5-Kerzen-Summe, fester SL+TP)</option>
      <option value="ut_bot_hull">UT Bot + Hull Flip (ATR-Trailing-Stop, immer im Markt, Flip-Trigger wählbar, kein SL/TP)</option>
      <option value="wavetrend_cross">WaveTrend Cross (Cipher-B-Kernsignal, Zonenfilter wählbar, immer im Markt oder normal, fester SL+TP)</option>
      <option value="pieki_algo">Pieki Algo (SuperTrend+SMA9-Signal, Flip oder fester SL+TP, optionaler MTF-Trend%-Filter)</option>
      <option value="fractals_flip">Williams Fractals (Swing-High/Low-Umkehrpunkte, immer im Markt, nur Buy/Sell-Wechsel)</option>
      <option value="candle_dna">Kerzen-DNA (eigener Konviktions-Score aus Körper+Docht je Kerze, immer im Markt, nur Buy/Sell-Wechsel)</option>
    </select>
  </div>
  <div data-mode="obi_scalp"><label>OBI Schwelle</label><input type="number" step="0.01" id="obi_threshold"></div>
  <div data-mode="obi_scalp"><label>OBI Modus</label>
    <select class="cfg" id="obi_mode">
      <option value="momentum">Momentum (mit dem Ungleichgewicht - empfohlen)</option>
      <option value="mean_reversion">Mean-Reversion (dagegen, wie RSI)</option>
      <option value="reversal">Reversal (separater Long/Short-Einstieg bei Umkehr aus Extremzone)</option>
      <option value="reversal_instant">Reversal-Sofort (getrennte Long/Short-Schwellen, sofort bei Durchbruch, ohne Rückprall-Wartezeit)</option>
    </select>
  </div>
  <div data-mode="obi_scalp"><label>OBI-Fenster (Sek.)</label><input type="number" step="1" id="obi_window_fast_seconds"></div>
  <div data-mode="obi_scalp"><label>Orderbuch-Level (Empfehlung: oberste 5-10)</label><input type="number" step="1" id="obi_levels"></div>
  <div data-mode="obi_scalp"><label>TP (%)</label><input type="number" step="any" id="obi_tp_pct"></div>
  <div data-mode="obi_scalp"><label>SL (%)</label><input type="number" step="any" id="obi_sl_pct"></div>
  <div data-mode="obi_scalp"><label>Cooldown (Sek.)</label><input type="number" step="1" id="obi_cooldown_seconds"></div>
  <div data-mode="obi_scalp" style="grid-column:1/-1;">
    <label style="display:flex; align-items:center; gap:6px; cursor:pointer; font-weight:400;">
      <input type="checkbox" id="obi-advanced-toggle" style="width:auto;">
      ⚙️ Erweiterte OBI-Einstellungen anzeigen (Reversal-Feinjustierung, Filter, Breakeven - für die meisten nicht nötig)
    </label>
  </div>
  <div id="obi-advanced-fields" style="display:none; grid-column:1/-1; grid-template-columns: repeat(auto-fit, minmax(170px,1fr)); gap:14px; align-items:end;">
  <div data-mode="obi_scalp"><label>Reversal OBI-Wert Long (überverkauft, negativ)</label><input type="number" step="0.01" id="obi_long_threshold"></div>
  <div data-mode="obi_scalp"><label>Reversal OBI-Wert Short (überkauft, positiv)</label><input type="number" step="0.01" id="obi_short_threshold"></div>
  <div data-mode="obi_scalp"><label>Reversal Rückprall-Schwelle</label><input type="number" step="0.01" id="obi_reversal_min_bounce"></div>
  <div data-mode="obi_scalp"><label>Reversal-Sofort: Reset-Verhältnis (Anteil der Schwelle, z.B. 0.5 = 50%)</label><input type="number" step="0.05" id="obi_instant_reset_ratio"></div>
  <div data-mode="obi_scalp"><label>OBI mittel (Sek.)</label><input type="number" step="1" id="obi_window_medium_seconds"></div>
  <div data-mode="obi_scalp"><label>OBI langsam (Sek.)</label><input type="number" step="1" id="obi_window_slow_seconds"></div>
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
  <div data-mode="obi_scalp"><label>TP ($)</label><input type="number" step="any" id="obi_tp_usd"></div>
  <div data-mode="obi_scalp"><label>SL ($)</label><input type="number" step="any" id="obi_sl_usd"></div>
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
  </div>

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
  <div data-mode="oms_scalp"><label>Exit-Modus</label>
    <select class="cfg" id="oms_exit_mode">
      <option value="tp1_trail">TP1 + Trailing (Teilverkauf, Rest wird nachgezogen)</option>
      <option value="single_tp">Nur TP (kompletter Ausstieg bei Zielerreichung, kein Teilverkauf/Trailing)</option>
    </select>
  </div>
  <div data-mode="oms_scalp"><label id="oms_tp1_usd_label">TP1 Ziel ($, Teilverkauf)</label><input type="number" step="0.1" id="oms_tp1_usd"></div>
  <div data-mode="oms_scalp" data-oms-exit-mode="tp1_trail"><label>TP1 Teilverkauf (% der Position)</label><input type="number" step="1" id="oms_tp1_close_pct"></div>
  <div data-mode="oms_scalp"><label>Stop-Loss ($, gesamte Position - NICHT die Liquidation)</label><input type="number" step="0.1" id="oms_sl_usd"></div>
  <div data-mode="oms_scalp" data-oms-exit-mode="tp1_trail"><label>Trailing-Abstand nach TP1 ($)</label><input type="number" step="0.1" id="oms_trail_distance_usd"></div>
  <div data-mode="oms_scalp"><label>Nachkauf (DCA)</label>
    <select class="cfg" id="oms_dca_enabled">
      <option value="true">An</option>
      <option value="false">Aus</option>
    </select>
  </div>
  <div data-mode="oms_scalp"><label>Nachkauf: max. Stufen</label><input type="number" step="1" id="oms_dca_max_entries"></div>
  <div data-mode="oms_scalp"><label>Nachkauf: Größen-Faktor je Stufe (0-1, fallend)</label><input type="number" step="0.05" id="oms_dca_size_fraction"></div>
  <div data-mode="oms_scalp"><label>Nachkauf: Mindest-Rücksetzer ($, bevor nachgekauft wird)</label><input type="number" step="0.1" id="oms_dca_min_pullback_usd"></div>
  <div data-mode="oms_scalp"><label>Bei Gegen-Signal sofort umdrehen (Reverse) statt auf SL/TP1/Trail zu warten</label>
    <select class="cfg" id="oms_reverse_on_signal">
      <option value="false">Aus (nur SL/TP1/Trail schließt die Position)</option>
      <option value="true">An (bestätigtes Gegen-Signal dreht sofort um)</option>
    </select>
  </div>
  <div data-mode="oms_scalp"><label>RSI-Regime-Filter (RSI &lt; Mittellinie → nur Short, RSI &gt; Mittellinie → nur Long)</label>
    <select class="cfg" id="oms_rsi_filter_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="oms_scalp"><label>RSI Zeitrahmen</label>
    <select class="cfg" id="oms_rsi_resolution">
      <option value="10s">10 Sekunden</option>
      <option value="15s">15 Sekunden</option>
      <option value="30s">30 Sekunden</option>
      <option value="45s">45 Sekunden</option>
      <option value="1m">1 Minute</option>
      <option value="2m">2 Minuten</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
    </select>
  </div>
  <div data-mode="oms_scalp"><label>RSI Periode</label><input type="number" step="1" id="oms_rsi_period"></div>
  <div data-mode="oms_scalp"><label>RSI Mittellinie</label><input type="number" step="1" id="oms_rsi_midline"></div>
  <div data-mode="oms_scalp"><label>Open-Interest-Filter (Preis+OI kombiniert muss Richtung stützen)</label>
    <select class="cfg" id="oms_oi_filter_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="oms_scalp"><label>OI Zeitfenster (Sek.)</label><input type="number" step="1" id="oms_oi_window_seconds"></div>
  <div data-mode="oms_scalp"><label>OI Mindest-Änderung (%, z.B. 0.001 = 0.1%)</label><input type="number" step="0.0001" id="oms_oi_min_change_pct"></div>
  <div data-mode="oms_scalp"><label>OI Mindest-Score (0-1)</label><input type="number" step="0.05" id="oms_oi_min_score"></div>
  <div data-mode="oms_scalp"><label>Liquidations-Filter (Zwangsliquidationen müssen Richtung stützen)</label>
    <select class="cfg" id="oms_liq_filter_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="oms_scalp"><label>Liquidations Zeitfenster (Sek.)</label><input type="number" step="1" id="oms_liq_window_seconds"></div>
  <div data-mode="oms_scalp"><label>Liquidations Mindest-Verhältnis (0-1)</label><input type="number" step="0.05" id="oms_liq_min_ratio"></div>

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
  <div data-mode="halftrend"><label>Zeitrahmen</label>
    <select class="cfg" id="ht_resolution">
      <option value="10s">10 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="15s">15 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="30s">30 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="45s">45 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="1m">1 Minute</option>
      <option value="2m">2 Minuten</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="30m">30 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
      <option value="custom">Eigene Minuten...</option>
    </select>
    <input type="number" step="1" min="1" id="ht_resolution_custom_minutes" placeholder="z.B. 8 oder 24" style="display:none; margin-top:6px; width:140px;">
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

  <div data-mode="diamond_algo" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:6px 0;">
    📡 <b>Signal</b>: SuperTrend (Sensitivity×2 als ATR-Multiplikator) kreuzt den Kurs + SMA-Filter bestätigt.
    💎 <b>Smart</b>: zusätzlich muss der 200er-EMA-Trend zustimmen (Original-Skript nennt das nur so, hier ein echter Filter).
    🎯 <b>SL/TP</b> optional, ATR-basiert wie im Original.
  </div>
  <div data-mode="diamond_algo"><label>Zeitrahmen</label>
    <select class="cfg" id="da_resolution">
      <option value="10s">10 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="15s">15 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="30s">30 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="45s">45 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="1m">1 Minute</option>
      <option value="2m">2 Minuten</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="30m">30 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
      <option value="custom">Eigene Minuten...</option>
    </select>
    <input type="number" step="1" min="1" id="da_resolution_custom_minutes" placeholder="z.B. 8 oder 24" style="display:none; margin-top:6px; width:140px;">
  </div>
  <div data-mode="diamond_algo"><label>ATR-Periode (SuperTrend-Kern)</label><input type="number" step="1" id="da_atr_period"></div>
  <div data-mode="diamond_algo"><label>Sensitivity (ATR-Multiplikator = Sensitivity × 2 - höher = weniger empfindlich!)</label><input type="number" step="0.01" id="da_sensitivity"></div>
  <div data-mode="diamond_algo"><label>SMA-Filter-Periode</label><input type="number" step="1" id="da_sma_period"></div>
  <div data-mode="diamond_algo"><label>EMA-Trendfilter-Periode (für Smart-Signale)</label><input type="number" step="1" id="da_ema_trend_period"></div>
  <div data-mode="diamond_algo"><label>Signal-Auswahl</label>
    <select class="cfg" id="da_signal_mode">
      <option value="all">Alle Signale (Buy/Sell)</option>
      <option value="smart_only">Nur Smart-Signale (200-EMA-bestätigt)</option>
    </select>
  </div>
  <div data-mode="diamond_algo"><label>Einstieg auslösen</label>
    <select class="cfg" id="da_entry_trigger">
      <option value="candle_close">Bei Kerzenschluss</option>
      <option value="tick">Sofort bei jedem Preis-Tick</option>
    </select>
  </div>
  <div data-mode="diamond_algo"><label>Ausstieg auslösen</label>
    <select class="cfg" id="da_exit_trigger">
      <option value="candle_close">Bei Kerzenschluss</option>
      <option value="tick">Sofort bei jedem Preis-Tick</option>
    </select>
  </div>
  <div data-mode="diamond_algo"><label>Richtung invertieren (Kontra-Modus)</label>
    <select class="cfg" id="da_invert_direction">
      <option value="false">Aus (normal)</option>
      <option value="true">An (invertiert)</option>
    </select>
  </div>
  <div data-mode="diamond_algo"><label>Stop-Loss (ATR(Risk-Periode) × Risk-Multiplikator)</label>
    <select class="cfg" id="da_sl_enabled">
      <option value="false">Aus (nur Gegen-Signal-Exit)</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="diamond_algo"><label>Take-Profit (SL-Abstand × R:R-Multiplikator)</label>
    <select class="cfg" id="da_tp_enabled">
      <option value="false">Aus (nur Gegen-Signal-Exit)</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="diamond_algo"><label>Risiko-ATR-Periode (separat vom Signal-ATR, Original-Default 14)</label><input type="number" step="1" id="da_risk_atr_period"></div>
  <div data-mode="diamond_algo"><label>Risiko-Multiplikator (Original: "Risk %", Default 1)</label><input type="number" step="0.1" id="da_risk_mult"></div>
  <div data-mode="diamond_algo"><label>TP R:R-Multiplikator (Original: TP1=1, TP2=2, TP3=3)</label><input type="number" step="0.5" id="da_tp_rr"></div>
  <div data-mode="diamond_algo"><label>Cooldown nach SL (Sek.)</label><input type="number" step="1" id="da_sl_cooldown_seconds"></div>
  <div data-mode="diamond_algo"><label>Kerzenart für die Signalberechnung</label>
    <select class="cfg" id="da_use_heikin_ashi">
      <option value="false">Normale Kerzen</option>
      <option value="true">Heikin Ashi (wie bei TradingView Chart-Typ-Umschaltung - glättet den Trend, SL/TP lösen trotzdem am echten Kurs aus)</option>
    </select>
  </div>

  <div data-mode="elte_smart" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:6px 0;">
    📡 <b>Signal</b>: SuperTrend auf ohlc4 kreuzt den Kurs - reiner Crossover, kein Zusatzfilter (Original "Normal"-Modus).
    🎯 <b>Sensitivity</b> standardmäßig automatisch aus der Marktvolatilität abgeleitet (2.85-4.0).
    💰 <b>TP1 50% → Break-Even, TP2 50% vom Rest (=25% gesamt) → SL auf TP1, TP3 Rest.</b>
  </div>
  <div data-mode="elte_smart"><label>Zeitrahmen</label>
    <select class="cfg" id="es_resolution">
      <option value="10s">10 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="15s">15 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="30s">30 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="45s">45 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="1m">1 Minute</option>
      <option value="2m">2 Minuten</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="30m">30 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
      <option value="custom">Eigene Minuten...</option>
    </select>
    <input type="number" step="1" min="1" id="es_resolution_custom_minutes" placeholder="z.B. 8 oder 24" style="display:none; margin-top:6px; width:140px;">
  </div>
  <div data-mode="elte_smart"><label>ATR-Periode (SuperTrend-Kern)</label><input type="number" step="1" id="es_atr_period"></div>
  <div data-mode="elte_smart"><label>Sensitivity-Modus</label>
    <select class="cfg" id="es_auto_sensitivity">
      <option value="true">Automatisch (aus Marktvolatilität abgeleitet)</option>
      <option value="false">Manuell (fester Wert)</option>
    </select>
  </div>
  <div data-mode="elte_smart"><label>Sensitivity (nur bei manuellem Modus)</label><input type="number" step="0.01" id="es_sensitivity"></div>
  <div data-mode="elte_smart"><label>Volatilitäts-Periode (EWMA, für Auto-Sensitivity)</label><input type="number" step="1" id="es_vol_period"></div>
  <div data-mode="elte_smart"><label>Volatilitäts-Durchschnitt-Periode (für Auto-Sensitivity)</label><input type="number" step="1" id="es_vol_ma_len"></div>
  <div data-mode="elte_smart"><label>Einstieg auslösen</label>
    <select class="cfg" id="es_entry_trigger">
      <option value="candle_close">Bei Kerzenschluss</option>
      <option value="tick">Sofort bei jedem Preis-Tick</option>
    </select>
  </div>
  <div data-mode="elte_smart"><label>Ausstieg auslösen</label>
    <select class="cfg" id="es_exit_trigger">
      <option value="candle_close">Bei Kerzenschluss</option>
      <option value="tick">Sofort bei jedem Preis-Tick</option>
    </select>
  </div>
  <div data-mode="elte_smart"><label>Richtung invertieren (Kontra-Modus)</label>
    <select class="cfg" id="es_invert_direction">
      <option value="false">Aus (normal)</option>
      <option value="true">An (invertiert)</option>
    </select>
  </div>
  <div data-mode="elte_smart"><label>Risiko-ATR-Periode (Original-Default 14)</label><input type="number" step="1" id="es_risk_atr_period"></div>
  <div data-mode="elte_smart"><label>Risiko-Multiplikator (Original-Default 2.2)</label><input type="number" step="0.1" id="es_risk_mult"></div>
  <div data-mode="elte_smart"><label>TP1 Teilverkauf (% der Gesamtposition)</label><input type="number" step="1" id="es_tp1_close_pct"></div>
  <div data-mode="elte_smart"><label>TP2 Teilverkauf (% der VERBLEIBENDEN Position)</label><input type="number" step="1" id="es_tp2_close_pct"></div>
  <div data-mode="elte_smart"><label>TP1 R:R-Multiplikator</label><input type="number" step="0.5" id="es_tp1_rr"></div>
  <div data-mode="elte_smart"><label>TP2 R:R-Multiplikator</label><input type="number" step="0.5" id="es_tp2_rr"></div>
  <div data-mode="elte_smart"><label>TP3 R:R-Multiplikator</label><input type="number" step="0.5" id="es_tp3_rr"></div>
  <div data-mode="elte_smart"><label>Cooldown nach SL (Sek.)</label><input type="number" step="1" id="es_sl_cooldown_seconds"></div>
  <div data-mode="elte_smart"><label>Bei Gegen-Signal sofort umdrehen (Reverse)</label>
    <select class="cfg" id="es_reenter_on_flip">
      <option value="false">Aus (Standard) - Gegen-Signal schließt nur, neue Position erst bei einem wirklich neuen Signal</option>
      <option value="true">An - dasselbe Gegen-Signal schließt UND eröffnet sofort die Gegenposition</option>
    </select>
  </div>
  <div data-mode="elte_smart"><label>Stop-Loss</label>
    <select class="cfg" id="es_sl_enabled">
      <option value="true">An</option>
      <option value="false">Aus (nur Gegen-Signal schließt die Position - z.B. für reines Flip-System mit "Sofort umdrehen")</option>
    </select>
  </div>
  <div data-mode="elte_smart"><label>SL-Modus</label>
    <select class="cfg" id="es_sl_mode">
      <option value="atr">ATR-basiert (Risiko-ATR × Risiko-Multiplikator, wie TP1/TP2/TP3)</option>
      <option value="manual">Fester $-Betrag (nur der ANFÄNGLICHE SL vor TP1 - danach übernimmt Break-Even/TP1-Lock wie gehabt)</option>
    </select>
  </div>
  <div data-mode="elte_smart"><label>SL Fester $-Betrag (nur bei SL-Modus "Fest")</label><input type="number" step="0.5" id="es_sl_manual_usd"></div>
  <div data-mode="elte_smart"><label>Take-Profit (TP1/TP2/TP3)</label>
    <select class="cfg" id="es_tp_enabled">
      <option value="true">An</option>
      <option value="false">Aus (nur Gegen-Signal schließt die Position - z.B. für reines Flip-System mit "Sofort umdrehen")</option>
    </select>
  </div>
  <div data-mode="elte_smart"><label>TP-Modus</label>
    <select class="cfg" id="es_tp_mode">
      <option value="atr">ATR-basiert (TP1/TP2/TP3-Stufen mit Teilverkäufen, Original-System)</option>
      <option value="manual">Fester $-Betrag (EIN einzelnes Ziel, komplette Position schließt dort - kein TP2/TP3)</option>
    </select>
  </div>
  <div data-mode="elte_smart"><label>TP Fester $-Betrag (nur bei TP-Modus "Fest")</label><input type="number" step="0.5" id="es_tp_manual_usd"></div>
  <div data-mode="elte_smart"><label>Prozent-Break-Even (unabhängig von TP1 - SL sofort auf Einstieg sobald Kurs sich X% bewegt hat)</label>
    <select class="cfg" id="es_breakeven_pct_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="elte_smart"><label>Prozent-Break-Even Auslöse-Schwelle (%)</label><input type="number" step="0.01" id="es_breakeven_trigger_pct"></div>

  <div data-mode="candle_patterns" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:6px 0;">
    🕯️ Signal kommt aus reinen Candlestick-Mustern (aus dem TMA-Overlay-Pine-Script portiert), kein
    Trend-Indikator. SL/TP je einzeln ATR-basiert oder fester $-Betrag wählbar, dazu optionaler
    ATR-Breakeven (Stop wandert auf Einstieg) - wie bei ELTE Smart / "The Phoenix".
  </div>
  <div data-mode="candle_patterns"><label>Zeitrahmen</label>
    <select class="cfg" id="cp_resolution">
      <option value="10s">10 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="15s">15 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="30s">30 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="45s">45 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="1m">1 Minute</option>
      <option value="2m">2 Minuten</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="30m">30 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
      <option value="custom">Eigene Minuten...</option>
    </select>
    <input type="number" step="1" min="1" id="cp_resolution_custom_minutes" placeholder="z.B. 8 oder 24" style="display:none; margin-top:6px; width:140px;">
  </div>
  <div data-mode="candle_patterns"><label>Signalquelle</label>
    <select class="cfg" id="cp_signal_source">
      <option value="three_line_strike">3 Line Strike</option>
      <option value="engulfing">Engulfing (Big A$$ Candles)</option>
      <option value="both">Beide (3 Line Strike ODER Engulfing)</option>
    </select>
  </div>
  <div data-mode="candle_patterns"><label>3 Line Strike "Strict"-Filter (nur bei Signalquelle 3 Line Strike/Beide)</label>
    <select class="cfg" id="cp_three_line_strict">
      <option value="true">An - RSI(14) muss zur Signalrichtung passen (bullisch nur wenn RSI &gt; 50, bearisch nur wenn RSI &lt; 50)</option>
      <option value="false">Aus - reines Kerzenmuster ohne RSI-Filter</option>
    </select>
  </div>
  <div data-mode="candle_patterns"><label>Engulfing "Strict"-Filter (nur bei Signalquelle Engulfing/Beide)</label>
    <select class="cfg" id="cp_engulfing_strict">
      <option value="true">An - Schlusskurs muss zwischen MA1(21, SMMA) und MA4(200, SMMA) liegen (Original-Default)</option>
      <option value="false">Aus - reines Kerzenmuster ohne MA-Filter</option>
    </select>
  </div>
  <div data-mode="candle_patterns"><label>Richtung</label>
    <select class="cfg" id="cp_direction_mode">
      <option value="both">Beide (Long + Short)</option>
      <option value="long_only">Nur Long</option>
      <option value="short_only">Nur Short</option>
    </select>
  </div>
  <div data-mode="candle_patterns"><label>Bei Gegen-Signal sofort schließen (Flip-Exit)</label>
    <select class="cfg" id="cp_flip_exit_enabled">
      <option value="true">An - Gegen-Signal schließt die Position sofort, unabhängig von SL/TP</option>
      <option value="false">Aus - nur SL/TP entscheiden über den Ausstieg</option>
    </select>
  </div>
  <div data-mode="candle_patterns"><label>Risiko-ATR-Periode</label><input type="number" step="1" id="cp_risk_atr_period"></div>
  <div data-mode="candle_patterns"><label>Risiko-Multiplikator (ATR-Modus, Default 1.5 wie "The Phoenix")</label><input type="number" step="0.1" id="cp_risk_mult"></div>
  <div data-mode="candle_patterns"><label>TP R:R-Multiplikator (TP-Abstand = SL-Abstand × dieser Wert, nur ATR-Modus)</label><input type="number" step="0.1" id="cp_tp_rr"></div>
  <div data-mode="candle_patterns"><label>Cooldown nach SL (Sek.)</label><input type="number" step="1" id="cp_sl_cooldown_seconds"></div>
  <div data-mode="candle_patterns"><label>Stop-Loss</label>
    <select class="cfg" id="cp_sl_enabled">
      <option value="true">An</option>
      <option value="false">Aus (nur Gegen-Signal/TP schließt die Position)</option>
    </select>
  </div>
  <div data-mode="candle_patterns"><label>SL-Modus</label>
    <select class="cfg" id="cp_sl_mode">
      <option value="atr">ATR-basiert (Risiko-ATR × Risiko-Multiplikator)</option>
      <option value="manual">Fester $-Betrag</option>
    </select>
  </div>
  <div data-mode="candle_patterns"><label>SL Fester $-Betrag (nur bei SL-Modus "Fest")</label><input type="number" step="0.5" id="cp_sl_manual_usd"></div>
  <div data-mode="candle_patterns"><label>Take-Profit</label>
    <select class="cfg" id="cp_tp_enabled">
      <option value="true">An</option>
      <option value="false">Aus (nur Gegen-Signal/SL schließt die Position)</option>
    </select>
  </div>
  <div data-mode="candle_patterns"><label>TP-Modus</label>
    <select class="cfg" id="cp_tp_mode">
      <option value="atr">ATR-basiert (SL-Abstand × TP-R:R-Multiplikator)</option>
      <option value="manual">Fester $-Betrag</option>
    </select>
  </div>
  <div data-mode="candle_patterns"><label>TP Fester $-Betrag (nur bei TP-Modus "Fest")</label><input type="number" step="0.5" id="cp_tp_manual_usd"></div>
  <div data-mode="candle_patterns"><label>ATR-Breakeven (Stop wandert auf Einstieg, sobald Kurs im Gewinn ist - wie "The Phoenix")</label>
    <select class="cfg" id="cp_breakeven_enabled">
      <option value="true">An</option>
      <option value="false">Aus</option>
    </select>
  </div>
  <div data-mode="candle_patterns"><label>Breakeven Auslöse-Schwelle (× Risiko-ATR, Default 0.5 wie "The Phoenix")</label><input type="number" step="0.1" id="cp_breakeven_trigger_mult"></div>

  <div data-mode="mo7_scalp" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:6px 0;">
    📊 MO7 = Mittelwert aus RSI, Stochastic %K, Williams %R, MFI, MACD (normiert), ROC (normiert)
    und Percent-Rank - alle 0-100 skaliert (portiert aus dem "MO7 Buy/Sell Signal"-Pine-Script).
    NUR native Binance-Zeitrahmen (1m/3m/5m/15m/30m/1h/2h/4h) - kein 2m/Sekunden/eigene Minuten,
    weil MFI Handelsvolumen braucht. Nur fester SL/TP (kein ATR-Modus, kein Breakeven).
  </div>
  <div data-mode="mo7_scalp"><label>Zeitrahmen</label>
    <select class="cfg" id="mo7_resolution">
      <option value="1m">1 Minute</option>
      <option value="3m">3 Minuten</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="30m">30 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="2h">2 Stunden</option>
      <option value="4h">4 Stunden</option>
    </select>
  </div>
  <div data-mode="mo7_scalp"><label>Einstiegsmodus</label>
    <select class="cfg" id="mo7_entry_mode">
      <option value="threshold_cross">Schwellenwert-Cross (BUY beim Unterschreiten der Buy-Schwelle, SELL beim Überschreiten der Sell-Schwelle)</option>
      <option value="five_candle_sum">5-Kerzen-Summe (Summe der letzten 5 MO7-Werte unter/über eigener Schwelle)</option>
      <option value="trend_state">Trend-Zustand (MO7 über Schwelle = Uptrend/Long, darunter = Downtrend/Short - Bot bleibt immer entsprechend positioniert)</option>
    </select>
  </div>
  <div data-mode="mo7_scalp"><label>Trend-Schwelle (nur bei Trend-Zustand)</label><input type="number" step="1" id="mo7_trend_threshold"></div>
  <div data-mode="mo7_scalp"><label>Trend-Totzone (± um die Schwelle, reduziert Hin-und-Her bei Werten nahe der Schwelle)</label><input type="number" step="1" id="mo7_trend_deadband"></div>
  <div data-mode="mo7_scalp"><label>Buy-Schwelle (nur Schwellenwert-Cross, MO7 &lt; Wert)</label><input type="number" step="1" id="mo7_buy_threshold"></div>
  <div data-mode="mo7_scalp"><label>Sell-Schwelle (nur Schwellenwert-Cross, MO7 &gt; Wert)</label><input type="number" step="1" id="mo7_sell_threshold"></div>
  <div data-mode="mo7_scalp"><label>5-Kerzen-Summe Long-Schwelle (nur 5-Kerzen-Summe, Summe &lt; Wert)</label><input type="number" step="1" id="mo7_sum_low"></div>
  <div data-mode="mo7_scalp"><label>5-Kerzen-Summe Short-Schwelle (nur 5-Kerzen-Summe, Summe &gt; Wert)</label><input type="number" step="1" id="mo7_sum_high"></div>
  <div data-mode="mo7_scalp"><label>Richtung</label>
    <select class="cfg" id="mo7_direction_mode">
      <option value="both">Beide (Long + Short)</option>
      <option value="long_only">Nur Long</option>
      <option value="short_only">Nur Short</option>
    </select>
  </div>
  <div data-mode="mo7_scalp"><label>Bei Gegen-Signal sofort schließen (Flip-Exit)</label>
    <select class="cfg" id="mo7_flip_exit_enabled">
      <option value="true">An</option>
      <option value="false">Aus - nur SL/TP entscheiden</option>
    </select>
  </div>
  <div data-mode="mo7_scalp"><label>Stop-Loss</label>
    <select class="cfg" id="mo7_sl_enabled">
      <option value="true">An</option>
      <option value="false">Aus</option>
    </select>
  </div>
  <div data-mode="mo7_scalp"><label>SL Fester $-Betrag</label><input type="number" step="0.5" id="mo7_sl_manual_usd"></div>
  <div data-mode="mo7_scalp"><label>Take-Profit</label>
    <select class="cfg" id="mo7_tp_enabled">
      <option value="true">An</option>
      <option value="false">Aus</option>
    </select>
  </div>
  <div data-mode="mo7_scalp"><label>TP Fester $-Betrag</label><input type="number" step="0.5" id="mo7_tp_manual_usd"></div>
  <div data-mode="mo7_scalp"><label>Cooldown nach SL (Sek.)</label><input type="number" step="1" id="mo7_sl_cooldown_seconds"></div>

  <div data-mode="ut_bot_hull" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:6px 0;">
    🌀 UT Bot Alerts (ATR-Trailing-Stop, weit verbreitetes Pine-Script) gefiltert durch die Hull
    Moving Average-Farbe. IMMER IM MARKT (kein SL/TP, keine flache Position) - die Strategie
    dreht kontinuierlich zwischen Long/Short. Flip-Trigger wählbar.
  </div>
  <div data-mode="ut_bot_hull"><label>Zeitrahmen</label>
    <select class="cfg" id="utb_resolution">
      <option value="10s">10 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="15s">15 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="30s">30 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="45s">45 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="1m">1 Minute</option>
      <option value="2m">2 Minuten</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="30m">30 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
      <option value="custom">Eigene Minuten...</option>
    </select>
    <input type="number" step="1" min="1" id="utb_resolution_custom_minutes" placeholder="z.B. 8 oder 24" style="display:none; margin-top:6px; width:140px;">
  </div>
  <div data-mode="ut_bot_hull"><label>ATR-Periode (UT-Bot, Original-Default 1)</label><input type="number" step="1" id="utb_atr_period"></div>
  <div data-mode="ut_bot_hull"><label>Sensitivity (ATR-Multiplikator für die Trailing-Stop-Distanz)</label><input type="number" step="0.01" id="utb_sensitivity"></div>
  <div data-mode="ut_bot_hull"><label>Signalquelle</label>
    <select class="cfg" id="utb_heikin_ashi">
      <option value="false">Normale Kerzen</option>
      <option value="true">Heikin-Ashi-Kerzen</option>
    </select>
  </div>
  <div data-mode="ut_bot_hull"><label>Hull-MA-Periode</label><input type="number" step="1" id="utb_hull_period"></div>
  <div data-mode="ut_bot_hull"><label>Flip-Trigger</label>
    <select class="cfg" id="utb_flip_trigger">
      <option value="hull_color">Nur Hull-Farbwechsel (UT-Bot-Signal nur für Ersteinstieg)</option>
      <option value="hull_and_signal">Hull-Farbwechsel UND UT-Bot-Gegensignal gleichzeitig</option>
      <option value="opposite_signal">Nur UT-Bot-Gegensignal (Hull nur für Ersteinstieg)</option>
      <option value="signal_only">Nur UT-Bot Buy/Sell im Wechsel (Hull komplett ignoriert, auch beim Ersteinstieg)</option>
    </select>
  </div>
  <div data-mode="ut_bot_hull"><label>Richtung</label>
    <select class="cfg" id="utb_direction_mode">
      <option value="both">Beide (immer im Markt, dreht zwischen Long/Short)</option>
      <option value="long_only">Nur Long (bei Gegen-Flip glattstellen statt drehen)</option>
      <option value="short_only">Nur Short (bei Gegen-Flip glattstellen statt drehen)</option>
    </select>
  </div>
  <div data-mode="ut_bot_hull"><label>Stop-Loss (fester $-Betrag, optional - durchbricht "immer im Markt" nur im SL-Fall)</label>
    <select class="cfg" id="utb_sl_enabled">
      <option value="false">Aus (Standard - reines Flip-System ohne SL)</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="ut_bot_hull"><label>SL Fester $-Betrag (nur wenn Stop-Loss An)</label><input type="number" step="0.5" id="utb_sl_manual_usd"></div>
  <div data-mode="ut_bot_hull"><label>Cooldown nach SL (Sek.)</label><input type="number" step="1" id="utb_sl_cooldown_seconds"></div>
  <div data-mode="ut_bot_hull" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:2px 0;">
    MTF-Trend%-Filter (wie bei Pieki Algo): Trend% wird als Durchschnitt aus bis zu 3
    Zeiteinheiten berechnet. Gilt für JEDEN Einstieg, auch beim Flip in die Gegenrichtung -
    Short nur wenn Trend% unter der Short-Schwelle, Long nur wenn über der Long-Schwelle.
  </div>
  <div data-mode="ut_bot_hull"><label>MTF-Trend%-Filter</label>
    <select class="cfg" id="utb_mtf_filter_enabled">
      <option value="false">Aus</option>
      <option value="true">An - Short nur unter Short-Schwelle, Long nur über Long-Schwelle</option>
    </select>
  </div>
  <div data-mode="ut_bot_hull"><label>Trend% Zeiteinheit 1</label>
    <select class="cfg" id="utb_mtf_tf1">
      <option value="off">Aus</option>
      <option value="1m">1 Minute</option>
      <option value="2m">2 Minuten</option>
      <option value="3m">3 Minuten</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="30m">30 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
      <option value="1d">1 Tag</option>
      <option value="custom">Eigene Minuten...</option>
    </select>
    <input type="number" step="1" min="1" id="utb_mtf_tf1_custom_minutes" placeholder="z.B. 8" style="display:none; margin-top:6px; width:140px;">
  </div>
  <div data-mode="ut_bot_hull"><label>Trend% Zeiteinheit 2</label>
    <select class="cfg" id="utb_mtf_tf2">
      <option value="off">Aus</option>
      <option value="1m">1 Minute</option>
      <option value="2m">2 Minuten</option>
      <option value="3m">3 Minuten</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="30m">30 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
      <option value="1d">1 Tag</option>
      <option value="custom">Eigene Minuten...</option>
    </select>
    <input type="number" step="1" min="1" id="utb_mtf_tf2_custom_minutes" placeholder="z.B. 8" style="display:none; margin-top:6px; width:140px;">
  </div>
  <div data-mode="ut_bot_hull"><label>Trend% Zeiteinheit 3</label>
    <select class="cfg" id="utb_mtf_tf3">
      <option value="off">Aus</option>
      <option value="1m">1 Minute</option>
      <option value="2m">2 Minuten</option>
      <option value="3m">3 Minuten</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="30m">30 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
      <option value="1d">1 Tag</option>
      <option value="custom">Eigene Minuten...</option>
    </select>
    <input type="number" step="1" min="1" id="utb_mtf_tf3_custom_minutes" placeholder="z.B. 8" style="display:none; margin-top:6px; width:140px;">
  </div>
  <div data-mode="ut_bot_hull"><label>Long-Schwelle (Trend% muss darüber liegen)</label><input type="number" step="0.1" id="utb_mtf_long_threshold"></div>
  <div data-mode="ut_bot_hull"><label>Short-Schwelle (Trend% muss darunter liegen)</label><input type="number" step="0.1" id="utb_mtf_short_threshold"></div>
  <div data-mode="ut_bot_hull"><label>Trend% Fast-EMA-Länge</label><input type="number" step="1" id="utb_mtf_fast_len"></div>
  <div data-mode="ut_bot_hull"><label>Trend% Slow-EMA-Länge</label><input type="number" step="1" id="utb_mtf_slow_len"></div>
  <div data-mode="ut_bot_hull"><label>Trend% ATR-Länge (Normierung)</label><input type="number" step="1" id="utb_mtf_atr_len"></div>

  <div data-mode="wavetrend_cross" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:6px 0;">
    🌊 WaveTrend Cross (Kernsignal aus "Cipher B"): wt1 kreuzt wt2 - das sind die grünen/roten
    Punkte im Oszillator. Zonenfilter wählbar (nur in Überkauft/Überverkauft), Richtung "immer im
    Markt" (dreht direkt) oder normal (SL/TP/optional Gegen-Signal beendet die Position). Nur
    fester SL/TP (kein ATR-Modus).
  </div>
  <div data-mode="wavetrend_cross"><label>Zeitrahmen</label>
    <select class="cfg" id="wtc_resolution">
      <option value="10s">10 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="15s">15 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="30s">30 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="45s">45 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="1m">1 Minute</option>
      <option value="2m">2 Minuten</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="30m">30 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
      <option value="custom">Eigene Minuten...</option>
    </select>
    <input type="number" step="1" min="1" id="wtc_resolution_custom_minutes" placeholder="z.B. 8 oder 24" style="display:none; margin-top:6px; width:140px;">
  </div>
  <div data-mode="wavetrend_cross"><label>WT Channel-Länge</label><input type="number" step="1" id="wtc_channel_len"></div>
  <div data-mode="wavetrend_cross"><label>WT Average-Länge</label><input type="number" step="1" id="wtc_average_len"></div>
  <div data-mode="wavetrend_cross"><label>WT MA-Länge</label><input type="number" step="1" id="wtc_ma_len"></div>
  <div data-mode="wavetrend_cross"><label>Zonenfilter (nur in Überkauft/Überverkauft signalisieren)</label>
    <select class="cfg" id="wtc_require_zone">
      <option value="true">An (Original-Verhalten)</option>
      <option value="false">Aus - jeder Cross zählt</option>
    </select>
  </div>
  <div data-mode="wavetrend_cross"><label>Überverkauft-Schwelle (nur bei Zonenfilter An)</label><input type="number" step="1" id="wtc_os_level"></div>
  <div data-mode="wavetrend_cross"><label>Überkauft-Schwelle (nur bei Zonenfilter An)</label><input type="number" step="1" id="wtc_ob_level"></div>
  <div data-mode="wavetrend_cross"><label>Richtung</label>
    <select class="cfg" id="wtc_direction_mode">
      <option value="both">Beide (Long + Short)</option>
      <option value="long_only">Nur Long</option>
      <option value="short_only">Nur Short</option>
    </select>
  </div>
  <div data-mode="wavetrend_cross"><label>Immer im Markt (Buy/Sell im direkten Wechsel, wie UT Bot + Hull)</label>
    <select class="cfg" id="wtc_always_in_market">
      <option value="false">Aus (Standard) - normaler Ein-/Ausstieg, geht zwischendurch flach</option>
      <option value="true">An - dreht direkt bei Gegen-Signal, nie flach außer bei SL/TP</option>
    </select>
  </div>
  <div data-mode="wavetrend_cross"><label>Bei Gegen-Signal sofort schließen (nur relevant wenn "Immer im Markt" Aus)</label>
    <select class="cfg" id="wtc_flip_exit_enabled">
      <option value="true">An</option>
      <option value="false">Aus - nur SL/TP entscheiden</option>
    </select>
  </div>
  <div data-mode="wavetrend_cross"><label>Stop-Loss</label>
    <select class="cfg" id="wtc_sl_enabled">
      <option value="true">An</option>
      <option value="false">Aus</option>
    </select>
  </div>
  <div data-mode="wavetrend_cross"><label>SL Fester $-Betrag</label><input type="number" step="0.5" id="wtc_sl_manual_usd"></div>
  <div data-mode="wavetrend_cross"><label>Take-Profit</label>
    <select class="cfg" id="wtc_tp_enabled">
      <option value="true">An</option>
      <option value="false">Aus</option>
    </select>
  </div>
  <div data-mode="wavetrend_cross"><label>TP Fester $-Betrag</label><input type="number" step="0.5" id="wtc_tp_manual_usd"></div>
  <div data-mode="wavetrend_cross"><label>Cooldown nach SL (Sek.)</label><input type="number" step="1" id="wtc_sl_cooldown_seconds"></div>

  <div data-mode="pieki_algo" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:6px 0;">
    🎯 Pieki Algo (portiert aus "Pieki Algo | Signals &amp; Overlays"): SuperTrend (ATR-Periode × Sensitivity,
    Faktor = Sensitivity×2, wie im Original) kreuzt den Kurs UND SMA9 bestätigt gleichzeitig - erst dann
    zählt das Signal. Exit wählbar: Flip (immer im Markt, dreht direkt) oder fester SL/TP. Optionaler
    MTF-Trend%-Filter: Short nur erlaubt wenn Trend% unter der Short-Schwelle, Long nur wenn über der
    Long-Schwelle (vereinfachte Version des EMA-Spread-Trend% aus deinem MTF-Dashboard-Indikator - hier auf
    EINEM Zeitrahmen berechnet, nicht auf den vollen 9 Timeframes des Original-Scripts).
  </div>
  <div data-mode="pieki_algo"><label>Zeitrahmen</label>
    <select class="cfg" id="pk_resolution">
      <option value="10s">10 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="15s">15 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="30s">30 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="45s">45 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="1m">1 Minute</option>
      <option value="2m">2 Minuten</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="30m">30 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
      <option value="custom">Eigene Minuten...</option>
    </select>
    <input type="number" step="1" min="1" id="pk_resolution_custom_minutes" placeholder="z.B. 8 oder 24" style="display:none; margin-top:6px; width:140px;">
  </div>
  <div data-mode="pieki_algo"><label>Sensitivity (0.01-Schritte, Original-Default 3)</label><input type="number" step="0.01" id="pk_sensitivity"></div>
  <div data-mode="pieki_algo"><label>ATR-Periode (SuperTrend, Original fest 11)</label><input type="number" step="1" id="pk_atr_period"></div>
  <div data-mode="pieki_algo"><label>SMA-Periode (Bestätigungsfilter, Original sma9 = 13)</label><input type="number" step="1" id="pk_sma_period"></div>
  <div data-mode="pieki_algo"><label>Richtung</label>
    <select class="cfg" id="pk_direction_mode">
      <option value="both">Beide (Long + Short)</option>
      <option value="long_only">Nur Long</option>
      <option value="short_only">Nur Short</option>
    </select>
  </div>
  <div data-mode="pieki_algo"><label>Exit-Modus</label>
    <select class="cfg" id="pk_exit_mode">
      <option value="flip">Wechsel (immer im Markt, Flip beim Gegen-Signal)</option>
      <option value="fixed_tp_sl">Fester SL/TP (geht bei Treffer flach, wartet auf nächstes Ersteinstiegs-Signal)</option>
    </select>
  </div>
  <div data-mode="pieki_algo"><label>Stop-Loss (nur bei Exit-Modus "Fester SL/TP")</label>
    <select class="cfg" id="pk_sl_enabled">
      <option value="true">An</option>
      <option value="false">Aus</option>
    </select>
  </div>
  <div data-mode="pieki_algo"><label>SL Fester $-Betrag</label><input type="number" step="0.5" id="pk_sl_manual_usd"></div>
  <div data-mode="pieki_algo"><label>Take-Profit (nur bei Exit-Modus "Fester SL/TP")</label>
    <select class="cfg" id="pk_tp_enabled">
      <option value="true">An</option>
      <option value="false">Aus</option>
    </select>
  </div>
  <div data-mode="pieki_algo"><label>TP Fester $-Betrag</label><input type="number" step="0.5" id="pk_tp_manual_usd"></div>
  <div data-mode="pieki_algo"><label>Cooldown nach SL (Sek., nur bei Exit-Modus "Fester SL/TP")</label><input type="number" step="1" id="pk_sl_cooldown_seconds"></div>
  <div data-mode="pieki_algo" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:2px 0;">
    Trailing-Stop funktioniert in BEIDEN Exit-Modi: sobald der Trade um die Aktivierungs-Schwelle
    im Profit ist, springt der SL auf Breakeven (Einstiegspreis) und wird danach immer im
    gewählten Prozent-Abstand zum bisherigen besten Preis nachgezogen (nie zurück, nur in die
    profitable Richtung). Bei "Fester SL/TP" überschreibt es den festen SL, sobald aktiv. Bei
    "Wechsel" (immer im Markt) unterbricht ein Trailing-Treffer das Prinzip NUR in diesem einen
    Fall - die Position geht dann glatt (inkl. Cooldown) statt auf ein Gegen-Signal zu warten.
  </div>
  <div data-mode="pieki_algo"><label>Trailing-Stop</label>
    <select class="cfg" id="pk_trailing_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="pieki_algo"><label>Aktivierung ab Profit (%)</label><input type="number" step="0.01" id="pk_trailing_activation_pct"></div>
  <div data-mode="pieki_algo"><label>Nachzieh-Abstand (%)</label><input type="number" step="0.01" id="pk_trailing_step_pct"></div>
  <div data-mode="pieki_algo"><label>MTF-Trend%-Filter</label>
    <select class="cfg" id="pk_mtf_filter_enabled">
      <option value="false">Aus</option>
      <option value="true">An - Short nur unter Short-Schwelle, Long nur über Long-Schwelle</option>
    </select>
  </div>
  <div data-mode="pieki_algo" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:2px 0;">
    Trend% wird - wie "Block 1" im Original-Indikator - als Durchschnitt aus bis zu 3 Zeiteinheiten
    berechnet. Eine TF auf "Aus" stellen, um sie aus dem Durchschnitt rauszunehmen (z.B. nur 1
    oder 2 TFs statt 3 nutzen).
  </div>
  <div data-mode="pieki_algo"><label>Trend% Zeiteinheit 1</label>
    <select class="cfg" id="pk_mtf_tf1">
      <option value="off">Aus</option>
      <option value="1m">1 Minute</option>
      <option value="2m">2 Minuten</option>
      <option value="3m">3 Minuten</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="30m">30 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
      <option value="1d">1 Tag</option>
      <option value="custom">Eigene Minuten...</option>
    </select>
    <input type="number" step="1" min="1" id="pk_mtf_tf1_custom_minutes" placeholder="z.B. 8" style="display:none; margin-top:6px; width:140px;">
  </div>
  <div data-mode="pieki_algo"><label>Trend% Zeiteinheit 2</label>
    <select class="cfg" id="pk_mtf_tf2">
      <option value="off">Aus</option>
      <option value="1m">1 Minute</option>
      <option value="2m">2 Minuten</option>
      <option value="3m">3 Minuten</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="30m">30 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
      <option value="1d">1 Tag</option>
      <option value="custom">Eigene Minuten...</option>
    </select>
    <input type="number" step="1" min="1" id="pk_mtf_tf2_custom_minutes" placeholder="z.B. 8" style="display:none; margin-top:6px; width:140px;">
  </div>
  <div data-mode="pieki_algo"><label>Trend% Zeiteinheit 3</label>
    <select class="cfg" id="pk_mtf_tf3">
      <option value="off">Aus</option>
      <option value="1m">1 Minute</option>
      <option value="2m">2 Minuten</option>
      <option value="3m">3 Minuten</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="30m">30 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
      <option value="1d">1 Tag</option>
      <option value="custom">Eigene Minuten...</option>
    </select>
    <input type="number" step="1" min="1" id="pk_mtf_tf3_custom_minutes" placeholder="z.B. 8" style="display:none; margin-top:6px; width:140px;">
  </div>
  <div data-mode="pieki_algo"><label>Long-Schwelle (Trend% muss darüber liegen)</label><input type="number" step="0.1" id="pk_mtf_long_threshold"></div>
  <div data-mode="pieki_algo"><label>Short-Schwelle (Trend% muss darunter liegen)</label><input type="number" step="0.1" id="pk_mtf_short_threshold"></div>
  <div data-mode="pieki_algo"><label>Trend% Fast-EMA-Länge</label><input type="number" step="1" id="pk_mtf_fast_len"></div>
  <div data-mode="pieki_algo"><label>Trend% Slow-EMA-Länge</label><input type="number" step="1" id="pk_mtf_slow_len"></div>
  <div data-mode="pieki_algo"><label>Trend% ATR-Länge (Normierung)</label><input type="number" step="1" id="pk_mtf_atr_len"></div>

  <div data-mode="fractals_flip" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:6px 0;">
    🔺 Williams Fractals: eine Kerze ist ein "Hoch-Fraktal", wenn sie hoeher ist als die
    "Perioden" Kerzen davor UND danach (analog fuer "Tief-Fraktal" beim Tief) - wie im
    Original-Pine-Script, aber vereinfacht (ohne die Gleichstand-Sonderfaelle des Originals,
    die bei fast identischen Hochs/Tiefs noch mehr Fraktale zulassen). Ein Fraktal wird erst
    "Perioden" Kerzen im Nachhinein bestaetigt, kein Echtzeit-Signal. Tief-Fraktal = Kauf-Signal,
    Hoch-Fraktal = Verkauf-Signal. Immer im Markt: dreht direkt beim jeweils naechsten
    Gegen-Signal, keine Filter, kein SL/TP - reiner Buy/Sell-Wechsel.
  </div>
  <div data-mode="fractals_flip"><label>Zeitrahmen</label>
    <select class="cfg" id="fr_resolution">
      <option value="10s">10 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="15s">15 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="30s">30 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="45s">45 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="1m">1 Minute</option>
      <option value="2m">2 Minuten</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="30m">30 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
      <option value="custom">Eigene Minuten...</option>
    </select>
    <input type="number" step="1" min="1" id="fr_resolution_custom_minutes" placeholder="z.B. 8 oder 24" style="display:none; margin-top:6px; width:140px;">
  </div>
  <div data-mode="fractals_flip"><label>Perioden (links+rechts für die Fraktal-Bestätigung)</label><input type="number" step="1" id="fr_periods"></div>
  <div data-mode="fractals_flip"><label>Richtung</label>
    <select class="cfg" id="fr_direction_mode">
      <option value="both">Beide (Long + Short)</option>
      <option value="long_only">Nur Long</option>
      <option value="short_only">Nur Short</option>
    </select>
  </div>
  <div data-mode="fractals_flip"><label>Invertiert-Modus</label>
    <select class="cfg" id="fr_invert_direction">
      <option value="false">Aus - Tief-Fraktal=Kauf, Hoch-Fraktal=Verkauf</option>
      <option value="true">An - Tief-Fraktal=Verkauf, Hoch-Fraktal=Kauf</option>
    </select>
  </div>
  <div data-mode="fractals_flip" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:2px 0;">
    Optionaler Z-Score-Filter (portiert aus "Rolling Z-Score Trend [QuantAlgo]"): misst, wie
    viele Standardabweichungen der Kurs vom gleitenden Durchschnitt entfernt ist. Über 0 = nur
    Long erlaubt, unter 0 = nur Short erlaubt - gilt für jeden Einstieg, auch beim Flip.
  </div>
  <div data-mode="fractals_flip"><label>Z-Score-Filter</label>
    <select class="cfg" id="fr_zscore_filter_enabled">
      <option value="false">Aus</option>
      <option value="true">An - Long nur über 0, Short nur unter 0</option>
    </select>
  </div>
  <div data-mode="fractals_flip"><label>Z-Score Zeiteinheit</label>
    <select class="cfg" id="fr_zscore_resolution">
      <option value="same">Eigener Handels-Zeitrahmen (siehe oben)</option>
      <option value="1m">1 Minute</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="30m">30 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
      <option value="1d">1 Tag</option>
      <option value="custom">Eigene Minuten...</option>
    </select>
    <input type="number" step="1" min="1" id="fr_zscore_resolution_custom_minutes" placeholder="z.B. 120" style="display:none; margin-top:6px; width:140px;">
  </div>
  <div data-mode="fractals_flip"><label>Z-Score Lookback (Kerzen)</label><input type="number" step="1" id="fr_zscore_lookback"></div>
  <div data-mode="fractals_flip"><label>Z-Score Glättung (EMA)</label><input type="number" step="1" id="fr_zscore_smooth"></div>
  <div data-mode="fractals_flip" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:2px 0;">
    Optionaler fester Stop-Loss (fester $-Betrag, wie bei UT-Bot+Hull): durchbricht "immer im
    Markt" NUR im SL-Fall - die Position geht dann glatt (statt zu drehen) und wartet nach einem
    Cooldown auf das nächste gültige Ersteinstiegs-Signal.
  </div>
  <div data-mode="fractals_flip"><label>Stop-Loss</label>
    <select class="cfg" id="fr_sl_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="fractals_flip"><label>SL Fester $-Betrag</label><input type="number" step="0.5" id="fr_sl_manual_usd"></div>
  <div data-mode="fractals_flip"><label>Cooldown nach SL (Sek.)</label><input type="number" step="1" id="fr_sl_cooldown_seconds"></div>

  <div data-mode="candle_dna" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:6px 0;">
    🧬 Kerzen-DNA (eigene Entwicklung, kein Port eines bestehenden Scripts): jede Kerze bekommt
    einen Konviktions-Score von -100 (voll bearisch) bis +100 (voll bullisch) - Basis ist, wie
    groß der Kerzenkörper im Verhältnis zur gesamten Hoch-Tief-Spanne ist (Marubozu-artige Kerzen
    = nah an ±100, Doji-artige Kerzen = nah an 0), plus ein Bonus/Abzug, wenn ein langer Docht auf
    der Gegenseite eine Ablehnung zeigt (Hammer/Shooting-Star). Kreuzt der Score die Schwelle nach
    oben → Kauf, nach unten → Verkauf. Immer im Markt, reiner Buy/Sell-Wechsel - kein Filter, kein
    SL/TP.
  </div>
  <div data-mode="candle_dna"><label>Zeitrahmen</label>
    <select class="cfg" id="cd_resolution">
      <option value="10s">10 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="15s">15 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="30s">30 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="45s">45 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="1m">1 Minute</option>
      <option value="2m">2 Minuten</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="30m">30 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
      <option value="custom">Eigene Minuten...</option>
    </select>
    <input type="number" step="1" min="1" id="cd_resolution_custom_minutes" placeholder="z.B. 8 oder 24" style="display:none; margin-top:6px; width:140px;">
  </div>
  <div data-mode="candle_dna"><label>Konviktions-Schwelle (0-100)</label><input type="number" step="1" id="cd_threshold"></div>
  <div data-mode="candle_dna"><label>Docht-Ablehnung-Faktor (Docht muss X-mal so lang wie der Körper sein)</label><input type="number" step="0.1" id="cd_rejection_mult"></div>
  <div data-mode="candle_dna"><label>Richtung</label>
    <select class="cfg" id="cd_direction_mode">
      <option value="both">Beide (Long + Short)</option>
      <option value="long_only">Nur Long</option>
      <option value="short_only">Nur Short</option>
    </select>
  </div>
  <div data-mode="candle_dna" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:2px 0;">
    Optionaler Z-Score-Filter (portiert aus "Rolling Z-Score Trend [QuantAlgo]"): misst, wie
    viele Standardabweichungen der Kurs vom gleitenden Durchschnitt entfernt ist. Über 0 = nur
    Long erlaubt, unter 0 = nur Short erlaubt - gilt für jeden Einstieg, auch beim Flip.
  </div>
  <div data-mode="candle_dna"><label>Z-Score-Filter</label>
    <select class="cfg" id="cd_zscore_filter_enabled">
      <option value="false">Aus</option>
      <option value="true">An - Long nur über 0, Short nur unter 0</option>
    </select>
  </div>
  <div data-mode="candle_dna"><label>Z-Score Zeiteinheit</label>
    <select class="cfg" id="cd_zscore_resolution">
      <option value="same">Eigener Handels-Zeitrahmen (siehe oben)</option>
      <option value="1m">1 Minute</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="30m">30 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
      <option value="1d">1 Tag</option>
      <option value="custom">Eigene Minuten...</option>
    </select>
    <input type="number" step="1" min="1" id="cd_zscore_resolution_custom_minutes" placeholder="z.B. 120" style="display:none; margin-top:6px; width:140px;">
  </div>
  <div data-mode="candle_dna"><label>Z-Score Lookback (Kerzen)</label><input type="number" step="1" id="cd_zscore_lookback"></div>
  <div data-mode="candle_dna"><label>Z-Score Glättung (EMA)</label><input type="number" step="1" id="cd_zscore_smooth"></div>
  <div data-mode="candle_dna" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:2px 0;">
    Optionaler RSI-Regime-Filter, unabhängig vom Z-Score-Filter kombinierbar (beide können
    gleichzeitig an sein - dann müssen beide zustimmen): RSI über der Mittellinie -> nur Long
    erlaubt, RSI unter der Mittellinie -> nur Short erlaubt. Läuft auf demselben Zeitrahmen wie
    das Kerzen-DNA-Signal selbst.
  </div>
  <div data-mode="candle_dna"><label>RSI-Filter</label>
    <select class="cfg" id="cd_rsi_filter_enabled">
      <option value="false">Aus</option>
      <option value="true">An - Long nur über Mittellinie, Short nur darunter</option>
    </select>
  </div>
  <div data-mode="candle_dna"><label>RSI-Länge</label><input type="number" step="1" min="2" id="cd_rsi_length"></div>
  <div data-mode="candle_dna"><label>RSI-Mittellinie</label><input type="number" step="1" min="1" max="99" id="cd_rsi_midline"></div>
  <div data-mode="candle_dna" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:2px 0;">
    Optionaler ADX/DI-Trendfilter, unabhängig von Z-Score- und RSI-Filter kombinierbar (mehrere
    gleichzeitig aktiv -> alle müssen zustimmen): ADX über der Schwelle UND +DI über -DI -> nur
    Long erlaubt. ADX über der Schwelle UND -DI über +DI -> nur Short erlaubt. Liegt der ADX
    UNTER der Schwelle (kein klarer Trend), sind BEIDE Richtungen gesperrt. Läuft auf demselben
    Zeitrahmen und derselben Kerzenbasis (Heikin-Ashi ja/nein) wie das Kerzen-DNA-Signal selbst.
  </div>
  <div data-mode="candle_dna"><label>ADX/DI-Filter</label>
    <select class="cfg" id="cd_adx_filter_enabled">
      <option value="false">Aus</option>
      <option value="true">An - nur bei genug Trendstärke und passender Richtung</option>
    </select>
  </div>
  <div data-mode="candle_dna"><label>ADX-Länge</label><input type="number" step="1" min="2" id="cd_adx_length"></div>
  <div data-mode="candle_dna"><label>ADX-Schwelle</label><input type="number" step="1" min="0" max="100" id="cd_adx_threshold"></div>
  <div data-mode="candle_dna" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:2px 0;">
    Optionaler fester Stop-Loss (fester $-Betrag, wie bei UT-Bot+Hull): durchbricht "immer im
    Markt" NUR im SL-Fall - die Position geht dann glatt (statt zu drehen) und wartet nach einem
    Cooldown auf das nächste gültige Ersteinstiegs-Signal.
  </div>
  <div data-mode="candle_dna"><label>Stop-Loss</label>
    <select class="cfg" id="cd_sl_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="candle_dna"><label>SL Fester $-Betrag</label><input type="number" step="0.5" id="cd_sl_manual_usd"></div>
  <div data-mode="candle_dna"><label>Cooldown nach SL (Sek.)</label><input type="number" step="1" id="cd_sl_cooldown_seconds"></div>
  <div data-mode="candle_dna"><label>Heikin-Ashi für Score-Berechnung</label>
    <select class="cfg" id="cd_use_heikin_ashi">
      <option value="false">Aus - normale Kerzen</option>
      <option value="true">An - Score wird auf geglätteten HA-Kerzen berechnet (Ein-/Ausstieg trotzdem immer zum echten Kurs)</option>
    </select>
  </div>

  <div data-mode="grid"><label>Richtung</label>
    <select class="cfg" id="grid_direction_mode">
      <option value="both">Beide (Long unter Anker, Short über Anker)</option>
      <option value="long_only">Nur Long</option>
      <option value="short_only">Nur Short</option>
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
  <div data-mode="grid"><label>Stop-Loss (fester $-Betrag auf die Gesamtposition, unabhängig von Nachkauf)</label>
    <select class="cfg" id="grid_sl_enabled">
      <option value="false">Aus (Standard)</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="grid"><label>SL Fester $-Betrag</label><input type="number" step="0.5" id="grid_sl_manual_usd"></div>
  <div data-mode="grid" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:2px 0;">
    Nur relevant bei "Nur Long"/"Nur Short": läuft der Kurs weit in die GESPERRTE Richtung weg
    (z.B. Kurs steigt bei "Nur Long" immer weiter über den Anker), würde der Bot sonst endlos auf
    eine Rückkehr in die alte Zone warten. Ist der Abstand größer als der eingestellte Prozentwert,
    wird der Anker auf den aktuellen Kurs nachgezogen - die Entry-Schwelle bleibt so erreichbar.
    Bei "Beide" ohne Wirkung (dort wird irgendwann immer eine Seite erreicht).
  </div>
  <div data-mode="grid"><label>Anker-Nachführung</label>
    <select class="cfg" id="grid_anchor_follow_enabled">
      <option value="false">Aus (Standard - Anker bleibt fest, bis eine Position schließt)</option>
      <option value="true">An - Anker folgt dem Kurs bei zu großem Abstand in gesperrter Richtung</option>
    </select>
  </div>
  <div data-mode="grid"><label>Nachführ-Schwelle (%)</label><input type="number" step="0.1" min="0.1" id="grid_anchor_follow_pct"></div>
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
  <div><label>Binance-Datenquelle (für alle Kerzen-Strategien, Backtest + Live)</label>
    <select class="cfg" id="binance_market_type">
      <option value="spot">Spot</option>
      <option value="futures">Futures (USD-M Perpetual - zum 1:1-Vergleich mit TradingView ".P"-Charts)</option>
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
    Nur für Fibonacci-Reversal und HalfTrend (Grid/OBI-Scalp/OBI-Momentum-Scalp brauchen
    historische Orderbuch-/Tick-Daten, die es nicht gibt). SL/TP werden pro Kerze am Schlusskurs geprüft,
    nicht Tick-für-Tick wie live. Lighter ist gebührenfrei, es werden also keine Gebühren simuliert.
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:16px;">
    <div><label>Zeitraum (Tage)</label><input type="number" step="1" id="backtest-days" value="30" style="width:100px;"></div>
    <div><label>Robustheits-Check: beste N Trades ausschließen</label><input type="number" step="1" min="0" id="backtest-exclude-top-n" value="1" style="width:100px;"></div>
    <button id="btn-backtest" style="padding:12px 24px;">▶️ Backtest starten</button>
  </div>
  <div id="backtest-status" style="color:var(--text-dim); font-size:13px;"></div>
  <div id="backtest-results" style="display:none; margin-top:16px;">
    <div style="display:flex; gap:20px; flex-wrap:wrap; margin-bottom:12px;">
      <div><div class="label">Kerzen verarbeitet</div><div class="value" id="bt-candles">-</div></div>
      <div><div class="label">Zeitraum tatsächlich</div><div class="value" id="bt-days">-</div></div>
      <div><div class="label">Trades</div><div class="value" id="bt-trades">-</div></div>
      <div><div class="label">davon Teilverkäufe (Fills)</div><div class="value" id="bt-fills">-</div></div>
      <div><div class="label">Trefferquote</div><div class="value" id="bt-winrate">-</div></div>
      <div><div class="label">Gesamt-PnL $</div><div class="value" id="bt-pnl">-</div></div>
      <div><div class="label">Max Drawdown $</div><div class="value" id="bt-dd">-</div></div>
      <div><div class="label">Ø Gewinn / Ø Verlust $</div><div class="value" id="bt-avg">-</div></div>
      <div><div class="label">Bester Einzel-Trade $</div><div class="value" id="bt-best-trade">-</div></div>
      <div><div class="label">PnL ohne beste N Trades $ <span style="font-weight:400;">(Robustheits-Check)</span></div><div class="value" id="bt-pnl-excl-best">-</div></div>
      <div><div class="label">Median-Trade $</div><div class="value" id="bt-median-trade">-</div></div>
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

<div data-mode-section="diamond_algo" style="display:none;">
<h2 class="section-title">🎲 Diamond-Algo-Parameter-Sweep (ATR-Periode × Sensitivity)</h2>
<div class="panel-card">
  <div style="font-size:13px; color:var(--text-dim); margin-bottom:12px;">
    Testet alle Kombinationen aus ATR-Periode (SuperTrend-Kernbaustein) und Sensitivity (ATR-
    Multiplikator = Sensitivity × 2) gegeneinander - das sind die beiden Parameter, die im
    Original tatsächlich das Signal beeinflussen. SMA-/EMA-Perioden bleiben auf den aktuell
    gespeicherten Werten. Ergebnisse mit weniger als 5 Trades sind statistisch kaum
    aussagekräftig und werden nach unten sortiert, aber nicht versteckt.
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Zeitraum (Tage)</label><input type="number" step="1" id="da-sweep-days" value="30" style="width:90px;"></div>
    <div><label>ATR-Periode von</label><input type="number" step="1" id="da-sweep-period-min" value="5" style="width:80px;"></div>
    <div><label>bis</label><input type="number" step="1" id="da-sweep-period-max" value="20" style="width:80px;"></div>
    <div><label>Schritt</label><input type="number" step="1" id="da-sweep-period-step" value="1" style="width:70px;"></div>
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Sensitivity von</label><input type="number" step="0.1" id="da-sweep-sens-min" value="1.0" style="width:80px;"></div>
    <div><label>bis</label><input type="number" step="0.1" id="da-sweep-sens-max" value="5.0" style="width:80px;"></div>
    <div><label>Schritt</label><input type="number" step="0.1" id="da-sweep-sens-step" value="0.5" style="width:70px;"></div>
    <button id="btn-da-sweep" style="padding:12px 24px;">🎲 Sweep starten</button>
  </div>
  <div id="da-sweep-status" style="color:var(--text-dim); font-size:13px;"></div>
  <table id="da-sweep-results-table" style="display:none; margin-top:12px;">
    <thead><tr>
      <th class="sortable" data-key="da_atr_period">ATR-Periode ⇅</th>
      <th class="sortable" data-key="da_sensitivity">Sensitivity ⇅</th>
      <th class="sortable" data-key="trades">Trades ⇅</th>
      <th class="sortable" data-key="win_rate_pct">Trefferquote ⇅</th>
      <th class="sortable" data-key="total_pnl_usd">PnL $ ⇅</th>
      <th class="sortable" data-key="max_drawdown_usd">Max DD $ ⇅</th>
      <th class="sortable" data-key="avg_bars_held">Ø Kerzen gehalten ⇅</th>
    </tr></thead>
    <tbody></tbody>
  </table>
  <h3 style="margin-top:20px; font-size:14px; color:var(--text-dim); display:none;" id="da-sweep-worst-title">📉 Die 20 schlechtesten Kombinationen (nach PnL, unabhängig von der Trade-Anzahl)</h3>
  <table id="da-sweep-worst-table" style="display:none; margin-top:8px;">
    <thead><tr>
      <th class="sortable" data-key="da_atr_period">ATR-Periode ⇅</th>
      <th class="sortable" data-key="da_sensitivity">Sensitivity ⇅</th>
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

<div data-mode-section="elte_smart" style="display:none;">
<h2 class="section-title">🎲 ELTE-Smart-Sensitivity-Sweep</h2>
<div class="panel-card">
  <div style="font-size:13px; color:var(--text-dim); margin-bottom:12px;">
    Testet nur die manuelle Sensitivity (Auto-Sensitivity wird für den Sweep zwangsweise
    deaktiviert) über einen Wertebereich - mit 2 Nachkommastellen, genau wie im Original-Skript
    (Schritt 0,01, Bereich 0,11 bis 20). Alle anderen Einstellungen (ATR-Periode, SL/TP-Modus,
    R:R usw.) bleiben auf den aktuell gespeicherten Werten. Ergebnisse mit weniger als 5 Trades
    sind statistisch kaum aussagekräftig und werden nach unten sortiert, aber nicht versteckt.
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Zeitraum (Tage)</label><input type="number" step="1" id="es-sweep-days" value="30" style="width:90px;"></div>
    <div><label>Sensitivity von</label><input type="number" step="0.01" id="es-sweep-sens-min" value="0.11" style="width:90px;"></div>
    <div><label>bis</label><input type="number" step="0.01" id="es-sweep-sens-max" value="5.00" style="width:90px;"></div>
    <div><label>Schritt</label><input type="number" step="0.01" id="es-sweep-sens-step" value="0.01" style="width:90px;"></div>
    <button id="btn-es-sweep" style="padding:12px 24px;">🎲 Sweep starten</button>
  </div>
  <div id="es-sweep-status" style="color:var(--text-dim); font-size:13px;"></div>
  <table id="es-sweep-results-table" style="display:none; margin-top:12px;">
    <thead><tr>
      <th class="sortable" data-key="es_sensitivity">Sensitivity ⇅</th>
      <th class="sortable" data-key="trades">Trades ⇅</th>
      <th class="sortable" data-key="win_rate_pct">Trefferquote ⇅</th>
      <th class="sortable" data-key="total_pnl_usd">PnL $ ⇅</th>
      <th class="sortable" data-key="max_drawdown_usd">Max DD $ ⇅</th>
      <th class="sortable" data-key="avg_bars_held">Ø Kerzen gehalten ⇅</th>
    </tr></thead>
    <tbody></tbody>
  </table>
  <h3 style="margin-top:20px; font-size:14px; color:var(--text-dim); display:none;" id="es-sweep-worst-title">📉 Die 20 schlechtesten Werte (nach PnL, unabhängig von der Trade-Anzahl)</h3>
  <table id="es-sweep-worst-table" style="display:none; margin-top:8px;">
    <thead><tr>
      <th class="sortable" data-key="es_sensitivity">Sensitivity ⇅</th>
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

<div data-mode-section="pieki_algo" style="display:none;">
<h2 class="section-title">🎲 Pieki-Algo-Sensitivity-Sweep</h2>
<div class="panel-card">
  <div style="font-size:13px; color:var(--text-dim); margin-bottom:12px;">
    Testet nur die Sensitivity über einen Wertebereich - mit 2 Nachkommastellen wie im Original-
    Pine-Script (Schritt 0,01). Alle anderen Einstellungen (ATR-Periode, SMA-Periode, Exit-Modus,
    SL/TP, MTF-Filter) bleiben auf den aktuell gespeicherten Werten. Ergebnisse mit weniger als 5
    Trades sind statistisch kaum aussagekräftig und werden nach unten sortiert, aber nicht versteckt.
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Zeitraum (Tage)</label><input type="number" step="1" id="pk-sweep-days" value="30" style="width:90px;"></div>
    <div><label>Sensitivity von</label><input type="number" step="0.01" id="pk-sweep-sens-min" value="0.50" style="width:90px;"></div>
    <div><label>bis</label><input type="number" step="0.01" id="pk-sweep-sens-max" value="8.00" style="width:90px;"></div>
    <div><label>Schritt</label><input type="number" step="0.01" id="pk-sweep-sens-step" value="0.01" style="width:90px;"></div>
    <button id="btn-pk-sweep" style="padding:12px 24px;">🎲 Sweep starten</button>
  </div>
  <div id="pk-sweep-status" style="color:var(--text-dim); font-size:13px;"></div>
  <table id="pk-sweep-results-table" style="display:none; margin-top:12px;">
    <thead><tr>
      <th class="sortable" data-key="pk_sensitivity">Sensitivity ⇅</th>
      <th class="sortable" data-key="trades">Trades ⇅</th>
      <th class="sortable" data-key="win_rate_pct">Trefferquote ⇅</th>
      <th class="sortable" data-key="total_pnl_usd">PnL $ ⇅</th>
      <th class="sortable" data-key="max_drawdown_usd">Max DD $ ⇅</th>
      <th class="sortable" data-key="avg_bars_held">Ø Kerzen gehalten ⇅</th>
    </tr></thead>
    <tbody></tbody>
  </table>
  <h3 style="margin-top:20px; font-size:14px; color:var(--text-dim); display:none;" id="pk-sweep-worst-title">📉 Die 20 schlechtesten Werte (nach PnL, unabhängig von der Trade-Anzahl)</h3>
  <table id="pk-sweep-worst-table" style="display:none; margin-top:8px;">
    <thead><tr>
      <th class="sortable" data-key="pk_sensitivity">Sensitivity ⇅</th>
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

<div data-mode-section="mo7_scalp" style="display:none;">
<h2 class="section-title">🎲 MO7-Summenschwellen-Sweep ("5-Kerzen-Summe")</h2>
<div class="panel-card">
  <div style="font-size:13px; color:var(--text-dim); margin-bottom:12px;">
    Testet einen Bereich von Long-Summenschwelle (mo7_sum_low) gegen Short-Summenschwelle
    (mo7_sum_high) - nur relevant im Einstiegsmodus "5-Kerzen-Summe". Der MO7-Score selbst wird
    nur einmal berechnet und für alle Kombinationen wiederverwendet, deshalb ist der Sweep trotz
    vieler Kombinationen relativ schnell. Ergebnisse mit weniger als 5 Trades sind statistisch
    kaum aussagekräftig und werden nach unten sortiert, aber nicht versteckt.
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Zeitraum (Tage)</label><input type="number" step="1" id="mo7-sweep-days" value="30" style="width:90px;"></div>
    <div><label>Robustheits-Check: beste N ausschließen</label><input type="number" step="1" min="0" id="mo7-sweep-exclude-top-n" value="1" style="width:90px;"></div>
    <div><label>Long-Schwelle von</label><input type="number" step="1" id="mo7-sweep-sumlow-min" value="20" style="width:90px;"></div>
    <div><label>bis</label><input type="number" step="1" id="mo7-sweep-sumlow-max" value="200" style="width:90px;"></div>
    <div><label>Schritt</label><input type="number" step="1" id="mo7-sweep-sumlow-step" value="20" style="width:90px;"></div>
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Short-Schwelle von</label><input type="number" step="1" id="mo7-sweep-sumhigh-min" value="300" style="width:90px;"></div>
    <div><label>bis</label><input type="number" step="1" id="mo7-sweep-sumhigh-max" value="480" style="width:90px;"></div>
    <div><label>Schritt</label><input type="number" step="1" id="mo7-sweep-sumhigh-step" value="20" style="width:90px;"></div>
    <button id="btn-mo7-sweep" style="padding:12px 24px;">🎲 Sweep starten</button>
  </div>
  <div id="mo7-sweep-status" style="color:var(--text-dim); font-size:13px;"></div>
  <table id="mo7-sweep-results-table" style="display:none; margin-top:12px;">
    <thead><tr>
      <th class="sortable" data-key="mo7_sum_low">Long-Schwelle ⇅</th>
      <th class="sortable" data-key="mo7_sum_high">Short-Schwelle ⇅</th>
      <th class="sortable" data-key="trades">Trades ⇅</th>
      <th class="sortable" data-key="win_rate_pct">Trefferquote ⇅</th>
      <th class="sortable" data-key="total_pnl_usd">PnL $ ⇅</th>
      <th class="sortable" data-key="total_pnl_excl_top_n_usd">PnL ohne beste N $ ⇅</th>
      <th class="sortable" data-key="max_drawdown_usd">Max DD $ ⇅</th>
      <th class="sortable" data-key="avg_bars_held">Ø Kerzen gehalten ⇅</th>
    </tr></thead>
    <tbody></tbody>
  </table>
  <h3 style="margin-top:20px; font-size:14px; color:var(--text-dim); display:none;" id="mo7-sweep-worst-title">📉 Die 20 schlechtesten Werte (nach PnL, unabhängig von der Trade-Anzahl)</h3>
  <table id="mo7-sweep-worst-table" style="display:none; margin-top:8px;">
    <thead><tr>
      <th class="sortable" data-key="mo7_sum_low">Long-Schwelle ⇅</th>
      <th class="sortable" data-key="mo7_sum_high">Short-Schwelle ⇅</th>
      <th class="sortable" data-key="trades">Trades ⇅</th>
      <th class="sortable" data-key="win_rate_pct">Trefferquote ⇅</th>
      <th class="sortable" data-key="total_pnl_usd">PnL $ ⇅</th>
      <th class="sortable" data-key="total_pnl_excl_top_n_usd">PnL ohne beste N $ ⇅</th>
      <th class="sortable" data-key="max_drawdown_usd">Max DD $ ⇅</th>
      <th class="sortable" data-key="avg_bars_held">Ø Kerzen gehalten ⇅</th>
    </tr></thead>
    <tbody></tbody>
  </table>
</div>
</div>

<div data-mode-section="ut_bot_hull" style="display:none;">
<h2 class="section-title">🎲 UT-Bot+Hull ATR-Periode/Sensitivity-Sweep</h2>
<div class="panel-card">
  <div style="font-size:13px; color:var(--text-dim); margin-bottom:12px;">
    Testet einen Bereich von ATR-Periode und Sensitivity gegeneinander (die zwei Parameter, die im
    Original-Pine-Script beide irreführend "Period" heißen). Die Hull-MA wird nur einmal berechnet
    und für alle Kombinationen wiederverwendet.
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Zeitraum (Tage)</label><input type="number" step="1" id="utb-sweep-days" value="30" style="width:90px;"></div>
    <div><label>Robustheits-Check: beste N ausschließen</label><input type="number" step="1" min="0" id="utb-sweep-exclude-top-n" value="1" style="width:90px;"></div>
    <div><label>ATR-Periode von</label><input type="number" step="1" id="utb-sweep-atrp-min" value="1" style="width:90px;"></div>
    <div><label>bis</label><input type="number" step="1" id="utb-sweep-atrp-max" value="20" style="width:90px;"></div>
    <div><label>Schritt</label><input type="number" step="1" id="utb-sweep-atrp-step" value="1" style="width:90px;"></div>
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Sensitivity von</label><input type="number" step="0.01" id="utb-sweep-sens-min" value="0.5" style="width:90px;"></div>
    <div><label>bis</label><input type="number" step="0.01" id="utb-sweep-sens-max" value="5.0" style="width:90px;"></div>
    <div><label>Schritt</label><input type="number" step="0.01" id="utb-sweep-sens-step" value="0.5" style="width:90px;"></div>
  </div>
  <div style="font-size:12px; color:var(--text-dim); margin-bottom:6px;">
    Optional: MTF-Trend%-Schwellen mit sweepen (nur wirksam wenn "MTF-Trend%-Filter" oben auf "An" steht). Von=Bis lässt die Schwelle einfach fest wie eingestellt, keine zusätzlichen Kombinationen.
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Long-Schwelle von</label><input type="number" step="0.1" id="utb-sweep-long-min" value="0.5" style="width:90px;"></div>
    <div><label>bis</label><input type="number" step="0.1" id="utb-sweep-long-max" value="0.5" style="width:90px;"></div>
    <div><label>Schritt</label><input type="number" step="0.1" id="utb-sweep-long-step" value="0.5" style="width:90px;"></div>
    <div><label>Short-Schwelle von</label><input type="number" step="0.1" id="utb-sweep-short-min" value="-0.5" style="width:90px;"></div>
    <div><label>bis</label><input type="number" step="0.1" id="utb-sweep-short-max" value="-0.5" style="width:90px;"></div>
    <div><label>Schritt</label><input type="number" step="0.1" id="utb-sweep-short-step" value="0.5" style="width:90px;"></div>
    <button id="btn-utb-sweep" style="padding:12px 24px;">🎲 Sweep starten</button>
  </div>
  <div id="utb-sweep-status" style="color:var(--text-dim); font-size:13px;"></div>
  <table id="utb-sweep-results-table" style="display:none; margin-top:12px;">
    <thead><tr>
      <th class="sortable" data-key="utb_atr_period">ATR-Periode ⇅</th>
      <th class="sortable" data-key="utb_sensitivity">Sensitivity ⇅</th>
      <th class="sortable" data-key="utb_mtf_long_threshold">Long-Schwelle ⇅</th>
      <th class="sortable" data-key="utb_mtf_short_threshold">Short-Schwelle ⇅</th>
      <th class="sortable" data-key="trades">Trades ⇅</th>
      <th class="sortable" data-key="win_rate_pct">Trefferquote ⇅</th>
      <th class="sortable" data-key="total_pnl_usd">PnL $ ⇅</th>
      <th class="sortable" data-key="total_pnl_excl_top_n_usd">PnL ohne beste N $ ⇅</th>
      <th class="sortable" data-key="max_drawdown_usd">Max DD $ ⇅</th>
      <th class="sortable" data-key="avg_bars_held">Ø Kerzen gehalten ⇅</th>
    </tr></thead>
    <tbody></tbody>
  </table>
  <h3 style="margin-top:20px; font-size:14px; color:var(--text-dim); display:none;" id="utb-sweep-worst-title">📉 Die 20 schlechtesten Werte (nach PnL, unabhängig von der Trade-Anzahl)</h3>
  <table id="utb-sweep-worst-table" style="display:none; margin-top:8px;">
    <thead><tr>
      <th class="sortable" data-key="utb_atr_period">ATR-Periode ⇅</th>
      <th class="sortable" data-key="utb_sensitivity">Sensitivity ⇅</th>
      <th class="sortable" data-key="utb_mtf_long_threshold">Long-Schwelle ⇅</th>
      <th class="sortable" data-key="utb_mtf_short_threshold">Short-Schwelle ⇅</th>
      <th class="sortable" data-key="trades">Trades ⇅</th>
      <th class="sortable" data-key="win_rate_pct">Trefferquote ⇅</th>
      <th class="sortable" data-key="total_pnl_usd">PnL $ ⇅</th>
      <th class="sortable" data-key="total_pnl_excl_top_n_usd">PnL ohne beste N $ ⇅</th>
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
let quadStochChart;

// ===== Verschieb-/größenveränderbares Widget-Dashboard (wie bei Lighter) =====
// Jede Kachel behält ihre bestehende ID (oms-trend-meter, oms-gauge-wrap, ...) im Inneren -
// die ganze bisherige Render-Logik funktioniert dadurch unveraendert weiter, nur die AUSSENHUELLE
// ist jetzt per GridStack frei verschieb-/groessenveraenderbar. Layout wird pro Browser
// gespeichert (localStorage), nicht auf dem Server - jeder Nutzer kann sein eigenes Layout haben.
const OMS_WIDGET_DEFS = [
  { id: "gsi-signal", title: "📡 Signal", x: 0, y: 0, w: 4, h: 3,
    body: '<div id="oms-trend-meter" style="padding:16px; border-radius:10px; text-align:center; font-weight:800; font-size:20px;"></div><div id="oms-trend-meter-detail" style="margin-top:8px; font-size:11px; color:var(--text-dim); text-align:center;"></div>' },
  { id: "gsi-gauge", title: "📶 OBI-Gauge", x: 4, y: 0, w: 4, h: 3, body: '<div id="oms-gauge-wrap"></div>' },
  { id: "gsi-cvd-gauge", title: "💹 CVD-Gauge", x: 8, y: 0, w: 4, h: 3, body: '<div id="oms-cvd-gauge-wrap"></div>' },
  { id: "gsi-oi-gauge", title: "📊 Open-Interest-Gauge", x: 0, y: 3, w: 4, h: 3, body: '<div id="oms-oi-gauge-wrap"></div>' },
  { id: "gsi-liq-gauge", title: "💥 Liquidations-Gauge", x: 4, y: 3, w: 4, h: 3, body: '<div id="oms-liq-gauge-wrap"></div>' },
  { id: "gsi-checklist", title: "✅ Warum feuert's?", x: 8, y: 3, w: 4, h: 3, body: '<div id="oms-checklist-wrap"></div>' },
  { id: "gsi-pocket", title: "⚡ Pocket-Trading", x: 0, y: 6, w: 4, h: 5, body: `
    <div style="display:flex; gap:12px; flex-wrap:wrap; margin-bottom:10px; font-size:11px;">
      <div><div class="label">Margin</div><div class="value" id="pocket-margin" style="font-size:14px;">-</div></div>
      <div><div class="label">Position</div><div class="value" id="pocket-position" style="font-size:14px;">-</div></div>
      <div><div class="label">Ø-Einstieg</div><div class="value" id="pocket-entry" style="font-size:14px;">-</div></div>
      <div><div class="label">Unrealisiert $</div><div class="value" id="pocket-pnl" style="font-size:14px;">-</div></div>
    </div>
    <div style="display:flex; gap:8px; margin-bottom:12px;">
      <button id="btn-manual-buy" style="flex:1; padding:16px 6px; font-size:15px; font-weight:700; background:#16a34a; color:white; border:none; border-radius:10px; cursor:pointer;">⬆️ BUY</button>
      <button id="btn-manual-sell" style="flex:1; padding:16px 6px; font-size:15px; font-weight:700; background:#dc2626; color:white; border:none; border-radius:10px; cursor:pointer;">⬇️ SELL</button>
      <button id="btn-manual-tp" style="flex:1; padding:16px 6px; font-size:15px; font-weight:700; background:#2563eb; color:white; border:none; border-radius:10px; cursor:pointer;">✅ TP</button>
    </div>
    <div class="label" style="margin-bottom:4px; font-size:10px;">Letzte 10 Kerzen</div>
    <div id="mini-candles" style="display:flex; gap:3px; align-items:center; height:60px;"></div>` },
  { id: "gsi-chart", title: "📈 Preisverlauf", x: 4, y: 6, w: 8, h: 5, body: '<div id="oms-chart-wrap"></div>' },
  { id: "gsi-obi", title: "〰️ OBI-Verlauf", x: 0, y: 11, w: 12, h: 4, body: '<div style="position:relative; height:100%; min-height:180px;"><canvas id="obiChart"></canvas></div>' },
  { id: "gsi-scalp-board", title: "⚡ Scalp-Board (30s/45s/60s)", x: 0, y: 15, w: 12, h: 5, body: '<div id="scalp-board-wrap"></div>' },
  { id: "gsi-quad-stoch", title: "〰️ Quad-Stochastic-Verlauf", x: 0, y: 20, w: 12, h: 5, body: `
    <div style="display:flex; justify-content:flex-end; margin-bottom:6px;">
      <select id="quad-stoch-resolution-select" style="font-size:11px; padding:3px 8px; background:var(--panel); color:var(--text); border:1px solid var(--panel-border); border-radius:6px;">
        <option value="30s">30 Sekunden</option>
        <option value="1m">1 Minute</option>
        <option value="2m">2 Minuten</option>
        <option value="5m">5 Minuten</option>
      </select>
    </div>
    <div style="position:relative; height:calc(100% - 34px); min-height:160px;"><canvas id="quadStochChart"></canvas></div>` },
];

let omsGrid;
function initOmsGrid() {
  const container = document.getElementById('oms-grid');
  container.innerHTML = OMS_WIDGET_DEFS.map(w => `
    <div class="grid-stack-item" gs-id="${w.id}" gs-x="${w.x}" gs-y="${w.y}" gs-w="${w.w}" gs-h="${w.h}" id="${w.id}">
      <div class="grid-stack-item-content">
        <div class="widget-drag-handle">${w.title}</div>
        <div class="widget-body">${w.body}</div>
      </div>
    </div>`).join('');

  omsGrid = GridStack.init({ cellHeight: 46, margin: 6, float: true, handle: '.widget-drag-handle', animate: true }, container);

  // Gespeichertes Layout mit den AKTUELL bekannten Widgets zusammenfuehren: Kacheln, die der
  // Nutzer schon verschoben/skaliert hat, behalten seine Position; neu hinzugekommene Kacheln
  // (die im gespeicherten Layout noch gar nicht existierten) fallen auf ihre Default-Position
  // zurueck, statt komplett zu verschwinden - das war der Bug, der OI-/Liq-Gauge unsichtbar
  // gemacht hat, als sie zu einem bereits gespeicherten Layout hinzukamen.
  let saved = null;
  try { saved = JSON.parse(localStorage.getItem('oms_dashboard_layout') || 'null'); } catch (e) {}
  const savedById = {};
  if (saved && Array.isArray(saved)) {
    saved.forEach(item => { if (item && item.id) savedById[item.id] = item; });
  }
  const merged = OMS_WIDGET_DEFS.map(w => savedById[w.id]
    ? { id: w.id, x: savedById[w.id].x, y: savedById[w.id].y, w: savedById[w.id].w, h: savedById[w.id].h }
    : { id: w.id, x: w.x, y: w.y, w: w.w, h: w.h });
  omsGrid.load(merged);

  omsGrid.on('change', () => {
    try { localStorage.setItem('oms_dashboard_layout', JSON.stringify(omsGrid.save(false))); } catch (e) {}
  });

  document.getElementById('btn-reset-layout').addEventListener('click', () => {
    try { localStorage.removeItem('oms_dashboard_layout'); } catch (e) {}
    omsGrid.load(OMS_WIDGET_DEFS.map(w => ({ id: w.id, x: w.x, y: w.y, w: w.w, h: w.h })));
  });

  // Manuelle Buy/Sell/TP-Buttons neu verdrahten, da sie jetzt per innerHTML neu erzeugt wurden
  document.getElementById('btn-manual-buy').addEventListener('click', () => manualTrade('long'));
  document.getElementById('btn-manual-sell').addEventListener('click', () => manualTrade('short'));
  document.getElementById('btn-manual-tp').addEventListener('click', async () => {
    const res = await fetch(`/api/close?symbol=${currentSymbol}`, { method: 'POST' });
    const data = await res.json();
    if (data.error) alert(data.error);
    refresh();
  });

  // Quad-Stochastic Zeitrahmen-Dropdown: schreibt direkt (Partial-Update, kein ganzes
  // Formular noetig) ins Config-Backend und laedt die Anzeige neu
  document.getElementById('quad-stoch-resolution-select').addEventListener('change', async (e) => {
    await fetch(`/api/config?symbol=${currentSymbol}`, {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ quad_stoch_resolution: e.target.value })
    });
    refresh();
  });
}
initOmsGrid();
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
  if (isOms) updateOmsExitModeFields();
}

function updateOmsExitModeFields() {
  const exitMode = document.getElementById('oms_exit_mode').value;
  document.querySelectorAll('[data-oms-exit-mode]').forEach(el => {
    el.style.display = (el.dataset.omsExitMode === exitMode) ? '' : 'none';
  });
  document.getElementById('oms_tp1_usd_label').innerText = exitMode === 'single_tp' ? 'TP Ziel ($)' : 'TP1 Ziel ($, Teilverkauf)';
}
document.getElementById('entry_mode').addEventListener('change', () => {
  window.formTouched = true;
  updateModeFields();
});
document.getElementById('obi-advanced-toggle').addEventListener('change', (e) => {
  document.getElementById('obi-advanced-fields').style.display = e.target.checked ? 'grid' : 'none';
});
document.getElementById('oms_exit_mode').addEventListener('change', () => {
  window.formTouched = true;
  updateOmsExitModeFields();
});

async function loadSymbols() {
  const res = await fetch('/api/symbols');
  const data = await res.json();
  allSymbols = data.symbols;
  const sel = document.getElementById('symbol-select');
  sel.innerHTML = allSymbols.map(s => `<option value="${s}">${s}</option>`).join('');
  currentSymbol = allSymbols[0];
  sel.value = currentSymbol;
  sel.addEventListener('change', () => {
    if (window.formTouched && !confirm(`Ungespeicherte Änderungen für ${currentSymbol} gehen verloren, wenn du jetzt wechselst. Trotzdem wechseln (ohne zu speichern)?`)) {
      sel.value = currentSymbol;  // Auswahl zurücksetzen, Wechsel abgebrochen
      return;
    }
    currentSymbol = sel.value;
    window.formTouched = false;
    resetBacktestUI();
    refresh();
  });
}

document.getElementById('btn-start').addEventListener('click', async () => {
  // Erst die aktuellen Formular-Einstellungen speichern (Backtest speichert NICHT dauerhaft,
  // nur bot_active zu setzen wuerde sonst mit der zuletzt GESPEICHERTEN Config starten statt
  // mit dem, was gerade im Formular steht - genau das fuehrte zu "startet mit alter Strategie").
  try {
    const cfgRes = await fetch(`/api/config?symbol=${currentSymbol}`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(buildConfigPayload()) });
    const cfgData = await cfgRes.json().catch(() => null);
    if (!cfgRes.ok || !cfgData || cfgData.success !== true) {
      showToast(`❌ Speichern fehlgeschlagen (${cfgRes.status}): ${cfgData?.error || 'unbekannter Fehler'} - Bot NICHT gestartet.`);
      return;
    }
    window.formTouched = false;
    const ctrlRes = await fetch(`/api/control?symbol=${currentSymbol}`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({bot_active:true}) });
    if (!ctrlRes.ok) {
      showToast(`❌ Gespeichert, aber Start fehlgeschlagen (${ctrlRes.status}).`);
      return;
    }
    showToast(`✅ Gespeichert & gestartet für ${currentSymbol} (${cfgData.config.entry_mode})!`);
  } catch (e) {
    showToast(`❌ Netzwerkfehler beim Speichern/Starten: ${e}`);
  }
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
// btn-manual-buy/sell/tp werden jetzt in initOmsGrid() verdrahtet, da diese Buttons dort
// dynamisch per innerHTML erzeugt werden (Teil der verschiebbaren Pocket-Trading-Kachel)

document.getElementById('btn-backtest').addEventListener('click', async () => {
  const days = parseInt(document.getElementById('backtest-days').value) || 30;
  const excludeTopN = parseInt(document.getElementById('backtest-exclude-top-n').value) || 0;
  const btn = document.getElementById('btn-backtest');
  const statusEl = document.getElementById('backtest-status');
  const resultsEl = document.getElementById('backtest-results');
  const btSymbol = currentSymbol;
  btn.disabled = true;
  resultsEl.style.display = 'none';
  statusEl.innerText = `⏳ Lade Kerzen von Binance und simuliere... kann bei langen Zeiträumen 1-2 Minuten dauern.`;
  try {
    const res = await fetch(`/api/backtest?symbol=${btSymbol}`, {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({days, exclude_top_n: excludeTopN, config: buildConfigPayload()})
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
      document.getElementById('bt-fills').innerText = data.stats.fills ?? data.stats.trades;
      document.getElementById('bt-winrate').innerText = data.stats.win_rate_pct + '%';
      const pnlEl = document.getElementById('bt-pnl');
      pnlEl.innerText = data.stats.total_pnl_usd;
      pnlEl.className = data.stats.total_pnl_usd >= 0 ? 'value green' : 'value red';
      document.getElementById('bt-dd').innerText = data.stats.max_drawdown_usd;
      document.getElementById('bt-avg').innerText = `${data.stats.avg_win_usd} / ${data.stats.avg_loss_usd}`;
      document.getElementById('bt-best-trade').innerText = data.stats.best_trade_pnl_usd;
      const pnlExclEl = document.getElementById('bt-pnl-excl-best');
      pnlExclEl.innerText = `${data.stats.total_pnl_excl_top_n_usd} (ohne ${data.stats.top_n_excluded_count} Trade${data.stats.top_n_excluded_count === 1 ? '' : 's'})`;
      pnlExclEl.className = data.stats.total_pnl_excl_top_n_usd >= 0 ? 'value green' : 'value red';
      document.getElementById('bt-median-trade').innerText = data.stats.median_trade_pnl_usd;
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
// Stabile Gruppen-Farbe je Trade (entry_ts) - haengt NICHT von der Zeilen-Reihenfolge ab,
// bleibt also auch nach Sortieren nach einer anderen Spalte konsistent zugeordnet
const BT_GROUP_COLORS = ['#60a5fa', '#f472b6', '#34d399', '#fbbf24', '#a78bfa', '#fb923c', '#22d3ee', '#f87171'];
// Gruppen-Farbe nach ERSCHEINUNGSREIHENFOLGE in der aktuell angezeigten Sortierung vergeben,
// nicht per Hash - Hash-Kollisionen liessen bei vielen Trades zu haeufig benachbarte, aber
// UNTERSCHIEDLICHE Positionen dieselbe Farbe bekommen (sah aus wie ein einziger großer Trade).
// Mit Reihenfolge-Vergabe bekommt garantiert jede neue Gruppe eine andere Farbe als die direkt
// vorherige, unabhaengig davon, wonach gerade sortiert ist.
let btColorMap = {};
function computeBtColorMap(rows) {
  const map = {};
  let idx = 0;
  for (const r of rows) {
    const key = String(r.entry_ts);
    if (!(key in map)) {
      map[key] = BT_GROUP_COLORS[idx % BT_GROUP_COLORS.length];
      idx++;
    }
  }
  return map;
}
const renderBtTrades = makeSortableTable('bt-trades-table', () => window.btTradesData, (r, i, allRows) => {
  if (i === 0) btColorMap = computeBtColorMap(allRows);
  const groupColor = btColorMap[String(r.entry_ts)];
  const pnlClass = r.pnl > 0 ? 'green' : r.pnl < 0 ? 'red' : '';
  return `
  <tr style="border-left: 4px solid ${groupColor};">
    <td>${fmtTs(r.entry_ts)}</td>
    <td>${r.dir === 'long' ? '🟢 Long' : '🔴 Short'}</td>
    <td>${r.entry}</td>
    <td>${fmtTs(r.exit_ts)}</td>
    <td>${r.exit}</td>
    <td>${r.reason}</td>
    <td class="${pnlClass}">${r.pnl.toFixed(2)}</td>
  </tr>`;
});

// Setzt einen Zeitrahmen-Dropdown (da_/es_/ht_resolution) korrekt, auch wenn der gespeicherte
// Wert eine EIGENE Minutenzahl ist (z.B. "8m"), die keine feste <option> im Dropdown hat -
// dann wird "custom" ausgewaehlt und das Zahlenfeld daneben eingeblendet/befuellt.
function setResolutionField(fieldId, value) {
  const select = document.getElementById(fieldId);
  const customInput = document.getElementById(fieldId + '_custom_minutes');
  const hasOption = Array.from(select.options).some(o => o.value === value);
  if (hasOption) {
    select.value = value;
    customInput.style.display = 'none';
  } else {
    const m = /^(\d+)m$/.exec(value || '');
    select.value = 'custom';
    customInput.style.display = '';
    customInput.value = m ? m[1] : '';
  }
}
function getResolutionField(fieldId) {
  const select = document.getElementById(fieldId);
  if (select.value === 'custom') {
    const n = document.getElementById(fieldId + '_custom_minutes').value;
    return (n && parseInt(n) > 0) ? `${parseInt(n)}m` : '1m';
  }
  return select.value;
}
document.querySelectorAll('#da_resolution, #es_resolution, #ht_resolution, #cp_resolution, #utb_resolution, #wtc_resolution, #pk_resolution, #pk_mtf_tf1, #pk_mtf_tf2, #pk_mtf_tf3, #utb_mtf_tf1, #utb_mtf_tf2, #utb_mtf_tf3, #fr_resolution, #cd_resolution, #fr_zscore_resolution, #cd_zscore_resolution').forEach(sel => {
  sel.addEventListener('change', () => {
    const customInput = document.getElementById(sel.id + '_custom_minutes');
    customInput.style.display = sel.value === 'custom' ? '' : 'none';
  });
});

function resetBacktestUI() {
  document.getElementById('backtest-results').style.display = 'none';
  document.getElementById('backtest-status').innerText = '';
  window.btTradesData = [];
  document.getElementById('ht-sweep-status').innerText = '';
  document.getElementById('ht-sweep-results-table').style.display = 'none';
  document.getElementById('ht-sweep-worst-table').style.display = 'none';
  document.getElementById('ht-sweep-worst-title').style.display = 'none';
  window.htSweepResultsData = [];
  window.htSweepWorstData = [];
  document.getElementById('da-sweep-status').innerText = '';
  document.getElementById('da-sweep-results-table').style.display = 'none';
  document.getElementById('da-sweep-worst-table').style.display = 'none';
  document.getElementById('da-sweep-worst-title').style.display = 'none';
  window.daSweepResultsData = [];
  window.daSweepWorstData = [];
  document.getElementById('es-sweep-status').innerText = '';
  document.getElementById('es-sweep-results-table').style.display = 'none';
  document.getElementById('es-sweep-worst-table').style.display = 'none';
  document.getElementById('es-sweep-worst-title').style.display = 'none';
  window.esSweepResultsData = [];
  window.esSweepWorstData = [];
  document.getElementById('pk-sweep-status').innerText = '';
  document.getElementById('pk-sweep-results-table').style.display = 'none';
  document.getElementById('pk-sweep-worst-table').style.display = 'none';
  document.getElementById('pk-sweep-worst-title').style.display = 'none';
  window.pkSweepResultsData = [];
  window.pkSweepWorstData = [];
  document.getElementById('mo7-sweep-status').innerText = '';
  document.getElementById('mo7-sweep-results-table').style.display = 'none';
  document.getElementById('mo7-sweep-worst-table').style.display = 'none';
  document.getElementById('mo7-sweep-worst-title').style.display = 'none';
  window.mo7SweepResultsData = [];
  window.mo7SweepWorstData = [];
  document.getElementById('utb-sweep-status').innerText = '';
  document.getElementById('utb-sweep-results-table').style.display = 'none';
  document.getElementById('utb-sweep-worst-table').style.display = 'none';
  document.getElementById('utb-sweep-worst-title').style.display = 'none';
  window.utbSweepResultsData = [];
  window.utbSweepWorstData = [];
}

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

document.getElementById('btn-da-sweep').addEventListener('click', async () => {
  const btn = document.getElementById('btn-da-sweep');
  const statusEl = document.getElementById('da-sweep-status');
  const tableEl = document.getElementById('da-sweep-results-table');
  const worstTableEl = document.getElementById('da-sweep-worst-table');
  const worstTitleEl = document.getElementById('da-sweep-worst-title');
  const sweepSymbol = currentSymbol;
  const payload = {
    days: parseInt(document.getElementById('da-sweep-days').value) || 30,
    atr_period_min: parseInt(document.getElementById('da-sweep-period-min').value),
    atr_period_max: parseInt(document.getElementById('da-sweep-period-max').value),
    atr_period_step: parseInt(document.getElementById('da-sweep-period-step').value),
    sensitivity_min: parseFloat(document.getElementById('da-sweep-sens-min').value),
    sensitivity_max: parseFloat(document.getElementById('da-sweep-sens-max').value),
    sensitivity_step: parseFloat(document.getElementById('da-sweep-sens-step').value),
    config: buildConfigPayload(),
  };
  btn.disabled = true;
  tableEl.style.display = 'none';
  worstTableEl.style.display = 'none';
  worstTitleEl.style.display = 'none';
  statusEl.innerText = `⏳ Lade Kerzen und teste alle Kombinationen... kann bei vielen Kombinationen etwas dauern.`;
  try {
    const res = await fetch(`/api/da_sweep?symbol=${sweepSymbol}`, {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)
    });
    const data = await res.json();
    if (sweepSymbol !== currentSymbol) return;
    if (data.error) {
      statusEl.innerText = `❌ ${data.error}`;
    } else {
      statusEl.innerText = `${data.combos_tested} Kombinationen getestet auf ${data.candles_processed} Kerzen (${data.actual_days_covered} Tage, ${data.resolution}) - Ergebnisse mit weniger als ${data.min_reliable_trades} Trades sind unten einsortiert.`;
      window.daSweepResultsData = data.results || [];
      window.daSweepWorstData = data.worst_results || [];
      renderDaSweepResults();
      renderDaSweepWorst();
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

window.daSweepResultsData = [];
window.daSweepWorstData = [];
const daSweepRowHtml = (r) => `
  <tr>
    <td>${r.da_atr_period}</td>
    <td>${r.da_sensitivity}</td>
    <td>${r.trades}</td>
    <td>${r.win_rate_pct}%</td>
    <td class="${r.total_pnl_usd >= 0 ? 'green' : 'red'}">${r.total_pnl_usd}</td>
    <td>${r.max_drawdown_usd}</td>
    <td>${r.avg_bars_held}</td>
  </tr>`;
const renderDaSweepResults = makeSortableTable('da-sweep-results-table', () => window.daSweepResultsData, daSweepRowHtml);
const renderDaSweepWorst = makeSortableTable('da-sweep-worst-table', () => window.daSweepWorstData, daSweepRowHtml);

document.getElementById('btn-es-sweep').addEventListener('click', async () => {
  const btn = document.getElementById('btn-es-sweep');
  const statusEl = document.getElementById('es-sweep-status');
  const tableEl = document.getElementById('es-sweep-results-table');
  const worstTableEl = document.getElementById('es-sweep-worst-table');
  const worstTitleEl = document.getElementById('es-sweep-worst-title');
  const sweepSymbol = currentSymbol;
  const payload = {
    days: parseInt(document.getElementById('es-sweep-days').value) || 30,
    sens_min: parseFloat(document.getElementById('es-sweep-sens-min').value),
    sens_max: parseFloat(document.getElementById('es-sweep-sens-max').value),
    sens_step: parseFloat(document.getElementById('es-sweep-sens-step').value),
    config: buildConfigPayload(),
  };
  btn.disabled = true;
  tableEl.style.display = 'none';
  worstTableEl.style.display = 'none';
  worstTitleEl.style.display = 'none';
  statusEl.innerText = `⏳ Lade Kerzen und teste alle Sensitivity-Werte... kann bei vielen Werten etwas dauern.`;
  try {
    const res = await fetch(`/api/es_sensitivity_sweep?symbol=${sweepSymbol}`, {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)
    });
    const data = await res.json();
    if (sweepSymbol !== currentSymbol) return;
    if (data.error) {
      statusEl.innerText = `❌ ${data.error}`;
    } else {
      statusEl.innerText = `${data.combos_tested} Sensitivity-Werte getestet auf ${data.candles_processed} Kerzen (${data.actual_days_covered} Tage, ${data.resolution}) - Ergebnisse mit weniger als ${data.min_reliable_trades} Trades sind unten einsortiert.`;
      window.esSweepResultsData = data.results || [];
      window.esSweepWorstData = data.worst_results || [];
      renderEsSweepResults();
      renderEsSweepWorst();
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

window.esSweepResultsData = [];
window.esSweepWorstData = [];
const esSweepRowHtml = (r) => `
  <tr>
    <td>${r.es_sensitivity.toFixed(2)}</td>
    <td>${r.trades}</td>
    <td>${r.win_rate_pct}%</td>
    <td class="${r.total_pnl_usd >= 0 ? 'green' : 'red'}">${r.total_pnl_usd}</td>
    <td>${r.max_drawdown_usd}</td>
    <td>${r.avg_bars_held}</td>
  </tr>`;
const renderEsSweepResults = makeSortableTable('es-sweep-results-table', () => window.esSweepResultsData, esSweepRowHtml);
const renderEsSweepWorst = makeSortableTable('es-sweep-worst-table', () => window.esSweepWorstData, esSweepRowHtml);

document.getElementById('btn-pk-sweep').addEventListener('click', async () => {
  const btn = document.getElementById('btn-pk-sweep');
  const statusEl = document.getElementById('pk-sweep-status');
  const tableEl = document.getElementById('pk-sweep-results-table');
  const worstTableEl = document.getElementById('pk-sweep-worst-table');
  const worstTitleEl = document.getElementById('pk-sweep-worst-title');
  const sweepSymbol = currentSymbol;
  const payload = {
    days: parseInt(document.getElementById('pk-sweep-days').value) || 30,
    sens_min: parseFloat(document.getElementById('pk-sweep-sens-min').value),
    sens_max: parseFloat(document.getElementById('pk-sweep-sens-max').value),
    sens_step: parseFloat(document.getElementById('pk-sweep-sens-step').value),
    config: buildConfigPayload(),
  };
  btn.disabled = true;
  tableEl.style.display = 'none';
  worstTableEl.style.display = 'none';
  worstTitleEl.style.display = 'none';
  statusEl.innerText = `⏳ Lade Kerzen und teste alle Sensitivity-Werte... kann bei vielen Werten etwas dauern.`;
  try {
    const res = await fetch(`/api/pk_sensitivity_sweep?symbol=${sweepSymbol}`, {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)
    });
    const data = await res.json();
    if (sweepSymbol !== currentSymbol) return;
    if (data.error) {
      statusEl.innerText = `❌ ${data.error}`;
    } else {
      statusEl.innerText = `${data.combos_tested} Sensitivity-Werte getestet auf ${data.candles_processed} Kerzen (${data.actual_days_covered} Tage, ${data.resolution}) - Ergebnisse mit weniger als ${data.min_reliable_trades} Trades sind unten einsortiert.`;
      window.pkSweepResultsData = data.results || [];
      window.pkSweepWorstData = data.worst_results || [];
      renderPkSweepResults();
      renderPkSweepWorst();
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

window.pkSweepResultsData = [];
window.pkSweepWorstData = [];
const pkSweepRowHtml = (r) => `
  <tr>
    <td>${r.pk_sensitivity.toFixed(2)}</td>
    <td>${r.trades}</td>
    <td>${r.win_rate_pct}%</td>
    <td class="${r.total_pnl_usd >= 0 ? 'green' : 'red'}">${r.total_pnl_usd}</td>
    <td>${r.max_drawdown_usd}</td>
    <td>${r.avg_bars_held}</td>
  </tr>`;
const renderPkSweepResults = makeSortableTable('pk-sweep-results-table', () => window.pkSweepResultsData, pkSweepRowHtml);
const renderPkSweepWorst = makeSortableTable('pk-sweep-worst-table', () => window.pkSweepWorstData, pkSweepRowHtml);

document.getElementById('btn-mo7-sweep').addEventListener('click', async () => {
  const btn = document.getElementById('btn-mo7-sweep');
  const statusEl = document.getElementById('mo7-sweep-status');
  const tableEl = document.getElementById('mo7-sweep-results-table');
  const worstTableEl = document.getElementById('mo7-sweep-worst-table');
  const worstTitleEl = document.getElementById('mo7-sweep-worst-title');
  const sweepSymbol = currentSymbol;
  const payload = {
    days: parseInt(document.getElementById('mo7-sweep-days').value) || 30,
    sum_low_min: parseFloat(document.getElementById('mo7-sweep-sumlow-min').value),
    sum_low_max: parseFloat(document.getElementById('mo7-sweep-sumlow-max').value),
    sum_low_step: parseFloat(document.getElementById('mo7-sweep-sumlow-step').value),
    sum_high_min: parseFloat(document.getElementById('mo7-sweep-sumhigh-min').value),
    sum_high_max: parseFloat(document.getElementById('mo7-sweep-sumhigh-max').value),
    sum_high_step: parseFloat(document.getElementById('mo7-sweep-sumhigh-step').value),
    exclude_top_n: parseInt(document.getElementById('mo7-sweep-exclude-top-n').value) || 0,
    config: buildConfigPayload(),
  };
  btn.disabled = true;
  tableEl.style.display = 'none';
  worstTableEl.style.display = 'none';
  worstTitleEl.style.display = 'none';
  statusEl.innerText = `⏳ Lade Kerzen und teste alle Schwellen-Kombinationen... kann bei vielen Werten etwas dauern.`;
  try {
    const res = await fetch(`/api/mo7_sum_sweep?symbol=${sweepSymbol}`, {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)
    });
    const data = await res.json();
    if (sweepSymbol !== currentSymbol) return;
    if (data.error) {
      statusEl.innerText = `❌ ${data.error}`;
    } else {
      statusEl.innerText = `${data.combos_tested} Kombinationen getestet auf ${data.candles_processed} Kerzen (${data.actual_days_covered} Tage, ${data.resolution}) - Ergebnisse mit weniger als ${data.min_reliable_trades} Trades sind unten einsortiert.`;
      window.mo7SweepResultsData = data.results || [];
      window.mo7SweepWorstData = data.worst_results || [];
      renderMo7SweepResults();
      renderMo7SweepWorst();
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

window.mo7SweepResultsData = [];
window.mo7SweepWorstData = [];
const mo7SweepRowHtml = (r) => `
  <tr>
    <td>${r.mo7_sum_low}</td>
    <td>${r.mo7_sum_high}</td>
    <td>${r.trades}</td>
    <td>${r.win_rate_pct}%</td>
    <td class="${r.total_pnl_usd >= 0 ? 'green' : 'red'}">${r.total_pnl_usd}</td>
    <td class="${r.total_pnl_excl_top_n_usd >= 0 ? 'green' : 'red'}">${r.total_pnl_excl_top_n_usd}</td>
    <td>${r.max_drawdown_usd}</td>
    <td>${r.avg_bars_held}</td>
  </tr>`;
const renderMo7SweepResults = makeSortableTable('mo7-sweep-results-table', () => window.mo7SweepResultsData, mo7SweepRowHtml);
const renderMo7SweepWorst = makeSortableTable('mo7-sweep-worst-table', () => window.mo7SweepWorstData, mo7SweepRowHtml);

document.getElementById('btn-utb-sweep').addEventListener('click', async () => {
  const btn = document.getElementById('btn-utb-sweep');
  const statusEl = document.getElementById('utb-sweep-status');
  const tableEl = document.getElementById('utb-sweep-results-table');
  const worstTableEl = document.getElementById('utb-sweep-worst-table');
  const worstTitleEl = document.getElementById('utb-sweep-worst-title');
  const sweepSymbol = currentSymbol;
  const payload = {
    days: parseInt(document.getElementById('utb-sweep-days').value) || 30,
    atr_period_min: parseInt(document.getElementById('utb-sweep-atrp-min').value),
    atr_period_max: parseInt(document.getElementById('utb-sweep-atrp-max').value),
    atr_period_step: parseInt(document.getElementById('utb-sweep-atrp-step').value),
    sensitivity_min: parseFloat(document.getElementById('utb-sweep-sens-min').value),
    sensitivity_max: parseFloat(document.getElementById('utb-sweep-sens-max').value),
    sensitivity_step: parseFloat(document.getElementById('utb-sweep-sens-step').value),
    long_threshold_min: parseFloat(document.getElementById('utb-sweep-long-min').value),
    long_threshold_max: parseFloat(document.getElementById('utb-sweep-long-max').value),
    long_threshold_step: parseFloat(document.getElementById('utb-sweep-long-step').value),
    short_threshold_min: parseFloat(document.getElementById('utb-sweep-short-min').value),
    short_threshold_max: parseFloat(document.getElementById('utb-sweep-short-max').value),
    short_threshold_step: parseFloat(document.getElementById('utb-sweep-short-step').value),
    exclude_top_n: parseInt(document.getElementById('utb-sweep-exclude-top-n').value) || 0,
    config: buildConfigPayload(),
  };
  btn.disabled = true;
  tableEl.style.display = 'none';
  worstTableEl.style.display = 'none';
  worstTitleEl.style.display = 'none';
  statusEl.innerText = `⏳ Lade Kerzen und teste alle Kombinationen... kann bei vielen Werten etwas dauern.`;
  try {
    const res = await fetch(`/api/utb_param_sweep?symbol=${sweepSymbol}`, {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)
    });
    const data = await res.json();
    if (sweepSymbol !== currentSymbol) return;
    if (data.error) {
      statusEl.innerText = `❌ ${data.error}`;
    } else {
      statusEl.innerText = `${data.combos_tested} Kombinationen getestet auf ${data.candles_processed} Kerzen (${data.actual_days_covered} Tage, ${data.resolution}) - Ergebnisse mit weniger als ${data.min_reliable_trades} Trades sind unten einsortiert.`;
      window.utbSweepResultsData = data.results || [];
      window.utbSweepWorstData = data.worst_results || [];
      renderUtbSweepResults();
      renderUtbSweepWorst();
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

window.utbSweepResultsData = [];
window.utbSweepWorstData = [];
const utbSweepRowHtml = (r) => `
  <tr>
    <td>${r.utb_atr_period}</td>
    <td>${r.utb_sensitivity}</td>
    <td>${r.utb_mtf_long_threshold}</td>
    <td>${r.utb_mtf_short_threshold}</td>
    <td>${r.trades}</td>
    <td>${r.win_rate_pct}%</td>
    <td class="${r.total_pnl_usd >= 0 ? 'green' : 'red'}">${r.total_pnl_usd}</td>
    <td class="${r.total_pnl_excl_top_n_usd >= 0 ? 'green' : 'red'}">${r.total_pnl_excl_top_n_usd}</td>
    <td>${r.max_drawdown_usd}</td>
    <td>${r.avg_bars_held}</td>
  </tr>`;
const renderUtbSweepResults = makeSortableTable('utb-sweep-results-table', () => window.utbSweepResultsData, utbSweepRowHtml);
const renderUtbSweepWorst = makeSortableTable('utb-sweep-worst-table', () => window.utbSweepWorstData, utbSweepRowHtml);





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

function _omsGaugeStageLabel(v, t) {
  if (v == null) return 'Keine Daten';
  if (v >= t) return 'STARK LONG';
  if (v >= t / 2) return 'Vor-Long';
  if (v > -t / 2) return 'Neutral';
  if (v > -t) return 'Vor-Short';
  return 'STARK SHORT';
}

function renderOmsSimpleGauge(value, threshold, title, explanation) {
  const t = threshold ?? 0.15;
  const clamp = v => Math.max(-1, Math.min(1, v ?? 0));
  const pctOf = v => (clamp(v) + 1) / 2 * 100;
  const zoneStop1 = ((1 - t) / 2 * 100).toFixed(0);
  const zoneStop2 = (50 + t / 2 * 50).toFixed(0);
  return `<div class="panel-card" style="padding:14px;">
    <div style="font-size:12px; color:var(--text-dim); margin-bottom:8px;">${title} — Stufe: <b style="color:var(--text);">${_omsGaugeStageLabel(value, t)}</b></div>
    <div style="position:relative; height:28px; border-radius:6px; background:linear-gradient(90deg, #f0526b 0%, #7c3f47 ${zoneStop1}%, #3a3f52 48%, #3a3f52 52%, #2f6b45 ${zoneStop2}%, #22c55e 100%);">
      <div style="position:absolute; top:-4px; left:${pctOf(value)}%; width:2px; height:36px; background:#fff; transform:translateX(-1px);"></div>
    </div>
    <div style="display:flex; justify-content:space-between; font-size:10px; color:var(--text-dim); margin-top:4px;">
      <span>Stark Short</span><span>Neutral</span><span>Stark Long</span>
    </div>
    <div style="font-size:11px; color:var(--text-dim); margin-top:6px;">${explanation}</div>
  </div>`;
}

function renderOmsCvdGauge(cvdRatio, minRatio) {
  return renderOmsSimpleGauge(cvdRatio, minRatio ?? 0.15, 'Cumulative Volume Delta (CVD)',
    'Zeigt, wer gerade aktiv (aggressiv) kauft/verkauft - anders als OBI, das nur zeigt, wer im Orderbuch bereitsteht. Muss "Vor-Long"/"Stark Long" (bzw. Short) erreichen, um ein OBI-Signal zu bestätigen.');
}

function renderOmsOiGauge(oiScore, minScore) {
  return renderOmsSimpleGauge(oiScore, minScore ?? 0.3, 'Open Interest (Preis + OI kombiniert)',
    'Stark Long/Short = Preis UND offene Positionen laufen in dieselbe Richtung (neues Geld, echte Überzeugung). Vor-Long/Vor-Short = nur Eindeckung/Kapitulation der Gegenseite (schwächer, kann schnell drehen). Neutral = OI ändert sich kaum.');
}

function renderOmsLiqGauge(liqRatio, minRatio, liqCount) {
  const note = (liqCount ?? 0) === 0 ? '<br><i>Keine Liquidationen im aktuellen Zeitfenster.</i>' : '';
  return renderOmsSimpleGauge(liqRatio, minRatio ?? 0.2, 'Liquidationen (Zwangs-Events)',
    'Forcierte Short-Liquidation (Zwangskauf) = bullischer Druck, forcierte Long-Liquidation (Zwangsverkauf) = bärischer Druck. Anders als CVD sind das keine freiwilligen Trades, sondern echte Zwangsereignisse - oft Vorbote kurzer, heftiger Gegenbewegungen (Squeeze).' + note);
}

function renderScalpBoard(board) {
  const tfs = [['10s','10s'], ['30s','30s'], ['45s','45s'], ['60s','60s']];
  if (!board || tfs.every(([k]) => !board[k])) {
    return '<div style="padding:14px; color:var(--text-dim); font-size:13px;">Sammelt noch Daten... (60 Sek. sollte binnen weniger Sekunden erscheinen, egal ob der Bot aktiv ist - 10/30/45 Sek. brauchen zusätzlich den Bot einmal im aktiven Zustand, damit der Sekunden-Kerzen-Puffer gefüllt wird)</div>';
  }
  const rsiCell = v => {
    if (v == null) return '<td>-</td>';
    const color = v >= 70 ? 'red' : v <= 30 ? 'green' : '';
    return `<td class="${color}">${v}</td>`;
  };
  const stochCell = tf => {
    if (!tf) return '<td>-</td>';
    const color = tf.stoch_k >= 80 ? 'red' : tf.stoch_k <= 20 ? 'green' : '';
    return `<td class="${color}">${tf.stoch_k} / ${tf.stoch_d}</td>`;
  };
  const macdCell = v => {
    if (v == null) return '<td>-</td>';
    return `<td class="${v >= 0 ? 'green' : 'red'}">${v >= 0 ? '▲' : '▼'} ${v}</td>`;
  };
  const cvdCell = v => {
    if (v == null) return '<td>-</td>';
    const color = v >= 0.15 ? 'green' : v <= -0.15 ? 'red' : '';
    return `<td class="${color}">${v}</td>`;
  };
  const mo7Cell = v => {
    if (v == null) return '<td>-</td>';
    const color = v <= 20 ? 'green' : v >= 80 ? 'red' : '';
    return `<td class="${color}">${v}</td>`;
  };
  const obiCell = v => {
    if (v == null) return '<td>-</td>';
    const color = v >= 0.15 ? 'green' : v <= -0.15 ? 'red' : '';
    return `<td class="${color}">${v}</td>`;
  };
  const row = (label, cells) => `<tr><td style="color:var(--text-dim); text-align:left;">${label}</td>${cells}</tr>`;
  const obi = board.obi || {};
  return `<div style="padding:4px 10px;">
    <div style="font-size:11px; color:var(--text-dim); margin-bottom:8px;">Rein manuell zur Entscheidungshilfe - RSI(8) rot ≥70/grün ≤30 · Stochastic(5,3,3) K/D rot ≥80/grün ≤20 · MACD-Histogramm(5,13,3) grün=positiv · MO7 (ohne Volumen-Anteil) grün ≤20/rot ≥80 · CVD/OBI grün/rot ab ±0.15</div>
    <table style="width:100%; text-align:center;">
      <thead><tr><th style="text-align:left;"></th><th>10 Sek.</th><th>30 Sek.</th><th>45 Sek.</th><th>60 Sek.</th></tr></thead>
      <tbody>
        ${row('RSI(8)', tfs.map(([k]) => rsiCell(board[k]?.rsi)).join(''))}
        ${row('Stochastic %K/%D', tfs.map(([k]) => stochCell(board[k])).join(''))}
        ${row('MACD-Hist', tfs.map(([k]) => macdCell(board[k]?.macd_hist)).join(''))}
        ${row('MO7', tfs.map(([k]) => mo7Cell(board[k]?.mo7)).join(''))}
        ${row('CVD', tfs.map(([k]) => cvdCell(board[k]?.cvd)).join(''))}
      </tbody>
    </table>
    <table style="width:100%; text-align:center; margin-top:10px;">
      <thead><tr><th style="text-align:left;"></th><th>OBI schnell</th><th>OBI mittel</th><th>OBI langsam</th></tr></thead>
      <tbody>${row('Orderbuch', [obiCell(obi.fast), obiCell(obi.medium), obiCell(obi.slow)].join(''))}</tbody>
    </table>
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
  const rsiDetail = data.config.oms_rsi_filter_enabled ? `RSI ${data.oms_rsi ?? '-'} (Mittellinie ${data.config.oms_rsi_midline})` : 'deaktiviert';
  const oiDetail = data.config.oms_oi_filter_enabled ? `Score ${data.oms_oi_score ?? '-'} (min. ${data.config.oms_oi_min_score})` : 'deaktiviert';
  const liqDetail = data.config.oms_liq_filter_enabled ? `${data.oms_liq_ratio ?? '-'} (${data.oms_liq_count ?? 0} Events im Fenster, min. ${data.config.oms_liq_min_ratio})` : 'deaktiviert';
  return `<div class="panel-card" style="padding:14px;">
    <div style="font-size:12px; color:var(--text-dim); margin-bottom:6px;">Warum feuert (nicht)?</div>
    ${row('OBI-Übereinstimmung (3 Fenster gleiche Richtung)', obiOk, obiDetail)}
    ${row('CVD-Bestätigung', data.oms_cvd_ok, cvdDetail)}
    ${row('Funding-Filter bestanden', data.oms_funding_ok, fundingDetail)}
    ${row('RSI-Regime-Filter bestanden', data.oms_rsi_ok, rsiDetail)}
    ${row('Open-Interest-Filter bestanden', data.oms_oi_ok, oiDetail)}
    ${row('Liquidations-Filter bestanden', data.oms_liq_ok, liqDetail)}
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
  if (tp1Price != null) levelLines += `<line x1="${pad}" y1="${yOf(tp1Price).toFixed(1)}" x2="${w-pad}" y2="${yOf(tp1Price).toFixed(1)}" stroke="#22c55e" stroke-width="1" stroke-dasharray="4,3"/><text x="${w-pad}" y="${(yOf(tp1Price)-3).toFixed(1)}" fill="#22c55e" font-size="9" text-anchor="end">${pos && pos.exit_mode === 'single_tp' ? 'TP' : 'TP1'}</text>`;
  if (trailPrice != null) levelLines += `<line x1="${pad}" y1="${yOf(trailPrice).toFixed(1)}" x2="${w-pad}" y2="${yOf(trailPrice).toFixed(1)}" stroke="#3b82f6" stroke-width="1" stroke-dasharray="4,3"/><text x="${w-pad}" y="${(yOf(trailPrice)-3).toFixed(1)}" fill="#3b82f6" font-size="9" text-anchor="end">Trail</text>`;

  const styles = {
    entry_long: { shape: 'triUp', color: '#22c55e', label: 'LONG' },
    entry_short: { shape: 'triDown', color: '#f0526b', label: 'SHORT' },
    dca_long: { shape: 'circle', color: '#86efac', r: 3, label: '+' },
    dca_short: { shape: 'circle', color: '#fca5a5', r: 3, label: '+' },
    exit_sl: { shape: 'x', color: '#f0526b', label: 'SL' },
    exit_tp1: { shape: 'circle', color: '#22c55e', r: 4, label: 'TP1' },
    exit_tp: { shape: 'circle', color: '#22c55e', r: 5, label: 'TP' },
    exit_trail: { shape: 'circle', color: '#3b82f6', r: 4, label: 'Exit' },
    exit_reverse: { shape: 'x', color: '#a855f7', label: 'Reverse' },
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
    <div style="font-size:11px; color:var(--text-dim); margin-bottom:4px;">Preisverlauf (15 Min) · 🔺LONG · 🔻SHORT · ⭕Nachkauf · ✖️SL · 🟢TP1 · 🔵Trail-Exit · 🟣Reverse</div>
    <svg viewBox="0 0 ${w} ${h}" style="width:100%; height:130px; display:block;">
      ${levelLines}
      <polyline points="${points}" fill="none" stroke="var(--accent)" stroke-width="1.5"/>
      ${markerSvgs}
    </svg>
  </div>`;
}

async function refresh() {
  if (!currentSymbol) return;
  const requestedSymbol = currentSymbol;
  const res = await fetch(`/api/status?symbol=${requestedSymbol}`);
  const data = await res.json();
  // Race-Condition-Schutz: waehrend die Antwort unterwegs war, koennte der Nutzer schon auf
  // einen anderen Coin gewechselt haben (z.B. schnell BTC -> ETH -> BTC). Ohne diese Pruefung
  // wuerde die verspaetete Antwort fuer den ALTEN Coin die Formularfelder des inzwischen
  // angezeigten Coins ueberschreiben - genau das fuehrte zu falsch angezeigten Werten
  // (z.B. entry_mode) nach schnellem Hin- und Herwechseln.
  if (requestedSymbol !== currentSymbol) return;

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

  const showGridWidget = (gsiId, show) => {
    const el = document.getElementById(gsiId);
    if (el) el.style.display = show ? '' : 'none';
  };
  const isOms = data.config.entry_mode === 'oms_scalp';
  const isObiLikeMode = isOms || data.config.entry_mode === 'obi_scalp';
  // Das Grid selbst ist jetzt IMMER sichtbar (Pocket-Trading und das Scalp-Board sind
  // absichtlich unabhaengig von der gewaehlten Strategie nutzbar) - nur einzelne Kacheln
  // darin bleiben an bestimmte Modi gebunden (z.B. die OBI-/CVD-Gauges an oms_scalp).
  document.getElementById('oms-grid').style.display = '';
  document.getElementById('oms-grid-header').style.display = '';
  showGridWidget('gsi-signal', isOms);
  showGridWidget('gsi-gauge', isOms);
  showGridWidget('gsi-cvd-gauge', isOms);
  showGridWidget('gsi-oi-gauge', isOms);
  showGridWidget('gsi-liq-gauge', isOms);
  showGridWidget('gsi-checklist', isOms);
  showGridWidget('gsi-chart', isOms);
  showGridWidget('gsi-pocket', true);
  showGridWidget('gsi-obi', isObiLikeMode);
  showGridWidget('gsi-scalp-board', true);
  document.getElementById('scalp-board-wrap').innerHTML = renderScalpBoard(data.scalp_board);
  showGridWidget('gsi-quad-stoch', true);

  if (isOms) {
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
      `CVD: ${data.oms_cvd_ratio ?? '-'}  |  Funding: ${data.oms_funding_rate != null ? (data.oms_funding_rate*100).toFixed(4)+'%' : '-'}` +
      (data.config.oms_rsi_filter_enabled ? `  |  RSI: ${data.oms_rsi ?? '-'}` : '');

    gaugeWrap.innerHTML = renderOmsGauge(data.oms_obi_fast, data.oms_obi_medium, data.oms_obi_slow, data.config.oms_obi_threshold);
    document.getElementById('oms-cvd-gauge-wrap').innerHTML = renderOmsCvdGauge(data.oms_cvd_ratio, data.config.oms_cvd_min_ratio);
    document.getElementById('oms-oi-gauge-wrap').innerHTML = renderOmsOiGauge(data.oms_oi_score, data.config.oms_oi_min_score);
    document.getElementById('oms-liq-gauge-wrap').innerHTML = renderOmsLiqGauge(data.oms_liq_ratio, data.config.oms_liq_min_ratio, data.oms_liq_count);
    checklistWrap.innerHTML = renderOmsChecklist(data);
    chartWrap.innerHTML = renderOmsChart(data.oms_price_history, data.oms_markers, {
      position: data.position, avg_entry_price: data.avg_entry_price, size: data.total_coin_size,
      sl_usd: data.config.oms_sl_usd, tp1_usd: data.config.oms_tp1_usd,
      tp1_done: data.oms_tp1_done, trail_price: data.oms_trail_price, exit_mode: data.config.oms_exit_mode,
    });
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
    <div class="card"><div class="label">HalfTrend (${data.config.entry_mode==='halftrend'?'aktiv':'inaktiv'})</div><div class="value ${data.ht_direction===1?'green':data.ht_direction===-1?'red':''}">${data.ht_direction===1?'LONG-Signal':data.ht_direction===-1?'SHORT-Signal':'-'}</div></div>
    <div class="card"><div class="label">HalfTrend SL</div><div class="value">${data.ht_sl_price!=null?data.ht_sl_price.toFixed(4):'-'}${data.ht_tp1_done?' (Break-Even)':''}</div></div>
    <div class="card"><div class="label">HalfTrend TP1 / TP2 / TP3</div><div class="value">${data.ht_tp1_price!=null?data.ht_tp1_price.toFixed(4):'-'}${data.ht_tp1_done?'✓':''} / ${data.ht_tp2_price!=null?data.ht_tp2_price.toFixed(4):'-'}${data.ht_tp2_done?'✓':''} / ${data.ht_tp3_price!=null?data.ht_tp3_price.toFixed(4):'-'}</div></div>
    <div class="card"><div class="label">Diamond Algo (${data.config.entry_mode==='diamond_algo'?'aktiv':'inaktiv'})</div><div class="value ${data.da_direction===1?'green':data.da_direction===-1?'red':''}">${data.da_direction===1?'LONG-Signal':data.da_direction===-1?'SHORT-Signal':'-'}</div></div>
    <div class="card"><div class="label">Diamond Algo SL / TP</div><div class="value">${data.da_sl_price!=null?data.da_sl_price.toFixed(4):'-'} / ${data.da_tp_price!=null?data.da_tp_price.toFixed(4):'-'}</div></div>
    <div class="card"><div class="label">ELTE Smart (${data.config.entry_mode==='elte_smart'?'aktiv':'inaktiv'})</div><div class="value ${data.es_direction===1?'green':data.es_direction===-1?'red':''}">${data.es_direction===1?'LONG-Signal':data.es_direction===-1?'SHORT-Signal':'-'} (Sens. ${data.es_sensitivity_last!=null?data.es_sensitivity_last.toFixed(2):'-'})</div></div>
    <div class="card"><div class="label">ELTE Smart SL / TP1 / TP2 / TP3</div><div class="value">${data.es_sl_price!=null?data.es_sl_price.toFixed(4):'-'} / ${data.es_tp1_price!=null?data.es_tp1_price.toFixed(4):'-'}${data.es_tp1_done?'✓':''} / ${data.es_tp2_price!=null?data.es_tp2_price.toFixed(4):'-'}${data.es_tp2_done?'✓':''} / ${data.es_tp3_price!=null?data.es_tp3_price.toFixed(4):'-'}</div></div>
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
    document.getElementById('oms_exit_mode').value = data.config.oms_exit_mode;
    document.getElementById('oms_tp1_close_pct').value = data.config.oms_tp1_close_pct;
    document.getElementById('oms_sl_usd').value = data.config.oms_sl_usd;
    document.getElementById('oms_trail_distance_usd').value = data.config.oms_trail_distance_usd;
    document.getElementById('oms_dca_enabled').value = String(data.config.oms_dca_enabled);
    document.getElementById('oms_dca_max_entries').value = data.config.oms_dca_max_entries;
    document.getElementById('oms_dca_size_fraction').value = data.config.oms_dca_size_fraction;
    document.getElementById('oms_dca_min_pullback_usd').value = data.config.oms_dca_min_pullback_usd;
    document.getElementById('oms_reverse_on_signal').value = String(data.config.oms_reverse_on_signal);
    document.getElementById('oms_rsi_filter_enabled').value = String(data.config.oms_rsi_filter_enabled);
    document.getElementById('oms_rsi_resolution').value = data.config.oms_rsi_resolution;
    document.getElementById('oms_rsi_period').value = data.config.oms_rsi_period;
    document.getElementById('oms_rsi_midline').value = data.config.oms_rsi_midline;
    document.getElementById('oms_oi_filter_enabled').value = String(data.config.oms_oi_filter_enabled);
    document.getElementById('oms_oi_window_seconds').value = data.config.oms_oi_window_seconds;
    document.getElementById('oms_oi_min_change_pct').value = data.config.oms_oi_min_change_pct;
    document.getElementById('oms_oi_min_score').value = data.config.oms_oi_min_score;
    document.getElementById('oms_liq_filter_enabled').value = String(data.config.oms_liq_filter_enabled);
    document.getElementById('oms_liq_window_seconds').value = data.config.oms_liq_window_seconds;
    document.getElementById('oms_liq_min_ratio').value = data.config.oms_liq_min_ratio;
    document.getElementById('fib_resolution').value = data.config.fib_resolution;
    document.getElementById('fib_lookback_candles').value = data.config.fib_lookback_candles;
    document.getElementById('fib_entry1_level').value = data.config.fib_entry1_level;
    document.getElementById('fib_entry2_level').value = data.config.fib_entry2_level;
    document.getElementById('fib_tp1_level').value = data.config.fib_tp1_level;
    document.getElementById('fib_tp1_close_pct').value = data.config.fib_tp1_close_pct;
    document.getElementById('fib_tp2_level').value = data.config.fib_tp2_level;
    document.getElementById('fib_sl_level').value = data.config.fib_sl_level;
    document.getElementById('fib_cooldown_seconds').value = data.config.fib_cooldown_seconds;
    setResolutionField('ht_resolution', data.config.ht_resolution);
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
    setResolutionField('da_resolution', data.config.da_resolution);
    document.getElementById('da_atr_period').value = data.config.da_atr_period;
    document.getElementById('da_sensitivity').value = data.config.da_sensitivity;
    document.getElementById('da_sma_period').value = data.config.da_sma_period;
    document.getElementById('da_ema_trend_period').value = data.config.da_ema_trend_period;
    document.getElementById('da_signal_mode').value = data.config.da_signal_mode;
    document.getElementById('da_entry_trigger').value = data.config.da_entry_trigger;
    document.getElementById('da_exit_trigger').value = data.config.da_exit_trigger;
    document.getElementById('da_invert_direction').value = String(data.config.da_invert_direction);
    document.getElementById('da_sl_enabled').value = String(data.config.da_sl_enabled);
    document.getElementById('da_tp_enabled').value = String(data.config.da_tp_enabled);
    document.getElementById('da_risk_atr_period').value = data.config.da_risk_atr_period;
    document.getElementById('da_risk_mult').value = data.config.da_risk_mult;
    document.getElementById('da_tp_rr').value = data.config.da_tp_rr;
    document.getElementById('da_sl_cooldown_seconds').value = data.config.da_sl_cooldown_seconds;
    document.getElementById('da_use_heikin_ashi').value = String(data.config.da_use_heikin_ashi);
    setResolutionField('es_resolution', data.config.es_resolution);
    document.getElementById('es_atr_period').value = data.config.es_atr_period;
    document.getElementById('es_auto_sensitivity').value = String(data.config.es_auto_sensitivity);
    document.getElementById('es_sensitivity').value = data.config.es_sensitivity;
    document.getElementById('es_vol_period').value = data.config.es_vol_period;
    document.getElementById('es_vol_ma_len').value = data.config.es_vol_ma_len;
    document.getElementById('es_entry_trigger').value = data.config.es_entry_trigger;
    document.getElementById('es_exit_trigger').value = data.config.es_exit_trigger;
    document.getElementById('es_invert_direction').value = String(data.config.es_invert_direction);
    document.getElementById('es_risk_atr_period').value = data.config.es_risk_atr_period;
    document.getElementById('es_risk_mult').value = data.config.es_risk_mult;
    document.getElementById('es_tp1_close_pct').value = data.config.es_tp1_close_pct;
    document.getElementById('es_tp2_close_pct').value = data.config.es_tp2_close_pct;
    document.getElementById('es_tp1_rr').value = data.config.es_tp1_rr;
    document.getElementById('es_tp2_rr').value = data.config.es_tp2_rr;
    document.getElementById('es_tp3_rr').value = data.config.es_tp3_rr;
    document.getElementById('es_sl_cooldown_seconds').value = data.config.es_sl_cooldown_seconds;
    document.getElementById('es_reenter_on_flip').value = String(data.config.es_reenter_on_flip);
    document.getElementById('es_sl_enabled').value = String(data.config.es_sl_enabled);
    document.getElementById('es_sl_mode').value = data.config.es_sl_mode;
    document.getElementById('es_sl_manual_usd').value = data.config.es_sl_manual_usd;
    document.getElementById('es_tp_mode').value = data.config.es_tp_mode;
    document.getElementById('es_tp_manual_usd').value = data.config.es_tp_manual_usd;
    document.getElementById('es_breakeven_pct_enabled').value = String(data.config.es_breakeven_pct_enabled);
    document.getElementById('es_breakeven_trigger_pct').value = data.config.es_breakeven_trigger_pct;
    document.getElementById('es_tp_enabled').value = String(data.config.es_tp_enabled);
    setResolutionField('cp_resolution', data.config.cp_resolution);
    document.getElementById('cp_signal_source').value = data.config.cp_signal_source;
    document.getElementById('cp_three_line_strict').value = String(data.config.cp_three_line_strict);
    document.getElementById('cp_engulfing_strict').value = String(data.config.cp_engulfing_strict);
    document.getElementById('cp_direction_mode').value = data.config.cp_direction_mode;
    document.getElementById('cp_flip_exit_enabled').value = String(data.config.cp_flip_exit_enabled);
    document.getElementById('cp_risk_atr_period').value = data.config.cp_risk_atr_period;
    document.getElementById('cp_risk_mult').value = data.config.cp_risk_mult;
    document.getElementById('cp_tp_rr').value = data.config.cp_tp_rr;
    document.getElementById('cp_sl_cooldown_seconds').value = data.config.cp_sl_cooldown_seconds;
    document.getElementById('cp_sl_enabled').value = String(data.config.cp_sl_enabled);
    document.getElementById('cp_sl_mode').value = data.config.cp_sl_mode;
    document.getElementById('cp_sl_manual_usd').value = data.config.cp_sl_manual_usd;
    document.getElementById('cp_tp_enabled').value = String(data.config.cp_tp_enabled);
    document.getElementById('cp_tp_mode').value = data.config.cp_tp_mode;
    document.getElementById('cp_tp_manual_usd').value = data.config.cp_tp_manual_usd;
    document.getElementById('cp_breakeven_enabled').value = String(data.config.cp_breakeven_enabled);
    document.getElementById('cp_breakeven_trigger_mult').value = data.config.cp_breakeven_trigger_mult;
    document.getElementById('mo7_resolution').value = data.config.mo7_resolution;
    document.getElementById('mo7_entry_mode').value = data.config.mo7_entry_mode;
    document.getElementById('mo7_buy_threshold').value = data.config.mo7_buy_threshold;
    document.getElementById('mo7_sell_threshold').value = data.config.mo7_sell_threshold;
    document.getElementById('mo7_sum_low').value = data.config.mo7_sum_low;
    document.getElementById('mo7_sum_high').value = data.config.mo7_sum_high;
    document.getElementById('mo7_trend_threshold').value = data.config.mo7_trend_threshold;
    document.getElementById('mo7_trend_deadband').value = data.config.mo7_trend_deadband;
    document.getElementById('mo7_direction_mode').value = data.config.mo7_direction_mode;
    document.getElementById('mo7_flip_exit_enabled').value = String(data.config.mo7_flip_exit_enabled);
    document.getElementById('mo7_sl_enabled').value = String(data.config.mo7_sl_enabled);
    document.getElementById('mo7_sl_manual_usd').value = data.config.mo7_sl_manual_usd;
    document.getElementById('mo7_tp_enabled').value = String(data.config.mo7_tp_enabled);
    document.getElementById('mo7_tp_manual_usd').value = data.config.mo7_tp_manual_usd;
    document.getElementById('mo7_sl_cooldown_seconds').value = data.config.mo7_sl_cooldown_seconds;
    setResolutionField('utb_resolution', data.config.utb_resolution);
    document.getElementById('utb_atr_period').value = data.config.utb_atr_period;
    document.getElementById('utb_sensitivity').value = data.config.utb_sensitivity;
    document.getElementById('utb_heikin_ashi').value = String(data.config.utb_heikin_ashi);
    document.getElementById('utb_hull_period').value = data.config.utb_hull_period;
    document.getElementById('utb_flip_trigger').value = data.config.utb_flip_trigger;
    document.getElementById('utb_direction_mode').value = data.config.utb_direction_mode;
    document.getElementById('utb_sl_enabled').value = String(data.config.utb_sl_enabled);
    document.getElementById('utb_sl_manual_usd').value = data.config.utb_sl_manual_usd;
    document.getElementById('utb_sl_cooldown_seconds').value = data.config.utb_sl_cooldown_seconds;
    document.getElementById('utb_mtf_filter_enabled').value = String(data.config.utb_mtf_filter_enabled);
    setResolutionField('utb_mtf_tf1', data.config.utb_mtf_tf1);
    setResolutionField('utb_mtf_tf2', data.config.utb_mtf_tf2);
    setResolutionField('utb_mtf_tf3', data.config.utb_mtf_tf3);
    document.getElementById('utb_mtf_long_threshold').value = data.config.utb_mtf_long_threshold;
    document.getElementById('utb_mtf_short_threshold').value = data.config.utb_mtf_short_threshold;
    document.getElementById('utb_mtf_fast_len').value = data.config.utb_mtf_fast_len;
    document.getElementById('utb_mtf_slow_len').value = data.config.utb_mtf_slow_len;
    document.getElementById('utb_mtf_atr_len').value = data.config.utb_mtf_atr_len;
    setResolutionField('wtc_resolution', data.config.wtc_resolution);
    document.getElementById('wtc_channel_len').value = data.config.wtc_channel_len;
    document.getElementById('wtc_average_len').value = data.config.wtc_average_len;
    document.getElementById('wtc_ma_len').value = data.config.wtc_ma_len;
    document.getElementById('wtc_require_zone').value = String(data.config.wtc_require_zone);
    document.getElementById('wtc_os_level').value = data.config.wtc_os_level;
    document.getElementById('wtc_ob_level').value = data.config.wtc_ob_level;
    document.getElementById('wtc_direction_mode').value = data.config.wtc_direction_mode;
    document.getElementById('wtc_always_in_market').value = String(data.config.wtc_always_in_market);
    document.getElementById('wtc_flip_exit_enabled').value = String(data.config.wtc_flip_exit_enabled);
    document.getElementById('wtc_sl_enabled').value = String(data.config.wtc_sl_enabled);
    document.getElementById('wtc_sl_manual_usd').value = data.config.wtc_sl_manual_usd;
    document.getElementById('wtc_tp_enabled').value = String(data.config.wtc_tp_enabled);
    document.getElementById('wtc_tp_manual_usd').value = data.config.wtc_tp_manual_usd;
    document.getElementById('wtc_sl_cooldown_seconds').value = data.config.wtc_sl_cooldown_seconds;
    setResolutionField('pk_resolution', data.config.pk_resolution);
    document.getElementById('pk_sensitivity').value = data.config.pk_sensitivity;
    document.getElementById('pk_atr_period').value = data.config.pk_atr_period;
    document.getElementById('pk_sma_period').value = data.config.pk_sma_period;
    document.getElementById('pk_direction_mode').value = data.config.pk_direction_mode;
    document.getElementById('pk_exit_mode').value = data.config.pk_exit_mode;
    document.getElementById('pk_sl_enabled').value = String(data.config.pk_sl_enabled);
    document.getElementById('pk_sl_manual_usd').value = data.config.pk_sl_manual_usd;
    document.getElementById('pk_tp_enabled').value = String(data.config.pk_tp_enabled);
    document.getElementById('pk_tp_manual_usd').value = data.config.pk_tp_manual_usd;
    document.getElementById('pk_sl_cooldown_seconds').value = data.config.pk_sl_cooldown_seconds;
    document.getElementById('pk_trailing_enabled').value = String(data.config.pk_trailing_enabled);
    document.getElementById('pk_trailing_activation_pct').value = data.config.pk_trailing_activation_pct;
    document.getElementById('pk_trailing_step_pct').value = data.config.pk_trailing_step_pct;
    document.getElementById('pk_mtf_filter_enabled').value = String(data.config.pk_mtf_filter_enabled);
    setResolutionField('pk_mtf_tf1', data.config.pk_mtf_tf1);
    setResolutionField('pk_mtf_tf2', data.config.pk_mtf_tf2);
    setResolutionField('pk_mtf_tf3', data.config.pk_mtf_tf3);
    document.getElementById('pk_mtf_long_threshold').value = data.config.pk_mtf_long_threshold;
    document.getElementById('pk_mtf_short_threshold').value = data.config.pk_mtf_short_threshold;
    document.getElementById('pk_mtf_fast_len').value = data.config.pk_mtf_fast_len;
    document.getElementById('pk_mtf_slow_len').value = data.config.pk_mtf_slow_len;
    document.getElementById('pk_mtf_atr_len').value = data.config.pk_mtf_atr_len;
    setResolutionField('fr_resolution', data.config.fr_resolution);
    document.getElementById('fr_periods').value = data.config.fr_periods;
    document.getElementById('fr_direction_mode').value = data.config.fr_direction_mode;
    document.getElementById('fr_invert_direction').value = String(data.config.fr_invert_direction);
    document.getElementById('fr_zscore_filter_enabled').value = String(data.config.fr_zscore_filter_enabled);
    setResolutionField('fr_zscore_resolution', data.config.fr_zscore_resolution);
    document.getElementById('fr_zscore_lookback').value = data.config.fr_zscore_lookback;
    document.getElementById('fr_zscore_smooth').value = data.config.fr_zscore_smooth;
    document.getElementById('fr_sl_enabled').value = String(data.config.fr_sl_enabled);
    document.getElementById('fr_sl_manual_usd').value = data.config.fr_sl_manual_usd;
    document.getElementById('fr_sl_cooldown_seconds').value = data.config.fr_sl_cooldown_seconds;
    setResolutionField('cd_resolution', data.config.cd_resolution);
    document.getElementById('cd_threshold').value = data.config.cd_threshold;
    document.getElementById('cd_rejection_mult').value = data.config.cd_rejection_mult;
    document.getElementById('cd_direction_mode').value = data.config.cd_direction_mode;
    document.getElementById('cd_zscore_filter_enabled').value = String(data.config.cd_zscore_filter_enabled);
    setResolutionField('cd_zscore_resolution', data.config.cd_zscore_resolution);
    document.getElementById('cd_zscore_lookback').value = data.config.cd_zscore_lookback;
    document.getElementById('cd_zscore_smooth').value = data.config.cd_zscore_smooth;
    document.getElementById('cd_rsi_filter_enabled').value = String(data.config.cd_rsi_filter_enabled);
    document.getElementById('cd_rsi_length').value = data.config.cd_rsi_length;
    document.getElementById('cd_rsi_midline').value = data.config.cd_rsi_midline;
    document.getElementById('cd_adx_filter_enabled').value = String(data.config.cd_adx_filter_enabled);
    document.getElementById('cd_adx_length').value = data.config.cd_adx_length;
    document.getElementById('cd_adx_threshold').value = data.config.cd_adx_threshold;
    document.getElementById('cd_sl_enabled').value = String(data.config.cd_sl_enabled);
    document.getElementById('cd_sl_manual_usd').value = data.config.cd_sl_manual_usd;
    document.getElementById('cd_sl_cooldown_seconds').value = data.config.cd_sl_cooldown_seconds;
    document.getElementById('cd_use_heikin_ashi').value = String(data.config.cd_use_heikin_ashi);
    document.getElementById('grid_direction_mode').value = data.config.grid_direction_mode;
    document.getElementById('grid_mode').value = data.config.grid_mode;
    document.getElementById('grid_step_pct').value = data.config.grid_step_pct;
    document.getElementById('tp_step_pct').value = data.config.tp_step_pct;
    document.getElementById('grid_step_usd').value = data.config.grid_step_usd;
    document.getElementById('tp_step_usd').value = data.config.tp_step_usd;
    document.getElementById('max_nachkauf').value = data.config.max_nachkauf;
    document.getElementById('grid_sl_enabled').value = String(data.config.grid_sl_enabled);
    document.getElementById('grid_sl_manual_usd').value = data.config.grid_sl_manual_usd;
    document.getElementById('grid_anchor_follow_enabled').value = String(data.config.grid_anchor_follow_enabled);
    document.getElementById('grid_anchor_follow_pct').value = data.config.grid_anchor_follow_pct;
    document.getElementById('dry_run').value = String(data.config.dry_run);
    document.getElementById('binance_market_type').value = data.config.binance_market_type;
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
    const isObiLike = data.config.entry_mode === 'obi_scalp' || data.config.entry_mode === 'oms_scalp';
    const rawHist = data.config.entry_mode === 'oms_scalp' ? (data.oms_obi_history || []) : (data.obi_history || []);
    const threshold = data.config.entry_mode === 'oms_scalp' ? data.config.oms_obi_threshold : data.config.obi_threshold;
    if (isObiLike && rawHist.length > 0) {
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
      const obiCanvas = document.getElementById('obiChart');
      if (obiCanvas) {
        if (!obiChart) {
          obiChart = new Chart(obiCanvas, {
            type: 'line',
            data: { labels: obiLabels, datasets: obiDatasets },
            options: {
              responsive:true, maintainAspectRatio:false, animation:false,
              scales: { x:{ display:false }, y:{ min:-1, max:1, ticks:{color:'#9ca3af'} } },
              plugins:{legend:{labels:{color:'#e5e7eb', boxWidth:10, font:{size:10}}}}
            }
          });
          // Falls die Kachel beim ersten Erstellen noch unsichtbar war (display:none),
          // rechnet Chart.js sonst dauerhaft mit Groesse 0 - erzwingt Neuberechnung
          requestAnimationFrame(() => obiChart && obiChart.resize());
        } else {
          obiChart.data.labels = obiLabels;
          obiChart.data.datasets = obiDatasets;
          obiChart.update('none');
        }
      }
    }
  } catch (e) {
    console.error('OBI-Chart-Fehler:', e);
  }

  try {
    const resSelect = document.getElementById('quad-stoch-resolution-select');
    if (resSelect && document.activeElement !== resSelect) {
      resSelect.value = data.config.quad_stoch_resolution || '1m';
    }
    const qHist = data.quad_stoch_history || [];
    if (qHist.length > 0) {
      const qLabels = qHist.map(p => new Date(p.ts).toLocaleTimeString());
      const qDatasets = [
        { label:'Stoch 1 (9,3)', data: qHist.map(p=>p.s1), borderColor:'#f87171', pointRadius:0, borderWidth:2 },
        { label:'Stoch 2 (14,3)', data: qHist.map(p=>p.s2), borderColor:'#4ade80', pointRadius:0, borderWidth:1 },
        { label:'Stoch 3 (40,4)', data: qHist.map(p=>p.s3), borderColor:'#22d3ee', pointRadius:0, borderWidth:1 },
        { label:'Stoch 4 (60,10)', data: qHist.map(p=>p.s4), borderColor:'#e879f9', pointRadius:0, borderWidth:1 },
        { label:'Überkauft', data: Array(qHist.length).fill(80), borderColor:'#6b7280', borderDash:[4,4], pointRadius:0, borderWidth:1 },
        { label:'Überverkauft', data: Array(qHist.length).fill(20), borderColor:'#6b7280', borderDash:[4,4], pointRadius:0, borderWidth:1 },
      ];
      const qCanvas = document.getElementById('quadStochChart');
      if (qCanvas) {
        if (!quadStochChart) {
          quadStochChart = new Chart(qCanvas, {
            type: 'line',
            data: { labels: qLabels, datasets: qDatasets },
            options: {
              responsive:true, maintainAspectRatio:false, animation:false,
              scales: { x:{ display:false }, y:{ min:0, max:100, ticks:{color:'#9ca3af'} } },
              plugins:{legend:{labels:{color:'#e5e7eb', boxWidth:10, font:{size:10}}}}
            }
          });
          requestAnimationFrame(() => quadStochChart && quadStochChart.resize());
        } else {
          quadStochChart.data.labels = qLabels;
          quadStochChart.data.datasets = qDatasets;
          quadStochChart.update('none');
        }
      }
    }
  } catch (e) {
    console.error('Quad-Stochastic-Chart-Fehler:', e);
  }

  try {
    document.getElementById('pocket-margin').innerText = `$${data.config.margin} (${data.config.leverage}x)`;
    document.getElementById('pocket-position').innerText = data.position ? data.position.toUpperCase() : 'flach';
    document.getElementById('pocket-entry').innerText = data.avg_entry_price ?? '-';
    const pnlEl = document.getElementById('pocket-pnl');
    pnlEl.innerText = data.unrealized_pnl_usd ?? '-';
    pnlEl.className = (data.unrealized_pnl_usd ?? 0) >= 0 ? 'value green' : 'value red';
    renderMiniCandles(hist);
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
    oms_exit_mode: document.getElementById('oms_exit_mode').value,
    oms_tp1_close_pct: parseFloat(document.getElementById('oms_tp1_close_pct').value),
    oms_sl_usd: parseFloat(document.getElementById('oms_sl_usd').value),
    oms_trail_distance_usd: parseFloat(document.getElementById('oms_trail_distance_usd').value),
    oms_dca_enabled: document.getElementById('oms_dca_enabled').value === 'true',
    oms_dca_max_entries: parseInt(document.getElementById('oms_dca_max_entries').value),
    oms_dca_size_fraction: parseFloat(document.getElementById('oms_dca_size_fraction').value),
    oms_dca_min_pullback_usd: parseFloat(document.getElementById('oms_dca_min_pullback_usd').value),
    oms_reverse_on_signal: document.getElementById('oms_reverse_on_signal').value === 'true',
    oms_rsi_filter_enabled: document.getElementById('oms_rsi_filter_enabled').value === 'true',
    oms_rsi_resolution: document.getElementById('oms_rsi_resolution').value,
    oms_rsi_period: parseInt(document.getElementById('oms_rsi_period').value),
    oms_rsi_midline: parseFloat(document.getElementById('oms_rsi_midline').value),
    oms_oi_filter_enabled: document.getElementById('oms_oi_filter_enabled').value === 'true',
    oms_oi_window_seconds: parseFloat(document.getElementById('oms_oi_window_seconds').value),
    oms_oi_min_change_pct: parseFloat(document.getElementById('oms_oi_min_change_pct').value),
    oms_oi_min_score: parseFloat(document.getElementById('oms_oi_min_score').value),
    oms_liq_filter_enabled: document.getElementById('oms_liq_filter_enabled').value === 'true',
    oms_liq_window_seconds: parseFloat(document.getElementById('oms_liq_window_seconds').value),
    oms_liq_min_ratio: parseFloat(document.getElementById('oms_liq_min_ratio').value),
    fib_resolution: document.getElementById('fib_resolution').value,
    fib_lookback_candles: parseInt(document.getElementById('fib_lookback_candles').value),
    fib_entry1_level: parseFloat(document.getElementById('fib_entry1_level').value),
    fib_entry2_level: parseFloat(document.getElementById('fib_entry2_level').value),
    fib_tp1_level: parseFloat(document.getElementById('fib_tp1_level').value),
    fib_tp1_close_pct: parseFloat(document.getElementById('fib_tp1_close_pct').value),
    fib_tp2_level: parseFloat(document.getElementById('fib_tp2_level').value),
    fib_sl_level: parseFloat(document.getElementById('fib_sl_level').value),
    fib_cooldown_seconds: parseFloat(document.getElementById('fib_cooldown_seconds').value),
    ht_resolution: getResolutionField('ht_resolution'),
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
    da_resolution: getResolutionField('da_resolution'),
    da_atr_period: parseInt(document.getElementById('da_atr_period').value),
    da_sensitivity: parseFloat(document.getElementById('da_sensitivity').value),
    da_sma_period: parseInt(document.getElementById('da_sma_period').value),
    da_ema_trend_period: parseInt(document.getElementById('da_ema_trend_period').value),
    da_signal_mode: document.getElementById('da_signal_mode').value,
    da_entry_trigger: document.getElementById('da_entry_trigger').value,
    da_exit_trigger: document.getElementById('da_exit_trigger').value,
    da_invert_direction: document.getElementById('da_invert_direction').value === 'true',
    da_sl_enabled: document.getElementById('da_sl_enabled').value === 'true',
    da_tp_enabled: document.getElementById('da_tp_enabled').value === 'true',
    da_risk_atr_period: parseInt(document.getElementById('da_risk_atr_period').value),
    da_risk_mult: parseFloat(document.getElementById('da_risk_mult').value),
    da_tp_rr: parseFloat(document.getElementById('da_tp_rr').value),
    da_sl_cooldown_seconds: parseFloat(document.getElementById('da_sl_cooldown_seconds').value),
    da_use_heikin_ashi: document.getElementById('da_use_heikin_ashi').value === 'true',
    es_resolution: getResolutionField('es_resolution'),
    es_atr_period: parseInt(document.getElementById('es_atr_period').value),
    es_auto_sensitivity: document.getElementById('es_auto_sensitivity').value === 'true',
    es_sensitivity: parseFloat(document.getElementById('es_sensitivity').value),
    es_vol_period: parseInt(document.getElementById('es_vol_period').value),
    es_vol_ma_len: parseInt(document.getElementById('es_vol_ma_len').value),
    es_entry_trigger: document.getElementById('es_entry_trigger').value,
    es_exit_trigger: document.getElementById('es_exit_trigger').value,
    es_invert_direction: document.getElementById('es_invert_direction').value === 'true',
    es_risk_atr_period: parseInt(document.getElementById('es_risk_atr_period').value),
    es_risk_mult: parseFloat(document.getElementById('es_risk_mult').value),
    es_tp1_close_pct: parseFloat(document.getElementById('es_tp1_close_pct').value),
    es_tp2_close_pct: parseFloat(document.getElementById('es_tp2_close_pct').value),
    es_tp1_rr: parseFloat(document.getElementById('es_tp1_rr').value),
    es_tp2_rr: parseFloat(document.getElementById('es_tp2_rr').value),
    es_tp3_rr: parseFloat(document.getElementById('es_tp3_rr').value),
    es_sl_cooldown_seconds: parseFloat(document.getElementById('es_sl_cooldown_seconds').value),
    es_reenter_on_flip: document.getElementById('es_reenter_on_flip').value === 'true',
    es_sl_enabled: document.getElementById('es_sl_enabled').value === 'true',
    es_sl_mode: document.getElementById('es_sl_mode').value,
    es_sl_manual_usd: parseFloat(document.getElementById('es_sl_manual_usd').value),
    es_tp_mode: document.getElementById('es_tp_mode').value,
    es_tp_manual_usd: parseFloat(document.getElementById('es_tp_manual_usd').value),
    es_breakeven_pct_enabled: document.getElementById('es_breakeven_pct_enabled').value === 'true',
    es_breakeven_trigger_pct: parseFloat(document.getElementById('es_breakeven_trigger_pct').value),
    es_tp_enabled: document.getElementById('es_tp_enabled').value === 'true',
    cp_resolution: getResolutionField('cp_resolution'),
    cp_signal_source: document.getElementById('cp_signal_source').value,
    cp_three_line_strict: document.getElementById('cp_three_line_strict').value === 'true',
    cp_engulfing_strict: document.getElementById('cp_engulfing_strict').value === 'true',
    cp_direction_mode: document.getElementById('cp_direction_mode').value,
    cp_flip_exit_enabled: document.getElementById('cp_flip_exit_enabled').value === 'true',
    cp_risk_atr_period: parseInt(document.getElementById('cp_risk_atr_period').value),
    cp_risk_mult: parseFloat(document.getElementById('cp_risk_mult').value),
    cp_tp_rr: parseFloat(document.getElementById('cp_tp_rr').value),
    cp_sl_cooldown_seconds: parseFloat(document.getElementById('cp_sl_cooldown_seconds').value),
    cp_sl_enabled: document.getElementById('cp_sl_enabled').value === 'true',
    cp_sl_mode: document.getElementById('cp_sl_mode').value,
    cp_sl_manual_usd: parseFloat(document.getElementById('cp_sl_manual_usd').value),
    cp_tp_enabled: document.getElementById('cp_tp_enabled').value === 'true',
    cp_tp_mode: document.getElementById('cp_tp_mode').value,
    cp_tp_manual_usd: parseFloat(document.getElementById('cp_tp_manual_usd').value),
    cp_breakeven_enabled: document.getElementById('cp_breakeven_enabled').value === 'true',
    cp_breakeven_trigger_mult: parseFloat(document.getElementById('cp_breakeven_trigger_mult').value),
    mo7_resolution: document.getElementById('mo7_resolution').value,
    mo7_entry_mode: document.getElementById('mo7_entry_mode').value,
    mo7_buy_threshold: parseFloat(document.getElementById('mo7_buy_threshold').value),
    mo7_sell_threshold: parseFloat(document.getElementById('mo7_sell_threshold').value),
    mo7_sum_low: parseFloat(document.getElementById('mo7_sum_low').value),
    mo7_sum_high: parseFloat(document.getElementById('mo7_sum_high').value),
    mo7_trend_threshold: parseFloat(document.getElementById('mo7_trend_threshold').value),
    mo7_trend_deadband: parseFloat(document.getElementById('mo7_trend_deadband').value),
    mo7_direction_mode: document.getElementById('mo7_direction_mode').value,
    mo7_flip_exit_enabled: document.getElementById('mo7_flip_exit_enabled').value === 'true',
    mo7_sl_enabled: document.getElementById('mo7_sl_enabled').value === 'true',
    mo7_sl_manual_usd: parseFloat(document.getElementById('mo7_sl_manual_usd').value),
    mo7_tp_enabled: document.getElementById('mo7_tp_enabled').value === 'true',
    mo7_tp_manual_usd: parseFloat(document.getElementById('mo7_tp_manual_usd').value),
    mo7_sl_cooldown_seconds: parseFloat(document.getElementById('mo7_sl_cooldown_seconds').value),
    utb_resolution: getResolutionField('utb_resolution'),
    utb_atr_period: parseInt(document.getElementById('utb_atr_period').value),
    utb_sensitivity: parseFloat(document.getElementById('utb_sensitivity').value),
    utb_heikin_ashi: document.getElementById('utb_heikin_ashi').value === 'true',
    utb_hull_period: parseInt(document.getElementById('utb_hull_period').value),
    utb_flip_trigger: document.getElementById('utb_flip_trigger').value,
    utb_direction_mode: document.getElementById('utb_direction_mode').value,
    utb_sl_enabled: document.getElementById('utb_sl_enabled').value === 'true',
    utb_sl_manual_usd: parseFloat(document.getElementById('utb_sl_manual_usd').value),
    utb_sl_cooldown_seconds: parseFloat(document.getElementById('utb_sl_cooldown_seconds').value),
    utb_mtf_filter_enabled: document.getElementById('utb_mtf_filter_enabled').value === 'true',
    utb_mtf_tf1: getResolutionField('utb_mtf_tf1'),
    utb_mtf_tf2: getResolutionField('utb_mtf_tf2'),
    utb_mtf_tf3: getResolutionField('utb_mtf_tf3'),
    utb_mtf_long_threshold: parseFloat(document.getElementById('utb_mtf_long_threshold').value),
    utb_mtf_short_threshold: parseFloat(document.getElementById('utb_mtf_short_threshold').value),
    utb_mtf_fast_len: parseInt(document.getElementById('utb_mtf_fast_len').value),
    utb_mtf_slow_len: parseInt(document.getElementById('utb_mtf_slow_len').value),
    utb_mtf_atr_len: parseInt(document.getElementById('utb_mtf_atr_len').value),
    wtc_resolution: getResolutionField('wtc_resolution'),
    wtc_channel_len: parseInt(document.getElementById('wtc_channel_len').value),
    wtc_average_len: parseInt(document.getElementById('wtc_average_len').value),
    wtc_ma_len: parseInt(document.getElementById('wtc_ma_len').value),
    wtc_require_zone: document.getElementById('wtc_require_zone').value === 'true',
    wtc_os_level: parseFloat(document.getElementById('wtc_os_level').value),
    wtc_ob_level: parseFloat(document.getElementById('wtc_ob_level').value),
    wtc_direction_mode: document.getElementById('wtc_direction_mode').value,
    wtc_always_in_market: document.getElementById('wtc_always_in_market').value === 'true',
    wtc_flip_exit_enabled: document.getElementById('wtc_flip_exit_enabled').value === 'true',
    wtc_sl_enabled: document.getElementById('wtc_sl_enabled').value === 'true',
    wtc_sl_manual_usd: parseFloat(document.getElementById('wtc_sl_manual_usd').value),
    wtc_tp_enabled: document.getElementById('wtc_tp_enabled').value === 'true',
    wtc_tp_manual_usd: parseFloat(document.getElementById('wtc_tp_manual_usd').value),
    wtc_sl_cooldown_seconds: parseFloat(document.getElementById('wtc_sl_cooldown_seconds').value),
    pk_resolution: getResolutionField('pk_resolution'),
    pk_sensitivity: parseFloat(document.getElementById('pk_sensitivity').value),
    pk_atr_period: parseInt(document.getElementById('pk_atr_period').value),
    pk_sma_period: parseInt(document.getElementById('pk_sma_period').value),
    pk_direction_mode: document.getElementById('pk_direction_mode').value,
    pk_exit_mode: document.getElementById('pk_exit_mode').value,
    pk_sl_enabled: document.getElementById('pk_sl_enabled').value === 'true',
    pk_sl_manual_usd: parseFloat(document.getElementById('pk_sl_manual_usd').value),
    pk_tp_enabled: document.getElementById('pk_tp_enabled').value === 'true',
    pk_tp_manual_usd: parseFloat(document.getElementById('pk_tp_manual_usd').value),
    pk_sl_cooldown_seconds: parseFloat(document.getElementById('pk_sl_cooldown_seconds').value),
    pk_trailing_enabled: document.getElementById('pk_trailing_enabled').value === 'true',
    pk_trailing_activation_pct: parseFloat(document.getElementById('pk_trailing_activation_pct').value),
    pk_trailing_step_pct: parseFloat(document.getElementById('pk_trailing_step_pct').value),
    pk_mtf_filter_enabled: document.getElementById('pk_mtf_filter_enabled').value === 'true',
    pk_mtf_tf1: getResolutionField('pk_mtf_tf1'),
    pk_mtf_tf2: getResolutionField('pk_mtf_tf2'),
    pk_mtf_tf3: getResolutionField('pk_mtf_tf3'),
    pk_mtf_long_threshold: parseFloat(document.getElementById('pk_mtf_long_threshold').value),
    pk_mtf_short_threshold: parseFloat(document.getElementById('pk_mtf_short_threshold').value),
    pk_mtf_fast_len: parseInt(document.getElementById('pk_mtf_fast_len').value),
    pk_mtf_slow_len: parseInt(document.getElementById('pk_mtf_slow_len').value),
    pk_mtf_atr_len: parseInt(document.getElementById('pk_mtf_atr_len').value),
    fr_resolution: getResolutionField('fr_resolution'),
    fr_periods: parseInt(document.getElementById('fr_periods').value),
    fr_direction_mode: document.getElementById('fr_direction_mode').value,
    fr_invert_direction: document.getElementById('fr_invert_direction').value === 'true',
    fr_zscore_filter_enabled: document.getElementById('fr_zscore_filter_enabled').value === 'true',
    fr_zscore_resolution: getResolutionField('fr_zscore_resolution'),
    fr_zscore_lookback: parseInt(document.getElementById('fr_zscore_lookback').value),
    fr_zscore_smooth: parseInt(document.getElementById('fr_zscore_smooth').value),
    fr_sl_enabled: document.getElementById('fr_sl_enabled').value === 'true',
    fr_sl_manual_usd: parseFloat(document.getElementById('fr_sl_manual_usd').value),
    fr_sl_cooldown_seconds: parseFloat(document.getElementById('fr_sl_cooldown_seconds').value),
    cd_resolution: getResolutionField('cd_resolution'),
    cd_threshold: parseFloat(document.getElementById('cd_threshold').value),
    cd_rejection_mult: parseFloat(document.getElementById('cd_rejection_mult').value),
    cd_direction_mode: document.getElementById('cd_direction_mode').value,
    cd_zscore_filter_enabled: document.getElementById('cd_zscore_filter_enabled').value === 'true',
    cd_zscore_resolution: getResolutionField('cd_zscore_resolution'),
    cd_zscore_lookback: parseInt(document.getElementById('cd_zscore_lookback').value),
    cd_zscore_smooth: parseInt(document.getElementById('cd_zscore_smooth').value),
    cd_rsi_filter_enabled: document.getElementById('cd_rsi_filter_enabled').value === 'true',
    cd_rsi_length: parseInt(document.getElementById('cd_rsi_length').value),
    cd_rsi_midline: parseFloat(document.getElementById('cd_rsi_midline').value),
    cd_adx_filter_enabled: document.getElementById('cd_adx_filter_enabled').value === 'true',
    cd_adx_length: parseInt(document.getElementById('cd_adx_length').value),
    cd_adx_threshold: parseFloat(document.getElementById('cd_adx_threshold').value),
    cd_sl_enabled: document.getElementById('cd_sl_enabled').value === 'true',
    cd_sl_manual_usd: parseFloat(document.getElementById('cd_sl_manual_usd').value),
    cd_sl_cooldown_seconds: parseFloat(document.getElementById('cd_sl_cooldown_seconds').value),
    cd_use_heikin_ashi: document.getElementById('cd_use_heikin_ashi').value === 'true',
    grid_direction_mode: document.getElementById('grid_direction_mode').value,
    grid_mode: document.getElementById('grid_mode').value,
    grid_step_pct: parseFloat(document.getElementById('grid_step_pct').value),
    tp_step_pct: parseFloat(document.getElementById('tp_step_pct').value),
    grid_step_usd: parseFloat(document.getElementById('grid_step_usd').value),
    tp_step_usd: parseFloat(document.getElementById('tp_step_usd').value),
    max_nachkauf: parseInt(document.getElementById('max_nachkauf').value),
    grid_sl_enabled: document.getElementById('grid_sl_enabled').value === 'true',
    grid_sl_manual_usd: parseFloat(document.getElementById('grid_sl_manual_usd').value),
    grid_anchor_follow_enabled: document.getElementById('grid_anchor_follow_enabled').value === 'true',
    grid_anchor_follow_pct: parseFloat(document.getElementById('grid_anchor_follow_pct').value),
    dry_run: document.getElementById('dry_run').value === 'true',
    binance_market_type: document.getElementById('binance_market_type').value,
    auto_reverse: document.getElementById('auto_reverse').value === 'true',
  };
}

document.getElementById('config-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const payload = buildConfigPayload();
  try {
    const res = await fetch(`/api/config?symbol=${currentSymbol}`, { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(payload) });
    const data = await res.json().catch(() => null);
    if (!res.ok || !data || data.success !== true) {
      showToast(`❌ Speichern fehlgeschlagen (${res.status}): ${data?.error || 'unbekannter Fehler'}`);
      return;
    }
    window.formTouched = false;
    showToast(`✅ Gespeichert für ${currentSymbol} (${data.config.entry_mode})!`);
  } catch (e) {
    showToast(`❌ Netzwerkfehler beim Speichern: ${e}`);
  }
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

// GENERISCHER Schutz vor dem 3-Sekunden-Refresh: JEDES Formularfeld (alle <select class="cfg">
// UND alle Zahlen-/Text-Eingabefelder mit id) setzt formTouched, sobald es beruehrt wird - vorher
// gab es dafuer nur eine HANDGEPFLEGTE Liste (~100 Felder per 'input'-Event), die weder die ~48
// Dropdown-Felder (die 'change' statt 'input' feuern) noch neu hinzugekommene Felder wie die
// "Eigene Minuten"-Eingabe abdeckte - beim Tippen/Auswaehlen in einem nicht gelisteten Feld hat
// der naechste Refresh (alle 3s) die Eingabe deshalb einfach wieder ueberschrieben, bevor
// gespeichert werden konnte. Jetzt: JEDES Feld auf der Seite mit id ist automatisch geschuetzt.
document.querySelectorAll('select.cfg, input[id]').forEach(el => {
  const markTouched = (e) => {
    window.formTouched = true;
    if (typeof e.target.value === 'string' && e.target.value.includes(',')) {
      e.target.value = e.target.value.replace(',', '.');
    }
  };
  el.addEventListener('input', markTouched);
  el.addEventListener('change', markTouched);
});

async function loadGlobalSettings() {
  try {
    const res = await fetch('/api/global_settings');
    const data = await res.json();
    document.getElementById('toggle-scalp-board-global').checked = !!data.scalp_board_enabled;
    document.getElementById('toggle-copytrading-global').checked = !!data.copytrading_enabled;
  } catch (e) {
    console.error('Globale Einstellungen konnten nicht geladen werden:', e);
  }
}
document.getElementById('toggle-scalp-board-global').addEventListener('change', async (e) => {
  await fetch('/api/global_settings', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({scalp_board_enabled: e.target.checked}) });
  showToast(e.target.checked ? '✅ Scalp-Details global AN' : '⏸️ Scalp-Details global AUS (spart Ressourcen)');
});
document.getElementById('toggle-copytrading-global').addEventListener('change', async (e) => {
  await fetch('/api/global_settings', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({copytrading_enabled: e.target.checked}) });
  showToast(e.target.checked ? '✅ Copytrading global AN' : '⏸️ Copytrading global AUS');
});

(async () => {
  await loadSymbols();
  await loadGlobalSettings();
  refresh();
  setInterval(refresh, 6000);
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
        "oms_funding_ok": st.get("oms_funding_ok"), "oms_rsi_ok": st.get("oms_rsi_ok"), "oms_rsi": st.get("oms_rsi"),
        "oms_oi_ok": st.get("oms_oi_ok"), "oms_oi_score": st.get("oms_oi_score"), "oms_open_interest": st.get("oms_open_interest"),
        "oms_liq_ok": st.get("oms_liq_ok"), "oms_liq_ratio": st.get("oms_liq_ratio"), "oms_liq_count": st.get("oms_liq_count"),
        "scalp_board": st.get("scalp_board", {}),
        "quad_stoch_history": st.get("quad_stoch_history", [])[-100:],
        "oms_cvd_ratio": st.get("oms_cvd_ratio"), "oms_funding_rate": st.get("oms_funding_rate"),
        "oms_tp1_done": st.get("oms_tp1_done"), "oms_trail_price": st.get("oms_trail_price"),
        "oms_dca_count": st.get("oms_dca_count"),
        "oms_price_history": [[round(ts, 1), price] for ts, price in st.get("oms_price_history", [])[-100:]],
        "oms_markers": st.get("oms_markers", [])[-30:],
        "oms_obi_history": st.get("oms_obi_history", [])[-100:],
        "obi_history": st.get("obi_history", [])[-100:],
        "obi_spread_pct": st.get("obi_spread_pct"), "obi_recent_vol_pct": st.get("obi_recent_vol_pct"),
        "fib": st.get("fib"),
        "ht_direction": st.get("ht_direction"), "ht_sl_price": st.get("ht_sl_price"),
        "ht_tp1_price": st.get("ht_tp1_price"), "ht_tp2_price": st.get("ht_tp2_price"), "ht_tp3_price": st.get("ht_tp3_price"),
        "ht_tp1_done": st.get("ht_tp1_done"), "ht_tp2_done": st.get("ht_tp2_done"),
        "da_direction": st.get("da_direction"), "da_sl_price": st.get("da_sl_price"), "da_tp_price": st.get("da_tp_price"),
        "es_direction": st.get("es_direction"), "es_sensitivity_last": st.get("es_sensitivity_last"),
        "es_sl_price": st.get("es_sl_price"), "es_tp1_price": st.get("es_tp1_price"),
        "es_tp2_price": st.get("es_tp2_price"), "es_tp3_price": st.get("es_tp3_price"),
        "es_tp1_done": st.get("es_tp1_done"), "es_tp2_done": st.get("es_tp2_done"),
        "cp_last_signal": st.get("cp_last_signal"), "cp_sl_price": st.get("cp_sl_price"),
        "cp_tp_price": st.get("cp_tp_price"), "cp_breakeven_done": st.get("cp_breakeven_done"),
        "mo7_last_value": st.get("mo7_last_value"), "mo7_sl_price": st.get("mo7_sl_price"),
        "mo7_tp_price": st.get("mo7_tp_price"),
        "utb_last_hull_green": st.get("utb_last_hull_green"),
        "utb_sl_price": st.get("utb_sl_price"),
        "fr_sl_price": st.get("fr_sl_price"),
        "cd_sl_price": st.get("cd_sl_price"),
        "utb_trend_pct_last": st.get("utb_trend_pct_last"),
        "wtc_last_wt1": st.get("wtc_last_wt1"), "wtc_last_wt2": st.get("wtc_last_wt2"),
        "wtc_sl_price": st.get("wtc_sl_price"), "wtc_tp_price": st.get("wtc_tp_price"),
        "pk_sl_price": st.get("pk_sl_price"), "pk_tp_price": st.get("pk_tp_price"),
        "pk_trail_active": st.get("pk_trail_active"), "pk_trail_best_price": st.get("pk_trail_best_price"),
        "pk_trend_pct_last": st.get("pk_trend_pct_last"),
        "binance_1s_buffer_size": len(st.get("binance_1s_buffer", [])),
        "binance_1s_buffer_span_sec": (
            (st["binance_1s_buffer"][-1]["ts"] - st["binance_1s_buffer"][0]["ts"]) // 1000
            if len(st.get("binance_1s_buffer", [])) > 1 else 0
        ),
        "local_1s_buffer_size": len(st.get("local_1s_buffer", [])),
        "config": cfg,
        "stats": {"trades": stats["trades"], "win_rate_pct": win_rate, "total_pnl_usd": round(stats["total_pnl_usd"], 3)},
        "trade_log": st["trade_log"][-20:],
        "price_history": st["price_history"][-100:],
    }
    return web.json_response(payload)


async def handle_config_update(request):
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    body = await request.json()
    cfg = BOTS[symbol]["config"]
    for key in ["margin", "leverage", "entry_mode", "grid_mode", "grid_direction_mode", "grid_step_pct", "tp_step_pct",
                "grid_step_usd", "tp_step_usd", "max_nachkauf", "grid_sl_enabled", "grid_sl_manual_usd",
                "grid_anchor_follow_enabled", "grid_anchor_follow_pct", "dry_run", "auto_reverse", "binance_market_type",
                "obi_threshold", "obi_mode", "obi_long_threshold", "obi_short_threshold", "obi_reversal_min_bounce", "obi_instant_reset_ratio", "obi_window_fast_seconds", "obi_window_medium_seconds", "obi_window_slow_seconds", "obi_levels", "obi_depth_weighting_enabled", "obi_use_median", "obi_min_liquidity", "obi_breakeven_enabled", "obi_breakeven_trigger_ratio", "obi_breakeven_lock_usd", "obi_breakeven_lock_pct", "obi_tp_sl_mode", "obi_tp_pct", "obi_sl_pct", "obi_tp_usd", "obi_sl_usd",
                "obi_cooldown_seconds", "obi_trend_filter", "obi_trend_ema_length",
                "obi_spread_filter_enabled", "obi_max_spread_pct",
                "obi_vol_filter_enabled", "obi_vol_window_seconds", "obi_vol_min_pct", "obi_vol_max_pct",
                "oms_levels", "oms_obi_threshold", "oms_window_fast_seconds", "oms_window_medium_seconds",
                "oms_window_slow_seconds", "oms_cvd_confirm_enabled", "oms_cvd_window_seconds", "oms_cvd_min_ratio",
                "oms_funding_filter_enabled", "oms_funding_max_abs", "oms_cooldown_seconds",
                "oms_tp1_usd", "oms_exit_mode", "oms_tp1_close_pct", "oms_sl_usd", "oms_trail_distance_usd",
                "oms_dca_enabled", "oms_dca_max_entries", "oms_dca_size_fraction", "oms_dca_min_pullback_usd",
                "oms_reverse_on_signal",
                "oms_rsi_filter_enabled", "oms_rsi_resolution", "oms_rsi_period", "oms_rsi_midline",
                "oms_oi_filter_enabled", "oms_oi_window_seconds", "oms_oi_min_change_pct", "oms_oi_min_score",
                "oms_liq_filter_enabled", "oms_liq_window_seconds", "oms_liq_min_ratio",
                "fib_resolution", "fib_lookback_candles", "fib_entry1_level", "fib_entry2_level",
                "fib_tp1_level", "fib_tp1_close_pct", "fib_tp2_level", "fib_sl_level", "fib_cooldown_seconds",
                "ht_resolution", "ht_amplitude", "ht_channel_deviation", "ht_base_risk_mult",
                "ht_entry_trigger", "ht_exit_trigger", "ht_invert_direction",
                "ht_tp_enabled", "ht_tp1_close_pct", "ht_tp2_close_pct", "ht_sl_enabled", "ht_sl_cooldown_seconds",
                "da_resolution", "da_atr_period", "da_sensitivity", "da_sma_period", "da_ema_trend_period",
                "da_signal_mode", "da_entry_trigger", "da_exit_trigger", "da_invert_direction",
                "da_sl_enabled", "da_tp_enabled", "da_risk_atr_period", "da_risk_mult", "da_tp_rr", "da_sl_cooldown_seconds",
                "da_use_heikin_ashi",
                "es_resolution", "es_atr_period", "es_auto_sensitivity", "es_sensitivity",
                "es_vol_period", "es_vol_ma_len", "es_entry_trigger", "es_exit_trigger", "es_invert_direction",
                "es_risk_atr_period", "es_risk_mult", "es_tp1_close_pct", "es_tp2_close_pct",
                "es_tp1_rr", "es_tp2_rr", "es_tp3_rr", "es_sl_cooldown_seconds", "es_reenter_on_flip",
                "es_sl_enabled", "es_sl_mode", "es_sl_manual_usd", "es_tp_enabled", "es_tp_mode", "es_tp_manual_usd",
                "es_breakeven_pct_enabled", "es_breakeven_trigger_pct",
                "cp_resolution", "cp_signal_source", "cp_three_line_strict", "cp_engulfing_strict", "cp_direction_mode",
                "cp_flip_exit_enabled", "cp_risk_atr_period", "cp_risk_mult", "cp_tp_rr",
                "cp_sl_cooldown_seconds", "cp_sl_enabled", "cp_sl_mode", "cp_sl_manual_usd",
                "cp_tp_enabled", "cp_tp_mode", "cp_tp_manual_usd",
                "cp_breakeven_enabled", "cp_breakeven_trigger_mult",
                "mo7_resolution", "mo7_entry_mode", "mo7_buy_threshold", "mo7_sell_threshold",
                "mo7_sum_low", "mo7_sum_high", "mo7_trend_threshold", "mo7_trend_deadband",
                "mo7_direction_mode", "mo7_flip_exit_enabled",
                "mo7_sl_enabled", "mo7_sl_manual_usd", "mo7_tp_enabled", "mo7_tp_manual_usd",
                "mo7_sl_cooldown_seconds",
                "utb_resolution", "utb_atr_period", "utb_sensitivity", "utb_heikin_ashi",
                "utb_hull_period", "utb_flip_trigger", "utb_direction_mode",
                "utb_sl_enabled", "utb_sl_manual_usd", "utb_sl_cooldown_seconds",
                "utb_mtf_filter_enabled", "utb_mtf_tf1", "utb_mtf_tf2", "utb_mtf_tf3",
                "utb_mtf_fast_len", "utb_mtf_slow_len", "utb_mtf_atr_len",
                "utb_mtf_long_threshold", "utb_mtf_short_threshold",
                "wtc_resolution", "wtc_channel_len", "wtc_average_len", "wtc_ma_len",
                "wtc_require_zone", "wtc_os_level", "wtc_ob_level", "wtc_direction_mode",
                "wtc_always_in_market", "wtc_flip_exit_enabled", "wtc_sl_enabled",
                "wtc_sl_manual_usd", "wtc_tp_enabled", "wtc_tp_manual_usd", "wtc_sl_cooldown_seconds",
                "pk_resolution", "pk_sensitivity", "pk_atr_period", "pk_sma_period", "pk_direction_mode",
                "pk_exit_mode", "pk_sl_enabled", "pk_sl_manual_usd", "pk_tp_enabled", "pk_tp_manual_usd",
                "pk_sl_cooldown_seconds", "pk_trailing_enabled", "pk_trailing_activation_pct", "pk_trailing_step_pct",
                "pk_mtf_filter_enabled", "pk_mtf_tf1", "pk_mtf_tf2", "pk_mtf_tf3", "pk_mtf_fast_len", "pk_mtf_slow_len",
                "pk_mtf_atr_len", "pk_mtf_long_threshold", "pk_mtf_short_threshold",
                "fr_resolution", "fr_periods", "fr_direction_mode", "fr_invert_direction",
                "fr_zscore_filter_enabled", "fr_zscore_resolution", "fr_zscore_lookback", "fr_zscore_smooth",
                "fr_sl_enabled", "fr_sl_manual_usd", "fr_sl_cooldown_seconds",
                "cd_resolution", "cd_threshold", "cd_rejection_mult", "cd_direction_mode",
                "cd_zscore_filter_enabled", "cd_zscore_resolution", "cd_zscore_lookback", "cd_zscore_smooth",
                "cd_rsi_filter_enabled", "cd_rsi_length", "cd_rsi_midline",
                "cd_adx_filter_enabled", "cd_adx_length", "cd_adx_threshold",
                "cd_sl_enabled", "cd_sl_manual_usd", "cd_sl_cooldown_seconds", "cd_use_heikin_ashi",
                "quad_stoch_resolution"]:
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
        await save_bot_configs()  # sonst geht bot_active bei Neustart/Redeploy verloren
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
    try:
        exclude_top_n = max(0, min(50, int(body.get("exclude_top_n", 1))))
    except (TypeError, ValueError):
        exclude_top_n = 1
    cfg = dict(BOTS[symbol]["config"])  # Kopie - Backtest darf die Live-Config nicht veraendern
    overrides = body.get("config")
    if isinstance(overrides, dict):
        # Nur bekannte Config-Felder uebernehmen (das Formular schickt ohnehin nur solche) -
        # so testet der Backtest immer das, was gerade im Formular steht, auch wenn noch
        # nicht auf "Speichern" geklickt wurde.
        cfg.update({k: v for k, v in overrides.items() if k in cfg})
    entry_mode = cfg["entry_mode"]
    result = await run_backtest(symbol, entry_mode, cfg, days, exclude_top_n)
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


async def handle_da_sweep(request):
    """'Monte-Carlo'-Parametersweep fuer Diamond Algo: testet einen Bereich von ATR-Periode
    und Sensitivity gegeneinander und gibt die besten/schlechtesten Kombinationen zurueck."""
    from strategies import run_da_param_sweep
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    body = await request.json()
    try:
        days = max(1, min(365, int(body.get("days", 30))))
        atr_period_min = max(1, int(body.get("atr_period_min", 5)))
        atr_period_max = max(atr_period_min, int(body.get("atr_period_max", 20)))
        atr_period_step = max(1, int(body.get("atr_period_step", 1)))
        sensitivity_min = max(0.01, float(body.get("sensitivity_min", 1.0)))
        sensitivity_max = max(sensitivity_min, float(body.get("sensitivity_max", 5.0)))
        sensitivity_step = max(0.01, float(body.get("sensitivity_step", 0.5)))
    except (TypeError, ValueError):
        return web.json_response({"error": "Ungültige Zahlenwerte im Sweep-Bereich."}, status=400)

    cfg = dict(BOTS[symbol]["config"])
    overrides = body.get("config")
    if isinstance(overrides, dict):
        cfg.update({k: v for k, v in overrides.items() if k in cfg})

    result = await run_da_param_sweep(symbol, cfg, days, atr_period_min, atr_period_max, atr_period_step,
                                       sensitivity_min, sensitivity_max, sensitivity_step)
    return web.json_response(result)


async def handle_es_sensitivity_sweep(request):
    """'Monte-Carlo'-Parametersweep fuer ELTE Smart, NUR ueber die manuelle Sensitivity (2
    Nachkommastellen wie im Original-Skript)."""
    from strategies import run_es_sensitivity_sweep
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    body = await request.json()
    try:
        days = max(1, min(365, int(body.get("days", 30))))
        sens_min = max(0.01, round(float(body.get("sens_min", 0.11)), 2))
        sens_max = max(sens_min, round(float(body.get("sens_max", 5.0)), 2))
        sens_step = max(0.01, round(float(body.get("sens_step", 0.01)), 2))
    except (TypeError, ValueError):
        return web.json_response({"error": "Ungültige Zahlenwerte im Sweep-Bereich."}, status=400)

    cfg = dict(BOTS[symbol]["config"])
    overrides = body.get("config")
    if isinstance(overrides, dict):
        cfg.update({k: v for k, v in overrides.items() if k in cfg})

    result = await run_es_sensitivity_sweep(symbol, cfg, days, sens_min, sens_max, sens_step)
    return web.json_response(result)


async def handle_pk_sensitivity_sweep(request):
    """'Monte-Carlo'-Parametersweep fuer Pieki Algo, NUR ueber die Sensitivity (2 Nachkommastellen
    wie im Original-Pine-Script, Schritt 0.01)."""
    from strategies import run_pk_sensitivity_sweep
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    body = await request.json()
    try:
        days = max(1, min(365, int(body.get("days", 30))))
        sens_min = max(0.01, round(float(body.get("sens_min", 0.5)), 2))
        sens_max = max(sens_min, round(float(body.get("sens_max", 8.0)), 2))
        sens_step = max(0.01, round(float(body.get("sens_step", 0.01)), 2))
    except (TypeError, ValueError):
        return web.json_response({"error": "Ungültige Zahlenwerte im Sweep-Bereich."}, status=400)

    cfg = dict(BOTS[symbol]["config"])
    overrides = body.get("config")
    if isinstance(overrides, dict):
        cfg.update({k: v for k, v in overrides.items() if k in cfg})

    result = await run_pk_sensitivity_sweep(symbol, cfg, days, sens_min, sens_max, sens_step)
    return web.json_response(result)


async def handle_mo7_sum_sweep(request):
    """'Monte-Carlo'-Parametersweep fuer den MO7 'five_candle_sum'-Einstiegsmodus: testet einen
    Bereich von Long-/Short-Summenschwellen gegeneinander (der MO7-Score selbst wird nur EINMAL
    berechnet und fuer alle Kombinationen wiederverwendet)."""
    from strategies import run_mo7_sum_sweep
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    body = await request.json()
    try:
        days = max(1, min(365, int(body.get("days", 30))))
        sum_low_min = max(0.0, float(body.get("sum_low_min", 20)))
        sum_low_max = max(sum_low_min, float(body.get("sum_low_max", 200)))
        sum_low_step = max(1.0, float(body.get("sum_low_step", 20)))
        sum_high_min = max(0.0, float(body.get("sum_high_min", 300)))
        sum_high_max = max(sum_high_min, float(body.get("sum_high_max", 480)))
        sum_high_step = max(1.0, float(body.get("sum_high_step", 20)))
    except (TypeError, ValueError):
        return web.json_response({"error": "Ungültige Zahlenwerte im Sweep-Bereich."}, status=400)
    try:
        exclude_top_n = max(0, min(50, int(body.get("exclude_top_n", 1))))
    except (TypeError, ValueError):
        exclude_top_n = 1

    cfg = dict(BOTS[symbol]["config"])
    overrides = body.get("config")
    if isinstance(overrides, dict):
        cfg.update({k: v for k, v in overrides.items() if k in cfg})

    result = await run_mo7_sum_sweep(symbol, cfg, days, sum_low_min, sum_low_max, sum_low_step,
                                      sum_high_min, sum_high_max, sum_high_step, exclude_top_n)
    return web.json_response(result)


async def handle_utb_param_sweep(request):
    """'Monte-Carlo'-Parametersweep fuer UT Bot + Hull Flip: testet einen Bereich von ATR-Periode
    und Sensitivity gegeneinander."""
    from strategies import run_utb_param_sweep
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    body = await request.json()
    try:
        days = max(1, min(365, int(body.get("days", 30))))
        atr_period_min = max(1, int(body.get("atr_period_min", 1)))
        atr_period_max = max(atr_period_min, int(body.get("atr_period_max", 20)))
        atr_period_step = max(1, int(body.get("atr_period_step", 1)))
        sensitivity_min = max(0.01, float(body.get("sensitivity_min", 0.5)))
        sensitivity_max = max(sensitivity_min, float(body.get("sensitivity_max", 5.0)))
        sensitivity_step = max(0.01, float(body.get("sensitivity_step", 0.5)))
        long_threshold_min = float(body.get("long_threshold_min", 0.5))
        long_threshold_max = max(long_threshold_min, float(body.get("long_threshold_max", 0.5)))
        long_threshold_step = max(0.01, float(body.get("long_threshold_step", 0.5)))
        short_threshold_min = float(body.get("short_threshold_min", -0.5))
        short_threshold_max = max(short_threshold_min, float(body.get("short_threshold_max", -0.5)))
        short_threshold_step = max(0.01, float(body.get("short_threshold_step", 0.5)))
    except (TypeError, ValueError):
        return web.json_response({"error": "Ungültige Zahlenwerte im Sweep-Bereich."}, status=400)
    try:
        exclude_top_n = max(0, min(50, int(body.get("exclude_top_n", 1))))
    except (TypeError, ValueError):
        exclude_top_n = 1

    cfg = dict(BOTS[symbol]["config"])
    overrides = body.get("config")
    if isinstance(overrides, dict):
        cfg.update({k: v for k, v in overrides.items() if k in cfg})

    result = await run_utb_param_sweep(symbol, cfg, days, atr_period_min, atr_period_max, atr_period_step,
                                        sensitivity_min, sensitivity_max, sensitivity_step, exclude_top_n,
                                        long_threshold_min, long_threshold_max, long_threshold_step,
                                        short_threshold_min, short_threshold_max, short_threshold_step)
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


