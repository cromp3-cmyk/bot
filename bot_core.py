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
    "LIT": 120,  # Lighters eigener Token - market_id per /api/v1/orderBooks bestätigt (2026-09-14)
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
    "LIT": 100,
}
PRICE_DECIMALS_MAP = {
    "ETH": 2, "BTC": 1, "SOL": 3, "DOGE": 6, "XRP": 6, "LINK": 5, "AVAX": 4,
    "NEAR": 5, "DOT": 5, "GRAM": 5, "SUI": 5, "BNB": 4, "UNI": 4, "APT": 4,
    "ADA": 5, "TRX": 5, "LTC": 3, "BCH": 3, "HBAR": 5, "ICP": 4, "HYPE": 4,
    "EURUSD": 5, "GBPUSD": 5, "USDJPY": 3, "USDCHF": 5, "USDCAD": 5,
    "AUDUSD": 5, "NZDUSD": 5, "USDKRW": 2, "XAU": 2, "XAG": 4, "WTI": 3,
    "LIT": 4,
}
MIN_BASE_AMOUNT_MAP = {
    "ETH": 0.005, "BTC": 0.0001, "SOL": 0.1, "DOGE": 100.0, "XRP": 7.0, "LINK": 1.0, "AVAX": 1.0,
    "NEAR": 4.0, "DOT": 9.5, "GRAM": 5.0, "SUI": 10.0, "BNB": 0.02, "UNI": 2.0, "APT": 10.0,
    "ADA": 45.0, "TRX": 25.0, "LTC": 0.15, "BCH": 0.035, "HBAR": 100.0, "ICP": 3.5, "HYPE": 0.15,
    "EURUSD": 6.5, "GBPUSD": 5.5, "USDJPY": 0.05, "USDCHF": 8.0, "USDCAD": 5.5,
    "AUDUSD": 10.0, "NZDUSD": 10.0, "USDKRW": 0.005, "XAU": 0.002, "XAG": 0.15, "WTI": 0.1,
    "LIT": 3.5,
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
        "entry_mode": os.getenv("ENTRY_MODE", "grid"),  # "grid", "grid_v2", "grid_scalp", "ab_breakout" (weitere folgen bei Bedarf)
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
        "grid_sl_cooldown_min": float(os.getenv("GRID_SL_COOLDOWN_MIN", "0")),  # Pause nach Grid-SL; 0 = aus. Ohne das baut der Bot im Trend sofort dieselbe Position wieder auf.
        # ===== Grid-Scalp (Maker-Only, entry_mode "grid_scalp") =====
        # gs_step_notional_usd hat eine Obergrenze durch den SPREAD, nicht durchs Risiko:
        # Kosten pro Round-Trip = Notional x Spread. Bei fixem 1$-TP frisst ein zu grosses
        # Notional das Ziel komplett auf. probe_grid_scalp() misst den Spread und rechnet
        # den passenden Wert aus - der Default hier ist nur eine Schaetzung.
        "gs_step_notional_usd": float(os.getenv("GS_STEP_NOTIONAL_USD", "1000")),
        "gs_max_levels": int(os.getenv("GS_MAX_LEVELS", "5")),
        "gs_step_pct": float(os.getenv("GS_STEP_PCT", "0.10")),
        "gs_tp_usd": float(os.getenv("GS_TP_USD", "1.0")),
        "gs_flatten_usd": float(os.getenv("GS_FLATTEN_USD", "25.0")),
        "gs_cooldown_min": float(os.getenv("GS_COOLDOWN_MIN", "30")),
        "gs_anchor_follow_pct": float(os.getenv("GS_ANCHOR_FOLLOW_PCT", "1.0")),
        "gs_requote_ticks": int(os.getenv("GS_REQUOTE_TICKS", "2")),
        "gs_max_open_orders": int(os.getenv("GS_MAX_OPEN_ORDERS", "8")),
        "gs_poll_seconds": float(os.getenv("GS_POLL_SECONDS", "2.0")),
        "bot_active": True,
        "auto_reverse": os.getenv("AUTO_REVERSE", "true").lower() == "true",
        # ===== Grid 2 (zweite, unabhaengige Grid-Strategie mit Revisit- und Verdopplungs-Option) =====
        "g2_mode": os.getenv("G2_MODE", "pct"),  # "pct" oder "usd"
        "g2_direction_mode": os.getenv("G2_DIRECTION_MODE", "both"),  # "both" | "long_only" | "short_only" | "smart"
        "g2_step_pct": float(os.getenv("G2_STEP_PCT", "0.25")),
        "g2_tp_step_pct": float(os.getenv("G2_TP_STEP_PCT", "0.25")),
        "g2_step_usd": float(os.getenv("G2_STEP_USD", "150")),
        "g2_tp_step_usd": float(os.getenv("G2_TP_STEP_USD", "150")),
        "g2_max_nachkauf": int(os.getenv("G2_MAX_NACHKAUF", "5")),
        "g2_sl_enabled": os.getenv("G2_SL_ENABLED", "false").lower() == "true",
        "g2_sl_mode": os.getenv("G2_SL_MODE", "usd"),  # "usd" oder "pct" - beide schliessen die GESAMTE Position
        "g2_sl_manual_usd": float(os.getenv("G2_SL_MANUAL_USD", "20.0")),
        "g2_sl_pct": float(os.getenv("G2_SL_PCT", "5.0")),  # Notausstieg als % vom Ø-Einstieg, falls g2_sl_mode="pct"
        "g2_anchor_follow_enabled": os.getenv("G2_ANCHOR_FOLLOW_ENABLED", "false").lower() == "true",
        "g2_anchor_follow_pct": float(os.getenv("G2_ANCHOR_FOLLOW_PCT", "1.0")),
        "g2_auto_reverse": os.getenv("G2_AUTO_REVERSE", "true").lower() == "true",
        "g2_revisit_enabled": os.getenv("G2_REVISIT_ENABLED", "false").lower() == "true",  # Nachkauf-Schwelle bleibt FEST am Anker-Level statt sich mit jedem Nachkauf weiter zu verschieben - kann dadurch mehrfach an derselben Kursmarke ausloesen
        "g2_revisit_rearm_pct": float(os.getenv("G2_REVISIT_REARM_PCT", "50.0")),  # Mindest-Erholung (in % der Grid-Stufe) bevor ein Level wieder "scharf" wird - schuetzt vor Ausloesen durch reines Markt-Rauschen
        "g2_double_enabled": os.getenv("G2_DOUBLE_ENABLED", "false").lower() == "true",  # jede Nachkauf-Stufe verdoppelt die Positionsgroesse der vorherigen (1x, 2x, 4x, 8x, ...)
        "g2_size_multiplier": float(os.getenv("G2_SIZE_MULTIPLIER", "1.0")),  # Alternative zu g2_double_enabled: frei waehlbarer Faktor statt fixer Verdopplung (1.0 = aus, greift nur wenn g2_double_enabled=false)
        "g2_deviation_multiplier": float(os.getenv("G2_DEVIATION_MULTIPLIER", "1.0")),  # jeder weitere Nachkauf braucht einen groesseren Abstand als der vorherige (1.0 = fix wie bisher)
        # HalfTrend (portiert aus "HalfTrend Long/Short Signal Engine [BigBeluga]", Basis:
        # everget's HalfTrend-Indikator): ATR-Periode ist im Original fest auf 100. Channel-
        # Deviation und Base-Risk-Multiplikator sind hier (anders als im rein optischen Original)
        # echte SL-/TP-Abstands-Multiplikatoren (in ATR2-Vielfachen), damit beide Parameter
        # tatsaechlich das Backtest-/Sweep-Ergebnis beeinflussen:
        # Al-Shatri Breakout (portiert aus Pine-Script "Al-Shatri | Arabic Breakout • Entry &
        # 3 Targets (Presets)"): Range-Breakout + EMA-Trend + RSI + optionaler Volumen-Filter.
        # Preset uebernimmt die Original-Presets 1:1, "custom" nutzt die ab_*-Werte direkt.
        # Ausstieg: Wechsel-System (Gegen-Signal schliesst die Position und oeffnet die Gegenrichtung,
        # immer im Markt) + optionaler fester Dollar-SL. KEIN ATR-SL, keine Targets/Teilverkaeufe.
        "ab_resolution": os.getenv("AB_RESOLUTION", "1m"),
        "ab_preset": os.getenv("AB_PRESET", "intraday"),  # scalping/intraday/swing/custom
        "ab_lookback": int(os.getenv("AB_LOOKBACK", "20")),
        "ab_fast_len": int(os.getenv("AB_FAST_LEN", "20")),
        "ab_slow_len": int(os.getenv("AB_SLOW_LEN", "50")),
        "ab_rsi_len": int(os.getenv("AB_RSI_LEN", "14")),
        "ab_rsi_gate": float(os.getenv("AB_RSI_GATE", "55")),
        "ab_use_volume": os.getenv("AB_USE_VOLUME", "false").lower() == "true",
        "ab_vol_mult": float(os.getenv("AB_VOL_MULT", "1.5")),
        "ab_atr_len": int(os.getenv("AB_ATR_LEN", "14")),
        "ab_direction_mode": os.getenv("AB_DIRECTION_MODE", "both"),
        "ab_sl_cooldown_seconds": float(os.getenv("AB_SL_COOLDOWN_SECONDS", "30")),
        # Signal (Range/EMA/RSI, plus der ATR fuer den Plan-Modus) auf Heikin-Ashi-Kerzen statt
        # normalen Kerzen berechnen - wie bei Diamond Algo/UT Bot/Candle DNA. Ein-/Ausstieg loest
        # weiterhin am ECHTEN Marktpreis aus.
        "ab_use_heikin_ashi": os.getenv("AB_USE_HEIKIN_ASHI", "false").lower() == "true",

        # ================= RSI Signal =================
        # Reine Handelsidee: RSI < oversold -> long, RSI > overbought -> short. Ausstieg identisch
        # zu Al-Shatris Wechsel-System (Gegen-Signal dreht die Position, optionaler $-SL,
        # optionales "SL auf Einstieg"). Trendfilter kommen aus dem generischen Filter-Baukasten -
        # SuperTrend (eigene, meist hoehere Zeiteinheit), ADX (Trendstaerke/-richtung), MACD
        # (bullisch/baerisch) - jeder einzeln zu- und abschaltbar.
        "rsi_resolution": os.getenv("RSI_RESOLUTION", "5m"),
        "rsi_length": int(os.getenv("RSI_LENGTH", "14")),
        "rsi_oversold": float(os.getenv("RSI_OVERSOLD", "30")),
        "rsi_overbought": float(os.getenv("RSI_OVERBOUGHT", "70")),
        "rsi_direction_mode": os.getenv("RSI_DIRECTION_MODE", "both"),
        "rsi_sl_enabled": os.getenv("RSI_SL_ENABLED", "true").lower() == "true",
        "rsi_sl_manual_usd": float(os.getenv("RSI_SL_MANUAL_USD", "5.0")),
        "rsi_be_enabled": os.getenv("RSI_BE_ENABLED", "false").lower() == "true",
        "rsi_be_trigger_usd": float(os.getenv("RSI_BE_TRIGGER_USD", "5.0")),
        "rsi_tp_enabled": os.getenv("RSI_TP_ENABLED", "false").lower() == "true",
        "rsi_tp_manual_usd": float(os.getenv("RSI_TP_MANUAL_USD", "10.0")),
        "rsi_sl_cooldown_seconds": float(os.getenv("RSI_SL_COOLDOWN_SECONDS", "30")),
        "rsi_supertrend_filter_enabled": os.getenv("RSI_SUPERTREND_FILTER_ENABLED", "false").lower() == "true",
        "rsi_supertrend_filter_resolution": os.getenv("RSI_SUPERTREND_FILTER_RESOLUTION", "15m"),
        "rsi_supertrend_filter_multiplier": float(os.getenv("RSI_SUPERTREND_FILTER_MULTIPLIER", "3.0")),
        "rsi_supertrend_filter_atr_period": int(os.getenv("RSI_SUPERTREND_FILTER_ATR_PERIOD", "10")),
        "rsi_adx_filter_enabled": os.getenv("RSI_ADX_FILTER_ENABLED", "false").lower() == "true",
        "rsi_adx_filter_length": int(os.getenv("RSI_ADX_FILTER_LENGTH", "14")),
        "rsi_adx_filter_threshold": float(os.getenv("RSI_ADX_FILTER_THRESHOLD", "20")),
        "rsi_adx_filter_directional": os.getenv("RSI_ADX_FILTER_DIRECTIONAL", "true").lower() == "true",
        "rsi_macd_filter_enabled": os.getenv("RSI_MACD_FILTER_ENABLED", "false").lower() == "true",
        "rsi_macd_filter_fast": int(os.getenv("RSI_MACD_FILTER_FAST", "12")),
        "rsi_macd_filter_slow": int(os.getenv("RSI_MACD_FILTER_SLOW", "26")),
        "rsi_macd_filter_signal": int(os.getenv("RSI_MACD_FILTER_SIGNAL", "9")),
        # Ausstiegs-Modus: "flip" = Wechsel bei Gegen-Signal (immer im Markt) + optionaler fester
        # Dollar-SL; "plan" = wie das Original-Skript (ATR-SL + TP1/TP2/TP3 mit Teilverkaeufen).
        "ab_exit_mode": os.getenv("AB_EXIT_MODE", "flip"),
        # flip-Modus: fester Dollar-SL, abschaltbar (Verlust der Position in USD beim SL, Preisabstand =
        # Betrag / Positionsgroesse, wie bei MO7/UTB u.a.). Aus = Ausstieg nur per Gegen-Signal.
        "ab_sl_enabled": os.getenv("AB_SL_ENABLED", "true").lower() == "true",
        "ab_sl_manual_usd": float(os.getenv("AB_SL_MANUAL_USD", "5.0")),
        # flip-Modus: sobald die Position um diesen Dollar-Betrag im Gewinn ist, wird der SL auf den
        # Einstiegskurs gesetzt (Break-Even). Auch ohne festen $-SL nutzbar.
        "ab_be_enabled": os.getenv("AB_BE_ENABLED", "false").lower() == "true",
        "ab_be_trigger_usd": float(os.getenv("AB_BE_TRIGGER_USD", "5.0")),
        # plan-Modus: SL = ATR x Multiplikator, TP1/TP2/TP3 = Risiko x r1/r2/r3 (bei Preset "custom" frei
        # einstellbar, sonst aus dem Preset). TP1/TP2 = echte Teilverkaeufe, beide Prozentsaetze sowie die
        # zwei SL-Nachzieh-Stufen (Break-Even bei TP1, SL-auf-TP1 bei TP2) einzeln abschaltbar.
        "ab_atr_mult": float(os.getenv("AB_ATR_MULT", "1.5")),
        "ab_r1": float(os.getenv("AB_R1", "1.0")),
        "ab_r2": float(os.getenv("AB_R2", "2.0")),
        "ab_r3": float(os.getenv("AB_R3", "3.0")),
        "ab_tp1_close_pct": float(os.getenv("AB_TP1_CLOSE_PCT", "33")),
        "ab_tp2_close_pct": float(os.getenv("AB_TP2_CLOSE_PCT", "50")),
        "ab_sl_to_breakeven_on_tp1": os.getenv("AB_SL_TO_BREAKEVEN_ON_TP1", "true").lower() == "true",
        "ab_sl_to_tp1_on_tp2": os.getenv("AB_SL_TO_TP1_ON_TP2", "true").lower() == "true",
        # Optionaler uebergeordneter SuperTrend-Trendfilter (eigene, hoehere Zeiteinheit) - wie bei
        # [Hoss] VWAP+RSI+Hull+DI: Long nur wenn SuperTrend dort bullisch, Short nur wenn baerisch.
        "ab_trend_filter_enabled": os.getenv("AB_TREND_FILTER_ENABLED", "false").lower() == "true",
        "ab_trend_filter_resolution": os.getenv("AB_TREND_FILTER_RESOLUTION", "15m"),
        "ab_trend_filter_atr_period": int(os.getenv("AB_TREND_FILTER_ATR_PERIOD", "10")),
        "ab_trend_filter_multiplier": float(os.getenv("AB_TREND_FILTER_MULTIPLIER", "3.0")),
        # Optionaler ASO-Sentiment-Filter (Nutzer-eigener "Average Sentiment Oscillator"-Pine-
        # Indikator, siehe compute_aso_filter) - Long nur wenn ASOBulls>ASOBears, Short umgekehrt.
        "ab_aso_filter_enabled": os.getenv("AB_ASO_FILTER_ENABLED", "false").lower() == "true",
        "ab_aso_filter_length": int(os.getenv("AB_ASO_FILTER_LENGTH", "10")),
        "ab_aso_filter_mode": int(os.getenv("AB_ASO_FILTER_MODE", "0")),
        "ab_aso_filter_confirm_bars": int(os.getenv("AB_ASO_FILTER_CONFIRM_BARS", "1")),
        # Diamond Algo (portiert aus dem gleichnamigen Pine-v5-Indikator) - nur der Signal-Kern:
        # SuperTrend(Sensitivity*2, ATR-Periode) + SMA-Filter, optionaler 200er-EMA-Trendfilter
        # fuer "Smart"-Signale (im Original nur Label-Text, hier ein echter Filter). SL/TP
        # ATR-basiert wie im Original (atrBand = ta.atr(atrLen) * atrRisk), TP als R:R-Vielfaches:
        # ELTE Smart (portiert aus dem gleichnamigen Pine-v5-Indikator, nur "Normal"-Modus):
        # SuperTrend(ohlc4) mit automatisch aus der Marktvolatilitaet abgeleiteter Sensitivity.
        # TP1(50%)->Break-Even, TP2(50% vom Rest=25% gesamt)->SL auf TP1, TP3(Rest):
        # Optionaler ASO-Sentiment-Filter (Nutzer-eigener "Average Sentiment Oscillator"-Pine-
        # Indikator, siehe compute_aso_filter) - Long nur wenn ASOBulls>ASOBears, Short umgekehrt.
        # Optionaler ASO-Sentiment-Filter (Nutzer-eigener "Average Sentiment Oscillator"-Pine-
        # Indikator, siehe compute_aso_filter) - Long nur wenn ASOBulls>ASOBears, Short umgekehrt.
        # Wird intern in denselben trend_filter_long_ok/short_ok-Slot eingehaengt wie der
        # SuperTrend-Filter oben (siehe hvd_poll_loop/backtest_hvd_signal), nutzt also automatisch
        # dasselbe hvd_trend_filter_signal_window_candles-Wartefenster mit.
    }


def default_state():
    return {
        "position": None, "avg_entry_price": None, "total_coin_size": 0.0,
        "entry_count": 0, "anchor_price": None, "last_price": None, "g2_trigger_armed": True, "g2_levels": None,
        "current_position_entries": [],
        "price_history": [],
        "position_opened_at": None,
        "last_entry_price": None,
        "gs_anchor": None, "gs_cooldown_until": 0.0, "gs_tag_map": {},
        "gs_last_error": None, "gs_open_orders": 0,
        "grid_sl_cooldown_until": 0.0,
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
    for key in ("copytrading_enabled",):
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
    # gs_tag_map MUSS persistiert werden: sonst weiss der Bot nach einem Redeploy nicht
    # mehr, welche offenen Orders im Buch seine eigenen sind, und cancelt sie als fremd.
    "gs_anchor", "gs_cooldown_until", "gs_tag_map", "grid_sl_cooldown_until",
    "ab_sl_price", "ab_tp1_price", "ab_tp2_price", "ab_tp3_price", "ab_tp1_done", "ab_tp2_done", "ab_be_done",
    "rsi_sl_price", "rsi_tp_price", "rsi_be_done", "rsi_sl_cooldown_until",
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


async def get_account_position_from_exchange(client, market_index, retries=5, delay=0.6):
    """Fragt die ECHTE Positionsdaten (u.a. avg_entry_price, realized_pnl) direkt von der Boerse
    ab - im Gegensatz zum theoretischen Zielpreis, mit dem eine Market-Order platziert wird, ist
    das der TATSAECHLICHE, von der Boersen-Matching-Engine bestimmte Wert. Kurzer Retry, weil die
    on-chain Verbuchung nach einer Order minimal verzoegert sein kann. Gibt None zurueck, wenn die
    Position nicht gefunden wird oder die Abfrage fehlschlaegt - der Aufrufer MUSS in diesem Fall
    auf die bisherige (theoretische) Berechnung zurueckfallen, damit ein API-Hakler niemals einen
    Trade blockiert oder falsche Daten erzwingt."""
    try:
        import lighter
        account_api = lighter.AccountApi(client.api_client)
        for attempt in range(retries):
            try:
                resp = await account_api.account(by="index", value=str(client.account_index))
                if resp and resp.accounts:
                    for pos in (resp.accounts[0].positions or []):
                        if pos.market_id == market_index:
                            return pos
            except Exception as e:
                debug_log(f"⚠️ Positionsabfrage fehlgeschlagen (Versuch {attempt+1}/{retries})", {"error": str(e)})
            await asyncio.sleep(delay)
    except Exception as e:
        debug_log("⚠️ Konnte AccountApi nicht initialisieren - falle auf theoretischen Preis zurück", {"error": str(e)})
    return None



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
        val = cfg["grid_step_usd"] if which == "grid" else cfg["tp_step_usd"]
        return val if val is not None else 0.0
    pct = cfg["grid_step_pct"] if which == "grid" else cfg["tp_step_pct"]
    # Absicherung: calc_grid_levels() wird fuer JEDEN Coin bei JEDEM /api/status-Aufruf berechnet,
    # auch wenn die aktive Strategie gar nicht Grid ist - ein verunreinigter/leerer Wert hier
    # (z.B. durch einen frontend-seitigen Bug, der einen NaN-Wert als "null" gespeichert hat)
    # legt sonst SOFORT jede einzelne Status-Abfrage fuer den betroffenen Coin lahm.
    if pct is None or reference_price is None:
        return 0.0
    return reference_price * (pct / 100)


def compute_step_abs_g2(reference_price, cfg, which):
    """Identisches Muster zu compute_step_abs, aber fuer die zweite, unabhaengige Grid-Strategie
    ('Grid 2', eigenes Feld-Praefix g2_) - eigene Einstellungen, komplett unabhaengig von der
    ersten Grid-Strategie, auch wenn beide fuer denselben Coin nacheinander getestet werden."""
    if cfg.get("g2_mode", "pct") == "usd":
        val = cfg.get("g2_step_usd") if which == "grid" else cfg.get("g2_tp_step_usd")
        return val if val is not None else 0.0
    pct = cfg.get("g2_step_pct") if which == "grid" else cfg.get("g2_tp_step_pct")
    if pct is None or reference_price is None:
        return 0.0
    return reference_price * (pct / 100)


def calc_grid_levels(symbol):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    levels = {"anchor": st["anchor_price"], "tp_price": None, "next_nachkauf_price": None,
              "grid_step_abs": None, "tp_step_abs": None}
    is_g2 = cfg.get("entry_mode") == "grid_v2"
    step_fn = compute_step_abs_g2 if is_g2 else compute_step_abs
    if st["position"] is None:
        if st["anchor_price"] is not None:
            step = step_fn(st["anchor_price"], cfg, "grid")
            levels["next_entry_long"] = round(st["anchor_price"] - step, 4)
            levels["next_entry_short"] = round(st["anchor_price"] + step, 4)
            levels["grid_step_abs"] = round(step, 4)
    elif st["avg_entry_price"] is not None:
        tp_step = step_fn(st["avg_entry_price"], cfg, "tp")
        # Nachkauf-Referenz: IMMER vom letzten Kaufpreis aus gemessen (nicht vom laufenden
        # Durchschnitt - sonst schrumpft der angezeigte Abstand mit jedem Nachkauf, obwohl der
        # echte Trigger das nicht tut, siehe Bugfix dazu in execute_entry). Gilt fuer Grid 1 UND
        # Grid 2 identisch - der Revisit-Modus bei Grid 2 aendert NICHT die Referenz, sondern
        # erlaubt zusaetzlich ein erneutes Ausloesen GENAU AUF diesem Referenz-Level, wenn der
        # Kurs zwischenzeitlich darueber/darunter war (siehe check_grid_v2_tick).
        nachkauf_ref = st["last_entry_price"] or st["avg_entry_price"]
        grid_step = step_fn(nachkauf_ref, cfg, "grid")
        levels["tp_step_abs"] = round(tp_step, 4)
        levels["grid_step_abs"] = round(grid_step, 4)
        if st["position"] == "long":
            levels["tp_price"] = round(st["avg_entry_price"] + tp_step, 4)
            levels["next_nachkauf_price"] = round(nachkauf_ref - grid_step, 4)
        else:
            levels["tp_price"] = round(st["avg_entry_price"] - tp_step, 4)
            levels["next_nachkauf_price"] = round(nachkauf_ref + grid_step, 4)
    return levels



_symbol_execution_locks = {}


def _get_symbol_lock(symbol):
    """Ein Lock pro Symbol, damit execute_entry/execute_exit/execute_partial_exit fuer
    dasselbe Symbol NIEMALS ueberlappend laufen koennen. Noetig, weil diese Funktionen echte
    Boersen-Anfragen awaiten (Order platzieren, danach Positionsdaten abfragen) - ohne Lock
    koennte ein zweiter, fast gleichzeitiger Aufruf (z.B. zwei knapp aufeinanderfolgende
    Preis-Ticks) den Zwischenzustand sehen, in dem 'position' noch nicht gesetzt ist, und
    faelschlich EBENFALLS eine neue Position eroeffnen (siehe echter Vorfall: zwei 'Neue
    Position'-Eintraege im selben Sekundenbereich)."""
    lock = _symbol_execution_locks.get(symbol)
    if lock is None:
        lock = asyncio.Lock()
        _symbol_execution_locks[symbol] = lock
    return lock


async def execute_entry(symbol, direction, price, is_add_on, size_multiplier=1.0):
    async with _get_symbol_lock(symbol):
        return await _execute_entry_locked(symbol, direction, price, is_add_on, size_multiplier)


async def _execute_entry_locked(symbol, direction, price, is_add_on, size_multiplier=1.0):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    market_index = MARKET_INDICES[symbol]

    position_usdc = cfg["margin"] * cfg["leverage"] * size_multiplier
    raw_units = position_usdc / price
    precision = get_precision(symbol)
    base_amount = int(raw_units * precision)
    new_units = base_amount / precision
    real_price_confirmed = False  # True, wenn 'price' unten durch den ECHTEN Boersen-Durchschnitt ersetzt wurde
    avg_entry_before = st["avg_entry_price"] if is_add_on else None  # siehe Nachkauf-Bug unten

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
        if err:
            await client.close()
            debug_log(f"⚠️ [{symbol}] Entry-Order fehlgeschlagen", {"error": str(err)})
            return False
        debug_log(f"✅ [{symbol}] ECHTE Order ausgeführt: {direction.upper()} @ ~{price}", {"tx_hash": str(tx_hash)})

        # Der tatsaechliche Fill-Preis einer Market-Order kann vom theoretischen Zielpreis
        # abweichen (Slippage, Latenz, schnelle Kursbewegung) - deshalb hier den ECHTEN Preis
        # direkt von der Boerse abfragen statt blind den Zielpreis zu uebernehmen. WICHTIG: das
        # ist der GESAMT-Durchschnittspreis der kompletten aktuellen Position auf der Boerse
        # (nicht nur dieses einen Fills) - siehe Verwendung unten bei is_add_on. Schlaegt die
        # Abfrage fehl, wird bewusst der Zielpreis als Naeherung beibehalten (kein Blockieren).
        #
        # DRITTER BUG HIER GEFUNDEN+GEFIXT (live beobachtet: DCA-Nachkauf bei Fractals hat TP nie
        # erreicht, obwohl der Kurs es haette hergeben muessen): die alte Pruefung 'parsed > 0'
        # schuetzt nur beim ERSTEINSTIEG (da war die Position vorher wirklich bei 0). Bei einem
        # NACHKAUF ist der alte Durchschnittspreis schon > 0 - kam die Boerse zu schnell zurueck
        # (Verbuchung des neuen Fills noch nicht durch), lieferte sie den ALTEN, unveraenderten
        # Durchschnitt zurueck, der die Pruefung 'parsed > 0' trotzdem bestand. Der Bot hielt das
        # faelschlich fuer bestaetigt und uebernahm den UNVERAENDERTEN alten Durchschnitt, obwohl
        # total_coin_size trotzdem korrekt erhoeht wurde - der interne Ø-Einstieg blieb dadurch zu
        # hoch haengen (bei einem Long-Nachkauf tiefer im Kurs muesste er ja SINKEN), TP wurde nie
        # erreicht. Fix: bei einem Nachkauf zusaetzlich verlangen, dass sich der Wert TATSAECHLICH
        # vom Stand VOR dieser Order unterscheidet - identisches Muster zum realized_pnl-Fix beim
        # Exit (siehe _execute_exit_locked).
        real_pos = await get_account_position_from_exchange(client, market_index)
        real_price = None

        def _extract_valid_price(pos):
            if pos is None or pos.avg_entry_price is None:
                return None
            try:
                parsed = float(pos.avg_entry_price)
            except (TypeError, ValueError):
                return None
            if parsed <= 0:
                return None
            if avg_entry_before is not None and abs(parsed - avg_entry_before) < 1e-9:
                return None  # unveraendert gegenueber vorher -> Nachkauf noch nicht verbucht
            return parsed

        real_price = _extract_valid_price(real_pos)
        extra_attempts = 0
        while real_price is None and extra_attempts < 8:
            await asyncio.sleep(0.6)
            real_pos = await get_account_position_from_exchange(client, market_index, retries=1, delay=0)
            real_price = _extract_valid_price(real_pos)
            extra_attempts += 1
        await client.close()
        if real_price is not None:
            if price and abs(real_price - price) / price > 0.0005:
                debug_log(f"🎯 [{symbol}] Echter Fill-Preis von der Börse: {real_price} (Ziel war {price}, Abweichung {round((real_price-price)/price*100,3)}%)")
            price = real_price
            real_price_confirmed = True
        else:
            debug_log(f"⚠️ [{symbol}] Konnte echten Fill-Preis nicht bestätigen (blieb leer/0/unverändert) - verwende Zielpreis {price} als Näherung")

    if is_add_on:
        if real_price_confirmed:
            # 'price' ist hier bereits der ECHTE, von der Boerse bereits korrekt gewichtete
            # Gesamt-Durchschnitt ueber ALLE bisherigen Fills - NICHT nochmal lokal reinrechnen
            # (das wuerde die alte Position doppelt gewichten und den Durchschnitt mit jedem
            # weiteren Nachkauf staerker verzerren - das war der Kaskaden-Bug).
            #
            # VIERTER BUG HIER GEFUNDEN+GEFIXT (live beobachtet: Grid-Nachkauf-Abstaende
            # schrumpften trotz gesetztem grid_step_usd immer weiter bis auf ~0, obwohl der Code
            # extra 'last_entry_price statt avg_entry_price' nutzt um genau das zu verhindern -
            # siehe Kommentar in strategies.py bei der Nachkauf-Pruefung): 'price' WAR an dieser
            # Stelle schon auf den GESAMT-Durchschnitt umgeschrieben (Zeile oben), und
            # 'last_entry_price = price' (weiter unten) hat dadurch faelschlich den DURCHSCHNITT
            # statt den Preis DIESES EINEN Fills gespeichert - die Absicherung griff nur dem
            # Namen nach, in Wirklichkeit war last_entry_price == avg_entry_price und der Abstand
            # schrumpfte trotzdem mit jedem Nachkauf. Fix: den echten Fill-Preis DIESES Nachkaufs
            # aus altem/neuem Durchschnitt und den hinzugekommenen Einheiten zurueckrechnen,
            # BEVOR 'price' unten fuer last_entry_price verwendet wird.
            old_total_size = st["total_coin_size"]
            new_total_size = old_total_size + new_units
            this_fill_price = ((price * new_total_size) - (avg_entry_before * old_total_size)) / new_units if new_units > 0 else price
            st["avg_entry_price"] = price
            st["total_coin_size"] = new_total_size
            price = this_fill_price  # ab hier nur noch fuer last_entry_price unten relevant
        else:
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
    if st.get("current_position_entries") is None:
        st["current_position_entries"] = []
    st["current_position_entries"].append({
        "time": now_local().isoformat(), "price": round(price, 6), "size": round(new_units, 8),
        "stufe": st["entry_count"], "is_add_on": is_add_on,
    })
    debug_log(f"📈 [{symbol}] {'Nachkauf' if is_add_on else 'Neue Position'}: {direction.upper()} @ {price} | Ø-Einstieg {round(st['avg_entry_price'], 2)} | Stufe {st['entry_count']}")
    await save_bot_state()
    return True


async def execute_partial_exit(symbol, price, fraction, reason):
    async with _get_symbol_lock(symbol):
        return await _execute_partial_exit_locked(symbol, price, fraction, reason)


async def _execute_partial_exit_locked(symbol, price, fraction, reason):
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
    exit_price_for_log = price

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

        pos_before = await get_account_position_from_exchange(client, market_index, retries=1, delay=0)
        realized_pnl_before = float(pos_before.realized_pnl) if pos_before is not None and pos_before.realized_pnl is not None else None

        tx, tx_hash, err = await place_market_order(client, market_index, symbol, is_ask, base_amount, price, reduce_only=True)
        if err:
            await client.close()
            debug_log(f"⚠️ [{symbol}] Teil-Exit-Order fehlgeschlagen", {"error": str(err)})
            return False

        # Siehe _execute_exit_locked fuer die ausfuehrliche Begruendung: nicht nur pruefen,
        # ob EINE Antwort da ist, sondern ob sich realized_pnl tatsaechlich veraendert hat -
        # sonst wird bei noch nicht durchgebuchtem PnL faelschlich real_pnl_usd=0 verwendet.
        real_pnl_usd = None
        if realized_pnl_before is not None:
            extra_attempts = 0
            while real_pnl_usd is None and extra_attempts < 8:
                pos_after = await get_account_position_from_exchange(client, market_index, retries=1, delay=0)
                if pos_after is not None and pos_after.realized_pnl is not None:
                    try:
                        parsed = float(pos_after.realized_pnl)
                        if abs(parsed - realized_pnl_before) > 1e-9:
                            real_pnl_usd = parsed - realized_pnl_before
                    except (TypeError, ValueError):
                        pass
                if real_pnl_usd is None:
                    await asyncio.sleep(0.6)
                extra_attempts += 1
        await client.close()
        if real_pnl_usd is not None:
            if abs(real_pnl_usd - pnl_usd) > 0.01:
                debug_log(f"🎯 [{symbol}] Echter realisierter Teil-PnL von der Börse: ${round(real_pnl_usd,3)} (Schätzung war ${round(pnl_usd,3)})")
            pnl_usd = real_pnl_usd
            if close_size > 0:
                exit_price_for_log = round(st["avg_entry_price"] + (pnl_usd / close_size if position_side == "long" else -pnl_usd / close_size), 4)
        else:
            debug_log(f"⚠️ [{symbol}] Konnte echten Teil-PnL nicht bestätigen (realized_pnl änderte sich nicht rechtzeitig) - verwende Schätzung basierend auf Zielpreis {price}")

    st["stats"]["total_pnl_usd"] += pnl_usd
    st["trade_log"].append({
        "side": position_side, "avg_entry": round(st["avg_entry_price"], 2), "exit": exit_price_for_log,
        "entries": st["entry_count"], "pnl_usd": round(pnl_usd, 3),
        "opened_at": st.get("position_opened_at"), "closed_at": now_local().isoformat(),
        "reason": reason, "partial": True, "fraction": fraction,
    })

    st["total_coin_size"] -= close_size
    debug_log(f"✂️ [{symbol}] Teil-Exit ({reason}): {position_side.upper()} {round(fraction*100)}% @ {exit_price_for_log} | PnL ${round(pnl_usd,3)} | Rest {round(st['total_coin_size'],6)}")
    await save_bot_state()
    return True


async def execute_exit(symbol, price, reason):
    async with _get_symbol_lock(symbol):
        return await _execute_exit_locked(symbol, price, reason)


async def _execute_exit_locked(symbol, price, reason):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    market_index = MARKET_INDICES[symbol]

    pnl_usd = (price - st["avg_entry_price"]) * st["total_coin_size"] if st["position"] == "long" else (st["avg_entry_price"] - price) * st["total_coin_size"]
    closing_side = st["position"]
    exit_price_for_log = price

    if not cfg["dry_run"]:
        client = get_lighter_client()
        if client is None:
            debug_log(f"⚠️ [{symbol}] Kein Lighter-Client - Exit übersprungen (Position bleibt offen!)")
            return
        precision = get_precision(symbol)
        base_amount = int(round(st["total_coin_size"] * precision))
        is_ask = st["position"] == "long"

        # realized_pnl VOR dem Exit merken, um danach den ECHTEN PnL-Zuwachs zu bestimmen (siehe
        # unten) - schlaegt das fehl, wird still auf die theoretische Schaetzung zurueckgefallen.
        pos_before = await get_account_position_from_exchange(client, market_index, retries=1, delay=0)
        realized_pnl_before = float(pos_before.realized_pnl) if pos_before is not None and pos_before.realized_pnl is not None else None

        tx, tx_hash, err = await place_market_order(client, market_index, symbol, is_ask, base_amount, price, reduce_only=True)
        if err:
            await client.close()
            debug_log(f"⚠️ [{symbol}] Exit-Order fehlgeschlagen - Position bleibt offen!", {"error": str(err)})
            return

        # Der tatsaechliche Fuellpreis einer Market-Order (und damit der echte PnL) kann vom
        # theoretischen Zielpreis abweichen (Slippage, Latenz, schnelle Kursbewegung) - deshalb
        # hier den ECHTEN realisierten PnL direkt von der Boerse abfragen (Differenz von
        # realized_pnl vor/nach dem Exit) statt blind mit dem Zielpreis zu rechnen.
        #
        # GLEICHER BUG-TYP wie beim Einstieg (siehe _execute_entry_locked), hier aber
        # konsequent statt sporadisch aufgetreten: 'pos_after is not None and
        # pos_after.realized_pnl is not None' prueft nur, ob ueberhaupt EINE Antwort da ist -
        # nicht, ob sie sich von der VOR dem Exit gemerkten Zahl tatsaechlich unterscheidet.
        # Wenn die Verbuchung des realisierten PnL auf der Boerse noch nicht durch war, kam
        # dieselbe (unveraenderte) Zahl zurueck -> real_pnl_usd wurde IMMER 0 -> exit_price_for_log
        # wurde IMMER exakt gleich avg_entry_price gesetzt (live beobachtet: jeder einzelne
        # Live-Exit zeigte Entry==Exit, PnL $0). Fix: explizit auf eine ECHTE AENDERUNG warten,
        # mit mehreren Versuchen - erst wenn das dauerhaft ausbleibt, auf die theoretische
        # Schaetzung (Zielpreis) zurueckfallen, NIE auf eine unveraenderte alte Zahl.
        real_pnl_usd = None
        if realized_pnl_before is not None:
            extra_attempts = 0
            while real_pnl_usd is None and extra_attempts < 8:
                pos_after = await get_account_position_from_exchange(client, market_index, retries=1, delay=0)
                if pos_after is not None and pos_after.realized_pnl is not None:
                    try:
                        parsed = float(pos_after.realized_pnl)
                        if abs(parsed - realized_pnl_before) > 1e-9:
                            real_pnl_usd = parsed - realized_pnl_before
                    except (TypeError, ValueError):
                        pass
                if real_pnl_usd is None:
                    await asyncio.sleep(0.6)
                extra_attempts += 1
        await client.close()
        if real_pnl_usd is not None:
            if abs(real_pnl_usd - pnl_usd) > 0.01:
                debug_log(f"🎯 [{symbol}] Echter realisierter PnL von der Börse: ${round(real_pnl_usd,3)} (Schätzung war ${round(pnl_usd,3)})")
            pnl_usd = real_pnl_usd
            # Nur fuer die Anzeige im Trade-Log: echten Exit-Preis aus dem echten PnL zurueckrechnen.
            if st["total_coin_size"] > 0:
                exit_price_for_log = round(st["avg_entry_price"] + (pnl_usd / st["total_coin_size"] if closing_side == "long" else -pnl_usd / st["total_coin_size"]), 4)
        else:
            debug_log(f"⚠️ [{symbol}] Konnte echten PnL nicht bestätigen (realized_pnl änderte sich nicht rechtzeitig) - verwende Schätzung basierend auf Zielpreis {price}")

    stats = st["stats"]
    stats["trades"] += 1
    stats["total_pnl_usd"] += pnl_usd
    stats["wins" if pnl_usd > 0 else "losses"] += 1
    st["trade_log"].append({
        "side": st["position"], "avg_entry": round(st["avg_entry_price"], 2), "exit": exit_price_for_log,
        "entries": st["entry_count"], "pnl_usd": round(pnl_usd, 3),
        "opened_at": st.get("position_opened_at"), "closed_at": now_local().isoformat(), "reason": reason,
    })

    debug_log(f"🏁 [{symbol}] Position geschlossen ({reason}): {st['position'].upper()} Ø{round(st['avg_entry_price'],2)} -> {exit_price_for_log} | PnL ${round(pnl_usd,3)}")

    st["position"] = None
    st["avg_entry_price"] = None
    st["total_coin_size"] = 0.0
    st["entry_count"] = 0
    st["anchor_price"] = exit_price_for_log
    st["position_opened_at"] = None
    st["last_entry_price"] = None
    st["g2_trigger_armed"] = True  # ungenutzt, siehe g2_levels - bleibt fuer Abwaertskompatibilitaet
    st["g2_levels"] = None  # Grid 2 Revisit-Modus: fuer den naechsten Zyklus alle Level zuruecksetzen
    st["current_position_entries"] = []  # Tabelle "Laufende Nachkäufe" - neuer Zyklus, alte Eintraege weg
    await save_bot_state()

    if cfg.get("auto_reverse", True) and cfg["bot_active"] and cfg["entry_mode"] == "grid":
        opposite = "short" if closing_side == "long" else "long"
        direction_mode = cfg.get("grid_direction_mode", "both")
        if direction_mode == "both" or (direction_mode == "long_only" and opposite == "long") or (direction_mode == "short_only" and opposite == "short"):
            await _execute_entry_locked(symbol, opposite, exit_price_for_log, is_add_on=False)
        # sonst (Richtung erlaubt die Gegenrichtung nicht): bleibt flach, wartet auf das naechste
        # Grid-Level in der erlaubten Richtung (siehe on_price_update)
    elif cfg.get("g2_auto_reverse", True) and cfg["bot_active"] and cfg["entry_mode"] == "grid_v2":
        opposite = "short" if closing_side == "long" else "long"
        direction_mode = cfg.get("g2_direction_mode", "both")
        if direction_mode == "both" or (direction_mode == "long_only" and opposite == "long") or (direction_mode == "short_only" and opposite == "short"):
            await _execute_entry_locked(symbol, opposite, exit_price_for_log, is_add_on=False)



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
    <label style="font-size:12px; color:var(--text-dim); margin-right:14px; display:inline-flex; align-items:center; gap:5px; cursor:pointer;" title="Copytrading komplett an/aus - pausiert Leaderboard-Abruf und alle Trader-Beobachtung/Kopie">
      <input type="checkbox" id="toggle-copytrading-global" style="cursor:pointer;"> 📡 Copytrading
    </label>
    <a href="/copytrading" style="color:#93c5fd; text-decoration:none; font-size:13px; margin-right:14px;">📡 Copy-Trading →</a><span id="mode-badge"></span><span id="active-badge"></span>
  </div>
</div>
<div class="container">

<div class="coin-overview" id="coin-overview"></div>

<div class="panel-card" style="margin-top:8px;">
<h2 class="section-title">⚡ Manuelles Trading</h2>
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
<div id="mini-candles" style="display:flex; gap:3px; align-items:center; height:60px;"></div>
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
<form id="config-form" novalidate>
  <div><label>Margin (USDC)</label><input type="number" step="any" id="margin"></div>

  <div><label>Hebel</label><input type="number" step="1" id="leverage"></div>
  <div><label>Strategie</label>
    <select class="cfg" id="entry_mode">
      <option value="grid">Neutrales Grid (Ø-Einstieg/Nachkauf/TP)</option>
      <option value="grid_v2">Grid 2 (wie Grid, optional wiederkehrende Nachkauf-Level + Verdopplung)</option>
      <option value="grid_scalp">Grid-Scalp (Maker-Only, Post-Only-Quotes, TP in $, Notausstieg)</option>
      <option value="ab_breakout">Al-Shatri Breakout (Range-Ausbruch + EMA-Trend + RSI, Presets, Ausstieg wählbar: Wechsel bei Gegen-Signal + $-SL oder Original-Plan mit ATR-SL + TP1/TP2/TP3)</option>
      <option value="rsi_signal">RSI Signal (überverkauft/überkauft, Wechsel-System, optional SuperTrend-/ADX-/MACD-Filter)</option>
    </select>
  </div>



  <div data-mode="ab_breakout" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:6px 0;">
    📡 <b>Signal</b>: Kerzenschluss bricht über/unter das Hoch/Tief der letzten "Breakout-Kerzen" (ohne die aktuelle Kerze) aus, schnelle EMA über/unter langsamer EMA bestätigt den Trend, RSI muss die Schwelle erreichen, optional zusätzlich ein Volumen-Filter.
  </div>
  <div data-mode="ab_breakout" data-requires="ab_exit_mode" data-requires-value="flip" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:2px 0;">
    🔄 <b>Wechsel</b>: Immer im Markt - der erste Buy bleibt offen, bis das erste Sell kommt; das Sell schließt ihn und öffnet direkt einen Sell (und umgekehrt). Keine Targets. Optional ein <b>fester Dollar-SL</b> (Verlust der Position in $) - ohne SL ist das Risiko pro Position unbegrenzt. Optional <b>SL auf Einstieg</b>: sobald die Position den eingestellten Dollar-Betrag im Gewinn ist, wird der SL auf den Einstiegskurs gesetzt.
  </div>
  <div data-mode="ab_breakout" data-requires="ab_exit_mode" data-requires-value="plan" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:2px 0;">
    🎯 <b>Plan (wie Original-Skript)</b>: SL = ATR × Multiplikator, TP1/TP2/TP3 = Risiko × 1x/2x/3x (Werte je Preset; bei "Custom" frei einstellbar). Solange ein Plan läuft (bis SL oder TP3), wird kein neues Signal angenommen; ein Gegen-Signal schließt nichts.
  </div>
  <div data-mode="ab_breakout"><label>Preset</label>
    <select class="cfg" id="ab_preset">
      <option value="scalping">Scalping</option>
      <option value="intraday">Intraday</option>
      <option value="swing">Swing</option>
      <option value="custom">Custom (eigene Werte unten)</option>
    </select>
  </div>
  <div data-mode="ab_breakout"><label>Zeitrahmen</label>
    <select class="cfg" id="ab_resolution">
      <option value="10s">10 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="15s">15 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="30s">30 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="45s">45 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="1m">1 Minute</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="30m">30 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
      <option value="custom">Eigene Minuten...</option>
    </select>
    <input type="number" step="1" min="1" id="ab_resolution_custom_minutes" placeholder="z.B. 8 oder 24" style="display:none; margin-top:6px; width:140px;">
  </div>
  <div data-mode="ab_breakout" data-requires="ab_preset" data-requires-value="custom"><label>Breakout-Range (Kerzen)</label><input type="number" step="1" min="2" id="ab_lookback"></div>
  <div data-mode="ab_breakout" data-requires="ab_preset" data-requires-value="custom"><label>Schnelle EMA</label><input type="number" step="1" min="1" id="ab_fast_len"></div>
  <div data-mode="ab_breakout" data-requires="ab_preset" data-requires-value="custom"><label>Langsame EMA</label><input type="number" step="1" min="2" id="ab_slow_len"></div>
  <div data-mode="ab_breakout" data-requires="ab_preset" data-requires-value="custom"><label>RSI-Periode</label><input type="number" step="1" min="2" id="ab_rsi_len"></div>
  <div data-mode="ab_breakout" data-requires="ab_preset" data-requires-value="custom"><label>RSI-Schwelle (Long ab, Short = 100 minus diesem Wert)</label><input type="number" step="1" min="50" max="80" id="ab_rsi_gate"></div>
  <div data-mode="ab_breakout" data-requires="ab_preset" data-requires-value="custom"><label>Volumen-Filter verlangen</label>
    <select class="cfg" id="ab_use_volume">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="ab_breakout" data-requires="ab_preset" data-requires-value="custom"><label>Volumen vs. 20er-Durchschnitt (Vielfaches)</label><input type="number" step="0.1" min="0.1" id="ab_vol_mult"></div>
  <div data-mode="ab_breakout" data-requires="ab_preset" data-requires-value="custom"><label>ATR-Periode</label><input type="number" step="1" min="1" id="ab_atr_len"></div>
  <div data-mode="ab_breakout" data-requires="ab_preset" data-requires-value="custom" data-requires-also="ab_exit_mode=plan"><label>SL-Abstand (ATR-Multiplikator)</label><input type="number" step="0.1" min="0.1" id="ab_atr_mult"></div>
  <div data-mode="ab_breakout" data-requires="ab_preset" data-requires-value="custom" data-requires-also="ab_exit_mode=plan"><label>Target 1 (× Risiko)</label><input type="number" step="0.1" min="0.1" id="ab_r1"></div>
  <div data-mode="ab_breakout" data-requires="ab_preset" data-requires-value="custom" data-requires-also="ab_exit_mode=plan"><label>Target 2 (× Risiko)</label><input type="number" step="0.1" min="0.1" id="ab_r2"></div>
  <div data-mode="ab_breakout" data-requires="ab_preset" data-requires-value="custom" data-requires-also="ab_exit_mode=plan"><label>Target 3 (× Risiko)</label><input type="number" step="0.1" min="0.1" id="ab_r3"></div>
  <div data-mode="ab_breakout"><label>Richtung</label>
    <select class="cfg" id="ab_direction_mode">
      <option value="both">Beide</option>
      <option value="long_only">Nur Long</option>
      <option value="short_only">Nur Short</option>
    </select>
  </div>
  <div data-mode="ab_breakout"><label>Ausstieg</label>
    <select class="cfg" id="ab_exit_mode">
      <option value="flip">Wechsel bei Gegen-Signal (+ optionaler fester $-SL)</option>
      <option value="plan">Plan wie Original-Skript (ATR-SL + TP1/TP2/TP3)</option>
    </select>
  </div>
  <div data-mode="ab_breakout" data-requires="ab_exit_mode" data-requires-value="flip"><label>Stop-Loss (fester Dollar-Betrag)</label>
    <select class="cfg" id="ab_sl_enabled">
      <option value="true">An</option>
      <option value="false">Aus (Ausstieg nur per Gegen-Signal)</option>
    </select>
  </div>
  <div data-mode="ab_breakout" data-requires="ab_sl_enabled" data-requires-also="ab_exit_mode=flip"><label>SL-Betrag ($ Verlust der Position)</label><input type="number" step="0.1" min="0.1" id="ab_sl_manual_usd"></div>
  <div data-mode="ab_breakout" data-requires="ab_exit_mode" data-requires-value="flip"><label>SL auf Einstieg bei Gewinn (Break-Even)</label>
    <select class="cfg" id="ab_be_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="ab_breakout" data-requires="ab_be_enabled" data-requires-also="ab_exit_mode=flip"><label>Gewinn-Schwelle ($ Gewinn der Position)</label><input type="number" step="0.1" min="0.1" id="ab_be_trigger_usd"></div>
  <div data-mode="ab_breakout" data-requires="ab_exit_mode" data-requires-value="plan"><label>TP1 Teilverkauf (% der Position)</label><input type="number" step="1" min="1" max="99" id="ab_tp1_close_pct"></div>
  <div data-mode="ab_breakout" data-requires="ab_exit_mode" data-requires-value="plan"><label>TP2 Teilverkauf (% der verbleibenden Position)</label><input type="number" step="1" min="1" max="99" id="ab_tp2_close_pct"></div>
  <div data-mode="ab_breakout" data-requires="ab_exit_mode" data-requires-value="plan"><label>SL auf Break-Even bei TP1</label>
    <select class="cfg" id="ab_sl_to_breakeven_on_tp1">
      <option value="false">Aus (SL bleibt unverändert)</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="ab_breakout" data-requires="ab_exit_mode" data-requires-value="plan"><label>SL auf TP1 bei TP2</label>
    <select class="cfg" id="ab_sl_to_tp1_on_tp2">
      <option value="false">Aus (SL bleibt unverändert)</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="ab_breakout"><label>Cooldown nach SL (Sek.)</label><input type="number" step="1" id="ab_sl_cooldown_seconds"></div>
  <div data-mode="ab_breakout"><label>Kerzenart für die Signalberechnung</label>
    <select class="cfg" id="ab_use_heikin_ashi">
      <option value="false">Normale Kerzen</option>
      <option value="true">Heikin Ashi (wie bei TradingView Chart-Typ-Umschaltung - glättet den Trend, Ein-/Ausstieg löst trotzdem am echten Kurs aus)</option>
    </select>
  </div>
  <div data-mode="ab_breakout"><label>SuperTrend-Trendfilter (höhere Zeiteinheit)</label>
    <select class="cfg" id="ab_trend_filter_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="ab_breakout" data-requires="ab_trend_filter_enabled"><label>Trendfilter-Zeiteinheit</label>
    <select class="cfg" id="ab_trend_filter_resolution">
      <option value="10s">10 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="15s">15 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="30s">30 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="45s">45 Sekunden (aus echten Binance-1s-Kerzen zusammengesetzt)</option>
      <option value="1m">1 Minute</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="30m">30 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
      <option value="custom">Eigene Minuten...</option>
    </select>
    <input type="number" step="1" min="1" id="ab_trend_filter_resolution_custom_minutes" placeholder="z.B. 8 oder 24" style="display:none; margin-top:6px; width:140px;">
  </div>
  <div data-mode="ab_breakout" data-requires="ab_trend_filter_enabled"><label>Trendfilter ATR-Periode</label><input type="number" step="1" min="1" id="ab_trend_filter_atr_period"></div>
  <div data-mode="ab_breakout" data-requires="ab_trend_filter_enabled"><label>Trendfilter Multiplikator</label><input type="number" step="0.1" min="0.1" id="ab_trend_filter_multiplier"></div>
  <div data-mode="ab_breakout" data-requires="ab_trend_filter_enabled" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:2px 0;">
    📈 Long-Einstiege nur, wenn der SuperTrend auf der Trendfilter-Zeiteinheit bullisch ist (Kurs über der Linie), Short-Einstiege nur bei bärischem SuperTrend. Wie bei [Hoss] VWAP+RSI+Hull+DI, hier ohne das dortige Wartefenster - ein Signal, das der Trendfilter im selben Moment nicht bestätigt, verfällt einfach.
  </div>
  <div data-mode="ab_breakout"><label>ASO-Sentiment-Filter</label>
    <select class="cfg" id="ab_aso_filter_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="ab_breakout" data-requires="ab_aso_filter_enabled"><label>ASO-Periode</label><input type="number" step="1" min="1" id="ab_aso_filter_length"></div>
  <div data-mode="ab_breakout" data-requires="ab_aso_filter_enabled"><label>ASO-Berechnung</label>
    <select class="cfg" id="ab_aso_filter_mode">
      <option value="0">Mittel aus Intrabar+Gruppe</option>
      <option value="1">Nur Intrabar</option>
      <option value="2">Nur Gruppe</option>
    </select>
  </div>
  <div data-mode="ab_breakout" data-requires="ab_aso_filter_enabled"><label>Bestätigungs-Kerzen</label><input type="number" step="1" min="1" id="ab_aso_filter_confirm_bars"></div>
  <div data-mode="ab_breakout" data-requires="ab_aso_filter_enabled" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:2px 0;">
    🎭 Eigener Average-Sentiment-Oscillator-Filter (aus deinem Pine-Script): Long-Einstiege nur, wenn ASOBulls&gt;ASOBears auf derselben Zeiteinheit, Short nur umgekehrt. Bestätigungs-Kerzen &gt;1 verlangt, dass mehrere Kerzen hintereinander in dieselbe Richtung zeigen, bevor der Filter kippt.
  </div>

  <div data-mode="rsi_signal" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:6px 0;">
    📶 <b>RSI Signal</b>: Long, sobald der RSI unter die Überverkauft-Schwelle fällt, Short sobald er über die Überkauft-Schwelle steigt. Ausstieg wie bei Al-Shatris Wechsel-Modus - der erste Long bleibt offen, bis das Gegen-Signal kommt, das dreht dann direkt. Optional ein fester Dollar-SL und "SL auf Einstieg bei Gewinn". Darunter drei unabhängig zuschaltbare Filter aus dem gemeinsamen Filter-Baukasten (auch für künftige Strategien wiederverwendbar).
  </div>
  <div data-mode="rsi_signal"><label>Zeitrahmen</label>
    <select class="cfg" id="rsi_resolution">
      <option value="1m">1 Minute</option>
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="30m">30 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
    </select>
  </div>
  <div data-mode="rsi_signal"><label>RSI-Periode</label><input type="number" step="1" min="2" id="rsi_length"></div>
  <div data-mode="rsi_signal"><label>Überverkauft (Long-Schwelle)</label><input type="number" step="1" min="1" max="49" id="rsi_oversold"></div>
  <div data-mode="rsi_signal"><label>Überkauft (Short-Schwelle)</label><input type="number" step="1" min="51" max="99" id="rsi_overbought"></div>
  <div data-mode="rsi_signal"><label>Richtung</label>
    <select class="cfg" id="rsi_direction_mode">
      <option value="both">Beide</option>
      <option value="long_only">Nur Long</option>
      <option value="short_only">Nur Short</option>
    </select>
  </div>
  <div data-mode="rsi_signal"><label>Stop-Loss (fester Dollar-Betrag)</label>
    <select class="cfg" id="rsi_sl_enabled">
      <option value="true">An</option>
      <option value="false">Aus (Ausstieg nur per Gegen-Signal)</option>
    </select>
  </div>
  <div data-mode="rsi_signal" data-requires="rsi_sl_enabled"><label>SL-Betrag ($ Verlust der Position)</label><input type="number" step="0.1" min="0.1" id="rsi_sl_manual_usd"></div>
  <div data-mode="rsi_signal"><label>SL auf Einstieg bei Gewinn (Break-Even)</label>
    <select class="cfg" id="rsi_be_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="rsi_signal" data-requires="rsi_be_enabled"><label>Gewinn-Schwelle ($ Gewinn der Position)</label><input type="number" step="0.1" min="0.1" id="rsi_be_trigger_usd"></div>
  <div data-mode="rsi_signal"><label>Take-Profit (fester Dollar-Betrag)</label>
    <select class="cfg" id="rsi_tp_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="rsi_signal" data-requires="rsi_tp_enabled"><label>TP-Betrag ($ Gewinn der Position)</label><input type="number" step="0.1" min="0.1" id="rsi_tp_manual_usd"></div>
  <div data-mode="rsi_signal"><label>Cooldown nach SL (Sek.)</label><input type="number" step="1" id="rsi_sl_cooldown_seconds"></div>

  <div data-mode="rsi_signal"><label>SuperTrend-Trendfilter (höhere Zeiteinheit)</label>
    <select class="cfg" id="rsi_supertrend_filter_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="rsi_signal" data-requires="rsi_supertrend_filter_enabled"><label>Trendfilter-Zeiteinheit</label>
    <select class="cfg" id="rsi_supertrend_filter_resolution">
      <option value="5m">5 Minuten</option>
      <option value="15m">15 Minuten</option>
      <option value="30m">30 Minuten</option>
      <option value="1h">1 Stunde</option>
      <option value="4h">4 Stunden</option>
    </select>
  </div>
  <div data-mode="rsi_signal" data-requires="rsi_supertrend_filter_enabled"><label>SuperTrend-Multiplikator</label><input type="number" step="0.1" min="0.1" id="rsi_supertrend_filter_multiplier"></div>
  <div data-mode="rsi_signal" data-requires="rsi_supertrend_filter_enabled"><label>SuperTrend ATR-Periode</label><input type="number" step="1" min="1" id="rsi_supertrend_filter_atr_period"></div>

  <div data-mode="rsi_signal"><label>ADX-Trendfilter</label>
    <select class="cfg" id="rsi_adx_filter_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="rsi_signal" data-requires="rsi_adx_filter_enabled"><label>ADX-Periode</label><input type="number" step="1" min="1" id="rsi_adx_filter_length"></div>
  <div data-mode="rsi_signal" data-requires="rsi_adx_filter_enabled"><label>ADX-Schwelle</label><input type="number" step="1" min="1" id="rsi_adx_filter_threshold"></div>
  <div data-mode="rsi_signal" data-requires="rsi_adx_filter_enabled"><label>Mit Richtung (+DI/-DI)</label>
    <select class="cfg" id="rsi_adx_filter_directional">
      <option value="true">An (Long nur bei +DI&gt;-DI, Short umgekehrt)</option>
      <option value="false">Aus (nur Trendstärke, Richtung egal)</option>
    </select>
  </div>

  <div data-mode="rsi_signal"><label>MACD-Trendfilter</label>
    <select class="cfg" id="rsi_macd_filter_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="rsi_signal" data-requires="rsi_macd_filter_enabled"><label>MACD schnell</label><input type="number" step="1" min="1" id="rsi_macd_filter_fast"></div>
  <div data-mode="rsi_signal" data-requires="rsi_macd_filter_enabled"><label>MACD langsam</label><input type="number" step="1" min="1" id="rsi_macd_filter_slow"></div>
  <div data-mode="rsi_signal" data-requires="rsi_macd_filter_enabled"><label>MACD-Signal</label><input type="number" step="1" min="1" id="rsi_macd_filter_signal"></div>




























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
  <div data-mode="grid"><label>Cooldown nach Grid-SL (Min., 0 = aus)</label><input type="number" step="any" id="grid_sl_cooldown_min"></div>
  <div data-mode="grid_scalp"><label>Notional pro Stufe ($) - Obergrenze kommt vom Spread!</label><input type="number" step="any" id="gs_step_notional_usd"></div>
  <div data-mode="grid_scalp"><label>Max. Stufen</label><input type="number" step="1" id="gs_max_levels"></div>
  <div data-mode="grid_scalp"><label>Stufen-Abstand (%)</label><input type="number" step="any" id="gs_step_pct"></div>
  <div data-mode="grid_scalp"><label>TP ($ echter Gewinn auf Gesamtposition)</label><input type="number" step="any" id="gs_tp_usd"></div>
  <div data-mode="grid_scalp"><label>Notausstieg bei uPnL ($)</label><input type="number" step="any" id="gs_flatten_usd"></div>
  <div data-mode="grid_scalp"><label>Cooldown nach Notausstieg (Min.)</label><input type="number" step="any" id="gs_cooldown_min"></div>
  <div data-mode="grid_scalp"><label>Anker-Nachf&uuml;hrung ab (%)</label><input type="number" step="any" id="gs_anchor_follow_pct"></div>
  <div data-mode="grid_scalp"><label>Requote-Drift (Ticks)</label><input type="number" step="1" id="gs_requote_ticks"></div>
  <div data-mode="grid_scalp"><label>Max. offene Orders</label><input type="number" step="1" id="gs_max_open_orders"></div>
  <div data-mode="grid_scalp"><label>Poll-Intervall (Sek.)</label><input type="number" step="any" id="gs_poll_seconds"></div>
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

  <div data-mode="grid_v2" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:6px 0;">
    🔁 Grid 2: identische Grundmechanik wie das erste Grid (Anker, Nachkauf, TP, SL, Richtung,
    Anker-Nachführung) - eigene, komplett unabhängige Einstellungen, plus zwei zusätzliche
    Optionen weiter unten (wiederkehrende Nachkauf-Level + Verdopplung).
  </div>
  <div data-mode="grid_v2"><label>Richtung</label>
    <select class="cfg" id="g2_direction_mode">
      <option value="both">Beide (Long unter Anker, Short über Anker)</option>
      <option value="long_only">Nur Long</option>
      <option value="short_only">Nur Short</option>
      <option value="smart">Smart (24h-Binance-Trend entscheidet, alle 5 Min. neu geprüft)</option>
    </select>
  </div>
  <div data-mode="grid_v2"><label>Grid-Modus</label>
    <select class="cfg" id="g2_mode">
      <option value="pct">Prozent (%)</option>
      <option value="usd">Fester $-Betrag</option>
    </select>
  </div>
  <div data-mode="grid_v2"><label>Grid-Stufe (%)</label><input type="number" step="any" id="g2_step_pct"></div>
  <div data-mode="grid_v2"><label>TP-Stufe (%)</label><input type="number" step="any" id="g2_tp_step_pct"></div>
  <div data-mode="grid_v2"><label>Grid-Stufe ($)</label><input type="number" step="any" id="g2_step_usd"></div>
  <div data-mode="grid_v2"><label>TP-Stufe ($)</label><input type="number" step="any" id="g2_tp_step_usd"></div>
  <div data-mode="grid_v2"><label>Max. Nachkauf</label><input type="number" step="1" id="g2_max_nachkauf"></div>
  <div data-mode="grid_v2"><label>Stop-Loss (auf die Gesamtposition, unabhängig von Nachkauf)</label>
    <select class="cfg" id="g2_sl_enabled">
      <option value="false">Aus (Standard)</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="grid_v2" data-requires="g2_sl_enabled"><label>SL-Modus</label>
    <select class="cfg" id="g2_sl_mode">
      <option value="usd">Fester $-Betrag</option>
      <option value="pct">Prozent vom Ø-Einstieg</option>
    </select>
  </div>
  <div data-mode="grid_v2" data-requires="g2_sl_enabled"><label>SL Fester $-Betrag</label><input type="number" step="0.5" id="g2_sl_manual_usd"></div>
  <div data-mode="grid_v2" data-requires="g2_sl_enabled"><label>SL (%)</label><input type="number" step="0.1" id="g2_sl_pct"></div>
  <div data-mode="grid_v2" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:2px 0;">
    Relevant bei "Nur Long"/"Nur Short" UND bei "Smart": läuft der Kurs weit in die aktuell NICHT
    gehandelte Richtung weg (bei Smart: die Richtung, die der 24h-Trend gerade NICHT vorschlägt),
    würde der Bot sonst endlos auf eine Rückkehr in die alte Zone warten. Ist der Abstand größer
    als der eingestellte Prozentwert, wird der Anker auf den aktuellen Kurs nachgezogen. Bei
    "Beide" ohne Wirkung (dort bleibt immer irgendeine Seite erreichbar).
  </div>
  <div data-mode="grid_v2"><label>Anker-Nachführung</label>
    <select class="cfg" id="g2_anchor_follow_enabled">
      <option value="false">Aus (Standard - Anker bleibt fest, bis eine Position schließt)</option>
      <option value="true">An - Anker folgt dem Kurs bei zu großem Abstand in gesperrter Richtung</option>
    </select>
  </div>
  <div data-mode="grid_v2"><label>Nachführ-Schwelle (%)</label><input type="number" step="0.1" min="0.1" id="g2_anchor_follow_pct"></div>
  <div data-mode="grid_v2"><label>Nach TP sofort drehen</label>
    <select class="cfg" id="g2_auto_reverse">
      <option value="true">Ja - sofort Gegenposition</option>
      <option value="false">Nein - warten auf neues Gitter-Signal</option>
    </select>
  </div>
  <div data-mode="grid_v2" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:2px 0;">
    Wiederkehrende Nachkauf-Level: läuft GENAUSO wie beim ersten Grid weiter absteigend (jeder
    neue, tiefere Level braucht einen NEUEN, weiter entfernten Kurs) - AUS (Standard) ändert daran
    nichts. AN = zusätzlich kann JEDES bereits gekaufte Level nochmal auslösen, wenn der Kurs
    zwischenzeitlich ausreichend darüber (Long) bzw. darunter (Short) zurückgekehrt ist - "ausreichend"
    steuerst du über die Mindest-Erholung darunter, damit reines Kurs-Rauschen ein Level nicht
    ständig scharf/entschärft schaltet. Beispiel: Kurs fällt von 1$ auf 90 Cent (Nachkauf), weiter
    auf 80 Cent (Nachkauf, neuer Level). Kurs steigt auf 87 Cent (nur das 80-Cent-Level wird wieder
    scharf), fällt zurück auf 80 Cent -> Nachkauf ERNEUT dort (nicht erst bei 70 Cent nötig) - bis
    die maximale Nachkauf-Anzahl erreicht ist.
  </div>
  <div data-mode="grid_v2"><label>Wiederkehrende Nachkauf-Level</label>
    <select class="cfg" id="g2_revisit_enabled">
      <option value="false">Aus (Standard - wie Grid 1, nur neue, tiefere Level)</option>
      <option value="true">An - zuletzt gekauftes Level kann zusätzlich erneut auslösen</option>
    </select>
  </div>
  <div data-mode="grid_v2" data-requires="g2_revisit_enabled"><label>Mindest-Erholung für "wieder scharf" (% der Grid-Stufe)</label><input type="number" step="1" min="1" max="200" id="g2_revisit_rearm_pct"></div>
  <div data-mode="grid_v2" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:2px 0;">
    Nachkauf-Größe verdoppeln: AUS (Standard) = jeder Nachkauf nutzt dieselbe Positionsgröße
    (Margin × Hebel). AN = jede weitere Nachkauf-Stufe verdoppelt die Größe der vorherigen
    (1x, 2x, 4x, 8x, ...) - z.B. bei 100$ Basisgröße: 1. Nachkauf 100$, 2. Nachkauf 200$,
    3. Nachkauf 400$, 4. Nachkauf 800$, begrenzt durch "Max. Nachkauf" oben.
  </div>
  <div data-mode="grid_v2"><label>Nachkauf-Größe verdoppeln</label>
    <select class="cfg" id="g2_double_enabled">
      <option value="false">Aus (Standard - immer gleiche Größe)</option>
      <option value="true">An - jede Stufe verdoppelt die vorherige</option>
    </select>
  </div>
  <div data-mode="grid_v2" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:2px 0;">
    Alternative zur festen Verdopplung oben: frei wählbarer Faktor statt fix ×2 - greift nur,
    wenn "Nachkauf-Größe verdoppeln" AUS ist. 1.0 = aus (gleiche Größe, Standard), z.B. 1.5 = jede
    Stufe 50% größer als die vorherige.
  </div>
  <div data-mode="grid_v2"><label>Nachkauf-Größe Multiplikator</label><input type="number" step="0.01" min="1" id="g2_size_multiplier"></div>
  <div data-mode="grid_v2" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:2px 0;">
    Nachkauf-Abstand-Multiplikator: 1.0 = fixer Abstand wie bisher (Standard). Größer als 1.0 =
    jeder weitere Nachkauf braucht einen größeren Abstand als der vorherige (z.B. 1.3 = jede Stufe
    30% weiter entfernt) - verteilt die Nachkäufe über eine größere Preisspanne statt sie am Anfang
    zu stapeln.
  </div>
  <div data-mode="grid_v2"><label>Nachkauf-Abstand Multiplikator</label><input type="number" step="0.01" min="1" id="g2_deviation_multiplier"></div>
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
    Nur für Al-Shatri Breakout und RSI Signal (Grid braucht historische Orderbuch-/Tick-Daten,
    die es nicht gibt). SL/TP werden pro Kerze am Schlusskurs geprüft,
    nicht Tick-für-Tick wie live. Lighter ist gebührenfrei, es werden also keine Gebühren simuliert.
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:16px;">
    <div><label>Zeitraum</label>
      <div style="display:flex; gap:6px;">
        <input type="number" step="0.1" min="0.1" id="backtest-period" value="30" style="width:90px;">
        <select class="cfg" id="backtest-period-unit" style="width:100px;">
          <option value="days">Tage</option>
          <option value="hours">Stunden</option>
        </select>
      </div>
    </div>
    <div><label>Robustheits-Check: beste N Trades ausschließen</label><input type="number" step="1" min="0" id="backtest-exclude-top-n" value="1" style="width:100px;"></div>
    <button id="btn-backtest" style="padding:12px 24px;">▶️ Backtest starten</button>
  </div>
  <div style="font-size:12px; color:var(--text-dim); margin-top:-10px; margin-bottom:16px;">
    "Stunden" eignet sich für kleine Zeiteinheiten (Sekunden-Auflösungen, 1-3 Minuten) - so lässt
    sich z.B. gezielt "die letzten 6 Stunden" statt zwangsweise ganzer Tage testen.
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









<div data-mode-section="ab_breakout" style="display:none;">
<h2 class="section-title">🎲 Al-Shatri Breakout Sweep (SuperTrend-Zeiteinheit × Multiplikator)</h2>
<div class="panel-card">
  <div style="font-size:13px; color:var(--text-dim); margin-bottom:12px;">
    Testet den übergeordneten SuperTrend-Trendfilter über alle gewählten Zeiteinheiten und einen Bereich
    von Multiplikatoren gegeneinander. Der Filter ist dabei für jede Kombination fest eingeschaltet; alles
    andere (Signal-Parameter, Ausstiegs-Modus mit SL/TP, Richtung, ASO-Filter, ATR-Periode des SuperTrends) kommt aus den
    Einstellungen oben. Die Roh-Signale werden nur EINMAL berechnet.
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Zeitraum (Tage)</label><input type="number" step="1" id="ab-sweep-days" value="30" style="width:90px;"></div>
    <div><label>Robustheits-Check: beste N ausschließen</label><input type="number" step="1" min="0" id="ab-sweep-exclude-top-n" value="1" style="width:90px;"></div>
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Multiplikator von</label><input type="number" step="0.1" min="0.1" id="ab-sweep-st-mult-min" value="0.1" style="width:90px;"></div>
    <div><label>bis</label><input type="number" step="0.1" min="0.1" id="ab-sweep-st-mult-max" value="3.0" style="width:90px;"></div>
    <div><label>Schritt</label><input type="number" step="0.01" min="0.01" id="ab-sweep-st-mult-step" value="0.1" style="width:90px;"></div>
  </div>
  <div style="margin-bottom:12px;">
    <label>SuperTrend-Zeiteinheiten (übergeordnet)</label><br>
    <label style="display:inline-flex; gap:4px; align-items:center; margin-right:10px;"><input type="checkbox" class="ab-sweep-tf" value="1m" checked> 1m</label>
    <label style="display:inline-flex; gap:4px; align-items:center; margin-right:10px;"><input type="checkbox" class="ab-sweep-tf" value="5m" checked> 5m</label>
    <label style="display:inline-flex; gap:4px; align-items:center; margin-right:10px;"><input type="checkbox" class="ab-sweep-tf" value="6m"> 6m</label>
    <label style="display:inline-flex; gap:4px; align-items:center; margin-right:10px;"><input type="checkbox" class="ab-sweep-tf" value="7m"> 7m</label>
    <label style="display:inline-flex; gap:4px; align-items:center; margin-right:10px;"><input type="checkbox" class="ab-sweep-tf" value="8m"> 8m</label>
    <label style="display:inline-flex; gap:4px; align-items:center; margin-right:10px;"><input type="checkbox" class="ab-sweep-tf" value="9m"> 9m</label>
    <label style="display:inline-flex; gap:4px; align-items:center; margin-right:10px;"><input type="checkbox" class="ab-sweep-tf" value="10m"> 10m</label>
    <label style="display:inline-flex; gap:4px; align-items:center; margin-right:10px;"><input type="checkbox" class="ab-sweep-tf" value="11m"> 11m</label>
    <label style="display:inline-flex; gap:4px; align-items:center; margin-right:10px;"><input type="checkbox" class="ab-sweep-tf" value="12m"> 12m</label>
    <label style="display:inline-flex; gap:4px; align-items:center; margin-right:10px;"><input type="checkbox" class="ab-sweep-tf" value="13m"> 13m</label>
    <label style="display:inline-flex; gap:4px; align-items:center; margin-right:10px;"><input type="checkbox" class="ab-sweep-tf" value="14m"> 14m</label>
    <label style="display:inline-flex; gap:4px; align-items:center; margin-right:10px;"><input type="checkbox" class="ab-sweep-tf" value="15m" checked> 15m</label>
    <label style="display:inline-flex; gap:4px; align-items:center; margin-right:10px;"><input type="checkbox" class="ab-sweep-tf" value="16m"> 16m</label>
    <label style="display:inline-flex; gap:4px; align-items:center; margin-right:10px;"><input type="checkbox" class="ab-sweep-tf" value="17m"> 17m</label>
    <label style="display:inline-flex; gap:4px; align-items:center; margin-right:10px;"><input type="checkbox" class="ab-sweep-tf" value="18m"> 18m</label>
    <label style="display:inline-flex; gap:4px; align-items:center; margin-right:10px;"><input type="checkbox" class="ab-sweep-tf" value="19m"> 19m</label>
    <label style="display:inline-flex; gap:4px; align-items:center; margin-right:10px;"><input type="checkbox" class="ab-sweep-tf" value="20m"> 20m</label>
    <label style="display:inline-flex; gap:4px; align-items:center; margin-right:10px;"><input type="checkbox" class="ab-sweep-tf" value="30m" checked> 30m</label>
    <label style="display:inline-flex; gap:4px; align-items:center; margin-right:10px;"><input type="checkbox" class="ab-sweep-tf" value="1h" checked> 1h</label>
    <label style="display:inline-flex; gap:4px; align-items:center; margin-right:10px;"><input type="checkbox" class="ab-sweep-tf" value="4h" checked> 4h</label>
    <div style="margin-top:6px;"><button type="button" id="btn-ab-sweep-tf-6-20" style="padding:4px 10px; font-size:12px;">6–20 min alle an/aus</button></div>
    <div style="margin-top:6px;"><label>weitere (kommagetrennt, z.B. 3m,8m,2h)</label> <input type="text" id="ab-sweep-tf-extra" placeholder="optional" style="width:180px;"></div>
  </div>
  <div style="font-size:12px; color:var(--text-dim); padding:2px 0; margin-bottom:8px;">
    Minuten-Zeiteinheiten außerhalb von 1/3/5/15/30 Minuten (6–14, 16–20 usw.) setzt der Bot selbst aus 1m-Kerzen
    zusammen - die 1m-Historie wird dafür nur EINMAL geladen und für alle gewählten Zeiteinheiten wiederverwendet (der erste Lauf
    lädt bei 30 Tagen etwa 45 Anfragen, danach ist sie im Cache). Alle 20 Boxen × 30 Multiplikatoren = 600 Kombinationen = das Limit.
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <button id="btn-ab-sweep" style="padding:12px 24px;">🎲 Sweep starten</button>
  </div>
  <div id="ab-sweep-status" style="color:var(--text-dim); font-size:13px;"></div>
  <h3 style="margin-top:16px; font-size:14px; color:var(--text-dim); display:none;" id="ab-sweep-best-tf-title">🏁 Bester Multiplikator je Zeiteinheit</h3>
  <table id="ab-sweep-best-tf-table" style="display:none; margin-top:8px;">
    <thead><tr>
      <th class="sortable" data-key="ab_trend_filter_resolution">ST-Zeiteinheit ⇅</th>
      <th class="sortable" data-key="ab_trend_filter_multiplier">ST-Multiplikator ⇅</th>
      <th class="sortable" data-key="trades">Trades ⇅</th>
      <th class="sortable" data-key="win_rate_pct">Trefferquote ⇅</th>
      <th class="sortable" data-key="total_pnl_usd">PnL $ ⇅</th>
      <th class="sortable" data-key="total_pnl_excl_top_n_usd">PnL ohne beste N $ ⇅</th>
      <th class="sortable" data-key="max_drawdown_usd">Max DD $ ⇅</th>
      <th class="sortable" data-key="avg_bars_held">Ø Kerzen gehalten ⇅</th>
    </tr></thead>
    <tbody></tbody>
  </table>
  <h3 style="margin-top:20px; font-size:14px; color:var(--text-dim); display:none;" id="ab-sweep-top-title">📈 Die 30 besten Kombinationen</h3>
  <table id="ab-sweep-results-table" style="display:none; margin-top:8px;">
    <thead><tr>
      <th class="sortable" data-key="ab_trend_filter_resolution">ST-Zeiteinheit ⇅</th>
      <th class="sortable" data-key="ab_trend_filter_multiplier">ST-Multiplikator ⇅</th>
      <th class="sortable" data-key="trades">Trades ⇅</th>
      <th class="sortable" data-key="win_rate_pct">Trefferquote ⇅</th>
      <th class="sortable" data-key="total_pnl_usd">PnL $ ⇅</th>
      <th class="sortable" data-key="total_pnl_excl_top_n_usd">PnL ohne beste N $ ⇅</th>
      <th class="sortable" data-key="max_drawdown_usd">Max DD $ ⇅</th>
      <th class="sortable" data-key="avg_bars_held">Ø Kerzen gehalten ⇅</th>
    </tr></thead>
    <tbody></tbody>
  </table>
  <h3 style="margin-top:20px; font-size:14px; color:var(--text-dim); display:none;" id="ab-sweep-worst-title">📉 Die 20 schlechtesten Werte (nach PnL, unabhängig von der Trade-Anzahl)</h3>
  <table id="ab-sweep-worst-table" style="display:none; margin-top:8px;">
    <thead><tr>
      <th class="sortable" data-key="ab_trend_filter_resolution">ST-Zeiteinheit ⇅</th>
      <th class="sortable" data-key="ab_trend_filter_multiplier">ST-Multiplikator ⇅</th>
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

<div data-mode-section="ab_breakout" style="display:none;">
<h2 class="section-title">🎲 Al-Shatri Signal-Sweep (Breakout-Range × EMAs, unabhängig vom SuperTrend)</h2>
<div class="panel-card">
  <div style="font-size:13px; color:var(--text-dim); margin-bottom:12px;">
    Testet Breakout-Range (Kerzen), schnelle EMA und langsame EMA gegeneinander. Der SuperTrend-
    Trendfilter wird hier NICHT mitvariiert - er bleibt genau so an oder aus, wie oben im Strategie-
    Panel eingestellt (für den SuperTrend selbst gibt es den separaten Sweep darüber). RSI, Volumen,
    ATR-Periode, Ausstiegs-Modus, Richtung und ASO-Filter kommen ebenfalls aus den Einstellungen oben.
    Kombinationen mit schneller ≥ langsamer EMA werden automatisch übersprungen.
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Zeitraum (Tage)</label><input type="number" step="1" id="ab-sig-sweep-days" value="30" style="width:90px;"></div>
    <div><label>Robustheits-Check: beste N ausschließen</label><input type="number" step="1" min="0" id="ab-sig-sweep-exclude-top-n" value="1" style="width:90px;"></div>
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Breakout-Range von</label><input type="number" step="1" min="2" id="ab-sig-sweep-lb-min" value="10" style="width:80px;"></div>
    <div><label>bis</label><input type="number" step="1" min="2" id="ab-sig-sweep-lb-max" value="60" style="width:80px;"></div>
    <div><label>Schritt</label><input type="number" step="1" min="1" id="ab-sig-sweep-lb-step" value="10" style="width:80px;"></div>
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Schnelle EMA von</label><input type="number" step="1" min="2" id="ab-sig-sweep-fast-min" value="10" style="width:80px;"></div>
    <div><label>bis</label><input type="number" step="1" min="2" id="ab-sig-sweep-fast-max" value="60" style="width:80px;"></div>
    <div><label>Schritt</label><input type="number" step="1" min="1" id="ab-sig-sweep-fast-step" value="10" style="width:80px;"></div>
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Langsame EMA von</label><input type="number" step="1" min="2" id="ab-sig-sweep-slow-min" value="30" style="width:80px;"></div>
    <div><label>bis</label><input type="number" step="1" min="2" id="ab-sig-sweep-slow-max" value="150" style="width:80px;"></div>
    <div><label>Schritt</label><input type="number" step="1" min="1" id="ab-sig-sweep-slow-step" value="20" style="width:80px;"></div>
  </div>
  <div style="font-size:12px; color:var(--text-dim); padding:2px 0; margin-bottom:8px;">
    Limit 600 gültige Kombinationen (nur fast &lt; slow zählt). Bei den Standardwerten sind das
    6 × 6 × 7 = 252 mögliche, davon ein Teil ungültig (fast ≥ slow) und übersprungen.
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <button id="btn-ab-sig-sweep" style="padding:12px 24px;">🎲 Sweep starten</button>
  </div>
  <div id="ab-sig-sweep-status" style="color:var(--text-dim); font-size:13px;"></div>
  <h3 style="margin-top:20px; font-size:14px; color:var(--text-dim); display:none;" id="ab-sig-sweep-top-title">📈 Die 30 besten Kombinationen</h3>
  <table id="ab-sig-sweep-results-table" style="display:none; margin-top:8px;">
    <thead><tr>
      <th class="sortable" data-key="ab_lookback">Breakout-Range ⇅</th>
      <th class="sortable" data-key="ab_fast_len">Schnelle EMA ⇅</th>
      <th class="sortable" data-key="ab_slow_len">Langsame EMA ⇅</th>
      <th class="sortable" data-key="trades">Trades ⇅</th>
      <th class="sortable" data-key="win_rate_pct">Trefferquote ⇅</th>
      <th class="sortable" data-key="total_pnl_usd">PnL $ ⇅</th>
      <th class="sortable" data-key="total_pnl_excl_top_n_usd">PnL ohne beste N $ ⇅</th>
      <th class="sortable" data-key="max_drawdown_usd">Max DD $ ⇅</th>
      <th class="sortable" data-key="avg_bars_held">Ø Kerzen gehalten ⇅</th>
    </tr></thead>
    <tbody></tbody>
  </table>
  <h3 style="margin-top:20px; font-size:14px; color:var(--text-dim); display:none;" id="ab-sig-sweep-worst-title">📉 Die 20 schlechtesten Werte (nach PnL, unabhängig von der Trade-Anzahl)</h3>
  <table id="ab-sig-sweep-worst-table" style="display:none; margin-top:8px;">
    <thead><tr>
      <th class="sortable" data-key="ab_lookback">Breakout-Range ⇅</th>
      <th class="sortable" data-key="ab_fast_len">Schnelle EMA ⇅</th>
      <th class="sortable" data-key="ab_slow_len">Langsame EMA ⇅</th>
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

<h2 class="section-title">Laufende Nachkäufe (aktuelle Position) <span id="entries-debug" style="font-size:11px; color:var(--text-dim); font-weight:normal;"></span></h2>
<div class="panel-card">
<table id="entries-table"><thead><tr><th>Zeit</th><th>Stufe</th><th>Preis</th><th>Größe (Coins)</th><th>Typ</th></tr></thead><tbody></tbody></table>
</div>

<h2 class="section-title">Letzte abgeschlossene Trades <span id="trades-debug" style="font-size:11px; color:var(--text-dim); font-weight:normal;"></span></h2>
<div class="panel-card">
<table id="trades-table"><thead><tr><th>Eröffnet</th><th>Geschlossen</th><th>Seite</th><th>Ø-Einstieg</th><th>Exit</th><th>Stufen</th><th>Grund</th><th>PnL $</th></tr></thead><tbody></tbody></table>
</div>
</div>

<script>
let priceChart;
let obiChart;
let quadStochChart;

// Manuelles Trading (BUY/SELL/TP) - fest im Dashboard, nicht mehr Teil eines verschiebbaren
// Kacheln-Systems (das frueher hier alle Diagnose-Widgets fuer OBI-Momentum-Scalp/Scalp-Board
// enthielt - mit deren Entfernung ist das jetzt ein einfaches statisches Panel).
document.getElementById('btn-manual-buy').addEventListener('click', () => manualTrade('long'));
document.getElementById('btn-manual-sell').addEventListener('click', () => manualTrade('short'));
document.getElementById('btn-manual-tp').addEventListener('click', async () => {
  const res = await fetch(`/api/close?symbol=${currentSymbol}`, { method: 'POST' });
  const data = await res.json();
  if (data.error) alert(data.error);
  refresh();
});

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
  applyFilterRequires();
}

// Generischer "Filter aufklappen"-Mechanismus: jedes Element mit data-requires="checkbox_id"
// blendet sich aus, solange die referenzierte Checkbox/Auswahl nicht auf "true" steht - so
// zeigt jede Strategie nur die Unterfelder der Filter, die man tatsaechlich aktiviert hat,
// statt immer alle Filter-Unterfelder gleichzeitig anzuzeigen. Rein Anzeige, aendert nichts
// an gespeicherten Werten oder der Backend-Logik.
function applyFilterRequires() {
  const mode = document.getElementById('entry_mode').value;
  document.querySelectorAll('[data-requires]').forEach(el => {
    // Wenn das Feld ohnehin zu einer anderen Strategie gehoert, nicht anfassen -
    // updateModeFields() hat es schon per data-mode ausgeblendet.
    if (el.dataset.mode && el.dataset.mode !== mode) return;
    const ctrl = document.getElementById(el.dataset.requires);
    // Standard: Checkbox-Auswahl (true/false). Optional data-requires-value="wert" fuer
    // Mehrwert-Selects (z.B. mv_sl_mode: "fixed"/"guide_trail") - dann zaehlt Gleichheit mit
    // diesem Wert statt 'true'.
    const expected = el.dataset.requiresValue;
    let active = ctrl && (expected !== undefined ? ctrl.value === expected : ctrl.value === 'true');
    // Optional zweite Bedingung "data-requires-also=\"id=wert\"" (z.B. Feld gilt nur bei Preset Custom UND
    // Ausstiegs-Modus Plan) - beide muessen erfuellt sein.
    if (active && el.dataset.requiresAlso) {
      const [alsoId, alsoVal] = el.dataset.requiresAlso.split('=');
      const alsoCtrl = document.getElementById(alsoId);
      active = !!alsoCtrl && alsoCtrl.value === alsoVal;
    }
    el.style.display = active ? '' : 'none';
  });
}
document.getElementById('config-form').addEventListener('change', () => {
  applyFilterRequires();
});

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
  const period = parseFloat(document.getElementById('backtest-period').value) || 30;
  const unit = document.getElementById('backtest-period-unit').value;
  const days = unit === 'hours' ? period / 24 : period;
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
      const coveredLabel = unit === 'hours' ? `${(data.actual_days_covered * 24).toFixed(1)} Stunden` : `${data.actual_days_covered} Tage`;
      statusEl.innerText = `${data.cache_used ? '⚡ aus Cache' : '📡 neu von Binance geladen'} - ${data.candles_processed} Kerzen verarbeitet (${coveredLabel}, Zeitrahmen ${data.resolution})` +
        (data.candles_processed >= data.candle_cap ? ` - auf ${data.candle_cap} Kerzen begrenzt (Performance-Schutz)` : '');
      document.getElementById('bt-candles').innerText = data.candles_processed;
      document.getElementById('bt-days').innerText = coveredLabel;
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
  // Defensiv: manche Zeitrahmen-Selects (z.B. Maverick Edge) haben bewusst KEINE "Eigene
  // Minuten"-Option und damit auch kein customInput-Element - ohne diese Absicherung wuerde
  // das die komplette Formular-Befuellung fuer JEDEN Aufruf danach abbrechen (live beobachtet:
  // dadurch blieb tp_step_pct dauerhaft leer, was spaeter beim Speichern zu einem serverseitigen
  // Absturz fuehrte, weil ein leerer Wert als 'null' ankam).
  if (!customInput) {
    select.value = value;
    return;
  }
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
document.querySelectorAll('#da_resolution, #es_resolution, #ht_resolution, #cp_resolution, #utb_resolution, #wtc_resolution, #pk_resolution, #pk_mtf_tf1, #pk_mtf_tf2, #pk_mtf_tf3, #utb_mtf_tf1, #utb_mtf_tf2, #utb_mtf_tf3, #fr_resolution, #cd_resolution, #fr_zscore_resolution, #cd_zscore_resolution, #rf_resolution, #rf_zscore_resolution, #utb_zscore_resolution, #fr_mtf_tf1, #fr_adx_resolution, #sr_resolution, #sr_adx_resolution, #sr_ema_resolution, #hvd_resolution, #hvd_adx_filter_resolution, #ab_resolution, #ab_trend_filter_resolution, #hvd_trend_filter_resolution').forEach(sel => {
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
  document.getElementById('rf-sweep-status').innerText = '';
  document.getElementById('rf-sweep-results-table').style.display = 'none';
  document.getElementById('rf-sweep-worst-table').style.display = 'none';
  document.getElementById('rf-sweep-worst-title').style.display = 'none';
  window.rfSweepResultsData = [];
  window.rfSweepWorstData = [];
}

document.getElementById('btn-ab-sig-sweep').addEventListener('click', async () => {
  const btn = document.getElementById('btn-ab-sig-sweep');
  const statusEl = document.getElementById('ab-sig-sweep-status');
  const tables = {top: document.getElementById('ab-sig-sweep-results-table'), worst: document.getElementById('ab-sig-sweep-worst-table')};
  const titles = {top: document.getElementById('ab-sig-sweep-top-title'), worst: document.getElementById('ab-sig-sweep-worst-title')};
  const sweepSymbol = currentSymbol;
  const payload = {
    days: parseInt(document.getElementById('ab-sig-sweep-days').value) || 30,
    exclude_top_n: parseInt(document.getElementById('ab-sig-sweep-exclude-top-n').value) || 0,
    lookback_min: parseInt(document.getElementById('ab-sig-sweep-lb-min').value),
    lookback_max: parseInt(document.getElementById('ab-sig-sweep-lb-max').value),
    lookback_step: parseInt(document.getElementById('ab-sig-sweep-lb-step').value),
    fast_min: parseInt(document.getElementById('ab-sig-sweep-fast-min').value),
    fast_max: parseInt(document.getElementById('ab-sig-sweep-fast-max').value),
    fast_step: parseInt(document.getElementById('ab-sig-sweep-fast-step').value),
    slow_min: parseInt(document.getElementById('ab-sig-sweep-slow-min').value),
    slow_max: parseInt(document.getElementById('ab-sig-sweep-slow-max').value),
    slow_step: parseInt(document.getElementById('ab-sig-sweep-slow-step').value),
    config: buildConfigPayload(),
  };
  btn.disabled = true;
  Object.values(tables).forEach(t => t.style.display = 'none');
  Object.values(titles).forEach(t => t.style.display = 'none');
  statusEl.innerText = `⏳ Lade Kerzen und teste alle Kombinationen...`;
  try {
    const res = await fetch(`/api/ab_signal_sweep?symbol=${sweepSymbol}`, {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)
    });
    const data = await res.json();
    if (sweepSymbol !== currentSymbol) return;
    if (data.error) {
      statusEl.innerText = `❌ ${data.error}`;
    } else {
      const tf = data.trend_filter_enabled ? `SuperTrend an (${data.trend_filter_resolution}, unverändert)` : 'SuperTrend aus';
      statusEl.innerText = `${data.combos_tested} gültige Kombinationen getestet auf ${data.candles_processed} Kerzen (${data.actual_days_covered} Tage, ${data.resolution}), ${tf}, Ausstieg: ${data.exit_mode === 'plan' ? 'Plan (ATR-SL + TP1/2/3)' : 'Wechsel, SL ' + (data.sl_enabled ? '$' + data.sl_usd : 'aus')} - Ergebnisse mit weniger als ${data.min_reliable_trades} Trades stehen unten in den Listen.`;
      window.abSigSweepResultsData = data.results || [];
      window.abSigSweepWorstData = data.worst_results || [];
      renderAbSigSweepResults();
      renderAbSigSweepWorst();
      Object.values(tables).forEach(t => t.style.display = '');
      Object.values(titles).forEach(t => t.style.display = '');
    }
  } catch (e) {
    if (sweepSymbol !== currentSymbol) return;
    statusEl.innerText = `❌ Fehler: ${e}`;
  }
  if (sweepSymbol === currentSymbol) btn.disabled = false;
});

window.abSigSweepResultsData = [];
window.abSigSweepWorstData = [];
const abSigSweepRowHtml = (r) => `
  <tr>
    <td>${r.ab_lookback}</td>
    <td>${r.ab_fast_len}</td>
    <td>${r.ab_slow_len}</td>
    <td>${r.trades}</td>
    <td>${r.win_rate_pct}%</td>
    <td class="${r.total_pnl_usd >= 0 ? 'green' : 'red'}">${r.total_pnl_usd}</td>
    <td class="${r.total_pnl_excl_top_n_usd >= 0 ? 'green' : 'red'}">${r.total_pnl_excl_top_n_usd}</td>
    <td>${r.max_drawdown_usd}</td>
    <td>${r.avg_bars_held}</td>
  </tr>`;
const renderAbSigSweepResults = makeSortableTable('ab-sig-sweep-results-table', () => window.abSigSweepResultsData, abSigSweepRowHtml);
const renderAbSigSweepWorst = makeSortableTable('ab-sig-sweep-worst-table', () => window.abSigSweepWorstData, abSigSweepRowHtml);

document.getElementById('btn-ab-sweep-tf-6-20').addEventListener('click', () => {
  const boxes = Array.from(document.querySelectorAll('.ab-sweep-tf')).filter(x => { const m = parseInt(x.value); return x.value.endsWith('m') && m >= 6 && m <= 20 && m !== 15; });
  const allOn = boxes.every(x => x.checked);
  boxes.forEach(x => x.checked = !allOn);
});

document.getElementById('btn-ab-sweep').addEventListener('click', async () => {
  const btn = document.getElementById('btn-ab-sweep');
  const statusEl = document.getElementById('ab-sweep-status');
  const els = ['best-tf', 'top', 'worst'].map(k => k);
  const tables = {best: document.getElementById('ab-sweep-best-tf-table'), top: document.getElementById('ab-sweep-results-table'), worst: document.getElementById('ab-sweep-worst-table')};
  const titles = {best: document.getElementById('ab-sweep-best-tf-title'), top: document.getElementById('ab-sweep-top-title'), worst: document.getElementById('ab-sweep-worst-title')};
  const sweepSymbol = currentSymbol;
  const tfs = Array.from(document.querySelectorAll('.ab-sweep-tf:checked')).map(x => x.value);
  document.getElementById('ab-sweep-tf-extra').value.split(',').map(x => x.trim()).filter(x => x).forEach(x => { if (!tfs.includes(x)) tfs.push(x); });
  const payload = {
    days: parseInt(document.getElementById('ab-sweep-days').value) || 30,
    exclude_top_n: parseInt(document.getElementById('ab-sweep-exclude-top-n').value) || 0,
    st_mult_min: parseFloat(document.getElementById('ab-sweep-st-mult-min').value),
    st_mult_max: parseFloat(document.getElementById('ab-sweep-st-mult-max').value),
    st_mult_step: parseFloat(document.getElementById('ab-sweep-st-mult-step').value),
    timeframes: tfs,
    config: buildConfigPayload(),
  };
  btn.disabled = true;
  Object.values(tables).forEach(t => t.style.display = 'none');
  Object.values(titles).forEach(t => t.style.display = 'none');
  statusEl.innerText = `⏳ Lade Kerzen und teste alle Kombinationen... kann bei vielen Werten etwas dauern.`;
  try {
    const res = await fetch(`/api/ab_sweep?symbol=${sweepSymbol}`, {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)
    });
    const data = await res.json();
    if (sweepSymbol !== currentSymbol) return;
    if (data.error) {
      statusEl.innerText = `❌ ${data.error}`;
    } else {
      let msg = `${data.combos_tested} Kombinationen (${data.multipliers_tested} Multiplikatoren × ${data.timeframes_tested.length} Zeiteinheiten: ${data.timeframes_tested.join(', ')}) getestet auf ${data.candles_processed} Kerzen (${data.actual_days_covered} Tage, ${data.resolution}), Ausstieg: ${data.exit_mode === 'plan' ? 'Plan (ATR-SL + TP1/2/3)' : 'Wechsel, SL ' + (data.sl_enabled ? '$' + data.sl_usd : 'aus')} - Ergebnisse mit weniger als ${data.min_reliable_trades} Trades stehen unten in den Listen.`;
      if (data.skipped_timeframes && data.skipped_timeframes.length) {
        msg += ` ⚠️ Übersprungen: ` + data.skipped_timeframes.map(x => `${x.timeframe} (${x.reason})`).join(' | ');
      }
      statusEl.innerText = msg;
      window.abSweepBestTfData = data.best_per_timeframe || [];
      window.abSweepResultsData = data.results || [];
      window.abSweepWorstData = data.worst_results || [];
      renderAbSweepBestTf();
      renderAbSweepResults();
      renderAbSweepWorst();
      Object.values(tables).forEach(t => t.style.display = '');
      Object.values(titles).forEach(t => t.style.display = '');
    }
  } catch (e) {
    if (sweepSymbol !== currentSymbol) return;
    statusEl.innerText = `❌ Fehler: ${e}`;
  }
  if (sweepSymbol === currentSymbol) btn.disabled = false;
});

window.abSweepBestTfData = [];
window.abSweepResultsData = [];
window.abSweepWorstData = [];
const abSweepRowHtml = (r) => `
  <tr>
    <td>${r.ab_trend_filter_resolution}</td>
    <td>${r.ab_trend_filter_multiplier}</td>
    <td>${r.trades}</td>
    <td>${r.win_rate_pct}%</td>
    <td class="${r.total_pnl_usd >= 0 ? 'green' : 'red'}">${r.total_pnl_usd}</td>
    <td class="${r.total_pnl_excl_top_n_usd >= 0 ? 'green' : 'red'}">${r.total_pnl_excl_top_n_usd}</td>
    <td>${r.max_drawdown_usd}</td>
    <td>${r.avg_bars_held}</td>
  </tr>`;
const renderAbSweepBestTf = makeSortableTable('ab-sweep-best-tf-table', () => window.abSweepBestTfData, abSweepRowHtml);
const renderAbSweepResults = makeSortableTable('ab-sweep-results-table', () => window.abSweepResultsData, abSweepRowHtml);
const renderAbSweepWorst = makeSortableTable('ab-sweep-worst-table', () => window.abSweepWorstData, abSweepRowHtml);






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

  const gl = data.grid_levels || {};
  const mode = data.config.entry_mode;
  // Nachkauf-Stufe: die generische max_nachkauf-Einstellung gilt eigentlich nur fuer Grid -
  let nachkaufMax = data.config.max_nachkauf || '∞';

  // Uebersicht: nur noch die Kern-Kacheln (immer relevant, egal welche Strategie) plus
  // GENAU die Diagnose-Kacheln der aktuell gewaehlten Strategie - vorher standen hier
  // IMMER alle Kacheln aller Strategien gleichzeitig (nur mit "(aktiv/inaktiv)"-Text),
  // das war der groesste Uebersichtlichkeits-Kritikpunkt.
  const coreCards = [
    `<div class="card"><div class="label">Symbol</div><div class="value">${data.symbol}</div></div>`,
    `<div class="card"><div class="label">Preis</div><div class="value">${data.last_price ?? '-'}</div></div>`,
    `<div class="card"><div class="label">Position</div><div class="value ${data.position==='long'?'green':data.position==='short'?'red':'yellow'}">${data.position || 'flach'}</div></div>`,
    `<div class="card"><div class="label">Ø-Einstieg</div><div class="value">${data.avg_entry_price ?? '-'}</div></div>`,
    `<div class="card"><div class="label">Unrealisiert $</div><div class="value ${data.unrealized_pnl_usd>=0?'green':'red'}">${data.unrealized_pnl_usd}</div></div>`,
    `<div class="card"><div class="label">Nachkauf-Stufe</div><div class="value">${data.entry_count} / ${nachkaufMax}</div></div>`,
    `<div class="card"><div class="label">Geschätzter Liq.-Preis</div><div class="value red">${data.liquidation_price ?? '-'}</div></div>`,
    `<div class="card"><div class="label">Realisiert (gesamt) $</div><div class="value ${data.stats.total_pnl_usd>=0?'green':'red'}">${data.stats.total_pnl_usd}</div></div>`,
    `<div class="card"><div class="label">Trades / Trefferquote</div><div class="value">${data.stats.trades} / ${data.stats.win_rate_pct}%</div></div>`,
  ];

  const diagnosticCards = [
    `<div class="card"><div class="label">Binance-1s-Puffer (Diagnose)</div><div class="value">${data.binance_1s_buffer_size ?? 0} Kerzen / ${Math.round((data.binance_1s_buffer_span_sec ?? 0)/60)} Min</div></div>`,
    `<div class="card"><div class="label">Lighter-Tick-Fallback-Puffer (Diagnose)</div><div class="value">${data.local_1s_buffer_size ?? 0} Kerzen</div></div>`,
  ];

  document.getElementById('status-grid').innerHTML = coreCards.concat(diagnosticCards).join('');

  if (!window.formTouched) {
    document.getElementById('margin').value = data.config.margin;
    document.getElementById('leverage').value = data.config.leverage;
    document.getElementById('entry_mode').value = data.config.entry_mode;
    document.getElementById('ab_preset').value = data.config.ab_preset;
    setResolutionField('ab_resolution', data.config.ab_resolution);
    document.getElementById('ab_lookback').value = data.config.ab_lookback;
    document.getElementById('ab_fast_len').value = data.config.ab_fast_len;
    document.getElementById('ab_slow_len').value = data.config.ab_slow_len;
    document.getElementById('ab_rsi_len').value = data.config.ab_rsi_len;
    document.getElementById('ab_rsi_gate').value = data.config.ab_rsi_gate;
    document.getElementById('ab_use_volume').value = String(data.config.ab_use_volume);
    document.getElementById('ab_vol_mult').value = data.config.ab_vol_mult;
    document.getElementById('ab_direction_mode').value = data.config.ab_direction_mode;
    document.getElementById('ab_exit_mode').value = data.config.ab_exit_mode || 'flip';
    document.getElementById('ab_sl_enabled').value = String(data.config.ab_sl_enabled);
    document.getElementById('ab_be_enabled').value = String(data.config.ab_be_enabled);
    document.getElementById('ab_be_trigger_usd').value = data.config.ab_be_trigger_usd;
    document.getElementById('ab_atr_len').value = data.config.ab_atr_len;
    document.getElementById('ab_atr_mult').value = data.config.ab_atr_mult;
    document.getElementById('ab_r1').value = data.config.ab_r1;
    document.getElementById('ab_r2').value = data.config.ab_r2;
    document.getElementById('ab_r3').value = data.config.ab_r3;
    document.getElementById('ab_tp1_close_pct').value = data.config.ab_tp1_close_pct;
    document.getElementById('ab_tp2_close_pct').value = data.config.ab_tp2_close_pct;
    document.getElementById('ab_sl_to_breakeven_on_tp1').value = String(data.config.ab_sl_to_breakeven_on_tp1);
    document.getElementById('ab_sl_to_tp1_on_tp2').value = String(data.config.ab_sl_to_tp1_on_tp2);
    document.getElementById('ab_sl_manual_usd').value = data.config.ab_sl_manual_usd;
    document.getElementById('ab_sl_cooldown_seconds').value = data.config.ab_sl_cooldown_seconds;
    document.getElementById('ab_use_heikin_ashi').value = String(data.config.ab_use_heikin_ashi);

    document.getElementById('rsi_resolution').value = data.config.rsi_resolution;
    document.getElementById('rsi_length').value = data.config.rsi_length;
    document.getElementById('rsi_oversold').value = data.config.rsi_oversold;
    document.getElementById('rsi_overbought').value = data.config.rsi_overbought;
    document.getElementById('rsi_direction_mode').value = data.config.rsi_direction_mode;
    document.getElementById('rsi_sl_enabled').value = String(data.config.rsi_sl_enabled);
    document.getElementById('rsi_sl_manual_usd').value = data.config.rsi_sl_manual_usd;
    document.getElementById('rsi_be_enabled').value = String(data.config.rsi_be_enabled);
    document.getElementById('rsi_be_trigger_usd').value = data.config.rsi_be_trigger_usd;
    document.getElementById('rsi_tp_enabled').value = String(data.config.rsi_tp_enabled);
    document.getElementById('rsi_tp_manual_usd').value = data.config.rsi_tp_manual_usd;
    document.getElementById('rsi_sl_cooldown_seconds').value = data.config.rsi_sl_cooldown_seconds;
    document.getElementById('rsi_supertrend_filter_enabled').value = String(data.config.rsi_supertrend_filter_enabled);
    document.getElementById('rsi_supertrend_filter_resolution').value = data.config.rsi_supertrend_filter_resolution;
    document.getElementById('rsi_supertrend_filter_multiplier').value = data.config.rsi_supertrend_filter_multiplier;
    document.getElementById('rsi_supertrend_filter_atr_period').value = data.config.rsi_supertrend_filter_atr_period;
    document.getElementById('rsi_adx_filter_enabled').value = String(data.config.rsi_adx_filter_enabled);
    document.getElementById('rsi_adx_filter_length').value = data.config.rsi_adx_filter_length;
    document.getElementById('rsi_adx_filter_threshold').value = data.config.rsi_adx_filter_threshold;
    document.getElementById('rsi_adx_filter_directional').value = String(data.config.rsi_adx_filter_directional);
    document.getElementById('rsi_macd_filter_enabled').value = String(data.config.rsi_macd_filter_enabled);
    document.getElementById('rsi_macd_filter_fast').value = data.config.rsi_macd_filter_fast;
    document.getElementById('rsi_macd_filter_slow').value = data.config.rsi_macd_filter_slow;
    document.getElementById('rsi_macd_filter_signal').value = data.config.rsi_macd_filter_signal;
    document.getElementById('ab_trend_filter_enabled').value = String(data.config.ab_trend_filter_enabled);
    setResolutionField('ab_trend_filter_resolution', data.config.ab_trend_filter_resolution);
    document.getElementById('ab_trend_filter_atr_period').value = data.config.ab_trend_filter_atr_period;
    document.getElementById('ab_trend_filter_multiplier').value = data.config.ab_trend_filter_multiplier;
    document.getElementById('ab_aso_filter_enabled').value = String(data.config.ab_aso_filter_enabled);
    document.getElementById('ab_aso_filter_length').value = data.config.ab_aso_filter_length;
    document.getElementById('ab_aso_filter_mode').value = data.config.ab_aso_filter_mode;
    document.getElementById('ab_aso_filter_confirm_bars').value = data.config.ab_aso_filter_confirm_bars;
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
    document.getElementById('grid_sl_cooldown_min').value = data.config.grid_sl_cooldown_min;
    document.getElementById('gs_step_notional_usd').value = data.config.gs_step_notional_usd;
    document.getElementById('gs_max_levels').value = data.config.gs_max_levels;
    document.getElementById('gs_step_pct').value = data.config.gs_step_pct;
    document.getElementById('gs_tp_usd').value = data.config.gs_tp_usd;
    document.getElementById('gs_flatten_usd').value = data.config.gs_flatten_usd;
    document.getElementById('gs_cooldown_min').value = data.config.gs_cooldown_min;
    document.getElementById('gs_anchor_follow_pct').value = data.config.gs_anchor_follow_pct;
    document.getElementById('gs_requote_ticks').value = data.config.gs_requote_ticks;
    document.getElementById('gs_max_open_orders').value = data.config.gs_max_open_orders;
    document.getElementById('gs_poll_seconds').value = data.config.gs_poll_seconds;
    document.getElementById('grid_anchor_follow_pct').value = data.config.grid_anchor_follow_pct;
    document.getElementById('dry_run').value = String(data.config.dry_run);
    document.getElementById('binance_market_type').value = data.config.binance_market_type;
    document.getElementById('auto_reverse').value = String(data.config.auto_reverse);
    document.getElementById('g2_direction_mode').value = data.config.g2_direction_mode;
    document.getElementById('g2_mode').value = data.config.g2_mode;
    document.getElementById('g2_step_pct').value = data.config.g2_step_pct;
    document.getElementById('g2_tp_step_pct').value = data.config.g2_tp_step_pct;
    document.getElementById('g2_step_usd').value = data.config.g2_step_usd;
    document.getElementById('g2_tp_step_usd').value = data.config.g2_tp_step_usd;
    document.getElementById('g2_max_nachkauf').value = data.config.g2_max_nachkauf;
    document.getElementById('g2_sl_enabled').value = String(data.config.g2_sl_enabled);
    document.getElementById('g2_sl_mode').value = data.config.g2_sl_mode;
    document.getElementById('g2_sl_manual_usd').value = data.config.g2_sl_manual_usd;
    document.getElementById('g2_sl_pct').value = data.config.g2_sl_pct;
    document.getElementById('g2_anchor_follow_enabled').value = String(data.config.g2_anchor_follow_enabled);
    document.getElementById('g2_anchor_follow_pct').value = data.config.g2_anchor_follow_pct;
    document.getElementById('g2_auto_reverse').value = String(data.config.g2_auto_reverse);
    document.getElementById('g2_revisit_enabled').value = String(data.config.g2_revisit_enabled);
    document.getElementById('g2_revisit_rearm_pct').value = data.config.g2_revisit_rearm_pct;
    document.getElementById('g2_double_enabled').value = String(data.config.g2_double_enabled);
    document.getElementById('g2_size_multiplier').value = data.config.g2_size_multiplier;
    document.getElementById('g2_deviation_multiplier').value = data.config.g2_deviation_multiplier;
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
    const entries = (data.current_position_entries || []).slice().reverse();
    document.getElementById('entries-debug').innerText = entries.length ? `(${entries.length} bisher)` : '';
    const fmtTime2 = (iso) => iso ? new Date(iso).toLocaleString('de-DE', {day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit'}) : '-';
    document.querySelector('#entries-table tbody').innerHTML = entries.map(e => `
      <tr><td>${fmtTime2(e.time)}</td><td>${e.stufe}</td><td>${e.price}</td><td>${e.size}</td><td>${e.is_add_on ? 'Nachkauf' : 'Ersteinstieg'}</td></tr>
    `).join('') || '<tr><td colspan="5" style="color:var(--text-dim);">Aktuell keine offene Position</td></tr>';
  } catch (e) {
    console.error('Nachkauf-Tabelle-Fehler:', e);
    const dbg = document.getElementById('entries-debug');
    if (dbg) dbg.innerText = `(Fehler: ${e})`;
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
    ab_preset: document.getElementById('ab_preset').value,
    ab_resolution: getResolutionField('ab_resolution'),
    ab_lookback: parseInt(document.getElementById('ab_lookback').value),
    ab_fast_len: parseInt(document.getElementById('ab_fast_len').value),
    ab_slow_len: parseInt(document.getElementById('ab_slow_len').value),
    ab_rsi_len: parseInt(document.getElementById('ab_rsi_len').value),
    ab_rsi_gate: parseFloat(document.getElementById('ab_rsi_gate').value),
    ab_use_volume: document.getElementById('ab_use_volume').value === 'true',
    ab_vol_mult: parseFloat(document.getElementById('ab_vol_mult').value),
    ab_direction_mode: document.getElementById('ab_direction_mode').value,
    ab_exit_mode: document.getElementById('ab_exit_mode').value,
    ab_sl_enabled: document.getElementById('ab_sl_enabled').value === 'true',
    ab_be_enabled: document.getElementById('ab_be_enabled').value === 'true',
    ab_be_trigger_usd: parseFloat(document.getElementById('ab_be_trigger_usd').value),
    ab_atr_len: parseInt(document.getElementById('ab_atr_len').value),
    ab_atr_mult: parseFloat(document.getElementById('ab_atr_mult').value),
    ab_r1: parseFloat(document.getElementById('ab_r1').value),
    ab_r2: parseFloat(document.getElementById('ab_r2').value),
    ab_r3: parseFloat(document.getElementById('ab_r3').value),
    ab_tp1_close_pct: parseFloat(document.getElementById('ab_tp1_close_pct').value),
    ab_tp2_close_pct: parseFloat(document.getElementById('ab_tp2_close_pct').value),
    ab_sl_to_breakeven_on_tp1: document.getElementById('ab_sl_to_breakeven_on_tp1').value === 'true',
    ab_sl_to_tp1_on_tp2: document.getElementById('ab_sl_to_tp1_on_tp2').value === 'true',
    ab_sl_manual_usd: parseFloat(document.getElementById('ab_sl_manual_usd').value),
    ab_sl_cooldown_seconds: parseFloat(document.getElementById('ab_sl_cooldown_seconds').value),
    ab_use_heikin_ashi: document.getElementById('ab_use_heikin_ashi').value === 'true',

    rsi_resolution: document.getElementById('rsi_resolution').value,
    rsi_length: parseInt(document.getElementById('rsi_length').value),
    rsi_oversold: parseFloat(document.getElementById('rsi_oversold').value),
    rsi_overbought: parseFloat(document.getElementById('rsi_overbought').value),
    rsi_direction_mode: document.getElementById('rsi_direction_mode').value,
    rsi_sl_enabled: document.getElementById('rsi_sl_enabled').value === 'true',
    rsi_sl_manual_usd: parseFloat(document.getElementById('rsi_sl_manual_usd').value),
    rsi_be_enabled: document.getElementById('rsi_be_enabled').value === 'true',
    rsi_be_trigger_usd: parseFloat(document.getElementById('rsi_be_trigger_usd').value),
    rsi_tp_enabled: document.getElementById('rsi_tp_enabled').value === 'true',
    rsi_tp_manual_usd: parseFloat(document.getElementById('rsi_tp_manual_usd').value),
    rsi_sl_cooldown_seconds: parseFloat(document.getElementById('rsi_sl_cooldown_seconds').value),
    rsi_supertrend_filter_enabled: document.getElementById('rsi_supertrend_filter_enabled').value === 'true',
    rsi_supertrend_filter_resolution: document.getElementById('rsi_supertrend_filter_resolution').value,
    rsi_supertrend_filter_multiplier: parseFloat(document.getElementById('rsi_supertrend_filter_multiplier').value),
    rsi_supertrend_filter_atr_period: parseInt(document.getElementById('rsi_supertrend_filter_atr_period').value),
    rsi_adx_filter_enabled: document.getElementById('rsi_adx_filter_enabled').value === 'true',
    rsi_adx_filter_length: parseInt(document.getElementById('rsi_adx_filter_length').value),
    rsi_adx_filter_threshold: parseFloat(document.getElementById('rsi_adx_filter_threshold').value),
    rsi_adx_filter_directional: document.getElementById('rsi_adx_filter_directional').value === 'true',
    rsi_macd_filter_enabled: document.getElementById('rsi_macd_filter_enabled').value === 'true',
    rsi_macd_filter_fast: parseInt(document.getElementById('rsi_macd_filter_fast').value),
    rsi_macd_filter_slow: parseInt(document.getElementById('rsi_macd_filter_slow').value),
    rsi_macd_filter_signal: parseInt(document.getElementById('rsi_macd_filter_signal').value),
    ab_trend_filter_enabled: document.getElementById('ab_trend_filter_enabled').value === 'true',
    ab_trend_filter_resolution: getResolutionField('ab_trend_filter_resolution'),
    ab_trend_filter_atr_period: parseInt(document.getElementById('ab_trend_filter_atr_period').value),
    ab_trend_filter_multiplier: parseFloat(document.getElementById('ab_trend_filter_multiplier').value),
    ab_aso_filter_enabled: document.getElementById('ab_aso_filter_enabled').value === 'true',
    ab_aso_filter_length: parseInt(document.getElementById('ab_aso_filter_length').value),
    ab_aso_filter_mode: parseInt(document.getElementById('ab_aso_filter_mode').value),
    ab_aso_filter_confirm_bars: parseInt(document.getElementById('ab_aso_filter_confirm_bars').value),
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
    grid_sl_cooldown_min: parseFloat(document.getElementById('grid_sl_cooldown_min').value),
    gs_step_notional_usd: parseFloat(document.getElementById('gs_step_notional_usd').value),
    gs_max_levels: parseInt(document.getElementById('gs_max_levels').value),
    gs_step_pct: parseFloat(document.getElementById('gs_step_pct').value),
    gs_tp_usd: parseFloat(document.getElementById('gs_tp_usd').value),
    gs_flatten_usd: parseFloat(document.getElementById('gs_flatten_usd').value),
    gs_cooldown_min: parseFloat(document.getElementById('gs_cooldown_min').value),
    gs_anchor_follow_pct: parseFloat(document.getElementById('gs_anchor_follow_pct').value),
    gs_requote_ticks: parseInt(document.getElementById('gs_requote_ticks').value),
    gs_max_open_orders: parseInt(document.getElementById('gs_max_open_orders').value),
    gs_poll_seconds: parseFloat(document.getElementById('gs_poll_seconds').value),
    grid_anchor_follow_pct: parseFloat(document.getElementById('grid_anchor_follow_pct').value),
    dry_run: document.getElementById('dry_run').value === 'true',
    binance_market_type: document.getElementById('binance_market_type').value,
    auto_reverse: document.getElementById('auto_reverse').value === 'true',
    g2_direction_mode: document.getElementById('g2_direction_mode').value,
    g2_mode: document.getElementById('g2_mode').value,
    g2_step_pct: parseFloat(document.getElementById('g2_step_pct').value),
    g2_tp_step_pct: parseFloat(document.getElementById('g2_tp_step_pct').value),
    g2_step_usd: parseFloat(document.getElementById('g2_step_usd').value),
    g2_tp_step_usd: parseFloat(document.getElementById('g2_tp_step_usd').value),
    g2_max_nachkauf: parseInt(document.getElementById('g2_max_nachkauf').value),
    g2_sl_enabled: document.getElementById('g2_sl_enabled').value === 'true',
    g2_sl_mode: document.getElementById('g2_sl_mode').value,
    g2_sl_manual_usd: parseFloat(document.getElementById('g2_sl_manual_usd').value),
    g2_sl_pct: parseFloat(document.getElementById('g2_sl_pct').value),
    g2_anchor_follow_enabled: document.getElementById('g2_anchor_follow_enabled').value === 'true',
    g2_anchor_follow_pct: parseFloat(document.getElementById('g2_anchor_follow_pct').value),
    g2_auto_reverse: document.getElementById('g2_auto_reverse').value === 'true',
    g2_revisit_enabled: document.getElementById('g2_revisit_enabled').value === 'true',
    g2_revisit_rearm_pct: parseFloat(document.getElementById('g2_revisit_rearm_pct').value),
    g2_double_enabled: document.getElementById('g2_double_enabled').value === 'true',
    g2_size_multiplier: parseFloat(document.getElementById('g2_size_multiplier').value),
    g2_deviation_multiplier: parseFloat(document.getElementById('g2_deviation_multiplier').value),
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
    document.getElementById('toggle-copytrading-global').checked = !!data.copytrading_enabled;
  } catch (e) {
    console.error('Globale Einstellungen konnten nicht geladen werden:', e);
  }
}
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
        "current_position_entries": st.get("current_position_entries", []),
        "ht_direction": st.get("ht_direction"), "ht_sl_price": st.get("ht_sl_price"),
        "ht_tp1_price": st.get("ht_tp1_price"), "ht_tp2_price": st.get("ht_tp2_price"), "ht_tp3_price": st.get("ht_tp3_price"),
        "ht_tp1_done": st.get("ht_tp1_done"), "ht_tp2_done": st.get("ht_tp2_done"),
        "ab_sl_price": st.get("ab_sl_price"), "ab_tp1_price": st.get("ab_tp1_price"),
        "ab_tp2_price": st.get("ab_tp2_price"), "ab_tp3_price": st.get("ab_tp3_price"),
        "ab_tp1_done": st.get("ab_tp1_done"), "ab_tp2_done": st.get("ab_tp2_done"),
        "ab_be_done": st.get("ab_be_done"),
        "ab_atr_last": st.get("ab_atr_last"),
        "rsi_sl_price": st.get("rsi_sl_price"), "rsi_tp_price": st.get("rsi_tp_price"), "rsi_be_done": st.get("rsi_be_done"), "rsi_last": st.get("rsi_last"),
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
                "grid_anchor_follow_enabled", "grid_anchor_follow_pct", "grid_sl_cooldown_min",
                "gs_step_notional_usd", "gs_max_levels", "gs_step_pct", "gs_tp_usd",
                "gs_flatten_usd", "gs_cooldown_min", "gs_anchor_follow_pct",
                "gs_requote_ticks", "gs_max_open_orders", "gs_poll_seconds",
                "dry_run", "auto_reverse", "binance_market_type",
                "g2_direction_mode", "g2_mode", "g2_step_pct", "g2_tp_step_pct", "g2_step_usd", "g2_tp_step_usd",
                "g2_max_nachkauf", "g2_sl_enabled", "g2_sl_mode", "g2_sl_manual_usd", "g2_sl_pct",
                "g2_anchor_follow_enabled", "g2_anchor_follow_pct",
                "g2_auto_reverse", "g2_revisit_enabled", "g2_revisit_rearm_pct", "g2_double_enabled",
                "g2_size_multiplier", "g2_deviation_multiplier",
                "ab_resolution", "ab_preset", "ab_lookback", "ab_fast_len", "ab_slow_len", "ab_rsi_len", "ab_rsi_gate",
                "ab_use_volume", "ab_vol_mult", "ab_atr_len", "ab_atr_mult", "ab_r1", "ab_r2", "ab_r3", "ab_direction_mode",
                "ab_exit_mode", "ab_sl_enabled", "ab_sl_manual_usd", "ab_be_enabled", "ab_be_trigger_usd", "ab_tp1_close_pct", "ab_tp2_close_pct",
                "ab_sl_to_breakeven_on_tp1", "ab_sl_to_tp1_on_tp2", "ab_sl_cooldown_seconds", "ab_use_heikin_ashi",
                "ab_trend_filter_enabled", "ab_trend_filter_resolution", "ab_trend_filter_atr_period", "ab_trend_filter_multiplier",
                "ab_aso_filter_enabled", "ab_aso_filter_length", "ab_aso_filter_mode", "ab_aso_filter_confirm_bars",
                "rsi_resolution", "rsi_length", "rsi_oversold", "rsi_overbought", "rsi_direction_mode",
                "rsi_sl_enabled", "rsi_sl_manual_usd", "rsi_be_enabled", "rsi_be_trigger_usd", "rsi_tp_enabled", "rsi_tp_manual_usd", "rsi_sl_cooldown_seconds",
                "rsi_supertrend_filter_enabled", "rsi_supertrend_filter_resolution", "rsi_supertrend_filter_multiplier", "rsi_supertrend_filter_atr_period",
                "rsi_adx_filter_enabled", "rsi_adx_filter_length", "rsi_adx_filter_threshold", "rsi_adx_filter_directional",
                "rsi_macd_filter_enabled", "rsi_macd_filter_fast", "rsi_macd_filter_slow", "rsi_macd_filter_signal"]:
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
        days = max(1 / 24, min(365, float(days)))  # Untergrenze 1 Stunde statt 1 Tag - fuer kleine Zeiteinheiten per "Zeitraum-Einheit: Stunden" im Formular
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
    try:
        result = await run_backtest(symbol, entry_mode, cfg, days, exclude_top_n)
    except Exception as e:
        # Ohne dieses try/except wuerde ein unerwarteter Fehler (z.B. bei sehr kurzen Zeitraeumen
        # mit zu wenig Kerzen fuer die Einschwingphase eines Filters) als rohe aiohttp-Fehlerseite
        # statt JSON beim Frontend ankommen - der Browser bricht dann mit einem kryptischen
        # "JSON.parse"-Fehler ab, statt die eigentliche Ursache anzuzeigen.
        debug_log(f"⚠️ [{symbol}] Backtest ({entry_mode}) fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        return web.json_response({"error": f"Backtest fehlgeschlagen: {e}"}, status=500)
    return web.json_response(result)


async def handle_ab_signal_sweep(request):
    """'Monte-Carlo'-Sweep fuer Al-Shatri Breakout: Breakout-Range x schnelle EMA x langsame EMA,
    unabhaengig vom SuperTrend-Trendfilter (der bleibt unveraendert wie konfiguriert), siehe
    run_ab_signal_sweep."""
    from strategies import run_ab_signal_sweep
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    body = await request.json()
    try:
        days = max(1, min(365, int(body.get("days", 30))))
        lookback_min = max(2, int(body.get("lookback_min", 10)))
        lookback_max = max(lookback_min, int(body.get("lookback_max", 60)))
        lookback_step = max(1, int(body.get("lookback_step", 10)))
        fast_min = max(2, int(body.get("fast_min", 10)))
        fast_max = max(fast_min, int(body.get("fast_max", 60)))
        fast_step = max(1, int(body.get("fast_step", 10)))
        slow_min = max(2, int(body.get("slow_min", 30)))
        slow_max = max(slow_min, int(body.get("slow_max", 150)))
        slow_step = max(1, int(body.get("slow_step", 20)))
    except (TypeError, ValueError):
        return web.json_response({"error": "Ungültige Zahlenwerte in den Bereichen."}, status=400)
    try:
        exclude_top_n = max(0, min(50, int(body.get("exclude_top_n", 1))))
    except (TypeError, ValueError):
        exclude_top_n = 1

    cfg = dict(BOTS[symbol]["config"])
    overrides = body.get("config")
    if isinstance(overrides, dict):
        cfg.update({k: v for k, v in overrides.items() if k in cfg})

    result = await run_ab_signal_sweep(symbol, cfg, days, lookback_min, lookback_max, lookback_step,
                                        fast_min, fast_max, fast_step, slow_min, slow_max, slow_step, exclude_top_n)
    return web.json_response(result)


async def handle_ab_sweep(request):
    """'Monte-Carlo'-Sweep fuer Al-Shatri Breakout: uebergeordnete SuperTrend-Zeiteinheit(en) x
    Multiplikator-Bereich (Standard 0.1-3.0 in 0.1-Schritten), siehe run_ab_sweep."""
    from strategies import run_ab_sweep
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    body = await request.json()
    try:
        days = max(1, min(365, int(body.get("days", 30))))
        st_mult_min = max(0.1, float(body.get("st_mult_min", 0.1)))
        st_mult_max = max(st_mult_min, float(body.get("st_mult_max", 3.0)))
        st_mult_step = max(0.01, float(body.get("st_mult_step", 0.1)))
    except (TypeError, ValueError):
        return web.json_response({"error": "Ungültige Zahlenwerte im Multiplikator-Bereich."}, status=400)
    try:
        exclude_top_n = max(0, min(50, int(body.get("exclude_top_n", 1))))
    except (TypeError, ValueError):
        exclude_top_n = 1
    timeframes = body.get("timeframes")
    if not isinstance(timeframes, list) or len(timeframes) > 20:
        return web.json_response({"error": "Bitte 1-20 SuperTrend-Zeiteinheiten auswählen."}, status=400)

    cfg = dict(BOTS[symbol]["config"])
    overrides = body.get("config")
    if isinstance(overrides, dict):
        cfg.update({k: v for k, v in overrides.items() if k in cfg})

    try:
        result = await run_ab_sweep(symbol, cfg, days, [str(t) for t in timeframes], st_mult_min, st_mult_max, st_mult_step, exclude_top_n)
    except Exception as e:
        debug_log(f"⚠️ [{symbol}] SuperTrend-Sweep fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        return web.json_response({"error": f"Sweep fehlgeschlagen: {e}"}, status=500)
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


