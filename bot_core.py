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
        # ===== Scalp VWAP OBV RSI (Mean-Reversion Scalper, entry_mode "scalp_vwap_obv_rsi") =====
        # Positionsgroesse laeuft ueber die gemeinsamen margin/leverage-Felder oben (wie bei
        # Grid/AB-Breakout/RSI/MVWAP) - kein eigenes scalp_position_size_usd noetig.
        "scalp_timeframe": os.getenv("SCALP_TIMEFRAME", "5m"),
        "scalp_vwap_length": int(os.getenv("SCALP_VWAP_LENGTH", "60")),
        "scalp_rsi_length": int(os.getenv("SCALP_RSI_LENGTH", "5")),
        "scalp_rsi_upper": float(os.getenv("SCALP_RSI_UPPER", "70")),
        "scalp_rsi_lower": float(os.getenv("SCALP_RSI_LOWER", "30")),
        "scalp_docht_threshold": float(os.getenv("SCALP_DOCHT_THRESHOLD", "0.5")),
        "scalp_sl_mode": os.getenv("SCALP_SL_MODE", "pct"),  # "pct" (Standard) oder "usd"
        "scalp_sl_pct": float(os.getenv("SCALP_SL_PCT", "0.6")),  # SL in % ab Ø-Einstieg, Standard 0.6%, einstellbar
        "scalp_sl_usd": float(os.getenv("SCALP_SL_USD", "5.0")),  # SL in $ Verlust ab Ø-Einstieg, falls scalp_sl_mode="usd"
        "scalp_max_nachkauf": int(os.getenv("SCALP_MAX_NACHKAUF", "3")),  # bis zu 3 Nachkaeufe, wie im MVWAP-Skript
        "scalp_nachkauf_min_abstand_usd": float(os.getenv("SCALP_NACHKAUF_MIN_ABSTAND_USD", "0.0")),
        "scalp_nachkauf_min_candles": int(os.getenv("SCALP_NACHKAUF_MIN_CANDLES", "10")),  # Mindestabstand zum letzten Fill in Kerzen, Standard 10
        "scalp_nachkauf_require_reversal": os.getenv("SCALP_NACHKAUF_REQUIRE_REVERSAL", "true").lower() == "true",  # Nachkauf nur, wenn die
        # neu geschlossene Kerze selbst wie eine Trendwende aussieht (Kerzenfarbe in Richtung Mittellinie UND
        # Schlusskurs jenseits des letzten Schlusskurses in dieselbe Richtung) - verhindert, dass bei einem
        # laengeren durchlaufenden Trend JEDE weitere Kerze im Band automatisch nachkauft, obwohl der Kurs
        # einfach nur weiter in dieselbe Richtung durchlaeuft statt sich der Mittellinie wieder anzunaehern.
        "scalp_tp1_full_close": os.getenv("SCALP_TP1_FULL_CLOSE", "false").lower() == "true",  # true = TP1 schliesst 100% statt 50% (dann kein TP2 mehr)
        "scalp_tp1_require_profit": os.getenv("SCALP_TP1_REQUIRE_PROFIT", "true").lower() == "true",  # TP1 nur ausloesen,
        # wenn die (bei jeder Kerze neu berechnete) Mittellinie noch auf der profitablen Seite des
        # Ø-Einstiegs liegt - sonst warten (SL bleibt aktiv) statt mit kleinem Verlust bei "TP1" rauszugehen
        "scalp_halfway_sl_enabled": os.getenv("SCALP_HALFWAY_SL_ENABLED", "true").lower() == "true",  # nach TP1: auf halbem Weg zu TP2 den SL zusaetzlich auf die Mittellinie nachziehen
        "scalp_supertrend_filter_enabled": os.getenv("SCALP_SUPERTREND_FILTER_ENABLED", "false").lower() == "true",  # uebergeordneter SuperTrend-Trendfilter (hoehere Zeiteinheit)
        "scalp_supertrend_filter_resolution": os.getenv("SCALP_SUPERTREND_FILTER_RESOLUTION", "15m"),
        "scalp_supertrend_filter_multiplier": float(os.getenv("SCALP_SUPERTREND_FILTER_MULTIPLIER", "3.0")),
        "scalp_supertrend_filter_atr_period": int(os.getenv("SCALP_SUPERTREND_FILTER_ATR_PERIOD", "10")),
        # ===== Liquidity Waves (Nachbau des gleichnamigen Pine-Script-Indikators) =====
        # Markiert Kerzen mit kleinem Koerper (relativ zur Kerzenspanne) als "Sweep"-Kerzen: eine
        # bearische Sweep-Kerze setzt ein Level am HOCH (Short/Sell-Level), eine bullische am TIEF
        # (Long/Buy-Level). Von den neuesten "liq_max_levels" Levels zaehlen nur die "gueltigen"
        # (Short-Level noch UEBER dem Kurs, Long-Level noch UNTER dem Kurs, falls liq_side_filter an) -
        # daraus ergibt sich Buyers% (Anteil gueltiger Long-Level) und Sellers% (Anteil gueltiger
        # Short-Level). Long-Einstieg wenn Buyers% unter liq_entry_threshold_pct faellt (Kontra-
        # Signal: kaum noch Kauf-Liquiditaet uebrig), Short spiegelbildlich mit Sellers%. TP1 (50%
        # der Position) sobald der jeweilige Prozentwert wieder auf liq_tp1_pct steht, TP2 (Rest)
        # bei liq_tp2_pct. SL ist ein fester $-Verlust ab Ø-Einstieg (kein Nachkauf/DCA).
        "liq_timeframe": os.getenv("LIQ_TIMEFRAME", "5m"),
        "liq_body_max_pct": float(os.getenv("LIQ_BODY_MAX_PCT", "50.0")),  # max. Koerper in % der Kerzenspanne, um als Sweep-Kerze zu zaehlen
        "liq_max_levels": int(os.getenv("LIQ_MAX_LEVELS", "50")),  # wie viele der neuesten Level insgesamt betrachtet werden
        "liq_side_filter": os.getenv("LIQ_SIDE_FILTER", "true").lower() == "true",  # nur Short-Level ueber / Long-Level unter dem Kurs zaehlen
        "liq_dup_remove": os.getenv("LIQ_DUP_REMOVE", "true").lower() == "true",  # aelteres Level derselben Art ungueltig, wenn ein spaeteres auf (fast) gleicher Hoehe liegt
        "liq_dup_tolerance_usd": float(os.getenv("LIQ_DUP_TOLERANCE_USD", "0.0")),  # Toleranz fuer "gleiche Hoehe" in $ (Ersatz fuer "Ticks" aus dem Original, da wir keine Tick-Groesse pro Symbol fuehren)
        "liq_entry_threshold_pct": float(os.getenv("LIQ_ENTRY_THRESHOLD_PCT", "5.0")),  # Long wenn Buyers% < das, Short wenn Sellers% < das
        "liq_tp1_pct": float(os.getenv("LIQ_TP1_PCT", "50.0")),  # TP1 (50% Teil-Exit, SL -> Einstieg) sobald Buyers%/Sellers% wieder hier steht
        "liq_tp2_pct": float(os.getenv("LIQ_TP2_PCT", "85.0")),  # TP2 (Rest-Exit) sobald Buyers%/Sellers% hier steht
        "liq_sl_usd": float(os.getenv("LIQ_SL_USD", "5.0")),  # fester $-Verlust ab Ø-Einstieg
        "liq_tp1_require_profit": os.getenv("LIQ_TP1_REQUIRE_PROFIT", "true").lower() == "true",  # TP1 haengt am Imbalance-%, nicht am Preis - kann sonst im Minus feuern; wenn an, wird dann bis TP2/SL gewartet
        "liq_max_nachkauf": int(os.getenv("LIQ_MAX_NACHKAUF", "0")),  # 0 = kein Nachkauf, sonst bis zu X Nachkaeufe
        "liq_nachkauf_progress_pct": float(os.getenv("LIQ_NACHKAUF_PROGRESS_PCT", "30.0")),  # Nachkauf nur, wenn Buyers%/Sellers% seit Einstieg mind. hierhin gestiegen war, TP1 aber nicht erreicht wurde, und dann wieder unter die Einstiegs-Schwelle faellt (erneutes Einstiegssignal)
        # ===== Wellenanker (WaveTrend-Punkte, siehe compute_wavetrend_series in strategies.py) =====
        # Long-Punkt: WaveTrend-Welle (wt1) kreuzt die Signallinie (wt2) von unten, waehrend wt2
        # unter -wa_zone1 steht (ueberverkauft) - Short-Punkt spiegelbildlich ueber +wa_zone1.
        # TP ueber wa_tp_mode waehlbar: "gegentrade" (Exit bei entgegengesetztem Punkt), "ueberlauf"
        # (Exit sobald wt2 wieder bis wa_ueberlauf_level zurueckgelaufen ist) oder "fester_betrag"
        # ($-Gewinn). SL ist immer ein fester $-Verlust (wa_sl_usd). Nachkauf: bis zu wa_max_nachkauf
        # (0-4) weitere Einstiege bei jedem weiteren Punkt in dieselbe Richtung.
        "wa_timeframe": os.getenv("WA_TIMEFRAME", "15m"),
        "wa_src": os.getenv("WA_SRC", "close"),  # close/hlc3/ohlc4/hl2 - Quelle der Welle
        "wa_n1": int(os.getenv("WA_N1", "10")),  # Kanal-Laenge
        "wa_n2": int(os.getenv("WA_N2", "21")),  # Durchschnitt-Laenge
        "wa_sig_len": int(os.getenv("WA_SIG_LEN", "4")),  # Signallinien-Glaettung
        "wa_wave_scale": float(os.getenv("WA_WAVE_SCALE", "1.35")),  # streckt die Welle (wt1 *= wave_scale), damit sie die Zonen so oft erreicht wie im Original-Indikator
        "wa_zone1": float(os.getenv("WA_ZONE1", "53.0")),  # Zone: Long-Punkt wenn wt2 < -zone1, Short wenn wt2 > zone1
        "wa_entry_mode": os.getenv("WA_ENTRY_MODE", "zone"),  # "zone" = Kreuzung ausserhalb der Zone, "level" = Durchbruch durch +-wa_level
        "wa_level": float(os.getenv("WA_LEVEL", "10")),  # Level-Modus: Long beim Durchbruch von -X nach oben, Short bei +X nach unten (0-30)
        "wa_trend_mode": os.getenv("WA_TREND_MODE", "off"),   # Trendfilter: off / intern / swing (Marktstruktur BOS/CHoCH)
        "wa_trend_tf": os.getenv("WA_TREND_TF", "1h"),        # Zeitebene des Trends
        "wa_trend_ilen": 4, "wa_trend_slen": 50,              # Pivot-Laengen (intern / swing)
        "wa_level_line": os.getenv("WA_LEVEL_LINE", "signal"),  # Level-Modus: signal / wave / both
        "wa_max_nachkauf": int(os.getenv("WA_MAX_NACHKAUF", "0")),  # 0 = kein Nachkauf, sonst bis zu X (max. 20)
        "wa_reverse_only_profit": os.getenv("WA_REVERSE_ONLY_PROFIT", "false").lower() == "true",  # Gegentrade/Wechsel nur, wenn die laufende Position im Plus liegt (nie mit Verlust drehen)
        "wa_nachkauf_min_pct": float(os.getenv("WA_NACHKAUF_MIN_PCT", "0")),  # Nachkauf nur, wenn der Kurs mind. X % vom letzten Einstieg entfernt ist (0 = aus)
        "wa_tp_mode": os.getenv("WA_TP_MODE", "gegentrade"),  # gegentrade / ueberlauf / fester_betrag
        "wa_ueberlauf_level": float(os.getenv("WA_UEBERLAUF_LEVEL", "45.0")),  # nur Modus "ueberlauf" - entspricht "Überlauf" im Original
        "wa_tp_usd": float(os.getenv("WA_TP_USD", "10.0")),  # nur Modus "fester_betrag"
        "wa_sl_usd": float(os.getenv("WA_SL_USD", "5.0")),  # fester $-Verlust ab Ø-Einstieg
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

        # ================= Multi-VWAP Money-Flow Signal =================
        # Composite-Oszillator aus Daily/Weekly/Monthly-VWAP-Abweichung + MFI/CMF (Original-
        # Pine-Skript), EMA-geglaettet. Buy/Sell = der Oszillator dreht die Richtung. Ausstieg/
        # Filter identisch zu RSI Signal (Wechsel-System, $-SL/-TP, Break-Even, SuperTrend/ADX/MACD).
        "mvwap_resolution": os.getenv("MVWAP_RESOLUTION", "5m"),
        "mvwap_use_daily": os.getenv("MVWAP_USE_DAILY", "true").lower() == "true",
        "mvwap_use_weekly": os.getenv("MVWAP_USE_WEEKLY", "true").lower() == "true",
        "mvwap_use_monthly": os.getenv("MVWAP_USE_MONTHLY", "true").lower() == "true",
        "mvwap_w_daily": float(os.getenv("MVWAP_W_DAILY", "0.5")),
        "mvwap_w_weekly": float(os.getenv("MVWAP_W_WEEKLY", "0.3")),
        "mvwap_w_monthly": float(os.getenv("MVWAP_W_MONTHLY", "0.2")),
        "mvwap_mf_source": os.getenv("MVWAP_MF_SOURCE", "MFI"),
        "mvwap_mf_length": int(os.getenv("MVWAP_MF_LENGTH", "14")),
        "mvwap_cmf_length": int(os.getenv("MVWAP_CMF_LENGTH", "20")),
        "mvwap_mf_weight": float(os.getenv("MVWAP_MF_WEIGHT", "0.35")),
        "mvwap_smooth_len": int(os.getenv("MVWAP_SMOOTH_LEN", "3")),
        "mvwap_use_zone_filter": os.getenv("MVWAP_USE_ZONE_FILTER", "false").lower() == "true",
        "mvwap_ob_level": float(os.getenv("MVWAP_OB_LEVEL", "2.0")),
        "mvwap_os_level": float(os.getenv("MVWAP_OS_LEVEL", "-2.0")),
        "mvwap_direction_mode": os.getenv("MVWAP_DIRECTION_MODE", "both"),
        "mvwap_max_entries": int(os.getenv("MVWAP_MAX_ENTRIES", "1")),
        "mvwap_nachkauf_min_abstand_usd": float(os.getenv("MVWAP_NACHKAUF_MIN_ABSTAND_USD", "0")),  # 0 = aus
        "mvwap_sl_enabled": os.getenv("MVWAP_SL_ENABLED", "true").lower() == "true",
        "mvwap_sl_manual_usd": float(os.getenv("MVWAP_SL_MANUAL_USD", "5.0")),
        "mvwap_tp_enabled": os.getenv("MVWAP_TP_ENABLED", "false").lower() == "true",
        "mvwap_tp_manual_usd": float(os.getenv("MVWAP_TP_MANUAL_USD", "10.0")),
        "mvwap_be_enabled": os.getenv("MVWAP_BE_ENABLED", "false").lower() == "true",
        "mvwap_be_trigger_usd": float(os.getenv("MVWAP_BE_TRIGGER_USD", "5.0")),
        "mvwap_sl_cooldown_seconds": float(os.getenv("MVWAP_SL_COOLDOWN_SECONDS", "30")),
        "mvwap_supertrend_filter_enabled": os.getenv("MVWAP_SUPERTREND_FILTER_ENABLED", "false").lower() == "true",
        "mvwap_supertrend_filter_resolution": os.getenv("MVWAP_SUPERTREND_FILTER_RESOLUTION", "15m"),
        "mvwap_supertrend_filter_multiplier": float(os.getenv("MVWAP_SUPERTREND_FILTER_MULTIPLIER", "3.0")),
        "mvwap_supertrend_filter_atr_period": int(os.getenv("MVWAP_SUPERTREND_FILTER_ATR_PERIOD", "10")),
        "mvwap_adx_filter_enabled": os.getenv("MVWAP_ADX_FILTER_ENABLED", "false").lower() == "true",
        "mvwap_adx_filter_length": int(os.getenv("MVWAP_ADX_FILTER_LENGTH", "14")),
        "mvwap_adx_filter_threshold": float(os.getenv("MVWAP_ADX_FILTER_THRESHOLD", "20")),
        "mvwap_adx_filter_directional": os.getenv("MVWAP_ADX_FILTER_DIRECTIONAL", "true").lower() == "true",
        "mvwap_adx_filter_mode": os.getenv("MVWAP_ADX_FILTER_MODE", "require_trend"),  # "require_trend" (Standard, alt) oder "avoid_trend" (neu - pausiert bei starkem Trend)
        "mvwap_macd_filter_enabled": os.getenv("MVWAP_MACD_FILTER_ENABLED", "false").lower() == "true",
        "mvwap_macd_filter_fast": int(os.getenv("MVWAP_MACD_FILTER_FAST", "12")),
        "mvwap_macd_filter_slow": int(os.getenv("MVWAP_MACD_FILTER_SLOW", "26")),
        "mvwap_macd_filter_signal": int(os.getenv("MVWAP_MACD_FILTER_SIGNAL", "9")),
        # RSI-Ueberdehnungsfilter (aus dem Pine-Skript "MVWAP-MF Osc" portiert): Long erst wenn RSI
        # unter os_level, Short erst wenn RSI ueber ob_level - zusaetzlich zur bestehenden OB/OS-
        # Zone auf dem Oszillator selbst (mvwap_use_zone_filter), unabhaengig davon an/abschaltbar.
        "mvwap_rsi_filter_enabled": os.getenv("MVWAP_RSI_FILTER_ENABLED", "false").lower() == "true",
        "mvwap_rsi_filter_length": int(os.getenv("MVWAP_RSI_FILTER_LENGTH", "14")),
        "mvwap_rsi_filter_os_level": float(os.getenv("MVWAP_RSI_FILTER_OS_LEVEL", "30")),
        "mvwap_rsi_filter_ob_level": float(os.getenv("MVWAP_RSI_FILTER_OB_LEVEL", "70")),
        # Cloud-Filter (VWAP-Deviation-Baender, aus dem "[Hoss] VWAP+RSI+Hull+DI System"-Skript
        # portiert): Short erst wenn der Kurs zuvor das obere Band beruehrt hat, Long erst nach
        # Beruehrung des unteren Bandes - bleibt scharf geschaltet bis zur jeweiligen Gegenseite.
        "mvwap_cloud_filter_enabled": os.getenv("MVWAP_CLOUD_FILTER_ENABLED", "false").lower() == "true",
        "mvwap_cloud_filter_length": int(os.getenv("MVWAP_CLOUD_FILTER_LENGTH", "60")),
        "mvwap_cloud_filter_dev_mult": float(os.getenv("MVWAP_CLOUD_FILTER_DEV_MULT", "2.0")),
        "mvwap_cloud_filter_touch_arm": os.getenv("MVWAP_CLOUD_FILTER_TOUCH_ARM", "false").lower() == "true",
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
        # ===== Scalp VWAP OBV RSI State =====
        # Position/Ø-Einstieg/Groesse/Stats/Trade-Log laufen ueber die gemeinsamen Felder oben
        # (position, avg_entry_price, total_coin_size, entry_count, stats, trade_log) - wie bei
        # allen anderen Strategien. Hier nur das Strategie-eigene: SL-Preis, TP1-Flag, die
        # zuletzt berechneten Baender + OBV-RSI (fuers Dashboard).
        "scalp_sl_price": None,
        "scalp_tp1_done": False,
        "scalp_mean_price": None,
        "scalp_upper_band": None,
        "scalp_lower_band": None,
        "scalp_obv_rsi": None,
        "scalp_candle_seq": 0,  # zaehlt bei JEDER neu geschlossenen Kerze hoch (fuer den Kerzen-Mindestabstand beim Nachkauf)
        "scalp_last_entry_seq": None,  # scalp_candle_seq-Stand beim letzten Erst-/Nachkauf
        "scalp_halfway_lock_done": False,  # true sobald der SL nach TP1 auf halbem Weg zu TP2 auf die Mittellinie nachgezogen wurde
        "scalp_left_band_since_fill": False,  # siehe scalp_nachkauf_require_reversal: wird True, sobald seit dem
        # letzten Einstieg/Nachkauf eine Kerze NICHT im Band geschlossen hat - erst dann darf die naechste
        # Kerze, die wieder im Band schliesst UND den RSI erfuellt, einen weiteren Nachkauf ausloesen.
        "scalp_band_history": [],  # Verlauf von Mittellinie/Baendern ueber Zeit, NUR fuers Dashboard-Chart
        # (Zeitpunkt + mean/upper/lower je neu geschlossener Kerze) - unabhaengig von den "scalp_*"-
        # Einzelwerten oben, die immer nur den JEWEILS AKTUELLEN Stand halten.
        "scalp_price_history": [],  # Verlauf des tatsaechlichen TRIGGER-Preises (Binance-Live, siehe
        # scalp_poll_loop) ueber Zeit - NUR fuers Dashboard-Chart. Wichtig, weil der normale
        # "Preis"-Chart den Lighter-Tick-Preis zeigt, SL/TP aber am Binance-Preis ausgeloest werden -
        # ohne diese zweite Linie sieht ein TP/SL-Trigger im Chart aus wie "hat die Linie gar nicht
        # beruehrt", obwohl Binance sie zum Ausloese-Zeitpunkt sehr wohl beruehrt/durchbrochen hat.
        # ===== Liquidity Waves State =====
        "liq_sl_price": None,
        "liq_tp1_done": False,
        "liq_buyers_pct": None,  # zuletzt berechneter Wert, nur fuers Dashboard
        "liq_sellers_pct": None,
        "liq_peak_pct": None,  # hoechster Buyers%/Sellers%-Wert seit Einstieg/letztem Nachkauf (Nachkauf-Trigger)
        "liq_nachkauf_count": 0,
        # ===== Wellenanker State =====
        "wa_sl_price": None,
        "wa_tp_price": None,
        "wa_wt2_last": None,  # zuletzt berechneter WaveTrend-Signalwert, nur fuers Dashboard
    }


# ========== GLOBALER STATE - EIN EINTRAG PRO COIN ==========
BOTS = {s: {"config": default_config(), "state": default_state()} for s in SYMBOLS}

# ========== REDIS-PERSISTENZ (Grid-Bot-Configs) ==========
REDIS_URL = os.getenv("REDIS_URL", "").strip().strip('"').strip("'")
_redis_client = None


# Hartes Zeitlimit fuer JEDEN einzelnen Redis-Call (Connect, Ping, Get, Set). Ohne das kann ein
# haengendes/totes Redis (z.B. kurzer Netzwerk-Hänger bei Render) den einen globalen, von ALLEN
# Coins geteilten Redis-Client fuer immer blockieren - da save_bot_state() nach JEDEM Trade-Close
# UND alle 60s im state_persist_loop aufgerufen wird, reisst ein haengender Call so nach und nach
# ALLE Coins mit in den Stillstand, obwohl das Trading selbst nichts mit Redis zu tun hat.
REDIS_TIMEOUT_SECONDS = 5


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
            _redis_client = redis_lib.from_url(
                REDIS_URL,
                decode_responses=True,
                socket_connect_timeout=REDIS_TIMEOUT_SECONDS,
                socket_timeout=REDIS_TIMEOUT_SECONDS,
            )
            await asyncio.wait_for(_redis_client.ping(), timeout=REDIS_TIMEOUT_SECONDS)
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
        await asyncio.wait_for(r.set("gridbot:configs", json.dumps(data)), timeout=REDIS_TIMEOUT_SECONDS)
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
        await asyncio.wait_for(r.set("gridbot:global_settings", json.dumps(GLOBAL_SETTINGS)), timeout=REDIS_TIMEOUT_SECONDS)
    except Exception as e:
        debug_log("⚠️ Speichern der globalen Einstellungen fehlgeschlagen", {"error": str(e)})


async def load_global_settings():
    r = await get_redis()
    if r is None:
        return
    try:
        raw = await asyncio.wait_for(r.get("gridbot:global_settings"), timeout=REDIS_TIMEOUT_SECONDS)
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
        raw_configs = await asyncio.wait_for(r.get("gridbot:configs"), timeout=REDIS_TIMEOUT_SECONDS)
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
    # current_position_entries MUSS persistiert werden: sonst zeigt die Tabelle "Laufende
    # Nachkäufe" nach einem Neustart/Redeploy faelschlich "keine offene Position", OBWOHL
    # Position/Ø-Einstieg/Groesse (siehe oben) ganz normal wiederhergestellt wurden - genau
    # dieser Widerspruch (oben "short" + Verlust, unten leere Tabelle) wurde live beobachtet.
    "current_position_entries",
    # gs_tag_map MUSS persistiert werden: sonst weiss der Bot nach einem Redeploy nicht
    # mehr, welche offenen Orders im Buch seine eigenen sind, und cancelt sie als fremd.
    "gs_anchor", "gs_cooldown_until", "gs_tag_map", "grid_sl_cooldown_until",
    "ab_sl_price", "ab_tp1_price", "ab_tp2_price", "ab_tp3_price", "ab_tp1_done", "ab_tp2_done", "ab_be_done",
    "rsi_sl_price", "rsi_tp_price", "rsi_be_done", "rsi_sl_cooldown_until",
    "mvwap_sl_price", "mvwap_tp_price", "mvwap_be_done", "mvwap_sl_cooldown_until",
    # Scalp VWAP OBV RSI: fehlte hier komplett (anders als bei allen anderen Strategien oben) -
    # nach jedem Neustart waren SL-Preis, TP1-Done und der Halfway-Lock-Status einer offenen
    # Scalp-Position weg, d.h. der Stop-Loss war bis zur naechsten geschlossenen Kerze faktisch
    # nicht mehr aktiv (check_scalp_sl braucht "scalp_sl_price" != None).
    "scalp_sl_price", "scalp_tp1_done", "scalp_mean_price", "scalp_upper_band", "scalp_lower_band",
    "scalp_obv_rsi", "scalp_candle_seq", "scalp_last_entry_seq", "scalp_halfway_lock_done",
    "scalp_left_band_since_fill",
    "liq_sl_price", "liq_tp1_done", "liq_peak_pct", "liq_nachkauf_count",
    "wa_sl_price", "wa_tp_price",
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
        await asyncio.wait_for(r.set("gridbot:state", json.dumps(data, default=str)), timeout=REDIS_TIMEOUT_SECONDS)
    except Exception as e:
        debug_log("⚠️ Speichern des Bot-States fehlgeschlagen", {"error": str(e)})


async def load_bot_state():
    r = await get_redis()
    if r is None:
        return
    try:
        raw_state = await asyncio.wait_for(r.get("gridbot:state"), timeout=REDIS_TIMEOUT_SECONDS)
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


# Hartes Zeitlimit fuer JEDEN einzelnen Aufruf an den Lighter-Exchange-Client (Order platzieren,
# Positionsdaten abfragen, Client schliessen). BEGRUENDUNG: trading_loop() (siehe strategies.py)
# ist der EINE geteilte Task, der Preis-Ticks fuer ALLE Coins sequenziell ueber eine einzige
# WebSocket-Verbindung verarbeitet ('async for raw in ws: ... await on_price_update(...)'). Haengt
# die Boerse/Chain auch nur bei EINER einzigen Order fuer EINEN Coin (kein Timeout = kein Fehler,
# der Await kehrt einfach nie zurueck), blockiert das denselben einen Task fuer IMMER - und damit
# JEDEN anderen Coin gleich mit, weil dessen naechster Tick nie mehr verarbeitet wird. Exakt dasselbe
# Fehlerbild wie der bereits gefixte globale Redis-Client, nur zentraler (sitzt direkt im Live-
# Trading-Pfad). Nach Ablauf des Timeouts wird der jeweilige Aufruf als fehlgeschlagen behandelt
# (wie ein normaler Boersen-Fehler) statt den Bot fuer immer einzufrieren.
EXCHANGE_CALL_TIMEOUT_SECONDS = 15


async def _safe_close_client(client):
    """client.close() mit Timeout - schlaegt das Schliessen selbst fehl/haengt, darf das trading_loop
    trotzdem nicht fuer immer blockieren (siehe EXCHANGE_CALL_TIMEOUT_SECONDS oben)."""
    try:
        await asyncio.wait_for(client.close(), timeout=EXCHANGE_CALL_TIMEOUT_SECONDS)
    except Exception as e:
        debug_log("⚠️ Lighter-Client schliessen fehlgeschlagen/Timeout (ignoriert)", {"error": str(e)})


async def place_market_order(client, market_index, symbol, is_ask, base_amount, reference_price, reduce_only=False):
    price_decimals = get_price_decimals(symbol)
    adjusted_price = reference_price * 0.98 if is_ask else reference_price * 1.02
    price_scaled = int(adjusted_price * (10 ** price_decimals))
    try:
        tx, tx_hash, err = await asyncio.wait_for(client.create_order(
            market_index=market_index, client_order_index=int(time.time() * 1000),
            base_amount=base_amount, price=price_scaled, is_ask=is_ask,
            order_type=client.ORDER_TYPE_MARKET,
            time_in_force=client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL, reduce_only=reduce_only,
            order_expiry=client.DEFAULT_IOC_EXPIRY,
        ), timeout=EXCHANGE_CALL_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        debug_log(f"⚠️ [{symbol}] Order-Aufruf nach {EXCHANGE_CALL_TIMEOUT_SECONDS}s abgebrochen (Timeout) - Boerse/Chain hat nicht rechtzeitig geantwortet")
        return None, None, f"Timeout nach {EXCHANGE_CALL_TIMEOUT_SECONDS}s"
    return tx, tx_hash, err


async def get_account_position_from_exchange(client, market_index, retries=5, delay=0.6):
    """Fragt die ECHTE Positionsdaten (u.a. avg_entry_price, realized_pnl) direkt von der Boerse
    ab - im Gegensatz zum theoretischen Zielpreis, mit dem eine Market-Order platziert wird, ist
    das der TATSAECHLICHE, von der Boersen-Matching-Engine bestimmte Wert. Kurzer Retry, weil die
    on-chain Verbuchung nach einer Order minimal verzoegert sein kann. Gibt None zurueck, wenn die
    Position nicht gefunden wird oder die Abfrage fehlschlaegt - der Aufrufer MUSS in diesem Fall
    auf die bisherige (theoretische) Berechnung zurueckfallen, damit ein API-Hakler niemals einen
    Trade blockiert oder falsche Daten erzwingt. Jeder einzelne Abfrage-Versuch hat ein hartes
    Zeitlimit (EXCHANGE_CALL_TIMEOUT_SECONDS) - ohne das wuerde schon der ERSTE haengende Versuch
    die komplette Retry-Schleife (und damit das trading_loop) fuer immer blockieren, statt nach
    einem Fehlversuch weiterzumachen."""
    try:
        import lighter
        account_api = lighter.AccountApi(client.api_client)
        for attempt in range(retries):
            try:
                resp = await asyncio.wait_for(
                    account_api.account(by="index", value=str(client.account_index)),
                    timeout=EXCHANGE_CALL_TIMEOUT_SECONDS)
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
            await _safe_close_client(client)
            return False
        is_ask = direction == "short"
        tx, tx_hash, err = await place_market_order(client, market_index, symbol, is_ask, base_amount, price, reduce_only=False)
        if err:
            await _safe_close_client(client)
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
        await _safe_close_client(client)
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


async def execute_reverse(symbol, price, reason, size_multiplier=None):
    """SCHNELLER WECHSEL Long <-> Short mit EINER einzigen Market-Order (wie der "Reverse"-Knopf bei Lighter):
    die Order ist so gross wie die alte Position PLUS die neue Position, in Gegenrichtung und OHNE reduce_only -
    die Boerse dreht die Position dadurch in einem Schritt. Das ist viel schneller als execute_exit (wartet auf
    Bestaetigung des realisierten PnL) + execute_entry (wartet auf den Fill-Preis).
    size_multiplier=None -> neue Position gleich gross (USDC-Wert) wie die alte; sonst margin*leverage*size_multiplier.
    Der Bot-State wird sofort mit dem Zielpreis umgestellt, der ECHTE Einstiegspreis wird danach im Hintergrund
    von der Boerse geholt und nachgetragen (_reverse_confirm). Gibt True zurueck, wenn die Position gedreht wurde."""
    async with _get_symbol_lock(symbol):
        return await _execute_reverse_locked(symbol, price, reason, size_multiplier)


async def _execute_reverse_locked(symbol, price, reason, size_multiplier=None):
    b = BOTS[symbol]
    st, cfg = b["state"], b["config"]
    market_index = MARKET_INDICES[symbol]
    if st["position"] is None or not st.get("total_coin_size") or not st.get("avg_entry_price") or not price:
        return False

    _t0 = time.time()
    closing_side = st["position"]
    target = "short" if closing_side == "long" else "long"
    old_size = st["total_coin_size"]
    avg_old = st["avg_entry_price"]
    pnl_usd = (price - avg_old) * old_size if closing_side == "long" else (avg_old - price) * old_size

    precision = get_precision(symbol)
    new_usdc = old_size * price if size_multiplier is None else cfg["margin"] * cfg["leverage"] * size_multiplier
    new_base = int((new_usdc / price) * precision)
    new_units = new_base / precision
    old_base = int(round(old_size * precision))

    if not cfg["dry_run"]:
        client = get_lighter_client()
        if client is None:
            debug_log(f"⚠️ [{symbol}] Kein Lighter-Client - Reverse übersprungen (Position bleibt wie sie ist!)")
            return False
        if new_base * (1 / precision) < get_min_base_amount(symbol):
            await _safe_close_client(client)
            debug_log(f"⚠️ [{symbol}] Reverse: neue Position unter Mindestgröße - abgebrochen")
            return False
        is_ask = closing_side == "long"   # Long wird verkauft (is_ask), Short wird gekauft
        tx, tx_hash, err = await place_market_order(client, market_index, symbol, is_ask, old_base + new_base, price, reduce_only=False)
        await _safe_close_client(client)
        if err:
            debug_log(f"⚠️ [{symbol}] Reverse-Order fehlgeschlagen - Position bleibt unverändert", {"error": str(err)})
            return False
        debug_log(f"⚡ [{symbol}] REVERSE ausgeführt (1 Order, {round((time.time() - _t0) * 1000)} ms): {closing_side.upper()} -> {target.upper()} @ ~{price}", {"tx_hash": str(tx_hash)})

    stats = st["stats"]
    stats["trades"] += 1
    stats["total_pnl_usd"] += pnl_usd
    stats["wins" if pnl_usd > 0 else "losses"] += 1
    st["trade_log"].append({
        "side": closing_side, "avg_entry": round(avg_old, 2), "exit": price,
        "entries": st["entry_count"], "pnl_usd": round(pnl_usd, 3),
        "opened_at": st.get("position_opened_at"), "closed_at": now_local().isoformat(), "reason": reason,
    })
    debug_log(f"🏁 [{symbol}] Position gedreht ({reason}): {closing_side.upper()} Ø{round(avg_old,2)} -> {price} | PnL ${round(pnl_usd,3)} (Schätzung)")

    st["position"] = target
    st["avg_entry_price"] = price
    st["total_coin_size"] = new_units
    st["position_opened_at"] = now_local().isoformat()
    st["last_entry_price"] = price
    st["entry_count"] = 1
    st["anchor_price"] = price
    st["g2_trigger_armed"] = True
    st["g2_levels"] = None
    st["current_position_entries"] = [{"time": now_local().isoformat(), "price": round(price, 6), "size": round(new_units, 8),
                                       "stufe": 1, "is_add_on": False}]
    debug_log(f"📈 [{symbol}] Neue Position: {target.upper()} @ {price} | Stufe 1")
    await save_bot_state()
    if not cfg["dry_run"]:
        asyncio.create_task(_reverse_confirm(symbol, target, avg_old))
    return True


async def _reverse_confirm(symbol, target, avg_old):
    """Hintergrund nach einem Reverse: den ECHTEN Ø-Einstiegspreis der neuen Position von der Boerse holen und im
    Bot-State nachtragen (der Zielpreis war nur eine Schaetzung). Schlaegt das fehl, bleibt der Zielpreis stehen."""
    try:
        await asyncio.sleep(1.0)
        client = get_lighter_client()
        if client is None:
            return
        try:
            for _ in range(6):
                pos = await get_account_position_from_exchange(client, MARKET_INDICES[symbol], retries=1, delay=0)
                try:
                    real = float(pos.avg_entry_price) if pos is not None and pos.avg_entry_price is not None else None
                except (TypeError, ValueError):
                    real = None
                if real and real > 0 and abs(real - avg_old) > 1e-9:
                    st = BOTS[symbol]["state"]
                    if st.get("position") == target and st.get("entry_count") == 1:
                        old = st["avg_entry_price"]
                        st["avg_entry_price"] = real
                        st["last_entry_price"] = real
                        if st.get("current_position_entries"):
                            st["current_position_entries"][0]["price"] = round(real, 6)
                        debug_log(f"🎯 [{symbol}] Reverse: echter Einstiegspreis von der Börse {real} (Ziel war {round(old, 6)})")
                        await save_bot_state()
                    return
                await asyncio.sleep(0.7)
        finally:
            await _safe_close_client(client)
    except Exception as e:
        debug_log(f"⚠️ [{symbol}] Reverse-Bestätigung fehlgeschlagen (ignoriert)", {"error": str(e)})


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
            await _safe_close_client(client)
            return False
        is_ask = position_side == "long"

        pos_before = await get_account_position_from_exchange(client, market_index, retries=1, delay=0)
        realized_pnl_before = float(pos_before.realized_pnl) if pos_before is not None and pos_before.realized_pnl is not None else None

        tx, tx_hash, err = await place_market_order(client, market_index, symbol, is_ask, base_amount, price, reduce_only=True)
        if err:
            await _safe_close_client(client)
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
        await _safe_close_client(client)
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
            await _safe_close_client(client)
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
        await _safe_close_client(client)
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
<script src="https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.production.js"></script>
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
  .badge.pending { background:rgba(240,82,107,0.18); color:#fecdd3; border:1px solid rgba(240,82,107,0.55); animation: pendingPulse 1.6s ease-in-out infinite; }
  @keyframes pendingPulse { 0%,100% { box-shadow:0 0 0 0 rgba(240,82,107,0.45); } 50% { box-shadow:0 0 0 6px rgba(240,82,107,0); } }
  .start-banner { flex:1 1 100%; background:rgba(240,82,107,0.12); border:1px solid rgba(240,82,107,0.5); color:#fecdd3; border-radius:12px; padding:10px 16px; font-size:13px; line-height:1.5; }
  .coin-pill.pending { border-color:rgba(240,82,107,0.6); }
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
  #wac-price canvas, #wac-wt canvas { background:transparent; border:0; border-radius:0; padding:0; box-shadow:none; }
  #wac-price table, #wac-wt table { width:auto; margin:0; font-size:inherit; }
  #wac-price td, #wac-wt td, #wac-price tr:hover td, #wac-wt tr:hover td { padding:0; border:0; background:transparent; }
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
    <a href="/copytrading" style="color:#93c5fd; text-decoration:none; font-size:13px; margin-right:14px;">📡 Copy-Trading →</a><a href="/screener" style="color:#93c5fd; text-decoration:none; font-size:13px; margin-right:14px;">🔍 Coin Screener →</a><a href="/scalp" style="color:#93c5fd; text-decoration:none; font-size:13px; margin-right:14px;">⚡ Scalp →</a><span id="mode-badge"></span><span id="active-badge"></span>
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
  <button id="btn-reverse" class="danger" style="background:#7c3aed;" title="Position mit einer einzigen Order in die Gegenrichtung drehen (gleiche Größe)">⇄ Reverse (Long ↔ Short)</button>
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
      <option value="scalp_vwap_obv_rsi">Scalp VWAP OBV RSI (Mean-Reversion, VWAP-Bänder + OBV RSI, TP1/TP2)</option>
      <option value="liquidity_waves">Liquidity Waves (Sweep-Level Buyers%/Sellers%, Kontra-Einstieg, TP1/TP2, $-SL)</option>
      <option value="wellenanker">Wellenanker (WaveTrend-Punkte, Nachkauf, TP Gegentrade/Überlauf/Fest, $-SL)</option>
      <option value="ab_breakout">Al-Shatri Breakout (Range-Ausbruch + EMA-Trend + RSI, Presets, Ausstieg wählbar: Wechsel bei Gegen-Signal + $-SL oder Original-Plan mit ATR-SL + TP1/TP2/TP3)</option>
      <option value="rsi_signal">RSI Signal (überverkauft/überkauft, Wechsel-System, optional SuperTrend-/ADX-/MACD-Filter)</option>
      <option value="mvwap_mf_signal">Multi-VWAP Money-Flow Signal (VWAP+MFI/CMF-Oszillator dreht Richtung, Wechsel-System, optional SuperTrend-/ADX-/MACD-Filter)</option>
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
    <input type="number" step="1" min="1" id="rsi_resolution_custom_minutes" placeholder="z.B. 8 oder 24" style="display:none; margin-top:6px; width:140px;">
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
    <input type="number" step="1" min="1" id="rsi_supertrend_filter_resolution_custom_minutes" placeholder="z.B. 8 oder 24" style="display:none; margin-top:6px; width:140px;">
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

  <div data-mode="mvwap_mf_signal" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:6px 0;">
    🌊 <b>Multi-VWAP Money-Flow Signal</b>: Long, sobald der Oszillator (gewichteter Daily/Weekly/Monthly-VWAP-Verbund + MFI/CMF) von fallend auf steigend dreht, Short umgekehrt. Ausstieg wie bei RSI - Wechsel-System, optionaler fester $-SL/$-TP, "SL auf Einstieg bei Gewinn". Darunter dieselben drei Filter aus dem Baukasten.
  </div>
  <div data-mode="mvwap_mf_signal"><label>Zeitrahmen</label>
    <select class="cfg" id="mvwap_resolution">
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
    <input type="number" step="1" min="1" id="mvwap_resolution_custom_minutes" placeholder="z.B. 8 oder 24" style="display:none; margin-top:6px; width:140px;">
  </div>
  <div data-mode="mvwap_mf_signal"><label>Richtung</label>
    <select class="cfg" id="mvwap_direction_mode">
      <option value="both">Beide</option>
      <option value="long_only">Nur Long</option>
      <option value="short_only">Nur Short</option>
    </select>
  </div>
  <div data-mode="mvwap_mf_signal"><label>Max. Nachkäufe pro Position (1 = kein Nachkauf)</label><input type="number" step="1" min="1" max="50" id="mvwap_max_entries"></div>
  <div data-mode="mvwap_mf_signal"><label>Mindestabstand zum letzten Einstieg/Nachkauf ($, 0 = aus)</label>
    <input type="number" step="0.01" min="0" id="mvwap_nachkauf_min_abstand_usd">
    <div style="font-size:12px; color:var(--text-dim); margin-top:4px;">
      Ein weiterer Nachkauf zaehlt nur, wenn sich der Kurs seit dem letzten Fill um mindestens
      diesen $-Betrag bewegt hat (egal in welche Richtung) - verhindert viele Nachkaeufe dicht
      hintereinander bei schnellen Kursbewegungen.
    </div>
  </div>
  <div data-mode="mvwap_mf_signal" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:2px 0;">VWAP-Ebenen (Gewichte sollten zusammen ~1.0 ergeben)</div>
  <div data-mode="mvwap_mf_signal"><label>Daily VWAP</label>
    <select class="cfg" id="mvwap_use_daily"><option value="true">An</option><option value="false">Aus</option></select>
  </div>
  <div data-mode="mvwap_mf_signal" data-requires="mvwap_use_daily"><label>Gewicht Daily</label><input type="number" step="0.05" min="0" max="1" id="mvwap_w_daily"></div>
  <div data-mode="mvwap_mf_signal"><label>Weekly VWAP</label>
    <select class="cfg" id="mvwap_use_weekly"><option value="true">An</option><option value="false">Aus</option></select>
  </div>
  <div data-mode="mvwap_mf_signal" data-requires="mvwap_use_weekly"><label>Gewicht Weekly</label><input type="number" step="0.05" min="0" max="1" id="mvwap_w_weekly"></div>
  <div data-mode="mvwap_mf_signal"><label>Monthly VWAP</label>
    <select class="cfg" id="mvwap_use_monthly"><option value="true">An</option><option value="false">Aus</option></select>
  </div>
  <div data-mode="mvwap_mf_signal" data-requires="mvwap_use_monthly"><label>Gewicht Monthly</label><input type="number" step="0.05" min="0" max="1" id="mvwap_w_monthly"></div>
  <div data-mode="mvwap_mf_signal"><label>Money-Flow-Typ</label>
    <select class="cfg" id="mvwap_mf_source"><option value="MFI">MFI</option><option value="CMF">CMF</option></select>
  </div>
  <div data-mode="mvwap_mf_signal"><label>MFI-Länge</label><input type="number" step="1" min="2" id="mvwap_mf_length"></div>
  <div data-mode="mvwap_mf_signal"><label>CMF-Länge</label><input type="number" step="1" min="2" id="mvwap_cmf_length"></div>
  <div data-mode="mvwap_mf_signal"><label>Money-Flow-Gewicht</label><input type="number" step="0.05" min="0" max="1" id="mvwap_mf_weight"></div>
  <div data-mode="mvwap_mf_signal"><label>EMA-Glättung Oszillator</label><input type="number" step="1" min="1" id="mvwap_smooth_len"></div>
  <div data-mode="mvwap_mf_signal"><label>Signal nur außerhalb OB/OS-Zone</label>
    <select class="cfg" id="mvwap_use_zone_filter"><option value="false">Aus</option><option value="true">An</option></select>
  </div>
  <div data-mode="mvwap_mf_signal" data-requires="mvwap_use_zone_filter"><label>Overbought-Level</label><input type="number" step="0.1" min="0" id="mvwap_ob_level"></div>
  <div data-mode="mvwap_mf_signal" data-requires="mvwap_use_zone_filter"><label>Oversold-Level</label><input type="number" step="0.1" max="0" id="mvwap_os_level"></div>

  <div data-mode="mvwap_mf_signal"><label>Stop-Loss (fester Dollar-Betrag)</label>
    <select class="cfg" id="mvwap_sl_enabled">
      <option value="true">An</option>
      <option value="false">Aus (Ausstieg nur per Gegen-Signal)</option>
    </select>
  </div>
  <div data-mode="mvwap_mf_signal" data-requires="mvwap_sl_enabled"><label>SL-Betrag ($ Verlust der Position)</label><input type="number" step="0.1" min="0.1" id="mvwap_sl_manual_usd"></div>
  <div data-mode="mvwap_mf_signal"><label>Take-Profit (fester Dollar-Betrag)</label>
    <select class="cfg" id="mvwap_tp_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="mvwap_mf_signal" data-requires="mvwap_tp_enabled"><label>TP-Betrag ($ Gewinn der Position)</label><input type="number" step="0.1" min="0.1" id="mvwap_tp_manual_usd"></div>
  <div data-mode="mvwap_mf_signal"><label>SL auf Einstieg bei Gewinn (Break-Even)</label>
    <select class="cfg" id="mvwap_be_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="mvwap_mf_signal" data-requires="mvwap_be_enabled"><label>Gewinn-Schwelle ($ Gewinn der Position)</label><input type="number" step="0.1" min="0.1" id="mvwap_be_trigger_usd"></div>
  <div data-mode="mvwap_mf_signal"><label>Cooldown nach SL (Sek.)</label><input type="number" step="1" id="mvwap_sl_cooldown_seconds"></div>

  <div data-mode="mvwap_mf_signal"><label>SuperTrend-Trendfilter (höhere Zeiteinheit)</label>
    <select class="cfg" id="mvwap_supertrend_filter_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="mvwap_mf_signal" data-requires="mvwap_supertrend_filter_enabled"><label>Trendfilter-Zeiteinheit</label>
    <select class="cfg" id="mvwap_supertrend_filter_resolution">
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
    <input type="number" step="1" min="1" id="mvwap_supertrend_filter_resolution_custom_minutes" placeholder="z.B. 8 oder 24" style="display:none; margin-top:6px; width:140px;">
  </div>
  <div data-mode="mvwap_mf_signal" data-requires="mvwap_supertrend_filter_enabled"><label>SuperTrend-Multiplikator</label><input type="number" step="0.1" min="0.1" id="mvwap_supertrend_filter_multiplier"></div>
  <div data-mode="mvwap_mf_signal" data-requires="mvwap_supertrend_filter_enabled"><label>SuperTrend ATR-Periode</label><input type="number" step="1" min="1" id="mvwap_supertrend_filter_atr_period"></div>

  <div data-mode="mvwap_mf_signal"><label>ADX-Trendfilter</label>
    <select class="cfg" id="mvwap_adx_filter_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="mvwap_mf_signal" data-requires="mvwap_adx_filter_enabled"><label>ADX-Periode</label><input type="number" step="1" min="1" id="mvwap_adx_filter_length"></div>
  <div data-mode="mvwap_mf_signal" data-requires="mvwap_adx_filter_enabled"><label>ADX-Schwelle</label><input type="number" step="1" min="1" id="mvwap_adx_filter_threshold"></div>
  <div data-mode="mvwap_mf_signal" data-requires="mvwap_adx_filter_enabled">
    <label>Modus</label>
    <div style="font-size:12px; color:var(--text-dim); margin-bottom:4px;">
      "Trend fordern" lässt nur handeln, wenn die Schwelle ÜBERSCHRITTEN wird (starker Trend nötig).
      "Trend vermeiden" ist für ein Reversal-System wie MVWAP-MF meist die bessere Wahl: er PAUSIERT
      Einstieg und Nachkauf, solange die Schwelle überschritten ist (starker Auf-/Abwärtstrend),
      und lässt nur in ruhigeren/Seitwärts-Phasen handeln - verhindert genau das "in einen starken
      Trend hinein nachkaufen und im Minus verkaufen".
    </div>
    <select class="cfg" id="mvwap_adx_filter_mode">
      <option value="require_trend">Trend fordern (ADX &gt; Schwelle nötig)</option>
      <option value="avoid_trend">Trend vermeiden (pausiert wenn ADX &gt; Schwelle)</option>
    </select>
  </div>
  <div data-mode="mvwap_mf_signal" data-requires="mvwap_adx_filter_enabled" id="mvwap_adx_filter_directional_wrap"><label>Mit Richtung (+DI/-DI)</label>
    <select class="cfg" id="mvwap_adx_filter_directional">
      <option value="true">An (Long nur bei +DI&gt;-DI, Short umgekehrt)</option>
      <option value="false">Aus (nur Trendstärke, Richtung egal)</option>
    </select>
  </div>

  <div data-mode="mvwap_mf_signal"><label>MACD-Trendfilter</label>
    <select class="cfg" id="mvwap_macd_filter_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="mvwap_mf_signal" data-requires="mvwap_macd_filter_enabled"><label>MACD schnell</label><input type="number" step="1" min="1" id="mvwap_macd_filter_fast"></div>
  <div data-mode="mvwap_mf_signal" data-requires="mvwap_macd_filter_enabled"><label>MACD langsam</label><input type="number" step="1" min="1" id="mvwap_macd_filter_slow"></div>
  <div data-mode="mvwap_mf_signal" data-requires="mvwap_macd_filter_enabled"><label>MACD-Signal</label><input type="number" step="1" min="1" id="mvwap_macd_filter_signal"></div>

  <div data-mode="mvwap_mf_signal"><label>RSI-Überdehnungsfilter</label>
    <select class="cfg" id="mvwap_rsi_filter_enabled">
      <option value="false">Aus</option>
      <option value="true">An (Long erst wenn RSI unter OS, Short erst wenn RSI über OB)</option>
    </select>
  </div>
  <div data-mode="mvwap_mf_signal" data-requires="mvwap_rsi_filter_enabled"><label>RSI-Länge</label><input type="number" step="1" min="1" id="mvwap_rsi_filter_length"></div>
  <div data-mode="mvwap_mf_signal" data-requires="mvwap_rsi_filter_enabled"><label>RSI Oversold (Long erst darunter)</label><input type="number" step="1" min="1" max="100" id="mvwap_rsi_filter_os_level"></div>
  <div data-mode="mvwap_mf_signal" data-requires="mvwap_rsi_filter_enabled"><label>RSI Overbought (Short erst darüber)</label><input type="number" step="1" min="1" max="100" id="mvwap_rsi_filter_ob_level"></div>

  <div data-mode="mvwap_mf_signal"><label>Cloud-Filter (VWAP-Bänder)</label>
    <select class="cfg" id="mvwap_cloud_filter_enabled">
      <option value="false">Aus</option>
      <option value="true">An (Short erst nach Berührung oberes Band, Long erst nach unterem Band)</option>
    </select>
  </div>
  <div data-mode="mvwap_mf_signal" data-requires="mvwap_cloud_filter_enabled"><label>Cloud VWAP-Länge</label><input type="number" step="1" min="1" id="mvwap_cloud_filter_length"></div>
  <div data-mode="mvwap_mf_signal" data-requires="mvwap_cloud_filter_enabled"><label>Cloud Band-Multiplikator</label><input type="number" step="0.1" min="0.1" id="mvwap_cloud_filter_dev_mult"></div>
  <div data-mode="mvwap_mf_signal" data-requires="mvwap_cloud_filter_enabled"><label>Scharfschaltung bei Docht-Berührung</label>
    <select class="cfg" id="mvwap_cloud_filter_touch_arm">
      <option value="false">Aus (Kerzenschluss über/unter Band nötig)</option>
      <option value="true">An (High/Low reicht)</option>
    </select>
  </div>

  <div data-mode="scalp_vwap_obv_rsi" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:6px 0;">
    📉 <b>Scalp VWAP OBV RSI</b>: Mean-Reversion. Long, wenn eine Kerze im grünen Band (Dev2-Dev3-Zone) schließt ODER der Docht mind. die Hälfte des Bandes durchbricht UND der OBV-RSI unter dem unteren Schwellenwert liegt (Short spiegelbildlich im roten Band). Solange dasselbe Signal weiter gilt, legt der Bot bis zu "Max. Nachkäufe" weitere Stufen nach. TP1 (50%) am Mittelband zieht den SL auf Einstieg, TP2 (Rest) am Gegenband. SL ist ein fester %-Abstand vom Ø-Einstieg.
  </div>
  <div data-mode="scalp_vwap_obv_rsi"><label>Zeitrahmen</label>
    <select class="cfg" id="scalp_timeframe">
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
    <input type="number" step="1" min="1" id="scalp_timeframe_custom_minutes" placeholder="z.B. 8 oder 24" style="display:none; margin-top:6px; width:140px;">
  </div>
  <div data-mode="scalp_vwap_obv_rsi"><label>VWAP-Deviation Länge (Kerzen)</label><input type="number" step="1" min="10" id="scalp_vwap_length"></div>
  <div data-mode="scalp_vwap_obv_rsi"><label>OBV-RSI Länge</label><input type="number" step="1" min="2" id="scalp_rsi_length"></div>
  <div data-mode="scalp_vwap_obv_rsi"><label>OBV-RSI oberer Schwellenwert (Short)</label><input type="number" step="1" min="50" max="100" id="scalp_rsi_upper"></div>
  <div data-mode="scalp_vwap_obv_rsi"><label>OBV-RSI unterer Schwellenwert (Long)</label><input type="number" step="1" min="0" max="50" id="scalp_rsi_lower"></div>
  <div data-mode="scalp_vwap_obv_rsi"><label>Docht-Schwelle (Anteil des Bandes, 0.5 = 50%)</label><input type="number" step="0.05" min="0" max="1" id="scalp_docht_threshold"></div>
  <div data-mode="scalp_vwap_obv_rsi"><label>SL-Modus</label>
    <select class="cfg" id="scalp_sl_mode">
      <option value="pct">Prozent vom Ø-Einstieg</option>
      <option value="usd">Fester $-Verlust</option>
    </select>
  </div>
  <div data-mode="scalp_vwap_obv_rsi"><label>Stop-Loss (% vom Ø-Einstieg)</label><input type="number" step="0.05" min="0.05" id="scalp_sl_pct"></div>
  <div data-mode="scalp_vwap_obv_rsi"><label>Stop-Loss ($ Verlust ab Ø-Einstieg)</label><input type="number" step="0.5" min="0.1" id="scalp_sl_usd"></div>
  <div data-mode="scalp_vwap_obv_rsi"><label>Max. Nachkäufe (0 = kein Nachkauf)</label><input type="number" step="1" min="0" max="10" id="scalp_max_nachkauf"></div>
  <div data-mode="scalp_vwap_obv_rsi"><label>Mindestabstand zum letzten Einstieg/Nachkauf ($, 0 = aus)</label>
    <input type="number" step="0.01" min="0" id="scalp_nachkauf_min_abstand_usd">
  </div>
  <div data-mode="scalp_vwap_obv_rsi"><label>Mindestabstand zum letzten Einstieg/Nachkauf (Kerzen, 0 = aus)</label>
    <input type="number" step="1" min="0" id="scalp_nachkauf_min_candles">
  </div>
  <div data-mode="scalp_vwap_obv_rsi">
    <label><input type="checkbox" id="scalp_nachkauf_require_reversal" style="width:auto; vertical-align:middle;"> Nachkauf erst, wenn seit dem letzten Fill eine Kerze NICHT im Band geschlossen hat (statt bei jeder Kerze im Band nachzukaufen, obwohl der Kurs einfach weiter durchläuft)</label>
  </div>
  <div data-mode="scalp_vwap_obv_rsi">
    <label><input type="checkbox" id="scalp_tp1_require_profit" style="width:auto; vertical-align:middle;"> TP1 nur wenn mindestens Breakeven (sonst warten, statt mit kleinem Verlust zu schließen)</label>
  </div>
  <div data-mode="scalp_vwap_obv_rsi">
    <label><input type="checkbox" id="scalp_tp1_full_close" style="width:auto; vertical-align:middle;"> TP1 komplett schließen (100% statt 50%) - danach kein TP2 mehr</label>
  </div>
  <div data-mode="scalp_vwap_obv_rsi">
    <label><input type="checkbox" id="scalp_halfway_sl_enabled" style="width:auto; vertical-align:middle;"> Nach TP1: SL auf halbem Weg zur TP2-Linie auf die Mittellinie nachziehen</label>
  </div>

  <div data-mode="scalp_vwap_obv_rsi"><label>SuperTrend-Trendfilter (übergeordnete, höhere Zeiteinheit)</label>
    <select class="cfg" id="scalp_supertrend_filter_enabled">
      <option value="false">Aus</option>
      <option value="true">An</option>
    </select>
  </div>
  <div data-mode="scalp_vwap_obv_rsi" data-requires="scalp_supertrend_filter_enabled"><label>Trendfilter-Zeiteinheit</label>
    <select class="cfg" id="scalp_supertrend_filter_resolution">
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
    <input type="number" step="1" min="1" id="scalp_supertrend_filter_resolution_custom_minutes" placeholder="z.B. 8 oder 24" style="display:none; margin-top:6px; width:140px;">
  </div>
  <div data-mode="scalp_vwap_obv_rsi" data-requires="scalp_supertrend_filter_enabled"><label>SuperTrend-Multiplikator</label><input type="number" step="0.1" min="0.1" id="scalp_supertrend_filter_multiplier"></div>
  <div data-mode="scalp_vwap_obv_rsi" data-requires="scalp_supertrend_filter_enabled"><label>SuperTrend ATR-Periode</label><input type="number" step="1" min="1" id="scalp_supertrend_filter_atr_period"></div>

  <div data-mode="liquidity_waves" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:6px 0;">
    🌊 <b>Liquidity Waves</b>: Kerzen mit kleinem Körper (relativ zur Spanne) markieren "Sweep"-Level - Hoch bei bärischer, Tief bei bullischer Sweep-Kerze. Von den neuesten "Levels betrachten" zählen nur die noch gültigen (Short-Level über, Long-Level unter dem Kurs) als Buyers%/Sellers%. Long wenn Buyers% unter der Einstiegs-Schwelle fällt (Short spiegelbildlich mit Sellers%). TP1 (50%, SL → Einstieg) sobald der Wert wieder beim TP1-Schwellenwert steht, TP2 (Rest) beim TP2-Schwellenwert. SL ist ein fester $-Verlust ab Ø-Einstieg, kein Nachkauf.
  </div>
  <div data-mode="liquidity_waves"><label>Zeitrahmen</label>
    <select class="cfg" id="liq_timeframe">
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
    <input type="number" step="1" min="1" id="liq_timeframe_custom_minutes" placeholder="z.B. 8 oder 24" style="display:none; margin-top:6px; width:140px;">
  </div>
  <div data-mode="liquidity_waves"><label>Max. Körper (% der Kerzenspanne) für Sweep-Kerze</label><input type="number" step="1" min="1" max="100" id="liq_body_max_pct"></div>
  <div data-mode="liquidity_waves"><label>Levels betrachten (neueste N)</label><input type="number" step="1" min="1" max="100" id="liq_max_levels"></div>
  <div data-mode="liquidity_waves"><label>Long-Sweep nur unter, Short-Sweep nur über dem Kurs zählen</label>
    <select class="cfg" id="liq_side_filter">
      <option value="true">An</option>
      <option value="false">Aus (alle Levels zählen, unabhängig von der Kursrichtung)</option>
    </select>
  </div>
  <div data-mode="liquidity_waves"><label>Älteres Level ungültig bei späterem Sweep auf gleicher Höhe</label>
    <select class="cfg" id="liq_dup_remove">
      <option value="true">An</option>
      <option value="false">Aus</option>
    </select>
  </div>
  <div data-mode="liquidity_waves" data-requires="liq_dup_remove"><label>Toleranz für "gleiche Höhe" ($)</label><input type="number" step="0.01" min="0" id="liq_dup_tolerance_usd"></div>
  <div data-mode="liquidity_waves"><label>Einstiegs-Schwelle (Buyers%/Sellers% unter diesem Wert)</label><input type="number" step="0.5" min="0" max="49" id="liq_entry_threshold_pct"></div>
  <div data-mode="liquidity_waves"><label>TP1-Schwelle (%, 50% Teil-Exit, SL → Einstieg)</label><input type="number" step="1" min="1" max="99" id="liq_tp1_pct"></div>
  <div data-mode="liquidity_waves"><label>TP2-Schwelle (%, Rest-Exit)</label><input type="number" step="1" min="1" max="100" id="liq_tp2_pct"></div>
  <div data-mode="liquidity_waves"><label>Stop-Loss ($ Verlust ab Ø-Einstieg)</label><input type="number" step="0.5" min="0.1" id="liq_sl_usd"></div>
  <div data-mode="liquidity_waves" style="grid-column:1/-1;">
    <label><input type="checkbox" id="liq_tp1_require_profit" style="width:auto; vertical-align:middle;"> TP1 nur wenn mindestens Breakeven (sonst warten auf TP2 oder SL, statt mit Verlust zu schließen)</label>
  </div>
  <div data-mode="liquidity_waves"><label>Max. Nachkäufe (0 = aus)</label><input type="number" step="1" min="0" max="2" id="liq_max_nachkauf"></div>
  <div data-mode="liquidity_waves"><label>Nachkauf-Trigger: Fortschritt Richtung TP1 (%), danach zurück unter Einstiegs-Schwelle (nur bei Max. Nachkäufe &gt; 0)</label><input type="number" step="1" min="1" max="99" id="liq_nachkauf_progress_pct"></div>

  <div data-mode="wellenanker" style="grid-column:1/-1; font-size:12px; color:var(--text-dim); padding:6px 0;">
    🌊 <b>Wellenanker</b>: Nachbau der WaveTrend-Punkte (ohne Geldfluss/Divergenzen/Panel). Long-Punkt,
    wenn die Welle die Signallinie von unten kreuzt, während die Signallinie unter -Zone1 steht
    (Short spiegelbildlich über +Zone1). TP wählbar: Gegentrade (Exit beim entgegengesetzten Punkt),
    Überlauflinie (Exit sobald die Signallinie wieder bis dahin zurückgelaufen ist) oder fester
    $-Betrag. SL ist immer ein fester $-Verlust ab Ø-Einstieg. Nachkauf bei jedem weiteren Punkt in
    dieselbe Richtung (bis zu 4).
  </div>
  <div data-mode="wellenanker"><label>Zeitrahmen</label>
    <select class="cfg" id="wa_timeframe">
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
    <input type="number" step="1" min="1" id="wa_timeframe_custom_minutes" placeholder="z.B. 8 oder 24" style="display:none; margin-top:6px; width:140px;">
  </div>
  <div data-mode="wellenanker"><label>Quelle der Welle</label>
    <select class="cfg" id="wa_src">
      <option value="close">Close</option>
      <option value="hlc3">HLC3</option>
      <option value="ohlc4">OHLC4</option>
      <option value="hl2">HL2</option>
    </select>
  </div>
  <div data-mode="wellenanker"><label>Kanal-Länge</label><input type="number" step="1" min="1" id="wa_n1"></div>
  <div data-mode="wellenanker"><label>Durchschnitt-Länge</label><input type="number" step="1" min="1" id="wa_n2"></div>
  <div data-mode="wellenanker"><label>Signallinien-Glättung</label><input type="number" step="1" min="1" id="wa_sig_len"></div>
  <div data-mode="wellenanker"><label>Wellen-Skalierung</label><input type="number" step="0.05" min="0.5" max="3.0" id="wa_wave_scale"></div>
  <div data-mode="wellenanker"><label>Einstiegs-Modus</label>
    <select class="cfg" id="wa_entry_mode">
      <option value="zone">Zone: Kreuzung der Linien außerhalb ±Zone 1</option>
      <option value="level">Level: Durchbruch durch −X (Long) / +X (Short)</option>
      <option value="cross">Kreuzung zu Kreuzung: jede Kreuzung der beiden Linien (egal wo) = Wechsel Long ↔ Short</option>
    </select>
  </div>
  <div data-mode="wellenanker" data-requires="wa_entry_mode" data-requires-value="zone"><label>Zone 1 (Long &lt; -X, Short &gt; X)</label><input type="number" step="1" min="1" max="100" id="wa_zone1"></div>
  <div data-mode="wellenanker" data-requires="wa_entry_mode" data-requires-value="level"><label>Level-Abstand ± (0 bis 30): Long bricht −X nach oben durch, Short bricht +X nach unten durch</label><input type="number" step="0.5" min="0" max="30" id="wa_level"></div>
  <div data-mode="wellenanker" data-requires="wa_entry_mode" data-requires-value="level"><label>Welche Linie zählt</label>
    <select class="cfg" id="wa_level_line">
      <option value="signal">Signallinie</option>
      <option value="wave">Welle</option>
      <option value="both">Beide (beide müssen durch das Level)</option>
    </select>
  </div>
  <div data-mode="wellenanker"><label>Max. Nachkäufe (0 = aus)</label><input type="number" step="1" min="0" max="20" id="wa_max_nachkauf"></div>
  <div data-mode="wellenanker"><label><input type="checkbox" id="wa_reverse_only_profit" style="width:auto; vertical-align:middle;"> Gegentrade nur wenn die Position im Plus ist (nie mit Verlust schließen/drehen)</label></div>
  <div data-mode="wellenanker"><label>Nachkauf-Abstand zum letzten Einstieg (%, 0 = aus) – bei „Kreuzung zu Kreuzung": nachkaufen, wenn der Kurs so viel % GEGEN die Position läuft</label><input type="number" step="0.01" min="0" id="wa_nachkauf_min_pct"></div>
  <div data-mode="wellenanker"><label>TP-Modus</label>
    <select class="cfg" id="wa_tp_mode">
      <option value="gegentrade">Gegentrade (Exit beim entgegengesetzten Punkt)</option>
      <option value="ueberlauf">Überlauflinie (Exit bei Rücklauf zur Linie)</option>
      <option value="fester_betrag">Fester $-Betrag</option>
      <option value="kreuzung">Gegenkreuzung der beiden Linien (egal wo sie sich kreuzen)</option>
    </select>
  </div>
  <div data-mode="wellenanker" data-requires="wa_tp_mode" data-requires-value="ueberlauf"><label>Überlauflinie (±)</label><input type="number" step="1" min="1" max="100" id="wa_ueberlauf_level"></div>
  <div data-mode="wellenanker" data-requires="wa_tp_mode" data-requires-value="fester_betrag"><label>TP-Betrag ($ Gewinn der Position)</label><input type="number" step="0.1" min="0.1" id="wa_tp_usd"></div>
  <div data-mode="wellenanker"><label>Stop-Loss ($ Verlust ab Ø-Einstieg)</label><input type="number" step="0.5" min="0.1" id="wa_sl_usd"></div>
  <div data-mode="wellenanker"><label>🧭 Trendfilter (Marktstruktur: nur Trades in Trendrichtung)</label>
    <select class="cfg" id="wa_trend_mode">
      <option value="off">Aus</option>
      <option value="intern">Intern (kurze Pivots)</option>
      <option value="swing">Swing (lange Pivots)</option>
    </select></div>
  <div data-mode="wellenanker"><label>Trend-Zeitebene</label>
    <select class="cfg" id="wa_trend_tf">
      <option value="5m">5m</option><option value="15m">15m</option><option value="30m">30m</option>
      <option value="1h">1h</option><option value="4h">4h</option><option value="1d">1d</option><option value="1w">1w</option>
    </select></div>
  <div data-mode="wellenanker"><label>Pivot-Länge intern</label><input type="number" step="1" min="2" id="wa_trend_ilen"></div>
  <div data-mode="wellenanker"><label>Pivot-Länge swing</label><input type="number" step="1" min="2" id="wa_trend_slen"></div>























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

<div data-mode-section="wellenanker" style="display:none;">
<h2 class="section-title">🌊 Wellenanker Chart (Live &amp; Backtest)</h2>
<div class="panel-card">
  <div style="display:flex; gap:8px; align-items:center; flex-wrap:wrap; margin-bottom:10px;">
    <button type="button" id="wac-tab-live" style="padding:8px 16px;">🔴 Live</button>
    <button type="button" id="wac-tab-bt" style="padding:8px 16px; opacity:.6;">📊 Backtest-Trades</button>
    <span id="wac-info" style="font-size:13px; color:var(--text-dim);"></span>
  </div>
  <div id="wac-price" style="height:420px;"></div>
  <div style="font-size:12px; color:var(--text-dim); margin:8px 0 4px;">WaveTrend (Welle blau, Signallinie gelb) – gestrichelt: Levels</div>
  <div id="wac-wt" style="height:180px;"></div>
  <div id="wac-trend" style="display:flex; gap:8px; flex-wrap:wrap; align-items:center; margin-top:10px; font-size:12px;"></div>
  <div style="font-size:12px; color:var(--text-dim); margin-top:6px;" id="wac-legend">Live: Pfeile = Signale (laufende Kerze zählt sofort). Backtest: ▲/▼ Einstieg, ● Ausstieg mit PnL – Klick auf eine Trade-Zeile springt zum Trade.</div>
</div>
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
    <div class="label" style="margin-top:16px; margin-bottom:8px;">Alle Trades (neueste zuerst)</div>
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

<div data-mode-section="wellenanker" style="display:none;">
<h2 class="section-title">🎲 Wellenanker Sweep (Zone 1 × Max. Nachkäufe × Stop-Loss $)</h2>
<div class="panel-card">
  <div style="font-size:13px; color:var(--text-dim); margin-bottom:12px;">
    Testet Zone 1 gegen Max. Nachkäufe (0-20) und Stop-Loss ($). TP-Modus (samt Überlauflinie/TP-
    Betrag) sowie die Wellen-Parameter (Kanal-/Durchschnitt-Länge, Signallinien-Glättung, Quelle,
    Wellen-Skalierung) kommen unverändert aus den Einstellungen oben.
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Zeitraum (Tage)</label><input type="number" step="1" id="wa-sweep-days" value="30" style="width:90px;"></div>
    <div><label>Robustheits-Check: beste N ausschließen</label><input type="number" step="1" min="0" id="wa-sweep-exclude-top-n" value="1" style="width:90px;"></div>
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Zone 1 von</label><input type="number" step="1" min="1" id="wa-sweep-zone1-min" value="30" style="width:80px;"></div>
    <div><label>bis</label><input type="number" step="1" min="1" id="wa-sweep-zone1-max" value="80" style="width:80px;"></div>
    <div><label>Schritt</label><input type="number" step="1" min="1" id="wa-sweep-zone1-step" value="1" style="width:80px;"></div>
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div style="color:var(--text-dim); font-size:12px; max-width:200px;">Nur im Modus „Level" (statt Zone 1):</div>
    <div><label>Level-Abstand von</label><input type="number" step="0.5" min="0" max="30" id="wa-sweep-lvl-min" value="0" style="width:80px;"></div>
    <div><label>bis</label><input type="number" step="0.5" min="0" max="30" id="wa-sweep-lvl-max" value="30" style="width:80px;"></div>
    <div><label>Schritt</label><input type="number" step="0.5" min="0.5" id="wa-sweep-lvl-step" value="2" style="width:80px;"></div>
    <label style="font-size:13px;"><input type="checkbox" id="wa-sweep-line-signal" checked style="width:auto; vertical-align:middle;"> Signallinie</label>
    <label style="font-size:13px;"><input type="checkbox" id="wa-sweep-line-wave" checked style="width:auto; vertical-align:middle;"> Welle</label>
    <label style="font-size:13px;"><input type="checkbox" id="wa-sweep-line-both" checked style="width:auto; vertical-align:middle;"> Beide</label>
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Max. Nachkäufe von</label><input type="number" step="1" min="0" max="20" id="wa-sweep-nachkauf-min" value="0" style="width:80px;"></div>
    <div><label>bis</label><input type="number" step="1" min="0" max="20" id="wa-sweep-nachkauf-max" value="4" style="width:80px;"></div>
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Stop-Loss ($) von</label><input type="number" step="0.5" min="0.1" id="wa-sweep-sl-min" value="5" style="width:80px;"></div>
    <div><label>bis</label><input type="number" step="0.5" min="0.1" id="wa-sweep-sl-max" value="25" style="width:80px;"></div>
    <div><label>Schritt</label><input type="number" step="0.5" min="0.1" id="wa-sweep-sl-step" value="2" style="width:80px;"></div>
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <button id="btn-wa-sweep" style="padding:12px 24px;">🎲 Sweep starten</button>
  </div>
  <div id="wa-sweep-status" style="color:var(--text-dim); font-size:13px;"></div>
  <h3 style="margin-top:20px; font-size:14px; color:var(--text-dim); display:none;" id="wa-sweep-top-title">📈 Die 30 besten Kombinationen</h3>
  <table id="wa-sweep-results-table" style="display:none; margin-top:8px;">
    <thead><tr>
      <th class="sortable" data-key="wa_zone1">Zone 1 / Level ⇅</th>
      <th class="sortable" data-key="wa_level_line">Linie ⇅</th>
      <th class="sortable" data-key="wa_max_nachkauf">Max. Nachkäufe ⇅</th>
      <th class="sortable" data-key="wa_sl_usd">SL $ ⇅</th>
      <th class="sortable" data-key="trades">Trades ⇅</th>
      <th class="sortable" data-key="win_rate_pct">Trefferquote ⇅</th>
      <th class="sortable" data-key="total_pnl_usd">PnL $ ⇅</th>
      <th class="sortable" data-key="total_pnl_excl_top_n_usd">PnL ohne beste N $ ⇅</th>
      <th class="sortable" data-key="max_drawdown_usd">Max DD $ ⇅</th>
      <th class="sortable" data-key="avg_bars_held">Ø Kerzen gehalten ⇅</th>
    </tr></thead>
    <tbody></tbody>
  </table>
  <h3 style="margin-top:20px; font-size:14px; color:var(--text-dim); display:none;" id="wa-sweep-worst-title">📉 Die 20 schlechtesten Werte (nach PnL, unabhängig von der Trade-Anzahl)</h3>
  <table id="wa-sweep-worst-table" style="display:none; margin-top:8px;">
    <thead><tr>
      <th class="sortable" data-key="wa_zone1">Zone 1 / Level ⇅</th>
      <th class="sortable" data-key="wa_level_line">Linie ⇅</th>
      <th class="sortable" data-key="wa_max_nachkauf">Max. Nachkäufe ⇅</th>
      <th class="sortable" data-key="wa_sl_usd">SL $ ⇅</th>
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

<div data-mode-section="liquidity_waves" style="display:none;">
<h2 class="section-title">🎲 Liquidity Waves Sweep (Einstiegs-Schwelle × Levels keep alive)</h2>
<div class="panel-card">
  <div style="font-size:13px; color:var(--text-dim); margin-bottom:12px;">
    Testet die Einstiegs-Schwelle (Buyers%/Sellers%) gegen "Levels keep alive". TP1/TP2/SL,
    Nachkauf-Einstellungen, Sweep-Erkennung (max. Körper-%), Side-Filter und Duplikat-Entfernung
    kommen unverändert aus den Einstellungen oben.
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Zeitraum (Tage)</label><input type="number" step="1" id="liq-sweep-days" value="30" style="width:90px;"></div>
    <div><label>Robustheits-Check: beste N ausschließen</label><input type="number" step="1" min="0" id="liq-sweep-exclude-top-n" value="1" style="width:90px;"></div>
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Einstiegs-Schwelle (%) von</label><input type="number" step="0.5" min="0.1" id="liq-sweep-entry-min" value="1" style="width:80px;"></div>
    <div><label>bis</label><input type="number" step="0.5" min="0.1" id="liq-sweep-entry-max" value="10" style="width:80px;"></div>
    <div><label>Schritt</label><input type="number" step="0.5" min="0.1" id="liq-sweep-entry-step" value="1" style="width:80px;"></div>
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <div><label>Levels keep alive von</label><input type="number" step="1" min="1" id="liq-sweep-levels-min" value="30" style="width:80px;"></div>
    <div><label>bis</label><input type="number" step="1" min="1" id="liq-sweep-levels-max" value="100" style="width:80px;"></div>
    <div><label>Schritt</label><input type="number" step="1" min="1" id="liq-sweep-levels-step" value="5" style="width:80px;"></div>
  </div>
  <div style="display:flex; gap:12px; align-items:end; flex-wrap:wrap; margin-bottom:12px;">
    <button id="btn-liq-sweep" style="padding:12px 24px;">🎲 Sweep starten</button>
  </div>
  <div id="liq-sweep-status" style="color:var(--text-dim); font-size:13px;"></div>
  <h3 style="margin-top:20px; font-size:14px; color:var(--text-dim); display:none;" id="liq-sweep-top-title">📈 Die 30 besten Kombinationen</h3>
  <table id="liq-sweep-results-table" style="display:none; margin-top:8px;">
    <thead><tr>
      <th class="sortable" data-key="liq_entry_threshold_pct">Einstiegs-Schwelle % ⇅</th>
      <th class="sortable" data-key="liq_max_levels">Levels keep alive ⇅</th>
      <th class="sortable" data-key="trades">Trades ⇅</th>
      <th class="sortable" data-key="win_rate_pct">Trefferquote ⇅</th>
      <th class="sortable" data-key="total_pnl_usd">PnL $ ⇅</th>
      <th class="sortable" data-key="total_pnl_excl_top_n_usd">PnL ohne beste N $ ⇅</th>
      <th class="sortable" data-key="max_drawdown_usd">Max DD $ ⇅</th>
      <th class="sortable" data-key="avg_bars_held">Ø Kerzen gehalten ⇅</th>
    </tr></thead>
    <tbody></tbody>
  </table>
  <h3 style="margin-top:20px; font-size:14px; color:var(--text-dim); display:none;" id="liq-sweep-worst-title">📉 Die 20 schlechtesten Werte (nach PnL, unabhängig von der Trade-Anzahl)</h3>
  <table id="liq-sweep-worst-table" style="display:none; margin-top:8px;">
    <thead><tr>
      <th class="sortable" data-key="liq_entry_threshold_pct">Einstiegs-Schwelle % ⇅</th>
      <th class="sortable" data-key="liq_max_levels">Levels keep alive ⇅</th>
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
document.getElementById('btn-reverse').addEventListener('click', async () => {
  if (!confirm(`Position für ${currentSymbol} jetzt in die Gegenrichtung drehen (gleiche Größe)?`)) return;
  const res = await fetch(`/api/reverse?symbol=${currentSymbol}`, { method:'POST' });
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
      if (data.chart) { WAC.bt = data; wacShowBacktest(); } else { WAC.bt = null; }
      resultsEl.style.display = 'block';
    }
  } catch (e) {
    if (btSymbol !== currentSymbol) return;
    statusEl.innerText = `❌ Fehler: ${e}`;
  }
  if (btSymbol === currentSymbol) btn.disabled = false;
});

// ===== Wellenanker Chart (Live + Backtest) =====
const WAC = {mode: 'live', price: null, wt: null, cs: null, l1: null, l2: null, lines: [], timer: null, bt: null, inited: false, sym: null, fitted: false};
function wacInit() {
  if (WAC.inited || !window.LightweightCharts) return;
  const LW = window.LightweightCharts;
  const opt = {layout: {background: {color: 'transparent'}, textColor: '#9aa4bf'}, grid: {vertLines: {color: '#141c33'}, horzLines: {color: '#141c33'}},
               rightPriceScale: {borderColor: '#1c2542'}, timeScale: {borderColor: '#1c2542', timeVisible: true, secondsVisible: true}, crosshair: {mode: 0}};
  WAC.price = LW.createChart(document.getElementById('wac-price'), Object.assign({autoSize: true}, opt));
  WAC.wt = LW.createChart(document.getElementById('wac-wt'), Object.assign({autoSize: true}, opt));
  WAC.cs = WAC.price.addCandlestickSeries({upColor: '#22c55e', downColor: '#ef4444', borderVisible: false, wickUpColor: '#22c55e', wickDownColor: '#ef4444'});
  WAC.l1 = WAC.wt.addLineSeries({color: '#3b82f6', lineWidth: 2, priceLineVisible: false});
  WAC.l2 = WAC.wt.addLineSeries({color: '#eab308', lineWidth: 1, priceLineVisible: false});
  let busy = false;
  const sync = (from, to) => from.timeScale().subscribeVisibleTimeRangeChange(r => { if (busy || !r) return; busy = true; try { to.timeScale().setVisibleRange(r); } catch (e) {} busy = false; });
  sync(WAC.price, WAC.wt); sync(WAC.wt, WAC.price);
  WAC.inited = true;
}
function wacSetLevels(levels) {
  WAC.lines.forEach(x => WAC.l1.removePriceLine(x)); WAC.lines = [];
  (levels || []).concat([0]).forEach(v => WAC.lines.push(WAC.l1.createPriceLine({price: v, color: v === 0 ? '#3a4466' : '#6b7280', lineWidth: 1, lineStyle: 2, axisLabelVisible: true, title: String(v)})));
}
let wacPosLines = [];
function wacSetPosition(p) {
  wacPosLines.forEach(x => WAC.cs.removePriceLine(x)); wacPosLines = [];
  if (!p) return;
  wacPosLines.push(WAC.cs.createPriceLine({price: p.avg, color: p.dir === 'long' ? '#22c55e' : '#ef4444', lineWidth: 2, lineStyle: 0, axisLabelVisible: true, title: p.dir.toUpperCase() + ' Ø'}));
  wacPosLines.push(WAC.cs.createPriceLine({price: p.sl, color: '#f97316', lineWidth: 1, lineStyle: 2, axisLabelVisible: true, title: 'SL'}));
}
function wacRender(d, markers, keepRange) {
  WAC.cs.setData(d.candles); WAC.l1.setData(d.wt1); WAC.l2.setData(d.wt2);
  WAC.cs.setMarkers(markers);
  wacSetLevels(d.levels);
}
function wacSigMarkers(d) {
  return (d.markers || []).map(m => m.kind === 'long'
    ? {time: m.time, position: 'belowBar', color: '#22c55e', shape: 'arrowUp', text: ''}
    : {time: m.time, position: 'aboveBar', color: '#ef4444', shape: 'arrowDown', text: ''});
}
async function wacPollLive() {
  if (WAC.mode !== 'live') return;
  const sec = document.querySelector('#wac-price');
  if (!sec || sec.offsetParent === null) return;   // nicht sichtbar (anderer Modus) -> nichts abfragen
  wacInit(); if (!WAC.inited) return;
  const sym = currentSymbol;
  try {
    const r = await fetch(`/api/wa/live_chart?symbol=${sym}`);
    const d = await r.json();
    if (WAC.mode !== 'live' || sym !== currentSymbol) return;
    const info = document.getElementById('wac-info');
    if (!d.ok) { info.innerText = d.reason || 'keine Daten'; return; }
    const first = WAC.sym !== sym || !WAC.fitted;
    wacRender(d, wacSigMarkers(d));
    wacSetPosition(d.position);
    if (first) { WAC.price.timeScale().setVisibleLogicalRange({from: d.candles.length - 120, to: d.candles.length + 5}); WAC.fitted = true; WAC.sym = sym; }
    const p = d.position;
    info.innerText = `${sym} ${d.resolution} · Kurs ${d.price ?? '-'} · WT1 ${d.wt1v} / WT2 ${d.wt2v}` + (p ? ` · ${p.dir.toUpperCase()} Ø ${p.avg.toFixed(2)} PnL ${p.pnl.toFixed(2)} $` : ' · flat') + (d.active ? '' : ' · Bot nicht aktiv (nur Anzeige)');
  } catch (e) { /* naechster Versuch */ }
}
function wacBtMarkers(trades) {
  const out = [];
  (trades || []).forEach(t => {
    const long = t.dir === 'long';
    if (t.exit === null || t.exit === undefined) {
      const ts = t.row_ts || t.entry_ts;
      if (ts) out.push({time: Math.floor(ts / 1000), position: long ? 'belowBar' : 'aboveBar', color: long ? '#22c55e' : '#ef4444', shape: long ? 'arrowUp' : 'arrowDown', text: t.reason && t.reason.startsWith('NACHKAUF') ? '+' : ''});
    } else if (t.exit_ts) {
      const win = t.pnl >= 0;
      out.push({time: Math.floor(t.exit_ts / 1000), position: long ? 'aboveBar' : 'belowBar', color: win ? '#22c55e' : '#ef4444', shape: 'circle', text: (t.pnl >= 0 ? '+' : '') + t.pnl.toFixed(1)});
    }
  });
  out.sort((a, b) => a.time - b.time);
  return out;
}
function wacShowBacktest() {
  WAC.mode = 'bt';
  document.getElementById('wac-tab-bt').style.opacity = 1; document.getElementById('wac-tab-live').style.opacity = .6;
  wacInit(); if (!WAC.inited) return;
  wacSetPosition(null);
  const b = WAC.bt, info = document.getElementById('wac-info');
  if (!b) { info.innerText = 'Noch kein Backtest – unten „Backtest starten“ klicken.'; return; }
  wacRender(b.chart, wacBtMarkers(b.trades));
  WAC.price.timeScale().fitContent();
  const n = (b.trades || []).filter(t => t.pnl !== null && t.pnl !== undefined).length;
  info.innerText = `Backtest ${b.symbol} ${b.resolution}: ${n} Trades im Chart (letzte ${b.chart.candles.length} Kerzen)`;
}
function wacShowLive() {
  WAC.mode = 'live'; WAC.fitted = false;
  document.getElementById('wac-tab-live').style.opacity = 1; document.getElementById('wac-tab-bt').style.opacity = .6;
  wacPollLive();
}
function wacZoomTrade(t) {
  if (!WAC.inited || WAC.mode !== 'bt' || !t) return;
  const a = Math.floor((t.entry_ts || t.row_ts) / 1000), z = Math.floor((t.exit_ts || t.row_ts || t.entry_ts) / 1000);
  const cc = (WAC.bt && WAC.bt.chart.candles) || [], bar = cc.length > 1 ? cc[1].time - cc[0].time : 60;
  const pad = Math.max(bar * 20, (z - a) * 0.6);
  WAC.price.timeScale().setVisibleRange({from: a - pad, to: z + pad});
  document.getElementById('wac-price').scrollIntoView({behavior: 'smooth', block: 'center'});
}
document.getElementById('wac-tab-live').addEventListener('click', wacShowLive);
document.getElementById('wac-tab-bt').addEventListener('click', wacShowBacktest);
async function wacPollTrend() {
  const box = document.getElementById('wac-trend');
  if (!box || box.offsetParent === null) return;
  try {
    const r = await fetch(`/api/wa/trend?symbol=${currentSymbol}`);
    const d = await r.json();
    if (!d.rows) return;
    const cell = (v) => v === 1 ? '<b style="color:#22c55e">▲ BULLISH</b>' : v === -1 ? '<b style="color:#ef4444">▼ BEARISH</b>' : '<span style="color:#6b7280">–</span>';
    const pick = d.mode === 'swing' ? 'swing' : 'intern';
    box.innerHTML = '<span style="color:var(--text-dim)">🧭 Trend (Marktstruktur):</span>' + d.rows.map(x =>
      `<span style="padding:4px 8px; border:1px solid ${d.mode !== 'off' && x.tf === d.tf ? '#3b82f6' : 'var(--panel-border)'}; border-radius:8px;">${x.tf} · intern ${cell(x.intern)} · swing ${cell(x.swing)}</span>`).join('') +
      `<span style="color:var(--text-dim)">${d.mode === 'off' ? 'Filter aus' : 'Filter: ' + d.mode + ' ' + d.tf + ' → ' + (d.active === 1 ? 'nur Long' : d.active === -1 ? 'nur Short' : 'beide')}</span>`;
  } catch (e) {}
}
setInterval(wacPollLive, 2000);
setInterval(wacPollTrend, 30000); setTimeout(wacPollTrend, 3000);
document.getElementById('bt-trades-table').addEventListener('click', ev => {
  const tr = ev.target.closest('tbody tr'); if (!tr) return;
  const i = Array.from(tr.parentNode.children).indexOf(tr);
  if (window.btTradesShown && window.btTradesShown[i]) { if (WAC.mode !== 'bt') wacShowBacktest(); wacZoomTrade(window.btTradesShown[i]); }
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
  if (i === 0) { btColorMap = computeBtColorMap(allRows); window.btTradesShown = allRows; }
  const groupColor = btColorMap[String(r.entry_ts)];
  // Ersteinstiegs-/Nachkauf-Zeilen (siehe _bt_record_addon) sind noch offen - kein Exit/PnL
  // bis die Position tatsaechlich schliesst (eigene Zeile aus _bt_close_trade).
  const isOpenRow = r.pnl === null || r.pnl === undefined;
  const pnlClass = isOpenRow ? '' : (r.pnl > 0 ? 'green' : r.pnl < 0 ? 'red' : '');
  const rowStyle = isOpenRow
    ? `border-left: 4px solid ${groupColor}; opacity: 0.75;`
    : `border-left: 4px solid ${groupColor};`;
  // Bei EINSTIEG/NACHKAUF-Zeilen den TATSAECHLICHEN Kerzenzeitpunkt dieser Stufe zeigen
  // (row_ts), nicht den urspruenglichen Einstiegszeitpunkt der Position (entry_ts) - sonst
  // sehen alle Stufen einer Position faelschlich nach demselben Zeitpunkt aus.
  const startTs = isOpenRow ? r.row_ts : r.entry_ts;
  return `
  <tr style="${rowStyle}; cursor:pointer;" title="Klick: im Chart anzeigen">
    <td>${fmtTs(startTs)}</td>
    <td>${r.dir === 'long' ? '🟢 Long' : '🔴 Short'}</td>
    <td>${r.entry}</td>
    <td>${isOpenRow ? '–' : fmtTs(r.exit_ts)}</td>
    <td>${isOpenRow ? '–' : r.exit}</td>
    <td>${r.reason}</td>
    <td class="${pnlClass}">${isOpenRow ? '–' : r.pnl.toFixed(2)}</td>
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
document.querySelectorAll('#da_resolution, #es_resolution, #ht_resolution, #cp_resolution, #utb_resolution, #wtc_resolution, #pk_resolution, #pk_mtf_tf1, #pk_mtf_tf2, #pk_mtf_tf3, #utb_mtf_tf1, #utb_mtf_tf2, #utb_mtf_tf3, #fr_resolution, #cd_resolution, #fr_zscore_resolution, #cd_zscore_resolution, #rf_resolution, #rf_zscore_resolution, #utb_zscore_resolution, #fr_mtf_tf1, #fr_adx_resolution, #sr_resolution, #sr_adx_resolution, #sr_ema_resolution, #hvd_resolution, #hvd_adx_filter_resolution, #ab_resolution, #ab_trend_filter_resolution, #hvd_trend_filter_resolution, #rsi_resolution, #rsi_supertrend_filter_resolution, #mvwap_resolution, #mvwap_supertrend_filter_resolution, #scalp_timeframe, #scalp_supertrend_filter_resolution, #liq_timeframe, #wa_timeframe').forEach(sel => {
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
  document.getElementById('liq-sweep-status').innerText = '';
  document.getElementById('liq-sweep-results-table').style.display = 'none';
  document.getElementById('liq-sweep-worst-table').style.display = 'none';
  document.getElementById('liq-sweep-worst-title').style.display = 'none';
  window.liqSweepResultsData = [];
  window.liqSweepWorstData = [];
  document.getElementById('wa-sweep-status').innerText = '';
  document.getElementById('wa-sweep-results-table').style.display = 'none';
  document.getElementById('wa-sweep-worst-table').style.display = 'none';
  document.getElementById('wa-sweep-worst-title').style.display = 'none';
  window.waSweepResultsData = [];
  window.waSweepWorstData = [];
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

document.getElementById('btn-liq-sweep').addEventListener('click', async () => {
  const btn = document.getElementById('btn-liq-sweep');
  const statusEl = document.getElementById('liq-sweep-status');
  const tables = {top: document.getElementById('liq-sweep-results-table'), worst: document.getElementById('liq-sweep-worst-table')};
  const titles = {top: document.getElementById('liq-sweep-top-title'), worst: document.getElementById('liq-sweep-worst-title')};
  const sweepSymbol = currentSymbol;
  const payload = {
    days: parseInt(document.getElementById('liq-sweep-days').value) || 30,
    exclude_top_n: parseInt(document.getElementById('liq-sweep-exclude-top-n').value) || 0,
    entry_min: parseFloat(document.getElementById('liq-sweep-entry-min').value),
    entry_max: parseFloat(document.getElementById('liq-sweep-entry-max').value),
    entry_step: parseFloat(document.getElementById('liq-sweep-entry-step').value),
    levels_min: parseInt(document.getElementById('liq-sweep-levels-min').value),
    levels_max: parseInt(document.getElementById('liq-sweep-levels-max').value),
    levels_step: parseInt(document.getElementById('liq-sweep-levels-step').value),
    config: buildConfigPayload(),
  };
  btn.disabled = true;
  Object.values(tables).forEach(t => t.style.display = 'none');
  Object.values(titles).forEach(t => t.style.display = 'none');
  statusEl.innerText = `⏳ Lade Kerzen und teste alle Kombinationen...`;
  try {
    const res = await fetch(`/api/liq_sweep?symbol=${sweepSymbol}`, {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)
    });
    const data = await res.json();
    if (sweepSymbol !== currentSymbol) return;
    if (data.error) {
      statusEl.innerText = `❌ ${data.error}`;
    } else {
      statusEl.innerText = `${data.combos_tested} Kombinationen getestet auf ${data.candles_processed} Kerzen (${data.actual_days_covered} Tage, ${data.resolution}) - Ergebnisse mit weniger als ${data.min_reliable_trades} Trades stehen unten in den Listen.`;
      window.liqSweepResultsData = data.results || [];
      window.liqSweepWorstData = data.worst_results || [];
      renderLiqSweepResults();
      renderLiqSweepWorst();
      Object.values(tables).forEach(t => t.style.display = '');
      Object.values(titles).forEach(t => t.style.display = '');
    }
  } catch (e) {
    if (sweepSymbol !== currentSymbol) return;
    statusEl.innerText = `❌ Fehler: ${e}`;
  }
  if (sweepSymbol === currentSymbol) btn.disabled = false;
});

window.liqSweepResultsData = [];
window.liqSweepWorstData = [];
const liqSweepRowHtml = (r) => `
  <tr>
    <td>${r.liq_entry_threshold_pct}</td>
    <td>${r.liq_max_levels}</td>
    <td>${r.trades}</td>
    <td>${r.win_rate_pct}%</td>
    <td class="${r.total_pnl_usd >= 0 ? 'green' : 'red'}">${r.total_pnl_usd}</td>
    <td class="${r.total_pnl_excl_top_n_usd >= 0 ? 'green' : 'red'}">${r.total_pnl_excl_top_n_usd}</td>
    <td>${r.max_drawdown_usd}</td>
    <td>${r.avg_bars_held}</td>
  </tr>`;
const renderLiqSweepResults = makeSortableTable('liq-sweep-results-table', () => window.liqSweepResultsData, liqSweepRowHtml);
const renderLiqSweepWorst = makeSortableTable('liq-sweep-worst-table', () => window.liqSweepWorstData, liqSweepRowHtml);

document.getElementById('btn-wa-sweep').addEventListener('click', async () => {
  const btn = document.getElementById('btn-wa-sweep');
  const statusEl = document.getElementById('wa-sweep-status');
  const tables = {top: document.getElementById('wa-sweep-results-table'), worst: document.getElementById('wa-sweep-worst-table')};
  const titles = {top: document.getElementById('wa-sweep-top-title'), worst: document.getElementById('wa-sweep-worst-title')};
  const sweepSymbol = currentSymbol;
  const payload = {
    days: parseInt(document.getElementById('wa-sweep-days').value) || 30,
    exclude_top_n: parseInt(document.getElementById('wa-sweep-exclude-top-n').value) || 0,
    zone1_min: parseFloat(document.getElementById('wa-sweep-zone1-min').value),
    zone1_max: parseFloat(document.getElementById('wa-sweep-zone1-max').value),
    zone1_step: parseFloat(document.getElementById('wa-sweep-zone1-step').value),
    level_min: parseFloat(document.getElementById('wa-sweep-lvl-min').value),
    level_max: parseFloat(document.getElementById('wa-sweep-lvl-max').value),
    level_step: parseFloat(document.getElementById('wa-sweep-lvl-step').value),
    level_lines: ['signal', 'wave', 'both'].filter(k => document.getElementById('wa-sweep-line-' + k).checked),
    nachkauf_min: parseInt(document.getElementById('wa-sweep-nachkauf-min').value),
    nachkauf_max: parseInt(document.getElementById('wa-sweep-nachkauf-max').value),
    sl_min: parseFloat(document.getElementById('wa-sweep-sl-min').value),
    sl_max: parseFloat(document.getElementById('wa-sweep-sl-max').value),
    sl_step: parseFloat(document.getElementById('wa-sweep-sl-step').value),
    config: buildConfigPayload(),
  };
  btn.disabled = true;
  Object.values(tables).forEach(t => t.style.display = 'none');
  Object.values(titles).forEach(t => t.style.display = 'none');
  statusEl.innerText = `⏳ Lade Kerzen und teste alle Kombinationen...`;
  try {
    const res = await fetch(`/api/wa_sweep?symbol=${sweepSymbol}`, {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)
    });
    const data = await res.json();
    if (sweepSymbol !== currentSymbol) return;
    if (data.error) {
      statusEl.innerText = `❌ ${data.error}`;
    } else {
      statusEl.innerText = `[Modus: ${({zone:'Zone 1', level:'Level', cross:'Kreuzung zu Kreuzung (ohne Zone/Level)'})[data.entry_mode] || data.entry_mode}] ${data.combos_tested} Kombinationen getestet auf ${data.candles_processed} Kerzen (${data.actual_days_covered} Tage, ${data.resolution}) - Ergebnisse mit weniger als ${data.min_reliable_trades} Trades stehen unten in den Listen.`;
      window.waSweepResultsData = data.results || [];
      window.waSweepWorstData = data.worst_results || [];
      renderWaSweepResults();
      renderWaSweepWorst();
      Object.values(tables).forEach(t => t.style.display = '');
      Object.values(titles).forEach(t => t.style.display = '');
    }
  } catch (e) {
    if (sweepSymbol !== currentSymbol) return;
    statusEl.innerText = `❌ Fehler: ${e}`;
  }
  if (sweepSymbol === currentSymbol) btn.disabled = false;
});

window.waSweepResultsData = [];
window.waSweepWorstData = [];
const waSweepRowHtml = (r) => `
  <tr>
    <td>${r.wa_zone1}</td>
    <td>${({signal:'Signallinie', wave:'Welle', both:'Beide'})[r.wa_level_line] || '–'}</td>
    <td>${r.wa_max_nachkauf}</td>
    <td>${r.wa_sl_usd}</td>
    <td>${r.trades}</td>
    <td>${r.win_rate_pct}%</td>
    <td class="${r.total_pnl_usd >= 0 ? 'green' : 'red'}">${r.total_pnl_usd}</td>
    <td class="${r.total_pnl_excl_top_n_usd >= 0 ? 'green' : 'red'}">${r.total_pnl_excl_top_n_usd}</td>
    <td>${r.max_drawdown_usd}</td>
    <td>${r.avg_bars_held}</td>
  </tr>`;
const renderWaSweepResults = makeSortableTable('wa-sweep-results-table', () => window.waSweepResultsData, waSweepRowHtml);
const renderWaSweepWorst = makeSortableTable('wa-sweep-worst-table', () => window.waSweepWorstData, waSweepRowHtml);

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
  // Start-Zustand je Coin: laeuft (gruen) / gestoppt / "nach Deploy NICHT gestartet" (rot - gespeichert aktiv, aber laeuft in diesem Prozess nicht)
  const stateOf = o => !o.bot_active ? 'stopped' : (o.session_started ? 'running' : 'pending');
  const pendingCoins = Object.entries(overview).filter(([s, o]) => stateOf(o) === 'pending');
  const pendingWithPos = pendingCoins.filter(([s, o]) => o.position).map(([s]) => s);
  const startBanner = pendingCoins.length ? `<div class="start-banner">⚠️ <b>${pendingCoins.length} Bot${pendingCoins.length>1?'s sind':' ist'} nach dem Deploy NICHT gestartet:</b> ${pendingCoins.map(([s]) => s).join(', ')}.
      Sie sind als „aktiv“ gespeichert, laufen aber erst nach einem Klick auf <b>Start</b> (Coin wählen → Start).${pendingWithPos.length ? ` <b>Achtung: offene Position ohne laufenden Bot bei ${pendingWithPos.join(', ')}</b> – TP/SL/Strategie werden nicht überwacht!` : ''}</div>` : '';
  document.getElementById('coin-overview').innerHTML = startBanner + Object.entries(overview).map(([sym, o]) => {
    const stt = stateOf(o);
    const icon = stt === 'running' ? '🟢' : stt === 'pending' ? '⚠️' : '⏸️';
    const tip = stt === 'running' ? 'Bot läuft' : stt === 'pending' ? 'Nach Deploy NICHT gestartet – Start klicken' : 'Bot gestoppt';
    return `
    <div class="coin-pill ${sym===currentSymbol?'selected':''} ${stt==='pending'?'pending':''}" title="${tip}" onclick="document.getElementById('symbol-select').value='${sym}'; document.getElementById('symbol-select').dispatchEvent(new Event('change'));">
      ${icon} ${sym}: ${o.position || 'flach'} | PnL $${o.total_pnl_usd}
    </div>`;
  }).join('');

  document.getElementById('mode-badge').innerHTML =
    data.config.dry_run ? '<span class="badge dry">DRY RUN</span>' : '<span class="badge live">LIVE</span>';
  document.getElementById('active-badge').innerHTML =
    !data.config.bot_active ? '<span class="badge paused">GESTOPPT</span>'
      : (data.session_started ? '<span class="badge active">AKTIV</span>'
        : '<span class="badge pending" title="Als aktiv gespeichert, läuft aber erst nach Klick auf Start">⚠️ NICHT GESTARTET – Start klicken</span>');
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

    setResolutionField('rsi_resolution', data.config.rsi_resolution);
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
    setResolutionField('rsi_supertrend_filter_resolution', data.config.rsi_supertrend_filter_resolution);
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

    setResolutionField('mvwap_resolution', data.config.mvwap_resolution);
    document.getElementById('mvwap_direction_mode').value = data.config.mvwap_direction_mode;
    document.getElementById('mvwap_max_entries').value = data.config.mvwap_max_entries;
    document.getElementById('mvwap_nachkauf_min_abstand_usd').value = data.config.mvwap_nachkauf_min_abstand_usd;
    document.getElementById('mvwap_use_daily').value = String(data.config.mvwap_use_daily);
    document.getElementById('mvwap_w_daily').value = data.config.mvwap_w_daily;
    document.getElementById('mvwap_use_weekly').value = String(data.config.mvwap_use_weekly);
    document.getElementById('mvwap_w_weekly').value = data.config.mvwap_w_weekly;
    document.getElementById('mvwap_use_monthly').value = String(data.config.mvwap_use_monthly);
    document.getElementById('mvwap_w_monthly').value = data.config.mvwap_w_monthly;
    document.getElementById('mvwap_mf_source').value = data.config.mvwap_mf_source;
    document.getElementById('mvwap_mf_length').value = data.config.mvwap_mf_length;
    document.getElementById('mvwap_cmf_length').value = data.config.mvwap_cmf_length;
    document.getElementById('mvwap_mf_weight').value = data.config.mvwap_mf_weight;
    document.getElementById('mvwap_smooth_len').value = data.config.mvwap_smooth_len;
    document.getElementById('mvwap_use_zone_filter').value = String(data.config.mvwap_use_zone_filter);
    document.getElementById('mvwap_ob_level').value = data.config.mvwap_ob_level;
    document.getElementById('mvwap_os_level').value = data.config.mvwap_os_level;
    document.getElementById('mvwap_sl_enabled').value = String(data.config.mvwap_sl_enabled);
    document.getElementById('mvwap_sl_manual_usd').value = data.config.mvwap_sl_manual_usd;
    document.getElementById('mvwap_tp_enabled').value = String(data.config.mvwap_tp_enabled);
    document.getElementById('mvwap_tp_manual_usd').value = data.config.mvwap_tp_manual_usd;
    document.getElementById('mvwap_be_enabled').value = String(data.config.mvwap_be_enabled);
    document.getElementById('mvwap_be_trigger_usd').value = data.config.mvwap_be_trigger_usd;
    document.getElementById('mvwap_sl_cooldown_seconds').value = data.config.mvwap_sl_cooldown_seconds;
    document.getElementById('mvwap_supertrend_filter_enabled').value = String(data.config.mvwap_supertrend_filter_enabled);
    setResolutionField('mvwap_supertrend_filter_resolution', data.config.mvwap_supertrend_filter_resolution);
    document.getElementById('mvwap_supertrend_filter_multiplier').value = data.config.mvwap_supertrend_filter_multiplier;
    document.getElementById('mvwap_supertrend_filter_atr_period').value = data.config.mvwap_supertrend_filter_atr_period;
    document.getElementById('mvwap_adx_filter_enabled').value = String(data.config.mvwap_adx_filter_enabled);
    document.getElementById('mvwap_adx_filter_length').value = data.config.mvwap_adx_filter_length;
    document.getElementById('mvwap_adx_filter_threshold').value = data.config.mvwap_adx_filter_threshold;
    document.getElementById('mvwap_adx_filter_directional').value = String(data.config.mvwap_adx_filter_directional);
    document.getElementById('mvwap_adx_filter_mode').value = data.config.mvwap_adx_filter_mode || 'require_trend';
    document.getElementById('mvwap_macd_filter_enabled').value = String(data.config.mvwap_macd_filter_enabled);
    document.getElementById('mvwap_macd_filter_fast').value = data.config.mvwap_macd_filter_fast;
    document.getElementById('mvwap_macd_filter_slow').value = data.config.mvwap_macd_filter_slow;
    document.getElementById('mvwap_macd_filter_signal').value = data.config.mvwap_macd_filter_signal;
    document.getElementById('mvwap_rsi_filter_enabled').value = String(data.config.mvwap_rsi_filter_enabled);
    document.getElementById('mvwap_rsi_filter_length').value = data.config.mvwap_rsi_filter_length;
    document.getElementById('mvwap_rsi_filter_os_level').value = data.config.mvwap_rsi_filter_os_level;
    document.getElementById('mvwap_rsi_filter_ob_level').value = data.config.mvwap_rsi_filter_ob_level;
    document.getElementById('mvwap_cloud_filter_enabled').value = String(data.config.mvwap_cloud_filter_enabled);
    document.getElementById('mvwap_cloud_filter_length').value = data.config.mvwap_cloud_filter_length;
    document.getElementById('mvwap_cloud_filter_dev_mult').value = data.config.mvwap_cloud_filter_dev_mult;
    document.getElementById('mvwap_cloud_filter_touch_arm').value = String(data.config.mvwap_cloud_filter_touch_arm);
    setResolutionField('scalp_timeframe', data.config.scalp_timeframe);
    document.getElementById('scalp_vwap_length').value = data.config.scalp_vwap_length;
    document.getElementById('scalp_rsi_length').value = data.config.scalp_rsi_length;
    document.getElementById('scalp_rsi_upper').value = data.config.scalp_rsi_upper;
    document.getElementById('scalp_rsi_lower').value = data.config.scalp_rsi_lower;
    document.getElementById('scalp_docht_threshold').value = data.config.scalp_docht_threshold;
    document.getElementById('scalp_sl_mode').value = data.config.scalp_sl_mode;
    document.getElementById('scalp_sl_pct').value = data.config.scalp_sl_pct;
    document.getElementById('scalp_sl_usd').value = data.config.scalp_sl_usd;
    document.getElementById('scalp_max_nachkauf').value = data.config.scalp_max_nachkauf;
    document.getElementById('scalp_nachkauf_require_reversal').checked = !!data.config.scalp_nachkauf_require_reversal;
    document.getElementById('scalp_tp1_require_profit').checked = !!data.config.scalp_tp1_require_profit;
    document.getElementById('scalp_tp1_full_close').checked = !!data.config.scalp_tp1_full_close;
    document.getElementById('scalp_halfway_sl_enabled').checked = !!data.config.scalp_halfway_sl_enabled;
    document.getElementById('scalp_nachkauf_min_abstand_usd').value = data.config.scalp_nachkauf_min_abstand_usd;
    document.getElementById('scalp_nachkauf_min_candles').value = data.config.scalp_nachkauf_min_candles;
    document.getElementById('scalp_supertrend_filter_enabled').value = String(data.config.scalp_supertrend_filter_enabled);
    setResolutionField('scalp_supertrend_filter_resolution', data.config.scalp_supertrend_filter_resolution);
    document.getElementById('scalp_supertrend_filter_multiplier').value = data.config.scalp_supertrend_filter_multiplier;
    document.getElementById('scalp_supertrend_filter_atr_period').value = data.config.scalp_supertrend_filter_atr_period;
    setResolutionField('liq_timeframe', data.config.liq_timeframe);
    document.getElementById('liq_body_max_pct').value = data.config.liq_body_max_pct;
    document.getElementById('liq_max_levels').value = data.config.liq_max_levels;
    document.getElementById('liq_side_filter').value = String(data.config.liq_side_filter);
    document.getElementById('liq_dup_remove').value = String(data.config.liq_dup_remove);
    document.getElementById('liq_dup_tolerance_usd').value = data.config.liq_dup_tolerance_usd;
    document.getElementById('liq_entry_threshold_pct').value = data.config.liq_entry_threshold_pct;
    document.getElementById('liq_tp1_pct').value = data.config.liq_tp1_pct;
    document.getElementById('liq_tp2_pct').value = data.config.liq_tp2_pct;
    document.getElementById('liq_sl_usd').value = data.config.liq_sl_usd;
    document.getElementById('liq_tp1_require_profit').checked = !!data.config.liq_tp1_require_profit;
    document.getElementById('liq_max_nachkauf').value = data.config.liq_max_nachkauf;
    document.getElementById('liq_nachkauf_progress_pct').value = data.config.liq_nachkauf_progress_pct;
    setResolutionField('wa_timeframe', data.config.wa_timeframe);
    document.getElementById('wa_src').value = data.config.wa_src;
    document.getElementById('wa_n1').value = data.config.wa_n1;
    document.getElementById('wa_n2').value = data.config.wa_n2;
    document.getElementById('wa_sig_len').value = data.config.wa_sig_len;
    document.getElementById('wa_wave_scale').value = data.config.wa_wave_scale;
    document.getElementById('wa_zone1').value = data.config.wa_zone1;
    document.getElementById('wa_entry_mode').value = data.config.wa_entry_mode || 'zone';
    document.getElementById('wa_level').value = data.config.wa_level ?? 10;
    document.getElementById('wa_level_line').value = data.config.wa_level_line || 'signal';
    document.getElementById('wa_max_nachkauf').value = data.config.wa_max_nachkauf;
    document.getElementById('wa_nachkauf_min_pct').value = data.config.wa_nachkauf_min_pct ?? 0;
    document.getElementById('wa_reverse_only_profit').checked = !!data.config.wa_reverse_only_profit;
    document.getElementById('wa_tp_mode').value = data.config.wa_tp_mode;
    document.getElementById('wa_ueberlauf_level').value = data.config.wa_ueberlauf_level;
    document.getElementById('wa_tp_usd').value = data.config.wa_tp_usd;
    document.getElementById('wa_sl_usd').value = data.config.wa_sl_usd;
    document.getElementById('wa_trend_mode').value = data.config.wa_trend_mode || 'off';
    document.getElementById('wa_trend_tf').value = data.config.wa_trend_tf || '1h';
    document.getElementById('wa_trend_ilen').value = data.config.wa_trend_ilen ?? 4;
    document.getElementById('wa_trend_slen').value = data.config.wa_trend_slen ?? 50;
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

  // Scalp VWAP OBV RSI: Mittellinie (=TP1) + obere/untere TP2-Baender als Verlauf ueber die Zeit
  // einblenden (nicht als starre Linie wie bei Grid oben, weil sich die Baender mit jeder neu
  // geschlossenen Kerze verschieben), plus die tatsaechlichen Einstiege als Marker im Kursverlauf.
  if (data.config.entry_mode === 'scalp_vwap_obv_rsi') {
    const bandHist = (data.scalp_band_history || []).slice().sort((a, b) => a.ts - b.ts);
    const lookupBand = (ts, key) => {
      let val = null;
      for (const b of bandHist) {
        if (b.ts <= ts) val = b[key]; else break;
      }
      return val;
    };
    datasets.push({ label:'Mittellinie (TP1)', data: hist.map(p => lookupBand(p.ts, 'mean')), borderColor:'#facc15', borderDash:[5,3], pointRadius:0, borderWidth:1.5, spanGaps:true });
    datasets.push({ label:'TP2 oben', data: hist.map(p => lookupBand(p.ts, 'upper')), borderColor:'#4ade80', borderDash:[3,3], pointRadius:0, borderWidth:1, spanGaps:true });
    datasets.push({ label:'TP2 unten', data: hist.map(p => lookupBand(p.ts, 'lower')), borderColor:'#f87171', borderDash:[3,3], pointRadius:0, borderWidth:1, spanGaps:true });

    // Trigger-Preis (Binance-Live, siehe scalp_poll_loop) als eigene Linie - WEICHT vom Lighter-
    // Preis oben ab und ist der Preis, an dem SL/TP tatsaechlich ausgeloest werden. Ohne diese
    // Linie sieht ein Trigger im Chart faelschlich aus wie "Preis hat die Linie nicht beruehrt".
    const scalpPriceHist = (data.scalp_price_history || []).slice().sort((a, b) => a.ts - b.ts);
    const lookupPrice = (ts) => {
      let val = null;
      for (const p of scalpPriceHist) {
        if (p.ts <= ts) val = p.price; else break;
      }
      return val;
    };
    datasets.push({ label:'Preis (Binance-Live, Trigger)', data: hist.map(p => lookupPrice(p.ts)), borderColor:'#c084fc', pointRadius:0, borderWidth:1.5, spanGaps:true });

    const scalpEntries = data.current_position_entries || [];
    const entryArr = Array(n).fill(null);
    scalpEntries.forEach(e => {
      const eTs = new Date(e.time).getTime();
      let bestIdx = -1, bestDiff = Infinity;
      hist.forEach((p, i) => {
        const d = Math.abs(p.ts - eTs);
        if (d < bestDiff) { bestDiff = d; bestIdx = i; }
      });
      if (bestIdx >= 0) entryArr[bestIdx] = e.price;
    });
    datasets.push({ label:'Einstiege', data: entryArr, borderColor:'#60a5fa', backgroundColor:'#facc15', pointRadius:6, pointStyle:'triangle', showLine:false });
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

    rsi_resolution: getResolutionField('rsi_resolution'),
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
    rsi_supertrend_filter_resolution: getResolutionField('rsi_supertrend_filter_resolution'),
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

    mvwap_resolution: getResolutionField('mvwap_resolution'),
    mvwap_direction_mode: document.getElementById('mvwap_direction_mode').value,
    mvwap_max_entries: parseInt(document.getElementById('mvwap_max_entries').value),
    mvwap_nachkauf_min_abstand_usd: parseFloat(document.getElementById('mvwap_nachkauf_min_abstand_usd').value),
    mvwap_use_daily: document.getElementById('mvwap_use_daily').value === 'true',
    mvwap_w_daily: parseFloat(document.getElementById('mvwap_w_daily').value),
    mvwap_use_weekly: document.getElementById('mvwap_use_weekly').value === 'true',
    mvwap_w_weekly: parseFloat(document.getElementById('mvwap_w_weekly').value),
    mvwap_use_monthly: document.getElementById('mvwap_use_monthly').value === 'true',
    mvwap_w_monthly: parseFloat(document.getElementById('mvwap_w_monthly').value),
    mvwap_mf_source: document.getElementById('mvwap_mf_source').value,
    mvwap_mf_length: parseInt(document.getElementById('mvwap_mf_length').value),
    mvwap_cmf_length: parseInt(document.getElementById('mvwap_cmf_length').value),
    mvwap_mf_weight: parseFloat(document.getElementById('mvwap_mf_weight').value),
    mvwap_smooth_len: parseInt(document.getElementById('mvwap_smooth_len').value),
    mvwap_use_zone_filter: document.getElementById('mvwap_use_zone_filter').value === 'true',
    mvwap_ob_level: parseFloat(document.getElementById('mvwap_ob_level').value),
    mvwap_os_level: parseFloat(document.getElementById('mvwap_os_level').value),
    mvwap_sl_enabled: document.getElementById('mvwap_sl_enabled').value === 'true',
    mvwap_sl_manual_usd: parseFloat(document.getElementById('mvwap_sl_manual_usd').value),
    mvwap_tp_enabled: document.getElementById('mvwap_tp_enabled').value === 'true',
    mvwap_tp_manual_usd: parseFloat(document.getElementById('mvwap_tp_manual_usd').value),
    mvwap_be_enabled: document.getElementById('mvwap_be_enabled').value === 'true',
    mvwap_be_trigger_usd: parseFloat(document.getElementById('mvwap_be_trigger_usd').value),
    mvwap_sl_cooldown_seconds: parseFloat(document.getElementById('mvwap_sl_cooldown_seconds').value),
    mvwap_supertrend_filter_enabled: document.getElementById('mvwap_supertrend_filter_enabled').value === 'true',
    mvwap_supertrend_filter_resolution: getResolutionField('mvwap_supertrend_filter_resolution'),
    mvwap_supertrend_filter_multiplier: parseFloat(document.getElementById('mvwap_supertrend_filter_multiplier').value),
    mvwap_supertrend_filter_atr_period: parseInt(document.getElementById('mvwap_supertrend_filter_atr_period').value),
    mvwap_adx_filter_enabled: document.getElementById('mvwap_adx_filter_enabled').value === 'true',
    mvwap_adx_filter_length: parseInt(document.getElementById('mvwap_adx_filter_length').value),
    mvwap_adx_filter_threshold: parseFloat(document.getElementById('mvwap_adx_filter_threshold').value),
    mvwap_adx_filter_directional: document.getElementById('mvwap_adx_filter_directional').value === 'true',
    mvwap_adx_filter_mode: document.getElementById('mvwap_adx_filter_mode').value,
    mvwap_macd_filter_enabled: document.getElementById('mvwap_macd_filter_enabled').value === 'true',
    mvwap_macd_filter_fast: parseInt(document.getElementById('mvwap_macd_filter_fast').value),
    mvwap_macd_filter_slow: parseInt(document.getElementById('mvwap_macd_filter_slow').value),
    mvwap_macd_filter_signal: parseInt(document.getElementById('mvwap_macd_filter_signal').value),
    mvwap_rsi_filter_enabled: document.getElementById('mvwap_rsi_filter_enabled').value === 'true',
    mvwap_rsi_filter_length: parseInt(document.getElementById('mvwap_rsi_filter_length').value),
    mvwap_rsi_filter_os_level: parseFloat(document.getElementById('mvwap_rsi_filter_os_level').value),
    mvwap_rsi_filter_ob_level: parseFloat(document.getElementById('mvwap_rsi_filter_ob_level').value),
    mvwap_cloud_filter_enabled: document.getElementById('mvwap_cloud_filter_enabled').value === 'true',
    mvwap_cloud_filter_length: parseInt(document.getElementById('mvwap_cloud_filter_length').value),
    mvwap_cloud_filter_dev_mult: parseFloat(document.getElementById('mvwap_cloud_filter_dev_mult').value),
    mvwap_cloud_filter_touch_arm: document.getElementById('mvwap_cloud_filter_touch_arm').value === 'true',
    scalp_timeframe: getResolutionField('scalp_timeframe'),
    scalp_vwap_length: parseInt(document.getElementById('scalp_vwap_length').value),
    scalp_rsi_length: parseInt(document.getElementById('scalp_rsi_length').value),
    scalp_rsi_upper: parseFloat(document.getElementById('scalp_rsi_upper').value),
    scalp_rsi_lower: parseFloat(document.getElementById('scalp_rsi_lower').value),
    scalp_docht_threshold: parseFloat(document.getElementById('scalp_docht_threshold').value),
    scalp_sl_mode: document.getElementById('scalp_sl_mode').value,
    scalp_sl_pct: parseFloat(document.getElementById('scalp_sl_pct').value),
    scalp_sl_usd: parseFloat(document.getElementById('scalp_sl_usd').value),
    scalp_max_nachkauf: parseInt(document.getElementById('scalp_max_nachkauf').value),
    scalp_nachkauf_require_reversal: document.getElementById('scalp_nachkauf_require_reversal').checked,
    scalp_tp1_require_profit: document.getElementById('scalp_tp1_require_profit').checked,
    scalp_tp1_full_close: document.getElementById('scalp_tp1_full_close').checked,
    scalp_halfway_sl_enabled: document.getElementById('scalp_halfway_sl_enabled').checked,
    scalp_nachkauf_min_abstand_usd: parseFloat(document.getElementById('scalp_nachkauf_min_abstand_usd').value),
    scalp_nachkauf_min_candles: parseInt(document.getElementById('scalp_nachkauf_min_candles').value),
    scalp_supertrend_filter_enabled: document.getElementById('scalp_supertrend_filter_enabled').value === 'true',
    scalp_supertrend_filter_resolution: getResolutionField('scalp_supertrend_filter_resolution'),
    scalp_supertrend_filter_multiplier: parseFloat(document.getElementById('scalp_supertrend_filter_multiplier').value),
    scalp_supertrend_filter_atr_period: parseInt(document.getElementById('scalp_supertrend_filter_atr_period').value),
    liq_timeframe: getResolutionField('liq_timeframe'),
    liq_body_max_pct: parseFloat(document.getElementById('liq_body_max_pct').value),
    liq_max_levels: parseInt(document.getElementById('liq_max_levels').value),
    liq_side_filter: document.getElementById('liq_side_filter').value === 'true',
    liq_dup_remove: document.getElementById('liq_dup_remove').value === 'true',
    liq_dup_tolerance_usd: parseFloat(document.getElementById('liq_dup_tolerance_usd').value),
    liq_entry_threshold_pct: parseFloat(document.getElementById('liq_entry_threshold_pct').value),
    liq_tp1_pct: parseFloat(document.getElementById('liq_tp1_pct').value),
    liq_tp2_pct: parseFloat(document.getElementById('liq_tp2_pct').value),
    liq_sl_usd: parseFloat(document.getElementById('liq_sl_usd').value),
    liq_tp1_require_profit: document.getElementById('liq_tp1_require_profit').checked,
    liq_max_nachkauf: parseInt(document.getElementById('liq_max_nachkauf').value),
    liq_nachkauf_progress_pct: parseFloat(document.getElementById('liq_nachkauf_progress_pct').value),
    wa_timeframe: getResolutionField('wa_timeframe'),
    wa_src: document.getElementById('wa_src').value,
    wa_n1: parseInt(document.getElementById('wa_n1').value),
    wa_n2: parseInt(document.getElementById('wa_n2').value),
    wa_sig_len: parseInt(document.getElementById('wa_sig_len').value),
    wa_wave_scale: parseFloat(document.getElementById('wa_wave_scale').value),
    wa_zone1: parseFloat(document.getElementById('wa_zone1').value),
    wa_entry_mode: document.getElementById('wa_entry_mode').value,
    wa_level: parseFloat(document.getElementById('wa_level').value),
    wa_level_line: document.getElementById('wa_level_line').value,
    wa_max_nachkauf: parseInt(document.getElementById('wa_max_nachkauf').value),
    wa_nachkauf_min_pct: parseFloat(document.getElementById('wa_nachkauf_min_pct').value) || 0,
    wa_reverse_only_profit: document.getElementById('wa_reverse_only_profit').checked,
    wa_tp_mode: document.getElementById('wa_tp_mode').value,
    wa_ueberlauf_level: parseFloat(document.getElementById('wa_ueberlauf_level').value),
    wa_tp_usd: parseFloat(document.getElementById('wa_tp_usd').value),
    wa_sl_usd: parseFloat(document.getElementById('wa_sl_usd').value),
    wa_trend_mode: document.getElementById('wa_trend_mode').value,
    wa_trend_tf: document.getElementById('wa_trend_tf').value,
    wa_trend_ilen: parseInt(document.getElementById('wa_trend_ilen').value) || 4,
    wa_trend_slen: parseInt(document.getElementById('wa_trend_slen').value) || 50,
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
        result[s] = {"position": st["position"], "total_pnl_usd": round(st["stats"]["total_pnl_usd"], 3),
                     # Start-Zustand: bot_active = gespeicherter Wunsch, session_started = laeuft in DIESEM Prozess wirklich
                     # (faellt nach jedem Deploy/Neustart auf False, siehe handle_control)
                     "bot_active": bool(BOTS[s]["config"].get("bot_active")), "session_started": bool(st.get("session_started"))}
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
        "session_started": bool(st.get("session_started")),
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
        "mvwap_sl_price": st.get("mvwap_sl_price"), "mvwap_tp_price": st.get("mvwap_tp_price"), "mvwap_be_done": st.get("mvwap_be_done"), "mvwap_osc_last": st.get("mvwap_osc_last"),
        "scalp_sl_price": st.get("scalp_sl_price"), "scalp_tp1_done": st.get("scalp_tp1_done"),
        "scalp_mean_price": st.get("scalp_mean_price"), "scalp_upper_band": st.get("scalp_upper_band"),
        "scalp_lower_band": st.get("scalp_lower_band"), "scalp_obv_rsi": st.get("scalp_obv_rsi"),
        "scalp_band_history": st.get("scalp_band_history", [])[-500:],
        "scalp_price_history": st.get("scalp_price_history", [])[-500:],
        "liq_sl_price": st.get("liq_sl_price"), "liq_tp1_done": st.get("liq_tp1_done"),
        "liq_buyers_pct": st.get("liq_buyers_pct"), "liq_sellers_pct": st.get("liq_sellers_pct"),
        "liq_peak_pct": st.get("liq_peak_pct"), "liq_nachkauf_count": st.get("liq_nachkauf_count"),
        "wa_sl_price": st.get("wa_sl_price"), "wa_tp_price": st.get("wa_tp_price"), "wa_wt2_last": st.get("wa_wt2_last"),
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
                "rsi_macd_filter_enabled", "rsi_macd_filter_fast", "rsi_macd_filter_slow", "rsi_macd_filter_signal",
                "mvwap_resolution", "mvwap_direction_mode", "mvwap_max_entries", "mvwap_nachkauf_min_abstand_usd", "mvwap_use_daily", "mvwap_w_daily", "mvwap_use_weekly", "mvwap_w_weekly",
                "mvwap_use_monthly", "mvwap_w_monthly", "mvwap_mf_source", "mvwap_mf_length", "mvwap_cmf_length", "mvwap_mf_weight",
                "mvwap_smooth_len", "mvwap_use_zone_filter", "mvwap_ob_level", "mvwap_os_level",
                "mvwap_sl_enabled", "mvwap_sl_manual_usd", "mvwap_tp_enabled", "mvwap_tp_manual_usd",
                "mvwap_be_enabled", "mvwap_be_trigger_usd", "mvwap_sl_cooldown_seconds",
                "mvwap_supertrend_filter_enabled", "mvwap_supertrend_filter_resolution", "mvwap_supertrend_filter_multiplier", "mvwap_supertrend_filter_atr_period",
                "mvwap_adx_filter_enabled", "mvwap_adx_filter_length", "mvwap_adx_filter_threshold", "mvwap_adx_filter_directional", "mvwap_adx_filter_mode",
                "mvwap_macd_filter_enabled", "mvwap_macd_filter_fast", "mvwap_macd_filter_slow", "mvwap_macd_filter_signal",
                "mvwap_rsi_filter_enabled", "mvwap_rsi_filter_length", "mvwap_rsi_filter_os_level", "mvwap_rsi_filter_ob_level",
                "mvwap_cloud_filter_enabled", "mvwap_cloud_filter_length", "mvwap_cloud_filter_dev_mult", "mvwap_cloud_filter_touch_arm",
                "scalp_timeframe", "scalp_vwap_length", "scalp_rsi_length", "scalp_rsi_upper", "scalp_rsi_lower",
                "scalp_docht_threshold", "scalp_sl_mode", "scalp_sl_pct", "scalp_sl_usd", "scalp_max_nachkauf", "scalp_nachkauf_min_abstand_usd", "scalp_nachkauf_min_candles", "scalp_nachkauf_require_reversal", "scalp_tp1_require_profit", "scalp_tp1_full_close", "scalp_halfway_sl_enabled",
                "scalp_supertrend_filter_enabled", "scalp_supertrend_filter_resolution", "scalp_supertrend_filter_multiplier", "scalp_supertrend_filter_atr_period",
                "liq_timeframe", "liq_body_max_pct", "liq_max_levels", "liq_side_filter", "liq_dup_remove", "liq_dup_tolerance_usd",
                "liq_entry_threshold_pct", "liq_tp1_pct", "liq_tp2_pct", "liq_sl_usd", "liq_tp1_require_profit",
                "liq_max_nachkauf", "liq_nachkauf_progress_pct",
                "wa_timeframe", "wa_src", "wa_n1", "wa_n2", "wa_sig_len", "wa_wave_scale", "wa_zone1", "wa_entry_mode", "wa_level", "wa_level_line", "wa_max_nachkauf", "wa_nachkauf_min_pct", "wa_reverse_only_profit",
                "wa_tp_mode", "wa_ueberlauf_level", "wa_tp_usd", "wa_sl_usd",
                "wa_trend_mode", "wa_trend_tf", "wa_trend_ilen", "wa_trend_slen"]:
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
        # session_started: rein prozessinterner Flag (NICHT gespeichert, faellt bei jedem
        # Neustart/Deploy automatisch auf False zurueck) - erst ein manueller Klick auf Start HIER
        # setzt ihn auf True und gibt damit die REST-Kerzen-Abfrage in den Poll-Loops frei (siehe
        # dortige "session_started"-Checks). So loesen nach einem Deploy nicht mehr ALLE vorher
        # aktiven Coins gleichzeitig ihre erste Abfrage aus (das war der IP-Bann-Burst) - jeder
        # Coin bleibt stumm, bis er hier im Panel explizit neu gestartet wird, unabhaengig vom
        # gespeicherten bot_active-Wert.
        BOTS[symbol]["state"]["session_started"] = cfg["bot_active"]
        debug_log(f"{'▶️' if cfg['bot_active'] else '⏸️'} [{symbol}] Bot {'gestartet' if cfg['bot_active'] else 'gestoppt'}")
        await save_bot_configs()  # sonst geht bot_active bei Neustart/Redeploy verloren
    return web.json_response({"success": True, "bot_active": cfg["bot_active"]})


BACKTEST_TIMEOUT_SECONDS = 90  # siehe Kommentar in handle_backtest - verhindert unbegrenzt
# haengende Backtest-Tasks, die den globalen Binance-Throttle fuer alle Live-Coins blockieren


async def handle_wa_trend(request):
    """Trend (Marktstruktur BOS/CHoCH) aller Zeitebenen fuers Dashboard - 60 s gecacht."""
    from strategies import wa_trend_table, _wa_trend_cfg
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    cfg = BOTS[symbol]["config"]
    ent = _wa_trend_ui_cache.get(symbol)
    if ent and time.time() - ent[0] < 60 and ent[2] == (cfg.get("wa_trend_ilen"), cfg.get("wa_trend_slen")):
        rows = ent[1]
    else:
        rows = await wa_trend_table(symbol, cfg)
        _wa_trend_ui_cache[symbol] = (time.time(), rows, (cfg.get("wa_trend_ilen"), cfg.get("wa_trend_slen")))
    mode, tf, _i, _s = _wa_trend_cfg(cfg)
    return web.json_response({"rows": rows, "mode": mode, "tf": tf, "active": BOTS[symbol]["state"].get("wa_trend", 0)})


_wa_trend_ui_cache = {}


async def handle_wa_live_chart(request):
    """Live-Chart fuer Wellenanker im Dashboard: Kerzen (inkl. laufender Kerze), Wellenlinien, Signale, Position."""
    from strategies import fetch_candles_binance_multi, wa_chart_payload
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    cfg = BOTS[symbol]["config"]
    st = BOTS[symbol]["state"]
    n1, n2, sl = int(cfg.get("wa_n1", 10)), int(cfg.get("wa_n2", 21)), int(cfg.get("wa_sig_len", 4))
    bars = max(300, (n1 + n2 + sl + 20) * 3)
    data = await fetch_candles_binance_multi(symbol, cfg.get("wa_timeframe", "15m"), count_back=min(1000, bars), market_type=cfg.get("binance_market_type", "spot"))
    if not data:
        return web.json_response({"ok": False, "reason": "lädt noch … (keine Kerzen erhalten)"})
    ts, o, h, l, c = data
    if len(c) < n1 + n2 + sl + 21:
        return web.json_response({"ok": False, "reason": "lädt noch … (zu wenig Kerzen)"})
    payload = await asyncio.to_thread(wa_chart_payload, ts, o, h, l, c, cfg, 1000)
    pos = None
    if st.get("position") and st.get("avg_entry_price"):
        size = st.get("total_coin_size") or 0
        avg = st["avg_entry_price"]
        sl_usd = float(cfg.get("wa_sl_usd", 5.0))
        d = (sl_usd / size) if size else 0
        pos = {"dir": st["position"], "avg": avg, "size": size, "pnl": ((st.get("last_price") or avg) - avg) * size * (1 if st["position"] == "long" else -1),
               "sl": (avg - d) if st["position"] == "long" else (avg + d)}
    payload.update({"ok": True, "price": st.get("last_price"), "position": pos, "resolution": cfg.get("wa_timeframe", "15m"),
                    "active": bool(cfg.get("bot_active")) and cfg.get("entry_mode") == "wellenanker"})
    return web.json_response(payload)


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
    # BACKTEST_TIMEOUT_SECONDS: ohne dieses Limit kann ein Backtest (v.a. bei Sekunden-
    # Aufloesungen wie 10s/15s/30s/45s ueber mehrere Tage/Wochen) im schlimmsten Fall
    # unbegrenzt lange im Hintergrund weiterlaufen - selbst wenn der Browser-Tab laengst
    # geschlossen wurde, der aiohttp-Request also niemand mehr zuhoert. Beobachtet: ein
    # solcher haengender Task blockierte ueber eine Stunde denselben globalen Binance-
    # Throttle/Lock, den auch die LIVE-Strategien aller anderen Coins benutzen - die
    # bekamen in der Zeit "zu wenig Kerzen" bzw. "keine Kerzen erhalten". asyncio.wait_for
    # bricht die Coroutine nach Ablauf hart ab (CancelledError propagiert nach innen),
    # damit sowas nie wieder unbegrenzt weiterlaufen und Ressourcen binden kann.
    try:
        result = await asyncio.wait_for(
            run_backtest(symbol, entry_mode, cfg, days, exclude_top_n),
            timeout=BACKTEST_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        debug_log(f"⏱️ [{symbol}] Backtest ({entry_mode}) nach {BACKTEST_TIMEOUT_SECONDS}s abgebrochen (Timeout)",
                  {"days": days})
        return web.json_response({
            "error": f"Backtest nach {BACKTEST_TIMEOUT_SECONDS}s abgebrochen (Timeout) - "
                     f"wahrscheinlich zu viele Kerzen fuer den gewaehlten Zeitraum/Aufloesung. "
                     f"Bei Sekunden-Aufloesungen (10s/15s/30s/45s) einen kuerzeren Zeitraum wählen."
        }, status=504)
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


async def handle_liq_sweep(request):
    """'Monte-Carlo'-Sweep fuer Liquidity Waves: Einstiegs-Schwelle x Levels keep alive,
    siehe run_liq_sweep."""
    from strategies import run_liq_sweep
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    body = await request.json()
    try:
        days = max(1, min(365, int(body.get("days", 30))))
        entry_min = max(0.1, float(body.get("entry_min", 1.0)))
        entry_max = max(entry_min, float(body.get("entry_max", 10.0)))
        entry_step = max(0.1, float(body.get("entry_step", 1.0)))
        levels_min = max(1, int(body.get("levels_min", 30)))
        levels_max = max(levels_min, int(body.get("levels_max", 100)))
        levels_step = max(1, int(body.get("levels_step", 5)))
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

    try:
        result = await run_liq_sweep(symbol, cfg, days, entry_min, entry_max, entry_step,
                                      levels_min, levels_max, levels_step, exclude_top_n)
    except Exception as e:
        debug_log(f"⚠️ [{symbol}] Liquidity-Waves-Sweep fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        return web.json_response({"error": f"Sweep fehlgeschlagen: {e}"}, status=500)
    return web.json_response(result)


async def handle_wa_sweep(request):
    """'Monte-Carlo'-Sweep fuer Wellenanker: Zone 1 x Max. Nachkäufe (0-20) x Stop-Loss ($),
    siehe run_wa_sweep."""
    from strategies import run_wa_sweep
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    body = await request.json()
    try:
        days = max(1, min(365, int(body.get("days", 30))))
        zone1_min = max(1.0, float(body.get("zone1_min", 30.0)))
        zone1_max = max(zone1_min, float(body.get("zone1_max", 80.0)))
        zone1_step = max(0.1, float(body.get("zone1_step", 1.0)))
        level_min = max(0.0, min(30.0, float(body.get("level_min", 0.0))))
        level_max = max(level_min, min(30.0, float(body.get("level_max", 30.0))))
        level_step = max(0.1, float(body.get("level_step", 2.0)))
        nachkauf_min = max(0, min(20, int(body.get("nachkauf_min", 0))))
        nachkauf_max = max(nachkauf_min, min(20, int(body.get("nachkauf_max", 4))))
        sl_min = max(0.1, float(body.get("sl_min", 5.0)))
        sl_max = max(sl_min, float(body.get("sl_max", 25.0)))
        sl_step = max(0.1, float(body.get("sl_step", 2.0)))
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

    try:
        if cfg.get("wa_entry_mode") == "level":
            zone1_min, zone1_max, zone1_step = level_min, level_max, level_step   # im Level-Modus wird der Abstand 0-30 getestet
        lines = body.get("level_lines")
        result = await run_wa_sweep(symbol, cfg, days, zone1_min, zone1_max, zone1_step,
                                     nachkauf_min, nachkauf_max, sl_min, sl_max, sl_step, exclude_top_n,
                                     lines if isinstance(lines, list) else None)
    except Exception as e:
        debug_log(f"⚠️ [{symbol}] Wellenanker-Sweep fehlgeschlagen", {"error": str(e), "traceback": traceback.format_exc()})
        return web.json_response({"error": f"Sweep fehlgeschlagen: {e}"}, status=500)
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


async def handle_reverse_position(request):
    """Manueller Reverse: laufende Position mit EINER Order in die Gegenrichtung drehen (gleiche Groesse)."""
    symbol = request.query.get("symbol", SYMBOLS[0]).upper()
    if symbol not in BOTS:
        return web.json_response({"error": "unknown symbol"}, status=404)
    st = BOTS[symbol]["state"]
    if st["position"] is None:
        return web.json_response({"error": "keine offene Position zum Umkehren"}, status=400)
    if st["last_price"] is None:
        return web.json_response({"error": "kein aktueller Preis bekannt"}, status=400)
    ok = await execute_reverse(symbol, st["last_price"], "MANUAL-REVERSE")
    if not ok:
        return web.json_response({"error": "Reverse fehlgeschlagen - siehe Log"}, status=500)
    return web.json_response({"success": True, "position": st["position"]})


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


