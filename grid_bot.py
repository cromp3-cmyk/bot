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

