import aiohttp
import asyncio
import websockets
import json
import time
import os
from collections import deque

# ========== KONFIGURATION ==========
SYMBOL = os.getenv("SYMBOL", "SOL")
MARGIN = float(os.getenv("MARGIN", "10"))
LEVERAGE = int(os.getenv("LEVERAGE", "20"))
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
BASE_URL = "https://mainnet.zklighter.elliot.ai"
WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
MARKET_INDEX = 2  # SOL

# ========== EMA ==========
class EMACalculator:
    def __init__(self, period):
        self.period = period
        self.closes = []
        self.ema = None
        
    def add_candle(self, close_price):
        self.closes.append(close_price)
        if len(self.closes) > self.period * 2:
            self.closes = self.closes[-self.period * 2:]
        
        if len(self.closes) == self.period:
            self.ema = sum(self.closes) / self.period
        elif len(self.closes) > self.period:
            multiplier = 2 / (self.period + 1)
            self.ema = (close_price - self.ema) * multiplier + self.ema

# ========== HISTORISCHE CANDLES ==========
async def get_historical_candles(market_id, limit=50):
    """Holt historische Candles von der API"""
    url = f"{BASE_URL}/api/v1/candles"
    params = {"market_id": market_id, "resolution": "1m", "limit": limit}
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("candles", [])
                else:
                    print(f"❌ API Fehler: {response.status}")
                    return []
    except Exception as e:
        print(f"❌ Fehler beim Abrufen: {e}")
        return []

# ========== MAIN ==========
async def main():
    print(f"🚀 EMA-Crossover-Bot für {SYMBOL}")
    print(f"   EMA: 7/21 auf 1-Minuten Candles")
    print(f"   DRY_RUN: {DRY_RUN}")
    
    # EMAs initialisieren
    fast_ema = EMACalculator(7)
    slow_ema = EMACalculator(21)
    
    # Historische Candles laden
    print("📊 Lade historische Candles...")
    candles = await get_historical_candles(MARKET_INDEX, 50)
    
    if candles:
        for candle in candles:
            close = candle.get("c", 0)
            if close > 0:
                fast_ema.add_candle(close)
                slow_ema.add_candle(close)
        print(f"✅ EMAs initialisiert mit {len(candles)} Candles")
        print(f"   EMA7: {fast_ema.ema:.3f}" if fast_ema.ema else "   EMA7: None")
        print(f"   EMA21: {slow_ema.ema:.3f}" if slow_ema.ema else "   EMA21: None")
    else:
        print("⚠️ Keine historischen Candles verfügbar - muss 21 Minuten warten!")
    
    # WebSocket Verbindung
    async with websockets.connect(WS_URL) as ws:
        await ws.send(json.dumps({"type": "subscribe", "channel": f"trade/{MARKET_INDEX}"}))
        print("✅ Verbunden")
        
        current_candle = None
        position = None
        
        async for raw in ws:
            msg = json.loads(raw)
            trades = msg.get("trades", [])
            if not trades:
                continue
            
            for trade in trades:
                price = float(trade["price"])
                size = float(trade["size"])
                minute = int(time.time() / 60)
                
                # Neue Candle
                if current_candle is None or current_candle["minute"] != minute:
                    if current_candle is not None:
                        # Candle schließen - EMA updaten
                        close = current_candle["close"]
                        fast_ema.add_candle(close)
                        slow_ema.add_candle(close)
                        
                        # Crossover prüfen
                        if fast_ema.ema and slow_ema.ema:
                            if fast_ema.ema > slow_ema.ema and position != "long":
                                print(f"📈 CROSSOVER UP @ {close}")
                                if not DRY_RUN:
                                    print(f"✅ LONG @ {close}")
                                position = "long"
                            elif fast_ema.ema < slow_ema.ema and position != "short":
                                print(f"📉 CROSSOVER DOWN @ {close}")
                                if not DRY_RUN:
                                    print(f"✅ SHORT @ {close}")
                                position = "short"
                    
                    # Neue Candle starten
                    current_candle = {
                        "minute": minute,
                        "open": price,
                        "high": price,
                        "low": price,
                        "close": price,
                        "volume": 0
                    }
                else:
                    # Candle updaten
                    current_candle["high"] = max(current_candle["high"], price)
                    current_candle["low"] = min(current_candle["low"], price)
                    current_candle["close"] = price
                    current_candle["volume"] += size

if __name__ == "__main__":
    asyncio.run(main())
