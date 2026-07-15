"""
EINFACHER EMA-CROSSOVER-BOT MIT 1-MINUTEN CANDLES
- Sammelt Trades zu 1-Minuten Candles
- EMA7 + EMA21 basierend auf Candle-Closes
- Kauft/Verkauft bei Crossover
"""

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

# EMA Parameter (wie TradingView)
EMA_FAST = 7
EMA_SLOW = 21

# ========== BASE ==========
BASE_URL = "https://mainnet.zklighter.elliot.ai"
WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
MARKET_INDEX = 2  # SOL

# ========== 1-MINUTE CANDLE ==========
class MinuteCandle:
    def __init__(self):
        self.open = None
        self.high = None
        self.low = None
        self.close = None
        self.volume = 0
        self.timestamp = 0
        
    def add_trade(self, price, size):
        if self.open is None:
            self.open = price
            self.high = price
            self.low = price
            self.close = price
            self.timestamp = int(time.time() / 60)
        else:
            self.high = max(self.high, price)
            self.low = min(self.low, price)
            self.close = price
        self.volume += size
    
    def is_new_minute(self):
        return int(time.time() / 60) != self.timestamp

# ========== EMA CALCULATOR (basierend auf Candles) ==========
class EMACalculator:
    def __init__(self, period):
        self.period = period
        self.closes = []  # Nur Candle-Closes!
        self.ema = None
        
    def add_candle(self, close_price):
        """Nur 1 Wert pro Minute!"""
        self.closes.append(close_price)
        
        # Nur die letzten Perioden behalten
        if len(self.closes) > self.period * 2:
            self.closes = self.closes[-self.period * 2:]
        
        if len(self.closes) == self.period:
            # Erster EMA = SMA
            self.ema = sum(self.closes) / self.period
        elif len(self.closes) > self.period:
            # EMA Formel (wie TradingView)
            multiplier = 2 / (self.period + 1)
            self.ema = (close_price - self.ema) * multiplier + self.ema

# ========== LIGHTER CLIENT ==========
def get_lighter_client():
    try:
        import lighter
        return lighter.SignerClient(
            url=BASE_URL,
            api_private_keys={int(os.getenv("API_KEY_INDEX", "5")): os.getenv("PRIVATE_KEY")},
            account_index=int(os.getenv("ACCOUNT_INDEX", "50960"))
        )
    except Exception as e:
        print(f"❌ Client Error: {e}")
        return None

async def execute_order(action, price):
    if DRY_RUN:
        print(f"🧪 {action} @ {price}")
        return
    
    client = get_lighter_client()
    if not client:
        return
    
    print(f"✅ {action} @ {price}")

# ========== MAIN ==========
async def main():
    print(f"🚀 EMA-Crossover-Bot für {SYMBOL}")
    print(f"   EMA: {EMA_FAST}/{EMA_SLOW} auf 1-Minuten Candles")
    print(f"   DRY_RUN: {DRY_RUN}")
    
    # State
    current_candle = MinuteCandle()
    fast_ema = EMACalculator(EMA_FAST)
    slow_ema = EMACalculator(EMA_SLOW)
    position = None
    
    async with websockets.connect(WS_URL) as ws:
        await ws.send(json.dumps({"type": "subscribe", "channel": f"trade/{MARKET_INDEX}"}))
        print("✅ Verbunden")
        
        async for raw in ws:
            msg = json.loads(raw)
            trades = msg.get("trades", [])
            if not trades:
                continue
            
            for trade in trades:
                price = float(trade["price"])
                size = float(trade["size"])
                
                # ===== 1-MINUTE CANDLE =====
                if current_candle.is_new_minute() and current_candle.close is not None:
                    # Candle schließen - EMA mit Close updaten
                    close = current_candle.close
                    fast_ema.add_candle(close)
                    slow_ema.add_candle(close)
                    
                    print(f"🕐 Neue Candle: Close={close:.3f}")
                    
                    # Crossover prüfen (wenn EMAs bereit)
                    if fast_ema.ema and slow_ema.ema:
                        if fast_ema.ema > slow_ema.ema and position != "long":
                            print(f"📈 CROSSOVER UP: EMA7 ({fast_ema.ema:.3f}) > EMA21 ({slow_ema.ema:.3f})")
                            await execute_order("long", close)
                            position = "long"
                            
                        elif fast_ema.ema < slow_ema.ema and position != "short":
                            print(f"📉 CROSSOVER DOWN: EMA7 ({fast_ema.ema:.3f}) < EMA21 ({slow_ema.ema:.3f})")
                            await execute_order("short", close)
                            position = "short"
                    
                    # Neue Candle starten
                    current_candle = MinuteCandle()
                
                # Trade zur aktuellen Candle hinzufügen
                current_candle.add_trade(price, size)

if __name__ == "__main__":
    asyncio.run(main())
