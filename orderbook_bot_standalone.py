# ========== AM ANFANG DEINER DATEI EINFÜGEN ==========

class EMATrendFilter:
    """
    Einfacher EMA-basierter Trend-Filter
    """
    def __init__(self, fast_period=9, slow_period=21):
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.prices = deque(maxlen=50)  # Speichert letzte 50 Preise
        self.fast_ema = None
        self.slow_ema = None
        self.last_update_time = 0
        
    def add_price(self, price):
        """Fügt neuen Preis hinzu und berechnet EMAs"""
        self.prices.append(price)
        
        if len(self.prices) >= self.fast_period:
            self.fast_ema = self.calculate_ema(self.fast_period)
        
        if len(self.prices) >= self.slow_period:
            self.slow_ema = self.calculate_ema(self.slow_period)
    
    def calculate_ema(self, period):
        """Berechnet EMA für angegebenen Zeitraum"""
        if len(self.prices) < period:
            return None
        
        # Nur die letzten 'period' Preise verwenden
        prices_list = list(self.prices)[-period:]
        
        # EMA-Formel: EMA = (Preis - vorheriger_EMA) * Multiplikator + vorheriger_EMA
        multiplier = 2 / (period + 1)
        ema = prices_list[0]  # Startwert = erster Preis
        
        for price in prices_list[1:]:
            ema = (price - ema) * multiplier + ema
        
        return ema
    
    def get_trend(self):
        """
        Gibt Trend zurück:
        - "up": Aufwärtstrend
        - "down": Abwärtstrend
        - "neutral": Seitwärts
        """
        if self.fast_ema is None or self.slow_ema is None:
            return "neutral"
        
        # Berechne prozentuale Differenz
        diff_percent = ((self.fast_ema - self.slow_ema) / self.slow_ema) * 100
        
        # 0.2% Puffer gegen Rauschen
        if diff_percent > 0.2:
            return "up"
        elif diff_percent < -0.2:
            return "down"
        else:
            return "neutral"
    
    def get_trend_strength(self):
        """
        Gibt Trendstärke zurück (0-1):
        - 1 = Sehr starker Trend
        - 0 = Kein Trend
        """
        if self.fast_ema is None or self.slow_ema is None:
            return 0
        
        diff = abs(self.fast_ema - self.slow_ema) / self.slow_ema
        return min(diff * 10, 1)  # Skaliert auf 0-1
    
    def get_status(self):
        """Gibt kompletten Status zurück (für Debug)"""
        return {
            "fast_ema": round(self.fast_ema, 3) if self.fast_ema else None,
            "slow_ema": round(self.slow_ema, 3) if self.slow_ema else None,
            "trend": self.get_trend(),
            "strength": round(self.get_trend_strength(), 2),
            "price_count": len(self.prices)
        }

# ========== IN DEINER listen() FUNKTION ==========

# Erstelle den Trend-Filter (global)
trend_filter = EMATrendFilter(fast_period=9, slow_period=21)

# In der listen() Funktion, bei jedem Trade-Update:
async def listen():
    global last_trade_price, lean_direction, lean_since
    
    # ... dein WebSocket-Code ...
    
    async for raw in ws:
        msg = json.loads(raw)
        channel = msg.get("channel", "")
        
        if channel.startswith("order_book"):
            apply_order_book_update(msg)
            obi = calc_obi()
            obi_history.append(obi)
            
            # ===== NEU: TREND-CHECK =====
            # Nur wenn wir einen letzten Preis haben
            if last_trade_price is not None:
                # Update EMA mit aktuellem Preis
                trend_filter.add_price(last_trade_price)
                trend = trend_filter.get_trend()
                trend_strength = trend_filter.get_trend_strength()
                
                # ===== SIGNAL MIT TREND-BESTÄTIGUNG =====
                global lean_direction, lean_since
                now = time.time()
                
                # Nur handeln wenn:
                # 1. OBI über Schwelle
                # 2. Trend stimmt mit OBI überein
                # 3. Trend ist stark genug (strength > 0.3)
                if obi >= OBI_THRESHOLD and trend == "up" and trend_strength > 0.3:
                    current_lean = "buy"
                elif obi <= -OBI_THRESHOLD and trend == "down" and trend_strength > 0.3:
                    current_lean = "sell"
                else:
                    current_lean = None
                
                # ===== ZEIT-BASIERTE BESTÄTIGUNG =====
                if current_lean != lean_direction:
                    # Richtung geändert
                    if lean_direction is not None:
                        debug_log(f"🔄 Lean-Richtung geändert (mit Trend-Filter)", {
                            "von": lean_direction,
                            "zu": current_lean,
                            "obi": round(obi, 3),
                            "trend": trend,
                            "trend_strength": round(trend_strength, 2)
                        })
                    lean_direction = current_lean
                    lean_since = now if current_lean is not None else 0.0
                elif current_lean is not None and (now - lean_since) >= OBI_CONFIRM_SECONDS:
                    # Signal bestätigt
                    await execute_signal(current_lean, last_trade_price)
                
                # ===== STATUS-LOG MIT TREND =====
                if now - last_status_log >= STATUS_LOG_INTERVAL:
                    last_status_log = now
                    status = trend_filter.get_status()
                    debug_log(f"📊 Status {SYMBOL} mit Trend", {
                        "aktueller_OBI": round(obi, 3),
                        "schwelle": OBI_THRESHOLD,
                        "trend": status["trend"],
                        "trend_staerke": status["strength"],
                        "fast_ema": status["fast_ema"],
                        "slow_ema": status["slow_ema"],
                        "letzter_preis": last_trade_price,
                        "bot_position": current_position_side or "flach",
                        "lean_seit": round(now - lean_since, 1) if lean_direction else 0
                    })
        
        elif channel.startswith("trade"):
            trades = msg.get("trades", [])
            if trades:
                last_trade_price = float(trades[-1]["price"])
                # Preis sofort in Trend-Filter geben
                trend_filter.add_price(last_trade_price)
