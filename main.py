# main.py - 50 SYMBOLS OPTIMIZED VERSION - PINE STRATEGY (EMA50/200 + RSI55/45 + ADX + Volume)
"""
50 SYMBOLS OPTIMIZED VERSION - PINE STRATEGY
- PINE STRATEGY: EMA50/200 + RSI55/45 + ADX + Volume (REQUIRED)
- 50 symbols for maximum signals
- Optimized parameters for more opportunities
- Enhanced performance for multiple symbols
- INSTITUTE GRADE FEATURES:
  - Risk Management System
  - Multi-Timeframe Analysis
  - Advanced Order Types
  - Real-time Performance Monitoring
  - Configuration Management (JSON based)
  - Connection Pooling for API Calls
  - Data Caching for Performance
  - Dashboard Authentication
  - Fixed Trade History with Cumulative P&L
  - Manual Trade Closure with Real-time P&L Update
  - Per-Side Position Limits (Long/Short)
  - FRESH SIGNAL ONLY - No trades on old/historical signals
  - Optional Indicators: Volume, MACD, Support/Resistance (Toggle ON/OFF)
  - SECTOR-BASED POSITION LIMITS - Prevents correlated losses
  - FIXED TP TRIGGER - Proper partial close detection
  - SIDEWAYS MARKET DETECTION - Avoids choppy/range-bound markets
  - FIXED DUPLICATE TRADES - Prevents duplicate entries in dashboard
  - FIXED DIVISION BY ZERO - Safe sideways detection
  - PINE STRATEGY EXACT MATCH - EMA50/200 + RSI55/45 + ADX20 + Volume SMA20
  - ATR-BASED SL/TP - Matching Pine's ATR multiplier approach
  - FRESH CROSSOVER DETECTION - Only trades crossovers after bot start
"""

import os
import time
import threading
import json
import argparse
import tempfile
import math
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import logging
from logging.handlers import RotatingFileHandler
import traceback
import requests
from collections import deque
from typing import Dict, List, Tuple, Optional

# ============================================================================
# LOGGING SETUP - MUST BE EARLY
# ============================================================================

def setup_logging():
    """Setup comprehensive logging with rotation"""
    logger = logging.getLogger('trading_bot')
    logger.setLevel(logging.INFO)
    
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    file_handler = RotatingFileHandler(
        'trading_bot.log', 
        maxBytes=10*1024*1024,
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# Initialize logger IMMEDIATELY
logger = setup_logging()

# Load environment early
load_dotenv()

# Binance client
try:
    from binance.client import Client
    from binance.exceptions import BinanceAPIException
    BINANCE_AVAILABLE = True
except Exception as e:
    logger.warning(f"⚠️ Binance library not available: {e}")
    Client = None
    BINANCE_AVAILABLE = False

# FastAPI + dashboard
try:
    from fastapi import FastAPI, Form, Depends, HTTPException, status
    from fastapi.responses import HTMLResponse, JSONResponse
    from fastapi.security import HTTPBasic, HTTPBasicCredentials
    import uvicorn
    FASTAPI_AVAILABLE = True
except Exception as e:
    logger.warning(f"⚠️ FastAPI not available: {e}")
    FASTAPI_AVAILABLE = False

# ============================================================================
# TRADING PARAMETERS - DEFINED EARLY FOR PROPER ORDER
# ============================================================================

# Config - OPTIMIZED FOR 50 SYMBOLS
def _env_bool(name, default="False"):
    v = os.getenv(name, default)
    try:
        return str(v).lower() in ("1", "true", "yes")
    except Exception:
        return False

API_KEY    = os.getenv("BINANCE_API_KEY", "") or os.getenv("API_KEY", "")
API_SECRET = os.getenv("BINANCE_API_SECRET", "") or os.getenv("API_SECRET", "")
USE_TESTNET = _env_bool("USE_TESTNET", "True")
DRY_RUN = _env_bool("DRY_RUN", "True")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", 60))
TRADE_USDT = float(os.getenv("TRADE_USDT", 100.0))

PORT = int(os.getenv("PORT", 8000))

# Dashboard authentication
DASHBOARD_USER = os.getenv("DASHBOARD_USER", "admin")
DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "changeme")

# Per-side position limits
MAX_LONG_TRADES = int(os.getenv("MAX_LONG_TRADES", 2))
MAX_SHORT_TRADES = int(os.getenv("MAX_SHORT_TRADES", 2))

# Fresh signal settings
SIGNAL_FRESHNESS_MINUTES = int(os.getenv("SIGNAL_FRESHNESS_MINUTES", 5))

# Sideways market detection settings
SIDEWAYS_ADX_THRESHOLD = int(os.getenv("SIDEWAYS_ADX_THRESHOLD", 20))
SIDEWAYS_CI_THRESHOLD = float(os.getenv("SIDEWAYS_CI_THRESHOLD", 61.8))
SIDEWAYS_ENABLED = _env_bool("SIDEWAYS_ENABLED", "True")

# ============================================================================
# CONNECTION POOLING & RATE LIMITING
# ============================================================================

class BinanceSession:
    """Connection pooling and rate limiting for Binance API calls"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'TradingBot/1.0',
            'Accept': 'application/json'
        })
        self.last_request_time = 0
        self.request_count = 0
        self.min_interval = 0.1  # 100ms between requests
    
    def get(self, url, params=None, timeout=15):
        """Rate-limited GET request"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_interval:
            time.sleep(self.min_interval - time_since_last)
        
        self.last_request_time = time.time()
        self.request_count += 1
        
        return self.session.get(url, params=params, timeout=timeout)
    
    def get_stats(self):
        """Get session statistics"""
        return {
            'total_requests': self.request_count,
            'last_request': self.last_request_time
        }

# Global session
binance_session = BinanceSession()

# ============================================================================
# DATA CACHING
# ============================================================================

class DataCache:
    """Caching system for market data"""
    
    def __init__(self, ttl=30):
        self.cache = {}
        self.ttl = ttl
        self.hits = 0
        self.misses = 0
    
    def get(self, symbol, interval, limit=100):
        """Get cached data if available"""
        key = f"{symbol}_{interval}_{limit}"
        if key in self.cache:
            data, timestamp = self.cache[key]
            if time.time() - timestamp < self.ttl:
                self.hits += 1
                return data
        self.misses += 1
        return None
    
    def set(self, symbol, interval, limit, data):
        """Store data in cache"""
        key = f"{symbol}_{interval}_{limit}"
        self.cache[key] = (data, time.time())
    
    def clear(self):
        """Clear all cache"""
        self.cache.clear()
        self.hits = 0
        self.misses = 0
    
    def get_stats(self):
        """Get cache statistics"""
        total = self.hits + self.misses
        hit_rate = (self.hits / total * 100) if total > 0 else 0
        return {
            'hits': self.hits,
            'misses': self.misses,
            'hit_rate': f"{hit_rate:.1f}%",
            'cache_size': len(self.cache)
        }

# Initialize cache
data_cache = DataCache(ttl=30)

# Custom JSON encoder for NaN values
class SafeJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj):
                return None
        if pd.isna(obj):
            return None
        if isinstance(obj, np.floating) and (np.isnan(obj) or np.isinf(obj)):
            return None
        return super().default(obj)

# Safe JSON Response class for API endpoints
class SafeJSONResponse(JSONResponse):
    def render(self, content) -> bytes:
        cleaned_content = self.clean_nan(content)
        return json.dumps(
            cleaned_content,
            ensure_ascii=False,
            allow_nan=False,
            indent=None,
            separators=(",", ":"),
            cls=SafeJSONEncoder
        ).encode("utf-8")
    
    def clean_nan(self, obj):
        if isinstance(obj, dict):
            return {k: self.clean_nan(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.clean_nan(item) for item in obj]
        elif isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj):
                return 0.0
            return obj
        elif isinstance(obj, (int, str, bool, type(None))):
            return obj
        elif pd.isna(obj):
            return 0.0 if isinstance(obj, float) else ""
        return obj

# ============================================================================
# CONFIGURATION MANAGEMENT (JSON BASED)
# ============================================================================

class ConfigManager:
    """Advanced configuration management system using JSON"""
    
    def __init__(self, config_file='config.json'):
        self.config_file = config_file
        self.config = self.load_config()
    
    def load_config(self):
        """Load configuration from JSON file"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"⚠️ Error loading config: {e}")
        
        return self.get_default_config()
    
    def get_default_config(self):
        """Return default configuration with PINE STRATEGY settings"""
        return {
            'trading': {
                'max_concurrent_trades': 5,
                'max_long_trades': 2,
                'max_short_trades': 2,
                'max_positions_per_sector': 2,
                'risk_per_trade': 2.0,
                'max_daily_loss': 500,
                'max_drawdown': 0.15,
                'total_capital': 10000
            },
            'indicators': {
                'ema_fast': 50,
                'ema_slow': 200,
                'ema_mid': 50,
                'rsi_period': 14,
                'rsi_oversold': 45,
                'rsi_overbought': 55,
                'adx_period': 14,
                'adx_threshold': 20,
                'volume_period': 20,
                'volume_multiplier': 1.0,
                'macd_fast': 12,
                'macd_slow': 26,
                'macd_signal': 9
            },
            'optional_indicators': {
                'volume_enabled': True,
                'macd_enabled': False,
                'support_resistance_enabled': False,
                'support_resistance_period': 20
            },
            'risk_management': {
                'use_trailing_stop': True,
                'trailing_activation': 0.025,
                'trailing_distance': 0.008,
                'stop_loss_atr_multiplier': 2.0
            },
            'multi_timeframe': {
                'enabled': True,
                'timeframes': ['1h'],
                'required_confirmations': 2,
                'higher_tf_weight': 1.5
            },
            'order_management': {
                'use_limit_orders': False,
                'order_timeout': 30,
                'slippage_tolerance': 0.001,
                'retry_attempts': 3
            },
            'monitoring': {
                'performance_update_interval': 60,
                'alert_on_drawdown': 0.10,
                'alert_on_consecutive_losses': 5,
                'save_metrics_to_file': True
            },
            'signal_quality': {
                'freshness_minutes': 10,
                'require_volume_confirmation': True,
                'min_volume_multiplier': 1.0
            },
            'sideways_detection': {
                'enabled': True,
                'adx_threshold': 20,
                'choppiness_threshold': 61.8
            },
            'pine_strategy_settings': {
                'description': 'Pine Script Strategy: 1h EMA+RSI+ManualADX+Volume (ATR SL/TP)',
                'ema_fast': 50,
                'ema_slow': 200,
                'rsi_length': 14,
                'rsi_long_level': 55,
                'rsi_short_level': 45,
                'adx_length': 14,
                'adx_threshold': 20,
                'volume_sma_length': 20,
                'atr_length': 14,
                'atr_sl_multiplier': 2.0,
                'atr_tp_multiplier': 1.2,
                'atr_trail_multiplier': 1.0,
                'use_trailing_stop': True,
                'allow_long': True,
                'allow_short': True,
                'confirmation_bars': 1,
                'signal_freshness_minutes': 5
            }
        }
    
    def get(self, key, default=None):
        """Get configuration value by dot notation"""
        keys = key.split('.')
        value = self.config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k, default)
            else:
                return default
        return value
    
    def update(self, key, value):
        """Update configuration value"""
        keys = key.split('.')
        config = self.config
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        config[keys[-1]] = value
        self.save()
    
    def save(self):
        """Save configuration to file"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2, cls=SafeJSONEncoder)
            return True
        except Exception as e:
            logger.error(f"Error saving config: {e}")
            return False

# Initialize configuration
config_manager = ConfigManager()

# Get limits from config
MAX_CONCURRENT_TRADES = config_manager.get('trading.max_concurrent_trades', 5)
MAX_LONG_TRADES = config_manager.get('trading.max_long_trades', 2)
MAX_SHORT_TRADES = config_manager.get('trading.max_short_trades', 2)
MAX_POSITIONS_PER_SECTOR = config_manager.get('trading.max_positions_per_sector', 2)

# Get sideways detection settings
SIDEWAYS_ENABLED = config_manager.get('sideways_detection.enabled', True)
SIDEWAYS_ADX_THRESHOLD = config_manager.get('sideways_detection.adx_threshold', 20)
SIDEWAYS_CI_THRESHOLD = config_manager.get('sideways_detection.choppiness_threshold', 61.8)

# ============================================================================
# PINE STRATEGY SETTINGS FROM CONFIG.JSON
# ============================================================================

PINE_CONFIG = config_manager.get('pine_strategy_settings', {})

EMA_FAST = PINE_CONFIG.get('ema_fast', 50)
EMA_SLOW = PINE_CONFIG.get('ema_slow', 200)
RSI_LEN = PINE_CONFIG.get('rsi_length', 14)
RSI_OVERBOUGHT = PINE_CONFIG.get('rsi_long_level', 55)
RSI_OVERSOLD = PINE_CONFIG.get('rsi_short_level', 45)
ADX_LEN = PINE_CONFIG.get('adx_length', 14)
ADX_THR = PINE_CONFIG.get('adx_threshold', 20)
VOLUME_PERIOD = PINE_CONFIG.get('volume_sma_length', 20)
ATR_LEN = PINE_CONFIG.get('atr_length', 14)
ATR_SL_MULT = PINE_CONFIG.get('atr_sl_multiplier', 2.0)
ATR_TP_BASE_MULT = PINE_CONFIG.get('atr_tp_multiplier', 1.2)
ATR_TRAIL_MULT = PINE_CONFIG.get('atr_trail_multiplier', 1.0)
USE_TRAILING_STOP = PINE_CONFIG.get('use_trailing_stop', True)

# Calculate TP multipliers (3 levels)
ATR_TP1_MULT = ATR_TP_BASE_MULT      # TP1: ATR × 1.2
ATR_TP2_MULT = ATR_TP_BASE_MULT * 2  # TP2: ATR × 2.4
ATR_TP3_MULT = ATR_TP_BASE_MULT * 3  # TP3: ATR × 3.6

# Log loaded Pine settings
logger.info(f"PINE STRATEGY SETTINGS LOADED FROM CONFIG:")
logger.info(f"EMA: {EMA_FAST}/{EMA_SLOW}, RSI: {RSI_LEN} (Long>={RSI_OVERBOUGHT}, Short<={RSI_OVERSOLD})")
logger.info(f"ADX: {ADX_LEN} period, threshold {ADX_THR}")
logger.info(f"Volume SMA: {VOLUME_PERIOD}, ATR: {ATR_LEN}")
logger.info(f"SL Multiplier: {ATR_SL_MULT}, TP Base Multiplier: {ATR_TP_BASE_MULT}")
logger.info(f"TP Multipliers: TP1=ATR×{ATR_TP1_MULT}, TP2=ATR×{ATR_TP2_MULT}, TP3=ATR×{ATR_TP3_MULT}")
logger.info(f"Trail Multiplier: {ATR_TRAIL_MULT}, Use Trailing: {USE_TRAILING_STOP}")

# ============================================================================
# OPTIONAL INDICATOR SETTINGS (TOGGLE ON/OFF)
# ============================================================================

VOLUME_ENABLED = config_manager.get('optional_indicators.volume_enabled', True)
MACD_ENABLED = config_manager.get('optional_indicators.macd_enabled', False)
SUPPORT_RESISTANCE_ENABLED = config_manager.get('optional_indicators.support_resistance_enabled', False)

# Optional indicator parameters
VOLUME_MULTIPLIER = config_manager.get('indicators.volume_multiplier', 1.0)
MACD_FAST = config_manager.get('indicators.macd_fast', 12)
MACD_SLOW = config_manager.get('indicators.macd_slow', 26)
MACD_SIGNAL = config_manager.get('indicators.macd_signal', 9)
SUPPORT_RESISTANCE_PERIOD = config_manager.get('optional_indicators.support_resistance_period', 20)

# Log optional indicator status
logger.info(f" Optional Indicators: Volume={VOLUME_ENABLED}, MACD={MACD_ENABLED}, Support/Resistance={SUPPORT_RESISTANCE_ENABLED}")
logger.info(f" Position Limits: Longs={MAX_LONG_TRADES}, Shorts={MAX_SHORT_TRADES}, Total={MAX_CONCURRENT_TRADES}")
logger.info(f" Sector Limits: Max {MAX_POSITIONS_PER_SECTOR} positions per sector")
logger.info(f" Sideways Detection: {'✅ ENABLED' if SIDEWAYS_ENABLED else '❌ DISABLED'} (ADX<{SIDEWAYS_ADX_THRESHOLD} or CI>{SIDEWAYS_CI_THRESHOLD})")

# ============================================================================
# OPTIONAL INDICATOR FUNCTIONS
# ============================================================================

def calculate_volume_confirmation(df: pd.DataFrame, multiplier: float = VOLUME_MULTIPLIER) -> bool:
    """Check if current volume is above average volume"""
    try:
        if df.empty or len(df) < VOLUME_PERIOD:
            return True
        
        avg_volume = df['volume'].tail(VOLUME_PERIOD).mean()
        current_volume = df['volume'].iloc[-1]
        
        if current_volume > avg_volume * multiplier:
            logger.debug(f"✅ Volume confirmation: {current_volume:.0f} > {avg_volume * multiplier:.0f}")
            return True
        else:
            logger.debug(f"❌ Volume confirmation failed: {current_volume:.0f} < {avg_volume * multiplier:.0f}")
            return False
    except Exception as e:
        logger.debug(f"Volume calculation error: {e}")
        return True

def calculate_macd(df: pd.DataFrame, fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIGNAL) -> Tuple[bool, str]:
    """Calculate MACD and return signal"""
    try:
        if df.empty or len(df) < slow + signal:
            return True, "INSUFFICIENT_DATA"
        
        close = df['close']
        ema_fast = close.ewm(span=fast, adjust=False).mean()
        ema_slow = close.ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        
        current_macd = macd_line.iloc[-1]
        current_signal = signal_line.iloc[-1]
        current_histogram = histogram.iloc[-1]
        
        if current_macd > current_signal and current_histogram > 0:
            logger.debug(f"✅ MACD Bullish: MACD={current_macd:.4f} > Signal={current_signal:.4f}")
            return True, "BULLISH"
        elif current_macd < current_signal and current_histogram < 0:
            logger.debug(f"✅ MACD Bearish: MACD={current_macd:.4f} < Signal={current_signal:.4f}")
            return True, "BEARISH"
        else:
            logger.debug(f"❌ MACD Neutral: MACD={current_macd:.4f}, Signal={current_signal:.4f}")
            return False, "NEUTRAL"
    except Exception as e:
        logger.debug(f"MACD calculation error: {e}")
        return True, "ERROR"

def find_support_resistance(df: pd.DataFrame, current_price: float, period: int = SUPPORT_RESISTANCE_PERIOD) -> Tuple[bool, bool]:
    """Find support and resistance levels"""
    try:
        if df.empty or len(df) < period:
            return True, True
        
        recent_highs = df['high'].tail(period).max()
        recent_lows = df['low'].tail(period).min()
        
        resistance_threshold = recent_highs * 0.99
        support_threshold = recent_lows * 1.01
        
        is_above_support = current_price > support_threshold
        is_below_resistance = current_price < resistance_threshold
        
        logger.debug(f"📊 S/R: Support={recent_lows:.4f}, Resistance={recent_highs:.4f}, Price={current_price:.4f}")
        
        return is_above_support, is_below_resistance
    except Exception as e:
        logger.debug(f"Support/Resistance calculation error: {e}")
        return True, True

# ============================================================================
# SIDEWAYS MARKET DETECTION FUNCTION - FIXED DIVISION BY ZERO
# ============================================================================

def is_sideways_market(df: pd.DataFrame) -> bool:
    """
    Detect sideways/choppy market using ADX and Choppiness Index.
    Returns True if market is sideways (should avoid trading), False if trending.
    """
    if not SIDEWAYS_ENABLED:
        return False
    
    try:
        if df.empty or len(df) < 30:
            return False
        
        _, _, adx_val = calculate_adx(df)
        adx_current = adx_val.iloc[-1]
        
        if adx_current < SIDEWAYS_ADX_THRESHOLD:
            logger.info(f" Sideways market detected (ADX: {adx_current:.1f} < {SIDEWAYS_ADX_THRESHOLD})")
            return True
        
        period = 14
        high = df['high']
        low = df['low']
        
        tr = high - low
        atr_sum = tr.rolling(period).sum().iloc[-1]
        highest_high = high.rolling(period).max().iloc[-1]
        lowest_low = low.rolling(period).min().iloc[-1]
        
        price_range = highest_high - lowest_low
        if price_range <= 0.00000001:
            logger.debug(f"Price range too small ({price_range:.10f}), cannot calculate Choppiness Index")
            return False
        
        if atr_sum <= 0:
            logger.debug(f"ATR sum is zero or negative ({atr_sum:.10f}), cannot calculate Choppiness Index")
            return False
        
        ci = 100 * np.log10(atr_sum / price_range) / np.log10(period)
        
        if not np.isfinite(ci):
            logger.debug(f"Choppiness Index is not finite ({ci}), skipping")
            return False
        
        if ci > SIDEWAYS_CI_THRESHOLD:
            logger.info(f" Sideways market detected (Choppiness Index: {ci:.1f} > {SIDEWAYS_CI_THRESHOLD})")
            return True
        
        return False
    except Exception as e:
        logger.debug(f"Sideways detection error: {e}")
        return False

# ============================================================================
# SECTOR DEFINITIONS - CORRELATION PROTECTION
# ============================================================================

SECTORS = {
    'Layer1_Major': ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT']
}

def get_symbol_sector(symbol: str) -> str:
    """Get the sector for a given symbol"""
    for sector, symbols in SECTORS.items():
        if symbol in symbols:
            return sector
    return 'Unknown'

# ============================================================================
# RISK MANAGEMENT SYSTEM WITH PER-SIDE LIMITS AND SECTOR LIMITS
# ============================================================================

class RiskManager:
    """Advanced risk management system with per-side limits and sector limits"""
    
    def __init__(self):
        self.max_daily_loss = float(os.getenv("MAX_DAILY_LOSS", config_manager.get('trading.max_daily_loss', 500)))
        self.max_drawdown = float(os.getenv("MAX_DRAWDOWN", config_manager.get('trading.max_drawdown', 15)))
        self.max_concurrent_trades = int(os.getenv("MAX_CONCURRENT_TRADES", MAX_CONCURRENT_TRADES))
        self.max_position_size_pct = float(os.getenv("MAX_POSITION_SIZE_PCT", config_manager.get('trading.risk_per_trade', 2.0)))
        self.total_capital = float(os.getenv("TOTAL_CAPITAL", config_manager.get('trading.total_capital', 10000)))
        
        self.max_long_trades = int(os.getenv("MAX_LONG_TRADES", MAX_LONG_TRADES))
        self.max_short_trades = int(os.getenv("MAX_SHORT_TRADES", MAX_SHORT_TRADES))
        self.max_positions_per_sector = int(os.getenv("MAX_POSITIONS_PER_SECTOR", MAX_POSITIONS_PER_SECTOR))
        
        self.daily_pnl = 0
        self.peak_equity = 0
        self.daily_reset_time = datetime.now().replace(hour=0, minute=0, second=0)
        self.consecutive_losses = 0
        self.max_consecutive_losses = 5
        self.trade_history = deque(maxlen=100)
        self.risk_metrics = {
            'current_risk': 0,
            'var_95': 0,
            'expected_shortfall': 0,
            'sharpe_ratio': 0
        }
    
    def reset_daily(self):
        """Reset daily limits"""
        now = datetime.now()
        if now >= self.daily_reset_time + timedelta(days=1):
            self.daily_pnl = 0
            self.daily_reset_time = now.replace(hour=0, minute=0, second=0)
            self.consecutive_losses = 0
            logger.info(" Daily risk limits reset")
    
    def can_trade(self, symbol: str, side: str, capital: float, current_price: float) -> Tuple[bool, str]:
        """Check if trade is allowed based on risk rules including per-side limits and sector limits"""
        self.reset_daily()
        
        if self.daily_pnl <= -self.max_daily_loss:
            return False, f"Daily loss limit reached: {self.daily_pnl:.2f} USDT"
        
        if self.consecutive_losses >= self.max_consecutive_losses:
            return False, f"Max consecutive losses reached: {self.consecutive_losses}"
        
        state = load_state()
        open_trades = state.get("open_trades", {})
        total_trades = len(open_trades)
        
        long_count = 0
        short_count = 0
        for trade_symbol, trade in open_trades.items():
            trade_side = trade.get("side", "")
            remaining_qty = float(trade.get("remaining_quantity", 0))
            if remaining_qty > 0:
                if trade_side == "long":
                    long_count += 1
                elif trade_side == "short":
                    short_count += 1
        
        logger.info(f" Current positions: Longs={long_count}/{self.max_long_trades}, Shorts={short_count}/{self.max_short_trades}, Total={total_trades}/{self.max_concurrent_trades}")
        
        if side == "long" and long_count >= self.max_long_trades:
            logger.warning(f"❌ LONG LIMIT REACHED: {long_count}/{self.max_long_trades}")
            return False, f"Max LONG positions reached: {long_count}/{self.max_long_trades}"
        
        if side == "short" and short_count >= self.max_short_trades:
            logger.warning(f"❌ SHORT LIMIT REACHED: {short_count}/{self.max_short_trades}")
            return False, f"Max SHORT positions reached: {short_count}/{self.max_short_trades}"
        
        new_sector = get_symbol_sector(symbol)
        sector_counts = {}
        
        for trade_symbol, trade in open_trades.items():
            trade_sector = get_symbol_sector(trade_symbol)
            if trade_sector:
                sector_counts[trade_sector] = sector_counts.get(trade_sector, 0) + 1
        
        current_sector_count = sector_counts.get(new_sector, 0)
        if new_sector != 'Unknown' and current_sector_count >= self.max_positions_per_sector:
            return False, f"Max {self.max_positions_per_sector} positions in {new_sector} sector reached (currently {current_sector_count})"
        
        if total_trades >= self.max_concurrent_trades:
            return False, f"Max concurrent trades reached: {total_trades}/{self.max_concurrent_trades}"
        
        position_value = (TRADE_USDT / self.total_capital) * 100 if self.total_capital > 0 else 0
        if position_value > self.max_position_size_pct:
            return False, f"Position size {position_value:.1f}% exceeds {self.max_position_size_pct}%"
        
        logger.info(f" Risk check passed for {symbol} {side}. Longs: {long_count}/{self.max_long_trades}, Shorts: {short_count}/{self.max_short_trades}, Total: {total_trades}/{self.max_concurrent_trades}, Sector {new_sector}: {current_sector_count + 1}/{self.max_positions_per_sector}")
        
        return True, "OK"
    
    def update_after_trade(self, pnl: float):
        """Update risk metrics after trade"""
        self.daily_pnl += pnl
        self.trade_history.append(pnl)
        
        if pnl < 0:
            self.consecutive_losses += 1
        else:
            self.consecutive_losses = 0
        
        if len(self.trade_history) > 20:
            pnl_array = np.array(list(self.trade_history))
            self.risk_metrics['var_95'] = np.percentile(pnl_array, 5)
            self.risk_metrics['expected_shortfall'] = pnl_array[pnl_array <= self.risk_metrics['var_95']].mean()
            
            returns = pnl_array / TRADE_USDT
            if len(returns) > 1 and np.std(returns) > 0:
                self.risk_metrics['sharpe_ratio'] = np.mean(returns) / np.std(returns) * np.sqrt(252)
    
    def get_risk_report(self) -> Dict:
        """Get current risk report"""
        return {
            'daily_pnl': self.daily_pnl,
            'daily_loss_limit': self.max_daily_loss,
            'consecutive_losses': self.consecutive_losses,
            'max_consecutive_losses': self.max_consecutive_losses,
            'var_95': self.risk_metrics['var_95'],
            'expected_shortfall': self.risk_metrics['expected_shortfall'],
            'sharpe_ratio': self.risk_metrics['sharpe_ratio'],
            'current_risk_level': 'HIGH' if self.daily_pnl <= -self.max_daily_loss * 0.5 else 'MEDIUM' if self.daily_pnl <= -self.max_daily_loss * 0.25 else 'LOW',
            'max_long_trades': self.max_long_trades,
            'max_short_trades': self.max_short_trades,
            'max_positions_per_sector': self.max_positions_per_sector
        }

# Initialize risk manager
risk_manager = RiskManager()

# ============================================================================
# MULTI-TIMEFRAME ANALYSIS
# ============================================================================

class MultiTimeframeAnalyzer:
    """Advanced multi-timeframe analysis system"""
    
    def __init__(self):
        self.timeframes = config_manager.get('multi_timeframe.timeframes', ['5m', '15m', '1h'])
        self.required_confirmations = config_manager.get('multi_timeframe.required_confirmations', 3)
        self.higher_tf_weight = config_manager.get('multi_timeframe.higher_tf_weight', 1.5)
    
    def analyze(self, symbol: str, current_price: float) -> Tuple[str, int, Dict]:
        """Analyze multiple timeframes for signal confluence"""
        signals = {}
        confidences = {}
        
        for tf in self.timeframes:
            try:
                df = get_klines(symbol, tf, 100)
                if not df.empty and len(df) >= 30:
                    signal, confidence = self.calculate_signal_on_timeframe(df, symbol, current_price, tf)
                    signals[tf] = signal
                    confidences[tf] = confidence
                    logger.info(f" {symbol} - {tf} TF: Signal={signal}, Confidence={confidence:.1f}%")
            except Exception as e:
                logger.error(f"Error analyzing {symbol} on {tf}: {e}")
                signals[tf] = "HOLD"
                confidences[tf] = 0
        
        final_signal, final_confidence = self.calculate_weighted_signal(signals, confidences)
        
        analysis = {
            'signals': signals,
            'confidences': confidences,
            'weighted_signal': final_signal,
            'weighted_confidence': final_confidence,
            'confluence_level': self.get_confluence_level(signals)
        }
        
        return final_signal, final_confidence, analysis
    
    def calculate_signal_on_timeframe(self, df: pd.DataFrame, symbol: str, current_price: float, timeframe: str) -> Tuple[str, float]:
        """Calculate signal on specific timeframe"""
        try:
            ema_fast = df['close'].ewm(span=EMA_FAST).mean()
            ema_slow = df['close'].ewm(span=EMA_SLOW).mean()
            ema_mid = df['close'].ewm(span=EMA_FAST).mean()
            rsi_val = rsi(df['close'])
            _, _, adx_val = calculate_adx(df)
            
            ema_fast_current = ema_fast.iloc[-1]
            ema_slow_current = ema_slow.iloc[-1]
            ema_mid_current = ema_mid.iloc[-1]
            rsi_current = rsi_val.iloc[-1]
            adx_current = adx_val.iloc[-1]
            
            ema_fast_prev = ema_fast.iloc[-2] if len(ema_fast) > 1 else ema_fast_current
            ema_slow_prev = ema_slow.iloc[-2] if len(ema_slow) > 1 else ema_slow_current
            
            current_bullish = ema_fast_current > ema_slow_current
            previous_bullish = ema_fast_prev > ema_slow_prev
            fresh_bullish_crossover = (not previous_bullish and current_bullish)
            
            current_bearish = ema_fast_current < ema_slow_current
            previous_bearish = ema_fast_prev < ema_slow_prev
            fresh_bearish_crossover = (not previous_bearish and current_bearish)
            
            strong_bullish_trend = (current_bullish and (ema_fast_current - ema_slow_current) > (ema_slow_current * 0.002))
            strong_bearish_trend = (current_bearish and (ema_slow_current - ema_fast_current) > (ema_slow_current * 0.002))
            
            buy_condition = (fresh_bullish_crossover or strong_bullish_trend) and current_price > ema_mid_current and rsi_current > RSI_OVERSOLD and adx_current > ADX_THR
            sell_condition = (fresh_bearish_crossover or strong_bearish_trend) and current_price < ema_mid_current and rsi_current < RSI_OVERBOUGHT and adx_current > ADX_THR
            
            if buy_condition:
                confidence = min(100, 50 + (adx_current - ADX_THR) + (rsi_current - RSI_OVERSOLD))
                return "BUY", confidence
            elif sell_condition:
                confidence = min(100, 50 + (adx_current - ADX_THR) + (RSI_OVERBOUGHT - rsi_current))
                return "SELL", confidence
            else:
                return "HOLD", 0
        except Exception as e:
            logger.error(f"Error in timeframe analysis: {e}")
            return "HOLD", 0
    
    def calculate_weighted_signal(self, signals: Dict, confidences: Dict) -> Tuple[str, int]:
        """Calculate weighted signal from multiple timeframes"""
        buy_weight = 0
        sell_weight = 0
        total_weight = 0
        
        for i, (tf, signal) in enumerate(signals.items()):
            weight = 1.0
            if tf == self.timeframes[-1]:
                weight = self.higher_tf_weight
            
            if signal == "BUY":
                buy_weight += confidences.get(tf, 0) * weight
            elif signal == "SELL":
                sell_weight += confidences.get(tf, 0) * weight
            
            total_weight += weight
        
        if total_weight > 0:
            buy_confidence = (buy_weight / total_weight) if buy_weight > 0 else 0
            sell_confidence = (sell_weight / total_weight) if sell_weight > 0 else 0
            
            if buy_confidence > 60 and buy_confidence > sell_confidence:
                return "BUY", int(buy_confidence)
            elif sell_confidence > 60 and sell_confidence > buy_confidence:
                return "SELL", int(sell_confidence)
        
        return "HOLD", 0
    
    def get_confluence_level(self, signals: Dict) -> str:
        """Get confluence level based on signal alignment"""
        buy_count = sum(1 for s in signals.values() if s == "BUY")
        sell_count = sum(1 for s in signals.values() if s == "SELL")
        total = len(signals)
        
        if buy_count >= self.required_confirmations:
            return f"STRONG_BUY ({buy_count}/{total})"
        elif sell_count >= self.required_confirmations:
            return f"STRONG_SELL ({sell_count}/{total})"
        elif buy_count > 0 or sell_count > 0:
            return f"WEAK ({max(buy_count, sell_count)}/{total})"
        else:
            return "NO_CONFLUENCE"

# Initialize multi-timeframe analyzer
mtf_analyzer = MultiTimeframeAnalyzer()

# ============================================================================
# ADVANCED ORDER TYPES
# ============================================================================

class AdvancedOrderManager:
    """Advanced order types management"""
    
    def __init__(self):
        self.order_timeout = config_manager.get('order_management.order_timeout', 30)
        self.slippage_tolerance = config_manager.get('order_management.slippage_tolerance', 0.001)
        self.retry_attempts = config_manager.get('order_management.retry_attempts', 3)
    
    def place_limit_order(self, symbol: str, side: str, quantity: float, price: float) -> Optional[Dict]:
        """Place limit order for better entry prices"""
        if DRY_RUN:
            logger.info(f"[DRY-RUN] LIMIT {side.upper()} {quantity:.6f} {symbol} @ {price:.4f}")
            return {"orderId": "DRY_RUN", "status": "FILLED"}
        
        try:
            order_side = "BUY" if side.lower() in ("buy", "long") else "SELL"
            order = client.create_order(
                symbol=symbol,
                side=order_side,
                type='LIMIT',
                timeInForce='GTC',
                quantity=round(quantity, 6),
                price=round(price, 6)
            )
            logger.info(f" LIMIT order placed: {order}")
            return order
        except Exception as e:
            logger.error(f"Limit order failed for {symbol}: {e}")
            return None
    
    def get_smart_entry_price(self, symbol: str, side: str, current_price: float) -> float:
        """Calculate smart entry price based on order book"""
        try:
            if client is not None:
                order_book = client.get_order_book(symbol=symbol, limit=10)
                if side == "BUY":
                    bids = order_book.get('bids', [])
                    if bids:
                        return float(bids[0][0]) * (1 - self.slippage_tolerance)
                else:
                    asks = order_book.get('asks', [])
                    if asks:
                        return float(asks[0][0]) * (1 + self.slippage_tolerance)
        except Exception as e:
            logger.debug(f"Error getting order book: {e}")
        
        return current_price

# Initialize advanced order manager
order_manager = AdvancedOrderManager()

# ============================================================================
# REAL-TIME PERFORMANCE MONITORING
# ============================================================================

class PerformanceMonitor:
    """Real-time performance monitoring system"""
    
    def __init__(self):
        self.metrics = {
            'hourly': deque(maxlen=24),
            'daily': deque(maxlen=30),
            'weekly': deque(maxlen=52)
        }
        self.start_time = datetime.now()
        self.last_update = datetime.now()
        self.update_interval = config_manager.get('monitoring.performance_update_interval', 60)
        self.alert_on_drawdown = config_manager.get('monitoring.alert_on_drawdown', 0.10)
        self.alert_on_consecutive_losses = config_manager.get('monitoring.alert_on_consecutive_losses', 5)
        self.save_metrics = config_manager.get('monitoring.save_metrics_to_file', True)
        self.peak_equity = 0
        self.equity_curve = []
    
    def update_metrics(self) -> Dict:
        """Update performance metrics in real-time"""
        state = load_state()
        open_trades = state.get("open_trades", {})
        df = safe_read_trades()
        
        total_pnl = self.calculate_total_pnl(df)
        win_rate = self.calculate_win_rate(df)
        sharpe_ratio = self.calculate_sharpe_ratio(df)
        max_drawdown = self.calculate_max_drawdown(total_pnl)
        profit_factor = self.calculate_profit_factor(df)
        avg_win = self.calculate_avg_win(df)
        avg_loss = self.calculate_avg_loss(df)
        
        self.update_equity_curve(total_pnl)
        
        metrics = {
            "timestamp": datetime.now().isoformat(),
            "open_trades": len(open_trades),
            "total_pnl": total_pnl,
            "win_rate": win_rate,
            "sharpe_ratio": sharpe_ratio,
            "max_drawdown": max_drawdown,
            "profit_factor": profit_factor,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "total_trades": len(df[df["Signal"].str.contains("Exit|Close", na=False)]),
            "active_symbols": len(open_trades),
            "system_uptime": str(datetime.now() - self.start_time),
            "risk_metrics": risk_manager.get_risk_report(),
            "cache_stats": data_cache.get_stats(),
            "session_stats": binance_session.get_stats(),
            "optional_indicators": {
                "volume": VOLUME_ENABLED,
                "macd": MACD_ENABLED,
                "support_resistance": SUPPORT_RESISTANCE_ENABLED
            },
            "sideways_detection": {
                "enabled": SIDEWAYS_ENABLED,
                "adx_threshold": SIDEWAYS_ADX_THRESHOLD,
                "ci_threshold": SIDEWAYS_CI_THRESHOLD
            }
        }
        
        self.metrics['hourly'].append(metrics)
        
        alerts = self.check_alerts(metrics)
        if alerts:
            for alert in alerts:
                logger.warning(f"⚠️ ALERT: {alert}")
        
        if self.save_metrics:
            self.save_metrics_to_file(metrics)
        
        self.last_update = datetime.now()
        return metrics
    
    def calculate_total_pnl(self, df: pd.DataFrame) -> float:
        """Calculate total P&L"""
        if df.empty:
            return 0.0
        exit_trades = df[df["Signal"].str.contains("Exit|Close", na=False)]
        total = 0.0
        for _, trade in exit_trades.iterrows():
            pnl = trade.get("Net P&L", "0")
            try:
                pnl_val = float(str(pnl).replace('USDT', '').replace('+', '').replace(',', '').strip())
                if not math.isnan(pnl_val):
                    total += pnl_val
            except:
                continue
        return total
    
    def calculate_win_rate(self, df: pd.DataFrame) -> float:
        """Calculate win rate percentage"""
        exit_trades = df[df["Signal"].str.contains("Exit|Close", na=False)]
        if exit_trades.empty:
            return 0.0
        wins = 0
        for _, trade in exit_trades.iterrows():
            pnl = trade.get("Net P&L", "0")
            try:
                pnl_val = float(str(pnl).replace('USDT', '').replace('+', '').replace(',', '').strip())
                if pnl_val > 0:
                    wins += 1
            except:
                continue
        return (wins / len(exit_trades)) * 100 if len(exit_trades) > 0 else 0
    
    def calculate_sharpe_ratio(self, df: pd.DataFrame) -> float:
        """Calculate Sharpe ratio"""
        exit_trades = df[df["Signal"].str.contains("Exit|Close", na=False)]
        if len(exit_trades) < 2:
            return 0.0
        
        returns = []
        for _, trade in exit_trades.iterrows():
            pnl = trade.get("Net P&L", "0")
            try:
                pnl_val = float(str(pnl).replace('USDT', '').replace('+', '').replace(',', '').strip())
                if not math.isnan(pnl_val):
                    returns.append(pnl_val / TRADE_USDT)
            except:
                continue
        
        if len(returns) > 1 and np.std(returns) > 0:
            return np.mean(returns) / np.std(returns) * np.sqrt(252)
        return 0.0
    
    def calculate_max_drawdown(self, total_pnl: float) -> float:
        """Calculate maximum drawdown"""
        self.peak_equity = max(self.peak_equity, total_pnl)
        if self.peak_equity > 0:
            drawdown = (self.peak_equity - total_pnl) / self.peak_equity * 100
            return max(0, drawdown)
        return 0.0
    
    def calculate_profit_factor(self, df: pd.DataFrame) -> float:
        """Calculate profit factor"""
        exit_trades = df[df["Signal"].str.contains("Exit|Close", na=False)]
        gross_profit = 0.0
        gross_loss = 0.0
        
        for _, trade in exit_trades.iterrows():
            pnl = trade.get("Net P&L", "0")
            try:
                pnl_val = float(str(pnl).replace('USDT', '').replace('+', '').replace(',', '').strip())
                if pnl_val > 0:
                    gross_profit += pnl_val
                else:
                    gross_loss += abs(pnl_val)
            except:
                continue
        
        return gross_profit / gross_loss if gross_loss > 0 else float('inf')
    
    def calculate_avg_win(self, df: pd.DataFrame) -> float:
        """Calculate average winning trade"""
        exit_trades = df[df["Signal"].str.contains("Exit|Close", na=False)]
        wins = []
        for _, trade in exit_trades.iterrows():
            pnl = trade.get("Net P&L", "0")
            try:
                pnl_val = float(str(pnl).replace('USDT', '').replace('+', '').replace(',', '').strip())
                if pnl_val > 0:
                    wins.append(pnl_val)
            except:
                continue
        return np.mean(wins) if wins else 0.0
    
    def calculate_avg_loss(self, df: pd.DataFrame) -> float:
        """Calculate average losing trade"""
        exit_trades = df[df["Signal"].str.contains("Exit|Close", na=False)]
        losses = []
        for _, trade in exit_trades.iterrows():
            pnl = trade.get("Net P&L", "0")
            try:
                pnl_val = float(str(pnl).replace('USDT', '').replace('+', '').replace(',', '').strip())
                if pnl_val < 0:
                    losses.append(abs(pnl_val))
            except:
                continue
        return np.mean(losses) if losses else 0.0
    
    def update_equity_curve(self, total_pnl: float):
        """Update equity curve"""
        self.equity_curve.append(total_pnl)
        if len(self.equity_curve) > 1000:
            self.equity_curve = self.equity_curve[-1000:]
    
    def check_alerts(self, metrics: Dict) -> List[str]:
        """Check for alert conditions"""
        alerts = []
        
        if metrics['max_drawdown'] > self.alert_on_drawdown * 100:
            alerts.append(f"High drawdown detected: {metrics['max_drawdown']:.2f}%")
        
        if risk_manager.consecutive_losses >= self.alert_on_consecutive_losses:
            alerts.append(f"Consecutive losses: {risk_manager.consecutive_losses}")
        
        if metrics['profit_factor'] < 1.0 and metrics['profit_factor'] != float('inf'):
            alerts.append(f"Profit factor below 1.0: {metrics['profit_factor']:.2f}")
        
        if metrics['win_rate'] < 30 and metrics['total_trades'] > 10:
            alerts.append(f"Low win rate: {metrics['win_rate']:.1f}%")
        
        return alerts
    
    def save_metrics_to_file(self, metrics: Dict):
        """Save metrics to file for analysis"""
        try:
            filename = f"performance_metrics_{datetime.now().strftime('%Y%m%d')}.json"
            existing = []
            if os.path.exists(filename):
                with open(filename, 'r') as f:
                    existing = json.load(f)
            
            existing.append(metrics)
            
            with open(filename, 'w') as f:
                json.dump(existing[-1000:], f, indent=2, cls=SafeJSONEncoder)
        except Exception as e:
            logger.error(f"Error saving metrics: {e}")
    
    def get_performance_report(self) -> Dict:
        """Get comprehensive performance report"""
        metrics = self.update_metrics()
        return {
            'current_metrics': metrics,
            'hourly_trend': list(self.metrics['hourly'])[-24:],
            'daily_trend': list(self.metrics['daily'])[-30:],
            'equity_curve': self.equity_curve[-100:],
            'peak_equity': self.peak_equity,
            'start_time': self.start_time.isoformat(),
            'uptime': str(datetime.now() - self.start_time)
        }

# Initialize performance monitor
performance_monitor = PerformanceMonitor()

if FASTAPI_AVAILABLE:
    app = FastAPI(title="Trading Bot", version="1.0.0")
    
    # Dashboard authentication
    security = HTTPBasic()
    
    def verify_auth(credentials: HTTPBasicCredentials = Depends(security)):
        """Verify dashboard authentication"""
        correct_username = DASHBOARD_USER
        correct_password = DASHBOARD_PASSWORD
        if credentials.username != correct_username or credentials.password != correct_password:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid credentials",
                headers={"WWW-Authenticate": "Basic"},
            )
        return True
else:
    app = None

# Use values from PINE_CONFIG (already set above)
CONFIRMATION_REQUIRED = 1

# TP closure percentages (keep your existing)
TP1_CLOSE_PERCENT = 0.35
TP2_CLOSE_PERCENT = 0.30  
TP3_CLOSE_PERCENT = 0.20
TRAILING_PERCENT = 0.15

TRAILING_ACTIVATION_PERCENT = config_manager.get('risk_management.trailing_activation', 0.025)
TRAILING_DISTANCE_PERCENT = config_manager.get('risk_management.trailing_distance', 0.008)

TRADE_COOLDOWN = 900

# EXPANDED SYMBOLS
SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"
]

BASE_DIR = os.path.dirname(__file__) or "."
TRADES_FILE = os.path.join(BASE_DIR, "trades.csv")
STATE_FILE  = os.path.join(BASE_DIR, "state.json")

_state_lock = threading.RLock()
crossover_lock = threading.Lock()

# Store bot start time to track signal freshness
BOT_START_TIME = datetime.now()

def load_ema_crossover_states():
    """Load EMA crossover states from persistent storage"""
    try:
        state = load_state()
        return state.get("ema_crossover_states", {})
    except Exception as e:
        logger.error(f"Error loading EMA crossover states: {e}")
        return {}

def save_ema_crossover_states(ema_states):
    """Save EMA crossover states to persistent storage"""
    try:
        state = load_state()
        state["ema_crossover_states"] = ema_states
        save_state(state)
        logger.debug("✅ EMA crossover states saved successfully")
    except Exception as e:
        logger.error(f"Error saving EMA crossover states: {e}")

def reset_crossover_states():
    """Reset all EMA crossover states on bot startup to prevent old signals"""
    try:
        state = load_state()
        state["ema_crossover_states"] = {}
        save_state(state)
        logger.info(" Reset all EMA crossover states - ignoring historical signals")
    except Exception as e:
        logger.error(f"Error resetting crossover states: {e}")

# ============================================================================
# PINE STYLE WILDER RMA AND ADX FUNCTIONS
# ============================================================================

def wilder_rma(series: pd.Series, length: int) -> pd.Series:
    """
    Wilder's Moving Average (RMA) - exact match to Pine's ta.rma
    RMA = (alpha * value) + (1 - alpha) * previous_rma, where alpha = 1/length
    """
    alpha = 1.0 / length
    rma = series.copy()
    
    for i in range(len(series)):
        if i == 0:
            rma.iloc[i] = series.iloc[i] if not pd.isna(series.iloc[i]) else 0.0
        else:
            if not pd.isna(series.iloc[i]) and not pd.isna(rma.iloc[i-1]):
                rma.iloc[i] = alpha * series.iloc[i] + (1 - alpha) * rma.iloc[i-1]
            elif not pd.isna(series.iloc[i]):
                rma.iloc[i] = series.iloc[i]
            else:
                rma.iloc[i] = rma.iloc[i-1] if i > 0 else 0.0
    
    return rma

def calculate_pine_adx(df: pd.DataFrame, length: int = 14) -> pd.Series:
    """
    Manual ADX calculation matching Pine Script exactly
    Uses Wilder's smoothing (RMA) for +DM, -DM, and TR
    """
    high = df['high']
    low = df['low']
    close = df['close']
    
    # True Range
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    
    # +DM and -DM
    up = high - high.shift(1)
    down = low.shift(1) - low
    
    plus_dm = pd.Series(0.0, index=df.index)
    minus_dm = pd.Series(0.0, index=df.index)
    
    for i in range(1, len(df)):
        up_val = up.iloc[i] if not pd.isna(up.iloc[i]) else 0.0
        down_val = down.iloc[i] if not pd.isna(down.iloc[i]) else 0.0
        
        if up_val > down_val and up_val > 0:
            plus_dm.iloc[i] = up_val
        else:
            plus_dm.iloc[i] = 0.0
        
        if down_val > up_val and down_val > 0:
            minus_dm.iloc[i] = down_val
        else:
            minus_dm.iloc[i] = 0.0
    
    # Wilder smoothing (RMA)
    atr_wilder = wilder_rma(tr, length)
    plus_dm_r = wilder_rma(plus_dm, length)
    minus_dm_r = wilder_rma(minus_dm, length)
    
    # +DI and -DI (avoid division by zero)
    plus_di = pd.Series(0.0, index=df.index)
    minus_di = pd.Series(0.0, index=df.index)
    
    mask = atr_wilder != 0
    plus_di[mask] = 100 * (plus_dm_r[mask] / atr_wilder[mask])
    minus_di[mask] = 100 * (minus_dm_r[mask] / atr_wilder[mask])
    
    # DX
    sum_di = plus_di + minus_di
    dx = pd.Series(0.0, index=df.index)
    mask2 = sum_di != 0
    dx[mask2] = 100 * (plus_di[mask2] - minus_di[mask2]).abs() / sum_di[mask2]
    
    # ADX (smoothed with RMA)
    adx = wilder_rma(dx, length)
    
    return adx

# ============================================================================
# FRESH SIGNAL DETECTION FUNCTION
# ============================================================================

def is_signal_fresh(crossover_time: datetime) -> bool:
    """Check if the signal occurred within the freshness window"""
    now = datetime.now()
    age_minutes = (now - crossover_time).total_seconds() / 60
    
    if age_minutes > SIGNAL_FRESHNESS_MINUTES:
        logger.debug(f"Signal is OLD ({age_minutes:.1f} min ago), ignoring")
        return False
    
    logger.debug(f"Signal is FRESH ({age_minutes:.1f} min ago)")
    return True

def get_crossover_time_from_df(df: pd.DataFrame) -> datetime:
    """Extract the timestamp of the most recent candle from dataframe"""
    try:
        if 'close_time' in df.columns:
            timestamp_ms = df['close_time'].iloc[-1]
            return datetime.fromtimestamp(timestamp_ms / 1000)
        else:
            return datetime.now()
    except Exception as e:
        logger.debug(f"Error getting crossover time: {e}")
        return datetime.now()

# FastAPI Routes with MANUAL CONTROLS and AUTHENTICATION
if FASTAPI_AVAILABLE:
    @app.get("/", response_class=HTMLResponse, dependencies=[Depends(verify_auth)])
    async def dashboard():
        try:
            html_content = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>Trading Bot Dashboard - Institute Grade</title>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <style>
                    body { font-family: Arial, sans-serif; margin: 20px; background: #0f0f23; color: #00ff00; }
                    .container { max-width: 1400px; margin: 0 auto; }
                    .header { text-align: center; padding: 20px; border-bottom: 1px solid #00ff00; }
                    .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 15px; margin: 20px 0; }
                    .stat-card { background: #1a1a2e; padding: 15px; border-radius: 8px; border: 1px solid #00ff00; }
                    .risk-card { background: #1a1a2e; padding: 15px; border-radius: 8px; border: 1px solid #ffaa00; }
                    .metric { font-size: 1.5em; font-weight: bold; }
                    .positive { color: #00ff00; }
                    .negative { color: #ff4444; }
                    .warning { color: #ffaa00; }
                    .open-trades { margin: 20px 0; }
                    .trade-card { background: #1a1a2e; padding: 15px; margin: 10px 0; border-radius: 8px; border: 1px solid #444; position: relative; }
                    .long { border-left: 4px solid #00ff00; }
                    .short { border-left: 4px solid #ff4444; }
                    button { background: #00ff00; color: black; border: none; padding: 10px 20px; margin: 5px; border-radius: 4px; cursor: pointer; }
                    button:hover { background: #00cc00; }
                    .close-btn { background: #ff4444; color: white; padding: 5px 10px; font-size: 12px; margin-left: 10px; }
                    .close-btn:hover { background: #cc0000; }
                    .controls { margin: 20px 0; }
                    .tp-hit { background: #00ff00; color: black; padding: 2px 6px; border-radius: 3px; font-weight: bold; }
                    .system-stats { background: #1a1a2e; padding: 15px; border-radius: 8px; margin-top: 20px; }
                    .indicator-status { display: inline-block; padding: 2px 6px; border-radius: 4px; font-size: 10px; margin-left: 5px; }
                    .indicator-enabled { background: #00aa00; color: white; }
                    .indicator-disabled { background: #666666; color: white; }
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        <h1>🤖 Institute Grade Trading Bot - PINE STRATEGY EXACT MATCH</h1>
                        <p>EMA50/200 + RSI55/45 + ADX20 + Volume | ATR-Based SL/TP | 3-TP System | Fresh Signals Only | Per-Side Limits</p>
                    </div>
                    
                    <div class="controls">
                        <button onclick="refreshData()">🔄 Refresh</button>
                        <button onclick="clearHistory()" style="background: #ff4444; color: white;">🗑️ Clear History</button>
                        <button onclick="showPerformance()" style="background: #ffaa00;">📊 Performance Report</button>
                        <button onclick="showRisk()" style="background: #ffaa00;">⚠️ Risk Report</button>
                        <button onclick="showSystemStats()" style="background: #00aaff;">📈 System Stats</button>
                    </div>
                    
                    <div id="stats" class="stats-grid"></div>
                    <div id="risk-stats" class="stats-grid"></div>
                    
                    <div class="open-trades">
                        <h2>📊 Open Trades</h2>
                        <div id="open-trades"></div>
                    </div>
                    
                    <div class="trade-history">
                        <h2>📈 Trade History</h2>
                        <div id="trade-history"></div>
                    </div>
                    
                    <div id="system-stats" class="system-stats" style="display:none;">
                        <h3>⚙️ System Statistics</h3>
                        <div id="system-stats-content"></div>
                    </div>
                </div>
                
                <script>
                    async function refreshData() {
                        try {
                            const [statsRes, riskRes, tradesRes, historyRes, systemRes] = await Promise.all([
                                fetch('/api/stats'),
                                fetch('/api/risk-metrics'),
                                fetch('/api/open-trades'),
                                fetch('/api/trade-history'),
                                fetch('/api/system-stats')
                            ]);
                            
                            const stats = await statsRes.json();
                            const risk = await riskRes.json();
                            const openTrades = await tradesRes.json();
                            const tradeHistory = await historyRes.json();
                            const systemStats = await systemRes.json();
                            
                            document.getElementById('stats').innerHTML = `
                                <div class="stat-card">
                                    <h3>📈 Long Trades</h3>
                                    <p>Total: ${stats.long?.total || 0}</p>
                                    <p class="positive">Wins: ${stats.long?.success || 0}</p>
                                    <p class="negative">Losses: ${stats.long?.fail || 0}</p>
                                </div>
                                <div class="stat-card">
                                    <h3>📉 Short Trades</h3>
                                    <p>Total: ${stats.short?.total || 0}</p>
                                    <p class="positive">Wins: ${stats.short?.success || 0}</p>
                                    <p class="negative">Losses: ${stats.short?.fail || 0}</p>
                                </div>
                                <div class="stat-card">
                                    <h3>⚙️ System</h3>
                                    <p>Symbols: ${stats.symbols_count || 0}</p>
                                    <p>Dry Run: ${stats.dry_run ? 'Yes' : 'No'}</p>
                                    <p>Confirmations: ${stats.confirmations || 0}</p>
                                    <p>Open Trades: ${stats.open_trades_count || 0}</p>
                                    <p>Fresh Signals Only: ${stats.freshness_minutes} min</p>
                                    <p>Sector Limit: ${stats.sector_limit || 2} per sector</p>
                                    <p>Sideways Detection: ${stats.sideways_enabled ? 'Active' : 'Disabled'}</p>
                                    <p>Optional Indicators: <span class="indicator-status ${stats.volume_enabled ? 'indicator-enabled' : 'indicator-disabled'}">Volume</span> <span class="indicator-status ${stats.macd_enabled ? 'indicator-enabled' : 'indicator-disabled'}">MACD</span> <span class="indicator-status ${stats.sr_enabled ? 'indicator-enabled' : 'indicator-disabled'}">S/R</span></p>
                                </div>
                                <div class="stat-card">
                                    <h3>📊 Performance</h3>
                                    <p>Win Rate: ${stats.win_rate || 0}%</p>
                                    <p>Profit Factor: ${stats.profit_factor || 0}</p>
                                    <p>Sharpe Ratio: ${stats.sharpe_ratio || 0}</p>
                                </div>
                            `;
                            
                            document.getElementById('risk-stats').innerHTML = `
                                <div class="risk-card">
                                    <h3>⚠️ Risk Metrics</h3>
                                    <p>Daily P&L: <span class="${risk.daily_pnl >= 0 ? 'positive' : 'negative'}">${risk.daily_pnl || 0} USDT</span></p>
                                    <p>Daily Limit: ${risk.daily_loss_limit || 0} USDT</p>
                                    <p>Consecutive Losses: <span class="${risk.consecutive_losses > 3 ? 'warning' : ''}">${risk.consecutive_losses || 0}</span></p>
                                    <p>VaR (95%): ${risk.var_95 || 0} USDT</p>
                                    <p>Risk Level: <span class="${risk.current_risk_level === 'HIGH' ? 'negative' : risk.current_risk_level === 'MEDIUM' ? 'warning' : 'positive'}">${risk.current_risk_level || 'LOW'}</span></p>
                                    <p>Position Limits: Longs: ${risk.max_long_trades || 2} | Shorts: ${risk.max_short_trades || 2} | Per Sector: ${risk.max_positions_per_sector || 2}</p>
                                </div>
                            `;
                            
                            const uniqueOpenTrades = [];
                            const seenSymbols = new Set();
                            for (const trade of openTrades) {
                                if (!seenSymbols.has(trade.symbol)) {
                                    seenSymbols.add(trade.symbol);
                                    uniqueOpenTrades.push(trade);
                                }
                            }
                            
                            const openTradesHtml = uniqueOpenTrades.length > 0 ? 
                                uniqueOpenTrades.map(trade => {
                                    let unrealizedPnl = 0;
                                    let unrealizedClass = '';
                                    let unrealizedText = '';
                                    
                                    if (trade.current_price && trade.entry_price && trade.remaining_quantity && trade.current_price !== "Loading...") {
                                        const entry = parseFloat(trade.entry_price);
                                        const current = parseFloat(trade.current_price);
                                        const qty = parseFloat(trade.remaining_quantity);
                                        
                                        if (!isNaN(entry) && !isNaN(current) && !isNaN(qty) && qty > 0 && current > 0) {
                                            if (trade.side === 'long') {
                                                unrealizedPnl = (current - entry) * qty;
                                            } else {
                                                unrealizedPnl = (entry - current) * qty;
                                            }
                                            unrealizedClass = unrealizedPnl >= 0 ? 'positive' : 'negative';
                                            unrealizedText = unrealizedPnl >= 0 ? `+${unrealizedPnl.toFixed(2)} USDT` : `${unrealizedPnl.toFixed(2)} USDT`;
                                        } else {
                                            unrealizedText = 'N/A';
                                        }
                                    } else {
                                        unrealizedText = 'Loading...';
                                    }
                                    
                                    return `
                                    <div class="trade-card ${trade.side}" id="trade-${trade.symbol}">
                                        <div style="display: flex; justify-content: space-between; align-items: center;">
                                            <h3 style="margin:0;">${trade.symbol} - ${trade.side.toUpperCase()} #${trade.trade_num}</h3>
                                            <button onclick="closeTrade('${trade.symbol}')" class="close-btn" style="background: #ff4444; color: white; padding: 5px 10px; border-radius: 4px; cursor: pointer;">🔴 Close Trade</button>
                                        </div>
                                        <p>Entry: ${parseFloat(trade.entry_price).toFixed(8)} | Current: ${typeof trade.current_price === 'number' ? trade.current_price.toFixed(8) : trade.current_price}</p>
                                        <p>Remaining: ${trade.remaining_quantity} | Trailing: ${trade.trailing_active ? 'Active' : 'Inactive'}</p>
                                        <p>SL: ${typeof trade.sl === 'number' ? trade.sl.toFixed(8) : trade.sl} 
                                           ${trade.tp1_hit ? '<span class="tp-hit">TP1✓</span>' : `TP1: ${typeof trade.tp1 === 'number' ? trade.tp1.toFixed(8) : trade.tp1}`} 
                                           ${trade.tp2_hit ? '<span class="tp-hit">TP2✓</span>' : `TP2: ${typeof trade.tp2 === 'number' ? trade.tp2.toFixed(8) : trade.tp2}`} 
                                           ${trade.tp3_hit ? '<span class="tp-hit">TP3✓</span>' : `TP3: ${typeof trade.tp3 === 'number' ? trade.tp3.toFixed(8) : trade.tp3}`}</p>
                                        ${trade.partial_profit ? `<p class="positive">💰 Partial Profit: ${trade.partial_profit} USDT</p>` : ''}
                                        <p>📊 Unrealized P&L: <span class="${unrealizedClass}">${unrealizedText}</span></p>
                                    </div>
                                `}).join('') : '<p>No open trades</p>';
                            
                            document.getElementById('open-trades').innerHTML = openTradesHtml;
                            
                            const historyHtml = tradeHistory.length > 0 ?
                                '<table style="width:100%; border-collapse:collapse; margin-top:10px;">' +
                                '    <tr style="background:#00ff00; color:black;"><th>Trade#</th><th>Symbol</th><th>Side</th><th>Entry</th><th>Exit</th><th>P&L</th><th>Time</th></tr>' +
                                tradeHistory.map(trade => `
                                    <tr style="border-bottom:1px solid #444;">
                                        <td style="padding:8px;">${trade.trade_num}</td>
                                        <td style="padding:8px;">${trade.symbol}</td>
                                        <td style="padding:8px;" class="${trade.side === 'LONG' ? 'positive' : 'negative'}">${trade.side}</td>
                                        <td style="padding:8px;">${trade.entry_price}</td>
                                        <td style="padding:8px;">${trade.exit_price || 'Open'}</td>
                                        <td style="padding:8px;" class="${trade.pnl >= 0 ? 'positive' : 'negative'}">${trade.pnl}</td>
                                        <td style="padding:8px;">${trade.time}</td>
                                    </tr>
                                `).join('') + '</table>' : '<p>No trade history</p>';
                            document.getElementById('trade-history').innerHTML = historyHtml;
                            
                            document.getElementById('system-stats-content').innerHTML = `
                                <p><strong>Cache:</strong> Hits: ${systemStats.cache_hits}, Misses: ${systemStats.cache_misses}, Hit Rate: ${systemStats.cache_hit_rate}</p>
                                <p><strong>API Requests:</strong> ${systemStats.api_requests} total requests</p>
                                <p><strong>Uptime:</strong> ${systemStats.uptime}</p>
                                <p><strong>Signal Freshness:</strong> Only signals within last ${systemStats.freshness_minutes} minutes</p>
                                <p><strong>Sector Limit:</strong> Max ${systemStats.sector_limit} positions per sector</p>
                                <p><strong>Sideways Detection:</strong> ${systemStats.sideways_enabled ? 'Enabled' : 'Disabled'} (ADX<${systemStats.sideways_adx_threshold} or CI>${systemStats.sideways_ci_threshold})</p>
                                <p><strong>Optional Indicators:</strong> Volume=${systemStats.volume_enabled}, MACD=${systemStats.macd_enabled}, Support/Resistance=${systemStats.sr_enabled}</p>
                            `;
                            
                        } catch (error) {
                            console.error('Error refreshing data:', error);
                        }
                    }
                    
                    async function closeTrade(symbol) {
                        if (!confirm(`Are you sure you want to close ${symbol} trade?`)) return;
                        
                        try {
                            const formData = new FormData();
                            formData.append('symbol', symbol);
                            
                            const response = await fetch('/api/close-trade', {
                                method: 'POST',
                                body: formData
                            });
                            
                            const result = await response.json();
                            
                            if (response.ok) {
                                alert(result.message);
                                refreshData();
                            } else {
                                alert('Error: ' + result.error);
                            }
                        } catch (error) {
                            console.error('Error closing trade:', error);
                            alert('Failed to close trade: ' + error.message);
                        }
                    }
                    
                    async function showPerformance() {
                        try {
                            const response = await fetch('/api/performance-report');
                            const report = await response.json();
                            alert(JSON.stringify(report.current_metrics, null, 2));
                        } catch (error) {
                            console.error('Error fetching performance:', error);
                        }
                    }
                    
                    async function showRisk() {
                        try {
                            const response = await fetch('/api/risk-report');
                            const report = await response.json();
                            alert(JSON.stringify(report, null, 2));
                        } catch (error) {
                            console.error('Error fetching risk:', error);
                        }
                    }
                    
                    function showSystemStats() {
                        const statsDiv = document.getElementById('system-stats');
                        if (statsDiv.style.display === 'none') {
                            statsDiv.style.display = 'block';
                            refreshData();
                        } else {
                            statsDiv.style.display = 'none';
                        }
                    }
                    
                    async function clearHistory() {
                        if (confirm('Are you sure you want to clear all trade history?')) {
                            try {
                                const response = await fetch('/api/clear-history', { method: 'POST' });
                                if (response.ok) {
                                    alert('History cleared successfully');
                                    refreshData();
                                } else {
                                    alert('Error clearing history');
                                }
                            } catch (error) {
                                console.error('Error clearing history:', error);
                            }
                        }
                    }
                    
                    document.addEventListener('DOMContentLoaded', refreshData);
                    setInterval(refreshData, 30000);
                </script>
            </body>
            </html>
            """
            return HTMLResponse(content=html_content)
        except Exception as e:
            logger.error(f"Dashboard error: {e}")
            return HTMLResponse(content=f"<h1>Error loading dashboard: {e}</h1>")

    @app.get("/api/stats", response_class=SafeJSONResponse, dependencies=[Depends(verify_auth)])
    async def get_stats():
        try:
            state = load_state()
            stats = state.get("stats", {
                "long": {"total": 0, "success": 0, "fail": 0},
                "short": {"total": 0, "success": 0, "fail": 0}
            })
            
            perf_metrics = performance_monitor.update_metrics()
            
            def clean_stats(d):
                if isinstance(d, dict):
                    return {k: clean_stats(v) for k, v in d.items()}
                elif isinstance(d, float) and (math.isnan(d) or math.isinf(d)):
                    return 0
                elif pd.isna(d):
                    return 0
                return d
            
            cleaned_stats = clean_stats(stats)
            
            return {
                "long": cleaned_stats.get("long", {"total": 0, "success": 0, "fail": 0}),
                "short": cleaned_stats.get("short", {"total": 0, "success": 0, "fail": 0}),
                "symbols_count": len(SYMBOLS),
                "dry_run": DRY_RUN,
                "confirmations": CONFIRMATION_REQUIRED,
                "open_trades_count": len(state.get("open_trades", {})),
                "win_rate": perf_metrics.get('win_rate', 0),
                "profit_factor": perf_metrics.get('profit_factor', 0),
                "sharpe_ratio": perf_metrics.get('sharpe_ratio', 0),
                "freshness_minutes": SIGNAL_FRESHNESS_MINUTES,
                "volume_enabled": VOLUME_ENABLED,
                "macd_enabled": MACD_ENABLED,
                "sr_enabled": SUPPORT_RESISTANCE_ENABLED,
                "sector_limit": MAX_POSITIONS_PER_SECTOR,
                "sideways_enabled": SIDEWAYS_ENABLED,
                "sideways_adx_threshold": SIDEWAYS_ADX_THRESHOLD,
                "sideways_ci_threshold": SIDEWAYS_CI_THRESHOLD
            }
        except Exception as e:
            logger.error(f"Stats API error: {e}")
            return {"error": str(e)}

    @app.get("/api/system-stats", response_class=SafeJSONResponse, dependencies=[Depends(verify_auth)])
    async def get_system_stats():
        try:
            cache_stats = data_cache.get_stats()
            session_stats = binance_session.get_stats()
            
            return {
                "cache_hits": cache_stats['hits'],
                "cache_misses": cache_stats['misses'],
                "cache_hit_rate": cache_stats['hit_rate'],
                "cache_size": cache_stats['cache_size'],
                "api_requests": session_stats['total_requests'],
                "uptime": str(datetime.now() - performance_monitor.start_time),
                "freshness_minutes": SIGNAL_FRESHNESS_MINUTES,
                "volume_enabled": VOLUME_ENABLED,
                "macd_enabled": MACD_ENABLED,
                "sr_enabled": SUPPORT_RESISTANCE_ENABLED,
                "sector_limit": MAX_POSITIONS_PER_SECTOR,
                "sideways_enabled": SIDEWAYS_ENABLED,
                "sideways_adx_threshold": SIDEWAYS_ADX_THRESHOLD,
                "sideways_ci_threshold": SIDEWAYS_CI_THRESHOLD
            }
        except Exception as e:
            logger.error(f"System stats API error: {e}")
            return {"error": str(e)}

    @app.get("/api/risk-metrics", response_class=SafeJSONResponse, dependencies=[Depends(verify_auth)])
    async def get_risk_metrics():
        try:
            return risk_manager.get_risk_report()
        except Exception as e:
            logger.error(f"Risk metrics API error: {e}")
            return {"error": str(e)}

    @app.get("/api/risk-report", response_class=SafeJSONResponse, dependencies=[Depends(verify_auth)])
    async def get_risk_report():
        try:
            return risk_manager.get_risk_report()
        except Exception as e:
            logger.error(f"Risk report API error: {e}")
            return {"error": str(e)}

    @app.get("/api/performance-report", response_class=SafeJSONResponse, dependencies=[Depends(verify_auth)])
    async def get_performance_report():
        try:
            return performance_monitor.get_performance_report()
        except Exception as e:
            logger.error(f"Performance report API error: {e}")
            return {"error": str(e)}

    @app.get("/api/open-trades", response_class=SafeJSONResponse, dependencies=[Depends(verify_auth)])
    async def get_open_trades():
        try:
            state = load_state()
            open_trades = state.get("open_trades", {})
            result = []
            
            for symbol, trade in open_trades.items():
                try:
                    current_price = get_latest_price(symbol)
                    if current_price is None or current_price == 0:
                        current_price = "Loading..."
                    
                    tp_targets = trade.get("tp_targets", {})
                    
                    def safe_float_val(val, default=0):
                        try:
                            if val is None or pd.isna(val):
                                return default
                            fval = float(val)
                            if math.isnan(fval) or math.isinf(fval):
                                return default
                            return fval
                        except:
                            return default
                    
                    result.append({
                        "symbol": symbol,
                        "side": str(trade.get("side", "")),
                        "entry_price": safe_float_val(trade.get("entry_price", 0)),
                        "current_price": current_price,
                        "quantity": safe_float_val(trade.get("total_quantity", 0)),
                        "trade_num": trade.get("trade_num", 0),
                        "pnl": 0,
                        "entry_time": str(trade.get("entry_time", "")),
                        "sl": safe_float_val(trade.get("sl", 0)),
                        "tp1": safe_float_val(trade.get("tp1", 0)),
                        "tp2": safe_float_val(trade.get("tp2", 0)),
                        "tp3": safe_float_val(trade.get("tp3", 0)),
                        "tp1_hit": tp_targets.get("tp1", {}).get("hit", False),
                        "tp2_hit": tp_targets.get("tp2", {}).get("hit", False),
                        "tp3_hit": tp_targets.get("tp3", {}).get("hit", False),
                        "remaining_quantity": safe_float_val(trade.get("remaining_quantity", 0)),
                        "trailing_active": trade.get("trailing_active", False),
                        "partial_profit": safe_float_val(trade.get("partial_profit", 0))
                    })
                except Exception as e:
                    logger.error(f"Error processing trade {symbol}: {e}")
                    continue
            
            return result
        except Exception as e:
            logger.error(f"Open trades API error: {e}")
            return {"error": str(e)}
    
    @app.post("/api/close-trade", response_class=SafeJSONResponse, dependencies=[Depends(verify_auth)])
    async def close_trade_api(symbol: str = Form(...)):
        """Manually close an open trade at current market price"""
        try:
            state = load_state()
            open_trades = state.get("open_trades", {})
            
            if symbol not in open_trades:
                return JSONResponse(
                    status_code=404,
                    content={"error": f"No open trade found for {symbol}"}
                )
            
            trade = open_trades[symbol]
            side = trade.get("side")
            entry_price = float(trade.get("entry_price", 0))
            remaining_quantity = float(trade.get("remaining_quantity", 0))
            trade_num = trade.get("trade_num", 0)
            
            if remaining_quantity <= 0:
                return JSONResponse(
                    status_code=400,
                    content={"error": f"Trade for {symbol} has no remaining quantity"}
                )
            
            current_price = get_validated_price(symbol)
            if current_price is None:
                return JSONResponse(
                    status_code=500,
                    content={"error": f"Could not get current price for {symbol}"}
                )
            
            if side == "long":
                pnl = (current_price - entry_price) * remaining_quantity
            else:
                pnl = (entry_price - current_price) * remaining_quantity
            
            close_side = "sell" if side == "long" else "buy"
            execute_success = execute_trade_with_validation(close_side, symbol, remaining_quantity, current_price)
            
            if not execute_success:
                return JSONResponse(
                    status_code=500,
                    content={"error": f"Failed to execute close order for {symbol}"}
                )
            
            risk_manager.update_after_trade(pnl)
            
            log_close(symbol, side, entry_price, current_price, remaining_quantity, trade_num, "Manual Close")
            
            del open_trades[symbol]
            save_state(state)
            
            logger.info(f" Manually closed {symbol} {side} trade at {current_price:.4f}, PnL: {pnl:.2f} USDT")
            
            return {
                "message": f"Closed {symbol} {side} trade at {current_price:.4f}, PnL: {pnl:.2f} USDT",
                "symbol": symbol,
                "side": side,
                "exit_price": current_price,
                "pnl": round(pnl, 2)
            }
        except Exception as e:
            logger.error(f"Manual close error for {symbol}: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse(
                status_code=500,
                content={"error": str(e)}
            )

    @app.get("/api/trade-history", response_class=SafeJSONResponse, dependencies=[Depends(verify_auth)])
    async def get_trade_history():
        """Fixed trade history - groups by trade number, shows cumulative P&L and final exit price"""
        try:
            df = safe_read_trades()
            if df.empty:
                return []
            
            def safe_str(val, default=""):
                try:
                    if val is None or pd.isna(val):
                        return default
                    return str(val).strip()
                except:
                    return default
            
            def safe_pnl(val):
                try:
                    if val is None or pd.isna(val):
                        return 0
                    clean = str(val).replace('USDT', '').replace('+', '').replace(',', '').strip()
                    if clean == '' or clean.lower() in ('nan', 'none', 'null'):
                        return 0
                    fval = float(clean)
                    if math.isnan(fval) or math.isinf(fval):
                        return 0
                    return fval
                except:
                    return 0
            
            trade_numbers = df["Trade #"].dropna().unique()
            
            result = []
            
            for trade_num in trade_numbers:
                trade_num_str = str(trade_num).strip()
                if not trade_num_str:
                    continue
                
                trade_rows = df[df["Trade #"].astype(str).str.strip() == trade_num_str]
                
                entry_row = None
                total_pnl = 0
                last_exit_price = None
                
                for _, row in trade_rows.iterrows():
                    signal = safe_str(row.get("Signal", ""))
                    
                    if "Entry" in signal:
                        entry_row = row
                    elif "Exit" in signal or "Partial Close" in signal:
                        pnl = safe_pnl(row.get("Net P&L", "0"))
                        total_pnl += pnl
                        last_exit_price = safe_str(row.get("Price", ""))
                
                if entry_row is not None:
                    symbol = safe_str(entry_row.get("Symbol", ""))
                    side = safe_str(entry_row.get("Side", ""))
                    entry_price = safe_str(entry_row.get("Price", ""))
                    time_str = safe_str(entry_row.get("Date/Time", ""))
                    
                    if last_exit_price == "" or last_exit_price is None:
                        last_exit_price = "Open"
                        total_pnl = 0
                    
                    result.append({
                        "trade_num": trade_num_str,
                        "symbol": symbol,
                        "side": side,
                        "entry_price": entry_price,
                        "exit_price": last_exit_price,
                        "pnl": round(total_pnl, 2),
                        "time": time_str
                    })
            
            result.sort(key=lambda x: int(x['trade_num']) if x['trade_num'].isdigit() else 0, reverse=True)
            
            return result[:100]
        except Exception as e:
            logger.error(f"Trade history API error: {e}")
            logger.error(traceback.format_exc())
            return {"error": str(e)}

    @app.post("/api/clear-history", response_class=SafeJSONResponse, dependencies=[Depends(verify_auth)])
    async def clear_history_api():
        try:
            success = reset_history()
            if success:
                return {"message": "History cleared successfully"}
            else:
                return {"error": "Failed to clear history"}
        except Exception as e:
            logger.error(f"Clear history API error: {e}")
            return {"error": str(e)}

    @app.post("/api/update-trade", response_class=SafeJSONResponse, dependencies=[Depends(verify_auth)])
    async def update_trade_api(
        symbol: str = Form(...),
        sl: float = Form(None),
        tp1: float = Form(None),
        tp2: float = Form(None),
        tp3: float = Form(None)
    ):
        try:
            state = load_state()
            if symbol not in state.get("open_trades", {}):
                return {"error": f"No open trade found for {symbol}"}
            
            trade = state["open_trades"][symbol]
            updated = False
            updates = []
            
            if sl is not None:
                trade["sl"] = float(sl)
                updates.append(f"SL: {sl}")
                updated = True
            
            if tp1 is not None:
                trade["tp1"] = float(tp1)
                if "tp_targets" in trade and "tp1" in trade["tp_targets"]:
                    trade["tp_targets"]["tp1"]["price"] = float(tp1)
                updates.append(f"TP1: {tp1}")
                updated = True
            
            if tp2 is not None:
                trade["tp2"] = float(tp2)
                if "tp_targets" in trade and "tp2" in trade["tp_targets"]:
                    trade["tp_targets"]["tp2"]["price"] = float(tp2)
                updates.append(f"TP2: {tp2}")
                updated = True
            
            if tp3 is not None:
                trade["tp3"] = float(tp3)
                if "tp_targets" in trade and "tp3" in trade["tp_targets"]:
                    trade["tp_targets"]["tp3"]["price"] = float(tp3)
                updates.append(f"TP3: {tp3}")
                updated = True
            
            if updated:
                save_state(state)
                message = f"Successfully updated {symbol} - " + ", ".join(updates)
                logger.info(f" Manual update: {message}")
                return {"message": message}
            else:
                return {"error": "No valid parameters provided"}
        except Exception as e:
            logger.error(f"Manual update error for {symbol}: {e}")
            return {"error": str(e)}

# Enhanced Error Handling Decorator
def safe_execute(default_return=None, max_retries=3):
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {str(e)}")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                    continue
            logger.error(f"All retries failed for {func.__name__}: {str(last_exception)}")
            return default_return
        return wrapper
    return decorator

# Helpers
def _to_float(x, default=0.0):
    try:
        if x is None:
            return default
        str_value = str(x).strip().upper()
        if 'E' in str_value:
            return float(str_value)
        return float(str_value)
    except Exception as e:
        logger.debug(f"Float conversion failed for {x}: {e}")
        return default

def _ensure_state_keys(st: dict):
    if not isinstance(st, dict):
        st = {}
    if "open_trades" not in st:
        st["open_trades"] = {}
    if "stats" not in st:
        st["stats"] = {
            "long": {"total": 0, "success": 0, "fail": 0},
            "short": {"total": 0, "success": 0, "fail": 0}
        }
    if "ema_crossover_states" not in st:
        st["ema_crossover_states"] = {}
    if "trade_history" not in st:
        st["trade_history"] = []
    return st

# Enhanced File helpers
@safe_execute(default_return=pd.DataFrame())
def safe_read_trades():
    with _state_lock:
        try:
            if os.path.exists(TRADES_FILE):
                df = pd.read_csv(TRADES_FILE, dtype=str)
                logger.info(f"Successfully loaded {len(df)} trades from CSV")
                return df
            else:
                logger.info("Trades file does not exist, returning empty DataFrame")
        except Exception as e:
            logger.error(f"Error reading trades file: {e}")
        cols = ["Trade #","Symbol","Side","Type","Date/Time","Signal","Price","Position size","Net P&L","Run-up","Drawdown","Cumulative P&L"]
        return pd.DataFrame(columns=cols)

@safe_execute(default_return=False)
def append_trade_row(row: dict):
    with _state_lock:
        try:
            cols = ["Trade #","Symbol","Side","Type","Date/Time","Signal","Price","Position size","Net P&L","Run-up","Drawdown","Cumulative P&L"]
            df = safe_read_trades()
            df = df[[c for c in df.columns if c in cols]] if not df.empty else pd.DataFrame(columns=cols)
            
            for c in cols:
                if c not in df.columns:
                    df[c] = ""
            
            new_row = {c: row.get(c,"") for c in cols}
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            
            df.to_csv(TRADES_FILE, index=False, columns=cols)
            logger.info(f"Successfully appended trade #{row.get('Trade #', 'N/A')} to CSV")
            return True
        except Exception as e:
            logger.error(f"Failed to append trade row: {e}")
            return False

def clean_state_for_json(state):
    """Recursively clean state dictionary of NaN/inf values"""
    if isinstance(state, dict):
        cleaned = {}
        for key, value in state.items():
            cleaned[key] = clean_state_for_json(value)
        return cleaned
    elif isinstance(state, list):
        return [clean_state_for_json(item) for item in state]
    elif isinstance(state, float):
        if math.isnan(state) or math.isinf(state):
            return None
        return state
    elif isinstance(state, (int, str, bool, type(None))):
        return state
    elif pd.isna(state):
        return None
    else:
        return state

@safe_execute(default_return={})
def load_state():
    with _state_lock:
        try:
            if not os.path.exists(STATE_FILE):
                logger.info("State file does not exist, returning empty state")
                return {}
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                st = json.load(f) or {}
                logger.debug("Successfully loaded state from file")
        except Exception as e:
            logger.error(f"Error loading state file: {e}")
            st = {}
        st = _ensure_state_keys(st)
        return st

@safe_execute(default_return=False)
def save_state(st):
    with _state_lock:
        try:
            st = _ensure_state_keys(st)
            st = clean_state_for_json(st)
            retries = 6
            delay = 0.08
            data = json.dumps(st, indent=2, ensure_ascii=False, cls=SafeJSONEncoder)
            
            for attempt in range(retries):
                try:
                    dirn = os.path.dirname(STATE_FILE) or "."
                    fd, tmp = tempfile.mkstemp(prefix="state_", dir=dirn, text=True)
                    try:
                        with os.fdopen(fd, "w", encoding="utf-8") as tmpf:
                            tmpf.write(data)
                        os.replace(tmp, STATE_FILE)
                        logger.debug("Successfully saved state to file")
                        return True
                    finally:
                        if os.path.exists(tmp):
                            try:
                                os.remove(tmp)
                            except Exception:
                                pass
                except PermissionError:
                    logger.warning(f"Permission error saving state, retry {attempt + 1}")
                    time.sleep(delay)
                    delay *= 1.5
                except Exception as e:
                    logger.warning(f"Error saving state, retry {attempt + 1}: {e}")
                    time.sleep(delay)
                    delay *= 1.5
            
            logger.error("Failed to save state after all retries")
            return False
        except Exception as e:
            logger.error(f"Unexpected error in save_state: {e}")
            return False

@safe_execute(default_return=1)
def next_trade_number():
    with _state_lock:
        try:
            state = load_state()
            open_trades = state.get("open_trades", {})
            max_trade_num = 0
            
            for symbol, trade in open_trades.items():
                trade_num = trade.get("trade_num", 0)
                if trade_num and trade_num > max_trade_num:
                    max_trade_num = trade_num
            
            df = safe_read_trades()
            if not df.empty and "Trade #" in df.columns:
                nums = pd.to_numeric(df["Trade #"], errors="coerce")
                if nums.notna().any():
                    csv_max = int(nums.max())
                    if csv_max > max_trade_num:
                        max_trade_num = csv_max
            
            import threading
            if not hasattr(next_trade_number, "_lock"):
                next_trade_number._lock = threading.Lock()
            
            with next_trade_number._lock:
                if not hasattr(next_trade_number, "_last_used"):
                    next_trade_number._last_used = max_trade_num
                
                next_num = max(next_trade_number._last_used + 1, max_trade_num + 1)
                next_trade_number._last_used = next_num
                
                logger.debug(f"Next trade number: {next_num}")
                return next_num
        except Exception as e:
            logger.error(f"Error calculating next trade number: {e}")
            return int(time.time()) % 10000

@safe_execute(default_return=False)
def reset_history():
    with _state_lock:
        try:
            df_init = pd.DataFrame(columns=[
                "Trade #","Symbol","Side","Type","Date/Time","Signal","Price",
                "Position size","Net P&L","Run-up","Drawdown","Cumulative P&L"
            ])
            df_init.to_csv(TRADES_FILE, index=False)
            save_state(_ensure_state_keys({}))
            logger.info("Trade history and state cleared successfully")
            return True
        except Exception as e:
            logger.error(f"Error resetting history: {e}")
            return False

@safe_execute(default_return=False)
def ensure_files():
    try:
        if not os.path.exists(TRADES_FILE):
            reset_history()
        if not os.path.exists(STATE_FILE):
            save_state(_ensure_state_keys({}))
        logger.info("Required files ensured")
        return True
    except Exception as e:
        logger.error(f"Error ensuring files: {e}")
        return False

# Enhanced Indicators with Error Handling
@safe_execute(default_return=pd.Series([50.0]))
def rsi(series: pd.Series, length=14):
    try:
        delta = series.diff()
        up = delta.clip(lower=0.0)
        down = -delta.clip(upper=0.0)
        roll_up = up.ewm(alpha=1/length, adjust=False).mean()
        roll_down = down.ewm(alpha=1/length, adjust=False).mean()
        roll_down = roll_down.replace(0, np.nan)
        rs = roll_up / roll_down
        r = 100 - (100 / (1 + rs))
        return r.fillna(50).replace([np.inf, -np.inf], 50)
    except Exception as e:
        logger.error(f"RSI calculation error: {e}")
        length_data = len(series) if hasattr(series, "__len__") else 1
        return pd.Series([50.0] * length_data)

@safe_execute(default_return=(pd.Series([0.0]), pd.Series([0.0]), pd.Series([0.0])))
def calculate_adx(df, period=14):
    try:
        if df.empty:
            return pd.Series([0.0]), pd.Series([0.0]), pd.Series([0.0])
            
        high = df['high']; low = df['low']; close = df['close']
        tr1 = high - low
        tr2 = (high - close.shift()).abs()
        tr3 = (low - close.shift()).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        plus_dm = high.diff()
        minus_dm = (-low.diff()).abs()
        plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0)
        minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0)
        atr_val = tr.rolling(period).mean()
        plus_di = 100 * (plus_dm.rolling(period).mean() / atr_val.replace(0, np.nan))
        minus_di = 100 * (minus_dm.rolling(period).mean() / atr_val.replace(0, np.nan))
        dx = (abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, np.nan)) * 100
        adx = dx.rolling(period).mean().fillna(0)
        return plus_di.fillna(0), minus_di.fillna(0), adx
    except Exception as e:
        logger.error(f"ADX calculation error: {e}")
        length = len(df) if hasattr(df, "__len__") else 0
        return pd.Series([0]*length), pd.Series([0]*length), pd.Series([0]*length)

@safe_execute(default_return=pd.Series([0.0]))
def atr(df: pd.DataFrame, length=14):
    try:
        if df.empty:
            return pd.Series([0.0])
            
        high = df['high']; low = df['low']; close = df['close']
        tr1 = high - low
        tr2 = (high - close.shift()).abs()
        tr3 = (low - close.shift()).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        return tr.rolling(length).mean().fillna(0)
    except Exception as e:
        logger.error(f"ATR calculation error: {e}")
        length_data = len(df) if hasattr(df, "__len__") else 0
        return pd.Series([0]*length_data)

# Enhanced Market Helpers with Caching
client = None

def _parse_kline_value(val):
    try:
        return float(val) if (val is not None and str(val).lower() not in ("nan","none","")) else 0.0
    except Exception:
        return 0.0

def initialize_binance_client():
    global client
    
    if not BINANCE_AVAILABLE:
        logger.error("❌ Binance library not available")
        return False
    
    if not API_KEY or not API_SECRET:
        logger.error("❌ API_KEY or API_SECRET missing")
        return False
    
    try:
        import socket
        socket.create_connection(("8.8.8.8", 53), timeout=5)
        
        if USE_TESTNET:
            logger.info(" Configuring for BINANCE TESTNET")
            client = Client(
                API_KEY, 
                API_SECRET,
                testnet=True,
                requests_params={"timeout": 15}
            )
            try:
                account = client.get_account()
                logger.info(" Binance TESTNET connection SUCCESSFUL")
                logger.info(f" Testnet Account: {account['accountType']}")
                return True
            except Exception as e:
                logger.error(f"❌ Testnet connection failed: {e}")
                return False
        else:
            client = Client(API_KEY, API_SECRET, requests_params={"timeout": 15})
            client.get_account()
            logger.info(" Binance MAINNET connection SUCCESSFUL")
            return True
            
    except Exception as e:
        logger.error(f"❌ Binance initialization failed: {e}")
        client = None
        return False

@safe_execute(default_return=pd.DataFrame())
def get_klines(symbol, interval='1h', limit=250):
    """Get klines with caching - 1h timeframe for Pine strategy"""
    cached_data = data_cache.get(symbol, interval, limit)
    if cached_data is not None:
        return cached_data
    
    if client is None:
        logger.error(f" Binance client not initialized for {symbol}")
        return pd.DataFrame()
    
    try:
        logger.info(f" Fetching ACTUAL Binance data for {symbol}")
        raw = client.get_klines(symbol=symbol, interval=interval, limit=limit)
        data = []
        for k in raw:
            try:
                data.append({
                    "open_time": k[0],
                    "open": _parse_kline_value(k[1]),
                    "high": _parse_kline_value(k[2]),
                    "low": _parse_kline_value(k[3]),
                    "close": _parse_kline_value(k[4]),
                    "volume": _parse_kline_value(k[5]),
                    "close_time": k[6]
                })
            except Exception as e:
                logger.debug(f"Error parsing kline data: {e}")
                continue
        
        df = pd.DataFrame(data)
        if not df.empty:
            df.ffill(inplace=True)
            df.fillna(0, inplace=True)
            logger.info(f" Successfully fetched {len(df)} ACTUAL klines for {symbol}")
            data_cache.set(symbol, interval, limit, df)
        else:
            logger.error(f"❌ No data received for {symbol}")
            
        return df
        
    except Exception as e:
        logger.error(f"❌ ACTUAL klines fetch error for {symbol}: {e}")
        return pd.DataFrame()

@safe_execute(default_return=None)
def get_latest_price(symbol):
    """Get real-time price directly from Binance - NO CACHE"""
    try:
        if client is not None:
            ticker = client.get_symbol_ticker(symbol=symbol)
            price = float(ticker['price'])
            logger.info(f"💰 REAL-TIME PRICE {symbol}: {price}")
            return price
        else:
            if USE_TESTNET:
                url = "https://testnet.binance.vision/api/v3/ticker/price"
            else:
                url = "https://api.binance.com/api/v3/ticker/price"
            
            response = requests.get(url, params={"symbol": symbol}, timeout=5)
            if response.status_code == 200:
                price = float(response.json()['price'])
                logger.info(f" REAL-TIME PRICE {symbol}: {price}")
                return price
    except Exception as e:
        logger.error(f"❌ Failed to get price for {symbol}: {e}")
    
    return None

@safe_execute(default_return=None)
def get_validated_price(symbol, max_retries=3):
    for attempt in range(max_retries):
        try:
            if client is not None:
                ticker = client.get_symbol_ticker(symbol=symbol)
                price = float(ticker['price'])
                logger.info(f" ACTUAL BINANCE PRICE: {symbol} = {price}")
                return price
            else:
                if USE_TESTNET:
                    url = "https://testnet.binance.vision/api/v3/ticker/price"
                else:
                    url = "https://api.binance.com/api/v3/ticker/price"
                
                response = binance_session.get(url, params={"symbol": symbol}, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    price = float(data['price'])
                    logger.info(f" DIRECT API PRICE: {symbol} = {price}")
                    return price
        except Exception as e:
            logger.warning(f"Attempt {attempt+1}: Error getting actual price for {symbol}: {e}")
            if attempt < max_retries - 1:
                time.sleep(1)
    
    logger.error(f"❌ Failed to get ACTUAL price for {symbol} after {max_retries} attempts")
    return None

# Enhanced Orders
@safe_execute(default_return=False)
def place_order(side, symbol, qty):
    if DRY_RUN or client is None:
        logger.info(f"[DRY-RUN] {side.upper()} {qty:.6f} {symbol}")
        return True
    try:
        if isinstance(side, str) and side.lower() in ("buy", "sell"):
            order_side = side.upper()
        else:
            order_side = "BUY" if side == "long" else "SELL"
        
        use_limit_orders = config_manager.get('order_management.use_limit_orders', False)
        
        if use_limit_orders:
            current_price = get_validated_price(symbol)
            smart_price = order_manager.get_smart_entry_price(symbol, order_side, current_price)
            order = order_manager.place_limit_order(symbol, order_side, qty, smart_price)
            return order is not None
        else:
            result = client.create_order(symbol=symbol, side=order_side, type="MARKET", quantity=round(qty, 6))
            logger.info(f"Successfully placed {order_side} order for {qty:.6f} {symbol}")
            return True
    except Exception as e:
        logger.error(f"Order error for {symbol}: {e}")
        return False

def execute_trade_with_validation(side, symbol, quantity, price=None):
    try:
        if price is None:
            price = get_validated_price(symbol)
            if price is None:
                logger.error(f"❌ Cannot execute trade for {symbol} - invalid price")
                return False
        
        if quantity is None:
            quantity = calculate_quantity(price)
            if quantity <= 0:
                logger.error(f"❌ Invalid quantity calculated for {symbol}: {quantity}")
                return False
        
        capital = risk_manager.total_capital
        
        risk_side = "long" if side.lower() in ("buy", "long") else "short"
        
        can_trade, reason = risk_manager.can_trade(symbol, risk_side, capital, price)
        if not can_trade:
            logger.warning(f"⚠️ Risk check failed for {symbol}: {reason}")
            return False
        
        logger.info(f" Executing {side.upper()} trade for {symbol} at validated price: {price}")
        
        if place_order(side, symbol, quantity):
            logger.info(f" Successfully executed {side.upper()} trade for {symbol}")
            return True
        else:
            logger.error(f"❌ Failed to execute {side.upper()} trade for {symbol}")
            return False
    except Exception as e:
        logger.error(f"❌ Error executing trade for {symbol}: {e}")
        return False

# ============================================================================
# PINE MATCH: ATR-BASED STOP LOSS (EXACT MATCH)
# ============================================================================

def calculate_proper_sl(symbol, entry_price, side, df):
    """
    Match Pine strategy EXACTLY: SL = entry_price ± (ATR × multiplier)
    Uses ATR from the last closed candle
    """
    try:
        if df.empty or len(df) < ATR_LEN + 5:
            logger.warning(f"Insufficient data for {symbol}, using fallback SL")
            if side == "long":
                return entry_price * 0.99
            else:
                return entry_price * 1.01
        
        # Calculate ATR value
        atr_value = atr(df, length=ATR_LEN).iloc[-1]
        
        if atr_value <= 0:
            logger.warning(f"ATR is zero for {symbol}, using fallback SL")
            if side == "long":
                return entry_price * 0.99
            else:
                return entry_price * 1.01
        
        if side == "long":
            # Pine: longSL = close - atr * atrSLMult
            sl = entry_price - (atr_value * ATR_SL_MULT)
            logger.info(f" LONG SL (Pine match): {sl:.8f} (entry - ATR×{ATR_SL_MULT})")
        else:
            # Pine: shortSL = close + atr * atrSLMult
            sl = entry_price + (atr_value * ATR_SL_MULT)
            logger.info(f" SHORT SL (Pine match): {sl:.8f} (entry + ATR×{ATR_SL_MULT})")
        
        return sl
        
    except Exception as e:
        logger.error(f"Error calculating proper SL: {e}")
        if side == "long":
            return entry_price * 0.99
        else:
            return entry_price * 1.01

# ============================================================================
# PINE MATCH: ATR-BASED TP WITH 3 LEVELS (YOUR ENHANCEMENT)
# ============================================================================

def set_multi_tp_profit_distribution(symbol, entry_price, side, df, total_quantity):
    """Set ATR-based TP targets matching Pine strategy (with 3 TP levels)"""
    try:
        # Calculate ATR value
        atr_value = atr(df, length=ATR_LEN).iloc[-1]
        
        # ATR-based TP (Pine base multiplier with your 3-level enhancement)
        if side == "long":
            tp1_price = entry_price + (atr_value * ATR_TP1_MULT)
            tp2_price = entry_price + (atr_value * ATR_TP2_MULT)
            tp3_price = entry_price + (atr_value * ATR_TP3_MULT)
            
            logger.info(f" LONG TP Calculated (ATR={atr_value:.8f}):")
            logger.info(f"   TP1={tp1_price:.8f} (ATR×{ATR_TP1_MULT})")
            logger.info(f"   TP2={tp2_price:.8f} (ATR×{ATR_TP2_MULT})")
            logger.info(f"   TP3={tp3_price:.8f} (ATR×{ATR_TP3_MULT})")
        else:
            tp1_price = entry_price - (atr_value * ATR_TP1_MULT)
            tp2_price = entry_price - (atr_value * ATR_TP2_MULT)
            tp3_price = entry_price - (atr_value * ATR_TP3_MULT)
            
            logger.info(f" SHORT TP Calculated (ATR={atr_value:.8f}):")
            logger.info(f"   TP1={tp1_price:.8f} (ATR×{ATR_TP1_MULT})")
            logger.info(f"   TP2={tp2_price:.8f} (ATR×{ATR_TP2_MULT})")
            logger.info(f"   TP3={tp3_price:.8f} (ATR×{ATR_TP3_MULT})")
        
        tp1_quantity = total_quantity * TP1_CLOSE_PERCENT
        tp2_quantity = total_quantity * TP2_CLOSE_PERCENT
        tp3_quantity = total_quantity * TP3_CLOSE_PERCENT
        trailing_quantity = total_quantity * TRAILING_PERCENT
        
        state = load_state()
        if symbol in state.get("open_trades", {}):
            state["open_trades"][symbol]["tp_targets"] = {
                "tp1": {"price": tp1_price, "hit": False, "level": 1, "quantity": tp1_quantity, "closed": False},
                "tp2": {"price": tp2_price, "hit": False, "level": 2, "quantity": tp2_quantity, "closed": False}, 
                "tp3": {"price": tp3_price, "hit": False, "level": 3, "quantity": tp3_quantity, "closed": False}
            }
            
            state["open_trades"][symbol]["remaining_quantity"] = total_quantity
            state["open_trades"][symbol]["trailing_quantity"] = trailing_quantity
            
            initial_sl = calculate_proper_sl(symbol, entry_price, side, df)
            state["open_trades"][symbol]["sl"] = initial_sl
            state["open_trades"][symbol]["tp1"] = tp1_price
            state["open_trades"][symbol]["tp2"] = tp2_price
            state["open_trades"][symbol]["tp3"] = tp3_price
            
            state["open_trades"][symbol]["trailing_active"] = False
            state["open_trades"][symbol]["trailing_triggered"] = False
            state["open_trades"][symbol]["highest_price"] = entry_price if side == "long" else entry_price
            state["open_trades"][symbol]["lowest_price"] = entry_price if side == "short" else entry_price
            state["open_trades"][symbol]["trailing_distance_percent"] = TRAILING_DISTANCE_PERCENT
            
            save_state(state)
            logger.info(f" Multi-TP set for {symbol} (ATR-based, Pine match)")
            logger.info(f"   SL: {initial_sl:.8f} (ATR×{ATR_SL_MULT})")
            logger.info(f"   TP1: {tp1_price:.8f} (ATR×{ATR_TP1_MULT}) - {TP1_CLOSE_PERCENT*100}%")
            logger.info(f"   TP2: {tp2_price:.8f} (ATR×{ATR_TP2_MULT}) - {TP2_CLOSE_PERCENT*100}%")
            logger.info(f"   TP3: {tp3_price:.8f} (ATR×{ATR_TP3_MULT}) - {TP3_CLOSE_PERCENT*100}%")
        
        return tp1_price, tp2_price, tp3_price
    except Exception as e:
        logger.error(f"Error setting multi-TP: {e}")
        logger.error(traceback.format_exc())
        return None, None, None

# ============================================================================
# TP TRIGGER FUNCTION WITH PARTIAL CLOSE
# ============================================================================

def check_tp_targets_with_partial_close(symbol, current_price, trade_info):
    """Check TP targets with improved price comparison and logging"""
    try:
        side = trade_info.get("side", "long")
        entry_price = float(trade_info.get("entry_price", 0))
        tp_targets = trade_info.get("tp_targets", {})
        remaining_quantity = float(trade_info.get("remaining_quantity", 0))
        
        state = load_state()
        if symbol not in state.get("open_trades", {}):
            return False
        
        trade_data = state["open_trades"][symbol]
        updated = False
        
        epsilon = 0.00000001
        
        logger.debug(f"🔍 Checking TP for {symbol} {side} - Current: {current_price:.8f}")
        
        # Check TP1
        if not tp_targets.get("tp1", {}).get("hit", False):
            tp1_price = float(tp_targets["tp1"]["price"])
            logger.debug(f"   TP1 Price: {tp1_price:.8f}")
            
            tp1_hit = (side == "long" and current_price >= tp1_price - epsilon) or \
                      (side == "short" and current_price <= tp1_price + epsilon)
            
            if tp1_hit:
                tp1_quantity = float(tp_targets["tp1"]["quantity"])
                close_side = "sell" if side == "long" else "buy"
                
                logger.info(f" TP1 Hit for {symbol}! Price: {current_price:.8f} (Target: {tp1_price:.8f})")
                logger.info(f"   Closing {tp1_quantity:.6f} ({TP1_CLOSE_PERCENT*100}%) as {close_side.upper()}")
                
                if execute_trade_with_validation(close_side, symbol, tp1_quantity, current_price):
                    if side == "long":
                        tp1_profit = (current_price - entry_price) * tp1_quantity
                    else:
                        tp1_profit = (entry_price - current_price) * tp1_quantity
                    
                    trade_data["tp_targets"]["tp1"]["hit"] = True
                    trade_data["tp_targets"]["tp1"]["closed"] = True
                    trade_data["remaining_quantity"] = remaining_quantity - tp1_quantity
                    trade_data["sl"] = entry_price
                    
                    current_partial = float(trade_data.get("partial_profit", 0))
                    trade_data["partial_profit"] = current_partial + tp1_profit
                    updated = True
                    
                    log_partial_close(symbol, side, entry_price, current_price, tp1_quantity, 
                                    trade_data.get("trade_num", 0), "TP1", tp1_profit)
                    
                    logger.info(f"💰 Partial Profit: {tp1_profit:.2f} USDT - SL moved to break-even ({entry_price:.8f})")
                else:
                    logger.error(f"❌ TP1 execution FAILED for {symbol}")
        
        # Check TP2 (only if TP1 hit)
        elif not tp_targets.get("tp2", {}).get("hit", False) and tp_targets.get("tp1", {}).get("hit", False):
            tp2_price = float(tp_targets["tp2"]["price"])
            logger.debug(f"   TP2 Price: {tp2_price:.8f}")
            
            tp2_hit = (side == "long" and current_price >= tp2_price - epsilon) or \
                      (side == "short" and current_price <= tp2_price + epsilon)
            
            if tp2_hit:
                tp2_quantity = float(tp_targets["tp2"]["quantity"])
                close_side = "sell" if side == "long" else "buy"
                
                logger.info(f" TP2 Hit for {symbol}! Price: {current_price:.8f} (Target: {tp2_price:.8f})")
                logger.info(f"   Closing {tp2_quantity:.6f} ({TP2_CLOSE_PERCENT*100}%) as {close_side.upper()}")
                
                if execute_trade_with_validation(close_side, symbol, tp2_quantity, current_price):
                    if side == "long":
                        tp2_profit = (current_price - entry_price) * tp2_quantity
                    else:
                        tp2_profit = (entry_price - current_price) * tp2_quantity
                    
                    trade_data["tp_targets"]["tp2"]["hit"] = True
                    trade_data["tp_targets"]["tp2"]["closed"] = True
                    trade_data["remaining_quantity"] = remaining_quantity - tp2_quantity
                    tp1_price = float(tp_targets["tp1"]["price"])
                    trade_data["sl"] = tp1_price
                    
                    current_partial = float(trade_data.get("partial_profit", 0))
                    trade_data["partial_profit"] = current_partial + tp2_profit
                    updated = True
                    
                    log_partial_close(symbol, side, entry_price, current_price, tp2_quantity,
                                    trade_data.get("trade_num", 0), "TP2", tp2_profit)
                    
                    logger.info(f" Additional Profit: {tp2_profit:.2f} USDT - SL moved to TP1 ({tp1_price:.8f})")
                else:
                    logger.error(f"❌ TP2 execution FAILED for {symbol}")
        
        # Check TP3 (only if TP2 hit)
        elif not tp_targets.get("tp3", {}).get("hit", False) and tp_targets.get("tp2", {}).get("hit", False):
            tp3_price = float(tp_targets["tp3"]["price"])
            logger.debug(f"   TP3 Price: {tp3_price:.8f}")
            
            tp3_hit = (side == "long" and current_price >= tp3_price - epsilon) or \
                      (side == "short" and current_price <= tp3_price + epsilon)
            
            if tp3_hit:
                tp3_quantity = float(tp_targets["tp3"]["quantity"])
                close_side = "sell" if side == "long" else "buy"
                
                logger.info(f" TP3 Hit for {symbol}! Price: {current_price:.8f} (Target: {tp3_price:.8f})")
                logger.info(f"   Closing {tp3_quantity:.6f} ({TP3_CLOSE_PERCENT*100}%) as {close_side.upper()}")
                
                if execute_trade_with_validation(close_side, symbol, tp3_quantity, current_price):
                    if side == "long":
                        tp3_profit = (current_price - entry_price) * tp3_quantity
                    else:
                        tp3_profit = (entry_price - current_price) * tp3_quantity
                    
                    trade_data["tp_targets"]["tp3"]["hit"] = True
                    trade_data["tp_targets"]["tp3"]["closed"] = True
                    trade_data["remaining_quantity"] = remaining_quantity - tp3_quantity
                    tp2_price = float(tp_targets["tp2"]["price"])
                    trade_data["sl"] = tp2_price
                    trade_data["trailing_active"] = True
                    
                    current_partial = float(trade_data.get("partial_profit", 0))
                    trade_data["partial_profit"] = current_partial + tp3_profit
                    updated = True
                    
                    log_partial_close(symbol, side, entry_price, current_price, tp3_quantity,
                                    trade_data.get("trade_num", 0), "TP3", tp3_profit)
                    
                    logger.info(f" Additional Profit: {tp3_profit:.2f} USDT")
                    logger.info(f" Trailing stop ACTIVATED for remaining {trade_data['remaining_quantity']:.6f} {symbol}")
                else:
                    logger.error(f" TP3 execution FAILED for {symbol}")
        
        if updated:
            save_state(state)
            logger.info(f" TP update saved for {symbol}")
        
        return updated
        
    except Exception as e:
        logger.error(f"Error checking TP targets with partial close: {e}")
        logger.error(traceback.format_exc())
        return False

def log_partial_close(symbol, side, entry_price, exit_price, quantity, trade_num, tp_level, profit):
    try:
        row = {
            "Trade #": trade_num,
            "Symbol": symbol.replace('USDT',''),
            "Side": side.upper(),
            "Type": f"{symbol} {side.upper()} - {tp_level}",
            "Date/Time": datetime.now().strftime("%b %d, %Y, %H:%M"),
            "Signal": f"Partial Close ({tp_level})",
            "Price": f"{exit_price:.8f}",
            "Position size": f"{quantity:.6f} ({round(quantity*exit_price,2):.2f} USDT)",
            "Net P&L": f"{profit:+.2f} USDT",
            "Run-up": "0",
            "Drawdown": "0",
            "Cumulative P&L": ""
        }
        
        success = append_trade_row(row)
        if success:
            logger.info(f"Logged PARTIAL CLOSE for {symbol} {tp_level}: {profit:+.2f} USDT")
        else:
            logger.error(f"Failed to log partial close for {symbol}")
    except Exception as e:
        logger.error(f"Error logging partial close: {e}")

def update_trailing_stop(symbol, current_price, trade_info):
    try:
        if not trade_info.get("trailing_active", False):
            return False
        
        side = trade_info.get("side", "long")
        trailing_distance_percent = trade_info.get("trailing_distance_percent", TRAILING_DISTANCE_PERCENT)
        highest_price = float(trade_info.get("highest_price", current_price))
        lowest_price = float(trade_info.get("lowest_price", current_price))
        
        state = load_state()
        if symbol not in state.get("open_trades", {}):
            return False
        
        trade_data = state["open_trades"][symbol]
        updated = False
        
        if side == "long":
            if current_price > highest_price:
                trade_data["highest_price"] = current_price
                updated = True
                highest_price = current_price
            
            new_trailing_stop = highest_price * (1 - trailing_distance_percent)
            current_sl = float(trade_data.get("sl", 0))
            
            if new_trailing_stop > current_sl:
                trade_data["sl"] = new_trailing_stop
                updated = True
                logger.info(f" Trailing SL updated for {symbol}: {new_trailing_stop:.8f} (Current: {current_price:.8f})")
        else:
            if current_price < lowest_price:
                trade_data["lowest_price"] = current_price
                updated = True
                lowest_price = current_price
            
            new_trailing_stop = lowest_price * (1 + trailing_distance_percent)
            current_sl = float(trade_data.get("sl", 0))
            
            if new_trailing_stop < current_sl:
                trade_data["sl"] = new_trailing_stop
                updated = True
                logger.info(f" Trailing SL updated for {symbol}: {new_trailing_stop:.8f} (Current: {current_price:.8f})")
        
        if updated:
            save_state(state)
        
        return updated
    except Exception as e:
        logger.error(f"Error updating trailing stop: {e}")
        return False

# ============================================================================
# PINE MATCH: CHECK SL/TP WITH IMPROVED PRECISION
# ============================================================================

def check_sl_tp(symbol, current_price, trade_info):
    try:
        sl = trade_info.get("sl")
        remaining_quantity = float(trade_info.get("remaining_quantity", 0))
        
        if not sl:
            return False
        
        sl_price = float(sl)
        side = trade_info.get("side", "long")
        
        epsilon = 0.00000001
        
        sl_hit = False
        if side == "long" and current_price <= sl_price + epsilon:
            sl_hit = True
            logger.info(f" LONG SL CONDITION MET: Current={current_price:.8f} <= SL={sl_price:.8f}")
        elif side == "short" and current_price >= sl_price - epsilon:
            sl_hit = True
            logger.info(f" SHORT SL CONDITION MET: Current={current_price:.8f} >= SL={sl_price:.8f}")
        
        if sl_hit and remaining_quantity > 0:
            logger.info(f" SL EXECUTING for {symbol} {side.upper()} at {current_price:.8f} (SL: {sl_price:.8f})")
            
            if execute_trade_with_validation("sell" if side == "long" else "buy", 
                                           symbol, remaining_quantity, current_price):
                if side == "long":
                    pnl = (current_price - trade_info.get("entry_price", 0)) * remaining_quantity
                else:
                    pnl = (trade_info.get("entry_price", 0) - current_price) * remaining_quantity
                risk_manager.update_after_trade(pnl)
                logger.info(f" SL executed for {symbol}, PnL: {pnl:.2f} USDT")
                return "SL"
            else:
                logger.error(f"❌ SL execution FAILED for {symbol}")
        
        tp_hit = check_tp_targets_with_partial_close(symbol, current_price, trade_info)
        if tp_hit:
            return "TP_TARGET"
        
        if trade_info.get("trailing_active", False):
            update_trailing_stop(symbol, current_price, trade_info)
        
        return False
    except Exception as e:
        logger.error(f"❌ Error checking SL/TP for {symbol}: {e}")
        return False

@safe_execute(default_return=1)
def log_open(symbol, side, price, qty):
    try:
        trade_num = next_trade_number()
        row = {
            "Trade #": trade_num,
            "Symbol": symbol.replace('USDT',''),
            "Side": side.upper(),
            "Type": f"{symbol} {side.upper()}",
            "Date/Time": datetime.now().strftime("%b %d, %Y, %H:%M"),
            "Signal": "Entry",
            "Price": f"{price:.8f}",
            "Position size": f"{TRADE_USDT:.2f} USDT",
            "Net P&L": "",
            "Run-up": "",
            "Drawdown": "",
            "Cumulative P&L": ""
        }
        success = append_trade_row(row)
        if success:
            logger.info(f"Logged OPEN trade #{trade_num} for {symbol} {side} at {price:.8f}")
        else:
            logger.error(f"Failed to log OPEN trade for {symbol}")
        return trade_num
    except Exception as e:
        logger.error(f"Error in log_open for {symbol}: {e}")
        return 1

def _update_stats_from_pnl(side, pnl):
    try:
        st = load_state()
        side = side.lower()
        if "stats" not in st:
            st["stats"] = {
                "long": {"total": 0, "success": 0, "fail": 0},
                "short": {"total": 0, "success": 0, "fail": 0}
            }
        if side not in ("long","short"):
            side = "long" if str(side).lower().startswith("l") else "short"
        
        st["stats"].setdefault(side, {"total":0,"success":0,"fail":0})
        st["stats"][side]["total"] = st["stats"][side].get("total",0) + 1
        
        if pnl is not None and pnl > 0:
            st["stats"][side]["success"] = st["stats"][side].get("success",0) + 1
            logger.debug(f"Updated stats: {side} success")
        else:
            st["stats"][side]["fail"] = st["stats"][side].get("fail",0) + 1
            logger.debug(f"Updated stats: {side} fail")
        
        save_state(st)
    except Exception as e:
        logger.error(f"Error updating stats: {e}")

@safe_execute(default_return=(0.0, 0.0))
def log_close(symbol, side, entry_price, exit_price, qty, trade_num, reason="Manual"):
    try:
        if side == "long":
            pnl = (exit_price - entry_price) * qty
        else:
            pnl = (entry_price - exit_price) * qty
            
        pnl = round(pnl, 2)
        
        max_realistic_pnl = TRADE_USDT * 5
        if abs(pnl) > max_realistic_pnl:
            logger.warning(f"⚠️ Suspicious P&L for {symbol}: {pnl} USDT (trade: {TRADE_USDT} USDT)")
            pnl = max_realistic_pnl if pnl > 0 else -max_realistic_pnl
        
        risk_manager.update_after_trade(pnl)
        
        df = safe_read_trades()
        try:
            existing = pd.to_numeric(df["Net P&L"].str.replace("[^0-9.-]","", regex=True), errors="coerce").dropna()
            cumulative = existing.sum() + pnl if not existing.empty else pnl
        except Exception:
            cumulative = pnl
            
        row = {
            "Trade #": trade_num,
            "Symbol": symbol.replace('USDT',''),
            "Side": side.upper(),
            "Type": f"{symbol} {side.upper()}",
            "Date/Time": datetime.now().strftime("%b %d, %Y, %H:%M"),
            "Signal": f"Exit ({reason})",
            "Price": f"{exit_price:.8f}",
            "Position size": f"{qty:.6f} ({round(qty*exit_price,2):.2f} USDT)",
            "Net P&L": f"{pnl:+.2f} USDT",
            "Run-up": "0",
            "Drawdown": "0",
            "Cumulative P&L": f"{cumulative:.2f}"
        }
        
        success = append_trade_row(row)
        if success:
            logger.info(f" Logged CLOSE trade #{trade_num} for {symbol} {side} at {exit_price:.8f}, PnL: {pnl} ({reason})")
            
            try:
                _update_stats_from_pnl(side, pnl)
            except Exception as e:
                logger.error(f"Error updating stats for trade close: {e}")
        else:
            logger.error(f"❌ Failed to log CLOSE trade for {symbol}")

        return pnl, cumulative
    except Exception as e:
        logger.error(f"❌ Error in log_close for {symbol}: {e}")
        return 0.0, 0.0

def calculate_quantity(price):
    try:
        if price <= 0:
            return 0
        quantity = TRADE_USDT / price
        logger.debug(f"Calculated quantity: {quantity} for price {price}")
        return quantity
    except Exception as e:
        logger.error(f"Error calculating quantity: {e}")
        return 0

# ============================================================================
# PINE MATCH: CHECK TRADING SIGNAL (USES ONLY CLOSED CANDLES)
# ============================================================================

def check_trading_signal(df, symbol, current_price):
    """
    PINE STRATEGY EXACT MATCH: 
    - Uses ONLY closed candles (index -2, not -1)
    - Detects ONLY crossovers that happen AFTER bot start
    - Entry: EMA crossover + RSI + Volume + ADX
    
    LONG:  EMAfast > EMAslow AND crossover(close, EMAfast) AND RSI >= rsi_long_level AND volume > SMA(volume) AND ADX >= threshold
    SHORT: EMAfast < EMAslow AND crossunder(close, EMAfast) AND RSI <= rsi_short_level AND volume > SMA(volume) AND ADX >= threshold
    """
    try:
        # Need enough closed candles
        if len(df) < EMA_SLOW + 10:
            logger.debug(f"Insufficient data for {symbol} (need {EMA_SLOW}+ bars)")
            return "HOLD"
        
        # ========== USE ONLY CLOSED CANDLES (index -2 is last closed candle) ==========
        if len(df) < 2:
            return "HOLD"
        
        # Get the last CLOSED candle data (index -2)
        closed_candle_idx = -2
        candle_close = df['close'].iloc[closed_candle_idx]
        candle_high = df['high'].iloc[closed_candle_idx]
        candle_low = df['low'].iloc[closed_candle_idx]
        candle_volume = df['volume'].iloc[closed_candle_idx]
        
        # Get candle close time for freshness check
        if 'close_time' in df.columns:
            candle_close_time_ms = df['close_time'].iloc[closed_candle_idx]
            candle_close_time = datetime.fromtimestamp(candle_close_time_ms / 1000)
        else:
            candle_close_time = datetime.now()
        
        # ========== FRESH SIGNAL CHECK: Only trade candles AFTER bot started ==========
        if candle_close_time < BOT_START_TIME:
            logger.info(f" {symbol} - Signal from OLD candle ({candle_close_time.strftime('%Y-%m-%d %H:%M:%S')}), ignoring (Bot started at {BOT_START_TIME.strftime('%Y-%m-%d %H:%M:%S')})")
            return "HOLD"
        
        # ========== SIDEWAYS MARKET DETECTION ==========
        if SIDEWAYS_ENABLED and is_sideways_market(df):
            logger.info(f" {symbol} - Market is sideways/choppy, skipping trade")
            return "HOLD"
        
        # ========== CALCULATE INDICATORS using ALL closed candles ==========
        # Use df.iloc[:-1] to exclude the current incomplete candle
        df_closed = df.iloc[:-1].copy() if len(df) > 1 else df.copy()
        
        # EMA calculations
        ema_fast = df_closed['close'].ewm(span=EMA_FAST, adjust=False).mean()
        ema_slow = df_closed['close'].ewm(span=EMA_SLOW, adjust=False).mean()
        
        # RSI
        rsi_val = rsi(df_closed['close'], length=RSI_LEN)
        
        # Volume SMA
        vol_sma = df_closed['volume'].rolling(window=VOLUME_PERIOD).mean()
        
        # Manual ADX (Pine style with Wilder smoothing)
        adx_series = calculate_pine_adx(df_closed, ADX_LEN)
        
        # ========== CURRENT VALUES (from last closed candle) ==========
        ema_fast_current = ema_fast.iloc[-1]
        ema_slow_current = ema_slow.iloc[-1]
        rsi_current = rsi_val.iloc[-1]
        vol_sma_current = vol_sma.iloc[-1]
        adx_current = adx_series.iloc[-1]
        
        # Previous values for crossover detection (from candle before last closed)
        if len(df_closed) >= 2:
            close_prev = df_closed['close'].iloc[-2]
            ema_fast_prev = ema_fast.iloc[-2]
        else:
            close_prev = candle_close
            ema_fast_prev = ema_fast_current
        
        # ========== PINE CONDITIONS ==========
        # Trend filters
        trend_long = ema_fast_current > ema_slow_current
        trend_short = ema_fast_current < ema_slow_current
        
        # Crossover detection (EXACT Pine match)
        # Pine: ta.crossover(close, emaFast) = close[1] <= emaFast[1] AND close > emaFast
        crossover_bullish = (close_prev <= ema_fast_prev and candle_close > ema_fast_current)
        
        # Crossunder detection (EXACT Pine match)
        # Pine: ta.crossunder(close, emaFast) = close[1] >= emaFast[1] AND close < emaFast
        crossover_bearish = (close_prev >= ema_fast_prev and candle_close < ema_fast_current)
        
        # Volume condition
        vol_ok = candle_volume > vol_sma_current
        
        # RSI conditions (using config values)
        rsi_long_ok = rsi_current >= RSI_OVERBOUGHT
        rsi_short_ok = rsi_current <= RSI_OVERSOLD
        
        # ADX condition
        adx_ok = adx_current >= ADX_THR
        
        # ========== DETAILED LOGGING ==========
        logger.info(f" {symbol} - PINE SIGNAL CHECK (Candle: {candle_close_time.strftime('%Y-%m-%d %H:%M:%S')})")
        logger.info(f"   Price: {candle_close:.8f}, EMA{EMA_FAST}: {ema_fast_current:.8f}, EMA{EMA_SLOW}: {ema_slow_current:.8f}")
        logger.info(f"   RSI: {rsi_current:.1f} (Long>={RSI_OVERBOUGHT}: {rsi_long_ok}, Short<={RSI_OVERSOLD}: {rsi_short_ok})")
        logger.info(f"   ADX: {adx_current:.1f} (>={ADX_THR}: {adx_ok})")
        logger.info(f"   Volume: {candle_volume:.0f} > SMA{VOLUME_PERIOD}: {vol_sma_current:.0f} = {vol_ok}")
        logger.info(f"   Trend: {'LONG' if trend_long else 'SHORT' if trend_short else 'NEUTRAL'}")
        logger.info(f"   Crossover: Bullish={crossover_bullish}, Bearish={crossover_bearish}")
        
        # ========== PINE SIGNAL LOGIC ==========
        long_trigger = trend_long and crossover_bullish and rsi_long_ok and vol_ok and adx_ok
        short_trigger = trend_short and crossover_bearish and rsi_short_ok and vol_ok and adx_ok
        
        # ========== FINAL SIGNAL ==========
        if long_trigger:
            logger.info(f" LONG SIGNAL for {symbol} - ALL CONDITIONS PASSED (FRESH CLOSED CANDLE)")
            logger.info(f"    EMA{EMA_FAST} > EMA{EMA_SLOW}: {ema_fast_current:.2f} > {ema_slow_current:.2f}")
            logger.info(f"    CROSSOVER: {close_prev:.2f} <= {ema_fast_prev:.2f} and {candle_close:.2f} > {ema_fast_current:.2f}")
            logger.info(f"    RSI >= {RSI_OVERBOUGHT}: {rsi_current:.1f}")
            logger.info(f"    Volume > SMA{VOLUME_PERIOD}: {candle_volume:.0f} > {vol_sma_current:.0f}")
            logger.info(f"    ADX >= {ADX_THR}: {adx_current:.1f}")
            return "BUY"
        
        elif short_trigger:
            logger.info(f" SHORT SIGNAL for {symbol} - ALL CONDITIONS PASSED (FRESH CLOSED CANDLE)")
            logger.info(f"    EMA{EMA_FAST} < EMA{EMA_SLOW}: {ema_fast_current:.2f} < {ema_slow_current:.2f}")
            logger.info(f"    CROSSUNDER: {close_prev:.2f} >= {ema_fast_prev:.2f} and {candle_close:.2f} < {ema_fast_current:.2f}")
            logger.info(f"    RSI <= {RSI_OVERSOLD}: {rsi_current:.1f}")
            logger.info(f"    Volume > SMA{VOLUME_PERIOD}: {candle_volume:.0f} > {vol_sma_current:.0f}")
            logger.info(f"    ADX >= {ADX_THR}: {adx_current:.1f}")
            return "SELL"
        
        return "HOLD"
        
    except Exception as e:
        logger.error(f"Error in check_trading_signal for {symbol}: {e}")
        logger.error(traceback.format_exc())
        return "HOLD"

def has_open_trade_for_symbol(symbol):
    """Check if symbol already has an open trade with remaining quantity"""
    try:
        state = load_state()
        open_trades = state.get("open_trades", {})
        
        if symbol in open_trades:
            trade = open_trades[symbol]
            remaining_qty = float(trade.get("remaining_quantity", 0))
            if remaining_qty > 0:
                return True, trade.get("side", "unknown")
        return False, None
    except Exception as e:
        logger.error(f"Error checking open trade for {symbol}: {e}")
        return False, None

def manage_open_trades(symbol, current_price, signal):
    try:
        state = load_state()
        open_trades = state.get("open_trades", {})
        
        if symbol in open_trades:
            trade = open_trades[symbol]
            entry_price = float(trade.get("entry_price", 0))
            side = trade.get("side", "")
            trade_num = trade.get("trade_num", 0)
            remaining_quantity = float(trade.get("remaining_quantity", 0))
            
            if remaining_quantity <= 0:
                logger.warning(f"⚠️ {symbol} has zero remaining quantity, removing from open trades")
                del open_trades[symbol]
                save_state(state)
                return
            
            sl_tp_result = check_sl_tp(symbol, current_price, trade)
            if sl_tp_result:
                if sl_tp_result == "SL" and remaining_quantity > 0:
                    logger.info(f"Closing remaining {remaining_quantity:.6f} {symbol} due to SL")
                    execute_trade_with_validation("sell" if side == "long" else "buy", symbol, remaining_quantity, current_price)
                    log_close(symbol, side, entry_price, current_price, remaining_quantity, trade_num, "SL")
                    del open_trades[symbol]
                    save_state(state)
                return
            
            if trade.get("trailing_active", False):
                update_trailing_stop(symbol, current_price, trade)
            
            if side == "long":
                if signal == "SELL" and remaining_quantity > 0:
                    logger.info(f"Exiting remaining LONG position for {symbol} at {current_price} (Signal Change)")
                    execute_trade_with_validation("sell", symbol, remaining_quantity, current_price)
                    log_close(symbol, "long", entry_price, current_price, remaining_quantity, trade_num, "Signal")
                    del open_trades[symbol]
                    save_state(state)
            elif side == "short":
                if signal == "BUY" and remaining_quantity > 0:
                    logger.info(f"Exiting remaining SHORT position for {symbol} at {current_price} (Signal Change)")
                    execute_trade_with_validation("buy", symbol, remaining_quantity, current_price)
                    log_close(symbol, "short", entry_price, current_price, remaining_quantity, trade_num, "Signal")
                    del open_trades[symbol]
                    save_state(state)
    except Exception as e:
        logger.error(f"Error managing open trades for {symbol}: {e}")

def strategy_loop(symbol):
    logger.info(f" Starting PINE STRATEGY bot for {symbol}")
    logger.info(f" Bot started at: {BOT_START_TIME.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f" Using 1h timeframe - Will only trade signals from closed candles AFTER this time")
    logger.info(f" Pine Settings: EMA{EMA_FAST}/{EMA_SLOW}, RSI{RSI_LEN}({RSI_OVERBOUGHT}/{RSI_OVERSOLD}), ADX{ADX_LEN}({ADX_THR})")
    
    consecutive_count = 0
    last_trade_time = None
    last_signal = "HOLD"
    
    while True:
        try:
            current_time = time.time()
            
            if last_trade_time and (current_time - last_trade_time) < TRADE_COOLDOWN:
                time.sleep(CHECK_INTERVAL)
                continue
            
            # Get 1h klines with 250 bars for EMA200
            df = get_klines(symbol, '1h', 250)
            if df.empty or len(df) < 30:
                logger.warning(f"⚠️ Insufficient data for {symbol}, skipping...")
                time.sleep(CHECK_INTERVAL)
                continue
            
            # Skip if we don't have at least 2 candles (need closed candle)
            if len(df) < 2:
                logger.debug(f"⏳ {symbol} - Need at least 2 candles, have {len(df)}")
                time.sleep(CHECK_INTERVAL)
                continue
            
            # Get current price
            current_price = get_validated_price(symbol)
            if current_price is None:
                logger.warning(f"⚠️ Could not get price for {symbol}, skipping...")
                time.sleep(CHECK_INTERVAL)
                continue
            
            logger.info(f" {symbol} ACTUAL Price: {current_price:.8f}")
            
            # Check signal using ONLY closed candles
            signal = check_trading_signal(df, symbol, current_price)
            
            # Manage existing trades
            manage_open_trades(symbol, current_price, signal)
            
            state = load_state()
            open_trades = state.get("open_trades", {})
            
            if symbol not in open_trades:
                # Signal confirmation logic
                if signal != last_signal:
                    if signal != "HOLD":
                        consecutive_count = 1
                    else:
                        consecutive_count = 0
                    last_signal = signal
                    if signal != "HOLD":
                        logger.info(f"🔄 New signal for {symbol}: {signal}, starting confirmation count")
                else:
                    if signal != "HOLD":
                        consecutive_count += 1
                        logger.info(f" Signal confirmation {consecutive_count}/{CONFIRMATION_REQUIRED} for {symbol} ({signal})")
                    else:
                        consecutive_count = 0
                
                if consecutive_count >= CONFIRMATION_REQUIRED and signal != "HOLD":
                    state_check = load_state()
                    if symbol in state_check.get("open_trades", {}):
                        logger.warning(f"⚠️ {symbol} trade appeared between checks, skipping entry")
                        consecutive_count = 0
                        time.sleep(CHECK_INTERVAL)
                        continue
                    
                    total_quantity = calculate_quantity(current_price)
                    if total_quantity > 0:
                        logger.info(f" ENTERING {signal} trade for {symbol} after {consecutive_count} confirmations")
                        
                        execute_success = execute_trade_with_validation("buy" if signal == "BUY" else "sell", 
                                                       symbol, total_quantity, current_price)
                        
                        if execute_success:
                            trade_num = log_open(symbol, "long" if signal == "BUY" else "short", 
                                               current_price, total_quantity)
                            
                            tp1, tp2, tp3 = set_multi_tp_profit_distribution(symbol, current_price, 
                                                                            "long" if signal == "BUY" else "short", 
                                                                            df, total_quantity)
                            
                            initial_sl = calculate_proper_sl(symbol, current_price, 
                                                           "long" if signal == "BUY" else "short", df)
                            
                            final_state = load_state()
                            if "open_trades" not in final_state:
                                final_state["open_trades"] = {}
                            
                            final_state["open_trades"][symbol] = {
                                "entry_price": current_price,
                                "side": "long" if signal == "BUY" else "short",
                                "total_quantity": total_quantity,
                                "remaining_quantity": total_quantity,
                                "trade_num": trade_num,
                                "entry_time": datetime.now().isoformat(),
                                "signal": signal,
                                "sl": initial_sl,
                                "tp1": tp1,
                                "tp2": tp2,
                                "tp3": tp3,
                                "tp_targets": {
                                    "tp1": {"price": tp1, "hit": False, "level": 1, "quantity": total_quantity * TP1_CLOSE_PERCENT, "closed": False},
                                    "tp2": {"price": tp2, "hit": False, "level": 2, "quantity": total_quantity * TP2_CLOSE_PERCENT, "closed": False},
                                    "tp3": {"price": tp3, "hit": False, "level": 3, "quantity": total_quantity * TP3_CLOSE_PERCENT, "closed": False}
                                },
                                "trailing_active": False,
                                "trailing_triggered": False,
                                "highest_price": current_price if signal == "BUY" else current_price,
                                "lowest_price": current_price if signal == "SELL" else current_price,
                                "trailing_distance_percent": TRAILING_DISTANCE_PERCENT,
                                "trailing_quantity": total_quantity * TRAILING_PERCENT,
                                "partial_profit": 0.0
                            }
                            save_state(final_state)
                            
                            consecutive_count = 0
                            last_trade_time = current_time
                            last_signal = "HOLD"
                            logger.info(f" Cooldown period started for {symbol}")
                        else:
                            logger.error(f"❌ Failed to execute trade for {symbol}")
                            consecutive_count = 0
            else:
                if signal != "HOLD":
                    logger.info(f" {symbol} has open {open_trades[symbol].get('side', 'unknown')} trade, ignoring {signal} signal")
                consecutive_count = 0
            
            logger.info(f" {symbol} - Signal: {signal}, Confirmations: {consecutive_count}/{CONFIRMATION_REQUIRED}, Open Trade: {'Yes' if symbol in open_trades else 'No'}")
                
        except Exception as e:
            logger.error(f"❌ Error in strategy_loop for {symbol}: {e}")
        
        time.sleep(CHECK_INTERVAL)

# ============================================================================
# ENTRY POINT FOR RAILWAY - ADDED WITHOUT MODIFYING EXISTING CODE
# ============================================================================

def main():
    """Entry point for Railway deployment - calls your existing __main__ logic"""
    import argparse
    import sys
    
    # Parse arguments
    parser = argparse.ArgumentParser(description="Trading Bot - Institute Grade with PINE STRATEGY EXACT MATCH")
    parser.add_argument("--clear-history", action="store_true", help="Clear trades.csv and state.json then exit")
    parser.add_argument("--symbol", type=str, help="Run bot for a single symbol")
    parser.add_argument("--all", action="store_true", help="Run bot for all symbols in SYMBOLS list")
    parser.add_argument("--confirmations", type=int, help="Override CONFIRMATION_REQUIRED")
    parser.add_argument("--check", action="store_true", help="Run health checks and exit")
    parser.add_argument("--reset", action="store_true", help="Clear trade history and state")
    parser.add_argument("--dry-run", action="store_true", help="Force dry run mode")
    
    # If no args provided, default to --all for Railway
    if len(sys.argv) == 1:
        sys.argv.append("--all")
    
    args = parser.parse_args()
    
    # Set global variables
    global CONFIRMATION_REQUIRED, DRY_RUN
    
    if args.clear_history or args.reset:
        reset_history()
        reset_crossover_states()
        logger.info(" Trade history and EMA crossover states reset successfully")
        if args.clear_history:
            return
    
    if args.check:
        ensure_files()
        logger.info(" Health checks passed")
        cache_stats = data_cache.get_stats()
        session_stats = binance_session.get_stats()
        logger.info(f" Cache stats: {cache_stats}")
        logger.info(f" Session stats: {session_stats}")
        return
    
    if args.confirmations is not None:
        CONFIRMATION_REQUIRED = max(1, int(args.confirmations))
        logger.info(f" CONFIRMATION_REQUIRED set to {CONFIRMATION_REQUIRED} from CLI")
    
    if args.dry_run:
        DRY_RUN = True
        logger.info(" DRY_RUN set to True from CLI")
    
    # Determine symbols to run
    if args.symbol:
        symbols_to_run = [args.symbol]
    elif args.all:
        symbols_to_run = SYMBOLS
    else:
        symbols_to_run = SYMBOLS
        logger.info(f" Running for {len(symbols_to_run)} symbols")
    
    # Reset crossover states on startup
    reset_crossover_states()
    logger.info(f" Reset all crossover states - Bot will only trade FRESH signals")
    logger.info(f" Bot start time recorded: {BOT_START_TIME.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Initialize Binance client
    binance_connected = initialize_binance_client()
    
    if not binance_connected:
        logger.error("🚫 CRITICAL: Cannot connect to Binance. Please check API keys and internet connection.")
        return
    
    ensure_files()
    state = load_state()
    logger.info(f" Loaded state with {len(state.get('open_trades', {}))} open trades")
    
    # Start trading bots
    threads = []
    for symbol in symbols_to_run:
        t = threading.Thread(target=strategy_loop, args=(symbol,), daemon=True)
        t.start()
        threads.append(t)
        logger.info(f" Started bot for {symbol}")
        time.sleep(0.5)
    
    logger.info(f" Started {len(threads)} trading bots with PINE STRATEGY EXACT MATCH")
    
    # Start monitoring thread
    def monitor_loop():
        while True:
            try:
                performance_monitor.update_metrics()
                cache_stats = data_cache.get_stats()
                session_stats = binance_session.get_stats()
                logger.debug(f"📊 Cache: {cache_stats['hit_rate']} hit rate, {cache_stats['cache_size']} items")
                logger.debug(f"📊 API: {session_stats['total_requests']} total requests")
                time.sleep(60)
            except Exception as e:
                logger.error(f"Monitor error: {e}")
                time.sleep(60)
    
    monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
    monitor_thread.start()
    
    # Start FastAPI server if available
    if FASTAPI_AVAILABLE:
        logger.info(f" Starting FastAPI server on http://0.0.0.0:{PORT}")
        logger.info(f" Dashboard credentials: {DASHBOARD_USER} / {DASHBOARD_PASSWORD}")
        logger.info(" Trading Bot is now ACTIVE with PINE STRATEGY EXACT MATCH...")
        
        try:
            uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="error")
        except KeyboardInterrupt:
            logger.info(" Bot stopped by user (Ctrl+C)")
        except Exception as e:
            logger.error(f" API server error: {e}")
    else:
        logger.info(" FastAPI not available - running in console mode only")
        logger.info(" Trading Bot is now ACTIVE (console mode)...")
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info(" Bot stopped by user (Ctrl+C)")

# This is the entry point for Railway
if __name__ == "__main__":
    main()