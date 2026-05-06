"""
MarketPulse - Signal Engine
Computes RSI, MACD, and Bollinger Bands from OHLCV tick data.
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass


@dataclass
class Signal:
    symbol:           str
    timestamp:        str
    close:            float
    rsi:              float | None
    macd:             float | None
    macd_signal:      float | None
    macd_crossover:   str | None     # "bullish" | "bearish" | None
    bb_upper:         float | None
    bb_lower:         float | None
    bb_position:      str | None     # "above" | "below" | "inside"
    volume_anomaly:   bool


class SignalEngine:
    """
    Maintains a rolling window of ticks per symbol
    and computes technical indicators.
    """

    def __init__(self, window: int = 30):
        self.window = window
        self.history: dict[str, list[dict]] = {}

    def add_tick(self, tick: dict) -> Signal | None:
        """Add a new tick and return computed signals if enough data exists."""
        symbol = tick["symbol"]

        if symbol not in self.history:
            self.history[symbol] = []

        self.history[symbol].append(tick)

        # Keep only the rolling window
        if len(self.history[symbol]) > self.window:
            self.history[symbol] = self.history[symbol][-self.window:]

        # Need at least 26 ticks for MACD
        if len(self.history[symbol]) < 26:
            return None

        return self._compute(symbol)

    def _compute(self, symbol: str) -> Signal:
        """Compute all signals for a symbol."""
        df = pd.DataFrame(self.history[symbol])
        close = df["close"].astype(float)
        volume = df["volume"].astype(float)
        latest = df.iloc[-1]

        return Signal(
            symbol=symbol,
            timestamp=latest["timestamp"],
            close=float(latest["close"]),
            rsi=self._rsi(close),
            macd=self._macd(close)[0],
            macd_signal=self._macd(close)[1],
            macd_crossover=self._macd_crossover(close),
            bb_upper=self._bollinger(close)[0],
            bb_lower=self._bollinger(close)[1],
            bb_position=self._bb_position(close),
            volume_anomaly=self._volume_anomaly(volume),
        )

    # ─── Indicators ───────────────────────────────────────────

    def _rsi(self, close: pd.Series, period: int = 14) -> float | None:
        """Relative Strength Index (14-period)."""
        if len(close) < period:
            return None
        delta = close.diff()
        gain  = delta.clip(lower=0).rolling(period).mean()
        loss  = -delta.clip(upper=0).rolling(period).mean()
        rs    = gain / loss
        rsi   = 100 - (100 / (1 + rs))
        val   = rsi.iloc[-1]
        return round(float(val), 2) if not np.isnan(val) else None

    def _macd(self, close: pd.Series) -> tuple[float | None, float | None]:
        """MACD line and signal line (12/26/9)."""
        if len(close) < 26:
            return None, None
        ema12  = close.ewm(span=12, adjust=False).mean()
        ema26  = close.ewm(span=26, adjust=False).mean()
        macd   = ema12 - ema26
        signal = macd.ewm(span=9, adjust=False).mean()
        m = macd.iloc[-1]
        s = signal.iloc[-1]
        return (
            round(float(m), 4) if not np.isnan(m) else None,
            round(float(s), 4) if not np.isnan(s) else None,
        )

    def _macd_crossover(self, close: pd.Series) -> str | None:
        """Detect bullish or bearish MACD crossover."""
        if len(close) < 27:
            return None
        ema12  = close.ewm(span=12, adjust=False).mean()
        ema26  = close.ewm(span=26, adjust=False).mean()
        macd   = ema12 - ema26
        signal = macd.ewm(span=9, adjust=False).mean()
        prev_diff = macd.iloc[-2] - signal.iloc[-2]
        curr_diff = macd.iloc[-1] - signal.iloc[-1]
        if prev_diff < 0 and curr_diff > 0:
            return "bullish"
        if prev_diff > 0 and curr_diff < 0:
            return "bearish"
        return None

    def _bollinger(self, close: pd.Series, period: int = 20) -> tuple[float | None, float | None]:
        """Bollinger Bands upper and lower (20-period, 2σ)."""
        if len(close) < period:
            return None, None
        sma   = close.rolling(period).mean()
        std   = close.rolling(period).std()
        upper = (sma + 2 * std).iloc[-1]
        lower = (sma - 2 * std).iloc[-1]
        return (
            round(float(upper), 4) if not np.isnan(upper) else None,
            round(float(lower), 4) if not np.isnan(lower) else None,
        )

    def _bb_position(self, close: pd.Series, period: int = 20) -> str | None:
        """Position of latest close relative to Bollinger Bands."""
        upper, lower = self._bollinger(close, period)
        if upper is None or lower is None:
            return None
        price = float(close.iloc[-1])
        if price > upper:
            return "above"
        if price < lower:
            return "below"
        return "inside"

    def _volume_anomaly(self, volume: pd.Series, threshold: float = 2.0) -> bool:
        """Detect volume spike using z-score."""
        if len(volume) < 10:
            return False
        mean = volume.rolling(10).mean().iloc[-1]
        std  = volume.rolling(10).std().iloc[-1]
        if std == 0:
            return False
        z_score = (volume.iloc[-1] - mean) / std
        return float(z_score) > threshold
