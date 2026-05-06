"""
MarketPulse - Alerts Routes
REST endpoints for querying alerts — signals that crossed thresholds.
"""

from fastapi import APIRouter, HTTPException, Query
import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv

load_dotenv()

router = APIRouter(prefix="/alerts", tags=["Alerts"])

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:password@localhost:5432/marketpulse")

# Thresholds
RSI_OVERBOUGHT = 70
RSI_OVERSOLD   = 30


def get_db():
    """Create a database connection."""
    return psycopg2.connect(DATABASE_URL)


@router.get("/")
def get_alerts(limit: int = Query(default=50, le=200)):
    """
    Get signals that crossed alert thresholds:
    - RSI > 70 (overbought)
    - RSI < 30 (oversold)
    - MACD crossover detected
    - Volume anomaly detected
    """
    try:
        conn = get_db()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT symbol, close, rsi, macd, macd_crossover,
                       bb_position, volume_anomaly, created_at,
                       CASE
                           WHEN rsi > %s THEN 'RSI Overbought'
                           WHEN rsi < %s THEN 'RSI Oversold'
                           WHEN macd_crossover = 'bullish' THEN 'MACD Bullish Crossover'
                           WHEN macd_crossover = 'bearish' THEN 'MACD Bearish Crossover'
                           WHEN volume_anomaly = TRUE THEN 'Volume Spike'
                           ELSE 'Unknown'
                       END AS alert_type
                FROM signals
                WHERE rsi > %s
                   OR rsi < %s
                   OR macd_crossover IS NOT NULL
                   OR volume_anomaly = TRUE
                ORDER BY created_at DESC
                LIMIT %s
            """, (RSI_OVERBOUGHT, RSI_OVERSOLD, RSI_OVERBOUGHT, RSI_OVERSOLD, limit))
            rows = cur.fetchall()
        conn.close()
        return {"count": len(rows), "alerts": [dict(r) for r in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{symbol}")
def get_alerts_by_symbol(symbol: str, limit: int = Query(default=20, le=100)):
    """
    Get alerts for a specific symbol.
    Example: GET /alerts/AAPL
    """
    try:
        conn = get_db()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT symbol, close, rsi, macd, macd_crossover,
                       bb_position, volume_anomaly, created_at
                FROM signals
                WHERE symbol = %s
                  AND (rsi > %s OR rsi < %s OR macd_crossover IS NOT NULL OR volume_anomaly = TRUE)
                ORDER BY created_at DESC
                LIMIT %s
            """, (symbol.upper(), RSI_OVERBOUGHT, RSI_OVERSOLD, limit))
            rows = cur.fetchall()
        conn.close()
        return {"symbol": symbol.upper(), "count": len(rows), "alerts": [dict(r) for r in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))