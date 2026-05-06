"""
MarketPulse - Signals Routes
REST endpoints for querying signals from PostgreSQL.
"""

from fastapi import APIRouter, HTTPException, Query
import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv

load_dotenv()

router = APIRouter(prefix="/signals", tags=["Signals"])

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:password@localhost:5432/marketpulse")


def get_db():
    """Create a database connection."""
    return psycopg2.connect(DATABASE_URL)


@router.get("/")
def get_all_signals(limit: int = Query(default=50, le=200)):
    """
    Get latest signals for all symbols.
    Returns the most recent `limit` signals ordered by created_at DESC.
    """
    try:
        conn = get_db()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT symbol, close, rsi, macd, macd_signal, macd_crossover,
                       bb_upper, bb_lower, bb_position, volume_anomaly, created_at
                FROM signals
                ORDER BY created_at DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
        conn.close()
        return {"count": len(rows), "signals": [dict(r) for r in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/latest")
def get_latest_per_symbol():
    """
    Get the single most recent signal for each symbol.
    Useful for the dashboard summary cards.
    """
    try:
        conn = get_db()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT ON (symbol)
                    symbol, close, rsi, macd, macd_signal, macd_crossover,
                    bb_upper, bb_lower, bb_position, volume_anomaly, created_at
                FROM signals
                ORDER BY symbol, created_at DESC
            """)
            rows = cur.fetchall()
        conn.close()
        return {"signals": [dict(r) for r in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{symbol}")
def get_signals_by_symbol(symbol: str, limit: int = Query(default=50, le=200)):
    """
    Get signals for a specific symbol.
    Example: GET /signals/AAPL?limit=20
    """
    try:
        conn = get_db()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT symbol, close, rsi, macd, macd_signal, macd_crossover,
                       bb_upper, bb_lower, bb_position, volume_anomaly, created_at
                FROM signals
                WHERE symbol = %s
                ORDER BY created_at DESC
                LIMIT %s
            """, (symbol.upper(), limit))
            rows = cur.fetchall()
        conn.close()
        if not rows:
            raise HTTPException(status_code=404, detail=f"No signals found for {symbol.upper()}")
        return {"symbol": symbol.upper(), "count": len(rows), "signals": [dict(r) for r in rows]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))