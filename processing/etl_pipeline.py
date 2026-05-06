"""
MarketPulse - ETL Pipeline
Consumes raw-ticks from Kafka, runs signal engine,
stores results in PostgreSQL, publishes to enriched-signals topic.
Usage: python processing/etl_pipeline.py
"""

import json
import logging
import time
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaConnectionError
import psycopg2
import yfinance as yf

from signal_engine import SignalEngine, Signal

load_dotenv()

# ─── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [ETL]  %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_TOPIC       = "raw-ticks"
OUTPUT_TOPIC      = "enriched-signals"
GROUP_ID          = "marketpulse-etl-group"
DATABASE_URL      = os.getenv("DATABASE_URL", "postgresql://admin:password@localhost:5432/marketpulse")
SYMBOLS           = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]


# ─── Database ─────────────────────────────────────────────────

def get_db_connection():
    """Create PostgreSQL connection."""
    return psycopg2.connect(DATABASE_URL)


def create_tables(conn):
    """Create signals table if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id               SERIAL PRIMARY KEY,
                symbol           VARCHAR(10)   NOT NULL,
                timestamp        TIMESTAMPTZ   NOT NULL,
                close            NUMERIC(12,4),
                rsi              NUMERIC(8,2),
                macd             NUMERIC(12,4),
                macd_signal      NUMERIC(12,4),
                macd_crossover   VARCHAR(10),
                bb_upper         NUMERIC(12,4),
                bb_lower         NUMERIC(12,4),
                bb_position      VARCHAR(10),
                volume_anomaly   BOOLEAN,
                created_at       TIMESTAMPTZ   DEFAULT NOW()
            );
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_signals_symbol
            ON signals(symbol);
        """)
        conn.commit()
    log.info("Database tables ready.")


def save_signal(conn, signal: Signal):
    """Insert a signal into PostgreSQL."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO signals (
                symbol, timestamp, close, rsi, macd, macd_signal,
                macd_crossover, bb_upper, bb_lower, bb_position, volume_anomaly
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            signal.symbol,
            signal.timestamp,
            signal.close,
            signal.rsi,
            signal.macd,
            signal.macd_signal,
            signal.macd_crossover,
            signal.bb_upper,
            signal.bb_lower,
            signal.bb_position,
            signal.volume_anomaly,
        ))
        conn.commit()


# ─── Historical Seed ──────────────────────────────────────────

def seed_historical(engine: SignalEngine):
    """
    Pre-fill the signal engine with 60 days of daily historical data
    so signals compute immediately without waiting for 26 live ticks.
    """
    log.info("Seeding signal engine with historical data...")
    for symbol in SYMBOLS:
        try:
            df = yf.download(symbol, period="60d", interval="1d", progress=False)
            if df.empty:
                log.warning(f"No historical data for {symbol}")
                continue
            for ts, row in df.iterrows():
                tick = {
                    "symbol":    symbol,
                    "open":      round(float(row["Open"].iloc[0]), 4),
                    "high":      round(float(row["High"].iloc[0]), 4),
                    "low":       round(float(row["Low"].iloc[0]), 4),
                    "close":     round(float(row["Close"].iloc[0]), 4),
                    "volume":    int(row["Volume"].iloc[0]),
                    "timestamp": str(ts),
                    "source":    "historical",
                }
                engine.add_tick(tick)
            log.info(f"{symbol} — seeded {len(df)} historical ticks")
        except Exception as e:
            log.error(f"Error seeding {symbol}: {e}")
    log.info("Historical seeding complete. Signals ready.")


# ─── Kafka ────────────────────────────────────────────────────

def create_consumer():
    for attempt in range(1, 6):
        try:
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=GROUP_ID,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
            )
            log.info("Consumer connected to Kafka.")
            return consumer
        except KafkaConnectionError:
            log.warning(f"Kafka not ready, attempt {attempt}/5 — retrying in 5s...")
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka.")


def create_producer():
    for attempt in range(1, 6):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
            )
            log.info("Producer connected to Kafka.")
            return producer
        except KafkaConnectionError:
            log.warning(f"Kafka not ready, attempt {attempt}/5 — retrying in 5s...")
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka.")


def signal_to_dict(signal: Signal) -> dict:
    """Convert Signal dataclass to dict for Kafka."""
    return {
        "symbol":         signal.symbol,
        "timestamp":      signal.timestamp,
        "close":          signal.close,
        "rsi":            signal.rsi,
        "macd":           signal.macd,
        "macd_signal":    signal.macd_signal,
        "macd_crossover": signal.macd_crossover,
        "bb_upper":       signal.bb_upper,
        "bb_lower":       signal.bb_lower,
        "bb_position":    signal.bb_position,
        "volume_anomaly": signal.volume_anomaly,
    }


# ─── Main ─────────────────────────────────────────────────────

def run():
    # Setup DB
    conn = get_db_connection()
    create_tables(conn)

    # Setup Kafka
    consumer = create_consumer()
    producer = create_producer()

    # Pre-seed signal engine with historical data
    engine = SignalEngine(window=60)
    seed_historical(engine)

    log.info(f"Consuming from '{INPUT_TOPIC}' → computing signals → saving to PostgreSQL")
    log.info(f"Publishing enriched signals to '{OUTPUT_TOPIC}'")
    log.info("─" * 50)

    try:
        for message in consumer:
            tick = message.value
            signal = engine.add_tick(tick)

            if signal is None:
                log.info(f"{tick['symbol']} — collecting data ({len(engine.history.get(tick['symbol'], []))}/26 ticks needed)")
                continue

            # Log signal summary
            log.info(
                f"{signal.symbol:<6}  close={signal.close:<10}  "
                f"RSI={str(signal.rsi):<8}  MACD={str(signal.macd):<10}  "
                f"BB={str(signal.bb_position):<8}  "
                f"{'⚠ VOLUME SPIKE' if signal.volume_anomaly else ''}"
                f"{'🔼 BULLISH' if signal.macd_crossover == 'bullish' else ''}"
                f"{'🔽 BEARISH' if signal.macd_crossover == 'bearish' else ''}"
            )

            # Save to PostgreSQL
            save_signal(conn, signal)

            # Publish to enriched-signals topic
            producer.send(OUTPUT_TOPIC, key=signal.symbol, value=signal_to_dict(signal))
            producer.flush()

    except KeyboardInterrupt:
        log.info("Stopped by user.")
    finally:
        consumer.close()
        producer.close()
        conn.close()
        log.info("ETL pipeline closed.")


if __name__ == "__main__":
    run()
