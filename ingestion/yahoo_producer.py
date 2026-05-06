"""
MarketPulse - Yahoo Finance Kafka Producer
Fetches live stock data and publishes to Kafka topic: raw-ticks
Usage: python ingestion/yahoo_producer.py
"""

import json
import time
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaConnectionError
import yfinance as yf
from dotenv import load_dotenv
import os

load_dotenv()

# ─── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [PRODUCER]  %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC           = "raw-ticks"
SYMBOLS         = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
INTERVAL_SEC    = 10   # fetch every 10 seconds


def create_producer():
    """Create Kafka producer with retry logic."""
    retries = 5
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=3,
            )
            log.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP}")
            return producer
        except KafkaConnectionError:
            log.warning(f"Kafka not ready, attempt {attempt}/{retries} — retrying in 5s...")
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after multiple attempts.")


def fetch_tick(symbol: str) -> dict | None:
    """Fetch latest 1-minute tick for a symbol using yfinance."""
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="1d", interval="1m")
        if df.empty:
            log.warning(f"No data returned for {symbol}")
            return None

        latest = df.iloc[-1]
        return {
            "symbol":    symbol,
            "open":      round(float(latest["Open"]), 4),
            "high":      round(float(latest["High"]), 4),
            "low":       round(float(latest["Low"]), 4),
            "close":     round(float(latest["Close"]), 4),
            "volume":    int(latest["Volume"]),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source":    "yahoo_finance",
        }
    except Exception as e:
        log.error(f"Error fetching {symbol}: {e}")
        return None


def run():
    producer = create_producer()
    log.info(f"Streaming symbols: {SYMBOLS}")
    log.info(f"Publishing to topic: '{TOPIC}' every {INTERVAL_SEC}s")
    log.info("─" * 50)

    try:
        while True:
            for symbol in SYMBOLS:
                tick = fetch_tick(symbol)
                if tick:
                    producer.send(TOPIC, key=symbol, value=tick)
                    log.info(
                        f"{symbol:<6}  close={tick['close']:<10}  "
                        f"vol={tick['volume']:<12}  ts={tick['timestamp']}"
                    )
            producer.flush()
            log.info(f"Batch sent. Waiting {INTERVAL_SEC}s...\n")
            time.sleep(INTERVAL_SEC)

    except KeyboardInterrupt:
        log.info("Stopped by user.")
    finally:
        producer.close()
        log.info("Producer closed.")


if __name__ == "__main__":
    run()
