"""
MarketPulse - Kafka Consumer
Reads live tick data from Kafka topic: raw-ticks and prints to terminal
Usage: python ingestion/kafka_consumer.py
"""

import json
import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaConnectionError
from dotenv import load_dotenv
import os
import time

load_dotenv()

# ─── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [CONSUMER]  %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC           = "raw-ticks"
GROUP_ID        = "marketpulse-consumer-group"


def create_consumer():
    """Create Kafka consumer with retry logic."""
    retries = 5
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=GROUP_ID,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
            )
            log.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP}")
            return consumer
        except KafkaConnectionError:
            log.warning(f"Kafka not ready, attempt {attempt}/{retries} — retrying in 5s...")
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after multiple attempts.")


def format_tick(tick: dict) -> str:
    """Format a tick for pretty terminal output."""
    return (
        f"  Symbol : {tick.get('symbol')}\n"
        f"  Open   : {tick.get('open')}\n"
        f"  High   : {tick.get('high')}\n"
        f"  Low    : {tick.get('low')}\n"
        f"  Close  : {tick.get('close')}\n"
        f"  Volume : {tick.get('volume')}\n"
        f"  Time   : {tick.get('timestamp')}\n"
        f"  Source : {tick.get('source')}\n"
    )


def run():
    consumer = create_consumer()
    log.info(f"Listening on topic: '{TOPIC}'")
    log.info("Waiting for messages... (Ctrl+C to stop)")
    log.info("─" * 50)

    try:
        for message in consumer:
            print(f"\n{'─'*50}")
            print(f"  Key    : {message.key}")
            print(f"  Offset : {message.offset}")
            print(format_tick(message.value))

    except KeyboardInterrupt:
        log.info("Stopped by user.")
    finally:
        consumer.close()
        log.info("Consumer closed.")


if __name__ == "__main__":
    run()
