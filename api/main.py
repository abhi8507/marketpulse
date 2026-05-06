"""
MarketPulse - FastAPI Main
Entry point for the MarketPulse API.
Serves REST endpoints and WebSocket stream for live signals.
Usage: uvicorn api.main:app --reload --port 8000
"""

import json
import logging
import asyncio
import os
from contextlib import asynccontextmanager
from dotenv import load_dotenv

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer
from kafka.errors import KafkaConnectionError

from api.routes.signals import router as signals_router
from api.routes.alerts import router as alerts_router
from api.websocket_manager import manager

load_dotenv()

# ─── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [API]  %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SIGNAL_TOPIC    = "enriched-signals"


# ─── Kafka → WebSocket broadcaster ────────────────────────────

async def kafka_to_websocket():
    """
    Background task: consumes enriched-signals from Kafka
    and broadcasts each message to all connected WebSocket clients.
    """
    loop = asyncio.get_event_loop()

    def consume():
        """Runs in a thread — consumes Kafka and schedules broadcasts."""
        try:
            consumer = KafkaConsumer(
                SIGNAL_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="marketpulse-api-group",
                auto_offset_reset="latest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
            )
            log.info(f"Broadcasting '{SIGNAL_TOPIC}' to WebSocket clients...")
            for message in consumer:
                if manager.active_connections:
                    asyncio.run_coroutine_threadsafe(
                        manager.broadcast(message.value), loop
                    )
        except Exception as e:
            log.error(f"Kafka broadcast error: {e}")

    import threading
    thread = threading.Thread(target=consume, daemon=True)
    thread.start()


# ─── App Lifespan ─────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start Kafka broadcaster on startup."""
    # task = asyncio.create_task(kafka_to_websocket())  # disabled temporarily
    log.info("MarketPulse API started.")
    yield
    # task.cancel()
    log.info("MarketPulse API stopped.")


# ─── App ──────────────────────────────────────────────────────

app = FastAPI(
    title="MarketPulse API",
    description="Real-time stock market intelligence platform",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS — allow Angular dev server
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Routers ──────────────────────────────────────────────────
app.include_router(signals_router)
app.include_router(alerts_router)


# ─── Health Check ─────────────────────────────────────────────

@app.get("/health", tags=["Health"])
def health():
    """Health check endpoint."""
    return {"status": "ok", "service": "MarketPulse API"}


# ─── WebSocket ────────────────────────────────────────────────

@app.websocket("/ws/signals")
async def websocket_signals(websocket: WebSocket):
    """
    WebSocket endpoint — streams live enriched signals to connected clients.
    Connect from Angular: new WebSocket('ws://localhost:8000/ws/signals')
    """
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()  # keep connection alive
    except WebSocketDisconnect:
        manager.disconnect(websocket)