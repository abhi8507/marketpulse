# MarketPulse — Copilot Instructions

## Project Overview
MarketPulse is a full-stack, event-driven, real-time stock market intelligence platform.
It ingests live market data, computes technical signals, and surfaces LLM-generated
summaries through a real-time Angular dashboard.

---

## Developer Profile
- **Name:** Abhishek Kumar
- **Role:** Software Engineer 2 at JPMorganChase
- **Background:** Python ETL pipelines, Apache Kafka, FastAPI, Angular, PostgreSQL,
  NumPy/Pandas, AWS, ELK stack, Prometheus, LLM agents
- **Experience:** 4+ years across JPMorganChase, TCS, Cognizant

---

## Tech Stack

| Layer            | Technology                                      |
|------------------|-------------------------------------------------|
| Data Sources     | Binance WebSocket, yfinance, Alpha Vantage, Polygon.io |
| Streaming        | Apache Kafka                                    |
| Processing       | Python, NumPy, Pandas                           |
| Signal Engine    | RSI, MACD, Bollinger Bands, anomaly detection   |
| AI Layer         | OpenAI / GitHub Copilot API                     |
| Storage (hot)    | PostgreSQL                                      |
| Storage (cold)   | AWS S3                                          |
| API              | FastAPI (REST + WebSocket)                      |
| Frontend         | Angular                                         |
| Observability    | ELK Stack, Prometheus, Grafana                  |
| Infra            | Docker, Docker Compose, AWS                     |

---

## Architecture

```
Market APIs (Binance WS, Yahoo Finance, Alpha Vantage, Polygon.io)
        │
        ▼
  Apache Kafka
  Topics: raw-ticks | enriched-signals | alerts | llm-summaries
        │
        ▼
  Python ETL Layer  ──►  Signal Engine  ──►  LLM Agent
  (ingest, enrich)       (RSI, MACD,         (OpenAI API —
  NumPy, Pandas          Bollinger Bands)     plain-English summaries)
        │                      │                     │
        ▼                      ▼                     ▼
  PostgreSQL (hot)       AWS S3 (cold)         FastAPI (REST + WS)
        │                                            │
        └──────────────────┬─────────────────────────┘
                           ▼
                  Angular Dashboard
            (live charts, signals, alert feed)
                           │
              ELK + Prometheus + Grafana
                  (observability layer)
```

---

## Project Structure

```
marketpulse/
├── ingestion/                  # Kafka producers for each data source
│   ├── __init__.py
│   ├── yahoo_producer.py       # yfinance → Kafka (raw-ticks)
│   ├── binance_ws.py           # Binance WebSocket → Kafka
│   └── alpha_vantage.py        # Alpha Vantage → Kafka
├── processing/                 # ETL + signal computation
│   ├── __init__.py
│   ├── etl_pipeline.py         # Kafka consumer → enrich → DB
│   ├── signal_engine.py        # RSI, MACD, Bollinger Bands
│   └── anomaly_detector.py     # Volume + price anomalies
├── llm_agent/                  # LLM summarisation layer
│   └── market_summariser.py    # Reads signals → OpenAI → summaries
├── api/                        # FastAPI backend
│   ├── main.py
│   ├── routes/
│   │   ├── signals.py
│   │   └── alerts.py
│   └── websocket_manager.py
├── frontend/                   # Angular dashboard
│   └── src/
├── infra/                      # Docker, Prometheus, ELK config
│   ├── docker-compose.yml
│   ├── prometheus.yml
│   └── logstash.conf
├── .env                        # Environment variables
├── requirements.txt
└── README.md
```

---

## Kafka Topics

| Topic               | Description                                      |
|---------------------|--------------------------------------------------|
| `raw-ticks`         | Raw OHLCV price data from all sources            |
| `enriched-signals`  | Normalised data + computed technical indicators  |
| `alerts`            | Triggered signals (RSI overbought, MACD cross)   |
| `llm-summaries`     | LLM-generated plain-English market commentary    |

---

## Environment Variables (.env)

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
DATABASE_URL=postgresql://admin:password@localhost:5432/marketpulse
ALPHA_VANTAGE_API_KEY=your_key
POLYGON_API_KEY=your_key
OPENAI_API_KEY=your_key
AWS_S3_BUCKET=your_bucket
```

---

## Docker Services

| Container                  | Port | Purpose                    |
|----------------------------|------|----------------------------|
| marketpulse-zookeeper      | 2181 | Required by Kafka          |
| marketpulse-kafka          | 9092 | Message streaming          |
| marketpulse-kafka-ui       | 8080 | Visual Kafka dashboard     |
| marketpulse-postgres       | 5432 | Database                   |
| marketpulse-prometheus     | 9090 | Metrics collection         |
| marketpulse-grafana        | 3000 | Metrics dashboard          |

---

## Signal Engine

Signals computed on rolling windows using NumPy and Pandas:

- **RSI** (14-period) — overbought > 70, oversold < 30
- **MACD** (12/26/9) — bullish/bearish crossovers
- **Bollinger Bands** (20-period, 2σ) — breakout detection
- **Volume anomaly** — z-score spike detection via rolling mean/std

---

## Build Phases

| Phase | What we build                                              | Status      |
|-------|------------------------------------------------------------|-------------|
| 1     | Local setup, Docker, venv, folder structure                | ✅ Done     |
| 2     | yfinance producer → Kafka → consumer → terminal output     | 🔄 Current  |
| 3     | ETL pipeline + signal engine → PostgreSQL storage          | Pending     |
| 4     | FastAPI REST + WebSocket layer                             | Pending     |
| 5     | LLM summary layer (OpenAI)                                 | Pending     |
| 6     | Angular dashboard with live charts                         | Pending     |
| 7     | ELK + Prometheus observability                             | Pending     |
| 8     | Docker Compose everything + AWS deploy                     | Pending     |

---

## Coding Conventions

- Language: **Python 3.11+**
- All files use `python-dotenv` to load `.env` via `load_dotenv()`
- Kafka producers use `key_serializer` and `value_serializer` with JSON
- Kafka consumers use `group_id = "marketpulse-consumer-group"`
- All Kafka topics use kebab-case: `raw-ticks`, `enriched-signals`
- Logging uses `logging` module with format: `%(asctime)s  [MODULE]  %(message)s`
- All functions include docstrings
- Retry logic on all Kafka connections (5 attempts, 5s delay)
- Environment variables always have fallback defaults

---

## Current File Status

| File                              | Status      |
|-----------------------------------|-------------|
| `docker-compose.yml`              | ✅ Done     |
| `infra/prometheus.yml`            | ✅ Done     |
| `.env`                            | ✅ Created  |
| `requirements.txt`                | ✅ Created  |
| `ingestion/__init__.py`           | ✅ Created  |
| `processing/__init__.py`          | ✅ Created  |
| `ingestion/yahoo_producer.py`     | ✅ Done     |
| `ingestion/kafka_consumer.py`     | ✅ Done     |
| `processing/signal_engine.py`     | 🔄 Next     |
| `processing/etl_pipeline.py`      | Pending     |
| `llm_agent/market_summariser.py`  | Pending     |
| `api/main.py`                     | Pending     |
| `frontend/`                       | Pending     |
