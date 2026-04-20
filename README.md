# MarketPulse

A full-stack, event-driven platform that ingests live market data, computes technical signals, and surfaces LLM-generated summaries through a real-time dashboard.

---

## Architecture Overview

```
Market APIs (Binance WS, Yahoo Finance, Alpha Vantage, Polygon.io)
        │
        ▼
  Apache Kafka  ──────────────────────────────────────────────────┐
  (raw-ticks, enriched-signals, alerts topics)                    │
        │                                                         │
        ▼                                                         │
  Python ETL Layer                                                │
  (NumPy, Pandas — ingestion, enrichment, normalisation)          │
        │                                                         │
        ▼                                                         │
  Signal Engine                                          LLM Agent (GPT/Copilot)
  (RSI, MACD, Bollinger Bands, anomaly detection)    ◄──  Plain-English summaries
        │                                                         │
        ▼                                                         ▼
  PostgreSQL (hot signals)    AWS S3 (cold historical)    FastAPI (REST + WS)
        │                                                         │
        └──────────────────────────┬──────────────────────────────┘
                                   ▼
                         Angular Dashboard
                   (live charts, signals, alert feed)
                                   │
                      ELK + Prometheus + Grafana
                        (observability layer)
```

---

## Tech Stack

| Layer | Tech |
|---|---|
| Data Sources | Binance WebSocket, yfinance, Alpha Vantage, Polygon.io |
| Streaming | Apache Kafka |
| Processing | Python, NumPy, Pandas |
| Signal Engine | RSI, MACD, Bollinger Bands, rolling statistics |
| AI Layer | LLM API (OpenAI / GitHub Copilot) |
| Storage | PostgreSQL (hot), AWS S3 (cold archive) |
| API | FastAPI (REST + WebSocket) |
| Frontend | Angular |
| Observability | ELK Stack, Prometheus, Grafana, Filebeat |
| Infra | Docker, Docker Compose, AWS |

---

## Features

- **Live tick ingestion** — streams real-time price data from multiple sources via Kafka topics
- **Technical signal computation** — RSI, MACD, Bollinger Bands computed on rolling windows using NumPy/Pandas
- **LLM summaries** — an LLM agent reads signal clusters and generates plain-English market commentary and anomaly explanations
- **Real-time dashboard** — Angular frontend connected via WebSocket showing live candlestick charts, signal alerts, and LLM commentary feed
- **Full observability** — ELK stack for log aggregation, Prometheus + Grafana for pipeline health metrics
- **Cloud-native** — Dockerised services deployed on AWS, historical data archived to S3

---

## Getting Started

### Prerequisites
- Docker + Docker Compose
- Python 3.11+
- Node.js 18+
- AWS account (S3 bucket)
- API keys: Alpha Vantage, Polygon.io, OpenAI (or Copilot)

### Setup

```bash
git clone https://github.com/abhi8507/marketpulse
cd marketpulse

# Copy and fill in your API keys
cp .env.example .env

# Start all services (Kafka, PostgreSQL, FastAPI, ELK, Prometheus)
docker-compose up -d

# Install and start Angular dashboard
cd frontend
npm install
ng serve
```

### Environment Variables

```env
BINANCE_WS_URL=wss://stream.binance.com:9443/ws
ALPHA_VANTAGE_API_KEY=your_key
POLYGON_API_KEY=your_key
OPENAI_API_KEY=your_key
AWS_S3_BUCKET=your_bucket
DATABASE_URL=postgresql://user:password@localhost:5432/market_db
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

---

## Project Structure

```
market-intelligence-platform/
├── ingestion/              # Kafka producers for each data source
│   ├── binance_ws.py
│   ├── yahoo_finance.py
│   └── alpha_vantage.py
├── processing/             # ETL + signal computation
│   ├── etl_pipeline.py
│   ├── signal_engine.py    # RSI, MACD, Bollinger
│   └── anomaly_detector.py
├── llm_agent/              # LLM summarisation layer
│   └── market_summariser.py
├── api/                    # FastAPI backend
│   ├── main.py
│   ├── routes/
│   └── websocket_manager.py
├── frontend/               # Angular dashboard
│   └── src/
├── infra/                  # Docker Compose, Prometheus config, ELK config
│   ├── docker-compose.yml
│   ├── prometheus.yml
│   └── logstash.conf
└── README.md
```

---

## Kafka Topics

| Topic | Description |
|---|---|
| `raw-ticks` | Raw price data from all sources |
| `enriched-signals` | Normalised OHLCV + computed indicators |
| `alerts` | Triggered signals (RSI overbought, MACD crossover, etc.) |
| `llm-summaries` | LLM-generated commentary on signals |

---

## Signal Engine

Signals computed on rolling windows:

- **RSI** (14-period) — overbought > 70, oversold < 30
- **MACD** (12/26/9) — bullish/bearish crossovers
- **Bollinger Bands** (20-period, 2σ) — breakout detection
- **Volume anomaly** — z-score spike detection using rolling mean/std

---

## Dashboard

The Angular dashboard connects via WebSocket to the FastAPI backend and displays:
- Live candlestick charts (per symbol)
- Signal alert feed with LLM explanation
- Pipeline health panel (Prometheus metrics embedded)
- Symbol selector and timeframe controls

---

## Resume Bullet

> Built **MarketPulse**, a real-time market intelligence platform ingesting live tick data via Kafka, running technical analysis with NumPy/Pandas, and surfacing LLM-generated signals and summaries through a FastAPI + Angular dashboard — deployed on AWS.