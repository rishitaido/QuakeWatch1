# 🌍 QuakeWatch

A real-time earthquake monitoring platform built with Python microservices, AWS (SQS + DynamoDB), Docker, and a Leaflet.js geo dashboard.

---

## Architecture

```
USGS GeoJSON Feed
       │
       ▼
┌─────────────┐      SQS Queue      ┌──────────────┐
│  Ingester   │ ──────────────────► │  Processor   │
│  (Python)   │                     │  (Python)    │
└─────────────┘                     └──────┬───────┘
                                           │ writes impact scores
                                           ▼
                                    ┌─────────────────┐
                                    │   DynamoDB       │
                                    │  earthquakes     │
                                    │  alerts          │
                                    │  cities          │
                                    └──────┬──────────┘
                                           │
                          ┌────────────────┼────────────────┐
                          ▼                ▼                ▼
                  ┌──────────────┐  ┌──────────────┐  ┌──────────┐
                  │Alert Evaluator│  │   REST API   │  │Dashboard │
                  │  (Python)    │  │  (FastAPI)   │  │ (Nginx + │
                  └──────────────┘  └──────────────┘  │ Leaflet) │
                                                       └──────────┘
```

---

## Services

| Service | Owner | Description |
|---------|-------|-------------|
| `ingester` | Rishi | Polls USGS every 60 s, deduplicates by event ID, publishes to SQS |
| `api` | Rishi | FastAPI — serves `/earthquakes`, `/alerts`, `/stats` |
| `processor` | Asha | Consumes SQS messages, calculates Haversine impact scores, writes to DynamoDB |
| `alert-evaluator` | Asha | Monitors earthquakes table, creates alert records for high/medium severity events |
| `dashboard` | Hania | Nginx-served Leaflet map with real-time markers, sidebar, and filters |

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [AWS CLI](https://aws.amazon.com/cli/) configured, or fill in `.env` manually
- An AWS account with:
  - An SQS queue named `quakewatch-queue`
  - Three DynamoDB tables: `earthquakes`, `alerts`, `cities`

---

## Quick Start

### 1. Clone the repo

```bash
git clone https://github.com/rishitaido/quakewatch.git
cd quakewatch
```

### 2. Configure environment variables

```bash
cp .env.example .env
# Edit .env and fill in your AWS credentials and resource names
```

### 3. Build and run all services

```bash
docker compose up --build
```

### 4. Access the dashboard

Open [http://localhost](http://localhost) in your browser.

The REST API is available at [http://localhost:8000](http://localhost:8000).

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/earthquakes` | List recent earthquakes (optional: `?min_mag=4.5&limit=50`) |
| `GET` | `/alerts` | List active alerts |
| `GET` | `/stats` | Summary stats (count, avg magnitude, top region) |
| `GET` | `/health` | Health check |

---

## DynamoDB Table Schemas

### `earthquakes`
| Attribute | Type | Notes |
|-----------|------|-------|
| `event_id` | String | Partition key (USGS event ID) |
| `magnitude` | Number | Richter magnitude |
| `place` | String | Human-readable location |
| `time` | Number | Unix timestamp (ms) |
| `lat` / `lon` | Number | Coordinates |
| `impact_score` | Number | Computed by processor |

### `alerts`
| Attribute | Type | Notes |
|-----------|------|-------|
| `alert_id` | String | Partition key (UUID) |
| `event_id` | String | FK → earthquakes |
| `severity` | String | `HIGH` or `MEDIUM` |
| `created_at` | String | ISO 8601 timestamp |

### `cities`
| Attribute | Type | Notes |
|-----------|------|-------|
| `city_id` | String | Partition key |
| `name` | String | City name |
| `lat` / `lon` | Number | Coordinates |
| `population` | Number | Used for impact score calculation |

---

## AWS Setup

### Create the SQS Queue

```bash
aws sqs create-queue --queue-name quakewatch-queue --region us-east-1
```

### Create DynamoDB Tables

```bash
# Earthquakes table
aws dynamodb create-table \
  --table-name earthquakes \
  --attribute-definitions AttributeName=event_id,AttributeType=S \
  --key-schema AttributeName=event_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region us-east-1

# Alerts table
aws dynamodb create-table \
  --table-name alerts \
  --attribute-definitions AttributeName=alert_id,AttributeType=S \
  --key-schema AttributeName=alert_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region us-east-1

# Cities table
aws dynamodb create-table \
  --table-name cities \
  --attribute-definitions AttributeName=city_id,AttributeType=S \
  --key-schema AttributeName=city_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region us-east-1
```

### Seed City Data

```bash
cd seed-data
pip install -r requirements.txt
python seed_cities.py
```

---

## Stopping Services

```bash
docker compose down
```

---

## Team

| Member | Role |
|--------|------|
| Rishi | Infrastructure, Seismic Ingester, REST API |
| Asha | Impact Processor, Alert Evaluator, Seed Data |
| Hania | Geo Dashboard (Leaflet + Nginx) |

---

## License

MIT
