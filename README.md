# End-to-End Stock Prediction Data Pipeline

> **Already set up Metabase, pgAdmin, and Streamlit?** Jump straight to the [⚡ Quick Start](#-quick-start) below.

---

## ⚡ Quick Start

> Use this every time you want to run the project. Open **5 terminals**.

### Before you begin — set environment variables (every new terminal)
```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export SPARK_HOME=/opt/anaconda3/lib/python3.12/site-packages/pyspark
```

---

### Terminal 1 — Start infrastructure
```bash
docker compose up -d
```
✅ Wait ~30 seconds. Then verify everything is up:
```bash
docker compose ps
```
All containers should show **Up** or **healthy**.

Open your services in the browser now — they're ready:
| Service | URL | Login |
|---|---|---|
| pgAdmin | http://localhost:5051 | `admin@stock.com` / `admin123` |
| Metabase | http://localhost:3000 | your account |
| FastAPI docs | http://localhost:8000/docs | — |
| Streamlit | http://localhost:8501 | — |

---

### Terminal 1 — Run the batch pipeline (one after another)
> Skip Step A if you already downloaded 2 years of data.

```bash
# Step A — Download historical data (skip if already done)
python -m src.ingestion.yfinance_backfill --ticker AAPL --years 2

# Step B — Bronze layer
$SPARK_HOME/bin/spark-submit src/etl/spark_batch.py --ticker AAPL --input-format parquet

# Step C — Silver layer
$SPARK_HOME/bin/spark-submit src/warehouse/bronze_to_silver.py --ticker AAPL

# Step D — Gold layer (Star Schema)
$SPARK_HOME/bin/spark-submit src/warehouse/silver_to_gold.py --ticker AAPL

# Step E — Load Gold → PostgreSQL
python -m src.warehouse.gold_to_postgres --ticker AAPL

# Step F — Train models (ARIMA, LightGBM, LSTM)
python -m src.training.train_pipeline --ticker AAPL
```
✅ Done when you see the model comparison table printed in the terminal.

---

### Terminal 2 — Start FastAPI
```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export SPARK_HOME=/opt/anaconda3/lib/python3.12/site-packages/pyspark
uvicorn src.app.api:app --host 0.0.0.0 --port 8000 --reload
```
✅ API is ready when you see `Application startup complete.`

---

### Terminal 3 — Start Streamlit
```bash
streamlit run src/app/dashboard.py
```
✅ Opens automatically at http://localhost:8501

---

### Terminal 4 — Start Kafka producer (Finnhub → Kafka)
> ⚠️ Only streams data during US market hours: **Mon–Fri, 9:30am–4:00pm EST**

```bash
python -m src.etl.kafka_producer --ticker AAPL
```
✅ You should see live trade JSON printing every few seconds.

---

### Terminal 5 — Start PySpark streaming (Kafka → Bronze + PostgreSQL)
```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export SPARK_HOME=/opt/anaconda3/lib/python3.12/site-packages/pyspark

$SPARK_HOME/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  src/etl/spark_streaming.py
```
✅ After 2 minutes, verify rows are arriving in PostgreSQL:
```bash
docker exec stock_postgres psql -U stock_user -d stock_db -c \
  "SELECT ticker, window_start, close FROM stock_prices_stream ORDER BY window_start DESC LIMIT 5;"
```

---

### Everything is running ✅

| Terminal | Process | Status check |
|---|---|---|
| 1 | Docker + batch pipeline | `docker compose ps` |
| 2 | FastAPI | http://localhost:8000/docs |
| 3 | Streamlit | http://localhost:8501 |
| 4 | Kafka producer | trade JSON printing |
| 5 | PySpark streaming | rows in `stock_prices_stream` |

---
---

## 📖 Full Setup Guide

> First time running this project? Follow this section completely before using the Quick Start above.

---

## 🛠 Prerequisites

Before running the pipeline, ensure you have the following installed:
- **Docker & Docker Compose** (for Kafka, Zookeeper, PostgreSQL, pgAdmin, Metabase)
- **Python 3.10+**
- **Java 17** (required for PySpark)

Install dependencies:
```bash
pip install -r requirements.txt
pip install pyspark==3.5.1
```

> ⚠️ PySpark must be `3.5.1` to stay compatible with the Kafka Scala 2.12 streaming libraries.

Set these permanently in your shell so they work in every new terminal:
```bash
echo 'export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home' >> ~/.zshrc
echo 'export SPARK_HOME=/opt/anaconda3/lib/python3.12/site-packages/pyspark' >> ~/.zshrc
source ~/.zshrc
```

---

## 🚀 1. Start the Infrastructure (Docker)

```bash
docker compose up -d
```

**Services started:**
| Service | Port | Purpose |
|---|---|---|
| Kafka + Zookeeper | 9092 | Real-time message queue |
| PostgreSQL | 5433 (host) / 5432 (internal) | Data warehouse |
| pgAdmin | 5051 | Database browser UI |
| Metabase | 3000 | BI dashboard |

---

## 🏗 2. Run the Batch Pipeline (Historical Data)

Run each command and wait for it to finish before the next.

```bash
# A — Download 2 years of AAPL data from Yahoo Finance
python -m src.ingestion.yfinance_backfill --ticker AAPL --years 2

# B — Load into Bronze layer (raw Parquet)
$SPARK_HOME/bin/spark-submit src/etl/spark_batch.py --ticker AAPL --input-format parquet

# C — Clean and enrich → Silver layer
$SPARK_HOME/bin/spark-submit src/warehouse/bronze_to_silver.py --ticker AAPL

# D — Build Star Schema → Gold layer
$SPARK_HOME/bin/spark-submit src/warehouse/silver_to_gold.py --ticker AAPL

# E — Load Gold data into PostgreSQL
python -m src.warehouse.gold_to_postgres --ticker AAPL
```

---

## 🧠 3. Train Machine Learning Models

Trains ARIMA, LightGBM, and LSTM. Saves plots to `data/plots/` and logs to MLflow.

```bash
python -m src.training.train_pipeline --ticker AAPL
```

View results in MLflow:
```bash
mlflow ui --backend-store-uri file://$(pwd)/data/mlruns
```
Visit: http://127.0.0.1:5000

---

## 🌐 4. Start the Application Layer

**Terminal A — FastAPI backend:**
```bash
uvicorn src.app.api:app --host 0.0.0.0 --port 8000 --reload
```
Docs: http://localhost:8000/docs

**Terminal B — Streamlit dashboard:**
```bash
streamlit run src/app/dashboard.py
```
Dashboard: http://localhost:8501

---

## ⚡ 5. Start Real-Time Streaming (Kafka + Finnhub)

> Only works during US market hours: Mon–Fri 9:30am–4:00pm EST.

**Terminal C — Kafka producer (Finnhub websocket → Kafka):**
```bash
python -m src.etl.kafka_producer --ticker AAPL
```

**Terminal D — PySpark streaming (Kafka → Bronze + PostgreSQL):**
```bash
python src/etl/spark_streaming.py
```

---

## 🔐 Logins, URLs & Credentials

### 🐘 pgAdmin
| Field | Value |
|---|---|
| URL | http://localhost:5051 |
| Email | `admin@stock.com` |
| Password | `admin123` |

**To connect to the database inside pgAdmin:**
| Field | Value |
|---|---|
| Host | `stock_postgres` |
| Port | `5432` *(use 5433 only from your Mac terminal)* |
| Username | `stock_user` |
| Password | `stock_password` |
| Database | `stock_db` |

---

### 📊 Metabase
| Field | Value |
|---|---|
| URL | http://localhost:3000 |
| Setup | Create your account on first visit |

**Database connection settings:**
| Field | Value |
|---|---|
| Type | PostgreSQL |
| Host | `stock_postgres` |
| Port | `5432` |
| Database | `stock_db` |
| Username | `stock_user` |
| Password | `stock_password` |

---

### 📈 All URLs at a glance
| Service | URL |
|---|---|
| Streamlit dashboard | http://localhost:8501 |
| FastAPI docs | http://localhost:8000/docs |
| pgAdmin | http://localhost:5051 |
| Metabase | http://localhost:3000 |
| MLflow (when running) | http://127.0.0.1:5000 |