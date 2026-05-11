# Stage 2 — ETL Pipeline (Streaming + Batch)

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          REAL-TIME PATH                                    │
│                                                                             │
│  Finnhub WS ──► kafka_producer.py ──► Kafka [stock-prices-raw] ──►         │
│                       │                        3 partitions                 │
│                       │ (invalid)                    │                      │
│                       ▼                              ▼                      │
│              Kafka [stock-prices-dlq]     spark_streaming.py                │
│                                          • Parse JSON                      │
│                                          • Validate trades                 │
│                                          • Watermark (5 min)              │
│                                          • 1-min tumbling window          │
│                                          • OHLCV aggregation              │
│                                                │                           │
├────────────────────────────────────────────────┼───────────────────────────┤
│                          BATCH PATH            │                           │
│                                                │                           │
│  yfinance_backfill.py ──► data/raw/*.parquet   │                           │
│                                 │              │                           │
│                                 ▼              │                           │
│                          spark_batch.py         │                           │
│                          • Read Parquet        │                           │
│                          • Validate OHLCV      │                           │
│                          • Align schema        │                           │
│                                 │              │                           │
├─────────────────────────────────┼──────────────┘                           │
│                                 ▼                                          │
│                     ┌──────────────────────┐                               │
│                     │   BRONZE LAYER       │                               │
│                     │   data/bronze/       │                               │
│                     │   Partitioned by     │                               │
│                     │   (date, ticker)     │                               │
│                     └──────────────────────┘                               │
└─────────────────────────────────────────────────────────────────────────────┘
```

> [!IMPORTANT]
> Both paths use the **same shared transforms** (`transforms.py`) — this is the Lambda Architecture pattern ensuring consistency.

---

## 2.1 Kafka Setup

### Infrastructure (Docker Compose)

The [docker-compose.yml](file:///Users/dukpagnarith/Documents/data_engineer_prject_final_course/docker-compose.yml) spins up:

| Service | Port | Purpose |
|---|---|---|
| **Zookeeper** | 2181 | Kafka coordination |
| **Kafka broker** | 9092 (host), 29092 (internal) | Message broker |
| **Schema Registry** | 8081 | Schema management (for future Avro use) |
| **kafka-init** | — | One-shot: creates topics on startup |

### Topics created automatically

| Topic | Partitions | Retention | Purpose |
|---|---|---|---|
| `stock-prices-raw` | 3 | 7 days | Main trade events from Finnhub |
| `stock-prices-dlq` | 1 | 30 days | Dead-letter queue for malformed/invalid messages |

### Commands

```bash
# Start infrastructure
docker-compose up -d

# Verify topics were created
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Check topic details
docker exec kafka kafka-topics --describe --topic stock-prices-raw \
    --bootstrap-server localhost:9092

# Tear down
docker-compose down -v
```

### Kafka Producer → [kafka_producer.py](file:///Users/dukpagnarith/Documents/data_engineer_prject_final_course/src/etl/kafka_producer.py)

Bridges Finnhub websocket → Kafka. For each trade event:
1. Validates required fields (`s`, `p`, `v`, `t`)
2. If **valid** → normalises to canonical schema → publishes to `stock-prices-raw`
3. If **invalid** → routes to `stock-prices-dlq` with error reason

```bash
# Start producing (after docker-compose up)
python -m src.etl.kafka_producer
python -m src.etl.kafka_producer --ticker MSFT
```

### Kafka Consumer → [kafka_consumer.py](file:///Users/dukpagnarith/Documents/data_engineer_prject_final_course/src/etl/kafka_consumer.py)

Verification consumer for debugging:

```bash
# Read from main topic
python -m src.etl.kafka_consumer

# Read from DLQ
python -m src.etl.kafka_consumer --dlq

# Read only 10 messages
python -m src.etl.kafka_consumer --max-messages 10
```

---

## 2.2 PySpark Streaming (Real-time Path)

**File:** [spark_streaming.py](file:///Users/dukpagnarith/Documents/data_engineer_prject_final_course/src/etl/spark_streaming.py)

### Pipeline steps:

```
Kafka (stock-prices-raw)
  │
  ▼ readStream (format=kafka)
  │
  ▼ Parse JSON → canonical trade schema
  │
  ▼ Add event_time column (from timestamp_ms)
  │
  ▼ Watermark: 5 minutes (drop late data beyond this)
  │
  ▼ Validate (filter invalid trades)
  │
  ▼ Tumbling window: 1 minute
  │  Aggregation: open (first), high (max), low (min),
  │               close (last), volume (sum), vwap, trade_count
  │
  ▼ Add Bronze metadata (ingested_at, source_api, batch_id)
  │
  ▼ Add date partition column
  │
  ▼ writeStream → Parquet (data/bronze/stock_prices/)
     partitionBy(date, ticker)
     checkpointLocation → data/checkpoints/streaming_bronze/
     trigger: processingTime = 1 minute
```

### Running

```bash
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    src/etl/spark_streaming.py
```

### Key settings

| Parameter | Value | Rationale |
|---|---|---|
| Watermark | 5 minutes | Allows moderately late data while keeping state manageable |
| Trigger | 1 minute | Balances latency vs. throughput for stock data |
| Output mode | append | OHLCV bars are immutable once the window closes |
| Checkpoint | `data/checkpoints/` | Exactly-once semantics; recoverable on restart |

---

## 2.3 PySpark Batch (Historical Path)

**File:** [spark_batch.py](file:///Users/dukpagnarith/Documents/data_engineer_prject_final_course/src/etl/spark_batch.py)

### Pipeline steps:

```
data/raw/historical_AAPL.parquet
  │
  ▼ spark.read.parquet()
  │
  ▼ Validate (same rules as streaming, via shared transforms.py)
  │
  ▼ Schema alignment (rename date→window_start, add window_end, etc.)
  │
  ▼ Add Bronze metadata (source_api="yfinance", batch_id)
  │
  ▼ Add date partition column
  │
  ▼ write → Parquet (data/bronze/stock_prices/)
     mode=append, partitionBy(date, ticker)
```

### Running

```bash
# Default: AAPL, parquet format
spark-submit src/etl/spark_batch.py

# Override ticker and format
spark-submit src/etl/spark_batch.py --ticker MSFT --input-format csv
```

---

## 2.4 Streaming vs. Batch Processing

### Shared transforms — Lambda Architecture

**File:** [transforms.py](file:///Users/dukpagnarith/Documents/data_engineer_prject_final_course/src/etl/transforms.py)

Both paths use the **same** functions:

| Function | Purpose | Used by |
|---|---|---|
| `validate_trades()` | Filter invalid raw trades | Streaming |
| `validate_historical()` | Filter invalid daily OHLCV | Batch |
| `aggregate_ohlcv_1min()` | 1-minute tumbling window | Streaming |
| `add_bronze_metadata()` | Append ingested_at, source, batch_id | Both |
| `add_date_partition_column()` | Add date for partitioning | Both |

### When each path is used

| Aspect | Streaming (Real-time) | Batch (Historical) |
|---|---|---|
| **Data source** | Finnhub websocket → Kafka | yfinance → local Parquet |
| **Latency** | ~1 minute (trigger interval) | Minutes–hours (run on demand) |
| **Granularity** | Trade ticks → 1-min OHLCV | Daily OHLCV (pre-aggregated) |
| **When used** | During market hours for live data | One-time backfill or scheduled catch-up |
| **Fault tolerance** | Checkpointing + watermarks | Idempotent (re-runnable) |
| **State** | Stateful (windowed aggregation) | Stateless (read → transform → write) |
| **Trigger** | Continuous (while running) | Manual or Airflow-scheduled |

> [!TIP]
> The batch path exists so you can bootstrap the system with years of historical data before turning on the real-time stream. Both converge on the same Bronze layer, making downstream stages (Silver, Gold, ML) source-agnostic.

---

## Files Created in Stage 2

| File | Purpose |
|---|---|
| [docker-compose.yml](file:///Users/dukpagnarith/Documents/data_engineer_prject_final_course/docker-compose.yml) | Kafka infrastructure |
| [kafka_producer.py](file:///Users/dukpagnarith/Documents/data_engineer_prject_final_course/src/etl/kafka_producer.py) | Finnhub → Kafka bridge |
| [kafka_consumer.py](file:///Users/dukpagnarith/Documents/data_engineer_prject_final_course/src/etl/kafka_consumer.py) | Verification consumer |
| [transforms.py](file:///Users/dukpagnarith/Documents/data_engineer_prject_final_course/src/etl/transforms.py) | Shared validation + aggregation |
| [spark_streaming.py](file:///Users/dukpagnarith/Documents/data_engineer_prject_final_course/src/etl/spark_streaming.py) | PySpark Structured Streaming job |
| [spark_batch.py](file:///Users/dukpagnarith/Documents/data_engineer_prject_final_course/src/etl/spark_batch.py) | PySpark batch ingestion job |
| [requirements.txt](file:///Users/dukpagnarith/Documents/data_engineer_prject_final_course/requirements.txt) | Updated with Kafka + PySpark deps |

---

## Updated Project Structure

```
data_engineer_prject_final_course/
├── .env / .env.example / .gitignore
├── requirements.txt
├── project_guide.txt
├── docker-compose.yml                    ← NEW (Stage 2)
├── config/
│   ├── __init__.py
│   └── settings.py
├── src/
│   ├── __init__.py
│   ├── ingestion/                        ← Stage 1
│   │   ├── __init__.py
│   │   ├── finnhub_producer.py
│   │   └── yfinance_backfill.py
│   └── etl/                              ← NEW (Stage 2)
│       ├── __init__.py
│       ├── kafka_producer.py
│       ├── kafka_consumer.py
│       ├── transforms.py
│       ├── spark_streaming.py
│       └── spark_batch.py
└── data/
    ├── raw/
    │   ├── historical_AAPL.csv
    │   └── historical_AAPL.parquet
    ├── bronze/                           ← Written by PySpark jobs
    │   └── stock_prices/
    └── checkpoints/                      ← Streaming fault tolerance
        └── streaming_bronze/
```
