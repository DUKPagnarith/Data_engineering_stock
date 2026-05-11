"""
spark_streaming.py — PySpark Structured Streaming job (real-time path).

Reads raw trade events from the Kafka topic `stock-prices-raw`,
validates and aggregates them into 1-minute OHLCV bars using a
tumbling window, then writes to TWO sinks simultaneously:

    Sink 1 — Bronze Parquet (existing, unchanged)
    Sink 2 — PostgreSQL stock_prices_stream table (NEW)

The Parquet sink is the primary sink and must NEVER fail due to a
Postgres issue. The Postgres sink is wrapped in try/except and will
log a warning and continue if the database is unavailable.

Features:
    - Reads from Kafka with Structured Streaming
    - Parses JSON messages using the canonical trade schema
    - Applies watermarking (5-minute threshold) for late data
    - 1-minute tumbling window → OHLCV aggregation
    - Checkpointing for fault tolerance (separate per sink)
    - Dual-sink: Parquet (append) + PostgreSQL (foreachBatch upsert)

Usage:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \\
        src/etl/spark_streaming.py

    # Or using the provided run script:
    python -m src.etl.spark_streaming
"""

import sys
import uuid
import logging
from pathlib import Path
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

# ── Project imports ──────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import RAW_DATA_DIR, DEFAULT_TICKER
from src.etl.transforms import (
    get_raw_trade_schema,
    validate_trades,
    aggregate_ohlcv_1min,
    add_bronze_metadata,
    add_date_partition_column,
)

# ── Logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "stock-prices-raw"
BRONZE_PATH = str(RAW_DATA_DIR.parent / "bronze" / "stock_prices")
CHECKPOINT_PATH_PARQUET = str(RAW_DATA_DIR.parent / "checkpoints" / "streaming_bronze")
CHECKPOINT_PATH_POSTGRES = str(RAW_DATA_DIR.parent / "checkpoints" / "streaming_postgres")
WATERMARK_THRESHOLD = "5 minutes"
TRIGGER_INTERVAL = "1 minute"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PostgreSQL foreachBatch sink
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _write_to_postgres(batch_df: DataFrame, batch_id: int) -> None:
    """
    foreachBatch callback: convert Spark micro-batch → pandas → PostgreSQL.

    This function is called for each micro-batch. If Postgres is unavailable,
    it logs a WARNING and returns silently — the Parquet sink is unaffected.

    Args:
        batch_df: Spark DataFrame for this micro-batch.
        batch_id: Monotonically increasing batch identifier.
    """
    if batch_df.isEmpty():
        return

    try:
        # Import here to avoid issues if psycopg2 not available
        from src.utils.db_writer import write_df_to_postgres

        # Convert Spark DF → pandas
        pdf = batch_df.toPandas()

        # Select columns matching the stock_prices_stream table schema
        pg_columns = [
            "ticker", "window_start", "window_end",
            "open", "high", "low", "close",
            "volume", "vwap", "trade_count",
        ]
        available = [c for c in pg_columns if c in pdf.columns]
        pdf = pdf[available]

        # Add source_api metadata
        pdf["source_api"] = "finnhub_ws"

        write_df_to_postgres(
            pdf,
            table_name="stock_prices_stream",
            conflict_columns=["ticker", "window_start"],
        )

        logger.info(
            f"[Postgres sink] Batch {batch_id}: {len(pdf)} rows written "
            f"to stock_prices_stream"
        )

    except Exception as e:
        # ── CRITICAL: never let a DB failure crash the streaming job ──────
        # The Bronze Parquet sink must continue running.
        logger.warning(
            f"[Postgres sink] Batch {batch_id} FAILED — DB may be down. "
            f"Parquet sink is unaffected. Error: {e}"
        )


def main() -> None:
    """Run the PySpark Structured Streaming job with dual sinks."""
    
    # Auto-configure packages so 'spark-submit' is not strictly required
    import os
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

    # ── Create Spark session ─────────────────────────────────────────────
    spark = (
        SparkSession.builder
        .appName("StockPrice-Streaming-Bronze")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created — starting streaming job")

    # ── Read from Kafka ──────────────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    logger.info(f"Subscribed to Kafka topic: {KAFKA_TOPIC}")

    # ── Parse JSON from Kafka value ──────────────────────────────────────
    trade_schema = get_raw_trade_schema()

    parsed_df = (
        raw_stream
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.from_json(
                F.col("value").cast("string"),
                trade_schema,
            ).alias("trade"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .select("kafka_key", "kafka_timestamp", "trade.*")
    )

    # ── Add event_time column (from epoch millis) ────────────────────────
    parsed_df = parsed_df.withColumn(
        "event_time",
        (F.col("timestamp_ms") / 1000).cast(TimestampType()),
    )

    # ── Apply watermark for late data handling ───────────────────────────
    # Events arriving more than 5 minutes after the window closes are dropped
    watermarked_df = parsed_df.withWatermark("event_time", WATERMARK_THRESHOLD)

    logger.info(f"Watermark threshold: {WATERMARK_THRESHOLD}")

    # ── Validate trades ──────────────────────────────────────────────────
    valid_df, invalid_df = validate_trades(watermarked_df)

    # Note: in a production system, invalid_df would be written to a
    # separate error/DLQ sink. For now, we filter them out silently.

    # ── Aggregate into 1-minute OHLCV bars ───────────────────────────────
    ohlcv_df = aggregate_ohlcv_1min(valid_df)

    # ── Add Bronze metadata ──────────────────────────────────────────────
    batch_id = f"stream-{uuid.uuid4().hex[:8]}"
    bronze_df = add_bronze_metadata(ohlcv_df, source_api="finnhub_ws", batch_id=batch_id)
    bronze_df = add_date_partition_column(bronze_df, time_col="window_start")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # SINK 1 — Bronze Parquet (existing, unchanged)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    parquet_query = (
        bronze_df.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH_PARQUET)
        .partitionBy("date", "ticker")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName("stock_prices_to_bronze")
        .start()
    )

    logger.info(f"[Sink 1] Parquet → {BRONZE_PATH}")
    logger.info(f"  Checkpoint: {CHECKPOINT_PATH_PARQUET}")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # SINK 2 — PostgreSQL via foreachBatch (NEW)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Uses the OHLCV aggregation (before Bronze metadata is added)
    # so we write clean 1-min bars without the batch_id/date partition columns.
    postgres_query = (
        ohlcv_df.writeStream
        .outputMode("update")
        .foreachBatch(_write_to_postgres)
        .option("checkpointLocation", CHECKPOINT_PATH_POSTGRES)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName("stock_prices_to_postgres")
        .start()
    )

    logger.info(f"[Sink 2] PostgreSQL → stock_prices_stream table")
    logger.info(f"  Checkpoint: {CHECKPOINT_PATH_POSTGRES}")

    # ── Await either query termination ───────────────────────────────────
    logger.info(f"Trigger interval: {TRIGGER_INTERVAL}")
    logger.info("Both sinks running. Waiting for termination (Ctrl+C to stop)…")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
