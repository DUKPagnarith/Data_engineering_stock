"""
spark_batch.py — PySpark batch job (historical path).

Reads historical OHLCV data from the yfinance backfill (CSV or Parquet),
validates it using the same shared transforms as the streaming job,
and writes to the Bronze layer in the same partitioned format.

This ensures the Bronze layer contains a unified dataset from both
real-time (streaming) and historical (batch) sources — following the
Lambda Architecture pattern.

Usage:
    spark-submit src/etl/spark_batch.py
    spark-submit src/etl/spark_batch.py --ticker MSFT
    spark-submit src/etl/spark_batch.py --input-format csv

    # Or using Python directly (if PySpark is on PATH):
    python -m src.etl.spark_batch
"""

import argparse
import sys
import uuid
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ── Project imports ──────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import RAW_DATA_DIR, DEFAULT_TICKER
from src.etl.transforms import (
    get_historical_ohlcv_schema,
    validate_historical,
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
BRONZE_PATH = str(RAW_DATA_DIR.parent / "bronze" / "stock_prices")


def main(ticker: str = DEFAULT_TICKER, input_format: str = "parquet") -> None:
    """
    Run the PySpark batch ingestion job for historical data.

    Args:
        ticker:       Stock ticker symbol (used to locate the input file).
        input_format: 'parquet' or 'csv' — format of the backfill file.
    """
    ticker = ticker.upper()

    # ── Locate input file ────────────────────────────────────────────────
    if input_format == "parquet":
        input_path = RAW_DATA_DIR / f"historical_{ticker}.parquet"
    else:
        input_path = RAW_DATA_DIR / f"historical_{ticker}.csv"

    if not input_path.exists():
        logger.error(
            f"Input file not found: {input_path}\n"
            f"Run the backfill first: python -m src.ingestion.yfinance_backfill --ticker {ticker}"
        )
        sys.exit(1)

    logger.info(f"Input file: {input_path} ({input_format})")

    # ── Create Spark session ─────────────────────────────────────────────
    spark = (
        SparkSession.builder
        .appName(f"StockPrice-Batch-Bronze-{ticker}")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.legacy.parquet.nanosAsLong", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created — starting batch job")

    # ── Read historical data ─────────────────────────────────────────────
    if input_format == "parquet":
        raw_df = spark.read.parquet(str(input_path))
    else:
        raw_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(str(input_path))
        )

    total_rows = raw_df.count()
    logger.info(f"Loaded {total_rows} rows from {input_path.name}")

    # ── Ensure column names match the expected schema ────────────────────
    # yfinance output columns should already match, but let's be safe
    expected_cols = ["ticker", "date", "open", "high", "low", "close", "volume", "source", "ingested_at"]
    for col in expected_cols:
        if col not in raw_df.columns:
            logger.error(f"Missing expected column: {col}")
            logger.error(f"Available columns: {raw_df.columns}")
            sys.exit(1)

    # Cast types to ensure consistency
    raw_df = (
        raw_df
        .withColumn("open", F.col("open").cast("double"))
        .withColumn("high", F.col("high").cast("double"))
        .withColumn("low", F.col("low").cast("double"))
        .withColumn("close", F.col("close").cast("double"))
        .withColumn("volume", F.col("volume").cast("long"))
        .withColumn("date", F.col("date").cast("timestamp"))
    )

    # ── Validate ─────────────────────────────────────────────────────────
    valid_df, invalid_df = validate_historical(raw_df)

    valid_count = valid_df.count()
    invalid_count = invalid_df.count()
    logger.info(f"Validation: {valid_count} valid, {invalid_count} invalid rows")

    if invalid_count > 0:
        logger.warning("Sample invalid rows:")
        invalid_df.show(5, truncate=False)

    if valid_count == 0:
        logger.error("No valid rows — aborting batch job")
        sys.exit(1)

    # ── Prepare for Bronze layer ─────────────────────────────────────────
    # Rename 'date' to 'window_start' for schema alignment with streaming output
    # Historical daily data: window_start = date, window_end = date + 1 day
    bronze_df = (
        valid_df
        .withColumnRenamed("date", "window_start")
        .withColumn("window_end", F.date_add(F.col("window_start"), 1).cast("timestamp"))
        .withColumn("vwap", F.lit(None).cast("double"))   # not available from daily data
        .withColumn("trade_count", F.lit(None).cast("long"))  # not available
        .select(
            "ticker",
            "window_start",
            "window_end",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "vwap",
            "trade_count",
        )
    )

    # Add Bronze metadata
    batch_id = f"batch-{ticker}-{uuid.uuid4().hex[:8]}"
    bronze_df = add_bronze_metadata(bronze_df, source_api="yfinance", batch_id=batch_id)
    bronze_df = add_date_partition_column(bronze_df, time_col="window_start")

    # ── Write to Bronze layer ────────────────────────────────────────────
    (
        bronze_df.write
        .mode("append")
        .partitionBy("date", "ticker")
        .parquet(BRONZE_PATH)
    )

    final_count = bronze_df.count()
    logger.info(f"Wrote {final_count} rows to Bronze layer: {BRONZE_PATH}")
    logger.info(f"Batch ID: {batch_id}")

    # ── Summary ──────────────────────────────────────────────────────────
    logger.info("Sample output:")
    bronze_df.show(5, truncate=False)

    spark.stop()
    logger.info("Batch job completed successfully ✓")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CLI entry point
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="PySpark batch job — historical OHLCV → Bronze layer"
    )
    parser.add_argument(
        "--ticker",
        type=str,
        default=DEFAULT_TICKER,
        help=f"Stock ticker symbol (default: {DEFAULT_TICKER})",
    )
    parser.add_argument(
        "--input-format",
        type=str,
        choices=["parquet", "csv"],
        default="parquet",
        help="Format of the backfill input file (default: parquet)",
    )
    args = parser.parse_args()

    main(ticker=args.ticker, input_format=args.input_format)
