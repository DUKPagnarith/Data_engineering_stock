"""
transforms.py — Shared transformation logic for both streaming and batch paths.

This module implements the Lambda Architecture pattern: a single set of
transformation functions used by both the PySpark Structured Streaming job
(real-time path) and the PySpark batch job (historical path).

This ensures consistent schema validation and aggregation logic regardless
of whether data arrives via Kafka or from a historical backfill file.

Functions:
    get_raw_trade_schema()   → StructType for raw trade events
    validate_trades(df)      → filter valid rows, flag invalids
    aggregate_ohlcv(df)      → 1-minute tumbling window OHLCV aggregation
    add_bronze_metadata(df)  → add ingested_at, source_api, batch_id columns
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    IntegerType,
    ArrayType,
    TimestampType,
)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Schema definitions
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def get_raw_trade_schema() -> StructType:
    """
    Schema for raw trade events as produced by finnhub_producer / kafka_producer.

    Fields:
        ticker       — stock symbol (string)
        price        — trade price (double)
        volume       — trade volume (long)
        timestamp_ms — epoch milliseconds (long)
        timestamp    — ISO-8601 UTC string (string)
        conditions   — trade condition codes (array<int>)
        source       — data source identifier (string)
    """
    return StructType([
        StructField("ticker", StringType(), nullable=False),
        StructField("price", DoubleType(), nullable=False),
        StructField("volume", LongType(), nullable=False),
        StructField("timestamp_ms", LongType(), nullable=False),
        StructField("timestamp", StringType(), nullable=True),
        StructField("conditions", ArrayType(IntegerType()), nullable=True),
        StructField("source", StringType(), nullable=True),
    ])


def get_historical_ohlcv_schema() -> StructType:
    """
    Schema for historical daily OHLCV data from yfinance backfill.

    Fields:
        ticker      — stock symbol (string)
        date        — trading date (timestamp)
        open        — opening price (double)
        high        — daily high (double)
        low         — daily low (double)
        close       — closing price (double)
        volume      — total shares traded (long)
        source      — data source identifier (string)
        ingested_at — UTC ISO-8601 fetch timestamp (string)
    """
    return StructType([
        StructField("ticker", StringType(), nullable=False),
        StructField("date", TimestampType(), nullable=False),
        StructField("open", DoubleType(), nullable=False),
        StructField("high", DoubleType(), nullable=False),
        StructField("low", DoubleType(), nullable=False),
        StructField("close", DoubleType(), nullable=False),
        StructField("volume", LongType(), nullable=False),
        StructField("source", StringType(), nullable=True),
        StructField("ingested_at", StringType(), nullable=True),
    ])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Validation
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def validate_trades(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Validate raw trade events and split into valid/invalid DataFrames.

    Validation rules:
        1. ticker is not null or empty
        2. price > 0
        3. volume >= 0
        4. timestamp_ms > 0

    Args:
        df: Raw trade DataFrame.

    Returns:
        (valid_df, invalid_df) — tuple of valid and invalid DataFrames.
    """
    valid_condition = (
        F.col("ticker").isNotNull()
        & (F.length(F.col("ticker")) > 0)
        & (F.col("price") > 0)
        & (F.col("volume") >= 0)
        & (F.col("timestamp_ms") > 0)
    )

    valid_df = df.filter(valid_condition)
    invalid_df = df.filter(~valid_condition)

    return valid_df, invalid_df


def validate_historical(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Validate historical OHLCV records.

    Validation rules:
        1. ticker is not null or empty
        2. open, high, low, close > 0
        3. high >= low
        4. volume >= 0
        5. date is not null

    Args:
        df: Historical OHLCV DataFrame.

    Returns:
        (valid_df, invalid_df) — tuple of valid and invalid DataFrames.
    """
    valid_condition = (
        F.col("ticker").isNotNull()
        & (F.length(F.col("ticker")) > 0)
        & (F.col("open") > 0)
        & (F.col("high") > 0)
        & (F.col("low") > 0)
        & (F.col("close") > 0)
        & (F.col("high") >= F.col("low"))
        & (F.col("volume") >= 0)
        & F.col("date").isNotNull()
    )

    valid_df = df.filter(valid_condition)
    invalid_df = df.filter(~valid_condition)

    return valid_df, invalid_df


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Aggregation (used by the streaming path)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def aggregate_ohlcv_1min(df: DataFrame) -> DataFrame:
    """
    Aggregate raw trade ticks into 1-minute OHLCV bars using a tumbling window.

    Input: individual trade events with (ticker, price, volume, timestamp_ms).
    Output: 1-minute OHLCV bars with (ticker, window_start, window_end,
            open, high, low, close, volume, vwap, trade_count).

    The 'open' price is the first trade in the window and 'close' is the last,
    determined by timestamp_ms ordering.

    Args:
        df: Validated trade DataFrame with an `event_time` timestamp column.

    Returns:
        Aggregated OHLCV DataFrame.
    """
    # Add event_time as a proper Timestamp column (from epoch millis)
    if "event_time" not in df.columns:
        df = df.withColumn(
            "event_time",
            (F.col("timestamp_ms") / 1000).cast(TimestampType()),
        )

    # Compute VWAP component: price * volume (for weighted average)
    df = df.withColumn("pv", F.col("price") * F.col("volume"))

    # 1-minute tumbling window aggregation
    agg_df = (
        df.groupBy(
            F.col("ticker"),
            F.window(F.col("event_time"), "1 minute"),
        )
        .agg(
            F.first("price").alias("open"),       # first trade price in window
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price").alias("close"),        # last trade price in window
            F.sum("volume").alias("volume"),
            # VWAP = sum(price * volume) / sum(volume)
            F.when(F.sum("volume") > 0, F.sum("pv") / F.sum("volume"))
            .otherwise(F.avg("price"))
            .alias("vwap"),
            F.count("*").alias("trade_count"),
        )
        .select(
            F.col("ticker"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("open"),
            F.col("high"),
            F.col("low"),
            F.col("close"),
            F.col("volume"),
            F.round(F.col("vwap"), 4).alias("vwap"),
            F.col("trade_count"),
        )
    )

    return agg_df


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Bronze layer metadata
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def add_bronze_metadata(
    df: DataFrame,
    source_api: str,
    batch_id: str,
) -> DataFrame:
    """
    Add metadata columns required for the Bronze layer.

    Adds:
        ingested_at  — current UTC timestamp (when this row was written to Bronze)
        source_api   — identifier of the data source (e.g. "finnhub_ws", "yfinance")
        batch_id     — unique identifier for this ingestion batch/micro-batch

    Args:
        df:         Input DataFrame.
        source_api: Source identifier string.
        batch_id:   Batch identifier string.

    Returns:
        DataFrame with metadata columns appended.
    """
    return (
        df.withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_api", F.lit(source_api))
        .withColumn("batch_id", F.lit(batch_id))
    )


def add_date_partition_column(df: DataFrame, time_col: str = "window_start") -> DataFrame:
    """
    Add a `date` partition column derived from a timestamp column.

    This column is used for Parquet/Delta partitioning: partitioned by (date, ticker).

    Args:
        df:       Input DataFrame.
        time_col: Name of the timestamp column to extract the date from.

    Returns:
        DataFrame with a `date` column (DateType) added.
    """
    return df.withColumn("date", F.to_date(F.col(time_col)))
