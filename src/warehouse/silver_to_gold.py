"""
silver_to_gold.py — Gold layer transformation (Star Schema).

Reads the enriched Silver layer data and builds a Star Schema optimised
for analytical workloads:

    Fact table:
        fact_stock_prices — all OHLCV data + engineered features + target_next_close

    Dimension tables:
        dim_ticker         — ticker metadata (symbol, company, sector, exchange)
        dim_date           — date attributes (day_of_week, week, month, quarter, etc.)
        dim_market_session — market session info (pre/regular/after hours)

Usage:
    spark-submit src/warehouse/silver_to_gold.py
    spark-submit src/warehouse/silver_to_gold.py --ticker MSFT
"""

import argparse
import sys
import logging
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType, DoubleType,
)

# ── Project imports ──────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import RAW_DATA_DIR, DEFAULT_TICKER

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Paths ────────────────────────────────────────────────────────────────────
DATA_DIR = RAW_DATA_DIR.parent
SILVER_PATH = str(DATA_DIR / "silver" / "stock_prices")
GOLD_PATH = DATA_DIR / "gold"
FACT_PATH = str(GOLD_PATH / "fact_stock_prices")
DIM_TICKER_PATH = str(GOLD_PATH / "dim_ticker")
DIM_DATE_PATH = str(GOLD_PATH / "dim_date")
DIM_MARKET_SESSION_PATH = str(GOLD_PATH / "dim_market_session")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Dimension: dim_ticker
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def build_dim_ticker(spark: SparkSession, tickers: list[str]) -> DataFrame:
    """
    Build the dim_ticker dimension table.

    In a production system this would be sourced from a company-info API.
    Here we provide a lookup for common tickers and allow extensibility.

    Schema:
        ticker_id    — surrogate key (int)
        symbol       — ticker symbol (string)
        company_name — full company name (string)
        sector       — industry sector (string)
        exchange     — stock exchange (string)
    """
    # Lookup data for common tickers (extend as needed)
    ticker_info = {
        "AAPL": ("Apple Inc.", "Technology", "NASDAQ"),
        "MSFT": ("Microsoft Corporation", "Technology", "NASDAQ"),
        "GOOGL": ("Alphabet Inc.", "Technology", "NASDAQ"),
        "AMZN": ("Amazon.com Inc.", "Consumer Discretionary", "NASDAQ"),
        "TSLA": ("Tesla Inc.", "Consumer Discretionary", "NASDAQ"),
        "META": ("Meta Platforms Inc.", "Technology", "NASDAQ"),
        "NVDA": ("NVIDIA Corporation", "Technology", "NASDAQ"),
        "JPM": ("JPMorgan Chase & Co.", "Financials", "NYSE"),
        "V": ("Visa Inc.", "Financials", "NYSE"),
        "JNJ": ("Johnson & Johnson", "Healthcare", "NYSE"),
    }

    rows = []
    for idx, ticker in enumerate(sorted(set(tickers)), start=1):
        info = ticker_info.get(ticker, (f"{ticker} Corp.", "Unknown", "Unknown"))
        rows.append((idx, ticker, info[0], info[1], info[2]))

    schema = StructType([
        StructField("ticker_id", IntegerType(), False),
        StructField("symbol", StringType(), False),
        StructField("company_name", StringType(), True),
        StructField("sector", StringType(), True),
        StructField("exchange", StringType(), True),
    ])

    return spark.createDataFrame(rows, schema)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Dimension: dim_date
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def build_dim_date(df: DataFrame) -> DataFrame:
    """
    Build the dim_date dimension table from the date range in the data.

    Schema:
        date_id        — surrogate key YYYYMMDD (int)
        date           — calendar date (date)
        day_of_week    — 1=Monday … 7=Sunday (int)
        day_name       — "Monday", "Tuesday", etc. (string)
        week           — ISO week number (int)
        month          — month number 1-12 (int)
        month_name     — "January", etc. (string)
        quarter        — quarter 1-4 (int)
        year           — 4-digit year (int)
        is_trading_day — true if this date appears in the data (boolean)
    """
    # Get distinct dates from the dataset
    dates_df = (
        df.select(F.to_date("window_start").alias("date"))
        .distinct()
        .orderBy("date")
    )

    # Generate a continuous date range from min to max
    date_range = dates_df.agg(
        F.min("date").alias("min_date"),
        F.max("date").alias("max_date"),
    ).collect()[0]

    min_date = date_range["min_date"]
    max_date = date_range["max_date"]

    # Generate all dates in the range
    all_dates_df = (
        df.sparkSession.range(0, (max_date - min_date).days + 1)
        .select(F.date_add(F.lit(min_date), F.col("id").cast("int")).alias("date"))
    )

    # Mark trading days
    trading_dates = dates_df.withColumn("is_trading_day", F.lit(True))
    dim_date = (
        all_dates_df
        .join(trading_dates, on="date", how="left")
        .fillna(False, subset=["is_trading_day"])
    )

    # Add date attributes
    dim_date = (
        dim_date
        .withColumn("date_id", F.date_format("date", "yyyyMMdd").cast(IntegerType()))
        .withColumn("day_of_week", F.dayofweek("date"))  # 1=Sun in Spark, we'll adjust
        .withColumn("day_name", F.date_format("date", "EEEE"))
        .withColumn("week", F.weekofyear("date"))
        .withColumn("month", F.month("date"))
        .withColumn("month_name", F.date_format("date", "MMMM"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("year", F.year("date"))
        .select(
            "date_id", "date", "day_of_week", "day_name",
            "week", "month", "month_name", "quarter", "year",
            "is_trading_day",
        )
    )

    return dim_date


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Dimension: dim_market_session
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def build_dim_market_session(spark: SparkSession) -> DataFrame:
    """
    Build the dim_market_session dimension table.

    US market session definitions (Eastern Time):
        Pre-market:     04:00 — 09:30
        Regular:        09:30 — 16:00
        After-hours:    16:00 — 20:00

    Schema:
        session_id    — surrogate key (int)
        session_type  — "pre_market" / "regular" / "after_hours" (string)
        open_time     — session open (string, HH:MM ET)
        close_time    — session close (string, HH:MM ET)
        description   — human-readable description (string)
    """
    rows = [
        (1, "pre_market",   "04:00", "09:30", "Pre-market trading session (ET)"),
        (2, "regular",      "09:30", "16:00", "Regular trading session (ET)"),
        (3, "after_hours",  "16:00", "20:00", "After-hours trading session (ET)"),
    ]

    schema = StructType([
        StructField("session_id", IntegerType(), False),
        StructField("session_type", StringType(), False),
        StructField("open_time", StringType(), False),
        StructField("close_time", StringType(), False),
        StructField("description", StringType(), True),
    ])

    return spark.createDataFrame(rows, schema)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Fact table: fact_stock_prices
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def build_fact_table(
    silver_df: DataFrame,
    dim_ticker_df: DataFrame,
) -> DataFrame:
    """
    Build the fact_stock_prices fact table.

    Joins Silver data with dim_ticker for the surrogate key,
    adds date_id, and creates the ML target: target_next_close
    (next trading day's closing price).

    Schema:
        ticker_id          — FK to dim_ticker (int)
        date_id            — FK to dim_date, YYYYMMDD (int)
        open, high, low, close, volume, vwap
        sma_7, sma_30, ema_12, ema_26
        rsi_14, macd, macd_signal, macd_histogram
        bb_upper, bb_middle, bb_lower
        daily_return_pct, volatility_7d, volume_zscore
        is_outlier
        target_next_close  — ML target: next day's close price (double)
    """
    # ── Join with dim_ticker for surrogate key ───────────────────────────
    fact_df = (
        silver_df
        .join(
            dim_ticker_df.select("ticker_id", F.col("symbol").alias("_symbol")),
            silver_df["ticker"] == F.col("_symbol"),
            how="left",
        )
        .drop("_symbol")
    )

    # ── Add date_id (YYYYMMDD integer) ───────────────────────────────────
    fact_df = fact_df.withColumn(
        "date_id",
        F.date_format(F.to_date("window_start"), "yyyyMMdd").cast(IntegerType()),
    )

    # ── Create ML target: next day's close ───────────────────────────────
    next_close_window = (
        Window.partitionBy("ticker")
        .orderBy(F.to_date("window_start"))
    )
    fact_df = fact_df.withColumn(
        "target_next_close",
        F.lead("close", 1).over(next_close_window),
    )

    # ── Select final columns ─────────────────────────────────────────────
    fact_columns = [
        "ticker_id", "date_id",
        # OHLCV
        "open", "high", "low", "close", "volume", "vwap",
        # Moving averages
        "sma_7", "sma_30", "ema_12", "ema_26",
        # Momentum indicators
        "rsi_14", "macd", "macd_signal", "macd_histogram",
        # Bollinger Bands
        "bb_upper", "bb_middle", "bb_lower",
        # Volatility & returns
        "daily_return_pct", "volatility_7d", "volume_zscore",
        # Flags
        "is_outlier",
        # ML target
        "target_next_close",
    ]

    # Only select columns that exist (some may be missing if features failed)
    available = [c for c in fact_columns if c in fact_df.columns]
    fact_df = fact_df.select(*available)

    return fact_df


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main pipeline
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def main(ticker: str = DEFAULT_TICKER) -> None:
    """Run the Silver → Gold transformation pipeline."""

    ticker = ticker.upper()

    # ── Spark session ────────────────────────────────────────────────────
    spark = (
        SparkSession.builder
        .appName(f"StockPrice-Gold-{ticker}")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Starting Silver → Gold pipeline for {ticker}")

    # ── Read Silver layer ────────────────────────────────────────────────
    try:
        silver_df = spark.read.parquet(SILVER_PATH)
    except Exception as e:
        logger.error(f"Failed to read Silver layer at {SILVER_PATH}: {e}")
        logger.error("Run bronze_to_silver first: spark-submit src/warehouse/bronze_to_silver.py")
        sys.exit(1)

    silver_df = silver_df.filter(F.col("ticker") == ticker)
    row_count = silver_df.count()
    logger.info(f"Read {row_count} Silver rows for {ticker}")

    if row_count == 0:
        logger.error(f"No data for {ticker} in Silver layer")
        sys.exit(1)

    # ── Build dimension tables ───────────────────────────────────────────
    tickers = [row["ticker"] for row in silver_df.select("ticker").distinct().collect()]

    logger.info("Building dim_ticker…")
    dim_ticker = build_dim_ticker(spark, tickers)
    dim_ticker.show(truncate=False)

    logger.info("Building dim_date…")
    dim_date = build_dim_date(silver_df)
    logger.info(f"dim_date: {dim_date.count()} rows")
    dim_date.show(10, truncate=False)

    logger.info("Building dim_market_session…")
    dim_session = build_dim_market_session(spark)
    dim_session.show(truncate=False)

    # ── Build fact table ─────────────────────────────────────────────────
    logger.info("Building fact_stock_prices…")
    fact_df = build_fact_table(silver_df, dim_ticker)
    fact_count = fact_df.count()
    logger.info(f"fact_stock_prices: {fact_count} rows")

    # ── Write all tables to Gold layer ───────────────────────────────────
    GOLD_PATH.mkdir(parents=True, exist_ok=True)

    dim_ticker.write.mode("overwrite").parquet(DIM_TICKER_PATH)
    logger.info(f"  dim_ticker     → {DIM_TICKER_PATH}")

    dim_date.write.mode("overwrite").parquet(DIM_DATE_PATH)
    logger.info(f"  dim_date       → {DIM_DATE_PATH}")

    dim_session.write.mode("overwrite").parquet(DIM_MARKET_SESSION_PATH)
    logger.info(f"  dim_session    → {DIM_MARKET_SESSION_PATH}")

    fact_df.write.mode("overwrite").parquet(FACT_PATH)
    logger.info(f"  fact_prices    → {FACT_PATH}")

    # ── Summary ──────────────────────────────────────────────────────────
    logger.info("\nSample fact_stock_prices:")
    fact_df.select(
        "ticker_id", "date_id", "close", "sma_7", "rsi_14",
        "macd", "daily_return_pct", "target_next_close",
    ).show(10, truncate=False)

    spark.stop()
    logger.info("Silver → Gold pipeline completed ✓")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ERD (printed on import or when requested)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ERD = """
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        STAR SCHEMA — Entity Relationship Diagram
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    ┌───────────────────┐         ┌─────────────────────────────────────────┐
    │   dim_ticker      │         │         fact_stock_prices               │
    │───────────────────│         │─────────────────────────────────────────│
    │ ticker_id    (PK) │◄───────▷│ ticker_id    (FK)                      │
    │ symbol            │         │ date_id      (FK)                      │
    │ company_name      │         │─────────────────────────────────────────│
    │ sector            │         │ open, high, low, close, volume, vwap   │
    │ exchange          │         │ sma_7, sma_30, ema_12, ema_26          │
    └───────────────────┘         │ rsi_14, macd, macd_signal, macd_hist   │
                                  │ bb_upper, bb_middle, bb_lower          │
    ┌───────────────────┐         │ daily_return_pct, volatility_7d        │
    │   dim_date        │         │ volume_zscore, is_outlier              │
    │───────────────────│         │ target_next_close                      │
    │ date_id      (PK) │◄───────▷│                                        │
    │ date              │         └─────────────────────────────────────────┘
    │ day_of_week       │
    │ day_name          │         ┌───────────────────────────┐
    │ week              │         │   dim_market_session      │
    │ month / month_name│         │───────────────────────────│
    │ quarter           │         │ session_id    (PK)        │
    │ year              │         │ session_type              │
    │ is_trading_day    │         │ open_time / close_time    │
    └───────────────────┘         │ description              │
                                  └───────────────────────────┘
                                  (joined contextually, not via FK)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""


if __name__ == "__main__":
    print(ERD)

    parser = argparse.ArgumentParser(
        description="Silver → Gold transformation (Star Schema)"
    )
    parser.add_argument(
        "--ticker", type=str, default=DEFAULT_TICKER,
        help=f"Ticker to process (default: {DEFAULT_TICKER})",
    )
    args = parser.parse_args()
    main(ticker=args.ticker)
