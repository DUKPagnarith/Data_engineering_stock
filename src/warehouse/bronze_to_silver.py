"""
bronze_to_silver.py — Silver layer transformation (Cleaned & Enriched).

Reads raw OHLCV data from the Bronze layer, applies cleaning, deduplication,
outlier detection, schema enforcement, and comprehensive feature engineering,
then writes the enriched dataset to the Silver layer.

Feature engineering includes:
    - SMA_7, SMA_30           (Simple Moving Averages)
    - EMA_12, EMA_26          (Exponential Moving Averages)
    - RSI_14                  (Relative Strength Index)
    - MACD, MACD_signal       (Moving Average Convergence Divergence)
    - Bollinger Bands         (upper, middle, lower)
    - Daily return %          (close-to-close percentage change)
    - Rolling 7-day volatility (standard deviation of daily returns)
    - Volume z-score          (normalised volume relative to 20-day window)

Data quality checks are run after transformation and a quality report is
printed / saved.

Usage:
    spark-submit src/warehouse/bronze_to_silver.py
    spark-submit src/warehouse/bronze_to_silver.py --ticker MSFT
"""

import argparse
import sys
import logging
from pathlib import Path
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

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
DATA_DIR = RAW_DATA_DIR.parent               # data/
BRONZE_PATH = str(DATA_DIR / "bronze" / "stock_prices")
SILVER_PATH = str(DATA_DIR / "silver" / "stock_prices")
QUALITY_REPORT_PATH = str(DATA_DIR / "silver" / "quality_report")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Step 1: Cleaning & Deduplication
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def clean_and_deduplicate(df: DataFrame) -> DataFrame:
    """
    Clean the Bronze data:
        1. Drop rows with null critical fields (ticker, close, volume)
        2. Cast types for safety
        3. Deduplicate by (ticker, window_start) — keep last ingested
        4. Flag outliers (price spikes > 3σ from 20-day rolling mean)
    """
    logger.info("Step 1: Cleaning & deduplication")

    # ── Null handling ────────────────────────────────────────────────────
    df = df.dropna(subset=["ticker", "close", "volume", "window_start"])

    # ── Type enforcement ─────────────────────────────────────────────────
    for col_name in ["open", "high", "low", "close", "vwap"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

    df = df.withColumn("volume", F.col("volume").cast("long"))

    # ── Deduplication by (ticker, window_start) ──────────────────────────
    # If duplicates exist (e.g. from overlapping batch + stream), keep
    # the most recently ingested row.
    dedup_window = Window.partitionBy("ticker", "window_start").orderBy(
        F.col("ingested_at").desc()
    )
    df = (
        df.withColumn("_row_num", F.row_number().over(dedup_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    # ── Outlier detection (3σ price spike flag) ──────────────────────────
    # Rolling 20-day window for mean/stddev of close price
    rolling_20 = (
        Window.partitionBy("ticker")
        .orderBy("window_start")
        .rowsBetween(-20, -1)  # look back 20 rows, exclude current
    )
    df = (
        df.withColumn("_rolling_mean", F.avg("close").over(rolling_20))
        .withColumn("_rolling_std", F.stddev("close").over(rolling_20))
        .withColumn(
            "is_outlier",
            F.when(
                F.col("_rolling_std").isNotNull() & (F.col("_rolling_std") > 0),
                F.abs(F.col("close") - F.col("_rolling_mean"))
                > (3 * F.col("_rolling_std")),
            ).otherwise(False),
        )
        .drop("_rolling_mean", "_rolling_std")
    )

    return df


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Step 2: Feature Engineering
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def add_technical_indicators(df: DataFrame) -> DataFrame:
    """
    Add all required technical indicator features to the DataFrame.

    All computations use pandas via applyInPandas for per-ticker accuracy,
    especially for recursive indicators like EMA and RSI.
    """
    logger.info("Step 2: Feature engineering (technical indicators)")

    import pandas as pd
    import pyspark.sql.types as T

    # All features to be computed inside the pandas UDF
    feature_columns = [
        ("daily_return_pct", DoubleType()),
        ("sma_7", DoubleType()),
        ("sma_30", DoubleType()),
        ("ema_12", DoubleType()),
        ("ema_26", DoubleType()),
        ("macd", DoubleType()),
        ("macd_signal", DoubleType()),
        ("macd_histogram", DoubleType()),
        ("rsi_14", DoubleType()),
        ("bb_middle", DoubleType()),
        ("bb_upper", DoubleType()),
        ("bb_lower", DoubleType()),
        ("volatility_7d", DoubleType()),
        ("volume_zscore", DoubleType()),
    ]

    # Build the output schema: existing columns + new feature columns
    output_fields = list(df.schema.fields)
    existing_names = {f.name for f in output_fields}
    for col_name, col_type in feature_columns:
        if col_name not in existing_names:
            output_fields.append(T.StructField(col_name, col_type, nullable=True))
    output_schema = T.StructType(output_fields)

    def compute_all_features(pdf: pd.DataFrame) -> pd.DataFrame:
        """Compute all technical indicators for a single ticker's data."""
        pdf = pdf.sort_values("window_start").reset_index(drop=True)

        close = pdf["close"]
        volume = pdf["volume"]

        # ── Daily return % ───────────────────────────────────────────
        pdf["daily_return_pct"] = (close.pct_change() * 100).round(4)

        # ── Simple Moving Averages ───────────────────────────────────
        pdf["sma_7"] = close.rolling(7).mean().round(4)
        pdf["sma_30"] = close.rolling(30).mean().round(4)

        # ── Exponential Moving Averages ──────────────────────────────
        pdf["ema_12"] = close.ewm(span=12, adjust=False).mean().round(4)
        pdf["ema_26"] = close.ewm(span=26, adjust=False).mean().round(4)

        # ── MACD ─────────────────────────────────────────────────────
        pdf["macd"] = (pdf["ema_12"] - pdf["ema_26"]).round(4)
        pdf["macd_signal"] = pdf["macd"].ewm(span=9, adjust=False).mean().round(4)
        pdf["macd_histogram"] = (pdf["macd"] - pdf["macd_signal"]).round(4)

        # ── RSI (14-period) ──────────────────────────────────────────
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = (-delta.clip(upper=0))
        avg_gain = gain.ewm(span=14, adjust=False).mean()
        avg_loss = loss.ewm(span=14, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, float("nan"))
        pdf["rsi_14"] = (100 - (100 / (1 + rs))).round(4)

        # ── Bollinger Bands (20-day, 2σ) ─────────────────────────────
        pdf["bb_middle"] = close.rolling(20).mean().round(4)
        bb_std = close.rolling(20).std()
        pdf["bb_upper"] = (pdf["bb_middle"] + 2 * bb_std).round(4)
        pdf["bb_lower"] = (pdf["bb_middle"] - 2 * bb_std).round(4)

        # ── Rolling 7-day volatility ─────────────────────────────────
        daily_ret = close.pct_change()
        pdf["volatility_7d"] = daily_ret.rolling(7).std().round(6)

        # ── Volume z-score (20-day window) ───────────────────────────
        vol_mean = volume.rolling(20).mean()
        vol_std = volume.rolling(20).std()
        pdf["volume_zscore"] = (
            (volume - vol_mean) / vol_std.replace(0, float("nan"))
        ).round(4)

        return pdf

    df = df.groupBy("ticker").applyInPandas(compute_all_features, schema=output_schema)

    return df


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Step 3: Data Quality Checks
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def run_quality_checks(df: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Run data quality checks and produce a quality report.

    Rules:
        1. No null close prices                       (CRITICAL)
        2. No null tickers                            (CRITICAL)
        3. close > 0                                  (CRITICAL)
        4. high >= low                                (WARNING)
        5. volume >= 0                                (WARNING)
        6. SMA_7 is not null for rows after day 7     (INFO)
        7. RSI_14 is in [0, 100] where not null       (WARNING)
        8. Outlier percentage < 5%                    (WARNING)
    """
    logger.info("Step 3: Data quality checks")

    total = df.count()
    if total == 0:
        logger.error("DataFrame is empty — quality check failed")
        return df

    checks = []

    # Rule 1: No null close prices
    null_close = df.filter(F.col("close").isNull()).count()
    checks.append({
        "rule": "no_null_close",
        "severity": "CRITICAL",
        "pass": null_close == 0,
        "violations": null_close,
        "total_rows": total,
    })

    # Rule 2: No null tickers
    null_ticker = df.filter(F.col("ticker").isNull()).count()
    checks.append({
        "rule": "no_null_ticker",
        "severity": "CRITICAL",
        "pass": null_ticker == 0,
        "violations": null_ticker,
        "total_rows": total,
    })

    # Rule 3: close > 0
    neg_close = df.filter(F.col("close") <= 0).count()
    checks.append({
        "rule": "close_positive",
        "severity": "CRITICAL",
        "pass": neg_close == 0,
        "violations": neg_close,
        "total_rows": total,
    })

    # Rule 4: high >= low
    hl_violation = df.filter(F.col("high") < F.col("low")).count()
    checks.append({
        "rule": "high_gte_low",
        "severity": "WARNING",
        "pass": hl_violation == 0,
        "violations": hl_violation,
        "total_rows": total,
    })

    # Rule 5: volume >= 0
    neg_vol = df.filter(F.col("volume") < 0).count()
    checks.append({
        "rule": "volume_non_negative",
        "severity": "WARNING",
        "pass": neg_vol == 0,
        "violations": neg_vol,
        "total_rows": total,
    })

    # Rule 6: RSI in [0, 100]
    rsi_invalid = df.filter(
        F.col("rsi_14").isNotNull()
        & ((F.col("rsi_14") < 0) | (F.col("rsi_14") > 100))
    ).count()
    checks.append({
        "rule": "rsi_in_range",
        "severity": "WARNING",
        "pass": rsi_invalid == 0,
        "violations": rsi_invalid,
        "total_rows": total,
    })

    # Rule 7: Outlier percentage < 5%
    if "is_outlier" in df.columns:
        outlier_count = df.filter(F.col("is_outlier") == True).count()
        outlier_pct = (outlier_count / total) * 100
        checks.append({
            "rule": "outlier_pct_below_5",
            "severity": "WARNING",
            "pass": outlier_pct < 5.0,
            "violations": outlier_count,
            "total_rows": total,
        })

    # ── Print quality report ─────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("DATA QUALITY REPORT")
    logger.info("=" * 60)

    all_passed = True
    critical_failed = False

    for check in checks:
        status = "✓ PASS" if check["pass"] else "✗ FAIL"
        logger.info(
            f"  [{check['severity']:8s}] {status}  {check['rule']:25s}  "
            f"violations: {check['violations']}/{check['total_rows']}"
        )
        if not check["pass"]:
            all_passed = False
            if check["severity"] == "CRITICAL":
                critical_failed = True

    logger.info("=" * 60)
    if all_passed:
        logger.info("Overall: ALL CHECKS PASSED ✓")
    elif critical_failed:
        logger.error("Overall: CRITICAL CHECKS FAILED ✗ — review data before proceeding")
    else:
        logger.warning("Overall: SOME WARNINGS — review but can proceed")
    logger.info("=" * 60)

    # ── Save quality report as Parquet ────────────────────────────────────
    report_df = spark.createDataFrame(checks)
    Path(QUALITY_REPORT_PATH).parent.mkdir(parents=True, exist_ok=True)
    report_df.write.mode("overwrite").json(QUALITY_REPORT_PATH)
    logger.info(f"Quality report saved to {QUALITY_REPORT_PATH}")

    return df


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main pipeline
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def main(ticker: str = DEFAULT_TICKER) -> None:
    """Run the Bronze → Silver transformation pipeline."""

    ticker = ticker.upper()

    # ── Create Spark session ─────────────────────────────────────────────
    spark = (
        SparkSession.builder
        .appName(f"StockPrice-Silver-{ticker}")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Starting Bronze → Silver pipeline for {ticker}")

    # ── Read Bronze layer ────────────────────────────────────────────────
    try:
        bronze_df = spark.read.parquet(BRONZE_PATH)
    except Exception as e:
        logger.error(f"Failed to read Bronze layer at {BRONZE_PATH}: {e}")
        logger.error("Run the batch job first: spark-submit src/etl/spark_batch.py")
        sys.exit(1)

    # Filter to requested ticker
    bronze_df = bronze_df.filter(F.col("ticker") == ticker)
    row_count = bronze_df.count()
    logger.info(f"Read {row_count} Bronze rows for {ticker}")

    if row_count == 0:
        logger.error(f"No data found for ticker {ticker} in Bronze layer")
        sys.exit(1)

    # ── Step 1: Clean & deduplicate ──────────────────────────────────────
    cleaned_df = clean_and_deduplicate(bronze_df)
    clean_count = cleaned_df.count()
    logger.info(f"After cleaning: {clean_count} rows (removed {row_count - clean_count})")

    # ── Step 2: Feature engineering ──────────────────────────────────────
    enriched_df = add_technical_indicators(cleaned_df)

    # ── Step 3: Quality checks ───────────────────────────────────────────
    enriched_df = run_quality_checks(enriched_df, spark)

    # ── Write Silver layer ───────────────────────────────────────────────
    (
        enriched_df.write
        .mode("overwrite")
        .partitionBy("date", "ticker")
        .parquet(SILVER_PATH)
    )

    final_count = enriched_df.count()
    logger.info(f"Wrote {final_count} rows to Silver layer: {SILVER_PATH}")

    # Show sample
    logger.info("Sample Silver data:")
    enriched_df.select(
        "ticker", "window_start", "close", "sma_7", "sma_30",
        "ema_12", "rsi_14", "macd", "bb_upper", "bb_lower",
        "daily_return_pct", "volatility_7d", "volume_zscore", "is_outlier",
    ).show(10, truncate=False)

    spark.stop()
    logger.info("Bronze → Silver pipeline completed ✓")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Bronze → Silver transformation (cleaning + feature engineering)"
    )
    parser.add_argument(
        "--ticker", type=str, default=DEFAULT_TICKER,
        help=f"Ticker to process (default: {DEFAULT_TICKER})",
    )
    args = parser.parse_args()
    main(ticker=args.ticker)
