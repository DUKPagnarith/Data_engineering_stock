"""
gold_to_postgres.py — Batch loader: Gold Parquet → PostgreSQL Star Schema.

Reads all Gold layer Parquet files and inserts them into the corresponding
PostgreSQL tables, respecting foreign key load order:

    dim_ticker → dim_date → dim_market_session → fact_stock_prices

All writes are idempotent (upserts via ON CONFLICT ... DO UPDATE).
Safe to re-run at any time — duplicate rows are updated, not duplicated.

Usage:
    python -m src.warehouse.gold_to_postgres
    python -m src.warehouse.gold_to_postgres --ticker MSFT
"""

import argparse
import sys
import logging
from pathlib import Path

import pandas as pd
import numpy as np

# ── Project imports ──────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import RAW_DATA_DIR, DEFAULT_TICKER
from src.utils.db_writer import get_engine, write_df_to_postgres

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Paths ────────────────────────────────────────────────────────────────────
DATA_DIR = RAW_DATA_DIR.parent
GOLD_DIR = DATA_DIR / "gold"


def load_dim_ticker() -> int:
    """Load dim_ticker from Gold Parquet → PostgreSQL."""
    path = GOLD_DIR / "dim_ticker"
    if not path.exists():
        logger.warning(f"dim_ticker not found at {path} — skipping")
        return 0

    df = pd.read_parquet(path)

    # Align columns to SQL schema
    df = df[["ticker_id", "symbol", "company_name", "sector", "exchange"]]

    return write_df_to_postgres(
        df,
        table_name="dim_ticker",
        conflict_columns=["symbol"],
    )


def load_dim_date() -> int:
    """Load dim_date from Gold Parquet → PostgreSQL."""
    path = GOLD_DIR / "dim_date"
    if not path.exists():
        logger.warning(f"dim_date not found at {path} — skipping")
        return 0

    df = pd.read_parquet(path)

    # Ensure date_id is int (YYYYMMDD format)
    df["date_id"] = df["date_id"].astype(int)

    # Convert date to string for Postgres (handles pandas Timestamp correctly)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # Select columns matching SQL schema
    df = df[[
        "date_id", "date", "day_of_week", "day_name",
        "week", "month", "month_name", "quarter", "year", "is_trading_day",
    ]]

    return write_df_to_postgres(
        df,
        table_name="dim_date",
        conflict_columns=["date_id"],
    )


def load_dim_market_session() -> int:
    """Load dim_market_session from Gold Parquet → PostgreSQL."""
    path = GOLD_DIR / "dim_market_session"
    if not path.exists():
        logger.warning(f"dim_market_session not found at {path} — skipping")
        return 0

    df = pd.read_parquet(path)

    # Convert time columns to string for Postgres TIME type
    for col in ["open_time", "close_time"]:
        if col in df.columns:
            df[col] = df[col].astype(str)

    df = df[["session_id", "session_type", "open_time", "close_time", "description"]]

    return write_df_to_postgres(
        df,
        table_name="dim_market_session",
        conflict_columns=["session_type"],
    )


def load_fact_stock_prices() -> int:
    """Load fact_stock_prices from Gold Parquet → PostgreSQL."""
    path = GOLD_DIR / "fact_stock_prices"
    if not path.exists():
        logger.warning(f"fact_stock_prices not found at {path} — skipping")
        return 0

    df = pd.read_parquet(path)

    # Ensure correct types
    df["ticker_id"] = df["ticker_id"].astype(int)
    df["date_id"] = df["date_id"].astype(int)
    df["volume"] = df["volume"].astype("Int64")  # nullable int

    # Replace NaN with None for Postgres NULL handling
    df = df.replace({np.nan: None})

    # Select columns matching SQL schema (exclude 'id' — auto-generated)
    sql_columns = [
        "ticker_id", "date_id",
        "open", "high", "low", "close", "volume", "vwap",
        "daily_return_pct", "sma_7", "sma_30", "ema_12", "ema_26",
        "macd", "macd_signal", "macd_histogram", "rsi_14",
        "bb_upper", "bb_middle", "bb_lower",
        "volatility_7d", "volume_zscore",
        "is_outlier", "target_next_close",
    ]

    # Keep only columns that exist in both DataFrame and SQL schema
    available = [c for c in sql_columns if c in df.columns]
    df = df[available]

    return write_df_to_postgres(
        df,
        table_name="fact_stock_prices",
        conflict_columns=["ticker_id", "date_id"],
    )


def main(ticker: str = DEFAULT_TICKER) -> None:
    """
    Run the Gold → PostgreSQL batch load.

    Load order respects foreign keys:
        dim_ticker → dim_date → dim_market_session → fact_stock_prices
    """
    ticker = ticker.upper()

    logger.info("=" * 60)
    logger.info("Gold Parquet → PostgreSQL Batch Loader")
    logger.info("=" * 60)

    # Initialise engine (verifies connection)
    get_engine()

    # Load in FK-safe order
    results = {}

    logger.info("\n[1/4] Loading dim_ticker…")
    results["dim_ticker"] = load_dim_ticker()

    logger.info("\n[2/4] Loading dim_date…")
    results["dim_date"] = load_dim_date()

    logger.info("\n[3/4] Loading dim_market_session…")
    results["dim_market_session"] = load_dim_market_session()

    logger.info("\n[4/4] Loading fact_stock_prices…")
    results["fact_stock_prices"] = load_fact_stock_prices()

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("LOAD COMPLETE")
    logger.info("=" * 60)
    for table, count in results.items():
        logger.info(f"  ✓ {table:25s} {count:>5} rows loaded")
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Batch loader: Gold Parquet → PostgreSQL Star Schema"
    )
    parser.add_argument(
        "--ticker", type=str, default=DEFAULT_TICKER,
        help=f"Ticker to load (default: {DEFAULT_TICKER})",
    )
    args = parser.parse_args()
    main(ticker=args.ticker)
