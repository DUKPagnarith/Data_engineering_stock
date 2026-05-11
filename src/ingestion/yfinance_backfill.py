"""
yfinance_backfill.py — Historical OHLCV backfill using Yahoo Finance.

Downloads daily OHLCV data for a configurable ticker and time range,
then saves it as both CSV and Parquet in data/raw/ for downstream
consumption by the PySpark batch pipeline (Stage 2).

This is the "fallback / batch ingestion" path complementing the
real-time Finnhub websocket producer.

Usage:
    python -m src.ingestion.yfinance_backfill                          # defaults from .env
    python -m src.ingestion.yfinance_backfill --ticker MSFT --years 3  # override
"""

import argparse
import sys
import logging
from datetime import datetime, timezone
from pathlib import Path

import yfinance as yf
import pandas as pd

# ── Project imports ──────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import DEFAULT_TICKER, BACKFILL_YEARS, RAW_DATA_DIR

# ── Logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Core backfill function
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def download_historical_data(
    ticker: str = DEFAULT_TICKER,
    years: int = BACKFILL_YEARS,
    output_dir: Path = RAW_DATA_DIR,
) -> pd.DataFrame:
    """
    Download historical daily OHLCV data from Yahoo Finance.

    Args:
        ticker:     Stock ticker symbol (e.g. "AAPL").
        years:      Number of years of history to fetch.
        output_dir: Directory to write CSV and Parquet outputs.

    Returns:
        pandas DataFrame with the downloaded data.

    Output schema (canonical raw format):
        ticker       — stock symbol (str)
        date         — trading date (datetime)
        open         — opening price (float)
        high         — daily high (float)
        low          — daily low (float)
        close        — adjusted closing price (float)
        volume       — shares traded (int)
        source       — "yfinance" (str)
        ingested_at  — UTC timestamp of when this record was fetched (str)
    """
    ticker = ticker.upper()
    logger.info(f"Downloading {years}-year daily OHLCV for {ticker} from Yahoo Finance…")

    # ── Fetch data ───────────────────────────────────────────────────────
    period = f"{years}y"
    yf_ticker = yf.Ticker(ticker)
    df = yf_ticker.history(period=period, interval="1d", auto_adjust=True)

    if df.empty:
        logger.error(f"No data returned for {ticker}. Check the ticker symbol.")
        sys.exit(1)

    logger.info(f"Received {len(df)} rows  ({df.index.min().date()} → {df.index.max().date()})")

    # ── Normalise column names to our canonical schema ───────────────────
    df = df.reset_index()
    df = df.rename(columns={
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
    })

    # Keep only the columns we need (drop Dividends, Stock Splits, etc.)
    df = df[["date", "open", "high", "low", "close", "volume"]].copy()

    # Make date tz-naive and cast to datetime64[us] for PySpark Parquet compatibility
    if df["date"].dt.tz is not None:
        df["date"] = df["date"].dt.tz_convert(None)
    df["date"] = df["date"].astype("datetime64[us]")

    # Add metadata columns
    df["ticker"] = ticker
    df["source"] = "yfinance"
    df["ingested_at"] = datetime.now(timezone.utc).isoformat()

    # Ensure types
    df["volume"] = df["volume"].astype(int)
    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].round(4)

    # Reorder columns for clarity
    df = df[["ticker", "date", "open", "high", "low", "close", "volume", "source", "ingested_at"]]

    # ── Write outputs ────────────────────────────────────────────────────
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / f"historical_{ticker}.csv"
    parquet_path = output_dir / f"historical_{ticker}.parquet"

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False, engine="pyarrow")

    logger.info(f"Saved CSV     → {csv_path}")
    logger.info(f"Saved Parquet → {parquet_path}")
    logger.info(f"Schema:\n{df.dtypes.to_string()}")

    return df


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CLI entry point
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Historical OHLCV backfill via Yahoo Finance"
    )
    parser.add_argument(
        "--ticker",
        type=str,
        default=DEFAULT_TICKER,
        help=f"Stock ticker symbol (default: {DEFAULT_TICKER})",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=BACKFILL_YEARS,
        help=f"Years of history to download (default: {BACKFILL_YEARS})",
    )
    args = parser.parse_args()

    download_historical_data(ticker=args.ticker, years=args.years)
