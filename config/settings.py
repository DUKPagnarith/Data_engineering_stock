"""
settings.py — Central configuration loader for the stock prediction system.

Reads all config from environment variables (via .env file).
Every module imports from here — no hardcoded keys or paths anywhere else.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# ── Load .env from project root ─────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# ── API Keys ─────────────────────────────────────────────────────────────────
FINNHUB_API_KEY: str = os.getenv("FINNHUB_API_KEY", "")

# ── Ticker Config ────────────────────────────────────────────────────────────
DEFAULT_TICKER: str = os.getenv("DEFAULT_TICKER", "AAPL")

# ── Historical Backfill ──────────────────────────────────────────────────────
BACKFILL_YEARS: int = int(os.getenv("BACKFILL_YEARS", "2"))

# ── Data Directories ─────────────────────────────────────────────────────────
RAW_DATA_DIR: Path = PROJECT_ROOT / os.getenv("RAW_DATA_DIR", "data/raw")

# ── Finnhub Websocket URL ────────────────────────────────────────────────────
FINNHUB_WS_URL: str = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"

# ── Finnhub REST base URL ────────────────────────────────────────────────────
FINNHUB_REST_URL: str = "https://finnhub.io/api/v1"

# ── Websocket reconnection settings ─────────────────────────────────────────
WS_RECONNECT_DELAY_SEC: int = 3      # initial delay between reconnect attempts
WS_MAX_RECONNECT_DELAY_SEC: int = 60  # cap for exponential backoff
WS_MAX_RETRIES: int = 10              # give up after this many consecutive failures

# ── PostgreSQL ──────────────────────────────────────────────────────────────
POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5433"))
POSTGRES_DB: str = os.getenv("POSTGRES_DB", "stock_db")
POSTGRES_USER: str = os.getenv("POSTGRES_USER", "stock_user")
POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "stock_password")


def get_postgres_url(host_override: str | None = None) -> str:
    """
    Build a PostgreSQL connection URL for SQLAlchemy.

    Args:
        host_override: If provided, use this host instead of POSTGRES_HOST.
                       Useful when code runs outside Docker (use 'localhost')
                       vs inside Docker (use 'stock_postgres').
    """
    host = host_override or POSTGRES_HOST
    # When running outside Docker, swap container name → localhost
    if host == "stock_postgres":
        host = "localhost"
    return (
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
        f"@{host}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )


def validate() -> None:
    """Raise early if critical config is missing."""
    if not FINNHUB_API_KEY or FINNHUB_API_KEY == "your_finnhub_api_key_here":
        raise EnvironmentError(
            "FINNHUB_API_KEY is not set. "
            "Paste your key into the .env file at the project root."
        )


if __name__ == "__main__":
    # Quick sanity check
    validate()
    print("✓ Settings loaded successfully")
    print(f"  Ticker  : {DEFAULT_TICKER}")
    print(f"  Backfill: {BACKFILL_YEARS} years")
    print(f"  Raw dir : {RAW_DATA_DIR}")
    print(f"  WS URL  : wss://ws.finnhub.io?token=****")
