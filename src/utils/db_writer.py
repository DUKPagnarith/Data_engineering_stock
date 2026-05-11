"""
db_writer.py — Shared PostgreSQL utility for the stock prediction pipeline.

Provides:
    get_engine()          → SQLAlchemy engine (connection-pooled, created once)
    write_df_to_postgres() → Bulk upsert a pandas DataFrame into any table

Used by:
    - gold_to_postgres.py  (batch loader)
    - spark_streaming.py   (foreachBatch sink)
    - api.py               (prediction logging)

All writes are idempotent — uses INSERT ... ON CONFLICT ... DO UPDATE.
"""

import sys
import time
import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ── Project imports ──────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import get_postgres_url

# ── Logging ──────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)

# ── Module-level engine (singleton) ──────────────────────────────────────────
_engine: Engine | None = None


def get_engine(host_override: str | None = None) -> Engine:
    """
    Return a connection-pooled SQLAlchemy engine (singleton).

    Creates the engine on first call and reuses it thereafter.
    Pool settings:
        pool_size=5    — keep 5 connections open
        max_overflow=10 — allow 10 extra under burst load

    Args:
        host_override: Pass 'localhost' when running outside Docker,
                       or 'stock_postgres' when running inside.
    """
    global _engine

    if _engine is None:
        url = get_postgres_url(host_override)
        _engine = create_engine(
            url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,      # verify connections before use
            pool_recycle=3600,        # recycle stale connections after 1h
        )
        logger.info(f"SQLAlchemy engine created → {url.split('@')[1]}")  # log host only, not creds

    return _engine


def write_df_to_postgres(
    df: pd.DataFrame,
    table_name: str,
    conflict_columns: list[str] | None = None,
    max_retries: int = 3,
    engine: Engine | None = None,
) -> int:
    """
    Bulk-upsert a pandas DataFrame into a PostgreSQL table.

    Uses psycopg2's execute_values for fast multi-row inserts, with
    INSERT ... ON CONFLICT ... DO UPDATE for idempotency.

    Args:
        df:               pandas DataFrame to write.
        table_name:       Target PostgreSQL table name.
        conflict_columns: Columns forming the UNIQUE constraint for upserts.
                          If None, plain INSERT is used (e.g. for append-only tables).
        max_retries:      Number of retry attempts on connection failure.
        engine:           SQLAlchemy engine (uses singleton if None).

    Returns:
        Number of rows written.

    Raises:
        Exception: After max_retries failures.
    """
    if df.empty:
        logger.warning(f"Empty DataFrame — nothing to write to {table_name}")
        return 0

    eng = engine or get_engine()
    columns = list(df.columns)
    col_list = ", ".join(columns)
    placeholders = ", ".join([f":{c}" for c in columns])

    # Build the SQL
    if conflict_columns:
        conflict_cols = ", ".join(conflict_columns)
        # Update all non-conflict columns on conflict
        update_cols = [c for c in columns if c not in conflict_columns]
        if update_cols:
            update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
            sql = text(
                f"INSERT INTO {table_name} ({col_list}) "
                f"VALUES ({placeholders}) "
                f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_set}"
            )
        else:
            # All columns are conflict columns — just skip on conflict
            sql = text(
                f"INSERT INTO {table_name} ({col_list}) "
                f"VALUES ({placeholders}) "
                f"ON CONFLICT ({conflict_cols}) DO NOTHING"
            )
    else:
        # Plain insert (append-only, e.g. predictions table)
        sql = text(
            f"INSERT INTO {table_name} ({col_list}) "
            f"VALUES ({placeholders})"
        )

    # Convert DataFrame to list of dicts for parameterised execution
    records = df.where(df.notna(), None).to_dict(orient="records")

    # Retry loop with exponential backoff
    for attempt in range(1, max_retries + 1):
        try:
            with eng.begin() as conn:
                conn.execute(sql, records)

            logger.info(f"✓ Wrote {len(records)} rows to {table_name}")
            return len(records)

        except Exception as e:
            delay = 2 ** attempt
            logger.warning(
                f"DB write to {table_name} failed (attempt {attempt}/{max_retries}): {e}"
            )
            if attempt < max_retries:
                logger.info(f"Retrying in {delay}s…")
                time.sleep(delay)
            else:
                logger.error(
                    f"All {max_retries} attempts to write to {table_name} failed. "
                    f"Last error: {e}"
                )
                raise

    return 0  # unreachable, but satisfies type checker


def read_query(sql: str, engine: Engine | None = None) -> pd.DataFrame:
    """
    Execute a read query and return results as a pandas DataFrame.

    Convenience wrapper used by the API for /history queries.

    Args:
        sql:    SQL query string.
        engine: SQLAlchemy engine (uses singleton if None).

    Returns:
        pandas DataFrame with query results.
    """
    eng = engine or get_engine()
    with eng.connect() as conn:
        return pd.read_sql(text(sql), conn)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Self-test
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    eng = get_engine()
    logger.info("Testing connection…")

    result = read_query("SELECT current_database(), current_user, version();")
    print(result.to_string(index=False))

    # Check tables exist
    tables = read_query(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public' ORDER BY table_name;"
    )
    print(f"\nTables in stock_db:\n{tables.to_string(index=False)}")
    logger.info("db_writer self-test passed ✓")
