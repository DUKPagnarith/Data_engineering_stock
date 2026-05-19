"""
daily_training_dag.py — Daily batch retraining pipeline (Airflow).

Runs ONCE PER DAY (not streaming, not hourly). This is the BATCH side of the
Lambda Architecture — completely separate from the real-time Kafka/Spark
streaming path, which keeps running independently.

Daily chain (each step is the project's existing CLI module):

    ingest            python -m src.ingestion.yfinance_backfill   (Yahoo → data/raw)
      │
    raw_to_bronze     python -m src.etl.spark_batch               (raw → Bronze)   [Spark]
      │
    bronze_to_silver  python -m src.warehouse.bronze_to_silver    (clean + indicators) [Spark]
      │
    silver_to_gold    python -m src.warehouse.silver_to_gold      (Star Schema)    [Spark]
      │
    gold_to_postgres  python -m src.warehouse.gold_to_postgres    (Gold → Postgres)
      │
    train             python -m src.training.train_pipeline       (ARIMA/LGBM/LSTM + MLflow)

The project source is bind-mounted at /opt/project (see docker-compose.yml),
so artifacts (data/, mlruns/, data/models, data/plots) are written back to the
host and picked up by the host-run FastAPI / Streamlit without a rebuild.

Trigger a one-off run from the UI ("Trigger DAG w/ config") and override the
params below — e.g. set skip_lstm=true for a fast live demo run.
"""

from __future__ import annotations

import os
import pendulum

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

# ── Defaults ────────────────────────────────────────────────────────────────
PROJECT_DIR = "/opt/project"
DEFAULT_TICKER = os.getenv("DEFAULT_TICKER", "AAPL")

# Environment every task needs. POSTGRES_HOST/PORT point at the shared Postgres
# container via the `stockdb` network alias (added in docker-compose.yml).
# Using `stockdb` (not the literal "stock_postgres") deliberately avoids the
# host→localhost swap in config/settings.get_postgres_url().
TASK_ENV = {
    "PYTHONPATH": PROJECT_DIR,
    "PYTHONUNBUFFERED": "1",
    "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "stockdb"),
    "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
    "POSTGRES_DB": os.getenv("POSTGRES_DB", "stock_db"),
    "POSTGRES_USER": os.getenv("POSTGRES_USER", "stock_user"),
    "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "stock_password"),
}

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=2),
}

with DAG(
    dag_id="daily_stock_training",
    description="Daily batch: ingest → bronze → silver → gold → Postgres → train",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",          # once per day
    catchup=False,              # don't backfill historical runs
    max_active_runs=1,          # never overlap daily runs
    default_args=default_args,
    tags=["batch", "training", "daily"],
    params={
        "ticker": Param(DEFAULT_TICKER, type="string", title="Ticker symbol"),
        "years": Param(2, type="integer", title="Years of history to backfill"),
        "skip_lstm": Param(
            False,
            type="boolean",
            title="Skip LSTM (fast run — ARIMA + LightGBM only)",
        ),
    },
) as dag:

    def step(task_id: str, module_cmd: str) -> BashOperator:
        """A pipeline step: cd into the project and run a module CLI."""
        return BashOperator(
            task_id=task_id,
            bash_command=f"cd {PROJECT_DIR} && python -m {module_cmd}",
            env=TASK_ENV,
            append_env=True,   # keep JAVA_HOME etc. from the image
        )

    ingest = step(
        "ingest",
        "src.ingestion.yfinance_backfill "
        "--ticker {{ params.ticker }} --years {{ params.years }}",
    )

    raw_to_bronze = step(
        "raw_to_bronze",
        "src.etl.spark_batch --ticker {{ params.ticker }} --input-format parquet",
    )

    bronze_to_silver = step(
        "bronze_to_silver",
        "src.warehouse.bronze_to_silver --ticker {{ params.ticker }}",
    )

    silver_to_gold = step(
        "silver_to_gold",
        "src.warehouse.silver_to_gold --ticker {{ params.ticker }}",
    )

    gold_to_postgres = step(
        "gold_to_postgres",
        "src.warehouse.gold_to_postgres --ticker {{ params.ticker }}",
    )

    train = step(
        "train",
        "src.training.train_pipeline --ticker {{ params.ticker }} "
        "{{ '--skip-lstm' if params.skip_lstm else '' }}",
    )

    ingest >> raw_to_bronze >> bronze_to_silver >> silver_to_gold >> gold_to_postgres >> train
