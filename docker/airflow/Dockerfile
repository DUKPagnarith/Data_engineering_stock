# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Custom Airflow image for the Stock Prediction daily training pipeline
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# Base = official Airflow, plus:
#   • a JDK   — PySpark (spark_batch / bronze_to_silver / silver_to_gold) needs Java
#   • the project's Python requirements — so DAG tasks can run `python -m src.*`
#     (yfinance, pyspark, lightgbm, tensorflow, mlflow, sqlalchemy, …)
#
# Build context is the PROJECT ROOT (see docker-compose.yml `build.context: .`)
# so the COPY below can reach ./requirements.txt.
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

FROM apache/airflow:2.10.4-python3.11

# ── System deps (Java for Spark) ────────────────────────────────────────────
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jdk procps \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# default-jdk symlinks the active JVM here on Debian
ENV JAVA_HOME=/usr/lib/jvm/default-java

# ── Project Python deps ─────────────────────────────────────────────────────
# Installed into the airflow user's environment (same interpreter the
# scheduler/worker use), so BashOperator `python -m src.*` resolves everything.
USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
