#!/usr/bin/env bash
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Airflow one-shot bootstrap (run by the `airflow-init` service).
#
#   1. Create the `airflow` metadata DB on the shared Postgres if missing.
#      (config/init.sql also creates it, but only on a FIRST Postgres start —
#       this makes it work on an existing/pre-populated postgres_data volume.)
#   2. airflow db migrate   — create/upgrade Airflow's schema.
#   3. Create the admin web user (idempotent).
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
set -euo pipefail

echo "── [1/3] Ensuring 'airflow' metadata database exists ───────────────"
python - <<'PY'
import os, time, psycopg2
host = os.getenv("AIRFLOW_META_PG_HOST", "stock_postgres")
user = os.getenv("POSTGRES_USER", "stock_user")
pw   = os.getenv("POSTGRES_PASSWORD", "stock_password")

for attempt in range(30):
    try:
        con = psycopg2.connect(host=host, port=5432, user=user,
                                password=pw, dbname="postgres")
        break
    except psycopg2.OperationalError:
        print(f"  Postgres not ready yet (attempt {attempt+1}/30)…")
        time.sleep(2)
else:
    raise SystemExit("Could not connect to Postgres to create 'airflow' DB")

con.autocommit = True
cur = con.cursor()
cur.execute("SELECT 1 FROM pg_database WHERE datname = 'airflow'")
if cur.fetchone():
    print("  Database 'airflow' already exists — skipping create")
else:
    cur.execute("CREATE DATABASE airflow")
    print("  Created database 'airflow'")
cur.close()
con.close()
PY

echo "── [2/3] airflow db migrate ────────────────────────────────────────"
airflow db migrate

echo "── [3/3] Creating admin user (idempotent) ─────────────────────────"
airflow users create \
  --username "${_AIRFLOW_WWW_USER_USERNAME:-admin}" \
  --password "${_AIRFLOW_WWW_USER_PASSWORD:-admin}" \
  --firstname Admin --lastname User --role Admin \
  --email admin@stock.local || true

echo "── Airflow init complete ──────────────────────────────────────────"
