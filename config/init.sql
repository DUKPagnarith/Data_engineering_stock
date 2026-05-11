-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- init.sql — PostgreSQL bootstrap script
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
--
-- Runs automatically on FIRST Postgres startup (mounted via docker-compose).
-- Creates:
--   A) "metabase" database   — Metabase internal storage (isolated)
--   B) Star Schema tables    — dim_ticker, dim_date, dim_market_session, fact_stock_prices
--   C) Streaming table       — stock_prices_stream (1-min bars from PySpark)
--   D) Predictions table     — logged by FastAPI on every /predict call
--   E) Performance indexes
--
-- All tables use ON CONFLICT-friendly UNIQUE constraints for idempotent writes.
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

-- ────────────────────────────────────────────────────────────────────────────
-- A) Create Metabase's internal database (separate from our stock data)
-- ────────────────────────────────────────────────────────────────────────────
SELECT 'CREATE DATABASE metabase'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase')\gexec


-- ────────────────────────────────────────────────────────────────────────────
-- B) Star Schema — Dimension Tables
-- ────────────────────────────────────────────────────────────────────────────

-- dim_ticker: one row per stock symbol
CREATE TABLE IF NOT EXISTS dim_ticker (
    ticker_id   SERIAL PRIMARY KEY,
    symbol      VARCHAR(10) UNIQUE NOT NULL,
    company_name VARCHAR(100),
    sector      VARCHAR(50),
    exchange    VARCHAR(20)
);

-- dim_date: continuous calendar (populated by gold_to_postgres.py)
CREATE TABLE IF NOT EXISTS dim_date (
    date_id     INTEGER PRIMARY KEY,           -- YYYYMMDD format
    date        DATE UNIQUE NOT NULL,
    day_of_week INTEGER,
    day_name    VARCHAR(10),
    week        INTEGER,
    month       INTEGER,
    month_name  VARCHAR(10),
    quarter     INTEGER,
    year        INTEGER,
    is_trading_day BOOLEAN
);

-- dim_market_session: pre_market, regular, after_hours
CREATE TABLE IF NOT EXISTS dim_market_session (
    session_id   SERIAL PRIMARY KEY,
    session_type VARCHAR(20) UNIQUE NOT NULL,
    open_time    TIME,
    close_time   TIME,
    description  VARCHAR(100)
);


-- ────────────────────────────────────────────────────────────────────────────
-- B) Star Schema — Fact Table
-- ────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fact_stock_prices (
    id                  SERIAL PRIMARY KEY,
    ticker_id           INTEGER REFERENCES dim_ticker(ticker_id),
    date_id             INTEGER REFERENCES dim_date(date_id),

    -- OHLCV
    open                DOUBLE PRECISION,
    high                DOUBLE PRECISION,
    low                 DOUBLE PRECISION,
    close               DOUBLE PRECISION,
    volume              BIGINT,
    vwap                DOUBLE PRECISION,

    -- Technical indicators (from Silver layer)
    daily_return_pct    DOUBLE PRECISION,
    sma_7               DOUBLE PRECISION,
    sma_30              DOUBLE PRECISION,
    ema_12              DOUBLE PRECISION,
    ema_26              DOUBLE PRECISION,
    macd                DOUBLE PRECISION,
    macd_signal         DOUBLE PRECISION,
    macd_histogram      DOUBLE PRECISION,
    rsi_14              DOUBLE PRECISION,
    bb_upper            DOUBLE PRECISION,
    bb_middle           DOUBLE PRECISION,
    bb_lower            DOUBLE PRECISION,
    volatility_7d       DOUBLE PRECISION,
    volume_zscore       DOUBLE PRECISION,

    -- Flags & target
    is_outlier          BOOLEAN DEFAULT FALSE,
    target_next_close   DOUBLE PRECISION,

    -- Metadata
    ingested_at         TIMESTAMP DEFAULT NOW(),

    -- Idempotent: one row per (ticker, date)
    UNIQUE(ticker_id, date_id)
);


-- ────────────────────────────────────────────────────────────────────────────
-- C) Streaming Table — real-time 1-min bars from PySpark Structured Streaming
-- ────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS stock_prices_stream (
    id              SERIAL PRIMARY KEY,
    ticker          VARCHAR(10) NOT NULL,
    window_start    TIMESTAMP NOT NULL,
    window_end      TIMESTAMP,
    open            DOUBLE PRECISION,
    high            DOUBLE PRECISION,
    low             DOUBLE PRECISION,
    close           DOUBLE PRECISION,
    volume          BIGINT,
    vwap            DOUBLE PRECISION,
    trade_count     INTEGER,
    ingested_at     TIMESTAMP DEFAULT NOW(),
    source_api      VARCHAR(20),

    -- Idempotent: one row per (ticker, window_start)
    UNIQUE(ticker, window_start)
);


-- ────────────────────────────────────────────────────────────────────────────
-- D) Predictions Table — logged by FastAPI on every /predict call
-- ────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS predictions (
    id                  SERIAL PRIMARY KEY,
    ticker              VARCHAR(10) NOT NULL,
    predicted_at        TIMESTAMP DEFAULT NOW(),
    predicted_close     DOUBLE PRECISION,
    predicted_direction VARCHAR(10),
    model_used          VARCHAR(50),
    actual_close        DOUBLE PRECISION       -- filled in later for accuracy tracking
);


-- ────────────────────────────────────────────────────────────────────────────
-- E) Performance Indexes
-- ────────────────────────────────────────────────────────────────────────────

-- Fact table: speed up joins and time-range queries
CREATE INDEX IF NOT EXISTS idx_fact_ticker_date
    ON fact_stock_prices(ticker_id, date_id);

-- Date dimension: speed up date lookups
CREATE INDEX IF NOT EXISTS idx_dim_date_date
    ON dim_date(date);

-- Streaming table: speed up "latest N bars" queries
CREATE INDEX IF NOT EXISTS idx_stream_ticker_window
    ON stock_prices_stream(ticker, window_start DESC);

-- Predictions: speed up "recent predictions for ticker" queries
CREATE INDEX IF NOT EXISTS idx_predictions_ticker_time
    ON predictions(ticker, predicted_at DESC);


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Done. All tables and indexes are ready.
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
