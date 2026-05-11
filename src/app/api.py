"""
api.py — FastAPI REST API for the stock price prediction system.

Endpoints:
    GET  /predict?ticker=AAPL      → Next-day price prediction from best model
    GET  /history?ticker=AAPL&days=30 → Recent OHLCV from PostgreSQL
    GET  /health                   → Service health check
    POST /retrain                  → Trigger a new training run (async)

The best model (ARIMA) is loaded at startup. Predictions are served
from the trained model in-memory.

/history reads from PostgreSQL (fact_stock_prices JOIN dim_date JOIN dim_ticker).
/predict logs each prediction to the PostgreSQL predictions table.

Usage:
    uvicorn src.app.api:app --host 0.0.0.0 --port 8000 --reload
"""

import sys
import logging
import asyncio
from pathlib import Path
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import Optional

import numpy as np
import pandas as pd
from fastapi import FastAPI, Query, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field

# ── Project imports ──────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import DEFAULT_TICKER, RAW_DATA_DIR

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Paths ────────────────────────────────────────────────────────────────────
DATA_DIR = RAW_DATA_DIR.parent
GOLD_FACT_PATH = DATA_DIR / "gold" / "fact_stock_prices"

# ── Global state ─────────────────────────────────────────────────────────────
_model = None
_model_name = "ARIMA"
_gold_data: Optional[pd.DataFrame] = None
_db_engine = None
_db_available = False
_retrain_in_progress = False
_startup_time: Optional[datetime] = None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Pydantic response models
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class PredictionResponse(BaseModel):
    """Response schema for /predict endpoint."""
    ticker: str = Field(..., description="Stock ticker symbol")
    predicted_next_close: float = Field(..., description="Predicted next-day closing price (USD)")
    current_close: float = Field(..., description="Most recent closing price (USD)")
    predicted_change_pct: float = Field(..., description="Predicted % change from current close")
    direction: str = Field(..., description="Predicted direction: UP or DOWN")
    model: str = Field(..., description="Model used for prediction")
    prediction_date: str = Field(..., description="Date of the prediction (ISO-8601)")
    confidence_note: str = Field(
        default="Predictions are based on historical patterns and should not be used as financial advice.",
        description="Risk disclaimer",
    )


class OHLCVRecord(BaseModel):
    """Single OHLCV record."""
    date_id: int
    open: float
    high: float
    low: float
    close: float
    volume: Optional[int] = None
    sma_7: Optional[float] = None
    sma_30: Optional[float] = None
    rsi_14: Optional[float] = None


class HistoryResponse(BaseModel):
    """Response schema for /history endpoint."""
    ticker: str
    days_returned: int
    data: list[OHLCVRecord]


class HealthResponse(BaseModel):
    """Response schema for /health endpoint."""
    status: str
    model_loaded: bool
    model_name: str
    data_loaded: bool
    data_rows: int
    db_connected: bool
    uptime_seconds: float
    timestamp: str


class RetrainResponse(BaseModel):
    """Response schema for /retrain endpoint."""
    status: str
    message: str


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Startup / Shutdown
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _load_model_and_data():
    """Load the ARIMA model, Gold layer data, and DB engine at startup."""
    global _model, _gold_data, _db_engine, _db_available, _startup_time

    _startup_time = datetime.now(timezone.utc)

    # ── Connect to PostgreSQL ────────────────────────────────────────────
    logger.info("Connecting to PostgreSQL…")
    try:
        from src.utils.db_writer import get_engine
        from sqlalchemy import text as sa_text
        _db_engine = get_engine()
        # Quick connectivity test
        with _db_engine.connect() as conn:
            conn.execute(sa_text("SELECT 1"))
        _db_available = True
        logger.info("PostgreSQL connection established ✓")
    except Exception as e:
        _db_available = False
        logger.warning(f"PostgreSQL unavailable — falling back to Parquet. Error: {e}")

    # ── Load Gold layer data (Parquet fallback + ARIMA training data) ────
    logger.info("Loading Gold layer data…")
    try:
        _gold_data = pd.read_parquet(GOLD_FACT_PATH)
        _gold_data = _gold_data.sort_values("date_id").reset_index(drop=True)
        logger.info(f"Loaded {len(_gold_data)} rows from Gold layer")
    except Exception as e:
        logger.error(f"Failed to load Gold data: {e}")
        _gold_data = pd.DataFrame()

    # ── Train ARIMA on available data ────────────────────────────────────
    logger.info("Training ARIMA model on all available data…")
    try:
        from src.training.models import ARIMAModel

        close_series = _gold_data["close"].dropna().values
        if len(close_series) > 10:
            _model = ARIMAModel(order=(5, 1, 2))
            _model.train(
                pd.DataFrame({"close": close_series}),
                None,
            )
            logger.info(f"ARIMA model trained on {len(close_series)} data points")
        else:
            logger.warning("Not enough data to train ARIMA")
    except Exception as e:
        logger.error(f"Failed to train model: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown lifecycle."""
    _load_model_and_data()
    logger.info("API ready to serve requests")
    yield
    logger.info("API shutting down")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# FastAPI App
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
app = FastAPI(
    title="Stock Price Prediction API",
    description=(
        "REST API for next-day stock price predictions using ARIMA, LightGBM, and LSTM models. "
        "Part of the end-to-end stock prediction data engineering pipeline."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /health
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.get("/health", response_model=HealthResponse, tags=["System"])
async def health_check():
    """Service health check — returns model status, data status, and uptime."""
    now = datetime.now(timezone.utc)
    uptime = (now - _startup_time).total_seconds() if _startup_time else 0

    return HealthResponse(
        status="healthy" if _model is not None else "degraded",
        model_loaded=_model is not None,
        model_name=_model_name,
        data_loaded=_gold_data is not None and len(_gold_data) > 0,
        data_rows=len(_gold_data) if _gold_data is not None else 0,
        db_connected=_db_available,
        uptime_seconds=round(uptime, 1),
        timestamp=now.isoformat(),
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /predict
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.get("/predict", response_model=PredictionResponse, tags=["Predictions"])
async def predict(
    ticker: str = Query(
        default=DEFAULT_TICKER,
        description="Stock ticker symbol (e.g. AAPL)",
        min_length=1,
        max_length=10,
    ),
):
    """
    Get next-day price prediction from the best model (ARIMA).

    Returns the predicted next-day closing price, predicted change %,
    and predicted direction (UP/DOWN).
    """
    ticker = ticker.upper()

    if _model is None:
        raise HTTPException(status_code=503, detail="Model not loaded. Service is starting up.")

    if _gold_data is None or len(_gold_data) == 0:
        raise HTTPException(status_code=503, detail="No data available.")

    # Get the most recent close price
    current_close = float(_gold_data["close"].iloc[-1])

    # Predict using ARIMA (one-step-ahead forecast)
    try:
        from statsmodels.tsa.arima.model import ARIMA

        close_series = _gold_data["close"].dropna().values
        model = ARIMA(close_series, order=(5, 1, 2))
        fitted = model.fit()
        forecast = fitted.forecast(steps=1)[0]
        predicted_close = float(forecast)
    except Exception as e:
        logger.error(f"Prediction failed: {e}")
        raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")

    change_pct = ((predicted_close - current_close) / current_close) * 100
    direction = "UP" if predicted_close > current_close else "DOWN"

    # ── Log prediction to PostgreSQL ─────────────────────────────────────
    if _db_available and _db_engine is not None:
        try:
            from src.utils.db_writer import write_df_to_postgres
            pred_log = pd.DataFrame([{
                "ticker": ticker,
                "predicted_close": round(predicted_close, 2),
                "predicted_direction": direction,
                "model_used": _model_name,
            }])
            write_df_to_postgres(pred_log, "predictions", conflict_columns=None, engine=_db_engine)
            logger.info(f"Prediction logged to DB: {ticker} → ${predicted_close:.2f} ({direction})")
        except Exception as e:
            logger.warning(f"Failed to log prediction to DB: {e}")

    return PredictionResponse(
        ticker=ticker,
        predicted_next_close=round(predicted_close, 2),
        current_close=round(current_close, 2),
        predicted_change_pct=round(change_pct, 4),
        direction=direction,
        model=_model_name,
        prediction_date=datetime.now(timezone.utc).isoformat(),
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /history
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.get("/history", response_model=HistoryResponse, tags=["Data"])
async def get_history(
    ticker: str = Query(
        default=DEFAULT_TICKER,
        description="Stock ticker symbol",
    ),
    days: int = Query(
        default=30,
        ge=1,
        le=500,
        description="Number of recent trading days to return",
    ),
):
    """
    Get recent OHLCV data from PostgreSQL (fact_stock_prices JOIN dim_date + dim_ticker).

    Falls back to Gold layer Parquet if PostgreSQL is unavailable.
    Returns the most recent N trading days of price data including
    select technical indicators (SMA, RSI).
    """
    ticker = ticker.upper()

    # ── Try PostgreSQL first ─────────────────────────────────────────────
    if _db_available and _db_engine is not None:
        try:
            from src.utils.db_writer import read_query

            sql = f"""
                SELECT f.date_id, f.open, f.high, f.low, f.close,
                       f.volume, f.sma_7, f.sma_30, f.rsi_14
                FROM fact_stock_prices f
                JOIN dim_ticker t ON f.ticker_id = t.ticker_id
                WHERE t.symbol = '{ticker}'
                ORDER BY f.date_id DESC
                LIMIT {days}
            """
            recent = read_query(sql, engine=_db_engine)

            if len(recent) > 0:
                # Reverse to chronological order
                recent = recent.iloc[::-1].reset_index(drop=True)

                records = []
                for _, row in recent.iterrows():
                    records.append(OHLCVRecord(
                        date_id=int(row["date_id"]),
                        open=round(float(row["open"]), 2),
                        high=round(float(row["high"]), 2),
                        low=round(float(row["low"]), 2),
                        close=round(float(row["close"]), 2),
                        volume=int(row["volume"]) if pd.notna(row.get("volume")) else None,
                        sma_7=round(float(row["sma_7"]), 2) if pd.notna(row.get("sma_7")) else None,
                        sma_30=round(float(row["sma_30"]), 2) if pd.notna(row.get("sma_30")) else None,
                        rsi_14=round(float(row["rsi_14"]), 2) if pd.notna(row.get("rsi_14")) else None,
                    ))

                logger.info(f"/history served {len(records)} rows from PostgreSQL")
                return HistoryResponse(
                    ticker=ticker,
                    days_returned=len(records),
                    data=records,
                )
        except Exception as e:
            logger.warning(f"PostgreSQL query failed, falling back to Parquet: {e}")

    # ── Fallback: read from Gold Parquet ─────────────────────────────────
    if _gold_data is None or len(_gold_data) == 0:
        raise HTTPException(status_code=503, detail="No data available.")

    recent = _gold_data.tail(days).reset_index(drop=True)

    records = []
    for _, row in recent.iterrows():
        records.append(OHLCVRecord(
            date_id=int(row["date_id"]),
            open=round(float(row["open"]), 2),
            high=round(float(row["high"]), 2),
            low=round(float(row["low"]), 2),
            close=round(float(row["close"]), 2),
            volume=int(row["volume"]) if pd.notna(row.get("volume")) else None,
            sma_7=round(float(row["sma_7"]), 2) if pd.notna(row.get("sma_7")) else None,
            sma_30=round(float(row["sma_30"]), 2) if pd.notna(row.get("sma_30")) else None,
            rsi_14=round(float(row["rsi_14"]), 2) if pd.notna(row.get("rsi_14")) else None,
        ))

    logger.info(f"/history served {len(records)} rows from Parquet (fallback)")
    return HistoryResponse(
        ticker=ticker,
        days_returned=len(records),
        data=records,
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# POST /retrain
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _run_retrain():
    """Background task: re-run the training pipeline."""
    global _retrain_in_progress, _model

    _retrain_in_progress = True
    logger.info("Retraining started…")

    try:
        # Re-load data and retrain ARIMA
        from src.training.models import ARIMAModel

        gold_df = pd.read_parquet(GOLD_FACT_PATH)
        gold_df = gold_df.sort_values("date_id").reset_index(drop=True)

        close_series = gold_df["close"].dropna().values
        new_model = ARIMAModel(order=(5, 1, 2))
        new_model.train(pd.DataFrame({"close": close_series}), None)

        _model = new_model
        logger.info("Retraining completed — model updated ✓")

    except Exception as e:
        logger.error(f"Retraining failed: {e}")

    finally:
        _retrain_in_progress = False


@app.post("/retrain", response_model=RetrainResponse, tags=["System"])
async def retrain(background_tasks: BackgroundTasks):
    """
    Trigger a new training run (async background task).

    The model is retrained in the background. Existing predictions
    continue to be served from the current model until retraining
    completes.
    """
    if _retrain_in_progress:
        return RetrainResponse(
            status="already_running",
            message="A retraining job is already in progress. Please wait.",
        )

    background_tasks.add_task(_run_retrain)

    return RetrainResponse(
        status="started",
        message="Retraining started in the background. The model will update automatically upon completion.",
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Root redirect
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.get("/", tags=["System"])
async def root():
    """Redirect to API docs."""
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/docs")
