"""
train_pipeline.py — Main training orchestrator for Stage 4.

Runs the complete ML pipeline:
    1. Load and prepare data from Gold layer
    2. Train ARIMA, LightGBM, and LSTM models
    3. Generate predictions on test set
    4. Compute evaluation metrics
    5. Produce all visualisations
    6. Log everything to MLflow

Usage:
    python -m src.training.train_pipeline
    python -m src.training.train_pipeline --ticker MSFT --skip-lstm
"""

import argparse
import sys
import logging
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

# ── Project imports ──────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import DEFAULT_TICKER, RAW_DATA_DIR
from src.training.data_prep import load_and_prepare_data, FEATURE_COLS, TARGET_COL
from src.training.models import ARIMAModel, LightGBMModel, LSTMModel
from src.training.evaluate import evaluate_model, build_comparison_table
from src.training.visualize import (
    plot_actual_vs_predicted,
    plot_feature_importance,
    plot_lstm_loss_curve,
    plot_residuals,
    plot_candlestick_with_predictions,
    plot_comparison_table,
)

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore")

# ── Paths ────────────────────────────────────────────────────────────────────
MODEL_DIR = RAW_DATA_DIR.parent / "models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)


def main(ticker: str = DEFAULT_TICKER, skip_lstm: bool = False) -> None:
    """
    Run the complete training pipeline.

    Args:
        ticker:    Stock ticker symbol.
        skip_lstm: If True, skip LSTM training (useful for quick runs).
    """
    logger.info("=" * 70)
    logger.info("STAGE 4 — TRAINING & VISUALIZATION PIPELINE")
    logger.info("=" * 70)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Step 1: Data Preparation
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    logger.info("\n[1/5] Data Preparation")
    logger.info("-" * 40)

    data = load_and_prepare_data(ticker)

    train_df = data["train_df"]
    val_df = data["val_df"]
    test_df = data["test_df"]
    train_scaled = data["train_scaled"]
    val_scaled = data["val_scaled"]
    test_scaled = data["test_scaled"]

    logger.info(f"Train: {len(train_df)}, Val: {len(val_df)}, Test: {len(test_df)}")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Step 2: MLflow Setup
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    logger.info("\n[2/5] MLflow Setup")
    logger.info("-" * 40)

    try:
        import mlflow
        mlflow_available = True

        mlflow_dir = RAW_DATA_DIR.parent / "mlruns"
        mlflow.set_tracking_uri(f"file://{mlflow_dir}")
        mlflow.set_experiment(f"stock_prediction_{ticker}")
        logger.info(f"MLflow tracking URI: file://{mlflow_dir}")
    except ImportError:
        mlflow_available = False
        logger.warning("MLflow not installed — skipping experiment tracking")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Step 3: Train Models
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    logger.info("\n[3/5] Model Training")
    logger.info("-" * 40)

    predictions = {}
    all_metrics = []
    lstm_history = None

    # ── Model 1: ARIMA ───────────────────────────────────────────────────
    logger.info("\n--- Model 1: ARIMA ---")
    arima = ARIMAModel(order=(5, 1, 2))

    if mlflow_available:
        with mlflow.start_run(run_name="ARIMA"):
            mlflow.log_params(arima.get_params())
            train_result = arima.train(train_df, val_df)
            mlflow.log_metric("aic", train_result["aic"])

            # Predict on test set (unscaled — ARIMA uses raw close prices)
            arima_preds = arima.predict(test_df)
            predictions["ARIMA"] = arima_preds

            metrics = evaluate_model(
                "ARIMA",
                test_df[TARGET_COL].values,
                arima_preds,
                test_df["close"].values,
            )
            for k, v in metrics.items():
                if k != "model":
                    mlflow.log_metric(k.replace(" ", "_").replace("(%)", "pct"), v)
            all_metrics.append(metrics)
    else:
        arima.train(train_df, val_df)
        arima_preds = arima.predict(test_df)
        predictions["ARIMA"] = arima_preds
        metrics = evaluate_model(
            "ARIMA", test_df[TARGET_COL].values,
            arima_preds, test_df["close"].values,
        )
        all_metrics.append(metrics)

    # ── Model 2: LightGBM ───────────────────────────────────────────────
    logger.info("\n--- Model 2: LightGBM ---")
    lgbm = LightGBMModel()

    if mlflow_available:
        with mlflow.start_run(run_name="LightGBM"):
            mlflow.log_params(lgbm.get_params())

            # LightGBM uses scaled features
            train_result = lgbm.train(train_scaled, val_scaled, FEATURE_COLS)
            mlflow.log_metric("best_iteration", train_result["best_iteration"])

            lgbm_preds = lgbm.predict(test_scaled, FEATURE_COLS)
            predictions["LightGBM"] = lgbm_preds

            metrics = evaluate_model(
                "LightGBM",
                test_df[TARGET_COL].values,
                lgbm_preds,
                test_df["close"].values,
            )
            for k, v in metrics.items():
                if k != "model":
                    mlflow.log_metric(k.replace(" ", "_").replace("(%)", "pct"), v)
            all_metrics.append(metrics)
    else:
        lgbm.train(train_scaled, val_scaled, FEATURE_COLS)
        lgbm_preds = lgbm.predict(test_scaled, FEATURE_COLS)
        predictions["LightGBM"] = lgbm_preds
        metrics = evaluate_model(
            "LightGBM", test_df[TARGET_COL].values,
            lgbm_preds, test_df["close"].values,
        )
        all_metrics.append(metrics)

    # ── Model 3: LSTM ────────────────────────────────────────────────────
    if not skip_lstm:
        logger.info("\n--- Model 3: LSTM ---")
        lstm = LSTMModel(seq_length=30)

        if mlflow_available:
            with mlflow.start_run(run_name="LSTM"):
                mlflow.log_params(lstm.get_params())

                train_result = lstm.train(train_scaled, val_scaled, FEATURE_COLS)
                lstm_history = train_result.get("history")
                mlflow.log_metric("best_val_loss", train_result["best_val_loss"])
                mlflow.log_metric("epochs_trained", train_result["epochs_trained"])

                lstm_preds = lstm.predict(test_scaled, FEATURE_COLS)
                predictions["LSTM"] = lstm_preds

                metrics = evaluate_model(
                    "LSTM",
                    test_df[TARGET_COL].values,
                    lstm_preds,
                    test_df["close"].values,
                )
                for k, v in metrics.items():
                    if k != "model":
                        mlflow.log_metric(k.replace(" ", "_").replace("(%)", "pct"), v)
                all_metrics.append(metrics)
        else:
            train_result = lstm.train(train_scaled, val_scaled, FEATURE_COLS)
            lstm_history = train_result.get("history")
            lstm_preds = lstm.predict(test_scaled, FEATURE_COLS)
            predictions["LSTM"] = lstm_preds
            metrics = evaluate_model(
                "LSTM", test_df[TARGET_COL].values,
                lstm_preds, test_df["close"].values,
            )
            all_metrics.append(metrics)
    else:
        logger.info("\n--- Skipping LSTM (--skip-lstm flag) ---")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Step 4: Comparison Table
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    logger.info("\n[4/5] Model Comparison")
    logger.info("-" * 40)

    comparison_df = build_comparison_table(all_metrics)
    logger.info(f"\n{comparison_df.to_string()}")

    # Save comparison table as CSV
    comparison_path = RAW_DATA_DIR.parent / "models" / "comparison_table.csv"
    comparison_df.to_csv(comparison_path)
    logger.info(f"Comparison table saved: {comparison_path}")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Step 5: Visualisations
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    logger.info("\n[5/5] Generating Visualisations")
    logger.info("-" * 40)

    # Plot 1: Actual vs Predicted
    plot_actual_vs_predicted(test_df, predictions, TARGET_COL)

    # Plot 2: Feature Importance (LightGBM)
    fi = lgbm.get_feature_importance()
    if fi is not None:
        plot_feature_importance(fi)

    # Plot 3: LSTM Loss Curve
    if lstm_history is not None:
        plot_lstm_loss_curve(lstm_history)

    # Plot 4: Residual Distribution
    plot_residuals(test_df, predictions, TARGET_COL)

    # Plot 5: Candlestick with Predictions
    plot_candlestick_with_predictions(test_df, predictions)

    # Plot 6: Comparison Table
    plot_comparison_table(comparison_df)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Done
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    logger.info("\n" + "=" * 70)
    logger.info("STAGE 4 COMPLETE ✓")
    logger.info(f"Plots saved to: {RAW_DATA_DIR.parent / 'data' / 'plots'}")
    if mlflow_available:
        logger.info(f"MLflow UI: mlflow ui --backend-store-uri file://{RAW_DATA_DIR.parent / 'mlruns'}")
    logger.info("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Stage 4 — Training & Visualization Pipeline"
    )
    parser.add_argument(
        "--ticker", type=str, default=DEFAULT_TICKER,
        help=f"Ticker to train on (default: {DEFAULT_TICKER})",
    )
    parser.add_argument(
        "--skip-lstm", action="store_true",
        help="Skip LSTM training (faster run for testing)",
    )
    args = parser.parse_args()

    main(ticker=args.ticker, skip_lstm=args.skip_lstm)
