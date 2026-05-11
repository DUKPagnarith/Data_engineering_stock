"""
evaluate.py — Evaluation metrics for stock price prediction models.

Metrics:
    - MAE   (Mean Absolute Error)
    - RMSE  (Root Mean Squared Error)
    - MAPE  (Mean Absolute Percentage Error)
    - Directional Accuracy (% of up/down moves correctly predicted)
    - Sharpe Ratio of a simple long/short backtest strategy
"""

import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def compute_mae(actual: np.ndarray, predicted: np.ndarray) -> float:
    """Mean Absolute Error."""
    mask = ~np.isnan(predicted)
    return float(np.mean(np.abs(actual[mask] - predicted[mask])))


def compute_rmse(actual: np.ndarray, predicted: np.ndarray) -> float:
    """Root Mean Squared Error."""
    mask = ~np.isnan(predicted)
    return float(np.sqrt(np.mean((actual[mask] - predicted[mask]) ** 2)))


def compute_mape(actual: np.ndarray, predicted: np.ndarray) -> float:
    """Mean Absolute Percentage Error (%)."""
    mask = ~np.isnan(predicted) & (actual != 0)
    return float(np.mean(np.abs((actual[mask] - predicted[mask]) / actual[mask])) * 100)


def compute_directional_accuracy(
    actual: np.ndarray,
    predicted: np.ndarray,
    current_close: np.ndarray,
) -> float:
    """
    Directional Accuracy: % of correctly predicted up/down moves.

    Compares:
        actual move   = actual_next_close - current_close
        predicted move = predicted_next_close - current_close

    Returns percentage of correct direction predictions.
    """
    mask = ~np.isnan(predicted)
    actual_dir = np.sign(actual[mask] - current_close[mask])
    pred_dir = np.sign(predicted[mask] - current_close[mask])

    correct = np.sum(actual_dir == pred_dir)
    total = len(actual_dir)

    return float(correct / total * 100) if total > 0 else 0.0


def compute_sharpe_ratio(
    actual: np.ndarray,
    predicted: np.ndarray,
    current_close: np.ndarray,
    risk_free_rate: float = 0.0,
    annualise: bool = True,
) -> float:
    """
    Sharpe Ratio of a simple long/short backtest strategy.

    Strategy:
        - If predicted_next > current_close → go LONG (buy)
        - If predicted_next < current_close → go SHORT (sell)
        - Return = actual daily return × position direction

    Args:
        actual:         Actual next-day close prices.
        predicted:      Predicted next-day close prices.
        current_close:  Current-day close prices.
        risk_free_rate: Annual risk-free rate (default 0).
        annualise:      If True, annualise the Sharpe ratio (√252 trading days).

    Returns:
        Sharpe ratio (float).
    """
    mask = ~np.isnan(predicted) & (current_close != 0)

    # Position: +1 if predicted up, -1 if predicted down
    position = np.sign(predicted[mask] - current_close[mask])

    # Actual daily return
    actual_return = (actual[mask] - current_close[mask]) / current_close[mask]

    # Strategy return = position × actual return
    strategy_return = position * actual_return

    if len(strategy_return) == 0 or np.std(strategy_return) == 0:
        return 0.0

    daily_rf = risk_free_rate / 252
    excess_return = strategy_return - daily_rf

    sharpe = np.mean(excess_return) / np.std(excess_return)

    if annualise:
        sharpe *= np.sqrt(252)

    return float(sharpe)


def evaluate_model(
    model_name: str,
    actual: np.ndarray,
    predicted: np.ndarray,
    current_close: np.ndarray,
) -> dict:
    """
    Compute all evaluation metrics for a model.

    Args:
        model_name:    Name of the model.
        actual:        Actual next-day close prices.
        predicted:     Predicted next-day close prices.
        current_close: Current-day close prices.

    Returns:
        Dict of metric name → value.
    """
    metrics = {
        "model": model_name,
        "MAE": compute_mae(actual, predicted),
        "RMSE": compute_rmse(actual, predicted),
        "MAPE (%)": compute_mape(actual, predicted),
        "Dir. Accuracy (%)": compute_directional_accuracy(actual, predicted, current_close),
        "Sharpe Ratio": compute_sharpe_ratio(actual, predicted, current_close),
    }

    logger.info(f"  {model_name}:")
    for k, v in metrics.items():
        if k != "model":
            logger.info(f"    {k}: {v:.4f}")

    return metrics


def build_comparison_table(results: list[dict]) -> pd.DataFrame:
    """
    Build a comparison table: model × metric.

    Args:
        results: List of metric dicts from evaluate_model().

    Returns:
        DataFrame with models as rows, metrics as columns.
    """
    df = pd.DataFrame(results)
    df = df.set_index("model")
    return df
