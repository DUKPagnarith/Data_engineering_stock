"""
visualize.py — All required plots for Stage 4.

Produces:
    1. Actual vs Predicted price (all models overlaid)
    2. Feature importance chart (LightGBM)
    3. LSTM training/validation loss curve
    4. Residual error distribution
    5. Candlestick chart with predictions overlaid

All plots are saved as PNG to data/plots/.
"""

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# ── Logging ──────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)

# ── Output directory ─────────────────────────────────────────────────────────
PLOTS_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "plots"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)


def _save_plot(fig, filename: str) -> str:
    """Save a figure to PLOTS_DIR and return the path."""
    path = PLOTS_DIR / filename
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    logger.info(f"Saved plot: {path}")
    return str(path)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Plot 1: Actual vs Predicted (all models overlaid)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def plot_actual_vs_predicted(
    test_df: pd.DataFrame,
    predictions: dict[str, np.ndarray],
    target_col: str = "target_next_close",
) -> str:
    """
    Plot actual vs predicted prices on test set, all models overlaid.

    Args:
        test_df:     Test DataFrame with date_id and target.
        predictions: Dict of model_name → predicted values array.
        target_col:  Name of the target column.

    Returns:
        Path to saved plot.
    """
    fig, ax = plt.subplots(figsize=(14, 6))

    dates = pd.to_datetime(test_df["date_id"].astype(str), format="%Y%m%d")
    actual = test_df[target_col].values

    ax.plot(dates, actual, label="Actual", color="#1a1a2e", linewidth=2, zorder=5)

    colors = ["#e94560", "#0f3460", "#16c79a"]
    for (name, preds), color in zip(predictions.items(), colors):
        mask = ~np.isnan(preds)
        ax.plot(
            dates[mask], preds[mask],
            label=name, color=color, linewidth=1.5, alpha=0.8,
        )

    ax.set_title("Actual vs Predicted — Next-Day Close Price (Test Set)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Date")
    ax.set_ylabel("Price (USD)")
    ax.legend(loc="best", fontsize=10)
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    fig.autofmt_xdate()

    return _save_plot(fig, "01_actual_vs_predicted.png")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Plot 2: Feature Importance (LightGBM)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def plot_feature_importance(importance_df: pd.DataFrame, top_n: int = 15) -> str:
    """
    Horizontal bar chart of feature importance from LightGBM.

    Args:
        importance_df: DataFrame with 'feature' and 'importance' columns.
        top_n:         Number of top features to show.

    Returns:
        Path to saved plot.
    """
    top = importance_df.head(top_n).sort_values("importance")

    fig, ax = plt.subplots(figsize=(10, 6))

    colors = plt.cm.viridis(np.linspace(0.3, 0.9, len(top)))
    ax.barh(top["feature"], top["importance"], color=colors)

    ax.set_title("LightGBM Feature Importance (Top 15)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Importance (split count)")
    ax.grid(True, axis="x", alpha=0.3)

    return _save_plot(fig, "02_feature_importance.png")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Plot 3: LSTM Loss Curve
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def plot_lstm_loss_curve(history: dict) -> str:
    """
    Plot LSTM training and validation loss curves.

    Args:
        history: Keras training history dict with 'loss' and 'val_loss'.

    Returns:
        Path to saved plot.
    """
    fig, ax = plt.subplots(figsize=(10, 5))

    epochs = range(1, len(history["loss"]) + 1)
    ax.plot(epochs, history["loss"], label="Training Loss", color="#e94560", linewidth=1.5)
    ax.plot(epochs, history["val_loss"], label="Validation Loss", color="#0f3460", linewidth=1.5)

    ax.set_title("LSTM Training & Validation Loss", fontsize=14, fontweight="bold")
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Loss (MSE)")
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)

    return _save_plot(fig, "03_lstm_loss_curve.png")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Plot 4: Residual Distribution
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def plot_residuals(
    test_df: pd.DataFrame,
    predictions: dict[str, np.ndarray],
    target_col: str = "target_next_close",
) -> str:
    """
    Plot residual (error) distribution for all models.

    Args:
        test_df:     Test DataFrame with target column.
        predictions: Dict of model_name → predicted values array.
        target_col:  Name of the target column.

    Returns:
        Path to saved plot.
    """
    actual = test_df[target_col].values

    fig, axes = plt.subplots(1, len(predictions), figsize=(5 * len(predictions), 5))
    if len(predictions) == 1:
        axes = [axes]

    colors = ["#e94560", "#0f3460", "#16c79a"]

    for ax, (name, preds), color in zip(axes, predictions.items(), colors):
        mask = ~np.isnan(preds)
        residuals = actual[mask] - preds[mask]

        ax.hist(residuals, bins=30, color=color, alpha=0.7, edgecolor="white")
        ax.axvline(0, color="black", linestyle="--", linewidth=1)

        mean_res = np.mean(residuals)
        std_res = np.std(residuals)
        ax.set_title(f"{name}\nμ={mean_res:.2f}, σ={std_res:.2f}", fontsize=12, fontweight="bold")
        ax.set_xlabel("Residual (Actual - Predicted)")
        ax.set_ylabel("Frequency")
        ax.grid(True, alpha=0.3)

    fig.suptitle("Residual Error Distribution (Test Set)", fontsize=14, fontweight="bold", y=1.02)
    fig.tight_layout()

    return _save_plot(fig, "04_residual_distribution.png")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Plot 5: Candlestick with Predictions
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def plot_candlestick_with_predictions(
    test_df: pd.DataFrame,
    predictions: dict[str, np.ndarray],
) -> str:
    """
    Candlestick chart of the test period with model predictions overlaid.

    Uses matplotlib to draw candlesticks (no mplfinance dependency).

    Args:
        test_df:     Test DataFrame with OHLCV data and date_id.
        predictions: Dict of model_name → predicted values array.

    Returns:
        Path to saved plot.
    """
    fig, ax = plt.subplots(figsize=(16, 7))

    dates = pd.to_datetime(test_df["date_id"].astype(str), format="%Y%m%d")
    opens = test_df["open"].values
    highs = test_df["high"].values
    lows = test_df["low"].values
    closes = test_df["close"].values

    # Draw candlesticks
    width = 0.6
    for i in range(len(dates)):
        color = "#16c79a" if closes[i] >= opens[i] else "#e94560"

        # Wick (high-low line)
        ax.plot(
            [mdates.date2num(dates.iloc[i])] * 2,
            [lows[i], highs[i]],
            color=color, linewidth=0.8,
        )

        # Body (open-close rectangle)
        body_bottom = min(opens[i], closes[i])
        body_height = abs(closes[i] - opens[i])
        rect = plt.Rectangle(
            (mdates.date2num(dates.iloc[i]) - width / 2, body_bottom),
            width, body_height,
            facecolor=color, edgecolor=color, linewidth=0.5,
        )
        ax.add_patch(rect)

    # Overlay predictions
    pred_colors = ["#ff6b35", "#004e89", "#7b2d8e"]
    for (name, preds), color in zip(predictions.items(), pred_colors):
        mask = ~np.isnan(preds)
        ax.plot(
            dates[mask], preds[mask],
            label=f"{name} prediction",
            color=color, linewidth=1.5, alpha=0.85, linestyle="--",
        )

    ax.set_title("Candlestick Chart — Test Period with Predictions", fontsize=14, fontweight="bold")
    ax.set_xlabel("Date")
    ax.set_ylabel("Price (USD)")
    ax.legend(loc="best", fontsize=10)
    ax.grid(True, alpha=0.2)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    fig.autofmt_xdate()

    return _save_plot(fig, "05_candlestick_predictions.png")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Plot comparison table as image
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def plot_comparison_table(comparison_df: pd.DataFrame) -> str:
    """
    Render the model comparison table as a styled plot.

    Args:
        comparison_df: DataFrame from evaluate.build_comparison_table().

    Returns:
        Path to saved plot.
    """
    fig, ax = plt.subplots(figsize=(12, 3))
    ax.axis("off")

    table_data = comparison_df.round(4).reset_index()
    table = ax.table(
        cellText=table_data.values,
        colLabels=table_data.columns,
        cellLoc="center",
        loc="center",
    )

    # Style
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.8)

    # Header styling
    for j, col in enumerate(table_data.columns):
        table[0, j].set_facecolor("#1a1a2e")
        table[0, j].set_text_props(color="white", fontweight="bold")

    # Alternate row colors
    for i in range(1, len(table_data) + 1):
        color = "#f0f0f5" if i % 2 == 0 else "white"
        for j in range(len(table_data.columns)):
            table[i, j].set_facecolor(color)

    ax.set_title("Model Comparison — All Metrics", fontsize=14, fontweight="bold", pad=20)

    return _save_plot(fig, "06_comparison_table.png")
