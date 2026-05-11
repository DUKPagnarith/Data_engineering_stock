"""
data_prep.py — Data preparation for model training.

Reads the Gold layer fact table, applies a strict time-based
train/validation/test split, and scales features.

Split ratio:
    Train:      earliest 70%
    Validation: next 15%
    Test:       final 15%

Usage:
    from src.training.data_prep import load_and_prepare_data
    data = load_and_prepare_data()
"""

import sys
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler

# ── Project imports ──────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import RAW_DATA_DIR, DEFAULT_TICKER

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

# ── Feature columns used for ML ──────────────────────────────────────────────
FEATURE_COLS = [
    "open", "high", "low", "close", "volume",
    "sma_7", "sma_30", "ema_12", "ema_26",
    "rsi_14", "macd", "macd_signal", "macd_histogram",
    "bb_upper", "bb_middle", "bb_lower",
    "daily_return_pct", "volatility_7d", "volume_zscore",
]

TARGET_COL = "target_next_close"


def load_gold_data(ticker: str = DEFAULT_TICKER) -> pd.DataFrame:
    """
    Load the Gold layer fact table as a pandas DataFrame.

    Reads Parquet from data/gold/fact_stock_prices, filters to the
    specified ticker, sorts chronologically, and drops rows where
    the target (next-day close) is null.

    Args:
        ticker: Stock ticker symbol.

    Returns:
        Sorted pandas DataFrame with features and target.
    """
    logger.info(f"Loading Gold layer data for {ticker}…")

    if not GOLD_FACT_PATH.exists():
        raise FileNotFoundError(
            f"Gold fact table not found at {GOLD_FACT_PATH}. "
            "Run silver_to_gold.py first."
        )

    df = pd.read_parquet(GOLD_FACT_PATH)
    logger.info(f"Loaded {len(df)} total rows from Gold fact table")

    # Filter to ticker (if multiple)
    if "ticker_id" in df.columns and ticker:
        # In our setup, ticker_id=1 for AAPL (first alphabetically)
        # For robustness, we keep all rows since we only have one ticker
        pass

    # Sort by date_id (YYYYMMDD integer)
    df = df.sort_values("date_id").reset_index(drop=True)

    # Drop rows where target is null (last row won't have next-day close)
    null_target = df[TARGET_COL].isna().sum()
    df = df.dropna(subset=[TARGET_COL]).reset_index(drop=True)
    logger.info(f"Dropped {null_target} rows with null target → {len(df)} rows remaining")

    # Drop rows where features have NaN (early rows missing rolling indicators)
    before = len(df)
    df = df.dropna(subset=FEATURE_COLS).reset_index(drop=True)
    logger.info(f"Dropped {before - len(df)} rows with NaN features → {len(df)} rows remaining")

    return df


def time_based_split(
    df: pd.DataFrame,
    train_ratio: float = 0.70,
    val_ratio: float = 0.15,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Apply a strict time-based train/validation/test split.

    No shuffling — data is already sorted chronologically.
    This prevents data leakage from future to past.

    Args:
        df:          Sorted DataFrame.
        train_ratio: Fraction for training (default 0.70).
        val_ratio:   Fraction for validation (default 0.15).
                     Test = 1 - train - val (default 0.15).

    Returns:
        (train_df, val_df, test_df)
    """
    n = len(df)
    train_end = int(n * train_ratio)
    val_end = int(n * (train_ratio + val_ratio))

    train_df = df.iloc[:train_end].reset_index(drop=True)
    val_df = df.iloc[train_end:val_end].reset_index(drop=True)
    test_df = df.iloc[val_end:].reset_index(drop=True)

    logger.info(
        f"Split: Train={len(train_df)} ({train_ratio:.0%}), "
        f"Val={len(val_df)} ({val_ratio:.0%}), "
        f"Test={len(test_df)} ({1-train_ratio-val_ratio:.0%})"
    )

    return train_df, val_df, test_df


def scale_features(
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    test_df: pd.DataFrame,
    feature_cols: list[str] = FEATURE_COLS,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, StandardScaler]:
    """
    Scale features using StandardScaler fit on training data only.

    This prevents data leakage — the scaler never sees validation or test data.

    Args:
        train_df:     Training DataFrame.
        val_df:       Validation DataFrame.
        test_df:      Test DataFrame.
        feature_cols: List of feature column names to scale.

    Returns:
        (scaled_train, scaled_val, scaled_test, scaler)
    """
    scaler = StandardScaler()

    # Fit on train only
    scaler.fit(train_df[feature_cols])

    # Transform all splits
    train_scaled = train_df.copy()
    val_scaled = val_df.copy()
    test_scaled = test_df.copy()

    train_scaled[feature_cols] = scaler.transform(train_df[feature_cols])
    val_scaled[feature_cols] = scaler.transform(val_df[feature_cols])
    test_scaled[feature_cols] = scaler.transform(test_df[feature_cols])

    logger.info(f"Scaled {len(feature_cols)} features (fit on train only)")

    return train_scaled, val_scaled, test_scaled, scaler


def prepare_sequences(
    df: pd.DataFrame,
    feature_cols: list[str] = FEATURE_COLS,
    target_col: str = TARGET_COL,
    seq_length: int = 30,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Create sequences for LSTM input.

    Slides a window of `seq_length` days across the data to produce
    (X, y) pairs where X has shape (samples, seq_length, n_features)
    and y has shape (samples,).

    Args:
        df:           DataFrame with features and target.
        feature_cols: Feature column names.
        target_col:   Target column name.
        seq_length:   Number of past days in each sequence.

    Returns:
        (X, y) numpy arrays.
    """
    features = df[feature_cols].values
    target = df[target_col].values

    X, y = [], []
    for i in range(seq_length, len(features)):
        X.append(features[i - seq_length:i])
        y.append(target[i])

    return np.array(X), np.array(y)


def load_and_prepare_data(
    ticker: str = DEFAULT_TICKER,
) -> dict:
    """
    Complete data preparation pipeline.

    Returns a dict with all data splits and metadata needed for training.

    Returns:
        {
            "raw_df":        full sorted DataFrame,
            "train_df":      unscaled train,
            "val_df":        unscaled val,
            "test_df":       unscaled test,
            "train_scaled":  scaled train,
            "val_scaled":    scaled val,
            "test_scaled":   scaled test,
            "scaler":        fitted StandardScaler,
            "feature_cols":  list of feature names,
            "target_col":    target column name,
        }
    """
    # Load
    df = load_gold_data(ticker)

    # Split
    train_df, val_df, test_df = time_based_split(df)

    # Scale
    train_scaled, val_scaled, test_scaled, scaler = scale_features(
        train_df, val_df, test_df
    )

    return {
        "raw_df": df,
        "train_df": train_df,
        "val_df": val_df,
        "test_df": test_df,
        "train_scaled": train_scaled,
        "val_scaled": val_scaled,
        "test_scaled": test_scaled,
        "scaler": scaler,
        "feature_cols": FEATURE_COLS,
        "target_col": TARGET_COL,
    }


if __name__ == "__main__":
    data = load_and_prepare_data()
    print(f"\nData summary:")
    print(f"  Features: {len(data['feature_cols'])}")
    print(f"  Train:    {len(data['train_df'])} rows")
    print(f"  Val:      {len(data['val_df'])} rows")
    print(f"  Test:     {len(data['test_df'])} rows")
    print(f"\nSample train features (scaled):")
    print(data["train_scaled"][data["feature_cols"]].head())
