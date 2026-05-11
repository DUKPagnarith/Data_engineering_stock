"""
models.py — Three stock price prediction models with increasing complexity.

    1. Baseline:      ARIMA / SARIMA (classical, interpretable)
    2. Intermediate:  LightGBM using all engineered features
    3. Advanced:      LSTM (Keras) with sequence length = 30 days

Each model class follows the same interface:
    .train(train_data, val_data)  → trains the model
    .predict(data)                → returns numpy array of predictions
    .get_params()                 → returns dict of hyperparameters

All models are logged to MLflow for experiment tracking.
"""

import sys
import logging
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

# ── Project imports ──────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.training.data_prep import FEATURE_COLS, TARGET_COL, prepare_sequences

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Model 1: ARIMA (Baseline)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class ARIMAModel:
    """
    ARIMA baseline for next-day close price prediction.

    Rationale:
        ARIMA models the time series directly using autoregressive (AR),
        differencing (I), and moving average (MA) components. It's a strong
        classical baseline that captures linear temporal dependencies without
        needing feature engineering. If ARIMA performs well, it suggests the
        series has strong linear autocorrelation.

    Hyperparameters:
        order = (p, d, q) = (5, 1, 2)
        p=5: use 5 past values (weekly seasonality in daily data)
        d=1: first-order differencing (remove trend)
        q=2: 2 MA terms (smooth short-term noise)
    """

    def __init__(self, order=(5, 1, 2)):
        self.order = order
        self.model = None
        self.fitted = None
        self.name = "ARIMA"
        self.train_close = None

    def get_params(self) -> dict:
        return {
            "model_type": "ARIMA",
            "order_p": self.order[0],
            "order_d": self.order[1],
            "order_q": self.order[2],
        }

    def train(self, train_df: pd.DataFrame, val_df: pd.DataFrame = None) -> dict:
        """
        Train ARIMA on the training close price series.

        ARIMA uses only the close price series (univariate).
        """
        from statsmodels.tsa.arima.model import ARIMA

        logger.info(f"Training ARIMA{self.order}…")

        self.train_close = train_df["close"].values

        self.model = ARIMA(self.train_close, order=self.order)
        self.fitted = self.model.fit()

        logger.info(f"ARIMA AIC: {self.fitted.aic:.2f}")

        return {"aic": self.fitted.aic}

    def predict(self, data_df: pd.DataFrame) -> np.ndarray:
        """
        Predict next-day close for each row in data_df.

        Uses a rolling one-step-ahead forecast approach:
        extend the series with each observed close, then forecast one step.
        """
        from statsmodels.tsa.arima.model import ARIMA

        predictions = []
        history = list(self.train_close)

        for i in range(len(data_df)):
            try:
                model = ARIMA(history, order=self.order)
                fitted = model.fit()
                forecast = fitted.forecast(steps=1)[0]
                predictions.append(forecast)
            except Exception:
                # Fallback to last known value
                predictions.append(history[-1])

            # Append actual close to history for next iteration
            history.append(data_df["close"].iloc[i])

        return np.array(predictions)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Model 2: LightGBM (Intermediate)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class LightGBMModel:
    """
    LightGBM gradient boosted trees for next-day close prediction.

    Rationale:
        Tree-based models excel at capturing non-linear relationships
        between features. LightGBM is fast, handles missing values natively,
        and provides feature importance rankings. Using all 19 engineered
        features lets us leverage the Silver layer's technical indicators.

    Hyperparameters:
        n_estimators=500, learning_rate=0.05, max_depth=6,
        num_leaves=31, min_child_samples=20,
        subsample=0.8, colsample_bytree=0.8,
        early_stopping_rounds=50
    """

    def __init__(self):
        self.model = None
        self.name = "LightGBM"
        self.feature_importance = None
        self.params = {
            "n_estimators": 500,
            "learning_rate": 0.05,
            "max_depth": 6,
            "num_leaves": 31,
            "min_child_samples": 20,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "reg_alpha": 0.1,
            "reg_lambda": 0.1,
            "random_state": 42,
            "verbosity": -1,
        }

    def get_params(self) -> dict:
        return {"model_type": "LightGBM", **self.params}

    def train(
        self,
        train_df: pd.DataFrame,
        val_df: pd.DataFrame,
        feature_cols: list[str] = FEATURE_COLS,
    ) -> dict:
        """Train LightGBM with early stopping on validation set."""
        import lightgbm as lgb

        logger.info("Training LightGBM…")

        X_train = train_df[feature_cols]
        y_train = train_df[TARGET_COL]
        X_val = val_df[feature_cols]
        y_val = val_df[TARGET_COL]

        self.model = lgb.LGBMRegressor(**self.params)

        self.model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            callbacks=[
                lgb.early_stopping(stopping_rounds=50),
                lgb.log_evaluation(period=100),
            ],
        )

        # Feature importance
        self.feature_importance = pd.DataFrame({
            "feature": feature_cols,
            "importance": self.model.feature_importances_,
        }).sort_values("importance", ascending=False)

        best_iter = self.model.best_iteration_
        logger.info(f"LightGBM best iteration: {best_iter}")

        return {"best_iteration": best_iter}

    def predict(
        self,
        data_df: pd.DataFrame,
        feature_cols: list[str] = FEATURE_COLS,
    ) -> np.ndarray:
        """Predict next-day close using trained LightGBM."""
        return self.model.predict(data_df[feature_cols])

    def get_feature_importance(self) -> pd.DataFrame:
        """Return feature importance DataFrame sorted by importance."""
        return self.feature_importance


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Model 3: LSTM (Advanced)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class LSTMModel:
    """
    LSTM (Long Short-Term Memory) neural network for sequence prediction.

    Rationale:
        LSTMs are designed for sequential data. They can learn complex
        temporal patterns over the 30-day lookback window, capturing
        non-linear dependencies that ARIMA and tree models miss. The
        gating mechanism handles long-range dependencies and avoids
        vanishing gradients.

    Architecture:
        Input(30, 19) → LSTM(64) → Dropout(0.2) →
        LSTM(32) → Dropout(0.2) → Dense(16, relu) → Dense(1)

    Hyperparameters:
        sequence_length=30, epochs=100, batch_size=32,
        learning_rate=0.001, patience=15 (early stopping)
    """

    def __init__(self, seq_length: int = 30):
        self.seq_length = seq_length
        self.model = None
        self.history = None
        self.name = "LSTM"
        self.params = {
            "seq_length": seq_length,
            "lstm_units_1": 64,
            "lstm_units_2": 32,
            "dropout": 0.2,
            "dense_units": 16,
            "learning_rate": 0.001,
            "epochs": 100,
            "batch_size": 32,
            "patience": 15,
        }

    def get_params(self) -> dict:
        return {"model_type": "LSTM", **self.params}

    def _build_model(self, n_features: int):
        """Build the Keras LSTM model."""
        import tensorflow as tf
        from tensorflow import keras
        from tensorflow.keras import layers

        model = keras.Sequential([
            layers.LSTM(
                self.params["lstm_units_1"],
                return_sequences=True,
                input_shape=(self.seq_length, n_features),
            ),
            layers.Dropout(self.params["dropout"]),
            layers.LSTM(self.params["lstm_units_2"], return_sequences=False),
            layers.Dropout(self.params["dropout"]),
            layers.Dense(self.params["dense_units"], activation="relu"),
            layers.Dense(1),
        ])

        optimizer = keras.optimizers.Adam(learning_rate=self.params["learning_rate"])
        model.compile(optimizer=optimizer, loss="mse", metrics=["mae"])

        return model

    def train(
        self,
        train_df: pd.DataFrame,
        val_df: pd.DataFrame,
        feature_cols: list[str] = FEATURE_COLS,
    ) -> dict:
        """Train LSTM with early stopping and learning rate reduction."""
        from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau

        logger.info(f"Training LSTM (seq_length={self.seq_length})…")

        # Prepare sequences
        X_train, y_train = prepare_sequences(
            train_df, feature_cols, TARGET_COL, self.seq_length
        )
        X_val, y_val = prepare_sequences(
            val_df, feature_cols, TARGET_COL, self.seq_length
        )

        logger.info(f"LSTM train sequences: {X_train.shape}, val: {X_val.shape}")

        if len(X_train) == 0 or len(X_val) == 0:
            logger.warning("Not enough data for LSTM sequences. Reducing seq_length.")
            self.seq_length = min(10, len(train_df) // 2)
            self.params["seq_length"] = self.seq_length
            X_train, y_train = prepare_sequences(
                train_df, feature_cols, TARGET_COL, self.seq_length
            )
            X_val, y_val = prepare_sequences(
                val_df, feature_cols, TARGET_COL, self.seq_length
            )

        n_features = X_train.shape[2]

        # Build model
        self.model = self._build_model(n_features)
        self.model.summary(print_fn=logger.info)

        # Callbacks
        callbacks = [
            EarlyStopping(
                monitor="val_loss",
                patience=self.params["patience"],
                restore_best_weights=True,
                verbose=1,
            ),
            ReduceLROnPlateau(
                monitor="val_loss",
                factor=0.5,
                patience=5,
                min_lr=1e-6,
                verbose=1,
            ),
        ]

        # Train
        self.history = self.model.fit(
            X_train, y_train,
            validation_data=(X_val, y_val),
            epochs=self.params["epochs"],
            batch_size=self.params["batch_size"],
            callbacks=callbacks,
            verbose=1,
        )

        best_val_loss = min(self.history.history["val_loss"])
        logger.info(f"LSTM best val_loss: {best_val_loss:.4f}")

        return {
            "best_val_loss": best_val_loss,
            "epochs_trained": len(self.history.history["loss"]),
            "history": self.history.history,
        }

    def predict(
        self,
        data_df: pd.DataFrame,
        feature_cols: list[str] = FEATURE_COLS,
    ) -> np.ndarray:
        """
        Predict next-day close for each timestep in data_df.

        Returns predictions aligned with data_df rows (first seq_length
        rows will have no prediction, padded with NaN).
        """
        X, _ = prepare_sequences(
            data_df, feature_cols, TARGET_COL, self.seq_length
        )

        if len(X) == 0:
            return np.full(len(data_df), np.nan)

        preds = self.model.predict(X, verbose=0).flatten()

        # Pad with NaN for the first seq_length rows (no prediction available)
        padded = np.full(len(data_df), np.nan)
        padded[self.seq_length:] = preds

        return padded

    def get_training_history(self) -> dict:
        """Return training history for loss curve plotting."""
        if self.history is None:
            return {}
        return self.history.history
