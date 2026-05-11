"""
dashboard.py — Streamlit dashboard for the stock price prediction system.

Features:
    - Live price feed (polling the FastAPI every 30 seconds)
    - Candlestick chart of recent prices with prediction overlaid
    - Model performance metrics panel
    - Ticker selector dropdown

Usage:
    streamlit run src/app/dashboard.py
"""

import sys
import time
import logging
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
import streamlit as st
import requests
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ── Project imports ──────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import DEFAULT_TICKER, RAW_DATA_DIR

# ── Constants ────────────────────────────────────────────────────────────────
API_BASE_URL = "http://localhost:8000"
DATA_DIR = RAW_DATA_DIR.parent
POLL_INTERVAL = 30  # seconds
PLOTS_DIR = DATA_DIR / "plots"

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Stock Price Prediction Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ───────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        padding: 20px;
        border-radius: 12px;
        color: white;
        text-align: center;
        margin: 5px;
    }
    .metric-value {
        font-size: 28px;
        font-weight: bold;
        color: #e94560;
    }
    .metric-label {
        font-size: 14px;
        color: #a0a0b0;
        margin-top: 5px;
    }
    .up { color: #16c79a !important; }
    .down { color: #e94560 !important; }
    .stApp { background-color: #0f0f23; }
    h1, h2, h3 { color: #e0e0ff; }
</style>
""", unsafe_allow_html=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Helper functions
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def fetch_prediction(ticker: str) -> dict | None:
    """Fetch prediction from the FastAPI /predict endpoint."""
    try:
        r = requests.get(f"{API_BASE_URL}/predict", params={"ticker": ticker}, timeout=10)
        if r.status_code == 200:
            return r.json()
    except requests.ConnectionError:
        return None
    return None


def fetch_history(ticker: str, days: int = 60) -> pd.DataFrame | None:
    """Fetch historical data from the FastAPI /history endpoint."""
    try:
        r = requests.get(
            f"{API_BASE_URL}/history",
            params={"ticker": ticker, "days": days},
            timeout=10,
        )
        if r.status_code == 200:
            data = r.json()
            if data["data"]:
                return pd.DataFrame(data["data"])
    except requests.ConnectionError:
        return None
    return None


def fetch_health() -> dict | None:
    """Fetch health status from the FastAPI /health endpoint."""
    try:
        r = requests.get(f"{API_BASE_URL}/health", timeout=5)
        if r.status_code == 200:
            return r.json()
    except requests.ConnectionError:
        return None
    return None


def load_local_data(ticker: str) -> pd.DataFrame | None:
    """Fallback: load data directly from Gold layer Parquet."""
    gold_path = DATA_DIR / "gold" / "fact_stock_prices"
    if gold_path.exists():
        df = pd.read_parquet(gold_path)
        return df.sort_values("date_id").reset_index(drop=True)
    return None


def load_comparison_table() -> pd.DataFrame | None:
    """Load the model comparison table from CSV."""
    path = DATA_DIR / "models" / "comparison_table.csv"
    if path.exists():
        return pd.read_csv(path, index_col=0)
    return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Sidebar
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
st.sidebar.title("📈 Stock Predictor")
st.sidebar.markdown("---")

# Ticker selector
ticker = st.sidebar.selectbox(
    "Select Ticker",
    options=["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA"],
    index=0,
)

# Days of history
history_days = st.sidebar.slider("History (days)", 10, 200, 60)

# Auto-refresh toggle
auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", value=False)

# API status
st.sidebar.markdown("---")
st.sidebar.markdown("### System Status")
health = fetch_health()
if health:
    st.sidebar.success(f"API: {health['status']}")
    st.sidebar.info(f"Model: {health['model_name']}")
    st.sidebar.info(f"Data: {health['data_rows']} rows")
else:
    st.sidebar.warning("API offline — using local data")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main content
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
st.title(f"📊 {ticker} Stock Price Prediction")
st.markdown(f"*Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")

# ── Row 1: Prediction metrics ───────────────────────────────────────────────
pred = fetch_prediction(ticker)

col1, col2, col3, col4 = st.columns(4)

if pred:
    direction_class = "up" if pred["direction"] == "UP" else "down"
    direction_emoji = "🟢" if pred["direction"] == "UP" else "🔴"

    with col1:
        st.metric(
            label="Current Close",
            value=f"${pred['current_close']:.2f}",
        )

    with col2:
        st.metric(
            label="Predicted Next Close",
            value=f"${pred['predicted_next_close']:.2f}",
            delta=f"{pred['predicted_change_pct']:.2f}%",
        )

    with col3:
        st.metric(
            label="Direction",
            value=f"{direction_emoji} {pred['direction']}",
        )

    with col4:
        st.metric(
            label="Model",
            value=pred["model"],
        )
else:
    # Fallback: load local data for display
    local_df = load_local_data(ticker)
    if local_df is not None and len(local_df) > 0:
        last_row = local_df.iloc[-1]
        with col1:
            st.metric("Current Close", f"${last_row['close']:.2f}")
        with col2:
            st.info("Start API for live predictions")
        with col3:
            st.info("—")
        with col4:
            st.info("Offline mode")

st.markdown("---")

# ── Row 2: Candlestick chart ────────────────────────────────────────────────
st.subheader("📉 Price Chart")

# Try API first, fallback to local data
history_df = fetch_history(ticker, history_days)
if history_df is None:
    local_df = load_local_data(ticker)
    if local_df is not None:
        history_df = local_df.tail(history_days).reset_index(drop=True)
        # Ensure columns match
        if "date_id" in history_df.columns:
            history_df["date"] = pd.to_datetime(history_df["date_id"].astype(str), format="%Y%m%d")

if history_df is not None and len(history_df) > 0:
    # Create date column if needed
    if "date" not in history_df.columns and "date_id" in history_df.columns:
        history_df["date"] = pd.to_datetime(history_df["date_id"].astype(str), format="%Y%m%d")

    # Build candlestick chart
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.7, 0.3],
        subplot_titles=("", "Volume"),
    )

    # Candlestick
    fig.add_trace(
        go.Candlestick(
            x=history_df["date"] if "date" in history_df.columns else history_df.index,
            open=history_df["open"],
            high=history_df["high"],
            low=history_df["low"],
            close=history_df["close"],
            name="OHLC",
            increasing_line_color="#16c79a",
            decreasing_line_color="#e94560",
        ),
        row=1, col=1,
    )

    # SMA overlays (if available)
    date_col = "date" if "date" in history_df.columns else history_df.index
    if "sma_7" in history_df.columns:
        sma7 = history_df["sma_7"].dropna()
        if len(sma7) > 0:
            fig.add_trace(
                go.Scatter(
                    x=history_df.loc[sma7.index, "date"] if "date" in history_df.columns else sma7.index,
                    y=sma7,
                    name="SMA 7",
                    line=dict(color="#ff6b35", width=1),
                ),
                row=1, col=1,
            )

    if "sma_30" in history_df.columns:
        sma30 = history_df["sma_30"].dropna()
        if len(sma30) > 0:
            fig.add_trace(
                go.Scatter(
                    x=history_df.loc[sma30.index, "date"] if "date" in history_df.columns else sma30.index,
                    y=sma30,
                    name="SMA 30",
                    line=dict(color="#004e89", width=1),
                ),
                row=1, col=1,
            )

    # Prediction line (if available)
    if pred:
        last_date = history_df["date"].iloc[-1] if "date" in history_df.columns else len(history_df) - 1
        fig.add_trace(
            go.Scatter(
                x=[last_date],
                y=[pred["predicted_next_close"]],
                name=f"Prediction ({pred['model']})",
                mode="markers",
                marker=dict(
                    color="#ffd700", size=12, symbol="star",
                    line=dict(color="white", width=1),
                ),
            ),
            row=1, col=1,
        )

    # Volume bars
    if "volume" in history_df.columns:
        colors = ["#16c79a" if c >= o else "#e94560"
                  for c, o in zip(history_df["close"], history_df["open"])]
        fig.add_trace(
            go.Bar(
                x=history_df["date"] if "date" in history_df.columns else history_df.index,
                y=history_df["volume"],
                name="Volume",
                marker_color=colors,
                opacity=0.5,
            ),
            row=2, col=1,
        )

    fig.update_layout(
        template="plotly_dark",
        height=600,
        showlegend=True,
        xaxis_rangeslider_visible=False,
        paper_bgcolor="#0f0f23",
        plot_bgcolor="#1a1a2e",
        font=dict(color="#e0e0ff"),
    )

    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No historical data available. Run the pipeline first.")

# ── Row 3: Model performance ────────────────────────────────────────────────
st.markdown("---")
st.subheader("🏆 Model Performance")

comparison_df = load_comparison_table()
if comparison_df is not None:
    # Style the table
    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.dataframe(
            comparison_df.style
            .highlight_min(subset=["MAE", "RMSE", "MAPE (%)"], color="#16c79a", axis=0)
            .highlight_max(subset=["Dir. Accuracy (%)", "Sharpe Ratio"], color="#16c79a", axis=0)
            .format("{:.4f}"),
            use_container_width=True,
        )

    with col_right:
        # Best model card
        best_model = comparison_df["MAE"].idxmin()
        best_mae = comparison_df.loc[best_model, "MAE"]
        best_sharpe = comparison_df.loc[best_model, "Sharpe Ratio"]

        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Best Model</div>
            <div class="metric-value">{best_model}</div>
            <div class="metric-label">MAE: ${best_mae:.2f} | Sharpe: {best_sharpe:.2f}</div>
        </div>
        """, unsafe_allow_html=True)

    # Show plots if they exist
    plot_files = {
        "Actual vs Predicted": "01_actual_vs_predicted.png",
        "Feature Importance": "02_feature_importance.png",
        "LSTM Loss Curve": "03_lstm_loss_curve.png",
        "Residual Distribution": "04_residual_distribution.png",
    }

    available_plots = {k: v for k, v in plot_files.items() if (PLOTS_DIR / v).exists()}

    if available_plots:
        st.markdown("---")
        st.subheader("📊 Model Visualizations")
        selected_plot = st.selectbox("Select plot", list(available_plots.keys()))
        plot_path = PLOTS_DIR / available_plots[selected_plot]
        st.image(str(plot_path), use_container_width=True)

else:
    st.info("Run the training pipeline first to see model performance metrics.")

# ── Row 4: Grafana alternative note ─────────────────────────────────────────
st.markdown("---")
with st.expander("💡 Production Alternative: Grafana + InfluxDB"):
    st.markdown("""
    For a **production monitoring dashboard**, Grafana + InfluxDB offers several advantages over Streamlit:

    | Aspect | Streamlit | Grafana + InfluxDB |
    |---|---|---|
    | **Real-time streaming** | Polling (30s interval) | Native push-based (sub-second) |
    | **Alerting** | Manual (custom code) | Built-in alert rules (Slack, email, PagerDuty) |
    | **Scalability** | Single process | Horizontally scalable |
    | **Persistence** | Session-based | Persistent dashboards & data retention policies |
    | **Multi-user** | Limited | Full RBAC, teams, organizations |
    | **Time-series optimised** | No | Yes — InfluxDB is purpose-built for time-series |

    **Architecture:**
    ```
    Kafka → Telegraf (consumer) → InfluxDB → Grafana Dashboard
    ```

    InfluxDB stores the OHLCV time series with configurable retention (e.g. 90 days hot, 2 years cold).
    Grafana queries InfluxDB via Flux/InfluxQL and renders live candlestick panels with alerting
    on price anomalies or prediction drift.
    """)

# ── Auto-refresh ────────────────────────────────────────────────────────────
if auto_refresh:
    time.sleep(POLL_INTERVAL)
    st.rerun()
