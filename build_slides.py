"""
Generates the 10-slide presentation for the Data Engineering Stock project.
Color scheme: dark navy blue header bar, white slide body.
"""

from pptx import Presentation
from pptx.util import Inches, Pt, Emu
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.util import Inches, Pt
from pptx.oxml.ns import qn
from pptx.enum.dml import MSO_THEME_COLOR
import copy
from lxml import etree

# ── Color palette ──────────────────────────────────────────────────────────────
NAVY       = RGBColor(0x0D, 0x2B, 0x55)   # dark navy blue
NAVY_LIGHT = RGBColor(0x1A, 0x3F, 0x7A)   # slightly lighter navy (accents)
WHITE      = RGBColor(0xFF, 0xFF, 0xFF)
DARK_TEXT  = RGBColor(0x1A, 0x1A, 0x2E)   # near-black for body text
ACCENT     = RGBColor(0x4A, 0x90, 0xD9)   # medium blue for table headers
LIGHT_GRAY = RGBColor(0xF2, 0xF4, 0xF8)   # very light gray for alt rows

SLIDE_W = Inches(13.33)
SLIDE_H = Inches(7.5)

# ── Helpers ────────────────────────────────────────────────────────────────────

def new_prs():
    prs = Presentation()
    prs.slide_width  = SLIDE_W
    prs.slide_height = SLIDE_H
    return prs


def blank_slide(prs):
    layout = prs.slide_layouts[6]   # completely blank layout
    return prs.slides.add_slide(layout)


def set_shape_fill(shape, color):
    shape.fill.solid()
    shape.fill.fore_color.rgb = color


def add_rect(slide, left, top, width, height, color):
    shape = slide.shapes.add_shape(
        1,   # MSO_SHAPE_TYPE.RECTANGLE
        left, top, width, height
    )
    shape.line.fill.background()   # no border
    set_shape_fill(shape, color)
    return shape


def add_textbox(slide, text, left, top, width, height,
                font_size=18, bold=False, color=DARK_TEXT,
                align=PP_ALIGN.LEFT, wrap=True, italic=False):
    txBox = slide.shapes.add_textbox(left, top, width, height)
    tf    = txBox.text_frame
    tf.word_wrap = wrap
    p = tf.paragraphs[0]
    p.alignment = align
    run = p.add_run()
    run.text = text
    run.font.size  = Pt(font_size)
    run.font.bold  = bold
    run.font.color.rgb = color
    run.font.italic = italic
    run.font.name  = "Calibri"
    return txBox


def add_header(slide, title_text, subtitle_text=None):
    """Dark navy header bar spanning the full slide width."""
    bar_h = Inches(1.25)
    add_rect(slide, 0, 0, SLIDE_W, bar_h, NAVY)

    # Accent strip at the bottom of the header
    add_rect(slide, 0, bar_h - Inches(0.05), SLIDE_W, Inches(0.05), ACCENT)

    # Title text
    add_textbox(
        slide, title_text,
        Inches(0.4), Inches(0.12),
        Inches(12.5), Inches(0.75),
        font_size=32, bold=True, color=WHITE,
        align=PP_ALIGN.LEFT
    )
    if subtitle_text:
        add_textbox(
            slide, subtitle_text,
            Inches(0.4), Inches(0.82),
            Inches(12.5), Inches(0.38),
            font_size=15, bold=False, color=ACCENT,
            align=PP_ALIGN.LEFT
        )


def add_bullet_block(slide, items, left, top, width, height,
                     font_size=16, indent=False, line_spacing=1.2):
    """Add a list of bullet strings as separate text boxes stacked vertically."""
    from pptx.util import Pt
    line_h = Pt(font_size) * line_spacing * 1.5
    y = top
    for item in items:
        prefix = "    " if indent else ""
        tb = slide.shapes.add_textbox(left, y, width, line_h)
        tf = tb.text_frame
        tf.word_wrap = True
        p = tf.paragraphs[0]
        p.alignment = PP_ALIGN.LEFT
        run = p.add_run()
        run.text = prefix + item
        run.font.size = Pt(font_size)
        run.font.color.rgb = DARK_TEXT
        run.font.name = "Calibri"
        y += line_h


def add_section_label(slide, label, top):
    """Small navy pill label above a section of content."""
    add_textbox(
        slide, label,
        Inches(0.4), top, Inches(4), Inches(0.35),
        font_size=11, bold=True, color=NAVY_LIGHT
    )


def add_divider(slide, top):
    line = slide.shapes.add_shape(1, Inches(0.4), top, Inches(12.5), Inches(0.02))
    line.fill.solid()
    line.fill.fore_color.rgb = ACCENT
    line.line.fill.background()


def add_table_simple(slide, headers, rows, left, top, width, height,
                     font_size=13):
    """Create a styled table."""
    from pptx.util import Pt
    from pptx.enum.text import PP_ALIGN

    cols    = len(headers)
    n_rows  = len(rows) + 1   # +1 for header
    col_w   = width // cols

    table = slide.shapes.add_table(n_rows, cols, left, top, width, height).table

    # Header row
    for ci, hdr in enumerate(headers):
        cell = table.cell(0, ci)
        cell.fill.solid()
        cell.fill.fore_color.rgb = NAVY
        p = cell.text_frame.paragraphs[0]
        p.alignment = PP_ALIGN.CENTER
        run = p.add_run()
        run.text = hdr
        run.font.size  = Pt(font_size)
        run.font.bold  = True
        run.font.color.rgb = WHITE
        run.font.name  = "Calibri"

    # Data rows
    for ri, row in enumerate(rows):
        bg = LIGHT_GRAY if ri % 2 == 0 else WHITE
        for ci, val in enumerate(row):
            cell = table.cell(ri + 1, ci)
            cell.fill.solid()
            cell.fill.fore_color.rgb = bg
            p = cell.text_frame.paragraphs[0]
            p.alignment = PP_ALIGN.CENTER
            run = p.add_run()
            run.text = str(val)
            run.font.size  = Pt(font_size)
            run.font.color.rgb = DARK_TEXT
            run.font.name  = "Calibri"

    return table


# ── Slide builders ─────────────────────────────────────────────────────────────

def slide_01_title(prs):
    s = blank_slide(prs)

    # Full navy background
    add_rect(s, 0, 0, SLIDE_W, SLIDE_H, NAVY)

    # Large white title
    add_textbox(
        s, "End-to-End Stock Price Prediction\nData Pipeline",
        Inches(1.0), Inches(1.6),
        Inches(11.3), Inches(1.8),
        font_size=40, bold=True, color=WHITE,
        align=PP_ALIGN.CENTER
    )

    # Accent divider
    add_rect(s, Inches(3.5), Inches(3.6), Inches(6.3), Inches(0.05), ACCENT)

    # Subtitle
    add_textbox(
        s, "From Real-Time Data Collection to Machine Learning Predictions",
        Inches(1.0), Inches(3.8),
        Inches(11.3), Inches(0.6),
        font_size=18, bold=False, color=ACCENT,
        align=PP_ALIGN.CENTER
    )

    # Bottom meta line
    add_textbox(
        s, "[Your Name]   |   [Student ID]   |   [Course Name]   |   [Date]",
        Inches(1.0), Inches(5.8),
        Inches(11.3), Inches(0.45),
        font_size=14, bold=False, color=RGBColor(0xAA, 0xC4, 0xE8),
        align=PP_ALIGN.CENTER
    )


def slide_02_problem(prs):
    s = blank_slide(prs)
    add_header(s, "Problem Statement", "Why this project was built")

    add_section_label(s, "THE PROBLEM", Inches(1.35))
    bullets_problem = [
        "- Stock prices arrive in real time, but most projects only work with downloaded CSV files",
        "- There is no single unified system that handles live data, cleans it, stores it, and makes predictions",
        "- It is unclear whether complex AI models actually outperform simple ones on small financial datasets",
    ]
    add_bullet_block(s, bullets_problem, Inches(0.5), Inches(1.75), Inches(12.3), Inches(1.6))

    add_divider(s, Inches(3.5))

    add_section_label(s, "PROJECT GOALS", Inches(3.6))
    bullets_goals = [
        "- Build a complete pipeline: from live data collection to serving predictions",
        "- Handle both historical data and live streaming data in the same system",
        "- Train and compare three prediction models to find which one works best",
        "- Display predictions through a web dashboard and a REST API",
    ]
    add_bullet_block(s, bullets_goals, Inches(0.5), Inches(4.0), Inches(12.3), Inches(2.2))


def slide_03_architecture(prs):
    s = blank_slide(prs)
    add_header(s, "System Architecture", "Five-stage Lambda Architecture pipeline")

    headers = ["Stage", "What It Does", "Tools Used"]
    rows = [
        ["1 -- Ingestion",   "Collect stock data (live + historical)",   "yfinance, Finnhub WebSocket"],
        ["2 -- ETL",         "Clean and transform the data",              "Apache Kafka, PySpark"],
        ["3 -- Warehouse",   "Store data in an organized structure",      "PostgreSQL, Parquet"],
        ["4 -- Training",    "Train prediction models",                   "ARIMA, LightGBM, LSTM, MLflow"],
        ["5 -- Serving",     "Show predictions to users",                 "FastAPI, Streamlit"],
    ]
    add_table_simple(s, headers, rows,
                     Inches(0.5), Inches(1.45), Inches(12.3), Inches(3.5), font_size=14)

    add_divider(s, Inches(5.2))
    add_textbox(
        s,
        "Key design choice: both the live path and the historical batch path use the same cleaning logic, "
        "so results are always consistent regardless of the data source.",
        Inches(0.5), Inches(5.3), Inches(12.3), Inches(0.9),
        font_size=14, italic=True, color=NAVY_LIGHT
    )


def slide_04_ingestion(prs):
    s = blank_slide(prs)
    add_header(s, "Stage 1 -- Data Ingestion", "Collecting stock data from two sources")

    # Left column
    add_section_label(s, "LIVE DATA (REAL-TIME)", Inches(1.35))
    live_items = [
        "- Connects to Finnhub stock market feed during US trading hours",
        "- Receives trade events every second: price, volume, and timestamp",
        "- Bad or missing records are saved separately for review (Dead Letter Queue)",
    ]
    add_bullet_block(s, live_items, Inches(0.5), Inches(1.75), Inches(5.8), Inches(1.8), font_size=15)

    # Vertical divider
    add_rect(s, Inches(6.55), Inches(1.35), Inches(0.04), Inches(3.8), ACCENT)

    # Right column
    add_section_label(s, "HISTORICAL DATA (BATCH)", Inches(1.35))
    hist_items = [
        "- Downloads 2 years of daily AAPL stock prices from Yahoo Finance",
        "- Each row contains: open, high, low, close, and volume for that day",
        "- Saved as a Parquet file for fast processing later",
    ]
    add_bullet_block(s, hist_items, Inches(6.75), Inches(1.75), Inches(6.0), Inches(1.8), font_size=15)

    add_divider(s, Inches(3.6))

    add_section_label(s, "OUTPUT", Inches(3.7))
    add_textbox(
        s, "501 rows of AAPL daily OHLCV data covering approximately 2 years of trading days",
        Inches(0.5), Inches(4.1), Inches(12.3), Inches(0.5),
        font_size=15, bold=True, color=NAVY
    )


def slide_05_etl(prs):
    s = blank_slide(prs)
    add_header(s, "Stage 2 -- ETL Pipeline", "Extract, Transform, Load: moving and cleaning the data")

    add_section_label(s, "APACHE KAFKA (MESSAGE QUEUE)", Inches(1.35))
    kafka_items = [
        "- Acts as a waiting room for incoming live data",
        "- Stores messages for 7 days so nothing is lost if the system crashes",
        "- Separates data collection from data processing (loose coupling)",
    ]
    add_bullet_block(s, kafka_items, Inches(0.5), Inches(1.75), Inches(12.3), Inches(1.2), font_size=15)

    add_divider(s, Inches(3.1))

    add_section_label(s, "PYSPARK (PROCESSING ENGINE)", Inches(3.2))
    spark_items = [
        "- Live path: groups raw trade ticks into 1-minute price bars (open, high, low, close, volume)",
        "- Handles data arriving up to 5 minutes late without losing it",
        "- Batch path: processes the 2-year historical file using the exact same logic",
        "- Saves results to both Parquet files and PostgreSQL at the same time",
    ]
    add_bullet_block(s, spark_items, Inches(0.5), Inches(3.6), Inches(12.3), Inches(2.0), font_size=15)

    add_divider(s, Inches(5.85))
    add_textbox(
        s,
        "Both paths write to the same Bronze storage layer, so the data format is always identical.",
        Inches(0.5), Inches(5.95), Inches(12.3), Inches(0.45),
        font_size=14, italic=True, color=NAVY_LIGHT
    )


def slide_06_warehouse(prs):
    s = blank_slide(prs)
    add_header(s, "Stage 3 -- Medallion Data Warehouse", "Three quality layers: Bronze, Silver, Gold")

    layers = [
        ("BRONZE  --  Raw Storage",
         ["Data stored exactly as received, nothing changed",
          "Acts as a permanent backup -- any error downstream can be fixed by reprocessing from here",
          "Partitioned by date and ticker for fast access"]),
        ("SILVER  --  Clean Data",
         ["Remove rows with missing values and duplicates",
          "Flag unusual price spikes (more than 3 standard deviations from the average)",
          "Add 14 features: moving averages, RSI, MACD, Bollinger Bands, daily return, volume signal",
          "All 6 data quality checks passed; only 3.6% of rows flagged as outliers"]),
        ("GOLD  --  Ready for Analysis",
         ["Organized as a Star Schema: one fact table and 3 dimension tables",
          "Fact table: 501 rows with all features and next-day closing price as the ML target",
          "Loaded into PostgreSQL so the API can query it in real time"]),
    ]

    y = Inches(1.4)
    for label, items in layers:
        add_section_label(s, label, y)
        y += Inches(0.35)
        for item in items:
            add_textbox(s, "  -  " + item, Inches(0.5), y, Inches(12.3), Inches(0.38),
                        font_size=14, color=DARK_TEXT)
            y += Inches(0.38)
        y += Inches(0.1)


def slide_07_models(prs):
    s = blank_slide(prs)
    add_header(s, "Stage 4 -- Machine Learning Models", "Three models trained and compared")

    add_section_label(s, "DATA SPLIT (TIME-BASED -- NO SHUFFLING)", Inches(1.35))
    split_items = [
        "  70% Train (earliest data)  -->  329 rows     |     15% Validation  -->  70 rows     |     15% Test (most recent)  -->  71 rows",
        "  Scaler fitted on training data only to prevent future data from leaking into training",
    ]
    add_bullet_block(s, split_items, Inches(0.5), Inches(1.75), Inches(12.3), Inches(0.9), font_size=13)

    add_divider(s, Inches(2.85))

    headers = ["Model", "Type", "Input", "Key Settings"]
    rows = [
        ["ARIMA (5,1,2)", "Classic Time Series", "Past closing prices only",
         "5 AR lags, 1st-order differencing, 2 MA terms"],
        ["LightGBM",      "Gradient Boosting",   "All 19 engineered features",
         "500 trees, learning rate 0.05, early stopping at 50 rounds"],
        ["LSTM",          "Deep Learning",        "30-day sliding window, 19 features",
         "2 stacked LSTM layers with Dropout, Dense output"],
    ]
    add_table_simple(s, headers, rows,
                     Inches(0.5), Inches(3.0), Inches(12.3), Inches(2.8), font_size=13)

    add_textbox(
        s, "All training runs are tracked with MLflow so experiments are reproducible.",
        Inches(0.5), Inches(6.0), Inches(12.3), Inches(0.4),
        font_size=13, italic=True, color=NAVY_LIGHT
    )


def slide_08_results(prs):
    s = blank_slide(prs)
    add_header(s, "Model Performance Results", "Test set -- 71 most recent trading days")

    headers = ["Model", "MAE (USD)", "RMSE (USD)", "MAPE (%)", "Direction Correct (%)", "Sharpe Ratio"]
    rows = [
        ["ARIMA  (Winner)", "$3.15",  "$4.33",  "1.19%",  "23.97%", "+1.88"],
        ["LightGBM",         "$9.96",  "$13.13", "3.66%",  "23.29%", "-1.13"],
        ["LSTM",             "$41.97", "$43.26", "15.80%", "22.41%", "-1.85"],
    ]
    add_table_simple(s, headers, rows,
                     Inches(0.5), Inches(1.45), Inches(12.3), Inches(2.2), font_size=14)

    add_divider(s, Inches(3.85))
    add_section_label(s, "WHY DID THE SIMPLE MODEL WIN?", Inches(3.95))
    finding_items = [
        "- The training set has only 471 rows -- too small for LightGBM and LSTM to learn without overfitting",
        "- ARIMA focuses directly on the repeating pattern in daily closing prices",
        "- A Sharpe Ratio of +1.88 means the ARIMA predictions, used for trading signals, would be profitable",
        "- Conclusion: on small financial datasets, a simpler model is often better than a complex one",
    ]
    add_bullet_block(s, finding_items, Inches(0.5), Inches(4.35), Inches(12.3), Inches(2.4), font_size=15)


def slide_09_app(prs):
    s = blank_slide(prs)
    add_header(s, "Stage 5 -- Application Layer", "REST API and interactive dashboard")

    # Left: FastAPI
    add_section_label(s, "REST API  (FastAPI, port 8000)", Inches(1.35))
    api_items = [
        "- /predict     Returns tomorrow's predicted closing price for AAPL",
        "- /history     Returns recent price history from the database",
        "- /retrain     Triggers model retraining in the background",
        "- /health      Checks that the system is running correctly",
        "- Every prediction is saved to the database for tracking",
    ]
    add_bullet_block(s, api_items, Inches(0.5), Inches(1.75), Inches(6.0), Inches(2.8), font_size=14)

    # Vertical divider
    add_rect(s, Inches(6.55), Inches(1.35), Inches(0.04), Inches(3.8), ACCENT)

    # Right: Streamlit
    add_section_label(s, "DASHBOARD  (Streamlit, port 8501)", Inches(1.35))
    dash_items = [
        "- Live prediction panel refreshing every 30 seconds",
        "- Interactive candlestick chart with volume bars and moving averages",
        "- Model comparison table and training plots",
        "- Works offline -- reads from data files if the API is unavailable",
    ]
    add_bullet_block(s, dash_items, Inches(6.75), Inches(1.75), Inches(6.0), Inches(2.4), font_size=14)

    add_divider(s, Inches(5.3))
    add_section_label(s, "INFRASTRUCTURE (DOCKER COMPOSE)", Inches(5.4))
    infra = "All supporting services run in Docker containers: Kafka, PostgreSQL, pgAdmin, Metabase -- started with one command."
    add_textbox(s, infra, Inches(0.5), Inches(5.8), Inches(12.3), Inches(0.55), font_size=14, color=NAVY_LIGHT)


def slide_10_conclusion(prs):
    s = blank_slide(prs)
    add_header(s, "Conclusion & Future Work", "What was achieved and what comes next")

    add_section_label(s, "WHAT THE PROJECT ACHIEVED", Inches(1.35))
    achieved = [
        "- A fully working pipeline: from live stock feeds all the way to served predictions",
        "- Real-time and batch data processed with the same logic -- consistent results",
        "- ARIMA selected as the best model: average prediction error of only $3.15",
        "- All 6 data quality checks passed; pipeline is safe to re-run at any time",
    ]
    add_bullet_block(s, achieved, Inches(0.5), Inches(1.75), Inches(12.3), Inches(1.7), font_size=15)

    add_divider(s, Inches(3.65))

    add_section_label(s, "CURRENT LIMITATIONS", Inches(3.75))
    limits = [
        "- Training data is small (471 rows), which limits more complex models",
        "- ARIMA is univariate -- it does not use the 14 engineered features",
        "- Retraining must be triggered manually; there is no automatic schedule",
    ]
    add_bullet_block(s, limits, Inches(0.5), Inches(4.15), Inches(12.3), Inches(1.25), font_size=15)

    add_divider(s, Inches(5.5))

    add_section_label(s, "FUTURE IMPROVEMENTS", Inches(5.6))
    future = [
        "- Use ARIMAX to include external features (market sentiment, macroeconomic data)  |  "
        "- Add Apache Airflow for automated daily retraining  |  "
        "- Extend to multiple tickers (MSFT, NVDA, GOOGL)  |  "
        "- Deploy to cloud infrastructure for production use",
    ]
    add_bullet_block(s, future, Inches(0.5), Inches(5.98), Inches(12.3), Inches(0.9), font_size=14)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    prs = new_prs()

    slide_01_title(prs)
    slide_02_problem(prs)
    slide_03_architecture(prs)
    slide_04_ingestion(prs)
    slide_05_etl(prs)
    slide_06_warehouse(prs)
    slide_07_models(prs)
    slide_08_results(prs)
    slide_09_app(prs)
    slide_10_conclusion(prs)

    out_path = "/home/user/Data_engineering_stock/Stock_Pipeline_Presentation.pptx"
    prs.save(out_path)
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
