"""
Generates the 10-slide presentation for the Data Engineering Stock project.
Color scheme: dark navy blue header bar, white body, boxed sections, charts.
"""

from pptx import Presentation
from pptx.util import Inches, Pt, Emu
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.chart.data import ChartData
from pptx.enum.chart import XL_CHART_TYPE
from pptx.util import Pt
from pptx.dml.color import RGBColor
import copy

# ── Palette ────────────────────────────────────────────────────────────────────
NAVY        = RGBColor(0x0D, 0x2B, 0x55)
NAVY_MID    = RGBColor(0x1A, 0x3F, 0x7A)
ACCENT      = RGBColor(0x2E, 0x86, 0xC1)
ACCENT_LITE = RGBColor(0x85, 0xC1, 0xE9)
GOLD        = RGBColor(0xD4, 0xAC, 0x0D)
SILVER_CLR  = RGBColor(0x85, 0x92, 0x9E)
BRONZE_CLR  = RGBColor(0xA0, 0x52, 0x2D)
GREEN       = RGBColor(0x1E, 0x8B, 0x4C)
RED         = RGBColor(0xC0, 0x39, 0x2B)
WHITE       = RGBColor(0xFF, 0xFF, 0xFF)
DARK        = RGBColor(0x1A, 0x1A, 0x2E)
LIGHT_BG    = RGBColor(0xF4, 0xF6, 0xF9)
BOX_BORDER  = RGBColor(0x2E, 0x86, 0xC1)

SLIDE_W = Inches(13.33)
SLIDE_H = Inches(7.5)
HEADER_H = Inches(1.2)

# ── Core helpers ───────────────────────────────────────────────────────────────

def new_prs():
    prs = Presentation()
    prs.slide_width  = SLIDE_W
    prs.slide_height = SLIDE_H
    return prs


def blank_slide(prs):
    return prs.slides.add_slide(prs.slide_layouts[6])


def rect(slide, l, t, w, h, fill=WHITE, line_color=None, line_width=Pt(0),
         radius=None):
    """Add a filled rectangle (or rounded rect) shape."""
    from pptx.util import Pt as Pt2
    shape = slide.shapes.add_shape(1, l, t, w, h)
    shape.fill.solid()
    shape.fill.fore_color.rgb = fill
    if line_color:
        shape.line.color.rgb = line_color
        shape.line.width = line_width
    else:
        shape.line.fill.background()
    return shape


def arrow(slide, l, t, w, h=Inches(0.04), color=ACCENT):
    """Horizontal arrow connector drawn as a thin rectangle + triangle."""
    rect(slide, l, t, w, h, fill=color)


def txt(slide, text, l, t, w, h, size=14, bold=False, color=DARK,
        align=PP_ALIGN.LEFT, italic=False, wrap=True):
    tb = slide.shapes.add_textbox(l, t, w, h)
    tf = tb.text_frame
    tf.word_wrap = wrap
    p = tf.paragraphs[0]
    p.alignment = align
    run = p.add_run()
    run.text = text
    run.font.size   = Pt(size)
    run.font.bold   = bold
    run.font.italic = italic
    run.font.color.rgb = color
    run.font.name   = "Calibri"
    return tb


def header(slide, title, subtitle=None):
    """Full-width navy header bar."""
    rect(slide, 0, 0, SLIDE_W, HEADER_H, fill=NAVY)
    rect(slide, 0, HEADER_H - Inches(0.055), SLIDE_W, Inches(0.055), fill=ACCENT)
    txt(slide, title,
        Inches(0.4), Inches(0.1), Inches(12.5), Inches(0.75),
        size=30, bold=True, color=WHITE, align=PP_ALIGN.LEFT)
    if subtitle:
        txt(slide, subtitle,
            Inches(0.4), Inches(0.82), Inches(12.5), Inches(0.32),
            size=13, color=ACCENT_LITE, align=PP_ALIGN.LEFT)


def box(slide, l, t, w, h, fill=LIGHT_BG, border=BOX_BORDER,
        border_w=Pt(1.5), label=None, label_color=NAVY):
    """Bordered content box with optional top label."""
    r = rect(slide, l, t, w, h, fill=fill, line_color=border, line_width=border_w)
    if label:
        txt(slide, label, l + Inches(0.12), t + Inches(0.08),
            w - Inches(0.24), Inches(0.35),
            size=11, bold=True, color=label_color)
    return r


def pill(slide, text, l, t, w=Inches(2.2), h=Inches(0.38),
         fill=NAVY, text_color=WHITE, size=12):
    """Small colored pill / badge."""
    rect(slide, l, t, w, h, fill=fill)
    txt(slide, text, l, t, w, h, size=size, bold=True,
        color=text_color, align=PP_ALIGN.CENTER)


def divider(slide, t):
    rect(slide, Inches(0.4), t, Inches(12.5), Inches(0.03), fill=ACCENT)


def stage_box(slide, l, t, w, h, number, label, fill=NAVY):
    """Numbered stage box for pipeline diagrams."""
    rect(slide, l, t, w, h, fill=fill, line_color=ACCENT, line_width=Pt(1.5))
    txt(slide, number, l, t + Inches(0.05), w, Inches(0.4),
        size=22, bold=True, color=ACCENT, align=PP_ALIGN.CENTER)
    txt(slide, label, l, t + Inches(0.42), w, h - Inches(0.42),
        size=12, bold=True, color=WHITE, align=PP_ALIGN.CENTER, wrap=True)


def flow_arrow(slide, l, t, length=Inches(0.35)):
    """Small horizontal flow arrow."""
    rect(slide, l, t + Inches(0.13), length, Inches(0.06), fill=ACCENT)
    # arrowhead triangle approximated by a narrow rect
    rect(slide, l + length - Inches(0.05), t + Inches(0.04),
         Inches(0.05), Inches(0.24), fill=ACCENT)


def add_bar_chart(slide, categories, values, colors_hex,
                  l, t, w, h, title="", value_label=""):
    """Add a clustered bar chart."""
    chart_data = ChartData()
    chart_data.categories = categories
    chart_data.add_series(value_label, values)
    from pptx.enum.chart import XL_CHART_TYPE
    chart_frame = slide.shapes.add_chart(
        XL_CHART_TYPE.COLUMN_CLUSTERED, l, t, w, h, chart_data
    )
    chart = chart_frame.chart
    chart.has_legend = False
    if title:
        chart.has_title = True
        chart.chart_title.text_frame.text = title
        chart.chart_title.text_frame.paragraphs[0].runs[0].font.size = Pt(12)
        chart.chart_title.text_frame.paragraphs[0].runs[0].font.bold = True
        chart.chart_title.text_frame.paragraphs[0].runs[0].font.color.rgb = NAVY
    # Color each bar
    series = chart.series[0]
    for i, hex_color in enumerate(colors_hex):
        pt = series.points[i]
        pt.format.fill.solid()
        pt.format.fill.fore_color.rgb = hex_color
    # Style axes
    chart.value_axis.tick_labels.font.size = Pt(10)
    chart.category_axis.tick_labels.font.size = Pt(11)
    chart.category_axis.tick_labels.font.bold = True
    return chart


def add_horiz_bar_chart(slide, categories, values, colors_hex, l, t, w, h,
                        title=""):
    chart_data = ChartData()
    chart_data.categories = categories
    chart_data.add_series("", values)
    chart_frame = slide.shapes.add_chart(
        XL_CHART_TYPE.BAR_CLUSTERED, l, t, w, h, chart_data
    )
    chart = chart_frame.chart
    chart.has_legend = False
    if title:
        chart.has_title = True
        chart.chart_title.text_frame.text = title
        chart.chart_title.text_frame.paragraphs[0].runs[0].font.size = Pt(11)
        chart.chart_title.text_frame.paragraphs[0].runs[0].font.bold = True
        chart.chart_title.text_frame.paragraphs[0].runs[0].font.color.rgb = NAVY
    series = chart.series[0]
    for i, c in enumerate(colors_hex):
        pt = series.points[i]
        pt.format.fill.solid()
        pt.format.fill.fore_color.rgb = c
    chart.value_axis.tick_labels.font.size = Pt(9)
    chart.category_axis.tick_labels.font.size = Pt(10)
    return chart


def styled_table(slide, headers, rows, l, t, w, h, font_size=12):
    """Navy-header styled table."""
    n_cols = len(headers)
    n_rows = len(rows) + 1
    tbl = slide.shapes.add_table(n_rows, n_cols, l, t, w, h).table
    for ci, hdr in enumerate(headers):
        cell = tbl.cell(0, ci)
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
    for ri, row in enumerate(rows):
        bg = LIGHT_BG if ri % 2 == 0 else WHITE
        for ci, val in enumerate(row):
            cell = tbl.cell(ri + 1, ci)
            cell.fill.solid()
            cell.fill.fore_color.rgb = bg
            p = cell.text_frame.paragraphs[0]
            p.alignment = PP_ALIGN.CENTER
            run = p.add_run()
            run.text = str(val)
            run.font.size  = Pt(font_size)
            run.font.color.rgb = DARK
            run.font.name  = "Calibri"
    return tbl


def bullets_in_box(slide, items, l, t, w, h, size=13, top_pad=Inches(0.08)):
    """Render bullet list inside a region (no box drawn — caller draws the box)."""
    y = t + top_pad
    line_h = Pt(size) * 1.9
    for item in items:
        txt(slide, item, l + Inches(0.18), y, w - Inches(0.25), line_h,
            size=size, color=DARK)
        y += line_h


# ══════════════════════════════════════════════════════════════════════════════
# SLIDES
# ══════════════════════════════════════════════════════════════════════════════

def slide_01_title(prs):
    s = blank_slide(prs)
    rect(s, 0, 0, SLIDE_W, SLIDE_H, fill=NAVY)

    # Accent bar
    rect(s, 0, Inches(3.35), SLIDE_W, Inches(0.07), fill=ACCENT)

    # Background pattern rectangles (decorative)
    rect(s, Inches(10.5), 0, Inches(2.83), Inches(2.5),
         fill=RGBColor(0x15, 0x3A, 0x6E))
    rect(s, 0, Inches(5.5), Inches(3.5), Inches(2.0),
         fill=RGBColor(0x15, 0x3A, 0x6E))

    txt(s, "End-to-End Stock Price Prediction",
        Inches(0.9), Inches(1.1), Inches(11.5), Inches(0.9),
        size=42, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
    txt(s, "Data Pipeline",
        Inches(0.9), Inches(1.95), Inches(11.5), Inches(0.85),
        size=42, bold=True, color=WHITE, align=PP_ALIGN.CENTER)

    txt(s, "From Real-Time Data Collection to Machine Learning Predictions",
        Inches(0.9), Inches(3.55), Inches(11.5), Inches(0.55),
        size=18, color=ACCENT_LITE, align=PP_ALIGN.CENTER)

    # Metadata pills row
    for i, label in enumerate(["[Your Name]", "[Student ID]", "[Course Name]", "[Date]"]):
        pill(s, label, Inches(1.5 + i * 2.65), Inches(4.6),
             w=Inches(2.3), fill=NAVY_MID, size=12)

    # Bottom tech badges
    techs = ["Apache Kafka", "PySpark", "PostgreSQL", "ARIMA / LightGBM / LSTM",
             "FastAPI", "Streamlit"]
    bx = Inches(0.55)
    for tech in techs:
        w = Inches(1.85)
        rect(s, bx, Inches(5.9), w, Inches(0.38),
             fill=RGBColor(0x0A, 0x20, 0x40),
             line_color=ACCENT, line_width=Pt(1))
        txt(s, tech, bx, Inches(5.9), w, Inches(0.38),
            size=11, color=ACCENT_LITE, align=PP_ALIGN.CENTER)
        bx += Inches(2.1)


def slide_02_problem(prs):
    s = blank_slide(prs)
    header(s, "Problem Statement", "Why this project was built")

    # --- Problem box (left) ---
    box(s, Inches(0.4), Inches(1.35), Inches(5.9), Inches(5.7),
        fill=RGBColor(0xEB, 0xF5, 0xFB), border=RED, label="THE PROBLEM",
        label_color=RED)

    prob_items = [
        "- Stock prices arrive every second in real",
        "  time, but most projects only work with",
        "  downloaded CSV files",
        "",
        "- No unified system exists that handles",
        "  live data, cleans it, stores it, and",
        "  makes predictions all in one place",
        "",
        "- It is unclear whether complex deep",
        "  learning models actually outperform",
        "  simple models on small datasets",
    ]
    bullets_in_box(s, prob_items,
                   Inches(0.4), Inches(1.75),
                   Inches(5.9), Inches(5.2), size=14,
                   top_pad=Inches(0.1))

    # --- Goals box (right) ---
    box(s, Inches(6.55), Inches(1.35), Inches(6.4), Inches(5.7),
        fill=RGBColor(0xEA, 0xF4, 0xEE), border=GREEN, label="PROJECT GOALS",
        label_color=GREEN)

    goal_items = [
        "- Build a complete pipeline from live",
        "  data collection to serving predictions",
        "",
        "- Handle both historical and live",
        "  streaming data in the same system",
        "",
        "- Train and compare three prediction",
        "  models to find which works best",
        "",
        "- Display predictions through a web",
        "  dashboard and a REST API",
    ]
    bullets_in_box(s, goal_items,
                   Inches(6.55), Inches(1.75),
                   Inches(6.4), Inches(5.2), size=14,
                   top_pad=Inches(0.1))


def slide_03_architecture(prs):
    s = blank_slide(prs)
    header(s, "System Architecture", "Five-stage Lambda Architecture pipeline")

    # ── Pipeline flow boxes ──────────────────────────────────────────────────
    stage_labels = [
        ("01", "Data\nIngestion"),
        ("02", "ETL\nPipeline"),
        ("03", "Data\nWarehouse"),
        ("04", "ML\nTraining"),
        ("05", "Serving\nLayer"),
    ]
    fills = [NAVY_MID, NAVY_MID, NAVY_MID, NAVY_MID, NAVY_MID]
    bw = Inches(2.1)
    bh = Inches(1.4)
    gap = Inches(0.22)
    start_x = Inches(0.35)
    by = Inches(1.45)

    for i, (num, lbl) in enumerate(stage_labels):
        bx = start_x + i * (bw + gap)
        stage_box(s, bx, by, bw, bh, num, lbl)
        if i < len(stage_labels) - 1:
            # arrow between boxes
            ax = bx + bw
            ay = by + bh / 2 - Inches(0.03)
            rect(s, ax, ay, gap, Inches(0.06), fill=ACCENT)
            # arrowhead
            rect(s, ax + gap - Inches(0.06), ay - Inches(0.1),
                 Inches(0.06), Inches(0.26), fill=ACCENT)

    # ── Tool badges below each stage box ─────────────────────────────────────
    tools = [
        ["yfinance", "Finnhub"],
        ["Kafka", "PySpark"],
        ["Parquet", "PostgreSQL"],
        ["ARIMA", "LightGBM\nLSTM"],
        ["FastAPI", "Streamlit"],
    ]
    badge_fill = RGBColor(0xD6, 0xEA, 0xF8)
    for i, tool_list in enumerate(tools):
        bx = start_x + i * (bw + gap)
        ty = by + bh + Inches(0.12)
        for j, tool in enumerate(tool_list):
            rect(s, bx + Inches(0.05), ty + j * Inches(0.45),
                 bw - Inches(0.1), Inches(0.38),
                 fill=badge_fill, line_color=ACCENT, line_width=Pt(0.75))
            txt(s, tool, bx + Inches(0.05), ty + j * Inches(0.45),
                bw - Inches(0.1), Inches(0.38),
                size=11, bold=True, color=NAVY, align=PP_ALIGN.CENTER)

    # ── Key design note box ───────────────────────────────────────────────────
    divider(s, Inches(4.5))
    box(s, Inches(0.4), Inches(4.6), Inches(12.5), Inches(0.75),
        fill=RGBColor(0xFE, 0xF9, 0xE7), border=GOLD, border_w=Pt(1.5))
    txt(s, "Key Design Principle:  The real-time path and the historical batch path "
        "share the same cleaning and transform logic -- so results are always consistent "
        "regardless of the data source.",
        Inches(0.6), Inches(4.65), Inches(12.1), Inches(0.65),
        size=13, italic=True, color=RGBColor(0x78, 0x6C, 0x00))

    # ── Lambda Architecture label ─────────────────────────────────────────────
    box(s, Inches(0.4), Inches(5.5), Inches(12.5), Inches(1.6),
        fill=LIGHT_BG, border=BOX_BORDER)
    txt(s, "Lambda Architecture -- Two Paths, One Output",
        Inches(0.6), Inches(5.55), Inches(6.0), Inches(0.38),
        size=12, bold=True, color=NAVY)

    paths = [
        ("SPEED LAYER (Real-Time)",
         "Finnhub WebSocket  ->  Kafka  ->  PySpark Streaming  ->  Bronze Layer",
         ACCENT),
        ("BATCH LAYER (Historical)",
         "Yahoo Finance API  ->  Parquet  ->  PySpark Batch  ->  Bronze Layer",
         NAVY_MID),
    ]
    for i, (lbl, path, c) in enumerate(paths):
        y = Inches(5.95) + i * Inches(0.5)
        rect(s, Inches(0.6), y, Inches(2.1), Inches(0.36), fill=c)
        txt(s, lbl, Inches(0.6), y, Inches(2.1), Inches(0.36),
            size=10, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
        txt(s, path, Inches(2.85), y, Inches(9.8), Inches(0.36),
            size=12, color=DARK)


def slide_04_ingestion(prs):
    s = blank_slide(prs)
    header(s, "Stage 1 -- Data Ingestion", "Collecting stock data from two sources")

    # ── Live Data box ─────────────────────────────────────────────────────────
    box(s, Inches(0.4), Inches(1.35), Inches(5.9), Inches(4.4),
        fill=RGBColor(0xEA, 0xF4, 0xFF), border=ACCENT, border_w=Pt(2))
    pill(s, "LIVE DATA  (REAL-TIME)", Inches(0.4), Inches(1.35),
         w=Inches(5.9), h=Inches(0.42), fill=ACCENT, size=12)

    live_items = [
        "- Connects to Finnhub WebSocket during",
        "  US market hours (9:30am - 4:00pm EST)",
        "",
        "- Receives trade events every second:",
        "  price, volume, and timestamp",
        "",
        "- Bad records sent to a Dead Letter",
        "  Queue for review and auditing",
    ]
    bullets_in_box(s, live_items,
                   Inches(0.4), Inches(1.82), Inches(5.9), Inches(3.9),
                   size=14, top_pad=Inches(0.08))

    # ── Batch Data box ────────────────────────────────────────────────────────
    box(s, Inches(6.55), Inches(1.35), Inches(6.4), Inches(4.4),
        fill=RGBColor(0xEA, 0xF4, 0xFF), border=NAVY_MID, border_w=Pt(2))
    pill(s, "HISTORICAL DATA  (BATCH)", Inches(6.55), Inches(1.35),
         w=Inches(6.4), h=Inches(0.42), fill=NAVY_MID, size=12)

    hist_items = [
        "- Downloads 2 years of AAPL daily",
        "  stock prices from Yahoo Finance",
        "",
        "- Each row: open, high, low, close,",
        "  volume for that trading day",
        "",
        "- Saved as a Parquet file for fast",
        "  processing in the next stage",
    ]
    bullets_in_box(s, hist_items,
                   Inches(6.55), Inches(1.82), Inches(6.4), Inches(3.9),
                   size=14, top_pad=Inches(0.08))

    # ── Output result box ─────────────────────────────────────────────────────
    divider(s, Inches(5.85))
    box(s, Inches(0.4), Inches(5.95), Inches(12.5), Inches(1.15),
        fill=RGBColor(0xEA, 0xF4, 0xEE), border=GREEN, border_w=Pt(1.5))
    txt(s, "OUTPUT", Inches(0.6), Inches(6.0), Inches(1.2), Inches(0.35),
        size=11, bold=True, color=GREEN)
    txt(s, "501 rows of AAPL daily OHLCV data  |  Approx. 2 years of trading days  |  "
        "Both sources produce the same schema for unified downstream processing",
        Inches(0.6), Inches(6.35), Inches(12.1), Inches(0.55),
        size=14, bold=True, color=DARK)


def slide_05_etl(prs):
    s = blank_slide(prs)
    header(s, "Stage 2 -- ETL Pipeline", "Extract, Transform, Load: cleaning and moving the data")

    # ── Flow diagram: Source -> Kafka -> PySpark -> Bronze ────────────────────
    flow_items = [
        (ACCENT,    "Finnhub\nWebSocket",  "Live Ticks"),
        (NAVY_MID,  "Apache\nKafka",       "Message Queue"),
        (ACCENT,    "PySpark\nStreaming",   "1-min OHLCV Bars"),
        (GREEN,     "Bronze\nLayer",        "Parquet + PostgreSQL"),
    ]
    box_w = Inches(2.5)
    box_h = Inches(1.35)
    bx = Inches(0.35)
    by = Inches(1.4)
    arr_w = Inches(0.4)

    for i, (fill, top_label, sub_label) in enumerate(flow_items):
        x = bx + i * (box_w + arr_w)
        rect(s, x, by, box_w, box_h, fill=fill,
             line_color=WHITE, line_width=Pt(0))
        txt(s, top_label, x, by + Inches(0.1), box_w, Inches(0.65),
            size=14, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
        rect(s, x, by + Inches(0.75), box_w, Inches(0.03), fill=WHITE)
        txt(s, sub_label, x, by + Inches(0.82), box_w, Inches(0.45),
            size=11, color=RGBColor(0xD6, 0xEA, 0xF8), align=PP_ALIGN.CENTER)
        if i < len(flow_items) - 1:
            ax = x + box_w
            ay = by + box_h / 2 - Inches(0.03)
            rect(s, ax, ay, arr_w - Inches(0.08), Inches(0.06), fill=ACCENT_LITE)
            rect(s, ax + arr_w - Inches(0.14),
                 ay - Inches(0.12), Inches(0.08), Inches(0.3), fill=ACCENT_LITE)

    # ── Kafka details box ─────────────────────────────────────────────────────
    box(s, Inches(0.35), Inches(2.9), Inches(5.85), Inches(3.65),
        fill=LIGHT_BG, border=ACCENT, label="APACHE KAFKA  --  MESSAGE QUEUE",
        label_color=ACCENT)
    kafka_items = [
        "- Main topic: 3 partitions, 7-day retention",
        "- Dead Letter Queue: holds bad messages",
        "  for 30 days for review",
        "- Acts as a buffer: data is never lost",
        "  even if the processor crashes",
        "- Decouples ingestion from processing",
    ]
    bullets_in_box(s, kafka_items,
                   Inches(0.35), Inches(3.28), Inches(5.85), Inches(3.2),
                   size=13, top_pad=Inches(0.05))

    # ── PySpark details box ───────────────────────────────────────────────────
    box(s, Inches(6.45), Inches(2.9), Inches(6.5), Inches(3.65),
        fill=LIGHT_BG, border=NAVY_MID, label="PYSPARK  --  PROCESSING ENGINE",
        label_color=NAVY_MID)
    spark_items = [
        "- Streaming: groups ticks into 1-minute bars",
        "  (open, high, low, close, volume, VWAP)",
        "- Accepts late data arriving up to 5 min late",
        "- Batch: same transform applied to 2-year file",
        "- Writes to Parquet and PostgreSQL together",
        "- Exactly-once delivery via checkpointing",
    ]
    bullets_in_box(s, spark_items,
                   Inches(6.45), Inches(3.28), Inches(6.5), Inches(3.2),
                   size=13, top_pad=Inches(0.05))


def slide_06_warehouse(prs):
    s = blank_slide(prs)
    header(s, "Stage 3 -- Medallion Data Warehouse",
           "Three quality tiers: Bronze -> Silver -> Gold")

    tier_w = Inches(3.8)
    tier_h = Inches(5.5)
    tier_y = Inches(1.35)
    gap    = Inches(0.35)

    tiers = [
        (BRONZE_CLR, RGBColor(0xFD, 0xF2, 0xEC),
         "BRONZE", "Raw Storage",
         ["Data stored exactly as received,", "nothing is changed or removed",
          "", "Append-only -- acts as a permanent", "backup for the entire pipeline",
          "", "Partitioned by date and ticker", "for fast access later",
          "", "If anything goes wrong downstream,", "reprocessing starts from here"]),
        (SILVER_CLR, RGBColor(0xF2, 0xF3, 0xF4),
         "SILVER", "Clean + Enriched",
         ["Remove nulls, duplicates, and", "invalid records",
          "", "Flag price spikes > 3 standard", "deviations (3.6% flagged)",
          "", "Add 14 features: SMA-7/30, EMA,", "RSI, MACD, Bollinger Bands,",
          "daily return, volume signal",
          "", "All 6 data quality checks passed"]),
        (GOLD,       RGBColor(0xFE, 0xF9, 0xE7),
         "GOLD", "Ready for Analysis",
         ["Organized as a Star Schema:", "one fact table + 3 dimension tables",
          "", "Fact table: 501 rows with all 14", "features + next-day close as target",
          "", "dim_ticker, dim_date (730 days),", "dim_market_session",
          "", "Loaded into PostgreSQL for", "fast real-time API queries"]),
    ]

    for i, (border_c, fill_c, tier, sub, items) in enumerate(tiers):
        bx = Inches(0.35) + i * (tier_w + gap)
        box(s, bx, tier_y, tier_w, tier_h,
            fill=fill_c, border=border_c, border_w=Pt(2))
        # Header strip
        rect(s, bx, tier_y, tier_w, Inches(0.55), fill=border_c)
        txt(s, tier, bx, tier_y, tier_w, Inches(0.35),
            size=15, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
        txt(s, sub, bx, tier_y + Inches(0.35), tier_w, Inches(0.28),
            size=10, color=WHITE, align=PP_ALIGN.CENTER)
        # Content
        bullets_in_box(s, ["  " + it for it in items],
                       bx, tier_y + Inches(0.6), tier_w, tier_h - Inches(0.6),
                       size=12, top_pad=Inches(0.1))
        # Arrow between tiers
        if i < len(tiers) - 1:
            ax = bx + tier_w
            ay = tier_y + tier_h / 2 - Inches(0.03)
            rect(s, ax, ay, gap - Inches(0.08), Inches(0.06), fill=ACCENT)
            rect(s, ax + gap - Inches(0.15), ay - Inches(0.12),
                 Inches(0.09), Inches(0.3), fill=ACCENT)


def slide_07_models(prs):
    s = blank_slide(prs)
    header(s, "Stage 4 -- Machine Learning Models",
           "Three models trained and compared using time-based split")

    # ── Data split bar ────────────────────────────────────────────────────────
    box(s, Inches(0.4), Inches(1.35), Inches(12.5), Inches(0.85),
        fill=LIGHT_BG, border=BOX_BORDER)
    txt(s, "Data Split (Time-Based -- No Shuffling to prevent data leakage)",
        Inches(0.6), Inches(1.38), Inches(12.1), Inches(0.35),
        size=11, bold=True, color=NAVY)
    # Train bar
    rect(s, Inches(0.5),  Inches(1.82), Inches(7.3), Inches(0.28), fill=GREEN)
    txt(s, "TRAIN  70%  (329 rows)",
        Inches(0.5), Inches(1.82), Inches(7.3), Inches(0.28),
        size=11, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
    # Val bar
    rect(s, Inches(7.82), Inches(1.82), Inches(2.15), Inches(0.28), fill=ACCENT)
    txt(s, "VAL  15%",
        Inches(7.82), Inches(1.82), Inches(2.15), Inches(0.28),
        size=11, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
    # Test bar
    rect(s, Inches(9.99), Inches(1.82), Inches(2.55), Inches(0.28), fill=RED)
    txt(s, "TEST  15%  (71 rows)",
        Inches(9.99), Inches(1.82), Inches(2.55), Inches(0.28),
        size=11, bold=True, color=WHITE, align=PP_ALIGN.CENTER)

    # ── Model cards ───────────────────────────────────────────────────────────
    card_w = Inches(3.8)
    card_h = Inches(4.45)
    card_y = Inches(2.4)
    gap    = Inches(0.45)

    models = [
        (NAVY_MID, "ARIMA  (5,1,2)", "Classic Time Series",
         ["Input: past closing prices only",
          "(univariate)",
          "",
          "5 AR lags capture recent price",
          "memory and trend patterns",
          "",
          "1st-order differencing removes",
          "the long-term price trend",
          "",
          "Best suited for small datasets",
          "with strong autocorrelation"]),
        (ACCENT, "LightGBM", "Gradient Boosting",
         ["Input: all 19 engineered features",
          "",
          "500 decision trees built",
          "sequentially to fix errors",
          "",
          "Early stopping at 50 rounds",
          "to prevent overfitting",
          "",
          "Learning rate: 0.05",
          "Max tree depth: 6"]),
        (RGBColor(0x6C, 0x37, 0x8E), "LSTM", "Deep Learning",
         ["Input: 30-day sliding window",
          "with 19 features per day",
          "",
          "Two stacked LSTM layers:",
          "64 units -> Dropout -> 32 units",
          "",
          "Dropout (20%) prevents",
          "overfitting to training data",
          "",
          "Outputs a single next-day",
          "price prediction"]),
    ]

    for i, (c, name, sub, items) in enumerate(models):
        bx = Inches(0.4) + i * (card_w + gap)
        rect(s, bx, card_y, card_w, card_h,
             fill=LIGHT_BG, line_color=c, line_width=Pt(2))
        rect(s, bx, card_y, card_w, Inches(0.62), fill=c)
        txt(s, name, bx, card_y + Inches(0.04), card_w, Inches(0.38),
            size=15, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
        txt(s, sub, bx, card_y + Inches(0.42), card_w, Inches(0.28),
            size=11, color=WHITE, align=PP_ALIGN.CENTER)
        bullets_in_box(s, ["  " + it for it in items],
                       bx, card_y + Inches(0.65), card_w,
                       card_h - Inches(0.65), size=12, top_pad=Inches(0.1))

    txt(s, "All runs tracked with MLflow for reproducibility",
        Inches(0.4), Inches(7.0), Inches(12.5), Inches(0.35),
        size=12, italic=True, color=NAVY_MID)


def slide_08_results(prs):
    s = blank_slide(prs)
    header(s, "Model Performance Results",
           "Test set -- 71 most recent trading days")

    # ── Results table ─────────────────────────────────────────────────────────
    headers = ["Model", "MAE ($)", "RMSE ($)", "MAPE (%)", "Direction (%)", "Sharpe"]
    rows = [
        ["ARIMA  (Winner)", "3.15",  "4.33",  "1.19",  "23.97", "+1.88"],
        ["LightGBM",         "9.96",  "13.13", "3.66",  "23.29", "-1.13"],
        ["LSTM",             "41.97", "43.26", "15.80", "22.41", "-1.85"],
    ]
    styled_table(s, headers, rows,
                 Inches(0.4), Inches(1.38), Inches(6.8), Inches(1.85), font_size=13)

    # ── MAE Bar Chart ─────────────────────────────────────────────────────────
    add_bar_chart(
        s,
        categories=["ARIMA", "LightGBM", "LSTM"],
        values=[3.15, 9.96, 41.97],
        colors_hex=[GREEN, ACCENT, RED],
        l=Inches(7.4), t=Inches(1.25), w=Inches(5.5), h=Inches(3.0),
        title="Mean Absolute Error -- USD (lower is better)",
        value_label="MAE"
    )

    # ── Sharpe Ratio bar chart ────────────────────────────────────────────────
    add_horiz_bar_chart(
        s,
        categories=["LSTM", "LightGBM", "ARIMA"],
        values=[-1.85, -1.13, 1.88],
        colors_hex=[RED, ACCENT, GREEN],
        l=Inches(0.4), t=Inches(3.35), w=Inches(6.5), h=Inches(2.5),
        title="Sharpe Ratio (higher = profitable trading signal)"
    )

    # ── Key findings box ──────────────────────────────────────────────────────
    box(s, Inches(7.1), Inches(4.4), Inches(5.85), Inches(2.65),
        fill=RGBColor(0xEA, 0xF4, 0xEE), border=GREEN, border_w=Pt(1.5),
        label="KEY FINDINGS", label_color=GREEN)
    findings = [
        "- ARIMA wins on all five metrics",
        "- Sharpe +1.88 means profitable signals",
        "- LightGBM and LSTM overfit on only",
        "  471 training rows",
        "- Simpler models can outperform deep",
        "  learning on small financial datasets",
    ]
    bullets_in_box(s, findings,
                   Inches(7.1), Inches(4.8), Inches(5.85), Inches(2.2),
                   size=13, top_pad=Inches(0.05))


def slide_09_app(prs):
    s = blank_slide(prs)
    header(s, "Stage 5 -- Application Layer",
           "REST API and interactive dashboard for serving predictions")

    # ── FastAPI box ───────────────────────────────────────────────────────────
    box(s, Inches(0.4), Inches(1.35), Inches(5.9), Inches(4.5),
        fill=RGBColor(0xEA, 0xF4, 0xFF), border=ACCENT, border_w=Pt(2))
    pill(s, "FastAPI  --  REST API  (port 8000)",
         Inches(0.4), Inches(1.35), w=Inches(5.9), h=Inches(0.42),
         fill=ACCENT, size=12)

    endpoints = [
        ("/predict", "Next-day predicted closing price"),
        ("/history", "Recent OHLCV from PostgreSQL"),
        ("/retrain", "Trigger retraining in background"),
        ("/health",  "Check system and model status"),
    ]
    ey = Inches(1.88)
    for ep, desc in endpoints:
        rect(s, Inches(0.55), ey, Inches(1.4), Inches(0.32),
             fill=NAVY, line_color=None)
        txt(s, ep, Inches(0.55), ey, Inches(1.4), Inches(0.32),
            size=11, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
        txt(s, desc, Inches(2.05), ey, Inches(4.0), Inches(0.32),
            size=12, color=DARK)
        ey += Inches(0.42)

    txt(s, "- Best model (ARIMA) loaded once at startup",
        Inches(0.6), Inches(3.65), Inches(5.5), Inches(0.35), size=13, color=DARK)
    txt(s, "- Every prediction saved to database for tracking",
        Inches(0.6), Inches(4.0), Inches(5.5), Inches(0.35), size=13, color=DARK)
    txt(s, "- Input/output validated with Pydantic schemas",
        Inches(0.6), Inches(4.35), Inches(5.5), Inches(0.35), size=13, color=DARK)
    txt(s, "- Docs auto-generated at /docs",
        Inches(0.6), Inches(4.7), Inches(5.5), Inches(0.35), size=13, color=DARK)

    # ── Streamlit box ─────────────────────────────────────────────────────────
    box(s, Inches(6.55), Inches(1.35), Inches(6.4), Inches(4.5),
        fill=RGBColor(0xF9, 0xEB, 0xEA), border=RED, border_w=Pt(2))
    pill(s, "Streamlit Dashboard  (port 8501)",
         Inches(6.55), Inches(1.35), w=Inches(6.4), h=Inches(0.42),
         fill=RGBColor(0xC0, 0x39, 0x2B), size=12)

    dash_items = [
        "- Live prediction panel that refreshes",
        "  every 30 seconds automatically",
        "",
        "- Interactive candlestick chart with",
        "  volume bars and moving average lines",
        "",
        "- Model metrics comparison table",
        "  and all 6 training plots",
        "",
        "- Works offline: falls back to local",
        "  data files if the API is unavailable",
    ]
    bullets_in_box(s, dash_items,
                   Inches(6.55), Inches(1.82), Inches(6.4), Inches(4.0),
                   size=13, top_pad=Inches(0.08))

    # ── Infrastructure strip ──────────────────────────────────────────────────
    divider(s, Inches(6.0))
    box(s, Inches(0.4), Inches(6.1), Inches(12.5), Inches(1.0),
        fill=LIGHT_BG, border=BOX_BORDER)
    txt(s, "Infrastructure (Docker Compose) -- One command starts everything:",
        Inches(0.6), Inches(6.14), Inches(4.5), Inches(0.35),
        size=11, bold=True, color=NAVY)
    services = [
        ("Kafka + Zookeeper", NAVY_MID),
        ("PostgreSQL :5433", NAVY),
        ("pgAdmin :5051", ACCENT),
        ("Metabase :3000", GREEN),
        ("Schema Registry :8081", RGBColor(0x6C, 0x37, 0x8E)),
    ]
    sx = Inches(0.55)
    for name, c in services:
        rect(s, sx, Inches(6.5), Inches(2.2), Inches(0.35),
             fill=c, line_color=None)
        txt(s, name, sx, Inches(6.5), Inches(2.2), Inches(0.35),
            size=11, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
        sx += Inches(2.45)


def slide_10_conclusion(prs):
    s = blank_slide(prs)
    header(s, "Conclusion & Future Work",
           "What was achieved and what comes next")

    # ── Top row: Achieved + Limitations ───────────────────────────────────────
    # Achieved
    box(s, Inches(0.4), Inches(1.35), Inches(6.0), Inches(3.4),
        fill=RGBColor(0xEA, 0xF4, 0xEE), border=GREEN, border_w=Pt(2))
    pill(s, "WHAT THE PROJECT ACHIEVED",
         Inches(0.4), Inches(1.35), w=Inches(6.0), h=Inches(0.42),
         fill=GREEN, size=12)
    achieved = [
        "- Complete pipeline from live feeds to",
        "  served predictions -- all stages working",
        "",
        "- Real-time and batch data processed with",
        "  the same logic (consistent results)",
        "",
        "- ARIMA selected as best model:",
        "  avg. error $3.15, Sharpe +1.88",
        "",
        "- All 6 data quality checks passed;",
        "  pipeline is safe to re-run at any time",
    ]
    bullets_in_box(s, achieved,
                   Inches(0.4), Inches(1.82), Inches(6.0), Inches(2.9),
                   size=13, top_pad=Inches(0.05))

    # Limitations
    box(s, Inches(6.65), Inches(1.35), Inches(6.3), Inches(3.4),
        fill=RGBColor(0xFD, 0xF2, 0xEC), border=RED, border_w=Pt(2))
    pill(s, "CURRENT LIMITATIONS",
         Inches(6.65), Inches(1.35), w=Inches(6.3), h=Inches(0.42),
         fill=RED, size=12)
    limits = [
        "- Training set is small (471 rows),",
        "  limiting complex model performance",
        "",
        "- ARIMA is univariate -- does not use",
        "  the 14 engineered features",
        "",
        "- Retraining must be triggered manually;",
        "  no automatic daily schedule",
        "",
        "- No authentication on the API",
        "  or dashboard (development only)",
    ]
    bullets_in_box(s, limits,
                   Inches(6.65), Inches(1.82), Inches(6.3), Inches(2.9),
                   size=13, top_pad=Inches(0.05))

    # ── Future Work box ───────────────────────────────────────────────────────
    divider(s, Inches(4.9))
    box(s, Inches(0.4), Inches(4.98), Inches(12.5), Inches(2.15),
        fill=RGBColor(0xFE, 0xF9, 0xE7), border=GOLD, border_w=Pt(2))
    pill(s, "FUTURE IMPROVEMENTS",
         Inches(0.4), Inches(4.98), w=Inches(12.5), h=Inches(0.42),
         fill=GOLD, text_color=DARK, size=12)

    future = [
        ("Use ARIMAX to include external features like market",
         "sentiment and macroeconomic indicators"),
        ("Add Apache Airflow for automated",
         "daily retraining on a fixed schedule"),
        ("Extend to multiple tickers",
         "(MSFT, NVDA, GOOGL) with separate models"),
        ("Deploy to cloud infrastructure",
         "(AWS or GCP) for production use"),
    ]
    fx = Inches(0.55)
    fw = Inches(2.9)
    for line1, line2 in future:
        rect(s, fx, Inches(5.48), fw, Inches(1.45),
             fill=WHITE, line_color=GOLD, line_width=Pt(1))
        txt(s, line1, fx + Inches(0.1), Inches(5.52),
            fw - Inches(0.2), Inches(0.7), size=12, color=DARK)
        txt(s, line2, fx + Inches(0.1), Inches(6.18),
            fw - Inches(0.2), Inches(0.65), size=12, color=NAVY_MID)
        fx += Inches(3.1)


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

    out = "/home/user/Data_engineering_stock/Stock_Pipeline_Presentation.pptx"
    prs.save(out)
    print(f"Saved: {out}")


if __name__ == "__main__":
    main()
