"""
finnhub_producer.py — Real-time websocket producer for Finnhub trade data.

Connects to Finnhub's websocket feed, subscribes to real-time trade events
for a configurable ticker, and writes each event as structured JSON to stdout
(and optionally to a JSONL file for local testing).

In Stage 2 this script will be adapted to publish directly to Kafka.
For now it outputs raw trade events that can be piped or inspected.

Usage:
    python -m src.ingestion.finnhub_producer              # uses DEFAULT_TICKER from .env
    python -m src.ingestion.finnhub_producer --ticker MSFT # override ticker
    python -m src.ingestion.finnhub_producer --output-file # also write to data/raw/trades_AAPL.jsonl
"""

import argparse
import json
import sys
import time
import signal
import logging
from datetime import datetime, timezone
from pathlib import Path

import websocket  # websocket-client library

# ── Project imports ──────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import (
    FINNHUB_API_KEY,
    FINNHUB_WS_URL,
    DEFAULT_TICKER,
    RAW_DATA_DIR,
    WS_RECONNECT_DELAY_SEC,
    WS_MAX_RECONNECT_DELAY_SEC,
    WS_MAX_RETRIES,
    validate,
)

# ── Logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Global state ─────────────────────────────────────────────────────────────
_running = True               # flipped by SIGINT/SIGTERM handler
_retry_count = 0              # consecutive reconnection failures
_output_file = None           # optional JSONL file handle
_ticker = DEFAULT_TICKER      # active ticker symbol
_message_count = 0            # total messages received this session


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Signal handling for graceful shutdown
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _shutdown_handler(signum, frame):
    """Handle SIGINT/SIGTERM for graceful shutdown."""
    global _running
    logger.info("Shutdown signal received — closing websocket…")
    _running = False


signal.signal(signal.SIGINT, _shutdown_handler)
signal.signal(signal.SIGTERM, _shutdown_handler)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Message processing
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _process_trade(trade: dict) -> dict:
    """
    Transform a single Finnhub trade event into our canonical schema.

    Finnhub raw trade fields:
        s  — symbol (e.g. "AAPL")
        p  — last price
        v  — volume
        t  — unix timestamp in milliseconds
        c  — list of trade condition codes

    Our canonical raw schema:
        ticker       — stock symbol
        price        — trade price (float)
        volume       — trade volume (int)
        timestamp_ms — epoch millis (int)
        timestamp    — ISO-8601 UTC string (for human readability)
        conditions   — trade condition codes (list[int])
        source       — "finnhub_ws"
    """
    ts_ms = trade.get("t", 0)
    return {
        "ticker": trade.get("s", _ticker),
        "price": float(trade.get("p", 0.0)),
        "volume": int(trade.get("v", 0)),
        "timestamp_ms": ts_ms,
        "timestamp": datetime.fromtimestamp(
            ts_ms / 1000, tz=timezone.utc
        ).isoformat(),
        "conditions": trade.get("c", []),
        "source": "finnhub_ws",
    }


def _emit(record: dict) -> None:
    """Write a processed trade record to stdout (and optionally to file)."""
    global _message_count
    line = json.dumps(record)
    print(line, flush=True)

    if _output_file is not None:
        _output_file.write(line + "\n")
        _output_file.flush()

    _message_count += 1
    if _message_count % 100 == 0:
        logger.info(f"Processed {_message_count} trade events so far")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Websocket callbacks
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def on_open(ws):
    """Subscribe to the configured ticker on connection open."""
    global _retry_count
    _retry_count = 0  # reset on successful connection
    subscribe_msg = json.dumps({"type": "subscribe", "symbol": _ticker})
    ws.send(subscribe_msg)
    logger.info(f"Connected & subscribed to {_ticker}")


def on_message(ws, message):
    """Parse incoming websocket messages and emit structured trade records."""
    try:
        data = json.loads(message)
    except json.JSONDecodeError:
        logger.warning(f"Malformed JSON received: {message[:200]}")
        return

    msg_type = data.get("type", "")

    # Finnhub sends {"type": "ping"} heartbeats — acknowledge silently
    if msg_type == "ping":
        return

    # Trade events arrive under {"type": "trade", "data": [...]}
    if msg_type == "trade":
        for trade in data.get("data", []):
            record = _process_trade(trade)
            _emit(record)
    else:
        # Log unexpected message types for debugging
        logger.debug(f"Non-trade message: {msg_type}")


def on_error(ws, error):
    """Log websocket errors."""
    logger.error(f"Websocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    """Handle websocket close — trigger reconnection logic."""
    logger.warning(
        f"Websocket closed (code={close_status_code}, msg={close_msg})"
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main loop with exponential-backoff reconnection
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def run(ticker: str, write_file: bool = False) -> None:
    """
    Start the websocket consumer with automatic reconnection.

    Args:
        ticker:     Stock symbol to subscribe to (e.g. "AAPL").
        write_file: If True, also write JSONL to data/raw/trades_{ticker}.jsonl.
    """
    global _ticker, _output_file, _retry_count, _running, _message_count

    validate()  # fail fast if API key is missing

    _ticker = ticker.upper()
    _message_count = 0

    # Optionally open a JSONL output file
    if write_file:
        RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
        out_path = RAW_DATA_DIR / f"trades_{_ticker}.jsonl"
        _output_file = open(out_path, "a", encoding="utf-8")
        logger.info(f"Also writing trades to {out_path}")

    logger.info(f"Starting Finnhub websocket producer for {_ticker}")

    delay = WS_RECONNECT_DELAY_SEC

    while _running:
        ws = websocket.WebSocketApp(
            FINNHUB_WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )

        # run_forever blocks until the connection drops
        ws.run_forever(ping_interval=30, ping_timeout=10)

        if not _running:
            break

        # ── Reconnection with exponential backoff ────────────────────────
        _retry_count += 1
        if _retry_count > WS_MAX_RETRIES:
            logger.critical(
                f"Exceeded {WS_MAX_RETRIES} reconnection attempts — giving up."
            )
            break

        logger.info(
            f"Reconnecting in {delay}s (attempt {_retry_count}/{WS_MAX_RETRIES})…"
        )
        time.sleep(delay)
        delay = min(delay * 2, WS_MAX_RECONNECT_DELAY_SEC)  # exponential backoff

    # ── Cleanup ──────────────────────────────────────────────────────────
    if _output_file is not None:
        _output_file.close()
        logger.info("Output file closed")

    logger.info(f"Producer stopped. Total trades processed: {_message_count}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CLI entry point
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Finnhub real-time trade websocket producer"
    )
    parser.add_argument(
        "--ticker",
        type=str,
        default=DEFAULT_TICKER,
        help=f"Stock ticker to subscribe to (default: {DEFAULT_TICKER})",
    )
    parser.add_argument(
        "--output-file",
        action="store_true",
        help="Also write trades to a JSONL file in data/raw/",
    )
    args = parser.parse_args()

    run(ticker=args.ticker, write_file=args.output_file)
