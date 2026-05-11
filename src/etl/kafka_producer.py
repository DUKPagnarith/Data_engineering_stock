"""
kafka_producer.py — Kafka producer that bridges Finnhub websocket → Kafka topic.

Connects to the Finnhub websocket feed (Stage 1) and publishes each
normalised trade event as a JSON message to the `stock-prices-raw` Kafka topic.

Malformed messages that fail validation are routed to the `stock-prices-dlq`
dead-letter topic for later inspection.

Usage:
    python -m src.etl.kafka_producer                  # uses defaults from .env
    python -m src.etl.kafka_producer --ticker MSFT     # override ticker
"""

import argparse
import json
import sys
import time
import signal
import logging
from datetime import datetime, timezone
from pathlib import Path

import websocket as ws_client
from confluent_kafka import Producer, KafkaError

# ── Project imports ──────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import (
    FINNHUB_WS_URL,
    DEFAULT_TICKER,
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

# ── Kafka configuration ─────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_RAW = "stock-prices-raw"
TOPIC_DLQ = "stock-prices-dlq"

# ── Global state ─────────────────────────────────────────────────────────────
_running = True
_retry_count = 0
_ticker = DEFAULT_TICKER
_message_count = 0
_dlq_count = 0
_kafka_producer: Producer = None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Signal handling
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _shutdown_handler(signum, frame):
    global _running
    logger.info("Shutdown signal received — flushing Kafka producer…")
    _running = False


signal.signal(signal.SIGINT, _shutdown_handler)
signal.signal(signal.SIGTERM, _shutdown_handler)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Kafka helpers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _create_kafka_producer() -> Producer:
    """Create and return a confluent-kafka Producer."""
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "finnhub-stock-producer",
        # Delivery reliability settings
        "acks": "all",                # wait for all in-sync replicas
        "retries": 5,                 # retry transient failures
        "retry.backoff.ms": 500,
        # Batching for throughput (small batches since trades are bursty)
        "batch.size": 16384,          # 16 KB batch
        "linger.ms": 50,              # wait up to 50ms to fill a batch
        "compression.type": "snappy", # compress for efficiency
    }
    return Producer(conf)


def _delivery_callback(err, msg):
    """Called once per message to confirm delivery or log failures."""
    if err is not None:
        logger.error(f"Delivery failed for {msg.key()}: {err}")
    else:
        logger.debug(
            f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}"
        )


def _publish(topic: str, key: str, value: dict) -> None:
    """Serialise and publish a message to the given Kafka topic."""
    try:
        _kafka_producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=json.dumps(value).encode("utf-8"),
            callback=_delivery_callback,
        )
        # Trigger any queued delivery callbacks without blocking
        _kafka_producer.poll(0)
    except BufferError:
        logger.warning("Kafka producer buffer full — flushing…")
        _kafka_producer.flush(timeout=5)
        # Retry once after flush
        _kafka_producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=json.dumps(value).encode("utf-8"),
            callback=_delivery_callback,
        )


def _send_to_dlq(raw_message: str, reason: str) -> None:
    """Route a malformed message to the dead-letter queue."""
    global _dlq_count
    dlq_record = {
        "original_message": raw_message[:2000],  # cap size
        "error_reason": reason,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ticker": _ticker,
    }
    _publish(TOPIC_DLQ, _ticker, dlq_record)
    _dlq_count += 1
    logger.warning(f"Sent to DLQ ({reason}). Total DLQ: {_dlq_count}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Message validation & processing
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _validate_trade(trade: dict) -> bool:
    """
    Validate a raw Finnhub trade event has required fields with correct types.

    Required: s (str), p (number > 0), v (number >= 0), t (int > 0)
    """
    try:
        symbol = trade.get("s")
        price = trade.get("p")
        volume = trade.get("v")
        ts = trade.get("t")

        if not isinstance(symbol, str) or len(symbol) == 0:
            return False
        if not isinstance(price, (int, float)) or price <= 0:
            return False
        if not isinstance(volume, (int, float)) or volume < 0:
            return False
        if not isinstance(ts, (int, float)) or ts <= 0:
            return False

        return True
    except Exception:
        return False


def _process_trade(trade: dict) -> dict:
    """
    Transform a validated Finnhub trade event into our canonical schema.

    Output schema matches Stage 1 definition:
        ticker, price, volume, timestamp_ms, timestamp, conditions, source
    """
    ts_ms = int(trade["t"])
    return {
        "ticker": trade["s"],
        "price": float(trade["p"]),
        "volume": int(trade["v"]),
        "timestamp_ms": ts_ms,
        "timestamp": datetime.fromtimestamp(
            ts_ms / 1000, tz=timezone.utc
        ).isoformat(),
        "conditions": trade.get("c", []),
        "source": "finnhub_ws",
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Websocket callbacks
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def on_open(ws):
    global _retry_count
    _retry_count = 0
    subscribe_msg = json.dumps({"type": "subscribe", "symbol": _ticker})
    ws.send(subscribe_msg)
    logger.info(f"Connected & subscribed to {_ticker} → publishing to Kafka [{TOPIC_RAW}]")


def on_message(ws, message):
    global _message_count

    try:
        data = json.loads(message)
    except json.JSONDecodeError:
        _send_to_dlq(message[:500], "invalid_json")
        return

    msg_type = data.get("type", "")

    if msg_type == "ping":
        return

    if msg_type == "trade":
        for trade in data.get("data", []):
            if _validate_trade(trade):
                record = _process_trade(trade)
                _publish(TOPIC_RAW, record["ticker"], record)
                _message_count += 1
                if _message_count % 100 == 0:
                    logger.info(
                        f"Published {_message_count} trades to Kafka "
                        f"(DLQ: {_dlq_count})"
                    )
            else:
                _send_to_dlq(json.dumps(trade), "validation_failed")


def on_error(ws, error):
    logger.error(f"Websocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    logger.warning(f"Websocket closed (code={close_status_code}, msg={close_msg})")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main loop
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def run(ticker: str) -> None:
    """
    Start the Finnhub → Kafka pipeline with automatic reconnection.

    Args:
        ticker: Stock symbol to subscribe to (e.g. "AAPL").
    """
    global _ticker, _kafka_producer, _retry_count, _running, _message_count, _dlq_count

    validate()  # fail fast if Finnhub API key is missing

    _ticker = ticker.upper()
    _message_count = 0
    _dlq_count = 0

    # Initialise Kafka producer
    _kafka_producer = _create_kafka_producer()
    logger.info(f"Kafka producer connected to {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Starting Finnhub → Kafka pipeline for {_ticker}")

    delay = WS_RECONNECT_DELAY_SEC

    while _running:
        ws = ws_client.WebSocketApp(
            FINNHUB_WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )

        ws.run_forever(ping_interval=30, ping_timeout=10)

        if not _running:
            break

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
        delay = min(delay * 2, WS_MAX_RECONNECT_DELAY_SEC)

    # ── Cleanup ──────────────────────────────────────────────────────────
    remaining = _kafka_producer.flush(timeout=10)
    if remaining > 0:
        logger.warning(f"{remaining} messages were not delivered on shutdown")
    logger.info(
        f"Producer stopped. Total published: {_message_count}, DLQ: {_dlq_count}"
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CLI entry point
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Finnhub → Kafka real-time trade producer"
    )
    parser.add_argument(
        "--ticker",
        type=str,
        default=DEFAULT_TICKER,
        help=f"Stock ticker to subscribe to (default: {DEFAULT_TICKER})",
    )
    args = parser.parse_args()

    run(ticker=args.ticker)
