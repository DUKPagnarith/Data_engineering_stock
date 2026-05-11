"""
kafka_consumer.py — Verification consumer for the stock-prices-raw Kafka topic.

Reads messages from `stock-prices-raw` and prints them to stdout for
visual verification that the producer pipeline is working correctly.

Also optionally reads from the `stock-prices-dlq` dead-letter topic
when invoked with --dlq flag.

Usage:
    python -m src.etl.kafka_consumer                # consume from stock-prices-raw
    python -m src.etl.kafka_consumer --dlq           # consume from stock-prices-dlq
    python -m src.etl.kafka_consumer --max-messages 50  # stop after 50 messages
"""

import argparse
import json
import sys
import signal
import logging
from pathlib import Path

from confluent_kafka import Consumer, KafkaError

# ── Project imports ──────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import DEFAULT_TICKER

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


def _shutdown_handler(signum, frame):
    global _running
    logger.info("Shutdown signal received…")
    _running = False


signal.signal(signal.SIGINT, _shutdown_handler)
signal.signal(signal.SIGTERM, _shutdown_handler)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Consumer
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def run(topic: str, max_messages: int = 0) -> None:
    """
    Consume and print messages from the given Kafka topic.

    Args:
        topic:        Kafka topic to consume from.
        max_messages: Stop after this many messages (0 = unlimited).
    """
    global _running

    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "stock-price-verifier",
        "auto.offset.reset": "earliest",  # read from beginning
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic])
    logger.info(f"Consuming from topic: {topic} (Ctrl+C to stop)")

    count = 0

    try:
        while _running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(
                        f"Reached end of partition {msg.partition()} "
                        f"at offset {msg.offset()}"
                    )
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                continue

            # Decode and pretty-print the message
            key = msg.key().decode("utf-8") if msg.key() else None
            try:
                value = json.loads(msg.value().decode("utf-8"))
                formatted = json.dumps(value, indent=2)
            except (json.JSONDecodeError, UnicodeDecodeError):
                formatted = msg.value().decode("utf-8", errors="replace")

            count += 1
            print(
                f"─── Message #{count} | "
                f"Partition: {msg.partition()} | "
                f"Offset: {msg.offset()} | "
                f"Key: {key} ───"
            )
            print(formatted)
            print()

            if max_messages > 0 and count >= max_messages:
                logger.info(f"Reached max messages ({max_messages}) — stopping")
                break

    finally:
        consumer.close()
        logger.info(f"Consumer closed. Total messages read: {count}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CLI entry point
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Verification consumer for stock price Kafka topics"
    )
    parser.add_argument(
        "--dlq",
        action="store_true",
        help="Consume from the dead-letter queue instead of the main topic",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Stop after N messages (0 = unlimited)",
    )
    args = parser.parse_args()

    topic = TOPIC_DLQ if args.dlq else TOPIC_RAW
    run(topic=topic, max_messages=args.max_messages)
