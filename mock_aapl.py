import json
import time
import random
from datetime import datetime, timezone
from confluent_kafka import Producer

def run_mock():
    conf = {"bootstrap.servers": "localhost:9092", "client.id": "mock-aapl-producer"}
    producer = Producer(conf)
    
    price = 185.50
    print("Starting MOCK AAPL Producer. Publishing fake live trades...")
    
    while True:
        # Simulate small price movements
        price += random.uniform(-0.10, 0.10)
        ts_ms = int(time.time() * 1000)
        
        trade = {
            "ticker": "AAPL",
            "price": round(price, 2),
            "volume": random.randint(10, 500),
            "timestamp_ms": ts_ms,
            "timestamp": datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat(),
            "conditions": [],
            "source": "mock_generator"
        }
        
        producer.produce(
            topic="stock-prices-raw",
            key="AAPL".encode("utf-8"),
            value=json.dumps(trade).encode("utf-8")
        )
        producer.poll(0)
        print(f"Published MOCK trade: {trade}")
        time.sleep(1)

if __name__ == "__main__":
    run_mock()
