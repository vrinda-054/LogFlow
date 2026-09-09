"""Publish valid and malformed records for a manual consumer/DLQ smoke test.

Run with Kafka available:
    python tests/manual_test_consumer.py
    python consumers/consumer.py --consumer-id consumer-01
"""

from __future__ import annotations

import json
import os
import uuid
import argparse
from datetime import datetime, timezone

try:
    from confluent_kafka import Producer
except ImportError as exc:  # pragma: no cover
    raise SystemExit("Install consumers/requirements.txt first") from exc


def valid_message(index: int) -> dict[str, str]:
    """Build a record using exactly shared/schemas/log_schema.json fields."""
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "service": "manual-test",
        "severity": "INFO" if index % 2 else "ERROR",
        "message": f"manual consumer test message {index}",
        "trace_id": uuid.uuid4().hex,
    }


def main() -> None:
    """Publish valid records and three deliberately bad payloads."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=3, help="Number of valid records to publish")
    args = parser.parse_args()
    if args.count < 0:
        parser.error("--count must be non-negative")

    broker = os.environ.get("KAFKA_BROKER", "localhost:9092")
    topic = os.environ.get("KAFKA_TOPIC_LOGS", "logs-raw")
    producer = Producer({"bootstrap.servers": broker})
    payloads = [
        json.dumps(valid_message(index)).encode("utf-8") for index in range(args.count)
    ] + [
        json.dumps({"timestamp": datetime.now(timezone.utc).isoformat(), "service": "manual-test"}).encode("utf-8"),
        b"{invalid_json_payload",
        json.dumps({**valid_message(99), "trace_id": "not-a-trace-id"}).encode("utf-8"),
    ]
    for payload in payloads:
        producer.produce(topic, value=payload)
        producer.poll(0)
    producer.flush()
    print(f"Published {len(payloads)} messages to {topic}; watch consumer logs for retries and DLQ publication.")


if __name__ == "__main__":
    main()
