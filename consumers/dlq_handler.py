"""Retries and publishes failed records to the ``logs-dlq`` Kafka topic."""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Callable

try:
    from confluent_kafka import Producer
except ImportError:  # pragma: no cover
    Producer = None

logger = logging.getLogger(__name__)
_producer: Any = None


@dataclass(frozen=True)
class RetryAttempt:
    """Typed retry-history item embedded in the DLQ envelope."""

    attempt: int
    result: str
    timestamp: str
    reason: str


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _get_dlq_producer() -> Any:
    """Lazily create the Kafka producer used for DLQ messages."""
    global _producer
    if _producer is None:
        if Producer is None:
            raise RuntimeError("confluent-kafka package is not installed")
        broker = os.environ.get("KAFKA_BROKER")
        if not broker:
            raise EnvironmentError("KAFKA_BROKER not set. See .env.example.")
        _producer = Producer({"bootstrap.servers": broker, "acks": "all"})
    return _producer


def classify_failure(exception_or_validation_result: Any) -> str:
    """Map common failures to stable, human-readable reasons."""
    text = str(exception_or_validation_result)
    name = type(exception_or_validation_result).__name__.lower()
    if isinstance(exception_or_validation_result, (json.JSONDecodeError, UnicodeDecodeError)) or "json" in name:
        return f"Deserialization error: {text}"[:1024]
    if "validation" in name or "schema" in text.lower() or "required property" in text.lower():
        return f"JSON schema validation failed: {text}"[:1024]
    if "deserialization" in text.lower() or "utf-8" in text.lower():
        return f"Deserialization error: {text}"[:1024]
    return f"Processing failure: {text}"[:1024]


def retry_with_backoff(
    message: dict[str, Any] | str,
    process_fn: Callable[[dict[str, Any] | str], None],
    max_retries: int = 3,
    base_delay_ms: int = 200,
) -> tuple[bool, list[dict[str, Any]]]:
    """Process a message, retry failures, and route final failure to the DLQ."""
    if max_retries < 0 or base_delay_ms < 0:
        raise ValueError("max_retries and base_delay_ms must be non-negative")
    history: list[dict[str, Any]] = []
    last_reason = "Processing failure: unknown error"
    for attempt in range(1, max_retries + 2):
        try:
            process_fn(message)
            history.append(asdict(RetryAttempt(attempt, "success", _now(), "")))
            logger.info(
                "event=%s",
                json.dumps({"timestamp": _now(), "event": "retry_attempt", "attempt": attempt, "result": "success"}),
            )
            return True, history
        except Exception as exc:
            last_reason = classify_failure(exc)
            history.append(asdict(RetryAttempt(attempt, "failure", _now(), last_reason)))
            logger.warning(
                "event=%s",
                json.dumps({
                    "timestamp": _now(),
                    "event": "retry_attempt",
                    "attempt": attempt,
                    "result": "failure",
                    "reason": last_reason,
                }),
            )
            if attempt <= max_retries:
                time.sleep((base_delay_ms * (2 ** (attempt - 1))) / 1000)
    publish_to_dlq(message, last_reason, history)
    return False, history


def publish_to_dlq(
    message: dict[str, Any] | str, failure_reason: str, retry_history: list[dict[str, Any]]
) -> dict[str, Any]:
    """Publish a stable DLQ envelope and return it for inspection/tests."""
    envelope = {
        "original_message": message,
        "failure_reason": failure_reason[:1024] or "Unknown processing failure",
        "retry_count": sum(1 for item in retry_history if item.get("result") == "failure"),
        "failed_at": _now(),
        "retry_history": retry_history,
    }
    producer = _get_dlq_producer()
    topic = os.environ.get("KAFKA_TOPIC_DLQ", "logs-dlq")
    producer.produce(topic, value=json.dumps(envelope).encode("utf-8"))
    producer.poll(0)
    logger.error(
        "event=%s",
        json.dumps({
            "timestamp": envelope["failed_at"],
            "event": "dlq_publish",
            "topic": topic,
            "retry_count": envelope["retry_count"],
            "failure_reason": envelope["failure_reason"],
        }),
    )
    return envelope