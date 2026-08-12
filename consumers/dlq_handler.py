"""
consumers/dlq_handler.py — Person 2 (Consumer Layer)
======================================================

Handles failed messages within the consumer layer. When consumer.py cannot
successfully process a Kafka message (schema violation, parse error, or repeated
transient failures), it delegates here to either retry or publish to the DLQ.

OUTPUT → Kafka topic `logs-dlq` : UTF-8 JSON conforming to dlq_schema.json
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

from confluent_kafka import Producer, KafkaException

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
_DLQ_TOPIC   = os.environ.get("KAFKA_TOPIC_DLQ", "logs-dlq")
_KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")

# ---------------------------------------------------------------------------
# Singleton DLQ producer
# ---------------------------------------------------------------------------
_dlq_producer: Producer | None = None


def _get_dlq_producer() -> Producer:
    """
    Lazily initialise and return a module-level singleton confluent-kafka
    Producer dedicated to publishing DLQ messages.

    Returns
    -------
    confluent_kafka.Producer
        Thread-safe; safe to call produce() from multiple threads.

    Raises
    ------
    KafkaException
        If the broker is unreachable during the first call.
    """
    global _dlq_producer
    if _dlq_producer is None:
        broker = os.environ.get("KAFKA_BROKER", "localhost:9092")
        _dlq_producer = Producer({
            "bootstrap.servers": broker,
            "acks":              "all",   # wait for leader + all ISR replicas
            "retries":           5,
            "retry.backoff.ms":  300,
        })
        logger.info("[dlq_handler] DLQ producer initialised → broker=%s", broker)
    return _dlq_producer


# ---------------------------------------------------------------------------
# Delivery callback
# ---------------------------------------------------------------------------
def _delivery_report(err, msg) -> None:
    """
    Callback invoked by confluent-kafka after each produce() attempt.
    Logs success or failure of DLQ message delivery.
    """
    if err:
        logger.error(
            "[dlq_handler] DLQ delivery FAILED | topic=%s partition=%s error=%s",
            msg.topic(), msg.partition(), err
        )
    else:
        logger.debug(
            "[dlq_handler] DLQ delivery OK | topic=%s partition=%s offset=%s",
            msg.topic(), msg.partition(), msg.offset()
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def publish_to_dlq(
    original_message,
    failure_reason: str,
    retry_count: int,
) -> None:
    """
    Wrap the failed message in a DLQ envelope and publish it to `logs-dlq`.

    Parameters
    ----------
    original_message : str | dict
        The original payload. Pass as dict if valid JSON was parsed,
        or as a raw string if the payload was unparseable.
    failure_reason   : str
        Human-readable explanation of the failure (truncated to 1024 chars).
    retry_count      : int
        Number of retries already attempted (0 = sent to DLQ immediately).

    Behaviour
    ---------
    - Wraps payload in dlq_schema.json envelope.
    - Publishes to `logs-dlq` Kafka topic (1 partition, no key needed).
    - Calls producer.poll(0) to serve any pending delivery callbacks.
    - Does NOT flush — flushing happens on consumer shutdown.

    Raises
    ------
    KafkaException
        If the local produce queue is full (BufferError) or broker errors.
    """
    envelope = {
        "original_message": original_message,
        "failure_reason":   failure_reason[:1024],
        "retry_count":      retry_count,
        "failed_at":        datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    }

    payload = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
    producer = _get_dlq_producer()

    try:
        producer.produce(
            topic=_DLQ_TOPIC,
            value=payload,
            on_delivery=_delivery_report,
        )
        producer.poll(0)  # serve delivery callbacks without blocking
    except BufferError:
        # Local queue full → flush enough to make room, then retry once
        logger.warning("[dlq_handler] Producer queue full — flushing before retry")
        producer.flush(timeout=5)
        producer.produce(
            topic=_DLQ_TOPIC,
            value=payload,
            on_delivery=_delivery_report,
        )

    logger.warning(
        "[dlq_handler] → DLQ | topic=%s reason='%s' retries=%d",
        _DLQ_TOPIC, failure_reason[:120], retry_count
    )


def retry_with_backoff(
    fn,
    message_bytes: bytes,
    max_retries: int = 3,
    base_delay: float = 0.5,
) -> bool:
    """
    Attempt fn(message_bytes) up to max_retries times with exponential backoff.
    If all retries fail, publishes the message to the DLQ automatically.

    Parameters
    ----------
    fn            : callable   fn(bytes) → Any
    message_bytes : bytes      Raw Kafka message payload.
    max_retries   : int        Maximum retry attempts (default: 3).
    base_delay    : float      Initial backoff in seconds (doubles each attempt).

    Returns
    -------
    bool
        True  — fn succeeded within the retry budget.
        False — all retries exhausted; message was sent to DLQ.
    """
    last_exc: Exception | None = None

    for attempt in range(max_retries + 1):
        try:
            fn(message_bytes)
            if attempt > 0:
                logger.info(
                    "[dlq_handler] Recovered on attempt %d/%d", attempt, max_retries
                )
            return True

        except Exception as exc:          # noqa: BLE001
            last_exc = exc
            if attempt < max_retries:
                delay = base_delay * (2 ** attempt)
                logger.warning(
                    "[dlq_handler] Attempt %d/%d failed — retrying in %.1fs | %s",
                    attempt + 1, max_retries, delay, exc
                )
                time.sleep(delay)

    # All retries exhausted → DLQ
    logger.error(
        "[dlq_handler] All %d retries failed — routing to DLQ | %s",
        max_retries, last_exc
    )
    publish_to_dlq(
        original_message=message_bytes.decode("utf-8", errors="replace"),
        failure_reason=str(last_exc),
        retry_count=max_retries,
    )
    return False


def flush(timeout: float = 10.0) -> None:
    """
    Block until all pending DLQ produce requests are delivered or timeout.
    Call this on consumer shutdown to avoid losing in-flight DLQ messages.

    Parameters
    ----------
    timeout : float   Max seconds to wait (default: 10).
    """
    if _dlq_producer is not None:
        remaining = _dlq_producer.flush(timeout=timeout)
        if remaining > 0:
            logger.warning(
                "[dlq_handler] flush() timed out — %d DLQ messages may be lost",
                remaining
            )
        else:
            logger.info("[dlq_handler] flush() complete — all DLQ messages delivered")
