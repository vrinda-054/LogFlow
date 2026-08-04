"""
consumers/dlq_handler.py — Person 2 (Consumer Layer)
======================================================

Role
----
Handles failed messages within the consumer layer. When consumer.py cannot
successfully process a Kafka message (schema violation, parse error, or repeated
transient failures), it delegates here to either retry the message or publish
it to the Dead Letter Queue (DLQ).

Upstream / Downstream Contracts
--------------------------------
  CALLED BY ← consumers/consumer.py:
    - retry_with_backoff(fn, message_bytes, max_retries) — retry a failed handler
    - publish_to_dlq(original_message, failure_reason, retry_count) — send to DLQ

  OUTPUT → Kafka topic `logs-dlq` (env: KAFKA_TOPIC_DLQ):
    Each DLQ message is a UTF-8 JSON string conforming to
    shared/schemas/dlq_schema.json:
      {
        "original_message" : <original payload as object or raw string>,
        "failure_reason"   : <str>,
        "retry_count"      : <int>,
        "failed_at"        : <ISO 8601 UTC timestamp>
      }

  OUTPUT → processing/db/schema.sql (table: dlq_log):
    Person 3's aggregator also persists DLQ events for the FastAPI
    /dlq/messages endpoint consumed by the React dashboard (Person 4).

Retry Strategy
--------------
  Exponential backoff: delay = base_delay * (2 ** attempt)
  Default: base_delay=0.5s, max_retries=3
  After max_retries exhausted → publish_to_dlq() is called automatically.

Key Functions (to be implemented)
----------------------------------
  retry_with_backoff(fn, message_bytes, max_retries=3) → bool
  publish_to_dlq(original_message, failure_reason, retry_count) → None
"""

import os
import json
import time
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# TODO (Person 2): initialise a confluent_kafka.Producer for DLQ publishing
# ---------------------------------------------------------------------------
# from confluent_kafka import Producer
# _dlq_producer = None   # initialised lazily in _get_dlq_producer()

_DLQ_TOPIC = os.environ.get("KAFKA_TOPIC_DLQ", "logs-dlq")


def _get_dlq_producer():
    """
    Lazily initialise and return a shared confluent-kafka Producer for the DLQ.

    Returns
    -------
    confluent_kafka.Producer
        Configured to connect to KAFKA_BROKER.

    Notes
    -----
    Uses a module-level singleton to avoid creating a new Producer per message.
    Thread-safety: confluent-kafka Producer is thread-safe for produce() calls.
    """
    # TODO: implement singleton Producer init
    raise NotImplementedError("_get_dlq_producer: not yet implemented")


def retry_with_backoff(fn, message_bytes: bytes, max_retries: int = 3) -> bool:
    """
    Attempt to call `fn(message_bytes)` up to `max_retries` times with
    exponential backoff between attempts.

    Parameters
    ----------
    fn           : callable
        The processing function to retry. Signature: fn(bytes) → Any.
        Expected to be consumer.process_message or a downstream handler.
    message_bytes: bytes
        The raw Kafka message payload to reprocess.
    max_retries  : int
        Maximum number of retry attempts before giving up (default: 3).

    Returns
    -------
    bool
        True if fn succeeded within max_retries attempts, False otherwise.

    Side Effects
    ------------
    If all retries fail, calls publish_to_dlq() with retry_count=max_retries
    and failure_reason taken from the last exception.
    """
    base_delay = 0.5  # seconds
    last_exc   = None

    for attempt in range(max_retries + 1):
        try:
            fn(message_bytes)
            return True
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries:
                delay = base_delay * (2 ** attempt)
                print(f"[dlq_handler] Retry {attempt + 1}/{max_retries} "
                      f"after {delay:.1f}s — {exc}")
                time.sleep(delay)

    # All retries exhausted
    publish_to_dlq(
        original_message=message_bytes.decode("utf-8", errors="replace"),
        failure_reason=str(last_exc),
        retry_count=max_retries,
    )
    return False


def publish_to_dlq(
    original_message,
    failure_reason: str,
    retry_count: int,
) -> None:
    """
    Wrap the failed message in a DLQ envelope and publish it to the `logs-dlq`
    Kafka topic.

    Parameters
    ----------
    original_message : str | dict
        The original message payload. Pass as a dict if it was valid JSON,
        or as a raw string if it could not be parsed.
    failure_reason   : str
        Human-readable explanation of the failure (≤1024 chars).
    retry_count      : int
        Number of retries attempted before this DLQ publication (≥0).

    Output
    ------
    Publishes a single message to Kafka topic `logs-dlq`.
    The message key is None (DLQ topic has 1 partition; no ordering needed).
    The message value is a UTF-8 JSON string conforming to dlq_schema.json.

    Raises
    ------
    RuntimeError
        If the Kafka produce call fails after the internal delivery callback
        reports an error.
    """
    envelope = {
        "original_message": original_message,
        "failure_reason":   failure_reason[:1024],
        "retry_count":      retry_count,
        "failed_at":        datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    }
    print(f"[dlq_handler] Publishing to DLQ | reason='{failure_reason[:80]}...' "
          f"retries={retry_count}")
    # TODO: producer = _get_dlq_producer()
    # TODO: producer.produce(_DLQ_TOPIC, value=json.dumps(envelope).encode("utf-8"))
    # TODO: producer.poll(0)
