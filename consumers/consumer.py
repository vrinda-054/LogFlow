"""
consumers/consumer.py — Person 2 (Consumer Layer)
===================================================

Reads from the Kafka `logs` topic (4 partitions), validates each message
against shared/schemas/log_schema.json, and either:
  - Forwards valid records to processing/aggregator.py (HTTP POST), or
  - Routes invalid / repeatedly-failing records to the DLQ via dlq_handler.py.

Backpressure (REQ-17–REQ-20) is enforced before each poll by checking
buffer depth and consumer lag via backpressure.py.

Run three instances in separate terminals for a full consumer group:
    python consumer.py
    python consumer.py
    python consumer.py
"""

import json
import logging
import os
import queue
import signal
import sys
import threading
import time
from pathlib import Path

import jsonschema
import requests
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from dotenv import load_dotenv

import backpressure
import dlq_handler
from rebalance_config import get_consumer_config, get_topic

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------
load_dotenv(Path(__file__).parent.parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("consumer")

# ---------------------------------------------------------------------------
# Load JSON schema once at import time
# ---------------------------------------------------------------------------
_SCHEMA_PATH = Path(__file__).parent.parent / "shared" / "schemas" / "log_schema.json"

with _SCHEMA_PATH.open() as _f:
    _LOG_SCHEMA = json.load(_f)

_VALIDATOR = jsonschema.Draft7Validator(_LOG_SCHEMA)

# ---------------------------------------------------------------------------
# Aggregator forwarding config
# ---------------------------------------------------------------------------
_AGGREGATOR_URL = os.environ.get(
    "AGGREGATOR_URL", "http://localhost:8000/internal/ingest"
)
_AGGREGATOR_TIMEOUT = float(os.environ.get("AGGREGATOR_TIMEOUT_S", "2.0"))

# ---------------------------------------------------------------------------
# Internal buffer queue (consumer → aggregator forwarding thread)
# ---------------------------------------------------------------------------
_MAX_QUEUE_SIZE = int(os.environ.get("INTERNAL_QUEUE_SIZE", "2000"))
_forward_queue: queue.Queue[dict] = queue.Queue(maxsize=_MAX_QUEUE_SIZE)

# ---------------------------------------------------------------------------
# Shutdown event
# ---------------------------------------------------------------------------
_shutdown = threading.Event()

# ---------------------------------------------------------------------------
# Currently assigned TopicPartitions (updated in callbacks)
# ---------------------------------------------------------------------------
_assigned_tps: list[TopicPartition] = []


# ---------------------------------------------------------------------------
# Partition callbacks
# ---------------------------------------------------------------------------

def on_assign(consumer: Consumer, partitions: list[TopicPartition]) -> None:
    """
    Called by librdkafka when partitions are assigned to this consumer.
    With cooperative-sticky rebalance, this is called for NEWLY added
    partitions only — existing partitions are not revoked first.
    """
    global _assigned_tps
    logger.info(
        "[consumer] on_assign: partitions=%s",
        [p.partition for p in partitions],
    )
    _assigned_tps = list(partitions)
    # Seek to committed offsets for each assigned partition
    consumer.assign(partitions)


def on_revoke(consumer: Consumer, partitions: list[TopicPartition]) -> None:
    """
    Called before partitions are revoked (rebalance or shutdown).
    Commit current offsets for revoked partitions so no work is repeated
    after re-assignment.
    """
    logger.info(
        "[consumer] on_revoke: partitions=%s",
        [p.partition for p in partitions],
    )
    try:
        consumer.commit(offsets=partitions, asynchronous=False)
    except KafkaException as exc:
        logger.warning("[consumer] Could not commit on revoke: %s", exc)

    global _assigned_tps
    revoked_ids = {p.partition for p in partitions}
    _assigned_tps = [tp for tp in _assigned_tps if tp.partition not in revoked_ids]


def on_lost(consumer: Consumer, partitions: list[TopicPartition]) -> None:
    """
    Called when partitions are lost due to consumer failure / timeout.
    Unlike on_revoke, we cannot commit here — offsets may be re-processed.
    """
    logger.error(
        "[consumer] on_lost: partitions=%s — possible duplicate processing risk",
        [p.partition for p in partitions],
    )
    global _assigned_tps
    lost_ids = {p.partition for p in partitions}
    _assigned_tps = [tp for tp in _assigned_tps if tp.partition not in lost_ids]


# ---------------------------------------------------------------------------
# Message validation
# ---------------------------------------------------------------------------

def process_message(message_bytes: bytes) -> dict:
    """
    Deserialise and validate a raw Kafka message value.

    Parameters
    ----------
    message_bytes : bytes
        UTF-8 encoded JSON payload from the `logs` topic.

    Returns
    -------
    dict
        Validated log record conforming to shared/schemas/log_schema.json.

    Raises
    ------
    ValueError
        If the bytes are not valid UTF-8 JSON.
    jsonschema.ValidationError
        If the parsed JSON does not conform to log_schema.json.
    """
    # Step 1 — decode UTF-8
    try:
        raw = message_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"Invalid UTF-8 payload: {exc}") from exc

    # Step 2 — parse JSON
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"JSON parse error at position {exc.pos}: {exc.msg}") from exc

    # Step 3 — validate schema (raises jsonschema.ValidationError on failure)
    errors = list(_VALIDATOR.iter_errors(data))
    if errors:
        # Report all validation errors, not just the first
        messages = "; ".join(e.message for e in errors)
        raise jsonschema.ValidationError(
            f"Schema validation failed ({len(errors)} error(s)): {messages}"
        )

    return data


# ---------------------------------------------------------------------------
# Aggregator forwarding (background thread)
# ---------------------------------------------------------------------------

def _forward_worker() -> None:
    """
    Background thread: drains _forward_queue and POSTs validated log records
    to processing/aggregator.py via HTTP.

    Uses a small batch (up to 50 records per request) for efficiency.
    Falls back to individual sends if the batch endpoint is unavailable.
    """
    batch: list[dict] = []
    BATCH_SIZE   = int(os.environ.get("FORWARD_BATCH_SIZE", "50"))
    FLUSH_EVERY  = float(os.environ.get("FORWARD_FLUSH_INTERVAL_S", "1.0"))
    last_flush   = time.monotonic()

    logger.info("[consumer] Forward worker started → %s", _AGGREGATOR_URL)

    while not _shutdown.is_set() or not _forward_queue.empty():
        # Drain up to BATCH_SIZE messages
        try:
            record = _forward_queue.get(timeout=0.1)
            batch.append(record)
            _forward_queue.task_done()
        except queue.Empty:
            pass

        now = time.monotonic()
        should_flush = (
            len(batch) >= BATCH_SIZE
            or (batch and now - last_flush >= FLUSH_EVERY)
        )

        if should_flush and batch:
            _send_batch(batch)
            batch.clear()
            last_flush = now

    # Flush remainder on shutdown
    if batch:
        _send_batch(batch)

    logger.info("[consumer] Forward worker stopped.")


def _send_batch(records: list[dict]) -> None:
    """
    POST a batch of validated log records to the aggregator HTTP endpoint.
    Silently drops on network error (records already committed to Kafka —
    they will NOT be re-consumed). Log the error for ops visibility.
    """
    try:
        resp = requests.post(
            _AGGREGATOR_URL,
            json={"records": records},
            timeout=_AGGREGATOR_TIMEOUT,
        )
        resp.raise_for_status()
        logger.debug("[consumer] Forwarded %d records → aggregator", len(records))
    except requests.RequestException as exc:
        logger.error(
            "[consumer] Failed to forward %d records to aggregator: %s",
            len(records), exc
        )


# ---------------------------------------------------------------------------
# Backpressure check + enforce
# ---------------------------------------------------------------------------

def _apply_backpressure(consumer: Consumer, partition_id: int) -> None:
    """
    Check backpressure state for the partition and call pause/resume as needed.
    Updates the buffer depth from the current queue size first.
    """
    current_depth = _forward_queue.qsize()
    backpressure.update_buffer_depth(partition_id, current_depth)

    state = backpressure.check_backpressure(partition_id)

    if state == backpressure.BackpressureState.HIGH_WATER:
        backpressure.pause_partition(consumer, partition_id)

    elif state == backpressure.BackpressureState.LOW_WATER:
        backpressure.resume_partition(consumer, partition_id)


# ---------------------------------------------------------------------------
# Lag polling (runs on a timer inside the poll loop)
# ---------------------------------------------------------------------------
_LAG_POLL_INTERVAL = float(os.environ.get("LAG_POLL_INTERVAL_S", "15.0"))
_last_lag_poll     = 0.0


def _maybe_poll_lag(consumer: Consumer) -> None:
    """Poll consumer lag if enough time has elapsed since the last poll."""
    global _last_lag_poll
    now = time.monotonic()
    if now - _last_lag_poll >= _LAG_POLL_INTERVAL and _assigned_tps:
        lags = backpressure.poll_lag(consumer, _assigned_tps)
        logger.info("[consumer] Consumer lag snapshot: %s", lags)
        _last_lag_poll = now


# ---------------------------------------------------------------------------
# Main poll loop
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Entry point. Builds the consumer, subscribes to the logs topic, and runs
    the poll loop until SIGTERM or KeyboardInterrupt.
    """
    # -- Config ---------------------------------------------------------------
    config = get_consumer_config()
    topic  = get_topic()

    logger.info(
        "[consumer] Starting | broker=%s topic=%s group=%s",
        config["bootstrap.servers"], topic, config["group.id"],
    )

    # -- Consumer -------------------------------------------------------------
    consumer = Consumer(config)
    consumer.subscribe(
        [topic],
        on_assign=on_assign,
        on_revoke=on_revoke,
        on_lost=on_lost,
    )

    # -- Graceful shutdown signal handlers ------------------------------------
    def _handle_signal(signum, frame):
        logger.info("[consumer] Signal %d received — shutting down…", signum)
        _shutdown.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT,  _handle_signal)

    # -- Background forwarding thread -----------------------------------------
    fwd_thread = threading.Thread(target=_forward_worker, daemon=False, name="forwarder")
    fwd_thread.start()

    # -- Poll loop ------------------------------------------------------------
    committed_offsets: dict[int, int] = {}   # partition_id → last committed offset

    try:
        while not _shutdown.is_set():

            # Periodically poll lag and enforce backpressure for all partitions
            _maybe_poll_lag(consumer)
            for tp in _assigned_tps:
                _apply_backpressure(consumer, tp.partition)

            # Poll for one message (timeout lets us re-check _shutdown)
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue   # timeout — re-check shutdown flag

            # Handle Kafka errors
            if msg.error():
                err = msg.error()
                if err.code() == KafkaError._PARTITION_EOF:
                    # End of partition — not an error; just no new messages
                    logger.debug(
                        "[consumer] Reached end of partition=%d offset=%d",
                        msg.partition(), msg.offset(),
                    )
                    continue
                else:
                    logger.error(
                        "[consumer] Kafka error on partition=%d: %s",
                        msg.partition(), err
                    )
                    continue

            partition_id = msg.partition()

            # Enforce backpressure before processing this message
            _apply_backpressure(consumer, partition_id)

            # Process (validate) the message
            try:
                record = process_message(msg.value())

            except jsonschema.ValidationError as exc:
                # Schema violation — send directly to DLQ without retry
                logger.warning(
                    "[consumer] Schema validation failed | partition=%d offset=%d | %s",
                    partition_id, msg.offset(), exc.message[:200],
                )
                dlq_handler.publish_to_dlq(
                    original_message=msg.value().decode("utf-8", errors="replace"),
                    failure_reason=f"Schema validation failed: {exc.message}",
                    retry_count=0,
                )
                # Commit the offset so we don't re-process this bad message
                consumer.commit(message=msg, asynchronous=True)
                continue

            except ValueError as exc:
                # Unparseable bytes — DLQ immediately
                logger.warning(
                    "[consumer] Unparseable message | partition=%d offset=%d | %s",
                    partition_id, msg.offset(), exc,
                )
                dlq_handler.publish_to_dlq(
                    original_message=msg.value().decode("utf-8", errors="replace"),
                    failure_reason=str(exc),
                    retry_count=0,
                )
                consumer.commit(message=msg, asynchronous=True)
                continue

            # Valid record — enqueue for forwarding to aggregator
            try:
                _forward_queue.put_nowait(record)
            except queue.Full:
                # Queue full (extreme backpressure) — still commit offset,
                # log the drop. Backpressure should have paused us before this.
                logger.error(
                    "[consumer] Forward queue FULL — dropping record "
                    "partition=%d offset=%d service=%s",
                    partition_id, msg.offset(), record.get("service"),
                )

            # Manual offset commit (at-least-once)
            consumer.commit(message=msg, asynchronous=True)
            committed_offsets[partition_id] = msg.offset()

            logger.debug(
                "[consumer] ✓ partition=%d offset=%d service=%s severity=%s",
                partition_id, msg.offset(),
                record.get("service"), record.get("severity"),
            )

    except Exception as exc:               # noqa: BLE001
        logger.exception("[consumer] Unexpected error in poll loop: %s", exc)

    finally:
        # -- Shutdown sequence ------------------------------------------------
        logger.info("[consumer] Closing consumer — flushing pending commits…")

        # Signal forwarding thread to drain and stop
        _shutdown.set()
        fwd_thread.join(timeout=15)

        # Flush in-flight DLQ messages
        dlq_handler.flush(timeout=10)

        # Final synchronous offset commit
        try:
            consumer.commit(asynchronous=False)
        except KafkaException as exc:
            logger.warning("[consumer] Final commit failed: %s", exc)

        consumer.close()
        logger.info("[consumer] Shutdown complete.")


if __name__ == "__main__":
    main()
