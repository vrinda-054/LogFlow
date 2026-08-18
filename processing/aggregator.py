"""
processing/aggregator.py — Person 3 (Processing Layer)
========================================================

Role
----
Receives validated log records from consumers/consumer.py, enriches and
aggregates them, then persists results to PostgreSQL via processing/db/connection.py.
Operates as a stateful, continuously-running process (or can be called as a
library by a FastAPI background task).

Upstream / Downstream Contracts
--------------------------------
  INPUT ← consumers/consumer.py:
    A stream of validated dict objects, each conforming to
    shared/schemas/log_schema.json:
      {
        "timestamp" : str (ISO 8601 UTC),
        "service"   : str,
        "severity"  : str,
        "message"   : str,
        "trace_id"  : str (32-char hex)
      }

  OUTPUT → PostgreSQL (via processing/db/connection.py):
    Inserts into tables defined in processing/db/schema.sql:
      - processed_logs       : raw validated log records
      - metrics_throughput   : messages/second per service (1-min rolling windows)
      - metrics_error_rate   : error% per service per window
      - metrics_consumer_lag : Kafka lag per partition (polled from AdminClient)
      - dlq_log              : DLQ events forwarded from consumers/dlq_handler.py

  OUTPUT → FastAPI (processing/api/main.py):
    Data is queried by the API from PostgreSQL; aggregator writes DB only.

Aggregation Windows
-------------------
  Messages are bucketed into 1-minute tumbling windows (configurable).
  Within each window the aggregator computes:
    - throughput    : count of messages per service
    - error_rate    : count(severity IN ('ERROR','CRITICAL')) / total * 100
    - consumer_lag  : latest lag snapshot from Kafka AdminClient

Input Interface (callable form — for testing/integration)
-----------------------------------------------------------
  ingest(log_record: dict) → None
    Add one validated log record to the aggregation state machine.

  flush_window() → None
    Force-write current window metrics to DB; called at window boundary.
"""

import os
import json
import signal
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient, ListConsumerGroupOffsetsRequest
from confluent_kafka import TopicPartition

from db.connection import get_connection

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
)

# ---------------------------------------------------------------------------
# Configuration (all from environment, with sane defaults)
# ---------------------------------------------------------------------------
WINDOW_SECONDS = int(os.environ.get("AGGREGATION_WINDOW_SECONDS", "60"))
LAG_POLL_SECONDS = int(os.environ.get("LAG_POLL_SECONDS", "30"))
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC_LOGS = os.environ.get("KAFKA_TOPIC_LOGS", "logs")
KAFKA_TOPIC_DLQ = os.environ.get("KAFKA_TOPIC_DLQ", "logs-dlq")
KAFKA_CONSUMER_GROUP = os.environ.get("KAFKA_CONSUMER_GROUP", "logflow-group")

# The aggregator uses its OWN consumer group so it reads independently
# from Person 2's consumer group. This way Person 2's consumers and the
# aggregator both get every message without interfering with each other.
AGGREGATOR_GROUP = "logflow-aggregator"

# Severity levels that count towards the error rate metric.
ERROR_SEVERITIES = {"ERROR", "CRITICAL"}

# ---------------------------------------------------------------------------
# In-memory aggregation state
# ---------------------------------------------------------------------------
# Buffer of raw log records for batch insert into processed_logs.
_log_buffer: list[dict] = []

# Per-service message count for the current window.
_throughput_counters: dict[str, int] = defaultdict(int)

# Per-service error count (severity in ERROR_SEVERITIES) for the current window.
_error_counters: dict[str, int] = defaultdict(int)

# Timestamp marking the start of the current aggregation window.
_window_start: datetime = datetime.now(timezone.utc)

# Flag for graceful shutdown via SIGTERM/SIGINT.
_shutdown_requested = False


# ═══════════════════════════════════════════════════════════════════════════
# INTERNAL HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def _reset_window() -> None:
    """Reset all in-memory accumulators and advance the window start."""
    global _log_buffer, _throughput_counters, _error_counters, _window_start
    _log_buffer = []
    _throughput_counters = defaultdict(int)
    _error_counters = defaultdict(int)
    _window_start = datetime.now(timezone.utc)


def _handle_shutdown(signum, frame):
    """Signal handler for graceful shutdown (SIGTERM / SIGINT)."""
    global _shutdown_requested
    logger.info(
        "Shutdown signal received (signal=%d), flushing final window...", signum
    )
    _shutdown_requested = True


# ═══════════════════════════════════════════════════════════════════════════
# PUBLIC API — callable by consumers or tests
# ═══════════════════════════════════════════════════════════════════════════

def ingest(log_record: dict) -> None:
    """
    Accept one validated log record and update in-memory aggregation state.

    Parameters
    ----------
    log_record : dict
        Must conform to shared/schemas/log_schema.json.
        Required fields: timestamp, service, severity, message, trace_id.

    Side Effects
    ------------
    - Increments per-service throughput counter for the current window.
    - Increments per-service error counter if severity in ('ERROR','CRITICAL').
    - Appends the raw record to a write buffer for batch DB insert.

    Raises
    ------
    KeyError
        If log_record is missing required fields (should be caught upstream
        in consumer.py's process_message()).
    """
    service = log_record["service"]
    severity = log_record["severity"]

    # Buffer the raw record for batch insert.
    _log_buffer.append(log_record)

    # Update per-service throughput counter.
    _throughput_counters[service] += 1

    # Update per-service error counter.
    if severity in ERROR_SEVERITIES:
        _error_counters[service] += 1

    logger.debug(
        "ingest | service=%s severity=%s buffer_size=%d",
        service, severity, len(_log_buffer),
    )


def flush_window() -> None:
    """
    Write the current aggregation window to PostgreSQL and reset state.

    Writes to:
      - processed_logs       (batch insert of buffered raw records)
      - metrics_throughput   (one row per service)
      - metrics_error_rate   (one row per service)

    Called:
      - Automatically at every 1-minute window boundary by the main loop.
      - Manually in tests or on shutdown to ensure no data loss.

    Side Effects
    ------------
    - Commits a DB transaction via processing/db/connection.py.
    - Resets in-memory accumulators for the next window.
    - Logs window summary to stdout.
    """
    global _window_start

    window_end = datetime.now(timezone.utc)
    total_records = len(_log_buffer)

    # Skip DB write if the window is completely empty.
    if total_records == 0 and not _throughput_counters:
        logger.info("flush_window | empty window, skipping DB write")
        _reset_window()
        return

    conn = get_connection()
    try:
        with conn.cursor() as cur:

            # ----------------------------------------------------------
            # 1. Batch insert raw records into processed_logs
            # ----------------------------------------------------------
            if _log_buffer:
                args = [
                    (
                        rec["timestamp"],
                        rec["service"],
                        rec["severity"],
                        rec["message"],
                        rec["trace_id"],
                    )
                    for rec in _log_buffer
                ]

                cur.executemany(
                    """
                    INSERT INTO processed_logs
                        (log_ts, service, severity, message, trace_id)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    args,
                )
                logger.info(
                    "flush_window | inserted %d records into processed_logs",
                    len(args),
                )

            # ----------------------------------------------------------
            # 2. Insert throughput metrics (one row per service)
            #    The DB auto-computes messages_per_sec via GENERATED column.
            # ----------------------------------------------------------
            for service, count in _throughput_counters.items():
                cur.execute(
                    """
                    INSERT INTO metrics_throughput
                        (window_start, window_end, service, message_count)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (_window_start, window_end, service, count),
                )

            # ----------------------------------------------------------
            # 3. Insert error rate metrics (one row per service)
            #    The DB auto-computes error_rate_pct via GENERATED column.
            # ----------------------------------------------------------
            for service, total_count in _throughput_counters.items():
                error_count = _error_counters.get(service, 0)
                cur.execute(
                    """
                    INSERT INTO metrics_error_rate
                        (window_start, window_end, service,
                         total_messages, error_messages)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (_window_start, window_end, service, total_count, error_count),
                )

        conn.commit()
        logger.info(
            "flush_window | window [%s → %s] | %d records | %d services",
            _window_start.strftime("%H:%M:%S"),
            window_end.strftime("%H:%M:%S"),
            total_records,
            len(_throughput_counters),
        )

    except Exception:
        conn.rollback()
        logger.exception("flush_window | DB write failed, rolling back")
        raise
    finally:
        conn.close()

    _reset_window()


def update_consumer_lag(partition_lags: dict[int, int],
                        consumer_id: str | None = None) -> None:
    """
    Persist a consumer lag snapshot to PostgreSQL.

    Parameters
    ----------
    partition_lags : dict[int, int]
        Mapping of partition_id → lag (number of unconsumed messages).
        Provided by consumers/consumer.py by polling Kafka AdminClient.
    consumer_id : str, optional
        Identifier of the consumer instance reporting lag.

    Writes to:
      metrics_consumer_lag table (one row per partition per call).
    """
    if not partition_lags:
        return

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for partition_id, lag in partition_lags.items():
                cur.execute(
                    """
                    INSERT INTO metrics_consumer_lag
                        (partition_id, lag, consumer_id)
                    VALUES (%s, %s, %s)
                    """,
                    (partition_id, lag, consumer_id),
                )
        conn.commit()
        logger.info(
            "update_consumer_lag | %d partitions | lags=%s",
            len(partition_lags),
            dict(partition_lags),
        )
    except Exception:
        conn.rollback()
        logger.exception("update_consumer_lag | DB write failed")
        raise
    finally:
        conn.close()


def ingest_dlq_event(dlq_envelope: dict) -> None:
    """
    Persist a DLQ event to the dlq_log table for dashboard inspection.

    Parameters
    ----------
    dlq_envelope : dict
        Must conform to shared/schemas/dlq_schema.json.
        Required fields: original_message, failure_reason, retry_count, failed_at.

    Called by:
      consumers/dlq_handler.py after publishing to the logs-dlq Kafka topic,
      OR by a separate consumer that reads the logs-dlq topic.
    """
    conn = get_connection()
    try:
        # original_message can be a dict or string per the DLQ schema;
        # we always store it as a JSON string in the DB.
        original_msg = dlq_envelope["original_message"]
        if isinstance(original_msg, dict):
            original_msg = json.dumps(original_msg)

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dlq_log
                    (failed_at, original_message, failure_reason, retry_count)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    dlq_envelope["failed_at"],
                    original_msg,
                    dlq_envelope["failure_reason"],
                    dlq_envelope["retry_count"],
                ),
            )
        conn.commit()
        logger.info(
            "ingest_dlq_event | reason=%s | retries=%d",
            dlq_envelope["failure_reason"][:60],
            dlq_envelope["retry_count"],
        )
    except Exception:
        conn.rollback()
        logger.exception("ingest_dlq_event | DB write failed")
        raise
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# KAFKA LAG POLLING
# ═══════════════════════════════════════════════════════════════════════════

def _poll_consumer_lag(admin_client: AdminClient) -> None:
    """
    Query Kafka AdminClient for consumer group lag and persist to DB.

    Fetches the committed offsets for the main consumer group
    (KAFKA_CONSUMER_GROUP, i.e., Person 2's consumer group) and compares
    them against the topic's high-water marks to compute per-partition lag.
    """
    try:
        # Get committed offsets for Person 2's consumer group.
        request = ListConsumerGroupOffsetsRequest(KAFKA_CONSUMER_GROUP)
        future_map = admin_client.list_consumer_group_offsets([request])

        committed = {}
        for group_id, future in future_map.items():
            response = future.result()
            for tp in response.topic_partitions:
                if tp.topic == KAFKA_TOPIC_LOGS and tp.offset >= 0:
                    committed[tp.partition] = tp.offset

        if not committed:
            logger.debug("_poll_consumer_lag | no committed offsets found")
            return

        # Get high-water marks (end offsets) for each partition using
        # a temporary consumer to query watermark offsets.
        temp_conf = {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": "_lag_checker_temp",
        }
        temp_consumer = Consumer(temp_conf)
        partition_lags = {}
        try:
            for partition_id, committed_offset in committed.items():
                tp = TopicPartition(KAFKA_TOPIC_LOGS, partition_id)
                low, high = temp_consumer.get_watermark_offsets(tp, timeout=5.0)
                lag = max(0, high - committed_offset)
                partition_lags[partition_id] = lag
        finally:
            temp_consumer.close()

        if partition_lags:
            update_consumer_lag(partition_lags)

    except Exception:
        logger.exception("_poll_consumer_lag | failed to fetch lag")


# ═══════════════════════════════════════════════════════════════════════════
# MAIN AGGREGATION LOOP
# ═══════════════════════════════════════════════════════════════════════════

def run_aggregation_loop() -> None:
    """
    Main entry point for the aggregator as a long-running process.

    Loop:
      1. Connect to Kafka as an independent consumer group (logflow-aggregator)
      2. Subscribe to both 'logs' and 'logs-dlq' topics
      3. For each log message:  call ingest(log_record)
      4. For each DLQ message:  call ingest_dlq_event(dlq_envelope)
      5. Every WINDOW_SECONDS (default 60s):  call flush_window()
      6. Every LAG_POLL_SECONDS (default 30s): poll Kafka AdminClient → lag
      7. On shutdown (SIGTERM/SIGINT): flush final window and exit
    """
    global _shutdown_requested

    # Register signal handlers for graceful shutdown.
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    logger.info("=" * 60)
    logger.info("LogFlow Aggregator starting")
    logger.info("  Kafka broker   : %s", KAFKA_BROKER)
    logger.info("  Log topic      : %s", KAFKA_TOPIC_LOGS)
    logger.info("  DLQ topic      : %s", KAFKA_TOPIC_DLQ)
    logger.info("  Window         : %d seconds", WINDOW_SECONDS)
    logger.info("  Lag poll       : %d seconds", LAG_POLL_SECONDS)
    logger.info("=" * 60)

    # --- Kafka consumer setup (aggregator's own consumer group) ---
    consumer_conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": AGGREGATOR_GROUP,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC_LOGS, KAFKA_TOPIC_DLQ])

    # --- Kafka AdminClient for consumer lag polling ---
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKER})

    last_flush_time = time.monotonic()
    last_lag_poll_time = time.monotonic()
    messages_in_window = 0

    logger.info("Subscribed to topics. Entering main loop...")

    try:
        while not _shutdown_requested:
            now = time.monotonic()

            # ----------------------------------------------------------
            # Check if it's time to flush the aggregation window (60s)
            # ----------------------------------------------------------
            if now - last_flush_time >= WINDOW_SECONDS:
                flush_window()
                logger.info(
                    "Window flushed | %d messages processed in this window",
                    messages_in_window,
                )
                messages_in_window = 0
                last_flush_time = now

            # ----------------------------------------------------------
            # Check if it's time to poll consumer lag (30s)
            # ----------------------------------------------------------
            if now - last_lag_poll_time >= LAG_POLL_SECONDS:
                _poll_consumer_lag(admin_client)
                last_lag_poll_time = now

            # ----------------------------------------------------------
            # Poll Kafka for new messages (1-second timeout)
            # ----------------------------------------------------------
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                logger.error("Kafka consumer error: %s", msg.error())
                continue

            # Parse the message value from raw bytes → dict.
            try:
                value = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.warning("Failed to decode message: %s", e)
                continue

            topic = msg.topic()

            if topic == KAFKA_TOPIC_LOGS:
                # --- Process a valid log record ---
                try:
                    ingest(value)
                    messages_in_window += 1
                except KeyError as e:
                    logger.warning("Log record missing required field %s, skipping", e)

            elif topic == KAFKA_TOPIC_DLQ:
                # --- Process a DLQ event ---
                try:
                    ingest_dlq_event(value)
                except KeyError as e:
                    logger.warning("DLQ envelope missing required field %s, skipping", e)

    except KafkaException as e:
        logger.exception("Fatal Kafka error: %s", e)
    finally:
        # Flush any remaining buffered data before shutdown.
        logger.info("Flushing final window before shutdown...")
        try:
            flush_window()
        except Exception:
            logger.exception("Final flush failed")

        consumer.close()
        logger.info("Aggregator shut down cleanly.")


if __name__ == "__main__":
    run_aggregation_loop()
