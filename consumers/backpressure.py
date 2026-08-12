"""
consumers/backpressure.py — Person 2 (Consumer Layer)
=======================================================

Monitors consumer-side buffer depth and Kafka consumer lag.
Implements REQ-17 through REQ-20 (pause / resume partitions).

REQ-17: Detect when buffer exceeds HIGH_WATER mark.
REQ-18: Pause the Kafka partition when HIGH_WATER is crossed.
REQ-19: Resume the partition when lag drops below LOW_WATER.
REQ-20: Log every pause/resume with partition ID, lag, and buffer depth.
"""

import logging
import os
import threading
from enum import Enum

from confluent_kafka import TopicPartition
from confluent_kafka.admin import AdminClient

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Thresholds — override via .env
# ---------------------------------------------------------------------------
BUFFER_HIGH_WATER = int(os.environ.get("BACKPRESSURE_HIGH_WATER", "1000"))
BUFFER_LOW_WATER  = int(os.environ.get("BACKPRESSURE_LOW_WATER",  "200"))
LAG_HIGH_WATER    = int(os.environ.get("BACKPRESSURE_LAG_HIGH",   "5000"))

_TOPIC = os.environ.get("KAFKA_TOPIC_LOGS", "logs")


class BackpressureState(Enum):
    """Result of a backpressure evaluation for one partition."""
    NORMAL     = "normal"      # continue processing normally
    HIGH_WATER = "high_water"  # buffer/lag too high → caller should pause
    LOW_WATER  = "low_water"   # draining → caller should resume if paused


# ---------------------------------------------------------------------------
# Thread-safe module state
# ---------------------------------------------------------------------------
_lock              = threading.Lock()
_paused_partitions: set[int]       = set()
_buffer_depth:      dict[int, int] = {}   # partition_id → queued message count
_last_lag:          dict[int, int] = {}   # partition_id → last polled lag


# ---------------------------------------------------------------------------
# AdminClient for lag polling
# ---------------------------------------------------------------------------
_admin: AdminClient | None = None


def _get_admin() -> AdminClient:
    """Return a lazily-created AdminClient singleton for lag polling."""
    global _admin
    if _admin is None:
        broker = os.environ.get("KAFKA_BROKER", "localhost:9092")
        _admin = AdminClient({"bootstrap.servers": broker})
        logger.debug("[backpressure] AdminClient initialised → broker=%s", broker)
    return _admin


# ---------------------------------------------------------------------------
# Public helpers called by consumer.py
# ---------------------------------------------------------------------------

def update_buffer_depth(partition_id: int, depth: int) -> None:
    """
    Record the current internal buffer depth for a partition.
    Called by consumer.py each time it enqueues a validated message
    for the aggregator.

    Parameters
    ----------
    partition_id : int
    depth        : int   Number of messages waiting in the aggregator queue.
    """
    with _lock:
        _buffer_depth[partition_id] = depth


def poll_lag(consumer, assigned_tps: list) -> dict[int, int]:
    """
    Calculate consumer lag for every assigned TopicPartition.

    Strategy: compare committed offset vs. high-watermark offset
    for each partition. Updates the _last_lag cache.

    Parameters
    ----------
    consumer     : confluent_kafka.Consumer
    assigned_tps : list[TopicPartition]   Currently assigned partitions.

    Returns
    -------
    dict[int, int]
        Mapping partition_id → lag (unconsumed messages).
    """
    lags: dict[int, int] = {}
    for tp in assigned_tps:
        try:
            # high-watermark = next offset that will be written
            low, high = consumer.get_watermark_offsets(tp, timeout=1.0, cached=False)
            committed_list = consumer.committed([tp], timeout=2.0)
            committed_offset = committed_list[0].offset if committed_list else -1001

            if committed_offset < 0:
                # OFFSET_INVALID / no committed offset yet — entire topic is lag
                lag = high - low
            else:
                lag = max(0, high - committed_offset)

            lags[tp.partition] = lag

        except Exception as exc:           # noqa: BLE001
            logger.warning(
                "[backpressure] Could not poll lag for partition=%d: %s",
                tp.partition, exc
            )
            lags[tp.partition] = 0

    with _lock:
        _last_lag.update(lags)

    return lags


def check_backpressure(partition_id: int) -> BackpressureState:
    """
    Evaluate the backpressure state for one partition.

    Decision logic (in priority order):
      1. If buffer depth ≥ BUFFER_HIGH_WATER → HIGH_WATER
      2. If cached lag  ≥ LAG_HIGH_WATER    → HIGH_WATER
      3. If partition is currently paused AND
         buffer depth ≤ BUFFER_LOW_WATER    → LOW_WATER (safe to resume)
      4. Otherwise                          → NORMAL

    Parameters
    ----------
    partition_id : int

    Returns
    -------
    BackpressureState
    """
    with _lock:
        depth = _buffer_depth.get(partition_id, 0)
        lag   = _last_lag.get(partition_id, 0)
        paused = partition_id in _paused_partitions

    # --- HIGH_WATER checks ---
    if depth >= BUFFER_HIGH_WATER:
        logger.warning(
            "[backpressure] REQ-17: HIGH_WATER (buffer) partition=%d "
            "buffer_depth=%d >= %d",
            partition_id, depth, BUFFER_HIGH_WATER,
        )
        return BackpressureState.HIGH_WATER

    if lag >= LAG_HIGH_WATER:
        logger.warning(
            "[backpressure] REQ-17: HIGH_WATER (lag) partition=%d "
            "lag=%d >= %d",
            partition_id, lag, LAG_HIGH_WATER,
        )
        return BackpressureState.HIGH_WATER

    # --- LOW_WATER check (only meaningful if paused) ---
    if paused and depth <= BUFFER_LOW_WATER:
        logger.info(
            "[backpressure] REQ-19: LOW_WATER partition=%d "
            "buffer_depth=%d <= %d — safe to resume",
            partition_id, depth, BUFFER_LOW_WATER,
        )
        return BackpressureState.LOW_WATER

    return BackpressureState.NORMAL


def pause_partition(consumer, partition_id: int) -> None:
    """
    Instruct the consumer to stop fetching from this partition (REQ-18).

    Calling consumer.pause() causes poll() to return immediately with
    no messages from the paused partition — it does NOT disconnect from
    the broker or lose the partition assignment.

    Parameters
    ----------
    consumer     : confluent_kafka.Consumer
    partition_id : int
    """
    with _lock:
        if partition_id in _paused_partitions:
            return   # already paused — no-op
        _paused_partitions.add(partition_id)

    tp = TopicPartition(_TOPIC, partition_id)
    consumer.pause([tp])

    logger.warning(
        "[backpressure] REQ-18 | REQ-20: PAUSED partition=%d "
        "buffer=%d lag=%d topic=%s",
        partition_id,
        _buffer_depth.get(partition_id, 0),
        _last_lag.get(partition_id, 0),
        _TOPIC,
    )


def resume_partition(consumer, partition_id: int) -> None:
    """
    Resume fetching from a previously paused partition (REQ-19).

    Parameters
    ----------
    consumer     : confluent_kafka.Consumer
    partition_id : int
    """
    with _lock:
        if partition_id not in _paused_partitions:
            return   # not paused — no-op
        _paused_partitions.discard(partition_id)

    tp = TopicPartition(_TOPIC, partition_id)
    consumer.resume([tp])

    logger.info(
        "[backpressure] REQ-19 | REQ-20: RESUMED partition=%d "
        "buffer=%d lag=%d topic=%s",
        partition_id,
        _buffer_depth.get(partition_id, 0),
        _last_lag.get(partition_id, 0),
        _TOPIC,
    )


def get_paused_partitions() -> frozenset[int]:
    """Return a snapshot of currently paused partition IDs (thread-safe)."""
    with _lock:
        return frozenset(_paused_partitions)


def get_lag_snapshot() -> dict[int, int]:
    """Return the last polled lag values per partition (thread-safe)."""
    with _lock:
        return dict(_last_lag)
