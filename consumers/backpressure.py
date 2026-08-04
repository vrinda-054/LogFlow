"""
consumers/backpressure.py — Person 2 (Consumer Layer)
=======================================================

Role
----
Monitors consumer-side buffer depth and Kafka consumer lag. Exposes a
simple API that consumer.py calls before processing each batch to decide
whether to pause or resume partition consumption.

This module implements requirements REQ-17 through REQ-20:
  REQ-17: The system MUST detect when the consumer-to-aggregator internal
          buffer exceeds a configurable high-water mark.
  REQ-18: On high-water detection, the system MUST pause the relevant
          Kafka partition(s) to stop new messages from being fetched.
  REQ-19: The system MUST resume paused partitions once the buffer depth
          falls below the configurable low-water mark.
  REQ-20: Pause/resume decisions MUST be logged with partition ID,
          current lag, and buffer depth for observability.

Interface (called by consumer.py)
-----------------------------------
  check_backpressure(partition_id: int) → BackpressureState
    Returns NORMAL, HIGH_WATER, or LOW_WATER based on current metrics.

  pause_partition(consumer, partition_id: int) → None
    Instructs the confluent-kafka consumer to stop fetching from partition.

  resume_partition(consumer, partition_id: int) → None
    Instructs the consumer to resume fetching from a paused partition.

Input Sources
-------------
  - Internal buffer depth: shared queue/counter maintained by consumer.py
  - Kafka consumer lag: polled from consumer.committed() / consumer.position()
    or via confluent-kafka AdminClient metrics.

Output / Side Effects
---------------------
  - Calls consumer.pause([TopicPartition(...)]) / consumer.resume(...)
  - Emits log lines for REQ-20 observability
  - No Kafka messages produced; no DB writes

Upstream caller  : consumers/consumer.py
Downstream effect: slows/stops message flow to processing/aggregator.py
"""

import os
from enum import Enum

# ---------------------------------------------------------------------------
# Configuration constants (override via env or rebalance_config.py settings)
# ---------------------------------------------------------------------------
BUFFER_HIGH_WATER = int(os.environ.get("BACKPRESSURE_HIGH_WATER", "1000"))
BUFFER_LOW_WATER  = int(os.environ.get("BACKPRESSURE_LOW_WATER", "200"))
LAG_HIGH_WATER    = int(os.environ.get("BACKPRESSURE_LAG_HIGH",  "5000"))


class BackpressureState(Enum):
    """Possible outcomes of a backpressure check."""
    NORMAL     = "normal"       # Processing may continue normally
    HIGH_WATER = "high_water"   # Buffer/lag too high → pause partition
    LOW_WATER  = "low_water"    # Buffer draining → safe to resume


# ---------------------------------------------------------------------------
# Module-level state (keyed by partition_id)
# ---------------------------------------------------------------------------
_paused_partitions: set[int] = set()
_buffer_depth: dict[int, int] = {}   # partition_id → current buffer size


def update_buffer_depth(partition_id: int, depth: int) -> None:
    """
    Called by consumer.py to report the current internal buffer depth
    for a partition after enqueueing a batch for the aggregator.

    Parameters
    ----------
    partition_id : int
    depth        : int   Number of unprocessed messages in the internal buffer.
    """
    _buffer_depth[partition_id] = depth


def check_backpressure(partition_id: int) -> BackpressureState:
    """
    Evaluate whether a partition should be paused, resumed, or left running.

    Parameters
    ----------
    partition_id : int
        The Kafka partition ID to evaluate.

    Returns
    -------
    BackpressureState
        - HIGH_WATER : caller should call pause_partition()
        - LOW_WATER  : caller should call resume_partition() if currently paused
        - NORMAL     : no action needed

    Notes
    -----
    TODO (Person 2): also incorporate Kafka consumer lag by querying
    consumer.position() vs consumer.committed() offsets.
    """
    depth = _buffer_depth.get(partition_id, 0)

    if depth >= BUFFER_HIGH_WATER:
        print(f"[backpressure] REQ-17: HIGH_WATER on partition={partition_id} "
              f"buffer_depth={depth} >= {BUFFER_HIGH_WATER}")
        return BackpressureState.HIGH_WATER

    if partition_id in _paused_partitions and depth <= BUFFER_LOW_WATER:
        print(f"[backpressure] REQ-19: LOW_WATER on partition={partition_id} "
              f"buffer_depth={depth} <= {BUFFER_LOW_WATER}")
        return BackpressureState.LOW_WATER

    return BackpressureState.NORMAL


def pause_partition(consumer, partition_id: int) -> None:
    """
    Pause Kafka consumption on the given partition (REQ-18).

    Parameters
    ----------
    consumer     : confluent_kafka.Consumer
        The active consumer instance.
    partition_id : int
        The partition to pause.

    Side Effects
    ------------
    - Calls consumer.pause([TopicPartition(topic, partition_id)])
    - Adds partition_id to _paused_partitions
    - Logs the pause event (REQ-20)
    """
    topic = os.environ.get("KAFKA_TOPIC_LOGS", "logs")
    print(f"[backpressure] REQ-18/REQ-20: PAUSING partition={partition_id} "
          f"topic={topic} buffer={_buffer_depth.get(partition_id, 'unknown')}")
    _paused_partitions.add(partition_id)
    # TODO: from confluent_kafka import TopicPartition
    # TODO: consumer.pause([TopicPartition(topic, partition_id)])


def resume_partition(consumer, partition_id: int) -> None:
    """
    Resume Kafka consumption on a previously paused partition (REQ-19).

    Parameters
    ----------
    consumer     : confluent_kafka.Consumer
    partition_id : int

    Side Effects
    ------------
    - Calls consumer.resume([TopicPartition(topic, partition_id)])
    - Removes partition_id from _paused_partitions
    - Logs the resume event (REQ-20)
    """
    topic = os.environ.get("KAFKA_TOPIC_LOGS", "logs")
    print(f"[backpressure] REQ-19/REQ-20: RESUMING partition={partition_id} "
          f"topic={topic} buffer={_buffer_depth.get(partition_id, 'unknown')}")
    _paused_partitions.discard(partition_id)
    # TODO: from confluent_kafka import TopicPartition
    # TODO: consumer.resume([TopicPartition(topic, partition_id)])


def get_paused_partitions() -> set[int]:
    """Return the set of currently paused partition IDs (for observability/testing)."""
    return frozenset(_paused_partitions)
