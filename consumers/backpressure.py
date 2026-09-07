"""Lag-based Kafka backpressure decisions and structured pause/resume events."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Literal

try:
    from confluent_kafka import TopicPartition
except ImportError:  # pragma: no cover
    TopicPartition = None

logger = logging.getLogger(__name__)
BackpressureAction = Literal["PAUSE", "RESUME", "OK"]


@dataclass(frozen=True)
class BackpressureEvent:
    """Stable event contract for future metrics/event-feed integration."""

    timestamp: str
    consumer_id: str
    partition: int
    action: Literal["PAUSE", "RESUME"]
    current_lag: int
    threshold: int


def check_backpressure(current_lag: int, low_water: int, high_water: int) -> BackpressureAction:
    """Return ``PAUSE``, ``RESUME`` or ``OK`` from current Kafka lag."""
    if high_water < low_water:
        raise ValueError("high_water must be greater than or equal to low_water")
    if current_lag >= high_water:
        return "PAUSE"
    if current_lag <= low_water:
        return "RESUME"
    return "OK"


def _emit_event(event: BackpressureEvent) -> BackpressureEvent:
    logger.info("event=%s", json.dumps(asdict(event), sort_keys=True))
    return event


def _topic_partition(topic_partition: Any) -> Any:
    if TopicPartition is None or hasattr(topic_partition, "topic"):
        return topic_partition
    return TopicPartition(os.environ.get("KAFKA_TOPIC_LOGS", "logs-raw"), int(topic_partition))


def pause_partition(
    consumer: Any, topic_partition: Any, current_lag: int = 0, threshold: int | None = None
) -> BackpressureEvent:
    """Pause one Kafka partition and emit a typed pause event."""
    partition = int(topic_partition.partition if hasattr(topic_partition, "partition") else topic_partition)
    consumer.pause([_topic_partition(topic_partition)])
    return _emit_event(BackpressureEvent(
        datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        str(getattr(consumer, "consumer_id", "unknown")),
        partition,
        "PAUSE",
        int(current_lag),
        int(threshold if threshold is not None else os.environ.get("BACKPRESSURE_HIGH_WATER", "5000")),
    ))


def resume_partition(
    consumer: Any, topic_partition: Any, current_lag: int = 0, threshold: int | None = None
) -> BackpressureEvent:
    """Resume one Kafka partition and emit a typed resume event."""
    partition = int(topic_partition.partition if hasattr(topic_partition, "partition") else topic_partition)
    consumer.resume([_topic_partition(topic_partition)])
    return _emit_event(BackpressureEvent(
        datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        str(getattr(consumer, "consumer_id", "unknown")),
        partition,
        "RESUME",
        int(current_lag),
        int(threshold if threshold is not None else os.environ.get("BACKPRESSURE_LOW_WATER", "200")),
    ))