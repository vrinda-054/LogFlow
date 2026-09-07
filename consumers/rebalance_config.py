"""Consumer-group configuration and cooperative rebalance callbacks."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Literal

logger = logging.getLogger(__name__)
RebalanceEventName = Literal["partition_assigned", "group_stable", "partition_revoked", "reassignment"]


@dataclass(frozen=True)
class RebalanceEvent:
    """Stable event contract for assignment and reassignment observability."""

    timestamp: str
    consumer_id: str
    event: RebalanceEventName
    partition: int | None
    topic: str | None


def _emit(
    consumer: Any, event_name: RebalanceEventName, partition: Any = None, consumer_id: str | None = None
) -> RebalanceEvent:
    event = RebalanceEvent(
        datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        consumer_id or str(getattr(consumer, "consumer_id", os.environ.get("CONSUMER_ID", "unknown"))),
        event_name,
        getattr(partition, "partition", None),
        getattr(partition, "topic", None),
    )
    logger.info("event=%s", json.dumps(asdict(event), sort_keys=True))
    return event


def on_assign(consumer: Any, partitions: list[Any], consumer_id: str | None = None) -> None:
    """Emit one assignment event per partition, then a group-stable event."""
    for partition in partitions:
        _emit(consumer, "partition_assigned", partition, consumer_id)
    _emit(consumer, "group_stable", consumer_id=consumer_id)


def on_revoke(consumer: Any, partitions: list[Any], consumer_id: str | None = None) -> None:
    """Commit safely owned offsets and emit revoke/reassignment events."""
    if partitions:
        try:
            consumer.commit(offsets=partitions, asynchronous=False)
        except Exception as exc:
            logger.warning("offset commit during revoke failed: %s", exc)
    for partition in partitions:
        _emit(consumer, "partition_revoked", partition, consumer_id)
        _emit(consumer, "reassignment", partition, consumer_id)


def _on_error(error: Any) -> None:
    """Log a Kafka client-level error without interrupting polling."""
    logger.error("Kafka client error: %s", error)


def get_consumer_config(extra_config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build the common manual-commit, cooperative-sticky Consumer config."""
    broker = os.environ.get("KAFKA_BROKER")
    if not broker:
        raise EnvironmentError("KAFKA_BROKER not set. See .env.example.")
    config: dict[str, Any] = {
        "bootstrap.servers": broker,
        "group.id": os.environ.get("KAFKA_CONSUMER_GROUP", "logflow-group"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "partition.assignment.strategy": "cooperative-sticky",
        "session.timeout.ms": 30_000,
        "heartbeat.interval.ms": 9_000,
        "max.poll.interval.ms": int(os.environ.get("KAFKA_MAX_POLL_INTERVAL_MS", "300000")),
        "error_cb": _on_error,
    }
    if extra_config:
        config.update(extra_config)
    return config


def get_topic() -> str:
    """Return the raw-log topic name shared with the producer."""
    return os.environ.get("KAFKA_TOPIC_LOGS", "logs-raw")