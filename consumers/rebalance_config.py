"""
consumers/rebalance_config.py — Person 2 (Consumer Layer)
===========================================================

Centralises Kafka consumer group rebalance settings.
All consumer instances MUST use get_consumer_config() so every instance
in the group negotiates with the same protocol parameters.
"""

import logging
import os

logger = logging.getLogger(__name__)


def get_consumer_config(extra_config: dict | None = None) -> dict:
    """
    Build and return the confluent-kafka Consumer configuration dict.

    Parameters
    ----------
    extra_config : dict | None
        Optional overrides merged on top of the base config.
        Useful for test fixtures (e.g. inject a mock broker address).

    Returns
    -------
    dict
        Full consumer configuration for confluent_kafka.Consumer().

    Raises
    ------
    EnvironmentError
        If KAFKA_BROKER is not set in the environment.
    """
    broker = os.environ.get("KAFKA_BROKER")
    if not broker:
        raise EnvironmentError(
            "KAFKA_BROKER is not set. Copy .env.example → .env and set the value."
        )

    config = {
        # -------------------------------------------------------------------
        # Connection
        # -------------------------------------------------------------------
        "bootstrap.servers": broker,
        "group.id":          os.environ.get("KAFKA_CONSUMER_GROUP", "logflow-group"),

        # -------------------------------------------------------------------
        # Offset strategy — at-least-once delivery
        # Manual commit lets us commit ONLY after successful processing or DLQ.
        # -------------------------------------------------------------------
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,

        # -------------------------------------------------------------------
        # Cooperative-sticky rebalance
        # Only the partitions that NEED to move are revoked during a rebalance.
        # Other partitions keep running — this prevents processing gaps on
        # pod restart or consumer group expansion.
        # -------------------------------------------------------------------
        "partition.assignment.strategy": "cooperative-sticky",

        # -------------------------------------------------------------------
        # Session & heartbeat
        # heartbeat.interval.ms must be < session.timeout.ms / 3
        # max.poll.interval.ms must be > the longest possible processing time
        # -------------------------------------------------------------------
        "session.timeout.ms":    30_000,   # 30s  — declare dead after this
        "heartbeat.interval.ms":  9_000,   # 9s   — heartbeat frequency
        "max.poll.interval.ms": 300_000,   # 5min — max between poll() calls

        # -------------------------------------------------------------------
        # Fetch tuning
        # -------------------------------------------------------------------
        "fetch.min.bytes":            1,
        "fetch.max.wait.ms":        500,
        "max.partition.fetch.bytes": 1_048_576,  # 1 MiB per partition per fetch

        # -------------------------------------------------------------------
        # Callbacks
        # -------------------------------------------------------------------
        "error_cb":  _on_error,
        "stats_cb":  _on_stats,

        # Emit broker/topic stats every 30 seconds for lag observability
        "statistics.interval.ms": 30_000,
    }

    if extra_config:
        config.update(extra_config)

    return config


def _on_error(error) -> None:
    """
    Called by librdkafka on client-level errors (NOT per-message errors).
    These are typically connectivity or broker issues.
    """
    logger.error("[rebalance_config] Kafka client error: code=%s msg=%s",
                 error.code(), error.str())


def _on_stats(stats_json: str) -> None:
    """
    Called every statistics.interval.ms with a JSON stats blob from librdkafka.
    Useful for external monitoring integrations (Prometheus, Datadog, etc.).
    Currently just debug-logs the raw JSON; replace with metric emission.
    """
    logger.debug("[rebalance_config] librdkafka stats: %s", stats_json[:200])


def get_topic() -> str:
    """Return the configured main log topic name."""
    return os.environ.get("KAFKA_TOPIC_LOGS", "logs")
