"""
consumers/rebalance_config.py — Person 2 (Consumer Layer)
===========================================================

Role
----
Centralises Kafka consumer group rebalance settings. Provides a factory
function that returns a fully-configured consumer config dict ready for
confluent_kafka.Consumer(). All consumer instances in the group MUST use
these settings to ensure consistent rebalance behaviour.

Why Cooperative-Sticky Rebalance?
----------------------------------
The default eager rebalance protocol revokes ALL partitions from ALL
consumers at the start of every rebalance, causing processing gaps.
The cooperative-sticky protocol minimises disruption:
  - Only the partitions that need to move are revoked.
  - Consumers retain their other partitions and keep processing.
  - Stickiness ensures consumers tend to re-acquire the same partitions
    across rebalances, which helps locality (e.g. in-memory aggregator state).

Upstream caller : consumers/consumer.py (imports get_consumer_config())
No downstream contract — this is purely configuration.

Input
-----
  Environment variables:
    KAFKA_BROKER           : Kafka bootstrap server (required)
    KAFKA_CONSUMER_GROUP   : Consumer group ID (default: logflow-group)
    KAFKA_TOPIC_LOGS       : Topic to subscribe to (default: logs)

Output
------
  Returns a dict suitable for confluent_kafka.Consumer(config).
"""

import os


def get_consumer_config(
    extra_config: dict | None = None,
) -> dict:
    """
    Build and return the confluent-kafka Consumer configuration dict.

    Parameters
    ----------
    extra_config : dict | None
        Optional overrides to merge into the base configuration.
        Useful for test fixtures or per-instance tuning.

    Returns
    -------
    dict
        Full consumer configuration ready for confluent_kafka.Consumer().

    Raises
    ------
    EnvironmentError
        If KAFKA_BROKER is not set.
    """
    broker = os.environ.get("KAFKA_BROKER")
    if not broker:
        raise EnvironmentError("KAFKA_BROKER not set. See .env.example.")

    config = {
        # ---------------------------------------------------------------
        # Connection
        # ---------------------------------------------------------------
        "bootstrap.servers": broker,
        "group.id":          os.environ.get("KAFKA_CONSUMER_GROUP", "logflow-group"),

        # ---------------------------------------------------------------
        # Offset behaviour
        # ---------------------------------------------------------------
        "auto.offset.reset":  "earliest",   # consume from beginning if no committed offset
        "enable.auto.commit": False,         # manual commit for at-least-once guarantees

        # ---------------------------------------------------------------
        # Cooperative-sticky rebalance (REQ-related: minimise processing gaps)
        # ---------------------------------------------------------------
        "partition.assignment.strategy": "cooperative-sticky",

        # ---------------------------------------------------------------
        # Session & heartbeat tuning
        # ---------------------------------------------------------------
        "session.timeout.ms":      30_000,  # 30s — declare consumer dead after this
        "heartbeat.interval.ms":    9_000,  # 9s  — must be < session.timeout / 3
        "max.poll.interval.ms":   300_000,  # 5min — max time between poll() calls
                                             # (increase if processing is slow)

        # ---------------------------------------------------------------
        # Fetch tuning
        # ---------------------------------------------------------------
        "fetch.min.bytes":           1,
        "fetch.max.wait.ms":       500,
        "max.partition.fetch.bytes": 1_048_576,  # 1 MiB per partition per fetch

        # ---------------------------------------------------------------
        # Error handling
        # ---------------------------------------------------------------
        "error_cb": _on_error,
    }

    if extra_config:
        config.update(extra_config)

    return config


def _on_error(error) -> None:
    """
    Global error callback for the consumer.
    Logs Kafka client-level errors (not message-level errors).

    Parameters
    ----------
    error : confluent_kafka.KafkaError
    """
    print(f"[rebalance_config] Kafka client error: {error}")
    # TODO: integrate with a monitoring/alerting system


def get_topic() -> str:
    """Return the configured log topic name from environment."""
    return os.environ.get("KAFKA_TOPIC_LOGS", "logs")
