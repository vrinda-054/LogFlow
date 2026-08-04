"""
ingestion/producer.py — Person 1 (Ingestion Layer)
====================================================

Role
----
Entry point for the LogFlow ingestion layer. Generates synthetic log messages
and publishes them to the Kafka topic `logs` (4 partitions) using the
confluent-kafka producer client.

This module is the **upstream boundary** of the pipeline:
  - It PRODUCES messages conforming to shared/schemas/log_schema.json
  - The Kafka topic / broker config comes from environment variables (see .env.example)
  - Downstream consumers (consumers/consumer.py) depend on the schema contract

CLI Interface
-------------
Run via:  python producer.py [OPTIONS]

Options
-------
  --rate INT          Messages per second to produce (default: 10)
  --duration INT      Total run time in seconds; 0 = run forever (default: 0)
  --malformed-pct INT Percentage of messages [0–100] that should be intentionally
                      malformed (missing fields / bad types) to exercise DLQ handling
                      in consumers/dlq_handler.py (default: 0)
  --scenario STR      Load profile preset. One of:
                        normal    — steady rate, all valid messages
                        spike     — normal rate with periodic 10× burst for 5 s
                        malformed — 50% malformed messages regardless of --malformed-pct

Input
-----
  - Environment variables: KAFKA_BROKER, KAFKA_TOPIC_LOGS (see .env.example)
  - log_templates.py: provides generate_log(service) → dict

Output
------
  - Kafka topic `logs` (env: KAFKA_TOPIC_LOGS): JSON-serialised LogMessage objects
    conforming to shared/schemas/log_schema.json
  - stdout: per-message confirmation (rate-limited) + summary stats on exit

Dependencies
------------
  See requirements.txt
  Key: confluent-kafka, faker (used inside log_templates.py)

Interface Contract
------------------
  OUTPUT → consumers/consumer.py:
    Every message on the `logs` topic must be a UTF-8 encoded JSON string
    matching shared/schemas/log_schema.json exactly.
    Partition key = service name (ensures same-service messages stay ordered).
"""

import argparse
import os
import json
import time

# ---------------------------------------------------------------------------
# TODO (Person 1): implement the following when building the ingestion layer
# ---------------------------------------------------------------------------
# from confluent_kafka import Producer
# from log_templates import generate_log, SERVICES


def parse_args() -> argparse.Namespace:
    """Parse and return CLI arguments."""
    parser = argparse.ArgumentParser(
        description="LogFlow synthetic log producer — publishes to Kafka topic 'logs'."
    )
    parser.add_argument("--rate", type=int, default=10,
                        help="Messages per second (default: 10)")
    parser.add_argument("--duration", type=int, default=0,
                        help="Run duration in seconds; 0 = run forever (default: 0)")
    parser.add_argument("--malformed-pct", type=int, default=0,
                        choices=range(0, 101), metavar="[0-100]",
                        help="Percentage of intentionally malformed messages (default: 0)")
    parser.add_argument("--scenario", type=str, default="normal",
                        choices=["normal", "spike", "malformed"],
                        help="Load profile preset (default: normal)")
    return parser.parse_args()


def get_kafka_config() -> dict:
    """
    Build confluent-kafka Producer config from environment variables.

    Returns
    -------
    dict
        Config dict for confluent_kafka.Producer()

    Raises
    ------
    EnvironmentError
        If KAFKA_BROKER is not set.
    """
    broker = os.environ.get("KAFKA_BROKER")
    if not broker:
        raise EnvironmentError(
            "KAFKA_BROKER is not set. Copy .env.example → .env and fill in values."
        )
    return {
        "bootstrap.servers": broker,
        "acks": "all",              # wait for all in-sync replicas
        "retries": 5,
        "linger.ms": 10,            # micro-batching for throughput
    }


def main() -> None:
    """
    Main producer loop.

    Stub flow (to be implemented):
      1. Parse CLI args
      2. Instantiate confluent_kafka.Producer with get_kafka_config()
      3. Enter production loop:
         a. Call log_templates.generate_log(service) for a random service
         b. Optionally corrupt the message (if malformed-pct / scenario requires)
         c. Serialise to JSON bytes
         d. producer.produce(topic, key=service, value=message_bytes)
         e. producer.poll(0)  — serve delivery callbacks
         f. Sleep to honour --rate
      4. On exit (duration elapsed or KeyboardInterrupt):
         producer.flush()
         Print summary: total sent, total failed, elapsed time
    """
    args = parse_args()

    print(f"[producer] Starting | scenario={args.scenario} rate={args.rate}/s "
          f"duration={args.duration}s malformed={args.malformed_pct}%")
    print("[producer] STUB — business logic not yet implemented.")
    print(f"[producer] Would connect to broker: {os.environ.get('KAFKA_BROKER', '<not set>')}")
    print(f"[producer] Would publish to topic : {os.environ.get('KAFKA_TOPIC_LOGS', 'logs')}")


if __name__ == "__main__":
    main()
