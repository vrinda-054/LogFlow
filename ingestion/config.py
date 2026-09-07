"""
ingestion/config.py — Person 1 (Ingestion Layer)
=================================================

Role
----
Configuration settings and environment variable parser for the ingestion layer.
"""

import os
from typing import NamedTuple

class IngestionConfig(NamedTuple):
    kafka_broker: str
    kafka_topic_logs: str

def load_config() -> IngestionConfig:
    """
    Load ingestion configuration from environment variables.
    
    Defaults:
      KAFKA_BROKER: localhost:9092
      KAFKA_TOPIC_LOGS: logs
    """
    kafka_broker = os.environ.get("KAFKA_BROKER", "localhost:9092")
    kafka_topic_logs = os.environ.get("KAFKA_TOPIC_LOGS", "logs")
    return IngestionConfig(
        kafka_broker=kafka_broker,
        kafka_topic_logs=kafka_topic_logs,
    )
