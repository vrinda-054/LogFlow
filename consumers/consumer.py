
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import time
from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import jsonschema

try:
    from confluent_kafka import Consumer, KafkaException, TopicPartition
except ImportError:  # pragma: no cover
    Consumer = None
    KafkaException = Exception
    TopicPartition = None

try:
    from .backpressure import check_backpressure, pause_partition, resume_partition
    from .dlq_handler import retry_with_backoff, classify_failure, publish_to_dlq
    from .rebalance_config import get_consumer_config, on_assign, on_revoke, get_topic
except ImportError:
    from backpressure import check_backpressure, pause_partition, resume_partition
    from dlq_handler import retry_with_backoff, classify_failure, publish_to_dlq
    from rebalance_config import get_consumer_config, on_assign, on_revoke, get_topic

logger = logging.getLogger(__name__)
SCHEMA_PATH = Path(__file__).resolve().parents[1] / "shared" / "schemas" / "log_schema.json"
with SCHEMA_PATH.open(encoding="utf-8") as schema_file:
    LOG_SCHEMA = json.load(schema_file)


@dataclass(frozen=True)
class ConsumerStatus:
    """Stable dashboard/metrics contract for one consumer process."""

    consumer_id: str
    status: str
    assigned_partitions: list[int]
    processing_rate: float
    consumer_lag: int
    last_heartbeat: str
    backpressure_active: bool


@dataclass(frozen=True)
class PartitionStatus:
    """Stable per-partition health contract derived from Kafka lag."""

    partition: int
    throughput: float
    current_lag: int
    assigned_consumer: str
    health: str


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _configure_logging() -> None:
    class ConsumerIdFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            if not hasattr(record, "consumer_id"):
                record.consumer_id = os.environ.get("CONSUMER_ID", "system")
            return True

    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s consumer_id=%(consumer_id)s %(levelname)s %(message)s",
    )
    for handler in logging.getLogger().handlers:
        handler.addFilter(ConsumerIdFilter())


def _log(consumer_id: str, level: int, message: str, *args: Any) -> None:
    logger.log(level, message, *args, extra={"consumer_id": consumer_id})


def process_message(message_bytes: bytes) -> dict[str, Any]:
    """Decode UTF-8 JSON and validate the exact shared log schema."""
    try:
        value = json.loads(message_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(classify_failure(exc)) from exc
    try:
        jsonschema.validate(value, LOG_SCHEMA, format_checker=jsonschema.FormatChecker())
    except jsonschema.ValidationError as exc:
        raise ValueError(classify_failure(exc)) from exc
    return value


def report_consumer_status(status: ConsumerStatus | dict[str, Any]) -> None:
    """Write a local status snapshot; Person 3 can replace this with metrics/DB output."""
    payload = asdict(status) if isinstance(status, ConsumerStatus) else status
    path = Path(os.environ.get("CONSUMER_STATUS_FILE", "consumer-status.json"))
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def report_partition_status(status: PartitionStatus | dict[str, Any]) -> None:
    """Write one partition snapshot; Person 3 can replace this with metrics output."""
    payload = asdict(status) if isinstance(status, PartitionStatus) else status
    path = Path(os.environ.get("PARTITION_STATUS_FILE", "partition-status.json"))
    path.parent.mkdir(parents=True, exist_ok=True)
    existing: dict[str, Any] = {}
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            existing = {}
    existing[str(payload["partition"])] = payload
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(existing, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def _partition_lag(consumer: Any, topic_partition: Any) -> int:
    try:
        committed = consumer.committed([topic_partition], timeout=1.0)[0].offset
        high_water = consumer.get_watermark_offsets(topic_partition, timeout=1.0)[1]
        if committed < 0:
            committed = consumer.position([topic_partition])[0].offset
        return max(0, int(high_water - committed))
    except KafkaException as exc:
        _log(
            str(getattr(consumer, "consumer_id", "unknown")),
            logging.WARNING,
            "lag sample unavailable during rebalance: %s",
            exc,
        )
        return 0


def _process_record(record: dict[str, Any], delay_ms: int) -> None:
    """Forward a validated record to Person 3's callable integration point."""
    if delay_ms:
        time.sleep(delay_ms / 1000)
    try:
        from processing.aggregator import ingest
        ingest(record)
    except ImportError:
        logger.debug("processing.aggregator unavailable; validated record accepted")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="LogFlow Kafka consumer")
    parser.add_argument("--consumer-id", default=os.environ.get("CONSUMER_ID", "consumer-01"))
    parser.add_argument("--inject-delay-ms", type=int, default=0)
    return parser


def main() -> None:
    """Run the manual-commit poll loop until SIGTERM or SIGINT."""
    _configure_logging()
    args = _build_parser().parse_args()
    if args.inject_delay_ms < 0:
        raise SystemExit("--inject-delay-ms must be non-negative")
    if Consumer is None or TopicPartition is None:
        raise RuntimeError("confluent-kafka package is not installed")

    def _kafka_error_cb(err: Any) -> None:
        logger.error("Kafka client error: %s", err)

    config = get_consumer_config()
    config["error_cb"] = _kafka_error_cb
    consumer = Consumer(config, logger=logging.getLogger("logflow.kafka"))
    try:
        consumer.consumer_id = args.consumer_id
    except AttributeError:
        pass
    stopping = False

    def stop(_signum: int, _frame: Any) -> None:
        nonlocal stopping
        stopping = True
        _log(
            args.consumer_id,
            logging.INFO,
            json.dumps({
                "timestamp": _now(),
                "event": "consumer_disconnect",
                "consumer_id": args.consumer_id,
            }),
        )

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    topic = get_topic()
    consumer.subscribe(
        [topic],
        on_assign=lambda c, p: on_assign(c, p, args.consumer_id),
        on_revoke=lambda c, p: on_revoke(c, p, args.consumer_id),
    )
    _log(args.consumer_id, logging.INFO, "subscription requested topic=%s", topic)

    low_water = int(os.environ.get("BACKPRESSURE_LOW_WATER", "200"))
    high_water = int(os.environ.get("BACKPRESSURE_HIGH_WATER", "5000"))
    heartbeat_interval = float(os.environ.get("CONSUMER_HEARTBEAT_INTERVAL_SEC", "10"))
    max_retries = int(os.environ.get("DLQ_MAX_RETRIES", "3"))
    pause_duration_sec = max(0.0, float(os.environ.get("BACKPRESSURE_PAUSE_SEC", "1")))
    paused: set[int] = set()
    paused_since: dict[int, float] = {}
    processed: deque[float] = deque()
    partition_counts: dict[int, deque[float]] = {}
    last_heartbeat = 0.0
    _log(args.consumer_id, logging.INFO, "consumer started")

    try:
        while not stopping:
            message = consumer.poll(1.0)
            now = time.time()

            # A paused partition cannot reduce its own Kafka lag. Re-check it
            # on a bounded cooldown so normal polling can resume and drain it.
            if paused:
                assigned_by_partition = {tp.partition: tp for tp in consumer.assignment()}
                for partition in list(paused):
                    if now - paused_since.get(partition, now) < pause_duration_sec:
                        continue
                    topic_partition = assigned_by_partition.get(partition)
                    if topic_partition is None:
                        paused.discard(partition)
                        paused_since.pop(partition, None)
                        continue
                    lag = _partition_lag(consumer, topic_partition)
                    resume_partition(consumer, topic_partition, lag, low_water)
                    paused.discard(partition)
                    paused_since.pop(partition, None)

            if message is not None and message.error():
                _log(args.consumer_id, logging.ERROR, "Kafka poll error: %s", message.error())

            elif message is not None:
                partition = message.partition()
                topic_partition = TopicPartition(message.topic(), message.partition(), message.offset())
                lag = _partition_lag(consumer, topic_partition)
                action = check_backpressure(lag, low_water, high_water)

                if action == "PAUSE" and partition not in paused:
                    pause_partition(consumer, topic_partition, lag, high_water)
                    paused.add(partition)
                    paused_since[partition] = now
                elif action == "RESUME" and partition in paused:
                    resume_partition(consumer, topic_partition, lag, low_water)
                    paused.discard(partition)
                    paused_since.pop(partition, None)

                raw = message.value()
                raw_text = raw.decode("utf-8", errors="replace")
                try:
                    record = process_message(raw)
                    process_fn = lambda item: _process_record(item, args.inject_delay_ms)
                    success, _history = retry_with_backoff(
                        record,
                        process_fn,
                        max_retries=max_retries,
                        base_delay_ms=int(os.environ.get("DLQ_BASE_DELAY_MS", "200")),
                    )
                except ValueError as exc:
                    failure_reason = classify_failure(exc)
                    publish_to_dlq(
                        raw_text,
                        failure_reason,
                        [{
                            "attempt": 1,
                            "result": "immediate_failure",
                            "timestamp": _now(),
                            "reason": failure_reason,
                        }],
                    )
                    success = False
                try:
                    consumer.commit(message=message, asynchronous=False)
                except KafkaException as exc:
                    # A rebalance may invalidate this generation after the
                    # message was processed; the new owner can replay it.
                    _log(args.consumer_id, logging.WARNING, "message commit deferred during rebalance: %s", exc)

                if success:
                    processed.append(now)
                    partition_times = partition_counts.setdefault(partition, deque())
                    partition_times.append(now)

            if now - last_heartbeat >= heartbeat_interval:
                assigned = consumer.assignment()
                lags = [_partition_lag(consumer, tp) for tp in assigned]
                while processed and processed[0] <= now - 60:
                    processed.popleft()

                status = ConsumerStatus(
                    args.consumer_id,
                    "PAUSED" if paused else "RUNNING",
                    [tp.partition for tp in assigned],
                    len(processed) / 60.0,
                    sum(lags),
                    _now(),
                    bool(paused),
                )
                report_consumer_status(status)
                _log(
                    args.consumer_id,
                    logging.INFO,
                    "heartbeat assigned=%s lag=%s processed_last_60s=%d",
                    status.assigned_partitions,
                    status.consumer_lag,
                    len(processed),
                )

                for topic_partition, partition_lag in zip(assigned, lags):
                    partition_times = partition_counts.setdefault(topic_partition.partition, deque())
                    while partition_times and partition_times[0] <= now - 60:
                        partition_times.popleft()
                    throughput = len(partition_times) / 60.0
                    health = (
                        "HEALTHY" if partition_lag <= low_water
                        else "DEGRADED" if partition_lag < high_water
                        else "UNHEALTHY"
                    )
                    report_partition_status(
                        PartitionStatus(topic_partition.partition, throughput, partition_lag, args.consumer_id, health)
                    )
                last_heartbeat = now
    finally:
        _log(args.consumer_id, logging.INFO, "consumer disconnecting")
        consumer.close()


if __name__ == "__main__":
    main()