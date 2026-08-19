"""
ingestion/producer.py — Person 1 (Ingestion Layer)
====================================================

Role
----
Entry point for the LogFlow ingestion layer. Generates synthetic log messages
and publishes them to the Kafka topic `logs` (4 partitions) using the
confluent-kafka producer client.

CLI Interface
-------------
Run via:  python producer.py [OPTIONS]

Options
-------
  --rate INT          Messages per second target (default: 10)
  --duration INT      Total run time in seconds; 0 = run forever (default: 0)
  --malformed-pct INT Percentage of messages [0–100] that should be intentionally
                      malformed (default: 0)
  --scenario STR      Load profile preset (normal, spike, malformed) (default: normal)
"""

import argparse
import json
import os
import sys
import time
from typing import Any, Dict

try:
    from confluent_kafka import Producer, KafkaError, KafkaException
except ImportError:
    Producer = None  # Fallback check for missing library during standalone run

# Support both relative invocation (cd ingestion && python producer.py) and module invocation
try:
    from config import load_config
    from log_generator import LogGenerator
    from log_templates import SERVICES
except ImportError:
    from ingestion.config import load_config
    from ingestion.log_generator import LogGenerator
    from ingestion.log_templates import SERVICES


def parse_args() -> argparse.Namespace:
    """Parse and validate CLI arguments."""
    parser = argparse.ArgumentParser(
        description="LogFlow synthetic log producer — publishes to Kafka topic 'logs'."
    )
    parser.add_argument("--rate", type=int, default=10,
                        help="Messages per second target (default: 10)")
    parser.add_argument("--duration", type=int, default=0,
                        help="Run duration in seconds; 0 = run forever (default: 0)")
    parser.add_argument("--malformed-pct", type=int, default=0, choices=range(0, 101),
                        metavar="[0-100]",
                        help="Percentage of intentionally malformed messages [0-100] (default: 0)")
    parser.add_argument("--scenario", type=str, default="normal",
                        choices=["normal", "spike", "malformed"],
                        help="Load profile preset (default: normal)")

    args = parser.parse_args()

    if args.rate <= 0:
        parser.error("--rate must be a positive integer > 0")
    if args.duration < 0:
        parser.error("--duration cannot be negative")

    return args


class ProducerStats:
    """Tracks message delivery metrics for console instrumentation."""

    def __init__(self) -> None:
        self.attempted: int = 0
        self.delivered: int = 0
        self.failed: int = 0
        self.start_time: float = time.time()

    def delivery_callback(self, err: Any, msg: Any) -> None:
        """Callback triggered on Kafka broker ack / delivery failure."""
        if err is not None:
            self.failed += 1
        else:
            self.delivered += 1

    def print_periodic_report(self) -> None:
        """Print readable console stats snapshot."""
        elapsed = time.time() - self.start_time
        rate = self.delivered / elapsed if elapsed > 0 else 0.0
        print(f"[producer] Produced: {self.attempted} | Delivered: {self.delivered} | "
              f"Failed: {self.failed} | Rate: {rate:.1f} msg/s")

    def print_final_summary(self) -> None:
        """Print final summary on exit."""
        elapsed = time.time() - self.start_time
        rate = self.delivered / elapsed if elapsed > 0 else 0.0
        print("\n" + "=" * 60)
        print("LogFlow Producer Execution Summary")
        print("=" * 60)
        print(f"Elapsed Time:       {elapsed:.2f} seconds")
        print(f"Messages Attempted: {self.attempted}")
        print(f"Messages Delivered: {self.delivered}")
        print(f"Messages Failed:    {self.failed}")
        print(f"Achieved Rate:      {rate:.2f} msg/s")
        print("=" * 60)


def create_kafka_producer(broker: str) -> Any:
    """Build confluent_kafka Producer instance with optimized throughput settings."""
    if Producer is None:
        raise RuntimeError("confluent-kafka package is not installed. Install via requirements.txt")

    config = {
        "bootstrap.servers": broker,
        "acks": "all",              # Wait for full in-sync replica acknowledgements
        "retries": 5,
        "linger.ms": 10,            # Micro-batching for throughput target >= 2000 msg/s
        "batch.num.messages": 1000,
    }
    return Producer(config)


def run_producer(args: argparse.Namespace) -> None:
    """Main producer loop supporting normal, spike, and malformed modes."""
    cfg = load_config()
    print(f"[producer] Starting LogFlow Producer")
    print(f"[producer] Broker: {cfg.kafka_broker} | Topic: {cfg.kafka_topic_logs}")
    print(f"[producer] Scenario: {args.scenario} | Target Rate: {args.rate}/s | Duration: {args.duration}s | Malformed: {args.malformed_pct}%")

    producer = create_kafka_producer(cfg.kafka_broker)
    generator = LogGenerator(malformed_pct=args.malformed_pct, scenario=args.scenario)
    stats = ProducerStats()

    # Traffic Spike configuration defaults (burst every 10s for 3s duration at 5x target rate)
    spike_interval_sec = 10.0
    spike_duration_sec = 3.0
    spike_multiplier = 5.0

    last_report_time = time.time()
    stats.start_time = time.time()
    next_batch_target_time = stats.start_time
    batch_count = 0

    try:
        while True:
            now = time.time()
            elapsed_total = now - stats.start_time

            if args.duration > 0 and elapsed_total >= args.duration:
                break

            # Determine current rate limit based on scenario
            current_target_rate = args.rate
            if args.scenario == "spike":
                cycle_time = (now - stats.start_time) % spike_interval_sec
                if cycle_time < spike_duration_sec:
                    current_target_rate = int(args.rate * spike_multiplier)

            # Generate and serialize log
            # Key MUST be service name to partition by service (REQ-5)
            log_dict = generator.generate()
            service_key = log_dict.get("service", "unknown-service")
            if not isinstance(service_key, str):
                service_key = str(service_key)

            # Support malformed JSON string injection for invalid JSON cases
            if log_dict.get("_invalid_json"):
                value_bytes = b"{{invalid_json_payload: missing_quotes}"
            else:
                value_bytes = json.dumps(log_dict).encode("utf-8")

            stats.attempted += 1
            batch_count += 1

            # Produce asynchronously
            producer.produce(
                topic=cfg.kafka_topic_logs,
                key=service_key.encode("utf-8"),
                value=value_bytes,
                on_delivery=stats.delivery_callback
            )

            producer.poll(0)  # Serve queued delivery callbacks without blocking

            # Rate pacing calculation using continuous schedule targeting to prevent sleep drift
            target_batch_size = max(1, int(current_target_rate * 0.01))
            if batch_count >= target_batch_size:
                pacing_rate = current_target_rate * 1.0008
                next_batch_target_time += batch_count / pacing_rate
                now = time.time()
                # Reset schedule target if system lags significantly behind (> 0.5s)
                if now - next_batch_target_time > 0.5:
                    next_batch_target_time = now

                sleep_time = next_batch_target_time - now
                if sleep_time > 0:
                    time.sleep(sleep_time)
                batch_count = 0

            # Periodic console reporting (every 3 seconds)
            if time.time() - last_report_time >= 3.0:
                stats.print_periodic_report()
                last_report_time = time.time()

    except KeyboardInterrupt:
        print("\n[producer] Interrupted by user.")
    finally:
        print("[producer] Flushing remaining queued messages...")
        producer.flush(timeout=10.0)
        stats.print_final_summary()


def main() -> None:
    args = parse_args()
    run_producer(args)


if __name__ == "__main__":
    main()
