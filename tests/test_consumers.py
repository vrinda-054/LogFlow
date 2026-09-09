"""Isolated unit tests for Person 2 consumer and fault-tolerance behavior."""

from __future__ import annotations

import json
import os
import sys
import unittest
from unittest.mock import Mock, patch

from consumers.backpressure import check_backpressure, pause_partition, resume_partition
from consumers.consumer import process_message
from consumers.dlq_handler import publish_to_dlq, retry_with_backoff
from consumers.rebalance_config import get_consumer_config, on_assign, on_revoke


class RetryAndDlqTests(unittest.TestCase):
    def test_retry_succeeds_after_transient_failure(self) -> None:
        attempts = 0

        def process(_message: dict[str, str]) -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise TimeoutError("temporary downstream timeout")

        with patch("consumers.dlq_handler.time.sleep") as sleep:
            success, history = retry_with_backoff({"id": "1"}, process, max_retries=3, base_delay_ms=10)

        self.assertTrue(success)
        self.assertEqual(attempts, 2)
        self.assertEqual([item["result"] for item in history], ["failure", "success"])
        sleep.assert_called_once()

    def test_retry_exhaustion_publishes_dlq_without_looping(self) -> None:
        process = Mock(side_effect=RuntimeError("permanent failure"))
        with patch("consumers.dlq_handler.time.sleep"), patch(
            "consumers.dlq_handler.publish_to_dlq", return_value={"retry_count": 3}
        ) as publish:
            success, history = retry_with_backoff("payload", process, max_retries=2, base_delay_ms=0)

        self.assertFalse(success)
        self.assertEqual(process.call_count, 3)
        self.assertEqual(sum(item["result"] == "failure" for item in history), 3)
        publish.assert_called_once()

    def test_dlq_envelope_preserves_payload_and_context(self) -> None:
        producer = Mock()
        with patch("consumers.dlq_handler._get_dlq_producer", return_value=producer):
            envelope = publish_to_dlq(
                "{bad-json}",
                "Deserialization error",
                [{"attempt": 1, "result": "immediate_failure", "reason": "bad JSON"}],
            )

        self.assertEqual(envelope["original_message"], "{bad-json}")
        self.assertEqual(envelope["retry_count"], 0)
        self.assertEqual(len(envelope["retry_history"]), 1)
        produced = json.loads(producer.produce.call_args.kwargs["value"])
        self.assertEqual(produced["failure_reason"], "Deserialization error")


class ConsumerValidationTests(unittest.TestCase):
    def test_valid_message_is_decoded_and_schema_validated(self) -> None:
        payload = {
            "timestamp": "2026-09-09T12:00:00+00:00",
            "service": "unit-test",
            "severity": "INFO",
            "message": "hello",
            "trace_id": "0" * 32,
        }
        self.assertEqual(process_message(json.dumps(payload).encode()), payload)

    def test_malformed_message_is_rejected_before_processing(self) -> None:
        with self.assertRaises(ValueError) as error:
            process_message(b"{bad-json}")
        self.assertIn("Deserialization error", str(error.exception))


class RebalanceAndBackpressureTests(unittest.TestCase):
    def test_consumer_config_uses_manual_cooperative_consumption(self) -> None:
        with patch.dict(os.environ, {"KAFKA_BROKER": "kafka:9092"}, clear=False):
            config = get_consumer_config()
        self.assertFalse(config["enable.auto.commit"])
        self.assertEqual(config["partition.assignment.strategy"], "cooperative-sticky")

    def test_rebalance_callbacks_commit_and_cover_partitions(self) -> None:
        consumer = Mock()
        partitions = [Mock(partition=0, topic="logs-raw"), Mock(partition=1, topic="logs-raw")]
        with self.assertLogs("consumers.rebalance_config", level="INFO") as logs:
            on_assign(consumer, partitions, "consumer-01")
            on_revoke(consumer, partitions, "consumer-01")
        consumer.commit.assert_called_once_with(offsets=partitions, asynchronous=False)
        output = "\n".join(logs.output)
        self.assertIn("partition_assigned", output)
        self.assertIn("partition_revoked", output)

    def test_backpressure_has_hysteresis_and_controls_partition(self) -> None:
        self.assertEqual(check_backpressure(100, 10, 50), "PAUSE")
        self.assertEqual(check_backpressure(5, 10, 50), "RESUME")
        self.assertEqual(check_backpressure(30, 10, 50), "OK")
        consumer = Mock()
        partition = Mock(partition=2, topic="logs-raw")
        self.assertEqual(pause_partition(consumer, partition, 100, 50).action, "PAUSE")
        self.assertEqual(resume_partition(consumer, partition, 5, 10).action, "RESUME")
        consumer.pause.assert_called_once()
        consumer.resume.assert_called_once()


if __name__ == "__main__":
    unittest.main()