"""Focused unit checks for Person 3's aggregation and validation contracts."""

from datetime import datetime, timezone
from unittest import TestCase
from unittest.mock import patch

from jsonschema import ValidationError

from processing import aggregator


def _record(severity: str = "INFO", service: str = "auth-service") -> dict:
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": service,
        "severity": severity,
        "message": "test event",
        "trace_id": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6",
    }


class AggregatorContractTests(TestCase):
    def setUp(self) -> None:
        aggregator._reset_window()

    def tearDown(self) -> None:
        aggregator._reset_window()

    def test_validation_rejects_missing_required_fields(self) -> None:
        record = _record()
        del record["trace_id"]

        with self.assertRaises(ValidationError):
            aggregator.validate_log_record(record)

    def test_ingest_counts_errors_per_service(self) -> None:
        aggregator.ingest(_record("INFO"))
        aggregator.ingest(_record("ERROR"))
        aggregator.ingest(_record("CRITICAL", service="payment-service"))

        self.assertEqual(aggregator._throughput_counters["auth-service"], 2)
        self.assertEqual(aggregator._error_counters["auth-service"], 1)
        self.assertEqual(aggregator._throughput_counters["payment-service"], 1)
        self.assertEqual(aggregator._error_counters["payment-service"], 1)

    def test_flush_window_writes_raw_and_metric_rows(self) -> None:
        class Cursor:
            def __init__(self) -> None:
                self.statements = []

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def executemany(self, statement, args):
                self.statements.append(("many", statement, args))

            def execute(self, statement, args):
                self.statements.append(("one", statement, args))

        class Connection:
            def __init__(self) -> None:
                self.cursor_instance = Cursor()
                self.committed = False

            def cursor(self):
                return self.cursor_instance

            def commit(self):
                self.committed = True

            def rollback(self):
                raise AssertionError("flush_window unexpectedly rolled back")

            def close(self):
                return None

        connection = Connection()
        aggregator.ingest(_record("ERROR"))

        with patch.object(aggregator, "get_connection", return_value=connection):
            aggregator.flush_window()

        self.assertTrue(connection.committed)
        self.assertEqual(len(connection.cursor_instance.statements), 3)
        self.assertEqual(aggregator._log_buffer, [])
        self.assertEqual(aggregator._throughput_counters, {})
