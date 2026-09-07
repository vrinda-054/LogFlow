"""
ingestion/log_generator.py — Person 1 (Ingestion Layer)
=========================================================

Role
----
Core log generator logic supporting rate control, execution modes/scenarios,
and malformed message generation for Person 1 requirements (REQ-1 through REQ-4).

Supported Scenarios / Modes:
  - normal: steady rate, 100% valid messages (unless overridden by malformed_pct)
  - spike: normal load with periodic high traffic bursts
  - malformed: high percentage (50% default) malformed log generation
"""

import random
from typing import Any, Dict

try:
    from log_templates import generate_log, SERVICES
except ImportError:
    from ingestion.log_templates import generate_log, SERVICES


class LogGenerator:
    """
    Handles generation of synthetic log dictionary objects, including
    optional malformed message injection.
    """

    def __init__(self, malformed_pct: int = 0, scenario: str = "normal"):
        self.malformed_pct = malformed_pct
        self.scenario = scenario
        if self.scenario == "malformed" and self.malformed_pct == 0:
            self.malformed_pct = 50

    def generate(self, service: str | None = None) -> Dict[str, Any]:
        """
        Generate a single log message payload. If malformed check triggers,
        returns an intentionally malformed record.
        """
        if not service:
            service = random.choice(SERVICES)

        # Check if message should be malformed
        if self.malformed_pct > 0 and random.randint(1, 100) <= self.malformed_pct:
            return self._generate_malformed(service)

        return generate_log(service)

    def _generate_malformed(self, service: str) -> Dict[str, Any]:
        """
        Produce a malformed log dict violating shared/schemas/log_schema.json.
        """
        corruption_type = random.choice([
            "missing_timestamp",
            "invalid_severity",
            "invalid_trace_id",
            "missing_service",
            "non_string_message",
            "invalid_json"
        ])

        if corruption_type == "invalid_json":
            return {"_invalid_json": True, "service": service}

        log = generate_log(service)

        if corruption_type == "missing_timestamp":
            log.pop("timestamp", None)
        elif corruption_type == "invalid_severity":
            log["severity"] = "UNKNOWN_SEVERITY"
        elif corruption_type == "invalid_trace_id":
            log["trace_id"] = "invalid-trace-id-with-hyphens-and-too-short"
        elif corruption_type == "missing_service":
            log.pop("service", None)
        elif corruption_type == "non_string_message":
            log["message"] = {"nested_error": "message is an object instead of string"}

        return log
