"""
ingestion/log_templates.py — Person 1 (Ingestion Layer)
=========================================================

Role
----
Provides parameterised log message generators for each known service.
Called exclusively by producer.py to generate realistic synthetic log data.

This module is internal to the ingestion/ layer and has no direct Kafka
dependency. It must be imported by producer.py — not by consumers.

Input
-----
  generate_log(service: str, severity: str | None = None) is called with:
    - service : one of the names in SERVICES
    - severity: optional override; if None, severity is chosen probabilistically
                (INFO 60%, WARNING 20%, ERROR 15%, CRITICAL 5%)

Output (Return Value)
---------------------
  Returns a dict conforming to shared/schemas/log_schema.json:
    {
      "timestamp" : "<ISO 8601 UTC>",
      "service"   : "<service name>",
      "severity"  : "<DEBUG|INFO|WARNING|ERROR|CRITICAL>",
      "message"   : "<realistic log line>",
      "trace_id"  : "<32-char lowercase hex>"
    }
  The caller (producer.py) is responsible for JSON-serialising this dict.

Generators (to be implemented per service)
-------------------------------------------
  _auth_log()     → login attempts, token issues, 2FA events, session timeouts
  _payment_log()  → transaction start/end, payment failures, refund events
  _api_log()      → HTTP request/response lines (method, path, status, latency)
  _db_log()       → query events, slow query warnings, connection pool metrics

Interface Contract
------------------
  Consumed by: ingestion/producer.py only
  No Kafka, DB, or network I/O in this module.
"""

import uuid
import random
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Known services — must match service names used in processing/db/schema.sql
# and referenced by consumers during aggregation.
# ---------------------------------------------------------------------------
SERVICES = ["auth-service", "payment-service", "api-gateway", "db-proxy"]

SEVERITIES = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
_SEVERITY_WEIGHTS = [5, 60, 20, 12, 3]   # roughly realistic distribution


def _make_trace_id() -> str:
    """Return a 32-character lowercase hex trace ID (UUID4, no hyphens)."""
    return uuid.uuid4().hex


def _pick_severity(override: str | None) -> str:
    """Return severity, either the provided override or a weighted random choice."""
    if override and override in SEVERITIES:
        return override
    return random.choices(SEVERITIES, weights=_SEVERITY_WEIGHTS, k=1)[0]


# ---------------------------------------------------------------------------
# Per-service log generators — stubs to be replaced with realistic templates
# ---------------------------------------------------------------------------

def _auth_log(severity: str) -> str:
    """
    Generate an auth-service log message.

    TODO: implement varied messages for:
      - Successful / failed login (include user_id, IP)
      - Token refresh / expiry
      - 2FA challenge events
      - Session revocation
    """
    return f"[auth-service] STUB severity={severity} — implement me"


def _payment_log(severity: str) -> str:
    """
    Generate a payment-service log message.

    TODO: implement varied messages for:
      - Transaction initiated / completed (include txn_id, amount, currency)
      - Payment gateway timeout
      - Refund processed / failed
      - Fraud flag raised
    """
    return f"[payment-service] STUB severity={severity} — implement me"


def _api_log(severity: str) -> str:
    """
    Generate an api-gateway log message.

    TODO: implement varied messages for:
      - HTTP request line: method, path, status code, latency_ms
      - Rate limit exceeded
      - Upstream service error (5xx)
      - Request validation failure (4xx)
    """
    return f"[api-gateway] STUB severity={severity} — implement me"


def _db_log(severity: str) -> str:
    """
    Generate a db-proxy log message.

    TODO: implement varied messages for:
      - Query executed (include query_hash, duration_ms)
      - Slow query warning (duration > threshold)
      - Connection pool exhausted
      - Deadlock detected / resolved
    """
    return f"[db-proxy] STUB severity={severity} — implement me"


_GENERATORS = {
    "auth-service":    _auth_log,
    "payment-service": _payment_log,
    "api-gateway":     _api_log,
    "db-proxy":        _db_log,
}


def generate_log(service: str, severity: str | None = None) -> dict:
    """
    Generate one synthetic log message dict for the given service.

    Parameters
    ----------
    service  : str
        Must be one of SERVICES. If not recognised, raises ValueError.
    severity : str | None
        Optional severity override. If None, weighted random selection is used.

    Returns
    -------
    dict
        Conforms to shared/schemas/log_schema.json. Ready to be JSON-serialised
        by producer.py and published to the Kafka `logs` topic.

    Raises
    ------
    ValueError
        If `service` is not a known service name.
    """
    if service not in _GENERATORS:
        raise ValueError(f"Unknown service '{service}'. Must be one of: {SERVICES}")

    sev     = _pick_severity(severity)
    message = _GENERATORS[service](sev)

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "service":   service,
        "severity":  sev,
        "message":   message,
        "trace_id":  _make_trace_id(),
    }
