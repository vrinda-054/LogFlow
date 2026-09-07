"""
ingestion/log_templates.py — Person 1 (Ingestion Layer)
=========================================================

Role
----
Provides parameterised log message generators for each known service.
Called by log_generator.py and producer.py to generate realistic synthetic log data.

This module is internal to the ingestion/ layer and has no direct Kafka
dependency. It must be imported by log_generator.py / producer.py — not by consumers.

Input
-----
  generate_log(service: str, severity: str | None = None) is called with:
    - service : one of the names in SERVICES
    - severity: optional override; if None, severity is chosen probabilistically
                (DEBUG 5%, INFO 60%, WARNING 20%, ERROR 12%, CRITICAL 3%)

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

Generators (per service)
------------------------
  _auth_log()     → login attempts, token issues, 2FA events, session timeouts
  _payment_log()  → transaction start/end, payment failures, refund events
  _api_log()      → HTTP request/response lines (method, path, status, latency)
  _db_log()       → query events, slow query warnings, connection pool metrics
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
# Per-service realistic log generators
# ---------------------------------------------------------------------------

def _auth_log(severity: str) -> str:
    user_id = random.randint(1000, 9999)
    ip_addr = f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,255)}"
    
    if severity == "DEBUG":
        return f"User session evaluation started for user_id={user_id} ip={ip_addr}"
    elif severity == "INFO":
        action = random.choice(["User login successful", "Token refreshed", "2FA challenge verified", "Session created"])
        return f"{action} for user_id={user_id} ip={ip_addr}"
    elif severity == "WARNING":
        reason = random.choice(["Invalid password attempt (1/3)", "Token near expiration", "Unusual login location detected"])
        return f"{reason} for user_id={user_id} ip={ip_addr}"
    elif severity == "ERROR":
        reason = random.choice(["Login failed: invalid credentials", "2FA validation failure", "Session expired due to inactivity"])
        return f"{reason} for user_id={user_id} ip={ip_addr}"
    else:  # CRITICAL
        return f"Account locked after multiple failed login attempts user_id={user_id} ip={ip_addr}"


def _payment_log(severity: str) -> str:
    txn_id = f"txn_{uuid.uuid4().hex[:12]}"
    amount = round(random.uniform(5.0, 1500.0), 2)
    currency = random.choice(["USD", "EUR", "GBP", "CAD"])

    if severity == "DEBUG":
        return f"Payment payload validation passed txn_id={txn_id}"
    elif severity == "INFO":
        status = random.choice(["Transaction initiated", "Payment processed successfully", "Refund requested", "Settlement completed"])
        return f"{status} txn_id={txn_id} amount={amount} {currency}"
    elif severity == "WARNING":
        msg = random.choice(["Card expiry date warning", "High amount transaction flagged for manual review"])
        return f"{msg} txn_id={txn_id} amount={amount} {currency}"
    elif severity == "ERROR":
        reason = random.choice(["Payment declined by gateway: insufficient_funds", "Gateway connection timeout", "Invalid card CVV"])
        return f"{reason} txn_id={txn_id} amount={amount} {currency}"
    else:  # CRITICAL
        return f"Payment gateway outage detected during txn_id={txn_id} amount={amount} {currency}"


def _api_log(severity: str) -> str:
    method = random.choice(["GET", "POST", "PUT", "DELETE"])
    endpoint = random.choice(["/v1/users", "/v1/orders", "/v1/auth/login", "/v1/products", "/v1/checkout"])
    latency_ms = random.randint(5, 450)
    
    if severity == "DEBUG":
        return f"Routing HTTP request {method} {endpoint} header_count={random.randint(5,15)}"
    elif severity == "INFO":
        status = random.choice([200, 201, 204])
        return f"HTTP {method} {endpoint} status={status} latency={latency_ms}ms"
    elif severity == "WARNING":
        status = random.choice([400, 401, 403, 404, 429])
        latency_ms = random.randint(200, 1200)
        return f"HTTP {method} {endpoint} status={status} latency={latency_ms}ms"
    elif severity == "ERROR":
        status = random.choice([500, 502, 503, 504])
        latency_ms = random.randint(1500, 5000)
        return f"HTTP {method} {endpoint} status={status} latency={latency_ms}ms error='Upstream service failure'"
    else:  # CRITICAL
        return f"API Gateway threshold reached: circuit breaker tripped on endpoint {endpoint}"


def _db_log(severity: str) -> str:
    query_hash = f"q_{uuid.uuid4().hex[:8]}"
    duration_ms = random.randint(1, 45)

    if severity == "DEBUG":
        return f"Executing query_hash={query_hash} params_count={random.randint(1,5)}"
    elif severity == "INFO":
        return f"Query executed query_hash={query_hash} rows_affected={random.randint(1,100)} duration={duration_ms}ms"
    elif severity == "WARNING":
        slow_duration = random.randint(250, 1500)
        return f"Slow query detected query_hash={query_hash} duration={slow_duration}ms threshold=200ms"
    elif severity == "ERROR":
        return f"Database query failed query_hash={query_hash} error='deadlock detected' duration={duration_ms}ms"
    else:  # CRITICAL
        return f"Connection pool exhausted max_connections=100 active_connections=100 query_hash={query_hash}"


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
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "service":   service,
        "severity":  sev,
        "message":   message,
        "trace_id":  _make_trace_id(),
    }
