"""
processing/api/main.py — Person 3 (Processing Layer)
======================================================

Role
----
FastAPI application exposing the LogFlow metrics and DLQ data to the React
dashboard (Person 4). All data is read from PostgreSQL via the tables defined
in processing/db/schema.sql.

This module is the **downstream boundary** of the processing layer:
  - It READS from PostgreSQL (via processing/db/connection.py)
  - It SERVES HTTP JSON responses consumed by dashboard/src/api/client.js

Base URL: http://localhost:8000  (configurable via FASTAPI_PORT in .env)

Endpoints
---------
  GET /health
    Returns {"status": "ok"} — used by Docker healthcheck and dashboard.

  GET /metrics/throughput
    Returns per-service message throughput for the last N minutes.
    Query params: ?minutes=60&service=auth-service (both optional)
    Response: { "windows": [ { "window_start", "service", "messages_per_sec" } ] }
    Source table: logflow.metrics_throughput

  GET /metrics/lag
    Returns Kafka consumer lag per partition.
    Query params: ?partition=0 (optional filter)
    Response: { "partitions": [ { "partition_id", "lag", "recorded_at" } ] }
    Source table: logflow.metrics_consumer_lag

  GET /metrics/errors
    Returns per-service error rate for the last N minutes.
    Query params: ?minutes=60&service=payment-service (both optional)
    Response: { "windows": [ { "window_start", "service", "error_rate_pct" } ] }
    Source table: logflow.metrics_error_rate

  GET /dlq/messages
    Returns recent DLQ entries for inspection.
    Query params: ?limit=50&offset=0 (pagination)
    Response: { "total": N, "messages": [ { "id", "failed_at", "failure_reason",
                                             "retry_count", "original_message" } ] }
    Source table: logflow.dlq_log

CORS
----
  Origins: [env: VITE_API_BASE_URL, "http://localhost:5173"]
  The dashboard (Person 4) makes requests from a browser; CORS must be enabled.

Consumed by
-----------
  dashboard/src/api/client.js (Person 4)
"""

import os
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

# ---------------------------------------------------------------------------
# TODO (Person 3): import DB session and query helpers
# ---------------------------------------------------------------------------
# from processing.db.connection import get_session


app = FastAPI(
    title="LogFlow Metrics API",
    description="Real-time log processing metrics and DLQ inspection for the LogFlow pipeline.",
    version="0.1.0",
)

# ---------------------------------------------------------------------------
# CORS — allow React dev server (port 5173) and configurable API base URL
# ---------------------------------------------------------------------------
_allowed_origins = [
    "http://localhost:5173",
    os.environ.get("VITE_API_BASE_URL", "http://localhost:8000"),
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health", tags=["infra"])
def health_check():
    """
    Health check endpoint.

    Returns
    -------
    dict
        {"status": "ok"} when the API is running.
        TODO: extend to also check DB connectivity.
    """
    return {"status": "ok"}


@app.get("/metrics/throughput", tags=["metrics"])
def get_throughput(
    minutes: int = Query(default=60, ge=1, le=1440,
                         description="Lookback window in minutes"),
    service: str | None = Query(default=None,
                                description="Filter by service name"),
):
    """
    Return per-service message throughput over the last `minutes` minutes.

    Input  ← logflow.metrics_throughput (PostgreSQL)
    Output → dashboard/src/components/ThroughputChart.jsx

    TODO (Person 3): implement DB query:
      SELECT window_start, service, messages_per_sec
      FROM logflow.metrics_throughput
      WHERE window_start >= NOW() - INTERVAL '{minutes} minutes'
      [AND service = :service]
      ORDER BY window_start ASC;
    """
    return {
        "stub": True,
        "message": "GET /metrics/throughput — not yet implemented",
        "params": {"minutes": minutes, "service": service},
    }


@app.get("/metrics/lag", tags=["metrics"])
def get_consumer_lag(
    partition: int | None = Query(default=None,
                                  description="Filter by partition ID (0–3)"),
):
    """
    Return the latest Kafka consumer lag per partition.

    Input  ← logflow.metrics_consumer_lag (PostgreSQL)
    Output → dashboard/src/components/ConsumerLagPanel.jsx

    TODO (Person 3): implement DB query:
      SELECT DISTINCT ON (partition_id)
             partition_id, lag, recorded_at, consumer_id
      FROM logflow.metrics_consumer_lag
      [WHERE partition_id = :partition]
      ORDER BY partition_id, recorded_at DESC;
    """
    return {
        "stub": True,
        "message": "GET /metrics/lag — not yet implemented",
        "params": {"partition": partition},
    }


@app.get("/metrics/errors", tags=["metrics"])
def get_error_rate(
    minutes: int = Query(default=60, ge=1, le=1440),
    service: str | None = Query(default=None),
):
    """
    Return per-service error rate over the last `minutes` minutes.

    Input  ← logflow.metrics_error_rate (PostgreSQL)
    Output → dashboard/src/components/ErrorRatePanel.jsx

    TODO (Person 3): implement DB query:
      SELECT window_start, service, error_rate_pct
      FROM logflow.metrics_error_rate
      WHERE window_start >= NOW() - INTERVAL '{minutes} minutes'
      [AND service = :service]
      ORDER BY window_start ASC;
    """
    return {
        "stub": True,
        "message": "GET /metrics/errors — not yet implemented",
        "params": {"minutes": minutes, "service": service},
    }


@app.get("/dlq/messages", tags=["dlq"])
def get_dlq_messages(
    limit:  int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0,  ge=0),
):
    """
    Return paginated DLQ entries for dashboard inspection.

    Input  ← logflow.dlq_log (PostgreSQL)
    Output → dashboard/src/components/DLQViewer.jsx

    TODO (Person 3): implement DB query:
      SELECT id, failed_at, failure_reason, retry_count, original_message
      FROM logflow.dlq_log
      ORDER BY failed_at DESC
      LIMIT :limit OFFSET :offset;
      -- also return COUNT(*) for pagination total
    """
    return {
        "stub": True,
        "message": "GET /dlq/messages — not yet implemented",
        "params": {"limit": limit, "offset": offset},
        "total": 0,
        "messages": [],
    }


# ---------------------------------------------------------------------------
# Dev server entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=os.environ.get("FASTAPI_HOST", "0.0.0.0"),
        port=int(os.environ.get("FASTAPI_PORT", "8000")),
        reload=True,
    )
