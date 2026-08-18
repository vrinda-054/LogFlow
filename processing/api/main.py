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
    Returns {"status": "ok", "database": "connected"} — used by Docker
    healthcheck and dashboard system status panel.

  GET /metrics/throughput
    Returns per-service message throughput for the last N minutes.
    Query params: ?minutes=60&service=auth-service (both optional)
    Response: { "windows": [...], "summary": { "current_rate", "peak", "avg" } }
    Source table: logflow.metrics_throughput

  GET /metrics/lag
    Returns Kafka consumer lag per partition.
    Query params: ?partition=0 (optional filter)
    Response: { "total_lag": N, "partitions": [...] }
    Source table: logflow.metrics_consumer_lag

  GET /metrics/errors
    Returns per-service error rate for the last N minutes.
    Query params: ?minutes=60&service=payment-service (both optional)
    Response: { "windows": [...], "summary": { "overall_error_rate", ... } }
    Source table: logflow.metrics_error_rate

  GET /dlq/messages
    Returns recent DLQ entries for inspection.
    Query params: ?limit=50&offset=0 (pagination)
    Response: { "total": N, "messages": [...] }
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
import logging
from datetime import datetime, timezone

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from db.connection import get_session, get_engine

logger = logging.getLogger(__name__)

app = FastAPI(
    title="LogFlow Metrics API",
    description=(
        "Real-time log processing metrics and DLQ inspection "
        "for the LogFlow pipeline."
    ),
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


# ═══════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════


@app.get("/health", tags=["infra"])
def health_check():
    """
    Health check endpoint.

    Returns
    -------
    dict
        {"status": "ok", "database": "connected"} when the API and DB are up.
        {"status": "degraded", "database": "unreachable"} if DB is down.
    """
    try:
        with get_session() as session:
            session.execute(text("SELECT 1"))
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        logger.warning("Health check: DB unreachable — %s", e)
        return {"status": "degraded", "database": "unreachable", "error": str(e)}


@app.get("/metrics/throughput", tags=["metrics"])
def get_throughput(
    minutes: int = Query(
        default=60, ge=1, le=1440,
        description="Lookback window in minutes",
    ),
    service: str | None = Query(
        default=None,
        description="Filter by service name",
    ),
):
    """
    Return per-service message throughput over the last `minutes` minutes.

    Input  ← logflow.metrics_throughput (PostgreSQL)
    Output → dashboard/src/components/ThroughputChart.jsx

    Returns a list of time windows with throughput data, plus a summary
    with current_rate, peak_rate, and average_rate for the dashboard header.
    """
    with get_session() as session:

        # --- Build the query with optional service filter ---
        query = """
            SELECT window_start, window_end, service,
                   message_count, messages_per_sec
            FROM metrics_throughput
            WHERE window_start >= NOW() - MAKE_INTERVAL(mins => :minutes)
        """
        params = {"minutes": minutes}

        if service:
            query += " AND service = :service"
            params["service"] = service

        query += " ORDER BY window_start ASC"

        rows = session.execute(text(query), params).fetchall()

        windows = [
            {
                "window_start": row.window_start.isoformat(),
                "window_end": row.window_end.isoformat(),
                "service": row.service,
                "message_count": row.message_count,
                "messages_per_sec": float(row.messages_per_sec),
            }
            for row in rows
        ]

        # --- Compute summary stats for the dashboard header ---
        rates = [w["messages_per_sec"] for w in windows]
        summary = {
            "current_rate": rates[-1] if rates else 0.0,
            "peak_rate": max(rates) if rates else 0.0,
            "average_rate": round(sum(rates) / len(rates), 2) if rates else 0.0,
            "total_windows": len(windows),
        }

    return {"windows": windows, "summary": summary}


@app.get("/metrics/lag", tags=["metrics"])
def get_consumer_lag(
    partition: int | None = Query(
        default=None,
        description="Filter by partition ID (0–3)",
    ),
):
    """
    Return the latest Kafka consumer lag per partition.

    Input  ← logflow.metrics_consumer_lag (PostgreSQL)
    Output → dashboard/src/components/ConsumerLagPanel.jsx

    Uses DISTINCT ON to get the most recent lag reading per partition.
    """
    with get_session() as session:

        query = """
            SELECT DISTINCT ON (partition_id)
                   partition_id, lag, recorded_at, consumer_id
            FROM metrics_consumer_lag
        """
        params = {}

        if partition is not None:
            query += " WHERE partition_id = :partition"
            params["partition"] = partition

        query += " ORDER BY partition_id, recorded_at DESC"

        rows = session.execute(text(query), params).fetchall()

        partitions = [
            {
                "partition_id": row.partition_id,
                "lag": row.lag,
                "recorded_at": row.recorded_at.isoformat(),
                "consumer_id": row.consumer_id,
            }
            for row in rows
        ]

        total_lag = sum(p["lag"] for p in partitions)

    return {"total_lag": total_lag, "partitions": partitions}


@app.get("/metrics/errors", tags=["metrics"])
def get_error_rate(
    minutes: int = Query(default=60, ge=1, le=1440),
    service: str | None = Query(default=None),
):
    """
    Return per-service error rate over the last `minutes` minutes.

    Input  ← logflow.metrics_error_rate (PostgreSQL)
    Output → dashboard/src/components/ErrorRatePanel.jsx

    Returns time-series windows plus a summary with overall error rate
    and per-service breakdown.
    """
    with get_session() as session:

        query = """
            SELECT window_start, window_end, service,
                   total_messages, error_messages, error_rate_pct
            FROM metrics_error_rate
            WHERE window_start >= NOW() - MAKE_INTERVAL(mins => :minutes)
        """
        params = {"minutes": minutes}

        if service:
            query += " AND service = :service"
            params["service"] = service

        query += " ORDER BY window_start ASC"

        rows = session.execute(text(query), params).fetchall()

        windows = [
            {
                "window_start": row.window_start.isoformat(),
                "window_end": row.window_end.isoformat(),
                "service": row.service,
                "total_messages": row.total_messages,
                "error_messages": row.error_messages,
                "error_rate_pct": float(row.error_rate_pct),
            }
            for row in rows
        ]

        # --- Per-service breakdown for the dashboard donut chart ---
        service_totals: dict[str, dict] = {}
        for w in windows:
            svc = w["service"]
            if svc not in service_totals:
                service_totals[svc] = {"total": 0, "errors": 0}
            service_totals[svc]["total"] += w["total_messages"]
            service_totals[svc]["errors"] += w["error_messages"]

        per_service = []
        grand_total = 0
        grand_errors = 0
        for svc, counts in service_totals.items():
            rate = (
                round((counts["errors"] / counts["total"]) * 100, 2)
                if counts["total"] > 0
                else 0.0
            )
            per_service.append({
                "service": svc,
                "total_messages": counts["total"],
                "error_messages": counts["errors"],
                "error_rate_pct": rate,
            })
            grand_total += counts["total"]
            grand_errors += counts["errors"]

        overall_rate = (
            round((grand_errors / grand_total) * 100, 2)
            if grand_total > 0
            else 0.0
        )

        summary = {
            "overall_error_rate_pct": overall_rate,
            "total_messages": grand_total,
            "total_errors": grand_errors,
            "per_service": per_service,
        }

    return {"windows": windows, "summary": summary}


@app.get("/dlq/messages", tags=["dlq"])
def get_dlq_messages(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    """
    Return paginated DLQ entries for dashboard inspection.

    Input  ← logflow.dlq_log (PostgreSQL)
    Output → dashboard/src/components/DLQViewer.jsx

    Returns the total count of DLQ messages and a paginated list of entries
    sorted by most recent first.
    """
    with get_session() as session:

        # --- Get total count for pagination ---
        count_row = session.execute(
            text("SELECT COUNT(*) AS cnt FROM dlq_log")
        ).fetchone()
        total = count_row.cnt if count_row else 0

        # --- Fetch paginated results ---
        rows = session.execute(
            text("""
                SELECT id, failed_at, failure_reason,
                       retry_count, original_message
                FROM dlq_log
                ORDER BY failed_at DESC
                LIMIT :limit OFFSET :offset
            """),
            {"limit": limit, "offset": offset},
        ).fetchall()

        messages = [
            {
                "id": row.id,
                "failed_at": row.failed_at.isoformat(),
                "failure_reason": row.failure_reason,
                "retry_count": row.retry_count,
                "original_message": row.original_message,
            }
            for row in rows
        ]

    return {"total": total, "messages": messages}


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
