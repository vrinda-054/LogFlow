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
import json
import subprocess
import sys
from pathlib import Path
from datetime import datetime, timezone

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

try:
    from processing.db.connection import get_session, get_engine
except ImportError:  # pragma: no cover - fallback for direct script execution
    from db.connection import get_session, get_engine

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

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
    "http://localhost:5174",
    os.environ.get("VITE_API_BASE_URL", "http://localhost:8000"),
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ═══════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════


_scenario_processes: dict[str, subprocess.Popen] = {}
_scenario_states: dict[str, dict[str, object]] = {}
_scenario_history: list[dict[str, object]] = []

# In-memory consumer event history. These records are derived from detected
# telemetry transitions in the current FastAPI process and are intentionally
# not persisted to PostgreSQL or generated from client-side state.
_consumer_events: list[dict[str, object]] = []
_previous_consumer_state: dict[str, dict[str, object]] = {}

_scenario_keys = {
    "normal-load",
    "traffic-spike",
    "malformed",
    "slow-consumer",
    "worker-failure",
}
_producer_scenarios = {
    "normal-load": ["--rate", "10", "--duration", "120", "--scenario", "normal"],
    "traffic-spike": ["--rate", "10", "--duration", "300", "--scenario", "spike"],
    "malformed": [
        "--rate", "10", "--duration", "120", "--malformed-pct", "30",
        "--scenario", "malformed",
    ],
}


def _docker_run(args: list[str], *, check: bool = True) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(
            ["docker", *args],
            cwd=PROJECT_ROOT,
            check=check,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=503,
            detail="Docker scenario control is unavailable in the processing service.",
        ) from exc
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or exc.stdout or "Docker command failed").strip()
        raise HTTPException(status_code=503, detail=detail) from exc


def _docker_container_running(container_name: str) -> bool:
    result = _docker_run(
        ["inspect", "--format", "{{.State.Running}}", container_name],
        check=False,
    )
    return result.returncode == 0 and result.stdout.strip() == "true"


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _record_consumer_event(
    severity: str,
    component: str,
    message: str,
) -> None:
    """Persist a detected consumer state transition in memory."""
    _consumer_events.append({
        "timestamp": _timestamp(),
        "severity": severity.upper(),
        "component": component,
        "message": message,
    })
    if len(_consumer_events) > 100:
        _consumer_events[:] = _consumer_events[-100:]


def _assignment_label(partitions: list[int]) -> str:
    return ",".join(f"P{partition}" for partition in sorted(set(partitions))) or "None"


def _normalize_assignments(values: object) -> list[int]:
    if not isinstance(values, (list, tuple, set)):
        return []
    assignments: list[int] = []
    for value in values:
        try:
            assignments.append(int(value))
        except (TypeError, ValueError):
            continue
    return sorted(set(assignments))


def _consumer_label(consumer_id: str) -> str:
    if consumer_id.startswith("consumer-"):
        suffix = consumer_id.split("-", 1)[1]
        return f"Consumer {suffix}"
    return consumer_id


def _handle_consumer_state_changes(
    consumer_snapshot: dict[str, object],
    rebalance_state: str,
) -> None:
    """Record transitions only when consumer telemetry materially changes."""
    if not consumer_snapshot:
        return

    consumer_id = str(consumer_snapshot.get("consumer_id") or "consumer-group")
    state_key = consumer_id if consumer_id.startswith("consumer-") else "consumer-group"
    current_state = {
        "status": consumer_snapshot.get("status"),
        "backpressure_active": bool(consumer_snapshot.get("backpressure_active")),
        "assigned_partitions": _normalize_assignments(consumer_snapshot.get("assigned_partitions")),
        "heartbeat_available": bool(consumer_snapshot.get("last_heartbeat")),
    }

    previous_state = _previous_consumer_state.get(state_key)
    if previous_state is None:
        _previous_consumer_state[state_key] = current_state
        return

    label = _consumer_label(consumer_id)

    previous_status = previous_state.get("status")
    current_status = current_state["status"]
    if previous_status != current_status and current_status not in (None, "UNKNOWN"):
        _record_consumer_event(
            "WARN",
            consumer_id,
            f"{label} status changed from {previous_status or 'UNKNOWN'} to {current_status}",
        )

    previous_backpressure = bool(previous_state.get("backpressure_active"))
    current_backpressure = bool(current_state["backpressure_active"])
    if previous_backpressure != current_backpressure:
        if current_backpressure:
            _record_consumer_event(
                "WARN",
                consumer_id,
                f"{label} backpressure activated",
            )
        else:
            _record_consumer_event(
                "INFO",
                consumer_id,
                f"{label} backpressure cleared",
            )

    previous_assignments = previous_state.get("assigned_partitions", [])
    current_assignments = current_state["assigned_partitions"]
    if previous_assignments != current_assignments:
        _record_consumer_event(
            "INFO",
            consumer_id,
            f"{label} assignment changed: {_assignment_label(previous_assignments)} -> {_assignment_label(current_assignments)}",
        )

    previous_heartbeat = bool(previous_state.get("heartbeat_available"))
    current_heartbeat = bool(current_state["heartbeat_available"])
    if previous_heartbeat != current_heartbeat:
        if current_heartbeat:
            _record_consumer_event(
                "INFO",
                consumer_id,
                f"{label} heartbeat/status updated",
            )
        else:
            _record_consumer_event(
                "WARN",
                consumer_id,
                f"{label} heartbeat unavailable",
            )

    _previous_consumer_state[state_key] = current_state


def _new_scenario_state(scenario: str) -> dict[str, object]:
    return {
        "scenario": scenario,
        "status": "ready",
        "message": "Ready to run",
        "started_at": None,
        "finished_at": None,
        "error": None,
    }


def _state_for(scenario: str) -> dict[str, object]:
    return _scenario_states.setdefault(scenario, _new_scenario_state(scenario))


def _finish_scenario(
    scenario: str,
    status: str,
    message: str,
    error: str | None = None,
) -> None:
    state = _state_for(scenario)
    state.update({
        "status": status,
        "message": message,
        "finished_at": _timestamp(),
        "error": error,
    })
    if status in {"passed", "failed", "stopped"}:
        _scenario_history.append(dict(state))


def _refresh_producer_state(scenario: str) -> dict[str, object]:
    process = _scenario_processes.get(scenario)
    state = _state_for(scenario)
    if process is None or state["status"] != "running":
        return state

    return_code = process.poll()
    if return_code is None:
        return state

    _scenario_processes.pop(scenario, None)
    if return_code == 0:
        _finish_scenario(scenario, "passed", "Scenario process completed successfully")
    else:
        _finish_scenario(
            scenario,
            "failed",
            "Scenario process exited with an error",
            f"Scenario process exited with code {return_code}",
        )
    return _state_for(scenario)


def _start_slow_consumer() -> None:
    replacement_name = "logflow-consumer-3-slow"
    if _docker_container_running(replacement_name):
        return

    container = json.loads(_docker_run(["inspect", "logflow-consumer-3"]).stdout)[0]
    config = container["Config"]
    host_config = container["HostConfig"]
    command = [
        "run", "-d", "--name", replacement_name,
        "--network", host_config["NetworkMode"],
        "-e", "BACKPRESSURE_HIGH_WATER=50",
        "-e", "BACKPRESSURE_LOW_WATER=10",
    ]
    for environment in config.get("Env", []):
        command.extend(["-e", environment])
    for mount in container.get("Mounts", []):
        source = mount.get("Name") or mount.get("Source")
        if source and mount.get("Destination"):
            command.extend(["-v", f"{source}:{mount['Destination']}"])
    command.extend([
        config["Image"],
        "--consumer-id", "consumer-03",
        "--inject-delay-ms", "2000",
    ])

    _docker_run(["stop", "logflow-consumer-3"])
    try:
        _docker_run(command)
    except HTTPException:
        _docker_run(["start", "logflow-consumer-3"], check=False)
        raise


def _start_worker_failure() -> None:
    if _docker_container_running("logflow-consumer-2"):
        _docker_run(["stop", "logflow-consumer-2"])


def _stop_docker_scenario(scenario: str) -> None:
    if scenario == "slow-consumer":
        _docker_run(["rm", "-f", "logflow-consumer-3-slow"], check=False)
        _docker_run(["start", "logflow-consumer-3"], check=False)
    elif scenario == "worker-failure":
        _docker_run(["start", "logflow-consumer-2"], check=False)


@app.post("/scenarios/{scenario}", tags=["scenarios"])
def start_scenario(scenario: str):
    """Start one of the repository's fixed producer scenario presets."""
    if scenario not in _scenario_keys:
        raise HTTPException(status_code=404, detail="Unknown scenario")

    state = _refresh_producer_state(scenario)
    if state["status"] == "running":
        return {"status": "running", "scenario": scenario}

    state = _state_for(scenario)
    state.update({
        "status": "running",
        "message": "Scenario started",
        "started_at": _timestamp(),
        "finished_at": None,
        "error": None,
    })

    if scenario == "slow-consumer":
        try:
            _start_slow_consumer()
        except HTTPException as exc:
            _finish_scenario(scenario, "failed", "Unable to start slow consumer", str(exc.detail))
            raise
        _finish_scenario(scenario, "passed", "Slow consumer replacement started")
        return {"status": "started", "scenario": scenario}

    if scenario == "worker-failure":
        try:
            _start_worker_failure()
        except HTTPException as exc:
            _finish_scenario(scenario, "failed", "Unable to stop consumer-2", str(exc.detail))
            raise
        _finish_scenario(scenario, "passed", "Consumer-2 stopped; rebalancing triggered")
        return {"status": "started", "scenario": scenario}

    producer_path = PROJECT_ROOT / "ingestion" / "producer.py"
    try:
        _scenario_processes[scenario] = subprocess.Popen(
            [sys.executable, str(producer_path), *_producer_scenarios[scenario]],
            cwd=PROJECT_ROOT,
            env=os.environ.copy(),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
    except OSError as exc:
        logger.exception("Unable to start scenario %s", scenario)
        _finish_scenario(scenario, "failed", "Unable to start scenario", str(exc))
        raise HTTPException(status_code=503, detail=f"Unable to start scenario: {exc}") from exc

    return {"status": "started", "scenario": scenario}


@app.get("/scenarios/{scenario}/status", tags=["scenarios"])
def get_scenario_status(scenario: str):
    if scenario not in _scenario_keys:
        raise HTTPException(status_code=404, detail="Unknown scenario")
    return _refresh_producer_state(scenario)


@app.get("/scenarios/history", tags=["scenarios"])
def get_scenario_history():
    return {"history": list(reversed(_scenario_history))}


@app.post("/scenarios/{scenario}/stop", tags=["scenarios"])
def stop_scenario(scenario: str):
    if scenario not in _scenario_keys:
        raise HTTPException(status_code=404, detail="Unknown scenario")

    state = _refresh_producer_state(scenario)
    if state["status"] != "running":
        if scenario in {"slow-consumer", "worker-failure"} and state["status"] == "passed":
            try:
                _stop_docker_scenario(scenario)
            except HTTPException as exc:
                _finish_scenario(scenario, "failed", "Unable to restore Docker scenario", str(exc.detail))
                raise
            _finish_scenario(scenario, "stopped", "Scenario cleanup completed")
            return _state_for(scenario)
        return state

    process = _scenario_processes.pop(scenario, None)
    if process is not None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    elif scenario in {"slow-consumer", "worker-failure"}:
        try:
            _stop_docker_scenario(scenario)
        except HTTPException as exc:
            _finish_scenario(scenario, "failed", "Unable to restore Docker scenario", str(exc.detail))
            raise

    _finish_scenario(scenario, "stopped", "Scenario stopped and cleaned up")
    return _state_for(scenario)


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


def _read_status_file(environment_name: str, default_name: str):
    directory_name = environment_name.replace("_FILE", "_DIR")
    configured_directory = os.environ.get(directory_name)
    if configured_directory:
        path = Path(configured_directory)
    else:
        path = Path(os.environ.get(environment_name, PROJECT_ROOT / default_name))
    if path.is_dir():
        snapshots = []
        for snapshot_path in sorted(path.glob("*.json")):
            try:
                snapshots.append(json.loads(snapshot_path.read_text(encoding="utf-8")))
            except json.JSONDecodeError as exc:
                logger.warning("Ignoring invalid telemetry snapshot %s: %s", snapshot_path, exc)
        if snapshots:
            return snapshots
        raise HTTPException(
            status_code=503,
            detail=f"Consumer telemetry is unavailable: {path} has no snapshots yet",
        )
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Consumer telemetry is unavailable: {path.name} has not been written yet",
        ) from exc
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Consumer telemetry is unavailable: {path.name} is invalid",
        ) from exc


def _normalize_consumer_snapshots(raw: object) -> list[dict[str, object]]:
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    if isinstance(raw, dict):
        return [raw]
    return []


@app.get("/metrics/consumers", tags=["metrics"])
def get_consumer_status():
    """Return the latest Person 2 consumer and partition telemetry snapshots."""
    partitions = _read_status_file("PARTITION_STATUS_FILE", "partition-status.json")
    consumer = _read_status_file("CONSUMER_STATUS_FILE", "consumer-status.json")
    tracked_consumer_ids = {"consumer-01", "consumer-02", "consumer-03"}

    if isinstance(consumer, list):
        consumer_snapshots = [
            item for item in consumer
            if isinstance(item, dict) and item.get("consumer_id") in tracked_consumer_ids
        ]
    elif isinstance(consumer, dict) and consumer.get("consumer_id") in tracked_consumer_ids:
        consumer_snapshots = [consumer]
    else:
        consumer_snapshots = []

    selected_consumer = max(
        consumer_snapshots,
        key=lambda item: item.get("last_heartbeat", ""),
        default={},
    )
    rebalance_state = "BACKPRESSURE" if selected_consumer.get("backpressure_active") else "STABLE"

    for consumer_snapshot in consumer_snapshots:
        _handle_consumer_state_changes(
            consumer_snapshot,
            rebalance_state,
        )

    if isinstance(partitions, list):
        active_assignments = {
            (item.get("consumer_id"), partition)
            for item in consumer_snapshots
            for partition in item.get("assigned_partitions", [])
        }
        partitions = [
            item
            for item in partitions
            if (item.get("assigned_consumer"), item.get("partition")) in active_assignments
        ]

    if isinstance(partitions, dict):
        partitions = list(partitions.values())

    return {
        "consumer": selected_consumer,
        "partitions": partitions,
        "rebalancing": {
            "state": rebalance_state,
            "current_assignment": selected_consumer.get("assigned_partitions", []),
            "after_recovery": "AUTOMATIC_REBALANCE",
        },
    }


@app.get("/consumers/events", tags=["metrics"])
def get_consumer_events(
    limit: int = Query(default=20, ge=1, le=100),
):
    """Return the newest in-memory consumer telemetry events."""
    return {"events": list(reversed(_consumer_events))[:limit]}


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
@app.get("/dlq/activity", tags=["dlq"])
def get_dlq_activity(
    hours: int = Query(default=24, ge=1, le=168),
):
    """
    Return DLQ activity grouped by hour for the dashboard.
    """
    with get_session() as session:
        rows = session.execute(
            text("""
                SELECT
                    date_trunc('hour', failed_at) AS hour,
                    COUNT(*) AS count
                FROM dlq_log
                WHERE failed_at >= NOW() - (:hours * INTERVAL '1 hour')
                GROUP BY date_trunc('hour', failed_at)
                ORDER BY hour ASC
            """),
            {"hours": hours},
        ).fetchall()

    activity = [
        {
            "timestamp": row.hour.isoformat(),
            "count": row.count,
        }
        for row in rows
    ]

    return {"activity": activity}
@app.get("/logs", tags=["logs"])
def get_logs(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    """
    Return recent processed log records for the dashboard.

    Input  ← logflow.processed_logs (PostgreSQL)
    Output → React Live Logs page.
    """
    with get_session() as session:
        rows = session.execute(
            text("""
                SELECT id, ingested_at, log_ts, service,
                       severity, message, trace_id
                FROM processed_logs
                ORDER BY log_ts DESC
                LIMIT :limit OFFSET :offset
            """),
            {"limit": limit, "offset": offset},
        ).fetchall()

        logs = [
            {
                "id": row.id,
                "ingested_at": row.ingested_at.isoformat(),
                "timestamp": row.log_ts.isoformat(),
                "service": row.service,
                "severity": row.severity,
                "message": row.message,
                "trace_id": row.trace_id,
            }
            for row in rows
        ]

    return {"logs": logs}

# ---------------------------------------------------------------------------
# Dev server entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host=os.environ.get("FASTAPI_HOST", "0.0.0.0"),
        port=int(os.environ.get("FASTAPI_PORT", "8000")),
        reload=False,
    )
