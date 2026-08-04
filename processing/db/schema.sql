-- =============================================================================
-- processing/db/schema.sql — Person 3 (Processing Layer)
-- =============================================================================
-- PostgreSQL 15 schema for the LogFlow processing layer.
-- This file is automatically executed by the postgres Docker container on first
-- startup (mounted at /docker-entrypoint-initdb.d/01-schema.sql).
--
-- Tables
-- ------
--   processed_logs        Raw validated log records from the consumer group.
--   metrics_throughput    Per-service throughput (messages/sec) per time window.
--   metrics_error_rate    Per-service error percentage per time window.
--   metrics_consumer_lag  Kafka consumer lag snapshot per partition.
--   dlq_log               Dead Letter Queue events for dashboard inspection.
--
-- Read by (FastAPI endpoints in processing/api/main.py):
--   GET /metrics/throughput   → metrics_throughput
--   GET /metrics/errors       → metrics_error_rate
--   GET /metrics/lag          → metrics_consumer_lag
--   GET /dlq/messages         → dlq_log
-- =============================================================================

-- Use a dedicated schema to isolate LogFlow tables
CREATE SCHEMA IF NOT EXISTS logflow;
SET search_path TO logflow;

-- ---------------------------------------------------------------------------
-- processed_logs
-- Individual log records after validation and schema conformance check.
-- Populated by: processing/aggregator.py (batch insert per window flush)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS processed_logs (
    id          BIGSERIAL PRIMARY KEY,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),  -- when aggregator received it
    log_ts      TIMESTAMPTZ NOT NULL,                -- original log timestamp
    service     VARCHAR(64)  NOT NULL,
    severity    VARCHAR(16)  NOT NULL
                    CHECK (severity IN ('DEBUG','INFO','WARNING','ERROR','CRITICAL')),
    message     TEXT         NOT NULL,
    trace_id    CHAR(32)     NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_processed_logs_service  ON processed_logs (service);
CREATE INDEX IF NOT EXISTS idx_processed_logs_severity ON processed_logs (severity);
CREATE INDEX IF NOT EXISTS idx_processed_logs_log_ts   ON processed_logs (log_ts);

-- ---------------------------------------------------------------------------
-- metrics_throughput
-- Aggregated message count per service per 1-minute tumbling window.
-- Populated by: aggregator.flush_window()
-- Queried by:   GET /metrics/throughput
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metrics_throughput (
    id              BIGSERIAL    PRIMARY KEY,
    window_start    TIMESTAMPTZ  NOT NULL,
    window_end      TIMESTAMPTZ  NOT NULL,
    service         VARCHAR(64)  NOT NULL,
    message_count   BIGINT       NOT NULL DEFAULT 0,
    messages_per_sec NUMERIC(10,2) GENERATED ALWAYS AS (
        CASE WHEN EXTRACT(EPOCH FROM (window_end - window_start)) > 0
             THEN message_count::NUMERIC / EXTRACT(EPOCH FROM (window_end - window_start))
             ELSE 0
        END
    ) STORED
);

CREATE INDEX IF NOT EXISTS idx_throughput_window ON metrics_throughput (window_start DESC);

-- ---------------------------------------------------------------------------
-- metrics_error_rate
-- Error percentage per service per 1-minute window.
-- Populated by: aggregator.flush_window()
-- Queried by:   GET /metrics/errors
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metrics_error_rate (
    id              BIGSERIAL    PRIMARY KEY,
    window_start    TIMESTAMPTZ  NOT NULL,
    window_end      TIMESTAMPTZ  NOT NULL,
    service         VARCHAR(64)  NOT NULL,
    total_messages  BIGINT       NOT NULL DEFAULT 0,
    error_messages  BIGINT       NOT NULL DEFAULT 0,
    error_rate_pct  NUMERIC(5,2) GENERATED ALWAYS AS (
        CASE WHEN total_messages > 0
             THEN (error_messages::NUMERIC / total_messages) * 100
             ELSE 0
        END
    ) STORED
);

CREATE INDEX IF NOT EXISTS idx_error_rate_window ON metrics_error_rate (window_start DESC);

-- ---------------------------------------------------------------------------
-- metrics_consumer_lag
-- Kafka consumer lag snapshots per partition, polled by the aggregator.
-- Populated by: aggregator.update_consumer_lag()
-- Queried by:   GET /metrics/lag
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metrics_consumer_lag (
    id           BIGSERIAL    PRIMARY KEY,
    recorded_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    partition_id INTEGER      NOT NULL,
    lag          BIGINT       NOT NULL,
    consumer_id  VARCHAR(128)          -- optional: which consumer instance reported
);

CREATE INDEX IF NOT EXISTS idx_consumer_lag_partition ON metrics_consumer_lag (partition_id, recorded_at DESC);

-- ---------------------------------------------------------------------------
-- dlq_log
-- Dead Letter Queue events published by consumers/dlq_handler.py.
-- Populated by: aggregator.ingest_dlq_event()
-- Queried by:   GET /dlq/messages
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dlq_log (
    id               BIGSERIAL    PRIMARY KEY,
    failed_at        TIMESTAMPTZ  NOT NULL,
    original_message TEXT         NOT NULL,  -- raw JSON string or error payload
    failure_reason   TEXT         NOT NULL,
    retry_count      INTEGER      NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_dlq_failed_at ON dlq_log (failed_at DESC);
