/**
 * dashboard/src/api/client.js — Person 4 (Dashboard Layer)
 * ==========================================================
 *
 * Role
 * ----
 * Centralised fetch wrappers that call Person 3's FastAPI endpoints.
 * All dashboard components import from this module — never fetch directly —
 * so the base URL and error handling logic live in one place.
 *
 * Base URL
 * --------
 * Reads VITE_API_BASE_URL from the Vite environment (set in .env or .env.local).
 * Falls back to http://localhost:8000 for local development.
 * In the Vite dev server, /api/* requests are proxied (see vite.config.js),
 * so the base URL is not needed in the paths — just use /api/... directly.
 *
 * Upstream Contract (FastAPI — processing/api/main.py)
 * -----------------------------------------------------
 *   GET /metrics/throughput  → { windows: [ { window_start, service, messages_per_sec } ] }
 *   GET /metrics/lag         → { partitions: [ { partition_id, lag, recorded_at } ] }
 *   GET /metrics/errors      → { windows: [ { window_start, service, error_rate_pct } ] }
 *   GET /dlq/messages        → { total: N, messages: [ { id, failed_at, failure_reason,
 *                                                         retry_count, original_message } ] }
 *
 * Output (consumed by React components)
 * --------------------------------------
 *   getThroughput(options)  → Promise<ThroughputResponse>
 *   getConsumerLag(options) → Promise<LagResponse>
 *   getErrorRates(options)  → Promise<ErrorRateResponse>
 *   getDLQMessages(options) → Promise<DLQResponse>
 *
 * Error Handling
 * --------------
 * All functions throw an Error with a human-readable message on non-2xx responses
 * or network failures. Components should wrap calls in try/catch or use a hook.
 */

const API_BASE = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';

/**
 * Internal helper — wraps fetch with JSON parsing and error handling.
 *
 * @param {string} path    - Absolute path, e.g. '/metrics/throughput'
 * @param {object} params  - Query string params as key/value pairs (optional)
 * @returns {Promise<any>} - Parsed JSON response body
 * @throws {Error}         - On HTTP error or network failure
 */
async function apiFetch(path, params = {}) {
  const url = new URL(path, API_BASE);
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null) url.searchParams.set(k, v);
  });

  const response = await fetch(url.toString(), {
    headers: { Accept: 'application/json' },
  });

  if (!response.ok) {
    const body = await response.text().catch(() => '');
    throw new Error(
      `API error ${response.status} on ${path}: ${body || response.statusText}`
    );
  }

  return response.json();
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Fetch per-service throughput metrics.
 *
 * @param {{ minutes?: number, service?: string }} options
 * @returns {Promise<{ windows: Array<{ window_start: string, service: string, messages_per_sec: number }> }>}
 *
 * Consumed by: dashboard/src/components/ThroughputChart.jsx
 */
export async function getThroughput({ minutes = 60, service = undefined } = {}) {
  return apiFetch('/metrics/throughput', { minutes, service });
}

/**
 * Fetch Kafka consumer lag per partition.
 *
 * @param {{ partition?: number }} options
 * @returns {Promise<{ partitions: Array<{ partition_id: number, lag: number, recorded_at: string }> }>}
 *
 * Consumed by: dashboard/src/components/ConsumerLagPanel.jsx
 */
export async function getConsumerLag({ partition = undefined } = {}) {
  return apiFetch('/metrics/lag', { partition });
}

/**
 * Fetch per-service error rate metrics.
 *
 * @param {{ minutes?: number, service?: string }} options
 * @returns {Promise<{ windows: Array<{ window_start: string, service: string, error_rate_pct: number }> }>}
 *
 * Consumed by: dashboard/src/components/ErrorRatePanel.jsx
 */
export async function getErrorRates({ minutes = 60, service = undefined } = {}) {
  return apiFetch('/metrics/errors', { minutes, service });
}

/**
 * Fetch paginated DLQ entries.
 *
 * @param {{ limit?: number, offset?: number }} options
 * @returns {Promise<{ total: number, messages: Array<{ id: number, failed_at: string,
 *           failure_reason: string, retry_count: number, original_message: string }> }>}
 *
 * Consumed by: dashboard/src/components/DLQViewer.jsx
 */
export async function getDLQMessages({ limit = 50, offset = 0 } = {}) {
  return apiFetch('/dlq/messages', { limit, offset });
}

/**
 * Health check — useful for dashboard "connection status" indicator.
 *
 * @returns {Promise<{ status: string }>}
 */
export async function healthCheck() {
  return apiFetch('/health');
}
