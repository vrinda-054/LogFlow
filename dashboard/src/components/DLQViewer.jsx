/**
 * dashboard/src/components/DLQViewer.jsx — Person 4 (Dashboard Layer)
 * ======================================================================
 *
 * Role
 * ----
 * Displays a paginated table of Dead Letter Queue entries so operators can
 * inspect messages that failed processing. Each entry shows the original
 * payload, the failure reason, and the retry count.
 *
 * Input (Props)
 * -------------
 *   data : object | null
 *     Shape: {
 *       total    : number,
 *       messages : Array<{
 *         id               : number,
 *         failed_at        : string,   // ISO 8601
 *         failure_reason   : string,
 *         retry_count      : number,
 *         original_message : string,   // raw JSON string or error payload
 *       }>
 *     }
 *     Source: dashboard/src/api/client.js → getDLQMessages()
 *     ← FastAPI GET /dlq/messages
 *     ← PostgreSQL logflow.dlq_log
 *
 * Output
 * ------
 *   Renders an HTML table with columns:
 *     ID | Failed At | Failure Reason | Retries | Original Message (truncated, expandable)
 *   Pagination controls: Previous / Next page buttons.
 *   Total count shown above the table.
 *   Empty state: "No DLQ entries — pipeline is healthy ✓"
 *
 * TODO (Person 4)
 * ---------------
 *   1. Implement pagination state (page, pageSize) and call getDLQMessages
 *      with matching limit/offset when page changes.
 *   2. Add an expandable row or modal to show the full original_message.
 *   3. Auto-refresh on the same POLL_INTERVAL_MS as App.jsx.
 *   4. Highlight rows with retry_count >= 3 in red.
 */

import { useState } from 'react';

export default function DLQViewer({ data }) {
  const [expandedId, setExpandedId] = useState(null);

  if (!data) {
    return (
      <div className="panel-stub">
        <h2>Dead Letter Queue</h2>
        <p className="stub-label">⏳ Waiting for data…</p>
        <p className="stub-hint">
          Stub — implement a paginated table with expand-on-click rows.<br />
          Data shape:{' '}
          <code>
            {'{ total: N, messages: [{ id, failed_at, failure_reason, retry_count, original_message }] }'}
          </code>
        </p>
      </div>
    );
  }

  if (data.messages.length === 0) {
    return (
      <div className="panel-stub">
        <h2>Dead Letter Queue</h2>
        <p className="healthy-badge">✓ No DLQ entries — pipeline is healthy</p>
      </div>
    );
  }

  // TODO: implement paginated table render
  return (
    <div className="panel-stub">
      <h2>Dead Letter Queue ({data.total} total)</h2>
      <pre>{JSON.stringify(data.messages.slice(0, 3), null, 2)}</pre>
      <p className="stub-hint">Stub — implement full paginated table</p>
    </div>
  );
}
