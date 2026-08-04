/**
 * dashboard/src/components/ThroughputChart.jsx — Person 4 (Dashboard Layer)
 * ============================================================================
 *
 * Role
 * ----
 * Renders a real-time line chart showing messages-per-second throughput,
 * broken down by service, over a rolling time window.
 *
 * Input (Props)
 * -------------
 *   data : object | null
 *     Shape: { windows: Array<{ window_start: string, service: string, messages_per_sec: number }> }
 *     Source: dashboard/src/api/client.js → getThroughput()
 *     ← FastAPI GET /metrics/throughput
 *     ← PostgreSQL logflow.metrics_throughput
 *
 * Output
 * ------
 *   Renders a <LineChart> (Recharts) with one line per service.
 *   X-axis: window_start (formatted as HH:mm)
 *   Y-axis: messages_per_sec
 *   Legend: service names
 *
 * TODO (Person 4)
 * ---------------
 *   1. Pivot the flat `windows` array into per-service data series.
 *   2. Render using Recharts <LineChart>, <Line>, <XAxis>, <YAxis>, <Tooltip>.
 *   3. Animate line updates with Recharts isAnimationActive or CSS transitions.
 *   4. Handle data=null with a skeleton/loading state.
 */

export default function ThroughputChart({ data }) {
  if (!data) {
    return (
      <div className="panel-stub">
        <h2>Throughput (msg/sec)</h2>
        <p className="stub-label">⏳ Waiting for data…</p>
        <p className="stub-hint">
          Stub — implement using Recharts &lt;LineChart&gt;.<br />
          Data shape: <code>{'{ windows: [{ window_start, service, messages_per_sec }] }'}</code>
        </p>
      </div>
    );
  }

  // TODO: implement chart render
  return (
    <div className="panel-stub">
      <h2>Throughput (msg/sec)</h2>
      <pre>{JSON.stringify(data, null, 2)}</pre>
    </div>
  );
}
