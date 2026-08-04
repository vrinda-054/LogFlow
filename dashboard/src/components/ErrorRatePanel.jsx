/**
 * dashboard/src/components/ErrorRatePanel.jsx — Person 4 (Dashboard Layer)
 * ===========================================================================
 *
 * Role
 * ----
 * Displays the error rate percentage per service over time as an area chart
 * or grouped bar chart. Useful for detecting anomalous spikes in ERROR/CRITICAL
 * log events.
 *
 * Input (Props)
 * -------------
 *   data : object | null
 *     Shape: { windows: Array<{ window_start: string, service: string, error_rate_pct: number }> }
 *     Source: dashboard/src/api/client.js → getErrorRates()
 *     ← FastAPI GET /metrics/errors
 *     ← PostgreSQL logflow.metrics_error_rate
 *
 * Output
 * ------
 *   Renders a time-series chart (Recharts <AreaChart> or <LineChart>).
 *   X-axis: window_start (formatted as HH:mm)
 *   Y-axis: error_rate_pct (0–100%)
 *   One series per service; filled area below line for emphasis.
 *   Threshold marker line at 10% to indicate SLO boundary.
 *
 * TODO (Person 4)
 * ---------------
 *   1. Pivot flat `windows` array into per-service series.
 *   2. Render using Recharts <AreaChart>.
 *   3. Add a horizontal ReferenceLine at 10% for the SLO threshold.
 *   4. Handle data=null with a skeleton/loading state.
 */

export default function ErrorRatePanel({ data }) {
  if (!data) {
    return (
      <div className="panel-stub">
        <h2>Error Rate (%)</h2>
        <p className="stub-label">⏳ Waiting for data…</p>
        <p className="stub-hint">
          Stub — implement using Recharts &lt;AreaChart&gt; with SLO reference line.<br />
          Data shape: <code>{'{ windows: [{ window_start, service, error_rate_pct }] }'}</code>
        </p>
      </div>
    );
  }

  // TODO: implement area chart render
  return (
    <div className="panel-stub">
      <h2>Error Rate (%)</h2>
      <pre>{JSON.stringify(data, null, 2)}</pre>
    </div>
  );
}
