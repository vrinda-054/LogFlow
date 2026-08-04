/**
 * dashboard/src/components/ConsumerLagPanel.jsx — Person 4 (Dashboard Layer)
 * =============================================================================
 *
 * Role
 * ----
 * Displays Kafka consumer lag per partition as a bar chart or numeric display.
 * High lag values should be visually highlighted to indicate backpressure events.
 *
 * The backpressure.py module (consumers/) governs pause/resume of partitions
 * when lag exceeds thresholds. This panel makes that state visible.
 *
 * Input (Props)
 * -------------
 *   data : object | null
 *     Shape: { partitions: Array<{ partition_id: number, lag: number, recorded_at: string }> }
 *     Source: dashboard/src/api/client.js → getConsumerLag()
 *     ← FastAPI GET /metrics/lag
 *     ← PostgreSQL logflow.metrics_consumer_lag
 *
 * Output
 * ------
 *   Renders a 4-bar chart (one bar per partition, partitions 0–3).
 *   Bars coloured:
 *     green  — lag < 500
 *     yellow — lag 500–2000
 *     red    — lag > 2000 (backpressure likely triggered)
 *   Shows recorded_at timestamp for each partition.
 *
 * TODO (Person 4)
 * ---------------
 *   1. Render using Recharts <BarChart>.
 *   2. Apply conditional fill colour based on lag thresholds.
 *   3. Add a "PAUSED" badge on partitions where backpressure is active.
 *   4. Handle data=null with a skeleton/loading state.
 */

export default function ConsumerLagPanel({ data }) {
  if (!data) {
    return (
      <div className="panel-stub">
        <h2>Consumer Lag (per partition)</h2>
        <p className="stub-label">⏳ Waiting for data…</p>
        <p className="stub-hint">
          Stub — implement using Recharts &lt;BarChart&gt; with 4 bars.<br />
          Data shape: <code>{'{ partitions: [{ partition_id, lag, recorded_at }] }'}</code>
        </p>
      </div>
    );
  }

  // TODO: implement bar chart render
  return (
    <div className="panel-stub">
      <h2>Consumer Lag (per partition)</h2>
      <pre>{JSON.stringify(data, null, 2)}</pre>
    </div>
  );
}
