import React from 'react';
import { useMetricsPolling } from '../hooks/useMetricsPolling.js';

const metricValue = (metrics, keys, fallback = 0) => {
  for (const key of keys) {
    const value = metrics?.[key];

    if (value !== undefined && value !== null && value !== '') {
      return value;
    }
  }

  return fallback;
};

function LoadingSpinner() {
  return <span aria-label="Loading">Loading…</span>;
}

export default function Overview() {
  const { metrics, loading, error, refetch } = useMetricsPolling();

  const cards = [
    {
      label: 'Log Throughput',
      value: metricValue(metrics, ['logsPerSecond', 'log_throughput', 'throughput']),
      suffix: ' logs/sec',
    },
    {
      label: 'Average Latency',
      value: metricValue(metrics, ['averageLatencyMs', 'avg_latency_ms', 'latency']),
      suffix: ' ms',
    },
    {
      label: 'Consumer Lag',
      value: metricValue(metrics, ['consumerLag', 'consumer_lag', 'lag']),
      suffix: '',
    },
    {
      label: 'Active Worker Nodes',
      value: metricValue(metrics, ['activeWorkerNodes', 'active_workers', 'workerNodes']),
      suffix: '',
    },
  ];

  return (
    <section aria-labelledby="overview-title">
      <h1 id="overview-title">System Overview</h1>

      {error && (
        <div role="alert">
          Unable to load metrics.{' '}
          <button type="button" onClick={refetch}>
            Retry
          </button>
        </div>
      )}

      <div className="metrics-grid">
        {cards.map(({ label, value, suffix }) => (
          <article className="metric-card" key={label}>
            <h2>{label}</h2>
            {loading ? (
              <LoadingSpinner />
            ) : (
              <strong>
                {value}
                {suffix}
              </strong>
            )}
          </article>
        ))}
      </div>
    </section>
  );
}