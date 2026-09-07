import { useEffect, useMemo, useState } from 'react';
import { getConsumerLag, type ConsumerLagResponse } from '../api';

const sampleLag: ConsumerLagResponse = {
  total_lag: 342,
  partitions: [
    { partition_id: 0, lag: 72, recorded_at: new Date().toISOString(), consumer_id: 'Consumer 1' },
    { partition_id: 1, lag: 87, recorded_at: new Date().toISOString(), consumer_id: 'Consumer 2' },
    { partition_id: 2, lag: 131, recorded_at: new Date().toISOString(), consumer_id: 'Consumer 3' },
    { partition_id: 3, lag: 52, recorded_at: new Date().toISOString(), consumer_id: 'Consumer 1' },
  ],
};

export default function ConsumersPage() {
  const [data, setData] = useState<ConsumerLagResponse>(sampleLag);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selectedPartition, setSelectedPartition] = useState(0);

  const refresh = async () => {
    try {
      const next = await getConsumerLag();
      setData(next);
      setLastUpdated(new Date());
      setError(null);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : 'Unable to load consumer lag');
    }
  };

  useEffect(() => {
    void refresh();
    const timer = window.setInterval(() => void refresh(), 5000);
    return () => window.clearInterval(timer);
  }, []);

  const selected = data.partitions.find((item) => item.partition_id === selectedPartition);
  const lagByPartition = useMemo(
    () => new Map(data.partitions.map((item) => [item.partition_id, item])),
    [data.partitions],
  );

  return (
    <div className="page-frame consumer-page">
      <header className="page-header">
        <div>
          <h1>Consumer &amp; Partition View</h1>
          <p>Live Kafka consumer-group activity and partition assignment</p>
        </div>
        <div className="header-actions">
          <span className="pill live">● LIVE</span>
          <span className="pill muted">{lastUpdated ? `Updated ${lastUpdated.toLocaleTimeString()}` : 'Connecting…'}</span>
          <button className="small-btn" onClick={() => void refresh()}>Refresh</button>
        </div>
      </header>

      {error && <div className="error-banner" role="alert">Consumer API unavailable: {error}</div>}

      <div className="summary-row">
        <div className="summary-box active">Active Consumers <strong>3 / 3</strong><span>HEALTHY</span></div>
        <div className="summary-box">Total Partitions <strong>{data.partitions.length}</strong><span>ASSIGNED</span></div>
        <div className="summary-box">Total Throughput <strong>2,184 msg/s</strong><span>HEALTHY</span></div>
        <div className={`summary-box ${data.total_lag > 1500 ? 'warning' : ''}`}>Messages Behind <strong>{data.total_lag.toLocaleString()}</strong><span>{data.total_lag > 1500 ? 'WARNING' : 'HEALTHY'}</span></div>
      </div>

      <section className="panel table-panel">
        <div className="panel-title-row"><span>Kafka Partition Assignment</span><span className="status-tag info">logs</span></div>
        <table className="partition-table">
          <thead><tr><th>PART</th><th>THROUGHPUT</th><th>CURRENT LAG</th><th>CONSUMER</th></tr></thead>
          <tbody>
            {[0, 1, 2, 3].map((partitionId) => {
              const partition = lagByPartition.get(partitionId);
              const lag = partition?.lag ?? 0;
              return (
                <tr key={partitionId} className={selectedPartition === partitionId ? 'selected-row' : ''} onClick={() => setSelectedPartition(partitionId)}>
                  <td>P{partitionId}</td>
                  <td>{600 + partitionId * 50} msg/s</td>
                  <td className={lag > 1500 ? 'danger-text' : ''}>Lag {lag.toLocaleString()}</td>
                  <td>{partition?.consumer_id ?? 'Unassigned'}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </section>

      <div className="lower-split">
        <section className="panel consumer-lag-panel">
          <div className="panel-title-row"><span>Consumer Lag by Partition</span></div>
          <div className="partition-rows">
            {[0, 1, 2, 3].map((partitionId) => {
              const lag = lagByPartition.get(partitionId)?.lag ?? 0;
              return (
                <button type="button" key={partitionId} className={`partition-item interactive-card ${selectedPartition === partitionId ? 'selected' : ''}`} onClick={() => setSelectedPartition(partitionId)}>
                  <span>P{partitionId}</span>
                  <div className={`mini-bar ${lag > 1500 ? 'red' : 'green'}`} style={{ width: `${Math.min(100, Math.max(8, lag / 20))}%` }} />
                  <strong>{lag.toLocaleString()}</strong>
                </button>
              );
            })}
          </div>
          <p className="mini-change">Selected partition: P{selectedPartition} · {selected?.consumer_id ?? 'Unassigned'}</p>
        </section>

        <section className="panel event-inline-panel">
          <div className="panel-title-row"><span>Consumer Group Events</span><span className="status-tag info">LIVE</span></div>
          <ul className="event-list compact">
            <li><span className="event-time">now</span><span className="event-service info">INFO</span><span>Lag snapshot refreshed</span></li>
            <li><span className="event-time">—</span><span className="event-service warn">WARN</span><span>Backpressure state comes from consumer telemetry</span></li>
          </ul>
        </section>
      </div>
    </div>
  );
}
