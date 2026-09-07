import { useEffect, useMemo, useState } from 'react';
import { getConsumerLag, getConsumerStatus, type ConsumerLagResponse, type ConsumerStatusResponse } from '../api';
import DashboardShell from '../components/DashboardShell';

type DemoConsumer = {
  id: string;
  status: 'RUNNING' | 'PAUSED';
  rate: number;
  lag: number;
  heartbeat: string;
  partitions: number[];
  backpressure?: string;
};

const sampleLag: ConsumerLagResponse = {
  total_lag: 342,
  partitions: [
    { partition_id: 0, lag: 72, recorded_at: new Date().toISOString(), consumer_id: 'Consumer 1' },
    { partition_id: 1, lag: 87, recorded_at: new Date().toISOString(), consumer_id: 'Consumer 2' },
    { partition_id: 2, lag: 131, recorded_at: new Date().toISOString(), consumer_id: 'Consumer 3' },
    { partition_id: 3, lag: 52, recorded_at: new Date().toISOString(), consumer_id: 'Consumer 1' },
  ],
};

const demoStatus: ConsumerStatusResponse = {
  consumer: {
    consumer_id: 'consumer-01',
    status: 'RUNNING',
    assigned_partitions: [0, 3],
    processing_rate: 742,
    consumer_lag: 124,
    last_heartbeat: new Date().toISOString(),
    backpressure_active: false,
  },
  partitions: [
    { partition: 0, throughput: 612, current_lag: 72, assigned_consumer: 'Consumer 1', health: 'HEALTHY' },
    { partition: 1, throughput: 811, current_lag: 87, assigned_consumer: 'Consumer 2', health: 'HEALTHY' },
    { partition: 2, throughput: 421, current_lag: 1842, assigned_consumer: 'Consumer 3', health: 'CRITICAL' },
    { partition: 3, throughput: 340, current_lag: 52, assigned_consumer: 'Consumer 1', health: 'HEALTHY' },
  ],
  rebalancing: { state: 'STABLE', current_assignment: [0, 1, 2, 3], after_recovery: 'STABLE' },
};

const demoConsumers: DemoConsumer[] = [
  { id: 'Consumer 1', status: 'RUNNING', rate: 742, lag: 124, heartbeat: '2s ago', partitions: [0, 3] },
  { id: 'Consumer 2', status: 'RUNNING', rate: 811, lag: 87, heartbeat: '1s ago', partitions: [1] },
  { id: 'Consumer 3', status: 'PAUSED', rate: 421, lag: 1842, heartbeat: '2s ago', partitions: [2], backpressure: 'Consumption paused — waiting for backlog to fall below threshold' },
];

const demoEvents = [
  ['15:45:31', 'WARN', 'Consumer 3 paused — lag exceeded high-water threshold'],
  ['15:45:28', 'INFO', 'Consumer 1 heartbeat received'],
  ['15:45:24', 'INFO', 'Partition P3 assigned to Consumer 1'],
  ['15:45:20', 'INFO', 'Consumer group stable'],
  ['15:45:14', 'INFO', 'Consumer 2 heartbeat received'],
  ['15:45:11', 'INFO', 'Consumer group coordinator elected'],
];

function toConsumers(status: ConsumerStatusResponse): DemoConsumer[] {
  const known = new Map<string, DemoConsumer>();
  status.partitions.forEach((partition) => {
    const id = partition.assigned_consumer;
    const existing = known.get(id) ?? {
      id,
      status: 'RUNNING',
      rate: 0,
      lag: 0,
      heartbeat: 'just now',
      partitions: [],
    };
    existing.rate += partition.throughput;
    existing.lag += partition.current_lag;
    existing.partitions.push(partition.partition);
    if (partition.health === 'UNHEALTHY') existing.status = 'PAUSED';
    known.set(id, existing);
  });
  return Array.from(known.values());
}

export default function ConsumersPage() {
  const [data, setData] = useState(sampleLag);
  const [status, setStatus] = useState(demoStatus);
  const [consumers, setConsumers] = useState(demoConsumers);
  const [lastUpdated, setLastUpdated] = useState('2s ago');
  const [isLive, setIsLive] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedPartition, setSelectedPartition] = useState(0);

  const refresh = async () => {
    const [lagResult, statusResult] = await Promise.allSettled([getConsumerLag(), getConsumerStatus()]);
    if (lagResult.status === 'fulfilled') setData(lagResult.value);
    if (statusResult.status === 'fulfilled') {
      setStatus(statusResult.value);
      setConsumers(toConsumers(statusResult.value));
    }
    if (lagResult.status === 'fulfilled' && statusResult.status === 'fulfilled') {
      setIsLive(true);
      setError(null);
      setLastUpdated('just now');
    } else {
      const reason = lagResult.status === 'rejected' ? lagResult.reason : statusResult.status === 'rejected' ? statusResult.reason : undefined;
      setIsLive(false);
      setError(reason instanceof Error ? reason.message : 'Live API unavailable; showing demo data');
    }
  };

  useEffect(() => {
    void refresh();
    const timer = window.setInterval(() => void refresh(), 5000);
    return () => window.clearInterval(timer);
  }, []);

  const partitionMap = useMemo(() => new Map(status.partitions.map((item) => [item.partition, item])), [status.partitions]);
  const selectedLag = data.partitions.find((item) => item.partition_id === selectedPartition)?.lag ?? 0;

  return (
    <DashboardShell>
      <div className="page-frame consumer-page">
        <header className="page-header">
          <div>
            <h1>Consumer &amp; Partition View</h1>
            <p>Live Kafka consumer-group activity and partition assignment</p>
          </div>
          <div className="header-actions">
            <div className="header-meta"><span>Consumer Group</span><strong>logflow-consumer-group</strong></div>
            <div className="header-meta"><span>Status</span><strong className="status-live">● STABLE</strong></div>
            <div className="header-meta"><span>Last updated</span><strong>• {lastUpdated}</strong></div>
          </div>
        </header>

        {error && <div className="error-banner" role="alert">Showing demo data: {error}</div>}

        <div className="summary-row">
          <div className="summary-box active"><span>ACTIVE CONSUMERS</span><strong>3 / 3</strong><b>HEALTHY</b></div>
          <div className="summary-box"><span>TOTAL PARTITIONS</span><strong>4</strong><b className="blue-text">ASSIGNED</b></div>
          <div className="summary-box"><span>TOTAL THROUGHPUT</span><strong>2,184 msg/s</strong><b>+8.4% HEALTHY</b></div>
          <div className="summary-box warning"><span>MESSAGES BEHIND</span><strong>{data.total_lag.toLocaleString()}</strong><b className="orange-text">WARNING</b></div>
        </div>

        <div className="section-heading"><h2>Consumer Instances</h2><span className="updating">● updating live</span></div>
        <div className="three-cards consumer-instance-grid">
          {consumers.map((consumer) => (
            <article key={consumer.id} className={`consumer-card ${consumer.status === 'PAUSED' ? 'paused-card' : ''}`}>
              <div className="health-header">
                <h3>● {consumer.id}</h3>
                <span className={`status-tag ${consumer.status === 'RUNNING' ? 'running' : 'warning-tag'}`}>{consumer.status}</span>
              </div>
              {consumer.backpressure && <div className="backpressure-box"><strong>⚠ BACKPRESSURE ACTIVE</strong><span>{consumer.backpressure}</span></div>}
              <div className="consumer-body">
                <div><label>Consumer ID</label><strong>consumer-{consumer.id.slice(-1).padStart(2, '0')}</strong></div>
                <div><label>Processing rate</label><strong className={consumer.status === 'PAUSED' ? 'orange-text' : ''}>{consumer.rate} msg/s</strong></div>
                <div><label>Consumer lag</label><strong className={consumer.lag > 1500 ? 'danger-text' : ''}>{consumer.lag.toLocaleString()}</strong></div>
                <div><label>Heartbeat</label><strong>{consumer.heartbeat}</strong></div>
              </div>
              <div className="partition-label">PARTITIONS: {consumer.partitions.map((partition) => `P${partition}`).join('  ')}</div>
              {consumer.status === 'PAUSED' && <div className="threshold-row"><span>LOW-WATER: 500</span><span>HIGH-WATER: 1,500</span></div>}
              <div className="partition-assignment-label">PARTITION ASSIGNMENT</div>
              {consumer.partitions.map((partition) => {
                const lag = partitionMap.get(partition)?.current_lag ?? consumer.lag;
                return <div className="assignment-row" key={partition}><span>P{partition}</span><i className={lag > 1500 ? 'critical' : ''} style={{ width: `${Math.min(100, Math.max(8, lag / 20))}%` }} /><b>Lag {lag.toLocaleString()}</b></div>;
              })}
            </article>
          ))}
        </div>

        <section className="panel table-panel">
          <div className="panel-title-row"><div><h2>Kafka Partition Assignment</h2><p>Live partition-to-consumer mapping</p></div><span>TOPIC <b className="topic-tag">logs</b></span></div>
          <table className="partition-table">
            <thead><tr><th>PART.</th><th>THROUGHPUT</th><th>CURRENT LAG</th><th>CONSUMER</th></tr></thead>
            <tbody>{[0, 1, 2, 3].map((partition) => {
              const item = partitionMap.get(partition);
              const lag = item?.current_lag ?? data.partitions.find((p) => p.partition_id === partition)?.lag ?? 0;
              return <tr key={partition} className={selectedPartition === partition ? 'selected-row' : ''} onClick={() => setSelectedPartition(partition)}><td>P{partition}</td><td>{item?.throughput ?? [612, 811, 421, 340][partition]} msg/s</td><td className={lag > 1500 ? 'danger-text' : 'healthy-text'}>Lag {lag.toLocaleString()}</td><td>{item?.assigned_consumer ?? `Consumer ${partition === 2 ? 3 : partition === 1 ? 2 : 1}`}</td></tr>;
            })}</tbody>
          </table>
        </section>

        <div className="lower-split">
          <section className="panel consumer-lag-panel">
            <div className="panel-title-row"><div><h2>Consumer Lag by Partition</h2><p>Messages behind latest offset</p></div><span className="lag-legend">● Healthy　● Warning　● Critical</span></div>
            <div className="partition-rows">{[0, 1, 2, 3].map((partition) => {
              const lag = partitionMap.get(partition)?.current_lag ?? data.partitions.find((p) => p.partition_id === partition)?.lag ?? 0;
              return <button type="button" key={partition} className={`partition-item ${selectedPartition === partition ? 'selected' : ''}`} onClick={() => setSelectedPartition(partition)}><span>P{partition}</span><i className={`lag-track ${lag > 1500 ? 'critical' : ''}`}><b style={{ width: `${Math.min(100, Math.max(5, lag / 20))}%` }} /></i><strong className={lag > 1500 ? 'danger-text' : ''}>{lag}</strong></button>;
            })}</div>
            <div className="high-water-mark">└ High-Water Mark: 1,500</div>
          </section>
          <section className="panel event-inline-panel">
            <div className="panel-title-row"><div><h2>Consumer Group Events</h2><p>Real-time activity log</p></div><span className="status-tag info">● LIVE</span></div>
            <ul className="event-list compact">{demoEvents.map(([time, level, message]) => <li key={`${time}-${message}`}><span className="event-time">{time}</span><span className={`event-service ${level.toLowerCase()}`}>{level === 'WARN' ? '⚠' : '✓'}</span><span className={level === 'WARN' ? 'orange-text' : ''}>{message}</span></li>)}</ul>
          </section>
        </div>

        <section className="rebalancing-panel panel">
          <div className="panel-title-row"><div><h2>Rebalancing State</h2><p>Alternate state — worker failure &amp; partition reassignment</p></div><button className="scenario-button">SCENARIO VIEW</button></div>
          <div className="rebalance-body">
            <div className="rebalance-card"><h3>Consumer 2 <span className="danger-tag">STOPPED</span><span className="warning-tag">REBALANCING</span></h3><strong>Consumer 2 disconnected — Kafka is reassigning partitions...</strong><p>→ P1 reassigning → Consumer 1</p></div>
            <div className="rebalance-card"><h3>After Recovery <span className="status-tag running">STABLE</span></h3><p>Partitions redistributed among remaining consumers</p><p>P0 → <b>Consumer 1</b><br />P1 → <b>Consumer 1</b><br />P2 → <b>Consumer 3</b><br />P3 → <b>Consumer 1</b></p></div>
          </div>
        </section>
      </div>
    </DashboardShell>
  );
}
