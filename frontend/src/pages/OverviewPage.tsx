import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  getConsumerLag,
  getConsumerStatus,
  getDlqMessages,
  getErrorRates,
  getThroughput,
  type ConsumerLagResponse,
  type ConsumerStatusResponse,
  type DlqResponse,
  type ErrorRateResponse,
  type ThroughputResponse,
} from '../api';
import DashboardShell from '../components/DashboardShell';

const initialBaseMetrics = [
  { label: 'System', value: '5/5', status: 'Healthy' },
  { label: 'AI pipeline', value: 'Normal', status: 'Healthy' },
  { label: 'Consumer', value: '3 / 3', status: 'Active' },
  { label: 'Kafka', value: 'Online', status: 'Healthy' },
  { label: 'API', value: 'Online', status: 'Healthy' },
  { label: 'Database', value: 'Online', status: 'Healthy' },
];

const defaultConsumerCards = [
  { id: 'C1', status: 'RUNNING', rate: '742/s', lag: '124' },
  { id: 'C2', status: 'RUNNING', rate: '811/s', lag: '87' },
  { id: 'C3', status: 'RUNNING', rate: '631/s', lag: '131' },
];

const baseEvents = [
  { ts: '18:59:42', service: 'INFO', text: 'Consumer 3 heartbeat received' },
  { ts: '18:59:37', service: 'INFO', text: 'Consumer 2 rebalanced automatically' },
  { ts: '18:59:39', service: 'WARN', text: 'Partition 2 lag spike detected' },
  { ts: '18:59:45', service: 'ERROR', text: 'Consumer 1 retry queue exceeded threshold' },
  { ts: '18:59:52', service: 'INFO', text: 'Kafka cluster state stable' },
];

export default function OverviewPage() {
  const navigate = useNavigate();
  const [metrics, setMetrics] = useState(initialBaseMetrics);
  const [throughput, setThroughput] = useState<ThroughputResponse | null>(null);
  const [lagData, setLagData] = useState<ConsumerLagResponse | null>(null);
  const [errorRates, setErrorRates] = useState<ErrorRateResponse | null>(null);
  const [dlqData, setDlqData] = useState<DlqResponse | null>(null);
  const [consumerStatus, setConsumerStatus] = useState<ConsumerStatusResponse | null>(null);
  const [isLive, setIsLive] = useState(false);
  const [lastUpdated, setLastUpdated] = useState('just now');

  const fetchLiveData = async () => {
    const results = await Promise.allSettled([
      getThroughput(60),
      getConsumerLag(),
      getErrorRates(60),
      getDlqMessages(10),
      getConsumerStatus(),
    ]);

    const [tpRes, lagRes, errRes, dlqRes, consRes] = results;
    let anySuccess = false;

    if (tpRes.status === 'fulfilled') { setThroughput(tpRes.value); anySuccess = true; }
    if (lagRes.status === 'fulfilled') { setLagData(lagRes.value); anySuccess = true; }
    if (errRes.status === 'fulfilled') { setErrorRates(errRes.value); anySuccess = true; }
    if (dlqRes.status === 'fulfilled') { setDlqData(dlqRes.value); anySuccess = true; }
    if (consRes.status === 'fulfilled') { setConsumerStatus(consRes.value); anySuccess = true; }

    setIsLive(anySuccess);
    setLastUpdated('just now');
  };

  useEffect(() => {
    void fetchLiveData();
    const timer = setInterval(() => void fetchLiveData(), 5000);
    return () => clearInterval(timer);
  }, []);

  const currentThroughput = throughput?.summary.current_rate ?? 2184;
  const currentTotalLag = lagData?.total_lag ?? 342;
  const currentErrorRate = errorRates?.summary.overall_error_rate_pct ?? 0.42;
  const currentDlqCount = dlqData?.total ?? 127;

  const consumerCards = consumerStatus?.partitions ? [
    {
      id: 'C1',
      status: consumerStatus.consumer.status || 'RUNNING',
      rate: `${(consumerStatus.partitions.filter(p => p.assigned_consumer.includes('1') || p.assigned_consumer.includes('C1')).reduce((a, c) => a + c.throughput, 0) || 742).toFixed(2)} msg/s`,
      lag: (consumerStatus.partitions.filter(p => p.assigned_consumer.includes('1') || p.assigned_consumer.includes('C1')).reduce((a, c) => a + c.current_lag, 0) || 124).toLocaleString(),
    },
    {
      id: 'C2',
      status: 'RUNNING',
      rate: `${(consumerStatus.partitions.filter(p => p.assigned_consumer.includes('2') || p.assigned_consumer.includes('C2')).reduce((a, c) => a + c.throughput, 0) || 811).toFixed(2)} msg/s`,
      lag: (consumerStatus.partitions.filter(p => p.assigned_consumer.includes('2') || p.assigned_consumer.includes('C2')).reduce((a, c) => a + c.current_lag, 0) || 87).toLocaleString(),
    },
    {
      id: 'C3',
      status: 'RUNNING',
      rate: `${(consumerStatus.partitions.filter(p => p.assigned_consumer.includes('3') || p.assigned_consumer.includes('C3')).reduce((a, c) => a + c.throughput, 0) || 631).toFixed(2)} msg/s`,
      lag: (consumerStatus.partitions.filter(p => p.assigned_consumer.includes('3') || p.assigned_consumer.includes('C3')).reduce((a, c) => a + c.current_lag, 0) || 131).toLocaleString(),
    },
  ] : defaultConsumerCards;

  return (
    <DashboardShell>
      <div className="page-frame overview-page">
        <header className="page-header">
          <div>
            <h1>System Overview</h1>
            <p>Realtime health and performance of the LogFlow pipeline</p>
          </div>
          <div className="header-actions">
            <span className={`pill ${isLive ? 'live' : 'muted'}`}>{isLive ? '● LIVE' : '○ DEMO'}</span>
            <span className="pill muted">Updated {lastUpdated}</span>
            <button className="small-btn" onClick={() => void fetchLiveData()}>Refresh</button>
          </div>
        </header>

        <div className="metric-strip top-strip">
          {metrics.map((item) => (
            <div key={item.label} className="mini-metric">
              <span className="metric-label">{item.label}</span>
              <strong>{item.value}</strong>
              <span className="metric-status">{item.status}</span>
            </div>
          ))}
        </div>

        <div className="dashboard-grid overview-grid">
          <section className="panel chart-panel">
            <div className="panel-title-row"><span>THROUGHPUT</span><span className="status-tag healthy">● HEALTHY</span></div>
            <div className="big-number">{currentThroughput.toLocaleString()} <span>msg/s</span></div>
            <div className="mini-change">Avg: {throughput?.summary.average_rate ?? 2.21} msg/s</div>
            <div className="sparkline spark-green" />
            <div className="axis">Peak: {throughput?.summary.peak_rate ?? 4.42} msg/s</div>
          </section>

          <section className="panel">
            <div className="panel-title-row"><span>CONSUMER LAG</span><span className="status-tag healthy">● HEALTHY</span></div>
            <div className="bars-stack">
              {lagData?.partitions ? lagData.partitions.map((p) => (
                <div key={p.partition_id} className="bar-row">
                  <span>P{p.partition_id}</span>
                  <div className="bar"><i style={{ width: `${Math.min(100, Math.max(10, p.lag / 20))}%` }} /></div>
                  <span>{p.lag}</span>
                </div>
              )) : (
                <>
                  <div className="bar-row"><span>P1</span><div className="bar"><i style={{ width: '88%' }} /></div><span>91</span></div>
                  <div className="bar-row"><span>P2</span><div className="bar"><i style={{ width: '75%' }} /></div><span>87</span></div>
                  <div className="bar-row"><span>P3</span><div className="bar"><i style={{ width: '60%' }} /></div><span>124</span></div>
                </>
              )}
            </div>
          </section>

          <section className="panel chart-panel">
            <div className="panel-title-row"><span>ERROR RATE</span><span className="status-tag healthy">● HEALTHY</span></div>
            <div className="big-number small">{currentErrorRate}%</div>
            <div className="tiny-legend"><span>API</span><span>Payment</span><span>Auth</span><span>Database</span></div>
            <div className="donut-wrap"><div className="donut" /></div>
          </section>

          <section className="panel">
            <div className="panel-title-row"><span>DEAD LETTER QUEUE</span><span className={currentDlqCount > 0 ? 'status-tag danger' : 'status-tag healthy'}>● ATTENTION</span></div>
            <div className="big-number small red">{currentDlqCount}</div>
            <div className="queue-bars"><span style={{ width: '88%' }} /><span style={{ width: '72%' }} /><span style={{ width: '65%' }} /></div>
            <div className="inline-action-row"><button className="ghost-btn" onClick={() => navigate('/dlq')}>View DLQ</button></div>
          </section>

          <section className="panel wide-panel">
            <div className="panel-title-row"><span>CONSUMER HEALTH</span></div>
            <div className="three-cards">
              {consumerCards.map((item) => (
                <button type="button" key={item.id} className="health-card clickable-card" onClick={() => navigate('/consumers')}>
                  <div className="health-header"><span className="dot green" /> {item.id} <span className="status-tag running">{item.status}</span></div>
                  <div className="health-metrics"><div><strong>{item.rate}</strong><span>Processing rate</span></div><div><strong>{item.lag}</strong><span>Lag</span></div></div>
                </button>
              ))}
            </div>
          </section>

          <section className="panel recent-panel">
            <div className="panel-title-row"><span>RECENT EVENTS</span><span className="status-tag info">LIVE</span></div>
            <ul className="event-list">
              {baseEvents.map((row) => (
                <li key={`${row.ts}-${row.text}`}><span className="event-time">{row.ts}</span><span className={`event-service ${row.service.toLowerCase()}`}>{row.service}</span><span>{row.text}</span></li>
              ))}
            </ul>
          </section>
        </div>

        <div className="bottom-bar">
          {['LOG GENERATOR', 'CONSUMERS', 'PROCESSING', 'POSTGRES'].map((label, idx) => (
            <div key={label} className="pipeline-card"><span>{label}</span><strong>{idx === 0 || idx === 1 ? `${currentThroughput}/s` : `${Math.round(currentThroughput * 0.95)}/s`}</strong><small>ONLINE</small></div>
          ))}
        </div>
      </div>
    </DashboardShell>
  );
}

