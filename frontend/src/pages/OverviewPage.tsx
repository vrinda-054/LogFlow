import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import DashboardShell from '../components/DashboardShell';

const initialBaseMetrics = [
  { label: 'System', value: '5/5', status: 'Healthy' },
  { label: 'AI pipeline', value: 'Normal', status: 'Healthy' },
  { label: 'Consumer', value: '3 / 3', status: 'Active' },
  { label: 'Kafka', value: 'Online', status: 'Healthy' },
  { label: 'API', value: 'Online', status: 'Healthy' },
  { label: 'Database', value: 'Online', status: 'Healthy' },
];

const overviewConsumerCards = [
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

  const refreshOverview = () => {
    setMetrics(initialBaseMetrics.map((item, idx) => {
      if (idx === 0) return { ...item, value: `${Math.max(3, Math.min(5, Math.floor(Math.random() * 5) + 1))}/5` };
      if (idx === 2) return { ...item, value: `${Math.floor(Math.random() * 3) + 1} / 3` };
      if (idx === 3) return { ...item, value: Math.random() > 0.7 ? 'Paused' : 'Online' };
      if (idx === 4) return { ...item, value: Math.random() > 0.7 ? 'Degraded' : 'Online' };
      return item;
    }));
  };

  return (
    <DashboardShell>
      <div className="page-frame overview-page">
        <header className="page-header">
          <div>
            <h1>System Overview</h1>
            <p>Realtime health and performance of the LogFlow pipeline</p>
          </div>
          <div className="header-actions">
            <span className="pill live">● LIVE</span>
            <span className="pill muted">Updated 2s ago</span>
            <span className="pill muted">Last 5 minutes</span>
            <button className="small-btn" onClick={refreshOverview}>Refresh</button>
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
            <div className="big-number">2,184 <span>msg/s</span></div>
            <div className="mini-change">+8.6% vs previous period</div>
            <div className="sparkline spark-green" />
            <div className="axis">Peak 4.42s, Avg 2.21s, Log 2.18s</div>
          </section>

          <section className="panel">
            <div className="panel-title-row"><span>CONSUMER LAG</span><span className="status-tag healthy">● HEALTHY</span></div>
            <div className="bars-stack">
              <div className="bar-row"><span>P1</span><div className="bar"><i style={{ width: '88%' }} /></div><span>91</span></div>
              <div className="bar-row"><span>P2</span><div className="bar"><i style={{ width: '75%' }} /></div><span>87</span></div>
              <div className="bar-row"><span>P3</span><div className="bar"><i style={{ width: '60%' }} /></div><span>124</span></div>
            </div>
          </section>

          <section className="panel chart-panel">
            <div className="panel-title-row"><span>ERROR RATE</span><span className="status-tag healthy">● HEALTHY</span></div>
            <div className="big-number small">0.42%</div>
            <div className="tiny-legend"><span>API</span><span>Payment</span><span>Auth</span><span>Database</span></div>
            <div className="donut-wrap"><div className="donut" /></div>
          </section>

          <section className="panel">
            <div className="panel-title-row"><span>DEAD LETTER QUEUE</span><span className="status-tag danger">● ATTENTION</span></div>
            <div className="big-number small red">127</div>
            <div className="queue-bars"><span style={{ width: '88%' }} /><span style={{ width: '72%' }} /><span style={{ width: '65%' }} /></div>
            <div className="inline-action-row"><button className="ghost-btn" onClick={() => navigate('/dlq')}>View DLQ</button></div>
          </section>

          <section className="panel wide-panel">
            <div className="panel-title-row"><span>CONSUMER HEALTH</span></div>
            <div className="three-cards">
              {overviewConsumerCards.map((item) => (
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
            <div key={label} className="pipeline-card"><span>{label}</span><strong>{idx === 0 || idx === 1 ? '2,184/s' : '2,102/s'}</strong><small>ONLINE</small></div>
          ))}
        </div>
      </div>
    </DashboardShell>
  );
}
