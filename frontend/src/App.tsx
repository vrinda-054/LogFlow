import { useEffect, useMemo, useState } from 'react';
import { NavLink, Route, Routes, useNavigate } from 'react-router-dom';

type LogLevel = 'ALL' | 'INFO' | 'WARN' | 'ERROR';
type LogEntry = {
  id: number;
  timestamp: string;
  level: 'INFO' | 'WARN' | 'ERROR';
  service: string;
  consumer: string;
  trace: string;
  message: string;
  payload: string;
};

type DlqEntry = {
  id: string;
  timestamp: string;
  service: string;
  severity: 'ERROR' | 'WARN';
  reason: string;
  trace: string;
  message: string;
  payload: string;
  retryCount: number;
  status: 'PENDING' | 'RETRIED';
};

const navItems = [
  { label: 'Overview', path: '/', icon: '◫' },
  { label: 'Consumers', path: '/consumers', icon: '◍' },
  { label: 'DLQ Inspector', path: '/dlq', icon: '▣' },
  { label: 'Live Logs', path: '/logs', icon: '⎇' },
  { label: 'Test Scenarios', path: '/scenarios', icon: '▤' },
];

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

const consumerPartitionData = [
  { id: 'Consumer 1', status: 'RUNNING', rate: '942 msg/s', lag: '124', health: 'Healthy' },
  { id: 'Consumer 2', status: 'RUNNING', rate: '851 msg/s', lag: '87', health: 'Healthy' },
  { id: 'Consumer 3', status: 'PAUSED', rate: '425 msg/s', lag: '1,842', health: 'Warning' },
];

const baseDlqData: DlqEntry[] = [
  { id: 'DLQ-001217', timestamp: '15:46:23', service: 'Payment', severity: 'ERROR', reason: 'Invalid JSON', trace: 'tr-1a2', message: 'Payment request payload malformed', payload: '{"service":"payment","payload":"{invalid json}"}', retryCount: 3, status: 'PENDING' },
  { id: 'DLQ-001218', timestamp: '15:45:27', service: 'API', severity: 'ERROR', reason: 'Shutdown', trace: 'tr-4c9', message: 'Worker shutdown while reading from queue', payload: '{"service":"api","shutdown":true}', retryCount: 2, status: 'PENDING' },
  { id: 'DLQ-001219', timestamp: '15:45:15', service: 'Authentication', severity: 'WARN', reason: 'Invalid JWT', trace: 'tr-12d', message: 'JWT payload missing required claims', payload: '{"service":"auth","claim":"missing"}', retryCount: 1, status: 'PENDING' },
  { id: 'DLQ-001220', timestamp: '15:45:11', service: 'Database', severity: 'ERROR', reason: 'Timeout', trace: 'tr-3f1', message: 'Write transaction timed out after 15s', payload: '{"service":"database","timeout":15}', retryCount: 4, status: 'RETRIED' },
  { id: 'DLQ-001221', timestamp: '15:44:41', service: 'API', severity: 'ERROR', reason: 'Missing field', trace: 'tr-9a0', message: 'Request missing required field', payload: '{"service":"api","missing":"customer_id"}', retryCount: 2, status: 'PENDING' },
];

const baseLogData: LogEntry[] = [
  { id: 1, timestamp: '18:59:42', level: 'INFO', service: 'API', consumer: 'C1', trace: 'tr-2f5', message: 'Message processed successfully', payload: '{"status":"ok","route":"api"}' },
  { id: 2, timestamp: '18:59:43', level: 'INFO', service: 'API', consumer: 'C2', trace: 'tr-3a8', message: 'Heartbeat received from consumer', payload: '{"type":"heartbeat","consumer":"C2"}' },
  { id: 3, timestamp: '18:59:45', level: 'WARN', service: 'PAYMENT', consumer: 'C3', trace: 'tr-4c0', message: 'Retry queue threshold reached', payload: '{"level":"warn","queue":"retry"}' },
  { id: 4, timestamp: '18:59:46', level: 'ERROR', service: 'PAYMENT', consumer: 'C2', trace: 'tr-2932', message: 'Processing failed, moving to DLQ', payload: '{"level":"error","action":"dlq"}' },
  { id: 5, timestamp: '18:59:48', level: 'INFO', service: 'DATABASE', consumer: 'C3', trace: 'tr-9b7', message: 'Recovered after retry', payload: '{"type":"recovery","service":"database"}' },
  { id: 6, timestamp: '18:59:52', level: 'INFO', service: 'AUTH', consumer: 'C1', trace: 'tr-8d4', message: 'Token validated successfully', payload: '{"auth":"success"}' },
  { id: 7, timestamp: '19:00:01', level: 'WARN', service: 'API', consumer: 'C2', trace: 'tr-6a1', message: 'Rate limit approaching', payload: '{"rate_limit":"warning"}' },
  { id: 8, timestamp: '19:00:05', level: 'ERROR', service: 'PAYMENT', consumer: 'C3', trace: 'tr-11f', message: 'Transaction rejected by payment gateway', payload: '{"payment":"rejected"}' },
];

const scenarioCards = [
  { title: 'Normal Load', tag: 'BASELINE', status: 'READY', description: 'Simulate standard queue traffic', metrics: [{ label: 'Messages', value: '1000' }, { label: 'Throughput', value: '8.2k msg/s' }] },
  { title: 'Traffic Spike', tag: 'LOAD TEST', status: 'READY', description: 'Generate sudden burst traffic', metrics: [{ label: 'Messages', value: '5600' }, { label: 'Throughput', value: '14.2k msg/s' }] },
  { title: 'Slow Consumer', tag: 'BACKPRESSURE', status: 'RUNNING', description: 'Trigger lag growth for a consumer', metrics: [{ label: 'Messages', value: '500' }, { label: 'Lag', value: '1,842' }] },
  { title: 'Work Failure', tag: 'FAILURE TEST', status: 'READY', description: 'Injected faults and retries in flight', metrics: [{ label: 'Messages', value: '2130' }, { label: 'Errors', value: '127' }] },
];

function AppShell({ children }: { children: React.ReactNode }) {
  return (
    <div className="app-layout">
      <aside className="sidebar">
        <div className="brand-block">
          <div className="brand-title">LOGFLOW</div>
          <div className="brand-subtitle">Process. Scale. Recover.</div>
        </div>

        <nav className="nav-menu">
          {navItems.map((item) => (
            <NavLink
              key={item.path}
              to={item.path}
              className={({ isActive }) => `nav-item ${isActive ? 'active' : ''}`}
            >
              <span className="nav-icon">{item.icon}</span>
              <span>{item.label}</span>
            </NavLink>
          ))}
        </nav>

        <div className="system-status">
          <div className="status-header">SYSTEM STATUS</div>
          <div className="status-row"><span className="dot green" /> Kafka <span className="online">ONLINE</span></div>
          <div className="status-row"><span className="dot green" /> API <span className="online">ONLINE</span></div>
          <div className="status-row"><span className="dot green" /> Database <span className="online">ONLINE</span></div>
        </div>
      </aside>

      <main className="main-panel">{children}</main>
    </div>
  );
}

function OverviewPage() {
  const navigate = useNavigate();
  const [metrics, setMetrics] = useState(initialBaseMetrics);

  const refreshOverview = () => {
    const next = [...initialBaseMetrics].map((item, idx) => {
      if (idx === 0) return { ...item, value: `${Math.max(3, Math.min(5, Math.floor(Math.random() * 5) + 1))}/5` };
      if (idx === 2) return { ...item, value: `${Math.floor(Math.random() * 3) + 1} / 3` };
      if (idx === 3) return { ...item, value: Math.random() > 0.7 ? 'Paused' : 'Online' };
      if (idx === 4) return { ...item, value: Math.random() > 0.7 ? 'Degraded' : 'Online' };
      return item;
    });
    setMetrics(next);
  };

  return (
    <AppShell>
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
          {metrics.map((item, idx) => (
            <div key={idx} className="mini-metric">
              <span className="metric-label">{item.label}</span>
              <strong>{item.value}</strong>
              <span className="metric-status">{item.status}</span>
            </div>
          ))}
        </div>

        <div className="dashboard-grid overview-grid">
          <section className="panel chart-panel">
            <div className="panel-title-row">
              <span>THROUGHPUT</span>
              <span className="status-tag healthy">● HEALTHY</span>
            </div>
            <div className="big-number">2,184 <span>msg/s</span></div>
            <div className="mini-change">+8.6% vs previous period</div>
            <div className="sparkline spark-green" />
            <div className="axis">Peak 4.42s, Avg 2.21s, Log 2.18s</div>
          </section>

          <section className="panel">
            <div className="panel-title-row">
              <span>CONSUMER LAG</span>
              <span className="status-tag healthy">● HEALTHY</span>
            </div>
            <div className="bars-stack">
              <div className="bar-row"><span>P1</span><div className="bar"><i style={{ width: '88%' }} /></div><span>91</span></div>
              <div className="bar-row"><span>P2</span><div className="bar"><i style={{ width: '75%' }} /></div><span>87</span></div>
              <div className="bar-row"><span>P3</span><div className="bar"><i style={{ width: '60%' }} /></div><span>124</span></div>
            </div>
          </section>

          <section className="panel chart-panel">
            <div className="panel-title-row">
              <span>ERROR RATE</span>
              <span className="status-tag healthy">● HEALTHY</span>
            </div>
            <div className="big-number small">0.42% </div>
            <div className="tiny-legend">
              <span>API</span><span>Payment</span><span>Auth</span><span>Database</span>
            </div>
            <div className="donut-wrap"><div className="donut" /></div>
          </section>

          <section className="panel">
            <div className="panel-title-row">
              <span>DEAD LETTER QUEUE</span>
              <span className="status-tag danger">● ATTENTION</span>
            </div>
            <div className="big-number small red">127</div>
            <div className="queue-bars">
              <span style={{ width: '88%' }} />
              <span style={{ width: '72%' }} />
              <span style={{ width: '65%' }} />
            </div>
            <div className="inline-action-row">
              <button className="ghost-btn" onClick={() => navigate('/dlq')}>View DLQ</button>
            </div>
          </section>

          <section className="panel wide-panel">
            <div className="panel-title-row"><span>CONSUMER HEALTH</span></div>
            <div className="three-cards">
              {overviewConsumerCards.map((item) => (
                <button type="button" key={item.id} className="health-card clickable-card" onClick={() => navigate('/consumers')}>
                  <div className="health-header"><span className="dot green"/> {item.id} <span className="status-tag running">{item.status}</span></div>
                  <div className="health-metrics">
                    <div><strong>{item.rate}</strong><span>Processing rate</span></div>
                    <div><strong>{item.lag}</strong><span>Lag</span></div>
                  </div>
                </button>
              ))}
            </div>
          </section>

          <section className="panel recent-panel">
            <div className="panel-title-row"><span>RECENT EVENTS</span><span className="status-tag info">LIVE</span></div>
            <ul className="event-list">
              {baseEvents.map((row, idx) => (
                <li key={idx}><span className="event-time">{row.ts}</span><span className={`event-service ${row.service.toLowerCase()}`}>{row.service}</span><span>{row.text}</span></li>
              ))}
            </ul>
          </section>
        </div>

        <div className="bottom-bar">
          {['LOG GENERATOR', 'CONSUMERS', 'PROCESSING', 'POSTGRES'].map((label, idx) => (
            <div key={label} className="pipeline-card">
              <span>{label}</span>
              <strong>{idx === 0 || idx === 1 ? '2,184/s' : '2,102/s'}</strong>
              <small>ONLINE</small>
            </div>
          ))}
        </div>
      </div>
    </AppShell>
  );
}

function ConsumersPage() {
  const navigate = useNavigate();
  const [selectedConsumer, setSelectedConsumer] = useState('Consumer 1');
  const [selectedPartition, setSelectedPartition] = useState('P0');
  const [members, setMembers] = useState(consumerPartitionData);

  const refreshConsumers = () => {
    setMembers((prev) => prev.map((entry, idx) => ({
      ...entry,
      rate: `${(900 + idx * 80 + Math.floor(Math.random() * 70))} msg/s`,
      lag: `${(100 + idx * 40 + Math.floor(Math.random() * 120))}`,
      status: Math.random() > 0.15 ? 'RUNNING' : 'PAUSED',
      health: Math.random() > 0.15 ? 'Healthy' : 'Warning',
    })));
  };

  return (
    <AppShell>
      <div className="page-frame consumer-page">
        <header className="page-header">
          <div>
            <h1>Consumer &amp; Partition View</h1>
            <p>Live Kafka consumer group and partition assignment</p>
          </div>
          <div className="header-actions">
            <span className="pill live">● LIVE</span>
            <span className="pill muted">Last updated 2m ago</span>
            <button className="small-btn" onClick={refreshConsumers}>Refresh</button>
          </div>
        </header>

        <div className="summary-row">
          <div className="summary-box active">Active Consumers <strong>3 / 3</strong><span>HEALTHY</span></div>
          <div className="summary-box">Total Partitions <strong>4</strong><span>ASSIGNED</span></div>
          <div className="summary-box">Total Throughput <strong>2,184</strong><span>HEALTHY</span></div>
          <div className="summary-box warning">Messages Behind <strong>342</strong><span>WARNING</span></div>
        </div>

        <div className="consumer-grid">
          <section className="panel full-panel">
            <div className="panel-title-row"><span>Consumer Instances</span></div>
            <div className="three-cards">
              {members.map((consumer) => {
                const isSelected = selectedConsumer === consumer.id;
                return (
                  <button type="button" key={consumer.id} className={`consumer-card interactive-card ${isSelected ? 'selected' : ''}`} onClick={() => setSelectedConsumer(consumer.id)}>
                    <div className="health-header"><span>{consumer.id}</span><span className="status-tag running">{consumer.status}</span></div>
                    <div className="consumer-body">
                      <div><label>Consumer ID</label><strong>{consumer.id}</strong></div>
                      <div><label>Processing rate</label><strong>{consumer.rate}</strong></div>
                      <div><label>Lag</label><strong>{consumer.lag}</strong></div>
                      <div><label>Health</label><strong>{consumer.health}</strong></div>
                    </div>
                    <div className="mini-assign"><span>Partition assignment</span><div className="mini-meter"><i style={{ width: consumer.lag.includes('1,842') ? '72%' : '60%' }} /></div></div>
                  </button>
                );
              })}
            </div>
          </section>
        </div>

        <section className="panel table-panel">
          <div className="panel-title-row"><span>Kafka Partition Assignment</span></div>
          <table className="partition-table">
            <thead>
              <tr><th>PART</th><th>THROUGHPUT</th><th>CURRENT LAG</th><th>CONSUMER</th></tr>
            </thead>
            <tbody>
              {['P0', 'P1', 'P2', 'P3'].map((part, idx) => {
                const consumerName = idx % 2 === 0 ? 'Consumer 1' : 'Consumer 2';
                const isSelected = selectedPartition === part;
                return (
                  <tr key={part} className={isSelected ? 'selected-row' : ''} onClick={() => setSelectedPartition(part)}>
                    <td>{part}</td>
                    <td>{600 + idx * 50} msg/s</td>
                    <td>Lag {120 + idx * 35}</td>
                    <td>{consumerName}</td>
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
              {['P0', 'P1', 'P2', 'P3'].map((part, idx) => (
                <button type="button" key={part} className={`partition-item interactive-card ${selectedPartition === part ? 'selected' : ''}`} onClick={() => setSelectedPartition(part)}>
                  <span>{part}</span>
                  <div className="mini-bar green" style={{ width: idx === 2 ? '92%' : '72%' }} />
                </button>
              ))}
            </div>
          </section>

          <section className="panel event-inline-panel">
            <div className="panel-title-row"><span>Consumer Group Events</span></div>
            <ul className="event-list compact">
              {baseEvents.map((row, idx) => (
                <li key={idx}><span className="event-time">{row.ts}</span><span className={`event-service ${row.service.toLowerCase()}`}>{row.service}</span><span>{row.text}</span></li>
              ))}
            </ul>
          </section>
        </div>

        <div className="rebalancing-panel panel">
          <div className="panel-title-row"><span>Rebalancing State</span></div>
          <div className="rebalance-body">
            <div className="rebalance-status red">{selectedConsumer} {selectedConsumer === 'Consumer 3' ? 'stopped' : 'healthy'}</div>
            <div className="rebalance-status blue">After Recovery</div>
            <div className="rebalance-legend">
              <button type="button" className="mini-action" onClick={() => navigate('/dlq')}>View DLQ</button>
              <span>Consumer 1</span>
              <span>Consumer 2</span>
              <span>Consumer 3</span>
            </div>
          </div>
        </div>
      </div>
    </AppShell>
  );
}

function DlqPage() {
  const [dlqData, setDlqData] = useState(baseDlqData);
  const [selectedId, setSelectedId] = useState(baseDlqData[0].id);
  const [search, setSearch] = useState('');
  const [severityFilter, setSeverityFilter] = useState<'ALL' | 'ERROR' | 'WARN'>('ALL');
  const [serviceFilter, setServiceFilter] = useState('ALL');

  const visibleRows = useMemo(() => {
    return dlqData.filter((entry) => {
      const matchesSearch = !search || `${entry.id} ${entry.message} ${entry.service} ${entry.trace}`.toLowerCase().includes(search.toLowerCase());
      const matchesSeverity = severityFilter === 'ALL' || entry.severity === severityFilter;
      const matchesService = serviceFilter === 'ALL' || entry.service === serviceFilter;
      return matchesSearch && matchesSeverity && matchesService;
    });
  }, [dlqData, search, severityFilter, serviceFilter]);

  const selectedMessage = visibleRows.find((item) => item.id === selectedId) ?? dlqData[0];

  const refreshDlq = () => {
    const next = [{
      id: `DLQ-${Math.floor(Math.random() * 1000).toString().padStart(6, '0')}`,
      timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }),
      service: 'API',
      severity: 'ERROR',
      reason: 'Malformed JSON',
      trace: `tr-${Math.random().toString(16).slice(2, 7)}`,
      message: 'Request payload could not be parsed',
      payload: '{"service":"api","payload":"malformed"}',
      retryCount: 1,
      status: 'PENDING',
    }, ...dlqData].slice(0, 6);
    setDlqData(next);
    setSelectedId(next[0].id);
  };

  const handleRetry = (entry: DlqEntry) => {
    setDlqData((prev) => prev.map((item) => item.id === entry.id ? { ...item, status: 'RETRIED', retryCount: item.retryCount + 1 } : item));
  };

  const copyPayload = async (payload: string) => {
    try {
      await navigator.clipboard.writeText(payload);
    } catch {
      // ignore clipboard errors in mock mode
    }
  };

  return (
    <AppShell>
      <div className="page-frame dlq-page">
        <header className="page-header">
          <div>
            <h1>Dead Letter Queue Inspector</h1>
            <p>Inspect messages that failed processing and were isolated from the main pipeline</p>
          </div>
          <div className="header-actions">
            <span className="pill live">● LIVE</span>
            <span className="pill muted">Updated 2s ago</span>
            <button className="small-btn" onClick={refreshDlq}>Refresh</button>
          </div>
        </header>

        <div className="dlq-metrics">
          <div className="metric-box"><strong>{dlqData.length}</strong><span>Total Failed Messages</span></div>
          <div className="metric-box"><strong>{dlqData.filter((item) => item.status === 'RETRIED').length}</strong><span>Retried Today</span></div>
          <div className="metric-box accent"><strong>{(dlqData.reduce((sum, item) => sum + item.retryCount, 0) / dlqData.length).toFixed(1)}</strong><span>Average Retry Count</span></div>
          <div className="metric-box alt">
            <strong>{dlqData[0].reason}</strong>
            <span>Top Failure Reason</span>
          </div>
        </div>

        <div className="filter-bar">
          <input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Search message, ID, service" />
          <select value={severityFilter} onChange={(e) => setSeverityFilter(e.target.value as 'ALL' | 'ERROR' | 'WARN')}>
            <option value="ALL">All Failure Types</option>
            <option value="ERROR">ERROR</option>
            <option value="WARN">WARN</option>
          </select>
          <select value={serviceFilter} onChange={(e) => setServiceFilter(e.target.value)}>
            <option value="ALL">All Services</option>
            {Array.from(new Set(dlqData.map((item) => item.service))).map((service) => (
              <option key={service} value={service}>{service}</option>
            ))}
          </select>
          <button className="small-btn" onClick={() => { setSearch(''); setSeverityFilter('ALL'); setServiceFilter('ALL'); }}>Reset</button>
        </div>

        <div className="dlq-content">
          <section className="panel table-panel large-table">
            <div className="panel-title-row"><span>Dead-Lettered Messages</span></div>
            <table className="dlq-table">
              <thead>
                <tr><th>Timestamp</th><th>Service</th><th>Severity</th><th>Reason</th><th>Trace ID</th><th>Status</th><th>Action</th></tr>
              </thead>
              <tbody>
                {visibleRows.map((row) => (
                  <tr key={row.id} className={selectedMessage?.id === row.id ? 'selected-row' : ''} onClick={() => setSelectedId(row.id)}>
                    <td>{row.timestamp}</td>
                    <td>{row.service}</td>
                    <td><span className="cell-badge error">{row.severity}</span></td>
                    <td>{row.reason}</td>
                    <td>{row.trace}</td>
                    <td><span className={`status-pill ${row.status === 'RETRIED' ? 'retried' : 'pending'}`}>{row.status}</span></td>
                    <td>
                      <div className="table-actions">
                        <button className="mini-button" onClick={(e) => { e.stopPropagation(); setSelectedId(row.id); }}>Inspect</button>
                        {row.status === 'PENDING' && (
                          <button className="mini-button alt" onClick={(e) => { e.stopPropagation(); handleRetry(row); }}>Retry</button>
                        )}
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>

          <aside className="side-panel detail-panel">
            <div className="detail-card">
              <div className="detail-header">Message Details</div>
              <div className="detail-key">ID <span>{selectedMessage.id}</span></div>
              <div className="detail-key">Service <span>{selectedMessage.service}</span></div>
              <div className="detail-key">Failure <span>{selectedMessage.reason}</span></div>
              <div className="detail-key">Trace ID <span>{selectedMessage.trace}</span></div>
              <div className="detail-key">Retries <span>{selectedMessage.retryCount}</span></div>
              <div className="preview-box"><code>{selectedMessage.payload}</code></div>
              <div className="detail-actions">
                <button className="mini-button" onClick={() => copyPayload(selectedMessage.payload)}>Copy Payload</button>
                <button className="mini-button alt" onClick={() => handleRetry(selectedMessage)}>Retry</button>
              </div>
            </div>
            <div className="detail-card">
              <div className="detail-header">Retry History</div>
              <ul className="retry-list">
                <li>Attempt 1 <span>{selectedMessage.retryCount >= 1 ? 'FAILED' : 'PENDING'}</span></li>
                <li>Attempt 2 <span>{selectedMessage.retryCount >= 2 ? 'FAILED' : 'PENDING'}</span></li>
                <li>Attempt 3 <span>{selectedMessage.retryCount >= 3 ? 'FAILED' : 'PENDING'}</span></li>
              </ul>
            </div>
          </aside>
        </div>
      </div>
    </AppShell>
  );
}

function LogsPage() {
  const [logs, setLogs] = useState(baseLogData);
  const [live, setLive] = useState(true);
  const [autoScroll, setAutoScroll] = useState(true);
  const [levelFilter, setLevelFilter] = useState<LogLevel>('ALL');
  const [serviceFilter, setServiceFilter] = useState('ALL');
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedId, setSelectedId] = useState<number | null>(baseLogData[0].id);

  useEffect(() => {
    if (!live) return;
    const timer = window.setInterval(() => {
      setLogs((prev) => [
        {
          id: prev[0] ? prev[0].id + 1 : 1,
          timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }),
          level: Math.random() > 0.7 ? 'WARN' : 'INFO',
          service: ['API', 'PAYMENT', 'DATABASE', 'AUTH'][Math.floor(Math.random() * 4)],
          consumer: `C${Math.floor(Math.random() * 3) + 1}`,
          trace: `tr-${Math.random().toString(16).slice(2, 7)}`,
          message: 'Mock event received from pipeline',
          payload: '{"mock":true,"event":"live"}',
        },
        ...prev,
      ].slice(0, 14));
    }, 3000);
    return () => window.clearInterval(timer);
  }, [live]);

  const visibleLogs = useMemo(() => {
    return logs.filter((entry) => {
      const messageMatch = searchTerm ? `${entry.message} ${entry.service} ${entry.trace}`.toLowerCase().includes(searchTerm.toLowerCase()) : true;
      const levelMatch = levelFilter === 'ALL' || entry.level === levelFilter;
      const serviceMatch = serviceFilter === 'ALL' || entry.service === serviceFilter;
      return messageMatch && levelMatch && serviceMatch;
    });
  }, [logs, levelFilter, serviceFilter, searchTerm]);

  const selectedLog = visibleLogs.find((entry) => entry.id === selectedId) ?? visibleLogs[0] ?? logs[0];

  const exportLogs = () => {
    const blob = new Blob([JSON.stringify(visibleLogs, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = 'logflow-logs.json';
    link.click();
    URL.revokeObjectURL(url);
  };

  const copySelectedLog = async () => {
    if (!selectedLog) return;
    try {
      await navigator.clipboard.writeText(selectedLog.payload);
    } catch {
      // ignore clipboard errors in mock mode
    }
  };

  const clearStream = () => {
    setLogs([]);
    setSelectedId(null);
  };

  return (
    <AppShell>
      <div className="page-frame logs-page">
        <header className="page-header">
          <div>
            <h1>Live Log Stream</h1>
            <p>Real-time view of messages flowing through the LogFlow pipeline</p>
          </div>
          <div className="header-actions">
            <button className="toggle-btn" onClick={() => setLive((v) => !v)}>{live ? 'LIVE' : 'PAUSED'}</button>
            <button className="small-btn" onClick={() => setAutoScroll((v) => !v)}>{autoScroll ? 'Auto Scroll ON' : 'Auto Scroll OFF'}</button>
          </div>
        </header>

        <div className="stats-row">
          <div className="mini-stat"><strong>{logs.length}</strong><span>Processed</span></div>
          <div className="mini-stat"><strong>{logs.filter((item) => item.level === 'ERROR').length}</strong><span>Errors</span></div>
          <div className="mini-stat"><strong>{logs.filter((item) => item.level === 'WARN').length}</strong><span>Dead Letter</span></div>
          <div className="mini-stat"><strong>{logs.filter((item) => item.service === 'API').length}</strong><span>Active</span></div>
        </div>

        <div className="logs-shell">
          <section className="panel stream-panel">
            <div className="stream-controls">
              <input value={searchTerm} onChange={(e) => setSearchTerm(e.target.value)} placeholder="Search logs..." />
              <select value={levelFilter} onChange={(e) => setLevelFilter(e.target.value as LogLevel)}>
                <option value="ALL">ALL</option>
                <option value="INFO">INFO</option>
                <option value="WARN">WARN</option>
                <option value="ERROR">ERROR</option>
              </select>
              <select value={serviceFilter} onChange={(e) => setServiceFilter(e.target.value)}>
                <option value="ALL">ALL SERVICES</option>
                {Array.from(new Set(logs.map((item) => item.service))).map((service) => (
                  <option key={service} value={service}>{service}</option>
                ))}
              </select>
              <button className="ghost-btn" onClick={clearStream}>Clear Stream</button>
              <button className="ghost-btn" onClick={exportLogs}>Export</button>
            </div>
            <div className="log-table-wrap">
              <table className="log-table">
                <thead>
                  <tr><th>Timestamp</th><th>Level</th><th>Service</th><th>Consumer</th><th>Trace</th><th>Message</th></tr>
                </thead>
                <tbody>
                  {visibleLogs.map((row) => (
                    <tr key={row.id} className={selectedId === row.id ? 'selected-row' : ''} onClick={() => setSelectedId(row.id)}>
                      <td>{row.timestamp}</td>
                      <td><span className={`level level-${row.level.toLowerCase()}`}>{row.level}</span></td>
                      <td>{row.service}</td>
                      <td>{row.consumer}</td>
                      <td>{row.trace}</td>
                      <td>{row.message}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <aside className="side-panel log-detail">
            <div className="detail-card">
              <div className="detail-header">Log Event Details</div>
              {selectedLog ? (
                <>
                  <div className="detail-key">Timestamp <span>{selectedLog.timestamp}</span></div>
                  <div className="detail-key">Service <span>{selectedLog.service}</span></div>
                  <div className="detail-key">Consumer <span>{selectedLog.consumer}</span></div>
                  <div className="detail-key">Trace ID <span>{selectedLog.trace}</span></div>
                  <div className="preview-box small"><code>{selectedLog.payload}</code></div>
                  <div className="detail-actions">
                    <button className="mini-button" onClick={copySelectedLog}>Copy JSON</button>
                    <button className="mini-button alt" onClick={() => window.location.assign('/dlq')}>View in DLQ</button>
                  </div>
                </>
              ) : (
                <div className="empty-state">No log selected</div>
              )}
            </div>
          </aside>
        </div>
      </div>
    </AppShell>
  );
}

function ScenariosPage() {
  const [scenarioState, setScenarioState] = useState({
    status: 'READY',
    elapsed: 0,
    throughput: 4200,
    lag: 420,
    errors: 12,
    retries: 8,
    dlq: 5,
    selectedConsumer: 'Consumer 1',
    failedConsumers: [] as string[],
  });

  useEffect(() => {
    if (scenarioState.status !== 'RUNNING') return;
    const timer = window.setInterval(() => {
      setScenarioState((prev) => ({
        ...prev,
        elapsed: prev.elapsed + 1,
        throughput: prev.throughput + 30,
        lag: prev.lag + 11,
      }));
    }, 1000);
    return () => window.clearInterval(timer);
  }, [scenarioState.status]);

  const startScenario = () => setScenarioState((prev) => ({ ...prev, status: 'RUNNING', elapsed: 0, throughput: 4200, lag: 420 }));
  const stopScenario = () => setScenarioState((prev) => ({ ...prev, status: 'STOPPED' }));
  const resetScenario = () => setScenarioState({ status: 'READY', elapsed: 0, throughput: 4200, lag: 420, errors: 12, retries: 8, dlq: 5, selectedConsumer: 'Consumer 1', failedConsumers: [] });
  const injectSpike = () => setScenarioState((prev) => ({ ...prev, status: 'RUNNING', throughput: prev.throughput + 1900, lag: prev.lag + 600, errors: prev.errors + 8 }));
  const injectMalformedLogs = () => setScenarioState((prev) => ({ ...prev, status: 'RUNNING', errors: prev.errors + 35, retries: prev.retries + 22, dlq: prev.dlq + 16 }));
  const killConsumer = () => setScenarioState((prev) => ({ ...prev, status: 'RUNNING', selectedConsumer: 'Consumer 3', failedConsumers: ['Consumer 3'], lag: prev.lag + 240 }));

  return (
    <AppShell>
      <div className="page-frame scenario-page">
        <header className="page-header">
          <div>
            <h1>Scenario Control Panel</h1>
            <p>Run, interrupt, and inspect test scenarios against the LogFlow pipeline</p>
          </div>
          <div className="header-actions">
            <span className="pill live">● LIVE</span>
            <span className="pill muted">{scenarioState.status}</span>
          </div>
        </header>

        <div className="scenario-toolbar">
          <span className="status-tag running">● TEST {scenarioState.status}</span>
          <span className="status-tag healthy">Kafka Online</span>
          <span className="status-tag healthy">Consumers 3 / 3 active</span>
          <span className="status-tag healthy">Selected: {scenarioState.selectedConsumer}</span>
          <button className="primary-btn" onClick={stopScenario}>Stop Test</button>
          <button className="small-btn" onClick={resetScenario}>Reset</button>
        </div>

        <div className="scenario-grid">
          {scenarioCards.map((card, idx) => (
            <div key={idx} className={`scenario-card ${card.status === 'RUNNING' || scenarioState.status === 'RUNNING' ? 'active' : ''}`}>
              <div className="scenario-head"><span>{card.title}</span><span className="scenario-badge">{card.tag}</span></div>
              <div className="scenario-status">{scenarioState.status === 'RUNNING' ? 'RUNNING' : card.status}</div>
              <p>{card.description}</p>
              <div className="scenario-metrics">
                {card.metrics.map((metric) => (
                  <div key={metric.label}><strong>{metric.value}</strong><span>{metric.label}</span></div>
                ))}
              </div>
              <button className="scenario-btn" onClick={startScenario}>{card.status === 'RUNNING' ? 'Stop' : 'Start Test'}</button>
            </div>
          ))}
        </div>

        <div className="scenario-tools">
          <button className="mini-button action" onClick={injectSpike}>Inject Spike</button>
          <button className="mini-button action" onClick={injectMalformedLogs}>Inject Malformed Logs</button>
          <button className="mini-button action" onClick={killConsumer}>Kill Consumer</button>
        </div>

        <div className="scenario-table panel">
          <div className="panel-title-row"><span>Robot Test Runs</span></div>
          <table>
            <thead>
              <tr><th>Scenario</th><th>Elapsed</th><th>Throughput</th><th>Lag</th><th>Errors</th><th>Retries</th><th>Status</th></tr>
            </thead>
            <tbody>
              <tr>
                <td>Slow Consumer</td>
                <td>{scenarioState.elapsed}s</td>
                <td>{scenarioState.throughput} msg/s</td>
                <td>{scenarioState.lag}</td>
                <td>{scenarioState.errors}</td>
                <td>{scenarioState.retries}</td>
                <td><span className="status-tag running">{scenarioState.status}</span></td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </AppShell>
  );
}

function App() {
  return (
    <Routes>
      <Route path="/" element={<OverviewPage />} />
      <Route path="/consumers" element={<ConsumersPage />} />
      <Route path="/dlq" element={<DlqPage />} />
      <Route path="/logs" element={<LogsPage />} />
      <Route path="/scenarios" element={<ScenariosPage />} />
    </Routes>
  );
}

export default App;
