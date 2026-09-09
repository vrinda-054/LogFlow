import { useEffect, useMemo, useState } from 'react';
import DashboardShell from '../components/DashboardShell';

type LogLevel = 'INFO' | 'WARN' | 'ERROR';

type LiveLog = {
  id: number;
  timestamp: string;
  level: LogLevel;
  service: string;
  consumer: string;
  partition: string;
  trace: string;
  message: string;
  payload: string;
  status: 'processed' | 'failed';
};

const initialLogs: LiveLog[] = [
  { id: 1, timestamp: '18:59:44.885', level: 'ERROR', service: 'DATABASE', consumer: 'C3', partition: 'P1', trace: 'tr-6nr7j', message: 'Processing failed — connection timeout', payload: '{"timestamp":"2026-08-18T18:59:44.885","service":"database","severity":"ERROR","message":"Processing failed — connection timeout","trace_id":"tr-6nr7j","consumer":"consumer-3","partition":"P1","status":"failed"}', status: 'failed' },
  { id: 2, timestamp: '18:59:45.042', level: 'INFO', service: 'DATABASE', consumer: 'C2', partition: 'P2', trace: 'tr-mqrj0', message: 'Request processed successfully in 68ms', payload: '{"status":"processed","duration_ms":68}', status: 'processed' },
  { id: 3, timestamp: '18:59:35.355', level: 'ERROR', service: 'PAYMENT', consumer: 'C3', partition: 'P1', trace: 'tr-mtkvt', message: 'Processing failed — connection timeout', payload: '{"status":"failed","reason":"gateway timeout"}', status: 'failed' },
  { id: 4, timestamp: '18:59:39.131', level: 'ERROR', service: 'DATABASE', consumer: 'C2', partition: 'P3', trace: 'tr-j0vda', message: 'Processing failed — connection timeout', payload: '{"status":"failed","reason":"connection timeout"}', status: 'failed' },
  { id: 5, timestamp: '18:59:47.277', level: 'INFO', service: 'API', consumer: 'C3', partition: 'P2', trace: 'tr-fnbyv', message: 'Request processed successfully in 63ms', payload: '{"status":"processed","duration_ms":63}', status: 'processed' },
  { id: 6, timestamp: '18:59:09.056', level: 'INFO', service: 'API', consumer: 'C2', partition: 'P1', trace: 'tr-8x98v', message: 'Request processed successfully in 88ms', payload: '{"status":"processed","duration_ms":88}', status: 'processed' },
  { id: 7, timestamp: '18:59:15.510', level: 'WARN', service: 'AUTH', consumer: 'C1', partition: 'P2', trace: 'tr-fomjq', message: 'Latency threshold exceeded — 578ms', payload: '{"status":"warning","latency_ms":578}', status: 'processed' },
  { id: 8, timestamp: '18:59:33.742', level: 'INFO', service: 'AUTH', consumer: 'C2', partition: 'P3', trace: 'tr-nk1bk', message: 'Request processed successfully in 84ms', payload: '{"status":"processed","duration_ms":84}', status: 'processed' },
];

const services = ['API', 'PAYMENT', 'DATABASE', 'AUTH'];

function createMockLog(id: number): LiveLog {
  const service = services[Math.floor(Math.random() * services.length)];
  const failed = Math.random() > 0.78;
  const level: LogLevel = failed ? 'ERROR' : Math.random() > 0.84 ? 'WARN' : 'INFO';
  const timestamp = new Date().toLocaleTimeString([], { hour12: false }) + `.${Math.floor(Math.random() * 999).toString().padStart(3, '0')}`;
  return {
    id,
    timestamp,
    level,
    service,
    consumer: `C${Math.floor(Math.random() * 3) + 1}`,
    partition: `P${Math.floor(Math.random() * 3) + 1}`,
    trace: `tr-${Math.random().toString(16).slice(2, 7)}`,
    message: failed ? 'Processing failed — connection timeout' : level === 'WARN' ? 'Latency threshold exceeded — 578ms' : 'Request processed successfully in 68ms',
    payload: JSON.stringify({ timestamp, service: service.toLowerCase(), severity: level, status: failed ? 'failed' : 'processed' }),
    status: failed ? 'failed' : 'processed',
  };
}

export default function LiveLogsPage() {
  const [logs, setLogs] = useState(initialLogs);
  const [live, setLive] = useState(true);
  const [autoScroll, setAutoScroll] = useState(true);
  const [levelFilter, setLevelFilter] = useState<'ALL' | LogLevel>('ALL');
  const [serviceFilter, setServiceFilter] = useState('ALL');
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedId, setSelectedId] = useState(initialLogs[0].id);

  useEffect(() => {
    if (!live) return undefined;
    const timer = window.setInterval(() => {
      setLogs((current) => {
        const nextId = Math.max(0, ...current.map((entry) => entry.id)) + 1;
        return [createMockLog(nextId), ...current].slice(0, 100);
      });
    }, 3000);
    return () => window.clearInterval(timer);
  }, [live]);

  const visibleLogs = useMemo(() => logs.filter((entry) => {
    const text = `${entry.message} ${entry.service} ${entry.trace} ${entry.consumer}`.toLowerCase();
    return (levelFilter === 'ALL' || entry.level === levelFilter)
      && (serviceFilter === 'ALL' || entry.service === serviceFilter)
      && (!searchTerm || text.includes(searchTerm.toLowerCase()));
  }), [levelFilter, logs, searchTerm, serviceFilter]);

  const selectedLog = visibleLogs.find((entry) => entry.id === selectedId) ?? visibleLogs[0] ?? logs[0];
  const errors = logs.filter((entry) => entry.level === 'ERROR').length;

  const exportLogs = () => {
    const url = URL.createObjectURL(new Blob([JSON.stringify(visibleLogs, null, 2)], { type: 'application/json' }));
    const link = document.createElement('a');
    link.href = url;
    link.download = 'logflow-live-events.json';
    link.click();
    URL.revokeObjectURL(url);
  };

  return (
    <DashboardShell>
      <div className="page-frame logs-page live-logs-page">
        <header className="page-header">
          <div>
            <h1>Live Log Stream</h1>
            <p>Real-time view of messages flowing through the LogFlow pipeline</p>
          </div>
          <div className="header-actions live-header-meta">
            <span className="muted">Last event: 18:59:42</span>
            <button className={`toggle-btn ${live ? '' : 'paused'}`} onClick={() => setLive((value) => !value)}>● {live ? 'LIVE' : 'PAUSED'}</button>
          </div>
        </header>

        <div className="stats-row live-stats">
          <div className="mini-stat"><strong>2,184</strong><span>Messages/sec</span><small>+8.4%</small></div>
          <div className="mini-stat"><strong>143,201</strong><span>Processed</span></div>
          <div className="mini-stat danger-stat"><strong>{errors + 334}</strong><span>Errors</span></div>
          <div className="mini-stat danger-stat"><strong>127</strong><span>Dead-lettered</span></div>
          <div className="mini-stat live-stat"><strong>● LIVE</strong><span>Stream Status</span></div>
        </div>

        <section className="panel live-toolbar">
          <button className="toggle-btn" onClick={() => setLive((value) => !value)}>● {live ? 'LIVE' : 'PAUSED'}</button>
          <button className="small-btn" onClick={() => setAutoScroll((value) => !value)}>Auto Scroll {autoScroll ? 'ON' : 'OFF'}</button>
          {(['ALL', 'INFO', 'WARN', 'ERROR'] as const).map((level) => (
            <button key={level} className={`small-btn ${levelFilter === level ? 'selected-filter' : ''}`} onClick={() => setLevelFilter(level)}>Level {level}</button>
          ))}
          <select value={serviceFilter} onChange={(event) => setServiceFilter(event.target.value)}>
            <option value="ALL">ALL SERVICES</option>
            {services.map((service) => <option key={service} value={service}>{service}</option>)}
          </select>
          <input value={searchTerm} onChange={(event) => setSearchTerm(event.target.value)} placeholder="Search logs, trace ID, service..." />
          <button className="small-btn" onClick={() => { setLogs([]); setSelectedId(0); }}>Clear Stream</button>
          <button className="small-btn" onClick={exportLogs}>↑ Export</button>
        </section>

        <div className="logs-shell live-log-content">
          <section className="panel stream-panel">
            <div className="panel-title-row"><span>LIVE EVENTS</span><span className="status-tag healthy">2,184 events/sec</span></div>
            <div className="log-table-wrap">
              <table className="log-table live-log-table">
                <thead><tr><th>TIMESTAMP</th><th>LEVEL</th><th>SERVICE</th><th>CON.</th><th>TRACE ID</th><th>MESSAGE</th></tr></thead>
                <tbody>
                  {visibleLogs.map((row) => (
                    <tr key={row.id} className={`${selectedLog?.id === row.id ? 'selected-row ' : ''}${row.level === 'ERROR' ? 'error-row' : ''}`} onClick={() => setSelectedId(row.id)}>
                      <td>{row.timestamp}</td>
                      <td><span className={`level level-${row.level.toLowerCase()}`}>{row.level}</span></td>
                      <td><span className="service-badge">{row.service}</span></td>
                      <td>{row.consumer}</td>
                      <td className="trace-id">{row.trace}</td>
                      <td>{row.message}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="live-log-footer">Showing latest {visibleLogs.length} events · Rate: <strong>2,184 msg/s</strong> · Processed: <strong>143,201</strong> · Errors: <strong className="danger-text">342</strong> · DLQ: <strong className="danger-text">127</strong></div>
          </section>

          <aside className="side-panel log-detail">
            <div className="detail-card">
              <div className="detail-header">Selected event</div>
              {selectedLog && (
                <>
                  <div className="detail-grid"><div><label>TIMESTAMP</label><strong>{selectedLog.timestamp}</strong></div><div><label>SERVICE</label><strong>{selectedLog.service}</strong></div><div><label>CONSUMER</label><strong>{selectedLog.consumer}</strong></div><div><label>PARTITION</label><strong>{selectedLog.partition}</strong></div><div><label>TRACE ID</label><strong className="trace-id">{selectedLog.trace}</strong></div><div><label>PROCESSING STATUS</label><strong className={selectedLog.status === 'failed' ? 'danger-text' : 'live-text'}>{selectedLog.status.toUpperCase()}</strong></div></div>
                  <div className="detail-section-label">MESSAGE</div><p className="selected-message">{selectedLog.message}</p>
                  <div className="detail-section-label">RAW JSON <button className="mini-button copy-button" onClick={() => navigator.clipboard?.writeText(selectedLog.payload)}>Copy JSON</button></div>
                  <pre className="payload-box">{selectedLog.payload}</pre>
                </>
              )}
            </div>
          </aside>
        </div>

        <div className="bottom-bar">
          {['KAFKA', 'CONSUMER GROUP', 'PROCESSING'].map((label, index) => <div key={label} className="pipeline-card"><span>{label}</span><strong>{index === 1 ? '3 consumers' : index === 0 ? '2,184/s' : '2,102/s'}</strong><small>● HEALTHY</small></div>)}
        </div>
      </div>
    </DashboardShell>
  );
}
