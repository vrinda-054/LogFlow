import { useEffect, useMemo, useState } from 'react';
import { getDlqMessages, type DlqRecord } from '../api';
import DashboardShell from '../components/DashboardShell';

type ViewMessage = DlqRecord & {
  service: string;
  severity: 'ERROR' | 'WARN' | 'INFO';
  trace: string;
  message: string;
  payload: string;
};

const demoMessages: ViewMessage[] = [
  { id: 1217, failed_at: '2026-08-18T15:45:31Z', failure_reason: 'Invalid JSON', retry_count: 3, original_message: '{"service":"payment"}', service: 'Payment', severity: 'ERROR', trace: 'tr-8f21a', message: 'Invalid JSON structure', payload: '{\n  "timestamp": "2026-08-18T15:45:31",\n  "service": "payment",\n  "severity": "ERROR",\n  "message": "{invalid-json}"\n}' },
  { id: 1218, failed_at: '2026-08-18T15:45:27Z', failure_reason: 'Missing trace_id', retry_count: 3, original_message: '{"service":"api"}', service: 'API', severity: 'ERROR', trace: 'tr-4c91b', message: 'Missing trace_id', payload: '{\n  "service": "api",\n  "message": "trace_id is required"\n}' },
  { id: 1219, failed_at: '2026-08-18T15:45:19Z', failure_reason: 'Invalid severity', retry_count: 3, original_message: '{"service":"authentication"}', service: 'Authentication', severity: 'WARN', trace: 'tr-72ab4', message: 'Invalid severity', payload: '{\n  "service": "authentication",\n  "severity": "UNKNOWN"\n}' },
  { id: 1220, failed_at: '2026-08-18T15:45:11Z', failure_reason: 'Processing exception', retry_count: 3, original_message: '{"service":"database"}', service: 'Database', severity: 'ERROR', trace: 'tr-19ef2', message: 'Processing exception', payload: '{\n  "service": "database",\n  "error": "connection timeout"\n}' },
  { id: 1221, failed_at: '2026-08-18T15:44:58Z', failure_reason: 'Invalid JSON', retry_count: 3, original_message: '{"service":"payment"}', service: 'Payment', severity: 'ERROR', trace: 'tr-a3bc1', message: 'Invalid JSON', payload: '{\n  "service": "payment",\n  "message": "{invalid-json}"\n}' },
  { id: 1222, failed_at: '2026-08-18T15:44:41Z', failure_reason: 'Missing field: severity', retry_count: 2, original_message: '{"service":"api"}', service: 'API', severity: 'INFO', trace: 'tr-22cd8', message: 'Missing field: severity', payload: '{\n  "service": "api",\n  "message": "severity is required"\n}' },
  { id: 1223, failed_at: '2026-08-18T15:44:33Z', failure_reason: 'Invalid JSON', retry_count: 3, original_message: '{"service":"authentication"}', service: 'Authentication', severity: 'ERROR', trace: 'tr-91fa0', message: 'Invalid JSON', payload: '{\n  "service": "authentication"\n}' },
  { id: 1224, failed_at: '2026-08-18T15:44:18Z', failure_reason: 'Processing exception', retry_count: 3, original_message: '{"service":"database"}', service: 'Database', severity: 'WARN', trace: 'tr-5e3d2', message: 'Processing exception', payload: '{\n  "service": "database"\n}' },
  { id: 1225, failed_at: '2026-08-18T15:43:55Z', failure_reason: 'Missing field: service', retry_count: 3, original_message: '{"service":"payment"}', service: 'Payment', severity: 'ERROR', trace: 'tr-b72c3', message: 'Missing field: service', payload: '{\n  "message": "service is required"\n}' },
];

function toViewMessage(record: DlqRecord): ViewMessage {
  let parsed: Record<string, unknown> = {};
  try { parsed = JSON.parse(record.original_message) as Record<string, unknown>; } catch { /* Preserve malformed messages. */ }
  return {
    ...record,
    service: typeof parsed.service === 'string' ? parsed.service : 'Unknown',
    severity: 'ERROR',
    trace: typeof parsed.trace_id === 'string' ? parsed.trace_id : '—',
    message: typeof parsed.message === 'string' ? parsed.message : record.failure_reason,
    payload: JSON.stringify(parsed, null, 2),
  };
}

export default function DlqInspectorPage() {
  const [messages, setMessages] = useState<ViewMessage[]>(demoMessages);
  const [total, setTotal] = useState(127);
  const [selectedId, setSelectedId] = useState(1217);
  const [search, setSearch] = useState('');
  const [service, setService] = useState('ALL');
  const [error, setError] = useState<string | null>(null);
  const [isLive, setIsLive] = useState(false);

  const refresh = async () => {
    try {
      const response = await getDlqMessages();
      const next = response.messages.map(toViewMessage);
      setMessages(next);
      setTotal(response.total);
      setSelectedId((current) => next.some((item) => item.id === current) ? current : next[0]?.id ?? 0);
      setIsLive(true);
      setError(null);
    } catch (requestError) {
      setIsLive(false);
      setError(requestError instanceof Error ? requestError.message : 'Unable to load DLQ messages');
    }
  };

  useEffect(() => {
    void refresh();
    const timer = window.setInterval(() => void refresh(), 5000);
    return () => window.clearInterval(timer);
  }, []);

  const visibleMessages = useMemo(() => messages.filter((item) => {
    const query = search.toLowerCase();
    return (!query || `${item.service} ${item.failure_reason} ${item.message} ${item.trace}`.toLowerCase().includes(query))
      && (service === 'ALL' || item.service === service);
  }), [messages, search, service]);
  const selected = messages.find((item) => item.id === selectedId) ?? visibleMessages[0];

  return (
    <DashboardShell>
      <div className="page-frame dlq-page">
        <header className="page-header">
          <div><h1>Dead Letter Queue Inspector</h1><p>Inspect messages that failed processing and were isolated from the main pipeline</p></div>
          <div className="header-actions dlq-header-meta"><div><span>DLQ TOPIC</span><strong>logs.DLQ</strong></div><span className="pill live">● {isLive ? 'LIVE' : 'DEMO'}</span><span>Updated 2s ago</span></div>
        </header>
        {error && <div className="error-banner" role="alert">Showing demo data: {error}</div>}

        <div className="dlq-metrics screenshot-metrics">
          <div className="metric-box danger-metric"><strong>{total}</strong><b>Total Failed Messages</b><span>in DLQ</span></div>
          <div className="metric-box danger-metric"><strong>84</strong><b>Failed Today</b><span>+12 in last hour</span></div>
          <div className="metric-box warning-metric"><strong>2.7</strong><b>Average Retry Count</b><span>out of max 3</span></div>
          <div className="metric-box reason-metric"><strong>Invalid JSON</strong><b>Top Failure Reason</b><span>58 occurrences</span></div>
        </div>

        <div className="filter-bar screenshot-filters">
          <button className="search-button" aria-label="Search messages">⌕</button>
          <input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search" />
          <select value={service} onChange={(event) => setService(event.target.value)}><option value="ALL">All Services</option>{Array.from(new Set(messages.map((item) => item.service))).map((name) => <option key={name} value={name}>{name}</option>)}</select>
          <select defaultValue="ALL"><option>All Failure Types</option></select>
          <select defaultValue="30"><option value="30">Last 30 Minutes</option></select>
          <select defaultValue="ALL"><option>All Retries</option></select>
        </div>

        <div className="dlq-content screenshot-dlq-content">
          <section className="panel table-panel large-table">
            <div className="panel-title-row"><div><h2>Dead-Lettered Messages</h2><p>{total} messages requiring inspection</p></div><span className="new-count">● 3 new in last 60s</span></div>
            <table className="dlq-table screenshot-dlq-table"><thead><tr><th>Timestamp</th><th>Service</th><th>Severity</th><th>Failure Reason</th><th>Retries</th><th>Trace ID</th><th>Status</th><th>Action</th></tr></thead>
              <tbody>{visibleMessages.map((row) => <tr key={row.id} className={selected?.id === row.id ? 'selected-row' : ''} onClick={() => setSelectedId(row.id)}>
                <td>{new Date(row.failed_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false })}</td><td><b>{row.service}</b></td><td><span className={`severity-badge ${row.severity.toLowerCase()}`}>● {row.severity}</span></td><td>{row.failure_reason}</td><td className="retry-count">{row.retry_count}/3</td><td className="trace-id">{row.trace}</td><td><span className="failed-badge">FAILED</span></td><td><button className="mini-button" onClick={() => setSelectedId(row.id)}>View</button></td>
              </tr>)}</tbody></table>
            <div className="table-footer"><span>Rows per page: 25</span><span>1–25 of {total}　‹ 1 2 3 4 5 ›</span></div>
          </section>
          <aside className="side-panel detail-panel screenshot-details"><div className="detail-card">
            <div className="detail-title-row"><h2>Message Details</h2><span className="failed-badge">DEAD-LETTERED</span></div>
            {selected ? <><p className="detail-section-label">FAILURE INFORMATION</p><div className="detail-grid"><div><label>FAILURE REASON</label><strong className="danger-text">{selected.message}</strong></div><div><label>RETRY COUNT</label><strong className="orange-text">{selected.retry_count} / 3</strong></div><div><label>FAILURE TIMESTAMP</label><strong>{new Date(selected.failed_at).toLocaleString()}</strong></div><div><label>SERVICE</label><strong>{selected.service}</strong></div><div><label>SEVERITY</label><strong className="danger-text">{selected.severity}</strong></div><div><label>TRACE ID</label><strong className="trace-id">{selected.trace}</strong></div></div><div className="detail-tabs"><button>Original<br />Message</button><button>Raw<br />Payload</button><button className="copy-button" onClick={() => navigator.clipboard.writeText(selected.payload)}>Copy<br />Payload</button></div><pre className="payload-box">{selected.payload}</pre><p className="detail-section-label">RETRY HISTORY</p></> : <div className="empty-state">Select a message</div>}
          </div></aside>
        </div>

        <div className="dlq-bottom-panels">
          <section className="panel failure-reasons"><h2>Failure Reasons</h2><div className="reason-bars">{[['Invalid JSON', 58, 'red'], ['Missing Fields', 31, 'orange'], ['Processing Exception', 22, 'blue'], ['Invalid Severity', 16, 'gray']].map(([name, count, color]) => <div className="reason-bar" key={name as string}><span>{name}</span><b>{count}</b><i className={color as string} style={{ width: `${Number(count) * 1.45}%` }} /></div>)}</div></section>
          <section className="panel dlq-activity"><h2>● DLQ Activity</h2><strong>+3 messages</strong><p>entered DLQ in the last 60 seconds</p><div className="activity-bars">{[12, 20, 15, 28, 22, 34, 30, 42, 38, 52, 46, 65, 58, 80, 90].map((height, index) => <i key={index} style={{ height: `${height}%` }} />)}</div><small>Last 60s</small></section>
          <section className="panel failure-flow"><h2>Failure Flow</h2><div>PROCESSING FAILURE</div><span>↓</span><div className="flow-retry">RETRY 1　·　RETRY 2　·　RETRY 3</div><span>↓</span><div>DEAD LETTER QUEUE</div><span>↓</span><div className="flow-inspect">INSPECT</div></section>
        </div>
      </div>
    </DashboardShell>
  );
}
