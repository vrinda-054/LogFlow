import { useEffect, useMemo, useState } from 'react';
import { getDlqMessages, type DlqRecord } from '../api';

type ViewMessage = DlqRecord & {
  service: string;
  severity: 'ERROR' | 'WARN';
  trace: string;
  message: string;
  payload: string;
};

function toViewMessage(record: DlqRecord): ViewMessage {
  let service = 'Unknown';
  let trace = '—';
  let message = record.failure_reason;
  let payload = record.original_message;

  try {
    const parsed = JSON.parse(record.original_message) as Record<string, unknown>;
    service = typeof parsed.service === 'string' ? parsed.service : service;
    trace = typeof parsed.trace_id === 'string' ? parsed.trace_id : trace;
    message = typeof parsed.message === 'string' ? parsed.message : message;
    payload = JSON.stringify(parsed, null, 2);
  } catch {
    // Preserve malformed payloads as raw text for inspection.
  }

  return { ...record, service, severity: 'ERROR', trace, message, payload };
}

export default function DlqInspectorPage() {
  const [messages, setMessages] = useState<ViewMessage[]>([]);
  const [total, setTotal] = useState(0);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [search, setSearch] = useState('');
  const [service, setService] = useState('ALL');
  const [error, setError] = useState<string | null>(null);

  const refresh = async () => {
    try {
      const response = await getDlqMessages();
      const next = response.messages.map(toViewMessage);
      setMessages(next);
      setTotal(response.total);
      setSelectedId((current) => current ?? next[0]?.id ?? null);
      setError(null);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : 'Unable to load DLQ messages');
    }
  };

  useEffect(() => {
    void refresh();
    const timer = window.setInterval(() => void refresh(), 5000);
    return () => window.clearInterval(timer);
  }, []);

  const visibleMessages = useMemo(
    () => messages.filter((item) => {
      const query = search.toLowerCase();
      const matchesSearch = !query || `${item.service} ${item.failure_reason} ${item.message} ${item.trace}`.toLowerCase().includes(query);
      return matchesSearch && (service === 'ALL' || item.service === service);
    }),
    [messages, search, service],
  );
  const selected = visibleMessages.find((item) => item.id === selectedId) ?? visibleMessages[0];
  const averageRetries = messages.length
    ? (messages.reduce((sum, item) => sum + item.retry_count, 0) / messages.length).toFixed(1)
    : '0.0';

  return (
    <div className="page-frame dlq-page">
      <header className="page-header">
        <div>
          <h1>Dead Letter Queue Inspector</h1>
          <p>Inspect messages that failed processing and were isolated from the main pipeline</p>
        </div>
        <div className="header-actions">
          <span className="pill live">● LIVE</span>
          <button className="small-btn" onClick={() => void refresh()}>Refresh</button>
        </div>
      </header>

      {error && <div className="error-banner" role="alert">DLQ API unavailable: {error}</div>}

      <div className="dlq-metrics">
        <div className="metric-box"><strong>{total}</strong><span>Total Failed Messages</span></div>
        <div className="metric-box"><strong>{messages.filter((item) => item.retry_count > 0).length}</strong><span>Retried Messages</span></div>
        <div className="metric-box accent"><strong>{averageRetries}</strong><span>Average Retry Count</span></div>
        <div className="metric-box alt"><strong>{messages[0]?.failure_reason ?? 'None'}</strong><span>Top Failure Reason</span></div>
      </div>

      <div className="filter-bar">
        <input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search message, service, reason" />
        <select value={service} onChange={(event) => setService(event.target.value)}>
          <option value="ALL">All Services</option>
          {Array.from(new Set(messages.map((item) => item.service))).map((name) => <option key={name} value={name}>{name}</option>)}
        </select>
        <button className="small-btn" onClick={() => { setSearch(''); setService('ALL'); }}>Reset</button>
      </div>

      <div className="dlq-content">
        <section className="panel table-panel large-table">
          <div className="panel-title-row"><span>Dead-Lettered Messages ({total})</span></div>
          <table className="dlq-table">
            <thead><tr><th>Timestamp</th><th>Service</th><th>Reason</th><th>Retries</th><th>Trace ID</th><th>Action</th></tr></thead>
            <tbody>
              {visibleMessages.map((row) => (
                <tr key={row.id} className={selected?.id === row.id ? 'selected-row' : ''} onClick={() => setSelectedId(row.id)}>
                  <td>{new Date(row.failed_at).toLocaleTimeString()}</td>
                  <td>{row.service}</td>
                  <td>{row.failure_reason}</td>
                  <td>{row.retry_count}</td>
                  <td>{row.trace}</td>
                  <td><button className="mini-button" onClick={() => setSelectedId(row.id)}>Inspect</button></td>
                </tr>
              ))}
            </tbody>
          </table>
          {!visibleMessages.length && <div className="empty-state">No DLQ messages found</div>}
        </section>

        <aside className="side-panel detail-panel">
          <div className="detail-card">
            <div className="detail-header">Message Details</div>
            {selected ? (
              <>
                <div className="detail-key">Service <span>{selected.service}</span></div>
                <div className="detail-key">Failure <span>{selected.failure_reason}</span></div>
                <div className="detail-key">Trace ID <span>{selected.trace}</span></div>
                <div className="detail-key">Retries <span>{selected.retry_count}</span></div>
                <div className="preview-box"><code>{selected.payload}</code></div>
                <button className="mini-button" onClick={() => navigator.clipboard.writeText(selected.payload)}>Copy Payload</button>
              </>
            ) : (
              <div className="empty-state">Select a DLQ message to inspect it</div>
            )}
          </div>
        </aside>
      </div>
    </div>
  );
}
