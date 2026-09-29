import { useEffect, useMemo, useRef, useState } from 'react';
import { getDlqMessages, getDlqActivity, type DlqRecord, type DlqActivityRecord } from '../api';
import DashboardShell from '../components/DashboardShell';

type ViewMessage = DlqRecord & {
  service: string;
  severity: 'ERROR' | 'WARN' | 'INFO';
  trace: string;
  message: string;
  payload: string;
};

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

function formatFailureReason(reason: string | undefined): string {
  const normalized = reason?.toLowerCase() ?? '';

  if (normalized.includes('invalid-trace-id-with-hyphens-and-too-short')) {
    return 'Invalid trace ID';
  }

  if (normalized.includes('missing-service')) {
    return 'Missing service';
  }

  if (normalized.includes('message-object')) {
    return 'Invalid message format';
  }

  if (normalized.includes('unknown-severity')) {
    return 'Unknown severity';
  }

  const readable = (reason ?? 'Unknown failure')
    .replace(/[{}[\]"']/g, ' ')
    .split(/schema validation|validation error|details?:/i)[0]
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  if (!readable || readable.length > 48) {
    return 'Processing validation error';
  }

  return readable.charAt(0).toUpperCase() + readable.slice(1);
}

type FilterOption = {
  value: string;
  label: string;
};

function FilterSelect({
  value,
  options,
  onChange,
}: {
  value: string;
  options: FilterOption[];
  onChange: (value: string) => void;
}) {
  const [isOpen, setIsOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const selectedOption = options.find((option) => option.value === value) ?? options[0];

  useEffect(() => {
    if (!isOpen) return;

    const handlePointerDown = (event: PointerEvent) => {
      if (!containerRef.current?.contains(event.target as Node)) {
        setIsOpen(false);
      }
    };

    document.addEventListener('pointerdown', handlePointerDown);
    return () => document.removeEventListener('pointerdown', handlePointerDown);
  }, [isOpen]);

  return (
    <div className={`filter-select${isOpen ? ' is-open' : ''}`} ref={containerRef}>
      <button
        type="button"
        className="filter-select-trigger"
        aria-expanded={isOpen}
        aria-haspopup="listbox"
        onClick={() => setIsOpen((open) => !open)}
      >
        <span>{selectedOption?.label}</span>
        <span className="filter-select-chevron" aria-hidden="true">⌄</span>
      </button>
      {isOpen && (
        <div className="filter-select-menu" role="listbox" aria-label={selectedOption?.label}>
          {options.map((option) => (
            <button
              type="button"
              role="option"
              aria-selected={option.value === value}
              className={`filter-select-option${option.value === value ? ' is-selected' : ''}`}
              key={option.value}
              onClick={() => {
                onChange(option.value);
                setIsOpen(false);
              }}
            >
              <span>{option.label}</span>
              {option.value === value && <span className="filter-select-check" aria-hidden="true">✓</span>}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

export default function DlqInspectorPage() {
  const [messages, setMessages] = useState<ViewMessage[]>([]);
  const [activity, setActivity] = useState<DlqActivityRecord[]>([]);
  const [total, setTotal] = useState(0);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [search, setSearch] = useState('');
  const [service, setService] = useState('ALL');
  const [failureType, setFailureType] = useState('ALL');
  const [timeRange, setTimeRange] = useState('30');
  const [retryFilter, setRetryFilter] = useState('ALL');
  const [error, setError] = useState<string | null>(null);
  const [isLive, setIsLive] = useState(false);
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null);

  const refresh = async () => {
    try {
      const [response, activityResponse] = await Promise.all([
        getDlqMessages(),
        getDlqActivity(24),
    ]);

    const next = response.messages.map(toViewMessage);

    setMessages(next);
    setTotal(response.total);
    setActivity(activityResponse.activity);
    setSelectedId((current) =>
      next.some((item) => item.id === current) ? current : next[0]?.id ?? null
    );
    setIsLive(true);
    setLastRefresh(new Date());
    setError(null);
  } catch (requestError) {
    setIsLive(false);
    setError(
      requestError instanceof Error
        ? requestError.message
        : 'Unable to load DLQ data'
    );
  }};

  useEffect(() => {
    void refresh();
    const timer = window.setInterval(() => void refresh(), 5000);
    return () => window.clearInterval(timer);
  }, []);

  const visibleMessages = useMemo(() => messages.filter((item) => {
    const query = search.toLowerCase();
    const ageInMinutes = (Date.now() - new Date(item.failed_at).getTime()) / 60000;
    const matchesTime = timeRange === 'ALL' || ageInMinutes <= Number(timeRange);
    const matchesRetries = retryFilter === 'ALL'
      || (retryFilter === '4+' ? item.retry_count >= 4 : item.retry_count === Number(retryFilter));
    return (!query || `${item.service} ${item.failure_reason} ${item.message} ${item.trace}`.toLowerCase().includes(query))
      && (service === 'ALL' || item.service === service)
      && (failureType === 'ALL' || formatFailureReason(item.failure_reason) === failureType)
      && matchesTime
      && matchesRetries;
  }), [messages, search, service, failureType, timeRange, retryFilter]);
  const selected = messages.find((item) => item.id === selectedId) ?? visibleMessages[0];
  const averageRetryCount = messages.length
    ? messages.reduce((sum, message) => sum + message.retry_count, 0) / messages.length
    : null;
  const failureReasonCounts = useMemo(() => {
    const counts = new Map<string, number>();
    messages.forEach((message) => {
      counts.set(message.failure_reason, (counts.get(message.failure_reason) ?? 0) + 1);
    });
    return Array.from(counts.entries()).sort(([, countA], [, countB]) => countB - countA);
  }, [messages]);
  const failedToday = messages.filter((message) => {
    const failedAt = new Date(message.failed_at);
    const now = new Date();
    return failedAt.getFullYear() === now.getFullYear()
      && failedAt.getMonth() === now.getMonth()
      && failedAt.getDate() === now.getDate();
  }).length;

  return (
    <DashboardShell>
      <div className="page-frame dlq-page">
        <header className="page-header">
          <div><h1>Dead Letter Queue Inspector</h1><p>Inspect messages that failed processing and were isolated from the main pipeline</p></div>
          <div className="header-actions dlq-header-meta"><div><span>DLQ TOPIC</span><strong>logs.DLQ</strong></div><span className="pill live">● {isLive ? 'LIVE' : 'WAITING'}</span><span>Updated {lastRefresh ? lastRefresh.toLocaleTimeString() : 'not yet'}</span></div>
        </header>
        {error && <div className="error-banner" role="alert">Unable to load DLQ messages: {error}</div>}

        <div className="dlq-metrics screenshot-metrics">
          <div className="metric-box danger-metric"><strong>{total}</strong><b>Total Failed Messages</b><span>in DLQ</span></div>
          <div className="metric-box danger-metric"><strong>{failedToday}</strong><b>Failed Today</b><span>from loaded messages</span></div>
          <div className="metric-box warning-metric"><strong>{averageRetryCount?.toFixed(1) ?? '—'}</strong><b>Average Retry Count</b><span>from loaded messages</span></div>
          <div className="metric-box reason-metric"><strong>{formatFailureReason(failureReasonCounts[0]?.[0])}</strong><b>Top Failure Reason</b><span>{failureReasonCounts[0]?.[1] ?? 0} occurrences</span></div>
        </div>

        <div className="filter-bar screenshot-filters">
          <button className="search-button" aria-label="Search messages">⌕</button>
          <input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search" />
          <FilterSelect
            value={service}
            onChange={setService}
            options={[{ value: 'ALL', label: 'All Services' }, ...Array.from(new Set(messages.map((item) => item.service))).map((name) => ({ value: name, label: name }))]}
          />
          <FilterSelect
            value={failureType}
            onChange={setFailureType}
            options={[{ value: 'ALL', label: 'All Failure Types' }, ...Array.from(new Set(messages.map((item) => formatFailureReason(item.failure_reason)))).map((reason) => ({ value: reason, label: reason }))]}
          />
          <FilterSelect
            value={timeRange}
            onChange={setTimeRange}
            options={[
              { value: '30', label: 'Last 30 Minutes' },
              { value: '60', label: 'Last Hour' },
              { value: '360', label: 'Last 6 Hours' },
              { value: '1440', label: 'Last 24 Hours' },
              { value: 'ALL', label: 'All Time' },
            ]}
          />
          <FilterSelect
            value={retryFilter}
            onChange={setRetryFilter}
            options={[
              { value: 'ALL', label: 'All Retry Attempts' },
              { value: '0', label: '0 Attempts' },
              { value: '1', label: '1 Attempt' },
              { value: '2', label: '2 Attempts' },
              { value: '3', label: '3 Attempts' },
              { value: '4+', label: '4+ Attempts' },
            ]}
          />
        </div>

        <div className="dlq-content screenshot-dlq-content">
          <section className="panel table-panel large-table">
            <div className="panel-title-row"><div><h2>Dead-Lettered Messages</h2><p>{total} messages requiring inspection</p></div></div>
            <table className="dlq-table screenshot-dlq-table"><thead><tr><th>Timestamp</th><th>Service</th><th>Severity</th><th>Failure Reason</th><th>Retries</th><th>Trace ID</th><th>Status</th><th>Action</th></tr></thead>
              <tbody>{visibleMessages.map((row) => <tr key={row.id} className={selected?.id === row.id ? 'selected-row' : ''} onClick={() => setSelectedId(row.id)}>
                <td>{new Date(row.failed_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false })}</td><td><b>{row.service}</b></td><td><span className={`severity-badge ${row.severity.toLowerCase()}`}>● {row.severity}</span></td><td>{row.failure_reason}</td><td className="retry-count">{row.retry_count}/3</td><td className="trace-id">{row.trace}</td><td><span className="failed-badge">FAILED</span></td><td><button className="mini-button" onClick={() => setSelectedId(row.id)}>View</button></td>
              </tr>)}</tbody></table>
            <div className="table-footer"><span>Rows per page: 25</span><span>{messages.length === 0 ? '0 messages' : `1–25 of ${total}　‹ 1 2 3 4 5 ›`}</span></div>
          </section>
          <aside className="side-panel detail-panel screenshot-details"><div className="detail-card">
            <div className="detail-title-row"><h2>Message Details</h2><span className="failed-badge">DEAD-LETTERED</span></div>
            {selected ? <><p className="detail-section-label">FAILURE INFORMATION</p><div className="detail-grid"><div><label>FAILURE REASON</label><strong className="danger-text">{selected.message}</strong></div><div><label>RETRY COUNT</label><strong className="orange-text">{selected.retry_count} / 3</strong></div><div><label>FAILURE TIMESTAMP</label><strong>{new Date(selected.failed_at).toLocaleString()}</strong></div><div><label>SERVICE</label><strong>{selected.service}</strong></div><div><label>SEVERITY</label><strong className="danger-text">{selected.severity}</strong></div><div><label>TRACE ID</label><strong className="trace-id">{selected.trace}</strong></div></div><div className="detail-tabs"><button>Original<br />Message</button><button>Raw<br />Payload</button><button className="copy-button" onClick={() => navigator.clipboard.writeText(selected.payload)}>Copy<br />Payload</button></div><pre className="payload-box">{selected.payload}</pre><p className="detail-section-label">RETRY HISTORY</p></> : <div className="empty-state">Select a message</div>}
          </div></aside>
        </div>

        <div className="dlq-bottom-panels">
          <section className="panel failure-reasons"><h2>Failure Reasons</h2><div className="reason-bars">{failureReasonCounts.length > 0 ? failureReasonCounts.map(([name, count], index) => <div className="reason-bar" key={name}><span>{name}</span><b>{count}</b><i className={['red', 'orange', 'blue', 'gray'][index % 4]} style={{ width: `${count * 1.45}%` }} /></div>) : <div className="empty-state">No failure reasons available</div>}</div></section>
          <section className="panel dlq-activity">
            <h2>● DLQ Activity</h2>

  {activity.length > 0 ? (
    <>
      <div className="activity-total">
        <strong>
          {activity.reduce((sum, item) => sum + item.count, 0)}
        </strong>
        <span>dead-lettered messages</span>
      </div>

      <div className="activity-bars">
        {activity.map((item) => {
          const maxCount = Math.max(...activity.map((entry) => entry.count), 1);
          const width = (item.count / maxCount) * 100;

          return (

            <div className="activity-row" key={item.timestamp}>
              <span>
                {new Date(item.timestamp).toLocaleTimeString([], {
                  hour: '2-digit',
                  minute: '2-digit',
                  hour12: false,
                })}
              </span>
              <div className="activity-bar-track">
                <i style={{ width: `${width}%` }} />
              </div>
              <b>{item.count}</b>
            </div>
          );
        })}
      </div>

      <small>Last 24 hours · Live from API</small>
    </>
  ) : (
    <>
      <strong>No DLQ activity in the last 24 hours</strong>
      <p>Activity will appear here when messages enter the dead-letter queue.</p>
      <small>Live from API</small>
    </>
  )}
</section>
          <section className="panel failure-flow"><h2>Failure Flow</h2><div>PROCESSING FAILURE</div><span>↓</span><div className="flow-retry">RETRY 1　·　RETRY 2　·　RETRY 3</div><span>↓</span><div>DEAD LETTER QUEUE</div><span>↓</span><div className="flow-inspect">INSPECT</div></section>
        </div>
      </div>
    </DashboardShell>
  );
}
