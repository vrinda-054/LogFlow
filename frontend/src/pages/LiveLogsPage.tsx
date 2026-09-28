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

type ApiLog = {
  id: number;
  ingested_at: string;
  timestamp: string;
  service: string;
  severity: string;
  message: string;
  trace_id: string;
};

const API_BASE_URL =
  import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';

let storedLogs: LiveLog[] = [];
let pollTimer: number | null = null;
let polling = false;
let listeners = new Set<() => void>();

function formatLocalTime(date: Date) {
  return date.toLocaleTimeString([], {
    hour12: false,
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

function normalizeLog(log: ApiLog): LiveLog {
  const level =
    log.severity === 'ERROR'
      ? 'ERROR'
      : log.severity === 'WARN' || log.severity === 'WARNING'
        ? 'WARN'
        : 'INFO';

  return {
    id: log.id,
    timestamp: new Date(log.timestamp).toLocaleTimeString([], {
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    }),
    level,
    service: log.service.toUpperCase(),
    consumer: '—',
    partition: '—',
    trace: log.trace_id || '—',
    message: log.message,
    payload: JSON.stringify(log, null, 2),
    status: level === 'ERROR' ? 'failed' : 'processed',
  };
}

function notifyListeners() {
  listeners.forEach((listener) => listener());
}

async function fetchLogs() {
  try {
    const response = await fetch(`${API_BASE_URL}/logs?limit=100`);

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = (await response.json()) as { logs?: ApiLog[] };

    if (!Array.isArray(data.logs)) {
      return;
    }

    storedLogs = data.logs
      .map(normalizeLog)
      .sort((a, b) => b.id - a.id)
      .slice(0, 100);

    notifyListeners();
  } catch (error) {
    console.warn('Failed to fetch live logs:', error);
  }
}

function startPolling() {
  if (polling) {
    return;
  }

  polling = true;

  void fetchLogs();

  pollTimer = window.setInterval(() => {
    void fetchLogs();
  }, 3000);
}

function stopPolling() {
  if (pollTimer !== null) {
    window.clearInterval(pollTimer);
    pollTimer = null;
  }

  polling = false;
}

function subscribe(listener: () => void) {
  listeners.add(listener);
  startPolling();

  return () => {
    listeners.delete(listener);
  };
}

function clearStoredLogs() {
  storedLogs = [];
  notifyListeners();
}

export default function LiveLogsPage() {
  const [, refresh] = useState(0);
  const [lastUpdated, setLastUpdated] = useState(() =>
    formatLocalTime(new Date()),
  );
  const [live, setLive] = useState(true);
  const [autoScroll, setAutoScroll] = useState(true);
  const [levelFilter, setLevelFilter] = useState<'ALL' | LogLevel>('ALL');
  const [serviceFilter, setServiceFilter] = useState('ALL');
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedId, setSelectedId] = useState<number | null>(null);

  useEffect(() => {
    const unsubscribe = subscribe(() => {
      refresh((value) => value + 1);
      setLastUpdated(formatLocalTime(new Date()));
    });

    return unsubscribe;
  }, []);

  useEffect(() => {
  if (live) {
    startPolling();
  } else {
    stopPolling();
  }
}, [live]);


  useEffect(() => {
    if (selectedId === null && storedLogs.length > 0) {
      setSelectedId(storedLogs[0].id);
    }
  }, [selectedId]);

  const logs = storedLogs;

  const services = useMemo(
    () => Array.from(new Set(logs.map((entry) => entry.service))).sort(),
    [logs],
  );

  const visibleLogs = useMemo(
    () =>
      logs.filter((entry) => {
        const text =
          `${entry.message} ${entry.service} ${entry.trace}`.toLowerCase();

        return (
          (levelFilter === 'ALL' || entry.level === levelFilter) &&
          (serviceFilter === 'ALL' || entry.service === serviceFilter) &&
          (!searchTerm || text.includes(searchTerm.toLowerCase()))
        );
      }),
    [levelFilter, logs, searchTerm, serviceFilter],
  );

  const selectedLog =
    visibleLogs.find((entry) => entry.id === selectedId) ??
    visibleLogs[0] ??
    logs[0];

  const errors = logs.filter((entry) => entry.level === 'ERROR').length;

  const exportLogs = () => {
    const url = URL.createObjectURL(
      new Blob([JSON.stringify(visibleLogs, null, 2)], {
        type: 'application/json',
      }),
    );

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
            <span className="muted">Last updated: {lastUpdated}</span>
            <button
              className={`toggle-btn ${live ? '' : 'paused'}`}
              onClick={() => setLive((value) => !value)}
            >
              ● {live ? 'LIVE' : 'PAUSED'}
            </button>
          </div>
        </header>

        <div className="stats-row live-stats">
          <div className="mini-stat">
            <strong>LIVE</strong>
            <span>Log Stream</span>
          </div>

          <div className="mini-stat">
            <strong>{logs.length}</strong>
            <span>Recent Logs</span>
          </div>

          <div className="mini-stat danger-stat">
            <strong>{errors}</strong>
            <span>Errors</span>
          </div>

          <div className="mini-stat live-stat">
            <strong>● {live ? 'LIVE' : 'PAUSED'}</strong>
            <span>Stream Status</span>
          </div>
        </div>

        <section className="panel live-toolbar">
          <button
            className="toggle-btn"
            onClick={() => setLive((value) => !value)}
          >
            ● {live ? 'LIVE' : 'PAUSED'}
          </button>

          <button
            className="small-btn"
            onClick={() => setAutoScroll((value) => !value)}
          >
            Auto Scroll {autoScroll ? 'ON' : 'OFF'}
          </button>

          {(['ALL', 'INFO', 'WARN', 'ERROR'] as const).map((level) => (
            <button
              key={level}
              className={`small-btn ${
                levelFilter === level ? 'selected-filter' : ''
              }`}
              onClick={() => setLevelFilter(level)}
            >
              Level {level}
            </button>
          ))}

          <select
            value={serviceFilter}
            onChange={(event) => setServiceFilter(event.target.value)}
          >
            <option value="ALL">ALL SERVICES</option>
            {services.map((service) => (
              <option key={service} value={service}>
                {service}
              </option>
            ))}
          </select>

          <input
            value={searchTerm}
            onChange={(event) => setSearchTerm(event.target.value)}
            placeholder="Search logs, trace ID, service..."
          />

          <button
            className="small-btn"
            onClick={() => {
              clearStoredLogs();
              setSelectedId(null);
            }}
          >
            Clear Stream
          </button>

          <button className="small-btn" onClick={exportLogs}>
            ↑ Export
          </button>
        </section>

        <div className="logs-shell live-log-content">
          <section className="panel stream-panel">
            <div className="panel-title-row">
              <span>LIVE EVENTS</span>
              <span className="status-tag healthy">
                {live ? 'STREAMING' : 'PAUSED'}
              </span>
            </div>

            <div className="log-table-wrap">
              <table className="log-table live-log-table">
                <thead>
                  <tr>
                    <th>TIMESTAMP</th>
                    <th>LEVEL</th>
                    <th>SERVICE</th>
                    <th>CON.</th>
                    <th>TRACE ID</th>
                    <th>MESSAGE</th>
                  </tr>
                </thead>

                <tbody>
                  {visibleLogs.map((row) => (
                    <tr
                      key={row.id}
                      className={`${selectedLog?.id === row.id ? 'selected-row ' : ''}${
                        row.level === 'ERROR' ? 'error-row' : ''
                      }`}
                      onClick={() => setSelectedId(row.id)}
                    >
                      <td>{row.timestamp}</td>
                      <td>
                        <span
                          className={`level level-${row.level.toLowerCase()}`}
                        >
                          {row.level}
                        </span>
                      </td>
                      <td>
                        <span className="service-badge">{row.service}</span>
                      </td>
                      <td>{row.consumer}</td>
                      <td className="trace-id">{row.trace}</td>
                      <td>{row.message}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            <div className="live-log-footer">
              Showing latest {visibleLogs.length} events · Errors:{' '}
              <strong className="danger-text">{errors}</strong>
            </div>
          </section>

          <aside className="side-panel log-detail">
            <div className="detail-card">
              <div className="detail-header">Selected event</div>

              {selectedLog && (
                <>
                  <div className="detail-grid">
                    <div>
                      <label>TIMESTAMP</label>
                      <strong>{selectedLog.timestamp}</strong>
                    </div>

                    <div>
                      <label>SERVICE</label>
                      <strong>{selectedLog.service}</strong>
                    </div>

                    <div>
                      <label>CONSUMER</label>
                      <strong>{selectedLog.consumer}</strong>
                    </div>

                    <div>
                      <label>PARTITION</label>
                      <strong>{selectedLog.partition}</strong>
                    </div>

                    <div>
                      <label>TRACE ID</label>
                      <strong className="trace-id">
                        {selectedLog.trace}
                      </strong>
                    </div>

                    <div>
                      <label>PROCESSING STATUS</label>
                      <strong
                        className={
                          selectedLog.status === 'failed'
                            ? 'danger-text'
                            : 'live-text'
                        }
                      >
                        {selectedLog.status.toUpperCase()}
                      </strong>
                    </div>
                  </div>

                  <div className="detail-section-label">MESSAGE</div>
                  <p className="selected-message">{selectedLog.message}</p>

                  <div className="detail-section-label">
                    RAW JSON{' '}
                    <button
                      className="mini-button copy-button"
                      onClick={() =>
                        navigator.clipboard?.writeText(selectedLog.payload)
                      }
                    >
                      Copy JSON
                    </button>
                  </div>

                  <pre className="payload-box">{selectedLog.payload}</pre>
                </>
              )}
            </div>
          </aside>
        </div>

        <div className="bottom-bar">
          {['KAFKA', 'CONSUMER GROUP', 'PROCESSING'].map((label) => (
            <div key={label} className="pipeline-card">
              <span>{label}</span>
              <strong>LIVE</strong>
              <small>● HEALTHY</small>
            </div>
          ))}
        </div>
      </div>
    </DashboardShell>
  );
}
