/**
 * dashboard/src/App.jsx — Person 4 (Dashboard Layer)
 * ====================================================
 *
 * Role
 * ----
 * Root React component and layout shell for the LogFlow monitoring dashboard.
 * Renders the four metric panels as a responsive grid.
 * Manages global polling interval and API connection status.
 *
 * Child Components
 * ----------------
 *   ThroughputChart   — live line chart of messages/sec per service
 *   ConsumerLagPanel  — partition lag bars (4 partitions)
 *   ErrorRatePanel    — error rate % over time per service
 *   DLQViewer         — paginated table of dead-letter queue entries
 *
 * Data Flow
 * ---------
 *   App polls the FastAPI (Person 3) every POLL_INTERVAL_MS via client.js.
 *   On each tick, each panel receives fresh data as props.
 *   No Redux/Context needed at this scale — props drilling is sufficient.
 *
 * TODO (Person 4): implement polling logic, state management, and layout.
 */

import { useState, useEffect, useCallback } from 'react';
import ThroughputChart   from './components/ThroughputChart';
import ConsumerLagPanel  from './components/ConsumerLagPanel';
import ErrorRatePanel    from './components/ErrorRatePanel';
import DLQViewer         from './components/DLQViewer';
import {
  getThroughput,
  getConsumerLag,
  getErrorRates,
  getDLQMessages,
  healthCheck,
} from './api/client';

/** Polling interval in milliseconds (default: 5 seconds) */
const POLL_INTERVAL_MS = 5_000;

export default function App() {
  // ---------------------------------------------------------------------------
  // State
  // ---------------------------------------------------------------------------
  const [throughputData,  setThroughputData]  = useState(null);
  const [lagData,         setLagData]         = useState(null);
  const [errorData,       setErrorData]       = useState(null);
  const [dlqData,         setDlqData]         = useState(null);
  const [apiConnected,    setApiConnected]    = useState(false);
  const [lastUpdated,     setLastUpdated]     = useState(null);
  const [error,           setError]           = useState(null);

  // ---------------------------------------------------------------------------
  // Data fetch (called on each poll tick)
  // ---------------------------------------------------------------------------
  const fetchAll = useCallback(async () => {
    try {
      // TODO (Person 4): parallelise with Promise.all
      // const [throughput, lag, errors, dlq] = await Promise.all([
      //   getThroughput({ minutes: 60 }),
      //   getConsumerLag(),
      //   getErrorRates({ minutes: 60 }),
      //   getDLQMessages({ limit: 50 }),
      // ]);
      // setThroughputData(throughput);
      // setLagData(lag);
      // setErrorData(errors);
      // setDlqData(dlq);
      // setApiConnected(true);
      // setLastUpdated(new Date());
      // setError(null);
      console.log('[App] fetchAll stub — implement polling logic');
    } catch (err) {
      setApiConnected(false);
      setError(err.message);
    }
  }, []);

  // ---------------------------------------------------------------------------
  // Polling effect
  // ---------------------------------------------------------------------------
  useEffect(() => {
    fetchAll(); // initial fetch
    const timer = setInterval(fetchAll, POLL_INTERVAL_MS);
    return () => clearInterval(timer);
  }, [fetchAll]);

  // ---------------------------------------------------------------------------
  // Render — TODO (Person 4): replace stub layout with full dashboard UI
  // ---------------------------------------------------------------------------
  return (
    <div className="app-shell">
      <header className="app-header">
        <h1>LogFlow Dashboard</h1>
        <span className={`status-badge ${apiConnected ? 'connected' : 'disconnected'}`}>
          {apiConnected ? '● Live' : '○ Connecting…'}
        </span>
        {lastUpdated && (
          <span className="last-updated">
            Updated {lastUpdated.toLocaleTimeString()}
          </span>
        )}
      </header>

      {error && (
        <div className="error-banner" role="alert">
          ⚠ API unreachable: {error}. Is the FastAPI server running?
        </div>
      )}

      <main className="dashboard-grid">
        <section className="panel panel--wide">
          <ThroughputChart data={throughputData} />
        </section>
        <section className="panel">
          <ConsumerLagPanel data={lagData} />
        </section>
        <section className="panel">
          <ErrorRatePanel data={errorData} />
        </section>
        <section className="panel panel--wide">
          <DLQViewer data={dlqData} />
        </section>
      </main>
    </div>
  );
}
