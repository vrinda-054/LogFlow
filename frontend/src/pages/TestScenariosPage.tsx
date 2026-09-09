import { useEffect, useState } from 'react';
import DashboardShell from '../components/DashboardShell';

type ScenarioStatus = 'READY' | 'RUNNING' | 'PASSED' | 'STOPPED';

type ScenarioState = {
  status: ScenarioStatus;
  name: string;
  elapsed: number;
  throughput: number;
  lag: number;
  generated: number;
  processed: number;
  errors: number;
  dlq: number;
  selectedConsumer: string;
};

const initialState: ScenarioState = {
  status: 'RUNNING',
  name: 'Slow Consumer',
  elapsed: 22,
  throughput: 892,
  lag: 1869,
  generated: 26760,
  processed: 24918,
  errors: 0,
  dlq: 0,
  selectedConsumer: 'Consumer 3',
};

const scenarioCards = [
  { name: 'Normal Load', tag: 'BASELINE', description: 'Verify correct processing under steady traffic.', metrics: ['Message rate|1000|msg/s', 'Duration|30|seconds'], expected: 'Stable throughput · low consumer lag', action: 'START TEST', tone: 'normal' },
  { name: 'Traffic Spike', tag: 'LOAD TEST', description: 'Generate a sudden increase in traffic and observe Kafka buffering and consumer lag.', metrics: ['Spike rate|5000|msg/s', 'Duration|30|seconds', 'Ramp-up|Immediate|'], expected: 'Kafka buffers incoming traffic · consumer lag increases · pipeline recovers after spike', action: 'INJECT SPIKE', tone: 'spike' },
  { name: 'Slow Consumer', tag: 'BACKPRESSURE', description: 'Artificially delay a consumer to demonstrate lag-based backpressure.', metrics: ['Target consumer|Consumer 3|', 'Processing delay|500|ms/msg', 'Trigger threshold|1500|messages'], expected: 'Consumer slows → lag increases → HW threshold exceeded → consumption paused', action: 'STOP TEST', tone: 'slow' },
  { name: 'Malformed Log Injection', tag: 'FAULT INJECTION', description: 'Inject malformed messages to verify retry and Dead Letter Queue handling.', metrics: ['Malformed %|10|%', 'Message rate|1000|msg/s', 'Duration|30|seconds'], expected: 'Message → failed messages → retries → DLQ', action: 'INJECT MALFORMED LOGS', tone: 'failure' },
];

const recentRuns = [
  ['Slow Consumer', '18:47', '30s', '892 msg/s', '1,842', '0', '0', 'RUNNING'],
  ['Traffic Spike', '18:42', '30s', '4,823 msg/s', '2,431', '0', '0', 'PASSED'],
  ['Malformed Logs', '18:35', '30s', '1,002 msg/s', '182', '127', '127', 'PASSED'],
  ['Worker Failure', '18:27', '45s', '2,184 msg/s', '913', '0', '0', 'PASSED'],
];

export default function TestScenariosPage() {
  const [state, setState] = useState(initialState);

  useEffect(() => {
    if (state.status !== 'RUNNING') return undefined;
    const timer = window.setInterval(() => setState((current) => ({
      ...current,
      elapsed: Math.min(30, current.elapsed + 1),
      throughput: current.throughput + 18,
      lag: current.lag + 11,
      generated: current.generated + current.throughput,
      processed: current.processed + Math.max(0, current.throughput - 11),
    })), 1000);
    return () => window.clearInterval(timer);
  }, [state.status]);

  const startScenario = (name: string) => setState((current) => ({
    ...current,
    status: 'RUNNING',
    name,
    elapsed: 0,
    throughput: name === 'Traffic Spike' ? 4823 : name === 'Normal Load' ? 1000 : 892,
    lag: name === 'Slow Consumer' ? 420 : 182,
    generated: 0,
    processed: 0,
    errors: name === 'Malformed Log Injection' ? 127 : 0,
    dlq: name === 'Malformed Log Injection' ? 127 : 0,
  }));

  const reset = () => setState({ ...initialState, status: 'READY', elapsed: 0 });
  const stop = () => setState((current) => ({ ...current, status: 'STOPPED' }));
  const inject = (name: string) => setState((current) => ({
    ...current,
    status: 'RUNNING',
    name,
    errors: name === 'Malformed Log Injection' ? current.errors + 35 : current.errors + 8,
    dlq: name === 'Malformed Log Injection' ? current.dlq + 16 : current.dlq,
    lag: current.lag + 600,
  }));

  return (
    <DashboardShell>
      <div className="page-frame scenario-page test-scenarios-page">
        <header className="page-header">
          <div><h1>Scenario Control Panel</h1><p>Run controlled load and fault-injection experiments against the LogFlow pipeline</p></div>
          <div className="header-actions"><span className="status-header">TEST ENVIRONMENT</span><span className="pill live">● READY</span><span className="pill muted">Last test: Traffic Spike — 18:42</span></div>
        </header>

        <section className="scenario-toolbar scenario-status-banner">
          <span className="status-tag warning">● TEST {state.status}</span>
          <span>{state.name} scenario in progress — {state.selectedConsumer}</span>
          <span className="status-tag healthy">Kafka <strong>ONLINE</strong></span>
          <span className="status-tag healthy">Consumers <strong>3 / 3 ACTIVE</strong></span>
          <span className="status-tag healthy">API <strong>ONLINE</strong></span>
          <span className="status-tag healthy">Database <strong>ONLINE</strong></span>
          <button className="small-btn" onClick={() => window.location.assign('/')}>View Dashboard</button>
        </section>

        <section className={`panel active-scenario ${state.status === 'RUNNING' ? 'scenario-running' : ''}`}>
          <div className="panel-title-row"><span>● ACTIVE TEST &nbsp; {state.name.toUpperCase()} <span className="scenario-badge">{state.status}</span></span><button className="danger-button" onClick={stop}>■ STOP TEST</button></div>
          <div className="scenario-progress-label"><span>ELAPSED</span><strong>{String(state.elapsed).padStart(2, '0')}s / 00:30</strong></div>
          <div className="progress-track"><i style={{ width: `${Math.min(100, state.elapsed / 30 * 100)}%` }} /></div>
          <div className="active-scenario-layout">
            <div>
              <div className="active-meta"><span>Target Consumer: {state.selectedConsumer}</span><span>Processing Delay: 500ms/msg</span></div>
              <div className="active-metrics">
                {[
                  ['CURRENT RATE', `${state.throughput} msg/s`], ['CONSUMER LAG', state.lag.toLocaleString()], ['MSGS GENERATED', state.generated.toLocaleString()],
                  ['MSGS PROCESSED', state.processed.toLocaleString()], ['ERRORS', String(state.errors)], ['DLQ', String(state.dlq)],
                ].map(([label, value]) => <div key={label}><label>{label}</label><strong>{value}</strong></div>)}
              </div>
            </div>
            <div className="system-response"><label>LIVE SYSTEM RESPONSE</label><p>18:47:02 &nbsp; <strong>Slow consumer test started — {state.selectedConsumer}</strong></p><p>18:47:03 &nbsp; Processing delay applied: 500ms/message</p><p>18:47:09 &nbsp; Consumer lag increasing: 412</p><p className="orange-text">18:47:19 &nbsp; High-water threshold exceeded: 1,842 &gt; 1,500</p><p className="danger-text">18:47:20 &nbsp; Backpressure ACTIVE — consumption PAUSED</p></div>
          </div>
        </section>

        <div className="scenario-section-heading"><h2>Test Scenarios</h2><span>Configure parameters and inject conditions below</span></div>
        <div className="scenario-grid scenario-card-grid">
          {scenarioCards.map((card) => (
            <section key={card.name} className={`scenario-card detailed-scenario-card ${card.tone === 'slow' && state.status === 'RUNNING' ? 'active' : ''}`}>
              <div className="scenario-head"><span>{card.name}</span><span className="scenario-status">{card.tone === 'slow' && state.status === 'RUNNING' ? '● RUNNING' : 'READY'}</span></div>
              <span className={`scenario-badge ${card.tone}`}>{card.tag}</span>
              <p>{card.description}</p>
              <div className="scenario-inputs">{card.metrics.map((metric) => { const [label, value, suffix] = metric.split('|'); return <div key={label}><label>{label}</label><strong>{value}</strong><small>{suffix}</small></div>; })}</div>
              <div className={`expected-text ${card.tone}`}>{card.expected}</div>
              <div className="scenario-actions"><button className={`scenario-btn ${card.tone}`} onClick={() => (card.tone === 'slow' ? stop() : inject(card.name))}>{card.action}</button><button className="small-btn" onClick={reset}>Reset</button></div>
            </section>
          ))}
          <section className="scenario-card detailed-scenario-card worker-failure-card">
            <div className="scenario-head"><span>⚙ Worker Failure</span><span className="scenario-status">READY</span></div>
            <span className="scenario-badge failure">FAILURE TEST</span><p>Stop a consumer and verify Kafka consumer-group rebalancing.</p>
            <div className="failure-warning">◉ This will intentionally stop the selected consumer.</div>
            <div className="failure-grid"><div><label>TARGET CONSUMER</label><select value={state.selectedConsumer} onChange={(event) => setState((current) => ({ ...current, selectedConsumer: event.target.value }))}><option>Consumer 1</option><option>Consumer 2</option><option>Consumer 3</option></select></div><div><label>BEFORE → AFTER REBALANCE</label><p>P0 → C1<br />P1 → C2<br />P2 → C3</p></div><div><label>FLOW</label><p className="danger-text">C2 STOPPED<br /><span className="orange-text">REBALANCING</span><br /><span className="live-text">GROUP STABLE</span></p></div></div>
            <button className="danger-button" onClick={() => inject('Worker Failure')}>⚠ KILL CONSUMER</button>
          </section>
        </div>

        <section className="panel recent-runs"><div className="panel-title-row"><span>◉ Recent Test Runs</span><button className="small-btn">View all ↗</button></div><table><thead><tr><th>SCENARIO</th><th>STARTED</th><th>DURATION</th><th>PEAK RATE</th><th>PEAK LAG</th><th>ERRORS</th><th>DLQ</th><th>RESULT</th></tr></thead><tbody>{recentRuns.map((run) => <tr key={run[0]}>{run.map((value, index) => <td key={`${run[0]}-${index}`} className={index === 7 ? value.toLowerCase() : ''}>{value}</td>)}</tr>)}</tbody></table></section>
        <p className="scenario-footnote">⚙ Test Environment — These controls intentionally generate load and failure conditions. Use only against the LogFlow test environment.</p>
      </div>
    </DashboardShell>
  );
}
