import { useEffect, useState } from 'react';
import {
  getConsumerLag,
  getConsumerStatus,
  getDlqActivity,
  getDlqMessages,
  getHealth,
  startScenario,
  type ScenarioKey,
  getThroughput,
  type ConsumerLagResponse,
  type ConsumerStatusResponse,
  type DlqActivityResponse,
  type DlqResponse,
  type HealthResponse,
  type ThroughputResponse,
} from '../api';
import DashboardShell from '../components/DashboardShell';

type LiveScenarioData = {
  throughput: ThroughputResponse | null;
  lag: ConsumerLagResponse | null;
  consumers: ConsumerStatusResponse | null;
  dlq: DlqResponse | null;
  activity: DlqActivityResponse | null;
  health: HealthResponse | null;
  error: string | null;
};

const emptyData: LiveScenarioData = {
  throughput: null,
  lag: null,
  consumers: null,
  dlq: null,
  activity: null,
  health: null,
  error: null,
};

const scenarioCards = [
  { key: 'normal-load' as ScenarioKey, name: 'Normal Load', tag: 'BASELINE', description: 'Run the pipeline under normal traffic and verify stable processing.', expected: 'Consumers process messages normally with stable lag.', tone: 'normal' },
  { key: 'traffic-spike' as ScenarioKey, name: 'Traffic Spike', tag: 'LOAD TEST', description: 'Generate a sudden increase in traffic and observe consumer backpressure.', expected: 'Consumer lag may increase and partitions may pause/resume under backpressure.', tone: 'spike' },
  { key: 'malformed' as ScenarioKey, name: 'Malformed Logs', tag: 'DLQ TEST', description: 'Send malformed log messages and verify that invalid messages are routed to the DLQ.', expected: 'Invalid messages are isolated in the dead-letter queue.', tone: 'failure' },
  { key: 'slow-consumer' as ScenarioKey, name: 'Slow Consumer', tag: 'BACKPRESSURE', description: 'Simulate a slow consumer and observe consumer lag and backpressure.', expected: 'Consumer lag and backpressure are observable in live consumer metrics.', tone: 'slow' },
  { key: 'worker-failure' as ScenarioKey, name: 'Worker Failure', tag: 'FAILURE TEST', description: 'Simulate worker failure and verify Kafka consumer-group rebalancing.', expected: 'Partitions are reassigned after worker failure.', tone: 'failure' },
];

function formatFailureReason(reason: string | undefined): string {
  const normalized = reason?.toLowerCase() ?? '';

  if (
    normalized.includes('unknown_severity') ||
    normalized.includes('unknown-severity') ||
    normalized.includes('invalid severity') ||
    normalized.includes('severity enum')
  ) {
    return 'Invalid severity';
  }

  if (
    normalized.includes('invalid-trace-id') ||
    normalized.includes('invalid trace id') ||
    normalized.includes('trace_id') && normalized.includes('invalid')
  ) {
    return 'Invalid trace ID';
  }

  if (normalized.includes('missing-service') || normalized.includes('missing service')) {
    return 'Missing service';
  }

  if (
    normalized.includes('missing required') ||
    normalized.includes('required field')
  ) {
    return 'Missing required field';
  }

  if (
    normalized.includes('message-object') ||
    normalized.includes('message object') ||
    normalized.includes('malformed message') ||
    normalized.includes('message schema')
  ) {
    return 'Malformed message';
  }

  return 'Schema validation failed';
}

export default function TestScenariosPage() {
  const [data, setData] = useState(emptyData);
  const [lastUpdated, setLastUpdated] = useState('not yet');
  const [scenarioStatus, setScenarioStatus] = useState<Record<ScenarioKey, 'READY' | 'STARTING' | 'RUNNING' | 'FAILED'>>({
    'normal-load': 'READY',
    'traffic-spike': 'READY',
    malformed: 'READY',
    'slow-consumer': 'READY',
    'worker-failure': 'READY',
  });
  const [scenarioError, setScenarioError] = useState<Record<string, string>>({});

  useEffect(() => {
    const refresh = async () => {
      const [throughput, lag, consumers, dlq, activity, health] =
        await Promise.allSettled([
          getThroughput(5),
          getConsumerLag(),
          getConsumerStatus(),
          getDlqMessages(10),
          getDlqActivity(24),
          getHealth(),
        ]);

      const firstError = [throughput, lag, consumers, dlq, activity, health]
        .find((result) => result.status === 'rejected');

      setData({
        throughput: throughput.status === 'fulfilled' ? throughput.value : null,
        lag: lag.status === 'fulfilled' ? lag.value : null,
        consumers: consumers.status === 'fulfilled' ? consumers.value : null,
        dlq: dlq.status === 'fulfilled' ? dlq.value : null,
        activity: activity.status === 'fulfilled' ? activity.value : null,
        health: health.status === 'fulfilled' ? health.value : null,
        error: firstError?.status === 'rejected'
          ? firstError.reason instanceof Error
            ? firstError.reason.message
            : 'Live scenario metrics unavailable'
          : null,
      });
      setLastUpdated(new Date().toLocaleTimeString());
    };

    void refresh();
    const timer = window.setInterval(() => void refresh(), 5000);
    return () => window.clearInterval(timer);
  }, []);

  const throughput = data.throughput?.summary.current_rate;
  const totalLag = data.lag?.total_lag;
  const consumerStatus = data.consumers?.consumer.status;
  const rebalanceState = data.consumers?.rebalancing.state;
  const latestFailure = formatFailureReason(data.dlq?.messages[0]?.failure_reason);
  const dlqActivityCount = data.activity?.activity.reduce(
    (total, item) => total + item.count,
    0,
  );
  const liveStatus = data.health?.status === 'ok' || data.health?.status === 'healthy'
    ? 'ONLINE'
    : data.health?.status?.toUpperCase() ?? 'NOT AVAILABLE';

  const refreshLiveData = async () => {
    const [throughputResult, lagResult, consumersResult, dlqResult, activityResult, healthResult] =
      await Promise.allSettled([
        getThroughput(5),
        getConsumerLag(),
        getConsumerStatus(),
        getDlqMessages(10),
        getDlqActivity(24),
        getHealth(),
      ]);

    setData({
      throughput: throughputResult.status === 'fulfilled' ? throughputResult.value : data.throughput,
      lag: lagResult.status === 'fulfilled' ? lagResult.value : data.lag,
      consumers: consumersResult.status === 'fulfilled' ? consumersResult.value : data.consumers,
      dlq: dlqResult.status === 'fulfilled' ? dlqResult.value : data.dlq,
      activity: activityResult.status === 'fulfilled' ? activityResult.value : data.activity,
      health: healthResult.status === 'fulfilled' ? healthResult.value : data.health,
      error: null,
    });
    setLastUpdated(new Date().toLocaleTimeString());
  };

  const runScenario = async (scenario: ScenarioKey) => {
    setScenarioError((current) => ({ ...current, [scenario]: '' }));
    setScenarioStatus((current) => ({ ...current, [scenario]: 'STARTING' }));

    try {
      await startScenario(scenario);
      setScenarioStatus((current) => ({ ...current, [scenario]: 'RUNNING' }));
      await refreshLiveData();
    } catch (requestError) {
      setScenarioStatus((current) => ({ ...current, [scenario]: 'FAILED' }));
      setScenarioError((current) => ({
        ...current,
        [scenario]: requestError instanceof Error
          ? requestError.message
          : 'Failed to start scenario',
      }));
    }
  };

  return (
    <DashboardShell>
      <div className="page-frame scenario-page test-scenarios-page">
        <header className="page-header">
          <div><h1>Scenario Control Panel</h1><p>Run controlled load and fault-injection experiments against the LogFlow pipeline</p></div>
          <div className="header-actions"><span className="status-header">TEST ENVIRONMENT</span><span className="pill live">● {liveStatus}</span><span className="pill muted">Updated {lastUpdated}</span></div>
        </header>

        <section className="scenario-toolbar scenario-status-banner">
          <span className="status-tag info">● NO SCENARIO RUNNING</span>
          <span>Scenario controls are ready; no execution endpoint is available.</span>
          <span className="status-tag healthy">Kafka <strong>{liveStatus}</strong></span>
          <span className="status-tag healthy">Consumers <strong>{consumerStatus ?? 'NOT AVAILABLE'}</strong></span>
          <span className="status-tag healthy">API <strong>{data.health ? 'ONLINE' : 'NOT AVAILABLE'}</strong></span>
          <span className="status-tag healthy">Database <strong>{data.health?.database?.toUpperCase() ?? 'NOT AVAILABLE'}</strong></span>
          <button className="small-btn" onClick={() => window.location.assign('/')}>View Dashboard</button>
        </section>

        <section className="panel active-scenario">
          <div className="panel-title-row"><span>● LIVE SYSTEM OBSERVATION <span className="scenario-badge">READY</span></span></div>
          <div className="active-scenario-layout">
            <div>
              <div className="active-meta"><span>Scenario execution: Not available</span><span>Updated: {lastUpdated}</span></div>
              <div className="active-metrics">
                {[
                  ['CURRENT RATE', throughput === undefined ? 'Not available' : `${throughput.toFixed(2)} msg/s`],
                  ['CONSUMER LAG', totalLag === undefined ? 'Not available' : totalLag.toLocaleString()],
                  ['CONSUMER STATUS', consumerStatus ?? 'Not available'],
                  ['REBALANCING', rebalanceState ?? 'Not available'],
                  ['DLQ MESSAGES', data.dlq ? data.dlq.total.toLocaleString() : 'Not available'],
                  ['DLQ ACTIVITY', dlqActivityCount === undefined ? 'Not available' : dlqActivityCount.toLocaleString()],
                ].map(([label, value]) => <div key={label}><label>{label}</label><strong>{value}</strong></div>)}
              </div>
            </div>
            <div className="system-response"><label>LIVE SYSTEM RESPONSE</label><p>{data.error ?? 'Live metrics loaded from the LogFlow API.'}</p><p>Expected scenario outcomes are descriptive only.</p></div>
          </div>
        </section>

        <div className="scenario-section-heading"><h2>Test Scenarios</h2><span>Configure parameters and inject conditions below</span></div>
        <div className="scenario-grid scenario-card-grid">
          {scenarioCards.map((card) => (
            <section key={card.name} className="scenario-card detailed-scenario-card">
              <div className="scenario-head"><span>{card.name}</span><span className="scenario-status">{scenarioStatus[card.key]}</span></div>
              <span className={`scenario-badge ${card.tone}`}>{card.tag}</span>
              <p>{card.description}</p>
              <div className="scenario-inputs">
                {card.name === 'Malformed Logs' ? (
                  <>
                    <div><label>DLQ MESSAGES</label><strong>{data.dlq?.total.toLocaleString() ?? 'Not available'}</strong></div>
                    <div><label>RECENT FAILURE</label><strong>{latestFailure ?? 'Not available'}</strong></div>
                    <div><label>DLQ ACTIVITY</label><strong>{dlqActivityCount?.toLocaleString() ?? 'Not available'}</strong></div>
                  </>
                ) : (
                  <>
                    <div><label>THROUGHPUT</label><strong>{throughput === undefined ? 'Not available' : `${throughput.toFixed(2)} msg/s`}</strong></div>
                    <div><label>CONSUMER LAG</label><strong>{totalLag === undefined ? 'Not available' : totalLag.toLocaleString()}</strong></div>
                    <div><label>CONSUMER STATUS</label><strong>{consumerStatus ?? 'Not available'}</strong></div>
                  </>
                )}
              </div>
              <div className={`expected-text ${card.tone}`}>{card.expected}</div>
              <div className="scenario-actions">
                <button
                  className={`scenario-btn ${card.tone}`}
                  disabled={scenarioStatus[card.key] === 'STARTING'}
                  onClick={() => void runScenario(card.key)}
                >
                  {scenarioStatus[card.key] === 'STARTING'
                    ? 'STARTING...'
                    : scenarioStatus[card.key] === 'RUNNING'
                      ? 'RUNNING'
                      : scenarioStatus[card.key] === 'FAILED'
                        ? 'RETRY'
                        : 'READY TO RUN'}
                </button>
              </div>
              {scenarioError[card.key] && (
                <small className="danger-text">{scenarioError[card.key]}</small>
              )}
            </section>
          ))}
        </div>

        <section className="panel recent-runs"><div className="panel-title-row"><span>LIVE SCENARIO DATA</span></div><p className="empty-state">No scenario execution history is exposed by the current API.</p></section>
        <p className="scenario-footnote">Test execution results are unavailable until a scenario control endpoint is provided.</p>
      </div>
    </DashboardShell>
  );
}
