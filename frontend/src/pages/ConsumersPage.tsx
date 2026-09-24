import { useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  getConsumerEvents,
  getConsumerLag,
  getConsumerStatus,
  getThroughput,
  type ConsumerEvent,
  type RebalanceEvent,
  type ConsumerLagResponse,
  type PartitionStatus,
  type ConsumerStatusResponse,
  type ThroughputResponse,
} from '../api';
import DashboardShell from '../components/DashboardShell';

type ConsumerView = {
  id: string;
  status: 'RUNNING' | 'PAUSED';
  rate: number;
  lag: number;
  heartbeat: string;
  partitions: number[];
  backpressure?: string;
};

function formatClock(value: Date): string {
  return value.toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });
}

function formatUpdatedAt(value: Date | null): string {
  if (!value) {
    return '—';
  }

  return value.toLocaleString([], {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });
}

const emptyLag: ConsumerLagResponse = {
  total_lag: 0,
  partitions: [],
};

const emptyStatus: ConsumerStatusResponse = {
  consumer: {
    consumer_id: '',
    status: 'UNKNOWN',
    assigned_partitions: [],
    consumer_lag: 0,
    processing_rate: 0,
    last_heartbeat: '',
    backpressure_active: false,
},
  partitions: [],
  rebalancing: {
    state: 'STABLE',
    current_assignment: [],
    after_recovery: 'AUTOMATIC_REBALANCE',
  },
};

const emptyThroughput: ThroughputResponse = {
  windows: [],
  summary: {
    current_rate: 0,
    peak_rate: 0,
    average_rate: 0,
    total_windows: 0,
  },
};

function toConsumers(
  status: ConsumerStatusResponse,
  lagData: ConsumerLagResponse,
): ConsumerView[] {
  const lagMap = new Map(
    lagData.partitions.map((item) => [item.partition_id, item.lag]),
  );

  const known = new Map<string, ConsumerView>();
  status.partitions.forEach((partition) => {
    const id = partition.assigned_consumer;
    if (!['consumer-01', 'consumer-02', 'consumer-03'].includes(id)) {
      return;
    }
    const existing = known.get(id) ?? {
      id,
      status: 'RUNNING',
      rate: 0,
      lag: 0,
      heartbeat: status.consumer.last_heartbeat
        ? new Date(status.consumer.last_heartbeat).toLocaleTimeString()
        : 'Unavailable',
      partitions: [],
    };

    existing.rate += partition.throughput;

    const partitionLag = lagMap.get(partition.partition);
    if (partitionLag !== undefined) {
      existing.lag += partitionLag;
    }

    existing.partitions.push(partition.partition);

    if (partition.health === 'UNHEALTHY') {
      existing.status = 'PAUSED';
    }

    known.set(id, existing);
  });

  return Array.from(known.values());
}

export default function ConsumersPage() {
  const navigate = useNavigate();
  const [lagData, setLagData] = useState(emptyLag);
  const [status, setStatus] = useState(emptyStatus);
  const previousStatusRef = useRef(emptyStatus);
  const [rebalanceBefore, setRebalanceBefore] = useState<PartitionStatus[]>([]);
  const [throughput, setThroughput] = useState(emptyThroughput);
  const [consumers, setConsumers] = useState<ConsumerView[]>([]);
  const [lastUpdated, setLastUpdated] = useState('—');
  const [lastUpdatedAt, setLastUpdatedAt] = useState<Date | null>(null);
  const [isLive, setIsLive] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [consumerEvents, setConsumerEvents] = useState<ConsumerEvent[]>([]);
  const [rebalanceEvents, setRebalanceEvents] = useState<RebalanceEvent[]>([]);
  const [consumerEventsError, setConsumerEventsError] = useState<string | null>(null);
  const [eventsLive, setEventsLive] = useState(false);
  const [selectedPartition, setSelectedPartition] = useState(0);

  const refresh = async () => {
    const [lagResult, statusResult, throughputResult, eventsResult] =
      await Promise.allSettled([
        getConsumerLag(),
        getConsumerStatus(),
        getThroughput(5),
        getConsumerEvents(20),
      ]);

    if (lagResult.status === 'fulfilled') {
      setLagData(lagResult.value);
    }

    if (statusResult.status === 'fulfilled') {
      const nextPartitions = statusResult.value.partitions.filter(
        (partition) =>
          partition.assigned_consumer !== 'dlq-fast' &&
          partition.assigned_consumer !== 'dlq-test',
      );
      const currentPartitions = previousStatusRef.current.partitions.filter(
        (partition) =>
          partition.assigned_consumer !== 'dlq-fast' &&
          partition.assigned_consumer !== 'dlq-test',
      );
      const assignmentsChanged = currentPartitions.some((partition) => {
        const next = nextPartitions.find(
          (candidate) => candidate.partition === partition.partition,
        );
        return next?.assigned_consumer !== partition.assigned_consumer;
      });

      if (assignmentsChanged) {
        setRebalanceBefore((previous) =>
          previous.length > 0 ? previous : currentPartitions,
        );
      }
      previousStatusRef.current = statusResult.value;
      setStatus(statusResult.value);
    }

    if (throughputResult.status === 'fulfilled') {
      setThroughput(throughputResult.value);
    }

    if (eventsResult.status === 'fulfilled') {
      setConsumerEvents(eventsResult.value.events ?? []);
      setRebalanceEvents(eventsResult.value.rebalance_events ?? []);
      setConsumerEventsError(null);
      setEventsLive(true);
    } else {
      setConsumerEvents([]);
      setRebalanceEvents([]);
      setConsumerEventsError('Consumer group event data unavailable.');
      setEventsLive(false);
    }

    if (
      lagResult.status === 'fulfilled' &&
      statusResult.status === 'fulfilled' &&
      throughputResult.status === 'fulfilled'
    ) {
      setConsumers(
        toConsumers(statusResult.value, lagResult.value),
      );
      setIsLive(true);
      setError(null);
      setLastUpdated('just now');
      setLastUpdatedAt(new Date());
    } else {
      const reasons = [
        lagResult.status === 'rejected' ? lagResult.reason : null,
        statusResult.status === 'rejected' ? statusResult.reason : null,
        throughputResult.status === 'rejected'
          ? throughputResult.reason
          : null,
      ].filter(Boolean);

      setIsLive(false);

      const firstReason = reasons[0];

      setError(
        firstReason instanceof Error
          ? firstReason.message
          : 'One or more live metrics are unavailable',
      );
    }
  };

  useEffect(() => {
    void refresh();

    const timer = window.setInterval(() => {
      void refresh();
    }, 5000);

    return () => window.clearInterval(timer);
  }, []);

  const partitionMap = useMemo(
    () =>
      new Map(
        status.partitions.map((item) => [item.partition, item]),
      ),
    [status.partitions],
  );

  const mainPartitions = useMemo(() => {
    const mainConsumerIds = new Set([
      'consumer-01',
      'consumer-02',
      'consumer-03',
    ]);
    const uniquePartitions = new Map<
      number,
      (typeof status.partitions)[number]
    >();

    status.partitions.forEach((partition) => {
      if (
        mainConsumerIds.has(partition.assigned_consumer) &&
        !uniquePartitions.has(partition.partition)
      ) {
        uniquePartitions.set(partition.partition, partition);
      }
    });

    return Array.from(uniquePartitions.values()).sort(
      (first, second) => first.partition - second.partition,
    );
  }, [status.partitions]);

  const lagMap = useMemo(
    () =>
      new Map(
        lagData.partitions.map((item) => [
          item.partition_id,
          item.lag,
        ]),
      ),
    [lagData.partitions],
  );

  const maxPartitionLag = Math.max(
    0,
    ...mainPartitions.map(
      (partition) => Math.max(0, lagMap.get(partition.partition) ?? 0),
    ),
  );

  const activeConsumers = consumers.filter(
    (consumer) => consumer.status === 'RUNNING',
  ).length;

  const totalConsumers = consumers.length;
  const totalPartitions = mainPartitions.length;

  const totalThroughput = throughput.summary.current_rate;

  const hasConsumerEvents = consumerEvents.length > 0;

  const currentAssignments = useMemo(
    () =>
      status.partitions.filter(
        (partition) =>
          partition.assigned_consumer !== 'dlq-fast' &&
          partition.assigned_consumer !== 'dlq-test',
      ),
    [status.partitions],
  );

  const movedPartitions = useMemo(() => {
    if (rebalanceBefore.length === 0) return new Set<number>();

    return new Set(
      currentAssignments
        .filter((partition) => {
          const before = rebalanceBefore.find(
            (candidate) => candidate.partition === partition.partition,
          );
          return before && before.assigned_consumer !== partition.assigned_consumer;
        })
        .map((partition) => partition.partition),
    );
  }, [currentAssignments, rebalanceBefore]);

  const lifecycleState = status.rebalancing.state.toUpperCase();
  const hasRecovered = rebalanceBefore.length > 0 && lifecycleState === 'STABLE';

  const healthyPartitions = mainPartitions.filter(
    (partition) => partition.health === 'HEALTHY',
  ).length;

  const getPartitionLag = (partition: number): number | null => {
    return lagMap.get(partition) ?? null;
  };

  const selectedLag = getPartitionLag(selectedPartition);

  return (
    <DashboardShell>
      <div className="page-frame consumer-page">
        <header className="page-header">
          <div>
            <h1>Consumer &amp; Partition View</h1>
            <p>
              Live Kafka consumer-group activity and partition assignment
            </p>
          </div>

          <div className="header-actions consumer-header-actions">
            <div className="header-meta consumer-header-meta consumer-group-meta">
              <span>Consumer Group</span>
              <strong>logflow-consumer-group</strong>
            </div>

            <div className="header-meta consumer-header-meta">
              <span>Status</span>
              <strong className={isLive ? 'status-live' : ''}>
                ● {status.rebalancing.state}
              </strong>
            </div>

            <div className="header-meta consumer-header-meta">
              <span>Last updated</span>
              <strong>• {lastUpdated}</strong>
            </div>

            <div className="header-meta consumer-header-meta">
              <span>Timestamp</span>
              <strong>{formatUpdatedAt(lastUpdatedAt)}</strong>
            </div>
          </div>
        </header>

        {error && (
          <div className="error-banner" role="alert">
            Live metric issue: {error}
          </div>
        )}

        <div className="summary-row">
          <div className="summary-box active">
            <span>ACTIVE CONSUMERS</span>
            <strong>
              {activeConsumers} / {totalConsumers}
            </strong>
            <b>
              {activeConsumers === totalConsumers && totalConsumers > 0
                ? 'HEALTHY'
                : 'WARNING'}
            </b>
          </div>

          <div className="summary-box">
            <span>TOTAL PARTITIONS</span>
            <strong>{totalPartitions}</strong>
            <b className="blue-text">ASSIGNED</b>
          </div>

          <div className="summary-box">
            <span>TOTAL THROUGHPUT</span>
            <strong>
              {totalThroughput.toFixed(2)} msg/s
            </strong>
            <b>
              {healthyPartitions === totalPartitions &&
              totalPartitions > 0
                ? 'HEALTHY'
                : 'WARNING'}
            </b>
          </div>

          <div className="summary-box warning">
            <span>MESSAGES BEHIND</span>
            <strong>{lagData.total_lag.toLocaleString()}</strong>
            <b className={lagData.total_lag > 1500 ? 'orange-text' : ''}>
              {lagData.total_lag > 1500 ? 'WARNING' : 'HEALTHY'}
            </b>
          </div>
        </div>

        <div className="consumer-view-layout">
          <div className="consumer-instance-list">
            <div className="section-heading">
              <h2>Consumer Instances</h2>
              <span className="updating">
                ● {isLive ? 'updating live' : 'connecting'}
              </span>
            </div>

            {consumers.map((consumer, index) => (
              <article
                key={consumer.id}
                className={`consumer-card consumer-row ${
                  consumer.status === 'PAUSED' ? 'paused-card' : ''
                }`}
              >
                <div className="consumer-number">
                  {String(index + 1).padStart(2, '0')}
                </div>

                <div className="consumer-identity">
                  <div className="health-header">
                    <div>
                      <h3>● {consumer.id}</h3>
                      <span className="consumer-subtitle">
                        Main Consumer
                      </span>
                    </div>

                    <span
                      className={`status-tag ${
                        consumer.status === 'RUNNING'
                          ? 'running'
                          : 'warning-tag'
                      }`}
                    >
                      {consumer.status}
                    </span>
                  </div>
                </div>

                {consumer.backpressure && (
                  <div className="backpressure-box">
                    <strong>⚠ BACKPRESSURE ACTIVE</strong>
                    <span>{consumer.backpressure}</span>
                  </div>
                )}

                <div className="consumer-body">
                  <div>
                    <label>Processing rate</label>
                    <strong
                      className={
                        consumer.status === 'PAUSED'
                          ? 'orange-text'
                          : ''
                      }
                    >
                      {consumer.rate.toFixed(2)} msg/s
                    </strong>
                  </div>

                  <div>
                    <label>Consumer lag</label>
                    <strong
                      className={
                        consumer.lag > 1500
                          ? 'danger-text'
                          : ''
                      }
                    >
                      {consumer.lag.toLocaleString()}
                    </strong>
                  </div>

                  <div>
                    <label>Heartbeat</label>
                    <strong>{consumer.heartbeat}</strong>
                  </div>
                </div>

                <div className="partition-label consumer-partitions">
                  <label>Partitions</label>
                  <div className="partition-chips">
                    {consumer.partitions.length > 0 ? (
                      consumer.partitions.map((partition) => (
                        <span
                          className={`partition-chip partition-chip-${partition}`}
                          key={partition}
                        >
                          P{partition}
                        </span>
                      ))
                    ) : (
                      <span>None</span>
                    )}
                  </div>
                </div>

                <div className="consumer-assignment">
                  <div className="partition-assignment-label">
                    PARTITION ASSIGNMENT
                  </div>

                  {consumer.partitions.map((partition) => {
                    const lag = getPartitionLag(partition);

                    return (
                      <div className="assignment-row" key={partition}>
                        <span>P{partition}</span>

                        <i
                          className={`assignment-indicator assignment-partition-${partition} ${
                            lag !== null && lag > 1500
                              ? 'critical'
                              : ''
                          }`}
                          style={{
                            width:
                              lag === null
                                ? '8%'
                                : `${Math.min(
                                    100,
                                    Math.max(8, lag / 20),
                                  )}%`,
                          }}
                        />

                        <b>
                          Lag{' '}
                          {lag === null
                            ? '—'
                            : lag.toLocaleString()}
                        </b>
                      </div>
                    );
                  })}
                </div>
              </article>
            ))}
          </div>

          <section className="panel table-panel partition-assignment-panel">
            <div className="panel-title-row">
              <div>
                <h2>Kafka Partition Assignment</h2>
                <p>Live partition-to-consumer mapping</p>
              </div>

              <span className="topic-control">
                <span>TOPIC</span>
                <b className="topic-tag">LOGS</b>
              </span>
            </div>

            <table className="partition-table">
              <thead>
                <tr>
                  <th>PART.</th>
                  <th>THROUGHPUT</th>
                  <th>CURRENT LAG</th>
                  <th>CONSUMER</th>
                </tr>
              </thead>

              <tbody>
                {status.partitions
                  .filter(
                    (partition) =>
                      partition.assigned_consumer !== 'dlq-fast' &&
                      partition.assigned_consumer !== 'dlq-test',
                  )
                  .map((partition, index) => {
                    const lag = getPartitionLag(partition.partition);

                  return (
                    <tr
                      key={`${partition.partition}-${partition.assigned_consumer}-${index}`}
                      className={
                        selectedPartition === partition.partition
                          ? 'selected-row'
                          : ''
                      }
                      onClick={() =>
                        setSelectedPartition(partition.partition)
                      }
                    >
                      <td>
                        <span
                          className={`partition-badge partition-chip-${partition.partition}`}
                        >
                          P{partition.partition}
                        </span>
                      </td>

                      <td>
                        {partition.throughput.toFixed(2)} msg/s
                      </td>

                      <td
                        className={
                          lag !== null && lag > 1500
                            ? 'danger-text'
                            : 'healthy-text'
                        }
                      >
                        Lag{' '}
                        {lag === null
                          ? '—'
                          : lag.toLocaleString()}
                      </td>

                      <td>
                        <span className="consumer-badge">
                          {partition.assigned_consumer}
                        </span>
                      </td>
                    </tr>
                  );
                  })}
              </tbody>
            </table>
          </section>
        </div>

        <div className="lower-split">
          <section className="panel consumer-lag-panel">
            <div className="panel-title-row">
              <div>
                <h2>Consumer Lag by Partition</h2>
                <p>Messages behind latest offset</p>
              </div>

              <span className="lag-legend">
                ● Healthy　● Warning　● Critical
              </span>
            </div>

            <div className="partition-rows">
              {mainPartitions.map((partition) => {
                const lag = getPartitionLag(partition.partition);

                return (
                  <button
                    type="button"
                    key={partition.partition}
                    className={`partition-item ${
                      selectedPartition === partition.partition
                        ? 'selected'
                        : ''
                    }`}
                    onClick={() =>
                      setSelectedPartition(partition.partition)
                    }
                  >
                    <span>P{partition.partition}</span>

                    <i
                      className={`lag-track lag-partition-${partition.partition}`}
                    >
                      <b
                        style={{
                          width:
                            lag === null || maxPartitionLag === 0
                              ? '0%'
                              : `${Math.min(
                                  100,
                                  (Math.max(0, lag) / maxPartitionLag) * 100,
                                )}%`,
                        }}
                      />
                    </i>

                    <strong
                      className={
                        lag !== null && lag > 1500
                          ? 'danger-text'
                          : ''
                      }
                    >
                      {lag === null
                        ? '—'
                        : lag.toLocaleString()}
                    </strong>
                  </button>
                );
              })}
            </div>

            <div className="high-water-mark">
              └ High-Water Mark: 1,500
            </div>

            {selectedLag === null && (
              <div className="event-empty">
                No lag snapshot is currently available for P
                {selectedPartition}.
              </div>
            )}
          </section>

          <section className="panel event-inline-panel">
            <div className="panel-title-row">
              <div>
                <h2>Consumer Events</h2>
                <p>Real-time consumer activity</p>
              </div>

              {eventsLive && (
                <span className="status-tag info">
                  ● LIVE
                </span>
              )}
            </div>

            {consumerEventsError ? (
              <div className="event-empty">
                {consumerEventsError}
              </div>
            ) : hasConsumerEvents ? (
              <ul className="event-list compact">
                {consumerEvents.map((event) => (
                  <li key={`${event.timestamp}-${event.component}-${event.message}`}>
                    <span className="event-time">
                      {formatClock(new Date(event.timestamp))}
                    </span>
                    <span className={`event-service ${event.severity.toLowerCase()}`}>
                      {event.severity}
                    </span>
                    <span>{event.component}</span>
                    <span>{event.message}</span>
                  </li>
                ))}
              </ul>
            ) : (
              <div className="event-empty">
                No consumer group events have been recorded yet.
              </div>
            )}
          </section>
        </div>

        <section className="rebalancing-panel panel">
          <div className="panel-title-row">
            <div>
              <h2>Rebalancing State</h2>
              <p>
                Alternate state — worker failure &amp; partition
                reassignment
              </p>
            </div>

            <button
              className="scenario-button"
              onClick={() => navigate('/scenarios')}
            >
              SCENARIO VIEW
            </button>
          </div>

          <div className="rebalance-body">
            <div className="rebalance-card rebalance-lifecycle-card">
              <div className="rebalance-card-heading">
                <h3>Rebalance lifecycle</h3>
                <span className="status-tag info">● {lifecycleState}</span>
              </div>

              <div className="rebalance-lifecycle">
                <div className={lifecycleState === 'STABLE' && !hasRecovered ? 'is-current' : 'is-complete'}>
                  <b>STABLE</b>
                  <span>Group operating normally</span>
                </div>
                <div className={lifecycleState === 'REBALANCING' ? 'is-current' : lifecycleState === 'STABLE' && hasRecovered ? 'is-complete' : ''}>
                  <b>REBALANCING</b>
                  <span>Assignments are being coordinated</span>
                </div>
                <div className={hasRecovered ? 'is-current' : ''}>
                  <b>RECOVERED / STABLE</b>
                  <span>Processing resumed after reassignment</span>
                </div>
              </div>

              <div className="rebalance-current-state">
                <span>Current assignment</span>
                <strong>
                  {status.rebalancing.current_assignment.length > 0
                    ? status.rebalancing.current_assignment.map((p) => `P${p}`).join(', ')
                    : 'None'}
                </strong>
              </div>
            </div>

            <div className="rebalance-card rebalance-assignment-card">
              <div className="rebalance-card-heading">
                <h3>Partition reassignment</h3>
                <span className="lag-legend">{movedPartitions.size} moved</span>
              </div>
              <div className="rebalance-assignment-grid">
                <div>
                  <span className="rebalance-label">Before</span>
                  {rebalanceBefore.length > 0 ? rebalanceBefore.map((partition) => (
                    <div className="rebalance-assignment-row" key={`before-${partition.partition}`}>
                      <span>P{partition.partition}</span>
                      <strong>{partition.assigned_consumer}</strong>
                    </div>
                  )) : <p className="rebalance-unavailable">No prior assignment captured.</p>}
                </div>
                <div>
                  <span className="rebalance-label">After</span>
                  {currentAssignments.length > 0 ? currentAssignments.map((partition) => (
                    <div className={`rebalance-assignment-row ${movedPartitions.has(partition.partition) ? 'is-moved' : ''}`} key={`after-${partition.partition}`}>
                      <span>P{partition.partition}</span>
                      <strong>{partition.assigned_consumer}</strong>
                    </div>
                  )) : <p className="rebalance-unavailable">No current assignment available.</p>}
                </div>
              </div>
            </div>

            <div className="rebalance-card rebalance-health-card">
              <div className="rebalance-card-heading">
                <h3>Consumer health</h3>
                <span className="lag-legend">Current API state</span>
              </div>
              {consumers.length > 0 ? (
                <div className="rebalance-consumer-list">
                  {consumers.map((consumer) => (
                    <div className="rebalance-consumer-row" key={consumer.id}>
                      <strong>{consumer.id}</strong>
                      <span className={consumer.status === 'RUNNING' ? 'healthy-text' : 'orange-text'}>{consumer.status}</span>
                      <span>{consumer.rate.toFixed(2)} msg/s · Lag {consumer.lag.toLocaleString()}</span>
                    </div>
                  ))}
                </div>
              ) : (
                <p className="rebalance-unavailable">No consumer health data is available.</p>
              )}
            </div>

            <div className="rebalance-card rebalance-result-card">
              <div className="rebalance-card-heading">
                <h3>Recovery result</h3>
                <span className={isLive ? 'healthy-text' : 'orange-text'}>{isLive ? 'LIVE' : 'UNAVAILABLE'}</span>
              </div>
              <div className="rebalance-result-grid">
                <div><span>Rebalance duration</span><strong>Not available</strong></div>
                <div><span>Partitions reassigned</span><strong>{rebalanceBefore.length > 0 ? movedPartitions.size : 'Not available'}</strong></div>
                <div><span>Consumers available</span><strong>{consumers.length > 0 ? `${activeConsumers} / ${totalConsumers}` : 'Not available'}</strong></div>
                <div><span>Group state</span><strong>{lifecycleState || 'Not available'}</strong></div>
                <div><span>Processing resumed</span><strong>{isLive ? 'YES' : 'NO'}</strong></div>
              </div>
            </div>
          </div>
        </section>
      </div>
    </DashboardShell>
  );
}
