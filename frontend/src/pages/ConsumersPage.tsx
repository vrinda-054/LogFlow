import { useEffect, useMemo, useState } from 'react';
import {
  getConsumerEvents,
  getConsumerLag,
  getConsumerStatus,
  getThroughput,
  type ConsumerEvent,
  type ConsumerLagResponse,
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
  const [lagData, setLagData] = useState(emptyLag);
  const [status, setStatus] = useState(emptyStatus);
  const [throughput, setThroughput] = useState(emptyThroughput);
  const [consumers, setConsumers] = useState<ConsumerView[]>([]);
  const [lastUpdated, setLastUpdated] = useState('—');
  const [isLive, setIsLive] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [consumerEvents, setConsumerEvents] = useState<ConsumerEvent[]>([]);
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
      setStatus(statusResult.value);
    }

    if (throughputResult.status === 'fulfilled') {
      setThroughput(throughputResult.value);
    }

    if (eventsResult.status === 'fulfilled') {
      setConsumerEvents(eventsResult.value.events ?? []);
      setConsumerEventsError(null);
      setEventsLive(true);
    } else {
      setConsumerEvents([]);
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
            <div className="header-meta consumer-header-meta">
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

            <button className="scenario-button">
              SCENARIO VIEW
            </button>
          </div>

          <div className="rebalance-body">
            <div className="rebalance-card">
              <h3>
                Consumer Group

                <span
                  className={
                    status.rebalancing.state === 'STABLE'
                      ? 'success-tag'
                      : 'warning-tag'
                  }
                >
                  {status.rebalancing.state}
                </span>
              </h3>

              <strong>
                Current assignment:{' '}
                {status.rebalancing.current_assignment.length > 0
                  ? status.rebalancing.current_assignment
                      .map((p) => `P${p}`)
                      .join(', ')
                  : 'None'}
              </strong>

              <p>
                After recovery:{' '}
                {status.rebalancing.after_recovery}
              </p>
            </div>
          </div>
        </section>
      </div>
    </DashboardShell>
  );
}
