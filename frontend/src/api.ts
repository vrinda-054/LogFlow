const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';

export type ConsumerLagRecord = {
  partition_id: number;
  lag: number;
  recorded_at: string;
  consumer_id: string | null;
};

export type ConsumerLagResponse = {
  total_lag: number;
  partitions: ConsumerLagRecord[];
};

export type DlqRecord = {
  id: number;
  failed_at: string;
  failure_reason: string;
  retry_count: number;
  original_message: string;
};

export type DlqActivityRecord = {
  timestamp: string;
  count: number;
};

export type DlqActivityResponse = {
  activity: DlqActivityRecord[];
};

export type DlqResponse = {
  total: number;
  messages: DlqRecord[];
};

export type ConsumerStatus = {
  consumer_id: string;
  status: string;
  assigned_partitions: number[];
  processing_rate: number;
  consumer_lag: number;
  last_heartbeat: string;
  backpressure_active: boolean;
};

export type PartitionStatus = {
  partition: number;
  throughput: number;
  current_lag: number;
  assigned_consumer: string;
  health: string;
};

export type ConsumerStatusResponse = {
  consumer: ConsumerStatus;
  partitions: PartitionStatus[];
  rebalancing: {
    state: string;
    current_assignment: number[];
    after_recovery: string;
  };
};

export type ThroughputWindow = {
  window_start: string;
  window_end: string;
  service: string;
  message_count: number;
  messages_per_sec: number;
};

export type ThroughputResponse = {
  windows: ThroughputWindow[];
  summary: {
    current_rate: number;
    peak_rate: number;
    average_rate: number;
    total_windows: number;
  };
};

export type ErrorRateWindow = {
  window_start: string;
  window_end: string;
  service: string;
  total_messages: number;
  error_messages: number;
  error_rate_pct: number;
};

export type ErrorRateResponse = {
  windows: ErrorRateWindow[];
  summary: {
    overall_error_rate_pct: number;
    total_messages: number;
    total_errors: number;
    per_service: Array<{
      service: string;
      total_messages: number;
      error_messages: number;
      error_rate_pct: number;
    }>;
  };
};

export type HealthResponse = {
  status: string;
  database: string;
  error?: string;
};
export type LogRecord = {
  id: number;
  ingested_at: string;
  timestamp: string;
  service: string;
  severity: string;
  message: string;
  trace_id: string;
};

export type LogsResponse = {
  logs: LogRecord[];
};

async function apiFetch<T>(
  path: string,
  params: Record<string, string | number> = {}
): Promise<T> {
  const url = new URL(path, API_BASE_URL);

  Object.entries(params).forEach(([key, value]) => {
    url.searchParams.set(key, String(value));
  });

  const response = await fetch(url, {
    headers: { Accept: 'application/json' },
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`API ${response.status}: ${body || response.statusText}`);
  }

  return response.json() as Promise<T>;
}

async function apiPost<T>(path: string): Promise<T> {
  const response = await fetch(new URL(path, API_BASE_URL), {
    method: 'POST',
    headers: { Accept: 'application/json' },
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`API ${response.status}: ${body || response.statusText}`);
  }

  return response.json() as Promise<T>;
}

export type ScenarioKey =
  | 'normal-load'
  | 'traffic-spike'
  | 'malformed'
  | 'slow-consumer'
  | 'worker-failure';

export type ScenarioStartResponse = {
  status: 'started' | 'running';
  scenario: ScenarioKey;
};

export function startScenario(scenario: ScenarioKey): Promise<ScenarioStartResponse> {
  return apiPost<ScenarioStartResponse>(`/scenarios/${scenario}`);
}
export function getLogs(limit = 50, offset = 0): Promise<LogsResponse> {
  return apiFetch<LogsResponse>('/logs', { limit, offset });
}

export function getHealth(): Promise<HealthResponse> {
  return apiFetch<HealthResponse>('/health');
}

export function healthCheck(): Promise<HealthResponse> {
  return apiFetch<HealthResponse>('/health');
}

export function getThroughput(minutes = 60, service?: string): Promise<ThroughputResponse> {
  const params: Record<string, string | number> = { minutes };
  if (service) params.service = service;
  return apiFetch<ThroughputResponse>('/metrics/throughput', params);
}

export function getErrorRates(minutes = 60, service?: string): Promise<ErrorRateResponse> {
  const params: Record<string, string | number> = { minutes };
  if (service) params.service = service;
  return apiFetch<ErrorRateResponse>('/metrics/errors', params);
}

export function getConsumerLag(): Promise<ConsumerLagResponse> {
  return apiFetch<ConsumerLagResponse>('/metrics/lag');
}

export function getDlqMessages(limit = 50, offset = 0): Promise<DlqResponse> {
  return apiFetch<DlqResponse>('/dlq/messages', { limit, offset });
}

export function getDlqActivity(hours = 24): Promise<DlqActivityResponse> {
  return apiFetch<DlqActivityResponse>('/dlq/activity', { hours });
}

export function getConsumerStatus(): Promise<ConsumerStatusResponse> {
  return apiFetch<ConsumerStatusResponse>('/metrics/consumers');
}

