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

export type DlqResponse = {
  total: number;
  messages: DlqRecord[];
};

async function apiFetch<T>(path: string, params: Record<string, string | number> = {}): Promise<T> {
  const url = new URL(path, API_BASE_URL);
  Object.entries(params).forEach(([key, value]) => url.searchParams.set(key, String(value)));

  const response = await fetch(url, {
    headers: { Accept: 'application/json' },
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`API ${response.status}: ${body || response.statusText}`);
  }

  return response.json() as Promise<T>;
}

export function getConsumerLag(): Promise<ConsumerLagResponse> {
  return apiFetch<ConsumerLagResponse>('/metrics/lag');
}

export function getDlqMessages(limit = 50, offset = 0): Promise<DlqResponse> {
  return apiFetch<DlqResponse>('/dlq/messages', { limit, offset });
}
