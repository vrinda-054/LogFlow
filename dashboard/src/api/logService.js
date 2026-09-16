import client from './client.js';

/**
 * Fetches current system metrics.
 * @returns {Promise<unknown>}
 */
export async function getSystemMetrics() {
  try {
    const response = await client.get('/api/v1/metrics');
    return response?.data ?? {};
  } catch (error) {
    console.error('Failed to fetch system metrics:', error);
    throw error;
  }
}

/**
 * Fetches live logs.
 * @param {Record<string, unknown>} [params={}]
 * @returns {Promise<unknown>}
 */
export async function getLiveLogs(params = {}) {
  try {
    const response = await client.get('/api/v1/logs', { params });
    return response?.data ?? [];
  } catch (error) {
    console.error('Failed to fetch live logs:', error);
    throw error;
  }
}

/**
 * Fetches dead letter queue messages.
 * @param {Record<string, unknown>} [params={}]
 * @returns {Promise<unknown>}
 */
export async function getDLQMessages(params = {}) {
  try {
    const response = await client.get('/api/v1/dlq', { params });
    return response?.data ?? [];
  } catch (error) {
    console.error('Failed to fetch DLQ messages:', error);
    throw error;
  }
}

/**
 * Retries a DLQ message.
 * @param {string|number} messageId
 * @returns {Promise<unknown>}
 */
export async function retryDLQMessage(messageId) {
  try {
    const response = await client.post('/api/v1/dlq/retry', { messageId });
    return response?.data ?? {};
  } catch (error) {
    console.error(`Failed to retry DLQ message ${messageId}:`, error);
    throw error;
  }
}

/**
 * Triggers a fault-injection scenario.
 * @param {Record<string, unknown>} scenarioPayload
 * @returns {Promise<unknown>}
 */
export async function triggerFaultScenario(scenarioPayload = {}) {
  try {
    const response = await client.post(
      '/api/v1/scenarios/trigger',
      scenarioPayload,
    );
    return response?.data ?? {};
  } catch (error) {
    console.error('Failed to trigger fault scenario:', error);
    throw error;
  }
}