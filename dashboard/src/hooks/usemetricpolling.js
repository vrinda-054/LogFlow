import { useCallback, useEffect, useState } from 'react';
import { getSystemMetrics } from '../api/logService.js';

export function useMetricsPolling(intervalMs = 3000) {
  const [metrics, setMetrics] = useState({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const refetch = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      const result = await getSystemMetrics();
      setMetrics(result && typeof result === 'object' ? result : {});
    } catch (requestError) {
      setError(requestError);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    let mounted = true;
    let timerId;

    const loadMetrics = async () => {
      if (!mounted) return;

      setLoading(true);
      setError(null);

      try {
        const result = await getSystemMetrics();

        if (mounted) {
          setMetrics(result && typeof result === 'object' ? result : {});
        }
      } catch (requestError) {
        if (mounted) setError(requestError);
      } finally {
        if (mounted) setLoading(false);
      }
    };

    loadMetrics();
    timerId = window.setInterval(loadMetrics, Math.max(1000, intervalMs));

    return () => {
      mounted = false;
      window.clearInterval(timerId);
    };
  }, [intervalMs]);

  return { metrics, loading, error, refetch };
}