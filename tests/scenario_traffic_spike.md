# Scenario: Traffic Spike

**Owner**: Person 1 (ingestion/) and Person 2 (consumers/)  
**Test Type**: Load / Stress Test

---

## Objective

Verify that the pipeline handles a sudden 10× traffic spike gracefully:
consumer lag temporarily increases, backpressure.py pauses partitions,
and the system recovers without message loss once the spike subsides.

---

## Prerequisites

- Normal load scenario passes (baseline established)
- 3 consumer instances running
- `BACKPRESSURE_HIGH_WATER` and `BACKPRESSURE_LOW_WATER` set in `.env`
  (defaults: 1000 and 200)

---

## Steps

1. Start 3 consumer instances (Person 2):
   ```bash
   python consumers/consumer.py  # × 3 terminals
   ```

2. Start the spike scenario producer (Person 1):
   ```bash
   cd ingestion && python producer.py --rate 10 --duration 300 --scenario spike
   ```
   *The `spike` preset produces 100 msg/s for 5-second bursts every 30 seconds.*

3. Watch consumer lag during a spike burst:
   ```bash
   watch -n 2 'docker exec logflow-kafka kafka-consumer-groups \
     --bootstrap-server localhost:9092 \
     --group logflow-group --describe'
   ```

4. Observe backpressure logs in consumer terminals — look for:
   ```
   [backpressure] REQ-18/REQ-20: PAUSING partition=X ...
   [backpressure] REQ-19/REQ-20: RESUMING partition=X ...
   ```

5. After 300 seconds, verify full recovery:
   ```bash
   curl http://localhost:8000/metrics/lag
   ```

---

## Expected Results

| Phase              | Metric                      | Expected                              |
|--------------------|-----------------------------|---------------------------------------|
| Steady (10 msg/s)  | Consumer lag / partition    | < 50                                  |
| Spike (100 msg/s)  | Consumer lag / partition    | Rises to 500–2000 (HIGH_WATER region) |
| Spike              | Backpressure state          | At least one partition PAUSED         |
| Post-spike         | Consumer lag / partition    | Returns to < 50 within 60s            |
| Post-spike         | DLQ messages                | 0 (no messages dropped)               |

---

## How to Verify

- [ ] Backpressure PAUSE log lines appear during spike (REQ-18)
- [ ] Backpressure RESUME log lines appear after spike (REQ-19)
- [ ] `GET /metrics/lag` shows lag returns to baseline after spike
- [ ] `GET /dlq/messages` returns `total: 0` (no loss during backpressure)
- [ ] `GET /metrics/throughput` shows the spike visible as a clear peak in chart
- [ ] PostgreSQL `metrics_consumer_lag` table shows lag trajectory:
  ```sql
  SELECT partition_id, lag, recorded_at
  FROM logflow.metrics_consumer_lag
  ORDER BY recorded_at DESC LIMIT 40;
  ```
