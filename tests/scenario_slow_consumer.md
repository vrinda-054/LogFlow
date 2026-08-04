# Scenario: Slow Consumer

**Owner**: Person 2 (consumers/) and Person 3 (processing/)  
**Test Type**: Fault Tolerance / Backpressure Test

---

## Objective

Verify that when one or more consumers are artificially slowed (simulating an
overloaded aggregation layer), the backpressure mechanism correctly pauses
the affected partition(s), prevents unbounded lag growth, and resumes normal
processing once the bottleneck clears.

---

## Prerequisites

- Normal load scenario passes
- Backpressure thresholds configured in `.env`:
  ```
  BACKPRESSURE_HIGH_WATER=500
  BACKPRESSURE_LOW_WATER=100
  ```

---

## Steps

1. Start 3 consumer instances (Person 2) — one will be throttled:
   ```bash
   # Terminal 1 (normal)
   python consumers/consumer.py

   # Terminal 2 (normal)
   python consumers/consumer.py

   # Terminal 3 (slow — Person 2 must add a --slow-mode flag or sleep injection)
   python consumers/consumer.py --slow-mode  # add 2s sleep per message
   ```

2. Start the producer at normal rate (Person 1):
   ```bash
   cd ingestion && python producer.py --rate 20 --duration 180 --scenario normal
   ```

3. Monitor partition lag every 5 seconds:
   ```bash
   watch -n 5 'docker exec logflow-kafka kafka-consumer-groups \
     --bootstrap-server localhost:9092 \
     --group logflow-group --describe'
   ```

4. After 90 seconds, "fix" the slow consumer (Ctrl+C in Terminal 3, restart normally):
   ```bash
   python consumers/consumer.py
   ```

5. Observe lag recovery and partition resumption in logs.

---

## Expected Results

| Phase                   | Metric                       | Expected                                |
|-------------------------|------------------------------|-----------------------------------------|
| 0–90s (slow consumer)   | Lag on slow consumer's partition | Rises above BACKPRESSURE_HIGH_WATER |
| 0–90s                   | Backpressure state           | Partition PAUSED (REQ-18)               |
| 0–90s                   | DLQ messages                 | 0 (paused, not dropped)                 |
| 90–180s (after fix)     | Lag on affected partition    | Drains to < LOW_WATER within 60s        |
| 90–180s                 | Backpressure state           | Partition RESUMED (REQ-19)              |
| End                     | processed_logs total         | All non-malformed messages accounted for|

---

## How to Verify

- [ ] PAUSE log line appears on the slow partition (REQ-18 / REQ-20)
- [ ] RESUME log line appears after the consumer is restarted (REQ-19 / REQ-20)
- [ ] Lag on the slow partition never exceeds 2× BACKPRESSURE_HIGH_WATER
  (backpressure prevents runaway growth)
- [ ] `GET /metrics/lag` shows lag trajectory (rise then drain) in PostgreSQL:
  ```sql
  SELECT partition_id, lag, recorded_at
  FROM logflow.metrics_consumer_lag
  WHERE partition_id = <affected>
  ORDER BY recorded_at ASC;
  ```
- [ ] React ConsumerLagPanel shows the partition transitioning red → green
