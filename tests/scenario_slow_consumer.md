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

1. Start the full stack, then replace consumer 3 with a delayed instance:
   ```bash
  docker compose up -d kafka postgres kafka-init consumer-1 consumer-2 consumer-3
  docker compose stop consumer-3
  docker compose run -d --name logflow-consumer-3-slow \
    -e BACKPRESSURE_HIGH_WATER=50 \
    -e BACKPRESSURE_LOW_WATER=10 \
    consumer-3 --consumer-id consumer-03 --inject-delay-ms 2000
   ```

2. Start the producer at normal rate (Person 1):
   ```bash
  python tests/manual_test_consumer.py
   ```

  For a 200-message burst, run the following from the project root:
  ```bash
  python -c "from confluent_kafka import Producer; import json, uuid; from datetime import datetime, timezone; p=Producer({'bootstrap.servers':'localhost:9092'}); [p.produce('logs-raw', value=json.dumps({'timestamp':datetime.now(timezone.utc).isoformat(),'service':'slow-test','severity':'INFO','message':str(i),'trace_id':uuid.uuid4().hex}).encode()) for i in range(200)]; p.flush()"
  ```

3. Monitor partition lag every 5 seconds:
   ```bash
   watch -n 5 'docker exec logflow-kafka kafka-consumer-groups \
     --bootstrap-server localhost:9092 \
     --group logflow-group --describe'
   ```

4. After the lag has crossed the high-water mark, remove the delayed instance
  and restart the normal service:
   ```bash
  docker rm -f logflow-consumer-3-slow
  docker compose start consumer-3
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

- [ ] `event={"action": "PAUSE", ...}` appears on the slow partition (REQ-18 / REQ-20)
- [ ] `event={"action": "RESUME", ...}` appears after lag falls below the low-water mark (REQ-19 / REQ-20)
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
