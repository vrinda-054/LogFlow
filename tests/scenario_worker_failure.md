# Scenario: Worker Failure

**Owner**: Person 2 (consumers/) and Person 3 (processing/)  
**Test Type**: Fault Tolerance / Resilience Test

---

## Objective

Verify that when one or more consumer instances are killed unexpectedly
(simulating a worker crash or pod eviction in Kubernetes), the consumer group
rebalances correctly, no messages are lost, and processing resumes with the
remaining consumers — all without operator intervention.

---

## Prerequisites

- Normal load scenario passes
- Cooperative-sticky rebalance configured (`rebalance_config.py`)
- At least 3 consumer instances started

---

## Steps

1. Start 3 consumer instances (Person 2):
   ```bash
   # Terminal 1 — consumer A
   python consumers/consumer.py

   # Terminal 2 — consumer B (will be killed)
   python consumers/consumer.py

   # Terminal 3 — consumer C
   python consumers/consumer.py
   ```

2. Start producer at steady rate (Person 1):
   ```bash
   cd ingestion && python producer.py --rate 15 --duration 300 --scenario normal
   ```

3. After 60 seconds of steady operation, kill Consumer B hard:
   ```bash
   # In a new terminal — find Consumer B's PID and kill it
   # Linux/Mac:  kill -9 <pid>
   # Windows:    taskkill /PID <pid> /F
   ```

4. Observe rebalance in Consumers A and C logs:
   ```
   [consumer] Partitions revoked: [...]
   [consumer] Partitions assigned: [...]
   ```

5. Check that Kafka consumer group still shows all 4 partitions owned:
   ```bash
   docker exec logflow-kafka kafka-consumer-groups \
     --bootstrap-server localhost:9092 \
     --group logflow-group --describe
   ```

6. After 120 more seconds (Consumer B has been dead for 2 min), check for message loss.

7. Restart Consumer B (simulating auto-restart / Kubernetes pod restart):
   ```bash
   python consumers/consumer.py
   ```

8. Verify second rebalance redistributes partitions across all 3 consumers again.

---

## Expected Results

| Phase                       | Metric                          | Expected                                    |
|-----------------------------|---------------------------------|---------------------------------------------|
| Before kill (steady)        | Consumer group members          | 3 (A, B, C)                                 |
| During kill (0–30s)         | Session timeout expiry          | Consumer B declared dead after ~30s         |
| Rebalance                   | Partition ownership             | B's partitions acquired by A or C           |
| Rebalance                   | Cooperative-sticky: disruption  | Only B's partitions revoked (A/C unaffected)|
| Post-kill (steady 2 members)| Consumer lag                    | Stabilises within 60s of rebalance          |
| Post-kill                   | DLQ messages                    | 0 (offset committed before kill or retried) |
| After B restart             | Consumer group members          | Back to 3                                   |
| After B restart             | Partition distribution          | ~1–2 partitions per consumer (rebalanced)   |

---

## How to Verify

- [ ] `on_assign` and `on_revoke` callbacks are logged during rebalance
- [ ] After kill, `kafka-consumer-groups --describe` shows only 2 members
  but all 4 partitions still assigned
- [ ] No gap in `processed_logs` timestamps > session.timeout.ms (30s):
  ```sql
  SELECT log_ts, ingested_at
  FROM logflow.processed_logs
  ORDER BY log_ts ASC;
  -- Look for timestamp gaps > 30 seconds
  ```
- [ ] `GET /dlq/messages` returns 0 entries (no messages lost to DLQ)
- [ ] After Consumer B restart, `kafka-consumer-groups --describe` shows 3 members
- [ ] React ConsumerLagPanel shows all 4 partitions with lag < 100 after stabilisation
- [ ] Confirm cooperative-sticky rebalance was used — A and C logs should NOT show
  their own partitions revoked when B is killed:
  ```
  [consumer] Partitions revoked: []    ← expected for A and C
  ```
