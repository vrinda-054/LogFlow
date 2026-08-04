# Scenario: Normal Load

**Owner**: Person 1 (ingestion/) and Person 2 (consumers/)  
**Test Type**: Baseline / Smoke Test

---

## Objective

Verify that the pipeline can sustain steady-state log ingestion and processing
without message loss, consumer lag build-up, or DLQ entries.

---

## Prerequisites

- `docker compose up -d` completed successfully
- `kafka-init` container exited with code 0 (topics created)
- All four partitions of the `logs` topic confirmed: `docker exec logflow-kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic logs`
- PostgreSQL schema applied (check: `docker exec logflow-postgres psql -U logflow_user -d logflow -c "\dt logflow.*"`)

---

## Steps

1. Start 3 consumer instances (Person 2):
   ```bash
   # Terminal 1
   cd consumers && python consumer.py

   # Terminal 2
   cd consumers && python consumer.py

   # Terminal 3
   cd consumers && python consumer.py
   ```

2. Start the producer in normal scenario (Person 1):
   ```bash
   cd ingestion && python producer.py --rate 10 --duration 120 --scenario normal
   ```

3. Monitor throughput via the API (Person 3):
   ```bash
   curl http://localhost:8000/metrics/throughput?minutes=5
   ```

4. Check consumer lag:
   ```bash
   docker exec logflow-kafka kafka-consumer-groups \
     --bootstrap-server localhost:9092 \
     --group logflow-group --describe
   ```

5. After 120 seconds, check DLQ:
   ```bash
   curl http://localhost:8000/dlq/messages?limit=10
   ```

---

## Expected Results

| Metric                    | Expected Value                              |
|---------------------------|---------------------------------------------|
| Messages produced         | ~1200 (10/s × 120s)                         |
| Messages in DLQ           | 0                                           |
| Consumer lag (all parts.) | < 50 messages each                          |
| Throughput API response   | windows present, messages_per_sec ≈ 10.0    |
| Error rate                | < 15% (matches log_templates severity dist) |

---

## How to Verify

- [ ] `GET /metrics/throughput` returns non-empty `windows` array
- [ ] `GET /dlq/messages` returns `total: 0`
- [ ] `GET /metrics/lag` shows lag < 50 per partition
- [ ] React dashboard displays live charts without "API unreachable" banner
- [ ] PostgreSQL `processed_logs` row count ≈ 1200:
  ```sql
  SELECT COUNT(*) FROM logflow.processed_logs;
  ```
