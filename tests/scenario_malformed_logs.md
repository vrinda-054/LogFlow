# Scenario: Malformed Logs

**Owner**: Person 1 (ingestion/) and Person 2 (consumers/)  
**Test Type**: Fault Tolerance / DLQ Validation

---

## Objective

Verify that malformed or schema-violating messages are correctly identified,
retried (if configured), and routed to the Dead Letter Queue — without
crashing the consumer or blocking processing of valid messages.

---

## Prerequisites

- Normal load scenario passes
- `shared/schemas/log_schema.json` reviewed and understood by both Person 1 and 2
- DLQ topic `logs-dlq` confirmed to exist with 1 partition

---

## Steps

1. Start 3 consumer instances (Person 2):
   ```bash
   python consumers/consumer.py  # × 3 terminals
   ```

2. Start the producer in malformed scenario (Person 1):
   ```bash
   cd ingestion && python producer.py \
     --rate 10 --duration 120 \
     --malformed-pct 30 \
     --scenario malformed
   ```
   *30% of messages will be intentionally malformed — missing fields,
    wrong types (e.g. `severity: 42`), or invalid JSON.*

3. Watch consumer output for validation errors and DLQ routing:
   ```
   [consumer] Schema validation failed: ...
   [dlq_handler] Retry 1/3 after 0.5s — ...
   [dlq_handler] Publishing to DLQ | reason='...' retries=3
   ```

4. After 120 seconds, check DLQ:
   ```bash
   curl http://localhost:8000/dlq/messages?limit=50
   ```

5. Also check dashboard DLQViewer panel.

---

## Expected Results

| Metric                        | Expected                                       |
|-------------------------------|------------------------------------------------|
| Messages produced             | ~1200 (120s × 10/s)                            |
| Malformed messages produced   | ~360 (30%)                                     |
| DLQ entries                   | ≈ 360 (each malformed message → DLQ)           |
| Valid messages processed       | ≈ 840 in processed_logs                        |
| Consumer crash / restart      | None — errors handled gracefully               |
| DLQ `failure_reason` field    | Non-empty string describing the schema error   |
| DLQ `retry_count`             | 0 for schema failures (no retry on bad schema) |

---

## How to Verify

- [ ] `GET /dlq/messages` returns `total` ≈ 360
- [ ] Each DLQ entry has a non-empty `failure_reason`
- [ ] Consumer processes do NOT crash during the 120-second run
- [ ] Valid messages still appear in `processed_logs`:
  ```sql
  SELECT COUNT(*) FROM logflow.processed_logs;
  -- Expected: ~840
  ```
- [ ] DLQ entries appear in the React dashboard DLQViewer
- [ ] Kafka `logs-dlq` topic has messages:
  ```bash
  docker exec logflow-kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic logs-dlq --from-beginning --max-messages 5
  ```
