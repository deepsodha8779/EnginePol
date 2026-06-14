# RabbitMQ Bulk Testing Report

## Objective

Validate that Tadpole Engine can receive many canonical event-envelope messages from RabbitMQ, process each message through the same intake pipeline as HTTP, publish response events to `events_intake_response`, persist intake/audit data, and handle invalid or duplicate messages without stopping the consumer.

## System Under Test

- Consumer: `http-gateway/src/rabbitmq_consumer.rs`
- Input queue: `RABBITMQ_QUEUE`, default `events_intake`
- Response queue: `RABBITMQ_RESPONSE_QUEUE`, default `events_intake_response`
- Message body: one JSON canonical envelope per RabbitMQ message
- Success behavior: publish response envelope array, then ACK original message
- Failure behavior:
  - Invalid JSON: publish `{"status":"error","errors":[...]}` and NACK without requeue
  - Pipeline validation error: publish standard error body and NACK without requeue
- Storage expected: `intake_events`, event logs, metrics, and actions when rules fail

## Environment

Use `.env` or shell variables:

```bash
MONGODB_URI=mongodb://...
MONGODB_DB=devdb
MONGODB_COLLECTION=intake_events
RABBITMQ_URL=amqp://user:password@host:5672/%2F
RABBITMQ_QUEUE=events_intake
RABBITMQ_RESPONSE_QUEUE=events_intake_response
PLAYBOOK_CONFIG=config/playbooks-test-fail.json
```

Start the engine:

```bash
cargo run -p http-gateway
```

Install the Python RabbitMQ client if needed:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

## Single Event String

This is the compact JSON string format to send as the RabbitMQ message body. Publish it to the default exchange with routing key `events_intake`.

```json
{"head":{"event_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","event_name":"InvoiceReceivedForExternalDependency","event_category":"Transaction","tenant_id":"acme-corp","correlation_id":null,"causation_id":null,"occurred_at":"2026-05-29T12:00:00Z","originating_function":"Finance","originating_application":"BulkRabbitMqTest","environment":"bulk-test","external_dependency_id":"01J0M8WZ9K7F6R5T4P3Q2N1M0","changed_object_type":"Invoice","changed_object_id":"01J0M8Y8A4D5F6G7H8J9KLMNO","change_kind":"created"},"body":{"snapshots":[{"object_type":"Invoice","object_id":"01J0M8Y8A4D5F6G7H8J9KLMNO","supplier_reference":"SUP-BULK-000001","amount":12500,"currency":"EUR"}]}}
```

Python one-liner shape:

```python
ch.basic_publish(exchange="", routing_key="events_intake", body=json.dumps(event).encode("utf-8"), properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"))
```

## Bulk Test Commands

Publish 100 valid events:

```bash
.venv/bin/python demo/bulk_publish.py --count 100 --drain-responses
```

The command prints a single observation line:

```text
bulk_publish_complete queue=events_intake count=100 valid=100 invalid=0 duplicates=0 elapsed_sec=... publish_per_sec=... total_payload_bytes=... avg_payload_bytes=... avg_publish_latency_ms=... p95_publish_latency_ms=... max_publish_latency_ms=... user_cpu_ms=... system_cpu_ms=... max_rss_kib=...
```

If it prints `rabbitmq_connection_failed url=amqp://***:***@localhost:5672/`, the script did not find `RABBITMQ_URL` in `.env` or your shell environment and fell back to localhost. Add `RABBITMQ_URL`, `RABBITMQ_QUEUE`, and `RABBITMQ_RESPONSE_QUEUE` to `.env`, or export them before running the command.

Publish 1,000 events at 50 messages/sec:

```bash
.venv/bin/python demo/bulk_publish.py --count 1000 --rate 50 --drain-responses
```

Publish mixed valid, invalid, and duplicate messages:

```bash
.venv/bin/python demo/bulk_publish.py --count 200 --invalid-every 25 --duplicate-every 40 --rate 25 --drain-responses
```

Publish 100 valid/pass-candidate events and 150 validation-failure events in one shot:

```bash
.venv/bin/python demo/bulk_publish.py --count 250 --failure-count 150 --failure-reason-mix --drain-responses
```

Publish 100 valid/pass-candidate events and 150 mixed error events in one shot:

```bash
.venv/bin/python demo/bulk_publish.py --count 250 --failure-count 150 --error-type-mix --drain-responses
```

Run parser-level RabbitMQ tests:

```bash
cargo test -p tadpole-tests rabbitmq_tests -- --nocapture
```

Run in-memory bulk intake performance test:

```bash
cargo test -p tadpole-tests bulk_intake_captures_resource_utilization_and_performance_observations -- --nocapture
```

## Test Cases

| ID | Scenario | Input | Expected Result |
| --- | --- | --- | --- |
| RB-BULK-001 | Small valid bulk | 100 valid invoice events | All messages ACKed, response queue receives success arrays, Mongo has 100 intake records |
| RB-BULK-002 | Medium valid bulk | 1,000 valid events at controlled rate | Consumer remains connected, no message loss, throughput recorded |
| RB-BULK-003 | High burst | 1,000 valid events with no rate limit | Queue may build temporarily, then drains; no consumer crash |
| RB-BULK-004 | Invalid JSON | Raw body `{ not valid json }` | Error response, original message NACKed with `requeue=false` |
| RB-BULK-005 | Missing `head` | `{"body":{}}` | Parser/test rejects; live pipeline returns error response |
| RB-BULK-006 | Blank tenant | Valid JSON with `tenant_id:""` | Validation error response, NACK without requeue |
| RB-BULK-007 | Duplicate event id | Reuse same `event_id` every Nth event | Pipeline should complete; action creation should remain idempotent for same action key |
| RB-BULK-008 | Mixed traffic | Valid plus every 25th invalid and every 40th duplicate | Valid messages continue processing after failures |
| RB-BULK-009 | Response queue verification | Consume `events_intake_response` after test | Success responses are arrays; failures are error objects |
| RB-BULK-010 | Restart/reconnect | Stop RabbitMQ briefly, then restore | Consumer logs reconnect attempts every 10s and resumes |

## Metrics To Capture

- Published message count
- Valid, invalid, and duplicate counts from `demo/bulk_publish.py`
- Publisher elapsed seconds and publish throughput
- Publisher total payload bytes and average payload size
- Publisher average, p95, and max `basic_publish` latency
- Publisher user CPU ms, system CPU ms, and peak RSS KiB
- RabbitMQ ready/unacked counts for `events_intake`
- Response message count on `events_intake_response`
- Engine logs for ACK/NACK, validation errors, and reconnects
- Mongo `intake_events` count for the test window
- Event log count for `rabbitmq_consumer`
- Metrics collection count
- Actions collection count for failing playbooks
- End-to-end elapsed time and approximate messages/sec

For engine-side in-memory resource observations, run:

```bash
cargo test -p tadpole-tests bulk_intake_captures_resource_utilization_and_performance_observations -- --nocapture
```

That test prints `bulk test observations` with event count, total elapsed time, average latency, throughput, user CPU, system CPU, peak RSS, RSS delta, persisted intake records, event logs, metric records, and serialized response bytes.

## Pass Criteria

- 100 percent of valid messages receive success responses or expected governance conclusions.
- Invalid JSON and validation failures generate error responses and do not block later valid messages.
- The input queue drains back to zero ready messages after processing completes.
- No consumer panic or process crash during burst tests.
- Duplicate/failure scenarios do not create unexpected duplicate action records.
- In-memory bulk test remains under its current thresholds: average latency below 50 ms, throughput above 20 events/sec, total elapsed below 5 sec for 100 events.

## Result Template

| Field | Value |
| --- | --- |
| Date/time | |
| Environment | |
| RabbitMQ URL/host | |
| Input queue | `events_intake` |
| Response queue | `events_intake_response` |
| Test command | |
| Published count | |
| Valid count | |
| Invalid count | |
| Duplicate count | |
| Publish elapsed/sec | |
| Publish throughput/sec | |
| Total payload bytes | |
| Avg payload bytes | |
| Avg publish latency ms | |
| P95 publish latency ms | |
| Max publish latency ms | |
| Publisher user CPU ms | |
| Publisher system CPU ms | |
| Publisher peak RSS KiB | |
| Engine avg latency us | |
| Engine throughput/sec | |
| Engine user CPU us | |
| Engine system CPU us | |
| Engine peak RSS KiB | |
| Engine processed count | |
| Response count | |
| Mongo intake count | |
| Action count | |
| Error count | |
| Final status | PASS / FAIL |
| Notes | |

## Current Coverage Added

RabbitMQ parser tests now cover:

- Valid full canonical envelope
- Minimal envelope with default body
- Compact single-line JSON payload
- Invalid JSON
- Empty body
- Non-object root
- Missing `head`
- Wrong `head` shape
- Non-string required fields
