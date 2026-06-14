# RabbitMQ Bulk Test Result - 250 Events, 100 Valid and 150 Mixed Error Cases

## Summary

A one-shot RabbitMQ bulk test completed successfully with 250 published messages. The publisher connected to RabbitMQ, drained stale responses, and sent all messages to the `events_intake` queue in a single burst.

This run was designed to test more than validation failures. It included valid messages plus multiple error categories:

- malformed JSON parse errors
- non-object JSON root errors
- missing or wrongly shaped envelope errors
- validation failures

This confirms the producer can generate a mixed error workload for RabbitMQ and pipeline resilience testing. Full end-to-end behavior should still be confirmed from the consumer responses, gateway logs, queue state, and MongoDB records.

## Test Details

| Field | Value |
| --- | --- |
| Test type | RabbitMQ one-shot mixed error bulk publish |
| Command | `.venv/bin/python demo/bulk_publish.py --count 250 --failure-count 150 --error-type-mix --drain-responses` |
| Input queue | `events_intake` |
| Response queue cleanup | Enabled |
| Stale responses drained | `250` |
| Published events | `250` |
| Valid/pass-candidate events | `100` |
| Mixed error events | `150` |
| Duplicate events | `0` |
| Rate limit | None, one-shot burst |
| Final publish status | PASS |

## Error Mix

The 150 error cases were spread evenly across 10 failure types.

| Error Type | Count | Expected Engine Behavior |
| --- | --- | --- |
| Malformed JSON | `15` | JSON parse error response, NACK without requeue |
| Array root | `15` | Envelope deserialize error response, NACK without requeue |
| String root | `15` | Envelope deserialize error response, NACK without requeue |
| Missing `head` | `15` | Envelope deserialize error response, NACK without requeue |
| Wrong `head` shape | `15` | Envelope deserialize error response, NACK without requeue |
| Blank tenant ID | `15` | Validation error response, NACK without requeue |
| Invalid event ID | `15` | Validation error response, NACK without requeue |
| Blank event name | `15` | Validation error response, NACK without requeue |
| Invalid change kind | `15` | Validation error response, NACK without requeue |
| Null body | `15` | Validation error response, NACK without requeue |
| Total mixed errors | `150` | Error response expected |

## Performance Observations

| Metric | Value |
| --- | --- |
| Total publish elapsed time | `0.388 sec` |
| Publish throughput | `644.97 events/sec` |
| Total payload size | `126050 bytes` |
| Average payload size | `504.2 bytes/event` |
| Average publish latency | `1.507 ms` |
| P95 publish latency | `0.115 ms` |
| Max publish latency | `190.508 ms` |
| User CPU time | `19.298 ms` |
| System CPU time | `3.463 ms` |
| Peak RSS memory | `32624 KiB` |

## Performance Interpretation

- RabbitMQ accepted all 250 messages from the publisher.
- The one-shot burst completed in `0.388 sec`.
- Publish throughput was `644.97 events/sec`, higher than the previous validation-only run.
- Average payload size dropped to `504.2 bytes/event` because several error payloads were intentionally small raw JSON or malformed JSON strings.
- P95 publish latency was very low at `0.115 ms`.
- Max publish latency was `190.508 ms`, indicating one or a few publish outliers while most publishes remained fast.
- Publisher CPU usage remained low at about `22.761 ms` combined user/system CPU.
- Peak memory was about `31.86 MiB`.

## Current Logic Fit

This test fits the current RabbitMQ consumer logic:

- Malformed JSON and incompatible envelope shapes fail at `parse_envelope_from_bytes`.
- Parse/deserialization failures publish an error response and NACK the original message with `requeue=false`.
- Structurally valid envelopes then enter `process_envelope`.
- Validation failures are recorded, publish an error response, and NACK with `requeue=false`.
- Valid messages should continue into playbook lookup, rule evaluation, governance response publishing, and ACK.

Important note: the 100 valid messages are pass-candidate messages. Actual governance `PASS` depends on the active playbook and rule configuration. If no matching playbook exists for `Invoice + created`, valid messages may complete as `INCONCLUSIVE` rather than `PASS`.

## Follow-Up Verification

To mark this as a full end-to-end result, verify:

| Check | Expected Result |
| --- | --- |
| RabbitMQ `events_intake` ready messages | Queue drains to `0` |
| RabbitMQ `events_intake` unacked messages | Returns to `0` after processing |
| RabbitMQ `events_intake_response` | Contains responses for 100 valid messages and 150 error messages |
| Gateway logs | Shows parse errors, validation failures, valid ACKs, and invalid NACKs without requeue |
| MongoDB `intake_events` | Contains success records for valid messages and validation failure records for validation-layer errors |
| MongoDB event logs | Contains `rabbitmq_consumer` receive/ack/nack entries where an envelope could be parsed |
| Governance outcomes | Valid events show `PASS`, `FAIL`, or `INCONCLUSIVE` according to active playbook/rule configuration |

## Raw Output

```text
drained_responses=250
bulk_publish_complete queue=events_intake count=250 valid=100 invalid=150 duplicates=0 elapsed_sec=0.388 publish_per_sec=644.97 total_payload_bytes=126050 avg_payload_bytes=504.2 avg_publish_latency_ms=1.507 p95_publish_latency_ms=0.115 max_publish_latency_ms=190.508 user_cpu_ms=19.298 system_cpu_ms=3.463 max_rss_kib=32624
failure_reason_counts malformed_json=15 array_root=15 string_root=15 missing_head=15 wrong_head_shape=15 blank_tenant_id=15 invalid_event_id=15 blank_event_name=15 invalid_change_kind=15 null_body=15
```

## Conclusion

The 250-message one-shot mixed-error RabbitMQ bulk publish test passed for the producer side. The publisher successfully sent 100 valid/pass-candidate messages and 150 mixed error messages covering JSON parse errors, envelope shape errors, and validation failures. Performance remained strong, with sub-second total publish time and low CPU/memory usage. Full end-to-end PASS should be recorded after confirming the consumer returned the expected success/error responses and persisted the expected records.
