# RabbitMQ Bulk Test Result - 200 Mixed Events

## Summary

Bulk publishing to RabbitMQ completed successfully for a controlled 200-message mixed test: `100` valid events and `100` intentionally failing events. The failing half was split evenly across five validation failure reasons so the consumer path is exercised with different bad-message shapes.

This report records the producer-side performance metrics from the actual command output and a broker-side queue-depth check after the run. The input queue drained to zero ready messages after publishing, confirming RabbitMQ accepted the batch and consumers took the messages from `events_intake`.

## Test Details

| Field | Value |
| --- | --- |
| Test type | RabbitMQ mixed bulk publish |
| Run date | `2026-06-02` |
| Command | `.venv/bin/python demo/bulk_publish.py --count 200 --failure-count 100 --failure-reason-mix --drain-responses` |
| Input queue | `events_intake` |
| Response queue | `events_intake_response` |
| Response queue cleanup before run | Enabled |
| Stale responses drained before run | `174` |
| Published events | `200` |
| Expected pass events | `100` |
| Expected fail events | `100` |
| Duplicate event IDs | `0` |
| Rate limit requested | None |
| Final publish status | PASS |

## Traffic Mix

| Category | Count | Notes |
| --- | --- | --- |
| Valid messages | `100` | Normal canonical `InvoiceReceivedForExternalDependency` envelopes |
| Invalid messages | `100` | Evenly split across five failure reasons |
| Duplicate event IDs | `0` | This run focused on pass/fail validation behavior, not dedupe behavior |
| Total published | `200` | All messages were sent to RabbitMQ |

## Failure Reason Breakdown

| Failure reason | Count | Expected engine behavior |
| --- | ---: | --- |
| Blank `head.tenant_id` | `20` | Pipeline validation error: `head.tenant_id is required`; NACK without requeue |
| Invalid `head.event_id` | `20` | Pipeline validation error: `head.event_id must be a valid ULID`; NACK without requeue |
| Blank `head.event_name` | `20` | Pipeline validation error: `head.event_name is required`; NACK without requeue |
| Invalid `head.change_kind` | `20` | Pipeline validation error: `head.change_kind must be create/created, update/updated, or delete/deleted`; NACK without requeue |
| Null `body` | `20` | Pipeline validation error: `body is required`; NACK without requeue |
| Total failing messages | `100` | All failure cases are structurally valid JSON and should reach pipeline validation |

## Performance Observations

| Metric | Value |
| --- | --- |
| Total publish elapsed time | `0.527 sec` |
| Publish throughput | `379.61 events/sec` |
| Total payload size | `140620 bytes` |
| Average payload size | `703.1 bytes/event` |
| Average publish latency | `2.584 ms` |
| P95 publish latency | `0.106 ms` |
| Max publish latency | `219.314 ms` |
| User CPU time | `17.301 ms` |
| System CPU time | `3.295 ms` |
| Peak RSS memory | `31984 KiB` |

## Broker Queue Check After Run

| Queue | Ready messages | Consumers | Observation |
| --- | ---: | ---: | --- |
| `events_intake` | `0` | `2` | Input queue drained after the batch |
| `events_intake_response` | `20` | `0` | Response queue had visible response messages after the run |

## Interpretation

- RabbitMQ accepted all `200` messages from the publisher.
- The requested test split was met exactly: `100` valid messages and `100` invalid messages.
- Failure reasons were distributed evenly: `20` messages per reason.
- No rate limit was applied. The publisher sent the 200-message batch in `0.527 sec`, for `379.61 events/sec`.
- Publish latency had one large outlier: max latency was `219.314 ms`. This is why average latency (`2.584 ms`) is higher than p95 (`0.106 ms`).
- Publisher resource usage stayed low for the run: `20.596 ms` combined CPU time and about `31.2 MiB` peak RSS.
- The input queue drained to `0`, which is the key RabbitMQ-side confirmation that the queued batch was consumed.
- The response queue count was `20` after the run. Treat that as observed broker state, not as a full response-count assertion, because this check only measured ready messages in the response queue after processing.

## Expected Consumer Behavior

| Message type | Expected behavior |
| --- | --- |
| Valid messages | Process through the intake pipeline, publish governance response events, and ACK original message |
| Invalid pipeline-validation messages | Publish an error response body and NACK original message without requeue |
| Input queue after processing | `events_intake` returns to `0` ready messages |
| Response queue after processing | Contains visible success/error response messages for consumers to inspect |

## Raw Output

```text
drained_responses=174
bulk_publish_complete queue=events_intake count=200 valid=100 invalid=100 duplicates=0 elapsed_sec=0.527 publish_per_sec=379.61 total_payload_bytes=140620 avg_payload_bytes=703.1 avg_publish_latency_ms=2.584 p95_publish_latency_ms=0.106 max_publish_latency_ms=219.314 user_cpu_ms=17.301 system_cpu_ms=3.295 max_rss_kib=31984
failure_reason_counts blank_tenant_id=20 invalid_event_id=20 blank_event_name=20 invalid_change_kind=20 null_body=20
```

## Queue Check Output

```text
queue_status name=events_intake ready=0 consumers=2
queue_status name=events_intake_response ready=20 consumers=0
```

## Conclusion

The 200-message mixed RabbitMQ bulk publish test passed for the requested pass/fail split without a publisher-side rate limit. The publisher sent exactly `100` valid and `100` invalid messages in `0.527 sec`, with failures distributed across five different validation reasons, and RabbitMQ drained the input queue after the run.
