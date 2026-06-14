# RabbitMQ Bulk Test Result - 100 Events

## Summary

Bulk publishing to RabbitMQ completed successfully. The test publisher connected to RabbitMQ, cleared stale response messages, and published 100 valid canonical Tadpole event envelopes to the `events_intake` queue.

This result confirms the producer-side RabbitMQ path is working. Consumer-side completion should be verified separately by checking the `events_intake` queue drain, `events_intake_response` messages, gateway logs, and MongoDB intake records.

## Test Details

| Field | Value |
| --- | --- |
| Test type | RabbitMQ bulk publish |
| Command | `.venv/bin/python demo/bulk_publish.py --count 100 --drain-responses` |
| Input queue | `events_intake` |
| Response queue cleanup | Enabled |
| Stale responses drained | `60` |
| Published events | `100` |
| Valid events | `100` |
| Invalid events | `0` |
| Duplicate events | `0` |
| Final publish status | PASS |

## Performance Observations

| Metric | Value |
| --- | --- |
| Total publish elapsed time | `0.009 sec` |
| Publish throughput | `11389.52 events/sec` |
| Total payload size | `73100 bytes` |
| Average payload size | `731.0 bytes/event` |
| Average publish latency | `0.044 ms` |
| P95 publish latency | `0.073 ms` |
| Max publish latency | `0.365 ms` |
| User CPU time | `6.973 ms` |
| System CPU time | `1.690 ms` |
| Peak RSS memory | `32112 KiB` |

## Interpretation

- RabbitMQ accepted all 100 test messages from the publisher.
- No invalid or duplicate events were generated in this run.
- The publish path was very fast, with average publish latency below `0.1 ms`.
- Publisher resource usage was low: less than `10 ms` total CPU time and about `32 MiB` peak memory.
- `60` stale messages were present in the response queue before the test and were drained before publishing, so new response checks can be evaluated cleanly after this run.

## Follow-Up Verification

To confirm full end-to-end processing, verify these items after the engine consumer runs:

| Check | Expected Result |
| --- | --- |
| RabbitMQ `events_intake` ready messages | Queue drains to `0` |
| RabbitMQ `events_intake` unacked messages | Returns to `0` after processing |
| RabbitMQ `events_intake_response` | Contains responses for processed messages |
| Gateway logs | Shows RabbitMQ messages received, processed, and ACKed |
| MongoDB `intake_events` | Contains 100 new intake records for this test window |
| MongoDB event logs | Contains `rabbitmq_consumer` receive/ack entries |
| Actions collection | Only populated if the active playbook produces FAIL decisions |

## Raw Output

```text
drained_responses=60
bulk_publish_complete queue=events_intake count=100 valid=100 invalid=0 duplicates=0 elapsed_sec=0.009 publish_per_sec=11389.52 total_payload_bytes=73100 avg_payload_bytes=731.0 avg_publish_latency_ms=0.044 p95_publish_latency_ms=0.073 max_publish_latency_ms=0.365 user_cpu_ms=6.973 system_cpu_ms=1.690 max_rss_kib=32112
```

## Conclusion

The 100-event RabbitMQ bulk publish test passed for the producer side. The publisher successfully sent all generated events to `events_intake` with low latency and low resource usage. Full end-to-end PASS should be recorded after confirming the consumer processed the queue and persisted the expected records.
