#!/usr/bin/env python3
"""
Publish many canonical Tadpole events to RabbitMQ for bulk intake testing.

Usage:
    python3 demo/bulk_publish.py --count 100
    python3 demo/bulk_publish.py --count 1000 --rate 50
    python3 demo/bulk_publish.py --count 200 --invalid-every 25 --duplicate-every 40
    python3 demo/bulk_publish.py --count 200 --failure-count 100 --failure-reason-mix
    python3 demo/bulk_publish.py --count 250 --failure-count 150 --error-type-mix

Requires:
    pip3 install pika
"""

import argparse
import json
import os
import random
import resource
import sys
import time
from datetime import datetime, timezone

import pika


_ENV_PATH = os.path.join(os.path.dirname(__file__), "..", ".env")
if os.path.exists(_ENV_PATH):
    with open(_ENV_PATH) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip())


RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
INTAKE_QUEUE = os.getenv("RABBITMQ_QUEUE", "events_intake")
RESPONSE_QUEUE = os.getenv("RABBITMQ_RESPONSE_QUEUE", "events_intake_response")
CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
FAILURE_REASONS = [
    "blank_tenant_id",
    "invalid_event_id",
    "blank_event_name",
    "invalid_change_kind",
    "null_body",
]
ERROR_TYPE_REASONS = [
    "malformed_json",
    "array_root",
    "string_root",
    "missing_head",
    "wrong_head_shape",
    "blank_tenant_id",
    "invalid_event_id",
    "blank_event_name",
    "invalid_change_kind",
    "null_body",
]


def ulid_like():
    # 26 Crockford Base32 chars are enough for test data accepted by validation.
    timestamp_ms = int(time.time() * 1000)
    chars = []
    value = timestamp_ms
    for _ in range(10):
        chars.append(CROCKFORD[value % 32])
        value //= 32
    chars.reverse()
    chars.extend(random.choice(CROCKFORD) for _ in range(16))
    return "".join(chars)


def event_for(index, tenant, invalid=False, duplicate_event_id=None, failure_reason=None):
    event_id = duplicate_event_id or ulid_like()
    object_id = ulid_like()
    tenant_id = "" if invalid or failure_reason == "blank_tenant_id" else tenant
    amount = 1000 + index

    event = {
        "head": {
            "event_id": event_id,
            "event_name": "InvoiceReceivedForExternalDependency",
            "event_category": "Transaction",
            "tenant_id": tenant_id,
            "correlation_id": ulid_like(),
            "causation_id": None,
            "occurred_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "originating_function": "Finance",
            "originating_application": "BulkRabbitMqTest",
            "environment": "bulk-test",
            "external_dependency_id": ulid_like(),
            "changed_object_type": "Invoice",
            "changed_object_id": object_id,
            "change_kind": "created",
        },
        "body": {
            "snapshots": [
                {
                    "object_type": "Invoice",
                    "object_id": object_id,
                    "supplier_reference": f"SUP-BULK-{index:06d}",
                    "amount": amount,
                    "currency": "EUR",
                    "description": "RabbitMQ bulk test event",
                }
            ]
        },
    }
    if failure_reason == "invalid_event_id":
        event["head"]["event_id"] = "invalid-event-id"
    elif failure_reason == "blank_event_name":
        event["head"]["event_name"] = ""
    elif failure_reason == "invalid_change_kind":
        event["head"]["change_kind"] = "archived"
    elif failure_reason == "null_body":
        event["body"] = None
    return event


def failure_reason_for(failure_index):
    return FAILURE_REASONS[(failure_index - 1) % len(FAILURE_REASONS)]


def error_type_reason_for(failure_index):
    return ERROR_TYPE_REASONS[(failure_index - 1) % len(ERROR_TYPE_REASONS)]


def payload_for(event, error_reason):
    if error_reason == "malformed_json":
        return b"{ not valid json }"
    if error_reason == "array_root":
        return b"[]"
    if error_reason == "string_root":
        return b'"not a canonical envelope"'
    if error_reason == "missing_head":
        return json.dumps({"body": {"error_case": "missing_head"}, "tail": {}}).encode("utf-8")
    if error_reason == "wrong_head_shape":
        return json.dumps({"head": [], "body": {"error_case": "wrong_head_shape"}}).encode("utf-8")
    return json.dumps(event, separators=(",", ":")).encode("utf-8")


def drain_response_queue(channel):
    drained = 0
    while True:
        _, _, body = channel.basic_get(queue=RESPONSE_QUEUE, auto_ack=True)
        if body is None:
            return drained
        drained += 1


def max_rss_kib(usage):
    # macOS reports ru_maxrss in bytes; Linux reports KiB.
    return int(usage.ru_maxrss / 1024) if sys.platform == "darwin" else int(usage.ru_maxrss)


def masked_url(url):
    if "://" not in url or "@" not in url:
        return url
    scheme, rest = url.split("://", 1)
    _, host = rest.rsplit("@", 1)
    return f"{scheme}://***:***@{host}"


def main():
    parser = argparse.ArgumentParser(description="RabbitMQ bulk publisher for Tadpole events")
    parser.add_argument("--count", type=int, default=100, help="number of events to publish")
    parser.add_argument("--tenant", default="acme-corp", help="tenant_id for valid events")
    parser.add_argument("--rate", type=float, default=0, help="max publish rate per second; 0 means unlimited")
    parser.add_argument("--invalid-every", type=int, default=0, help="make every Nth event invalid")
    parser.add_argument("--duplicate-every", type=int, default=0, help="reuse a previous event_id every Nth event")
    parser.add_argument(
        "--failure-count",
        type=int,
        default=0,
        help="make exactly this many events invalid at the end of the run",
    )
    parser.add_argument(
        "--failure-reason-mix",
        action="store_true",
        help="spread --failure-count across several validation failure reasons",
    )
    parser.add_argument(
        "--error-type-mix",
        action="store_true",
        help="spread --failure-count across JSON parse, envelope shape, and validation errors",
    )
    parser.add_argument("--drain-responses", action="store_true", help="clear response queue before publishing")
    args = parser.parse_args()
    if args.failure_count < 0 or args.failure_count > args.count:
        parser.error("--failure-count must be between 0 and --count")
    if args.failure_count and not (args.failure_reason_mix or args.error_type_mix):
        parser.error("--failure-count requires --failure-reason-mix or --error-type-mix")
    if args.failure_reason_mix and args.error_type_mix:
        parser.error("use only one of --failure-reason-mix or --error-type-mix")

    params = pika.URLParameters(RABBITMQ_URL)
    try:
        conn = pika.BlockingConnection(params)
    except pika.exceptions.AMQPConnectionError as exc:
        print(
            "rabbitmq_connection_failed "
            f"url={masked_url(RABBITMQ_URL)} queue={INTAKE_QUEUE} "
            "reason=AMQPConnectionError "
            "hint='Check RABBITMQ_URL, network/VPN/firewall, credentials, vhost, and whether the broker is running.'",
            file=sys.stderr,
        )
        raise SystemExit(2) from exc

    channel = conn.channel()
    channel.queue_declare(queue=INTAKE_QUEUE, durable=True)
    channel.queue_declare(queue=RESPONSE_QUEUE, durable=True)

    if args.drain_responses:
        drained = drain_response_queue(channel)
        print(f"drained_responses={drained}")

    first_event_id = None
    started_at = time.time()
    resource_start = resource.getrusage(resource.RUSAGE_SELF)
    delay = 1.0 / args.rate if args.rate > 0 else 0
    valid_count = 0
    invalid_count = 0
    duplicate_count = 0
    total_payload_bytes = 0
    publish_latencies_ms = []
    reason_names = ERROR_TYPE_REASONS if args.error_type_mix else FAILURE_REASONS
    failure_reason_counts = {reason: 0 for reason in reason_names}

    for index in range(1, args.count + 1):
        invalid = args.invalid_every > 0 and index % args.invalid_every == 0
        failure_reason = None
        if (args.failure_reason_mix or args.error_type_mix) and index > args.count - args.failure_count:
            failure_index = index - (args.count - args.failure_count)
            failure_reason = (
                error_type_reason_for(failure_index)
                if args.error_type_mix
                else failure_reason_for(failure_index)
            )
            failure_reason_counts[failure_reason] += 1
            invalid = True
        duplicate_event_id = None
        if args.duplicate_every > 0 and first_event_id and index % args.duplicate_every == 0:
            duplicate_event_id = first_event_id
            duplicate_count += 1

        event = event_for(
            index,
            args.tenant,
            invalid=invalid,
            duplicate_event_id=duplicate_event_id,
            failure_reason=failure_reason,
        )
        first_event_id = first_event_id or event["head"]["event_id"]
        payload = payload_for(event, failure_reason)
        total_payload_bytes += len(payload)

        publish_started = time.perf_counter()
        channel.basic_publish(
            exchange="",
            routing_key=INTAKE_QUEUE,
            body=payload,
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
                message_id=event["head"].get("event_id") or f"bulk-error-{index}",
                type=event["head"].get("event_name") or "BulkRabbitMqErrorTest",
            ),
        )
        publish_latencies_ms.append((time.perf_counter() - publish_started) * 1000.0)

        if invalid:
            invalid_count += 1
        else:
            valid_count += 1

        if delay:
            time.sleep(delay)

    elapsed = time.time() - started_at
    resource_end = resource.getrusage(resource.RUSAGE_SELF)
    conn.close()

    throughput = args.count / elapsed if elapsed else args.count
    avg_payload_bytes = total_payload_bytes / args.count if args.count else 0
    avg_publish_latency_ms = (
        sum(publish_latencies_ms) / len(publish_latencies_ms) if publish_latencies_ms else 0
    )
    max_publish_latency_ms = max(publish_latencies_ms) if publish_latencies_ms else 0
    sorted_latencies = sorted(publish_latencies_ms)
    p95_index = (
        min(len(sorted_latencies) - 1, max(0, int(len(sorted_latencies) * 0.95) - 1))
        if sorted_latencies
        else 0
    )
    p95_publish_latency_ms = sorted_latencies[p95_index] if sorted_latencies else 0
    user_cpu_ms = (resource_end.ru_utime - resource_start.ru_utime) * 1000.0
    system_cpu_ms = (resource_end.ru_stime - resource_start.ru_stime) * 1000.0
    peak_rss_kib = max_rss_kib(resource_end)

    print(
        "bulk_publish_complete "
        f"queue={INTAKE_QUEUE} count={args.count} valid={valid_count} invalid={invalid_count} "
        f"duplicates={duplicate_count} elapsed_sec={elapsed:.3f} publish_per_sec={throughput:.2f} "
        f"total_payload_bytes={total_payload_bytes} avg_payload_bytes={avg_payload_bytes:.1f} "
        f"avg_publish_latency_ms={avg_publish_latency_ms:.3f} "
        f"p95_publish_latency_ms={p95_publish_latency_ms:.3f} "
        f"max_publish_latency_ms={max_publish_latency_ms:.3f} "
        f"user_cpu_ms={user_cpu_ms:.3f} system_cpu_ms={system_cpu_ms:.3f} "
        f"max_rss_kib={peak_rss_kib}"
    )
    if args.failure_reason_mix or args.error_type_mix:
        print(
            "failure_reason_counts "
            + " ".join(f"{reason}={count}" for reason, count in failure_reason_counts.items())
        )


if __name__ == "__main__":
    main()
