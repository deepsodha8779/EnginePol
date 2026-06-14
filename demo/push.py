#!/usr/bin/env python3
"""
Push an event to events_intake WITHOUT consuming the response.
Use this together with monitor.py --watch to see responses live.

Usage:
    python3 demo/push.py            # sends example event
    python3 demo/push.py --invalid  # sends invalid event
"""

import json, os, sys, pika

_env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
if os.path.exists(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip())

RABBITMQ_URL = os.getenv("RABBITMQ_URL",   "amqp://guest:guest@localhost:5672/")
INTAKE_QUEUE = os.getenv("RABBITMQ_QUEUE", "events_intake")

EXAMPLE_EVENT = {
    "head": {
        "event_id":               "01KVSYJ8M2Q5ZP4H6X7N8R9T0A",
        "event_name":             "InvoiceReceivedForExternalDependency",
        "event_category":         "Transaction",
        "tenant_id":              "acme-corp",
        "correlation_id":         "01KVSYJ8M2Q5ZP4H6X7N8R9T0B",
        "causation_id":           "01KVSYJ8M2Q5ZP4H6X7N8R9T0C",
        "occurred_at":            "2026-02-23T12:34:56Z",
        "originating_function":   "Finance",
        "originating_application":"AccountingIntegration",
        "environment":            "prd",
        "external_dependency_id": "01KVSYJ8M2Q5ZP4H6X7N8R9T0D",
        "changed_object_type":    "invoice",
        "changed_object_id":      "01KVSYJ8M2Q5ZP4H6X7N8R9T0E",
        "change_kind":            "created",
    },
    "body": {
        "snapshots": {
            "invoice": {
                "invoice_amount":   1500,
                "invoice_currency": "GBP",
            }
        }
    },
}

INVALID_EVENT = {
    "head": {
        "event_id":               "01JFNKM0Y7Q0ZJ9W8YH5ARV0FQ",
        "event_name":             "InvoiceReceivedForExternalDependency",
        "event_category":         "Transaction",
        "tenant_id":              "",
        "external_dependency_id": "01J0M8WZ9K7F6R5T4P3Q2N1M0",
    },
    "body": {},
}

event = INVALID_EVENT if "--invalid" in sys.argv else EXAMPLE_EVENT

conn = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
ch   = conn.channel()
ch.queue_declare(queue=INTAKE_QUEUE, durable=True)
ch.basic_publish(
    exchange    = "",
    routing_key = INTAKE_QUEUE,
    body        = json.dumps(event).encode(),
    properties  = pika.BasicProperties(delivery_mode=2, content_type="application/json"),
)
conn.close()

print(f"→ Pushed to [{INTAKE_QUEUE}]  event_id={event['head'].get('event_id')}  tenant_id={event['head'].get('tenant_id')!r}")
