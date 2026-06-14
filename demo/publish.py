#!/usr/bin/env python3
"""
Tadpole Engine — end-to-end demo publisher.

Usage:
    python3 demo/publish.py                          # sends example event
    python3 demo/publish.py schema/event-envelope.example.json
    python3 demo/publish.py --invalid                # validation failure demo

Requires:  pip3 install pika
"""

import json
import os
import sys
import time
import pika

# ── load .env ────────────────────────────────────────────────────────────────
_env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
if os.path.exists(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip())

RABBITMQ_URL   = os.getenv("RABBITMQ_URL",      "amqp://guest:guest@localhost:5672/")
INTAKE_QUEUE   = os.getenv("RABBITMQ_QUEUE",    "events_intake")
RESPONSE_QUEUE = os.getenv("RABBITMQ_RESPONSE_QUEUE", "events_intake_response")

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

# ── colours ───────────────────────────────────────────────────────────────────
G  = "\033[92m"   # green
R  = "\033[91m"   # red
Y  = "\033[93m"   # yellow
B  = "\033[94m"   # blue
C  = "\033[96m"   # cyan
W  = "\033[97m"   # white bold
DIM= "\033[2m"
RST= "\033[0m"

W80 = "─" * 72

def box(title):
    pad = (72 - len(title) - 2) // 2
    print(f"\n{'═' * 72}")
    print(f"{'║':1}{'':>{pad}}{W}{title}{RST}{'':>{72 - pad - len(title) - 1}}║")
    print(f"{'═' * 72}")

def section(title, colour=C):
    print(f"\n{colour}┌─ {title} {'─' * max(0, 65 - len(title))}┐{RST}")

def row(label, value, colour=W):
    print(f"  {DIM}{label:<28}{RST}{colour}{value}{RST}")

def divider():
    print(f"  {DIM}{W80}{RST}")

# ── display helpers ───────────────────────────────────────────────────────────

STATUS_COLOUR = {"PASS": G, "FAIL": R, "INCONCLUSIVE": Y}

def print_event_sent(event):
    h = event["head"]
    section("STEP 1 — EVENT PUBLISHED TO RabbitMQ", B)
    row("Queue",          INTAKE_QUEUE)
    row("event_id",       h.get("event_id", "—"))
    row("event_name",     h.get("event_name", "—"))
    row("event_category", h.get("event_category", "—"))
    row("tenant_id",      h.get("tenant_id") or f"{R}(blank){RST}")
    row("correlation_id", h.get("correlation_id", "—"))
    row("causation_id",   h.get("causation_id", "—"))
    row("occurred_at",    h.get("occurred_at", "—"))
    row("environment",    h.get("environment", "—"))
    row("changed_object", f"{h.get('changed_object_type','—')} / {h.get('changed_object_id','—')}")
    row("change_kind",    h.get("change_kind", "—"))
    divider()
    body = event.get("body", {})
    snapshots = body.get("snapshots", {})
    if snapshots:
        row("body.snapshots", "")
        for obj_type, fields in snapshots.items():
            print(f"  {'':28}{DIM}{obj_type}{RST}")
            for k, v in fields.items():
                print(f"  {'':30}{DIM}{k}{RST} = {W}{v}{RST}")


def print_validation_error(parsed):
    section("STEP 2 — VALIDATION", R)
    errors = parsed.get("errors") or [parsed.get("error", "unknown error")]
    row("Result", f"{R}FAILED{RST}")
    for e in errors:
        print(f"    {R}✗ {e}{RST}")
    section("STEP 3 — PIPELINE", DIM)
    print(f"  {DIM}(skipped — validation failed){RST}")
    section("STEP 4 — RESPONSE → events_intake_response", R)
    row("Status", f"{R}error{RST}")
    row("Errors", ", ".join(str(e) for e in errors))


def print_success_flow(envelopes):
    conclusion = envelopes[0]
    body       = conclusion.get("body", {})
    status     = body.get("status", "?")
    sc         = STATUS_COLOUR.get(status, W)

    # ── step 2: validation ────────────────────────────────────────────────────
    section("STEP 2 — VALIDATION", G)
    row("Result", f"{G}PASSED{RST}")

    # ── step 3: rule evaluation ───────────────────────────────────────────────
    section("STEP 3 — RULE EVALUATION & PIPELINE", C)
    playbook = body.get("matched_playbook")
    if playbook:
        row("Matched Playbook", f"{W}{playbook.get('name','?')}{RST}  {DIM}v{playbook.get('version','?')}{RST}")
        row("Playbook ID",      playbook.get("id", "—"))
        row("Execution Mode",   playbook.get("execution_mode", "—"))
        trigger = playbook.get("trigger", {})
        row("Trigger",          f"object_type={trigger.get('object_type','?')}  change_kind={trigger.get('change_kind','?')}")
    else:
        row("Matched Playbook", f"{Y}none — no playbook matched this event{RST}")

    rule_evals = body.get("rule_evaluations", [])
    if rule_evals:
        divider()
        print(f"  {C}Rule Evaluations ({len(rule_evals)}){RST}")
        for i, rule in enumerate(rule_evals, 1):
            r_status = rule.get("result", "?")
            r_colour = STATUS_COLOUR.get(r_status, W)
            print(f"\n  [{i}] {W}{rule.get('rule_name') or rule.get('rule_id','?')}{RST}")
            row("  rule_id",    rule.get("rule_id", "—"))
            row("  object_type",rule.get("object_type", "—"))
            row("  priority",   rule.get("priority", "—"))
            row("  is_critical",str(rule.get("is_critical", False)))
            row("  result",     f"{r_colour}{r_status}{RST}")
            row("  reason",     rule.get("reason", "—"))
            checks = rule.get("checks", [])
            if checks:
                row("  checks", "")
                for c in checks:
                    passed = c.get("passed", False)
                    sym    = f"{G}✓{RST}" if passed else f"{R}✗{RST}"
                    print(f"  {'':30}{sym} {DIM}{c.get('key','?')} {c.get('operator','?')} {c.get('expected','?')} (got {c.get('actual','?')}){RST}")
    else:
        divider()
        row("Rule Evaluations", f"{DIM}none (no playbook matched){RST}")

    # ── step 4: governance conclusion ─────────────────────────────────────────
    section("STEP 4 — GOVERNANCE CONCLUSION", sc)
    row("event_name",      conclusion.get("head", {}).get("event_name", "—"))
    row("event_id",        conclusion.get("head", {}).get("event_id", "—"))
    row("causation_id",    conclusion.get("head", {}).get("causation_id", "—"))
    divider()
    row("status",          f"{sc}{status}{RST}")
    row("conclusion_type", body.get("conclusion_type", "—"))
    row("rationale",       body.get("rationale", "—"))
    detail = body.get("final_decision_detail", {})
    if detail:
        row("final_decision",  f"{sc}{detail.get('decision','?')}{RST} — {detail.get('reason','')}")

    # ── step 5: metrics ───────────────────────────────────────────────────────
    metric_envelopes = envelopes[1:]
    section("STEP 5 — METRICS → events_intake_response", C if metric_envelopes else DIM)
    if metric_envelopes:
        for me in metric_envelopes:
            ename = me.get("head", {}).get("event_name", "?")
            mb    = me.get("body", {})
            if ename == "MetricRecorded":
                mt     = mb.get("metric_type", "?")
                colour = G if "pass" in mt else (R if "fail" in mt else Y)
                print(f"\n  {colour}● MetricRecorded  [{mt}]{RST}")
                row("  event_id",       me.get("head", {}).get("event_id", "—"))
                row("  metric_type",    mt)
                row("  value",          str(mb.get("value", "?")))
                row("  playbook_id",    mb.get("playbook_id") or "—")
                row("  rule_id",        mb.get("rule_id") or "—")
                row("  action_id",      mb.get("action_id") or "—")
                meta = mb.get("metadata") or {}
                if meta.get("decision"):
                    row("  decision",   str(meta.get("decision")))
                if meta.get("reason"):
                    row("  reason",     str(meta.get("reason")))
            elif ename == "KpiThresholdBreached":
                print(f"\n  {R}● KpiThresholdBreached{RST}")
                row("  threshold_id",   mb.get("threshold_id", "—"))
                row("  metric_type",    mb.get("metric_type", "—"))
                row("  observed_count", str(mb.get("observed_count", "?")))
                row("  max_count",      str(mb.get("max_count", "?")))
                row("  window_seconds", str(mb.get("window_seconds", "?")))
            else:
                print(f"\n  {Y}● {ename}{RST}")
                print(json.dumps(mb, indent=4))
    else:
        print(f"  {DIM}No metrics generated (no playbook matched).{RST}")

    # ── summary ───────────────────────────────────────────────────────────────
    print(f"\n{'═' * 72}")
    print(f"  DECISION:  {sc}{status}{RST}    events in response: {len(envelopes)}", end="")
    if metric_envelopes:
        print(f"  ({Y}{len(metric_envelopes)} breach alert(s){RST})", end="")
    print(f"\n{'═' * 72}\n")


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    if "--invalid" in sys.argv:
        event = INVALID_EVENT
        label = "INVALID event (blank tenant_id)"
    elif len(sys.argv) > 1 and not sys.argv[1].startswith("--"):
        with open(sys.argv[1]) as f:
            event = json.load(f)
        label = f"event from {sys.argv[1]}"
    else:
        event = EXAMPLE_EVENT
        label = "example event"

    box(f"TADPOLE ENGINE  —  {label}")

    params = pika.URLParameters(RABBITMQ_URL)
    conn   = pika.BlockingConnection(params)
    ch     = conn.channel()
    ch.queue_declare(queue=INTAKE_QUEUE,   durable=True)
    ch.queue_declare(queue=RESPONSE_QUEUE, durable=True)

    print_event_sent(event)

    # drain any stale responses from previous runs
    drained = 0
    while True:
        _, _, stale = ch.basic_get(queue=RESPONSE_QUEUE, auto_ack=True)
        if stale is None:
            break
        drained += 1
    if drained:
        print(f"\n  {DIM}Drained {drained} stale message(s) from [{RESPONSE_QUEUE}]{RST}")

    ch.basic_publish(
        exchange    = "",
        routing_key = INTAKE_QUEUE,
        body        = json.dumps(event).encode(),
        properties  = pika.BasicProperties(
            delivery_mode = 2,
            content_type  = "application/json",
        ),
    )

    print(f"\n  {DIM}Message delivered to [{INTAKE_QUEUE}]. Waiting for engine response…{RST}")

    raw      = None
    deadline = time.time() + 15
    while time.time() < deadline:
        _, _, raw = ch.basic_get(queue=RESPONSE_QUEUE, auto_ack=True)
        if raw is not None:
            break
        time.sleep(0.3)

    conn.close()

    if raw is None:
        print(f"\n  {R}✗ No response received within 15 seconds.{RST}")
        print(f"  {DIM}Is the engine running?  →  cargo run -p http-gateway{RST}\n")
        sys.exit(1)

    parsed = json.loads(raw)

    # error response  {"status":"error", ...}
    if isinstance(parsed, dict) and parsed.get("status") == "error":
        print_validation_error(parsed)
        print(f"\n{'═' * 72}\n  {R}RESULT: ERROR{RST}\n{'═' * 72}\n")
        return

    # success response  [ CanonicalEnvelope, ... ]
    if isinstance(parsed, list):
        print_success_flow(parsed)
        return

    # fallback
    section("RAW RESPONSE", Y)
    print(json.dumps(parsed, indent=2))


if __name__ == "__main__":
    main()
