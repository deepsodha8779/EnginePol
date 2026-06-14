#!/usr/bin/env python3
"""
Monitor events_intake_response queue.

Modes:
    python3 demo/monitor.py          # drain & display all queued messages then exit
    python3 demo/monitor.py --watch  # live: print each message as it arrives (Ctrl+C to stop)
    python3 demo/monitor.py --count  # just print the message count and exit
"""

import json
import os
import sys
import time
import pika

# ── load .env ─────────────────────────────────────────────────────────────────
_env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
if os.path.exists(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip())

RABBITMQ_URL   = os.getenv("RABBITMQ_URL",      "amqp://guest:guest@localhost:5672/")
RESPONSE_QUEUE = os.getenv("RABBITMQ_RESPONSE_QUEUE", "events_intake_response")

G   = "\033[92m"; R = "\033[91m"; Y = "\033[93m"
B   = "\033[94m"; C = "\033[96m"; W = "\033[97m"
DIM = "\033[2m";  RST = "\033[0m"

METRIC_COLOUR = {
    "rule.pass":             G,
    "rule.fail":             R,
    "rule.inconclusive":     Y,
    "action.triggered":      C,
    "kpi.threshold_breach":  R,
}

STATUS_COLOUR = {"PASS": G, "FAIL": R, "INCONCLUSIVE": Y}


def fmt_metric_type(mt):
    c = METRIC_COLOUR.get(mt, W)
    return f"{c}{mt}{RST}"


def print_envelope(idx, envelope, ts=None):
    head  = envelope.get("head", {}) if isinstance(envelope, dict) else {}
    body  = envelope.get("body", {}) if isinstance(envelope, dict) else {}
    ename = head.get("event_name", "?")
    prefix = f"[{idx:>3}]"

    if ename == "MetricRecorded":
        mt = body.get("metric_type", "?")
        c  = METRIC_COLOUR.get(mt, W)
        print(f"  {prefix} {c}MetricRecorded{RST}  metric_type={fmt_metric_type(mt)}"
              f"  value={W}{body.get('value','?')}{RST}"
              f"  rule_id={DIM}{body.get('rule_id') or '—'}{RST}"
              f"  playbook_id={DIM}{body.get('playbook_id') or '—'}{RST}")
        meta = body.get("metadata") or {}
        if meta.get("reason"):
            print(f"  {'':6} {DIM}reason: {meta['reason']}{RST}")

    elif ename == "KpiThresholdBreached":
        print(f"  {prefix} {R}KpiThresholdBreached{RST}"
              f"  threshold={W}{body.get('threshold_id','?')}{RST}"
              f"  observed={R}{body.get('observed_count','?')}{RST}/{body.get('max_count','?')}"
              f"  window={body.get('window_seconds','?')}s")

    elif ename == "GovernanceConclusionEmitted":
        status = body.get("status", "?")
        sc = STATUS_COLOUR.get(status, W)
        print(f"  {prefix} {sc}GovernanceConclusionEmitted{RST}"
              f"  status={sc}{status}{RST}"
              f"  conclusion={W}{body.get('conclusion_type','?')}{RST}"
              f"  tenant={DIM}{head.get('tenant_id','?')}{RST}")

    else:
        print(f"  {prefix} {Y}{ename}{RST}  event_id={DIM}{head.get('event_id','?')}{RST}")

    if ts:
        print(f"  {'':6} {DIM}received at {ts}{RST}")


def summarise(messages):
    counts = {}
    for msg in messages:
        if isinstance(msg, list):
            for env in msg:
                k = env.get("head", {}).get("event_name", "unknown") if isinstance(env, dict) else "unknown"
                mt = env.get("body", {}).get("metric_type", "") if isinstance(env, dict) else ""
                label = f"{k}({mt})" if mt else k
                counts[label] = counts.get(label, 0) + 1
        elif isinstance(msg, dict):
            k = msg.get("status", msg.get("head", {}).get("event_name", "unknown"))
            counts[k] = counts.get(k, 0) + 1

    print(f"\n  {'─'*60}")
    print(f"  {W}Summary{RST}")
    for label, n in sorted(counts.items(), key=lambda x: -x[1]):
        c = METRIC_COLOUR.get(label.split("(")[-1].rstrip(")"), W)
        print(f"    {c}{label:<45}{RST} {W}{n:>4}x{RST}")


def drain_mode(ch):
    print(f"\n  {C}Draining [{RESPONSE_QUEUE}]…{RST}\n")
    messages = []
    idx = 1
    while True:
        _, _, raw = ch.basic_get(queue=RESPONSE_QUEUE, auto_ack=True)
        if raw is None:
            break
        parsed = json.loads(raw)
        messages.append(parsed)

        if isinstance(parsed, list):
            print(f"  {DIM}── message {idx} ({len(parsed)} envelope(s)) ──────────────────────{RST}")
            for i, env in enumerate(parsed, 1):
                print_envelope(i, env)
        elif isinstance(parsed, dict):
            status = parsed.get("status", "?")
            sc = R if status == "error" else G
            print(f"  {DIM}── message {idx} ──────────────────────────────────────────{RST}")
            print(f"  [  1] {sc}Error response{RST}  status={sc}{status}{RST}")
            errs = parsed.get("errors") or [parsed.get("error", "?")]
            for e in errs:
                print(f"         {R}✗ {e}{RST}")
        idx += 1

    if not messages:
        print(f"  {DIM}Queue is empty.{RST}\n")
        return

    summarise(messages)
    print(f"\n  Total messages drained: {W}{len(messages)}{RST}\n")


def watch_mode(ch):
    print(f"\n  {C}Watching [{RESPONSE_QUEUE}] — Ctrl+C to stop{RST}\n")
    idx = 1
    try:
        while True:
            _, _, raw = ch.basic_get(queue=RESPONSE_QUEUE, auto_ack=True)
            if raw is not None:
                ts = time.strftime("%H:%M:%S")
                parsed = json.loads(raw)
                print(f"  {DIM}── {ts} — message {idx} ──────────────────────────────────{RST}")
                if isinstance(parsed, list):
                    for i, env in enumerate(parsed, 1):
                        print_envelope(i, env)
                elif isinstance(parsed, dict):
                    status = parsed.get("status", "?")
                    sc = R if status == "error" else G
                    errs = parsed.get("errors") or [parsed.get("error", "?")]
                    print(f"  [  1] {sc}Error{RST}  {R}{' | '.join(str(e) for e in errs)}{RST}")
                idx += 1
            else:
                time.sleep(0.3)
    except KeyboardInterrupt:
        print(f"\n  {DIM}Stopped.{RST}\n")


def count_mode(ch):
    q = ch.queue_declare(queue=RESPONSE_QUEUE, durable=True, passive=True)
    n = q.method.message_count
    print(f"\n  [{RESPONSE_QUEUE}]  messages ready: {W}{n}{RST}\n")


def main():
    params = pika.URLParameters(RABBITMQ_URL)
    conn   = pika.BlockingConnection(params)
    ch     = conn.channel()
    ch.queue_declare(queue=RESPONSE_QUEUE, durable=True)

    print(f"\n{'═'*66}")
    print(f"  {W}TADPOLE ENGINE — Response Queue Monitor{RST}")
    print(f"  {DIM}queue: {RESPONSE_QUEUE}{RST}")
    print(f"{'═'*66}")

    if "--watch" in sys.argv:
        watch_mode(ch)
    elif "--count" in sys.argv:
        count_mode(ch)
    else:
        drain_mode(ch)

    conn.close()


if __name__ == "__main__":
    main()
