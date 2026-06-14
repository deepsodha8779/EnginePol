# Tadpole Engine – Testing Guide

This guide walks through testing the intake flow and verifying ActionBuilder.

---

## 1. Prerequisites

- **MongoDB** running (e.g. `mongodb://127.0.0.1:27017/devdb`)
- **.env** with at least:
  - `MONGODB_URI`, `MONGODB_DB`
  - `EVENTS_COLLECTION=intake_events` (or `MONGODB_COLLECTION`)
  - `ACTIONS_COLLECTION=actions` (for ActionBuilder)
  - `PLAYBOOK_CONFIG=config/playbooks.json`
- **RabbitMQ** optional; you can test everything via HTTP only.

---

## 2. Start the gateway

```bash
cargo run -p http-gateway
```

You should see:

- `mongodb connected: db=devdb collection=intake_events`
- `action store connected; ActionBuilder enabled` (or a warning if actions store is disabled)
- `playbook config loaded: codex=... playbooks=...`
- `http-gateway binding: 127.0.0.1:8083`

If `RABBITMQ_URL` is set, you’ll also see RabbitMQ connection attempts (or errors until the URL is fixed).

---

## 3. Test the full flow (HTTP)

### 3.1 Playbook that matches and passes

Use an event that matches a playbook and where the rule expression evaluates to **true** (PASS).

Example: playbook with `match_expr`: `event_name == "InvoiceReceivedForExternalDependency"` and rule `expr`: `event_name == "InvoiceReceivedForExternalDependency"`.

- **Send event** (same shape as `schema/event-envelope.example.json`):

```bash
# Bash
curl -s -X POST http://127.0.0.1:8083/intake \
  -H "Content-Type: application/json" \
  -d @schema/event-envelope.example.json
```

- **Expect:** HTTP 200 and a JSON array with a governance conclusion (e.g. `"status": "PASS"`).
- **MongoDB:** `intake_events`: one document with `status: "success"`.
- **MongoDB:** `actions`: no new document (decision was PASS).

### 3.2 Playbook that matches and fails (ActionBuilder)

To trigger **FAIL** and ActionBuilder, the event must match a playbook but at least one rule must evaluate to **false** (boolean handler returns FAIL).

**Option A – Add a “fail” rule for testing**

In `config/playbooks.json`, use a codex/playbook that matches your event and include a rule whose `expr` is **false** for that event, e.g.:

- `match_expr`: `event_name == "InvoiceReceivedForExternalDependency"`
- One rule with `expr`: `event_name == "InvoiceReceivedForExternalDependency"` (PASS)
- Another rule with `expr`: `tenant_id == "nonexistent"` (FAIL)

So the playbook matches, one rule passes and one fails → orchestrator decision FAIL → ActionBuilder runs.

**Option B – Use a playbook that only has a failing expr**

Example playbook (conceptually):

- Match: `event_name == "InvoiceReceivedForExternalDependency"`
- Single rule: `expr`: `event_name == "OtherEvent"` → always false for your event → FAIL.

**Example playbook file for testing FAIL + ActionBuilder**

Save as `config/playbooks-test-fail.json` and run with `PLAYBOOK_CONFIG=config/playbooks-test-fail.json`:

```json
{
  "codex": [
    {
      "version_id": "v1",
      "playbooks": [
        {
          "id": "playbook.invoice_test_fail",
          "match_expr": "event_name == \"InvoiceReceivedForExternalDependency\"",
          "rules": [
            {
              "id": "rule.always_fail",
              "kind": "boolean",
              "expr": "tenant_id == \"will-not-match\""
            }
          ]
        }
      ]
    }
  ]
}
```

This playbook matches the example event (by `event_name`) but the rule expr is false → FAIL → ActionBuilder creates one action.

**Send the same event as above.** Then:

- **Expect:** HTTP 200 and a conclusion with `"status": "FAIL"`.
- **Logs:** You should see `action created: action_id=... playbook_id=... rule_id=...`.
- **MongoDB `actions`:** One new document with:
  - `action_id`, `idempotency_key`, `idempotency_hash`
  - `tenant_id`, `event_id`, `playbook_id`, `rule_id`
  - `task_type`: `TASK_GOVERNANCE_REVIEW`
  - `status`: `created`

### 3.3 Idempotency (duplicate action not created)

Send the **same** event again (same `event_id`, same tenant, same object type/id, same playbook).

- **Expect:** Again HTTP 200 and FAIL conclusion.
- **Logs:** `action already exists for idempotency_key (hash prefix); skipping`.
- **MongoDB `actions`:** Still only one document for that (tenant, object_type, object_id, playbook); no second action.

---

## 4. How ActionBuilder fits in (verification)

Flow:

1. **Assigner** – Matches event to playbooks, builds tadpole(s) with rules in the tail.
2. **Dispatcher** – Runs each rule (e.g. boolean handler); appends evaluations (PASS/FAIL/INCONCLUSIVE).
3. **Orchestrator** – Aggregates evaluations: if any rule is FAIL → decision FAIL.
4. **Pipeline** – If `result.decision.is_fail()`, calls `ActionBuilder::create_actions_for_failure(envelope, result)`.
5. **ActionBuilder** – For each playbook that has at least one failing evaluation:
   - Builds idempotency key: `{tenant}:{object_type}:{object_id}:TASK_GOVERNANCE_REVIEW:{playbook_id}`.
   - Hashes it, looks up `actions` by `idempotency_hash`.
   - If not found, inserts one action record (ULID, status `created`, etc.).

Checks:

- Orchestrator sets `decision` to FAIL when any evaluation is FAIL (see `engine_core::actors::orchestrator`).
- Pipeline only calls ActionBuilder when `result.decision.is_fail()` and `action_builder` is `Some`.
- ActionBuilder only creates actions when `result.decision.is_fail()`, one per playbook with a failing rule, and skips when `find_by_idempotency_hash` already returns a record.

---

## 5. Diagnostics and MongoDB

- **Recent intake:**  
  `GET http://127.0.0.1:8083/diagnostics`  
  Returns recent records from the intake store (e.g. last 50).

- **Intake events:**  
  Query MongoDB collection `intake_events` (or your `EVENTS_COLLECTION`) for `event_id`, `status`, `envelope`.

- **Actions:**  
  Query MongoDB collection `actions` (or your `ACTIONS_COLLECTION`) for `action_id`, `idempotency_key`, `tenant_id`, `event_id`, `playbook_id`, `status: "created"`.

---

## 6. RabbitMQ (optional)

If RabbitMQ is configured and connecting successfully:

- Publish a message to the configured queue (`RABBITMQ_QUEUE`, default `events_intake`).
- Body: **one JSON document** = same canonical envelope as for HTTP (e.g. `schema/event-envelope.example.json`).
- The consumer will run the same pipeline (assign → dispatch → orchestrate → ActionBuilder on FAIL) and record intake; actions appear in `actions` as with HTTP.

If you see **InvalidContentType** or connection errors, try `amqp://` instead of `amqps://` (or the opposite) as in the main setup guide, or disable RabbitMQ and test only via HTTP.

---

## 7. Quick checklist

| Step | What to do | What to check |
|------|-------------|----------------|
| 1 | Start gateway | Logs: MongoDB + playbooks loaded, ActionBuilder enabled if configured |
| 2 | POST event that matches and passes | 200, conclusion PASS; intake_events has record; no action |
| 3 | POST event that matches but has a failing rule | 200, conclusion FAIL; log “action created”; one doc in `actions` |
| 4 | POST same event again | 200, FAIL; log “action already exists”; still one doc in `actions` |
| 5 | GET /diagnostics | 200, list of recent intake records |

---

## 8. Bulk Testing, Performance, and Coverage

For RabbitMQ-specific bulk publishing, mixed valid/invalid event cases, exact payload strings,
and the report template, see `docs/RABBITMQ_BULK_TEST_REPORT.md`.

Run the in-memory bulk intake test with observations visible:

```bash
cargo test -p tadpole-tests bulk_intake_captures_resource_utilization_and_performance_observations -- --nocapture
```

The test prints a `bulk test observations` line with:

- event count
- total elapsed time
- average per-event latency
- throughput per second
- user CPU time consumed by the bulk loop
- system CPU time consumed by the bulk loop
- process peak RSS and peak RSS delta
- persisted intake record count
- persisted event log count
- emitted metric record count
- total serialized response bytes

Verify the coverage gate with:

```bash
cargo coverage
```

Generate the matching HTML report with the same coverage scope:

```bash
cargo coverage-html
```

Open `target/llvm-cov/html/index.html`. Do not use raw `cargo llvm-cov --html` for the project
gate, because that includes runtime integration files intentionally excluded by the repo coverage
aliases.
