# Decision Model

This document defines the PASS / FAIL / INCONCLUSIVE contract used across rule evaluation
and orchestration.

## Decision enum

`Decision` is serialized as an object with a `status` tag and two fields:

```json
{ "status": "PASS", "reason_code": "rule.expr_pass", "message": "expr evaluated: ..." }
```

Status meanings:

- `PASS`: rule condition evaluated to true.
- `FAIL`: rule condition evaluated to false.
- `INCONCLUSIVE`: rule could not be evaluated, was skipped, or is not implemented.

## Reason code + message

Every rule evaluation returns both:

- `reason_code`: stable, machine-readable identifier.
- `reason`: human-readable message.

The decision itself also carries:

- `reason_code`: stable, machine-readable identifier.
- `message`: human-readable message.

The dispatcher and handlers currently emit the following codes:

- `rule.expr_missing`: no expression configured.
- `rule.expr_pass`: expression evaluated to true.
- `rule.expr_fail`: expression evaluated to false.
- `rule.expr_invalid`: expression failed to parse or evaluate.
- `rule.enrichment_stub`: handler not implemented.
- `dispatcher.skipped`: rule was skipped by config.
- `dispatcher.no_handler`: no handler registered for the rule kind.
- `dispatcher.handler_error`: handler returned an error.

The orchestrator currently emits:

- `orchestrator.fail`: at least one rule failed.
- `orchestrator.inconclusive`: at least one rule inconclusive and none failed.
- `orchestrator.pass`: all rules passed.
- `orchestrator.playbook_empty`: playbook had no evaluated rules.
- `orchestrator.playbook_fail`: playbook had a failing rule.
- `orchestrator.playbook_inconclusive`: playbook had an inconclusive rule.
- `orchestrator.playbook_pass`: playbook rules all passed.
