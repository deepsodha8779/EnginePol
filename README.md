# TadpoleEngine
Event-driven governance engine powering Evotu.re’s rule execution and playbook orchestration.

This repository contains the Rust implementation of the Tadpole Engine Core, responsible for:

Ingesting domain change events in a canonical envelope

Assigning events to the correct playbooks

Dispatching rules sequentially

Aggregating PASS / FAIL / INCONCLUSIVE decisions

Exposing diagnostics & metrics for observability

# Architectural Overview

The Tadpole Engine processes events using a Tadpole Pattern:

1. Tadpole Head

Metadata such as:

Event ID

Tenant ID

Correlation ID

Event name

Event category

Origin metadata

External dependency id

Changed object type/id

Change kind

2. Tadpole Body

Immutable business payload, e.g.:

Contract snapshot

Delta data (changes)

3. Tadpole Tail

Execution chain consisting of:

Assigned Playbooks

Ordered Rules

Intermediate rule evaluation results

Final aggregated decision

Optional retry metadata

# Core Components (MVP)
1. Event Intake (Actix HTTP endpoint)

Accept canonical envelope (JSON)

Validate event id, tenant id, event name, scope fields, payload

Return governance conclusion events

See the end-to-end intake and conclusion flow in `docs/flow.md`.

2. Assigner

Matches incoming events to playbooks

Supports static lookup & CEL expressions

Must expose reasoning for debugging

3. Dispatcher

Sequential rule execution over Tadpole tail

Invokes RuleHandlers and others with shared context

Preserves ordering

4. RuleHandlers

Boolean evaluator (first handler type)

Data-enrichment stub

More handlers added later

5. Orchestrator

Aggregates PASS / FAIL / INCONCLUSIVE

Emits action candidates (not persisted here)

6. Diagnostics & Metrics

Recent evaluations

Per-rule execution times

Error counters

Simple JSON endpoint (no infra dependency)

# MongoDB Storage

The intake API records each request/response to MongoDB. Configure these environment variables before starting `http-gateway`:

```
MONGODB_URI=mongodb://user:pass@host:27017/devdb
MONGODB_DB=devdb
MONGODB_COLLECTION=intake_events
```

# Contributor Notes

Dispatcher + handler contract: see `docs/dispatching.md`.
Decision model: see `docs/decisions.md`.
Content hash strategy: see `docs/content-hash.md`.
Repository file and runtime flow guide: see `docs/FILE_AND_FLOW_GUIDE.md`.
