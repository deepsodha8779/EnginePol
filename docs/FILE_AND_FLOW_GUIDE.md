# Tadpole Engine File And Flow Guide

This guide is meant for quick team handoff. Read the flow section first to understand how an event moves through the system, then use the file map when someone asks what a specific file does.

## Big Picture

Tadpole Engine is a Rust workspace with four crates:

- `domain`: shared event envelope model, envelope validation helpers, and hashing utilities.
- `engine-core`: actor-based rule engine. It assigns playbooks, dispatches rules to handlers, and aggregates rule decisions.
- `http-gateway`: runtime application. It exposes HTTP endpoints, consumes RabbitMQ messages, stores intake/action/metric records, and runs the pipeline.
- `tests`: integration-style test crate covering the engine and gateway behavior.

The main runtime path is:

1. An event arrives as a `CanonicalEnvelope` through HTTP `/intake` or RabbitMQ.
2. `http-gateway/src/pipeline.rs` validates the envelope.
3. The pipeline asks the assigner to match playbooks and build a `Tadpole` execution object.
4. The dispatcher executes each ordered rule through the correct handler.
5. The orchestrator aggregates rule evaluations into a final `PASS`, `FAIL`, or `INCONCLUSIVE` decision.
6. The gateway records logs/diagnostics/metrics.
7. If configured, the action builder creates governance action records, routes them to assignees, and publishes action feed messages.
8. The caller receives governance conclusion JSON.

## Runtime Flow In Detail

### 1. Startup

`http-gateway/src/main.rs` starts the service. It loads `.env`, configures logging and optional Seq delivery, connects to MongoDB stores, optionally enables RabbitMQ publishers, builds the Actix actors, starts the RabbitMQ consumer, and binds the HTTP server on `127.0.0.1:8083`.

Important startup dependencies:

- `MongoStore` implements intake/event-log/playbook/rule lookup.
- `MongoMetricStore` and `MetricManager` enable metrics if Mongo config is valid.
- `MongoActionTemplateStore` enables template-driven actions if Mongo config is valid.
- `MongoActionStore` enables persisted action records if Mongo config is valid.
- `ConfigWorkRouter` resolves responsible roles/users into assignees.
- `BooleanRuleHandler`, `DispatcherActor`, `AssignerActor`, `OrchestratorActor`, and `DiagnosticsActor` form the core actor pipeline.

### 2. Intake

HTTP intake enters `http-gateway/src/intake_api.rs` through `intake()`. RabbitMQ intake enters `http-gateway/src/rabbitmq_consumer.rs`. Both paths call `process_envelope()` in `http-gateway/src/pipeline.rs`.

The API also exposes:

- `/diagnostics`: recent intake records.
- `/event-logs/{event_id}`: detailed stage logs for one event.
- `/metrics` and `/metrics/{event_id}`: metric records.
- `/action-templates` and `/action-templates/{template_id}`: action template lookup.

### 3. Validation

`pipeline.rs` validates:

- `event_id` format using `domain/src/envelope/validation.rs`.
- required tenant and event fields.
- allowed event category and change kind.
- scope fields like changed object type/id.

Failures return a structured `PipelineError::ValidationFailed` response and are logged to the intake store.

### 4. Playbook Assignment

The pipeline creates or enriches a `Tadpole` object and calls `AssignerActor`.

The assigner has two modes:

- Static config mode in `engine-core/src/actors/assigner/assigner.rs`, using playbooks from config.
- Database-backed enrichment in `pipeline.rs`, which asks `IntakeStore` for matched playbooks and active rules.

Playbooks match on `changed_object_type` and `change_kind`, or a simple header expression. Matched playbooks add `PlaybookAssignment` entries and ordered `RuleSpec` entries to the Tadpole tail.

### 5. Rule Dispatch

`engine-core/src/actors/dispatcher/dispatcher.rs` receives a `Dispatch` message containing a `Tadpole`.

It executes rules in order. Rule kinds are mapped to `RuleEvaluator` implementations:

- `BooleanRuleEvaluator` calls `BooleanRuleHandler`.
- `EnrichmentStubEvaluator` returns `INCONCLUSIVE` because enrichment is not implemented yet.

If the playbook execution mode is `Fail_First`, a critical failed rule stops further rule execution.

### 6. Boolean Rule Evaluation

`engine-core/src/actors/handlers/boolean_handler/boolean_handler.rs` evaluates boolean rules.

It supports two styles:

- Snapshot conditions from `body.snapshots`, using operators like `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, and `contains`.
- Simple expressions against envelope head fields through `engine-core/src/simple_expr.rs`.

The output is a `RuleEvaluation` with:

- rule/playbook identifiers,
- `Decision`,
- reason code and message,
- per-condition `ConditionCheck` records,
- duration and optional action template id.

### 7. Orchestration

`engine-core/src/actors/orchestrator/orchestrator.rs` aggregates all rule evaluations:

- Any failed rule makes the overall decision `FAIL`.
- If none fail but at least one is inconclusive, the decision is `INCONCLUSIVE`.
- If all evaluated rules pass, the decision is `PASS`.

It also builds:

- `CodexResult`: playbook/rule decision tree for API output.
- `PlaybookSummary`: per-playbook summary.
- `action_candidates`: legacy marker for failed decisions.
- `route_to_action_builder`: currently always true so templates can match any decision type.

### 8. Actions

`http-gateway/src/action_builder.rs` creates persisted `ActionRecord` values.

It supports three action paths:

- Template-driven actions from active `ActionTemplate` records. These can trigger on rule passed, failed, or inconclusive.
- Rule-bound actions where a `RuleEvaluation` references an action template id.
- Legacy fallback actions for failed or inconclusive decisions when no template path applies.

Idempotency is important. The builder constructs an idempotency key from tenant/object/task/playbook/rule/template details, hashes it with `compute_idempotency_hash()`, checks the store, and inserts only if no existing record is found.

If a `WorkRouter` is configured, actions are assigned to a user, team, or queue, and assignment fields are persisted back to MongoDB.

### 9. Metrics And Logs

`http-gateway/src/metric_manager.rs` turns orchestration results into metric records such as rule pass/fail/inconclusive, action triggered, and KPI threshold breach. It stores them through `MetricStore` and can publish events through RabbitMQ.

`pipeline.rs` writes event logs at major stages, including validation, assignment, dispatch, orchestration, action creation, and errors. Those logs are queryable through the intake API.

## File Map

### Root Files

- `Cargo.toml`: workspace definition. Lists `engine-core`, `http-gateway`, `domain`, and `tests` as workspace members.
- `Cargo.lock`: locked dependency versions for reproducible builds.
- `readme.md`: high-level project overview and component summary.
- `LICENSE`: project license.
- `.env.example`: example environment variables for local runtime configuration.
- `.env`: local environment file. Usually machine-specific and not documentation.
- `.gitignore`: files ignored by Git.
- `.cargo/config.toml`: Cargo configuration for this workspace.
- `Evoture_Flow.drawio`: architecture/flow diagram source.
- `test_event.json`: sample intake event payload.

### `domain`

- `domain/Cargo.toml`: crate manifest for the shared domain crate.
- `domain/src/lib.rs`: exposes the `envelope` module.
- `domain/src/envelope/mod.rs`: module barrel file. Re-exports envelope models, validation, and hashing helpers.
- `domain/src/envelope/model.rs`: defines `TadpoleHead`, `CanonicalEnvelope`, and a small `IntakeEnvelope` structure. This is the shared event shape used across the engine.
- `domain/src/envelope/validation.rs`: validates ULID format for event ids.
- `domain/src/envelope/hashing.rs`: computes idempotency hashes and content hashes using SHA-256. Used to deduplicate actions and detect event content identity.

### `engine-core`

- `engine-core/Cargo.toml`: crate manifest for the actor/rule engine.
- `engine-core/src/lib.rs`: exposes `actors`, `dto`, and `ulid`; keeps `simple_expr` internal.
- `engine-core/src/ulid.rs`: ULID generation service used by runtime code that needs sortable unique ids.
- `engine-core/src/simple_expr.rs`: small expression evaluator for envelope-head matching and simple boolean rules.

#### Engine Actors

- `engine-core/src/actors/mod.rs`: exposes actor modules.
- `engine-core/src/actors/assigner/mod.rs`: re-exports assigner types.
- `engine-core/src/actors/assigner/assigner.rs`: playbook assignment actor and config parser. Matches playbooks, orders rules, and builds `Tadpole` tails.
- `engine-core/src/actors/dispatcher/mod.rs`: re-exports dispatcher types.
- `engine-core/src/actors/dispatcher/dispatcher.rs`: sequential rule dispatcher. Maps rule kinds to evaluators, executes rules, handles missing handlers, and honors fail-first critical failures.
- `engine-core/src/actors/handlers/mod.rs`: exposes handler modules.
- `engine-core/src/actors/handlers/boolean_handler/mod.rs`: re-exports boolean handler types.
- `engine-core/src/actors/handlers/boolean_handler/boolean_handler.rs`: boolean rule handler. Reads snapshots, resolves condition values, compares actual vs expected values, and returns rule decisions.
- `engine-core/src/actors/orchestrator/mod.rs`: re-exports orchestrator types.
- `engine-core/src/actors/orchestrator/orchestrator.rs`: aggregates rule evaluations into final orchestration output.
- `engine-core/src/actors/diagnostics/mod.rs`: re-exports diagnostics actor.
- `engine-core/src/actors/diagnostics/diagnostics.rs`: in-memory diagnostics actor for recording recent orchestration results and errors.

#### Engine DTOs

- `engine-core/src/dto/mod.rs`: exposes all DTO modules.
- `engine-core/src/dto/action.rs`: defines lightweight `ActionCandidate` output from orchestration.
- `engine-core/src/dto/assigner.rs`: Actix message for assigning an envelope to one or more Tadpoles.
- `engine-core/src/dto/boolean_handler.rs`: Actix message for boolean rule evaluation.
- `engine-core/src/dto/decision.rs`: `Decision` enum with `PASS`, `FAIL`, and `INCONCLUSIVE` variants plus helper methods.
- `engine-core/src/dto/diagnostics.rs`: messages for recording diagnostics and errors.
- `engine-core/src/dto/dispatcher.rs`: Actix message for dispatching a Tadpole through ordered rules.
- `engine-core/src/dto/evaluation.rs`: `ConditionCheck` and `RuleEvaluation` structures produced by rule handlers.
- `engine-core/src/dto/orchestration.rs`: final orchestration result DTOs, including Codex-style summaries.
- `engine-core/src/dto/orchestrator.rs`: Actix message for orchestration.
- `engine-core/src/dto/rules.rs`: playbook assignment, matched playbook, rule condition, rule kind, rule logic, and rule spec DTOs.
- `engine-core/src/dto/tadpole.rs`: runtime execution object. Combines envelope head/body with a tail containing assigned playbooks, ordered rules, evaluations, and metadata.

### `http-gateway`

- `http-gateway/Cargo.toml`: crate manifest for the HTTP/RabbitMQ/MongoDB runtime.
- `http-gateway/src/lib.rs`: public module exports used by the binary and tests.
- `http-gateway/src/main.rs`: application entry point. Wires stores, actors, routers, publishers, HTTP routes, and RabbitMQ consumer.
- `http-gateway/src/pipeline.rs`: shared pipeline used by HTTP and RabbitMQ. Validates, assigns, dispatches, orchestrates, records diagnostics/metrics/logs, and triggers actions.
- `http-gateway/src/intake_api.rs`: Actix HTTP handlers for intake, diagnostics, event logs, metrics, and action template queries.
- `http-gateway/src/rabbitmq_consumer.rs`: RabbitMQ consumer path. Parses incoming messages, calls the same pipeline, and publishes success/error responses to a response queue.
- `http-gateway/src/mongo_store.rs`: Mongo-backed intake store and playbook/rule lookup store. Defines database DTOs such as `DbPlaybook` and `DbRule`.
- `http-gateway/src/action_store.rs`: Mongo-backed action persistence. Defines `ActionRecord`, idempotency lookup, insert, and assignment update behavior.
- `http-gateway/src/action_template.rs`: action template model and Mongo store. Templates define triggers, execution mode, responsibility, evidence requirements, and rule/playbook associations.
- `http-gateway/src/action_builder.rs`: creates action records from templates, rule bindings, or legacy fallback behavior. Handles idempotency, action descriptions, work routing, and event logging.
- `http-gateway/src/work_router.rs`: resolves responsibility fields into actual assignees using config. Supports user/team/queue-style routing and fallback assignment.
- `http-gateway/src/action_feed_publisher.rs`: publishes created actions to RabbitMQ for downstream consumers.
- `http-gateway/src/metric_manager.rs`: builds and stores metric records from orchestration results and action activity. Also evaluates configured thresholds.
- `http-gateway/src/metric_store.rs`: Mongo-backed metric storage and query implementation.
- `http-gateway/src/metric_publisher.rs`: RabbitMQ publisher for metric events.
- `http-gateway/src/seq_layer.rs`: custom tracing layer that sends logs to Seq over HTTP when `SEQ_URL` is configured.

### `config`

- `config/playbooks-test-fail.json`: small playbook config used for failure-path testing or local experiments.
- `config/action_template.example.json`: example action template document showing trigger, responsibility, evidence, and association fields.
- `config/work_router_roles.example.json`: example role-to-assignee routing config.
- `config/work_router_roles.json`: active local work router config used by `ConfigWorkRouter` if pointed to by environment/config.

### `schema`

- `schema/event-envelope.schema.json`: JSON schema for the canonical event envelope accepted by intake.
- `schema/event-envelope.example.json`: sample event envelope matching the schema.

### `docs`

- `docs/flow.md`: compact Mermaid diagram for the current runtime path.
- `docs/dispatching.md`: notes on dispatcher and handler contracts.
- `docs/decisions.md`: decision semantics for pass/fail/inconclusive behavior.
- `docs/content-hash.md`: explains content hash strategy.
- `docs/SETUP_GUIDE.md`: local setup steps and environment notes.
- `docs/TESTING.md`: testing guide and useful commands.
- `docs/FILE_AND_FLOW_GUIDE.md`: this handoff guide.

### `demo`

- `demo/push.py`: helper script for pushing a sample event into the running gateway.
- `demo/publish.py`: helper script for publishing sample events, likely through RabbitMQ-oriented local demos.
- `demo/monitor.py`: helper script for watching demo output/events.

### `tests`

- `tests/Cargo.toml`: integration test crate manifest.
- `tests/src/lib.rs`: test module registry.
- `tests/src/decision.rs`: tests decision serialization and helper behavior.
- `tests/src/rabbitmq_tests.rs`: tests RabbitMQ message body parsing into envelopes.
- `tests/src/action_builder_tests.rs`: tests action builder behavior: idempotency, per-rule actions, template/rule bindings, routing, event logs, fallback actions, and duplicate suppression.
- `tests/src/action_template_tests.rs`: tests action template matching and template-driven action creation.
- `tests/src/work_router_tests.rs`: tests config-based work routing, fallback, and notification channel behavior.
- `tests/src/assigner/mod.rs`: assigner test module.
- `tests/src/assigner/assigner.rs`: tests playbook assignment, trigger matching, rule ordering, and config parsing.
- `tests/src/boolean_handler/mod.rs`: boolean handler test module.
- `tests/src/boolean_handler/boolean_handler.rs`: tests boolean rule evaluation, operators, snapshots, missing data, and rule logic.
- `tests/src/diagnostics/mod.rs`: diagnostics test module.
- `tests/src/diagnostics/diagnostics.rs`: tests diagnostics actor record/error behavior.
- `tests/src/dispatcher/mod.rs`: dispatcher test module.
- `tests/src/dispatcher/dispatcher.rs`: tests dispatcher ordering, handler selection, fail-first behavior, and missing handler behavior.
- `tests/src/envelope/mod.rs`: envelope test module.
- `tests/src/envelope/envelope.rs`: tests envelope model, validation, and hashing behavior.
- `tests/src/http_gateway_tests/mod.rs`: HTTP gateway test module.
- `tests/src/http_gateway_tests/http_gateway_tests.rs`: tests intake API/pipeline behavior, store interactions, diagnostics, metrics, and error handling.
- `tests/src/orchestrator/mod.rs`: orchestrator test module.
- `tests/src/orchestrator/orchestrator.rs`: tests orchestration aggregation, Codex output, playbook summaries, and action candidate behavior.
- `tests/src/ulid/mod.rs`: ULID test module.
- `tests/src/ulid/ulid.rs`: tests ULID generation/format behavior.

## Common Questions And Where To Look

- "Where does an HTTP request enter?" Start with `http-gateway/src/intake_api.rs`, then follow `process_envelope()` in `http-gateway/src/pipeline.rs`.
- "Where does RabbitMQ enter?" Start with `http-gateway/src/rabbitmq_consumer.rs`; it uses the same pipeline as HTTP.
- "Where are playbooks matched?" Check `engine-core/src/actors/assigner/assigner.rs` and the Mongo lookup code in `http-gateway/src/pipeline.rs` / `http-gateway/src/mongo_store.rs`.
- "Where are rules evaluated?" Check `engine-core/src/actors/dispatcher/dispatcher.rs` and `engine-core/src/actors/handlers/boolean_handler/boolean_handler.rs`.
- "Where is the final decision calculated?" Check `engine-core/src/actors/orchestrator/orchestrator.rs`.
- "Where are actions created?" Check `http-gateway/src/action_builder.rs`, then `action_store.rs`, `action_template.rs`, and `work_router.rs`.
- "Where are metrics created?" Check `http-gateway/src/metric_manager.rs` and `http-gateway/src/metric_store.rs`.
- "Where are event logs stored and queried?" Check `http-gateway/src/mongo_store.rs` and `http-gateway/src/intake_api.rs`.
- "Where do tests for my active file live?" For `ActionBuilder`, use `tests/src/action_builder_tests.rs`.

