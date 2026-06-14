//! Shared intake pipeline: validate → assign → dispatch → orchestrate.
//! Used by both HTTP intake and RabbitMQ consumer.

use actix::Addr;
use domain::envelope::{CanonicalEnvelope, compute_content_hash, is_valid_ulid};
use engine_core::actors::{
    assigner::AssignerActor, diagnostics::DiagnosticsActor, dispatcher::DispatcherActor,
    orchestrator::OrchestratorActor,
};
use engine_core::dto::{
    decision::Decision,
    diagnostics::{RecordError, RecordEvaluation},
    dispatcher::Dispatch,
    evaluation::{ConditionCheck, RuleEvaluation},
    orchestrator::Orchestrate,
    rules::{MatchedPlaybook, RuleCondition, RuleKind, RuleSpec},
    tadpole::Tadpole,
};
use engine_core::ulid::UlidService;
use log::{debug, error, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::action_builder::ActionBuilder;
use crate::action_feed_publisher::ActionFeedPublisher;
use crate::metric_manager::MetricManager;
use crate::mongo_store::IntakeStore;

#[derive(Debug)]
pub enum PipelineError {
    ValidationFailed(Vec<String>),
    AssignerFailed(String),
    DispatchFailed(String),
    OrchestratorFailed(String),
}

impl PipelineError {
    pub fn stage(&self) -> &'static str {
        match self {
            Self::ValidationFailed(_) => "pipeline.validation",
            Self::AssignerFailed(_) => "pipeline.assign",
            Self::DispatchFailed(_) => "pipeline.dispatch",
            Self::OrchestratorFailed(_) => "pipeline.orchestrate",
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            Self::ValidationFailed(_) => "validation_failed",
            Self::AssignerFailed(_) => "assigner_failed",
            Self::DispatchFailed(_) => "dispatch_failed",
            Self::OrchestratorFailed(_) => "orchestrator_failed",
        }
    }

    pub fn description(&self) -> String {
        match self {
            Self::ValidationFailed(errors) => {
                format!("Request validation failed with {} error(s)", errors.len())
            }
            Self::AssignerFailed(message) => {
                format!("Playbook/rule assignment failed: {message}")
            }
            Self::DispatchFailed(message) => {
                format!("Rule dispatch failed: {message}")
            }
            Self::OrchestratorFailed(message) => {
                format!("Governance orchestration failed: {message}")
            }
        }
    }

    pub fn primary_message(&self) -> String {
        match self {
            Self::ValidationFailed(_) => self.description(),
            Self::AssignerFailed(message)
            | Self::DispatchFailed(message)
            | Self::OrchestratorFailed(message) => message.clone(),
        }
    }

    pub fn response_body(&self) -> serde_json::Value {
        match self {
            Self::ValidationFailed(errors) => serde_json::json!({
                "status": "error",
                "error_code": self.code(),
                "stage": self.stage(),
                "description": self.description(),
                "errors": errors,
            }),
            Self::AssignerFailed(message)
            | Self::DispatchFailed(message)
            | Self::OrchestratorFailed(message) => serde_json::json!({
                "status": "error",
                "error_code": self.code(),
                "stage": self.stage(),
                "error": message,
                "description": self.description(),
            }),
        }
    }
}

fn normalize_change_kind(value: &str) -> Option<&'static str> {
    match value.trim().to_ascii_lowercase().as_str() {
        "created" | "create" => Some("created"),
        "updated" | "update" => Some("updated"),
        "deleted" | "delete" => Some("deleted"),
        _ => None,
    }
}

async fn append_event_log(
    store: Option<&Arc<dyn IntakeStore>>,
    envelope: &CanonicalEnvelope,
    stage: &str,
    level: &str,
    message: &str,
    details: Option<serde_json::Value>,
) {
    if let Some(store) = store {
        let _ = store
            .append_event_log(envelope, stage, level, message, details.as_ref())
            .await;
    }
}

fn decision_status(decision: &Decision) -> &'static str {
    match decision {
        Decision::Pass { .. } => "PASS",
        Decision::Fail { .. } => "FAIL",
        Decision::Inconclusive { .. } => "INCONCLUSIVE",
    }
}

fn decision_reason_code(decision: &Decision) -> &str {
    match decision {
        Decision::Pass { reason_code, .. } => reason_code,
        Decision::Fail { reason_code, .. } => reason_code,
        Decision::Inconclusive { reason_code, .. } => reason_code,
    }
}

fn decision_message(decision: &Decision) -> &str {
    match decision {
        Decision::Pass { message, .. } => message,
        Decision::Fail { message, .. } => message,
        Decision::Inconclusive { message, .. } => message,
    }
}

fn should_create_review_actions(decision: &Decision) -> bool {
    decision.is_fail() || decision.is_inconclusive()
}

fn format_json_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(inner) => inner.clone(),
        _ => value.to_string(),
    }
}

fn format_optional_json_value(value: Option<&serde_json::Value>) -> String {
    value
        .map(format_json_value)
        .unwrap_or_else(|| "null".to_string())
}

fn build_check_pass_hint(check: &ConditionCheck) -> String {
    match check.status.as_str() {
        "FAIL" => format!(
            "Make '{}' satisfy '{}': expected {}, actual {}.",
            check.object_key,
            check.operator,
            format_optional_json_value(check.expected.as_ref()),
            format_optional_json_value(check.actual.as_ref()),
        ),
        "INCONCLUSIVE" if check.actual.is_none() => format!(
            "Provide '{}' in the evaluated snapshot so '{}' can be checked against {}.",
            check.object_key,
            check.operator,
            format_optional_json_value(check.expected.as_ref()),
        ),
        "INCONCLUSIVE" if check.expected.is_none() => format!(
            "Configure an expected value for '{}' with operator '{}'.",
            check.object_key, check.operator
        ),
        "INCONCLUSIVE" => format!(
            "Fix the data or operator for '{}' so '{}' can compare actual {} with expected {}.",
            check.object_key,
            check.operator,
            format_optional_json_value(check.actual.as_ref()),
            format_optional_json_value(check.expected.as_ref()),
        ),
        _ => "No action required for this check.".to_string(),
    }
}

fn build_rule_diagnostics(evaluation: &RuleEvaluation) -> serde_json::Value {
    let failing_checks: Vec<serde_json::Value> = evaluation
        .checks
        .iter()
        .filter(|check| check.status != "PASS")
        .map(|check| {
            serde_json::json!({
                "object_key": check.object_key,
                "operator": check.operator,
                "status": check.status,
                "reason": check.reason,
                "expected": check.expected,
                "actual": check.actual,
                "how_to_pass": build_check_pass_hint(check),
            })
        })
        .collect();

    let pass_guidance: Vec<String> = evaluation
        .checks
        .iter()
        .filter(|check| check.status != "PASS")
        .map(build_check_pass_hint)
        .collect();

    let pass_count = evaluation
        .checks
        .iter()
        .filter(|check| check.status == "PASS")
        .count();
    let fail_count = evaluation
        .checks
        .iter()
        .filter(|check| check.status == "FAIL")
        .count();
    let inconclusive_count = evaluation
        .checks
        .iter()
        .filter(|check| check.status == "INCONCLUSIVE")
        .count();

    serde_json::json!({
        "rule_status": decision_status(&evaluation.decision),
        "failed_because": evaluation.reason,
        "reason_code": evaluation.reason_code,
        "check_summary": {
            "total": evaluation.checks.len(),
            "pass": pass_count,
            "fail": fail_count,
            "inconclusive": inconclusive_count,
        },
        "failing_checks": failing_checks,
        "how_to_pass": if pass_guidance.is_empty() {
            vec!["No failing checks were recorded; inspect reason_code and upstream dispatcher/orchestrator logs.".to_string()]
        } else {
            pass_guidance
        },
    })
}

async fn append_rule_evaluation_logs(
    store: Option<&Arc<dyn IntakeStore>>,
    envelope: &CanonicalEnvelope,
    evaluations: &[RuleEvaluation],
) {
    for evaluation in evaluations {
        let diagnostics = build_rule_diagnostics(evaluation);
        let level = match evaluation.decision {
            Decision::Pass { .. } => "INFO",
            Decision::Fail { .. } => "WARN",
            Decision::Inconclusive { .. } => "WARN",
        };
        let message = match evaluation.decision {
            Decision::Pass { .. } => "rule evaluation passed",
            Decision::Fail { .. } => "rule evaluation failed",
            Decision::Inconclusive { .. } => "rule evaluation inconclusive",
        };
        append_event_log(
            store,
            envelope,
            "pipeline.rules",
            level,
            message,
            Some(serde_json::json!({
                "playbook_id": evaluation.playbook_id,
                "rule_id": evaluation.rule_id,
                "rule_name": evaluation.rule_name,
                "result": decision_status(&evaluation.decision),
                "reason_code": evaluation.reason_code,
                "reason": evaluation.reason,
                "duration_ms": evaluation.duration_ms,
                "checks": evaluation.checks,
                "diagnostics": diagnostics,
            })),
        )
        .await;

        match evaluation.decision {
            Decision::Fail { .. } | Decision::Inconclusive { .. } => {
                warn!(
                    "rule evaluation diagnostic: event_id={} playbook_id={} rule_id={} rule_name={} result={} reason_code={} reason={} diagnostics={}",
                    envelope.head.event_id,
                    evaluation.playbook_id,
                    evaluation.rule_id,
                    evaluation.rule_name.as_deref().unwrap_or(""),
                    decision_status(&evaluation.decision),
                    evaluation.reason_code,
                    evaluation.reason,
                    diagnostics
                );
            }
            Decision::Pass { .. } => {}
        }
    }
}

async fn build_tadpole_from_store(
    envelope: &CanonicalEnvelope,
    store: &Arc<dyn IntakeStore>,
) -> Result<Option<Tadpole>, PipelineError> {
    let changed_object_type = envelope
        .head
        .changed_object_type
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let Some(changed_object_type) = changed_object_type else {
        append_event_log(
            Some(store),
            envelope,
            "pipeline.assign",
            "WARN",
            "playbook lookup skipped because changed_object_type is missing or empty",
            None,
        )
        .await;
        warn!(
            "skipping playbook lookup: changed_object_type is missing or empty (event_id={})",
            envelope.head.event_id
        );
        return Ok(None);
    };
    let change_kind_raw = envelope
        .head
        .change_kind
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let Some(change_kind_raw) = change_kind_raw else {
        append_event_log(
            Some(store),
            envelope,
            "pipeline.assign",
            "WARN",
            "playbook lookup skipped because change_kind is missing or empty",
            None,
        )
        .await;
        warn!(
            "skipping playbook lookup: change_kind is missing or empty (event_id={})",
            envelope.head.event_id
        );
        return Ok(None);
    };
    let Some(change_kind) = normalize_change_kind(change_kind_raw) else {
        append_event_log(
            Some(store),
            envelope,
            "pipeline.assign",
            "WARN",
            "playbook lookup skipped because change_kind is unsupported",
            Some(serde_json::json!({ "change_kind": change_kind_raw })),
        )
        .await;
        warn!(
            "skipping playbook lookup: invalid change_kind='{}' (event_id={})",
            change_kind_raw, envelope.head.event_id
        );
        return Ok(None);
    };

    append_event_log(
        Some(store),
        envelope,
        "pipeline.assign",
        "INFO",
        "looking up matched playbook",
        Some(serde_json::json!({
            "changed_object_type": changed_object_type,
            "change_kind": change_kind,
        })),
    )
    .await;

    let matched_playbooks = store
        .find_matched_playbooks(changed_object_type, change_kind)
        .await
        .map_err(|err| PipelineError::AssignerFailed(format!("playbook lookup error: {err}")))?;

    if matched_playbooks.is_empty() {
        append_event_log(
            Some(store),
            envelope,
            "pipeline.assign",
            "INFO",
            "no active playbook matched for event",
            Some(serde_json::json!({
                "changed_object_type": changed_object_type,
                "change_kind": change_kind,
            })),
        )
        .await;
        return Ok(None);
    }

    let mut tadpole = Tadpole::from_envelope(envelope.clone());

    append_event_log(
        Some(store),
        envelope,
        "pipeline.assign",
        "INFO",
        "matched playbooks",
        Some(serde_json::json!({
            "matched_playbook_count": matched_playbooks.len(),
            "playbook_ids": matched_playbooks.iter().map(|playbook| playbook.id.as_str()).collect::<Vec<_>>(),
        })),
    )
    .await;

    let first_playbook = matched_playbooks
        .first()
        .expect("matched playbooks not empty");
    tadpole.tail.matched_playbook = Some(MatchedPlaybook {
        id: first_playbook.id.clone(),
        name: first_playbook.name.clone(),
        version: first_playbook.version.clone(),
        execution_mode: first_playbook.execution_mode.clone(),
        trigger_object_type: first_playbook.trigger.object_type.clone(),
        trigger_change_kind: first_playbook.trigger.change_kind.clone(),
    });
    tadpole.tail.execution_mode = first_playbook.execution_mode.clone();

    let mut all_rule_ids: Vec<String> = Vec::new();
    for playbook in &matched_playbooks {
        for rule_ref in &playbook.rules {
            if !all_rule_ids.contains(&rule_ref.rule_id) {
                all_rule_ids.push(rule_ref.rule_id.clone());
            }
        }
    }

    let loaded_rules = store
        .find_active_rules(&all_rule_ids)
        .await
        .map_err(|err| PipelineError::AssignerFailed(format!("rule lookup error: {err}")))?;

    append_event_log(
        Some(store),
        envelope,
        "pipeline.assign",
        "INFO",
        "loaded active rules for matched playbooks",
        Some(serde_json::json!({
            "matched_playbook_count": matched_playbooks.len(),
            "requested_rule_count": all_rule_ids.len(),
            "loaded_rule_count": loaded_rules.len(),
        })),
    )
    .await;

    let by_id: HashMap<String, _> = loaded_rules
        .into_iter()
        .map(|rule| (rule.id.clone(), rule))
        .collect();

    for playbook in matched_playbooks {
        append_event_log(
            Some(store),
            envelope,
            "pipeline.assign",
            "INFO",
            "matched playbook",
            Some(serde_json::json!({
                "playbook_id": playbook.id,
                "playbook_name": playbook.name,
                "execution_mode": playbook.execution_mode,
            })),
        )
        .await;

        tadpole
            .tail
            .assigned_playbooks
            .push(engine_core::dto::rules::PlaybookAssignment {
                playbook_id: playbook.id.clone(),
                reason: format!(
                    "trigger matched: object_type={} change_kind={}",
                    changed_object_type, change_kind
                ),
            });

        let mut ordered_refs = playbook.rules.clone();
        ordered_refs.sort_by_key(|rule| rule.order_seq);

        for rule_ref in ordered_refs {
            match by_id.get(&rule_ref.rule_id) {
                Some(rule) => {
                    let conditions = rule
                        .conditions
                        .iter()
                        .map(|condition| RuleCondition {
                            object_key: condition.object_key.clone(),
                            operator: condition.operator.clone(),
                            key_data_type: condition.key_data_type.clone(),
                            value: condition.expected_value(),
                        })
                        .collect();

                    tadpole.tail.ordered_rules.push(RuleSpec {
                        playbook_id: playbook.id.clone(),
                        rule_id: rule.id.clone(),
                        kind: RuleKind::Boolean,
                        expr: None,
                        rule_name: rule.name.clone(),
                        object_type: Some(rule.object.object_type.clone()),
                        order_seq: Some(rule_ref.order_seq),
                        priority: Some(if rule_ref.is_critical {
                            "HIGH".to_string()
                        } else {
                            "NORMAL".to_string()
                        }),
                        conditions,
                        logic: rule.logic.clone(),
                        is_critical: rule_ref.is_critical,
                        skip_reason: None,
                        action_template_id: rule_ref.action_template_id.clone(),
                    });
                }
                None => {
                    warn!(
                        "excluding rule from evaluation: playbook_id={} rule_id={} reason=missing_or_not_active",
                        playbook.id, rule_ref.rule_id
                    );
                    append_event_log(
                        Some(store),
                        envelope,
                        "pipeline.assign",
                        "WARN",
                        "rule excluded because it is missing or inactive",
                        Some(serde_json::json!({
                            "playbook_id": playbook.id,
                            "rule_id": rule_ref.rule_id,
                        })),
                    )
                    .await;
                }
            }
        }

        append_event_log(
            Some(store),
            envelope,
            "pipeline.assign",
            "INFO",
            "playbook rules added to tadpole",
            Some(serde_json::json!({
                "playbook_id": playbook.id,
                "rule_count": tadpole
                    .tail
                    .ordered_rules
                    .iter()
                    .filter(|rule| rule.playbook_id == playbook.id)
                    .count(),
            })),
        )
        .await;
    }

    append_event_log(
        Some(store),
        envelope,
        "pipeline.assign",
        "INFO",
        "tadpole built from playbooks and rules",
        Some(serde_json::json!({
            "playbook_count": tadpole.tail.assigned_playbooks.len(),
            "rule_count": tadpole.tail.ordered_rules.len(),
        })),
    )
    .await;

    Ok(Some(tadpole))
}

/// Run the full intake pipeline and optionally record to MongoDB.
/// When the decision is FAIL, optionally creates actions via ActionBuilder.
/// Returns governance conclusion events on success.
pub async fn process_envelope(
    envelope: CanonicalEnvelope,
    _assigner: Addr<AssignerActor>,
    dispatcher: Addr<DispatcherActor>,
    orchestrator: Addr<OrchestratorActor>,
    diagnostics: Addr<DiagnosticsActor>,
    store: Option<Arc<dyn IntakeStore>>,
    action_builder: Option<Arc<ActionBuilder>>,
    metric_manager: Option<Arc<MetricManager>>,
    action_feed_publisher: Option<Arc<dyn ActionFeedPublisher>>,
) -> Result<Vec<serde_json::Value>, PipelineError> {
    append_event_log(
        store.as_ref(),
        &envelope,
        "pipeline.start",
        "INFO",
        "pipeline processing started",
        Some(serde_json::json!({
            "event_name": envelope.head.event_name,
            "tenant_id": envelope.head.tenant_id,
        })),
    )
    .await;

    let validation_errors = validate_envelope(&envelope);
    if !validation_errors.is_empty() {
        warn!(
            "intake validation failed: event_id={} errors={:?}",
            envelope.head.event_id, validation_errors
        );
        diagnostics.do_send(RecordError {
            kind: "intake.validation_failed".to_string(),
        });
        if let Some(ref s) = store {
            let _ = s
                .record_intake(
                    &envelope,
                    "validation_failed",
                    Some(&validation_errors),
                    None,
                    None,
                )
                .await;
        }
        append_event_log(
            store.as_ref(),
            &envelope,
            "pipeline.validation",
            "ERROR",
            "envelope validation failed",
            Some(serde_json::json!({ "errors": validation_errors })),
        )
        .await;
        return Err(PipelineError::ValidationFailed(validation_errors));
    }

    append_event_log(
        store.as_ref(),
        &envelope,
        "pipeline.validation",
        "INFO",
        "envelope validation passed",
        None,
    )
    .await;

    let Some(store_ref) = store.as_ref() else {
        let msg = "intake store is required for playbook/rule lookup".to_string();
        error!("intake assigner failed: {}", msg);
        diagnostics.do_send(RecordError {
            kind: "intake.assigner_failed".to_string(),
        });
        append_event_log(None, &envelope, "pipeline.assign", "ERROR", &msg, None).await;
        return Err(PipelineError::AssignerFailed(msg));
    };

    let tadpole = build_tadpole_from_store(&envelope, store_ref).await?;

    if tadpole.is_none() {
        let decision = Decision::inconclusive("intake.no_playbook_match", "no playbooks matched");
        let conclusion = build_governance_conclusion_from_decision(&envelope, &decision, &[], None);
        info!("intake completed: decision={:?} events=1", decision);
        if let Some(ref s) = store {
            if let Ok(response) = serde_json::to_value(&[conclusion.clone()]) {
                let _ = s
                    .record_intake(&envelope, "success", None, Some(&response), None)
                    .await;
            }
        }
        append_event_log(
            store.as_ref(),
            &envelope,
            "pipeline.complete",
            "INFO",
            "pipeline completed without matched playbook",
            Some(serde_json::json!({
                "decision": "INCONCLUSIVE",
                "response_event_count": 1,
            })),
        )
        .await;
        return Ok(vec![
            serde_json::to_value(conclusion).expect("governance conclusion serialization"),
        ]);
    }

    append_event_log(
        store.as_ref(),
        &envelope,
        "pipeline.dispatch",
        "INFO",
        "sending event to dispatcher",
        None,
    )
    .await;

    let tadpole = match dispatcher
        .send(Dispatch {
            tadpole: tadpole.expect("tadpole checked as some"),
        })
        .await
    {
        Ok(t) => t,
        Err(e) => {
            let msg = format!("dispatcher error: {e}");
            error!("intake dispatcher failed: {}", msg);
            diagnostics.do_send(RecordError {
                kind: "intake.dispatch_failed".to_string(),
            });
            if let Some(ref s) = store {
                let _ = s
                    .record_intake(
                        &envelope,
                        "dispatcher_failed",
                        None,
                        None,
                        Some(msg.clone()),
                    )
                    .await;
            }
            append_event_log(
                store.as_ref(),
                &envelope,
                "pipeline.dispatch",
                "ERROR",
                "dispatcher execution failed",
                Some(serde_json::json!({ "error": msg })),
            )
            .await;
            return Err(PipelineError::DispatchFailed(msg));
        }
    };

    append_event_log(
        store.as_ref(),
        &envelope,
        "pipeline.dispatch",
        "INFO",
        "dispatcher completed",
        Some(serde_json::json!({
            "evaluation_count": tadpole.tail.evaluations.len(),
        })),
    )
    .await;

    let result = match orchestrator.send(Orchestrate { tadpole }).await {
        Ok(r) => r,
        Err(e) => {
            let msg = format!("orchestrator error: {e}");
            error!("intake orchestrator failed: {}", msg);
            diagnostics.do_send(RecordError {
                kind: "intake.orchestrator_failed".to_string(),
            });
            if let Some(ref s) = store {
                let _ = s
                    .record_intake(
                        &envelope,
                        "orchestrator_failed",
                        None,
                        None,
                        Some(msg.clone()),
                    )
                    .await;
            }
            append_event_log(
                store.as_ref(),
                &envelope,
                "pipeline.orchestrate",
                "ERROR",
                "orchestrator execution failed",
                Some(serde_json::json!({ "error": msg })),
            )
            .await;
            return Err(PipelineError::OrchestratorFailed(msg));
        }
    };

    append_event_log(
        store.as_ref(),
        &envelope,
        "pipeline.orchestrate",
        "INFO",
        "orchestrator completed",
        Some(serde_json::json!({
            "decision": &result.decision,
            "evaluation_count": result.evaluations.len(),
        })),
    )
    .await;

    append_rule_evaluation_logs(store.as_ref(), &envelope, &result.evaluations).await;

    diagnostics.do_send(RecordEvaluation {
        result: result.clone(),
    });

    let mut created_actions = Vec::new();
    if let Some(ref ab) = action_builder {
        append_event_log(
            store.as_ref(),
            &envelope,
            "pipeline.actions",
            "INFO",
            "evaluating action creation",
            Some(serde_json::json!({
                "decision": &result.decision,
                "route_to_action_builder": result.route_to_action_builder,
            })),
        )
        .await;

        if result.route_to_action_builder {
            // Priority 1: Rule-level template bindings (action_template_id on each failed rule).
            let binding_actions = ab
                .create_actions_from_rule_bindings(&envelope, &result)
                .await;
            match binding_actions {
                Ok(created) if !created.is_empty() => {
                    info!(
                        "ActionBuilder created {} action(s) from rule bindings",
                        created.len()
                    );
                    append_event_log(
                        store.as_ref(),
                        &envelope,
                        "pipeline.actions",
                        "INFO",
                        "rule-binding action creation completed",
                        Some(serde_json::json!({
                            "created_action_count": created.len(),
                            "creation_mode": "rule_bindings",
                        })),
                    )
                    .await;
                    created_actions = created;
                }
                Ok(_) => {
                    // Priority 2: Template-driven lookup by trigger criteria.
                    let template_actions =
                        ab.create_actions_from_templates(&envelope, &result).await;
                    match template_actions {
                        Ok(created) if !created.is_empty() => {
                            info!(
                                "ActionBuilder created {} action(s) from templates",
                                created.len()
                            );
                            append_event_log(
                                store.as_ref(),
                                &envelope,
                                "pipeline.actions",
                                "INFO",
                                "template-driven action creation completed",
                                Some(serde_json::json!({
                                    "created_action_count": created.len(),
                                    "creation_mode": "templates",
                                })),
                            )
                            .await;
                            created_actions = created;
                        }
                        Ok(_) => {
                            // Priority 3: Legacy review action behavior for FAIL/INCONCLUSIVE.
                            if should_create_review_actions(&result.decision) {
                                append_event_log(
                                    store.as_ref(),
                                    &envelope,
                                    "pipeline.actions",
                                    "INFO",
                                    "no matching templates; falling back to review action creation",
                                    None,
                                )
                                .await;
                                match ab
                                    .create_actions_for_failure(&envelope, &result, store.as_ref())
                                    .await
                                {
                                    Ok(actions) => {
                                        append_event_log(
                                            store.as_ref(),
                                            &envelope,
                                            "pipeline.actions",
                                            "INFO",
                                            "fallback action creation completed",
                                            Some(serde_json::json!({
                                                "created_action_count": actions.len(),
                                                "creation_mode": "legacy_fail",
                                            })),
                                        )
                                        .await;
                                        created_actions = actions;
                                    }
                                    Err(e) => {
                                        error!("ActionBuilder failed: {}", e);
                                        append_event_log(
                                            store.as_ref(),
                                            &envelope,
                                            "pipeline.actions",
                                            "ERROR",
                                            "fallback action creation failed",
                                            Some(serde_json::json!({ "error": e.to_string() })),
                                        )
                                        .await;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!("ActionBuilder template lookup failed: {}; falling back", e);
                            append_event_log(
                                store.as_ref(),
                                &envelope,
                                "pipeline.actions",
                                "ERROR",
                                "template-driven action creation failed",
                                Some(serde_json::json!({ "error": e.to_string() })),
                            )
                            .await;
                            if should_create_review_actions(&result.decision) {
                                match ab
                                    .create_actions_for_failure(&envelope, &result, store.as_ref())
                                    .await
                                {
                                    Ok(actions) => {
                                        append_event_log(
                                            store.as_ref(),
                                            &envelope,
                                            "pipeline.actions",
                                            "INFO",
                                            "fallback action creation completed after template failure",
                                            Some(serde_json::json!({
                                                "created_action_count": actions.len(),
                                                "creation_mode": "legacy_fail",
                                            })),
                                        )
                                        .await;
                                        created_actions = actions;
                                    }
                                    Err(e) => {
                                        error!("ActionBuilder failed: {}", e);
                                        append_event_log(
                                            store.as_ref(),
                                            &envelope,
                                            "pipeline.actions",
                                            "ERROR",
                                            "fallback action creation failed after template failure",
                                            Some(serde_json::json!({ "error": e.to_string() })),
                                        )
                                        .await;
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "ActionBuilder rule binding failed: {}; falling back to templates",
                        e
                    );
                    append_event_log(
                        store.as_ref(),
                        &envelope,
                        "pipeline.actions",
                        "ERROR",
                        "rule-binding action creation failed; falling back to templates",
                        Some(serde_json::json!({ "error": e.to_string() })),
                    )
                    .await;
                    // Fall back to template-driven lookup.
                    let template_actions =
                        ab.create_actions_from_templates(&envelope, &result).await;
                    match template_actions {
                        Ok(created) if !created.is_empty() => {
                            created_actions = created;
                        }
                        Ok(_) if should_create_review_actions(&result.decision) => {
                            match ab
                                .create_actions_for_failure(&envelope, &result, store.as_ref())
                                .await
                            {
                                Ok(actions) => created_actions = actions,
                                Err(e) => error!("ActionBuilder failed: {}", e),
                            }
                        }
                        Err(e) => error!("ActionBuilder template lookup also failed: {}", e),
                        _ => {}
                    }
                }
            }
        } else {
            info!("Orchestrator: skipping ActionBuilder (route_to_action_builder=false)");
            append_event_log(
                store.as_ref(),
                &envelope,
                "pipeline.actions",
                "INFO",
                "skipping ActionBuilder (route_to_action_builder=false)",
                None,
            )
            .await;
        }
    }

    if let Some(ref ab) = action_builder
        && should_create_review_actions(&result.decision)
    {
        let covered_rules: HashSet<(String, String)> = created_actions
            .iter()
            .map(|action| (action.playbook_id.clone(), action.rule_id.clone()))
            .collect();
        match ab
            .create_actions_for_failure_excluding(
                &envelope,
                &result,
                store.as_ref(),
                &covered_rules,
            )
            .await
        {
            Ok(actions) if !actions.is_empty() => {
                append_event_log(
                    store.as_ref(),
                    &envelope,
                    "pipeline.actions",
                    "INFO",
                    "missing failed-rule action creation completed",
                    Some(serde_json::json!({
                        "created_action_count": actions.len(),
                        "creation_mode": "legacy_fail_top_up",
                    })),
                )
                .await;
                created_actions.extend(actions);
            }
            Ok(_) => {}
            Err(e) => {
                error!(
                    "ActionBuilder failed while creating missing failed-rule actions: {}",
                    e
                );
                append_event_log(
                    store.as_ref(),
                    &envelope,
                    "pipeline.actions",
                    "ERROR",
                    "missing failed-rule action creation failed",
                    Some(serde_json::json!({ "error": e.to_string() })),
                )
                .await;
            }
        }
    }

    // Publish created actions to the actions queue for downstream consumers.
    if !created_actions.is_empty() {
        if let Some(ref publisher) = action_feed_publisher {
            match publisher.publish_actions(&created_actions).await {
                Ok(()) => {
                    info!(
                        "ActionFeedPublisher: published {} action(s) to queue",
                        created_actions.len()
                    );
                    append_event_log(
                        store.as_ref(),
                        &envelope,
                        "pipeline.action_feed",
                        "INFO",
                        "actions published to queue",
                        Some(serde_json::json!({
                            "published_action_count": created_actions.len(),
                        })),
                    )
                    .await;
                }
                Err(e) => {
                    error!("ActionFeedPublisher failed: {}", e);
                    append_event_log(
                        store.as_ref(),
                        &envelope,
                        "pipeline.action_feed",
                        "ERROR",
                        "failed to publish actions to queue",
                        Some(serde_json::json!({ "error": e.to_string() })),
                    )
                    .await;
                }
            }
        }
    }

    let mut metric_response = None;
    if let Some(ref metrics) = metric_manager {
        append_event_log(
            store.as_ref(),
            &envelope,
            "pipeline.metrics",
            "INFO",
            "recording pipeline metrics",
            None,
        )
        .await;
        match metrics
            .record_pipeline_outcome(&envelope, &result, &created_actions, store.as_ref())
            .await
        {
            Ok(events) => {
                append_event_log(
                    store.as_ref(),
                    &envelope,
                    "pipeline.metrics",
                    "INFO",
                    "metrics recorded",
                    Some(serde_json::json!({
                        "metric_event_count": events.len(),
                    })),
                )
                .await;
                metric_response = if events.is_empty() {
                    None
                } else {
                    Some(MetricManager::format_metric_response(
                        Some(&envelope.head.event_id),
                        Some(&envelope.head.event_name),
                        Some(&envelope.head.tenant_id),
                        &events,
                    ))
                };
            }
            Err(e) => {
                error!("MetricManager failed: {}", e);
                append_event_log(
                    store.as_ref(),
                    &envelope,
                    "pipeline.metrics",
                    "ERROR",
                    "metrics recording failed",
                    Some(serde_json::json!({ "error": e.to_string() })),
                )
                .await;
            }
        }
    }

    let mut governance_events = vec![
        serde_json::to_value(build_governance_conclusion(&envelope, &result))
            .expect("governance conclusion serialization"),
    ];
    if let Some(metric_response) = metric_response {
        governance_events.push(metric_response);
    }
    let overall_decision = result.decision.clone();

    info!(
        "intake completed: decision={:?} events={}",
        overall_decision,
        governance_events.len()
    );
    if let Some(ref s) = store {
        if let Ok(response) = serde_json::to_value(&governance_events) {
            let _ = s
                .record_intake(&envelope, "success", None, Some(&response), None)
                .await;
        }
    }
    append_event_log(
        store.as_ref(),
        &envelope,
        "pipeline.complete",
        "INFO",
        "pipeline completed successfully",
        Some(serde_json::json!({
            "decision": decision_status(&overall_decision),
            "decision_reason_code": decision_reason_code(&overall_decision),
            "decision_reason": decision_message(&overall_decision),
            "response_event_count": governance_events.len(),
            "created_action_count": created_actions.len(),
        })),
    )
    .await;
    Ok(governance_events)
}

pub fn validate_envelope(payload: &CanonicalEnvelope) -> Vec<String> {
    let mut errors = Vec::new();

    if !is_valid_ulid(payload.head.event_id.trim()) {
        errors.push("head.event_id must be a valid ULID".to_string());
    }
    if let Some(correlation_id) = payload.head.correlation_id.as_deref() {
        if !is_valid_ulid(correlation_id.trim()) {
            errors.push("head.correlation_id must be a valid ULID".to_string());
        }
    }
    if let Some(causation_id) = payload.head.causation_id.as_deref() {
        if !causation_id.trim().is_empty() && !is_valid_ulid(causation_id.trim()) {
            errors.push("head.causation_id must be a valid ULID".to_string());
        }
    }
    if payload.head.tenant_id.trim().is_empty() {
        errors.push("head.tenant_id is required".to_string());
    }
    if payload.head.event_name.trim().is_empty() {
        errors.push("head.event_name is required".to_string());
    }
    // external_dependency_id is optional; empty strings are tolerated
    if let Some(change_kind) = payload.head.change_kind.as_deref() {
        if normalize_change_kind(change_kind).is_none() {
            errors.push(
                "head.change_kind must be create/created, update/updated, or delete/deleted"
                    .to_string(),
            );
        }
    }
    if payload.body.is_null() {
        errors.push("body is required".to_string());
    }

    if errors.is_empty() {
        debug!(
            "envelope validation passed: event_id={} tenant_id={} event_name= {}",
            payload.head.event_id, payload.head.tenant_id, payload.head.event_name
        );
    }
    errors
}

fn build_governance_conclusion(
    envelope: &CanonicalEnvelope,
    result: &engine_core::dto::orchestration::OrchestrationResult,
) -> CanonicalEnvelope {
    build_governance_conclusion_from_decision(
        envelope,
        &result.decision,
        &result.evaluations,
        result.matched_playbook.as_ref(),
    )
}

fn build_governance_conclusion_from_decision(
    envelope: &CanonicalEnvelope,
    decision: &Decision,
    evaluations: &[RuleEvaluation],
    matched_playbook: Option<&MatchedPlaybook>,
) -> CanonicalEnvelope {
    let ulid_service = UlidService::new();
    let status = match decision {
        Decision::Pass { .. } => "PASS",
        Decision::Fail { .. } => "FAIL",
        Decision::Inconclusive { .. } => "INCONCLUSIVE",
    };
    let rationale = decision_message(decision).to_string();
    let final_reason_code = decision_reason_code(decision).to_string();
    let conclusion_type = match decision {
        Decision::Pass { .. } => "KnownSupplierConfirmed".to_string(),
        Decision::Fail { .. } => evaluations
            .iter()
            .find(|eval| eval.decision.is_fail())
            .map(|eval| eval.rule_id.clone())
            .unwrap_or_else(|| "UnknownSupplierDetected".to_string()),
        Decision::Inconclusive { .. } => "GovernanceReviewRequired".to_string(),
    };

    let mut evidence_refs = Vec::new();
    let object_type = envelope.head.changed_object_type.clone();
    let object_id = envelope.head.changed_object_id.clone();
    if object_type.is_some() || object_id.is_some() {
        evidence_refs.push(serde_json::json!({
            "event_id": envelope.head.event_id.clone(),
            "object_type": object_type,
            "object_id": object_id,
        }));
    } else {
        evidence_refs.push(serde_json::json!({
            "event_id": envelope.head.event_id.clone(),
        }));
    }

    let matched_playbook_json = matched_playbook.map(|playbook| {
        serde_json::json!({
            "id": playbook.id,
            "name": playbook.name,
            "version": playbook.version,
            "execution_mode": playbook.execution_mode,
            "trigger": {
                "object_type": playbook.trigger_object_type,
                "change_kind": playbook.trigger_change_kind,
            }
        })
    });

    let rule_evaluations_json: Vec<serde_json::Value> = evaluations
        .iter()
        .map(|evaluation| {
            let result = match &evaluation.decision {
                Decision::Pass { .. } => "PASS",
                Decision::Fail { .. } => "FAIL",
                Decision::Inconclusive { .. } => "INCONCLUSIVE",
            };
            serde_json::json!({
                "order_seq": evaluation.order_seq,
                "rule_id": evaluation.rule_id,
                "rule_name": evaluation.rule_name,
                "object_type": evaluation.object_type,
                "is_critical": evaluation.is_critical,
                "priority": evaluation.priority,
                "result": result,
                "reason_code": evaluation.reason_code,
                "reason": evaluation.reason,
                "checks": evaluation.checks,
                "diagnostics": build_rule_diagnostics(evaluation),
                "action_template_id": evaluation.action_template_id,
            })
        })
        .collect();

    let trace_id = ulid_service.generate_string();
    let content_hash = compute_content_hash(&envelope.body);
    CanonicalEnvelope {
        head: domain::envelope::TadpoleHead {
            event_id: ulid_service.generate_string(),
            event_name: "GovernanceConclusionEmitted".to_string(),
            event_category: Some("GovernanceConclusion".to_string()),
            tenant_id: envelope.head.tenant_id.clone(),
            correlation_id: envelope.head.correlation_id.clone(),
            causation_id: Some(envelope.head.event_id.clone()),
            occurred_at: envelope.head.occurred_at.clone(),
            originating_function: Some("GovernanceEngine".to_string()),
            originating_application: Some("TadpoleEngine".to_string()),
            environment: envelope.head.environment.clone(),
            external_dependency_id: envelope.head.external_dependency_id.clone(),
            changed_object_type: envelope.head.changed_object_type.clone(),
            changed_object_id: envelope.head.changed_object_id.clone(),
            change_kind: envelope.head.change_kind.clone(),
        },
        body: serde_json::json!({
            "conclusion_type": conclusion_type,
            "scope_model": "Dependency",
            "status": status,
            "rationale": rationale,
            "matched_playbook": matched_playbook_json,
            "rule_evaluations": rule_evaluations_json,
            "final_decision_detail": {
                "decision": status,
                "reason_code": final_reason_code,
                "reason": rationale,
            },
            "content_hash": content_hash,
            "evidence_refs": evidence_refs,
            "trace_ref": {
                "trace_id": trace_id,
                "trace_level": "SUMMARY"
            }
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_evaluation(decision: Decision, checks: Vec<ConditionCheck>) -> RuleEvaluation {
        RuleEvaluation {
            playbook_id: "playbook_1".to_string(),
            rule_id: "rule_1".to_string(),
            rule_name: Some("Contract Active Rule".to_string()),
            object_type: Some("contract".to_string()),
            order_seq: Some(1),
            is_critical: true,
            priority: Some("HIGH".to_string()),
            decision,
            reason_code: "rule.conditions_fail".to_string(),
            reason: "one or more conditions failed for snapshot 'contract'".to_string(),
            checks,
            duration_ms: 7,
            action_template_id: None,
        }
    }

    #[test]
    fn build_rule_diagnostics_includes_failure_guidance() {
        let evaluation = sample_evaluation(
            Decision::fail("rule.conditions_fail", "rule failed"),
            vec![ConditionCheck {
                object_key: "status".to_string(),
                operator: "eq".to_string(),
                expected: Some(serde_json::json!("ACTIVE")),
                actual: Some(serde_json::json!("INACTIVE")),
                status: "FAIL".to_string(),
                reason: "value does not satisfy condition".to_string(),
            }],
        );

        let diagnostics = build_rule_diagnostics(&evaluation);
        assert_eq!(diagnostics["rule_status"], "FAIL");
        assert_eq!(
            diagnostics["failing_checks"][0]["how_to_pass"],
            "Make 'status' satisfy 'eq': expected ACTIVE, actual INACTIVE."
        );
        assert_eq!(diagnostics["check_summary"]["fail"], serde_json::json!(1));
    }

    #[test]
    fn build_rule_diagnostics_includes_missing_key_guidance() {
        let evaluation = sample_evaluation(
            Decision::inconclusive("rule.conditions_inconclusive", "rule inconclusive"),
            vec![ConditionCheck {
                object_key: "supplier.id".to_string(),
                operator: "exists".to_string(),
                expected: Some(serde_json::json!(true)),
                actual: None,
                status: "INCONCLUSIVE".to_string(),
                reason: "key missing in snapshot".to_string(),
            }],
        );

        let diagnostics = build_rule_diagnostics(&evaluation);
        assert_eq!(diagnostics["rule_status"], "INCONCLUSIVE");
        assert_eq!(
            diagnostics["how_to_pass"][0],
            "Provide 'supplier.id' in the evaluated snapshot so 'exists' can be checked against true."
        );
        assert_eq!(
            diagnostics["check_summary"]["inconclusive"],
            serde_json::json!(1)
        );
    }
}
