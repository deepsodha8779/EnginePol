//! ActionBuilder: creates task/action records based on orchestration results.
//! Supports template-driven action creation (PASS/FAIL/INCONCLUSIVE triggers)
//! and legacy per-failed-rule action creation. Uses idempotency key/hash to avoid duplicates.

use domain::envelope::{CanonicalEnvelope, compute_idempotency_hash};
use engine_core::dto::evaluation::RuleEvaluation;
use engine_core::dto::orchestration::OrchestrationResult;
use engine_core::ulid::UlidService;
use log::{info, warn};
use mongodb::bson::DateTime;
use std::sync::Arc;

use crate::action_store::{ActionRecord, ActionStore};
use crate::action_template::{ActionTemplate, ActionTemplateStore, TriggerEventType};
use crate::mongo_store::IntakeStore;
use crate::work_router::{RouteRequest, WorkRouter};

const TASK_TYPE: &str = "TASK_GOVERNANCE_REVIEW";
const STATUS_CREATED: &str = "created";

/// Build a human-readable action message from a rule evaluation's failing checks.
/// Describes what is missing or what needs to be done for the user.
/// Returns `(action_title, action_description)`.
/// The title is a short, user-facing label; the description gives detail
/// about what failed — neither exposes the internal rule name.
fn build_action_title_and_description(evaluation: &RuleEvaluation) -> (String, String) {
    let failing_hints: Vec<String> = evaluation
        .checks
        .iter()
        .filter(|c| c.status != "PASS")
        .map(|check| {
            let expected_str = check
                .expected
                .as_ref()
                .map(|v| match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .unwrap_or_else(|| "N/A".to_string());
            let actual_str = check
                .actual
                .as_ref()
                .map(|v| match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .unwrap_or_else(|| "N/A".to_string());

            match check.status.as_str() {
                "FAIL" => format!(
                    "'{}' does not satisfy '{}': expected {}, got {}",
                    check.object_key, check.operator, expected_str, actual_str
                ),
                "INCONCLUSIVE" if check.actual.is_none() => format!(
                    "'{}' is missing from the data; provide it so '{}' can be evaluated",
                    check.object_key, check.operator
                ),
                "INCONCLUSIVE" => format!(
                    "'{}' could not be evaluated with operator '{}'",
                    check.object_key, check.operator
                ),
                _ => format!("'{}' requires attention", check.object_key),
            }
        })
        .collect();

    let title = "Governance Review Required".to_string();

    let description = if failing_hints.is_empty() {
        format!("Review required: {}", evaluation.reason)
    } else {
        format!("Action required: {}", failing_hints.join("; "))
    };

    (title, description)
}

/// Builds and persists action records when the pipeline produces results.
/// When an ActionTemplateStore is configured, matches templates by trigger criteria
/// (event type, object type, tenant) to create template-driven actions.
/// Falls back to per-failed-rule behavior when no template store is available.
/// Deduplicates by idempotency key (hash) before inserting.
/// Optionally uses WorkRouter to resolve assignee/queue and notification channels.
#[derive(Clone)]
pub struct ActionBuilder {
    store: Arc<dyn ActionStore>,
    work_router: Option<Arc<dyn WorkRouter>>,
    template_store: Option<Arc<dyn ActionTemplateStore>>,
}

impl ActionBuilder {
    pub fn new(store: Arc<dyn ActionStore>) -> Self {
        Self {
            store,
            work_router: None,
            template_store: None,
        }
    }

    pub fn with_work_router(store: Arc<dyn ActionStore>, work_router: Arc<dyn WorkRouter>) -> Self {
        Self {
            store,
            work_router: Some(work_router),
            template_store: None,
        }
    }

    pub fn with_template_store(
        store: Arc<dyn ActionStore>,
        template_store: Arc<dyn ActionTemplateStore>,
    ) -> Self {
        Self {
            store,
            work_router: None,
            template_store: Some(template_store),
        }
    }

    pub fn with_all(
        store: Arc<dyn ActionStore>,
        work_router: Arc<dyn WorkRouter>,
        template_store: Arc<dyn ActionTemplateStore>,
    ) -> Self {
        Self {
            store,
            work_router: Some(work_router),
            template_store: Some(template_store),
        }
    }

    /// Build action title and description from the first matching rule evaluation for a playbook.
    fn build_title_and_description_for_rule(
        result: &OrchestrationResult,
        playbook_id: &str,
        rule_id: &str,
    ) -> (Option<String>, Option<String>) {
        match result
            .evaluations
            .iter()
            .find(|e| e.playbook_id == playbook_id && e.rule_id == rule_id)
        {
            Some(eval) => {
                let (title, desc) = build_action_title_and_description(eval);
                (Some(title), Some(desc))
            }
            None => (None, None),
        }
    }

    /// Map an orchestration decision to a trigger event type.
    fn decision_to_event_type(result: &OrchestrationResult) -> TriggerEventType {
        if result.decision.is_fail() {
            TriggerEventType::RuleFailed
        } else if result.decision.is_pass() {
            TriggerEventType::RulePassed
        } else {
            TriggerEventType::RuleInconclusive
        }
    }

    fn should_create_review_action(result: &OrchestrationResult) -> bool {
        result.decision.is_fail() || result.decision.is_inconclusive()
    }

    /// Create actions driven by matched ActionTemplates.
    /// Looks up ACTIVE templates matching the decision's event type, tenant, and object type.
    /// Returns the list of newly created actions (empty if no templates match).
    pub async fn create_actions_from_templates(
        &self,
        envelope: &CanonicalEnvelope,
        result: &OrchestrationResult,
    ) -> Result<Vec<ActionRecord>, Box<dyn std::error::Error + Send + Sync>> {
        let template_store = match &self.template_store {
            Some(ts) => ts,
            None => return Ok(Vec::new()),
        };

        let event_type = Self::decision_to_event_type(result);
        let tenant_id = envelope.head.tenant_id.as_str();
        let object_type = envelope
            .head
            .changed_object_type
            .as_deref()
            .unwrap_or("Unknown");

        let templates = template_store
            .find_templates_by_trigger(tenant_id, object_type, &event_type)
            .await?;

        if templates.is_empty() {
            return Ok(Vec::new());
        }

        info!(
            "ActionBuilder: {} template(s) matched for tenant={} object_type={} event_type={:?}",
            templates.len(),
            tenant_id,
            object_type,
            event_type
        );

        let ulid = UlidService::new();
        let object_id = envelope
            .head
            .changed_object_id
            .as_deref()
            .unwrap_or("Unknown");
        let event_id = envelope.head.event_id.clone();
        let event_name = envelope.head.event_name.clone();
        let changed_object_type = envelope.head.changed_object_type.clone();
        let changed_object_id = envelope.head.changed_object_id.clone();

        let mut created = Vec::new();

        for template in &templates {
            // Determine which playbooks to create actions for.
            let playbook_ids = self.resolve_playbook_ids(result, template);

            for playbook_id in &playbook_ids {
                let first_rule =
                    self.find_first_rule_for_playbook(result, playbook_id, &event_type);

                // Include template_id in idempotency key to allow multiple templates
                // to create separate actions for the same event+playbook.
                let idempotency_key = format!(
                    "{}:{}:{}:{}:{}:{}",
                    tenant_id, object_type, object_id, TASK_TYPE, playbook_id, template.template_id
                );
                let idempotency_hash = compute_idempotency_hash(&idempotency_key);

                if let Some(_existing) = self
                    .store
                    .find_by_idempotency_hash(&idempotency_hash)
                    .await?
                {
                    info!(
                        "action already exists for template={} playbook={}; skipping publish/metric output",
                        template.template_id, playbook_id
                    );
                    continue;
                }

                let execution_mode_str = format!("{:?}", template.trigger.execution_mode);
                let escalation_unit_str = template
                    .responsibility
                    .escalation_duration_unit
                    .as_ref()
                    .map(|u| format!("{:?}", u));

                let action_id = ulid.generate_string();
                let (action_title, action_description) =
                    Self::build_title_and_description_for_rule(result, playbook_id, first_rule);
                let mut record = ActionRecord {
                    action_id: action_id.clone(),
                    idempotency_key: idempotency_key.clone(),
                    idempotency_hash: idempotency_hash.clone(),
                    tenant_id: tenant_id.to_string(),
                    event_id: event_id.clone(),
                    event_name: event_name.clone(),
                    playbook_id: playbook_id.to_string(),
                    rule_id: first_rule.to_string(),
                    task_type: TASK_TYPE.to_string(),
                    status: STATUS_CREATED.to_string(),
                    changed_object_type: changed_object_type.clone(),
                    changed_object_id: changed_object_id.clone(),
                    created_at: DateTime::now(),
                    action_template_id: Some(template.template_id.clone()),
                    execution_mode: Some(execution_mode_str),
                    responsible_user: template.responsibility.responsible_user.clone(),
                    responsible_role: template.responsibility.responsible_role.clone(),
                    escalation_duration: template.responsibility.escalation_duration,
                    escalation_duration_unit: escalation_unit_str,
                    require_document_upload: Some(template.evidence.require_document_upload),
                    require_comment: Some(template.evidence.require_comment),
                    require_approval_reference: Some(template.evidence.require_approval_reference),
                    action_title,
                    action_description,
                    assigned_to_type: None,
                    assigned_to_id: None,
                    assigned_to_name: None,
                };

                self.store.insert_action(&record).await?;
                info!(
                    "action created from template: action_id={} template={} playbook={}",
                    action_id, template.template_id, playbook_id
                );

                let routing = self
                    .route_action(
                        tenant_id,
                        playbook_id,
                        &action_id,
                        template.responsibility.responsible_user.as_deref(),
                        template.responsibility.responsible_role.as_deref(),
                    )
                    .await;
                Self::apply_routing(&mut record, routing);
                created.push(record);
            }
        }

        Ok(created)
    }

    /// Determine which playbook IDs to create actions for based on the template's
    /// associated playbooks/rules and the orchestration result.
    fn resolve_playbook_ids<'a>(
        &self,
        result: &'a OrchestrationResult,
        template: &ActionTemplate,
    ) -> Vec<String> {
        // Collect all playbook IDs from the orchestration result.
        let result_playbook_ids: std::collections::HashSet<&str> = result
            .playbooks
            .iter()
            .map(|p| p.playbook_id.as_str())
            .collect();

        if !template.associated_playbook_ids.is_empty() {
            // Filter to playbooks that appear in both the template's list and the result.
            template
                .associated_playbook_ids
                .iter()
                .filter(|pid| result_playbook_ids.contains(pid.as_str()))
                .cloned()
                .collect()
        } else if !template.associated_rule_ids.is_empty() {
            // Find playbooks that contain any of the template's associated rules.
            let rule_set: std::collections::HashSet<&str> = template
                .associated_rule_ids
                .iter()
                .map(|r| r.as_str())
                .collect();
            result
                .evaluations
                .iter()
                .filter(|e| rule_set.contains(e.rule_id.as_str()))
                .map(|e| e.playbook_id.clone())
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect()
        } else {
            // No filtering: use all playbooks from the result.
            result_playbook_ids
                .into_iter()
                .map(|s| s.to_string())
                .collect()
        }
    }

    /// Find the first relevant rule for a playbook given the trigger event type.
    fn find_first_rule_for_playbook<'a>(
        &self,
        result: &'a OrchestrationResult,
        playbook_id: &str,
        event_type: &TriggerEventType,
    ) -> &'a str {
        result
            .evaluations
            .iter()
            .find(|e| {
                e.playbook_id == playbook_id
                    && match event_type {
                        TriggerEventType::RuleFailed => e.decision.is_fail(),
                        TriggerEventType::RulePassed => e.decision.is_pass(),
                        TriggerEventType::RuleInconclusive => e.decision.is_inconclusive(),
                        TriggerEventType::Manual => true,
                    }
            })
            .map(|e| e.rule_id.as_str())
            .unwrap_or("none")
    }

    /// Route an action via WorkRouter if configured.
    /// Persists the resolved assignee on the action record and returns the
    /// assignment so callers can update in-memory records before publishing.
    async fn route_action(
        &self,
        tenant_id: &str,
        playbook_id: &str,
        action_id: &str,
        responsible_user: Option<&str>,
        responsible_role: Option<&str>,
    ) -> Option<(String, String, Option<String>)> {
        if let Some(ref router) = self.work_router {
            let req = RouteRequest {
                tenant_id: tenant_id.to_string(),
                role_id: playbook_id.to_string(),
                task_type: TASK_TYPE.to_string(),
                playbook_id: playbook_id.to_string(),
                action_id: action_id.to_string(),
                responsible_user: responsible_user.map(String::from),
                responsible_role: responsible_role.map(String::from),
            };
            match router.resolve(&req).await {
                Ok(route) => {
                    let type_str = match route.assignee_type {
                        crate::work_router::AssigneeType::User => "user",
                        crate::work_router::AssigneeType::Team => "team",
                        crate::work_router::AssigneeType::Queue => "queue",
                    };
                    info!(
                        "WorkRouter: action_id={} -> {} {} (fallback={})",
                        action_id,
                        route.assignee_id,
                        route.display_name.as_deref().unwrap_or(""),
                        route.used_fallback
                    );
                    if let Err(e) = self
                        .store
                        .update_action_assignment(
                            action_id,
                            type_str,
                            &route.assignee_id,
                            route.display_name.as_deref(),
                        )
                        .await
                    {
                        warn!(
                            "WorkRouter: failed to persist assignment for action_id={}: {}",
                            action_id, e
                        );
                    }
                    return Some((type_str.to_string(), route.assignee_id, route.display_name));
                }
                Err(e) => {
                    warn!(
                        "WorkRouter: resolve failed for action_id={}: {}",
                        action_id, e
                    );
                }
            }
        }
        None
    }

    /// Apply routing result to an in-memory action record so it reflects
    /// the assignment before being published to the queue.
    fn apply_routing(record: &mut ActionRecord, routing: Option<(String, String, Option<String>)>) {
        if let Some((assigned_type, assigned_id, assigned_name)) = routing {
            record.assigned_to_type = Some(assigned_type);
            record.assigned_to_id = Some(assigned_id);
            record.assigned_to_name = assigned_name;
        }
    }

    /// Create actions using rule-level template bindings.
    /// Each non-pass RuleEvaluation carries its own `action_template_id` (bound at config time).
    /// The ActionBuilder looks up each template by ID, checks relationships between
    /// actions (parent/child suppression), and creates ActionRecords.
    pub async fn create_actions_from_rule_bindings(
        &self,
        envelope: &CanonicalEnvelope,
        result: &OrchestrationResult,
    ) -> Result<Vec<ActionRecord>, Box<dyn std::error::Error + Send + Sync>> {
        let template_store = match &self.template_store {
            Some(ts) => ts,
            None => return Ok(Vec::new()),
        };

        // Collect failed/inconclusive evaluations that have a bound action_template_id.
        let bound_evals: Vec<_> = result
            .evaluations
            .iter()
            .filter(|e| {
                (e.decision.is_fail() || e.decision.is_inconclusive())
                    && e.action_template_id.is_some()
            })
            .collect();

        if bound_evals.is_empty() {
            return Ok(Vec::new());
        }

        let tenant_id = envelope.head.tenant_id.as_str();
        let object_id = envelope
            .head
            .changed_object_id
            .as_deref()
            .unwrap_or("Unknown");
        let object_type = envelope
            .head
            .changed_object_type
            .as_deref()
            .unwrap_or("Unknown");
        let event_id = envelope.head.event_id.clone();
        let event_name = envelope.head.event_name.clone();
        let changed_object_type = envelope.head.changed_object_type.clone();
        let changed_object_id = envelope.head.changed_object_id.clone();

        // Load all referenced templates.
        let mut templates_by_id = std::collections::HashMap::new();
        for eval in &bound_evals {
            let tmpl_id = eval.action_template_id.as_ref().unwrap();
            if !templates_by_id.contains_key(tmpl_id.as_str()) {
                if let Ok(Some(tmpl)) = template_store.find_template_by_id(tmpl_id).await {
                    templates_by_id.insert(tmpl_id.clone(), tmpl);
                } else {
                    warn!(
                        "ActionBuilder: template_id={} not found for rule_id={}; skipping",
                        tmpl_id, eval.rule_id
                    );
                }
            }
        }

        let ulid = UlidService::new();
        let mut created = Vec::new();

        for eval in &bound_evals {
            let tmpl_id = eval.action_template_id.as_ref().unwrap();
            let Some(template) = templates_by_id.get(tmpl_id.as_str()) else {
                continue;
            };

            // Parent/child suppression: if this rule's object_type contains a dot
            // (e.g. "sla.response_time"), check if the parent ("sla") also failed.
            // If the parent object itself failed, skip child-level actions.
            if let Some(ref rule_obj_type) = eval.object_type {
                if let Some(dot_pos) = rule_obj_type.find('.') {
                    let parent = &rule_obj_type[..dot_pos];
                    // Only suppress if a different rule failed for the parent object
                    let parent_failed = bound_evals.iter().any(|other| {
                        other.rule_id != eval.rule_id
                            && other.object_type.as_deref() == Some(parent)
                    });
                    if parent_failed {
                        info!(
                            "ActionBuilder: suppressing child action for rule_id={} (parent '{}' failed)",
                            eval.rule_id, parent
                        );
                        continue;
                    }
                }
            }

            let idempotency_key = format!(
                "{}:{}:{}:{}:{}:{}:{}",
                tenant_id,
                object_type,
                object_id,
                TASK_TYPE,
                eval.playbook_id,
                eval.rule_id,
                tmpl_id
            );
            let idempotency_hash = compute_idempotency_hash(&idempotency_key);

            if let Some(_existing) = self
                .store
                .find_by_idempotency_hash(&idempotency_hash)
                .await?
            {
                info!(
                    "action already exists for rule={} template={}; skipping publish/metric output",
                    eval.rule_id, tmpl_id
                );
                continue;
            }

            let execution_mode_str = format!("{:?}", template.trigger.execution_mode);
            let escalation_unit_str = template
                .responsibility
                .escalation_duration_unit
                .as_ref()
                .map(|u| format!("{:?}", u));

            let action_id = ulid.generate_string();
            let (action_title, action_description) = {
                let (t, d) = build_action_title_and_description(eval);
                (Some(t), Some(d))
            };
            let mut record = ActionRecord {
                action_id: action_id.clone(),
                idempotency_key,
                idempotency_hash,
                tenant_id: tenant_id.to_string(),
                event_id: event_id.clone(),
                event_name: event_name.clone(),
                playbook_id: eval.playbook_id.clone(),
                rule_id: eval.rule_id.clone(),
                task_type: TASK_TYPE.to_string(),
                status: STATUS_CREATED.to_string(),
                changed_object_type: changed_object_type.clone(),
                changed_object_id: changed_object_id.clone(),
                created_at: DateTime::now(),
                action_template_id: Some(tmpl_id.clone()),
                execution_mode: Some(execution_mode_str),
                responsible_user: template.responsibility.responsible_user.clone(),
                responsible_role: template.responsibility.responsible_role.clone(),
                escalation_duration: template.responsibility.escalation_duration,
                escalation_duration_unit: escalation_unit_str,
                require_document_upload: Some(template.evidence.require_document_upload),
                require_comment: Some(template.evidence.require_comment),
                require_approval_reference: Some(template.evidence.require_approval_reference),
                action_title,
                action_description,
                assigned_to_type: None,
                assigned_to_id: None,
                assigned_to_name: None,
            };

            self.store.insert_action(&record).await?;
            info!(
                "action created from rule binding: action_id={} rule_id={} template={}",
                action_id, eval.rule_id, tmpl_id
            );
            let routing = self
                .route_action(
                    tenant_id,
                    &eval.playbook_id,
                    &action_id,
                    template.responsibility.responsible_user.as_deref(),
                    template.responsibility.responsible_role.as_deref(),
                )
                .await;
            Self::apply_routing(&mut record, routing);
            created.push(record);
        }

        Ok(created)
    }

    /// Legacy method: create actions when the decision is FAIL or INCONCLUSIVE.
    /// Kept for backward compatibility when no template store is configured.
    /// Skips creation if an action with the same idempotency hash already exists.
    /// Returns only newly created actions so downstream feeds are not republished on duplicate intake.
    pub async fn create_actions_for_failure(
        &self,
        envelope: &CanonicalEnvelope,
        result: &OrchestrationResult,
        event_logger: Option<&Arc<dyn IntakeStore>>,
    ) -> Result<Vec<ActionRecord>, Box<dyn std::error::Error + Send + Sync>> {
        let excluded_rules = std::collections::HashSet::new();
        self.create_actions_for_failure_excluding(envelope, result, event_logger, &excluded_rules)
            .await
    }

    pub async fn create_actions_for_failure_excluding(
        &self,
        envelope: &CanonicalEnvelope,
        result: &OrchestrationResult,
        event_logger: Option<&Arc<dyn IntakeStore>>,
        excluded_rules: &std::collections::HashSet<(String, String)>,
    ) -> Result<Vec<ActionRecord>, Box<dyn std::error::Error + Send + Sync>> {
        if !Self::should_create_review_action(result) {
            return Ok(Vec::new());
        }

        let ulid = UlidService::new();
        let tenant_id = envelope.head.tenant_id.as_str();
        let object_type = envelope
            .head
            .changed_object_type
            .as_deref()
            .unwrap_or("Unknown");
        let object_id = envelope
            .head
            .changed_object_id
            .as_deref()
            .unwrap_or("Unknown");
        let event_id = envelope.head.event_id.clone();
        let event_name = envelope.head.event_name.clone();
        let changed_object_type = envelope.head.changed_object_type.clone();
        let changed_object_id = envelope.head.changed_object_id.clone();

        let mut created = Vec::new();

        let mut seen_failed_rules = std::collections::HashSet::new();
        let failed_rules: Vec<_> = result
            .evaluations
            .iter()
            .filter(|e| e.decision.is_fail() || e.decision.is_inconclusive())
            .filter(|e| !excluded_rules.contains(&(e.playbook_id.clone(), e.rule_id.clone())))
            .filter(|e| seen_failed_rules.insert((e.playbook_id.as_str(), e.rule_id.as_str())))
            .collect();

        for evaluation in failed_rules {
            let playbook_id = evaluation.playbook_id.as_str();
            let rule_id = evaluation.rule_id.as_str();
            let idempotency_key = format!(
                "{}:{}:{}:{}:{}:{}",
                tenant_id, object_type, object_id, TASK_TYPE, playbook_id, rule_id
            );
            let idempotency_hash = compute_idempotency_hash(&idempotency_key);

            if let Some(_existing) = self
                .store
                .find_by_idempotency_hash(&idempotency_hash)
                .await?
            {
                info!(
                    "action already exists for idempotency_key (hash prefix); skipping publish/metric output: {}",
                    &idempotency_hash[..idempotency_hash.len().min(16)]
                );
                if let Some(store) = event_logger {
                    let details = serde_json::json!({
                        "playbook_id": playbook_id,
                        "rule_id": rule_id,
                        "idempotency_hash_prefix": &idempotency_hash[..idempotency_hash.len().min(16)],
                    });
                    let _ = store
                        .append_event_log(
                            envelope,
                            "action_builder",
                            "INFO",
                            "action reused because matching idempotency hash already exists",
                            Some(&details),
                        )
                        .await;
                }
                continue;
            }

            let action_id = ulid.generate_string();
            let (action_title, action_description) =
                Self::build_title_and_description_for_rule(result, playbook_id, rule_id);
            let mut record = ActionRecord {
                action_id: action_id.clone(),
                idempotency_key: idempotency_key.clone(),
                idempotency_hash: idempotency_hash.clone(),
                tenant_id: tenant_id.to_string(),
                event_id: event_id.clone(),
                event_name: event_name.clone(),
                playbook_id: playbook_id.to_string(),
                rule_id: rule_id.to_string(),
                task_type: TASK_TYPE.to_string(),
                status: STATUS_CREATED.to_string(),
                changed_object_type: changed_object_type.clone(),
                changed_object_id: changed_object_id.clone(),
                created_at: DateTime::now(),
                action_template_id: None,
                execution_mode: None,
                responsible_user: None,
                responsible_role: None,
                escalation_duration: None,
                escalation_duration_unit: None,
                require_document_upload: None,
                require_comment: None,
                require_approval_reference: None,
                action_title,
                action_description,
                assigned_to_type: None,
                assigned_to_id: None,
                assigned_to_name: None,
            };

            self.store.insert_action(&record).await?;
            info!(
                "action created: action_id={} playbook_id={} rule_id={}",
                action_id, playbook_id, rule_id
            );
            if let Some(store) = event_logger {
                let details = serde_json::json!({
                    "action_id": action_id,
                    "playbook_id": playbook_id,
                    "rule_id": rule_id,
                    "task_type": TASK_TYPE,
                });
                let _ = store
                    .append_event_log(
                        envelope,
                        "action_builder",
                        "INFO",
                        "action created",
                        Some(&details),
                    )
                    .await;
            }
            let routing = self
                .route_action(tenant_id, playbook_id, &action_id, None, None)
                .await;
            Self::apply_routing(&mut record, routing);
            created.push(record);
        }

        if Self::should_create_review_action(result)
            && created.is_empty()
            && result.evaluations.is_empty()
        {
            warn!("review decision with no evaluations; creating single action");
            let playbook_id = result
                .playbooks
                .first()
                .map(|p| p.playbook_id.as_str())
                .unwrap_or("unknown");
            let rule_id = "none".to_string();
            let idempotency_key = format!(
                "{}:{}:{}:{}:{}",
                tenant_id, object_type, object_id, TASK_TYPE, playbook_id
            );
            let idempotency_hash = compute_idempotency_hash(&idempotency_key);
            if let Some(existing) = self
                .store
                .find_by_idempotency_hash(&idempotency_hash)
                .await?
            {
                info!(
                    "action already exists for fallback idempotency_key (hash prefix); skipping publish/metric output: {}",
                    &existing.idempotency_hash[..existing.idempotency_hash.len().min(16)]
                );
            } else {
                let action_id = ulid.generate_string();
                let mut record = ActionRecord {
                    action_id: action_id.clone(),
                    idempotency_key: idempotency_key.clone(),
                    idempotency_hash: idempotency_hash.clone(),
                    tenant_id: tenant_id.to_string(),
                    event_id: event_id.clone(),
                    event_name: event_name.clone(),
                    playbook_id: playbook_id.to_string(),
                    rule_id: rule_id.clone(),
                    task_type: TASK_TYPE.to_string(),
                    status: STATUS_CREATED.to_string(),
                    changed_object_type: changed_object_type.clone(),
                    changed_object_id: changed_object_id.clone(),
                    created_at: DateTime::now(),
                    action_template_id: None,
                    execution_mode: None,
                    responsible_user: None,
                    responsible_role: None,
                    escalation_duration: None,
                    escalation_duration_unit: None,
                    require_document_upload: None,
                    require_comment: None,
                    require_approval_reference: None,
                    action_title: Some("Governance Review Required".to_string()),
                    action_description: Some(
                        "Decision was FAIL but no rule evaluations were recorded".to_string(),
                    ),
                    assigned_to_type: None,
                    assigned_to_id: None,
                    assigned_to_name: None,
                };
                self.store.insert_action(&record).await?;
                info!("action created (no evaluations): action_id={}", action_id);
                if let Some(store) = event_logger {
                    let details = serde_json::json!({
                        "action_id": action_id,
                        "playbook_id": playbook_id,
                        "rule_id": rule_id,
                        "task_type": TASK_TYPE,
                    });
                    let _ = store
                        .append_event_log(
                            envelope,
                            "action_builder",
                            "INFO",
                            "fallback action created for failed decision with no evaluations",
                            Some(&details),
                        )
                        .await;
                }
                let routing = self
                    .route_action(tenant_id, playbook_id, &action_id, None, None)
                    .await;
                Self::apply_routing(&mut record, routing);
                created.push(record);
            }
        }

        Ok(created)
    }
}
