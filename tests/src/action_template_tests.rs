//! Tests for ActionTemplate: template-driven action creation, trigger matching,
//! evidence/responsibility fields, and backward compatibility.

use async_trait::async_trait;
use domain::envelope::{CanonicalEnvelope, TadpoleHead};
use engine_core::dto::{
    decision::Decision,
    evaluation::RuleEvaluation,
    orchestration::{
        CodexPlaybookResult, CodexResult, CodexRuleResult, OrchestrationResult, PlaybookSummary,
    },
    rules::PlaybookAssignment,
};
use http_gateway::{
    ActionBuilder, ActionRecord, ActionStore, ActionTemplate, ActionTemplateListFilter,
    ActionTemplateStore, EscalationDurationUnit, EvidenceConfig, ExecutionMode,
    ResponsibilityConfig, TemplateStatus, TriggerConfig, TriggerEventType,
};
use mongodb::bson::DateTime;
use std::sync::Arc;
use tokio::sync::Mutex;

// ── Test fixtures ──

fn test_envelope() -> CanonicalEnvelope {
    CanonicalEnvelope {
        head: TadpoleHead {
            event_id: "01EVENT000000000000000001".to_string(),
            event_name: "InvoiceReceived".to_string(),
            event_category: Some("Transaction".to_string()),
            tenant_id: "tenant-a".to_string(),
            correlation_id: None,
            causation_id: None,
            occurred_at: None,
            originating_function: None,
            originating_application: None,
            environment: None,
            external_dependency_id: Some("ext-1".to_string()),
            changed_object_type: Some("Invoice".to_string()),
            changed_object_id: Some("inv-001".to_string()),
            change_kind: Some("created".to_string()),
        },
        body: serde_json::json!({}),
    }
}

fn make_template(
    template_id: &str,
    event_type: TriggerEventType,
    object_type: &str,
) -> ActionTemplate {
    ActionTemplate {
        template_id: template_id.to_string(),
        tenant_id: "tenant-a".to_string(),
        name: format!("Template {}", template_id),
        description: Some("Test template".to_string()),
        version: 1,
        status: TemplateStatus::Active,
        trigger: TriggerConfig {
            event_type,
            object_type: object_type.to_string(),
            execution_mode: ExecutionMode::Automatic,
        },
        responsibility: ResponsibilityConfig {
            responsible_user: Some("user-jane".to_string()),
            responsible_role: Some("Compliance Officer".to_string()),
            escalation_duration: Some(48),
            escalation_duration_unit: Some(EscalationDurationUnit::Hours),
        },
        evidence: EvidenceConfig {
            require_document_upload: true,
            require_comment: true,
            require_approval_reference: false,
        },
        associated_rule_ids: vec![],
        associated_playbook_ids: vec![],
    }
}

fn orchestration_result_with_decision(
    decision: Decision,
    playbook_id: &str,
    rule_id: &str,
) -> OrchestrationResult {
    let eval_decision = decision.clone();
    let evaluations = vec![RuleEvaluation {
        playbook_id: playbook_id.to_string(),
        rule_id: rule_id.to_string(),
        rule_name: None,
        object_type: None,
        order_seq: None,
        is_critical: true,
        priority: None,
        decision: eval_decision,
        reason_code: "test".to_string(),
        reason: "test reason".to_string(),
        checks: Vec::new(),
        duration_ms: 0,
        action_template_id: None,
    }];
    let playbooks = vec![PlaybookAssignment {
        playbook_id: playbook_id.to_string(),
        reason: "matched".to_string(),
    }];
    let codex = CodexResult {
        version_id: Some("v1".to_string()),
        playbooks: vec![CodexPlaybookResult {
            playbook_id: playbook_id.to_string(),
            decision: decision.clone(),
            reason: "test".to_string(),
            rules: vec![CodexRuleResult {
                rule_id: rule_id.to_string(),
                decision: decision.clone(),
                reason: "test".to_string(),
            }],
        }],
        decision: decision.clone(),
    };
    let playbook_summaries = vec![PlaybookSummary {
        playbook_id: playbook_id.to_string(),
        decision: decision.clone(),
        reason: "test".to_string(),
    }];
    OrchestrationResult {
        decision: decision.clone(),
        action_candidates: vec![],
        playbooks,
        matched_playbook: None,
        evaluations,
        codex,
        playbook_summaries,
        route_to_action_builder: decision.is_fail(),
    }
}

// ── Mock stores ──

struct MockActionStore {
    inserted: Arc<Mutex<Vec<ActionRecord>>>,
    existing_hashes: Arc<Mutex<Vec<String>>>,
}

impl MockActionStore {
    fn new() -> Self {
        Self {
            inserted: Arc::new(Mutex::new(Vec::new())),
            existing_hashes: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl ActionStore for MockActionStore {
    async fn find_by_idempotency_hash(
        &self,
        idempotency_hash: &str,
    ) -> Result<Option<ActionRecord>, Box<dyn std::error::Error + Send + Sync>> {
        let existing = self.existing_hashes.lock().await;
        if existing.iter().any(|h| h == idempotency_hash) {
            return Ok(Some(ActionRecord {
                action_id: "existing-01".to_string(),
                idempotency_key: "key".to_string(),
                idempotency_hash: idempotency_hash.to_string(),
                tenant_id: "t".to_string(),
                event_id: "e".to_string(),
                event_name: "e".to_string(),
                playbook_id: "p".to_string(),
                rule_id: "r".to_string(),
                task_type: "TASK".to_string(),
                status: "created".to_string(),
                changed_object_type: None,
                changed_object_id: None,
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
                action_title: None,
                action_description: None,
                assigned_to_type: None,
                assigned_to_id: None,
                assigned_to_name: None,
            }));
        }
        Ok(None)
    }

    async fn insert_action(
        &self,
        record: &ActionRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.inserted.lock().await.push(record.clone());
        Ok(())
    }

    async fn update_action_assignment(
        &self,
        _action_id: &str,
        _assigned_to_type: &str,
        _assigned_to_id: &str,
        _assigned_to_name: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

struct MockTemplateStore {
    templates: Arc<Mutex<Vec<ActionTemplate>>>,
}

impl MockTemplateStore {
    fn new(templates: Vec<ActionTemplate>) -> Self {
        Self {
            templates: Arc::new(Mutex::new(templates)),
        }
    }
}

#[async_trait]
impl ActionTemplateStore for MockTemplateStore {
    async fn list_templates(
        &self,
        filter: ActionTemplateListFilter,
        limit: i64,
    ) -> Result<Vec<ActionTemplate>, Box<dyn std::error::Error + Send + Sync>> {
        let all = self.templates.lock().await;
        let limit = limit.clamp(1, 200) as usize;
        let matched = all
            .iter()
            .filter(|t| {
                filter.tenant_id.as_ref().is_none_or(|v| t.tenant_id == *v)
                    && filter.status.as_ref().is_none_or(|v| t.status == *v)
                    && filter
                        .object_type
                        .as_ref()
                        .is_none_or(|v| t.trigger.object_type == *v)
                    && filter
                        .event_type
                        .as_ref()
                        .is_none_or(|v| t.trigger.event_type == *v)
            })
            .take(limit)
            .cloned()
            .collect();
        Ok(matched)
    }

    async fn find_templates_by_trigger(
        &self,
        tenant_id: &str,
        object_type: &str,
        event_type: &TriggerEventType,
    ) -> Result<Vec<ActionTemplate>, Box<dyn std::error::Error + Send + Sync>> {
        let all = self.templates.lock().await;
        let matched = all
            .iter()
            .filter(|t| {
                t.tenant_id == tenant_id
                    && t.trigger.object_type == object_type
                    && t.trigger.event_type == *event_type
                    && t.status == TemplateStatus::Active
            })
            .cloned()
            .collect();
        Ok(matched)
    }

    async fn find_template_by_id(
        &self,
        template_id: &str,
    ) -> Result<Option<ActionTemplate>, Box<dyn std::error::Error + Send + Sync>> {
        let all = self.templates.lock().await;
        Ok(all.iter().find(|t| t.template_id == template_id).cloned())
    }
}

// ── Tests ──

#[tokio::test]
async fn template_store_returns_empty_when_no_templates_match() {
    let store = MockTemplateStore::new(vec![]);
    let result = store
        .find_templates_by_trigger("tenant-a", "Invoice", &TriggerEventType::RuleFailed)
        .await
        .unwrap();
    assert!(result.is_empty());
}

#[tokio::test]
async fn template_store_returns_matching_template_for_fail_trigger() {
    let template = make_template("tmpl-1", TriggerEventType::RuleFailed, "Invoice");
    let store = MockTemplateStore::new(vec![template]);
    let result = store
        .find_templates_by_trigger("tenant-a", "Invoice", &TriggerEventType::RuleFailed)
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].template_id, "tmpl-1");
}

#[tokio::test]
async fn template_store_returns_matching_template_for_pass_trigger() {
    let template = make_template("tmpl-pass", TriggerEventType::RulePassed, "Invoice");
    let store = MockTemplateStore::new(vec![template]);
    let result = store
        .find_templates_by_trigger("tenant-a", "Invoice", &TriggerEventType::RulePassed)
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].template_id, "tmpl-pass");
}

#[tokio::test]
async fn template_store_filters_by_active_status() {
    let mut template = make_template("tmpl-draft", TriggerEventType::RuleFailed, "Invoice");
    template.status = TemplateStatus::Draft;
    let store = MockTemplateStore::new(vec![template]);
    let result = store
        .find_templates_by_trigger("tenant-a", "Invoice", &TriggerEventType::RuleFailed)
        .await
        .unwrap();
    assert!(result.is_empty());
}

#[tokio::test]
async fn template_store_filters_by_object_type() {
    let template = make_template("tmpl-contract", TriggerEventType::RuleFailed, "Contract");
    let store = MockTemplateStore::new(vec![template]);
    // Should not match Invoice
    let result = store
        .find_templates_by_trigger("tenant-a", "Invoice", &TriggerEventType::RuleFailed)
        .await
        .unwrap();
    assert!(result.is_empty());
}

#[tokio::test]
async fn action_builder_creates_action_with_template_fields() {
    let action_store = Arc::new(MockActionStore::new());
    let template = make_template("tmpl-1", TriggerEventType::RuleFailed, "Invoice");
    let template_store = Arc::new(MockTemplateStore::new(vec![template]));

    let builder = ActionBuilder::with_template_store(action_store.clone(), template_store);

    let envelope = test_envelope();
    let result = orchestration_result_with_decision(
        Decision::fail("test.fail", "rule failed"),
        "pb1",
        "rule1",
    );

    let created = builder
        .create_actions_from_templates(&envelope, &result)
        .await
        .unwrap();

    assert_eq!(created.len(), 1);
    let action = &created[0];
    assert_eq!(action.action_template_id.as_deref(), Some("tmpl-1"));
    assert_eq!(action.execution_mode.as_deref(), Some("Automatic"));
    assert_eq!(action.responsible_user.as_deref(), Some("user-jane"));
    assert_eq!(
        action.responsible_role.as_deref(),
        Some("Compliance Officer")
    );
    assert_eq!(action.escalation_duration, Some(48));
    assert_eq!(action.escalation_duration_unit.as_deref(), Some("Hours"));
    assert_eq!(action.require_document_upload, Some(true));
    assert_eq!(action.require_comment, Some(true));
    assert_eq!(action.require_approval_reference, Some(false));
    assert_eq!(action.tenant_id, "tenant-a");
    assert_eq!(action.playbook_id, "pb1");
    assert_eq!(action.task_type, "TASK_GOVERNANCE_REVIEW");
    assert_eq!(action.status, "created");

    let inserted = action_store.inserted.lock().await;
    assert_eq!(inserted.len(), 1);
}

#[tokio::test]
async fn action_builder_creates_action_on_pass_when_template_matches() {
    let action_store = Arc::new(MockActionStore::new());
    let template = make_template("tmpl-pass", TriggerEventType::RulePassed, "Invoice");
    let template_store = Arc::new(MockTemplateStore::new(vec![template]));

    let builder = ActionBuilder::with_template_store(action_store.clone(), template_store);

    let envelope = test_envelope();
    let result = orchestration_result_with_decision(
        Decision::pass("test.pass", "all passed"),
        "pb1",
        "rule1",
    );

    let created = builder
        .create_actions_from_templates(&envelope, &result)
        .await
        .unwrap();

    assert_eq!(created.len(), 1);
    assert_eq!(created[0].action_template_id.as_deref(), Some("tmpl-pass"));
    assert_eq!(created[0].playbook_id, "pb1");
}

#[tokio::test]
async fn action_builder_creates_action_on_inconclusive_when_template_matches() {
    let action_store = Arc::new(MockActionStore::new());
    let template = make_template("tmpl-inc", TriggerEventType::RuleInconclusive, "Invoice");
    let template_store = Arc::new(MockTemplateStore::new(vec![template]));

    let builder = ActionBuilder::with_template_store(action_store.clone(), template_store);

    let envelope = test_envelope();
    let result = orchestration_result_with_decision(
        Decision::inconclusive("test.inc", "inconclusive"),
        "pb1",
        "rule1",
    );

    let created = builder
        .create_actions_from_templates(&envelope, &result)
        .await
        .unwrap();

    assert_eq!(created.len(), 1);
    assert_eq!(created[0].action_template_id.as_deref(), Some("tmpl-inc"));
}

#[tokio::test]
async fn action_builder_returns_empty_when_no_templates_match() {
    let action_store = Arc::new(MockActionStore::new());
    // Template for Contract, but envelope has Invoice
    let template = make_template("tmpl-contract", TriggerEventType::RuleFailed, "Contract");
    let template_store = Arc::new(MockTemplateStore::new(vec![template]));

    let builder = ActionBuilder::with_template_store(action_store.clone(), template_store);

    let envelope = test_envelope();
    let result =
        orchestration_result_with_decision(Decision::fail("test.fail", "failed"), "pb1", "rule1");

    let created = builder
        .create_actions_from_templates(&envelope, &result)
        .await
        .unwrap();

    assert!(created.is_empty());
    assert!(action_store.inserted.lock().await.is_empty());
}

#[tokio::test]
async fn action_builder_returns_empty_when_no_template_store() {
    let action_store = Arc::new(MockActionStore::new());
    let builder = ActionBuilder::new(action_store.clone());

    let envelope = test_envelope();
    let result =
        orchestration_result_with_decision(Decision::fail("test.fail", "failed"), "pb1", "rule1");

    let created = builder
        .create_actions_from_templates(&envelope, &result)
        .await
        .unwrap();

    assert!(created.is_empty());
}

#[tokio::test]
async fn action_builder_legacy_still_works_without_template_store() {
    let action_store = Arc::new(MockActionStore::new());
    let builder = ActionBuilder::new(action_store.clone());

    let envelope = test_envelope();
    let result =
        orchestration_result_with_decision(Decision::fail("test.fail", "failed"), "pb1", "rule1");

    let created = builder
        .create_actions_for_failure(&envelope, &result, None)
        .await
        .unwrap();

    assert_eq!(created.len(), 1);
    assert!(created[0].action_template_id.is_none());
    assert!(created[0].execution_mode.is_none());
    assert!(created[0].responsible_user.is_none());
}

#[tokio::test]
async fn action_builder_reuses_template_action_when_idempotency_hash_exists() {
    let action_store = Arc::new(MockActionStore::new());
    let template = make_template("tmpl-1", TriggerEventType::RuleFailed, "Invoice");
    let template_store = Arc::new(MockTemplateStore::new(vec![template]));

    let builder = ActionBuilder::with_template_store(action_store.clone(), template_store);

    let envelope = test_envelope();
    let result =
        orchestration_result_with_decision(Decision::fail("test.fail", "failed"), "pb1", "rule1");

    // First call creates the action.
    let created1 = builder
        .create_actions_from_templates(&envelope, &result)
        .await
        .unwrap();
    assert_eq!(created1.len(), 1);

    // Simulate the hash existing by adding it to the mock store.
    action_store
        .existing_hashes
        .lock()
        .await
        .push(created1[0].idempotency_hash.clone());

    // Second call should skip output due to idempotency so downstream feeds are not republished.
    let created2 = builder
        .create_actions_from_templates(&envelope, &result)
        .await
        .unwrap();
    assert!(created2.is_empty());
}

#[tokio::test]
async fn action_builder_creates_actions_for_multiple_matching_templates() {
    let action_store = Arc::new(MockActionStore::new());
    let template1 = make_template("tmpl-1", TriggerEventType::RuleFailed, "Invoice");
    let mut template2 = make_template("tmpl-2", TriggerEventType::RuleFailed, "Invoice");
    template2.responsibility.responsible_role = Some("Procurement Lead".to_string());
    template2.evidence.require_document_upload = false;

    let template_store = Arc::new(MockTemplateStore::new(vec![template1, template2]));

    let builder = ActionBuilder::with_template_store(action_store.clone(), template_store);

    let envelope = test_envelope();
    let result =
        orchestration_result_with_decision(Decision::fail("test.fail", "failed"), "pb1", "rule1");

    let created = builder
        .create_actions_from_templates(&envelope, &result)
        .await
        .unwrap();

    // Two templates matched, so two actions should be created.
    assert_eq!(created.len(), 2);
    let template_ids: Vec<&str> = created
        .iter()
        .map(|a| a.action_template_id.as_deref().unwrap())
        .collect();
    assert!(template_ids.contains(&"tmpl-1"));
    assert!(template_ids.contains(&"tmpl-2"));

    // Each action should have different responsibility from its template.
    let tmpl2_action = created
        .iter()
        .find(|a| a.action_template_id.as_deref() == Some("tmpl-2"))
        .unwrap();
    assert_eq!(
        tmpl2_action.responsible_role.as_deref(),
        Some("Procurement Lead")
    );
    assert_eq!(tmpl2_action.require_document_upload, Some(false));
}

#[tokio::test]
async fn action_builder_filters_by_associated_playbook_ids() {
    let action_store = Arc::new(MockActionStore::new());
    let mut template = make_template("tmpl-filtered", TriggerEventType::RuleFailed, "Invoice");
    // Only applies to playbook "pb-special", not "pb1".
    template.associated_playbook_ids = vec!["pb-special".to_string()];
    let template_store = Arc::new(MockTemplateStore::new(vec![template]));

    let builder = ActionBuilder::with_template_store(action_store.clone(), template_store);

    let envelope = test_envelope();
    let result =
        orchestration_result_with_decision(Decision::fail("test.fail", "failed"), "pb1", "rule1");

    let created = builder
        .create_actions_from_templates(&envelope, &result)
        .await
        .unwrap();

    // Template's associated_playbook_ids doesn't include "pb1", so no action.
    assert!(created.is_empty());
}

#[tokio::test]
async fn action_builder_template_with_matching_associated_playbook() {
    let action_store = Arc::new(MockActionStore::new());
    let mut template = make_template("tmpl-match", TriggerEventType::RuleFailed, "Invoice");
    template.associated_playbook_ids = vec!["pb1".to_string()];
    let template_store = Arc::new(MockTemplateStore::new(vec![template]));

    let builder = ActionBuilder::with_template_store(action_store.clone(), template_store);

    let envelope = test_envelope();
    let result =
        orchestration_result_with_decision(Decision::fail("test.fail", "failed"), "pb1", "rule1");

    let created = builder
        .create_actions_from_templates(&envelope, &result)
        .await
        .unwrap();

    assert_eq!(created.len(), 1);
    assert_eq!(created[0].playbook_id, "pb1");
}

// ── Rule-level binding tests ──

/// Helper: build an OrchestrationResult with rule-level action_template_id bindings.
fn orchestration_result_with_rule_bindings(
    evals: Vec<RuleEvaluation>,
    playbook_id: &str,
) -> OrchestrationResult {
    let decision = Decision::fail("test.fail", "rule failed");
    let playbooks = vec![PlaybookAssignment {
        playbook_id: playbook_id.to_string(),
        reason: "matched".to_string(),
    }];
    let codex = CodexResult {
        version_id: Some("v1".to_string()),
        playbooks: vec![CodexPlaybookResult {
            playbook_id: playbook_id.to_string(),
            decision: decision.clone(),
            reason: "fail".to_string(),
            rules: vec![],
        }],
        decision: decision.clone(),
    };
    let playbook_summaries = vec![PlaybookSummary {
        playbook_id: playbook_id.to_string(),
        decision: decision.clone(),
        reason: "fail".to_string(),
    }];
    OrchestrationResult {
        decision,
        action_candidates: vec![],
        playbooks,
        matched_playbook: None,
        evaluations: evals,
        codex,
        playbook_summaries,
        route_to_action_builder: true,
    }
}

fn failing_eval_with_template(
    playbook_id: &str,
    rule_id: &str,
    object_type: Option<&str>,
    template_id: Option<&str>,
) -> RuleEvaluation {
    RuleEvaluation {
        playbook_id: playbook_id.to_string(),
        rule_id: rule_id.to_string(),
        rule_name: None,
        object_type: object_type.map(String::from),
        order_seq: None,
        is_critical: true,
        priority: None,
        decision: Decision::fail("test.fail", "rule failed"),
        reason_code: "test.fail".to_string(),
        reason: "rule failed".to_string(),
        checks: Vec::new(),
        duration_ms: 0,
        action_template_id: template_id.map(String::from),
    }
}

fn inconclusive_eval_with_template(
    playbook_id: &str,
    rule_id: &str,
    object_type: Option<&str>,
    template_id: Option<&str>,
) -> RuleEvaluation {
    RuleEvaluation {
        playbook_id: playbook_id.to_string(),
        rule_id: rule_id.to_string(),
        rule_name: None,
        object_type: object_type.map(String::from),
        order_seq: None,
        is_critical: true,
        priority: None,
        decision: Decision::inconclusive("test.inconclusive", "rule inconclusive"),
        reason_code: "test.inconclusive".to_string(),
        reason: "rule inconclusive".to_string(),
        checks: Vec::new(),
        duration_ms: 0,
        action_template_id: template_id.map(String::from),
    }
}

#[tokio::test]
async fn rule_binding_creates_action_per_failed_rule_with_template() {
    let action_store = Arc::new(MockActionStore::new());
    let tmpl1 = make_template("tmpl_notify_desk", TriggerEventType::RuleFailed, "SLA");
    let mut tmpl2 = make_template("tmpl_request_docs", TriggerEventType::RuleFailed, "SLA");
    tmpl2.responsibility.responsible_role = Some("Assigned Agent".to_string());
    let template_store = Arc::new(MockTemplateStore::new(vec![tmpl1, tmpl2]));

    let builder = ActionBuilder::with_template_store(action_store.clone(), template_store);

    let envelope = test_envelope();
    let evals = vec![
        failing_eval_with_template(
            "pb1",
            "rule.response_time",
            Some("SLA"),
            Some("tmpl_notify_desk"),
        ),
        failing_eval_with_template(
            "pb1",
            "rule.documentation",
            Some("SLA"),
            Some("tmpl_request_docs"),
        ),
    ];
    let result = orchestration_result_with_rule_bindings(evals, "pb1");

    let created = builder
        .create_actions_from_rule_bindings(&envelope, &result)
        .await
        .unwrap();

    assert_eq!(created.len(), 2);
    let template_ids: Vec<&str> = created
        .iter()
        .map(|a| a.action_template_id.as_deref().unwrap())
        .collect();
    assert!(template_ids.contains(&"tmpl_notify_desk"));
    assert!(template_ids.contains(&"tmpl_request_docs"));

    // Each action should reference its specific rule_id.
    let rule_ids: Vec<&str> = created.iter().map(|a| a.rule_id.as_str()).collect();
    assert!(rule_ids.contains(&"rule.response_time"));
    assert!(rule_ids.contains(&"rule.documentation"));
}

#[tokio::test]
async fn rule_binding_creates_action_for_inconclusive_rule_with_template() {
    let action_store = Arc::new(MockActionStore::new());
    let tmpl = make_template(
        "tmpl_review_missing_data",
        TriggerEventType::RuleFailed,
        "SLA",
    );
    let template_store = Arc::new(MockTemplateStore::new(vec![tmpl]));

    let builder = ActionBuilder::with_template_store(action_store.clone(), template_store);

    let envelope = test_envelope();
    let evals = vec![inconclusive_eval_with_template(
        "pb1",
        "rule.missing_data",
        Some("SLA"),
        Some("tmpl_review_missing_data"),
    )];
    let result = orchestration_result_with_rule_bindings(evals, "pb1");

    let created = builder
        .create_actions_from_rule_bindings(&envelope, &result)
        .await
        .unwrap();

    assert_eq!(created.len(), 1);
    assert_eq!(created[0].rule_id, "rule.missing_data");
    assert_eq!(
        created[0].action_template_id.as_deref(),
        Some("tmpl_review_missing_data")
    );
}

#[tokio::test]
async fn rule_binding_skips_passed_rules() {
    let action_store = Arc::new(MockActionStore::new());
    let tmpl = make_template("tmpl_notify_desk", TriggerEventType::RuleFailed, "SLA");
    let template_store = Arc::new(MockTemplateStore::new(vec![tmpl]));

    let builder = ActionBuilder::with_template_store(action_store.clone(), template_store);

    let envelope = test_envelope();
    // One rule failed with template, one passed with template — only the failed one should create action.
    let evals = vec![
        failing_eval_with_template(
            "pb1",
            "rule.response_time",
            Some("SLA"),
            Some("tmpl_notify_desk"),
        ),
        RuleEvaluation {
            playbook_id: "pb1".to_string(),
            rule_id: "rule.resolution_time".to_string(),
            rule_name: None,
            object_type: Some("SLA".to_string()),
            order_seq: None,
            is_critical: true,
            priority: None,
            decision: Decision::pass("test.pass", "ok"),
            reason_code: "test.pass".to_string(),
            reason: "ok".to_string(),
            checks: Vec::new(),
            duration_ms: 0,
            action_template_id: Some("tmpl_escalate_ops".to_string()),
        },
    ];
    let result = orchestration_result_with_rule_bindings(evals, "pb1");

    let created = builder
        .create_actions_from_rule_bindings(&envelope, &result)
        .await
        .unwrap();

    assert_eq!(created.len(), 1);
    assert_eq!(created[0].rule_id, "rule.response_time");
    assert_eq!(
        created[0].action_template_id.as_deref(),
        Some("tmpl_notify_desk")
    );
}

#[tokio::test]
async fn rule_binding_returns_empty_when_no_bindings() {
    let action_store = Arc::new(MockActionStore::new());
    let template_store = Arc::new(MockTemplateStore::new(vec![]));
    let builder = ActionBuilder::with_template_store(action_store.clone(), template_store);

    let envelope = test_envelope();
    // Failed rules without action_template_id.
    let evals = vec![failing_eval_with_template("pb1", "rule1", None, None)];
    let result = orchestration_result_with_rule_bindings(evals, "pb1");

    let created = builder
        .create_actions_from_rule_bindings(&envelope, &result)
        .await
        .unwrap();

    assert!(created.is_empty());
}

#[tokio::test]
async fn rule_binding_suppresses_child_when_parent_failed() {
    let action_store = Arc::new(MockActionStore::new());
    let tmpl_parent = make_template("tmpl_parent", TriggerEventType::RuleFailed, "SLA");
    let tmpl_child = make_template("tmpl_child", TriggerEventType::RuleFailed, "SLA");
    let template_store = Arc::new(MockTemplateStore::new(vec![tmpl_parent, tmpl_child]));

    let builder = ActionBuilder::with_template_store(action_store.clone(), template_store);

    let envelope = test_envelope();
    // Parent "sla" failed AND child "sla.response_time" also failed.
    // Child should be suppressed because parent already failed.
    let evals = vec![
        failing_eval_with_template("pb1", "rule.sla_overall", Some("sla"), Some("tmpl_parent")),
        failing_eval_with_template(
            "pb1",
            "rule.sla_response",
            Some("sla.response_time"),
            Some("tmpl_child"),
        ),
    ];
    let result = orchestration_result_with_rule_bindings(evals, "pb1");

    let created = builder
        .create_actions_from_rule_bindings(&envelope, &result)
        .await
        .unwrap();

    // Only the parent action should be created, child is suppressed.
    assert_eq!(created.len(), 1);
    assert_eq!(
        created[0].action_template_id.as_deref(),
        Some("tmpl_parent")
    );
    assert_eq!(created[0].rule_id, "rule.sla_overall");
}

#[tokio::test]
async fn rule_binding_keeps_independent_children() {
    let action_store = Arc::new(MockActionStore::new());
    let tmpl1 = make_template("tmpl_response", TriggerEventType::RuleFailed, "SLA");
    let tmpl2 = make_template("tmpl_docs", TriggerEventType::RuleFailed, "SLA");
    let template_store = Arc::new(MockTemplateStore::new(vec![tmpl1, tmpl2]));

    let builder = ActionBuilder::with_template_store(action_store.clone(), template_store);

    let envelope = test_envelope();
    // Two child-level rules failed, but parent "sla" did NOT fail.
    // Both should create actions since they are independent.
    let evals = vec![
        failing_eval_with_template(
            "pb1",
            "rule.response",
            Some("sla.response_time"),
            Some("tmpl_response"),
        ),
        failing_eval_with_template(
            "pb1",
            "rule.docs",
            Some("sla.documentation"),
            Some("tmpl_docs"),
        ),
    ];
    let result = orchestration_result_with_rule_bindings(evals, "pb1");

    let created = builder
        .create_actions_from_rule_bindings(&envelope, &result)
        .await
        .unwrap();

    assert_eq!(created.len(), 2);
}

#[tokio::test]
async fn rule_binding_skips_when_template_not_found() {
    let action_store = Arc::new(MockActionStore::new());
    // Template store has no templates — the referenced template_id won't be found.
    let template_store = Arc::new(MockTemplateStore::new(vec![]));
    let builder = ActionBuilder::with_template_store(action_store.clone(), template_store);

    let envelope = test_envelope();
    let evals = vec![failing_eval_with_template(
        "pb1",
        "rule1",
        Some("SLA"),
        Some("tmpl_nonexistent"),
    )];
    let result = orchestration_result_with_rule_bindings(evals, "pb1");

    let created = builder
        .create_actions_from_rule_bindings(&envelope, &result)
        .await
        .unwrap();

    assert!(created.is_empty());
    assert!(action_store.inserted.lock().await.is_empty());
}

#[tokio::test]
async fn rule_binding_deduplicates_by_idempotency() {
    let action_store = Arc::new(MockActionStore::new());
    let tmpl = make_template("tmpl_notify", TriggerEventType::RuleFailed, "SLA");
    let template_store = Arc::new(MockTemplateStore::new(vec![tmpl]));

    let builder = ActionBuilder::with_template_store(action_store.clone(), template_store);

    let envelope = test_envelope();
    let evals = vec![failing_eval_with_template(
        "pb1",
        "rule1",
        Some("SLA"),
        Some("tmpl_notify"),
    )];
    let result = orchestration_result_with_rule_bindings(evals, "pb1");

    // First call creates the action.
    let created1 = builder
        .create_actions_from_rule_bindings(&envelope, &result)
        .await
        .unwrap();
    assert_eq!(created1.len(), 1);

    // Add the hash to simulate existing record.
    action_store
        .existing_hashes
        .lock()
        .await
        .push(created1[0].idempotency_hash.clone());

    // Second call should skip output due to idempotency so downstream feeds are not republished.
    let created2 = builder
        .create_actions_from_rule_bindings(&envelope, &result)
        .await
        .unwrap();
    assert!(created2.is_empty());
}
