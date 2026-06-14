//! Tests for ActionBuilder: create_actions_for_failure, idempotency, one per failed rule.

use async_trait::async_trait;
use domain::envelope::{CanonicalEnvelope, TadpoleHead};
use engine_core::dto::{
    decision::Decision,
    evaluation::{ConditionCheck, RuleEvaluation},
    orchestration::{
        CodexPlaybookResult, CodexResult, CodexRuleResult, OrchestrationResult, PlaybookSummary,
    },
    rules::{PlaybookAssignment, RuleLogic},
};
use http_gateway::mongo_store::{
    DbPlaybook, DbPlaybookRuleRef, DbPlaybookTrigger, DbRule, DbRuleObject,
};
use http_gateway::{ActionBuilder, ActionRecord, ActionStore};
use http_gateway::{
    ActionTemplate, ActionTemplateListFilter, ActionTemplateStore, AssigneeType,
    EscalationDurationUnit, EvidenceConfig, ExecutionMode, IntakeStore, ResponsibilityConfig,
    RouteRequest, RouteResult, TemplateStatus, TriggerConfig, TriggerEventType, WorkRouter,
};
use mongodb::bson::DateTime;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;

fn fail_envelope() -> CanonicalEnvelope {
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

fn minimal_orchestration_result_fail(playbook_id: &str, rule_id: &str) -> OrchestrationResult {
    let decision = Decision::fail("test.fail", "rule failed");
    let evaluations = vec![RuleEvaluation {
        playbook_id: playbook_id.to_string(),
        rule_id: rule_id.to_string(),
        rule_name: None,
        object_type: None,
        order_seq: None,
        is_critical: true,
        priority: None,
        decision: decision.clone(),
        reason_code: "rule.expr_fail".to_string(),
        reason: "expr evaluated false".to_string(),
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
            reason: "fail".to_string(),
            rules: vec![CodexRuleResult {
                rule_id: rule_id.to_string(),
                decision: decision.clone(),
                reason: "fail".to_string(),
            }],
        }],
        decision: decision.clone(),
    };
    let playbook_summaries = vec![PlaybookSummary {
        playbook_id: playbook_id.to_string(),
        decision: decision.clone(),
        reason: "fail".to_string(),
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

fn failing_rule_evaluation(playbook_id: &str, rule_id: &str, reason: &str) -> RuleEvaluation {
    RuleEvaluation {
        playbook_id: playbook_id.to_string(),
        rule_id: rule_id.to_string(),
        rule_name: None,
        object_type: None,
        order_seq: None,
        is_critical: true,
        priority: None,
        decision: Decision::fail("fail", "rule failed"),
        reason_code: "fail".to_string(),
        reason: reason.to_string(),
        checks: vec![ConditionCheck {
            object_key: "status".to_string(),
            operator: "eq".to_string(),
            expected: None,
            actual: None,
            status: "FAIL".to_string(),
            reason: reason.to_string(),
        }],
        duration_ms: 0,
        action_template_id: None,
    }
}

fn inconclusive_rule_evaluation(playbook_id: &str, rule_id: &str, reason: &str) -> RuleEvaluation {
    RuleEvaluation {
        playbook_id: playbook_id.to_string(),
        rule_id: rule_id.to_string(),
        rule_name: None,
        object_type: None,
        order_seq: None,
        is_critical: true,
        priority: None,
        decision: Decision::inconclusive("inconclusive", "rule inconclusive"),
        reason_code: "inconclusive".to_string(),
        reason: reason.to_string(),
        checks: vec![ConditionCheck {
            object_key: "status".to_string(),
            operator: "eq".to_string(),
            expected: None,
            actual: None,
            status: "INCONCLUSIVE".to_string(),
            reason: reason.to_string(),
        }],
        duration_ms: 0,
        action_template_id: None,
    }
}

fn template(template_id: &str) -> ActionTemplate {
    ActionTemplate {
        template_id: template_id.to_string(),
        tenant_id: "tenant-a".to_string(),
        name: "Review template".to_string(),
        description: Some("Review failed rule".to_string()),
        version: 1,
        status: TemplateStatus::Active,
        trigger: TriggerConfig {
            event_type: TriggerEventType::RuleFailed,
            object_type: "Invoice".to_string(),
            execution_mode: ExecutionMode::ApprovalRequired,
        },
        responsibility: ResponsibilityConfig {
            responsible_user: Some("user-123".to_string()),
            responsible_role: Some("governance-reviewer".to_string()),
            escalation_duration: Some(2),
            escalation_duration_unit: Some(EscalationDurationUnit::Days),
        },
        evidence: EvidenceConfig {
            require_document_upload: true,
            require_comment: true,
            require_approval_reference: false,
        },
        associated_rule_ids: Vec::new(),
        associated_playbook_ids: Vec::new(),
    }
}

/// Mock store that records inserts and optionally returns existing by hash.
struct MockActionStore {
    inserted: Arc<Mutex<Vec<ActionRecord>>>,
    existing_hashes: Arc<Mutex<Vec<String>>>,
    assignments: Arc<Mutex<Vec<(String, String, String, Option<String>)>>>,
}

impl MockActionStore {
    fn new() -> Self {
        Self {
            inserted: Arc::new(Mutex::new(Vec::new())),
            existing_hashes: Arc::new(Mutex::new(Vec::new())),
            assignments: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn with_existing_hash(hash: &str) -> Self {
        let store = Self::new();
        store.existing_hashes.lock().await.push(hash.to_string());
        store
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
        action_id: &str,
        assigned_to_type: &str,
        assigned_to_id: &str,
        assigned_to_name: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.assignments.lock().await.push((
            action_id.to_string(),
            assigned_to_type.to_string(),
            assigned_to_id.to_string(),
            assigned_to_name.map(String::from),
        ));
        Ok(())
    }
}

struct MockActionTemplateStore {
    templates: Vec<ActionTemplate>,
}

#[async_trait]
impl ActionTemplateStore for MockActionTemplateStore {
    async fn list_templates(
        &self,
        _filter: ActionTemplateListFilter,
        _limit: i64,
    ) -> Result<Vec<ActionTemplate>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.templates.clone())
    }

    async fn find_templates_by_trigger(
        &self,
        tenant_id: &str,
        object_type: &str,
        event_type: &TriggerEventType,
    ) -> Result<Vec<ActionTemplate>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self
            .templates
            .iter()
            .filter(|template| {
                template.tenant_id == tenant_id
                    && template.trigger.object_type == object_type
                    && &template.trigger.event_type == event_type
                    && template.status == TemplateStatus::Active
            })
            .cloned()
            .collect())
    }

    async fn find_template_by_id(
        &self,
        template_id: &str,
    ) -> Result<Option<ActionTemplate>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self
            .templates
            .iter()
            .find(|template| template.template_id == template_id)
            .cloned())
    }
}

struct MockWorkRouter {
    route: RouteResult,
    requests: Arc<Mutex<Vec<RouteRequest>>>,
}

impl MockWorkRouter {
    fn team_route() -> Self {
        Self {
            route: RouteResult {
                assignee_type: AssigneeType::Team,
                assignee_id: "team-risk".to_string(),
                display_name: Some("Risk Review".to_string()),
                notification_channels: Vec::new(),
                used_fallback: false,
            },
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl WorkRouter for MockWorkRouter {
    async fn resolve(
        &self,
        request: &RouteRequest,
    ) -> Result<RouteResult, Box<dyn std::error::Error + Send + Sync>> {
        self.requests.lock().await.push(request.clone());
        Ok(self.route.clone())
    }
}

#[derive(Default)]
struct MockIntakeStore {
    event_logs: Arc<Mutex<Vec<(String, String, String, Option<serde_json::Value>)>>>,
}

#[async_trait]
impl IntakeStore for MockIntakeStore {
    async fn record_intake(
        &self,
        _envelope: &CanonicalEnvelope,
        _status: &str,
        _errors: Option<&Vec<String>>,
        _response: Option<&serde_json::Value>,
        _error_message: Option<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn list_recent_intake(
        &self,
        _limit: i64,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Vec::new())
    }

    async fn append_event_log(
        &self,
        _envelope: &CanonicalEnvelope,
        stage: &str,
        level: &str,
        message: &str,
        details: Option<&serde_json::Value>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.event_logs.lock().await.push((
            stage.to_string(),
            level.to_string(),
            message.to_string(),
            details.cloned(),
        ));
        Ok(())
    }

    async fn list_event_logs_by_event_id(
        &self,
        _event_id: &str,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Vec::new())
    }

    async fn find_matched_playbook(
        &self,
        _changed_object_type: &str,
        _change_kind: &str,
    ) -> Result<Option<DbPlaybook>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(None)
    }

    async fn find_active_rules(
        &self,
        _rule_ids: &[String],
    ) -> Result<Vec<DbRule>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Vec::new())
    }

    async fn find_playbooks_by_ids(
        &self,
        playbook_ids: &[String],
    ) -> Result<Vec<DbPlaybook>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(playbook_ids
            .iter()
            .map(|id| DbPlaybook {
                id: id.clone(),
                name: None,
                version: None,
                execution_mode: None,
                trigger: DbPlaybookTrigger {
                    object_type: "Invoice".to_string(),
                    change_kind: vec!["created".to_string()],
                },
                rules: vec![DbPlaybookRuleRef {
                    order_seq: 1,
                    rule_id: "rule.check".to_string(),
                    is_critical: true,
                    action_template_id: None,
                }],
                status: Some("active".to_string()),
            })
            .collect())
    }

    async fn find_rules_by_ids(
        &self,
        rule_ids: &[String],
    ) -> Result<Vec<DbRule>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(rule_ids
            .iter()
            .map(|id| DbRule {
                id: id.clone(),
                name: None,
                object: DbRuleObject {
                    object_type: "Invoice".to_string(),
                },
                conditions: Vec::new(),
                logic: RuleLogic::All,
                status: Some("active".to_string()),
            })
            .collect())
    }
}

#[tokio::test]
async fn action_builder_returns_empty_when_decision_is_pass() {
    let store = Arc::new(MockActionStore::new());
    let builder = ActionBuilder::new(store.clone());

    let envelope = fail_envelope();
    let mut result = minimal_orchestration_result_fail("pb1", "rule1");
    result.decision = Decision::pass("pass", "all passed");

    let created = builder
        .create_actions_for_failure(&envelope, &result, None)
        .await
        .unwrap();

    assert!(created.is_empty());
    assert!(store.inserted.lock().await.is_empty());
}

#[tokio::test]
async fn action_builder_creates_one_action_when_one_playbook_fails() {
    let store = Arc::new(MockActionStore::new());
    let builder = ActionBuilder::new(store.clone());

    let envelope = fail_envelope();
    let result = minimal_orchestration_result_fail("playbook.invoice_fail", "rule.check");

    let created = builder
        .create_actions_for_failure(&envelope, &result, None)
        .await
        .unwrap();

    assert_eq!(created.len(), 1);
    assert_eq!(created[0].playbook_id, "playbook.invoice_fail");
    assert_eq!(created[0].rule_id, "rule.check");
    assert_eq!(created[0].tenant_id, "tenant-a");
    assert_eq!(created[0].task_type, "TASK_GOVERNANCE_REVIEW");
    assert_eq!(created[0].status, "created");
    assert!(created[0].idempotency_key.contains("tenant-a"));
    assert!(created[0].idempotency_key.contains("Invoice"));
    assert!(created[0].idempotency_key.contains("inv-001"));
    assert!(created[0].idempotency_key.contains("playbook.invoice_fail"));

    let inserted = store.inserted.lock().await;
    assert_eq!(inserted.len(), 1);
}

#[tokio::test]
async fn action_builder_skips_existing_when_idempotency_hash_exists() {
    let envelope = fail_envelope();
    let result = minimal_orchestration_result_fail("pb1", "r1");
    let idempotency_key = "tenant-a:Invoice:inv-001:TASK_GOVERNANCE_REVIEW:pb1:r1";
    let hash = domain::envelope::compute_idempotency_hash(idempotency_key);

    let store = Arc::new(MockActionStore::with_existing_hash(&hash).await);
    let builder = ActionBuilder::new(store.clone());

    let created = builder
        .create_actions_for_failure(&envelope, &result, None)
        .await
        .unwrap();

    assert!(created.is_empty());
    assert!(store.inserted.lock().await.is_empty());
}

#[tokio::test]
async fn action_builder_creates_one_per_failing_rule() {
    let store = Arc::new(MockActionStore::new());
    let builder = ActionBuilder::new(store.clone());

    let envelope = fail_envelope();
    let decision = Decision::fail("fail", "rule failed");
    let evaluations = vec![
        failing_rule_evaluation("playbook.a", "rule.a", "a"),
        failing_rule_evaluation("playbook.b", "rule.b", "b"),
    ];
    let playbooks = vec![
        PlaybookAssignment {
            playbook_id: "playbook.a".to_string(),
            reason: "a".to_string(),
        },
        PlaybookAssignment {
            playbook_id: "playbook.b".to_string(),
            reason: "b".to_string(),
        },
    ];
    let codex = CodexResult {
        version_id: Some("v1".to_string()),
        playbooks: vec![
            CodexPlaybookResult {
                playbook_id: "playbook.a".to_string(),
                decision: decision.clone(),
                reason: "a".to_string(),
                rules: vec![],
            },
            CodexPlaybookResult {
                playbook_id: "playbook.b".to_string(),
                decision: decision.clone(),
                reason: "b".to_string(),
                rules: vec![],
            },
        ],
        decision: decision.clone(),
    };
    let playbook_summaries = vec![
        PlaybookSummary {
            playbook_id: "playbook.a".to_string(),
            decision: decision.clone(),
            reason: "a".to_string(),
        },
        PlaybookSummary {
            playbook_id: "playbook.b".to_string(),
            decision: decision.clone(),
            reason: "b".to_string(),
        },
    ];
    let result = OrchestrationResult {
        decision: decision.clone(),
        action_candidates: vec![],
        playbooks,
        matched_playbook: None,
        evaluations,
        codex,
        playbook_summaries,
        route_to_action_builder: true,
    };

    let created = builder
        .create_actions_for_failure(&envelope, &result, None)
        .await
        .unwrap();

    assert_eq!(created.len(), 2);
    let action_keys: Vec<(&str, &str)> = created
        .iter()
        .map(|a| (a.playbook_id.as_str(), a.rule_id.as_str()))
        .collect();
    assert!(action_keys.contains(&("playbook.a", "rule.a")));
    assert!(action_keys.contains(&("playbook.b", "rule.b")));

    let inserted = store.inserted.lock().await;
    assert_eq!(inserted.len(), 2);
}

#[tokio::test]
async fn action_builder_creates_one_per_failed_rule_in_same_playbook() {
    let store = Arc::new(MockActionStore::new());
    let builder = ActionBuilder::new(store.clone());

    let envelope = fail_envelope();
    let decision = Decision::fail("fail", "rules failed");
    let evaluations = vec![
        failing_rule_evaluation("playbook.a", "rule.a", "a"),
        failing_rule_evaluation("playbook.a", "rule.b", "b"),
    ];
    let result = OrchestrationResult {
        decision: decision.clone(),
        action_candidates: vec![],
        playbooks: vec![PlaybookAssignment {
            playbook_id: "playbook.a".to_string(),
            reason: "matched".to_string(),
        }],
        matched_playbook: None,
        evaluations,
        codex: CodexResult {
            version_id: Some("v1".to_string()),
            playbooks: vec![],
            decision: decision.clone(),
        },
        playbook_summaries: vec![PlaybookSummary {
            playbook_id: "playbook.a".to_string(),
            decision,
            reason: "fail".to_string(),
        }],
        route_to_action_builder: true,
    };

    let created = builder
        .create_actions_for_failure(&envelope, &result, None)
        .await
        .unwrap();

    assert_eq!(created.len(), 2);
    let rule_ids: Vec<&str> = created
        .iter()
        .map(|action| action.rule_id.as_str())
        .collect();
    assert!(rule_ids.contains(&"rule.a"));
    assert!(rule_ids.contains(&"rule.b"));
    assert!(
        created
            .iter()
            .all(|action| action.playbook_id == "playbook.a")
    );
    assert!(
        created
            .iter()
            .all(|action| action.idempotency_key.contains(&action.rule_id))
    );

    let inserted = store.inserted.lock().await;
    assert_eq!(inserted.len(), 2);
}

#[tokio::test]
async fn action_builder_creates_one_per_inconclusive_rule() {
    let store = Arc::new(MockActionStore::new());
    let builder = ActionBuilder::new(store.clone());

    let envelope = fail_envelope();
    let decision = Decision::inconclusive("inconclusive", "rules inconclusive");
    let evaluations = vec![
        inconclusive_rule_evaluation("playbook.a", "rule.a", "a"),
        inconclusive_rule_evaluation("playbook.a", "rule.b", "b"),
    ];
    let result = OrchestrationResult {
        decision,
        action_candidates: vec![],
        playbooks: vec![PlaybookAssignment {
            playbook_id: "playbook.a".to_string(),
            reason: "matched".to_string(),
        }],
        matched_playbook: None,
        evaluations,
        codex: CodexResult {
            version_id: Some("v1".to_string()),
            playbooks: vec![],
            decision: Decision::inconclusive("inconclusive", "rules inconclusive"),
        },
        playbook_summaries: Vec::new(),
        route_to_action_builder: true,
    };

    let created = builder
        .create_actions_for_failure(&envelope, &result, None)
        .await
        .unwrap();

    assert_eq!(created.len(), 2);
    let rule_ids: Vec<&str> = created
        .iter()
        .map(|action| action.rule_id.as_str())
        .collect();
    assert!(rule_ids.contains(&"rule.a"));
    assert!(rule_ids.contains(&"rule.b"));

    let inserted = store.inserted.lock().await;
    assert_eq!(inserted.len(), 2);
}

#[tokio::test]
async fn action_builder_uses_unknown_for_missing_object_type_and_id() {
    let store = Arc::new(MockActionStore::new());
    let builder = ActionBuilder::new(store.clone());

    let mut envelope = fail_envelope();
    envelope.head.changed_object_type = None;
    envelope.head.changed_object_id = None;

    let result = minimal_orchestration_result_fail("pb1", "r1");
    let created = builder
        .create_actions_for_failure(&envelope, &result, None)
        .await
        .unwrap();

    assert_eq!(created.len(), 1);
    assert!(created[0].idempotency_key.contains("Unknown"));
}

#[tokio::test]
async fn action_builder_deduplicates_duplicate_evaluations_for_same_rule() {
    let store = Arc::new(MockActionStore::new());
    let builder = ActionBuilder::new(store.clone());

    let decision = Decision::fail("fail", "same rule reported twice");
    let result = OrchestrationResult {
        decision: decision.clone(),
        action_candidates: vec![],
        playbooks: vec![PlaybookAssignment {
            playbook_id: "playbook.a".to_string(),
            reason: "matched".to_string(),
        }],
        matched_playbook: None,
        evaluations: vec![
            failing_rule_evaluation("playbook.a", "rule.a", "first"),
            failing_rule_evaluation("playbook.a", "rule.a", "duplicate"),
        ],
        codex: CodexResult {
            version_id: Some("v1".to_string()),
            playbooks: vec![],
            decision,
        },
        playbook_summaries: Vec::new(),
        route_to_action_builder: true,
    };

    let created = builder
        .create_actions_for_failure(&fail_envelope(), &result, None)
        .await
        .unwrap();

    assert_eq!(created.len(), 1);
    assert_eq!(created[0].playbook_id, "playbook.a");
    assert_eq!(created[0].rule_id, "rule.a");
    assert_eq!(store.inserted.lock().await.len(), 1);
}

#[tokio::test]
async fn action_builder_excludes_rules_that_were_already_handled() {
    let store = Arc::new(MockActionStore::new());
    let builder = ActionBuilder::new(store.clone());

    let decision = Decision::fail("fail", "rules failed");
    let result = OrchestrationResult {
        decision: decision.clone(),
        action_candidates: vec![],
        playbooks: vec![PlaybookAssignment {
            playbook_id: "playbook.a".to_string(),
            reason: "matched".to_string(),
        }],
        matched_playbook: None,
        evaluations: vec![
            failing_rule_evaluation("playbook.a", "rule.bound", "handled elsewhere"),
            failing_rule_evaluation("playbook.a", "rule.fallback", "needs fallback action"),
        ],
        codex: CodexResult {
            version_id: Some("v1".to_string()),
            playbooks: vec![],
            decision,
        },
        playbook_summaries: Vec::new(),
        route_to_action_builder: true,
    };
    let excluded_rules = HashSet::from([("playbook.a".to_string(), "rule.bound".to_string())]);

    let created = builder
        .create_actions_for_failure_excluding(&fail_envelope(), &result, None, &excluded_rules)
        .await
        .unwrap();

    assert_eq!(created.len(), 1);
    assert_eq!(created[0].rule_id, "rule.fallback");
    assert_eq!(store.inserted.lock().await.len(), 1);
}

#[tokio::test]
async fn action_builder_creates_fallback_action_when_review_decision_has_no_evaluations() {
    let store = Arc::new(MockActionStore::new());
    let builder = ActionBuilder::new(store.clone());

    let decision = Decision::fail("fail", "failed without evaluations");
    let result = OrchestrationResult {
        decision: decision.clone(),
        action_candidates: vec![],
        playbooks: vec![PlaybookAssignment {
            playbook_id: "playbook.fallback".to_string(),
            reason: "matched".to_string(),
        }],
        matched_playbook: None,
        evaluations: Vec::new(),
        codex: CodexResult {
            version_id: Some("v1".to_string()),
            playbooks: vec![],
            decision,
        },
        playbook_summaries: Vec::new(),
        route_to_action_builder: true,
    };

    let created = builder
        .create_actions_for_failure(&fail_envelope(), &result, None)
        .await
        .unwrap();

    assert_eq!(created.len(), 1);
    assert_eq!(created[0].playbook_id, "playbook.fallback");
    assert_eq!(created[0].rule_id, "none");
    assert_eq!(
        created[0].action_description.as_deref(),
        Some("Decision was FAIL but no rule evaluations were recorded")
    );
}

#[tokio::test]
async fn action_builder_suppresses_child_rule_binding_when_parent_object_failed() {
    let store = Arc::new(MockActionStore::new());
    let template_store = Arc::new(MockActionTemplateStore {
        templates: vec![template("tmpl-review")],
    });
    let builder = ActionBuilder::with_template_store(store.clone(), template_store);

    let decision = Decision::fail("fail", "parent and child failed");
    let mut parent = failing_rule_evaluation("playbook.a", "rule.parent", "parent failed");
    parent.object_type = Some("sla".to_string());
    parent.action_template_id = Some("tmpl-review".to_string());

    let mut child = failing_rule_evaluation("playbook.a", "rule.child", "child failed");
    child.object_type = Some("sla.response_time".to_string());
    child.action_template_id = Some("tmpl-review".to_string());

    let result = OrchestrationResult {
        decision: decision.clone(),
        action_candidates: vec![],
        playbooks: vec![PlaybookAssignment {
            playbook_id: "playbook.a".to_string(),
            reason: "matched".to_string(),
        }],
        matched_playbook: None,
        evaluations: vec![parent, child],
        codex: CodexResult {
            version_id: Some("v1".to_string()),
            playbooks: vec![],
            decision,
        },
        playbook_summaries: Vec::new(),
        route_to_action_builder: true,
    };

    let created = builder
        .create_actions_from_rule_bindings(&fail_envelope(), &result)
        .await
        .unwrap();

    assert_eq!(created.len(), 1);
    assert_eq!(created[0].rule_id, "rule.parent");
    assert_eq!(
        created[0].action_template_id.as_deref(),
        Some("tmpl-review")
    );
    assert_eq!(
        created[0].execution_mode.as_deref(),
        Some("ApprovalRequired")
    );
    assert_eq!(created[0].responsible_user.as_deref(), Some("user-123"));
    assert_eq!(
        created[0].responsible_role.as_deref(),
        Some("governance-reviewer")
    );
    assert_eq!(created[0].escalation_duration, Some(2));
    assert_eq!(created[0].escalation_duration_unit.as_deref(), Some("Days"));
    assert_eq!(created[0].require_document_upload, Some(true));
    assert_eq!(created[0].require_comment, Some(true));
    assert_eq!(created[0].require_approval_reference, Some(false));
    assert_eq!(store.inserted.lock().await.len(), 1);
}

#[tokio::test]
async fn action_builder_creates_pass_action_from_matching_template() {
    let store = Arc::new(MockActionStore::new());
    let template_store = Arc::new(MockActionTemplateStore {
        templates: vec![{
            let mut t = template("tmpl-pass");
            t.trigger.event_type = TriggerEventType::RulePassed;
            t.trigger.execution_mode = ExecutionMode::Automatic;
            t.responsibility.escalation_duration_unit = Some(EscalationDurationUnit::Hours);
            t.evidence.require_approval_reference = true;
            t
        }],
    });
    let builder = ActionBuilder::with_template_store(store.clone(), template_store);

    let mut result = minimal_orchestration_result_fail("playbook.pass", "rule.ok");
    let decision = Decision::pass("pass", "passed");
    result.decision = decision.clone();
    result.evaluations[0].decision = decision.clone();
    result.evaluations[0].reason = "all checks passed".to_string();
    result.codex.decision = decision;

    let created = builder
        .create_actions_from_templates(&fail_envelope(), &result)
        .await
        .unwrap();

    assert_eq!(created.len(), 1);
    assert_eq!(created[0].action_template_id.as_deref(), Some("tmpl-pass"));
    assert_eq!(created[0].rule_id, "rule.ok");
    assert_eq!(created[0].execution_mode.as_deref(), Some("Automatic"));
    assert_eq!(
        created[0].escalation_duration_unit.as_deref(),
        Some("Hours")
    );
    assert_eq!(created[0].require_approval_reference, Some(true));
    assert_eq!(
        created[0].action_description.as_deref(),
        Some("Review required: all checks passed")
    );
    assert_eq!(store.inserted.lock().await.len(), 1);
}

#[tokio::test]
async fn action_builder_returns_empty_when_template_store_is_not_configured() {
    let store = Arc::new(MockActionStore::new());
    let builder = ActionBuilder::new(store);
    let result = minimal_orchestration_result_fail("playbook.a", "rule.a");

    let created = builder
        .create_actions_from_templates(&fail_envelope(), &result)
        .await
        .unwrap();

    assert!(created.is_empty());
}

#[tokio::test]
async fn action_builder_template_associated_rule_limits_created_playbooks() {
    let store = Arc::new(MockActionStore::new());
    let mut associated = template("tmpl-rule-bound");
    associated.associated_rule_ids = vec!["rule.b".to_string()];
    let template_store = Arc::new(MockActionTemplateStore {
        templates: vec![associated],
    });
    let builder = ActionBuilder::with_template_store(store.clone(), template_store);

    let decision = Decision::fail("fail", "rules failed");
    let result = OrchestrationResult {
        decision: decision.clone(),
        action_candidates: vec![],
        playbooks: vec![
            PlaybookAssignment {
                playbook_id: "playbook.a".to_string(),
                reason: "matched".to_string(),
            },
            PlaybookAssignment {
                playbook_id: "playbook.b".to_string(),
                reason: "matched".to_string(),
            },
        ],
        matched_playbook: None,
        evaluations: vec![
            failing_rule_evaluation("playbook.a", "rule.a", "a"),
            failing_rule_evaluation("playbook.b", "rule.b", "b"),
        ],
        codex: CodexResult {
            version_id: Some("v1".to_string()),
            playbooks: vec![],
            decision,
        },
        playbook_summaries: Vec::new(),
        route_to_action_builder: true,
    };

    let created = builder
        .create_actions_from_templates(&fail_envelope(), &result)
        .await
        .unwrap();

    assert_eq!(created.len(), 1);
    assert_eq!(created[0].playbook_id, "playbook.b");
    assert_eq!(created[0].rule_id, "rule.b");
}

#[tokio::test]
async fn action_builder_template_associated_playbook_ignores_unmatched_playbook() {
    let store = Arc::new(MockActionStore::new());
    let mut associated = template("tmpl-playbook-bound");
    associated.associated_playbook_ids = vec!["playbook.not-in-result".to_string()];
    let template_store = Arc::new(MockActionTemplateStore {
        templates: vec![associated],
    });
    let builder = ActionBuilder::with_template_store(store.clone(), template_store);

    let result = minimal_orchestration_result_fail("playbook.a", "rule.a");
    let created = builder
        .create_actions_from_templates(&fail_envelope(), &result)
        .await
        .unwrap();

    assert!(created.is_empty());
    assert!(store.inserted.lock().await.is_empty());
}

#[tokio::test]
async fn action_builder_routes_legacy_action_and_persists_assignment() {
    let store = Arc::new(MockActionStore::new());
    let router = Arc::new(MockWorkRouter::team_route());
    let builder = ActionBuilder::with_work_router(store.clone(), router.clone());

    let result = minimal_orchestration_result_fail("playbook.route", "rule.route");
    let created = builder
        .create_actions_for_failure(&fail_envelope(), &result, None)
        .await
        .unwrap();

    assert_eq!(created.len(), 1);
    assert_eq!(created[0].assigned_to_type.as_deref(), Some("team"));
    assert_eq!(created[0].assigned_to_id.as_deref(), Some("team-risk"));
    assert_eq!(created[0].assigned_to_name.as_deref(), Some("Risk Review"));

    let requests = router.requests.lock().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].playbook_id, "playbook.route");
    assert_eq!(requests[0].responsible_user, None);
    assert_eq!(requests[0].responsible_role, None);

    let assignments = store.assignments.lock().await;
    assert_eq!(assignments.len(), 1);
    assert_eq!(assignments[0].1, "team");
    assert_eq!(assignments[0].2, "team-risk");
}

#[tokio::test]
async fn action_builder_logs_created_and_reused_actions_to_event_logger() {
    let envelope = fail_envelope();
    let result = minimal_orchestration_result_fail("pb1", "r1");
    let logger = Arc::new(MockIntakeStore::default());
    let logger_dyn: Arc<dyn IntakeStore> = logger.clone();

    let store = Arc::new(MockActionStore::new());
    let builder = ActionBuilder::new(store.clone());
    let created = builder
        .create_actions_for_failure(&envelope, &result, Some(&logger_dyn))
        .await
        .unwrap();
    assert_eq!(created.len(), 1);

    let duplicate_store =
        Arc::new(MockActionStore::with_existing_hash(&created[0].idempotency_hash).await);
    let duplicate_builder = ActionBuilder::new(duplicate_store);
    let duplicate = duplicate_builder
        .create_actions_for_failure(&envelope, &result, Some(&logger_dyn))
        .await
        .unwrap();
    assert!(duplicate.is_empty());

    let logs = logger.event_logs.lock().await;
    assert_eq!(logs.len(), 2);
    assert_eq!(logs[0].2, "action created");
    assert_eq!(
        logs[1].2,
        "action reused because matching idempotency hash already exists"
    );
}

#[tokio::test]
async fn action_builder_rule_bindings_skip_missing_template_and_create_child_without_parent() {
    let store = Arc::new(MockActionStore::new());
    let template_store = Arc::new(MockActionTemplateStore {
        templates: vec![template("tmpl-child")],
    });
    let builder = ActionBuilder::with_template_store(store.clone(), template_store);

    let decision = Decision::fail("fail", "child failed");
    let mut child = failing_rule_evaluation("playbook.a", "rule.child", "child failed");
    child.object_type = Some("sla.response_time".to_string());
    child.action_template_id = Some("tmpl-child".to_string());
    child.checks[0].expected = Some(serde_json::json!(30));
    child.checks[0].actual = Some(serde_json::json!(45));

    let mut missing_template = failing_rule_evaluation("playbook.a", "rule.missing", "missing");
    missing_template.action_template_id = Some("tmpl-missing".to_string());

    let result = OrchestrationResult {
        decision: decision.clone(),
        action_candidates: vec![],
        playbooks: vec![PlaybookAssignment {
            playbook_id: "playbook.a".to_string(),
            reason: "matched".to_string(),
        }],
        matched_playbook: None,
        evaluations: vec![child, missing_template],
        codex: CodexResult {
            version_id: Some("v1".to_string()),
            playbooks: vec![],
            decision,
        },
        playbook_summaries: Vec::new(),
        route_to_action_builder: true,
    };

    let created = builder
        .create_actions_from_rule_bindings(&fail_envelope(), &result)
        .await
        .unwrap();

    assert_eq!(created.len(), 1);
    assert_eq!(created[0].rule_id, "rule.child");
    assert_eq!(created[0].action_template_id.as_deref(), Some("tmpl-child"));
    assert_eq!(
        created[0].action_description.as_deref(),
        Some("Action required: 'status' does not satisfy 'eq': expected 30, got 45")
    );
    assert_eq!(store.inserted.lock().await.len(), 1);
}
