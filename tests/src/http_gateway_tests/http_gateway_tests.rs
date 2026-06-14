use actix::prelude::*;
use actix_web::{Responder, body::to_bytes, http::StatusCode, test as aw_test, web};
use async_trait::async_trait;
use domain::envelope::{CanonicalEnvelope, TadpoleHead, compute_content_hash};
use engine_core::actors::{
    assigner::{AssignerActor, PlaybookConfig},
    diagnostics::DiagnosticsActor,
    dispatcher::{BooleanRuleEvaluator, DispatcherActor, EnrichmentStubEvaluator, RuleEvaluator},
    handlers::boolean_handler::BooleanRuleHandler,
    orchestrator::OrchestratorActor,
};
use engine_core::dto::rules::RuleKind;
use engine_core::dto::rules::RuleLogic;
use engine_core::ulid::UlidService;
use http_gateway::mongo_store::{
    DbPlaybook, DbPlaybookRuleRef, DbPlaybookTrigger, DbRule, DbRuleCondition, DbRuleObject,
    IntakeStore,
};
use http_gateway::{
    ActionFeedPublisher, ActionRecord, ActionStore, ActionTemplate, ActionTemplateListFilter,
    ActionTemplateStore, EscalationDurationUnit, EvidenceConfig, ExecutionMode,
    METRIC_TYPE_ACTION_TRIGGERED, METRIC_TYPE_RULE_FAIL, METRIC_TYPE_RULE_INCONCLUSIVE,
    METRIC_TYPE_RULE_PASS, MetricManager, MetricRecord, MetricStore, MongoStore, PipelineError,
    ResponsibilityConfig, TemplateStatus, TriggerConfig, TriggerEventType, action_template_by_id,
    action_templates, diagnostics, event_logs_by_event_id, intake, metrics, metrics_by_event_id,
    process_envelope, validate_envelope,
};
use mongodb::{
    Client,
    bson::{DateTime, doc},
};
use std::collections::HashMap;
#[cfg(unix)]
use std::os::raw::{c_int, c_long};
use std::sync::{Arc, Mutex};
use std::time::Instant;

fn disabled_action_builder() -> web::Data<Option<Arc<http_gateway::ActionBuilder>>> {
    web::Data::new(None)
}

#[derive(Clone, Default)]
struct TestActionStore {
    inserted: Arc<Mutex<Vec<ActionRecord>>>,
}

#[async_trait]
impl ActionStore for TestActionStore {
    async fn find_by_idempotency_hash(
        &self,
        _idempotency_hash: &str,
    ) -> Result<Option<ActionRecord>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(None)
    }

    async fn insert_action(
        &self,
        record: &ActionRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.inserted.lock().unwrap().push(record.clone());
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

fn action_builder_bundle() -> (
    Arc<Mutex<Vec<ActionRecord>>>,
    web::Data<Option<Arc<http_gateway::ActionBuilder>>>,
) {
    let store = Arc::new(TestActionStore::default());
    let inserted = store.inserted.clone();
    let builder = http_gateway::ActionBuilder::new(store);
    (inserted, web::Data::new(Some(Arc::new(builder))))
}

fn disabled_metric_manager() -> web::Data<Option<Arc<http_gateway::MetricManager>>> {
    web::Data::new(None)
}

fn disabled_action_feed_publisher() -> web::Data<Option<Arc<dyn ActionFeedPublisher>>> {
    web::Data::new(None)
}

fn valid_envelope() -> CanonicalEnvelope {
    CanonicalEnvelope {
        head: TadpoleHead {
            event_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_string(),
            event_name: "ContractCreated".to_string(),
            event_category: Some("Transaction".to_string()),
            tenant_id: "acme".to_string(),
            correlation_id: None,
            causation_id: None,
            occurred_at: None,
            originating_function: None,
            originating_application: None,
            environment: None,
            external_dependency_id: Some("dep_123".to_string()),
            changed_object_type: Some("Contract".to_string()),
            changed_object_id: Some("ctr_987".to_string()),
            change_kind: Some("created".to_string()),
        },
        body: serde_json::json!({
            "snapshots": {
                "contract": {
                    "object_id": "ctr_987",
                    "amount": 12500,
                    "currency": "USD"
                }
            }
        }),
    }
}

#[derive(Clone, Default)]
struct TestStore {
    records: Arc<Mutex<Vec<TestRecord>>>,
    event_logs: Arc<Mutex<Vec<TestEventLog>>>,
    matched_playbook: Option<DbPlaybook>,
    extra_matched_playbooks: Vec<DbPlaybook>,
    rules: Vec<DbRule>,
}

#[derive(Clone)]
struct TestRecord {
    status: String,
    event_id: String,
    has_errors: bool,
    response_event_count: Option<usize>,
}

#[derive(Clone)]
struct TestEventLog {
    event_id: String,
    stage: String,
    level: String,
    message: String,
    details: Option<serde_json::Value>,
}

#[derive(Clone)]
struct ErrorStore {
    find_playbook_error: Option<String>,
    find_rules_error: Option<String>,
    list_recent_error: Option<String>,
    list_event_logs_error: Option<String>,
}

#[derive(Clone, Default)]
struct TestMetricStore {
    records: Arc<Mutex<Vec<MetricRecord>>>,
}

struct ErrorMetricStore {
    list_error: Option<String>,
    list_by_event_error: Option<String>,
}

#[derive(Clone)]
struct TestTemplateStore {
    templates: Arc<Mutex<Vec<ActionTemplate>>>,
    list_error: Option<String>,
    find_error: Option<String>,
}

#[async_trait]
impl IntakeStore for ErrorStore {
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
        match &self.list_recent_error {
            Some(msg) => Err(msg.clone().into()),
            None => Ok(Vec::new()),
        }
    }

    async fn append_event_log(
        &self,
        _envelope: &CanonicalEnvelope,
        _stage: &str,
        _level: &str,
        _message: &str,
        _details: Option<&serde_json::Value>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn list_event_logs_by_event_id(
        &self,
        _event_id: &str,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
        match &self.list_event_logs_error {
            Some(msg) => Err(msg.clone().into()),
            None => Ok(Vec::new()),
        }
    }

    async fn find_matched_playbook(
        &self,
        _changed_object_type: &str,
        _change_kind: &str,
    ) -> Result<Option<DbPlaybook>, Box<dyn std::error::Error + Send + Sync>> {
        match &self.find_playbook_error {
            Some(msg) => Err(msg.clone().into()),
            None if self.find_rules_error.is_some() => Ok(Some(DbPlaybook {
                id: "playbook.contract_governance".to_string(),
                name: Some("Contract Governance".to_string()),
                version: Some("1.0".to_string()),
                execution_mode: Some("Fail_First".to_string()),
                trigger: DbPlaybookTrigger {
                    object_type: "Contract".to_string(),
                    change_kind: vec!["created".to_string()],
                },
                rules: vec![DbPlaybookRuleRef {
                    order_seq: 1,
                    rule_id: "rule.contract_created_check".to_string(),
                    is_critical: true,
                    action_template_id: None,
                }],
                status: Some("Active".to_string()),
            })),
            None => Ok(None),
        }
    }

    async fn find_active_rules(
        &self,
        _rule_ids: &[String],
    ) -> Result<Vec<DbRule>, Box<dyn std::error::Error + Send + Sync>> {
        match &self.find_rules_error {
            Some(msg) => Err(msg.clone().into()),
            None => Ok(Vec::new()),
        }
    }

    async fn find_playbooks_by_ids(
        &self,
        _playbook_ids: &[String],
    ) -> Result<Vec<DbPlaybook>, Box<dyn std::error::Error + Send + Sync>> {
        match &self.find_playbook_error {
            Some(msg) => Err(msg.clone().into()),
            None => Ok(Vec::new()),
        }
    }

    async fn find_rules_by_ids(
        &self,
        _rule_ids: &[String],
    ) -> Result<Vec<DbRule>, Box<dyn std::error::Error + Send + Sync>> {
        match &self.find_rules_error {
            Some(msg) => Err(msg.clone().into()),
            None => Ok(Vec::new()),
        }
    }
}

#[async_trait]
impl MetricStore for TestMetricStore {
    async fn upsert_metric(
        &self,
        record: &MetricRecord,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        self.records.lock().unwrap().push(record.clone());
        Ok(true)
    }

    async fn sum_metric_values(
        &self,
        _query: &http_gateway::MetricWindowQuery,
    ) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        Ok(0)
    }

    async fn list_metrics(
        &self,
    ) -> Result<Vec<MetricRecord>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.records.lock().unwrap().clone())
    }

    async fn list_metrics_by_event_id(
        &self,
        event_id: &str,
    ) -> Result<Vec<MetricRecord>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|record| record.event_id == event_id)
            .cloned()
            .collect())
    }
}

#[async_trait]
impl MetricStore for ErrorMetricStore {
    async fn upsert_metric(
        &self,
        _record: &MetricRecord,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        Ok(true)
    }

    async fn sum_metric_values(
        &self,
        _query: &http_gateway::MetricWindowQuery,
    ) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        Ok(0)
    }

    async fn list_metrics(
        &self,
    ) -> Result<Vec<MetricRecord>, Box<dyn std::error::Error + Send + Sync>> {
        match &self.list_error {
            Some(msg) => Err(msg.clone().into()),
            None => Ok(Vec::new()),
        }
    }

    async fn list_metrics_by_event_id(
        &self,
        _event_id: &str,
    ) -> Result<Vec<MetricRecord>, Box<dyn std::error::Error + Send + Sync>> {
        match &self.list_by_event_error {
            Some(msg) => Err(msg.clone().into()),
            None => Ok(Vec::new()),
        }
    }
}

#[async_trait]
impl ActionTemplateStore for TestTemplateStore {
    async fn list_templates(
        &self,
        filter: ActionTemplateListFilter,
        limit: i64,
    ) -> Result<Vec<ActionTemplate>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(msg) = &self.list_error {
            return Err(msg.clone().into());
        }

        let limit = limit.clamp(1, 200) as usize;
        let templates = self.templates.lock().unwrap();
        Ok(templates
            .iter()
            .filter(|template| {
                filter
                    .tenant_id
                    .as_ref()
                    .is_none_or(|tenant_id| template.tenant_id == *tenant_id)
                    && filter
                        .status
                        .as_ref()
                        .is_none_or(|status| template.status == *status)
                    && filter
                        .object_type
                        .as_ref()
                        .is_none_or(|object_type| template.trigger.object_type == *object_type)
                    && filter
                        .event_type
                        .as_ref()
                        .is_none_or(|event_type| template.trigger.event_type == *event_type)
            })
            .take(limit)
            .cloned()
            .collect())
    }

    async fn find_templates_by_trigger(
        &self,
        tenant_id: &str,
        object_type: &str,
        event_type: &TriggerEventType,
    ) -> Result<Vec<ActionTemplate>, Box<dyn std::error::Error + Send + Sync>> {
        let templates = self.templates.lock().unwrap();
        Ok(templates
            .iter()
            .filter(|template| {
                template.tenant_id == tenant_id
                    && template.trigger.object_type == object_type
                    && template.trigger.event_type == *event_type
                    && template.status == TemplateStatus::Active
            })
            .cloned()
            .collect())
    }

    async fn find_template_by_id(
        &self,
        template_id: &str,
    ) -> Result<Option<ActionTemplate>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(msg) = &self.find_error {
            return Err(msg.clone().into());
        }

        let templates = self.templates.lock().unwrap();
        Ok(templates
            .iter()
            .find(|template| template.template_id == template_id)
            .cloned())
    }
}

#[async_trait]
impl IntakeStore for TestStore {
    async fn record_intake(
        &self,
        envelope: &CanonicalEnvelope,
        status: &str,
        errors: Option<&Vec<String>>,
        response: Option<&serde_json::Value>,
        _error_message: Option<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let record = TestRecord {
            status: status.to_string(),
            event_id: envelope.head.event_id.clone(),
            has_errors: errors.is_some(),
            response_event_count: response
                .and_then(|value| value.as_array())
                .map(|events| events.len()),
        };
        self.records.lock().unwrap().push(record);
        Ok(())
    }

    async fn list_recent_intake(
        &self,
        limit: i64,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
        let records = self.records.lock().unwrap();
        let rows = records
            .iter()
            .rev()
            .take(limit as usize)
            .map(|record| {
                serde_json::json!({
                    "status": record.status,
                    "event_id": record.event_id,
                    "has_errors": record.has_errors,
                    "response_event_count": record.response_event_count,
                })
            })
            .collect();
        Ok(rows)
    }

    async fn append_event_log(
        &self,
        envelope: &CanonicalEnvelope,
        stage: &str,
        level: &str,
        _message: &str,
        _details: Option<&serde_json::Value>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.event_logs.lock().unwrap().push(TestEventLog {
            event_id: envelope.head.event_id.clone(),
            stage: stage.to_string(),
            level: level.to_string(),
            message: _message.to_string(),
            details: _details.cloned(),
        });
        Ok(())
    }

    async fn list_event_logs_by_event_id(
        &self,
        event_id: &str,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
        let rows = self
            .event_logs
            .lock()
            .unwrap()
            .iter()
            .filter(|log| log.event_id == event_id)
            .map(|log| {
                serde_json::json!({
                    "event_id": log.event_id,
                    "stage": log.stage,
                    "level": log.level,
                    "message": log.message,
                    "details": log.details,
                })
            })
            .collect();
        Ok(rows)
    }

    async fn find_matched_playbook(
        &self,
        _changed_object_type: &str,
        _change_kind: &str,
    ) -> Result<Option<DbPlaybook>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.matched_playbook.clone())
    }

    async fn find_matched_playbooks(
        &self,
        _changed_object_type: &str,
        _change_kind: &str,
    ) -> Result<Vec<DbPlaybook>, Box<dyn std::error::Error + Send + Sync>> {
        let mut playbooks: Vec<DbPlaybook> = self.matched_playbook.clone().into_iter().collect();
        playbooks.extend(self.extra_matched_playbooks.clone());
        Ok(playbooks)
    }

    async fn find_active_rules(
        &self,
        rule_ids: &[String],
    ) -> Result<Vec<DbRule>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self
            .rules
            .iter()
            .filter(|rule| {
                rule_ids.contains(&rule.id)
                    && rule
                        .status
                        .as_deref()
                        .map(|status| status.eq_ignore_ascii_case("active"))
                        .unwrap_or(false)
            })
            .cloned()
            .collect())
    }

    async fn find_playbooks_by_ids(
        &self,
        playbook_ids: &[String],
    ) -> Result<Vec<DbPlaybook>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self
            .matched_playbook
            .clone()
            .into_iter()
            .chain(self.extra_matched_playbooks.clone())
            .filter(|playbook| playbook_ids.contains(&playbook.id))
            .collect())
    }

    async fn find_rules_by_ids(
        &self,
        rule_ids: &[String],
    ) -> Result<Vec<DbRule>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self
            .rules
            .iter()
            .filter(|rule| rule_ids.contains(&rule.id))
            .cloned()
            .collect())
    }
}

fn matching_store_bundle() -> (TestStore, web::Data<Arc<dyn IntakeStore>>) {
    let store = TestStore {
        records: Arc::new(Mutex::new(Vec::new())),
        event_logs: Arc::new(Mutex::new(Vec::new())),
        matched_playbook: Some(DbPlaybook {
            id: "playbook.contract_governance".to_string(),
            name: Some("Contract Governance".to_string()),
            version: Some("1.0".to_string()),
            execution_mode: Some("Fail_First".to_string()),
            trigger: DbPlaybookTrigger {
                object_type: "Contract".to_string(),
                change_kind: vec!["created".to_string()],
            },
            rules: vec![DbPlaybookRuleRef {
                order_seq: 1,
                rule_id: "rule.contract_created_check".to_string(),
                is_critical: true,
                action_template_id: None,
            }],
            status: Some("Active".to_string()),
        }),
        extra_matched_playbooks: Vec::new(),
        rules: vec![DbRule {
            id: "rule.contract_created_check".to_string(),
            name: Some("Contract Amount Check".to_string()),
            object: DbRuleObject {
                object_type: "contract".to_string(),
            },
            conditions: vec![DbRuleCondition {
                object_key: "amount".to_string(),
                operator: "eq".to_string(),
                key_data_type: Some("NUMBER".to_string()),
                value: None,
                value_int: Some(12500),
                value_float: None,
                value_bool: None,
            }],
            logic: RuleLogic::All,
            status: Some("ACTIVE".to_string()),
        }],
    };
    let store_data: web::Data<Arc<dyn IntakeStore>> = web::Data::new(Arc::new(store.clone()));
    (store, store_data)
}

fn no_match_store_bundle() -> (TestStore, web::Data<Arc<dyn IntakeStore>>) {
    let store = TestStore {
        records: Arc::new(Mutex::new(Vec::new())),
        event_logs: Arc::new(Mutex::new(Vec::new())),
        matched_playbook: None,
        extra_matched_playbooks: Vec::new(),
        rules: Vec::new(),
    };
    let store_data: web::Data<Arc<dyn IntakeStore>> = web::Data::new(Arc::new(store.clone()));
    (store, store_data)
}

fn inactive_rule_store_bundle() -> (TestStore, web::Data<Arc<dyn IntakeStore>>) {
    let store = TestStore {
        records: Arc::new(Mutex::new(Vec::new())),
        event_logs: Arc::new(Mutex::new(Vec::new())),
        matched_playbook: Some(DbPlaybook {
            id: "playbook.contract_governance".to_string(),
            name: Some("Contract Governance".to_string()),
            version: Some("1.0".to_string()),
            execution_mode: Some("Fail_First".to_string()),
            trigger: DbPlaybookTrigger {
                object_type: "Contract".to_string(),
                change_kind: vec!["created".to_string()],
            },
            rules: vec![
                DbPlaybookRuleRef {
                    order_seq: 1,
                    rule_id: "rule.contract_created_check".to_string(),
                    is_critical: true,
                    action_template_id: None,
                },
                DbPlaybookRuleRef {
                    order_seq: 2,
                    rule_id: "rule.contract_created_check_inactive".to_string(),
                    is_critical: false,
                    action_template_id: None,
                },
            ],
            status: Some("Active".to_string()),
        }),
        extra_matched_playbooks: Vec::new(),
        rules: vec![
            DbRule {
                id: "rule.contract_created_check".to_string(),
                name: Some("Contract Amount Check".to_string()),
                object: DbRuleObject {
                    object_type: "contract".to_string(),
                },
                conditions: vec![DbRuleCondition {
                    object_key: "amount".to_string(),
                    operator: "eq".to_string(),
                    key_data_type: Some("NUMBER".to_string()),
                    value: None,
                    value_int: Some(12500),
                    value_float: None,
                    value_bool: None,
                }],
                logic: RuleLogic::All,
                status: Some("ACTIVE".to_string()),
            },
            DbRule {
                id: "rule.contract_created_check_inactive".to_string(),
                name: Some("Inactive Rule".to_string()),
                object: DbRuleObject {
                    object_type: "contract".to_string(),
                },
                conditions: vec![DbRuleCondition {
                    object_key: "amount".to_string(),
                    operator: "gt".to_string(),
                    key_data_type: Some("NUMBER".to_string()),
                    value: None,
                    value_int: Some(1_000_000),
                    value_float: None,
                    value_bool: None,
                }],
                logic: RuleLogic::All,
                status: Some("INACTIVE".to_string()),
            },
        ],
    };
    let store_data: web::Data<Arc<dyn IntakeStore>> = web::Data::new(Arc::new(store.clone()));
    (store, store_data)
}

fn actor_bundle() -> (
    Addr<AssignerActor>,
    Addr<DispatcherActor>,
    Addr<OrchestratorActor>,
    Addr<DiagnosticsActor>,
) {
    let config: PlaybookConfig = serde_json::from_str(r#"{"playbooks":[]}"#).unwrap();
    let assigner = AssignerActor::from_config(config).start();

    let mut evaluators: HashMap<RuleKind, Arc<dyn RuleEvaluator>> = HashMap::new();
    evaluators.insert(
        RuleKind::Boolean,
        Arc::new(BooleanRuleEvaluator::new(BooleanRuleHandler.start())),
    );
    evaluators.insert(RuleKind::EnrichmentStub, Arc::new(EnrichmentStubEvaluator));
    let dispatcher = DispatcherActor { evaluators }.start();
    let orchestrator = OrchestratorActor.start();
    let diagnostics = DiagnosticsActor::default().start();

    (assigner, dispatcher, orchestrator, diagnostics)
}

fn sample_metric_record(event_id: &str, metric_type: &str, timestamp_ms: i64) -> MetricRecord {
    MetricRecord {
        dedupe_key: format!("{event_id}:{metric_type}"),
        event_id: event_id.to_string(),
        tenant_id: "acme".to_string(),
        metric_type: metric_type.to_string(),
        value: 1,
        timestamp: DateTime::from_millis(timestamp_ms),
        playbook_id: Some("playbook.contract_governance".to_string()),
        rule_id: Some("rule.contract_created_check".to_string()),
        action_id: None,
        threshold_id: None,
        metadata: Some(serde_json::json!({ "source": "test" })),
    }
}

fn metric_manager_bundle(records: Vec<MetricRecord>) -> web::Data<Option<Arc<MetricManager>>> {
    let store = TestMetricStore {
        records: Arc::new(Mutex::new(records)),
    };
    web::Data::new(Some(Arc::new(MetricManager::from_thresholds(
        Arc::new(store),
        None,
        Vec::new(),
    ))))
}

fn metric_store_and_manager_bundle() -> (
    Arc<Mutex<Vec<MetricRecord>>>,
    web::Data<Option<Arc<MetricManager>>>,
) {
    let records = Arc::new(Mutex::new(Vec::new()));
    let store = TestMetricStore {
        records: records.clone(),
    };
    let manager = MetricManager::from_thresholds(Arc::new(store), None, Vec::new());
    (records, web::Data::new(Some(Arc::new(manager))))
}

fn failing_metric_manager_bundle(
    list_error: Option<&str>,
    list_by_event_error: Option<&str>,
) -> web::Data<Option<Arc<MetricManager>>> {
    let store = ErrorMetricStore {
        list_error: list_error.map(str::to_string),
        list_by_event_error: list_by_event_error.map(str::to_string),
    };
    web::Data::new(Some(Arc::new(MetricManager::from_thresholds(
        Arc::new(store),
        None,
        Vec::new(),
    ))))
}

fn sample_action_template(template_id: &str) -> ActionTemplate {
    ActionTemplate {
        template_id: template_id.to_string(),
        tenant_id: "acme".to_string(),
        name: "Contract Review".to_string(),
        description: Some("Review contract governance result".to_string()),
        version: 1,
        status: TemplateStatus::Active,
        trigger: TriggerConfig {
            event_type: TriggerEventType::RuleFailed,
            object_type: "Contract".to_string(),
            execution_mode: ExecutionMode::Manual,
        },
        responsibility: ResponsibilityConfig {
            responsible_user: Some("user-123".to_string()),
            responsible_role: Some("Compliance".to_string()),
            escalation_duration: Some(24),
            escalation_duration_unit: Some(EscalationDurationUnit::Hours),
        },
        evidence: EvidenceConfig {
            require_document_upload: true,
            require_comment: true,
            require_approval_reference: false,
        },
        associated_rule_ids: vec!["rule.contract_created_check".to_string()],
        associated_playbook_ids: vec!["playbook.contract_governance".to_string()],
    }
}

fn template_store_bundle(
    templates: Vec<ActionTemplate>,
) -> web::Data<Option<Arc<dyn ActionTemplateStore>>> {
    web::Data::new(Some(Arc::new(TestTemplateStore {
        templates: Arc::new(Mutex::new(templates)),
        list_error: None,
        find_error: None,
    })))
}

fn failing_template_store_bundle(
    list_error: Option<&str>,
    find_error: Option<&str>,
) -> web::Data<Option<Arc<dyn ActionTemplateStore>>> {
    web::Data::new(Some(Arc::new(TestTemplateStore {
        templates: Arc::new(Mutex::new(vec![sample_action_template(
            "tmpl-contract-review",
        )])),
        list_error: list_error.map(str::to_string),
        find_error: find_error.map(str::to_string),
    })))
}

fn disabled_template_store() -> web::Data<Option<Arc<dyn ActionTemplateStore>>> {
    web::Data::new(None)
}

fn assert_standard_error(json: &serde_json::Value, error_code: &str, stage: &str, error: &str) {
    assert_eq!(json["status"], "error");
    assert_eq!(json["error_code"], error_code);
    assert_eq!(json["stage"], stage);
    assert_eq!(json["error"], error);
    assert!(
        json["description"].as_str().is_some_and(|description| {
            description.contains(stage) && description.contains(error)
        })
    );
}

#[derive(Debug)]
struct BulkTestObservation {
    event_count: usize,
    total_elapsed_ms: u128,
    avg_latency_us: u128,
    throughput_per_sec: f64,
    user_cpu_us: u128,
    system_cpu_us: u128,
    max_rss_kib: u64,
    max_rss_delta_kib: u64,
    persisted_intake_records: usize,
    persisted_event_logs: usize,
    emitted_metric_records: usize,
    total_response_bytes: usize,
}

impl BulkTestObservation {
    fn from_run(
        event_count: usize,
        total_elapsed_ms: u128,
        resources: ResourceObservation,
        persisted_intake_records: usize,
        persisted_event_logs: usize,
        emitted_metric_records: usize,
        total_response_bytes: usize,
    ) -> Self {
        let total_elapsed_us = total_elapsed_ms.saturating_mul(1_000);
        let avg_latency_us = total_elapsed_us / event_count as u128;
        let throughput_per_sec = if total_elapsed_ms == 0 {
            event_count as f64
        } else {
            (event_count as f64 * 1_000.0) / total_elapsed_ms as f64
        };

        Self {
            event_count,
            total_elapsed_ms,
            avg_latency_us,
            throughput_per_sec,
            user_cpu_us: resources.user_cpu_delta_us,
            system_cpu_us: resources.system_cpu_delta_us,
            max_rss_kib: resources.max_rss_kib,
            max_rss_delta_kib: resources.max_rss_delta_kib,
            persisted_intake_records,
            persisted_event_logs,
            emitted_metric_records,
            total_response_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct ResourceSnapshot {
    user_cpu_us: u128,
    system_cpu_us: u128,
    max_rss_kib: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct ResourceObservation {
    user_cpu_delta_us: u128,
    system_cpu_delta_us: u128,
    max_rss_kib: u64,
    max_rss_delta_kib: u64,
}

impl ResourceObservation {
    fn from_snapshots(start: ResourceSnapshot, end: ResourceSnapshot) -> Self {
        Self {
            user_cpu_delta_us: end.user_cpu_us.saturating_sub(start.user_cpu_us),
            system_cpu_delta_us: end.system_cpu_us.saturating_sub(start.system_cpu_us),
            max_rss_kib: end.max_rss_kib,
            max_rss_delta_kib: end.max_rss_kib.saturating_sub(start.max_rss_kib),
        }
    }
}

#[cfg(unix)]
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct TimeVal {
    tv_sec: c_long,
    tv_usec: c_long,
}

#[cfg(unix)]
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct RUsage {
    ru_utime: TimeVal,
    ru_stime: TimeVal,
    ru_maxrss: c_long,
    ru_ixrss: c_long,
    ru_idrss: c_long,
    ru_isrss: c_long,
    ru_minflt: c_long,
    ru_majflt: c_long,
    ru_nswap: c_long,
    ru_inblock: c_long,
    ru_oublock: c_long,
    ru_msgsnd: c_long,
    ru_msgrcv: c_long,
    ru_nsignals: c_long,
    ru_nvcsw: c_long,
    ru_nivcsw: c_long,
}

#[cfg(unix)]
unsafe extern "C" {
    fn getrusage(who: c_int, usage: *mut RUsage) -> c_int;
}

#[cfg(unix)]
fn current_resource_snapshot() -> ResourceSnapshot {
    const RUSAGE_SELF: c_int = 0;

    let mut usage = RUsage::default();
    let result = unsafe { getrusage(RUSAGE_SELF, &mut usage) };
    assert_eq!(result, 0, "getrusage should capture process resources");

    ResourceSnapshot {
        user_cpu_us: timeval_to_micros(usage.ru_utime),
        system_cpu_us: timeval_to_micros(usage.ru_stime),
        max_rss_kib: max_rss_to_kib(usage.ru_maxrss),
    }
}

#[cfg(not(unix))]
fn current_resource_snapshot() -> ResourceSnapshot {
    ResourceSnapshot::default()
}

#[cfg(unix)]
fn timeval_to_micros(value: TimeVal) -> u128 {
    (value.tv_sec as u128)
        .saturating_mul(1_000_000)
        .saturating_add(value.tv_usec as u128)
}

#[cfg(target_os = "macos")]
fn max_rss_to_kib(max_rss: c_long) -> u64 {
    (max_rss.max(0) as u64) / 1024
}

#[cfg(all(unix, not(target_os = "macos")))]
fn max_rss_to_kib(max_rss: c_long) -> u64 {
    max_rss.max(0) as u64
}

#[actix_rt::test]
async fn intake_rejects_invalid_payload() {
    let (assigner, dispatcher, orchestrator, diagnostics_actor) = actor_bundle();
    let (store, store_data) = matching_store_bundle();

    let mut envelope = valid_envelope();
    envelope.head.event_id = "invalid".to_string();
    envelope.head.tenant_id = " ".to_string();
    envelope.head.event_name = "".to_string();
    envelope.body = serde_json::Value::Null;

    let req = aw_test::TestRequest::default().to_http_request();

    let resp = intake(
        web::Json(envelope),
        web::Data::new(assigner),
        web::Data::new(dispatcher),
        web::Data::new(orchestrator),
        web::Data::new(diagnostics_actor),
        store_data,
        web::Data::new(None),
        disabled_metric_manager(),
        web::Data::new(None::<Arc<dyn ActionFeedPublisher>>),
    )
    .await;

    let response = resp.respond_to(&req);

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response.into_body();
    let bytes = match to_bytes(body).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read validation response body"),
    };
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("validation json");
    assert_eq!(json["error_code"], "validation_failed");
    assert_eq!(json["stage"], "pipeline.validation");
    assert!(json["description"].as_str().is_some());
    let records = store.records.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].status, "validation_failed");
    assert!(records[0].has_errors);
}

#[actix_rt::test]
async fn intake_rejects_invalid_correlation_id() {
    let (assigner, dispatcher, orchestrator, diagnostics_actor) = actor_bundle();
    let (store, store_data) = matching_store_bundle();

    let mut envelope = valid_envelope();
    envelope.head.correlation_id = Some("not-a-valid-ulid".to_string());

    let req = aw_test::TestRequest::default().to_http_request();
    let resp = intake(
        web::Json(envelope),
        web::Data::new(assigner),
        web::Data::new(dispatcher),
        web::Data::new(orchestrator),
        web::Data::new(diagnostics_actor),
        store_data,
        disabled_action_builder(),
        disabled_metric_manager(),
        disabled_action_feed_publisher(),
    )
    .await;

    let response = resp.respond_to(&req);
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let bytes = match to_bytes(response.into_body()).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read validation response body"),
    };
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("validation json");
    assert_eq!(json["error_code"], "validation_failed");
    assert_eq!(json["stage"], "pipeline.validation");
    let errors = json["errors"].as_array().expect("validation errors");
    assert!(errors.iter().any(|error| {
        error
            .as_str()
            .is_some_and(|message| message == "head.correlation_id must be a valid ULID")
    }));

    let records = store.records.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].status, "validation_failed");
    assert!(records[0].has_errors);
}

#[actix_rt::test]
async fn intake_rejects_invalid_causation_id() {
    let (assigner, dispatcher, orchestrator, diagnostics_actor) = actor_bundle();
    let (store, store_data) = matching_store_bundle();

    let mut envelope = valid_envelope();
    envelope.head.causation_id = Some("not-a-valid-ulid".to_string());

    let req = aw_test::TestRequest::default().to_http_request();

    let resp = intake(
        web::Json(envelope),
        web::Data::new(assigner),
        web::Data::new(dispatcher),
        web::Data::new(orchestrator),
        web::Data::new(diagnostics_actor),
        store_data,
        web::Data::new(None),
        disabled_metric_manager(),
        disabled_action_feed_publisher(),
    )
    .await;

    let response = resp.respond_to(&req);

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response.into_body();
    let bytes = match to_bytes(body).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read validation response body"),
    };
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("validation json");
    assert_eq!(json["error_code"], "validation_failed");
    assert_eq!(json["stage"], "pipeline.validation");
    let errors = json["errors"].as_array().expect("validation errors");
    assert!(errors.iter().any(|error| {
        error
            .as_str()
            .is_some_and(|message| message == "head.causation_id must be a valid ULID")
    }));

    let records = store.records.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].status, "validation_failed");
    assert!(records[0].has_errors);
}

#[actix_rt::test]
async fn intake_accepts_valid_payload() {
    let (assigner, dispatcher, orchestrator, diagnostics_actor) = actor_bundle();
    let (store, store_data) = matching_store_bundle();

    let req = aw_test::TestRequest::default().to_http_request();

    let input = valid_envelope();
    let expected_hash = compute_content_hash(&input.body);
    let resp = intake(
        web::Json(input),
        web::Data::new(assigner),
        web::Data::new(dispatcher),
        web::Data::new(orchestrator),
        web::Data::new(diagnostics_actor),
        store_data,
        web::Data::new(None),
        disabled_metric_manager(),
        web::Data::new(None::<Arc<dyn ActionFeedPublisher>>),
    )
    .await;

    let response = resp.respond_to(&req);

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body();
    let bytes = match to_bytes(body).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read response body"),
    };

    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("governance json");
    let events = json.as_array().expect("governance events array");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["body"]["content_hash"], expected_hash);
    assert_eq!(events[0]["body"]["status"], "PASS");
    assert_eq!(
        events[0]["body"]["final_decision_detail"]["reason_code"],
        "orchestrator.pass"
    );
    assert_eq!(
        events[0]["body"]["rule_evaluations"][0]["reason_code"],
        "rule.conditions_pass"
    );

    let records = store.records.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].status, "success");

    let event_logs = store.event_logs.lock().unwrap();
    assert!(event_logs.len() >= 5);
    assert!(
        event_logs
            .iter()
            .any(|log| log.event_id == "01ARZ3NDEKTSV4RRFFQ69G5FAV"
                && log.stage == "intake_api"
                && log.level == "INFO")
    );
    assert!(
        event_logs
            .iter()
            .any(|log| log.stage == "pipeline.dispatch" && log.level == "INFO")
    );
    assert!(
        event_logs
            .iter()
            .any(|log| log.stage == "pipeline.complete" && log.level == "INFO")
    );
    assert!(event_logs.iter().any(|log| {
        log.stage == "pipeline.rules"
            && log.level == "INFO"
            && log.message == "rule evaluation passed"
            && log
                .details
                .as_ref()
                .and_then(|details| details.get("reason_code"))
                .and_then(|value| value.as_str())
                .is_some()
    }));
}

#[actix_rt::test]
async fn intake_payload_rule_failure_generates_fail_and_action_metrics_in_response() {
    let (assigner, dispatcher, orchestrator, diagnostics_actor) = actor_bundle();
    let (store, store_data) = matching_store_bundle();
    let (metric_records, metric_manager) = metric_store_and_manager_bundle();
    let (action_records, action_builder) = action_builder_bundle();

    let mut input = valid_envelope();
    input.body = serde_json::json!({
        "snapshots": {
            "contract": {
                "object_id": "ctr_987",
                "amount": 999,
                "currency": "USD"
            }
        }
    });

    let req = aw_test::TestRequest::default().to_http_request();
    let resp = intake(
        web::Json(input),
        web::Data::new(assigner),
        web::Data::new(dispatcher),
        web::Data::new(orchestrator),
        web::Data::new(diagnostics_actor),
        store_data,
        action_builder,
        metric_manager,
        disabled_action_feed_publisher(),
    )
    .await;

    let response = resp.respond_to(&req);
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = match to_bytes(response.into_body()).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read fail response body"),
    };
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("governance json");
    let events = json.as_array().expect("governance events array");

    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["body"]["status"], "FAIL");
    assert_eq!(
        events[0]["body"]["rule_evaluations"][0]["reason_code"],
        "rule.conditions_fail"
    );
    assert_eq!(
        events[0]["body"]["rule_evaluations"][0]["checks"][0]["actual"],
        999
    );

    assert_eq!(
        events[1]["header"]["event_id"],
        "01ARZ3NDEKTSV4RRFFQ69G5FAV"
    );
    assert_eq!(events[1]["header"]["status"], "fail");
    let results = events[1]["results"].as_array().expect("metric results");
    assert!(!results.is_empty());
    let playbook = &results[0]["playbook"];
    assert_eq!(playbook["playbook_id"], "playbook.contract_governance");
    assert_eq!(playbook["playbook_name"], "Contract Governance");
    let rules = playbook["rules"].as_array().expect("rules");
    let rule_fail = rules
        .iter()
        .find(|r| r["metric_type"] == "RULE_FAIL")
        .expect("rule fail metric");
    assert_eq!(rule_fail["rule_id"], "rule.contract_created_check");
    assert_eq!(rule_fail["rule_name"], "Contract Amount Check");
    assert!(
        rule_fail["reason"]
            .as_str()
            .expect("metric reason")
            .contains("amount eq expected 12500, actual 999")
    );
    let actions = playbook["actions_triggered"]
        .as_array()
        .expect("actions triggered");
    assert!(!actions.is_empty());
    assert!(
        actions[0]["action_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty())
    );

    let created_actions = action_records.lock().unwrap();
    assert_eq!(created_actions.len(), 1);
    assert_eq!(
        actions[0]["action_id"].as_str(),
        Some(created_actions[0].action_id.as_str())
    );

    let persisted_metrics = metric_records.lock().unwrap();
    assert_eq!(persisted_metrics.len(), 2);
    assert!(
        persisted_metrics
            .iter()
            .any(|record| record.metric_type == METRIC_TYPE_RULE_FAIL
                && record.playbook_id.as_deref() == Some("playbook.contract_governance")
                && record.rule_id.as_deref() == Some("rule.contract_created_check"))
    );
    assert!(
        persisted_metrics
            .iter()
            .any(|record| record.metric_type == METRIC_TYPE_ACTION_TRIGGERED
                && record.action_id == Some(created_actions[0].action_id.clone()))
    );

    let records = store.records.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].status, "success");
    assert_eq!(records[0].response_event_count, Some(2));
}

#[actix_rt::test]
async fn intake_payload_metrics_include_all_evaluated_playbooks() {
    let req = aw_test::TestRequest::default().to_http_request();
    let (assigner, dispatcher, orchestrator, diagnostics_actor) = actor_bundle();
    let (store, _) = matching_store_bundle();
    let (metric_records, metric_manager) = metric_store_and_manager_bundle();

    let mut multi_store = store.clone();
    multi_store.extra_matched_playbooks = vec![DbPlaybook {
        id: "playbook.contract_followup".to_string(),
        name: Some("Contract Followup".to_string()),
        version: Some("1.0".to_string()),
        execution_mode: Some("Fail_First".to_string()),
        trigger: DbPlaybookTrigger {
            object_type: "Contract".to_string(),
            change_kind: vec!["created".to_string()],
        },
        rules: vec![DbPlaybookRuleRef {
            order_seq: 1,
            rule_id: "rule.contract_followup_check".to_string(),
            is_critical: true,
            action_template_id: None,
        }],
        status: Some("Active".to_string()),
    }];
    multi_store.rules.push(DbRule {
        id: "rule.contract_followup_check".to_string(),
        name: Some("Contract Followup Check".to_string()),
        object: DbRuleObject {
            object_type: "contract".to_string(),
        },
        conditions: vec![DbRuleCondition {
            object_key: "amount".to_string(),
            operator: "eq".to_string(),
            key_data_type: Some("NUMBER".to_string()),
            value: None,
            value_int: Some(12500),
            value_float: None,
            value_bool: None,
        }],
        logic: RuleLogic::All,
        status: Some("ACTIVE".to_string()),
    });
    let store_data: web::Data<Arc<dyn IntakeStore>> = web::Data::new(Arc::new(multi_store));

    let mut input = valid_envelope();
    input.body = serde_json::json!({
        "snapshots": {
            "contract": {
                "amount": 12500
            }
        }
    });

    let resp = intake(
        web::Json(input),
        web::Data::new(assigner),
        web::Data::new(dispatcher),
        web::Data::new(orchestrator),
        web::Data::new(diagnostics_actor),
        store_data,
        disabled_action_builder(),
        metric_manager,
        disabled_action_feed_publisher(),
    )
    .await;
    let response = resp.respond_to(&req);
    assert_eq!(response.status(), StatusCode::OK);

    let bytes = match to_bytes(response.into_body()).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read response body"),
    };
    let events: serde_json::Value = serde_json::from_slice(&bytes).expect("response json");
    let metric_event = &events.as_array().expect("events")[1];
    let results = metric_event["results"].as_array().expect("metric results");
    assert_eq!(results.len(), 2);

    let playbook_ids: Vec<&str> = results
        .iter()
        .filter_map(|result| result["playbook"]["playbook_id"].as_str())
        .collect();
    assert!(playbook_ids.contains(&"playbook.contract_governance"));
    assert!(playbook_ids.contains(&"playbook.contract_followup"));

    let persisted_metrics = metric_records.lock().unwrap();
    assert_eq!(persisted_metrics.len(), 2);
    assert!(
        persisted_metrics
            .iter()
            .all(|metric| metric.metric_type == METRIC_TYPE_RULE_PASS)
    );
}

#[actix_rt::test]
async fn intake_payload_missing_snapshot_generates_inconclusive_metric_in_response() {
    let (assigner, dispatcher, orchestrator, diagnostics_actor) = actor_bundle();
    let (store, store_data) = matching_store_bundle();
    let (metric_records, metric_manager) = metric_store_and_manager_bundle();
    let (action_records, action_builder) = action_builder_bundle();

    let mut input = valid_envelope();
    input.body = serde_json::json!({
        "snapshots": {
            "invoice": {
                "amount": 12500
            }
        }
    });

    let req = aw_test::TestRequest::default().to_http_request();
    let resp = intake(
        web::Json(input),
        web::Data::new(assigner),
        web::Data::new(dispatcher),
        web::Data::new(orchestrator),
        web::Data::new(diagnostics_actor),
        store_data,
        action_builder,
        metric_manager,
        disabled_action_feed_publisher(),
    )
    .await;

    let response = resp.respond_to(&req);
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = match to_bytes(response.into_body()).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read inconclusive response body"),
    };
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("governance json");
    let events = json.as_array().expect("governance events array");

    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["body"]["status"], "INCONCLUSIVE");
    assert_eq!(
        events[0]["body"]["rule_evaluations"][0]["reason_code"],
        "rule.snapshot_missing"
    );
    assert_eq!(
        events[1]["header"]["event_id"],
        "01ARZ3NDEKTSV4RRFFQ69G5FAV"
    );
    let results = events[1]["results"].as_array().expect("metric results");
    assert!(!results.is_empty());
    let rules = results[0]["playbook"]["rules"].as_array().expect("rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0]["metric_type"], "RULE_INCONCLUSIVE");
    let actions = results[0]["playbook"]["actions_triggered"]
        .as_array()
        .expect("actions");
    assert_eq!(actions.len(), 1);
    assert!(
        actions[0]["action_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty())
    );

    let created_actions = action_records.lock().unwrap();
    assert_eq!(created_actions.len(), 1);
    assert_eq!(
        actions[0]["action_id"].as_str(),
        Some(created_actions[0].action_id.as_str())
    );

    let persisted_metrics = metric_records.lock().unwrap();
    assert_eq!(persisted_metrics.len(), 2);
    assert!(
        persisted_metrics
            .iter()
            .any(|metric| metric.metric_type == METRIC_TYPE_RULE_INCONCLUSIVE)
    );
    assert!(
        persisted_metrics
            .iter()
            .any(|metric| metric.metric_type == METRIC_TYPE_ACTION_TRIGGERED
                && metric.action_id == Some(created_actions[0].action_id.clone()))
    );

    let records = store.records.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].response_event_count, Some(2));
}

#[actix_rt::test]
async fn intake_returns_inconclusive_when_no_playbook_matches() {
    let (assigner, dispatcher, orchestrator, diagnostics_actor) = actor_bundle();
    let (store, store_data) = no_match_store_bundle();

    let req = aw_test::TestRequest::default().to_http_request();

    let resp = intake(
        web::Json(valid_envelope()),
        web::Data::new(assigner),
        web::Data::new(dispatcher),
        web::Data::new(orchestrator),
        web::Data::new(diagnostics_actor),
        store_data,
        web::Data::new(None),
        disabled_metric_manager(),
        web::Data::new(None::<Arc<dyn ActionFeedPublisher>>),
    )
    .await;

    let response = resp.respond_to(&req);

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body();
    let bytes = match to_bytes(body).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read response body"),
    };

    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("governance json");
    let events = json.as_array().expect("governance events array");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["body"]["status"], "INCONCLUSIVE");

    let records = store.records.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].status, "success");
}

#[actix_rt::test]
async fn intake_skips_inactive_rules_from_evaluation() {
    let (assigner, dispatcher, orchestrator, diagnostics_actor) = actor_bundle();
    let (store, store_data) = inactive_rule_store_bundle();

    let req = aw_test::TestRequest::default().to_http_request();

    let resp = intake(
        web::Json(valid_envelope()),
        web::Data::new(assigner),
        web::Data::new(dispatcher),
        web::Data::new(orchestrator),
        web::Data::new(diagnostics_actor),
        store_data,
        disabled_action_builder(),
        disabled_metric_manager(),
        web::Data::new(None::<Arc<dyn ActionFeedPublisher>>),
    )
    .await;

    let response = resp.respond_to(&req);
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body();
    let bytes = match to_bytes(body).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read response body"),
    };

    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("governance json");
    let events = json.as_array().expect("governance events array");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["body"]["status"], "PASS");

    let rule_evaluations = events[0]["body"]["rule_evaluations"]
        .as_array()
        .expect("rule evaluations array");
    assert_eq!(rule_evaluations.len(), 1);
    assert_eq!(
        rule_evaluations[0]["rule_id"],
        "rule.contract_created_check"
    );
    assert_eq!(rule_evaluations[0]["result"], "PASS");
    assert_eq!(rule_evaluations[0]["reason_code"], "rule.conditions_pass");

    let records = store.records.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].status, "success");
}

#[actix_rt::test]
async fn diagnostics_endpoint_returns_snapshot() {
    let (_store, store_data) = matching_store_bundle();

    let req = aw_test::TestRequest::default().to_http_request();

    let resp = diagnostics(store_data).await;
    let response = resp.respond_to(&req);

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body();
    let bytes = match to_bytes(body).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read response body"),
    };

    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("diagnostics json");

    assert!(json.get("records").is_some());
}

#[actix_rt::test]
async fn metrics_endpoint_returns_all_metrics() {
    let req = aw_test::TestRequest::default().to_http_request();
    let (_store, store_data) = matching_store_bundle();
    let metric_manager = metric_manager_bundle(vec![
        sample_metric_record("evt_1", METRIC_TYPE_RULE_PASS, 1_000),
        sample_metric_record("evt_2", METRIC_TYPE_RULE_FAIL, 2_000),
        sample_metric_record("evt_2", METRIC_TYPE_ACTION_TRIGGERED, 3_000),
    ]);

    let resp = metrics(metric_manager, store_data).await;
    let response = resp.respond_to(&req);

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = match to_bytes(response.into_body()).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read metrics body"),
    };
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("metrics json");
    assert!(json.get("events").is_none());
    assert_eq!(json["header"]["event_name"], "Metrics");
    assert_eq!(json["header"]["event_id"], serde_json::Value::Null);
    assert_eq!(json["header"]["total_playbooks_triggered"], 1);
    assert_eq!(json["header"]["total_rules_triggered"], 2);
    assert_eq!(json["header"]["total_no_action_triggered"], 1);
    assert_eq!(json["header"]["total_no_of_success"], 1);
    assert_eq!(json["header"]["total_no_of_failure"], 1);
    let results = json["results"].as_array().expect("results");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0]["playbook"]["playbook_name"],
        "Contract Governance"
    );
    let rules = results[0]["playbook"]["rules"].as_array().expect("rules");
    assert_eq!(rules.len(), 2);
    assert!(rules.iter().any(|rule| rule["metric_type"] == "RULE_PASS"));
    assert!(rules.iter().any(|rule| rule["metric_type"] == "RULE_FAIL"));
    assert!(
        rules
            .iter()
            .all(|rule| rule["rule_name"] == "Contract Amount Check")
    );
    let actions = results[0]["playbook"]["actions_triggered"]
        .as_array()
        .expect("actions");
    assert_eq!(actions.len(), 1);
    let total_actions_rendered: usize = results
        .iter()
        .map(|result| {
            result["playbook"]["actions_triggered"]
                .as_array()
                .expect("actions_triggered")
                .len()
        })
        .sum();
    assert_eq!(
        json["header"]["total_no_action_triggered"],
        total_actions_rendered
    );
}

#[actix_rt::test]
async fn metrics_by_event_id_endpoint_filters_records() {
    let req = aw_test::TestRequest::default().to_http_request();
    let (_store, store_data) = matching_store_bundle();
    let metric_manager = metric_manager_bundle(vec![
        sample_metric_record("evt_1", METRIC_TYPE_RULE_PASS, 1_000),
        sample_metric_record("evt_2", METRIC_TYPE_RULE_FAIL, 2_000),
        sample_metric_record("evt_2", METRIC_TYPE_ACTION_TRIGGERED, 3_000),
    ]);

    let resp = metrics_by_event_id(
        web::Path::from("evt_2".to_string()),
        metric_manager,
        store_data,
    )
    .await;
    let response = resp.respond_to(&req);

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = match to_bytes(response.into_body()).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read metrics by event_id body"),
    };
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("metrics by event_id json");

    assert_eq!(json["header"]["event_id"], "evt_2");
    let results = json["results"].as_array().expect("results");
    assert_eq!(results.len(), 1);
    let playbook = &results[0]["playbook"];
    assert_eq!(playbook["playbook_id"], "playbook.contract_governance");
    assert_eq!(playbook["playbook_name"], "Contract Governance");
    let rules = playbook["rules"].as_array().expect("rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0]["metric_type"], "RULE_FAIL");
    assert_eq!(rules[0]["rule_name"], "Contract Amount Check");
    let actions = playbook["actions_triggered"].as_array().expect("actions");
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0]["status"], "CREATED");
}

#[actix_rt::test]
async fn event_logs_by_event_id_endpoint_filters_records() {
    let (store, store_data) = matching_store_bundle();
    let mut envelope = valid_envelope();
    envelope.head.event_id = "evt_1".to_string();
    store
        .append_event_log(
            &envelope,
            "pipeline.start",
            "INFO",
            "started",
            Some(&serde_json::json!({ "step": 1 })),
        )
        .await
        .expect("append first log");

    envelope.head.event_id = "evt_2".to_string();
    store
        .append_event_log(&envelope, "pipeline.start", "INFO", "started", None)
        .await
        .expect("append second log");
    store
        .append_event_log(&envelope, "pipeline.complete", "INFO", "completed", None)
        .await
        .expect("append third log");

    let req = aw_test::TestRequest::default().to_http_request();
    let resp = event_logs_by_event_id(web::Path::from("evt_2".to_string()), store_data).await;
    let response = resp.respond_to(&req);

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = match to_bytes(response.into_body()).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read event logs body"),
    };
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("event logs json");
    let records = json["records"].as_array().expect("event log records");

    assert_eq!(json["event_id"], "evt_2");
    assert_eq!(records.len(), 2);
    assert!(records.iter().all(|record| record["event_id"] == "evt_2"));
    assert_eq!(records[0]["stage"], "pipeline.start");
    assert_eq!(records[1]["stage"], "pipeline.complete");
}

#[actix_rt::test]
async fn metrics_endpoint_returns_service_unavailable_when_manager_disabled() {
    let req = aw_test::TestRequest::default().to_http_request();
    let (_store, store_data) = matching_store_bundle();
    let resp = metrics(disabled_metric_manager(), store_data).await;
    let response = resp.respond_to(&req);

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let bytes = match to_bytes(response.into_body()).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read disabled metrics body"),
    };
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("disabled metrics json");
    assert_standard_error(
        &json,
        "metrics_service_unavailable",
        "metrics.service",
        "metrics service is not available",
    );
}

#[actix_rt::test]
async fn metrics_endpoint_returns_internal_server_error_when_store_fails() {
    let req = aw_test::TestRequest::default().to_http_request();
    let (_store, store_data) = matching_store_bundle();
    let resp = metrics(
        failing_metric_manager_bundle(Some("metrics query failed"), None),
        store_data,
    )
    .await;
    let response = resp.respond_to(&req);

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let bytes = match to_bytes(response.into_body()).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read metrics error body"),
    };
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("metrics error json");
    assert_standard_error(
        &json,
        "metrics_query_failed",
        "metrics.query",
        "metrics database error: metrics query failed",
    );
}

#[actix_rt::test]
async fn metrics_by_event_id_endpoint_returns_internal_server_error_when_store_fails() {
    let req = aw_test::TestRequest::default().to_http_request();
    let (_store, store_data) = matching_store_bundle();
    let resp = metrics_by_event_id(
        web::Path::from("evt_1".to_string()),
        failing_metric_manager_bundle(None, Some("metrics by event query failed")),
        store_data,
    )
    .await;
    let response = resp.respond_to(&req);

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let bytes = match to_bytes(response.into_body()).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read metrics by event error body"),
    };
    let json: serde_json::Value =
        serde_json::from_slice(&bytes).expect("metrics by event error json");
    assert_standard_error(
        &json,
        "metrics_query_failed",
        "metrics.query",
        "metrics database error: metrics by event query failed",
    );
}

#[actix_rt::test]
async fn metrics_by_event_id_endpoint_returns_service_unavailable_when_manager_disabled() {
    let req = aw_test::TestRequest::default().to_http_request();
    let (_store, store_data) = matching_store_bundle();
    let resp = metrics_by_event_id(
        web::Path::from("evt_1".to_string()),
        disabled_metric_manager(),
        store_data,
    )
    .await;
    let response = resp.respond_to(&req);

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let bytes = match to_bytes(response.into_body()).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read disabled metrics by event body"),
    };
    let json: serde_json::Value =
        serde_json::from_slice(&bytes).expect("disabled metrics by event json");
    assert_standard_error(
        &json,
        "metrics_service_unavailable",
        "metrics.service",
        "metrics service is not available",
    );
}

#[actix_rt::test]
async fn action_templates_endpoint_returns_filtered_templates() {
    let req = aw_test::TestRequest::default().to_http_request();
    let mut archived = sample_action_template("tmpl-archived");
    archived.status = TemplateStatus::Archive;
    let resp = action_templates(
        web::Query::from_query(
            "tenant_id=acme&status=ACTIVE&object_type=Contract&event_type=RULE_FAILED&limit=5",
        )
        .expect("template query"),
        template_store_bundle(vec![
            sample_action_template("tmpl-contract-review"),
            archived,
        ]),
    )
    .await;
    let response = resp.respond_to(&req);

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = match to_bytes(response.into_body()).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read action templates body"),
    };
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("action templates json");
    let records = json["records"].as_array().expect("template records");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["template_id"], "tmpl-contract-review");
}

#[actix_rt::test]
async fn action_templates_endpoint_returns_service_unavailable_when_store_disabled() {
    let req = aw_test::TestRequest::default().to_http_request();
    let resp = action_templates(
        web::Query::from_query("").expect("empty query"),
        disabled_template_store(),
    )
    .await;
    let response = resp.respond_to(&req);

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let bytes = match to_bytes(response.into_body()).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read disabled action templates body"),
    };
    let json: serde_json::Value =
        serde_json::from_slice(&bytes).expect("disabled action templates json");
    assert_standard_error(
        &json,
        "action_template_service_unavailable",
        "action_templates.service",
        "action template service is not available",
    );
}

#[actix_rt::test]
async fn action_templates_endpoint_returns_internal_server_error_when_store_fails() {
    let req = aw_test::TestRequest::default().to_http_request();
    let resp = action_templates(
        web::Query::from_query("limit=10").expect("template query"),
        failing_template_store_bundle(Some("template query failed"), None),
    )
    .await;
    let response = resp.respond_to(&req);

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let bytes = match to_bytes(response.into_body()).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read action templates error body"),
    };
    let json: serde_json::Value =
        serde_json::from_slice(&bytes).expect("action templates error json");
    assert_standard_error(
        &json,
        "action_templates_query_failed",
        "action_templates.query",
        "action templates database error: template query failed",
    );
}

#[actix_rt::test]
async fn action_template_by_id_endpoint_returns_template() {
    let req = aw_test::TestRequest::default().to_http_request();
    let resp = action_template_by_id(
        web::Path::from("tmpl-contract-review".to_string()),
        template_store_bundle(vec![sample_action_template("tmpl-contract-review")]),
    )
    .await;
    let response = resp.respond_to(&req);

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = match to_bytes(response.into_body()).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read action template body"),
    };
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("action template json");
    assert_eq!(json["template_id"], "tmpl-contract-review");
}

#[actix_rt::test]
async fn action_template_by_id_endpoint_returns_not_found() {
    let req = aw_test::TestRequest::default().to_http_request();
    let resp = action_template_by_id(
        web::Path::from("missing-template".to_string()),
        template_store_bundle(vec![sample_action_template("tmpl-contract-review")]),
    )
    .await;
    let response = resp.respond_to(&req);

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let bytes = match to_bytes(response.into_body()).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read missing action template body"),
    };
    let json: serde_json::Value =
        serde_json::from_slice(&bytes).expect("missing action template json");
    assert_standard_error(
        &json,
        "action_template_not_found",
        "action_templates.lookup",
        "action template not found: missing-template",
    );
}

#[actix_rt::test]
async fn action_template_by_id_endpoint_returns_service_unavailable_when_store_disabled() {
    let req = aw_test::TestRequest::default().to_http_request();
    let resp = action_template_by_id(
        web::Path::from("tmpl-contract-review".to_string()),
        disabled_template_store(),
    )
    .await;
    let response = resp.respond_to(&req);

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let bytes = match to_bytes(response.into_body()).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read disabled action template lookup body"),
    };
    let json: serde_json::Value =
        serde_json::from_slice(&bytes).expect("disabled action template lookup json");
    assert_standard_error(
        &json,
        "action_template_service_unavailable",
        "action_templates.service",
        "action template service is not available",
    );
}

#[actix_rt::test]
async fn action_template_by_id_endpoint_returns_internal_server_error_when_store_fails() {
    let req = aw_test::TestRequest::default().to_http_request();
    let resp = action_template_by_id(
        web::Path::from("tmpl-contract-review".to_string()),
        failing_template_store_bundle(None, Some("template lookup failed")),
    )
    .await;
    let response = resp.respond_to(&req);

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let bytes = match to_bytes(response.into_body()).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read action template lookup error body"),
    };
    let json: serde_json::Value =
        serde_json::from_slice(&bytes).expect("action template lookup error json");
    assert_standard_error(
        &json,
        "action_template_lookup_failed",
        "action_templates.lookup",
        "action template database error: template lookup failed",
    );
}

#[actix_rt::test]
async fn intake_persists_to_mongodb_when_configured() {
    let Ok(uri) = std::env::var("MONGODB_URI") else {
        return;
    };
    let db_name = std::env::var("MONGODB_DB").unwrap_or_else(|_| "devdb".to_string());
    let collection_name =
        std::env::var("MONGODB_COLLECTION").unwrap_or_else(|_| "intake_events".to_string());

    let store = MongoStore::from_env().await.expect("mongo store");
    let mut envelope = valid_envelope();
    envelope.head.event_id = UlidService::new().generate_string();

    store
        .record_intake(&envelope, "success", None, None, None)
        .await
        .expect("mongo insert");

    let client = Client::with_uri_str(&uri).await.expect("mongo client");
    let collection = client
        .database(&db_name)
        .collection::<mongodb::bson::Document>(&collection_name);
    let found = collection
        .find_one(doc! { "event_id": &envelope.head.event_id }, None)
        .await
        .expect("mongo find");

    assert!(found.is_some());
}

#[actix_rt::test]
async fn intake_endpoint_persists_input_and_output_to_mongodb_when_configured() {
    let Ok(uri) = std::env::var("MONGODB_URI") else {
        return;
    };
    let db_name = std::env::var("MONGODB_DB").unwrap_or_else(|_| "devdb".to_string());
    let collection_name =
        std::env::var("MONGODB_COLLECTION").unwrap_or_else(|_| "intake_events".to_string());

    let store = MongoStore::from_env().await.expect("mongo store");
    let store_data: web::Data<Arc<dyn IntakeStore>> = web::Data::new(Arc::new(store));

    let (assigner, dispatcher, orchestrator, diagnostics_actor) = actor_bundle();
    let req = aw_test::TestRequest::default().to_http_request();

    let mut envelope = valid_envelope();
    envelope.head.event_id = UlidService::new().generate_string();

    let resp = intake(
        web::Json(envelope.clone()),
        web::Data::new(assigner),
        web::Data::new(dispatcher),
        web::Data::new(orchestrator),
        web::Data::new(diagnostics_actor),
        store_data,
        disabled_action_builder(),
        disabled_metric_manager(),
        web::Data::new(None::<Arc<dyn ActionFeedPublisher>>),
    )
    .await;

    let response = resp.respond_to(&req);
    assert_eq!(response.status(), StatusCode::OK);

    let client = Client::with_uri_str(&uri).await.expect("mongo client");
    let collection = client
        .database(&db_name)
        .collection::<mongodb::bson::Document>(&collection_name);
    let found = collection
        .find_one(doc! { "event_id": &envelope.head.event_id }, None)
        .await
        .expect("mongo find")
        .expect("intake record");

    assert!(found.get("envelope").is_some());
    assert!(found.get("response").is_some());
    assert_eq!(found.get_str("status").expect("status"), "success");
}

#[actix_rt::test]
async fn diagnostics_endpoint_returns_internal_server_error_when_store_fails() {
    let failing: web::Data<Arc<dyn IntakeStore>> = web::Data::new(Arc::new(ErrorStore {
        find_playbook_error: None,
        find_rules_error: None,
        list_recent_error: Some("db unavailable".to_string()),
        list_event_logs_error: None,
    }));

    let req = aw_test::TestRequest::default().to_http_request();
    let resp = diagnostics(failing).await;
    let response = resp.respond_to(&req);
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let bytes = match to_bytes(response.into_body()).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read diagnostics error body"),
    };
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("diagnostics error json");
    assert_standard_error(
        &json,
        "diagnostics_query_failed",
        "diagnostics.query",
        "diagnostics database error: db unavailable",
    );
}

#[actix_rt::test]
async fn event_logs_by_event_id_endpoint_returns_internal_server_error_when_store_fails() {
    let failing: web::Data<Arc<dyn IntakeStore>> = web::Data::new(Arc::new(ErrorStore {
        find_playbook_error: None,
        find_rules_error: None,
        list_recent_error: None,
        list_event_logs_error: Some("event logs query failed".to_string()),
    }));

    let req = aw_test::TestRequest::default().to_http_request();
    let resp = event_logs_by_event_id(web::Path::from("evt_1".to_string()), failing).await;
    let response = resp.respond_to(&req);
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let bytes = match to_bytes(response.into_body()).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read event logs error body"),
    };
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("event logs error json");
    assert_standard_error(
        &json,
        "event_logs_query_failed",
        "event_logs.query",
        "event logs database error: event logs query failed",
    );
}

#[actix_rt::test]
async fn intake_returns_internal_server_error_when_playbook_lookup_fails() {
    let (assigner, dispatcher, orchestrator, diagnostics_actor) = actor_bundle();
    let failing: web::Data<Arc<dyn IntakeStore>> = web::Data::new(Arc::new(ErrorStore {
        find_playbook_error: Some("playbooks query failed".to_string()),
        find_rules_error: None,
        list_recent_error: None,
        list_event_logs_error: None,
    }));

    let req = aw_test::TestRequest::default().to_http_request();
    let resp = intake(
        web::Json(valid_envelope()),
        web::Data::new(assigner),
        web::Data::new(dispatcher),
        web::Data::new(orchestrator),
        web::Data::new(diagnostics_actor),
        failing,
        disabled_action_builder(),
        disabled_metric_manager(),
        web::Data::new(None::<Arc<dyn ActionFeedPublisher>>),
    )
    .await;
    let response = resp.respond_to(&req);
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = response.into_body();
    let bytes = match to_bytes(body).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read internal error response body"),
    };
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("error json");
    assert_eq!(json["error_code"], "assigner_failed");
    assert_eq!(json["stage"], "pipeline.assign");
    assert_eq!(
        json["error"],
        "playbook lookup error: playbooks query failed"
    );
    assert!(json["description"].as_str().is_some());
}

#[actix_rt::test]
async fn intake_returns_internal_server_error_when_rule_lookup_fails() {
    let (assigner, dispatcher, orchestrator, diagnostics_actor) = actor_bundle();
    let failing: web::Data<Arc<dyn IntakeStore>> = web::Data::new(Arc::new(ErrorStore {
        find_playbook_error: None,
        find_rules_error: Some("rules query failed".to_string()),
        list_recent_error: None,
        list_event_logs_error: None,
    }));

    let req = aw_test::TestRequest::default().to_http_request();
    let resp = intake(
        web::Json(valid_envelope()),
        web::Data::new(assigner),
        web::Data::new(dispatcher),
        web::Data::new(orchestrator),
        web::Data::new(diagnostics_actor),
        failing,
        disabled_action_builder(),
        disabled_metric_manager(),
        web::Data::new(None::<Arc<dyn ActionFeedPublisher>>),
    )
    .await;
    let response = resp.respond_to(&req);

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let bytes = match to_bytes(response.into_body()).await {
        Ok(b) => b,
        Err(_) => panic!("failed to read rule lookup error response body"),
    };
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("error json");
    assert_eq!(json["error_code"], "assigner_failed");
    assert_eq!(json["stage"], "pipeline.assign");
    assert_eq!(json["error"], "rule lookup error: rules query failed");
    assert!(json["description"].as_str().is_some());
}

#[actix_rt::test]
async fn process_envelope_returns_assigner_error_when_store_is_missing() {
    let (assigner, dispatcher, orchestrator, diagnostics_actor) = actor_bundle();
    let result = process_envelope(
        valid_envelope(),
        assigner,
        dispatcher,
        orchestrator,
        diagnostics_actor,
        None,
        None,
        None,
        None,
    )
    .await;

    match result {
        Err(PipelineError::AssignerFailed(msg)) => {
            assert_eq!(msg, "intake store is required for playbook/rule lookup")
        }
        other => panic!("expected assigner error, got {other:?}"),
    }
}

#[actix_rt::test]
async fn bulk_intake_captures_resource_utilization_and_performance_observations() {
    let event_count = 100;
    let (assigner, dispatcher, orchestrator, diagnostics_actor) = actor_bundle();
    let (store, store_data) = matching_store_bundle();
    let (metric_records, metric_manager) = metric_store_and_manager_bundle();
    let ulids = UlidService::new();
    let mut total_response_bytes = 0;

    let resource_start = current_resource_snapshot();
    let started_at = Instant::now();
    for index in 0..event_count {
        let mut envelope = valid_envelope();
        envelope.head.event_id = ulids.generate_string();
        envelope.body["snapshots"]["contract"]["object_id"] =
            serde_json::json!(format!("ctr_bulk_{index:03}"));

        let response = process_envelope(
            envelope,
            assigner.clone(),
            dispatcher.clone(),
            orchestrator.clone(),
            diagnostics_actor.clone(),
            Some(store_data.get_ref().clone()),
            None,
            metric_manager.get_ref().clone(),
            None,
        )
        .await
        .expect("bulk envelope should process");

        assert_eq!(response.len(), 2);
        assert_eq!(response[0]["body"]["status"], "PASS");
        let results = response[1]["results"].as_array().expect("metric results");
        assert_eq!(results.len(), 1);
        let rules = results[0]["playbook"]["rules"]
            .as_array()
            .expect("bulk rules");
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0]["metric_type"], "RULE_PASS");
        total_response_bytes += serde_json::to_vec(&response)
            .expect("bulk response serialization")
            .len();
    }
    let total_elapsed_ms = started_at.elapsed().as_millis();
    let resource_end = current_resource_snapshot();
    let resources = ResourceObservation::from_snapshots(resource_start, resource_end);

    let persisted_intake_records = store.records.lock().unwrap().len();
    let persisted_event_logs = store.event_logs.lock().unwrap().len();
    let emitted_metric_records = metric_records.lock().unwrap().len();
    let observations = BulkTestObservation::from_run(
        event_count,
        total_elapsed_ms,
        resources,
        persisted_intake_records,
        persisted_event_logs,
        emitted_metric_records,
        total_response_bytes,
    );

    eprintln!("bulk test observations: {observations:?}");

    assert_eq!(observations.event_count, event_count);
    assert_eq!(observations.persisted_intake_records, event_count);
    assert!(observations.persisted_event_logs >= event_count * 5);
    assert_eq!(observations.emitted_metric_records, event_count);
    #[cfg(unix)]
    assert!(observations.max_rss_kib > 0);
    #[cfg(unix)]
    assert!(observations.max_rss_kib >= observations.max_rss_delta_kib);
    #[cfg(unix)]
    assert!(observations.user_cpu_us + observations.system_cpu_us > 0);
    assert!(observations.total_response_bytes > event_count * 500);
    assert!(observations.avg_latency_us < 50_000);
    assert!(observations.throughput_per_sec > 20.0);
    assert!(observations.total_elapsed_ms < 5_000);
}

#[test]
fn validate_envelope_rejects_invalid_change_kind() {
    let mut envelope = valid_envelope();
    envelope.head.change_kind = Some("moved".to_string());

    let errors = validate_envelope(&envelope);
    assert!(
        errors
            .iter()
            .any(|e| e.contains("head.change_kind must be"))
    );
}
