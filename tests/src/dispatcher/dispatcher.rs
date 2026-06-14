use actix::prelude::*;
use domain::envelope::{CanonicalEnvelope, TadpoleHead};
use engine_core::actors::dispatcher::{
    BooleanRuleEvaluator, DispatcherActor, EnrichmentStubEvaluator, RuleEvalFuture, RuleEvaluator,
};
use engine_core::actors::handlers::boolean_handler::BooleanRuleHandler;
use engine_core::dto::{
    decision::Decision,
    dispatcher::Dispatch,
    evaluation::RuleEvaluation,
    rules::{RuleKind, RuleLogic, RuleSpec},
    tadpole::Tadpole,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

fn sample_tadpole_with_rules() -> Tadpole {
    let mut tadpole = Tadpole::from_envelope(CanonicalEnvelope {
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
            changed_object_type: None,
            changed_object_id: None,
            change_kind: None,
        },
        body: serde_json::json!({}),
    });

    tadpole.tail.ordered_rules = vec![
        RuleSpec {
            playbook_id: "playbook.contract_governance".to_string(),
            rule_id: "rule.boolean".to_string(),
            kind: RuleKind::Boolean,
            expr: Some("event_name == \"ContractCreated\"".to_string()),
            rule_name: None,
            object_type: None,
            order_seq: None,
            priority: None,
            conditions: Vec::new(),
            logic: RuleLogic::All,
            is_critical: false,
            skip_reason: None,
            action_template_id: None,
        },
        RuleSpec {
            playbook_id: "playbook.contract_governance".to_string(),
            rule_id: "rule.enrichment".to_string(),
            kind: RuleKind::EnrichmentStub,
            expr: None,
            rule_name: None,
            object_type: None,
            order_seq: None,
            priority: None,
            conditions: Vec::new(),
            logic: RuleLogic::All,
            is_critical: false,
            skip_reason: None,
            action_template_id: None,
        },
    ];

    tadpole
}

#[test]
fn dispatcher_executes_boolean_and_enrichment_rules() {
    let result = System::new().block_on(async move {
        let boolean_handler = BooleanRuleHandler.start();
        let mut evaluators: HashMap<RuleKind, Arc<dyn RuleEvaluator>> = HashMap::new();
        evaluators.insert(
            RuleKind::Boolean,
            Arc::new(BooleanRuleEvaluator::new(boolean_handler)),
        );
        evaluators.insert(RuleKind::EnrichmentStub, Arc::new(EnrichmentStubEvaluator));
        let dispatcher = DispatcherActor { evaluators }.start();

        dispatcher
            .send(Dispatch {
                tadpole: sample_tadpole_with_rules(),
            })
            .await
            .expect("dispatcher failed")
    });

    assert_eq!(result.tail.evaluations.len(), 2);
    assert_eq!(result.tail.evaluations[0].rule_id, "rule.boolean");
    assert!(result.tail.evaluations[0].decision.is_pass());
    assert_eq!(result.tail.evaluations[1].rule_id, "rule.enrichment");
    assert!(result.tail.evaluations[1].decision.is_inconclusive());
    assert_eq!(
        result.tail.evaluations[1].reason,
        "enrichment stub not implemented"
    );
}

struct FakeEvaluator {
    calls: Arc<Mutex<Vec<String>>>,
    decision: Decision,
}

impl RuleEvaluator for FakeEvaluator {
    fn evaluate(&self, _tadpole: Tadpole, rule: RuleSpec) -> RuleEvalFuture {
        let calls = Arc::clone(&self.calls);
        let decision = self.decision.clone();
        Box::pin(async move {
            calls
                .lock()
                .expect("lock fake evaluator")
                .push(rule.rule_id.clone());
            RuleEvaluation {
                playbook_id: rule.playbook_id,
                rule_id: rule.rule_id,
                rule_name: None,
                object_type: None,
                order_seq: None,
                is_critical: rule.is_critical,
                priority: rule.priority,
                decision,
                reason_code: "test.fake".to_string(),
                reason: "fake".to_string(),
                checks: Vec::new(),
                duration_ms: 0,
                action_template_id: rule.action_template_id,
            }
        })
    }
}

#[actix_rt::test]
async fn dispatcher_preserves_rule_order_with_fake_handler() {
    let calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let calls_for_handler = Arc::clone(&calls);

    let mut tadpole = sample_tadpole_with_rules();
    tadpole.tail.ordered_rules = vec![
        RuleSpec {
            playbook_id: "playbook.contract_governance".to_string(),
            rule_id: "rule.first".to_string(),
            kind: RuleKind::Boolean,
            expr: Some("event_name == \"ContractCreated\"".to_string()),
            rule_name: None,
            object_type: None,
            order_seq: None,
            priority: None,
            conditions: Vec::new(),
            logic: RuleLogic::All,
            is_critical: false,
            skip_reason: None,
            action_template_id: None,
        },
        RuleSpec {
            playbook_id: "playbook.contract_governance".to_string(),
            rule_id: "rule.second".to_string(),
            kind: RuleKind::Boolean,
            expr: Some("event_name == \"ContractCreated\"".to_string()),
            rule_name: None,
            object_type: None,
            order_seq: None,
            priority: None,
            conditions: Vec::new(),
            logic: RuleLogic::All,
            is_critical: false,
            skip_reason: None,
            action_template_id: None,
        },
        RuleSpec {
            playbook_id: "playbook.contract_governance".to_string(),
            rule_id: "rule.third".to_string(),
            kind: RuleKind::Boolean,
            expr: Some("event_name == \"ContractCreated\"".to_string()),
            rule_name: None,
            object_type: None,
            order_seq: None,
            priority: None,
            conditions: Vec::new(),
            logic: RuleLogic::All,
            is_critical: false,
            skip_reason: None,
            action_template_id: None,
        },
    ];

    let mut evaluators: HashMap<RuleKind, Arc<dyn RuleEvaluator>> = HashMap::new();
    evaluators.insert(
        RuleKind::Boolean,
        Arc::new(FakeEvaluator {
            calls: calls_for_handler,
            decision: Decision::pass("test.fake", "fake"),
        }),
    );
    let dispatcher = DispatcherActor { evaluators }.start();

    dispatcher
        .send(Dispatch { tadpole })
        .await
        .expect("dispatcher failed");

    let recorded = calls.lock().expect("lock calls").clone();
    assert_eq!(recorded, vec!["rule.first", "rule.second", "rule.third"]);
}

#[actix_rt::test]
async fn dispatcher_marks_skip_reason_as_inconclusive_without_handler_call() {
    let calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let calls_for_handler = Arc::clone(&calls);

    let mut tadpole = sample_tadpole_with_rules();
    tadpole.tail.ordered_rules = vec![RuleSpec {
        playbook_id: "playbook.contract_governance".to_string(),
        rule_id: "rule.skipped".to_string(),
        kind: RuleKind::Boolean,
        expr: Some("event_name == \"ContractCreated\"".to_string()),
        rule_name: None,
        object_type: None,
        order_seq: Some(1),
        priority: Some("NORMAL".to_string()),
        conditions: Vec::new(),
        logic: RuleLogic::All,
        is_critical: false,
        skip_reason: Some("already evaluated upstream".to_string()),
        action_template_id: None,
    }];

    let mut evaluators: HashMap<RuleKind, Arc<dyn RuleEvaluator>> = HashMap::new();
    evaluators.insert(
        RuleKind::Boolean,
        Arc::new(FakeEvaluator {
            calls: calls_for_handler,
            decision: Decision::pass("test.fake", "fake"),
        }),
    );
    let dispatcher = DispatcherActor { evaluators }.start();

    let result = dispatcher
        .send(Dispatch { tadpole })
        .await
        .expect("dispatcher failed");

    assert_eq!(result.tail.evaluations.len(), 1);
    assert!(result.tail.evaluations[0].decision.is_inconclusive());
    assert_eq!(result.tail.evaluations[0].reason_code, "dispatcher.skipped");
    assert!(calls.lock().expect("lock calls").is_empty());
}

#[actix_rt::test]
async fn dispatcher_marks_missing_handler_as_inconclusive() {
    let mut tadpole = sample_tadpole_with_rules();
    tadpole.tail.ordered_rules = vec![RuleSpec {
        playbook_id: "playbook.contract_governance".to_string(),
        rule_id: "rule.no_handler".to_string(),
        kind: RuleKind::Boolean,
        expr: Some("event_name == \"ContractCreated\"".to_string()),
        rule_name: None,
        object_type: None,
        order_seq: Some(1),
        priority: Some("NORMAL".to_string()),
        conditions: Vec::new(),
        logic: RuleLogic::All,
        is_critical: false,
        skip_reason: None,
        action_template_id: None,
    }];

    let dispatcher = DispatcherActor {
        evaluators: HashMap::new(),
    }
    .start();

    let result = dispatcher
        .send(Dispatch { tadpole })
        .await
        .expect("dispatcher failed");

    assert_eq!(result.tail.evaluations.len(), 1);
    assert!(result.tail.evaluations[0].decision.is_inconclusive());
    assert_eq!(
        result.tail.evaluations[0].reason_code,
        "dispatcher.no_handler"
    );
}

#[actix_rt::test]
async fn dispatcher_stops_on_fail_first_critical_failure() {
    let calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let calls_for_handler = Arc::clone(&calls);

    let mut tadpole = sample_tadpole_with_rules();
    tadpole.tail.execution_mode = Some("Fail_First".to_string());
    tadpole.tail.ordered_rules = vec![
        RuleSpec {
            playbook_id: "playbook.contract_governance".to_string(),
            rule_id: "rule.critical".to_string(),
            kind: RuleKind::Boolean,
            expr: Some("event_name == \"ContractCreated\"".to_string()),
            rule_name: None,
            object_type: None,
            order_seq: Some(1),
            priority: Some("HIGH".to_string()),
            conditions: Vec::new(),
            logic: RuleLogic::All,
            is_critical: true,
            skip_reason: None,
            action_template_id: None,
        },
        RuleSpec {
            playbook_id: "playbook.contract_governance".to_string(),
            rule_id: "rule.should_not_execute".to_string(),
            kind: RuleKind::Boolean,
            expr: Some("event_name == \"ContractCreated\"".to_string()),
            rule_name: None,
            object_type: None,
            order_seq: Some(2),
            priority: Some("NORMAL".to_string()),
            conditions: Vec::new(),
            logic: RuleLogic::All,
            is_critical: false,
            skip_reason: None,
            action_template_id: None,
        },
    ];

    let mut evaluators: HashMap<RuleKind, Arc<dyn RuleEvaluator>> = HashMap::new();
    evaluators.insert(
        RuleKind::Boolean,
        Arc::new(FakeEvaluator {
            calls: calls_for_handler,
            decision: Decision::fail("test.fake_fail", "fail"),
        }),
    );
    let dispatcher = DispatcherActor { evaluators }.start();

    let result = dispatcher
        .send(Dispatch { tadpole })
        .await
        .expect("dispatcher failed");

    assert_eq!(result.tail.evaluations.len(), 1);
    assert!(result.tail.evaluations[0].decision.is_fail());
    let recorded = calls.lock().expect("lock calls").clone();
    assert_eq!(recorded, vec!["rule.critical"]);
}
