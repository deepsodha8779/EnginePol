use actix::prelude::*;
use domain::envelope::{CanonicalEnvelope, TadpoleHead};
use engine_core::actors::orchestrator::OrchestratorActor;
use engine_core::dto::{
    decision::Decision, evaluation::RuleEvaluation, orchestrator::Orchestrate,
    rules::PlaybookAssignment, tadpole::Tadpole,
};

fn base_tadpole() -> Tadpole {
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

    tadpole.tail.assigned_playbooks = vec![PlaybookAssignment {
        playbook_id: "playbook.contract_governance".to_string(),
        reason: "matched".to_string(),
    }];

    tadpole
}

#[test]
fn orchestrator_returns_fail_and_action_candidate() {
    let mut tadpole = base_tadpole();
    tadpole.tail.evaluations = vec![RuleEvaluation {
        playbook_id: "playbook.contract_governance".to_string(),
        rule_id: "rule.boolean".to_string(),
        rule_name: None,
        object_type: None,
        order_seq: None,
        is_critical: false,
        priority: None,
        decision: Decision::fail("test.fail", "failed"),
        reason_code: "test.fail".to_string(),
        reason: "failed".to_string(),
        checks: Vec::new(),
        duration_ms: 5,
        action_template_id: None,
    }];

    let result = System::new().block_on(async move {
        let addr = OrchestratorActor.start();
        addr.send(Orchestrate { tadpole })
            .await
            .expect("orchestrator failed")
    });

    assert!(result.decision.is_fail());
    assert_eq!(result.action_candidates.len(), 1);
    assert_eq!(result.action_candidates[0].reason, "decision=FAIL");
}

#[test]
fn orchestrator_returns_inconclusive_when_any_inconclusive() {
    let mut tadpole = base_tadpole();
    tadpole.tail.evaluations = vec![RuleEvaluation {
        playbook_id: "playbook.contract_governance".to_string(),
        rule_id: "rule.boolean".to_string(),
        rule_name: None,
        object_type: None,
        order_seq: None,
        is_critical: false,
        priority: None,
        decision: Decision::inconclusive("test.inconclusive", "unknown"),
        reason_code: "test.inconclusive".to_string(),
        reason: "unknown".to_string(),
        checks: Vec::new(),
        duration_ms: 5,
        action_template_id: None,
    }];

    let result = System::new().block_on(async move {
        let addr = OrchestratorActor.start();
        addr.send(Orchestrate { tadpole })
            .await
            .expect("orchestrator failed")
    });

    assert!(result.decision.is_inconclusive());
    assert!(result.action_candidates.is_empty());
}

#[test]
fn orchestrator_returns_pass_when_all_pass() {
    let mut tadpole = base_tadpole();
    tadpole.tail.evaluations = vec![
        RuleEvaluation {
            playbook_id: "playbook.contract_governance".to_string(),
            rule_id: "rule.boolean".to_string(),
            rule_name: None,
            object_type: None,
            order_seq: None,
            is_critical: false,
            priority: None,
            decision: Decision::pass("test.pass", "ok"),
            reason_code: "test.pass".to_string(),
            reason: "ok".to_string(),
            checks: Vec::new(),
            duration_ms: 5,
            action_template_id: None,
        },
        RuleEvaluation {
            playbook_id: "playbook.contract_governance".to_string(),
            rule_id: "rule.boolean_2".to_string(),
            rule_name: None,
            object_type: None,
            order_seq: None,
            is_critical: false,
            priority: None,
            decision: Decision::pass("test.pass", "ok"),
            reason_code: "test.pass".to_string(),
            reason: "ok".to_string(),
            checks: Vec::new(),
            duration_ms: 5,
            action_template_id: None,
        },
    ];

    let result = System::new().block_on(async move {
        let addr = OrchestratorActor.start();
        addr.send(Orchestrate { tadpole })
            .await
            .expect("orchestrator failed")
    });

    assert!(result.decision.is_pass());
    assert!(result.action_candidates.is_empty());
}

#[test]
fn orchestrator_routes_to_action_builder_on_fail() {
    let mut tadpole = base_tadpole();
    tadpole.tail.evaluations = vec![RuleEvaluation {
        playbook_id: "playbook.contract_governance".to_string(),
        rule_id: "rule.boolean".to_string(),
        rule_name: None,
        object_type: None,
        order_seq: None,
        is_critical: false,
        priority: None,
        decision: Decision::fail("test.fail", "failed"),
        reason_code: "test.fail".to_string(),
        reason: "failed".to_string(),
        checks: Vec::new(),
        duration_ms: 5,
        action_template_id: Some("tmpl_1".to_string()),
    }];

    let result = System::new().block_on(async move {
        let addr = OrchestratorActor.start();
        addr.send(Orchestrate { tadpole })
            .await
            .expect("orchestrator failed")
    });

    assert!(result.decision.is_fail());
    assert!(result.route_to_action_builder);
}

#[test]
fn orchestrator_skips_action_builder_when_all_pass() {
    let mut tadpole = base_tadpole();
    tadpole.tail.evaluations = vec![RuleEvaluation {
        playbook_id: "playbook.contract_governance".to_string(),
        rule_id: "rule.boolean".to_string(),
        rule_name: None,
        object_type: None,
        order_seq: None,
        is_critical: false,
        priority: None,
        decision: Decision::pass("test.pass", "ok"),
        reason_code: "test.pass".to_string(),
        reason: "ok".to_string(),
        checks: Vec::new(),
        duration_ms: 5,
        action_template_id: None,
    }];

    let result = System::new().block_on(async move {
        let addr = OrchestratorActor.start();
        addr.send(Orchestrate { tadpole })
            .await
            .expect("orchestrator failed")
    });

    assert!(result.decision.is_pass());
    assert!(result.route_to_action_builder);
}

#[test]
fn orchestrator_returns_pass_with_no_evaluations() {
    let tadpole = base_tadpole();

    let result = System::new().block_on(async move {
        let addr = OrchestratorActor.start();
        addr.send(Orchestrate { tadpole })
            .await
            .expect("orchestrator failed")
    });

    assert!(result.decision.is_pass());
    assert!(result.action_candidates.is_empty());
}
