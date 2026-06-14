use actix::prelude::*;
use domain::envelope::{CanonicalEnvelope, TadpoleHead};
use engine_core::actors::handlers::boolean_handler::BooleanRuleHandler;
use engine_core::dto::{
    boolean_handler::EvaluateBooleanRule,
    rules::{RuleCondition, RuleLogic, RuleSpec},
    tadpole::Tadpole,
};

fn sample_tadpole() -> Tadpole {
    Tadpole::from_envelope(CanonicalEnvelope {
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
    })
}

async fn evaluate(expr: Option<&str>) -> engine_core::dto::evaluation::RuleEvaluation {
    let rule = RuleSpec {
        playbook_id: "playbook.test".to_string(),
        rule_id: "rule.boolean".to_string(),
        kind: engine_core::dto::rules::RuleKind::Boolean,
        expr: expr.map(|value| value.to_string()),
        rule_name: None,
        object_type: None,
        order_seq: None,
        priority: None,
        conditions: Vec::new(),
        logic: RuleLogic::All,
        is_critical: false,
        skip_reason: None,
        action_template_id: None,
    };
    let addr = BooleanRuleHandler.start();
    addr.send(EvaluateBooleanRule {
        tadpole: sample_tadpole(),
        rule,
    })
    .await
    .expect("boolean handler failed")
}

async fn evaluate_snapshot_condition(
    snapshots: serde_json::Value,
    object_type: &str,
    object_key: &str,
    operator: &str,
    key_data_type: Option<&str>,
    expected: serde_json::Value,
) -> engine_core::dto::evaluation::RuleEvaluation {
    let mut tadpole = sample_tadpole();
    tadpole.body = serde_json::json!({ "snapshots": snapshots });

    let rule = RuleSpec {
        playbook_id: "playbook.test".to_string(),
        rule_id: "rule.snapshot".to_string(),
        kind: engine_core::dto::rules::RuleKind::Boolean,
        expr: None,
        rule_name: Some("Snapshot Condition".to_string()),
        object_type: Some(object_type.to_string()),
        order_seq: Some(1),
        priority: Some("NORMAL".to_string()),
        conditions: vec![RuleCondition {
            object_key: object_key.to_string(),
            operator: operator.to_string(),
            key_data_type: key_data_type.map(str::to_string),
            value: Some(expected),
        }],
        logic: RuleLogic::All,
        is_critical: false,
        skip_reason: None,
        action_template_id: None,
    };

    let addr = BooleanRuleHandler.start();
    addr.send(EvaluateBooleanRule { tadpole, rule })
        .await
        .expect("boolean handler failed")
}

async fn evaluate_snapshot_conditions_with_rule(
    snapshots: serde_json::Value,
    object_type: &str,
    conditions: Vec<RuleCondition>,
    logic: RuleLogic,
) -> engine_core::dto::evaluation::RuleEvaluation {
    let mut tadpole = sample_tadpole();
    tadpole.body = serde_json::json!({ "snapshots": snapshots });

    let rule = RuleSpec {
        playbook_id: "playbook.test".to_string(),
        rule_id: "rule.snapshot".to_string(),
        kind: engine_core::dto::rules::RuleKind::Boolean,
        expr: None,
        rule_name: Some("Snapshot Condition".to_string()),
        object_type: Some(object_type.to_string()),
        order_seq: Some(1),
        priority: Some("NORMAL".to_string()),
        conditions,
        logic,
        is_critical: false,
        skip_reason: None,
        action_template_id: None,
    };

    let addr = BooleanRuleHandler.start();
    addr.send(EvaluateBooleanRule { tadpole, rule })
        .await
        .expect("boolean handler failed")
}

#[test]
fn boolean_handler_returns_inconclusive_without_expr() {
    let evaluation = System::new().block_on(evaluate(None));
    assert!(evaluation.decision.is_inconclusive());
    assert_eq!(evaluation.reason, "no expr configured");
}

#[test]
fn boolean_handler_passes_on_match() {
    let evaluation = System::new().block_on(evaluate(Some("event_name == \"ContractCreated\"")));
    assert!(evaluation.decision.is_pass());
}

#[test]
fn boolean_handler_fails_on_mismatch() {
    let evaluation = System::new().block_on(evaluate(Some("event_name == \"ContractUpdated\"")));
    assert!(evaluation.decision.is_fail());
}

#[test]
fn boolean_handler_inconclusive_on_unknown_identifier() {
    let evaluation = System::new().block_on(evaluate(Some("risk_level == \"high\"")));
    assert!(evaluation.decision.is_inconclusive());
}

#[test]
fn boolean_handler_inconclusive_on_invalid_term() {
    let evaluation = System::new().block_on(evaluate(Some("event_name = \"ContractCreated\"")));
    assert!(evaluation.decision.is_inconclusive());
}

#[test]
fn boolean_handler_inconclusive_on_unquoted_literal() {
    let evaluation = System::new().block_on(evaluate(Some("event_name == ContractCreated")));
    assert!(evaluation.decision.is_inconclusive());
}

#[test]
fn boolean_handler_resolves_snapshot_key_case_insensitively() {
    let evaluation = System::new().block_on(evaluate_snapshot_condition(
        serde_json::json!({
            "Contract": { "amount": 1000000 }
        }),
        "contract",
        "amount",
        "eq",
        Some("NUMBER"),
        serde_json::json!(1000000),
    ));

    assert!(evaluation.decision.is_pass());
}

#[test]
fn boolean_handler_resolves_dotted_object_key_inside_snapshot() {
    let evaluation = System::new().block_on(evaluate_snapshot_condition(
        serde_json::json!({
            "Contract": { "value": 1000000 }
        }),
        "Contract",
        "contract.value",
        "eq",
        Some("NUMBER"),
        serde_json::json!(1000000),
    ));

    assert!(evaluation.decision.is_pass());
}

#[test]
fn boolean_handler_resolves_object_key_descriptor_payload_shape() {
    let evaluation = System::new().block_on(evaluate_snapshot_condition(
        serde_json::json!({
            "Contract": {
                "object_key": "contract.value",
                "amount": 1000000
            }
        }),
        "Contract",
        "contract.value",
        "eq",
        Some("NUMBER"),
        serde_json::json!(1000000),
    ));

    assert!(evaluation.decision.is_pass());
}

#[test]
fn boolean_handler_supports_equals_alias_for_boolean() {
    let evaluation = System::new().block_on(evaluate_snapshot_condition(
        serde_json::json!({
            "Supplier": { "verified": true }
        }),
        "Supplier",
        "supplier.verified",
        "EQUALS",
        Some("BOOLEAN"),
        serde_json::json!(true),
    ));

    assert!(evaluation.decision.is_pass());
}

#[test]
fn boolean_handler_supports_gt_with_numeric_string_expected_value() {
    let evaluation = System::new().block_on(evaluate_snapshot_condition(
        serde_json::json!({
            "Contract": { "value": 1000001 }
        }),
        "Contract",
        "contract.value",
        "GT",
        Some("NUMBER"),
        serde_json::json!("1000000"),
    ));

    assert!(evaluation.decision.is_pass());
}

#[test]
fn boolean_handler_inconclusive_when_snapshot_missing() {
    let evaluation = System::new().block_on(evaluate_snapshot_condition(
        serde_json::json!({
            "Contract": { "amount": 1000000 }
        }),
        "Supplier",
        "supplier.verified",
        "eq",
        Some("BOOLEAN"),
        serde_json::json!(true),
    ));

    assert!(evaluation.decision.is_inconclusive());
    assert_eq!(
        evaluation.reason,
        "snapshot 'Supplier' not present in body.snapshots"
    );
}

#[test]
fn boolean_handler_inconclusive_when_snapshot_is_not_object() {
    let evaluation = System::new().block_on(evaluate_snapshot_condition(
        serde_json::json!({
            "Supplier": true
        }),
        "Supplier",
        "supplier.verified",
        "eq",
        Some("BOOLEAN"),
        serde_json::json!(true),
    ));

    assert!(evaluation.decision.is_inconclusive());
    assert_eq!(evaluation.reason, "snapshot 'Supplier' is not an object");
}

#[test]
fn boolean_handler_fails_when_condition_key_missing() {
    let evaluation = System::new().block_on(evaluate_snapshot_condition(
        serde_json::json!({
            "Supplier": { "verified": true }
        }),
        "Supplier",
        "supplier.trust_level",
        "eq",
        None,
        serde_json::json!("gold"),
    ));

    assert!(evaluation.decision.is_fail());
    assert_eq!(evaluation.reason_code, "rule.conditions_fail");
    assert_eq!(evaluation.checks[0].status, "FAIL");
    assert_eq!(evaluation.checks[0].reason, "key missing in snapshot");
}

#[test]
fn boolean_handler_inconclusive_when_expected_value_missing() {
    let evaluation = System::new().block_on(evaluate_snapshot_conditions_with_rule(
        serde_json::json!({
            "Supplier": { "verified": true }
        }),
        "Supplier",
        vec![RuleCondition {
            object_key: "supplier.verified".to_string(),
            operator: "eq".to_string(),
            key_data_type: Some("BOOLEAN".to_string()),
            value: None,
        }],
        RuleLogic::All,
    ));

    assert!(evaluation.decision.is_inconclusive());
    assert_eq!(evaluation.checks[0].status, "INCONCLUSIVE");
    assert_eq!(
        evaluation.checks[0].reason,
        "expected value missing in rule condition"
    );
}

#[test]
fn boolean_handler_supports_any_logic_for_snapshot_conditions() {
    let evaluation = System::new().block_on(evaluate_snapshot_conditions_with_rule(
        serde_json::json!({
            "Contract": { "contract_currency": "EUR" }
        }),
        "contract",
        vec![
            RuleCondition {
                object_key: "contract_currency".to_string(),
                operator: "eq".to_string(),
                key_data_type: Some("STRING".to_string()),
                value: Some(serde_json::json!("€")),
            },
            RuleCondition {
                object_key: "contract_currency".to_string(),
                operator: "eq".to_string(),
                key_data_type: Some("STRING".to_string()),
                value: Some(serde_json::json!("EUR")),
            },
        ],
        RuleLogic::Any,
    ));

    assert!(evaluation.decision.is_pass());
    assert_eq!(evaluation.checks[0].status, "FAIL");
    assert_eq!(evaluation.checks[1].status, "PASS");
}

#[test]
fn boolean_handler_inconclusive_on_unsupported_operator() {
    let evaluation = System::new().block_on(evaluate_snapshot_condition(
        serde_json::json!({
            "Contract": { "amount": 1000000 }
        }),
        "Contract",
        "contract.amount",
        "between",
        Some("NUMBER"),
        serde_json::json!(500000),
    ));

    assert!(evaluation.decision.is_inconclusive());
    assert_eq!(evaluation.checks[0].status, "INCONCLUSIVE");
    assert!(
        evaluation.checks[0]
            .reason
            .contains("unsupported operator: between")
    );
}

#[test]
fn boolean_handler_supports_contains_for_string_values() {
    let evaluation = System::new().block_on(evaluate_snapshot_condition(
        serde_json::json!({
            "Contract": { "title": "Master Service Agreement" }
        }),
        "Contract",
        "contract.title",
        "contains",
        None,
        serde_json::json!("Service"),
    ));

    assert!(evaluation.decision.is_pass());
}

#[test]
fn boolean_handler_supports_contains_for_array_values() {
    let evaluation = System::new().block_on(evaluate_snapshot_condition(
        serde_json::json!({
            "Contract": { "tags": ["dpa", "security", "gdpr"] }
        }),
        "Contract",
        "contract.tags",
        "contains",
        None,
        serde_json::json!("gdpr"),
    ));

    assert!(evaluation.decision.is_pass());
}

#[test]
fn boolean_handler_supports_neq_operator() {
    let evaluation = System::new().block_on(evaluate_snapshot_condition(
        serde_json::json!({
            "Contract": { "country": "DE" }
        }),
        "Contract",
        "contract.country",
        "neq",
        None,
        serde_json::json!("FR"),
    ));

    assert!(evaluation.decision.is_pass());
}

#[test]
fn boolean_handler_supports_gte_lt_and_lte_operators() {
    let gte_eval = System::new().block_on(evaluate_snapshot_condition(
        serde_json::json!({
            "Contract": { "value": 100 }
        }),
        "Contract",
        "contract.value",
        "gte",
        Some("NUMBER"),
        serde_json::json!(100),
    ));
    assert!(gte_eval.decision.is_pass());

    let lt_eval = System::new().block_on(evaluate_snapshot_condition(
        serde_json::json!({
            "Contract": { "value": 99 }
        }),
        "Contract",
        "contract.value",
        "lt",
        Some("NUMBER"),
        serde_json::json!(100),
    ));
    assert!(lt_eval.decision.is_pass());

    let lte_eval = System::new().block_on(evaluate_snapshot_condition(
        serde_json::json!({
            "Contract": { "value": 100 }
        }),
        "Contract",
        "contract.value",
        "lte",
        Some("NUMBER"),
        serde_json::json!(100),
    ));
    assert!(lte_eval.decision.is_pass());
}

#[test]
fn boolean_handler_prioritizes_fail_over_inconclusive_when_mixed_checks() {
    let evaluation = System::new().block_on(evaluate_snapshot_conditions_with_rule(
        serde_json::json!({
            "Contract": { "amount": 100 }
        }),
        "Contract",
        vec![
            RuleCondition {
                object_key: "contract.amount".to_string(),
                operator: "gt".to_string(),
                key_data_type: Some("NUMBER".to_string()),
                value: Some(serde_json::json!(200)),
            },
            RuleCondition {
                object_key: "contract.currency".to_string(),
                operator: "eq".to_string(),
                key_data_type: None,
                value: Some(serde_json::json!("USD")),
            },
        ],
        RuleLogic::All,
    ));

    assert!(evaluation.decision.is_fail());
}
