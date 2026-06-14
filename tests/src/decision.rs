use engine_core::dto::evaluation::RuleEvaluation;
use engine_core::dto::{decision::Decision, rules::RuleLogic};

#[test]
fn decision_roundtrip() {
    let decisions = [
        Decision::pass("rule.expr_pass", "ok"),
        Decision::fail("rule.expr_fail", "failed"),
        Decision::inconclusive("rule.expr_invalid", "invalid"),
    ];
    for decision in decisions {
        let json = serde_json::to_string(&decision).expect("serialize decision");
        let parsed: Decision = serde_json::from_str(&json).expect("deserialize decision");
        assert_eq!(parsed, decision);
    }
}

#[test]
fn rule_evaluation_roundtrip() {
    let evaluation = RuleEvaluation {
        playbook_id: "playbook.contract_governance".to_string(),
        rule_id: "rule.boolean".to_string(),
        rule_name: None,
        object_type: None,
        order_seq: None,
        is_critical: false,
        priority: None,
        decision: Decision::pass("rule.expr_pass", "ok"),
        reason_code: "rule.expr_pass".to_string(),
        reason: "expr evaluated: event_name == \"ContractCreated\"".to_string(),
        checks: Vec::new(),
        duration_ms: 12,
        action_template_id: None,
    };

    let json = serde_json::to_string(&evaluation).expect("serialize evaluation");
    let parsed: RuleEvaluation = serde_json::from_str(&json).expect("deserialize evaluation");

    assert_eq!(parsed.playbook_id, evaluation.playbook_id);
    assert_eq!(parsed.rule_id, evaluation.rule_id);
    assert_eq!(parsed.decision, evaluation.decision);
    assert_eq!(parsed.reason_code, evaluation.reason_code);
    assert_eq!(parsed.reason, evaluation.reason);
    assert_eq!(parsed.duration_ms, evaluation.duration_ms);
}

#[test]
fn rule_logic_defaults_to_all() {
    assert_eq!(RuleLogic::default(), RuleLogic::All);
}
