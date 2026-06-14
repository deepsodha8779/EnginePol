use actix::prelude::*;
use log::{debug, info, warn};
use serde_json::Value;

use crate::dto::boolean_handler::EvaluateBooleanRule;
use crate::dto::{
    decision::Decision,
    evaluation::{ConditionCheck, RuleEvaluation},
    rules::RuleLogic,
};
use crate::simple_expr::eval_head_expr;

fn normalize_operator(operator: &str) -> String {
    match operator.trim().to_ascii_lowercase().as_str() {
        "eq" | "equals" | "equal" | "==" => "eq".to_string(),
        "neq" | "ne" | "not_equals" | "!=" => "neq".to_string(),
        "gt" | ">" => "gt".to_string(),
        "gte" | "ge" | ">=" | "gts" => "gte".to_string(),
        "lt" | "<" => "lt".to_string(),
        "lte" | "le" | "<=" => "lte".to_string(),
        "contains" => "contains".to_string(),
        _ => operator.trim().to_ascii_lowercase(),
    }
}

fn as_number(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|s| s.trim().parse::<f64>().ok()))
}

fn as_bool(value: &Value) -> Option<bool> {
    if let Some(v) = value.as_bool() {
        return Some(v);
    }
    value
        .as_str()
        .and_then(|s| match s.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Some(true),
            "false" | "0" | "no" => Some(false),
            _ => None,
        })
}

fn compare_values(
    operator: &str,
    actual: &Value,
    expected: &Value,
    key_data_type: Option<&str>,
) -> Result<bool, String> {
    let operator = normalize_operator(operator);
    let key_data_type = key_data_type.map(|v| v.trim().to_ascii_uppercase());

    match operator.as_str() {
        "eq" => {
            if key_data_type.as_deref() == Some("NUMBER")
                || (as_number(actual).is_some() && as_number(expected).is_some())
            {
                let a = as_number(actual).ok_or_else(|| "actual is not numeric".to_string())?;
                let e = as_number(expected).ok_or_else(|| "expected is not numeric".to_string())?;
                return Ok((a - e).abs() < f64::EPSILON);
            }
            if key_data_type.as_deref() == Some("BOOLEAN")
                || (as_bool(actual).is_some() && as_bool(expected).is_some())
            {
                let a = as_bool(actual).ok_or_else(|| "actual is not boolean".to_string())?;
                let e = as_bool(expected).ok_or_else(|| "expected is not boolean".to_string())?;
                return Ok(a == e);
            }
            Ok(actual == expected)
        }
        "neq" => {
            let eq = compare_values("eq", actual, expected, key_data_type.as_deref())?;
            Ok(!eq)
        }
        "gt" => {
            let a = as_number(actual).ok_or_else(|| "actual is not numeric".to_string())?;
            let e = as_number(expected).ok_or_else(|| "expected is not numeric".to_string())?;
            Ok(a > e)
        }
        "gte" => {
            let a = as_number(actual).ok_or_else(|| "actual is not numeric".to_string())?;
            let e = as_number(expected).ok_or_else(|| "expected is not numeric".to_string())?;
            Ok(a >= e)
        }
        "lt" => {
            let a = as_number(actual).ok_or_else(|| "actual is not numeric".to_string())?;
            let e = as_number(expected).ok_or_else(|| "expected is not numeric".to_string())?;
            Ok(a < e)
        }
        "lte" => {
            let a = as_number(actual).ok_or_else(|| "actual is not numeric".to_string())?;
            let e = as_number(expected).ok_or_else(|| "expected is not numeric".to_string())?;
            Ok(a <= e)
        }
        "contains" => {
            if let (Some(a), Some(e)) = (actual.as_str(), expected.as_str()) {
                Ok(a.contains(e))
            } else if let Some(items) = actual.as_array() {
                Ok(items.iter().any(|item| item == expected))
            } else {
                Err("contains requires string or array actual value".to_string())
            }
        }
        _ => Err(format!("unsupported operator: {operator}")),
    }
}

fn resolve_snapshot(
    snapshots: &serde_json::Map<String, Value>,
    object_type: &str,
) -> Option<Value> {
    snapshots
        .get(object_type)
        .or_else(|| {
            snapshots
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(object_type))
                .map(|(_, value)| value)
        })
        .cloned()
}

fn resolve_object_key_value(
    snapshot_obj: &serde_json::Map<String, Value>,
    object_type: &str,
    object_key: &str,
) -> Option<Value> {
    if let Some(value) = snapshot_obj.get(object_key) {
        return Some(value.clone());
    }

    let by_path = |path: &str| -> Option<Value> {
        let mut parts = path.split('.').filter(|part| !part.is_empty());
        let first = parts.next()?;
        let mut current = snapshot_obj.get(first)?;
        for part in parts {
            current = current.as_object()?.get(part)?;
        }
        Some(current.clone())
    };

    if let Some(value) = by_path(object_key) {
        return Some(value);
    }

    if let Some((prefix, remainder)) = object_key.split_once('.')
        && prefix.eq_ignore_ascii_case(object_type)
        && let Some(value) = by_path(remainder)
    {
        return Some(value);
    }

    let mapped_object_key = snapshot_obj
        .get("object_key")
        .and_then(Value::as_str)
        .map(str::trim);
    if mapped_object_key == Some(object_key.trim()) {
        return snapshot_obj
            .get("value")
            .or_else(|| snapshot_obj.get("amount"))
            .cloned();
    }

    None
}

fn evaluate_snapshot_conditions(msg: &EvaluateBooleanRule) -> Option<RuleEvaluation> {
    if msg.rule.conditions.is_empty() {
        return None;
    }

    let object_type = msg.rule.object_type.clone().unwrap_or_default();
    let snapshots = msg
        .tadpole
        .body
        .get("snapshots")
        .and_then(Value::as_object)
        .cloned();

    let snapshot = snapshots
        .as_ref()
        .and_then(|snapshots_obj| resolve_snapshot(snapshots_obj, &object_type));

    let Some(snapshot) = snapshot else {
        return Some(RuleEvaluation {
            playbook_id: msg.rule.playbook_id.clone(),
            rule_id: msg.rule.rule_id.clone(),
            rule_name: msg.rule.rule_name.clone(),
            object_type: msg.rule.object_type.clone(),
            order_seq: msg.rule.order_seq,
            is_critical: msg.rule.is_critical,
            priority: msg.rule.priority.clone(),
            decision: Decision::inconclusive(
                "rule.snapshot_missing",
                format!("snapshot '{object_type}' not present in body.snapshots"),
            ),
            reason_code: "rule.snapshot_missing".to_string(),
            reason: format!("snapshot '{object_type}' not present in body.snapshots"),
            checks: Vec::new(),
            duration_ms: 0,
            action_template_id: msg.rule.action_template_id.clone(),
        });
    };

    let Some(snapshot_obj) = snapshot.as_object() else {
        return Some(RuleEvaluation {
            playbook_id: msg.rule.playbook_id.clone(),
            rule_id: msg.rule.rule_id.clone(),
            rule_name: msg.rule.rule_name.clone(),
            object_type: msg.rule.object_type.clone(),
            order_seq: msg.rule.order_seq,
            is_critical: msg.rule.is_critical,
            priority: msg.rule.priority.clone(),
            decision: Decision::inconclusive(
                "rule.snapshot_invalid",
                format!("snapshot '{object_type}' is not an object"),
            ),
            reason_code: "rule.snapshot_invalid".to_string(),
            reason: format!("snapshot '{object_type}' is not an object"),
            checks: Vec::new(),
            duration_ms: 0,
            action_template_id: msg.rule.action_template_id.clone(),
        });
    };

    let mut pass_count = 0usize;
    let mut fail_count = 0usize;
    let mut inconclusive_count = 0usize;
    let mut checks = Vec::new();

    for condition in &msg.rule.conditions {
        let actual = resolve_object_key_value(snapshot_obj, &object_type, &condition.object_key);
        let expected = condition.value.clone();
        let check = match (actual.clone(), expected.clone()) {
            (None, _) => {
                fail_count += 1;
                ConditionCheck {
                    object_key: condition.object_key.clone(),
                    operator: condition.operator.clone(),
                    expected,
                    actual: None,
                    status: "FAIL".to_string(),
                    reason: "key missing in snapshot".to_string(),
                }
            }
            (Some(actual_val), None) => {
                inconclusive_count += 1;
                ConditionCheck {
                    object_key: condition.object_key.clone(),
                    operator: condition.operator.clone(),
                    expected: None,
                    actual: Some(actual_val),
                    status: "INCONCLUSIVE".to_string(),
                    reason: "expected value missing in rule condition".to_string(),
                }
            }
            (Some(actual_val), Some(expected_val)) => {
                match compare_values(
                    &condition.operator,
                    &actual_val,
                    &expected_val,
                    condition.key_data_type.as_deref(),
                ) {
                    Ok(true) => {
                        pass_count += 1;
                        ConditionCheck {
                            object_key: condition.object_key.clone(),
                            operator: condition.operator.clone(),
                            expected: Some(expected_val),
                            actual: Some(actual_val),
                            status: "PASS".to_string(),
                            reason: "value matches condition".to_string(),
                        }
                    }
                    Ok(false) => {
                        fail_count += 1;
                        ConditionCheck {
                            object_key: condition.object_key.clone(),
                            operator: condition.operator.clone(),
                            expected: Some(expected_val),
                            actual: Some(actual_val),
                            status: "FAIL".to_string(),
                            reason: "value does not satisfy condition".to_string(),
                        }
                    }
                    Err(err) => {
                        inconclusive_count += 1;
                        ConditionCheck {
                            object_key: condition.object_key.clone(),
                            operator: condition.operator.clone(),
                            expected: Some(expected_val),
                            actual: Some(actual_val),
                            status: "INCONCLUSIVE".to_string(),
                            reason: err,
                        }
                    }
                }
            }
        };
        checks.push(check);
    }

    let (decision, reason_code, reason) = match msg.rule.logic {
        RuleLogic::All => {
            if fail_count > 0 {
                (
                    Decision::fail(
                        "rule.conditions_fail",
                        format!("one or more conditions failed for snapshot '{object_type}'"),
                    ),
                    "rule.conditions_fail".to_string(),
                    format!("one or more conditions failed for snapshot '{object_type}'"),
                )
            } else if inconclusive_count > 0 {
                (
                    Decision::inconclusive(
                        "rule.conditions_inconclusive",
                        format!("one or more conditions inconclusive for snapshot '{object_type}'"),
                    ),
                    "rule.conditions_inconclusive".to_string(),
                    format!("one or more conditions inconclusive for snapshot '{object_type}'"),
                )
            } else {
                (
                    Decision::pass(
                        "rule.conditions_pass",
                        format!("all conditions passed for snapshot '{object_type}'"),
                    ),
                    "rule.conditions_pass".to_string(),
                    format!("all conditions passed for snapshot '{object_type}'"),
                )
            }
        }
        RuleLogic::Any => {
            if pass_count > 0 {
                (
                    Decision::pass(
                        "rule.conditions_pass",
                        format!("one or more conditions passed for snapshot '{object_type}'"),
                    ),
                    "rule.conditions_pass".to_string(),
                    format!("one or more conditions passed for snapshot '{object_type}'"),
                )
            } else if inconclusive_count > 0 {
                (
                    Decision::inconclusive(
                        "rule.conditions_inconclusive",
                        format!(
                            "no conditions passed and one or more conditions were inconclusive for snapshot '{object_type}'"
                        ),
                    ),
                    "rule.conditions_inconclusive".to_string(),
                    format!(
                        "no conditions passed and one or more conditions were inconclusive for snapshot '{object_type}'"
                    ),
                )
            } else {
                (
                    Decision::fail(
                        "rule.conditions_fail",
                        format!("no conditions passed for snapshot '{object_type}'"),
                    ),
                    "rule.conditions_fail".to_string(),
                    format!("no conditions passed for snapshot '{object_type}'"),
                )
            }
        }
    };

    Some(RuleEvaluation {
        playbook_id: msg.rule.playbook_id.clone(),
        rule_id: msg.rule.rule_id.clone(),
        rule_name: msg.rule.rule_name.clone(),
        object_type: msg.rule.object_type.clone(),
        order_seq: msg.rule.order_seq,
        is_critical: msg.rule.is_critical,
        priority: msg.rule.priority.clone(),
        decision,
        reason_code,
        reason,
        checks,
        duration_ms: 0,
        action_template_id: msg.rule.action_template_id.clone(),
    })
}

pub struct BooleanRuleHandler;

impl Actor for BooleanRuleHandler {
    type Context = Context<Self>;
}

impl Handler<EvaluateBooleanRule> for BooleanRuleHandler {
    type Result = MessageResult<EvaluateBooleanRule>;

    fn handle(&mut self, msg: EvaluateBooleanRule, _: &mut Context<Self>) -> Self::Result {
        if let Some(evaluation) = evaluate_snapshot_conditions(&msg) {
            info!(
                "boolean snapshot rule evaluated: rule_id={} decision={:?}",
                msg.rule.rule_id, evaluation.decision
            );
            return MessageResult(evaluation);
        }

        let (decision, reason_code, reason) = match msg.rule.expr.as_deref() {
            None => {
                debug!("boolean rule missing expr: rule_id={}", msg.rule.rule_id);
                (
                    Decision::inconclusive("rule.expr_missing", "no expr configured"),
                    "rule.expr_missing".to_string(),
                    "no expr configured".to_string(),
                )
            }
            Some(expr) => match eval_head_expr(expr, &msg.tadpole.head) {
                Ok(true) => (
                    Decision::pass("rule.expr_pass", format!("expr evaluated: {expr}")),
                    "rule.expr_pass".to_string(),
                    format!("expr evaluated: {expr}"),
                ),
                Ok(false) => (
                    Decision::fail("rule.expr_fail", format!("expr evaluated: {expr}")),
                    "rule.expr_fail".to_string(),
                    format!("expr evaluated: {expr}"),
                ),
                Err(err) => {
                    warn!(
                        "boolean rule eval error: rule_id={} error={}",
                        msg.rule.rule_id, err.message
                    );
                    (
                        Decision::inconclusive(
                            "rule.expr_invalid",
                            format!("expr invalid: {}", err.message),
                        ),
                        "rule.expr_invalid".to_string(),
                        format!("expr invalid: {}", err.message),
                    )
                }
            },
        };

        info!(
            "boolean rule evaluated: rule_id={} decision={:?}",
            msg.rule.rule_id, decision
        );

        MessageResult(RuleEvaluation {
            playbook_id: msg.rule.playbook_id.clone(),
            rule_id: msg.rule.rule_id.clone(),
            rule_name: msg.rule.rule_name.clone(),
            object_type: msg.rule.object_type.clone(),
            order_seq: msg.rule.order_seq,
            is_critical: msg.rule.is_critical,
            priority: msg.rule.priority.clone(),
            decision,
            reason_code,
            reason,
            checks: Vec::new(),
            duration_ms: 0,
            action_template_id: msg.rule.action_template_id.clone(),
        })
    }
}
