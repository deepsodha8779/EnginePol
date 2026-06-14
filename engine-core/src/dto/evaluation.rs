use serde::{Deserialize, Serialize};

use super::decision::Decision;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConditionCheck {
    pub object_key: String,
    pub operator: String,
    pub expected: Option<serde_json::Value>,
    pub actual: Option<serde_json::Value>,
    pub status: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleEvaluation {
    pub playbook_id: String,
    pub rule_id: String,
    pub rule_name: Option<String>,
    pub object_type: Option<String>,
    pub order_seq: Option<u32>,
    pub is_critical: bool,
    pub priority: Option<String>,
    pub decision: Decision,
    pub reason_code: String,
    pub reason: String,
    pub checks: Vec<ConditionCheck>,
    pub duration_ms: u128,
    /// Carried from RuleSpec: which action template to use if this rule failed.
    #[serde(default)]
    pub action_template_id: Option<String>,
}
