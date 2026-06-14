use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlaybookAssignment {
    pub playbook_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchedPlaybook {
    pub id: String,
    pub name: Option<String>,
    pub version: Option<String>,
    pub execution_mode: Option<String>,
    pub trigger_object_type: String,
    pub trigger_change_kind: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleKind {
    Boolean,
    EnrichmentStub,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuleLogic {
    #[serde(alias = "ALL", alias = "all")]
    All,
    #[serde(alias = "ANY", alias = "any")]
    Any,
}

impl Default for RuleLogic {
    fn default() -> Self {
        Self::All
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleSpec {
    pub playbook_id: String,
    pub rule_id: String,
    pub kind: RuleKind,
    pub expr: Option<String>,
    #[serde(default)]
    pub rule_name: Option<String>,
    #[serde(default)]
    pub object_type: Option<String>,
    #[serde(default)]
    pub order_seq: Option<u32>,
    #[serde(default)]
    pub priority: Option<String>,
    #[serde(default)]
    pub conditions: Vec<RuleCondition>,
    #[serde(default)]
    pub logic: RuleLogic,
    #[serde(default)]
    pub is_critical: bool,
    #[serde(default)]
    pub skip_reason: Option<String>,
    /// Pre-bound action template ID: when this rule fails, use this template.
    /// Set at config time from the playbook rule reference.
    #[serde(default)]
    pub action_template_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleCondition {
    pub object_key: String,
    pub operator: String,
    #[serde(default)]
    pub key_data_type: Option<String>,
    #[serde(default)]
    pub value: Option<serde_json::Value>,
}
