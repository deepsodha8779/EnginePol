use serde::{Deserialize, Serialize};

use super::{
    action::ActionCandidate,
    decision::Decision,
    evaluation::RuleEvaluation,
    rules::{MatchedPlaybook, PlaybookAssignment},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodexResult {
    pub version_id: Option<String>,
    pub playbooks: Vec<CodexPlaybookResult>,
    pub decision: Decision,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodexPlaybookResult {
    pub playbook_id: String,
    pub decision: Decision,
    pub reason: String,
    pub rules: Vec<CodexRuleResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodexRuleResult {
    pub rule_id: String,
    pub decision: Decision,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlaybookSummary {
    pub playbook_id: String,
    pub decision: Decision,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestrationResult {
    pub decision: Decision,
    pub action_candidates: Vec<ActionCandidate>,
    pub playbooks: Vec<PlaybookAssignment>,
    pub matched_playbook: Option<MatchedPlaybook>,
    pub evaluations: Vec<RuleEvaluation>,
    pub codex: CodexResult,
    pub playbook_summaries: Vec<PlaybookSummary>,
    /// Orchestrator routing decision: true when failed rules exist and ActionBuilder
    /// should create action records. False when all rules passed (MetricManager only).
    #[serde(default)]
    pub route_to_action_builder: bool,
}
