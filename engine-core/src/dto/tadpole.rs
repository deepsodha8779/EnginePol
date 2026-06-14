use uuid::Uuid;

use domain::envelope::{CanonicalEnvelope, TadpoleHead};

use super::{
    decision::Decision,
    evaluation::RuleEvaluation,
    rules::{MatchedPlaybook, PlaybookAssignment, RuleSpec},
};

#[derive(Debug, Clone)]
pub struct TadpoleTail {
    pub assigned_playbooks: Vec<PlaybookAssignment>,
    pub matched_playbook: Option<MatchedPlaybook>,
    pub execution_mode: Option<String>,
    pub ordered_rules: Vec<RuleSpec>,
    pub evaluations: Vec<RuleEvaluation>,
    pub final_decision: Option<Decision>,
    pub codex_version_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Tadpole {
    pub id: Uuid,
    pub head: TadpoleHead,
    pub body: serde_json::Value,
    pub tail: TadpoleTail,
}

impl Tadpole {
    pub fn from_envelope(envelope: CanonicalEnvelope) -> Self {
        Self {
            id: Uuid::new_v4(),
            head: envelope.head,
            body: envelope.body,
            tail: TadpoleTail {
                assigned_playbooks: Vec::new(),
                matched_playbook: None,
                execution_mode: None,
                ordered_rules: Vec::new(),
                evaluations: Vec::new(),
                final_decision: None,
                codex_version_id: None,
            },
        }
    }
}
