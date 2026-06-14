use std::collections::{HashMap, VecDeque};

use actix::prelude::*;
use log::{debug, info};
use serde::{Deserialize, Serialize};

use crate::dto::diagnostics::{GetDiagnostics, RecordError, RecordEvaluation};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticsSnapshot {
    pub recent: Vec<RecentEvaluation>,
    pub error_counters: HashMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecentEvaluation {
    pub decision: String,
    pub playbooks: Vec<String>,
    pub rule_count: usize,
    pub total_duration_ms: u128,
}

pub struct DiagnosticsActor {
    recent: VecDeque<RecentEvaluation>,
    error_counters: HashMap<String, u64>,
    recent_capacity: usize,
}

impl Default for DiagnosticsActor {
    fn default() -> Self {
        Self {
            recent: VecDeque::new(),
            error_counters: HashMap::new(),
            recent_capacity: 50,
        }
    }
}

impl Actor for DiagnosticsActor {
    type Context = Context<Self>;
}

impl Handler<RecordEvaluation> for DiagnosticsActor {
    type Result = ();

    fn handle(&mut self, msg: RecordEvaluation, _: &mut Context<Self>) -> Self::Result {
        info!(
            "diagnostics record evaluation: decision={:?} evaluations={}",
            msg.result.decision,
            msg.result.evaluations.len()
        );
        let total_duration_ms: u128 = msg.result.evaluations.iter().map(|e| e.duration_ms).sum();
        let entry = RecentEvaluation {
            decision: format!("{:?}", msg.result.decision),
            playbooks: msg
                .result
                .playbooks
                .iter()
                .map(|p| p.playbook_id.clone())
                .collect(),
            rule_count: msg.result.evaluations.len(),
            total_duration_ms,
        };
        self.recent.push_front(entry);
        while self.recent.len() > self.recent_capacity {
            self.recent.pop_back();
        }
    }
}

impl Handler<RecordError> for DiagnosticsActor {
    type Result = ();

    fn handle(&mut self, msg: RecordError, _: &mut Context<Self>) -> Self::Result {
        info!("diagnostics record error: kind={}", msg.kind);
        *self.error_counters.entry(msg.kind).or_insert(0) += 1;
    }
}

impl Handler<GetDiagnostics> for DiagnosticsActor {
    type Result = MessageResult<GetDiagnostics>;

    fn handle(&mut self, _: GetDiagnostics, _: &mut Context<Self>) -> Self::Result {
        debug!("diagnostics snapshot requested");
        MessageResult(DiagnosticsSnapshot {
            recent: self.recent.iter().cloned().collect(),
            error_counters: self.error_counters.clone(),
        })
    }
}
