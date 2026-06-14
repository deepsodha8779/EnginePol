use actix::prelude::*;
use log::{debug, info};
use std::collections::HashMap;

use crate::dto::{
    action::ActionCandidate,
    decision::Decision,
    orchestration::{
        CodexPlaybookResult, CodexResult, CodexRuleResult, OrchestrationResult, PlaybookSummary,
    },
    orchestrator::Orchestrate,
    tadpole::Tadpole,
};

pub struct OrchestratorActor;

impl Actor for OrchestratorActor {
    type Context = Context<Self>;
}

impl OrchestratorActor {
    fn aggregate_decision(tadpole: &Tadpole) -> Decision {
        let mut saw_inconclusive = false;
        for eval in &tadpole.tail.evaluations {
            debug!(
                "orchestrator eval: rule_id={} decision={:?}",
                eval.rule_id, eval.decision
            );
            if eval.decision.is_fail() {
                return Decision::fail("orchestrator.fail", "rule evaluation failed");
            }
            if eval.decision.is_inconclusive() {
                saw_inconclusive = true;
            }
        }
        if saw_inconclusive {
            Decision::inconclusive("orchestrator.inconclusive", "rule evaluation inconclusive")
        } else {
            Decision::pass("orchestrator.pass", "all rules passed")
        }
    }

    fn summarize_playbook(
        evaluations: &[crate::dto::evaluation::RuleEvaluation],
    ) -> (Decision, String) {
        if evaluations.is_empty() {
            return (
                Decision::inconclusive("orchestrator.playbook_empty", "no rules evaluated"),
                "no rules evaluated".to_string(),
            );
        }

        for eval in evaluations {
            if eval.decision.is_fail() {
                return (
                    Decision::fail("orchestrator.playbook_fail", "rule failed"),
                    format!("rule {} failed: {}", eval.rule_id, eval.reason),
                );
            }
        }

        for eval in evaluations {
            if eval.decision.is_inconclusive() {
                return (
                    Decision::inconclusive(
                        "orchestrator.playbook_inconclusive",
                        "rule inconclusive",
                    ),
                    format!("rule {} inconclusive: {}", eval.rule_id, eval.reason),
                );
            }
        }

        (
            Decision::pass("orchestrator.playbook_pass", "all rules passed"),
            "all rules passed".to_string(),
        )
    }
}

impl Handler<Orchestrate> for OrchestratorActor {
    type Result = MessageResult<Orchestrate>;

    fn handle(&mut self, msg: Orchestrate, _: &mut Context<Self>) -> Self::Result {
        info!(
            "orchestrator start: event_id={} evaluations={}",
            msg.tadpole.head.event_id,
            msg.tadpole.tail.evaluations.len()
        );
        let decision = Self::aggregate_decision(&msg.tadpole);

        let action_candidates = if decision.is_fail() {
            vec![ActionCandidate {
                kind: "OUTSYSTEMS_ACTION".to_string(),
                target: Some(msg.tadpole.head.event_id.clone()),
                reason: "decision=FAIL".to_string(),
            }]
        } else {
            Vec::new()
        };

        let mut evaluations_by_playbook: HashMap<
            String,
            Vec<crate::dto::evaluation::RuleEvaluation>,
        > = HashMap::new();
        for eval in &msg.tadpole.tail.evaluations {
            evaluations_by_playbook
                .entry(eval.playbook_id.clone())
                .or_default()
                .push(eval.clone());
        }

        let codex_playbooks = msg
            .tadpole
            .tail
            .assigned_playbooks
            .iter()
            .map(|assignment| {
                let evals = evaluations_by_playbook
                    .get(&assignment.playbook_id)
                    .map(|evals| evals.as_slice())
                    .unwrap_or(&[]);
                let (summary_decision, _) = Self::summarize_playbook(evals);
                CodexPlaybookResult {
                    playbook_id: assignment.playbook_id.clone(),
                    decision: summary_decision,
                    reason: assignment.reason.clone(),
                    rules: evals
                        .iter()
                        .map(|eval| CodexRuleResult {
                            rule_id: eval.rule_id.clone(),
                            decision: eval.decision.clone(),
                            reason: eval.reason.clone(),
                        })
                        .collect(),
                }
            })
            .collect();

        let codex = CodexResult {
            version_id: msg.tadpole.tail.codex_version_id.clone(),
            playbooks: codex_playbooks,
            decision: decision.clone(),
        };

        let playbook_summaries = msg
            .tadpole
            .tail
            .assigned_playbooks
            .iter()
            .map(|assignment| {
                let evals = evaluations_by_playbook
                    .get(&assignment.playbook_id)
                    .map(|evals| evals.as_slice())
                    .unwrap_or(&[]);
                let (summary_decision, reason) = Self::summarize_playbook(evals);
                PlaybookSummary {
                    playbook_id: assignment.playbook_id.clone(),
                    decision: summary_decision,
                    reason,
                }
            })
            .collect();

        // Orchestrator routing: always route to ActionBuilder so that
        // template-driven actions can match any decision type (PASS, FAIL,
        // INCONCLUSIVE).  The ActionBuilder itself filters by trigger criteria.
        let route_to_action_builder = true;

        info!(
            "orchestrator complete: event_id={} decision={:?} actions={} route_to_action_builder={}",
            msg.tadpole.head.event_id,
            decision,
            action_candidates.len(),
            route_to_action_builder
        );
        MessageResult(OrchestrationResult {
            decision,
            action_candidates,
            playbooks: msg.tadpole.tail.assigned_playbooks.clone(),
            matched_playbook: msg.tadpole.tail.matched_playbook.clone(),
            evaluations: msg.tadpole.tail.evaluations.clone(),
            codex,
            playbook_summaries,
            route_to_action_builder,
        })
    }
}
