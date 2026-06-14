use actix::prelude::*;
use log::{debug, info, warn};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::actors::handlers::boolean_handler::BooleanRuleHandler;
use crate::dto::{
    boolean_handler::EvaluateBooleanRule,
    decision::Decision,
    dispatcher::Dispatch,
    evaluation::RuleEvaluation,
    rules::{RuleKind, RuleSpec},
    tadpole::Tadpole,
};

/// Rule evaluation interface for dispatcher-supported handlers.
/// Implement this for new handler types and register them with the dispatcher.
pub trait RuleEvaluator: Send + Sync {
    fn evaluate(&self, tadpole: Tadpole, rule: RuleSpec) -> RuleEvalFuture;
}

pub type RuleEvalFuture = Pin<Box<dyn Future<Output = RuleEvaluation> + Send>>;

pub struct BooleanRuleEvaluator {
    handler: Addr<BooleanRuleHandler>,
}

impl BooleanRuleEvaluator {
    pub fn new(handler: Addr<BooleanRuleHandler>) -> Self {
        Self { handler }
    }
}

impl RuleEvaluator for BooleanRuleEvaluator {
    fn evaluate(&self, tadpole: Tadpole, rule: RuleSpec) -> RuleEvalFuture {
        let handler = self.handler.clone();
        Box::pin(async move {
            debug!(
                "dispatch evaluate boolean rule: event_id={} rule_id={}",
                tadpole.head.event_id, rule.rule_id
            );
            let action_template_id = rule.action_template_id.clone();
            let mut eval = handler
                .send(EvaluateBooleanRule {
                    tadpole,
                    rule: rule.clone(),
                })
                .await
                .unwrap_or_else(|err| RuleEvaluation {
                    playbook_id: rule.playbook_id.clone(),
                    rule_id: rule.rule_id.clone(),
                    rule_name: rule.rule_name.clone(),
                    object_type: rule.object_type.clone(),
                    order_seq: rule.order_seq,
                    is_critical: rule.is_critical,
                    priority: rule.priority.clone(),
                    decision: Decision::inconclusive(
                        "dispatcher.handler_error",
                        format!("handler error: {err}"),
                    ),
                    reason_code: "dispatcher.handler_error".to_string(),
                    reason: format!("handler error: {err}"),
                    checks: Vec::new(),
                    duration_ms: 0,
                    action_template_id: None,
                });
            eval.action_template_id = action_template_id;
            eval
        })
    }
}

pub struct EnrichmentStubEvaluator;

impl RuleEvaluator for EnrichmentStubEvaluator {
    fn evaluate(&self, _tadpole: Tadpole, rule: RuleSpec) -> RuleEvalFuture {
        Box::pin(async move {
            warn!("dispatch enrichment stub invoked: rule_id={}", rule.rule_id);
            RuleEvaluation {
                playbook_id: rule.playbook_id.clone(),
                rule_id: rule.rule_id.clone(),
                rule_name: rule.rule_name.clone(),
                object_type: rule.object_type.clone(),
                order_seq: rule.order_seq,
                is_critical: rule.is_critical,
                priority: rule.priority.clone(),
                decision: Decision::inconclusive(
                    "rule.enrichment_stub",
                    "enrichment stub not implemented",
                ),
                reason_code: "rule.enrichment_stub".to_string(),
                reason: "enrichment stub not implemented".to_string(),
                checks: Vec::new(),
                duration_ms: 0,
                action_template_id: rule.action_template_id.clone(),
            }
        })
    }
}

pub struct DispatcherActor {
    pub evaluators: HashMap<RuleKind, Arc<dyn RuleEvaluator>>,
}

impl Actor for DispatcherActor {
    type Context = Context<Self>;
}

impl DispatcherActor {
    async fn execute_rule(
        evaluators: &HashMap<RuleKind, Arc<dyn RuleEvaluator>>,
        tadpole: Tadpole,
        rule: RuleSpec,
    ) -> RuleEvaluation {
        if let Some(reason) = rule.skip_reason.clone() {
            return RuleEvaluation {
                playbook_id: rule.playbook_id.clone(),
                rule_id: rule.rule_id.clone(),
                rule_name: rule.rule_name.clone(),
                object_type: rule.object_type.clone(),
                order_seq: rule.order_seq,
                is_critical: rule.is_critical,
                priority: rule.priority.clone(),
                decision: Decision::inconclusive("dispatcher.skipped", reason.clone()),
                reason_code: "dispatcher.skipped".to_string(),
                reason,
                checks: Vec::new(),
                duration_ms: 0,
                action_template_id: rule.action_template_id.clone(),
            };
        }

        let started = std::time::Instant::now();
        debug!(
            "dispatcher executing rule: event_id={} rule_id={} kind={:?}",
            tadpole.head.event_id, rule.rule_id, rule.kind
        );

        let mut evaluation = match evaluators.get(&rule.kind) {
            Some(evaluator) => evaluator.evaluate(tadpole, rule.clone()).await,
            None => RuleEvaluation {
                playbook_id: rule.playbook_id.clone(),
                rule_id: rule.rule_id.clone(),
                rule_name: rule.rule_name.clone(),
                object_type: rule.object_type.clone(),
                order_seq: rule.order_seq,
                is_critical: rule.is_critical,
                priority: rule.priority.clone(),
                decision: Decision::inconclusive(
                    "dispatcher.no_handler",
                    format!("no handler registered for {:?}", rule.kind),
                ),
                reason_code: "dispatcher.no_handler".to_string(),
                reason: format!("no handler registered for {:?}", rule.kind),
                checks: Vec::new(),
                duration_ms: 0,
                action_template_id: rule.action_template_id.clone(),
            },
        };
        if evaluation.decision.is_inconclusive() {
            warn!(
                "dispatcher inconclusive: rule_id={} reason={}",
                evaluation.rule_id, evaluation.reason
            );
        }

        evaluation.duration_ms = started.elapsed().as_millis();
        debug!(
            "dispatcher rule complete: rule_id={} decision={:?} duration_ms={}",
            evaluation.rule_id, evaluation.decision, evaluation.duration_ms
        );
        evaluation
    }
}

impl Handler<Dispatch> for DispatcherActor {
    type Result = ResponseFuture<Tadpole>;

    fn handle(&mut self, msg: Dispatch, _: &mut Context<Self>) -> Self::Result {
        let evaluators = self.evaluators.clone();
        let mut tadpole = msg.tadpole;

        Box::pin(async move {
            info!(
                "dispatcher start: event_id={} rules={}",
                tadpole.head.event_id,
                tadpole.tail.ordered_rules.len()
            );
            let ordered_rules = tadpole.tail.ordered_rules.clone();
            for rule in ordered_rules {
                let evaluation =
                    DispatcherActor::execute_rule(&evaluators, tadpole.clone(), rule).await;
                let should_stop = tadpole
                    .tail
                    .execution_mode
                    .as_deref()
                    .map(|mode| mode.eq_ignore_ascii_case("Fail_First"))
                    .unwrap_or(false)
                    && evaluation.is_critical
                    && evaluation.decision.is_fail();
                tadpole.tail.evaluations.push(evaluation);
                if should_stop {
                    info!(
                        "dispatcher stop due to fail_first critical fail: event_id={}",
                        tadpole.head.event_id
                    );
                    break;
                }
            }
            info!(
                "dispatcher complete: event_id={} evaluations={}",
                tadpole.head.event_id,
                tadpole.tail.evaluations.len()
            );
            tadpole
        })
    }
}
