use actix::Message;

use crate::dto::evaluation::RuleEvaluation;
use crate::dto::rules::RuleSpec;
use crate::dto::tadpole::Tadpole;

#[derive(Message)]
#[rtype(result = "RuleEvaluation")]
pub struct EvaluateBooleanRule {
    pub tadpole: Tadpole,
    pub rule: RuleSpec,
}
