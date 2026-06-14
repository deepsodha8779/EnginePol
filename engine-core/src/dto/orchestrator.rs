use actix::Message;

use crate::dto::orchestration::OrchestrationResult;
use crate::dto::tadpole::Tadpole;

#[derive(Message)]
#[rtype(result = "OrchestrationResult")]
pub struct Orchestrate {
    pub tadpole: Tadpole,
}
