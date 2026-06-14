use actix::Message;

use crate::dto::orchestration::OrchestrationResult;

#[derive(Message)]
#[rtype(result = "()")]
pub struct RecordEvaluation {
    pub result: OrchestrationResult,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct RecordError {
    pub kind: String,
}

#[derive(Message)]
#[rtype(result = "crate::actors::diagnostics::DiagnosticsSnapshot")]
pub struct GetDiagnostics;
