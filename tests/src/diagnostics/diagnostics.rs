use actix::prelude::*;
use engine_core::actors::diagnostics::DiagnosticsActor;
use engine_core::dto::{
    decision::Decision,
    diagnostics::{GetDiagnostics, RecordError, RecordEvaluation},
    evaluation::RuleEvaluation,
    orchestration::{
        CodexPlaybookResult, CodexResult, CodexRuleResult, OrchestrationResult, PlaybookSummary,
    },
    rules::PlaybookAssignment,
};

fn sample_result(decision: Decision, duration_ms: u128) -> OrchestrationResult {
    OrchestrationResult {
        decision: decision.clone(),
        action_candidates: Vec::new(),
        playbooks: vec![PlaybookAssignment {
            playbook_id: "playbook.contract_governance".to_string(),
            reason: "matched".to_string(),
        }],
        matched_playbook: None,
        evaluations: vec![RuleEvaluation {
            playbook_id: "playbook.contract_governance".to_string(),
            rule_id: "rule.boolean".to_string(),
            rule_name: None,
            object_type: None,
            order_seq: None,
            is_critical: false,
            priority: None,
            decision: Decision::pass("test.pass", "ok"),
            reason_code: "test.pass".to_string(),
            reason: "ok".to_string(),
            checks: Vec::new(),
            duration_ms,
            action_template_id: None,
        }],
        codex: CodexResult {
            version_id: Some("v1".to_string()),
            playbooks: vec![CodexPlaybookResult {
                playbook_id: "playbook.contract_governance".to_string(),
                decision: Decision::pass("test.pass", "ok"),
                reason: "matched".to_string(),
                rules: vec![CodexRuleResult {
                    rule_id: "rule.boolean".to_string(),
                    decision: Decision::pass("test.pass", "ok"),
                    reason: "ok".to_string(),
                }],
            }],
            decision,
        },
        playbook_summaries: vec![PlaybookSummary {
            playbook_id: "playbook.contract_governance".to_string(),
            decision: Decision::pass("test.pass", "ok"),
            reason: "all rules passed".to_string(),
        }],
        route_to_action_builder: false,
    }
}

#[test]
fn diagnostics_tracks_recent_and_errors() {
    let snapshot = System::new().block_on(async move {
        let addr = DiagnosticsActor::default().start();
        addr.send(RecordEvaluation {
            result: sample_result(Decision::pass("test.pass", "ok"), 10),
        })
        .await
        .expect("record evaluation failed");
        addr.send(RecordError {
            kind: "intake.validation_failed".to_string(),
        })
        .await
        .expect("record error failed");
        addr.send(RecordError {
            kind: "intake.validation_failed".to_string(),
        })
        .await
        .expect("record error failed");
        addr.send(GetDiagnostics)
            .await
            .expect("get diagnostics failed")
    });

    assert_eq!(snapshot.recent.len(), 1);
    assert_eq!(snapshot.recent[0].rule_count, 1);
    assert_eq!(snapshot.recent[0].total_duration_ms, 10);
    assert_eq!(
        snapshot.error_counters.get("intake.validation_failed"),
        Some(&2)
    );
}

#[test]
fn diagnostics_enforces_recent_capacity() {
    let snapshot = System::new().block_on(async move {
        let addr = DiagnosticsActor::default().start();
        for idx in 0..51 {
            addr.send(RecordEvaluation {
                result: sample_result(Decision::pass("test.pass", "ok"), idx),
            })
            .await
            .expect("record evaluation failed");
        }
        addr.send(GetDiagnostics)
            .await
            .expect("get diagnostics failed")
    });

    assert_eq!(snapshot.recent.len(), 50);
    assert_eq!(snapshot.recent[0].total_duration_ms, 50);
}
