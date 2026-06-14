use actix::prelude::*;
use domain::envelope::{CanonicalEnvelope, TadpoleHead};
use engine_core::actors::assigner::{AssignerActor, PlaybookConfig};
use engine_core::dto::assigner::Assign;
use std::fs;
use std::path::PathBuf;

fn envelope_for(
    event_name: &str,
    tenant_id: &str,
    changed_object_type: Option<&str>,
    change_kind: Option<&str>,
    body: serde_json::Value,
) -> CanonicalEnvelope {
    CanonicalEnvelope {
        head: TadpoleHead {
            event_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_string(),
            event_name: event_name.to_string(),
            event_category: Some("Transaction".to_string()),
            tenant_id: tenant_id.to_string(),
            correlation_id: None,
            causation_id: None,
            occurred_at: None,
            originating_function: None,
            originating_application: None,
            environment: None,
            external_dependency_id: Some("dep_123".to_string()),
            changed_object_type: changed_object_type.map(str::to_string),
            changed_object_id: None,
            change_kind: change_kind.map(str::to_string),
        },
        body,
    }
}

fn contract_payload(risk_level: &str) -> serde_json::Value {
    serde_json::json!({
        "contract_id": "ctr_987",
        "supplier_id": "sup_456",
        "risk_level": risk_level,
        "amount": 12500,
        "currency": "USD"
    })
}

fn assign_with_config(
    config: PlaybookConfig,
    envelope: CanonicalEnvelope,
) -> Vec<engine_core::dto::tadpole::Tadpole> {
    System::new().block_on(async move {
        let addr = AssignerActor::from_config(config).start();
        addr.send(Assign { envelope })
            .await
            .expect("assigner actor failed")
    })
}

#[test]
fn assigns_governance_playbook_for_contract_created() {
    let config: PlaybookConfig = serde_json::from_str(
        r#"
        {
          "playbooks": [
            {
              "id": "playbook.contract_governance",
              "trigger": {
                "object_type": "Contract",
                "change_kind": ["created", "updated"]
              },
              "rules": [
                {
                  "order_seq": 2,
                  "rule_id": "rule.event_type_is_contract_created",
                  "is_critical": false
                }
              ]
            }
          ]
        }
    "#,
    )
    .unwrap();

    let envelope = envelope_for(
        "ContractCreated",
        "acme",
        Some("Contract"),
        Some("created"),
        contract_payload("low"),
    );
    let tadpoles = assign_with_config(config, envelope);

    assert_eq!(tadpoles.len(), 1);
    assert!(
        tadpoles[0]
            .tail
            .assigned_playbooks
            .iter()
            .any(|p| p.playbook_id == "playbook.contract_governance")
    );
}

#[test]
fn assigns_playbooks_even_when_no_match() {
    let config: PlaybookConfig = serde_json::from_str(
        r#"
        {
          "playbooks": [
            {
              "id": "playbook.contract_delta",
              "trigger": {
                "object_type": "Contract",
                "change_kind": ["updated"]
              },
              "rules": []
            }
          ]
        }
    "#,
    )
    .unwrap();

    let envelope = envelope_for(
        "ContractCancelled",
        "acme",
        Some("Contract"),
        Some("cancelled"),
        contract_payload("low"),
    );
    let tadpoles = assign_with_config(config, envelope);

    assert!(tadpoles.is_empty());
}

#[test]
fn assigns_multiple_playbooks_when_multiple_match() {
    let config: PlaybookConfig = serde_json::from_str(
        r#"
        {
          "playbooks": [
            {
              "id": "playbook.contract_created",
              "trigger": {
                "object_type": "Contract",
                "change_kind": ["created"]
              },
              "rules": []
            },
            {
              "id": "playbook.acme_tenant",
              "match_expr": "tenant_id == \"acme\"",
              "rules": []
            }
          ]
        }
    "#,
    )
    .unwrap();

    let envelope = envelope_for(
        "ContractCreated",
        "acme",
        Some("Contract"),
        Some("created"),
        contract_payload("high"),
    );
    let tadpoles = assign_with_config(config, envelope);

    assert_eq!(tadpoles.len(), 1);
    assert_eq!(tadpoles[0].tail.assigned_playbooks.len(), 2);
    assert!(
        tadpoles[0]
            .tail
            .assigned_playbooks
            .iter()
            .any(|p| p.playbook_id == "playbook.contract_created")
    );
    assert!(
        tadpoles[0]
            .tail
            .assigned_playbooks
            .iter()
            .any(|p| p.playbook_id == "playbook.acme_tenant")
    );
}

#[test]
fn ignores_invalid_expr_and_marks_not_matched() {
    let config: PlaybookConfig = serde_json::from_str(
        r#"
        {
          "playbooks": [
            {
              "id": "playbook.invalid_expr",
              "match_expr": "risk_level == \"high\"",
              "rules": []
            }
          ]
        }
    "#,
    )
    .unwrap();

    let envelope = envelope_for(
        "ContractCreated",
        "acme",
        Some("Contract"),
        Some("created"),
        contract_payload("high"),
    );
    let tadpoles = assign_with_config(config, envelope);

    assert!(tadpoles.is_empty());
}

#[test]
fn playbook_config_from_path_returns_error_for_missing_file() {
    let missing = PathBuf::from("this_file_should_not_exist_12345.json");
    let err = PlaybookConfig::from_path(&missing).unwrap_err();
    assert!(err.contains("unable to read config"));
}

#[test]
fn playbook_config_from_path_returns_error_for_invalid_json() {
    let mut path = std::env::temp_dir();
    path.push("tadpole_invalid_playbooks.json");
    fs::write(&path, "not-json").expect("write temp file");

    let err = PlaybookConfig::from_path(&path).unwrap_err();
    assert!(err.contains("invalid playbook config JSON"));

    let _ = fs::remove_file(&path);
}

#[test]
fn playbook_config_from_path_parses_valid_json() {
    let mut path = std::env::temp_dir();
    path.push("tadpole_valid_playbooks.json");
    fs::write(
        &path,
        r#"
        {
          "playbooks": [
            {
              "id": "playbook.contract_governance",
              "trigger": {
                "object_type": "Contract",
                "change_kind": ["created"]
              },
              "rules": []
            }
          ]
        }
    "#,
    )
    .expect("write temp file");

    let config = PlaybookConfig::from_path(&path).expect("valid config");
    assert_eq!(config.codex.len(), 1);
    assert_eq!(config.codex[0].playbooks.len(), 1);

    let _ = fs::remove_file(&path);
}

#[test]
fn playbook_config_accepts_codex_array() {
    let config: PlaybookConfig = serde_json::from_str(
        r#"
        {
          "codex": [
            {
              "version_id": "v1",
              "playbooks": [
                {
                  "id": "playbook.contract_governance",
                  "trigger": {
                    "object_type": "Contract",
                    "change_kind": ["created"]
                  },
                  "rules": []
                }
              ]
            }
          ]
        }
    "#,
    )
    .unwrap();

    assert_eq!(config.codex.len(), 1);
    assert_eq!(config.codex[0].playbooks.len(), 1);
}

#[test]
fn assigns_all_codex_entries() {
    let config: PlaybookConfig = serde_json::from_str(
        r#"
        {
          "codex": [
            {
              "version_id": "v1",
              "playbooks": [
                {
                  "id": "playbook.contract_governance",
                  "trigger": {
                    "object_type": "Contract",
                    "change_kind": ["created"]
                  },
                  "rules": []
                }
              ]
            },
            {
              "version_id": "v2",
              "playbooks": [
                {
                  "id": "playbook.contract_review",
                  "trigger": {
                    "object_type": "Contract",
                    "change_kind": ["created"]
                  },
                  "rules": []
                }
              ]
            }
          ]
        }
    "#,
    )
    .unwrap();

    let envelope = envelope_for(
        "ContractCreated",
        "acme",
        Some("Contract"),
        Some("created"),
        contract_payload("low"),
    );
    let tadpoles = assign_with_config(config, envelope);

    assert_eq!(tadpoles.len(), 2);
    assert_eq!(tadpoles[0].tail.codex_version_id, Some("v1".to_string()));
    assert_eq!(tadpoles[1].tail.codex_version_id, Some("v2".to_string()));
}

#[test]
fn playbook_config_accepts_single_playbook_document() {
    let config: PlaybookConfig = serde_json::from_str(
        r#"
        {
          "id": "playbook.contract_governance",
          "execution_mode": "Fail_First",
          "trigger": {
            "object_type": "Contract",
            "change_kind": ["created"]
          },
          "rules": [
            {
              "order_seq": 1,
              "rule_id": "rule.contract_created_check",
              "is_critical": true
            }
          ]
        }
    "#,
    )
    .unwrap();

    assert_eq!(config.codex.len(), 1);
    assert_eq!(config.codex[0].playbooks.len(), 1);
    assert_eq!(
        config.codex[0].playbooks[0].id,
        "playbook.contract_governance"
    );
}

#[test]
fn playbook_config_rejects_duplicate_rule_ids() {
    let err = serde_json::from_str::<PlaybookConfig>(
        r#"
        {
          "playbooks": [
            {
              "id": "playbook.contract_governance",
              "trigger": {
                "object_type": "Contract",
                "change_kind": ["created"]
              },
              "rules": [
                {
                  "order_seq": 1,
                  "rule_id": "rule.duplicate",
                  "is_critical": true
                },
                {
                  "order_seq": 2,
                  "rule_id": "rule.duplicate",
                  "is_critical": false
                }
              ]
            }
          ]
        }
    "#,
    )
    .unwrap_err()
    .to_string();

    assert!(err.contains("duplicate rule_id"));
}

#[test]
fn assigns_rules_critical_first_then_order_seq() {
    let config: PlaybookConfig = serde_json::from_str(
        r#"
        {
          "playbooks": [
            {
              "id": "playbook.contract_governance",
              "trigger": {
                "object_type": "Contract",
                "change_kind": ["created"]
              },
              "rules": [
                {
                  "order_seq": 3,
                  "rule_id": "rule.non_critical_late",
                  "is_critical": false
                },
                {
                  "order_seq": 2,
                  "rule_id": "rule.critical_second",
                  "is_critical": true
                },
                {
                  "order_seq": 1,
                  "rule_id": "rule.non_critical_first",
                  "is_critical": false
                },
                {
                  "order_seq": 4,
                  "rule_id": "rule.critical_last",
                  "is_critical": true
                }
              ]
            }
          ]
        }
    "#,
    )
    .unwrap();

    let envelope = envelope_for(
        "ContractCreated",
        "acme",
        Some("Contract"),
        Some("created"),
        contract_payload("low"),
    );
    let tadpoles = assign_with_config(config, envelope);

    assert_eq!(tadpoles.len(), 1);
    let ordered_ids: Vec<String> = tadpoles[0]
        .tail
        .ordered_rules
        .iter()
        .map(|rule| rule.rule_id.clone())
        .collect();
    assert_eq!(
        ordered_ids,
        vec![
            "rule.critical_second",
            "rule.critical_last",
            "rule.non_critical_first",
            "rule.non_critical_late"
        ]
    );
}
