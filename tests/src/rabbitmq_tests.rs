//! Tests for RabbitMQ consumer behaviour: envelope parsing (message body format).

use http_gateway::parse_envelope_from_bytes;

const VALID_ENVELOPE_JSON: &str = r#"{
  "head": {
    "event_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
    "event_name": "InvoiceReceivedForExternalDependency",
    "event_category": "Transaction",
    "tenant_id": "acme-corp",
    "correlation_id": null,
    "causation_id": null,
    "occurred_at": "2024-11-05T12:34:56Z",
    "originating_function": "Finance",
    "originating_application": "AccountingIntegration",
    "environment": "prd",
    "external_dependency_id": "01J0M8WZ9K7F6R5T4P3Q2N1M0",
    "changed_object_type": "Invoice",
    "changed_object_id": "01J0M8Y8A4D5F6G7H8J9KLMNO",
    "change_kind": "created"
  },
  "body": {
    "snapshots": [{
      "object_type": "Invoice",
      "object_id": "01J0M8Y8A4D5F6G7H8J9KLMNO",
      "supplier_reference": "SUP-1001",
      "amount": 12500,
      "currency": "EUR"
    }]
  },
  "tail": {}
}"#;

#[test]
fn parse_envelope_from_bytes_accepts_valid_canonical_envelope() {
    let bytes = VALID_ENVELOPE_JSON.as_bytes();
    let result = parse_envelope_from_bytes(bytes);
    assert!(
        result.is_ok(),
        "valid envelope should parse: {:?}",
        result.err()
    );
    let envelope = result.unwrap();
    assert_eq!(envelope.head.event_id, "01ARZ3NDEKTSV4RRFFQ69G5FAV");
    assert_eq!(
        envelope.head.event_name,
        "InvoiceReceivedForExternalDependency"
    );
    assert_eq!(envelope.head.tenant_id, "acme-corp");
    assert_eq!(
        envelope.head.changed_object_type.as_deref(),
        Some("Invoice")
    );
    assert!(!envelope.body.is_null());
}

#[test]
fn parse_envelope_from_bytes_accepts_minimal_valid_envelope() {
    let json = r#"{
      "head": {
        "event_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "event_name": "ContractCreated",
        "tenant_id": "tenant-minimal",
        "correlation_id": null,
        "causation_id": null,
        "occurred_at": null,
        "originating_function": null,
        "originating_application": null,
        "environment": null,
        "changed_object_type": null,
        "changed_object_id": null,
        "change_kind": null
      }
    }"#;

    let envelope = parse_envelope_from_bytes(json.as_bytes())
        .expect("minimal envelope should deserialize with default body");

    assert_eq!(envelope.head.event_name, "ContractCreated");
    assert_eq!(envelope.head.tenant_id, "tenant-minimal");
    assert_eq!(envelope.body, serde_json::json!({}));
}

#[test]
fn parse_envelope_from_bytes_accepts_pretty_or_compact_json() {
    let compact = r#"{"head":{"event_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","event_name":"InvoiceReceivedForExternalDependency","event_category":"Transaction","tenant_id":"acme-corp","correlation_id":null,"causation_id":null,"occurred_at":"2024-11-05T12:34:56Z","originating_function":"Finance","originating_application":"AccountingIntegration","environment":"prd","external_dependency_id":"01J0M8WZ9K7F6R5T4P3Q2N1M0","changed_object_type":"Invoice","changed_object_id":"01J0M8Y8A4D5F6G7H8J9KLMNO","change_kind":"created"},"body":{"snapshots":[{"object_type":"Invoice"}]}}"#;

    let envelope =
        parse_envelope_from_bytes(compact.as_bytes()).expect("compact JSON should parse");

    assert_eq!(envelope.head.changed_object_type.as_deref(), Some("Invoice"));
    assert!(envelope.body["snapshots"].is_array());
}

#[test]
fn parse_envelope_from_bytes_rejects_invalid_json() {
    let invalid: &[u8] = b"{ not valid json }";
    let result = parse_envelope_from_bytes(invalid);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("JSON parse error") || err.to_lowercase().contains("parse"));
}

#[test]
fn parse_envelope_from_bytes_rejects_empty_body() {
    let result = parse_envelope_from_bytes(b"");
    assert!(result.is_err());
}

#[test]
fn parse_envelope_from_bytes_rejects_non_object_root() {
    let result = parse_envelope_from_bytes(b"[]");
    assert!(result.is_err());
}

#[test]
fn parse_envelope_from_bytes_rejects_missing_head() {
    let json = r#"{"body":{},"tail":{}}"#;
    let result = parse_envelope_from_bytes(json.as_bytes());
    assert!(result.is_err());
}

#[test]
fn parse_envelope_from_bytes_rejects_wrong_head_shape() {
    let json = r#"{"head":[],"body":{}}"#;
    let result = parse_envelope_from_bytes(json.as_bytes());
    assert!(result.is_err());
}

#[test]
fn parse_envelope_from_bytes_rejects_non_string_required_fields() {
    let json = r#"{
      "head": {
        "event_id": 123,
        "event_name": "InvoiceReceivedForExternalDependency",
        "tenant_id": "acme-corp",
        "correlation_id": null,
        "causation_id": null,
        "occurred_at": null,
        "originating_function": null,
        "originating_application": null,
        "environment": null,
        "changed_object_type": null,
        "changed_object_id": null,
        "change_kind": null
      },
      "body": {}
    }"#;

    let result = parse_envelope_from_bytes(json.as_bytes());

    assert!(result.is_err());
}
