use domain::envelope::{
    CONTENT_HASH_PREFIX, CanonicalEnvelope, compute_content_hash, is_valid_ulid,
};

#[test]
fn canonical_envelope_defaults_missing_body_to_empty_object() {
    let parsed: CanonicalEnvelope = serde_json::from_str(
        r#"{
            "head": {
                "event_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
                "event_name": "ContractCreated",
                "tenant_id": "tenant-a",
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
        }"#,
    )
    .expect("deserialize envelope");

    assert_eq!(parsed.body, serde_json::json!({}));
}

#[test]
fn content_hash_has_sha256_prefix() {
    let hash = compute_content_hash(&serde_json::json!({"b": 2, "a": 1}));

    assert!(hash.starts_with(CONTENT_HASH_PREFIX));
}

#[test]
fn ulid_validation_accepts_valid() {
    // Example ULID from spec-style examples.
    assert!(is_valid_ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV"));
}

#[test]
fn ulid_validation_rejects_invalid_length() {
    assert!(!is_valid_ulid("01ARZ3NDEKTSV4RRFFQ69G5FA"));
}

#[test]
fn ulid_validation_rejects_invalid_chars() {
    // "I", "L", "O", "U" are not allowed in Crockford base32.
    assert!(!is_valid_ulid("01ARZ3NDEKTSV4RRFFQ69G5FAI"));
}

#[test]
fn ulid_validation_accepts_lowercase() {
    assert!(is_valid_ulid("01arz3ndektsv4rrffq69g5fav"));
}

#[test]
fn ulid_validation_rejects_symbols() {
    assert!(!is_valid_ulid("01ARZ3NDEKTSV4RRFFQ69G5FA-"));
}
