use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::Write;

pub const CONTENT_HASH_PREFIX: &str = "sha256:";

/// Computes a fixed-size digest of the idempotency key for fast DB lookups.
pub fn compute_idempotency_hash(key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut hex, "{:02x}", byte).expect("hex write");
    }
    hex
}

/// Computes a deterministic hash for the event body.
/// - Only `body` is hashed
/// - Object keys are sorted
/// - Array order is preserved
pub fn compute_content_hash(body: &serde_json::Value) -> String {
    let canonical = canonicalize_json(body);
    let json = serde_json::to_string(&canonical).unwrap_or_else(|_| "null".to_string());

    let mut hasher = Sha256::new();
    hasher.update(json.as_bytes());
    let digest = hasher.finalize();

    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut hex, "{:02x}", byte).expect("hex write");
    }

    format!("{CONTENT_HASH_PREFIX}{hex}")
}

fn canonicalize_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut ordered = BTreeMap::new();
            for (k, v) in map {
                ordered.insert(k.clone(), canonicalize_json(v));
            }
            serde_json::Value::Object(ordered.into_iter().collect())
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(canonicalize_json).collect())
        }
        _ => value.clone(),
    }
}
