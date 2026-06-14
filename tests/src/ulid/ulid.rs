use engine_core::ulid::UlidService;
use std::collections::HashSet;

#[test]
fn generates_unique_and_monotonic_ulids() {
    let service = UlidService::new();
    let mut seen = HashSet::new();
    let mut ids = Vec::new();

    for _ in 0..256 {
        let id = service.generate_string();
        assert_eq!(id.len(), 26, "ULID should be 26 characters");
        assert!(seen.insert(id.clone()), "duplicate ULID generated");
        ids.push(id);
    }

    let mut sorted = ids.clone();
    sorted.sort_unstable();
    assert_eq!(ids, sorted, "ULIDs should be monotonically increasing");
}
