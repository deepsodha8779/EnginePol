use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionCandidate {
    pub kind: String,
    pub target: Option<String>,
    pub reason: String,
}
