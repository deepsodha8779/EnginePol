use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TadpoleHead {
    pub event_id: String,
    pub event_name: String,
    #[serde(default)]
    pub event_category: Option<String>,
    pub tenant_id: String,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
    pub occurred_at: Option<String>,
    pub originating_function: Option<String>,
    pub originating_application: Option<String>,
    pub environment: Option<String>,
    #[serde(default)]
    pub external_dependency_id: Option<String>,
    pub changed_object_type: Option<String>,
    pub changed_object_id: Option<String>,
    pub change_kind: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalEnvelope {
    pub head: TadpoleHead,
    #[serde(default = "default_body")]
    pub body: serde_json::Value,
}

fn default_body() -> serde_json::Value {
    serde_json::json!({})
}

#[derive(Debug, Deserialize)]
pub struct IntakeEnvelope {
    pub event_name: String,
    pub object_id: String,
    pub tenant_id: String,
}
