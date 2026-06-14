//! ActionTemplate: reusable configuration that defines what action to take,
//! who is responsible, and what evidence is required when a rule triggers.
//! Templates are linked to rules and playbooks, enabling standardized responses
//! to compliance events, validation failures, and operational exceptions.

use async_trait::async_trait;
use futures::StreamExt;
use log::info;
use mongodb::bson::{Document, doc};
use mongodb::options::FindOptions;
use mongodb::{Client, Collection};
use serde::{Deserialize, Serialize};

/// When should this template trigger?
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TriggerEventType {
    RulePassed,
    RuleFailed,
    RuleInconclusive,
    Manual,
}

/// How should the action be executed?
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionMode {
    Automatic,
    Manual,
    ApprovalRequired,
}

/// Template lifecycle status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TemplateStatus {
    Draft,
    Active,
    Archive,
}

/// Escalation time unit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EscalationDurationUnit {
    Hours,
    Days,
    Weeks,
}

/// Trigger configuration: when and for what object type the action fires.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerConfig {
    pub event_type: TriggerEventType,
    pub object_type: String,
    pub execution_mode: ExecutionMode,
}

/// Responsibility assignment: who performs the action and escalation rules.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponsibilityConfig {
    pub responsible_user: Option<String>,
    pub responsible_role: Option<String>,
    pub escalation_duration: Option<u32>,
    pub escalation_duration_unit: Option<EscalationDurationUnit>,
}

/// Evidence requirements: what supporting data must accompany action completion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvidenceConfig {
    pub require_document_upload: bool,
    pub require_comment: bool,
    pub require_approval_reference: bool,
}

/// A reusable action template that defines the structured response
/// when rule evaluation occurs within the tadpole engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionTemplate {
    pub template_id: String,
    pub tenant_id: String,
    pub name: String,
    pub description: Option<String>,
    pub version: u32,
    pub status: TemplateStatus,
    pub trigger: TriggerConfig,
    pub responsibility: ResponsibilityConfig,
    pub evidence: EvidenceConfig,
    pub associated_rule_ids: Vec<String>,
    pub associated_playbook_ids: Vec<String>,
}

/// Persistence trait for action templates.
#[async_trait]
pub trait ActionTemplateStore: Send + Sync {
    /// List templates, optionally filtered by tenant, status, object type, and event type.
    async fn list_templates(
        &self,
        filter: ActionTemplateListFilter,
        limit: i64,
    ) -> Result<Vec<ActionTemplate>, Box<dyn std::error::Error + Send + Sync>>;

    /// Find all ACTIVE templates matching the given trigger criteria.
    async fn find_templates_by_trigger(
        &self,
        tenant_id: &str,
        object_type: &str,
        event_type: &TriggerEventType,
    ) -> Result<Vec<ActionTemplate>, Box<dyn std::error::Error + Send + Sync>>;

    /// Find a single template by its ID.
    async fn find_template_by_id(
        &self,
        template_id: &str,
    ) -> Result<Option<ActionTemplate>, Box<dyn std::error::Error + Send + Sync>>;
}

#[derive(Debug, Clone, Default)]
pub struct ActionTemplateListFilter {
    pub tenant_id: Option<String>,
    pub status: Option<TemplateStatus>,
    pub object_type: Option<String>,
    pub event_type: Option<TriggerEventType>,
}

/// MongoDB-backed action template store.
#[derive(Clone)]
pub struct MongoActionTemplateStore {
    collection: Collection<Document>,
}

impl MongoActionTemplateStore {
    pub async fn from_env() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let uri = std::env::var("MONGODB_URI")?;
        let db_name = std::env::var("MONGODB_DB").unwrap_or_else(|_| "devdb".to_string());
        let collection_name = std::env::var("ACTION_TEMPLATES_COLLECTION")
            .unwrap_or_else(|_| "action_templates".to_string());
        let client = Client::with_uri_str(&uri).await?;
        let collection = client
            .database(&db_name)
            .collection::<Document>(&collection_name);
        info!(
            "mongodb action templates: db={} collection={}",
            db_name, collection_name
        );
        Ok(Self { collection })
    }

    /// Build from an existing MongoDB client (shares connection with other stores).
    pub fn from_client(client: &Client, db_name: &str, collection_name: &str) -> Self {
        let collection = client
            .database(db_name)
            .collection::<Document>(collection_name);
        Self { collection }
    }
}

#[async_trait]
impl ActionTemplateStore for MongoActionTemplateStore {
    async fn list_templates(
        &self,
        filter: ActionTemplateListFilter,
        limit: i64,
    ) -> Result<Vec<ActionTemplate>, Box<dyn std::error::Error + Send + Sync>> {
        let mut query = Document::new();
        if let Some(tenant_id) = filter.tenant_id {
            query.insert("tenant_id", tenant_id);
        }
        if let Some(status) = filter.status {
            let status_str = serde_json::to_value(status)?
                .as_str()
                .unwrap_or("")
                .to_string();
            query.insert("status", status_str);
        }
        if let Some(object_type) = filter.object_type {
            query.insert("trigger.object_type", object_type);
        }
        if let Some(event_type) = filter.event_type {
            let event_type_str = serde_json::to_value(event_type)?
                .as_str()
                .unwrap_or("")
                .to_string();
            query.insert("trigger.event_type", event_type_str);
        }

        let find_options = FindOptions::builder()
            .limit(limit.clamp(1, 200))
            .sort(doc! { "template_id": 1 })
            .build();
        let mut cursor = self.collection.find(query, find_options).await?;
        let mut templates = Vec::new();
        while let Some(doc_result) = cursor.next().await {
            let doc = doc_result?;
            if let Ok(template) = mongodb::bson::from_document::<ActionTemplate>(doc) {
                templates.push(template);
            }
        }
        Ok(templates)
    }

    async fn find_templates_by_trigger(
        &self,
        tenant_id: &str,
        object_type: &str,
        event_type: &TriggerEventType,
    ) -> Result<Vec<ActionTemplate>, Box<dyn std::error::Error + Send + Sync>> {
        let event_type_str = serde_json::to_value(event_type)?
            .as_str()
            .unwrap_or("")
            .to_string();

        let filter = doc! {
            "tenant_id": tenant_id,
            "trigger.object_type": object_type,
            "trigger.event_type": &event_type_str,
            "status": "ACTIVE",
        };

        let mut cursor = self.collection.find(filter, None).await?;
        let mut templates = Vec::new();
        while let Some(doc_result) = cursor.next().await {
            let doc = doc_result?;
            if let Ok(template) = mongodb::bson::from_document::<ActionTemplate>(doc) {
                templates.push(template);
            }
        }
        Ok(templates)
    }

    async fn find_template_by_id(
        &self,
        template_id: &str,
    ) -> Result<Option<ActionTemplate>, Box<dyn std::error::Error + Send + Sync>> {
        let filter = doc! { "template_id": template_id };
        let opt = self.collection.find_one(filter, None).await?;
        match opt {
            Some(doc) => Ok(mongodb::bson::from_document::<ActionTemplate>(doc).ok()),
            None => Ok(None),
        }
    }
}
