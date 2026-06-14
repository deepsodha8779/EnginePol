//! Action persistence: idempotency check and insert for created tasks.

use async_trait::async_trait;
use log::info;
use mongodb::bson::{DateTime, Document, doc};
use mongodb::{Client, Collection};

/// A created action record stored in MongoDB.
#[derive(Debug, Clone)]
pub struct ActionRecord {
    pub action_id: String,
    pub idempotency_key: String,
    pub idempotency_hash: String,
    pub tenant_id: String,
    pub event_id: String,
    pub event_name: String,
    pub playbook_id: String,
    pub rule_id: String,
    pub task_type: String,
    pub status: String,
    pub changed_object_type: Option<String>,
    pub changed_object_id: Option<String>,
    pub created_at: DateTime,
    // Template-derived fields (populated when an ActionTemplate is matched)
    pub action_template_id: Option<String>,
    pub execution_mode: Option<String>,
    pub responsible_user: Option<String>,
    pub responsible_role: Option<String>,
    pub escalation_duration: Option<u32>,
    pub escalation_duration_unit: Option<String>,
    pub require_document_upload: Option<bool>,
    pub require_comment: Option<bool>,
    pub require_approval_reference: Option<bool>,
    // Short user-facing label for the action (e.g. "Governance Review Required")
    pub action_title: Option<String>,
    // Detailed description of what failed or what needs to be done (no rule names)
    pub action_description: Option<String>,
    // Work routing fields (populated after WorkRouter resolves the action)
    pub assigned_to_type: Option<String>,
    pub assigned_to_id: Option<String>,
    pub assigned_to_name: Option<String>,
}

#[async_trait]
pub trait ActionStore: Send + Sync {
    /// Returns an existing action if one exists with this idempotency hash.
    async fn find_by_idempotency_hash(
        &self,
        idempotency_hash: &str,
    ) -> Result<Option<ActionRecord>, Box<dyn std::error::Error + Send + Sync>>;

    /// Inserts a new action. Caller must have already checked for duplicates.
    async fn insert_action(
        &self,
        record: &ActionRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Update the assignee fields on an existing action after work routing.
    async fn update_action_assignment(
        &self,
        action_id: &str,
        assigned_to_type: &str,
        assigned_to_id: &str,
        assigned_to_name: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

#[derive(Clone)]
pub struct MongoActionStore {
    collection: Collection<Document>,
}

impl MongoActionStore {
    pub async fn from_env() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let uri = std::env::var("MONGODB_URI")?;
        let db_name = std::env::var("MONGODB_DB").unwrap_or_else(|_| "devdb".to_string());
        let collection_name =
            std::env::var("ACTIONS_COLLECTION").unwrap_or_else(|_| "actions".to_string());
        let client = Client::with_uri_str(&uri).await?;
        let collection = client
            .database(&db_name)
            .collection::<Document>(&collection_name);
        info!(
            "mongodb actions: db={} collection={}",
            db_name, collection_name
        );
        Ok(Self { collection })
    }

    fn doc_to_record(doc: &Document) -> Option<ActionRecord> {
        let action_id = doc.get_str("action_id").ok()?.to_string();
        let idempotency_key = doc.get_str("idempotency_key").ok()?.to_string();
        let idempotency_hash = doc.get_str("idempotency_hash").ok()?.to_string();
        let tenant_id = doc.get_str("tenant_id").ok()?.to_string();
        let event_id = doc.get_str("event_id").ok()?.to_string();
        let event_name = doc.get_str("event_name").ok()?.to_string();
        let playbook_id = doc.get_str("playbook_id").ok()?.to_string();
        let rule_id = doc.get_str("rule_id").ok()?.to_string();
        let task_type = doc.get_str("task_type").ok()?.to_string();
        let status = doc.get_str("status").ok()?.to_string();
        let changed_object_type = doc.get_str("changed_object_type").ok().map(String::from);
        let changed_object_id = doc.get_str("changed_object_id").ok().map(String::from);
        let created_at = doc.get_datetime("created_at").ok()?.clone();
        let action_template_id = doc.get_str("action_template_id").ok().map(String::from);
        let execution_mode = doc.get_str("execution_mode").ok().map(String::from);
        let responsible_user = doc.get_str("responsible_user").ok().map(String::from);
        let responsible_role = doc.get_str("responsible_role").ok().map(String::from);
        let escalation_duration = doc.get_i32("escalation_duration").ok().map(|v| v as u32);
        let escalation_duration_unit = doc
            .get_str("escalation_duration_unit")
            .ok()
            .map(String::from);
        let require_document_upload = doc.get_bool("require_document_upload").ok();
        let require_comment = doc.get_bool("require_comment").ok();
        let require_approval_reference = doc.get_bool("require_approval_reference").ok();
        let action_title = doc.get_str("action_title").ok().map(String::from);
        let action_description = doc.get_str("action_description").ok().map(String::from);
        let assigned_to_type = doc.get_str("assigned_to_type").ok().map(String::from);
        let assigned_to_id = doc.get_str("assigned_to_id").ok().map(String::from);
        let assigned_to_name = doc.get_str("assigned_to_name").ok().map(String::from);
        Some(ActionRecord {
            action_id,
            idempotency_key,
            idempotency_hash,
            tenant_id,
            event_id,
            event_name,
            playbook_id,
            rule_id,
            task_type,
            status,
            changed_object_type,
            changed_object_id,
            created_at,
            action_template_id,
            execution_mode,
            responsible_user,
            responsible_role,
            escalation_duration,
            escalation_duration_unit,
            require_document_upload,
            require_comment,
            require_approval_reference,
            action_title,
            action_description,
            assigned_to_type,
            assigned_to_id,
            assigned_to_name,
        })
    }

    fn record_to_doc(record: &ActionRecord) -> Document {
        let mut doc = doc! {
            "action_id": record.action_id.clone(),
            "idempotency_key": record.idempotency_key.clone(),
            "idempotency_hash": record.idempotency_hash.clone(),
            "tenant_id": record.tenant_id.clone(),
            "event_id": record.event_id.clone(),
            "event_name": record.event_name.clone(),
            "playbook_id": record.playbook_id.clone(),
            "rule_id": record.rule_id.clone(),
            "task_type": record.task_type.clone(),
            "status": record.status.clone(),
            "created_at": record.created_at,
        };
        if let Some(ref v) = record.changed_object_type {
            doc.insert("changed_object_type", v.clone());
        }
        if let Some(ref v) = record.changed_object_id {
            doc.insert("changed_object_id", v.clone());
        }
        if let Some(ref v) = record.action_template_id {
            doc.insert("action_template_id", v.clone());
        }
        if let Some(ref v) = record.execution_mode {
            doc.insert("execution_mode", v.clone());
        }
        if let Some(ref v) = record.responsible_user {
            doc.insert("responsible_user", v.clone());
        }
        if let Some(ref v) = record.responsible_role {
            doc.insert("responsible_role", v.clone());
        }
        if let Some(v) = record.escalation_duration {
            doc.insert("escalation_duration", v as i32);
        }
        if let Some(ref v) = record.escalation_duration_unit {
            doc.insert("escalation_duration_unit", v.clone());
        }
        if let Some(v) = record.require_document_upload {
            doc.insert("require_document_upload", v);
        }
        if let Some(v) = record.require_comment {
            doc.insert("require_comment", v);
        }
        if let Some(v) = record.require_approval_reference {
            doc.insert("require_approval_reference", v);
        }
        if let Some(ref v) = record.action_title {
            doc.insert("action_title", v.clone());
        }
        if let Some(ref v) = record.action_description {
            doc.insert("action_description", v.clone());
        }
        if let Some(ref v) = record.assigned_to_type {
            doc.insert("assigned_to_type", v.clone());
        }
        if let Some(ref v) = record.assigned_to_id {
            doc.insert("assigned_to_id", v.clone());
        }
        if let Some(ref v) = record.assigned_to_name {
            doc.insert("assigned_to_name", v.clone());
        }
        doc
    }
}

#[async_trait]
impl ActionStore for MongoActionStore {
    async fn find_by_idempotency_hash(
        &self,
        idempotency_hash: &str,
    ) -> Result<Option<ActionRecord>, Box<dyn std::error::Error + Send + Sync>> {
        let filter = doc! { "idempotency_hash": idempotency_hash };
        let opt = self.collection.find_one(filter, None).await?;
        Ok(opt.and_then(|d| Self::doc_to_record(&d)))
    }

    async fn insert_action(
        &self,
        record: &ActionRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let doc = Self::record_to_doc(record);
        self.collection.insert_one(doc, None).await?;
        Ok(())
    }

    async fn update_action_assignment(
        &self,
        action_id: &str,
        assigned_to_type: &str,
        assigned_to_id: &str,
        assigned_to_name: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let filter = doc! { "action_id": action_id };
        let mut update_doc = doc! {
            "assigned_to_type": assigned_to_type,
            "assigned_to_id": assigned_to_id,
        };
        if let Some(name) = assigned_to_name {
            update_doc.insert("assigned_to_name", name);
        }
        let update = doc! { "$set": update_doc };
        self.collection.update_one(filter, update, None).await?;
        Ok(())
    }
}
