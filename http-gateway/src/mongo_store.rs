use async_trait::async_trait;
use domain::envelope::CanonicalEnvelope;
use engine_core::dto::rules::RuleLogic;
use futures::TryStreamExt;
use log::{info, warn};
use mongodb::bson::{self, DateTime, Document, doc};
use mongodb::options::FindOptions;
use mongodb::{Client, Collection};
use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbPlaybookTrigger {
    pub object_type: String,
    #[serde(deserialize_with = "deserialize_change_kind")]
    pub change_kind: Vec<String>,
}

fn deserialize_change_kind<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    struct ChangeKindVisitor;

    impl<'de> Visitor<'de> for ChangeKindVisitor {
        type Value = Vec<String>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a string or a sequence of strings")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(vec![value.to_string()])
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(vec![value])
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut values = Vec::new();
            while let Some(value) = seq.next_element::<String>()? {
                values.push(value);
            }
            Ok(values)
        }
    }

    deserializer.deserialize_any(ChangeKindVisitor)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbPlaybookRuleRef {
    #[serde(default, alias = "orderSeq", alias = "order", alias = "sequence")]
    pub order_seq: u32,
    #[serde(alias = "ruleId")]
    pub rule_id: String,
    #[serde(default)]
    pub is_critical: bool,
    #[serde(default)]
    pub action_template_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbPlaybook {
    pub id: String,
    #[serde(
        default,
        alias = "playbook_name",
        alias = "playbookName",
        alias = "title"
    )]
    pub name: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub execution_mode: Option<String>,
    pub trigger: DbPlaybookTrigger,
    #[serde(default)]
    pub rules: Vec<DbPlaybookRuleRef>,
    #[serde(default)]
    pub status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbRuleObject {
    #[serde(rename = "type")]
    pub object_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbRuleCondition {
    #[serde(alias = "objectKey", alias = "key", alias = "field")]
    pub object_key: String,
    #[serde(default = "default_rule_operator")]
    pub operator: String,
    #[serde(
        default,
        alias = "keyDataType",
        alias = "data_type",
        alias = "dataType"
    )]
    pub key_data_type: Option<String>,
    #[serde(
        default,
        alias = "expected",
        alias = "expected_value",
        alias = "expectedValue"
    )]
    pub value: Option<serde_json::Value>,
    #[serde(
        default,
        alias = "expected_int",
        alias = "expectedInt",
        alias = "valueInt"
    )]
    pub value_int: Option<i64>,
    #[serde(
        default,
        alias = "expected_float",
        alias = "expectedFloat",
        alias = "valueFloat"
    )]
    pub value_float: Option<f64>,
    #[serde(
        default,
        alias = "expected_bool",
        alias = "expectedBool",
        alias = "valueBool"
    )]
    pub value_bool: Option<bool>,
}

fn default_rule_operator() -> String {
    "eq".to_string()
}

impl DbRuleCondition {
    pub fn expected_value(&self) -> Option<serde_json::Value> {
        if let Some(v) = self.value_int {
            return Some(serde_json::Value::Number(v.into()));
        }
        if let Some(v) = self.value_float {
            return serde_json::Number::from_f64(v).map(serde_json::Value::Number);
        }
        if let Some(v) = self.value_bool {
            return Some(serde_json::Value::Bool(v));
        }
        if let Some(v) = &self.value {
            return Some(v.clone());
        }
        None
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbRule {
    pub id: String,
    #[serde(default, alias = "rule_name", alias = "ruleName", alias = "title")]
    pub name: Option<String>,
    pub object: DbRuleObject,
    #[serde(default)]
    pub conditions: Vec<DbRuleCondition>,
    #[serde(default)]
    pub logic: RuleLogic,
    #[serde(default)]
    pub status: Option<String>,
}

#[async_trait]
pub trait IntakeStore: Send + Sync {
    async fn record_intake(
        &self,
        envelope: &CanonicalEnvelope,
        status: &str,
        errors: Option<&Vec<String>>,
        response: Option<&serde_json::Value>,
        error_message: Option<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn list_recent_intake(
        &self,
        limit: i64,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>>;

    async fn append_event_log(
        &self,
        envelope: &CanonicalEnvelope,
        stage: &str,
        level: &str,
        message: &str,
        details: Option<&serde_json::Value>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn list_event_logs_by_event_id(
        &self,
        event_id: &str,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>>;

    async fn find_matched_playbook(
        &self,
        changed_object_type: &str,
        change_kind: &str,
    ) -> Result<Option<DbPlaybook>, Box<dyn std::error::Error + Send + Sync>>;

    async fn find_matched_playbooks(
        &self,
        changed_object_type: &str,
        change_kind: &str,
    ) -> Result<Vec<DbPlaybook>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self
            .find_matched_playbook(changed_object_type, change_kind)
            .await?
            .into_iter()
            .collect())
    }

    async fn find_active_rules(
        &self,
        rule_ids: &[String],
    ) -> Result<Vec<DbRule>, Box<dyn std::error::Error + Send + Sync>>;

    async fn find_playbooks_by_ids(
        &self,
        playbook_ids: &[String],
    ) -> Result<Vec<DbPlaybook>, Box<dyn std::error::Error + Send + Sync>>;

    async fn find_rules_by_ids(
        &self,
        rule_ids: &[String],
    ) -> Result<Vec<DbRule>, Box<dyn std::error::Error + Send + Sync>>;
}

#[derive(Clone)]
pub struct MongoStore {
    intake_collection: Collection<Document>,
    event_logs_collection: Collection<Document>,
    playbooks_collection: Collection<Document>,
    rules_collection: Collection<Document>,
}

impl MongoStore {
    fn change_kind_pattern(change_kind: &str) -> String {
        match change_kind.trim().to_ascii_lowercase().as_str() {
            "created" | "create" => "^create(d)?$".to_string(),
            "updated" | "update" => "^update(d)?$".to_string(),
            "deleted" | "delete" => "^delete(d)?$".to_string(),
            _ => format!("^{}$", change_kind.trim()),
        }
    }

    pub async fn from_env() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let uri = std::env::var("MONGODB_URI")?;
        let db_name = std::env::var("MONGODB_DB")?;
        let intake_collection_name = std::env::var("MONGODB_COLLECTION")?;
        let event_logs_collection_name = std::env::var("MONGODB_EVENT_LOGS_COLLECTION")
            .unwrap_or_else(|_| "event_logs".to_string());
        let playbooks_collection_name = std::env::var("MONGODB_PLAYBOOKS_COLLECTION")
            .unwrap_or_else(|_| "Playbooks".to_string());
        let rules_collection_name =
            std::env::var("MONGODB_RULES_COLLECTION").unwrap_or_else(|_| "Rules".to_string());

        let client = Client::with_uri_str(&uri).await?;
        let db = client.database(&db_name);

        let intake_collection = db.collection::<Document>(&intake_collection_name);
        let event_logs_collection = db.collection::<Document>(&event_logs_collection_name);
        let playbooks_collection = db.collection::<Document>(&playbooks_collection_name);
        let rules_collection = db.collection::<Document>(&rules_collection_name);

        info!(
            "mongodb connected: db={} intake_collection={} event_logs_collection={} playbooks_collection={} rules_collection={}",
            db_name,
            intake_collection_name,
            event_logs_collection_name,
            playbooks_collection_name,
            rules_collection_name
        );
        Ok(Self {
            intake_collection,
            event_logs_collection,
            playbooks_collection,
            rules_collection,
        })
    }

    pub async fn insert_intake(
        &self,
        envelope: &CanonicalEnvelope,
        status: &str,
        errors: Option<&Vec<String>>,
        response: Option<&serde_json::Value>,
        error_message: Option<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let record = Self::build_intake_record(envelope, status, errors, response, error_message)?;
        self.intake_collection.insert_one(record, None).await?;
        Ok(())
    }

    pub async fn insert_event_log(
        &self,
        envelope: &CanonicalEnvelope,
        stage: &str,
        level: &str,
        message: &str,
        details: Option<&serde_json::Value>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let record = Self::build_event_log_record(envelope, stage, level, message, details)?;
        self.event_logs_collection.insert_one(record, None).await?;
        Ok(())
    }

    fn build_intake_record(
        envelope: &CanonicalEnvelope,
        status: &str,
        errors: Option<&Vec<String>>,
        response: Option<&serde_json::Value>,
        error_message: Option<String>,
    ) -> Result<Document, Box<dyn std::error::Error + Send + Sync>> {
        let envelope_bson = bson::to_bson(envelope)?;
        let mut record = doc! {
            "received_at": DateTime::now(),
            "status": status,
            "event_id": envelope.head.event_id.clone(),
            "tenant_id": envelope.head.tenant_id.clone(),
            "event_name": envelope.head.event_name.clone(),
            "envelope": envelope_bson,
        };

        if let Some(errors) = errors {
            record.insert("errors", bson::to_bson(errors)?);
        }
        if let Some(response) = response {
            record.insert("response", bson::to_bson(response)?);
        }
        if let Some(error_message) = error_message {
            record.insert("error_message", error_message);
        }

        Ok(record)
    }

    fn build_event_log_record(
        envelope: &CanonicalEnvelope,
        stage: &str,
        level: &str,
        message: &str,
        details: Option<&serde_json::Value>,
    ) -> Result<Document, Box<dyn std::error::Error + Send + Sync>> {
        let mut record = doc! {
            "logged_at": DateTime::now(),
            "event_id": envelope.head.event_id.clone(),
            "tenant_id": envelope.head.tenant_id.clone(),
            "event_name": envelope.head.event_name.clone(),
            "stage": stage,
            "level": level,
            "message": message,
        };

        if let Some(correlation_id) = &envelope.head.correlation_id {
            record.insert("correlation_id", correlation_id.clone());
        }
        if let Some(causation_id) = &envelope.head.causation_id {
            record.insert("causation_id", causation_id.clone());
        }
        if let Some(changed_object_type) = &envelope.head.changed_object_type {
            record.insert("changed_object_type", changed_object_type.clone());
        }
        if let Some(changed_object_id) = &envelope.head.changed_object_id {
            record.insert("changed_object_id", changed_object_id.clone());
        }
        if let Some(change_kind) = &envelope.head.change_kind {
            record.insert("change_kind", change_kind.clone());
        }
        if let Some(details) = details {
            record.insert("details", bson::to_bson(details)?);
        }

        Ok(record)
    }
}

#[async_trait]
impl IntakeStore for MongoStore {
    async fn record_intake(
        &self,
        envelope: &CanonicalEnvelope,
        status: &str,
        errors: Option<&Vec<String>>,
        response: Option<&serde_json::Value>,
        error_message: Option<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.insert_intake(envelope, status, errors, response, error_message)
            .await
    }

    async fn list_recent_intake(
        &self,
        limit: i64,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
        let options = FindOptions::builder()
            .sort(doc! { "received_at": -1 })
            .limit(Some(limit))
            .build();
        let mut cursor = self.intake_collection.find(None, options).await?;
        let mut records = Vec::new();
        while let Some(document) = cursor.try_next().await? {
            let value: serde_json::Value = bson::from_bson(bson::Bson::Document(document))?;
            records.push(value);
        }
        Ok(records)
    }

    async fn append_event_log(
        &self,
        envelope: &CanonicalEnvelope,
        stage: &str,
        level: &str,
        message: &str,
        details: Option<&serde_json::Value>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.insert_event_log(envelope, stage, level, message, details)
            .await
    }

    async fn list_event_logs_by_event_id(
        &self,
        event_id: &str,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
        let options = FindOptions::builder().sort(doc! { "logged_at": 1 }).build();
        let mut cursor = self
            .event_logs_collection
            .find(doc! { "event_id": event_id }, options)
            .await?;
        let mut records = Vec::new();
        while let Some(document) = cursor.try_next().await? {
            let value: serde_json::Value = bson::from_bson(bson::Bson::Document(document))?;
            records.push(value);
        }
        Ok(records)
    }

    async fn find_matched_playbook(
        &self,
        changed_object_type: &str,
        change_kind: &str,
    ) -> Result<Option<DbPlaybook>, Box<dyn std::error::Error + Send + Sync>> {
        let object_type = changed_object_type.trim();
        let change_kind_regex = Self::change_kind_pattern(change_kind);
        let object_type_re = bson::Regex {
            pattern: format!("^{}$", object_type),
            options: "i".to_string(),
        };
        let change_kind_re = bson::Regex {
            pattern: change_kind_regex.clone(),
            options: "i".to_string(),
        };
        let status_re = bson::Regex {
            pattern: "^active$".to_string(),
            options: "i".to_string(),
        };
        let filter = doc! {
            "trigger.object_type": object_type_re,
            "trigger.change_kind": change_kind_re,
            "status": status_re,
        };

        let Some(document) = self.playbooks_collection.find_one(filter, None).await? else {
            warn!(
                "no playbook matched: object_type={} change_kind={} change_kind_regex={}",
                object_type, change_kind, change_kind_regex
            );
            return Ok(None);
        };

        let playbook: DbPlaybook = bson::from_document(document)?;
        info!(
            "playbook matched: id={} name={:?} object_type={} change_kind={}",
            playbook.id, playbook.name, object_type, change_kind
        );
        Ok(Some(playbook))
    }

    async fn find_matched_playbooks(
        &self,
        changed_object_type: &str,
        change_kind: &str,
    ) -> Result<Vec<DbPlaybook>, Box<dyn std::error::Error + Send + Sync>> {
        let object_type = changed_object_type.trim();
        let change_kind_regex = Self::change_kind_pattern(change_kind);
        let object_type_re = bson::Regex {
            pattern: format!("^{}$", object_type),
            options: "i".to_string(),
        };
        let change_kind_re = bson::Regex {
            pattern: change_kind_regex.clone(),
            options: "i".to_string(),
        };
        let status_re = bson::Regex {
            pattern: "^active$".to_string(),
            options: "i".to_string(),
        };
        let filter = doc! {
            "trigger.object_type": object_type_re,
            "trigger.change_kind": change_kind_re,
            "status": status_re,
        };

        let mut cursor = self.playbooks_collection.find(filter, None).await?;
        let mut playbooks = Vec::new();
        while let Some(document) = cursor.try_next().await? {
            playbooks.push(bson::from_document(document)?);
        }

        if playbooks.is_empty() {
            warn!(
                "no playbook matched: object_type={} change_kind={} change_kind_regex={}",
                object_type, change_kind, change_kind_regex
            );
        } else {
            info!(
                "playbooks matched: count={} object_type={} change_kind={}",
                playbooks.len(),
                object_type,
                change_kind
            );
        }

        Ok(playbooks)
    }

    async fn find_active_rules(
        &self,
        rule_ids: &[String],
    ) -> Result<Vec<DbRule>, Box<dyn std::error::Error + Send + Sync>> {
        if rule_ids.is_empty() {
            return Ok(Vec::new());
        }

        let filter = doc! {
            "id": { "$in": bson::to_bson(rule_ids)? },
            "status": doc! { "$regex": "^active$", "$options": "i" },
        };
        let mut cursor = self.rules_collection.find(filter, None).await?;
        let mut rules = Vec::new();
        while let Some(document) = cursor.try_next().await? {
            let rule: DbRule = bson::from_document(document)?;
            rules.push(rule);
        }
        Ok(rules)
    }

    async fn find_playbooks_by_ids(
        &self,
        playbook_ids: &[String],
    ) -> Result<Vec<DbPlaybook>, Box<dyn std::error::Error + Send + Sync>> {
        if playbook_ids.is_empty() {
            return Ok(Vec::new());
        }

        let filter = doc! { "id": { "$in": bson::to_bson(playbook_ids)? } };
        let mut cursor = self.playbooks_collection.find(filter, None).await?;
        let mut playbooks = Vec::new();
        while let Some(document) = cursor.try_next().await? {
            playbooks.push(bson::from_document(document)?);
        }
        Ok(playbooks)
    }

    async fn find_rules_by_ids(
        &self,
        rule_ids: &[String],
    ) -> Result<Vec<DbRule>, Box<dyn std::error::Error + Send + Sync>> {
        if rule_ids.is_empty() {
            return Ok(Vec::new());
        }

        let filter = doc! { "id": { "$in": bson::to_bson(rule_ids)? } };
        let mut cursor = self.rules_collection.find(filter, None).await?;
        let mut rules = Vec::new();
        while let Some(document) = cursor.try_next().await? {
            rules.push(bson::from_document(document)?);
        }
        Ok(rules)
    }
}

#[cfg(test)]
mod tests {
    use super::{DbPlaybook, DbRule, MongoStore};
    use domain::envelope::{CanonicalEnvelope, TadpoleHead};
    use mongodb::bson::doc;

    fn sample_envelope() -> CanonicalEnvelope {
        CanonicalEnvelope {
            head: TadpoleHead {
                event_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_string(),
                event_name: "InvoiceReceivedForExternalDependency".to_string(),
                event_category: Some("Transaction".to_string()),
                tenant_id: "acme-corp".to_string(),
                correlation_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAA".to_string()),
                causation_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAB".to_string()),
                occurred_at: Some("2026-02-23T12:34:56Z".to_string()),
                originating_function: Some("Finance".to_string()),
                originating_application: Some("AccountingIntegration".to_string()),
                environment: Some("prd".to_string()),
                external_dependency_id: Some("dep_123".to_string()),
                changed_object_type: Some("contract".to_string()),
                changed_object_id: Some("ctr_987".to_string()),
                change_kind: Some("CREATED".to_string()),
            },
            body: serde_json::json!({ "snapshots": { "Contract": { "value": 10 } } }),
        }
    }

    #[test]
    fn build_intake_record_contains_input_and_output() {
        let envelope = sample_envelope();
        let errors = vec!["some error".to_string()];
        let response = serde_json::json!([{ "status": "PASS" }]);

        let record = MongoStore::build_intake_record(
            &envelope,
            "success",
            Some(&errors),
            Some(&response),
            Some("none".to_string()),
        )
        .expect("record");

        assert_eq!(record.get_str("status").expect("status"), "success");
        assert_eq!(
            record.get_str("event_id").expect("event_id"),
            envelope.head.event_id
        );
        assert!(record.get("envelope").is_some());
        assert!(record.get("response").is_some());
        assert!(record.get("errors").is_some());
        assert_eq!(
            record.get_str("error_message").expect("error_message"),
            "none"
        );
    }

    #[test]
    fn build_event_log_record_contains_event_fields_and_details() {
        let envelope = sample_envelope();
        let details = serde_json::json!({
            "status": "success",
            "step": "dispatch",
        });

        let record = MongoStore::build_event_log_record(
            &envelope,
            "pipeline.dispatch",
            "INFO",
            "dispatcher completed",
            Some(&details),
        )
        .expect("event log record");

        assert_eq!(
            record.get_str("event_id").expect("event_id"),
            envelope.head.event_id
        );
        assert_eq!(record.get_str("stage").expect("stage"), "pipeline.dispatch");
        assert_eq!(record.get_str("level").expect("level"), "INFO");
        assert_eq!(
            record.get_str("message").expect("message"),
            "dispatcher completed"
        );
        assert!(record.get("logged_at").is_some());
        assert!(record.get("details").is_some());
    }

    #[test]
    fn db_playbook_trigger_accepts_change_kind_as_string() {
        let document = doc! {
            "id": "playbook.invoice_governance",
            "trigger": {
                "object_type": "invoice",
                "change_kind": "CREATED",
            },
            "status": "active",
        };

        let parsed: DbPlaybook = mongodb::bson::from_document(document).expect("playbook parse");
        assert_eq!(parsed.trigger.change_kind, vec!["CREATED"]);
    }

    #[test]
    fn db_playbook_trigger_accepts_change_kind_as_array() {
        let document = doc! {
            "id": "playbook.invoice_governance",
            "trigger": {
                "object_type": "invoice",
                "change_kind": ["created", "updated"],
            },
            "status": "active",
        };

        let parsed: DbPlaybook = mongodb::bson::from_document(document).expect("playbook parse");
        assert_eq!(parsed.trigger.change_kind, vec!["created", "updated"]);
    }

    #[test]
    fn db_playbook_rule_ref_defaults_missing_order_seq() {
        let document = doc! {
            "id": "playbook.invoice_governance",
            "trigger": {
                "object_type": "invoice",
                "change_kind": "created",
            },
            "rules": [{
                "rule_id": "rule.invoice_amount",
                "is_critical": true,
            }],
            "status": "active",
        };

        let parsed: DbPlaybook = mongodb::bson::from_document(document).expect("playbook parse");
        let rule_ref = parsed.rules.first().expect("rule ref");
        assert_eq!(rule_ref.order_seq, 0);
        assert_eq!(rule_ref.rule_id, "rule.invoice_amount");
        assert!(rule_ref.is_critical);
    }

    #[test]
    fn db_playbook_rule_ref_accepts_camel_case_aliases() {
        let document = doc! {
            "id": "playbook.invoice_governance",
            "trigger": {
                "object_type": "invoice",
                "change_kind": "created",
            },
            "rules": [{
                "ruleId": "rule.invoice_amount",
                "orderSeq": 12,
            }],
            "status": "active",
        };

        let parsed: DbPlaybook = mongodb::bson::from_document(document).expect("playbook parse");
        let rule_ref = parsed.rules.first().expect("rule ref");
        assert_eq!(rule_ref.order_seq, 12);
        assert_eq!(rule_ref.rule_id, "rule.invoice_amount");
    }

    #[test]
    fn db_playbook_accepts_playbook_name_alias() {
        let document = doc! {
            "id": "playbook.invoice_governance",
            "playbook_name": "Invoice Governance",
            "trigger": {
                "object_type": "invoice",
                "change_kind": "CREATED",
            },
            "status": "active",
        };

        let parsed: DbPlaybook = mongodb::bson::from_document(document).expect("playbook parse");
        assert_eq!(parsed.name.as_deref(), Some("Invoice Governance"));
    }

    #[test]
    fn db_rule_accepts_rule_name_alias() {
        let document = doc! {
            "id": "rule.invoice_amount",
            "rule_name": "Invoice Amount Check",
            "object": { "type": "invoice" },
            "status": "active",
        };

        let parsed: DbRule = mongodb::bson::from_document(document).expect("rule parse");
        assert_eq!(parsed.name.as_deref(), Some("Invoice Amount Check"));
    }

    #[test]
    fn db_rule_condition_accepts_common_expected_value_aliases() {
        let document = doc! {
            "id": "rule.invoice_amount",
            "ruleName": "Invoice Amount Check",
            "object": { "type": "invoice" },
            "conditions": [{
                "objectKey": "amount",
                "operator": "eq",
                "keyDataType": "NUMBER",
                "expected_value": 12500,
            }],
            "status": "active",
        };

        let parsed: DbRule = mongodb::bson::from_document(document).expect("rule parse");
        let condition = parsed.conditions.first().expect("condition");
        assert_eq!(condition.object_key, "amount");
        assert_eq!(condition.key_data_type.as_deref(), Some("NUMBER"));
        assert_eq!(condition.expected_value(), Some(serde_json::json!(12500)));
    }
}
