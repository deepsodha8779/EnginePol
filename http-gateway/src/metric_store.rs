use async_trait::async_trait;
use chrono::NaiveDateTime;
use futures::TryStreamExt;
use log::info;
use mongodb::bson::{self, Bson, DateTime, Document, doc};
use mongodb::options::UpdateOptions;
use mongodb::{Client, Collection};

use crate::metric_manager::{
    METRIC_TYPE_ACTION_TRIGGERED, METRIC_TYPE_RULE_FAIL, METRIC_TYPE_RULE_INCONCLUSIVE,
    METRIC_TYPE_RULE_PASS,
};
use crate::metric_manager::{MetricRecord, MetricStore, MetricWindowQuery};

type DynError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone)]
pub struct MongoMetricStore {
    collection: Collection<Document>,
}

impl MongoMetricStore {
    pub async fn from_env() -> Result<Self, DynError> {
        let uri = std::env::var("MONGODB_URI")?;
        let db_name = std::env::var("MONGODB_DB")?;
        let collection_name = std::env::var("MONGODB_METRICS_COLLECTION")
            .unwrap_or_else(|_| "metrics_collection".to_string());
        let client = Client::with_uri_str(&uri).await?;
        let collection = client
            .database(&db_name)
            .collection::<Document>(&collection_name);
        info!(
            "mongodb metric store connected: db={} metrics_collection={}",
            db_name, collection_name
        );
        Ok(Self { collection })
    }

    #[cfg(test)]
    fn build_metric_response_document(response: &serde_json::Value) -> Result<Document, DynError> {
        Ok(bson::to_document(response)?)
    }

    fn parse_metric_document(document: Document) -> Result<MetricRecord, DynError> {
        Ok(bson::from_document(document)?)
    }

    fn parse_metric_documents(document: Document) -> Result<Vec<MetricRecord>, DynError> {
        if document.get_document("header").is_ok() && document.get_array("results").is_ok() {
            return Self::flatten_metric_response_document(&document);
        }
        Ok(vec![Self::parse_metric_document(document)?])
    }

    fn flatten_metric_response_document(
        document: &Document,
    ) -> Result<Vec<MetricRecord>, DynError> {
        let header = document.get_document("header")?;
        let event_id = header.get_str("event_id").unwrap_or_default().to_string();
        let tenant_id = header.get_str("tenant_id").unwrap_or_default().to_string();
        let event_name = header.get_str("event_name").unwrap_or_default().to_string();
        let fallback_timestamp = parse_metric_timestamp(header.get("timestamp"));

        let mut records = Vec::new();
        for result in document.get_array("results")? {
            let Some(result_doc) = result.as_document() else {
                continue;
            };
            let Ok(playbook) = result_doc.get_document("playbook") else {
                continue;
            };
            let playbook_id = optional_string(playbook.get("playbook_id"));
            let playbook_name = optional_string(playbook.get("playbook_name"));

            if let Ok(rules) = playbook.get_array("rules") {
                for rule in rules {
                    let Some(rule_doc) = rule.as_document() else {
                        continue;
                    };
                    let metric_type = response_metric_type_to_store(
                        rule_doc.get_str("metric_type").unwrap_or_default(),
                    );
                    let Some(metric_type) = metric_type else {
                        continue;
                    };
                    let rule_id = optional_string(rule_doc.get("rule_id"));
                    let timestamp = parse_metric_timestamp(rule_doc.get("timestamp"))
                        .unwrap_or_else(|| fallback_timestamp.unwrap_or_else(DateTime::now));
                    let metadata = serde_json::json!({
                        "reason_code": optional_string(rule_doc.get("reason_code")),
                        "reason": optional_string(rule_doc.get("reason")),
                        "event_name": event_name,
                        "rule_name": optional_string(rule_doc.get("rule_name")),
                        "playbook_name": playbook_name,
                    });
                    records.push(MetricRecord {
                        dedupe_key: format!(
                            "{}:{}:{}:{}:{}",
                            tenant_id,
                            event_id,
                            metric_type,
                            playbook_id.as_deref().unwrap_or_default(),
                            rule_id.as_deref().unwrap_or_default()
                        ),
                        event_id: event_id.clone(),
                        tenant_id: tenant_id.clone(),
                        metric_type,
                        value: 1,
                        timestamp,
                        playbook_id: playbook_id.clone(),
                        rule_id,
                        action_id: None,
                        threshold_id: None,
                        metadata: Some(metadata),
                    });
                }
            }

            if let Ok(actions) = playbook.get_array("actions_triggered") {
                for action in actions {
                    let Some(action_doc) = action.as_document() else {
                        continue;
                    };
                    let action_id = optional_string(action_doc.get("action_id"));
                    let rule_id = optional_string(action_doc.get("rule_id"));
                    let timestamp = parse_metric_timestamp(action_doc.get("timestamp"))
                        .unwrap_or_else(|| fallback_timestamp.unwrap_or_else(DateTime::now));
                    let metadata = serde_json::json!({
                        "task_type": optional_string(action_doc.get("task_type")),
                        "status": optional_string(action_doc.get("status")),
                        "event_name": event_name,
                    });
                    records.push(MetricRecord {
                        dedupe_key: format!(
                            "{}:{}:{}:{}",
                            tenant_id,
                            event_id,
                            METRIC_TYPE_ACTION_TRIGGERED,
                            action_id.as_deref().unwrap_or_default()
                        ),
                        event_id: event_id.clone(),
                        tenant_id: tenant_id.clone(),
                        metric_type: METRIC_TYPE_ACTION_TRIGGERED.to_string(),
                        value: 1,
                        timestamp,
                        playbook_id: playbook_id.clone(),
                        rule_id,
                        action_id,
                        threshold_id: None,
                        metadata: Some(metadata),
                    });
                }
            }
        }

        Ok(records)
    }
}

#[async_trait]
impl MetricStore for MongoMetricStore {
    async fn upsert_metric(&self, record: &MetricRecord) -> Result<bool, DynError> {
        let filter = doc! { "dedupe_key": &record.dedupe_key };
        let update = doc! {
            "$setOnInsert": {
                "dedupe_key": &record.dedupe_key,
                "event_id": &record.event_id,
                "tenant_id": &record.tenant_id,
                "metric_type": &record.metric_type,
                "value": record.value,
            },
            "$set": {
                "timestamp": record.timestamp,
                "playbook_id": bson::to_bson(&record.playbook_id)?,
                "rule_id": bson::to_bson(&record.rule_id)?,
                "action_id": bson::to_bson(&record.action_id)?,
                "threshold_id": bson::to_bson(&record.threshold_id)?,
                "metadata": bson::to_bson(&record.metadata)?,
            },
        };
        let result = self
            .collection
            .update_one(
                filter,
                update,
                UpdateOptions::builder().upsert(true).build(),
            )
            .await?;
        Ok(result.upserted_id.is_some())
    }

    async fn upsert_metrics_response(
        &self,
        records: &[MetricRecord],
        response: &serde_json::Value,
    ) -> Result<Vec<bool>, DynError> {
        let Some(first_record) = records.first() else {
            return Ok(Vec::new());
        };
        let event_id = &first_record.event_id;
        let filter = doc! {
            "$or": [
                { "event_id": event_id },
                { "header.event_id": event_id },
            ],
        };
        self.collection.delete_many(filter, None).await?;
        let document = bson::to_document(response)?;
        self.collection.insert_one(document, None).await?;
        Ok(vec![true; records.len()])
    }

    async fn sum_metric_values(&self, query: &MetricWindowQuery) -> Result<i64, DynError> {
        let records = self.list_metrics().await?;
        Ok(records
            .iter()
            .filter(|record| {
                record.tenant_id == query.tenant_id
                    && record.metric_type == query.metric_type
                    && record.timestamp >= query.since
                    && query
                        .playbook_id
                        .as_ref()
                        .is_none_or(|value| record.playbook_id.as_ref() == Some(value))
                    && query
                        .rule_id
                        .as_ref()
                        .is_none_or(|value| record.rule_id.as_ref() == Some(value))
            })
            .map(|record| record.value)
            .sum())
    }

    async fn list_metrics(&self) -> Result<Vec<MetricRecord>, DynError> {
        let mut cursor = self.collection.find(doc! {}, None).await?;
        let mut records = Vec::new();
        while let Some(document) = cursor.try_next().await? {
            records.extend(Self::parse_metric_documents(document)?);
        }
        records.sort_by(|left, right| right.timestamp.cmp(&left.timestamp));
        Ok(records)
    }

    async fn list_metrics_by_event_id(
        &self,
        event_id: &str,
    ) -> Result<Vec<MetricRecord>, DynError> {
        let mut cursor = self
            .collection
            .find(
                doc! {
                    "$or": [
                        { "event_id": event_id },
                        { "header.event_id": event_id },
                    ],
                },
                None,
            )
            .await?;
        let mut records = Vec::new();
        while let Some(document) = cursor.try_next().await? {
            records.extend(Self::parse_metric_documents(document)?);
        }
        records.sort_by(|left, right| right.timestamp.cmp(&left.timestamp));
        Ok(records)
    }
}

fn optional_string(value: Option<&Bson>) -> Option<String> {
    match value {
        Some(Bson::String(value)) if !value.is_empty() => Some(value.clone()),
        _ => None,
    }
}

fn parse_metric_timestamp(value: Option<&Bson>) -> Option<DateTime> {
    match value {
        Some(Bson::DateTime(value)) => Some(*value),
        Some(Bson::String(value)) => NaiveDateTime::parse_from_str(value, "%d-%m-%Y %I:%M %p")
            .ok()
            .map(|value| DateTime::from_millis(value.and_utc().timestamp_millis())),
        _ => None,
    }
}

fn response_metric_type_to_store(metric_type: &str) -> Option<String> {
    match metric_type {
        "RULE_PASS" => Some(METRIC_TYPE_RULE_PASS.to_string()),
        "RULE_FAIL" => Some(METRIC_TYPE_RULE_FAIL.to_string()),
        "RULE_INCONCLUSIVE" => Some(METRIC_TYPE_RULE_INCONCLUSIVE.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metric_manager::METRIC_TYPE_RULE_FAIL;

    #[test]
    fn build_metric_response_document_contains_required_shape() {
        let response = serde_json::json!({
            "header": {
                "event_name": "ContractCreated",
                "event_id": "evt_1",
                "tenant_id": "tenant_a",
                "timestamp": "13-05-2026 10:00 AM",
                "total_playbooks_triggered": 1,
                "total_rules_triggered": 1,
                "status": "fail"
            },
            "results": [
                {
                    "playbook": {
                        "playbook_id": "playbook.a",
                        "playbook_name": "Playbook A",
                        "rules": [
                            {
                                "rule_id": "rule.a",
                                "rule_name": "Rule A",
                                "metric_type": "RULE_FAIL",
                                "result": "FAIL",
                                "reason_code": "rule.conditions_fail",
                                "reason": "test",
                                "timestamp": "13-05-2026 10:00 AM"
                            }
                        ],
                        "actions_triggered": []
                    }
                }
            ]
        });

        let document =
            MongoMetricStore::build_metric_response_document(&response).expect("metric document");
        let header = document.get_document("header").expect("header");
        assert_eq!(header.get_str("event_id").expect("event_id"), "evt_1");
        assert_eq!(document.get_array("results").expect("results").len(), 1);
    }

    #[test]
    fn grouped_metric_response_document_flattens_to_metric_records() {
        let response = serde_json::json!({
            "header": {
                "event_name": "ContractCreated",
                "event_id": "evt_1",
                "tenant_id": "tenant_a",
                "timestamp": "13-05-2026 10:00 AM",
                "total_playbooks_triggered": 1,
                "total_rules_triggered": 1,
                "status": "fail"
            },
            "results": [
                {
                    "playbook": {
                        "playbook_id": "playbook.a",
                        "playbook_name": "Playbook A",
                        "rules": [
                            {
                                "rule_id": "rule.a",
                                "rule_name": "Rule A",
                                "metric_type": "RULE_FAIL",
                                "result": "FAIL",
                                "reason_code": "rule.conditions_fail",
                                "reason": "test",
                                "timestamp": "13-05-2026 10:00 AM"
                            }
                        ],
                        "actions_triggered": [
                            {
                                "rule_id": "rule.a",
                                "action_id": "action.a",
                                "task_type": "TASK_GOVERNANCE_REVIEW",
                                "status": "CREATED",
                                "timestamp": "13-05-2026 10:00 AM"
                            }
                        ]
                    }
                }
            ]
        });
        let document =
            MongoMetricStore::build_metric_response_document(&response).expect("metric document");

        let records =
            MongoMetricStore::flatten_metric_response_document(&document).expect("records");

        assert_eq!(records.len(), 2);
        assert!(
            records
                .iter()
                .any(|record| record.metric_type == METRIC_TYPE_RULE_FAIL)
        );
        assert!(
            records
                .iter()
                .any(|record| record.metric_type == METRIC_TYPE_ACTION_TRIGGERED
                    && record.action_id.as_deref() == Some("action.a"))
        );
    }
}
