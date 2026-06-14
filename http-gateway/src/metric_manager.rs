use async_trait::async_trait;
use domain::envelope::CanonicalEnvelope;
use engine_core::dto::{decision::Decision, orchestration::OrchestrationResult};
use engine_core::ulid::UlidService;
use mongodb::bson::DateTime;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

fn format_metric_timestamp(ts: &DateTime) -> String {
    chrono::DateTime::from_timestamp_millis(ts.timestamp_millis())
        .map(|dt| dt.format("%d-%m-%Y %I:%M %p").to_string())
        .unwrap_or_default()
}

use crate::action_store::ActionRecord;
use crate::mongo_store::IntakeStore;

pub const METRIC_TYPE_RULE_PASS: &str = "rule.pass";
pub const METRIC_TYPE_RULE_FAIL: &str = "rule.fail";
pub const METRIC_TYPE_RULE_INCONCLUSIVE: &str = "rule.inconclusive";
pub const METRIC_TYPE_ACTION_TRIGGERED: &str = "action.triggered";
pub const METRIC_TYPE_KPI_THRESHOLD_BREACH: &str = "kpi.threshold_breach";

type DynError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Clone, Copy)]
struct MetricSaveSummary {
    total_no_action_triggered: usize,
    total_no_of_success: usize,
    total_no_of_failure: usize,
}

impl MetricSaveSummary {
    fn from_metrics(metrics: &[MetricRecord]) -> Self {
        Self {
            total_no_action_triggered: metrics
                .iter()
                .filter(|metric| metric.metric_type == METRIC_TYPE_ACTION_TRIGGERED)
                .count(),
            total_no_of_success: metrics
                .iter()
                .filter(|metric| metric.metric_type == METRIC_TYPE_RULE_PASS)
                .count(),
            total_no_of_failure: metrics
                .iter()
                .filter(|metric| metric.metric_type == METRIC_TYPE_RULE_FAIL)
                .count(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetricRecord {
    pub dedupe_key: String,
    pub event_id: String,
    pub tenant_id: String,
    pub metric_type: String,
    pub value: i64,
    pub timestamp: DateTime,
    #[serde(default)]
    pub playbook_id: Option<String>,
    #[serde(default)]
    pub rule_id: Option<String>,
    #[serde(default)]
    pub action_id: Option<String>,
    #[serde(default)]
    pub threshold_id: Option<String>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct MetricWindowQuery {
    pub tenant_id: String,
    pub metric_type: String,
    pub since: DateTime,
    pub playbook_id: Option<String>,
    pub rule_id: Option<String>,
}

#[async_trait]
pub trait MetricStore: Send + Sync {
    async fn upsert_metric(&self, record: &MetricRecord) -> Result<bool, DynError>;

    async fn upsert_metrics_response(
        &self,
        records: &[MetricRecord],
        _response: &serde_json::Value,
    ) -> Result<Vec<bool>, DynError> {
        let mut inserted = Vec::with_capacity(records.len());
        for record in records {
            inserted.push(self.upsert_metric(record).await?);
        }
        Ok(inserted)
    }

    async fn sum_metric_values(&self, query: &MetricWindowQuery) -> Result<i64, DynError>;

    async fn list_metrics(&self) -> Result<Vec<MetricRecord>, DynError>;

    async fn list_metrics_by_event_id(&self, event_id: &str)
    -> Result<Vec<MetricRecord>, DynError>;
}

#[async_trait]
pub trait MetricEventPublisher: Send + Sync {
    async fn publish_event(&self, envelope: &CanonicalEnvelope) -> Result<(), DynError>;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetricThreshold {
    pub threshold_id: String,
    pub metric_type: String,
    pub window_seconds: i64,
    pub max_count: i64,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub playbook_id: Option<String>,
    #[serde(default)]
    pub rule_id: Option<String>,
}

impl MetricThreshold {
    pub fn default_thresholds() -> Vec<Self> {
        vec![Self {
            threshold_id: "rule_failures_3_in_5m".to_string(),
            metric_type: METRIC_TYPE_RULE_FAIL.to_string(),
            window_seconds: 300,
            max_count: 3,
            tenant_id: None,
            playbook_id: None,
            rule_id: None,
        }]
    }

    fn applies_to(&self, record: &MetricRecord) -> bool {
        if self.metric_type != record.metric_type {
            return false;
        }
        if let Some(tenant_id) = &self.tenant_id {
            if tenant_id != &record.tenant_id {
                return false;
            }
        }
        if let Some(playbook_id) = &self.playbook_id {
            if record.playbook_id.as_ref() != Some(playbook_id) {
                return false;
            }
        }
        if let Some(rule_id) = &self.rule_id {
            if record.rule_id.as_ref() != Some(rule_id) {
                return false;
            }
        }
        true
    }
}

#[derive(Clone)]
pub struct MetricManager {
    store: Arc<dyn MetricStore>,
    publisher: Option<Arc<dyn MetricEventPublisher>>,
    thresholds: Vec<MetricThreshold>,
}

impl MetricManager {
    pub fn new(
        store: Arc<dyn MetricStore>,
        publisher: Option<Arc<dyn MetricEventPublisher>>,
        thresholds: Vec<MetricThreshold>,
    ) -> Self {
        Self {
            store,
            publisher,
            thresholds,
        }
    }

    pub fn from_thresholds(
        store: Arc<dyn MetricStore>,
        publisher: Option<Arc<dyn MetricEventPublisher>>,
        thresholds: Vec<MetricThreshold>,
    ) -> Self {
        Self::new(store, publisher, thresholds)
    }

    pub fn thresholds_from_env() -> Result<Vec<MetricThreshold>, DynError> {
        match std::env::var("KPI_THRESHOLDS_JSON") {
            Ok(raw) if !raw.trim().is_empty() => Ok(serde_json::from_str(&raw)?),
            _ => Ok(MetricThreshold::default_thresholds()),
        }
    }

    pub async fn list_metrics(&self) -> Result<Vec<MetricRecord>, DynError> {
        self.store.list_metrics().await
    }

    pub async fn list_metrics_by_event_id(
        &self,
        event_id: &str,
    ) -> Result<Vec<MetricRecord>, DynError> {
        self.store.list_metrics_by_event_id(event_id).await
    }

    pub fn format_metric_response(
        event_id: Option<&str>,
        event_name: Option<&str>,
        tenant_id: Option<&str>,
        records: &[MetricRecord],
    ) -> serde_json::Value {
        let rule_types = [
            METRIC_TYPE_RULE_PASS,
            METRIC_TYPE_RULE_FAIL,
            METRIC_TYPE_RULE_INCONCLUSIVE,
        ];

        let mut playbook_ids: Vec<String> = Vec::new();
        for record in records {
            if let Some(pid) = &record.playbook_id {
                if !playbook_ids.contains(pid) {
                    playbook_ids.push(pid.clone());
                }
            }
        }

        let rule_records: Vec<&MetricRecord> = records
            .iter()
            .filter(|r| rule_types.contains(&r.metric_type.as_str()))
            .collect();
        let action_records: Vec<&MetricRecord> = records
            .iter()
            .filter(|r| r.metric_type == METRIC_TYPE_ACTION_TRIGGERED)
            .collect();
        let total_no_action_triggered = action_records.len();
        let total_no_of_success = rule_records
            .iter()
            .filter(|r| r.metric_type == METRIC_TYPE_RULE_PASS)
            .count();
        let total_no_of_failure = rule_records
            .iter()
            .filter(|r| r.metric_type == METRIC_TYPE_RULE_FAIL)
            .count();

        let has_fail = records
            .iter()
            .any(|r| r.metric_type == METRIC_TYPE_RULE_FAIL);
        let status = if has_fail { "fail" } else { "success" };

        let header_timestamp_ms = records
            .iter()
            .map(|r| r.timestamp.timestamp_millis())
            .min()
            .unwrap_or_else(|| DateTime::now().timestamp_millis());
        let header_timestamp = DateTime::from_millis(header_timestamp_ms);

        let results: Vec<serde_json::Value> = playbook_ids
            .iter()
            .map(|pid| {
                let playbook_name: Option<String> = records
                    .iter()
                    .filter(|r| r.playbook_id.as_deref() == Some(pid.as_str()))
                    .find_map(|r| {
                        metadata_string(
                            r.metadata.as_ref(),
                            &["playbook_name", "playbookName", "name"],
                        )
                    });

                let rules: Vec<serde_json::Value> = rule_records
                    .iter()
                    .filter(|r| r.playbook_id.as_deref() == Some(pid.as_str()))
                    .map(|r| {
                        let rule_name: Option<String> = r.metadata.as_ref().and_then(|metadata| {
                            metadata_string(Some(metadata), &["rule_name", "ruleName", "name"])
                        });
                        let (reason_code, reason) = if r.metric_type == METRIC_TYPE_RULE_PASS {
                            (None::<String>, None::<String>)
                        } else {
                            let rc = r.metadata.as_ref().and_then(|metadata| {
                                metadata_string(Some(metadata), &["reason_code", "reasonCode"])
                            });
                            let rs = r
                                .metadata
                                .as_ref()
                                .and_then(|metadata| metadata_string(Some(metadata), &["reason"]));
                            (rc, rs)
                        };
                        let result = match r.metric_type.as_str() {
                            METRIC_TYPE_RULE_PASS => "PASS",
                            METRIC_TYPE_RULE_FAIL => "FAIL",
                            _ => "INCONCLUSIVE",
                        };
                        serde_json::json!({
                            "rule_id": r.rule_id,
                            "rule_name": rule_name,
                            "metric_type": r.metric_type.to_uppercase().replace('.', "_"),
                            "result": result,
                            "reason_code": reason_code,
                            "reason": reason,
                            "timestamp": format_metric_timestamp(&r.timestamp),
                        })
                    })
                    .collect();

                let actions_triggered: Vec<serde_json::Value> = action_records
                    .iter()
                    .filter(|r| r.playbook_id.as_deref() == Some(pid.as_str()))
                    .map(|r| {
                        let task_type: Option<String> = r.metadata.as_ref().and_then(|metadata| {
                            metadata_string(Some(metadata), &["task_type", "taskType"])
                        });
                        let action_status: Option<String> = r
                            .metadata
                            .as_ref()
                            .and_then(|metadata| metadata_string(Some(metadata), &["status"]));
                        serde_json::json!({
                            "rule_id": r.rule_id,
                            "action_id": r.action_id,
                            "task_type": task_type,
                            "status": action_status
                                .map(|status| status.to_ascii_uppercase())
                                .unwrap_or_else(|| "CREATED".to_string()),
                            "timestamp": format_metric_timestamp(&r.timestamp),
                        })
                    })
                    .collect();

                serde_json::json!({
                    "playbook": {
                        "playbook_id": pid,
                        "playbook_name": playbook_name,
                        "rules": rules,
                        "actions_triggered": actions_triggered,
                    }
                })
            })
            .collect();

        serde_json::json!({
            "header": {
                "event_name": event_name,
                "event_id": event_id,
                "tenant_id": tenant_id,
                "timestamp": format_metric_timestamp(&header_timestamp),
                "total_playbooks_triggered": playbook_ids.len(),
                "total_rules_triggered": rule_records.len(),
                "total_no_action_triggered": total_no_action_triggered,
                "total_no_of_success": total_no_of_success,
                "total_no_of_failure": total_no_of_failure,
                "status": status,
            },
            "results": results,
        })
    }

    pub fn format_all_metrics_response(records: &[MetricRecord]) -> serde_json::Value {
        let event_name = unique_metadata_value(records, "event_name");
        let event_id = unique_record_value(records, |record| record.event_id.as_str());
        let tenant_id = unique_record_value(records, |record| record.tenant_id.as_str());

        Self::format_metric_response(
            event_id.as_deref(),
            event_name.as_deref().or(Some("Metrics")),
            tenant_id.as_deref(),
            records,
        )
    }

    pub async fn record_pipeline_outcome(
        &self,
        envelope: &CanonicalEnvelope,
        result: &OrchestrationResult,
        created_actions: &[ActionRecord],
        event_logger: Option<&Arc<dyn IntakeStore>>,
    ) -> Result<Vec<MetricRecord>, DynError> {
        let mut metrics = self.build_rule_metrics(envelope, result);
        metrics.extend(self.build_action_metrics(envelope, created_actions));
        let summary = MetricSaveSummary::from_metrics(&metrics);
        for metric in &mut metrics {
            append_metric_save_summary(metric, &summary);
        }

        for metric in &metrics {
            if let Some(store) = event_logger {
                let details = serde_json::json!({
                    "metric_type": &metric.metric_type,
                    "playbook_id": &metric.playbook_id,
                    "rule_id": &metric.rule_id,
                    "action_id": &metric.action_id,
                    "value": metric.value,
                });
                let _ = store
                    .append_event_log(
                        envelope,
                        "metric_manager",
                        "INFO",
                        "metric generated for event",
                        Some(&details),
                    )
                    .await;
            }
        }

        let response = Self::format_metric_response(
            Some(&envelope.head.event_id),
            Some(&envelope.head.event_name),
            Some(&envelope.head.tenant_id),
            &metrics,
        );
        let inserted_flags = self
            .store
            .upsert_metrics_response(&metrics, &response)
            .await?;

        for (index, metric) in metrics.iter().enumerate() {
            // only check thresholds for newly inserted records
            if inserted_flags.get(index).copied().unwrap_or(false) {
                if let Some(store) = event_logger {
                    let details = serde_json::json!({
                        "metric_type": &metric.metric_type,
                        "dedupe_key": &metric.dedupe_key,
                    });
                    let _ = store
                        .append_event_log(
                            envelope,
                            "metric_store",
                            "INFO",
                            "metric persisted",
                            Some(&details),
                        )
                        .await;
                }
                let breaches = self.evaluate_thresholds(envelope, metric).await?;
                for breach in breaches {
                    if let Some(ref publisher) = self.publisher {
                        publisher.publish_event(&breach).await?;
                    }
                    if let Some(store) = event_logger {
                        let details = serde_json::json!({
                            "breach_event_id": &breach.head.event_id,
                            "threshold_id": breach.body.get("threshold_id"),
                        });
                        let _ = store
                            .append_event_log(
                                envelope,
                                "metric_manager",
                                "WARN",
                                "kpi threshold breach generated",
                                Some(&details),
                            )
                            .await;
                    }
                }
            } else if let Some(store) = event_logger {
                let details = serde_json::json!({
                    "metric_type": &metric.metric_type,
                    "dedupe_key": &metric.dedupe_key,
                });
                let _ = store
                    .append_event_log(
                        envelope,
                        "metric_store",
                        "INFO",
                        "metric deduplicated and skipped",
                        Some(&details),
                    )
                    .await;
            }
        }

        Ok(metrics)
    }

    fn build_rule_metrics(
        &self,
        envelope: &CanonicalEnvelope,
        result: &OrchestrationResult,
    ) -> Vec<MetricRecord> {
        let timestamp = DateTime::now();
        result
            .evaluations
            .iter()
            .map(|evaluation| {
                let metric_type = match evaluation.decision {
                    Decision::Pass { .. } => METRIC_TYPE_RULE_PASS,
                    Decision::Fail { .. } => METRIC_TYPE_RULE_FAIL,
                    Decision::Inconclusive { .. } => METRIC_TYPE_RULE_INCONCLUSIVE,
                };
                MetricRecord {
                    dedupe_key: format!(
                        "{}:{}:{}:{}:{}",
                        envelope.head.tenant_id,
                        envelope.head.event_id,
                        metric_type,
                        evaluation.playbook_id,
                        evaluation.rule_id
                    ),
                    event_id: envelope.head.event_id.clone(),
                    tenant_id: envelope.head.tenant_id.clone(),
                    metric_type: metric_type.to_string(),
                    value: 1,
                    timestamp: timestamp.clone(),
                    playbook_id: Some(evaluation.playbook_id.clone()),
                    rule_id: Some(evaluation.rule_id.clone()),
                    action_id: None,
                    threshold_id: None,
                    metadata: Some(serde_json::json!({
                        "decision": &evaluation.decision,
                        "reason_code": &evaluation.reason_code,
                        "reason": detailed_metric_reason(evaluation),
                        "event_name": &envelope.head.event_name,
                        "rule_name": &evaluation.rule_name,
                        "playbook_name": result
                            .matched_playbook
                            .as_ref()
                            .filter(|playbook| playbook.id == evaluation.playbook_id)
                            .map(|playbook| playbook.name.clone()),
                    })),
                }
            })
            .collect()
    }

    fn build_action_metrics(
        &self,
        envelope: &CanonicalEnvelope,
        created_actions: &[ActionRecord],
    ) -> Vec<MetricRecord> {
        let timestamp = DateTime::now();
        created_actions
            .iter()
            .map(|action| MetricRecord {
                dedupe_key: format!(
                    "{}:{}:{}:{}",
                    envelope.head.tenant_id,
                    envelope.head.event_id,
                    METRIC_TYPE_ACTION_TRIGGERED,
                    action.action_id
                ),
                event_id: envelope.head.event_id.clone(),
                tenant_id: envelope.head.tenant_id.clone(),
                metric_type: METRIC_TYPE_ACTION_TRIGGERED.to_string(),
                value: 1,
                timestamp: timestamp.clone(),
                playbook_id: Some(action.playbook_id.clone()),
                rule_id: Some(action.rule_id.clone()),
                action_id: Some(action.action_id.clone()),
                threshold_id: None,
                metadata: Some(serde_json::json!({
                    "task_type": &action.task_type,
                    "status": &action.status,
                    "event_name": &envelope.head.event_name,
                })),
            })
            .collect()
    }

    async fn evaluate_thresholds(
        &self,
        envelope: &CanonicalEnvelope,
        record: &MetricRecord,
    ) -> Result<Vec<CanonicalEnvelope>, DynError> {
        let mut events = Vec::new();
        for threshold in self.thresholds.iter().filter(|t| t.applies_to(record)) {
            let since = DateTime::from_millis(
                record.timestamp.timestamp_millis() - (threshold.window_seconds * 1000),
            );
            let query = MetricWindowQuery {
                tenant_id: record.tenant_id.clone(),
                metric_type: record.metric_type.clone(),
                since,
                playbook_id: threshold
                    .playbook_id
                    .clone()
                    .or_else(|| record.playbook_id.clone()),
                rule_id: threshold.rule_id.clone().or_else(|| record.rule_id.clone()),
            };
            let total = self.store.sum_metric_values(&query).await?;
            if total < threshold.max_count {
                continue;
            }

            let breach_metric = MetricRecord {
                dedupe_key: format!(
                    "{}:{}:{}:{}",
                    record.tenant_id,
                    record.event_id,
                    METRIC_TYPE_KPI_THRESHOLD_BREACH,
                    threshold.threshold_id
                ),
                event_id: record.event_id.clone(),
                tenant_id: record.tenant_id.clone(),
                metric_type: METRIC_TYPE_KPI_THRESHOLD_BREACH.to_string(),
                value: total,
                timestamp: DateTime::now(),
                playbook_id: query.playbook_id.clone(),
                rule_id: query.rule_id.clone(),
                action_id: None,
                threshold_id: Some(threshold.threshold_id.clone()),
                metadata: Some(serde_json::json!({
                    "source_metric_type": &record.metric_type,
                    "window_seconds": threshold.window_seconds,
                    "max_count": threshold.max_count,
                    "observed_count": total,
                })),
            };

            if self.store.upsert_metric(&breach_metric).await? {
                events.push(self.build_breach_event(envelope, record, threshold, total));
            }
        }

        Ok(events)
    }

    fn build_breach_event(
        &self,
        envelope: &CanonicalEnvelope,
        record: &MetricRecord,
        threshold: &MetricThreshold,
        observed_count: i64,
    ) -> CanonicalEnvelope {
        let ulid = UlidService::new();
        CanonicalEnvelope {
            head: domain::envelope::TadpoleHead {
                event_id: ulid.generate_string(),
                event_name: "KpiThresholdBreached".to_string(),
                event_category: Some("MetricsKpi".to_string()),
                tenant_id: envelope.head.tenant_id.clone(),
                correlation_id: envelope.head.correlation_id.clone(),
                causation_id: Some(envelope.head.event_id.clone()),
                occurred_at: envelope.head.occurred_at.clone(),
                originating_function: Some("MetricManager".to_string()),
                originating_application: Some("TadpoleEngine".to_string()),
                environment: envelope.head.environment.clone(),
                external_dependency_id: envelope.head.external_dependency_id.clone(),
                changed_object_type: Some("MetricThreshold".to_string()),
                changed_object_id: Some(threshold.threshold_id.clone()),
                change_kind: Some("created".to_string()),
            },
            body: serde_json::json!({
                "threshold_id": threshold.threshold_id,
                "metric_type": &record.metric_type,
                "tenant_id": &record.tenant_id,
                "observed_count": observed_count,
                "max_count": threshold.max_count,
                "window_seconds": threshold.window_seconds,
                "playbook_id": &record.playbook_id,
                "rule_id": &record.rule_id,
                "source_event_id": &record.event_id,
            }),
        }
    }
}

fn detailed_metric_reason(evaluation: &engine_core::dto::evaluation::RuleEvaluation) -> String {
    let non_passing_checks: Vec<String> = evaluation
        .checks
        .iter()
        .filter(|check| check.status != "PASS")
        .map(|check| {
            format!(
                "{} {} expected {}, actual {} ({})",
                check.object_key,
                check.operator,
                format_optional_json_value(check.expected.as_ref()),
                format_optional_json_value(check.actual.as_ref()),
                check.reason,
            )
        })
        .collect();

    if non_passing_checks.is_empty() {
        return evaluation.reason.clone();
    }

    format!(
        "{}. Non-passing checks: {}.",
        evaluation.reason,
        non_passing_checks.join("; ")
    )
}

fn format_optional_json_value(value: Option<&serde_json::Value>) -> String {
    value
        .map(format_json_value)
        .unwrap_or_else(|| "null".to_string())
}

fn format_json_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(inner) => inner.clone(),
        _ => value.to_string(),
    }
}

fn metadata_string(metadata: Option<&serde_json::Value>, keys: &[&str]) -> Option<String> {
    let metadata = metadata?;
    keys.iter().find_map(|key| {
        metadata
            .get(*key)
            .and_then(|value| value.as_str())
            .map(str::to_string)
    })
}

fn append_metric_save_summary(record: &mut MetricRecord, summary: &MetricSaveSummary) {
    if !record
        .metadata
        .as_ref()
        .is_some_and(|metadata| metadata.is_object())
    {
        record.metadata = Some(serde_json::json!({}));
    }

    if let Some(metadata) = record
        .metadata
        .as_mut()
        .and_then(|metadata| metadata.as_object_mut())
    {
        metadata.insert(
            "total_no_action_triggered".to_string(),
            serde_json::json!(summary.total_no_action_triggered),
        );
        metadata.insert(
            "total_no_of_success".to_string(),
            serde_json::json!(summary.total_no_of_success),
        );
        metadata.insert(
            "total_no_of_failure".to_string(),
            serde_json::json!(summary.total_no_of_failure),
        );
    }
}

fn unique_record_value<'a>(
    records: &'a [MetricRecord],
    get: impl Fn(&'a MetricRecord) -> &'a str,
) -> Option<String> {
    let first = records.first().map(&get)?;
    if records.iter().all(|record| get(record) == first) {
        Some(first.to_string())
    } else {
        None
    }
}

fn unique_metadata_value(records: &[MetricRecord], key: &str) -> Option<String> {
    let values: Vec<&str> = records
        .iter()
        .filter_map(|record| {
            record
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get(key))
                .and_then(|value| value.as_str())
        })
        .collect();
    let first = values.first().copied()?;
    if values.iter().all(|value| *value == first) {
        Some(first.to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct MemoryMetricStore {
        records: Mutex<Vec<MetricRecord>>,
    }

    #[async_trait]
    impl MetricStore for MemoryMetricStore {
        async fn upsert_metric(&self, record: &MetricRecord) -> Result<bool, DynError> {
            let mut records = self.records.lock().expect("records lock");
            if records
                .iter()
                .any(|existing| existing.dedupe_key == record.dedupe_key)
            {
                return Ok(false);
            }
            records.push(record.clone());
            Ok(true)
        }

        async fn sum_metric_values(&self, query: &MetricWindowQuery) -> Result<i64, DynError> {
            let records = self.records.lock().expect("records lock");
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
            let mut records = self.records.lock().expect("records lock").clone();
            records.sort_by(|left, right| right.timestamp.cmp(&left.timestamp));
            Ok(records)
        }

        async fn list_metrics_by_event_id(
            &self,
            event_id: &str,
        ) -> Result<Vec<MetricRecord>, DynError> {
            let mut records: Vec<_> = self
                .records
                .lock()
                .expect("records lock")
                .iter()
                .filter(|record| record.event_id == event_id)
                .cloned()
                .collect();
            records.sort_by(|left, right| right.timestamp.cmp(&left.timestamp));
            Ok(records)
        }
    }

    #[derive(Default)]
    struct MemoryPublisher {
        events: Mutex<Vec<CanonicalEnvelope>>,
    }

    #[async_trait]
    impl MetricEventPublisher for MemoryPublisher {
        async fn publish_event(&self, envelope: &CanonicalEnvelope) -> Result<(), DynError> {
            self.events
                .lock()
                .expect("events lock")
                .push(envelope.clone());
            Ok(())
        }
    }

    fn sample_envelope(event_id: &str, tenant_id: &str) -> CanonicalEnvelope {
        CanonicalEnvelope {
            head: domain::envelope::TadpoleHead {
                event_id: event_id.to_string(),
                event_name: "ContractCreated".to_string(),
                event_category: Some("Transaction".to_string()),
                tenant_id: tenant_id.to_string(),
                correlation_id: None,
                causation_id: None,
                occurred_at: None,
                originating_function: None,
                originating_application: None,
                environment: None,
                external_dependency_id: Some("dep_123".to_string()),
                changed_object_type: Some("Contract".to_string()),
                changed_object_id: Some("ctr_123".to_string()),
                change_kind: Some("created".to_string()),
            },
            body: serde_json::json!({ "snapshots": { "contract": { "amount": 42 } } }),
        }
    }

    fn sample_result(decision: Decision) -> OrchestrationResult {
        OrchestrationResult {
            decision: decision.clone(),
            action_candidates: Vec::new(),
            playbooks: Vec::new(),
            matched_playbook: None,
            evaluations: vec![engine_core::dto::evaluation::RuleEvaluation {
                playbook_id: "playbook.contract".to_string(),
                rule_id: "rule.contract".to_string(),
                rule_name: Some("Contract Rule".to_string()),
                object_type: Some("contract".to_string()),
                decision,
                reason_code: "test.reason".to_string(),
                reason: "test".to_string(),
                duration_ms: 1,
                order_seq: Some(1),
                priority: Some("HIGH".to_string()),
                is_critical: true,
                checks: Vec::new(),
                action_template_id: None,
            }],
            codex: engine_core::dto::orchestration::CodexResult {
                version_id: None,
                playbooks: Vec::new(),
                decision: Decision::inconclusive("test", "test"),
            },
            playbook_summaries: Vec::new(),
            route_to_action_builder: false,
        }
    }

    #[test]
    fn metric_threshold_defaults_and_filters_are_applied() {
        let defaults = MetricThreshold::default_thresholds();
        assert_eq!(defaults.len(), 1);
        assert_eq!(defaults[0].threshold_id, "rule_failures_3_in_5m");
        assert_eq!(defaults[0].metric_type, METRIC_TYPE_RULE_FAIL);
        assert_eq!(defaults[0].window_seconds, 300);
        assert_eq!(defaults[0].max_count, 3);

        let threshold = MetricThreshold {
            threshold_id: "contract_failures".to_string(),
            metric_type: METRIC_TYPE_RULE_FAIL.to_string(),
            window_seconds: 60,
            max_count: 1,
            tenant_id: Some("tenant_a".to_string()),
            playbook_id: Some("playbook.contract".to_string()),
            rule_id: Some("rule.contract".to_string()),
        };
        let mut record = MetricRecord {
            dedupe_key: "dedupe".to_string(),
            event_id: "evt_1".to_string(),
            tenant_id: "tenant_a".to_string(),
            metric_type: METRIC_TYPE_RULE_FAIL.to_string(),
            value: 1,
            timestamp: DateTime::now(),
            playbook_id: Some("playbook.contract".to_string()),
            rule_id: Some("rule.contract".to_string()),
            action_id: None,
            threshold_id: None,
            metadata: None,
        };

        assert!(threshold.applies_to(&record));

        record.metric_type = METRIC_TYPE_RULE_PASS.to_string();
        assert!(!threshold.applies_to(&record));
        record.metric_type = METRIC_TYPE_RULE_FAIL.to_string();
        record.tenant_id = "tenant_b".to_string();
        assert!(!threshold.applies_to(&record));
        record.tenant_id = "tenant_a".to_string();
        record.playbook_id = Some("other_playbook".to_string());
        assert!(!threshold.applies_to(&record));
        record.playbook_id = Some("playbook.contract".to_string());
        record.rule_id = Some("other_rule".to_string());
        assert!(!threshold.applies_to(&record));
    }

    #[tokio::test]
    async fn record_pipeline_outcome_deduplicates_metric_writes() {
        let store = Arc::new(MemoryMetricStore::default());
        let manager = MetricManager::from_thresholds(store.clone(), None, Vec::new());
        let envelope = sample_envelope("evt_1", "tenant_a");
        let result = sample_result(Decision::Pass {
            reason_code: "ok".to_string(),
            message: "ok".to_string(),
        });

        manager
            .record_pipeline_outcome(&envelope, &result, &[], None)
            .await
            .expect("first record");
        manager
            .record_pipeline_outcome(&envelope, &result, &[], None)
            .await
            .expect("second record");

        let records = store.records.lock().expect("records lock");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].metric_type, METRIC_TYPE_RULE_PASS);
    }

    #[tokio::test]
    async fn threshold_detection_is_isolated_per_tenant() {
        let store = Arc::new(MemoryMetricStore::default());
        let publisher = Arc::new(MemoryPublisher::default());
        let manager = MetricManager::from_thresholds(
            store.clone(),
            Some(publisher.clone()),
            vec![MetricThreshold {
                threshold_id: "fails".to_string(),
                metric_type: METRIC_TYPE_RULE_FAIL.to_string(),
                window_seconds: 300,
                max_count: 2,
                tenant_id: None,
                playbook_id: None,
                rule_id: Some("rule.contract".to_string()),
            }],
        );

        manager
            .record_pipeline_outcome(
                &sample_envelope("evt_1", "tenant_a"),
                &sample_result(Decision::fail("fail", "fail")),
                &[],
                None,
            )
            .await
            .expect("record a1");
        manager
            .record_pipeline_outcome(
                &sample_envelope("evt_2", "tenant_b"),
                &sample_result(Decision::fail("fail", "fail")),
                &[],
                None,
            )
            .await
            .expect("record b1");
        manager
            .record_pipeline_outcome(
                &sample_envelope("evt_3", "tenant_a"),
                &sample_result(Decision::fail("fail", "fail")),
                &[],
                None,
            )
            .await
            .expect("record a2");

        let events = publisher.events.lock().expect("events lock");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].head.tenant_id, "tenant_a");
        assert_eq!(events[0].body["threshold_id"], "fails");
        assert_eq!(events[0].body["observed_count"], 2);
    }

    #[tokio::test]
    async fn action_metrics_are_recorded() {
        let store = Arc::new(MemoryMetricStore::default());
        let manager = MetricManager::from_thresholds(store.clone(), None, Vec::new());
        let envelope = sample_envelope("evt_1", "tenant_a");
        let result = sample_result(Decision::fail("fail", "fail"));
        let action = ActionRecord {
            action_id: "act_1".to_string(),
            idempotency_key: "idk".to_string(),
            idempotency_hash: "idh".to_string(),
            tenant_id: "tenant_a".to_string(),
            event_id: "evt_1".to_string(),
            event_name: "ContractCreated".to_string(),
            playbook_id: "playbook.contract".to_string(),
            rule_id: "rule.contract".to_string(),
            task_type: "TASK".to_string(),
            status: "created".to_string(),
            changed_object_type: Some("Contract".to_string()),
            changed_object_id: Some("ctr_123".to_string()),
            created_at: DateTime::now(),
            action_template_id: None,
            execution_mode: None,
            responsible_user: None,
            responsible_role: None,
            escalation_duration: None,
            escalation_duration_unit: None,
            require_document_upload: None,
            require_comment: None,
            require_approval_reference: None,
            action_title: None,
            action_description: None,
            assigned_to_type: None,
            assigned_to_id: None,
            assigned_to_name: None,
        };

        manager
            .record_pipeline_outcome(&envelope, &result, &[action], None)
            .await
            .expect("record action metric");

        let records = store.records.lock().expect("records lock");
        assert!(
            records
                .iter()
                .any(|record| record.metric_type == METRIC_TYPE_ACTION_TRIGGERED)
        );
        assert!(records.iter().all(|record| {
            let metadata = record.metadata.as_ref().expect("metric metadata");
            metadata["total_no_action_triggered"] == 1
                && metadata["total_no_of_success"] == 0
                && metadata["total_no_of_failure"] == 1
        }));
    }

    #[tokio::test]
    async fn list_metrics_by_event_id_returns_only_matching_records() {
        let store = Arc::new(MemoryMetricStore::default());
        let manager = MetricManager::from_thresholds(store.clone(), None, Vec::new());

        manager
            .record_pipeline_outcome(
                &sample_envelope("evt_1", "tenant_a"),
                &sample_result(Decision::Pass {
                    reason_code: "ok".to_string(),
                    message: "ok".to_string(),
                }),
                &[],
                None,
            )
            .await
            .expect("record evt_1");
        manager
            .record_pipeline_outcome(
                &sample_envelope("evt_2", "tenant_a"),
                &sample_result(Decision::fail("fail", "fail")),
                &[],
                None,
            )
            .await
            .expect("record evt_2");

        let metrics = manager
            .list_metrics_by_event_id("evt_1")
            .await
            .expect("list metrics");

        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].event_id, "evt_1");
        assert_eq!(metrics[0].metric_type, METRIC_TYPE_RULE_PASS);
    }
}
