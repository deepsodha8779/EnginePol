use actix_web::{HttpResponse, Responder, web};
use domain::envelope::CanonicalEnvelope;
use engine_core::actors::diagnostics::DiagnosticsActor;
use log::{debug, error, info};
use std::collections::HashMap;
use std::sync::Arc;

use crate::action_builder::ActionBuilder;
use crate::action_feed_publisher::ActionFeedPublisher;
use crate::action_template::{
    ActionTemplateListFilter, ActionTemplateStore, TemplateStatus, TriggerEventType,
};
use crate::metric_manager::{MetricManager, MetricRecord};
use crate::mongo_store::IntakeStore;
use crate::pipeline::{PipelineError, process_envelope};

#[derive(Debug, serde::Deserialize)]
pub struct ActionTemplateQuery {
    tenant_id: Option<String>,
    status: Option<TemplateStatus>,
    object_type: Option<String>,
    event_type: Option<TriggerEventType>,
    limit: Option<i64>,
}

fn gateway_error_response(error_code: &str, stage: &str, error: String) -> serde_json::Value {
    serde_json::json!({
        "status": "error",
        "error_code": error_code,
        "stage": stage,
        "error": error,
        "description": format!("{stage} failed: {error}"),
    })
}

async fn enrich_metric_names(
    records: &mut [MetricRecord],
    store: &Arc<dyn IntakeStore>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let playbook_ids = unique_metric_ids(records.iter().filter_map(|record| {
        record
            .playbook_id
            .as_ref()
            .filter(|_| !metadata_has_string(record.metadata.as_ref(), "playbook_name"))
    }));
    let rule_ids = unique_metric_ids(records.iter().filter_map(|record| {
        record
            .rule_id
            .as_ref()
            .filter(|_| !metadata_has_string(record.metadata.as_ref(), "rule_name"))
    }));

    let playbook_names: HashMap<String, String> = store
        .find_playbooks_by_ids(&playbook_ids)
        .await?
        .into_iter()
        .filter_map(|playbook| playbook.name.map(|name| (playbook.id, name)))
        .collect();
    let rule_names: HashMap<String, String> = store
        .find_rules_by_ids(&rule_ids)
        .await?
        .into_iter()
        .filter_map(|rule| rule.name.map(|name| (rule.id, name)))
        .collect();

    for record in records {
        if let Some(playbook_id) = &record.playbook_id
            && let Some(playbook_name) = playbook_names.get(playbook_id)
        {
            insert_metric_metadata(record, "playbook_name", playbook_name);
        }
        if let Some(rule_id) = &record.rule_id
            && let Some(rule_name) = rule_names.get(rule_id)
        {
            insert_metric_metadata(record, "rule_name", rule_name);
        }
    }

    Ok(())
}

fn unique_metric_ids<'a>(ids: impl Iterator<Item = &'a String>) -> Vec<String> {
    ids.fold(Vec::new(), |mut unique, id| {
        if !unique.contains(id) {
            unique.push(id.clone());
        }
        unique
    })
}

fn metadata_has_string(metadata: Option<&serde_json::Value>, key: &str) -> bool {
    metadata
        .and_then(|metadata| metadata.get(key))
        .and_then(|value| value.as_str())
        .is_some_and(|value| !value.trim().is_empty())
}

fn insert_metric_metadata(record: &mut MetricRecord, key: &str, value: &str) {
    if metadata_has_string(record.metadata.as_ref(), key) {
        return;
    }
    if !record
        .metadata
        .as_ref()
        .is_some_and(|value| value.is_object())
    {
        record.metadata = Some(serde_json::json!({}));
    }
    if let Some(metadata) = record
        .metadata
        .as_mut()
        .and_then(|value| value.as_object_mut())
    {
        metadata.insert(
            key.to_string(),
            serde_json::Value::String(value.to_string()),
        );
    }
}

pub async fn intake(
    payload: web::Json<CanonicalEnvelope>,
    assigner: web::Data<actix::Addr<engine_core::actors::assigner::AssignerActor>>,
    dispatcher: web::Data<actix::Addr<engine_core::actors::dispatcher::DispatcherActor>>,
    orchestrator: web::Data<actix::Addr<engine_core::actors::orchestrator::OrchestratorActor>>,
    diagnostics: web::Data<actix::Addr<DiagnosticsActor>>,
    store: web::Data<Arc<dyn IntakeStore>>,
    action_builder: web::Data<Option<Arc<ActionBuilder>>>,
    metric_manager: web::Data<Option<Arc<MetricManager>>>,
    action_feed_publisher: web::Data<Option<Arc<dyn ActionFeedPublisher>>>,
) -> impl Responder {
    let envelope = payload.into_inner();
    info!(
        "intake request received: event_id={} tenant_id={} event_name={}",
        envelope.head.event_id, envelope.head.tenant_id, envelope.head.event_name
    );
    let details = serde_json::json!({
        "transport": "http",
        "path": "/intake",
    });
    let _ = store
        .get_ref()
        .append_event_log(
            &envelope,
            "intake_api",
            "INFO",
            "http intake request received",
            Some(&details),
        )
        .await;

    match process_envelope(
        envelope.clone(),
        assigner.get_ref().clone(),
        dispatcher.get_ref().clone(),
        orchestrator.get_ref().clone(),
        diagnostics.get_ref().clone(),
        Some(store.get_ref().clone()),
        action_builder.get_ref().clone(),
        metric_manager.get_ref().clone(),
        action_feed_publisher.get_ref().clone(),
    )
    .await
    {
        Ok(governance_events) => HttpResponse::Ok().json(governance_events),
        Err(err @ PipelineError::ValidationFailed(_)) => {
            let details = serde_json::json!({
                "error_code": err.code(),
                "stage": err.stage(),
                "description": err.description(),
                "response": err.response_body(),
            });
            let _ = store
                .get_ref()
                .append_event_log(
                    &envelope,
                    "intake_api",
                    "WARN",
                    "http intake request rejected with validation errors",
                    Some(&details),
                )
                .await;
            HttpResponse::BadRequest().json(err.response_body())
        }
        Err(err @ PipelineError::AssignerFailed(_))
        | Err(err @ PipelineError::DispatchFailed(_))
        | Err(err @ PipelineError::OrchestratorFailed(_)) => {
            error!(
                "intake pipeline error: code={} stage={} description={}",
                err.code(),
                err.stage(),
                err.description()
            );
            let details = serde_json::json!({
                "error_code": err.code(),
                "stage": err.stage(),
                "error": err.primary_message(),
                "description": err.description(),
                "response": err.response_body(),
            });
            let _ = store
                .get_ref()
                .append_event_log(
                    &envelope,
                    "intake_api",
                    "ERROR",
                    "http intake request failed",
                    Some(&details),
                )
                .await;
            HttpResponse::InternalServerError().json(err.response_body())
        }
    }
}

pub async fn diagnostics(store: web::Data<Arc<dyn IntakeStore>>) -> impl Responder {
    debug!("diagnostics request received");
    match store.get_ref().list_recent_intake(50).await {
        Ok(records) => {
            debug!("diagnostics data loaded from mongodb");
            HttpResponse::Ok().json(serde_json::json!({
                "records": records,
            }))
        }
        Err(err) => {
            error!("diagnostics request failed: {err}");
            HttpResponse::InternalServerError().json(gateway_error_response(
                "diagnostics_query_failed",
                "diagnostics.query",
                format!("diagnostics database error: {err}"),
            ))
        }
    }
}

pub async fn event_logs_by_event_id(
    event_id: web::Path<String>,
    store: web::Data<Arc<dyn IntakeStore>>,
) -> impl Responder {
    debug!("event logs by event_id request received: {}", event_id);
    match store.get_ref().list_event_logs_by_event_id(&event_id).await {
        Ok(records) => HttpResponse::Ok().json(serde_json::json!({
            "event_id": event_id.into_inner(),
            "records": records,
        })),
        Err(err) => {
            error!("event logs by event_id request failed: {err}");
            HttpResponse::InternalServerError().json(gateway_error_response(
                "event_logs_query_failed",
                "event_logs.query",
                format!("event logs database error: {err}"),
            ))
        }
    }
}

pub async fn metrics(
    metric_manager: web::Data<Option<Arc<MetricManager>>>,
    store: web::Data<Arc<dyn IntakeStore>>,
) -> impl Responder {
    debug!("metrics request received");
    let Some(metric_manager) = metric_manager.get_ref().clone() else {
        return HttpResponse::ServiceUnavailable().json(gateway_error_response(
            "metrics_service_unavailable",
            "metrics.service",
            "metrics service is not available".to_string(),
        ));
    };

    match metric_manager.list_metrics().await {
        Ok(mut records) => match enrich_metric_names(&mut records, store.get_ref()).await {
            Ok(()) => HttpResponse::Ok().json(MetricManager::format_all_metrics_response(&records)),
            Err(err) => {
                error!("metrics enrichment failed: {err}");
                HttpResponse::InternalServerError().json(gateway_error_response(
                    "metrics_query_failed",
                    "metrics.query",
                    format!("metrics enrichment error: {err}"),
                ))
            }
        },
        Err(err) => {
            error!("metrics request failed: {err}");
            HttpResponse::InternalServerError().json(gateway_error_response(
                "metrics_query_failed",
                "metrics.query",
                format!("metrics database error: {err}"),
            ))
        }
    }
}

pub async fn metrics_by_event_id(
    event_id: web::Path<String>,
    metric_manager: web::Data<Option<Arc<MetricManager>>>,
    store: web::Data<Arc<dyn IntakeStore>>,
) -> impl Responder {
    debug!("metrics by event_id request received: {}", event_id);
    let Some(metric_manager) = metric_manager.get_ref().clone() else {
        return HttpResponse::ServiceUnavailable().json(gateway_error_response(
            "metrics_service_unavailable",
            "metrics.service",
            "metrics service is not available".to_string(),
        ));
    };

    match metric_manager.list_metrics_by_event_id(&event_id).await {
        Ok(mut records) => {
            let event_id = event_id.into_inner();
            if let Err(err) = enrich_metric_names(&mut records, store.get_ref()).await {
                error!("metrics by event_id enrichment failed: {err}");
                return HttpResponse::InternalServerError().json(gateway_error_response(
                    "metrics_query_failed",
                    "metrics.query",
                    format!("metrics enrichment error: {err}"),
                ));
            }
            let event_name = records
                .iter()
                .find_map(|r| {
                    r.metadata
                        .as_ref()
                        .and_then(|m| m.get("event_name"))
                        .and_then(|v| v.as_str())
                        .map(str::to_string)
                })
                .unwrap_or_default();
            let tenant_id = records
                .first()
                .map(|r| r.tenant_id.clone())
                .unwrap_or_default();
            HttpResponse::Ok().json(MetricManager::format_metric_response(
                Some(&event_id),
                Some(&event_name),
                Some(&tenant_id),
                &records,
            ))
        }
        Err(err) => {
            error!("metrics by event_id request failed: {err}");
            HttpResponse::InternalServerError().json(gateway_error_response(
                "metrics_query_failed",
                "metrics.query",
                format!("metrics database error: {err}"),
            ))
        }
    }
}

pub async fn action_templates(
    query: web::Query<ActionTemplateQuery>,
    template_store: web::Data<Option<Arc<dyn ActionTemplateStore>>>,
) -> impl Responder {
    debug!("action templates request received");
    let Some(template_store) = template_store.get_ref().clone() else {
        return HttpResponse::ServiceUnavailable().json(gateway_error_response(
            "action_template_service_unavailable",
            "action_templates.service",
            "action template service is not available".to_string(),
        ));
    };

    let query = query.into_inner();
    let filter = ActionTemplateListFilter {
        tenant_id: query.tenant_id,
        status: query.status,
        object_type: query.object_type,
        event_type: query.event_type,
    };
    let limit = query.limit.unwrap_or(50);

    match template_store.list_templates(filter, limit).await {
        Ok(records) => HttpResponse::Ok().json(serde_json::json!({
            "records": records,
        })),
        Err(err) => {
            error!("action templates request failed: {err}");
            HttpResponse::InternalServerError().json(gateway_error_response(
                "action_templates_query_failed",
                "action_templates.query",
                format!("action templates database error: {err}"),
            ))
        }
    }
}

pub async fn action_template_by_id(
    template_id: web::Path<String>,
    template_store: web::Data<Option<Arc<dyn ActionTemplateStore>>>,
) -> impl Responder {
    let template_id = template_id.into_inner();
    debug!("action template by id request received: {}", template_id);
    let Some(template_store) = template_store.get_ref().clone() else {
        return HttpResponse::ServiceUnavailable().json(gateway_error_response(
            "action_template_service_unavailable",
            "action_templates.service",
            "action template service is not available".to_string(),
        ));
    };

    match template_store.find_template_by_id(&template_id).await {
        Ok(Some(record)) => HttpResponse::Ok().json(record),
        Ok(None) => HttpResponse::NotFound().json(gateway_error_response(
            "action_template_not_found",
            "action_templates.lookup",
            format!("action template not found: {template_id}"),
        )),
        Err(err) => {
            error!("action template by id request failed: {err}");
            HttpResponse::InternalServerError().json(gateway_error_response(
                "action_template_lookup_failed",
                "action_templates.lookup",
                format!("action template database error: {err}"),
            ))
        }
    }
}
