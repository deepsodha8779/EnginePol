//! RabbitMQ consumer: consume events from a queue and run the intake pipeline.
//! Publishes a response envelope for every message (success or error) to
//! RABBITMQ_RESPONSE_QUEUE, mirroring what the HTTP intake API returns.

use actix::Addr;
use domain::envelope::CanonicalEnvelope;
use engine_core::actors::{
    assigner::AssignerActor, diagnostics::DiagnosticsActor, dispatcher::DispatcherActor,
    orchestrator::OrchestratorActor,
};
use futures::StreamExt;
use lapin::{BasicProperties, Connection, ConnectionProperties, options::*, types::FieldTable};
use log::{error, info, warn};
use std::sync::Arc;
use url::Url;

use crate::action_builder::ActionBuilder;
use crate::action_feed_publisher::ActionFeedPublisher;
use crate::metric_manager::MetricManager;
use crate::mongo_store::IntakeStore;
use crate::pipeline::process_envelope;

fn validate_rabbitmq_url(url: &str) -> Result<(), String> {
    let trimmed = url.trim();
    let parsed = Url::parse(trimmed).map_err(|err| format!("invalid RabbitMQ URL: {err}"))?;

    match parsed.scheme() {
        "amqp" | "amqps" => {}
        scheme => {
            return Err(format!(
                "invalid RabbitMQ URL scheme '{scheme}'; expected amqp or amqps"
            ));
        }
    }

    let authority = trimmed
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(trimmed)
        .split(&['/', '?', '#'][..])
        .next()
        .unwrap_or("");
    if authority.matches('@').count() > 1 {
        return Err(
            "RabbitMQ URL contains an unescaped '@' in username or password; percent-encode it as '%40'"
                .to_string(),
        );
    }

    if parsed.host_str().is_none() {
        return Err("RabbitMQ URL is missing a host".to_string());
    }

    Ok(())
}

/// Start the RabbitMQ consumer in a background task.
/// Consumes messages from the configured queue and runs the intake pipeline.
/// If RABBITMQ_URL is not set, returns immediately without starting.
pub fn run_consumer(
    assigner: Addr<AssignerActor>,
    dispatcher: Addr<DispatcherActor>,
    orchestrator: Addr<OrchestratorActor>,
    diagnostics: Addr<DiagnosticsActor>,
    store: Arc<dyn IntakeStore>,
    action_builder: Option<Arc<ActionBuilder>>,
    metric_manager: Option<Arc<MetricManager>>,
    action_feed_publisher: Option<Arc<dyn ActionFeedPublisher>>,
) {
    let url = match std::env::var("RABBITMQ_URL") {
        Ok(u) if !u.trim().is_empty() => u,
        _ => {
            info!("RABBITMQ_URL not set; RabbitMQ consumer disabled");
            return;
        }
    };
    let queue_name =
        std::env::var("RABBITMQ_QUEUE").unwrap_or_else(|_| "events_intake".to_string());
    let response_queue = std::env::var("RABBITMQ_RESPONSE_QUEUE")
        .unwrap_or_else(|_| "events_intake_response".to_string());

    if let Err(err) = validate_rabbitmq_url(&url) {
        warn!("invalid RABBITMQ_URL; RabbitMQ consumer disabled: {}", err);
        return;
    }

    actix::spawn(async move {
        run_with_retry(
            url,
            queue_name,
            response_queue,
            assigner,
            dispatcher,
            orchestrator,
            diagnostics,
            store,
            action_builder,
            metric_manager,
            action_feed_publisher,
        )
        .await;
    });
}

const RECONNECT_DELAY_SECS: u64 = 10;

async fn run_with_retry(
    url: String,
    queue_name: String,
    response_queue: String,
    assigner: Addr<AssignerActor>,
    dispatcher: Addr<DispatcherActor>,
    orchestrator: Addr<OrchestratorActor>,
    diagnostics: Addr<DiagnosticsActor>,
    store: Arc<dyn IntakeStore>,
    action_builder: Option<Arc<ActionBuilder>>,
    metric_manager: Option<Arc<MetricManager>>,
    action_feed_publisher: Option<Arc<dyn ActionFeedPublisher>>,
) {
    loop {
        match consume_loop(
            &url,
            &queue_name,
            &response_queue,
            assigner.clone(),
            dispatcher.clone(),
            orchestrator.clone(),
            diagnostics.clone(),
            store.clone(),
            action_builder.clone(),
            metric_manager.clone(),
            action_feed_publisher.clone(),
        )
        .await
        {
            Ok(()) => {
                warn!(
                    "RabbitMQ consumer loop ended unexpectedly; reconnecting in {}s",
                    RECONNECT_DELAY_SECS
                );
            }
            Err(e) => {
                error!(
                    "RabbitMQ consumer error: {}; reconnecting in {}s",
                    e, RECONNECT_DELAY_SECS
                );
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(RECONNECT_DELAY_SECS)).await;
    }
}

async fn consume_loop(
    url: &str,
    queue_name: &str,
    response_queue: &str,
    assigner: Addr<AssignerActor>,
    dispatcher: Addr<DispatcherActor>,
    orchestrator: Addr<OrchestratorActor>,
    diagnostics: Addr<DiagnosticsActor>,
    store: Arc<dyn IntakeStore>,
    action_builder: Option<Arc<ActionBuilder>>,
    metric_manager: Option<Arc<MetricManager>>,
    action_feed_publisher: Option<Arc<dyn ActionFeedPublisher>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("RabbitMQ consumer connecting to queue: {}", queue_name);

    let conn = Connection::connect(url, ConnectionProperties::default())
        .await
        .map_err(|e| format!("RabbitMQ connection failed: {}", e))?;

    info!("RabbitMQ connected");

    let channel = conn.create_channel().await?;
    let publish_channel = conn.create_channel().await?;

    channel
        .queue_declare(
            &queue_name,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    publish_channel
        .queue_declare(
            &response_queue,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    let mut consumer = channel
        .basic_consume(
            &queue_name,
            "tadpole-engine",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    info!(
        "RabbitMQ consumer started; waiting for messages (responses → {})",
        response_queue
    );

    while let Some(delivery_result) = consumer.next().await {
        let delivery = match delivery_result {
            Ok(d) => d,
            Err(e) => {
                warn!("consumer delivery error: {}", e);
                continue;
            }
        };

        let raw = String::from_utf8_lossy(&delivery.data);
        info!(
            "RabbitMQ raw message ({} bytes): {}",
            delivery.data.len(),
            &raw[..raw.len().min(500)]
        );

        let envelope = match parse_envelope(&delivery.data) {
            Ok(e) => e,
            Err(e) => {
                warn!("invalid envelope JSON: {}; nacking message", e);
                publish_response(
                    &publish_channel,
                    response_queue,
                    &serde_json::json!({
                        "status": "error",
                        "errors": [format!("invalid JSON: {}", e)],
                    }),
                )
                .await;
                let _ = delivery
                    .nack(BasicNackOptions {
                        multiple: false,
                        requeue: false,
                    })
                    .await;
                continue;
            }
        };

        info!(
            "RabbitMQ processing: event_id={} tenant_id={} event_name={}",
            envelope.head.event_id, envelope.head.tenant_id, envelope.head.event_name
        );
        let details = serde_json::json!({
            "transport": "rabbitmq",
            "queue": queue_name,
            "response_queue": response_queue,
            "delivery_size_bytes": delivery.data.len(),
        });
        let _ = store
            .append_event_log(
                &envelope,
                "rabbitmq_consumer",
                "INFO",
                "rabbitmq message received",
                Some(&details),
            )
            .await;

        let pipeline_envelope = envelope.clone();
        let pipeline_result = process_envelope(
            pipeline_envelope,
            assigner.clone(),
            dispatcher.clone(),
            orchestrator.clone(),
            diagnostics.clone(),
            Some(store.clone()),
            action_builder.clone(),
            metric_manager.clone(),
            action_feed_publisher.clone(),
        )
        .await;

        match &pipeline_result {
            Ok(governance_events) => {
                publish_response(
                    &publish_channel,
                    response_queue,
                    &serde_json::json!(governance_events),
                )
                .await;
            }
            Err(err) => {
                publish_response(&publish_channel, response_queue, &err.response_body()).await;
            }
        }

        match pipeline_result {
            Ok(_) => {
                let _ = store
                    .append_event_log(
                        &envelope,
                        "rabbitmq_consumer",
                        "INFO",
                        "rabbitmq message acknowledged",
                        None,
                    )
                    .await;
                if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                    error!("failed to ack message: {}", e);
                }
            }
            Err(e) => {
                warn!(
                    "pipeline error: code={} stage={} description={}; nacking message (no requeue)",
                    e.code(),
                    e.stage(),
                    e.description()
                );
                let details = serde_json::json!({
                    "error_code": e.code(),
                    "stage": e.stage(),
                    "error": e.primary_message(),
                    "description": e.description(),
                    "response": e.response_body(),
                });
                let _ = store
                    .append_event_log(
                        &envelope,
                        "rabbitmq_consumer",
                        "WARN",
                        "rabbitmq message nacked after pipeline failure",
                        Some(&details),
                    )
                    .await;
                if let Err(na) = delivery
                    .nack(BasicNackOptions {
                        multiple: false,
                        requeue: false,
                    })
                    .await
                {
                    error!("failed to nack message: {}", na);
                }
            }
        }
    }

    Ok(())
}

async fn publish_response(channel: &lapin::Channel, queue: &str, body: &serde_json::Value) {
    match serde_json::to_vec(body) {
        Ok(payload) => {
            if let Err(e) = channel
                .basic_publish(
                    "",
                    queue,
                    BasicPublishOptions::default(),
                    &payload,
                    BasicProperties::default().with_content_type("application/json".into()),
                )
                .await
            {
                error!("failed to publish response to {}: {}", queue, e);
            }
        }
        Err(e) => {
            error!("failed to serialize response: {}", e);
        }
    }
}

fn parse_envelope(data: &[u8]) -> Result<CanonicalEnvelope, String> {
    parse_envelope_from_bytes(data)
}

/// Parses a canonical event envelope from JSON bytes (e.g. RabbitMQ message body).
/// Public for use in tests.
pub fn parse_envelope_from_bytes(data: &[u8]) -> Result<CanonicalEnvelope, String> {
    let envelope: CanonicalEnvelope =
        serde_json::from_slice(data).map_err(|e| format!("JSON parse error: {}", e))?;
    Ok(envelope)
}
