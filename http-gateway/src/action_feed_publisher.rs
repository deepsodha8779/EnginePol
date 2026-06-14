//! Publishes created actions to a RabbitMQ queue so downstream consumers
//! (e.g. OutSystems) can read the actions that need to be undertaken.

use async_trait::async_trait;
use lapin::{
    BasicProperties, Connection, ConnectionProperties,
    options::{BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
};
use log::{info, warn};
use serde::Serialize;
use url::Url;

use crate::action_store::ActionRecord;

type DynError = Box<dyn std::error::Error + Send + Sync>;

/// Payload shape published to the actions queue.
/// Always sent as a JSON array (`Vec<ActionFeedPayload>`).
#[derive(Debug, Clone, Serialize)]
pub struct ActionFeedPayload {
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
    pub created_at: String,

    pub action_template_id: Option<String>,
    pub execution_mode: Option<String>,
    pub responsible_user: Option<String>,
    pub responsible_role: Option<String>,
    pub escalation_duration: Option<u32>,
    pub escalation_duration_unit: Option<String>,
    pub require_document_upload: Option<bool>,
    pub require_comment: Option<bool>,
    pub require_approval_reference: Option<bool>,

    pub action_title: Option<String>,
    pub action_description: Option<String>,

    pub assigned_to_type: Option<String>,
    pub assigned_to_id: Option<String>,
    pub assigned_to_name: Option<String>,
}

impl ActionFeedPayload {
    pub fn from_record(record: &ActionRecord) -> Self {
        // Convert mongodb::bson::DateTime to ISO 8601 string.
        let created_at: chrono::DateTime<chrono::Utc> = record.created_at.to_system_time().into();
        let created_at = created_at.format("%Y-%m-%dT%H:%M:%SZ").to_string();

        Self {
            action_id: record.action_id.clone(),
            idempotency_key: record.idempotency_key.clone(),
            idempotency_hash: record.idempotency_hash.clone(),
            tenant_id: record.tenant_id.clone(),
            event_id: record.event_id.clone(),
            event_name: record.event_name.clone(),
            playbook_id: record.playbook_id.clone(),
            rule_id: record.rule_id.clone(),
            task_type: record.task_type.clone(),
            status: record.status.clone(),
            changed_object_type: record.changed_object_type.clone(),
            changed_object_id: record.changed_object_id.clone(),
            created_at,
            action_template_id: record.action_template_id.clone(),
            execution_mode: record.execution_mode.clone(),
            responsible_user: record.responsible_user.clone(),
            responsible_role: record.responsible_role.clone(),
            escalation_duration: record.escalation_duration,
            escalation_duration_unit: record.escalation_duration_unit.clone(),
            require_document_upload: record.require_document_upload,
            require_comment: record.require_comment,
            require_approval_reference: record.require_approval_reference,
            action_title: record.action_title.clone(),
            action_description: record.action_description.clone(),
            assigned_to_type: record.assigned_to_type.clone(),
            assigned_to_id: record.assigned_to_id.clone(),
            assigned_to_name: record.assigned_to_name.clone(),
        }
    }
}

/// Trait for publishing actions to an external feed (queue or any future target).
#[async_trait]
pub trait ActionFeedPublisher: Send + Sync {
    async fn publish_actions(&self, actions: &[ActionRecord]) -> Result<(), DynError>;
}

/// Publishes actions to a RabbitMQ queue so downstream consumers (e.g. OutSystems)
/// can read and process the action records.
///
/// Configuration via environment variables:
/// - `ACTION_FEED_RABBITMQ_URL`   – RabbitMQ connection string (amqp/amqps);
///                                   falls back to `RABBITMQ_URL` if not set
/// - `ACTION_FEED_QUEUE`          – queue name (default: `actions_feed`)
pub struct RabbitMqActionFeedPublisher {
    url: String,
    queue_name: String,
}

impl RabbitMqActionFeedPublisher {
    pub fn new(url: String, queue_name: String) -> Self {
        Self { url, queue_name }
    }

    fn validate_url(url: &str) -> Result<(), String> {
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

    /// Build from environment variables. Returns `None` when the required
    /// env vars are not set (publisher is optional).
    pub fn from_env() -> Option<Self> {
        let url = std::env::var("ACTION_FEED_RABBITMQ_URL")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .or_else(|| {
                std::env::var("RABBITMQ_URL")
                    .ok()
                    .filter(|v| !v.trim().is_empty())
            })?;

        let queue_name = std::env::var("ACTION_FEED_QUEUE")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| "actions_feed".to_string());

        if let Err(err) = Self::validate_url(&url) {
            warn!(
                "invalid ACTION_FEED_RABBITMQ_URL/RABBITMQ_URL; ActionFeedPublisher disabled: {}",
                err
            );
            return None;
        }

        info!("ActionFeedPublisher configured: queue={}", queue_name);
        Some(Self::new(url, queue_name))
    }
}

#[async_trait]
impl ActionFeedPublisher for RabbitMqActionFeedPublisher {
    async fn publish_actions(&self, actions: &[ActionRecord]) -> Result<(), DynError> {
        if actions.is_empty() {
            return Ok(());
        }

        let payloads: Vec<ActionFeedPayload> =
            actions.iter().map(ActionFeedPayload::from_record).collect();

        let payload_json = serde_json::to_vec(&payloads)?;

        let connection = Connection::connect(&self.url, ConnectionProperties::default()).await?;
        let channel = connection.create_channel().await?;

        channel
            .queue_declare(
                &self.queue_name,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        channel
            .basic_publish(
                "",
                &self.queue_name,
                BasicPublishOptions::default(),
                &payload_json,
                BasicProperties::default().with_content_type("application/json".into()),
            )
            .await?
            .await?;

        info!(
            "ActionFeedPublisher: published {} action(s) to queue '{}'",
            payloads.len(),
            self.queue_name
        );

        Ok(())
    }
}
