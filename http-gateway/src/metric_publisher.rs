use async_trait::async_trait;
use domain::envelope::CanonicalEnvelope;
use lapin::{BasicProperties, Connection, ConnectionProperties, options::BasicPublishOptions};
use log::{info, warn};
use url::Url;

use crate::metric_manager::MetricEventPublisher;

type DynError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone)]
pub struct RabbitMqMetricEventPublisher {
    url: String,
    queue_name: String,
}

impl RabbitMqMetricEventPublisher {
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

    pub fn from_env() -> Option<Self> {
        let url = std::env::var("KPI_RABBITMQ_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                std::env::var("RABBITMQ_URL")
                    .ok()
                    .filter(|value| !value.trim().is_empty())
            })?;

        let queue_name = std::env::var("KPI_RABBITMQ_QUEUE")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                std::env::var("RABBITMQ_QUEUE")
                    .ok()
                    .filter(|value| !value.trim().is_empty())
            })
            .unwrap_or_else(|| "events_intake".to_string());

        if let Err(err) = Self::validate_url(&url) {
            warn!(
                "invalid KPI_RABBITMQ_URL/RABBITMQ_URL; metric publisher disabled: {}",
                err
            );
            return None;
        }

        Some(Self { url, queue_name })
    }
}

#[async_trait]
impl MetricEventPublisher for RabbitMqMetricEventPublisher {
    async fn publish_event(&self, envelope: &CanonicalEnvelope) -> Result<(), DynError> {
        let connection = Connection::connect(&self.url, ConnectionProperties::default()).await?;
        let channel = connection.create_channel().await?;
        let payload = serde_json::to_vec(envelope)?;
        channel
            .basic_publish(
                "",
                &self.queue_name,
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default(),
            )
            .await?
            .await?;
        info!(
            "published KPI event: queue={} event_id={} event_name={}",
            self.queue_name, envelope.head.event_id, envelope.head.event_name
        );
        Ok(())
    }
}
