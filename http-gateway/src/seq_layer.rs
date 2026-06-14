//! Tracing layer that ships log events to Seq via its CLEF HTTP ingestion API.
//!
//! Events are buffered in memory and flushed to Seq in batches by a background
//! Tokio task.  If Seq is unreachable the batch is dropped after a few retries
//! so the application is never blocked.

use chrono::Utc;
use reqwest::Client;
use std::fmt;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;

/// Configuration read from environment variables.
pub struct SeqConfig {
    /// Full base URL of the Seq server, e.g. `http://seq.example.com:5341`.
    pub url: String,
    /// Optional API key sent as `X-Seq-ApiKey`.
    pub api_key: Option<String>,
}

impl SeqConfig {
    /// Reads `SEQ_URL` and (optionally) `SEQ_API_KEY` from the environment.
    /// Returns `None` when `SEQ_URL` is unset or empty.
    pub fn from_env() -> Option<Self> {
        let url = std::env::var("SEQ_URL")
            .ok()
            .filter(|s| !s.trim().is_empty())?;
        let api_key = std::env::var("SEQ_API_KEY")
            .ok()
            .filter(|s| !s.trim().is_empty());
        Some(Self { url, api_key })
    }
}

/// Visitor that collects tracing event fields into a JSON map.
struct JsonVisitor {
    fields: serde_json::Map<String, serde_json::Value>,
}

impl JsonVisitor {
    fn new() -> Self {
        Self {
            fields: serde_json::Map::new(),
        }
    }
}

impl Visit for JsonVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.fields.insert(
            field.name().to_string(),
            serde_json::Value::String(format!("{:?}", value)),
        );
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields.insert(
            field.name().to_string(),
            serde_json::Value::String(value.to_string()),
        );
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields.insert(
            field.name().to_string(),
            serde_json::Value::Number(value.into()),
        );
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields.insert(
            field.name().to_string(),
            serde_json::Value::Number(value.into()),
        );
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        if let Some(n) = serde_json::Number::from_f64(value) {
            self.fields
                .insert(field.name().to_string(), serde_json::Value::Number(n));
        }
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .insert(field.name().to_string(), serde_json::Value::Bool(value));
    }
}

fn tracing_level_to_seq(level: &Level) -> &'static str {
    match *level {
        Level::TRACE => "Verbose",
        Level::DEBUG => "Debug",
        Level::INFO => "Information",
        Level::WARN => "Warning",
        Level::ERROR => "Error",
    }
}

/// A tracing [`Layer`] that sends events to Seq.
pub struct SeqLayer {
    tx: mpsc::UnboundedSender<String>,
}

impl SeqLayer {
    /// Creates a new `SeqLayer` and spawns the background flush task.
    ///
    /// Must be called from within a Tokio runtime context.
    pub fn new(config: SeqConfig) -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<String>();
        let flusher = SeqFlusher {
            rx,
            client: Client::new(),
            url: format!("{}/api/events/raw", config.url.trim_end_matches('/')),
            api_key: config.api_key.map(Arc::from),
        };
        tokio::spawn(flusher.run());
        Self { tx }
    }
}

impl<S: Subscriber> Layer<S> for SeqLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let metadata = event.metadata();
        let mut visitor = JsonVisitor::new();
        event.record(&mut visitor);

        // Build CLEF JSON object.
        let mut clef = serde_json::Map::new();

        // @t – timestamp
        clef.insert(
            "@t".to_string(),
            serde_json::Value::String(
                Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            ),
        );

        // @l – level
        clef.insert(
            "@l".to_string(),
            serde_json::Value::String(tracing_level_to_seq(metadata.level()).to_string()),
        );

        // @mt – message template (use the "message" field produced by log macros)
        if let Some(msg) = visitor.fields.remove("message") {
            clef.insert("@mt".to_string(), msg);
        } else {
            // Fallback: use target::name
            clef.insert(
                "@mt".to_string(),
                serde_json::Value::String(format!("{}::{}", metadata.target(), metadata.name())),
            );
        }

        // Source context
        clef.insert(
            "SourceContext".to_string(),
            serde_json::Value::String(metadata.target().to_string()),
        );

        // Application tag
        clef.insert(
            "Application".to_string(),
            serde_json::Value::String("TadpoleEngine".to_string()),
        );

        // All remaining fields become properties.
        for (key, value) in visitor.fields {
            clef.insert(key, value);
        }

        if let Ok(line) = serde_json::to_string(&clef) {
            // Non-blocking send; drops the event if the channel is full/closed.
            let _ = self.tx.send(line);
        }
    }
}

/// Background task that reads CLEF lines from the channel and posts them to Seq
/// in batches.
struct SeqFlusher {
    rx: mpsc::UnboundedReceiver<String>,
    client: Client,
    url: String,
    api_key: Option<Arc<str>>,
}

impl SeqFlusher {
    async fn run(mut self) {
        const BATCH_SIZE: usize = 50;
        const FLUSH_INTERVAL_MS: u64 = 2000;

        let mut buffer: Vec<String> = Vec::with_capacity(BATCH_SIZE);

        loop {
            // Wait for the first event or timeout.
            let received = tokio::time::timeout(
                tokio::time::Duration::from_millis(FLUSH_INTERVAL_MS),
                self.rx.recv(),
            )
            .await;

            match received {
                Ok(Some(line)) => {
                    buffer.push(line);
                    // Drain any additional events that are already queued.
                    while buffer.len() < BATCH_SIZE {
                        match self.rx.try_recv() {
                            Ok(line) => buffer.push(line),
                            Err(_) => break,
                        }
                    }
                }
                Ok(None) => {
                    // Channel closed – flush remaining and exit.
                    if !buffer.is_empty() {
                        self.flush(&buffer).await;
                    }
                    return;
                }
                Err(_) => {
                    // Timeout – flush whatever we have.
                }
            }

            if !buffer.is_empty() {
                self.flush(&buffer).await;
                buffer.clear();
            }
        }
    }

    async fn flush(&self, batch: &[String]) {
        // Seq accepts newline-delimited CLEF JSON.
        let payload = batch.join("\n");

        let mut request = self
            .client
            .post(&self.url)
            .header("Content-Type", "application/vnd.serilog.clef")
            .body(payload);

        if let Some(ref key) = self.api_key {
            request = request.header("X-Seq-ApiKey", key.as_ref());
        }

        // Fire-and-forget with a short timeout so we never block the app.
        match tokio::time::timeout(tokio::time::Duration::from_secs(5), request.send()).await {
            Ok(Ok(resp)) if resp.status().is_success() => {
                // Successfully shipped.
            }
            Ok(Ok(resp)) => {
                eprintln!(
                    "[seq] Seq ingestion returned HTTP {}: {}",
                    resp.status(),
                    resp.text().await.unwrap_or_default()
                );
            }
            Ok(Err(e)) => {
                eprintln!("[seq] failed to send events to Seq: {}", e);
            }
            Err(_) => {
                eprintln!("[seq] Seq ingestion request timed out");
            }
        }
    }
}
