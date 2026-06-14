use std::collections::HashMap;
use std::sync::Arc;

use actix::prelude::*;
use actix_web::{App, HttpServer, web};
use engine_core::actors::{
    assigner::{AssignerActor, PlaybookConfig},
    diagnostics::DiagnosticsActor,
    dispatcher::{BooleanRuleEvaluator, DispatcherActor, EnrichmentStubEvaluator, RuleEvaluator},
    handlers::boolean_handler::BooleanRuleHandler,
    orchestrator::OrchestratorActor,
};
use engine_core::dto::rules::RuleKind;
use http_gateway::action_builder::ActionBuilder;
use http_gateway::action_feed_publisher::{ActionFeedPublisher, RabbitMqActionFeedPublisher};
use http_gateway::action_store::MongoActionStore;
use http_gateway::action_template::{ActionTemplateStore, MongoActionTemplateStore};
use http_gateway::metric_manager::{MetricEventPublisher, MetricManager};
use http_gateway::metric_publisher::RabbitMqMetricEventPublisher;
use http_gateway::metric_store::MongoMetricStore;
use http_gateway::mongo_store::{IntakeStore, MongoStore};
use http_gateway::run_consumer;
use http_gateway::seq_layer::{SeqConfig, SeqLayer};
use http_gateway::work_router::ConfigWorkRouter;
use log::{debug, info, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().ok();

    // Console layer (replaces env_logger).
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_level(true);

    // Default filter: INFO, overridable via RUST_LOG env var.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // Optional Seq layer – enabled when SEQ_URL is set.
    let seq_layer = SeqConfig::from_env().map(|cfg| {
        println!("Seq integration enabled: {}", cfg.url);
        SeqLayer::new(cfg)
    });

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(seq_layer)
        .init();

    // Bridge existing `log` crate macros into tracing.
    // (tracing-subscriber already installs a tracing-log compat layer by default
    //  when the `tracing-log` feature is enabled, but we call this explicitly
    //  so the bridge is guaranteed.)
    let _ = tracing_log::LogTracer::init();

    println!("http-gateway starting (stdout)");
    info!("http-gateway starting (logger)");

    let mongo_store = MongoStore::from_env()
        .await
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err.to_string()))?;
    let store: Arc<dyn IntakeStore> = Arc::new(mongo_store);

    let metric_manager: Option<Arc<MetricManager>> = match MongoMetricStore::from_env().await {
        Ok(metric_store) => {
            let publisher: Option<Arc<dyn MetricEventPublisher>> =
                RabbitMqMetricEventPublisher::from_env()
                    .map(|publisher| Arc::new(publisher) as Arc<dyn MetricEventPublisher>);
            let thresholds = match MetricManager::thresholds_from_env() {
                Ok(thresholds) => thresholds,
                Err(err) => {
                    warn!(
                        "metric thresholds config invalid: {}; using default thresholds",
                        err
                    );
                    http_gateway::MetricThreshold::default_thresholds()
                }
            };
            Some(Arc::new(MetricManager::new(
                Arc::new(metric_store),
                publisher,
                thresholds,
            )))
        }
        Err(err) => {
            warn!(
                "metric store not available: {}; MetricManager disabled",
                err
            );
            None
        }
    };
    let template_store: Option<Arc<dyn ActionTemplateStore>> =
        match MongoActionTemplateStore::from_env().await {
            Ok(ts) => {
                info!("action template store connected; template-driven actions enabled");
                Some(Arc::new(ts))
            }
            Err(e) => {
                warn!(
                    "action template store not available: {}; templates disabled",
                    e
                );
                None
            }
        };

    let action_builder: Option<Arc<ActionBuilder>> = match MongoActionStore::from_env().await {
        Ok(action_store) => {
            info!("action store connected; ActionBuilder enabled");
            let action_store_arc = Arc::new(action_store);
            let work_router = Arc::new(ConfigWorkRouter::from_env());

            let builder = match template_store.clone() {
                Some(ts) => ActionBuilder::with_all(action_store_arc, work_router, ts),
                None => ActionBuilder::with_work_router(action_store_arc, work_router),
            };
            Some(Arc::new(builder))
        }
        Err(e) => {
            warn!("action store not available: {}; ActionBuilder disabled", e);
            None
        }
    };

    let action_feed_publisher: Option<Arc<dyn ActionFeedPublisher>> =
        RabbitMqActionFeedPublisher::from_env()
            .map(|p| Arc::new(p) as Arc<dyn ActionFeedPublisher>);
    if action_feed_publisher.is_some() {
        info!("ActionFeedPublisher enabled (RabbitMQ queue)");
    } else {
        info!("ActionFeedPublisher disabled (ACTION_FEED_RABBITMQ_URL / RABBITMQ_URL not set)");
    }

    let boolean_handler = BooleanRuleHandler.start();
    debug!("boolean handler started");
    let mut evaluators: HashMap<RuleKind, Arc<dyn RuleEvaluator>> = HashMap::new();
    evaluators.insert(
        RuleKind::Boolean,
        Arc::new(BooleanRuleEvaluator::new(boolean_handler)),
    );
    evaluators.insert(RuleKind::EnrichmentStub, Arc::new(EnrichmentStubEvaluator));

    let dispatcher = DispatcherActor { evaluators }.start();
    let assigner_config: PlaybookConfig =
        serde_json::from_str(r#"{"playbooks":[]}"#).expect("assigner config");
    let assigner = AssignerActor::from_config(assigner_config).start();
    let orchestrator = OrchestratorActor.start();
    let diagnostics = DiagnosticsActor::default().start();
    info!("actors started: assigner/dispatcher/orchestrator/diagnostics");

    run_consumer(
        assigner.clone(),
        dispatcher.clone(),
        orchestrator.clone(),
        diagnostics.clone(),
        store.clone(),
        action_builder.clone(),
        metric_manager.clone(),
        action_feed_publisher.clone(),
    );

    info!("http-gateway binding: 127.0.0.1:8083");
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(assigner.clone()))
            .app_data(web::Data::new(dispatcher.clone()))
            .app_data(web::Data::new(orchestrator.clone()))
            .app_data(web::Data::new(diagnostics.clone()))
            .app_data(web::Data::new(store.clone()))
            .app_data(web::Data::new(action_builder.clone()))
            .app_data(web::Data::new(template_store.clone()))
            .app_data(web::Data::new(metric_manager.clone()))
            .app_data(web::Data::new(action_feed_publisher.clone()))
            .route("/intake", web::post().to(http_gateway::intake))
            .route("/diagnostics", web::get().to(http_gateway::diagnostics))
            .route(
                "/action-templates",
                web::get().to(http_gateway::action_templates),
            )
            .route(
                "/action-templates/{template_id}",
                web::get().to(http_gateway::action_template_by_id),
            )
            .route(
                "/event-logs/{event_id}",
                web::get().to(http_gateway::event_logs_by_event_id),
            )
            .route("/metrics", web::get().to(http_gateway::metrics))
            .route(
                "/metrics/{event_id}",
                web::get().to(http_gateway::metrics_by_event_id),
            )
    })
    .bind(("127.0.0.1", 8083))?
    .run()
    .await
}
