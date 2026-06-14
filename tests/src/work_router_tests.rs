//! Tests for WorkRouter: resolve role to assignee/queue, fallback, notification channels.

use async_trait::async_trait;
use http_gateway::{
    AssigneeEntry, AssigneeType, ConfigWorkRouter, NotificationChannel, RouteRequest, RouteResult,
    WorkRouter,
};
use std::collections::HashMap;

fn sample_request() -> RouteRequest {
    RouteRequest {
        tenant_id: "tenant-a".to_string(),
        role_id: "TASK_GOVERNANCE_REVIEW".to_string(),
        task_type: "TASK_GOVERNANCE_REVIEW".to_string(),
        playbook_id: "playbook.invoice_fail".to_string(),
        action_id: "01ACTION0000000000000001".to_string(),
        responsible_user: None,
        responsible_role: None,
    }
}

#[tokio::test]
async fn work_router_returns_fallback_queue_when_no_routes_configured() {
    let router = ConfigWorkRouter::new(HashMap::new(), "fallback-queue-1".to_string());
    let request = sample_request();

    let result = router.resolve(&request).await.unwrap();

    assert_eq!(result.assignee_type, AssigneeType::Queue);
    assert_eq!(result.assignee_id, "fallback-queue-1");
    assert!(result.used_fallback);
    assert!(!result.notification_channels.is_empty());
}

#[tokio::test]
async fn work_router_returns_configured_user_when_task_type_matches() {
    let mut routes = HashMap::new();
    routes.insert(
        "TASK_GOVERNANCE_REVIEW".to_string(),
        AssigneeEntry {
            assignee_type: "user".to_string(),
            id: "user-123".to_string(),
            display_name: Some("Jane Doe".to_string()),
            notification_channels: vec!["email".to_string(), "slack".to_string()],
        },
    );
    let router = ConfigWorkRouter::new(routes, "fallback".to_string());
    let request = sample_request();

    let result = router.resolve(&request).await.unwrap();

    assert_eq!(result.assignee_type, AssigneeType::User);
    assert_eq!(result.assignee_id, "user-123");
    assert_eq!(result.display_name.as_deref(), Some("Jane Doe"));
    assert!(!result.used_fallback);
    assert!(
        result
            .notification_channels
            .contains(&NotificationChannel::Email)
    );
    assert!(
        result
            .notification_channels
            .contains(&NotificationChannel::Slack)
    );
}

#[tokio::test]
async fn work_router_returns_configured_team_when_playbook_matches() {
    let mut routes = HashMap::new();
    routes.insert(
        "playbook.invoice_fail".to_string(),
        AssigneeEntry {
            assignee_type: "team".to_string(),
            id: "team-governance".to_string(),
            display_name: None,
            notification_channels: vec!["teams".to_string()],
        },
    );
    let router = ConfigWorkRouter::new(routes, "fallback".to_string());
    let request = sample_request();

    let result = router.resolve(&request).await.unwrap();

    assert_eq!(result.assignee_type, AssigneeType::Team);
    assert_eq!(result.assignee_id, "team-governance");
    assert!(!result.used_fallback);
    assert!(
        result
            .notification_channels
            .contains(&NotificationChannel::Teams)
    );
}

#[tokio::test]
async fn work_router_tenant_task_type_key_takes_precedence_over_task_type_only() {
    let mut routes = HashMap::new();
    routes.insert(
        "tenant-a:TASK_GOVERNANCE_REVIEW".to_string(),
        AssigneeEntry {
            assignee_type: "queue".to_string(),
            id: "tenant-a-queue".to_string(),
            display_name: None,
            notification_channels: vec![],
        },
    );
    routes.insert(
        "TASK_GOVERNANCE_REVIEW".to_string(),
        AssigneeEntry {
            assignee_type: "user".to_string(),
            id: "global-user".to_string(),
            display_name: None,
            notification_channels: vec![],
        },
    );
    let router = ConfigWorkRouter::new(routes, "fallback".to_string());
    let request = sample_request();

    let result = router.resolve(&request).await.unwrap();

    assert_eq!(result.assignee_type, AssigneeType::Queue);
    assert_eq!(result.assignee_id, "tenant-a-queue");
}

#[tokio::test]
async fn work_router_uses_default_route_and_falls_back_to_email_channel() {
    let mut routes = HashMap::new();
    routes.insert(
        "default".to_string(),
        AssigneeEntry {
            assignee_type: "unknown".to_string(),
            id: "default-queue".to_string(),
            display_name: Some("Default Queue".to_string()),
            notification_channels: vec!["pager".to_string()],
        },
    );
    let router = ConfigWorkRouter::new(routes, "fallback".to_string());
    let request = sample_request();

    let result = router.resolve(&request).await.unwrap();

    assert_eq!(result.assignee_type, AssigneeType::Queue);
    assert_eq!(result.assignee_id, "default-queue");
    assert_eq!(result.display_name.as_deref(), Some("Default Queue"));
    assert_eq!(
        result.notification_channels,
        vec![NotificationChannel::Email]
    );
    assert!(!result.used_fallback);
}

/// Mock router for integration tests: returns a fixed result.
struct MockWorkRouter {
    result: RouteResult,
}

#[async_trait]
impl WorkRouter for MockWorkRouter {
    async fn resolve(
        &self,
        _request: &RouteRequest,
    ) -> Result<RouteResult, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.result.clone())
    }
}

#[tokio::test]
async fn mock_work_router_returns_configured_result() {
    let result = RouteResult {
        assignee_type: AssigneeType::User,
        assignee_id: "mock-user".to_string(),
        display_name: Some("Mock".to_string()),
        notification_channels: vec![NotificationChannel::Email],
        used_fallback: false,
    };
    let router = MockWorkRouter {
        result: result.clone(),
    };
    let request = sample_request();

    let resolved = router.resolve(&request).await.unwrap();

    assert_eq!(resolved.assignee_id, result.assignee_id);
    assert_eq!(resolved.assignee_type, result.assignee_type);
}

#[tokio::test]
async fn work_router_uses_responsible_user_from_template() {
    // Even with config routes, responsible_user takes highest priority.
    let mut routes = HashMap::new();
    routes.insert(
        "TASK_GOVERNANCE_REVIEW".to_string(),
        AssigneeEntry {
            assignee_type: "queue".to_string(),
            id: "default-queue".to_string(),
            display_name: None,
            notification_channels: vec!["email".to_string()],
        },
    );
    let router = ConfigWorkRouter::new(routes, "fallback".to_string());

    let mut request = sample_request();
    request.responsible_user = Some("user-jane".to_string());

    let result = router.resolve(&request).await.unwrap();

    assert_eq!(result.assignee_type, AssigneeType::User);
    assert_eq!(result.assignee_id, "user-jane");
    assert!(!result.used_fallback);
}

#[tokio::test]
async fn work_router_uses_responsible_role_from_template() {
    let mut routes = HashMap::new();
    routes.insert(
        "Compliance Officer".to_string(),
        AssigneeEntry {
            assignee_type: "team".to_string(),
            id: "team-compliance".to_string(),
            display_name: Some("Compliance Team".to_string()),
            notification_channels: vec!["email".to_string(), "teams".to_string()],
        },
    );
    let router = ConfigWorkRouter::new(routes, "fallback".to_string());

    let mut request = sample_request();
    request.responsible_role = Some("Compliance Officer".to_string());

    let result = router.resolve(&request).await.unwrap();

    assert_eq!(result.assignee_type, AssigneeType::Team);
    assert_eq!(result.assignee_id, "team-compliance");
    assert_eq!(result.display_name.as_deref(), Some("Compliance Team"));
    assert!(!result.used_fallback);
}

#[tokio::test]
async fn work_router_responsible_user_takes_precedence_over_role() {
    let mut routes = HashMap::new();
    routes.insert(
        "Compliance Officer".to_string(),
        AssigneeEntry {
            assignee_type: "team".to_string(),
            id: "team-compliance".to_string(),
            display_name: None,
            notification_channels: vec![],
        },
    );
    let router = ConfigWorkRouter::new(routes, "fallback".to_string());

    let mut request = sample_request();
    request.responsible_user = Some("user-direct".to_string());
    request.responsible_role = Some("Compliance Officer".to_string());

    let result = router.resolve(&request).await.unwrap();

    // responsible_user wins over responsible_role.
    assert_eq!(result.assignee_type, AssigneeType::User);
    assert_eq!(result.assignee_id, "user-direct");
}

#[tokio::test]
async fn work_router_falls_back_when_role_not_in_config() {
    let router = ConfigWorkRouter::new(HashMap::new(), "fallback-q".to_string());

    let mut request = sample_request();
    request.responsible_role = Some("Unknown Role".to_string());

    let result = router.resolve(&request).await.unwrap();

    // Role not in config, no other routes → fallback.
    assert_eq!(result.assignee_type, AssigneeType::Queue);
    assert_eq!(result.assignee_id, "fallback-q");
    assert!(result.used_fallback);
}

#[tokio::test]
async fn work_router_tenant_scoped_role_takes_precedence() {
    let mut routes = HashMap::new();
    routes.insert(
        "tenant-a:Compliance Officer".to_string(),
        AssigneeEntry {
            assignee_type: "team".to_string(),
            id: "tenant-a-compliance".to_string(),
            display_name: Some("Tenant A Compliance".to_string()),
            notification_channels: vec!["teams".to_string()],
        },
    );
    routes.insert(
        "Compliance Officer".to_string(),
        AssigneeEntry {
            assignee_type: "team".to_string(),
            id: "global-compliance".to_string(),
            display_name: None,
            notification_channels: vec![],
        },
    );
    let router = ConfigWorkRouter::new(routes, "fallback".to_string());

    let mut request = sample_request();
    request.responsible_role = Some("Compliance Officer".to_string());

    let result = router.resolve(&request).await.unwrap();

    assert_eq!(result.assignee_id, "tenant-a-compliance");
    assert_eq!(result.display_name.as_deref(), Some("Tenant A Compliance"));
}
