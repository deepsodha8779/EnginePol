//! WorkRouter: resolves role → person/team/queue and notification channels.
//! Placeholder until OutSystems API integration; supports config-based routing and fallback queue.

use async_trait::async_trait;
use log::{info, warn};
use serde::Deserialize;
use std::collections::HashMap;

/// Who or where the task is assigned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssigneeType {
    User,
    Team,
    Queue,
}

/// A single notification channel (e.g. email, Slack, Teams).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotificationChannel {
    Email,
    Slack,
    Teams,
}

/// Result of resolving a route: assignee and channels to notify.
#[derive(Debug, Clone)]
pub struct RouteResult {
    pub assignee_type: AssigneeType,
    pub assignee_id: String,
    pub display_name: Option<String>,
    pub notification_channels: Vec<NotificationChannel>,
    pub used_fallback: bool,
}

/// Input for routing: tenant, role (e.g. task_type or playbook-derived), and action context.
/// When an ActionTemplate is matched, `responsible_user` and `responsible_role` carry the
/// template's responsibility assignment so the router can use them before falling back to
/// config-based routing.
#[derive(Debug, Clone)]
pub struct RouteRequest {
    pub tenant_id: String,
    pub role_id: String,
    pub task_type: String,
    pub playbook_id: String,
    pub action_id: String,
    /// Direct user assignment from the action template (highest priority).
    pub responsible_user: Option<String>,
    /// Role-based assignment from the action template (second priority).
    pub responsible_role: Option<String>,
}

#[async_trait]
pub trait WorkRouter: Send + Sync {
    /// Resolve where to send the action: assignee (user/team/queue) and notification channels.
    async fn resolve(
        &self,
        request: &RouteRequest,
    ) -> Result<RouteResult, Box<dyn std::error::Error + Send + Sync>>;
}

/// Assignee entry in config (user, team, or queue id and optional display name).
#[derive(Debug, Clone, Deserialize)]
pub struct AssigneeEntry {
    #[serde(rename = "type")]
    pub assignee_type: String,
    pub id: String,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub notification_channels: Vec<String>,
}

/// Config-based router: role_id → assignee, plus fallback queue when no match.
#[derive(Clone)]
pub struct ConfigWorkRouter {
    /// role_id (e.g. task_type or "tenant:task_type") -> assignee entry
    routes: HashMap<String, AssigneeEntry>,
    /// Used when no role matches
    fallback_queue_id: String,
}

impl ConfigWorkRouter {
    pub fn new(routes: HashMap<String, AssigneeEntry>, fallback_queue_id: String) -> Self {
        Self {
            routes,
            fallback_queue_id,
        }
    }

    /// Build from env: WORK_ROUTER_FALLBACK_QUEUE, optional WORK_ROUTER_ROLES path to JSON.
    pub fn from_env() -> Self {
        let fallback = std::env::var("WORK_ROUTER_FALLBACK_QUEUE")
            .unwrap_or_else(|_| "default-governance-queue".to_string());
        let routes = load_routes_from_env();
        Self::new(routes, fallback)
    }

    fn parse_channels(names: &[String]) -> Vec<NotificationChannel> {
        names
            .iter()
            .filter_map(|s| match s.to_lowercase().as_str() {
                "email" => Some(NotificationChannel::Email),
                "slack" => Some(NotificationChannel::Slack),
                "teams" => Some(NotificationChannel::Teams),
                _ => None,
            })
            .collect()
    }
}

fn load_routes_from_env() -> HashMap<String, AssigneeEntry> {
    let mut routes = HashMap::new();
    if let Ok(path) = std::env::var("WORK_ROUTER_ROLES_CONFIG") {
        if let Ok(data) = std::fs::read_to_string(&path) {
            if let Ok(parsed) = serde_json::from_str::<HashMap<String, AssigneeEntry>>(&data) {
                routes = parsed;
            }
        }
    }
    routes
}

#[async_trait]
impl WorkRouter for ConfigWorkRouter {
    async fn resolve(
        &self,
        request: &RouteRequest,
    ) -> Result<RouteResult, Box<dyn std::error::Error + Send + Sync>> {
        // Priority 1: Template specifies a responsible_user — route directly to them.
        if let Some(ref user_id) = request.responsible_user {
            if !user_id.is_empty() {
                info!(
                    "WorkRouter resolved via template responsible_user={}",
                    user_id
                );
                return Ok(RouteResult {
                    assignee_type: AssigneeType::User,
                    assignee_id: user_id.clone(),
                    display_name: None,
                    notification_channels: vec![NotificationChannel::Email],
                    used_fallback: false,
                });
            }
        }

        // Priority 2: Template specifies a responsible_role — look it up in config,
        // then fall through to standard keys if not found.
        if let Some(ref role) = request.responsible_role {
            if !role.is_empty() {
                // Try tenant-scoped role first, then role alone.
                let role_keys = [format!("{}:{}", request.tenant_id, role), role.clone()];
                for key in &role_keys {
                    if let Some(entry) = self.routes.get(key) {
                        let assignee_type = match entry.assignee_type.to_lowercase().as_str() {
                            "user" => AssigneeType::User,
                            "team" => AssigneeType::Team,
                            _ => AssigneeType::Queue,
                        };
                        let channels = Self::parse_channels(&entry.notification_channels);
                        info!(
                            "WorkRouter resolved via template responsible_role={} -> {} {}",
                            role, entry.assignee_type, entry.id
                        );
                        return Ok(RouteResult {
                            assignee_type,
                            assignee_id: entry.id.clone(),
                            display_name: entry.display_name.clone(),
                            notification_channels: if channels.is_empty() {
                                vec![NotificationChannel::Email]
                            } else {
                                channels
                            },
                            used_fallback: false,
                        });
                    }
                }
            }
        }

        // Priority 3–6: Config-based lookup by tenant:task_type, task_type, playbook, default.
        let role_keys = [
            format!("{}:{}", request.tenant_id, request.task_type),
            request.task_type.clone(),
            request.playbook_id.clone(),
            "default".to_string(),
        ];

        for key in &role_keys {
            if let Some(entry) = self.routes.get(key) {
                let assignee_type = match entry.assignee_type.to_lowercase().as_str() {
                    "user" => AssigneeType::User,
                    "team" => AssigneeType::Team,
                    _ => AssigneeType::Queue,
                };
                let channels = Self::parse_channels(&entry.notification_channels);
                info!(
                    "WorkRouter resolved role_key={} -> {} {}",
                    key, entry.assignee_type, entry.id
                );
                return Ok(RouteResult {
                    assignee_type,
                    assignee_id: entry.id.clone(),
                    display_name: entry.display_name.clone(),
                    notification_channels: if channels.is_empty() {
                        vec![NotificationChannel::Email]
                    } else {
                        channels
                    },
                    used_fallback: false,
                });
            }
        }

        warn!(
            "WorkRouter no match for tenant={} task_type={}; using fallback queue",
            request.tenant_id, request.task_type
        );
        Ok(RouteResult {
            assignee_type: AssigneeType::Queue,
            assignee_id: self.fallback_queue_id.clone(),
            display_name: Some("Fallback governance queue".to_string()),
            notification_channels: vec![NotificationChannel::Email],
            used_fallback: true,
        })
    }
}
