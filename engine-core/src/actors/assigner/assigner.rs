use actix::prelude::*;
use domain::envelope::CanonicalEnvelope;
use log::{debug, info, warn};
use serde::{Deserialize, Deserializer, de};
use std::{collections::HashSet, fs, path::Path};

use crate::dto::{
    assigner::Assign,
    rules::{PlaybookAssignment, RuleKind, RuleLogic, RuleSpec},
    tadpole::Tadpole,
};
use crate::simple_expr::eval_head_expr;

#[derive(Debug, Clone)]
pub struct PlaybookConfig {
    pub codex: Vec<CodexDef>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CodexDef {
    #[serde(rename = "version_id")]
    pub version_id: Option<String>,
    pub playbooks: Vec<PlaybookDef>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum PlaybookConfigWire {
    Flat { playbooks: Vec<PlaybookDef> },
    Codex { codex: Vec<CodexDef> },
    Single(PlaybookDef),
}

impl<'de> Deserialize<'de> for PlaybookConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = PlaybookConfigWire::deserialize(deserializer)?;
        let config = match wire {
            PlaybookConfigWire::Flat { playbooks } => Ok(Self {
                codex: vec![CodexDef {
                    version_id: None,
                    playbooks,
                }],
            }),
            PlaybookConfigWire::Codex { codex } => {
                if codex.is_empty() {
                    return Err(de::Error::custom("codex array is empty"));
                }
                Ok(Self { codex })
            }
            PlaybookConfigWire::Single(playbook) => Ok(Self {
                codex: vec![CodexDef {
                    version_id: None,
                    playbooks: vec![playbook],
                }],
            }),
        }?;

        config.validate().map_err(de::Error::custom)?;
        Ok(config)
    }
}

impl PlaybookConfig {
    fn validate(&self) -> Result<(), String> {
        for codex in &self.codex {
            for playbook in &codex.playbooks {
                if playbook.trigger.is_none() && playbook.match_expr.is_none() {
                    return Err(format!(
                        "playbook {} missing trigger or match_expr",
                        playbook.id
                    ));
                }

                let mut seen_rule_ids = HashSet::new();
                for rule in &playbook.rules {
                    let Some(rule_id) = rule.effective_rule_id() else {
                        return Err(format!(
                            "playbook {} contains rule missing rule_id",
                            playbook.id
                        ));
                    };
                    if !seen_rule_ids.insert(rule_id.to_string()) {
                        return Err(format!(
                            "playbook {} contains duplicate rule_id {}",
                            playbook.id, rule_id
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, String> {
        debug!("loading playbook config: {}", path.as_ref().display());
        let data = fs::read_to_string(path.as_ref())
            .map_err(|err| format!("unable to read config {}: {err}", path.as_ref().display()))?;
        let config: PlaybookConfig = serde_json::from_str(&data).map_err(|err| {
            format!(
                "invalid playbook config JSON in {}: {err}",
                path.as_ref().display()
            )
        })?;
        let playbook_count: usize = config.codex.iter().map(|entry| entry.playbooks.len()).sum();
        info!(
            "playbook config loaded: codex={} playbooks={}",
            config.codex.len(),
            playbook_count
        );
        Ok(config)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PlaybookDef {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub execution_mode: Option<String>,
    #[serde(default)]
    pub trigger: Option<TriggerDef>,
    #[serde(default)]
    pub match_expr: Option<String>,
    #[serde(default)]
    pub rules: Vec<RuleDef>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TriggerDef {
    pub object_type: String,
    pub change_kind: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RuleDef {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub rule_id: Option<String>,
    #[serde(default)]
    pub order_seq: Option<u32>,
    #[serde(default)]
    pub is_critical: bool,
    #[serde(default)]
    pub kind: Option<RuleKind>,
    #[serde(default)]
    pub expr: Option<String>,
}

impl RuleDef {
    fn effective_rule_id(&self) -> Option<&str> {
        self.rule_id.as_deref().or(self.id.as_deref())
    }

    fn effective_kind(&self) -> RuleKind {
        self.kind.unwrap_or(RuleKind::Boolean)
    }
}

pub struct AssignerActor {
    config: PlaybookConfig,
}

impl AssignerActor {
    pub fn from_config(config: PlaybookConfig) -> Self {
        Self { config }
    }

    pub fn from_default_path() -> Self {
        let path = std::env::var("PLAYBOOK_CONFIG")
            .unwrap_or_else(|_| "config/playbooks.json".to_string());
        info!("assigner loading config: path={}", path);
        let config = PlaybookConfig::from_path(&path)
            .unwrap_or_else(|err| panic!("failed to load playbook config: {err}"));
        Self { config }
    }

    fn assign_for_codex(&self, envelope: CanonicalEnvelope, codex: &CodexDef) -> Option<Tadpole> {
        info!(
            "assigner received envelope: event_id={} tenant_id={} event_name={}",
            envelope.head.event_id, envelope.head.tenant_id, envelope.head.event_name
        );
        let mut tadpole = Tadpole::from_envelope(envelope);
        tadpole.tail.codex_version_id = codex.version_id.clone();
        debug!(
            "assigner evaluating playbooks: count={}",
            codex.playbooks.len()
        );
        let mut matched_any = false;

        for playbook in &codex.playbooks {
            let matched = match (&playbook.trigger, &playbook.match_expr) {
                (Some(trigger), _) => {
                    let object_matches = tadpole
                        .head
                        .changed_object_type
                        .as_deref()
                        .map(|value| value == trigger.object_type)
                        .unwrap_or(false);
                    let kind_matches = tadpole
                        .head
                        .change_kind
                        .as_deref()
                        .map(|value| {
                            trigger
                                .change_kind
                                .iter()
                                .any(|kind| kind.eq_ignore_ascii_case(value))
                        })
                        .unwrap_or(false);
                    object_matches && kind_matches
                }
                (None, Some(match_expr)) => {
                    eval_head_expr(match_expr, &tadpole.head).unwrap_or(false)
                }
                (None, None) => false,
            };
            if !matched {
                info!(
                    "assigner did not match playbook: playbook_id={}",
                    playbook.id
                );
                continue;
            }

            matched_any = true;
            info!("assigner matched playbook: playbook_id={}", playbook.id);
            let reason = if let Some(trigger) = &playbook.trigger {
                format!(
                    "trigger matched: object_type={} change_kind={:?}",
                    trigger.object_type, trigger.change_kind
                )
            } else {
                format!(
                    "matched: {}",
                    playbook.match_expr.as_deref().unwrap_or("unknown")
                )
            };

            tadpole.tail.assigned_playbooks.push(PlaybookAssignment {
                playbook_id: playbook.id.clone(),
                reason,
            });

            let mut ordered_rules = playbook.rules.clone();
            ordered_rules.sort_by_key(|rule| {
                (
                    if rule.is_critical { 0 } else { 1 },
                    rule.order_seq.unwrap_or(u32::MAX),
                )
            });

            for rule in &ordered_rules {
                let Some(rule_id) = rule.effective_rule_id() else {
                    warn!(
                        "assigner skipped rule with missing rule_id: playbook_id={}",
                        playbook.id
                    );
                    continue;
                };
                tadpole.tail.ordered_rules.push(RuleSpec {
                    playbook_id: playbook.id.clone(),
                    rule_id: rule_id.to_string(),
                    kind: rule.effective_kind(),
                    expr: rule.expr.clone(),
                    rule_name: None,
                    object_type: None,
                    order_seq: rule.order_seq,
                    priority: Some(if rule.is_critical {
                        "HIGH".to_string()
                    } else {
                        "NORMAL".to_string()
                    }),
                    conditions: Vec::new(),
                    logic: RuleLogic::All,
                    is_critical: rule.is_critical,
                    skip_reason: None,
                    action_template_id: None,
                });
                info!(
                    "assigner added rule to order: playbook_id={} rule_id={} kind={:?}",
                    playbook.id,
                    rule_id,
                    rule.effective_kind()
                );
            }
        }

        if matched_any {
            Some(tadpole)
        } else {
            warn!("assigner no matching playbook for codex; skipping");
            None
        }
    }

    fn assign_all(&self, envelope: CanonicalEnvelope) -> Vec<Tadpole> {
        self.config
            .codex
            .iter()
            .filter_map(|codex| self.assign_for_codex(envelope.clone(), codex))
            .collect()
    }
}

impl Actor for AssignerActor {
    type Context = Context<Self>;
}

impl Handler<Assign> for AssignerActor {
    type Result = MessageResult<Assign>;

    fn handle(&mut self, msg: Assign, _: &mut Context<Self>) -> Self::Result {
        MessageResult(self.assign_all(msg.envelope))
    }
}
