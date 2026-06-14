use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Decision {
    Pass {
        reason_code: String,
        message: String,
    },
    Fail {
        reason_code: String,
        message: String,
    },
    Inconclusive {
        reason_code: String,
        message: String,
    },
}

impl Decision {
    pub fn pass(reason_code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Pass {
            reason_code: reason_code.into(),
            message: message.into(),
        }
    }

    pub fn fail(reason_code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Fail {
            reason_code: reason_code.into(),
            message: message.into(),
        }
    }

    pub fn inconclusive(reason_code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Inconclusive {
            reason_code: reason_code.into(),
            message: message.into(),
        }
    }

    pub fn is_fail(&self) -> bool {
        matches!(self, Decision::Fail { .. })
    }

    pub fn is_inconclusive(&self) -> bool {
        matches!(self, Decision::Inconclusive { .. })
    }

    pub fn is_pass(&self) -> bool {
        matches!(self, Decision::Pass { .. })
    }
}
