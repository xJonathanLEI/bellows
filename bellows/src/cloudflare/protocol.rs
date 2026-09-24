use serde::{Deserialize, Serialize};

use super::{BoxDispatchError, validate_task_id, validate_task_name};

/// Acceptance policy; never passed to a processor or persisted in a schedule.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DispatchIntent {
    Run,
    Ensure,
}

/// One entry in an atomic-validation bulk dispatch request.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DispatchTask {
    pub task: TaskIdentity,
    pub intent: DispatchIntent,
}

impl DispatchTask {
    pub(super) fn validate(&self) -> Result<(), BoxDispatchError> {
        self.task.validate_dispatch()?;
        if self.intent == DispatchIntent::Ensure
            && matches!(self.task, TaskIdentity::Published { .. })
        {
            return Err("ensure requires a singleton identity".into());
        }
        Ok(())
    }
}

/// Logical identity; singleton row IDs belong exclusively to the execution backend.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum TaskIdentity {
    Published {
        #[serde(rename = "taskId")]
        task_id: String,
        #[serde(rename = "taskName")]
        task_name: String,
    },
    Singleton {
        #[serde(rename = "taskName")]
        task_name: String,
    },
}

impl TaskIdentity {
    /// Collision-free logical key, excluding the storage adapter's `task:` prefix.
    pub fn tracking_key(&self) -> String {
        match self {
            Self::Published { task_id, .. } => format!("published:{task_id}"),
            Self::Singleton { task_name } => format!("singleton:{task_name}"),
        }
    }

    pub(super) fn validate_dispatch(&self) -> Result<(), BoxDispatchError> {
        validate_task_name(self.name())?;
        match self {
            Self::Published { task_id, .. } => validate_task_id(task_id)?,
            Self::Singleton { .. } if "task:".len() + self.tracking_key().len() > 2048 => {
                return Err("singleton name exceeds storage key byte limit".into());
            }
            Self::Singleton { .. } => {}
        }
        Ok(())
    }
    pub fn name(&self) -> &str {
        match self {
            Self::Published { task_name, .. } | Self::Singleton { task_name } => task_name,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Published { .. } => "published",
            Self::Singleton { .. } => "singleton",
        }
    }
}
