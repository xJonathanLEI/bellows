use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{BoxDispatchError, TaskIdentity};

pub(super) const HEARTBEAT_INTERVAL_MS: i64 = 30_000;
pub(super) const ATTEMPT_WATCHDOG_MS: i64 = 60_000;
pub(super) const MAX_DATE_MS: i64 = 8_640_000_000_000_000;
const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

/// Durable scheduler metadata, stored separately from per-task records.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SchedulerMetadata {
    pub next_heartbeat_at_ms: i64,
    pub next_attempt_id: u64,
}

/// A pending hint or a running attempt's finite watchdog.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum TaskSchedule {
    Pending,
    Running {
        #[serde(rename = "attemptId")]
        attempt_id: u64,
    },
}

/// One scheduled task. `next_attempt_at_ms` is a hint, never permission to bypass a DB claim.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DispatcherTask {
    pub task: TaskIdentity,
    pub next_attempt_at_ms: i64,
    pub infrastructure_failures: u32,
    pub state: TaskSchedule,
}

/// Transaction-local schedule. Adapters persist metadata and individual changed records,
/// not this whole snapshot as a single storage value.
#[derive(Clone, Debug, Default)]
pub struct DispatcherState {
    pub metadata: Option<SchedulerMetadata>,
    /// Keyed by `TaskIdentity::tracking_key()`, excluding the storage prefix.
    pub tasks: BTreeMap<String, DispatcherTask>,
    /// Current platform alarm, used only when initializing scheduler metadata.
    pub alarm: Option<i64>,
}

impl DispatcherState {
    pub fn validate(&mut self, now: i64) -> Result<(), BoxDispatchError> {
        if self.metadata.is_none() {
            if !self.tasks.is_empty() {
                return Err("missing dispatcher metadata".into());
            }
            self.metadata = Some(SchedulerMetadata {
                next_heartbeat_at_ms: self
                    .alarm
                    .filter(|at| *at > now)
                    .unwrap_or(now + HEARTBEAT_INTERVAL_MS)
                    .min(now + HEARTBEAT_INTERVAL_MS),
                next_attempt_id: 0,
            });
        }
        let metadata = self.metadata.as_ref().unwrap();
        if !timestamp(metadata.next_heartbeat_at_ms) || metadata.next_attempt_id > MAX_SAFE_INTEGER
        {
            return Err("invalid dispatcher metadata".into());
        }
        for (id, task) in &self.tasks {
            task.task.validate_dispatch()?;
            if id != &task.task.tracking_key()
                || !timestamp(task.next_attempt_at_ms)
                || task.infrastructure_failures > 6
                || matches!(task.state, TaskSchedule::Running { attempt_id } if attempt_id >= metadata.next_attempt_id)
            {
                return Err("invalid dispatcher task record".into());
            }
        }
        Ok(())
    }

    pub fn alarm_at_ms(&self) -> i64 {
        self.tasks.values().fold(
            self.metadata.as_ref().unwrap().next_heartbeat_at_ms,
            |alarm, task| alarm.min(task.next_attempt_at_ms),
        )
    }

    pub(super) fn start(&mut self, id: &str, now: i64) -> Result<DispatcherTask, BoxDispatchError> {
        let metadata = self.metadata.as_mut().unwrap();
        if metadata.next_attempt_id >= MAX_SAFE_INTEGER {
            return Err("dispatcher attempt identifiers exhausted".into());
        }
        let task = self.tasks.get_mut(id).unwrap();
        task.state = TaskSchedule::Running {
            attempt_id: metadata.next_attempt_id,
        };
        metadata.next_attempt_id += 1;
        task.next_attempt_at_ms = now + ATTEMPT_WATCHDOG_MS;
        Ok(task.clone())
    }
}

impl DispatcherTask {
    pub(super) fn retry(&mut self, now: i64) {
        self.infrastructure_failures = (self.infrastructure_failures + 1).min(6);
        self.state = TaskSchedule::Pending;
        self.next_attempt_at_ms =
            now + (1_000_i64 << (self.infrastructure_failures - 1)).min(30_000);
    }

    pub(super) fn attempt_id(&self) -> Option<u64> {
        match self.state {
            TaskSchedule::Running { attempt_id } => Some(attempt_id),
            TaskSchedule::Pending => None,
        }
    }
}

pub(super) fn timestamp(value: i64) -> bool {
    (0..=MAX_DATE_MS).contains(&value)
}

/// Synchronous bookkeeping applied to a transaction-local durable snapshot.
pub type ScheduleUpdate<T> =
    Box<dyn FnOnce(&mut DispatcherState) -> Result<T, BoxDispatchError> + Send>;

/// Transactional storage for the shared alarm and per-task schedule.
///
/// The callback contains only synchronous bookkeeping. Implementations must atomically commit
/// its changed records, metadata, and `state.alarm_at_ms()`. Failed transactions must not commit
/// partial changes; a lost commit acknowledgement may leave the entire transition committed.
/// Each transaction reads current durable state, including after an uncertain commit.
pub trait DispatcherStorage: Send + Sync + 'static {
    fn get_alarm(&self) -> impl Future<Output = Result<Option<i64>, BoxDispatchError>> + Send;
    fn set_alarm(&self, at_ms: i64) -> impl Future<Output = Result<(), BoxDispatchError>> + Send;
    fn contains_task(
        &self,
        key: &str,
    ) -> impl Future<Output = Result<bool, BoxDispatchError>> + Send;

    fn transaction<T>(
        &self,
        update: ScheduleUpdate<T>,
    ) -> impl Future<Output = Result<T, BoxDispatchError>> + Send
    where
        T: Send + 'static;
}
