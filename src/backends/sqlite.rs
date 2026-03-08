//! SQLite task backend implementation.

use std::{
    error::Error as StdError,
    fmt::{Display, Formatter},
    str::FromStr,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use sqlx::{
    Row,
    sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions},
};
use tokio::sync::broadcast::{self, Sender as BroadcastSender};

use crate::backends::{
    BackendSignal, BackendSignalSubscription, ClaimTaskError, ClaimedTask, FinishTaskError,
    FinishedTask, NewTaskAvailableSignalPayload, PublishTaskError, PublishedTask, RenewTaskError,
    RenewedTaskLease, SubscribeError, SweepTasksError, SweptTask,
};
use crate::{Backend, TaskDefinition};

const SIGNAL_CHANNEL_SIZE: usize = 1024;
const INITIALIZE_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS bellows_tasks (
    task_id INTEGER PRIMARY KEY AUTOINCREMENT,
    payload_json TEXT NOT NULL,
    lease_worker_id INTEGER,
    lease_expiration_unix_ms INTEGER,
    CHECK ((lease_worker_id IS NULL) = (lease_expiration_unix_ms IS NULL))
);

CREATE INDEX IF NOT EXISTS bellows_tasks_sweep_idx
    ON bellows_tasks (lease_expiration_unix_ms, task_id);
"#;

#[derive(Debug)]
pub enum SqliteBackendError {
    Sqlx(sqlx::Error),
    InvalidTaskId(std::num::TryFromIntError),
    InvalidWorkerId(std::num::TryFromIntError),
    PayloadSerialization(serde_json::Error),
    PayloadDeserialization(serde_json::Error),
}

impl Display for SqliteBackendError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqlx(error) => write!(f, "sqlite operation failed: {error}"),
            Self::InvalidTaskId(error) => {
                write!(f, "task ID could not be represented in SQLite: {error}")
            }
            Self::InvalidWorkerId(error) => {
                write!(f, "worker ID could not be represented in SQLite: {error}")
            }
            Self::PayloadSerialization(error) => {
                write!(f, "task payload serialization failed: {error}")
            }
            Self::PayloadDeserialization(error) => {
                write!(f, "task payload deserialization failed: {error}")
            }
        }
    }
}

impl StdError for SqliteBackendError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Sqlx(error) => Some(error),
            Self::InvalidTaskId(error) => Some(error),
            Self::InvalidWorkerId(error) => Some(error),
            Self::PayloadSerialization(error) => Some(error),
            Self::PayloadDeserialization(error) => Some(error),
        }
    }
}

/// SQLite-backed task registry.
///
/// This backend stores tasks durably in SQLite via `sqlx`, but its signaling layer is in-process
/// only. That means dispatchers in other processes will not receive new task notifications until
/// they perform a startup sweep again. This makes the backend suitable for local development,
/// testing, and single-process deployments, but not for truly distributed systems.
#[derive(Clone, Debug)]
pub struct SqliteBackend {
    pool: SqlitePool,
    signal: BroadcastSender<BackendSignal>,
}

impl SqliteBackend {
    /// Connects to a SQLite database URL.
    ///
    /// This only establishes the connection pool. Call [`Self::initialize`] separately if you want
    /// the backend to create its schema automatically.
    pub async fn connect(database_url: &str) -> Result<Self, sqlx::Error> {
        let options = SqliteConnectOptions::from_str(database_url)?
            .create_if_missing(true)
            .busy_timeout(Duration::from_secs(5));

        Self::connect_with(options).await
    }

    /// Connects using preconfigured SQLite connection options.
    ///
    /// This only establishes the connection pool. Call [`Self::initialize`] separately if you want
    /// the backend to create its schema automatically.
    pub async fn connect_with(options: SqliteConnectOptions) -> Result<Self, sqlx::Error> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;

        let (signal, _) = broadcast::channel(SIGNAL_CHANNEL_SIZE);

        Ok(Self { pool, signal })
    }

    /// Initializes the SQLite schema required by the backend.
    ///
    /// This operation is idempotent and can be safely called multiple times.
    pub async fn initialize(&self) -> Result<(), sqlx::Error> {
        sqlx::raw_sql(INITIALIZE_SCHEMA_SQL)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

impl Backend for SqliteBackend {
    async fn subscribe(&self) -> Result<BackendSignalSubscription, SubscribeError> {
        Ok(BackendSignalSubscription::new(self.signal.subscribe()))
    }

    async fn sweep(&self) -> Result<Vec<SweptTask>, SweepTasksError> {
        let now_unix_ms = unix_timestamp_ms(SystemTime::now());
        let rows = sqlx::query(
            r#"
SELECT task_id
FROM bellows_tasks
WHERE lease_worker_id IS NULL
   OR lease_expiration_unix_ms IS NULL
   OR lease_expiration_unix_ms <= ?
ORDER BY task_id
"#,
        )
        .bind(now_unix_ms)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| SweepTasksError::Backend(Box::new(SqliteBackendError::Sqlx(err))))?;

        rows.into_iter()
            .map(|row| {
                let task_id = row.get::<i64, _>("task_id");
                let task_id = u64::try_from(task_id).map_err(|err| {
                    SweepTasksError::Backend(Box::new(SqliteBackendError::InvalidTaskId(err)))
                })?;
                Ok(SweptTask { task_id })
            })
            .collect()
    }

    async fn publish<T>(&self, payload: T::Payload) -> Result<PublishedTask, PublishTaskError>
    where
        T: TaskDefinition,
    {
        let payload_json = serde_json::to_string(&payload).map_err(|err| {
            PublishTaskError::Backend(Box::new(SqliteBackendError::PayloadSerialization(err)))
        })?;

        let result = sqlx::query(
            r#"
INSERT INTO bellows_tasks (payload_json)
VALUES (?)
"#,
        )
        .bind(payload_json)
        .execute(&self.pool)
        .await
        .map_err(|err| PublishTaskError::Backend(Box::new(SqliteBackendError::Sqlx(err))))?;

        let task_id = u64::try_from(result.last_insert_rowid()).map_err(|err| {
            PublishTaskError::Backend(Box::new(SqliteBackendError::InvalidTaskId(err)))
        })?;

        let _ = self.signal.send(BackendSignal::NewTaskAvailable(
            NewTaskAvailableSignalPayload { task_id },
        ));

        Ok(PublishedTask { task_id })
    }

    async fn claim<T>(
        &self,
        worker_id: u64,
        task_id: u64,
        lease_expiration: Instant,
    ) -> Result<ClaimedTask<T::Payload>, ClaimTaskError>
    where
        T: TaskDefinition,
    {
        let task_id_db = i64::try_from(task_id).map_err(|err| {
            ClaimTaskError::Backend(Box::new(SqliteBackendError::InvalidTaskId(err)))
        })?;
        let worker_id_db = i64::try_from(worker_id).map_err(|err| {
            ClaimTaskError::Backend(Box::new(SqliteBackendError::InvalidWorkerId(err)))
        })?;
        let now_system = SystemTime::now();
        let now_unix_ms = unix_timestamp_ms(now_system);
        let lease_expiration_unix_ms = instant_to_unix_ms(lease_expiration, now_system);

        let claimed_row = sqlx::query(
            r#"
UPDATE bellows_tasks
SET lease_worker_id = ?, lease_expiration_unix_ms = ?
WHERE task_id = ?
  AND (
        lease_worker_id IS NULL
        OR lease_expiration_unix_ms IS NULL
        OR lease_expiration_unix_ms <= ?
      )
RETURNING payload_json
"#,
        )
        .bind(worker_id_db)
        .bind(lease_expiration_unix_ms)
        .bind(task_id_db)
        .bind(now_unix_ms)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| ClaimTaskError::Backend(Box::new(SqliteBackendError::Sqlx(err))))?;

        match claimed_row {
            Some(claimed_row) => {
                let payload_json = claimed_row.get::<String, _>("payload_json");
                let task_payload = serde_json::from_str(&payload_json).map_err(|err| {
                    ClaimTaskError::Backend(Box::new(SqliteBackendError::PayloadDeserialization(
                        err,
                    )))
                })?;

                Ok(ClaimedTask {
                    task_id,
                    task_payload,
                    lease_expiration,
                })
            }
            None => {
                let current = sqlx::query(
                    r#"
SELECT lease_expiration_unix_ms
FROM bellows_tasks
WHERE task_id = ?
"#,
                )
                .bind(task_id_db)
                .fetch_optional(&self.pool)
                .await
                .map_err(|err| ClaimTaskError::Backend(Box::new(SqliteBackendError::Sqlx(err))))?;

                let Some(current) = current else {
                    return Err(ClaimTaskError::TaskNotFound);
                };

                if let Some(current_expiration_unix_ms) =
                    current.get::<Option<i64>, _>("lease_expiration_unix_ms")
                    && current_expiration_unix_ms > now_unix_ms
                {
                    return Err(ClaimTaskError::TaskLeased {
                        expiration: unix_ms_to_instant(current_expiration_unix_ms, now_system),
                    });
                }

                Err(ClaimTaskError::TaskNotFound)
            }
        }
    }

    async fn renew(
        &self,
        worker_id: u64,
        task_id: u64,
        lease_expiration: Instant,
    ) -> Result<RenewedTaskLease, RenewTaskError> {
        let task_id_db = i64::try_from(task_id).map_err(|err| {
            RenewTaskError::Backend(Box::new(SqliteBackendError::InvalidTaskId(err)))
        })?;
        let worker_id_db = i64::try_from(worker_id).map_err(|err| {
            RenewTaskError::Backend(Box::new(SqliteBackendError::InvalidWorkerId(err)))
        })?;
        let lease_expiration_unix_ms = instant_to_unix_ms(lease_expiration, SystemTime::now());

        let result = sqlx::query(
            r#"
UPDATE bellows_tasks
SET lease_expiration_unix_ms = ?
WHERE task_id = ?
  AND lease_worker_id = ?
"#,
        )
        .bind(lease_expiration_unix_ms)
        .bind(task_id_db)
        .bind(worker_id_db)
        .execute(&self.pool)
        .await
        .map_err(|err| RenewTaskError::Backend(Box::new(SqliteBackendError::Sqlx(err))))?;

        if result.rows_affected() == 0 {
            Err(RenewTaskError::LeaseLost)
        } else {
            Ok(RenewedTaskLease {
                new_expiration: lease_expiration,
            })
        }
    }

    async fn finish(&self, worker_id: u64, task_id: u64) -> Result<FinishedTask, FinishTaskError> {
        let task_id_db = i64::try_from(task_id).map_err(|err| {
            FinishTaskError::Backend(Box::new(SqliteBackendError::InvalidTaskId(err)))
        })?;
        let worker_id_db = i64::try_from(worker_id).map_err(|err| {
            FinishTaskError::Backend(Box::new(SqliteBackendError::InvalidWorkerId(err)))
        })?;

        let result = sqlx::query(
            r#"
DELETE FROM bellows_tasks
WHERE task_id = ?
  AND lease_worker_id = ?
"#,
        )
        .bind(task_id_db)
        .bind(worker_id_db)
        .execute(&self.pool)
        .await
        .map_err(|err| FinishTaskError::Backend(Box::new(SqliteBackendError::Sqlx(err))))?;

        if result.rows_affected() == 0 {
            Err(FinishTaskError::LeaseLost)
        } else {
            Ok(FinishedTask { task_id })
        }
    }
}

fn unix_timestamp_ms(time: SystemTime) -> i64 {
    let duration = time.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);

    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn instant_to_unix_ms(instant: Instant, now_system: SystemTime) -> i64 {
    let now_instant = Instant::now();
    let system_deadline = if instant >= now_instant {
        now_system + instant.duration_since(now_instant)
    } else {
        now_system
            .checked_sub(now_instant.duration_since(instant))
            .unwrap_or(UNIX_EPOCH)
    };

    unix_timestamp_ms(system_deadline)
}

fn unix_ms_to_instant(unix_ms: i64, now_system: SystemTime) -> Instant {
    let now_instant = Instant::now();
    let now_unix_ms = unix_timestamp_ms(now_system);

    if unix_ms <= now_unix_ms {
        now_instant
    } else {
        let delta_ms = u64::try_from(unix_ms - now_unix_ms).unwrap_or(u64::MAX);
        now_instant + Duration::from_millis(delta_ms)
    }
}
