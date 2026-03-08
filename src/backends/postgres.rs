//! Postgres task backend implementation.

use std::{
    collections::HashMap,
    error::Error as StdError,
    fmt::{Display, Formatter},
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::Deserialize;
use sqlx::{
    Row,
    postgres::{PgListener, PgPool, PgPoolOptions},
};
use tokio::sync::{
    broadcast::{self, Sender as BroadcastSender},
    watch,
};
use tracing::warn;

use crate::backends::{
    BackendSignal, BackendSignalSubscription, ClaimTaskError, ClaimedTask, FinishTaskError,
    FinishedTask, NewTaskAvailableSignalPayload, PublishTaskError, PublishedTask, RenewTaskError,
    RenewedTaskLease, SubscribeError, SweepTasksError, SweptTask,
};
use crate::{Backend, TaskDefinition};

const SIGNAL_CHANNEL_SIZE: usize = 1024;
const NOTIFY_CHANNEL: &str = "bellows_tasks";
const LISTENER_RETRY_DELAY: Duration = Duration::from_secs(1);

const INITIALIZE_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS bellows_tasks (
    task_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_name TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    lease_worker_id BIGINT,
    lease_expiration_unix_ms BIGINT,
    CHECK ((lease_worker_id IS NULL) = (lease_expiration_unix_ms IS NULL))
);

CREATE INDEX IF NOT EXISTS bellows_tasks_sweep_idx
    ON bellows_tasks (task_name, lease_expiration_unix_ms, task_id);

CREATE OR REPLACE FUNCTION bellows_notify_new_task()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify(
        'bellows_tasks',
        json_build_object(
            'task_name', NEW.task_name,
            'task_id', NEW.task_id
        )::text
    );

    RETURN NEW;
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_trigger
        WHERE tgname = 'bellows_tasks_notify_insert'
          AND tgrelid = 'bellows_tasks'::regclass
          AND NOT tgisinternal
    ) THEN
        CREATE TRIGGER bellows_tasks_notify_insert
        AFTER INSERT ON bellows_tasks
        FOR EACH ROW
        EXECUTE FUNCTION bellows_notify_new_task();
    END IF;
END;
$$;
"#;

#[derive(Debug)]
pub enum PostgresBackendError {
    Sqlx(sqlx::Error),
    InvalidTaskId(std::num::TryFromIntError),
    InvalidWorkerId(std::num::TryFromIntError),
    PayloadSerialization(serde_json::Error),
    PayloadDeserialization(serde_json::Error),
}

impl Display for PostgresBackendError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqlx(error) => write!(f, "postgres operation failed: {error}"),
            Self::InvalidTaskId(error) => {
                write!(f, "task ID could not be represented in Postgres: {error}")
            }
            Self::InvalidWorkerId(error) => {
                write!(f, "worker ID could not be represented in Postgres: {error}")
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

impl StdError for PostgresBackendError {
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

/// Postgres-backed task registry with native `LISTEN`/`NOTIFY` signaling.
///
/// This type can be cheaply cloned.
///
/// Unlike the SQLite backend, task availability signals are emitted directly by Postgres via a
/// trigger on the task table, making this backend suitable for multi-process and distributed
/// deployments as long as all participants can reach the same database.
#[derive(Clone, Debug)]
pub struct PostgresBackend {
    pool: PgPool,
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    signals: Mutex<HashMap<&'static str, BroadcastSender<BackendSignal>>>,
    shutdown_signal: watch::Sender<bool>,
}

impl Drop for Shared {
    fn drop(&mut self) {
        let _ = self.shutdown_signal.send(true);
    }
}

impl PostgresBackend {
    /// Connects to a Postgres database URL.
    ///
    /// This only establishes the connection pool and starts the background notification listener.
    /// Call [`Self::initialize`] separately if you want the backend to create its schema and
    /// triggers automatically.
    pub async fn connect(database_url: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new().connect(database_url).await?;
        let listener = connect_listener(database_url).await?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let shared = Arc::new(Shared {
            signals: Default::default(),
            shutdown_signal: shutdown_tx,
        });

        let daemon = Daemon {
            database_url: database_url.to_owned(),
            listener: Some(listener),
            shared: Arc::downgrade(&shared),
            shutdown_signal: shutdown_rx,
        };

        tokio::spawn(daemon.run());

        Ok(Self { pool, shared })
    }

    /// Initializes the Postgres schema required by the backend.
    ///
    /// This operation is idempotent and can be safely called multiple times.
    pub async fn initialize(&self) -> Result<(), sqlx::Error> {
        sqlx::raw_sql(INITIALIZE_SCHEMA_SQL)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    fn signal_for_task(&self, task_name: &'static str) -> BroadcastSender<BackendSignal> {
        let mut signals = self
            .shared
            .signals
            .lock()
            .expect("postgres backend signal registry mutex should not be poisoned");

        signals
            .entry(task_name)
            .or_insert_with(|| broadcast::channel(SIGNAL_CHANNEL_SIZE).0)
            .clone()
    }
}

impl Backend for PostgresBackend {
    async fn subscribe<T>(&self) -> Result<BackendSignalSubscription<T>, SubscribeError>
    where
        T: TaskDefinition,
    {
        Ok(BackendSignalSubscription::new(
            self.signal_for_task(T::NAME).subscribe(),
        ))
    }

    async fn sweep<T>(&self) -> Result<Vec<SweptTask>, SweepTasksError>
    where
        T: TaskDefinition,
    {
        let now_unix_ms = unix_timestamp_ms(SystemTime::now());
        let rows = sqlx::query(
            r#"
SELECT task_id
FROM bellows_tasks
WHERE task_name = $1
  AND (
        lease_worker_id IS NULL
        OR lease_expiration_unix_ms IS NULL
        OR lease_expiration_unix_ms <= $2
      )
ORDER BY task_id
"#,
        )
        .bind(T::NAME)
        .bind(now_unix_ms)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| SweepTasksError::Backend(Box::new(PostgresBackendError::Sqlx(err))))?;

        rows.into_iter()
            .map(|row| {
                let task_id = row.get::<i64, _>("task_id");
                let task_id = u64::try_from(task_id).map_err(|err| {
                    SweepTasksError::Backend(Box::new(PostgresBackendError::InvalidTaskId(err)))
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
            PublishTaskError::Backend(Box::new(PostgresBackendError::PayloadSerialization(err)))
        })?;

        let row = sqlx::query(
            r#"
INSERT INTO bellows_tasks (task_name, payload_json)
VALUES ($1, $2)
RETURNING task_id
"#,
        )
        .bind(T::NAME)
        .bind(payload_json)
        .fetch_one(&self.pool)
        .await
        .map_err(|err| PublishTaskError::Backend(Box::new(PostgresBackendError::Sqlx(err))))?;

        let task_id = u64::try_from(row.get::<i64, _>("task_id")).map_err(|err| {
            PublishTaskError::Backend(Box::new(PostgresBackendError::InvalidTaskId(err)))
        })?;

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
            ClaimTaskError::Backend(Box::new(PostgresBackendError::InvalidTaskId(err)))
        })?;
        let worker_id_db = i64::try_from(worker_id).map_err(|err| {
            ClaimTaskError::Backend(Box::new(PostgresBackendError::InvalidWorkerId(err)))
        })?;
        let now_system = SystemTime::now();
        let now_unix_ms = unix_timestamp_ms(now_system);
        let lease_expiration_unix_ms = instant_to_unix_ms(lease_expiration, now_system);

        let claimed_row = sqlx::query(
            r#"
UPDATE bellows_tasks
SET lease_worker_id = $1, lease_expiration_unix_ms = $2
WHERE task_id = $3
  AND task_name = $4
  AND (
        lease_worker_id IS NULL
        OR lease_expiration_unix_ms IS NULL
        OR lease_expiration_unix_ms <= $5
      )
RETURNING payload_json
"#,
        )
        .bind(worker_id_db)
        .bind(lease_expiration_unix_ms)
        .bind(task_id_db)
        .bind(T::NAME)
        .bind(now_unix_ms)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| ClaimTaskError::Backend(Box::new(PostgresBackendError::Sqlx(err))))?;

        match claimed_row {
            Some(claimed_row) => {
                let payload_json = claimed_row.get::<String, _>("payload_json");
                let task_payload = serde_json::from_str(&payload_json).map_err(|err| {
                    ClaimTaskError::Backend(Box::new(PostgresBackendError::PayloadDeserialization(
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
WHERE task_id = $1
  AND task_name = $2
"#,
                )
                .bind(task_id_db)
                .bind(T::NAME)
                .fetch_optional(&self.pool)
                .await
                .map_err(|err| {
                    ClaimTaskError::Backend(Box::new(PostgresBackendError::Sqlx(err)))
                })?;

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
            RenewTaskError::Backend(Box::new(PostgresBackendError::InvalidTaskId(err)))
        })?;
        let worker_id_db = i64::try_from(worker_id).map_err(|err| {
            RenewTaskError::Backend(Box::new(PostgresBackendError::InvalidWorkerId(err)))
        })?;
        let lease_expiration_unix_ms = instant_to_unix_ms(lease_expiration, SystemTime::now());

        let result = sqlx::query(
            r#"
UPDATE bellows_tasks
SET lease_expiration_unix_ms = $1
WHERE task_id = $2
  AND lease_worker_id = $3
"#,
        )
        .bind(lease_expiration_unix_ms)
        .bind(task_id_db)
        .bind(worker_id_db)
        .execute(&self.pool)
        .await
        .map_err(|err| RenewTaskError::Backend(Box::new(PostgresBackendError::Sqlx(err))))?;

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
            FinishTaskError::Backend(Box::new(PostgresBackendError::InvalidTaskId(err)))
        })?;
        let worker_id_db = i64::try_from(worker_id).map_err(|err| {
            FinishTaskError::Backend(Box::new(PostgresBackendError::InvalidWorkerId(err)))
        })?;

        let result = sqlx::query(
            r#"
DELETE FROM bellows_tasks
WHERE task_id = $1
  AND lease_worker_id = $2
"#,
        )
        .bind(task_id_db)
        .bind(worker_id_db)
        .execute(&self.pool)
        .await
        .map_err(|err| FinishTaskError::Backend(Box::new(PostgresBackendError::Sqlx(err))))?;

        if result.rows_affected() == 0 {
            Err(FinishTaskError::LeaseLost)
        } else {
            Ok(FinishedTask { task_id })
        }
    }
}

#[derive(Debug)]
struct Daemon {
    database_url: String,
    listener: Option<PgListener>,
    shared: Weak<Shared>,
    shutdown_signal: watch::Receiver<bool>,
}

impl Daemon {
    async fn run(mut self) {
        while let EventLoopResult::Continue = self.event_loop().await {}
    }

    async fn event_loop(&mut self) -> EventLoopResult {
        if self.shared.strong_count() == 0 {
            return EventLoopResult::Exit;
        }

        if self.listener.is_none() {
            match connect_listener(&self.database_url).await {
                Ok(listener) => self.listener = Some(listener),
                Err(error) => {
                    warn!(
                        "postgres notification listener failed to reconnect: {}",
                        error
                    );
                    return self.wait_for_retry().await;
                }
            }
        }

        let listener = self
            .listener
            .as_mut()
            .expect("postgres listener should exist before waiting for notifications");

        tokio::select! {
            changed = self.shutdown_signal.changed() => {
                let _ = changed;
                EventLoopResult::Exit
            }
            notification = listener.recv() => {
                match notification {
                    Ok(notification) => self.handle_notification(notification.payload()),
                    Err(error) => {
                        warn!(
                            "postgres notification listener failed and will restart: {}",
                            error
                        );
                        self.listener = None;
                        self.wait_for_retry().await
                    }
                }
            }
        }
    }

    fn handle_notification(&self, payload_json: &str) -> EventLoopResult {
        let payload = match serde_json::from_str::<NotificationPayload>(payload_json) {
            Ok(payload) => payload,
            Err(error) => {
                warn!(
                    "failed to deserialize postgres notification payload {:?}: {}",
                    payload_json, error
                );
                return EventLoopResult::Continue;
            }
        };

        let Ok(task_id) = u64::try_from(payload.task_id) else {
            warn!(
                "received postgres notification with out-of-range task ID: {}",
                payload.task_id
            );
            return EventLoopResult::Continue;
        };

        let Some(shared) = self.shared.upgrade() else {
            return EventLoopResult::Exit;
        };

        let sender = {
            let signals = shared
                .signals
                .lock()
                .expect("postgres backend signal registry mutex should not be poisoned");
            signals.get(payload.task_name.as_str()).cloned()
        };

        if let Some(sender) = sender {
            let _ = sender.send(BackendSignal::NewTaskAvailable(
                NewTaskAvailableSignalPayload { task_id },
            ));
        }

        EventLoopResult::Continue
    }

    async fn wait_for_retry(&mut self) -> EventLoopResult {
        tokio::select! {
            changed = self.shutdown_signal.changed() => {
                let _ = changed;
                EventLoopResult::Exit
            }
            _ = tokio::time::sleep(LISTENER_RETRY_DELAY) => {
                if self.shared.strong_count() == 0 {
                    EventLoopResult::Exit
                } else {
                    EventLoopResult::Continue
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EventLoopResult {
    Continue,
    Exit,
}

async fn connect_listener(database_url: &str) -> Result<PgListener, sqlx::Error> {
    let mut listener = PgListener::connect(database_url).await?;
    listener.listen(NOTIFY_CHANNEL).await?;
    Ok(listener)
}

#[derive(Debug, Deserialize)]
struct NotificationPayload {
    task_name: String,
    task_id: i64,
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
