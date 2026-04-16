//! Postgres task backend implementation.

use std::{
    collections::{HashMap, hash_map::Entry as HashMapEntry},
    error::Error as StdError,
    fmt::{Display, Formatter},
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rand::RngExt;
use serde::{Deserialize, Serialize};
use sqlx::{
    Row,
    postgres::{PgListener, PgPool, PgPoolOptions},
};
use tokio::sync::{
    broadcast::{self, Sender as BroadcastSender},
    oneshot::Sender as OneshotSender,
    watch,
};
use tracing::warn;

use crate::backends::{
    Backend, BackendSignal, BackendSignalSubscription, ClaimTaskError, ClaimedTask, FailTaskError,
    FailedTask, FinishTaskError, FinishedTask, NewTaskAvailableSignalPayload, PublishTaskError,
    PublishedTask, RenewTaskError, RenewedTaskLease, SubscribeError,
};
use crate::{AwaitableTask, PublishActivationStrategy, TaskDefinition};

const SIGNAL_CHANNEL_SIZE: usize = 1024;
const NOTIFY_CHANNEL: &str = "bellows_tasks";
const LISTENER_RETRY_DELAY: Duration = Duration::from_secs(1);

const INITIALIZE_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS bellows_tasks (
    task_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_name TEXT NOT NULL,
    task_unique_key TEXT,
    payload_json TEXT NOT NULL,
    callback_id BIGINT,
    lease_worker_id BIGINT,
    available_from_unix_ms BIGINT,
    CHECK (lease_worker_id IS NULL OR available_from_unix_ms IS NOT NULL)
);

CREATE UNIQUE INDEX IF NOT EXISTS bellows_tasks_unique_key_idx
    ON bellows_tasks (task_unique_key);

CREATE INDEX IF NOT EXISTS bellows_tasks_available_idx
    ON bellows_tasks (task_name, task_unique_key, available_from_unix_ms, task_id);

CREATE OR REPLACE FUNCTION bellows_notify_task_available()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify(
        'bellows_tasks',
        json_build_object(
            'kind', 'new_task_available',
            'task_name', NEW.task_name,
            'task_id', NEW.task_id,
            'available_from_unix_ms', NEW.available_from_unix_ms
        )::text
    );

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS bellows_tasks_notify_available ON bellows_tasks;

CREATE TRIGGER bellows_tasks_notify_available
AFTER INSERT OR UPDATE OF lease_worker_id, available_from_unix_ms ON bellows_tasks
FOR EACH ROW
EXECUTE FUNCTION bellows_notify_task_available();
"#;

#[derive(Debug)]
pub enum PostgresBackendError {
    Sqlx(sqlx::Error),
    InvalidTaskId(std::num::TryFromIntError),
    InvalidWorkerId(std::num::TryFromIntError),
    PayloadSerialization(serde_json::Error),
    PayloadDeserialization(serde_json::Error),
    CallbackSerialization(serde_json::Error),
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
            Self::CallbackSerialization(error) => {
                write!(f, "task callback serialization failed: {error}")
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
            Self::CallbackSerialization(error) => Some(error),
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
#[derive(Clone)]
pub struct PostgresBackend {
    pool: PgPool,
    shared: Arc<Shared>,
}

struct Shared {
    signals: Mutex<HashMap<&'static str, BroadcastSender<BackendSignal>>>,
    callbacks: Mutex<HashMap<u64, Box<dyn CallbackSink>>>,
    shutdown_signal: watch::Sender<bool>,
}

impl Drop for Shared {
    fn drop(&mut self) {
        let _ = self.shutdown_signal.send(true);
    }
}

impl std::fmt::Debug for PostgresBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresBackend")
            .field("pool", &self.pool)
            .finish_non_exhaustive()
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
            callbacks: Default::default(),
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

    fn reserve_callback<T>(&self) -> (i64, tokio::sync::oneshot::Receiver<T>)
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();
        let mut callbacks = self
            .shared
            .callbacks
            .lock()
            .expect("postgres backend callback registry mutex should not be poisoned");
        let mut rng = rand::rng();

        let callback_id = loop {
            let callback_id = rng.random::<i64>();
            if callback_id >= 0
                && let HashMapEntry::Vacant(entry) = callbacks.entry(callback_id as u64)
            {
                entry.insert(Box::new(TypedCallbackSink { tx: callback_tx }));
                break callback_id;
            }
        };

        (callback_id, callback_rx)
    }

    fn drop_reserved_callback(&self, callback_id: i64) {
        if let Ok(callback_id) = u64::try_from(callback_id) {
            self.shared
                .callbacks
                .lock()
                .expect("postgres backend callback registry mutex should not be poisoned")
                .remove(&callback_id);
        }
    }

    async fn publish_impl<T>(
        &self,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
        callback_id: Option<i64>,
    ) -> Result<PublishedTask, PublishTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        let payload_json = serde_json::to_string(&payload).map_err(|err| {
            PublishTaskError::Backend(Box::new(PostgresBackendError::PayloadSerialization(err)))
        })?;

        let row = sqlx::query(
            r#"
INSERT INTO bellows_tasks (
    task_name,
    task_unique_key,
    payload_json,
    callback_id,
    lease_worker_id,
    available_from_unix_ms
)
VALUES ($1, NULL, $2, $3, NULL, NULL)
RETURNING task_id
"#,
        )
        .bind(T::NAME)
        .bind(payload_json)
        .bind(callback_id)
        .fetch_one(&self.pool)
        .await;

        let row = match row {
            Ok(row) => row,
            Err(err) => {
                if let Some(callback_id) = callback_id {
                    self.drop_reserved_callback(callback_id);
                }
                return Err(PublishTaskError::Backend(Box::new(
                    PostgresBackendError::Sqlx(err),
                )));
            }
        };

        let task_id = u64::try_from(row.get::<i64, _>("task_id")).map_err(|err| {
            if let Some(callback_id) = callback_id {
                self.drop_reserved_callback(callback_id);
            }
            PublishTaskError::Backend(Box::new(PostgresBackendError::InvalidTaskId(err)))
        })?;

        Ok(PublishedTask { task_id })
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

    async fn publish<T>(
        &self,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
    ) -> Result<PublishedTask, PublishTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        self.publish_impl::<T>(payload, None).await
    }

    async fn publish_awaitable<T>(
        &self,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
    ) -> Result<AwaitableTask<T::Callback>, PublishTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        let (callback_id, callback_rx) = self.reserve_callback::<T::Callback>();
        let published = self.publish_impl::<T>(payload, Some(callback_id)).await?;
        Ok(AwaitableTask::new(published.task_id, callback_rx))
    }

    async fn claim_published<T>(
        &self,
        worker_id: u64,
        task_id: u64,
        lease_expiration: Instant,
    ) -> Result<
        ClaimedTask<<<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload>,
        ClaimTaskError,
    >
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
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
SET lease_worker_id = $1,
    available_from_unix_ms = $2
WHERE task_id = $3
  AND task_name = $4
  AND task_unique_key IS NULL
  AND (
        available_from_unix_ms IS NULL
        OR available_from_unix_ms <= $5
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
SELECT lease_worker_id, available_from_unix_ms
FROM bellows_tasks
WHERE task_id = $1
  AND task_name = $2
  AND task_unique_key IS NULL
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

                match current.get::<Option<i64>, _>("available_from_unix_ms") {
                    Some(available_from_unix_ms) if available_from_unix_ms > now_unix_ms => {
                        if current.get::<Option<i64>, _>("lease_worker_id").is_some() {
                            Err(ClaimTaskError::TaskLeased {
                                expiration: unix_ms_to_instant(available_from_unix_ms, now_system),
                            })
                        } else {
                            Err(ClaimTaskError::TaskUnavailable {
                                available_from: Some(unix_ms_to_instant(
                                    available_from_unix_ms,
                                    now_system,
                                )),
                            })
                        }
                    }
                    Some(_) | None => Err(ClaimTaskError::TaskNotFound),
                }
            }
        }
    }

    async fn claim_earliest_published<T>(
        &self,
        worker_id: u64,
        lease_expiration: Instant,
    ) -> Result<
        ClaimedTask<<<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload>,
        ClaimTaskError,
    >
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        let worker_id_db = i64::try_from(worker_id).map_err(|err| {
            ClaimTaskError::Backend(Box::new(PostgresBackendError::InvalidWorkerId(err)))
        })?;
        let now_system = SystemTime::now();
        let now_unix_ms = unix_timestamp_ms(now_system);
        let lease_expiration_unix_ms = instant_to_unix_ms(lease_expiration, now_system);

        let claimed_row = sqlx::query(
            r#"
WITH next_task AS (
    SELECT task_id
    FROM bellows_tasks
    WHERE task_name = $1
      AND task_unique_key IS NULL
      AND (
            available_from_unix_ms IS NULL
            OR available_from_unix_ms <= $2
          )
    ORDER BY available_from_unix_ms NULLS FIRST, task_id
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE bellows_tasks
SET lease_worker_id = $3,
    available_from_unix_ms = $4
FROM next_task
WHERE bellows_tasks.task_id = next_task.task_id
RETURNING bellows_tasks.task_id, bellows_tasks.payload_json
"#,
        )
        .bind(T::NAME)
        .bind(now_unix_ms)
        .bind(worker_id_db)
        .bind(lease_expiration_unix_ms)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| ClaimTaskError::Backend(Box::new(PostgresBackendError::Sqlx(err))))?;

        match claimed_row {
            Some(claimed_row) => {
                let task_id =
                    u64::try_from(claimed_row.get::<i64, _>("task_id")).map_err(|err| {
                        ClaimTaskError::Backend(Box::new(PostgresBackendError::InvalidTaskId(err)))
                    })?;
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
                let earliest_available_from = sqlx::query(
                    r#"
SELECT MIN(available_from_unix_ms) AS available_from_unix_ms
FROM bellows_tasks
WHERE task_name = $1
  AND task_unique_key IS NULL
  AND available_from_unix_ms > $2
"#,
                )
                .bind(T::NAME)
                .bind(now_unix_ms)
                .fetch_one(&self.pool)
                .await
                .map_err(|err| ClaimTaskError::Backend(Box::new(PostgresBackendError::Sqlx(err))))?
                .get::<Option<i64>, _>("available_from_unix_ms")
                .map(|unix_ms| unix_ms_to_instant(unix_ms, now_system));

                Err(ClaimTaskError::TaskUnavailable {
                    available_from: earliest_available_from,
                })
            }
        }
    }

    async fn claim_singleton<T>(
        &self,
        worker_id: u64,
        lease_expiration: Instant,
    ) -> Result<ClaimedTask<()>, ClaimTaskError>
    where
        T: TaskDefinition,
    {
        let worker_id_db = i64::try_from(worker_id).map_err(|err| {
            ClaimTaskError::Backend(Box::new(PostgresBackendError::InvalidWorkerId(err)))
        })?;
        let now_system = SystemTime::now();
        let now_unix_ms = unix_timestamp_ms(now_system);
        let lease_expiration_unix_ms = instant_to_unix_ms(lease_expiration, now_system);

        let claimed_row = sqlx::query(
            r#"
INSERT INTO bellows_tasks (
    task_name,
    task_unique_key,
    payload_json,
    callback_id,
    lease_worker_id,
    available_from_unix_ms
)
VALUES ($1, $2, 'null', NULL, $3, $4)
ON CONFLICT (task_unique_key) DO UPDATE
SET lease_worker_id = EXCLUDED.lease_worker_id,
    available_from_unix_ms = EXCLUDED.available_from_unix_ms
WHERE bellows_tasks.task_name = EXCLUDED.task_name
  AND (
        bellows_tasks.available_from_unix_ms IS NULL
        OR bellows_tasks.available_from_unix_ms <= $5
      )
RETURNING task_id
"#,
        )
        .bind(T::NAME)
        .bind(T::NAME)
        .bind(worker_id_db)
        .bind(lease_expiration_unix_ms)
        .bind(now_unix_ms)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| ClaimTaskError::Backend(Box::new(PostgresBackendError::Sqlx(err))))?;

        match claimed_row {
            Some(claimed_row) => {
                let task_id =
                    u64::try_from(claimed_row.get::<i64, _>("task_id")).map_err(|err| {
                        ClaimTaskError::Backend(Box::new(PostgresBackendError::InvalidTaskId(err)))
                    })?;

                Ok(ClaimedTask {
                    task_id,
                    task_payload: (),
                    lease_expiration,
                })
            }
            None => {
                let current = sqlx::query(
                    r#"
SELECT lease_worker_id, available_from_unix_ms
FROM bellows_tasks
WHERE task_name = $1
  AND task_unique_key = $2
"#,
                )
                .bind(T::NAME)
                .bind(T::NAME)
                .fetch_optional(&self.pool)
                .await
                .map_err(|err| {
                    ClaimTaskError::Backend(Box::new(PostgresBackendError::Sqlx(err)))
                })?;

                let Some(current) = current else {
                    return Err(ClaimTaskError::TaskNotFound);
                };

                match current.get::<Option<i64>, _>("available_from_unix_ms") {
                    Some(available_from_unix_ms) if available_from_unix_ms > now_unix_ms => {
                        if current.get::<Option<i64>, _>("lease_worker_id").is_some() {
                            Err(ClaimTaskError::TaskLeased {
                                expiration: unix_ms_to_instant(available_from_unix_ms, now_system),
                            })
                        } else {
                            Err(ClaimTaskError::TaskUnavailable {
                                available_from: Some(unix_ms_to_instant(
                                    available_from_unix_ms,
                                    now_system,
                                )),
                            })
                        }
                    }
                    Some(_) | None => Err(ClaimTaskError::TaskNotFound),
                }
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
SET available_from_unix_ms = $1
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

    async fn fail(
        &self,
        worker_id: u64,
        task_id: u64,
        available_from: Option<Instant>,
    ) -> Result<FailedTask, FailTaskError> {
        let task_id_db = i64::try_from(task_id).map_err(|err| {
            FailTaskError::Backend(Box::new(PostgresBackendError::InvalidTaskId(err)))
        })?;
        let worker_id_db = i64::try_from(worker_id).map_err(|err| {
            FailTaskError::Backend(Box::new(PostgresBackendError::InvalidWorkerId(err)))
        })?;
        let available_from_unix_ms =
            available_from.map(|instant| instant_to_unix_ms(instant, SystemTime::now()));

        let result = sqlx::query(
            r#"
UPDATE bellows_tasks
SET lease_worker_id = NULL,
    available_from_unix_ms = $1
WHERE task_id = $2
  AND lease_worker_id = $3
"#,
        )
        .bind(available_from_unix_ms)
        .bind(task_id_db)
        .bind(worker_id_db)
        .execute(&self.pool)
        .await
        .map_err(|err| FailTaskError::Backend(Box::new(PostgresBackendError::Sqlx(err))))?;

        if result.rows_affected() == 0 {
            Err(FailTaskError::LeaseLost)
        } else {
            Ok(FailedTask { task_id })
        }
    }

    async fn finish<T>(
        &self,
        worker_id: u64,
        task_id: u64,
        callback_payload: T::Callback,
    ) -> Result<FinishedTask, FinishTaskError>
    where
        T: TaskDefinition,
    {
        let task_id_db = i64::try_from(task_id).map_err(|err| {
            FinishTaskError::Backend(Box::new(PostgresBackendError::InvalidTaskId(err)))
        })?;
        let worker_id_db = i64::try_from(worker_id).map_err(|err| {
            FinishTaskError::Backend(Box::new(PostgresBackendError::InvalidWorkerId(err)))
        })?;
        let callback_payload_json = serde_json::to_string(&callback_payload).map_err(|err| {
            FinishTaskError::Backend(Box::new(PostgresBackendError::CallbackSerialization(err)))
        })?;

        let mut tx =
            self.pool.begin().await.map_err(|err| {
                FinishTaskError::Backend(Box::new(PostgresBackendError::Sqlx(err)))
            })?;

        let singleton_row = sqlx::query(
            r#"
UPDATE bellows_tasks
SET lease_worker_id = NULL,
    available_from_unix_ms = NULL
WHERE task_id = $1
  AND lease_worker_id = $2
  AND task_unique_key IS NOT NULL
RETURNING task_name, callback_id
"#,
        )
        .bind(task_id_db)
        .bind(worker_id_db)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| FinishTaskError::Backend(Box::new(PostgresBackendError::Sqlx(err))))?;

        let finished_row = if let Some(row) = singleton_row {
            Some(row)
        } else {
            sqlx::query(
                r#"
DELETE FROM bellows_tasks
WHERE task_id = $1
  AND lease_worker_id = $2
  AND task_unique_key IS NULL
RETURNING task_name, callback_id
"#,
            )
            .bind(task_id_db)
            .bind(worker_id_db)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|err| FinishTaskError::Backend(Box::new(PostgresBackendError::Sqlx(err))))?
        };

        let Some(finished_row) = finished_row else {
            tx.rollback().await.ok();
            return Err(FinishTaskError::LeaseLost);
        };

        let task_name = finished_row.get::<String, _>("task_name");
        let callback_id = finished_row.get::<Option<i64>, _>("callback_id");

        if let Some(callback_id) = callback_id {
            let payload_json = serde_json::to_string(&NotificationPayload::TaskCallback {
                task_name,
                callback_id,
                callback_payload_json,
            })
            .expect("postgres callback notification payload should serialize");

            sqlx::query("SELECT pg_notify($1, $2)")
                .bind(NOTIFY_CHANNEL)
                .bind(payload_json)
                .execute(&mut *tx)
                .await
                .map_err(|err| {
                    FinishTaskError::Backend(Box::new(PostgresBackendError::Sqlx(err)))
                })?;
        }

        tx.commit()
            .await
            .map_err(|err| FinishTaskError::Backend(Box::new(PostgresBackendError::Sqlx(err))))?;

        Ok(FinishedTask { task_id })
    }
}

struct TypedCallbackSink<T> {
    tx: OneshotSender<T>,
}

trait CallbackSink: Send {
    fn send(self: Box<Self>, callback_payload_json: String);
}

impl<T> CallbackSink for TypedCallbackSink<T>
where
    T: serde::de::DeserializeOwned + Send + 'static,
{
    fn send(self: Box<Self>, callback_payload_json: String) {
        if let Ok(callback_payload) = serde_json::from_str(&callback_payload_json) {
            let _ = self.tx.send(callback_payload);
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
                        warn!("postgres notification listener failed and will restart: {}", error);
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

        let Some(shared) = self.shared.upgrade() else {
            return EventLoopResult::Exit;
        };

        match payload {
            NotificationPayload::NewTaskAvailable {
                task_name,
                task_id,
                available_from_unix_ms,
            } => {
                let Ok(task_id) = u64::try_from(task_id) else {
                    warn!(
                        "received postgres notification with out-of-range task ID: {}",
                        task_id
                    );
                    return EventLoopResult::Continue;
                };

                let sender = {
                    let signals = shared
                        .signals
                        .lock()
                        .expect("postgres backend signal registry mutex should not be poisoned");
                    signals.get(task_name.as_str()).cloned()
                };

                if let Some(sender) = sender {
                    let available_from = available_from_unix_ms
                        .map(|unix_ms| unix_ms_to_instant(unix_ms, SystemTime::now()))
                        .unwrap_or_else(Instant::now);
                    let _ = sender.send(BackendSignal::NewTaskAvailable(
                        NewTaskAvailableSignalPayload {
                            task_id: Some(task_id),
                            available_from,
                        },
                    ));
                }
            }
            NotificationPayload::TaskCallback {
                task_name: _,
                callback_id,
                callback_payload_json,
            } => {
                let Ok(callback_id) = u64::try_from(callback_id) else {
                    warn!(
                        "received postgres callback notification with out-of-range callback ID: {}",
                        callback_id
                    );
                    return EventLoopResult::Continue;
                };

                if let Some(callback_sink) = shared
                    .callbacks
                    .lock()
                    .expect("postgres backend callback registry mutex should not be poisoned")
                    .remove(&callback_id)
                {
                    callback_sink.send(callback_payload_json);
                }
            }
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

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum NotificationPayload {
    NewTaskAvailable {
        task_name: String,
        task_id: i64,
        available_from_unix_ms: Option<i64>,
    },
    TaskCallback {
        task_name: String,
        callback_id: i64,
        callback_payload_json: String,
    },
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
