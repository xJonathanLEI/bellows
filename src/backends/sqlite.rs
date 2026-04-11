//! SQLite task backend implementation.

use std::{
    collections::{HashMap, hash_map::Entry as HashMapEntry},
    error::Error as StdError,
    fmt::{Display, Formatter},
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rand::RngExt;
use sqlx::{
    Row,
    sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions},
};
use tokio::sync::{
    broadcast::{self, Sender as BroadcastSender},
    oneshot::Sender as OneshotSender,
};

use crate::backends::{
    BackendSignal, BackendSignalSubscription, ClaimTaskError, ClaimedTask, FinishTaskError,
    FinishedTask, NewTaskAvailableSignalPayload, PublishTaskError, PublishedTask, RenewTaskError,
    RenewedTaskLease, SubscribeError, SweepTasksError, SweptTask,
};
use crate::{AwaitableTask, Backend, PublishActivationStrategy, TaskDefinition};

const SIGNAL_CHANNEL_SIZE: usize = 1024;
const INITIALIZE_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS bellows_tasks (
    task_id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_name TEXT NOT NULL,
    task_unique_key TEXT,
    payload_json TEXT NOT NULL,
    callback_id INTEGER,
    lease_worker_id INTEGER,
    lease_expiration_unix_ms INTEGER,
    CHECK ((lease_worker_id IS NULL) = (lease_expiration_unix_ms IS NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS bellows_tasks_unique_key_idx
    ON bellows_tasks (task_unique_key);

CREATE INDEX IF NOT EXISTS bellows_tasks_sweep_idx
    ON bellows_tasks (task_name, lease_expiration_unix_ms, task_id);
"#;

#[derive(Debug)]
pub enum SqliteBackendError {
    Sqlx(sqlx::Error),
    InvalidTaskId(std::num::TryFromIntError),
    InvalidWorkerId(std::num::TryFromIntError),
    PayloadSerialization(serde_json::Error),
    PayloadDeserialization(serde_json::Error),
    CallbackSerialization(serde_json::Error),
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
            Self::CallbackSerialization(error) => {
                write!(f, "task callback serialization failed: {error}")
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
            Self::CallbackSerialization(error) => Some(error),
        }
    }
}

/// SQLite-backed task registry.
///
/// This backend stores tasks durably in SQLite via `sqlx`, but its signaling layer is in-process
/// only. That means dispatchers in other processes will not receive new task notifications until
/// they perform a startup sweep again. This makes the backend suitable for local development,
/// testing, and single-process deployments, but not for truly distributed systems.
#[derive(Clone)]
pub struct SqliteBackend {
    pool: SqlitePool,
    shared: Arc<Shared>,
}

struct Shared {
    signals: Mutex<HashMap<&'static str, BroadcastSender<BackendSignal>>>,
    callbacks: Mutex<HashMap<u64, Box<dyn CallbackSink>>>,
}

impl std::fmt::Debug for SqliteBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteBackend")
            .field("pool", &self.pool)
            .finish_non_exhaustive()
    }
}

impl SqliteBackend {
    /// Creates a [`SqliteBackend`] from an existing connection pool.
    ///
    /// This can be useful for applications already using `sqlx` that want to reuse the existing
    /// connection.
    pub fn new(pool: SqlitePool) -> Self {
        let shared = Arc::new(Shared {
            signals: Default::default(),
            callbacks: Default::default(),
        });

        Self { pool, shared }
    }

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

        Ok(Self::new(pool))
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

    fn signal_for_task(&self, task_name: &'static str) -> BroadcastSender<BackendSignal> {
        let mut signals = self
            .shared
            .signals
            .lock()
            .expect("sqlite backend signal registry mutex should not be poisoned");

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
            .expect("sqlite backend callback registry mutex should not be poisoned");
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
                .expect("sqlite backend callback registry mutex should not be poisoned")
                .remove(&callback_id);
        }
    }

    fn emit_signal(&self, task_name: &'static str, signal: BackendSignal) {
        if let Some(signal_tx) = self
            .shared
            .signals
            .lock()
            .expect("sqlite backend signal registry mutex should not be poisoned")
            .get(task_name)
            .cloned()
        {
            let _ = signal_tx.send(signal);
        }
    }

    fn deliver_callback(&self, callback_id: u64, callback_payload_json: String) {
        if let Some(callback_sink) = self
            .shared
            .callbacks
            .lock()
            .expect("sqlite backend callback registry mutex should not be poisoned")
            .remove(&callback_id)
        {
            callback_sink.send(callback_payload_json);
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
            PublishTaskError::Backend(Box::new(SqliteBackendError::PayloadSerialization(err)))
        })?;

        let result = sqlx::query(
            r#"
INSERT INTO bellows_tasks (task_name, task_unique_key, payload_json, callback_id)
VALUES (?, NULL, ?, ?)
"#,
        )
        .bind(T::NAME)
        .bind(payload_json)
        .bind(callback_id)
        .execute(&self.pool)
        .await;

        let result = match result {
            Ok(result) => result,
            Err(err) => {
                if let Some(callback_id) = callback_id {
                    self.drop_reserved_callback(callback_id);
                }
                return Err(PublishTaskError::Backend(Box::new(
                    SqliteBackendError::Sqlx(err),
                )));
            }
        };

        let task_id = u64::try_from(result.last_insert_rowid()).map_err(|err| {
            if let Some(callback_id) = callback_id {
                self.drop_reserved_callback(callback_id);
            }
            PublishTaskError::Backend(Box::new(SqliteBackendError::InvalidTaskId(err)))
        })?;

        self.emit_signal(
            T::NAME,
            BackendSignal::NewTaskAvailable(NewTaskAvailableSignalPayload { task_id }),
        );

        Ok(PublishedTask { task_id })
    }
}

impl Backend for SqliteBackend {
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
WHERE task_name = ?
  AND (
        lease_worker_id IS NULL
        OR lease_expiration_unix_ms IS NULL
        OR lease_expiration_unix_ms <= ?
      )
ORDER BY task_id
"#,
        )
        .bind(T::NAME)
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
  AND task_name = ?
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
        .bind(T::NAME)
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
  AND task_name = ?
"#,
                )
                .bind(task_id_db)
                .bind(T::NAME)
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

    async fn claim_singleton<T>(
        &self,
        worker_id: u64,
        lease_expiration: Instant,
    ) -> Result<ClaimedTask<()>, ClaimTaskError>
    where
        T: TaskDefinition,
    {
        let worker_id_db = i64::try_from(worker_id).map_err(|err| {
            ClaimTaskError::Backend(Box::new(SqliteBackendError::InvalidWorkerId(err)))
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
    lease_expiration_unix_ms
)
VALUES (?, ?, 'null', NULL, ?, ?)
ON CONFLICT(task_unique_key) DO UPDATE
SET lease_worker_id = excluded.lease_worker_id,
    lease_expiration_unix_ms = excluded.lease_expiration_unix_ms
WHERE bellows_tasks.task_name = excluded.task_name
  AND (
        bellows_tasks.lease_worker_id IS NULL
        OR bellows_tasks.lease_expiration_unix_ms IS NULL
        OR bellows_tasks.lease_expiration_unix_ms <= ?
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
        .map_err(|err| ClaimTaskError::Backend(Box::new(SqliteBackendError::Sqlx(err))))?;

        match claimed_row {
            Some(claimed_row) => {
                let task_id =
                    u64::try_from(claimed_row.get::<i64, _>("task_id")).map_err(|err| {
                        ClaimTaskError::Backend(Box::new(SqliteBackendError::InvalidTaskId(err)))
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
SELECT lease_expiration_unix_ms
FROM bellows_tasks
WHERE task_name = ?
  AND task_unique_key = ?
"#,
                )
                .bind(T::NAME)
                .bind(T::NAME)
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
            FinishTaskError::Backend(Box::new(SqliteBackendError::InvalidTaskId(err)))
        })?;
        let worker_id_db = i64::try_from(worker_id).map_err(|err| {
            FinishTaskError::Backend(Box::new(SqliteBackendError::InvalidWorkerId(err)))
        })?;
        let callback_payload_json = serde_json::to_string(&callback_payload).map_err(|err| {
            FinishTaskError::Backend(Box::new(SqliteBackendError::CallbackSerialization(err)))
        })?;

        let cleared_singleton_task = sqlx::query(
            r#"
UPDATE bellows_tasks
SET lease_worker_id = NULL,
    lease_expiration_unix_ms = NULL
WHERE task_id = ?
  AND lease_worker_id = ?
  AND task_unique_key IS NOT NULL
RETURNING task_name, callback_id
"#,
        )
        .bind(task_id_db)
        .bind(worker_id_db)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| FinishTaskError::Backend(Box::new(SqliteBackendError::Sqlx(err))))?;

        if let Some(cleared_singleton_task) = cleared_singleton_task {
            let task_name = cleared_singleton_task.get::<String, _>("task_name");
            let callback_id = cleared_singleton_task.get::<Option<i64>, _>("callback_id");

            let registered_task_name = self
                .shared
                .signals
                .lock()
                .expect("sqlite backend signal registry mutex should not be poisoned")
                .keys()
                .copied()
                .find(|registered_task_name| *registered_task_name == task_name);

            if let Some(task_name) = registered_task_name {
                if let Some(callback_id) = callback_id
                    && let Ok(callback_id) = u64::try_from(callback_id)
                {
                    self.deliver_callback(callback_id, callback_payload_json.clone());
                }

                self.emit_signal(
                    task_name,
                    BackendSignal::NewTaskAvailable(NewTaskAvailableSignalPayload { task_id }),
                );
            } else if let Some(callback_id) = callback_id
                && let Ok(callback_id) = u64::try_from(callback_id)
            {
                self.deliver_callback(callback_id, callback_payload_json);
            }

            return Ok(FinishedTask { task_id });
        }

        let deleted_published_task = sqlx::query(
            r#"
DELETE FROM bellows_tasks
WHERE task_id = ?
  AND lease_worker_id = ?
  AND task_unique_key IS NULL
RETURNING task_name, callback_id
"#,
        )
        .bind(task_id_db)
        .bind(worker_id_db)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| FinishTaskError::Backend(Box::new(SqliteBackendError::Sqlx(err))))?;

        let Some(deleted_published_task) = deleted_published_task else {
            return Err(FinishTaskError::LeaseLost);
        };

        let task_name = deleted_published_task.get::<String, _>("task_name");
        let callback_id = deleted_published_task.get::<Option<i64>, _>("callback_id");

        let registered_task_name = self
            .shared
            .signals
            .lock()
            .expect("sqlite backend signal registry mutex should not be poisoned")
            .keys()
            .copied()
            .find(|registered_task_name| *registered_task_name == task_name);

        if registered_task_name.is_some()
            && let Some(callback_id) = callback_id
            && let Ok(callback_id) = u64::try_from(callback_id)
        {
            self.deliver_callback(callback_id, callback_payload_json);
        } else if let Some(callback_id) = callback_id
            && let Ok(callback_id) = u64::try_from(callback_id)
        {
            self.deliver_callback(callback_id, callback_payload_json);
        }

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
