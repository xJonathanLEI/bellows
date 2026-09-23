//! Shared PostgreSQL task operations, independent of notification listeners.

use std::{
    error::Error as StdError,
    fmt::{Display, Formatter},
    sync::Arc,
    time::{Instant, SystemTime},
};

use sqlx::{
    Connection, PgConnection, Postgres, Row, Transaction,
    postgres::{PgPool, PgPoolOptions},
};

use crate::backends::{
    BoxBackendError, ClaimTaskError, ClaimedTask, FailTaskError, FailedTask, FinishTaskError,
    FinishedTask, PublishTaskError, PublishedTask, RenewTaskError, RenewedTaskLease,
};
use crate::{PublishActivationStrategy, TaskDefinition};

use super::postgres_common::{
    NOTIFY_CHANNEL, NOTIFY_SQL, NotificationPayload, PostgresBackendOptions, PreparedPublication,
    claim_earliest_sql, claim_published_sql, claim_singleton_sql, earliest_availability_sql,
    fail_sql, finish_published_sql, finish_rescheduled_sql, finish_singleton_sql,
    instant_to_unix_ms, published_state_sql, published_task, renew_sql, singleton_state_sql,
    unix_ms_to_instant, unix_timestamp_ms, validate_schema_name,
};
use super::postgres_publishing::{PostgresPublishQuery, PostgresPublishingExecutor};

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

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_trigger
        WHERE tgname = 'bellows_tasks_notify_available'
          AND tgrelid = 'bellows_tasks'::regclass
    ) THEN
        CREATE TRIGGER bellows_tasks_notify_available
        AFTER INSERT OR UPDATE OF lease_worker_id, available_from_unix_ms ON bellows_tasks
        FOR EACH ROW
        EXECUTE FUNCTION bellows_notify_task_available();
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

async fn query_task_id<'e>(
    executor: impl sqlx::Executor<'e, Database = Postgres>,
    query: PostgresPublishQuery<'_>,
) -> Result<i64, sqlx::Error> {
    sqlx::query(query.sql)
        .bind(query.task_name)
        .bind(query.payload_json)
        .bind(query.callback_id)
        .bind(query.available_from_unix_ms)
        .fetch_one(executor)
        .await?
        .try_get("task_id")
}

fn publishing_query_error(error: sqlx::Error) -> BoxBackendError {
    Box::new(PostgresBackendError::Sqlx(error))
}

impl PostgresPublishingExecutor for PgPool {
    async fn query_task_id(
        &mut self,
        query: PostgresPublishQuery<'_>,
    ) -> Result<i64, BoxBackendError> {
        query_task_id(&*self, query)
            .await
            .map_err(publishing_query_error)
    }
}

impl PostgresPublishingExecutor for &PgPool {
    async fn query_task_id(
        &mut self,
        query: PostgresPublishQuery<'_>,
    ) -> Result<i64, BoxBackendError> {
        query_task_id(*self, query)
            .await
            .map_err(publishing_query_error)
    }
}

impl PostgresPublishingExecutor for PgConnection {
    async fn query_task_id(
        &mut self,
        query: PostgresPublishQuery<'_>,
    ) -> Result<i64, BoxBackendError> {
        query_task_id(&mut *self, query)
            .await
            .map_err(publishing_query_error)
    }
}

impl PostgresPublishingExecutor for Transaction<'_, Postgres> {
    async fn query_task_id(
        &mut self,
        query: PostgresPublishQuery<'_>,
    ) -> Result<i64, BoxBackendError> {
        query_task_id(&mut **self, query)
            .await
            .map_err(publishing_query_error)
    }
}

/// Initializes Bellows tables, indexes, and notification triggers in an existing schema.
///
/// This administrative operation uses a temporary connection without a notification listener.
/// Initialization is transactional and idempotent. The schema must already exist; this does not
/// create it or fall back to `public`. Invalid schema names return [`sqlx::Error::Configuration`]
/// before connecting.
pub async fn initialize_postgres_schema(
    database_url: &str,
    schema_name: &str,
) -> Result<(), sqlx::Error> {
    validate_schema_name(schema_name)
        .map_err(|error| sqlx::Error::Configuration(Box::new(error)))?;
    let mut connection = PgConnection::connect(database_url).await?;
    let result = initialize_connection(&mut connection, Some(schema_name)).await;
    let close_result = connection.close().await;
    result.and(close_result)
}

async fn initialize_connection(
    connection: &mut PgConnection,
    schema: Option<&str>,
) -> Result<(), sqlx::Error> {
    let mut transaction = connection.begin().await?;
    let result = async {
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(5_024_011_519_i64)
            .execute(&mut *transaction)
            .await?;

        if let Some(schema) = schema {
            // All callers validate the identifier before opening connections.
            sqlx::query(&format!("SET LOCAL search_path TO \"{schema}\""))
                .execute(&mut *transaction)
                .await?;
        }

        sqlx::raw_sql(INITIALIZE_SCHEMA_SQL)
            .execute(&mut *transaction)
            .await?;
        Ok::<_, sqlx::Error>(())
    }
    .await;

    match result {
        Ok(()) => transaction.commit().await,
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct PostgresTaskOperations {
    pool: PgPool,
    schema: Option<Arc<str>>,
    table_name: Arc<str>,
}

impl PostgresTaskOperations {
    pub(super) async fn connect(
        database_url: &str,
        options: PostgresBackendOptions,
    ) -> Result<Self, sqlx::Error> {
        let table_name = options
            .table_name()
            .map_err(|error| sqlx::Error::Configuration(Box::new(error)))?;
        let pool = PgPoolOptions::new().connect(database_url).await?;
        Ok(Self {
            pool,
            schema: options.schema.map(Arc::from),
            table_name,
        })
    }

    pub(super) async fn initialize(&self) -> Result<(), sqlx::Error> {
        let mut connection = self.pool.acquire().await?;
        initialize_connection(&mut connection, self.schema.as_deref()).await
    }

    pub(super) async fn close(&self) -> Result<(), sqlx::Error> {
        self.pool.close().await;
        Ok(())
    }

    pub(super) async fn publish<T>(
        &self,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
        callback_id: Option<i64>,
        available_from: Option<Instant>,
    ) -> Result<PublishedTask, PublishTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        let publication =
            PreparedPublication::new::<T>(&self.table_name, payload, callback_id, available_from);
        let publication = publication.map_err(|err| {
            PublishTaskError::Backend(Box::new(PostgresBackendError::PayloadSerialization(err)))
        })?;
        let task_id = query_task_id(&self.pool, publication.query())
            .await
            .map_err(|error| PublishTaskError::Backend(publishing_query_error(error)))?;
        published_task(task_id).map_err(|err| {
            PublishTaskError::Backend(Box::new(PostgresBackendError::InvalidTaskId(err)))
        })
    }

    pub(super) async fn claim_published<T>(
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
        let lease_expiration_unix_ms = instant_to_unix_ms(lease_expiration);

        let claimed_row = sqlx::query(&claim_published_sql(&self.table_name))
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
                let current = sqlx::query(&published_state_sql(&self.table_name))
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

                Err(super::postgres_common::unclaimed_task(
                    current.get("lease_worker_id"),
                    current.get("available_from_unix_ms"),
                ))
            }
        }
    }

    pub(super) async fn claim_earliest_published<T>(
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
        let lease_expiration_unix_ms = instant_to_unix_ms(lease_expiration);

        let claimed_row = sqlx::query(&claim_earliest_sql(&self.table_name))
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
                let earliest_available_from =
                    sqlx::query(&earliest_availability_sql(&self.table_name))
                        .bind(T::NAME)
                        .bind(now_unix_ms)
                        .fetch_one(&self.pool)
                        .await
                        .map_err(|err| {
                            ClaimTaskError::Backend(Box::new(PostgresBackendError::Sqlx(err)))
                        })?
                        .get::<Option<i64>, _>("available_from_unix_ms")
                        .and_then(unix_ms_to_instant);

                Err(ClaimTaskError::TaskUnavailable {
                    available_from: earliest_available_from,
                })
            }
        }
    }

    pub(super) async fn claim_singleton<T>(
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
        let lease_expiration_unix_ms = instant_to_unix_ms(lease_expiration);

        let claimed_row = sqlx::query(&claim_singleton_sql(&self.table_name))
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
                let current = sqlx::query(&singleton_state_sql(&self.table_name))
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

                Err(super::postgres_common::unclaimed_task(
                    current.get("lease_worker_id"),
                    current.get("available_from_unix_ms"),
                ))
            }
        }
    }

    pub(super) async fn renew(
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
        let lease_expiration_unix_ms = instant_to_unix_ms(lease_expiration);

        let result = sqlx::query(&renew_sql(&self.table_name))
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

    pub(super) async fn fail(
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
        let available_from_unix_ms = available_from.map(instant_to_unix_ms);

        let result = sqlx::query(&fail_sql(&self.table_name))
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

    pub(super) async fn finish<T>(
        &self,
        worker_id: u64,
        task_id: u64,
        callback_payload: T::Callback,
        available_from: Option<Instant>,
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
        let available_from_unix_ms = available_from.map(instant_to_unix_ms);

        let mut tx =
            self.pool.begin().await.map_err(|err| {
                FinishTaskError::Backend(Box::new(PostgresBackendError::Sqlx(err)))
            })?;

        let finished_row = match (
            <T::Trigger as crate::ActivationStrategy>::KIND,
            available_from,
        ) {
            (crate::ActivationStrategyKind::Singleton, _) => {
                sqlx::query(&finish_singleton_sql(&self.table_name))
                    .bind(task_id_db)
                    .bind(worker_id_db)
                    .bind(available_from_unix_ms)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|err| {
                        FinishTaskError::Backend(Box::new(PostgresBackendError::Sqlx(err)))
                    })?
            }
            (crate::ActivationStrategyKind::Publish, Some(_)) => {
                sqlx::query(&finish_rescheduled_sql(&self.table_name))
                    .bind(task_id_db)
                    .bind(worker_id_db)
                    .bind(available_from_unix_ms)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|err| {
                        FinishTaskError::Backend(Box::new(PostgresBackendError::Sqlx(err)))
                    })?
            }
            (crate::ActivationStrategyKind::Publish, None) => {
                sqlx::query(&finish_published_sql(&self.table_name))
                    .bind(task_id_db)
                    .bind(worker_id_db)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|err| {
                        FinishTaskError::Backend(Box::new(PostgresBackendError::Sqlx(err)))
                    })?
            }
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

            sqlx::query(NOTIFY_SQL)
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
