//! Shared PostgreSQL task operations, independent of notification listeners.

use std::{
    error::Error as StdError,
    fmt::{Display, Formatter},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use sqlx::{
    Connection, PgConnection, Row,
    postgres::{PgPool, PgPoolOptions},
};

use crate::backends::{
    ClaimTaskError, ClaimedTask, FailTaskError, FailedTask, FinishTaskError, FinishedTask,
    PublishTaskError, PublishedTask, RenewTaskError, RenewedTaskLease,
};
use crate::{PublishActivationStrategy, TaskDefinition};

pub(super) const NOTIFY_CHANNEL: &str = "bellows_tasks";

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

#[cfg(test)]
mod tests {
    use super::validate_schema_name;

    #[test]
    fn schema_validation_accepts_the_typescript_name_rule_without_a_length_limit() {
        for schema in ["a", "_", "_tasks_17", "public", &"a".repeat(128)] {
            assert!(validate_schema_name(schema).is_ok(), "{schema:?}");
        }
    }
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

/// Options shared by PostgreSQL backends.
#[derive(Debug, Clone, Default)]
pub struct PostgresBackendOptions {
    /// An existing schema containing the Bellows tables.
    ///
    /// Names must start with a lowercase ASCII letter or underscore and contain only lowercase
    /// ASCII letters, digits, and underscores. `None` preserves the connection's search path.
    /// This qualifies table operations, but does not isolate the database-wide notification channel.
    pub schema: Option<String>,
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
    validate_schema_name(schema_name)?;
    let mut connection = PgConnection::connect(database_url).await?;
    let result = initialize_connection(&mut connection, Some(schema_name)).await;
    let close_result = connection.close().await;
    result.and(close_result)
}

fn validate_schema_name(schema_name: &str) -> Result<(), sqlx::Error> {
    let mut bytes = schema_name.bytes();
    if !bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_lowercase() || byte == b'_')
        || !bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(sqlx::Error::Configuration(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Postgres schema names must start with a lowercase ASCII letter or underscore and \
             contain only lowercase ASCII letters, digits, and underscores",
        ))));
    }
    Ok(())
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
        if let Some(schema) = &options.schema {
            validate_schema_name(schema)?;
        }
        let table_name = match &options.schema {
            Some(schema) => format!("\"{schema}\".\"bellows_tasks\"").into(),
            None => Arc::from("bellows_tasks"),
        };
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
        let payload_json = serde_json::to_string(&payload).map_err(|err| {
            PublishTaskError::Backend(Box::new(PostgresBackendError::PayloadSerialization(err)))
        })?;

        let now_system = SystemTime::now();
        let available_from_unix_ms =
            available_from.map(|available_from| instant_to_unix_ms(available_from, now_system));

        let row = sqlx::query(&format!(
            r#"
INSERT INTO {table_name} AS tasks (
    task_name,
    task_unique_key,
    payload_json,
    callback_id,
    lease_worker_id,
    available_from_unix_ms
)
VALUES ($1, NULL, $2, $3, NULL, $4)
RETURNING task_id
"#,
            table_name = self.table_name
        ))
        .bind(T::NAME)
        .bind(payload_json)
        .bind(callback_id)
        .bind(available_from_unix_ms)
        .fetch_one(&self.pool)
        .await
        .map_err(|err| PublishTaskError::Backend(Box::new(PostgresBackendError::Sqlx(err))))?;

        let task_id = u64::try_from(row.get::<i64, _>("task_id")).map_err(|err| {
            PublishTaskError::Backend(Box::new(PostgresBackendError::InvalidTaskId(err)))
        })?;

        Ok(PublishedTask { task_id })
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
        let lease_expiration_unix_ms = instant_to_unix_ms(lease_expiration, now_system);

        let claimed_row = sqlx::query(&format!(
            r#"
UPDATE {table_name} AS tasks
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
            table_name = self.table_name
        ))
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
                let current = sqlx::query(&format!(
                    r#"
SELECT lease_worker_id, available_from_unix_ms
FROM {table_name}
WHERE task_id = $1
  AND task_name = $2
  AND task_unique_key IS NULL
"#,
                    table_name = self.table_name
                ))
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
        let lease_expiration_unix_ms = instant_to_unix_ms(lease_expiration, now_system);

        let claimed_row = sqlx::query(&format!(
            r#"
WITH next_task AS (
    SELECT task_id
    FROM {table_name}
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
UPDATE {table_name} AS tasks
SET lease_worker_id = $3,
    available_from_unix_ms = $4
FROM next_task
WHERE tasks.task_id = next_task.task_id
RETURNING tasks.task_id, tasks.payload_json
"#,
            table_name = self.table_name
        ))
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
                let earliest_available_from = sqlx::query(&format!(
                    r#"
SELECT MIN(available_from_unix_ms) AS available_from_unix_ms
FROM {table_name}
WHERE task_name = $1
  AND task_unique_key IS NULL
  AND available_from_unix_ms > $2
"#,
                    table_name = self.table_name
                ))
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
        let lease_expiration_unix_ms = instant_to_unix_ms(lease_expiration, now_system);

        let claimed_row = sqlx::query(&format!(
            r#"
INSERT INTO {table_name} AS tasks (
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
WHERE tasks.task_name = EXCLUDED.task_name
  AND (
        tasks.available_from_unix_ms IS NULL
        OR tasks.available_from_unix_ms <= $5
      )
RETURNING task_id
"#,
            table_name = self.table_name
        ))
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
                let current = sqlx::query(&format!(
                    r#"
SELECT lease_worker_id, available_from_unix_ms
FROM {table_name}
WHERE task_name = $1
  AND task_unique_key = $2
"#,
                    table_name = self.table_name
                ))
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
        let lease_expiration_unix_ms = instant_to_unix_ms(lease_expiration, SystemTime::now());

        let result = sqlx::query(&format!(
            r#"
UPDATE {table_name} AS tasks
SET available_from_unix_ms = $1
WHERE task_id = $2
  AND lease_worker_id = $3
"#,
            table_name = self.table_name
        ))
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
        let available_from_unix_ms =
            available_from.map(|instant| instant_to_unix_ms(instant, SystemTime::now()));

        let result = sqlx::query(&format!(
            r#"
UPDATE {table_name} AS tasks
SET lease_worker_id = NULL,
    available_from_unix_ms = $1
WHERE task_id = $2
  AND lease_worker_id = $3
"#,
            table_name = self.table_name
        ))
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
        let now_system = SystemTime::now();
        let available_from_unix_ms =
            available_from.map(|available_from| instant_to_unix_ms(available_from, now_system));

        let mut tx =
            self.pool.begin().await.map_err(|err| {
                FinishTaskError::Backend(Box::new(PostgresBackendError::Sqlx(err)))
            })?;

        let finished_row = match (
            <T::Trigger as crate::ActivationStrategy>::KIND,
            available_from,
        ) {
            (crate::ActivationStrategyKind::Singleton, _) => sqlx::query(&format!(
                r#"
WITH claimed AS (
    SELECT task_id, task_name, callback_id
    FROM {table_name}
    WHERE task_id = $1
      AND lease_worker_id = $2
      AND task_unique_key IS NOT NULL
    FOR UPDATE
), updated AS (
    UPDATE {table_name} AS tasks
    SET lease_worker_id = NULL,
        callback_id = NULL,
        available_from_unix_ms = $3
    WHERE tasks.task_id IN (SELECT task_id FROM claimed)
    RETURNING tasks.task_id
)
SELECT claimed.task_name, claimed.callback_id
FROM claimed
JOIN updated ON updated.task_id = claimed.task_id
"#,
                table_name = self.table_name
            ))
            .bind(task_id_db)
            .bind(worker_id_db)
            .bind(available_from_unix_ms)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|err| FinishTaskError::Backend(Box::new(PostgresBackendError::Sqlx(err))))?,
            (crate::ActivationStrategyKind::Publish, Some(_)) => sqlx::query(&format!(
                r#"
WITH claimed AS (
    SELECT task_id, task_name, callback_id
    FROM {table_name}
    WHERE task_id = $1
      AND lease_worker_id = $2
      AND task_unique_key IS NULL
    FOR UPDATE
), updated AS (
    UPDATE {table_name} AS tasks
    SET lease_worker_id = NULL,
        callback_id = NULL,
        available_from_unix_ms = $3
    WHERE tasks.task_id IN (SELECT task_id FROM claimed)
    RETURNING tasks.task_id
)
SELECT claimed.task_name, claimed.callback_id
FROM claimed
JOIN updated ON updated.task_id = claimed.task_id
"#,
                table_name = self.table_name
            ))
            .bind(task_id_db)
            .bind(worker_id_db)
            .bind(available_from_unix_ms)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|err| FinishTaskError::Backend(Box::new(PostgresBackendError::Sqlx(err))))?,
            (crate::ActivationStrategyKind::Publish, None) => sqlx::query(&format!(
                r#"
DELETE FROM {table_name}
WHERE task_id = $1
  AND lease_worker_id = $2
  AND task_unique_key IS NULL
RETURNING task_name, callback_id
"#,
                table_name = self.table_name
            ))
            .bind(task_id_db)
            .bind(worker_id_db)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|err| FinishTaskError::Backend(Box::new(PostgresBackendError::Sqlx(err))))?,
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

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum NotificationPayload {
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

pub(super) fn unix_ms_to_instant(unix_ms: i64, now_system: SystemTime) -> Instant {
    let now_instant = Instant::now();
    let now_unix_ms = unix_timestamp_ms(now_system);

    if unix_ms <= now_unix_ms {
        now_instant
    } else {
        let delta_ms = u64::try_from(unix_ms - now_unix_ms).unwrap_or(u64::MAX);
        now_instant + Duration::from_millis(delta_ms)
    }
}
