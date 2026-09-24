//! Driver-independent PostgreSQL configuration, SQL, notification codec, and clocks.

use std::{io, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::{PublishActivationStrategy, TaskDefinition, backends::PublishedTask, time::Instant};

use super::postgres_publishing::PostgresPublishQuery;

pub(super) const NOTIFY_CHANNEL: &str = "bellows_tasks";
pub(super) const NOTIFY_SQL: &str = "SELECT pg_notify($1, $2)";

/// Options shared by PostgreSQL backends.
#[derive(Debug, Clone, Default)]
pub struct PostgresBackendOptions {
    /// An existing schema containing the Bellows tables.
    ///
    /// Use `[a-z_][a-z0-9_]*`, preferably within PostgreSQL's 63-byte identifier limit.
    /// `None` uses the connection's search path. Notifications remain database-wide.
    pub schema: Option<String>,
}

impl PostgresBackendOptions {
    pub(super) fn table_name(&self) -> Result<Arc<str>, io::Error> {
        match &self.schema {
            Some(schema) => {
                validate_schema_name(schema)?;
                Ok(format!("\"{schema}\".\"bellows_tasks\"").into())
            }
            None => Ok(Arc::from("bellows_tasks")),
        }
    }
}

pub(super) fn validate_schema_name(schema_name: &str) -> Result<(), io::Error> {
    let mut bytes = schema_name.bytes();
    if !bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_lowercase() || byte == b'_')
        || !bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Postgres schema names must start with a lowercase ASCII letter or underscore and \
             contain only lowercase ASCII letters, digits, and underscores",
        ));
    }
    Ok(())
}

pub(super) const DISCOVERY_PAGE_SIZE: i64 = 100;

/// Fixed bounds for a finite keyset pass, not a repeatable-read snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PostgresSweepWindow {
    pub cutoff_unix_ms: i64,
    /// `None` means there were no published rows when the window was captured.
    pub upper_id: Option<i64>,
}

/// Exact stored identity, including IDs/names unsupported by a particular consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresDiscoveryCandidate {
    pub task_id: i64,
    pub task_name: String,
    pub is_singleton: bool,
}

// Only validated, quoted table identifiers are formatted here. All application values remain
// query parameters. Both drivers use exactly these predicates and parameter positions.
pub(super) fn discovery_window_sql(table_name: &str) -> String {
    format!(
        "SELECT FLOOR(EXTRACT(EPOCH FROM statement_timestamp()) * 1000)::bigint AS cutoff_unix_ms,
                MAX(task_id) AS upper_id
         FROM {table_name}"
    )
}

pub(super) fn discovery_page_sql(table_name: &str) -> String {
    format!(
        "SELECT task_id, task_name, task_unique_key IS NOT NULL AS is_singleton FROM {table_name}
         WHERE (available_from_unix_ms IS NULL OR available_from_unix_ms <= $1)
           AND task_id <= $2
           AND ($3::bigint IS NULL OR task_id > $3)
         ORDER BY task_id LIMIT $4"
    )
}

pub(super) fn publish_sql(table_name: &str) -> String {
    format!(
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
"#
    )
}

pub(super) struct PreparedPublication {
    sql: String,
    task_name: &'static str,
    payload_json: String,
    callback_id: Option<i64>,
    available_from_unix_ms: Option<i64>,
}

impl PreparedPublication {
    pub(super) fn new<T>(
        table_name: &str,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
        callback_id: Option<i64>,
        available_from: Option<Instant>,
    ) -> Result<Self, serde_json::Error>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        let payload_json = serde_json::to_string(&payload)?;
        Ok(Self {
            sql: publish_sql(table_name),
            task_name: T::NAME,
            payload_json,
            callback_id,
            available_from_unix_ms: available_from.map(instant_to_unix_ms),
        })
    }

    pub(super) fn query(&self) -> PostgresPublishQuery<'_> {
        PostgresPublishQuery {
            sql: &self.sql,
            task_name: self.task_name,
            payload_json: &self.payload_json,
            callback_id: self.callback_id,
            available_from_unix_ms: self.available_from_unix_ms,
        }
    }
}

pub(super) fn published_task(task_id: i64) -> Result<PublishedTask, std::num::TryFromIntError> {
    Ok(PublishedTask {
        task_id: u64::try_from(task_id)?,
    })
}

pub(super) fn claim_published_sql(table_name: &str) -> String {
    format!(
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
"#
    )
}

pub(super) fn published_state_sql(table_name: &str) -> String {
    format!(
        r#"
SELECT lease_worker_id, available_from_unix_ms
FROM {table_name}
WHERE task_id = $1
  AND task_name = $2
  AND task_unique_key IS NULL
"#
    )
}

pub(super) fn claim_earliest_sql(table_name: &str) -> String {
    format!(
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
"#
    )
}

pub(super) fn earliest_availability_sql(table_name: &str) -> String {
    format!(
        r#"
SELECT MIN(available_from_unix_ms) AS available_from_unix_ms
FROM {table_name}
WHERE task_name = $1
  AND task_unique_key IS NULL
  AND available_from_unix_ms > $2
"#
    )
}

pub(super) fn claim_singleton_sql(table_name: &str) -> String {
    format!(
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
"#
    )
}

pub(super) fn singleton_state_sql(table_name: &str) -> String {
    format!(
        r#"
SELECT lease_worker_id, available_from_unix_ms
FROM {table_name}
WHERE task_name = $1
  AND task_unique_key = $2
"#
    )
}

pub(super) fn renew_sql(table_name: &str) -> String {
    format!(
        r#"
UPDATE {table_name} AS tasks
SET available_from_unix_ms = $1
WHERE task_id = $2
  AND lease_worker_id = $3
"#
    )
}

pub(super) fn fail_sql(table_name: &str) -> String {
    format!(
        r#"
UPDATE {table_name} AS tasks
SET lease_worker_id = NULL,
    available_from_unix_ms = $1
WHERE task_id = $2
  AND lease_worker_id = $3
"#
    )
}

pub(super) fn finish_singleton_sql(table_name: &str) -> String {
    format!(
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
"#
    )
}

pub(super) fn finish_rescheduled_sql(table_name: &str) -> String {
    format!(
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
"#
    )
}

pub(super) fn finish_published_sql(table_name: &str) -> String {
    format!(
        r#"
DELETE FROM {table_name}
WHERE task_id = $1
  AND lease_worker_id = $2
  AND task_unique_key IS NULL
RETURNING task_name, callback_id
"#
    )
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

pub(super) use crate::time::deadlines::{
    instant_to_unix_ms, unix_ms_to_instant, unix_timestamp_ms,
};

/// The follow-up SELECT found a matching row; even a now-due row is not missing.
pub(super) fn unclaimed_task(
    lease_worker_id: Option<i64>,
    available_ms: Option<i64>,
) -> crate::backends::ClaimTaskError {
    use crate::backends::ClaimTaskError;
    let available_from = available_ms.map_or_else(|| Some(Instant::now()), unix_ms_to_instant);
    match available_from {
        Some(expiration) if lease_worker_id.is_some() && expiration > Instant::now() => {
            ClaimTaskError::TaskLeased { expiration }
        }
        _ => ClaimTaskError::TaskUnavailable { available_from },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_validation_accepts_the_typescript_name_rule_without_a_length_limit() {
        for schema in ["a", "_", "_tasks_17", "public", &"a".repeat(128)] {
            assert!(validate_schema_name(schema).is_ok(), "{schema:?}");
            assert_eq!(
                &*PostgresBackendOptions {
                    schema: Some(schema.into()),
                }
                .table_name()
                .unwrap(),
                format!("\"{schema}\".\"bellows_tasks\"")
            );
        }
        for schema in ["", "A", "1a", "a.b", "a\"b", "a b", "a\n", "é", "a-b"] {
            assert!(validate_schema_name(schema).is_err(), "{schema:?}");
        }
    }
}
