//! Driver-independent PostgreSQL configuration, SQL, notification codec, and clocks.

use std::{io, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};

use crate::time::clock::{Instant, SystemTime, UNIX_EPOCH};

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

// Only validated, quoted table identifiers are formatted here. All application values remain
// query parameters. Both drivers use exactly these predicates and parameter positions.
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

pub(super) fn unix_timestamp_ms(time: SystemTime) -> i64 {
    let duration = time.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

pub(super) fn instant_to_unix_ms(instant: Instant, now_system: SystemTime) -> i64 {
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
