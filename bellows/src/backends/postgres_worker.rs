//! Request-scoped PostgreSQL operations, tested natively with only socket creation replaced.

use std::{error::Error as StdError, fmt, io, num::TryFromIntError, sync::Arc};

use tokio::sync::{MappedMutexGuard, Mutex, MutexGuard};
use tokio_postgres::{
    Client, Config, GenericClient, Row, Transaction,
    config::{Host, SslNegotiation, TargetSessionAttrs},
    types::{ToSql, Type},
};

use crate::{
    ActivationStrategy, ActivationStrategyKind, PublishActivationStrategy, TaskDefinition,
    backends::{
        BoxBackendError, ClaimTaskError, ClaimedTask, FailTaskError, FailedTask, FinishTaskError,
        FinishedTask, PublishTaskError, PublishedTask, RenewTaskError, RenewedTaskLease,
    },
    platform,
    time::clock::{Instant, SystemTime},
};

use super::postgres_common::{
    DISCOVERY_PAGE_SIZE, NOTIFY_CHANNEL, NOTIFY_SQL, NotificationPayload, PostgresBackendOptions,
    PostgresDiscoveryCandidate, PostgresSweepWindow, PreparedPublication, claim_earliest_sql,
    claim_published_sql, claim_singleton_sql, discovery_page_sql, discovery_window_sql,
    earliest_availability_sql, fail_sql, finish_published_sql, finish_rescheduled_sql,
    finish_singleton_sql, instant_to_unix_ms, published_state_sql, published_task, renew_sql,
    singleton_state_sql, unix_ms_to_instant, unix_timestamp_ms,
};
use super::postgres_publishing::{PostgresPublishQuery, PostgresPublishingExecutor};

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "postgres_worker_tests.rs"]
mod tests;

/// Errors from the request-scoped Workers PostgreSQL backend.
///
/// Retains driver/codec sources; sanitizes configuration and JS socket errors.
#[derive(Debug, Clone)]
pub enum PostgresWorkerError {
    Configuration(&'static str),
    InvalidSchema(Arc<io::Error>),
    Socket(Arc<io::Error>),
    Postgres(Arc<tokio_postgres::Error>),
    Driver(Arc<dyn StdError + Send + Sync>),
    Closed,
    InvalidTaskId(TryFromIntError),
    InvalidWorkerId(TryFromIntError),
    PayloadSerialization(Arc<serde_json::Error>),
    PayloadDeserialization(Arc<serde_json::Error>),
    CallbackSerialization(Arc<serde_json::Error>),
}

impl fmt::Display for PostgresWorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(message) => {
                write!(f, "invalid Workers Postgres configuration: {message}")
            }
            Self::InvalidSchema(error) => error.fmt(f),
            Self::Socket(_) => f.write_str("Workers Postgres socket creation failed"),
            Self::Postgres(error) => write!(f, "postgres operation failed: {error}"),
            Self::Driver(_) => f.write_str("postgres connection driver exited without a result"),
            Self::Closed => f.write_str("postgres connection is closed"),
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

impl StdError for PostgresWorkerError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Configuration(_) | Self::Closed => None,
            Self::InvalidSchema(error) | Self::Socket(error) => Some(error.as_ref()),
            Self::Postgres(error) => Some(error.as_ref()),
            Self::Driver(error) => Some(error.as_ref()),
            Self::InvalidTaskId(error) | Self::InvalidWorkerId(error) => Some(error),
            Self::PayloadSerialization(error)
            | Self::PayloadDeserialization(error)
            | Self::CallbackSerialization(error) => Some(error.as_ref()),
        }
    }
}

impl From<tokio_postgres::Error> for PostgresWorkerError {
    fn from(error: tokio_postgres::Error) -> Self {
        Self::Postgres(Arc::new(error))
    }
}

async fn query_task_id(
    client: &(impl GenericClient + Sync),
    query: PostgresPublishQuery<'_>,
) -> Result<i64, tokio_postgres::Error> {
    client
        .query_typed_one(
            query.sql,
            &[
                (&query.task_name, Type::TEXT),
                (&query.payload_json, Type::TEXT),
                (&query.callback_id, Type::INT8),
                (&query.available_from_unix_ms, Type::INT8),
            ],
        )
        .await?
        .try_get("task_id")
}

fn publishing_query_error(error: tokio_postgres::Error) -> BoxBackendError {
    Box::new(PostgresWorkerError::from(error))
}

impl PostgresPublishingExecutor for Client {
    async fn query_task_id(
        &mut self,
        query: PostgresPublishQuery<'_>,
    ) -> Result<i64, BoxBackendError> {
        query_task_id(self, query)
            .await
            .map_err(publishing_query_error)
    }
}

impl PostgresPublishingExecutor for Transaction<'_> {
    async fn query_task_id(
        &mut self,
        query: PostgresPublishQuery<'_>,
    ) -> Result<i64, BoxBackendError> {
        query_task_id(self, query)
            .await
            .map_err(publishing_query_error)
    }
}

fn publish_error(error: impl Into<PostgresWorkerError>) -> PublishTaskError {
    PublishTaskError::Backend(Box::new(error.into()))
}

fn claim_error(error: impl Into<PostgresWorkerError>) -> ClaimTaskError {
    ClaimTaskError::Backend(Box::new(error.into()))
}

fn renew_error(error: impl Into<PostgresWorkerError>) -> RenewTaskError {
    RenewTaskError::Backend(Box::new(error.into()))
}

fn fail_error(error: impl Into<PostgresWorkerError>) -> FailTaskError {
    FailTaskError::Backend(Box::new(error.into()))
}

fn finish_error(error: impl Into<PostgresWorkerError>) -> FinishTaskError {
    FinishTaskError::Backend(Box::new(error.into()))
}

// connect_raw deliberately bypasses native host selection and socket/session configuration.
// Reject unsupported configurations instead of silently choosing a different endpoint or TLS mode.
fn connection_config(database_url: &str) -> Result<(Config, String, u16), PostgresWorkerError> {
    let config: Config = database_url
        .parse()
        .map_err(|_| PostgresWorkerError::Configuration("could not parse the connection string"))?;
    let host = match config.get_hosts() {
        [Host::Tcp(host)]
            if !host.is_empty()
                && !host
                    .chars()
                    .any(|c| c.is_whitespace() || matches!(c, '/' | '\\' | '\0')) =>
        {
            host.clone()
        }
        _ => {
            return Err(PostgresWorkerError::Configuration(
                "exactly one TCP host is required",
            ));
        }
    };
    let port = match config.get_ports() {
        [] => 5432,
        [port] if *port != 0 => *port,
        _ => {
            return Err(PostgresWorkerError::Configuration(
                "exactly one nonzero TCP port is required",
            ));
        }
    };
    if !config.get_hostaddrs().is_empty() {
        return Err(PostgresWorkerError::Configuration(
            "hostaddr overrides are not supported",
        ));
    }
    if config.get_options().is_some()
        || config.get_target_session_attrs() != TargetSessionAttrs::Any
    {
        return Err(PostgresWorkerError::Configuration(
            "session setup options are not supported",
        ));
    }
    if config.get_ssl_negotiation() != SslNegotiation::Postgres {
        return Err(PostgresWorkerError::Configuration(
            "TLS requires PostgreSQL STARTTLS negotiation",
        ));
    }
    Ok((config, host, port))
}

#[derive(Clone)]
pub(super) struct PostgresTaskOperations {
    shared: Arc<Shared>,
    table_name: Arc<str>,
}

struct Shared {
    // One client, not one sender per backend clone. Also prevents transaction interleaving.
    client: Mutex<Option<Client>>,
    driver: Mutex<Driver>,
}

enum Driver {
    Running(platform::JoinHandle<Result<(), PostgresWorkerError>>),
    Closed(Result<(), PostgresWorkerError>),
}

impl fmt::Debug for PostgresTaskOperations {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresTaskOperations")
            .field("table_name", &self.table_name)
            .finish_non_exhaustive()
    }
}

impl PostgresTaskOperations {
    pub(super) async fn begin_sweep(&self) -> Result<PostgresSweepWindow, PostgresWorkerError> {
        let row = self
            .client()
            .await?
            .query_typed_one(&discovery_window_sql(&self.table_name), &[])
            .await?;
        Ok(PostgresSweepWindow {
            cutoff_unix_ms: row.try_get("cutoff_unix_ms")?,
            upper_id: row.try_get("upper_id")?,
        })
    }

    pub(super) async fn read_page(
        &self,
        window: &PostgresSweepWindow,
        last_seen_id: Option<i64>,
    ) -> Result<Vec<PostgresDiscoveryCandidate>, PostgresWorkerError> {
        self.client()
            .await?
            .query_typed(
                &discovery_page_sql(&self.table_name),
                &[
                    (&window.cutoff_unix_ms, Type::INT8),
                    (&window.upper_id, Type::INT8),
                    (&last_seen_id, Type::INT8),
                    (&DISCOVERY_PAGE_SIZE, Type::INT8),
                ],
            )
            .await?
            .into_iter()
            .map(|row| {
                Ok(PostgresDiscoveryCandidate {
                    task_id: row.try_get("task_id")?,
                    task_name: row.try_get("task_name")?,
                    is_singleton: row.try_get("is_singleton")?,
                })
            })
            .collect()
    }

    #[cfg(target_arch = "wasm32")]
    #[worker::send]
    pub(super) async fn connect(
        database_url: &str,
        options: PostgresBackendOptions,
    ) -> Result<Self, PostgresWorkerError> {
        use tokio_postgres::config::SslMode;
        use worker::{SecureTransport, Socket, postgres_tls::PassthroughTls, send::SendFuture};

        let table_name = options
            .table_name()
            .map_err(|error| PostgresWorkerError::InvalidSchema(Arc::new(error)))?;
        let (config, host, port) = connection_config(database_url)?;
        let transport = match config.get_ssl_mode() {
            SslMode::Disable => SecureTransport::Off,
            _ => SecureTransport::StartTls,
        };
        let socket = Socket::builder()
            .secure_transport(transport)
            .connect(host, port)
            .map_err(|_| {
                PostgresWorkerError::Socket(Arc::new(io::Error::other(
                    "Workers could not create the PostgreSQL socket",
                )))
            })?;
        // The SDK delegates TLS/certificate validation to Workers. The config still controls
        // disable/prefer/require; no native Tokio reactor, insecure verifier, or SQL proxy is used.
        let (client, connection) = config.connect_raw(socket, PassthroughTls).await?;
        Ok(Self::from_connection(
            client,
            SendFuture::new(connection),
            table_name,
        ))
    }

    fn from_connection(
        client: Client,
        connection: impl Future<Output = Result<(), tokio_postgres::Error>> + Send + 'static,
        table_name: Arc<str>,
    ) -> Self {
        let driver = platform::spawn(async move {
            let result = connection.await.map_err(PostgresWorkerError::from);
            if let Err(error) = &result {
                tracing::warn!(%error, "postgres connection driver failed");
            }
            result
        });
        Self {
            shared: Arc::new(Shared {
                client: Mutex::new(Some(client)),
                driver: Mutex::new(Driver::Running(driver)),
            }),
            table_name,
        }
    }

    async fn client(&self) -> Result<MappedMutexGuard<'_, Client>, PostgresWorkerError> {
        MutexGuard::try_map(self.shared.client.lock().await, Option::as_mut)
            .map_err(|_| PostgresWorkerError::Closed)
    }

    pub(super) async fn close(&self) -> Result<(), PostgresWorkerError> {
        // Await the current operation, then release the only client sender across ALL clones.
        self.shared.client.lock().await.take();
        let mut driver = self.shared.driver.lock().await;
        if let Driver::Running(handle) = &mut *driver {
            // Borrow, don't take, the handle: cancellation of close must not lose the result.
            let result = handle
                .await
                .map_err(|error| PostgresWorkerError::Driver(Arc::new(error)))
                .and_then(|result| result);
            *driver = Driver::Closed(result);
        }
        match &*driver {
            Driver::Closed(result) => result.clone(),
            Driver::Running(_) => unreachable!("the driver was awaited above"),
        }
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
        let publication = publication.map_err(|error| {
            publish_error(PostgresWorkerError::PayloadSerialization(Arc::new(error)))
        })?;
        let client = self.client().await.map_err(publish_error)?;
        let task_id = query_task_id(&*client, publication.query())
            .await
            .map_err(publish_error)?;
        published_task(task_id)
            .map_err(|error| publish_error(PostgresWorkerError::InvalidTaskId(error)))
    }

    pub(super) async fn claim_published<T>(
        &self,
        worker_id: u64,
        task_id: u64,
        lease_expiration: Instant,
    ) -> Result<ClaimedTask<<T::Trigger as PublishActivationStrategy>::Payload>, ClaimTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        let task_id_db = i64::try_from(task_id)
            .map_err(|error| claim_error(PostgresWorkerError::InvalidTaskId(error)))?;
        let worker_id_db = i64::try_from(worker_id)
            .map_err(|error| claim_error(PostgresWorkerError::InvalidWorkerId(error)))?;
        let now_system = SystemTime::now();
        let now = unix_timestamp_ms(now_system);
        let expiration = instant_to_unix_ms(lease_expiration);
        let client = self.client().await.map_err(claim_error)?;
        let row = client
            .query_typed_opt(
                &claim_published_sql(&self.table_name),
                &[
                    (&worker_id_db, Type::INT8),
                    (&expiration, Type::INT8),
                    (&task_id_db, Type::INT8),
                    (&T::NAME, Type::TEXT),
                    (&now, Type::INT8),
                ],
            )
            .await
            .map_err(claim_error)?;
        if let Some(row) = row {
            return Ok(ClaimedTask {
                task_id,
                task_payload: decode_payload(&row)?,
                lease_expiration,
            });
        }
        let current = client
            .query_typed_opt(
                &published_state_sql(&self.table_name),
                &[(&task_id_db, Type::INT8), (&T::NAME, Type::TEXT)],
            )
            .await
            .map_err(claim_error)?;
        Err(unclaimed(current))
    }

    pub(super) async fn claim_earliest_published<T>(
        &self,
        worker_id: u64,
        lease_expiration: Instant,
    ) -> Result<ClaimedTask<<T::Trigger as PublishActivationStrategy>::Payload>, ClaimTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        let worker_id_db = i64::try_from(worker_id)
            .map_err(|error| claim_error(PostgresWorkerError::InvalidWorkerId(error)))?;
        let now_system = SystemTime::now();
        let now = unix_timestamp_ms(now_system);
        let expiration = instant_to_unix_ms(lease_expiration);
        let client = self.client().await.map_err(claim_error)?;
        let row = client
            .query_typed_opt(
                &claim_earliest_sql(&self.table_name),
                &[
                    (&T::NAME, Type::TEXT),
                    (&now, Type::INT8),
                    (&worker_id_db, Type::INT8),
                    (&expiration, Type::INT8),
                ],
            )
            .await
            .map_err(claim_error)?;
        if let Some(row) = row {
            return Ok(ClaimedTask {
                task_id: decode_task_id(&row)?,
                task_payload: decode_payload(&row)?,
                lease_expiration,
            });
        }
        let row = client
            .query_typed_one(
                &earliest_availability_sql(&self.table_name),
                &[(&T::NAME, Type::TEXT), (&now, Type::INT8)],
            )
            .await
            .map_err(claim_error)?;
        let available_from = row
            .try_get::<_, Option<i64>>("available_from_unix_ms")
            .map_err(claim_error)?
            .and_then(unix_ms_to_instant);
        Err(ClaimTaskError::TaskUnavailable { available_from })
    }

    pub(super) async fn claim_singleton<T>(
        &self,
        worker_id: u64,
        lease_expiration: Instant,
    ) -> Result<ClaimedTask<()>, ClaimTaskError>
    where
        T: TaskDefinition,
    {
        let worker_id_db = i64::try_from(worker_id)
            .map_err(|error| claim_error(PostgresWorkerError::InvalidWorkerId(error)))?;
        let now_system = SystemTime::now();
        let now = unix_timestamp_ms(now_system);
        let expiration = instant_to_unix_ms(lease_expiration);
        let client = self.client().await.map_err(claim_error)?;
        let row = client
            .query_typed_opt(
                &claim_singleton_sql(&self.table_name),
                &[
                    (&T::NAME, Type::TEXT),
                    (&T::NAME, Type::TEXT),
                    (&worker_id_db, Type::INT8),
                    (&expiration, Type::INT8),
                    (&now, Type::INT8),
                ],
            )
            .await
            .map_err(claim_error)?;
        if let Some(row) = row {
            return Ok(ClaimedTask {
                task_id: decode_task_id(&row)?,
                task_payload: (),
                lease_expiration,
            });
        }
        let current = client
            .query_typed_opt(
                &singleton_state_sql(&self.table_name),
                &[(&T::NAME, Type::TEXT), (&T::NAME, Type::TEXT)],
            )
            .await
            .map_err(claim_error)?;
        Err(unclaimed(current))
    }

    pub(super) async fn renew(
        &self,
        worker_id: u64,
        task_id: u64,
        lease_expiration: Instant,
    ) -> Result<RenewedTaskLease, RenewTaskError> {
        let task_id_db = i64::try_from(task_id)
            .map_err(|error| renew_error(PostgresWorkerError::InvalidTaskId(error)))?;
        let worker_id_db = i64::try_from(worker_id)
            .map_err(|error| renew_error(PostgresWorkerError::InvalidWorkerId(error)))?;
        let expiration = instant_to_unix_ms(lease_expiration);
        let affected = self
            .client()
            .await
            .map_err(renew_error)?
            .execute_typed(
                &renew_sql(&self.table_name),
                &[
                    (&expiration, Type::INT8),
                    (&task_id_db, Type::INT8),
                    (&worker_id_db, Type::INT8),
                ],
            )
            .await
            .map_err(renew_error)?;
        if affected == 0 {
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
        let task_id_db = i64::try_from(task_id)
            .map_err(|error| fail_error(PostgresWorkerError::InvalidTaskId(error)))?;
        let worker_id_db = i64::try_from(worker_id)
            .map_err(|error| fail_error(PostgresWorkerError::InvalidWorkerId(error)))?;
        let available = available_from.map(instant_to_unix_ms);
        let affected = self
            .client()
            .await
            .map_err(fail_error)?
            .execute_typed(
                &fail_sql(&self.table_name),
                &[
                    (&available, Type::INT8),
                    (&task_id_db, Type::INT8),
                    (&worker_id_db, Type::INT8),
                ],
            )
            .await
            .map_err(fail_error)?;
        if affected == 0 {
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
        let task_id_db = i64::try_from(task_id)
            .map_err(|error| finish_error(PostgresWorkerError::InvalidTaskId(error)))?;
        let worker_id_db = i64::try_from(worker_id)
            .map_err(|error| finish_error(PostgresWorkerError::InvalidWorkerId(error)))?;
        let callback_payload_json = serde_json::to_string(&callback_payload).map_err(|error| {
            finish_error(PostgresWorkerError::CallbackSerialization(Arc::new(error)))
        })?;
        let available = available_from.map(instant_to_unix_ms);
        let mut client = self.client().await.map_err(finish_error)?;
        let tx = client.transaction().await.map_err(finish_error)?;
        let result = async {
            let (sql, reschedule) = match (T::Trigger::KIND, available_from) {
                (ActivationStrategyKind::Singleton, _) => {
                    (finish_singleton_sql(&self.table_name), true)
                }
                (ActivationStrategyKind::Publish, Some(_)) => {
                    (finish_rescheduled_sql(&self.table_name), true)
                }
                (ActivationStrategyKind::Publish, None) => {
                    (finish_published_sql(&self.table_name), false)
                }
            };
            let mut params: Vec<(&(dyn ToSql + Sync), Type)> =
                vec![(&task_id_db, Type::INT8), (&worker_id_db, Type::INT8)];
            if reschedule {
                params.push((&available, Type::INT8));
            }
            let row = tx
                .query_typed_opt(&sql, &params)
                .await
                .map_err(finish_error)?;
            let Some(row) = row else {
                return Err(FinishTaskError::LeaseLost);
            };
            let callback_id: Option<i64> = row.try_get("callback_id").map_err(finish_error)?;
            if let Some(callback_id) = callback_id {
                let payload = serde_json::to_string(&NotificationPayload::TaskCallback {
                    task_name: row.try_get("task_name").map_err(finish_error)?,
                    callback_id,
                    callback_payload_json,
                })
                .expect("postgres callback notification payload should serialize");
                tx.query_typed(
                    NOTIFY_SQL,
                    &[(&NOTIFY_CHANNEL, Type::TEXT), (&payload, Type::TEXT)],
                )
                .await
                .map_err(finish_error)?;
            }
            Ok(FinishedTask { task_id })
        }
        .await;
        match result {
            Ok(finished) => {
                tx.commit().await.map_err(finish_error)?;
                Ok(finished)
            }
            Err(error) => {
                // Await rollback on ordinary error paths, including a failed callback notification.
                // Transaction's Drop also queues rollback if this future is cancelled mid-query.
                if let Err(rollback) = tx.rollback().await {
                    tracing::warn!(%rollback, "postgres finalization rollback failed");
                }
                Err(error)
            }
        }
    }
}

fn decode_task_id(row: &Row) -> Result<u64, ClaimTaskError> {
    u64::try_from(row.try_get::<_, i64>("task_id").map_err(claim_error)?)
        .map_err(|error| claim_error(PostgresWorkerError::InvalidTaskId(error)))
}

fn decode_payload<P: serde::de::DeserializeOwned>(row: &Row) -> Result<P, ClaimTaskError> {
    let json: &str = row.try_get("payload_json").map_err(claim_error)?;
    serde_json::from_str(json)
        .map_err(|error| claim_error(PostgresWorkerError::PayloadDeserialization(Arc::new(error))))
}

fn unclaimed(row: Option<Row>) -> ClaimTaskError {
    let Some(row) = row else {
        return ClaimTaskError::TaskNotFound;
    };
    let available = match row.try_get::<_, Option<i64>>("available_from_unix_ms") {
        Ok(available) => available,
        Err(error) => return claim_error(error),
    };
    match row.try_get::<_, Option<i64>>("lease_worker_id") {
        Ok(worker_id) => super::postgres_common::unclaimed_task(worker_id, available),
        Err(error) => claim_error(error),
    }
}
