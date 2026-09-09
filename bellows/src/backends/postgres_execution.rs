//! Listener-free PostgreSQL task execution.
//!
//! Enable `postgres` on native targets or `cloudflare` on wasm.

use crate::{
    PublishActivationStrategy, TaskDefinition,
    backends::{
        ClaimTaskError, ClaimedTask, FailTaskError, FailedTask, FinishTaskError, FinishedTask,
        RenewTaskError, RenewedTaskLease, TaskExecutionBackend,
    },
    time::Instant,
};

pub use super::postgres_common::PostgresBackendOptions;
#[cfg(not(target_arch = "wasm32"))]
use super::postgres_operations::PostgresTaskOperations;
#[cfg(target_arch = "wasm32")]
use super::postgres_worker::PostgresTaskOperations;
#[cfg(target_arch = "wasm32")]
pub use super::postgres_worker::PostgresWorkerError;

#[cfg(target_arch = "wasm32")]
use PostgresWorkerError as ConnectionError;
#[cfg(not(target_arch = "wasm32"))]
use sqlx::Error as ConnectionError;

#[cfg_attr(
    target_arch = "wasm32",
    doc = "[`super::postgres::initialize_postgres_schema`]: https://docs.rs/bellows/latest/bellows/backends/postgres/fn.initialize_postgres_schema.html"
)]
/// PostgreSQL backend for externally triggered task attempts via [`crate::run_task_once`].
///
/// Clones share a native SQLx pool or one request-scoped Workers connection. Implements execution
/// only, without publishing or `LISTEN`; completion still supports transactional callback notifications.
///
/// Initialize tables separately with [`super::postgres::initialize_postgres_schema`].
///
/// Workers must use a Hyperdrive binding and await [`Self::close`] after every attempt.
/// Never retain connections across requests; use a separate side-effect connection so renewals can proceed.
#[derive(Debug, Clone)]
pub struct PostgresExecutionBackend {
    operations: PostgresTaskOperations,
}

impl PostgresExecutionBackend {
    /// Connects using the database connection's default search path, without a listener.
    ///
    /// Workers should prefer [`Self::connect_with_options`] with an explicit schema.
    pub async fn connect(database_url: &str) -> Result<Self, ConnectionError> {
        Self::connect_with_options(database_url, PostgresBackendOptions::default()).await
    }

    #[cfg_attr(
        target_arch = "wasm32",
        doc = "[`sqlx::Error::Configuration`]: https://docs.rs/sqlx/latest/sqlx/enum.Error.html#variant.Configuration"
    )]
    /// Connects with an optional, existing PostgreSQL schema, without a listener.
    ///
    /// Validates schema names before connecting; does not initialize tables or set `search_path`.
    /// Invalid schemas return [`sqlx::Error::Configuration`] natively or `PostgresWorkerError` on Workers.
    ///
    /// Workers require one TCP host and nonzero port (default 5432). Host lists, `hostaddr`, session
    /// options, and direct TLS are unsupported. `sslmode` controls SDK STARTTLS with host-verified
    /// certificates; native socket timeout/keepalive settings do not apply.
    pub async fn connect_with_options(
        database_url: &str,
        options: PostgresBackendOptions,
    ) -> Result<Self, ConnectionError> {
        Ok(Self {
            operations: PostgresTaskOperations::connect(database_url, options).await?,
        })
    }

    /// Closes the shared execution connection(s), affecting every clone.
    ///
    /// Waits for the native pool or Workers driver to close; subsequent operations fail.
    /// Workers closes are cancellation-safe and return the same result on repeated calls.
    ///
    /// Await before returning a Worker response; dropping clones is insufficient.
    /// Does not cancel task execution or side effects.
    pub async fn close(&self) -> Result<(), ConnectionError> {
        self.operations.close().await
    }
}

impl TaskExecutionBackend for PostgresExecutionBackend {
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
        self.operations
            .claim_published::<T>(worker_id, task_id, lease_expiration)
            .await
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
        self.operations
            .claim_earliest_published::<T>(worker_id, lease_expiration)
            .await
    }

    async fn claim_singleton<T>(
        &self,
        worker_id: u64,
        lease_expiration: Instant,
    ) -> Result<ClaimedTask<()>, ClaimTaskError>
    where
        T: TaskDefinition,
    {
        self.operations
            .claim_singleton::<T>(worker_id, lease_expiration)
            .await
    }

    async fn renew(
        &self,
        worker_id: u64,
        task_id: u64,
        lease_expiration: Instant,
    ) -> Result<RenewedTaskLease, RenewTaskError> {
        self.operations
            .renew(worker_id, task_id, lease_expiration)
            .await
    }

    async fn fail(
        &self,
        worker_id: u64,
        task_id: u64,
        available_from: Option<Instant>,
    ) -> Result<FailedTask, FailTaskError> {
        self.operations
            .fail(worker_id, task_id, available_from)
            .await
    }

    async fn finish<T>(
        &self,
        worker_id: u64,
        task_id: u64,
        callback_payload: T::Callback,
        available_from: Option<Instant>,
    ) -> Result<FinishedTask, FinishTaskError>
    where
        T: TaskDefinition,
    {
        self.operations
            .finish::<T>(worker_id, task_id, callback_payload, available_from)
            .await
    }
}
