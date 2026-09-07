//! Listener-free PostgreSQL task execution.

use std::time::Instant;

use crate::{
    PublishActivationStrategy, TaskDefinition,
    backends::{
        ClaimTaskError, ClaimedTask, FailTaskError, FailedTask, FinishTaskError, FinishedTask,
        RenewTaskError, RenewedTaskLease, TaskExecutionBackend,
    },
};

use super::postgres_operations::{PostgresBackendOptions, PostgresTaskOperations};

/// PostgreSQL backend for externally triggered task attempts via [`crate::run_task_once`].
///
/// This cheaply cloneable backend shares a connection pool without opening a dedicated `LISTEN`
/// connection. It implements only [`TaskExecutionBackend`], not publishing or subscriptions.
/// Completion still emits notifications for callbacks awaited by full PostgreSQL backends.
///
/// Connecting does not initialize tables. Use
/// [`super::postgres::initialize_postgres_schema`] separately for an existing named schema.
#[derive(Debug, Clone)]
pub struct PostgresExecutionBackend {
    operations: PostgresTaskOperations,
}

impl PostgresExecutionBackend {
    /// Connects using the database connection's default search path, without a listener.
    ///
    /// The required tables must be initialized separately.
    pub async fn connect(database_url: &str) -> Result<Self, sqlx::Error> {
        Self::connect_with_options(database_url, PostgresBackendOptions::default()).await
    }

    /// Connects with an optional, existing PostgreSQL schema, without a listener.
    ///
    /// Schema names are validated before connecting; invalid names return
    /// [`sqlx::Error::Configuration`]. This does not create schemas or initialize any tables.
    pub async fn connect_with_options(
        database_url: &str,
        options: PostgresBackendOptions,
    ) -> Result<Self, sqlx::Error> {
        Ok(Self {
            operations: PostgresTaskOperations::connect(database_url, options).await?,
        })
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
