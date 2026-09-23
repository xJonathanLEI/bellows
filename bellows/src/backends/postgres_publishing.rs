//! Listener-free PostgreSQL task publication.
//!
//! Enable `postgres` on native targets or `cloudflare` on wasm.
//! Import [`TaskPublishingBackend`] for owned publication, or borrow a caller-owned executor with
//! [`PostgresPublishingBackend::publish_with_executor`]. Awaitable publication requires the full
//! backend's listener-backed callback delivery, not merely a callback-bearing definition.

use crate::{
    PublishActivationStrategy, TaskDefinition,
    backends::{BoxBackendError, PublishTaskError, PublishedTask, TaskPublishingBackend},
    time::Instant,
};

pub use super::postgres_common::PostgresBackendOptions;
use super::postgres_common::{PreparedPublication, published_task};
#[cfg(not(target_arch = "wasm32"))]
use super::postgres_operations::PostgresBackendError as PublicationError;
#[cfg(not(target_arch = "wasm32"))]
use super::postgres_operations::PostgresTaskOperations;
#[cfg(target_arch = "wasm32")]
use super::postgres_worker::PostgresTaskOperations;
#[cfg(target_arch = "wasm32")]
pub use super::postgres_worker::PostgresWorkerError;

#[cfg(target_arch = "wasm32")]
use PostgresWorkerError as ConnectionError;
#[cfg(target_arch = "wasm32")]
use PostgresWorkerError as PublicationError;
#[cfg(not(target_arch = "wasm32"))]
use sqlx::Error as ConnectionError;

/// A Bellows-generated parameterized insert and its already-encoded inputs.
///
/// Execute [`Self::sql`] once, binding `$1` to [`Self::task_name`], `$2` to
/// [`Self::payload_json`] (text), `$3` to [`Self::callback_id`] (nullable bigint), and `$4` to
/// [`Self::available_from_unix_ms`] (nullable bigint). Do not reconstruct the insert or interpolate
/// application values into SQL.
#[derive(Debug, Clone, Copy)]
pub struct PostgresPublishQuery<'a> {
    pub sql: &'a str,
    pub task_name: &'a str,
    pub payload_json: &'a str,
    pub callback_id: Option<i64>,
    pub available_from_unix_ms: Option<i64>,
}

/// Query-only access to a caller-owned PostgreSQL executor, including an ORM transaction adapter.
///
/// Return the exact raw signed PostgreSQL `task_id`, without floating-point conversion; Bellows
/// validates the receipt. Return driver errors with their original sources. Implementations
/// need not be cloneable, shared, or `'static`. Bellows borrows the executor only for the operation
/// and never begins, finalizes, or closes its transaction, connection, pool, or connection driver.
///
/// Native adapters support SQLx `PgPool`, `&PgPool`, `PgConnection`, and `Transaction<'_, Postgres>`.
/// Workers adapters support `tokio_postgres::Client` and `tokio_postgres::Transaction<'_>`; their
/// connection driver remains caller-owned.
///
/// A shared pool reuses connections but provides no atomicity with business mutations. Use the same
/// transaction-bound executor for both; an ORM adapter must not fall back to a root client/pool.
/// If an ORM owns a transaction callback, await the outer operation, including commit, before
/// dispatching. No retries, savepoints, schema initialization, session changes, or explicit
/// dispatch are performed.
pub trait PostgresPublishingExecutor: Send {
    fn query_task_id(
        &mut self,
        query: PostgresPublishQuery<'_>,
    ) -> impl Future<Output = Result<i64, BoxBackendError>> + Send;
}

#[cfg_attr(
    target_arch = "wasm32",
    doc = "[`super::postgres::initialize_postgres_schema`]: https://docs.rs/bellows/latest/bellows/backends/postgres/fn.initialize_postgres_schema.html"
)]
/// PostgreSQL backend for producers that only need [`TaskPublishingBackend`].
///
/// Clones share a native SQLx pool or one request-scoped Workers connection. No listener, callback
/// registry, or execution API is provided. Callback-bearing definitions can still be published;
/// insert triggers continue notifying native consumers.
///
/// Initialize tables separately with [`super::postgres::initialize_postgres_schema`].
/// Future publication stores availability, not a scheduler or a future Worker request.
/// Owned publication uses its own connections. To join an application transaction, use
/// [`Self::publish_with_executor`] or [`Self::publish_future_with_executor`] instead.
/// Publication does not atomically dispatch to a Durable Object or retry. An error or cancellation
/// after sending an insert does not prove it failed to commit.
///
/// Owned Workers connections must be request-scoped and use Hyperdrive; await [`Self::close`] on
/// success and error paths before returning a response. Caller-owned connections and drivers
/// require caller cleanup instead. Never retain connections across requests.
/// For immediate or future publication followed by dispatch, `cloudflare::sdk::PostgresPublisher` owns that
/// lifecycle. This lower-level backend retains exact `u64` receipts, including IDs above the
/// Cloudflare processor's safe-positive range; the adapter returns string receipts and checks that range.
#[derive(Debug, Clone)]
pub struct PostgresPublishingBackend {
    operations: PostgresTaskOperations,
}

impl PostgresPublishingBackend {
    /// Publishes through a borrowed executor without creating or taking ownership of any resources.
    ///
    /// Options are validated before querying; an explicit schema qualifies the task table, while
    /// `None` uses the executor's existing search path without falling back to `public`.
    /// Callback-bearing definitions are supported but no callback is registered; singleton
    /// definitions are not publishable.
    ///
    /// A receipt (or receipt-validation error) inside a transaction is provisional: the row may
    /// still be pending, and a later caller commit can fail. Only the caller commits or rolls back.
    /// PostgreSQL delivers trigger notifications at commit; explicit dispatch is a separate caller
    /// action after successful commit. A post-commit dispatch failure does not undo publication:
    /// retain the receipt and recover dispatch rather than blindly republishing.
    /// Dropping this future releases the borrow, not the executor, and does not prove rollback.
    pub async fn publish_with_executor<T>(
        executor: &mut impl PostgresPublishingExecutor,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
        options: PostgresBackendOptions,
    ) -> Result<PublishedTask, PublishTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        Self::publish_on_executor::<T>(executor, payload, None, options).await
    }

    /// Publishes future availability through a borrowed executor, with the same ownership and
    /// provisional-receipt rules as [`Self::publish_with_executor`]. This does not schedule work.
    pub async fn publish_future_with_executor<T>(
        executor: &mut impl PostgresPublishingExecutor,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
        available_from: Instant,
        options: PostgresBackendOptions,
    ) -> Result<PublishedTask, PublishTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        Self::publish_on_executor::<T>(executor, payload, Some(available_from), options).await
    }

    async fn publish_on_executor<T>(
        executor: &mut impl PostgresPublishingExecutor,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
        available_from: Option<Instant>,
        options: PostgresBackendOptions,
    ) -> Result<PublishedTask, PublishTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        let table_name = options
            .table_name()
            .map_err(|error| PublishTaskError::Backend(Box::new(error)))?;
        let publication = PreparedPublication::new::<T>(&table_name, payload, None, available_from);
        #[cfg(not(target_arch = "wasm32"))]
        let publication = publication.map_err(PublicationError::PayloadSerialization);
        #[cfg(target_arch = "wasm32")]
        let publication =
            publication.map_err(|error| PublicationError::PayloadSerialization(error.into()));
        let publication =
            publication.map_err(|error| PublishTaskError::Backend(Box::new(error)))?;
        let task_id = executor
            .query_task_id(publication.query())
            .await
            .map_err(PublishTaskError::Backend)?;
        published_task(task_id).map_err(|error| {
            PublishTaskError::Backend(Box::new(PublicationError::InvalidTaskId(error)))
        })
    }

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

    /// Closes the shared, Bellows-owned publishing connection(s), affecting every clone.
    ///
    /// Waits for the native pool or Workers driver to close; subsequent operations fail.
    /// Workers closes are cancellation-safe and return the same result on repeated calls.
    /// Await before returning a Worker response; dropping clones is insufficient.
    /// The executor-associated functions create no backend or resources for this method to close.
    pub async fn close(&self) -> Result<(), ConnectionError> {
        self.operations.close().await
    }
}

impl TaskPublishingBackend for PostgresPublishingBackend {
    async fn publish<T>(
        &self,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
    ) -> Result<PublishedTask, PublishTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        self.operations.publish::<T>(payload, None, None).await
    }

    async fn publish_future<T>(
        &self,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
        available_from: Instant,
    ) -> Result<PublishedTask, PublishTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        self.operations
            .publish::<T>(payload, None, Some(available_from))
            .await
    }
}
